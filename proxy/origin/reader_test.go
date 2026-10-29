package origin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testBudget struct {
	used, peak atomic.Int64
	limit      int64
}

func (b *testBudget) TryAcquire(n int64) bool {
	for old := b.used.Load(); old+n <= b.limit; old = b.used.Load() {
		if b.used.CompareAndSwap(old, old+n) {
			for peak := b.peak.Load(); old+n > peak; peak = b.peak.Load() {
				if b.peak.CompareAndSwap(peak, old+n) {
					break
				}
			}
			return true
		}
	}
	return false
}
func (b *testBudget) Release(n int64) { b.used.Add(-n) }

type testOrigin struct {
	t                  testing.TB
	data               []byte
	server             *httptest.Server
	client             *http.Client
	mu                 sync.Mutex
	requests           map[int64]int
	active, peak, sent atomic.Int64
	behavior           func(http.ResponseWriter, *http.Request, int64, int64, int) bool
}

func newTestOrigin(t testing.TB, size int) *testOrigin {
	s := &testOrigin{t: t, data: make([]byte, size), requests: make(map[int64]int)}
	for i := range s.data {
		s.data[i] = byte(i*31 + i/251)
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	s.client = s.server.Client()
	t.Cleanup(s.server.Close)
	return s
}

func (s *testOrigin) serve(w http.ResponseWriter, req *http.Request) {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for peak := s.peak.Load(); active > peak; peak = s.peak.Load() {
		if s.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	start, end := int64(0), int64(len(s.data)-1)
	if value := req.Header.Get("Range"); value != "" {
		if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= int64(len(s.data)) {
			s.t.Errorf("invalid request range %q", value)
			w.WriteHeader(416)
			return
		}
		if req.Header.Get("If-Match") != `"version-one"` {
			s.t.Error("range missing version pin")
			w.WriteHeader(412)
			return
		}
	}
	s.mu.Lock()
	s.requests[start]++
	attempt := s.requests[start]
	s.mu.Unlock()
	if s.behavior != nil && s.behavior(w, req, start, end, attempt) {
		return
	}
	s.headers(w, req, start, end)
	n, _ := w.Write(s.data[start : end+1])
	s.sent.Add(int64(n))
}
func (s *testOrigin) headers(w http.ResponseWriter, req *http.Request, start, end int64) {
	w.Header().Set("ETag", `"version-one"`)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	if req.Header.Get("Range") != "" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.data)))
		w.WriteHeader(206)
	} else {
		w.WriteHeader(200)
	}
}
func (s *testOrigin) fetch(ctx context.Context, start, end int64, etag string, extra bool) (*http.Response, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", s.server.URL+"/bucket/key", nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("If-Match", etag)
	return s.client.Do(req)
}
func (s *testOrigin) open(t testing.TB, r *Reader) io.ReadCloser {
	t.Helper()
	req, _ := http.NewRequest("GET", s.server.URL+"/bucket/key", nil)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		r.Wrap(req, resp, s.fetch)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp.Body
}
func testReader(t testing.TB) (*Reader, *testBudget) {
	t.Helper()
	budget := &testBudget{limit: 32 << 20}
	r := New(budget)
	r.policy = policy{256 << 10, 80 * time.Millisecond, 500 * time.Millisecond, 4 * time.Second}
	t.Cleanup(func() {
		if n := budget.used.Load(); n != 0 {
			t.Errorf("leaked %d bytes", n)
		}
		if n := len(r.slots); n != 0 {
			t.Errorf("leaked %d slots", n)
		}
	})
	return r, budget
}

func TestOrderedStreamingAndHedge(t *testing.T) {
	for _, slowPart := range []int64{0, 256 << 10, 7 * 256 << 10} {
		t.Run(strconv.FormatInt(slowPart, 10), func(t *testing.T) {
			r, _ := testReader(t)
			s := newTestOrigin(t, 2<<20)
			canceled := make(chan struct{})
			s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
				if start != slowPart || attempt != 1 {
					return false
				}
				s.headers(w, req, start, end)
				// Commit a prefix before stalling, so recovery must skip bytes
				// already sent downstream rather than append the prefix twice.
				w.Write(s.data[start : start+8192])
				w.(http.Flusher).Flush()
				<-req.Context().Done()
				close(canceled)
				return true
			}
			body := s.open(t, r)
			prefix := make([]byte, 1024)
			start := time.Now()
			if _, err := io.ReadFull(body, prefix); err != nil {
				t.Fatal(err)
			}
			if time.Since(start) >= r.policy.hedgeDelay {
				t.Fatal("first bytes waited for a complete range")
			}
			rest, err := io.ReadAll(body)
			body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(append(prefix, rest...), s.data) {
				t.Fatal("reordered or duplicated data")
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("loser not canceled")
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if n := s.requests[slowPart]; n != 2 {
				t.Fatalf("slow part requested %d times", n)
			}
		})
	}
}

func TestInvalidRangeNeverCompletes(t *testing.T) {
	for _, mode := range []string{"version", "range", "length", "ignored", "missing", "denied", "changed", "short"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := testReader(t)
			s := newTestOrigin(t, 2<<20)
			s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
				if start != 256<<10 {
					return false
				}
				if mode == "ignored" {
					w.WriteHeader(200)
					return true
				}
				if mode == "missing" {
					w.WriteHeader(404)
					return true
				}
				if mode == "denied" {
					w.WriteHeader(403)
					return true
				}
				if mode == "changed" {
					w.WriteHeader(412)
					return true
				}
				w.Header().Set("ETag", `"version-one"`)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.data)))
				w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
				if mode == "version" {
					w.Header().Set("ETag", `"version-two"`)
				}
				if mode == "range" {
					w.Header().Set("Content-Range", "bytes 0-100/101")
				}
				if mode == "length" {
					w.Header().Set("Content-Length", "10")
				}
				w.WriteHeader(206)
				if mode == "short" {
					w.Write(s.data[start : start+1024])
				}
				return true
			}
			body := s.open(t, r)
			got, err := io.ReadAll(body)
			body.Close()
			if err == nil {
				t.Fatal("invalid object returned success")
			}
			if !bytes.Equal(got, s.data[:len(got)]) {
				t.Fatal("corrupt bytes emitted")
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			want := 1
			if mode == "short" {
				want = 3
			}
			if s.requests[256<<10] != want {
				t.Fatalf("requests=%v, want %d attempts", s.requests, want)
			}
		})
	}
}

func TestRetryTransientAndThreeAttemptLimit(t *testing.T) {
	for _, succeed := range []bool{true, false} {
		t.Run(strconv.FormatBool(succeed), func(t *testing.T) {
			r, _ := testReader(t)
			s := newTestOrigin(t, 2<<20)
			s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
				if start == 256<<10 && (!succeed || attempt < 3) {
					w.WriteHeader(503)
					return true
				}
				return false
			}
			body := s.open(t, r)
			got, err := io.ReadAll(body)
			body.Close()
			if succeed && (err != nil || !bytes.Equal(got, s.data)) {
				t.Fatalf("recovery failed: %v", err)
			}
			if !succeed && err == nil {
				t.Fatal("expected failure")
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.requests[256<<10] != 3 {
				t.Fatal(s.requests)
			}
		})
	}
}

func TestBackpressureBoundsReadAhead(t *testing.T) {
	r, budget := testReader(t)
	s := newTestOrigin(t, 8<<20)
	body := s.open(t, r)
	// Wait for the initial window to complete without consuming it. A slow
	// downstream must neither open the next window nor hedge complete buffers.
	deadline := time.NewTimer(3 * r.policy.hedgeDelay)
	defer deadline.Stop()
	<-deadline.C
	s.mu.Lock()
	count := len(s.requests)
	for _, n := range s.requests {
		if n != 1 {
			t.Error("slow downstream triggered a duplicate")
		}
	}
	s.mu.Unlock()
	if count != Concurrency {
		t.Fatalf("read ahead spans %d parts", count)
	}
	if budget.peak.Load() != int64(Concurrency+maxExtra)*r.policy.partSize {
		t.Fatal("unaccounted memory")
	}
	body.Close()
	if budget.used.Load() != 0 {
		t.Fatal("close did not release reservation")
	}
}

func TestBudgetFallbackAndUnsupportedResponses(t *testing.T) {
	for _, mode := range []string{"budget", "slots", "small", "weak", "unknown", "encoded", "range"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := testReader(t)
			req, _ := http.NewRequest("GET", "http://origin/bucket/key", nil)
			original := io.NopCloser(bytes.NewReader(nil))
			resp := &http.Response{StatusCode: 200, ContentLength: 2 << 20, Body: original, Header: http.Header{"Etag": {`"one"`}, "Accept-Ranges": {"bytes"}}}
			switch mode {
			case "budget":
				r.budget = &testBudget{}
			case "slots":
				for range maxRequests {
					r.acquire()
				}
				defer func() {
					for range maxRequests {
						r.release()
					}
				}()
			case "small":
				resp.ContentLength = 1
			case "weak":
				resp.Header.Set("ETag", `W/"one"`)
			case "unknown":
				resp.ContentLength = -1
			case "encoded":
				resp.Header.Set("Content-Encoding", "gzip")
			case "range":
				req.Header.Set("Range", "bytes=0-10")
			}
			r.Wrap(req, resp, func(context.Context, int64, int64, string, bool) (*http.Response, error) {
				t.Error("unexpected range")
				return nil, context.Canceled
			})
			if resp.Body != original {
				t.Fatal("fallback replaced original body")
			}
		})
	}
}

func TestCloseInterruptsStalledReads(t *testing.T) {
	r, _ := testReader(t)
	s := newTestOrigin(t, 2<<20)
	s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
		s.headers(w, req, start, end)
		w.(http.Flusher).Flush()
		<-req.Context().Done()
		return true
	}
	body := s.open(t, r)
	done := make(chan struct{})
	go func() { defer close(done); io.Copy(io.Discard, body) }()
	body.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Read did not unblock")
	}
}

func TestAllThreeAttemptsStall(t *testing.T) {
	r, _ := testReader(t)
	s := newTestOrigin(t, 2<<20)
	s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
		if start != 0 {
			return false
		}
		s.headers(w, req, start, end)
		w.(http.Flusher).Flush()
		<-req.Context().Done()
		return true
	}
	body := s.open(t, r)
	_, err := io.Copy(io.Discard, body)
	body.Close()
	if err == nil {
		t.Fatal("all stalled attempts returned success")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requests[0] != maxAttempts {
		t.Fatalf("attempts = %d", s.requests[0])
	}
	if s.peak.Load() > Concurrency+maxExtra {
		t.Fatal("request fanout exceeded the per-object limit")
	}
}

func TestBodyDeadlineReleasesIdleConsumer(t *testing.T) {
	r, budget := testReader(t)
	r.policy.timeout = 80 * time.Millisecond
	s := newTestOrigin(t, 2<<20)
	body := s.open(t, r)
	deadline := time.After(time.Second)
	for budget.used.Load() != 0 {
		select {
		case <-deadline:
			t.Fatal("body deadline retained range buffers without a Read or Close")
		case <-time.After(time.Millisecond):
		}
	}
	if _, err := body.Read(make([]byte, 1024)); err != context.DeadlineExceeded {
		t.Fatalf("read after deadline: %v", err)
	}
	body.Close()
}

func TestHedgeStreamsBeforeWholePart(t *testing.T) {
	r, _ := testReader(t)
	s := newTestOrigin(t, 2<<20)
	release := make(chan struct{})
	defer close(release)
	s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
		if start != 0 {
			return false
		}
		s.headers(w, req, start, end)
		w.(http.Flusher).Flush()
		if attempt == 1 {
			<-req.Context().Done()
			return true
		}
		w.Write(s.data[:8192])
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-req.Context().Done():
			return true
		}
		w.Write(s.data[8192 : end+1])
		return true
	}
	body := s.open(t, r)
	done := make(chan error, 1)
	go func() {
		prefix := make([]byte, 1024)
		_, err := io.ReadFull(body, prefix)
		if err == nil && !bytes.Equal(prefix, s.data[:1024]) {
			err = fmt.Errorf("wrong prefix")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("hedge waited for a whole range before producing bytes")
	}
	body.Close()
}

func TestConcurrentReadersRespectSharedAdmission(t *testing.T) {
	for _, memoryLimit := range []int64{12 * 64 << 10, 64 << 20} {
		t.Run(strconv.FormatInt(memoryLimit, 10), func(t *testing.T) {
			r, budget := testReader(t)
			r.policy.partSize = 64 << 10
			r.policy.hedgeDelay = time.Second
			budget.limit = memoryLimit
			s := newTestOrigin(t, 1<<20)
			s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
				s.headers(w, req, start, end)
				w.(http.Flusher).Flush()
				<-req.Context().Done()
				return true
			}
			var bodies []io.ReadCloser
			admitted := 0
			for range 20 {
				stream := s.open(t, r)
				bodies = append(bodies, stream)
				if _, ok := stream.(*body); ok {
					admitted++
				}
			}
			limit := min(16, int(memoryLimit/(6*64<<10)))
			if admitted != limit {
				t.Fatalf("admitted %d downloads, want %d", admitted, limit)
			}
			if budget.peak.Load() > memoryLimit {
				t.Fatal("memory budget exceeded")
			}
			var wg sync.WaitGroup
			for _, stream := range bodies {
				wg.Add(1)
				go func() { defer wg.Done(); stream.Close() }()
			}
			wg.Wait()
		})
	}
}

// Hash the entire output in every performance run so a fast partial response
// cannot masquerade as an improvement.
func checksumRead(body io.ReadCloser) ([32]byte, int64, error) {
	defer body.Close()
	h := sha256.New()
	n, err := io.Copy(h, body)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, n, err
}
