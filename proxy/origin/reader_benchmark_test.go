package origin

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tigrisdata/tag/metrics"
)

// BenchmarkOriginDownload uses the production policy and real HTTP bodies.
// The origin deliberately controls throughput; these are mechanism measurements,
// not estimates of R2 or of the deployed cluster's p95/p99.
func BenchmarkOriginDownload(b *testing.B) {
	for _, scenario := range []struct {
		name string
		size int
	}{
		{"healthy_10MiB", 10 << 20}, {"healthy_64MiB", 64 << 20}, {"slow_first_64MiB", 64 << 20},
		{"slow_middle_64MiB", 64 << 20}, {"stall_64MiB", 64 << 20}, {"shared_link_64MiB", 64 << 20},
		{"healthy_1GiB", 1 << 30},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			for _, enabled := range []bool{false, true} {
				name := "single"
				if enabled {
					name = "ranges"
				}
				b.Run(name, func(b *testing.B) {
					s := newTestOrigin(b, scenario.size)
					expected := sha256.Sum256(s.data)
					var linkMu sync.Mutex
					var linkNext time.Time
					s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
						s.headers(w, req, start, end)
						w.(http.Flusher).Flush()
						began := time.Now()
						if scenario.name == "stall_64MiB" && start == 0 && attempt == 1 {
							if !waitUntil(req.Context(), began.Add(8*time.Second)) {
								return true
							}
						}
						elapsed := time.Duration(0)
						for pos := start; pos <= end; {
							n := min(int64(256<<10), end-pos+1)
							rate := int64(32 << 20)
							if scenario.name == "healthy_1GiB" {
								rate = 64 << 20
							}
							if scenario.name == "slow_first_64MiB" && start == 0 && attempt == 1 {
								rate = 2 << 20
							}
							if scenario.name == "slow_middle_64MiB" && pos >= 32<<20 && pos < 48<<20 && attempt == 1 {
								rate = 2 << 20
							}
							elapsed += time.Duration(float64(n) / float64(rate) * float64(time.Second))
							until := began.Add(elapsed)
							if scenario.name == "shared_link_64MiB" {
								linkMu.Lock()
								if linkNext.Before(time.Now()) {
									linkNext = time.Now()
								}
								linkNext = linkNext.Add(time.Duration(float64(n) / float64(16<<20) * float64(time.Second)))
								until = linkNext
								linkMu.Unlock()
							}
							if !waitUntil(req.Context(), until) {
								return true
							}
							written, err := w.Write(s.data[pos : pos+n])
							s.sent.Add(int64(written))
							if err != nil {
								return true
							}
							pos += n
						}
						return true
					}
					budget := &testBudget{limit: 1 << 30}
					r := New(budget)
					ttfb := time.Duration(0)
					var received float64
					b.SetBytes(int64(scenario.size))
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						s.mu.Lock()
						clear(s.requests)
						s.mu.Unlock()
						before := testutil.ToFloat64(metrics.OriginRangeBytes.WithLabelValues("received"))
						started := time.Now()
						var body io.ReadCloser
						if enabled {
							body = s.open(b, r)
						} else {
							body = s.open(b, nil)
						}
						var first [1]byte
						if _, err := io.ReadFull(body, first[:]); err != nil {
							b.Fatal(err)
						}
						ttfb += time.Since(started)
						h := sha256.New()
						h.Write(first[:])
						n, err := io.Copy(h, body)
						body.Close()
						if err != nil || n+1 != int64(scenario.size) || string(h.Sum(nil)) != string(expected[:]) {
							b.Fatalf("incomplete or corrupt body: bytes=%d err=%v", n+1, err)
						}
						received += testutil.ToFloat64(metrics.OriginRangeBytes.WithLabelValues("received")) - before
						for s.active.Load() != 0 {
							if !waitUntil(context.Background(), time.Now().Add(time.Millisecond)) {
								break
							}
						}
					}
					b.StopTimer()
					if budget.used.Load() != 0 {
						b.Fatal("leaked range budget")
					}
					b.ReportMetric(float64(ttfb.Microseconds())/float64(b.N)/1000, "TTFB_ms")
					b.ReportMetric(float64(budget.peak.Load())/(1<<20), "reserved_MiB")
					b.ReportMetric(float64(s.sent.Load())/float64(b.N*scenario.size), "origin_sent_x")
					if received > 0 {
						b.ReportMetric(received/float64(b.N*scenario.size), "origin_read_x")
					}
				})
			}
		})
	}
}

func waitUntil(ctx context.Context, until time.Time) bool {
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// Five distinct objects match the reported pattern: four normal downloads and
// one slow initial request. All readers share the same pod admission budgets.
func BenchmarkOriginFiveDownloads(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "single"
		if enabled {
			name = "ranges"
		}
		b.Run(name, func(b *testing.B) {
			s := newTestOrigin(b, 64<<20)
			expected := sha256.Sum256(s.data)
			s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
				s.headers(w, req, start, end)
				w.(http.Flusher).Flush()
				rate := float64(32 << 20)
				if req.URL.Path == "/bucket/object-4" && req.Header.Get("Range") == "" {
					rate = 2 << 20
				}
				began := time.Now()
				for pos := start; pos <= end; {
					n := min(int64(256<<10), end-pos+1)
					if !waitUntil(req.Context(), began.Add(time.Duration(float64(pos-start+n)/rate*float64(time.Second)))) {
						return true
					}
					written, err := w.Write(s.data[pos : pos+n])
					s.sent.Add(int64(written))
					if err != nil {
						return true
					}
					pos += n
				}
				return true
			}
			budget := &testBudget{limit: 512 << 20}
			r := New(budget)
			var median, slowest float64
			b.SetBytes(5 * int64(len(s.data)))
			b.ResetTimer()
			for range b.N {
				type result struct {
					duration float64
					err      error
				}
				results := make(chan result, 5)
				for i := range 5 {
					go func() {
						started := time.Now()
						target := fmt.Sprintf("%s/bucket/object-%d", s.server.URL, i)
						req, _ := http.NewRequest("GET", target, nil)
						resp, err := s.client.Do(req)
						if err != nil {
							results <- result{err: err}
							return
						}
						if enabled {
							r.Wrap(req, resp, func(ctx context.Context, start, end int64, etag string, extra bool) (*http.Response, error) {
								get, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
								get.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
								get.Header.Set("If-Match", etag)
								return s.client.Do(get)
							})
						}
						sum, n, err := checksumRead(resp.Body)
						if err == nil && (sum != expected || n != int64(len(s.data))) {
							err = fmt.Errorf("invalid output length/checksum: %d", n)
						}
						results <- result{time.Since(started).Seconds(), err}
					}()
				}
				var durations []float64
				for range 5 {
					result := <-results
					if result.err != nil {
						b.Fatal(result.err)
					}
					durations = append(durations, result.duration)
				}
				sort.Float64s(durations)
				median += durations[2]
				slowest += durations[4]
			}
			b.StopTimer()
			if budget.used.Load() != 0 {
				b.Fatal("leaked reservation")
			}
			b.ReportMetric(median/float64(b.N), "median_s")
			b.ReportMetric(slowest/float64(b.N), "slowest_s")
			b.ReportMetric(float64(budget.peak.Load())/(1<<20), "reserved_MiB")
			b.ReportMetric(float64(s.sent.Load())/float64(b.N*5*len(s.data)), "origin_sent_x")
		})
	}
}
