package origin

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, value string
		want        time.Duration
	}{
		{"seconds", "2", 2 * time.Second},
		{"whitespace", " 2 \t", 2 * time.Second},
		{"date", now.Add(3 * time.Second).Format(http.TimeFormat), 3 * time.Second},
		{"duration overflow", "9223372037", time.Duration(math.MaxInt64)},
		{"integer overflow", "999999999999999999999999999999", time.Duration(math.MaxInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryDelay(tc.value, 1, now); got != tc.want {
				t.Fatalf("delay = %s, want %s", got, tc.want)
			}
		})
	}
	for _, value := range []string{"", "0", "-1", "+1", "1.5", "1, 2", "invalid", now.Add(-time.Hour).Format(http.TimeFormat)} {
		t.Run("backoff/"+value, func(t *testing.T) {
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				ceiling := time.Second << (attempt - 1)
				if got := retryDelay(value, attempt, now); got < ceiling/2 || got >= ceiling {
					t.Fatalf("attempt %d delay = %s, want [%s, %s)", attempt, got, ceiling/2, ceiling)
				}
			}
		})
	}
}

func TestRetryAfterPausesRetriesHedgesAndNewParts(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			r, _ := testReader(t)
			r.policy.attemptTimeout = 3 * time.Second
			r.policy.timeout = 6 * time.Second
			s := newTestOrigin(t, 2<<20)
			releaseInitial := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseInitial) }) }
			defer release()
			var notBefore atomic.Int64
			var early atomic.Int64
			s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
				if start == 0 && attempt == 1 {
					s.headers(w, req, start, end)
					w.(http.Flusher).Flush()
					select {
					case <-releaseInitial:
						w.Write(s.data[:r.policy.partSize])
					case <-req.Context().Done():
					}
					return true
				}
				if attempt > 1 || start >= Concurrency*r.policy.partSize {
					if deadline := notBefore.Load(); deadline != 0 && time.Now().UnixNano() < deadline {
						early.Add(1)
					}
				}
				if start == r.policy.partSize && attempt == 1 {
					deadline := time.Now().Add(time.Second)
					value := "1"
					if status == http.StatusServiceUnavailable {
						deadline = time.Now().Add(2 * time.Second).Truncate(time.Second)
						value = deadline.UTC().Format(http.TimeFormat)
					}
					notBefore.Store(deadline.UnixNano())
					w.Header().Set("Retry-After", value)
					w.WriteHeader(status)
					return true
				}
				if start == 2*r.policy.partSize && attempt == 1 {
					s.headers(w, req, start, end)
					w.Write(s.data[start : start+8192])
					w.(http.Flusher).Flush()
					<-req.Context().Done()
					return true
				}
				return false
			}
			stream := s.open(t, r)
			b := stream.(*body)
			waitForCooldown(t, b)
			release()
			got, err := io.ReadAll(stream)
			stream.Close()
			if err != nil || !bytes.Equal(got, s.data) {
				t.Fatalf("download did not recover intact: %v", err)
			}
			if n := early.Load(); n != 0 {
				t.Fatalf("%d retries, hedges or new parts bypassed Retry-After", n)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.requests[r.policy.partSize] != 2 || s.requests[2*r.policy.partSize] != 2 {
				t.Fatalf("expected one retry and one hedge: %v", s.requests)
			}
		})
	}
}

func TestRetryAfterCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "close"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			r, budget := testReader(t)
			if deadline {
				r.policy.timeout = 300 * time.Millisecond
			}
			s := newTestOrigin(t, 2<<20)
			s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
				if start == r.policy.partSize {
					w.Header().Set("Retry-After", "999999999999999999999999999999")
					w.WriteHeader(http.StatusTooManyRequests)
					return true
				}
				return false
			}
			stream := s.open(t, r)
			waitForCooldown(t, stream.(*body))
			done := make(chan error, 1)
			go func() { _, err := io.Copy(io.Discard, stream); done <- err }()
			if !deadline {
				stream.Close()
			}
			select {
			case err := <-done:
				want := error(context.Canceled)
				if deadline {
					want = context.DeadlineExceeded
				}
				if err != want {
					t.Fatalf("cancellation error = %v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("cooldown did not stop on cancellation")
			}
			stream.Close()
			if budget.used.Load() != 0 || len(r.slots) != 0 {
				t.Fatal("canceled cooldown retained memory or request slots")
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.requests[r.policy.partSize] != 1 {
				t.Fatal("retried before the server delay expired")
			}
		})
	}
}

func waitForCooldown(t *testing.T, b *body) {
	t.Helper()
	deadline := time.After(time.Second)
	for b.retryDelay() <= 0 {
		select {
		case <-deadline:
			t.Fatal("origin response did not start a cooldown")
		case <-time.After(time.Millisecond):
		}
	}
}
