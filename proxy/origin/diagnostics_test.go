package origin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDiagnosticsDistinguishPartDeadlineAndBudget(t *testing.T) {
	r, _ := testReader(t)
	r.policy.hedgeDelay = time.Second
	r.policy.attemptTimeout = 100 * time.Millisecond
	r.traffic = 0
	s := newTestOrigin(t, 2<<20)
	s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
		if start != 0 {
			return false
		}
		s.headers(w, req, start, end)
		w.Write(s.data[:1024])
		w.(http.Flusher).Flush()
		<-req.Context().Done()
		return true
	}
	var mu sync.Mutex
	var events []Diagnostic
	ctx := WithDiagnostics(context.Background(), func(d Diagnostic) { mu.Lock(); events = append(events, d); mu.Unlock() })
	req, _ := http.NewRequestWithContext(ctx, "GET", s.server.URL+"/bucket/key", nil)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Wrap(req, resp, s.fetch)
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err == nil {
		t.Fatal("expected stalled read failure")
	}
	mu.Lock()
	defer mu.Unlock()
	deadline, denied := false, false
	for _, e := range events {
		if e.Start == 0 && e.Event == "attempt" && e.Cause == "attempt_deadline" {
			deadline = true
			if e.Bytes != 1024 || e.ContextError != "deadline_exceeded" || e.ParentError != "" || e.LongestBodyPauseMS < 75 {
				t.Fatalf("bad deadline evidence: %+v", e)
			}
		}
		if e.Start == 0 && e.Denials["process_extra_bytes"] > 0 {
			denied = true
		}
	}
	if !deadline || !denied {
		t.Fatalf("missing evidence deadline=%v denied=%v: %+v", deadline, denied, events)
	}
}

func TestDiagnosticsHealthyTransferAndPrivacy(t *testing.T) {
	r, _ := testReader(t)
	s := newTestOrigin(t, 2<<20)
	var mu sync.Mutex
	var events []Diagnostic
	ctx := WithDiagnostics(context.Background(), func(d Diagnostic) { mu.Lock(); events = append(events, d); mu.Unlock() })
	req, _ := http.NewRequestWithContext(ctx, "GET", s.server.URL+"/bucket/private-key?signature=secret", nil)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Wrap(req, resp, s.fetch)
	n, err := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil || n != int64(len(s.data)) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	mu.Lock()
	defer mu.Unlock()
	var total int64
	for _, d := range events {
		if d.Event == "attempt" {
			if d.Cause != "complete" {
				t.Fatalf("unexpected cause %+v", d)
			}
			total += d.Bytes
		}
	}
	if total != n {
		t.Fatalf("observed %d bytes, read %d", total, n)
	}
	data, _ := json.Marshal(events)
	if strings.Contains(string(data), "signature") || strings.Contains(string(data), "private-key") {
		t.Fatal("request identity leaked")
	}
	if len(data) == 0 {
		t.Fatal("no diagnostics")
	}
	if ErrorType(errors.New("https://private/path?signature=secret")) != "transport_or_validation_error" {
		t.Fatal("raw error leaked")
	}
	if ErrorType(net.ErrClosed) != "closed_connection" {
		t.Fatal("closed error misclassified")
	}
}

func TestDiagnosticsProgressBoundedAndOptIn(t *testing.T) {
	ctx := context.Background()
	next, tr := TraceAttempt(ctx, 0, 10, 1, false)
	if tr != nil || next != ctx {
		t.Fatal("disabled diagnostics changed context")
	}
	var got Diagnostic
	ctx = WithDiagnostics(ctx, func(d Diagnostic) { got = d })
	next, tr = TraceAttempt(ctx, 0, 100, 1, false)
	tr.Headers(nil, false)
	for i := 0; i < 100; i++ {
		tr.lastSample = time.Time{}
		tr.Progress(1)
	}
	tr.Finish(next, ctx, nil, "complete")
	if len(got.Progress) != 32 || got.Bytes != 100 || got.Progress[31].Bytes != 100 {
		t.Fatalf("bad bounded history %+v", got)
	}
}

func TestDiagnosticsProgressingAttemptStillHitsTotalDeadline(t *testing.T) {
	r, _ := testReader(t)
	r.policy.hedgeDelay = time.Second
	r.policy.attemptTimeout = 120 * time.Millisecond
	r.traffic = 0
	s := newTestOrigin(t, 2<<20)
	s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
		if start != 0 {
			return false
		}
		s.headers(w, req, start, end)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.Write(s.data[:1024])
				w.(http.Flusher).Flush()
			case <-req.Context().Done():
				return true
			}
		}
	}
	var mu sync.Mutex
	var seen *Diagnostic
	ctx := WithDiagnostics(context.Background(), func(d Diagnostic) {
		if d.Start == 0 && d.Cause == "attempt_deadline" {
			mu.Lock()
			seen = &d
			mu.Unlock()
		}
	})
	req, _ := http.NewRequestWithContext(ctx, "GET", s.server.URL+"/bucket/key", nil)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Wrap(req, resp, s.fetch)
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err == nil {
		t.Fatal("expected deadline")
	}
	mu.Lock()
	defer mu.Unlock()
	if seen == nil || seen.Bytes < 2048 || seen.LastProgressAgoMS > 100 {
		t.Fatalf("missing continuing progress before deadline: %+v", seen)
	}
}

func TestDiagnosticsIdentifyLosingHedge(t *testing.T) {
	r, _ := testReader(t)
	s := newTestOrigin(t, 2<<20)
	s.behavior = func(w http.ResponseWriter, req *http.Request, start, end int64, attempt int) bool {
		if start != 0 || attempt != 1 {
			return false
		}
		s.headers(w, req, start, end)
		w.Write(s.data[:1024])
		w.(http.Flusher).Flush()
		<-req.Context().Done()
		return true
	}
	var mu sync.Mutex
	var events []Diagnostic
	ctx := WithDiagnostics(context.Background(), func(d Diagnostic) { mu.Lock(); events = append(events, d); mu.Unlock() })
	req, _ := http.NewRequestWithContext(ctx, "GET", s.server.URL+"/bucket/key", nil)
	resp, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Wrap(req, resp, s.fetch)
	n, err := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil || n != int64(len(s.data)) {
		t.Fatalf("n=%d error=%v", n, err)
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, d := range events {
		if d.Start == 0 && d.Attempt == 1 && d.Cause == "hedge_lost" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing canceled loser: %+v", events)
	}
}
