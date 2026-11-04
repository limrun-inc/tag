package origin

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"
)

// Diagnostic contains no object names, URLs, credentials, or raw transport errors.
// Timings are milliseconds from attempt start. Missing phases remain absent.
type Diagnostic struct {
	Event              string             `json:"event"`
	Start              int64              `json:"range_start"`
	End                int64              `json:"range_end"`
	Attempt            int                `json:"attempt"`
	Extra              bool               `json:"extra"`
	InitialStream      bool               `json:"initial_stream,omitempty"`
	Cause              string             `json:"cause"`
	ErrorType          string             `json:"error_type,omitempty"`
	ContextError       string             `json:"context_error,omitempty"`
	ParentError        string             `json:"parent_error,omitempty"`
	ElapsedMS          float64            `json:"elapsed_ms"`
	Bytes              int64              `json:"bytes"`
	LastProgressAgoMS  float64            `json:"last_progress_ago_ms"`
	LongestBodyPauseMS float64            `json:"longest_body_pause_ms"`
	Status             int                `json:"status,omitempty"`
	Remote             string             `json:"remote,omitempty"`
	Reused             bool               `json:"connection_reused"`
	Phases             map[string]float64 `json:"phases_ms,omitempty"`
	Progress           []ProgressSample   `json:"progress,omitempty"`
	Denials            map[string]int     `json:"denials,omitempty"`
}

type ProgressSample struct {
	ElapsedMS float64 `json:"elapsed_ms"`
	Bytes     int64   `json:"bytes"`
}

type diagnosticKey struct{}
type diagnosticObserver func(Diagnostic)

// WithDiagnostics opts this request into attempt tracing. The observer must be
// concurrency-safe and return promptly. It is called once per finished attempt
// and once per part with denied launches. Without an observer, no HTTP hooks
// or progress history are allocated.
func WithDiagnostics(ctx context.Context, observer func(Diagnostic)) context.Context {
	return context.WithValue(ctx, diagnosticKey{}, diagnosticObserver(observer))
}

// AttemptTrace also lets diagnostic clients measure direct reads identically.
// Progress and Finish belong to the read goroutine; HTTP hooks use their own lock.
type AttemptTrace struct {
	report       diagnosticObserver
	started      time.Time
	mu           sync.Mutex
	d            Diagnostic
	lastProgress time.Time
	lastSample   time.Time
	history      [32]ProgressSample
	next, count  int
}

func TraceAttempt(ctx context.Context, start, end int64, attempt int, extra bool) (context.Context, *AttemptTrace) {
	report, _ := ctx.Value(diagnosticKey{}).(diagnosticObserver)
	if report == nil {
		return ctx, nil
	}
	t := &AttemptTrace{report: report, started: time.Now(), d: Diagnostic{Event: "attempt", Start: start, End: end, Attempt: attempt, Extra: extra, Phases: make(map[string]float64)}}
	mark := func(phase string) {
		t.mu.Lock()
		t.d.Phases[phase] = milliseconds(time.Since(t.started))
		t.mu.Unlock()
	}
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { mark("get_connection") },
		GotConn: func(i httptrace.GotConnInfo) {
			t.mu.Lock()
			t.d.Remote = i.Conn.RemoteAddr().String()
			t.d.Reused = i.Reused
			t.d.Phases["got_connection"] = milliseconds(time.Since(t.started))
			t.mu.Unlock()
		},
		DNSStart: func(httptrace.DNSStartInfo) { mark("dns_start") }, DNSDone: func(httptrace.DNSDoneInfo) { mark("dns_done") },
		ConnectStart: func(string, string) { mark("connect_start") }, ConnectDone: func(string, string, error) { mark("connect_done") },
		TLSHandshakeStart: func() { mark("tls_start") }, TLSHandshakeDone: func(tls.ConnectionState, error) { mark("tls_done") },
		WroteRequest:         func(httptrace.WroteRequestInfo) { mark("request_written") },
		GotFirstResponseByte: func() { mark("first_response_header_byte") },
	}
	return httptrace.WithClientTrace(ctx, trace), t
}

func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (t *AttemptTrace) Headers(resp *http.Response, initial bool) {
	if t == nil {
		return
	}
	t.d.InitialStream = initial
	if resp != nil {
		t.d.Status = resp.StatusCode
	}
	t.lastProgress = time.Now()
	t.mu.Lock()
	t.d.Phases["headers_available"] = milliseconds(time.Since(t.started))
	t.mu.Unlock()
}

func (t *AttemptTrace) Progress(n int64) {
	if t == nil || n <= 0 {
		return
	}
	now := time.Now()
	if !t.lastProgress.IsZero() {
		t.d.LongestBodyPauseMS = max(t.d.LongestBodyPauseMS, milliseconds(now.Sub(t.lastProgress)))
	}
	t.d.Bytes += n
	t.lastProgress = now
	if t.lastSample.IsZero() || now.Sub(t.lastSample) >= time.Second {
		t.sample(now)
	}
}

func (t *AttemptTrace) sample(now time.Time) {
	t.history[t.next] = ProgressSample{milliseconds(now.Sub(t.started)), t.d.Bytes}
	t.next = (t.next + 1) % len(t.history)
	t.count = min(t.count+1, len(t.history))
	t.lastSample = now
}

func (t *AttemptTrace) Finish(ctx, parent context.Context, err error, cause string) {
	if t == nil {
		return
	}
	now := time.Now()
	t.d.Event = "attempt"
	t.d.Cause, t.d.ErrorType = cause, ErrorType(err)
	t.d.ContextError, t.d.ParentError = ErrorType(ctx.Err()), ErrorType(parent.Err())
	t.d.ElapsedMS = milliseconds(now.Sub(t.started))
	if !t.lastProgress.IsZero() {
		t.d.LastProgressAgoMS = milliseconds(now.Sub(t.lastProgress))
		t.d.LongestBodyPauseMS = max(t.d.LongestBodyPauseMS, t.d.LastProgressAgoMS)
	}
	t.sample(now)
	for i := 0; i < t.count; i++ {
		t.d.Progress = append(t.d.Progress, t.history[(t.next-t.count+i+len(t.history))%len(t.history)])
	}
	// Hooks may finish after cancellation. Copy their fields under the lock.
	t.mu.Lock()
	d := t.d
	d.Phases = make(map[string]float64, len(t.d.Phases))
	for k, v := range t.d.Phases {
		d.Phases[k] = v
	}
	t.mu.Unlock()
	t.report(d)
}

// ErrorType deliberately avoids err.Error(), which can contain a signed URL.
func ErrorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, net.ErrClosed):
		return "closed_connection"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		return "transport_or_validation_error"
	}
}

func (p *part) diagnosticCause(ctx context.Context, err error) string {
	if err == nil {
		return "complete"
	}
	if p.body.parent.Err() != nil {
		return "request_" + ErrorType(p.body.parent.Err())
	}
	if errors.Is(p.body.ctx.Err(), context.DeadlineExceeded) {
		return "body_deadline"
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "attempt_deadline"
	}
	p.mu.Lock()
	complete := p.complete
	p.mu.Unlock()
	if complete && p.ctx.Err() != nil {
		return "hedge_lost"
	}
	if p.ctx.Err() != nil {
		return "part_or_body_canceled"
	}
	return "read_or_validation_error"
}

func (p *part) reportDenials(denials map[string]int) {
	if len(denials) == 0 {
		return
	}
	if report, _ := p.ctx.Value(diagnosticKey{}).(diagnosticObserver); report != nil {
		report(Diagnostic{Event: "launch_denied", Start: p.start, End: p.end, Denials: denials})
	}
}
