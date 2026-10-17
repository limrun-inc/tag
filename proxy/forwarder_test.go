package proxy

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tigrisdata/tag/metrics"
)

type forwarderTestTransport func(*http.Request) (*http.Response, error)

func (f forwarderTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestExecuteAndStreamReturningMetaOwnsHeaders(t *testing.T) {
	upstreamHeaders := make(http.Header)
	upstreamHeaders.Set("ETag", `"upstream-etag"`)
	upstreamHeaders["X-Upstream-Metadata"] = []string{"first", "second"}

	forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     upstreamHeaders,
			Body:       io.NopCloser(strings.NewReader("response body")),
		}, nil
	})}

	writer := httptest.NewRecorder()
	request, err := http.NewRequest(http.MethodPut, "https://upstream.example.com/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	status, headers, err := forwarder.executeAndStreamReturningMeta(writer, request, 0, nil)
	if err != nil {
		t.Fatalf("executeAndStreamReturningMeta: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if writer.Body.String() != "response body" {
		t.Errorf("client body = %q, want response body", writer.Body.String())
	}
	if writer.Header().Get("ETag") != `"upstream-etag"` {
		t.Errorf("client ETag = %q, want upstream ETag", writer.Header().Get("ETag"))
	}
	if headers.Get("ETag") != `"upstream-etag"` {
		t.Fatalf("captured ETag = %q, want upstream ETag", headers.Get("ETag"))
	}
	if got := headers.Values("X-Upstream-Metadata"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("captured metadata = %q, want [first second]", got)
	}

	upstreamHeaders.Set("ETag", `"changed-upstream-etag"`)
	upstreamHeaders["X-Upstream-Metadata"][0] = "changed"
	if headers.Get("ETag") != `"upstream-etag"` {
		t.Errorf("captured ETag changed with upstream map: %q", headers.Get("ETag"))
	}
	if got := headers.Values("X-Upstream-Metadata"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("captured metadata changed with upstream slice: %q", got)
	}

	headers.Set("ETag", `"changed-captured-etag"`)
	if upstreamHeaders.Get("ETag") != `"changed-upstream-etag"` {
		t.Errorf("upstream ETag changed with captured map: %q", upstreamHeaders.Get("ETag"))
	}
}

// failingWriter accepts headers and fails every body write, like a client
// whose connection timed out.
type failingWriter struct {
	header http.Header
}

func (w *failingWriter) Header() http.Header { return w.header }
func (w *failingWriter) WriteHeader(int)     {}
func (w *failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write: connection timed out")
}

// A cut transfer still reports success because the headers already went out,
// so the copy-failure counter is its only record and must name the right side.
func TestExecuteAndStreamCountsCutTransfersBySide(t *testing.T) {
	tests := map[string]struct {
		body   io.Reader
		writer http.ResponseWriter
		side   string
	}{
		"UpstreamReset": {
			body:   &errAfterReader{data: []byte("partial")},
			writer: httptest.NewRecorder(),
			side:   "upstream",
		},
		"ClientGone": {
			body:   strings.NewReader("response body"),
			writer: &failingWriter{header: make(http.Header)},
			side:   "client",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			forwarder := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
			forwarder.httpClient = &http.Client{Transport: forwarderTestTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(tt.body)}, nil
			})}
			request, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
			if err != nil {
				t.Fatal(err)
			}
			before := map[string]float64{
				"upstream": testutil.ToFloat64(metrics.BodyCopyFailures.WithLabelValues("upstream")),
				"client":   testutil.ToFloat64(metrics.BodyCopyFailures.WithLabelValues("client")),
			}

			if err := forwarder.executeAndStream(tt.writer, request, 0, nil); err != nil {
				t.Fatalf("executeAndStream: %v", err)
			}

			for side, count := range before {
				want := count
				if side == tt.side {
					want++
				}
				if got := testutil.ToFloat64(metrics.BodyCopyFailures.WithLabelValues(side)); got != want {
					t.Errorf("%s copy failures = %v, want %v", side, got, want)
				}
			}
		})
	}
}

func TestIsSlowTransfer(t *testing.T) {
	tests := map[string]struct {
		bytes   int64
		elapsed time.Duration
		want    bool
	}{
		"QuickSmallDownload":         {bytes: 10 << 20, elapsed: time.Second, want: false},
		"LargeArchiveAtHealthySpeed": {bytes: 5 << 30, elapsed: 100 * time.Second, want: false},
		"TrickleUnderThreshold":      {bytes: 6 << 20, elapsed: 90 * time.Second, want: true},
		"ShortEvenIfSlow":            {bytes: 1 << 20, elapsed: 5 * time.Second, want: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := isSlowTransfer(tt.bytes, tt.elapsed); got != tt.want {
				t.Errorf("isSlowTransfer(%d, %s) = %t, want %t", tt.bytes, tt.elapsed, got, tt.want)
			}
		})
	}
}

// Against a real connection the trace hook fires; the body must still stream intact.
func TestExecuteAndStreamOverRealConnection(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "response body")
	}))
	t.Cleanup(upstream.Close)
	forwarder := newBaseForwarder(upstream.URL, "us-east-1", 1)
	request, err := http.NewRequest(http.MethodGet, upstream.URL+"/bucket/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	writer := httptest.NewRecorder()

	if err := forwarder.executeAndStream(writer, request, 0, nil); err != nil {
		t.Fatalf("executeAndStream: %v", err)
	}
	if writer.Body.String() != "response body" {
		t.Errorf("client body = %q, want response body", writer.Body.String())
	}
}
