package handlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestSlowResponseOutlivesTheWriteBudget pins that a response is bounded by stalling rather than by
// how long it runs.
//
// The server's write budget is a total: it starts when the request arrives and expires whether or
// not the response is making progress. A build-cache archive is over a gigabyte and the link it
// crosses is not always fast, so a healthy transfer routinely outruns any fixed budget and the
// connection is reset mid-body. The client then refetches from origin, which is the cost the cache
// exists to avoid.
func TestSlowResponseOutlivesTheWriteBudget(t *testing.T) {
	const chunks = 10
	const pause = 50 * time.Millisecond

	handler := withProgressDeadlines(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < chunks; i++ {
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(pause)
		}
	}), time.Second)

	srv := httptest.NewUnstartedServer(handler)
	// Far shorter than the whole response takes, the way a five-minute budget is shorter than a
	// multi-gigabyte restore over a slow link.
	srv.Config.WriteTimeout = 100 * time.Millisecond
	srv.Start()
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read the body: %v (the connection was cut mid-response)", err)
	}
	if len(body) != chunks {
		t.Errorf("got %d bytes, want %d: the response was truncated", len(body), chunks)
	}
}

// TestBodylessResponseOutlivesTheWindow pins that a response outlives the stall window itself when
// there is no request body to renew anything.
//
// The server reads the connection in the background to notice a client hanging up. A GET has no
// body, so nothing on the read side ever renews its deadline; when it expires the background read
// fails, the server takes the client for gone, and the request context is cancelled mid-response.
// Writing is progress too, so it has to renew both.
func TestBodylessResponseOutlivesTheWindow(t *testing.T) {
	const window = 200 * time.Millisecond
	const chunks = 10
	const pause = 50 * time.Millisecond // 500ms total, well past the window

	handler := withProgressDeadlines(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < chunks; i++ {
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(pause)
		}
	}), window)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read the body: %v (the read deadline cancelled the request mid-response)", err)
	}
	if len(body) != chunks {
		t.Errorf("got %d bytes, want %d: the response was truncated", len(body), chunks)
	}
}

// TestDrippingClientKeepsItsResponse is the shape a rate-limited restore has: the server blocks on
// writes because the client takes the bytes slowly, and the whole transfer runs many windows long.
func TestDrippingClientKeepsItsResponse(t *testing.T) {
	// Sized like production: a chunk drains in a fraction of the window, while the whole transfer
	// runs for several windows.
	const window = time.Second
	const chunk = 64 << 10
	const chunks = 64

	served := make(chan error, 1)
	handler := withProgressDeadlines(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := make([]byte, chunk)
		for i := 0; i < chunks; i++ {
			if _, err := w.Write(payload); err != nil {
				served <- err
				return
			}
		}
		served <- nil
	}), window)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	// Drip: slow enough that the transfer spans several windows, never so slow that it stalls.
	var total int
	buf := make([]byte, 4<<10)
	for {
		n, err := res.Body.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read after %d bytes: %v (the response was cut mid-body)", total, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := <-served; err != nil {
		t.Errorf("the handler could not finish writing: %v", err)
	}
	if total != chunk*chunks {
		t.Errorf("got %d bytes, want %d", total, chunk*chunks)
	}
}

// TestSlowUploadOutlivesTheReadBudget is the same guarantee for the other direction: publishing an
// archive through the gateway takes as long as it takes.
func TestSlowUploadOutlivesTheReadBudget(t *testing.T) {
	const chunks = 10
	const pause = 50 * time.Millisecond

	handler := withProgressDeadlines(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if n != chunks {
			http.Error(w, "short body", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}), time.Second)

	srv := httptest.NewUnstartedServer(handler)
	srv.Config.ReadTimeout = 100 * time.Millisecond
	srv.Start()
	defer srv.Close()

	// A body that trickles: every chunk arrives well inside the window, but the whole upload takes
	// several times the budget.
	pr, pw := io.Pipe()
	go func() {
		for i := 0; i < chunks; i++ {
			if _, err := io.WriteString(pw, "x"); err != nil {
				return
			}
			time.Sleep(pause)
		}
		_ = pw.Close()
	}()

	req, err := http.NewRequest(http.MethodPut, srv.URL, pr)
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v (the upload was cut mid-body)", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(res.Body)
		t.Errorf("status = %d, want 200: %s", res.StatusCode, strings.TrimSpace(string(msg)))
	}
}

// TestStalledResponseIsStillCut keeps the other half of the bargain: a client that stops reading
// loses its connection, so a wedged transfer cannot hold a slot forever.
func TestStalledResponseIsStillCut(t *testing.T) {
	writeErr := make(chan error, 1)
	handler := withProgressDeadlines(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Far more than any socket buffer, so the writes block on a client that never reads.
		big := strings.Repeat("x", 1<<20)
		for i := 0; i < 64; i++ {
			if _, err := w.Write([]byte(big)); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}), 200*time.Millisecond)

	srv := httptest.NewUnstartedServer(handler)
	srv.Config.WriteTimeout = time.Hour // only the stall window can end this
	srv.Start()
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	select {
	case err := <-writeErr:
		if err == nil {
			t.Error("the whole response was written to a client that never read it")
		}
	case <-time.After(30 * time.Second):
		t.Error("a stalled response was never cut")
	}
}
