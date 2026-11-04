// origin-diagnostics compares read-only origin paths without the cache or proxy.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/proxy/origin"
)

type config struct {
	Endpoint, Bucket string
	Keys             []string
}
type budget struct{}

func (budget) TryAcquire(n int64) bool { return n <= 128<<20 }
func (budget) Release(int64)           {}

type output struct {
	sync.Mutex
	encoder *json.Encoder
}

func (o *output) emit(v any) {
	o.Lock()
	defer o.Unlock()
	if err := o.encoder.Encode(v); err != nil {
		panic(err)
	}
}

type readResult struct {
	n    int64
	err  error
	data []byte
}

func main() {
	configPath := flag.String("config", "", "JSON configuration, without credentials")
	mode := flag.String("mode", "head", "head, full, ranges, or parallel")
	run := flag.String("run", "", "label for this observation")
	flag.Parse()
	raw, err := os.ReadFile(*configPath)
	if err != nil {
		panic("cannot read config")
	}
	var c config
	if json.Unmarshal(raw, &c) != nil || c.Endpoint == "" || len(c.Keys) != 1 {
		panic("config requires endpoint and exactly one key")
	}
	ak, sk := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if ak == "" || sk == "" {
		panic("missing storage credentials")
	}
	if *mode != "head" && *mode != "full" && *mode != "ranges" && *mode != "parallel" {
		panic("invalid mode")
	}
	out := &output{encoder: json.NewEncoder(os.Stdout)}
	emit := func(kind string, data any) {
		out.emit(map[string]any{"kind": kind, "run": *run, "mode": *mode, "at": time.Now().UTC(), "data": data})
	}
	signer := auth.NewRequestSigner(c.Endpoint, "auto")
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 10 * time.Second, ForceAttemptHTTP2: true, ResponseHeaderTimeout: 60 * time.Second, MaxIdleConns: 256, MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second, DisableCompression: true}
	client := &http.Client{Transport: transport}
	defer transport.CloseIdleConnections()
	extraTransport := transport.Clone()
	extraTransport.MaxConnsPerHost = 16
	extraTransport.MaxIdleConnsPerHost = 16
	extraTransport.MaxIdleConns = 16
	extraClient := &http.Client{Transport: extraTransport}
	defer extraTransport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = origin.WithDiagnostics(ctx, func(d origin.Diagnostic) { emit("diagnostic", d) })
	makeReq := func(ctx context.Context, method string, headers http.Header) (*http.Request, error) {
		return signer.SignObjectRequest(ctx, method, c.Bucket, c.Keys[0], nil, "UNSIGNED-PAYLOAD", ak, sk, headers)
	}
	head, err := makeReq(ctx, "HEAD", nil)
	if err != nil {
		panic("cannot create signed request")
	}
	resp, err := client.Do(head)
	if err != nil {
		emit("error", map[string]any{"stage": "head", "error_type": origin.ErrorType(err)})
		os.Exit(1)
	}
	resp.Body.Close()
	size := resp.ContentLength
	etag := resp.Header.Get("ETag")
	keyHash := sha256.Sum256([]byte(c.Keys[0]))
	emit("object", map[string]any{"key_hash": hex.EncodeToString(keyHash[:8]), "size": size, "etag": etag, "status": resp.StatusCode, "accept_ranges": resp.Header.Get("Accept-Ranges")})
	if resp.StatusCode != 200 || etag == "" {
		os.Exit(1)
	}
	if *mode == "head" {
		return
	}
	if size < 1_000_000_000 || size > 4<<30 {
		panic("test object must be 1 GB to 4 GiB")
	}
	baseline := tcpCounters()
	started := time.Now()
	h := sha256.New()
	var n int64
	responseInfo := func(r *http.Response, start, end int64, extra bool) {
		emit("origin_response", map[string]any{"start": start, "end": end, "extra": extra, "status": r.StatusCode, "protocol": r.Proto, "cf_ray": r.Header.Get("Cf-Ray"), "request_id": r.Header.Get("X-Amz-Request-Id")})
	}
	var fetch func(context.Context, int64, int64, string, bool) (*http.Response, error)
	fetch = func(ctx context.Context, start, end int64, tag string, extra bool) (*http.Response, error) {
		req, e := makeReq(ctx, "GET", http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", start, end)}, "If-Match": {tag}})
		if e != nil {
			return nil, e
		}
		cl := client
		if extra {
			cl = extraClient
		}
		r, e := cl.Do(req)
		if r != nil {
			responseInfo(r, start, end, extra)
		}
		return r, e
	}
	if *mode == "ranges" {
		n, err = readRanges(ctx, size, etag, fetch, h)
	} else {
		// This trace belongs only to the initial request; ranges receive fresh hooks.
		requestCtx, trace := origin.TraceAttempt(ctx, 0, size-1, 1, false)
		req, requestErr := makeReq(requestCtx, "GET", http.Header{"If-Match": {etag}})
		err = requestErr
		if err == nil {
			resp, err = client.Do(req)
		}
		if err == nil {
			responseInfo(resp, 0, size-1, false)
			trace.Headers(resp, false)
			if resp.StatusCode != 200 || resp.Header.Get("ETag") != etag || resp.ContentLength != size {
				err = errors.New("invalid full response")
				resp.Body.Close()
				trace.Finish(requestCtx, ctx, err, "read_or_validation_error")
			} else if *mode == "parallel" {
				trace.Finish(requestCtx, ctx, nil, "initial_headers_only")
				plainReq := req.WithContext(ctx)
				origin.New(budget{}).Wrap(plainReq, resp, fetch)
				n, err = io.CopyBuffer(h, resp.Body, make([]byte, 64<<10))
				resp.Body.Close()
			} else {
				n, err = copyMeasured(h, resp.Body, trace)
				resp.Body.Close()
				trace.Finish(requestCtx, ctx, err, cause(err, ctx))
			}
		} else {
			trace.Finish(requestCtx, ctx, err, cause(err, ctx))
		}
	}
	elapsed := time.Since(started).Seconds()
	after := tcpCounters()
	delta := map[string]int64{}
	for k, v := range after {
		delta[k] = v - baseline[k]
	}
	emit("result", map[string]any{"bytes": n, "expected_bytes": size, "seconds": elapsed, "MB_s": float64(n) / elapsed / 1e6, "sha256": hex.EncodeToString(h.Sum(nil)), "error_type": origin.ErrorType(err), "complete": err == nil && n == size, "pod_tcp_delta": delta})
	if err != nil || n != size {
		os.Exit(1)
	}
}
func cause(err error, ctx context.Context) string {
	if err == nil {
		return "complete"
	}
	if ctx.Err() != nil {
		return "request_" + origin.ErrorType(ctx.Err())
	}
	return "read_or_transport_error"
}
func copyMeasured(w io.Writer, r io.Reader, t *origin.AttemptTrace) (int64, error) {
	b := make([]byte, 64<<10)
	var n int64
	for {
		m, e := r.Read(b)
		if m > 0 {
			t.Progress(int64(m))
			z, we := w.Write(b[:m])
			n += int64(z)
			if we != nil {
				return n, we
			}
			if z != m {
				return n, io.ErrShortWrite
			}
		}
		if e != nil {
			if e == io.EOF {
				e = nil
			}
			return n, e
		}
	}
}
func readRanges(ctx context.Context, size int64, etag string, fetch origin.Fetch, h hash.Hash) (int64, error) {
	var total int64
	// Four parts at a time bound memory and keep the independent control simple.
	for start := int64(0); start < size; start += 4 * origin.PartSize {
		var wg sync.WaitGroup
		batch := make([]readResult, 4)
		for i := 0; i < 4; i++ {
			lo := start + int64(i)*origin.PartSize
			if lo >= size {
				continue
			}
			hi := min(size, lo+origin.PartSize) - 1
			wg.Add(1)
			go func(i int, lo, hi int64) {
				defer wg.Done()
				// A longer bounded deadline lets the control reveal reads beyond 30 seconds.
				partCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
				defer cancel()
				traceCtx, t := origin.TraceAttempt(partCtx, lo, hi, 1, false)
				r, e := fetch(traceCtx, lo, hi, etag, false)
				if e == nil {
					t.Headers(r, false)
					if r.StatusCode != 206 || r.ContentLength != hi-lo+1 || r.Header.Get("ETag") != etag || r.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", lo, hi, size) {
						e = errors.New("invalid range response")
					} else {
						b := make([]byte, hi-lo+1)
						var offset int
						for offset < len(b) {
							m, re := r.Body.Read(b[offset:min(len(b), offset+(64<<10))])
							offset += m
							t.Progress(int64(m))
							if re != nil {
								if re != io.EOF || offset != len(b) {
									e = re
									if re == io.EOF {
										e = io.ErrUnexpectedEOF
									}
								}
								break
							}
						}
						batch[i].data = b[:offset]
						batch[i].n = int64(offset)
					}
					r.Body.Close()
				}
				batch[i].err = e
				t.Finish(traceCtx, ctx, e, cause(e, partCtx))
			}(i, lo, hi)
		}
		wg.Wait()
		for _, r := range batch {
			if r.err != nil {
				return total, r.err
			}
			h.Write(r.data)
			total += r.n
		}
	}
	return total, nil
}
func tcpCounters() map[string]int64 {
	out := map[string]int64{}
	for _, path := range []string{"/proc/net/snmp", "/proc/net/netstat"} {
		raw, e := os.ReadFile(path)
		if e != nil {
			continue
		}
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		for i := 0; i+1 < len(lines); i += 2 {
			keys, vals := strings.Fields(lines[i]), strings.Fields(lines[i+1])
			if len(keys) != len(vals) {
				continue
			}
			for j := 1; j < len(keys); j++ {
				k := strings.TrimSuffix(keys[0], ":") + "_" + keys[j]
				if k == "Tcp_RetransSegs" || k == "Tcp_OutSegs" || k == "Tcp_InSegs" || k == "TcpExt_TCPTimeouts" || k == "TcpExt_TCPLostRetransmit" || k == "TcpExt_TCPSynRetrans" {
					v, _ := strconv.ParseInt(vals[j], 10, 64)
					out[k] = v
				}
			}
		}
	}
	return out
}
