package proxy

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func TestOriginRangesThroughForwardingAndCache(t *testing.T) {
	data := bytes.Repeat([]byte("range-data-"), 4<<20)
	store := auth.NewCredentialStore()
	store.AddCredential("access", "secret")
	validator := auth.NewRequestValidator(store)
	var ranges, full atomic.Int64
	var invalid atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := validator.Validate(r); err != nil {
			t.Errorf("upstream signature: %v", err)
			w.WriteHeader(403)
			return
		}
		if r.URL.Path != "/bucket/object" {
			t.Errorf("wrong object: %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		start, end := int64(0), int64(len(data)-1)
		w.Header().Set("ETag", `"test-version"`)
		w.Header().Set("Accept-Ranges", "bytes")
		status := 200
		if value := r.Header.Get("Range"); value != "" {
			ranges.Add(1)
			if invalid.Load() {
				w.Header().Set("ETag", `"replaced-version"`)
			}
			if r.Header.Get("If-Match") != `"test-version"` || r.Header.Get("If-None-Match") != "" {
				t.Error("range preconditions are wrong")
			}
			fmt.Sscanf(value, "bytes=%d-%d", &start, &end)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			status = 206
		} else {
			full.Add(1)
		}
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(status)
		w.Write(data[start : end+1])
	}))
	defer upstream.Close()
	for _, path := range []string{"cold", "invalid-range", "no-store", "presigned-bypass", "slow-client", "forward", "background", "revalidation", "disabled"} {
		t.Run(path, func(t *testing.T) {
			invalid.Store(path == "invalid-range")
			ranges.Store(0)
			full.Store(0)
			cfg := config.NewDefault()
			cfg.Upstream.ParallelDownloads = path != "disabled"
			cfg.Cache.SetBlockCachingEnabled(false)
			if path == "slow-client" {
				cfg.Broadcast.ChannelBuffer = 2
			}
			forwarder := NewForwarder(store, upstream.URL, "auto", 10, nil, nil)
			c := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
			s := NewService(forwarder, c, cfg)
			t.Cleanup(s.Stop)
			ctx := context.Background()
			var got []byte
			switch path {
			case "background":
				_, token, _, tokenErr := c.GetMetaWithVersion(ctx, "bucket", "object")
				if tokenErr != nil {
					t.Fatal(tokenErr)
				}
				if err := s.fetchFullObjectToCache(ctx, "bucket", "object", "access", "secret", false, priorityReadMiss, token); err != nil {
					t.Fatal(err)
				}
				var out bytes.Buffer
				meta, found, err := c.GetMeta(ctx, "bucket", "object")
				if err != nil || !found {
					t.Fatalf("cache missing: %v", err)
				}
				if err := c.GetBodyStream(ctx, "bucket", "object", meta.ETag, &out); err != nil {
					t.Fatal(err)
				}
				got = out.Bytes()
			case "revalidation":
				resp, err := forwarder.DoConditionalGetRequest(ctx, "bucket", "object", "access", "secret", `"old"`, 0, "")
				if err != nil {
					t.Fatal(err)
				}
				got, err = io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
			default:
				headers := http.Header{}
				if path == "no-store" {
					headers.Set("Cache-Control", "no-store")
				}
				req, err := auth.NewRequestSigner("http://tag", "auto").SignObjectRequest(ctx, "GET", "bucket", "object", nil, "UNSIGNED-PAYLOAD", "access", "secret", headers)
				if err != nil {
					t.Fatal(err)
				}
				if path == "presigned-bypass" {
					req, err = http.NewRequest("GET", "http://tag/bucket/object?X-Amz-Expires=900&X-Amz-Checksum-Mode=ENABLED", nil)
					if err != nil {
						t.Fatal(err)
					}
					signed, _, signErr := v4.NewSigner().PresignHTTP(ctx, aws.Credentials{AccessKeyID: "access", SecretAccessKey: "secret"}, req, "UNSIGNED-PAYLOAD", "s3", "auto", time.Now())
					if signErr != nil {
						t.Fatal(signErr)
					}
					req.URL, err = url.Parse(signed)
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
					req.Header.Set("Cache-Control", "no-cache")
				}
				w := httptest.NewRecorder()
				if path == "forward" {
					err = forwarder.Forward(ctx, w, req)
				} else {
					var writer http.ResponseWriter = w
					if path == "slow-client" {
						writer = &pacedRangeWriter{ResponseWriter: w}
					}
					err = s.HandleGetObject(writer, req)
				}
				if path == "invalid-range" {
					if err == nil {
						t.Fatal("invalid range completed successfully")
					}
					s.Stop()
					_, found, metaErr := c.GetMeta(ctx, "bucket", "object")
					if metaErr != nil || found {
						t.Fatalf("failed read published cache metadata: found=%v err=%v", found, metaErr)
					}
					c.Close()
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				got = w.Body.Bytes()
			}
			if !bytes.Equal(got, data) {
				t.Fatal("forwarded body differs")
			}
			if path == "presigned-bypass" {
				_, found, _ := c.GetMeta(ctx, "bucket", "object")
				if found {
					t.Fatal("presigned cache bypass populated the cache")
				}
			}
			if path != "disabled" && ranges.Load() != 2 {
				t.Fatalf("range GETs=%d, expected 2", ranges.Load())
			}
			if path == "disabled" && ranges.Load() != 0 {
				t.Fatal("disabled flag made range requests")
			}
			// Join asynchronous fills before checking budget release and warm hits.
			deadline := time.Now().Add(3 * time.Second)
			for path == "cold" {
				_, found, _ := c.GetMeta(ctx, "bucket", "object")
				if found {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("cold read was not cached")
				}
				<-time.After(time.Millisecond)
			}
			if path == "cold" || path == "background" {
				before := full.Load()
				req, _ := auth.NewRequestSigner("http://tag", "auto").SignObjectRequest(ctx, "GET", "bucket", "object", nil, "UNSIGNED-PAYLOAD", "access", "secret", nil)
				w := httptest.NewRecorder()
				if err := s.HandleGetObject(w, req); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(w.Body.Bytes(), data) || w.Header().Get("X-Cache") != "HIT" || full.Load() != before {
					t.Fatal("warm read used origin or changed bytes")
				}
			}
			s.Stop()
			c.Close()
		})
	}
}

func TestOriginRangeFactoryScope(t *testing.T) {
	b := newBaseForwarder("http://localhost", "auto", 10)
	b.configureOriginRanges(newByteBudget(1<<30, 0, 0))
	for _, query := range []string{"versionId=one", "partNumber=1", "response-content-type=text/plain", "list-type=2"} {
		req, _ := http.NewRequest("GET", "http://localhost/bucket/key?"+query, nil)
		if b.rangeFetch(req, "access", "secret") != nil {
			t.Errorf("unsafe synthetic ranges for %s", query)
		}
	}
	if b.httpClient.Transport == b.hedgeClient.Transport {
		t.Fatal("hedges share the primary transport")
	}
	if b.hedgeClient.Transport.(*http.Transport).MaxConnsPerHost != 16 {
		t.Fatal("unbounded hedge pool")
	}
}

// The delay models a consumer slower than the local origin, rather than test
// synchronization. Two queue slots make overflow deterministic without a GB fixture.
type pacedRangeWriter struct{ http.ResponseWriter }

func (w *pacedRangeWriter) Write(p []byte) (int, error) {
	<-time.After(100 * time.Microsecond)
	return w.ResponseWriter.Write(p)
}
