package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
)

const compatAccess = "LIMRUN_AUDIT_FAKE_ACCESS"
const compatSecret = "limrun-audit-fake-secret-not-a-real-credential"
const compatRegion = "auto"

func compatPresignedGET(t *testing.T, rawURL string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	q := r.URL.Query()
	q.Set("X-Amz-Expires", "900")
	q.Set("X-Amz-Checksum-Mode", "ENABLED")
	r.URL.RawQuery = q.Encode()
	signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), aws.Credentials{AccessKeyID: compatAccess, SecretAccessKey: compatSecret}, r, "UNSIGNED-PAYLOAD", "s3", compatRegion, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	r.URL, err = url.Parse(signed)
	if err != nil {
		t.Fatal(err)
	}
	r.Host = r.URL.Host
	r.Header.Set("Cache-Control", "no-cache")
	return r
}

func compatLearnedForwarder(t *testing.T, endpoint string, r *http.Request, bucket string) *transparentForwarder {
	t.Helper()
	creds := auth.NewCredentialStore()
	creds.AddCredential(compatAccess, compatSecret)
	info, err := auth.ParseAuthInfo(r)
	if err != nil {
		t.Fatal(err)
	}
	date := info.Date
	key, err := creds.GetSigningKey(compatAccess, date, compatRegion)
	if err != nil {
		t.Fatal(err)
	}
	derived := auth.NewDerivedKeyStore(time.Hour)
	derived.Store(compatAccess, date, compatRegion, key)
	grants := auth.NewAuthzCache(time.Minute)
	grants.Grant(info.AccessKey, bucket)
	f := NewForwarder(creds, endpoint, compatRegion, 1, auth.NewProxySigner(compatAccess, compatSecret), &LocalAuthConfig{DerivedKeyStore: derived, Validator: auth.NewRequestValidator(derived), AuthzCache: grants}).(*transparentForwarder)
	if result, _, _, err := f.ValidateAndGetCredentials(r); err != nil || result != AuthValidated {
		t.Fatalf("fixture must pass real local auth: result=%v err=%v", result, err)
	}
	return f
}

func presignedRevalidationRequest(t *testing.T, method string) *http.Request {
	t.Helper()
	r := compatPresignedGET(t, "https://asset-storage-production.s3.example.test/org_example/app.apk")
	if method == http.MethodHead {
		r, _ = http.NewRequest(method, r.URL.Scheme+"://"+r.Host+r.URL.Path+"?X-Amz-Expires=900", nil)
		r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), aws.Credentials{AccessKeyID: compatAccess, SecretAccessKey: compatSecret}, r, "UNSIGNED-PAYLOAD", "s3", compatRegion, time.Now().UTC())
		require.NoError(t, err)
		r.URL, _ = url.Parse(signed)
	}
	r.Header.Set("Cache-Control", "no-cache")
	return r
}

func presignedRevalidationForwarder(t *testing.T, mode, endpoint string, r *http.Request) RequestForwarder {
	t.Helper()
	if mode == "transparent" {
		return compatLearnedForwarder(t, endpoint, r, "asset-storage-production")
	}
	creds := auth.NewCredentialStore()
	creds.AddCredential(compatAccess, compatSecret)
	return NewForwarder(creds, endpoint, compatRegion, 1, nil, nil)
}

func TestPresignedCacheControlRevalidatesWarmCache(t *testing.T) {
	for _, mode := range []string{"signing", "transparent"} {
		for _, method := range []string{"GET", "HEAD"} {
			for _, status := range []int{200, 304, 403, 404, 410, 500, 503} {
				t.Run(fmt.Sprintf("%s/%s/%d", mode, method, status), func(t *testing.T) {
					var calls atomic.Int32
					original := presignedRevalidationRequest(t, method)
					origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						require.NoError(t, verifyPresignedWithSDK(r))
						require.Equal(t, original.URL.Query().Get("X-Amz-Date"), r.URL.Query().Get("X-Amz-Date"))
						require.Equal(t, original.URL.Query().Get("X-Amz-Expires"), r.URL.Query().Get("X-Amz-Expires"))
						require.Equal(t, original.URL.Query().Get("X-Amz-Checksum-Mode"), r.URL.Query().Get("X-Amz-Checksum-Mode"))
						require.Equal(t, `"old"`, r.Header.Get("If-None-Match"))
						require.Equal(t, "no-cache", r.Header.Get("Cache-Control"))
						require.Empty(t, r.Header.Get("If-Modified-Since"))
						require.Empty(t, r.Header.Get("Authorization"))
						if status == 304 {
							w.Header().Set("ETag", `"old"`)
						} else {
							w.Header().Set("ETag", `"fresh"`)
							w.Header().Set("Content-Length", "15")
						}
						w.WriteHeader(status)
						if r.Method != "HEAD" && status != 304 {
							io.WriteString(w, "origin response")
						}
					}))
					defer origin.Close()
					svc, store := newTestService(presignedRevalidationForwarder(t, mode, origin.URL, original), true)
					defer svc.Stop()
					meta := &cache.CachedObjectMeta{Bucket: "asset-storage-production", Key: "org_example/app.apk", ETag: `"old"`, ContentLength: 10, StatusCode: 200, ChecksumsRequested: true}
					require.NoError(t, store.PutWithMeta(t.Context(), meta.Bucket, meta.Key, meta, []byte("cached app"), 60))
					w := httptest.NewRecorder()
					if method == "GET" {
						require.NoError(t, svc.HandleGetObject(w, original))
					} else {
						require.NoError(t, svc.HandleHeadObject(w, original))
					}
					require.EqualValues(t, 1, calls.Load())
					wantStatus, wantCache := status, XCacheRevalidated
					if status == 304 {
						wantStatus, wantCache = 200, XCacheHit
					}
					require.Equal(t, wantStatus, w.Code)
					require.Equal(t, wantCache, w.Header().Get(XCacheHeader))
					if method == "GET" {
						expected := "origin response"
						if status == 304 {
							expected = "cached app"
						}
						require.Equal(t, expected, w.Body.String())
					} else {
						require.Empty(t, w.Body.String())
					}
					current, found, err := store.GetMeta(t.Context(), meta.Bucket, meta.Key)
					require.NoError(t, err)
					switch {
					case status == 404 || status == 410 || status == 200 && method == "HEAD":
						require.False(t, found)
					case status == 200:
						require.True(t, found)
						require.Equal(t, `"fresh"`, current.ETag)
						require.True(t, current.ChecksumsRequested)
					default:
						require.True(t, found)
						require.Equal(t, meta.ETag, current.ETag)
					}
				})
			}
		}
	}
}

func TestPresignedCacheControlColdWarmAndOverwrite(t *testing.T) {
	for _, mode := range []string{"signing", "transparent"} {
		for _, blocks := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/blocks=%t", mode, blocks), func(t *testing.T) {
				var version atomic.Int32
				version.Store(1)
				var calls, bodies atomic.Int32
				r := presignedRevalidationRequest(t, "GET")
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					calls.Add(1)
					require.NoError(t, verifyPresignedWithSDK(req))
					etag := fmt.Sprintf(`"v%d"`, version.Load())
					w.Header().Set("ETag", etag)
					if req.Header.Get("If-None-Match") == etag {
						w.WriteHeader(304)
						return
					}
					bodies.Add(1)
					body := fmt.Sprintf("app archive version %d", version.Load())
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					io.WriteString(w, body)
				}))
				defer origin.Close()
				svc, store := newTestService(presignedRevalidationForwarder(t, mode, origin.URL, r), true)
				defer svc.Stop()
				svc.config.Cache.SetBlockCachingEnabled(blocks)
				svc.config.Cache.BlockSize = 4
				for _, step := range []struct {
					version     int32
					cache       string
					rangeHeader string
				}{{1, XCacheMiss, ""}, {1, XCacheHit, ""}, {1, XCacheHit, "bytes=4-10"}, {2, XCacheRevalidated, ""}, {2, XCacheHit, ""}} {
					version.Store(step.version)
					req := r.Clone(t.Context())
					req.Header.Set("Range", step.rangeHeader)
					w := httptest.NewRecorder()
					require.NoError(t, svc.HandleGetObject(w, req))
					expected := fmt.Sprintf("app archive version %d", step.version)
					status := 200
					if step.rangeHeader != "" {
						expected, status = expected[4:11], 206
					}
					require.Equal(t, status, w.Code)
					require.Equal(t, expected, w.Body.String())
					require.Equal(t, step.cache, w.Header().Get(XCacheHeader))
					require.Eventually(t, func() bool {
						meta, found, err := store.GetMeta(t.Context(), "asset-storage-production", "org_example/app.apk")
						return err == nil && found && meta.ETag == fmt.Sprintf(`"v%d"`, step.version) && meta.ChecksumsRequested && (meta.BlockSize > 0) == blocks
					}, time.Second, time.Millisecond)
				}
				require.EqualValues(t, 5, calls.Load(), "every request must check origin")
				require.EqualValues(t, 2, bodies.Load(), "only the cold and changed objects transfer an upstream body")
			})
		}
	}
}

func TestPresignedCacheControlTransportFailureNeverServesStale(t *testing.T) {
	for _, method := range []string{"GET", "HEAD"} {
		t.Run(method, func(t *testing.T) {
			failure := fmt.Errorf("origin unavailable")
			f := &mockForwarder{doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
				return nil, failure
			}}
			svc, store := newTestService(f, true)
			defer svc.Stop()
			r := presignedRevalidationRequest(t, method)
			meta := &cache.CachedObjectMeta{Bucket: "asset-storage-production", Key: "org_example/app.apk", ETag: `"old"`, ContentLength: 10, StatusCode: 200, ChecksumsRequested: true}
			require.NoError(t, store.PutWithMeta(t.Context(), meta.Bucket, meta.Key, meta, []byte("cached app"), 60))
			w := httptest.NewRecorder()
			var err error
			if method == "GET" {
				err = svc.HandleGetObject(w, r)
			} else {
				err = svc.HandleHeadObject(w, r)
			}
			require.ErrorIs(t, err, failure)
			require.Empty(t, w.Body.String())
			require.Empty(t, w.Header().Get(XCacheHeader))
		})
	}
}

func TestPresignedCacheControlDoesNotJoinAnotherCapability(t *testing.T) {
	r := presignedRevalidationRequest(t, "GET")
	var calls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(403)
		io.WriteString(w, "revoked")
	}))
	defer origin.Close()
	svc, _ := newTestService(presignedRevalidationForwarder(t, "signing", origin.URL, r), true)
	defer svc.Stop()
	key := makeBroadcastKey("asset-storage-production", "org_example/app.apk", "") + "\x00checksum"
	// A different request already owns this object's fetch. Freshness requests
	// must still reach origin and observe their own authorization result.
	_, first := svc.broadcastManager.GetOrCreate(key)
	require.True(t, first)
	defer svc.broadcastManager.Remove(key)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	require.NoError(t, svc.HandleGetObject(w, r.Clone(ctx)))
	require.Equal(t, 403, w.Code)
	require.Equal(t, "revoked", w.Body.String())
	require.EqualValues(t, 1, calls.Load())
}

func TestPresignedCacheControlEligibility(t *testing.T) {
	for _, tc := range []struct {
		values             []string
		bypass, revalidate bool
	}{
		{[]string{"no-cache"}, false, true},
		{[]string{" MAX-AGE=0 "}, false, true},
		{[]string{"No-Cache", "max-age=0"}, false, true},
		{[]string{"no-cache, no-store"}, true, true},
		{[]string{"no-cache", "NO-STORE"}, true, true},
		{[]string{"private"}, true, false},
		{[]string{"max-age=0, private"}, true, true},
		{[]string{"max-age=0, unknown"}, true, true},
		{[]string{"x-no-cache"}, true, false},
	} {
		t.Run(strings.Join(tc.values, "/"), func(t *testing.T) {
			r := presignedRevalidationRequest(t, "GET")
			r.Header["Cache-Control"] = tc.values
			require.Equal(t, tc.bypass, bypassPresignedCache(r))
			require.Equal(t, tc.revalidate, shouldForceRevalidate(r))
		})
	}
}

func TestPresignedCacheControl304CannotReuseEntry(t *testing.T) {
	for _, reason := range []string{"body evicted", "no-store", "different etag"} {
		t.Run(reason, func(t *testing.T) {
			r := presignedRevalidationRequest(t, "GET")
			var calls atomic.Int32
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				require.NoError(t, verifyPresignedWithSDK(req))
				if req.Header.Get("If-None-Match") != "" {
					switch reason {
					case "no-store":
						w.Header().Set("Cache-Control", "NO-STORE")
					case "different etag":
						w.Header().Set("ETag", `"different"`)
					}
					w.WriteHeader(304)
					return
				}
				w.Header().Set("Content-Length", "5")
				io.WriteString(w, "fresh")
			}))
			defer origin.Close()
			svc, store := newTestService(presignedRevalidationForwarder(t, "signing", origin.URL, r), true)
			defer svc.Stop()
			meta := &cache.CachedObjectMeta{Bucket: "asset-storage-production", Key: "org_example/app.apk", ETag: `"old"`, ContentLength: 10, StatusCode: 200, ChecksumsRequested: true}
			require.NoError(t, store.PutWithMeta(t.Context(), meta.Bucket, meta.Key, meta, []byte("cached app"), 60))
			if reason == "body evicted" {
				require.NoError(t, store.DeleteBody(t.Context(), meta.Bucket, meta.Key, meta.ETag))
			}
			w := httptest.NewRecorder()
			require.NoError(t, svc.HandleGetObject(w, r))
			require.Equal(t, 200, w.Code)
			require.Equal(t, "fresh", w.Body.String())
			require.Equal(t, XCacheMiss, w.Header().Get(XCacheHeader))
			require.EqualValues(t, 2, calls.Load())
			_, found, err := store.GetMeta(t.Context(), meta.Bucket, meta.Key)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestPresignedVirtualHostCachesUnderBucketAndFullKey(t *testing.T) {
	var calls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("ETag", `"app-version"`)
		w.Header().Set("Content-Length", "11")
		io.WriteString(w, "app archive")
	}))
	defer origin.Close()
	r := compatPresignedGET(t, "https://asset-storage-production.s3.example.test/org_example/app.apk")
	r.Header.Del("Cache-Control")
	f := compatLearnedForwarder(t, origin.URL, r, "asset-storage-production")
	svc, store := newTestService(f, true)
	defer svc.Stop()
	cold := httptest.NewRecorder()
	if err := svc.HandleGetObject(cold, r.Clone(t.Context())); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, found, err := store.GetMeta(t.Context(), "asset-storage-production", "org_example/app.apk"); err == nil && found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cold request did not cache the correct bucket/key")
		}
		time.Sleep(time.Millisecond)
	}
	warm := httptest.NewRecorder()
	if err := svc.HandleGetObject(warm, r.Clone(t.Context())); err != nil {
		t.Fatal(err)
	}
	if warm.Header().Get(XCacheHeader) != XCacheHit || warm.Body.String() != "app archive" || calls.Load() != 1 {
		t.Fatalf("warm request: cache=%s body=%q upstream_calls=%d", warm.Header().Get(XCacheHeader), warm.Body.String(), calls.Load())
	}
}

func TestSignedBuildCacheRevalidationAndVerification(t *testing.T) {
	for _, tc := range []struct {
		method string
		status int
	}{{"GET", 304}, {"HEAD", 200}, {"HEAD", 404}} {
		t.Run(fmt.Sprintf("%s_origin_%d", tc.method, tc.status), func(t *testing.T) {
			var calls atomic.Int32
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/limrun-build-cache/org/archive.tar" {
					t.Errorf("wrong origin path: %s", r.URL.Path)
				}
				if tc.method == "HEAD" && r.Header.Get("If-None-Match") != "" {
					t.Error("verification HEAD unexpectedly used cached ETag")
				}
				if tc.status == 200 {
					w.Header().Set("X-Amz-Meta-Sha256", "new-digest")
					w.Header().Set("Content-Length", "19")
				}
				w.WriteHeader(tc.status)
			}))
			defer origin.Close()
			r, _ := http.NewRequest(tc.method, "https://t3.storage.dev/limrun-build-cache/org/archive.tar", nil)
			r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			if err := v4.NewSigner().SignHTTP(t.Context(), aws.Credentials{AccessKeyID: compatAccess, SecretAccessKey: compatSecret}, r, "UNSIGNED-PAYLOAD", "s3", compatRegion, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Cache-Control", "no-cache")
			if tc.method == "HEAD" {
				r.Header.Set("Cache-Control", "no-store")
			}
			f := compatLearnedForwarder(t, origin.URL, r, "limrun-build-cache")
			svc, c := newTestService(f, true)
			defer svc.Stop()
			body := []byte("old archive")
			meta := &cache.CachedObjectMeta{Bucket: "limrun-build-cache", Key: "org/archive.tar", ETag: `"old-etag"`, ContentLength: int64(len(body)), StatusCode: 200, UserMetadata: map[string]string{"x-amz-meta-sha256": "old-digest"}}
			if err := c.PutWithMeta(t.Context(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
				t.Fatal(err)
			}
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				if r.Method == "HEAD" {
					err = svc.HandleHeadObject(w, r)
				} else {
					err = svc.HandleGetObject(w, r)
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadGateway)
				}
			}))
			defer gateway.Close()
			gatewayURL, _ := r.URL.Parse(gateway.URL)
			r.URL.Scheme, r.URL.Host = gatewayURL.Scheme, gatewayURL.Host
			response, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			gotBody, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatalf("expected one origin request; got %d", calls.Load())
			}
			if tc.method == "HEAD" {
				if response.StatusCode != tc.status {
					t.Fatalf("verification HEAD did not preserve origin response: status=%d", response.StatusCode)
				}
				if tc.status == 200 && response.Header.Get("X-Amz-Meta-Sha256") != "new-digest" {
					t.Fatal("verification HEAD returned cached metadata")
				}
				return
			}
			if tc.status == 304 {
				if response.StatusCode != 200 || string(gotBody) != string(body) || response.Header.Get("X-Amz-Meta-Sha256") != "old-digest" {
					t.Fatalf("successful revalidation lost body or metadata: status=%d", response.StatusCode)
				}

			}
		})
	}
}
