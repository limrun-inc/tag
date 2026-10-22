package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
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

func TestPresignedCacheControlUsesOriginEvenWithWarmCache(t *testing.T) {
	for _, method := range []string{"GET", "HEAD"} {
		for _, status := range []int{200, 403, 404, 503} {
			t.Run(fmt.Sprintf("%s/%d", method, status), func(t *testing.T) {
				var calls atomic.Int32
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Host != "asset-storage-production.s3.example.test" || r.URL.Path != "/org_example/app.apk" || r.Header.Get("If-None-Match") != "" || r.Header.Get("Cache-Control") != "no-cache" {
						t.Errorf("request changed: host=%s path=%s headers=%v", r.Host, r.URL.Path, r.Header)
					}
					w.Header().Set("ETag", `"fresh"`)
					w.WriteHeader(status)
					if r.Method != "HEAD" {
						io.WriteString(w, "origin response")
					}
				}))
				defer origin.Close()
				r := compatPresignedGET(t, "https://asset-storage-production.s3.example.test/org_example/app.apk")
				if method == "HEAD" {
					// HEAD signatures require a content hash in upstream local auth.
					r, _ = http.NewRequest(method, r.URL.Scheme+"://"+r.Host+r.URL.Path+"?X-Amz-Expires=900", nil)
					r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
					signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), aws.Credentials{AccessKeyID: compatAccess, SecretAccessKey: compatSecret}, r, "UNSIGNED-PAYLOAD", "s3", compatRegion, time.Now().UTC())
					if err != nil {
						t.Fatal(err)
					}
					r.URL, _ = url.Parse(signed)
					r.Header.Set("Cache-Control", "no-cache")
				}
				f := compatLearnedForwarder(t, origin.URL, r, "asset-storage-production")
				svc, store := newTestService(f, true)
				defer svc.Stop()
				meta := &cache.CachedObjectMeta{Bucket: "asset-storage-production", Key: "org_example/app.apk", ETag: `"old"`, ContentLength: 10, StatusCode: 200}
				if err := store.PutWithMeta(t.Context(), meta.Bucket, meta.Key, meta, []byte("cached app"), 60); err != nil {
					t.Fatal(err)
				}
				w := httptest.NewRecorder()
				var err error
				if method == "HEAD" {
					err = svc.HandleHeadObject(w, r)
				} else {
					err = svc.HandleGetObject(w, r)
				}
				if err != nil {
					t.Fatal(err)
				}
				if w.Code != status || w.Header().Get(XCacheHeader) != XCacheBypass || calls.Load() != 1 {
					t.Fatalf("status=%d cache=%s calls=%d", w.Code, w.Header().Get(XCacheHeader), calls.Load())
				}
				if method == "GET" && w.Body.String() != "origin response" {
					t.Fatalf("served stale app: %q", w.Body.String())
				}
				current, found, err := store.GetMeta(t.Context(), meta.Bucket, meta.Key)
				if err != nil || !found || current.ETag != meta.ETag {
					t.Fatalf("bypass replaced cached entry: %+v, found=%v err=%v", current, found, err)
				}
			})
		}
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
