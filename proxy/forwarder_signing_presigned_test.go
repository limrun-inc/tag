package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"sync/atomic"
)

func signingPresign(t *testing.T, method, target string, body []byte, headers http.Header, at time.Time, expires string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, target, bytes.NewReader(body))
	require.NoError(t, err)
	// Match S3's URI serializer before applying its single-escaped SigV4 signer.
	req.URL.RawPath = strings.ReplaceAll(req.URL.EscapedPath(), "+", "%2B")
	req.Header = headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	q := req.URL.Query()
	q.Set("X-Amz-Expires", expires)
	req.URL.RawQuery = q.Encode()
	signed, _, err := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableHeaderHoisting = true; o.DisableURIPathEscaping = true }).PresignHTTP(t.Context(), aws.Credentials{AccessKeyID: compatAccess, SecretAccessKey: compatSecret}, req, "UNSIGNED-PAYLOAD", "s3", "client-region", at)
	require.NoError(t, err)
	req.URL, err = url.Parse(signed)
	require.NoError(t, err)
	return req
}

// Verify with the AWS SDK so the origin does not share TAG's signing code.
func verifyPresignedWithSDK(r *http.Request) error {
	q := r.URL.Query()
	at, err := time.Parse(auth.TimeFormat, q.Get("X-Amz-Date"))
	if err != nil {
		return err
	}
	signature := q.Get("X-Amz-Signature")
	info, err := auth.ParseAuthInfo(r)
	if err != nil {
		return err
	}
	req := r.Clone(r.Context())
	req.URL.Scheme = "http"
	req.URL.Host = r.Host
	req.RequestURI = ""
	req.Header = make(http.Header)
	for _, name := range info.SignedHeaders {
		if name != "host" && name != "content-length" {
			req.Header[name] = r.Header.Values(name)
		}
	}
	for _, name := range []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-SignedHeaders", "X-Amz-Signature"} {
		q.Del(name)
	}
	req.URL.RawQuery = q.Encode()
	signed, _, err := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableHeaderHoisting = true; o.DisableURIPathEscaping = true }).PresignHTTP(r.Context(), aws.Credentials{AccessKeyID: compatAccess, SecretAccessKey: compatSecret}, req, "UNSIGNED-PAYLOAD", "s3", compatRegion, at)
	if err != nil {
		return err
	}
	u, err := url.Parse(signed)
	if err != nil {
		return err
	}
	if u.Query().Get("X-Amz-Signature") != signature {
		return fmt.Errorf("upstream signature differs from AWS SDK")
	}
	return nil
}

func TestSigningForwarderPresigned(t *testing.T) {
	for _, host := range []string{"gateway.example", "bucket.0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com", "bucket.t3.tigrisfiles.io"} {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut} {
			for _, path := range []string{"forward", "capture", "credentials", "tee"} {
				if path == "tee" && method != http.MethodPut {
					continue
				}
				t.Run(host+"/"+method+"/"+path, func(t *testing.T) {
					at := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
					var upload []byte
					origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if err := verifyPresignedWithSDK(r); err != nil {
							t.Error(err)
							w.WriteHeader(403)
							return
						}
						require.Empty(t, r.Header.Get("Authorization"))
						require.Equal(t, at.Format(auth.TimeFormat), r.URL.Query().Get("X-Amz-Date"))
						require.Equal(t, "600", r.URL.Query().Get("X-Amz-Expires"))
						require.Equal(t, "/bucket/a?b+c% d/日本語", r.URL.Path)
						require.Equal(t, "selected-version", r.URL.Query().Get("versionId"))
						if method == http.MethodPut {
							upload, _ = io.ReadAll(r.Body)
							require.Equal(t, "signed-checksum", r.Header.Get("X-Amz-Checksum-Sha256"))
						}
						w.Header().Set("ETag", `"object"`)
						if method == http.MethodGet {
							io.WriteString(w, "download")
						}
					}))
					defer origin.Close()
					creds := auth.NewCredentialStore()
					loadScopedPresigner(t, creds)
					f := NewForwarder(creds, origin.URL, compatRegion, 1, nil, nil).(*signingForwarder)
					var body []byte
					headers := make(http.Header)
					if method == http.MethodPut {
						body = []byte("uploaded body")
						headers.Set("X-Amz-Checksum-Sha256", "signed-checksum")
					}
					u := &url.URL{Scheme: "http", Host: host, Path: "/bucket/a?b+c% d/日本語", RawQuery: "versionId=selected-version"}
					if host != "gateway.example" {
						u.Path = strings.TrimPrefix(u.Path, "/bucket")
					}
					r := signingPresign(t, method, u.String(), body, headers, at, "600")
					original := r.URL.String()
					require.Empty(t, r.Header.Get("X-Amz-Content-Sha256"))
					result, access, secret, err := f.ValidateAndGetCredentials(r)
					require.NoError(t, err)
					require.Equal(t, AuthValidated, result)
					w := httptest.NewRecorder()
					switch path {
					case "forward":
						err = f.Forward(t.Context(), w, r)
					case "capture":
						_, err = f.ForwardWithCapture(t.Context(), w, r)
					case "credentials":
						var resp *http.Response
						resp, err = f.DoRequestWithCreds(t.Context(), r, access, secret)
						if err == nil {
							require.Equal(t, 200, resp.StatusCode)
							resp.Body.Close()
						}
					case "tee":
						var tee bytes.Buffer
						var status int
						status, _, _, _, err = f.ForwardTeeingBody(t.Context(), w, r, &tee)
						require.Equal(t, 200, status)
						require.Equal(t, body, tee.Bytes())
					}
					require.NoError(t, err)
					require.Equal(t, 200, w.Code)
					require.Equal(t, original, r.URL.String())
					require.Empty(t, r.Header.Get("X-Amz-Content-Sha256"))
					if method == http.MethodPut {
						require.Equal(t, body, upload)
					}
				})
			}
		}
	}
}

// Keep the gateway key different from the asset presigner, as in production.
func loadScopedPresigner(t *testing.T, creds *auth.CredentialStore) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "gateway")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "gateway-secret")
	path := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("TAG_SIGNING_CREDENTIALS_FILE", path)
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`[{"access_key_id":%q,"secret_access_key":%q,"buckets":["bucket"]}]`, compatAccess, compatSecret)), 0600))
	require.NoError(t, creds.LoadFromEnv())
}

func TestSigningScopedPresignerCannotReadAnotherBucketsCache(t *testing.T) {
	for _, hostStyle := range []bool{false, true} {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut} {
			t.Run(fmt.Sprintf("virtualHost=%t/%s", hostStyle, method), func(t *testing.T) {
				var calls atomic.Int32
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(403) }))
				defer origin.Close()
				creds := auth.NewCredentialStore()
				loadScopedPresigner(t, creds)
				f := NewForwarder(creds, origin.URL, compatRegion, 1, nil, nil)
				svc, store := newTestService(f, true)
				defer svc.Stop()
				for _, bucket := range []string{"bucket", "private-build-cache"} {
					meta := &cache.CachedObjectMeta{Bucket: bucket, Key: "object", ETag: `"version"`, ContentLength: 4, StatusCode: 200}
					require.NoError(t, store.PutWithMeta(t.Context(), bucket, "object", meta, []byte("data"), 60))
					target := "http://gateway.example/" + bucket + "/object"
					if hostStyle {
						target = "http://" + bucket + ".0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/object"
					}
					r := signingPresign(t, method, target, nil, nil, time.Now(), "300")
					w := httptest.NewRecorder()
					var err error
					switch method {
					case http.MethodGet:
						err = svc.HandleGetObject(w, r)
					case http.MethodHead:
						err = svc.HandleHeadObject(w, r)
					case http.MethodPut:
						_, _, _, err = f.ValidateAndGetCredentials(r)
					}
					if bucket == "bucket" {
						require.NoError(t, err)
						if method != http.MethodPut {
							require.Equal(t, XCacheHit, w.Header().Get(XCacheHeader))
						}
					} else {
						require.ErrorIs(t, err, auth.ErrBucketAccessDenied)
						var authErr *AuthError
						require.ErrorAs(t, err, &authErr)
						require.Equal(t, ErrCodeAccessDenied, authErr.Code)
						require.Empty(t, w.Body.String())
					}
				}
				require.Zero(t, calls.Load(), "bucket scope must be checked before cache and upstream")
			})
		}
	}
}

func TestSigningForwarderPresignedRejectsInvalidRequests(t *testing.T) {
	creds := auth.NewCredentialStore()
	creds.AddCredential(compatAccess, compatSecret)
	f := NewForwarder(creds, "http://origin.example", compatRegion, 1, nil, nil)
	now := time.Now().UTC().Truncate(time.Second)
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut} {
		for _, tc := range []struct {
			name, expiry string
			at           time.Time
			mutate       func(*http.Request)
		}{
			{name: "expired", expiry: "1", at: now.Add(-time.Minute)},
			{name: "future", expiry: "300", at: now.Add(time.Hour)},
			{name: "invalid expiry", expiry: "not-an-integer", at: now},
			{name: "too long", expiry: "604801", at: now},
			{name: "wrong host", at: now, mutate: func(r *http.Request) { r.Host = "attacker.example" }},
			{name: "wrong path", at: now, mutate: func(r *http.Request) { r.URL.Path = "/bucket/another-object" }},
			{name: "wrong method", at: now, mutate: func(r *http.Request) { r.Method = http.MethodDelete }},
			{name: "signature", at: now, mutate: func(r *http.Request) {
				q := r.URL.Query()
				q.Set("X-Amz-Signature", strings.Repeat("0", 64))
				r.URL.RawQuery = q.Encode()
			}},
			{name: "session token", at: now, mutate: func(r *http.Request) { r.Header.Set("X-Amz-Security-Token", "token") }},
			{name: "dual auth", at: now, mutate: func(r *http.Request) { r.Header.Set("Authorization", "AWS4-HMAC-SHA256 anything") }},
			{name: "duplicate auth", at: now, mutate: func(r *http.Request) {
				q := r.URL.Query()
				q.Add("X-Amz-Credential", q.Get("X-Amz-Credential"))
				r.URL.RawQuery = q.Encode()
			}},
		} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				expiry := tc.expiry
				if expiry == "" {
					expiry = "300"
				}
				r := signingPresign(t, method, "http://gateway.example/bucket/object", nil, nil, tc.at, expiry)
				if tc.mutate != nil {
					tc.mutate(r)
				}
				result, _, _, err := f.ValidateAndGetCredentials(r)
				require.Error(t, err)
				require.Equal(t, AuthNotValidated, result)
			})
		}
	}
	t.Run("header auth still requires hash", func(t *testing.T) {
		signer := auth.NewRequestSigner("http://gateway.example", compatRegion)
		r, err := signer.SignRequest(context.Background(), http.MethodGet, "/bucket/object", nil, "", compatAccess, compatSecret, nil)
		require.NoError(t, err)
		r.Header.Del("X-Amz-Content-Sha256")
		_, _, _, err = f.ValidateAndGetCredentials(r)
		require.ErrorIs(t, err, auth.ErrMissingContentHash)
	})
}

func TestSigningPresignedCacheEligibility(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		headers     http.Header
		bypass      bool
	}{
		{name: "plain"},
		{name: "SDK checksum", query: "X-Amz-Checksum-Mode=ENABLED&x-id=GetObject"},
		{name: "version", query: "versionId=old", bypass: true},
		{name: "part", query: "partNumber=1", bypass: true},
		{name: "response override", query: "response-content-type=text/plain", bypass: true},
		{name: "unknown parameter", query: "custom=1", bypass: true},
		{name: "wrong operation", query: "x-id=HeadObject", bypass: true},
		{name: "no-cache", headers: http.Header{"Cache-Control": {"no-cache"}}, bypass: true},
		{name: "conditional", headers: http.Header{"If-Match": {`"version"`}}, bypass: true},
		{name: "signed range", headers: http.Header{"Range": {"bytes=0-9"}}, bypass: true},
		{name: "encryption", headers: http.Header{"X-Amz-Server-Side-Encryption-Customer-Key": {"key"}}, bypass: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := signingPresign(t, http.MethodGet, "http://gateway.example/bucket/object?"+tc.query, nil, tc.headers, time.Now(), "300")
			require.Equal(t, tc.bypass, bypassPresignedCache(r))
		})
	}
}

func TestSigningPresignedEmptyPUT(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, verifyPresignedWithSDK(r))
		require.Zero(t, r.ContentLength)
		require.Empty(t, r.TransferEncoding)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Empty(t, body)
	}))
	defer origin.Close()
	creds := auth.NewCredentialStore()
	creds.AddCredential(compatAccess, compatSecret)
	f := NewForwarder(creds, origin.URL, compatRegion, 1, nil, nil).(*signingForwarder)
	r := signingPresign(t, http.MethodPut, "http://gateway.example/bucket/empty", nil, nil, time.Now(), "300")
	var tee bytes.Buffer
	status, _, _, _, err := f.ForwardTeeingBody(t.Context(), httptest.NewRecorder(), r, &tee)
	require.NoError(t, err)
	require.Equal(t, 200, status)
	require.Empty(t, tee.Bytes())
}

func TestSigningPresignedChecksumReadRefreshesOrdinaryCacheEntry(t *testing.T) {
	for _, checksum := range []string{"", "checksum-from-origin"} {
		t.Run("checksum="+checksum, func(t *testing.T) {
			var calls atomic.Int32
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if err := verifyPresignedWithSDK(r); err != nil {
					t.Error(err)
					w.WriteHeader(403)
					return
				}
				w.Header().Set("ETag", `"version"`)
				w.Header().Set("Content-Length", "4")
				if checksum != "" {
					w.Header().Set("X-Amz-Checksum-Sha256", checksum)
				}
				io.WriteString(w, "data")
			}))
			defer origin.Close()
			creds := auth.NewCredentialStore()
			creds.AddCredential(compatAccess, compatSecret)
			f := NewForwarder(creds, origin.URL, compatRegion, 1, nil, nil)
			svc, store := newTestService(f, true)
			defer svc.Stop()
			meta := &cache.CachedObjectMeta{Bucket: "bucket", Key: "object", ETag: `"version"`, ContentLength: 4, StatusCode: 200}
			require.NoError(t, store.PutWithMeta(t.Context(), "bucket", "object", meta, []byte("data"), 60))
			r := signingPresign(t, http.MethodGet, "http://gateway.example/bucket/object?X-Amz-Checksum-Mode=ENABLED", nil, nil, time.Now(), "300")
			cold := httptest.NewRecorder()
			require.NoError(t, svc.HandleGetObject(cold, r.Clone(t.Context())))
			require.Equal(t, XCacheMiss, cold.Header().Get(XCacheHeader))
			require.Eventually(t, func() bool {
				meta, found, err := store.GetMeta(t.Context(), "bucket", "object")
				return err == nil && found && meta.ChecksumsRequested
			}, time.Second, time.Millisecond)
			warm := httptest.NewRecorder()
			require.NoError(t, svc.HandleGetObject(warm, r.Clone(t.Context())))
			require.Equal(t, XCacheHit, warm.Header().Get(XCacheHeader))
			require.Equal(t, checksum, warm.Header().Get("X-Amz-Checksum-Sha256"))
			require.Equal(t, "data", warm.Body.String())
			require.EqualValues(t, 1, calls.Load())
		})
	}
}
