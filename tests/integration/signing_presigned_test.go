package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
)

func TestSigningPresignedSDKUploadAndCachedReads(t *testing.T) {
	for _, virtualHost := range []bool{false, true} {
		for _, warmOnWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("virtual-host=%t/%s", virtualHost, map[bool]string{false: "invalidate", true: "write-through"}[warmOnWrite]), func(t *testing.T) {
				backend := s3mem.New()
				faker := gofakes3.New(backend).Server()
				store := auth.NewCredentialStore()
				store.AddCredential(TestAccessKey, TestSecretKey)
				validator := auth.NewRequestValidator(store)
				payload := []byte("presigned upload through the actual TAG router")
				digest := sha256.Sum256(payload)
				checksum := base64.StdEncoding.EncodeToString(digest[:])
				env := NewTestEnvironmentWithCacheHandler(func(w http.ResponseWriter, r *http.Request) {
					checked := r.Clone(r.Context())
					if auth.IsPresignedRequest(r) {
						if r.Header.Get("Authorization") != "" {
							t.Error("forwarded both authentication forms")
							w.WriteHeader(403)
							return
						}
						if checked.Header.Get("X-Amz-Content-Sha256") == "" {
							checked.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
						}
					}
					if _, err := validator.ValidateRequest(checked); err != nil {
						t.Errorf("origin rejected signature: %v", err)
						w.WriteHeader(403)
						return
					}
					if r.Method == http.MethodGet || r.Method == http.MethodHead {
						w.Header().Set("X-Amz-Checksum-Sha256", checksum)
					}
					faker.ServeHTTP(w, r)
				})
				defer env.Close()
				env.S3Backend = backend
				env.Config.Cache.WarmOnWrite = warmOnWrite
				bucket := fmt.Sprintf("presigned-%t-%x", warmOnWrite, time.Now().UnixNano())
				key := "folder/a?b+c% d.txt"
				require.NoError(t, env.PutTestObject(bucket, key, []byte("old bytes")))
				// Use the object API for keys containing query-reserved characters.
				client := s3.NewFromConfig(aws.Config{Region: TestRegion, Credentials: credentials.NewStaticCredentialsProvider(TestAccessKey, TestSecretKey, "")}, func(o *s3.Options) { o.BaseEndpoint = aws.String(env.TAGServer.URL); o.UsePathStyle = true })
				presigner := s3.NewPresignClient(client)
				if virtualHost {
					presigner = s3.NewPresignClient(s3.NewFromConfig(aws.Config{Region: TestRegion, Credentials: credentials.NewStaticCredentialsProvider(TestAccessKey, TestSecretKey, "")}, func(o *s3.Options) {
						o.BaseEndpoint = aws.String("https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com")
					}))
				}
				initial, err := presigner.PresignGetObject(t.Context(), &s3.GetObjectInput{Bucket: &bucket, Key: &key})
				require.NoError(t, err)
				requestFromPresign := func(method, target string, headers http.Header, body []byte) *http.Request {
					r, err := http.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
					require.NoError(t, err)
					r.Header = headers.Clone()
					if virtualHost {
						// Replay the production client's origin rewrite, retaining the signed Host.
						gateway, err := url.Parse(env.TAGServer.URL)
						require.NoError(t, err)
						r.Host = r.URL.Host
						r.URL.Scheme, r.URL.Host = gateway.Scheme, gateway.Host
					}
					return r
				}
				status, _, body := performLocalAuthRequestForTest(t, requestFromPresign(http.MethodGet, initial.URL, initial.SignedHeader, nil))
				require.Equal(t, 200, status)
				require.Equal(t, []byte("old bytes"), body)
				_, found := waitForReadableCachedObjectForLocalAuthTest(t, env, bucket, key, 2*time.Second)
				require.True(t, found)

				put, err := presigner.PresignPutObject(t.Context(), &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(payload), ContentType: aws.String("application/octet-stream"), ChecksumSHA256: &checksum})
				require.NoError(t, err)
				upload := requestFromPresign(http.MethodPut, put.URL, put.SignedHeader, payload)
				status, _, body = performLocalAuthRequestForTest(t, upload)
				require.Equal(t, 200, status, string(body))

				get, err := presigner.PresignGetObject(t.Context(), &s3.GetObjectInput{Bucket: &bucket, Key: &key, ChecksumMode: types.ChecksumModeEnabled})
				require.NoError(t, err)
				r := requestFromPresign(http.MethodGet, get.URL, get.SignedHeader, nil)
				status, _, body = performLocalAuthRequestForTest(t, r)
				require.Equal(t, 200, status, string(body))
				require.Equal(t, payload, body)
				_, found = waitForReadableCachedObjectForLocalAuthTest(t, env, bucket, key, 2*time.Second)
				require.True(t, found)
				before := env.GetUpstreamRequestCount()
				resp, err := http.DefaultClient.Do(r.Clone(context.Background()))
				require.NoError(t, err)
				downloaded, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				require.NoError(t, err)
				require.Equal(t, 200, resp.StatusCode)
				require.Equal(t, "HIT", resp.Header.Get("X-Cache"))
				require.Equal(t, checksum, resp.Header.Get("X-Amz-Checksum-Sha256"))
				require.Equal(t, payload, downloaded)
				require.Equal(t, before, env.GetUpstreamRequestCount())

				head, err := presigner.PresignHeadObject(t.Context(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
				require.NoError(t, err)
				status, xCache, body := performLocalAuthRequestForTest(t, requestFromPresign(http.MethodHead, head.URL, head.SignedHeader, nil))
				require.Equal(t, 200, status)
				require.Equal(t, "HIT", xCache)
				require.Empty(t, body)
				require.Equal(t, before, env.GetUpstreamRequestCount())

				tampered := requestFromPresign(http.MethodGet, get.URL, get.SignedHeader, nil)
				q := tampered.URL.Query()
				q.Set("X-Amz-Signature", strings.Repeat("0", 64))
				tampered.URL.RawQuery = q.Encode()
				status, xCache, _ = performLocalAuthRequestForTest(t, tampered)
				require.Equal(t, 403, status)
				require.NotEqual(t, "HIT", xCache)
				require.Equal(t, before, env.GetUpstreamRequestCount())

				// A query override keeps its semantics and cannot replace the cached object.
				override, err := presigner.PresignGetObject(t.Context(), &s3.GetObjectInput{Bucket: &bucket, Key: &key, ResponseContentType: aws.String("text/plain")})
				require.NoError(t, err)
				status, xCache, body = performLocalAuthRequestForTest(t, requestFromPresign(http.MethodGet, override.URL, override.SignedHeader, nil))
				require.Equal(t, 200, status, string(body))
				require.Equal(t, "BYPASS", xCache)
				require.Equal(t, payload, body)
				require.Greater(t, env.GetUpstreamRequestCount(), before)

				// A presigned PUT cannot be used to change another object.
				badPut := requestFromPresign(http.MethodPut, put.URL, put.SignedHeader, payload)
				badPut.URL.Path = "/" + bucket + "/another-object"
				badPut.URL.RawPath = ""
				status, _, _ = performLocalAuthRequestForTest(t, badPut)
				require.Equal(t, 403, status)
				_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: &bucket, Key: aws.String("another-object")})
				require.Error(t, err)
				// Verify that S3's serializer kept the literal question mark in the key.
				u, err := url.Parse(get.URL)
				require.NoError(t, err)
				expectedPath := "/" + bucket + "/" + key
				if virtualHost {
					expectedPath = "/" + key
				}
				require.Equal(t, expectedPath, u.Path)
			})
		}
	}
}
