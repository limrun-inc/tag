package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
)

// Exercise real signing/forwarding and the first checksum-bearing presigned GET,
// with no read to seed the cache. HEAD-confirmed tee and read-back fallback must
// both produce checksum-aware metadata, including when origin has no checksum.
func TestWriteThroughFirstPresignedDownload(t *testing.T) {
	for _, blocks := range []bool{false, true} {
		for _, checksum := range []bool{false, true} {
			for _, fallback := range []bool{false, true} {
				t.Run(fmt.Sprintf("blocks=%v/checksum=%v/fallback=%v", blocks, checksum, fallback), func(t *testing.T) {
					body := bytes.Repeat([]byte("large-upload"), 200000) // Above the old 1 MiB cap.
					sum := sha256.Sum256(body)
					checksumValue := base64.StdEncoding.EncodeToString(sum[:])
					var puts, heads, bodyGets, validations atomic.Int32
					origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method == http.MethodPut {
							require.NoError(t, verifyPresignedWithSDK(r))
							got, err := io.ReadAll(r.Body)
							require.NoError(t, err)
							require.Equal(t, body, got)
							puts.Add(1)
							w.Header().Set("ETag", `"uploaded"`)
							return
						}
						if r.Header.Get("If-None-Match") != "" {
							require.NoError(t, verifyPresignedWithSDK(r))
							require.Equal(t, `"uploaded"`, r.Header.Get("If-None-Match"))
							validations.Add(1)
							w.WriteHeader(http.StatusNotModified)
							return
						}
						require.Equal(t, "ENABLED", r.Header.Get("X-Amz-Checksum-Mode"))
						require.Contains(t, r.Header.Get("Authorization"), "x-amz-checksum-mode")
						w.Header().Set("ETag", `"uploaded"`)
						w.Header().Set("Content-Length", strconv.Itoa(len(body)))
						if checksum {
							w.Header().Set("X-Amz-Checksum-Sha256", checksumValue)
						}
						if r.Method == http.MethodHead {
							heads.Add(1)
							return
						}
						bodyGets.Add(1)
						_, _ = w.Write(body)
					}))
					defer origin.Close()
					creds := auth.NewCredentialStore()
					creds.AddCredential(compatAccess, compatSecret)
					svc, c := newTestService(NewForwarder(creds, origin.URL, compatRegion, 8, nil, nil), true)
					t.Cleanup(svc.Stop)
					svc.config.Cache.WarmOnWrite = true
					svc.config.Cache.SetBlockCachingEnabled(blocks)
					if fallback {
						svc.config.Cache.WriteThroughMaxSize = 1024
					}
					put := signingPresign(t, http.MethodPut, "http://gateway.example/bucket/asset.bin", body, nil, time.Now().UTC(), "600")
					w := httptest.NewRecorder()
					require.NoError(t, svc.HandlePutObject(w, put))
					require.Equal(t, http.StatusOK, w.Code)
					require.True(t, metaCached(c, "bucket", "asset.bin", 3*time.Second))
					meta, found, err := c.GetMeta(t.Context(), "bucket", "asset.bin")
					require.NoError(t, err)
					require.True(t, found)
					require.True(t, meta.ChecksumsRequested)
					require.Equal(t, blocks, meta.BlockSize > 0)
					require.EqualValues(t, 1, puts.Load())
					if fallback {
						require.EqualValues(t, 1, bodyGets.Load())
						require.Zero(t, heads.Load())
					} else {
						require.Zero(t, bodyGets.Load())
						require.EqualValues(t, 1, heads.Load())
					}
					before := bodyGets.Load()
					get := compatPresignedGET(t, "http://gateway.example/bucket/asset.bin")
					w = httptest.NewRecorder()
					require.NoError(t, svc.HandleGetObject(w, get))
					require.Equal(t, http.StatusOK, w.Code)
					require.Equal(t, "HIT", w.Header().Get("X-Cache"))
					require.Equal(t, body, w.Body.Bytes())
					if checksum {
						require.Equal(t, checksumValue, w.Header().Get("X-Amz-Checksum-Sha256"))
					}
					require.Equal(t, before, bodyGets.Load(), "first GET must not fetch the body again")
					require.EqualValues(t, 1, validations.Load())
				})
			}
		}
	}
}

func TestWriteThroughAdmissionIncludesBodyAndWriter(t *testing.T) {
	for _, mode := range []string{"admitted", "body-only-budget", "held-budget", "no-budget", "above-cap", "disabled", "invalid-credentials"} {
		t.Run(mode, func(t *testing.T) {
			const size = 2 << 20
			var tees, plain atomic.Int32
			f := &teeMockForwarder{mockForwarder: &mockForwarder{forwardFunc: func(_ context.Context, w http.ResponseWriter, r *http.Request) error {
				_, err := io.Copy(io.Discard, r.Body)
				plain.Add(1)
				w.WriteHeader(200)
				return err
			}}, teeFunc: teeUpstream(&tees, `"uploaded"`)}
			svc, _ := newTestService(f, true)
			t.Cleanup(svc.Stop)
			svc.config.Cache.WarmOnWrite = true
			svc.config.Cache.SetBlockCachingEnabled(false)
			svc.config.Cache.WriteThroughMaxSize = size // Inclusive boundary, independent of block size.
			weight := int64(size) + svc.backgroundPopulateWeight()
			svc.populateBudget = newByteBudget(weight, 0, 0)
			switch mode {
			case "body-only-budget":
				svc.populateBudget = newByteBudget(size, 0, 0)
			case "held-budget":
				require.True(t, svc.populateBudget.tryAcquireReadMiss(1))
			case "no-budget":
				svc.populateBudget = nil
			case "above-cap":
				svc.config.Cache.WriteThroughMaxSize = size - 1
			case "disabled":
				svc.config.Cache.WriteThroughMaxSize = -1
			case "invalid-credentials":
				f.validateFunc = func(*http.Request) (AuthResult, string, string, error) {
					return AuthNotValidated, "", "", fmt.Errorf("invalid credentials")
				}
			}
			state, rejected, err := svc.forwardPutMaybeTee(t.Context(), httptest.NewRecorder(), authedPut("bucket", "asset", strings.Repeat("x", size)), "bucket", "asset")
			require.NoError(t, err)
			require.False(t, rejected)
			if mode == "admitted" {
				require.NotNil(t, state)
				require.Equal(t, weight, state.weight)
				require.EqualValues(t, 1, tees.Load())
				require.Zero(t, plain.Load())
				require.Zero(t, svc.populateBudget.remaining)
				svc.releaseCacheSlot(state.weight)
				require.Equal(t, weight, svc.populateBudget.remaining)
			} else {
				require.Nil(t, state)
				require.Zero(t, tees.Load())
				require.EqualValues(t, 1, plain.Load())
			}
		})
	}
}
