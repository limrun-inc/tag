package handlers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

func TestPresignedVirtualHostRoutesAndBucketValidation(t *testing.T) {
	var calls atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, "app archive")
	}))
	defer origin.Close()
	cfg := config.NewDefault()
	f := proxy.NewForwarder(nil, origin.URL, "auto", 10, auth.NewProxySigner("test", "secret"), nil)
	service := proxy.NewService(f, cache.NewDisabledCache(), cfg)
	defer service.Stop()
	server := NewServer(service, "127.0.0.1", 0, false, 0)
	for _, tc := range []struct {
		host, path string
		status     int
	}{
		{"asset-storage-production.s3.limrun.net", "/org_example/app:staging:hash.apk", 200},
		{"appstore-production.s3.limrun.dev", "/ExpoGo.apk", 200},
		{"asset-storage-production.t3.tigrisfiles.io", "/org_example/app.ipa", 200},
		{"bad_bucket.s3.limrun.net", "/org_example/app.apk", 400},
		{"a.s3.limrun.net", "/ExpoGo.apk", 400},
		{"bad..bucket.s3.limrun.net", "/app.apk", 400},
	} {
		t.Run(tc.host+tc.path, func(t *testing.T) {
			before := calls.Load()
			r := httptest.NewRequest("GET", "https://"+tc.host+tc.path+"?X-Amz-Credential=test", nil)
			r.Header.Set("Cache-Control", "no-cache")
			w := httptest.NewRecorder()
			server.Router().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
			}
			if tc.status == 400 && calls.Load() != before {
				t.Fatal("invalid bucket reached origin")
			}
		})
	}
}
