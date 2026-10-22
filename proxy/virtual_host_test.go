package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
)

func TestPresignedVirtualHostBucket(t *testing.T) {
	for _, tc := range []struct{ host, path, bucket, key string }{
		{"asset-storage-production.s3.limrun.net", "/org_example/app:staging:hash.apk", "asset-storage-production", "org_example/app:staging:hash.apk"},
		{"appstore-production.s3.limrun.dev:443", "/ExpoGo.apk", "appstore-production", "ExpoGo.apk"},
		{"asset-storage-production.t3.tigrisfiles.io", "/org_example/app.ipa", "asset-storage-production", "org_example/app.ipa"},
		{"t3.storage.dev", "/limrun-build-cache/org/archive.tar", "limrun-build-cache", "org/archive.tar"},
	} {
		t.Run(tc.host, func(t *testing.T) {
			r := httptest.NewRequest("GET", "https://"+tc.host+tc.path+"?X-Amz-Credential=test", nil)
			bucket, key := ParseBucketKey(r)
			if bucket != tc.bucket || key != tc.key {
				t.Fatalf("got %q/%q, want %q/%q", bucket, key, tc.bucket, tc.key)
			}
			r.URL.RawQuery = ""
			if _, ok := PresignedVirtualHostBucket(r); ok {
				t.Fatal("unsigned requests must keep path-style parsing")
			}
		})
	}
}

func TestTransparentVirtualHostTLSUsesConfiguredOrigin(t *testing.T) {
	const first = "asset-storage-production.s3.example.test"
	const second = "appstore-production.s3.example.test"
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "TAG controlled origin"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{first, second}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	type observed struct{ host, sni, remote, path, query, forwarded, protocol string }
	requests := make(chan observed, 8)
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- observed{r.Host, r.TLS.ServerName, r.RemoteAddr, r.URL.EscapedPath(), r.URL.RawQuery, r.Header.Get("X-Tigris-Forwarded-Host"), r.Proto}
		io.WriteString(w, "complete object")
	}))
	origin.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	origin.EnableHTTP2 = true
	origin.StartTLS()
	defer origin.Close()
	f := NewForwarder(nil, origin.URL, "auto", 10, auth.NewProxySigner("test", "secret"), nil).(*transparentForwarder)
	transport := f.httpClient.Transport.(*http.Transport)
	transport.TLSClientConfig = &tls.Config{RootCAs: roots}
	t.Cleanup(transport.CloseIdleConnections)
	const path = "/org_example/app%2Fstaging%3Ahash.apk"
	const query = "X-Amz-Credential=test&X-Amz-Signature=untouched%2Bvalue&response-content-type=a%2Fb"
	var previous observed
	for i, host := range []string{first, first, second, first} {
		r := httptest.NewRequest("GET", "https://"+host+path+"?"+query, nil)
		w := httptest.NewRecorder()
		if err := f.Forward(t.Context(), w, r); err != nil {
			t.Fatal(err)
		}
		got := <-requests
		if got.host != host || got.sni != host || got.forwarded != host || got.path != path || got.query != query || got.protocol != "HTTP/2.0" || w.Body.String() != "complete object" {
			t.Fatalf("request changed: %+v, body=%q", got, w.Body.String())
		}
		if i == 1 && got.remote != previous.remote {
			t.Fatal("same hostname did not reuse its connection")
		}
		if i == 2 && got.remote == previous.remote {
			t.Fatal("different TLS hostnames shared a connection")
		}
		previous = got
	}
	// This hostname is deliberately absent from the certificate. It must still
	// reach the configured listener, then fail hostname verification.
	r := httptest.NewRequest("GET", "https://wrong.s3.example.test/object?"+query, nil)
	err = f.Forward(context.Background(), httptest.NewRecorder(), r)
	var hostnameErr x509.HostnameError
	if !errors.As(err, &hostnameErr) {
		t.Fatalf("wrong hostname error = %v, want certificate rejection", err)
	}
	if len(requests) != 0 {
		t.Fatal("unverified TLS request reached the HTTP handler")
	}
	// Header-signed build-cache traffic retains the configured host and path.
	r = httptest.NewRequest("GET", "http://tag.local/limrun-build-cache/org/archive.tar", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 test")
	if err := f.Forward(t.Context(), httptest.NewRecorder(), r); err != nil {
		t.Fatal(err)
	}
	got := <-requests
	if got.host != strings.TrimPrefix(origin.URL, "https://") || got.path != "/limrun-build-cache/org/archive.tar" {
		t.Fatalf("path-style request changed: %+v", got)
	}
}
