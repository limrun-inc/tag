package auth

import (
	"context"
	"net/http"
	"testing"
)

func TestSignRangePreservesObjectAndPinsVersion(t *testing.T) {
	signer := NewRequestSigner("https://account.r2.cloudflarestorage.com", "auto")
	req, err := signer.SignObjectRequest(context.Background(), "GET", "bucket", "literal?key+%/name", nil, "UNSIGNED-PAYLOAD", "access", "secret", http.Header{
		"If-None-Match": {`"old"`}, "If-Modified-Since": {"Thu, 01 Jan 1970 00:00:00 GMT"}, "X-Amz-Server-Side-Encryption-Customer-Algorithm": {"AES256"},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := req.Header.Clone()
	ranged, err := signer.SignRangeRequest(context.Background(), req, 16<<20, (32<<20)-1, `"version"`, "access", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if ranged.URL.String() != req.URL.String() {
		t.Fatal("object URL changed")
	}
	if ranged.Header.Get("Range") != "bytes=16777216-33554431" || ranged.Header.Get("If-Match") != `"version"` {
		t.Fatal("range is not pinned")
	}
	if ranged.Header.Get("If-None-Match") != "" || ranged.Header.Get("If-Modified-Since") != "" {
		t.Fatal("revalidation condition leaked into range")
	}
	if ranged.Header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != "AES256" {
		t.Fatal("object header lost")
	}
	if req.Header.Get("If-None-Match") != before.Get("If-None-Match") || req.Header.Get("Range") != "" {
		t.Fatal("original request mutated")
	}
	store := NewCredentialStore()
	store.AddCredential("access", "secret")
	if _, err := NewRequestValidator(store).Validate(ranged); err != nil {
		t.Fatalf("range signature: %v", err)
	}
}
