package proxy

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/tigrisdata/tag/auth"
)

func TestBuildSigningPath(t *testing.T) {
	t.Run("preserves regular query parameters", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/bucket/key?versionId=v1&foo=bar", nil)

		path := buildSigningPath(req)
		parsed, err := url.Parse(path)
		if err != nil {
			t.Fatalf("url.Parse() error = %v", err)
		}
		if parsed.Query().Get("versionId") != "v1" || parsed.Query().Get("foo") != "bar" {
			t.Errorf("buildSigningPath() query = %q, want regular parameters preserved", parsed.RawQuery)
		}
	})

	t.Run("preserves non-presigned raw query", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/bucket/key?acl&prefix=a%20b", nil)
		if got := buildSigningPath(req); got != "/bucket/key?acl&prefix=a%20b" {
			t.Errorf("buildSigningPath() = %q, want raw query preserved", got)
		}
	})

	t.Run("replaces presigned authentication with header authentication", func(t *testing.T) {
		req := httptest.NewRequest(
			http.MethodGet,
			"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=key%2Fdate%2Fregion%2Fs3%2Faws4_request&X-Amz-Date=20260715T080000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=signature&X-Amz-Security-Token=token&response-content-type=text%2Fplain&x-id=GetObject",
			nil,
		)

		path := buildSigningPath(req)
		parsed, err := url.Parse(path)
		if err != nil {
			t.Fatalf("url.Parse() error = %v", err)
		}
		query := parsed.Query()
		for key := range query {
			if auth.IsQueryAuthenticationParameter(key) {
				t.Errorf("buildSigningPath() retained presigned auth parameter %q", key)
			}
		}
		if query.Get("response-content-type") != "text/plain" {
			t.Errorf("response-content-type = %q, want %q", query.Get("response-content-type"), "text/plain")
		}
		if query.Get("x-id") != "GetObject" {
			t.Errorf("x-id = %q, want %q", query.Get("x-id"), "GetObject")
		}
	})
}

func TestSigningForwarder_InvalidPresignedDateIsMalformedAuth(t *testing.T) {
	store := auth.NewCredentialStore()
	store.AddCredential("access-key", "secret-key")
	fwd := &signingForwarder{
		credStore: store,
		validator: auth.NewRequestValidator(store),
	}
	req := httptest.NewRequest(
		http.MethodGet,
		"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access-key%2F20260715%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=invalid&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=abc",
		nil,
	)

	_, err := fwd.validateRequest(req)
	if !errors.Is(err, auth.ErrInvalidAuthFormat) {
		t.Fatalf("validateRequest() error = %v, want %v", err, auth.ErrInvalidAuthFormat)
	}
}
