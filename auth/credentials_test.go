package auth

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCredentialStore_LoadFromEnv(t *testing.T) {
	// Set environment variables
	t.Setenv("AWS_ACCESS_KEY_ID", "env_access_key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env_secret_key")
	t.Setenv("TAG_SIGNING_CREDENTIALS_FILE", "")

	store := NewCredentialStore()
	if err := store.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if store.Count() != 1 {
		t.Errorf("Count() = %d, want 1", store.Count())
	}

	secret, err := store.GetSecretKey("env_access_key")
	if err != nil {
		t.Fatalf("GetSecretKey() error = %v", err)
	}
	if secret != "env_secret_key" {
		t.Errorf("GetSecretKey() = %q, want %q", secret, "env_secret_key")
	}

	// Verify unknown key returns error
	_, err = store.GetSecretKey("unknown_key")
	if err == nil {
		t.Error("GetSecretKey() should return error for unknown key")
	}
}

func TestCredentialStore_LoadFromEnv_NoEnvVars(t *testing.T) {
	// Ensure env vars are not set
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("TAG_SIGNING_CREDENTIALS_FILE", "")

	store := NewCredentialStore()
	if err := store.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v, should not error when env vars not set", err)
	}

	if store.Count() != 0 {
		t.Errorf("Count() = %d, want 0", store.Count())
	}
}

func TestCredentialStoreScopedPresigners(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "gateway")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "gateway-secret")
	path := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("TAG_SIGNING_CREDENTIALS_FILE", path)
	if err := os.WriteFile(path, []byte(`[{"access_key_id":"assets","secret_access_key":"asset-secret","buckets":["asset-storage-staging"]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	store := NewCredentialStore()
	if err := store.LoadFromEnv(); err != nil {
		t.Fatal(err)
	}
	if store.Count() != 2 {
		t.Fatal("both gateway and asset presigner must be loaded")
	}
	for _, tc := range []struct {
		key, bucket string
		want        error
	}{
		{"gateway", "build-cache", nil},
		{"assets", "asset-storage-staging", nil},
		{"assets", "build-cache", ErrBucketAccessDenied},
		{"assets", "", ErrBucketAccessDenied},
		{"unknown", "asset-storage-staging", ErrUnknownAccessKey},
	} {
		if err := store.AuthorizeBucket(tc.key, tc.bucket); !errors.Is(err, tc.want) {
			t.Errorf("AuthorizeBucket(%q, %q) = %v, want %v", tc.key, tc.bucket, err, tc.want)
		}
	}
	store.RemoveCredential("assets")
	if !errors.Is(store.AuthorizeBucket("assets", "asset-storage-staging"), ErrUnknownAccessKey) {
		t.Fatal("removed key remains authorized")
	}
}

func TestCredentialStoreFileFailsClosed(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "gateway")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "gateway-secret")
	for _, data := range []string{
		`[]`, `null`, `{}`, `[`,
		`[{"access_key_id":"assets","secret_access_key":"secret","buckets":["assets"],"typo":true}]`,
		`[{"access_key_id":"assets","secret_access_key":"secret"}]`,
		`[{"access_key_id":"assets","secret_access_key":"secret","buckets":["*"]}]`,
		`[{"access_key_id":"assets","secret_access_key":"secret","buckets":[""]}]`,
		`[{"access_key_id":"gateway","secret_access_key":"wrong-secret","buckets":["assets"]}]`,
		`[{"access_key_id":"assets","secret_access_key":"secret","buckets":["assets"]},{"access_key_id":"assets","secret_access_key":"secret","buckets":["other"]}]`,
		`[{"access_key_id":"assets","secret_access_key":"secret","buckets":["assets"]}] {}`,
	} {
		path := filepath.Join(t.TempDir(), "credentials.json")
		t.Setenv("TAG_SIGNING_CREDENTIALS_FILE", path)
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		store := NewCredentialStore()
		if err := store.LoadFromEnv(); err == nil {
			t.Errorf("accepted invalid file: %s", data)
		}
		if store.Count() != 0 {
			t.Fatal("invalid file partially loaded credentials")
		}
	}
	t.Setenv("TAG_SIGNING_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing"))
	if err := NewCredentialStore().LoadFromEnv(); err == nil {
		t.Fatal("missing credential file accepted")
	}
}

func TestCredentialStoreFileCanScopePrimary(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "gateway")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "gateway-secret")
	path := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("TAG_SIGNING_CREDENTIALS_FILE", path)
	if err := os.WriteFile(path, []byte(`[{"access_key_id":"gateway","secret_access_key":"gateway-secret","buckets":["build-cache"]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	store := NewCredentialStore()
	if err := store.LoadFromEnv(); err != nil {
		t.Fatal(err)
	}
	if err := store.AuthorizeBucket("gateway", "build-cache"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(store.AuthorizeBucket("gateway", "assets"), ErrBucketAccessDenied) {
		t.Fatal("primary key's explicit scope ignored")
	}
}
