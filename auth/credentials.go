// Package auth provides authentication and authorization for TAG.
package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// ErrUnknownAccessKey is returned when the access key is not found in the store.
var ErrUnknownAccessKey = errors.New("unknown access key")

// ErrBucketAccessDenied prevents a scoped key from reading another bucket's cache.
var ErrBucketAccessDenied = errors.New("signing credential does not authorize this bucket")

// CredentialStore holds access_key → secret_key mappings.
// It is safe for concurrent use.
type CredentialStore struct {
	credentials map[string]string
	buckets     map[string]map[string]struct{}
	mu          sync.RWMutex
}

// NewCredentialStore creates a new empty credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		credentials: make(map[string]string),
		buckets:     make(map[string]map[string]struct{}),
	}
}

// LoadFromEnv also accepts bucket-scoped presigner keys from a mounted Secret.
// Each listed key must have upstream read access to every object in its buckets.
func (c *CredentialStore) LoadFromEnv() error {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	pending := make(map[string]string)
	scopes := make(map[string]map[string]struct{})
	if accessKey != "" && secretKey != "" {
		pending[accessKey] = secretKey
	}
	if path := os.Getenv("TAG_SIGNING_CREDENTIALS_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read TAG_SIGNING_CREDENTIALS_FILE: %w", err)
		}
		var entries []struct {
			AccessKey string   `json:"access_key_id"`
			SecretKey string   `json:"secret_access_key"`
			Buckets   []string `json:"buckets"`
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&entries); err != nil || len(entries) == 0 {
			return errors.New("TAG_SIGNING_CREDENTIALS_FILE must contain a nonempty credential array")
		}
		if decoder.Decode(new(any)) != io.EOF {
			return errors.New("TAG_SIGNING_CREDENTIALS_FILE has trailing data")
		}
		for _, entry := range entries {
			if entry.AccessKey == "" || entry.SecretKey == "" || len(entry.Buckets) == 0 {
				return errors.New("signing credential requires an access key, secret key, and explicit buckets")
			}
			if _, exists := scopes[entry.AccessKey]; exists {
				return errors.New("duplicate signing credential")
			}
			if previous, exists := pending[entry.AccessKey]; exists && previous != entry.SecretKey {
				return errors.New("conflicting signing credentials")
			}
			buckets := make(map[string]struct{}, len(entry.Buckets))
			for _, bucket := range entry.Buckets {
				if bucket == "" || strings.TrimSpace(bucket) != bucket || strings.ContainsAny(bucket, "/*?\\") {
					return errors.New("signing credential buckets must be explicit bucket names")
				}
				buckets[bucket] = struct{}{}
			}
			pending[entry.AccessKey] = entry.SecretKey
			scopes[entry.AccessKey] = buckets
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, secret := range pending {
		if previous, exists := c.credentials[key]; exists && previous != secret {
			return errors.New("configured signing credential conflicts with the credential store")
		}
	}
	for key, secret := range pending {
		c.credentials[key] = secret
		c.buckets[key] = scopes[key]
	}
	return nil
}

// AuthorizeBucket applies the operator's read grant before any shared cache lookup.
func (c *CredentialStore) AuthorizeBucket(accessKey, bucket string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, exists := c.credentials[accessKey]; !exists {
		return ErrUnknownAccessKey
	}
	if allowed := c.buckets[accessKey]; allowed != nil {
		if _, exists := allowed[bucket]; !exists {
			return ErrBucketAccessDenied
		}
	}
	return nil
}

// GetSecretKey looks up the secret key for a given access key.
func (c *CredentialStore) GetSecretKey(accessKey string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if secret, ok := c.credentials[accessKey]; ok {
		return secret, nil
	}
	return "", fmt.Errorf("%w: %s", ErrUnknownAccessKey, accessKey)
}

// AddCredential adds or updates a credential mapping.
func (c *CredentialStore) AddCredential(accessKey, secretKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.credentials[accessKey] = secretKey
	delete(c.buckets, accessKey)
}

// RemoveCredential removes a credential mapping.
func (c *CredentialStore) RemoveCredential(accessKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.credentials, accessKey)
	delete(c.buckets, accessKey)
}

// Count returns the number of credentials stored.
func (c *CredentialStore) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.credentials)
}

// HasCredential checks if a credential exists for the given access key.
func (c *CredentialStore) HasCredential(accessKey string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.credentials[accessKey]
	return ok
}

// GetSigningKey derives the SigV4 signing key for the given access key, date, and region.
// Implements the KeyProvider interface.
func (c *CredentialStore) GetSigningKey(accessKey, date, region string) ([]byte, error) {
	secretKey, err := c.GetSecretKey(accessKey)
	if err != nil {
		return nil, err
	}
	return deriveSigningKey(secretKey, date, region), nil
}

// HasKey returns whether a signing key can be produced for the given access key.
// Implements the KeyProvider interface.
func (c *CredentialStore) HasKey(accessKey string) bool {
	return c.HasCredential(accessKey)
}

// Compile-time check that CredentialStore implements KeyProvider.
var _ KeyProvider = (*CredentialStore)(nil)
