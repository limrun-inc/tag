// Package auth provides authentication and authorization for TAG.
package auth

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// ErrUnknownAccessKey is returned when the access key is not found in the store.
var ErrUnknownAccessKey = errors.New("unknown access key")

// CredentialStore holds access_key → secret_key mappings.
// It is safe for concurrent use.
type CredentialStore struct {
	credentials map[string]string
	mu          sync.RWMutex
}

// NewCredentialStore creates a new empty credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		credentials: make(map[string]string),
	}
}

// LoadFromEnv loads the proxy credential and an optional secondary validation
// credential. The secondary credential is used only for local signature
// validation; transparent forwarding continues using the proxy credential.
func (c *CredentialStore) LoadFromEnv() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, names := range [][2]string{
		{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"},
		{"TAG_VALIDATION_ACCESS_KEY_ID", "TAG_VALIDATION_SECRET_ACCESS_KEY"},
	} {
		accessKey := os.Getenv(names[0])
		secretKey := os.Getenv(names[1])
		if accessKey != "" && secretKey != "" {
			c.credentials[accessKey] = secretKey
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
}

// RemoveCredential removes a credential mapping.
func (c *CredentialStore) RemoveCredential(accessKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.credentials, accessKey)
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
