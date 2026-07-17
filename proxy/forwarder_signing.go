package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
)

// signingForwarder validates incoming request signatures and re-signs requests
// before forwarding to upstream Tigris. This is the default mode where TAG
// acts as a credential-translating proxy.
//
// DoFullObjectRequest is inherited from baseForwarder (always uses SigV4 signing).
type signingForwarder struct {
	baseForwarder
	credStore *auth.CredentialStore
	validator *auth.RequestValidator
}

// Forward forwards a request to Tigris and writes the response to the client.
// Validates the incoming request signature, re-signs with upstream credentials,
// and streams the response back. If the request uses AWS chunked transfer encoding
// (streaming SigV4), the body is decoded on-the-fly and forwarded as UNSIGNED-PAYLOAD.
func (f *signingForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)

	// Validate incoming request signature
	accessKey, err := f.validateRequest(r)
	if err != nil {
		log.Warn().Err(err).Str("path", r.URL.Path).Msg("Request signature validation failed")
		return mapAuthError(err)
	}

	// Look up secret key from credential store
	secretKey, err := f.credStore.GetSecretKey(accessKey)
	if err != nil {
		return mapAuthError(err)
	}

	fwdReq, err := f.signUpstreamRequest(ctx, r, body, bodyHash, accessKey, secretKey)
	if err != nil {
		return err
	}
	prepareForwardedRequest(fwdReq, contentLength, chunked)

	return f.executeAndStream(w, fwdReq, contentLength, nil)
}

// ForwardWithCapture forwards request and captures response for caching.
// Validates and re-signs like Forward, but also captures the response body
// for caching while streaming to the client.
func (f *signingForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)

	// Validate incoming request signature
	accessKey, err := f.validateRequest(r)
	if err != nil {
		log.Warn().Err(err).Str("path", r.URL.Path).Msg("Request signature validation failed")
		return nil, mapAuthError(err)
	}

	// Look up secret key
	secretKey, err := f.credStore.GetSecretKey(accessKey)
	if err != nil {
		return nil, mapAuthError(err)
	}

	fwdReq, err := f.signUpstreamRequest(ctx, r, body, bodyHash, accessKey, secretKey)
	if err != nil {
		return nil, err
	}
	prepareForwardedRequest(fwdReq, contentLength, chunked)

	return f.executeAndCapture(w, fwdReq, contentLength, nil)
}

// ValidateAndGetCredentials validates the request signature and returns credentials.
// In signing mode, validation is always performed locally — returns AuthValidated on success.
func (f *signingForwarder) ValidateAndGetCredentials(r *http.Request) (AuthResult, string, string, error) {
	accessKey, err := f.validateRequest(r)
	if err != nil {
		log.Warn().Err(err).Str("path", r.URL.Path).Msg("Request signature validation failed")
		return AuthNotValidated, "", "", mapAuthError(err)
	}

	secretKey, err := f.credStore.GetSecretKey(accessKey)
	if err != nil {
		return AuthNotValidated, "", "", mapAuthError(err)
	}

	return AuthValidated, accessKey, secretKey, nil
}

// DoRequestWithCreds executes a request with pre-validated credentials.
// Returns the raw response for streaming. Caller is responsible for closing the response body.
// If the request uses AWS chunked transfer encoding, the body is decoded on-the-fly.
func (f *signingForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
	// Decode AWS chunked encoding if present, otherwise pass through unchanged
	body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)

	fwdReq, err := f.signUpstreamRequest(ctx, r, body, bodyHash, accessKey, secretKey)
	if err != nil {
		return nil, err
	}
	prepareForwardedRequest(fwdReq, contentLength, chunked)

	return f.executeRequest(fwdReq, contentLength, nil)
}

func (f *signingForwarder) validateRequest(r *http.Request) (string, error) {
	if isPresignedRead(r) && hasSessionToken(r) {
		return "", auth.ErrInvalidAuthFormat
	}
	accessKey, err := f.validator.ValidateRequest(r)
	if isPresignedRead(r) && errors.Is(err, auth.ErrInvalidDate) {
		return "", fmt.Errorf("%w: %v", auth.ErrInvalidAuthFormat, err)
	}
	return accessKey, err
}

func (f *signingForwarder) signUpstreamRequest(
	ctx context.Context,
	r *http.Request,
	body io.Reader,
	bodyHash, accessKey, secretKey string,
) (*http.Request, error) {
	if isPresignedRead(r) {
		return f.signer.ResignPresignedRequest(ctx, r, accessKey, secretKey)
	}

	return f.signer.SignRequest(
		ctx,
		r.Method,
		buildHeaderSigningPath(r),
		body,
		bodyHash,
		accessKey,
		secretKey,
		r.Header,
	)
}

func isPresignedRead(r *http.Request) bool {
	return auth.IsPresignedRequest(r) &&
		(r.Method == http.MethodGet || r.Method == http.MethodHead)
}

func buildHeaderSigningPath(r *http.Request) string {
	if !auth.IsPresignedRequest(r) {
		if r.URL.RawQuery != "" {
			return r.URL.Path + "?" + r.URL.RawQuery
		}
		return r.URL.Path
	}

	query := r.URL.Query()
	auth.RemoveQueryAuthentication(query)
	if encodedQuery := query.Encode(); encodedQuery != "" {
		return r.URL.Path + "?" + encodedQuery
	}
	return r.URL.Path
}
