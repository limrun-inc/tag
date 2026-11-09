package proxy

import (
	"context"
	"net/http"
)

// checksumCacheFillForwarder explicitly requests checksum metadata for synthetic
// cache fills. The bool returned by cacheFillRequest must only be true when the
// origin was actually asked for checksums, including when it returns none.
type checksumCacheFillForwarder interface {
	DoCacheFillRequest(context.Context, string, string, string, string, string) (*http.Response, error)
}

func (s *Service) cacheFillRequest(ctx context.Context, method, bucket, key, accessKey, secretKey string) (*http.Response, bool, error) {
	if f, ok := s.forwarder.(checksumCacheFillForwarder); ok {
		resp, err := f.DoCacheFillRequest(ctx, method, bucket, key, accessKey, secretKey)
		return resp, true, err
	}
	// Custom forwarders without checksum support retain their existing behavior;
	// their entries must not pretend to contain authoritative checksum metadata.
	if method == http.MethodHead {
		resp, err := s.forwarder.DoConditionalHeadRequest(ctx, bucket, key, accessKey, secretKey, "", 0)
		return resp, false, err
	}
	resp, err := s.forwarder.DoFullObjectRequest(ctx, bucket, key, accessKey, secretKey)
	return resp, false, err
}
