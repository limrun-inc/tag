package proxy

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/metrics"
)

// Revalidate with the caller's signed capability, including its expiry and query
// parameters. Only a successful origin check permits serving the cached copy.
func (s *Service) revalidatePresignedAndServe(ctx context.Context, w http.ResponseWriter, r *http.Request,
	bucket, key, accessKey, secretKey string, meta *cache.CachedObjectMeta, start time.Time,
) error {
	metrics.RecordRevalidationTriggered()
	operation := "GetObject"
	if r.Method == http.MethodHead {
		operation = "HeadObject"
	}
	conditional := r.Clone(ctx)
	conditional.Header.Set("If-None-Match", meta.ETag)
	resp, err := s.forwarder.DoRequestWithCreds(ctx, conditional, accessKey, secretKey)
	if err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("Presigned revalidation failed")
		metrics.RecordRevalidationFailed()
		metrics.RecordRequestResult(operation, metrics.SourceUpstream, err, time.Since(start).Seconds())
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		// An origin policy change can make an unchanged object uncacheable.
		// Discard the old entry and fetch the original request in that case.
		if hasNoCacheDirectives(resp.Header.Values("Cache-Control")) || resp.Header.Get("ETag") != "" && resp.Header.Get("ETag") != meta.ETag {
			s.invalidateStaleMeta(bucket, key, meta.ETag)
			return s.forwardAfterCacheMiss(ctx, w, r, bucket, key, meta.ETag, nil, start)
		}
		if r.Method == http.MethodHead {
			metrics.RecordRevalidationNotModified()
			serveMetaHit(w, meta, operation, start)
			return nil
		}
		if meta.BlockSize > 0 {
			metrics.RecordRevalidationNotModified()
			var served bool
			var bodyErr error
			if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
				served, bodyErr = s.serveRangeFromBlockCache(ctx, w, r, bucket, key, accessKey, secretKey, meta, rangeHeader, start)
			} else {
				served, bodyErr = s.serveFullObjectFromBlockCache(ctx, w, bucket, key, accessKey, secretKey, meta, start)
			}
			if served {
				return bodyErr
			}
			return s.forwardAfterCacheMiss(ctx, w, r, bucket, key, meta.ETag, bodyErr, start)
		}
		served, bodyErr := s.handleRevalidation304(ctx, w, r, bucket, key, meta, r.Header.Get("Range"), start)
		if served {
			return bodyErr
		}
		// The validated version was evicted before its body could be opened.
		return s.forwardAfterCacheMiss(ctx, w, r, bucket, key, meta.ETag, bodyErr, start)
	case http.StatusOK:
		if r.Method == http.MethodGet {
			return s.handleRevalidation200(ctx, w, r, bucket, key, meta.ETag, resp, start)
		}
		metrics.RecordRevalidationUpdated()
		s.invalidateStaleMeta(bucket, key, meta.ETag)
	case http.StatusPartialContent:
		if r.Method == http.MethodGet {
			return s.handleRevalidation206Range(ctx, w, r, bucket, key, accessKey, secretKey, meta.ETag, resp, start)
		}
		metrics.RecordRevalidationFailed()
	default:
		// In particular, a 403/404 or transient 5xx must never become a cached 200.
		metrics.RecordRevalidationFailed()
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
			s.invalidateStaleMeta(bucket, key, meta.ETag)
		}
	}
	copyHeaders(w.Header(), resp.Header)
	writeCacheStatus(w, XCacheRevalidated)
	w.WriteHeader(resp.StatusCode)
	var n int64
	if r.Method != http.MethodHead {
		n, err = io.Copy(w, resp.Body)
	}
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
	status := "success"
	if err != nil || resp.StatusCode >= 400 {
		status = "error"
	}
	metrics.RecordRequest(operation, status, metrics.SourceUpstream, time.Since(start).Seconds())
	return err
}
