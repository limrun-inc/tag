package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy/origin"
)

func (b *byteBudget) TryAcquire(n int64) bool { return b.tryAcquireStaging(n) }
func (b *byteBudget) Release(n int64)         { b.releaseStaging(n) }

func (b *baseForwarder) configureOriginRanges(budget *byteBudget) {
	if budget == nil {
		// Disabling cache buffering limits must not enable unbounded range buffers.
		budget = newByteBudget(config.DefaultCacheMaxPopulateMemoryBytes, 0, 0)
		budget.stagingCap = budget.total / 2
	}
	b.originReader = origin.New(budget)
	client := *b.httpClient
	transport := b.httpClient.Transport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 16
	transport.MaxIdleConnsPerHost = 16
	transport.MaxIdleConns = 16
	client.Transport = transport
	b.hedgeClient = &client
}

// rangeFetch is supplied only after authentication. Transparent client requests
// keep their original signature and are not converted into service-credential reads.
func (b *baseForwarder) rangeFetch(req *http.Request, accessKey, secretKey string) origin.Fetch {
	if b.originReader == nil || req.Method != http.MethodGet || req.Header.Get("Range") != "" ||
		(req.Body != nil && req.Body != http.NoBody) ||
		accessKey == "" || secretKey == "" {
		return nil
	}
	query, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil
	}
	for key := range query {
		switch key {
		case "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires",
			"X-Amz-SignedHeaders", "X-Amz-Signature", "X-Amz-Checksum-Mode":
		case "x-id":
			if query.Get(key) != "GetObject" {
				return nil
			}
		default:
			return nil // versionId, partNumber, response overrides, and other operations
		}
	}
	var expires time.Time
	if query.Has("X-Amz-Signature") {
		date, err := time.Parse("20060102T150405Z", query.Get("X-Amz-Date"))
		seconds, parseErr := strconv.ParseInt(query.Get("X-Amz-Expires"), 10, 64)
		if err != nil || parseErr != nil || seconds < 0 || seconds > 7*24*3600 {
			return nil
		}
		expires = date.Add(time.Duration(seconds) * time.Second)
	}
	return func(ctx context.Context, start, end int64, etag string, extra bool) (*http.Response, error) {
		if !expires.IsZero() && !time.Now().Before(expires) {
			return nil, errors.New("presigned range authorization expired")
		}
		rangeReq, err := b.signer.SignRangeRequest(ctx, req, start, end, etag, accessKey, secretKey)
		if err != nil {
			return nil, err
		}
		client := b.httpClient
		if extra {
			client = b.hedgeClient
		}
		return client.Do(rangeReq)
	}
}

func (b *baseForwarder) doOriginRequest(req *http.Request, fetch origin.Fetch) (*http.Response, error) {
	resp, err := b.httpClient.Do(req)
	if err == nil && b.originReader != nil && fetch != nil {
		b.originReader.Wrap(req, resp, fetch)
	}
	return resp, err
}
