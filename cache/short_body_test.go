package cache

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// TestShortBodyIsNotPublished pins that a body which ended before its content length never becomes
// a visible entry.
//
// The metadata is built from upstream's headers, so it promises the full size however much of the
// body actually arrived. Published over a short body it is served as a complete hit for the rest of
// its TTL: a client that checks length refetches from origin on every read and never gets a hit
// again, and a client that does not check gets a truncated object.
func TestShortBodyIsNotPublished(t *testing.T) {
	c, _ := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: 1024, StatusCode: 200}
	wrote, err := c.PutWithMetaStreamTombstoneAware(ctx, bucket, key, meta,
		strings.NewReader(strings.Repeat("a", 512)), 60, time.Now().UnixNano())

	if err == nil {
		t.Error("err = nil, want an error: a stream that ended halfway is a failed write, not a quiet one")
	}
	if wrote {
		t.Error("wrote = true, want false")
	}
	if _, found, _ := c.GetMeta(ctx, bucket, key); found {
		t.Error("the entry is visible: every later read serves 512 bytes as the 1024 its metadata promises")
	}
}

// TestCompleteBodyIsPublished keeps the guard from rejecting the writes it exists to protect.
func TestCompleteBodyIsPublished(t *testing.T) {
	c, _ := newVersioningTestCache(t)
	ctx := context.Background()
	bucket, key := "b", "k"

	body := strings.Repeat("a", 1024)
	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: int64(len(body)), StatusCode: 200}
	wrote, err := c.PutWithMetaStreamTombstoneAware(ctx, bucket, key, meta,
		strings.NewReader(body), 60, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("PutWithMetaStreamTombstoneAware: %v", err)
	}
	if !wrote {
		t.Fatal("wrote = false, want true for a body that arrived whole")
	}

	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, meta.ETag, &buf); err != nil {
		t.Fatalf("GetBodyStream: %v", err)
	}
	if buf.Len() != len(body) {
		t.Errorf("cached body = %d bytes, want %d", buf.Len(), len(body))
	}
}
