package cache

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

func TestLegacyCoordinator_ExpiredPopulateCannotResurrectAfterMarkerExpires(t *testing.T) {
	for _, overwrite := range []bool{false, true} {
		name := "delete"
		if overwrite {
			name = "overwrite"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := config.NewDefault()
				client := cacheclient.NewMemoryCache()
				store := NewCacheWithClient(client, &cfg.Cache)
				ctx := context.Background()
				_, expected, _, err := store.GetMetaWithVersion(ctx, "bucket", "object")
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				if err := store.DeleteWithMeta(ctx, "bucket", "object"); err != nil {
					t.Fatal(err)
				}
				retention := TombstoneTTLSeconds(cfg.Cache.SizeThreshold)
				if overwrite {
					fresh := &CachedObjectMeta{Bucket: "bucket", Key: "object", ETag: `"new"`, StatusCode: 200}
					if wrote, err := store.PutMetaIfVersion(ctx, "bucket", "object", fresh, int(retention*2), VersionAny); err != nil || !wrote {
						t.Fatalf("seed replacement: wrote=%v err=%v", wrote, err)
					}
				}
				time.Sleep(time.Duration(retention+1) * time.Second)
				marker, err := client.Get(ctx, MakeTombstoneKey("bucket", "object"))
				if len(marker) != 0 || (err != nil && !isNotFoundError(err)) {
					t.Fatalf("marker did not expire: %x, %v", marker, err)
				}
				old := &CachedObjectMeta{Bucket: "bucket", Key: "object", ETag: `"old"`, StatusCode: 200}
				if wrote, err := store.PutMetaIfVersion(ctx, "bucket", "object", old, 60, expected); err != nil || wrote {
					t.Fatalf("expired populate: wrote=%v err=%v, want refused", wrote, err)
				}
				meta, _, found, err := store.GetMetaWithVersion(ctx, "bucket", "object")
				if err != nil || found != overwrite || (found && meta.ETag != `"new"`) {
					t.Fatalf("invalidated state replaced: found=%v meta=%+v err=%v", found, meta, err)
				}
				// A fresh fetch still has permission to warm after the old marker expires.
				_, freshToken, _, err := store.GetMetaWithVersion(ctx, "bucket", "object")
				if err != nil {
					t.Fatal(err)
				}
				if wrote, err := store.PutMetaIfVersion(ctx, "bucket", "object", old, 60, freshToken); err != nil || !wrote {
					t.Fatalf("fresh populate refused: wrote=%v err=%v", wrote, err)
				}
			})
		})
	}
}

func TestLegacyCoordinator_ProgressingPopulateCanExceedFiveMinutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.NewDefault()
		store := NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
		ctx := context.Background()
		_, expected, _, err := store.GetMetaWithVersion(ctx, "bucket", "object")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(8 * time.Minute)
		meta := &CachedObjectMeta{Bucket: "bucket", Key: "object", ETag: `"complete"`, StatusCode: 200}
		if wrote, err := store.PutMetaIfVersion(ctx, "bucket", "object", meta, 60, expected); err != nil || !wrote {
			t.Fatalf("progressing populate refused: wrote=%v err=%v", wrote, err)
		}
	})
}

// Model a peer that accepts the metadata operation but cannot complete it.
type delayedMetadataClient struct {
	cacheclient.CacheClient
	deadline time.Time
}

func (c *delayedMetadataClient) Put(ctx context.Context, key string, body []byte, ttl int64) error {
	if key != MakeMetaKey("bucket", "object") {
		return c.CacheClient.Put(ctx, key, body, ttl)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("metadata commit has no deadline")
	}
	c.deadline = deadline
	<-ctx.Done()
	return ctx.Err()
}

func TestLegacyCoordinator_CommitCannotOutlivePublicationWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := config.NewDefault()
		client := &delayedMetadataClient{CacheClient: cacheclient.NewMemoryCache()}
		store := NewCacheWithClient(client, &cfg.Cache)
		ctx := context.Background()
		_, expected, _, err := store.GetMetaWithVersion(ctx, "bucket", "object")
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Unix(0, int64(expected)).Add(time.Duration(TombstoneTTLSeconds(cfg.Cache.SizeThreshold)-tombstoneMarginSeconds) * time.Second)
		time.Sleep(time.Until(deadline) - time.Second)
		meta := &CachedObjectMeta{Bucket: "bucket", Key: "object", ETag: `"old"`, StatusCode: 200}
		wrote, err := store.PutMetaIfVersion(ctx, "bucket", "object", meta, 60, expected)
		if wrote || !errors.Is(err, context.DeadlineExceeded) || !client.deadline.Equal(deadline) {
			t.Fatalf("commit: wrote=%v err=%v deadline=%v, want bounded by %v", wrote, err, client.deadline, deadline)
		}
		if _, found, err := store.GetMeta(ctx, "bucket", "object"); err != nil || found {
			t.Fatalf("failed commit became visible: found=%v err=%v", found, err)
		}
	})
}
