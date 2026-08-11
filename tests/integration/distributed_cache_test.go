package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/ocache/embedded"
	tagcache "github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"golang.org/x/sync/errgroup"
)

func reserveClusterAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func TestDistributedCacheRoutesMetadataAndBodyAcrossNodes(t *testing.T) {
	const nodeCount = 3

	clusterAddresses := make([]string, nodeCount)
	grpcAddresses := make([]string, nodeCount)
	for i := range nodeCount {
		clusterAddresses[i] = reserveClusterAddress(t)
		grpcAddresses[i] = reserveClusterAddress(t)
	}

	clients := make([]*embedded.Client, nodeCount)
	for i := range nodeCount {
		client, err := embedded.New(&embedded.Config{
			DiskPath:      t.TempDir(),
			TTL:           time.Hour,
			NodeID:        fmt.Sprintf("node-%d", i),
			ClusterAddr:   clusterAddresses[i],
			GRPCAddr:      grpcAddresses[i],
			AdvertiseAddr: grpcAddresses[i],
			SeedNodes:     clusterAddresses,
			Registerer:    prometheus.NewRegistry(),
		})
		require.NoError(t, err)
		require.NoError(t, client.StartGRPCServer())
		clients[i] = client

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		require.NoError(t, client.WaitReady(ctx))
		cancel()
	}
	t.Cleanup(func() {
		var closeGroup errgroup.Group
		for _, client := range clients {
			client := client
			if client != nil {
				closeGroup.Go(client.Close)
			}
		}
		require.NoError(t, closeGroup.Wait())
	})

	// WaitReady only means each individual node reached ACTIVE. Memberlist still
	// needs time to propagate the complete ring to every node before key
	// ownership is stable across ingress points.
	time.Sleep(5 * time.Second)

	body := bytes.Repeat([]byte("distributed-cache-body"), 4096)
	headers := http.Header{}
	headers.Set("Content-Length", fmt.Sprint(len(body)))
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("ETag", `"distributed-etag"`)
	headers.Set("X-Amz-Checksum-Crc32", "checksum")
	meta := tagcache.MetaFromHTTPHeaders("bucket", "object", http.StatusOK, headers)
	meta.ChecksumMode = true

	writer := tagcache.NewCacheWithClient(clients[0], &config.CacheConfig{TTL: time.Hour})
	require.NoError(t, writer.PutWithMeta(t.Context(), "bucket", "object", meta, body, 0))

	var metadataOwners []int
	for i, client := range clients {
		reader, found, err := client.Storage().Get(tagcache.MakeMetaKey("bucket", "object"), 0, 0)
		require.NoError(t, err)
		if found {
			_, err := io.ReadAll(reader)
			require.NoError(t, err)
			metadataOwners = append(metadataOwners, i)
		}
	}
	require.Len(t, metadataOwners, 1, "metadata must be physically committed to exactly one owner")

	for i, client := range clients {
		reader := tagcache.NewCacheWithClient(client, &config.CacheConfig{TTL: time.Hour})
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			gotMeta, found, err := reader.GetMeta(t.Context(), "bucket", "object")
			require.NoError(collect, err)
			require.True(collect, found, "node %d did not route to physical owner %d", i, metadataOwners[0])
			require.Equal(collect, meta.ETag, gotMeta.ETag)

			var gotBody bytes.Buffer
			require.NoError(collect, reader.GetBodyStream(t.Context(), "bucket", "object", gotMeta.ETag, &gotBody))
			require.Equal(collect, body, gotBody.Bytes())
		}, 15*time.Second, 100*time.Millisecond)
	}
}
