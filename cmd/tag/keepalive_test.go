package main

import (
	"testing"

	"github.com/tigrisdata/ocache/coordinator"
)

func TestCacheGRPCKeepaliveMatchesPeerClients(t *testing.T) {
	policy := cacheGRPCKeepalivePolicy()
	if !policy.PermitWithoutStream || policy.MinTime > coordinator.DefaultRouterConfig().KeepaliveTime {
		t.Fatalf("server rejects OCache peer keepalives: %+v", policy)
	}
}
