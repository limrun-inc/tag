package broadcast

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClientBackpressureIgnoresCacheCapacity(t *testing.T) {
	b := NewBroadcaster(1)
	b.SetHeaders(200, nil)
	client, cache := b.Subscribe(), b.SubscribeCache()
	b.Broadcast([]byte("first"))
	chunk := <-cache.Chunks()
	chunk.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	waited, err := b.WaitForClientCapacity(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || waited <= 0 {
		t.Fatalf("wait=%s err=%v", waited, err)
	}
	chunk = <-client.Chunks()
	client.Consumed()
	chunk.Release()
	if _, err := b.WaitForClientCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.Broadcast([]byte("second"))
	b.Complete(nil)
	for _, l := range []*Listener{client, cache} {
		for chunk := range l.Chunks() {
			chunk.Release()
		}
		if l.Err() != nil {
			t.Fatal(l.Err())
		}
	}
}

func TestFastClientKeepsBroadcastMoving(t *testing.T) {
	b := NewBroadcaster(1)
	b.SetHeaders(200, nil)
	slow, fast := b.Subscribe(), b.Subscribe()
	b.Broadcast([]byte("first"))
	chunk := <-fast.Chunks()
	fast.Consumed()
	chunk.Release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if waited, err := b.WaitForClientCapacity(ctx); err != nil || waited != 0 {
		t.Fatalf("fast client blocked: %v", err)
	}
	b.Broadcast([]byte("second"))
	b.Complete(nil)
	for chunk := range slow.Chunks() {
		chunk.Release()
	}
	if !errors.Is(slow.Err(), ErrSlowConsumer) {
		t.Fatal("slow peer was not disconnected")
	}
	for chunk := range fast.Chunks() {
		chunk.Release()
	}
	if fast.Err() != nil {
		t.Fatal(fast.Err())
	}
}

func TestBackpressureWakesAfterClientConsumes(t *testing.T) {
	b := NewBroadcaster(1)
	b.SetHeaders(200, nil)
	l := b.Subscribe()
	b.Broadcast([]byte("first"))
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _, err := b.WaitForClientCapacity(ctx); done <- err }()
	chunk := <-l.Chunks()
	l.Consumed()
	chunk.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	b.Complete(nil)
	for chunk := range l.Chunks() {
		chunk.Release()
	}
}
