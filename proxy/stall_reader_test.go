package proxy

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// pacedReader delivers one byte per interval, forever.
type pacedReader struct{ interval time.Duration }

func (p pacedReader) Read(b []byte) (int, error) {
	time.Sleep(p.interval)
	b[0] = 'x'
	return 1, nil
}

// TestSlowStreamSurvivesTheStallGuard pins that a populate is bounded by progress, not by speed.
//
// The populate reads the same broadcast the clients read, and the client that started the fetch
// paces it, so one slow reader drags the stream down to its own speed. Under the old size-derived
// budget that looked identical to a hang and the write was cancelled mid-object — which is how a
// partial body reached storage in the first place.
func TestSlowStreamSurvivesTheStallGuard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limit := 100 * time.Millisecond
	// Slow enough that any fixed budget for this many bytes would expire, but never silent for
	// as long as limit.
	stalled := newStallReader(pacedReader{interval: limit / 5})
	go cancelWhenStalled(ctx, cancel, stalled, limit)

	if _, err := io.CopyN(io.Discard, stalled, 40); err != nil {
		t.Fatalf("read the slow stream: %v", err)
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("context cancelled after %v of steady progress: %v", 8*limit, err)
	}
}

// TestStalledStreamIsCancelled keeps the other half of the bargain: a stream that stops still fails,
// so a wedged populate cannot hold its slot forever.
func TestStalledStreamIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limit := 100 * time.Millisecond
	stalled := newStallReader(readerThatNeverDelivers{})
	go cancelWhenStalled(ctx, cancel, stalled, limit)

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Errorf("ctx.Err() = %v, want %v", ctx.Err(), context.Canceled)
		}
	case <-time.After(20 * limit):
		t.Error("a stream that delivered nothing was never cancelled")
	}
}

type readerThatNeverDelivers struct{}

func (readerThatNeverDelivers) Read([]byte) (int, error) {
	select {}
}
