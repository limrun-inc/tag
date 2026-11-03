package origin

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// completionContext lets validation finish between the consumer's state read
// and cancellation check, making the completion race deterministic.
type completionContext struct {
	context.Context
	once     sync.Once
	complete func()
}

func (c *completionContext) Done() <-chan struct{} {
	c.once.Do(c.complete)
	return c.Context.Done()
}

func (c *completionContext) Err() error {
	c.once.Do(c.complete)
	return c.Context.Err()
}

func TestPartCompletionWinsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &part{changed: make(chan struct{})}
	p.ctx = &completionContext{Context: ctx, complete: func() {
		p.mu.Lock()
		p.complete = true
		p.signal()
		p.mu.Unlock()
		cancel()
	}}
	if err := p.wait(); err != nil {
		t.Fatalf("successful part returned %v", err)
	}
}

func TestPartWaitReturnsFailureOrCancellation(t *testing.T) {
	failure := errors.New("range length mismatch")
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "canceled", want: context.Canceled},
		{name: "failed", err: failure, want: failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			p := &part{ctx: ctx, err: test.err, changed: make(chan struct{})}
			if err := p.wait(); !errors.Is(err, test.want) {
				t.Fatalf("wait returned %v, want %v", err, test.want)
			}
		})
	}
}
