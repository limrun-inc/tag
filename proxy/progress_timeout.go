package proxy

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// progressTimer bounds idle time without imposing a total transfer deadline.
// One timer observes successful reads or writes, regardless of object size.
type progressTimer struct {
	mu      sync.Mutex
	last    time.Time
	idle    time.Duration
	stopped bool
	timer   *time.Timer
	cancel  context.CancelCauseFunc
}

func newProgressContext(parent context.Context, idle time.Duration) (context.Context, *progressTimer) {
	ctx, cancel := context.WithCancelCause(parent)
	p := &progressTimer{last: time.Now(), idle: idle, cancel: cancel}
	p.timer = time.AfterFunc(idle, p.expire)
	return ctx, p
}

func (p *progressTimer) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	if remaining := p.idle - time.Since(p.last); remaining > 0 {
		p.timer.Reset(remaining)
		return
	}
	p.stopped = true
	log.Warn().Dur("idle_timeout", p.idle).Msg("Cache stream stopped making progress")
	p.cancel(context.DeadlineExceeded)
}

func (p *progressTimer) progress(n int) {
	if n > 0 {
		p.mu.Lock()
		p.last = time.Now()
		p.mu.Unlock()
	}
}

func (p *progressTimer) stop() {
	p.mu.Lock()
	p.stopped = true
	p.timer.Stop()
	p.mu.Unlock()
}

func (p *progressTimer) close() {
	p.stop()
	p.cancel(nil)
}

type progressReader struct {
	r io.Reader
	p *progressTimer
}

func (r *progressReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	r.p.progress(n)
	if err != nil {
		// The body has ended. Storage may still be committing its metadata.
		r.p.stop()
	}
	return n, err
}

type progressWriter struct {
	w io.Writer
	p *progressTimer
}

func (w *progressWriter) Write(b []byte) (int, error) {
	n, err := w.w.Write(b)
	w.p.progress(n)
	return n, err
}

// readCacheStream retains transient timeout errors while allowing client-paced reads.
func readCacheStream(ctx context.Context, w io.Writer, read func(context.Context, io.Writer) error) error {
	ctx, progress := newProgressContext(ctx, cacheStreamReadTimeout)
	defer progress.close()
	err := read(ctx, &progressWriter{w: w, p: progress})
	if err != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return err
}
