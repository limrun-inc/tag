// Package origin fetches large objects in bounded, version-pinned ranges.
package origin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigrisdata/tag/metrics"
)

const (
	// PartSize and Concurrency are the initial measured policy. Keep storage
	// blocks independent: this reader still emits one whole-object byte stream.
	PartSize     = 16 << 20
	Concurrency  = 4
	maxExtra     = 2
	maxAttempts  = 3
	maxRequests  = 64
	trafficBurst = 64 << 20
)

// Budget shares reservations with cache population and block-read staging.
type Budget interface {
	TryAcquire(int64) bool
	Release(int64)
}

// Fetch must preserve the authorized object and send Range and If-Match directly
// to the origin. Extra attempts use a separate, reusable connection pool.
type Fetch func(ctx context.Context, start, end int64, etag string, extra bool) (*http.Response, error)

type policy struct {
	partSize                            int64
	hedgeDelay, attemptTimeout, timeout time.Duration
}

type Reader struct {
	budget    Budget
	slots     chan struct{}
	policy    policy
	trafficMu sync.Mutex
	traffic   int64
}

func New(budget Budget) *Reader {
	return &Reader{budget: budget, slots: make(chan struct{}, maxRequests), traffic: trafficBurst,
		policy: policy{PartSize, 2 * time.Second, 30 * time.Second, 5 * time.Minute}}
}

// Wrap leaves unsupported responses untouched. Admission never delays the
// original stream, and every admitted body must be closed by its caller.
func (r *Reader) Wrap(req *http.Request, resp *http.Response, fetch Fetch) {
	if fetch == nil || req.Method != http.MethodGet || req.Header.Get("Range") != "" ||
		resp.StatusCode != http.StatusOK || resp.ContentLength <= r.policy.partSize ||
		resp.Uncompressed || resp.Header.Get("Content-Encoding") != "" ||
		resp.Header.Get("Accept-Ranges") != "bytes" || !strongETag(resp.Header.Get("ETag")) {
		return
	}
	reservation := int64(Concurrency+maxExtra) * r.policy.partSize
	if !r.budget.TryAcquire(reservation) {
		metrics.OriginRangeEvents.WithLabelValues("memory_fallback").Inc()
		return
	}
	acquired := 0
	for acquired < Concurrency {
		if !r.acquire() {
			break
		}
		acquired++
	}
	if acquired != Concurrency {
		for range acquired {
			r.release()
		}
		r.budget.Release(reservation)
		metrics.OriginRangeEvents.WithLabelValues("request_fallback").Inc()
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), r.policy.timeout)
	b := &body{engine: r, ctx: ctx, cancel: cancel, fetch: fetch, size: resp.ContentLength,
		etag: resp.Header.Get("ETag"), reservation: reservation, initial: resp.Body,
		buffers: make(chan []byte, Concurrency+maxExtra), extra: make(chan struct{}, maxExtra),
		parts: make(chan *part, Concurrency), consumed: make(chan struct{}, Concurrency),
		scheduled: make(chan struct{}), started: time.Now()}
	for range Concurrency + maxExtra {
		b.buffers <- nil
	}
	// Closing the initial HTTP body also interrupts a Read whose request context
	// predates this wrapper's deadline.
	b.stopInitial = context.AfterFunc(ctx, func() { b.initial.Close() })
	metrics.OriginRangeEvents.WithLabelValues("admitted").Inc()
	metrics.OriginRangeReservedBytes.Add(float64(reservation))
	go b.schedule()
	// A consumer can stop calling Read while blocked on its own output. Release
	// origin resources at the deadline even if it has not reached Close yet.
	context.AfterFunc(ctx, func() { b.Close() })
	resp.Body = b
}

func strongETag(v string) bool {
	return len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' &&
		!strings.ContainsAny(v[1:len(v)-1], "\"\r\n")
}

func (r *Reader) acquire() bool {
	select {
	case r.slots <- struct{}{}:
		return true
	default:
		return false
	}
}
func (r *Reader) release() { <-r.slots }

// Charge the entire extra range before starting it. Across this process, extra
// requested bytes cannot exceed 25% of delivered bytes plus one 64 MiB burst.
func (r *Reader) spend(n int64) bool {
	r.trafficMu.Lock()
	defer r.trafficMu.Unlock()
	if r.traffic < n {
		return false
	}
	r.traffic -= n
	return true
}
func (r *Reader) earn(n int64) {
	r.trafficMu.Lock()
	r.traffic = min(trafficBurst, r.traffic+n/4)
	r.trafficMu.Unlock()
}

type body struct {
	engine            *Reader
	ctx               context.Context
	cancel            context.CancelFunc
	fetch             Fetch
	size              int64
	etag              string
	reservation       int64
	initial           io.ReadCloser
	stopInitial       func() bool
	buffers           chan []byte
	extra             chan struct{}
	parts             chan *part
	consumed          chan struct{}
	scheduled         chan struct{}
	workers           sync.WaitGroup
	readMu            sync.Mutex
	once              sync.Once
	failed            bool
	current           *part
	offset, delivered int64
	head              atomic.Int64
	bestRate          atomic.Int64
	received          atomic.Int64
	extraBytes        atomic.Int64
	started           time.Time
}

func (b *body) spendExtra(n int64) bool {
	limit := min(int64(trafficBurst), max(b.engine.policy.partSize, b.size/4))
	for used := b.extraBytes.Load(); used+n <= limit; used = b.extraBytes.Load() {
		if b.extraBytes.CompareAndSwap(used, used+n) {
			if b.engine.spend(n) {
				return true
			}
			b.extraBytes.Add(-n)
			return false
		}
	}
	return false
}

func (b *body) schedule() {
	defer close(b.scheduled)
	defer close(b.parts)
	for start, index := int64(0), 0; start < b.size; start, index = start+b.engine.policy.partSize, index+1 {
		if index >= Concurrency {
			select {
			case <-b.consumed:
			case <-b.ctx.Done():
				return
			}
		}
		if b.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithCancel(b.ctx)
		p := &part{body: b, start: start, end: min(b.size, start+b.engine.policy.partSize) - 1,
			ctx: ctx, cancel: cancel, changed: make(chan struct{}), joined: make(chan struct{})}
		b.workers.Add(1)
		go func() { defer b.workers.Done(); p.run() }()
		select {
		case b.parts <- p:
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *body) Read(dst []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		if b.delivered == b.size {
			b.finish()
			return 0, io.EOF
		}
		if b.ctx.Err() != nil {
			b.finish()
			return 0, b.ctx.Err()
		}
		if b.current == nil {
			select {
			case p, ok := <-b.parts:
				if !ok {
					b.finish()
					return 0, b.ctx.Err()
				}
				b.current, b.offset = p, 0
			case <-b.ctx.Done():
				continue
			}
		}
		p := b.current
		p.mu.Lock()
		changed := p.changed
		if p.err != nil {
			err := p.err
			p.mu.Unlock()
			b.failed = true
			b.finish()
			return 0, err
		}
		if a := p.selected; a != nil && a.available.Load() > b.offset {
			n := copy(dst, a.buf[b.offset:a.available.Load()])
			p.mu.Unlock()
			b.offset += int64(n)
			b.delivered += int64(n)
			b.engine.earn(int64(n))
			// The final bytes are not acknowledged until the last range has
			// passed exact-length validation, even when they arrived earlier.
			if b.delivered == b.size {
				if err := p.wait(); err != nil {
					b.failed = true
					b.finish()
					return n, err
				}
			}
			return n, nil
		}
		complete := p.complete
		p.mu.Unlock()
		if complete && b.offset == p.length() {
			p.cancel()
			<-p.joined
			p.recycle()
			b.current = nil
			b.head.Store(p.end + 1)
			b.consumed <- struct{}{}
			continue
		}
		select {
		case <-changed:
		case <-b.ctx.Done():
		}
	}
}

// Context lets a producer include this body's deadline in downstream waits.
func (b *body) Context() context.Context { return b.ctx }

func (b *body) Close() error {
	b.cancel()
	b.readMu.Lock()
	defer b.readMu.Unlock()
	b.finish()
	return nil
}

func (b *body) finish() {
	b.once.Do(func() {
		b.cancel()
		b.initial.Close()
		b.stopInitial()
		<-b.scheduled
		b.workers.Wait()
		b.engine.budget.Release(b.reservation)
		for range Concurrency {
			b.engine.release()
		}
		metrics.OriginRangeReservedBytes.Sub(float64(b.reservation))
		metrics.OriginRangeBytes.WithLabelValues("received").Add(float64(b.received.Load()))
		metrics.OriginRangeBytes.WithLabelValues("delivered").Add(float64(b.delivered))
		metrics.OriginRangeBytes.WithLabelValues("discarded").Add(float64(max(0, b.received.Load()-b.delivered)))
		result := "complete"
		if b.delivered != b.size || b.failed {
			result = "incomplete"
		}
		metrics.OriginRangeDuration.WithLabelValues(result).Observe(time.Since(b.started).Seconds())
		b.current = nil
		b.parts = nil
		b.buffers = nil
	})
}

type attempt struct {
	buf          []byte
	available    atomic.Int64
	progress     atomic.Int64
	started      time.Time
	sampledAt    time.Time
	sampledBytes int64
	rate         float64
	err          error
	terminal     bool
	extra        bool
}

type part struct {
	body       *body
	start, end int64
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	selected   *attempt
	complete   bool
	err        error
	changed    chan struct{}
	joined     chan struct{}
}

func (p *part) length() int64 { return p.end - p.start + 1 }
func (p *part) signal()       { close(p.changed); p.changed = make(chan struct{}) }
func (p *part) wait() error {
	for {
		p.mu.Lock()
		complete, err, changed := p.complete, p.err, p.changed
		p.mu.Unlock()
		if err != nil || complete {
			return err
		}
		if p.ctx.Err() != nil {
			return p.ctx.Err()
		}
		select {
		case <-changed:
		case <-p.ctx.Done():
			// Completion cancels this part's losing attempts. Recheck its
			// result before treating that internal cancellation as a failure.
		}
	}
}
func (p *part) recycle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.selected != nil {
		p.body.buffers <- p.selected.buf
		p.selected = nil
	}
}

func (p *part) run() {
	defer close(p.joined)
	defer p.cancel()
	results := make(chan *attempt, maxAttempts)
	active := make(map[*attempt]struct{}, maxAttempts)
	var workers sync.WaitGroup
	defer workers.Wait()
	launched := 0
	lastLaunch := time.Time{}
	launch := func(extra bool) bool {
		b := p.body
		if extra {
			select {
			case b.extra <- struct{}{}:
			default:
				return false
			}
			if !b.engine.acquire() {
				<-b.extra
				return false
			}
		}
		var buf []byte
		select {
		case buf = <-b.buffers:
		default:
			if extra {
				b.engine.release()
				<-b.extra
			}
			return false
		}
		if extra && !b.spendExtra(p.length()) {
			b.buffers <- buf
			b.engine.release()
			<-b.extra
			return false
		}
		if buf == nil {
			buf = make([]byte, b.engine.policy.partSize)
		}
		a := &attempt{buf: buf[:p.length()], started: time.Now(), extra: extra}
		a.sampledAt = a.started
		a.progress.Store(a.started.UnixNano())
		active[a] = struct{}{}
		launched++
		if extra {
			metrics.OriginRangeEvents.WithLabelValues("extra_attempt").Inc()
		}
		lastLaunch = a.started
		workers.Add(1)
		go func() {
			defer workers.Done()
			if extra {
				defer func() { b.engine.release(); <-b.extra }()
			}
			p.fetch(a, extra)
			results <- a
		}()
		return true
	}
	if !launch(false) {
		p.fail(errors.New("origin range buffer unavailable"))
		return
	}
	ticker := time.NewTicker(p.body.engine.policy.hedgeDelay / 4)
	defer ticker.Stop()
	// All workers must relinquish their buffers before this part's reservation
	// can be reused. A losing HTTP request is canceled, closed, and joined.
	defer func() {
		p.cancel()
		for len(active) > 0 {
			a := <-results
			delete(active, a)
			p.discard(a)
		}
	}()
	var lastErr error
	for {
		select {
		case a := <-results:
			delete(active, a)
			if a.err == nil {
				p.mu.Lock()
				p.selected, p.complete = a, true
				p.signal()
				p.mu.Unlock()
				rate := int64(float64(p.length()) / time.Since(a.started).Seconds())
				for old := p.body.bestRate.Load(); rate > old; old = p.body.bestRate.Load() {
					if p.body.bestRate.CompareAndSwap(old, rate) {
						break
					}
				}
				metrics.OriginRangeEvents.WithLabelValues("part_complete").Inc()
				if a.extra {
					metrics.OriginRangeEvents.WithLabelValues("extra_won").Inc()
				}
				return
			}
			lastErr = a.err
			p.discard(a)
			if a.terminal {
				p.fail(a.err)
				return
			}
			if len(active) == 0 && (launched == maxAttempts || !launch(true)) {
				p.fail(lastErr)
				return
			}
		case now := <-ticker.C:
			if launched == maxAttempts || now.Sub(lastLaunch) < p.body.engine.policy.hedgeDelay {
				continue
			}
			// A completed peer supplies the comparison. Without one, hedge only
			// a stalled leading range, not a uniformly slow group of ranges.
			best := p.body.bestRate.Load()
			slow := true
			for a := range active {
				n := a.available.Load()
				if elapsed := now.Sub(a.sampledAt); elapsed >= p.body.engine.policy.hedgeDelay {
					a.rate = float64(n-a.sampledBytes) / elapsed.Seconds()
					a.sampledAt, a.sampledBytes = now, n
				}
				stalled := now.Sub(time.Unix(0, a.progress.Load())) >= p.body.engine.policy.hedgeDelay
				if n == p.length() || (!stalled && n > 0 && (best == 0 || a.rate >= float64(best)/3 ||
					float64(p.length()-n)/a.rate < 1.5*float64(p.length())/float64(best))) {
					slow = false
				}
			}
			if best == 0 && p.start != p.body.head.Load() {
				slow = false
			}
			if slow && launch(true) {
				metrics.OriginRangeEvents.WithLabelValues("hedge").Inc()
			}
		case <-p.ctx.Done():
			p.fail(p.ctx.Err())
			return
		}
	}
}

func (p *part) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
	metrics.OriginRangeEvents.WithLabelValues("part_failed").Inc()
	p.signal()
}
func (p *part) discard(a *attempt) {
	p.mu.Lock()
	if p.selected == a {
		p.selected = nil
		p.signal()
	}
	p.mu.Unlock()
	p.body.buffers <- a.buf[:cap(a.buf)]
}

func (p *part) fetch(a *attempt, extra bool) {
	b := p.body
	ctx, cancel := context.WithTimeout(p.ctx, b.engine.policy.attemptTimeout)
	defer cancel()
	var stream io.ReadCloser
	initial := p.start == 0 && !extra
	if initial {
		stream = b.initial
	} else {
		resp, err := b.fetch(ctx, p.start, p.end, b.etag, extra)
		if err != nil {
			a.err = err
			return
		}
		stream = resp.Body
		want := fmt.Sprintf("bytes %d-%d/%d", p.start, p.end, b.size)
		if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != want ||
			resp.ContentLength != p.length() || resp.Header.Get("ETag") != b.etag ||
			resp.Uncompressed || resp.Header.Get("Content-Encoding") != "" {
			stream.Close()
			a.err = fmt.Errorf("invalid origin range response: status %d", resp.StatusCode)
			// Invalid metadata and non-transient HTTP errors cannot be repaired
			// by racing more reads. Never splice another version into the stream.
			a.terminal = resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests
			return
		}
	}
	stop := context.AfterFunc(ctx, func() { stream.Close() })
	defer stop()
	defer stream.Close()
	p.mu.Lock()
	if p.selected == nil {
		p.selected = a
		p.signal()
	}
	p.mu.Unlock()
	var n int64
	for n < p.length() {
		got, err := stream.Read(a.buf[n:min(p.length(), n+64<<10)])
		n += int64(got)
		b.received.Add(int64(got))
		a.available.Store(n)
		if got > 0 {
			a.progress.Store(time.Now().UnixNano())
		}
		p.mu.Lock()
		// A validated faster attempt can supply the next bytes immediately.
		// Waiting for its entire range would turn a successful hedge into an
		// avoidable downstream idle period. Exact length still gates completion.
		if !p.complete && (p.selected == nil || n > p.selected.available.Load()) {
			p.selected = a
		}
		p.signal()
		p.mu.Unlock()
		if err != nil {
			if err == io.EOF && n == p.length() {
				break
			}
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			a.err = err
			return
		}
		if ctx.Err() != nil {
			a.err = ctx.Err()
			return
		}
	}
	if !initial {
		var probe [1]byte
		n, err := stream.Read(probe[:])
		b.received.Add(int64(n))
		if n != 0 || err != io.EOF {
			a.err = errors.New("origin range length mismatch")
			return
		}
	}
}
