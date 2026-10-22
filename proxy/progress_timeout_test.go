package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestCacheStreamProgressOutlivesFiveMinutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := bytes.Repeat([]byte("x"), 8*32768)
		stream := func(ctx context.Context, _ string, w io.Writer) error {
			for offset := 0; offset < len(body); offset += 32768 {
				time.Sleep(time.Minute)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if _, err := w.Write(body[offset : offset+32768]); err != nil {
					return err
				}
			}
			return nil
		}
		svc, _, meta := newLargeStreamCacheService(t, body, stream)
		defer svc.Stop()
		w := httptest.NewRecorder()
		if err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now()); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(w.Body.Bytes(), body) {
			t.Fatalf("progressing stream truncated to %d of %d bytes", w.Body.Len(), len(body))
		}
	})
}

func TestCacheStreamIdleTimeoutAfterProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var body bytes.Buffer
		start := time.Now()
		err := readCacheStream(context.Background(), &body, func(ctx context.Context, w io.Writer) error {
			time.Sleep(4 * time.Minute)
			if _, err := io.WriteString(w, "first bytes"); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 9*time.Minute || body.String() != "first bytes" {
			t.Fatalf("err=%v elapsed=%v body=%q", err, time.Since(start), body.String())
		}
		if bodyGone(err) {
			t.Fatal("a stalled peer must not invalidate its cache entry")
		}
	})
}

func TestPopulateProgressAndCommitTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, progress := newProgressContext(context.Background(), cacheWriteTimeout)
		defer progress.close()
		reader := &progressReader{r: strings.NewReader("complete"), p: progress}
		buf := make([]byte, 1)
		for range 8 {
			time.Sleep(30 * time.Second)
			if _, err := reader.Read(buf); err != nil {
				t.Fatal(err)
			}
			if ctx.Err() != nil {
				t.Fatal("progressing populate was canceled")
			}
		}
		if _, err := reader.Read(buf); !errors.Is(err, io.EOF) {
			t.Fatalf("read end = %v", err)
		}
		time.Sleep(2 * cacheWriteTimeout)
		if ctx.Err() != nil {
			t.Fatal("metadata commit after EOF was mistaken for a stalled body")
		}
	})
}

func TestProgressContextHonorsParentCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx, progress := newProgressContext(parent, time.Minute)
		defer progress.close()
		<-ctx.Done()
		if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			t.Fatalf("parent deadline lost: %v", context.Cause(ctx))
		}
	})
}
