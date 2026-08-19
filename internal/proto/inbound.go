package proto

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/internal/common/assert"
	"github.com/fujin-io/fujin/internal/proto/pool"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
)

var (
	ErrNilHandler = errors.New("handler is nil")
)

const (
	initialReadBufferSize = pool.SIZE_SMALL
	maximumReadBufferSize = pool.SIZE_LARGE
	readBufferShrinkAfter = 8
)

type inbound struct {
	str       session.Stream
	h         *Handler
	ftt       time.Duration // force terminate timeout
	abortRead func()        // transport-specific: abort read with error (QUIC: CancelRead(ConnErr))
	closeRead func()        // transport-specific: close read cleanly (QUIC: CancelRead(NoErr))
	l         *slog.Logger
}

func NewInbound(str session.Stream, ftt time.Duration, h *Handler, l *slog.Logger, abortRead, closeRead func()) *inbound {
	assert.NotNil(h)
	assert.NotNil(l)

	return &inbound{
		str:       str,
		h:         h,
		ftt:       ftt,
		abortRead: abortRead,
		closeRead: closeRead,
		l:         l,
	}
}

func (i *inbound) ReadLoop(ctx context.Context) {
	stopCh := make(chan struct{})
	buf := pool.Get(initialReadBufferSize)

	defer func() {
		pool.Put(buf)
		i.h.close()
		i.close()
		close(stopCh)
		i.h.out.BroadcastCond()
	}()

	var (
		n          int
		err        error
		shortReads int
	)

	go func() {
		select {
		case <-ctx.Done():
			i.waitAndDisconnect()
		case <-stopCh:
		}
	}()

	for {
		n, err = i.str.Read(buf[:cap(buf)])
		if n == 0 && err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			i.abortRead()
			i.l.Error("read stream", "err", err)
			break
		}

		err := i.h.handle(buf[:n])
		if err != nil {
			if !errors.Is(err, ErrClose) {
				i.l.Error("handle buf", "err", err)
				i.abortRead()
			}
			break
		}
		buf, shortReads = resizeReadBuffer(buf, n, shortReads)

		if i.h.stopRead {
			i.closeRead()
			break
		}
	}
}

func resizeReadBuffer(buf []byte, bytesRead, shortReads int) ([]byte, int) {
	if bytesRead == cap(buf) {
		if cap(buf) < maximumReadBufferSize {
			next := pool.Get(nextReadBufferSize(cap(buf)))
			pool.Put(buf)
			return next, 0
		}
		return buf, 0
	}
	if cap(buf) == initialReadBufferSize {
		return buf, 0
	}
	shortReads++
	if shortReads < readBufferShrinkAfter {
		return buf, shortReads
	}
	pool.Put(buf)
	return pool.Get(initialReadBufferSize), 0
}

func nextReadBufferSize(size int) int {
	switch size {
	case pool.SIZE_SMALL:
		return pool.SIZE_MEDIUM
	default:
		return pool.SIZE_LARGE
	}
}

func (i *inbound) waitAndDisconnect() {
	i.h.enqueueStop()
	time.Sleep(i.ftt)
	i.closeRead()
}

func (i *inbound) close() {
	i.closeRead()
	i.h.out.Close()
	<-i.h.closed
	i.h.flushBufs()
}
