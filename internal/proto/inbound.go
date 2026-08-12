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
	readBufferSize = 512
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
	buf := pool.Get(readBufferSize)

	defer func() {
		pool.Put(buf)
		i.h.close()
		i.close()
		close(stopCh)
		i.h.out.BroadcastCond()
	}()

	var (
		n   int
		err error
	)

	go func() {
		select {
		case <-ctx.Done():
			i.waitAndDisconnect()
		case <-stopCh:
		}
	}()

	for {
		n, err = i.str.Read(buf[:readBufferSize])
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
		buf = buf[:0]

		if i.h.stopRead {
			i.closeRead()
			break
		}
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
