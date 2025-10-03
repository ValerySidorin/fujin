package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/ValerySidorin/fujin/internal/api/fujin/ferr"
	"github.com/ValerySidorin/fujin/internal/api/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/common/assert"
	"github.com/quic-go/quic-go"
)

var (
	ErrNilHandler = errors.New("handler is nil")
)

const (
	readBufferSize = 512
)

type inbound struct {
	str *quic.Stream
	h   *handler

	ftt time.Duration // force terminate timeout

	l *slog.Logger
}

func newInbound(str *quic.Stream, ftt time.Duration, h *handler, l *slog.Logger) *inbound {
	assert.NotNil(h)
	assert.NotNil(l)

	return &inbound{
		str: str,
		h:   h,
		ftt: ftt,
		l:   l,
	}
}

func (i *inbound) readLoop(ctx context.Context) {
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
			i.str.CancelRead(ferr.ConnErr)
			i.l.Error("read stream", "err", err)
			break
		}

		err := i.h.handle(buf[:n])
		if err != nil {
			if !errors.Is(err, ErrClose) {
				i.l.Error("handle buf", "err", err)
				i.str.CancelRead(ferr.ConnErr)
			}
			break
		}
		buf = buf[:0]

		if i.h.stopRead {
			i.str.CancelRead(ferr.NoErr)
			break
		}
	}
}

func (i *inbound) waitAndDisconnect() {
	i.h.enqueueStop()
	time.Sleep(i.ftt)
	i.h.disconnect()
	i.close()
}

func (i *inbound) close() {
	i.str.CancelRead(ferr.NoErr)
	i.h.wg.Wait()
	i.h.out.Close()
	<-i.h.closed
	i.h.flushBufs()
}
