package fujin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/ValerySidorin/fujin/internal/server/fujin/pool"
	"github.com/quic-go/quic-go"
)

const (
	readBufferSize = 512
)

type inbound struct {
	str quic.Stream
	h   *handler

	ftt time.Duration // force terminate timeout

	l *slog.Logger
}

func newInbound(str quic.Stream, ftt time.Duration, h *handler, l *slog.Logger) *inbound {
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
		close(stopCh)
		i.close()
		i.h.out.c.Broadcast()

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

			i.l.Error("read stream", "err", err)
			break
		}

		err := i.h.handle(buf[:n])
		if err != nil {
			i.l.Error("handle buf", "err", err)
			break
		}
		buf = buf[:0]

		if i.h.stopRead {
			i.str.CancelRead(0x0)
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
	i.str.CancelRead(0x0)
	i.h.wg.Wait()
	i.h.out.close()
	<-i.h.closed
}
