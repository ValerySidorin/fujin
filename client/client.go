package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/quic-go/quic-go"
)

var (
	ErrConnClosed = errors.New("connection closed")
	ErrTimeout    = errors.New("timeout")

	ReadBufferSize = 512
)

type Conn struct {
	qconn quic.Connection

	timeout time.Duration
	closed  atomic.Bool

	l *slog.Logger
}

func Connect(ctx context.Context, addr string, tlsConf *tls.Config, opts ...Option) (*Conn, error) {
	conn, err := quic.DialAddr(ctx, addr, tlsConf, nil)
	if err != nil {
		return nil, fmt.Errorf("quic: dial addr: %w", err)
	}

	l := slog.Default()
	timeout := 10 * time.Second

	c := &Conn{
		qconn:   conn,
		timeout: timeout,
		closed:  atomic.Bool{},
		l:       l,
	}

	for _, opt := range opts {
		opt(c)
	}

	go func() {
		var pingBuf [1]byte

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if c.closed.Load() {
					return
				}
				str, err := conn.AcceptStream(ctx)
				if err != nil {
					c.l.Error("ping: accept stream", "err", err)
					continue
				}

				if err := handlePing(str, pingBuf[:]); err != nil {
					c.l.Error("ping: handle ping", "err", err)
					continue
				}
			}
		}
	}()

	return c, nil
}

func (c *Conn) Close() error {
	// disconnect and close all streams
	c.closed.Store(true)
	if err := c.qconn.CloseWithError(0x0, ""); err != nil {
		return fmt.Errorf("quic: close: %w", err)
	}
	return nil
}

func handlePing(str quic.Stream, buf []byte) error {
	defer str.Close()

	_, err := str.Read(buf[:])
	if err == io.EOF {
		buf[0] = byte(response.RESP_CODE_PONG)
		if _, err := str.Write(buf[:]); err != nil {
			return fmt.Errorf("ping: write pong: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("ping: read: %w", err)
	}

	return nil
}
