package faker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

var ErrNotSupported = errors.New("operation not supported")

type Reader struct {
	autoCommit bool
	l          *slog.Logger
}

func NewReader(autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	l.Info("faker reader initialized")
	return &Reader{
		autoCommit: autoCommit,
		l:          l.With("reader_type", "faker"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	r.l.Info("fake subscribe")
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	if err := ready(); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-t.C:
				r.l.Info("fake read")
				h([]byte("fake read"), "")
			case <-ctx.Done():
				return
			}
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	r.l.Info("fake subscribe with headers")
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	if err := ready(); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-t.C:
				r.l.Info("fake read")
				h([]byte("fake read"), "", [][]byte{[]byte("fake"), []byte("header")})
			case <-ctx.Done():
				return
			}
		}
	}()

	<-ctx.Done()
	return nil
}

func (r *Reader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	fetchHandler(0, ErrNotSupported)
}

func (r *Reader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	fetchHandler(0, ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(ErrNotSupported)
}

func (r *Reader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	return buf
}

func (r *Reader) MsgIDArgsLen() int {
	return 0
}

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	return nil
}
