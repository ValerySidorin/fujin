package faker

import (
	"context"
	"log/slog"
	"time"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
)

type Reader struct {
	autoCommit bool
	l          *slog.Logger
}

func NewReader(autoCommit bool, l *slog.Logger) (*Reader, error) {
	l.Info("faker reader initialized")
	return &Reader{
		autoCommit: autoCommit,
		l:          l.With("reader_type", "faker"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	r.l.Info("fake subscribe")
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

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

func (r *Reader) SubscribeH(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	r.l.Info("fake subscribe with headers")
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

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

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) FetchH(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(cerr.ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(cerr.ErrNotSupported)
}

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return buf
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 0
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() {}
