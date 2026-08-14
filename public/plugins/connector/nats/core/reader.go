package core

import (
	"context"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/nats-io/nats.go"
)

// Reader implements connector.ReadCloser for NATS Core
type Reader struct {
	conf       ConnectorConfig
	autoCommit bool
	nc         *nats.Conn
	l          *slog.Logger
}

// NewReader creates a new NATS Core reader
func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}

	return &Reader{
		conf:       conf,
		autoCommit: autoCommit,
		nc:         nc,
		l:          l.With("reader_type", "nats_core"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		h(msg.Data, msg.Subject)
	})
	if err != nil {
		return fmt.Errorf("nats: subscribe: %w", err)
	}
	if err := r.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return fmt.Errorf("nats: flush subscription: %w", err)
	}
	if err := ready(); err != nil {
		_ = sub.Unsubscribe()
		return err
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			r.l.Error("unsubscribe", "err", err)
		}
	}()
	<-ctx.Done()
	return nil
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, headers [][]byte, args ...any)) error {
	sub, err := r.nc.Subscribe(r.conf.Subject, func(msg *nats.Msg) {
		h(msg.Data, msg.Subject, natsHeadersToSlice(msg.Header))
	})
	if err != nil {
		return fmt.Errorf("nats: subscribe: %w", err)
	}
	if err := r.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return fmt.Errorf("nats: flush subscription: %w", err)
	}
	if err := ready(); err != nil {
		_ = sub.Unsubscribe()
		return err
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			r.l.Error("unsubscribe", "err", err)
		}
	}()
	<-ctx.Done()
	return nil
}
func natsHeadersToSlice(headers nats.Header) [][]byte {
	var result [][]byte
	for key, values := range headers {
		for _, value := range values {
			result = append(result,
				unsafe.Slice((*byte)(unsafe.StringData(key)), len(key)),
				unsafe.Slice((*byte)(unsafe.StringData(value)), len(value)),
			)
		}
	}
	return result
}

func (r *Reader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	// NATS Core doesn't support acknowledgments (at-most-once delivery)
	ackHandler(util.ErrNotSupported)
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	// NATS Core doesn't support acknowledgments (at-most-once delivery)
	nackHandler(util.ErrNotSupported)
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
	r.nc.Close()
	return nil
}

// joinBytes joins byte slices with a separator
func joinBytes(elems [][]byte, sep byte) []byte {
	switch len(elems) {
	case 0:
		return nil
	case 1:
		out := make([]byte, len(elems[0]))
		copy(out, elems[0])
		return out
	}

	totalLen := len(elems) - 1
	for _, e := range elems {
		totalLen += len(e)
	}

	out := make([]byte, totalLen)

	pos := copy(out, elems[0])
	for _, e := range elems[1:] {
		pos += copy(out[pos:], []byte{sep})
		pos += copy(out[pos:], e)
	}

	return out
}
