package test

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

func init() {
	if err := connector.Register("gen", newGenConnector); err != nil {
		panic(fmt.Sprintf("register gen connector: %v", err))
	}
}

// GenConfig configures the gen connector.
type GenConfig struct {
	MsgSize        int    `yaml:"msg_size"`
	SubscribeLimit uint64 `yaml:"subscribe_limit"`
}

// genConnector generates messages as fast as possible for benchmarking subscribe throughput.
type genConnector struct {
	msgSize        int
	subscribeLimit uint64
}

func newGenConnector(config any, l *slog.Logger) (connector.Connector, error) {
	msgSize := 32 // default
	var subscribeLimit uint64
	if m, ok := config.(GenConfig); ok {
		msgSize = m.MsgSize
		subscribeLimit = m.SubscribeLimit
	}
	if msgSize <= 0 {
		msgSize = 1
	}
	return &genConnector{msgSize: msgSize, subscribeLimit: subscribeLimit}, nil
}

func (g *genConnector) NewReader(config any, name string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	return &genReader{msg: sizedBytes(g.msgSize), autoCommit: autoCommit, subscribeLimit: g.subscribeLimit}, nil
}

func (g *genConnector) NewWriter(config any, name string, l *slog.Logger) (connector.WriteCloser, error) {
	return newWriter(), nil
}

func (g *genConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return func(settingPath, value string) (any, error) { return nil, nil }
}

// genReader generates messages in a tight loop until context is cancelled.
type genReader struct {
	msg            []byte
	headers        [][]byte
	autoCommit     bool
	fetchDoneFirst bool
	subscribeLimit uint64
	subscribeStart <-chan struct{}
	ackDoneFirst   bool
	nextID         atomic.Uint32
}

func (r *genReader) waitForSubscribeStart(ctx context.Context) bool {
	if r.subscribeStart == nil {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-r.subscribeStart:
		return true
	}
}
func (r *genReader) Subscribe(ctx context.Context, h func([]byte, string, ...any)) error {
	if !r.waitForSubscribeStart(ctx) {
		return nil
	}
	for emitted := uint64(0); ; emitted++ {
		if r.subscribeLimit > 0 && emitted >= r.subscribeLimit {
			<-ctx.Done()
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		default:
			id := r.nextID.Add(1)
			if r.autoCommit {
				h(r.msg, "sub")
			} else {
				h(r.msg, "sub", id)
			}
		}
	}
}

func (r *genReader) SubscribeWithHeaders(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	if !r.waitForSubscribeStart(ctx) {
		return nil
	}
	for emitted := uint64(0); ; emitted++ {
		if r.subscribeLimit > 0 && emitted >= r.subscribeLimit {
			<-ctx.Done()
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		default:
			id := r.nextID.Add(1)
			if r.autoCommit {
				h(r.msg, "sub", r.headers)
			} else {
				h(r.msg, "sub", r.headers, id)
			}
		}
	}
}

func (r *genReader) Fetch(_ context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
	if r.fetchDoneFirst {
		fetchHandler(n, nil)
	}
	for range n {
		id := r.nextID.Add(1)
		if r.autoCommit {
			msgHandler(r.msg, "sub")
		} else {
			msgHandler(r.msg, "sub", id)
		}
	}
	if !r.fetchDoneFirst {
		fetchHandler(n, nil)
	}
}

func (r *genReader) FetchWithHeaders(_ context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
	if r.fetchDoneFirst {
		fetchHandler(n, nil)
	}
	for range n {
		id := r.nextID.Add(1)
		if r.autoCommit {
			msgHandler(r.msg, "sub", r.headers)
		} else {
			msgHandler(r.msg, "sub", r.headers, id)
		}
	}
	if !r.fetchDoneFirst {
		fetchHandler(n, nil)
	}
}

func (r *genReader) Ack(_ context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	if r.ackDoneFirst {
		ackHandler(nil)
	}
	for _, id := range msgIDs {
		ackMsgHandler(id, nil)
	}
	if !r.ackDoneFirst {
		ackHandler(nil)
	}
}

func (r *genReader) Nack(_ context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	if r.ackDoneFirst {
		nackHandler(nil)
	}
	for _, id := range msgIDs {
		nackMsgHandler(id, nil)
	}
	if !r.ackDoneFirst {
		nackHandler(nil)
	}
}

func (r *genReader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	buf = append(buf, topic...)
	if len(args) == 0 {
		return buf
	}
	id, _ := args[0].(uint32)
	return binary.BigEndian.AppendUint32(buf, id)
}

func (r *genReader) MsgIDArgsLen() int { return 4 }

func (r *genReader) AutoCommit() bool { return r.autoCommit }

func (r *genReader) Close() error {
	return nil
}

var _ connector.Connector = (*genConnector)(nil)
var _ connector.ReadCloser = (*genReader)(nil)
