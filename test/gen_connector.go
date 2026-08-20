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
	if err := connector.Register("gen", genDescriptor()); err != nil {
		panic(fmt.Sprintf("register gen connector: %v", err))
	}
}

// GenConfig configures the gen connector.
type GenConfig struct {
	MsgSize        int    `yaml:"msg_size"`
	SubscribeLimit uint64 `yaml:"subscribe_limit"`
}

// genConnector is immutable compiled test configuration.
type genConnector struct {
	msgSize        int
	subscribeLimit uint64
}

func newGenConnector(raw any) (*genConnector, error) {
	config := GenConfig{MsgSize: 32}
	switch value := raw.(type) {
	case GenConfig:
		config = value
	case map[string]any:
		if msgSize, ok := integerSetting(value["msg_size"]); ok {
			config.MsgSize = int(msgSize)
		}
		if subscribeLimit, ok := integerSetting(value["subscribe_limit"]); ok {
			config.SubscribeLimit = subscribeLimit
		}
	}
	if config.MsgSize <= 0 {
		config.MsgSize = 1
	}
	return &genConnector{msgSize: config.MsgSize, subscribeLimit: config.SubscribeLimit}, nil
}

func integerSetting(value any) (uint64, bool) {
	switch value := value.(type) {
	case int:
		return uint64(value), value >= 0
	case uint64:
		return value, true
	case float64:
		return uint64(value), value >= 0
	default:
		return 0, false
	}
}

func genDescriptor() connector.Descriptor {
	return connector.Descriptor{Compile: func(raw any) (connector.Compiled, error) {
		compiled, err := newGenConnector(raw)
		if err != nil {
			return nil, err
		}
		profile := connector.RouteProfile{
			Headers:          true,
			Subscribe:        true,
			Fetch:            true,
			ManualSettlement: true,
			Settlement:       connector.SettlementProfile{Ack: connector.AckSingle, Nack: connector.NackDrop},
		}
		return connector.CompileStatic(
			map[string]connector.RouteProfile{"sub": profile},
			map[string]connector.RouteFactory{"sub": {Reader: compiled.NewReader}},
		)
	}}
}

func (g *genConnector) NewReader(autoSettle bool, _ *slog.Logger) (connector.ReadCloser, error) {
	return &genReader{msg: sizedBytes(g.msgSize), autoCommit: autoSettle, subscribeLimit: g.subscribeLimit}, nil
}

// genReader generates messages in a tight loop until context is cancelled.
type genReader struct {
	msg             []byte
	headers         [][]byte
	autoCommit      bool
	fetchDoneFirst  bool
	subscribeLimit  uint64
	subscribePermit <-chan struct{}
	subscribeStart  <-chan struct{}
	ackDoneFirst    bool
	nextID          atomic.Uint32
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
func (r *genReader) waitForSubscribePermit(ctx context.Context) bool {
	if r.subscribePermit == nil {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-r.subscribePermit:
		return true
	}
}

func (r *genReader) Subscribe(ctx context.Context, ready func() error, h func([]byte, string, ...any)) error {
	if err := ready(); err != nil {
		return err
	}
	if !r.waitForSubscribeStart(ctx) {
		return nil
	}
	for emitted := uint64(0); ; emitted++ {
		if r.subscribeLimit > 0 && emitted >= r.subscribeLimit {
			<-ctx.Done()
			return nil
		}
		if !r.waitForSubscribePermit(ctx) {
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

func (r *genReader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, headers [][]byte, args ...any)) error {
	if err := ready(); err != nil {
		return err
	}
	if !r.waitForSubscribeStart(ctx) {
		return nil
	}
	for emitted := uint64(0); ; emitted++ {
		if r.subscribeLimit > 0 && emitted >= r.subscribeLimit {
			<-ctx.Done()
			return nil
		}
		if !r.waitForSubscribePermit(ctx) {
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

var _ connector.ReadCloser = (*genReader)(nil)
