package jetstream

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Reader implements connector.ReadCloser for NATS JetStream.
type Reader struct {
	conf       ConnectorConfig
	autoCommit bool
	nc         *nats.Conn
	js         jetstream.JetStream
	cons       jetstream.Consumer
	l          *slog.Logger

	// pendingMsgs stores messages by stream sequence for Ack/Nack.
	pendingMsgs sync.Map // uint64 -> jetstream.Msg

	fetching atomic.Bool
}

// NewReader creates a new NATS JetStream reader.
func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	nc, err := nats.Connect(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("nats_jetstream: connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats_jetstream: create jetstream context: %w", err)
	}

	consConfig := jetstream.ConsumerConfig{
		FilterSubject: conf.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       conf.ackWaitDuration(),
	}

	if conf.Consumer != "" {
		consConfig.Durable = conf.Consumer
	}
	if conf.MaxDeliver > 0 {
		consConfig.MaxDeliver = conf.MaxDeliver
	}
	if conf.MaxAckPending > 0 {
		consConfig.MaxAckPending = conf.MaxAckPending
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cons, err := js.CreateOrUpdateConsumer(ctx, conf.Stream, consConfig)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats_jetstream: create consumer: %w", err)
	}

	return &Reader{
		conf:       conf,
		autoCommit: autoCommit,
		nc:         nc,
		js:         js,
		cons:       cons,
		l:          l.With("reader_type", "nats_jetstream"),
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	cc, err := r.cons.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			r.l.Error("failed to get message metadata", "err", err)
			return
		}

		seq := meta.Sequence.Stream

		if r.autoCommit {
			h(msg.Data(), msg.Subject())
			if err := msg.Ack(); err != nil {
				r.l.Error("auto ack failed", "err", err, "seq", seq)
			}
		} else {
			r.pendingMsgs.Store(seq, msg)
			h(msg.Data(), msg.Subject(), seq)
		}
	})
	if err != nil {
		return fmt.Errorf("nats_jetstream: consume: %w", err)
	}
	if err := ready(); err != nil {
		cc.Stop()
		return err
	}

	defer cc.Stop()
	<-ctx.Done()
	return nil
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	cc, err := r.cons.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			r.l.Error("failed to get message metadata", "err", err)
			return
		}

		seq := meta.Sequence.Stream
		hs := natsHeadersToSlice(msg.Headers())

		if r.autoCommit {
			h(msg.Data(), msg.Subject(), hs)
			if err := msg.Ack(); err != nil {
				r.l.Error("auto ack failed", "err", err, "seq", seq)
			}
		} else {
			r.pendingMsgs.Store(seq, msg)
			h(msg.Data(), msg.Subject(), hs, seq)
		}
	})
	if err != nil {
		return fmt.Errorf("nats_jetstream: consume: %w", err)
	}
	if err := ready(); err != nil {
		cc.Stop()
		return err
	}

	defer cc.Stop()
	<-ctx.Done()
	return nil
}

func (r *Reader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	if !r.fetching.CompareAndSwap(false, true) {
		fetchHandler(0, connector.ErrFetchBusy)
		return
	}
	defer r.fetching.Store(false)

	batch, err := r.cons.Fetch(int(n), jetstream.FetchMaxWait(r.conf.fetchMaxWaitDuration()))
	if err != nil {
		fetchHandler(0, fmt.Errorf("nats_jetstream: fetch: %w", err))
		return
	}

	// Collect all messages first so we can send the count header before message payloads.
	type fetchedMsg struct {
		msg jetstream.Msg
		seq uint64
	}
	var collected []fetchedMsg
	for msg := range batch.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			r.l.Error("failed to get message metadata", "err", err)
			continue
		}
		collected = append(collected, fetchedMsg{msg: msg, seq: meta.Sequence.Stream})
	}

	if err := batch.Error(); err != nil {
		fetchHandler(uint32(len(collected)), fmt.Errorf("nats_jetstream: fetch: %w", err))
		return
	}

	fetchHandler(uint32(len(collected)), nil)

	for _, fm := range collected {
		if r.autoCommit {
			msgHandler(fm.msg.Data(), fm.msg.Subject())
			if err := fm.msg.Ack(); err != nil {
				r.l.Error("auto ack failed", "err", err, "seq", fm.seq)
			}
		} else {
			r.pendingMsgs.Store(fm.seq, fm.msg)
			msgHandler(fm.msg.Data(), fm.msg.Subject(), fm.seq)
		}
	}
}

func (r *Reader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	if !r.fetching.CompareAndSwap(false, true) {
		fetchHandler(0, connector.ErrFetchBusy)
		return
	}
	defer r.fetching.Store(false)

	batch, err := r.cons.Fetch(int(n), jetstream.FetchMaxWait(r.conf.fetchMaxWaitDuration()))
	if err != nil {
		fetchHandler(0, fmt.Errorf("nats_jetstream: fetch: %w", err))
		return
	}

	type fetchedMsg struct {
		msg jetstream.Msg
		seq uint64
		hs  [][]byte
	}
	var collected []fetchedMsg
	for msg := range batch.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			r.l.Error("failed to get message metadata", "err", err)
			continue
		}
		collected = append(collected, fetchedMsg{
			msg: msg,
			seq: meta.Sequence.Stream,
			hs:  natsHeadersToSlice(msg.Headers()),
		})
	}

	if err := batch.Error(); err != nil {
		fetchHandler(uint32(len(collected)), fmt.Errorf("nats_jetstream: fetch: %w", err))
		return
	}

	fetchHandler(uint32(len(collected)), nil)

	for _, fm := range collected {
		if r.autoCommit {
			msgHandler(fm.msg.Data(), fm.msg.Subject(), fm.hs)
			if err := fm.msg.Ack(); err != nil {
				r.l.Error("auto ack failed", "err", err, "seq", fm.seq)
			}
		} else {
			r.pendingMsgs.Store(fm.seq, fm.msg)
			msgHandler(fm.msg.Data(), fm.msg.Subject(), fm.hs, fm.seq)
		}
	}
}

func (r *Reader) Ack(
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	// ackHandler must be called first (sends header with count),
	// then ackMsgHandler for each message (sends per-message results).
	ackHandler(nil)

	for _, id := range msgIDs {
		if err := connector.ValidateMessageIDPayload(id, r.MsgIDArgsLen(), false); err != nil {
			ackMsgHandler(id, fmt.Errorf("nats_jetstream: ack: %w", err))
			continue
		}

		seq := binary.BigEndian.Uint64(id[:8])
		raw, ok := r.pendingMsgs.LoadAndDelete(seq)
		if !ok {
			ackMsgHandler(id, fmt.Errorf("nats_jetstream: message not found for seq %d", seq))
			continue
		}

		msg := raw.(jetstream.Msg)
		if err := msg.Ack(); err != nil {
			ackMsgHandler(id, fmt.Errorf("nats_jetstream: ack: %w", err))
			continue
		}
		ackMsgHandler(id, nil)
	}
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	// nackHandler must be called first (sends header with count),
	// then nackMsgHandler for each message (sends per-message results).
	nackHandler(nil)

	for _, id := range msgIDs {
		if err := connector.ValidateMessageIDPayload(id, r.MsgIDArgsLen(), false); err != nil {
			nackMsgHandler(id, fmt.Errorf("nats_jetstream: nack: %w", err))
			continue
		}

		seq := binary.BigEndian.Uint64(id[:8])
		raw, ok := r.pendingMsgs.LoadAndDelete(seq)
		if !ok {
			nackMsgHandler(id, fmt.Errorf("nats_jetstream: message not found for seq %d", seq))
			continue
		}

		msg := raw.(jetstream.Msg)
		if err := msg.Nak(); err != nil {
			nackMsgHandler(id, fmt.Errorf("nats_jetstream: nak: %w", err))
			continue
		}
		nackMsgHandler(id, nil)
	}
}

func (r *Reader) EncodeMsgID(buf []byte, _ string, args ...any) []byte {
	return binary.BigEndian.AppendUint64(buf, args[0].(uint64))
}

func (r *Reader) MsgIDArgsLen() int { return 8 }

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	r.nc.Close()
	return nil
}

// natsHeadersToSlice preserves every duplicate header value.
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
