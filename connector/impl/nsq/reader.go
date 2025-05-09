//go:build nsq

package nsq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ValerySidorin/fujin/internal/connector/cerr"
	"github.com/nsqio/go-nsq"
)

type Reader struct {
	conf                   ReaderConfig
	consumer               *nsq.Consumer
	handler                func(msg *nsq.Message, h func(message []byte, topic string, args ...any))
	msgs                   sync.Map // msgID string -> *nsq.Message
	autoAck                bool
	connectThroughLookupds bool
	logger                 *slog.Logger
}

func NewReader(conf ReaderConfig, autoAck bool, l *slog.Logger) (*Reader, error) {
	cfg := nsq.NewConfig()
	cfg.MaxInFlight = conf.MaxInFlight

	consumer, err := nsq.NewConsumer(conf.Topic, conf.Channel, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer: %w", err)
	}

	reader := &Reader{
		conf:     conf,
		consumer: consumer,
		autoAck:  autoAck,
		logger:   l,
	}

	if autoAck {
		reader.handler = func(msg *nsq.Message, h func([]byte, string, ...any)) {
			h(msg.Body, conf.Topic)
			msg.Finish()
		}
	} else {
		reader.handler = func(msg *nsq.Message, h func([]byte, string, ...any)) {
			reader.msgs.Store(msg.ID, msg)
			h(msg.Body, conf.Topic, msg.ID)
		}
	}

	if len(conf.LookupdAddresses) > 0 {
		reader.connectThroughLookupds = true
	}

	return reader, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func([]byte, string, ...any)) error {
	r.consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		r.handler(msg, h)
		return nil
	}))

	if r.connectThroughLookupds {
		if err := r.consumer.ConnectToNSQLookupds(r.conf.LookupdAddresses); err != nil {
			return fmt.Errorf("connect to nsq lookupds: %w", err)
		}
	} else {
		if err := r.consumer.ConnectToNSQDs(r.conf.Addresses); err != nil {
			return fmt.Errorf("connect to nsqds: %w", err)
		}
	}

	<-ctx.Done()
	r.consumer.Stop()
	<-r.consumer.StopChan
	return nil
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context,
	msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	for _, id := range msgIDs {
		if v, ok := r.msgs.LoadAndDelete(string(id)); ok {
			if msg, ok := v.(*nsq.Message); ok {
				msg.Finish()
				ackMsgHandler(id, nil)
				continue
			}
		}
		ackMsgHandler(id, fmt.Errorf("message not found for ack"))
	}
}

func (r *Reader) Nack(
	ctx context.Context,
	msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	for _, id := range msgIDs {
		if v, ok := r.msgs.LoadAndDelete(string(id)); ok {
			if msg, ok := v.(*nsq.Message); ok {
				msg.Requeue(-1)
				nackMsgHandler(id, nil)
				continue
			}
		}
		nackMsgHandler(id, fmt.Errorf("message not found for nack"))
	}
}

func (r *Reader) encodeMsgID(buf []byte, _ string, args ...any) []byte {
	msgID := args[0].(nsq.MessageID)
	return append(buf, msgID[:]...)
}

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return r.encodeMsgID(buf, topic, args...)
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 16
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoAck
}

func (r *Reader) Close() {
	r.consumer.Stop()
	<-r.consumer.StopChan
}
