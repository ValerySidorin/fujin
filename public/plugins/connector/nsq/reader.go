package nsq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	"github.com/nsqio/go-nsq"
)

// Reader implements connector.ReadCloser for NSQ
type Reader struct {
	conf                   ConnectorConfig
	consumer               *nsq.Consumer
	handler                func(msg *nsq.Message, h func(message []byte, source string, args ...any))
	msgs                   sync.Map // msgID string -> *nsq.Message
	autoAck                bool
	connectThroughLookupds bool
	l                      *slog.Logger
}

// NewReader creates a new NSQ reader
func NewReader(conf ConnectorConfig, autoAck bool, l *slog.Logger) (connector.ReadCloser, error) {
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
		l:        l.With("reader_type", "nsq"),
	}

	if autoAck {
		reader.handler = func(msg *nsq.Message, h func([]byte, string, ...any)) {
			h(msg.Body, "")
			msg.Finish()
		}
	} else {
		reader.handler = func(msg *nsq.Message, h func([]byte, string, ...any)) {
			reader.msgs.Store(string(msg.ID[:]), msg)
			h(msg.Body, "", msg.ID)
		}
	}

	if len(conf.LookupdAddresses) > 0 {
		reader.connectThroughLookupds = true
	}

	return reader, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func([]byte, string, ...any)) error {
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
	if err := ready(); err != nil {
		r.consumer.Stop()
		<-r.consumer.StopChan
		return err
	}

	<-ctx.Done()
	r.consumer.Stop()
	<-r.consumer.StopChan
	return nil
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	r.consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		if r.autoAck {
			h(msg.Body, "", nil)
			msg.Finish()
		} else {
			r.msgs.Store(string(msg.ID[:]), msg)
			h(msg.Body, "", nil, msg.ID)
		}
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
	if err := ready(); err != nil {
		r.consumer.Stop()
		<-r.consumer.StopChan
		return err
	}

	<-ctx.Done()
	r.consumer.Stop()
	<-r.consumer.StopChan
	return nil
}

func (r *Reader) Fetch(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) FetchWithHeaders(ctx context.Context, n uint32, fetchHandler func(n uint32, err error), msgHandler func(message []byte, source string, hs [][]byte, args ...any)) {
	fetchHandler(0, util.ErrNotSupported)
}

func (r *Reader) Ack(
	ctx context.Context,
	msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	for _, id := range msgIDs {
		if err := connector.ValidateMessageIDPayload(id, r.MsgIDArgsLen(), false); err != nil {
			ackMsgHandler(id, fmt.Errorf("nsq: ack: %w", err))
			continue
		}
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
		if err := connector.ValidateMessageIDPayload(id, r.MsgIDArgsLen(), false); err != nil {
			nackMsgHandler(id, fmt.Errorf("nsq: nack: %w", err))
			continue
		}
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

func (r *Reader) EncodeMsgID(buf []byte, _ string, args ...any) []byte {
	msgID := args[0].(nsq.MessageID)
	return append(buf, msgID[:]...)
}

func (r *Reader) MsgIDArgsLen() int {
	return 16 // NSQ MessageID is [16]byte
}

func (r *Reader) AutoCommit() bool {
	return r.autoAck
}

func (r *Reader) Close() error {
	r.consumer.Stop()
	<-r.consumer.StopChan
	return nil
}
