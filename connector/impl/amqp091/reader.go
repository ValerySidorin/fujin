//go:build amqp091

package amqp091

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ValerySidorin/fujin/connector/cerr"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Reader struct {
	conf ReaderConfig
	mu   sync.Mutex

	conn       *amqp.Connection
	channel    *amqp.Channel
	autoCommit bool

	l *slog.Logger
}

func NewReader(conf ReaderConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	conn, err := amqp.DialConfig(conf.Conn.URL, amqp.Config{
		Vhost:      conf.Conn.Vhost,
		ChannelMax: conf.Conn.ChannelMax,
		FrameSize:  conf.Conn.FrameSize,
		Heartbeat:  conf.Conn.Heartbeat,
	})
	if err != nil {
		return nil, fmt.Errorf("amqp091: dial config: %w", err)
	}

	return &Reader{
		conf:       conf,
		conn:       conn,
		autoCommit: autoCommit,
		l:          l,
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	r.mu.Lock()
	if r.channel != nil {
		return fmt.Errorf("amqp091: reader busy")
	}

	if err := r.checkConnAndReconnectIfNeeded(); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("amqp091: check conn and reconnect if needed: %w", err)
	}

	var err error
	r.channel, err = r.conn.Channel()
	if err != nil {
		_ = r.conn.Close()
		r.mu.Unlock()
		return fmt.Errorf("amqp091: open channel: %w", err)
	}

	if err = r.channel.ExchangeDeclare(
		r.conf.Exchange.Name,
		r.conf.Exchange.Kind,
		r.conf.Exchange.Durable,
		r.conf.Exchange.AutoDelete,
		r.conf.Exchange.Internal,
		r.conf.Exchange.NoWait,
		r.conf.Exchange.Args,
	); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("amqp091: declare exchange: %w", err)
	}

	queue, err := r.channel.QueueDeclare(
		r.conf.Queue.Name,
		r.conf.Queue.Durable,
		r.conf.Queue.AutoDelete,
		r.conf.Queue.Exclusive,
		r.conf.Queue.NoWait,
		r.conf.Queue.Args,
	)
	if err != nil {
		r.mu.Unlock()
		return fmt.Errorf("amqp091: declare queue: %w", err)
	}

	if err = r.channel.QueueBind(
		queue.Name,
		r.conf.QueueBind.RoutingKey,
		r.conf.Exchange.Name,
		r.conf.QueueBind.NoWait,
		r.conf.QueueBind.Args,
	); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("amqp091: queue bind: %w", err)
	}

	r.mu.Unlock()

	msgs, err := r.channel.Consume(
		r.conf.Queue.Name,
		r.conf.Consume.Consumer,
		r.autoCommit,
		r.conf.Consume.Exclusive,
		r.conf.Consume.NoLocal,
		r.conf.Consume.NoWait,
		r.conf.Consume.Args,
	)
	if err != nil {
		return err
	}

	var handler func(d amqp.Delivery)
	if r.IsAutoCommit() {
		handler = func(d amqp.Delivery) {
			h(d.Body, d.Exchange)
		}
	} else {
		handler = func(d amqp.Delivery) {
			h(d.Body, d.Exchange, d.DeliveryTag)
		}
	}

	go func() {
		for d := range msgs {
			handler(d)
		}
	}()

	<-ctx.Done()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.channel.Close(); err != nil {
		r.l.Error("close channel", "err", err)
	}
	r.channel = nil

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
	ctx context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	for _, msgID := range msgIDs {
		ackMsgHandler(msgID, r.channel.Ack(binary.BigEndian.Uint64(msgID), r.conf.Ack.Multiple))
	}
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	for _, msgID := range msgIDs {
		nackMsgHandler(msgID, r.channel.Nack(binary.BigEndian.Uint64(msgID), r.conf.Nack.Multiple, r.conf.Nack.Requeue))
	}
}

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return binary.BigEndian.AppendUint32(buf, uint32(args[0].(int64)))
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 8
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}

func (r *Reader) checkConnAndReconnectIfNeeded() error {
	if r.conn.IsClosed() {
		var err error
		r.conn, err = amqp.DialConfig(r.conf.Conn.URL, amqp.Config{
			Vhost:      r.conf.Conn.Vhost,
			ChannelMax: r.conf.Conn.ChannelMax,
			FrameSize:  r.conf.Conn.FrameSize,
			Heartbeat:  r.conf.Conn.Heartbeat,
		})
		if err != nil {
			return fmt.Errorf("amqp091: dial config  on reconnect: %w", err)
		}
	}

	return nil
}
