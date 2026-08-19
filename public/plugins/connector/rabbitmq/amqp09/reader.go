package amqp09

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Reader struct {
	conf ConnectorConfig
	mu   sync.Mutex

	conn       *amqp.Connection
	channel    *amqp.Channel
	autoCommit bool

	l *slog.Logger
}

func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	if conf.Consume == nil {
		return nil, fmt.Errorf("rabbitmq_amqp09: consume config is required")
	}
	if conf.Ack == nil {
		conf.Ack = &AckConfig{Multiple: false}
	}
	if conf.Nack == nil {
		conf.Nack = &NackConfig{Multiple: false, Requeue: false}
	}

	conn, err := amqp.DialConfig(conf.Conn.URL, amqp.Config{
		Vhost:      conf.Conn.Vhost,
		ChannelMax: conf.Conn.ChannelMax,
		FrameSize:  conf.Conn.FrameSize,
		Heartbeat:  conf.Conn.Heartbeat,
	})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq_amqp09: dial config: %w", err)
	}

	return &Reader{
		conf:       conf,
		conn:       conn,
		autoCommit: autoCommit,
		l:          l,
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	r.mu.Lock()
	if r.channel != nil {
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: reader busy")
	}

	if err := r.checkConnAndReconnectIfNeeded(); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: check conn and reconnect if needed: %w", err)
	}

	var err error
	r.channel, err = r.conn.Channel()
	if err != nil {
		_ = r.conn.Close()
		r.conn = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: open channel: %w", err)
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
		_ = r.channel.Close()
		r.channel = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: declare exchange: %w", err)
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
		_ = r.channel.Close()
		r.channel = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: declare queue: %w", err)
	}

	if err = r.channel.QueueBind(
		queue.Name,
		r.conf.QueueBind.RoutingKey,
		r.conf.Exchange.Name,
		r.conf.QueueBind.NoWait,
		r.conf.QueueBind.Args,
	); err != nil {
		_ = r.channel.Close()
		r.channel = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: queue bind: %w", err)
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
	if err := ready(); err != nil {
		return err
	}

	var handler func(d amqp.Delivery)
	if r.AutoCommit() {
		handler = func(d amqp.Delivery) {
			h(d.Body, d.Exchange)
		}
	} else {
		handler = func(d amqp.Delivery) {
			h(d.Body, d.Exchange, d.DeliveryTag)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for d := range msgs {
			handler(d)
		}
	}()

	<-ctx.Done()
	r.mu.Lock()
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			r.l.Error("close channel", "err", err)
		}
		r.channel = nil
	}
	r.mu.Unlock()
	wg.Wait()

	return nil
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	r.mu.Lock()
	if r.channel != nil {
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: reader busy")
	}

	if err := r.checkConnAndReconnectIfNeeded(); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: check conn and reconnect if needed: %w", err)
	}

	var err error
	r.channel, err = r.conn.Channel()
	if err != nil {
		_ = r.conn.Close()
		r.conn = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: open channel: %w", err)
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
		_ = r.channel.Close()
		r.channel = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: declare exchange: %w", err)
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
		_ = r.channel.Close()
		r.channel = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: declare queue: %w", err)
	}

	if err = r.channel.QueueBind(
		queue.Name,
		r.conf.QueueBind.RoutingKey,
		r.conf.Exchange.Name,
		r.conf.QueueBind.NoWait,
		r.conf.QueueBind.Args,
	); err != nil {
		_ = r.channel.Close()
		r.channel = nil
		r.mu.Unlock()
		return fmt.Errorf("rabbitmq_amqp09: queue bind: %w", err)
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
	if err := ready(); err != nil {
		return err
	}

	var handler func(d amqp.Delivery)
	if r.AutoCommit() {
		handler = func(d amqp.Delivery) {
			// Extract headers from d.Headers
			var hs [][]byte
			if d.Headers != nil {
				for k, v := range d.Headers {
					keyBytes := []byte(k)
					var valueBytes []byte
					switch val := v.(type) {
					case string:
						valueBytes = []byte(val)
					case []byte:
						valueBytes = val
					default:
						continue
					}
					hs = append(hs, keyBytes, valueBytes)
				}
			}
			h(d.Body, d.Exchange, hs)
		}
	} else {
		handler = func(d amqp.Delivery) {
			var hs [][]byte
			if d.Headers != nil {
				for k, v := range d.Headers {
					keyBytes := []byte(k)
					var valueBytes []byte
					switch val := v.(type) {
					case string:
						valueBytes = []byte(val)
					case []byte:
						valueBytes = val
					default:
						continue
					}
					hs = append(hs, keyBytes, valueBytes)
				}
			}
			h(d.Body, d.Exchange, hs, d.DeliveryTag)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for d := range msgs {
			handler(d)
		}
	}()

	<-ctx.Done()
	r.mu.Lock()
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			r.l.Error("close channel", "err", err)
		}
		r.channel = nil
	}
	r.mu.Unlock()
	wg.Wait()

	return nil
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
	ackHandler(nil)
	r.mu.Lock()
	ch := r.channel
	r.mu.Unlock()
	for _, msgID := range msgIDs {
		if err := connector.ValidateMessageIDPayload(msgID, r.MsgIDArgsLen(), false); err != nil {
			ackMsgHandler(msgID, fmt.Errorf("rabbitmq_amqp09: ack: %w", err))
			continue
		}
		ackMsgHandler(msgID, ch.Ack(binary.BigEndian.Uint64(msgID), r.conf.Ack.Multiple))
	}
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	r.mu.Lock()
	ch := r.channel
	r.mu.Unlock()
	for _, msgID := range msgIDs {
		if err := connector.ValidateMessageIDPayload(msgID, r.MsgIDArgsLen(), false); err != nil {
			nackMsgHandler(msgID, fmt.Errorf("rabbitmq_amqp09: nack: %w", err))
			continue
		}
		nackMsgHandler(msgID, ch.Nack(binary.BigEndian.Uint64(msgID), r.conf.Nack.Multiple, r.conf.Nack.Requeue))
	}
}

func (r *Reader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	return binary.BigEndian.AppendUint64(buf, args[0].(uint64))
}

func (r *Reader) MsgIDArgsLen() int {
	return 8
}

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel != nil {
		r.channel.Close()
		r.channel = nil
	}
	if r.conn != nil {
		r.conn.Close()
	}
	return nil
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
			return fmt.Errorf("rabbitmq_amqp09: dial config on reconnect: %w", err)
		}
	}

	return nil
}
