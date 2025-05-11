//go:build amqp091

package amqp091

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Writer struct {
	conf    WriterConfig
	conn    *amqp.Connection
	channel *amqp.Channel
	l       *slog.Logger
}

func NewWriter(conf WriterConfig, l *slog.Logger) (*Writer, error) {
	conn, err := amqp.DialConfig(conf.Conn.URL, amqp.Config{
		Vhost:      conf.Conn.Vhost,
		ChannelMax: conf.Conn.ChannelMax,
		FrameSize:  conf.Conn.FrameSize,
		Heartbeat:  conf.Conn.Heartbeat,
	})
	if err != nil {
		return nil, fmt.Errorf("amqp091: dial config: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("amqp091: open channel: %w", err)
	}

	if err = channel.ExchangeDeclare(
		conf.Exchange.Name,
		conf.Exchange.Kind,
		conf.Exchange.Durable,
		conf.Exchange.AutoDelete,
		conf.Exchange.Internal,
		conf.Exchange.NoWait,
		conf.Exchange.Args,
	); err != nil {
		return nil, fmt.Errorf("amqp091: declare exchange: %w", err)
	}

	queue, err := channel.QueueDeclare(
		conf.Queue.Name,
		conf.Queue.Durable,
		conf.Queue.AutoDelete,
		conf.Queue.Exclusive,
		conf.Queue.NoWait,
		conf.Queue.Args,
	)
	if err != nil {
		return nil, fmt.Errorf("amqp091: declare queue: %w", err)
	}

	if err = channel.QueueBind(
		queue.Name,
		conf.QueueBind.RoutingKey,
		conf.Exchange.Name,
		conf.QueueBind.NoWait,
		conf.QueueBind.Args,
	); err != nil {
		return nil, fmt.Errorf("amqp091: queue bind: %w", err)
	}

	return &Writer{
		conf:    conf,
		conn:    conn,
		channel: channel,
		l:       l,
	}, nil
}

func (w *Writer) Write(ctx context.Context, msg []byte, callback func(err error)) {
	callback(
		w.channel.PublishWithContext(
			ctx,
			w.conf.Exchange.Name,
			w.conf.QueueBind.RoutingKey,
			w.conf.Publish.Mandatory,
			w.conf.Publish.Immediate,
			amqp.Publishing{
				ContentType:     w.conf.Publish.ContentType,
				ContentEncoding: w.conf.Publish.ContentEncoding,
				DeliveryMode:    w.conf.Publish.DeliveryMode,
				Priority:        w.conf.Publish.Priority,
				ReplyTo:         w.conf.Publish.ReplyTo,
				AppId:           w.conf.Publish.AppId,
				Body:            msg,
			}),
	)
}

func (w *Writer) Flush(ctx context.Context) error {
	return nil
}

func (w *Writer) BeginTx(ctx context.Context) error {
	return w.channel.Tx()
}

func (w *Writer) CommitTx(ctx context.Context) error {
	return w.channel.TxCommit()
}

func (w *Writer) RollbackTx(ctx context.Context) error {
	return w.channel.TxRollback()
}

func (w *Writer) Endpoint() string {
	return w.conf.Conn.URL
}

func (w *Writer) Close() error {
	if err := w.channel.Close(); err != nil {
		return fmt.Errorf("close channel: %w", err)
	}
	if err := w.conn.Close(); err != nil {
		return fmt.Errorf("close conn: %w", err)
	}
	return nil
}
