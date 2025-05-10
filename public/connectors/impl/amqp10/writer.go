//go:build amqp10

package amqp10

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Azure/go-amqp"
	"github.com/ValerySidorin/fujin/public/connectors/cerr"
)

type Writer struct {
	conf *WriterConfig

	conn    *amqp.Conn
	session *amqp.Session
	sender  *amqp.Sender

	l *slog.Logger
}

func NewWriter(conf *WriterConfig, l *slog.Logger) (*Writer, error) {
	conn, err := amqp.Dial(context.Background(), conf.Conn.Addr, &amqp.ConnOptions{
		ContainerID:  conf.Conn.ContainerID,
		HostName:     conf.Conn.HostName,
		IdleTimeout:  conf.Conn.IdleTimeout,
		MaxFrameSize: conf.Conn.MaxFrameSize,
		MaxSessions:  conf.Conn.MaxSessions,
		Properties:   conf.Conn.Properties,
		WriteTimeout: conf.Conn.WriteTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("amqp10: dial: %w", err)
	}

	session, err := conn.NewSession(context.Background(), &amqp.SessionOptions{
		MaxLinks: conf.Session.MaxLinks,
	})
	if err != nil {
		return nil, fmt.Errorf("amqp10: new session: %w", err)
	}

	sender, err := session.NewSender(context.Background(), conf.Sender.Target, &amqp.SenderOptions{
		Capabilities:                conf.Sender.Capabilities,
		Durability:                  conf.Sender.Durability,
		DynamicAddress:              conf.Sender.DynamicAddress,
		DesiredCapabilities:         conf.Sender.DesiredCapabilities,
		ExpiryPolicy:                conf.Sender.ExpiryPolicy,
		ExpiryTimeout:               conf.Sender.ExpiryTimeout,
		Name:                        conf.Sender.Name,
		Properties:                  conf.Sender.Properties,
		RequestedReceiverSettleMode: conf.Sender.RequestedReceiverSettleMode,
		SettlementMode:              conf.Sender.SettlementMode,
		SourceAddress:               conf.Sender.SourceAddress,
		TargetCapabilities:          conf.Sender.TargetCapabilities,
		TargetDurability:            conf.Sender.TargetDurability,
		TargetExpiryPolicy:          conf.Sender.TargetExpiryPolicy,
		TargetExpiryTimeout:         conf.Sender.TargetExpiryTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("amqp10: new sender: %w", err)
	}

	return &Writer{
		conf:    conf,
		conn:    conn,
		session: session,
		sender:  sender,
		l:       l,
	}, nil
}

func (w *Writer) Write(ctx context.Context, msg []byte, callback func(err error)) {
	callback(
		w.sender.Send(ctx, amqp.NewMessage(msg), &amqp.SendOptions{
			Settled: w.conf.Send.Settled,
		}),
	)
}

func (w *Writer) Flush(ctx context.Context) error {
	return nil
}

func (w *Writer) BeginTx(ctx context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) CommitTx(ctx context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) RollbackTx(ctx context.Context) error {
	return cerr.ErrNotSupported
}

func (w *Writer) Endpoint() string {
	return w.conf.Conn.Addr
}

func (w *Writer) Close() error {
	if w.sender != nil {
		if err := w.sender.Close(context.Background()); err != nil {
			w.l.Error("amqp10: close sender", "err", err)
		}
	}
	if w.session != nil {
		if err := w.session.Close(context.Background()); err != nil {
			w.l.Error("amqp10: close session", "err", err)
		}
	}
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}
