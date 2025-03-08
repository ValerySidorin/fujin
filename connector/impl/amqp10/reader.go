package amqp10

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/Azure/go-amqp"
	"github.com/ValerySidorin/fujin/connector/cerr"
)

type Reader struct {
	conf ReaderConfig

	conn     *amqp.Conn
	session  *amqp.Session
	receiver *amqp.Receiver

	l *slog.Logger
}

func NewReader(conf ReaderConfig, l *slog.Logger) (*Reader, error) {
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

	receiver, err := session.NewReceiver(context.Background(), conf.Receiver.Source, &amqp.ReceiverOptions{
		Credit:                    conf.Receiver.Credit,
		Durability:                conf.Receiver.Durability,
		DynamicAddress:            conf.Receiver.DynamicAddress,
		ExpiryPolicy:              conf.Receiver.ExpiryPolicy,
		ExpiryTimeout:             conf.Receiver.ExpiryTimeout,
		Filters:                   conf.Receiver.Filters,
		Name:                      conf.Receiver.Name,
		Properties:                conf.Receiver.Properties,
		RequestedSenderSettleMode: conf.Receiver.RequestedSenderSettleMode,
		SettlementMode:            conf.Receiver.SettlementMode,
	})
	if err != nil {
		return nil, fmt.Errorf("amqp10: new receiver: %w", err)
	}

	return &Reader{
		conf:     conf,
		conn:     conn,
		session:  session,
		receiver: receiver,
		l:        l,
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, args ...any) error) error {
	var handler func(msg *amqp.Message) error
	fmt.Println(r.IsAutoCommit())
	if r.IsAutoCommit() {
		handler = func(msg *amqp.Message) error {
			_ = h(msg.GetData())
			return r.receiver.AcceptMessage(ctx, msg)
		}
	} else {
		handler = func(msg *amqp.Message) error {
			return h(msg.GetData(), GetDeliveryId(msg))
		}
	}

	for {
		msg, err := r.receiver.Receive(ctx, nil)
		if err != nil {
			return fmt.Errorf("amqp10: receive: %w", err)
		}
		if err := r.receiver.AcceptMessage(ctx, msg); err != nil {
			return err
		}
		_ = handler(msg)
	}
}

func (r *Reader) Consume(ctx context.Context, ch <-chan struct{}, n uint32, h func(message []byte, args ...any) error) error {
	return cerr.ErrNotSupported
}

func (r *Reader) Ack(ctx context.Context, meta []byte) error {
	if r.IsAutoCommit() {
		return nil
	}

	msg := &amqp.Message{}
	SetDeliveryId(msg, binary.BigEndian.Uint32(meta))
	return r.receiver.AcceptMessage(ctx, msg)
}

func (r *Reader) Nack(ctx context.Context, meta []byte) error {
	if r.IsAutoCommit() {
		return nil
	}

	msg := &amqp.Message{}
	SetDeliveryId(msg, binary.BigEndian.Uint32(meta))
	return r.receiver.ReleaseMessage(ctx, msg)
}

func (r *Reader) MessageMetaLen() byte {
	if r.IsAutoCommit() {
		return 0
	}
	return 4
}

func (r *Reader) EncodeMeta(buf []byte, args ...any) []byte {
	return binary.BigEndian.AppendUint32(buf, uint32(args[0].(uint32)))
}

func (r *Reader) IsAutoCommit() bool {
	return r.conf.Receiver.SettlementMode == nil || *r.conf.Receiver.SettlementMode == amqp.ReceiverSettleModeFirst
}

func (r *Reader) Close() {
	if r.receiver != nil {
		if err := r.receiver.Close(context.Background()); err != nil {
			r.l.Error("amqp10: close receiver", "err", err)
		}
	}
	if r.session != nil {
		if err := r.session.Close(context.Background()); err != nil {
			r.l.Error("amqp10: close session", "err", err)
		}
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}

func SetDeliveryId(msg *amqp.Message, val uint32) {
	ptr := unsafe.Pointer(msg)
	fieldPtr := (*uint32)(unsafe.Pointer(uintptr(ptr) + 84))
	*fieldPtr = val
}

func GetDeliveryId(msg *amqp.Message) uint32 {
	ptr := unsafe.Pointer(msg)
	fieldPtr := (*uint32)(unsafe.Pointer(uintptr(ptr) + 84))
	return *fieldPtr
}
