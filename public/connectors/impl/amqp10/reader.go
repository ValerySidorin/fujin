//go:build amqp10

package amqp10

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"unsafe"

	"github.com/Azure/go-amqp"
	"github.com/ValerySidorin/fujin/public/connectors/cerr"
)

type Reader struct {
	conf ReaderConfig

	conn     *amqp.Conn
	session  *amqp.Session
	receiver *amqp.Receiver

	autoCommit bool

	l *slog.Logger
}

func NewReader(conf ReaderConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
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

	settlementMode := amqp.ReceiverSettleModeFirst
	if !autoCommit {
		settlementMode = amqp.ReceiverSettleModeSecond
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
		SettlementMode:            &settlementMode,
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

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	var handler func(msg *amqp.Message) error
	if r.IsAutoCommit() {
		handler = func(msg *amqp.Message) error {
			h(msg.GetData(), r.conf.Receiver.Source)
			return r.receiver.AcceptMessage(ctx, msg)
		}
	} else {
		handler = func(msg *amqp.Message) error {
			h(msg.GetData(), r.conf.Receiver.Source, GetDeliveryId(msg))
			return nil
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
		if err := handler(msg); err != nil {
			return fmt.Errorf("amqp10: handler: %w", err)
		}
	}
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
	if r.autoCommit {
		for _, msgID := range msgIDs {
			if r.autoCommit {
				ackMsgHandler(msgID, nil)
				continue
			}

			msg := &amqp.Message{}
			SetDeliveryId(msg, binary.BigEndian.Uint32(msgID))
			ackMsgHandler(msgID, r.receiver.AcceptMessage(ctx, msg))
		}
	}
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	if r.autoCommit {
		for _, msgID := range msgIDs {
			if r.autoCommit {
				nackMsgHandler(msgID, nil)
				continue
			}

			msg := &amqp.Message{}
			SetDeliveryId(msg, binary.BigEndian.Uint32(msgID))
			nackMsgHandler(msgID, r.receiver.ReleaseMessage(ctx, msg))
		}
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
