package amqp1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"unsafe"

	"github.com/Azure/go-amqp"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

type Reader struct {
	conf ConnectorConfig

	conn     *amqp.Conn
	session  *amqp.Session
	receiver *amqp.Receiver

	autoCommit bool

	l *slog.Logger
}

func NewReader(conf ConnectorConfig, autoCommit bool, l *slog.Logger) (*Reader, error) {
	if conf.Receiver == nil {
		return nil, fmt.Errorf("azure_amqp1: receiver config is required")
	}

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
		return nil, fmt.Errorf("azure_amqp1: dial: %w", err)
	}

	session, err := conn.NewSession(context.Background(), &amqp.SessionOptions{
		MaxLinks: conf.Session.MaxLinks,
	})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("azure_amqp1: new session: %w", err)
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
		_ = session.Close(context.Background())
		_ = conn.Close()
		return nil, fmt.Errorf("azure_amqp1: new receiver: %w", err)
	}

	return &Reader{
		conf:       conf,
		conn:       conn,
		session:    session,
		receiver:   receiver,
		autoCommit: autoCommit,
		l:          l,
	}, nil
}

func (r *Reader) Subscribe(ctx context.Context, ready func() error, h func(message []byte, source string, args ...any)) error {
	var handler func(msg *amqp.Message) error
	if r.AutoCommit() {
		handler = func(msg *amqp.Message) error {
			h(msg.GetData(), "")
			return r.receiver.AcceptMessage(ctx, msg)
		}
	} else {
		handler = func(msg *amqp.Message) error {
			h(msg.GetData(), "", GetDeliveryId(msg))
			return nil
		}
	}
	if err := ready(); err != nil {
		return err
	}

	for {
		msg, err := r.receiver.Receive(ctx, nil)
		if err != nil {
			return fmt.Errorf("azure_amqp1: receive: %w", err)
		}
		if err := handler(msg); err != nil {
			return fmt.Errorf("azure_amqp1: handler: %w", err)
		}
	}
}

func (r *Reader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, source string, hs [][]byte, args ...any)) error {
	var handler func(msg *amqp.Message) error
	if r.AutoCommit() {
		handler = func(msg *amqp.Message) error {
			var hs [][]byte
			if msg.ApplicationProperties != nil {
				for k, v := range msg.ApplicationProperties {
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
			h(msg.GetData(), "", hs)
			return r.receiver.AcceptMessage(ctx, msg)
		}
	} else {
		handler = func(msg *amqp.Message) error {
			var hs [][]byte
			if msg.ApplicationProperties != nil {
				for k, v := range msg.ApplicationProperties {
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
			h(msg.GetData(), "", hs, GetDeliveryId(msg))
			return nil
		}
	}
	if err := ready(); err != nil {
		return err
	}

	for {
		msg, err := r.receiver.Receive(ctx, nil)
		if err != nil {
			return fmt.Errorf("azure_amqp1: receive: %w", err)
		}
		if err := handler(msg); err != nil {
			return fmt.Errorf("azure_amqp1: handler: %w", err)
		}
	}
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
	for _, msgID := range msgIDs {
		if err := connector.ValidateMessageIDPayload(msgID, r.MsgIDArgsLen(), false); err != nil {
			ackMsgHandler(msgID, fmt.Errorf("azure_amqp1: ack: %w", err))
			continue
		}
		if r.autoCommit {
			ackMsgHandler(msgID, connector.ErrOperationUnsupported)
			continue
		}
		msg := &amqp.Message{}
		SetDeliveryId(msg, binary.BigEndian.Uint32(msgID))
		ackMsgHandler(msgID, r.receiver.AcceptMessage(ctx, msg))
	}
}

func (r *Reader) Nack(
	ctx context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	for _, msgID := range msgIDs {
		if err := connector.ValidateMessageIDPayload(msgID, r.MsgIDArgsLen(), false); err != nil {
			nackMsgHandler(msgID, fmt.Errorf("azure_amqp1: nack: %w", err))
			continue
		}
		if r.autoCommit {
			nackMsgHandler(msgID, connector.ErrOperationUnsupported)
			continue
		}
		msg := &amqp.Message{}
		SetDeliveryId(msg, binary.BigEndian.Uint32(msgID))
		nackMsgHandler(msgID, r.receiver.ReleaseMessage(ctx, msg))
	}
}

func (r *Reader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	return binary.BigEndian.AppendUint32(buf, args[0].(uint32))
}

func (r *Reader) MsgIDArgsLen() int {
	return 4
}

func (r *Reader) AutoCommit() bool {
	return r.autoCommit
}

func (r *Reader) Close() error {
	var errs []error
	if r.receiver != nil {
		if err := r.receiver.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("close receiver: %w", err))
		}
	}
	if r.session != nil {
		if err := r.session.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("close session: %w", err))
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close conn: %w", err))
		}
	}
	return errors.Join(errs...)
}

// deliveryIDOffset is computed once at init via reflect, so it stays correct
// across go-amqp version bumps (the field is unexported).
var deliveryIDOffset uintptr

func init() {
	t := reflect.TypeOf(amqp.Message{})
	f, ok := t.FieldByName("deliveryID")
	if !ok {
		panic("azure_amqp1: amqp.Message has no deliveryID field — go-amqp version incompatible")
	}
	deliveryIDOffset = f.Offset
}

func SetDeliveryId(msg *amqp.Message, val uint32) {
	ptr := unsafe.Pointer(msg)
	fieldPtr := (*uint32)(unsafe.Pointer(uintptr(ptr) + deliveryIDOffset))
	*fieldPtr = val
}

func GetDeliveryId(msg *amqp.Message) uint32 {
	ptr := unsafe.Pointer(msg)
	fieldPtr := (*uint32)(unsafe.Pointer(uintptr(ptr) + deliveryIDOffset))
	return *fieldPtr
}
