//go:build mqtt

package mqtt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var ErrMsgNotFound = errors.New("msg not found")

type Reader struct {
	conf    MQTTConfig
	cl      mqtt.Client
	autoAck bool
	msgs    sync.Map
	handler func(msg mqtt.Message, h func([]byte, string, ...any))
	l       *slog.Logger
}

func NewReader(conf MQTTConfig, autoAck bool, l *slog.Logger) (*Reader, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(conf.BrokerURL).
		SetClientID(conf.ClientID).
		SetCleanSession(conf.CleanSession).
		SetKeepAlive(conf.KeepAlive).
		SetAutoAckDisabled(!autoAck)

	r := &Reader{
		conf:    conf,
		autoAck: autoAck,
		l:       l.With("reader_type", "mqtt"),
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqtt: connect: %w", token.Error())
	}
	r.cl = client

	if autoAck {
		r.handler = func(msg mqtt.Message, h func([]byte, string, ...any)) {
			h(msg.Payload(), msg.Topic())
		}
	} else {
		r.handler = func(msg mqtt.Message, h func([]byte, string, ...any)) {
			r.msgs.Store(msg.MessageID(), msg)
			h(msg.Payload(), msg.Topic(), msg.MessageID())
		}
	}

	return r, nil
}

func (r *Reader) Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error {
	sub := r.cl.Subscribe(r.conf.Topic, r.conf.QoS, func(_ mqtt.Client, msg mqtt.Message) {
		r.handler(msg, h)
	})

	if sub.Wait() && sub.Error() != nil {
		return sub.Error()
	}

	return nil
}

func (r *Reader) SubscribeH(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	sub := r.cl.Subscribe(r.conf.Topic, r.conf.QoS, func(_ mqtt.Client, msg mqtt.Message) {
		if r.autoAck {
			h(msg.Payload(), msg.Topic(), nil)
		} else {
			r.msgs.Store(msg.MessageID(), msg)
			h(msg.Payload(), msg.Topic(), nil, msg.MessageID())
		}
	})

	if sub.Wait() && sub.Error() != nil {
		return sub.Error()
	}

	return nil
}

func (r *Reader) Fetch(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) FetchH(
	ctx context.Context, n uint32,
	fetchHandler func(n uint32, err error),
	msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
) {
	fetchHandler(0, cerr.ErrNotSupported)
}

func (r *Reader) Ack(
	_ context.Context, msgIDs [][]byte,
	ackHandler func(error),
	ackMsgHandler func([]byte, error),
) {
	ackHandler(nil)
	for _, id := range msgIDs {
		msg, ok := r.msgs.LoadAndDelete(binary.BigEndian.Uint16(id))
		if ok {
			msg.(mqtt.Message).Ack()
			ackMsgHandler(id, nil)
			continue
		}
		ackMsgHandler(id, ErrMsgNotFound)
	}
}

func (r *Reader) Nack(
	_ context.Context, msgIDs [][]byte,
	nackHandler func(error),
	nackMsgHandler func([]byte, error),
) {
	nackHandler(nil)
	for _, id := range msgIDs {
		nackMsgHandler(id, nil)
	}
}

func (r *Reader) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return binary.BigEndian.AppendUint16(buf, args[0].(uint16))
}

func (r *Reader) MsgIDStaticArgsLen() int {
	return 2
}

func (r *Reader) IsAutoCommit() bool {
	return r.autoAck
}

func (r *Reader) Close() {
	r.cl.Disconnect(uint(r.conf.DisconnectTimeout))
}
