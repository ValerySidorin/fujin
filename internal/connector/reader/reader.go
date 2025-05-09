package reader

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader/config"
)

type Reader interface {
	Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error
	Fetch(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, args ...any),
	)
	Ack(
		ctx context.Context, msgIDs [][]byte,
		ackHandler func(error),
		ackMsgHandler func([]byte, error),
	)
	Nack(
		ctx context.Context, msgIDs [][]byte,
		nackHandler func(error),
		nackMsgHandler func([]byte, error),
	)
	MsgIDStaticArgsLen() int
	EncodeMsgID(buf []byte, topic string, args ...any) []byte
	IsAutoCommit() bool
	Close()
}

type ReaderFactoryFunc func(brokerSpecificConfig any, autoCommit bool, l *slog.Logger) (Reader, error)

var readerFactories = make(map[protocol.Protocol]ReaderFactoryFunc)

func RegisterReaderFactory(p protocol.Protocol, factory ReaderFactoryFunc) {
	readerFactories[p] = factory
}

func New(conf config.Config, autoCommit bool, l *slog.Logger) (Reader, error) {
	factory, ok := readerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported reader protocol: %s (is it compiled in?)", conf.Protocol)
	}

	var brokerConf any
	switch conf.Protocol {
	case protocol.Kafka:
		brokerConf = conf.Kafka
	case protocol.NatsCore:
		brokerConf = conf.NatsCore
	case protocol.AMQP091:
		brokerConf = conf.AMQP091
	case protocol.AMQP10:
		brokerConf = conf.AMQP10
	case protocol.RedisPubSub:
		brokerConf = conf.RedisPubSub
	case protocol.RedisStreams:
		brokerConf = conf.RedisStreams
	case protocol.MQTT:
		brokerConf = conf.MQTT
	case protocol.NSQ:
		brokerConf = conf.NSQ
	default:
		return nil, fmt.Errorf("unknown or unhandled reader protocol: %s", conf.Protocol)
	}

	return factory(brokerConf, autoCommit, l)
}
