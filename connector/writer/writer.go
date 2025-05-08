package writer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/writer/config"
)

type Writer interface {
	Write(ctx context.Context, msg []byte, callback func(err error))
	Flush(ctx context.Context) error
	BeginTx(ctx context.Context) error
	CommitTx(ctx context.Context) error
	RollbackTx(ctx context.Context) error
	Endpoint() string
	Close() error
}

type WriterFactoryFunc func(brokerSpecificConfig any, writerID string, l *slog.Logger) (Writer, error)

var writerFactories = make(map[protocol.Protocol]WriterFactoryFunc)

func RegisterWriterFactory(p protocol.Protocol, factory WriterFactoryFunc) {
	writerFactories[p] = factory
}

func NewWriter(conf config.Config, writerID string, l *slog.Logger) (Writer, error) {
	factory, ok := writerFactories[conf.Protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported writer protocol: %s (is it compiled in?)", conf.Protocol)
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
		// This case should ideally not be reached if the factory lookup succeeded
		// and the protocol is valid, but as a safeguard:
		return nil, fmt.Errorf("unknown or unhandled writer protocol in switch: %s", conf.Protocol)
	}

	return factory(brokerConf, writerID, l)
}
