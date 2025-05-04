package writer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	"github.com/ValerySidorin/fujin/connector/impl/mqtt"
	nats_core "github.com/ValerySidorin/fujin/connector/impl/nats/core"
	redis_pubsub "github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	redis_streams "github.com/ValerySidorin/fujin/connector/impl/redis/streams"
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

func NewWriter(conf config.Config, writerID string, l *slog.Logger) (Writer, error) {
	switch conf.Protocol {
	case protocol.Kafka:
		return kafka.NewWriter(conf.Kafka, writerID, l)
	case protocol.NatsCore:
		return nats_core.NewWriter(conf.NatsCore, l)
	case protocol.AMQP091:
		return amqp091.NewWriter(conf.AMQP091, l)
	case protocol.AMQP10:
		return amqp10.NewWriter(conf.AMQP10, l)
	case protocol.RedisPubSub:
		return redis_pubsub.NewWriter(conf.RedisPubSub, l)
	case protocol.RedisStreams:
		return redis_streams.NewWriter(conf.RedisStreams, l)
	case protocol.MQTT:
		return mqtt.NewWriter(conf.MQTT, l)
	}

	return nil, fmt.Errorf("invalid writer protocol: %s", conf.Protocol)
}
