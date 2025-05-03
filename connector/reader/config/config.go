package config

import (
	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	nats_core "github.com/ValerySidorin/fujin/connector/impl/nats/core"
	redis_pubsub "github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	redis_streams "github.com/ValerySidorin/fujin/connector/impl/redis/streams"
	"github.com/ValerySidorin/fujin/connector/protocol"
)

type Config struct {
	Protocol     protocol.Protocol          `yaml:"protocol"`
	Kafka        kafka.ReaderConfig         `yaml:"kafka"`
	NatsCore     nats_core.ReaderConfig     `yaml:"nats_core"`
	AMQP091      amqp091.ReaderConfig       `yaml:"amqp091"`
	AMQP10       amqp10.ReaderConfig        `yaml:"amqp10"`
	RedisPubSub  redis_pubsub.ReaderConfig  `yaml:"redis_pubsub"`
	RedisStreams redis_streams.ReaderConfig `yaml:"redis_streams"`
	Reusable     bool                       `yaml:"-"`
}
