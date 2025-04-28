package config

import (
	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	nats_streaming "github.com/ValerySidorin/fujin/connector/impl/nats/streaming"
	redis_pubsub "github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	redis_streams "github.com/ValerySidorin/fujin/connector/impl/redis/streams"
	"github.com/ValerySidorin/fujin/connector/protocol"
)

type Config struct {
	Protocol      protocol.Protocol           `yaml:"protocol"`
	Kafka         kafka.WriterConfig          `yaml:"kafka"`
	NatsStreaming nats_streaming.WriterConfig `yaml:"nats_streaming"`
	AMQP091       amqp091.WriterConfig        `yaml:"amqp091"`
	AMQP10        amqp10.WriterConfig         `yaml:"amqp10"`
	RedisPubSub   redis_pubsub.WriterConfig   `yaml:"redis_pubsub"`
	RedisStreams  redis_streams.WriterConfig  `yaml:"redis_streams"`
}
