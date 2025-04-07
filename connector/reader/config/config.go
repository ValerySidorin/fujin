package config

import (
	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	nats_streaming "github.com/ValerySidorin/fujin/connector/impl/nats/streaming"
	redis_pubsub "github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	"github.com/ValerySidorin/fujin/connector/protocol"
)

type Config struct {
	Protocol      protocol.Protocol           `yaml:"protocol"`
	Kafka         kafka.ReaderConfig          `yaml:"kafka"`
	NatsStreaming nats_streaming.ReaderConfig `yaml:"nats_streaming"`
	AMQP091       amqp091.ReaderConfig        `yaml:"amqp091"`
	AMQP10        amqp10.ReaderConfig         `yaml:"amqp10"`
	RedisPubSub   redis_pubsub.ReaderConfig   `yaml:"redis_pubsub"`
	Reusable      bool                        `yaml:"-"`
}
