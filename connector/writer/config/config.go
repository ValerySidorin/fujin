package config

import (
	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/amqp10"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	"github.com/ValerySidorin/fujin/connector/impl/mqtt"
	nats_core "github.com/ValerySidorin/fujin/connector/impl/nats/core"
	redis_pubsub "github.com/ValerySidorin/fujin/connector/impl/redis/pubsub"
	redis_streams "github.com/ValerySidorin/fujin/connector/impl/redis/streams"
	"github.com/ValerySidorin/fujin/connector/protocol"
)

type Config struct {
	Protocol     protocol.Protocol          `yaml:"protocol"`
	Kafka        kafka.WriterConfig         `yaml:"kafka"`
	NatsCore     nats_core.WriterConfig     `yaml:"nats_core"`
	AMQP091      amqp091.WriterConfig       `yaml:"amqp091"`
	AMQP10       amqp10.WriterConfig        `yaml:"amqp10"`
	RedisPubSub  redis_pubsub.WriterConfig  `yaml:"redis_pubsub"`
	RedisStreams redis_streams.WriterConfig `yaml:"redis_streams"`
	MQTT         mqtt.WriterConfig          `yaml:"mqtt"`
}
