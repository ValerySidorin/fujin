package config

import (
	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/impl/nats"
	"github.com/ValerySidorin/fujin/mq/protocol"
)

type Config struct {
	Protocol protocol.Protocol  `yaml:"protocol"`
	Kafka    kafka.WriterConfig `yaml:"kafka"`
	Nats     nats.WriterConfig  `yaml:"nats"`
}
