package config

import (
	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/impl/nats"
)

type Config struct {
	Protocol string             `yaml:"protocol"`
	Kafka    kafka.WriterConfig `yaml:"kafka"`
	Nats     nats.WriterConfig  `yaml:"nats"`
}
