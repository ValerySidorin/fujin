package config

import (
	"github.com/ValerySidorin/fujin/mq/impl/kafka"
	"github.com/ValerySidorin/fujin/mq/impl/nats"
)

type Config struct {
	Protocol string             `yaml:"protocol"`
	Kafka    kafka.ReaderConfig `yaml:"kafka"`
	Nats     nats.ReaderConfig  `yaml:"nats"`
	Reusable bool               `yaml:"-"`
}
