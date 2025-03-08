package config

import (
	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	"github.com/ValerySidorin/fujin/connector/impl/nats"
	"github.com/ValerySidorin/fujin/connector/protocol"
)

type Config struct {
	Protocol protocol.Protocol    `yaml:"protocol"`
	Kafka    kafka.ReaderConfig   `yaml:"kafka"`
	Nats     nats.ReaderConfig    `yaml:"nats"`
	AMQP091  amqp091.ReaderConfig `yaml:"amqp091"`
	Reusable bool                 `yaml:"-"`
}
