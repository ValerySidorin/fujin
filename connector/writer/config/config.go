package config

import (
	"github.com/ValerySidorin/fujin/connector/impl/amqp091"
	"github.com/ValerySidorin/fujin/connector/impl/kafka"
	"github.com/ValerySidorin/fujin/connector/impl/nats"
	"github.com/ValerySidorin/fujin/connector/protocol"
)

type Config struct {
	Protocol protocol.Protocol    `yaml:"protocol"`
	Kafka    kafka.WriterConfig   `yaml:"kafka"`
	Nats     nats.WriterConfig    `yaml:"nats"`
	AMQP091  amqp091.WriterConfig `yaml:"amqp091"`
}
