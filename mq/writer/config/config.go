package config

import "github.com/ValerySidorin/fujin/mq/impl/kafka"

type Config struct {
	Protocol string             `yaml:"protocol"`
	Kafka    kafka.WriterConfig `yaml:"kafka"`
}
