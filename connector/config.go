package connector

import (
	"fmt"

	"github.com/ValerySidorin/fujin/connector/protocol"
	reader "github.com/ValerySidorin/fujin/connector/reader/config"
	writer "github.com/ValerySidorin/fujin/connector/writer/config"
)

type Config struct {
	Readers map[string]reader.Config `yaml:"readers"`
	Writers map[string]writer.Config `yaml:"writers"`
}

func (c *Config) Validate() error {
	for _, r := range c.Readers {
		switch r.Protocol {
		case protocol.Kafka:
			if err := r.Kafka.Validate(); err != nil {
				return fmt.Errorf("validate reader config: %w", err)
			}
		case protocol.Nats:
			if err := r.Nats.Validate(); err != nil {
				return fmt.Errorf("validate reader config: %w", err)
			}
		case protocol.AMQP091:
			if err := r.AMQP091.Validate(); err != nil {
				return fmt.Errorf("validate reader config: %w", err)
			}
		}
	}

	for _, r := range c.Writers {
		switch r.Protocol {
		case protocol.Kafka:
			if err := r.Kafka.Validate(); err != nil {
				return fmt.Errorf("validate reader config: %w", err)
			}
		case protocol.Nats:
			if err := r.Nats.Validate(); err != nil {
				return fmt.Errorf("validate reader config: %w", err)
			}
		case protocol.AMQP091:
			if err := r.AMQP091.Validate(); err != nil {
				return fmt.Errorf("validate writer config: %w", err)
			}
		}
	}

	return nil
}
