package pubsub

import (
	"github.com/ValerySidorin/fujin/public/connectors/cerr"
	"github.com/ValerySidorin/fujin/public/connectors/impl/redis/config"
)

type ReaderConfig struct {
	config.ReaderConfig
	Channels []string `yaml:"channels"`
}

type WriterConfig struct {
	config.WriterConfig
	Channel string `yaml:"channel"`
}

func (c ReaderConfig) Validate() error {
	if err := c.ReaderConfig.Validate(); err != nil {
		return err
	}

	if len(c.Channels) <= 0 {
		return cerr.ValidationErr("at least one channel is required")
	}

	return nil
}

func (c WriterConfig) Validate() error {
	if err := c.WriterConfig.Validate(); err != nil {
		return err
	}

	if c.Channel == "" {
		return cerr.ValidationErr("channel is required")
	}

	return nil
}
