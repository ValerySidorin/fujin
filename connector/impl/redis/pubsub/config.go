package pubsub

import (
	"time"

	"github.com/ValerySidorin/fujin/connector/cerr"
)

type ReaderConfig struct {
	InitAddress []string `yaml:"init_address"`
	Username    string   `yaml:"username"`
	Password    string   `yaml:"password"`
	Channel     string   `yaml:"channel"`
}

type WriterConfig struct {
	InitAddress []string `yaml:"init_address"`
	Username    string   `yaml:"username"`
	Password    string   `yaml:"password"`
	Channel     string   `yaml:"channel"`

	BatchSize int           `yaml:"batch_size"`
	Linger    time.Duration `yaml:"linger"`

	Endpoint string `yaml:"-"` // Used to compare writers that can be shared in tx
}

func (c ReaderConfig) Validate() error {
	if len(c.InitAddress) == 0 {
		return cerr.ValidationErr("init_address is required")
	}
	if c.Channel == "" {
		return cerr.ValidationErr("channel is required")
	}

	return nil
}

func (c WriterConfig) Validate() error {
	if len(c.InitAddress) == 0 {
		return cerr.ValidationErr("init_address is required")
	}
	if c.Channel == "" {
		return cerr.ValidationErr("channel is required")
	}

	if c.BatchSize <= 0 {
		return cerr.ValidationErr("batch_size must be greater than 0")
	}

	if c.Linger <= 0 {
		return cerr.ValidationErr("linger must be greater than 0")
	}

	return nil
}
