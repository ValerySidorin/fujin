package config

import (
	"time"

	std_tls "crypto/tls"

	"github.com/ValerySidorin/fujin/config/tls"
	"github.com/ValerySidorin/fujin/connector/cerr"
)

type RedisConfig struct {
	InitAddress  []string `yaml:"init_address"`
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	DisableCache bool     `yaml:"disable_cache"`

	TLS *tls.ClientTLSConfig `yaml:"tls"`
}

type ReaderConfig struct {
	RedisConfig `yaml:",inline"`
}

type WriterConfig struct {
	RedisConfig `yaml:",inline"`

	BatchSize int           `yaml:"batch_size"`
	Linger    time.Duration `yaml:"linger"`

	Endpoint string `yaml:"-"` // Used to compare writers that can be shared in tx
}

func (c RedisConfig) Validate() error {
	if len(c.InitAddress) == 0 {
		return cerr.ValidationErr("init_address is required")
	}

	if c.TLS != nil {
		if err := c.TLS.Validate(); err != nil {
			return err
		}

	}

	return nil
}

func (c RedisConfig) TLSConfig() (*std_tls.Config, error) {
	if c.TLS != nil {
		return c.TLS.Parse()
	}

	return nil, nil
}

func (c WriterConfig) Validate() error {
	if c.BatchSize <= 0 {
		return cerr.ValidationErr("batch_size must be greater than 0")
	}

	if c.Linger <= 0 {
		return cerr.ValidationErr("linger must be greater than 0")
	}

	return c.RedisConfig.Validate()
}
