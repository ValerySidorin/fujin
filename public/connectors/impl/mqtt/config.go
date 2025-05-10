package mqtt

import (
	"time"
)

type MQTTConfig struct {
	BrokerURL         string        `yaml:"broker_url"`
	ClientID          string        `yaml:"client_id"`
	Topic             string        `yaml:"topic"`
	QoS               byte          `yaml:"qos"`
	Retain            bool          `yaml:"retain"`
	CleanSession      bool          `yaml:"clean_session"`
	KeepAlive         time.Duration `yaml:"keep_alive"`
	DisconnectTimeout time.Duration `yaml:"disconnect_timeout"`
}

type PoolConfig struct {
	Size           int           `yaml:"size"`
	PreAlloc       bool          `yaml:"pre_alloc"`
	ReleaseTimeout time.Duration `yaml:"release_timeout"`
}

type WriterConfig struct {
	MQTTConfig `yaml:",inline"`
	Pool       PoolConfig `yaml:"pool"`
}

func (c *WriterConfig) Validate() error {
	if c.Pool.Size == 0 {
		c.Pool.Size = 1000
	}
	if c.Pool.ReleaseTimeout == 0 {
		c.Pool.ReleaseTimeout = 5 * time.Second
	}

	return nil
}

func (c *WriterConfig) Endpoint() string {
	return c.BrokerURL
}
