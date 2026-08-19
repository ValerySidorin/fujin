package streams

import (
	"fmt"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector/redis/rueidis/config"
)

type Marshaller string

const (
	JSON Marshaller = "json"
)

type StreamConf struct {
	StartID       string `yaml:"start_id"`
	GroupCreateID string `yaml:"group_create_id"`
}

type GroupConf struct {
	Name     string `yaml:"name"`
	Consumer string `yaml:"consumer"`
}

// CommonSettings contains settings shared across all Redis Rueidis Streams routes.
type CommonSettings struct {
	config.RedisConfig       `yaml:",inline"`
	config.WriterBatchConfig `yaml:",inline"`
}

// RouteSettings contains settings specific to a route.
type RouteSettings struct {
	Streams map[string]StreamConf `yaml:"streams,omitempty"`
	Block   time.Duration         `yaml:"block,omitempty"`
	Count   int64                 `yaml:"count,omitempty"`
	Group   GroupConf             `yaml:"group,omitempty"`
	Stream  string                `yaml:"stream,omitempty"`

	Marshaller Marshaller `yaml:"marshaller,omitempty"`
}

// Config is the top-level configuration for RESP Streams connector.
type Config struct {
	Common CommonSettings           `yaml:"common"`
	Routes map[string]RouteSettings `yaml:"routes"`
}

// ConnectorConfig combines common and route-specific settings.
type ConnectorConfig struct {
	CommonSettings
	RouteSettings
}

func NewConnectorConfig(common CommonSettings, route RouteSettings) ConnectorConfig {
	return ConnectorConfig{CommonSettings: common, RouteSettings: route}
}

func (c *Config) Validate() error {
	if err := c.Common.RedisConfig.Validate(); err != nil {
		return fmt.Errorf("redis_rueidis_streams: %w", err)
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("redis_rueidis_streams: at least one route must be configured")
	}
	return nil
}

func (c *ConnectorConfig) Endpoint() string { return c.RedisConfig.Endpoint() }

func (c *ConnectorConfig) ValidateWriter() error {
	c.WriterBatchConfig.ApplyBatchDefaults()
	if err := c.WriterBatchConfig.ValidateBatch(); err != nil {
		return err
	}
	if c.Stream == "" {
		return fmt.Errorf("redis_rueidis_streams: stream is required for writer")
	}
	return nil
}

func (c *ConnectorConfig) ValidateReader() error {
	if len(c.Streams) == 0 {
		return fmt.Errorf("redis_rueidis_streams: at least one stream is required for reader")
	}
	return nil
}
