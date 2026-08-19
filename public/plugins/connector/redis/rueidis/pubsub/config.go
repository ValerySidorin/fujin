package pubsub

import (
	"fmt"

	"github.com/fujin-io/fujin/public/plugins/connector/redis/rueidis/config"
)

// CommonSettings contains settings shared across all Redis Rueidis PubSub routes.
type CommonSettings struct {
	config.RedisConfig       `yaml:",inline"`
	config.WriterBatchConfig `yaml:",inline"`
}

// RouteSettings contains settings specific to a route.
type RouteSettings struct {
	// For readers: multiple channels to subscribe
	Channels []string `yaml:"channels,omitempty"`
	// For writers: single channel to publish
	Channel string `yaml:"channel,omitempty"`
}

// Config is the top-level configuration structure for Redis Rueidis PubSub connector.
type Config struct {
	Common CommonSettings           `yaml:"common"`
	Routes map[string]RouteSettings `yaml:"routes"`
}

// ConnectorConfig combines common and route-specific settings.
type ConnectorConfig struct {
	CommonSettings
	RouteSettings
}

// NewConnectorConfig creates a ConnectorConfig from common and route-specific settings.
func NewConnectorConfig(common CommonSettings, route RouteSettings) ConnectorConfig {
	return ConnectorConfig{
		CommonSettings: common,
		RouteSettings:  route,
	}
}

// Validate validates the Redis PubSub configuration
func (c *Config) Validate() error {
	if err := c.Common.RedisConfig.Validate(); err != nil {
		return fmt.Errorf("resp_pubsub: %w", err)
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("resp_pubsub: at least one route must be configured")
	}
	return nil
}

// Endpoint returns the Redis endpoint
func (c *ConnectorConfig) Endpoint() string {
	return c.RedisConfig.Endpoint()
}

// ValidateWriter validates writer-specific settings
func (c *ConnectorConfig) ValidateWriter() error {
	c.WriterBatchConfig.ApplyBatchDefaults()
	if err := c.WriterBatchConfig.ValidateBatch(); err != nil {
		return err
	}
	if c.Channel == "" {
		return fmt.Errorf("resp_pubsub: channel is required for writer")
	}
	return nil
}

// ValidateReader validates reader-specific settings
func (c *ConnectorConfig) ValidateReader() error {
	if len(c.Channels) == 0 {
		return fmt.Errorf("resp_pubsub: at least one channel is required for reader")
	}
	return nil
}
