package jetstream

import (
	"fmt"
	"time"
)

// CommonSettings contains settings shared across all NATS JetStream routes.
type CommonSettings struct {
	URL    string `yaml:"url"`
	Stream string `yaml:"stream"`
}

// RouteSettings contains settings specific to a route.
type RouteSettings struct {
	Subject       string `yaml:"subject"`
	Consumer      string `yaml:"consumer,omitempty"`
	AckWait       string `yaml:"ack_wait,omitempty"`
	FetchMaxWait  string `yaml:"fetch_max_wait,omitempty"`
	MaxDeliver    int    `yaml:"max_deliver,omitempty"`
	MaxAckPending int    `yaml:"max_ack_pending,omitempty"`
}

// Config is the top-level configuration structure for NATS JetStream connector.
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

// Validate validates the NATS JetStream configuration.
func (c *Config) Validate() error {
	if c.Common.URL == "" {
		return fmt.Errorf("nats_jetstream: url is required")
	}
	if c.Common.Stream == "" {
		return fmt.Errorf("nats_jetstream: stream is required")
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("nats_jetstream: at least one route must be configured")
	}
	for name, route := range c.Routes {
		if route.Subject == "" {
			return fmt.Errorf("nats_jetstream: route %q: subject is required", name)
		}
		if route.AckWait != "" {
			if _, err := time.ParseDuration(route.AckWait); err != nil {
				return fmt.Errorf("nats_jetstream: route %q: invalid ack_wait: %w", name, err)
			}
		}
	}
	return nil
}

// Endpoint returns the NATS URL.
func (c *ConnectorConfig) Endpoint() string {
	return c.URL
}

// ackWaitDuration returns the configured ack wait duration or the default.
func (c *ConnectorConfig) ackWaitDuration() time.Duration {
	if c.AckWait != "" {
		d, _ := time.ParseDuration(c.AckWait)
		return d
	}
	return 30 * time.Second
}

// fetchMaxWaitDuration returns the configured fetch max wait duration or the default.
func (c *ConnectorConfig) fetchMaxWaitDuration() time.Duration {
	if c.FetchMaxWait != "" {
		d, _ := time.ParseDuration(c.FetchMaxWait)
		return d
	}
	return 5 * time.Second
}
