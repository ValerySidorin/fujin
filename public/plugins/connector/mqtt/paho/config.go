package paho

import (
	"fmt"
	"time"
)

// CommonSettings contains settings shared across all MQTT clients
type CommonSettings struct {
	BrokerURL         string        `yaml:"broker_url"`
	KeepAlive         uint16        `yaml:"keep_alive"` // Keep alive in seconds
	DisconnectTimeout time.Duration `yaml:"disconnect_timeout"`
	ConnectTimeout    time.Duration `yaml:"connect_timeout"`
}

// PoolConfig contains pool configuration for writers
type PoolConfig struct {
	Size           int           `yaml:"size"`
	PreAlloc       bool          `yaml:"pre_alloc"`
	ReleaseTimeout time.Duration `yaml:"release_timeout"`
}

// RouteSettings contains settings specific to a route.
type RouteSettings struct {
	ClientID         string        `yaml:"client_id"`
	Topic            string        `yaml:"topic"`
	QoS              byte          `yaml:"qos"`
	Retain           bool          `yaml:"retain"`
	CleanStart       bool          `yaml:"clean_start"`
	SessionExpiry    uint32        `yaml:"session_expiry"`     // Session expiry interval in seconds
	SendAcksInterval time.Duration `yaml:"send_acks_interval"` // Interval for sending batched acks (default: 50ms)
	AckTTL           time.Duration `yaml:"ack_ttl"`            // TTL for pending acks, after which they're cleaned up (default: 5m)
	Pool             PoolConfig    `yaml:"pool,omitempty"`     // Only used for writers
}

// Config is the top-level configuration structure for MQTT connector.
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

// Validate validates the MQTT configuration
func (c *Config) Validate() error {
	if c.Common.BrokerURL == "" {
		return fmt.Errorf("mqtt: broker_url is required")
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("mqtt: at least one route must be configured")
	}
	for name, route := range c.Routes {
		if route.ClientID == "" {
			return fmt.Errorf("mqtt: route %q: client_id is required", name)
		}
		if route.Topic == "" {
			return fmt.Errorf("mqtt: route %q: topic is required", name)
		}
		if route.QoS > 2 {
			return fmt.Errorf("mqtt: route %q: qos must be 0, 1, or 2", name)
		}
	}
	return nil
}

// Endpoint returns the broker URL
func (c *ConnectorConfig) Endpoint() string {
	return c.BrokerURL
}

// applyDefaults sets default values for optional fields
func (c *ConnectorConfig) applyDefaults() {
	if c.KeepAlive == 0 {
		c.KeepAlive = 30
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.DisconnectTimeout == 0 {
		c.DisconnectTimeout = 5 * time.Second
	}
	if c.SendAcksInterval == 0 {
		c.SendAcksInterval = 50 * time.Millisecond
	}
	if c.AckTTL == 0 {
		c.AckTTL = 5 * time.Minute
	}
	if c.Pool.Size == 0 {
		c.Pool.Size = 1000
	}
	if c.Pool.ReleaseTimeout == 0 {
		c.Pool.ReleaseTimeout = 5 * time.Second
	}
}

// ValidateWriter validates writer-specific settings
func (c *ConnectorConfig) ValidateWriter() error {
	c.applyDefaults()
	return nil
}

// ValidateReader validates reader-specific settings
func (c *ConnectorConfig) ValidateReader() error {
	c.applyDefaults()
	return nil
}
