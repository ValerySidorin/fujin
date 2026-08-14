package core

import (
	"fmt"
)

// CommonSettings contains settings shared across all NATS routes.
type CommonSettings struct {
	URL string `yaml:"url"`
}

// RouteSettings contains settings specific to a route.
type RouteSettings struct {
	Subject string `yaml:"subject"`
}

// Config is the top-level configuration structure for NATS Core connector.
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

// Validate validates the NATS Core configuration
func (c *Config) Validate() error {
	if c.Common.URL == "" {
		return fmt.Errorf("nats_core: url is required")
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("nats_core: at least one route must be configured")
	}
	for name, route := range c.Routes {
		if route.Subject == "" {
			return fmt.Errorf("nats_core: route %q: subject is required", name)
		}
	}
	return nil
}

// Endpoint returns the NATS URL
func (c *ConnectorConfig) Endpoint() string {
	return c.URL
}
