package config

import (
	"time"

	"github.com/ValerySidorin/fujin/internal/server/fujin"
	"github.com/ValerySidorin/fujin/public/connectors"
)

type Config struct {
	Fujin      fujin.ServerConfig
	Connectors connectors.Config
}

func (c *Config) SetDefaults() {
	if c.Fujin.Addr == "" {
		c.Fujin.Addr = ":4848"
	}

	if c.Fujin.PingInterval == 0 {
		c.Fujin.PingInterval = 2 * time.Second
	}

	if c.Fujin.PingTimeout == 0 {
		c.Fujin.PingTimeout = 5 * time.Second
	}

	if c.Fujin.WriteDeadline == 0 {
		c.Fujin.WriteDeadline = 10 * time.Second
	}

	if c.Fujin.ForceTerminateTimeout == 0 {
		c.Fujin.ForceTerminateTimeout = 15 * time.Second
	}
}
