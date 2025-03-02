package config

import (
	"time"

	"github.com/ValerySidorin/fujin/mq"
	"github.com/ValerySidorin/fujin/server/fujin"
)

type Config struct {
	Fujin fujin.ServerConfig
	MQ    mq.Config
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

	for _, c := range c.MQ.Readers {
		switch c.Protocol {
		// no reusable readers for now
		case "kafka":
			c.Reusable = false
		default:
			c.Reusable = false
		}
	}
}
