package config

import (
	"time"

	fujin_server "github.com/ValerySidorin/fujin/internal/api/fujin/server"
	grpc_server "github.com/ValerySidorin/fujin/internal/api/grpc/server"
	"github.com/ValerySidorin/fujin/internal/observability"
	"github.com/ValerySidorin/fujin/public/connectors"
)

type Config struct {
	Fujin         fujin_server.FujinServerConfig
	GRPC          grpc_server.GRPCServerConfig
	Connectors    connectors.Config
	Observability observability.Config
}

type ObservabilityConfig struct {
	Metrics struct {
		Enabled bool
		Bind    string
		Path    string
	}
	Tracing struct {
		Enabled     bool
		OTLPAddress string
		Insecure    bool
		SampleRatio float64
	}
	Resource struct {
		ServiceName    string
		ServiceVersion string
		Environment    string
	}
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

	if c.Fujin.PingMaxRetries == 0 {
		c.Fujin.PingMaxRetries = 3
	}

	if c.Fujin.WriteDeadline == 0 {
		c.Fujin.WriteDeadline = 10 * time.Second
	}

	if c.Fujin.ForceTerminateTimeout == 0 {
		c.Fujin.ForceTerminateTimeout = 15 * time.Second
	}

	if c.GRPC.Addr == "" {
		c.GRPC.Addr = ":4849"
	}

	if c.Observability.Metrics.Path == "" {
		c.Observability.Metrics.Path = "/metrics"
	}
	if c.Observability.Metrics.Addr == "" {
		c.Observability.Metrics.Addr = ":9090"
	}
}
