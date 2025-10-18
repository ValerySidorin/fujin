package config

import (
	"crypto/tls"
	"time"

	"github.com/ValerySidorin/fujin/internal/observability"
	"github.com/ValerySidorin/fujin/public/connectors"
	"github.com/quic-go/quic-go"
)

type Config struct {
	Fujin         FujinServerConfig
	GRPC          GRPCServerConfig
	Connectors    connectors.Config
	Observability observability.Config
}

type FujinServerConfig struct {
	Enabled               bool
	Addr                  string
	PingInterval          time.Duration
	PingTimeout           time.Duration
	PingStream            bool
	PingMaxRetries        int
	WriteDeadline         time.Duration
	ForceTerminateTimeout time.Duration
	TLS                   *tls.Config
	QUIC                  *quic.Config
}

type GRPCServerConfig struct {
	Enabled              bool
	Addr                 string
	ConnectionTimeout    time.Duration
	MaxConcurrentStreams uint32
	TLS                  *tls.Config

	// Message size limits (bytes)
	MaxRecvMsgSize int // Maximum message size the server can receive
	MaxSendMsgSize int // Maximum message size the server can send

	// Flow control window sizes (bytes)
	InitialWindowSize     int32 // Initial window size for stream-level flow control
	InitialConnWindowSize int32 // Initial window size for connection-level flow control

	// KeepAlive settings
	ServerKeepAlive ServerKeepAliveConfig
	ClientKeepAlive ClientKeepAliveConfig
}

type ServerKeepAliveConfig struct {
	Time                  time.Duration
	Timeout               time.Duration
	MaxConnectionIdle     time.Duration
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
}

type ClientKeepAliveConfig struct {
	MinTime             time.Duration
	PermitWithoutStream bool
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
