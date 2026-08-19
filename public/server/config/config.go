package config

import (
	"crypto/tls"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/quic-go/quic-go"
)

type Config struct {
	BuildVersion string
	Transports   []transport.Config
	GRPC         GRPCServerConfig
	Health       HealthConfig
	Connectors   config.ConnectorsConfig
}

type QUICServerConfig struct {
	Enabled bool
	Addr    string
	TLS     *tls.Config
	QUIC    *quic.Config
	Fujin   FujinProtocolConfig
}

type WebSocketServerConfig struct {
	Enabled         bool
	Addr            string
	Path            string
	TLS             *tls.Config
	AllowedOrigins  []string
	MaxMessageBytes int64
	Fujin           FujinProtocolConfig
}

type TCPServerConfig struct {
	Enabled bool
	Addr    string
	TLS     *tls.Config
	Fujin   FujinProtocolConfig
}

type UnixServerConfig struct {
	Path  string // e.g. /run/fujin/fujin.sock or /tmp/fujin.sock
	Fujin FujinProtocolConfig
}

// FujinProtocolConfig holds settings specific to the fujin binary protocol,
// independent of the underlying transport.
type FujinProtocolConfig struct {
	PingInterval          time.Duration
	PingTimeout           time.Duration
	PingStream            bool
	PingMaxRetries        int
	WriteDeadline         time.Duration
	ForceTerminateTimeout time.Duration
}

func (f *FujinProtocolConfig) SetDefaults() {
	if f.PingInterval == 0 {
		f.PingInterval = 2 * time.Second
	}
	if f.PingTimeout == 0 {
		f.PingTimeout = 5 * time.Second
	}
	if f.PingMaxRetries == 0 {
		f.PingMaxRetries = 3
	}
	if f.WriteDeadline == 0 {
		f.WriteDeadline = 10 * time.Second
	}
	if f.ForceTerminateTimeout == 0 {
		f.ForceTerminateTimeout = 15 * time.Second
	}
}

type GRPCServerConfig struct {
	Enabled              bool
	Addr                 string
	ConnectionTimeout    time.Duration
	MaxConcurrentStreams uint32
	TLS                  *tls.Config

	MaxRecvMsgSize int
	MaxSendMsgSize int

	InitialWindowSize     int32
	InitialConnWindowSize int32

	ServerKeepAlive      ServerKeepAliveConfig
	ClientKeepAlive      ClientKeepAliveConfig
	ObservabilityEnabled bool
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

type HealthConfig struct {
	Enabled bool
	Addr    string // default ":8080"
}

func (c *Config) SetDefaults() {
	if c.GRPC.Addr == "" {
		c.GRPC.Addr = ":4849"
	}
	if c.Health.Addr == "" {
		c.Health.Addr = ":8080"
	}
}
