package config

import (
	"fmt"
	"time"

	api_quirk "github.com/ValerySidorin/fujin/internal/api/fujin"
	tls_config "github.com/ValerySidorin/fujin/internal/config/tls"
	"github.com/ValerySidorin/fujin/internal/mq"
	"github.com/quic-go/quic-go"
)

type Config struct {
	Log   LogConfig   `yaml:"log"`
	Fujin FujinConfig `yaml:"fujin"`
	MQ    mq.Config   `yaml:"mq"`
}

type FujinConfig struct {
	Addr                  string               `yaml:"addr"`
	PingInterval          time.Duration        `yaml:"ping_interval"`
	PingTimeout           time.Duration        `yaml:"ping_timeout"`
	WriteDeadline         time.Duration        `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration        `yaml:"force_terminate_timeout"`
	TLS                   tls_config.TLSConfig `yaml:"tls"`
	QUIC                  QUICConfig           `yaml:"quic"`
}

type QUICConfig struct {
	MaxIncomingStreams   int64         `yaml:"max_incoming_streams"`
	KeepAlivePeriod      time.Duration `yaml:"keepalive_period"`
	HandshakeIdleTimeout time.Duration `yaml:"handshake_idle_timeout"`
	MaxIdleTimeout       time.Duration `yaml:"max_idle_timeout"`
}

type LogConfig struct {
	Level string `yaml:"level"`
}

func (c *Config) SetDefaults() {
	if c.Log.Level == "" {
		c.Log.Level = "INFO"
	}

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

func (c *Config) ParseQUICServerConfig() (*api_quirk.ServerConfig, error) {
	tlsConf, err := c.Fujin.TLS.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse TLS conf: %w", err)
	}

	return &api_quirk.ServerConfig{
		Addr:                  c.Fujin.Addr,
		PingInterval:          c.Fujin.PingInterval,
		PingTimeout:           c.Fujin.PingTimeout,
		WriteDeadine:          c.Fujin.WriteDeadline,
		ForceTerminateTimeout: c.Fujin.ForceTerminateTimeout,
		TLS:                   tlsConf,
		QUIC:                  c.Fujin.QUIC.Parse(),
	}, nil
}

func (c *QUICConfig) Parse() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:   c.MaxIncomingStreams,
		KeepAlivePeriod:      c.KeepAlivePeriod,
		HandshakeIdleTimeout: c.HandshakeIdleTimeout,
		MaxIdleTimeout:       c.MaxIdleTimeout,
	}
}
