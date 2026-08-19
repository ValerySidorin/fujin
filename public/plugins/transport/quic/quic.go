package quic

import (
	"fmt"
	"log/slog"
	"time"

	pconfig "github.com/fujin-io/fujin/public/config"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/transport"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	quicgo "github.com/quic-go/quic-go"
	"gopkg.in/yaml.v3"
)

type settings struct {
	Addr                 string            `yaml:"addr"`
	TLS                  pconfig.TLSConfig `yaml:"tls"`
	Fujin                fujinSettings     `yaml:"fujin"`
	MaxIncomingStreams   int64             `yaml:"max_incoming_streams"`
	KeepAlivePeriod      time.Duration     `yaml:"keepalive_period"`
	HandshakeIdleTimeout time.Duration     `yaml:"handshake_idle_timeout"`
	MaxIdleTimeout       time.Duration     `yaml:"max_idle_timeout"`
}

type fujinSettings struct {
	PingInterval          time.Duration `yaml:"ping_interval"`
	PingTimeout           time.Duration `yaml:"ping_timeout"`
	PingStream            bool          `yaml:"ping_stream"`
	PingMaxRetries        int           `yaml:"ping_max_retries"`
	WriteDeadline         time.Duration `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration `yaml:"force_terminate_timeout"`
}

func init() {
	if err := transport.Register("quic", parseConfig, factory); err != nil {
		panic(fmt.Sprintf("register quic transport: %v", err))
	}
}

func parseConfig(e transport.Config) (any, error) {
	enabled := e.Enabled == nil || *e.Enabled
	if !enabled {
		return serverconfig.QUICServerConfig{Enabled: false}, nil
	}
	var s settings
	if err := decodeSettings(e.Settings, &s); err != nil {
		return nil, fmt.Errorf("quic settings: %w", err)
	}
	if s.Addr == "" {
		s.Addr = ":4848"
	}
	if err := s.TLS.Parse(); err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	fujin := serverconfig.FujinProtocolConfig{
		PingInterval:          s.Fujin.PingInterval,
		PingTimeout:           s.Fujin.PingTimeout,
		PingStream:            s.Fujin.PingStream,
		PingMaxRetries:        s.Fujin.PingMaxRetries,
		WriteDeadline:         s.Fujin.WriteDeadline,
		ForceTerminateTimeout: s.Fujin.ForceTerminateTimeout,
	}
	fujin.SetDefaults()
	return serverconfig.QUICServerConfig{
		Enabled: true,
		Addr:    s.Addr,
		TLS:     s.TLS.Config,
		QUIC: &quicgo.Config{
			MaxIncomingStreams:   s.MaxIncomingStreams,
			KeepAlivePeriod:      s.KeepAlivePeriod,
			HandshakeIdleTimeout: s.HandshakeIdleTimeout,
			MaxIdleTimeout:       s.MaxIdleTimeout,
		},
		Fujin: fujin,
	}, nil
}

func decodeSettings(raw any, out any) error {
	data, err := yaml.Marshal(raw)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, out)
}

func factory(cfg any, catalog *connector.Catalog, runtime transport.Runtime, l *slog.Logger) (transport.TransportServer, error) {
	conf := cfg.(serverconfig.QUICServerConfig)
	if !conf.Enabled {
		return nil, nil
	}
	return NewServer(conf, catalog, l, runtime.BuildVersion), nil
}
