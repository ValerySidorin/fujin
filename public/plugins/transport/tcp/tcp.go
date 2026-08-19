package tcp

import (
	"fmt"
	"log/slog"
	"time"

	pconfig "github.com/fujin-io/fujin/public/config"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/transport"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"gopkg.in/yaml.v3"
)

// settings is the TCP transport plugin configuration (parsed from Settings).
type settings struct {
	Addr  string            `yaml:"addr"`
	TLS   pconfig.TLSConfig `yaml:"tls"`
	Fujin fujinSettings     `yaml:"fujin"`
}

type fujinSettings struct {
	PingInterval          time.Duration `yaml:"ping_interval"`
	PingTimeout           time.Duration `yaml:"ping_timeout"`
	PingMaxRetries        int           `yaml:"ping_max_retries"`
	WriteDeadline         time.Duration `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration `yaml:"force_terminate_timeout"`
}

func init() {
	if err := transport.Register("tcp", parseConfig, factory); err != nil {
		panic(fmt.Sprintf("register tcp transport: %v", err))
	}
}

func parseConfig(e transport.Config) (any, error) {
	enabled := e.Enabled == nil || *e.Enabled
	if !enabled {
		return serverconfig.TCPServerConfig{Enabled: false}, nil
	}
	var s settings
	if err := decodeSettings(e.Settings, &s); err != nil {
		return nil, fmt.Errorf("tcp settings: %w", err)
	}
	if s.Addr == "" {
		s.Addr = ":4850"
	}
	if err := s.TLS.Parse(); err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	fujin := parseFujin(s.Fujin)
	return serverconfig.TCPServerConfig{
		Enabled: true,
		Addr:    s.Addr,
		TLS:     s.TLS.Config,
		Fujin:   fujin,
	}, nil
}

func decodeSettings(raw any, out any) error {
	data, err := yaml.Marshal(raw)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, out)
}

func parseFujin(s fujinSettings) serverconfig.FujinProtocolConfig {
	f := serverconfig.FujinProtocolConfig{
		PingInterval:          s.PingInterval,
		PingTimeout:           s.PingTimeout,
		PingMaxRetries:        s.PingMaxRetries,
		WriteDeadline:         s.WriteDeadline,
		ForceTerminateTimeout: s.ForceTerminateTimeout,
	}
	f.SetDefaults()
	return f
}

func factory(cfg any, catalog *connector.Catalog, runtime transport.Runtime, l *slog.Logger) (transport.TransportServer, error) {
	c := cfg.(serverconfig.TCPServerConfig)
	if !c.Enabled {
		return nil, nil
	}
	return NewServer(c, catalog, l, runtime.BuildVersion), nil
}
