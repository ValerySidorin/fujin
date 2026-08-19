package websocket

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

const (
	defaultAddress         = ":4851"
	defaultPath            = "/fujin"
	defaultMaxMessageBytes = 4 << 20
)

type settings struct {
	Addr            string            `yaml:"addr"`
	Path            string            `yaml:"path"`
	TLS             pconfig.TLSConfig `yaml:"tls"`
	AllowedOrigins  []string          `yaml:"allowed_origins"`
	MaxMessageBytes int64             `yaml:"max_message_bytes"`
	Fujin           fujinSettings     `yaml:"fujin"`
}

type fujinSettings struct {
	PingInterval          time.Duration `yaml:"ping_interval"`
	PingTimeout           time.Duration `yaml:"ping_timeout"`
	WriteDeadline         time.Duration `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration `yaml:"force_terminate_timeout"`
}

func init() {
	if err := transport.Register("websocket", parseConfig, factory); err != nil {
		panic(fmt.Sprintf("register websocket transport: %v", err))
	}
}

func parseConfig(entry transport.Config) (any, error) {
	if entry.Enabled != nil && !*entry.Enabled {
		return serverconfig.WebSocketServerConfig{Enabled: false}, nil
	}
	data, err := yaml.Marshal(entry.Settings)
	if err != nil {
		return nil, fmt.Errorf("websocket settings: %w", err)
	}
	var settings settings
	if err := yaml.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("websocket settings: %w", err)
	}
	if settings.Addr == "" {
		settings.Addr = defaultAddress
	}
	if settings.Path == "" {
		settings.Path = defaultPath
	}
	if settings.Path[0] != '/' {
		return nil, fmt.Errorf("websocket path must start with /")
	}
	if settings.MaxMessageBytes <= 0 {
		settings.MaxMessageBytes = defaultMaxMessageBytes
	}
	if err := settings.TLS.Parse(); err != nil {
		return nil, fmt.Errorf("websocket tls: %w", err)
	}
	fujin := serverconfig.FujinProtocolConfig{
		PingInterval:          settings.Fujin.PingInterval,
		PingTimeout:           settings.Fujin.PingTimeout,
		WriteDeadline:         settings.Fujin.WriteDeadline,
		ForceTerminateTimeout: settings.Fujin.ForceTerminateTimeout,
	}
	fujin.SetDefaults()
	return serverconfig.WebSocketServerConfig{
		Enabled:         true,
		Addr:            settings.Addr,
		Path:            settings.Path,
		TLS:             settings.TLS.Config,
		AllowedOrigins:  append([]string(nil), settings.AllowedOrigins...),
		MaxMessageBytes: settings.MaxMessageBytes,
		Fujin:           fujin,
	}, nil
}

func factory(config any, catalog *connector.Catalog, runtime transport.Runtime, l *slog.Logger) (transport.TransportServer, error) {
	parsed := config.(serverconfig.WebSocketServerConfig)
	if !parsed.Enabled {
		return nil, nil
	}
	return NewServer(parsed, catalog, l, runtime.BuildVersion), nil
}
