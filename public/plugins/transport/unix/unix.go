package unix

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/transport"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	"gopkg.in/yaml.v3"
)

type settings struct {
	Path  string        `yaml:"path"`
	Fujin fujinSettings `yaml:"fujin"`
}

type fujinSettings struct {
	PingInterval          time.Duration `yaml:"ping_interval"`
	PingTimeout           time.Duration `yaml:"ping_timeout"`
	PingMaxRetries        int           `yaml:"ping_max_retries"`
	WriteDeadline         time.Duration `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration `yaml:"force_terminate_timeout"`
}

func init() {
	if err := transport.Register("unix", parseConfig, factory); err != nil {
		panic(fmt.Sprintf("register unix transport: %v", err))
	}
}

func parseConfig(e transport.Config) (any, error) {
	enabled := e.Enabled == nil || *e.Enabled
	if !enabled {
		return unixDisabledConfig{}, nil
	}
	var s settings
	if err := decodeSettings(e.Settings, &s); err != nil {
		return nil, fmt.Errorf("unix settings: %w", err)
	}
	if s.Path == "" {
		s.Path = "/tmp/fujin.sock"
	}
	fujin := serverconfig.FujinProtocolConfig{
		PingInterval:          s.Fujin.PingInterval,
		PingTimeout:           s.Fujin.PingTimeout,
		PingMaxRetries:        s.Fujin.PingMaxRetries,
		WriteDeadline:         s.Fujin.WriteDeadline,
		ForceTerminateTimeout: s.Fujin.ForceTerminateTimeout,
	}
	fujin.SetDefaults()
	return serverconfig.UnixServerConfig{
		Path:  s.Path,
		Fujin: fujin,
	}, nil
}

type unixDisabledConfig struct{}

func decodeSettings(raw any, out any) error {
	data, err := yaml.Marshal(raw)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, out)
}

func factory(cfg any, catalog *connector.Catalog, runtime transport.Runtime, l *slog.Logger) (transport.TransportServer, error) {
	if _, ok := cfg.(unixDisabledConfig); ok {
		return nil, nil
	}
	conf := cfg.(serverconfig.UnixServerConfig)
	return NewServer(conf, catalog, l, runtime.BuildVersion), nil
}
