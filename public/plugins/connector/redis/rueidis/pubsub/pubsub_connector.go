package pubsub

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

func descriptor() connector.Descriptor {
	return connector.Descriptor{Converter: convertConfigValue, Compile: compileConnector}
}
func compileConnector(raw any) (connector.Compiled, error) {
	var config Config
	if parsed, ok := raw.(Config); ok {
		config = parsed
	} else if err := util.ConvertConfig(raw, &config); err != nil {
		return nil, fmt.Errorf("resp_pubsub connector: convert config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("resp_pubsub connector: invalid config: %w", err)
	}
	profiles := make(map[string]connector.RouteProfile, len(config.Routes))
	factories := make(map[string]connector.RouteFactory, len(config.Routes))
	for route, settings := range config.Routes {
		conf := NewConnectorConfig(config.Common, settings)
		profile := connector.RouteProfile{}
		factory := connector.RouteFactory{}
		if err := conf.ValidateReader(); err == nil {
			profile.Subscribe = true
			factory.Reader = func(auto bool, l *slog.Logger) (connector.ReadCloser, error) { return NewReader(conf, true, l) }
		}
		if err := conf.ValidateWriter(); err == nil {
			profile.Produce = true
			profile.ProduceGuarantee = connector.AcceptancePeer
			factory.Writer = func(l *slog.Logger) (connector.WriteCloser, error) { return NewWriter(conf, l) }
		}
		profiles[route], factories[route] = profile, factory
	}
	return connector.CompileStatic(profiles, factories)
}
