package jetstream

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
		return nil, fmt.Errorf("nats_jetstream connector: convert config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("nats_jetstream connector: invalid config: %w", err)
	}
	profiles := make(map[string]connector.RouteProfile, len(config.Routes))
	factories := make(map[string]connector.RouteFactory, len(config.Routes))
	for route, settings := range config.Routes {
		conf := NewConnectorConfig(config.Common, settings)
		profiles[route] = connector.RouteProfile{Produce: true, Subscribe: true, Fetch: true, ManualSettlement: true, ProduceGuarantee: connector.AcceptanceDurable, Settlement: connector.SettlementProfile{Ack: connector.AckSingle, Nack: connector.NackRequeue}}
		factories[route] = connector.RouteFactory{Reader: func(auto bool, l *slog.Logger) (connector.ReadCloser, error) { return NewReader(conf, auto, l) }, Writer: func(l *slog.Logger) (connector.WriteCloser, error) { return NewWriter(conf, l) }}
	}
	return connector.CompileStatic(profiles, factories)
}
