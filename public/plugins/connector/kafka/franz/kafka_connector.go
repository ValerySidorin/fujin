package franz

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
		return nil, fmt.Errorf("kafka_franz connector: convert config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("kafka_franz connector: invalid config: %w", err)
	}
	profiles := make(map[string]connector.RouteProfile, len(config.Routes))
	factories := make(map[string]connector.RouteFactory, len(config.Routes))
	for route, settings := range config.Routes {
		conf := NewConnectorConfig(config.Common, settings)
		profile := connector.RouteProfile{Headers: true}
		factory := connector.RouteFactory{}
		if len(settings.ConsumeTopics) > 0 {
			profile.Subscribe, profile.Fetch, profile.ManualSettlement = true, true, true
			profile.Settlement.Ack = connector.AckCumulative
			factory.Reader = func(autoSettle bool, l *slog.Logger) (connector.ReadCloser, error) {
				return NewConnector(conf, autoSettle, l)
			}
		}
		if settings.ProduceTopic != "" {
			profile.Produce = true
			profile.ProduceGuarantee = connector.AcceptancePeer
			profile.Transactions = settings.TransactionalID != ""
			factory.Writer = func(l *slog.Logger) (connector.WriteCloser, error) { return NewConnector(conf, false, l) }
		}
		profiles[route], factories[route] = profile, factory
	}
	return connector.CompileStatic(profiles, factories)
}
