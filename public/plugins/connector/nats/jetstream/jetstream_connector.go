package jetstream

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// jetStreamConnector implements connector.Connector for NATS JetStream.
type jetStreamConnector struct {
	config Config
	l      *slog.Logger
}

// newJetStreamConnector creates a new NATS JetStream connector instance.
func newJetStreamConnector(config any, l *slog.Logger) (connector.Connector, error) {
	if config == nil {
		return &jetStreamConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("nats_jetstream connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("nats_jetstream connector: invalid config: %w", err)
	}

	return &jetStreamConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

func (c *jetStreamConnector) NewReader(config any, route string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	routeConf, ok := c.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("nats_jetstream: route not found: %s", route)
	}

	return NewReader(ConnectorConfig{
		CommonSettings: c.config.Common,
		RouteSettings:  routeConf,
	}, autoCommit, l)
}

func (c *jetStreamConnector) NewWriter(config any, route string, l *slog.Logger) (connector.WriteCloser, error) {
	routeConf, ok := c.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("nats_jetstream: route not found: %s", route)
	}

	return NewWriter(ConnectorConfig{
		CommonSettings: c.config.Common,
		RouteSettings:  routeConf,
	}, l)
}

func (c *jetStreamConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
