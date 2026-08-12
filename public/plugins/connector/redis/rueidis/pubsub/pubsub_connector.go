package pubsub

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// pubsubRueidisConnector implements connector.Connector interface for Redis PubSub
type pubsubRueidisConnector struct {
	config Config
	l      *slog.Logger
}

// newRESPPubSubConnector creates a new Redis PubSub connector instance
func newRESPPubSubConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &pubsubRueidisConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("resp_pubsub connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("resp_pubsub connector: invalid config: %w", err)
	}

	return &pubsubRueidisConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (p *pubsubRueidisConnector) NewReader(config any, route string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	routeConf, ok := p.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("resp_pubsub: route not found: %s", route)
	}

	connConf := ConnectorConfig{
		CommonSettings: p.config.Common,
		RouteSettings:  routeConf,
	}

	if err := connConf.ValidateReader(); err != nil {
		return nil, err
	}

	return NewReader(connConf, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (p *pubsubRueidisConnector) NewWriter(config any, route string, l *slog.Logger) (connector.WriteCloser, error) {
	routeConf, ok := p.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("resp_pubsub: route not found: %s", route)
	}

	connConf := ConnectorConfig{
		CommonSettings: p.config.Common,
		RouteSettings:  routeConf,
	}

	if err := connConf.ValidateWriter(); err != nil {
		return nil, err
	}

	return NewWriter(connConf, l)
}

// GetConfigValueConverter returns the config value converter for Redis PubSub
func (p *pubsubRueidisConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
