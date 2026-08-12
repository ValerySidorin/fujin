package amqp1

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// azureAmqp1Connector implements connector.Connector interface
type azureAmqp1Connector struct {
	config Config
	l      *slog.Logger
}

// newAzureAMQP1Connector creates a new Azure AMQP1.0 connector instance
func newAzureAMQP1Connector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &azureAmqp1Connector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("azure_amqp1 connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("azure_amqp1 connector: invalid config: %w", err)
	}

	return &azureAmqp1Connector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration
func (a *azureAmqp1Connector) NewReader(config any, route string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	routeConf, ok := a.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("azure_amqp1: route not found: %s", route)
	}

	if routeConf.Receiver == nil {
		return nil, fmt.Errorf("azure_amqp1: route %q is not configured as a reader (receiver not defined)", route)
	}

	return NewReader(ConnectorConfig{
		CommonSettings: a.config.Common,
		RouteSettings:  routeConf,
	}, autoCommit, l)
}

// NewWriter creates a writer from configuration
func (a *azureAmqp1Connector) NewWriter(config any, route string, l *slog.Logger) (connector.WriteCloser, error) {
	routeConf, ok := a.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("azure_amqp1: route not found: %s", route)
	}

	if routeConf.Sender == nil {
		return nil, fmt.Errorf("azure_amqp1: route %q is not configured as a writer (sender not defined)", route)
	}

	return NewWriter(ConnectorConfig{
		CommonSettings: a.config.Common,
		RouteSettings:  routeConf,
	}, l)
}

// GetConfigValueConverter returns the config value converter for AMQP10
func (a *azureAmqp1Connector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
