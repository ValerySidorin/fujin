package streams

import (
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/util"
)

// streamsConnector implements connector.Connector interface for Redis Rueidis Streams.
type streamsConnector struct {
	config Config
	l      *slog.Logger
}

// newRESPStreamsConnector creates a new Redis Rueidis Streams connector instance.
func newRESPStreamsConnector(config any, l *slog.Logger) (connector.Connector, error) {
	// Allow nil config for getting converter only
	if config == nil {
		return &streamsConnector{
			config: Config{},
			l:      l,
		}, nil
	}

	var typedConfig Config
	if parsedConfig, ok := config.(Config); ok {
		typedConfig = parsedConfig
	} else {
		if err := util.ConvertConfig(config, &typedConfig); err != nil {
			return nil, fmt.Errorf("redis_rueidis_streams connector: failed to convert config: %w", err)
		}
	}
	if err := typedConfig.Validate(); err != nil {
		return nil, fmt.Errorf("redis_rueidis_streams connector: invalid config: %w", err)
	}

	return &streamsConnector{
		config: typedConfig,
		l:      l,
	}, nil
}

// NewReader creates a reader from configuration.
func (s *streamsConnector) NewReader(config any, route string, autoCommit bool, l *slog.Logger) (connector.ReadCloser, error) {
	routeConf, ok := s.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("redis_rueidis_streams: route not found: %s", route)
	}

	connConf := ConnectorConfig{
		CommonSettings: s.config.Common,
		RouteSettings:  routeConf,
	}

	if err := connConf.ValidateReader(); err != nil {
		return nil, err
	}

	return NewReader(connConf, autoCommit, l)
}

// NewWriter creates a writer from configuration.
func (s *streamsConnector) NewWriter(config any, route string, l *slog.Logger) (connector.WriteCloser, error) {
	routeConf, ok := s.config.Routes[route]
	if !ok {
		return nil, fmt.Errorf("redis_rueidis_streams: route not found: %s", route)
	}

	connConf := ConnectorConfig{
		CommonSettings: s.config.Common,
		RouteSettings:  routeConf,
	}

	if err := connConf.ValidateWriter(); err != nil {
		return nil, err
	}

	return NewWriter(connConf, l)
}

// GetConfigValueConverter returns the config value converter for Redis Rueidis Streams.
func (s *streamsConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return convertConfigValue
}
