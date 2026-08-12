// Package connector provides a plugin system for message broker connectors.
// Connectors implement readers and writers for different message broker protocols
// like Kafka, NATS, RabbitMQ, etc.
//
// To register a connector, import it in your main package:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/connector/kafka/franz"
//
// The connector will register itself automatically via init().
package connector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector/config"
)

// Reader is the interface for message readers.
type Reader interface {
	Subscribe(ctx context.Context, h func(message []byte, topic string, args ...any)) error
	SubscribeWithHeaders(ctx context.Context, h func(message []byte, topic string, hs [][]byte, args ...any)) error
	// Fetch invokes fetchResponseHandler exactly once before any msgHandler call.
	// The reported count equals the number of subsequent message callbacks. Both
	// callbacks are synchronous: none may run after Fetch returns. An error response
	// must not be followed by message callbacks.
	Fetch(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, args ...any),
	)
	// FetchWithHeaders has the same callback ordering and lifetime contract as Fetch.
	FetchWithHeaders(
		ctx context.Context, n uint32,
		fetchResponseHandler func(n uint32, err error),
		msgHandler func(message []byte, topic string, hs [][]byte, args ...any),
	)
	// Ack invokes ackHandler exactly once and, if it reports nil, invokes
	// ackMsgHandler exactly once per message ID. The callback groups may arrive
	// in either order and may run after Ack returns, but are serialized per
	// invocation. An error result must not be followed by message callbacks.
	Ack(
		ctx context.Context, msgIDs [][]byte,
		ackHandler func(error),
		ackMsgHandler func([]byte, error),
	)
	// Nack has the same callback ordering, serialization, and lifetime contract as Ack.
	Nack(
		ctx context.Context, msgIDs [][]byte,
		nackHandler func(error),
		nackMsgHandler func([]byte, error),
	)
	MsgIDArgsLen() int
	EncodeMsgID(buf []byte, topic string, args ...any) []byte
	AutoCommit() bool
}

// Writer is the interface for message writers.
type Writer interface {
	Produce(ctx context.Context, msg []byte, callback func(err error))
	HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error))
	Flush(ctx context.Context) error
	BeginTx(ctx context.Context) error
	CommitTx(ctx context.Context) error
	RollbackTx(ctx context.Context) error
}

// Connector creates readers and writers for a specific message broker protocol.
type Connector interface {
	// NewReader creates a reader from the connector configuration for a route.
	// config is the connector-specific configuration (can be nil).
	// route selects the configured Fujin route.
	// autoCommit indicates whether the reader should auto-commit messages.
	NewReader(config any, route string, autoCommit bool, l *slog.Logger) (ReadCloser, error)

	// NewWriter creates a writer from the connector configuration for a route.
	// config is the connector-specific configuration (can be nil).
	// route selects the configured Fujin route.
	NewWriter(config any, route string, l *slog.Logger) (WriteCloser, error)

	// GetConfigValueConverter returns a converter for configuration values, or nil if not supported.
	// This is used for runtime configuration overrides.
	GetConfigValueConverter() ConfigValueConverterFunc
}

// ReadCloser combines Reader and Closer interfaces
type ReadCloser interface {
	Reader
	io.Closer
}

// WriteCloser combines Writer and Closer interfaces
type WriteCloser interface {
	Writer
	io.Closer
}

// Factory creates a connector from configuration.
// config is the connector-specific configuration (can be nil).
type Factory func(config any, l *slog.Logger) (Connector, error)

// ConfigValueConverterFunc converts and validates a configuration value for a specific setting path.
// Returns the converted value and any validation error.
type ConfigValueConverterFunc func(settingPath string, value string) (any, error)

var (
	factories = make(map[string]Factory)
	mu        sync.RWMutex
)

// Register registers a connector factory with the given protocol name.
// This is typically called from init() in connector implementations.
// Returns an error if the protocol is already registered.
func Register(protocol string, factory Factory) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[protocol]; exists {
		return fmt.Errorf("connector factory for protocol %q already registered", protocol)
	}

	factories[protocol] = factory
	return nil
}

// Get returns a connector factory by protocol name.
func Get(protocol string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[protocol]
	return factory, ok
}

// GetConfigValueConverter returns the value converter for a protocol, or nil if not registered.
func GetConfigValueConverter(protocol string) ConfigValueConverterFunc {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[protocol]
	if !ok {
		return nil
	}

	// Create a temporary connector instance to get the converter
	// This is safe because GetConfigValueConverter should not have side effects
	conn, err := factory(nil, nil)
	if err != nil || conn == nil {
		return nil
	}

	return conn.GetConfigValueConverter()
}

// List returns all registered protocol names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(factories))
	for protocol := range factories {
		names = append(names, protocol)
	}
	return names
}

// NewWriter creates a writer for the configured route using the registered factory.
func NewWriter(conf config.ConnectorConfig, route string, l *slog.Logger) (WriteCloser, error) {
	factory, ok := Get(conf.Type)
	if !ok {
		return nil, fmt.Errorf("unsupported protocol: %q (is it compiled in?)", conf.Type)
	}

	conn, err := factory(conf.Settings, l)
	if err != nil {
		return nil, fmt.Errorf("create connector: %w", err)
	}

	return conn.NewWriter(conf.Settings, route, l)
}

// NewReader creates a reader for the configured route using the registered factory.
func NewReader(
	conf config.ConnectorConfig, route string, autoCommit bool, l *slog.Logger,
) (ReadCloser, error) {
	factory, ok := Get(conf.Type)
	if !ok {
		return nil, fmt.Errorf("unsupported protocol: %q (is it compiled in?)", conf.Type)
	}

	conn, err := factory(conf.Settings, l)
	if err != nil {
		return nil, fmt.Errorf("create connector: %w", err)
	}

	return conn.NewReader(conf.Settings, route, autoCommit, l)
}
