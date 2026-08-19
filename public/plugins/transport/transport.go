// Package transport provides a plugin system for Fujin protocol transports (TCP, QUIC, Unix).
// gRPC transport is not a plugin due to its specific semantics.
//
// To register a transport, import it in your main package:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/transport/all"
package transport

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
)

// TransportServer is the interface for fujin-protocol transports.
type TransportServer interface {
	ListenAndServe(ctx context.Context) error
	ReadyForConnections(timeout time.Duration) bool
	Done() <-chan struct{}
}

// Config is the user-facing transport config from YAML (type + settings).
// Same pattern as connector.ConnectorConfig: each plugin parses its own Settings.
type Config struct {
	Type     string `yaml:"type"`
	Enabled  *bool  `yaml:"enabled,omitempty"` // nil = true (default)
	Settings any    `yaml:"settings"`
}

// ParseConfigFunc converts a transport Entry into typed server config.
type ParseConfigFunc func(e Config) (any, error)

// Factory creates a TransportServer from parsed config and the shared connector catalog.
type Factory func(config any, catalog *connector.Catalog, l *slog.Logger) (TransportServer, error)

type plugin struct {
	parse   ParseConfigFunc
	factory Factory
}

var (
	plugins = make(map[string]plugin)
	mu      sync.RWMutex
)

// Register registers a transport plugin. Called from init().
func Register(name string, parse ParseConfigFunc, factory Factory) error {
	mu.Lock()
	defer mu.Unlock()
	if _, exists := plugins[name]; exists {
		return fmt.Errorf("transport %q already registered", name)
	}
	plugins[name] = plugin{parse: parse, factory: factory}
	return nil
}

// Get returns parse and factory for a transport type, or false if not registered.
func Get(name string) (ParseConfigFunc, Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()
	p, ok := plugins[name]
	return p.parse, p.factory, ok
}

// List returns all registered transport names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()
	names := make([]string, 0, len(plugins))
	for n := range plugins {
		names = append(names, n)
	}
	return names
}

// ListenerFD holds a duplicated file descriptor for a listener along with metadata
// describing the listener type and address, used during graceful binary upgrades.
type ListenerFD struct {
	FD   *os.File
	Type string            // "tcp", "udp", "unix"
	Addr string            // ":4850", "/tmp/fujin.sock"
	Meta map[string]string // e.g. {"grpc":"true"}
}

// ListenerFDProvider is implemented by transports that can export their listener
// file descriptors for graceful binary upgrade via FD passing.
type ListenerFDProvider interface {
	ListenerFDs() ([]ListenerFD, error)
}

// ListenerInheritor is implemented by transports that can start serving
// on an inherited listener file descriptor from a previous process.
type ListenerInheritor interface {
	ListenAndServeInherited(ctx context.Context, fd *os.File) error
}

// FDKey returns the key used to match inherited FDs to transports during
// graceful binary upgrade. Format: "type:addr" (e.g. "tcp::4850", "unix:/tmp/fujin.sock").
// GRPC transports append ":grpc" suffix.
type FDKeyProvider interface {
	FDKey() string
}

// NewServer creates a TransportServer for the given entry.
func NewServer(e Config, catalog *connector.Catalog, l *slog.Logger) (TransportServer, error) {
	parse, factory, ok := Get(e.Type)
	if !ok {
		return nil, fmt.Errorf("transport %q not registered (available: %v)", e.Type, List())
	}
	cfg, err := parse(e)
	if err != nil {
		return nil, fmt.Errorf("parse %s config: %w", e.Type, err)
	}
	return factory(cfg, catalog, l)
}
