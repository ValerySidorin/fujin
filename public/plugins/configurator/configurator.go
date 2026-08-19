// Package configurator provides a plugin system for configuration loaders.
// Config loaders load configuration from various sources (files, vault, etcd, etc.)
// instead of static YAML files.
package configurator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
)

// Boot loads configuration from a source.
// Load is called once at application startup.
type Configurator interface {
	// Load loads and parses configuration from the source into the provided config struct.
	// The loader is responsible for determining the format (JSON, YAML, etc.)
	// and parsing it directly into the config struct.
	// cfg must be a pointer to the configuration struct.
	Load(ctx context.Context, cfg any) error
}

// ConnectorSnapshot is one complete immutable desired connector state from a runtime source.
type ConnectorSnapshot struct {
	Revision   uint64
	Connectors connectorconfig.ConnectorsConfig
}

// ApplyState is the outcome of applying one runtime connector snapshot.
type ApplyState uint8

const (
	ApplyAccepted ApplyState = iota
	ApplyRejected
	ApplyStale
	ApplySuperseded
)

// ApplyResult reports whether one runtime connector snapshot changed the active generation.
type ApplyResult struct {
	Revision uint64
	State    ApplyState
	Changed  bool
	Err      error
}

// ConnectorRuntimeStatus is the detached readonly runtime connector projection.
type ConnectorRuntimeStatus struct {
	BuildVersion           string
	ConnectorTypes         []string
	ActiveRevision         uint64
	ActiveDigest           [32]byte
	LastRejectedRevision   uint64
	LastRejectedDiagnostic string
	RuntimeSourceConnected bool
	Catalog                connector.CatalogStatus
}

// ConnectorRuntime is the transport-neutral capability offered to runtime sources.
type ConnectorRuntime interface {
	// Submit queues a complete snapshot and resolves with its terminal result.
	Submit(context.Context, ConnectorSnapshot) <-chan ApplyResult
	SetSourceConnected(bool)
	Status() ConnectorRuntimeStatus
}

// ConnectorWatcher is an optional configurator capability for post-start connector snapshots.
// WatchConnectors blocks until ctx is canceled or the runtime source terminates.
type ConnectorWatcher interface {
	WatchConnectors(ctx context.Context, runtime ConnectorRuntime) error
}

// ConnectorBootstrapSnapshot exposes the versioned snapshot used by Load, when available.
// The returned snapshot must describe the connector configuration written during bootstrap.
type ConnectorBootstrapSnapshot interface {
	InitialConnectorSnapshot() (ConnectorSnapshot, bool)
}

// Factory creates a configurator from configuration.
// config is the loader-specific configuration (can be nil).
type Factory func(l *slog.Logger) (Configurator, error)

var (
	factories = make(map[string]Factory)
	mu        sync.RWMutex
)

// Register registers a configurator factory with the given name.
// This is typically called from init() in loader implementations.
// Returns an error if the loader is already registered.
func Register(name string, factory Factory) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[name]; exists {
		return fmt.Errorf("configurator %q already registered", name)
	}

	factories[name] = factory
	return nil
}

// Get returns a configurator factory by name.
func Get(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[name]
	return factory, ok
}

// List returns all registered configurator names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	return names
}
