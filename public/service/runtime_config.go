package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/configurator"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"gopkg.in/yaml.v3"
)

// connectorReloader is the server capability owned by runtime connector configuration.
type connectorReloader interface {
	ReloadConnectors(connectorconfig.ConnectorsConfig) error
}

type connectorCatalogStatusProvider interface {
	ConnectorCatalogStatus() connector.CatalogStatus
}

// connectorRuntimeController serializes complete connector snapshot publication.
type connectorRuntimeController struct {
	reloader connectorReloader

	mu                     sync.Mutex
	activeRevision         uint64
	activeDigest           [sha256.Size]byte
	lastRejectedRevision   uint64
	lastRejectedDiagnostic string
	sourceConnected        bool
}

func newConnectorRuntimeController(
	reloader connectorReloader,
	initial configurator.ConnectorSnapshot,
) (*connectorRuntimeController, error) {
	if reloader == nil {
		return nil, errors.New("runtime connector reloader is nil")
	}
	_, digest, err := snapshotConnectorConfig(initial.Connectors)
	if err != nil {
		return nil, fmt.Errorf("snapshot initial connector config: %w", err)
	}
	return &connectorRuntimeController{
		reloader:       reloader,
		activeRevision: initial.Revision,
		activeDigest:   digest,
	}, nil
}

func (c *connectorRuntimeController) ActiveRevision() uint64 {
	return c.Status().ActiveRevision
}

func (c *connectorRuntimeController) Status() configurator.ConnectorRuntimeStatus {
	c.mu.Lock()
	connectorTypes := connector.List()
	sort.Strings(connectorTypes)
	status := configurator.ConnectorRuntimeStatus{
		BuildVersion:           Version,
		ConnectorTypes:         connectorTypes,
		ActiveRevision:         c.activeRevision,
		ActiveDigest:           c.activeDigest,
		LastRejectedRevision:   c.lastRejectedRevision,
		LastRejectedDiagnostic: c.lastRejectedDiagnostic,
		RuntimeSourceConnected: c.sourceConnected,
	}
	c.mu.Unlock()
	if provider, ok := c.reloader.(connectorCatalogStatusProvider); ok {
		status.Catalog = provider.ConnectorCatalogStatus()
	}
	return status
}

func (c *connectorRuntimeController) setSourceConnected(connected bool) {
	c.mu.Lock()
	c.sourceConnected = connected
	c.mu.Unlock()
}

// connectorBootstrapSnapshot binds optional source metadata to the configuration loaded into Fujin.
func connectorBootstrapSnapshot(
	loader configurator.Configurator,
	connectors connectorconfig.ConnectorsConfig,
) (configurator.ConnectorSnapshot, error) {
	loaded, loadedDigest, err := snapshotConnectorConfig(connectors)
	if err != nil {
		return configurator.ConnectorSnapshot{}, err
	}
	initial := configurator.ConnectorSnapshot{Connectors: loaded}
	source, ok := loader.(configurator.ConnectorBootstrapSnapshot)
	if !ok {
		return initial, nil
	}
	declared, ok := source.InitialConnectorSnapshot()
	if !ok {
		return initial, nil
	}
	declaredConnectors, declaredDigest, err := snapshotConnectorConfig(declared.Connectors)
	if err != nil {
		return configurator.ConnectorSnapshot{}, fmt.Errorf("snapshot declared bootstrap connectors: %w", err)
	}
	if declaredDigest != loadedDigest {
		return configurator.ConnectorSnapshot{}, errors.New("configurator bootstrap snapshot does not match loaded connectors")
	}
	declared.Connectors = declaredConnectors
	return declared, nil
}

// Apply validates ordering and atomically publishes one complete connector snapshot.
func (c *connectorRuntimeController) Apply(
	ctx context.Context,
	snapshot configurator.ConnectorSnapshot,
) configurator.ApplyResult {
	result := configurator.ApplyResult{Revision: snapshot.Revision}
	c.mu.Lock()
	defer c.mu.Unlock()

	connectors, digest, err := snapshotConnectorConfig(snapshot.Connectors)
	if err != nil {
		return c.rejectLocked(result, fmt.Errorf("snapshot connector config: %w", err))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return c.rejectLocked(result, err)
	}
	if snapshot.Revision < c.activeRevision {
		result.State = configurator.ApplyStale
		return result
	}
	if snapshot.Revision == c.activeRevision {
		if digest == c.activeDigest {
			result.State = configurator.ApplyAccepted
			return result
		}
		return c.rejectLocked(result, fmt.Errorf(
			"connector snapshot revision %d conflicts with active content",
			snapshot.Revision,
		))
	}
	if err := c.reloader.ReloadConnectors(connectors); err != nil {
		return c.rejectLocked(result, err)
	}
	c.activeRevision = snapshot.Revision
	c.activeDigest = digest
	result.State = configurator.ApplyAccepted
	result.Changed = true
	return result
}

func (c *connectorRuntimeController) rejectLocked(
	result configurator.ApplyResult,
	err error,
) configurator.ApplyResult {
	result.State = configurator.ApplyRejected
	result.Err = err
	c.lastRejectedRevision = result.Revision
	c.lastRejectedDiagnostic = err.Error()
	return result
}

func startConnectorWatcher(
	ctx context.Context,
	loader configurator.Configurator,
	controller *connectorRuntimeController,
) <-chan error {
	watcher, ok := loader.(configurator.ConnectorWatcher)
	if !ok {
		return nil
	}
	done := make(chan error, 1)
	go func() {
		defer close(done)
		runtime := newConnectorRuntimeQueue(ctx, controller)
		defer runtime.Close()
		done <- watcher.WatchConnectors(ctx, runtime)
	}()
	return done
}

func reloadConnectorsFromConfigurator(
	ctx context.Context,
	loader configurator.Configurator,
	controller *connectorRuntimeController,
	revision *uint64,
) error {
	if _, watcherOwnsConnectors := loader.(configurator.ConnectorWatcher); watcherOwnsConnectors {
		return nil
	}
	var next Config
	if err := loader.Load(ctx, &next); err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	*revision = *revision + 1
	result := controller.Apply(ctx, configurator.ConnectorSnapshot{
		Revision:   *revision,
		Connectors: next.Connectors,
	})
	return result.Err
}

func snapshotConnectorConfig(
	connectors connectorconfig.ConnectorsConfig,
) (connectorconfig.ConnectorsConfig, [sha256.Size]byte, error) {
	clone := make(connectorconfig.ConnectorsConfig, len(connectors))
	for name, config := range connectors {
		copied, err := connectorconfig.CloneConnectorConfig(config)
		if err != nil {
			return nil, [sha256.Size]byte{}, fmt.Errorf("connector %q: %w", name, err)
		}
		clone[name] = copied
	}
	encoded, err := yaml.Marshal(clone)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("encode connector snapshot: %w", err)
	}
	return clone, sha256.Sum256(encoded), nil
}
