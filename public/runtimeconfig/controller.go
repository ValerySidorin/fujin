// Package runtimeconfig owns transport-neutral runtime connector snapshot semantics.
package runtimeconfig

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

// Reloader atomically publishes a complete connector configuration.
type Reloader interface {
	ReloadConnectors(connectorconfig.ConnectorsConfig) error
}

type catalogStatusProvider interface {
	ConnectorCatalogStatus() connector.CatalogStatus
}

// Controller serializes complete connector snapshots and enforces monotonic revisions.
type Controller struct {
	reloader     Reloader
	buildVersion string

	mu                     sync.Mutex
	activeRevision         uint64
	activeDigest           [sha256.Size]byte
	lastRejectedRevision   uint64
	lastRejectedDiagnostic string
	sourceConnected        bool
}

// NewController records initial as the already-published connector snapshot.
func NewController(reloader Reloader, initial configurator.ConnectorSnapshot, buildVersion string) (*Controller, error) {
	if reloader == nil {
		return nil, errors.New("runtime connector reloader is nil")
	}
	_, digest, err := CloneSnapshot(initial.Connectors)
	if err != nil {
		return nil, fmt.Errorf("snapshot initial connector config: %w", err)
	}
	return &Controller{
		reloader:       reloader,
		buildVersion:   buildVersion,
		activeRevision: initial.Revision,
		activeDigest:   digest,
	}, nil
}

// ActiveRevision returns the active snapshot revision.
func (c *Controller) ActiveRevision() uint64 { return c.Status().ActiveRevision }

// Status returns a detached runtime connector projection.
func (c *Controller) Status() configurator.ConnectorRuntimeStatus {
	c.mu.Lock()
	connectorTypes := connector.List()
	sort.Strings(connectorTypes)
	status := configurator.ConnectorRuntimeStatus{
		BuildVersion:           c.buildVersion,
		ConnectorTypes:         connectorTypes,
		ActiveRevision:         c.activeRevision,
		ActiveDigest:           c.activeDigest,
		LastRejectedRevision:   c.lastRejectedRevision,
		LastRejectedDiagnostic: c.lastRejectedDiagnostic,
		RuntimeSourceConnected: c.sourceConnected,
	}
	c.mu.Unlock()
	if provider, ok := c.reloader.(catalogStatusProvider); ok {
		status.Catalog = provider.ConnectorCatalogStatus()
	}
	return status
}

// SetSourceConnected updates the source-connectivity projection.
func (c *Controller) SetSourceConnected(connected bool) {
	c.mu.Lock()
	c.sourceConnected = connected
	c.mu.Unlock()
}

// Apply validates ordering and atomically publishes one complete connector snapshot.
func (c *Controller) Apply(ctx context.Context, snapshot configurator.ConnectorSnapshot) configurator.ApplyResult {
	result := configurator.ApplyResult{Revision: snapshot.Revision}
	c.mu.Lock()
	defer c.mu.Unlock()

	connectors, digest, err := CloneSnapshot(snapshot.Connectors)
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
		return c.rejectLocked(result, fmt.Errorf("connector snapshot revision %d conflicts with active content", snapshot.Revision))
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

func (c *Controller) rejectLocked(result configurator.ApplyResult, err error) configurator.ApplyResult {
	result.State = configurator.ApplyRejected
	result.Err = err
	c.lastRejectedRevision = result.Revision
	c.lastRejectedDiagnostic = err.Error()
	return result
}

// CloneSnapshot deep-clones connector configuration and returns its stable digest.
func CloneSnapshot(connectors connectorconfig.ConnectorsConfig) (connectorconfig.ConnectorsConfig, [sha256.Size]byte, error) {
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
