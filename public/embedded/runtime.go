// Package embedded runs Fujin inside a host process while keeping the data
// plane on Fujin's native or gRPC network interfaces.
package embedded

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/fujin-io/fujin/public/plugins/configurator"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/runtimeconfig"
	"github.com/fujin-io/fujin/public/server"
	"github.com/fujin-io/fujin/public/service"
	"gopkg.in/yaml.v3"
)

const defaultReadyTimeout = 30 * time.Second

var (
	ErrInvalidConfig  = errors.New("invalid embedded Fujin configuration")
	ErrStartupTimeout = errors.New("embedded Fujin startup timed out")
	ErrNoListeners    = errors.New("embedded Fujin has no enabled listeners")
)

// Options controls host-process integration. Zero values are safe.
type Options struct {
	BuildVersion string
	Logger       *slog.Logger
	ReadyTimeout time.Duration
}

// Status is the stable versioned lifecycle document exposed through the C ABI.
type Status struct {
	SchemaVersion uint32               `json:"schema_version"`
	State         string               `json:"state"`
	Endpoints     []transport.Endpoint `json:"endpoints"`
	Connectors    ConnectorStatus      `json:"connectors"`
	Error         string               `json:"error,omitempty"`
}

type ConnectorStatus struct {
	BuildVersion           string        `json:"build_version"`
	ConnectorTypes         []string      `json:"connector_types"`
	ActiveRevision         uint64        `json:"active_revision"`
	ActiveDigest           string        `json:"active_digest"`
	LastRejectedRevision   uint64        `json:"last_rejected_revision"`
	LastRejectedDiagnostic string        `json:"last_rejected_diagnostic,omitempty"`
	RuntimeSourceConnected bool          `json:"runtime_source_connected"`
	Catalog                CatalogStatus `json:"catalog"`
}

type CatalogStatus struct {
	Current           *GenerationStatus      `json:"current,omitempty"`
	Draining          []GenerationStatus     `json:"draining"`
	RetiredTotal      uint64                 `json:"retired_total"`
	RecentTransitions []GenerationTransition `json:"recent_transitions"`
}

type GenerationStatus struct {
	ID       uint64                    `json:"id"`
	State    connector.GenerationState `json:"state"`
	Bindings int64                     `json:"bindings"`
	Error    string                    `json:"error,omitempty"`
}

type GenerationTransition struct {
	Sequence uint64 `json:"sequence"`
	GenerationStatus
}

// Runtime owns one embedded Fujin server. Close is idempotent and may be
// retried with a new context after a timeout.
type Runtime struct {
	server     *server.Server
	controller *runtimeconfig.Controller
	cancel     context.CancelFunc
	done       chan struct{}
	stopOnce   sync.Once

	mu        sync.RWMutex
	serveErr  error
	endpoints []transport.Endpoint
	state     string
}

// Start decodes complete Fujin YAML or JSON configuration and waits until all
// configured listeners are ready. It performs no process-global signal or
// environment handling.
func Start(config []byte, options Options) (*Runtime, error) {
	buildVersion := options.BuildVersion
	if buildVersion == "" {
		buildVersion = "dev"
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	readyTimeout := options.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = defaultReadyTimeout
	}

	serverConfig, err := service.DecodeConfig(config, buildVersion)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	connectors, _, err := runtimeconfig.CloneSnapshot(serverConfig.Connectors)
	if err != nil {
		return nil, err
	}
	serverConfig.Connectors = connectors
	instance, err := server.NewServer(serverConfig, logger)
	if err != nil {
		return nil, err
	}
	controller, err := runtimeconfig.NewController(instance, configurator.ConnectorSnapshot{
		Connectors: connectors,
	}, buildVersion)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	runtime := &Runtime{
		server:     instance,
		controller: controller,
		cancel:     cancel,
		done:       make(chan struct{}),
		state:      "starting",
	}
	go func() {
		err := instance.ListenAndServe(ctx)
		runtime.mu.Lock()
		runtime.serveErr = err
		runtime.state = "stopped"
		runtime.mu.Unlock()
		close(runtime.done)
	}()

	ready := make(chan bool, 1)
	go func() { ready <- instance.ReadyForConnections(readyTimeout) }()
	select {
	case <-runtime.done:
		return nil, runtime.startupError()
	case ok := <-ready:
		if !ok {
			cancel()
			<-runtime.done
			if err := runtime.startupError(); !errors.Is(err, ErrNoListeners) {
				return nil, err
			}
			return nil, ErrStartupTimeout
		}
	}
	select {
	case <-runtime.done:
		return nil, runtime.startupError()
	default:
	}

	endpoints := instance.Endpoints()
	if len(endpoints) == 0 {
		cancel()
		<-runtime.done
		return nil, ErrNoListeners
	}
	runtime.mu.Lock()
	runtime.endpoints = append([]transport.Endpoint(nil), endpoints...)
	runtime.state = "ready"
	runtime.mu.Unlock()
	return runtime, nil
}

// Endpoints returns actual bound listener addresses, including ephemeral ports.
func (r *Runtime) Endpoints() []transport.Endpoint {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]transport.Endpoint(nil), r.endpoints...)
}

// ApplyConnectorSnapshot applies one complete immutable connector snapshot.
func (r *Runtime) ApplyConnectorSnapshot(ctx context.Context, revision uint64, encoded []byte) configurator.ApplyResult {
	result := configurator.ApplyResult{Revision: revision}
	if r == nil || r.controller == nil {
		result.State = configurator.ApplyRejected
		result.Err = errors.New("embedded runtime is nil")
		return result
	}
	r.mu.RLock()
	ready := r.state == "ready"
	r.mu.RUnlock()
	if !ready {
		result.State = configurator.ApplyRejected
		result.Err = errors.New("embedded runtime is not ready")
		return result
	}
	var connectors connectorconfig.ConnectorsConfig
	if err := yaml.Unmarshal(encoded, &connectors); err != nil {
		result.State = configurator.ApplyRejected
		result.Err = err
		return result
	}
	return r.controller.Apply(ctx, configurator.ConnectorSnapshot{Revision: revision, Connectors: connectors})
}

// Status returns a detached projection suitable for serialization across an ABI.
func (r *Runtime) Status() Status {
	if r == nil {
		return Status{SchemaVersion: 1, State: "invalid"}
	}
	r.mu.RLock()
	status := Status{
		SchemaVersion: 1,
		State:         r.state,
		Endpoints:     append([]transport.Endpoint(nil), r.endpoints...),
	}
	if r.serveErr != nil {
		status.Error = r.serveErr.Error()
	}
	r.mu.RUnlock()
	status.Connectors = connectorStatus(r.controller.Status())
	return status
}

func connectorStatus(status configurator.ConnectorRuntimeStatus) ConnectorStatus {
	return ConnectorStatus{
		BuildVersion:           status.BuildVersion,
		ConnectorTypes:         append([]string(nil), status.ConnectorTypes...),
		ActiveRevision:         status.ActiveRevision,
		ActiveDigest:           hex.EncodeToString(status.ActiveDigest[:]),
		LastRejectedRevision:   status.LastRejectedRevision,
		LastRejectedDiagnostic: status.LastRejectedDiagnostic,
		RuntimeSourceConnected: status.RuntimeSourceConnected,
		Catalog:                catalogStatus(status.Catalog),
	}
}

func catalogStatus(status connector.CatalogStatus) CatalogStatus {
	converted := CatalogStatus{
		Draining:          make([]GenerationStatus, len(status.Draining)),
		RetiredTotal:      status.RetiredTotal,
		RecentTransitions: make([]GenerationTransition, len(status.RecentTransitions)),
	}
	if status.Current != nil {
		current := generationStatus(*status.Current)
		converted.Current = &current
	}
	for i, generation := range status.Draining {
		converted.Draining[i] = generationStatus(generation)
	}
	for i, transition := range status.RecentTransitions {
		converted.RecentTransitions[i] = GenerationTransition{
			Sequence:         transition.Sequence,
			GenerationStatus: generationStatus(transition.GenerationStatus),
		}
	}
	return converted
}

func generationStatus(status connector.GenerationStatus) GenerationStatus {
	return GenerationStatus{
		ID:       uint64(status.ID),
		State:    status.State,
		Bindings: status.Bindings,
		Error:    status.Error,
	}
}

// Close requests shutdown and waits for deterministic cleanup until ctx ends.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.stopOnce.Do(func() {
		r.mu.Lock()
		if r.state != "stopped" {
			r.state = "stopping"
		}
		r.mu.Unlock()
		r.cancel()
	})
	select {
	case <-r.done:
		r.mu.RLock()
		err := r.serveErr
		r.mu.RUnlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) startupError() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.serveErr != nil {
		return r.serveErr
	}
	return ErrNoListeners
}
