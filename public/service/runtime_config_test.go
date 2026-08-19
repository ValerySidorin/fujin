package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fujin-io/fujin/internal/core"
	"github.com/fujin-io/fujin/public/plugins/configurator"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type snapshotTestConfigurator struct {
	loadCount atomic.Int32
	snapshots chan configurator.ConnectorSnapshot
	results   chan configurator.ApplyResult
	started   chan struct{}
}

func (c *snapshotTestConfigurator) Load(context.Context, any) error {
	c.loadCount.Add(1)
	return nil
}

func (c *snapshotTestConfigurator) WatchConnectors(
	ctx context.Context,
	runtime configurator.ConnectorRuntime,
) error {
	runtime.SetSourceConnected(true)
	defer runtime.SetSourceConnected(false)
	close(c.started)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case snapshot := <-c.snapshots:
			c.results <- <-runtime.Submit(ctx, snapshot)
		}
	}
}

type catalogReloader struct {
	catalog *connector.Catalog
}

func (r catalogReloader) ReloadConnectors(config connectorconfig.ConnectorsConfig) error {
	return r.catalog.Reload(config)
}

func (r catalogReloader) ConnectorCatalogStatus() connector.CatalogStatus {
	return r.catalog.Status()
}

type snapshotRuntime struct {
	version string
	writes  chan string
}

func (*snapshotRuntime) NewReader(string, bool, *slog.Logger) (connector.ReadCloser, error) {
	return nil, connector.ErrOperationUnsupported
}

func (r *snapshotRuntime) NewWriter(string, *slog.Logger) (connector.WriteCloser, error) {
	return &snapshotWriter{version: r.version, writes: r.writes}, nil
}

func (*snapshotRuntime) Close(context.Context) error { return nil }

type snapshotWriter struct {
	version string
	writes  chan string
}

func (w *snapshotWriter) Produce(_ context.Context, _ []byte, callback func(error)) {
	w.writes <- w.version
	callback(nil)
}

func (w *snapshotWriter) HProduce(ctx context.Context, message []byte, _ [][]byte, callback func(error)) {
	w.Produce(ctx, message, callback)
}

func (*snapshotWriter) Flush(context.Context) error      { return nil }
func (*snapshotWriter) BeginTx(context.Context) error    { return connector.ErrOperationUnsupported }
func (*snapshotWriter) CommitTx(context.Context) error   { return connector.ErrOperationUnsupported }
func (*snapshotWriter) RollbackTx(context.Context) error { return connector.ErrOperationUnsupported }
func (*snapshotWriter) Close() error                     { return nil }

var (
	registerSnapshotConnector sync.Once
	snapshotWritesMu          sync.Mutex
	snapshotWrites            chan string
)

func testConnectorConfig(t *testing.T, version string, writes chan string) connectorconfig.ConnectorsConfig {
	t.Helper()
	snapshotWritesMu.Lock()
	snapshotWrites = writes
	snapshotWritesMu.Unlock()
	registerSnapshotConnector.Do(func() {
		require.NoError(t, connector.Register("service_runtime_snapshot", connector.Descriptor{
			Compile: func(settings any) (connector.Compiled, error) {
				values, ok := settings.(map[string]any)
				if !ok {
					return nil, errors.New("settings must be an object")
				}
				version, ok := values["version"].(string)
				if !ok || version == "" {
					return nil, errors.New("version is required")
				}
				snapshotWritesMu.Lock()
				writes := snapshotWrites
				snapshotWritesMu.Unlock()
				return connector.StaticCompiled(
					map[string]connector.RouteProfile{"route": {
						Produce:          true,
						ProduceGuarantee: connector.AcceptancePeer,
					}},
					func(*slog.Logger) (connector.Runtime, error) {
						return &snapshotRuntime{version: version, writes: writes}, nil
					},
				)
			},
		}))
	})
	return connectorconfig.ConnectorsConfig{
		"main": {Type: "service_runtime_snapshot", Settings: map[string]any{"version": version}},
	}
}

func TestConnectorWatcherPublishesSnapshotAndKeepsBoundGeneration(t *testing.T) {
	writes := make(chan string, 2)
	initialConfig := testConnectorConfig(t, "v1", writes)
	catalog, err := connector.CompileCatalog(initialConfig, slog.Default())
	require.NoError(t, err)
	initialGeneration := catalog.Current()

	oldSession := core.New(context.Background(), initialGeneration, catalog.Current, slog.Default())
	_, err = oldSession.Bind("main", nil, nil)
	require.NoError(t, err)

	controller, err := newConnectorRuntimeController(catalogReloader{catalog: catalog}, configurator.ConnectorSnapshot{
		Revision:   1,
		Connectors: initialConfig,
	})
	require.NoError(t, err)
	watcher := &snapshotTestConfigurator{
		snapshots: make(chan configurator.ConnectorSnapshot),
		results:   make(chan configurator.ApplyResult),
		started:   make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := startConnectorWatcher(ctx, watcher, controller)
	require.NotNil(t, done)
	select {
	case <-watcher.started:
	case <-time.After(time.Second):
		t.Fatal("connector watcher did not start")
	}
	status := controller.Status()
	assert.True(t, status.RuntimeSourceConnected)
	assert.Equal(t, uint64(1), status.ActiveRevision)
	require.NotNil(t, status.Catalog.Current)
	assert.Equal(t, initialGeneration.ID(), status.Catalog.Current.ID)

	watcher.snapshots <- configurator.ConnectorSnapshot{
		Revision:   2,
		Connectors: testConnectorConfig(t, "v2", writes),
	}
	result := <-watcher.results
	require.Equal(t, configurator.ApplyAccepted, result.State)
	require.True(t, result.Changed)
	require.NotSame(t, initialGeneration, catalog.Current())

	newSession := core.New(context.Background(), catalog.Current(), catalog.Current, slog.Default())
	_, err = newSession.Bind("main", nil, nil)
	require.NoError(t, err)

	oldCallback := make(chan error, 1)
	require.NoError(t, oldSession.Produce("route", []byte("old"), nil, func(err error) { oldCallback <- err }))
	require.NoError(t, <-oldCallback)
	require.Equal(t, "v1", <-writes)

	newCallback := make(chan error, 1)
	require.NoError(t, newSession.Produce("route", []byte("new"), nil, func(err error) { newCallback <- err }))
	require.NoError(t, <-newCallback)
	require.Equal(t, "v2", <-writes)

	current := catalog.Current()
	watcher.snapshots <- configurator.ConnectorSnapshot{
		Revision:   3,
		Connectors: connectorconfig.ConnectorsConfig{"main": {Type: "missing"}},
	}
	result = <-watcher.results
	require.Equal(t, configurator.ApplyRejected, result.State)
	require.Error(t, result.Err)
	require.Same(t, current, catalog.Current())
	status = controller.Status()
	assert.Equal(t, uint64(2), status.ActiveRevision)
	assert.Equal(t, uint64(3), status.LastRejectedRevision)
	assert.NotEmpty(t, status.LastRejectedDiagnostic)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool {
		return !controller.Status().RuntimeSourceConnected
	}, time.Second, time.Millisecond)
	require.NoError(t, oldSession.Close())
	require.NoError(t, newSession.Close())
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	require.NoError(t, initialGeneration.WaitClosed(waitCtx))
	require.Eventually(t, func() bool {
		status := controller.Status()
		return status.Catalog.RetiredTotal >= 1
	}, time.Second, time.Millisecond)
	require.NoError(t, catalog.Close(waitCtx))
}

type recordingReloader struct {
	mu      sync.Mutex
	calls   int
	configs []connectorconfig.ConnectorsConfig
	err     error
}

func (r *recordingReloader) ReloadConnectors(config connectorconfig.ConnectorsConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.configs = append(r.configs, config)
	return r.err
}

func (r *recordingReloader) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestRuntimeConnectorControllerOrdersAndDeduplicatesSnapshots(t *testing.T) {
	writes := make(chan string, 1)
	initial := testConnectorConfig(t, "v1", writes)
	reloader := &recordingReloader{}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{
		Revision:   10,
		Connectors: initial,
	})
	require.NoError(t, err)

	result := controller.Apply(context.Background(), configurator.ConnectorSnapshot{Revision: 9, Connectors: initial})
	assert.Equal(t, configurator.ApplyStale, result.State)
	assert.Zero(t, reloader.callCount())

	result = controller.Apply(context.Background(), configurator.ConnectorSnapshot{Revision: 10, Connectors: initial})
	assert.Equal(t, configurator.ApplyAccepted, result.State)
	assert.False(t, result.Changed)
	assert.Zero(t, reloader.callCount())

	result = controller.Apply(context.Background(), configurator.ConnectorSnapshot{
		Revision:   10,
		Connectors: testConnectorConfig(t, "collision", writes),
	})
	assert.Equal(t, configurator.ApplyRejected, result.State)
	assert.ErrorContains(t, result.Err, "conflicts")
	assert.Zero(t, reloader.callCount())

	result = controller.Apply(context.Background(), configurator.ConnectorSnapshot{
		Revision:   11,
		Connectors: testConnectorConfig(t, "v2", writes),
	})
	assert.Equal(t, configurator.ApplyAccepted, result.State)
	assert.True(t, result.Changed)
	assert.Equal(t, 1, reloader.callCount())
	assert.Equal(t, uint64(11), controller.ActiveRevision())

	reloader.err = errors.New("compile failed")
	result = controller.Apply(context.Background(), configurator.ConnectorSnapshot{
		Revision:   12,
		Connectors: testConnectorConfig(t, "v3", writes),
	})
	assert.Equal(t, configurator.ApplyRejected, result.State)
	assert.ErrorIs(t, result.Err, reloader.err)
	assert.Equal(t, uint64(11), controller.ActiveRevision())
}

type overlapReloader struct {
	active atomic.Int32
	max    atomic.Int32
}

func (r *overlapReloader) ReloadConnectors(connectorconfig.ConnectorsConfig) error {
	active := r.active.Add(1)
	for {
		maximum := r.max.Load()
		if active <= maximum || r.max.CompareAndSwap(maximum, active) {
			break
		}
	}
	time.Sleep(time.Millisecond)
	r.active.Add(-1)
	return nil
}

func TestRuntimeConnectorControllerSerializesConcurrentApply(t *testing.T) {
	writes := make(chan string, 1)
	reloader := &overlapReloader{}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{
		Connectors: testConnectorConfig(t, "initial", writes),
	})
	require.NoError(t, err)

	const revisions = 64
	var wg sync.WaitGroup
	for revision := uint64(1); revision <= revisions; revision++ {
		wg.Add(1)
		go func(revision uint64) {
			defer wg.Done()
			result := controller.Apply(context.Background(), configurator.ConnectorSnapshot{
				Revision:   revision,
				Connectors: testConnectorConfig(t, fmt.Sprintf("v%d", revision), writes),
			})
			assert.NotEqual(t, configurator.ApplyRejected, result.State)
		}(revision)
	}
	wg.Wait()

	assert.Equal(t, int32(1), reloader.max.Load())
	assert.Equal(t, uint64(revisions), controller.ActiveRevision())
}

func TestLoadConfigWithLoaderRetainsConstructedConfigurator(t *testing.T) {
	loader := &snapshotTestConfigurator{}
	name := "service_runtime_snapshot_loader"
	require.NoError(t, configurator.Register(name, func(*slog.Logger) (configurator.Configurator, error) {
		return loader, nil
	}))

	loaded, err := loadConfigWithLoader(context.Background(), name, &Config{})
	require.NoError(t, err)
	require.Same(t, loader, loaded)
	require.Equal(t, int32(1), loader.loadCount.Load())
}

type reloadTestConfigurator struct {
	loads      atomic.Int32
	connectors connectorconfig.ConnectorsConfig
}

func (c *reloadTestConfigurator) Load(_ context.Context, cfg any) error {
	c.loads.Add(1)
	target, ok := cfg.(*Config)
	if !ok {
		return errors.New("unexpected config target")
	}
	target.Connectors = c.connectors
	return nil
}

func TestRuntimeConnectorReloadHasOneOwningSource(t *testing.T) {
	writes := make(chan string, 1)
	initial := testConnectorConfig(t, "initial", writes)
	reloader := &recordingReloader{}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{Connectors: initial})
	require.NoError(t, err)
	revision := controller.ActiveRevision()

	watcher := &snapshotTestConfigurator{}
	require.NoError(t, reloadConnectorsFromConfigurator(context.Background(), watcher, controller, &revision))
	assert.Zero(t, watcher.loadCount.Load())
	assert.Zero(t, reloader.callCount())

	startupOnly := &reloadTestConfigurator{connectors: testConnectorConfig(t, "reloaded", writes)}
	require.NoError(t, reloadConnectorsFromConfigurator(context.Background(), startupOnly, controller, &revision))
	assert.Equal(t, int32(1), startupOnly.loads.Load())
	assert.Equal(t, 1, reloader.callCount())
	assert.Equal(t, uint64(1), revision)
	assert.Equal(t, uint64(1), controller.ActiveRevision())
}

type terminalSnapshotConfigurator struct {
	err error
}

func (*terminalSnapshotConfigurator) Load(context.Context, any) error { return nil }

func (c *terminalSnapshotConfigurator) WatchConnectors(
	_ context.Context,
	runtime configurator.ConnectorRuntime,
) error {
	runtime.SetSourceConnected(true)
	defer runtime.SetSourceConnected(false)
	return c.err
}

func TestTerminalWatcherFailureLeavesControllerUsable(t *testing.T) {
	writes := make(chan string, 1)
	reloader := &recordingReloader{}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{
		Connectors: testConnectorConfig(t, "initial", writes),
	})
	require.NoError(t, err)

	terminalErr := errors.New("source stopped")
	done := startConnectorWatcher(context.Background(), &terminalSnapshotConfigurator{err: terminalErr}, controller)
	require.ErrorIs(t, <-done, terminalErr)

	result := controller.Apply(context.Background(), configurator.ConnectorSnapshot{
		Revision:   1,
		Connectors: testConnectorConfig(t, "after-source-failure", writes),
	})
	require.Equal(t, configurator.ApplyAccepted, result.State)
	require.True(t, result.Changed)
}

func TestStopRuntimeSourcesWaitsForWatcherSettlement(t *testing.T) {
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	settled := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		stopRuntimeSources(runtimeCancel, settled)
		close(returned)
	}()

	select {
	case <-runtimeCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("runtime source was not canceled")
	}
	select {
	case <-returned:
		t.Fatal("shutdown continued before watcher settlement")
	case <-time.After(10 * time.Millisecond):
	}
	close(settled)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not continue after watcher settlement")
	}
}
