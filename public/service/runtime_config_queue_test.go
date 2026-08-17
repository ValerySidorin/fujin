package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/configurator"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type blockingSnapshotReloader struct {
	started chan string
	release chan struct{}

	mu      sync.Mutex
	applied []string
	reject  map[string]error
}

func (r *blockingSnapshotReloader) ReloadConnectors(config connectorconfig.ConnectorsConfig) error {
	version := config["main"].Settings.(map[string]any)["version"].(string)
	r.started <- version
	<-r.release
	r.mu.Lock()
	r.applied = append(r.applied, version)
	err := r.reject[version]
	r.mu.Unlock()
	return err
}

func (r *blockingSnapshotReloader) appliedVersions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.applied...)
}

func TestRuntimeConnectorQueueCoalescesPendingSnapshots(t *testing.T) {
	writes := make(chan string, 1)
	reloader := &blockingSnapshotReloader{
		started: make(chan string, 2),
		release: make(chan struct{}, 2),
		reject:  make(map[string]error),
	}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{
		Connectors: testConnectorConfig(t, "initial", writes),
	})
	require.NoError(t, err)
	queue := newConnectorRuntimeQueue(context.Background(), controller)
	defer queue.Close()

	first := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   1,
		Connectors: testConnectorConfig(t, "v1", writes),
	})
	require.Equal(t, "v1", <-reloader.started)
	second := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   2,
		Connectors: testConnectorConfig(t, "v2", writes),
	})
	third := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   3,
		Connectors: testConnectorConfig(t, "v3", writes),
	})
	latest := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   4,
		Connectors: testConnectorConfig(t, "v4", writes),
	})

	assert.Equal(t, configurator.ApplySuperseded, (<-second).State)
	assert.Equal(t, configurator.ApplySuperseded, (<-third).State)
	select {
	case version := <-reloader.started:
		t.Fatalf("overlapping snapshot apply started for %q", version)
	default:
	}
	reloader.release <- struct{}{}
	assert.Equal(t, configurator.ApplyAccepted, (<-first).State)
	require.Equal(t, "v4", <-reloader.started)
	reloader.release <- struct{}{}
	latestResult := <-latest
	assert.Equal(t, configurator.ApplyAccepted, latestResult.State)
	assert.True(t, latestResult.Changed)
	assert.Equal(t, []string{"v1", "v4"}, reloader.appliedVersions())
	assert.Equal(t, uint64(4), controller.ActiveRevision())
}

func TestRuntimeConnectorQueueKeepsLastGoodWhenNewestPendingFails(t *testing.T) {
	writes := make(chan string, 1)
	invalidErr := errors.New("invalid newest snapshot")
	reloader := &blockingSnapshotReloader{
		started: make(chan string, 2),
		release: make(chan struct{}, 2),
		reject:  map[string]error{"invalid": invalidErr},
	}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{
		Connectors: testConnectorConfig(t, "initial", writes),
	})
	require.NoError(t, err)
	queue := newConnectorRuntimeQueue(context.Background(), controller)
	defer queue.Close()

	accepted := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   1,
		Connectors: testConnectorConfig(t, "accepted", writes),
	})
	require.Equal(t, "accepted", <-reloader.started)
	superseded := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   2,
		Connectors: testConnectorConfig(t, "superseded", writes),
	})
	invalid := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   3,
		Connectors: testConnectorConfig(t, "invalid", writes),
	})
	assert.Equal(t, configurator.ApplySuperseded, (<-superseded).State)

	reloader.release <- struct{}{}
	assert.Equal(t, configurator.ApplyAccepted, (<-accepted).State)
	require.Equal(t, "invalid", <-reloader.started)
	reloader.release <- struct{}{}
	invalidResult := <-invalid
	assert.Equal(t, configurator.ApplyRejected, invalidResult.State)
	assert.ErrorIs(t, invalidResult.Err, invalidErr)
	assert.Equal(t, uint64(1), controller.ActiveRevision())
	assert.Equal(t, uint64(3), controller.Status().LastRejectedRevision)
}

func TestRuntimeConnectorQueueShutdownWaitsForStartedApplyAndCancelsPending(t *testing.T) {
	writes := make(chan string, 1)
	reloader := &blockingSnapshotReloader{
		started: make(chan string, 1),
		release: make(chan struct{}, 1),
		reject:  make(map[string]error),
	}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{
		Connectors: testConnectorConfig(t, "initial", writes),
	})
	require.NoError(t, err)
	queue := newConnectorRuntimeQueue(context.Background(), controller)

	started := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   1,
		Connectors: testConnectorConfig(t, "started", writes),
	})
	require.Equal(t, "started", <-reloader.started)
	pending := queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   2,
		Connectors: testConnectorConfig(t, "pending", writes),
	})

	closed := make(chan struct{})
	go func() {
		queue.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("queue closed before the started apply settled")
	case <-time.After(10 * time.Millisecond):
	}
	reloader.release <- struct{}{}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("queue did not close after the started apply settled")
	}
	assert.Equal(t, configurator.ApplyAccepted, (<-started).State)
	pendingResult := <-pending
	assert.Equal(t, configurator.ApplyRejected, pendingResult.State)
	assert.ErrorIs(t, pendingResult.Err, context.Canceled)
}

type fastSnapshotReloader struct {
	mu      sync.Mutex
	applied []uint64
}

func (r *fastSnapshotReloader) ReloadConnectors(config connectorconfig.ConnectorsConfig) error {
	version := config["main"].Settings.(map[string]any)["version"].(string)
	var revision uint64
	_, err := fmt.Sscanf(version, "v%d", &revision)
	if err != nil {
		return err
	}
	time.Sleep(50 * time.Microsecond)
	r.mu.Lock()
	r.applied = append(r.applied, revision)
	r.mu.Unlock()
	return nil
}

func TestRuntimeConnectorQueueConcurrentBurstConvergesToNewestRevision(t *testing.T) {
	writes := make(chan string, 1)
	reloader := &fastSnapshotReloader{}
	controller, err := newConnectorRuntimeController(reloader, configurator.ConnectorSnapshot{
		Connectors: testConnectorConfig(t, "initial", writes),
	})
	require.NoError(t, err)
	queue := newConnectorRuntimeQueue(context.Background(), controller)
	defer queue.Close()

	const latestRevision = 256
	results := make([]<-chan configurator.ApplyResult, latestRevision)
	var submit sync.WaitGroup
	for revision := uint64(1); revision <= latestRevision; revision++ {
		submit.Add(1)
		go func(revision uint64) {
			defer submit.Done()
			results[revision-1] = queue.Submit(context.Background(), configurator.ConnectorSnapshot{
				Revision:   revision,
				Connectors: testConnectorConfig(t, fmt.Sprintf("v%d", revision), writes),
			})
		}(revision)
	}
	submit.Wait()

	for _, resultChannel := range results {
		result := <-resultChannel
		assert.Contains(t, []configurator.ApplyState{
			configurator.ApplyAccepted,
			configurator.ApplyStale,
			configurator.ApplySuperseded,
		}, result.State)
	}
	assert.Equal(t, uint64(latestRevision), controller.ActiveRevision())

	duplicate := <-queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   latestRevision,
		Connectors: testConnectorConfig(t, fmt.Sprintf("v%d", latestRevision), writes),
	})
	assert.Equal(t, configurator.ApplyAccepted, duplicate.State)
	assert.False(t, duplicate.Changed)

	stale := <-queue.Submit(context.Background(), configurator.ConnectorSnapshot{
		Revision:   latestRevision - 1,
		Connectors: testConnectorConfig(t, fmt.Sprintf("v%d", latestRevision-1), writes),
	})
	assert.Equal(t, configurator.ApplyStale, stale.State)
}
