package connector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testReader struct{}

func (*testReader) Subscribe(context.Context, func() error, func([]byte, string, ...any)) error {
	return nil
}
func (*testReader) SubscribeWithHeaders(context.Context, func() error, func([]byte, string, [][]byte, ...any)) error {
	return nil
}
func (*testReader) Fetch(context.Context, uint32, func(uint32, error), func([]byte, string, ...any)) {
}
func (*testReader) FetchWithHeaders(context.Context, uint32, func(uint32, error), func([]byte, string, [][]byte, ...any)) {
}
func (*testReader) Ack(context.Context, [][]byte, func(error), func([]byte, error))  {}
func (*testReader) Nack(context.Context, [][]byte, func(error), func([]byte, error)) {}
func (*testReader) MsgIDArgsLen() int                                                { return 0 }
func (*testReader) EncodeMsgID(buf []byte, _ string, _ ...any) []byte                { return buf }
func (*testReader) AutoCommit() bool                                                 { return false }
func (*testReader) Close() error                                                     { return nil }

type testWriter struct {
	mu        sync.Mutex
	callbacks []func(error)
	closed    atomic.Int32
}

func (w *testWriter) Produce(_ context.Context, _ []byte, callback func(error)) {
	w.mu.Lock()
	w.callbacks = append(w.callbacks, callback)
	w.mu.Unlock()
}
func (w *testWriter) HProduce(ctx context.Context, _ []byte, _ [][]byte, callback func(error)) {
	w.Produce(ctx, nil, callback)
}
func (*testWriter) Flush(context.Context) error      { return nil }
func (*testWriter) BeginTx(context.Context) error    { return nil }
func (*testWriter) CommitTx(context.Context) error   { return nil }
func (*testWriter) RollbackTx(context.Context) error { return nil }
func (w *testWriter) Close() error                   { w.closed.Add(1); return nil }
func (w *testWriter) complete(index int, err error) {
	w.mu.Lock()
	callback := w.callbacks[index]
	w.mu.Unlock()
	callback(err)
}

type compliantTestWriter struct{ testWriter }

func (*compliantTestWriter) WriterContractCompliant() {}

func testDescriptor(profile RouteProfile, runtime Runtime, closed *atomic.Int32) Descriptor {
	return Descriptor{
		Converter: func(_ string, value string) (any, error) { return value, nil },
		Compile: func(any) (Compiled, error) {
			return StaticCompiled(map[string]RouteProfile{"route": profile}, func(*slog.Logger) (Runtime, error) {
				if runtime != nil {
					return runtime, nil
				}
				return &testRuntime{closed: closed}, nil
			})
		},
	}
}

type testRuntime struct{ closed *atomic.Int32 }

func (*testRuntime) NewReader(string, bool, *slog.Logger) (ReadCloser, error) {
	return &testReader{}, nil
}
func (*testRuntime) NewWriter(string, *slog.Logger) (WriteCloser, error) { return &testWriter{}, nil }
func (r *testRuntime) Close(context.Context) error {
	if r.closed != nil {
		r.closed.Add(1)
	}
	return nil
}

func validProfile() RouteProfile {
	return RouteProfile{
		Produce: true, Headers: true, Transactions: true, Subscribe: true, Fetch: true,
		ManualSettlement: true, ProduceGuarantee: AcceptancePeer,
		Settlement: SettlementProfile{Ack: AckSingle, Nack: NackRequeue},
	}
}

func TestValidateHeadersCanonicalMultimap(t *testing.T) {
	require.NoError(t, ValidateHeaders(nil))
	require.NoError(t, ValidateHeaders([][]byte{[]byte("k"), {0xff}, []byte("k"), []byte("v")}))
	assert.ErrorIs(t, ValidateHeaders([][]byte{[]byte("k")}), ErrInvalidHeaders)
	assert.ErrorIs(t, ValidateHeaders([][]byte{nil, []byte("v")}), ErrInvalidHeaders)
	assert.ErrorIs(t, ValidateHeaders([][]byte{{0xff}, []byte("v")}), ErrInvalidHeaders)
}

func TestRouteProfileRejectsContradictions(t *testing.T) {
	assert.Error(t, (RouteProfile{Produce: true}).Validate("route"))
	assert.Error(t, (RouteProfile{Transactions: true}).Validate("route"))
	assert.Error(t, (RouteProfile{ManualSettlement: true, Subscribe: true}).Validate("route"))
	require.NoError(t, validProfile().Validate("route"))
}

func TestDescriptorRegistryAndConverter(t *testing.T) {
	name := "connector_descriptor_registry_test"
	require.NoError(t, Register(name, testDescriptor(validProfile(), nil, nil)))
	assert.Error(t, Register(name, testDescriptor(validProfile(), nil, nil)))
	descriptor, ok := Get(name)
	require.True(t, ok)
	require.NotNil(t, descriptor.Compile)
	converted, err := GetConfigValueConverter(name)("route", "value")
	require.NoError(t, err)
	assert.Equal(t, "value", converted)
	assert.Contains(t, List(), name)
}

func TestCatalogReloadPublishesOnlyValidGeneration(t *testing.T) {
	name := "connector_catalog_reload_test"
	require.NoError(t, Register(name, testDescriptor(validProfile(), nil, nil)))
	catalog, err := CompileCatalog(connectorconfig.ConnectorsConfig{"old": {Type: name}}, slog.Default())
	require.NoError(t, err)
	old := catalog.Current()
	require.Error(t, catalog.Reload(connectorconfig.ConnectorsConfig{"bad": {Type: "missing"}}))
	assert.Same(t, old, catalog.Current())
	require.NoError(t, catalog.Reload(connectorconfig.ConnectorsConfig{"new": {Type: name}}))
	assert.NotSame(t, old, catalog.Current())
}
func TestGenerationCopiesMutableConfigurationAndProfiles(t *testing.T) {
	name := "connector_generation_immutability_test"
	profiles := map[string]RouteProfile{"route": validProfile()}
	require.NoError(t, Register(name, Descriptor{Compile: func(any) (Compiled, error) {
		return StaticCompiled(profiles, func(*slog.Logger) (Runtime, error) { return &testRuntime{}, nil })
	}}))
	settings := map[string]any{"nested": map[string]any{"value": "original"}}
	config := connectorconfig.ConnectorConfig{Type: name, Overridable: []string{"nested.value"}, Settings: settings}
	generation, err := CompileGeneration(connectorconfig.ConnectorsConfig{"connector": config}, slog.Default())
	require.NoError(t, err)

	profiles["route"] = RouteProfile{}
	settings["nested"].(map[string]any)["value"] = "mutated"
	config.Overridable[0] = "mutated"

	stored, ok := generation.Config("connector")
	require.True(t, ok)
	assert.Equal(t, "original", stored.Settings.(map[string]any)["nested"].(map[string]any)["value"])
	assert.Equal(t, []string{"nested.value"}, stored.Overridable)
	binding, err := generation.Acquire("connector")
	require.NoError(t, err)
	profile, err := binding.RouteProfile("route")
	require.NoError(t, err)
	assert.Equal(t, validProfile(), profile)
	binding.Close()
}

func TestGenerationConfigReturnsCallerOwnedCopy(t *testing.T) {
	name := "connector_generation_config_copy_test"
	require.NoError(t, Register(name, testDescriptor(validProfile(), nil, nil)))
	generation, err := CompileGeneration(connectorconfig.ConnectorsConfig{"connector": {
		Type: name, Settings: map[string]any{"nested": map[string]any{"value": "original"}},
	}}, slog.Default())
	require.NoError(t, err)

	first, ok := generation.Config("connector")
	require.True(t, ok)
	first.Settings.(map[string]any)["nested"].(map[string]any)["value"] = "mutated"
	second, ok := generation.Config("connector")
	require.True(t, ok)
	assert.Equal(t, "original", second.Settings.(map[string]any)["nested"].(map[string]any)["value"])
}

func TestRetiredGenerationClosesAfterLastBinding(t *testing.T) {
	var closed atomic.Int32
	name := "connector_generation_lifetime_test"
	require.NoError(t, Register(name, testDescriptor(validProfile(), nil, &closed)))
	catalog, err := CompileCatalog(connectorconfig.ConnectorsConfig{"connector": {Type: name}}, slog.Default())
	require.NoError(t, err)
	generation := catalog.Current()
	binding, err := generation.Acquire("connector")
	require.NoError(t, err)
	_, err = binding.NewWriter("route", slog.Default())
	require.NoError(t, err)
	generation.Retire()
	assert.Zero(t, closed.Load())
	binding.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, generation.WaitClosed(ctx))
	assert.Equal(t, int32(1), closed.Load())
}

func TestCatalogStatusTracksImmediateGenerationRetirement(t *testing.T) {
	name := "connector_catalog_status_immediate_test"
	require.NoError(t, Register(name, testDescriptor(validProfile(), nil, nil)))
	catalog, err := CompileCatalog(connectorconfig.ConnectorsConfig{"old": {Type: name}}, slog.Default())
	require.NoError(t, err)
	oldID := catalog.Current().ID()

	require.NoError(t, catalog.Reload(connectorconfig.ConnectorsConfig{"new": {Type: name}}))
	require.Eventually(t, func() bool {
		status := catalog.Status()
		return status.RetiredTotal == 1 && len(status.Draining) == 0
	}, time.Second, time.Millisecond)

	status := catalog.Status()
	require.NotNil(t, status.Current)
	assert.NotEqual(t, oldID, status.Current.ID)
	counts := map[GenerationState]int{}
	for _, transition := range status.RecentTransitions {
		if transition.ID == oldID {
			counts[transition.State]++
		}
	}
	assert.Equal(t, 1, counts[GenerationPublished])
	assert.Equal(t, 1, counts[GenerationDraining])
	assert.Equal(t, 1, counts[GenerationRetired])

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, catalog.Close(ctx))
}

func TestCatalogStatusTracksDrainingBinding(t *testing.T) {
	name := "connector_catalog_status_draining_test"
	require.NoError(t, Register(name, testDescriptor(validProfile(), nil, nil)))
	catalog, err := CompileCatalog(connectorconfig.ConnectorsConfig{"connector": {Type: name}}, slog.Default())
	require.NoError(t, err)
	old := catalog.Current()
	binding, err := old.Acquire("connector")
	require.NoError(t, err)

	require.NoError(t, catalog.Reload(connectorconfig.ConnectorsConfig{"connector": {Type: name}}))
	status := catalog.Status()
	require.Len(t, status.Draining, 1)
	assert.Equal(t, old.ID(), status.Draining[0].ID)
	assert.Equal(t, int64(1), status.Draining[0].Bindings)

	binding.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, old.WaitClosed(ctx))
	require.Eventually(t, func() bool {
		status := catalog.Status()
		return status.RetiredTotal == 1 && len(status.Draining) == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, catalog.Close(ctx))
}

func TestWriterContractCompliantWriterIsNotWrapped(t *testing.T) {
	underlying := &compliantTestWriter{}
	assert.Same(t, underlying, EnforceWriterContract(underlying))
}

func TestWriterContractFlushIsSnapshotBarrier(t *testing.T) {
	underlying := &testWriter{}
	writer := EnforceWriterContract(underlying)
	var callbacks atomic.Int32
	writer.Produce(context.Background(), nil, func(error) { callbacks.Add(1) })
	writer.Produce(context.Background(), nil, func(error) { callbacks.Add(1) })

	done := make(chan error, 1)
	go func() { done <- writer.Flush(context.Background()) }()
	select {
	case <-done:
		t.Fatal("flush returned before callbacks")
	case <-time.After(10 * time.Millisecond):
	}
	underlying.complete(1, nil)
	underlying.complete(0, nil)
	require.NoError(t, <-done)
	assert.Equal(t, int32(2), callbacks.Load())
}

func TestWriterContractCloseResolvesPendingExactlyOnce(t *testing.T) {
	underlying := &testWriter{}
	writer := EnforceWriterContract(underlying)
	var calls atomic.Int32
	var callbackErr error
	writer.Produce(context.Background(), nil, func(err error) {
		calls.Add(1)
		callbackErr = err
	})
	require.NoError(t, writer.Close())
	assert.ErrorIs(t, callbackErr, ErrWriterClosed)
	underlying.complete(0, errors.New("late"))
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, int32(1), underlying.closed.Load())
}
