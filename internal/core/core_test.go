package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	bmw "github.com/fujin-io/fujin/public/plugins/middleware/bind"
	bmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/bind/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var registerContracts sync.Once

func registerContractPlugins(t *testing.T) {
	t.Helper()
	registerContracts.Do(func() {
		require.NoError(t, connector.Register("session_core_contract", connector.Descriptor{Compile: func(any) (connector.Compiled, error) {
			return connector.CompileStatic(map[string]connector.RouteProfile{"route": testRouteProfile()}, map[string]connector.RouteFactory{"route": {}})
		}}))
		require.NoError(t, bmw.Register("session_core_first", func(any, *slog.Logger) (bmw.Middleware, error) {
			return bindMiddlewareFunc(func(_ context.Context, meta map[string]string) error {
				meta["order"] += "1"
				return nil
			}), nil
		}))
		require.NoError(t, bmw.Register("session_core_second", func(any, *slog.Logger) (bmw.Middleware, error) {
			return bindMiddlewareFunc(func(_ context.Context, meta map[string]string) error {
				meta["order"] += "2"
				return nil
			}), nil
		}))
		require.NoError(t, bmw.Register("session_core_reject", func(any, *slog.Logger) (bmw.Middleware, error) {
			return bindMiddlewareFunc(func(context.Context, map[string]string) error {
				return errors.New("bind rejected")
			}), nil
		}))
	})
}

type bindMiddlewareFunc func(context.Context, map[string]string) error

func (f bindMiddlewareFunc) ProcessBind(ctx context.Context, meta map[string]string) error {
	return f(ctx, meta)
}

type contractConnector struct{}

func (contractConnector) NewReader(any, string, bool, *slog.Logger) (connector.ReadCloser, error) {
	return nil, errors.New("not used")
}
func (contractConnector) NewWriter(any, string, *slog.Logger) (connector.WriteCloser, error) {
	return nil, errors.New("not used")
}
func (contractConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return func(_ string, value string) (any, error) { return value, nil }
}

type testManager struct {
	mu            sync.Mutex
	writers       map[string]*testWriter
	readers       []*testReader
	readerFactory func(string, bool) *testReader
	writerErrs    map[string]error
	readerErrs    map[string]error
	writerGets    map[string]int
	readerGets    map[string]int
	writerPuts    map[string]int
	closeCount    int
	putErr        error
	closeErr      error
	closeGate     <-chan struct{}
}

func newTestManager() *testManager {
	return &testManager{
		writers:    make(map[string]*testWriter),
		writerGets: make(map[string]int),
		readerGets: make(map[string]int),
		writerPuts: make(map[string]int),
		writerErrs: make(map[string]error),
		readerErrs: make(map[string]error),
	}
}
func testRouteProfile() connector.RouteProfile {
	return connector.RouteProfile{
		Produce: true, Headers: true, Transactions: true, Subscribe: true, Fetch: true,
		ManualSettlement: true, ProduceGuarantee: connector.AcceptanceLocal,
		Settlement: connector.SettlementProfile{Ack: connector.AckSingle, Nack: connector.NackDrop},
	}
}

func (m *testManager) RouteProfile(string) (connector.RouteProfile, error) {
	return testRouteProfile(), nil
}
func (m *testManager) RouteProfiles() map[string]connector.RouteProfile {
	return map[string]connector.RouteProfile{"route": testRouteProfile()}
}

func (m *testManager) GetReader(name string, autoCommit bool) (connector.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readerGets[readerKey(name, autoCommit)]++
	if err := m.readerErrs[readerKey(name, autoCommit)]; err != nil {
		return nil, err
	}
	var r *testReader
	if m.readerFactory != nil {
		r = m.readerFactory(name, autoCommit)
	} else {
		r = &testReader{autoCommit: autoCommit}
	}
	m.readers = append(m.readers, r)
	return r, nil
}

func (m *testManager) GetWriter(name string) (connector.WriteCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writerGets[name]++
	if err := m.writerErrs[name]; err != nil {
		return nil, err
	}
	w := m.writers[name]
	if w == nil {
		w = &testWriter{}
		m.writers[name] = w
	}
	return w, nil
}

func (m *testManager) PutWriter(_ connector.WriteCloser, name string) error {
	m.mu.Lock()
	m.writerPuts[name]++
	err := m.putErr
	m.mu.Unlock()
	return err
}

func (m *testManager) DiscardWriter(writer connector.WriteCloser) error {
	return writer.Close()
}

func (m *testManager) Close(context.Context) error {
	m.mu.Lock()
	m.closeCount++
	err := m.closeErr
	m.mu.Unlock()
	if m.closeGate != nil {
		<-m.closeGate
	}
	return err
}

func (m *testManager) closes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCount
}

func readerKey(name string, autoCommit bool) string {
	if autoCommit {
		return name + "/auto"
	}
	return name + "/manual"
}

type producedMessage struct {
	payload []byte
	headers [][]byte
}

type testWriter struct {
	mu            sync.Mutex
	produced      []producedMessage
	flushCount    int
	beginCount    int
	commitCount   int
	rollbackCount int
	closeCount    int
	beginErr      error
	produceErr    error
	flushErr      error
	rollbackErr   error
	commitErr     error
	closeErr      error
	closeGate     <-chan struct{}
	produceGate   <-chan struct{}
	pending       sync.WaitGroup
}

func (w *testWriter) Produce(_ context.Context, payload []byte, callback func(error)) {
	w.mu.Lock()
	w.produced = append(w.produced, producedMessage{payload: payload})
	gate := w.produceGate
	w.pending.Add(1)
	w.mu.Unlock()
	if gate == nil {
		callback(w.produceErr)
		w.pending.Done()
		return
	}
	go func() {
		defer w.pending.Done()
		<-gate
		callback(w.produceErr)
	}()
}

func (w *testWriter) HProduce(_ context.Context, payload []byte, headers [][]byte, callback func(error)) {
	w.mu.Lock()
	w.produced = append(w.produced, producedMessage{payload: payload, headers: headers})
	gate := w.produceGate
	w.pending.Add(1)
	w.mu.Unlock()
	if gate == nil {
		callback(w.produceErr)
		w.pending.Done()
		return
	}
	go func() {
		defer w.pending.Done()
		<-gate
		callback(w.produceErr)
	}()
}
func (w *testWriter) Flush(context.Context) error {
	w.pending.Wait()
	w.mu.Lock()
	w.flushCount++
	err := w.flushErr
	w.mu.Unlock()
	return err
}
func (w *testWriter) BeginTx(context.Context) error {
	w.mu.Lock()
	w.beginCount++
	err := w.beginErr
	w.mu.Unlock()
	return err
}
func (w *testWriter) CommitTx(context.Context) error {
	w.mu.Lock()
	w.commitCount++
	err := w.commitErr
	w.mu.Unlock()
	return err
}
func (w *testWriter) RollbackTx(context.Context) error {
	w.mu.Lock()
	w.rollbackCount++
	err := w.rollbackErr
	w.mu.Unlock()
	return err
}
func (w *testWriter) Close() error {
	if w.closeGate != nil {
		<-w.closeGate
	}
	w.mu.Lock()
	w.closeCount++
	err := w.closeErr
	w.mu.Unlock()
	return err
}

type fetchedMessage struct {
	topic   string
	payload []byte
	headers [][]byte
	id      []byte
}

type testReader struct {
	autoCommit     bool
	fetch          []fetchedMessage
	fetchErr       error
	fetchDoneFirst bool
	fetchStarted   chan struct{}
	fetchGate      <-chan struct{}
	fetchStartOnce sync.Once
	msgIDArgsLen   int
	ackErr         error
	nackErr        error
	doneFirst      bool
	ackGate        <-chan struct{}
	ackEachErr     map[string]error
	nackEachErr    map[string]error
	subscribe      func(context.Context, bool, func([]byte, string, [][]byte, ...any), func() error) error
	closeCount     atomic.Int32
	closeErr       error
	closeGate      <-chan struct{}
}

func (r *testReader) Subscribe(ctx context.Context, ready func() error, h func([]byte, string, ...any)) error {
	if r.subscribe == nil {
		if err := ready(); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return r.subscribe(ctx, false, func(payload []byte, source string, _ [][]byte, args ...any) {
		h(payload, source, args...)
	}, ready)
}

func (r *testReader) SubscribeWithHeaders(ctx context.Context, ready func() error, h func([]byte, string, [][]byte, ...any)) error {
	if r.subscribe == nil {
		if err := ready(); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return r.subscribe(ctx, true, h, ready)
}

func (r *testReader) waitForFetch() {
	if r.fetchStarted != nil {
		r.fetchStartOnce.Do(func() { close(r.fetchStarted) })
	}
	if r.fetchGate != nil {
		<-r.fetchGate
	}
}

func (r *testReader) Fetch(_ context.Context, _ uint32, done func(uint32, error), message func([]byte, string, ...any)) {
	r.waitForFetch()
	if r.fetchDoneFirst {
		done(uint32(len(r.fetch)), r.fetchErr)
	}
	for _, m := range r.fetch {
		message(m.payload, m.topic, m.id)
	}
	if !r.fetchDoneFirst {
		done(uint32(len(r.fetch)), r.fetchErr)
	}
}

func (r *testReader) FetchWithHeaders(_ context.Context, _ uint32, done func(uint32, error), message func([]byte, string, [][]byte, ...any)) {
	r.waitForFetch()
	if r.fetchDoneFirst {
		done(uint32(len(r.fetch)), r.fetchErr)
	}
	for _, m := range r.fetch {
		message(m.payload, m.topic, m.headers, m.id)
	}
	if !r.fetchDoneFirst {
		done(uint32(len(r.fetch)), r.fetchErr)
	}
}

func (r *testReader) Ack(_ context.Context, ids [][]byte, done func(error), each func([]byte, error)) {
	r.runAck(ids, done, each, r.ackErr, r.ackEachErr)
}

func (r *testReader) Nack(_ context.Context, ids [][]byte, done func(error), each func([]byte, error)) {
	r.runAck(ids, done, each, r.nackErr, r.nackEachErr)
}

func (r *testReader) runAck(ids [][]byte, done func(error), each func([]byte, error), topErr error, eachErr map[string]error) {
	run := func() {
		if r.doneFirst {
			done(topErr)
		}
		for _, id := range ids {
			each(id, eachErr[string(id)])
		}
		if !r.doneFirst {
			done(topErr)
		}
	}
	if r.ackGate == nil {
		run()
		return
	}
	go func() {
		<-r.ackGate
		run()
	}()
}

func (r *testReader) MsgIDArgsLen() int { return r.msgIDArgsLen }
func (r *testReader) EncodeMsgID(buf []byte, _ string, args ...any) []byte {
	if len(args) == 0 {
		return buf
	}
	id, _ := args[0].([]byte)
	return append(buf, id...)
}

func encodedTestMessageID(t *testing.T, core *Core, subscriptionID byte, payload []byte) []byte {
	t.Helper()
	core.mu.Lock()
	reader := core.readers[subscriptionID]
	core.mu.Unlock()
	require.NotNil(t, reader)
	return reader.scoped.EncodeMsgID(nil, "", payload)
}
func (r *testReader) AutoCommit() bool { return r.autoCommit }
func (r *testReader) Close() error {
	if r.closeGate != nil {
		<-r.closeGate
	}
	r.closeCount.Add(1)
	return r.closeErr
}

func newBoundCore(t *testing.T, m *testManager) *Core {
	t.Helper()
	configs := connectorconfig.ConnectorsConfig{
		"connector": {Type: "session_core_contract"},
	}
	core := NewWithManagerFactory(context.Background(), configs, nil, slog.Default(), func(connectorconfig.ConnectorConfig, string, *slog.Logger) Manager {
		return m
	})
	_, err := core.Bind("connector", nil, nil)
	require.NoError(t, err)
	return core
}

func TestBindUsesLatestConfigMiddlewareOrderAndOverrides(t *testing.T) {
	registerContractPlugins(t)
	var current atomic.Pointer[connectorconfig.ConnectorsConfig]
	first := connectorconfig.ConnectorsConfig{
		"old": {Type: "session_core_contract"},
	}
	current.Store(&first)
	second := connectorconfig.ConnectorsConfig{
		"new": {
			Type:        "session_core_contract",
			Overridable: []string{"routes.*.topic"},
			BindMiddlewares: []bmwconfig.Config{
				{Name: "session_core_first", Enabled: boolPointer(true)},
				{Name: "session_core_second", Enabled: boolPointer(true)},
			},
			Settings: map[string]any{"routes": map[string]any{"pub": map[string]any{"topic": "before"}}},
		},
	}
	current.Store(&second)

	var received connectorconfig.ConnectorConfig
	core := NewWithManagerFactory(context.Background(), nil, func() connectorconfig.ConnectorsConfig {
		return *current.Load()
	}, slog.Default(), func(conf connectorconfig.ConnectorConfig, _ string, _ *slog.Logger) Manager {
		received = conf
		return newTestManager()
	})
	meta := map[string]string{}
	_, err := core.Bind("new", meta, map[string]string{"routes.pub.topic": "after"})
	require.NoError(t, err)
	assert.Equal(t, "12", meta["order"])
	settings := received.Settings.(map[string]any)
	assert.Equal(t, "after", settings["routes"].(map[string]any)["pub"].(map[string]any)["topic"])
	_, err = core.Bind("new", nil, nil)
	assert.ErrorIs(t, err, ErrAlreadyBound)
}

func boolPointer(value bool) *bool { return &value }

func TestBindRejectsMissingConnectorAndDisallowedOverride(t *testing.T) {
	registerContractPlugins(t)
	configs := connectorconfig.ConnectorsConfig{
		"connector": {Type: "session_core_contract"},
	}
	core := NewWithManagerFactory(context.Background(), configs, nil, slog.Default(), func(connectorconfig.ConnectorConfig, string, *slog.Logger) Manager {
		return newTestManager()
	})
	_, err := core.Bind("missing", nil, nil)
	assert.ErrorIs(t, err, ErrConnectorNotFound)
	_, err = core.Bind("connector", nil, map[string]string{"forbidden": "value"})
	assert.Error(t, err)
	assert.Equal(t, StateUnbound, core.State())
}

func TestBindMiddlewareRejectionLeavesSessionUnbound(t *testing.T) {
	registerContractPlugins(t)
	configs := connectorconfig.ConnectorsConfig{
		"connector": {
			Type:            "session_core_contract",
			BindMiddlewares: []bmwconfig.Config{{Name: "session_core_reject", Enabled: boolPointer(true)}},
		},
	}
	core := NewWithManagerFactory(context.Background(), configs, nil, slog.Default(), func(connectorconfig.ConnectorConfig, string, *slog.Logger) Manager {
		return newTestManager()
	})
	_, err := core.Bind("connector", map[string]string{"api-key": "denied"}, nil)
	require.ErrorContains(t, err, "bind rejected")
	assert.Equal(t, StateUnbound, core.State())
}

func TestProduceReusesWritersAndPreservesHeaders(t *testing.T) {
	m := newTestManager()
	core := newBoundCore(t, m)

	var callbacks atomic.Int32
	require.NoError(t, core.Produce("topic", []byte("one"), nil, func(error) { callbacks.Add(1) }))
	require.NoError(t, core.Produce("topic", []byte("two"), [][]byte{[]byte("k"), []byte("v")}, func(error) { callbacks.Add(1) }))

	assert.Equal(t, int32(2), callbacks.Load())
	assert.Equal(t, 1, m.writerGets["topic"])
	w := m.writers["topic"]
	require.Len(t, w.produced, 2)
	assert.Nil(t, w.produced[0].headers)
	assert.Equal(t, [][]byte{[]byte("k"), []byte("v")}, w.produced[1].headers)
	require.NoError(t, core.Produce("other-topic", []byte("three"), nil, func(error) { callbacks.Add(1) }))
	assert.Equal(t, int32(3), callbacks.Load())
	assert.Equal(t, 1, m.writerGets["other-topic"])
}

func TestProducePropagatesWriterAndConnectorErrors(t *testing.T) {
	m := newTestManager()
	m.writerErrs["missing"] = errors.New("writer unavailable")
	m.writers["failing"] = &testWriter{produceErr: errors.New("produce failed")}
	core := newBoundCore(t, m)

	assert.EqualError(t, core.Produce("missing", []byte("message"), nil, nil), "writer unavailable")
	var callbackErr error
	require.NoError(t, core.Produce("failing", []byte("message"), nil, func(err error) { callbackErr = err }))
	require.EqualError(t, callbackErr, "produce failed")
}

func TestTransactionLifecycleAndInvalidTransitions(t *testing.T) {
	m := newTestManager()
	core := newBoundCore(t, m)

	assert.ErrorIs(t, core.Commit(), ErrNoTransaction)
	assert.ErrorIs(t, core.TxProduce([]byte("message"), nil, nil), ErrNoTransaction)
	require.NoError(t, core.Begin("tx-route"))
	assert.ErrorIs(t, core.Begin("tx-route"), ErrTransactionActive)
	assert.ErrorIs(t, core.Produce("tx-route", []byte("message"), nil, nil), ErrNormalProduceInTransaction)
	require.NoError(t, core.TxProduce([]byte("message"), nil, nil))
	w := m.writers["tx-route"]
	assert.Equal(t, 1, w.beginCount)
	require.NoError(t, core.Commit())
	assert.Equal(t, 1, w.flushCount)
	assert.Equal(t, 1, w.commitCount)
	assert.Equal(t, 1, m.writerPuts["tx-route"])

	require.NoError(t, core.Begin("tx-route"))
	require.NoError(t, core.TxProduce([]byte("message"), [][]byte{[]byte("key"), []byte("value")}, nil))
	require.NoError(t, core.Rollback())
	assert.Equal(t, 2, w.beginCount)
	assert.Equal(t, 1, w.rollbackCount)
	assert.ErrorIs(t, core.Rollback(), ErrNoTransaction)
}

func TestBeginEagerlyInitializesTransactionAndFailsClosed(t *testing.T) {
	m := newTestManager()
	beginErr := errors.New("begin failed")
	m.writers["tx"] = &testWriter{beginErr: beginErr}
	core := newBoundCore(t, m)

	assert.ErrorIs(t, core.Begin("tx"), beginErr)
	assert.Equal(t, StateConnected, core.State())
	assert.Equal(t, 1, m.writerGets["tx"])
	assert.Equal(t, 1, m.writers["tx"].closeCount)
	assert.ErrorIs(t, core.TxProduce([]byte("message"), nil, nil), ErrNoTransaction)

	m2 := newTestManager()
	m2.writers["tx"] = &testWriter{rollbackErr: errors.New("rollback failed")}
	core2 := newBoundCore(t, m2)
	require.NoError(t, core2.Begin("tx"))
	require.ErrorContains(t, core2.Close(), "rollback failed")
	assert.Equal(t, StateClosed, core2.State())
	assert.Equal(t, 1, m2.writers["tx"].closeCount)
}

func TestTransactionTerminalErrorsPoisonWriter(t *testing.T) {
	t.Run("flush failure rolls back without commit", func(t *testing.T) {
		m := newTestManager()
		w := &testWriter{flushErr: errors.New("flush failed")}
		m.writers["tx"] = w
		core := newBoundCore(t, m)
		require.NoError(t, core.Begin("tx"))
		err := core.Commit()
		assert.ErrorContains(t, err, "transaction aborted after flush")
		assert.ErrorIs(t, err, ErrTransactionAborted)
		assert.Equal(t, StateConnected, core.State())
		assert.Equal(t, 1, w.rollbackCount)
		assert.Zero(t, w.commitCount)
		assert.Equal(t, 1, w.closeCount)
		assert.Zero(t, m.writerPuts["tx"])
	})

	t.Run("commit failure reports unknown outcome", func(t *testing.T) {
		m := newTestManager()
		w := &testWriter{commitErr: errors.New("commit failed")}
		m.writers["tx"] = w
		core := newBoundCore(t, m)
		require.NoError(t, core.Begin("tx"))
		err := core.Commit()
		assert.ErrorIs(t, err, ErrCommitOutcomeUnknown)
		assert.Equal(t, StateConnected, core.State())
		assert.Zero(t, w.rollbackCount)
		assert.Equal(t, 1, w.closeCount)
		assert.Zero(t, m.writerPuts["tx"])
	})

	t.Run("rollback failure terminates transaction", func(t *testing.T) {
		m := newTestManager()
		w := &testWriter{rollbackErr: errors.New("rollback failed")}
		m.writers["tx"] = w
		core := newBoundCore(t, m)
		require.NoError(t, core.Begin("tx"))
		err := core.Rollback()
		assert.ErrorContains(t, err, "rollback failed")
		assert.Equal(t, StateConnected, core.State())
		assert.Equal(t, 1, w.closeCount)
		assert.Zero(t, m.writerPuts["tx"])
	})
}

func TestFetchCacheKeyAndStableSubscription(t *testing.T) {
	m := newTestManager()
	m.readerFactory = func(_ string, autoCommit bool) *testReader {
		return &testReader{autoCommit: autoCommit, fetch: []fetchedMessage{{payload: []byte("payload"), topic: "topic", id: []byte("id")}}}
	}
	core := newBoundCore(t, m)

	var ids []byte
	var delivered int
	fetch := func(autoCommit, withHeaders bool) {
		deliver := func(payload []byte) {
			assert.Equal(t, []byte("payload"), payload)
			delivered++
		}
		handlers := FetchMessageHandlers{
			Manual: func(_ byte, _ connector.Reader, payload []byte, _ string, _ [][]byte, _ ...any) {
				deliver(payload)
			},
		}
		if autoCommit && withHeaders {
			handlers.AutoCommitWithHeaders = func(payload []byte, _ string, _ [][]byte, _ ...any) { deliver(payload) }
		} else if autoCommit {
			handlers.AutoCommit = func(payload []byte, _ string, _ ...any) { deliver(payload) }
		}
		id, count, err := core.Fetch("topic", autoCommit, withHeaders, 32, handlers)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), count)
		ids = append(ids, id)
	}
	fetch(false, false)
	fetch(false, false)
	fetch(true, false)
	fetch(false, true)

	require.Len(t, ids, 4)
	assert.Equal(t, ids[0], ids[1])
	assert.NotEqual(t, ids[0], ids[2])
	assert.NotEqual(t, ids[0], ids[3])
	assert.Equal(t, 3, len(m.readers))
	assert.Equal(t, 4, delivered)
}

func TestFetchUsesDirectAutoCommitHandlersAndManualContext(t *testing.T) {
	m := newTestManager()
	m.readerFactory = func(_ string, autoCommit bool) *testReader {
		return &testReader{autoCommit: autoCommit, fetch: []fetchedMessage{{payload: []byte("payload"), topic: "topic"}}}
	}
	core := newBoundCore(t, m)

	var direct, manual int
	var manualID byte
	var manualReader connector.Reader
	handlers := FetchMessageHandlers{
		AutoCommit: func(_ []byte, _ string, _ ...any) {
			direct++
		},
		AutoCommitWithHeaders: func(_ []byte, _ string, _ [][]byte, _ ...any) {
			direct++
		},
		Manual: func(id byte, reader connector.Reader, _ []byte, _ string, _ [][]byte, _ ...any) {
			manual++
			manualID, manualReader = id, reader
		},
	}

	for _, withHeaders := range []bool{false, true} {
		_, count, err := core.Fetch("auto", true, withHeaders, 1, handlers)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), count)
	}
	assert.Equal(t, 2, direct)
	assert.Zero(t, manual)

	id, count, err := core.Fetch("manual", false, false, 1, handlers)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), count)
	assert.Equal(t, 1, manual)
	assert.Equal(t, id, manualID)
	assert.NotNil(t, manualReader)
}
func TestFetchReturnsConnectorResultAfterMessages(t *testing.T) {
	m := newTestManager()
	m.readerFactory = func(_ string, autoCommit bool) *testReader {
		return &testReader{
			autoCommit:     autoCommit,
			fetchDoneFirst: true,
			fetch: []fetchedMessage{
				{payload: []byte("one"), topic: "topic", headers: [][]byte{[]byte("k"), []byte("v")}, id: []byte("1")},
				{payload: []byte("two"), topic: "topic", headers: [][]byte{[]byte("k"), []byte("v")}, id: []byte("2")},
			},
		}
	}
	core := newBoundCore(t, m)

	var delivered int
	_, count, err := core.Fetch("topic", false, true, 2, FetchMessageHandlers{Manual: func(_ byte, _ connector.Reader, _ []byte, _ string, headers [][]byte, _ ...any) {
		assert.Equal(t, [][]byte{[]byte("k"), []byte("v")}, headers)
		delivered++
	}})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), count)
	assert.Equal(t, 2, delivered)
}
func TestFetchRejectsZeroBoundsOverflowAndContention(t *testing.T) {
	t.Run("zero batch", func(t *testing.T) {
		core := newBoundCore(t, newTestManager())
		_, _, err := core.Fetch("topic", false, false, 0, FetchMessageHandlers{})
		assert.ErrorIs(t, err, ErrInvalidBatchSize)
	})

	t.Run("strict maximum", func(t *testing.T) {
		m := newTestManager()
		m.readerFactory = func(_ string, autoCommit bool) *testReader {
			return &testReader{autoCommit: autoCommit, fetch: []fetchedMessage{
				{payload: []byte("one")}, {payload: []byte("two")}, {payload: []byte("three")},
			}}
		}
		core := newBoundCore(t, m)
		delivered := 0
		_, count, err := core.Fetch("topic", false, false, 2, FetchMessageHandlers{Manual: func(byte, connector.Reader, []byte, string, [][]byte, ...any) {
			delivered++
		}})
		assert.ErrorContains(t, err, "fetch contract violated")
		assert.Equal(t, uint32(2), count)
		assert.Equal(t, 2, delivered)
	})

	t.Run("same reader busy", func(t *testing.T) {
		m := newTestManager()
		started := make(chan struct{})
		gate := make(chan struct{})
		m.readerFactory = func(_ string, autoCommit bool) *testReader {
			return &testReader{autoCommit: autoCommit, fetchStarted: started, fetchGate: gate}
		}
		core := newBoundCore(t, m)
		first := make(chan error, 1)
		go func() {
			_, _, err := core.Fetch("topic", false, false, 1, FetchMessageHandlers{})
			first <- err
		}()
		<-started
		_, _, err := core.Fetch("topic", false, false, 1, FetchMessageHandlers{})
		assert.ErrorIs(t, err, ErrFetchBusy)
		close(gate)
		require.NoError(t, <-first)
	})
}

func TestSubscribeUnsubscribeAndCleanupCloseExactlyOnce(t *testing.T) {
	m := newTestManager()
	started := make(chan struct{})
	m.readerFactory = func(_ string, autoCommit bool) *testReader {
		return &testReader{autoCommit: autoCommit, subscribe: func(ctx context.Context, _ bool, _ func([]byte, string, [][]byte, ...any), ready func() error) error {
			select {
			case <-started:
			default:
				close(started)
			}
			if err := ready(); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}}
	}
	core := newBoundCore(t, m)
	var id byte
	require.NoError(t, core.Subscribe("topic", false, false, func(subscriptionID byte) error {
		id = subscriptionID
		return nil
	}, SubscriptionMessageHandlers{}, nil))
	<-started
	require.NoError(t, core.Unsubscribe(id))
	assert.ErrorIs(t, core.Unsubscribe(id), ErrSubscriptionNotFound)
	require.Eventually(t, func() bool { return m.readers[0].closeCount.Load() == 1 }, time.Second, time.Millisecond)
	require.NoError(t, core.Close())
	require.NoError(t, core.Close())
	assert.Equal(t, int32(1), m.readers[0].closeCount.Load())
	assert.Equal(t, 1, m.closeCount)
}
func TestSubscribeWaitsForReadinessAndReportsTerminalFailure(t *testing.T) {
	t.Run("pre-readiness failure", func(t *testing.T) {
		m := newTestManager()
		failure := errors.New("subscribe setup failed")
		m.readerFactory = func(_ string, autoCommit bool) *testReader {
			return &testReader{autoCommit: autoCommit, subscribe: func(context.Context, bool, func([]byte, string, [][]byte, ...any), func() error) error {
				return failure
			}}
		}
		core := newBoundCore(t, m)
		readyCalled := false
		err := core.Subscribe("topic", false, false, func(byte) error {
			readyCalled = true
			return nil
		}, SubscriptionMessageHandlers{}, nil)
		assert.ErrorIs(t, err, failure)
		assert.False(t, readyCalled)
		assert.Equal(t, int32(1), m.readers[0].closeCount.Load())
	})

	t.Run("delivery follows readiness and terminal error is observable", func(t *testing.T) {
		m := newTestManager()
		terminal := errors.New("receive loop failed")
		m.readerFactory = func(_ string, autoCommit bool) *testReader {
			return &testReader{autoCommit: autoCommit, subscribe: func(_ context.Context, _ bool, message func([]byte, string, [][]byte, ...any), ready func() error) error {
				if err := ready(); err != nil {
					return err
				}
				message([]byte("payload"), "source", nil)
				return terminal
			}}
		}
		core := newBoundCore(t, m)
		var mu sync.Mutex
		var order []string
		terminalResult := make(chan error, 1)
		err := core.Subscribe("topic", false, false, func(byte) error {
			mu.Lock()
			order = append(order, "ready")
			mu.Unlock()
			return nil
		}, SubscriptionMessageHandlers{Message: func(byte, connector.Reader) func([]byte, string, ...any) {
			return func([]byte, string, ...any) {
				mu.Lock()
				order = append(order, "message")
				mu.Unlock()
			}
		}}, func(err error) { terminalResult <- err })
		require.NoError(t, err)
		assert.ErrorIs(t, <-terminalResult, terminal)
		mu.Lock()
		assert.Equal(t, []string{"ready", "message"}, order)
		mu.Unlock()
		require.Eventually(t, func() bool { return m.readers[0].closeCount.Load() == 1 }, time.Second, time.Millisecond)
	})
}

func TestFetchAndSubscribePropagateReaderFailures(t *testing.T) {
	m := newTestManager()
	m.readerErrs[readerKey("fetch", false)] = errors.New("reader unavailable")
	m.readerErrs[readerKey("subscribe", true)] = errors.New("subscriber unavailable")
	core := newBoundCore(t, m)
	_, _, err := core.Fetch("fetch", false, false, 1, FetchMessageHandlers{})
	assert.EqualError(t, err, "reader unavailable")
	assert.EqualError(t, core.Subscribe("subscribe", true, false, nil, SubscriptionMessageHandlers{}, nil), "subscriber unavailable")
}

func TestAckNackStreamResultsAndZeroBatch(t *testing.T) {
	m := newTestManager()
	r := &testReader{
		ackEachErr:  map[string]error{},
		nackEachErr: map[string]error{},
	}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)

	id, _, err := core.Fetch("topic", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)
	for _, doneFirst := range []bool{false, true} {
		r.doneFirst = doneFirst
		for _, nack := range []bool{false, true} {
			goodPayload := []byte(fmt.Sprintf("good-%t-%t", doneFirst, nack))
			badPayload := []byte(fmt.Sprintf("bad-%t-%t", doneFirst, nack))
			if nack {
				r.nackEachErr[string(badPayload)] = errors.New("nack failed")
			} else {
				r.ackEachErr[string(badPayload)] = errors.New("ack failed")
			}
			ids := [][]byte{
				encodedTestMessageID(t, core, id, goodPayload),
				encodedTestMessageID(t, core, id, badPayload),
			}
			var order []string
			var gotIDs [][]byte
			var gotErrs []error
			handlers := AckResultHandlers{
				Result: func(err error) {
					require.NoError(t, err)
					order = append(order, "result")
				},
				Message: func(messageID []byte, err error) {
					order = append(order, "message")
					gotIDs = append(gotIDs, messageID)
					gotErrs = append(gotErrs, err)
				},
			}
			if nack {
				require.NoError(t, core.Nack(id, ids, handlers))
			} else {
				require.NoError(t, core.Ack(id, ids, handlers))
			}
			assert.Equal(t, []string{"result", "message", "message"}, order)
			assert.Equal(t, ids, gotIDs)
			require.Len(t, gotErrs, 2)
			assert.NoError(t, gotErrs[0])
			assert.Error(t, gotErrs[1])
		}
	}

	called := false
	require.NoError(t, core.Ack(254, nil, AckResultHandlers{Result: func(err error) {
		called = true
		assert.NoError(t, err)
	}}))
	assert.True(t, called)
	assert.ErrorIs(t, core.Ack(254, [][]byte{{1}}, AckResultHandlers{}), ErrSubscriptionNotFound)
}

func TestAckNackBatchSizesAutoSettleAndTopLevelErrors(t *testing.T) {
	m := newTestManager()
	r := &testReader{}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)
	id, _, err := core.Fetch("topic", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)
	for _, size := range []int{1, 32, 256} {
		ids := make([][]byte, size)
		for i := range ids {
			ids[i] = encodedTestMessageID(t, core, id, []byte(fmt.Sprintf("%d/%d", size, i)))
		}
		got := 0
		require.NoError(t, core.Ack(id, ids, AckResultHandlers{
			Result:  func(err error) { require.NoError(t, err) },
			Message: func([]byte, error) { got++ },
		}))
		assert.Equal(t, size, got)
	}

	topErr := errors.New("unsupported")
	r.ackErr = topErr
	messageCalls := 0
	retryID := encodedTestMessageID(t, core, id, []byte("retry"))
	require.NoError(t, core.Ack(id, [][]byte{retryID}, AckResultHandlers{
		Result:  func(err error) { assert.ErrorIs(t, err, topErr) },
		Message: func([]byte, error) { messageCalls++ },
	}))
	assert.Zero(t, messageCalls)
	r.ackErr = nil
	require.NoError(t, core.Ack(id, [][]byte{retryID}, AckResultHandlers{}))

	autoID, _, err := core.Fetch("auto", true, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)
	assert.ErrorIs(t, core.Ack(autoID, [][]byte{{1}}, AckResultHandlers{}), connector.ErrOperationUnsupported)
	assert.ErrorIs(t, core.Nack(autoID, [][]byte{{1}}, AckResultHandlers{}), connector.ErrOperationUnsupported)
}

func TestMessageIDValidationScopeAndConsumption(t *testing.T) {
	m := newTestManager()
	r := &testReader{}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)
	firstID, _, err := core.Fetch("first", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)
	secondID, _, err := core.Fetch("second", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)

	valid := encodedTestMessageID(t, core, firstID, []byte("valid"))
	assert.ErrorIs(t, core.Ack(firstID, [][]byte{{messageIDVersion}}, AckResultHandlers{}), ErrInvalidMessageID)
	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[0]++
	assert.ErrorIs(t, core.Ack(firstID, [][]byte{wrongVersion}, AckResultHandlers{}), ErrInvalidMessageID)
	assert.ErrorIs(t, core.Ack(secondID, [][]byte{valid}, AckResultHandlers{}), ErrInvalidMessageID)
	assert.ErrorIs(t, core.Ack(firstID, [][]byte{valid, valid}, AckResultHandlers{}), ErrInvalidMessageID)

	require.NoError(t, core.Ack(firstID, [][]byte{valid}, AckResultHandlers{}))
	assert.ErrorIs(t, core.Ack(firstID, [][]byte{valid}, AckResultHandlers{}), ErrInvalidMessageID)
}

func TestMessageIDSettlementTracksGapsAndInProgressRequests(t *testing.T) {
	m := newTestManager()
	r := &testReader{}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)
	id, _, err := core.Fetch("topic", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)

	first := encodedTestMessageID(t, core, id, []byte("first"))
	second := encodedTestMessageID(t, core, id, []byte("second"))
	third := encodedTestMessageID(t, core, id, []byte("third"))
	require.NoError(t, core.Ack(id, [][]byte{second}, AckResultHandlers{}))
	assert.ErrorIs(t, core.Ack(id, [][]byte{second}, AckResultHandlers{}), ErrInvalidMessageID)
	require.NoError(t, core.Ack(id, [][]byte{first}, AckResultHandlers{}))
	assert.ErrorIs(t, core.Ack(id, [][]byte{first}, AckResultHandlers{}), ErrInvalidMessageID)
	require.NoError(t, core.Ack(id, [][]byte{third}, AckResultHandlers{}))

	gate := make(chan struct{})
	r.ackGate = gate
	fourth := encodedTestMessageID(t, core, id, []byte("fourth"))
	require.NoError(t, core.Ack(id, [][]byte{fourth}, AckResultHandlers{}))
	assert.ErrorIs(t, core.Ack(id, [][]byte{fourth}, AckResultHandlers{}), ErrInvalidMessageID)
	close(gate)
}

func TestCleanupWaitsForPendingAck(t *testing.T) {
	m := newTestManager()
	gate := make(chan struct{})
	r := &testReader{ackGate: gate}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)
	id, _, err := core.Fetch("topic", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)
	messageID := encodedTestMessageID(t, core, id, []byte("id"))
	require.NoError(t, core.Ack(id, [][]byte{messageID}, AckResultHandlers{}))

	done := make(chan struct{})
	go func() {
		_ = core.Close()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("cleanup returned before pending acknowledgement")
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after acknowledgement")
	}
}

func TestCleanupFlushesRollsBackAndWaitsForPendingProduce(t *testing.T) {
	m := newTestManager()
	gate := make(chan struct{})
	w := &testWriter{produceGate: gate}
	m.writers["topic"] = w
	core := newBoundCore(t, m)

	require.NoError(t, core.Produce("topic", []byte("pending"), nil, nil))
	done := make(chan struct{})
	go func() {
		_ = core.Close()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("cleanup returned before pending produce callback")
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after pending callback")
	}
	assert.Equal(t, 1, w.flushCount)
	assert.Equal(t, 1, w.closeCount)
	assert.Zero(t, m.writerPuts["topic"])
	assert.Equal(t, 1, m.closeCount)

	m2 := newTestManager()
	core2 := newBoundCore(t, m2)
	require.NoError(t, core2.Begin("tx"))
	txWriter := m2.writers["tx"]
	require.NoError(t, core2.Close())
	assert.Equal(t, 1, txWriter.rollbackCount)
	assert.Equal(t, 1, txWriter.closeCount)
	assert.Zero(t, m2.writerPuts["tx"])
}

func TestCleanupAggregatesErrorsAndContinuesAfterDeadline(t *testing.T) {
	m := newTestManager()
	blocked := make(chan struct{})
	flushErr := errors.New("flush failed")
	closeErr := errors.New("close failed")
	readerErr := errors.New("reader close failed")
	managerErr := errors.New("manager close failed")
	m.writers["writer"] = &testWriter{flushErr: flushErr, closeErr: closeErr}
	m.readerFactory = func(string, bool) *testReader { return &testReader{closeErr: readerErr} }
	m.closeErr = managerErr
	core := newBoundCore(t, m)
	require.NoError(t, core.Produce("writer", []byte("message"), nil, nil))
	_, _, err := core.Fetch("reader", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)

	err = core.Close()
	assert.ErrorIs(t, err, flushErr)
	assert.ErrorIs(t, err, closeErr)
	assert.ErrorIs(t, err, readerErr)
	assert.ErrorIs(t, err, managerErr)
	assert.Equal(t, 1, m.closes())

	m2 := newTestManager()
	m2.readerFactory = func(string, bool) *testReader { return &testReader{closeGate: blocked} }
	core2 := newBoundCore(t, m2)
	core2.cleanupTimeout = 20 * time.Millisecond
	_, _, err = core2.Fetch("reader", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)
	started := time.Now()
	err = core2.Close()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second)
	assert.Equal(t, 1, m2.closes(), "manager cleanup must still be attempted after reader timeout")
	close(blocked)
}
