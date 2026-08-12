package core

import (
	"context"
	"errors"
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
		require.NoError(t, connector.Register("session_core_contract", func(any, *slog.Logger) (connector.Connector, error) {
			return contractConnector{}, nil
		}))
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

func (m *testManager) PutWriter(_ connector.WriteCloser, name string) {
	m.mu.Lock()
	m.writerPuts[name]++
	m.mu.Unlock()
}

func (m *testManager) Close() {
	m.mu.Lock()
	m.closeCount++
	m.mu.Unlock()
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
	w.mu.Lock()
	w.closeCount++
	w.mu.Unlock()
	return nil
}

type fetchedMessage struct {
	payload []byte
	topic   string
	headers [][]byte
	id      []byte
}

type testReader struct {
	autoCommit     bool
	fetch          []fetchedMessage
	fetchErr       error
	fetchDoneFirst bool
	ackErr         error
	nackErr        error
	doneFirst      bool
	ackGate        <-chan struct{}
	ackEachErr     map[string]error
	nackEachErr    map[string]error
	subscribe      func(context.Context, bool, func([]byte, string, [][]byte, ...any)) error
	closeCount     atomic.Int32
}

func (r *testReader) Subscribe(ctx context.Context, h func([]byte, string, ...any)) error {
	if r.subscribe == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.subscribe(ctx, false, func(payload []byte, topic string, _ [][]byte, args ...any) {
		h(payload, topic, args...)
	})
}

func (r *testReader) SubscribeWithHeaders(ctx context.Context, h func([]byte, string, [][]byte, ...any)) error {
	if r.subscribe == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.subscribe(ctx, true, h)
}

func (r *testReader) Fetch(_ context.Context, _ uint32, done func(uint32, error), message func([]byte, string, ...any)) {
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

func (r *testReader) MsgIDArgsLen() int { return 0 }
func (r *testReader) EncodeMsgID(buf []byte, _ string, args ...any) []byte {
	if len(args) == 0 {
		return buf
	}
	id, _ := args[0].([]byte)
	return append(buf, id...)
}
func (r *testReader) AutoCommit() bool { return r.autoCommit }
func (r *testReader) Close() error {
	r.closeCount.Add(1)
	return nil
}

func newBoundCore(t *testing.T, m *testManager) *Core {
	t.Helper()
	configs := connectorconfig.ConnectorsConfig{
		"connector": {Type: "session_core_contract"},
	}
	core := NewWithManagerFactory(context.Background(), configs, nil, slog.Default(), func(connectorconfig.ConnectorConfig, string, *slog.Logger) Manager {
		return m
	})
	require.NoError(t, core.Bind("connector", nil, nil))
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
	require.NoError(t, core.Bind("new", meta, map[string]string{"routes.pub.topic": "after"}))
	assert.Equal(t, "12", meta["order"])
	settings := received.Settings.(map[string]any)
	assert.Equal(t, "after", settings["routes"].(map[string]any)["pub"].(map[string]any)["topic"])
	assert.ErrorIs(t, core.Bind("new", nil, nil), ErrAlreadyBound)
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
	assert.ErrorIs(t, core.Bind("missing", nil, nil), ErrConnectorNotFound)
	assert.Error(t, core.Bind("connector", nil, map[string]string{"forbidden": "value"}))
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
	err := core.Bind("connector", map[string]string{"api-key": "denied"}, nil)
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

func TestTransactionWriterAcquisitionIsLazyAndFailureRemainsInTransaction(t *testing.T) {
	m := newTestManager()
	unsupported := errors.New("not supported")
	m.writers["tx"] = &testWriter{beginErr: unsupported}
	core := newBoundCore(t, m)

	require.NoError(t, core.Begin("tx"))
	assert.Equal(t, StateInTransaction, core.State())
	assert.Equal(t, 0, m.writerGets["tx"])
	assert.ErrorIs(t, core.TxProduce([]byte("message"), nil, nil), unsupported)
	assert.Equal(t, StateInTransaction, core.State())
	assert.Equal(t, 1, m.writerPuts["tx"])
	require.NoError(t, core.Rollback())
	assert.Equal(t, StateConnected, core.State())

	m2 := newTestManager()
	m2.writers["tx"] = &testWriter{rollbackErr: errors.New("rollback failed")}
	core2 := newBoundCore(t, m2)
	require.NoError(t, core2.Begin("tx"))
	require.NoError(t, core2.TxProduce([]byte("message"), nil, nil))
	require.ErrorContains(t, core2.Close(), "rollback failed")
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

func TestSubscribeUnsubscribeAndCleanupCloseExactlyOnce(t *testing.T) {
	m := newTestManager()
	started := make(chan struct{})
	m.readerFactory = func(_ string, autoCommit bool) *testReader {
		return &testReader{autoCommit: autoCommit, subscribe: func(ctx context.Context, _ bool, _ func([]byte, string, [][]byte, ...any)) error {
			select {
			case <-started:
			default:
				close(started)
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
		ackEachErr:  map[string]error{"bad": errors.New("ack failed")},
		nackEachErr: map[string]error{"bad": errors.New("nack failed")},
	}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)

	id, _, err := core.Fetch("topic", false, false, 1, FetchMessageHandlers{})
	require.NoError(t, err)
	ids := [][]byte{[]byte("good"), []byte("bad")}
	for _, doneFirst := range []bool{false, true} {
		r.doneFirst = doneFirst
		for _, nack := range []bool{false, true} {
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
	assert.ErrorIs(t, core.Ack(254, ids, AckResultHandlers{}), ErrSubscriptionNotFound)
}

func TestAckNackBatchSizesAutoCommitAndTopLevelErrors(t *testing.T) {
	m := newTestManager()
	r := &testReader{autoCommit: true}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)
	id, _, err := core.Fetch("topic", true, false, 0, FetchMessageHandlers{})
	require.NoError(t, err)
	for _, size := range []int{1, 32, 256} {
		ids := make([][]byte, size)
		for i := range ids {
			ids[i] = []byte{byte(i)}
		}
		for _, nack := range []bool{false, true} {
			got := 0
			handlers := AckResultHandlers{
				Result: func(err error) { require.NoError(t, err) },
				Message: func([]byte, error) {
					got++
				},
			}
			if nack {
				require.NoError(t, core.Nack(id, ids, handlers))
			} else {
				require.NoError(t, core.Ack(id, ids, handlers))
			}
			assert.Equal(t, size, got)
		}
	}

	topErr := errors.New("unsupported")
	r.ackErr = topErr
	messageCalls := 0
	require.NoError(t, core.Ack(id, [][]byte{[]byte("id")}, AckResultHandlers{
		Result: func(err error) { assert.ErrorIs(t, err, topErr) },
		Message: func([]byte, error) {
			messageCalls++
		},
	}))
	assert.Zero(t, messageCalls)
}

func TestCleanupWaitsForPendingAck(t *testing.T) {
	m := newTestManager()
	gate := make(chan struct{})
	r := &testReader{ackGate: gate}
	m.readerFactory = func(string, bool) *testReader { return r }
	core := newBoundCore(t, m)
	id, _, err := core.Fetch("topic", false, false, 0, FetchMessageHandlers{})
	require.NoError(t, err)
	require.NoError(t, core.Ack(id, [][]byte{[]byte("id")}, AckResultHandlers{}))

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
	assert.Equal(t, 1, m.writerPuts["topic"])
	assert.Equal(t, 1, m.closeCount)

	m2 := newTestManager()
	core2 := newBoundCore(t, m2)
	require.NoError(t, core2.Begin("tx"))
	require.NoError(t, core2.TxProduce([]byte("pending"), nil, nil))
	txWriter := m2.writers["tx"]
	require.NoError(t, core2.Close())
	assert.Equal(t, 1, txWriter.rollbackCount)
	assert.Equal(t, 1, m2.writerPuts["tx"])
}
