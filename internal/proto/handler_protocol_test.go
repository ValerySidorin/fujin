package proto

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fujin-io/fujin/internal/core"
	"github.com/fujin-io/fujin/internal/proto/pool"
	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/connector/config"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock connector writer
// ---------------------------------------------------------------------------

type mockConnectorWriter struct {
	mu       sync.Mutex
	produced []mockProduced
	flushed  int
	closed   bool

	txBegan      bool
	txCommitted  bool
	txRolledBack bool

	produceErr  error
	flushErr    error
	beginTxErr  error
	commitTxErr error
}

type mockProduced struct {
	msg     []byte
	headers [][]byte
}

func (w *mockConnectorWriter) Produce(_ context.Context, msg []byte, cb func(err error)) {
	w.mu.Lock()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	w.produced = append(w.produced, mockProduced{msg: cp})
	err := w.produceErr
	w.mu.Unlock()
	if cb != nil {
		cb(err)
	}
}

func (w *mockConnectorWriter) HProduce(_ context.Context, msg []byte, headers [][]byte, cb func(err error)) {
	w.mu.Lock()
	cp := make([]byte, len(msg))
	copy(cp, msg)
	hsCopy := make([][]byte, len(headers))
	for i, h := range headers {
		hsCopy[i] = append([]byte(nil), h...)
	}
	w.produced = append(w.produced, mockProduced{msg: cp, headers: hsCopy})
	err := w.produceErr
	w.mu.Unlock()
	if cb != nil {
		cb(err)
	}
}

func (w *mockConnectorWriter) Flush(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushed++
	return w.flushErr
}

func (w *mockConnectorWriter) BeginTx(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txBegan = true
	return w.beginTxErr
}

func (w *mockConnectorWriter) CommitTx(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txCommitted = true
	return w.commitTxErr
}

func (w *mockConnectorWriter) RollbackTx(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txRolledBack = true
	return nil
}

func (w *mockConnectorWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *mockConnectorWriter) getProduced() []mockProduced {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := make([]mockProduced, len(w.produced))
	copy(r, w.produced)
	return r
}

type deferredConnectorWriter struct {
	mu        sync.Mutex
	callbacks []func(error)
}

func (w *deferredConnectorWriter) Produce(_ context.Context, _ []byte, callback func(error)) {
	w.mu.Lock()
	w.callbacks = append(w.callbacks, callback)
	w.mu.Unlock()
}

func (w *deferredConnectorWriter) HProduce(_ context.Context, _ []byte, _ [][]byte, callback func(error)) {
	w.Produce(context.Background(), nil, callback)
}

func (*deferredConnectorWriter) Flush(context.Context) error      { return nil }
func (*deferredConnectorWriter) BeginTx(context.Context) error    { return nil }
func (*deferredConnectorWriter) CommitTx(context.Context) error   { return nil }
func (*deferredConnectorWriter) RollbackTx(context.Context) error { return nil }
func (*deferredConnectorWriter) Close() error                     { return nil }

func (w *deferredConnectorWriter) callbackCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.callbacks)
}

func (w *deferredConnectorWriter) complete(index int, err error) {
	w.mu.Lock()
	callback := w.callbacks[index]
	w.callbacks[index] = nil
	w.mu.Unlock()
	callback(err)
}

// ---------------------------------------------------------------------------
// Mock connector reader
// ---------------------------------------------------------------------------

type mockConnectorReader struct {
	autoCommit   bool
	msgIDArgsLen int
}

func (r *mockConnectorReader) Subscribe(_ context.Context, ready func() error, _ func([]byte, string, ...any)) error {
	return ready()
}
func (r *mockConnectorReader) SubscribeWithHeaders(_ context.Context, ready func() error, _ func([]byte, string, [][]byte, ...any)) error {
	return ready()
}
func (r *mockConnectorReader) Fetch(_ context.Context, _ uint32, frh func(uint32, error), _ func([]byte, string, ...any)) {
	frh(0, nil)
}
func (r *mockConnectorReader) FetchWithHeaders(_ context.Context, _ uint32, frh func(uint32, error), _ func([]byte, string, [][]byte, ...any)) {
	frh(0, nil)
}
func (r *mockConnectorReader) Ack(_ context.Context, _ [][]byte, ah func(error), _ func([]byte, error)) {
	ah(nil)
}
func (r *mockConnectorReader) Nack(_ context.Context, _ [][]byte, nh func(error), _ func([]byte, error)) {
	nh(nil)
}
func (r *mockConnectorReader) MsgIDArgsLen() int                                 { return r.msgIDArgsLen }
func (r *mockConnectorReader) EncodeMsgID(buf []byte, _ string, _ ...any) []byte { return buf }
func (r *mockConnectorReader) AutoCommit() bool                                  { return r.autoCommit }
func (r *mockConnectorReader) Close() error                                      { return nil }

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------
type testHarness struct {
	h       *Handler
	str     *mockStream
	out     *Outbound
	manager *protocolTestManager
}

type protocolTestManager struct {
	writers map[string]connector.WriteCloser
	reader  connector.ReadCloser
}

func (*protocolTestManager) RouteProfile(string) (connector.RouteProfile, error) {
	return connector.RouteProfile{
		Produce: true, Headers: true, Transactions: true, Subscribe: true, Fetch: true,
		ManualSettlement: true, ProduceGuarantee: connector.AcceptanceLocal,
		Settlement: connector.SettlementProfile{Ack: connector.AckSingle, Nack: connector.NackDrop},
	}, nil
}
func (m *protocolTestManager) RouteProfiles() map[string]connector.RouteProfile {
	profile, _ := m.RouteProfile("")
	return map[string]connector.RouteProfile{
		"pub": profile,
		"sub": profile,
		"tx":  profile,
	}
}

func (m *protocolTestManager) GetReader(string, bool) (connector.ReadCloser, error) {
	if m.reader == nil {
		return &mockConnectorReader{}, nil
	}
	return m.reader, nil
}

func (m *protocolTestManager) GetWriter(name string) (connector.WriteCloser, error) {
	if writer := m.writers[name]; writer != nil {
		return writer, nil
	}
	writer := &mockConnectorWriter{}
	m.writers[name] = writer
	return writer, nil
}

func (*protocolTestManager) PutWriter(connector.WriteCloser, string) error { return nil }
func (*protocolTestManager) DiscardWriter(w connector.WriteCloser) error   { return w.Close() }
func (*protocolTestManager) Close(context.Context) error                   { return nil }

func protocolTestConfigs(names ...string) config.ConnectorsConfig {
	configs := make(config.ConnectorsConfig, len(names))
	for _, name := range names {
		configs[name] = config.ConnectorConfig{Type: "test"}
	}
	return configs
}

func newProtocolTestHarness() *testHarness {
	str := &mockStream{}
	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	out := NewOutbound(str, 5*time.Second, l)
	manager := &protocolTestManager{writers: make(map[string]connector.WriteCloser)}
	baseConfig := protocolTestConfigs("test-connector", "my-connector", "test-conn", "conn1", "my-conn", "conn")

	h := &Handler{
		ctx: context.Background(),
		core: core.NewWithManagerFactory(context.Background(), baseConfig, nil, l, func(config.ConnectorConfig, string, *slog.Logger) core.Manager {
			return manager
		}),
		ps:           &parseState{},
		pingInterval: 2 * time.Second,
		pingTimeout:  5 * time.Second,
		closed:       make(chan struct{}),
		disconnect:   func() {},
		l:            l,
		out:          out,
		str:          str,
	}

	return &testHarness{h: h, str: str, out: out, manager: manager}
}

func newUnnegotiatedProtocolTestHarness() *testHarness {
	harness := newProtocolTestHarness()
	harness.h.ps.state = OP_EXPECT_HELLO
	harness.h.serverBuild = "v-test"
	return harness
}

// setConnected sets handler state to connected (as if BIND completed)
func (th *testHarness) setConnected(connectorName string) {
	configs := protocolTestConfigs(connectorName)
	th.manager = &protocolTestManager{writers: make(map[string]connector.WriteCloser)}
	th.h.core = core.NewWithManagerFactory(context.Background(), configs, nil, th.h.l, func(config.ConnectorConfig, string, *slog.Logger) core.Manager {
		return th.manager
	})
	if _, err := th.h.core.Bind(connectorName, nil, nil); err != nil {
		panic(err)
	}
}

// setConnectedWithWriter sets handler state to connected with a pre-populated route writer.
func (th *testHarness) setConnectedWithWriter(route string, w connector.WriteCloser) {
	th.setConnected("test")
	th.manager.writers[route] = w
}

func (th *testHarness) beginTransactionWithWriter(route string, w connector.WriteCloser) {
	th.manager.writers[route] = w
	if err := th.h.core.Begin(route); err != nil {
		panic(err)
	}
}

func (th *testHarness) activateTransactionWriter(route string, w connector.WriteCloser) {
	th.beginTransactionWithWriter(route, w)
	if err := th.h.core.TxProduce(nil, nil, nil); err != nil {
		panic(err)
	}
}

// startWriteLoop starts the outbound write loop and returns a done channel
func (th *testHarness) startWriteLoop() chan struct{} {
	done := make(chan struct{})
	go func() {
		th.out.WriteLoop()
		close(done)
	}()
	return done
}

// readResponse reads all bytes written to mock stream (with short timeout for async callbacks)
func (th *testHarness) readResponse(wait time.Duration) []byte {
	time.Sleep(wait)
	return th.str.written()
}

func (th *testHarness) close(done chan struct{}) {
	th.out.Close()
	if done != nil {
		<-done
	}
}

// feed feeds data through handler.handle()
func (th *testHarness) feed(data []byte) error {
	return th.h.handle(data)
}

// ---------------------------------------------------------------------------
// Frame builder helpers
// ---------------------------------------------------------------------------

// buildBindFrame constructs: BIND opcode + connectorNameLen(u32) + connectorName + metaCount(u16) + [meta k/v] + overridesCount(u16) + [overrides k/v]
func buildBindFrame(connectorName string, meta map[string]string, overrides map[string]string) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_BIND))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(connectorName)))
	buf = append(buf, connectorName...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(meta)))
	for k, v := range meta {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(overrides)))
	for k, v := range overrides {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

func buildHelloFrame(clientName, clientBuild string, versions ...v1.WireVersion) []byte {
	frame := []byte{byte(v1.OP_CODE_HELLO), v1.HelloFormat, byte(len(versions))}
	for _, version := range versions {
		frame = append(frame, byte(version))
	}
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(clientName)))
	frame = append(frame, clientName...)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(clientBuild)))
	return append(frame, clientBuild...)
}

// buildProduceFrame constructs: PRODUCE opcode + correlationID(4b) + routeLen(u32) + route + msgSize(u32) + msg.
func buildProduceFrame(cID [4]byte, route string, msg []byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_PRODUCE))
	buf = append(buf, cID[:]...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(route)))
	buf = append(buf, route...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	return buf
}

// buildHProduceFrame constructs HPRODUCE with a route, headers, and message.
func buildHProduceFrame(cID [4]byte, route string, headers []string, msg []byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_HPRODUCE))
	buf = append(buf, cID[:]...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(route)))
	buf = append(buf, route...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(headers)))
	for _, header := range headers {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(header)))
		buf = append(buf, header...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	return buf
}

// buildSubscribeFrame constructs SUBSCRIBE with the configured route.
func buildSubscribeFrame(cID [4]byte, autoCommit bool, route string) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_SUBSCRIBE))
	buf = append(buf, cID[:]...)
	if autoCommit {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(route)))
	buf = append(buf, route...)
	return buf
}

// buildFetchFrame constructs FETCH with the configured route.
func buildFetchFrame(cID [4]byte, autoCommit bool, route string, n uint32) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_FETCH))
	buf = append(buf, cID[:]...)
	if autoCommit {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(route)))
	buf = append(buf, route...)
	buf = binary.BigEndian.AppendUint32(buf, n)
	return buf
}

// buildTxBeginFrame constructs: TX_BEGIN opcode + correlationID(4b) + routeLen(u32) + route.
func buildTxBeginFrame(cID [4]byte, route string) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_BEGIN))
	buf = append(buf, cID[:]...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(route)))
	buf = append(buf, route...)
	return buf
}

func buildTxProduceFrame(cID [4]byte, msg []byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_PRODUCE))
	buf = append(buf, cID[:]...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	return buf
}

func buildTxHProduceFrame(cID [4]byte, headers []string, msg []byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_HPRODUCE))
	buf = append(buf, cID[:]...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(headers)))
	for _, header := range headers {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(header)))
		buf = append(buf, header...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	return buf
}

// buildTxCommitFrame constructs: TX_COMMIT opcode + correlationID(4b)
func buildTxCommitFrame(cID [4]byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_COMMIT))
	buf = append(buf, cID[:]...)
	return buf
}

// buildTxRollbackFrame constructs: TX_ROLLBACK opcode + correlationID(4b)
func buildTxRollbackFrame(cID [4]byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_TX_ROLLBACK))
	buf = append(buf, cID[:]...)
	return buf
}

// buildAckFrame constructs: ACK opcode + correlationID(4b) + subID(1b) + msgIDsLen(u32) + [msgIDTopicLen(u32) + topic + msgIDLen(u32) + msgID]...
func buildAckFrame(cID [4]byte, subID byte, msgIDs []struct{ topic, id string }) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_ACK))
	buf = append(buf, cID[:]...)
	buf = append(buf, subID)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgIDs)))
	for _, m := range msgIDs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.topic)))
		buf = append(buf, m.topic...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.id)))
		buf = append(buf, m.id...)
	}
	return buf
}

// buildNackFrame constructs: NACK opcode + correlationID(4b) + subID(1b) + msgIDsLen(u32) + [msgIDTopicLen(u32) + topic + msgIDLen(u32) + msgID]...
func buildNackFrame(cID [4]byte, subID byte, msgIDs []struct{ topic, id string }) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_NACK))
	buf = append(buf, cID[:]...)
	buf = append(buf, subID)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgIDs)))
	for _, m := range msgIDs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.topic)))
		buf = append(buf, m.topic...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.id)))
		buf = append(buf, m.id...)
	}
	return buf
}

func buildDisconnectFrame() []byte {
	return []byte{byte(v1.OP_CODE_DISCONNECT)}
}

func buildPongFrame() []byte {
	return []byte{byte(v1.RESP_CODE_PONG)}
}

func buildUnsubscribeFrame(cID [4]byte, subID byte) []byte {
	var buf []byte
	buf = append(buf, byte(v1.OP_CODE_UNSUBSCRIBE))
	buf = append(buf, cID[:]...)
	buf = append(buf, subID)
	return buf
}

// Ensure frame builders are referenced (used in various tests)
var (
	_ = buildSubscribeFrame
	_ = buildFetchFrame
	_ = buildAckFrame
	_ = buildNackFrame
)

// ---------------------------------------------------------------------------
// State Machine — Opcode Dispatch Tests
// ---------------------------------------------------------------------------

func TestHandle_RequiresSuccessfulHelloBeforeBind(t *testing.T) {
	harness := newUnnegotiatedProtocolTestHarness()
	assert.ErrorIs(t, harness.feed(buildBindFrame("test-connector", nil, nil)), ErrParseProto)
}

func TestHandle_HelloNegotiatesVersionBeforeBind(t *testing.T) {
	harness := newUnnegotiatedProtocolTestHarness()
	done := harness.startWriteLoop()
	defer harness.close(done)

	frame := append(buildHelloFrame("fujin-go", "v-client", v1.WireVersion(255), v1.Version), buildBindFrame("test-connector", nil, nil)...)
	for _, value := range frame {
		require.NoError(t, harness.feed([]byte{value}))
	}
	require.Eventually(t, func() bool { return len(harness.str.written()) > 0 }, time.Second, time.Millisecond)
	response := harness.str.written()
	require.GreaterOrEqual(t, len(response), 2)
	assert.Equal(t, byte(v1.RESP_CODE_HELLO), response[0])
	assert.Equal(t, byte(v1.STATUS_OK), response[1])
	require.GreaterOrEqual(t, len(response), 4)
	assert.Equal(t, byte(v1.Version), response[3])
	assert.Equal(t, OP_START, harness.h.ps.state)
	assert.Equal(t, core.StateConnected, harness.h.core.State())
}

func TestHandle_HelloRejectsUnsupportedProtocolVersion(t *testing.T) {
	harness := newUnnegotiatedProtocolTestHarness()
	done := harness.startWriteLoop()
	defer harness.close(done)

	err := harness.feed(buildHelloFrame("fujin-go", "v-client", v1.WireVersion(255)))
	require.ErrorIs(t, err, ErrClose)
	require.Eventually(t, func() bool { return len(harness.str.written()) > 0 }, time.Second, time.Millisecond)
	response := harness.str.written()
	require.GreaterOrEqual(t, len(response), 2)
	assert.Equal(t, byte(v1.RESP_CODE_HELLO), response[0])
	assert.Equal(t, byte(v1.STATUS_UNIMPLEMENTED), response[1])
	assert.Equal(t, OP_HELLO_CLIENT_BUILD, harness.h.ps.state)
}

func TestHandle_BindState_AcceptsPong(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed(buildPongFrame())
	assert.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state, "parser state should return to OP_START")
}

func TestHandle_BindState_AcceptsBind(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("test-connector", nil, nil)
	err := th.feed(frame)
	assert.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, core.StateConnected, th.h.core.State())
}

func TestHandle_BindState_RejectsProduceOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	frame := buildProduceFrame([4]byte{0, 0, 0, 1}, "topic", []byte("msg"))
	err := th.feed(frame)
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_RejectsFetchOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed([]byte{byte(v1.OP_CODE_FETCH)})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_RejectsDisconnect(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_RejectsInvalidOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed([]byte{0xFF})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_BindState_AcceptsSubscribe(t *testing.T) {
	// In BIND state, SUBSCRIBE is allowed (sets state to OP_SUBSCRIBE)
	th := newProtocolTestHarness()
	// Just feed the opcode byte to check dispatch
	err := th.feed([]byte{byte(v1.OP_CODE_SUBSCRIBE)})
	assert.NoError(t, err)
	// Handler accepted subscribe and is now parsing its args
	assert.Equal(t, OP_SUBSCRIBE, th.h.ps.state)
}

func TestHandle_ConnectedState_AcceptsDisconnect(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

func TestHandle_ConnectedState_AcceptsPong(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed(buildPongFrame())
	assert.NoError(t, err)
}

func TestHandle_ConnectedState_RejectsInvalidOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{0xFF})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_ConnectedState_AcceptsAllWriterCmds(t *testing.T) {
	ops := []struct {
		name     string
		opcode   byte
		expState int
	}{
		{"PRODUCE", byte(v1.OP_CODE_PRODUCE), OP_PRODUCE},
		{"HPRODUCE", byte(v1.OP_CODE_HPRODUCE), OP_PRODUCE_H},
		{"TX_BEGIN", byte(v1.OP_CODE_TX_BEGIN), OP_BEGIN_TX},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			th := newProtocolTestHarness()
			th.setConnected("test")
			err := th.feed([]byte{op.opcode})
			assert.NoError(t, err)
			assert.Equal(t, op.expState, th.h.ps.state)
		})
	}
}

func TestHandle_ConnectedState_AcceptsAllReaderCmds(t *testing.T) {
	ops := []struct {
		name     string
		opcode   byte
		expState int
	}{
		{"FETCH", byte(v1.OP_CODE_FETCH), OP_FETCH},
		{"SUBSCRIBE", byte(v1.OP_CODE_SUBSCRIBE), OP_SUBSCRIBE},
		{"UNSUBSCRIBE", byte(v1.OP_CODE_UNSUBSCRIBE), OP_UNSUBSCRIBE},
		{"ACK", byte(v1.OP_CODE_ACK), OP_ACK},
		{"NACK", byte(v1.OP_CODE_NACK), OP_NACK},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			th := newProtocolTestHarness()
			th.setConnected("test")
			err := th.feed([]byte{op.opcode})
			assert.NoError(t, err)
			assert.Equal(t, op.expState, th.h.ps.state)
		})
	}
}

func TestHandle_ConnectedState_HFetchSetsHeaderedFlag(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_HFETCH)})
	assert.NoError(t, err)
	assert.True(t, th.h.ps.fa.headered)
	assert.Equal(t, OP_FETCH, th.h.ps.state)
}

func TestHandle_ConnectedState_HSubscribeSetsHeaderedFlag(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_HSUBSCRIBE)})
	assert.NoError(t, err)
	assert.True(t, th.h.ps.sa.headered)
	assert.Equal(t, OP_SUBSCRIBE, th.h.ps.state)
}

func TestHandle_ConnectedState_TxCommitOutsideTx_DispatchesToCore(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_TX_COMMIT)})
	assert.NoError(t, err)
	assert.Equal(t, OP_COMMIT_TX, th.h.ps.state)
}

func TestHandle_ConnectedState_TxRollbackOutsideTx_DispatchesToCore(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed([]byte{byte(v1.OP_CODE_TX_ROLLBACK)})
	assert.NoError(t, err)
	assert.Equal(t, OP_ROLLBACK_TX, th.h.ps.state)
}

func TestHandle_InTxState_AcceptsDisconnect(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

func TestHandle_InTxState_AcceptsPong(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed(buildPongFrame())
	assert.NoError(t, err)
}

func TestHandle_InTxState_RejectsNormalProduce(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed([]byte{byte(v1.OP_CODE_PRODUCE)})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_InTxState_TxProduceUsesTransactionDecoder(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed([]byte{byte(v1.OP_CODE_TX_PRODUCE)})
	assert.NoError(t, err)
	assert.True(t, th.h.ps.pa.transactional)
	assert.Equal(t, OP_TX_PRODUCE, th.h.ps.state)
}

func TestHandle_InTxState_TxHProduceUsesTransactionDecoder(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed([]byte{byte(v1.OP_CODE_TX_HPRODUCE)})
	assert.NoError(t, err)
	assert.True(t, th.h.ps.pa.transactional)
	assert.True(t, th.h.ps.pa.headered)
	assert.Equal(t, OP_TX_PRODUCE_H, th.h.ps.state)
}

func TestHandle_InTxState_TxBeginDispatchesToCore(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed([]byte{byte(v1.OP_CODE_TX_BEGIN)})
	assert.NoError(t, err)
	assert.Equal(t, OP_BEGIN_TX, th.h.ps.state)
}

func TestHandle_InTxState_TxCommitDispatchesToCommit(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed([]byte{byte(v1.OP_CODE_TX_COMMIT)})
	assert.NoError(t, err)
	assert.Equal(t, OP_COMMIT_TX, th.h.ps.state)
}

func TestHandle_InTxState_TxRollbackDispatchesToRollback(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed([]byte{byte(v1.OP_CODE_TX_ROLLBACK)})
	assert.NoError(t, err)
	assert.Equal(t, OP_ROLLBACK_TX, th.h.ps.state)
}

func TestHandle_InTxState_RejectsInvalidOpcode(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed([]byte{0xFF})
	assert.ErrorIs(t, err, ErrParseProto)
}

// ---------------------------------------------------------------------------
// BIND Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_Bind_ParsesConnectorName(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("my-connector", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, core.StateConnected, th.h.core.State())
}

func TestHandle_Bind_WithMeta(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	meta := map[string]string{
		"api-key": "secret123",
		"user":    "test-user",
	}
	frame := buildBindFrame("test-conn", meta, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, core.StateConnected, th.h.core.State())
	assert.Equal(t, OP_START, th.h.ps.state)
}

func TestHandle_Bind_WithOverrides_ParsesCorrectly(t *testing.T) {
	// Note: ApplyOverrides may fail without a real connector factory registered,
	// but the parsing itself should succeed and the handler should respond with an error
	// response rather than crashing.
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	overrides := map[string]string{
		"writer.pub.transactional_id": "my-tx-id",
	}
	frame := buildBindFrame("test-conn", nil, overrides)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
}

func TestHandle_Bind_WithMetaAndOverrides(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	meta := map[string]string{"key1": "val1"}
	overrides := map[string]string{"setting1": "value1"}
	frame := buildBindFrame("conn1", meta, overrides)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
}

func TestHandle_Bind_AlreadyConnected(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	// First BIND
	frame := buildBindFrame("test-conn", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)
	assert.Equal(t, core.StateConnected, th.h.core.State())

	// Second BIND should fail (already connected, so we're in CONNECTED state)
	// In CONNECTED state, BIND is not a valid opcode
	err = th.feed([]byte{byte(v1.OP_CODE_BIND)})
	assert.ErrorIs(t, err, ErrParseProto)
}

func TestHandle_Bind_EmptyMeta(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("test-conn", map[string]string{}, map[string]string{})
	err := th.feed(frame)
	require.NoError(t, err)
	assert.Equal(t, core.StateConnected, th.h.core.State())
}

func TestHandle_Bind_ResponseIncludesSortedRouteCapabilities(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	require.NoError(t, th.feed(buildBindFrame("test-conn", nil, nil)))
	resp := th.readResponse(50 * time.Millisecond)

	want := []byte{byte(v1.RESP_CODE_BIND), byte(v1.STATUS_OK), 0, 0, 0, 3}
	for _, route := range []string{"pub", "sub", "tx"} {
		want = binary.BigEndian.AppendUint32(want, uint32(len(route)))
		want = append(want, route...)
		want = append(want,
			v1.ROUTE_CAP_PRODUCE|v1.ROUTE_CAP_HEADERS|v1.ROUTE_CAP_TRANSACTIONS|v1.ROUTE_CAP_SUBSCRIBE|v1.ROUTE_CAP_FETCH|v1.ROUTE_CAP_MANUAL_SETTLEMENT,
			byte(v1.PRODUCE_GUARANTEE_LOCAL_ACCEPT),
			byte(v1.ACK_GRANULARITY_SINGLE),
			byte(v1.NACK_EFFECT_DROP),
		)
	}
	assert.Equal(t, want, resp)
}

// ---------------------------------------------------------------------------
// PRODUCE Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_Produce_FullFrame(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("test-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	msg := []byte("hello world")
	frame := buildProduceFrame(cID, "test-topic", msg)
	err := th.feed(frame)
	require.NoError(t, err)

	// Wait for async produce callback
	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, msg, produced[0].msg)
}

func TestHandle_Produce_DeferredCallbacksPreserveCorrelationIDsAcrossResponseReuse(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &deferredConnectorWriter{}
	th.setConnectedWithWriter("route", w)

	for id := byte(1); id <= 2; id++ {
		correlationID := [4]byte{0, 0, 0, id}
		require.NoError(t, th.feed(buildProduceFrame(correlationID, "route", []byte{id})))
	}
	require.Eventually(t, func() bool { return w.callbackCount() == 2 }, time.Second, time.Millisecond)

	w.complete(0, nil)
	require.Eventually(t, func() bool { return len(th.str.written()) >= 6 }, time.Second, time.Millisecond)

	thirdCorrelationID := [4]byte{0, 0, 0, 3}
	require.NoError(t, th.feed(buildProduceFrame(thirdCorrelationID, "route", []byte{3})))
	require.Eventually(t, func() bool { return w.callbackCount() == 3 }, time.Second, time.Millisecond)
	w.complete(2, nil)
	w.complete(1, nil)
	require.Eventually(t, func() bool { return len(th.str.written()) >= 18 }, time.Second, time.Millisecond)
	th.close(done)

	response := th.str.written()
	require.Len(t, response, 18)
	for responseIndex, correlationID := range []byte{1, 3, 2} {
		offset := responseIndex * 6
		assert.Equal(t, byte(v1.RESP_CODE_PRODUCE), response[offset])
		assert.Equal(t, []byte{0, 0, 0, correlationID}, response[offset+1:offset+5])
		assert.Equal(t, byte(v1.STATUS_OK), response[offset+5])
	}
}

func TestHandle_Produce_LargePayload(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("big-topic", w)

	cID := [4]byte{0, 0, 0, 2}
	msg := make([]byte, 1024*1024) // 1 MiB, larger than the pooled buffer classes
	for i := range msg {
		msg[i] = byte(i % 256)
	}

	frame := buildProduceFrame(cID, "big-topic", msg)
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, msg, produced[0].msg)
}

func TestHandle_Produce_MultipleMessages(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic1", w)

	msgs := [][]byte{[]byte("msg1"), []byte("msg2"), []byte("msg3")}
	var combined []byte
	for i, msg := range msgs {
		cID := [4]byte{0, 0, 0, byte(i + 1)}
		combined = append(combined, buildProduceFrame(cID, "topic1", msg)...)
	}

	err := th.feed(combined)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 3)
	for i, p := range produced {
		assert.Equal(t, msgs[i], p.msg)
	}
}

func TestHandle_Produce_Response(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("resp-topic", w)

	cID := [4]byte{0, 0, 0, 42}
	frame := buildProduceFrame(cID, "resp-topic", []byte("data"))
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Response: RESP_CODE_PRODUCE(1b) + correlationID(4b) + STATUS_OK(1b)
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_PRODUCE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
}

func TestHandle_Produce_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("byte-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "byte-topic", []byte("byte-by-byte"))

	// Feed one byte at a time
	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("byte-by-byte"), produced[0].msg)
}

func TestHandle_Produce_ChunkedInput(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("chunk-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "chunk-topic", []byte("chunked-data-payload"))

	// Feed in random chunks
	chunks := []int{3, 7, 2, 5, 1, 4, 100} // last one will be clamped
	offset := 0
	for _, size := range chunks {
		end := offset + size
		if end > len(frame) {
			end = len(frame)
		}
		if offset >= len(frame) {
			break
		}
		err := th.feed(frame[offset:end])
		require.NoError(t, err)
		offset = end
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("chunked-data-payload"), produced[0].msg)
}

func TestHandle_Produce_StateResetAfterComplete(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "topic", []byte("msg"))
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state, "state should be reset to OP_START after complete PRODUCE")
	assert.Nil(t, th.h.ps.ca.cID, "correlation ID should be nil after complete PRODUCE")

	th.close(done)
}

// ---------------------------------------------------------------------------
// HPRODUCE Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_HProduce_WithHeaders(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	headers := []string{"key1", "val1", "key2", "val2"} // 4 strings = 2 key-value pairs
	msg := []byte("headered-msg")
	frame := buildHProduceFrame(cID, "h-topic", headers, msg)
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, msg, produced[0].msg)
	require.Len(t, produced[0].headers, 4)
	assert.Equal(t, "key1", string(produced[0].headers[0]))
	assert.Equal(t, "val1", string(produced[0].headers[1]))
	assert.Equal(t, "key2", string(produced[0].headers[2]))
	assert.Equal(t, "val2", string(produced[0].headers[3]))
}

func TestHandle_HProduce_ZeroHeaders(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildHProduceFrame(cID, "h-topic", nil, []byte("no-headers"))
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("no-headers"), produced[0].msg)
	assert.Empty(t, produced[0].headers)
}

func TestHandle_HProduce_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	headers := []string{"h1", "v1"}
	frame := buildHProduceFrame(cID, "h-topic", headers, []byte("hdr-msg"))

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("hdr-msg"), produced[0].msg)
}

func TestHandle_HProduce_Response(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 7}
	frame := buildHProduceFrame(cID, "h-topic", nil, []byte("x"))
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_HPRODUCE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
}

// ---------------------------------------------------------------------------
// BIND Parsing — Fragmented Input
// ---------------------------------------------------------------------------

func TestHandle_Bind_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	frame := buildBindFrame("my-conn", map[string]string{"k": "v"}, nil)

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.Equal(t, core.StateConnected, th.h.core.State())
	assert.Equal(t, OP_START, th.h.ps.state)
}

// ---------------------------------------------------------------------------
// TX Operations Tests
// ---------------------------------------------------------------------------

func TestHandle_TxBegin_FromConnected(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxBeginFrame(cID, "tx-route")
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, core.StateInTransaction, th.h.core.State())

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_BEGIN), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
}

func TestHandle_TxBegin_WhenAlreadyInTx(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx-route"))

	cID := [4]byte{0, 0, 0, 2}
	frame := buildTxBeginFrame(cID, "tx-route")
	err := th.feed(frame)
	require.NoError(t, err)

	// Should return to OP_START
	assert.Equal(t, OP_START, th.h.ps.state)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Should get an error response
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_BEGIN), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_FAILED_PRECONDITION), resp[5])
}

func TestHandle_TxProduce_UsesActiveRouteWithoutRouteField(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	w := &mockConnectorWriter{}
	th.beginTransactionWithWriter("tx-route", w)

	cID := [4]byte{0, 0, 0, 3}
	require.NoError(t, th.feed(buildTxProduceFrame(cID, []byte("message"))))
	assert.Equal(t, OP_START, th.h.ps.state)
	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("message"), produced[0].msg)
	assert.Nil(t, produced[0].headers)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_PRODUCE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
}

func TestHandle_TxHProduce_UsesActiveRouteWithoutRouteField(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	w := &mockConnectorWriter{}
	th.beginTransactionWithWriter("tx-route", w)

	cID := [4]byte{0, 0, 0, 4}
	require.NoError(t, th.feed(buildTxHProduceFrame(cID, []string{"key", "value"}, []byte("message"))))
	assert.Equal(t, OP_START, th.h.ps.state)
	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("message"), produced[0].msg)
	assert.Equal(t, [][]byte{[]byte("key"), []byte("value")}, produced[0].headers)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_HPRODUCE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
}

func TestHandle_TxCommit_InTxState(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	// Set up a mock transaction writer through Session Core.
	w := &mockConnectorWriter{}
	th.activateTransactionWriter("tx-topic", w)

	cID := [4]byte{0, 0, 0, 3}
	frame := buildTxCommitFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, core.StateConnected, th.h.core.State())

	w.mu.Lock()
	assert.True(t, w.txCommitted)
	w.mu.Unlock()

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_COMMIT), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
}

func TestHandle_TxCommit_OutsideTx(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 4}
	frame := buildTxCommitFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Should get error response (invalid tx state)
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_COMMIT), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_FAILED_PRECONDITION), resp[5])
}

func TestHandle_TxRollback_InTxState(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	w := &mockConnectorWriter{}
	th.activateTransactionWriter("tx-topic", w)

	cID := [4]byte{0, 0, 0, 5}
	frame := buildTxRollbackFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state)
	assert.Equal(t, core.StateConnected, th.h.core.State())

	w.mu.Lock()
	assert.True(t, w.txRolledBack)
	w.mu.Unlock()

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_ROLLBACK), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
}

func TestHandle_TxRollback_OutsideTx(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 6}
	frame := buildTxRollbackFrame(cID)
	err := th.feed(frame)
	require.NoError(t, err)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_TX_ROLLBACK), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_FAILED_PRECONDITION), resp[5])
}

func TestAppendAutoCommitFetchMessagePacksAcrossLargePoolBoundary(t *testing.T) {
	payload := bytes.Repeat([]byte{0x7f}, 32*1024)
	lease := &bufsLease{bufs: [][]byte{append(pool.Get(pool.SIZE_TINY), make([]byte, 11)...)}}
	t.Cleanup(func() {
		for _, buffer := range lease.bufs {
			pool.Put(buffer)
		}
	})

	appendAutoCommitFetchMessage(lease, payload)
	appendAutoCommitFetchMessage(lease, payload)

	require.Len(t, lease.bufs, 2)
	expected := make([]byte, 11)
	for range 2 {
		expected = binary.BigEndian.AppendUint32(expected, uint32(len(payload)))
		expected = append(expected, payload...)
	}
	assert.Equal(t, expected, bytes.Join(lease.bufs, nil))
}

func TestAppendAutoCommitHFetchMessagePacksAcrossLargePoolBoundary(t *testing.T) {
	payload := bytes.Repeat([]byte{0x7f}, 32*1024)
	headers := [][]byte{[]byte("key"), []byte("value")}
	lease := &bufsLease{bufs: [][]byte{append(pool.Get(pool.SIZE_TINY), make([]byte, 11)...)}}
	t.Cleanup(func() {
		for _, buffer := range lease.bufs {
			pool.Put(buffer)
		}
	})

	appendAutoCommitHFetchMessage(lease, payload, headers)
	appendAutoCommitHFetchMessage(lease, payload, headers)

	require.Len(t, lease.bufs, 2)
	expected := make([]byte, 11)
	for range 2 {
		expected = binary.BigEndian.AppendUint16(expected, uint16(len(headers)))
		for _, header := range headers {
			expected = binary.BigEndian.AppendUint32(expected, uint32(len(header)))
			expected = append(expected, header...)
		}
		expected = binary.BigEndian.AppendUint32(expected, uint32(len(payload)))
		expected = append(expected, payload...)
	}
	assert.Equal(t, expected, bytes.Join(lease.bufs, nil))
}

// ---------------------------------------------------------------------------
// FETCH Parsing Tests (parsing only — up to the fetch() call)
// ---------------------------------------------------------------------------

func TestHandle_Fetch_ParsesArgs(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")

	// Build a fetch frame manually up to the point where fetch() would be called
	cID := [4]byte{0, 0, 0, 1}

	// Feed opcode
	err := th.feed([]byte{byte(v1.OP_CODE_FETCH)})
	require.NoError(t, err)
	assert.Equal(t, OP_FETCH, th.h.ps.state)

	// Feed correlation ID
	err = th.feed(cID[:])
	require.NoError(t, err)
	assert.Equal(t, OP_FETCH_AUTO_COMMIT_ARG, th.h.ps.state)

	// Feed autoCommit = true
	err = th.feed([]byte{1})
	require.NoError(t, err)
	assert.True(t, th.h.ps.fa.autoCommit)
	assert.Equal(t, OP_FETCH_ROUTE_ARG, th.h.ps.state)

	// Feed route length (5 = "route").
	routeLen := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLen, 5)
	err = th.feed(routeLen)
	require.NoError(t, err)
	assert.Equal(t, uint32(5), th.h.ps.fa.routeLen)
	assert.Equal(t, OP_FETCH_ROUTE_PAYLOAD, th.h.ps.state)

	// Feed route.
	err = th.feed([]byte("route"))
	require.NoError(t, err)
	assert.Equal(t, "route", th.h.ps.fa.route)
	assert.Equal(t, OP_FETCH_N_ARG, th.h.ps.state)
}

func TestHandle_Fetch_InvalidAutoCommit(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	// Opcode + cID + invalid autoCommit byte
	frame := append([]byte{byte(v1.OP_CODE_FETCH)}, cID[:]...)
	frame = append(frame, 2) // invalid: not 0 or 1
	err := th.feed(frame)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// SUBSCRIBE Parsing Tests (parsing only)
// ---------------------------------------------------------------------------

func TestHandle_Subscribe_ParsesArgs(t *testing.T) {
	th := newProtocolTestHarness()

	cID := [4]byte{0, 0, 0, 1}

	// Feed opcode
	err := th.feed([]byte{byte(v1.OP_CODE_SUBSCRIBE)})
	require.NoError(t, err)
	assert.Equal(t, OP_SUBSCRIBE, th.h.ps.state)

	// Feed correlation ID
	err = th.feed(cID[:])
	require.NoError(t, err)
	assert.Equal(t, OP_SUBSCRIBE_AUTO_COMMIT_ARG, th.h.ps.state)

	// Feed autoCommit = false
	err = th.feed([]byte{0})
	require.NoError(t, err)
	assert.False(t, th.h.ps.sa.autoCommit)
	assert.Equal(t, OP_SUBSCRIBE_ROUTE_ARG, th.h.ps.state)

	// Feed route length (10 = "test-route").
	routeLen := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLen, 10)
	err = th.feed(routeLen)
	require.NoError(t, err)
	assert.Equal(t, uint32(10), th.h.ps.sa.routeLen)
	assert.Equal(t, OP_SUBSCRIBE_ROUTE_PAYLOAD, th.h.ps.state)
}

func TestHandle_Subscribe_InvalidAutoCommit(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	cID := [4]byte{0, 0, 0, 1}
	frame := append([]byte{byte(v1.OP_CODE_SUBSCRIBE)}, cID[:]...)
	frame = append(frame, 5) // invalid
	err := th.feed(frame)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// ACK Parsing Tests (parsing only — up to ack handler call)
// ---------------------------------------------------------------------------

func TestHandle_Ack_ZeroMsgIDs(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	// Build ACK frame with zero msgIDs (triggers enqueueAckSuccess)
	var frame []byte
	frame = append(frame, byte(v1.OP_CODE_ACK))
	frame = append(frame, cID[:]...)
	frame = append(frame, 42)                       // subID = 42
	frame = binary.BigEndian.AppendUint32(frame, 0) // 0 msgIDs

	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, OP_START, th.h.ps.state, "should reset to OP_START after zero-msgID ACK")

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Should get ACK success: RESP_CODE_ACK(1) + cID(4) + STATUS_OK(1) + count(4) = 10 bytes
	require.GreaterOrEqual(t, len(resp), 10)
	assert.Equal(t, byte(v1.RESP_CODE_ACK), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_OK), resp[5])
	// Verify count = 0 in response
	respCount := binary.BigEndian.Uint32(resp[6:10])
	assert.Equal(t, uint32(0), respCount)
}

func TestAckResponseQueuesSuccessfulBatchAsSingleFrame(t *testing.T) {
	out := newTestOutbound(&mockStream{})
	cID := append(pool.Get(v1.Uint32Len), 0, 0, 0, 7)
	messageIDs := GetBufs()
	for _, value := range []string{"first", "second"} {
		messageIDs = append(messageIDs, append(pool.Get(len(value)), value...))
	}
	expected := []byte{byte(v1.RESP_CODE_ACK), 0, 0, 0, 7, byte(v1.STATUS_OK), 0, 0, 0, 2}
	for _, messageID := range messageIDs {
		expected = binary.BigEndian.AppendUint32(expected, uint32(len(messageID)))
		expected = append(expected, messageID...)
		expected = append(expected, byte(v1.STATUS_OK))
	}

	response := &ackResponse{
		h:          &Handler{out: out},
		op:         byte(v1.RESP_CODE_ACK),
		messageIDs: messageIDs,
		cID:        cID,
		remaining:  len(messageIDs),
	}
	response.onResult(nil)
	response.onMessage(messageIDs[0], nil)
	response.onMessage(messageIDs[1], nil)

	require.Len(t, out.v, 1)
	assert.Equal(t, expected, out.v[0])
	pool.Put(out.v[0])
}

// ---------------------------------------------------------------------------
// PONG Tests
// ---------------------------------------------------------------------------

func TestHandle_Pong_AllStates(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testHarness)
	}{
		{name: "BIND", setup: func(*testHarness) {}},
		{name: "CONNECTED", setup: func(th *testHarness) { th.setConnected("test") }},
		{name: "IN_TX", setup: func(th *testHarness) {
			th.setConnected("test")
			require.NoError(t, th.h.core.Begin("tx"))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			th := newProtocolTestHarness()
			tt.setup(th)
			err := th.feed(buildPongFrame())
			assert.NoError(t, err)
			assert.Equal(t, OP_START, th.h.ps.state)
		})
	}
}

// ---------------------------------------------------------------------------
// DISCONNECT Tests
// ---------------------------------------------------------------------------

func TestHandle_Disconnect_FromConnected(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

func TestHandle_Disconnect_FromInTx(t *testing.T) {
	th := newProtocolTestHarness()
	th.setConnected("test")
	require.NoError(t, th.h.core.Begin("tx"))
	err := th.feed(buildDisconnectFrame())
	assert.ErrorIs(t, err, ErrClose)
}

// ---------------------------------------------------------------------------
// Fragmented Input Tests
// ---------------------------------------------------------------------------

func TestHandle_ProduceThenDisconnect_InOneBuffer(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	cID := [4]byte{0, 0, 0, 1}
	produceFrame := buildProduceFrame(cID, "topic", []byte("msg"))
	disconnectFrame := buildDisconnectFrame()

	combined := append(produceFrame, disconnectFrame...)
	err := th.feed(combined)
	assert.ErrorIs(t, err, ErrClose)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	// Produce should have been processed before disconnect
	produced := w.getProduced()
	assert.Len(t, produced, 1)
	assert.Equal(t, []byte("msg"), produced[0].msg)
}

func TestHandle_MultipleProducesInOneBuffer(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	var combined []byte
	for i := 0; i < 10; i++ {
		cID := [4]byte{0, 0, 0, byte(i + 1)}
		combined = append(combined, buildProduceFrame(cID, "topic", []byte("data"))...)
	}

	err := th.feed(combined)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	assert.Len(t, produced, 10)
}

func TestHandle_BindThenProduce_Sequential(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()

	// First: BIND
	bindFrame := buildBindFrame("test-conn", nil, nil)
	err := th.feed(bindFrame)
	require.NoError(t, err)
	require.Equal(t, core.StateConnected, th.h.core.State())

	// Inject a writer into the now-connected handler
	w := &mockConnectorWriter{}
	th.manager.writers["my-topic"] = w

	// Second: PRODUCE
	cID := [4]byte{0, 0, 0, 1}
	produceFrame := buildProduceFrame(cID, "my-topic", []byte("after-bind"))
	err = th.feed(produceFrame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	assert.Equal(t, []byte("after-bind"), produced[0].msg)
}

func TestHandle_BindThenProduce_InOneBuffer(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()

	bindFrame := buildBindFrame("test-conn", nil, nil)

	// We can't really do PRODUCE in the same buffer as BIND because
	// handleBind creates the cman but doesn't register writer factories.
	// However, we can test that parsing continues correctly.
	err := th.feed(bindFrame)
	require.NoError(t, err)
	assert.Equal(t, core.StateConnected, th.h.core.State())
	assert.Equal(t, OP_START, th.h.ps.state)

	th.close(done)
}

// ---------------------------------------------------------------------------
// TX_BEGIN → PRODUCE → TX_COMMIT flow
// ---------------------------------------------------------------------------

func TestHandle_TxBegin_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxBeginFrame(cID, "tx-route")

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.Equal(t, core.StateInTransaction, th.h.core.State())
	th.close(done)
}

func TestHandle_TxCommit_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	w := &mockConnectorWriter{}
	th.activateTransactionWriter("tx-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxCommitFrame(cID)

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.Equal(t, core.StateConnected, th.h.core.State())
	w.mu.Lock()
	assert.True(t, w.txCommitted)
	w.mu.Unlock()
	th.close(done)
}

func TestHandle_TxRollback_ByteByByte(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")
	w := &mockConnectorWriter{}
	th.activateTransactionWriter("tx-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	frame := buildTxRollbackFrame(cID)

	for _, b := range frame {
		err := th.feed([]byte{b})
		require.NoError(t, err)
	}

	assert.Equal(t, core.StateConnected, th.h.core.State())
	w.mu.Lock()
	assert.True(t, w.txRolledBack)
	w.mu.Unlock()
	th.close(done)
}

// ---------------------------------------------------------------------------
// UNSUBSCRIBE Parsing Tests
// ---------------------------------------------------------------------------

func TestHandle_Unsubscribe_ParsesCorrelationAndSubID(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildUnsubscribeFrame(cID, 5)
	err := th.feed(frame)
	require.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state)

	resp := th.readResponse(50 * time.Millisecond)
	th.close(done)

	// Unknown subscription IDs now share the gRPC error semantics.
	require.GreaterOrEqual(t, len(resp), 6)
	assert.Equal(t, byte(v1.RESP_CODE_UNSUBSCRIBE), resp[0])
	assert.Equal(t, cID[:], resp[1:5])
	assert.Equal(t, byte(v1.STATUS_NOT_FOUND), resp[5])
}

// ---------------------------------------------------------------------------
// Empty input
// ---------------------------------------------------------------------------

func TestHandle_EmptyInput(t *testing.T) {
	th := newProtocolTestHarness()
	err := th.feed([]byte{})
	assert.NoError(t, err)
	assert.Equal(t, OP_START, th.h.ps.state)
}

// ---------------------------------------------------------------------------
// Produce with empty payload (msg size = 0) should error
// ---------------------------------------------------------------------------

func TestHandle_Produce_EmptyPayload(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	frame := buildProduceFrame(cID, "topic", []byte{})
	err := th.feed(frame)
	// Empty msg size (0) should trigger ErrWriteMsgSizeArgEmpty
	assert.ErrorIs(t, err, ErrWriteMsgSizeArgEmpty)

	th.close(done)
}

// ---------------------------------------------------------------------------
// Produce with empty topic should error
// ---------------------------------------------------------------------------

func TestHandle_Produce_EmptyTopic(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	th.setConnected("test")

	cID := [4]byte{0, 0, 0, 1}
	// Build frame with topic len = 0 which will fail validation
	var frame []byte
	frame = append(frame, byte(v1.OP_CODE_PRODUCE))
	frame = append(frame, cID[:]...)
	frame = binary.BigEndian.AppendUint32(frame, 0) // topic len = 0
	err := th.feed(frame)
	assert.ErrorIs(t, err, ErrWriteRouteLenArgEmpty)

	th.close(done)
}

// ---------------------------------------------------------------------------
// Multiple sequential commands across handle() calls
// ---------------------------------------------------------------------------

func TestHandle_SequentialProduces_AcrossCalls(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("topic", w)

	for i := 0; i < 5; i++ {
		cID := [4]byte{0, 0, 0, byte(i + 1)}
		frame := buildProduceFrame(cID, "topic", []byte("seq-msg"))
		err := th.feed(frame)
		require.NoError(t, err)
		assert.Equal(t, OP_START, th.h.ps.state)
	}

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	assert.Len(t, produced, 5)
	for _, p := range produced {
		assert.Equal(t, []byte("seq-msg"), p.msg)
	}
}

// ---------------------------------------------------------------------------
// Verify BIND initializes the shared Session Core.
// ---------------------------------------------------------------------------

func TestHandle_Bind_SetsUpSessionCore(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	defer th.close(done)

	assert.Equal(t, core.StateUnbound, th.h.core.State())

	frame := buildBindFrame("my-connector", nil, nil)
	err := th.feed(frame)
	require.NoError(t, err)

	assert.Equal(t, core.StateConnected, th.h.core.State())
}

// ---------------------------------------------------------------------------
// Verify handler starts with clean state
// ---------------------------------------------------------------------------

func TestHandle_InitialState(t *testing.T) {
	th := newProtocolTestHarness()
	h := th.h

	assert.Equal(t, OP_START, h.ps.state)
	assert.NotNil(t, h.core)
	assert.Equal(t, core.StateUnbound, h.core.State())
	assert.NotNil(t, h.ps)
	assert.NotNil(t, h.out)
}

// ---------------------------------------------------------------------------
// PRODUCE with multiple topics
// ---------------------------------------------------------------------------

func TestHandle_Produce_MultipleDifferentTopics(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w1 := &mockConnectorWriter{}
	w2 := &mockConnectorWriter{}
	th.setConnected("test")
	th.manager.writers["topic-a"] = w1
	th.manager.writers["topic-b"] = w2

	cID1 := [4]byte{0, 0, 0, 1}
	cID2 := [4]byte{0, 0, 0, 2}
	frame1 := buildProduceFrame(cID1, "topic-a", []byte("for-a"))
	frame2 := buildProduceFrame(cID2, "topic-b", []byte("for-b"))

	err := th.feed(append(frame1, frame2...))
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced1 := w1.getProduced()
	produced2 := w2.getProduced()
	require.Len(t, produced1, 1)
	require.Len(t, produced2, 1)
	assert.Equal(t, []byte("for-a"), produced1[0].msg)
	assert.Equal(t, []byte("for-b"), produced2[0].msg)
}

// ---------------------------------------------------------------------------
// HProduce with many headers
// ---------------------------------------------------------------------------

func TestHandle_HProduce_MultipleHeaders(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("h-topic", w)

	cID := [4]byte{0, 0, 0, 1}
	// 6 strings = 3 key-value header pairs
	headers := []string{"k1", "v1", "k2", "v2", "k3", "v3"}
	frame := buildHProduceFrame(cID, "h-topic", headers, []byte("msg"))
	err := th.feed(frame)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	require.Len(t, produced, 1)
	require.Len(t, produced[0].headers, 6)
	for i, h := range headers {
		assert.Equal(t, h, string(produced[0].headers[i]))
	}
}

// ---------------------------------------------------------------------------
// Verify stream reads work with the test harness
// ---------------------------------------------------------------------------

func TestHarness_StreamCapture(t *testing.T) {
	// Basic sanity check that our test harness captures outbound data
	str := &mockStream{}
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	out := NewOutbound(str, 5*time.Second, l)
	done := make(chan struct{})
	go func() {
		out.WriteLoop()
		close(done)
	}()

	out.EnqueueProto([]byte("hello"))
	time.Sleep(50 * time.Millisecond)

	data := str.written()
	assert.Equal(t, []byte("hello"), data)

	out.Close()
	<-done
}

// ---------------------------------------------------------------------------
// Large batch: many PRODUCEs in one buffer
// ---------------------------------------------------------------------------

func TestHandle_Produce_LargeBatch(t *testing.T) {
	th := newProtocolTestHarness()
	done := th.startWriteLoop()
	w := &mockConnectorWriter{}
	th.setConnectedWithWriter("batch", w)

	const count = 100
	var combined []byte
	for i := 0; i < count; i++ {
		cID := [4]byte{0, 0, byte(i >> 8), byte(i & 0xFF)}
		combined = append(combined, buildProduceFrame(cID, "batch", []byte("batch-data"))...)
	}

	err := th.feed(combined)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	th.close(done)

	produced := w.getProduced()
	assert.Len(t, produced, count)
}

// ---------------------------------------------------------------------------
// Correlation ID preservation
// ---------------------------------------------------------------------------

func TestHandle_Produce_CorrelationIDPreservedInResponse(t *testing.T) {
	cIDs := [][4]byte{
		{0x00, 0x00, 0x00, 0x01},
		{0xDE, 0xAD, 0xBE, 0xEF},
		{0xFF, 0xFF, 0xFF, 0xFF},
	}

	for _, cID := range cIDs {
		t.Run(string(bytes.Join([][]byte{cID[:]}, nil)), func(t *testing.T) {
			th := newProtocolTestHarness()
			done := th.startWriteLoop()
			w := &mockConnectorWriter{}
			th.setConnectedWithWriter("topic", w)

			frame := buildProduceFrame(cID, "topic", []byte("x"))
			err := th.feed(frame)
			require.NoError(t, err)

			resp := th.readResponse(50 * time.Millisecond)
			th.close(done)

			require.GreaterOrEqual(t, len(resp), 6)
			assert.Equal(t, cID[:], resp[1:5], "correlation ID should be preserved in response")
		})
	}
}

func BenchmarkHandleHelloAllocations(b *testing.B) {
	harness := newUnnegotiatedProtocolTestHarness()
	frame := buildHelloFrame("fujin-go", "v-client", v1.Version)
	// Warm protocol pools and outbound vector capacity before measuring.
	if err := harness.feed(frame); err != nil {
		b.Fatal(err)
	}
	resetHelloBenchmarkHarness(harness)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := harness.feed(frame); err != nil {
			b.Fatal(err)
		}
		resetHelloBenchmarkHarness(harness)
	}
}

func resetHelloBenchmarkHarness(harness *testHarness) {
	harness.out.Lock()
	for _, buffer := range harness.out.v {
		pool.Put(buffer)
	}
	harness.out.v = harness.out.v[:0]
	harness.out.pb = 0
	harness.out.Unlock()
	*harness.h.ps = parseState{state: OP_EXPECT_HELLO}
}
