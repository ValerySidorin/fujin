package server

import (
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler() *handler {
	ctx := context.Background()
	h := &handler{
		ctx:          ctx,
		ps:           &parseState{},
		sessionState: STREAM_STATE_INIT,
		pingInterval: 2 * time.Second,
		pingTimeout:  5 * time.Second,
		closed:       make(chan struct{}),
		disconnect:   func() {},
		l:            slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	return h
}

func TestHandler_ParseWriteTopicLenArg_Valid(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 10)

	err := h.parseWriteTopicLenArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(10), h.ps.pa.topicLen)
}

func TestHandler_ParseWriteTopicLenArg_Zero(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 0)

	err := h.parseWriteTopicLenArg()

	assert.ErrorIs(t, err, ErrWriteTopicLenArgEmpty)
}

func TestHandler_ParseWriteTopicLenArg_MaxValue(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 4294967295) // max uint32

	err := h.parseWriteTopicLenArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(4294967295), h.ps.pa.topicLen)
}

func TestHandler_ParseWriteTopicArg_Valid(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte("test-topic")

	err := h.parseWriteTopicArg()

	assert.NoError(t, err)
	assert.Equal(t, "test-topic", h.ps.pa.topic)
}

func TestHandler_ParseWriteTopicArg_Empty(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte("")

	err := h.parseWriteTopicArg()

	assert.ErrorIs(t, err, ErrWriteTopicArgEmpty)
}

func TestHandler_ParseWriteTopicArg_WithSpecialCharacters(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte("test.topic-123_abc")

	err := h.parseWriteTopicArg()

	assert.NoError(t, err)
	assert.Equal(t, "test.topic-123_abc", h.ps.pa.topic)
}

func TestHandler_ParseWriteMsgSizeArg_Valid(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 1024)

	err := h.parseWriteMsgSizeArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(1024), h.ps.pma.size)
}

func TestHandler_ParseWriteMsgSizeArg_Zero(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 0)

	err := h.parseWriteMsgSizeArg()

	assert.ErrorIs(t, err, ErrWriteMsgSizeArgEmpty)
}

func TestHandler_ParseWriteMsgSizeArg_LargeValue(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 10485760) // 10 MB

	err := h.parseWriteMsgSizeArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(10485760), h.ps.pma.size)
}

func TestHandler_ParseSubscribeTopicLenArg_Valid(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 20)

	err := h.parseSubscribeTopicLenArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(20), h.ps.sa.topicLen)
}

func TestHandler_ParseSubscribeTopicLenArg_Zero(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 0)

	err := h.parseSubscribeTopicLenArg()

	assert.ErrorIs(t, err, ErrReaderNameSizeArgNotProvided)
}

func TestHandler_Close(t *testing.T) {
	h := newTestHandler()

	disconnectCalled := false
	h.disconnect = func() {
		disconnectCalled = true
	}

	assert.False(t, h.stopRead)

	h.close()

	assert.True(t, h.stopRead)
	assert.True(t, disconnectCalled)

	// Verify closed channel is closed
	select {
	case <-h.closed:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("closed channel should be closed")
	}
}

func TestHandler_FlushWriters_NoWriters(t *testing.T) {
	h := newTestHandler()
	h.nonTxSessionWriters = make(map[string]writer.Writer)

	err := h.flushWriters()

	assert.NoError(t, err)
}

func TestHandler_SessionStates(t *testing.T) {
	tests := []struct {
		name  string
		state sessionState
	}{
		{"init state", STREAM_STATE_INIT},
		{"connected state", STREAM_STATE_CONNECTED},
		{"connected in tx state", STREAM_STATE_CONNECTED_IN_TX},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler()
			h.sessionState = tt.state
			assert.Equal(t, tt.state, h.sessionState)
		})
	}
}

func TestHandler_ParseState(t *testing.T) {
	h := newTestHandler()

	assert.NotNil(t, h.ps)
	assert.Equal(t, OP_START, h.ps.state)
	assert.Nil(t, h.ps.argBuf)
	assert.Nil(t, h.ps.payloadBuf)
	assert.Nil(t, h.ps.payloadsBuf)
}

func TestHandler_CorrelationIDArg(t *testing.T) {
	h := newTestHandler()
	testCID := []byte{1, 2, 3, 4}
	h.ps.ca.cID = testCID

	assert.Equal(t, testCID, h.ps.ca.cID)
}

func TestHandler_ProduceArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.pa.topicLen = 10
	h.ps.pa.topic = "test-topic"

	assert.Equal(t, uint32(10), h.ps.pa.topicLen)
	assert.Equal(t, "test-topic", h.ps.pa.topic)
}

func TestHandler_ProduceMsgArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.pma.size = 1024

	assert.Equal(t, uint32(1024), h.ps.pma.size)
}

func TestHandler_SubscribeArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.sa.topicLen = 15
	h.ps.sa.topic = "subscribe-topic"
	h.ps.sa.autoCommit = true
	h.ps.sa.headered = false

	assert.Equal(t, uint32(15), h.ps.sa.topicLen)
	assert.Equal(t, "subscribe-topic", h.ps.sa.topic)
	assert.True(t, h.ps.sa.autoCommit)
	assert.False(t, h.ps.sa.headered)
}

func TestHandler_ConnectArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.cna.streamIDLen = 8
	h.ps.cna.streamID = "stream-1"

	assert.Equal(t, uint32(8), h.ps.cna.streamIDLen)
	assert.Equal(t, "stream-1", h.ps.cna.streamID)
}

func TestHandler_FetchArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.fa.autoCommit = true
	h.ps.fa.topicLen = 12
	h.ps.fa.topic = "fetch-topic"
	h.ps.fa.headered = true

	assert.True(t, h.ps.fa.autoCommit)
	assert.Equal(t, uint32(12), h.ps.fa.topicLen)
	assert.Equal(t, "fetch-topic", h.ps.fa.topic)
	assert.True(t, h.ps.fa.headered)
}

func TestHandler_AckArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.aa.currMsgIDLen = 16
	h.ps.aa.msgIDsLen = 5
	h.ps.aa.msgIDsBuf = []byte{1, 2, 3, 4, 5}

	assert.Equal(t, uint32(16), h.ps.aa.currMsgIDLen)
	assert.Equal(t, uint32(5), h.ps.aa.msgIDsLen)
	assert.Equal(t, []byte{1, 2, 3, 4, 5}, h.ps.aa.msgIDsBuf)
}

func TestHandler_HeaderArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.ha.count = 3
	h.ps.ha.currStrLn = 100
	h.ps.ha.read = 2
	h.ps.ha.headersKV = [][]byte{
		[]byte("key1:value1"),
		[]byte("key2:value2"),
	}

	assert.Equal(t, uint16(3), h.ps.ha.count)
	assert.Equal(t, uint32(100), h.ps.ha.currStrLn)
	assert.Equal(t, uint16(2), h.ps.ha.read)
	assert.Len(t, h.ps.ha.headersKV, 2)
}

func TestHandler_Subscribers(t *testing.T) {
	h := newTestHandler()
	h.subscribers = make(map[byte]reader.Reader)
	h.unsubFuncs = make(map[byte]func())

	assert.NotNil(t, h.subscribers)
	assert.Empty(t, h.subscribers)
	assert.NotNil(t, h.unsubFuncs)
	assert.Empty(t, h.unsubFuncs)
}

func TestHandler_FetchMsgHandlers(t *testing.T) {
	h := newTestHandler()
	h.fetchMsgHandlers = make(map[string]map[bool]func(message []byte, topic string, args ...any))
	h.fetchMsgWithHeadersHandlers = make(map[string]map[bool]func(message []byte, topic string, hs [][]byte, args ...any))

	assert.NotNil(t, h.fetchMsgHandlers)
	assert.Empty(t, h.fetchMsgHandlers)
	assert.NotNil(t, h.fetchMsgWithHeadersHandlers)
	assert.Empty(t, h.fetchMsgWithHeadersHandlers)
}

func TestHandler_MultipleParseCalls(t *testing.T) {
	h := newTestHandler()

	// Parse topic length multiple times
	for i := uint32(1); i <= 5; i++ {
		h.ps.argBuf = make([]byte, 4)
		binary.BigEndian.PutUint32(h.ps.argBuf, i*10)
		err := h.parseWriteTopicLenArg()
		require.NoError(t, err)
		assert.Equal(t, i*10, h.ps.pa.topicLen)
	}
}

func TestHandler_ParseTopicWithUnicode(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte("test-topic-中文-🚀")

	err := h.parseWriteTopicArg()

	assert.NoError(t, err)
	assert.Equal(t, "test-topic-中文-🚀", h.ps.pa.topic)
}

func TestHandler_ErrorConstants(t *testing.T) {
	assert.Error(t, ErrClose)
	assert.Error(t, ErrParseProto)
	assert.Error(t, ErrWriterCanNotBeReusedInTx)
	assert.Error(t, ErrFetchArgNotProvided)
	assert.Error(t, ErrInvalidReaderType)
	assert.Error(t, ErrReaderNameSizeArgNotProvided)
	assert.Error(t, ErrWriteTopicArgEmpty)
	assert.Error(t, ErrWriteMsgSizeArgEmpty)
	assert.Error(t, ErrWriteTopicLenArgEmpty)
	assert.Error(t, ErrConnectReaderIsAutoCommitArgInvalid)
	assert.Error(t, ErrInvalidTxState)
}

func TestHandler_StateConstants(t *testing.T) {
	assert.Equal(t, sessionState(0), STREAM_STATE_INIT)
	assert.Equal(t, sessionState(1), STREAM_STATE_CONNECTED)
	assert.Equal(t, sessionState(2), STREAM_STATE_CONNECTED_IN_TX)
}

func TestHandler_OpCodeConstants(t *testing.T) {
	// Test some operation code constants exist
	assert.Equal(t, 0, OP_START)
	assert.GreaterOrEqual(t, OP_CONNECT, 0)
	assert.GreaterOrEqual(t, OP_PRODUCE, 0)
	assert.GreaterOrEqual(t, OP_SUBSCRIBE, 0)
	assert.GreaterOrEqual(t, OP_FETCH, 0)
	assert.GreaterOrEqual(t, OP_ACK, 0)
	assert.GreaterOrEqual(t, OP_NACK, 0)
}
