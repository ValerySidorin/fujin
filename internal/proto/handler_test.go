package proto

import (
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler() *Handler {
	ctx := context.Background()
	h := &Handler{
		ctx:          ctx,
		ps:           &parseState{},
		pingInterval: 2 * time.Second,
		pingTimeout:  5 * time.Second,
		closed:       make(chan struct{}),
		disconnect:   func() {},
		l:            slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	return h
}

func TestHandler_ParseWriteRouteLenArg_Valid(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 10)

	err := h.parseWriteRouteLenArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(10), h.ps.pa.routeLen)
}

func TestHandler_ParseWriteRouteLenArg_Zero(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 0)

	err := h.parseWriteRouteLenArg()

	assert.ErrorIs(t, err, ErrWriteRouteLenArgEmpty)
}

func TestHandler_ParseWriteRouteLenArg_MaxValue(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 4294967295) // max uint32

	err := h.parseWriteRouteLenArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(4294967295), h.ps.pa.routeLen)
}

func TestHandler_ParseWriteRouteArg_Valid(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte("test-topic")

	err := h.parseWriteRouteArg()

	assert.NoError(t, err)
	assert.Equal(t, "test-topic", h.ps.pa.route)
}

func TestHandler_ParseWriteRouteArg_Empty(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte{}

	err := h.parseWriteRouteArg()

	assert.ErrorIs(t, err, ErrWriteRouteArgEmpty)
}

func TestHandler_ParseWriteRouteArg_WithSpecialCharacters(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte("test.topic-123_abc")

	err := h.parseWriteRouteArg()

	assert.NoError(t, err)
	assert.Equal(t, "test.topic-123_abc", h.ps.pa.route)
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

func TestHandler_ParseSubscribeRouteLenArg_Valid(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 20)

	err := h.parseSubscribeRouteLenArg()

	assert.NoError(t, err)
	assert.Equal(t, uint32(20), h.ps.sa.routeLen)
}

func TestHandler_ParseSubscribeRouteLenArg_Zero(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = make([]byte, 4)
	binary.BigEndian.PutUint32(h.ps.argBuf, 0)

	err := h.parseSubscribeRouteLenArg()

	assert.ErrorIs(t, err, ErrRouteSizeArgNotProvided)
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
	h.ps.pa.routeLen = 10
	h.ps.pa.route = "test-topic"

	assert.Equal(t, uint32(10), h.ps.pa.routeLen)
	assert.Equal(t, "test-topic", h.ps.pa.route)
}

func TestHandler_ProduceMsgArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.pma.size = 1024

	assert.Equal(t, uint32(1024), h.ps.pma.size)
}

func TestHandler_SubscribeArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.sa.routeLen = 15
	h.ps.sa.route = "subscribe-topic"
	h.ps.sa.autoCommit = true
	h.ps.sa.headered = false

	assert.Equal(t, uint32(15), h.ps.sa.routeLen)
	assert.Equal(t, "subscribe-topic", h.ps.sa.route)
	assert.True(t, h.ps.sa.autoCommit)
	assert.False(t, h.ps.sa.headered)
}

func TestHandler_InitArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.ba.configOverrides = make(map[string]string)
	h.ps.ba.configOverrides["writer.pub.transactional_id"] = "my-tx-id"
	h.ps.ba.configOverrides["reader.sub.group"] = "my-group"
	h.ps.ba.overridesCount = 2
	h.ps.ba.overridesRead = 2

	assert.Equal(t, uint16(2), h.ps.ba.overridesCount)
	assert.Equal(t, uint16(2), h.ps.ba.overridesRead)
	assert.Equal(t, "my-tx-id", h.ps.ba.configOverrides["writer.pub.transactional_id"])
	assert.Equal(t, "my-group", h.ps.ba.configOverrides["reader.sub.group"])
}

func TestHandler_FetchArgs(t *testing.T) {
	h := newTestHandler()
	h.ps.fa.autoCommit = true
	h.ps.fa.routeLen = 12
	h.ps.fa.route = "fetch-topic"
	h.ps.fa.headered = true

	assert.True(t, h.ps.fa.autoCommit)
	assert.Equal(t, uint32(12), h.ps.fa.routeLen)
	assert.Equal(t, "fetch-topic", h.ps.fa.route)
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

func TestHandler_MultipleParseCalls(t *testing.T) {
	h := newTestHandler()

	// Parse topic length multiple times
	for i := uint32(1); i <= 5; i++ {
		h.ps.argBuf = make([]byte, 4)
		binary.BigEndian.PutUint32(h.ps.argBuf, i*10)
		err := h.parseWriteRouteLenArg()
		require.NoError(t, err)
		assert.Equal(t, i*10, h.ps.pa.routeLen)
	}
}

func TestHandler_ParseTopicWithUnicode(t *testing.T) {
	h := newTestHandler()
	h.ps.argBuf = []byte("test-topic-中文-🚀")

	err := h.parseWriteRouteArg()

	assert.NoError(t, err)
	assert.Equal(t, "test-topic-中文-🚀", h.ps.pa.route)
}

func TestHandler_ErrorConstants(t *testing.T) {
	assert.Error(t, ErrClose)
	assert.Error(t, ErrParseProto)
	assert.Error(t, ErrFetchArgNotProvided)
	assert.Error(t, ErrInvalidReaderType)
	assert.Error(t, ErrRouteSizeArgNotProvided)
	assert.Error(t, ErrWriteRouteArgEmpty)
	assert.Error(t, ErrWriteMsgSizeArgEmpty)
	assert.Error(t, ErrWriteRouteLenArgEmpty)
	assert.Error(t, ErrConnectReaderIsAutoCommitArgInvalid)
}

func TestHandler_OpCodeConstants(t *testing.T) {
	// Test some operation code constants exist
	assert.Equal(t, 0, OP_START)
	assert.GreaterOrEqual(t, OP_BIND, 0)
	assert.GreaterOrEqual(t, OP_PRODUCE, 0)
	assert.GreaterOrEqual(t, OP_SUBSCRIBE, 0)
	assert.GreaterOrEqual(t, OP_FETCH, 0)
	assert.GreaterOrEqual(t, OP_ACK, 0)
	assert.GreaterOrEqual(t, OP_NACK, 0)
}
