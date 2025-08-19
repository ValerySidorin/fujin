package fujin

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unsafe"

	pool2 "github.com/ValerySidorin/fujin/internal/common/pool"
	"github.com/ValerySidorin/fujin/internal/connectors"
	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response/server"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	internal_reader "github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
	"github.com/quic-go/quic-go"
)

const (
	OP_START int = iota

	OP_CONNECT
	OP_CONNECT_STREAM_ID_ARG

	OP_PRODUCE
	OP_PRODUCE_CORRELATION_ID_ARG
	OP_PRODUCE_ARG
	OP_PRODUCE_MSG_ARG
	OP_PRODUCE_MSG_PAYLOAD

	OP_PRODUCE_H
	OP_PRODUCE_H_CORRELATION_ID_ARG
	OP_PRODUCE_H_ARG
	OP_PRODUCE_H_HEADERS_COUNT_ARG
	OP_PRODUCE_H_HEADER_STR_LEN_ARG
	OP_PRODUCE_H_HEADER_STR_PAYLOAD
	OP_PRODUCE_H_MSG_ARG
	OP_PRODUCE_H_MSG_PAYLOAD

	OP_SUBSCRIBE
	OP_SUBSCRIBE_CORRELATION_ID_ARG
	OP_SUBSCRIBE_AUTO_COMMIT_ARG
	OP_SUBSCRIBE_TOPIC_ARG
	OP_SUBSCRIBE_TOPIC_PAYLOAD

	OP_UNSUBSCRIBE
	OP_UNSUBSCRIBE_CORRELATION_ID_ARG
	OP_UNSUBSCRIBE_SUB_ID_ARG

	OP_FETCH
	OP_FETCH_CORRELATION_ID_ARG
	OP_FETCH_AUTO_COMMIT_ARG
	OP_FETCH_TOPIC_ARG
	OP_FETCH_TOPIC_PAYLOAD
	OP_FETCH_N_ARG

	OP_ACK
	OP_ACK_CORRELATION_ID_ARG
	OP_ACK_ARG
	OP_ACK_MSG_ID_TOPIC_ARG
	OP_ACK_MSG_ID_TOPIC_PAYLOAD
	OP_ACK_MSG_ID_ARG
	OP_ACK_MSG_ID_PAYLOAD

	OP_NACK
	OP_NACK_CORRELATION_ID_ARG
	OP_NACK_ARG
	OP_NACK_MSG_ID_TOPIC_ARG
	OP_NACK_MSG_ID_TOPIC_PAYLOAD
	OP_NACK_MSG_ID_ARG
	OP_NACK_MSG_ID_PAYLOAD

	OP_BEGIN_TX
	OP_BEGIN_TX_CORRELATION_ID_ARG

	OP_BEGIN_TX_FAIL
	OP_BEGIN_TX_FAIL_CORRELATION_ID_ARG

	OP_COMMIT_TX
	OP_COMMIT_TX_CORRELATION_ID_ARG

	OP_COMMIT_TX_FAIL
	OP_COMMIT_TX_FAIL_CORRELATION_ID_ARG

	OP_ROLLBACK_TX
	OP_ROLLBACK_TX_CORRELATION_ID_ARG

	OP_ROLLBACK_TX_FAIL
	OP_ROLLBACK_TX_FAIL_CORRELATION_ID_ARG

	OP_PRODUCE_TX
	OP_PRODUCE_TX_CORRELATION_ID_ARG
	OP_PRODUCE_TX_ARG
	OP_PRODUCE_TX_MSG_ARG
	OP_PRODUCE_TX_MSG_PAYLOAD

	OP_PRODUCE_H_TX
	OP_PRODUCE_H_TX_CORRELATION_ID_ARG
	OP_PRODUCE_H_TX_ARG
	OP_PRODUCE_H_TX_HEADERS_COUNT_ARG
	OP_PRODUCE_H_TX_HEADER_STR_LEN_ARG
	OP_PRODUCE_H_TX_HEADER_STR_PAYLOAD
	OP_PRODUCE_H_TX_MSG_ARG
	OP_PRODUCE_H_TX_MSG_PAYLOAD
)

var (
	ErrClose                        = errors.New("close")
	ErrParseProto                   = errors.New("parse proto")
	ErrWriterCanNotBeReusedInTx     = errors.New("writer can not be reuse in tx")
	ErrFetchArgNotProvided          = errors.New("fetch arg not provided")
	ErrInvalidReaderType            = errors.New("invalid reader type")
	ErrReaderNameSizeArgNotProvided = errors.New("reader name size arg not provided")

	ErrWriteTopicArgEmpty    = errors.New("write topic arg is empty")
	ErrWriteMsgSizeArgEmpty  = errors.New("write size arg not provided")
	ErrWriteTopicLenArgEmpty = errors.New("writer topic len arg not provided")

	ErrConnectReaderIsAutoCommitArgInvalid = errors.New("connect reader is auto commit arg invalid")

	ErrInvalidTxState = errors.New("invalid tx state")
)

var (
// removed templates; build response buffers per call to avoid races
)

type sessionState byte

const (
	STREAM_STATE_INIT sessionState = iota
	STREAM_STATE_CONNECTED
	STREAM_STATE_CONNECTED_IN_TX
)

type parseState struct {
	state       int
	argBuf      []byte
	payloadBuf  []byte
	payloadsBuf [][]byte

	ca correlationIDArg

	cna connectArgs
	pa  produceArgs
	pma produceMsgArgs

	sa subscribeArgs
	aa ackArgs
	fa fetchArgs

	// headered produce args
	ha headerArgs
}

type correlationIDArg struct {
	cID []byte
}

type produceArgs struct {
	topicLen uint32
	topic    string
}

type produceMsgArgs struct {
	size uint32
}

type subscribeArgs struct {
	topicLen   uint32
	topic      string
	autoCommit bool
	headered   bool
}

type connectArgs struct {
	streamIDLen uint32
	streamID    string
}

type fetchArgs struct {
	autoCommit bool
	topicLen   uint32
	topic      string
	headered   bool
}

type ackArgs struct {
	currMsgIDLen uint32
	msgIDsLen    uint32
	msgIDsBuf    []byte
}

type headerArgs struct {
	count     uint16
	currStrLn uint32
	read      uint16
	headersKV [][]byte
}

type handler struct {
	ctx  context.Context
	out  *fujin.Outbound
	str  *quic.Stream
	cman *connectors.Manager

	ps           *parseState
	sessionState sessionState

	// ping
	pingInterval time.Duration
	pingTimeout  time.Duration
	pingStream   bool

	// producer
	writerID             string
	nonTxSessionWriters  map[string]writer.Writer
	currentTxWriter      writer.Writer
	currentTxWriterTopic string

	// subscriber
	subIDPool   *pool2.BytePool
	subscribers map[byte]internal_reader.Reader
	unsubFuncs  map[byte]func()
	sMu         sync.Mutex

	cpool                       *connectors.Pool
	fetchMsgHandlers            map[string]map[bool]func(message []byte, topic string, args ...any)
	fetchMsgWithHeadersHandlers map[string]map[bool]func(message []byte, topic string, hs [][]byte, args ...any)
	fhMu                        sync.RWMutex

	acker internal_reader.Reader

	// response templates removed to prevent concurrent in-place mutations

	disconnect func()

	wg       sync.WaitGroup
	stopRead bool
	closed   chan struct{}

	l *slog.Logger
}

func newHandler(
	ctx context.Context,
	pingInterval time.Duration, pingTimeout time.Duration, pingStream bool,
	cman *connectors.Manager,
	out *fujin.Outbound, str *quic.Stream, l *slog.Logger,
) *handler {
	h := &handler{
		ctx:                         ctx,
		cman:                        cman,
		pingInterval:                pingInterval,
		pingTimeout:                 pingTimeout,
		subIDPool:                   pool2.NewBytePool(),
		subscribers:                 make(map[byte]internal_reader.Reader),
		unsubFuncs:                  make(map[byte]func()),
		fetchMsgHandlers:            make(map[string]map[bool]func(message []byte, topic string, args ...any)),
		fetchMsgWithHeadersHandlers: make(map[string]map[bool]func(message []byte, topic string, hs [][]byte, args ...any)),
		cpool:                       connectors.NewPool(cman),
		l:                           l,
		out:                         out,
		str:                         str,
		ps:                          &parseState{},
		disconnect:                  func() {},
		sessionState:                STREAM_STATE_INIT,
		closed:                      make(chan struct{}),
	}

	if pingStream {
		_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
		go h.writePing()
	}

	return h
}

func (h *handler) handle(buf []byte) error {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]
		switch h.ps.state {
		case OP_START:
			switch h.sessionState {
			case STREAM_STATE_INIT:
				switch b {
				case byte(request.OP_CODE_CONNECT):
					h.sessionState = STREAM_STATE_CONNECTED
					h.nonTxSessionWriters = make(map[string]writer.Writer)
					h.disconnect = func() {
						for pub, w := range h.nonTxSessionWriters {
							w.Flush(h.ctx)
							h.cman.PutWriter(w, pub, "")
						}
						if h.currentTxWriter != nil {
							_ = h.currentTxWriter.RollbackTx(h.ctx)
							h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterTopic, h.writerID)
							h.currentTxWriter = nil
						}
						h.cpool.Close()
						h.sMu.Lock()
						for _, unsub := range h.unsubFuncs {
							unsub()
						}
						h.sMu.Unlock()
						h.out.EnqueueProto(response.DISCONNECT_RESP)
					}
					h.ps.state = OP_CONNECT
				case byte(request.OP_CODE_SUBSCRIBE):
					h.ps.state = OP_SUBSCRIBE
				case byte(response.RESP_CODE_PONG):
					if h.pingStream {
						_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
					}
				default:
					return ErrParseProto
				}
			case STREAM_STATE_CONNECTED:
				switch b {
				// writer cmds
				case byte(request.OP_CODE_PRODUCE):
					h.ps.state = OP_PRODUCE
				case byte(request.OP_CODE_HPRODUCE):
					h.ps.state = OP_PRODUCE_H
				case byte(request.OP_CODE_TX_BEGIN):
					h.ps.state = OP_BEGIN_TX
					h.sessionState = STREAM_STATE_CONNECTED_IN_TX
				case byte(request.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX_FAIL
				case byte(request.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX_FAIL
				// reader cmds
				case byte(request.OP_CODE_FETCH):
					h.ps.state = OP_FETCH
				case byte(request.OP_CODE_HFETCH):
					h.ps.fa.headered = true
					h.ps.state = OP_FETCH
				case byte(request.OP_CODE_HSUBSCRIBE):
					h.ps.sa.headered = true
					h.ps.state = OP_SUBSCRIBE
				case byte(request.OP_CODE_ACK):
					h.ps.state = OP_ACK
				case byte(request.OP_CODE_NACK):
					h.ps.state = OP_NACK
				case byte(request.OP_CODE_SUBSCRIBE):
					h.ps.state = OP_SUBSCRIBE
				case byte(request.OP_CODE_UNSUBSCRIBE):
					h.ps.state = OP_UNSUBSCRIBE
				// common cmds
				case byte(request.OP_CODE_DISCONNECT):
					return ErrClose
				case byte(response.RESP_CODE_PONG):
					if h.pingStream {
						_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
					}
				default:
					return ErrParseProto
				}
			case STREAM_STATE_CONNECTED_IN_TX:
				switch b {
				case byte(request.OP_CODE_PRODUCE):
					h.ps.state = OP_PRODUCE_TX
				case byte(request.OP_CODE_TX_BEGIN):
					h.ps.state = OP_BEGIN_TX_FAIL
					h.sessionState = STREAM_STATE_CONNECTED_IN_TX
				case byte(request.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX
				case byte(request.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX
					// reader cmds
				case byte(request.OP_CODE_FETCH):
					h.ps.state = OP_FETCH
				case byte(request.OP_CODE_ACK):
					h.ps.state = OP_ACK
				case byte(request.OP_CODE_NACK):
					h.ps.state = OP_NACK
				case byte(request.OP_CODE_SUBSCRIBE):
					h.ps.state = OP_SUBSCRIBE
				case byte(request.OP_CODE_UNSUBSCRIBE):
					h.ps.state = OP_UNSUBSCRIBE
				// common cmds
				case byte(request.OP_CODE_DISCONNECT):
					return ErrClose
				case byte(response.RESP_CODE_PONG):
					if h.pingStream {
						_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
					}
				default:
					return ErrParseProto
				}
			}
		case OP_PRODUCE:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_CORRELATION_ID_ARG
		case OP_PRODUCE_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}

			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_PRODUCE_ARG
			}
		case OP_PRODUCE_ARG:
			if h.ps.pa.topicLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.topicLen))
				}

				toCopy := int(h.ps.pa.topicLen) - len(h.ps.argBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.argBuf)
					h.ps.argBuf = h.ps.argBuf[:start+toCopy]
					copy(h.ps.argBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.argBuf = append(h.ps.argBuf, b)
				}

				if len(h.ps.argBuf) >= int(h.ps.pa.topicLen) {
					if err := h.parseWriteTopicArg(); err != nil {
						h.l.Error("parse write topic arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.pa.topicLen == 0 {
					if err := h.parseWriteTopicLenArg(); err != nil {
						h.l.Error("parse write topic len arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}
				h.enqueueWriteErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				continue
			}
		case OP_PRODUCE_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseWriteMsgSizeArg(); err != nil {
					h.l.Error("parse write msg size arg", "err", err)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.ca.cID)
					pool.Put(h.ps.argBuf)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_MSG_PAYLOAD
			}
		case OP_PRODUCE_MSG_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.pma.size) - len(h.ps.payloadBuf)
				avail := len(buf) - i
				if avail < toCopy {
					toCopy = avail
				}
				if toCopy > 0 {
					start := len(h.ps.payloadBuf)
					h.ps.payloadBuf = h.ps.payloadBuf[:start+toCopy]
					copy(h.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				}
				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if _, ok := h.nonTxSessionWriters[h.ps.pa.topic]; !ok {
						w, err := h.cman.GetWriter(h.ps.pa.topic, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						h.nonTxSessionWriters[h.ps.pa.topic] = w
					}

					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if _, ok := h.nonTxSessionWriters[h.ps.pa.topic]; !ok {
						w, err := h.cman.GetWriter(h.ps.pa.topic, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}
						h.nonTxSessionWriters[h.ps.pa.topic] = w
					}
					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			}
		case OP_PRODUCE_H:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_H_CORRELATION_ID_ARG
		case OP_PRODUCE_H_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i
			if avail < toCopy {
				toCopy = avail
			}
			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_PRODUCE_H_ARG
			}
		case OP_PRODUCE_H_ARG:
			if h.ps.pa.topicLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.topicLen))
				}
				toCopy := int(h.ps.pa.topicLen) - len(h.ps.argBuf)
				avail := len(buf) - i
				if avail < toCopy {
					toCopy = avail
				}
				if toCopy > 0 {
					start := len(h.ps.argBuf)
					h.ps.argBuf = h.ps.argBuf[:start+toCopy]
					copy(h.ps.argBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.argBuf = append(h.ps.argBuf, b)
				}
				if len(h.ps.argBuf) >= int(h.ps.pa.topicLen) {
					if err := h.parseWriteTopicArg(); err != nil {
						h.l.Error("parse write topic arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					// init header args
					h.ps.ha = headerArgs{}
					h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_H_HEADERS_COUNT_ARG
				}
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.pa.topicLen == 0 {
					if err := h.parseWriteTopicLenArg(); err != nil {
						h.l.Error("parse write topic len arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}
				h.enqueueWriteErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				continue
			}
		case OP_PRODUCE_H_HEADERS_COUNT_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(2)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= 2 {
				h.ps.ha.count = binary.BigEndian.Uint16(h.ps.argBuf[:2])
				pool.Put(h.ps.argBuf)
				h.ps.ha.read, h.ps.ha.headersKV, h.ps.argBuf = 0, nil, nil
				if h.ps.ha.count == 0 {
					h.ps.state = OP_PRODUCE_H_MSG_ARG
					continue
				}
				h.ps.state = OP_PRODUCE_H_HEADER_STR_LEN_ARG
			}
		case OP_PRODUCE_H_HEADER_STR_LEN_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				h.ps.ha.currStrLn = binary.BigEndian.Uint32(h.ps.argBuf[:fujin.Uint32Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(int(h.ps.ha.currStrLn))
				h.ps.state = OP_PRODUCE_H_HEADER_STR_PAYLOAD
			}
		case OP_PRODUCE_H_HEADER_STR_PAYLOAD:
			toCopy := int(h.ps.ha.currStrLn) - len(h.ps.argBuf)
			avail := len(buf) - i
			if avail < toCopy {
				toCopy = avail
			}
			if toCopy > 0 {
				start := len(h.ps.argBuf)
				h.ps.argBuf = h.ps.argBuf[:start+toCopy]
				copy(h.ps.argBuf[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.argBuf = append(h.ps.argBuf, b)
			}
			if len(h.ps.argBuf) >= int(h.ps.ha.currStrLn) {
				// store as raw bytes slice in order (k1, v1, ...)
				// trim to exact length before storing
				b := make([]byte, len(h.ps.argBuf))
				copy(b, h.ps.argBuf)
				h.ps.ha.headersKV = append(h.ps.ha.headersKV, b)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				h.ps.ha.read++
				if h.ps.ha.read >= h.ps.ha.count { // count is total number of strings
					h.ps.state = OP_PRODUCE_H_MSG_ARG
					continue
				}
				h.ps.state = OP_PRODUCE_H_HEADER_STR_LEN_ARG
			}
		case OP_PRODUCE_H_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseWriteMsgSizeArg(); err != nil {
					h.l.Error("parse write msg size arg", "err", err)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.ca.cID)
					pool.Put(h.ps.argBuf)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_H_MSG_PAYLOAD
			}
		case OP_PRODUCE_H_MSG_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.pma.size) - len(h.ps.payloadBuf)
				avail := len(buf) - i
				if avail < toCopy {
					toCopy = avail
				}
				if toCopy > 0 {
					start := len(h.ps.payloadBuf)
					h.ps.payloadBuf = h.ps.payloadBuf[:start+toCopy]
					copy(h.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				}
				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if _, ok := h.nonTxSessionWriters[h.ps.pa.topic]; !ok {
						w, err := h.cman.GetWriter(h.ps.pa.topic, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						h.nonTxSessionWriters[h.ps.pa.topic] = w
					}

					h.hproduce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if _, ok := h.nonTxSessionWriters[h.ps.pa.topic]; !ok {
						w, err := h.cman.GetWriter(h.ps.pa.topic, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}
						h.nonTxSessionWriters[h.ps.pa.topic] = w
					}
					h.hproduce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			}
		case OP_FETCH:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_FETCH_CORRELATION_ID_ARG
		case OP_FETCH_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}

			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.state = OP_FETCH_AUTO_COMMIT_ARG
			}
		case OP_FETCH_AUTO_COMMIT_ARG:
			var err error
			h.ps.fa.autoCommit, err = parseBool(b)
			if err != nil {
				// TODO: Respond or abort stream?
				pool.Put(h.ps.ca.cID)
				return err
			}
			h.ps.state = OP_FETCH_TOPIC_ARG
			h.ps.argBuf = pool.Get(fujin.Uint32Len)
		case OP_FETCH_TOPIC_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				h.ps.fa.topicLen = binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(int(h.ps.fa.topicLen))
				h.ps.state = OP_FETCH_TOPIC_PAYLOAD
			}
		case OP_FETCH_TOPIC_PAYLOAD:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= int(h.ps.fa.topicLen) {
				h.ps.fa.topic = string(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_FETCH_N_ARG
			}
		case OP_FETCH_N_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				n := binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				h.fetch(h.ps.fa.topic, h.ps.fa.autoCommit, n)
				pool.Put(h.ps.ca.cID)
				h.ps.ca.cID, h.ps.state = nil, OP_START
			}
		case OP_ACK:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ACK_CORRELATION_ID_ARG
		case OP_ACK_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}

			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.aa.msgIDsBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_ACK_ARG
			}
		case OP_ACK_ARG:
			h.ps.aa.msgIDsBuf = append(h.ps.aa.msgIDsBuf, b)
			if len(h.ps.aa.msgIDsBuf) >= fujin.Uint32Len {
				h.ps.aa.msgIDsLen = binary.BigEndian.Uint32(h.ps.aa.msgIDsBuf)
				if h.ps.aa.msgIDsLen == 0 {
					h.enqueueAckSuccess(h.ps.ca.cID)
					pool.Put(h.ps.ca.cID)
					pool.Put(h.ps.aa.msgIDsBuf)
					h.ps.ca.cID, h.ps.aa, h.ps.state = nil, ackArgs{}, OP_START
					continue
				}
				h.ps.payloadsBuf = GetBufs()
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_ACK_MSG_ID_ARG
			}
		case OP_ACK_MSG_ID_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				h.ps.aa.currMsgIDLen = binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.payloadBuf = pool.Get(int(h.ps.aa.currMsgIDLen))
				h.ps.state = OP_ACK_MSG_ID_PAYLOAD
			}
		case OP_ACK_MSG_ID_PAYLOAD:
			h.ps.payloadBuf = append(h.ps.payloadBuf, b)
			if len(h.ps.payloadBuf) >= int(h.ps.aa.currMsgIDLen) {
				h.ps.payloadsBuf = append(h.ps.payloadsBuf, h.ps.payloadBuf)
				if len(h.ps.payloadsBuf) >= int(h.ps.aa.msgIDsLen) {
					h.acker.Ack(h.ctx, h.ps.payloadsBuf,
						func(err error) {
							h.out.Lock()
							if err != nil {
								h.enqueueAckErrNoLock(h.ps.ca.cID, err)
								return
							}
							h.enqueueAckSuccessNoLock(h.ps.ca.cID)
						},
						func(b []byte, err error) {
							if err != nil {
								h.enqueueAckMsgIDErrNoLock(b, err)
								return
							}
							h.enqueueAckMsgIDSuccessNoLock(b)
						},
					)
					h.out.Unlock()
					pool.Put(h.ps.ca.cID)
					pool.Put(h.ps.aa.msgIDsBuf)
					for _, payload := range h.ps.payloadsBuf {
						pool.Put(payload)
					}
					PutBufs(h.ps.payloadsBuf)
					h.ps.argBuf, h.ps.payloadBuf, h.ps.payloadsBuf, h.ps.ca.cID, h.ps.aa, h.ps.state = nil, nil, nil, nil, ackArgs{}, OP_START
					continue
				} else {
					h.ps.argBuf = pool.Get(fujin.Uint32Len)
					h.ps.state = OP_ACK_MSG_ID_ARG
				}
			}
		case OP_NACK:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_NACK_CORRELATION_ID_ARG
		case OP_NACK_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}

			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.aa.msgIDsBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_NACK_ARG
			}
		case OP_NACK_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				h.ps.aa.msgIDsLen = binary.BigEndian.Uint32(h.ps.argBuf)
				if h.ps.aa.msgIDsLen == 0 {
					pool.Put(h.ps.argBuf)
					h.enqueueNackSuccess(h.ps.ca.cID)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.aa, h.ps.state = nil, nil, ackArgs{}, OP_START
					continue
				}
				h.ps.payloadsBuf = GetBufs()
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_NACK_MSG_ID_ARG
			}
		case OP_NACK_MSG_ID_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				h.ps.aa.currMsgIDLen = binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.payloadBuf = pool.Get(int(h.ps.aa.currMsgIDLen))
				h.ps.state = OP_NACK_MSG_ID_PAYLOAD
			}
		case OP_NACK_MSG_ID_PAYLOAD:
			h.ps.payloadBuf = append(h.ps.payloadBuf, b)
			if len(h.ps.payloadBuf) >= int(h.ps.aa.currMsgIDLen) {
				h.ps.payloadsBuf = append(h.ps.payloadsBuf, h.ps.payloadBuf)
				if len(h.ps.payloadsBuf) >= int(h.ps.aa.msgIDsLen) {
					h.acker.Nack(h.ctx, h.ps.payloadsBuf,
						func(err error) {
							h.out.Lock()
							if err != nil {
								h.enqueueNackErrNoLock(h.ps.ca.cID, err)
								return
							}
							h.enqueueNackSuccessNoLock(h.ps.ca.cID)
						},
						func(b []byte, err error) {
							if err != nil {
								h.enqueueAckMsgIDErrNoLock(b, err)
								return
							}
							h.enqueueAckMsgIDSuccessNoLock(b)
						},
					)
					h.out.Unlock()
					pool.Put(h.ps.ca.cID)
					for _, payload := range h.ps.payloadsBuf {
						pool.Put(payload)
					}
					PutBufs(h.ps.payloadsBuf)
					h.ps.argBuf, h.ps.payloadBuf, h.ps.payloadsBuf, h.ps.ca.cID, h.ps.aa, h.ps.state = nil, nil, nil, nil, ackArgs{}, OP_START
					continue
				} else {
					h.ps.argBuf = pool.Get(fujin.Uint32Len)
					h.ps.state = OP_NACK_MSG_ID_ARG
				}
			}
		case OP_PRODUCE_TX:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_TX_CORRELATION_ID_ARG
		case OP_PRODUCE_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.ca.cID)
					h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
					copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.ca.cID = append(h.ps.ca.cID, b)
				}

				if len(h.ps.ca.cID) >= fujin.Uint32Len {
					h.ps.argBuf = pool.Get(fujin.Uint32Len)
					h.ps.state = OP_PRODUCE_TX_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_PRODUCE_TX_ARG:
			if h.ps.pa.topicLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.topicLen))
				}

				toCopy := int(h.ps.pa.topicLen) - len(h.ps.argBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.argBuf)
					h.ps.argBuf = h.ps.argBuf[:start+toCopy]
					copy(h.ps.argBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.argBuf = append(h.ps.argBuf, b)
				}

				if len(h.ps.argBuf) >= int(h.ps.pa.topicLen) {
					if err := h.parseWriteTopicArg(); err != nil {
						h.l.Error("parse write topic arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_TX_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.pa.topicLen == 0 {
					if err := h.parseWriteTopicLenArg(); err != nil {
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}

				// this should not happen ever
				panic("unreachable")
			}
		case OP_PRODUCE_TX_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseWriteMsgSizeArg(); err != nil {
					h.l.Error("parse write msg size arg", "err", err)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.ca.cID)
					pool.Put(h.ps.argBuf)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_TX_MSG_PAYLOAD
			}
		case OP_PRODUCE_TX_MSG_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.pma.size) - len(h.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.payloadBuf)
					h.ps.payloadBuf = h.ps.payloadBuf[:start+toCopy]
					copy(h.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				}

				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if h.currentTxWriter != nil {
						if !h.cman.WriterMatchEndpoint(h.currentTxWriter, h.ps.pa.topic) {
							h.l.Error("writer can not be reused in tx")
							h.enqueueWriteErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						currentTxWriter, err := h.cman.GetWriter(h.ps.pa.topic, h.writerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						if err := currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						h.currentTxWriter, h.currentTxWriterTopic = currentTxWriter, h.ps.pa.topic
					}

					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if h.currentTxWriter != nil {
						if !h.cman.WriterMatchEndpoint(h.currentTxWriter, h.ps.pa.topic) {
							h.l.Error("writer can not be reused in tx1")
							h.enqueueWriteErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						currentTxWriter, err := h.cman.GetWriter(h.ps.pa.topic, h.writerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						if err := currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						h.currentTxWriter, h.currentTxWriterTopic = currentTxWriter, h.ps.pa.topic
					}

					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			}
		case OP_PRODUCE_H_TX:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_H_TX_CORRELATION_ID_ARG
		case OP_PRODUCE_H_TX_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i
			if avail < toCopy {
				toCopy = avail
			}
			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.state = OP_PRODUCE_H_TX_ARG
			}
		case OP_PRODUCE_H_TX_ARG:
			if h.ps.pa.topicLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.topicLen))
				}
				toCopy := int(h.ps.pa.topicLen) - len(h.ps.argBuf)
				avail := len(buf) - i
				if avail < toCopy {
					toCopy = avail
				}
				if toCopy > 0 {
					start := len(h.ps.argBuf)
					h.ps.argBuf = h.ps.argBuf[:start+toCopy]
					copy(h.ps.argBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.argBuf = append(h.ps.argBuf, b)
				}
				if len(h.ps.argBuf) >= int(h.ps.pa.topicLen) {
					if err := h.parseWriteTopicArg(); err != nil {
						h.l.Error("parse write topic arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					// init header args
					h.ps.ha = headerArgs{}
					h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_H_TX_HEADERS_COUNT_ARG
				}
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.pa.topicLen == 0 {
					if err := h.parseWriteTopicLenArg(); err != nil {
						h.l.Error("parse write topic len arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						pool.Put(h.ps.argBuf)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}
				h.enqueueWriteErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				continue
			}
		case OP_PRODUCE_H_TX_HEADERS_COUNT_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(2)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= 2 {
				h.ps.ha.count = binary.BigEndian.Uint16(h.ps.argBuf[:2])
				pool.Put(h.ps.argBuf)
				h.ps.ha.read, h.ps.ha.headersKV, h.ps.argBuf = 0, nil, nil
				if h.ps.ha.count == 0 {
					h.ps.state = OP_PRODUCE_H_TX_MSG_ARG
					continue
				}
				h.ps.state = OP_PRODUCE_H_TX_HEADER_STR_LEN_ARG
			}
		case OP_PRODUCE_H_TX_HEADER_STR_LEN_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				h.ps.ha.currStrLn = binary.BigEndian.Uint32(h.ps.argBuf[:fujin.Uint32Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(int(h.ps.ha.currStrLn))
				h.ps.state = OP_PRODUCE_H_TX_HEADER_STR_PAYLOAD
			}
		case OP_PRODUCE_H_TX_HEADER_STR_PAYLOAD:
			toCopy := int(h.ps.ha.currStrLn) - len(h.ps.argBuf)
			avail := len(buf) - i
			if avail < toCopy {
				toCopy = avail
			}
			if toCopy > 0 {
				start := len(h.ps.argBuf)
				h.ps.argBuf = h.ps.argBuf[:start+toCopy]
				copy(h.ps.argBuf[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.argBuf = append(h.ps.argBuf, b)
			}
			if len(h.ps.argBuf) >= int(h.ps.ha.currStrLn) {
				// store as raw bytes slice in order (k1, v1, ...)
				// trim to exact length before storing
				b := make([]byte, len(h.ps.argBuf))
				copy(b, h.ps.argBuf)
				h.ps.ha.headersKV = append(h.ps.ha.headersKV, b)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				h.ps.ha.read++
				if h.ps.ha.read >= h.ps.ha.count { // count is total number of strings
					h.ps.state = OP_PRODUCE_H_TX_MSG_ARG
					continue
				}
				h.ps.state = OP_PRODUCE_H_TX_HEADER_STR_LEN_ARG
			}
		case OP_PRODUCE_H_TX_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseWriteMsgSizeArg(); err != nil {
					h.l.Error("parse write msg size arg", "err", err)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.ca.cID)
					pool.Put(h.ps.argBuf)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_H_TX_MSG_PAYLOAD
			}
		case OP_PRODUCE_H_TX_MSG_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.pma.size) - len(h.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.payloadBuf)
					h.ps.payloadBuf = h.ps.payloadBuf[:start+toCopy]
					copy(h.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				}

				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if h.currentTxWriter != nil {
						if !h.cman.WriterMatchEndpoint(h.currentTxWriter, h.ps.pa.topic) {
							h.l.Error("writer can not be reused in tx")
							h.enqueueWriteErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						currentTxWriter, err := h.cman.GetWriter(h.ps.pa.topic, h.writerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						if err := currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						h.currentTxWriter, h.currentTxWriterTopic = currentTxWriter, h.ps.pa.topic
					}

					h.hproduceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					if h.currentTxWriter != nil {
						if !h.cman.WriterMatchEndpoint(h.currentTxWriter, h.ps.pa.topic) {
							h.l.Error("writer can not be reused in tx1")
							h.enqueueWriteErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						currentTxWriter, err := h.cman.GetWriter(h.ps.pa.topic, h.writerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						if err := currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							pool.Put(h.ps.payloadBuf)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
							continue
						}

						h.currentTxWriter, h.currentTxWriterTopic = currentTxWriter, h.ps.pa.topic
					}

					h.hproduceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			}
		case OP_BEGIN_TX:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_BEGIN_TX_CORRELATION_ID_ARG
		case OP_BEGIN_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.ca.cID)
					h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
					copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.ca.cID = append(h.ps.ca.cID, b)
				}

				if len(h.ps.ca.cID) >= fujin.Uint32Len {
					if err := h.flushWriters(); err != nil {
						h.enqueueTxBeginErr(h.ps.ca.cID, err)
						h.l.Error("begin tx", "err", err)
						pool.Put(h.ps.ca.cID)
						h.ps.ca.cID, h.ps.state = nil, OP_START
						continue
					}

					h.enqueueTxBeginSuccess(h.ps.ca.cID)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, STREAM_STATE_CONNECTED_IN_TX, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_BEGIN_TX_FAIL:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_BEGIN_TX_FAIL_CORRELATION_ID_ARG
		case OP_BEGIN_TX_FAIL_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.ca.cID)
					h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
					copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.ca.cID = append(h.ps.ca.cID, b)
				}

				if len(h.ps.ca.cID) >= fujin.Uint32Len {
					if err := h.flushWriters(); err != nil {
						h.enqueueTxBeginErr(h.ps.ca.cID, err)
						h.l.Error("begin tx", "err", err)
						pool.Put(h.ps.ca.cID)
						h.ps.ca.cID, h.ps.state = nil, OP_START
						continue
					}

					h.enqueueTxBeginErr(h.ps.ca.cID, ErrInvalidTxState)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.state = nil, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_COMMIT_TX:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_COMMIT_TX_CORRELATION_ID_ARG
		case OP_COMMIT_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.ca.cID)
					h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
					copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.ca.cID = append(h.ps.ca.cID, b)
				}

				if len(h.ps.ca.cID) >= fujin.Uint32Len {
					if h.currentTxWriter != nil {
						if err := h.currentTxWriter.CommitTx(h.ctx); err != nil {
							h.enqueueTxCommitErr(h.ps.ca.cID, err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.state = nil, OP_START // We are keeping transaction opened here?
							continue
						}
						h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterTopic, h.writerID)
						h.currentTxWriter = nil
					}
					h.enqueueTxCommitSuccess(h.ps.ca.cID)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, STREAM_STATE_CONNECTED, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_COMMIT_TX_FAIL:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_COMMIT_TX_FAIL_CORRELATION_ID_ARG
		case OP_COMMIT_TX_FAIL_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.ca.cID)
					h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
					copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.ca.cID = append(h.ps.ca.cID, b)
				}

				if len(h.ps.ca.cID) >= fujin.Uint32Len {
					h.enqueueTxCommitErr(h.ps.ca.cID, ErrInvalidTxState)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.state = nil, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_ROLLBACK_TX:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ROLLBACK_TX_CORRELATION_ID_ARG
		case OP_ROLLBACK_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.ca.cID)
					h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
					copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.ca.cID = append(h.ps.ca.cID, b)
				}

				if len(h.ps.ca.cID) >= fujin.Uint32Len {
					if h.currentTxWriter != nil {
						if err := h.currentTxWriter.RollbackTx(h.ctx); err != nil {
							h.enqueueTxRollbackErr(h.ps.ca.cID, err)
							h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterTopic, h.writerID)
							pool.Put(h.ps.ca.cID)
							// We are not keeping tx opened here after rollback error
							h.ps.ca.cID, h.currentTxWriter, h.sessionState, h.ps.state = nil, nil, STREAM_STATE_CONNECTED, OP_START
							continue
						}
						h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterTopic, h.writerID)
						h.currentTxWriter = nil
					}
					h.enqueueTxRollbackSuccess(h.ps.ca.cID)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, STREAM_STATE_CONNECTED, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_ROLLBACK_TX_FAIL:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ROLLBACK_TX_FAIL_CORRELATION_ID_ARG
		case OP_ROLLBACK_TX_FAIL_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.ca.cID)
					h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
					copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.ca.cID = append(h.ps.ca.cID, b)
				}

				if len(h.ps.ca.cID) >= fujin.Uint32Len {
					h.enqueueTxRollbackErr(h.ps.ca.cID, ErrInvalidTxState)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.state = nil, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_CONNECT:
			h.ps.state = OP_CONNECT_STREAM_ID_ARG
			h.ps.argBuf = pool.Get(fujin.Uint32Len)
			h.ps.argBuf = append(h.ps.argBuf, b)
		case OP_CONNECT_STREAM_ID_ARG:
			if h.ps.cna.streamIDLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.cna.streamIDLen))
				}

				toCopy := int(h.ps.cna.streamIDLen) - len(h.ps.argBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(h.ps.argBuf)
					h.ps.argBuf = h.ps.argBuf[:start+toCopy]
					copy(h.ps.argBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					h.ps.argBuf = append(h.ps.argBuf, b)
				}

				if len(h.ps.argBuf) >= int(h.ps.cna.streamIDLen) {
					h.ps.cna.streamID = string(h.ps.argBuf)
					pool.Put(h.ps.argBuf)
					h.writerID = h.ps.cna.streamID
					h.ps.argBuf, h.ps.cna, h.ps.state = nil, connectArgs{}, OP_START
				}

				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.cna.streamIDLen == 0 {
					h.ps.cna.streamIDLen = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					if h.ps.cna.streamIDLen == 0 {
						h.ps.argBuf, h.ps.cna, h.ps.state = nil, connectArgs{}, OP_START
					}
				}
			}
		case OP_SUBSCRIBE:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_SUBSCRIBE_CORRELATION_ID_ARG
		case OP_SUBSCRIBE_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}

			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.state = OP_SUBSCRIBE_AUTO_COMMIT_ARG
			}
		case OP_SUBSCRIBE_AUTO_COMMIT_ARG:
			var err error
			h.ps.sa.autoCommit, err = parseBool(b)
			if err != nil {
				enqueueSubscribeErr(h.out, h.ps.ca.cID, response.RESP_CODE_SUBSCRIBE, response.ERR_CODE_YES, err)
				return err
			}

			h.ps.argBuf = pool.Get(fujin.Uint32Len)
			h.ps.state = OP_SUBSCRIBE_TOPIC_ARG
		case OP_SUBSCRIBE_TOPIC_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseSubscribeTopicLenArg(); err != nil {
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.sa, h.ps.state = nil, subscribeArgs{}, OP_START
					enqueueSubscribeErr(h.out, h.ps.ca.cID, response.RESP_CODE_SUBSCRIBE, response.ERR_CODE_YES, err)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.payloadBuf = pool.Get(int(h.ps.sa.topicLen))
				h.ps.argBuf, h.ps.state = nil, OP_SUBSCRIBE_TOPIC_PAYLOAD
			}
		case OP_SUBSCRIBE_TOPIC_PAYLOAD:
			toCopy := int(h.ps.sa.topicLen) - len(h.ps.payloadBuf)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(h.ps.payloadBuf)
				h.ps.payloadBuf = h.ps.payloadBuf[:start+toCopy]
				copy(h.ps.payloadBuf[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
			}

			if len(h.ps.payloadBuf) >= int(h.ps.sa.topicLen) {
				h.ps.state = OP_START
				h.ps.sa.topic = string(h.ps.payloadBuf)
				pool.Put(h.ps.payloadBuf)

				subID, err := h.subIDPool.Get()
				if err != nil {
					enqueueSubscribeErr(h.out, h.ps.ca.cID, response.RESP_CODE_SUBSCRIBE, response.ERR_CODE_YES, err)
					return fmt.Errorf("get sub id: %w", err)
				}
				r, err := h.cman.GetReader(h.ps.sa.topic, h.ps.sa.autoCommit)
				if err != nil {
					pErr := h.subIDPool.Put(subID)
					if pErr != nil {
						h.l.Error("put sub id", "err", pErr)
					}
					enqueueSubscribeErr(h.out, h.ps.ca.cID, response.RESP_CODE_SUBSCRIBE, response.ERR_CODE_YES, err)
					return fmt.Errorf("get reader: %w", err)
				}

				h.sMu.Lock()
				h.subscribers[subID] = r
				h.sMu.Unlock()

				code := response.RESP_CODE_SUBSCRIBE
				if h.ps.sa.headered {
					code = response.RESP_CODE_HSUBSCRIBE
				}
				enqueueSubscribeSuccess(h.out, code, h.ps.ca.cID, subID)
				pool.Put(h.ps.ca.cID)
				h.wg.Add(1)
				go func(headered bool) {
					ctx, cancel := context.WithCancel(h.ctx)
					unsub := sync.OnceFunc(func() {
						cancel()
						r.Close()
						err := h.subIDPool.Put(subID)
						if err != nil {
							h.l.Error("put sub id", "err", err)
						}
						delete(h.unsubFuncs, subID)
					})
					h.sMu.Lock()
					h.unsubFuncs[subID] = unsub
					h.sMu.Unlock()
					if headered {
						h.hsubscribe(ctx, subID, r)
					} else {
						h.subscribe(ctx, subID, r)
					}
					h.sMu.Lock()
					unsub()
					h.sMu.Unlock()

					h.wg.Done()
				}(h.ps.sa.headered)

				h.ps.payloadBuf, h.ps.sa, h.ps.state = nil, subscribeArgs{}, OP_START
				continue
			}
		case OP_UNSUBSCRIBE:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_UNSUBSCRIBE_CORRELATION_ID_ARG
		case OP_UNSUBSCRIBE_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(h.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(h.ps.ca.cID)
				h.ps.ca.cID = h.ps.ca.cID[:start+toCopy]
				copy(h.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}

			if len(h.ps.ca.cID) >= fujin.Uint32Len {
				h.ps.state = OP_UNSUBSCRIBE_SUB_ID_ARG
			}
		case OP_UNSUBSCRIBE_SUB_ID_ARG:
			h.sMu.Lock()
			if unsub, ok := h.unsubFuncs[b]; ok {
				unsub()
			}
			h.sMu.Unlock()
			enqueueUnsubscribeSuccess(h.out, h.ps.ca.cID)
			pool.Put(h.ps.ca.cID)
			h.ps.ca.cID, h.ps.state = nil, OP_START
		default:
			return ErrParseProto
		}
	}

	return nil
}

func (h *handler) writePing() {
	t := time.NewTicker(h.pingInterval)
	defer t.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-t.C:
			h.out.EnqueueProto(request.PING_REQ)
		}
	}
}

func (h *handler) subscribe(ctx context.Context, subID byte, r internal_reader.Reader) {
	msgHandler := h.subEnqueueMsgFunc(h.out, subID, r)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := r.Subscribe(ctx, msgHandler)
			if err != nil {
				h.l.Error("subscribe", "err", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (h *handler) hsubscribe(ctx context.Context, subID byte, r internal_reader.Reader) {
	msgHandler := h.subEnqueueMsgFuncWithHeaders(h.out, subID, r)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := r.SubscribeH(ctx, msgHandler)
			if err != nil {
				h.l.Error("subscribe with headers", "err", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (h *handler) produce(msg []byte) {
	buf := pool.Get(6) // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (err no/err yes)
	successResp := server.ProduceResponseSuccess(buf, h.ps.ca.cID)
	h.nonTxSessionWriters[h.ps.pa.topic].Write(h.ctx, msg, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write", "err", err)

			successResp[5] = response.ERR_CODE_YES
			errProtoBuf := errProtoBuf(err)
			h.out.EnqueueProtoMulti(successResp, errProtoBuf)
			pool.Put(errProtoBuf)
			pool.Put(buf)
			return
		}
		h.out.EnqueueProto(successResp)
		pool.Put(buf)
	})
}

func (h *handler) hproduce(msg []byte) {
	buf := pool.Get(6)
	hdr := buf[:0]
	hdr = append(hdr, byte(response.RESP_CODE_HPRODUCE))
	hdr = append(hdr, h.ps.ca.cID...)
	hdr = append(hdr, response.ERR_CODE_NO)
	h.nonTxSessionWriters[h.ps.pa.topic].WriteH(h.ctx, msg, h.ps.ha.headersKV, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write", "err", err)
			hdr[5] = response.ERR_CODE_YES
			errProto := errProtoBuf(err)
			h.out.EnqueueProtoMulti(hdr, errProto)
			pool.Put(errProto)
			pool.Put(buf)
			return
		}
		h.out.EnqueueProto(hdr)
		pool.Put(buf)
	})
}

func (h *handler) produceTx(msg []byte) {
	buf := pool.Get(6) // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
	successResp := server.ProduceResponseSuccess(buf, h.ps.ca.cID)
	h.currentTxWriter.Write(h.ctx, msg, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write", "err", err)

			successResp[5] = response.ERR_CODE_YES
			errProtoBuf := errProtoBuf(err)
			h.out.EnqueueProtoMulti(successResp, errProtoBuf)
			pool.Put(errProtoBuf)
			pool.Put(buf)
			return
		}
		h.out.EnqueueProto(successResp)
		pool.Put(buf)
	})
}

func (h *handler) hproduceTx(msg []byte) {
	buf := pool.Get(6)
	hdr := buf[:0]
	hdr = append(hdr, byte(response.RESP_CODE_HPRODUCE))
	hdr = append(hdr, h.ps.ca.cID...)
	hdr = append(hdr, response.ERR_CODE_NO)
	h.currentTxWriter.WriteH(h.ctx, msg, h.ps.ha.headersKV, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write with headers", "err", err)
			hdr[5] = response.ERR_CODE_YES
			errProto := errProtoBuf(err)
			h.out.EnqueueProtoMulti(hdr, errProto)
			pool.Put(errProto)
			pool.Put(buf)
			return
		}
		h.out.EnqueueProto(hdr)
		pool.Put(buf)
	})
}

func (h *handler) produceTxWithCode(respCode response.RespCode, msg []byte) {
	buf := pool.Get(6)
	hdr := buf[:0]
	hdr = append(hdr, byte(respCode))
	hdr = append(hdr, h.ps.ca.cID...)
	hdr = append(hdr, response.ERR_CODE_NO)
	h.currentTxWriter.Write(h.ctx, msg, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write", "err", err)
			hdr[5] = response.ERR_CODE_YES
			errProto := errProtoBuf(err)
			h.out.EnqueueProtoMulti(hdr, errProto)
			pool.Put(errProto)
			pool.Put(buf)
			return
		}
		h.out.EnqueueProto(hdr)
		pool.Put(buf)
	})
}

func (h *handler) fetch(topic string, autoCommit bool, n uint32) {
	header := pool.Get(6)[:0]
	op := byte(response.RESP_CODE_FETCH)
	if h.ps.fa.headered {
		op = byte(response.RESP_CODE_HFETCH)
	}
	header = append(header, op)
	header = append(header, h.ps.ca.cID...)
	header = append(header, response.ERR_CODE_NO)

	go func(headered bool) {
		fetcher, err := h.cpool.GetReader(topic, autoCommit)
		if err != nil {
			header[5] = response.ERR_CODE_YES
			h.out.EnqueueProtoMulti(header, errProtoBuf(err))
			pool.Put(header)
			return
		}

		if headered {
			fetcher.FetchH(h.ctx, n,
				func(n uint32, err error) {
					h.out.Lock()
					if err != nil {
						header[5] = response.ERR_CODE_YES
						h.out.QueueOutboundNoLock(header)
						h.out.QueueOutboundNoLock(errProtoBuf(err))
						pool.Put(header)
						return
					}

					h.out.QueueOutboundNoLock(header)
					pool.Put(header)
					count := pool.Get(fujin.Uint32Len)
					count = binary.BigEndian.AppendUint32(count, n)
					h.out.QueueOutboundNoLock(count)
					pool.Put(count)
				},
				h.fetchEnqueueMsgFuncWithHeaders(h.out, fetcher, topic, autoCommit),
			)
			h.out.SignalFlush()
			h.out.Unlock()
			return
		}

		fetcher.Fetch(h.ctx, n,
			func(n uint32, err error) {
				h.out.Lock()
				if err != nil {
					header[5] = response.ERR_CODE_YES
					h.out.QueueOutboundNoLock(header)
					h.out.QueueOutboundNoLock(errProtoBuf(err))
					pool.Put(header)
					return
				}

				h.out.QueueOutboundNoLock(header)
				pool.Put(header)
				count := pool.Get(fujin.Uint32Len)
				count = binary.BigEndian.AppendUint32(count, n)
				h.out.QueueOutboundNoLock(count)
				pool.Put(count)
			},
			h.fetchEnqueueMsgFunc(h.out, fetcher, topic, autoCommit),
		)
		h.out.SignalFlush()
		h.out.Unlock()
	}(h.ps.fa.headered)
}

func (h *handler) flushBufs() {
	if h.ps.ca.cID != nil {
		pool.Put(h.ps.ca.cID)
		h.ps.ca.cID = nil
	}
	if h.ps.argBuf != nil {
		pool.Put(h.ps.argBuf)
		h.ps.argBuf = nil
	}
	if h.ps.payloadBuf != nil {
		pool.Put(h.ps.payloadBuf)
	}
	if h.ps.payloadsBuf != nil {
		for _, buf := range h.ps.payloadsBuf {
			pool.Put(buf)
		}
		PutBufs(h.ps.payloadsBuf)
	}
}

func (h *handler) close() {
	h.stopRead = true
	h.disconnect()
	close(h.closed)
}

func (h *handler) parseWriteTopicLenArg() error {
	h.ps.pa.topicLen = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
	if h.ps.pa.topicLen == 0 {
		return ErrWriteTopicLenArgEmpty
	}

	return nil
}

func (h *handler) parseWriteTopicArg() error {
	h.ps.pa.topic = string(h.ps.argBuf)
	if h.ps.pa.topic == "" {
		return ErrWriteTopicArgEmpty
	}

	return nil
}

func (h *handler) parseWriteMsgSizeArg() error {
	h.ps.pma.size = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
	if h.ps.pma.size == 0 {
		return ErrWriteMsgSizeArgEmpty
	}

	return nil
}

func (h *handler) parseSubscribeTopicLenArg() error {
	h.ps.sa.topicLen = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
	if h.ps.sa.topicLen == 0 {
		return ErrReaderNameSizeArgNotProvided
	}

	return nil
}

func (h *handler) flushWriters() error {
	for _, sw := range h.nonTxSessionWriters {
		// TODO: on flush err return fail
		if err := sw.Flush(h.ctx); err != nil {
			return fmt.Errorf("flush: %w", err)
		}
	}

	return nil
}

func (h *handler) enqueueWriteErrResponse(err error) {
	errPayload := err.Error()
	errLen := len(errPayload)
	buf := pool.Get(10 + errLen) // resp code produce (1) + request id (4) + err code (1) + err (4 + errLen)
	buf = append(buf, byte(response.RESP_CODE_PRODUCE))
	buf = append(buf, h.ps.ca.cID...)
	buf = append(buf, response.ERR_CODE_YES)
	buf = binary.BigEndian.AppendUint32(buf, uint32(errLen))
	buf = append(buf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
	h.out.EnqueueProto(buf)
	pool.Put(buf)
}

func (h *handler) enqueueStop() {
	h.out.EnqueueProto(request.STOP_REQ)
}

func (h *handler) enqueueAckSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_ACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_NO)
	count := pool.Get(fujin.Uint32Len)
	count = binary.BigEndian.AppendUint32(count, h.ps.aa.msgIDsLen)
	h.out.EnqueueProtoMulti(header, count)
	pool.Put(header)
	pool.Put(count)
}

func (h *handler) enqueueAckSuccessNoLock(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_ACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_NO)
	count := pool.Get(fujin.Uint32Len)
	count = binary.BigEndian.AppendUint32(count, h.ps.aa.msgIDsLen)
	h.out.QueueOutboundNoLock(header)
	h.out.QueueOutboundNoLock(count)
	h.out.SignalFlush()
	pool.Put(header)
	pool.Put(count)
}

func (h *handler) enqueueAckErrNoLock(cID []byte, err error) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_ACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_YES)
	h.out.QueueOutboundNoLock(header)
	h.out.QueueOutboundNoLock(errProtoBuf(err))
	h.out.SignalFlush()
	pool.Put(header)
}

func (h *handler) enqueueAckMsgIDSuccessNoLock(msgID []byte) {
	lenBuf := pool.Get(fujin.Uint32Len)
	lenBuf = binary.BigEndian.AppendUint32(lenBuf, uint32(len(msgID)))
	okFlag := pool.Get(1)
	okFlag = append(okFlag, response.ERR_CODE_NO)
	h.out.QueueOutboundNoLock(lenBuf)
	h.out.QueueOutboundNoLock(msgID)
	h.out.QueueOutboundNoLock(okFlag)
	h.out.SignalFlush()
	pool.Put(lenBuf)
	pool.Put(okFlag)
}

func (h *handler) enqueueAckMsgIDErrNoLock(msgID []byte, err error) {
	lenBuf := pool.Get(fujin.Uint32Len)
	lenBuf = binary.BigEndian.AppendUint32(lenBuf, uint32(len(msgID)))
	errFlag := pool.Get(1)
	errFlag = append(errFlag, response.ERR_CODE_YES)
	h.out.QueueOutboundNoLock(lenBuf)
	h.out.QueueOutboundNoLock(msgID)
	h.out.QueueOutboundNoLock(errFlag)
	h.out.QueueOutboundNoLock(errProtoBuf(err))
	h.out.SignalFlush()
	pool.Put(lenBuf)
	pool.Put(errFlag)
}

func (h *handler) enqueueNackSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_NACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_NO)
	count := pool.Get(fujin.Uint32Len)
	count = binary.BigEndian.AppendUint32(count, h.ps.aa.msgIDsLen)
	h.out.EnqueueProtoMulti(header, count)
	pool.Put(header)
	pool.Put(count)
}

func (h *handler) enqueueNackSuccessNoLock(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_NACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_NO)
	count := pool.Get(fujin.Uint32Len)
	count = binary.BigEndian.AppendUint32(count, h.ps.aa.msgIDsLen)
	h.out.QueueOutboundNoLock(header)
	h.out.QueueOutboundNoLock(count)
	h.out.SignalFlush()
	pool.Put(header)
	pool.Put(count)
}

func (h *handler) enqueueNackErrNoLock(cID []byte, err error) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_NACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_YES)
	h.out.QueueOutboundNoLock(header)
	h.out.QueueOutboundNoLock(errProtoBuf(err))
	h.out.SignalFlush()
	pool.Put(header)
}

func (h *handler) enqueueTxBeginSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_TX_BEGIN))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_NO)
	h.out.EnqueueProto(header)
	pool.Put(header)
}

func (h *handler) enqueueTxBeginErr(cID []byte, err error) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_TX_BEGIN))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_YES)
	h.out.EnqueueProtoMulti(header, errProtoBuf(err))
	pool.Put(header)
}

func (h *handler) enqueueTxCommitSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_TX_COMMIT))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_NO)
	h.out.EnqueueProto(header)
	pool.Put(header)
}

func (h *handler) enqueueTxCommitErr(cID []byte, err error) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_TX_COMMIT))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_YES)
	h.out.EnqueueProtoMulti(header, errProtoBuf(err))
	pool.Put(header)
}

func (h *handler) enqueueTxRollbackSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_TX_ROLLBACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_NO)
	h.out.EnqueueProto(header)
	pool.Put(header)
}

func (h *handler) enqueueTxRollbackErr(cID []byte, err error) {
	header := pool.Get(6)
	header = append(header, byte(response.RESP_CODE_TX_ROLLBACK))
	header = append(header, cID...)
	header = append(header, response.ERR_CODE_YES)
	h.out.EnqueueProtoMulti(header, errProtoBuf(err))
	pool.Put(header)
}

func enqueueSubscribeSuccess(out *fujin.Outbound, code response.RespCode, cID []byte, subID byte) {
	sbuf := pool.Get(fujin.Uint32Len)
	sbuf = append(sbuf, byte(code))
	sbuf = append(sbuf, cID...)
	sbuf = append(sbuf, response.ERR_CODE_NO, subID)
	out.EnqueueProto(sbuf)
	pool.Put(sbuf)
}

func enqueueUnsubscribeSuccess(out *fujin.Outbound, cID []byte) {
	// TODO: add template
	sbuf := pool.Get(fujin.Uint32Len)
	sbuf = append(sbuf, byte(response.RESP_CODE_UNSUBSCRIBE))
	sbuf = append(sbuf, cID...)
	sbuf = append(sbuf, response.ERR_CODE_NO)
	out.EnqueueProto(sbuf)
	pool.Put(sbuf)
}

func (h *handler) subEnqueueMsgFunc(
	out *fujin.Outbound, subID byte, r internal_reader.Reader,
) func(message []byte, topic string, args ...any) {
	staticArgsLen := r.MsgIDStaticArgsLen()
	constBufLen := staticArgsLen + 6 // cmd(1) + subID(1) + msgLen(4)
	if !r.IsAutoCommit() {
		constBufLen += 4 // msgIDLen(4)
	}

	if r.IsAutoCommit() {
		return func(message []byte, topic string, args ...any) {
			resp := response.RESP_CODE_MSG
			buf := pool.Get(len(message) + constBufLen)
			buf = append(buf, byte(resp))
			buf = append(buf, subID)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.EnqueueProto(buf)
			pool.Put(buf)
		}
	}

	return func(message []byte, topic string, args ...any) {
		// In v1 we don't send headers; only optional msg-id metadata
		resp := response.RESP_CODE_MSG
		buf := pool.Get(len(message) + len(topic) + constBufLen)
		buf = append(buf, byte(resp))
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+staticArgsLen))
		buf = r.EncodeMsgID(buf, topic, args...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
		buf = append(buf, message...)
		out.EnqueueProto(buf)
		pool.Put(buf)
	}
}

func (h *handler) subEnqueueMsgFuncWithHeaders(
	out *fujin.Outbound, subID byte, r internal_reader.Reader,
) func(message []byte, topic string, hs [][]byte, args ...any) {
	staticArgsLen := r.MsgIDStaticArgsLen()
	constBufLen := staticArgsLen + 8 // cmd(1) + subID(1) + headersLen(2) + msgLen(4)
	if !r.IsAutoCommit() {
		constBufLen += 4
	}

	if r.IsAutoCommit() {
		return func(message []byte, topic string, hs [][]byte, args ...any) {
			resp := response.RESP_CODE_HMSG
			headersCount := uint16(len(hs))
			headersSize := 0
			for i := 0; i < len(hs); i += 1 {
				headersSize += 4 + len(hs[i])
			}
			buf := pool.Get(headersSize + len(message) + constBufLen)
			buf = append(buf, byte(resp))
			buf = append(buf, subID)
			buf = binary.BigEndian.AppendUint16(buf, headersCount)
			for i := 0; i < len(hs); i += 1 {
				buf = binary.BigEndian.AppendUint32(buf, uint32(len(hs[i])))
				buf = append(buf, hs[i]...)
			}
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.EnqueueProto(buf)
			pool.Put(buf)
		}
	}

	return func(message []byte, topic string, hs [][]byte, args ...any) {
		resp := response.RESP_CODE_HMSG
		headersCount := uint16(len(hs))
		headersSize := 0
		for i := 0; i < len(hs); i += 1 {
			headersSize += 4 + len(hs[i])
		}
		buf := pool.Get(headersSize + len(message) + constBufLen)
		buf = append(buf, byte(resp))
		buf = append(buf, subID)
		buf = binary.BigEndian.AppendUint16(buf, headersCount)
		for i := 0; i < len(hs); i += 1 {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(hs[i])))
			buf = append(buf, hs[i]...)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+staticArgsLen))
		buf = r.EncodeMsgID(buf, topic, args...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
		buf = append(buf, message...)
		out.EnqueueProto(buf)
		pool.Put(buf)
	}
}

func (h *handler) fetchEnqueueMsgFunc(
	out *fujin.Outbound, r internal_reader.Reader, topic string, autoCommit bool,
) func(message []byte, topic string, args ...any) {
	staticArgsLen := r.MsgIDStaticArgsLen()
	constBufLen := staticArgsLen + 4 // msgLen(4)
	if !autoCommit {
		constBufLen += 4
	}

	h.fhMu.RLock()
	mh := h.fetchMsgHandlers[topic][autoCommit]
	h.fhMu.RUnlock()
	if mh != nil {
		return mh
	}

	h.fhMu.Lock()
	defer h.fhMu.Unlock()

	mhm, ok := h.fetchMsgHandlers[topic]
	if !ok {
		mhm = make(map[bool]func(message []byte, topic string, args ...any))
		h.fetchMsgHandlers[topic] = mhm
	}

	switch autoCommit {
	case true:
		mh = func(message []byte, topic string, args ...any) {
			buf := pool.Get(len(message) + constBufLen)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.QueueOutboundNoLock(buf)
			pool.Put(buf)
		}
	default:
		mh = func(message []byte, topic string, args ...any) {
			buf := pool.Get(len(message) + len(topic) + constBufLen)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+staticArgsLen))
			buf = r.EncodeMsgID(buf, topic, args...)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.QueueOutboundNoLock(buf)
			pool.Put(buf)
		}
	}

	mhm[autoCommit] = mh
	return mh
}

func (h *handler) fetchEnqueueMsgFuncWithHeaders(
	out *fujin.Outbound, r internal_reader.Reader, topic string, autoCommit bool,
) func(message []byte, topic string, hs [][]byte, args ...any) {
	staticArgsLen := r.MsgIDStaticArgsLen()
	constBufLen := staticArgsLen + 6 // headersLen(2) + msgLen(4)
	if !autoCommit {
		constBufLen += 4
	}

	h.fhMu.RLock()
	mh := h.fetchMsgWithHeadersHandlers[topic][autoCommit]
	h.fhMu.RUnlock()
	if mh != nil {
		return mh
	}

	h.fhMu.Lock()
	defer h.fhMu.Unlock()

	mhm, ok := h.fetchMsgWithHeadersHandlers[topic]
	if !ok {
		mhm = make(map[bool]func(message []byte, topic string, hs [][]byte, args ...any))
		h.fetchMsgWithHeadersHandlers[topic] = mhm
	}

	switch autoCommit {
	case true:
		mh = func(message []byte, topic string, hs [][]byte, args ...any) {
			headersCount := uint16(len(hs))
			headersSize := 0
			for i := 0; i < len(hs); i += 1 {
				headersSize += 4 + len(hs[i])
			}
			buf := pool.Get(headersSize + len(message) + constBufLen)
			buf = binary.BigEndian.AppendUint16(buf, headersCount)
			for i := 0; i < len(hs); i += 1 {
				buf = binary.BigEndian.AppendUint32(buf, uint32(len(hs[i])))
				buf = append(buf, hs[i]...)
			}
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.QueueOutboundNoLock(buf)
			pool.Put(buf)
		}
	default:
		mh = func(message []byte, topic string, hs [][]byte, args ...any) {
			headersCount := uint16(len(hs))
			headersSize := 0
			for i := 0; i < len(hs); i += 1 {
				headersSize += 4 + len(hs[i])
			}
			buf := pool.Get(headersSize + len(message) + constBufLen)
			buf = binary.BigEndian.AppendUint16(buf, headersCount)
			for i := 0; i < len(hs); i += 1 {
				buf = binary.BigEndian.AppendUint32(buf, uint32(len(hs[i])))
				buf = append(buf, hs[i]...)
			}
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+staticArgsLen))
			buf = r.EncodeMsgID(buf, topic, args...)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.QueueOutboundNoLock(buf)
			pool.Put(buf)
		}
	}

	mhm[autoCommit] = mh
	return mh
}

func (h *handler) enqueueMsgFunc(
	out *fujin.Outbound, r internal_reader.Reader, readerType reader.ReaderType, msgConstsLen int,
) func(message []byte, topic string, args ...any) {
	staticArgsLen := r.MsgIDStaticArgsLen()

	if readerType == reader.Subscriber {
		if r.IsAutoCommit() {
			return func(message []byte, topic string, args ...any) {
				buf := pool.Get(len(message) + msgConstsLen)
				buf = append(buf, byte(response.RESP_CODE_MSG))
				buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
				buf = append(buf, message...)
				out.EnqueueProto(buf)
				pool.Put(buf)
			}
		}

		return func(message []byte, topic string, args ...any) {
			buf := pool.Get(len(message) + len(topic) + msgConstsLen)
			buf = append(buf, byte(response.RESP_CODE_MSG))
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+staticArgsLen))
			buf = r.EncodeMsgID(buf, topic, args...)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.EnqueueProto(buf)
			pool.Put(buf)
		}
	}

	if r.IsAutoCommit() {
		return func(message []byte, topic string, args ...any) {
			buf := pool.Get(len(message) + msgConstsLen)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.QueueOutboundNoLock(buf)
			pool.Put(buf)
		}
	}

	return func(message []byte, topic string, args ...any) {
		buf := pool.Get(len(message) + len(topic) + msgConstsLen)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+staticArgsLen))
		buf = r.EncodeMsgID(buf, topic, args...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
		buf = append(buf, message...)
		out.QueueOutboundNoLock(buf)
		pool.Put(buf)
	}
}

func enqueueSubscribeErr(out *fujin.Outbound, cID []byte, respCode response.RespCode, errCode response.ErrCode, err error) {
	errPayload := err.Error()
	errLen := len(errPayload)
	buf := pool.Get(10 + errLen) // cmd (1) + correlation ID (4) + err nil (1) + err len (4) + err payload (errLen)
	buf = append(buf, byte(respCode))
	buf = append(buf, cID...)
	buf = append(buf, byte(errCode))
	buf = binary.BigEndian.AppendUint32(buf, uint32(errLen))
	buf = append(buf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
	out.EnqueueProto(buf)
	pool.Put(buf)
}

func errProtoBuf(err error) []byte {
	errPayload := err.Error()
	errLen := len(errPayload)
	errBuf := pool.Get(fujin.Uint32Len + errLen) // err len + err payload
	errBuf = binary.BigEndian.AppendUint32(errBuf, uint32(errLen))
	return append(errBuf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
}

func replaceUnsafe(slice []byte, start int, new []byte) {
	ptr := unsafe.Pointer(&slice[start])
	dst := (*[1 << 30]byte)(ptr)
	copy(dst[:len(new)], new)
}

func parseBool(b byte) (bool, error) {
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, ErrParseProto
	}
}
