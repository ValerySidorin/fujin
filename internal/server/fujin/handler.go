package fujin

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"unsafe"

	"github.com/ValerySidorin/fujin/internal/connectors"
	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response/server"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	internal_reader "github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

const (
	OP_START int = iota

	OP_CONNECT_WRITER
	OP_CONNECT_WRITER_ARG
	OP_WRITE
	OP_WRITE_CORRELATION_ID_ARG
	OP_WRITE_ARG
	OP_WRITE_MSG_ARG
	OP_WRITE_MSG_PAYLOAD

	OP_CONNECT_READER
	OP_CONNECT_READER_ARG
	OP_CONNECT_READER_PAYLOAD

	OP_FETCH
	OP_FETCH_CORRELATION_ID_ARG
	OP_FETCH_ARG

	OP_ACK
	OP_ACK_CORRELATION_ID_ARG
	OP_ACK_ARG
	OP_ACK_MSG_ID_ARG
	OP_ACK_MSG_ID_PAYLOAD

	OP_NACK
	OP_NACK_CORRELATION_ID_ARG
	OP_NACK_ARG
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

	OP_WRITE_TX
	OP_WRITE_TX_CORRELATION_ID_ARG
	OP_WRITE_TX_ARG
	OP_WRITE_TX_MSG_ARG
	OP_WRITE_TX_MSG_PAYLOAD
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
	errNoTemplate  = []byte{0}
	errYesTemplate = []byte{1}
)

type sessionState byte

const (
	SESSION_STATE_UNKNOWN sessionState = iota
	SESSION_STATE_WRITER
	SESSION_STATE_READER
	SESSION_STATE_TX
)

type parseState struct {
	state       int
	argBuf      []byte
	payloadBuf  []byte
	payloadsBuf [][]byte

	ca correlationIDArg

	cwa connectWriterArgs
	wa  writeArgs
	wma writeMsgArgs

	cra connectReaderArgs
	aa  ackArgs
}

type correlationIDArg struct {
	cID []byte
}

type writeArgs struct {
	topicLen uint32
	topic    string
}

type writeMsgArgs struct {
	size uint32
}

type connectReaderArgs struct {
	readerNameLen uint32
	typ           byte
	autoCommit    bool
}

type connectWriterArgs struct {
	writerIDLen uint32
	writerID    string
}

type ackArgs struct {
	currMsgIDLen uint32
	msgIDsLen    uint32
	msgIDsBuf    []byte
}

type handler struct {
	ctx  context.Context
	out  *fujin.Outbound
	cman *connectors.Manager

	ps           *parseState
	sessionState sessionState

	// producer
	writerID             string
	nonTxSessionWriters  map[string]writer.Writer
	currentTxWriter      writer.Writer
	currentTxWriterTopic string

	// consumer/subscriber
	readerType               reader.ReaderType
	sessionReader            internal_reader.Reader
	readerMsgIDStaticArgsLen int
	msgHandler               func(message []byte, topic string, args ...any)

	ackSuccessRespTemplate []byte
	ackErrRespTemplate     []byte

	nackSuccessRespTemplate []byte
	nackErrRespTemplate     []byte

	txBeginSuccessRespTemplate    []byte
	txBeginErrRespTemplate        []byte
	txCommitSuccessRespTemplate   []byte
	txCommitErrRespTemplate       []byte
	txRollbackSuccessRespTemplate []byte
	txRollbackErrRespTemplate     []byte

	disconnect func()

	wg       sync.WaitGroup
	stopRead bool
	closed   chan struct{}

	l *slog.Logger
}

func newHandler(
	ctx context.Context, cman *connectors.Manager,
	out *fujin.Outbound, l *slog.Logger,
) *handler {
	return &handler{
		ctx:          ctx,
		cman:         cman,
		l:            l,
		out:          out,
		ps:           &parseState{},
		disconnect:   func() {},
		sessionState: SESSION_STATE_UNKNOWN,
		closed:       make(chan struct{}),

		ackSuccessRespTemplate:  []byte{byte(response.RESP_CODE_ACK), 0, 0, 0, 0, 0},
		ackErrRespTemplate:      []byte{byte(response.RESP_CODE_ACK), 0, 0, 0, 0, 1},
		nackSuccessRespTemplate: []byte{byte(response.RESP_CODE_NACK), 0, 0, 0, 0, 0},
		nackErrRespTemplate:     []byte{byte(response.RESP_CODE_NACK), 0, 0, 0, 0, 1},

		txBeginSuccessRespTemplate:    []byte{byte(response.RESP_CODE_TX_BEGIN), 0, 0, 0, 0, 0},
		txBeginErrRespTemplate:        []byte{byte(response.RESP_CODE_TX_BEGIN), 0, 0, 0, 0, 1},
		txCommitSuccessRespTemplate:   []byte{byte(response.RESP_CODE_TX_COMMIT), 0, 0, 0, 0, 0},
		txCommitErrRespTemplate:       []byte{byte(response.RESP_CODE_TX_COMMIT), 0, 0, 0, 0, 1},
		txRollbackSuccessRespTemplate: []byte{byte(response.RESP_CODE_TX_ROLLBACK), 0, 0, 0, 0, 0},
		txRollbackErrRespTemplate:     []byte{byte(response.RESP_CODE_TX_ROLLBACK), 0, 0, 0, 0, 1},
	}
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
			case SESSION_STATE_UNKNOWN:
				switch b {
				case byte(request.OP_CODE_CONNECT_WRITER):
					h.sessionState = SESSION_STATE_WRITER
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
						h.out.EnqueueProto(response.DISCONNECT_RESP)
					}
					h.ps.state = OP_CONNECT_WRITER
				case byte(request.OP_CODE_CONNECT_READER):
					h.ps.state = OP_CONNECT_READER
				default:
					return ErrParseProto
				}
			case SESSION_STATE_WRITER:
				switch b {
				case byte(request.OP_CODE_WRITE):
					h.ps.state = OP_WRITE
				case byte(request.OP_CODE_TX_BEGIN):
					h.ps.state = OP_BEGIN_TX
					h.sessionState = SESSION_STATE_TX
				case byte(request.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX_FAIL
				case byte(request.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX_FAIL
				case byte(request.OP_CODE_DISCONNECT):
					return ErrClose
				default:
					return ErrParseProto
				}
			case SESSION_STATE_TX:
				switch b {
				case byte(request.OP_CODE_WRITE):
					h.ps.state = OP_WRITE_TX
				case byte(request.OP_CODE_TX_BEGIN):
					h.ps.state = OP_BEGIN_TX_FAIL
					h.sessionState = SESSION_STATE_TX
				case byte(request.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX
				case byte(request.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX
				case byte(request.OP_CODE_DISCONNECT):
					return ErrClose
				}
			case SESSION_STATE_READER:
				switch b {
				case byte(request.OP_CODE_FETCH):
					h.ps.state = OP_FETCH
				case byte(request.OP_CODE_ACK):
					h.ps.state = OP_ACK
				case byte(request.OP_CODE_NACK):
					h.ps.state = OP_NACK
				case byte(request.OP_CODE_DISCONNECT):
					return ErrClose
				default:
					return ErrParseProto
				}
			}
		case OP_WRITE:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_WRITE_CORRELATION_ID_ARG
		case OP_WRITE_CORRELATION_ID_ARG:
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
				h.ps.state = OP_WRITE_ARG
			}
		case OP_WRITE_ARG:
			if h.ps.wa.topicLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.wa.topicLen))
				}

				toCopy := int(h.ps.wa.topicLen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.wa.topicLen) {
					if err := h.parseWriteTopicArg(); err != nil {
						h.l.Error("parse write topic arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.state = nil, OP_WRITE_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.wa.topicLen == 0 {
					if err := h.parseWriteTopicLenArg(); err != nil {
						h.l.Error("parse write topic len arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}
				pool.Put(h.ps.argBuf)
				h.enqueueWriteErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
				continue
			}
		case OP_WRITE_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseWriteMsgSizeArg(); err != nil {
					h.l.Error("parse write msg size arg", "err", err)
					pool.Put(h.ps.argBuf)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.ca.cID)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_WRITE_MSG_PAYLOAD
			}
		case OP_WRITE_MSG_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.wma.size) - len(h.ps.payloadBuf)
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

				if len(h.ps.payloadBuf) >= int(h.ps.wma.size) {
					if _, ok := h.nonTxSessionWriters[h.ps.wa.topic]; !ok {
						w, err := h.cman.GetWriter(h.ps.wa.topic, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}

						h.nonTxSessionWriters[h.ps.wa.topic] = w
					}

					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.wma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.wma.size) {
					if _, ok := h.nonTxSessionWriters[h.ps.wa.topic]; !ok {
						w, err := h.cman.GetWriter(h.ps.wa.topic, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}

						h.nonTxSessionWriters[h.ps.wa.topic] = w
					}

					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
				}
			}
		case OP_FETCH:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_FETCH_CORRELATION_ID_ARG
		case OP_FETCH_CORRELATION_ID_ARG:
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
					h.ps.state = OP_FETCH_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_FETCH_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				val := binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				h.fetch(val)
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
					h.sessionReader.Ack(h.ctx, h.ps.payloadsBuf,
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
					h.sessionReader.Nack(h.ctx, h.ps.payloadsBuf,
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
		case OP_WRITE_TX:
			h.ps.ca.cID = pool.Get(fujin.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_WRITE_TX_CORRELATION_ID_ARG
		case OP_WRITE_TX_CORRELATION_ID_ARG:
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
					h.ps.state = OP_WRITE_TX_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(fujin.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_WRITE_TX_ARG:
			if h.ps.wa.topicLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.wa.topicLen))
				}

				toCopy := int(h.ps.wa.topicLen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.wa.topicLen) {
					if err := h.parseWriteTopicArg(); err != nil {
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil

					h.ps.argBuf, h.ps.state = nil, OP_WRITE_TX_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.wa.topicLen == 0 {
					if err := h.parseWriteTopicLenArg(); err != nil {
						pool.Put(h.ps.argBuf)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						return err
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}

				// this should not happen ever
				pool.Put(h.ps.argBuf)
				h.enqueueWriteErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				return ErrParseProto
			}
		case OP_WRITE_TX_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(fujin.Uint32Len)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseWriteMsgSizeArg(); err != nil {
					h.l.Error("parse write msg size arg", "err", err)
					pool.Put(h.ps.argBuf)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.ca.cID)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_WRITE_TX_MSG_PAYLOAD
			}
		case OP_WRITE_TX_MSG_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.wma.size) - len(h.ps.payloadBuf)
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

				if len(h.ps.payloadBuf) >= int(h.ps.wma.size) {
					if h.currentTxWriter != nil {
						if !h.cman.WriterCanBeReusedInTx(h.currentTxWriter, h.ps.wa.topic) {
							h.l.Error("writer can not be reused in tx")
							h.enqueueWriteErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						currentTxWriter, err := h.cman.GetWriter(h.ps.wa.topic, h.writerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}

						if err := currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}

						h.currentTxWriter, h.currentTxWriterTopic = currentTxWriter, h.ps.wa.topic
					}

					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.wma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.wma.size) {
					if h.currentTxWriter != nil {
						if !h.cman.WriterCanBeReusedInTx(h.currentTxWriter, h.ps.wa.topic) {
							h.l.Error("writer can not be reused in tx1")
							h.enqueueWriteErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						currentTxWriter, err := h.cman.GetWriter(h.ps.wa.topic, h.writerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}

						if err := currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}

						h.currentTxWriter, h.currentTxWriterTopic = currentTxWriter, h.ps.wa.topic
					}

					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
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
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, SESSION_STATE_TX, OP_START
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
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, SESSION_STATE_WRITER, OP_START
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
							h.ps.ca.cID, h.currentTxWriter, h.sessionState, h.ps.state = nil, nil, SESSION_STATE_WRITER, OP_START
							continue
						}
						h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterTopic, h.writerID)
						h.currentTxWriter = nil
					}
					h.enqueueTxRollbackSuccess(h.ps.ca.cID)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, SESSION_STATE_WRITER, OP_START
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
		case OP_CONNECT_WRITER:
			h.ps.state = OP_CONNECT_WRITER_ARG
			h.ps.argBuf = pool.Get(fujin.Uint32Len)
			h.ps.argBuf = append(h.ps.argBuf, b)
		case OP_CONNECT_WRITER_ARG:
			if h.ps.cwa.writerIDLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.cwa.writerIDLen))
				}

				toCopy := int(h.ps.cwa.writerIDLen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.cwa.writerIDLen) {
					h.ps.cwa.writerID = string(h.ps.argBuf)
					pool.Put(h.ps.argBuf)
					h.writerID = h.ps.cwa.writerID
					h.ps.argBuf, h.ps.cwa, h.ps.state = nil, connectWriterArgs{}, OP_START
				}

				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if h.ps.cwa.writerIDLen == 0 {
					h.ps.cwa.writerIDLen = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					if h.ps.cwa.writerIDLen == 0 {
						h.ps.argBuf, h.ps.cwa, h.ps.state = nil, connectWriterArgs{}, OP_START
					}
				}
			}
		case OP_CONNECT_READER:
			if h.ps.cra.typ == 0 {
				if err := h.parseReaderTypeArg(b); err != nil {
					enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
					return err
				}
				continue
			}
			if err := h.parseReaderIsAutoCommitArg(b); err != nil {
				enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
				return err
			}

			h.ps.argBuf = pool.Get(fujin.Uint32Len)
			h.ps.state = OP_CONNECT_READER_ARG
		case OP_CONNECT_READER_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= fujin.Uint32Len {
				if err := h.parseReaderNameSizeArg(); err != nil {
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.cra, h.ps.state = nil, connectReaderArgs{}, OP_START
					enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_CONNECT_READER_PAYLOAD
			}
		case OP_CONNECT_READER_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.cra.readerNameLen) - len(h.ps.payloadBuf)
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

				if len(h.ps.payloadBuf) >= int(h.ps.cra.readerNameLen) {
					h.ps.state = OP_START
					pool.Put(h.ps.payloadBuf)

					h.sessionState = SESSION_STATE_READER

					r, err := h.cman.GetReader(string(h.ps.payloadBuf), h.ps.cra.autoCommit)
					if err != nil {
						enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
						return fmt.Errorf("get reader: %w", err)
					}

					h.readerMsgIDStaticArgsLen = r.MsgIDStaticArgsLen()
					h.readerType = reader.ReaderType(h.ps.cra.typ)

					enqueueConnectReaderSuccess(h.out)
					h.sessionReader = r

					h.wg.Add(1)
					go func() {
						ctx, cancel := context.WithCancel(h.ctx)
						if err := h.connectReader(ctx, cancel, r, h.readerType); err != nil {
							h.l.Error("subscribe", "err", err)
							enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
							cancel()
							h.stopRead = true
							h.wg.Done()
							return
						}

						cancel()
						h.stopRead = true
						h.wg.Done()
					}()

					h.ps.payloadBuf, h.ps.cra, h.ps.state = nil, connectReaderArgs{}, OP_START
					continue
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.cra.readerNameLen))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
			}
		default:
			return ErrParseProto
		}
	}

	return nil
}

func (h *handler) connectReader(
	ctx context.Context, cancel context.CancelFunc,
	r internal_reader.Reader, typ reader.ReaderType,
) error {
	h.disconnect = func() {
		cancel()
		r.Close()
		h.out.EnqueueProto(response.DISCONNECT_RESP)
	}

	msgConstsLen := h.readerMsgIDStaticArgsLen + 4 // msg args + msg len (4)
	if h.readerType == reader.Subscriber {
		msgConstsLen += 1 // cmd (1)
	}
	if !r.IsAutoCommit() {
		msgConstsLen += 4 // + msg id len (4). Maybe it can be 1 byte?
	}
	h.msgHandler = h.enqueueMsgFunc(h.out, r, h.readerType, msgConstsLen)

	switch typ {
	case reader.Subscriber:
		return r.Subscribe(ctx, func(message []byte, topic string, args ...any) {
			h.msgHandler(message, topic, args...)
		})
	case reader.Consumer:
		<-ctx.Done()
		return nil
	default:
		return fmt.Errorf("invalid reader type: %d", typ)
	}
}
func (h *handler) produce(msg []byte) {
	buf := pool.Get(6) // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (err no/err yes)
	successResp := server.WriteResponseSuccess(buf, h.ps.ca.cID)
	h.nonTxSessionWriters[h.ps.wa.topic].Write(h.ctx, msg, func(err error) {
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

func (h *handler) produceTx(msg []byte) {
	buf := pool.Get(6) // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
	successResp := server.WriteResponseSuccess(buf, h.ps.ca.cID)
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

func (h *handler) fetch(val uint32) {
	buf := pool.Get(6)[:6]
	buf[0] = byte(response.RESP_CODE_FETCH)
	replaceUnsafe(buf, 1, h.ps.ca.cID)

	go func() {
		if h.readerType != reader.Consumer {
			buf[5] = 1
			h.out.EnqueueProtoMulti(buf, errProtoBuf(ErrInvalidReaderType))
			pool.Put(buf)
			return
		}

		if val == 0 {
			buf[5] = 1
			h.out.EnqueueProtoMulti(buf, errProtoBuf(ErrFetchArgNotProvided))
			pool.Put(buf)
			return
		}

		buf[5] = 0

		h.sessionReader.Fetch(h.ctx, val,
			func(n uint32, err error) {
				h.out.Lock()
				if err != nil {
					buf[5] = 1
					h.out.QueueOutboundNoLock(buf)
					h.out.QueueOutboundNoLock(errProtoBuf(err))
					pool.Put(buf)
					return
				}

				h.out.QueueOutboundNoLock(buf)
				pool.Put(buf)
				buf = pool.Get(fujin.Uint32Len)
				buf = binary.BigEndian.AppendUint32(buf, n)
				h.out.QueueOutboundNoLock(buf)
				pool.Put(buf)
			},
			func(message []byte, topic string, args ...any) {
				h.msgHandler(message, topic, args...)
			},
		)
		h.out.SignalFlush()
		h.out.Unlock()
	}()
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
	h.ps.wa.topicLen = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
	if h.ps.wa.topicLen == 0 {
		return ErrWriteTopicLenArgEmpty
	}

	return nil
}

func (h *handler) parseWriteTopicArg() error {
	h.ps.wa.topic = string(h.ps.argBuf)
	if h.ps.wa.topic == "" {
		return ErrWriteTopicArgEmpty
	}

	return nil
}

func (h *handler) parseWriteMsgSizeArg() error {
	h.ps.wma.size = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
	if h.ps.wma.size == 0 {
		return ErrWriteMsgSizeArgEmpty
	}

	return nil
}

func (h *handler) parseReaderNameSizeArg() error {
	h.ps.cra.readerNameLen = binary.BigEndian.Uint32(h.ps.argBuf[0:fujin.Uint32Len])
	if h.ps.cra.readerNameLen == 0 {
		return ErrReaderNameSizeArgNotProvided
	}

	return nil
}

func (h *handler) parseReaderTypeArg(b byte) error {
	if b != 1 && b != 2 {
		return ErrInvalidReaderType
	}
	h.ps.cra.typ = b
	return nil
}

func (h *handler) parseReaderIsAutoCommitArg(b byte) error {
	switch b {
	case 0:
	case 1:
		h.ps.cra.autoCommit = true
	default:
		return ErrConnectReaderIsAutoCommitArgInvalid
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
	buf = append(buf, byte(response.RESP_CODE_WRITE))
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
	replaceUnsafe(h.ackSuccessRespTemplate, 1, cID)
	buf := pool.Get(fujin.Uint32Len)
	buf = binary.BigEndian.AppendUint32(buf, h.ps.aa.msgIDsLen)
	h.out.EnqueueProtoMulti(h.ackSuccessRespTemplate, buf)
	pool.Put(buf)
}

func (h *handler) enqueueAckSuccessNoLock(cID []byte) {
	replaceUnsafe(h.ackSuccessRespTemplate, 1, cID)
	buf := pool.Get(fujin.Uint32Len)
	buf = binary.BigEndian.AppendUint32(buf, h.ps.aa.msgIDsLen)
	h.out.QueueOutboundNoLock(h.ackSuccessRespTemplate)
	h.out.QueueOutboundNoLock(buf)
	h.out.SignalFlush()
	pool.Put(buf)
}

func (h *handler) enqueueAckErrNoLock(cID []byte, err error) {
	replaceUnsafe(h.ackErrRespTemplate, 1, cID)
	h.out.QueueOutboundNoLock(h.ackErrRespTemplate)
	h.out.QueueOutboundNoLock(errProtoBuf(err))
	h.out.SignalFlush()
}

func (h *handler) enqueueAckMsgIDSuccessNoLock(msgID []byte) {
	buf := pool.Get(fujin.Uint32Len)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgID)))
	h.out.QueueOutboundNoLock(buf)
	h.out.QueueOutboundNoLock(msgID)
	h.out.QueueOutboundNoLock(errNoTemplate)
	h.out.SignalFlush()
	pool.Put(buf)
}

func (h *handler) enqueueAckMsgIDErrNoLock(msgID []byte, err error) {
	buf := pool.Get(fujin.Uint32Len)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgID)))
	h.out.QueueOutboundNoLock(buf)
	h.out.QueueOutboundNoLock(msgID)
	h.out.QueueOutboundNoLock(errYesTemplate)
	h.out.QueueOutboundNoLock(errProtoBuf(err))
	h.out.SignalFlush()
	pool.Put(buf)
}

func (h *handler) enqueueNackSuccess(cID []byte) {
	replaceUnsafe(h.nackSuccessRespTemplate, 1, cID)
	buf := pool.Get(fujin.Uint32Len)
	buf = binary.BigEndian.AppendUint32(buf, h.ps.aa.msgIDsLen)
	h.out.EnqueueProtoMulti(h.nackSuccessRespTemplate, buf)
	pool.Put(buf)
}

func (h *handler) enqueueNackSuccessNoLock(cID []byte) {
	replaceUnsafe(h.nackSuccessRespTemplate, 1, cID)
	buf := pool.Get(fujin.Uint32Len)
	buf = binary.BigEndian.AppendUint32(buf, h.ps.aa.msgIDsLen)
	h.out.QueueOutboundNoLock(h.nackSuccessRespTemplate)
	h.out.QueueOutboundNoLock(buf)
	h.out.SignalFlush()
	pool.Put(buf)
}

func (h *handler) enqueueNackErrNoLock(cID []byte, err error) {
	replaceUnsafe(h.nackErrRespTemplate, 1, cID)
	h.out.QueueOutboundNoLock(h.nackErrRespTemplate)
	h.out.QueueOutboundNoLock(errProtoBuf(err))
	h.out.SignalFlush()
}

func (h *handler) enqueueTxBeginSuccess(cID []byte) {
	replaceUnsafe(h.txBeginSuccessRespTemplate, 1, cID)
	h.out.EnqueueProto(h.txBeginSuccessRespTemplate)
}

func (h *handler) enqueueTxBeginErr(cID []byte, err error) {
	replaceUnsafe(h.txBeginErrRespTemplate, 1, cID)
	h.out.EnqueueProtoMulti(h.txBeginErrRespTemplate, errProtoBuf(err))
}

func (h *handler) enqueueTxCommitSuccess(cID []byte) {
	replaceUnsafe(h.txCommitSuccessRespTemplate, 1, cID)
	h.out.EnqueueProto(h.txCommitSuccessRespTemplate)
}

func (h *handler) enqueueTxCommitErr(cID []byte, err error) {
	replaceUnsafe(h.txCommitErrRespTemplate, 1, cID)
	h.out.EnqueueProtoMulti(h.txCommitErrRespTemplate, errProtoBuf(err))
}

func (h *handler) enqueueTxRollbackSuccess(cID []byte) {
	replaceUnsafe(h.txRollbackSuccessRespTemplate, 1, cID)
	h.out.EnqueueProto(h.txRollbackSuccessRespTemplate)
}

func (h *handler) enqueueTxRollbackErr(cID []byte, err error) {
	replaceUnsafe(h.txRollbackErrRespTemplate, 1, cID)
	h.out.EnqueueProtoMulti(h.txRollbackErrRespTemplate, errProtoBuf(err))
}

func enqueueConnectReaderSuccess(out *fujin.Outbound) {
	sbuf := pool.Get(fujin.Uint32Len)
	sbuf = append(sbuf,
		byte(response.RESP_CODE_CONNECT_READER),
		byte(response.ERR_CODE_NO),
	)
	out.EnqueueProto(sbuf)
	pool.Put(sbuf)
}

func (h *handler) enqueueMsgFunc(
	out *fujin.Outbound, r internal_reader.Reader, readerType reader.ReaderType, msgConstsLen int,
) func(message []byte, topic string, args ...any) {
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
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+h.readerMsgIDStaticArgsLen))
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
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)+h.readerMsgIDStaticArgsLen))
		buf = r.EncodeMsgID(buf, topic, args...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
		buf = append(buf, message...)
		out.QueueOutboundNoLock(buf)
		pool.Put(buf)
	}
}

func enqueueConnectReaderErr(out *fujin.Outbound, respCode response.RespCode, errCode response.ErrCode, err error) {
	errPayload := err.Error()
	errLen := len(errPayload)
	buf := pool.Get(6 + errLen) // cmd (1)  + err code (1) + err len (4) + err payload (errLen)
	buf = append(buf, byte(respCode), byte(errCode))
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
