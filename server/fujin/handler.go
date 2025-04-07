package fujin

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"unsafe"

	"github.com/ValerySidorin/fujin/connector"
	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/connector/writer"
	"github.com/ValerySidorin/fujin/internal/server/fujin/pool"
	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
	"github.com/ValerySidorin/fujin/server/fujin/proto/response"
	"github.com/ValerySidorin/fujin/server/fujin/proto/response/server"
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

	OP_NACK
	OP_NACK_CORRELATION_ID_ARG
	OP_NACK_ARG

	OP_BEGIN_TX
	OP_BEGIN_TX_CORRELATION_ID_ARG

	OP_COMMIT_TX
	OP_COMMIT_TX_CORRELATION_ID_ARG

	OP_ROLLBACK_TX
	OP_ROLLBACK_TX_CORRELATION_ID_ARG

	OP_WRITE_TX
	OP_WRITE_TX_CORRELATION_ID_ARG
	OP_WRITE_TX_ARG
	OP_WRITE_TX_MSG_ARG
	OP_WRITE_TX_MSG_PAYLOAD
)

var (
	ErrParseProto               = errors.New("parse proto")
	ErrWriterCanNotBeReusedInTx = errors.New("writer can not be reuse in tx")
	ErrFetchArgNotProvided      = errors.New("fetch arg not provided")
)

type sessionState byte

const (
	SESSION_STATE_UNKNOWN sessionState = iota
	SESSION_STATE_WRITER
	SESSION_STATE_READER
	SESSION_STATE_TX
)

type parseState struct {
	state      int
	argBuf     []byte
	payloadBuf []byte

	ca correlationIDArg

	cwa connectWriterArgs
	wa  writeArgs
	wma writeMsgArgs

	cra connectReaderArgs
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
	size uint32
	typ  byte
}

type connectWriterArgs struct {
	writerIDlen uint32
	writerID    string
}

type handler struct {
	ctx  context.Context
	out  *outbound
	cman *connector.Manager

	ps           *parseState
	sessionState sessionState

	// producer
	writerID            string
	nonTxSessionWriters map[string]writer.Writer
	currentTxWriter     writer.Writer
	currentTxWriterPub  string

	// consumer/subscriber
	sessionReader           reader.Reader
	sessionReaderMsgMetaLen int
	msgHandler              func(message []byte, args ...any)
	consumeTrigger          func()

	ackSuccessRespTemplate []byte
	ackErrRespTemplate     []byte

	nAckSuccessRespTemplate []byte
	nAckErrRespTemplate     []byte

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
	ctx context.Context, cman *connector.Manager,
	out *outbound, l *slog.Logger,
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
		nAckSuccessRespTemplate: []byte{byte(response.RESP_CODE_NACK), 0, 0, 0, 0, 0},
		nAckErrRespTemplate:     []byte{byte(response.RESP_CODE_NACK), 0, 0, 0, 0, 1},

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
							if err := h.currentTxWriter.RollbackTx(h.ctx); err != nil {
								h.l.Error("rollback tx", "err", err)
							}
							h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterPub, h.writerID)
							h.currentTxWriter = nil
						}
						h.out.enqueueProto(response.DISCONNECT_RESP)
					}
					h.ps.state = OP_CONNECT_WRITER
				case byte(request.OP_CODE_CONNECT_READER):
					h.ps.state = OP_CONNECT_READER
				default:
					h.close()
					return ErrParseProto
				}
			case SESSION_STATE_WRITER:
				switch b {
				case byte(request.OP_CODE_WRITE):
					h.ps.state = OP_WRITE
				case byte(request.OP_CODE_TX_BEGIN):
					h.ps.state = OP_BEGIN_TX
					h.sessionState = SESSION_STATE_TX
				case byte(request.OP_CODE_DISCONNECT):
					h.close()
					return nil
				default:
					h.close()
					return ErrParseProto
				}
			case SESSION_STATE_TX:
				switch b {
				case byte(request.OP_CODE_WRITE):
					h.ps.state = OP_WRITE_TX
				case byte(request.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX
				case byte(request.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX
				case byte(request.OP_CODE_DISCONNECT):
					h.close()
					return nil
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
					h.close()
					return nil
				default:
					h.close()
					return ErrParseProto
				}
			}
		case OP_WRITE:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_WRITE_CORRELATION_ID_ARG
		case OP_WRITE_CORRELATION_ID_ARG:
			toCopy := Uint32Len - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= Uint32Len {
				h.ps.argBuf = pool.Get(Uint32Len)
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
					if err := h.parseProducePubArg(); err != nil {
						h.l.Error("parse produce pub arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
						continue
					}
					pool.Put(h.ps.argBuf)

					if _, ok := h.nonTxSessionWriters[h.ps.wa.topic]; !ok {
						w, err := h.cman.GetWriter(h.ps.wa.topic, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.argBuf)
							pool.Put(h.ps.ca.cID)
							h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
							continue
						}

						h.nonTxSessionWriters[h.ps.wa.topic] = w
					}
					h.ps.argBuf, h.ps.state = nil, OP_WRITE_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= Uint32Len {
				if h.ps.wa.topicLen == 0 {
					if err := h.parseProducePubLenArg(); err != nil {
						h.l.Error("parse produce pub len arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
						continue
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
				h.ps.argBuf = pool.Get(Uint32Len)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= Uint32Len {
				if err := h.parseProduceMsgSizeArg(); err != nil {
					h.l.Error("parse produce msg size arg", "err", err)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.argBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
					continue
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
					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, nil, writeArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.wma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.wma.size) {
					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, nil, writeArgs{}, OP_START
				}
			}
		case OP_FETCH:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_FETCH_CORRELATION_ID_ARG
		case OP_FETCH_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= Uint32Len {
					h.ps.argBuf = pool.Get(Uint32Len)
					h.ps.state = OP_FETCH_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_FETCH_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= Uint32Len {
				val := binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil

				h.fetch(val)
				pool.Put(h.ps.ca.cID)
				h.ps.ca.cID, h.ps.state = nil, OP_START
			}
		case OP_ACK:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ACK_CORRELATION_ID_ARG
		case OP_ACK_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= Uint32Len {
					h.ps.argBuf = pool.Get(h.sessionReaderMsgMetaLen)
					h.ps.state = OP_ACK_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_ACK_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= h.sessionReaderMsgMetaLen {
				if err := h.sessionReader.Ack(h.ctx, h.ps.argBuf); err != nil {
					pool.Put(h.ps.argBuf)
					h.l.Error("ack", "err", err)
					h.enqueueAckErr(h.ps.ca.cID, err)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
					continue
				}
				pool.Put(h.ps.argBuf)
				h.enqueueAckSuccess(h.ps.ca.cID)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
			}
		case OP_NACK:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ACK_CORRELATION_ID_ARG
		case OP_NACK_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= Uint32Len {
					h.ps.argBuf = pool.Get(h.sessionReaderMsgMetaLen)
					h.ps.state = OP_NACK_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_NACK_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= h.sessionReaderMsgMetaLen {
				if err := h.sessionReader.Nack(h.ctx, h.ps.argBuf); err != nil {
					pool.Put(h.ps.argBuf)
					h.l.Error("ack", "err", err)
					h.enqueueNAckErr(h.ps.ca.cID, err)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
					continue
				}
				pool.Put(h.ps.argBuf)
				h.enqueueNAckSuccess(h.ps.ca.cID)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
			}
		case OP_WRITE_TX:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_WRITE_TX_CORRELATION_ID_ARG
		case OP_WRITE_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= Uint32Len {
					h.ps.argBuf = pool.Get(Uint32Len)
					h.ps.state = OP_WRITE_TX_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(Uint32Len)
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
					if err := h.parseProducePubArg(); err != nil {
						h.l.Error("parse produce pub arg", "err", err)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
						continue
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil

					if h.currentTxWriter != nil {
						if !h.cman.WriterCanBeReusedInTx(h.currentTxWriter, h.ps.wa.topic) {
							h.l.Error("writer can not be reused in tx")
							h.enqueueWriteErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.wa, h.ps.state = nil, writeArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						h.currentTxWriter, err = h.cman.GetWriter(h.ps.wa.topic, h.writerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.wa, h.ps.state = nil, writeArgs{}, OP_START
							continue
						}

						if err := h.currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueWriteErrResponse(err)
							pool.Put(h.ps.ca.cID)
							continue
						}

						h.currentTxWriterPub = h.ps.wa.topic
					}

					h.ps.argBuf, h.ps.state = nil, OP_WRITE_TX_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= Uint32Len {
				if h.ps.wa.topicLen == 0 {
					if err := h.parseProducePubLenArg(); err != nil {
						h.l.Error("parse produce pub len arg", "err", err)
						pool.Put(h.ps.argBuf)
						h.enqueueWriteErrResponse(err)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
						continue
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}

				// this should not happen ever
				pool.Put(h.ps.argBuf)
				h.enqueueWriteErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
				continue
			}
		case OP_WRITE_TX_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(Uint32Len)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= Uint32Len {
				if err := h.parseProduceMsgSizeArg(); err != nil {
					h.l.Error("parse produce msg size arg", "err", err)
					pool.Put(h.ps.argBuf)
					h.enqueueWriteErrResponse(err)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.wa, h.ps.state = nil, nil, writeArgs{}, OP_START
					continue
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
					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, nil, writeArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.wma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.wma.size) {
					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.wa, h.ps.state = nil, nil, nil, writeArgs{}, OP_START
				}
			}
		case OP_BEGIN_TX:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_BEGIN_TX_CORRELATION_ID_ARG
		case OP_BEGIN_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= Uint32Len {
					if err := h.flushWriters(); err != nil {
						h.enqueueTxBeginErr(h.ps.ca.cID, err)
						h.l.Error("begin tx", "err", err)
						pool.Put(h.ps.ca.cID)
						h.ps.ca.cID, h.ps.state = nil, OP_START
						continue
					}

					h.enqueueTxBeginSuccess(h.ps.ca.cID)
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, SESSION_STATE_TX, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_COMMIT_TX:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_COMMIT_TX_CORRELATION_ID_ARG
		case OP_COMMIT_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= Uint32Len {
					if err := h.currentTxWriter.CommitTx(h.ctx); err != nil {
						h.enqueueTxCommitErr(h.ps.ca.cID, err)
						h.l.Error("commit tx", "err", err)
						pool.Put(h.ps.ca.cID)
						h.ps.ca.cID, h.ps.state = nil, OP_START // We are keeping transaction opened here?
						continue
					}
					h.enqueueTxCommitSuccess(h.ps.ca.cID)
					h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterPub, h.writerID)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.currentTxWriter, h.sessionState, h.ps.state = nil, nil, SESSION_STATE_WRITER, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_ROLLBACK_TX:
			h.ps.ca.cID = pool.Get(Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_COMMIT_TX_CORRELATION_ID_ARG
		case OP_ROLLBACK_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= Uint32Len {
					if err := h.currentTxWriter.RollbackTx(h.ctx); err != nil {
						h.enqueueTxRollbackErr(h.ps.ca.cID, err)
						h.l.Error("rollback tx", "err", err)
						h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterPub, h.writerID)
						pool.Put(h.ps.ca.cID)
						// We are not keeping tx opened here after rollback error
						h.ps.ca.cID, h.currentTxWriter, h.sessionState, h.ps.state = nil, nil, SESSION_STATE_WRITER, OP_START
						continue
					}
					h.enqueueTxRollbackSuccess(h.ps.ca.cID)
					h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterPub, h.writerID)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.currentTxWriter, h.sessionState, h.ps.state = nil, nil, SESSION_STATE_WRITER, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_CONNECT_WRITER:
			h.ps.state = OP_CONNECT_WRITER_ARG
			h.ps.argBuf = pool.Get(Uint32Len)
			h.ps.argBuf = append(h.ps.argBuf, b)
		case OP_CONNECT_WRITER_ARG:
			if h.ps.cwa.writerIDlen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.cwa.writerIDlen))
				}

				toCopy := int(h.ps.cwa.writerIDlen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.cwa.writerIDlen) {
					h.ps.cwa.writerID = string(h.ps.argBuf)
					pool.Put(h.ps.argBuf)
					h.writerID = h.ps.cwa.writerID
					h.ps.argBuf, h.ps.cwa, h.ps.state = nil, connectWriterArgs{}, OP_START
				}

				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= Uint32Len {
				if h.ps.cwa.writerIDlen == 0 {
					h.ps.cwa.writerIDlen = binary.BigEndian.Uint32(h.ps.argBuf[0:Uint32Len])
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					if h.ps.cwa.writerIDlen == 0 {
						h.ps.argBuf, h.ps.cwa, h.ps.state = nil, connectWriterArgs{}, OP_START
					}
				}
			}
		case OP_CONNECT_READER:
			if err := h.parseReaderTypeArg(b); err != nil {
				h.ps.state = OP_START
				enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
				h.close()
				return fmt.Errorf("parse reader type arg: %w", err)
			}
			h.ps.argBuf = pool.Get(Uint32Len)
			h.ps.state = OP_CONNECT_READER_ARG
		case OP_CONNECT_READER_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= Uint32Len {
				if err := h.parseReaderSizeArg(); err != nil {
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.cra, h.ps.state = nil, connectReaderArgs{}, OP_START
					enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
					h.close()
					return fmt.Errorf("parse reader size arg: %w", err)
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_CONNECT_READER_PAYLOAD
			}
		case OP_CONNECT_READER_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.cra.size) - len(h.ps.payloadBuf)
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

				if len(h.ps.payloadBuf) >= int(h.ps.cra.size) {
					h.ps.state = OP_START
					sub := string(h.ps.payloadBuf)
					pool.Put(h.ps.payloadBuf)

					h.sessionState = SESSION_STATE_READER

					r, err := h.cman.GetReader(sub)
					if err != nil {
						return fmt.Errorf("get reader: %w", err)
					}

					h.sessionReaderMsgMetaLen = int(r.MessageMetaLen())
					readerType := reader.ReaderType(h.ps.cra.typ)

					enqueueConnectSuccess(h.out, r)
					h.sessionReader = r

					h.wg.Add(1)
					go func() {
						defer h.wg.Done()
						if err := h.connectReader(r, readerType); err != nil {
							enqueueConnectReaderErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_YES, err)
							h.close()
							h.l.Error("subscribe", "err", err)
							return
						}

						h.stopRead = true
					}()

					h.ps.payloadBuf, h.ps.cra, h.ps.state = nil, connectReaderArgs{}, OP_START
					continue
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.cra.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
			}
		default:
			h.close()
			return ErrParseProto
		}
	}

	return nil
}

func (h *handler) connectReader(r reader.Reader, typ reader.ReaderType) error {
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()

	h.disconnect = func() {
		r.Close()
		cancel()
		h.out.enqueueProto(response.DISCONNECT_RESP)
	}

	constLen := h.sessionReaderMsgMetaLen + 5
	h.msgHandler = enqueueMsgFunc(h.out, r, constLen)

	switch typ {
	case reader.Subscriber:
		return r.Subscribe(ctx, func(message []byte, args ...any) {
			h.msgHandler(message, args...)
		})
	case reader.Consumer:
		<-ctx.Done()
		return nil
	default:
		return fmt.Errorf("invalid reader type: %d", typ)
	}
}
func (h *handler) produce(msg []byte) {
	buf := pool.Get(6) // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
	successResp := server.WriteResponseSuccess(buf, h.ps.ca.cID)
	h.nonTxSessionWriters[h.ps.wa.topic].Write(h.ctx, msg, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write", "err", err)

			successResp[5] = response.ERR_CODE_YES
			errProtoBuf := errProtoBuf(err)
			h.out.enqueueProtoMulti(successResp, errProtoBuf)
			pool.Put(errProtoBuf)
			pool.Put(buf)
			return
		}
		h.out.enqueueProto(successResp)
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
			h.out.enqueueProtoMulti(successResp, errProtoBuf)
			pool.Put(errProtoBuf)
			pool.Put(buf)
			return
		}
		h.out.enqueueProto(successResp)
		pool.Put(buf)
	})
}

func (h *handler) fetch(val uint32) {
	buf := pool.Get(10)[:10]
	buf[0] = byte(response.RESP_CODE_FETCH)
	replaceUnsafe(buf, 1, h.ps.ca.cID)

	go func() {
		if val == 0 {
			buf[9] = 1
			h.out.enqueueProtoMulti(buf, errProtoBuf(ErrFetchArgNotProvided))
			pool.Put(buf)
			return
		}

		if err := h.sessionReader.Fetch(h.ctx, val,
			func(n uint32) {
				replaceUnsafe(buf, 5, binary.BigEndian.AppendUint32(nil, n))
				h.out.enqueueProto(buf)
				pool.Put(buf)
			},
			func(message []byte, args ...any) {
				h.msgHandler(message, args...)
			},
		); err != nil {
			buf[9] = 1
			h.out.enqueueProtoMulti(buf, errProtoBuf(err))
			pool.Put(buf)
		}
	}()
}

func (h *handler) close() {
	h.stopRead = true
	h.disconnect()
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
	close(h.closed)
}

func (h *handler) parseProducePubLenArg() error {
	h.ps.wa.topicLen = binary.BigEndian.Uint32(h.ps.argBuf[0:Uint32Len])
	if h.ps.wa.topicLen == 0 {
		return errors.New("publish cmd pub len arg not provided")
	}

	return nil
}

func (h *handler) parseProducePubArg() error {
	h.ps.wa.topic = string(h.ps.argBuf)
	if h.ps.wa.topic == "" {
		return errors.New("publish cmd pub arg is empty")
	}

	return nil
}

func (h *handler) parseProduceMsgSizeArg() error {
	h.ps.wma.size = binary.BigEndian.Uint32(h.ps.argBuf[0:Uint32Len])
	if h.ps.wma.size == 0 {
		return errors.New("publish msg cmd size arg not provided")
	}

	return nil
}

func (h *handler) parseReaderSizeArg() error {
	h.ps.cra.size = binary.BigEndian.Uint32(h.ps.argBuf[0:Uint32Len])
	if h.ps.cra.size == 0 {
		return errors.New("reader size arg not provided")
	}

	return nil
}

func (h *handler) parseReaderTypeArg(b byte) error {
	if b != 1 && b != 2 {
		return errors.New("invalid reader type")
	}
	h.ps.cra.typ = b
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
	h.out.enqueueProto(buf)
	pool.Put(buf)
}

func (h *handler) enqueueStop() {
	h.out.enqueueProto(request.STOP_REQ)
}

func (h *handler) enqueueAckSuccess(cID []byte) {
	replaceUnsafe(h.ackSuccessRespTemplate, 1, cID)
	h.out.enqueueProto(h.ackSuccessRespTemplate)
}

func (h *handler) enqueueAckErr(cID []byte, err error) {
	replaceUnsafe(h.ackErrRespTemplate, 1, cID)
	h.out.enqueueProtoMulti(h.ackErrRespTemplate, errProtoBuf(err))
}

func (h *handler) enqueueNAckSuccess(cID []byte) {
	replaceUnsafe(h.nAckSuccessRespTemplate, 1, cID)
	h.out.enqueueProto(h.nAckSuccessRespTemplate)
}

func (h *handler) enqueueNAckErr(cID []byte, err error) {
	replaceUnsafe(h.nAckErrRespTemplate, 1, cID)
	h.out.enqueueProtoMulti(h.nAckErrRespTemplate, errProtoBuf(err))
}

func (h *handler) enqueueTxBeginSuccess(cID []byte) {
	replaceUnsafe(h.txBeginSuccessRespTemplate, 1, cID)
	h.out.enqueueProto(h.txBeginSuccessRespTemplate)
}

func (h *handler) enqueueTxBeginErr(cID []byte, err error) {
	replaceUnsafe(h.txBeginErrRespTemplate, 1, cID)
	h.out.enqueueProtoMulti(h.txBeginErrRespTemplate, errProtoBuf(err))
}

func (h *handler) enqueueTxCommitSuccess(cID []byte) {
	replaceUnsafe(h.txCommitSuccessRespTemplate, 1, cID)
	h.out.enqueueProto(h.txCommitSuccessRespTemplate)
}

func (h *handler) enqueueTxCommitErr(cID []byte, err error) {
	replaceUnsafe(h.txBeginErrRespTemplate, 1, cID)
	h.out.enqueueProtoMulti(h.txBeginErrRespTemplate, errProtoBuf(err))
}

func (h *handler) enqueueTxRollbackSuccess(cID []byte) {
	replaceUnsafe(h.txRollbackSuccessRespTemplate, 1, cID)
	h.out.enqueueProto(h.txRollbackSuccessRespTemplate)
}

func (h *handler) enqueueTxRollbackErr(cID []byte, err error) {
	replaceUnsafe(h.txRollbackErrRespTemplate, 1, cID)
	h.out.enqueueProtoMulti(h.txRollbackErrRespTemplate, errProtoBuf(err))
}

func enqueueConnectSuccess(out *outbound, r reader.Reader) {
	var autoCommit byte
	if r.IsAutoCommit() {
		autoCommit = 1
	}

	sbuf := pool.Get(Uint32Len)
	defer pool.Put(sbuf)
	sbuf = append(sbuf,
		byte(response.RESP_CODE_CONNECT_READER),
		autoCommit, byte(r.MessageMetaLen()),
		byte(response.ERR_CODE_NO),
	)
	out.enqueueProto(sbuf)
}

func enqueueMsgFunc(out *outbound, r reader.Reader, constLen int) func(message []byte, args ...any) {
	if constLen == 5 { // This means msg ID len == 0, and we do not need to encode it, as consumer is already aware of it
		return func(message []byte, args ...any) {
			buf := pool.Get(len(message) + constLen)
			defer pool.Put(buf)
			buf = append(buf, byte(response.RESP_CODE_MSG))
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
			buf = append(buf, message...)
			out.enqueueProto(buf)
		}
	}

	return func(message []byte, args ...any) {
		buf := pool.Get(len(message) + constLen)
		defer pool.Put(buf)
		buf = append(buf, byte(response.RESP_CODE_MSG))
		buf = r.EncodeMeta(buf, args...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(message)))
		buf = append(buf, message...)
		out.enqueueProto(buf)
	}
}

func enqueueConnectReaderErr(out *outbound, respCode response.RespCode, errCode response.ErrCode, err error) {
	errPayload := err.Error()
	errLen := len(errPayload)
	buf := pool.Get(8 + errLen) // cmd (1) + auto commit (1) + msg meta len (1) + err code (1) + err len (4) + err payload (errLen)
	buf = append(buf, byte(respCode), 0, 0, byte(errCode))
	buf = binary.BigEndian.AppendUint32(buf, uint32(errLen))
	buf = append(buf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
	out.enqueueProto(buf)
	pool.Put(buf)
}

func errProtoBuf(err error) []byte {
	errPayload := err.Error()
	errLen := len(errPayload)
	errBuf := pool.Get(Uint32Len + errLen) // err len + err payload
	errBuf = binary.BigEndian.AppendUint32(errBuf, uint32(errLen))
	return append(errBuf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
}

func replaceUnsafe(slice []byte, start int, new []byte) {
	ptr := unsafe.Pointer(&slice[start])
	dst := (*[1 << 30]byte)(ptr)
	copy(dst[:len(new)], new)
}
