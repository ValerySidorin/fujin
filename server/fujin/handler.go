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

	OP_CONNECT_PRODUCER
	OP_CONNECT_PRODUCER_ARG
	OP_PRODUCE
	OP_PRODUCE_REQUEST_ID_ARG
	OP_PRODUCE_ARG
	OP_PRODUCE_MSG_ARG
	OP_PRODUCE_MSG_PAYLOAD

	OP_CONNECT_SUBSCRIBER
	OP_CONNECT_SUBSCRIBER_ARG
	OP_CONNECT_SUBSCRIBER_PAYLOAD

	OP_CONNECT_CONSUMER
	OP_CONNECT_CONSUMER_ARG
	OP_CONSUME_TRIGGER

	OP_ACK
	OP_ACK_REQUEST_ID_ARG
	OP_ACK_ARG

	OP_NACK
	OP_NACK_REQUEST_ID_ARG
	OP_NACK_ARG

	OP_BEGIN_TX
	OP_BEGIN_TX_REQUEST_ID_ARG

	OP_COMMIT_TX
	OP_COMMIT_TX_REQUEST_ID_ARG

	OP_ROLLBACK_TX
	OP_ROLLBACK_TX_REQUEST_ID_ARG

	OP_PRODUCE_TX
	OP_PRODUCE_TX_REQUEST_ID_ARG
	OP_PRODUCE_TX_ARG
	OP_PRODUCE_TX_MSG_ARG
	OP_PRODUCE_TX_MSG_PAYLOAD
)

var (
	ErrParseProto               = errors.New("parse proto")
	ErrWriterCanNotBeReusedInTx = errors.New("writer can not be reuse in tx")
)

type sessionState byte

const (
	SESSION_STATE_UNKNOWN sessionState = iota
	SESSION_STATE_PRODUCER
	SESSION_STATE_SUBSCRIBER
	SESSION_STATE_CONSUMER
	SESSION_STATE_TX
)

type parseState struct {
	state      int
	argBuf     []byte
	payloadBuf []byte

	ca correlationIDArg

	cpa connectProducerArgs
	pa  publishArgs
	pma publishMsgArgs

	cca connectConsumerArgs
	csa connectSubscriberArgs
}

type correlationIDArg struct {
	cID []byte
}

type publishArgs struct {
	pubLen uint32
	pub    string
}

type connectConsumerArgs struct {
	n      uint32
	subLen uint32
	sub    string
}

type publishMsgArgs struct {
	size uint32
}

type connectSubscriberArgs struct {
	size uint32
}

type connectProducerArgs struct {
	producerIDlen uint32
	producerID    string
}

type handler struct {
	ctx  context.Context
	out  *outbound
	cman *connector.Manager

	ps           *parseState
	sessionState sessionState

	// producer
	producerID             string
	nonTxSessionWriters    map[string]writer.Writer
	currentTxWriter        writer.Writer
	currentTxWriterPub     string
	txBeginRespTemplate    []byte
	txCommitRespTemplate   []byte
	txRollbackRespTemplate []byte

	// consumer/subscriber
	sessionReader           reader.Reader
	sessionReaderMsgMetaLen int
	consumeTrigger          func()
	ackRespTemplate         []byte
	nAckRespTemplate        []byte

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

		txBeginRespTemplate:    []byte{byte(response.RESP_CODE_TX_BEGIN), 0, 0, 0, 0, 0},    // 4 bytes for request id, 1 byte for success/failure
		txCommitRespTemplate:   []byte{byte(response.RESP_CODE_TX_COMMIT), 0, 0, 0, 0, 0},   // 4 bytes for request id, 1 byte for success/failure
		txRollbackRespTemplate: []byte{byte(response.RESP_CODE_TX_ROLLBACK), 0, 0, 0, 0, 0}, // 4 bytes for request id, 1 byte for success/failure
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
				case byte(request.OP_CODE_CONNECT_PRODUCER):
					h.sessionState = SESSION_STATE_PRODUCER
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
							h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterPub, h.producerID)
							h.currentTxWriter = nil
						}
						h.out.enqueueProto(response.DISCONNECT_RESP)
					}
					h.ps.state = OP_CONNECT_PRODUCER
				case byte(request.OP_CODE_CONNECT_CONSUMER):
					h.ps.state = OP_CONNECT_CONSUMER
				case byte(request.OP_CODE_CONNECT_SUBSCRIBER):
					h.ps.state = OP_CONNECT_SUBSCRIBER
				default:
					h.close()
					return ErrParseProto
				}
			case SESSION_STATE_PRODUCER:
				switch b {
				case byte(request.OP_CODE_PRODUCE):
					h.ps.state = OP_PRODUCE
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
				case byte(request.OP_CODE_PRODUCE):
					h.ps.state = OP_PRODUCE_TX
				case byte(request.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX
				case byte(request.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX
				case byte(request.OP_CODE_DISCONNECT):
					h.close()
					return nil
				}
			case SESSION_STATE_CONSUMER:
				switch b {
				case byte(request.OP_CODE_CONSUME):
					h.ps.state = OP_CONSUME_TRIGGER
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
			case SESSION_STATE_SUBSCRIBER:
				switch b {
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
		case OP_PRODUCE:
			h.ps.ca.cID = pool.Get(4)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_REQUEST_ID_ARG
		case OP_PRODUCE_REQUEST_ID_ARG:
			toCopy := 4 - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= 4 {
				h.ps.argBuf = pool.Get(4)
				h.ps.state = OP_PRODUCE_ARG
			}
		case OP_PRODUCE_ARG:
			if h.ps.pa.pubLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.pubLen))
				}

				toCopy := int(h.ps.pa.pubLen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.pa.pubLen) {
					if err := h.parseProducePubArg(); err != nil {
						h.l.Error("parse produce pub arg", "err", err)
						h.enqueueProduceErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
						continue
					}
					pool.Put(h.ps.argBuf)

					if _, ok := h.nonTxSessionWriters[h.ps.pa.pub]; !ok {
						w, err := h.cman.GetWriter(h.ps.pa.pub, "")
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueProduceErrResponse(err)
							pool.Put(h.ps.argBuf)
							pool.Put(h.ps.ca.cID)
							h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
							continue
						}

						h.nonTxSessionWriters[h.ps.pa.pub] = w
					}
					h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= 4 {
				if h.ps.pa.pubLen == 0 {
					if err := h.parseProducePubLenArg(); err != nil {
						h.l.Error("parse produce pub len arg", "err", err)
						h.enqueueProduceErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
						continue
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}
				pool.Put(h.ps.argBuf)
				h.enqueueProduceErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
				continue
			}
		case OP_PRODUCE_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(4)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= 4 {
				if err := h.parseProduceMsgSizeArg(); err != nil {
					h.l.Error("parse produce msg size arg", "err", err)
					h.enqueueProduceErrResponse(err)
					pool.Put(h.ps.argBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
					continue
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
					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, nil, publishArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					h.produce(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, nil, publishArgs{}, OP_START
				}
			}
		case OP_CONSUME_TRIGGER:
			h.consumeTrigger()
			h.ps.state = OP_START
		case OP_ACK:
			h.ps.ca.cID = pool.Get(4)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ACK_REQUEST_ID_ARG
		case OP_ACK_REQUEST_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := 4 - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= 4 {
					h.ps.argBuf = pool.Get(h.sessionReaderMsgMetaLen)
					h.ps.state = OP_ACK_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(4)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_ACK_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= h.sessionReaderMsgMetaLen {
				// TODO: Handle ack errors properly
				if err := h.sessionReader.Ack(h.ctx, h.ps.argBuf); err != nil {
					h.l.Error("ack", "err", err)
					h.enqueueAckResp(h.ps.argBuf, h.ps.ca.cID, 0)
					pool.Put(h.ps.argBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
					continue
				}
				h.enqueueAckResp(h.ps.argBuf, h.ps.ca.cID, 1)
				pool.Put(h.ps.argBuf)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
			}
		case OP_NACK:
			h.ps.ca.cID = pool.Get(4)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ACK_REQUEST_ID_ARG
		case OP_NACK_REQUEST_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := 4 - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= 4 {
					h.ps.argBuf = pool.Get(h.sessionReaderMsgMetaLen)
					h.ps.state = OP_NACK_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(4)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_NACK_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= h.sessionReaderMsgMetaLen {
				// TODO: Handle nack errors properly
				if err := h.sessionReader.Nack(h.ctx, h.ps.argBuf); err != nil {
					h.l.Error("ack", "err", err)
					h.enqueueNAckResp(h.ps.argBuf, h.ps.ca.cID, 0)
					pool.Put(h.ps.argBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
					continue
				}
				h.enqueueNAckResp(h.ps.argBuf, h.ps.ca.cID, 1)
				pool.Put(h.ps.argBuf)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.state = nil, nil, OP_START
			}
		case OP_PRODUCE_TX:
			h.ps.ca.cID = pool.Get(4)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_TX_REQUEST_ID_ARG
		case OP_PRODUCE_TX_REQUEST_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := 4 - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= 4 {
					h.ps.argBuf = pool.Get(4)
					h.ps.state = OP_PRODUCE_TX_ARG
				}
			} else {
				h.ps.ca.cID = pool.Get(4)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_PRODUCE_TX_ARG:
			if h.ps.pa.pubLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.pubLen))
				}

				toCopy := int(h.ps.pa.pubLen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.pa.pubLen) {
					if err := h.parseProducePubArg(); err != nil {
						h.l.Error("parse produce pub arg", "err", err)
						h.enqueueProduceErrResponse(err)
						pool.Put(h.ps.argBuf)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
						continue
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil

					if h.currentTxWriter != nil {
						if !h.cman.WriterCanBeReusedInTx(h.currentTxWriter, h.ps.pa.pub) {
							h.l.Error("writer can not be reused in tx")
							h.enqueueProduceErrResponse(ErrWriterCanNotBeReusedInTx)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.pa, h.ps.state = nil, publishArgs{}, OP_START
							continue
						}
					} else {
						var err error // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
						h.currentTxWriter, err = h.cman.GetWriter(h.ps.pa.pub, h.producerID)
						if err != nil {
							h.l.Error("get writer", "err", err)
							h.enqueueProduceErrResponse(err)
							pool.Put(h.ps.ca.cID)
							h.ps.ca.cID, h.ps.pa, h.ps.state = nil, publishArgs{}, OP_START
							continue
						}

						if err := h.currentTxWriter.BeginTx(h.ctx); err != nil {
							h.l.Error("begin tx", "err", err)
							h.enqueueProduceErrResponse(err)
							pool.Put(h.ps.ca.cID)
							continue
						}

						h.currentTxWriterPub = h.ps.pa.pub
					}

					h.ps.argBuf, h.ps.state = nil, OP_PRODUCE_TX_MSG_ARG
				}
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= 4 {
				if h.ps.pa.pubLen == 0 {
					if err := h.parseProducePubLenArg(); err != nil {
						h.l.Error("parse produce pub len arg", "err", err)
						pool.Put(h.ps.argBuf)
						h.enqueueProduceErrResponse(err)
						pool.Put(h.ps.ca.cID)
						h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
						continue
					}
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					continue
				}

				// this should not happen ever
				pool.Put(h.ps.argBuf)
				h.enqueueProduceErrResponse(ErrParseProto)
				pool.Put(h.ps.ca.cID)
				h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
				continue
			}
		case OP_PRODUCE_TX_MSG_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(4)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= 4 {
				if err := h.parseProduceMsgSizeArg(); err != nil {
					h.l.Error("parse produce msg size arg", "err", err)
					pool.Put(h.ps.argBuf)
					h.enqueueProduceErrResponse(err)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.pa, h.ps.state = nil, nil, publishArgs{}, OP_START
					continue
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
					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, nil, publishArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)

				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					h.produceTx(h.ps.payloadBuf)
					pool.Put(h.ps.ca.cID)
					h.ps.argBuf, h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, nil, publishArgs{}, OP_START
				}
			}
		case OP_BEGIN_TX:
			h.ps.ca.cID = pool.Get(4)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_BEGIN_TX_REQUEST_ID_ARG
		case OP_BEGIN_TX_REQUEST_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := 4 - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= 4 {
					for _, sw := range h.nonTxSessionWriters {
						// TODO: on flush err return fail
						if err := sw.Flush(h.ctx); err != nil {
							h.l.Error("flush", "err", err)
						}
					}
					// TODO: Refactor
					h.txBeginRespTemplate = h.txBeginRespTemplate[:1]
					h.txBeginRespTemplate = append(h.txBeginRespTemplate, h.ps.ca.cID...)
					h.txBeginRespTemplate = append(h.txBeginRespTemplate, 0)
					h.out.enqueueProto(h.txBeginRespTemplate)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.sessionState, h.ps.state = nil, SESSION_STATE_TX, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(4)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_COMMIT_TX:
			h.ps.ca.cID = pool.Get(4)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_COMMIT_TX_REQUEST_ID_ARG
		case OP_COMMIT_TX_REQUEST_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := 4 - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= 4 {
					h.txCommitRespTemplate = h.txCommitRespTemplate[:1]
					h.txCommitRespTemplate = append(h.txCommitRespTemplate, h.ps.ca.cID...)
					if err := h.currentTxWriter.CommitTx(h.ctx); err != nil {
						h.l.Error("commit tx", "err", err)
						h.txCommitRespTemplate = append(h.txCommitRespTemplate, 1)
						h.out.enqueueProto(h.txCommitRespTemplate)
						pool.Put(h.ps.ca.cID)
						h.ps.ca.cID, h.ps.state = nil, OP_START
						continue
					}
					h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterPub, h.producerID)
					h.txCommitRespTemplate = append(h.txCommitRespTemplate, 0)
					h.out.enqueueProto(h.txCommitRespTemplate)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.currentTxWriter, h.sessionState, h.ps.state = nil, nil, SESSION_STATE_PRODUCER, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(4)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_ROLLBACK_TX:
			h.ps.ca.cID = pool.Get(4)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_COMMIT_TX_REQUEST_ID_ARG
		case OP_ROLLBACK_TX_REQUEST_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := 4 - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= 4 {
					h.txRollbackRespTemplate = h.txRollbackRespTemplate[:1]
					h.txRollbackRespTemplate = append(h.txRollbackRespTemplate, h.ps.ca.cID...)
					if err := h.currentTxWriter.CommitTx(h.ctx); err != nil {
						h.l.Error("rollback tx", "err", err)
						h.txRollbackRespTemplate = append(h.txRollbackRespTemplate, 1)
						h.out.enqueueProto(h.txRollbackRespTemplate)
						pool.Put(h.ps.ca.cID)
						h.ps.ca.cID, h.ps.state = nil, OP_START
						continue
					}
					h.cman.PutWriter(h.currentTxWriter, h.currentTxWriterPub, h.producerID)
					h.txRollbackRespTemplate = append(h.txRollbackRespTemplate, 0)
					h.out.enqueueProto(h.txRollbackRespTemplate)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.currentTxWriter, h.sessionState, h.ps.state = nil, nil, SESSION_STATE_PRODUCER, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(4)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_CONNECT_PRODUCER:
			h.ps.state = OP_CONNECT_PRODUCER_ARG
			h.ps.argBuf = pool.Get(4)
			h.ps.argBuf = append(h.ps.argBuf, b)
		case OP_CONNECT_PRODUCER_ARG:
			if h.ps.cpa.producerIDlen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.cpa.producerIDlen))
				}

				toCopy := int(h.ps.cpa.producerIDlen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.cpa.producerIDlen) {
					h.ps.cpa.producerID = string(h.ps.argBuf)
					pool.Put(h.ps.argBuf)
					h.producerID = h.ps.cpa.producerID
					h.ps.argBuf, h.ps.cpa, h.ps.state = nil, connectProducerArgs{}, OP_START
				}

				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= 4 {
				if h.ps.cpa.producerIDlen == 0 {
					h.ps.cpa.producerIDlen = binary.BigEndian.Uint32(h.ps.argBuf[0:4])
					pool.Put(h.ps.argBuf)
					h.ps.argBuf = nil
					if h.ps.cpa.producerIDlen == 0 {
						h.ps.argBuf, h.ps.cpa, h.ps.state = nil, connectProducerArgs{}, OP_START
					}
				}
			}
		case OP_CONNECT_CONSUMER:
			h.ps.state = OP_CONNECT_CONSUMER_ARG
			h.ps.argBuf = pool.Get(4)
			h.ps.argBuf = append(h.ps.argBuf, b)
		case OP_CONNECT_CONSUMER_ARG:
			if h.ps.cca.subLen != 0 {
				h.ps.argBuf = pool.Get(int(h.ps.cca.subLen))
				toCopy := int(h.ps.cca.subLen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.cca.subLen) {
					if err := h.parseConsumePubArg(); err != nil {
						pool.Put(h.ps.argBuf)
						h.ps.argBuf, h.ps.cca, h.ps.state = nil, connectConsumerArgs{}, OP_START
						return fmt.Errorf("parse consume pub arg: %w", err)
					}
					pool.Put(h.ps.argBuf)
				}

				h.sessionState = SESSION_STATE_CONSUMER

				r, err := h.cman.GetReader(h.ps.cca.sub)
				if err != nil {
					return fmt.Errorf("get reader: %w", err)
				}

				h.sessionReaderMsgMetaLen = int(r.MessageMetaLen())

				h.wg.Add(1)
				go func() {
					defer h.wg.Done()
					if err := h.connectConsumer(r, h.ps.cca.n); err != nil {
						enqueueErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_CONNECT_CONSUMER, err)
						h.close()
						h.l.Error("consume", "err", err)
						return
					}

					h.stopRead = true
				}()

				h.ps.argBuf, h.ps.cca, h.ps.state = nil, connectConsumerArgs{}, OP_START
				continue
			}

			h.ps.argBuf = append(h.ps.argBuf, b)

			if len(h.ps.argBuf) >= 4 {
				if h.ps.cca.n == 0 {
					if err := h.parseConsumeNArg(); err != nil {
						enqueueErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_CONNECT_CONSUMER, err)
						h.close()
						return fmt.Errorf("parse consume n arg: %w", err)
					}
					h.ps.argBuf = h.ps.argBuf[:0]
					continue
				}

				if h.ps.cca.subLen == 0 {
					if err := h.parseConsumePubLenArg(); err != nil {
						pool.Put(h.ps.argBuf)
						h.ps.argBuf, h.ps.cca, h.ps.state = nil, connectConsumerArgs{}, OP_START
						enqueueErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_CONNECT_CONSUMER, err)
						h.close()
						return fmt.Errorf("parse consume pub len arg: %w", err)
					}
					pool.Put(h.ps.argBuf)
					continue
				}

				h.ps.argBuf, h.ps.cca, h.ps.state = nil, connectConsumerArgs{}, OP_START
				h.close()
				return ErrParseProto
			}
		case OP_CONNECT_SUBSCRIBER:
			h.ps.state = OP_CONNECT_SUBSCRIBER_ARG
			h.ps.argBuf = pool.Get(4)
			h.ps.argBuf = append(h.ps.argBuf, b)
		case OP_CONNECT_SUBSCRIBER_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= 4 {
				if err := h.parseSubscribeSizeArg(); err != nil {
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.csa, h.ps.state = nil, connectSubscriberArgs{}, OP_START
					enqueueErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_CONNECT_SUBSCRIBER, err)
					h.close()
					return fmt.Errorf("parse subscribe size arg: %w", err)
				}
				pool.Put(h.ps.argBuf)
				h.ps.argBuf, h.ps.state = nil, OP_CONNECT_SUBSCRIBER_PAYLOAD
			}
		case OP_CONNECT_SUBSCRIBER_PAYLOAD:
			if h.ps.payloadBuf != nil {
				toCopy := int(h.ps.csa.size) - len(h.ps.payloadBuf)
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

				if len(h.ps.payloadBuf) >= int(h.ps.csa.size) {
					h.ps.state = OP_START
					sub := string(h.ps.payloadBuf)
					pool.Put(h.ps.payloadBuf)

					h.sessionState = SESSION_STATE_SUBSCRIBER

					r, err := h.cman.GetReader(sub)
					if err != nil {
						return fmt.Errorf("get reader: %w", err)
					}

					h.sessionReaderMsgMetaLen = int(r.MessageMetaLen())

					h.wg.Add(1)
					go func() {
						defer h.wg.Done()
						if err := h.connectSubscriber(r); err != nil {
							enqueueErr(h.out, response.RESP_CODE_CONNECT_READER, response.ERR_CODE_CONNECT_SUBSCRIBER, err)
							h.close()
							h.l.Error("subscribe", "err", err)
							return
						}

						h.stopRead = true
					}()

					h.ps.payloadBuf, h.ps.cca, h.ps.state = nil, connectConsumerArgs{}, OP_START
					continue
				}
			} else {
				h.ps.payloadBuf = pool.Get(int(h.ps.csa.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
			}
		default:
			h.close()
			return ErrParseProto
		}
	}

	return nil
}

func (h *handler) connectSubscriber(r reader.Reader) error {
	ctx, cancel := context.WithCancel(h.ctx)

	h.disconnect = func() {
		r.Close()
		cancel()
		h.out.enqueueProto(response.DISCONNECT_RESP)
	}

	return h.subscribe(ctx, h.out, r)
}

func (h *handler) connectConsumer(r reader.Reader, n uint32) error {
	ch := make(chan struct{}, 1)

	h.consumeTrigger = func() {
		ch <- struct{}{}
	}

	ctx, cancel := context.WithCancel(h.ctx)

	h.disconnect = func() {
		r.Close()
		cancel()
		h.out.enqueueProto(response.DISCONNECT_RESP)
	}

	return h.consume(ctx, h.out, r, ch, n)
}

func (h *handler) subscribe(ctx context.Context, out *outbound, r reader.Reader) error {
	enqueueConnectSuccess(out, r)
	h.sessionReader = r
	constLen := h.sessionReaderMsgMetaLen + 5
	handler := enqueueMsgFunc(out, r, constLen)
	if err := r.Subscribe(ctx, func(message []byte, args ...any) error {
		handler(message, args...)
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (h *handler) consume(ctx context.Context, out *outbound, r reader.Reader, ch <-chan struct{}, n uint32) error {
	enqueueConnectSuccess(out, r)
	h.sessionReader = r

	h.ackRespTemplate = make([]byte, 0, h.sessionReaderMsgMetaLen+6)
	h.ackRespTemplate = append(h.ackRespTemplate, byte(response.RESP_CODE_ACK))

	h.nAckRespTemplate = make([]byte, 0, h.sessionReaderMsgMetaLen+6)
	h.nAckRespTemplate = append(h.nAckRespTemplate, byte(response.RESP_CODE_NACK))

	constLen := h.sessionReaderMsgMetaLen + 5
	handler := enqueueMsgFunc(out, r, constLen)
	if err := r.Consume(ctx, ch, n, func(message []byte, args ...any) error {
		handler(message, args...)
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (h *handler) produce(msg []byte) {
	buf := pool.Get(6) // 1 byte (resp op code) + 4 bytes (request id) + 1 byte (success/failure)
	successResp := server.ProduceResponseSuccess(buf, h.ps.ca.cID)
	h.nonTxSessionWriters[h.ps.pa.pub].Write(h.ctx, msg, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write", "err", err)

			successResp[5] = response.ERR_CODE_PRODUCE
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
	successResp := server.ProduceResponseSuccess(buf, h.ps.ca.cID)
	h.currentTxWriter.Write(h.ctx, msg, func(err error) {
		pool.Put(msg)
		if err != nil {
			h.l.Error("write", "err", err)

			successResp[5] = response.ERR_CODE_PRODUCE
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
	h.ps.pa.pubLen = binary.BigEndian.Uint32(h.ps.argBuf[0:4])
	if h.ps.pa.pubLen == 0 {
		return errors.New("publish cmd pub len arg not provided")
	}

	return nil
}

func (h *handler) parseProducePubArg() error {
	h.ps.pa.pub = string(h.ps.argBuf)
	if h.ps.pa.pub == "" {
		return errors.New("publish cmd pub arg is empty")
	}

	return nil
}

func (h *handler) parseProduceMsgSizeArg() error {
	h.ps.pma.size = binary.BigEndian.Uint32(h.ps.argBuf[0:4])
	if h.ps.pma.size == 0 {
		return errors.New("publish msg cmd size arg not provided")
	}

	return nil
}

func (h *handler) parseConsumePubLenArg() error {
	h.ps.cca.subLen = binary.BigEndian.Uint32(h.ps.argBuf[0:4])
	if h.ps.cca.subLen == 0 {
		return errors.New("publish cmd pub len arg not provided")
	}

	return nil
}

func (h *handler) parseConsumePubArg() error {
	h.ps.cca.sub = string(h.ps.argBuf)
	if h.ps.cca.sub == "" {
		return errors.New("publish cmd pub arg is empty")
	}

	return nil
}

func (h *handler) parseConsumeNArg() error {
	h.ps.cca.n = binary.BigEndian.Uint32(h.ps.argBuf[0:4])
	if h.ps.cca.n == 0 {
		return errors.New("publish cmd n arg not provided")
	}

	return nil
}

func (h *handler) parseSubscribeSizeArg() error {
	h.ps.csa.size = binary.BigEndian.Uint32(h.ps.argBuf[0:4])
	if h.ps.csa.size == 0 {
		return errors.New("subscribe cmd size arg not provided")
	}

	return nil
}

func (h *handler) enqueueProduceErrResponse(err error) {
	errPayload := err.Error()
	errLen := len(errPayload)
	buf := pool.Get(10 + errLen) // resp code produce (1) + request id (4) + err code (1) + err len (4)
	buf = append(buf, byte(response.RESP_CODE_PRODUCE))
	buf = append(buf, h.ps.ca.cID...)
	buf = append(buf, response.ERR_CODE_PRODUCE)
	buf = binary.BigEndian.AppendUint32(buf, uint32(errLen))
	buf = append(buf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
	h.out.enqueueProto(buf)
	pool.Put(buf)
}

func (h *handler) enqueueStop() {
	h.out.enqueueProto(response.STOP_RESP)
}

func (h *handler) enqueueAckResp(meta, rID []byte, success byte) {
	h.ackRespTemplate = append(h.ackRespTemplate, success)
	h.ackRespTemplate = append(h.ackRespTemplate, rID...)
	h.ackRespTemplate = append(h.ackRespTemplate, meta...)
	h.out.enqueueProto(h.ackRespTemplate)
	h.ackRespTemplate = h.ackRespTemplate[:1]
}

func (h *handler) enqueueNAckResp(meta, rID []byte, success byte) {
	h.nAckRespTemplate = append(h.nAckRespTemplate, success)
	h.nAckRespTemplate = append(h.nAckRespTemplate, rID...)
	h.nAckRespTemplate = append(h.nAckRespTemplate, meta...)
	h.out.enqueueProto(h.nAckRespTemplate)
	h.nAckRespTemplate = h.nAckRespTemplate[:1]
}

func enqueueConnectSuccess(out *outbound, r reader.Reader) {
	var autoCommit byte
	if r.IsAutoCommit() {
		autoCommit = 1
	}

	sbuf := pool.Get(4)
	sbuf = append(sbuf,
		byte(response.RESP_CODE_CONNECT_READER), byte(response.ERR_CODE_NO),
		autoCommit, byte(r.MessageMetaLen()),
	)
	out.enqueueProto(sbuf)
	pool.Put(sbuf)
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

func enqueueErr(out *outbound, respCode response.RespCode, errCode response.ErrCode, err error) {
	errPayload := err.Error()
	errLen := len(errPayload)
	buf := pool.Get(6 + errLen) // cmd + err code + err len + err payload
	buf = append(buf, byte(respCode), byte(errCode))
	buf = binary.BigEndian.AppendUint32(buf, uint32(errLen))
	buf = append(buf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
	out.enqueueProto(buf)
	pool.Put(buf)
}

func errProtoBuf(err error) []byte {
	errPayload := err.Error()
	errLen := len(errPayload)
	errBuf := pool.Get(4 + errLen) // err len + err payload
	errBuf = binary.BigEndian.AppendUint32(errBuf, uint32(errLen))
	return append(errBuf,
		unsafe.Slice((*byte)(unsafe.Pointer((*[2]uintptr)(unsafe.Pointer(&errPayload))[0])), len(errPayload))...)
}
