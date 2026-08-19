package proto

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/fujin-io/fujin/internal/core"
	"github.com/fujin-io/fujin/internal/proto/pool"
	"github.com/fujin-io/fujin/public/plugins/connector"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
)

const (
	OP_START int = iota

	OP_BIND
	OP_BIND_CONNECTOR_NAME_LEN
	OP_BIND_CONNECTOR_NAME_PAYLOAD
	OP_BIND_META_COUNT
	OP_BIND_META_KEY_LEN
	OP_BIND_META_KEY
	OP_BIND_META_VALUE_LEN
	OP_BIND_META_VALUE
	OP_BIND_CONFIG_OVERRIDES_COUNT
	OP_BIND_CONFIG_OVERRIDE_KEY_LEN
	OP_BIND_CONFIG_OVERRIDE_KEY
	OP_BIND_CONFIG_OVERRIDE_VALUE_LEN
	OP_BIND_CONFIG_OVERRIDE_VALUE

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

	OP_TX_PRODUCE
	OP_TX_PRODUCE_CORRELATION_ID_ARG
	OP_TX_PRODUCE_H
	OP_TX_PRODUCE_H_CORRELATION_ID_ARG

	OP_SUBSCRIBE
	OP_SUBSCRIBE_CORRELATION_ID_ARG
	OP_SUBSCRIBE_AUTO_COMMIT_ARG
	OP_SUBSCRIBE_ROUTE_ARG
	OP_SUBSCRIBE_ROUTE_PAYLOAD

	OP_UNSUBSCRIBE
	OP_UNSUBSCRIBE_CORRELATION_ID_ARG
	OP_UNSUBSCRIBE_SUB_ID_ARG

	OP_FETCH
	OP_FETCH_CORRELATION_ID_ARG
	OP_FETCH_AUTO_COMMIT_ARG
	OP_FETCH_ROUTE_ARG
	OP_FETCH_ROUTE_PAYLOAD
	OP_FETCH_N_ARG

	OP_ACK
	OP_ACK_CORRELATION_ID_ARG
	OP_ACK_SUBSCRIPTION_ID_ARG
	OP_ACK_ARG
	OP_ACK_MSG_ID_ARG
	OP_ACK_MSG_ID_PAYLOAD

	OP_NACK
	OP_NACK_CORRELATION_ID_ARG
	OP_NACK_SUBSCRIPTION_ID_ARG
	OP_NACK_ARG
	OP_NACK_MSG_ID_ARG
	OP_NACK_MSG_ID_PAYLOAD

	OP_BEGIN_TX
	OP_BEGIN_TX_CORRELATION_ID_ARG
	OP_BEGIN_TX_ROUTE_ARG
	OP_BEGIN_TX_ROUTE_PAYLOAD
	OP_COMMIT_TX
	OP_COMMIT_TX_CORRELATION_ID_ARG

	OP_ROLLBACK_TX
	OP_ROLLBACK_TX_CORRELATION_ID_ARG
)

var (
	ErrClose                   = errors.New("close")
	ErrParseProto              = errors.New("parse proto")
	ErrFetchArgNotProvided     = errors.New("fetch arg not provided")
	ErrInvalidReaderType       = errors.New("invalid reader type")
	ErrRouteSizeArgNotProvided = errors.New("route size arg not provided")
	ErrWriteRouteArgEmpty      = errors.New("write route arg is empty")
	ErrWriteMsgSizeArgEmpty    = errors.New("write size arg not provided")
	ErrWriteRouteLenArgEmpty   = errors.New("writer route len arg not provided")

	ErrConnectReaderIsAutoCommitArgInvalid = errors.New("connect reader is auto commit arg invalid")
)

type parseState struct {
	state       int
	argBuf      []byte
	payloadBuf  []byte
	payloadsBuf [][]byte

	ca correlationIDArg

	ba  bindArgs
	pa  produceArgs
	pma produceMsgArgs
	ta  txArgs

	sa subscribeArgs
	aa ackArgs
	fa fetchArgs

	// Headered produce args.
	ha headerArgs
}

type correlationIDArg struct {
	cID []byte
}

type produceArgs struct {
	routeLen      uint32
	route         string
	transactional bool
	headered      bool
}

type produceMsgArgs struct {
	size uint32
}

type txArgs struct {
	routeLen uint32
	route    string
}

type subscribeArgs struct {
	routeLen   uint32
	route      string
	autoCommit bool
	headered   bool
}

type bindArgs struct {
	meta               map[string]string
	configOverrides    map[string]string
	connectorNameLen   uint32
	connectorNameValue string
	currentKey         string
	currentValue       string
	keyLen             uint32
	valueLen           uint32
	metaCount          uint16
	metaRead           uint16
	overridesCount     uint16
	overridesRead      uint16
}

type fetchArgs struct {
	autoCommit bool
	routeLen   uint32
	route      string
	headered   bool
}

type ackArgs struct {
	subID        byte
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

type Handler struct {
	ctx  context.Context
	out  *Outbound
	str  session.Stream
	core *core.Core

	ps                *parseState
	fetchBufsMu       sync.Mutex
	fetchBufs         *bufsLease
	fetches           sync.WaitGroup
	produceResponseMu sync.Mutex
	produceResponse   *produceResponse

	// ping
	pingInterval time.Duration
	pingTimeout  time.Duration
	pingStream   bool

	disconnect     func()
	disconnectOnce sync.Once

	stopRead bool
	closed   chan struct{}

	l *slog.Logger
}

func NewHandler(
	ctx context.Context,
	pingInterval time.Duration, pingTimeout time.Duration, pingStream bool,
	baseGeneration *connector.Generation,
	generationProvider core.GenerationProvider,
	out *Outbound, str session.Stream, l *slog.Logger,
) *Handler {
	h := &Handler{
		ctx:          ctx,
		core:         core.New(ctx, baseGeneration, generationProvider, l),
		pingInterval: pingInterval,
		pingTimeout:  pingTimeout,
		pingStream:   pingStream,
		l:            l,
		out:          out,
		str:          str,
		ps:           &parseState{},
		disconnect:   func() {},
		closed:       make(chan struct{}),
	}

	if pingStream {
		_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
		go h.writePing()
	}

	return h
}

func (h *Handler) handle(buf []byte) error {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]
		switch h.ps.state {
		case OP_START:
			switch h.core.State() {
			case core.StateUnbound:
				switch b {
				case byte(v1.OP_CODE_BIND):
					h.ps.state = OP_BIND_CONNECTOR_NAME_LEN
					h.ps.argBuf = pool.Get(v1.Uint32Len)
				case byte(v1.OP_CODE_SUBSCRIBE):
					h.ps.state = OP_SUBSCRIBE
				case byte(v1.RESP_CODE_PONG):
					if h.pingStream {
						_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
					}
				default:
					return ErrParseProto
				}
			case core.StateConnected:
				switch b {
				case byte(v1.OP_CODE_PRODUCE):
					h.ps.state = OP_PRODUCE
				case byte(v1.OP_CODE_HPRODUCE):
					h.ps.pa.headered = true
					h.ps.state = OP_PRODUCE_H
				case byte(v1.OP_CODE_TX_BEGIN):
					h.ps.state = OP_BEGIN_TX
				case byte(v1.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX
				case byte(v1.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX
				case byte(v1.OP_CODE_FETCH):
					h.ps.state = OP_FETCH
				case byte(v1.OP_CODE_HFETCH):
					h.ps.fa.headered = true
					h.ps.state = OP_FETCH
				case byte(v1.OP_CODE_HSUBSCRIBE):
					h.ps.sa.headered = true
					h.ps.state = OP_SUBSCRIBE
				case byte(v1.OP_CODE_ACK):
					h.ps.state = OP_ACK
				case byte(v1.OP_CODE_NACK):
					h.ps.state = OP_NACK
				case byte(v1.OP_CODE_SUBSCRIBE):
					h.ps.state = OP_SUBSCRIBE
				case byte(v1.OP_CODE_UNSUBSCRIBE):
					h.ps.state = OP_UNSUBSCRIBE
				case byte(v1.OP_CODE_DISCONNECT):
					return ErrClose
				case byte(v1.RESP_CODE_PONG):
					if h.pingStream {
						_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
					}
				default:
					return ErrParseProto
				}
			case core.StateInTransaction:
				switch b {
				case byte(v1.OP_CODE_TX_PRODUCE):
					h.ps.pa.transactional = true
					h.ps.state = OP_TX_PRODUCE
				case byte(v1.OP_CODE_TX_HPRODUCE):
					h.ps.pa.transactional = true
					h.ps.pa.headered = true
					h.ps.state = OP_TX_PRODUCE_H
				case byte(v1.OP_CODE_TX_BEGIN):
					h.ps.state = OP_BEGIN_TX
				case byte(v1.OP_CODE_TX_COMMIT):
					h.ps.state = OP_COMMIT_TX
				case byte(v1.OP_CODE_TX_ROLLBACK):
					h.ps.state = OP_ROLLBACK_TX
				case byte(v1.OP_CODE_FETCH):
					h.ps.state = OP_FETCH
				case byte(v1.OP_CODE_HFETCH):
					h.ps.fa.headered = true
					h.ps.state = OP_FETCH
				case byte(v1.OP_CODE_HSUBSCRIBE):
					h.ps.sa.headered = true
					h.ps.state = OP_SUBSCRIBE
				case byte(v1.OP_CODE_ACK):
					h.ps.state = OP_ACK
				case byte(v1.OP_CODE_NACK):
					h.ps.state = OP_NACK
				case byte(v1.OP_CODE_SUBSCRIBE):
					h.ps.state = OP_SUBSCRIBE
				case byte(v1.OP_CODE_UNSUBSCRIBE):
					h.ps.state = OP_UNSUBSCRIBE
				case byte(v1.OP_CODE_DISCONNECT):
					return ErrClose
				case byte(v1.RESP_CODE_PONG):
					if h.pingStream {
						_ = h.str.SetDeadline(time.Now().Add(h.pingTimeout))
					}
				default:
					return ErrParseProto
				}
			case core.StateClosed:
				return ErrParseProto
			}
		case OP_TX_PRODUCE, OP_TX_PRODUCE_H:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			if h.ps.state == OP_TX_PRODUCE_H {
				h.ps.state = OP_TX_PRODUCE_H_CORRELATION_ID_ARG
			} else {
				h.ps.state = OP_TX_PRODUCE_CORRELATION_ID_ARG
			}
		case OP_TX_PRODUCE_CORRELATION_ID_ARG, OP_TX_PRODUCE_H_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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
			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.argBuf = nil
				if h.ps.pa.headered {
					h.ps.ha = headerArgs{}
					h.ps.state = OP_PRODUCE_H_HEADERS_COUNT_ARG
				} else {
					h.ps.state = OP_PRODUCE_MSG_ARG
				}
			}
		case OP_PRODUCE:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_CORRELATION_ID_ARG
		case OP_PRODUCE_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.argBuf = pool.Get(v1.Uint32Len)
				h.ps.state = OP_PRODUCE_ARG
			}
		case OP_PRODUCE_ARG:
			if h.ps.pa.routeLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.routeLen))
				}

				toCopy := int(h.ps.pa.routeLen) - len(h.ps.argBuf)
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

				if len(h.ps.argBuf) >= int(h.ps.pa.routeLen) {
					if err := h.parseWriteRouteArg(); err != nil {
						h.l.Error("parse write route arg", "err", err)
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

			toCopy := v1.Uint32Len - len(h.ps.argBuf)
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

			if len(h.ps.argBuf) >= v1.Uint32Len {
				if h.ps.pa.routeLen == 0 {
					if err := h.parseWriteRouteLenArg(); err != nil {
						h.l.Error("parse write route len arg", "err", err)
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
				h.ps.argBuf = pool.Get(v1.Uint32Len)
				h.ps.argBuf = append(h.ps.argBuf, b)
				continue
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
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
					h.produce(h.ps.payloadBuf, nil)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.GetPayload(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					h.produce(h.ps.payloadBuf, nil)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			}
		case OP_PRODUCE_H:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_PRODUCE_H_CORRELATION_ID_ARG
		case OP_PRODUCE_H_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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
			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.argBuf = pool.Get(v1.Uint32Len)
				h.ps.state = OP_PRODUCE_H_ARG
			}
		case OP_PRODUCE_H_ARG:
			if h.ps.pa.routeLen != 0 {
				if h.ps.argBuf == nil {
					h.ps.argBuf = pool.Get(int(h.ps.pa.routeLen))
				}
				toCopy := int(h.ps.pa.routeLen) - len(h.ps.argBuf)
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
				if len(h.ps.argBuf) >= int(h.ps.pa.routeLen) {
					if err := h.parseWriteRouteArg(); err != nil {
						h.l.Error("parse write route arg", "err", err)
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

			toCopy := v1.Uint32Len - len(h.ps.argBuf)
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

			if len(h.ps.argBuf) >= v1.Uint32Len {
				if h.ps.pa.routeLen == 0 {
					if err := h.parseWriteRouteLenArg(); err != nil {
						h.l.Error("parse write route len arg", "err", err)
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
				h.ps.ha.read, h.ps.ha.headersKV, h.ps.argBuf = 0, [][]byte{}, nil
				if h.ps.ha.count == 0 {
					h.ps.state = OP_PRODUCE_H_MSG_ARG
					continue
				}
				h.ps.state = OP_PRODUCE_H_HEADER_STR_LEN_ARG
			}
		case OP_PRODUCE_H_HEADER_STR_LEN_ARG:
			if h.ps.argBuf == nil {
				h.ps.argBuf = pool.Get(v1.Uint32Len)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.ha.currStrLn = binary.BigEndian.Uint32(h.ps.argBuf[:v1.Uint32Len])
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
				h.ps.argBuf = pool.Get(v1.Uint32Len)
			}
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
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
					h.produce(h.ps.payloadBuf, h.ps.ha.headersKV)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			} else {
				h.ps.payloadBuf = pool.GetPayload(int(h.ps.pma.size))
				h.ps.payloadBuf = append(h.ps.payloadBuf, b)
				if len(h.ps.payloadBuf) >= int(h.ps.pma.size) {
					h.produce(h.ps.payloadBuf, h.ps.ha.headersKV)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.payloadBuf, h.ps.pa, h.ps.state = nil, nil, produceArgs{}, OP_START
				}
			}
		case OP_FETCH:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_FETCH_CORRELATION_ID_ARG
		case OP_FETCH_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= v1.Uint32Len {
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
			h.ps.state = OP_FETCH_ROUTE_ARG
			h.ps.argBuf = pool.Get(v1.Uint32Len)
		case OP_FETCH_ROUTE_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.fa.routeLen = binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(int(h.ps.fa.routeLen))
				h.ps.state = OP_FETCH_ROUTE_PAYLOAD
			}
		case OP_FETCH_ROUTE_PAYLOAD:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= int(h.ps.fa.routeLen) {
				h.ps.fa.route = string(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(v1.Uint32Len)
				h.ps.state = OP_FETCH_N_ARG
			}
		case OP_FETCH_N_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				n := binary.BigEndian.Uint32(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				h.fetch(h.ps.fa.route, h.ps.fa.autoCommit, n)
				pool.Put(h.ps.ca.cID)
				h.ps.ca.cID, h.ps.state = nil, OP_START
			}
		case OP_ACK:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ACK_CORRELATION_ID_ARG
		case OP_ACK_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.state = OP_ACK_SUBSCRIPTION_ID_ARG
			}
		case OP_ACK_SUBSCRIPTION_ID_ARG:
			h.ps.aa.subID = b
			h.ps.aa.msgIDsBuf = pool.Get(v1.Uint32Len)
			h.ps.state = OP_ACK_ARG
		case OP_ACK_ARG:
			h.ps.aa.msgIDsBuf = append(h.ps.aa.msgIDsBuf, b)
			if len(h.ps.aa.msgIDsBuf) >= v1.Uint32Len {
				h.ps.aa.msgIDsLen = binary.BigEndian.Uint32(h.ps.aa.msgIDsBuf)
				if h.ps.aa.msgIDsLen == 0 {
					cID := h.ps.ca.cID
					h.ack(false, h.ps.aa.subID, nil, cID)
					pool.Put(h.ps.aa.msgIDsBuf)
					h.ps.ca.cID, h.ps.aa, h.ps.state = nil, ackArgs{}, OP_START
					continue
				}
				h.ps.payloadsBuf = GetBufs()
				h.ps.argBuf = pool.Get(v1.Uint32Len)
				h.ps.state = OP_ACK_MSG_ID_ARG
			}
		case OP_ACK_MSG_ID_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
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
					msgIDs := h.ps.payloadsBuf
					cID := h.ps.ca.cID
					h.ack(false, h.ps.aa.subID, msgIDs, cID)
					pool.Put(h.ps.aa.msgIDsBuf)
					h.ps.argBuf, h.ps.payloadBuf, h.ps.payloadsBuf, h.ps.ca.cID, h.ps.aa, h.ps.state = nil, nil, nil, nil, ackArgs{}, OP_START
					continue
				} else {
					h.ps.argBuf = pool.Get(v1.Uint32Len)
					h.ps.state = OP_ACK_MSG_ID_ARG
				}
			}
		case OP_NACK:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_NACK_CORRELATION_ID_ARG
		case OP_NACK_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.state = OP_NACK_SUBSCRIPTION_ID_ARG
			}
		case OP_NACK_SUBSCRIPTION_ID_ARG:
			h.ps.aa.subID = b
			h.ps.aa.msgIDsBuf = pool.Get(v1.Uint32Len)
			h.ps.state = OP_NACK_ARG
		case OP_NACK_ARG:
			h.ps.aa.msgIDsBuf = append(h.ps.aa.msgIDsBuf, b)
			if len(h.ps.aa.msgIDsBuf) >= v1.Uint32Len {
				h.ps.aa.msgIDsLen = binary.BigEndian.Uint32(h.ps.aa.msgIDsBuf)
				if h.ps.aa.msgIDsLen == 0 {
					cID := h.ps.ca.cID
					h.ack(true, h.ps.aa.subID, nil, cID)
					pool.Put(h.ps.aa.msgIDsBuf)
					h.ps.ca.cID, h.ps.aa, h.ps.state = nil, ackArgs{}, OP_START
					continue
				}
				h.ps.payloadsBuf = GetBufs()
				pool.Put(h.ps.aa.msgIDsBuf)
				h.ps.argBuf = pool.Get(v1.Uint32Len)
				h.ps.state = OP_NACK_MSG_ID_ARG
			}
		case OP_NACK_MSG_ID_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
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
					msgIDs := h.ps.payloadsBuf
					cID := h.ps.ca.cID
					h.ack(true, h.ps.aa.subID, msgIDs, cID)
					h.ps.argBuf, h.ps.payloadBuf, h.ps.payloadsBuf, h.ps.ca.cID, h.ps.aa, h.ps.state = nil, nil, nil, nil, ackArgs{}, OP_START
					continue
				} else {
					h.ps.argBuf = pool.Get(v1.Uint32Len)
					h.ps.state = OP_NACK_MSG_ID_ARG
				}
			}
		case OP_BEGIN_TX:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_BEGIN_TX_CORRELATION_ID_ARG
		case OP_BEGIN_TX_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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
			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.argBuf = pool.Get(v1.Uint32Len)
				h.ps.state = OP_BEGIN_TX_ROUTE_ARG
			}
		case OP_BEGIN_TX_ROUTE_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.ta.routeLen = binary.BigEndian.Uint32(h.ps.argBuf[:v1.Uint32Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				if h.ps.ta.routeLen == 0 {
					h.enqueueTxBeginErr(h.ps.ca.cID, ErrRouteSizeArgNotProvided)
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.ta, h.ps.state = nil, txArgs{}, OP_START
					continue
				}
				h.ps.payloadBuf = pool.Get(int(h.ps.ta.routeLen))
				h.ps.state = OP_BEGIN_TX_ROUTE_PAYLOAD
			}
		case OP_BEGIN_TX_ROUTE_PAYLOAD:
			toCopy := int(h.ps.ta.routeLen) - len(h.ps.payloadBuf)
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
			if len(h.ps.payloadBuf) >= int(h.ps.ta.routeLen) {
				h.ps.ta.route = string(h.ps.payloadBuf)
				if err := h.core.Begin(h.ps.ta.route); err != nil {
					h.enqueueTxBeginErr(h.ps.ca.cID, err)
				} else {
					h.enqueueTxBeginSuccess(h.ps.ca.cID)
				}
				pool.Put(h.ps.ca.cID)
				pool.Put(h.ps.payloadBuf)
				h.ps.ca.cID, h.ps.payloadBuf, h.ps.ta, h.ps.state = nil, nil, txArgs{}, OP_START
			}
		case OP_COMMIT_TX:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_COMMIT_TX_CORRELATION_ID_ARG
		case OP_COMMIT_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= v1.Uint32Len {
					if err := h.core.Commit(); err != nil {
						h.enqueueTxCommitErr(h.ps.ca.cID, err)
					} else {
						h.enqueueTxCommitSuccess(h.ps.ca.cID)
					}
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.state = nil, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(v1.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_ROLLBACK_TX:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_ROLLBACK_TX_CORRELATION_ID_ARG
		case OP_ROLLBACK_TX_CORRELATION_ID_ARG:
			if h.ps.ca.cID != nil {
				toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

				if len(h.ps.ca.cID) >= v1.Uint32Len {
					if err := h.core.Rollback(); err != nil {
						h.enqueueTxRollbackErr(h.ps.ca.cID, err)
					} else {
						h.enqueueTxRollbackSuccess(h.ps.ca.cID)
					}
					pool.Put(h.ps.ca.cID)
					h.ps.ca.cID, h.ps.state = nil, OP_START
				}
			} else {
				h.ps.ca.cID = pool.Get(v1.Uint32Len)
				h.ps.ca.cID = append(h.ps.ca.cID, b)
			}
		case OP_BIND_CONNECTOR_NAME_LEN:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.ba.connectorNameLen = binary.BigEndian.Uint32(h.ps.argBuf[:v1.Uint32Len])
				pool.Put(h.ps.argBuf)
				if h.ps.ba.connectorNameLen == 0 {
					return ErrParseProto
				}
				h.ps.argBuf = pool.Get(int(h.ps.ba.connectorNameLen))
				h.ps.state = OP_BIND_CONNECTOR_NAME_PAYLOAD
			}
		case OP_BIND_CONNECTOR_NAME_PAYLOAD:
			toCopy := int(h.ps.ba.connectorNameLen) - len(h.ps.argBuf)
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
			if len(h.ps.argBuf) >= int(h.ps.ba.connectorNameLen) {
				h.ps.ba.connectorNameValue = string(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.state = OP_BIND_META_COUNT
				h.ps.argBuf = pool.Get(v1.Uint16Len)
			}
		case OP_BIND_META_COUNT:
			// Parse meta count (uint16)
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint16Len {
				h.ps.ba.metaCount = binary.BigEndian.Uint16(h.ps.argBuf[:v1.Uint16Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				if h.ps.ba.metaCount == 0 {
					// No meta, proceed to config overrides
					h.ps.state = OP_BIND_CONFIG_OVERRIDES_COUNT
					h.ps.argBuf = pool.Get(v1.Uint16Len)
				} else {
					h.ps.ba.meta = make(map[string]string, h.ps.ba.metaCount)
					h.ps.state = OP_BIND_META_KEY_LEN
					h.ps.argBuf = pool.Get(v1.Uint32Len)
				}
			}
		case OP_BIND_META_KEY_LEN:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.ba.keyLen = binary.BigEndian.Uint32(h.ps.argBuf[:v1.Uint32Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				if h.ps.ba.keyLen == 0 {
					return ErrParseProto
				}
				h.ps.argBuf = pool.Get(int(h.ps.ba.keyLen))
				h.ps.state = OP_BIND_META_KEY
			}
		case OP_BIND_META_KEY:
			toCopy := int(h.ps.ba.keyLen) - len(h.ps.argBuf)
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
			if len(h.ps.argBuf) >= int(h.ps.ba.keyLen) {
				h.ps.ba.currentKey = string(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.state = OP_BIND_META_VALUE_LEN
				h.ps.argBuf = pool.Get(v1.Uint32Len)
			}
		case OP_BIND_META_VALUE_LEN:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.ba.valueLen = binary.BigEndian.Uint32(h.ps.argBuf[:v1.Uint32Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(int(h.ps.ba.valueLen))
				h.ps.state = OP_BIND_META_VALUE
			}
		case OP_BIND_META_VALUE:
			toCopy := int(h.ps.ba.valueLen) - len(h.ps.argBuf)
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
			if len(h.ps.argBuf) >= int(h.ps.ba.valueLen) {
				h.ps.ba.currentValue = string(h.ps.argBuf)
				h.ps.ba.meta[h.ps.ba.currentKey] = h.ps.ba.currentValue
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				h.ps.ba.metaRead++
				if h.ps.ba.metaRead >= h.ps.ba.metaCount {
					// All meta parsed, proceed to config overrides
					h.ps.state = OP_BIND_CONFIG_OVERRIDES_COUNT
					h.ps.argBuf = pool.Get(v1.Uint16Len)
				} else {
					// Continue with next meta pair
					h.ps.state = OP_BIND_META_KEY_LEN
					h.ps.argBuf = pool.Get(v1.Uint32Len)
				}
			}
		case OP_BIND_CONFIG_OVERRIDES_COUNT:
			// Parse config_overrides count (uint16)
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint16Len {
				h.ps.ba.overridesCount = binary.BigEndian.Uint16(h.ps.argBuf[:v1.Uint16Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				if h.ps.ba.overridesCount == 0 {
					// No overrides, create Manager with base config
					meta := h.ps.ba.meta
					if meta == nil {
						meta = make(map[string]string)
					}
					if err := h.handleBind(meta, nil); err != nil {
						return err
					}
					h.ps.ba = bindArgs{}
					h.ps.state = OP_START
				} else {
					h.ps.ba.configOverrides = make(map[string]string, h.ps.ba.overridesCount)
					h.ps.state = OP_BIND_CONFIG_OVERRIDE_KEY_LEN
					h.ps.argBuf = pool.Get(v1.Uint32Len)
				}
			}
		case OP_BIND_CONFIG_OVERRIDE_KEY_LEN:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.ba.keyLen = binary.BigEndian.Uint32(h.ps.argBuf[:v1.Uint32Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				if h.ps.ba.keyLen == 0 {
					return ErrParseProto
				}
				h.ps.argBuf = pool.Get(int(h.ps.ba.keyLen))
				h.ps.state = OP_BIND_CONFIG_OVERRIDE_KEY
			}
		case OP_BIND_CONFIG_OVERRIDE_KEY:
			toCopy := int(h.ps.ba.keyLen) - len(h.ps.argBuf)
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
			if len(h.ps.argBuf) >= int(h.ps.ba.keyLen) {
				h.ps.ba.currentKey = string(h.ps.argBuf)
				pool.Put(h.ps.argBuf)
				h.ps.state = OP_BIND_CONFIG_OVERRIDE_VALUE_LEN
				h.ps.argBuf = pool.Get(v1.Uint32Len)
			}
		case OP_BIND_CONFIG_OVERRIDE_VALUE_LEN:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				h.ps.ba.valueLen = binary.BigEndian.Uint32(h.ps.argBuf[:v1.Uint32Len])
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = pool.Get(int(h.ps.ba.valueLen))
				h.ps.state = OP_BIND_CONFIG_OVERRIDE_VALUE
			}
		case OP_BIND_CONFIG_OVERRIDE_VALUE:
			toCopy := int(h.ps.ba.valueLen) - len(h.ps.argBuf)
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
			if len(h.ps.argBuf) >= int(h.ps.ba.valueLen) {
				h.ps.ba.currentValue = string(h.ps.argBuf)
				h.ps.ba.configOverrides[h.ps.ba.currentKey] = h.ps.ba.currentValue
				pool.Put(h.ps.argBuf)
				h.ps.argBuf = nil
				h.ps.ba.overridesRead++
				if h.ps.ba.overridesRead >= h.ps.ba.overridesCount {
					// All overrides parsed, create Manager
					meta := h.ps.ba.meta
					if meta == nil {
						meta = make(map[string]string)
					}
					if err := h.handleBind(meta, h.ps.ba.configOverrides); err != nil {
						return err
					}
					h.ps.ba = bindArgs{}
					h.ps.state = OP_START
				} else {
					// Continue with next override
					h.ps.state = OP_BIND_CONFIG_OVERRIDE_KEY_LEN
					h.ps.argBuf = pool.Get(v1.Uint32Len)
				}
			}
		case OP_SUBSCRIBE:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_SUBSCRIBE_CORRELATION_ID_ARG
		case OP_SUBSCRIBE_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.state = OP_SUBSCRIBE_AUTO_COMMIT_ARG
			}
		case OP_SUBSCRIBE_AUTO_COMMIT_ARG:
			var err error
			h.ps.sa.autoCommit, err = parseBool(b)
			if err != nil {
				enqueueSubscribeErr(h.out, h.ps.ca.cID, v1.RESP_CODE_SUBSCRIBE, err)
				return err
			}

			h.ps.argBuf = pool.Get(v1.Uint32Len)
			h.ps.state = OP_SUBSCRIBE_ROUTE_ARG
		case OP_SUBSCRIBE_ROUTE_ARG:
			h.ps.argBuf = append(h.ps.argBuf, b)
			if len(h.ps.argBuf) >= v1.Uint32Len {
				if err := h.parseSubscribeRouteLenArg(); err != nil {
					pool.Put(h.ps.argBuf)
					h.ps.argBuf, h.ps.sa, h.ps.state = nil, subscribeArgs{}, OP_START
					enqueueSubscribeErr(h.out, h.ps.ca.cID, v1.RESP_CODE_SUBSCRIBE, err)
					return err
				}
				pool.Put(h.ps.argBuf)
				h.ps.payloadBuf = pool.Get(int(h.ps.sa.routeLen))
				h.ps.argBuf, h.ps.state = nil, OP_SUBSCRIBE_ROUTE_PAYLOAD
			}
		case OP_SUBSCRIBE_ROUTE_PAYLOAD:
			toCopy := int(h.ps.sa.routeLen) - len(h.ps.payloadBuf)
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

			if len(h.ps.payloadBuf) >= int(h.ps.sa.routeLen) {
				h.ps.state = OP_START
				h.ps.sa.route = string(h.ps.payloadBuf)
				pool.Put(h.ps.payloadBuf)

				headered := h.ps.sa.headered
				code := v1.RESP_CODE_SUBSCRIBE
				if headered {
					code = v1.RESP_CODE_HSUBSCRIBE
				}
				ready := func(subscriptionID byte) error {
					enqueueSubscribeSuccess(h.out, code, h.ps.ca.cID, subscriptionID)
					return nil
				}
				handlers := core.SubscriptionMessageHandlers{}
				if headered {
					handlers.MessageWithHeaders = func(subscriptionID byte, reader connector.Reader) func([]byte, string, [][]byte, ...any) {
						if reader.AutoCommit() {
							return func(payload []byte, _ string, headers [][]byte, _ ...any) {
								enqueueAutoCommitSubscriptionMessage(h.out, subscriptionID, true, payload, headers)
							}
						}
						return func(payload []byte, source string, headers [][]byte, args ...any) {
							enqueueSubscriptionMessage(h.out, subscriptionID, reader, true, payload, source, headers, args...)
						}
					}
				} else {
					handlers.Message = func(subscriptionID byte, reader connector.Reader) func([]byte, string, ...any) {
						if reader.AutoCommit() {
							return func(payload []byte, _ string, _ ...any) {
								enqueueAutoCommitSubscriptionMessage(h.out, subscriptionID, false, payload, nil)
							}
						}
						return func(payload []byte, source string, args ...any) {
							enqueueSubscriptionMessage(h.out, subscriptionID, reader, false, payload, source, nil, args...)
						}
					}
				}
				err := h.core.Subscribe(h.ps.sa.route, h.ps.sa.autoCommit, headered, ready, handlers, func(err error) {
					h.l.Error("subscription ended", "route", h.ps.sa.route, "err", err)
					_ = h.str.Close()
				})
				if err != nil {
					enqueueSubscribeErr(h.out, h.ps.ca.cID, code, err)
				}
				pool.Put(h.ps.ca.cID)
				h.ps.ca.cID, h.ps.payloadBuf, h.ps.sa, h.ps.state = nil, nil, subscribeArgs{}, OP_START
				continue
			}
		case OP_UNSUBSCRIBE:
			h.ps.ca.cID = pool.Get(v1.Uint32Len)
			h.ps.ca.cID = append(h.ps.ca.cID, b)
			h.ps.state = OP_UNSUBSCRIBE_CORRELATION_ID_ARG
		case OP_UNSUBSCRIBE_CORRELATION_ID_ARG:
			toCopy := v1.Uint32Len - len(h.ps.ca.cID)
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

			if len(h.ps.ca.cID) >= v1.Uint32Len {
				h.ps.state = OP_UNSUBSCRIBE_SUB_ID_ARG
			}
		case OP_UNSUBSCRIBE_SUB_ID_ARG:
			err := h.core.Unsubscribe(b)
			enqueueUnsubscribeResponse(h.out, h.ps.ca.cID, err)
			pool.Put(h.ps.ca.cID)
			h.ps.ca.cID, h.ps.state = nil, OP_START
		default:
			return ErrParseProto
		}
	}

	return nil
}

// handleBind delegates connector selection, middleware, overrides, and cleanup to Session Core.
func (h *Handler) handleBind(meta map[string]string, configOverrides map[string]string) error {
	result, err := h.core.Bind(h.ps.ba.connectorNameValue, meta, configOverrides)
	if err != nil {
		header := pool.Get(1)
		header = append(header, byte(v1.RESP_CODE_BIND))
		errBuf := operationErrorBuf(err)
		h.out.EnqueueProtoMulti(header, errBuf)
		pool.Put(header)
		pool.Put(errBuf)
		return nil
	}

	buf := encodeBindSuccess(result.Routes)
	h.out.EnqueueProto(buf)
	pool.Put(buf)
	h.disconnect = func() {
		h.fetches.Wait()
		if err := h.core.Close(); err != nil {
			h.l.Error("close session", "err", err)
		}
		h.out.EnqueueProto(v1.DISCONNECT_RESP)
	}
	return nil
}

func encodeBindSuccess(routes map[string]connector.RouteProfile) []byte {
	names := make([]string, 0, len(routes))
	capacity := 2 + v1.Uint32Len
	for route := range routes {
		names = append(names, route)
		capacity += v1.Uint32Len + len(route) + 4
	}
	sort.Strings(names)

	buf := pool.Get(capacity)
	buf = append(buf, byte(v1.RESP_CODE_BIND), byte(v1.STATUS_OK))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(names)))
	for _, route := range names {
		profile := routes[route]
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(route)))
		buf = append(buf, route...)
		buf = append(buf,
			routeCapabilityFlags(profile),
			byte(profile.ProduceGuarantee),
			byte(profile.Settlement.Ack),
			byte(profile.Settlement.Nack),
		)
	}
	return buf
}

func routeCapabilityFlags(profile connector.RouteProfile) byte {
	var flags byte
	if profile.Produce {
		flags |= v1.ROUTE_CAP_PRODUCE
	}
	if profile.Headers {
		flags |= v1.ROUTE_CAP_HEADERS
	}
	if profile.Transactions {
		flags |= v1.ROUTE_CAP_TRANSACTIONS
	}
	if profile.Subscribe {
		flags |= v1.ROUTE_CAP_SUBSCRIBE
	}
	if profile.Fetch {
		flags |= v1.ROUTE_CAP_FETCH
	}
	if profile.ManualSettlement {
		flags |= v1.ROUTE_CAP_MANUAL_SETTLEMENT
	}
	return flags
}

type ackResponse struct {
	h          *Handler
	op         byte
	messageIDs [][]byte
	cID        []byte
	remaining  int
	handlers   core.AckResultHandlers
	response   []byte
}

var ackResponses = sync.Pool{
	New: func() any {
		return new(ackResponse)
	},
}

func (r *ackResponse) onResult(err error) {
	r.h.out.Lock()
	capacity := 10
	for _, messageID := range r.messageIDs {
		capacity += v1.Uint32Len + len(messageID) + 1
	}
	r.response = pool.Get(capacity)
	r.response = append(r.response, r.op)
	r.response = append(r.response, r.cID...)
	if err != nil {
		errBuf := operationErrorBuf(err)
		r.response = append(r.response, errBuf...)
		pool.Put(errBuf)
		r.enqueueResponseNoLock()
		r.h.out.Unlock()
		r.release()
		return
	}

	r.response = append(r.response, byte(v1.STATUS_OK))
	r.response = binary.BigEndian.AppendUint32(r.response, uint32(r.remaining))
	if r.remaining == 0 {
		r.enqueueResponseNoLock()
		r.h.out.Unlock()
		r.release()
	}
}

func (r *ackResponse) onMessage(messageID []byte, err error) {
	r.response = binary.BigEndian.AppendUint32(r.response, uint32(len(messageID)))
	r.response = append(r.response, messageID...)
	if err == nil {
		r.response = append(r.response, byte(v1.STATUS_OK))
	} else {
		errBuf := operationErrorBuf(err)
		r.response = append(r.response, errBuf...)
		pool.Put(errBuf)
	}

	r.remaining--
	if r.remaining == 0 {
		r.enqueueResponseNoLock()
		r.h.out.Unlock()
		r.release()
	}
}

func (r *ackResponse) enqueueResponseNoLock() {
	r.h.out.QueueOutboundOwnedMultiNoLock(r.response)
	r.h.out.SignalFlush()
	r.response = nil
}

func (r *ackResponse) release() {
	pool.Put(r.cID)
	for _, messageID := range r.messageIDs {
		pool.Put(messageID)
	}
	if r.messageIDs != nil {
		PutBufs(r.messageIDs)
	}
	r.h, r.messageIDs, r.cID, r.response = nil, nil, nil, nil
	r.op, r.remaining = 0, 0
	ackResponses.Put(r)
}

func (h *Handler) ack(nack bool, subscriptionID byte, messageIDs [][]byte, cID []byte) {
	op := byte(v1.RESP_CODE_ACK)
	if nack {
		op = byte(v1.RESP_CODE_NACK)
	}
	response := ackResponses.Get().(*ackResponse)
	if response.handlers.Result == nil {
		response.handlers.Result = response.onResult
		response.handlers.Message = response.onMessage
	}
	response.h, response.op, response.messageIDs, response.cID = h, op, messageIDs, cID
	response.remaining = len(messageIDs)
	var err error
	if nack {
		err = h.core.Nack(subscriptionID, messageIDs, response.handlers)
	} else {
		err = h.core.Ack(subscriptionID, messageIDs, response.handlers)
	}
	if err != nil {
		response.onResult(err)
	}
}
func (h *Handler) writePing() {
	t := time.NewTicker(h.pingInterval)
	defer t.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-t.C:
			h.out.EnqueueProto(v1.PING_REQ)
		}
	}
}

type produceResponse struct {
	h        *Handler
	message  []byte
	response []byte
	callback func(error)
}

func (h *Handler) getProduceResponse() *produceResponse {
	h.produceResponseMu.Lock()
	response := h.produceResponse
	h.produceResponse = nil
	h.produceResponseMu.Unlock()
	if response == nil {
		response = &produceResponse{h: h}
		response.callback = response.respond
	}
	return response
}

func (h *Handler) putProduceResponse(response *produceResponse) {
	h.produceResponseMu.Lock()
	if h.produceResponse == nil {
		h.produceResponse = response
	}
	h.produceResponseMu.Unlock()
}

func (r *produceResponse) respond(err error) {
	pool.PutPayload(r.message)
	if err != nil {
		header := r.response[:5]
		errBuf := operationErrorBuf(err)
		r.h.out.EnqueueProtoMulti(header, errBuf)
		pool.Put(errBuf)
	} else {
		r.h.out.EnqueueProto(r.response)
	}
	pool.Put(r.response)
	r.message, r.response = nil, nil
	r.h.putProduceResponse(r)
}

func (h *Handler) produce(msg []byte, headers [][]byte) {
	op := h.writeResponseCode()
	response := h.getProduceResponse()
	buf := pool.Get(6)
	buf = append(buf, byte(op))
	buf = append(buf, h.ps.ca.cID...)
	buf = append(buf, byte(v1.STATUS_OK))
	response.h, response.message, response.response = h, msg, buf

	var err error
	if h.ps.pa.transactional {
		err = h.core.TxProduce(msg, headers, response.callback)
	} else {
		err = h.core.Produce(h.ps.pa.route, msg, headers, response.callback)
	}
	if err != nil {
		response.respond(err)
	}
}

func (h *Handler) writeResponseCode() v1.RespCode {
	switch {
	case h.ps.pa.transactional && h.ps.pa.headered:
		return v1.RESP_CODE_TX_HPRODUCE
	case h.ps.pa.transactional:
		return v1.RESP_CODE_TX_PRODUCE
	case h.ps.pa.headered:
		return v1.RESP_CODE_HPRODUCE
	default:
		return v1.RESP_CODE_PRODUCE
	}
}

func (h *Handler) fetch(route string, autoCommit bool, n uint32) {
	headered := h.ps.fa.headered
	op := v1.RESP_CODE_FETCH
	if headered {
		op = v1.RESP_CODE_HFETCH
	}
	correlationID := binary.BigEndian.Uint32(h.ps.ca.cID)
	messages := h.getFetchBufs(int(n) + 1)
	header := pool.Get(11)[:11]
	header[0] = byte(op)
	binary.BigEndian.PutUint32(header[1:5], correlationID)
	header[5] = byte(v1.STATUS_OK)
	messages.bufs = append(messages.bufs, header)
	flush := func(subscriptionID byte, count uint32, fetchErr error) {
		h.out.Lock()
		if fetchErr != nil {
			for _, messageBuf := range messages.bufs {
				pool.Put(messageBuf)
			}
			header := pool.Get(5)[:0]
			header = append(header, byte(op))
			header = binary.BigEndian.AppendUint32(header, correlationID)
			errBuf := operationErrorBuf(fetchErr)
			h.out.QueueOutboundOwnedMultiNoLock(header, errBuf)
		} else {
			header[6] = subscriptionID
			binary.BigEndian.PutUint32(header[7:11], count)
			h.out.QueueOutboundOwnedMultiNoLock(messages.bufs...)
		}
		h.out.SignalFlush()
		h.out.Unlock()
		h.putFetchBufs(messages)
	}
	h.fetches.Add(1)
	go func() {
		defer h.fetches.Done()
		handlers := core.FetchMessageHandlers{}
		switch {
		case autoCommit && headered:
			handlers.AutoCommitWithHeaders = func(payload []byte, _ string, headers [][]byte, _ ...any) {
				appendAutoCommitHFetchMessage(messages, payload, headers)
			}
		case autoCommit:
			handlers.AutoCommit = func(payload []byte, _ string, _ ...any) {
				appendAutoCommitFetchMessage(messages, payload)
			}
		default:
			handlers.Manual = func(_ byte, reader connector.Reader, payload []byte, source string, headers [][]byte, args ...any) {
				appendFetchMessage(messages, reader, false, headered, payload, source, headers, args...)
			}
		}
		subscriptionID, count, fetchErr := h.core.Fetch(route, autoCommit, headered, n, handlers)
		flush(subscriptionID, count, fetchErr)
	}()
}

func appendAutoCommitFetchMessage(messages *bufsLease, payload []byte) {
	messageSize := v1.Uint32Len + len(payload)
	if fetchBufferHasCapacity(messages, messageSize) {
		buffer := &messages.bufs[len(messages.bufs)-1]
		*buffer = binary.BigEndian.AppendUint32(*buffer, uint32(len(payload)))
		*buffer = append(*buffer, payload...)
		return
	}

	var prefix [v1.Uint32Len]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	appendFetchBytes(messages, prefix[:])
	appendFetchBytes(messages, payload)
}

func appendAutoCommitHFetchMessage(messages *bufsLease, payload []byte, headers [][]byte) {
	messageSize := v1.Uint16Len + v1.Uint32Len + len(payload)
	for _, header := range headers {
		messageSize += v1.Uint32Len + len(header)
	}
	if fetchBufferHasCapacity(messages, messageSize) {
		buffer := &messages.bufs[len(messages.bufs)-1]
		*buffer = binary.BigEndian.AppendUint16(*buffer, uint16(len(headers)))
		for _, header := range headers {
			*buffer = binary.BigEndian.AppendUint32(*buffer, uint32(len(header)))
			*buffer = append(*buffer, header...)
		}
		*buffer = binary.BigEndian.AppendUint32(*buffer, uint32(len(payload)))
		*buffer = append(*buffer, payload...)
		return
	}

	var count [v1.Uint16Len]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(headers)))
	appendFetchBytes(messages, count[:])
	for _, header := range headers {
		var size [v1.Uint32Len]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(header)))
		appendFetchBytes(messages, size[:])
		appendFetchBytes(messages, header)
	}
	var size [v1.Uint32Len]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(payload)))
	appendFetchBytes(messages, size[:])
	appendFetchBytes(messages, payload)
}

func fetchBufferHasCapacity(messages *bufsLease, size int) bool {
	return len(messages.bufs) > 0 && cap(messages.bufs[len(messages.bufs)-1])-len(messages.bufs[len(messages.bufs)-1]) >= size
}

func appendFetchBytes(messages *bufsLease, data []byte) {
	for len(data) > 0 {
		if len(messages.bufs) == 0 || len(messages.bufs[len(messages.bufs)-1]) == cap(messages.bufs[len(messages.bufs)-1]) {
			size := len(data)
			if len(messages.bufs) > 0 && cap(messages.bufs[len(messages.bufs)-1]) == pool.SIZE_LARGE {
				size = pool.SIZE_LARGE
			}
			messages.bufs = append(messages.bufs, pool.Get(max(size, pool.SIZE_TINY)))
		}
		buffer := &messages.bufs[len(messages.bufs)-1]
		n := min(len(data), cap(*buffer)-len(*buffer))
		*buffer = append(*buffer, data[:n]...)
		data = data[n:]
	}
}

func appendFetchMessage(
	messages *bufsLease,
	reader connector.Reader,
	autoCommit bool,
	headered bool,
	payload []byte,
	source string,
	headers [][]byte,
	args ...any,
) {
	headersSize := 0
	if headered {
		for _, header := range headers {
			headersSize += v1.Uint32Len + len(header)
		}
	}
	messageIDSize := 0
	if !autoCommit {
		messageIDSize = len(source) + reader.MsgIDArgsLen()
	}
	size := len(payload) + v1.Uint32Len + headersSize
	if headered {
		size += v1.Uint16Len
	}
	if !autoCommit {
		size += v1.Uint32Len + messageIDSize
	}
	ensureFetchBufferCapacity(messages, size)
	buffer := &messages.bufs[len(messages.bufs)-1]
	*buffer = encodeFetchMessage(*buffer, reader, autoCommit, headered, payload, source, headers, messageIDSize, args...)
}

func encodeFetchMessage(
	buffer []byte,
	reader connector.Reader,
	autoCommit bool,
	headered bool,
	payload []byte,
	source string,
	headers [][]byte,
	messageIDSize int,
	args ...any,
) []byte {
	if headered {
		buffer = binary.BigEndian.AppendUint16(buffer, uint16(len(headers)))
		for _, header := range headers {
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(header)))
			buffer = append(buffer, header...)
		}
	}
	if !autoCommit {
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(messageIDSize))
		buffer = reader.EncodeMsgID(buffer, source, args...)
	}
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(payload)))
	return append(buffer, payload...)
}

func ensureFetchBufferCapacity(messages *bufsLease, size int) {
	if len(messages.bufs) > 0 {
		last := messages.bufs[len(messages.bufs)-1]
		if cap(last)-len(last) >= size {
			return
		}
	}
	messages.bufs = append(messages.bufs, pool.Get(max(size, pool.SIZE_TINY)))
}
func (h *Handler) flushBufs() {
	if h.ps.ca.cID != nil {
		pool.Put(h.ps.ca.cID)
		h.ps.ca.cID = nil
	}
	if h.ps.argBuf != nil {
		pool.Put(h.ps.argBuf)
		h.ps.argBuf = nil
	}
	if h.ps.payloadBuf != nil {
		pool.PutPayload(h.ps.payloadBuf)
		h.ps.payloadBuf = nil
	}
	if h.ps.payloadsBuf != nil {
		for _, buf := range h.ps.payloadsBuf {
			pool.Put(buf)
		}
		PutBufs(h.ps.payloadsBuf)
		h.ps.payloadsBuf = nil
	}
}

func (h *Handler) close() {
	h.stopRead = true
	h.disconnectOnce.Do(h.disconnect)
	close(h.closed)
}

func (h *Handler) parseWriteRouteLenArg() error {
	h.ps.pa.routeLen = binary.BigEndian.Uint32(h.ps.argBuf[0:v1.Uint32Len])
	if h.ps.pa.routeLen == 0 {
		return ErrWriteRouteLenArgEmpty
	}
	return nil
}

func (h *Handler) parseWriteRouteArg() error {
	h.ps.pa.route = string(h.ps.argBuf)
	if h.ps.pa.route == "" {
		return ErrWriteRouteArgEmpty
	}
	return nil
}

func (h *Handler) parseWriteMsgSizeArg() error {
	h.ps.pma.size = binary.BigEndian.Uint32(h.ps.argBuf[0:v1.Uint32Len])
	if h.ps.pma.size == 0 {
		return ErrWriteMsgSizeArgEmpty
	}
	return nil
}

func (h *Handler) parseSubscribeRouteLenArg() error {
	h.ps.sa.routeLen = binary.BigEndian.Uint32(h.ps.argBuf[0:v1.Uint32Len])
	if h.ps.sa.routeLen == 0 {
		return ErrRouteSizeArgNotProvided
	}
	return nil
}

func (h *Handler) enqueueWriteErrResponse(err error) {
	header := pool.Get(5)
	header = append(header, byte(h.writeResponseCode()))
	header = append(header, h.ps.ca.cID...)
	errBuf := operationErrorBuf(err)
	h.out.EnqueueProtoMulti(header, errBuf)
	pool.Put(header)
	pool.Put(errBuf)
}

func (h *Handler) enqueueStop() {
	h.out.EnqueueProto(v1.STOP_REQ)
}

func (h *Handler) enqueueTxBeginSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(v1.RESP_CODE_TX_BEGIN))
	header = append(header, cID...)
	header = append(header, byte(v1.STATUS_OK))
	h.out.EnqueueProto(header)
	pool.Put(header)
}

func (h *Handler) enqueueTxBeginErr(cID []byte, err error) {
	header := pool.Get(5)
	header = append(header, byte(v1.RESP_CODE_TX_BEGIN))
	header = append(header, cID...)
	errBuf := operationErrorBuf(err)
	h.out.EnqueueProtoMulti(header, errBuf)
	pool.Put(header)
	pool.Put(errBuf)
}

func (h *Handler) enqueueTxCommitSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(v1.RESP_CODE_TX_COMMIT))
	header = append(header, cID...)
	header = append(header, byte(v1.STATUS_OK))
	h.out.EnqueueProto(header)
	pool.Put(header)
}

func (h *Handler) enqueueTxCommitErr(cID []byte, err error) {
	header := pool.Get(5)
	header = append(header, byte(v1.RESP_CODE_TX_COMMIT))
	header = append(header, cID...)
	errBuf := operationErrorBuf(err)
	h.out.EnqueueProtoMulti(header, errBuf)
	pool.Put(header)
	pool.Put(errBuf)
}

func (h *Handler) enqueueTxRollbackSuccess(cID []byte) {
	header := pool.Get(6)
	header = append(header, byte(v1.RESP_CODE_TX_ROLLBACK))
	header = append(header, cID...)
	header = append(header, byte(v1.STATUS_OK))
	h.out.EnqueueProto(header)
	pool.Put(header)
}

func (h *Handler) enqueueTxRollbackErr(cID []byte, err error) {
	header := pool.Get(5)
	header = append(header, byte(v1.RESP_CODE_TX_ROLLBACK))
	header = append(header, cID...)
	errBuf := operationErrorBuf(err)
	h.out.EnqueueProtoMulti(header, errBuf)
	pool.Put(header)
	pool.Put(errBuf)
}

func enqueueSubscribeSuccess(out *Outbound, code v1.RespCode, cID []byte, subID byte) {
	sbuf := pool.Get(v1.Uint32Len)
	sbuf = append(sbuf, byte(code))
	sbuf = append(sbuf, cID...)
	sbuf = append(sbuf, byte(v1.STATUS_OK), subID)
	out.EnqueueProto(sbuf)
	pool.Put(sbuf)
}

func enqueueUnsubscribeResponse(out *Outbound, cID []byte, err error) {
	header := pool.Get(6)
	header = append(header, byte(v1.RESP_CODE_UNSUBSCRIBE))
	header = append(header, cID...)
	if err == nil {
		header = append(header, byte(v1.STATUS_OK))
		out.EnqueueProto(header)
		pool.Put(header)
		return
	}
	errBuf := operationErrorBuf(err)
	out.EnqueueProtoMulti(header, errBuf)
	pool.Put(header)
	pool.Put(errBuf)
}

func enqueueSubscriptionMessage(
	out *Outbound,
	subscriptionID byte,
	reader connector.Reader,
	headered bool,
	payload []byte,
	source string,
	headers [][]byte,
	args ...any,
) {
	if reader.AutoCommit() {
		enqueueAutoCommitSubscriptionMessage(out, subscriptionID, headered, payload, headers)
		return
	}

	messageIDSize := len(source) + reader.MsgIDArgsLen()
	capacity := subscriptionFrameSize(headered, payload, headers) + v1.Uint32Len + messageIDSize
	buf := pool.Get(capacity)
	buf = appendSubscriptionFramePrefix(buf, subscriptionID, headered, headers)
	buf = binary.BigEndian.AppendUint32(buf, uint32(messageIDSize))
	buf = reader.EncodeMsgID(buf, source, args...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	buf = append(buf, payload...)
	out.EnqueueOwned(buf)
}

func enqueueAutoCommitSubscriptionMessage(out *Outbound, subscriptionID byte, headered bool, payload []byte, headers [][]byte) {
	capacity := subscriptionFrameSize(headered, payload, headers)
	if headered && capacity <= pool.SIZE_TINY {
		var stack [pool.SIZE_TINY]byte
		buf := appendSubscriptionFramePrefix(stack[:0], subscriptionID, headered, headers)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
		buf = append(buf, payload...)
		out.EnqueueProto(buf)
		return
	}

	buf := pool.Get(capacity)
	buf = appendSubscriptionFramePrefix(buf, subscriptionID, headered, headers)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	buf = append(buf, payload...)
	out.EnqueueOwned(buf)
}

func subscriptionFrameSize(headered bool, payload []byte, headers [][]byte) int {
	size := 2 + v1.Uint32Len + len(payload)
	if headered {
		size += v1.Uint16Len
		for _, header := range headers {
			size += v1.Uint32Len + len(header)
		}
	}
	return size
}

func appendSubscriptionFramePrefix(buf []byte, subscriptionID byte, headered bool, headers [][]byte) []byte {
	op := v1.RESP_CODE_MSG
	if headered {
		op = v1.RESP_CODE_HMSG
	}
	buf = append(buf, byte(op), subscriptionID)
	if headered {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(headers)))
		for _, header := range headers {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(header)))
			buf = append(buf, header...)
		}
	}
	return buf
}

func enqueueSubscribeErr(out *Outbound, cID []byte, respCode v1.RespCode, err error) {
	header := pool.Get(5)
	header = append(header, byte(respCode))
	header = append(header, cID...)
	errBuf := operationErrorBuf(err)
	out.EnqueueProtoMulti(header, errBuf)
	pool.Put(header)
	pool.Put(errBuf)
}

func operationErrorBuf(err error) []byte {
	operationErr := core.ClassifyError(err)
	capacity := 2 + 2*v1.Uint32Len + len(operationErr.Reason) + len(operationErr.Message) + v1.Uint16Len
	keys := make([]string, 0, len(operationErr.Details))
	for key, value := range operationErr.Details {
		keys = append(keys, key)
		capacity += 2*v1.Uint32Len + len(key) + len(value)
	}
	sort.Strings(keys)
	buf := pool.Get(capacity)
	buf = append(buf, byte(operationErr.Code), byte(operationErr.Outcome))
	buf = appendProtocolString(buf, operationErr.Reason)
	buf = appendProtocolString(buf, operationErr.Message)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(keys)))
	for _, key := range keys {
		buf = appendProtocolString(buf, key)
		buf = appendProtocolString(buf, operationErr.Details[key])
	}
	return buf
}

func appendProtocolString(buf []byte, value string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(value)))
	return append(buf, value...)
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
