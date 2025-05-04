package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/panjf2000/ants/v2"
)

type SubscriberConfig struct {
	ReaderConfig
	MsgBufSize int
	Pool       PoolConfig
}

type Subscriber struct {
	conf SubscriberConfig
	*clientReader

	pool *ants.Pool

	h       func(Msg)
	msgs    chan Msg
	handled chan struct{}
}

func (c *SubscriberConfig) SetDefaults() {
	if c.MsgBufSize <= 0 {
		c.MsgBufSize = 1000
	}

	if c.Async {
		if c.Pool.Size == 0 {
			c.Pool.Size = 1000
		}
		if c.Pool.ReleaseTimeout == 0 {
			c.Pool.ReleaseTimeout = 5 * time.Second
		}
	}
}

func (c *Conn) ConnectSubscriber(conf SubscriberConfig, handler func(msg Msg)) (*Subscriber, error) {
	conf.SetDefaults()

	r, err := c.connectReader(conf.ReaderConfig, reader.Subscriber)
	if err != nil {
		return nil, fmt.Errorf("connect reader: %w", err)
	}

	h := func(msg Msg) {
		handler(msg)
		if msg.id != nil {
			pool.Put(msg.id)
		}
		pool.Put(msg.Value)
	}

	s := &Subscriber{
		conf:         conf,
		clientReader: r,
		h:            h,
		msgs:         make(chan Msg, conf.MsgBufSize),
		handled:      make(chan struct{}),
	}

	if conf.Async {
		pool, err := ants.NewPool(conf.Pool.Size, ants.WithPreAlloc(conf.Pool.PreAlloc))
		if err != nil {
			return nil, fmt.Errorf("new pool: %w", err)
		}

		s.pool = pool
		s.h = func(msg Msg) {
			_ = pool.Submit(func() {
				h(msg)
			})
		}
	}

	s.start(s.parse)
	go s.handle()

	return s, nil
}

func (s *Subscriber) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)

	s.out.EnqueueProto(DISCONNECT_REQ)
	select {
	case <-time.After(s.conn.timeout):
	case <-s.disconnectCh:
	}

	s.r.Close()

	if s.pool != nil {
		if s.conf.Pool.ReleaseTimeout != 0 {
			if err := s.pool.ReleaseTimeout(s.conf.Pool.ReleaseTimeout); err != nil {
				return fmt.Errorf("release pool: %w", err)
			}
		} else {
			s.pool.Release()
		}
	}

	s.out.Close()
	s.wg.Wait()

	close(s.msgs)
	<-s.handled
	return nil
}

func (s *Subscriber) parse(buf []byte) error {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]

		switch s.ps.state {
		case OP_START:
			switch b {
			case byte(response.RESP_CODE_MSG):
				if s.conf.AutoCommit {
					s.ps.state = OP_MSG_ARG
					continue
				}
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_MSG_ID_ARG
			case byte(response.RESP_CODE_ACK):
				s.ps.state = OP_ACK
			case byte(response.RESP_CODE_NACK):
				s.ps.state = OP_NACK
			case byte(response.RESP_CODE_DISCONNECT):
				close(s.disconnectCh)
				return nil
			case byte(request.OP_CODE_STOP):
				go s.Close()
				return nil
			}
		case OP_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.ma.idLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, pool.Get(int(s.ps.ma.idLen))
				s.ps.state = OP_MSG_ID_PAYLOAD
			}
		case OP_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.ma.idLen) {
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_MSG_ARG
			}
		case OP_MSG_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseMsgLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.r.Close()
					return fmt.Errorf("parse msg len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_MSG_PAYLOAD
			}
		case OP_MSG_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ma.len) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.msgs <- Msg{
						Value: s.ps.payloadBuf,
						id:    s.ps.metaBuf,
						r:     s.clientReader,
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ma.len))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.msgs <- Msg{
						Value: s.ps.payloadBuf,
						id:    s.ps.metaBuf,
						r:     s.clientReader,
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			}
		case OP_ACK:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_ACK_CORRELATION_ID_ARG
		case OP_ACK_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(s.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.ca.cID)
				s.ps.ca.cID = s.ps.ca.cID[:start+toCopy]
				copy(s.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.ca.cID = append(s.ps.ca.cID, b)
			}

			if len(s.ps.ca.cID) >= fujin.Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				s.ps.state = OP_ACK_ERROR_CODE_ARG
			}
		case OP_ACK_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_ACK_MSG_BATCH_LEN_ARG
				continue
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_ACK_ERROR_PAYLOAD_ARG
			default:
				s.r.Close()
				return ErrParseProto
			}
		case OP_ACK_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.r.Close()
					return fmt.Errorf("parse write err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_ACK_ERROR_PAYLOAD
			}
		case OP_ACK_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ea, s.ps.state = nil, nil, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ea, s.ps.state = nil, nil, errArg{}, OP_START
				}
			}
		case OP_ACK_MSG_BATCH_LEN_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.aa.n = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = pool.Get(fujin.Uint32Len), OP_ACK_MSG_ID_ARG
			}
		case OP_ACK_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.aa.currMsgIDLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, make([]byte, s.ps.aa.currMsgIDLen)
				s.ps.state = OP_ACK_MSG_ID_PAYLOAD
			}
		case OP_ACK_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.aa.currMsgIDLen) {
				s.ps.state = OP_ACK_MSG_ERROR_CODE_ARG
			}
		case OP_ACK_MSG_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.ps.aa.resps = append(s.ps.aa.resps, AckMsgResponse{
					MsgID: s.ps.metaBuf,
				})
				s.ps.metaBuf = nil
				if len(s.ps.aa.resps) >= int(s.ps.aa.n) {
					s.ac.send(s.ps.ca.cIDUint32, AckResponse{
						AckMsgResponses: s.ps.aa.resps,
					})
					s.ps.aa, s.ps.ca, s.ps.state = ackArg{}, correlationIDArg{}, OP_START
					continue
				}
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_ACK_MSG_ID_ARG
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_ACK_MSG_ERROR_PAYLOAD_ARG
			default:
				s.r.Close()
				return ErrParseProto
			}
		case OP_ACK_MSG_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.r.Close()
					return fmt.Errorf("parse ack msg err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_ACK_MSG_ERROR_PAYLOAD
			}
		case OP_ACK_MSG_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					msgID := make([]byte, len(s.ps.metaBuf))
					copy(msgID, s.ps.metaBuf)
					pool.Put(s.ps.metaBuf)
					s.ps.metaBuf = nil
					s.ps.aa.resps = append(s.ps.aa.resps, AckMsgResponse{
						MsgID: msgID,
						Err:   errors.New(string(s.ps.payloadBuf)),
					})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf = nil
					if len(s.ps.aa.resps) >= int(s.ps.aa.n) {
						s.ac.send(s.ps.ca.cIDUint32, AckResponse{
							AckMsgResponses: s.ps.aa.resps,
						})
						s.ps.ca.cID, s.ps.aa, s.ps.ea, s.ps.ca, s.ps.state = nil, ackArg{}, errArg{}, correlationIDArg{}, OP_START
						continue
					}
					s.ps.ea, s.ps.state = errArg{}, OP_ACK_MSG_ID_ARG
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					msgID := make([]byte, len(s.ps.metaBuf))
					copy(msgID, s.ps.metaBuf)
					pool.Put(s.ps.metaBuf)
					s.ps.metaBuf = nil
					s.ps.aa.resps = append(s.ps.aa.resps, AckMsgResponse{
						MsgID: msgID,
						Err:   errors.New(string(s.ps.payloadBuf)),
					})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf = nil
					if len(s.ps.aa.resps) >= int(s.ps.aa.n) {
						s.ac.send(s.ps.ca.cIDUint32, AckResponse{
							AckMsgResponses: s.ps.aa.resps,
						})
						s.ps.ca.cID, s.ps.aa, s.ps.ea, s.ps.ca, s.ps.state = nil, ackArg{}, errArg{}, correlationIDArg{}, OP_START
						continue
					}
					s.ps.ea, s.ps.state = errArg{}, OP_ACK_MSG_ID_ARG
				}
			}
		case OP_NACK:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_NACK_CORRELATION_ID_ARG
		case OP_NACK_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(s.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.ca.cID)
				s.ps.ca.cID = s.ps.ca.cID[:start+toCopy]
				copy(s.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.ca.cID = append(s.ps.ca.cID, b)
			}

			if len(s.ps.ca.cID) >= fujin.Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				s.ps.state = OP_NACK_ERROR_CODE_ARG
			}
		case OP_NACK_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_NACK_MSG_BATCH_LEN_ARG
				continue
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_NACK_ERROR_PAYLOAD_ARG
			default:
				s.r.Close()
				return ErrParseProto
			}
		case OP_NACK_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.r.Close()
					return fmt.Errorf("parse write err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_NACK_ERROR_PAYLOAD
			}
		case OP_NACK_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ea, s.ps.state = nil, nil, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ea, s.ps.state = nil, nil, errArg{}, OP_START
				}
			}
		case OP_NACK_MSG_BATCH_LEN_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.aa.n = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = pool.Get(fujin.Uint32Len), OP_NACK_MSG_ID_ARG
			}
		case OP_NACK_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.aa.currMsgIDLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, make([]byte, s.ps.aa.currMsgIDLen)
				s.ps.state = OP_NACK_MSG_ID_PAYLOAD
			}
		case OP_NACK_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.aa.currMsgIDLen) {
				s.ps.state = OP_NACK_MSG_ERROR_CODE_ARG
			}
		case OP_NACK_MSG_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.ps.aa.resps = append(s.ps.aa.resps, AckMsgResponse{
					MsgID: s.ps.metaBuf,
				})
				s.ps.metaBuf = nil
				if len(s.ps.aa.resps) >= int(s.ps.aa.n) {
					s.ac.send(s.ps.ca.cIDUint32, AckResponse{
						AckMsgResponses: s.ps.aa.resps,
					})
					s.ps.aa, s.ps.ca, s.ps.state = ackArg{}, correlationIDArg{}, OP_START
					continue
				}
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_NACK_MSG_ID_ARG
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_NACK_MSG_ERROR_PAYLOAD_ARG
			default:
				s.r.Close()
				return ErrParseProto
			}
		case OP_NACK_MSG_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.r.Close()
					return fmt.Errorf("parse ack msg err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_NACK_MSG_ERROR_PAYLOAD
			}
		case OP_NACK_MSG_ERROR_PAYLOAD:
			if s.ps.payloadBuf != nil {
				toCopy := int(s.ps.ea.errLen) - len(s.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(s.ps.payloadBuf)
					s.ps.payloadBuf = s.ps.payloadBuf[:start+toCopy]
					copy(s.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					s.ps.payloadBuf = append(s.ps.payloadBuf, b)
				}

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					msgID := make([]byte, len(s.ps.metaBuf))
					copy(msgID, s.ps.metaBuf)
					pool.Put(s.ps.metaBuf)
					s.ps.metaBuf = nil
					s.ps.aa.resps = append(s.ps.aa.resps, AckMsgResponse{
						MsgID: msgID,
						Err:   errors.New(string(s.ps.payloadBuf)),
					})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf = nil
					if len(s.ps.aa.resps) >= int(s.ps.aa.n) {
						s.ac.send(s.ps.ca.cIDUint32, AckResponse{
							AckMsgResponses: s.ps.aa.resps,
						})
						s.ps.ca.cID, s.ps.aa, s.ps.ea, s.ps.ca, s.ps.state = nil, ackArg{}, errArg{}, correlationIDArg{}, OP_START
						continue
					}
					s.ps.ea, s.ps.state = errArg{}, OP_NACK_MSG_ID_ARG
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					msgID := make([]byte, len(s.ps.metaBuf))
					copy(msgID, s.ps.metaBuf)
					pool.Put(s.ps.metaBuf)
					s.ps.metaBuf = nil
					s.ps.aa.resps = append(s.ps.aa.resps, AckMsgResponse{
						MsgID: msgID,
						Err:   errors.New(string(s.ps.payloadBuf)),
					})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf = nil
					if len(s.ps.aa.resps) >= int(s.ps.aa.n) {
						s.ac.send(s.ps.ca.cIDUint32, AckResponse{
							AckMsgResponses: s.ps.aa.resps,
						})
						s.ps.ca.cID, s.ps.aa, s.ps.ea, s.ps.ca, s.ps.state = nil, ackArg{}, errArg{}, correlationIDArg{}, OP_START
						continue
					}
					s.ps.ea, s.ps.state = errArg{}, OP_NACK_MSG_ID_ARG
				}
			}
		default:
			s.r.Close()
			return ErrParseProto
		}
	}

	return nil
}

func (s *Subscriber) handle() {
	for msg := range s.msgs {
		s.h(msg)
	}
	close(s.handled)
}
