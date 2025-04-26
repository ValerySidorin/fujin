package client

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/panjf2000/ants/v2"
)

type Subscriber struct {
	*clientReader
	h       func(Msg)
	msgs    chan Msg
	handled chan struct{}
}

func (c *Conn) ConnectSubscriber(conf ReaderConfig, handler func(msg Msg)) (*Subscriber, error) {
	r, err := c.connectReader(conf, reader.Subscriber)
	if err != nil {
		return nil, fmt.Errorf("connect reader: %w", err)
	}

	h := func(msg Msg) {
		handler(msg)
		if msg.meta != nil {
			pool.Put(msg.meta)
		}
		pool.Put(msg.Value)
	}

	s := &Subscriber{
		clientReader: r,
		h:            h,
		msgs:         make(chan Msg, 1),
		handled:      make(chan struct{}),
	}

	if conf.Async {
		pool, err := ants.NewPool(conf.Pool.Size)
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
	if err := s.clientReader.Close(); err != nil {
		return err
	}

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
				s.ps.state = OP_MSG
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
		case OP_MSG:
			if s.msgMetaLen == 0 {
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.argBuf = append(s.ps.argBuf, b)
				s.ps.state = OP_MSG_ARG
				continue
			}
			s.ps.metaBuf = pool.Get(int(s.msgMetaLen))
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			s.ps.state = OP_MSG_META_ARG
		case OP_MSG_META_ARG:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.msgMetaLen) {
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_MSG_ARG
				continue
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
						meta:  s.ps.metaBuf,
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
						meta:  s.ps.metaBuf,
						r:     s.clientReader,
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			}
		case OP_CORRELATION_ID_ARG:
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
				s.ps.state = OP_ERROR_CODE_ARG
			}
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.cm.send(s.ps.ca.cIDUint32, nil)
				pool.Put(s.ps.ca.cID)
				s.ps.ca, s.ps.state = correlationIDArg{}, OP_START
				continue
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_ERROR_PAYLOAD_ARG
			default:
				s.r.Close()
				return ErrParseProto
			}
		case OP_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.r.Close()
					return fmt.Errorf("parse write err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_ERROR_PAYLOAD
			}
		case OP_ERROR_PAYLOAD:
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
		case OP_ACK:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_NACK:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		default:
			s.r.Close()
			return ErrParseProto
		}
	}

	fmt.Println("parsed")
	return nil
}

func (s *Subscriber) handle() {
	for msg := range s.msgs {
		s.h(msg)
	}
	close(s.handled)
}
