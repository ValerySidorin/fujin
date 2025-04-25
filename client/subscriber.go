package client

import (
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
	h func(Msg)
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

	return s, nil
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
		case OP_MSG_META_ARG:
			if len(s.ps.metaBuf) >= int(s.msgMetaLen) {
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.argBuf = append(s.ps.argBuf, b)
				s.ps.state = OP_MSG_ARG
				continue
			}
			s.ps.metaBuf = append(s.ps.metaBuf, b)
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
					s.h(Msg{
						Value: s.ps.payloadBuf,
						meta:  s.ps.metaBuf,
						r:     s.clientReader,
					})
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ma.len))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.h(Msg{
						Value: s.ps.payloadBuf,
						meta:  s.ps.metaBuf,
						r:     s.clientReader,
					})
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			}
		default:
			s.r.Close()
			return ErrParseProto
		}
	}

	return nil
}
