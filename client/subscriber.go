package client

import (
	"encoding/binary"
	"fmt"

	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/panjf2000/ants/v2"
)

type Subscriber struct {
	conf SubscriberConfig
	*connected
	h func(Msg)
}

func (c *Conn) ConnectSubscriber(conf SubscriberConfig, handler func(msg Msg)) (*Subscriber, error) {
	if c == nil {
		return nil, ErrConnClosed
	}

	if c.closed.Load() {
		return nil, ErrConnClosed
	}

	if err := conf.ValidateAndSetDefaults(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	stream, err := c.qconn.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	out := fujin.NewOutbound(stream, c.wdl, c.l)

	connected := &connected{
		conn:         c,
		r:            stream,
		ps:           &parseState{},
		out:          out,
		cm:           newCorrelator(),
		disconnectCh: make(chan struct{}),
	}

	buf := pool.Get(7 + len(conf.Topic))
	defer pool.Put(buf)

	buf = append(buf,
		byte(request.OP_CODE_CONNECT_READER),
		1,
		boolToByte(conf.AutoCommit),
	)

	buf = binary.BigEndian.AppendUint32(buf, uint32(len(conf.Topic)))
	buf = append(buf, conf.Topic...)

	if _, err := stream.Write(buf); err != nil {
		stream.Close()
		return nil, fmt.Errorf("write connect reader: %w", err)
	}

	rBuf := pool.Get(ReadBufferSize)[:ReadBufferSize]
	defer pool.Put(rBuf)

	n, err := stream.Read(rBuf)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("read connect reader: %w", err)
	}

	msgMetaLen, err := connected.parseConnectReader(rBuf[:n])
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("parse connect reader: %w", err)
	}

	connected.msgMetaLen = int(msgMetaLen)

	h := func(msg Msg) {
		handler(msg)
		if msg.Meta != nil {
			pool.Put(msg.Meta)
		}
		pool.Put(msg.Payload)
	}

	s := &Subscriber{
		connected: connected,
		h:         h,
	}

	if conf.Async {
		pool, err := ants.NewPool(conf.Pool.Size)
		if err != nil {
			return nil, fmt.Errorf("new pool: %w", err)
		}

		s.pool = pool
		s.h = func(msg Msg) {
			pool.Submit(func() {
				h(msg)
			})
		}
	}

	s.wg.Add(2)
	go s.readLoop(s.parse)
	go func() {
		defer s.wg.Done()
		out.WriteLoop()
	}()

	return s, nil
}

func (s *Subscriber) MsgMetaLen() int {
	return s.msgMetaLen
}

func (s *Subscriber) Close() error {
	return s.connected.Close(s.conf.Pool.ReleaseTimeout)
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
						Meta:    s.ps.metaBuf,
						Payload: s.ps.payloadBuf,
					})
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ma.len))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.h(Msg{
						Meta:    s.ps.metaBuf,
						Payload: s.ps.payloadBuf,
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
