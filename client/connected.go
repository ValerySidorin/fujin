package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/quic-go/quic-go"
)

type connected struct {
	conn *Conn

	ps  *parseState
	r   quic.Stream
	out *fujin.Outbound

	cm *correlator

	msgMetaLen int

	closed       atomic.Bool
	disconnectCh chan struct{}
	wg           sync.WaitGroup
}

func (c *connected) readLoop(parse func(buf []byte) error) {
	defer c.wg.Done()

	buf := pool.Get(ReadBufferSize)[:ReadBufferSize]
	defer pool.Put(buf)

	for {
		n, err := c.r.Read(buf)
		if err != nil {
			if err == io.EOF {
				if n != 0 {
					err = parse(buf[:n])
					if err != nil {
						c.conn.l.Error("read loop", "err", err)
						return
					}
				}
				return
			}
		}

		if n == 0 {
			continue
		}

		err = parse(buf[:n])
		if err != nil {
			c.conn.l.Error("read loop", "err", err)
			return
		}
	}
}

func (c *connected) parseConnectReader(buf []byte) (byte, error) {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]

		switch c.ps.state {
		case OP_START:
			switch b {
			case byte(response.RESP_CODE_CONNECT_READER):
				c.ps.state = OP_CONNECT_READER
			default:
				c.r.Close()
				c.ps.state = OP_START
				return 0, ErrParseProto
			}
		case OP_CONNECT_READER:
			c.ps.cra.msgMetaLen = b
			c.ps.state = OP_ERROR_CODE_ARG
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				c.ps.state = OP_START
				return c.ps.cra.msgMetaLen, nil
			case byte(response.ERR_CODE_YES):
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.state = OP_ERROR_PAYLOAD_ARG
			default:
				c.r.Close()
				c.ps.state = OP_START
				return 0, ErrParseProto
			}
		case OP_ERROR_PAYLOAD_ARG:
			c.ps.argBuf = append(c.ps.argBuf, b)
			if len(c.ps.argBuf) >= fujin.Uint32Len {
				if err := c.parseErrLenArg(); err != nil {
					pool.Put(c.ps.argBuf)
					c.r.Close()
					c.ps.state = OP_START
					return 0, fmt.Errorf("parse err len arg: %w", err)
				}
				pool.Put(c.ps.argBuf)
				c.ps.argBuf, c.ps.state = nil, OP_ERROR_PAYLOAD
			}
		case OP_ERROR_PAYLOAD:
			if c.ps.payloadBuf != nil {
				toCopy := int(c.ps.ea.errLen) - len(c.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(c.ps.payloadBuf)
					c.ps.payloadBuf = c.ps.payloadBuf[:start+toCopy]
					copy(c.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					c.ps.payloadBuf = append(c.ps.payloadBuf, b)
				}

				if len(c.ps.payloadBuf) >= int(c.ps.ea.errLen) {
					defer pool.Put(c.ps.payloadBuf)
					c.ps.state = OP_START
					return 0, errors.New(string(c.ps.payloadBuf))
				}
			} else {
				c.ps.payloadBuf = pool.Get(int(c.ps.ea.errLen))
				c.ps.payloadBuf = append(c.ps.payloadBuf, b)

				if len(c.ps.payloadBuf) >= int(c.ps.ea.errLen) {
					defer pool.Put(c.ps.payloadBuf)
					c.ps.state = OP_START
					return 0, errors.New(string(c.ps.payloadBuf))
				}
			}
		}
	}

	return 0, ErrParseProto
}

func (c *connected) Close() error {
	if c.closed.Load() {
		return nil
	}

	c.closed.Store(true)

	c.out.EnqueueProto(DISCONNECT_REQ)
	select {
	case <-time.After(c.conn.timeout):
	case <-c.disconnectCh:
	}

	c.out.Close()
	c.r.Close()

	c.wg.Wait()

	if c.ps.argBuf != nil {
		pool.Put(c.ps.argBuf)
	}

	if c.ps.payloadBuf != nil {
		pool.Put(c.ps.payloadBuf)
	}

	if c.ps.metaBuf != nil {
		pool.Put(c.ps.metaBuf)
	}

	return nil
}

func (c *connected) parseErrLenArg() error {
	c.ps.ea.errLen = binary.BigEndian.Uint32(c.ps.argBuf[0:fujin.Uint32Len])
	if c.ps.ea.errLen == 0 {
		return ErrParseProto
	}

	return nil
}

func (c *connected) parseMsgLenArg() error {
	c.ps.ma.len = binary.BigEndian.Uint32(c.ps.argBuf[0:fujin.Uint32Len])
	if c.ps.ma.len == 0 {
		return ErrParseProto
	}

	return nil
}
