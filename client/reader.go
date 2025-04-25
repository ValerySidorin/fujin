package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/panjf2000/ants/v2"
	"github.com/quic-go/quic-go"
)

type clientReader struct {
	conf ReaderConfig

	conn *Conn

	ps  *parseState
	r   quic.Stream
	out *fujin.Outbound

	pool *ants.Pool

	cm *correlator

	msgMetaLen int

	closed       atomic.Bool
	disconnectCh chan struct{}
	wg           sync.WaitGroup
}

func (c *Conn) connectReader(conf ReaderConfig, typ reader.ReaderType) (*clientReader, error) {
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

	r := &clientReader{
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
		byte(typ),
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

	msgMetaLen, err := r.parseConnectReader(rBuf[:n])
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("parse connect reader: %w", err)
	}

	r.msgMetaLen = int(msgMetaLen)

	if conf.Async {
		pool, err := ants.NewPool(conf.Pool.Size)
		if err != nil {
			return nil, fmt.Errorf("new pool: %w", err)
		}

		r.pool = pool
	}

	return r, nil
}

func (c *clientReader) start(parse func(buf []byte) error) {
	c.wg.Add(2)
	go c.readLoop(parse)
	go func() {
		defer c.wg.Done()
		c.out.WriteLoop()
	}()
}

func (c *clientReader) readLoop(parse func(buf []byte) error) {
	defer c.wg.Done()

	buf := pool.Get(ReadBufferSize)[:ReadBufferSize]
	defer pool.Put(buf)

	for {
		n, err := c.r.Read(buf)
		if err != nil {
			if err == io.EOF {
				if n != 0 {
					fmt.Println("read:", buf[:n])
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

		fmt.Println("read:", buf[:n])
		err = parse(buf[:n])
		if err != nil {
			c.conn.l.Error("read loop", "err", err)
			return
		}
	}
}

func (c *clientReader) parseConnectReader(buf []byte) (byte, error) {
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

func (c *clientReader) Close() error {
	if c.closed.Load() {
		return nil
	}

	c.closed.Store(true)

	c.out.EnqueueProto(DISCONNECT_REQ)
	select {
	case <-time.After(c.conn.timeout):
	case <-c.disconnectCh:
	}

	c.r.Close()

	if c.pool != nil {
		if c.conf.Pool.ReleaseTimeout != 0 {
			if err := c.pool.ReleaseTimeout(c.conf.Pool.ReleaseTimeout); err != nil {
				return fmt.Errorf("release pool: %w", err)
			}
		} else {
			c.pool.Release()
		}
	}

	c.out.Close()
	c.wg.Wait()
	return nil
}

func (c *clientReader) parseErrLenArg() error {
	c.ps.ea.errLen = binary.BigEndian.Uint32(c.ps.argBuf[0:fujin.Uint32Len])
	if c.ps.ea.errLen == 0 {
		return ErrParseProto
	}

	return nil
}

func (c *clientReader) parseMsgLenArg() error {
	c.ps.ma.len = binary.BigEndian.Uint32(c.ps.argBuf[0:fujin.Uint32Len])
	if c.ps.ma.len == 0 {
		return ErrParseProto
	}

	return nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
