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
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/quic-go/quic-go"
)

type AckResponse struct {
	Err             error
	AckMsgResponses []AckMsgResponse
}

type AckMsgResponse struct {
	MsgID []byte
	Err   error
}

type clientReader struct {
	conn *Conn

	ps  *parseState
	r   quic.Stream
	out *fujin.Outbound

	cm *correlator
	ac *ackCorrelator

	closed       atomic.Bool
	disconnectCh chan struct{}
	wg           sync.WaitGroup
}

func (c *clientReader) CheckParseStateAfterOpForTests() error {
	if c.ps.state != OP_START {
		return fmt.Errorf("invalid state: %d", c.ps.state)
	}

	if c.ps.argBuf != nil {
		return errors.New("arg buf is not nil")
	}
	if c.ps.metaBuf != nil {
		return errors.New("meta buf is not nil")
	}
	if c.ps.payloadBuf != nil {
		return errors.New("payload buf is not nil")
	}

	ea := errArg{}
	if c.ps.ea != ea {
		return errors.New("err arg is not empty")
	}

	ma := msgArg{}
	if c.ps.ma != ma {
		return errors.New("msg arg is not empty")
	}

	if c.ps.fa.n != 0 || c.ps.fa.err != nil || c.ps.fa.handled != 0 || len(c.ps.fa.msgs) != 0 {
		return errors.New("fetch arg is not empty")
	}

	if c.ps.aa.currMsgIDLen != 0 || c.ps.aa.n != 0 || len(c.ps.aa.resps) != 0 {
		return errors.New("ack arg is not empty")
	}

	if c.ps.ca.cID != nil || c.ps.ca.cIDUint32 != 0 {
		return errors.New("correlation id arg is not empty")
	}

	return nil
}

func (c *clientReader) ack(id []byte) (AckResponse, error) {
	return c.sendAckCmd(byte(request.OP_CODE_ACK), id)
}

func (c *clientReader) nack(id []byte) (AckResponse, error) {
	return c.sendAckCmd(byte(request.OP_CODE_NACK), id)
}

func (c *clientReader) sendAckCmd(cmd byte, msgID []byte) (AckResponse, error) {
	if c.closed.Load() {
		return AckResponse{}, ErrWriterClosed
	}

	buf := pool.Get(9)
	defer pool.Put(buf)

	ch := make(chan AckResponse, 1)
	defer close(ch)

	correlationID := c.ac.next(ch)
	defer c.ac.delete(correlationID)

	buf = append(buf, cmd)
	buf = binary.BigEndian.AppendUint32(buf, correlationID)
	buf = binary.BigEndian.AppendUint32(buf, 1)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgID)))

	c.out.EnqueueProtoMulti(buf, msgID)

	select {
	case <-time.After(c.conn.timeout):
		return AckResponse{}, ErrTimeout
	case resp := <-ch:
		return resp, nil
	}
}

func (c *Conn) connectReader(conf ReaderConfig, typ reader.ReaderType) (*clientReader, error) {
	if c == nil {
		return nil, ErrConnClosed
	}

	if c.closed.Load() {
		return nil, ErrConnClosed
	}

	if err := conf.Validate(); err != nil {
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
		ac:           newAckCorrelator(),
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

	if err := r.parseConnectReader(rBuf[:n]); err != nil {
		stream.Close()
		return nil, fmt.Errorf("parse connect reader: %w", err)
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

func (c *clientReader) parseConnectReader(buf []byte) error {
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
				c.ps.state = OP_ERROR_CODE_ARG
			default:
				c.r.Close()
				c.ps.state = OP_START
				return ErrParseProto
			}
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				c.ps.state = OP_START
				return nil
			case byte(response.ERR_CODE_YES):
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.state = OP_ERROR_PAYLOAD_ARG
			default:
				c.r.Close()
				c.ps.state = OP_START
				return ErrParseProto
			}
		case OP_ERROR_PAYLOAD_ARG:
			c.ps.argBuf = append(c.ps.argBuf, b)
			if len(c.ps.argBuf) >= fujin.Uint32Len {
				if err := c.parseErrLenArg(); err != nil {
					pool.Put(c.ps.argBuf)
					c.r.Close()
					c.ps.state = OP_START
					return fmt.Errorf("parse err len arg: %w", err)
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
					return errors.New(string(c.ps.payloadBuf))
				}
			} else {
				c.ps.payloadBuf = pool.Get(int(c.ps.ea.errLen))
				c.ps.payloadBuf = append(c.ps.payloadBuf, b)

				if len(c.ps.payloadBuf) >= int(c.ps.ea.errLen) {
					defer pool.Put(c.ps.payloadBuf)
					c.ps.state = OP_START
					return errors.New(string(c.ps.payloadBuf))
				}
			}
		}
	}

	return ErrParseProto
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
