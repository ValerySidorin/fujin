package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/internal/fujin"
	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/response"
)

type Consumer struct {
	*clientReader
	fcm *fetchCorrelator
}

func (c *Conn) ConnectConsumer(conf ReaderConfig) (*Consumer, error) {
	r, err := c.connectReader(conf, reader.Consumer)
	if err != nil {
		return nil, fmt.Errorf("connect reader: %w", err)
	}

	cm := &Consumer{
		clientReader: r,
		fcm:          newFetchCorrelator(),
	}

	cm.start(cm.parse)

	return cm, nil
}

func (c *Consumer) Fetch(ctx context.Context, n uint32) ([]Msg, error) {
	if c.closed.Load() {
		return nil, ErrWriterClosed
	}

	buf := pool.Get(9)
	defer pool.Put(buf)

	eCh := make(chan error, 1)

	id := c.fcm.next(eCh)
	defer c.fcm.delete(id)

	buf = append(buf, byte(request.OP_CODE_FETCH))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, n)

	c.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(ctx, c.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil, ErrTimeout
	case err := <-eCh:
		if err != nil {
			return nil, err
		}

		return c.fcm.getMsgs(id), nil
	}
}

func (c *Consumer) Close() error {
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

	c.out.Close()
	c.wg.Wait()
	return nil
}

func (c *Consumer) parse(buf []byte) error {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]

		switch c.ps.state {
		case OP_START:
			switch b {
			case byte(response.RESP_CODE_FETCH):
				c.ps.state = OP_FETCH
			case byte(response.RESP_CODE_MSG):
				c.ps.state = OP_MSG
			case byte(response.RESP_CODE_ACK):
				c.ps.state = OP_ACK
			case byte(response.RESP_CODE_NACK):
				c.ps.state = OP_NACK
			case byte(response.RESP_CODE_DISCONNECT):
				close(c.disconnectCh)
				return nil
			case byte(request.OP_CODE_STOP):
				go c.Close()
				return nil
			}
		case OP_FETCH:
			c.ps.ca.cID = pool.Get(fujin.Uint32Len)
			c.ps.ca.cID = append(c.ps.ca.cID, b)
			c.ps.state = OP_FETCH_CORRELATION_ID_ARG
		case OP_FETCH_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(c.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(c.ps.ca.cID)
				c.ps.ca.cID = c.ps.ca.cID[:start+toCopy]
				copy(c.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				c.ps.ca.cID = append(c.ps.ca.cID, b)
			}

			if len(c.ps.ca.cID) >= fujin.Uint32Len {
				c.ps.ca.cIDUint32 = binary.BigEndian.Uint32(c.ps.ca.cID)
				c.ps.fa.msgs, c.ps.fa.err = c.fcm.get(c.ps.ca.cIDUint32)
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.state = OP_FETCH_BATCH_NUM_ARG
			}
		case OP_FETCH_BATCH_NUM_ARG:
			toCopy := fujin.Uint32Len - len(c.ps.argBuf)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(c.ps.argBuf)
				c.ps.argBuf = c.ps.argBuf[:start+toCopy]
				copy(c.ps.argBuf[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				c.ps.argBuf = append(c.ps.argBuf, b)
			}

			if len(c.ps.argBuf) >= fujin.Uint32Len {
				c.ps.fa.n = binary.BigEndian.Uint32(c.ps.argBuf)
				pool.Put(c.ps.argBuf)
				c.ps.argBuf, c.ps.state = nil, OP_FETCH_ERROR_CODE_ARG
			}
		case OP_FETCH_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				pool.Put(c.ps.ca.cID)
				c.ps.state = OP_START
				continue
			case byte(response.ERR_CODE_YES):
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.state = OP_FETCH_ERROR_PAYLOAD_ARG
			default:
				c.r.Close()
				return ErrParseProto
			}
		case OP_FETCH_ERROR_PAYLOAD_ARG:
			c.ps.argBuf = append(c.ps.argBuf, b)
			if len(c.ps.argBuf) >= fujin.Uint32Len {
				if err := c.parseErrLenArg(); err != nil {
					pool.Put(c.ps.argBuf)
					pool.Put(c.ps.ca.cID)
					c.r.Close()
					err = fmt.Errorf("parse write err len arg: %w", err)
					c.ps.fa.err <- err
					close(c.ps.fa.err)
					return err
				}
				pool.Put(c.ps.argBuf)
				c.ps.argBuf, c.ps.state = nil, OP_FETCH_ERROR_PAYLOAD
			}
		case OP_FETCH_ERROR_PAYLOAD:
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
					c.ps.fa.err <- errors.New(string(c.ps.payloadBuf))
					close(c.ps.fa.err)
					c.ps.ca, c.ps.payloadBuf, c.ps.ea, c.ps.state = correlationIDArg{}, nil, errArg{}, OP_START
				}
			} else {
				c.ps.payloadBuf = make([]byte, 0, c.ps.ea.errLen)
				c.ps.payloadBuf = append(c.ps.payloadBuf, b)

				if len(c.ps.payloadBuf) >= int(c.ps.ea.errLen) {
					c.ps.fa.err <- errors.New(string(c.ps.payloadBuf))
					close(c.ps.fa.err)
					c.ps.ca, c.ps.payloadBuf, c.ps.ea, c.ps.state = correlationIDArg{}, nil, errArg{}, OP_START
				}
			}
		case OP_MSG:
			if c.msgMetaLen == 0 {
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.argBuf = append(c.ps.argBuf, b)
				c.ps.state = OP_MSG_ARG
				continue
			}
			c.ps.metaBuf = make([]byte, 0, c.msgMetaLen)
			c.ps.metaBuf = append(c.ps.metaBuf, b)
			c.ps.state = OP_MSG_META_ARG
		case OP_MSG_META_ARG:
			c.ps.metaBuf = append(c.ps.metaBuf, b)
			if len(c.ps.metaBuf) >= int(c.msgMetaLen) {
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.state = OP_MSG_ARG
				continue
			}
		case OP_MSG_ARG:
			c.ps.argBuf = append(c.ps.argBuf, b)
			if len(c.ps.argBuf) >= fujin.Uint32Len {
				if err := c.parseMsgLenArg(); err != nil {
					pool.Put(c.ps.argBuf)
					c.r.Close()
					err = fmt.Errorf("parse msg len arg: %w", err)
					c.ps.fa.err <- err
					close(c.ps.fa.err)
					return err
				}
				pool.Put(c.ps.argBuf)
				c.ps.argBuf, c.ps.state = nil, OP_MSG_PAYLOAD
			}
		case OP_MSG_PAYLOAD:
			if c.ps.payloadBuf != nil {
				toCopy := int(c.ps.ma.len) - len(c.ps.payloadBuf)
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

				if len(c.ps.payloadBuf) >= int(c.ps.ma.len) {
					c.ps.fa.msgs = append(c.ps.fa.msgs, Msg{
						Value: c.ps.payloadBuf,
						meta:  c.ps.metaBuf,
						r:     c.clientReader,
					})
					c.ps.fa.handled++
					if c.ps.fa.handled >= c.ps.fa.n {
						c.fcm.setMsgs(c.ps.ca.cIDUint32, c.ps.fa.msgs)
						close(c.ps.fa.err)
						c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
					}
					c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
				}
			} else {
				c.ps.payloadBuf = make([]byte, 0, c.ps.ma.len)
				c.ps.payloadBuf = append(c.ps.payloadBuf, b)

				if len(c.ps.payloadBuf) >= int(c.ps.ma.len) {
					c.ps.fa.msgs = append(c.ps.fa.msgs, Msg{
						Value: c.ps.payloadBuf,
						meta:  c.ps.metaBuf,
						r:     c.clientReader,
					})
					c.ps.fa.handled++
					if c.ps.fa.handled >= c.ps.fa.n {
						c.fcm.setMsgs(c.ps.ca.cIDUint32, c.ps.fa.msgs)
						close(c.ps.fa.err)
						c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
					}
					c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
				}
			}
		case OP_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(c.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(c.ps.ca.cID)
				c.ps.ca.cID = c.ps.ca.cID[:start+toCopy]
				copy(c.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				c.ps.ca.cID = append(c.ps.ca.cID, b)
			}

			if len(c.ps.ca.cID) >= fujin.Uint32Len {
				c.ps.ca.cIDUint32 = binary.BigEndian.Uint32(c.ps.ca.cID)
				c.ps.state = OP_ERROR_CODE_ARG
			}
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				c.cm.send(c.ps.ca.cIDUint32, nil)
				pool.Put(c.ps.ca.cID)
				c.ps.ca, c.ps.state = correlationIDArg{}, OP_START
				continue
			case byte(response.ERR_CODE_YES):
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.state = OP_ERROR_PAYLOAD_ARG
			default:
				c.r.Close()
				return ErrParseProto
			}
		case OP_ERROR_PAYLOAD_ARG:
			c.ps.argBuf = append(c.ps.argBuf, b)
			if len(c.ps.argBuf) >= fujin.Uint32Len {
				if err := c.parseErrLenArg(); err != nil {
					pool.Put(c.ps.argBuf)
					pool.Put(c.ps.ca.cID)
					c.r.Close()
					return fmt.Errorf("parse write err len arg: %w", err)
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
					c.cm.send(c.ps.ca.cIDUint32, errors.New(string(c.ps.payloadBuf)))
					c.ps.ca.cID, c.ps.payloadBuf, c.ps.ea, c.ps.state = nil, nil, errArg{}, OP_START
				}
			} else {
				c.ps.payloadBuf = pool.Get(int(c.ps.ea.errLen))
				c.ps.payloadBuf = append(c.ps.payloadBuf, b)

				if len(c.ps.payloadBuf) >= int(c.ps.ea.errLen) {
					c.cm.send(c.ps.ca.cIDUint32, errors.New(string(c.ps.payloadBuf)))
					c.ps.ca.cID, c.ps.payloadBuf, c.ps.ea, c.ps.state = nil, nil, errArg{}, OP_START
				}
			}
		case OP_ACK:
			c.ps.ca.cID = pool.Get(fujin.Uint32Len)
			c.ps.ca.cID = append(c.ps.ca.cID, b)
			c.ps.state = OP_CORRELATION_ID_ARG
		case OP_NACK:
			c.ps.ca.cID = pool.Get(fujin.Uint32Len)
			c.ps.ca.cID = append(c.ps.ca.cID, b)
			c.ps.state = OP_CORRELATION_ID_ARG
		default:
			c.r.Close()
			return ErrParseProto
		}
	}

	return nil
}
