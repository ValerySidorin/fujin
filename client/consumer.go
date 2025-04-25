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

func (c *Consumer) Fetch(n uint32, handler func(msg Msg)) error {
	if c.closed.Load() {
		return ErrWriterClosed
	}

	buf := pool.Get(9)
	defer pool.Put(buf)

	mCh := make(chan Msg, 1)
	eCh := make(chan error, 1)

	id := c.fcm.next(mCh, eCh)
	defer c.fcm.delete(id)

	buf = append(buf, byte(request.OP_CODE_FETCH))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, n)

	c.out.EnqueueProto(buf)

	select {
	case <-time.After(c.conn.timeout):
		return ErrTimeout
	case err := <-eCh:
		if err != nil {
			return err
		}
	}

	for msg := range mCh {
		handler(msg)
		if msg.meta != nil {
			pool.Put(msg.meta)
		}
		pool.Put(msg.Value)
	}

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
			c.ps.state = OP_CORRELATION_ID_ARG
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
				c.ps.argBuf, c.ps.state = nil, OP_ERROR_CODE_ARG
			}
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				pool.Put(c.ps.ca.cID)
				c.ps.state = OP_START
				continue
			case byte(response.ERR_CODE_YES):
				close(c.ps.fa.msgs)
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
					err = fmt.Errorf("parse write err len arg: %w", err)
					c.ps.fa.err <- err
					close(c.ps.fa.err)
					return err
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
					c.ps.fa.err <- errors.New(string(c.ps.payloadBuf))
					close(c.ps.fa.err)
					pool.Put(c.ps.payloadBuf)
					c.ps.ca, c.ps.payloadBuf, c.ps.ea, c.ps.state = correlationIDArg{}, nil, errArg{}, OP_START
				}
			} else {
				c.ps.payloadBuf = pool.Get(int(c.ps.ea.errLen))
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
			c.ps.metaBuf = pool.Get(int(c.msgMetaLen))
		case OP_MSG_META_ARG:
			if len(c.ps.metaBuf) >= int(c.msgMetaLen) {
				c.ps.argBuf = pool.Get(fujin.Uint32Len)
				c.ps.argBuf = append(c.ps.argBuf, b)
				c.ps.state = OP_MSG_ARG
				continue
			}
			c.ps.metaBuf = append(c.ps.metaBuf, b)
		case OP_MSG_ARG:
			c.ps.argBuf = append(c.ps.argBuf, b)
			if len(c.ps.argBuf) >= fujin.Uint32Len {
				if err := c.parseMsgLenArg(); err != nil {
					pool.Put(c.ps.argBuf)
					c.r.Close()
					err = fmt.Errorf("parse msg len arg: %w", err)
					c.ps.fa.err <- err
					close(c.ps.fa.err)
					close(c.ps.fa.msgs)
					return err
				}
				close(c.ps.fa.err)
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
					c.ps.fa.msgs <- Msg{
						Value: c.ps.payloadBuf,
						meta:  c.ps.metaBuf,
						r:     c.clientReader,
					}
					c.ps.fa.handled++
					if c.ps.fa.handled >= c.ps.fa.n {
						close(c.ps.fa.msgs)
						c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
					}
					c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
				}
			} else {
				c.ps.payloadBuf = pool.Get(int(c.ps.ma.len))
				c.ps.payloadBuf = append(c.ps.payloadBuf, b)

				if len(c.ps.payloadBuf) >= int(c.ps.ma.len) {
					c.ps.fa.msgs <- Msg{
						Value: c.ps.payloadBuf,
						meta:  c.ps.metaBuf,
						r:     c.clientReader,
					}
					c.ps.fa.handled++
					if c.ps.fa.handled >= c.ps.fa.n {
						close(c.ps.fa.msgs)
						c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
					}
					c.ps.metaBuf, c.ps.payloadBuf, c.ps.ma, c.ps.fa, c.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
				}
			}
		default:
			c.r.Close()
			return ErrParseProto
		}
	}

	return nil
}
