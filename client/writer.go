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
	"github.com/quic-go/quic-go"
)

var (
	ErrWriterClosed = errors.New("writer closed")
)

type Writer struct {
	conn *Conn

	ps  *parseState
	r   quic.Stream
	out *fujin.Outbound

	wcm *defaultCorrelationManager

	closed       atomic.Bool
	disconnectCh chan struct{}
	wg           sync.WaitGroup
}

func (c *Conn) ConnectWriter(id string) (*Writer, error) {
	if c == nil {
		return nil, ErrConnClosed
	}

	if c.closed.Load() {
		return nil, ErrConnClosed
	}

	stream, err := c.qconn.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("quic: open stream: %w", err)
	}

	buf := pool.Get(5)
	defer pool.Put(buf)

	buf = append(buf, byte(request.OP_CODE_CONNECT_WRITER))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(id)))
	buf = append(buf, id...)

	if _, err := stream.Write(buf); err != nil {
		stream.Close()
		return nil, fmt.Errorf("quic: write connect writer: %w", err)
	}

	out := fujin.NewOutbound(stream, c.wdl, c.l)

	w := &Writer{
		conn:         c,
		out:          out,
		r:            stream,
		ps:           &parseState{},
		wcm:          newDefaultCorrelationManager(),
		disconnectCh: make(chan struct{}),
	}

	w.wg.Add(2)
	go w.readLoop()
	go func() {
		defer w.wg.Done()
		out.WriteLoop()
	}()

	return w, nil
}

func (w *Writer) Write(topic string, p []byte) error {
	if w.closed.Load() {
		return ErrWriterClosed
	}

	buf := pool.Get(len(topic) + len(p) + 13)
	defer pool.Put(buf)

	ch := make(chan error, 1)
	defer close(ch)

	id := w.wcm.next(ch)
	defer w.wcm.delete(id)

	buf = append(buf, byte(request.OP_CODE_WRITE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(p)))
	buf = append(buf, p...)

	w.out.EnqueueProto(buf)

	select {
	case <-time.After(w.conn.timeout):
		return ErrTimeout
	case err := <-ch:
		return err
	}
}

func (w *Writer) Close() error {
	if w.closed.Load() {
		return nil
	}

	w.closed.Store(true)

	w.out.EnqueueProto(DISCONNECT_REQ)
	select {
	case <-time.After(w.conn.timeout):
	case <-w.disconnectCh:
	}

	w.out.Close()
	w.r.Close()

	w.wg.Wait()
	return nil
}

func (w *Writer) readLoop() {
	defer w.wg.Done()

	buf := pool.Get(ReadBufferSize)[:ReadBufferSize]
	defer pool.Put(buf)

	for {
		n, err := w.r.Read(buf)
		if err != nil {
			if err == io.EOF {
				if n != 0 {
					err = w.parse(buf[:n])
					if err != nil {
						w.conn.l.Error("writer read loop", "err", err)
						return
					}
				}
				return
			}
		}

		if n == 0 {
			continue
		}

		err = w.parse(buf[:n])
		if err != nil {
			w.conn.l.Error("writer read loop", "err", err)
			return
		}
	}
}

func (w *Writer) parse(buf []byte) error {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]

		switch w.ps.state {
		case OP_START:
			switch b {
			case byte(response.RESP_CODE_WRITE):
				w.ps.state = OP_WRITE
			case byte(response.RESP_CODE_DISCONNECT):
				close(w.disconnectCh)
				return nil
			case byte(request.OP_CODE_STOP):
				go w.Close() // we probably can do something smarter here
				return nil
			}
		case OP_WRITE:
			w.ps.ca.cID = pool.Get(fujin.Uint32Len)
			w.ps.ca.cID = append(w.ps.ca.cID, b)
			w.ps.state = OP_WRITE_CORRELATION_ID_ARG
		case OP_WRITE_CORRELATION_ID_ARG:
			toCopy := fujin.Uint32Len - len(w.ps.ca.cID)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(w.ps.ca.cID)
				w.ps.ca.cID = w.ps.ca.cID[:start+toCopy]
				copy(w.ps.ca.cID[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				w.ps.ca.cID = append(w.ps.ca.cID, b)
			}

			if len(w.ps.ca.cID) >= fujin.Uint32Len {
				w.ps.state = OP_ERROR_CODE_ARG
			}
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				w.wcm.send(binary.BigEndian.Uint32(w.ps.ca.cID), nil)
				pool.Put(w.ps.ca.cID)
				w.ps.ca, w.ps.state = correlationIDArg{}, OP_START
				continue
			case byte(response.ERR_CODE_YES):
				w.ps.argBuf = pool.Get(fujin.Uint32Len)
				w.ps.state = OP_ERROR_PAYLOAD_ARG
			default:
				w.r.Close()
				return ErrParseProto
			}
		case OP_ERROR_PAYLOAD_ARG:
			w.ps.argBuf = append(w.ps.argBuf, b)
			if len(w.ps.argBuf) >= fujin.Uint32Len {
				if err := w.parseWriteErrLenArg(); err != nil {
					pool.Put(w.ps.argBuf)
					pool.Put(w.ps.ca.cID)
					w.r.Close()
					return fmt.Errorf("parse write err len arg: %w", err)
				}
				pool.Put(w.ps.argBuf)
				w.ps.argBuf, w.ps.state = nil, OP_ERROR_PAYLOAD
			}
		case OP_ERROR_PAYLOAD:
			if w.ps.payloadBuf != nil {
				toCopy := int(w.ps.wma.errLen) - len(w.ps.payloadBuf)
				avail := len(buf) - i

				if avail < toCopy {
					toCopy = avail
				}

				if toCopy > 0 {
					start := len(w.ps.payloadBuf)
					w.ps.payloadBuf = w.ps.payloadBuf[:start+toCopy]
					copy(w.ps.payloadBuf[start:], buf[i:i+toCopy])
					i = (i + toCopy) - 1
				} else {
					w.ps.payloadBuf = append(w.ps.payloadBuf, b)
				}

				if len(w.ps.payloadBuf) >= int(w.ps.wma.errLen) {
					w.wcm.send(binary.BigEndian.Uint32(w.ps.ca.cID), errors.New(string(w.ps.payloadBuf)))
					w.ps.ca.cID, w.ps.payloadBuf, w.ps.wma, w.ps.state = nil, nil, writeMessageArg{}, OP_START
				}
			} else {
				w.ps.payloadBuf = pool.Get(int(w.ps.wma.errLen))
				w.ps.payloadBuf = append(w.ps.payloadBuf, b)

				if len(w.ps.payloadBuf) >= int(w.ps.wma.errLen) {
					w.wcm.send(binary.BigEndian.Uint32(w.ps.ca.cID), errors.New(string(w.ps.payloadBuf)))
					w.ps.ca.cID, w.ps.payloadBuf, w.ps.wma, w.ps.state = nil, nil, writeMessageArg{}, OP_START
				}
			}
		default:
			w.r.Close()
			return ErrParseProto
		}
	}

	return nil
}

func (w *Writer) parseWriteErrLenArg() error {
	w.ps.wma.errLen = binary.BigEndian.Uint32(w.ps.argBuf[0:fujin.Uint32Len])
	if w.ps.wma.errLen == 0 {
		return ErrParseProto
	}

	return nil
}
