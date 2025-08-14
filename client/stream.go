package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	ErrStreamClosed = errors.New("stream closed")

	PONG_RESP = []byte{byte(response.RESP_CODE_PONG)}
)

type Stream struct {
	conn *Conn

	ps         *parseState
	quicStream *quic.Stream
	out        *fujin.Outbound

	subs *subscriptions

	cm  *correlator[error]
	ac  *correlator[AckResponse]
	fcm *fetchCorrelator
	sc  *correlator[SubscribeResponse]

	closed       atomic.Bool
	disconnectCh chan struct{}
	wg           sync.WaitGroup

	l *slog.Logger
}

func (c *Conn) Connect(id string) (*Stream, error) {
	if c == nil {
		return nil, ErrConnClosed
	}

	if c.closed.Load() {
		return nil, ErrConnClosed
	}

	stream, err := c.qconn.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	buf := pool.Get(5)
	defer pool.Put(buf)

	buf = append(buf, byte(request.OP_CODE_CONNECT))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(id)))
	buf = append(buf, id...)

	if _, err := stream.Write(buf); err != nil {
		stream.Close()
		return nil, fmt.Errorf("write connect: %w", err)
	}

	l := c.l.With("stream_id", id, "quic_stream_id", stream.StreamID())
	out := fujin.NewOutbound(stream, c.wdl, l)

	s := &Stream{
		conn:         c,
		out:          out,
		quicStream:   stream,
		ps:           &parseState{},
		subs:         newSubscriptions(),
		cm:           newCorrelator[error](),
		ac:           newCorrelator[AckResponse](),
		sc:           newCorrelator[SubscribeResponse](),
		fcm:          newFetchCorrelator(),
		disconnectCh: make(chan struct{}),
		l:            l,
	}

	s.wg.Add(2)
	go s.readLoop()
	go func() {
		defer s.wg.Done()
		out.WriteLoop()
	}()

	return s, nil
}

func (s *Stream) Produce(topic string, p []byte) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}

	buf := pool.Get(len(topic) + len(p) + 13)
	ch := make(chan error, 1)
	id := s.cm.next(ch)

	buf = append(buf, byte(request.OP_CODE_PRODUCE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(p)))
	buf = append(buf, p...)

	s.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)

	select {
	case <-ctx.Done():
		cancel()
		s.cm.delete(id)
		close(ch)
		pool.Put(buf)
		return ErrTimeout
	case err := <-ch:
		cancel()
		s.cm.delete(id)
		close(ch)
		pool.Put(buf)
		return err
	}
}

func (s *Stream) HProduce(topic string, p []byte, hs map[string]string) error {
	if len(hs) <= 0 {
		return s.Produce(topic, p)
	}

	if s.closed.Load() {
		return ErrStreamClosed
	}

	// Calculate buffer size: opcode(1) + cID(4) + topicLen(4) + topic + headerCount(2) + headers + payloadLen(4) + payload
	headersCount := len(hs) * 2
	headersSize := 0
	for k, v := range hs {
		headersSize += 4 + len(k) // key length + key
		headersSize += 4 + len(v) // value length + value
	}
	buf := pool.Get(1 + 4 + 4 + len(topic) + 2 + headersSize + 4 + len(p))
	ch := make(chan error, 1)
	id := s.cm.next(ch)

	buf = append(buf, byte(request.OP_CODE_HPRODUCE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(headersCount))
	for k, v := range hs {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(p)))
	buf = append(buf, p...)

	s.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		s.cm.delete(id)
		close(ch)
		pool.Put(buf)
		return ErrTimeout
	case err := <-ch:
		s.cm.delete(id)
		close(ch)
		pool.Put(buf)
		return err
	}
}

func (s *Stream) BeginTx() error {
	return s.sendTxCmd(byte(request.OP_CODE_TX_BEGIN))
}

func (s *Stream) CommitTx() error {
	return s.sendTxCmd(byte(request.OP_CODE_TX_COMMIT))
}

func (s *Stream) RollbackTx() error {
	return s.sendTxCmd(byte(request.OP_CODE_TX_ROLLBACK))
}

func (s *Stream) Fetch(
	ctx context.Context, topic string, n uint32, autoCommit bool,
) ([]Msg, error) {
	if s.closed.Load() {
		return nil, ErrStreamClosed
	}

	buf := pool.Get(9)
	defer pool.Put(buf)

	eCh := make(chan error, 1)

	id := s.fcm.next(eCh, autoCommit)
	defer s.fcm.delete(id)

	buf = append(buf, byte(request.OP_CODE_FETCH))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, boolToByte(autoCommit))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, []byte(topic)...)
	buf = binary.BigEndian.AppendUint32(buf, n)

	s.out.EnqueueProto(buf)

	ctx, cancel := context.WithTimeout(ctx, s.conn.timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil, ErrTimeout
	case err := <-eCh:
		if err != nil {
			return nil, err
		}

		return s.fcm.getMsgs(id), nil
	}
}

func (s *Stream) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)

	s.subs.close()

	s.out.EnqueueProto(DISCONNECT_REQ)
	select {
	case <-time.After(s.conn.timeout):
	case <-s.disconnectCh:
	}

	s.out.Close()
	s.quicStream.Close()

	s.wg.Wait()
	return nil
}

func (s *Stream) ack(id []byte) (AckResponse, error) {
	return s.sendAckCmd(byte(request.OP_CODE_ACK), id)
}

func (s *Stream) nack(id []byte) (AckResponse, error) {
	return s.sendAckCmd(byte(request.OP_CODE_NACK), id)
}

func (s *Stream) sendAckCmd(cmd byte, msgID []byte) (AckResponse, error) {
	if s.closed.Load() {
		return AckResponse{}, ErrStreamClosed
	}

	buf := pool.Get(9)
	defer pool.Put(buf)

	ch := make(chan AckResponse, 1)
	defer close(ch)

	correlationID := s.ac.next(ch)
	defer s.ac.delete(correlationID)

	buf = append(buf, cmd)
	buf = binary.BigEndian.AppendUint32(buf, correlationID)
	buf = binary.BigEndian.AppendUint32(buf, 1)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msgID)))

	s.out.EnqueueProtoMulti(buf, msgID)

	select {
	case <-time.After(s.conn.timeout):
		return AckResponse{}, ErrTimeout
	case resp := <-ch:
		return resp, nil
	}
}

func (s *Stream) sendTxCmd(cmd byte) error {
	if s.closed.Load() {
		return ErrStreamClosed
	}

	buf := pool.Get(5)
	defer pool.Put(buf)

	ch := make(chan error, 1)
	defer close(ch)

	id := s.cm.next(ch)
	defer s.cm.delete(id)

	buf = append(buf, cmd)
	buf = binary.BigEndian.AppendUint32(buf, id)

	s.out.EnqueueProto(buf)

	select {
	case <-time.After(s.conn.timeout):
		return ErrTimeout
	case err := <-ch:
		return err
	}
}

func (s *Stream) readLoop() {
	defer s.wg.Done()

	buf := pool.Get(ReadBufferSize)[:ReadBufferSize]
	defer pool.Put(buf)

	for {
		n, err := s.quicStream.Read(buf)
		if err != nil {
			if err == io.EOF {
				if n != 0 {
					err = s.parse(buf[:n])
					if err != nil {
						s.l.Error("writer read loop", "err", err)
						return
					}
				}
				return
			}
		}

		if n == 0 {
			continue
		}

		err = s.parse(buf[:n])
		if err != nil {
			s.l.Error("writer read loop", "err", err)
			return
		}
	}
}

func (s *Stream) parse(buf []byte) error {
	var (
		i int
		b byte
	)

	for i = 0; i < len(buf); i++ {
		b = buf[i]

		switch s.ps.state {
		case OP_START:
			switch b {
			case byte(response.RESP_CODE_PRODUCE):
				s.ps.state = OP_PRODUCE
			case byte(response.RESP_CODE_MSG):
				s.ps.state = OP_MSG
			case byte(response.RESP_CODE_FETCH):
				s.ps.state = OP_FETCH
			case byte(response.RESP_CODE_HPRODUCE):
				s.ps.state = OP_PRODUCE_H
			case byte(response.RESP_CODE_HMSG):
				s.ps.state = OP_MSG_H
			case byte(response.RESP_CODE_HFETCH):
				s.ps.state = OP_FETCH
			case byte(response.RESP_CODE_ACK):
				s.ps.state = OP_ACK
			case byte(response.RESP_CODE_NACK):
				s.ps.state = OP_NACK
			case byte(response.RESP_CODE_TX_BEGIN):
				s.ps.state = OP_TX_BEGIN
			case byte(response.RESP_CODE_TX_COMMIT):
				s.ps.state = OP_TX_COMMIT
			case byte(response.RESP_CODE_TX_ROLLBACK):
				s.ps.state = OP_TX_ROLLBACK
			case byte(request.OP_CODE_PING):
				s.writePong()
			case byte(response.RESP_CODE_SUBSCRIBE):
				s.ps.state = OP_SUBSCRIBE
			case byte(response.RESP_CODE_HSUBSCRIBE):
				s.ps.state = OP_SUBSCRIBE_H
			case byte(response.RESP_CODE_UNSUBSCRIBE):
				s.ps.state = OP_UNSUBSCRIBE
			case byte(response.RESP_CODE_DISCONNECT):
				close(s.disconnectCh)
				return nil
			case byte(request.OP_CODE_STOP):
				go s.Close() // we probably can do something smarter here
				return nil
			}
		case OP_PRODUCE, OP_PRODUCE_H:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
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
				pool.Put(s.ps.ca.cID)
			}
		case OP_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.cm.send(s.ps.ca.cIDUint32, nil)
				s.ps.ca, s.ps.state = correlationIDArg{}, OP_START
				continue
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
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
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, nil, correlationIDArg{}, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.ca.cID, s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, nil, correlationIDArg{}, errArg{}, OP_START
				}
			}
		case OP_MSG:
			sub, ok := s.subs.get(b)
			if !ok {
				return errors.New("subscription not found")
			}
			s.ps.ma.sub = sub
			if sub.autoCommit {
				s.ps.state = OP_MSG_ARG
				continue
			}
			s.ps.argBuf = pool.Get(fujin.Uint32Len)
			s.ps.state = OP_MSG_ID_ARG
		case OP_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.ma.idLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, pool.Get(int(s.ps.ma.idLen))
				if s.ps.ma.idLen <= 0 {
					s.ps.state = OP_MSG_ARG
					continue
				}
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
					s.quicStream.Close()
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
					s.ps.ma.sub.msgs <- Msg{
						Value:   s.ps.payloadBuf,
						Headers: s.ps.ma.headers,
						id:      s.ps.metaBuf,
						s:       s,
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ma.len))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.ma.sub.msgs <- Msg{
						Value:   s.ps.payloadBuf,
						Headers: s.ps.ma.headers,
						id:      s.ps.metaBuf,
						s:       s,
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.state = nil, nil, msgArg{}, OP_START
				}
			}
		case OP_MSG_H:
			sub, ok := s.subs.get(b)
			if !ok {
				return errors.New("subscription not found")
			}
			s.ps.ma.sub = sub
			s.ps.argBuf = pool.Get(fujin.Uint16Len)
			s.ps.state = OP_MSG_H_HEADERS_COUNT_ARG
		case OP_MSG_H_HEADERS_COUNT_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint16Len {
				s.ps.ma.headerCount = binary.BigEndian.Uint16(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				s.ps.ma.headers = make(map[string]string, s.ps.ma.headerCount/2)
				s.ps.ma.headerStep = 0
				s.ps.state = OP_MSG_H_HEADER_LEN
			}
		case OP_MSG_H_HEADER_LEN:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.ma.headerLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = nil
				s.ps.ma.headerBuf = pool.Get(int(s.ps.ma.headerLen))
				s.ps.state = OP_MSG_H_HEADER_PAYLOAD
			}
		case OP_MSG_H_HEADER_PAYLOAD:
			s.ps.ma.headerBuf = append(s.ps.ma.headerBuf, b)
			if len(s.ps.ma.headerBuf) >= int(s.ps.ma.headerLen) {
				if s.ps.ma.headerStep%2 == 0 {
					s.ps.ma.headerKey = string(s.ps.ma.headerBuf)
				} else {
					s.ps.ma.headers[s.ps.ma.headerKey] = string(s.ps.ma.headerBuf)
				}
				pool.Put(s.ps.ma.headerBuf)
				s.ps.ma.headerBuf = nil
				s.ps.ma.headerLen = 0
				s.ps.ma.headerStep++
				if s.ps.ma.headerStep < int(s.ps.ma.headerCount) {
					s.ps.state = OP_MSG_H_HEADER_LEN
				} else {
					fmt.Println("headers:", s.ps.ma.headers)
					if s.ps.ma.sub.autoCommit {
						s.ps.state = OP_MSG_ARG
						continue
					}
					s.ps.argBuf = pool.Get(fujin.Uint32Len)
					s.ps.state = OP_MSG_ID_ARG
				}
			}
		case OP_TX_BEGIN:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_TX_COMMIT:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_TX_ROLLBACK:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_CORRELATION_ID_ARG
		case OP_FETCH:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_FETCH_CORRELATION_ID_ARG
		case OP_FETCH_CORRELATION_ID_ARG:
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
				s.ps.fa.msgs, s.ps.fa.autoCommit, s.ps.fa.err = s.fcm.get(s.ps.ca.cIDUint32)
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_FETCH_ERROR_CODE_ARG
			}
		case OP_FETCH_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				pool.Put(s.ps.ca.cID)
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_FETCH_N_ARG
				continue
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_FETCH_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_FETCH_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					pool.Put(s.ps.ca.cID)
					s.quicStream.Close()
					err = fmt.Errorf("parse write err len arg: %w", err)
					s.ps.fa.err <- err
					close(s.ps.fa.err)
					return err
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_FETCH_ERROR_PAYLOAD
			}
		case OP_FETCH_ERROR_PAYLOAD:
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
					s.ps.fa.err <- errors.New(string(s.ps.payloadBuf))
					close(s.ps.fa.err)
					s.ps.ca, s.ps.fa, s.ps.payloadBuf, s.ps.ea, s.ps.state = correlationIDArg{}, fetchArg{}, nil, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = make([]byte, 0, s.ps.ea.errLen)
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.ps.fa.err <- errors.New(string(s.ps.payloadBuf))
					close(s.ps.fa.err)
					s.ps.ca, s.ps.fa, s.ps.payloadBuf, s.ps.ea, s.ps.state = correlationIDArg{}, fetchArg{}, nil, errArg{}, OP_START
				}
			}
		case OP_FETCH_N_ARG:
			toCopy := fujin.Uint32Len - len(s.ps.argBuf)
			avail := len(buf) - i

			if avail < toCopy {
				toCopy = avail
			}

			if toCopy > 0 {
				start := len(s.ps.argBuf)
				s.ps.argBuf = s.ps.argBuf[:start+toCopy]
				copy(s.ps.argBuf[start:], buf[i:i+toCopy])
				i = (i + toCopy) - 1
			} else {
				s.ps.argBuf = append(s.ps.argBuf, b)
			}

			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.fa.n = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				if s.ps.fa.autoCommit {
					s.ps.state = OP_FETCH_MSG_ARG
					continue
				}
				s.ps.state = OP_FETCH_MSG_ID_ARG
			}
		case OP_FETCH_MSG_ID_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				s.ps.ma.idLen = binary.BigEndian.Uint32(s.ps.argBuf)
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.metaBuf = nil, make([]byte, 0, s.ps.ma.idLen)
				if s.ps.ma.idLen <= 0 {
					s.ps.state = OP_FETCH_MSG_ARG
					continue
				}
				s.ps.state = OP_FETCH_MSG_ID_PAYLOAD
			}
		case OP_FETCH_MSG_ID_PAYLOAD:
			s.ps.metaBuf = append(s.ps.metaBuf, b)
			if len(s.ps.metaBuf) >= int(s.ps.ma.idLen) {
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_FETCH_MSG_ARG
			}
		case OP_FETCH_MSG_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseMsgLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					_ = s.quicStream.Close()
					return fmt.Errorf("parse msg len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_FETCH_MSG_PAYLOAD
			}
		case OP_FETCH_MSG_PAYLOAD:
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
					s.ps.fa.msgs = append(s.ps.fa.msgs, Msg{
						Value: s.ps.payloadBuf,
						id:    s.ps.metaBuf,
						s:     s,
					})
					s.ps.fa.handled++
					if s.ps.fa.handled >= s.ps.fa.n {
						s.fcm.setMsgs(s.ps.ca.cIDUint32, s.ps.fa.msgs)
						close(s.ps.fa.err)
						s.ps.metaBuf, s.ps.payloadBuf, s.ps.ca, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, correlationIDArg{}, msgArg{}, fetchArg{}, OP_START
						continue
					}

					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma = nil, nil, msgArg{}
					if true { // TODO: fix
						s.ps.state = OP_FETCH_MSG_ARG
						continue
					}
					s.ps.state = OP_FETCH_MSG_ID_ARG
				}
			} else {
				s.ps.payloadBuf = make([]byte, 0, s.ps.ma.len)
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ma.len) {
					s.ps.fa.msgs = append(s.ps.fa.msgs, Msg{
						Value: s.ps.payloadBuf,
						id:    s.ps.metaBuf,
						s:     s,
					})
					s.ps.fa.handled++
					if s.ps.fa.handled >= s.ps.fa.n {
						s.fcm.setMsgs(s.ps.ca.cIDUint32, s.ps.fa.msgs)
						close(s.ps.fa.err)
						s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
					}
					s.ps.metaBuf, s.ps.payloadBuf, s.ps.ma, s.ps.fa, s.ps.state = nil, nil, msgArg{}, fetchArg{}, OP_START
				}
			}
		case OP_SUBSCRIBE, OP_SUBSCRIBE_H:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_SUBSCRIBE_CORRELATION_ID_ARG
		case OP_SUBSCRIBE_CORRELATION_ID_ARG:
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
				pool.Put(s.ps.ca.cID)
				s.ps.state = OP_SUBSCRIBE_ERROR_CODE_ARG
			}
		case OP_SUBSCRIBE_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.ps.state = OP_SUBSCRIBE_SUB_ID_ARG
				continue
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_SUBSCRIBE_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_SUBSCRIBE_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse subscribe err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_SUBSCRIBE_ERROR_PAYLOAD
			}
		case OP_SUBSCRIBE_ERROR_PAYLOAD:
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
					s.sc.send(s.ps.ca.cIDUint32, SubscribeResponse{Err: errors.New(string(s.ps.payloadBuf))})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.sc.send(s.ps.ca.cIDUint32, SubscribeResponse{Err: errors.New(string(s.ps.payloadBuf))})
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			}
		case OP_SUBSCRIBE_SUB_ID_ARG:
			s.sc.send(s.ps.ca.cIDUint32, SubscribeResponse{SubID: b})
			s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
		case OP_UNSUBSCRIBE:
			s.ps.ca.cID = pool.Get(fujin.Uint32Len)
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			s.ps.state = OP_UNSUBSCRIBE_CORRELATION_ID_ARG
		case OP_UNSUBSCRIBE_CORRELATION_ID_ARG:
			s.ps.ca.cID = append(s.ps.ca.cID, b)
			if len(s.ps.ca.cID) >= fujin.Uint32Len {
				s.ps.ca.cIDUint32 = binary.BigEndian.Uint32(s.ps.ca.cID)
				pool.Put(s.ps.ca.cID)
				s.ps.state = OP_UNSUBSCRIBE_ERROR_CODE_ARG
			}
		case OP_UNSUBSCRIBE_ERROR_CODE_ARG:
			switch b {
			case byte(response.ERR_CODE_NO):
				s.cm.send(s.ps.ca.cIDUint32, nil)
				s.ps.ca, s.ps.state = correlationIDArg{}, OP_START
				continue
			case byte(response.ERR_CODE_YES):
				s.ps.argBuf = pool.Get(fujin.Uint32Len)
				s.ps.state = OP_UNSUBSCRIBE_ERROR_PAYLOAD_ARG
			default:
				s.quicStream.Close()
				return ErrParseProto
			}
		case OP_UNSUBSCRIBE_ERROR_PAYLOAD_ARG:
			s.ps.argBuf = append(s.ps.argBuf, b)
			if len(s.ps.argBuf) >= fujin.Uint32Len {
				if err := s.parseErrLenArg(); err != nil {
					pool.Put(s.ps.argBuf)
					s.quicStream.Close()
					return fmt.Errorf("parse unsubscribe err len arg: %w", err)
				}
				pool.Put(s.ps.argBuf)
				s.ps.argBuf, s.ps.state = nil, OP_UNSUBSCRIBE_ERROR_PAYLOAD
			}
		case OP_UNSUBSCRIBE_ERROR_PAYLOAD:
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
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			} else {
				s.ps.payloadBuf = pool.Get(int(s.ps.ea.errLen))
				s.ps.payloadBuf = append(s.ps.payloadBuf, b)

				if len(s.ps.payloadBuf) >= int(s.ps.ea.errLen) {
					s.cm.send(s.ps.ca.cIDUint32, errors.New(string(s.ps.payloadBuf)))
					pool.Put(s.ps.payloadBuf)
					s.ps.payloadBuf, s.ps.ca, s.ps.ea, s.ps.state = nil, correlationIDArg{}, errArg{}, OP_START
				}
			}
		default:
			s.quicStream.Close()
			return ErrParseProto
		}
	}

	return nil
}

func (s *Stream) writePong() {
	s.out.EnqueueProto(PONG_RESP)
}

func (s *Stream) CheckParseStateAfterOpForTests() error {
	if s.ps.state != OP_START {
		return fmt.Errorf("invalid state: %d", s.ps.state)
	}

	if s.ps.argBuf != nil {
		return errors.New("arg buf is not nil")
	}
	if s.ps.metaBuf != nil {
		return errors.New("meta buf is not nil")
	}
	if s.ps.payloadBuf != nil {
		return errors.New("payload buf is not nil")
	}

	ea := errArg{}
	if s.ps.ea != ea {
		return errors.New("err arg is not empty")
	}

	// ma := msgArg{}
	// if s.ps.ma != ma {
	// 	return errors.New("msg arg is not empty")
	// }

	if s.ps.fa.n != 0 || s.ps.fa.err != nil || s.ps.fa.handled != 0 || len(s.ps.fa.msgs) != 0 {
		return errors.New("fetch arg is not empty")
	}

	if s.ps.aa.currMsgIDLen != 0 || s.ps.aa.n != 0 || len(s.ps.aa.resps) != 0 {
		return errors.New("ack arg is not empty")
	}

	if s.ps.ca.cID != nil || s.ps.ca.cIDUint32 != 0 {
		return errors.New("correlation id arg is not empty")
	}

	return nil
}
