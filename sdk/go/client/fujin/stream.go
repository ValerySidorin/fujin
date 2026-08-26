package fujin

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/internal/nativeproto"
	"github.com/fujin-io/fujin/sdk/go/client/internal/session"
	"github.com/quic-go/quic-go"
)

var ErrStreamClosed = session.ErrClosed

const segmentedProduceThreshold = 32 * 1024

type requestMeta struct {
	autoSettle bool
	headered   bool
}

type subscriptionMeta struct {
	autoSettle bool
	headered   bool
}

type Stream struct {
	conn   *Conn
	stream *quic.Stream
	reader *nativeproto.Reader
	core   *session.Core

	writeMu  sync.Mutex
	metaMu   sync.RWMutex
	requests map[uint32]requestMeta
	subs     map[uint32]subscriptionMeta

	closed atomic.Bool
	done   chan struct{}
}

func newStream(conn *Conn, stream *quic.Stream, reader *nativeproto.Reader, routes map[string]client.RouteCapabilities) *Stream {
	s := &Stream{conn: conn, stream: stream, reader: reader, core: session.New(), requests: make(map[uint32]requestMeta), subs: make(map[uint32]subscriptionMeta), done: make(chan struct{})}
	s.core.Bind(routes)
	go s.readLoop()
	return s
}

func (s *Stream) Routes() map[string]client.RouteCapabilities { return s.core.Routes() }

func (s *Stream) Produce(ctx context.Context, route string, payload []byte) error {
	if err := s.core.EnsureNoTransaction(); err != nil {
		return err
	}
	return s.produce(ctx, nativeproto.OpProduce, route, payload, nil)
}

func (s *Stream) HProduce(ctx context.Context, route string, payload []byte, headers []client.Header) error {
	if err := s.core.EnsureNoTransaction(); err != nil {
		return err
	}
	return s.produce(ctx, nativeproto.OpHProduce, route, payload, headers)
}

func (s *Stream) TxProduce(ctx context.Context, payload []byte) error {
	if err := s.core.RequireTransaction(); err != nil {
		return err
	}
	return s.produce(ctx, nativeproto.OpTxProduce, "", payload, nil)
}

func (s *Stream) TxHProduce(ctx context.Context, payload []byte, headers []client.Header) error {
	if err := s.core.RequireTransaction(); err != nil {
		return err
	}
	return s.produce(ctx, nativeproto.OpTxHProduce, "", payload, headers)
}

func (s *Stream) produce(ctx context.Context, op byte, route string, payload []byte, headers []client.Header) error {
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	_, err := s.core.Call(ctx, func(id uint32) error {
		if len(payload) >= segmentedProduceThreshold {
			prefix := nativeproto.ProducePrefix(op, id, route, len(payload), headers)
			return s.writeParts(prefix, payload)
		}
		return s.write(nativeproto.Produce(op, id, route, payload, headers))
	})
	return err
}

func (s *Stream) BeginTx(ctx context.Context, route string) error {
	if err := s.core.BeginTransaction(); err != nil {
		return err
	}
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	_, err := s.core.Call(ctx, func(id uint32) error { return s.write(nativeproto.BeginTx(id, route)) })
	if err != nil {
		s.core.EndTransaction()
	}
	return err
}

func (s *Stream) CommitTx(ctx context.Context) error   { return s.endTx(ctx, nativeproto.OpCommitTx) }
func (s *Stream) RollbackTx(ctx context.Context) error { return s.endTx(ctx, nativeproto.OpRollbackTx) }

func (s *Stream) endTx(ctx context.Context, op byte) error {
	if err := s.core.RequireTransaction(); err != nil {
		return err
	}
	defer s.core.EndTransaction()
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	_, err := s.core.Call(ctx, func(id uint32) error { return s.write(nativeproto.Correlated(op, id)) })
	return err
}

func (s *Stream) Fetch(ctx context.Context, route string, autoSettle bool, maximum uint32) (client.FetchResult, error) {
	return s.fetch(ctx, nativeproto.OpFetch, route, autoSettle, maximum, false)
}

func (s *Stream) HFetch(ctx context.Context, route string, autoSettle bool, maximum uint32) (client.FetchResult, error) {
	return s.fetch(ctx, nativeproto.OpHFetch, route, autoSettle, maximum, true)
}

func (s *Stream) fetch(ctx context.Context, op byte, route string, autoSettle bool, maximum uint32, headered bool) (client.FetchResult, error) {
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	var requestID uint32
	response, err := s.core.Call(ctx, func(id uint32) error {
		requestID = id
		s.setRequest(id, requestMeta{autoSettle: autoSettle, headered: headered})
		return s.write(nativeproto.Fetch(op, id, route, autoSettle, maximum))
	})
	if err != nil {
		s.takeRequest(requestID)
		return client.FetchResult{}, err
	}
	result, ok := response.Value.(client.FetchResult)
	if !ok {
		return client.FetchResult{}, ErrParseProto
	}
	return result, nil
}

func (s *Stream) Subscribe(ctx context.Context, route string, autoSettle bool, handler func(client.Message)) (client.Subscription, error) {
	return s.subscribe(ctx, nativeproto.OpSubscribe, route, autoSettle, false, handler)
}

func (s *Stream) HSubscribe(ctx context.Context, route string, autoSettle bool, handler func(client.Message)) (client.Subscription, error) {
	return s.subscribe(ctx, nativeproto.OpHSubscribe, route, autoSettle, true, handler)
}

func (s *Stream) subscribe(ctx context.Context, op byte, route string, autoSettle, headered bool, handler func(client.Message)) (client.Subscription, error) {
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	if handler == nil {
		handler = func(client.Message) {}
	}
	var requestID uint32
	response, err := s.core.Call(ctx, func(id uint32) error {
		requestID = id
		s.setRequest(id, requestMeta{autoSettle: autoSettle, headered: headered})
		if err := s.core.PrepareSubscription(id, handler); err != nil {
			s.takeRequest(id)
			return err
		}
		return s.write(nativeproto.Subscribe(op, id, route, autoSettle))
	})
	if err != nil {
		s.takeRequest(requestID)
		return nil, err
	}
	id, ok := response.Value.(uint32)
	if !ok {
		return nil, ErrParseProto
	}
	return &Subscription{id: id, stream: s}, nil
}

func (s *Stream) Unsubscribe(ctx context.Context, id uint32) error {
	if id > 255 {
		return fmt.Errorf("subscription ID %d exceeds native range", id)
	}
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	_, err := s.core.Call(ctx, func(correlationID uint32) error { return s.write(nativeproto.Unsubscribe(correlationID, byte(id))) })
	if err == nil {
		s.removeSubscription(id)
	}
	return err
}

func (s *Stream) Ack(ctx context.Context, id uint32, messageIDs ...[]byte) ([]client.SettlementResult, error) {
	return s.settle(ctx, nativeproto.OpAck, id, messageIDs)
}
func (s *Stream) Nack(ctx context.Context, id uint32, messageIDs ...[]byte) ([]client.SettlementResult, error) {
	return s.settle(ctx, nativeproto.OpNack, id, messageIDs)
}
func (s *Stream) settle(ctx context.Context, op byte, id uint32, messageIDs [][]byte) ([]client.SettlementResult, error) {
	if id > 255 {
		return nil, fmt.Errorf("subscription ID %d exceeds native range", id)
	}
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	response, err := s.core.Call(ctx, func(correlationID uint32) error {
		return s.write(nativeproto.Settle(op, correlationID, byte(id), messageIDs))
	})
	if err != nil {
		return nil, err
	}
	results, ok := response.Value.([]client.SettlementResult)
	if !ok {
		return nil, ErrParseProto
	}
	return results, nil
}

func (s *Stream) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	ctx, cancel := s.conn.operationContext(ctx)
	defer cancel()
	if err := s.writeFrame([]byte{nativeproto.OpDisconnect}, true); err != nil {
		s.shutdown(err)
		return err
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.shutdown(ctx.Err())
		return ctx.Err()
	}
}

func (s *Stream) write(frame []byte) error {
	return s.writeFrames(frame, nil, false)
}

func (s *Stream) writeParts(prefix, payload []byte) error {
	return s.writeFrames(prefix, payload, false)
}

func (s *Stream) writeFrame(frame []byte, closing bool) error {
	return s.writeFrames(frame, nil, closing)
}

func (s *Stream) writeFrames(first, second []byte, closing bool) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() && !closing {
		return ErrStreamClosed
	}
	if s.conn.wdl > 0 {
		_ = s.stream.SetWriteDeadline(timeNow().Add(s.conn.wdl))
		defer s.stream.SetWriteDeadline(timeZero())
	}
	if _, err := s.stream.Write(first); err != nil {
		return err
	}
	if len(second) == 0 {
		return nil
	}
	_, err := s.stream.Write(second)
	return err
}

var timeNow = func() time.Time { return time.Now() }
var timeZero = func() time.Time { return time.Time{} }

func (s *Stream) readLoop() {
	defer close(s.done)
	for {
		op, err := s.reader.Byte()
		if err != nil {
			s.shutdown(err)
			return
		}
		if err := s.readResponse(op); err != nil {
			s.shutdown(err)
			return
		}
		if op == nativeproto.RespDisconnect {
			return
		}
	}
}

func (s *Stream) readResponse(op byte) error {
	switch op {
	case nativeproto.RespProduce, nativeproto.RespHProduce, nativeproto.RespBeginTx, nativeproto.RespCommitTx, nativeproto.RespRollbackTx, nativeproto.RespTxProduce, nativeproto.RespTxHProduce, nativeproto.RespUnsubscribe:
		id, err := s.reader.Uint32()
		if err != nil {
			return err
		}
		opErr, err := s.reader.Status()
		if err != nil {
			return err
		}
		s.core.Complete(id, session.Response{Error: opErr})
		return nil
	case nativeproto.RespSubscribe, nativeproto.RespHSubscribe:
		id, err := s.reader.Uint32()
		if err != nil {
			return err
		}
		opErr, err := s.reader.Status()
		if err != nil {
			return err
		}
		if opErr != nil {
			s.takeRequest(id)
			s.core.Complete(id, session.Response{Error: opErr})
			return nil
		}
		subID, err := s.reader.Byte()
		if err != nil {
			return err
		}
		meta := s.takeRequest(id)
		s.metaMu.Lock()
		s.subs[uint32(subID)] = subscriptionMeta{autoSettle: meta.autoSettle, headered: meta.headered}
		s.metaMu.Unlock()
		if err := s.core.ActivateSubscription(id, uint32(subID)); err != nil {
			return err
		}
		s.core.Complete(id, session.Response{Value: uint32(subID)})
		return nil
	case nativeproto.RespFetch, nativeproto.RespHFetch:
		return s.readFetch(op == nativeproto.RespHFetch)
	case nativeproto.RespAck, nativeproto.RespNack:
		return s.readSettlement()
	case nativeproto.RespMessage, nativeproto.RespHMessage:
		return s.readMessage(op == nativeproto.RespHMessage)
	case nativeproto.RespDisconnect:
		s.shutdown(nil)
		return nil
	case nativeproto.OpStop:
		s.shutdown(ErrStreamClosed)
		return nil
	default:
		return fmt.Errorf("%w: unexpected response opcode %d", ErrParseProto, op)
	}
}

func (s *Stream) readFetch(headered bool) error {
	id, err := s.reader.Uint32()
	if err != nil {
		return err
	}
	meta := s.takeRequest(id)
	opErr, err := s.reader.Status()
	if err != nil {
		return err
	}
	if opErr != nil {
		s.core.Complete(id, session.Response{Error: opErr})
		return nil
	}
	subID, err := s.reader.Byte()
	if err != nil {
		return err
	}
	count, err := s.reader.Uint32()
	if err != nil {
		return err
	}
	messages := make([]client.Message, 0, count)
	for range count {
		message := client.Message{SubscriptionID: uint32(subID)}
		if headered {
			message.Headers, err = s.reader.Headers()
			if err != nil {
				return err
			}
		}
		if !meta.autoSettle {
			message.MessageID, err = s.reader.Bytes()
			if err != nil {
				return err
			}
		}
		message.Payload, err = s.reader.Bytes()
		if err != nil {
			return err
		}
		messages = append(messages, message)
	}
	s.core.Complete(id, session.Response{Value: client.FetchResult{SubscriptionID: uint32(subID), Messages: messages}})
	return nil
}

func (s *Stream) readSettlement() error {
	id, err := s.reader.Uint32()
	if err != nil {
		return err
	}
	opErr, err := s.reader.Status()
	if err != nil {
		return err
	}
	if opErr != nil {
		s.core.Complete(id, session.Response{Error: opErr})
		return nil
	}
	count, err := s.reader.Uint32()
	if err != nil {
		return err
	}
	results := make([]client.SettlementResult, 0, count)
	for range count {
		messageID, err := s.reader.Bytes()
		if err != nil {
			return err
		}
		resultErr, err := s.reader.Status()
		if err != nil {
			return err
		}
		results = append(results, client.SettlementResult{MessageID: messageID, Error: resultErr})
	}
	s.core.Complete(id, session.Response{Value: results})
	return nil
}

func (s *Stream) readMessage(headered bool) error {
	subID, err := s.reader.Byte()
	if err != nil {
		return err
	}
	meta, ok := s.subscription(uint32(subID))
	if !ok {
		return fmt.Errorf("unknown subscription %d", subID)
	}
	message := client.Message{SubscriptionID: uint32(subID)}
	if headered {
		message.Headers, err = s.reader.Headers()
		if err != nil {
			return err
		}
	}
	if !meta.autoSettle {
		message.MessageID, err = s.reader.Bytes()
		if err != nil {
			return err
		}
	}
	message.Payload, err = s.reader.Bytes()
	if err != nil {
		return err
	}
	s.core.Deliver(message)
	return nil
}

func (s *Stream) shutdown(err error) {
	s.core.Close(err)
	_ = s.stream.Close()
}
func (s *Stream) setRequest(id uint32, meta requestMeta) {
	s.metaMu.Lock()
	s.requests[id] = meta
	s.metaMu.Unlock()
}
func (s *Stream) takeRequest(id uint32) requestMeta {
	s.metaMu.Lock()
	meta := s.requests[id]
	delete(s.requests, id)
	s.metaMu.Unlock()
	return meta
}
func (s *Stream) subscription(id uint32) (subscriptionMeta, bool) {
	s.metaMu.RLock()
	meta, ok := s.subs[id]
	s.metaMu.RUnlock()
	return meta, ok
}
func (s *Stream) removeSubscription(id uint32) {
	s.core.RemoveSubscription(id)
	s.metaMu.Lock()
	delete(s.subs, id)
	s.metaMu.Unlock()
}
