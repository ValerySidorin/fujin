package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ValerySidorin/fujin/internal/fujin/pool"
	"github.com/ValerySidorin/fujin/internal/fujin/proto/request"
	"github.com/panjf2000/ants/v2"
)

type SubscriptionConfig struct {
	Async      bool
	MsgBufSize int
	Pool       PoolConfig
}

type Subscription struct {
	conf       SubscriptionConfig
	autoCommit bool

	pool *ants.Pool

	stream *Stream

	id   byte
	h    func(Msg)
	msgs chan Msg

	closed atomic.Bool
}

func (s *Stream) Subscribe(
	topic string, autoCommit bool,
	handler func(msg Msg),
	opts ...SubscriptionOption) (*Subscription, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	if s == nil {
		return nil, ErrStreamClosed
	}

	if s.closed.Load() {
		return nil, ErrStreamClosed
	}

	h := func(msg Msg) {
		handler(msg)
		if msg.id != nil {
			pool.Put(msg.id)
		}
		pool.Put(msg.Value)
	}

	sub := &Subscription{
		conf: SubscriptionConfig{
			MsgBufSize: 1024,
		},
		autoCommit: autoCommit,
		stream:     s,
		h:          h,
	}
	for _, opt := range opts {
		opt(sub)
	}

	sub.msgs = make(chan Msg, sub.conf.MsgBufSize)

	if sub.conf.Async {
		pool, err := ants.NewPool(sub.conf.Pool.Size, ants.WithPreAlloc(sub.conf.Pool.PreAlloc))
		if err != nil {
			return nil, fmt.Errorf("new pool: %w", err)
		}

		sub.pool = pool
		sub.h = func(msg Msg) {
			_ = pool.Submit(func() {
				h(msg)
			})
		}
	}

	ch := make(chan SubscribeResponse, 1)
	id := s.sc.next(ch)

	buf := pool.Get(10 + len(topic))

	buf = append(buf, byte(request.OP_CODE_SUBSCRIBE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, boolToByte(autoCommit))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(topic)))
	buf = append(buf, topic...)

	s.out.EnqueueProto(buf)
	pool.Put(buf)

	ctx, cancel := context.WithTimeout(context.Background(), s.conn.timeout)

	select {
	case <-ctx.Done():
		cancel()
		s.sc.delete(id)
		close(ch)
		pool.Put(buf)
		return nil, ErrTimeout
	case resp := <-ch:
		cancel()
		s.cm.delete(id)
		close(ch)
		pool.Put(buf)
		if resp.Err != nil {
			return nil, resp.Err
		}
		sub.id = resp.SubID
		s.subs.add(sub)
		s.wg.Add(1)
		go sub.handle()
		return sub, nil
	}
}

func (s *Subscription) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)

	ch := make(chan error)
	id := s.stream.cm.next(ch)

	buf := pool.Get(6)
	buf = append(buf, byte(request.OP_CODE_UNSUBSCRIBE))
	buf = binary.BigEndian.AppendUint32(buf, id)
	buf = append(buf, s.id)

	s.stream.out.EnqueueProto(buf)
	pool.Put(buf)
	var err error
	select {
	case <-time.After(s.stream.conn.timeout):
		err = ErrTimeout
	case <-ch:
	}

	s.stream.subs.delete(s.id)
	close(s.msgs)
	return err
}

func (s *Subscription) handle() {
	defer s.stream.wg.Done()
	for msg := range s.msgs {
		s.h(msg)
	}
}
