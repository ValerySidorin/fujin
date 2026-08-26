package v1

import (
	"context"
	"sync/atomic"
)

type subscription struct {
	id     uint32
	stream *stream
	closed atomic.Bool
}

func (s *subscription) ID() uint32 { return s.id }

func (s *subscription) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	return s.stream.Unsubscribe(ctx, s.id)
}
