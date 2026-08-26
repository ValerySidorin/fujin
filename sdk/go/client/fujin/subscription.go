package fujin

import (
	"context"
	"sync/atomic"

	client "github.com/fujin-io/fujin/sdk/go/client"
)

type Subscription struct {
	id     uint32
	stream *Stream
	closed atomic.Bool
}

func (s *Subscription) ID() uint32 { return s.id }

func (s *Subscription) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	return s.stream.Unsubscribe(ctx, s.id)
}

var _ client.Subscription = (*Subscription)(nil)
