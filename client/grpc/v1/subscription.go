package v1

import (
	"sync/atomic"

	"github.com/ValerySidorin/fujin/client/models"
)

// subscription implements the Subscription interface
type subscription struct {
	id      uint32
	topic   string
	handler func(msg models.Msg)
	stream  *stream
	closed  atomic.Bool
}

// Close closes the subscription
func (s *subscription) Close() error {
	if s.closed.Load() {
		return nil
	}

	s.closed.Store(true)

	// Remove from stream's subscriptions
	s.stream.subsMu.Lock()
	delete(s.stream.subscriptions, s.id)
	s.stream.subsMu.Unlock()

	return nil
}
