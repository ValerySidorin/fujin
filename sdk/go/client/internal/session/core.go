package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	fujin "github.com/fujin-io/fujin/sdk/go/client"
)

var (
	ErrClosed            = errors.New("fujin session closed")
	ErrTransactionActive = errors.New("fujin transaction already active")
	ErrNoTransaction     = errors.New("fujin transaction not active")
)

type Response struct {
	Value any
	Error *fujin.OperationError
}

type Core struct {
	next atomic.Uint32

	mu          sync.RWMutex
	closed      bool
	routes      map[string]fujin.RouteCapabilities
	pending     map[uint32]chan Response
	pendingSubs map[uint32]func(fujin.Message)
	subs        map[uint32]func(fujin.Message)
	transaction bool
}

func New() *Core {
	return &Core{
		routes:      make(map[string]fujin.RouteCapabilities),
		pending:     make(map[uint32]chan Response),
		pendingSubs: make(map[uint32]func(fujin.Message)),
		subs:        make(map[uint32]func(fujin.Message)),
	}
}

func (c *Core) Bind(routes map[string]fujin.RouteCapabilities) {
	c.mu.Lock()
	c.routes = cloneRoutes(routes)
	c.mu.Unlock()
}

func (c *Core) Routes() map[string]fujin.RouteCapabilities {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneRoutes(c.routes)
}

func (c *Core) Call(ctx context.Context, send func(uint32) error) (Response, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return Response{}, ErrClosed
	}
	id := c.next.Add(1)
	ch := make(chan Response, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	if err := send(id); err != nil {
		c.removePending(id)
		return Response{}, err
	}

	select {
	case response := <-ch:
		if response.Error != nil {
			return response, response.Error
		}
		return response, nil
	case <-ctx.Done():
		c.removePending(id)
		return Response{}, ctx.Err()
	}
}

func (c *Core) Complete(id uint32, response Response) bool {
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	delete(c.pendingSubs, id)
	c.mu.Unlock()
	if ch == nil {
		return false
	}
	ch <- response
	return true
}

func (c *Core) PrepareSubscription(correlationID uint32, handler func(fujin.Message)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.pendingSubs[correlationID] = handler
	return nil
}

func (c *Core) ActivateSubscription(correlationID, subscriptionID uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	handler := c.pendingSubs[correlationID]
	delete(c.pendingSubs, correlationID)
	if handler == nil {
		return errors.New("missing pending subscription handler")
	}
	c.subs[subscriptionID] = handler
	return nil
}

func (c *Core) RemoveSubscription(id uint32) {
	c.mu.Lock()
	delete(c.subs, id)
	c.mu.Unlock()
}

func (c *Core) Deliver(message fujin.Message) bool {
	c.mu.RLock()
	handler := c.subs[message.SubscriptionID]
	c.mu.RUnlock()
	if handler == nil {
		return false
	}
	handler(message)
	return true
}

func (c *Core) BeginTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.transaction {
		return ErrTransactionActive
	}
	c.transaction = true
	return nil
}

func (c *Core) EnsureNoTransaction() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrClosed
	}
	if c.transaction {
		return ErrTransactionActive
	}
	return nil
}

func (c *Core) RequireTransaction() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrClosed
	}
	if !c.transaction {
		return ErrNoTransaction
	}
	return nil
}

func (c *Core) EndTransaction() {
	c.mu.Lock()
	c.transaction = false
	c.mu.Unlock()
}

func (c *Core) Close(err error) {
	if err == nil {
		err = ErrClosed
	}
	operationErr, ok := err.(*fujin.OperationError)
	if !ok {
		operationErr = &fujin.OperationError{Code: fujin.StatusUnavailable, Outcome: fujin.OutcomeUnknown, Reason: "SESSION_CLOSED", Message: err.Error()}
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	pending := c.pending
	c.pending = make(map[uint32]chan Response)
	c.pendingSubs = make(map[uint32]func(fujin.Message))
	c.subs = make(map[uint32]func(fujin.Message))
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- Response{Error: operationErr}
	}
}

func (c *Core) removePending(id uint32) {
	c.mu.Lock()
	delete(c.pending, id)
	delete(c.pendingSubs, id)
	c.mu.Unlock()
}

func cloneRoutes(routes map[string]fujin.RouteCapabilities) map[string]fujin.RouteCapabilities {
	clone := make(map[string]fujin.RouteCapabilities, len(routes))
	for route, capabilities := range routes {
		clone[route] = capabilities
	}
	return clone
}
