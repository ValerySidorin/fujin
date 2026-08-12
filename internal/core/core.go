package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	commonpool "github.com/fujin-io/fujin/internal/common/pool"
	"github.com/fujin-io/fujin/internal/connectors"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	bmw "github.com/fujin-io/fujin/public/plugins/middleware/bind"
)

var (
	ErrNotBound                   = errors.New("not binded")
	ErrAlreadyBound               = errors.New("already initialized")
	ErrConnectorNotFound          = errors.New("connector not found")
	ErrTransactionActive          = errors.New("transaction already in progress")
	ErrNoTransaction              = errors.New("no transaction in progress")
	ErrNormalProduceInTransaction = errors.New("normal produce is not allowed in a transaction")
	ErrSubscriptionNotFound       = errors.New("subscription not found")
	ErrClosed                     = errors.New("session closed")
)

var noopProduceCallback = func(error) {}

// State is the broker-facing state of a client session.
type State byte

const (
	StateUnbound State = iota
	StateConnected
	StateInTransaction
	StateClosed
)

// ConfigProvider returns the connector configuration visible to a new BIND.
type ConfigProvider func() connectorconfig.ConnectorsConfig

// MessageHandler receives a broker message without copying its payload or headers.
// The reader and opaque args are provided so an adapter can encode the broker-specific
// message ID directly into its wire response.
type MessageHandler func(
	subscriptionID byte,
	reader connector.Reader,
	message []byte,
	topic string,
	headers [][]byte,
	args ...any,
)

// FetchMessageHandlers let auto-commit adapters use the connector callback
// directly while manual-commit delivery retains reader and subscription context.
type FetchMessageHandlers struct {
	AutoCommit            func(message []byte, topic string, args ...any)
	AutoCommitWithHeaders func(message []byte, topic string, headers [][]byte, args ...any)
	Manual                MessageHandler
}
type SubscriptionMessageHandlers struct {
	Message func(
		subscriptionID byte,
		reader connector.Reader,
	) func(message []byte, topic string, args ...any)
	MessageWithHeaders func(
		subscriptionID byte,
		reader connector.Reader,
	) func(message []byte, topic string, headers [][]byte, args ...any)
}

// AckResultHandlers receive the top-level result before any per-message result.
// Core serializes the callbacks and invokes Message exactly once per message ID
// after a successful Result callback.
type AckResultHandlers struct {
	Result  func(error)
	Message func(messageID []byte, err error)
}

// Manager is the route resource factory/pool used by a Session Core.
type Manager interface {
	GetReader(route string, autoCommit bool) (connector.ReadCloser, error)
	GetWriter(route string) (connector.WriteCloser, error)
	PutWriter(writer connector.WriteCloser, route string)
	Close()
}

// ManagerFactory creates the connector manager selected by BIND.
type ManagerFactory func(connectorconfig.ConnectorConfig, string, *slog.Logger) Manager

type fetchKey struct {
	route       string
	autoCommit  bool
	withHeaders bool
}

type readerState struct {
	reader      connector.ReadCloser
	route       string
	autoCommit  bool
	withHeaders bool
	fetchKey    *fetchKey
	cancel      context.CancelFunc
	closeOnce   sync.Once
}

func (r *readerState) close() {
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		_ = r.reader.Close()
	})
}

// Core owns connector selection and every broker-facing session lifecycle.
type Core struct {
	ctx                context.Context
	baseConfig         connectorconfig.ConnectorsConfig
	baseConfigProvider ConfigProvider
	managerFactory     ManagerFactory
	l                  *slog.Logger

	mu           sync.Mutex
	state        atomic.Uint32
	manager      Manager
	writers      map[string]connector.WriteCloser
	txWriter     connector.WriteCloser
	txRoute      string
	readers      map[byte]*readerState
	fetchReaders map[fetchKey]byte
	subIDs       *commonpool.BytePool

	pending sync.WaitGroup
	opMu    sync.Mutex
	fetchMu fetchOpLock
	fetches *fetchOp
	acks    *ackOp

	closeOnce sync.Once
	closeErr  error
}

type fetchOpLock struct {
	held atomic.Bool
}

func (l *fetchOpLock) Lock() {
	for !l.held.CompareAndSwap(false, true) {
		runtime.Gosched()
	}
}

func (l *fetchOpLock) Unlock() {
	l.held.Store(false)
}

type fetchOp struct {
	core           *Core
	next           *fetchOp
	id             byte
	reader         connector.Reader
	message        MessageHandler
	count          uint32
	fetchErr       error
	respond        func(uint32, error)
	deliver        func([]byte, string, ...any)
	deliverHeaders func([]byte, string, [][]byte, ...any)
}

func newFetchOp() *fetchOp {
	op := &fetchOp{}
	op.respond = op.onResponse
	op.deliver = op.onMessage
	op.deliverHeaders = op.onMessageWithHeaders
	return op
}

func (op *fetchOp) onResponse(count uint32, err error) {
	op.count, op.fetchErr = count, err
}

func (op *fetchOp) onMessage(payload []byte, topic string, args ...any) {
	op.deliverMessage(payload, topic, nil, args...)
}

func (op *fetchOp) onMessageWithHeaders(payload []byte, topic string, headers [][]byte, args ...any) {
	op.deliverMessage(payload, topic, headers, args...)
}

func (op *fetchOp) deliverMessage(payload []byte, topic string, headers [][]byte, args ...any) {
	if op.message != nil {
		op.message(op.id, op.reader, payload, topic, headers, args...)
	}
}

func (op *fetchOp) finish() {
	core := op.core
	op.core, op.reader, op.message = nil, nil, nil
	op.count, op.fetchErr = 0, nil
	core.putFetchOp(op)
	core.pending.Done()
}

type ackMessageResult struct {
	messageID []byte
	err       error
}

type ackOp struct {
	core     *Core
	next     *ackOp
	handlers AckResultHandlers
	expected int
	received int
	buffered []ackMessageResult
	topDone  bool
	finished bool
	topErr   error
	done     func(error)
	each     func([]byte, error)
}

func newAckOp() *ackOp {
	op := &ackOp{}
	op.done = op.onDone
	op.each = op.onEach
	return op
}

func (op *ackOp) onDone(err error) {
	if op.finished || op.topDone {
		return
	}
	op.topDone, op.topErr = true, err
	if op.handlers.Result != nil {
		op.handlers.Result(err)
	}
	if err == nil && op.handlers.Message != nil {
		for i := range op.buffered {
			result := &op.buffered[i]
			op.handlers.Message(result.messageID, result.err)
		}
	}
	complete := err != nil || op.received >= op.expected
	if complete {
		op.finished = true
		op.finish()
	}
}

func (op *ackOp) onEach(messageID []byte, err error) {
	if op.finished || op.topErr != nil {
		return
	}
	op.received++
	if op.topDone {
		if op.handlers.Message != nil {
			op.handlers.Message(messageID, err)
		}
	} else {
		op.buffered = append(op.buffered, ackMessageResult{messageID: messageID, err: err})
	}
	if op.topDone && op.received >= op.expected {
		op.finished = true
		op.finish()
	}
}

func (op *ackOp) finish() {
	core := op.core
	op.core = nil
	op.handlers = AckResultHandlers{}
	op.expected, op.received = 0, 0
	clear(op.buffered)
	op.buffered = op.buffered[:0]
	op.topDone, op.finished, op.topErr = false, false, nil
	core.putAckOp(op)
	core.pending.Done()
}

// New creates an unbound Session Core.
func New(
	ctx context.Context,
	baseConfig connectorconfig.ConnectorsConfig,
	baseConfigProvider ConfigProvider,
	l *slog.Logger,
) *Core {
	return NewWithManagerFactory(ctx, baseConfig, baseConfigProvider, l, func(conf connectorconfig.ConnectorConfig, name string, l *slog.Logger) Manager {
		return connectors.NewManagerV2(conf, name, l)
	})
}

// NewWithManagerFactory creates a Session Core with an injectable manager seam.
// It is intended for deterministic contract tests and benchmark connectors.
func NewWithManagerFactory(
	ctx context.Context,
	baseConfig connectorconfig.ConnectorsConfig,
	baseConfigProvider ConfigProvider,
	l *slog.Logger,
	factory ManagerFactory,
) *Core {
	if ctx == nil {
		ctx = context.Background()
	}
	if l == nil {
		l = slog.Default()
	}
	return &Core{
		ctx:                ctx,
		baseConfig:         baseConfig,
		baseConfigProvider: baseConfigProvider,
		managerFactory:     factory,
		l:                  l,
		writers:            make(map[string]connector.WriteCloser),
		readers:            make(map[byte]*readerState),
		fetchReaders:       make(map[fetchKey]byte),
		subIDs:             commonpool.NewBytePool(),
	}
}

// State returns the current broker-facing session state.
func (c *Core) State() State {
	return State(c.state.Load())
}

// Bind selects a connector, runs bind middleware, applies allowed overrides,
// and creates the session's connector manager.
func (c *Core) Bind(connectorName string, meta, overrides map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.State()
	if state == StateClosed {
		return ErrClosed
	}
	if state != StateUnbound {
		return ErrAlreadyBound
	}

	configs := c.baseConfig
	if c.baseConfigProvider != nil {
		configs = c.baseConfigProvider()
	}
	connectorConfig, ok := configs[connectorName]
	if !ok {
		return ErrConnectorNotFound
	}

	if len(connectorConfig.BindMiddlewares) > 0 {
		if meta == nil {
			meta = map[string]string{}
		}
		if err := bmw.Chain(c.ctx, meta, connectorConfig.BindMiddlewares, c.l); err != nil {
			return err
		}
	}

	modifiedConfig := connectorConfig
	if len(overrides) > 0 {
		var err error
		modifiedConfig, err = connectors.ApplyOverrides(connectorConfig, overrides)
		if err != nil {
			return err
		}
	}

	c.manager = c.managerFactory(modifiedConfig, connectorName, c.l)
	c.state.Store(uint32(StateConnected))
	return nil
}

// Produce sends one non-transactional message through the shared route writer.
// Payload and header ownership stays with the caller until callback returns.
func (c *Core) Produce(route string, message []byte, headers [][]byte, callback func(error)) error {
	c.mu.Lock()
	state := c.State()
	if state == StateClosed {
		c.mu.Unlock()
		return ErrClosed
	}
	if state == StateUnbound {
		c.mu.Unlock()
		return ErrNotBound
	}
	if state == StateInTransaction {
		c.mu.Unlock()
		return ErrNormalProduceInTransaction
	}

	w, err := c.writerLocked(route)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if callback == nil {
		callback = noopProduceCallback
	}
	c.mu.Unlock()

	if headers == nil {
		w.Produce(c.ctx, message, callback)
	} else {
		w.HProduce(c.ctx, message, headers, callback)
	}
	return nil
}

// TxProduce lazily acquires and begins the writer selected by Begin, then sends
// one transaction message through it.
func (c *Core) TxProduce(message []byte, headers [][]byte, callback func(error)) error {
	c.mu.Lock()
	state := c.State()
	if state == StateClosed {
		c.mu.Unlock()
		return ErrClosed
	}
	if state == StateUnbound {
		c.mu.Unlock()
		return ErrNotBound
	}
	if state != StateInTransaction {
		c.mu.Unlock()
		return ErrNoTransaction
	}
	w, err := c.txWriterLocked()
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if callback == nil {
		callback = noopProduceCallback
	}
	c.mu.Unlock()

	if headers == nil {
		w.Produce(c.ctx, message, callback)
	} else {
		w.HProduce(c.ctx, message, headers, callback)
	}
	return nil
}

func (c *Core) writerLocked(route string) (connector.WriteCloser, error) {
	if w, ok := c.writers[route]; ok {
		return w, nil
	}
	w, err := c.manager.GetWriter(route)
	if err != nil {
		return nil, err
	}
	c.writers[route] = w
	return w, nil
}
func (c *Core) txWriterLocked() (connector.WriteCloser, error) {
	if c.txWriter != nil {
		return c.txWriter, nil
	}
	w, err := c.manager.GetWriter(c.txRoute)
	if err != nil {
		return nil, err
	}
	if err := w.BeginTx(c.ctx); err != nil {
		c.manager.PutWriter(w, c.txRoute)
		return nil, err
	}
	c.txWriter = w
	return w, nil
}

// Begin selects the transaction route after flushing and returning all
// non-transactional writers. The transaction writer is acquired lazily by the
// first TxProduce call.
func (c *Core) Begin(route string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.State()
	if state == StateClosed {
		return ErrClosed
	}
	if state == StateUnbound {
		return ErrNotBound
	}
	if state == StateInTransaction {
		return ErrTransactionActive
	}

	if len(c.writers) > 0 {
		for _, w := range c.writers {
			if err := w.Flush(c.ctx); err != nil {
				return fmt.Errorf("flush: %w", err)
			}
		}
		for writerRoute, w := range c.writers {
			c.manager.PutWriter(w, writerRoute)
			delete(c.writers, writerRoute)
		}
	}

	c.txRoute = route
	c.state.Store(uint32(StateInTransaction))
	return nil
}

// Commit flushes and commits the transaction writer exactly once.
func (c *Core) Commit() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.State()
	if state == StateClosed {
		return ErrClosed
	}
	if state == StateUnbound {
		return ErrNotBound
	}
	if state != StateInTransaction {
		return ErrNoTransaction
	}

	w, route := c.txWriter, c.txRoute
	c.txWriter, c.txRoute = nil, ""
	c.state.Store(uint32(StateConnected))
	if w == nil {
		return nil
	}

	flushErr := w.Flush(c.ctx)
	commitErr := w.CommitTx(c.ctx)
	c.manager.PutWriter(w, route)
	if flushErr == nil {
		return commitErr
	}
	if commitErr == nil {
		return flushErr
	}
	return errors.Join(flushErr, commitErr)
}

// Rollback rolls back the transaction writer exactly once.
func (c *Core) Rollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.State()
	if state == StateClosed {
		return ErrClosed
	}
	if state == StateUnbound {
		return ErrNotBound
	}
	if state != StateInTransaction {
		return ErrNoTransaction
	}

	w, route := c.txWriter, c.txRoute
	c.txWriter, c.txRoute = nil, ""
	c.state.Store(uint32(StateConnected))
	if w == nil {
		return nil
	}

	err := w.RollbackTx(c.ctx)
	c.manager.PutWriter(w, route)
	return err
}

// Fetch performs pull delivery through a shared implicit-reader cache.
// It returns the connector response after all synchronous message callbacks finish.
func (c *Core) Fetch(
	route string,
	autoCommit bool,
	withHeaders bool,
	batchSize uint32,
	handlers FetchMessageHandlers,
) (byte, uint32, error) {
	c.mu.Lock()
	if c.State() == StateClosed {
		c.mu.Unlock()
		return 0, 0, ErrClosed
	}
	if c.State() == StateUnbound {
		c.mu.Unlock()
		return 0, 0, ErrNotBound
	}

	key := fetchKey{route: route, autoCommit: autoCommit, withHeaders: withHeaders}
	id, ok := c.fetchReaders[key]
	var rs *readerState
	if ok {
		rs = c.readers[id]
	}
	if rs == nil {
		var err error
		id, err = c.subIDs.Get()
		if err != nil {
			c.mu.Unlock()
			return 0, 0, err
		}
		r, err := c.manager.GetReader(route, autoCommit)
		if err != nil {
			_ = c.subIDs.Put(id)
			c.mu.Unlock()
			return 0, 0, err
		}
		keyCopy := key
		rs = &readerState{
			reader:      r,
			route:       route,
			autoCommit:  autoCommit,
			withHeaders: withHeaders,
			fetchKey:    &keyCopy,
		}
		c.readers[id] = rs
		c.fetchReaders[key] = id
	}
	c.pending.Add(1)
	c.mu.Unlock()

	op := c.getFetchOp()
	op.core = c
	op.id = id
	op.reader = rs.reader
	op.message = handlers.Manual
	switch {
	case autoCommit && withHeaders && handlers.AutoCommitWithHeaders != nil:
		rs.reader.FetchWithHeaders(c.ctx, batchSize, op.respond, handlers.AutoCommitWithHeaders)
	case autoCommit && !withHeaders && handlers.AutoCommit != nil:
		rs.reader.Fetch(c.ctx, batchSize, op.respond, handlers.AutoCommit)
	case withHeaders:
		rs.reader.FetchWithHeaders(c.ctx, batchSize, op.respond, op.deliverHeaders)
	default:
		rs.reader.Fetch(c.ctx, batchSize, op.respond, op.deliver)
	}
	count, err := op.count, op.fetchErr
	op.finish()
	return id, count, err
}

// Subscribe creates a shared push reader. ready runs before delivery starts, so
// adapters can emit the subscription response before any message.
func (c *Core) Subscribe(
	route string,
	autoCommit bool,
	withHeaders bool,
	ready func(subscriptionID byte) error,
	handlers SubscriptionMessageHandlers,
	onRetry func(error),
) error {
	c.mu.Lock()
	if c.State() == StateClosed {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.State() == StateUnbound {
		c.mu.Unlock()
		return ErrNotBound
	}
	id, err := c.subIDs.Get()
	if err != nil {
		c.mu.Unlock()
		return err
	}
	r, err := c.manager.GetReader(route, autoCommit)
	if err != nil {
		_ = c.subIDs.Put(id)
		c.mu.Unlock()
		return err
	}
	ctx, cancel := context.WithCancel(c.ctx)
	rs := &readerState{
		reader:      r,
		route:       route,
		autoCommit:  autoCommit,
		withHeaders: withHeaders,
		cancel:      cancel,
	}
	c.readers[id] = rs
	c.pending.Add(1)
	c.mu.Unlock()

	if ready != nil {
		if err := ready(id); err != nil {
			c.pending.Done()
			c.removeReader(id, rs)
			return err
		}
	}

	go c.subscriptionLoop(ctx, id, rs, handlers, onRetry)
	return nil
}

func (c *Core) subscriptionLoop(
	ctx context.Context,
	id byte,
	rs *readerState,
	handlers SubscriptionMessageHandlers,
	onRetry func(error),
) {
	defer c.pending.Done()
	defer c.removeReader(id, rs)

	for {
		if ctx.Err() != nil {
			return
		}
		var err error
		if rs.withHeaders {
			var message func([]byte, string, [][]byte, ...any)
			if handlers.MessageWithHeaders != nil {
				message = handlers.MessageWithHeaders(id, rs.reader)
			}
			err = rs.reader.SubscribeWithHeaders(ctx, message)
		} else {
			var message func([]byte, string, ...any)
			if handlers.Message != nil {
				message = handlers.Message(id, rs.reader)
			}
			err = rs.reader.Subscribe(ctx, message)
		}
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil && onRetry != nil {
			onRetry(err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

// Unsubscribe releases a reader and subscription ID exactly once.
func (c *Core) Unsubscribe(subscriptionID byte) error {
	c.mu.Lock()
	if c.State() == StateClosed {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.State() == StateUnbound {
		c.mu.Unlock()
		return ErrNotBound
	}
	rs := c.readers[subscriptionID]
	c.mu.Unlock()
	if rs == nil {
		return fmt.Errorf("%w: %d", ErrSubscriptionNotFound, subscriptionID)
	}
	c.removeReader(subscriptionID, rs)
	return nil
}

func (c *Core) removeReader(id byte, rs *readerState) {
	removed := false
	c.mu.Lock()
	if current := c.readers[id]; current == rs {
		delete(c.readers, id)
		if rs.fetchKey != nil {
			delete(c.fetchReaders, *rs.fetchKey)
		}
		removed = true
	}
	c.mu.Unlock()
	if !removed {
		return
	}
	rs.close()
	if err := c.subIDs.Put(id); err != nil {
		c.l.Error("return subscription id", "subscription_id", id, "err", err)
	}
}

// Ack applies broker acknowledgement and streams its results to handlers.
func (c *Core) Ack(subscriptionID byte, messageIDs [][]byte, handlers AckResultHandlers) error {
	return c.acknowledge(subscriptionID, messageIDs, false, handlers)
}

// Nack applies broker negative acknowledgement and streams its results to handlers.
func (c *Core) Nack(subscriptionID byte, messageIDs [][]byte, handlers AckResultHandlers) error {
	return c.acknowledge(subscriptionID, messageIDs, true, handlers)
}

func (c *Core) acknowledge(subscriptionID byte, messageIDs [][]byte, nack bool, handlers AckResultHandlers) error {
	c.mu.Lock()
	state := c.State()
	if state == StateClosed {
		c.mu.Unlock()
		return ErrClosed
	}
	if state == StateUnbound {
		c.mu.Unlock()
		return ErrNotBound
	}
	if len(messageIDs) == 0 {
		c.mu.Unlock()
		if handlers.Result != nil {
			handlers.Result(nil)
		}
		return nil
	}
	rs := c.readers[subscriptionID]
	if rs != nil {
		c.pending.Add(1)
	}
	c.mu.Unlock()
	if rs == nil {
		return fmt.Errorf("%w: %d", ErrSubscriptionNotFound, subscriptionID)
	}

	op := c.getAckOp()
	op.core = c
	op.handlers = handlers
	op.expected = len(messageIDs)
	if nack {
		rs.reader.Nack(c.ctx, messageIDs, op.done, op.each)
	} else {
		rs.reader.Ack(c.ctx, messageIDs, op.done, op.each)
	}
	return nil
}

func (c *Core) getFetchOp() *fetchOp {
	c.fetchMu.Lock()
	op := c.fetches
	if op != nil {
		c.fetches = op.next
		op.next = nil
	}
	c.fetchMu.Unlock()
	if op == nil {
		op = newFetchOp()
	}
	return op
}

func (c *Core) putFetchOp(op *fetchOp) {
	c.fetchMu.Lock()
	op.next = c.fetches
	c.fetches = op
	c.fetchMu.Unlock()
}

func (c *Core) getAckOp() *ackOp {
	c.opMu.Lock()
	op := c.acks
	if op != nil {
		c.acks = op.next
		op.next = nil
	}
	c.opMu.Unlock()
	if op == nil {
		op = newAckOp()
	}
	return op
}

func (c *Core) putAckOp(op *ackOp) {
	c.opMu.Lock()
	op.next = c.acks
	c.acks = op
	c.opMu.Unlock()
}

// Close releases every broker resource exactly once and waits for pending work.
func (c *Core) Close() error {
	c.closeOnce.Do(func() {
		cleanupCtx := context.WithoutCancel(c.ctx)

		c.mu.Lock()
		if c.State() == StateClosed {
			c.mu.Unlock()
			return
		}
		c.state.Store(uint32(StateClosed))
		m := c.manager
		writers := c.writers
		c.writers = make(map[string]connector.WriteCloser)
		txWriter, txRoute := c.txWriter, c.txRoute
		c.txWriter, c.txRoute = nil, ""
		readers := c.readers
		c.readers = make(map[byte]*readerState)
		c.fetchReaders = make(map[fetchKey]byte)
		c.manager = nil
		c.mu.Unlock()

		var errs []error
		for route, w := range writers {
			if err := w.Flush(cleanupCtx); err != nil {
				errs = append(errs, fmt.Errorf("flush writer %q: %w", route, err))
			}
			if m != nil {
				m.PutWriter(w, route)
			}
		}
		if txWriter != nil {
			if err := txWriter.RollbackTx(cleanupCtx); err != nil {
				errs = append(errs, fmt.Errorf("rollback transaction: %w", err))
			}
			if m != nil {
				m.PutWriter(txWriter, txRoute)
			}
		}
		for id, rs := range readers {
			rs.close()
			if err := c.subIDs.Put(id); err != nil {
				errs = append(errs, fmt.Errorf("return subscription id %d: %w", id, err))
			}
		}

		c.pending.Wait()
		if m != nil {
			m.Close()
		}
		c.closeErr = errors.Join(errs...)
	})
	return c.closeErr
}
