package core

import (
	"bytes"
	"context"
	"encoding/binary"
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
	ErrNotBound                   = errors.New("not bound")
	ErrAlreadyBound               = errors.New("already bound")
	ErrConnectorNotFound          = errors.New("connector not found")
	ErrTransactionActive          = errors.New("transaction already in progress")
	ErrNoTransaction              = errors.New("no transaction in progress")
	ErrNormalProduceInTransaction = errors.New("normal produce is not allowed in a transaction")
	ErrSubscriptionNotFound       = errors.New("subscription not found")
	ErrSubscriptionEnded          = errors.New("subscription ended")
	ErrFetchBusy                  = connector.ErrFetchBusy
	ErrInvalidBatchSize           = errors.New("fetch batch size must be positive")
	ErrInvalidMessageID           = connector.ErrInvalidMessageID
	ErrCommitOutcomeUnknown       = errors.New("transaction commit outcome unknown")
	ErrTransactionAborted         = errors.New("transaction aborted")
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

// GenerationProvider returns the immutable generation visible to a new BIND.
type GenerationProvider func() *connector.Generation

// BindResult is the immutable connector contract pinned to a successful session BIND.
type BindResult struct {
	Routes map[string]connector.RouteProfile
}

// MessageHandler receives a broker message without copying its payload or headers.
// The reader and opaque args are provided so an adapter can encode the broker-specific
// message ID directly into its wire response.
type MessageHandler func(
	subscriptionID byte,
	reader connector.Reader,
	message []byte,
	source string,
	headers [][]byte,
	args ...any,
)

// FetchMessageHandlers let auto-commit adapters use the connector callback
// directly while manual-commit delivery retains reader and subscription context.
type FetchMessageHandlers struct {
	AutoCommit            func(message []byte, source string, args ...any)
	AutoCommitWithHeaders func(message []byte, source string, headers [][]byte, args ...any)
	Manual                MessageHandler
}
type SubscriptionMessageHandlers struct {
	Message func(
		subscriptionID byte,
		reader connector.Reader,
	) func(message []byte, source string, args ...any)
	MessageWithHeaders func(
		subscriptionID byte,
		reader connector.Reader,
	) func(message []byte, source string, headers [][]byte, args ...any)
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
	RouteProfile(route string) (connector.RouteProfile, error)
	RouteProfiles() map[string]connector.RouteProfile
	GetReader(route string, autoSettle bool) (connector.ReadCloser, error)
	GetWriter(route string) (connector.WriteCloser, error)
	PutWriter(writer connector.WriteCloser, route string) error
	DiscardWriter(writer connector.WriteCloser) error
	Close(context.Context) error
}

// ManagerFactory is the deterministic test seam for Session Core.
type ManagerFactory func(connectorconfig.ConnectorConfig, string, *slog.Logger) Manager

type fetchKey struct {
	route       string
	autoCommit  bool
	withHeaders bool
}

const (
	messageIDVersion      byte = 1
	messageIDTokenOffset       = 1
	messageIDEnvelopeLen       = messageIDTokenOffset + 8
	defaultCleanupTimeout      = 30 * time.Second
)

type settlementRange struct {
	first uint64
	last  uint64
}

type readerState struct {
	reader            connector.ReadCloser
	scoped            connector.Reader
	profile           connector.RouteProfile
	route             string
	autoSettle        bool
	withHeaders       bool
	incarnation       uint32
	fetchKey          *fetchKey
	cancel            context.CancelFunc
	closeOnce         sync.Once
	closeErr          error
	fetching          atomic.Bool
	settlementMu      sync.Mutex
	activeSettlements *ackOp
	settledThrough    uint64
	settledRanges     []settlementRange
}

func (r *readerState) close() error {
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.closeErr = r.reader.Close()
	})
	return r.closeErr
}

type scopedReader struct {
	connector.Reader
	incarnation uint32
	sequence    atomic.Uint32
}

func (r *scopedReader) MsgIDArgsLen() int {
	return messageIDEnvelopeLen + r.Reader.MsgIDArgsLen()
}

func (r *scopedReader) EncodeMsgID(buf []byte, source string, args ...any) []byte {
	sequence := r.sequence.Add(1)
	buf = append(buf, messageIDVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.incarnation)<<32|uint64(sequence))
	return r.Reader.EncodeMsgID(buf, source, args...)
}
func (r *readerState) containsSettlement(sequence uint64) bool {
	if sequence <= r.settledThrough {
		return true
	}
	for _, settled := range r.settledRanges {
		if sequence < settled.first {
			break
		}
		if sequence <= settled.last {
			return true
		}
	}
	for active := r.activeSettlements; active != nil; active = active.activeNext {
		if active.containsSequence(sequence) {
			return true
		}
	}
	return false
}

func (r *readerState) addActiveSettlement(op *ackOp) {
	op.activeNext = r.activeSettlements
	op.active = true
	r.activeSettlements = op
}

func (r *readerState) removeActiveSettlement(op *ackOp) {
	r.settlementMu.Lock()
	link := &r.activeSettlements
	for *link != nil {
		if *link == op {
			*link = op.activeNext
			break
		}
		link = &(*link).activeNext
	}
	r.settlementMu.Unlock()
}

func (r *readerState) markSettledBatch(sequences []uint64, successful []bool) {
	r.settlementMu.Lock()
	for i, sequence := range sequences {
		if successful[i] {
			r.markSettledLocked(sequence)
		}
	}
	r.settlementMu.Unlock()
}

func (r *readerState) markSettledLocked(sequence uint64) {
	if sequence <= r.settledThrough {
		return
	}
	if sequence == r.settledThrough+1 {
		r.settledThrough = sequence
		for len(r.settledRanges) > 0 && r.settledRanges[0].first == r.settledThrough+1 {
			r.settledThrough = r.settledRanges[0].last
			copy(r.settledRanges, r.settledRanges[1:])
			r.settledRanges = r.settledRanges[:len(r.settledRanges)-1]
		}
		return
	}
	for i := range r.settledRanges {
		settled := &r.settledRanges[i]
		if sequence >= settled.first && sequence <= settled.last {
			return
		}
		if sequence == settled.first-1 {
			settled.first = sequence
			return
		}
		if sequence == settled.last+1 {
			settled.last = sequence
			if i+1 < len(r.settledRanges) && settled.last+1 == r.settledRanges[i+1].first {
				settled.last = r.settledRanges[i+1].last
				copy(r.settledRanges[i+1:], r.settledRanges[i+2:])
				r.settledRanges = r.settledRanges[:len(r.settledRanges)-1]
			}
			return
		}
		if sequence < settled.first {
			r.settledRanges = append(r.settledRanges, settlementRange{})
			copy(r.settledRanges[i+1:], r.settledRanges[i:])
			r.settledRanges[i] = settlementRange{first: sequence, last: sequence}
			return
		}
	}
	r.settledRanges = append(r.settledRanges, settlementRange{first: sequence, last: sequence})
}

// Core owns connector selection and every broker-facing session lifecycle.
type Core struct {
	ctx                context.Context
	baseGeneration     *connector.Generation
	generationProvider GenerationProvider
	baseConfig         connectorconfig.ConnectorsConfig
	baseConfigProvider func() connectorconfig.ConnectorsConfig
	managerFactory     ManagerFactory
	l                  *slog.Logger
	cleanupTimeout     time.Duration

	mu           sync.Mutex
	state        atomic.Uint32
	manager      Manager
	writers      map[string]connector.WriteCloser
	txWriter     connector.WriteCloser
	txRoute      string
	readers      map[byte]*readerState
	fetchReaders map[fetchKey]byte
	subIDs       *commonpool.BytePool
	incarnation  atomic.Uint32

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

func (op *fetchOp) onMessage(payload []byte, source string, args ...any) {
	op.deliverMessage(payload, source, nil, args...)
}

func (op *fetchOp) onMessageWithHeaders(payload []byte, source string, headers [][]byte, args ...any) {
	op.deliverMessage(payload, source, headers, args...)
}

func (op *fetchOp) deliverMessage(payload []byte, source string, headers [][]byte, args ...any) {
	if op.message != nil {
		op.message(op.id, op.reader, payload, source, headers, args...)
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
	core       *Core
	reader     *readerState
	next       *ackOp
	activeNext *ackOp
	active     bool
	handlers   AckResultHandlers
	messageIDs [][]byte
	payloads   [][]byte
	sequences  []uint64
	successful []bool
	matched    []bool
	expected   int
	received   int
	buffered   []ackMessageResult
	topDone    bool
	finished   bool
	topErr     error
	done       func(error)
	each       func([]byte, error)
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
		if err == nil {
			op.reader.markSettledBatch(op.sequences, op.successful)
		}
		op.finished = true
		op.finish()
	}
}

func (op *ackOp) onEach(payload []byte, err error) {
	if op.finished || op.topErr != nil {
		return
	}
	op.received++
	messageID, index := op.matchMessageID(payload)
	if index >= 0 {
		op.successful[index] = err == nil
	}
	if op.topDone {
		if op.handlers.Message != nil {
			op.handlers.Message(messageID, err)
		}
	} else {
		op.buffered = append(op.buffered, ackMessageResult{messageID: messageID, err: err})
	}
	if op.topDone && op.received >= op.expected {
		op.reader.markSettledBatch(op.sequences, op.successful)
		op.finished = true
		op.finish()
	}
}

func (op *ackOp) matchMessageID(payload []byte) ([]byte, int) {
	if op.received <= len(op.payloads) {
		index := op.received - 1
		if !op.matched[index] && bytes.Equal(op.payloads[index], payload) {
			op.matched[index] = true
			return op.messageIDs[index], index
		}
	}
	for index := range op.payloads {
		if !op.matched[index] && bytes.Equal(op.payloads[index], payload) {
			op.matched[index] = true
			return op.messageIDs[index], index
		}
	}
	return nil, -1
}

func (op *ackOp) containsSequence(sequence uint64) bool {
	for _, candidate := range op.sequences {
		if candidate == sequence {
			return true
		}
	}
	return false
}

func (op *ackOp) finish() {
	core := op.core
	if op.active {
		op.reader.removeActiveSettlement(op)
	}
	op.core, op.reader, op.activeNext = nil, nil, nil
	op.active = false
	op.handlers = AckResultHandlers{}
	op.expected, op.received = 0, 0
	clear(op.messageIDs)
	clear(op.payloads)
	clear(op.sequences)
	clear(op.successful)
	clear(op.matched)
	clear(op.buffered)
	op.messageIDs = op.messageIDs[:0]
	op.payloads = op.payloads[:0]
	op.sequences = op.sequences[:0]
	op.successful = op.successful[:0]
	op.matched = op.matched[:0]
	op.buffered = op.buffered[:0]
	op.topDone, op.finished, op.topErr = false, false, nil
	core.putAckOp(op)
	core.pending.Done()
}

// New creates an unbound Session Core over a shared connector generation.
func New(
	ctx context.Context,
	baseGeneration *connector.Generation,
	generationProvider GenerationProvider,
	l *slog.Logger,
) *Core {
	return newCore(ctx, baseGeneration, generationProvider, nil, nil, nil, l)
}

// NewWithManagerFactory creates a Session Core with an injectable manager seam.
// It is intended for deterministic contract tests and benchmarks.
func NewWithManagerFactory(
	ctx context.Context,
	baseConfig connectorconfig.ConnectorsConfig,
	baseConfigProvider func() connectorconfig.ConnectorsConfig,
	l *slog.Logger,
	factory ManagerFactory,
) *Core {
	return newCore(ctx, nil, nil, baseConfig, baseConfigProvider, factory, l)
}

func newCore(
	ctx context.Context,
	baseGeneration *connector.Generation,
	generationProvider GenerationProvider,
	baseConfig connectorconfig.ConnectorsConfig,
	baseConfigProvider func() connectorconfig.ConnectorsConfig,
	managerFactory ManagerFactory,
	l *slog.Logger,
) *Core {
	if ctx == nil {
		ctx = context.Background()
	}
	if l == nil {
		l = slog.Default()
	}
	return &Core{
		ctx:                ctx,
		baseGeneration:     baseGeneration,
		generationProvider: generationProvider,
		baseConfig:         baseConfig,
		baseConfigProvider: baseConfigProvider,
		managerFactory:     managerFactory,
		l:                  l,
		cleanupTimeout:     defaultCleanupTimeout,
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
// creates the session's connector manager, and returns its pinned route contracts.
func (c *Core) Bind(connectorName string, meta, overrides map[string]string) (BindResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.State()
	if state == StateClosed {
		return BindResult{}, ErrClosed
	}
	if state != StateUnbound {
		return BindResult{}, ErrAlreadyBound
	}

	if c.managerFactory != nil {
		return c.bindWithFactory(connectorName, meta, overrides)
	}

	generation := c.baseGeneration
	if c.generationProvider != nil {
		generation = c.generationProvider()
	}
	if generation == nil {
		return BindResult{}, ErrConnectorNotFound
	}
	connectorConfig, ok := generation.Config(connectorName)
	if !ok {
		return BindResult{}, ErrConnectorNotFound
	}
	if err := c.runBindMiddleware(connectorConfig, meta); err != nil {
		return BindResult{}, err
	}

	ownedGeneration := false
	if len(overrides) > 0 {
		modified, err := connectors.ApplyOverrides(connectorConfig, overrides)
		if err != nil {
			return BindResult{}, err
		}
		generation, err = connector.CompileGeneration(connectorconfig.ConnectorsConfig{connectorName: modified}, c.l)
		if err != nil {
			return BindResult{}, err
		}
		ownedGeneration = true
	}
	binding, err := generation.Acquire(connectorName)
	if err != nil {
		if ownedGeneration {
			generation.Retire()
		}
		return BindResult{}, err
	}
	if ownedGeneration {
		generation.Retire()
	}
	c.manager = connectors.NewManagerV2(binding, c.l)
	c.state.Store(uint32(StateConnected))
	return BindResult{Routes: c.manager.RouteProfiles()}, nil
}

func (c *Core) bindWithFactory(connectorName string, meta, overrides map[string]string) (BindResult, error) {
	configs := c.baseConfig
	if c.baseConfigProvider != nil {
		configs = c.baseConfigProvider()
	}
	connectorConfig, ok := configs[connectorName]
	if !ok {
		return BindResult{}, ErrConnectorNotFound
	}
	if err := c.runBindMiddleware(connectorConfig, meta); err != nil {
		return BindResult{}, err
	}
	if len(overrides) > 0 {
		modified, err := connectors.ApplyOverrides(connectorConfig, overrides)
		if err != nil {
			return BindResult{}, err
		}
		connectorConfig = modified
	}
	manager := c.managerFactory(connectorConfig, connectorName, c.l)
	if manager == nil {
		return BindResult{}, errors.New("connector manager factory returned nil")
	}
	c.manager = manager
	c.state.Store(uint32(StateConnected))
	return BindResult{Routes: manager.RouteProfiles()}, nil
}

func (c *Core) runBindMiddleware(config connectorconfig.ConnectorConfig, meta map[string]string) error {
	if len(config.BindMiddlewares) == 0 {
		return nil
	}
	if meta == nil {
		meta = map[string]string{}
	}
	return bmw.Chain(c.ctx, meta, config.BindMiddlewares, c.l)
}

func (c *Core) routeProfile(route string) (connector.RouteProfile, error) {
	if c.manager == nil {
		return connector.RouteProfile{}, ErrNotBound
	}
	return c.manager.RouteProfile(route)
}

func validateHeaderOperation(profile connector.RouteProfile, headers [][]byte) error {
	if headers == nil {
		return nil
	}
	if !profile.Headers {
		return connector.ErrOperationUnsupported
	}
	return connector.ValidateHeaders(headers)
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
	profile, err := c.routeProfile(route)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if !profile.Produce {
		c.mu.Unlock()
		return connector.ErrOperationUnsupported
	}
	if err := validateHeaderOperation(profile, headers); err != nil {
		c.mu.Unlock()
		return err
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

// TxProduce sends one message through the transaction established by Begin.
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
	if state != StateInTransaction || c.txWriter == nil {
		c.mu.Unlock()
		return ErrNoTransaction
	}
	profile, err := c.routeProfile(c.txRoute)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if err := validateHeaderOperation(profile, headers); err != nil {
		c.mu.Unlock()
		return err
	}
	w := c.txWriter
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

// Begin flushes ordinary writers and establishes a concrete broker transaction.
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
	profile, err := c.routeProfile(route)
	if err != nil {
		return err
	}
	if !profile.Transactions {
		return connector.ErrOperationUnsupported
	}
	for writerRoute, w := range c.writers {
		if err := w.Flush(c.ctx); err != nil {
			delete(c.writers, writerRoute)
			discardErr := c.manager.DiscardWriter(w)
			return errors.Join(fmt.Errorf("flush writer %q: %w", writerRoute, err), discardErr)
		}
		if err := c.manager.PutWriter(w, writerRoute); err != nil {
			delete(c.writers, writerRoute)
			return fmt.Errorf("return writer %q: %w", writerRoute, err)
		}
		delete(c.writers, writerRoute)
	}

	w, err := c.manager.GetWriter(route)
	if err != nil {
		return err
	}
	if err := w.BeginTx(c.ctx); err != nil {
		return errors.Join(err, c.manager.DiscardWriter(w))
	}
	c.txWriter, c.txRoute = w, route
	c.state.Store(uint32(StateInTransaction))
	return nil
}

// Commit terminates local transaction state and fails closed on any terminal error.
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
	if state != StateInTransaction || c.txWriter == nil {
		return ErrNoTransaction
	}
	w, route := c.txWriter, c.txRoute
	c.txWriter, c.txRoute = nil, ""
	c.state.Store(uint32(StateConnected))

	if err := w.Flush(c.ctx); err != nil {
		rollbackErr := w.RollbackTx(c.ctx)
		discardErr := c.manager.DiscardWriter(w)
		return errors.Join(fmt.Errorf("%w after flush: %v", ErrTransactionAborted, err), rollbackErr, discardErr)
	}
	if err := w.CommitTx(c.ctx); err != nil {
		discardErr := c.manager.DiscardWriter(w)
		return errors.Join(fmt.Errorf("%w: %v", ErrCommitOutcomeUnknown, err), discardErr)
	}
	if err := c.manager.PutWriter(w, route); err != nil {
		return fmt.Errorf("return transaction writer: %w", err)
	}
	return nil
}

// Rollback always terminates local transaction state and poisons a failed writer.
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
	if state != StateInTransaction || c.txWriter == nil {
		return ErrNoTransaction
	}
	w, route := c.txWriter, c.txRoute
	c.txWriter, c.txRoute = nil, ""
	c.state.Store(uint32(StateConnected))
	if err := w.RollbackTx(c.ctx); err != nil {
		return errors.Join(err, c.manager.DiscardWriter(w))
	}
	return c.manager.PutWriter(w, route)
}

// Fetch performs pull delivery through a shared implicit-reader cache.
// It returns the connector response after all synchronous message callbacks finish.
func (c *Core) Fetch(
	route string,
	autoSettle bool,
	withHeaders bool,
	batchSize uint32,
	handlers FetchMessageHandlers,
) (byte, uint32, error) {
	if batchSize == 0 {
		return 0, 0, ErrInvalidBatchSize
	}
	c.mu.Lock()
	if c.State() == StateClosed {
		c.mu.Unlock()
		return 0, 0, ErrClosed
	}
	if c.State() == StateUnbound {
		c.mu.Unlock()
		return 0, 0, ErrNotBound
	}
	profile, err := c.routeProfile(route)
	if err != nil {
		c.mu.Unlock()
		return 0, 0, err
	}
	if !profile.Fetch || withHeaders && !profile.Headers || !autoSettle && !profile.ManualSettlement {
		c.mu.Unlock()
		return 0, 0, connector.ErrOperationUnsupported
	}

	key := fetchKey{route: route, autoCommit: autoSettle, withHeaders: withHeaders}
	id, ok := c.fetchReaders[key]
	var rs *readerState
	if ok {
		rs = c.readers[id]
	}
	if rs == nil {
		id, err = c.subIDs.Get()
		if err != nil {
			c.mu.Unlock()
			return 0, 0, err
		}
		r, getErr := c.manager.GetReader(route, autoSettle)
		if getErr != nil {
			_ = c.subIDs.Put(id)
			c.mu.Unlock()
			return 0, 0, getErr
		}
		keyCopy := key
		incarnation := c.incarnation.Add(1)
		rs = &readerState{
			reader:      r,
			profile:     profile,
			route:       route,
			autoSettle:  autoSettle,
			withHeaders: withHeaders,
			incarnation: incarnation,
			fetchKey:    &keyCopy,
		}
		rs.scoped = &scopedReader{Reader: r, incarnation: incarnation}
		c.readers[id] = rs
		c.fetchReaders[key] = id
	}
	if !rs.fetching.CompareAndSwap(false, true) {
		c.mu.Unlock()
		return id, 0, ErrFetchBusy
	}
	c.pending.Add(1)
	c.mu.Unlock()
	defer rs.fetching.Store(false)

	op := c.getFetchOp()
	op.core = c
	op.id = id
	op.reader = rs.scoped
	op.message = handlers.Manual
	delivered := uint32(0)
	overflow := false
	deliver := func(payload []byte, source string, args ...any) {
		if delivered >= batchSize {
			overflow = true
			return
		}
		delivered++
		op.onMessage(payload, source, args...)
	}
	deliverHeaders := func(payload []byte, source string, headers [][]byte, args ...any) {
		if delivered >= batchSize {
			overflow = true
			return
		}
		delivered++
		op.onMessageWithHeaders(payload, source, headers, args...)
	}
	switch {
	case autoSettle && withHeaders && handlers.AutoCommitWithHeaders != nil:
		rs.reader.FetchWithHeaders(c.ctx, batchSize, op.respond, func(payload []byte, source string, headers [][]byte, args ...any) {
			if delivered >= batchSize {
				overflow = true
				return
			}
			delivered++
			handlers.AutoCommitWithHeaders(payload, source, headers, args...)
		})
	case autoSettle && !withHeaders && handlers.AutoCommit != nil:
		rs.reader.Fetch(c.ctx, batchSize, op.respond, func(payload []byte, source string, args ...any) {
			if delivered >= batchSize {
				overflow = true
				return
			}
			delivered++
			handlers.AutoCommit(payload, source, args...)
		})
	case withHeaders:
		rs.reader.FetchWithHeaders(c.ctx, batchSize, op.respond, deliverHeaders)
	default:
		rs.reader.Fetch(c.ctx, batchSize, op.respond, deliver)
	}
	count, fetchErr := op.count, op.fetchErr
	if overflow || count > batchSize || count != delivered {
		fetchErr = errors.Join(fetchErr, fmt.Errorf("fetch contract violated: reported=%d delivered=%d maximum=%d", count, delivered, batchSize))
		count = delivered
	}
	op.finish()
	return id, count, fetchErr
}

// Subscribe creates a shared push reader. ready runs before delivery starts, so
// adapters can emit the subscription response before any message.
func (c *Core) Subscribe(
	route string,
	autoSettle bool,
	withHeaders bool,
	ready func(subscriptionID byte) error,
	handlers SubscriptionMessageHandlers,
	onTerminal func(error),
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
	profile, err := c.routeProfile(route)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if !profile.Subscribe || withHeaders && !profile.Headers || !autoSettle && !profile.ManualSettlement {
		c.mu.Unlock()
		return connector.ErrOperationUnsupported
	}
	id, err := c.subIDs.Get()
	if err != nil {
		c.mu.Unlock()
		return err
	}
	r, err := c.manager.GetReader(route, autoSettle)
	if err != nil {
		_ = c.subIDs.Put(id)
		c.mu.Unlock()
		return err
	}
	ctx, cancel := context.WithCancel(c.ctx)
	incarnation := c.incarnation.Add(1)
	rs := &readerState{
		reader:      r,
		profile:     profile,
		route:       route,
		autoSettle:  autoSettle,
		withHeaders: withHeaders,
		incarnation: incarnation,
		cancel:      cancel,
	}
	rs.scoped = &scopedReader{Reader: r, incarnation: incarnation}
	c.readers[id] = rs
	c.pending.Add(1)
	c.mu.Unlock()

	readyResult := make(chan error, 1)
	lifecycleDone := make(chan error, 1)
	var readyOnce sync.Once
	var readyErr error
	var readySucceeded atomic.Bool
	signalReady := func() error {
		readyOnce.Do(func() {
			if ready != nil {
				readyErr = ready(id)
			}
			if readyErr == nil {
				readySucceeded.Store(true)
			}
			readyResult <- readyErr
		})
		return readyErr
	}
	go func() {
		defer c.pending.Done()
		var lifecycleErr error
		if withHeaders {
			var message func([]byte, string, [][]byte, ...any)
			if handlers.MessageWithHeaders != nil {
				message = handlers.MessageWithHeaders(id, rs.scoped)
			}
			lifecycleErr = rs.reader.SubscribeWithHeaders(ctx, signalReady, message)
		} else {
			var message func([]byte, string, ...any)
			if handlers.Message != nil {
				message = handlers.Message(id, rs.scoped)
			}
			lifecycleErr = rs.reader.Subscribe(ctx, signalReady, message)
		}
		if readySucceeded.Load() && ctx.Err() == nil && onTerminal != nil {
			if lifecycleErr == nil {
				lifecycleErr = ErrSubscriptionEnded
			}
			onTerminal(lifecycleErr)
		}
		lifecycleDone <- lifecycleErr
		c.removeReader(id, rs)
	}()

	select {
	case err := <-readyResult:
		if err != nil {
			c.removeReader(id, rs)
			return err
		}
		return nil
	case lifecycleErr := <-lifecycleDone:
		if readySucceeded.Load() {
			return nil
		}
		c.removeReader(id, rs)
		if lifecycleErr == nil {
			return fmt.Errorf("%w before readiness", ErrSubscriptionEnded)
		}
		return lifecycleErr
	case <-c.ctx.Done():
		c.removeReader(id, rs)
		return c.ctx.Err()
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
	c.mu.Unlock()
	if rs == nil {
		return fmt.Errorf("%w: %d", ErrSubscriptionNotFound, subscriptionID)
	}
	if rs.autoSettle || !rs.profile.ManualSettlement {
		return connector.ErrOperationUnsupported
	}
	if nack && rs.profile.Settlement.Nack == connector.NackUnsupported {
		return connector.ErrOperationUnsupported
	}
	if !nack && rs.profile.Settlement.Ack == connector.AckUnsupported {
		return connector.ErrOperationUnsupported
	}
	c.pending.Add(1)

	op := c.getAckOp()
	op.core = c
	op.reader = rs
	op.handlers = handlers
	op.expected = len(messageIDs)
	if cap(op.messageIDs) < len(messageIDs) {
		op.messageIDs = make([][]byte, len(messageIDs))
		op.payloads = make([][]byte, len(messageIDs))
		op.sequences = make([]uint64, len(messageIDs))
		op.successful = make([]bool, len(messageIDs))
		op.matched = make([]bool, len(messageIDs))
	} else {
		op.messageIDs = op.messageIDs[:len(messageIDs)]
		op.payloads = op.payloads[:len(messageIDs)]
		op.sequences = op.sequences[:len(messageIDs)]
		op.successful = op.successful[:len(messageIDs)]
		op.matched = op.matched[:len(messageIDs)]
	}
	for i, id := range messageIDs {
		payload, sequence, err := decodeMessageID(id, rs.incarnation, rs.reader.MsgIDArgsLen())
		if err != nil {
			op.finished = true
			op.finish()
			return err
		}
		op.messageIDs[i] = id
		op.payloads[i] = payload
		op.sequences[i] = sequence
	}
	rs.settlementMu.Lock()
	for i, sequence := range op.sequences {
		if sequence == 0 || rs.containsSettlement(sequence) {
			rs.settlementMu.Unlock()
			op.finished = true
			op.finish()
			return fmt.Errorf("%w: message ID already settled or in progress", ErrInvalidMessageID)
		}
		for _, prior := range op.sequences[:i] {
			if prior == sequence {
				rs.settlementMu.Unlock()
				op.finished = true
				op.finish()
				return fmt.Errorf("%w: duplicate message ID in request", ErrInvalidMessageID)
			}
		}
	}
	rs.addActiveSettlement(op)
	rs.settlementMu.Unlock()
	if nack {
		rs.reader.Nack(c.ctx, op.payloads, op.done, op.each)
	} else {
		rs.reader.Ack(c.ctx, op.payloads, op.done, op.each)
	}
	return nil
}

func decodeMessageID(id []byte, incarnation uint32, adapterPrefixLen int) ([]byte, uint64, error) {
	if adapterPrefixLen < 0 || len(id) < messageIDEnvelopeLen+adapterPrefixLen || id[0] != messageIDVersion {
		return nil, 0, ErrInvalidMessageID
	}
	token := binary.BigEndian.Uint64(id[messageIDTokenOffset:messageIDEnvelopeLen])
	if uint32(token>>32) != incarnation {
		return nil, 0, fmt.Errorf("%w: stale reader incarnation", ErrInvalidMessageID)
	}
	return id[messageIDEnvelopeLen:], token, nil
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
		cleanupCtx, cancel := context.WithTimeout(context.Background(), c.cleanupTimeout)
		defer cancel()

		c.mu.Lock()
		if c.State() == StateClosed {
			c.mu.Unlock()
			return
		}
		c.state.Store(uint32(StateClosed))
		manager := c.manager
		writers := c.writers
		c.writers = make(map[string]connector.WriteCloser)
		txWriter := c.txWriter
		c.txWriter, c.txRoute = nil, ""
		readers := c.readers
		c.readers = make(map[byte]*readerState)
		c.fetchReaders = make(map[fetchKey]byte)
		c.manager = nil
		c.mu.Unlock()

		var errs []error
		if txWriter != nil {
			if err := runCleanup(cleanupCtx, txWriter.RollbackTx); err != nil {
				errs = append(errs, fmt.Errorf("rollback transaction: %w", err))
			}
			if manager != nil {
				if err := runCleanup(cleanupCtx, func(context.Context) error { return manager.DiscardWriter(txWriter) }); err != nil {
					errs = append(errs, fmt.Errorf("close transaction writer: %w", err))
				}
			}
		}
		for route, writer := range writers {
			if err := runCleanup(cleanupCtx, writer.Flush); err != nil {
				errs = append(errs, fmt.Errorf("flush writer %q: %w", route, err))
			}
			if manager != nil {
				if err := runCleanup(cleanupCtx, func(context.Context) error { return manager.DiscardWriter(writer) }); err != nil {
					errs = append(errs, fmt.Errorf("close writer %q: %w", route, err))
				}
			}
		}
		for id, reader := range readers {
			if err := runCleanup(cleanupCtx, func(context.Context) error { return reader.close() }); err != nil {
				errs = append(errs, fmt.Errorf("close reader %d: %w", id, err))
			}
			if err := c.subIDs.Put(id); err != nil {
				errs = append(errs, fmt.Errorf("return subscription id %d: %w", id, err))
			}
		}
		if err := waitGroupContext(cleanupCtx, &c.pending); err != nil {
			errs = append(errs, fmt.Errorf("wait pending operations: %w", err))
		}
		if manager != nil {
			if err := runCleanup(cleanupCtx, manager.Close); err != nil {
				errs = append(errs, fmt.Errorf("close connector manager: %w", err))
			}
		}
		c.closeErr = errors.Join(errs...)
	})
	return c.closeErr
}

func runCleanup(ctx context.Context, operation func(context.Context) error) error {
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- operation(ctx)
	}()
	<-started
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitGroupContext(ctx context.Context, group *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
