package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	cmwconfig "github.com/fujin-io/fujin/public/plugins/middleware/connector/config"
)

const (
	defaultRuntimeCleanupTimeout = 30 * time.Second
	generationTransitionLimit    = 64
)

var generationSequence atomic.Uint64

// Catalog atomically publishes immutable connector configuration generations.
type Catalog struct {
	current atomic.Pointer[Generation]
	l       *slog.Logger

	reloadMu      sync.Mutex
	statusMu      sync.RWMutex
	draining      map[GenerationID]*Generation
	transitionSeq uint64
	status        CatalogStatus
}

// GenerationID is one stable process-local connector generation identity.
type GenerationID uint64

// GenerationState is an observable connector generation lifecycle state.
type GenerationState string

const (
	GenerationPublished GenerationState = "published"
	GenerationDraining  GenerationState = "draining"
	GenerationRetired   GenerationState = "retired"
)

// GenerationStatus is a detached readonly generation lifecycle projection.
type GenerationStatus struct {
	ID       GenerationID
	State    GenerationState
	Bindings int64
	Error    string
}

// GenerationTransition is one ordered process-local lifecycle transition.
type GenerationTransition struct {
	Sequence uint64
	GenerationStatus
}

// CatalogStatus is a detached readonly connector catalog lifecycle projection.
type CatalogStatus struct {
	Current           *GenerationStatus
	Draining          []GenerationStatus
	RetiredTotal      uint64
	RecentTransitions []GenerationTransition
}

// CompileCatalog validates every connector without broker I/O and publishes generation one.
func CompileCatalog(config connectorconfig.ConnectorsConfig, l *slog.Logger) (*Catalog, error) {
	if l == nil {
		l = slog.Default()
	}
	generation, err := CompileGeneration(config, l)
	if err != nil {
		return nil, err
	}
	catalog := &Catalog{l: l, draining: make(map[GenerationID]*Generation)}
	catalog.publish(generation)
	return catalog, nil
}

// Current returns the generation visible to new BIND operations.
func (c *Catalog) Current() *Generation {
	if c == nil {
		return nil
	}
	return c.current.Load()
}

// Status returns a detached readonly connector generation lifecycle projection.
func (c *Catalog) Status() CatalogStatus {
	if c == nil {
		return CatalogStatus{}
	}
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	status := CatalogStatus{
		RetiredTotal:      c.status.RetiredTotal,
		RecentTransitions: append([]GenerationTransition(nil), c.status.RecentTransitions...),
	}
	if current := c.current.Load(); current != nil {
		currentStatus := current.status(GenerationPublished)
		status.Current = &currentStatus
	}
	status.Draining = make([]GenerationStatus, 0, len(c.draining))
	for _, generation := range c.draining {
		status.Draining = append(status.Draining, generation.status(GenerationDraining))
	}
	sort.Slice(status.Draining, func(i, j int) bool { return status.Draining[i].ID < status.Draining[j].ID })
	return status
}

// Reload compiles a complete replacement before atomically publishing it.
func (c *Catalog) Reload(config connectorconfig.ConnectorsConfig) error {
	c.reloadMu.Lock()
	defer c.reloadMu.Unlock()
	next, err := CompileGeneration(config, c.l)
	if err != nil {
		return err
	}
	next.onClosed = c.generationClosed
	previous := c.current.Swap(next)
	c.recordPublished(next)
	if previous != nil {
		c.retire(previous)
	}
	return nil
}

// Close retires the current generation and waits for generation-owned cleanup.
func (c *Catalog) Close(ctx context.Context) error {
	c.reloadMu.Lock()
	generation := c.current.Swap(nil)
	if generation != nil {
		c.retire(generation)
	}
	c.reloadMu.Unlock()
	if generation == nil {
		return nil
	}
	return generation.WaitClosed(ctx)
}

func (c *Catalog) publish(generation *Generation) {
	generation.onClosed = c.generationClosed
	c.current.Store(generation)
	c.recordPublished(generation)
}

func (c *Catalog) recordPublished(generation *Generation) {
	status := generation.status(GenerationPublished)
	c.statusMu.Lock()
	c.appendTransitionLocked(status)
	c.statusMu.Unlock()
	c.l.Info("connector generation published", "generation_id", status.ID)
}

func (c *Catalog) retire(generation *Generation) {
	status := generation.status(GenerationDraining)
	c.statusMu.Lock()
	c.draining[generation.id] = generation
	c.appendTransitionLocked(status)
	c.statusMu.Unlock()
	c.l.Info("connector generation draining", "generation_id", status.ID, "bindings", status.Bindings)
	generation.Retire()
}

func (c *Catalog) generationClosed(generation *Generation) {
	status := generation.status(GenerationRetired)
	c.statusMu.Lock()
	delete(c.draining, generation.id)
	c.status.RetiredTotal++
	c.appendTransitionLocked(status)
	c.statusMu.Unlock()
	c.l.Info("connector generation retired", "generation_id", status.ID, "error", status.Error)
}

func (c *Catalog) appendTransitionLocked(status GenerationStatus) {
	c.transitionSeq++
	c.status.RecentTransitions = append(c.status.RecentTransitions, GenerationTransition{
		Sequence:         c.transitionSeq,
		GenerationStatus: status,
	})
	if excess := len(c.status.RecentTransitions) - generationTransitionLimit; excess > 0 {
		copy(c.status.RecentTransitions, c.status.RecentTransitions[excess:])
		c.status.RecentTransitions = c.status.RecentTransitions[:generationTransitionLimit]
	}
}

// Generation is an immutable compiled connector snapshot.
type Generation struct {
	id         GenerationID
	connectors map[string]*compiledConnector
	l          *slog.Logger

	refs      atomic.Int64
	retired   atomic.Bool
	closeOnce sync.Once
	closed    chan struct{}
	closeMu   sync.Mutex
	closeErr  error
	onClosed  func(*Generation)
}

type compiledConnector struct {
	name        string
	config      connectorconfig.ConnectorConfig
	compiled    Compiled
	profiles    map[string]RouteProfile
	middlewares []cmwconfig.Config

	runtimeMu sync.Mutex
	runtime   Runtime
}

// CompileGeneration decodes and validates all connector settings and route profiles.
func CompileGeneration(configs connectorconfig.ConnectorsConfig, l *slog.Logger) (*Generation, error) {
	if l == nil {
		l = slog.Default()
	}
	generation := &Generation{
		id:         GenerationID(generationSequence.Add(1)),
		connectors: make(map[string]*compiledConnector, len(configs)),
		l:          l,
		closed:     make(chan struct{}),
	}
	for name, config := range configs {
		immutableConfig, err := connectorconfig.CloneConnectorConfig(config)
		if err != nil {
			return nil, fmt.Errorf("connector %q: clone config: %w", name, err)
		}
		descriptor, ok := Get(immutableConfig.Type)
		if !ok {
			return nil, fmt.Errorf("connector %q: unsupported protocol %q (is it compiled in?)", name, immutableConfig.Type)
		}
		compiled, err := descriptor.Compile(immutableConfig.Settings)
		if err != nil {
			return nil, fmt.Errorf("connector %q: compile: %w", name, err)
		}
		declaredProfiles := compiled.Routes()
		if len(declaredProfiles) == 0 {
			return nil, fmt.Errorf("connector %q: no routes", name)
		}
		profiles := make(map[string]RouteProfile, len(declaredProfiles))
		for route, profile := range declaredProfiles {
			if route == "" {
				return nil, fmt.Errorf("connector %q: route name is empty", name)
			}
			if err := profile.Validate(route); err != nil {
				return nil, fmt.Errorf("connector %q: %w", name, err)
			}
			profiles[route] = profile
		}
		generation.connectors[name] = &compiledConnector{
			name:        name,
			config:      immutableConfig,
			compiled:    compiled,
			profiles:    profiles,
			middlewares: immutableConfig.ConnectorMiddlewares,
		}
	}
	return generation, nil
}

// ID returns the stable process-local identity assigned during compilation.
func (g *Generation) ID() GenerationID {
	if g == nil {
		return 0
	}
	return g.id
}

func (g *Generation) status(state GenerationState) GenerationStatus {
	status := GenerationStatus{ID: g.id, State: state, Bindings: g.refs.Load()}
	if state == GenerationRetired {
		g.closeMu.Lock()
		if g.closeErr != nil {
			status.Error = g.closeErr.Error()
		}
		g.closeMu.Unlock()
	}
	return status
}

// Config returns a caller-owned copy of one connector's immutable raw configuration.
func (g *Generation) Config(name string) (connectorconfig.ConnectorConfig, bool) {
	if g == nil {
		return connectorconfig.ConnectorConfig{}, false
	}
	compiled := g.connectors[name]
	if compiled == nil {
		return connectorconfig.ConnectorConfig{}, false
	}
	config, err := connectorconfig.CloneConnectorConfig(compiled.config)
	if err != nil {
		return connectorconfig.ConnectorConfig{}, false
	}
	return config, true
}

// Acquire pins this generation and returns a session-scoped connector binding.
func (g *Generation) Acquire(name string) (*Binding, error) {
	if g == nil {
		return nil, errors.New("connector generation unavailable")
	}
	compiled := g.connectors[name]
	if compiled == nil {
		return nil, fmt.Errorf("connector %q not found", name)
	}
	if g.retired.Load() {
		return nil, errors.New("connector generation retired")
	}
	g.refs.Add(1)
	if g.retired.Load() {
		g.release()
		return nil, errors.New("connector generation retired")
	}
	return &Binding{generation: g, compiled: compiled}, nil
}

// Retire prevents new bindings and starts cleanup after the final binding closes.
func (g *Generation) Retire() {
	if g != nil && g.retired.CompareAndSwap(false, true) && g.refs.Load() == 0 {
		g.startClose()
	}
}

// WaitClosed waits for a retired generation's runtime cleanup.
func (g *Generation) WaitClosed(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.closed:
		g.closeMu.Lock()
		defer g.closeMu.Unlock()
		return g.closeErr
	}
}

func (g *Generation) release() {
	if g.refs.Add(-1) == 0 && g.retired.Load() {
		g.startClose()
	}
}

func (g *Generation) startClose() {
	g.closeOnce.Do(func() { go g.finishClose() })
}

func (g *Generation) finishClose() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRuntimeCleanupTimeout)
	defer cancel()
	var errs []error
	for name, compiled := range g.connectors {
		compiled.runtimeMu.Lock()
		runtime := compiled.runtime
		compiled.runtime = nil
		compiled.runtimeMu.Unlock()
		if runtime != nil {
			result := make(chan error, 1)
			go func() { result <- runtime.Close(ctx) }()
			select {
			case err := <-result:
				if err != nil {
					errs = append(errs, fmt.Errorf("close connector runtime %q: %w", name, err))
				}
			case <-ctx.Done():
				errs = append(errs, fmt.Errorf("close connector runtime %q: %w", name, ctx.Err()))
			}
		}
	}
	g.closeMu.Lock()
	g.closeErr = errors.Join(errs...)
	g.closeMu.Unlock()
	close(g.closed)
	if g.onClosed != nil {
		g.onClosed(g)
	}
}

func (c *compiledConnector) openRuntime(l *slog.Logger) (Runtime, error) {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	if c.runtime != nil {
		return c.runtime, nil
	}
	runtime, err := c.compiled.OpenRuntime(l)
	if err != nil {
		return nil, err
	}
	c.runtime = runtime
	return runtime, nil
}

// Binding is a session-scoped lease over one connector in a generation.
type Binding struct {
	generation *Generation
	compiled   *compiledConnector
	closeOnce  sync.Once
}

func (b *Binding) Name() string { return b.compiled.name }

func (b *Binding) RouteProfile(route string) (RouteProfile, error) {
	profile, ok := b.compiled.profiles[route]
	if !ok {
		return RouteProfile{}, fmt.Errorf("%w: %q", ErrRouteNotFound, route)
	}
	return profile, nil
}

// RouteProfiles returns a caller-owned copy of every route contract in this binding.
func (b *Binding) RouteProfiles() map[string]RouteProfile {
	profiles := make(map[string]RouteProfile, len(b.compiled.profiles))
	for route, profile := range b.compiled.profiles {
		profiles[route] = profile
	}
	return profiles
}

func (b *Binding) Middlewares() []cmwconfig.Config {
	config, err := connectorconfig.CloneConnectorConfig(b.compiled.config)
	if err != nil {
		return nil
	}
	return config.ConnectorMiddlewares
}

func (b *Binding) NewReader(route string, autoSettle bool, l *slog.Logger) (ReadCloser, error) {
	runtime, err := b.compiled.openRuntime(l)
	if err != nil {
		return nil, err
	}
	return runtime.NewReader(route, autoSettle, l)
}

func (b *Binding) NewWriter(route string, l *slog.Logger) (WriteCloser, error) {
	runtime, err := b.compiled.openRuntime(l)
	if err != nil {
		return nil, err
	}
	return runtime.NewWriter(route, l)
}

// Close releases the generation pin. Generation-owned cleanup is asynchronous.
func (b *Binding) Close() {
	if b != nil {
		b.closeOnce.Do(b.generation.release)
	}
}
