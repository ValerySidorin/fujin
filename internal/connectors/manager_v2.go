package connectors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

const writerPoolSize = 64

// ManagerV2 is a session-scoped lease over one connector in an immutable generation.
type ManagerV2 struct {
	binding *connector.Binding
	pools   map[string]*writerPool
	mu      sync.RWMutex
	l       *slog.Logger

	closeOnce sync.Once
	closeErr  error
}

type writerPool struct {
	ch chan connector.WriteCloser
}

func newWriterPool() *writerPool {
	return &writerPool{ch: make(chan connector.WriteCloser, writerPoolSize)}
}

func NewManagerV2(binding *connector.Binding, l *slog.Logger) *ManagerV2 {
	return &ManagerV2{binding: binding, pools: make(map[string]*writerPool), l: l}
}

func (m *ManagerV2) RouteProfile(route string) (connector.RouteProfile, error) {
	return m.binding.RouteProfile(route)
}

func (m *ManagerV2) RouteProfiles() map[string]connector.RouteProfile {
	return m.binding.RouteProfiles()
}

func (m *ManagerV2) GetReader(route string, autoSettle bool) (connector.ReadCloser, error) {
	profile, err := m.RouteProfile(route)
	if err != nil {
		return nil, err
	}
	if !profile.Subscribe && !profile.Fetch {
		return nil, fmt.Errorf("%w: route %q has no read capability", connector.ErrOperationUnsupported, route)
	}
	if !autoSettle && !profile.ManualSettlement {
		return nil, fmt.Errorf("%w: route %q has no manual settlement", connector.ErrOperationUnsupported, route)
	}
	r, err := m.binding.NewReader(route, autoSettle, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}
	middlewares := m.binding.Middlewares()
	if len(middlewares) == 0 {
		return r, nil
	}
	wrapped, err := cmw.ChainReader(r, m.binding.Name(), middlewares, m.l)
	if err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("apply connector middlewares: %w", err)
	}
	return wrapped, nil
}

func (m *ManagerV2) GetWriter(route string) (connector.WriteCloser, error) {
	profile, err := m.RouteProfile(route)
	if err != nil {
		return nil, err
	}
	if !profile.Produce {
		return nil, fmt.Errorf("%w: route %q has no produce capability", connector.ErrOperationUnsupported, route)
	}
	pool := m.pool(route)
	select {
	case writer := <-pool.ch:
		return writer, nil
	default:
	}
	writer, err := m.binding.NewWriter(route, m.l)
	if err != nil {
		return nil, fmt.Errorf("new writer: %w", err)
	}
	middlewares := m.binding.Middlewares()
	if len(middlewares) > 0 {
		writer, err = cmw.ChainWriter(writer, m.binding.Name(), middlewares, m.l)
		if err != nil {
			_ = writer.Close()
			return nil, fmt.Errorf("apply connector middlewares: %w", err)
		}
	}
	return connector.EnforceWriterContract(writer), nil
}

func (m *ManagerV2) pool(route string) *writerPool {
	m.mu.RLock()
	pool := m.pools[route]
	m.mu.RUnlock()
	if pool != nil {
		return pool
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if pool = m.pools[route]; pool == nil {
		pool = newWriterPool()
		m.pools[route] = pool
	}
	return pool
}

func (m *ManagerV2) PutWriter(writer connector.WriteCloser, route string) error {
	if writer == nil {
		return nil
	}
	m.mu.RLock()
	pool := m.pools[route]
	m.mu.RUnlock()
	if pool == nil {
		return writer.Close()
	}
	select {
	case pool.ch <- writer:
		return nil
	default:
		return writer.Close()
	}
}

// DiscardWriter removes a poisoned lease from reuse.
func (m *ManagerV2) DiscardWriter(writer connector.WriteCloser) error {
	if writer == nil {
		return nil
	}
	return writer.Close()
}

// Close releases pooled session leases and the generation reference.
func (m *ManagerV2) Close(ctx context.Context) error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		pools := m.pools
		m.pools = nil
		m.mu.Unlock()

		var writers []connector.WriteCloser
		for _, pool := range pools {
			for {
				select {
				case writer := <-pool.ch:
					writers = append(writers, writer)
				default:
					goto drained
				}
			}
		drained:
		}
		m.closeErr = closeWriters(ctx, writers)
		m.binding.Close()
	})
	return m.closeErr
}

func closeWriters(ctx context.Context, writers []connector.WriteCloser) error {
	if len(writers) == 0 {
		return nil
	}
	results := make(chan error, len(writers))
	for _, writer := range writers {
		go func() { results <- writer.Close() }()
	}
	var errs []error
	for range writers {
		select {
		case err := <-results:
			if err != nil {
				errs = append(errs, err)
			}
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		}
	}
	return errors.Join(errs...)
}
