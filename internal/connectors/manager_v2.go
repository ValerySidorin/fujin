package connectors

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/internal/common/pool"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

type ManagerV2 struct {
	conf          connectorconfig.ConnectorConfig
	connectorName string
	routePools    map[string]*pool.Pool
	mu            sync.RWMutex
	l             *slog.Logger
}

func NewManagerV2(conf connectorconfig.ConnectorConfig, connectorName string, l *slog.Logger) *ManagerV2 {
	return &ManagerV2{
		conf:          conf,
		connectorName: connectorName,
		routePools:    make(map[string]*pool.Pool),
		l:             l,
	}
}

func (m *ManagerV2) GetReader(route string, autoCommit bool) (connector.ReadCloser, error) {
	r, err := connector.NewReader(m.conf, route, autoCommit, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	// Apply connector middlewares.
	if len(m.conf.ConnectorMiddlewares) > 0 {
		wrapped, err := cmw.ChainReader(r, m.connectorName, m.conf.ConnectorMiddlewares, m.l)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("apply connector middlewares: %w", err)
		}
		return &decoratedReadCloser{Reader: wrapped, closer: r}, nil
	}

	return r, nil
}

// decoratedReadCloser wraps a decorated reader with the original closer
type decoratedReadCloser struct {
	connector.Reader
	closer connector.ReadCloser
}

func (d *decoratedReadCloser) Close() error {
	return d.closer.Close()
}

func (m *ManagerV2) GetWriter(route string) (connector.WriteCloser, error) {
	m.mu.RLock()
	p, ok := m.routePools[route]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		defer m.mu.Unlock()

		// Double-check after acquiring write lock.
		if p, ok = m.routePools[route]; ok {
			w, err := p.Get()
			if err != nil {
				return nil, fmt.Errorf("get writer: %w", err)
			}
			return w.(connector.WriteCloser), nil
		}

		p = pool.NewPool(func() (any, error) {
			w, err := connector.NewWriter(m.conf, route, m.l)
			if err != nil {
				return nil, err
			}

			// Apply connector middlewares.
			if len(m.conf.ConnectorMiddlewares) > 0 {
				wrapped, err := cmw.ChainWriter(w, m.connectorName, m.conf.ConnectorMiddlewares, m.l)
				if err != nil {
					w.Close()
					return nil, fmt.Errorf("apply connector middlewares: %w", err)
				}
				return &decoratedWriteCloser{Writer: wrapped, closer: w}, nil
			}

			return w, nil
		})
		m.routePools[route] = p
	}

	w, err := p.Get()
	if err != nil {
		return nil, fmt.Errorf("get writer: %w", err)
	}

	return w.(connector.WriteCloser), nil
}

// decoratedWriteCloser wraps a decorated writer with the original closer
type decoratedWriteCloser struct {
	connector.Writer
	closer connector.WriteCloser
}

func (d *decoratedWriteCloser) Close() error {
	return d.closer.Close()
}

func (m *ManagerV2) PutWriter(w connector.WriteCloser, route string) {
	m.mu.RLock()
	p, ok := m.routePools[route]
	m.mu.RUnlock()

	if !ok {
		w.Close()
		return
	}

	p.Put(w)
}

func (m *ManagerV2) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.routePools {
		p.Close()
	}
}
