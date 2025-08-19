package connectors

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ValerySidorin/fujin/internal/common/pool"
	"github.com/ValerySidorin/fujin/internal/connectors/observability"
	"github.com/ValerySidorin/fujin/public/connectors"
	"github.com/ValerySidorin/fujin/public/connectors/reader"
	"github.com/ValerySidorin/fujin/public/connectors/writer"
)

var (
	ErrReaderNotFound = errors.New("reader not found")
	ErrWriterNotFound = errors.New("writer not found")
)

type Manager struct {
	conf connectors.Config

	readers map[string]reader.Reader
	wpoolms map[string]map[string]*pool.Pool // a map of writer pools grouped by topic and writer ID

	pmu sync.RWMutex

	l *slog.Logger
}

func NewManager(conf connectors.Config, l *slog.Logger) *Manager {
	cman := &Manager{
		conf: conf,

		readers: make(map[string]reader.Reader, len(conf.Readers)),
		wpoolms: make(map[string]map[string]*pool.Pool, len(conf.Writers)),

		l: l,
	}

	return cman
}

func (m *Manager) GetReader(name string, autoCommit bool) (reader.Reader, error) {
	conf, ok := m.conf.Readers[name]
	if !ok {
		return nil, ErrReaderNotFound
	}

	r, err := reader.New(conf, autoCommit, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	r = observability.WrapOtelReaderIfEnabled(observability.WrapMetricsReaderIfEnabled(r, name), name)
	return r, nil
}

func (m *Manager) GetWriter(name, writerID string) (writer.Writer, error) {
	m.pmu.RLock()

	wpoolm, ok := m.wpoolms[name]
	if !ok {
		m.pmu.RUnlock()
		m.pmu.Lock()
		defer m.pmu.Unlock()

		conf, ok := m.conf.Writers[name]
		if !ok {
			return nil, ErrWriterNotFound
		}

		wpoolm = make(map[string]*pool.Pool, 1)
		pool := pool.NewPool(func() (any, error) {
			w, err := writer.NewWriter(conf, writerID, m.l)
			if err != nil {
				return nil, err
			}
			w = observability.WrapOtelWriterIfEnabled(observability.WrapMetricsWriterIfEnabled(w, name), name)
			return w, nil
		})
		wpoolm[writerID] = pool
		m.wpoolms[name] = wpoolm

		w, err := pool.Get()
		if err != nil {
			return nil, fmt.Errorf("get writer: %w", err)
		}

		return w.(writer.Writer), nil
	}

	p, ok := wpoolm[writerID]
	if !ok {
		m.pmu.RUnlock()
		m.pmu.Lock()
		defer m.pmu.Unlock()

		conf, ok := m.conf.Writers[name]
		if !ok {
			return nil, ErrWriterNotFound
		}

		wpoolm = make(map[string]*pool.Pool, 1)
		pool := pool.NewPool(func() (any, error) {
			w, err := writer.NewWriter(conf, writerID, m.l)
			if err != nil {
				return nil, err
			}
			w = observability.WrapOtelWriterIfEnabled(observability.WrapMetricsWriterIfEnabled(w, name), name)
			return w, nil
		})
		wpoolm[writerID] = pool
		m.wpoolms[name] = wpoolm

		w, err := pool.Get()
		if err != nil {
			return nil, fmt.Errorf("get writer: %w", err)
		}

		return w.(writer.Writer), nil
	}
	w, err := p.Get()
	if err != nil {
		return nil, fmt.Errorf("get writer: %w", err)
	}
	m.pmu.RUnlock()

	return w.(writer.Writer), nil
}

func (m *Manager) PutWriter(w writer.Writer, name, writerID string) {
	m.pmu.Lock()
	defer m.pmu.Unlock()

	m.wpoolms[name][writerID].Put(w)
}

func (m *Manager) Close() {
	for _, wpoolm := range m.wpoolms {
		for _, p := range wpoolm {
			p.Close()
		}
	}
}

func (m *Manager) WriterMatchEndpoint(w writer.Writer, pub string) bool {
	writerConf, ok := m.conf.Writers[pub]
	if !ok {
		return false
	}
	endpoint, err := writer.ParseEndpoint(writerConf)
	if err != nil {
		m.l.Error("parse endpoint", "err", err)
		return false
	}

	return endpoint == w.Endpoint()
}
