package connector

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/ValerySidorin/fujin/connector"
	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/connector/reader"
	"github.com/ValerySidorin/fujin/connector/writer"
	"github.com/ValerySidorin/fujin/internal/common/pool"
)

var (
	ErrReaderNotFound = errors.New("reader not found")
	ErrWriterNotFound = errors.New("writer not found")
)

type Manager struct {
	conf connector.Config

	readers map[string]reader.Reader
	wpoolms map[string]map[string]*pool.Pool // a map of writer pools grouped by topic and writer ID

	getReaderFuncs map[string]func(name string, autoCommit bool) (reader.Reader, error)

	cmu sync.RWMutex
	pmu sync.RWMutex

	l *slog.Logger
}

func NewManager(conf connector.Config, l *slog.Logger) *Manager {
	cman := &Manager{
		conf: conf,

		readers: make(map[string]reader.Reader, len(conf.Readers)),
		wpoolms: make(map[string]map[string]*pool.Pool, len(conf.Writers)),

		l: l,
	}

	getReaderFuncs := make(map[string]func(name string, autoCommit bool) (reader.Reader, error), len(conf.Readers))
	for name, conf := range conf.Readers {
		if conf.Reusable {
			getReaderFuncs[name] = cman.getReaderReuse
		} else {
			getReaderFuncs[name] = cman.getReaderNoReuse
		}
	}

	cman.getReaderFuncs = getReaderFuncs

	for name, c := range conf.Writers {
		rewriteConf := c
		rewriteConf.Kafka.Endpoint = strings.Join(rewriteConf.Kafka.Brokers, ",")
		rewriteConf.RedisPubSub.Endpoint = strings.Join(rewriteConf.RedisPubSub.InitAddress, ",")
		conf.Writers[name] = rewriteConf
	}

	return cman
}

func (m *Manager) GetReader(name string, autoCommit bool) (reader.Reader, error) {
	f, ok := m.getReaderFuncs[name]
	if !ok {
		return nil, fmt.Errorf("reader func not found for name: %s", name)
	}

	return f(name, autoCommit)
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
			return writer.NewWriter(conf, writerID, m.l)
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
			return writer.NewWriter(conf, writerID, m.l)
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

func (m *Manager) WriterCanBeReusedInTx(w writer.Writer, pub string) bool {
	conf := m.conf.Writers[pub]
	switch conf.Protocol {
	case protocol.Kafka:
		return conf.Kafka.Endpoint == w.Endpoint()
	case protocol.AMQP091:
		return conf.AMQP091.Conn.URL == w.Endpoint()
	}

	return false
}

func (m *Manager) getReaderReuse(name string, autoCommit bool) (reader.Reader, error) {
	m.cmu.RLock()
	r, ok := m.readers[name]
	if ok {
		m.cmu.RUnlock()
		return r, nil
	}
	m.cmu.RUnlock()

	m.cmu.Lock()
	defer m.cmu.Unlock()
	r, ok = m.readers[name]
	if ok {
		return r, nil
	}

	conf, ok := m.conf.Readers[name]
	if !ok {
		return nil, ErrReaderNotFound
	}

	r, err := reader.New(conf, autoCommit, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	return r, nil
}

func (m *Manager) getReaderNoReuse(name string, autoCommit bool) (reader.Reader, error) {
	conf, ok := m.conf.Readers[name]
	if !ok {
		return nil, ErrReaderNotFound
	}

	r, err := reader.New(conf, autoCommit, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	return r, nil
}
