package mq

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/ValerySidorin/fujin/internal/common/pool"
	"github.com/ValerySidorin/fujin/internal/mq/reader"
	"github.com/ValerySidorin/fujin/internal/mq/writer"
)

var (
	ErrReaderNotFound = errors.New("reader not found")
	ErrWriterNotFound = errors.New("writer not found")
)

type MQManager struct {
	conf Config

	readers map[string]reader.Reader
	wpoolms map[string]map[string]*pool.Pool // a map of writer pools grouped by pub and producer ID

	getReaderFuncs map[string]func(name string) (reader.Reader, error)

	cmu sync.RWMutex
	pmu sync.RWMutex

	l *slog.Logger
}

func NewMQManager(conf Config, l *slog.Logger) *MQManager {
	mqman := &MQManager{
		conf: conf,

		readers: make(map[string]reader.Reader, len(conf.Readers)),
		wpoolms: make(map[string]map[string]*pool.Pool, len(conf.Writers)),

		l: l,
	}

	getReaderFuncs := make(map[string]func(name string) (reader.Reader, error), len(conf.Readers))
	for name, conf := range conf.Readers {
		if conf.Reusable {
			getReaderFuncs[name] = mqman.getReaderReuse
		} else {
			getReaderFuncs[name] = mqman.getReaderNoReuse
		}
	}

	mqman.getReaderFuncs = getReaderFuncs

	for name, c := range conf.Writers {
		rewriteConf := c
		rewriteConf.Kafka.Endpoint = strings.Join(rewriteConf.Kafka.Brokers, ",")
		conf.Writers[name] = rewriteConf
	}

	return mqman
}

func (m *MQManager) GetReader(name string) (reader.Reader, error) {
	f, ok := m.getReaderFuncs[name]
	if !ok {
		return nil, fmt.Errorf("reader func not found for name: %s", name)
	}

	return f(name)
}

func (m *MQManager) GetWriter(name, producerID string) (writer.Writer, error) {
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
			return writer.NewWriter(conf, producerID, m.l)
		})
		wpoolm[producerID] = pool
		m.wpoolms[name] = wpoolm

		w, err := pool.Get()
		if err != nil {
			return nil, fmt.Errorf("get writer: %w", err)
		}

		return w.(writer.Writer), nil
	}

	p, ok := wpoolm[producerID]
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
			return writer.NewWriter(conf, producerID, m.l)
		})
		wpoolm[producerID] = pool
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

func (m *MQManager) PutWriter(w writer.Writer, name, producerID string) {
	m.pmu.Lock()
	defer m.pmu.Unlock()

	m.wpoolms[name][producerID].Put(w)
}

func (m *MQManager) Close() {
	for _, wpoolm := range m.wpoolms {
		for _, p := range wpoolm {
			p.Close()
		}
	}
}

func (m *MQManager) WriterCanBeReusedInTx(w writer.Writer, pub string) bool {
	conf := m.conf.Writers[pub]
	switch conf.Protocol {
	case "kafka":
		return conf.Kafka.Endpoint == w.Endpoint()
	}

	return false
}

func (m *MQManager) getReaderReuse(name string) (reader.Reader, error) {
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

	r, err := reader.New(conf, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	return r, nil
}

func (m *MQManager) getReaderNoReuse(name string) (reader.Reader, error) {
	conf, ok := m.conf.Readers[name]
	if !ok {
		return nil, ErrReaderNotFound
	}

	r, err := reader.New(conf, m.l)
	if err != nil {
		return nil, fmt.Errorf("new reader: %w", err)
	}

	return r, nil
}
