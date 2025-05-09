package connector

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/ValerySidorin/fujin/connector"
	"github.com/ValerySidorin/fujin/connector/protocol"
	"github.com/ValerySidorin/fujin/internal/common/pool"
	"github.com/ValerySidorin/fujin/internal/connector/reader"
	"github.com/ValerySidorin/fujin/internal/connector/writer"
)

var (
	ErrReaderNotFound = errors.New("reader not found")
	ErrWriterNotFound = errors.New("writer not found")
)

type Manager struct {
	conf connector.Config

	readers map[string]reader.Reader
	wpoolms map[string]map[string]*pool.Pool // a map of writer pools grouped by topic and writer ID

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
	writerConf, ok := m.conf.Writers[pub]
	if !ok {
		return false
	}

	switch writerConf.Protocol {
	case protocol.Kafka:
		kafkaConfRaw, ok := writerConf.Kafka.(map[string]any)
		if !ok {
			return false
		}
		brokersRaw, ok := kafkaConfRaw["brokers"].([]any)
		if !ok {
			return false
		}
		var brokersStr []string
		for _, b := range brokersRaw {
			if brokerStr, sOk := b.(string); sOk {
				brokersStr = append(brokersStr, brokerStr)
			}
		}
		configuredEndpoint := strings.Join(brokersStr, ",")
		return configuredEndpoint == w.Endpoint()
	case protocol.AMQP091:
		amqpConfRaw, ok := writerConf.AMQP091.(map[string]any)
		if !ok {
			return false
		}
		connConf, ok := amqpConfRaw["conn"].(map[string]any)
		if !ok {
			return false
		}
		url, ok := connConf["url"].(string)
		if !ok {
			return false
		}
		return url == w.Endpoint()
	case protocol.AMQP10:
		amqp10ConfRaw, ok := writerConf.AMQP10.(map[string]any)
		if !ok {
			return false
		}
		connConf, ok := amqp10ConfRaw["conn"].(map[string]any)
		if !ok {
			return false
		}
		addr, ok := connConf["addr"].(string)
		if !ok {
			return false
		}
		return addr == w.Endpoint()
	case protocol.RedisPubSub:
		redisConfRaw, ok := writerConf.RedisPubSub.(map[string]any)
		if !ok {
			return false
		}
		initAddressRaw, ok := redisConfRaw["init_address"].([]any)
		if !ok {
			redisEndpoint, endpointOk := redisConfRaw["endpoint"].(string)
			if endpointOk {
				return redisEndpoint == w.Endpoint()
			}
			return false
		}
		var initAddressStr []string
		for _, ia := range initAddressRaw {
			if addrStr, sOk := ia.(string); sOk {
				initAddressStr = append(initAddressStr, addrStr)
			}
		}
		configuredEndpoint := strings.Join(initAddressStr, ",")
		return configuredEndpoint == w.Endpoint()
	case protocol.RedisStreams:
		redisConfRaw, ok := writerConf.RedisStreams.(map[string]any)
		if !ok {
			return false
		}
		initAddressRaw, ok := redisConfRaw["init_address"].([]any)
		if !ok {
			redisEndpoint, endpointOk := redisConfRaw["endpoint"].(string)
			if endpointOk {
				return redisEndpoint == w.Endpoint()
			}
			return false
		}
		var initAddressStr []string
		for _, ia := range initAddressRaw {
			if addrStr, sOk := ia.(string); sOk {
				initAddressStr = append(initAddressStr, addrStr)
			}
		}
		configuredEndpoint := strings.Join(initAddressStr, ",")
		return configuredEndpoint == w.Endpoint()
	}

	return false
}
