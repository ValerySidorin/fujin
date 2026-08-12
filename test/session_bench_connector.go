package test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
)

func init() {
	if err := connector.Register("session_bench", newSessionBenchConnector); err != nil {
		panic(fmt.Sprintf("register session benchmark connector: %v", err))
	}
}

type sessionBenchConnector struct {
	msgSize         int
	subscribeLimit  uint64
	subscribeLimits <-chan uint64
	subscribeStart  <-chan struct{}
	ackDoneFirst    bool
}

func newSessionBenchConnector(config any, _ *slog.Logger) (connector.Connector, error) {
	msgSize := 1
	var subscribeLimit uint64
	var subscribeLimits <-chan uint64
	var subscribeStart <-chan struct{}
	var ackDoneFirst bool
	if settings, ok := config.(map[string]any); ok {
		switch value := settings["msg_size"].(type) {
		case int:
			msgSize = value
		case float64:
			msgSize = int(value)
		}
		switch value := settings["subscribe_limit"].(type) {
		case int:
			subscribeLimit = uint64(value)
		case uint64:
			subscribeLimit = value
		case float64:
			subscribeLimit = uint64(value)
		}
		switch value := settings["subscribe_limits"].(type) {
		case chan uint64:
			subscribeLimits = value
		case <-chan uint64:
			subscribeLimits = value
		}
		switch value := settings["subscribe_start"].(type) {
		case chan struct{}:
			subscribeStart = value
		case <-chan struct{}:
			subscribeStart = value
		}
		if value, ok := settings["ack_done_first"].(bool); ok {
			ackDoneFirst = value
		}
	}
	return &sessionBenchConnector{
		msgSize:         msgSize,
		subscribeLimit:  subscribeLimit,
		subscribeLimits: subscribeLimits,
		subscribeStart:  subscribeStart,
		ackDoneFirst:    ackDoneFirst,
	}, nil
}

func (c *sessionBenchConnector) NewReader(_ any, _ string, autoCommit bool, _ *slog.Logger) (connector.ReadCloser, error) {
	subscribeLimit := c.subscribeLimit
	if c.subscribeLimits != nil {
		var ok bool
		subscribeLimit, ok = <-c.subscribeLimits
		if !ok {
			return nil, fmt.Errorf("session benchmark subscription limit exhausted")
		}
	}
	return &genReader{
		msg:            sizedBytes(c.msgSize),
		headers:        [][]byte{[]byte("content-type"), []byte("application/octet-stream")},
		autoCommit:     autoCommit,
		fetchDoneFirst: true,
		subscribeLimit: subscribeLimit,
		subscribeStart: c.subscribeStart,
		ackDoneFirst:   c.ackDoneFirst,
	}, nil
}

func (*sessionBenchConnector) NewWriter(any, string, *slog.Logger) (connector.WriteCloser, error) {
	return sessionBenchWriter{}, nil
}

func (*sessionBenchConnector) GetConfigValueConverter() connector.ConfigValueConverterFunc {
	return func(_ string, value string) (any, error) { return value, nil }
}

type sessionBenchWriter struct{}

func (sessionBenchWriter) Produce(_ context.Context, _ []byte, callback func(error)) {
	callback(nil)
}
func (sessionBenchWriter) HProduce(_ context.Context, _ []byte, _ [][]byte, callback func(error)) {
	callback(nil)
}
func (sessionBenchWriter) Flush(context.Context) error      { return nil }
func (sessionBenchWriter) BeginTx(context.Context) error    { return nil }
func (sessionBenchWriter) CommitTx(context.Context) error   { return nil }
func (sessionBenchWriter) RollbackTx(context.Context) error { return nil }
func (sessionBenchWriter) Close() error                     { return nil }

func MakeSessionBenchConfig(msgSize int) connectorconfig.ConnectorsConfig {
	return connectorconfig.ConnectorsConfig{
		"connector": {
			Type:     "session_bench",
			Settings: map[string]any{"msg_size": msgSize},
		},
	}
}

func MakeSessionBenchConfigWithSubscribeLimit(msgSize, limit int) connectorconfig.ConnectorsConfig {
	return makeSessionBenchConfig(msgSize, []int{limit}, false, nil)
}

func makeSessionBenchConfig(msgSize int, limits []int, ackDoneFirst bool, subscribeStart <-chan struct{}) connectorconfig.ConnectorsConfig {
	config := MakeSessionBenchConfig(msgSize)
	settings := config["connector"].Settings.(map[string]any)
	if len(limits) > 0 {
		subscribeLimits := make(chan uint64, len(limits))
		for _, limit := range limits {
			subscribeLimits <- uint64(limit)
		}
		settings["subscribe_limits"] = subscribeLimits
	}
	settings["ack_done_first"] = ackDoneFirst
	if subscribeStart != nil {
		settings["subscribe_start"] = subscribeStart
	}
	return config
}
