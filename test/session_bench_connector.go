package test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
)

func init() {
	if err := connector.Register("session_bench", sessionBenchDescriptor()); err != nil {
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

func newSessionBenchConnector(config any) *sessionBenchConnector {
	result := &sessionBenchConnector{msgSize: 1}
	settings, ok := config.(map[string]any)
	if !ok {
		return result
	}
	switch value := settings["msg_size"].(type) {
	case int:
		result.msgSize = value
	case float64:
		result.msgSize = int(value)
	}
	switch value := settings["subscribe_limit"].(type) {
	case int:
		result.subscribeLimit = uint64(value)
	case uint64:
		result.subscribeLimit = value
	case float64:
		result.subscribeLimit = uint64(value)
	}
	switch value := settings["subscribe_limits"].(type) {
	case chan uint64:
		result.subscribeLimits = value
	case <-chan uint64:
		result.subscribeLimits = value
	}
	switch value := settings["subscribe_start"].(type) {
	case chan struct{}:
		result.subscribeStart = value
	case <-chan struct{}:
		result.subscribeStart = value
	}
	result.ackDoneFirst, _ = settings["ack_done_first"].(bool)
	return result
}

func sessionBenchDescriptor() connector.Descriptor {
	return connector.Descriptor{
		Converter: func(_ string, value string) (any, error) { return value, nil },
		Compile: func(raw any) (connector.Compiled, error) {
			compiled := newSessionBenchConnector(raw)
			profiles := map[string]connector.RouteProfile{
				"pub": {
					Produce:          true,
					Headers:          true,
					ProduceGuarantee: connector.AcceptanceLocal,
				},
				"tx": {
					Produce:          true,
					Headers:          true,
					Transactions:     true,
					ProduceGuarantee: connector.AcceptanceLocal,
				},
				"sub": {
					Headers:          true,
					Subscribe:        true,
					Fetch:            true,
					ManualSettlement: true,
					Settlement:       connector.SettlementProfile{Ack: connector.AckSingle, Nack: connector.NackDrop},
				},
			}
			factories := map[string]connector.RouteFactory{
				"pub": {Writer: compiled.NewWriter},
				"tx":  {Writer: compiled.NewWriter},
				"sub": {Reader: compiled.NewReader},
			}
			return connector.CompileStatic(profiles, factories)
		},
	}
}

func (c *sessionBenchConnector) NewReader(autoSettle bool, _ *slog.Logger) (connector.ReadCloser, error) {
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
		autoCommit:     autoSettle,
		fetchDoneFirst: true,
		subscribeLimit: subscribeLimit,
		subscribeStart: c.subscribeStart,
		ackDoneFirst:   c.ackDoneFirst,
	}, nil
}

func (*sessionBenchConnector) NewWriter(*slog.Logger) (connector.WriteCloser, error) {
	return sessionBenchWriter{}, nil
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
func (sessionBenchWriter) WriterContractCompliant()         {}

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
