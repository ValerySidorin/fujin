// Package prom provides a Prometheus metrics connector for connectors.
// Import this package to enable metrics collection:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/middleware/connector/prom"
//
// Configure in YAML:
//
//	connector_middlewares:
//	  - name: prom
//	    config:
//	      enabled: true
//	      addr: ":9090"      # HTTP server address for /metrics endpoint
//	      path: "/metrics"   # Metrics endpoint path
//
// Only one http server will be started to serve metrics.
package prom

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmv "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

var (
	once sync.Once

	opsTotal                 *prometheus.CounterVec
	errorsTotal              *prometheus.CounterVec
	connectorWriteLatencySec *prometheus.HistogramVec

	httpSrv          *http.Server
	httpSrvOnce      sync.Once
	httpSrvMu        sync.Mutex
	httpShutdownOnce sync.Once
)

// Config for prom connector middleware
type Config struct {
	// Enabled allows enabling/disabling the middleware. nil = true (enabled by default)
	Enabled *bool  `yaml:"enabled,omitempty"`
	Addr    string `yaml:"addr"` // HTTP server address (e.g., ":9090")
	Path    string `yaml:"path"` // Metrics endpoint path (default: "/metrics")
}

func init() {
	if err := cmv.Register("prom", newPromMiddleware); err != nil {
		panic(fmt.Sprintf("register prom connector middleware: %v", err))
	}
}

func newPromMiddleware(config any, l *slog.Logger) (cmv.Middleware, error) {
	cfg := Config{
		Addr: ":9090",    // default address
		Path: "/metrics", // default path
	}

	// Parse config if provided
	if m, ok := config.(map[string]any); ok {
		if enabled, exists := m["enabled"]; exists {
			if v, ok := enabled.(bool); ok {
				cfg.Enabled = &v
			}
		}
		if addr, exists := m["addr"]; exists {
			if v, ok := addr.(string); ok {
				cfg.Addr = v
			}
		}
		if path, exists := m["path"]; exists {
			if v, ok := path.(string); ok {
				cfg.Path = v
			}
		}
	}

	// nil or true = enabled (default)
	enabled := cfg.Enabled == nil || *cfg.Enabled
	if enabled {
		initProm()
		initHTTPServer(cfg.Addr, cfg.Path, l)
	}

	return &promMiddleware{enabled: enabled, l: l}, nil
}

func initProm() {
	once.Do(func() {
		opsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fujin_ops_total",
			Help: "Number of protocol operations",
		}, []string{"opcode", "connector"})

		errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fujin_errors_total",
			Help: "Errors by stage and connector",
		}, []string{"stage", "connector"})

		connectorWriteLatencySec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fujin_connector_produce_latency_seconds",
			Help:    "Connector produce latency",
			Buckets: prometheus.DefBuckets,
		}, []string{"connector"})

		prometheus.MustRegister(opsTotal, errorsTotal, connectorWriteLatencySec)
	})
}

// initHTTPServer starts the HTTP server for metrics endpoint (globally, once)
func initHTTPServer(addr, path string, l *slog.Logger) {
	httpSrvOnce.Do(func() {
		if path == "" {
			path = "/metrics"
		}
		mux := http.NewServeMux()
		mux.Handle(path, promhttp.Handler())

		httpSrvMu.Lock()
		httpSrv = &http.Server{Addr: addr, Handler: mux}
		httpSrvMu.Unlock()

		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				l.Error("metrics http server", "err", err)
			}
		}()
		l.Info("metrics server started", "addr", addr, "path", path)
	})
}

// Init initializes the metrics HTTP server globally.
// This should be called once at server startup.
// Config can be passed from YAML observability.metrics section.
func Init(ctx context.Context, addr, path string, l *slog.Logger) error {
	if path == "" {
		path = "/metrics"
	}
	initProm()
	initHTTPServer(addr, path, l)
	return nil
}

// Shutdown gracefully shuts down the metrics HTTP server
func Shutdown(ctx context.Context) error {
	var err error
	httpShutdownOnce.Do(func() {
		httpSrvMu.Lock()
		srv := httpSrv
		httpSrvMu.Unlock()
		if srv != nil {
			err = srv.Shutdown(ctx)
		}
	})
	return err
}

// IncOp increments the operations counter (exported for compatibility with v1 connectors)
func IncOp(opcode, connector string) {
	if opsTotal != nil {
		opsTotal.WithLabelValues(opcode, connector).Inc()
	}
}

// IncError increments the errors counter (exported for compatibility with v1 connectors)
func IncError(stage, connector string) {
	if errorsTotal != nil {
		errorsTotal.WithLabelValues(stage, connector).Inc()
	}
}

// ObserveProduceLatency observes the produce latency (exported for compatibility with v1 connectors)
func ObserveProduceLatency(connector string, d time.Duration) {
	if connectorWriteLatencySec != nil {
		connectorWriteLatencySec.WithLabelValues(connector).Observe(d.Seconds())
	}
}

// promMiddleware implements connector.Middleware
type promMiddleware struct {
	enabled bool
	l       *slog.Logger
}

func (d *promMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	if !d.enabled {
		return w
	}
	return &promWriterWrapper{w: w, connectorName: connectorName}
}

func (d *promMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	if !d.enabled {
		return r
	}
	return &promReaderWrapper{r: r, connectorName: connectorName}
}

// Writer wrapper

type promWriterWrapper struct {
	w             connector.WriteCloser
	connectorName string
}

func (d *promWriterWrapper) Produce(ctx context.Context, msg []byte, callback func(err error)) {
	start := time.Now()
	d.w.Produce(ctx, msg, func(err error) {
		IncOp("produce", d.connectorName)
		if err != nil {
			IncError("produce", d.connectorName)
		}
		ObserveProduceLatency(d.connectorName, time.Since(start))
		if callback != nil {
			callback(err)
		}
	})
}

func (d *promWriterWrapper) HProduce(ctx context.Context, msg []byte, headers [][]byte, callback func(err error)) {
	start := time.Now()
	d.w.HProduce(ctx, msg, headers, func(err error) {
		IncOp("hproduce", d.connectorName)
		if err != nil {
			IncError("hproduce", d.connectorName)
		}
		ObserveProduceLatency(d.connectorName, time.Since(start))
		if callback != nil {
			callback(err)
		}
	})
}

func (d *promWriterWrapper) Flush(ctx context.Context) error {
	err := d.w.Flush(ctx)
	if err != nil {
		IncError("flush", d.connectorName)
	}
	return err
}

func (d *promWriterWrapper) BeginTx(ctx context.Context) error {
	IncOp("begin_tx", d.connectorName)
	err := d.w.BeginTx(ctx)
	if err != nil {
		IncError("begin_tx", d.connectorName)
	}
	return err
}

func (d *promWriterWrapper) CommitTx(ctx context.Context) error {
	IncOp("commit_tx", d.connectorName)
	err := d.w.CommitTx(ctx)
	if err != nil {
		IncError("commit_tx", d.connectorName)
	}
	return err
}

func (d *promWriterWrapper) RollbackTx(ctx context.Context) error {
	IncOp("rollback_tx", d.connectorName)
	err := d.w.RollbackTx(ctx)
	if err != nil {
		IncError("rollback_tx", d.connectorName)
	}
	return err
}

func (d *promWriterWrapper) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = Shutdown(ctx)
	return d.w.Close()
}

// Reader wrapper

type promReaderWrapper struct {
	r             connector.ReadCloser
	connectorName string
}

func (d *promReaderWrapper) Subscribe(ctx context.Context, ready func() error, h func(message []byte, topic string, args ...any)) error {
	IncOp("subscribe", d.connectorName)
	return d.r.Subscribe(
		ctx,
		ready,
		func(message []byte, topic string, args ...any) {
			IncOp("msg", d.connectorName)
			h(message, topic, args...)
		},
	)
}

func (d *promReaderWrapper) SubscribeWithHeaders(ctx context.Context, ready func() error, h func(message []byte, topic string, hs [][]byte, args ...any)) error {
	IncOp("hsubscribe", d.connectorName)
	return d.r.SubscribeWithHeaders(
		ctx,
		ready,
		func(message []byte, topic string, hs [][]byte, args ...any) {
			IncOp("hmsg", d.connectorName)
			h(message, topic, hs, args...)
		},
	)
}

func (d *promReaderWrapper) Fetch(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, args ...any)) {
	frh := func(n uint32, err error) {
		IncOp("fetch", d.connectorName)
		if err != nil {
			IncError("fetch", d.connectorName)
		}
		fetchResponseHandler(n, err)
	}
	d.r.Fetch(ctx, n, frh, msgHandler)
}

func (d *promReaderWrapper) FetchWithHeaders(ctx context.Context, n uint32, fetchResponseHandler func(n uint32, err error), msgHandler func(message []byte, topic string, hs [][]byte, args ...any)) {
	frh := func(n uint32, err error) {
		IncOp("hfetch", d.connectorName)
		if err != nil {
			IncError("hfetch", d.connectorName)
		}
		fetchResponseHandler(n, err)
	}
	d.r.FetchWithHeaders(ctx, n, frh, msgHandler)
}

func (d *promReaderWrapper) Ack(ctx context.Context, msgIDs [][]byte, ackHandler func(error), ackMsgHandler func([]byte, error)) {
	d.r.Ack(
		ctx, msgIDs,
		func(err error) {
			IncOp("ack", d.connectorName)
			if err != nil {
				IncError("ack", d.connectorName)
			}
			ackHandler(err)
		},
		func(b []byte, err error) {
			if err != nil {
				IncError("ack_msg", d.connectorName)
			}
			ackMsgHandler(b, err)
		},
	)
}

func (d *promReaderWrapper) Nack(ctx context.Context, msgIDs [][]byte, nackHandler func(error), nackMsgHandler func([]byte, error)) {
	d.r.Nack(
		ctx, msgIDs,
		func(err error) {
			IncOp("nack", d.connectorName)
			if err != nil {
				IncError("nack", d.connectorName)
			}
			nackHandler(err)
		},
		func(b []byte, err error) {
			if err != nil {
				IncError("nack_msg", d.connectorName)
			}
			nackMsgHandler(b, err)
		},
	)
}

func (d *promReaderWrapper) MsgIDArgsLen() int {
	return d.r.MsgIDArgsLen()
}

func (d *promReaderWrapper) EncodeMsgID(buf []byte, topic string, args ...any) []byte {
	return d.r.EncodeMsgID(buf, topic, args...)
}

func (d *promReaderWrapper) AutoCommit() bool {
	return d.r.AutoCommit()
}

func (d *promReaderWrapper) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = Shutdown(ctx)
	return d.r.Close()
}
