package observability

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
	Path    string `yaml:"path"`
}

type TracingConfig struct {
	Enabled      bool           `yaml:"enabled"`
	OTLPEndpoint string         `yaml:"otlp_endpoint"`
	Insecure     bool           `yaml:"insecure"`
	SampleRatio  float64        `yaml:"sample_ratio"`
	Resource     ResourceConfig `yaml:"resource"`
}

type ResourceConfig struct {
	ServiceName    string `yaml:"service_name"`
	ServiceVersion string `yaml:"service_version"`
	Environment    string `yaml:"environment"`
}

type Config struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Tracing TracingConfig `yaml:"tracing"`
}

var (
	metricsEnabled int32
	tracingEnabled int32

	defaultTracer trace.Tracer

	opsTotal                 *prometheus.CounterVec
	errorsTotal              *prometheus.CounterVec
	connectorWriteLatencySec *prometheus.HistogramVec
	quicSessionsActive       prometheus.Gauge
	streamsActive            prometheus.Gauge

	httpSrv *http.Server
)

func MetricsEnabled() bool {
	return atomic.LoadInt32(&metricsEnabled) == 1
}

func TracingEnabled() bool {
	return atomic.LoadInt32(&tracingEnabled) == 1
}

func Tracer() trace.Tracer {
	if defaultTracer != nil {
		return defaultTracer
	}
	return otel.Tracer("fujin")
}

func Init(ctx context.Context, cfg Config, l *slog.Logger) (func(context.Context) error, error) {
	shutdownFns := []func(context.Context) error{}

	if cfg.Metrics.Enabled {
		atomic.StoreInt32(&metricsEnabled, 1)
		opsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fujin_ops_total",
			Help: "Number of protocol operations",
		}, []string{"opcode", "connector"})
		errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fujin_errors_total",
			Help: "Errors by stage and connector",
		}, []string{"stage", "connector"})
		connectorWriteLatencySec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fujin_connector_write_latency_seconds",
			Help:    "Connector write latency",
			Buckets: prometheus.DefBuckets,
		}, []string{"connector"})
		prometheus.MustRegister(opsTotal, errorsTotal, connectorWriteLatencySec)

		mux := http.NewServeMux()
		path := cfg.Metrics.Path
		if path == "" {
			path = "/metrics"
		}
		mux.Handle(path, promhttp.Handler())
		httpSrv = &http.Server{Addr: cfg.Metrics.Addr, Handler: mux}
		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				l.Error("metrics http server", "err", err)
			}
		}()
		l.Info("metrics server started", "addr", cfg.Metrics.Addr)
		shutdownFns = append(shutdownFns, func(ctx context.Context) error { return httpSrv.Shutdown(ctx) })
	}

	if cfg.Tracing.Enabled {
		atomic.StoreInt32(&tracingEnabled, 1)
		var opts []otlptracegrpc.Option
		opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Tracing.OTLPEndpoint))
		if cfg.Tracing.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			l.Error("init otlp exporter", "err", err)
		} else {
			sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Tracing.SampleRatio))
			res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(
				"",
				attribute.String("service.name", cfg.Tracing.Resource.ServiceName),
				attribute.String("service.version", cfg.Tracing.Resource.ServiceVersion),
				attribute.String("deployment.environment", cfg.Tracing.Resource.Environment),
			))
			tp := sdktrace.NewTracerProvider(
				sdktrace.WithBatcher(exp),
				sdktrace.WithSampler(sampler),
				sdktrace.WithResource(res),
			)
			otel.SetTracerProvider(tp)
			defaultTracer = tp.Tracer("fujin")
			shutdownFns = append(shutdownFns, func(ctx context.Context) error { return tp.Shutdown(ctx) })
		}
	}

	return func(ctx context.Context) error {
		for i := len(shutdownFns) - 1; i >= 0; i-- {
			_ = shutdownFns[i](ctx)
		}
		return nil
	}, nil
}

func IncOp(opcode, connector string) {
	opsTotal.WithLabelValues(opcode, connector).Inc()
}

func IncError(stage, connector string) {
	errorsTotal.WithLabelValues(stage, connector).Inc()
}

func ObserveWriteLatency(connector string, d time.Duration) {
	connectorWriteLatencySec.WithLabelValues(connector).Observe(d.Seconds())
}

func IncQuicSessions(delta float64) {
	quicSessionsActive.Add(delta)
}

func IncStreams(delta float64) {
	streamsActive.Add(delta)
}
