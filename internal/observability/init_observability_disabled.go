//go:build !observability

package observability

import (
	"context"
	"fmt"
	"log/slog"
)

var ErrObservabilityNotCompiledIn = fmt.Errorf("observability is not compiled in")

func MetricsEnabled() bool {
	return false
}

func TracingEnabled() bool {
	return false
}

func Init(ctx context.Context, cfg Config, l *slog.Logger) (func(context.Context) error, error) {
	if cfg.Metrics.Enabled {
		return nil, ErrObservabilityNotCompiledIn
	}

	if cfg.Tracing.Enabled {
		return nil, ErrObservabilityNotCompiledIn
	}

	return func(ctx context.Context) error {
		return nil
	}, nil
}
