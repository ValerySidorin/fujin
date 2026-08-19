package token_bucket

import (
	"fmt"
	"log/slog"

	"golang.org/x/time/rate"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

func init() {
	if err := cmw.Register("rate_limit_token_bucket", newRateLimitMiddleware); err != nil {
		panic(fmt.Sprintf("register rate_limit_token_bucket connector middleware: %v", err))
	}
}

type rateLimitMiddleware struct {
	limiter *rate.Limiter
	l       *slog.Logger
}

func newRateLimitMiddleware(config any, l *slog.Logger) (cmw.Middleware, error) {
	var rateVal float64
	var burst int

	if m, ok := config.(map[string]any); ok {
		switch v := m["rate"].(type) {
		case float64:
			rateVal = v
		case int:
			rateVal = float64(v)
		}
		switch v := m["burst"].(type) {
		case float64:
			burst = int(v)
		case int:
			burst = v
		}
	}

	if rateVal <= 0 {
		return nil, fmt.Errorf("rate_limit_token_bucket middleware: 'rate' must be a positive number (requests per second)")
	}
	if burst <= 0 {
		return nil, fmt.Errorf("rate_limit_token_bucket middleware: 'burst' must be a positive integer")
	}

	limiter := rate.NewLimiter(rate.Limit(rateVal), burst)

	l.Info("token bucket rate limit middleware initialized",
		"rate", rateVal,
		"burst", burst,
	)

	return &rateLimitMiddleware{
		limiter: limiter,
		l:       l,
	}, nil
}

func (m *rateLimitMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	return &rateLimitWriterWrapper{
		w:  w,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

func (m *rateLimitMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	return r
}

// RateLimitError indicates a message was rejected due to rate limiting.
type RateLimitError struct{}

func (e *RateLimitError) Error() string {
	return "rate limit exceeded"
}
