// Package auth_api_key provides API key authentication bind middleware.
// Import this package to enable API key authentication:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/middleware/bind/auth_api_key"
//
// Configure in YAML:
//
//	connectors:
//	  my_connector:
//	    type: kafka_franz
//	    bind_middlewares:
//	      - name: auth_api_key
//	        config:
//	          api_key: my-secret-api-key
//
// The client must provide the API key in the BIND request meta field:
// meta["api_key"] = "my-secret-api-key"
package auth_api_key

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"

	bmw "github.com/fujin-io/fujin/public/plugins/middleware/bind"
)

const (
	metaKeyAPIKey = "api_key"
)

// Config for API key authentication bind middleware
type Config struct {
	APIKey string `yaml:"api_key"`
}

func init() {
	_ = bmw.Register("auth_api_key", func(config any, l *slog.Logger) (bmw.Middleware, error) {
		cfg := Config{}

		// Parse config if provided
		if m, ok := config.(map[string]any); ok {
			if apiKey, exists := m[metaKeyAPIKey]; exists {
				if v, ok := apiKey.(string); ok {
					cfg.APIKey = v
				}
			}
		}

		if cfg.APIKey == "" {
			return nil, fmt.Errorf("api_key is required")
		}

		return &authAPIKeyMiddleware{
			apiKey: cfg.APIKey,
			l:      l,
		}, nil
	})
}

type authAPIKeyMiddleware struct {
	apiKey string
	l      *slog.Logger
}

func (m *authAPIKeyMiddleware) ProcessBind(
	ctx context.Context,
	meta map[string]string,
) error {
	// Extract API key from meta
	providedAPIKey, ok := meta[metaKeyAPIKey]
	if !ok {
		m.l.Warn("bind rejected: api_key missing in meta")
		return fmt.Errorf("%w: api_key missing in meta", bmw.ErrUnauthenticated)
	}

	// Validate API key (constant-time comparison to prevent timing attacks)
	if subtle.ConstantTimeCompare([]byte(providedAPIKey), []byte(m.apiKey)) != 1 {
		m.l.Warn("bind rejected: invalid api_key")
		return fmt.Errorf("%w: invalid api_key", bmw.ErrPermissionDenied)
	}

	m.l.Info("bind authenticated")
	return nil
}
