// Package bind provides a plugin system for bind middlewares.
// Bind middlewares process BIND requests before they are applied to connectors.
// They are useful for authentication, authorization, validation, and other pre-bind checks.
package bind

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fujin-io/fujin/public/plugins/middleware/bind/config"
)

var (
	ErrUnauthenticated  = errors.New("authentication required")
	ErrPermissionDenied = errors.New("authentication failed")
)

// Middleware processes a BIND request and can reject it.
// The middleware receives the bind request context and meta, and can:
// - Validate authentication/authorization
// - Reject the bind request by returning an error
type Middleware interface {
	// ProcessBind processes a BIND request.
	// ctx: request context
	// meta: bind request metadata (e.g., auth tokens, user info)
	// Returns an error if the bind request should be rejected.
	ProcessBind(
		ctx context.Context,
		meta map[string]string,
	) error
}

// Factory creates a bind middleware from configuration.
// config is the bind middleware-specific configuration (can be nil).
type Factory func(config any, l *slog.Logger) (Middleware, error)

var (
	factories = make(map[string]Factory)
	mu        sync.RWMutex
)

// Register registers a bind middleware factory with the given name.
// This is typically called from init() in bind middleware implementations.
// Returns an error if the middleware is already registered.
func Register(name string, factory Factory) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[name]; exists {
		return fmt.Errorf("bind middleware %q already registered", name)
	}

	factories[name] = factory
	return nil
}

// Get returns a bind middleware factory by name.
func Get(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, ok := factories[name]
	return factory, ok
}

// List returns all registered bind middleware names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(factories))
	for name := range factories {
		names = append(names, name)
	}
	return names
}

// Chain applies a chain of bind middlewares to a BIND request.
// Middlewares are executed in order. If any middleware returns an error,
// the chain stops and the error is returned.
func Chain(
	ctx context.Context,
	meta map[string]string,
	configs []config.Config,
	l *slog.Logger,
) error {
	for _, cfg := range configs {
		// Skip if disabled (nil = true = enabled by default)
		if cfg.Enabled != nil && !*cfg.Enabled {
			continue
		}

		factory, ok := Get(cfg.Name)
		if !ok {
			return fmt.Errorf("bind middleware %q not found (available: %v)", cfg.Name, List())
		}

		middleware, err := factory(cfg.Config, l)
		if err != nil {
			return fmt.Errorf("create bind middleware %q: %w", cfg.Name, err)
		}

		// Process the bind request
		if err := middleware.ProcessBind(ctx, meta); err != nil {
			return fmt.Errorf("bind middleware %q: %w", cfg.Name, err)
		}
	}

	return nil
}
