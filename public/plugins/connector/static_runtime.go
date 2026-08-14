package connector

import (
	"context"
	"errors"
	"log/slog"
)

// ReaderFactory creates a session-scoped reader lease for one compiled route.
type ReaderFactory func(autoSettle bool, l *slog.Logger) (ReadCloser, error)

// WriterFactory creates a session-scoped writer lease for one compiled route.
type WriterFactory func(l *slog.Logger) (WriteCloser, error)

// RouteFactory contains the already validated factories for one route.
type RouteFactory struct {
	Reader ReaderFactory
	Writer WriterFactory
}

// StaticRuntime is a generation runtime for adapters whose physical resources remain
// inside reader/writer leases. Adapters needing shared endpoints may implement Runtime directly.
type StaticRuntime struct {
	Routes map[string]RouteFactory
}

func (r *StaticRuntime) NewReader(route string, autoSettle bool, l *slog.Logger) (ReadCloser, error) {
	factory, ok := r.Routes[route]
	if !ok {
		return nil, ErrRouteNotFound
	}
	if factory.Reader == nil {
		return nil, ErrOperationUnsupported
	}
	return factory.Reader(autoSettle, l)
}

func (r *StaticRuntime) NewWriter(route string, l *slog.Logger) (WriteCloser, error) {
	factory, ok := r.Routes[route]
	if !ok {
		return nil, ErrRouteNotFound
	}
	if factory.Writer == nil {
		return nil, ErrOperationUnsupported
	}
	return factory.Writer(l)
}

func (*StaticRuntime) Close(context.Context) error { return nil }

// CompileStatic validates profiles and returns a generation runtime over immutable factories.
func CompileStatic(profiles map[string]RouteProfile, factories map[string]RouteFactory) (Compiled, error) {
	if len(factories) != len(profiles) {
		return nil, errors.New("connector route profile/factory mismatch")
	}
	for route := range profiles {
		if _, ok := factories[route]; !ok {
			return nil, errors.New("connector route factory missing: " + route)
		}
	}
	routeFactories := make(map[string]RouteFactory, len(factories))
	for route, factory := range factories {
		routeFactories[route] = factory
	}
	return StaticCompiled(profiles, func(*slog.Logger) (Runtime, error) {
		return &StaticRuntime{Routes: routeFactories}, nil
	})
}
