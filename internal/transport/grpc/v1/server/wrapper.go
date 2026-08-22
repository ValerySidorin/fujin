//go:build grpc

package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/transport"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

// GRPCServerWrapper wraps the gRPC server to implement the GRPCServer interface
type GRPCServerWrapper struct {
	server *GRPCServer
}

// NewGRPCServerWrapper creates a new gRPC server wrapper.
func NewGRPCServerWrapper(conf serverconfig.GRPCServerConfig, catalog *connector.Catalog, l *slog.Logger) *GRPCServerWrapper {
	return &GRPCServerWrapper{server: NewGRPCServer(conf, catalog, l)}
}

// ListenAndServe starts the gRPC server
func (w *GRPCServerWrapper) ListenAndServe(ctx context.Context) error {
	return w.server.ListenAndServe(ctx)
}

// Stop gracefully stops the gRPC server
func (w *GRPCServerWrapper) Stop() {
	w.server.Stop()
}

// ReadyForConnections waits until the gRPC listener is serving.
func (w *GRPCServerWrapper) ReadyForConnections(timeout time.Duration) bool {
	return w.server.ReadyForConnections(timeout)
}

// Done is closed after the gRPC server stops.
func (w *GRPCServerWrapper) Done() <-chan struct{} {
	return w.server.Done()
}

// Endpoint returns the actual gRPC listener endpoint after readiness.
func (w *GRPCServerWrapper) Endpoint() transport.Endpoint {
	address := w.server.conf.Addr
	if w.server.lis != nil {
		address = w.server.lis.Addr().String()
	}
	return transport.Endpoint{Interface: "grpc", Network: "tcp", Address: address, TLS: w.server.conf.TLS != nil}
}

// ListenerFDs implements transport.ListenerFDProvider.
func (w *GRPCServerWrapper) ListenerFDs() ([]transport.ListenerFD, error) {
	if w.server.lis == nil {
		return nil, fmt.Errorf("grpc listener not started")
	}
	type filer interface {
		File() (*os.File, error)
	}
	f, ok := w.server.lis.(filer)
	if !ok {
		return nil, fmt.Errorf("grpc listener does not support File()")
	}
	file, err := f.File()
	if err != nil {
		return nil, fmt.Errorf("grpc listener file: %w", err)
	}
	return []transport.ListenerFD{{
		FD:   file,
		Type: "tcp",
		Addr: w.server.conf.Addr,
		Meta: map[string]string{"grpc": "true"},
	}}, nil
}

// ListenAndServeInherited implements transport.ListenerInheritor.
func (w *GRPCServerWrapper) ListenAndServeInherited(ctx context.Context, fd *os.File) error {
	ln, err := net.FileListener(fd)
	fd.Close()
	if err != nil {
		return fmt.Errorf("inherit grpc listener: %w", err)
	}
	return w.server.ListenAndServeInherited(ctx, ln)
}
