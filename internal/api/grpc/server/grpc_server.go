package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	fujinv1 "github.com/ValerySidorin/fujin/internal/api/grpc/v1"
	iconn "github.com/ValerySidorin/fujin/internal/connectors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// GRPCServerWrapper wraps the gRPC server for integration with the main server
type GRPCServerWrapper struct {
	server *grpc.Server
	addr   string
	l      *slog.Logger
}

// NewGRPCServerWrapper creates a new gRPC server wrapper
func NewGRPCServerWrapper(addr string, cman *iconn.Manager, l *slog.Logger) *GRPCServerWrapper {
	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register the Fujin service
	fujinService := NewGRPCServer(cman, l)
	fujinv1.RegisterFujinServiceServer(grpcServer, fujinService)

	// Enable reflection for debugging
	reflection.Register(grpcServer)

	return &GRPCServerWrapper{
		server: grpcServer,
		addr:   addr,
		l:      l,
	}
}

// ListenAndServe starts the gRPC server
func (s *GRPCServerWrapper) ListenAndServe(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	defer lis.Close()

	s.l.Info("starting gRPC server", "addr", s.addr)

	// Start serving in a goroutine
	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.l.Error("failed to serve gRPC", "error", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	s.l.Info("stopping gRPC server")
	s.server.GracefulStop()

	return nil
}

// Stop gracefully stops the gRPC server
func (s *GRPCServerWrapper) Stop() {
	s.server.GracefulStop()
}
