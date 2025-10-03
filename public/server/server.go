package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/ValerySidorin/fujin/internal/connectors"
	obs "github.com/ValerySidorin/fujin/internal/observability"
	"github.com/ValerySidorin/fujin/public/server/config"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf config.Config

	fujinServer FujinServer
	grpcServer  GRPCServer
	cman        *connectors.Manager

	l *slog.Logger
}

// FujinServer interface for optional Fujin server
type FujinServer interface {
	ListenAndServe(ctx context.Context) error
	ReadyForConnections(timeout time.Duration) bool
	Done() <-chan struct{}
}

// GRPCServer interface for optional gRPC server
type GRPCServer interface {
	ListenAndServe(ctx context.Context) error
	Stop()
}

// NewServer creates a new server instance
func NewServer(conf config.Config, l *slog.Logger) (*Server, error) {
	conf.SetDefaults()

	s := &Server{
		conf: conf,
		l:    l,
	}

	s.cman = connectors.NewManager(s.conf.Connectors, s.l)

	if !conf.Fujin.Disabled {
		s.fujinServer = newFujinServerImpl(conf.Fujin, s.cman, s.l)
	}

	// Initialize gRPC server if enabled
	s.grpcServer = newGRPCServer(conf.GRPC, s.cman, l)

	return s, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	defer s.cman.Close()

	shutdown, _ := obs.Init(ctx, s.conf.Observability, s.l)
	if shutdown != nil {
		defer func() {
			stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			_ = shutdown(stopCtx)
		}()
	}

	eg, eCtx := errgroup.WithContext(ctx)

	if s.fujinServer != nil {
		eg.Go(func() error {
			return s.fujinServer.ListenAndServe(eCtx)
		})
	}

	if s.grpcServer != nil {
		eg.Go(func() error {
			return s.grpcServer.ListenAndServe(eCtx)
		})
	}

	return eg.Wait()
}

func (s *Server) ReadyForConnections(timeout time.Duration) bool {
	ready := make(chan struct{})
	go func() {
		if s.fujinServer != nil {
			if !s.fujinServer.ReadyForConnections(timeout) {
				return
			}
		}
		close(ready)
	}()

	select {
	case <-time.After(timeout):
		return false
	case <-ready:
		return true
	}
}

func (s *Server) Done() <-chan struct{} {
	if s.fujinServer != nil {
		return s.fujinServer.Done()
	}
	// Return a closed channel if no fujin server
	done := make(chan struct{})
	close(done)
	return done
}

// newGRPCServer creates a new gRPC server instance if gRPC is enabled
func newGRPCServer(conf config.GRPCConfig, cman *connectors.Manager, l *slog.Logger) GRPCServer {
	if conf.Disabled {
		return nil
	}

	// This will only be compiled when the grpc build tag is present
	return newGRPCServerImpl(conf.Addr, cman, l)
}
