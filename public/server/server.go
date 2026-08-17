package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/fujin-io/fujin/internal/health"
	grpc_server "github.com/fujin-io/fujin/internal/transport/grpc/v1/server"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/server/config"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf    config.Config
	catalog *connector.Catalog

	transportServers []transport.TransportServer
	grpcServer       GRPCServer
	healthServer     *health.Server

	inheritedFDs map[string]*os.File // keyed by "type:addr", e.g. "tcp::4850"

	l *slog.Logger
}

// TransportServer is the common interface for fujin-protocol transports.
// Use transport.TransportServer from the transport plugin package.
type TransportServer = transport.TransportServer

// GRPCServer interface for optional gRPC server
type GRPCServer interface {
	ListenAndServe(ctx context.Context) error
	Stop()
	ReadyForConnections(timeout time.Duration) bool
	Done() <-chan struct{}
}

// NewServer creates a new server instance and validates the complete connector generation.
func NewServer(conf config.Config, l *slog.Logger) (*Server, error) {
	conf.SetDefaults()
	catalog, err := connector.CompileCatalog(conf.Connectors, l)
	if err != nil {
		return nil, fmt.Errorf("compile connectors: %w", err)
	}

	s := &Server{conf: conf, catalog: catalog, l: l}
	for _, entry := range conf.Transports {
		srv, err := transport.NewServer(entry, catalog, l)
		if err != nil {
			_ = catalog.Close(context.Background())
			return nil, err
		}
		if srv != nil {
			s.transportServers = append(s.transportServers, srv)
		}
	}

	if conf.GRPC.Enabled {
		s.grpcServer = grpc_server.NewGRPCServerWrapper(s.conf.GRPC, catalog, s.l)
	}
	if conf.Health.Enabled {
		s.healthServer = health.NewServer(conf.Health, s.l)
	}
	return s, nil
}

// ReloadConnectors compiles a complete replacement before publishing it.
// Existing BINDs remain pinned to their prior generation.
func (s *Server) ReloadConnectors(cc connectorconfig.ConnectorsConfig) error {
	if err := s.catalog.Reload(cc); err != nil {
		return err
	}
	s.l.Info("connectors config reloaded", "count", len(cc))
	return nil
}

// ConnectorCatalogStatus returns a detached generation lifecycle projection.
func (s *Server) ConnectorCatalogStatus() connector.CatalogStatus {
	if s == nil || s.catalog == nil {
		return connector.CatalogStatus{}
	}
	return s.catalog.Status()
}

// SetInheritedFDs sets file descriptors inherited from a previous process
// during a graceful binary upgrade. Keys are "type:addr", e.g. "tcp::4850".
func (s *Server) SetInheritedFDs(fds map[string]*os.File) {
	s.inheritedFDs = fds
}

// ListenerFDs collects listener file descriptors from all transports
// for passing to a new process during graceful binary upgrade.
func (s *Server) ListenerFDs() ([]transport.ListenerFD, error) {
	var fds []transport.ListenerFD
	for _, ts := range s.transportServers {
		if p, ok := ts.(transport.ListenerFDProvider); ok {
			tsFDs, err := p.ListenerFDs()
			if err != nil {
				return nil, err
			}
			fds = append(fds, tsFDs...)
		}
	}
	if s.grpcServer != nil {
		if p, ok := s.grpcServer.(transport.ListenerFDProvider); ok {
			gFDs, err := p.ListenerFDs()
			if err != nil {
				return nil, err
			}
			fds = append(fds, gFDs...)
		}
	}
	return fds, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	eg, eCtx := errgroup.WithContext(ctx)

	for _, ts := range s.transportServers {
		ts := ts
		if inh, ok := ts.(transport.ListenerInheritor); ok && s.inheritedFDs != nil {
			if fd := s.findInheritedFD(ts); fd != nil {
				eg.Go(func() error { return inh.ListenAndServeInherited(eCtx, fd) })
				continue
			}
		}
		eg.Go(func() error { return ts.ListenAndServe(eCtx) })
	}

	if s.grpcServer != nil {
		if inh, ok := s.grpcServer.(interface {
			ListenAndServeInherited(context.Context, *os.File) error
		}); ok && s.inheritedFDs != nil {
			key := "tcp:" + s.conf.GRPC.Addr + ":grpc"
			if fd, ok := s.inheritedFDs[key]; ok {
				eg.Go(func() error { return inh.ListenAndServeInherited(eCtx, fd) })
			} else {
				eg.Go(func() error { return s.grpcServer.ListenAndServe(eCtx) })
			}
		} else {
			eg.Go(func() error { return s.grpcServer.ListenAndServe(eCtx) })
		}
	}
	if s.healthServer != nil {
		eg.Go(func() error { return s.healthServer.ListenAndServe(eCtx) })
	}

	serveErr := eg.Wait()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return errors.Join(serveErr, s.catalog.Close(cleanupCtx))
}

func (s *Server) findInheritedFD(ts transport.TransportServer) *os.File {
	if s.inheritedFDs == nil {
		return nil
	}
	if kp, ok := ts.(transport.FDKeyProvider); ok {
		if fd, ok := s.inheritedFDs[kp.FDKey()]; ok {
			return fd
		}
	}
	return nil
}

func (s *Server) ReadyForConnections(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for _, ts := range s.transportServers {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if !ts.ReadyForConnections(remaining) {
			return false
		}
	}
	if s.grpcServer != nil {
		remaining := time.Until(deadline)
		if remaining <= 0 || !s.grpcServer.ReadyForConnections(remaining) {
			return false
		}
	}
	if s.healthServer != nil {
		s.healthServer.SetReady(true)
	}
	return true
}

func (s *Server) Done() <-chan struct{} {
	done := make(chan struct{})
	if len(s.transportServers) == 0 && s.grpcServer == nil {
		close(done)
		return done
	}

	go func() {
		for _, ts := range s.transportServers {
			<-ts.Done()
		}
		if s.grpcServer != nil {
			<-s.grpcServer.Done()
		}
		close(done)
	}()
	return done
}
