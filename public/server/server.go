package server

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/fujin-io/fujin/internal/health"
	grpc_server "github.com/fujin-io/fujin/internal/transport/grpc/v1/server"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/server/config"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf config.Config

	connectorConfig atomic.Pointer[connectorconfig.ConnectorsConfig]

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

// NewServer creates a new server instance
func NewServer(conf config.Config, l *slog.Logger) (*Server, error) {
	conf.SetDefaults()

	s := &Server{
		conf: conf,
		l:    l,
	}

	// Store initial connectors config
	cc := conf.Connectors
	s.connectorConfig.Store(&cc)

	configProvider := func() connectorconfig.ConnectorsConfig {
		return *s.connectorConfig.Load()
	}

	for _, e := range conf.Transports {
		srv, err := transport.NewServer(e, conf.Connectors, l)
		if err != nil {
			return nil, err
		}
		if srv != nil {
			// Enable hot reload if transport supports it
			if hr, ok := srv.(transport.HotReloadable); ok {
				hr.SetBaseConfigProvider(configProvider)
			}
			s.transportServers = append(s.transportServers, srv)
		}
	}

	if conf.GRPC.Enabled {
		wrapper := grpc_server.NewGRPCServerWrapper(s.conf.GRPC, s.conf.Connectors, s.l)
		// Enable hot reload on gRPC wrapper
		if hr, ok := interface{}(wrapper).(transport.HotReloadable); ok {
			hr.SetBaseConfigProvider(configProvider)
		}
		s.grpcServer = wrapper
	}

	if conf.Health.Enabled {
		s.healthServer = health.NewServer(conf.Health, s.l)
	}

	return s, nil
}

// ReloadConnectors atomically swaps the connectors config.
// New connections will use the updated config; existing connections are unaffected.
func (s *Server) ReloadConnectors(cc connectorconfig.ConnectorsConfig) {
	s.connectorConfig.Store(&cc)
	s.l.Info("connectors config reloaded", "count", len(cc))
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

		// Check if we have an inherited FD for this transport
		if inh, ok := ts.(transport.ListenerInheritor); ok && s.inheritedFDs != nil {
			if fd := s.findInheritedFD(ts); fd != nil {
				eg.Go(func() error {
					return inh.ListenAndServeInherited(eCtx, fd)
				})
				continue
			}
		}

		eg.Go(func() error {
			return ts.ListenAndServe(eCtx)
		})
	}

	if s.grpcServer != nil {
		// Check for inherited gRPC FD
		if inh, ok := s.grpcServer.(interface {
			ListenAndServeInherited(ctx context.Context, fd *os.File) error
		}); ok && s.inheritedFDs != nil {
			key := "tcp:" + s.conf.GRPC.Addr + ":grpc"
			if fd, ok := s.inheritedFDs[key]; ok {
				eg.Go(func() error {
					return inh.ListenAndServeInherited(eCtx, fd)
				})
			} else {
				eg.Go(func() error {
					return s.grpcServer.ListenAndServe(eCtx)
				})
			}
		} else {
			eg.Go(func() error {
				return s.grpcServer.ListenAndServe(eCtx)
			})
		}
	}

	if s.healthServer != nil {
		eg.Go(func() error {
			return s.healthServer.ListenAndServe(eCtx)
		})
	}

	return eg.Wait()
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
