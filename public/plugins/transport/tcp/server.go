package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/handler"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

type Server struct {
	conf         serverconfig.TCPServerConfig
	catalog      *connector.Catalog
	buildVersion string

	ln net.Listener // stored for ListenerFDs

	ready chan struct{}
	done  chan struct{}

	l *slog.Logger
}

func NewServer(conf serverconfig.TCPServerConfig, catalog *connector.Catalog, l *slog.Logger, buildVersion ...string) *Server {
	return &Server{
		conf:         conf,
		catalog:      catalog,
		buildVersion: resolvedBuildVersion(buildVersion),
		ready:        make(chan struct{}),
		done:         make(chan struct{}),
		l:            l.With("server", "fujin_tcp"),
	}
}

// FDKey implements transport.FDKeyProvider.
func (s *Server) FDKey() string { return "tcp:" + s.conf.Addr }

// Endpoint returns the actual TCP listener endpoint after readiness.
func (s *Server) Endpoint() transport.Endpoint {
	address := s.conf.Addr
	if s.ln != nil {
		address = s.ln.Addr().String()
	}
	return transport.Endpoint{Interface: "native", Transport: "tcp", Network: "tcp", Address: address, TLS: s.conf.TLS != nil}
}

// ListenerFDs implements transport.ListenerFDProvider.
func (s *Server) ListenerFDs() ([]transport.ListenerFD, error) {
	if s.ln == nil {
		return nil, fmt.Errorf("tcp listener not started")
	}
	type filer interface {
		File() (*os.File, error)
	}
	f, ok := s.ln.(filer)
	if !ok {
		return nil, fmt.Errorf("tcp listener does not support File()")
	}
	file, err := f.File()
	if err != nil {
		return nil, fmt.Errorf("tcp listener file: %w", err)
	}
	return []transport.ListenerFD{{FD: file, Type: "tcp", Addr: s.conf.Addr}}, nil
}

// ListenAndServeInherited implements transport.ListenerInheritor.
func (s *Server) ListenAndServeInherited(ctx context.Context, fd *os.File) error {
	ln, err := net.FileListener(fd)
	fd.Close()
	if err != nil {
		return fmt.Errorf("inherit tcp listener: %w", err)
	}
	if s.conf.TLS != nil {
		ln = tls.NewListener(ln, s.conf.TLS)
	}
	return s.acceptLoop(ctx, ln)
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	var (
		ln  net.Listener
		err error
	)

	if s.conf.TLS != nil {
		ln, err = tls.Listen("tcp", s.conf.Addr, s.conf.TLS)
	} else {
		ln, err = net.Listen("tcp", s.conf.Addr)
	}
	if err != nil {
		return fmt.Errorf("listen tcp: %w", err)
	}

	return s.acceptLoop(ctx, ln)
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener) error {
	s.ln = ln

	connWg := &sync.WaitGroup{}

	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.l.Error("close tcp listener", "err", err)
		}

		timeout := time.After(30 * time.Second)
		done := make(chan struct{})
		go func() {
			connWg.Wait()
			close(done)
		}()

		select {
		case <-timeout:
			s.l.Error("closing tcp listener after timeout")
		case <-done:
			s.l.Info("closing tcp listener after all connections done")
		}

		close(s.done)
		s.l.Info("fujin tcp server stopped")
	}()

	close(s.ready)
	s.l.Info("fujin tcp server started", "addr", ln.Addr())

	// Close listener when context is cancelled to unblock Accept
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.l.Error("accept tcp conn", "err", err)
			continue
		}

		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
			_ = tc.SetNoDelay(true)
		}

		connWg.Add(1)
		go func() {
			defer func() {
				conn.Close()
				connWg.Done()
			}()

			handler.HandleStream(ctx, conn, session.StreamOptions{
				BuildVersion:          s.buildVersion,
				BaseGeneration:        s.catalog.Current(),
				GenerationProvider:    s.catalog.Current,
				PingInterval:          s.conf.Fujin.PingInterval,
				PingTimeout:           s.conf.Fujin.PingTimeout,
				WriteDeadline:         s.conf.Fujin.WriteDeadline,
				ForceTerminateTimeout: s.conf.Fujin.ForceTerminateTimeout,
				AbortRead:             func() { conn.SetReadDeadline(time.Now()) },
				CloseRead:             func() { conn.SetReadDeadline(time.Now()) },
				Logger:                s.l,
			})
		}()
	}
}

func (s *Server) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-time.After(timeout):
		return false
	case <-s.ready:
		return true
	}
}

func (s *Server) Done() <-chan struct{} {
	return s.done
}

func resolvedBuildVersion(values []string) string {
	if len(values) > 0 && values[0] != "" {
		return values[0]
	}
	return "dev"
}
