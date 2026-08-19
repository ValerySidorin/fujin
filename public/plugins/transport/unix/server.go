//go:build unix

package unix

import (
	"context"
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
	conf         serverconfig.UnixServerConfig
	catalog      *connector.Catalog
	buildVersion string

	ln net.Listener // stored for ListenerFDs

	ready chan struct{}
	done  chan struct{}

	l *slog.Logger
}

func NewServer(conf serverconfig.UnixServerConfig, catalog *connector.Catalog, l *slog.Logger, buildVersion ...string) *Server {
	return &Server{
		conf:         conf,
		catalog:      catalog,
		buildVersion: resolvedBuildVersion(buildVersion),
		ready:        make(chan struct{}),
		done:         make(chan struct{}),
		l:            l.With("server", "fujin_unix"),
	}
}

// FDKey implements transport.FDKeyProvider.
func (s *Server) FDKey() string { return "unix:" + s.conf.Path }

// ListenerFDs implements transport.ListenerFDProvider.
func (s *Server) ListenerFDs() ([]transport.ListenerFD, error) {
	if s.ln == nil {
		return nil, fmt.Errorf("unix listener not started")
	}
	type filer interface {
		File() (*os.File, error)
	}
	f, ok := s.ln.(filer)
	if !ok {
		return nil, fmt.Errorf("unix listener does not support File()")
	}
	file, err := f.File()
	if err != nil {
		return nil, fmt.Errorf("unix listener file: %w", err)
	}
	return []transport.ListenerFD{{FD: file, Type: "unix", Addr: s.conf.Path}}, nil
}

// ListenAndServeInherited implements transport.ListenerInheritor.
func (s *Server) ListenAndServeInherited(ctx context.Context, fd *os.File) error {
	ln, err := net.FileListener(fd)
	fd.Close()
	if err != nil {
		return fmt.Errorf("inherit unix listener: %w", err)
	}
	// Don't remove the socket file — inherited from old process
	return s.acceptLoop(ctx, ln, false)
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	path := s.conf.Path
	if path == "" {
		return fmt.Errorf("unix: path is required")
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unix: remove stale socket: %w", err)
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("unix: listen: %w", err)
	}

	return s.acceptLoop(ctx, ln, true)
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener, removeOnClose bool) error {
	s.ln = ln

	connWg := &sync.WaitGroup{}

	defer func() {
		ln.Close()
		if removeOnClose {
			os.Remove(s.conf.Path)
		}

		timeout := time.After(30 * time.Second)
		doneCh := make(chan struct{})
		go func() {
			connWg.Wait()
			close(doneCh)
		}()

		select {
		case <-timeout:
			s.l.Error("closing unix listener after timeout")
		case <-doneCh:
			s.l.Info("closing unix listener after all connections done")
		}

		close(s.done)
		s.l.Info("fujin unix server stopped")
	}()

	close(s.ready)
	s.l.Info("fujin unix server started", "path", s.conf.Path)

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
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.l.Error("accept unix conn", "err", err)
			continue
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
