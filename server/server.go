package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/ValerySidorin/fujin/config"
	"github.com/ValerySidorin/fujin/internal/connector"
	"github.com/ValerySidorin/fujin/server/fujin"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf config.Config

	fujinServer *fujin.Server
	cman        *connector.Manager

	l *slog.Logger
}

func NewServer(conf config.Config, l *slog.Logger) (*Server, error) {
	conf.SetDefaults()

	s := &Server{
		conf: conf,
		l:    l,
	}

	s.cman = connector.NewManager(s.conf.Connectors, s.l)

	if !conf.Fujin.Disabled {
		s.fujinServer = fujin.NewServer(conf.Fujin, s.cman, s.l)
	}

	return s, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	s.l.Info("starting fujin server")
	eg, eCtx := errgroup.WithContext(ctx)
	if s.fujinServer != nil {
		eg.Go(func() error {
			return s.fujinServer.ListenAndServe(eCtx)
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
	return s.fujinServer.Done()
}
