package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/ValerySidorin/fujin/config"
	"github.com/ValerySidorin/fujin/mq"
	"github.com/ValerySidorin/fujin/server/fujin"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	conf config.Config

	fujinServer *fujin.Server
	mqman       *mq.MQManager

	l *slog.Logger
}

func NewServer(conf config.Config, l *slog.Logger) (*Server, error) {
	conf.SetDefaults()

	s := &Server{
		conf: conf,
		l:    l,
	}

	s.mqman = mq.NewMQManager(s.conf.MQ, s.l)

	if !conf.Fujin.Disabled {
		s.fujinServer = fujin.NewServer(conf.Fujin, s.mqman, s.l)
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
	<-time.After(timeout)
	return s.fujinServer.ReadyForConnections()
}
