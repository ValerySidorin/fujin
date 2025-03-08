package fujin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/ValerySidorin/fujin/connector"
	"github.com/ValerySidorin/fujin/internal/server/fujin/pool"
	"github.com/ValerySidorin/fujin/server/fujin/ferr"
	"github.com/ValerySidorin/fujin/server/fujin/proto/request"
	"github.com/quic-go/quic-go"
)

type ServerConfig struct {
	Disabled              bool
	Addr                  string
	PingInterval          time.Duration
	PingTimeout           time.Duration
	WriteDeadline         time.Duration
	ForceTerminateTimeout time.Duration
	TLS                   *tls.Config
	QUIC                  *quic.Config
}

type Server struct {
	conf ServerConfig
	cman *connector.Manager

	ready chan struct{}

	l *slog.Logger
}

func NewServer(conf ServerConfig, cman *connector.Manager, l *slog.Logger) *Server {
	return &Server{
		conf:  conf,
		cman:  cman,
		ready: make(chan struct{}),
		l:     l,
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", s.conf.Addr)
	if err != nil {
		return fmt.Errorf("resolve udp addr: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}
	tr := &quic.Transport{
		Conn: conn,
	}

	ln, err := tr.Listen(s.conf.TLS, s.conf.QUIC)
	if err != nil {
		return fmt.Errorf("listen quic: %w", err)
	}

	connWg := &sync.WaitGroup{}

	defer func() {
		if err := ln.Close(); err != nil {
			s.l.Error("close quic listener", "err", err)
		}

		timeout := time.After(30 * time.Second)
		done := make(chan struct{})

		go func() {
			connWg.Wait()
			close(done)
		}()

		select {
		case <-timeout:
			s.l.Error("closing quic listener after timeout")
		case <-done:
			s.l.Info("closing quic listener after all connections done")
		}

		if err := tr.Close(); err != nil {
			s.l.Error("close quic transport", "err", err)
		}
		if err := conn.Close(); err != nil {
			s.l.Error("close udp listener", "err", err)
		}

		s.l.Info("fujin server stopped")
	}()

	close(s.ready)
	s.l.Info("fujin server started", "addr", ln.Addr())

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := ln.Accept(ctx)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					s.l.Error(fmt.Errorf("accept conn: %w", err).Error())
				}
				continue
			}

			ctx, cancel := context.WithCancel(ctx)
			go func() {
				t := time.NewTicker(s.conf.PingInterval)
				defer func() {
					t.Stop()
					cancel()
				}()

				pingBuf := pool.Get(1)
				pingBuf = pingBuf[:1]

				defer pool.Put(pingBuf)

				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						str, err := conn.OpenStreamSync(ctx)
						if err != nil {
							if err := conn.CloseWithError(ferr.PingErr, "open stream: "+err.Error()); err != nil {
								s.l.Error("close with error", "err", err)
							}
							return
						}

						pingBuf[0] = byte(request.OP_CODE_PING)

						if _, err := str.Write(pingBuf[:]); err != nil {
							if err := conn.CloseWithError(ferr.PingErr, "write: "+err.Error()); err != nil {
								s.l.Error("close with error", "err", err)
							}
							return
						}
						str.Close()

						_ = str.SetDeadline(time.Now().Add(s.conf.PingTimeout))
						if _, err := io.ReadAll(str); err != nil {
							if err := conn.CloseWithError(ferr.PingErr, "read: "+err.Error()); err != nil {
								s.l.Error("close with error", "err", err)
							}
						}
					}
				}
			}()

			go func() {
				for {
					str, err := conn.AcceptStream(ctx)
					if err != nil {
						if err != ctx.Err() {
							if err := conn.CloseWithError(ferr.ConnErr, "accept stream: "+err.Error()); err != nil {
								s.l.Error("close with error", "err", err)
							}
						}
						return
					}

					connWg.Add(1)
					go func() {
						defer connWg.Done()

						out := newOutbound(str, s.conf.WriteDeadline, s.l)
						h := newHandler(ctx, s.cman, out, s.l)
						in := newInbound(str, s.conf.ForceTerminateTimeout, h, s.l)

						go in.readLoop(ctx)
						out.writeloop()
						str.Close()
					}()
				}
			}()
		}
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
