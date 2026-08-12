package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	"github.com/fujin-io/fujin/public/plugins/transport"
	v1 "github.com/fujin-io/fujin/public/proto/fujin/v1"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/handler"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	quicgo "github.com/quic-go/quic-go"
)

var (
	NextProtos = []string{v1.Version}
)

type FujinServer struct {
	conf       serverconfig.QUICServerConfig
	baseConfig connectorconfig.ConnectorsConfig

	configProvider func() connectorconfig.ConnectorsConfig

	udpConn *net.UDPConn // stored for ListenerFDs

	ready chan struct{}
	done  chan struct{}

	l *slog.Logger
}

func NewServer(conf serverconfig.QUICServerConfig, baseConfig connectorconfig.ConnectorsConfig, l *slog.Logger) *FujinServer {
	return &FujinServer{
		conf:       conf,
		baseConfig: baseConfig,
		ready:      make(chan struct{}),
		done:       make(chan struct{}),
		l:          l.With("server", "fujin_quic"),
	}
}

// FDKey implements transport.FDKeyProvider.
func (s *FujinServer) FDKey() string { return "udp:" + s.conf.Addr }

// SetBaseConfigProvider implements transport.HotReloadable.
func (s *FujinServer) SetBaseConfigProvider(p func() connectorconfig.ConnectorsConfig) {
	s.configProvider = p
}

// ListenerFDs implements transport.ListenerFDProvider.
func (s *FujinServer) ListenerFDs() ([]transport.ListenerFD, error) {
	if s.udpConn == nil {
		return nil, fmt.Errorf("quic udp conn not started")
	}
	file, err := s.udpConn.File()
	if err != nil {
		return nil, fmt.Errorf("quic udp conn file: %w", err)
	}
	return []transport.ListenerFD{{FD: file, Type: "udp", Addr: s.conf.Addr}}, nil
}

// ListenAndServeInherited implements transport.ListenerInheritor.
func (s *FujinServer) ListenAndServeInherited(ctx context.Context, fd *os.File) error {
	pc, err := net.FilePacketConn(fd)
	fd.Close()
	if err != nil {
		return fmt.Errorf("inherit udp conn: %w", err)
	}
	udpConn, ok := pc.(*net.UDPConn)
	if !ok {
		pc.Close()
		return fmt.Errorf("inherited fd is not a UDP connection")
	}
	return s.serve(ctx, udpConn)
}

func (s *FujinServer) ListenAndServe(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", s.conf.Addr)
	if err != nil {
		return fmt.Errorf("resolve udp addr: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}

	return s.serve(ctx, conn)
}

func (s *FujinServer) serve(ctx context.Context, conn *net.UDPConn) error {
	s.udpConn = conn

	tr := &quicgo.Transport{
		Conn: conn,
	}

	s.conf.TLS = s.conf.TLS.Clone()

	if s.conf.TLS == nil {
		s.conf.TLS = &tls.Config{}
	}

	s.conf.TLS.NextProtos = NextProtos

	if len(s.conf.TLS.Certificates) == 0 ||
		s.conf.TLS.ClientCAs == nil {
		s.l.Warn("tls not configured, this is not recommended for production environment")
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

		close(s.done)
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

			negotiated := conn.ConnectionState().TLS.NegotiatedProtocol
			if negotiated == "" {
				_ = conn.CloseWithError(v1.ConnErr, "unsupported protocol: none")
				continue
			}
			switch negotiated {
			case v1.Version:
				// ok – current version
			default:
				s.l.Warn("rejecting connection: unsupported ALPN", "alpn", negotiated)
				_ = conn.CloseWithError(v1.ConnErr, "unsupported protocol: "+negotiated)
				continue
			}

			connCtx, cancel := context.WithCancel(ctx)
			connWg.Add(1)
			go func() {
				retryCount := 0
				t := time.NewTicker(s.conf.Fujin.PingInterval)
				defer func() {
					t.Stop()
					cancel()
					connWg.Done()
				}()

				for {
					select {
					case <-connCtx.Done():
						return
					case <-t.C:
						err := pingQUICConnection(connCtx, conn, s.conf.Fujin.PingTimeout)
						if err == nil {
							retryCount = 0
							continue
						}

						retryCount++
						s.l.Error("ping error", "err", err, "retry", retryCount)
						if retryCount < s.conf.Fujin.PingMaxRetries {
							continue
						}
						if closeErr := conn.CloseWithError(v1.PingErr, "ping failed after retries: "+err.Error()); closeErr != nil {
							s.l.Error("close with error", "err", closeErr)
						}
						return
					}
				}
			}()

			go func() {
				for {
					str, err := conn.AcceptStream(connCtx)
					if err != nil {
						if err != connCtx.Err() {
							if err := conn.CloseWithError(v1.ConnErr, "accept stream: "+err.Error()); err != nil {
								s.l.Error("close with error", "err", err)
							}
						}
						return
					}

					connWg.Add(1)
					go func() {
						handler.HandleStream(connCtx, str, session.StreamOptions{
							BaseConfig:            s.baseConfig,
							BaseConfigProvider:    s.configProvider,
							PingInterval:          s.conf.Fujin.PingInterval,
							PingTimeout:           s.conf.Fujin.PingTimeout,
							PingStream:            s.conf.Fujin.PingStream,
							WriteDeadline:         s.conf.Fujin.WriteDeadline,
							ForceTerminateTimeout: s.conf.Fujin.ForceTerminateTimeout,
							AbortRead:             func() { str.CancelRead(v1.ConnErr) },
							CloseRead:             func() { str.CancelRead(v1.NoErr) },
							Logger:                s.l,
						})
						str.Close()
						connWg.Done()
					}()
				}
			}()
		}
	}
}

func pingQUICConnection(ctx context.Context, conn *quicgo.Conn, timeout time.Duration) error {
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	str, err := conn.OpenStreamSync(pingCtx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	deadline, _ := pingCtx.Deadline()
	if err := str.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	if _, err := str.Write(v1.PING_REQ); err != nil {
		str.CancelWrite(v1.PingErr)
		str.CancelRead(v1.PingErr)
		return fmt.Errorf("write request: %w", err)
	}
	if err := str.Close(); err != nil {
		str.CancelRead(v1.PingErr)
		return fmt.Errorf("close request: %w", err)
	}

	var response [2]byte
	n, err := io.ReadFull(str, response[:])
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		str.CancelRead(v1.PingErr)
		if err == nil {
			return fmt.Errorf("invalid response length: at least %d", n)
		}
		return fmt.Errorf("read response: %w", err)
	}
	if n != 1 || response[0] != byte(v1.RESP_CODE_PONG) {
		return fmt.Errorf("invalid response: %v", response[:n])
	}
	return nil
}

func (s *FujinServer) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-time.After(timeout):
		return false
	case <-s.ready:
		return true
	}
}

func (s *FujinServer) Done() <-chan struct{} {
	return s.done
}
