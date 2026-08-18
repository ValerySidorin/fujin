package websocket

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/handler"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
	gorillaws "github.com/gorilla/websocket"
)

const connectionDrainTimeout = 30 * time.Second

type Server struct {
	config  serverconfig.WebSocketServerConfig
	catalog *connector.Catalog
	logger  *slog.Logger

	listener    net.Listener
	rawListener *net.TCPListener
	ready       chan struct{}
	done        chan struct{}
	readyOnce   sync.Once
	doneOnce    sync.Once

	connectionsMu sync.Mutex
	connections   map[*stream]struct{}
	connectionsWG sync.WaitGroup
}

func NewServer(config serverconfig.WebSocketServerConfig, catalog *connector.Catalog, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if config.Path == "" {
		config.Path = defaultPath
	}
	if config.MaxMessageBytes <= 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	config.Fujin.SetDefaults()
	return &Server{
		config:      config,
		catalog:     catalog,
		logger:      logger.With("server", "fujin_websocket"),
		ready:       make(chan struct{}),
		done:        make(chan struct{}),
		connections: make(map[*stream]struct{}),
	}
}

func (s *Server) FDKey() string { return "tcp:" + s.config.Addr }

func (s *Server) ListenerFDs() ([]transport.ListenerFD, error) {
	if s.rawListener == nil {
		return nil, errors.New("websocket listener not started")
	}
	file, err := s.rawListener.File()
	if err != nil {
		return nil, fmt.Errorf("websocket listener file: %w", err)
	}
	return []transport.ListenerFD{{FD: file, Type: "tcp", Addr: s.config.Addr}}, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return fmt.Errorf("listen websocket: %w", err)
	}
	return s.serve(ctx, listener)
}

func (s *Server) ListenAndServeInherited(ctx context.Context, file *os.File) error {
	listener, err := net.FileListener(file)
	_ = file.Close()
	if err != nil {
		return fmt.Errorf("inherit websocket listener: %w", err)
	}
	return s.serve(ctx, listener)
}

func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	raw, ok := listener.(*net.TCPListener)
	if !ok {
		_ = listener.Close()
		return errors.New("websocket transport requires a TCP listener")
	}
	s.rawListener = raw
	s.listener = listener
	servingListener := listener
	if s.config.TLS != nil {
		servingListener = tls.NewListener(listener, s.config.TLS.Clone())
	}

	upgrader := gorillaws.Upgrader{
		HandshakeTimeout: 10 * time.Second,
		CheckOrigin:      originChecker(s.config.AllowedOrigins),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(s.config.Path, func(response http.ResponseWriter, request *http.Request) {
		s.handleConnection(ctx, upgrader, response, request)
	})
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	s.readyOnce.Do(func() { close(s.ready) })
	s.logger.Info("fujin websocket server started", "addr", listener.Addr(), "path", s.config.Path)
	go func() {
		<-ctx.Done()
		_ = httpServer.Close()
	}()

	err := httpServer.Serve(servingListener)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		err = nil
	}
	s.waitForConnections()
	s.doneOnce.Do(func() { close(s.done) })
	s.logger.Info("fujin websocket server stopped")
	return err
}

func (s *Server) handleConnection(
	ctx context.Context,
	upgrader gorillaws.Upgrader,
	response http.ResponseWriter,
	request *http.Request,
) {
	connection, err := upgrader.Upgrade(response, request, nil)
	if err != nil {
		s.logger.Warn("upgrade websocket connection", "err", err)
		return
	}
	connection.SetReadLimit(s.config.MaxMessageBytes)
	stream := newStream(connection)
	s.track(stream, true)
	defer func() {
		_ = stream.Close()
		s.track(stream, false)
	}()

	handler.HandleStream(ctx, stream, session.StreamOptions{
		BaseGeneration:        s.catalog.Current(),
		GenerationProvider:    s.catalog.Current,
		PingInterval:          s.config.Fujin.PingInterval,
		PingTimeout:           s.config.Fujin.PingTimeout,
		WriteDeadline:         s.config.Fujin.WriteDeadline,
		ForceTerminateTimeout: s.config.Fujin.ForceTerminateTimeout,
		AbortRead:             func() { _ = stream.SetReadDeadline(time.Now()) },
		CloseRead:             func() { _ = stream.SetReadDeadline(time.Now()) },
		Logger:                s.logger,
	})
}

func (s *Server) track(stream *stream, add bool) {
	s.connectionsMu.Lock()
	defer s.connectionsMu.Unlock()
	if add {
		s.connections[stream] = struct{}{}
		s.connectionsWG.Add(1)
		return
	}
	delete(s.connections, stream)
	s.connectionsWG.Done()
}

func (s *Server) waitForConnections() {
	settled := make(chan struct{})
	go func() {
		s.connectionsWG.Wait()
		close(settled)
	}()
	select {
	case <-settled:
		return
	case <-time.After(connectionDrainTimeout):
		s.logger.Warn("forcing websocket connection shutdown")
		s.connectionsMu.Lock()
		for connection := range s.connections {
			_ = connection.Close()
		}
		s.connectionsMu.Unlock()
		<-settled
	}
}

func originChecker(allowed []string) func(*http.Request) bool {
	if len(allowed) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(allowed))
	for _, origin := range allowed {
		set[strings.TrimSuffix(origin, "/")] = struct{}{}
	}
	return func(request *http.Request) bool {
		origin := request.Header.Get("Origin")
		if origin == "" {
			return true
		}
		if _, ok := set["*"]; ok {
			return true
		}
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return false
		}
		_, ok := set[strings.TrimSuffix(origin, "/")]
		return ok
	}
}

func (s *Server) ReadyForConnections(timeout time.Duration) bool {
	select {
	case <-s.ready:
		return true
	case <-s.done:
		return false
	case <-time.After(timeout):
		return false
	}
}

func (s *Server) Done() <-chan struct{} { return s.done }
