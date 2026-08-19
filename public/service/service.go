package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	pconfig "github.com/fujin-io/fujin/public/config"
	"github.com/fujin-io/fujin/public/plugins/configurator"
	"github.com/fujin-io/fujin/public/plugins/connector"
	connectorconfig "github.com/fujin-io/fujin/public/plugins/connector/config"
	bmw "github.com/fujin-io/fujin/public/plugins/middleware/bind"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
	"github.com/fujin-io/fujin/public/plugins/transport"
	"github.com/fujin-io/fujin/public/server"
	serverconfig "github.com/fujin-io/fujin/public/server/config"
)

var (
	ErrNilConfig = errors.New("nil config")
)

type Config struct {
	Fujin      FujinConfig                      `yaml:"fujin"`
	GRPC       GRPCConfig                       `yaml:"grpc"`
	Health     HealthConfig                     `yaml:"health"`
	Connectors connectorconfig.ConnectorsConfig `yaml:"connectors"`
}

type FujinConfig struct {
	Transports []transport.Config `yaml:"transports"`
}

type GRPCConfig struct {
	Enabled               *bool                 `yaml:"enabled,omitempty"` // nil = true (default)
	Addr                  string                `yaml:"addr"`
	ConnectionTimeout     time.Duration         `yaml:"connection_timeout"`
	MaxConcurrentStreams  uint32                `yaml:"max_concurrent_streams"`
	MaxRecvMsgSize        int                   `yaml:"max_recv_msg_size"`
	MaxSendMsgSize        int                   `yaml:"max_send_msg_size"`
	InitialWindowSize     int32                 `yaml:"initial_window_size"`
	InitialConnWindowSize int32                 `yaml:"initial_conn_window_size"`
	ServerKeepAlive       ServerKeepAliveConfig `yaml:"server_keepalive"`
	ClientKeepAlive       ClientKeepAliveConfig `yaml:"client_keepalive"`
	TLS                   pconfig.TLSConfig     `yaml:"tls"`
	ObservabilityEnabled  *bool                 `yaml:"observability_enabled,omitempty"` // nil = false (default)
}

type ServerKeepAliveConfig struct {
	Time                  time.Duration `yaml:"time"`
	Timeout               time.Duration `yaml:"timeout"`
	MaxConnectionIdle     time.Duration `yaml:"max_connection_idle"`
	MaxConnectionAge      time.Duration `yaml:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `yaml:"max_connection_age_grace"`
}

type ClientKeepAliveConfig struct {
	MinTime             time.Duration `yaml:"min_time"`
	PermitWithoutStream bool          `yaml:"permit_without_stream"`
}

func (c *Config) parse() (serverconfig.Config, error) {
	grpcConf, err := c.parseGRPCConfig()
	if err != nil {
		return serverconfig.Config{}, fmt.Errorf("parse grpc server config: %w", err)
	}

	healthEnabled := c.Health.Enabled != nil && *c.Health.Enabled

	return serverconfig.Config{
		Transports: c.Fujin.Transports,
		GRPC:       grpcConf,
		Health: serverconfig.HealthConfig{
			Enabled: healthEnabled,
			Addr:    c.Health.Addr,
		},
		Connectors: c.Connectors,
	}, nil
}

func (c *Config) parseGRPCConfig() (serverconfig.GRPCServerConfig, error) {
	if c == nil {
		return serverconfig.GRPCServerConfig{}, ErrNilConfig
	}

	grpcEnabled := c.GRPC.Enabled == nil || *c.GRPC.Enabled
	if !grpcEnabled {
		return serverconfig.GRPCServerConfig{
			Enabled: false,
		}, nil
	}

	err := c.GRPC.TLS.Parse()
	if err != nil {
		return serverconfig.GRPCServerConfig{}, fmt.Errorf("parse tls conf: %w", err)
	}

	obsEnabled := c.GRPC.ObservabilityEnabled != nil && *c.GRPC.ObservabilityEnabled
	return serverconfig.GRPCServerConfig{
		Enabled:               grpcEnabled,
		Addr:                  c.GRPC.Addr,
		ConnectionTimeout:     c.GRPC.ConnectionTimeout,
		MaxConcurrentStreams:  c.GRPC.MaxConcurrentStreams,
		MaxRecvMsgSize:        c.GRPC.MaxRecvMsgSize,
		MaxSendMsgSize:        c.GRPC.MaxSendMsgSize,
		InitialWindowSize:     c.GRPC.InitialWindowSize,
		InitialConnWindowSize: c.GRPC.InitialConnWindowSize,
		ServerKeepAlive:       c.GRPC.ServerKeepAlive.parse(),
		ClientKeepAlive:       c.GRPC.ClientKeepAlive.parse(),
		TLS:                   c.GRPC.TLS.Config,
		ObservabilityEnabled:  obsEnabled,
	}, nil
}

func (c *ServerKeepAliveConfig) parse() serverconfig.ServerKeepAliveConfig {
	return serverconfig.ServerKeepAliveConfig{
		Time:                  c.Time,
		Timeout:               c.Timeout,
		MaxConnectionIdle:     c.MaxConnectionIdle,
		MaxConnectionAge:      c.MaxConnectionAge,
		MaxConnectionAgeGrace: c.MaxConnectionAgeGrace,
	}
}

func (c *ClientKeepAliveConfig) parse() serverconfig.ClientKeepAliveConfig {
	return serverconfig.ClientKeepAliveConfig{
		MinTime:             c.MinTime,
		PermitWithoutStream: c.PermitWithoutStream,
	}
}

type HealthConfig struct {
	Enabled *bool  `yaml:"enabled,omitempty"` // nil = false (default)
	Addr    string `yaml:"addr"`
}

var (
	Version string
	conf    Config
)

// ShutdownSignals returns the platform-appropriate OS signals for graceful shutdown.
func ShutdownSignals() []os.Signal { return shutdownSignals }

func RunCLI(ctx context.Context) {
	log.Printf("version: %s", Version)

	loader, err := loadConfig(ctx, &conf)
	if err != nil {
		log.Fatal(err)
	}
	serverConf, err := conf.parse()
	if err != nil {
		log.Fatal(err)
	}

	logType := os.Getenv("FUJIN_LOG_TYPE")
	logLevelVar := &slog.LevelVar{}
	logLevelVar.Set(parseLogLevel(os.Getenv("FUJIN_LOG_LEVEL")))
	logger := configureLoggerWithLevelVar(logType, logLevelVar)

	logRegisteredPlugins(logger)

	bootstrapSnapshot, err := connectorBootstrapSnapshot(loader, conf.Connectors)
	if err != nil {
		logger.Error("connector bootstrap snapshot", "err", err)
		os.Exit(1)
	}

	// If in upgrade mode, request FDs from old process before creating server
	us, err := requestUpgradeFromOld(logger)
	if err != nil {
		logger.Error("upgrade request failed", "err", err)
		os.Exit(1)
	}

	s, err := server.NewServer(serverConf, logger)
	if err != nil {
		logger.Error("new server", "err", err)
		os.Exit(1)
	}
	controller, err := newConnectorRuntimeController(s, bootstrapSnapshot)
	if err != nil {
		logger.Error("new runtime connector controller", "err", err)
		os.Exit(1)
	}

	// Pass inherited FDs to server if in upgrade mode
	us.applyTo(s)

	// Runtime sources and listeners share one lifecycle but stop in a strict order:
	// settle runtime-source work before canceling the server and closing its catalog.
	lifecycleBase := context.WithoutCancel(ctx)
	serverCtx, serverCancel := context.WithCancel(lifecycleBase)
	defer serverCancel()
	runtimeCtx, runtimeCancel := context.WithCancel(lifecycleBase)
	defer runtimeCancel()
	startupComplete := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			serverCancel()
		case <-startupComplete:
		}
	}()

	shutdownRequested := make(chan struct{}, 1)
	requestShutdown := func() {
		select {
		case shutdownRequested <- struct{}{}:
		default:
		}
	}

	// Start control socket for future upgrades. A drain request follows the same
	// runtime-source-before-server shutdown order as process-signal cancellation.
	upgradeSettled := startUpgradeListener(serverCtx, s, requestShutdown, logger)

	// SIGHUP reuses the selected configurator instance (Unix only, no-op elsewhere).
	reloadSettled := startReloadLoop(runtimeCtx, loader, controller, logLevelVar, logger)

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- s.ListenAndServe(serverCtx)
	}()

	// Runtime connector watching starts only after every listener is ready.
	var watcherSettled <-chan struct{}
	ready := s.ReadyForConnections(30 * time.Second)
	if ctx.Err() != nil {
		serverCancel()
	}
	close(startupComplete)
	if ready && ctx.Err() == nil {
		if us != nil {
			signalOldProcessReady(us, logger)
		}
		if watcherDone := startConnectorWatcher(runtimeCtx, loader, controller); watcherDone != nil {
			settled := make(chan struct{})
			watcherSettled = settled
			go func() {
				defer close(settled)
				err := <-watcherDone
				if runtimeCtx.Err() != nil || errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					logger.Error("connector watcher terminated", "err", err)
				} else {
					logger.Warn("connector watcher terminated")
				}
			}()
		}
	} else if ctx.Err() == nil {
		logger.Error("server did not become ready in time")
	}

	var serveErr error
	serverStopped := false
	select {
	case serveErr = <-serveDone:
		serverStopped = true
	case <-ctx.Done():
	case <-shutdownRequested:
	}

	stopRuntimeSources(runtimeCancel, watcherSettled, reloadSettled)
	serverCancel()
	waitForSettlement(upgradeSettled)
	if !serverStopped {
		serveErr = <-serveDone
	}
	if serveErr != nil {
		logger.Error("listen and serve", "err", serveErr)
	}
}

func stopRuntimeSources(cancel context.CancelFunc, settled ...<-chan struct{}) {
	cancel()
	waitForSettlement(settled...)
}

func waitForSettlement(channels ...<-chan struct{}) {
	for _, settled := range channels {
		if settled != nil {
			<-settled
		}
	}
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func configureLoggerWithLevelVar(logType string, level *slog.LevelVar) *slog.Logger {
	var handler slog.Handler
	switch strings.ToLower(logType) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		})
	}

	return slog.New(handler)
}

func configureLogger(logLevel, logType string) *slog.Logger {
	level := &slog.LevelVar{}
	level.Set(parseLogLevel(logLevel))
	return configureLoggerWithLevelVar(logType, level)
}

// loadConfig constructs the selected configurator once and retains it for runtime use.
func loadConfig(ctx context.Context, cfg *Config) (configurator.Configurator, error) {
	loaderType := os.Getenv("FUJIN_CONFIGURATOR")
	if loaderType == "" {
		return nil, errors.New("failed to load config: configurator not specified")
	}
	loader, err := loadConfigWithLoader(ctx, loaderType, cfg)
	if err != nil {
		return nil, fmt.Errorf("load config from env: loader type %s: %w", loaderType, err)
	}
	log.Printf("loaded config using configurator from environment: %s\n", loaderType)
	return loader, nil
}

// loadConfigWithLoader constructs one configurator and loads bootstrap configuration with it.
func loadConfigWithLoader(ctx context.Context, loaderType string, cfg *Config) (configurator.Configurator, error) {
	factory, ok := configurator.Get(loaderType)
	if !ok {
		return nil, fmt.Errorf("configurator %q not found (available: %v)", loaderType, configurator.List())
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	loader, err := factory(logger)
	if err != nil {
		return nil, fmt.Errorf("create configurator: %w", err)
	}
	if err := loader.Load(ctx, cfg); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return loader, nil
}

// logRegisteredPlugins logs all registered connectors, connector middlewares, bind middlewares, and configurators
func logRegisteredPlugins(l *slog.Logger) {
	transports := transport.List()
	connectors := connector.List()
	cmws := cmw.List()
	bmws := bmw.List()
	configurators := configurator.List()

	sort.Strings(transports)
	sort.Strings(connectors)
	sort.Strings(cmws)
	sort.Strings(bmws)
	sort.Strings(configurators)

	if len(transports) > 0 {
		l.Info("registered transports", "list", strings.Join(transports, ", "))
	} else {
		l.Warn("no transports registered")
	}

	if len(connectors) > 0 {
		l.Info("registered connectors", "list", strings.Join(connectors, ", "))
	} else {
		l.Warn("no connectors registered")
	}

	if len(cmws) > 0 {
		l.Info("registered connector middlewares", "list", strings.Join(cmws, ", "))
	} else {
		l.Warn("no connector middlewares registered")
	}

	if len(bmws) > 0 {
		l.Info("registered bind middlewares", "list", strings.Join(bmws, ", "))
	} else {
		l.Warn("no bind middlewares registered")
	}

	if len(configurators) > 0 {
		l.Info("registered configurators", "list", strings.Join(configurators, ", "))
	} else {
		l.Warn("no configurators registered")
	}
}
