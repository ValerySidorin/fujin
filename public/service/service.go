package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ValerySidorin/fujin/internal/observability"
	pconfig "github.com/ValerySidorin/fujin/public/config"
	"github.com/ValerySidorin/fujin/public/connectors"
	"github.com/ValerySidorin/fujin/public/server"
	"github.com/ValerySidorin/fujin/public/server/config"
	"github.com/quic-go/quic-go"
	"gopkg.in/yaml.v3"
)

var (
	ErrNilConfig = errors.New("nil config")
)

type Config struct {
	Fujin         FujinConfig          `yaml:"fujin"`
	GRPC          GRPCConfig           `yaml:"grpc"`
	Connectors    connectors.Config    `yaml:"connectors"`
	Observability observability.Config `yaml:"observability"`
}

type FujinConfig struct {
	Enabled               bool              `yaml:"enabled"`
	Addr                  string            `yaml:"addr"`
	WriteDeadline         time.Duration     `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration     `yaml:"force_terminate_timeout"`
	PingInterval          time.Duration     `yaml:"ping_interval"`
	PingTimeout           time.Duration     `yaml:"ping_timeout"`
	PingStream            bool              `yaml:"ping_stream"`
	PingMaxRetries        int               `yaml:"ping_max_retries"`
	TLS                   pconfig.TLSConfig `yaml:"tls"`
	QUIC                  QUICConfig        `yaml:"quic"`
}

type GRPCConfig struct {
	Enabled              bool              `yaml:"enabled"`
	Addr                 string            `yaml:"addr"`
	ConnectionTimeout    time.Duration     `yaml:"connection_timeout"`
	MaxConcurrentStreams uint32            `yaml:"max_concurrent_streams"`
	TLS                  pconfig.TLSConfig `yaml:"tls"`
}

type QUICConfig struct {
	MaxIncomingStreams   int64         `yaml:"max_incoming_streams"`
	KeepAlivePeriod      time.Duration `yaml:"keepalive_period"`
	HandshakeIdleTimeout time.Duration `yaml:"handshake_idle_timeout"`
	MaxIdleTimeout       time.Duration `yaml:"max_idle_timeout"`
}

func (c *Config) parse() (config.Config, error) {
	var (
		fujinConf config.FujinServerConfig
		grpcConf  config.GRPCServerConfig
		err       error
	)

	fujinConf, err = c.parseFujinServerConfig()
	if err != nil {
		return config.Config{}, fmt.Errorf("parse fujin server config: %w", err)
	}

	grpcConf, err = c.parseGRPCConfig()
	if err != nil {
		return config.Config{}, fmt.Errorf("parse grpc server config: %w", err)
	}

	if err := c.Connectors.Validate(); err != nil {
		return config.Config{}, fmt.Errorf("validate connectors config: %w", err)
	}

	return config.Config{
		Fujin:         fujinConf,
		GRPC:          grpcConf,
		Connectors:    c.Connectors,
		Observability: c.Observability,
	}, nil
}

func (c *Config) parseFujinServerConfig() (config.FujinServerConfig, error) {
	if c == nil {
		return config.FujinServerConfig{}, ErrNilConfig
	}

	if !c.Fujin.Enabled {
		return config.FujinServerConfig{
			Enabled: c.Fujin.Enabled,
		}, nil
	}

	err := c.Fujin.TLS.Parse()
	if err != nil {
		return config.FujinServerConfig{}, fmt.Errorf("parse tls conf: %w", err)
	}

	return config.FujinServerConfig{
		Enabled:               c.Fujin.Enabled,
		Addr:                  c.Fujin.Addr,
		WriteDeadline:         c.Fujin.WriteDeadline,
		ForceTerminateTimeout: c.Fujin.ForceTerminateTimeout,
		PingInterval:          c.Fujin.PingInterval,
		PingTimeout:           c.Fujin.PingTimeout,
		PingStream:            c.Fujin.PingStream,
		PingMaxRetries:        c.Fujin.PingMaxRetries,
		TLS:                   c.Fujin.TLS.Config,
		QUIC:                  c.Fujin.QUIC.parse(),
	}, nil
}

func (c *Config) parseGRPCConfig() (config.GRPCServerConfig, error) {
	if c == nil {
		return config.GRPCServerConfig{}, ErrNilConfig
	}

	if !c.GRPC.Enabled {
		return config.GRPCServerConfig{
			Enabled: c.GRPC.Enabled,
		}, nil
	}

	err := c.GRPC.TLS.Parse()
	if err != nil {
		return config.GRPCServerConfig{}, fmt.Errorf("parse tls conf: %w", err)
	}

	return config.GRPCServerConfig{
		Enabled:              c.GRPC.Enabled,
		Addr:                 c.GRPC.Addr,
		ConnectionTimeout:    c.GRPC.ConnectionTimeout,
		MaxConcurrentStreams: c.GRPC.MaxConcurrentStreams,
		TLS:                  c.GRPC.TLS.Config,
	}, nil
}

func (c *QUICConfig) parse() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:   c.MaxIncomingStreams,
		KeepAlivePeriod:      c.KeepAlivePeriod,
		HandshakeIdleTimeout: c.HandshakeIdleTimeout,
		MaxIdleTimeout:       c.MaxIdleTimeout,
	}
}

var (
	Version string
	conf    Config
)

func RunCLI(ctx context.Context) {
	log.Printf("version: %s", Version)

	if len(os.Args) > 2 {
		log.Fatal("invalid args")
	}
	confPath := ""
	if len(os.Args) == 2 {
		confPath = os.Args[1]
	}

	if err := loadConfig(confPath, &conf); err != nil {
		log.Fatal(err)
	}
	serverConf, err := conf.parse()
	if err != nil {
		log.Fatal(err)
	}

	logLevel := os.Getenv("FUJIN_LOG_LEVEL")
	logType := os.Getenv("FUJIN_LOG_TYPE")
	logger := configureLogger(logLevel, logType)

	s, err := server.NewServer(serverConf, logger)
	if err != nil {
		logger.Error("new server", "err", err)
		os.Exit(1)
	}

	if err := s.ListenAndServe(ctx); err != nil {
		logger.Error("listen and serve", "err", err)
	}
}

func configureLogger(logLevel, logType string) *slog.Logger {
	var parsedLogLevel slog.Level
	switch strings.ToUpper(logLevel) {
	case "DEBUG":
		parsedLogLevel = slog.LevelDebug
	case "WARN":
		parsedLogLevel = slog.LevelWarn
	case "ERROR":
		parsedLogLevel = slog.LevelError
	default:
		parsedLogLevel = slog.LevelInfo
	}

	var handler slog.Handler
	switch strings.ToLower(logType) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: parsedLogLevel,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: parsedLogLevel,
		})
	}

	return slog.New(handler)
}

func loadConfig(filePath string, cfg *Config) error {
	paths := []string{}

	if filePath == "" {
		paths = append(paths, "./config.yaml", "conf/config.yaml", "config/config.yaml")
	} else {
		paths = append(paths, filePath)
	}

	for _, p := range paths {
		f, err := os.Open(p)
		if err == nil {
			log.Printf("reading config from: %s\n", p)
			data, err := io.ReadAll(f)
			f.Close()
			if err != nil {
				return fmt.Errorf("read config: %w", err)
			}

			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return fmt.Errorf("unmarshal config: %w", err)
			}

			return nil
		}
	}

	return fmt.Errorf("failed to find config in: %v", paths)
}
