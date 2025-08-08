package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ValerySidorin/fujin/internal/server/fujin"
	"github.com/ValerySidorin/fujin/public/connectors"
	"github.com/ValerySidorin/fujin/public/server"
	"github.com/ValerySidorin/fujin/public/server/config"
	"github.com/quic-go/quic-go"
	"gopkg.in/yaml.v3"
)

var (
	ErrNilConfig                     = errors.New("nil config")
	ErrTLSClientCertsDirNotSpecified = errors.New("client certs dir not specified, while mTLS enabled")
	ErrTLSServerCertPathNotSpecified = errors.New("server cert path not specified")
	ErrTLSServerKeyPathNotSpecified  = errors.New("server cert path not specified")

	NextProtos = []string{"fujin"}
)

type Config struct {
	Fujin      FujinConfig       `yaml:"fujin"`
	Connectors connectors.Config `yaml:"connectors"`
}

type FujinConfig struct {
	Disabled              bool          `yaml:"disabled"`
	Addr                  string        `yaml:"addr"`
	WriteDeadline         time.Duration `yaml:"write_deadline"`
	ForceTerminateTimeout time.Duration `yaml:"force_terminate_timeout"`
	PingInterval          time.Duration `yaml:"ping_interval"`
	PingTimeout           time.Duration `yaml:"ping_timeout"`
	PingStream            bool          `yaml:"ping_stream"`
	PingMaxRetries        int           `yaml:"ping_max_retries"`
	TLS                   TLSConfig     `yaml:"tls"`
	QUIC                  QUICConfig    `yaml:"quic"`
}

type TLSConfig struct {
	ClientCertsDir    string `yaml:"client_certs_dir"`
	ServerCertPEMPath string `yaml:"server_cert_pem_path"`
	ServerKeyPEMPath  string `yaml:"server_key_pem_path"`
	MTLSEnabled       bool   `yaml:"mtls_enabled"`
}

type QUICConfig struct {
	MaxIncomingStreams   int64         `yaml:"max_incoming_streams"`
	KeepAlivePeriod      time.Duration `yaml:"keepalive_period"`
	HandshakeIdleTimeout time.Duration `yaml:"handshake_idle_timeout"`
	MaxIdleTimeout       time.Duration `yaml:"max_idle_timeout"`
}

func (c *Config) parse() (config.Config, error) {
	var (
		fujinConf fujin.ServerConfig
		err       error
	)

	if !c.Fujin.Disabled {
		fujinConf, err = c.parseFujinServerConfig()
		if err != nil {
			return config.Config{}, fmt.Errorf("parse fujin server config: %w", err)
		}
	}

	if err := c.Connectors.Validate(); err != nil {
		return config.Config{}, fmt.Errorf("validate connectors config: %w", err)
	}

	return config.Config{
		Fujin:      fujinConf,
		Connectors: c.Connectors,
	}, nil
}

func (c *Config) parseFujinServerConfig() (fujin.ServerConfig, error) {
	if c == nil {
		return fujin.ServerConfig{}, ErrNilConfig
	}

	tlsConf, err := c.Fujin.TLS.parse()
	if err != nil {
		return fujin.ServerConfig{}, fmt.Errorf("parse TLS conf: %w", err)
	}

	return fujin.ServerConfig{
		Disabled:              c.Fujin.Disabled,
		Addr:                  c.Fujin.Addr,
		WriteDeadline:         c.Fujin.WriteDeadline,
		ForceTerminateTimeout: c.Fujin.ForceTerminateTimeout,
		PingInterval:          c.Fujin.PingInterval,
		PingTimeout:           c.Fujin.PingTimeout,
		PingStream:            c.Fujin.PingStream,
		PingMaxRetries:        c.Fujin.PingMaxRetries,
		TLS:                   tlsConf,
		QUIC:                  c.Fujin.QUIC.parse(),
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

func (c *TLSConfig) validate() error {
	if c.ClientCertsDir == "" && c.MTLSEnabled {
		return ErrTLSClientCertsDirNotSpecified
	}

	if c.ServerCertPEMPath == "" {
		return ErrTLSServerCertPathNotSpecified
	}

	if c.ServerKeyPEMPath == "" {
		return ErrTLSServerKeyPathNotSpecified
	}

	return nil
}

func (c *TLSConfig) parse() (*tls.Config, error) {
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	caCertPool := x509.NewCertPool()

	if c.ClientCertsDir != "" {
		clientCAs, err := os.ReadDir(c.ClientCertsDir)
		if err != nil {
			return nil, fmt.Errorf("read client CAs dir: %w", err)
		}

		for _, certEntry := range clientCAs {
			if !certEntry.IsDir() {
				cert, err := os.ReadFile(filepath.Join(c.ClientCertsDir, certEntry.Name()))
				if err != nil {
					return nil, fmt.Errorf("read client CA: %w", err)
				}
				caCertPool.AppendCertsFromPEM(cert)
			}
		}
	}

	cert, err := tls.LoadX509KeyPair(c.ServerCertPEMPath, c.ServerKeyPEMPath)
	if err != nil {
		return nil, fmt.Errorf("load x509 key pair: %w", err)
	}

	clientAuth := tls.NoClientCert
	if c.MTLSEnabled {
		clientAuth = tls.RequireAndVerifyClientCert
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   clientAuth,
		NextProtos:   NextProtos,
	}, nil
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
