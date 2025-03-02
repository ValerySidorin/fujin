package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ValerySidorin/fujin/internal/api/fujin"
	"github.com/ValerySidorin/fujin/internal/config"
	"github.com/ValerySidorin/fujin/internal/mq"
	_ "go.uber.org/automaxprocs"
	"gopkg.in/yaml.v3"
)

var (
	Commit string
)

func main() {
	if len(os.Args) > 2 {
		log.Fatal("invalid args")
	}
	confPath := ""
	if len(os.Args) == 2 {
		confPath = os.Args[1]
	}
	var conf config.Config
	if err := loadConfig(confPath, &conf); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	logLevel := parseLogLevel(conf.Log.Level)
	var logger *slog.Logger
	if conf.Log.Type != "json" && conf.Log.Type != "text" {
		conf.Log.Type = "text"
	}
	switch conf.Log.Type {
	case "json":
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: logLevel,
		}))
	default:
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: logLevel,
		}))
	}

	logger.Info("starting fujin server")
	logger.Info(fmt.Sprintf("commit: %s", Commit))

	quicServerConf, err := conf.ParseQUICServerConfig()
	if err != nil {
		logger.Error(fmt.Errorf("parse server conf: %w", err).Error())
		os.Exit(1)
	}

	mqman := mq.NewMQManager(conf.MQ, logger)
	defer mqman.Close()

	quicServer := fujin.NewServer(*quicServerConf, mqman, logger)

	if err := quicServer.ListenAndServe(ctx); err != nil {
		logger.Error(fmt.Errorf("quic listen and serve: %w", err).Error())
	}
}

func parseLogLevel(name string) slog.Level {
	switch strings.ToUpper(name) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func loadConfig(filePath string, cfg *config.Config) error {
	paths := []string{}

	if filePath == "" {
		paths = append(paths, "./config.yaml", "conf/config.yaml", "config/config.yaml")
	} else {
		paths = append(paths, filePath)
	}

	for _, p := range paths {
		f, err := os.Open(p)
		if err == nil {
			defer f.Close()
			log.Printf("found config file in: %s\n", p)
			data, err := io.ReadAll(f)
			if err != nil {
				return fmt.Errorf("read config: %w", err)
			}

			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return fmt.Errorf("unmarshal config: %w", err)
			}

			for _, c := range cfg.MQ.Readers {
				switch c.Protocol {
				// no reusable readers for now
				case "kafka":
					c.Reusable = false
				default:
					c.Reusable = false
				}
			}

			cfg.SetDefaults()
			return nil
		}
	}

	return fmt.Errorf("failed to find config in: %v", paths)
}
