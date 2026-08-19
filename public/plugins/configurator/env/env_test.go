package env

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// testConfig mirrors a simplified version of the real config structure.
type testConfig struct {
	Fujin struct {
		Transports []struct {
			Type    string `yaml:"type" json:"type"`
			Enabled *bool  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
		} `yaml:"transports" json:"transports"`
	} `yaml:"fujin" json:"fujin"`
	GRPC struct {
		Enabled *bool  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
		Addr    string `yaml:"addr" json:"addr"`
	} `yaml:"grpc" json:"grpc"`
	Health struct {
		Enabled *bool  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
		Addr    string `yaml:"addr" json:"addr"`
	} `yaml:"health" json:"health"`
	Connectors map[string]struct {
		Type string `yaml:"type" json:"type"`
	} `yaml:"connectors" json:"connectors"`
}

func TestLoad_JSON(t *testing.T) {
	jsonConfig := `{
		"fujin": {"transports": [{"type": "tcp"}]},
		"grpc": {"enabled": true, "addr": ":4849"},
		"connectors": {"my_conn": {"type": "kafka_franz"}}
	}`
	t.Setenv(envVarName, jsonConfig)

	loader, err := newEnvConfigurator(testLogger())
	if err != nil {
		t.Fatalf("newEnvConfigurator: %v", err)
	}

	var cfg testConfig
	if err := loader.Load(context.Background(), &cfg); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Fujin.Transports) != 1 || cfg.Fujin.Transports[0].Type != "tcp" {
		t.Fatalf("expected 1 tcp transport, got: %+v", cfg.Fujin.Transports)
	}
	if cfg.GRPC.Addr != ":4849" {
		t.Fatalf("expected grpc addr :4849, got: %s", cfg.GRPC.Addr)
	}
	if cfg.Connectors["my_conn"].Type != "kafka_franz" {
		t.Fatalf("expected connector type kafka_franz, got: %s", cfg.Connectors["my_conn"].Type)
	}
}

func TestLoad_YAML(t *testing.T) {
	yamlConfig := `
fujin:
  transports:
    - type: tcp
grpc:
  enabled: false
  addr: ":4849"
health:
  enabled: true
  addr: ":8080"
connectors:
  my_conn:
    type: nats_core
`
	t.Setenv(envVarName, yamlConfig)

	loader, err := newEnvConfigurator(testLogger())
	if err != nil {
		t.Fatalf("newEnvConfigurator: %v", err)
	}

	var cfg testConfig
	if err := loader.Load(context.Background(), &cfg); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Fujin.Transports) != 1 || cfg.Fujin.Transports[0].Type != "tcp" {
		t.Fatalf("expected 1 tcp transport, got: %+v", cfg.Fujin.Transports)
	}
	if cfg.GRPC.Addr != ":4849" {
		t.Fatalf("expected grpc addr :4849, got: %s", cfg.GRPC.Addr)
	}
	if *cfg.GRPC.Enabled {
		t.Fatal("expected grpc disabled")
	}
	if !*cfg.Health.Enabled {
		t.Fatal("expected health enabled")
	}
	if cfg.Health.Addr != ":8080" {
		t.Fatalf("expected health addr :8080, got: %s", cfg.Health.Addr)
	}
	if cfg.Connectors["my_conn"].Type != "nats_core" {
		t.Fatalf("expected connector type nats_core, got: %s", cfg.Connectors["my_conn"].Type)
	}
}

func TestLoad_EmptyEnvVar(t *testing.T) {
	t.Setenv(envVarName, "")

	loader, err := newEnvConfigurator(testLogger())
	if err != nil {
		t.Fatalf("newEnvConfigurator: %v", err)
	}

	var cfg testConfig
	if err := loader.Load(context.Background(), &cfg); err == nil {
		t.Fatal("expected error when env var is empty")
	}
}

func TestLoad_UnsetEnvVar(t *testing.T) {
	os.Unsetenv(envVarName)

	loader, err := newEnvConfigurator(testLogger())
	if err != nil {
		t.Fatalf("newEnvConfigurator: %v", err)
	}

	var cfg testConfig
	if err := loader.Load(context.Background(), &cfg); err == nil {
		t.Fatal("expected error when env var is not set")
	}
}

func TestLoad_InvalidContent(t *testing.T) {
	t.Setenv(envVarName, "{{{{not valid yaml or json")

	loader, err := newEnvConfigurator(testLogger())
	if err != nil {
		t.Fatalf("newEnvConfigurator: %v", err)
	}

	var cfg testConfig
	if err := loader.Load(context.Background(), &cfg); err == nil {
		t.Fatal("expected error for invalid content")
	}
}
