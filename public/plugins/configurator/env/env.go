package env

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/fujin-io/fujin/public/plugins/configurator"
	"gopkg.in/yaml.v3"
)

const envVarName = "FUJIN_CONFIGURATOR_ENV_CONFIG"

func init() {
	if err := configurator.Register("env", newEnvConfigurator); err != nil {
		panic(fmt.Sprintf("register env configurator: %v", err))
	}
}

type envConfigurator struct {
	l *slog.Logger
}

func newEnvConfigurator(l *slog.Logger) (configurator.Configurator, error) {
	return &envConfigurator{
		l: l.With("config_loader", "env"),
	}, nil
}

// Load loads configuration from the FUJIN_CONFIGURATOR_ENV_CONFIG environment variable.
// Supports both JSON and YAML formats (auto-detected).
func (e *envConfigurator) Load(ctx context.Context, cfg any) error {
	data := os.Getenv(envVarName)
	if data == "" {
		return fmt.Errorf("env configurator: %s environment variable is not set or empty", envVarName)
	}

	e.l.Info("loading config from environment variable", "var", envVarName)

	// Try JSON first.
	if err := json.Unmarshal([]byte(data), cfg); err == nil {
		return nil
	}

	// Fall back to YAML.
	if err := yaml.Unmarshal([]byte(data), cfg); err != nil {
		return fmt.Errorf("env configurator: failed to parse %s as JSON or YAML: %w", envVarName, err)
	}

	return nil
}
