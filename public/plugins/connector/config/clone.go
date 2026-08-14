package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// CloneConnectorConfig returns a deep copy suitable for immutable generation storage
// or caller-owned mutation.
func CloneConnectorConfig(original ConnectorConfig) (ConnectorConfig, error) {
	data, err := yaml.Marshal(original)
	if err != nil {
		return ConnectorConfig{}, fmt.Errorf("marshal connector config: %w", err)
	}
	var clone ConnectorConfig
	if err := yaml.Unmarshal(data, &clone); err != nil {
		return ConnectorConfig{}, fmt.Errorf("unmarshal connector config: %w", err)
	}
	return clone, nil
}
