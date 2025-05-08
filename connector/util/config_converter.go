package util

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func ConvertConfig(rawBrokerConfig any, output any) error {
	yamlBytes, err := yaml.Marshal(rawBrokerConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal raw broker config: %w", err)
	}
	err = yaml.Unmarshal(yamlBytes, output)
	if err != nil {
		return fmt.Errorf("failed to unmarshal to target config struct (%T): %w", output, err)
	}
	return nil
}
