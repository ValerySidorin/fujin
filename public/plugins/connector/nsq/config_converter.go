package nsq

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedSettings contains settings that can be overridden at runtime
var allowedSettings = map[string]bool{
	"max_in_flight":        true,
	"pool.size":            true,
	"pool.release_timeout": true,
}

// convertConfigValue converts and validates a configuration value for NSQ
func convertConfigValue(settingPath string, value string) (any, error) {
	// Normalize the path by removing the "routes.<name>." prefix when present.
	normalizedPath := normalizePath(settingPath)

	// Check if this setting is allowed
	if !allowedSettings[normalizedPath] {
		if strings.HasPrefix(settingPath, "tls.") || strings.Contains(settingPath, ".tls.") {
			return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime (TLS configuration is security critical)", settingPath)
		}
		return nil, fmt.Errorf("setting '%s' cannot be overridden at runtime", settingPath)
	}

	switch normalizedPath {
	case "max_in_flight", "pool.size":
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer format for '%s': %w", settingPath, err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("%s must be positive, got: %d", settingPath, i)
		}
		return i, nil

	case "pool.release_timeout":
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for '%s': %w (expected format: '10ms', '5s', '1h', etc.)", settingPath, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("%s must be non-negative, got: %v", settingPath, d)
		}
		return d, nil

	default:
		return nil, fmt.Errorf("setting '%s' is allowed but not yet implemented in the converter", settingPath)
	}
}

// normalizePath removes the "routes.<name>." prefix from a setting path.
func normalizePath(fullPath string) string {
	if strings.HasPrefix(fullPath, "routes.") {
		parts := strings.SplitN(fullPath, ".", 3)
		if len(parts) >= 3 {
			return parts[2]
		}
	}
	return fullPath
}
