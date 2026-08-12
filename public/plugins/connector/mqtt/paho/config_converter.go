package paho

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// allowedSettings contains settings that can be overridden at runtime
var allowedSettings = map[string]bool{
	// Common settings
	"common.keep_alive":         true,
	"common.disconnect_timeout": true,
	"common.connect_timeout":    true,
	// Client settings
	"qos":                  true,
	"retain":               true,
	"clean_start":          true,
	"session_expiry":       true,
	"send_acks_interval":   true,
	"ack_ttl":              true,
	"pool.size":            true,
	"pool.release_timeout": true,
}

// convertConfigValue converts and validates a configuration value for MQTT
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
	case "common.keep_alive":
		// Keep alive is in seconds (uint16)
		i, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid keep_alive format: %w (expected integer seconds)", err)
		}
		return uint16(i), nil

	case "common.disconnect_timeout", "common.connect_timeout", "send_acks_interval", "ack_ttl", "pool.release_timeout":
		d, err := time.ParseDuration(value)
		if err != nil {
			return nil, fmt.Errorf("invalid duration format for '%s': %w (expected format: '10ms', '5s', '1h', etc.)", settingPath, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("%s must be non-negative, got: %v", settingPath, d)
		}
		return d, nil

	case "qos":
		qos, err := strconv.ParseUint(value, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid QoS format: %w (expected 0, 1, or 2)", err)
		}
		if qos > 2 {
			return nil, fmt.Errorf("QoS must be 0, 1, or 2, got: %d", qos)
		}
		return byte(qos), nil

	case "retain", "clean_start":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid boolean format for '%s': %w (expected 'true' or 'false')", settingPath, err)
		}
		return b, nil

	case "session_expiry":
		// Session expiry is in seconds (uint32)
		i, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid session_expiry format: %w (expected integer seconds)", err)
		}
		return uint32(i), nil

	case "pool.size":
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid integer format for 'pool.size': %w", err)
		}
		if i <= 0 {
			return nil, fmt.Errorf("pool.size must be positive, got: %d", i)
		}
		return i, nil

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
