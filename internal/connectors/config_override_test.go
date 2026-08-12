package connectors

import (
	"testing"
	"time"

	"github.com/fujin-io/fujin/public/plugins/connector/config"
)

func TestDeepCopyConfig(t *testing.T) {
	original := config.ConnectorConfig{
		Type: "test",
		Settings: map[string]any{
			"nested": map[string]any{
				"nested1": map[string]any{
					"key1": "value1",
				},
			},
		},
	}

	copy, err := DeepCopyConfig(original)
	if err != nil {
		t.Fatalf("DeepCopyConfig() error = %v", err)
	}

	// Modify copy
	copy.Settings.(map[string]any)["nested"].(map[string]any)["nested1"].(map[string]any)["key1"] = "modified"

	// Original should not be modified
	if original.Settings.(map[string]any)["nested"].(map[string]any)["nested1"].(map[string]any)["key1"] == "modified" {
		t.Error("DeepCopyConfig() modified original config")
	}
}

func TestApplyOverrides_Generic(t *testing.T) {
	originalConfig := config.ConnectorConfig{
		Type:        "test",
		Overridable: []string{"key"}, // Allow override
		Settings: map[string]any{
			"key": "value",
		},
	}

	overrides := map[string]string{
		"key": "new-value1",
	}

	result, err := ApplyOverrides(originalConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	// Check overrides
	baseSettings := result.Settings.(map[string]any)
	if baseSettings["key"] != "new-value1" {
		t.Errorf("ApplyOverrides() key = %v, want new-value1", baseSettings["key"])
	}
}

func TestApplyOverrides_NestedPath(t *testing.T) {
	originalConfig := config.ConnectorConfig{
		Type:        "test",
		Overridable: []string{"nested.key", "nested.key2"}, // Allow specific nested paths
		Settings: map[string]any{
			"nested": map[string]any{
				"key":  "value",
				"key2": "value2",
			},
		},
	}

	overrides := map[string]string{
		"nested.key":  "value2",
		"nested.key2": "value",
	}

	result, err := ApplyOverrides(originalConfig, overrides)
	if err != nil {
		t.Fatalf("ApplyOverrides() error = %v", err)
	}

	settings := result.Settings.(map[string]any)["nested"].(map[string]any)
	if settings["key"] != "value2" {
		t.Errorf("ApplyOverrides() nested.key = %v, want value2", settings["key"])
	}
	if settings["key2"] != "value" {
		t.Errorf("ApplyOverrides() nested.key = %v, want value", settings["key2"])
	}
}

func TestValidateOverridePath(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		whitelist []string
		wantErr   bool
	}{
		{
			name:      "exact match",
			path:      "routes.route1.topic",
			whitelist: []string{"routes.route1.topic"},
			wantErr:   false,
		},
		{
			name:      "wildcard match",
			path:      "routes.route1.topic",
			whitelist: []string{"routes.*.topic"},
			wantErr:   false,
		},
		{
			name:      "wildcard match different route",
			path:      "routes.route2.topic",
			whitelist: []string{"routes.*.topic"},
			wantErr:   false,
		},
		{
			name:      "wildcard at end",
			path:      "common.servers",
			whitelist: []string{"common.*"},
			wantErr:   false,
		},
		{
			name:      "not in whitelist",
			path:      "common.servers",
			whitelist: []string{"routes.*.topic"},
			wantErr:   true,
		},
		{
			name:      "empty whitelist",
			path:      "routes.route1.topic",
			whitelist: []string{},
			wantErr:   true,
		},
		{
			name:      "nil whitelist",
			path:      "routes.route1.topic",
			whitelist: nil,
			wantErr:   true,
		},
		{
			name:      "allow all with *",
			path:      "routes.route1.topic",
			whitelist: []string{"*"},
			wantErr:   false,
		},
		{
			name:      "allow all with * - any path",
			path:      "common.servers",
			whitelist: []string{"*"},
			wantErr:   false,
		},
		{
			name:      "allow all with * - deeply nested",
			path:      "some.very.deep.nested.path",
			whitelist: []string{"*"},
			wantErr:   false,
		},
		{
			name:      "path length mismatch - shorter",
			path:      "routes.route1",
			whitelist: []string{"routes.*.topic"},
			wantErr:   true,
		},
		{
			name:      "path length mismatch - longer",
			path:      "routes.route1.topic.extra",
			whitelist: []string{"routes.*.topic"},
			wantErr:   true,
		},
		{
			name:      "multiple patterns - first matches",
			path:      "routes.route1.topic",
			whitelist: []string{"routes.*.topic", "common.*"},
			wantErr:   false,
		},
		{
			name:      "multiple patterns - second matches",
			path:      "common.servers",
			whitelist: []string{"routes.*.topic", "common.*"},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOverridePath(tt.path, tt.whitelist)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateOverridePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMatchOverridePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		pattern string
		want    bool
	}{
		{"exact match", "a.b.c", "a.b.c", true},
		{"wildcard in middle", "a.x.c", "a.*.c", true},
		{"wildcard at end", "a.b.x", "a.b.*", true},
		{"wildcard at start", "x.b.c", "*.b.c", true},
		{"multiple wildcards", "a.x.y", "a.*.*", true},
		{"no match - different segment", "a.b.c", "a.b.d", false},
		{"no match - different length", "a.b", "a.b.c", false},
		{"no match - longer path", "a.b.c.d", "a.b.c", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchOverridePath(tt.path, tt.pattern); got != tt.want {
				t.Errorf("matchOverridePath(%q, %q) = %v, want %v", tt.path, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestApplyOverrides_NotAllowed(t *testing.T) {
	originalConfig := config.ConnectorConfig{
		Type:        "test",
		Overridable: []string{"routes.*.topic"}, // Only allow route topic.
		Settings: map[string]any{
			"common": map[string]any{
				"servers": []string{"host1:1234"},
			},
		},
	}

	// Try to override servers (not allowed).
	overrides := map[string]string{
		"common.servers": "host2:1234",
	}

	_, err := ApplyOverrides(originalConfig, overrides)
	if err == nil {
		t.Error("ApplyOverrides() should return error for non-allowed path")
	}
}

func TestSetNestedValueWithValue(t *testing.T) {
	m := make(map[string]any)

	// Test top-level setting
	if err := setNestedValueWithValue(m, "setting_key", "my-value"); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["setting_key"] != "my-value" {
		t.Errorf("setNestedValueWithValue() setting_key = %v, want my-value", m["setting_key"])
	}

	// Test nested setting
	if err := setNestedValueWithValue(m, "conn.addr", "localhost:1234"); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	conn, ok := m["conn"].(map[string]any)
	if !ok {
		t.Fatalf("setNestedValueWithValue() conn is not a map")
	}
	if conn["addr"] != "localhost:1234" {
		t.Errorf("setNestedValueWithValue() conn.addr = %v, want localhost:1234", conn["addr"])
	}

	// Test deeply nested
	if err := setNestedValueWithValue(m, "conn.tls.cert_path", "/path/to/cert"); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	tls, ok := conn["tls"].(map[string]any)
	if !ok {
		t.Fatalf("setNestedValueWithValue() conn.tls is not a map")
	}
	if tls["cert_path"] != "/path/to/cert" {
		t.Errorf("setNestedValueWithValue() conn.tls.cert_path = %v, want /path/to/cert", tls["cert_path"])
	}

	// Test with different types
	if err := setNestedValueWithValue(m, "count", 42); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["count"] != 42 {
		t.Errorf("setNestedValueWithValue() count = %v, want 42", m["count"])
	}

	if err := setNestedValueWithValue(m, "enabled", true); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["enabled"] != true {
		t.Errorf("setNestedValueWithValue() enabled = %v, want true", m["enabled"])
	}

	if err := setNestedValueWithValue(m, "duration", 5*time.Second); err != nil {
		t.Fatalf("setNestedValueWithValue() error = %v", err)
	}
	if m["duration"] != 5*time.Second {
		t.Errorf("setNestedValueWithValue() duration = %v, want 5s", m["duration"])
	}
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantType  string
		wantValue any
	}{
		{
			name:      "boolean true",
			input:     "true",
			wantType:  "bool",
			wantValue: true,
		},
		{
			name:      "boolean false",
			input:     "false",
			wantType:  "bool",
			wantValue: false,
		},
		{
			name:      "integer",
			input:     "42",
			wantType:  "int",
			wantValue: 42,
		},
		{
			name:      "float",
			input:     "3.14",
			wantType:  "float64",
			wantValue: 3.14,
		},
		{
			name:      "duration",
			input:     "5s",
			wantType:  "duration",
			wantValue: 5 * time.Second,
		},
		{
			name:      "comma-separated strings",
			input:     "a,b,c",
			wantType:  "[]string",
			wantValue: []string{"a", "b", "c"},
		},
		{
			name:      "string fallback",
			input:     "just a string",
			wantType:  "string",
			wantValue: "just a string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.input)
			if err != nil {
				t.Errorf("convertValue() error = %v", err)
				return
			}

			switch tt.wantType {
			case "bool":
				if b, ok := result.(bool); !ok || b != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (bool)", result, result, tt.wantValue)
				}
			case "int":
				if i, ok := result.(int); !ok || i != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (int)", result, result, tt.wantValue)
				}
			case "float64":
				if f, ok := result.(float64); !ok || f != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (float64)", result, result, tt.wantValue)
				}
			case "duration":
				if d, ok := result.(time.Duration); !ok || d != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (time.Duration)", result, result, tt.wantValue)
				}
			case "[]string":
				if arr, ok := result.([]string); !ok {
					t.Errorf("convertValue() = %v (%T), want []string", result, result)
				} else {
					wantArr := tt.wantValue.([]string)
					if len(arr) != len(wantArr) {
						t.Errorf("convertValue() []string length = %d, want %d", len(arr), len(wantArr))
					} else {
						for i, v := range arr {
							if v != wantArr[i] {
								t.Errorf("convertValue() []string[%d] = %v, want %v", i, v, wantArr[i])
							}
						}
					}
				}
			case "string":
				if s, ok := result.(string); !ok || s != tt.wantValue {
					t.Errorf("convertValue() = %v (%T), want %v (string)", result, result, tt.wantValue)
				}
			}
		})
	}
}
