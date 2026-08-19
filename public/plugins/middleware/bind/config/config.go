package config

// Config represents a connector middleware configuration in YAML.
type Config struct {
	// Name is the name of the bind middleware plugin to use.
	Name string `yaml:"name"`

	// Enabled allows enabling/disabling a middleware without removing it. nil = true (enabled by default)
	Enabled *bool `yaml:"enabled,omitempty"`

	// Config is the plugin-specific configuration.
	// The structure depends on the middleware plugin type.
	Config map[string]any `yaml:",inline"`
}
