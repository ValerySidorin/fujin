package observability

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
	Path    string `yaml:"path"`
}

type TracingConfig struct {
	Enabled      bool           `yaml:"enabled"`
	OTLPEndpoint string         `yaml:"otlp_endpoint"`
	Insecure     bool           `yaml:"insecure"`
	SampleRatio  float64        `yaml:"sample_ratio"`
	Resource     ResourceConfig `yaml:"resource"`
}

type ResourceConfig struct {
	ServiceName    string `yaml:"service_name"`
	ServiceVersion string `yaml:"service_version"`
	Environment    string `yaml:"environment"`
}

type Config struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Tracing TracingConfig `yaml:"tracing"`
}
