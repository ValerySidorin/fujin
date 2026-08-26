package fujin

// RuntimeConfig is one complete Fujin bootstrap document.
type RuntimeConfig struct {
	Fujin      FujinConfig                `json:"fujin,omitempty"`
	GRPC       *GRPCConfig                `json:"grpc,omitempty"`
	Health     HealthConfig               `json:"health,omitempty"`
	Connectors map[string]ConnectorConfig `json:"connectors,omitempty"`
}

// FujinConfig configures native protocol transports.
type FujinConfig struct {
	Transports []TransportConfig `json:"transports,omitempty"`
}

// TransportConfig configures one registered native transport plugin.
type TransportConfig struct {
	Type     string `json:"type"`
	Enabled  *bool  `json:"enabled,omitempty"`
	Settings any    `json:"settings,omitempty"`
}

// GRPCConfig configures the distinct protobuf gRPC interface.
type GRPCConfig struct {
	Enabled                     bool   `json:"enabled"`
	Addr                        string `json:"addr,omitempty"`
	Timeout                     string `json:"timeout,omitempty"`
	MaxConcurrentStreams        uint32 `json:"max_concurrent_streams,omitempty"`
	MaxDecodingMessageSize      uint64 `json:"max_decoding_message_size,omitempty"`
	MaxEncodingMessageSize      uint64 `json:"max_encoding_message_size,omitempty"`
	InitialStreamWindowSize     uint32 `json:"initial_stream_window_size,omitempty"`
	InitialConnectionWindowSize uint32 `json:"initial_connection_window_size,omitempty"`
	HTTP2KeepaliveInterval      string `json:"http2_keepalive_interval,omitempty"`
	HTTP2KeepaliveTimeout       string `json:"http2_keepalive_timeout,omitempty"`
	HTTP2AdaptiveWindow         *bool  `json:"http2_adaptive_window,omitempty"`
	MaxConnectionAge            string `json:"max_connection_age,omitempty"`
	MaxConnectionAgeGrace       string `json:"max_connection_age_grace,omitempty"`
	TLS                         any    `json:"tls,omitempty"`
}

// HealthConfig configures the HTTP liveness and readiness listener.
type HealthConfig struct {
	Enabled bool   `json:"enabled"`
	Addr    string `json:"addr,omitempty"`
}

// ConnectorConfig configures one registered connector and its middleware.
type ConnectorConfig struct {
	Type                 string           `json:"type"`
	Overridable          []string         `json:"overridable,omitempty"`
	BindMiddlewares      []map[string]any `json:"bind_middlewares,omitempty"`
	ConnectorMiddlewares []map[string]any `json:"connector_middlewares,omitempty"`
	Settings             any              `json:"settings,omitempty"`
}

// ConnectorSnapshot is a complete runtime connector configuration at one revision.
type ConnectorSnapshot struct {
	Revision   uint64                     `json:"revision"`
	Connectors map[string]ConnectorConfig `json:"connectors"`
}

// Options controls application startup.
type Options struct {
	// Config, when nil, lets the generated library select its registered configurator.
	Config *RuntimeConfig
	// WorkerThreads sets the Tokio worker count. Zero uses the Fujin default.
	WorkerThreads int
	// RuntimeThread sets the embedded runtime thread name. Empty uses the Fujin default.
	RuntimeThread string
	// GracefulUpgrade explicitly enables Fujin's Unix binary-upgrade machinery.
	GracefulUpgrade bool
}

// Endpoint describes one listener that completed startup.
type Endpoint struct {
	Interface string  `json:"interface"`
	Transport *string `json:"transport"`
	Network   string  `json:"network"`
	Address   string  `json:"address"`
	Path      *string `json:"path"`
	TLS       bool    `json:"tls"`
}

// ApplyResult is the terminal result of a connector snapshot request.
type ApplyResult struct {
	Revision uint64  `json:"revision"`
	State    string  `json:"state"`
	Changed  bool    `json:"changed"`
	Error    *string `json:"error"`
}

// RuntimeStatus is a point-in-time view of runtime connector state.
type RuntimeStatus struct {
	BuildVersion           string        `json:"build_version"`
	ConnectorTypes         []string      `json:"connector_types"`
	ActiveRevision         uint64        `json:"active_revision"`
	ActiveDigest           [32]byte      `json:"active_digest"`
	LastRejectedRevision   uint64        `json:"last_rejected_revision"`
	LastRejectedDiagnostic string        `json:"last_rejected_diagnostic"`
	RuntimeSourceConnected bool          `json:"runtime_source_connected"`
	Catalog                CatalogStatus `json:"catalog"`
}

// CatalogStatus describes current, draining, and retired connector generations.
type CatalogStatus struct {
	Current           *GenerationStatus      `json:"current"`
	Draining          []GenerationStatus     `json:"draining"`
	RetiredTotal      uint64                 `json:"retired_total"`
	RecentTransitions []GenerationTransition `json:"recent_transitions"`
}

// GenerationStatus describes one connector catalog generation.
type GenerationStatus struct {
	ID       uint64 `json:"id"`
	State    string `json:"state"`
	Bindings uint64 `json:"bindings"`
	Error    string `json:"error"`
}

// GenerationTransition records a recent connector generation state change.
type GenerationTransition struct {
	Sequence   uint64           `json:"sequence"`
	Generation GenerationStatus `json:"generation"`
}
