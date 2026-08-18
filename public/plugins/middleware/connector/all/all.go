// Package all imports all available connector middlewares for side-effect registration.
// Import this package to enable all connector middlewares:
//
//	import _ "github.com/fujin-io/fujin/public/plugins/middleware/connector/all"
package all

import (
	// prom connector middleware - provides Prometheus metrics
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/prom"
	// otel connector middleware - provides OpenTelemetry distributed tracing
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/otel"
	// schema/jsonschema connector middleware - provides JSON Schema message validation
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/schema/json"
	// transform/jq connector middleware - provides jq-based message transformation
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/transform/jq"
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/transform/wasm"
	// dedup connector middleware - provides message deduplication
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/dedup"
	// filter/jq connector middleware - provides jq-based message filtering
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/filter/jq"
	// compress/zstd connector middleware - provides zstd compression
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/compress/zstd"
	// rate_limit/token_bucket connector middleware - provides token bucket rate limiting
	_ "github.com/fujin-io/fujin/public/plugins/middleware/connector/rate_limit/token_bucket"
)
