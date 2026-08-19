# Contributing to Fujin

Thank you for considering contributing to Fujin! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites

- **Go 1.25+** - Required for building (see `go.mod`)
- **Make** - Cross-platform build automation (Windows/Linux/macOS)
- **Docker & Docker Compose** - For running message brokers locally
- **protoc** - Protocol Buffer compiler (optional, for regenerating proto files)

### Quick Setup

```bash
# 1. Clone the repository
git clone https://github.com/fujin-io/fujin.git
cd fujin

# 2. Build the project
make build

# 3. Run tests
make test

# 4. Start development environment (Kafka example)
make up-kafka
```

## Project Structure

The project uses a single Go module at the root:

```
fujin/
├── cmd/                        # Entry points
│   ├── main.go                 # Default server (all plugins)
│   └── builder/                # Custom binary builder (selective plugins)
├── public/                     # Public API and plugins
│   ├── plugins/                # Connectors, configurators, middlewares
│   │   ├── connector/          # Kafka, NATS, AMQP, MQTT, NSQ, RESP...
│   │   ├── configurator/       # File-based config loader
│   │   └── middleware/         # Bind (auth_api_key) and connector middlewares (prom, otel)
│   ├── proto/                  # gRPC and Fujin protocol definitions
│   ├── server/                 # Server abstraction and config
│   └── service/                # Core service (RunCLI)
├── internal/                   # Internal implementation (not exported)
│   ├── proto/         # Fujin binary protocol (transport-agnostic)
│   └── transport/              # Transport implementations (quic/, tcp/, grpc/)
├── examples/                   # Sample configs and runnable examples
├── resources/                  # Docker Compose, Grafana, example configs
├── test/                       # Benchmarks and test helpers
└── Makefile                    # Cross-platform build commands
```

## Building

### All Platforms

The Makefile works on Windows, Linux, and macOS:

```bash
# Default build (all connectors)
make build

# Minimal build (Kafka only)
make build CONNECTORS=kafka

# With specific connectors
make build CONNECTORS="kafka,nats/core"

# With specific transports
make build GO_BUILD_TAGS="fujin,grpc"
```

### Manual Build

If you prefer not to use Make, use the builder directly:

```bash
go run ./cmd/builder -local \
  -transport github.com/fujin-io/fujin/public/plugins/transport/tcp \
  -configurator github.com/fujin-io/fujin/public/plugins/configurator/yaml \
  -connector github.com/fujin-io/fujin/public/plugins/connector/kafka/franz \
  -bind-middleware github.com/fujin-io/fujin/public/plugins/middleware/bind/auth_api_key \
  -connector-middleware github.com/fujin-io/fujin/public/plugins/middleware/connector/prom \
  -tags "fujin,grpc" \
  -output ./bin/fujin
```

## Build Tags

Fujin uses Go build tags for conditional compilation:
  - `fujin` - Transport-agnostic Fujin protocol
  - `grpc` - gRPC server (language-agnostic)

The Fujin protocol code compiles when `fujin` tag is enabled. 

## Testing

```bash
# Run all tests
make test

# Run tests for specific package
go test -v -tags="fujin,grpc" ./internal/...

# Run benchmarks
make bench

# Custom benchmark
make bench BENCH_FUNC="BenchmarkMyFunction" BENCH_TIME="10s"
```

## Code Style

### Go Code

- Follow standard Go conventions
- Run `gofmt` before committing
- Use meaningful variable names
- Add comments for exported functions

## Generating Protocol Buffers

If you modify `.proto` files:

```bash
make generate
```

Or manually:
```bash
cd public/proto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       grpc/v1/fujin.proto
```

## Running Integration Tests

```bash
# Start broker (example: Kafka)
make up-kafka_franz

# Run tests
go test -v -tags="fujin,grpc" ./test/...

# Clean up
make down-kafka_franz
```

## Documentation

- Update relevant `README.md` files
- Add examples in `examples/` directory
- Update `protocol.md` for protocol changes
- Comment complex code sections

## Pull Request Process

1. **Fork & Branch**
   - Fork the repository
   - Create feature branch: `git checkout -b feat/my-feature`

2. **Make Changes**
   - Write clean, tested code
   - Follow code style guidelines
   - Update documentation

3. **Test**
   - Run `make test`
   - Test on multiple platforms if possible
   - Verify builds with different tag combinations

4. **Commit**
   - Use conventional commit messages
   - Keep commits focused and atomic

5. **Submit PR**
   - Provide clear description
   - Reference related issues
   - Wait for review

## Cross-Platform Testing

If you're developing on Windows but want to ensure Linux/macOS compatibility:

- Use Docker for testing
- Avoid platform-specific APIs
- Test with different Go versions
- Use CI/CD results as reference

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

Thank you for contributing to Fujin! 🚀

