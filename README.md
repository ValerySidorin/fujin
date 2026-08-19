# Fujin

High-performance message broker gateway. Sits between your applications and message brokers (Kafka, NATS, RabbitMQ, and others), exposing a single efficient protocol and gRPC interface.

Think of it as Envoy, but for message brokers instead of HTTP.

Current release: **v0.4.1**.

### v0.4.1 highlights

- Reduced native large-message request/response overhead with adaptive inbound reads and bounded payload-buffer reuse through 1 MiB.
- Added WebSocket to the canonical native Session Core benchmark matrix and clarified comparable TCP versus gRPC pipeline reporting.

## Why

Broker client libraries are heavy, language-specific, and tightly coupled to your application. Upgrading a Kafka client, adding metrics, or switching from RabbitMQ to NATS means changing and redeploying every service.

Fujin decouples applications from brokers. Your app talks to Fujin over TCP, QUIC, WebSocket, Unix sockets, or gRPC — Fujin handles the rest. This gives you:

- **Any language, any broker.** No need for a native Kafka or NATS client in every language. If your app can open a TCP socket or call gRPC, it can produce and consume messages.
- **Centralized operations.** Observability, authorization, broker client upgrades, and versioned connector desired state can be managed centrally without redeploying application clients.
- **Minimal overhead.** Zero-allocation protocol parser. TCP transport pushes ~840 MB/s on 32KB payloads through Kafka on Apple M2. The protocol layer adds negligible latency.
- **Zero-downtime deployments.** Graceful binary upgrade via FD passing (Unix). Hot config reload via SIGHUP. No dropped connections.

## Supported Brokers

| Broker | Configuration `type` |
|--------|----------------------|
| Kafka | `kafka_franz` |
| NATS Core | `nats_core` |
| NATS JetStream | `nats_jetstream` |
| RabbitMQ | `rabbitmq_amqp09` |
| Azure Service Bus / ActiveMQ | `azure_amqp1` |
| Redis/Valkey Pub/Sub | `redis_rueidis_pubsub` |
| Redis/Valkey Streams | `redis_rueidis_streams` |
| MQTT (EMQX, NanoMQ, etc.) | `mqtt_paho` |
| NSQ | `nsq` |
| ZeroMQ (`libzmq`, opt-in CGO build) | `zeromq_pebbe` |

## Client Interfaces

**Fujin Protocol** — Custom binary protocol over TCP, QUIC, WebSocket, or Unix sockets. Zero-allocation parsing, transactions, headers, push and pull delivery. A successful BIND returns the pinned route capability and guarantee profile. Best for high-throughput scenarios. Go client: [`fujin-go`](https://github.com/fujin-io/fujin-go).

**gRPC** — Standard gRPC interface. Works with any language that has a gRPC library. `BindResponse.routes` exposes the same pinned capability profile as the native protocol.

### Transports

| Transport | Best for |
|-----------|----------|
| TCP | Maximum single-stream throughput. Optional TLS. |
| QUIC | Multiplexed streams, built-in TLS, connection migration. |
| WebSocket | Browser and HTTP-infrastructure access to the native binary protocol. |
| Unix | Same-host IPC (sidecars, pods). Lowest latency. |

## Quick Start

```bash
# Build
make build

# Run (requires a config file)
FUJIN_CONFIGURATOR=yaml FUJIN_CONFIGURATOR_YAML_PATHS=./config.yaml ./bin/fujin
```

See [`examples/assets/config/config.yaml`](examples/assets/config/config.yaml) for a full configuration example.

## Build Options

Fujin uses build tags and a plugin system. You can build a full binary with all plugins, or a minimal one with only what you need.

```bash
# Full binary (all transports, all connectors, gRPC)
make build

# Minimal binary (only Kafka, only TCP, no gRPC)
go run ./cmd/builder \
  -transport github.com/fujin-io/fujin/public/plugins/transport/tcp \
  -configurator github.com/fujin-io/fujin/public/plugins/configurator/yaml \
  -connector github.com/fujin-io/fujin/public/plugins/connector/kafka/franz \
  -tags "fujin" \
  -output ./bin/fujin-minimal
```

Build tags: `fujin` (native protocol transports), `grpc` (gRPC server).

### Plugin System

Everything is pluggable: transports, connectors, config loaders, and middleware. Plugins self-register via `init()`. The custom binary builder (`cmd/builder`) generates a `main.go` that imports only selected plugins, keeping the binary small.

### Built-in Plugin Reference

Every built-in plugin has a package-local README containing its registered name, configuration, behavior, limits, and operational notes.

#### Transports

| Registered name | Documentation |
|---|---|
| `tcp` | [TCP](public/plugins/transport/tcp/README.md) |
| `quic` | [QUIC](public/plugins/transport/quic/README.md) |
| `websocket` | [WebSocket](public/plugins/transport/websocket/README.md) |
| `unix` | [Unix socket](public/plugins/transport/unix/README.md) |

#### Connectors

| Registered name | Documentation |
|---|---|
| `azure_amqp1` | [Azure AMQP 1.0](public/plugins/connector/azure/amqp1/README.md) |
| `kafka_franz` | [Kafka via franz-go](public/plugins/connector/kafka/franz/README.md) |
| `mqtt_paho` | [MQTT via Paho](public/plugins/connector/mqtt/paho/README.md) |
| `nats_core` | [NATS Core](public/plugins/connector/nats/core/README.md) |
| `nats_jetstream` | [NATS JetStream](public/plugins/connector/nats/jetstream/README.md) |
| `nsq` | [NSQ](public/plugins/connector/nsq/README.md) |
| `rabbitmq_amqp09` | [RabbitMQ AMQP 0.9.1](public/plugins/connector/rabbitmq/amqp09/README.md) |
| `redis_rueidis_pubsub` | [Redis Pub/Sub](public/plugins/connector/redis/rueidis/pubsub/README.md) |
| `redis_rueidis_streams` | [Redis Streams](public/plugins/connector/redis/rueidis/streams/README.md) |
| `zeromq_pebbe` | [ZeroMQ via pebbe/zmq4](public/plugins/connector/zeromq/pebbe/README.md) |

#### Configurators

| Registered name | Documentation |
|---|---|
| `yaml` | [YAML/JSON files](public/plugins/configurator/yaml/README.md) |
| `env` | [Environment variable](public/plugins/configurator/env/README.md) |

#### Bind Middleware

| Registered name | Documentation |
|---|---|
| `auth_api_key` | [API key authentication](public/plugins/middleware/bind/auth_api_key/README.md) |

#### Connector Middleware

| Registered name | Documentation |
|---|---|
| `prom` | [Prometheus metrics](public/plugins/middleware/connector/prom/README.md) |
| `otel` | [OpenTelemetry tracing](public/plugins/middleware/connector/otel/README.md) |
| `schema_json` | [JSON Schema validation](public/plugins/middleware/connector/schema/json/README.md) |
| `transform_jq` | [jq transformation](public/plugins/middleware/connector/transform/jq/README.md) |
| `transform_wasm` | [WebAssembly transformation](public/plugins/middleware/connector/transform/wasm/README.md) |
| `filter_jq` | [jq filtering](public/plugins/middleware/connector/filter/jq/README.md) |
| `dedup` | [Deduplication](public/plugins/middleware/connector/dedup/README.md) |
| `compress_zstd` | [Zstandard compression](public/plugins/middleware/connector/compress/zstd/README.md) |
| `rate_limit_token_bucket` | [Token-bucket rate limiting](public/plugins/middleware/connector/rate_limit/token_bucket/README.md) |

Write your own plugins using the examples under [`examples/plugins/`](examples/plugins/). The custom binary builder imports only the selected plugins, keeping the resulting binary small.

Connector plugins expose a side-effect-free descriptor. Fujin compiles settings and route capabilities without broker I/O, then lazily opens generation-owned runtimes when an operation first needs broker resources. A successful BIND proves local configuration validity and returns the pinned route profiles to native and gRPC clients; it does not prove broker availability.

`transform_wasm` runs SHA-256-pinned WebAssembly transforms in wazero without WASI, filesystem, environment, or network imports. The Rust example under [`examples/plugins/middleware/connector/wasm-uppercase`](examples/plugins/middleware/connector/wasm-uppercase) implements the guest ABI.

### Cross-Platform

Fujin compiles on Linux, macOS, and Windows:

```bash
GOOS=windows GOARCH=amd64 go build -tags=fujin,grpc ./...
```

On Windows, Unix-only features (Unix socket transport, SIGHUP reload, graceful binary upgrade) are unavailable. TCP, QUIC, WebSocket, and gRPC work normally.

## Deployment

### Docker

```bash
docker build -t fujin .

# Custom build (Kafka only, Fujin + gRPC)
docker build --build-arg FUJIN_CONNECTORS=github.com/fujin-io/fujin/public/plugins/connector/kafka/franz -t fujin .
```

### Kubernetes

Deploy with the Helm chart (see below), or use the Docker Compose example in [`examples/deployment/`](examples/deployment/).

### Helm

```bash
# Standalone: Fujin as a separate Deployment + Service
helm install fujin ./deploy/helm/fujin

# Sidecar: ConfigMap + helper templates to embed in your Deployment
helm install fujin ./deploy/helm/fujin --set mode=sidecar
```

See [`deploy/helm/fujin/values.yaml`](deploy/helm/fujin/values.yaml) for all options.

## Operations

### Hot Reload

```bash
kill -HUP $(pgrep fujin)
```

Reloads connector configuration and log level from YAML. A connector reload compiles and validates the complete replacement before publication. Failed reloads retain the current generation; existing bound sessions remain pinned to their prior immutable generation, while later BIND operations use the replacement.

### Control Plane

Public runtime-configurator contracts and generation lifecycle reporting support external management planes. [`fujin-control-plane`](https://github.com/fujin-io/fujin-control-plane) provides mTLS Sync, versioned desired snapshots, optimistic-concurrency updates, node status, audit records, and a `control_plane` configurator plugin.

Delivered connector snapshots are compiled completely before atomic publication. Invalid snapshots retain the active generation; existing BIND sessions continue on their pinned generation until it drains.

The control-plane repository documents the custom binary build and node bootstrap configuration.

### Graceful Binary Upgrade

Zero-downtime binary replacement on Unix systems. The new process inherits listener file descriptors from the old one via SCM_RIGHTS — no connections are dropped.

```bash
# 1. Old process is running and listening on the upgrade socket

# 2. Build the new binary
make build

# 3. Start new process in upgrade mode
FUJIN_UPGRADE=1 ./bin/fujin
```

The new process connects to the old process's control socket, receives listener FDs, starts serving, signals ready, and the old process drains and exits.

Custom socket path (default: `/run/fujin/upgrade.sock`):
```bash
export FUJIN_UPGRADE_SOCK=/tmp/fujin-upgrade.sock
```

### Health Checks

HTTP health check server for Kubernetes liveness and readiness probes.

Enable in config:
```yaml
health:
  enabled: true
  addr: ":8080"
```

Endpoints:
- `GET /healthz` — liveness probe, always returns 200
- `GET /readyz` — readiness probe, returns 200 when all transports are up, 503 otherwise

## Benchmarks

[`test/bench_report.md`](test/bench_report.md) is a reproducible local Session Core data-plane snapshot, not a cross-machine or broker-throughput comparison. It measures synchronous produce against the built-in `nop` connector over native TCP, QUIC, Unix sockets, and gRPC.

`nop` accepts messages immediately and performs no broker I/O. The report therefore isolates Fujin's protocol, session, scheduling, and callback overhead. It includes operations per second, payload throughput, p99 operation latency, and allocations.

Regenerate it with:

```bash
make bench-report
```

The default report captures 1 B, 128 B, and 1 MiB payloads at 1, 16, and 128 concurrent sessions, with a 3-second sample per subtest. It also records a 1 B, 1,000,000-message TCP pipeline peak. Override the scope for a longer focused run:

```bash
BENCHTIME=10s FUJIN_BENCH_PAYLOAD=1MiB FUJIN_BENCH_CONCURRENCY=128 make bench-report
```

The report records its exact source revision, Go toolchain, host, and parameters. Broker-backed benchmarks remain separate because broker topology, durability, and container state materially affect their figures.

## Documentation

- [Native Protocol Specification](protocol.md)
- [gRPC Proto Definition](public/proto/grpc/v1/fujin.proto)
- [Configuration Example](examples/assets/config/config.yaml)
- [Fujin Control Plane](https://github.com/fujin-io/fujin-control-plane)

- Plugin docs — each plugin has a README in its package under [`public/plugins/`](public/plugins/)

## License

MIT. See [LICENSE](LICENSE).
