# Fujin

High-performance message broker gateway. Sits between your applications and message brokers (Kafka, NATS, RabbitMQ, and others), exposing a single efficient protocol and gRPC interface.

Think of it as Envoy, but for message brokers instead of HTTP.

## Why

Broker client libraries are heavy, language-specific, and tightly coupled to your application. Upgrading a Kafka client, adding metrics, or switching from RabbitMQ to NATS means changing and redeploying every service.

Fujin decouples applications from brokers. Your app talks to Fujin over a simple TCP/QUIC connection or gRPC — Fujin handles the rest. This gives you:

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

## Client Interfaces

**Fujin Protocol** — Custom binary protocol over TCP, QUIC, or Unix sockets. Zero-allocation parsing, transactions, headers, push and pull delivery. A successful BIND returns the pinned route capability and guarantee profile. Best for high-throughput scenarios. Go client: [`fujin-go`](https://github.com/fujin-io/fujin-go).

**gRPC** — Standard gRPC interface. Works with any language that has a gRPC library. `BindResponse.routes` exposes the same pinned capability profile as the native protocol.

### Transports

| Transport | Best for |
|-----------|----------|
| TCP | Maximum single-stream throughput. Optional TLS. |
| QUIC | Multiplexed streams, built-in TLS, connection migration. |
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

| Plugin type | Examples |
|-------------|----------|
| Transports | `tcp`, `quic`, `unix` |
| Connectors | `kafka_franz`, `nats_core`, `rabbitmq_amqp09`, ... |
| Configurators | `yaml`, `env` |
| Bind middleware | `auth_api_key` |
| Connector middleware | `prom`, `otel`, `schema/json`, `transform/jq`, `filter/jq`, `dedup`, `compress/zstd`, `rate_limit/token_bucket` |

Write your own plugins — see [`examples/plugins/`](examples/plugins/) for a complete example with a custom connector and rate-limiting middleware. Each plugin has a README with configuration examples in its package directory under [`public/plugins/`](public/plugins/).

Connector plugins expose a side-effect-free descriptor. Fujin compiles settings and route capabilities without broker I/O, then lazily opens generation-owned runtimes when an operation first needs broker resources. A successful BIND proves local configuration validity and returns the pinned route profiles to native and gRPC clients; it does not prove broker availability.

### Cross-Platform

Fujin compiles on Linux, macOS, and Windows:

```bash
GOOS=windows GOARCH=amd64 go build -tags=fujin,grpc ./...
```

On Windows, Unix-only features (Unix socket transport, SIGHUP reload, graceful binary upgrade) are unavailable. TCP, QUIC, and gRPC work normally.

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

Fujin v0.3.0 adds public runtime-configurator contracts and generation lifecycle reporting for external management planes. [`fujin-control-plane`](https://github.com/fujin-io/fujin-control-plane) provides mTLS Sync, versioned desired snapshots, optimistic-concurrency updates, node status, audit records, and a `control_plane` configurator plugin.

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

Apple M2, macOS arm64, single connection, localhost. Raw results: [`test/bench_test.txt`](test/bench_test.txt).

## Documentation

- [Native Protocol Specification](protocol.md)
- [gRPC Proto Definition](public/proto/grpc/v1/fujin.proto)
- [Configuration Example](examples/assets/config/config.yaml)
- [Fujin Control Plane](https://github.com/fujin-io/fujin-control-plane)

- Plugin docs — each plugin has a README in its package under [`public/plugins/`](public/plugins/)

## License

MIT. See [LICENSE](LICENSE).
