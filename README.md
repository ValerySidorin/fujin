# Fujin

High-performance message broker gateway. Sits between your applications and message brokers (Kafka, NATS, RabbitMQ, and others), exposing a single efficient protocol and gRPC interface.

Think of it as Envoy, but for message brokers instead of HTTP.

Current release: **v0.5.0**.

### v0.5.0 highlights

- Added mandatory transport-neutral HELLO negotiation before every native BIND, with byte-sized wire versions and diagnostic client/server build metadata.
- Changed QUIC ALPN from versioned `fujin/1` to the version-independent `fujin`; native clients must upgrade to `fujin-go v0.3.0` or implement HELLO.
- Kept the warmed server HELLO path and coordinated SDK HELLO encode/decode paths at zero allocations.

## Why

Broker client libraries are heavy, language-specific, and tightly coupled to your application. Upgrading a Kafka client, adding metrics, or switching from RabbitMQ to NATS means changing and redeploying every service.

Fujin decouples applications from brokers. Your app talks to Fujin over TCP, QUIC, WebSocket, Unix sockets, or gRPC — Fujin handles the rest. This gives you:

- **Any language, any broker.** No need for a native Kafka or NATS client in every language. If your app can open a TCP socket or call gRPC, it can produce and consume messages.
- **Centralized operations.** Observability, authorization, broker client upgrades, and versioned connector desired state can be managed centrally without redeploying application clients.
- **Minimal overhead.** Zero-allocation protocol parser. TCP transport pushes ~840 MB/s on 32KB payloads through Kafka on Apple M2. The protocol layer adds negligible latency.
- **Zero-downtime deployments.** Graceful binary upgrade via FD passing (Unix). Hot config reload via SIGHUP. No dropped connections.

## Supported Brokers

The production Rust binary includes Kafka through `kafka_franz`. Additional connector types can be
loaded as trusted dynamic libraries without rebuilding the server.

| Broker | Configuration `type` |
|---|---|
| Kafka | `kafka_franz` |

## Client Interfaces

**Fujin Protocol** — Native protocol v1 over TCP, QUIC, WebSocket, or Unix sockets. Every session
starts with HELLO, then delegates BIND, produce, fetch, subscribe, settlement, and transaction
semantics to the shared Rust Session Core.

**gRPC** — The protobuf API in [`public/proto/grpc/v1/fujin.proto`](public/proto/grpc/v1/fujin.proto).
It uses the same Session Core and route profiles as the native adapter. The standard gRPC health
service reports `fujin.v1.FujinService` readiness.

### Transports

| Transport | Notes |
|---|---|
| TCP | Optional TLS or mutual TLS. |
| QUIC | TLS is mandatory; idle timeout, keepalive, and incoming stream limits are configurable. |
| WebSocket | Binary messages only; configurable path, browser Origin allowlist, size limit, and TLS. |
| Unix | Unix-domain stream transport on Unix hosts. |

## Quick Start

```bash
make build
FUJIN_CONFIG=./config.dev.yaml ./bin/fujin
```

The binary also accepts the configuration path as its first argument:

```bash
./bin/fujin ./config.dev.yaml
./bin/fujin --version
```

Use [`config.dev.yaml`](config.dev.yaml) as the complete local example and
[`examples/assets/config/config.deployment.example.yaml`](examples/assets/config/config.deployment.example.yaml)
for the container deployment shape.

## Build Options

The default production build is the Rust workspace under `fujin-rs/`.

```bash
# Full production binary: Kafka, TCP, QUIC, WebSocket, Unix, and gRPC
make build

# Minimal statically linked binary
cargo build --manifest-path fujin-rs/Cargo.toml --release -p fujin --features kafka,tcp
```

Available application features: `kafka`, `tcp`, `unix`, `websocket`, `quic`, and `grpc`; `full`
enables all of them. `FUJIN_VERSION` sets the build string returned by `--version` and native HELLO.

### Dynamic Connector Plugins

Set `FUJIN_CONNECTOR_PLUGINS` to a platform-native path list of connector libraries. Libraries are
loaded before the initial connector snapshot is compiled and remain loaded through catalog
shutdown.

```bash
cargo build --manifest-path fujin-rs/Cargo.toml --release -p fujin-plugin-nop
FUJIN_CONNECTOR_PLUGINS=./fujin-rs/target/release/libfujin_plugin_nop.so \
  ./bin/fujin ./config.yaml
```

The example plugin is under [`fujin-rs/plugins/nop`](fujin-rs/plugins/nop). Dynamic plugins are a
trusted-code boundary and must be built from the same Fujin source revision and Rust toolchain as
the host. The versioned C symbol stabilizes discovery; connector trait objects retain Rust's
compiler-specific ABI.

## Configuration

Configuration is one complete YAML or JSON bootstrap document:

```yaml
fujin:
  transports:
    - type: tcp
      settings:
        addr: 0.0.0.0:4850
grpc:
  enabled: true
  addr: 0.0.0.0:4849
health:
  enabled: true
  addr: 0.0.0.0:8080
connectors:
  primary:
    type: kafka_franz
    settings:
      common:
        brokers: [kafka:9092]
        properties: {}
      routes:
        events:
          produce_topic: events
          consume_topics: [events]
          group: app
```

Kafka `common.properties` and route-level `properties` map directly to librdkafka string settings.
Connector compilation validates configuration without broker I/O; broker clients are opened lazily
when a bound session first uses a route.

The base Rust build rejects Go-only configuration fields rather than silently ignoring them. In
particular, native `settings.fujin` ping/write tuning, gRPC client keepalive enforcement,
`connection_timeout`, and `server_keepalive.max_connection_idle` are not currently implemented.

## Deployment

### Docker

```bash
docker build --build-arg VERSION=v0.5.0 -t fujin .
docker run --rm -p 4850:4850 -p 4849:4849 -p 8080:8080 \
  -v "$PWD/config.yaml:/config/config.yaml:ro" fujin
```

The image sets `FUJIN_CONFIG=/config/config.yaml`, runs as an unprivileged user, and contains the
full Rust feature set.

### Kubernetes

```bash
helm install fujin ./deploy/helm/fujin
helm install fujin ./deploy/helm/fujin --set mode=sidecar
```

See [`deploy/helm/fujin/values.yaml`](deploy/helm/fujin/values.yaml). The Docker Compose example is
under [`examples/deployment/`](examples/deployment/).

## Operations

### Connector Reload

On Unix, `SIGHUP` reloads only the complete `connectors` snapshot from the selected bootstrap file:

```bash
kill -HUP $(pgrep fujin)
```

The replacement is compiled before publication. A rejected replacement leaves the active
generation untouched. Existing bound sessions remain pinned to their original immutable
generation; later BIND operations see the replacement. Listener, health, and logging settings are
bootstrap-only.

`SIGTERM`, Ctrl-C, or process cancellation stops listeners, drains session tasks, and closes the
connector catalog.

### Health Checks

```yaml
health:
  enabled: true
  addr: 0.0.0.0:8080
```

- `GET /healthz` — liveness; returns 200 while the process is running.
- `GET /readyz` — readiness; returns 200 after every configured listener has bound, otherwise 503.
- `grpc.health.v1.Health` — reports the Fujin gRPC service as serving while its listener is active.

## Benchmarks

[`fujin-rs/bench_report.md`](fujin-rs/bench_report.md) records the Rust Session Core snapshot.
Regenerate the validated report with:

```bash
make rust-bench-report
```

The retained Go implementation is used only for controlled migration comparisons; `make build`,
the release container, Compose, and Helm all select the Rust runtime.

## Documentation

- [Native Protocol Specification](protocol.md)
- [gRPC Proto Definition](public/proto/grpc/v1/fujin.proto)
- [Development Configuration](config.dev.yaml)
- [Deployment Configuration](examples/assets/config/config.deployment.example.yaml)

## License

MIT. See [LICENSE](LICENSE).
