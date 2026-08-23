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

The production Rust binary includes Kafka through `kafka_franz`. Additional connectors,
configurators, native transports, and middleware are ordinary Rust crates linked into an embedded
application or custom binary and registered explicitly through `ApplicationBuilder`.

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
FUJIN_CONFIGURATOR=yaml \
FUJIN_CONFIGURATOR_YAML_PATHS=./config.dev.yaml \
  ./bin/fujin
```

`FUJIN_CONFIGURATOR` is required. The full binary includes `yaml` and `env`; configuration source
selection stays in the configurator plugin layer. `./bin/fujin --version` prints the build version.

Use [`config.dev.yaml`](config.dev.yaml) as the complete local example and
[`examples/assets/config/config.deployment.example.yaml`](examples/assets/config/config.deployment.example.yaml)
for the container deployment shape.

## Build Options

The default production build is the Rust workspace under `fujin-rs/`.

```bash
# Full production binary: Kafka, TCP, QUIC, WebSocket, Unix, and gRPC
make build

# Minimal statically linked binary
cargo build --manifest-path fujin-rs/Cargo.toml --release -p fujin --features configurator-yaml,kafka,tcp

Available application features: `configurator-yaml`, `configurator-env`, `kafka`, `tcp`, `unix`,
`websocket`, `quic`, and `grpc`; `full` enables all of them. Minimal executable builds must include
at least one configurator feature. `VERSION` sets the build string returned by `--version` and
native HELLO.

Connector and transport availability is fixed by Cargo features, matching the Go builder/import
model. The executable does not discover runtime plugin libraries from environment variables.

## Rust Embedding and Plugins

`fujin` is both the production CLI package and the supported embedding facade. The CLI and embedded
applications use the same `ApplicationBuilder`, plugin registries, listener lifecycle, runtime
connector snapshots, readiness reporting, and graceful shutdown path.

Small adapters are grouped by category: `fujin-connectors` contains Kafka and NOP,
`fujin-configurators` contains the environment and YAML loaders, and `fujin-transports` contains
TCP, Unix, WebSocket, and QUIC. Application features select individual modules; the public
`fujin::plugins::*` namespace hides their physical crate layout. Shared certificate loading and
listener TLS setup live in `fujin-transport::tls`, alongside the listener boundary they support.

```rust
use fujin::{Application, plugins};

let application = plugins::full(Application::builder()).build().await?;
let running = application.start().await?;
println!("listeners: {:?}", running.endpoints());
running.shutdown().await?;
```

A complete TCP embedding example is in
[`fujin-rs/crates/fujin/examples/embed.rs`](fujin-rs/crates/fujin/examples/embed.rs):

```bash
cargo run --manifest-path fujin-rs/Cargo.toml -p fujin --example embed --features tcp
```

Third-party plugins use the contracts re-exported by `fujin` or the lightweight
`fujin-plugin-api` crate. A connector crate exposes an explicit constructor:

```rust
use fujin::connector::{ConnectorPlugin, ConnectorDescriptor};

pub fn plugin() -> ConnectorPlugin {
    ConnectorPlugin::new("acme_sqs", SqsDescriptor)
}
```

The host registers it without process-global state or side-effect imports:

```rust
let application = Application::builder()
    .connector(acme_fujin_connector_sqs::plugin())
    .transport(plugins::transport::tcp())
    .build()
    .await?;
```

Equivalent constructors exist for `ConfiguratorPlugin`, `TransportRegistration`,
`BindMiddlewareRegistration`, and `ConnectorMiddlewareRegistration`. Registration rejects empty or
duplicate names during `build()`. Configured but unregistered plugin names are rejected before any
listener binds.

Rust dynamic-library loading is intentionally unsupported: Rust trait-object ABI is not stable.
Plugins are statically linked Cargo dependencies, matching Go's build-time import model while
keeping composition explicit and allowing different plugin sets per `Application` instance.

Runtime logging uses the Go environment contract: `FUJIN_LOG_LEVEL` accepts `DEBUG`, `INFO`,
`WARN`, or `ERROR`, and `FUJIN_LOG_TYPE=json` selects structured JSON output. On Unix, SIGHUP
reloads `FUJIN_LOG_LEVEL` together with startup-only configurator connector reload behavior.

## Configuration

The selected configurator loads one complete YAML or JSON bootstrap document. The built-in sources
preserve the Go plugin contract:

- `FUJIN_CONFIGURATOR=yaml` reads the first existing comma-separated path in
  `FUJIN_CONFIGURATOR_YAML_PATHS`; defaults are `./config.yaml`, `conf/config.yaml`, and
  `config/config.yaml`.
- `FUJIN_CONFIGURATOR=env` reads YAML or JSON directly from
  `FUJIN_CONFIGURATOR_ENV_CONFIG`.

The bootstrap document shape is:

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

The image selects the `yaml` configurator and sets
`FUJIN_CONFIGURATOR_YAML_PATHS=/config/config.yaml`. It runs as an unprivileged user and contains
the full Rust feature set.

### Kubernetes

```bash
helm install fujin ./deploy/helm/fujin
helm install fujin ./deploy/helm/fujin --set mode=sidecar
```

See [`deploy/helm/fujin/values.yaml`](deploy/helm/fujin/values.yaml). The Docker Compose example is
under [`examples/deployment/`](examples/deployment/).

## Operations

### Connector Reload

On Unix, `SIGHUP` reuses the selected startup-only configurator and reloads only its complete
`connectors` snapshot. Configurators with a live connector watcher exclusively own runtime
connector state, so SIGHUP does not race or override them:
```bash
kill -HUP $(pgrep fujin)
```

The replacement is compiled before publication. A rejected replacement leaves the active
generation untouched. Existing bound sessions remain pinned to their original immutable
generation; later BIND operations see the replacement. Listener, health, and logging settings are
bootstrap-only.

`SIGTERM`, Ctrl-C, or process cancellation stops listeners, drains session tasks, and closes the
connector catalog.

### Graceful Binary Upgrade

On Unix, a running Fujin process exposes a local control socket and can transfer its TCP,
WebSocket, gRPC, health, Unix, and QUIC listener descriptors to a replacement process with
`SCM_RIGHTS`. Launch the replacement with the same listener configuration:

```bash
FUJIN_UPGRADE=1 \
FUJIN_UPGRADE_SOCK=/run/fujin/upgrade.sock \
  /path/to/new/fujin
```

`FUJIN_UPGRADE_SOCK` defaults to `/run/fujin/upgrade.sock`. The replacement requests the
descriptors, starts every configured listener, reapplies TLS in the new process, and reports
readiness. Only then does the old process stop accepting and drain its existing sessions. A failed
or incomplete replacement leaves the old process serving.

Both processes must run on the same Unix host, have access to the control socket, and use
compatible listener addresses and transport types. The container image creates `/run/fujin` for
the unprivileged `fujin` user. Windows supports normal server operation but not listener descriptor
handoff.

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
- [Rust Migration Evidence](fujin-rs/migration_report.md)

## License

MIT. See [LICENSE](LICENSE).
