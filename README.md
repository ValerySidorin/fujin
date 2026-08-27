# Fujin

High-performance message broker gateway written in Rust. Fujin sits between applications and
message brokers, exposing one native binary protocol and a semantically equivalent gRPC API.

Think of it as Envoy for message brokers instead of HTTP.

The active implementation is the root Rust workspace. **v0.5.0** is the final Go release and
remains available through the immutable `v0.5.0` tag and `legacy/go-v0.5` branch. Rust releases use
namespaced Git tags beginning with `fujin/`; the Rust release line starts with the namespaced
`fujin/v0.6` series.

## Why

Broker client libraries are heavy, language-specific, and tightly coupled to your application. Upgrading a Kafka client, adding metrics, or switching from RabbitMQ to NATS means changing and redeploying every service.

Fujin decouples applications from brokers. Your app talks to Fujin over TCP, QUIC, WebSocket, Unix sockets, or gRPC — Fujin handles the rest. This gives you:

- **Any language, one gateway.** Applications use the native protocol or gRPC instead of embedding
  a broker client in every service.
- **Centralized operations.** Connector configuration, observability, authorization, and broker
  client upgrades are owned by Fujin.
- **Low overhead.** The incremental native codec and shared Session Core avoid broker-specific work
  in transport adapters; retained measurements are documented in [`bench_report.md`](bench_report.md).
- **Zero-downtime deployments.** Unix listener handoff and bounded session draining preserve active
  service during binary replacement.

## Supported Brokers

The production Rust binary includes the `kafka` connector. Additional connectors,
configurators, native transports, and middleware are ordinary Rust crates linked into an embedded
application or custom binary and registered explicitly through `ApplicationBuilder`.

| Broker | Configuration `type` |
|---|---|
| Kafka | `kafka` |

## Client Interfaces

**Fujin Protocol** — Native protocol v1 over TCP, QUIC, WebSocket, or Unix sockets. Every session
starts with HELLO, then delegates BIND, produce, fetch, subscribe, settlement, and transaction
semantics to the shared Rust Session Core.

**gRPC** — The protobuf API in [`crates/fujin-grpc-proto/proto/fujin.proto`](crates/fujin-grpc-proto/proto/fujin.proto).
It uses the same Session Core and route profiles as the native adapter. The standard gRPC health
service reports `fujin.v1.FujinService` readiness.

Rust embedders and transport authors use the versioned constants, opcodes, decoded requests, and
incremental decoder re-exported from `fujin::native`; the authoritative byte layout remains
[`protocol.md`](protocol.md).

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
FUJIN_CONFIGURATOR=file \
FUJIN_CONFIGURATOR_FILE_PATHS=./config.dev.yaml \
  ./bin/fujin
```

`FUJIN_CONFIGURATOR` is required. The full binary includes `file` and `env`; configuration source
selection stays in the configurator plugin layer. `./bin/fujin --version` prints the build version.

Use [`config.dev.yaml`](config.dev.yaml) as the complete local example and
[`examples/assets/config/config.deployment.example.yaml`](examples/assets/config/config.deployment.example.yaml)
for the container deployment shape.

## Build Options

The production Cargo workspace lives at the repository root.

```bash
# Full production binary: Kafka, TCP, QUIC, WebSocket, Unix, and gRPC
make build

# Minimal built-in binary selected by Cargo features
cargo build --release \
  -p fujin-app --no-default-features \
  --features configurator-file,connector-kafka,transport-tcp
```

Available `fujin-app` features are `configurator-file`, `configurator-env`, `connector-kafka`,
`transport-tcp`, `transport-unix`, `transport-websocket`, `transport-quic`, and `grpc`; `full`
enables all of them and is the default.
`VERSION` sets the build string returned by `--version` and native HELLO for `fujin-app`.


Use `cargo-fujin` to build a custom binary, `cdylib`, or `staticlib` from built-in or third-party Cargo crates:

```bash
cargo install --path tools/cargo-fujin --locked

cargo fujin init --fujin-path ./crates/fujin
cargo fujin plugin add configurator \
  --name file --package fujin-configurator-file \
  --path ./plugins/configurator/file
cargo fujin plugin add connector \
  --name kafka --package fujin-connector-kafka \
  --path ./plugins/connector/kafka
cargo fujin plugin add transport \
  --name tcp --package fujin-transport-tcp \
  --path ./plugins/transport/tcp

cargo fujin build --locked
```

The first build resolves dependencies and creates `.fujin/generated/Cargo.lock`; subsequent
`--locked` builds require the same dependency graph. Generated `Cargo.toml` and Rust source files
are rewritten only when their contents change, so an unchanged composition retains Cargo's
incremental cache. `cargo fujin clean` removes the configured generated project and `target/` cache
while preserving the installed artifact. `cargo fujin build --clean-after` performs that cleanup
after a successful build when only the final artifact should remain.

Plugin sources may use a registry version, a Git URL with `--rev`, `--tag`, or `--branch`, or a
local `--path`. `plugin add` also accepts an explicit `--factory`, target `--cfg`, repeatable
dependency `--feature`, and `--no-default-features`. `build` accepts Cargo profiles, target triples,
offline/locked resolution, an output override, and `--lockfile` to seed the generated project with
an external immutable `Cargo.lock`. `FUJIN_BUILD_VERSION` overrides the product version reported by
generated binaries and libraries without changing Cargo package resolution. `fujin.build.toml` is
the authoritative composition manifest; `.fujin/` contains generated code and build artifacts and
is ignored by Git. Plugins are statically linked and are not discovered from runtime environment
variables.

Set `application.artifact` in `fujin.build.toml` to `binary` (default), `cdylib`, or `staticlib`.
Library artifacts export the versioned API declared by
[`crates/fujin-ffi/include/fujin.h`](crates/fujin-ffi/include/fujin.h). ABI v1 accepts explicit
JSON configuration, returns ready endpoints, exposes connector status and reload controls, and uses
caller-owned errors and output buffers. Rust plugins remain statically linked into the generated
library; no Rust trait object crosses the C ABI.

The embedding SDK under [`sdk/go/embed`](sdk/go/embed) loads a generated `cdylib` and exposes
readiness-gated startup, endpoints, connector reload/status, shutdown, and waiting:

```go
library, err := fujin.Open("./libfujin.dylib")
if err != nil { return err }
defer library.Close()

application, err := library.Start(ctx, fujin.Options{Config: &config})
if err != nil { return err }
defer application.Close()

endpoints, err := application.Endpoints()
```

`GracefulUpgrade` defaults to false for embedded applications. The generated library owns its
statically linked Rust plugins; Go passes configuration and lifecycle operations across ABI v1.
Embedded applications use the same logging environment variables but do not install signal
handlers or reload logging on SIGHUP; see the [Go embedding SDK guide](sdk/go/embed/README.md) for
lifecycle, logging, error, and host-responsibility details.

The independently versioned [Go network client](sdk/go/client) supports native QUIC and protobuf
gRPC. Its generated protobuf bindings come from this repository's canonical
[`crates/fujin-grpc-proto/proto/fujin.proto`](crates/fujin-grpc-proto/proto/fujin.proto), so server and client contract changes land
in one commit. See the [Go SDK module layout](sdk/go/README.md) for module paths and release tags.

## Rust Embedding and Plugins

`fujin` is the supported embedding library and public plugin facade. The standard production
executable is the separate `fujin-app` package under `apps/fujin`; it owns built-in composition.
`cargo-fujin` generates equivalent custom composition binaries from `fujin.build.toml`.

Every plugin implementation is one independent leaf crate in one of five plugin families:

```text
plugins/
├── connector/
│   └── kafka/                  # fujin-connector-kafka
├── configurator/
│   ├── env/                    # fujin-configurator-env
│   └── file/                   # fujin-configurator-file
├── middleware/
│   ├── bind/<name>/            # fujin-middleware-bind-<name>
│   └── connector/<name>/       # fujin-middleware-connector-<name>
└── transport/
    ├── quic/                   # fujin-transport-quic
    ├── tcp/                    # fujin-transport-tcp
    ├── unix/                   # fujin-transport-unix
    └── websocket/              # fujin-transport-websocket
```

The singular family directories are namespaces, not Cargo packages. Public authoring contracts are
re-exported under `fujin::connector`, `fujin::configurator`, `fujin::transport`, and
`fujin::middleware`. Built-in middleware implementations have not yet been ported.

Embedded applications register plugins explicitly:

```rust
use fujin::Application;

let application = Application::builder()
    .configurator(acme_configurator::plugin())
    .connector(acme_connector::plugin())
    .transport(fujin_transport_tcp::plugin())
    .build()
    .await?;
let running = application.start().await?;
println!("listeners: {:?}", running.endpoints());
running.shutdown().await?;
```

Synchronous hosts can use `EmbeddedApplication::start` to own Fujin on a dedicated Tokio runtime
thread. `EmbeddedRuntimeConfig` controls the worker count and runtime thread name; the returned
handle supports readiness-before-return, explicit shutdown, waiting, connector reload, and status.

A complete TCP embedding example is in
[`crates/fujin/examples/embed.rs`](crates/fujin/examples/embed.rs).

Third-party connector crates expose an explicit constructor:

```rust
use fujin::connector::{ConnectorDescriptor, ConnectorPlugin};

pub fn plugin() -> ConnectorPlugin {
    ConnectorPlugin::new("acme_sqs", SqsDescriptor)
}
```

Equivalent constructors exist for `ConfiguratorPlugin`, `TransportRegistration`,
`middleware::bind::BindMiddlewareRegistration`, and
`middleware::connector::ConnectorMiddlewareRegistration`. Registration rejects empty or duplicate
names, and configured but unregistered plugins are rejected before any listener binds.

Rust plugin dynamic loading is intentionally unsupported: Rust trait-object ABI is not stable.
Plugins are statically linked Cargo dependencies. `cargo-fujin` resolves external crates,
generates explicit `ApplicationBuilder` calls, preserves `Cargo.lock`, and produces a binary or a
stable C ABI library.

Runtime logging uses `FUJIN_LOG_LEVEL` with `DEBUG`, `INFO`, `WARN`, or `ERROR`;
`FUJIN_LOG_TYPE=json` selects structured JSON output. On Unix, SIGHUP reloads
`FUJIN_LOG_LEVEL` together with startup-only configurator connector reload behavior.

## Configuration

The selected configurator loads one complete YAML or JSON bootstrap document:

- `FUJIN_CONFIGURATOR=file` reads the first existing comma-separated path in
  `FUJIN_CONFIGURATOR_FILE_PATHS`; defaults are `./config.yaml`, `conf/config.yaml`, and
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
        tcp_keepalive:
          time: 60s
          interval: 10s
          retries: 5
        fujin:
          ping_interval: 5s
          ping_timeout: 10s
          ping_max_retries: 3
          write_buffer_size: 4194304
          write_deadline: 10s
          force_terminate_timeout: 15s
    - type: quic
      settings:
        addr: 0.0.0.0:4848
        max_concurrent_bidi_streams: 1000
        max_idle_timeout: 30s
        keep_alive_interval: 10s
        tls: { enabled: true, server_cert_pem_path: /certs/server.pem, server_key_pem_path: /certs/server-key.pem }
grpc:
  enabled: true
  addr: 0.0.0.0:4849
  max_concurrent_streams: 1024
  max_decoding_message_size: 4194304
  max_encoding_message_size: 4194304
  initial_stream_window_size: 1048576
  initial_connection_window_size: 1048576
  http2_keepalive_interval: 2h
  http2_keepalive_timeout: 20s
  max_connection_age: 30m
  max_connection_age_grace: 5s
health:
  enabled: true
  addr: 0.0.0.0:8080
connectors:
  primary:
    type: kafka
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

Native `settings.fujin` controls protocol PING/PONG, bounded output, write deadlines, and graceful
STOP termination. QUIC fields mirror `quinn::TransportConfig`; gRPC fields mirror Tonic's
`Server` and generated service limit methods. Unknown settings are rejected rather than ignored.

## Deployment

### Docker

```bash
docker build --build-arg VERSION=v0.6.0 -t fujin .
docker run --rm -p 4850:4850 -p 8080:8080 \
  -v "$PWD/config.yaml:/config/config.yaml:ro" fujin
```

The default and published image uses [`deploy/docker/fujin.build.toml`](deploy/docker/fujin.build.toml),
which links `configurator-file`, `connector-kafka`, and `transport-tcp`. Supply another composition
manifest from the Docker build context to link a different set of built-in or third-party plugins:

```bash
docker build \
  --build-arg FUJIN_BUILD_MANIFEST=deploy/my-fujin.build.toml \
  --build-arg FUJIN_BUILD_LOCK=Cargo.lock \
  -t my-fujin .
```

`FUJIN_BUILD_LOCK` is resolved relative to the selected manifest and passed to `cargo-fujin` as an
immutable dependency lock. Set it to an empty value for an unlocked build. The manifest and all
local-path plugin crates must be present in the Docker build context. The generated Cargo project
is removed after the final `/fujin` executable is installed.

The image sets `FUJIN_CONFIGURATOR_FILE_PATHS=/config/config.yaml`. Its final stage is `scratch`:
only the statically linked Fujin binary, CA certificate bundle, and writable `/run/fujin` directory
are present. The process runs as numeric non-root user `65532`.

### Kubernetes

```bash
helm install fujin ./deploy/helm/fujin
helm install fujin ./deploy/helm/fujin --set mode=sidecar
```

See [`deploy/helm/fujin/values.yaml`](deploy/helm/fujin/values.yaml). Complete local broker stacks
are documented under [`resources/`](resources/README.md); the Kafka example combines separate
broker and Fujin Compose files and includes Go SDK producer/consumer commands. The deployment
Compose example remains under [`examples/deployment/`](examples/deployment/).

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

[`bench_report.md`](bench_report.md) records the current Session Core snapshot. Regenerate it with:

```bash
make bench-report
```

Historical Go-versus-Rust migration measurements are retained in Git history; active benchmark
tools exercise only the root Rust workspace.

## Documentation

- [Native Protocol Specification](protocol.md)
- [gRPC Proto Definition](crates/fujin-grpc-proto/proto/fujin.proto)
- [Development Configuration](config.dev.yaml)
- [Deployment Configuration](examples/assets/config/config.deployment.example.yaml)

## License

MIT. See [LICENSE](LICENSE).
