# AGENTS.md

Repository-wide instructions for Fujin. The active implementation is the Rust workspace at the
repository root. The final Go implementation is preserved by tag `v0.5.0` and branch
`legacy/go-v0.5`; do not restore Go source to the active branch.

## Project summary

Fujin is a high-performance message-broker gateway. Applications use either the Fujin v1 binary
protocol over TCP, QUIC, WebSocket, or Unix sockets, or the distinct protobuf gRPC API. Both
adapters delegate session semantics to one transport-neutral Session Core.

- Required Rust version: `rust-toolchain.toml`
- Workspace version: `Cargo.toml`
- Primary documents: `README.md`, `protocol.md`, package-local crate documentation
- gRPC schema: `proto/grpc/v1/fujin.proto`

## Source of truth

1. Current executable Rust code and public interfaces.
2. Tests defending observable behavior.
3. `protocol.md` for the native wire contract.
4. README and deployment examples for user configuration.
5. `bench_report.md` and Git history as retained performance and migration evidence.

## Non-negotiable invariants

### Shared session semantics

- `crates/fujin-core` owns BIND, produce, fetch, subscribe, settlement, transaction, cleanup, and
  client-visible operation semantics.
- `crates/fujin-native` and the gRPC adapter in `crates/fujin-runtime` must not implement divergent
  business rules.
- Semantic changes normally require native and gRPC session-adapter coverage.
- Client-visible failures use `fujin_error::CoreError`; keep status, outcome, reason, message, and
  details consistent between adapters.

### Connector generations

- A successful BIND pins one immutable connector generation until the session closes.
- Never migrate an active session, transaction, reader, writer, subscription, or settlement to a
  newer generation.
- Compile and validate a complete replacement before atomic publication.
- Rejected replacement leaves the current generation untouched.
- Retired generations drain until their final lease closes, then release owned runtimes and
  middleware resources.
- Connector compilation performs no broker I/O; generation runtimes open broker resources lazily.

### Connector contracts

- Route profiles are immutable capability and guarantee contracts returned by BIND.
- Headers are an unordered byte multimap; keys are non-empty UTF-8, values are arbitrary bytes, and
  duplicates are valid.
- Writer completions are exactly once. Flush is a snapshot barrier. Close resolves pending work.
- Preserve reader readiness, delivery ordering, settlement, and opaque message-ID contracts.
- Copy payload, header, or message-ID bytes retained beyond callback ownership.

### Protocol and transport

- Native v1 is an incremental byte stream with no command delimiters. Reads may fragment or
  coalesce any field or frame.
- WebSocket message boundaries are not Fujin frame boundaries; only binary messages are valid.
- One QUIC bidirectional stream carries one Fujin session. Connection probes use dedicated streams.
- Keep socket I/O outside shared-state locks.
- Preserve bounded output queues, write deadlines, STOP delivery, and deterministic shutdown.

### Runtime lifecycle

- Runtime connector updates are complete snapshots, not patches.
- Snapshot application is serialized; only the newest pending full snapshot may remain queued.
- New snapshots affect later BIND operations only.
- Watchers start after every configured listener is ready and stop with the application lifecycle.
- Unix binary upgrade transfers listener descriptors through SCM_RIGHTS; readiness is reported only
  after inherited listeners are serving.

### Performance and ownership

- Native parsing and response generation are hot paths. Avoid needless allocation, copying,
  reflection, boxing, cloning, task spawning, and dynamic dispatch per message.
- Do not hold locks across network or broker I/O.
- Every task, watcher, listener, reader, writer, runtime, and subscription requires a close or
  cancellation path.

## Repository map

| Path | Responsibility |
|---|---|
| `apps/fujin/` | Production binary and built-in feature composition |
| `crates/fujin/` | Embedding facade, CLI lifecycle, public plugin surface |
| `crates/fujin-core/` | Shared Session Core |
| `crates/fujin-native/` | Native decoder, session adapter, and response encoding |
| `crates/fujin-runtime/` | Application lifecycle, gRPC, health, reload, and upgrade |
| `crates/fujin-connector/` | Connector contracts, catalog generations, and writer contract |
| `crates/fujin-configurator/` | Bootstrap and runtime configuration contracts |
| `crates/fujin-middleware/` | Bind and connector middleware contracts |
| `crates/fujin-transport/` | Transport interfaces, listener registry, TLS, handoff |
| `crates/fujin-grpc-proto/` | Generated protobuf crate build seam |
| `crates/fujin-ffi/` | Stable C ABI |
| `plugins/` | Built-in configurator, connector, and transport crates |
| `tools/cargo-fujin/` | Custom static composition CLI |
| `tools/bench/` | Native and gRPC benchmark harnesses |
| `proto/grpc/v1/` | Authoritative protobuf schema |
| `examples/` | Deployment and configuration examples |
| `resources/` | Broker and observability dependencies |
| `deploy/helm/fujin/` | Helm chart |

## Build and test

```bash
make build
make fmt
make lint
make check
make test
```

Prefer focused commands while iterating:

```bash
cargo test -p fujin-core
cargo test -p fujin-native
cargo test -p fujin-runtime --all-features
cargo test -p fujin-transport-tcp
cargo test -p fujin-transport-quic
```

Broker-backed Kafka validation:

```bash
make e2e-kafka
```

For listener, protocol, runtime lifecycle, or upgrade changes, smoke-test the real path after tests.
Do not use longer timeouts, swallowed errors, retries, or mock-only substitutes to hide a race or
broker-dependent failure.

## Feature and platform expectations

`fujin-app` features select built-in configurators, connectors, transports, and gRPC. `full` enables
the production bundle. Minimal builds must compile without accidental references to disabled
plugins.

- Unix: TCP, QUIC, WebSocket, Unix sockets, gRPC, SIGHUP, and listener-FD handoff.
- Windows: TCP, QUIC, WebSocket, gRPC, and normal connector operation; Unix sockets, SIGHUP, and FD
  handoff are unavailable.

When touching platform-specific or feature-gated code, compile the relevant enabled and disabled
configurations.

## Plugin development

- Each built-in plugin is one leaf crate under `plugins/<family>/<name>`.
- Export a stable `plugin()` constructor.
- Parse untyped settings into validated immutable configuration before serving.
- Connector compile functions perform no broker I/O.
- Transport plugins implement readiness, terminal completion, inherited listener handling where
  supported, and bounded session shutdown.
- Rust dynamic plugin loading is unsupported. `cargo-fujin` generates statically linked
  compositions.
- Add broker-backed coverage when a behavior depends on remote acknowledgement, settlement,
  reconnect, or transaction semantics.

## Protocol changes

Native protocol changes require coordinated updates to `protocol.md`, `crates/fujin-native`, shared
Session Core semantics where applicable, fragmentation tests, adapter contracts, and compatible
SDKs.

If `proto/grpc/v1/fujin.proto` changes:

```bash
make generate
cargo test -p fujin-grpc-proto -p fujin-runtime --all-features
```

Do not commit Cargo build output or generated protobuf artifacts.

## Benchmark discipline

- Reproduce and fix hangs before broad matrices.
- Compare equivalent source, harness, toolchain, features, operation counts, payloads, and machine
  conditions.
- Interleave variants when possible.
- Never certify partial, timed-out, invalid, or mismatched samples.
- `tools/bench` uses fixed local listener addresses; never run overlapping suites.
- `bench_report.md` is the retained root-workspace snapshot. Historical migration comparisons are
  evidence, not the active benchmark contract.

## Documentation and release hygiene

- Update `protocol.md` for native wire behavior.
- Update README, examples, and deployment manifests for public configuration or lifecycle changes.
- Update public Rust docs at the owning interface.
- Rust releases use namespaced Git tags such as `fujin/v0.6.0-alpha.1`; product, Cargo, Helm, and
  image versions omit the `fujin/` namespace.
- Do not create new root semantic-version tags: they belong to the historical Go module namespace.
- Do not edit vendored dependencies, Cargo build output, binaries, sockets, or benchmark scratch
  artifacts.

## Definition of done

- The changed behavior works through its real execution path.
- Focused tests pass, plus broader checks justified by the affected contracts.
- Feature/platform combinations are checked when touched.
- Protocol, examples, deployment files, and public docs are updated when the contract changes.
- No temporary process, listener, broker stack, generated binary, or benchmark job remains active.
- Final reports name exact verification commands and intentionally omitted checks.
