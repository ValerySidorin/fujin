# Contributing to Fujin

Fujin's active implementation is the Rust workspace at the repository root. The final Go release is
`v0.5.0`; its source remains available through that tag and the `legacy/go-v0.5` branch.

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- Make
- Docker and Docker Compose for broker-backed tests and container validation
- A C/C++ toolchain, CMake, and pkg-config for the Kafka connector
- Protocol Buffers compiler support; `prost-build` uses the vendored `protoc` fallback when needed

## Setup

```bash
git clone https://github.com/fujin-io/fujin.git
cd fujin
make build
make test
```

## Repository structure

```text
apps/fujin/                 Production binary composition
crates/fujin/               Embedding facade and public plugin API
crates/fujin-core/          Transport-neutral Session Core
crates/fujin-native/        Incremental native protocol codec and adapter
crates/fujin-runtime/       Listener, gRPC, health, reload, and upgrade lifecycle
crates/fujin-connector/     Connector contracts and immutable generations
crates/fujin-transport/     Shared transport contracts and listener handoff
plugins/                    Built-in configurator, connector, and transport crates
tools/cargo-fujin/          Custom composition CLI
tools/bench/                Native and gRPC benchmark harnesses
proto/grpc/v1/              Authoritative protobuf API
examples/                   Deployment configuration and Compose examples
resources/                  Broker and observability dependencies
deploy/helm/fujin/          Helm chart
```

## Building

```bash
# Full production binary
make build

# Minimal built-in composition
cargo build --release -p fujin-app --no-default-features \
  --features configurator-file,connector-kafka,transport-tcp

# Custom composition tool
cargo install --path tools/cargo-fujin --locked
cargo fujin init --fujin-path ./crates/fujin
```

`VERSION` overrides the build string reported by `fujin --version` and native HELLO. Rust releases
use namespaced Git tags such as `fujin/v0.6.0-alpha.1`; the product, Cargo, Helm, and image version is
`v0.6.0-alpha.1` without the `fujin/` namespace.

## Validation

Run focused package tests while iterating, then the relevant broader checks:

```bash
cargo test -p fujin-core
cargo test -p fujin-native
cargo test -p fujin-runtime --all-features
cargo test -p fujin-transport-tcp

make fmt
make lint
make check
make test
```

For a Kafka-backed contract:

```bash
make e2e-kafka
```

The target starts Kafka, runs `plugins/connector/kafka/tests/kafka_e2e.rs`, and removes the broker
stack. Do not replace broker acknowledgement, settlement, or transaction tests with mocks.

## Code style

- Run `cargo fmt` on touched Rust code.
- Keep Clippy clean under workspace lints.
- Avoid allocation and copying in native parsing, response encoding, connector callbacks, and
  transport loops.
- Never hold locks across broker or network I/O.
- Every spawned task, listener, reader, writer, and watcher needs a bounded cancellation path.
- Preserve connector generation pinning and shared native/gRPC Session Core semantics.

## Protocol changes

Native wire changes require coordinated updates to `protocol.md`, `fujin-native`, fragmentation and
contract tests, and compatible SDKs.

The protobuf source is `proto/grpc/v1/fujin.proto`. After editing it:

```bash
make generate
cargo test -p fujin-grpc-proto -p fujin-runtime --all-features
```

Generated Rust bindings are emitted into Cargo build output and must not be committed.

## Plugins

Each built-in plugin is an independent crate under `plugins/<family>/<name>`. Export a stable
`plugin()` constructor and register it through `ApplicationBuilder` or generated `cargo-fujin`
composition. Rust dynamic plugin loading is intentionally unsupported.

A plugin change must include configuration validation, focused tests, and user documentation. Add a
broker-backed test when behavior depends on remote acknowledgement, settlement, reconnect, or
transactions.

## Pull requests

1. Use a focused feature branch.
2. Reuse existing contracts and lifecycle seams; do not introduce parallel registries or error
   models.
3. Update every affected caller, adapter, example, and document in the same change.
4. Run focused tests, then the validation required by the changed contract.
5. Include exact verification commands and any intentionally omitted broker or platform checks.

## License

Contributions are licensed under the MIT License.
