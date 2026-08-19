# AGENTS.md

Repository-wide instructions for coding agents working on Fujin. This file applies to the entire repository unless a more specific nested `AGENTS.md` overrides it.

## Project Summary

Fujin is a high-performance message-broker gateway written in Go. Applications use either:

- the Fujin v1 binary protocol over TCP, QUIC, WebSocket, or Unix sockets; or
- the distinct protobuf gRPC wire API.

Both interfaces delegate session semantics to the same Session Core and expose connectors for Kafka, NATS, RabbitMQ, AMQP 1.0, Redis/Valkey, MQTT, and NSQ.

- Module: `github.com/fujin-io/fujin`
- Required Go version: use the version declared in `go.mod`
- Primary documents: `README.md`, `protocol.md`, package-local plugin READMEs

## Source of Truth

Use this precedence when documents disagree:

1. current executable code and public interfaces;
2. tests that defend observable behavior;
3. `protocol.md` for the native wire contract;
4. package-local plugin READMEs for user configuration;
5. `plans/` and `issues/` for design history and acceptance criteria.

Plans can describe an earlier interface. Do not copy a planned API into production without reconciling it with current code and tests.

## Non-Negotiable Invariants

### Shared session semantics

- `internal/core` owns transport-neutral BIND, produce, fetch, subscribe, settlement, transaction, cleanup, and error semantics.
- Native and gRPC adapters must not implement divergent business rules.
- A semantic change normally needs coverage through both native and gRPC session contracts.
- Client-visible failures must flow through `core.ClassifyError`; keep status, outcome, reason, message, and details consistent across wire adapters.

### Connector generations

- A successful BIND pins one immutable `connector.Generation` until the session closes.
- Never migrate an active session, transaction, reader, writer, subscription, or settlement operation to a newer generation.
- Compile and validate a complete replacement before atomic publication.
- A rejected replacement leaves the current generation untouched.
- Removed or replaced generations drain until their final `Binding` closes, then release generation-owned runtimes and middleware resources.
- Connector compilation must not perform broker I/O. Broker resources are opened lazily by the generation-owned runtime when an operation first needs them.

### Runtime configuration

- Runtime updates are complete connector snapshots, not patches.
- Snapshot application is serialized. Never apply two connector snapshots concurrently.
- Revisions are monotonic: duplicate content at the active revision is an accepted no-op; conflicting content at the same revision is rejected; older revisions are stale.
- While an apply is running, the queue may retain only the newest pending full snapshot. Superseded requests must receive a terminal result.
- Runtime updates currently cover connectors only. Do not silently extend them to transports, gRPC listeners, health listeners, logging bootstrap, or control-plane bootstrap settings.
- Snapshot acceptance means local compilation and publication succeeded. It does not prove broker reachability or readiness.

### Connector contracts

- `RouteProfile` is the immutable capability and guarantee contract returned by BIND. Unsupported operations return `connector.ErrOperationUnsupported`; never degrade silently.
- Header representation is an unordered alternating key/value byte multimap. Keys are non-empty UTF-8; values are arbitrary bytes; duplicate keys are valid.
- Writer callbacks are exactly once. `Flush` is a snapshot barrier for operations accepted before the call. `Close` deterministically resolves pending callbacks.
- Use `connector.EnforceWriterContract` unless an implementation deliberately and correctly marks itself `WriterContractCompliant`.
- Reader readiness and callback ordering in `public/plugins/connector.Reader` are contractual. Preserve them when adapting broker APIs.
- Message IDs are opaque adapter payloads wrapped by Session Core with session/incarnation sequencing. Validate before decoding and do not reuse settled IDs.

### Protocol streams

- Native v1 is an incremental byte-stream protocol with no command delimiters. Parsers and clients must tolerate arbitrary fragmentation and coalescing.
- WebSocket message boundaries are not Fujin frame boundaries; only binary WebSocket messages are valid.
- A QUIC bidirectional stream carries one Fujin session. Connection-level health probes use dedicated streams.
- Do not assume one `Read` returns one frame, one field, or EOF with the final byte.
- Keep socket I/O outside shared-state mutexes. Guard `sync.Cond` waits with the predicate loop under the same mutex used by mutation and signaling.

### Performance and ownership

- Native parsing and response generation are hot paths. Avoid avoidable allocations, copies, reflection, interface churn, and per-message goroutines.
- Reuse existing pools and buffer ownership conventions. Return pooled buffers exactly once.
- If an implementation retains payload, header, or message-ID bytes after a callback returns, copy them unless the relevant API explicitly transfers ownership.
- Do not hold locks across broker or network I/O.
- Preserve bounded shutdown and cleanup. Every goroutine, watcher, listener, runtime, reader, and writer needs a cancellation or close path.

## Repository Map

| Path | Responsibility |
|---|---|
| `cmd/main.go` | Default executable importing all plugin bundles |
| `cmd/builder/` | Builds minimal binaries from selected plugin package imports |
| `public/service/` | Bootstrap config, runtime connector snapshots, signals, process lifecycle, binary upgrade coordination |
| `public/server/` | Transport/gRPC/health orchestration and connector catalog ownership |
| `public/plugins/transport/` | Native protocol transport registry and TCP, QUIC, WebSocket, Unix implementations |
| `public/plugins/connector/` | Connector public contracts, registry, immutable catalog generations, writer contract |
| `public/plugins/configurator/` | Bootstrap loaders and optional runtime connector source contracts |
| `public/plugins/middleware/bind/` | BIND-time authentication/authorization middleware |
| `public/plugins/middleware/connector/` | Reader/writer middleware, including compiled generation-scoped middleware |
| `public/proto/fujin/v1/` | Public native protocol constants and stream adapter seam |
| `public/proto/grpc/v1/` | Protobuf schema and generated Go code |
| `internal/core/` | Shared Session Core and transport-neutral error classification |
| `internal/proto/` | Native parser, handler adapter, response encoding, outbound queue, protocol pools |
| `internal/connectors/` | Session-scoped connector binding and reader/writer lease management |
| `internal/transport/grpc/v1/server/` | gRPC lifecycle and protobuf-to-Core adapter |
| `internal/upgrade/` | Unix listener-FD passing and upgrade control protocol |
| `internal/health/` | Liveness/readiness HTTP server |
| `test/` | Cross-adapter contracts, broker E2E tests, benchmark harnesses |
| `examples/` | Complete configs, embedding, plugin examples, deployment examples |
| `resources/` | Docker Compose dependencies and observability resources |
| `issues/` | Tracked implementation acceptance criteria; checked boxes are historical evidence, not runtime truth |
| `plans/` | Architecture and sequencing documents |

## Build Tags and Platforms

- `fujin`: enables the `public/plugins/transport/all` native transport bundle.
- `grpc`: enables the gRPC implementation; without it, the stub compiles.
- `unix`: platform tag selected on Unix targets for Unix sockets, SIGHUP reload, and listener-FD upgrade code.

The transport packages can also be imported individually by `cmd/builder`. gRPC is not a native transport plugin: it has a separate protobuf wire format and an internal adapter to Session Core.

Standard validation uses:

```bash
go test -tags=fujin,grpc ./...
```

Platform expectations:

- Linux/macOS/BSD: native transports, SIGHUP reload, and graceful FD handoff are available.
- Windows: TCP, QUIC, WebSocket, gRPC, and normal connector operation are available; Unix sockets, SIGHUP reload, and graceful binary upgrade use stubs or are unavailable.
- Default builds use `CGO_ENABLED=0` through the custom builder unless `-cgo` is explicitly selected.

When changing build-tagged code, compile both sides of the tag and relevant target platforms. Do not add an unconditional reference to a symbol that exists only under one build constraint.

## Standard Development Workflow

1. Read the complete affected construct and nearby tests before editing.
2. Reuse the existing package seam; do not create a second registry, lifecycle, error model, or configuration path.
3. Identify the observable contract and write or select the narrowest reproducing test.
4. Implement at the owning layer, then update every adapter and caller affected by that contract.
5. Run `gofmt` on touched Go files.
6. Run focused tests first, then broader tests appropriate to the risk.
7. Smoke-test the actual path for listener, protocol, upgrade, or runtime-lifecycle changes.
8. Update user documentation, protocol specification, examples, and plugin index when the external contract changes.
9. Do not leave generated binaries, benchmark output, temporary certificates, sockets, or broker data in the repository.

Treat unexpected working-tree changes as user work. Adapt to them; do not discard or rewrite unrelated changes.

## Build and Run

Full binary with all built-in plugins and native plus gRPC interfaces:

```bash
make build
```

Run using the development YAML configurator:

```bash
make run
```

Equivalent explicit bootstrap:

```bash
FUJIN_CONFIGURATOR=yaml \
FUJIN_CONFIGURATOR_YAML_PATHS=./config.dev.yaml \
./bin/fujin
```

Minimal binary example:

```bash
go run ./cmd/builder -local \
  -transport github.com/fujin-io/fujin/public/plugins/transport/tcp \
  -configurator github.com/fujin-io/fujin/public/plugins/configurator/yaml \
  -connector github.com/fujin-io/fujin/public/plugins/connector/kafka/franz \
  -connector-middleware github.com/fujin-io/fujin/public/plugins/middleware/connector/prom \
  -tags "fujin" \
  -output ./bin/fujin-minimal
```

Builder flags are repeatable. A plugin must be imported into the generated binary to be available in its runtime registry.

## Testing

### Fast focused validation

Prefer package-level commands while iterating:

```bash
go test -tags=fujin,grpc ./internal/core
go test -tags=fujin,grpc ./internal/proto
go test -tags=fujin,grpc ./public/plugins/connector/...
go test -tags=fujin,grpc ./public/service ./public/server
go test -tags=fujin,grpc ./public/plugins/transport/websocket
```

Run one test repeatedly when diagnosing concurrency:

```bash
go test -tags=fujin,grpc -run '^TestName$' -count=100 ./path/to/package
```

Use the race detector for changed concurrency/lifecycle code:

```bash
go test -race -tags=fujin,grpc ./affected/package
```

### Broad validation

```bash
make test
make cross-build
```

`make test` runs `go test -v -tags=fujin,grpc ./...`. Do not use it as the first diagnostic tool when a focused package or scenario can fail faster.

For native/gRPC semantic changes, run the cross-adapter contract tests in `./test`, including `TestNativeSessionContract` and `TestGRPCSessionContract` where applicable.

For SDK-facing protocol changes, use the coordinated SDK checkout:

```bash
make sdk-compat FUJIN_GO_ROOT=../fujin-go
```

### Broker-backed E2E tests

Broker tests are skipped unless `FUJIN_E2E=1`. Prefer Make targets because they start and clean up the matching dependency:

```bash
make e2e-kafka_franz
make e2e-nats_core
make e2e-nats_jetstream
make e2e-rabbitmq_amqp09
make e2e-azure_amqp1
make e2e-redis_rueidis_pubsub
make e2e-redis_rueidis_streams
make e2e-mqtt_paho
make e2e-nsq
```

Do not silently replace a broker-backed contract with a mock-only test when remote acknowledgement, settlement, reconnect, or transaction semantics are the behavior under change.

## Verification by Change Type

| Change | Minimum evidence |
|---|---|
| Session Core semantics | `internal/core` tests plus native and gRPC session-contract coverage |
| Native parser/framing/encoding | `internal/proto` tests, fragmentation/boundary test, `protocol.md` update if externally visible |
| gRPC adapter or schema | gRPC server tests, session contract, regenerate protobuf if schema changed |
| Transport | transport package test plus a real listener/client smoke path; test shutdown and inherited listener behavior when touched |
| Connector | connector package tests, route-profile validation, callback/flush/settlement behavior; broker E2E when semantics depend on broker acknowledgement |
| Connector catalog/runtime reload | catalog, server, and service tests proving old BIND pinning, rejection safety, drain, and cleanup |
| Configurator watcher | bootstrap consistency, revision ordering, queue coalescing, cancellation, source connectivity, status reporting |
| Middleware | middleware package tests in every configured direction; verify failure behavior and concurrency safety |
| Binary upgrade | Unix upgrade tests and actual inherited-listener smoke where feasible; cross-build stubs |
| Performance-sensitive path | functional regression first, then controlled repeated benchmarks and statistical comparison |

## Benchmark Discipline

The approved session matrix is encoded in `test/performance_contract_test.go`:

- payloads: 1 B, 128 B, 1 KiB, 32 KiB, 1 MiB;
- concurrency: 1, 16, 128;
- batches: 1, 32, 256, bounded to 4 MiB payload per operation;
- native TCP/QUIC/Unix plus gRPC;
- 10 initial samples, 20 for inconclusive cells, alpha 0.05;
- allocation regressions block certification.

Useful focused controls:

```bash
FUJIN_BENCH_PAYLOAD=128B
FUJIN_BENCH_BATCH=1
FUJIN_BENCH_CONCURRENCY=16
FUJIN_BENCH_DEADLINE=30s
FUJIN_BENCH_QUIET=1
```

Run benchmarks with tests disabled explicitly:

```bash
go test -tags=fujin,grpc -run '^$' \
  -bench '^Benchmark_Name$' \
  -benchtime=10000x -count=10 -benchmem ./test
```

Rules:

- Reproduce and fix hangs before starting a full matrix.
- Never certify partial output, timed-out cells, invalid samples, or mismatched environments.
- Compare baseline and current with identical source-equivalent harnesses, Go version, tags, `GOMAXPROCS`, commands, and machine conditions.
- Interleave baseline/current variants when possible to reduce run-order bias.
- Use `benchstat` for `ns/op`, throughput, `B/op`, and `allocs/op`; compare emitted p99 separately.
- Benchmark servers use fixed local addresses: QUIC `:4848`, gRPC `:4849`, TCP `:4850`, WebSocket `:4851`, and Unix `/tmp/fujin-bench.sock`. Never run overlapping suites that bind the same addresses.
- `test/performance/` is ignored. Do not commit raw benchmark artifacts unless explicitly requested as release evidence.

## Plugin Development

Every built-in plugin must have:

1. a stable registered name;
2. side-effect registration through `init()`;
3. inclusion in the relevant `all/` package when it belongs in the full build;
4. package tests for configuration and behavior;
5. a package-local `README.md` with registered name, complete YAML, defaults, failure semantics, limits, and operational notes;
6. a link in the built-in plugin index in the root `README.md`.

Plugin configuration examples must match the actual YAML shape. Bind and connector middleware plugin-specific fields are inline beside `name` and `enabled`; do not place them under a nested `config:` key.

### Transport plugins

- Register with `transport.Register(name, parse, factory)`.
- Parse untyped user settings into validated typed configuration before constructing the server.
- Implement `ListenAndServe`, readiness, and terminal `Done` semantics.
- If graceful binary upgrade is supported, implement `ListenerFDProvider`, `ListenerInheritor`, and `FDKeyProvider` consistently.
- Adapt the transport to `session.Stream`; do not duplicate protocol parsing or Session Core logic.
- Preserve final protocol responses during shutdown. Interrupt reads without prematurely closing the write side.

### Connector plugins

- Register a static `connector.Descriptor`.
- `Descriptor.Compile` and override conversion are side-effect-free and perform no broker I/O.
- Return complete, validated `RouteProfile` values. A produce route must declare its real acceptance guarantee.
- Put shared physical broker resources in generation-owned `Runtime`; return session-scoped `ReadCloser` and `WriteCloser` leases.
- Preserve callback ordering, exactly-once completion, `Flush`, `Close`, transaction, and settlement contracts.
- Keep user configuration immutable after compilation. The catalog deep-clones maps, slices, pointers, interfaces, arrays, and exported struct fields; opaque leaves such as channels/functions retain identity.
- Add broker E2E coverage when a claim depends on real remote behavior.

### Configurator plugins

- Register with `configurator.Register`.
- `Load` is bootstrap-only and is called on one retained configurator instance.
- Implement `ConnectorBootstrapSnapshot` when bootstrap carries an external revision.
- Implement `ConnectorWatcher` for runtime connector snapshots; block until cancellation or terminal source failure.
- Submit complete immutable snapshots through `ConnectorRuntime`; do not mutate submitted maps afterward.
- Report source connectivity accurately and avoid including secret material in diagnostics/status.

### Middleware plugins

- Bind middleware handles BIND-time authentication/authorization only.
- Connector middleware wraps reader/writer leases and must preserve the underlying connector contracts.
- Middleware that compiles schemas, jq, WASM, or other resources should use generation-scoped compiled middleware and release resources in `Close`.
- Document whether failures reject produce, skip consume, pass through, or terminate the stream.
- State and test whether instances are safe for concurrent reader/writer use.

## Protocol and Protobuf Changes

Native protocol changes require coordinated updates to:

- `protocol.md`;
- opcode/constants under `public/proto/fujin/v1`;
- parser/handler/encoder code under `internal/proto`;
- Session Core when semantics change;
- contract and fragmentation tests;
- compatible client SDKs.

The gRPC API uses a separate protobuf wire format but should expose the same Session Core semantics and operation-error model.

If `public/proto/grpc/v1/fujin.proto` changes:

```bash
make generate
```

Never hand-edit `fujin.pb.go` or `fujin_grpc.pb.go`; they are generated files. Verify the generated API and run SDK compatibility when the public contract changes.

An incompatible native wire change requires deliberate protocol-version handling. Do not change field order, length encoding, opcodes, error envelopes, or message-ID layout casually.

## Runtime Reload and Process Upgrade

- SIGHUP reuses the selected configurator instance. For watcher-owned connector state, SIGHUP must not race or override the watcher.
- New connector snapshots affect only later BIND operations.
- Runtime source watchers start only after every listener reports ready and stop under the server lifecycle context.
- Graceful binary upgrade is Unix-only and passes duplicated listener FDs through `internal/upgrade` using SCM_RIGHTS.
- Preserve FD keys and metadata for TCP, QUIC/UDP, WebSocket/TCP, Unix, and gRPC/TCP listeners.
- TLS is re-applied by the new process after inheriting the raw listener.
- The new process must not signal readiness until inherited listeners are serving.

## Documentation Rules

Update documentation in the same change when behavior or configuration changes:

- native wire behavior: `protocol.md`;
- plugin behavior/configuration: package `README.md` and root plugin index when adding/removing/renaming;
- full configuration examples: `examples/assets/config/config.yaml` where appropriate;
- operator lifecycle/control-plane behavior: root README and relevant plan/issue documentation;
- exported Go contracts: GoDoc at the owning interface.

Prefer one authoritative detailed location and links from overview documents. Do not duplicate large configuration references that will drift.

Documentation and config examples are written in English to match the existing repository.

## Change Hygiene

- Use standard Go naming and `gofmt`; avoid repository-wide formatting unrelated to the task.
- Do not edit generated files, vendored module-cache code, binaries, or ignored benchmark outputs.
- Keep exported API changes intentional and migrate all in-repository callers in the same change.
- Remove obsolete code and comments after a clean cutover; do not leave compatibility aliases unless an external compatibility requirement justifies them.
- Do not weaken tests, increase timeouts, swallow errors, or add retries merely to hide a race or deadlock.
- Avoid logging payloads, credentials, certificate contents, connector secrets, or unredacted remote diagnostics.
- Keep errors contextual and compatible with `errors.Is`/`errors.As` where callers classify them.
- Use deterministic tests: bounded contexts, explicit readiness, no sleep-only synchronization, and no dependence on test order.

## Definition of Done

Before reporting completion:

- the changed behavior works through its real execution path;
- focused tests pass, plus broader tests justified by the affected contracts;
- race/build-tag/platform checks were run when relevant;
- protocol, generated code, examples, and plugin docs are updated where required;
- every changed public or plugin contract has observable test coverage;
- no temporary process, socket, broker stack, generated binary, or benchmark job remains active;
- the final report names the exact verification commands and any validation intentionally not run.
