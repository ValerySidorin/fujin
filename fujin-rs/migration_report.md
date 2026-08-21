# Fujin Go-to-Rust Production Migration Evidence

**Date:** 2026-08-21  
**Host:** Apple M2, Darwin 24.6.0 arm64  
**Rust:** 1.97.1  
**Production image tested:** `fujin:rust-cutover`, build version `v0.5.0-rust`

## Production cutover

The supported production entry points now select the Rust implementation:

- `make build` builds `fujin-rs/crates/fujin` with the `full` feature set.
- `Dockerfile` builds and packages the Rust binary as an unprivileged Alpine container.
- `.github/workflows/release-image.yml` publishes and smoke-tests the Rust image.
- Docker Compose and Helm use `FUJIN_CONFIG=/config/config.yaml` and `RUST_LOG`.
- The Go default executable, Go production build script, and Go-only ZeroMQ production image/workflow were removed.

The Go implementation remains in the repository only for compatibility tests, migration comparisons, and non-production library work. No active release or deployment definition invokes `go run ./cmd/builder`, `build.sh`, or the former Go configurator environment variables.

## Delivered Rust runtime

- One shared Session Core for native v1 and gRPC semantics.
- TCP, QUIC, Unix socket, and WebSocket native transports.
- TLS and mutual TLS for stream transports; mandatory TLS for QUIC.
- WebSocket path, Origin allowlist, and message-size enforcement.
- gRPC transport limits, TLS, and standard `grpc.health.v1.Health` reporting.
- HTTP liveness and readiness endpoints.
- Immutable connector generations and SIGHUP connector-snapshot reload on Unix.
- Kafka produce, subscribe, fetch, manual settlement, headers, and transactions.
- Process-lifetime dynamic connector loading through a versioned entry symbol.

Unsupported Go-only settings are rejected instead of ignored. These currently include native ping/write tuning, gRPC client keepalive enforcement, gRPC `connection_timeout`, and `server_keepalive.max_connection_idle`.

## Correctness evidence

### Complete Rust workspace

```text
cargo fmt --all
cargo test --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

Result: **58 tests passed across 17 suites; clippy passed with warnings denied.**

### Kafka broker-backed test

A real Kafka container was started through `resources/docker-compose.kafka.yaml`; cleanup ran after the test.

```text
FUJIN_KAFKA_E2E=1 cargo test -p fujin-kafka --features rdkafka \
  --test kafka_e2e -- --nocapture
```

Result: **1 test passed**. Covered broker acknowledgement, header delivery, subscription, offset settlement, and transaction commit.

### Transport smoke matrix

Each cell performed 100 end-to-end 128-byte PRODUCE operations with two concurrent sessions through the real Rust server adapter and NOP connector.

| Adapter | Result | Median operation time from smoke run | p99 |
|---|---:|---:|---:|
| TCP | PASS | 31,749 ns | 83,291 ns |
| QUIC | PASS | 40,845 ns | 128,625 ns |
| Unix | PASS | 11,319 ns | 40,708 ns |
| WebSocket | PASS | 23,071 ns | 92,833 ns |
| gRPC | PASS | 34,720 ns | 106,125 ns |

### Packaged binary and container

- `make build VERSION=v0.5.0-rust` completed and `./bin/fujin --version` returned the requested version.
- Helm rendered successfully and Docker Compose configuration validation passed.
- The Alpine production image built successfully after installing its explicit protobuf and native Kafka build dependencies.
- `docker run --rm fujin:rust-cutover --version` returned `version: v0.5.0-rust`.
- A real container reported `/readyz = ok` and completed native HELLO over the published TCP port, returning build `v0.5.0-rust`.
- The retained Go comparison tree compiled across 34 packages with 35 additional no-test packages.

## Repeated Go/Rust comparison

The comparison used the same host, NOP connector semantics, payload, concurrency, operation count, and synchronous request/response contract. Each common TCP/gRPC cell was run **five times**; the table reports the median. Small payload cells used 10,000 operations per sample; 1 MiB cells used 1,000. Raw measurements were validated as 180 rows: 90 Go and 90 Rust.

Negative delta means Rust completed an operation faster.

| Payload | Concurrency | Adapter | Go ns/op | Rust ns/op | Rust delta | Go p99 | Rust p99 | p99 delta |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| 1 B | 1 | TCP | 23,816 | 17,869 | -25.0% | 63,250 | 28,958 | -54.2% |
| 1 B | 1 | gRPC | 37,388 | 27,387 | -26.7% | 74,875 | 44,458 | -40.6% |
| 1 B | 16 | TCP | 9,212 | 4,673 | -49.3% | 302,917 | 140,791 | -53.5% |
| 1 B | 16 | gRPC | 11,676 | 5,845 | -49.9% | 490,625 | 178,042 | -63.7% |
| 1 B | 128 | TCP | 9,104 | 4,777 | -47.5% | 1,848,083 | 1,026,125 | -44.5% |
| 1 B | 128 | gRPC | 11,070 | 4,968 | -55.1% | 3,499,333 | 1,143,834 | -67.3% |
| 128 B | 1 | TCP | 22,376 | 18,775 | -16.1% | 36,083 | 30,125 | -16.5% |
| 128 B | 1 | gRPC | 37,819 | 27,694 | -26.8% | 75,625 | 45,166 | -40.3% |
| 128 B | 16 | TCP | 9,177 | 4,673 | -49.1% | 295,625 | 139,542 | -52.8% |
| 128 B | 16 | gRPC | 11,841 | 5,812 | -50.9% | 448,291 | 172,042 | -61.6% |
| 128 B | 128 | TCP | 9,045 | 4,730 | -47.7% | 1,805,916 | 1,054,209 | -41.6% |
| 128 B | 128 | gRPC | 11,310 | 4,980 | -56.0% | 3,868,917 | 1,140,083 | -70.5% |
| 1 MiB | 1 | TCP | 160,421 | 123,262 | -23.2% | 501,167 | 178,625 | -64.4% |
| 1 MiB | 1 | gRPC | 406,039 | 411,161 | +1.3% | 900,125 | 528,042 | -41.3% |
| 1 MiB | 16 | TCP | 100,080 | 97,798 | -2.3% | 3,501,375 | 2,805,625 | -19.9% |
| 1 MiB | 16 | gRPC | 232,202 | 225,985 | -2.7% | 10,372,584 | 6,050,625 | -41.7% |
| 1 MiB | 128 | TCP | 168,964 | 144,069 | -14.7% | 33,513,792 | 29,694,041 | -11.4% |
| 1 MiB | 128 | gRPC | 253,990 | 256,679 | +1.1% | 95,275,417 | 52,804,250 | -44.6% |

Across the 18 common cells, Rust's geometric-mean operation-time delta was **-33.0%** and its geometric-mean p99 delta was **-48.6%**. Rust was faster in 16 of 18 median operation-time cells and had lower p99 in all 18. The two operation-time regressions were 1 MiB gRPC at concurrency 1 (+1.3%) and 128 (+1.1%); these small deltas were not treated as statistically significant claims because this comparison reports five-sample medians rather than confidence intervals.

## Cleanup evidence

- Every Fujin smoke process and Kafka container stack was stopped.
- Local `bin/` and Rust `target/` artifacts were removed before final validation and rebuilt only as needed.
- Docker Desktop was recovered from a stuck daemon state.
- `docker builder prune -af` reclaimed **21.95 GB** of build cache.

## Cutover conclusion

The Rust binary is the verified build, container, Compose, Helm, and release path. Native and gRPC behavior, Kafka broker semantics, all transport adapters, dynamic plugin loading, shutdown, and readiness have direct execution evidence. The retained Go tree is not referenced by production deployment assets and exists only as migration support code.
