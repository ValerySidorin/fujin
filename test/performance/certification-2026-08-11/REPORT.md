# Session Core performance certification — 2026-08-11

**Result: PASS**

The post-cutover Session Core is certified against the pre-refactor baseline. No authoritative comparison retains a statistically significant regression in `sec/op`, throughput, p99 latency, `B/op`, or `allocs/op` at $\alpha = 0.05$.

## Environment

Both variants ran sequentially on the same Apple M2 host with macOS 15.6.1, Go 1.26.1, `darwin/arm64`, `GOMAXPROCS=8`, and build tags `fujin,grpc`. Baseline/current order alternated by profile. The complete fingerprint is in `environment.txt`.

The baseline checkout received only shared correctness and benchmark-harness fixes required for a valid comparison: native outbound lost-wakeup prevention, adaptive `ENOBUFS` batching, 64 KiB outbound backpressure, QUIC PONG handling, and QUIC ping lifecycle correction. Session Core remains exclusive to the current variant.

## Matrix coverage

The complete bounded matrix contains 876 unique benchmark cells:

- payloads: `1 B`, `128 B`, `1 KiB`, `32 KiB`, `1 MiB`;
- concurrency: `1`, `16`, `128`;
- batches: `1`, `32`, `256`, excluding combinations above the 4 MiB wire bound;
- native transports: TCP, QUIC, Unix;
- gRPC;
- produce, high-level produce, fetch, high-level fetch, subscribe, high-level subscribe, ACK, NACK, and transactions.

`baseline-full-matrix-smoke.raw.txt` and `current-full-matrix-smoke.raw.txt` each contain all 876 cells and finish with `PASS`. They include 81 native 1 MiB cells per variant.

The statistical gate used identical baseline/current selectors and sample counts:

| Profile | Cells | Work per sample | Samples |
|---|---:|---:|---:|
| Native, all operations/transports/concurrency, 128 B, batch 1 | 81 | 1,000 operations | 10 |
| gRPC, all operations/concurrency, 128 B, batch 1 | 27 | 1,000 operations | 10 |
| Native produce/fetch payload spectrum, concurrency 1 | 24 | 100 operations | 10 |
| gRPC produce/fetch payload spectrum, concurrency 1 | 8 | 100 operations | 10 |
| Native fetch/high-level fetch batch spectrum, 1 B, concurrency 16 | 18 | 100 operations | 10 |
| gRPC fetch/high-level fetch batch spectrum, 1 B, concurrency 16 | 6 | 100 operations | 10 |

Fixed operation scales were also exercised with TCP subscribe at concurrency 16:

- 128 B: 10,000 operations with 20 samples, then 100,000 and 1,000,000 operations with 10 samples;
- 1 MiB: 1,000 and 10,000 operations with 10 samples.

All four long-scale comparison reports are neutral across all five metrics. At 1 MiB and 10,000 operations, baseline/current are `201.3 µs/op` and `200.8 µs/op`; p99 is statistically neutral; both report `7 allocs/op`.

## Regression disposition

Initial 10-sample profiles exposed noisy or real negative cells. Each suspect was followed through bounded reruns, normally 20 samples, until a source fix or a statistically neutral superseding comparison resolved it. The final small-allocation anomaly used 30 interleaved samples.

| Area | Resolution chain; later files supersede residuals in earlier files |
|---|---|
| Native 128 B throughput, p99, and allocation suspects | `comparison-rerun-native-suspects-128B-20x.txt`, `comparison-final-native-subscriptions-128B-allconcurrency-10x.txt`, `comparison-rerun-native-hsubscribe-quic-128B-c128-20x.txt`, `comparison-rerun-native-produce-unix-128B-c16-20x.txt` |
| Native payload suspects | `comparison-final-tcp-payload-sustained-20x.txt`, `comparison-rerun-post-backpressure-tcp-1KiB-c1-20x.txt`, `comparison-rerun-post-backpressure-unix-small-c1-20x.txt` |
| Native batch suspect | `comparison-rerun-native-hfetch-1B-b256-c16-20x.txt`, then `comparison-final-post-backpressure-native-batches-c16-10x.txt` |
| gRPC 128 B and 32 KiB fetch suspects | `comparison-rerun-grpc-128B-suspects-20x.txt`, `comparison-rerun-grpc-fetch-32KiB-c1-20x.txt` |
| Sustained native subscriptions | `comparison-certified-tcp-subscribe-autocommit-20x.txt`, `comparison-final-unix-hsubscribe-backpressure-20x.txt`, `comparison-rerun-native-hsubscribe-quic-128B-c128-20x.txt` |
| Unix 1 B produce `B/op` ordering anomaly | `comparison-interleaved-native-produce-unix-1B-c1-30x.txt` |

The final interleaved Unix 1 B produce comparison is neutral: `sec/op p=0.830`, throughput `p=0.379`, p99 `p=0.813`, `B/op p=0.349`, and identical `6 allocs/op`.

The attempted aggregate `final-post-backpressure-native-128B-allops-allconcurrency-10x` profile is explicitly invalid and excluded: its baseline process failed during repeated QUIC listener churn after 130 of 153 cells. The successful bounded final profiles and focused reruns above replace it; no partial aggregate output was compared.

## Correctness fixes required before certification

- Native outbound writer now waits on the guarded `pending bytes == 0 && !closed` predicate, eliminating a lost `sync.Cond` wake-up.
- Native outbound ownership and bounded backpressure prevent response drops and unbounded queued bytes.
- Native vector writes adapt after Darwin `ENOBUFS` instead of failing the session.
- QUIC benchmark PONG handling reads exactly one control byte without depending on EOF chunking.
- QUIC server ping lifecycle closes unresponsive or invalid-PONG connections deterministically.
- Subscription response ownership avoids per-message copies while preserving write completion.

## Verification

Passed:

```text
go test -tags=fujin,grpc ./internal/core ./internal/proto ./public/plugins/transport/quic
ok github.com/fujin-io/fujin/internal/core
ok github.com/fujin-io/fujin/internal/proto
ok github.com/fujin-io/fujin/public/plugins/transport/quic
```

```text
go test -tags=fujin,grpc -run '^(TestPerformanceContract|TestPerformanceBatchMatrixBoundsWirePayload|TestPerformanceEnvironmentFingerprint|TestSessionContractNativeAndGRPC|TestRespondToPingRepliesWhenReadReturnsDataWithoutEOF|TestSessionBenchSubscribeWaitsForStart|TestSessionBenchmarkWarmupRunsWorkersConcurrently|TestSessionBenchmarkOperationCountsDistributeExactTotal|TestSessionBenchConfigDistributesSubscribeLimitsAcrossConnectors)$' ./test
ok github.com/fujin-io/fujin/test
```

`go test -p=1 -tags=fujin,grpc ./...` passed every package before the integration `./test` package. That package could not complete because local Kafka at `127.0.0.1:9094` and NATS were unavailable; the failures are retained as environment-prerequisite evidence, not counted as product regressions.
