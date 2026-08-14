# Capture a scalable native protocol and gRPC baseline

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Create a reproducible end-to-end benchmark harness for the current native protocol and gRPC implementations, then capture the authoritative pre-Session-Core baseline. The harness must exercise real server sessions with deterministic benchmark connectors and validate every response so throughput cannot be reported while work is dropped.

Preliminary Apple M2 / Go 1.26.1 observations, not yet the authoritative gate baseline:

- TCP produce, `1 B`: `240–249 ns/op`, about `5.3–6.3M` operations per one-second sample, `2 allocs/op`.
- QUIC produce, `1 B`: `416–444 ns/op`, about `2.7–3.1M` operations per sample, `2 allocs/op`.
- TCP produce, `32 KiB`: `36.9–37.6 µs/op`, `871–890 MB/s`, `3 allocs/op`.
- QUIC produce, `32 KiB`: `175–194 µs/op`, `169–187 MB/s`, about `307–309 allocs/op`.
- TCP produce, `1 MiB`: `1.10–1.34 ms/op`, `785–956 MB/s`, `7 allocs/op`.
- Unix produce, `1 MiB`: about `1.13 ms/op`, `927–929 MB/s`, `8 allocs/op`.
- QUIC produce, `1 MiB`: `3.58–3.70 ms/op`, `283–293 MB/s`, about `9.3K allocs/op`.

## User stories covered

- Preserve current throughput, tail latency, and allocation behavior.
- Detect regressions at multiple scales and payload sizes.
- Compare native protocol and gRPC against the same domain workload.

## Acceptance criteria

- [x] Stabilize benchmark setup and teardown: no broken pipes, remote-close errors, leaked listeners, fixed-port conflicts, or unvalidated responses.
- [x] Keep the regression test proving native protocol payloads of at least `1 MiB` do not panic.
- [x] Add gRPC benchmarks for BIND, PRODUCE/HPRODUCE, FETCH/HFETCH, SUBSCRIBE/HSUBSCRIBE, ACK/NACK, transactions, and cleanup.
- [x] Add deterministic native and gRPC fetch benchmarks that return payload batches instead of empty responses.
- [x] Cover payload, operation-count, batch-size, and concurrency dimensions approved in issue 001.
- [x] Run the agreed number of samples on an otherwise idle machine and retain raw benchmark output.
- [x] Record OS, architecture, CPU, Go version, `GOMAXPROCS`, build tags, and benchmark command lines.
- [x] Produce a machine-readable baseline and a human-readable summary suitable for later statistical comparison.

## Blocked by

- `issues/001-zero-regression-performance-contract.md`
