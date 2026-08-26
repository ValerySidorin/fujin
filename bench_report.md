# Fujin Rust Nop Connector Performance Report

**Generated:** 2026-08-23T09:48:04Z
**Source:** `ddf935c-dirty` (dirty)
**Environment:** `rustc 1.97.1 (8bab26f4f 2026-07-14)` on `Darwin 24.6.0 arm64`

## Scope

The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Rust Fujin's Session Core and real localhost wire adapters using the registered **`nop` connector plugin**. The connector accepts every message locally and performs no broker I/O. Results therefore isolate protocol, Session Core, scheduling, encoding, callback, and transport overhead.

- **Measured transports:** native TCP and gRPC. The production runtime also supports QUIC, Unix sockets, and WebSocket; those adapters are outside this focused no-broker benchmark.
- **Synchronous matrix payloads:** 1B,128B,1MiB
- **Synchronous concurrent sessions:** 1,16,128
- **Synchronous batch:** 1 message per operation
- **Synchronous operations per cell:** 10000 for 1B/128B; 1000 for 1MiB
- **Pipeline peak:** 1 B payload, one session, 1000000 messages for native TCP and gRPC
- **Allocation metrics:** a separate `stats_alloc` instrumented process; latency and throughput come only from normal allocator runs.

> These are single-host no-broker snapshots. They do not characterize connector durability, broker acknowledgement latency, unmeasured transports, or cross-machine performance.

## Synchronous request/response results

| Transport | Payload | Concurrent sessions | Messages/s | Mmsg/s | Throughput | p99 operation latency | Allocations/op | Bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| native TCP | 1B | 1 | 51114 | 0.051 | 0.05 MB/s | 60.33 µs | 0.00 | 0 |
| native TCP | 1B | 16 | 151653 | 0.152 | 0.15 MB/s | 292.33 µs | 0.00 | 2 |
| native TCP | 1B | 128 | 166973 | 0.167 | 0.17 MB/s | 2387.50 µs | 0.05 | 16 |
| native TCP | 128B | 1 | 50000 | 0.050 | 6.40 MB/s | 63.17 µs | 0.00 | 0 |
| native TCP | 128B | 16 | 155231 | 0.155 | 19.87 MB/s | 409.58 µs | 0.00 | 2 |
| native TCP | 128B | 128 | 162338 | 0.162 | 20.78 MB/s | 2307.38 µs | 0.05 | 16 |
| native TCP | 1MiB | 1 | 7827 | 0.008 | 8207.01 MB/s | 343.38 µs | 0.06 | 36247 |
| native TCP | 1MiB | 16 | 11911 | 0.012 | 12489.81 MB/s | 3214.21 µs | 0.26 | 127576 |
| native TCP | 1MiB | 128 | 7273 | 0.007 | 7626.10 MB/s | 21851.75 µs | 1.58 | 714789 |
| gRPC | 1B | 1 | 34344 | 0.034 | 0.03 MB/s | 69.58 µs | 5.00 | 44 |
| gRPC | 1B | 16 | 140115 | 0.140 | 0.14 MB/s | 237.38 µs | 5.00 | 58 |
| gRPC | 1B | 128 | 171174 | 0.171 | 0.17 MB/s | 2316.62 µs | 5.02 | 156 |
| gRPC | 128B | 1 | 34803 | 0.035 | 4.45 MB/s | 76.25 µs | 4.00 | 267 |
| gRPC | 128B | 16 | 136091 | 0.136 | 17.42 MB/s | 266.54 µs | 4.00 | 281 |
| gRPC | 128B | 128 | 164609 | 0.165 | 21.07 MB/s | 2317.08 µs | 4.02 | 379 |
| gRPC | 1MiB | 1 | 2320 | 0.002 | 2432.20 MB/s | 969.62 µs | 193.12 | 3148973 |
| gRPC | 1MiB | 16 | 4319 | 0.004 | 4528.63 MB/s | 8448.25 µs | 193.57 | 3149072 |
| gRPC | 1MiB | 128 | 4112 | 0.004 | 4311.47 MB/s | 46064.04 µs | 193.29 | 3150417 |

### Reading the two result modes

The synchronous matrix reports request/response behavior at the stated concurrent-session count. It is the p99 and capacity view. The pipeline table uses one 1 B session with concurrent response draining and is the sustainable throughput view, **not a latency comparison**.

## 1 B pipelined throughput

Both rows use one client session, exactly 1000000 PRODUCE messages, concurrent response draining, and the nop connector. Native TCP uses 512 KiB buffered writes and reads while validating every pre-encoded request and six-byte response. gRPC keeps at most 4096 operations in flight, matching the server response relay capacity so Tonic can coalesce ready messages up to its 32 KiB encoder yield threshold.

| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| native TCP | 1 B | One full-duplex pipelined session | 1000000 | 14492754 | 14.493 | 14.35 MB/s | 0.00 | 16 |
| gRPC | 1 B | One bounded full-duplex session | 1000000 | 2403846 | 2.404 | 2.40 MB/s | 5.01 | 109 |

## Reproduce

```bash
./scripts/generate_bench_report.sh
```

Run a smaller local validation report:

```bash
FUJIN_BENCH_SMALL_OPERATIONS=1000 FUJIN_BENCH_LARGE_OPERATIONS=100 FUJIN_BENCH_PEAK_ITERATIONS=10000 ./scripts/generate_bench_report.sh
```

The generator performs normal-allocation timing runs and isolated allocation runs, validates every required result, and atomically replaces the report only after the complete matrix succeeds.
