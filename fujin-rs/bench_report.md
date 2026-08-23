# Fujin Rust Nop Connector Performance Report

**Generated:** 2026-08-23T09:17:54Z
**Source:** `bb509be`
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
| native TCP | 1B | 1 | 50439 | 0.050 | 0.05 MB/s | 75.62 µs | 0.00 | 0 |
| native TCP | 1B | 16 | 147885 | 0.148 | 0.15 MB/s | 442.33 µs | 0.00 | 2 |
| native TCP | 1B | 128 | 154416 | 0.154 | 0.15 MB/s | 2381.33 µs | 0.05 | 16 |
| native TCP | 128B | 1 | 41048 | 0.041 | 5.25 MB/s | 73.79 µs | 0.00 | 0 |
| native TCP | 128B | 16 | 153210 | 0.153 | 19.61 MB/s | 480.12 µs | 0.00 | 2 |
| native TCP | 128B | 128 | 153092 | 0.153 | 19.59 MB/s | 2756.12 µs | 0.05 | 16 |
| native TCP | 1MiB | 1 | 8231 | 0.008 | 8630.63 MB/s | 248.17 µs | 0.06 | 36716 |
| native TCP | 1MiB | 16 | 11928 | 0.012 | 12507.82 MB/s | 3236.62 µs | 0.21 | 94515 |
| native TCP | 1MiB | 128 | 8946 | 0.009 | 9380.93 MB/s | 23463.46 µs | 1.42 | 604108 |
| gRPC | 1B | 1 | 35402 | 0.035 | 0.04 MB/s | 57.62 µs | 5.00 | 44 |
| gRPC | 1B | 16 | 141663 | 0.142 | 0.14 MB/s | 241.96 µs | 5.00 | 58 |
| gRPC | 1B | 128 | 170184 | 0.170 | 0.17 MB/s | 2263.75 µs | 5.02 | 156 |
| gRPC | 128B | 1 | 34878 | 0.035 | 4.46 MB/s | 78.04 µs | 4.00 | 267 |
| gRPC | 128B | 16 | 136258 | 0.136 | 17.44 MB/s | 250.08 µs | 4.00 | 281 |
| gRPC | 128B | 128 | 166472 | 0.166 | 21.31 MB/s | 2346.08 µs | 4.02 | 379 |
| gRPC | 1MiB | 1 | 2546 | 0.003 | 2669.19 MB/s | 794.00 µs | 193.58 | 3148632 |
| gRPC | 1MiB | 16 | 4258 | 0.004 | 4464.38 MB/s | 8143.38 µs | 193.62 | 3148982 |
| gRPC | 1MiB | 128 | 3819 | 0.004 | 4004.28 MB/s | 51832.38 µs | 193.34 | 3148666 |

### Reading the two result modes

The synchronous matrix reports request/response behavior at the stated concurrent-session count. It is the p99 and capacity view. The pipeline table uses one 1 B session with concurrent response draining and is the sustainable throughput view, **not a latency comparison**.

## 1 B pipelined throughput

Both rows use one client session, exactly 1000000 PRODUCE messages, concurrent response draining, and the nop connector. Native TCP uses 512 KiB buffered writes and reads while validating every pre-encoded request and six-byte response. gRPC keeps at most 4096 operations in flight, matching the server response relay capacity so Tonic can coalesce ready messages up to its 32 KiB encoder yield threshold.

| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| native TCP | 1 B | One full-duplex pipelined session | 1000000 | 13888889 | 13.889 | 13.78 MB/s | 0.00 | 14 |
| gRPC | 1 B | One bounded full-duplex session | 1000000 | 2415459 | 2.415 | 2.41 MB/s | 5.01 | 109 |

## Reproduce

```bash
./fujin-rs/scripts/generate_bench_report.sh
```

Run a smaller local validation report:

```bash
FUJIN_BENCH_SMALL_OPERATIONS=1000 FUJIN_BENCH_LARGE_OPERATIONS=100 FUJIN_BENCH_PEAK_ITERATIONS=10000 ./fujin-rs/scripts/generate_bench_report.sh
```

The generator performs normal-allocation timing runs and isolated allocation runs, validates every required result, and atomically replaces the report only after the complete matrix succeeds.
