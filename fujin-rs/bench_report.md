# Fujin Rust Nop Connector Performance Report

**Generated:** 2026-08-22T22:27:09Z
**Source:** `db75ae3` (clean)
**Environment:** `rustc 1.97.1 (8bab26f4f 2026-07-14)` on `Darwin 24.6.0 arm64`

## Scope

The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Rust Fujin's Session Core and real localhost wire adapters using the built-in **`nop` connector**. The connector accepts every message locally and performs no broker I/O. Results therefore isolate protocol, Session Core, scheduling, encoding, callback, and transport overhead.

- **Measured transports:** native TCP and gRPC. The production runtime also supports QUIC, Unix sockets, and WebSocket; those adapters are outside this focused no-broker benchmark.
- **Synchronous matrix payloads:** 1B,128B,1MiB
- **Synchronous concurrent sessions:** 1,16,128
- **Synchronous batch:** 1 message per operation
- **Synchronous operations per cell:** 1000 for 1B/128B; 100 for 1MiB
- **Pipeline peak:** 1 B payload, one session, 10000 messages for native TCP and gRPC
- **Allocation metrics:** a separate `stats_alloc` instrumented process; latency and throughput come only from normal allocator runs.

> These are single-host no-broker snapshots. They do not characterize connector durability, broker acknowledgement latency, unmeasured transports, or cross-machine performance.

## Synchronous request/response results

| Transport | Payload | Concurrent sessions | Messages/s | Mmsg/s | Throughput | p99 operation latency | Allocations/op | Bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| native TCP | 1B | 1 | 26627 | 0.027 | 0.03 MB/s | 107.00 µs | 0.01 | 2 |
| native TCP | 1B | 16 | 65686 | 0.066 | 0.07 MB/s | 1438.88 µs | 0.08 | 21 |
| native TCP | 1B | 128 | 69623 | 0.070 | 0.07 MB/s | 2417.21 µs | 0.59 | 170 |
| native TCP | 128B | 1 | 27715 | 0.028 | 3.55 MB/s | 100.42 µs | 0.01 | 2 |
| native TCP | 128B | 16 | 62116 | 0.062 | 7.95 MB/s | 533.38 µs | 0.08 | 21 |
| native TCP | 128B | 128 | 56970 | 0.057 | 7.29 MB/s | 2330.62 µs | 0.55 | 169 |
| native TCP | 1MiB | 1 | 4601 | 0.005 | 4824.40 MB/s | 456.54 µs | 0.39 | 179812 |
| native TCP | 1MiB | 16 | 5377 | 0.005 | 5638.48 MB/s | 4040.50 µs | 1.83 | 740378 |
| native TCP | 1MiB | 128 | 5552 | 0.006 | 5822.17 MB/s | 14853.21 µs | 6.77 | 909166 |
| gRPC | 1B | 1 | 23946 | 0.024 | 0.02 MB/s | 98.17 µs | 5.01 | 53 |
| gRPC | 1B | 16 | 68549 | 0.069 | 0.07 MB/s | 357.67 µs | 5.04 | 184 |
| gRPC | 1B | 128 | 77580 | 0.078 | 0.08 MB/s | 2356.62 µs | 5.01 | 44 |
| gRPC | 128B | 1 | 24485 | 0.024 | 3.13 MB/s | 91.25 µs | 4.01 | 276 |
| gRPC | 128B | 16 | 77381 | 0.077 | 9.90 MB/s | 347.83 µs | 4.03 | 407 |
| gRPC | 128B | 128 | 68512 | 0.069 | 8.77 MB/s | 2578.25 µs | 4.01 | 267 |
| gRPC | 1MiB | 1 | 1959 | 0.002 | 2054.18 MB/s | 1099.46 µs | 193.53 | 3148931 |
| gRPC | 1MiB | 16 | 2996 | 0.003 | 3141.44 MB/s | 9168.08 µs | 193.34 | 3148023 |
| gRPC | 1MiB | 128 | 3696 | 0.004 | 3875.70 MB/s | 26326.17 µs | 194.68 | 3159534 |

### Reading the two result modes

The synchronous matrix reports request/response behavior at the stated concurrent-session count. It is the p99 and capacity view. The pipeline table uses one 1 B session with concurrent response draining and is the sustainable throughput view, **not a latency comparison**.

## 1 B pipelined throughput

Both rows use one client session, exactly 10000 PRODUCE messages, concurrent response draining, and the nop connector. Native TCP uses 512 KiB buffered writes and reads while validating every pre-encoded request and six-byte response. gRPC keeps at most 4096 operations in flight, matching the server response relay capacity so Tonic can coalesce ready messages up to its 32 KiB encoder yield threshold.

| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| native TCP | 1 B | One full-duplex pipelined session | 10000 | 8547009 | 8.547 | 8.54 MB/s | 0.00 | 190 |
| gRPC | 1 B | One bounded full-duplex session | 10000 | 1893939 | 1.894 | 1.89 MB/s | 5.02 | 125 |

## Reproduce

```bash
./fujin-rs/scripts/generate_bench_report.sh
```

Run a smaller local validation report:

```bash
FUJIN_BENCH_SMALL_OPERATIONS=1000 FUJIN_BENCH_LARGE_OPERATIONS=100 FUJIN_BENCH_PEAK_ITERATIONS=10000 ./fujin-rs/scripts/generate_bench_report.sh
```

The generator performs normal-allocation timing runs and isolated allocation runs, validates every required result, and atomically replaces the report only after the complete matrix succeeds.
