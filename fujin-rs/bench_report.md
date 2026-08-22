# Fujin Rust Nop Connector Performance Report

**Generated:** 2026-08-22T18:37:12Z
**Source:** `d5302a7-dirty` (dirty)
**Environment:** `rustc 1.97.1 (8bab26f4f 2026-07-14)` on `Darwin 24.6.0 arm64`

## Scope

The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Rust Fujin's Session Core and real localhost wire adapters using the built-in **`nop` connector**. The connector accepts every message locally and performs no broker I/O. Results therefore isolate protocol, Session Core, scheduling, encoding, callback, and transport overhead.

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
| native TCP | 1B | 1 | 51201 | 0.051 | 0.05 MB/s | 46.17 µs | 0.00 | 0 |
| native TCP | 1B | 16 | 145307 | 0.145 | 0.15 MB/s | 338.54 µs | 0.00 | 2 |
| native TCP | 1B | 128 | 155788 | 0.156 | 0.16 MB/s | 2844.58 µs | 0.05 | 16 |
| native TCP | 128B | 1 | 50320 | 0.050 | 6.44 MB/s | 53.00 µs | 0.00 | 0 |
| native TCP | 128B | 16 | 137627 | 0.138 | 17.61 MB/s | 389.58 µs | 0.00 | 2 |
| native TCP | 128B | 128 | 157431 | 0.157 | 20.15 MB/s | 2579.00 µs | 0.05 | 16 |
| native TCP | 1MiB | 1 | 8108 | 0.008 | 8501.55 MB/s | 316.54 µs | 0.09 | 62718 |
| native TCP | 1MiB | 16 | 11107 | 0.011 | 11646.69 MB/s | 3704.29 µs | 0.22 | 99673 |
| native TCP | 1MiB | 128 | 7113 | 0.007 | 7458.87 MB/s | 27038.67 µs | 1.23 | 466484 |
| gRPC | 1B | 1 | 35045 | 0.035 | 0.04 MB/s | 72.79 µs | 5.00 | 44 |
| gRPC | 1B | 16 | 134048 | 0.134 | 0.13 MB/s | 244.71 µs | 5.00 | 58 |
| gRPC | 1B | 128 | 168919 | 0.169 | 0.17 MB/s | 2338.04 µs | 5.02 | 156 |
| gRPC | 128B | 1 | 35144 | 0.035 | 4.50 MB/s | 67.00 µs | 4.00 | 267 |
| gRPC | 128B | 16 | 138543 | 0.139 | 17.73 MB/s | 254.96 µs | 4.00 | 281 |
| gRPC | 128B | 128 | 163425 | 0.163 | 20.92 MB/s | 2330.75 µs | 4.02 | 379 |
| gRPC | 1MiB | 1 | 2432 | 0.002 | 2550.41 MB/s | 821.92 µs | 193.42 | 3148722 |
| gRPC | 1MiB | 16 | 4206 | 0.004 | 4410.21 MB/s | 8397.58 µs | 193.74 | 3148946 |
| gRPC | 1MiB | 128 | 3983 | 0.004 | 4176.33 MB/s | 48682.50 µs | 193.29 | 3150262 |

### Reading the two result modes

The synchronous matrix reports request/response behavior at the stated concurrent-session count. It is the p99 and capacity view. The pipeline table uses one 1 B session with concurrent response draining and is the sustainable throughput view, **not a latency comparison**.

## 1 B pipelined throughput

Both rows use one client session, exactly 1000000 PRODUCE messages, concurrent response draining, and the nop connector. Native TCP uses 512 KiB buffered writes and reads while validating every pre-encoded request and six-byte response. gRPC keeps at most 4096 operations in flight, matching the server response relay capacity so Tonic can coalesce ready messages up to its 32 KiB encoder yield threshold.

| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| native TCP | 1 B | One full-duplex pipelined session | 1000000 | 14925373 | 14.925 | 14.80 MB/s | 0.00 | 14 |
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
