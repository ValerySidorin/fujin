# Fujin Nop Connector Performance Report

**Generated:** 2026-08-19T07:01:53Z
**Source:** `71054db-dirty` (dirty)
**Environment:** `go version go1.26.1 darwin/arm64` on `Darwin 24.6.0 arm64`

## Scope

The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Fujin's Session Core and wire adapters using the built-in **`nop` connector**. The connector accepts every message immediately and performs no broker I/O; these figures isolate Fujin’s protocol, session, scheduling, and callback overhead on localhost. The pipeline table measures TCP and gRPC under identical 1 B, one-session, fixed-message-count, full-duplex conditions.

- **Transports:** native TCP, QUIC, Unix socket, WebSocket, and gRPC
- **Synchronous matrix payloads:** 1B,128B,1MiB
- **Synchronous concurrent sessions:** 1,16,128
- **Synchronous batch:** 1 message per operation
- **Synchronous sample duration:** 3s per subtest
- **Pipeline peak:** 1 B payload, one session, 1000000 messages for TCP and gRPC

> These are single-host performance snapshots, not a cross-machine comparison or a broker durability benchmark. Run broker-backed tests separately when evaluating connector throughput and acknowledgement latency.

## Synchronous request/response results

| Transport | Payload | Concurrent sessions | Messages/s | Mmsg/s | Throughput | p99 operation latency | Allocations/op | Bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tcp | 1B | 1 | 45356 | 0.045 | 0.05 MB/s | 34.96 µs | 3 | 32 |
| tcp | 1B | 16 | 107875 | 0.108 | 0.11 MB/s | 302.42 µs | 3 | 31 |
| tcp | 1B | 128 | 104888 | 0.105 | 0.10 MB/s | 1875.12 µs | 3 | 31 |
| tcp | 128B | 1 | 45073 | 0.045 | 5.77 MB/s | 34.79 µs | 3 | 32 |
| tcp | 128B | 16 | 107689 | 0.108 | 13.78 MB/s | 306.00 µs | 3 | 31 |
| tcp | 128B | 128 | 104417 | 0.104 | 13.37 MB/s | 1834.92 µs | 3 | 31 |
| tcp | 1MiB | 1 | 6545 | 0.007 | 6862.43 MB/s | 183.83 µs | 3 | 108 |
| tcp | 1MiB | 16 | 10199 | 0.010 | 10694.65 MB/s | 3931.04 µs | 3 | 67 |
| tcp | 1MiB | 128 | 5718 | 0.006 | 5996.17 MB/s | 26813.46 µs | 3 | 39 |
| quic | 1B | 1 | 21460 | 0.021 | 0.02 MB/s | 63.42 µs | 33 | 1176 |
| quic | 1B | 16 | 96006 | 0.096 | 0.10 MB/s | 308.79 µs | 29 | 1078 |
| quic | 1B | 128 | 362582 | 0.363 | 0.36 MB/s | 626.08 µs | 16 | 689 |
| quic | 128B | 1 | 21531 | 0.022 | 2.76 MB/s | 62.75 µs | 31 | 1103 |
| quic | 128B | 16 | 96805 | 0.097 | 12.39 MB/s | 303.08 µs | 27 | 1006 |
| quic | 128B | 128 | 346260 | 0.346 | 44.32 MB/s | 636.42 µs | 14 | 638 |
| quic | 1MiB | 1 | 272 | 0.000 | 285.09 MB/s | 4532.71 µs | 9373 | 579518 |
| quic | 1MiB | 16 | 273 | 0.000 | 286.47 MB/s | 93950.17 µs | 9271 | 356141 |
| quic | 1MiB | 128 | 273 | 0.000 | 286.04 MB/s | 473582.21 µs | 9267 | 332374 |
| unix | 1B | 1 | 115260 | 0.115 | 0.12 MB/s | 16.12 µs | 3 | 32 |
| unix | 1B | 16 | 233754 | 0.234 | 0.23 MB/s | 138.25 µs | 3 | 31 |
| unix | 1B | 128 | 279096 | 0.279 | 0.28 MB/s | 889.08 µs | 3 | 31 |
| unix | 128B | 1 | 115088 | 0.115 | 14.73 MB/s | 16.42 µs | 3 | 32 |
| unix | 128B | 16 | 233318 | 0.233 | 29.86 MB/s | 137.17 µs | 3 | 31 |
| unix | 128B | 128 | 276625 | 0.277 | 35.41 MB/s | 895.79 µs | 3 | 31 |
| unix | 1MiB | 1 | 2731 | 0.003 | 2863.35 MB/s | 421.54 µs | 3 | 140 |
| unix | 1MiB | 16 | 2381 | 0.002 | 2496.15 MB/s | 7741.71 µs | 3 | 942 |
| unix | 1MiB | 128 | 3014 | 0.003 | 3160.93 MB/s | 48523.00 µs | 3 | 802 |
| websocket | 1B | 1 | 45029 | 0.045 | 0.05 MB/s | 35.46 µs | 7 | 144 |
| websocket | 1B | 16 | 169952 | 0.170 | 0.17 MB/s | 224.46 µs | 7 | 143 |
| websocket | 1B | 128 | 208333 | 0.208 | 0.21 MB/s | 1745.88 µs | 7 | 143 |
| websocket | 128B | 1 | 44899 | 0.045 | 5.75 MB/s | 35.92 µs | 7 | 144 |
| websocket | 128B | 16 | 169463 | 0.169 | 21.69 MB/s | 229.71 µs | 7 | 143 |
| websocket | 128B | 128 | 207641 | 0.208 | 26.58 MB/s | 1787.88 µs | 7 | 143 |
| websocket | 1MiB | 1 | 1175 | 0.001 | 1231.65 MB/s | 990.08 µs | 7 | 372 |
| websocket | 1MiB | 16 | 2554 | 0.003 | 2678.31 MB/s | 15088.83 µs | 7 | 483 |
| websocket | 1MiB | 128 | 2408 | 0.002 | 2524.61 MB/s | 121532.67 µs | 7 | 144 |
| gRPC | 1B | 1 | 27189 | 0.027 | 0.03 MB/s | 55.75 µs | 44 | 1217 |
| gRPC | 1B | 16 | 241196 | 0.241 | 0.24 MB/s | 131.46 µs | 32 | 1034 |
| gRPC | 1B | 128 | 520833 | 0.521 | 0.52 MB/s | 499.42 µs | 32 | 1025 |
| gRPC | 128B | 1 | 26921 | 0.027 | 3.45 MB/s | 55.67 µs | 44 | 1594 |
| gRPC | 128B | 16 | 236742 | 0.237 | 30.30 MB/s | 137.38 µs | 32 | 1413 |
| gRPC | 128B | 128 | 501756 | 0.502 | 64.24 MB/s | 681.92 µs | 32 | 1404 |
| gRPC | 1MiB | 1 | 2643 | 0.003 | 2771.13 MB/s | 650.54 µs | 98 | 1547301 |
| gRPC | 1MiB | 16 | 3530 | 0.004 | 3701.18 MB/s | 5865.25 µs | 60 | 1094707 |
| gRPC | 1MiB | 128 | 3323 | 0.003 | 3484.83 MB/s | 42891.38 µs | 52 | 1056878 |

### Reading the two result modes

The synchronous matrix reports each adapter’s request/response behavior at the stated concurrency. It is useful for p99 and concurrent-session capacity, but it does not establish a universal protocol ranking. The 1 B pipeline table below is the direct native-TCP versus gRPC throughput comparison: same payload, one client session, a fixed number of messages, `nop`, and concurrent response draining.

## 1 B pipelined throughput

Both rows use one client session, exactly 1000000 PRODUCE messages, concurrent response draining, and the nop connector. **This is not a latency comparison.** TCP writes pre-encoded native frames to a buffered stream and relies on socket backpressure; gRPC keeps at most 1,024 operations in flight to respect HTTP/2 flow control. The table therefore shows each adapter’s sustainable pipeline behavior, while the synchronous matrix above is the transport-neutral request/response comparison.

| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP | 1 B | One pipelined session | 1000000 | 8354219 | 8.354 | 142.03 MB/s | 1 | 5 |
| gRPC | 1 B | One bounded full-duplex session | 1000000 | 563380 | 0.563 | 0.56 MB/s | 32 | 1187 |

## Reproduce

```bash
make bench-report
```

Run a longer, focused sample:

```bash
BENCHTIME=10s FUJIN_BENCH_PAYLOAD=1MiB FUJIN_BENCH_CONCURRENCY=128 make bench-report
```

The generator is [`test/generate_bench_report.sh`](generate_bench_report.sh). It fails without a result for each native transport and gRPC, and writes the report atomically only after all benchmark subtests succeed.
