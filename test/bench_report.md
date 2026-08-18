# Fujin Nop Connector Performance Report

**Generated:** 2026-08-18T22:52:51Z
**Source:** `e9e768c-dirty` (dirty)
**Environment:** `go version go1.26.1 darwin/arm64` on `Darwin 24.6.0 arm64`

## Scope

The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Fujin's Session Core and wire adapters using the built-in **`nop` connector**. The connector accepts every message immediately and performs no broker I/O; these figures isolate Fujin’s protocol, session, scheduling, and callback overhead on localhost. The pipeline table measures TCP and gRPC under identical 1 B, one-session, fixed-message-count, full-duplex conditions.

- **Transports:** native TCP, QUIC, Unix socket, and gRPC
- **Synchronous matrix payloads:** 1B,128B,1MiB
- **Synchronous concurrent sessions:** 1,16,128
- **Synchronous batch:** 1 message per operation
- **Synchronous sample duration:** 3s per subtest
- **Pipeline peak:** 1 B payload, one session, 1000000 messages for TCP and gRPC

> These are single-host performance snapshots, not a cross-machine comparison or a broker durability benchmark. Run broker-backed tests separately when evaluating connector throughput and acknowledgement latency.

## Synchronous request/response results

| Transport | Payload | Concurrent sessions | Messages/s | Mmsg/s | Throughput | p99 operation latency | Allocations/op | Bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tcp | 1B | 1 | 45438 | 0.045 | 0.05 MB/s | 34.38 µs | 3 | 32 |
| tcp | 1B | 16 | 107770 | 0.108 | 0.11 MB/s | 302.46 µs | 3 | 31 |
| tcp | 1B | 128 | 105865 | 0.106 | 0.11 MB/s | 1877.42 µs | 3 | 31 |
| tcp | 128B | 1 | 44926 | 0.045 | 5.75 MB/s | 35.54 µs | 3 | 32 |
| tcp | 128B | 16 | 107793 | 0.108 | 13.80 MB/s | 302.33 µs | 3 | 31 |
| tcp | 128B | 128 | 105585 | 0.106 | 13.51 MB/s | 1800.96 µs | 3 | 31 |
| tcp | 1MiB | 1 | 906 | 0.001 | 950.21 MB/s | 1285.75 µs | 7 | 1049479 |
| tcp | 1MiB | 16 | 810 | 0.001 | 848.98 MB/s | 49608.50 µs | 5 | 1048814 |
| tcp | 1MiB | 128 | 812 | 0.001 | 851.25 MB/s | 487797.38 µs | 4 | 1048658 |
| quic | 1B | 1 | 20609 | 0.021 | 0.02 MB/s | 64.46 µs | 33 | 1176 |
| quic | 1B | 16 | 88944 | 0.089 | 0.09 MB/s | 323.00 µs | 29 | 1092 |
| quic | 1B | 128 | 348797 | 0.349 | 0.35 MB/s | 647.54 µs | 16 | 689 |
| quic | 128B | 1 | 20950 | 0.021 | 2.68 MB/s | 63.92 µs | 31 | 1102 |
| quic | 128B | 16 | 89694 | 0.090 | 11.48 MB/s | 321.75 µs | 27 | 1016 |
| quic | 128B | 128 | 332005 | 0.332 | 42.50 MB/s | 672.04 µs | 15 | 641 |
| quic | 1MiB | 1 | 271 | 0.000 | 284.35 MB/s | 5035.71 µs | 9290 | 1408017 |
| quic | 1MiB | 16 | 277 | 0.000 | 290.12 MB/s | 60090.50 µs | 9198 | 1376528 |
| quic | 1MiB | 128 | 273 | 0.000 | 285.98 MB/s | 512758.50 µs | 9216 | 1376586 |
| unix | 1B | 1 | 116077 | 0.116 | 0.12 MB/s | 16.04 µs | 3 | 32 |
| unix | 1B | 16 | 232396 | 0.232 | 0.23 MB/s | 138.33 µs | 3 | 31 |
| unix | 1B | 128 | 281611 | 0.282 | 0.28 MB/s | 876.00 µs | 3 | 31 |
| unix | 128B | 1 | 116577 | 0.117 | 14.92 MB/s | 16.21 µs | 3 | 32 |
| unix | 128B | 16 | 232288 | 0.232 | 29.73 MB/s | 138.88 µs | 3 | 31 |
| unix | 128B | 128 | 279018 | 0.279 | 35.72 MB/s | 875.62 µs | 3 | 31 |
| unix | 1MiB | 1 | 922 | 0.001 | 966.41 MB/s | 1377.67 µs | 7 | 1049489 |
| unix | 1MiB | 16 | 637 | 0.001 | 668.37 MB/s | 29348.54 µs | 5 | 1048768 |
| unix | 1MiB | 128 | 664 | 0.001 | 696.40 MB/s | 218111.42 µs | 4 | 1048629 |
| gRPC | 1B | 1 | 27146 | 0.027 | 0.03 MB/s | 55.46 µs | 44 | 1217 |
| gRPC | 1B | 16 | 238663 | 0.239 | 0.24 MB/s | 132.67 µs | 32 | 1034 |
| gRPC | 1B | 128 | 519211 | 0.519 | 0.52 MB/s | 513.00 µs | 32 | 1026 |
| gRPC | 128B | 1 | 26702 | 0.027 | 3.42 MB/s | 56.71 µs | 44 | 1594 |
| gRPC | 128B | 16 | 238039 | 0.238 | 30.47 MB/s | 135.67 µs | 32 | 1412 |
| gRPC | 128B | 128 | 502260 | 0.502 | 64.29 MB/s | 689.42 µs | 32 | 1404 |
| gRPC | 1MiB | 1 | 2648 | 0.003 | 2776.52 MB/s | 642.12 µs | 97 | 1566934 |
| gRPC | 1MiB | 16 | 3596 | 0.004 | 3770.58 MB/s | 5739.38 µs | 59 | 1094979 |
| gRPC | 1MiB | 128 | 3289 | 0.003 | 3448.52 MB/s | 52740.29 µs | 54 | 1057539 |

### Reading the two result modes

The synchronous matrix reports each adapter’s request/response behavior at the stated concurrency. It is useful for p99 and concurrent-session capacity, but it does not establish a universal protocol ranking. The 1 B pipeline table below is the direct native-TCP versus gRPC throughput comparison: same payload, one client session, a fixed number of messages, `nop`, and concurrent response draining.

## 1 B pipelined throughput

Both rows use one client session, exactly 1000000 PRODUCE messages, concurrent response draining, and the nop connector. **This is not a latency comparison.** TCP writes pre-encoded native frames to a buffered stream and relies on socket backpressure; gRPC keeps at most 1,024 operations in flight to respect HTTP/2 flow control. The table therefore shows each adapter’s sustainable pipeline behavior, while the synchronous matrix above is the transport-neutral request/response comparison.

| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP | 1 B | One pipelined session | 1000000 | 5279831 | 5.280 | 89.77 MB/s | 1 | 6 |
| gRPC | 1 B | One bounded full-duplex session | 1000000 | 517866 | 0.518 | 0.52 MB/s | 32 | 1188 |

## Reproduce

```bash
make bench-report
```

Run a longer, focused sample:

```bash
BENCHTIME=10s FUJIN_BENCH_PAYLOAD=1MiB FUJIN_BENCH_CONCURRENCY=128 make bench-report
```

The generator is [`test/generate_bench_report.sh`](generate_bench_report.sh). It fails without a result for each native transport and gRPC, and writes the report atomically only after all benchmark subtests succeed.
