# Fujin Nop Connector Performance Report

**Generated:** 2026-08-18T22:20:48Z  
**Source:** `6b67336-dirty` (dirty)  
**Environment:** `go version go1.26.1 darwin/arm64` on `Darwin 24.6.0 arm64`

## Scope

The synchronous matrix measures end-to-end **PRODUCE** request/response operations through Fujin's Session Core and wire adapters using the built-in **`nop` connector**. The connector accepts every message immediately and performs no broker I/O; these figures isolate Fujin’s protocol, session, scheduling, and callback overhead on localhost.

- **Transports:** native TCP, QUIC, Unix socket, and gRPC
- **Synchronous matrix payloads:** 1B,128B,1MiB
- **Synchronous concurrent sessions:** 1,16,128
- **Synchronous batch:** 1 message per operation
- **Synchronous sample duration:** 3s per subtest
- **TCP pipeline peak:** 1 B payload, one session, 1000000 messages

> These are single-host performance snapshots, not a cross-machine comparison or a broker durability benchmark. Run broker-backed tests separately when evaluating connector throughput and acknowledgement latency.

## Synchronous request/response results

| Transport | Payload | Concurrent sessions | Messages/s | Mmsg/s | Throughput | p99 operation latency | Allocations/op | Bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| tcp | 1B | 1 | 45282 | 0.045 | 0.05 MB/s | 35.38 µs | 3 | 32 |
| tcp | 1B | 16 | 108225 | 0.108 | 0.11 MB/s | 296.62 µs | 3 | 31 |
| tcp | 1B | 128 | 104976 | 0.105 | 0.10 MB/s | 1791.08 µs | 3 | 31 |
| tcp | 128B | 1 | 45202 | 0.045 | 5.79 MB/s | 35.21 µs | 3 | 32 |
| tcp | 128B | 16 | 108284 | 0.108 | 13.86 MB/s | 300.79 µs | 3 | 31 |
| tcp | 128B | 128 | 105843 | 0.106 | 13.55 MB/s | 1825.42 µs | 3 | 31 |
| tcp | 1MiB | 1 | 894 | 0.001 | 937.85 MB/s | 1294.54 µs | 7 | 1049446 |
| tcp | 1MiB | 16 | 800 | 0.001 | 838.81 MB/s | 52019.79 µs | 5 | 1048819 |
| tcp | 1MiB | 128 | 803 | 0.001 | 841.72 MB/s | 325267.21 µs | 4 | 1048669 |
| quic | 1B | 1 | 20021 | 0.020 | 0.02 MB/s | 67.29 µs | 33 | 1175 |
| quic | 1B | 16 | 87982 | 0.088 | 0.09 MB/s | 344.25 µs | 29 | 1091 |
| quic | 1B | 128 | 346620 | 0.347 | 0.35 MB/s | 637.54 µs | 16 | 690 |
| quic | 128B | 1 | 20060 | 0.020 | 2.57 MB/s | 65.08 µs | 31 | 1102 |
| quic | 128B | 16 | 88960 | 0.089 | 11.39 MB/s | 326.58 µs | 27 | 1016 |
| quic | 128B | 128 | 337268 | 0.337 | 43.17 MB/s | 654.33 µs | 15 | 640 |
| quic | 1MiB | 1 | 265 | 0.000 | 277.88 MB/s | 4670.38 µs | 9372 | 1403427 |
| quic | 1MiB | 16 | 276 | 0.000 | 289.15 MB/s | 60101.38 µs | 9212 | 1376730 |
| quic | 1MiB | 128 | 219 | 0.000 | 229.41 MB/s | 1012617.92 µs | 9206 | 1374349 |
| unix | 1B | 1 | 116023 | 0.116 | 0.12 MB/s | 16.25 µs | 3 | 32 |
| unix | 1B | 16 | 232775 | 0.233 | 0.23 MB/s | 138.25 µs | 3 | 31 |
| unix | 1B | 128 | 274348 | 0.274 | 0.27 MB/s | 897.83 µs | 3 | 31 |
| unix | 128B | 1 | 115154 | 0.115 | 14.74 MB/s | 16.42 µs | 3 | 32 |
| unix | 128B | 16 | 233100 | 0.233 | 29.84 MB/s | 139.33 µs | 3 | 31 |
| unix | 128B | 128 | 277393 | 0.277 | 35.51 MB/s | 894.62 µs | 3 | 31 |
| unix | 1MiB | 1 | 862 | 0.001 | 903.86 MB/s | 1445.50 µs | 7 | 1049430 |
| unix | 1MiB | 16 | 629 | 0.001 | 659.96 MB/s | 29356.12 µs | 5 | 1048765 |
| unix | 1MiB | 128 | 656 | 0.001 | 688.03 MB/s | 221464.29 µs | 4 | 1048629 |
| gRPC | 1B | 1 | 27179 | 0.027 | 0.03 MB/s | 56.17 µs | 44 | 1217 |
| gRPC | 1B | 16 | 240154 | 0.240 | 0.24 MB/s | 130.29 µs | 32 | 1034 |
| gRPC | 1B | 128 | 518135 | 0.518 | 0.52 MB/s | 511.38 µs | 32 | 1025 |
| gRPC | 128B | 1 | 26961 | 0.027 | 3.45 MB/s | 54.79 µs | 44 | 1594 |
| gRPC | 128B | 16 | 236911 | 0.237 | 30.32 MB/s | 137.88 µs | 32 | 1413 |
| gRPC | 128B | 128 | 498753 | 0.499 | 63.85 MB/s | 696.92 µs | 32 | 1404 |
| gRPC | 1MiB | 1 | 2629 | 0.003 | 2756.76 MB/s | 653.29 µs | 101 | 1569940 |
| gRPC | 1MiB | 16 | 3514 | 0.004 | 3685.04 MB/s | 6124.12 µs | 63 | 1100226 |
| gRPC | 1MiB | 128 | 3280 | 0.003 | 3439.10 MB/s | 45356.42 µs | 56 | 1058239 |

## TCP pipelined peak throughput

| Transport | Payload | Session mode | Messages | Messages/s | Mmsg/s | Wire throughput | Allocations/op | Bytes/op |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP | 1 B | One pipelined session | 1000000 | 5271481 | 5.271 | 89.60 MB/s | 1 | 6 |

## Reproduce

```bash
make bench-report
```

Run a longer, focused sample:

```bash
BENCHTIME=10s FUJIN_BENCH_PAYLOAD=1MiB FUJIN_BENCH_CONCURRENCY=128 make bench-report
```

The generator is [`test/generate_bench_report.sh`](generate_bench_report.sh). It fails without a result for each native transport and gRPC, and writes the report atomically only after all benchmark subtests succeed.
