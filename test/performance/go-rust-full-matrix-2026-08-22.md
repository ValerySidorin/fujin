# Fujin Go/Rust Full Session Matrix Comparison

**Generated:** 2026-08-23T02:26:49.440940+00:00
**Source:** `ac6189b`
**Cells:** 1095

## Aggregate ratios

Rust/Go below `1.0x` is better for latency, p99, allocated bytes, and allocation count.

| Scope | Cells | ns/op | p99 | B/op | allocs/op | Rust faster | >10% slower | Significant regressions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| overall | 1095 | 0.641x | 0.493x | 2.520x | 0.268x | 981 | 76 | 0 |
| operation/produce | 75 | 0.685x | 0.592x | 4.142x | 0.148x | 66 | 5 | 0 |
| operation/hproduce | 75 | 0.688x | 0.584x | 5.160x | 0.367x | 68 | 6 | 0 |
| operation/fetch | 180 | 0.599x | 0.418x | 1.178x | 0.147x | 164 | 11 | 0 |
| operation/hfetch | 180 | 0.582x | 0.399x | 1.251x | 0.251x | 164 | 10 | 0 |
| operation/subscribe | 75 | 0.660x | 0.512x | 1.021x | 0.349x | 62 | 11 | 0 |
| operation/hsubscribe | 75 | 0.634x | 0.508x | 1.167x | 0.301x | 65 | 7 | 0 |
| operation/ack | 180 | 0.656x | 0.527x | 5.651x | 0.309x | 166 | 9 | 0 |
| operation/nack | 180 | 0.657x | 0.515x | 5.651x | 0.309x | 163 | 10 | 0 |
| operation/transaction | 75 | 0.720x | 0.618x | 2.756x | 0.596x | 63 | 7 | 0 |
| transport/tcp | 219 | 0.582x | 0.429x | 2.481x | 0.202x | 218 | 0 | 0 |
| transport/quic | 219 | 0.648x | 0.716x | 2.517x | 0.210x | 190 | 27 | 0 |
| transport/unix | 219 | 0.572x | 0.394x | 2.839x | 0.220x | 215 | 0 | 0 |
| transport/websocket | 219 | 0.814x | 0.608x | 5.016x | 0.278x | 151 | 41 | 0 |
| transport/grpc | 219 | 0.615x | 0.398x | 1.143x | 0.529x | 207 | 8 | 0 |

## Worst median latency ratios

| Cell | Samples | Go ns/op | Rust ns/op | Rust/Go | p-value |
|---|---:|---:|---:|---:|---:|
| `subscribe/quic/128B/1/128` | 1 | 1907 | 3935 | 2.063x | n/a |
| `ack/websocket/128B/1/128` | 1 | 4727 | 9192 | 1.945x | n/a |
| `subscribe/quic/1B/1/128` | 1 | 1803 | 3459 | 1.918x | n/a |
| `transaction/quic/1KiB/1/128` | 1 | 10873 | 20312 | 1.868x | n/a |
| `hsubscribe/quic/128B/1/128` | 1 | 2187 | 4038 | 1.846x | n/a |
| `nack/quic/1MiB/1/128` | 1 | 3465 | 6028 | 1.740x | n/a |
| `hproduce/quic/1B/1/128` | 1 | 2981 | 5113 | 1.715x | n/a |
| `hsubscribe/quic/1B/1/128` | 1 | 1969 | 3369 | 1.711x | n/a |
| `ack/quic/1MiB/1/128` | 1 | 3836 | 6205 | 1.618x | n/a |
| `fetch/quic/1B/1/128` | 1 | 3168 | 5111 | 1.613x | n/a |
| `subscribe/websocket/128B/1/128` | 1 | 2569 | 4110 | 1.600x | n/a |
| `produce/quic/1B/1/128` | 1 | 2947 | 4628 | 1.570x | n/a |
| `ack/quic/1B/1/128` | 1 | 3265 | 5097 | 1.561x | n/a |
| `nack/quic/1KiB/1/128` | 1 | 3126 | 4861 | 1.555x | n/a |
| `nack/quic/32KiB/1/128` | 1 | 3201 | 4926 | 1.539x | n/a |
| `hsubscribe/websocket/1B/1/128` | 1 | 2472 | 3755 | 1.519x | n/a |
| `transaction/quic/1B/1/128` | 1 | 9162 | 13877 | 1.515x | n/a |
| `ack/quic/128B/1/128` | 1 | 3157 | 4757 | 1.507x | n/a |
| `transaction/quic/128B/1/128` | 1 | 9131 | 13693 | 1.500x | n/a |
| `nack/quic/128B/1/128` | 1 | 3156 | 4666 | 1.478x | n/a |

The p-value is a two-sided Mann–Whitney approximation and is reported only with at least five samples per runtime. Raw samples and the complete per-cell summary are in the JSON artifact.
