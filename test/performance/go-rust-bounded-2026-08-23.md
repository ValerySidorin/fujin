# Fujin Go/Rust Full Session Matrix Comparison

**Generated:** 2026-08-23T07:53:04.551181+00:00
**Source:** `7776e03`
**Cells:** 45

## Aggregate ratios

Rust/Go below `1.0x` is better for latency, p99, allocated bytes, and allocation count.

| Scope | Cells | ns/op | p99 | B/op | allocs/op | Rust faster | >10% slower | Significant regressions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| overall | 45 | 0.611x | 0.494x | n/a | n/a | 41 | 0 | 0 |
| operation/produce | 5 | 0.615x | 0.517x | n/a | n/a | 4 | 0 | 0 |
| operation/hproduce | 5 | 0.632x | 0.508x | n/a | n/a | 4 | 0 | 0 |
| operation/fetch | 5 | 0.602x | 0.441x | n/a | n/a | 5 | 0 | 0 |
| operation/hfetch | 5 | 0.624x | 0.469x | n/a | n/a | 5 | 0 | 0 |
| operation/subscribe | 5 | 0.557x | 0.456x | n/a | n/a | 4 | 0 | 0 |
| operation/hsubscribe | 5 | 0.560x | 0.451x | n/a | n/a | 5 | 0 | 0 |
| operation/ack | 5 | 0.640x | 0.551x | n/a | n/a | 5 | 0 | 0 |
| operation/nack | 5 | 0.629x | 0.518x | n/a | n/a | 5 | 0 | 0 |
| operation/transaction | 5 | 0.650x | 0.545x | n/a | n/a | 4 | 0 | 0 |
| transport/tcp | 9 | 0.478x | 0.354x | n/a | n/a | 9 | 0 | 0 |
| transport/quic | 9 | 0.566x | 0.647x | n/a | n/a | 9 | 0 | 0 |
| transport/unix | 9 | 0.661x | 0.448x | n/a | n/a | 9 | 0 | 0 |
| transport/websocket | 9 | 0.992x | 0.806x | n/a | n/a | 5 | 0 | 0 |
| transport/grpc | 9 | 0.482x | 0.354x | n/a | n/a | 9 | 0 | 0 |

## Worst median latency ratios

| Cell | Samples | Go ns/op | Rust ns/op | Rust/Go | p-value |
|---|---:|---:|---:|---:|---:|
| `subscribe/websocket/128B/1/16` | 5 | 3256 | 3469 | 1.065x | 0.0122 |
| `hproduce/websocket/128B/1/16` | 5 | 5851 | 6051 | 1.034x | 0.0601 |
| `transaction/websocket/128B/1/16` | 5 | 17340 | 17495 | 1.009x | 0.1437 |
| `produce/websocket/128B/1/16` | 5 | 5852 | 5867 | 1.003x | 0.6004 |
| `nack/websocket/128B/1/16` | 5 | 5859 | 5853 | 0.999x | 1.0000 |
| `ack/websocket/128B/1/16` | 5 | 5845 | 5811 | 0.994x | 0.6761 |
| `hsubscribe/websocket/128B/1/16` | 5 | 3296 | 3262 | 0.990x | 1.0000 |
| `fetch/websocket/128B/1/16` | 5 | 5981 | 5525 | 0.924x | 0.0122 |
| `hfetch/websocket/128B/1/16` | 5 | 5770 | 5308 | 0.920x | 0.0947 |
| `ack/unix/128B/1/16` | 5 | 3969 | 2756 | 0.694x | 0.0122 |
| `hsubscribe/unix/128B/1/16` | 5 | 2301 | 1557 | 0.677x | 0.0112 |
| `subscribe/unix/128B/1/16` | 5 | 2259 | 1526 | 0.676x | 0.0119 |
| `hfetch/unix/128B/1/16` | 5 | 4185 | 2779 | 0.664x | 0.0122 |
| `transaction/unix/128B/1/16` | 5 | 12272 | 8066 | 0.657x | 0.0122 |
| `nack/unix/128B/1/16` | 5 | 4210 | 2761 | 0.656x | 0.0122 |
| `hproduce/unix/128B/1/16` | 5 | 4172 | 2721 | 0.652x | 0.0122 |
| `fetch/unix/128B/1/16` | 5 | 4196 | 2734 | 0.652x | 0.0122 |
| `hproduce/quic/128B/1/16` | 5 | 11194 | 7031 | 0.628x | 0.0122 |
| `transaction/quic/128B/1/16` | 5 | 31762 | 19854 | 0.625x | 0.0122 |
| `hfetch/quic/128B/1/16` | 5 | 11635 | 7271 | 0.625x | 0.0122 |

The p-value is a two-sided Mann–Whitney approximation and is reported only with at least five samples per runtime. Raw samples and the complete per-cell summary are in the JSON artifact.
