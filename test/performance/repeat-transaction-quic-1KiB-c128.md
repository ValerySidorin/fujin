# Fujin Go/Rust Full Session Matrix Comparison

**Generated:** 2026-08-23T02:29:26.841864+00:00
**Source:** `d6ee3c6`
**Cells:** 1

## Aggregate ratios

Rust/Go below `1.0x` is better for latency, p99, allocated bytes, and allocation count.

| Scope | Cells | ns/op | p99 | B/op | allocs/op | Rust faster | >10% slower | Significant regressions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| overall | 1 | 1.509x | 1.720x | n/a | n/a | 0 | 1 | 1 |
| operation/transaction | 1 | 1.509x | 1.720x | n/a | n/a | 0 | 1 | 1 |
| transport/quic | 1 | 1.509x | 1.720x | n/a | n/a | 0 | 1 | 1 |

## Worst median latency ratios

| Cell | Samples | Go ns/op | Rust ns/op | Rust/Go | p-value |
|---|---:|---:|---:|---:|---:|
| `transaction/quic/1KiB/1/128` | 10 | 10894 | 16434 | 1.509x | 0.0002 |

The p-value is a two-sided Mann–Whitney approximation and is reported only with at least five samples per runtime. Raw samples and the complete per-cell summary are in the JSON artifact.
