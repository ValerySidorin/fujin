# Fujin Go/Rust Full Session Matrix Comparison

**Generated:** 2026-08-23T05:18:28.148562+00:00
**Source:** `bfd8392-dirty`
**Cells:** 1

## Aggregate ratios

Rust/Go below `1.0x` is better for latency, p99, allocated bytes, and allocation count.

| Scope | Cells | ns/op | p99 | B/op | allocs/op | Rust faster | >10% slower | Significant regressions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| overall | 1 | 1.030x | 0.700x | n/a | n/a | 0 | 0 | 0 |
| operation/fetch | 1 | 1.030x | 0.700x | n/a | n/a | 0 | 0 | 0 |
| transport/quic | 1 | 1.030x | 0.700x | n/a | n/a | 0 | 0 | 0 |

## Worst median latency ratios

| Cell | Samples | Go ns/op | Rust ns/op | Rust/Go | p-value |
|---|---:|---:|---:|---:|---:|
| `fetch/quic/1B/1/128` | 10 | 3226 | 3322 | 1.030x | 0.1620 |

The p-value is a two-sided Mann–Whitney approximation and is reported only with at least five samples per runtime. Raw samples and the complete per-cell summary are in the JSON artifact.
