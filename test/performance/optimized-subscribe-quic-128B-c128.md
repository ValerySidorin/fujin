# Fujin Go/Rust Full Session Matrix Comparison

**Generated:** 2026-08-23T02:36:12.743110+00:00
**Source:** `e9e000d-dirty`
**Cells:** 1

## Aggregate ratios

Rust/Go below `1.0x` is better for latency, p99, allocated bytes, and allocation count.

| Scope | Cells | ns/op | p99 | B/op | allocs/op | Rust faster | >10% slower | Significant regressions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| overall | 1 | 1.045x | 0.807x | n/a | n/a | 0 | 0 | 0 |
| operation/subscribe | 1 | 1.045x | 0.807x | n/a | n/a | 0 | 0 | 0 |
| transport/quic | 1 | 1.045x | 0.807x | n/a | n/a | 0 | 0 | 0 |

## Worst median latency ratios

| Cell | Samples | Go ns/op | Rust ns/op | Rust/Go | p-value |
|---|---:|---:|---:|---:|---:|
| `subscribe/quic/128B/1/128` | 10 | 1982 | 2072 | 1.045x | 0.0539 |

The p-value is a two-sided Mann–Whitney approximation and is reported only with at least five samples per runtime. Raw samples and the complete per-cell summary are in the JSON artifact.
