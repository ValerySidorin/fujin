# Define the zero-regression performance contract

**Type:** HITL  
**Label:** `needs-triage`

## What to build

Agree on the performance contract that every Session Core migration slice must satisfy. The contract must cover native protocol transports and gRPC under realistic message counts, payload sizes, and session concurrency rather than relying on short or single-scale runs.

## User stories covered

- Preserve current throughput, latency, and allocation behavior while extracting the shared Session Core.
- Detect small regressions that only become visible under sustained load or large messages.
- Keep every migration slice independently mergeable.

## Acceptance criteria

- [x] Approve the mandatory payload sizes: `1 B`, `128 B`, `1 KiB`, `32 KiB`, and at least `1 MiB`.
- [x] Approve fixed operation scales: `10,000`, `100,000`, and `1,000,000` messages for small/medium payloads; `1,000` and `10,000` for `1 MiB` payloads.
- [x] Approve concurrency profiles of `1`, `16`, and `128` simultaneous sessions or streams.
- [x] Cover TCP, QUIC, Unix sockets, and gRPC, with platform-specific results clearly separated.
- [x] Require repeated samples and statistical comparison: no statistically significant regression in `ns/op`, throughput, or p99 latency.
- [x] Require `B/op` and `allocs/op` not to increase for the affected benchmark paths.
- [x] Define rerun rules for noisy or inconclusive benchmark results.
- [x] Require the environment fingerprint, raw results, and comparison report to be retained.

## Blocked by

None - can start immediately.
