# Route FETCH and HFETCH through the shared Session Core

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Move pull-based FETCH and HFETCH into the shared Session Core. Both adapters must use the same implicit reader lifecycle, batch delivery, subscription ID allocation, auto-commit behavior, connector middleware, and error semantics.

## User stories covered

- Preserve pull-consumer throughput across native protocol and gRPC.
- Remove divergent implicit-subscription behavior.
- Support later ACK/NACK migration through shared reader state.

## Acceptance criteria

- [x] Native protocol and gRPC share the implicit-reader cache and subscription ID lifecycle.
- [x] The cache key includes every parameter that changes reader semantics; FETCH and HFETCH cannot accidentally reuse an incompatible reader.
- [x] Batch payloads, headers, message IDs, empty batches, connector errors, and unsupported fetch are covered by cross-adapter contract tests.
- [x] Repeated fetches reuse the intended reader and return a stable subscription ID.
- [x] Benchmarks cover batch sizes `1`, `32`, and `256`, approved payload sizes, sustained message counts, and concurrency profiles.
- [x] Statistical comparison shows no throughput, p99, `B/op`, or `allocs/op` regression.

## Blocked by

- `issues/003-session-core-bind-cleanup.md`
