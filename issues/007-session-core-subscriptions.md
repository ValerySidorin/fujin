# Route SUBSCRIBE, HSUBSCRIBE, and UNSUBSCRIBE through the shared Session Core

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Move push subscription creation, message delivery, cancellation, and unsubscribe behavior into the shared Session Core. Native protocol and gRPC remain responsible only for transport-specific stream handling and response encoding.

## User stories covered

- Preserve sustained push-delivery throughput and tail latency.
- Keep reader cancellation and shutdown semantics identical across interfaces.
- Prepare shared subscription state for ACK/NACK.

## Acceptance criteria

- [x] Both adapters share subscription ID allocation, reader creation, auto-commit, header mode, cancellation, and cleanup behavior.
- [x] Message delivery does not add avoidable payload or header copies.
- [x] Subscribe retry and connector error behavior is explicit and covered by contract tests.
- [x] UNSUBSCRIBE releases the reader and subscription ID exactly once and cannot race session cleanup.
- [x] Benchmark teardown drains or cancels producers without broken-pipe or remote-close errors.
- [x] Benchmarks cover approved payload sizes, including `1 MiB`, message counts through `1,000,000` where practical, and concurrency profiles `1/16/128`.
- [x] Statistical comparison shows no throughput, p99, `B/op`, or `allocs/op` regression.

## Blocked by

- `issues/003-session-core-bind-cleanup.md`
