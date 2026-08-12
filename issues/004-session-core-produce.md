# Route PRODUCE and HPRODUCE through the shared Session Core

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Move non-transactional PRODUCE and HPRODUCE behavior into the shared Session Core. Native protocol and gRPC must decode into the same command, use the same writer lifecycle and middleware chain, and encode the core result back into their existing response formats.

## User stories covered

- Preserve produce throughput and allocation behavior.
- Keep headers, correlation IDs, and connector middleware semantics consistent across interfaces.
- Continue the extraction as an independently verifiable vertical slice.

## Acceptance criteria

- [x] Native protocol and gRPC share writer acquisition, writer reuse, callback completion, flush, and error mapping behavior.
- [x] Payload and header ownership is explicit; the core introduces no avoidable payload or header copies.
- [x] Correlation IDs and asynchronous response ordering retain their current observable behavior.
- [x] Contract tests cover empty/invalid input, connector failures, HPRODUCE headers, middleware rejection, and multiple topics.
- [x] Benchmarks cover all approved payload sizes through at least `1 MiB`, fixed message counts through `1,000,000`, and concurrency profiles `1/16/128`.
- [x] Statistical comparison shows no native or gRPC throughput, p99, `B/op`, or `allocs/op` regression.

## Blocked by

- `issues/003-session-core-bind-cleanup.md`
