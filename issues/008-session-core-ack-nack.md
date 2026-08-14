# Route ACK and NACK through the shared Session Core

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Move ACK and NACK routing, message ID handling, reader lookup, and per-message result aggregation into the shared Session Core. Native protocol and gRPC must map their wire representations to the same domain operation.

## User stories covered

- Preserve broker-specific acknowledgement semantics.
- Keep native protocol and gRPC per-message results equivalent.
- Complete the shared reader lifecycle.

## Acceptance criteria

- [x] ACK/NACK use shared subscription state created by FETCH or SUBSCRIBE.
- [x] Broker-specific encoded message IDs remain opaque to the core except for routing to the correct reader.
- [x] Unknown subscription IDs, unsupported operations, top-level errors, and per-message errors have equivalent adapter behavior.
- [x] Contract tests cover batch sizes `1`, `32`, and `256`, mixed success/error results, auto-commit readers, and cleanup races.
- [x] ACK/NACK benchmarks run at sustained operation counts and pass the statistical performance and allocation gate.

## Blocked by

- `issues/006-session-core-fetch.md`
- `issues/007-session-core-subscriptions.md`
