# Remove duplicate native and gRPC session implementations

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Complete the clean cutover to the shared Session Core. Delete the duplicated domain state machines and lifecycle code from the native protocol handler and gRPC server, leaving thin transport adapters around one core implementation.

## User stories covered

- Prevent semantic drift between native protocol and gRPC.
- Reduce the cost and risk of future broker-facing features.
- Finish the extraction without compatibility shims or deprecated paths.

## Acceptance criteria

- [x] BIND, produce, transactions, fetch, subscriptions, ACK/NACK, and cleanup have exactly one domain implementation.
- [x] Native protocol code retains streaming decode, protocol validation, ping/pong, STOP/DISCONNECT framing, and response encoding only.
- [x] gRPC code retains protobuf decode/encode, stream send serialization, and gRPC lifecycle only.
- [x] Obsolete fields, helpers, state enums, aliases, and duplicated tests are removed rather than deprecated.
- [x] Cross-adapter contract tests prove equivalent domain behavior while transport-specific tests preserve each wire contract.
- [x] The complete fast performance gate passes before this issue is merged.

## Blocked by

- `issues/004-session-core-produce.md`
- `issues/005-session-core-transactions.md`
- `issues/006-session-core-fetch.md`
- `issues/007-session-core-subscriptions.md`
- `issues/008-session-core-ack-nack.md`
