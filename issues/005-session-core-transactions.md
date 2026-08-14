# Route transaction lifecycle through the shared Session Core

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Move BEGIN, transactional PRODUCE/HPRODUCE, COMMIT, and ROLLBACK into the shared Session Core so native protocol and gRPC use one transaction state machine and one transaction-writer lifecycle.

## User stories covered

- Preserve broker transaction semantics across both client interfaces.
- Eliminate drift between native and gRPC transaction state.
- Keep transaction extraction independently testable and mergeable.

## Acceptance criteria

- [x] Both adapters share the same connected/in-transaction transitions and invalid-transition errors.
- [x] The transaction writer remains lazily acquired on the first transactional produce and is returned to the correct pool.
- [x] COMMIT flushes and commits once; ROLLBACK and disconnect roll back once.
- [x] Connector `ErrNotSupported` behavior remains observable and equivalent across native protocol and gRPC.
- [x] Contract tests cover begin/produce/commit, begin/produce/rollback, duplicate begin, commit outside a transaction, rollback outside a transaction, and disconnect during a transaction.
- [x] Transaction throughput, latency, and allocations pass the performance gate at sustained operation counts.

## Blocked by

- `issues/004-session-core-produce.md`
