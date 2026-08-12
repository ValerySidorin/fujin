# Route BIND and session cleanup through the shared Session Core

**Type:** AFK  
**Label:** `needs-triage`

## What to build

Introduce the shared Session Core through a complete BIND-to-cleanup tracer bullet. Native protocol and gRPC adapters must delegate connector selection, bind middleware, configuration overrides, connector manager creation, and resource cleanup to the same domain implementation while retaining their existing wire formats.

## User stories covered

- Keep native protocol and gRPC session semantics identical.
- Preserve hot-reloaded connector selection for new sessions.
- Merge a narrow part of the extraction without waiting for the full rewrite.

## Acceptance criteria

- [x] Native protocol and gRPC use one BIND implementation and return equivalent success and error semantics.
- [x] Bind middleware order, metadata, override whitelist validation, and connector lookup behavior remain covered by contract tests.
- [x] Session cleanup flushes and returns writers, cancels readers, rolls back an open transaction, waits for pending work, and closes the connector manager exactly once.
- [x] Existing hot reload behavior remains: a new BIND sees the latest connector config while existing sessions remain unchanged.
- [x] Transport adapters retain only decoding/encoding and transport lifecycle responsibilities for this flow.
- [x] The BIND/session lifecycle performance gate from issues 001–002 passes with no allocation increase.

## Blocked by

- `issues/002-scalable-native-grpc-baseline.md`
