# Resolve Session Core and connector contract ambiguities

**Type:** Design
**Label:** `needs-discussion`

## Purpose

This file records semantic decisions that must be made explicit before adding more connector adapters, including the library-specific ZeroMQ connectors planned in `issues/011-zeromq-connectors.md`.

The repository currently has no `CONTEXT.md` domain glossary or architecture decision records. The effective contract is distributed across Session Core, native and gRPC adapters, connector interfaces, connector implementations, and tests. Several interfaces constrain method shape without fully defining operation semantics, allowing individually reasonable but incompatible adapter behavior.

Each section below records:

- **Current behavior** — what the code does now.
- **Ambiguity** — the competing interpretations.
- **Risk** — what can fail or become misleading.
- **Decision required** — the question to resolve.

Decisions should be discussed in order. Once agreed, each section should receive a **Decision** subsection and the resulting implementation/documentation work should be tracked explicitly.

## 1. Meaning of successful BIND

**Current behavior**

`Core.Bind` selects a connector configuration, runs bind middleware, applies permitted overrides, creates `ManagerV2`, and changes the session to `StateConnected`. It does not instantiate the registered connector adapter, validate a requested route, create broker resources, or prove broker connectivity. Connector factories and route validation run lazily on the first reader or writer operation.

Consequently, BIND can succeed when:

- the configured plugin was not compiled into the binary;
- connector settings are invalid for the concrete adapter;
- the broker endpoint is unavailable;
- a later operation names a missing or directionally invalid route.

**Ambiguity**

BIND may mean one of three things:

1. only select a configuration snapshot;
2. validate that the selected connector is structurally usable while retaining lazy network resources;
3. establish broker readiness before responding successfully.

**Risk**

Clients receive a successful session establishment response but discover configuration or plugin errors only on an unrelated first operation. Eager readiness, however, can make BIND slow, require route-independent connectivity semantics, and defeat lazy resource allocation.

**Decision required**

What exact guarantee does a successful BIND provide, and which validation belongs at BIND versus the first route operation?

**Decision — locally valid session**

A successful BIND freezes one immutable connector-configuration snapshot and proves that it is locally usable. Before success, Fujin must verify that the connector plugin is registered in the running build, decode and validate its settings, and validate every declared route together with its operation capabilities. BIND remains free of broker I/O and does not create readers, writers, subscriptions, sockets, or connections.

Broker availability, remote authentication, and operation-specific readiness remain responsibilities of the first operation that needs the corresponding resource. Existing bound sessions retain their validated configuration generation across hot reloads.

This decision requires separating side-effect-free connector/configuration validation from lazy resource construction. A route named by a later command must still be checked against the already validated route set and capability declaration.

## 2. Lazy transaction initialization

**Current behavior**

`BEGIN(route)` flushes and returns ordinary writers, stores the transaction route, and enters `StateInTransaction`. It does not acquire a writer or call `BeginTx`. The first `TX_PRODUCE` lazily acquires the writer and calls `BeginTx`.

Consequently:

- BEGIN can succeed for a missing route;
- BEGIN can succeed for a connector without transactions;
- COMMIT and ROLLBACK without TX_PRODUCE succeed as empty transactions;
- a `BeginTx` failure occurs on TX_PRODUCE and leaves the session in transaction state;
- a later TX_PRODUCE may retry lazy initialization.

**Ambiguity**

BEGIN can mean declaration of transactional intent or successful creation of a broker transaction.

**Risk**

The response opcode called BEGIN does not necessarily correspond to a broker-side begin. Unsupported transactions and invalid routes fail later than clients may expect.

**Decision required**

Should transaction initialization remain lazy? If yes, are empty transactions and retries after lazy initialization failure intentional protocol guarantees?

**Decision — eager broker transaction initialization**

BEGIN must validate the route's transactional capability, acquire its writer, and successfully call `BeginTx` before changing the session state or returning success. `BEGIN_OK` therefore means that a concrete broker transaction exists. A failure leaves the session in `StateConnected` and the failed writer must be handled according to the terminal-error and resource-discard contract.

An empty transaction is a real broker transaction: COMMIT or ROLLBACK after BEGIN invokes the corresponding adapter operation even when no TX_PRODUCE occurred. Transaction resource allocation and any required broker round trip are intentionally paid at BEGIN because the protocol operation promises stronger semantics than declaration of future intent.

## 3. Transaction terminal-state and error semantics

**Current behavior**

COMMIT clears `txWriter` and `txRoute` and returns the session to `StateConnected` before flushing and committing. It calls `CommitTx` even when `Flush` fails and returns the writer to its pool regardless of either result. ROLLBACK also returns to connected state and returns the writer to the pool regardless of rollback success. Close implicitly rolls back an active initialized transaction; an uninitialized transaction simply disappears.

**Ambiguity**

A failed commit or rollback may either terminate the local transaction unconditionally or leave it available for retry/recovery. A writer that encountered a terminal transaction error may be reusable or poisoned.

**Risk**

Retry is impossible after failure, while a possibly invalid writer can be reused by a later operation. Different broker libraries have different post-failure guarantees.

**Decision required**

Which transaction errors are terminal, when is retry allowed, and when must a writer be discarded instead of returned to the pool?

**Decision — fail closed after terminal operations**

COMMIT and ROLLBACK always terminate the local transaction state. A writer involved in any flush, commit, or rollback error is poisoned: it must be closed and never returned to the pool. The next BEGIN acquires a fresh writer.

If the pre-commit Flush fails, Core must not call `CommitTx`; it performs best-effort rollback and reports the transaction as aborted. If `CommitTx` itself fails, the remote outcome may be unknowable, so Core must not retry automatically and must report an outcome-distinguishing error rather than claiming rollback or commit. A rollback failure likewise terminates local state and is reported, with the writer discarded.

Successful terminal operations may return a writer to the pool only when the adapter contract explicitly leaves it outside a transaction and reusable. Session close follows the same rollback and discard rules.

## 4. Meaning of successful PRODUCE

**Current behavior**

All writers report through the same callback, but adapters complete it at different durability levels:

- local client acceptance or publish call;
- completion of an asynchronous library callback;
- broker acknowledgement;
- confirmed persistence, where the library provides it.

For example, NATS Core and ZeroMQ PUB are fundamentally weaker than NATS JetStream or a confirmed broker publish.

**Ambiguity**

A successful Fujin PRODUCE can mean accepted locally, written to a connection, accepted by a peer, or durably accepted by a broker.

**Risk**

The same Fujin response appears to provide a uniform guarantee while actual loss windows differ substantially by connector and configuration.

**Decision required**

Does Fujin define a minimum produce guarantee, expose capability/guarantee metadata, or explicitly inherit and document each connector's native guarantee?

**Decision — explicit route-level acceptance guarantee**

Every producer route declares one stable success guarantee in its adapter-derived capability profile for the lifetime of the bound configuration snapshot:

- `local_accept` — the connector library or socket accepted the message into local process buffering;
- `peer_accept` — a remote broker or peer positively acknowledged receipt;
- `durable_accept` — a remote broker positively acknowledged durable storage under the configured persistence policy.

A successful PRODUCE means that the route's declared level was reached. An adapter must never advertise a stronger level than the acknowledgement it actually observes. Stronger acknowledgements are not erased to a common weakest meaning. ZeroMQ PUB, absent an application-level acknowledgement protocol, can provide only `local_accept`; it cannot claim subscriber delivery.

The operation response remains success or error. The guarantee is configuration/capability metadata established at BIND rather than a per-message response field.

## 5. Writer callback and Flush contract

**Current behavior**

The writer interface does not state whether callbacks are synchronous or asynchronous, whether they run exactly once, whether they may run after `Flush`, or what happens on `Close`. Session lifecycle code relies on adapter-specific `Flush` behavior to drain pending work. Payload ownership is documented in Session Core but not at the public writer interface.

**Ambiguity**

`Flush` may mean flush local batching, wait for callbacks, wait for network writes, or wait for broker acknowledgement.

**Risk**

Session close, transaction begin, and writer pooling can race callbacks or return resources before operations are complete. New adapters may satisfy the method signatures while violating Core lifecycle assumptions.

**Decision required**

Specify callback cardinality, ordering, payload ownership, and the exact lifecycle barrier guaranteed by `Flush` and `Close`.

**Decision — exactly-once callbacks and a snapshot Flush barrier**

Every Produce or HProduce accepted by a writer must invoke its callback exactly once, synchronously or asynchronously. The callback reports whether that message reached the route's declared acceptance guarantee or failed. Until the callback begins, payload and header bytes remain immutable and available to the adapter; once the callback returns, the adapter must no longer access them.

`Flush(ctx)` is a snapshot barrier. It may run concurrently with later Produce calls, but it must not return nil until every operation accepted before the Flush call has delivered its callback successfully. Failure to reach the declared guarantee or to complete the callbacks returns an error. Core treats a writer whose Flush failed as poisoned and closes it rather than pooling it.

`Close` rejects new operations, resolves every still-pending callback with an error, and releases resources. It is not an implicit successful Flush and must not report pending messages as successful merely to complete shutdown.

## 6. Explicit connector capabilities versus fat interfaces

**Current behavior**

Every reader must implement Subscribe, Fetch, Ack, Nack, and header-aware variants. Every writer must implement regular/headered produce and transactions. Unsupported operations are represented inconsistently:

- `util.ErrNotSupported`;
- successful no-op;
- fallback that silently discards unsupported data;
- partially equivalent broker behavior.

Capabilities are currently duplicated manually in E2E tests rather than exposed by the connector model.

**Examples**

- Kafka NACK succeeds without changing broker state.
- Redis Streams NACK succeeds without changing broker state.
- MQTT NACK deletes local pending state and relies on eventual redelivery.
- NSQ and Redis writers accept HProduce but discard headers.
- several readers return `ErrNotSupported` for Fetch.

**Ambiguity**

Unsupported behavior can be a runtime error, a documented degradation, or an alternate semantic implementation.

**Risk**

Clients cannot discover support before issuing an operation, and adapters can accidentally claim success for behavior they did not perform.

**Decision required**

Should capability and guarantee metadata become part of the connector/route contract? Which operations may legally degrade, and which must return `ErrNotSupported`?

**Decision — adapter-derived route capability profile**

Each connector adapter derives a capability profile for every route from the already decoded and validated configuration. The profile is frozen with the configuration snapshot during BIND. It must distinguish operation support from delivery guarantees: operation support covers produce, header preservation, transactions, subscribe, fetch, manual ACK, and meaningful NACK; guarantee metadata separately describes connector-specific acknowledgement or durability levels.

Session Core checks the selected route's capability before creating a reader or writer. An unsupported operation fails explicitly. An adapter must not report success while silently dropping headers, implementing NACK as a no-op, or otherwise weakening a declared operation. Behavior that is intentionally weaker but still meaningful must have its own explicit capability/guarantee value rather than masquerading as full support.

Capabilities are adapter-derived rather than manually duplicated in YAML or test matrices. Tests may assert the profile but must not be its source of truth.

## 7. Meaning of auto-commit

**Current behavior**

The `autoCommit` boolean maps to different broker operations:

- broker auto-ack/no-ack mode;
- explicit ACK immediately after invoking the message callback;
- NSQ `Finish` after callback;
- Redis `NOACK`;
- omission of message IDs.

The callback generally means that a transport adapter has enqueued a Fujin response, not that the client has received or processed it.

**Ambiguity**

Auto-commit could mean before delivery, after adapter callback, after network enqueue, or after confirmed client processing. The current behavior is usually at-most-once relative to the Fujin client.

**Risk**

The name implies a stronger delivery guarantee than the implementation provides, and behavior varies by connector.

**Decision required**

Define the common delivery point represented by auto-commit or replace it with more precise delivery-mode terminology.

**Decision — at-most-once auto-settle relative to the Fujin client**

The domain concept is `auto-settle`; the existing wire/config name `autoCommit` may remain for compatibility. In auto-settle mode, the client receives no settlement-capable message ID and Fujin or the broker completes delivery without a later client ACK/NACK. Once a message has been handed to Session Core, redelivery to that Fujin client is not guaranteed even if encoding, outbound enqueue, transport delivery, or client processing later fails.

Adapters may use native no-ack, immediate broker settlement, or settlement after their message callback, but all must fit this common at-most-once client-visible guarantee. Connector-specific settlement timing remains documented metadata rather than being implied by the generic mode name. Manual-settlement mode keeps delivery unsettled until a valid client ACK or NACK.

## 8. SUBSCRIBE readiness

**Current behavior**

Session Core allocates a reader and subscription ID, invokes `ready`, and sends the successful subscription response before starting `reader.Subscribe` in a goroutine. The first broker subscription attempt therefore occurs after client-visible success.

**Ambiguity**

Successful SUBSCRIBE can mean local registration or active broker subscription.

**Risk**

A client can receive success even when the broker subscription never becomes active. Messages published immediately after success may be lost, especially with best-effort systems such as ZeroMQ PUB/SUB.

**Decision required**

Must adapters establish broker readiness before success, or is local asynchronous registration the intended protocol contract?

**Decision — strongest adapter-observable readiness**

SUBSCRIBE succeeds only after the adapter has completed every readiness step its underlying system can observe: the reader exists, route and mode are validated, socket or consumer configuration is installed, subscription filters are active, any broker registration acknowledgement exposed by the client API has succeeded, and the delivery loop is ready to receive. Core must not send `SUBSCRIBE_OK` before invoking adapter subscription setup.

This guarantee does not imply that a publisher currently exists, that a peer will ever send a message, or that the subscription will remain healthy after success. For systems such as ZeroMQ PUB/SUB that provide no remote subscription acknowledgement, configured local socket/filter readiness is the strongest available guarantee. The reader contract must expose a readiness boundary separately from the long-running receive lifecycle, either through a staged API or an explicit ready signal.

## 9. Subscription retry policy and reader reuse

**Current behavior**

Session Core retries every subscription error forever with a fixed one-second delay, using the same reader instance. Errors are reported only through adapter logging. There is no retry classification, backoff, terminal state, or reader recreation policy.

**Ambiguity**

Errors may be transient, terminal, or leave the reader partially initialized. Some adapters can safely call Subscribe again; others may create duplicate subscriptions or remain poisoned.

**Risk**

Infinite hidden retries can consume resources, duplicate consumers, or keep a permanently failed subscription looking healthy to the client.

**Decision required**

Who owns retry policy: Session Core or the connector adapter? Define retryability, backoff, recreation, terminal failure, and client notification.

**Decision — adapter-owned recovery after readiness**

Session Core invokes the subscription receive lifecycle exactly once. An error before the readiness boundary fails SUBSCRIBE and is not hidden behind an automatic Core retry. After readiness, the adapter owns ordinary reconnect, rebalance, and retry behavior using the native client library and adapter configuration appropriate to that broker.

If the receive lifecycle returns after readiness, the subscription has terminally ended. Core closes and removes the reader and must make the failure observable to the client. Where the current wire protocol cannot represent an asynchronous terminal subscription error, closing the affected session is safer than leaving a subscription ID that appears live. Core must not blindly call Subscribe again on the same reader.

## 10. FETCH batch-size and concurrency semantics

**Current behavior**

The `n` argument is interpreted differently:

- Kafka and JetStream use it as a batch maximum;
- Redis Streams uses configured `Count` and does not directly honor the request value;
- overlapping Kafka/JetStream Fetch calls can return a successful empty result when the shared reader is already fetching.

Session Core caches implicit fetch readers by route, auto-commit, and header mode.

**Ambiguity**

`n` can be an exact target, maximum, or hint. Concurrent Fetch can queue, serialize, fail as busy, or return zero messages.

**Risk**

Identical Fujin requests behave differently by connector, and contention is indistinguishable from an empty broker result.

**Decision required**

Define the meaning of `n`, timeout/empty results, and concurrent Fetch behavior.

**Decision — strict maximum batch size and explicit contention**

`n` is a strict positive upper bound: zero is invalid, and one FETCH response contains between zero and `n` messages. A successful empty result is valid only when the underlying fetch/poll actually completed without messages or its configured wait expired. Adapter prefetch, broker batch sizes, and configured Count values may optimize internal reads but must not cause more than `n` messages to be emitted for the request.

Only one FETCH may be active on a given reader. A concurrent FETCH against that reader fails explicitly as busy; it must not masquerade as an empty broker result or wait in an invisible Core queue. Independent readers and routes remain concurrent.

## 11. ACK and NACK semantics

**Current behavior**

ACK/NACK behavior ranges from native broker acknowledgement/requeue to successful no-op. Some adapters accept ACK/NACK for auto-commit readers. NACK may mean immediate redelivery, delayed redelivery, release, local pending-state deletion, or nothing.

**Ambiguity**

A single protocol opcode represents multiple materially different delivery actions.

**Risk**

A successful NACK may not cause redelivery, and clients cannot reason consistently about recovery behavior.

**Decision required**

Define the minimum ACK/NACK guarantees, allowed connector-specific variants, and whether unsupported semantics must fail explicitly.

**Decision — explicit route settlement profile**

Every route that supports manual settlement declares a settlement profile. ACK declares its granularity, at minimum `single` or `cumulative`. NACK independently declares its effect as `requeue`, `release`, `drop`, or `unsupported`. A successful operation means that the declared effect was applied; a no-op must not return success.

ACK and NACK are invalid in auto-settle mode. A connector that cannot provide meaningful manual settlement does not advertise that capability. Connector-specific details such as delayed redelivery, cumulative offset advancement, or requeue policy remain profile metadata and configuration, but may not contradict the declared effect.

## 12. Message-ID validation and scope

**Current behavior**

Message IDs are opaque bytes encoded by each reader and decoded directly by adapters. Some implementations validate length; others call `binary.BigEndian` on untrusted slices without checking and can panic. IDs may include broker topic/source data, partition, epoch, offset, delivery tag, packet ID, or stream ID. Core validates neither expected length nor association with the selected reader beyond the subscription ID.

**Ambiguity**

An ID may be valid only for one reader instance, one route, one broker session, or indefinitely.

**Risk**

Malformed input can panic; stale or cross-reader IDs can acknowledge unintended messages or produce confusing errors.

**Decision required**

Define validation ownership, minimum length checks, reader/route scoping, stale-ID behavior, and whether message IDs need an internal type/version envelope.

**Decision — versioned Core envelope scoped to a reader incarnation**

Every client-visible manual-settlement ID is a Core envelope containing a format version, an unguessability-neutral reader-incarnation identifier, and an opaque adapter payload. The subscription ID selects the current reader; Core additionally verifies envelope version, minimum structure, and matching incarnation before invoking the adapter. Reusing a subscription slot or recreating its reader creates a new incarnation and invalidates every earlier ID.

The adapter validates the exact structure and values of its own payload and must return an error for malformed input rather than panic. Successful settlement consumes an ID; duplicate use is invalid. Closing a reader invalidates all outstanding IDs. The envelope provides type and lifecycle scope, not cryptographic authorization; session isolation remains the authorization boundary.

## 13. Header model

**Current behavior**

Headers are represented as alternating `[][]byte` key/value entries. The interface does not explicitly define even cardinality, duplicate keys, ordering, binary values, multiple values, nil versus empty, or unsupported behavior.

Adapters currently:

- collapse NATS multi-value headers into comma-joined values;
- iterate maps in nondeterministic order;
- drop AMQP header values of unsupported types;
- silently discard headers in some writers;
- expose empty headers for systems without native headers.

**Ambiguity**

Headers can be an ordered multimap, a string map, arbitrary binary metadata, or best-effort connector metadata.

**Risk**

Round trips are lossy and nondeterministic. A ZeroMQ multipart mapping cannot be designed safely without a canonical header contract.

**Decision required**

Define the canonical header data model and the required behavior when a connector cannot preserve it.

**Decision — unordered multimap with lossless preservation**

The canonical header model is an unordered multimap. Each key is a non-empty UTF-8 string; each value is an arbitrary byte sequence. Duplicate keys are valid and remain distinct values. Pair order carries no semantic meaning, and nil is equivalent to an empty collection. In the current alternating `[][]byte` representation, odd cardinality is malformed input.

A route advertises header support only when its adapter can preserve every key, binary value, and duplicate-key multiplicity losslessly. Adapters must not silently drop entries, coerce unsupported value types, or comma-join multiple values. Connectors without a lossless mapping reject headered operations explicitly. Implementations may use deterministic ordering for encoding or tests, but clients must not depend on it.

## 14. Route versus broker destination and delivered topic

**Current behavior**

Clients address a configured Fujin route. Reader callbacks separately return a `topic` string that adapters populate with Kafka topic, NATS subject, RabbitMQ exchange, AMQP source, NSQ topic, Redis stream/channel, or another broker address. Some message-ID encodings include this value.

**Ambiguity**

A route may correspond to one destination, a filter, a queue, or multiple streams. The callback `topic` is not a consistent domain concept.

**Risk**

Protocol adapters and clients may treat returned topic as the configured route even when it is a broker-specific source. ZeroMQ subscription prefixes and multipart topics make this distinction load-bearing.

**Decision required**

Separate and name Fujin route, broker destination, delivered source, and message-ID scope.

**Decision — distinct route, destination, source, and filter concepts**

`route` is the stable Fujin configuration name selected by a client operation. `destination` is the configured broker-specific address used to produce or consume. `filter` is a configured broker selector that may cover multiple destinations or sources. `source` is the actual broker-specific origin reported for one delivered message.

The current reader callback parameter named `topic` becomes `source`. It may be empty when a system cannot report an origin, but adapters must not substitute the Fujin route or configured destination merely to populate it. An adapter may embed source data in its opaque message-ID payload when settlement requires it.

For ZeroMQ, an endpoint is a destination, a SUB prefix is a filter, and a multipart frame is a source only when the route's declared framing schema assigns it that meaning.

## 15. Resource ownership and pooling

**Current behavior**

`ManagerV2` and its writer pools are session-local. Writers are reused within a session, particularly across transaction cycles, but not shared across sessions. Every Subscribe creates a reader. Pools have a fixed size and return no indication whether a writer was pooled or closed.

**Ambiguity**

Broker connections and bound endpoints may belong to a session, configuration generation, connector instance, or process.

**Risk**

Session-local ZeroMQ bind sockets conflict when multiple sessions use the same endpoint. Conversely, process-wide resources introduce refcounting, isolation, reload, and shutdown requirements absent from the current manager model.

**Decision required**

Define ownership levels for readers, writers, connections, and listen/bind endpoints before supporting adapters that own server-like resources.

**Decision — configuration-generation runtime with session-scoped leases**

Each validated configuration generation owns an adapter runtime and any safely shared physical resources: connection pools, bound or listening endpoints, and endpoint registries. A bound session acquires logical leases from that runtime for its readers, writers, subscriptions, and transactions. Closing a session releases its leases but does not close a physical resource still used by another session.

A retired configuration generation remains alive until its final bound session and outstanding lease are gone, then closes its runtime deterministically. Adapters choose which resources are shareable but must preserve session isolation and the route capability contract. A ZeroMQ bind socket is generation-owned; sessions receive compatible logical handles instead of attempting duplicate binds.

## 16. Configuration snapshot and hot reload

**Current behavior**

The config provider is evaluated at BIND. A bound session keeps its modified connector configuration and manager; later reloads affect only new BIND operations. Existing readers and writers are not reconfigured.

**Ambiguity**

Hot reload can mean new sessions only, new lazy resources within old sessions, or live mutation of existing resources.

**Risk**

Adapter implementations may independently attempt live updates, creating inconsistent behavior and races.

**Decision required**

Make configuration-generation ownership and reload visibility an explicit Session Core guarantee.

**Decision — immutable configuration generation per BIND**

A reload first decodes and validates a complete new configuration and constructs its generation runtime. Only after success is that generation atomically published for future BIND operations. A failed reload leaves the current generation untouched.

Each successful BIND pins one immutable generation, including its route definitions, capability and guarantee profiles, and adapter runtime. Existing sessions never migrate implicitly. Resources created lazily later by an old session still come from its pinned generation. A retired generation closes only after its final session and resource lease have been released.

## 17. Cleanup and error visibility

**Current behavior**

Session close reports writer Flush and transaction Rollback errors, but ignores reader Close errors. `Manager.Close`, pool close, and `PutWriter` cannot return errors; writer Close errors are discarded by the pool. Some cleanup uses `context.WithoutCancel` without an independent timeout.

**Ambiguity**

Cleanup errors can be client-visible session errors, operational logs, metrics only, or intentionally ignored. Cleanup may be bounded or potentially block forever.

**Risk**

Resource leaks and failed broker cleanup disappear silently, while unbounded shutdown can block server lifecycle.

**Decision required**

Define cleanup error reporting, resource discard rules, and bounded cleanup contexts.

**Decision — bounded aggregate cleanup at the ownership boundary**

Every Close operation is idempotent and executes within an explicit deadline. Session shutdown first rejects new operations, performs best-effort rollback of an active transaction, flushes ordinary writers within the remaining budget, and then attempts to close every reader, writer, and lease regardless of earlier failures. Errors are aggregated rather than short-circuiting cleanup. A failed or timed-out resource is poisoned and must never return to a pool.

Failures of session-owned cleanup are returned to the client when the protocol can still carry a response; otherwise they are emitted as structured logs and metrics. Failures of generation-owned runtime cleanup belong to server lifecycle and operator observability, not to an arbitrary session. Deadline expiry ends Fujin's wait even when an underlying broker library does not return.

## 18. Connector factory and validation lifecycle

**Current behavior**

The registered `Factory` receives settings and returns a connector adapter. `GetConfigValueConverter` calls the same factory with nil config and nil logger, requiring a special side-effect-free mode. `NewReader` and `NewWriter` each invoke the factory again rather than sharing a validated connector instance.

**Ambiguity**

A connector adapter can be a stateless configuration decoder, a connection owner, or a resource factory. The current seam supports all three without defining which lifecycle is intended.

**Risk**

Factories may perform unexpected I/O repeatedly, nil-converter construction can diverge from normal validation, and shared resources have no clear home.

**Decision required**

Define whether the connector adapter is immutable validated configuration, a session-scoped resource owner, or only a reader/writer factory; separate config conversion metadata if necessary.

**Decision — Descriptor, Compile, and Runtime lifecycle**

Connector registration exposes a static, side-effect-free plugin descriptor containing its type name, build availability, and configuration decoder/schema. `Compile` decodes, normalizes, and fully validates one raw connector configuration without broker I/O, derives every route capability and guarantee profile, and returns an immutable compiled configuration.

`OpenRuntime` creates the configuration-generation-owned runtime from that compiled configuration. The runtime owns shared physical resources and exposes route-aware acquisition methods for session-scoped reader and writer leases; acquisition does not repeat configuration parsing or capability derivation. `Runtime.Close(ctx)` performs bounded aggregate generation cleanup.

The current nil-config factory convention is removed. Schema/converter discovery never requires a logger or constructs resources, while runtime construction receives its explicit operational dependencies.


## Recommended discussion order

1. Meaning of successful BIND.
2. Connector capabilities and validation lifecycle.
3. Lazy transaction initialization.
4. Transaction terminal errors and poisoned writers.
5. Produce callback and Flush guarantees.
6. Auto-commit, ACK, and NACK guarantees.
7. Subscribe readiness and retry ownership.
8. Fetch batch and concurrency semantics.
9. Header and message-ID models.
10. Route/destination terminology.
11. Resource ownership, hot reload, and cleanup.

## Decision log

All 18 ambiguities now have recorded decisions. The agreed model is:

1. BIND creates a locally validated session without broker I/O.
2. Transaction BEGIN eagerly creates the broker transaction.
3. Transaction terminal failures fail closed and poison the writer.
4. PRODUCE success reaches an explicit route-level acceptance guarantee.
5. Writer callbacks are exactly-once; Flush is a snapshot barrier.
6. Capabilities are derived by the adapter for each route.
7. `autoCommit` is at-most-once client-visible auto-settlement.
8. SUBSCRIBE waits for the strongest adapter-observable readiness.
9. Subscription recovery after readiness belongs to the adapter.
10. FETCH uses a strict maximum batch size and explicit busy errors.
11. ACK/NACK behavior is declared in a route settlement profile.
12. Message IDs use a versioned, reader-incarnation-scoped Core envelope.
13. Headers are an unordered, losslessly preserved multimap.
14. Route, destination, source, and filter are distinct concepts.
15. Configuration generations own runtimes; sessions own leases.
16. Every BIND pins an immutable configuration generation.
17. Cleanup is bounded, exhaustive, and aggregates errors.
18. Connector lifecycle is split into Descriptor, Compile, and Runtime.

The next step is to turn these decisions into an ordered implementation migration before adding ZeroMQ connectors.
