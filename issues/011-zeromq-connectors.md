# Implement `zeromq_pebbe` connector

**Type:** Feature
**Status:** Complete

## Decision summary

- Implement the first ZeroMQ connector with `github.com/pebbe/zmq4` and native `libzmq`.
- Package: `public/plugins/connector/zeromq/pebbe`.
- Registration name: `zeromq_pebbe`.
- Pin `github.com/pebbe/zmq4 v1.4.0`; require `libzmq >= 4.3.5` in supported build and runtime images.
- Keep the plugin opt-in because it requires CGO and a system `libzmq`. Do not import it from the default `public/plugins/connector/all` package.
- Require the `zeromq_pebbe` build tag and the builder's `-cgo` option.
- Support PUB, SUB, PUSH, and PULL in the first release.
- Support both `connect` and `bind`; default to `connect`.
- Support `fujin_v1` and `raw` multipart framing; default to `fujin_v1`.
- Support NULL and full CURVE client/server security.
- Expose continuous `Subscribe` reads only. `Fetch`, manual ACK/NACK, and transactions are not capabilities of this release.

The earlier generic implementation from commit `9ea4091` is not a basis for restoration. It registered a generic `zeromq` type, supported only connect-side PUB/SUB, silently discarded headers and malformed messages, did not satisfy the current readiness contract, and had no real interoperability fixture.

## Why `pebbe/zmq4`

As of 2026-08-18, `pebbe/zmq4` has a current `v1.4.0` release and exposes the mature `libzmq` behavior needed here: real high-water marks, send/receive timeouts, polling, reconnect controls, socket monitoring, bind/unbind, connect/disconnect, and CURVE/ZAP support.

`go-zeromq/zmq4` remains a possible later plugin named `zeromq_gozeromq`, but it is not the first implementation. Its repository describes it as WIP and needing a maintainer; its current option layer does not provide equivalent `libzmq` backpressure controls, and its PLAIN server implementation does not perform real credential validation.

## Capability contract

| Pattern | Fujin role | Operations | Distribution | Produce guarantee | Headers |
|---|---|---|---|---|---|
| `pub` | writer | Produce | fan-out to matching SUB peers | `local_accept` | only with `fujin_v1` |
| `sub` | reader | Subscribe | every local Fujin subscriber receives every matching message | n/a | only with `fujin_v1` |
| `push` | writer | Produce | libzmq round-robin across PULL peers | `local_accept` | only with `fujin_v1` |
| `pull` | reader | Subscribe | one local Fujin subscriber receives each message, round-robin | n/a | only with `fujin_v1` |

All routes have:

- `Transactions=false`;
- `Fetch=false`;
- `ManualSettlement=false`;
- no message IDs, ACK, or NACK semantics.

`local_accept` means a successful send has been accepted by the local ZeroMQ socket queue. It does not mean a peer received, processed, or durably stored the message. PUB may successfully drop messages when no subscriber is ready or when a subscriber reaches its HWM.

## Multipart framing

### `raw`

- PUB/SUB: `[topic, payload]`
- PUSH/PULL: `[payload]`
- Any other frame count is malformed.
- Routes using `raw` declare `Headers=false`; HProduce and HSubscribe are rejected rather than silently losing headers.

### `fujin_v1`

The envelope preserves Fujin's canonical flat key/value header representation.

- PUB/SUB: `[topic, "fujin.v1", header_count_u16_be, header_0, ..., header_n, payload]`
- PUSH/PULL: `["fujin.v1", header_count_u16_be, header_0, ..., header_n, payload]`

Rules:

- `header_count` is the number of raw header strings, not the number of pairs; it must be even.
- Header keys must pass `connector.ValidateHeaders`; values remain arbitrary bytes.
- Empty payloads are valid.
- PUB/SUB topic remains frame zero so native ZeroMQ prefix subscriptions continue to work.
- The decoder requires exact magic, exact frame count, an even header count, and `max_message_bytes` compliance.
- Malformed or oversized messages are dropped, counted, and reported through rate-limited structured logs. They do not terminate the shared route.

For reader callbacks, SUB uses the topic frame as `source`. PULL uses the configured Fujin route name as `source`.

## Runtime topology

One generation-owned runtime creates one `zmq.Context` and one physical ZeroMQ socket actor per configured route. `NewReader` and `NewWriter` return lightweight session leases over those actors.

### Writer routes

- PUB and PUSH use one shared socket per route.
- Socket access is serialized because ZeroMQ sockets are not thread-safe.
- Produce invokes its callback exactly once after `SendMessage` returns.
- `Flush` is a mutex/barrier over every send accepted before the call; it does not claim peer delivery.
- `SNDTIMEO` bounds blocking send calls. Context cancellation is checked before the call and reported after a timed-out call.
- Writer leases do not close the physical socket; runtime retirement does.

### Reader routes

- SUB uses one shared socket and broadcasts each decoded message into bounded per-session queues.
- PULL uses one shared socket and assigns each decoded message to exactly one local subscriber in round-robin order.
- The actor pauses receiving when there are no local subscribers, allowing libzmq HWM/backpressure behavior to apply upstream.
- A subscriber whose local queue remains full is detached and its Subscribe call terminates with an explicit slow-consumer error. One slow client never blocks the route or causes silent local loss for other subscribers.
- The actor uses `zmq.Poller` with a bounded poll interval so cancellation and runtime retirement settle promptly.
- `ready` is invoked exactly once after the lease is registered and the actor reaches its strongest configured readiness boundary. Bind actors require `EVENT_LISTENING`; connect-side readers wait for `EVENT_HANDSHAKE_SUCCEEDED` up to `ready_timeout`.

### Shutdown and reconnect

- Default `linger` is `0s`; shutdown must not wait forever on libzmq queues.
- Connect routes use bounded reconnect intervals managed by libzmq.
- Runtime close stops new leases, terminates reader subscriptions, closes route sockets, stops the context-scoped ZAP actor, and terminates the ZeroMQ context within the catalog cleanup deadline.
- Existing sessions remain pinned to their generation during hot reload.

## Eager runtime preflight and generation sharing

Bind routes cannot be opened lazily after publication: a new generation could publish successfully and then fail its first BIND because the old generation still owns the endpoint. Add a generic connector-core seam rather than a ZeroMQ global registry.

Required catalog behavior:

1. Add an optional compiled-connector marker such as:

   ```go
   type EagerRuntimeCompiled interface {
       Compiled
       OpenRuntimeEagerly() bool
   }
   ```

2. Before generation publication, eagerly call `OpenRuntime` for connectors requesting preflight and retain the opened runtime in the candidate generation.
3. Store runtimes behind a reference-counted runtime owner that may be shared by multiple generations.
4. When connector type and immutable settings are unchanged, reuse the active generation's runtime owner. Middleware chains remain generation-owned.
5. A changed bind route on a different endpoint must successfully open before publication; failure rejects the snapshot and preserves the current generation.
6. A changed bind route that still targets an endpoint owned by the active generation is not atomically replaceable without violating generation pinning. Reject it with an explicit drain-required result. The supported cutover is two snapshots: remove the route, wait until status reports the old generation retired, then add the changed route. This intentionally introduces a maintenance gap instead of mutating an old generation or publishing a broken candidate.
7. On candidate-generation failure, close every newly opened runtime and release every reused runtime reference.
8. Close the physical runtime only after the final referencing generation retires.

This seam prevents unchanged bind connectors from being unnecessarily restarted when an unrelated connector changes. Connect routes and bind routes moving to a new endpoint remain zero-downtime; changing socket, framing, or CURVE settings on the same bind endpoint requires the explicit drain sequence above.

## CURVE security

Security is configured per route. `null` is allowed for trusted development networks; `curve` supports both roles:

- `mode: connect` is the CURVE client. It requires the client's public key, an immutable secret-key path, and the server public key.
- `mode: bind` is the CURVE server. It requires the server public key, an immutable secret-key path, and a non-empty allowlist of client public keys.

Do not use `pebbe/zmq4`'s process-global `AuthStart` maps: dynamic generation updates would mutate package-global unsynchronized policy state. Each generation-owned ZeroMQ context instead owns a Fujin ZAP REP actor on its context-local `inproc://zeromq.zap.01` endpoint.

The ZAP actor:

- receives immutable `domain -> allowed client keys` policy compiled for that generation;
- uses a route-specific ZAP domain;
- validates CURVE client keys without package-global mutation;
- emits no secret material in logs or status;
- starts before any CURVE server socket and stops before context termination.

Secret keys are loaded from files during eager runtime preflight, never during side-effect-free config compilation. Secret files are immutable for the lifetime of a generation; rotation uses a new path or secret identity so settings change and a new runtime is opened.

## Configuration shape

```yaml
connectors:
  events:
    type: zeromq_pebbe
    settings:
      common:
        io_threads: 1
        send_hwm: 1000
        receive_hwm: 1000
        send_timeout: 5s
        ready_timeout: 10s
        receive_poll_interval: 100ms
        reconnect_interval: 100ms
        reconnect_interval_max: 5s
        linger: 0s
        max_message_bytes: 4194304
        subscriber_queue_capacity: 256
      routes:
        events_out:
          pattern: pub
          endpoint: tcp://events.example:5555
          mode: connect
          framing: fujin_v1
          topic: events.
          security:
            mechanism: curve
            public_key: CLIENT_PUBLIC_Z85
            secret_key_path: /run/secrets/zmq-client-secret
            server_public_key: SERVER_PUBLIC_Z85
        events_in:
          pattern: sub
          endpoint: tcp://*:5556
          mode: bind
          framing: fujin_v1
          subscriptions: [events.]
          security:
            mechanism: curve
            public_key: SERVER_PUBLIC_Z85
            secret_key_path: /run/secrets/zmq-server-secret
            allowed_client_public_keys:
              - CLIENT_PUBLIC_Z85
```

Validation is side-effect-free and rejects:

- unsupported endpoint transports in the first release; support `tcp://` and `ipc://` only;
- missing endpoint or pattern;
- PUB without a non-empty UTF-8 topic;
- `subscriptions` on non-SUB routes or `topic` on non-PUB routes;
- invalid framing or ownership modes;
- non-positive HWM, size, timeout, or queue bounds where zero would mean unbounded behavior;
- malformed Z85 public keys;
- CURVE client settings without a server key;
- CURVE server settings without an explicit client allowlist;
- secret key values embedded directly in connector settings.

## Package and build layout

```text
public/plugins/connector/zeromq/pebbe/
  config.go
  config_converter.go
  framing.go
  runtime.go
  route.go
  reader.go
  writer.go
  zap.go
  init.go
  disabled.go
  README.md
```

- Implementation files use `//go:build zeromq_pebbe && cgo`.
- The disabled package keeps ordinary source discovery/builds valid without registering the connector.
- The custom builder example uses `-connector .../zeromq/pebbe -tags zeromq_pebbe -cgo`.
- The default full binary remains CGO-free and does not import this plugin through `connector/all`.
- Add a dedicated Alpine build/runtime image with `zeromq-dev` and `libsodium-dev` at build time and dynamic `libzmq`/`libsodium` packages at runtime; the scratch image cannot host this plugin.

## Interoperability and verification

Use an independent Python `pyzmq` fixture rather than a second Fujin adapter instance.

Required coverage:

- Fujin PUB -> pyzmq SUB and pyzmq PUB -> Fujin SUB.
- Fujin PUSH -> multiple pyzmq PULL peers and pyzmq PUSH -> multiple local Fujin PULL subscribers.
- Both bind and connect ownership for every pattern direction.
- Raw and `fujin_v1` framing, empty payloads, topic prefix filters, duplicate headers, and 1 MiB payloads.
- Malformed magic/count/frame layouts and oversized messages are dropped and reported without killing the route.
- A slow SUB consumer is detached while another subscriber continues receiving.
- CURVE success, wrong server key, unauthorized client key, and secret-file load failure.
- Peer restart and connect-side reconnect without replacing the Fujin generation.
- Unchanged bind runtime reuse across an unrelated connector reload.
- Same-endpoint bind changes are rejected with drain-required status; remove -> retire -> re-add succeeds.
- Writer contract tests for exactly-once callbacks, Flush snapshot barriers, send timeout, and Close.
- Race/leak tests for concurrent leases, actor shutdown, ZAP shutdown, and generation overlap.
- Benchmarks for PUB/SUB and PUSH/PULL with raw and `fujin_v1` framing at 128 B, 32 KiB, and 1 MiB.

Add real `build-zeromq-pebbe`, `test-zeromq-pebbe`, `up-zeromq-pebbe`, and `down-zeromq-pebbe` Make targets. Remove the current help-only generic `up-zeromq`/`down-zeromq` entries.

## Implementation order

1. **Catalog seam:** eager runtime preflight, shared runtime owners, rollback, unchanged-config reuse, and drain-required bind conflicts. Prove it first with fake runtimes; do not couple catalog tests to ZeroMQ.
2. **Plugin skeleton:** CGO/build tags, config conversion and validation, exact route profiles, framing codec, and NULL-security pyzmq smoke tests.
3. **Socket actors:** shared PUB/PUSH writers, shared SUB/PULL readers, bounded subscriber queues, slow-consumer detach, polling cancellation, reconnect, and bounded shutdown.
4. **CURVE:** context-scoped ZAP actor, client/server key setup, immutable allowlists, negative authentication tests, and secret-file handling.
5. **Operational finish:** reload/drain tests, dedicated Docker image, Make/builder integration, CI, README, E2E matrix, and focused benchmarks.

Each slice must leave the repository buildable. The default non-CGO build and existing connector matrix stay green after every slice.

## Acceptance criteria

- [x] `zeromq_pebbe` is opt-in, CGO-gated, and absent from default `connector/all` builds.
- [x] Config compilation is side-effect-free and publishes exact route capability profiles.
- [x] The generic eager-runtime/shared-runtime catalog seam preserves last-known-good publication for bind routes.
- [x] PUB/SUB and PUSH/PULL work in bind and connect modes with canonical shared-route socket semantics.
- [x] Raw framing rejects header operations; `fujin_v1` round-trips all valid Fujin headers losslessly.
- [x] Slow local subscribers are explicitly detached without blocking or silently dropping for other subscribers.
- [x] Full context-scoped CURVE/ZAP authentication works without process-global mutable auth policy.
- [x] Runtime reload, reconnect, generation drain, and shutdown have deterministic bounded behavior.
- [x] Independent pyzmq interoperability, contract, race/leak, E2E, and benchmark suites pass.
- [x] Plugin README, root plugin reference, builder examples, Make targets, Docker fixture, and CI job describe the exact `zeromq_pebbe` requirements and semantics.

## Non-goals

- `zeromq_gozeromq` implementation.
- REQ/REP, DEALER/ROUTER, XPUB/XSUB, RADIO/DISH, or draft socket APIs.
- Fetch buffering or batching.
- Manual settlement, durability, replay, or transactions.
- Treating ZeroMQ as a standalone broker service.
