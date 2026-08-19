# Fujin protocol

This document specifies Fujin native protocol v1. It is an incremental, length-prefixed binary protocol used over TCP, QUIC, WebSocket, and Unix domain sockets. The gRPC API has a separate protobuf wire format but delegates the same session semantics to Session Core.

The server parser consumes an arbitrary byte stream without command delimiters and uses pooled buffers on protocol hot paths.

## Protocol conventions

- **Byte order:** all integers use big-endian byte order.
- **Framing:** an opcode determines the remaining fields. There is no delimiter between commands.
- **Correlation IDs:** request/response operations use a client-selected `uint32` unless their section says otherwise.
- **Booleans:** exactly one byte: `0` is false and `1` is true. Other values are malformed.
- **Counts:** a collection count is the number of following elements, not its byte size.
- **Lengths:** a length prefixes the raw bytes immediately following it.

## Type system

| Type | Wire encoding | Example |
| --- | --- | --- |
| `byte` | 1 byte | `[1]` |
| `uint16` | 2 bytes | `[0, 1]` |
| `uint32` | 4 bytes | `[0, 0, 0, 1]` |
| `bool` | 1 byte, `0` or `1` | `[0]` |
| `[uint16]T` | `uint16` element count followed by the encoded elements | `[0, 1, ...]` |
| `[uint32]T` | `uint32` element count followed by the encoded elements | `[0, 0, 0, 1, ...]` |
| `bytes` | `uint32` byte length followed by raw bytes | `[0, 0, 0, 2, 104, 105]` |
| `string` | the same encoding as `bytes`; semantic UTF-8 requirements are stated per field | `[0, 0, 0, 2, 104, 105]` |

### Error encoding

Every operation response starts its result with a `status: byte`. Status values match the canonical gRPC status codes:

- `0 OK`
- `1 CANCELED`
- `2 UNKNOWN`
- `3 INVALID_ARGUMENT`
- `4 DEADLINE_EXCEEDED`
- `5 NOT_FOUND`
- `6 ALREADY_EXISTS`
- `7 PERMISSION_DENIED`
- `8 RESOURCE_EXHAUSTED`
- `9 FAILED_PRECONDITION`
- `10 ABORTED`
- `11 OUT_OF_RANGE`
- `12 UNIMPLEMENTED`
- `13 INTERNAL`
- `14 UNAVAILABLE`
- `15 DATA_LOSS`
- `16 UNAUTHENTICATED`

`0` is success and is followed immediately by the operation's success fields. A nonzero status is followed by:

`[<outcome: byte>, <reason: string>, <message: string>, <detail count: uint16>, <details>]`

Each detail is `<key: string>, <value: string>`. Detail keys are encoded in lexical order. Outcome values are `0 unspecified`, `1 not_applied`, `2 applied`, and `3 unknown`.

`reason` is a stable Fujin identifier suitable for programmatic handling. `message` is the human-readable explanation. Status gives the general failure class; outcome states whether a state-changing operation took effect. Retry policy is not encoded because it also depends on operation idempotency and outcome.

Fields documented after `<status>` are present only when status is `0` unless explicitly stated otherwise. Per-message ACK/NACK results use the same status and error envelope independently for each message ID.
### Message encodings

Delivery mode determines whether a client-settleable message ID is present:

- auto-settle message: `<payload: bytes>`
- manual-settlement message: `<message id: bytes>, <payload: bytes>`
- headered message: `<headers>, [<message id: bytes>], <payload: bytes>`

`headers` is a `uint16` count of raw byte strings followed by that many `bytes` fields. The count is the total number of alternating key and value strings and therefore must be even.

## Messaging terms

- `route` is the stable Fujin configuration name selected by a client command.
- `destination` is a connector-specific broker address configured under that route.
- `filter` is a connector-specific selector that may cover multiple destinations.
- `source` is the broker-specific origin supplied internally by an adapter. Native v1 does not expose it as a separate response field; in manual-settlement mode an adapter may include it inside the opaque message ID.
- Headers form an unordered multimap. Keys are non-empty UTF-8 bytes, values are arbitrary bytes, duplicate keys remain distinct, pair order has no meaning, and an empty collection means no headers.

## Route capabilities and guarantees

Each compiled connector route has an immutable capability profile pinned by `BIND`. Session Core validates the profile before opening a reader or writer. Unsupported operations return an explicit error rather than degrading silently.

The profile can advertise `produce`, lossless `headers`, `transactions`, `subscribe`, `fetch`, and manual settlement. A producer route also declares the strongest condition observed before a successful response:

- `local_accept` — accepted by local connector or socket buffering;
- `peer_accept` — positively acknowledged by a remote broker or peer;
- `durable_accept` — positively acknowledged as durably stored under the configured policy.

Manual settlement separately declares ACK granularity (`single` or `cumulative`) and NACK effect (`requeue`, `release`, `drop`, or unsupported).

Both native v1 and gRPC return the pinned profile on a successful `BIND`, so clients can reject unsupported workflows before issuing an operation. The profile is descriptive: broker availability and remote authentication are still established lazily by the first operation that opens a broker resource.

## Transports

The command and response frames below are identical on every native transport:

- **QUIC:** ALPN value `fujin` identifies the application protocol. One bidirectional QUIC stream carries one Fujin session. Every session stream starts with HELLO. A connection-level health probe uses a dedicated server-opened stream and does not perform HELLO. If `ping_stream` is enabled, the server additionally emits in-band PING frames after HELLO on each messaging stream.
- **TCP:** one connection carries one Fujin session and starts with HELLO. Fujin-level PING emission is not currently enabled; the transport enables TCP keepalive.
- **WebSocket:** one WebSocket connection carries one Fujin session and starts with HELLO. Binary WebSocket message payloads are concatenated into the protocol byte stream; text messages are rejected.
- **Unix:** one Unix domain socket connection carries one Fujin session and starts with HELLO. Fujin-level PING emission is not currently enabled.

## Versioning

`fujin` is the version-independent native application protocol identifier. QUIC carries it through ALPN; wire compatibility is negotiated by the mandatory HELLO exchange on every native session stream.

The current wire version is byte `1`, displayed as `fujin/1`. Once published, a wire version's opcodes, field encodings, and semantics are immutable. An incompatible wire or semantic change requires a new byte value advertised through HELLO. The server build version and client SDK build version are diagnostic metadata and never determine wire compatibility.

There is no legacy BIND-first mode. A native stream that does not begin with HELLO is malformed and is closed.

## HELLO

### Direction

Client -> Server

### Description

HELLO is the mandatory first frame on every native session stream. It selects one exact wire version before authentication, connector selection, or Session Core state exists. The client includes its SDK identity and build version for diagnostics. The server returns its executable build version for diagnostics.

The current HELLO format is `1`. Clients may advertise 1 to 16 nonzero protocol-version bytes in preference order. Client name and client build are non-empty strings limited to 256 bytes. The server selects the first advertised version it supports. Fujin currently supports wire version `1` (`fujin/1`).

### Request

`[0, <hello format: byte>, <version count: byte>, <versions: [count]byte>, <client name: string>, <client build: string>]`

### Response

- success: `[19, 0, <hello format: byte>, <selected version: byte>, <server build: string>]`
- failure: `[19, <status>, <error envelope after status>]`

An unsupported HELLO format or wire version returns `UNIMPLEMENTED`. A malformed HELLO returns `INVALID_ARGUMENT`. After either failure the server closes the stream. A successful HELLO must be followed by BIND.

### Example

- Client `fujin-go` built as `v0.4.1` advertising wire version `1`: `[0, 1, 1, 1, 0, 0, 0, 8, 102, 117, 106, 105, 110, 45, 103, 111, 0, 0, 0, 6, 118, 48, 46, 52, 46, 49]`.
- Server built as `v0.4.1`: `[19, 0, 1, 1, 0, 0, 0, 6, 118, 48, 46, 52, 46, 49]`.

## PING / PONG

### Direction

Server -> Client -> Server

### Description

When a transport emits PING, the client must reply with PONG on the same stream. Both frames are the single byte `99`.

QUIC always performs its connection-level probe on a dedicated stream. Failure to receive a valid one-byte PONG within the configured timeout counts as a failed attempt; after the configured retry limit the connection is closed. Optional QUIC `ping_stream` probing uses the same frame in-band on each messaging stream and closes an unresponsive stream.

### Syntax

Request: `[99]`

Response: `[99]`

### Example

- `[99]` -> `[99]`

## BIND

### Direction

Client -> Server

### Description

`BIND` selects one connector configuration and pins an immutable, locally validated configuration generation to the session. It runs bind middleware and may apply whitelisted configuration overrides. Success proves that the plugin is compiled in and that its settings, routes, capabilities, and guarantees are locally valid. It performs no broker I/O and does not prove broker availability or remote authentication.

Clients must successfully complete HELLO and BIND before issuing session operations. PONG may be sent after HELLO and before BIND when the transport has emitted an in-band PING.

### Request

`[1, <connector name: string>, <meta pair count: uint16>, <meta pairs>, <override pair count: uint16>, <override pairs>]`

Each pair is encoded as `<key: string>, <value: string>`. The pair count counts key/value pairs, not individual strings.

- `connector name` is the configured connector instance name, not its plugin type.
- `meta` is consumed by bind middleware.
- override keys are configuration paths such as `routes.orders.transactional_id` and must be permitted by the connector's `overridable` list.

### Response

- success: `[16, 0, <route count: uint32>, <route profiles>]`
- failure: `[16, <status>, <error envelope after status>]`

Each successful route profile is:

`[<route: string>, <capability flags: byte>, <produce guarantee: byte>, <ACK granularity: byte>, <NACK effect: byte>]`

Capability flag bits are `0x01 produce`, `0x02 headers`, `0x04 transactions`, `0x08 subscribe`, `0x10 fetch`, and `0x20 manual settlement`. Guarantee values are `0 unspecified`, `1 local_accept`, `2 peer_accept`, and `3 durable_accept`. ACK values are `0 unsupported`, `1 single`, and `2 cumulative`. NACK values are `0 unsupported`, `1 requeue`, `2 release`, and `3 drop`.

Routes are encoded in lexical order for deterministic framing. BIND has no correlation ID. A failure response has no route profiles.

### Example

- A successful connector with one `pub` route supporting produce and headers with `local_accept` returns `[16, 0, 0, 0, 0, 1, 0, 0, 0, 3, 112, 117, 98, 3, 1, 0, 0]`.

## PRODUCE

### Direction

Client -> Server

### Description

Sends one non-transactional message through a configured Fujin route. The route must advertise `produce`; success means the route's declared acceptance guarantee was reached. The payload length must be greater than zero. `PRODUCE` is rejected while a transaction is active.

### Request

`[2, <correlation id: uint32>, <route: string>, <payload: bytes>]`

### Response

- success: `[3, <correlation id>, 0]`
- failure: `[3, <correlation id>, <status>, <error envelope after status>]`

## HPRODUCE

### Direction

Client -> Server

### Description

Header-aware form of `PRODUCE`. The route must advertise both `produce` and lossless `headers`. Headers use the canonical alternating key/value representation. The payload length must be greater than zero, and the operation is rejected while a transaction is active.

### Request

`[3, <correlation id: uint32>, <route: string>, <headers>, <payload: bytes>]`

### Response

- success: `[4, <correlation id>, 0]`
- failure: `[4, <correlation id>, <status>, <error envelope after status>]`

## BEGIN TX

### Direction

Client -> Server

### Description

Flushes ordinary session writers, verifies that the route advertises transactions, acquires its writer, and successfully invokes the connector's transaction begin operation before returning success. A successful BEGIN therefore represents a concrete broker transaction, including an empty transaction with no subsequent produce. The transaction is restricted to the selected route.

### Request

`[4, <correlation id: uint32>, <route: string>]`

### Response

- success: `[5, <correlation id>, 0]`
- failure: `[5, <correlation id>, <status>, <error envelope after status>]`

## COMMIT TX

### Direction

Client -> Server

### Description

Flushes operations accepted by the transaction writer and then commits the active transaction. The local transaction always ends. A flush failure triggers best-effort rollback; a commit failure is reported as an unknown remote outcome. Any writer involved in a terminal error is closed and not returned to the session pool.

### Request

`[5, <correlation id: uint32>]`

### Response

- success: `[6, <correlation id>, 0]`
- failure: `[6, <correlation id>, <status>, <error envelope after status>]`

## ROLLBACK TX

### Direction

Client -> Server

### Description

Rolls back the active transaction. The local transaction always ends. A writer whose rollback fails is closed and not reused.

### Request

`[6, <correlation id: uint32>]`

### Response

- success: `[7, <correlation id>, 0]`
- failure: `[7, <correlation id>, <status>, <error envelope after status>]`

## TX_PRODUCE

### Direction

Client -> Server

### Description

Sends a non-empty payload through the route selected by BEGIN. The request carries no route and cannot switch transaction destinations.

### Request

`[15, <correlation id: uint32>, <payload: bytes>]`

### Response

- success: `[17, <correlation id>, 0]`
- failure: `[17, <correlation id>, <status>, <error envelope after status>]`

## TX_HPRODUCE

### Direction

Client -> Server

### Description

Header-aware form of TX_PRODUCE. The transaction route must advertise lossless header support.

### Request

`[16, <correlation id: uint32>, <headers>, <payload: bytes>]`

### Response

- success: `[18, <correlation id>, 0]`
- failure: `[18, <correlation id>, <status>, <error envelope after status>]`

Normal PRODUCE and HPRODUCE are not transaction message commands. During an active transaction, clients must use TX_PRODUCE or TX_HPRODUCE.

## SUBSCRIBE / HSUBSCRIBE

### Direction

Client -> Server

### Description

Creates a push reader for a configured route. SUBSCRIBE requires the `subscribe` capability; HSUBSCRIBE additionally requires lossless `headers`. Manual mode also requires manual-settlement capability.

The successful response is emitted only after the adapter reaches its strongest observable readiness boundary and before the first delivered message. Session Core invokes the receive lifecycle once. If it fails before readiness, the request fails. If it terminates after readiness, the native v1 stream is closed because v1 has no asynchronous subscription-terminal-error frame.

The wire field remains named `auto commit` for compatibility:

- `0` — manual settlement; delivered messages contain client-settleable IDs.
- `1` — auto-settle; delivered messages contain no IDs and later ACK/NACK is invalid. Delivery is at-most-once relative to the Fujin client after the adapter hands the message to Session Core.

### Requests

- SUBSCRIBE: `[11, <correlation id: uint32>, <auto commit: bool>, <route: string>]`
- HSUBSCRIBE: `[12, <correlation id: uint32>, <auto commit: bool>, <route: string>]`

### Responses

- SUBSCRIBE success: `[1, <correlation id>, 0, <subscription id: byte>]`
- HSUBSCRIBE success: `[2, <correlation id>, 0, <subscription id: byte>]`
- SUBSCRIBE failure: `[1, <correlation id>, <status>, <error envelope after status>]`
- HSUBSCRIBE failure: `[2, <correlation id>, <status>, <error envelope after status>]`

## MSG

### Direction

Server -> Client

### Description

Delivers one non-headered subscription message. The subscription mode established by SUBSCRIBE determines the frame layout.

### Frames

- auto-settle: `[8, <subscription id: byte>, <payload: bytes>]`
- manual settlement: `[8, <subscription id: byte>, <message id: bytes>, <payload: bytes>]`

The message ID is opaque to clients and must be returned unchanged to ACK or NACK.

## HMSG

### Direction

Server -> Client

### Description

Header-aware subscription delivery.

### Frames

- auto-settle: `[9, <subscription id: byte>, <headers>, <payload: bytes>]`
- manual settlement: `[9, <subscription id: byte>, <headers>, <message id: bytes>, <payload: bytes>]`

## UNSUBSCRIBE

### Direction

Client -> Server

### Description

Cancels and closes the reader identified by the subscription ID. Closing the reader invalidates every outstanding message ID from it. Repeating UNSUBSCRIBE for the same ID returns an error.

### Request

`[13, <correlation id: uint32>, <subscription id: byte>]`

### Response

- success: `[14, <correlation id>, 0]`
- failure: `[14, <correlation id>, <status>, <error envelope after status>]`

## ACK

### Direction

Client -> Server

### Description

Applies the route's declared acknowledgement granularity to a manual-settlement reader. ACK is invalid for an auto-settle reader. Message IDs are versioned Core envelopes scoped to one reader incarnation; malformed, stale, cross-reader, duplicated within the request, or already consumed IDs are rejected.

### Request

`[9, <correlation id: uint32>, <subscription id: byte>, <message id count: uint32>, <message ids>]`

Each message ID is a `bytes` field. A zero count is valid and produces an empty successful result set.

### Response

- top-level failure: `[12, <correlation id>, <status>, <error envelope after status>]`
- top-level success: `[12, <correlation id>, 0, <result count: uint32>, <results>]`

Each result is:

- success: `[<message id: bytes>, 0]`
- failure: `[<message id: bytes>, <status>, <error envelope after status>]`

Results are emitted once per requested ID after top-level success. A successful result consumes that message ID.

## NACK

### Direction

Client -> Server

### Description

Applies the route's declared NACK effect (`requeue`, `release`, or `drop`) to a manual-settlement reader. Unsupported or no-op behavior is a top-level error. NACK uses the same message-ID validation, request layout, and result layout as ACK, with response opcode `13`.

### Request

`[10, <correlation id: uint32>, <subscription id: byte>, <message id count: uint32>, <message ids>]`

### Response

- top-level failure: `[13, <correlation id>, <status>, <error envelope after status>]`
- top-level success: `[13, <correlation id>, 0, <result count: uint32>, <results>]`

## FETCH

### Direction

Client -> Server

### Description

Retrieves up to a strict positive maximum number of messages. Zero is an invalid batch size. A successful response may contain from zero through the requested maximum; zero is valid only after the connector's fetch or configured wait completes without messages.

The first FETCH for a `(route, auto commit, header mode)` tuple creates an implicit reader and assigns a subscription ID. Equivalent later requests reuse it. Only one FETCH may be active on that reader; contention returns an explicit error. FETCH and HFETCH use separate implicit readers.

### Request

`[7, <correlation id: uint32>, <auto commit: bool>, <route: string>, <maximum messages: uint32>]`

### Response

- failure: `[10, <correlation id>, <status>, <error envelope after status>]`
- success: `[10, <correlation id>, 0, <subscription id: byte>, <message count: uint32>, <messages>]`

Each message uses the MSG body layout without its opcode and subscription ID:

- auto-settle: `<payload: bytes>`
- manual settlement: `<message id: bytes>, <payload: bytes>`

The returned subscription ID is stable for the implicit reader and is used by ACK, NACK, and UNSUBSCRIBE.

## HFETCH

### Direction

Client -> Server

### Description

Header-aware FETCH. The route must advertise both `fetch` and lossless `headers`.

### Request

`[8, <correlation id: uint32>, <auto commit: bool>, <route: string>, <maximum messages: uint32>]`

### Response

- failure: `[11, <correlation id>, <status>, <error envelope after status>]`
- success: `[11, <correlation id>, 0, <subscription id: byte>, <message count: uint32>, <messages>]`

Each message uses the HMSG body layout without its opcode and subscription ID:

- auto-settle: `<headers>, <payload: bytes>`
- manual settlement: `<headers>, <message id: bytes>, <payload: bytes>`

## DISCONNECT

### Direction

Client -> Server

### Description

After a successful BIND, the client should send DISCONNECT and wait for its response before closing the session stream. The server stops accepting commands, waits for active FETCH operations, performs bounded Session Core cleanup, then emits the response and closes the stream. On QUIC, each Fujin session stream disconnects independently.

### Frames

- request: `[14]`
- response: `[15]`

DISCONNECT has no correlation ID or error field. Cleanup failures are logged because the v1 response cannot carry them.

## STOP

### Direction

Server -> Client

### Description

During graceful server shutdown, the server sends STOP and gives the client the configured force-termination interval to disconnect. STOP has no client response frame; the client should initiate DISCONNECT. When the interval expires, the server closes the read side of the stream.

### Frame

`[98]`
