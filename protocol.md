# Fujin protocol

This document provides a brief description of the native Fujin protocol, used for communication between the Fujin server and client. It is a byte-based protocol that supports various patterns. The protocol layer is transport-agnostic — the same binary protocol runs over QUIC or TCP.

The Fujin server implements a [zero allocation byte parser](https://youtu.be/ylRKac5kSOk?t=10m46s), inspired by the NATS server, ensuring high speed and efficiency.

## Protocol conventions

**Command as an Array of Bytes with Optional Content**: Each interaction between the client and server consists of a control (protocol) byte array, optionally followed by message content.  
**No Command Delimiters**: The Fujin server receives commands as a plain stream of bytes. Commands are parsed based on their structure.  
**Byte Order**: The Fujin server uses big-endian byte order.  

## Type system

Before describing the commands, let's explore the data types used in the Fujin protocol.

| Type               | Length (bytes)               | Example                                    | Representation     |
|--------------------|------------------------------|--------------------------------------------| ------------------ |
| byte               | 1                            | `[1]`                                      | `1`                |
| uint16             | 2                            | `[0, 1]`                                   | `1`                |
| uint32             | 4                            | `[0, 0, 0, 1]`                             | `1`                |
| bool               | 1                            | `[0]`                                      | `false`            |
| [uint16]type       | dynamic (uint16 len+payload) | `[0, 1, 1]`                                | `[1]`              |
| [uint32]type       | dynamic (uint32 len+payload) | `[0, 0, 0, 1, 1]`                          | `[1]`              |
| string             | dynamic (uint32 len+payload) | `[0, 0, 0, 5, 104, 101, 108, 108, 111]`    | `"hello"`          |
| type{string, bool} | dynamic                      | `[0, 0, 0, 5, 104, 101, 108, 108, 111, 0]` | `{"hello", false}` |

* **Nullability**: If type is nullable, 1 byte is always prepended before (0 if null, 1 if not). For this doc, nullable types will be illustrated as followed: `string?`. In some cases value nullability is client defined, and we don't need to prepend 1 byte. Such values will be illustrated as followed: `string??`.

## Type aliases

For convenience, some type aliases are introduced.
| Type       | Alias for                                            |
| ---------- | ---------------------------------------------------- |
| string     | [uint32]byte                                         |
| message    | type{[uint32]byte??, string}                         |
| hmessage   | type{[uint32]byte??, string, [uint16]byte??, string} |
| ackres     | type{[uint32]byte, bool}                             |

## Transports

The Fujin protocol is transport-agnostic. The same byte stream runs over any supported transport:

- **QUIC** — Multiplexed streams over UDP with built-in TLS. Each command session runs on a separate QUIC stream. PING uses a dedicated control stream.
- **TCP** — Plain TCP (with optional TLS). One connection carries a single command session. PING is in-band on the same connection.
- **Unix** — Unix domain sockets. Same-machine only; uses filesystem path. PING is in-band.

## Versioning

Over QUIC, Fujin uses protocol versioning at the TLS layer via ALPN.

- Current protocol version: "fujin/1" (v1)
- The server only accepts connections with supported ALPN versions and rejects others.

Compatibility rules:
- All opcodes and formats below apply to v1 (ALPN "fujin/1").
- Future versions will use a new ALPN value (e.g., "fujin/2"). Clients may provide multiple values to negotiate the highest mutually supported version.
- Header semantics may evolve in future versions without changing the opcodes.

Over TCP, the protocol version is implicitly v1 (no ALPN negotiation).

## PING
### Direction
Server -> Client
### Description
`PING` and `PONG` implement a simple keep-alive mechanism between the client and server. Once a client establishes a connection, the server sends `PING` messages at a configurable interval. If the client fails to respond with a `PONG` message within the configured response interval, the server will terminate its connection. If a connection remains idle for too long, it will be closed.
Additionally, the server can be configured to ping opened streams. This helps to determine broken protocol writes, and close such streams.

**Transport-specific behavior:**
- **QUIC**: `PING` messages are sent over dedicated control streams, separated from messaging ones (QUIC supports multiplexing).
- **TCP**: `PING` is sent in-band on the same connection as other commands. TCP keepalive is also enabled at the transport level.
### Syntax
##### Request
`[99]`
##### Response
`[99]`
### Examples
- `[99]` -> `[99]`

## BIND

### Direction
Client -> Server
### Description
Before producing messages, the client must open a stream (QUIC stream or TCP connection) and send a `BIND` command to the server. This command binds the session to a connector and optionally applies configuration overrides to connector settings. The `BIND` command must be sent before any other commands except `PING`/`PONG`.

The `BIND` command includes:
- `connector_name`: the connector configuration name (for example, `kafka_connector`)
- `meta`: optional metadata key-value pairs consumed by bind middleware
- `config_overrides`: optional overrides using configuration paths such as `routes.orders.transactional_id`

### Syntax
##### Request
 `[1, <connector_name>, <meta>, <config_overrides>]`  
 where:
 | name              | description                                          | type           |
| ------------------ | ---------------------------------------------------- | -------------- |
| `connector_name`   | The name of the connector to bind to.                | string         |
| `meta`             | Optional metadata key-value pairs.                   | [uint16]string |
| `config_overrides` | Array of key-value pairs for configuration override. | [uint16]string |

Where `meta` and `config_overrides` are arrays of key-value pairs, each pair represented as:
- `[uint32]string` (key length + key)
- `[uint32]string` (value length + value)

##### Response
`[16, <error>]` 

### Examples
- `[1, 0, 0, 0, 14, 107, 97, 102, 107, 97, 95, 99, 111, 110, 110, 101, 99, 116, 111, 114, 0, 0, 0, 0]` -> `[16, 0]` (BIND with connector name "kafka_connector", no meta, no overrides)
- `[1, 0, 0, 0, 14, 107, 97, 102, 107, 97, 95, 99, 111, 110, 110, 101, 99, 116, 111, 114, 0, 0, 0, 1, 0, 0, 0, 7, 97, 112, 105, 95, 107, 101, 121, 0, 0, 0, 16, 109, 121, 45, 115, 101, 99, 114, 101, 116, 45, 107, 101, 121, 45, 49, 50, 51, 0, 0]` -> `[16, 0]` (BIND with connector name "kafka_connector", one meta pair: `api_key` = `my-secret-key-123`, no overrides)
- A BIND override path selects a Fujin route, for example `routes.pub.transactional_id`; the broker-native destination remains inside that route's connector settings.

## PRODUCE

### Direction
Client -> Server

### Description
Sends one non-transactional message through a configured Fujin route. `route` is a key under the bound connector's `settings.routes` map; it is not necessarily a broker topic. `PRODUCE` is rejected while a transaction is active.

### Syntax
##### Request
`[2, <correlation id>, <route>, <message>]`

| name             | description                                                               | type         |
| ---------------- | ------------------------------------------------------------------------- | ------------ |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32       |
| `route`          | Fujin route name resolved by the bound connector.                         | string       |
| `message`        | Message content.                                                          | [uint32]byte |

##### Response
`[3, <correlation id>, <error>]`

## HPRODUCE

### Direction
Client -> Server

### Description
Header-aware form of `PRODUCE`. It is also non-transactional and rejected while a transaction is active.

### Syntax
##### Request
`[3, <correlation id>, <route>, <headers>, <message>]`

| name             | description                                                               | type           |
| ---------------- | ------------------------------------------------------------------------- | -------------- |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32         |
| `route`          | Fujin route name resolved by the bound connector.                         | string         |
| `headers`        | Header key/value strings in connector order.                              | [uint16]string |
| `message`        | Message content.                                                          | [uint32]byte   |

##### Response
`[4, <correlation id>, <error>]`

## BEGIN TX

### Direction
Client -> Server

### Description
Eagerly acquires the writer for one route and invokes the connector's transaction begin operation before returning success. A transaction is restricted to this route. For Kafka, configure `transactional_id` in that route, optionally through a BIND override such as `routes.orders.transactional_id`.

### Syntax
##### Request
`[4, <correlation id>, <route>]`

| name             | description                                                               | type   |
| ---------------- | ------------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32 |
| `route`          | Route whose writer owns the transaction.                                  | string |

##### Response
`[5, <correlation id>, <error>]`

## COMMIT TX

### Direction
Client -> Server

### Description
Flushes and commits the active transaction, then returns its route writer to the connector pool.

### Syntax
##### Request
`[5, <correlation id>]`

##### Response
`[6, <correlation id>, <error>]`

## ROLLBACK TX

### Direction
Client -> Server

### Description
Rolls back the active transaction and returns its route writer to the connector pool.

### Syntax
##### Request
`[6, <correlation id>]`

##### Response
`[7, <correlation id>, <error>]`

## TX_PRODUCE

### Direction
Client -> Server

### Description
Sends a message through the route selected by `BEGIN TX`. The request does not carry a route, so a transaction cannot switch destinations mid-flight.

### Syntax
##### Request
`[15, <correlation id>, <message>]`

##### Response
`[17, <correlation id>, <error>]`

## TX_HPRODUCE

### Direction
Client -> Server

### Description
Header-aware form of `TX_PRODUCE`.

### Syntax
##### Request
`[16, <correlation id>, <headers>, <message>]`

##### Response
`[18, <correlation id>, <error>]`

Normal `PRODUCE`/`HPRODUCE` opcodes are not transaction message commands. Clients must use `TX_PRODUCE` or `TX_HPRODUCE` after `BEGIN TX`.
## SUBSCRIBE

### Direction
Client -> Server

### Description
Creates a push reader for a configured route. Messages are delivered on the same session stream. The route resolves to broker-specific reader settings such as Kafka topics, a NATS subject, or a RabbitMQ queue binding.

### Syntax
##### Request
`[11, <correlation id>, <auto commit>, <route>]`

| name              | description                                                          | type   |
| ----------------- | -------------------------------------------------------------------- | ------ |
| `correlation id`  | Correlation ID used to match client request with server response.    | uint32 |
| `auto commit`     | Whether the connector reader automatically commits deliveries.       | bool   |
| `route`           | Fujin route name resolved by the bound connector.                    | string |

##### Response
`[1, <correlation id>, <error>, <subscription id>]`

The `HSUBSCRIBE` request uses opcode `12`, the same route fields, and response opcode `2`; delivered messages use `HMSG`.
## MSG

### Direction
Server -> Client
### Description
A message propagated by the server on the client's stream after issuing `SUBSCRIBE` command.
### Syntax
`[8, <subscription id>, <message>]`
where:
| name                  | description      | type           |
| --------------------- | ---------------- | -------------- |
| `subscription id`     | Subscription ID. | byte           |
| `message`             | Message.         | message        |
### Examples
- `-` -> `[8, 5, 0, 0, 0, 5, 104, 101, 108, 108, 111]`

## HMSG

### Direction
Server -> Client
### Description
A message with headers propagated by the server on the client's stream after issuing `SUBSCRIBE` command.
### Syntax
`[9, <subscription id>, <hmessage>]`  
where:
| name                  | description       | type           |
| --------------------- | ----------------- | -------------- |
| `subscription id`     | Subscription ID.  | byte           |
| `message`             | Headered message. | hmessage       |
### Examples
- `-` -> `[9, 5, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111]`


## ACK

### Direction
Client -> Server
### Description
If auto commit is disabled for the selected route, the reader must `ACK` each message or message offset. `ACK` rules are dictated by the underlying broker.
### Syntax
##### Request
`[9, <correlation id>, <subscription id>, <msg ids>]`  
where:
| name             | description                                                          | type                 |
| ---------------- | ---------------------------------------------------------------------| -------------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32               |
| `subscription id`| Subscription ID to identify the subscription.                        | byte                 |
| `msg ids`        | Message ID batch.                                                    | [uint32][uint32]byte |
##### Response
`[12, <correlation id>, <error>, <ack results>]`  
where:
| name             | description                                                          | type           |
| ---------------- | -------------------------------------------------------------------- | -------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32         |
| `error`          | An error.                                                            | string?        |
| `ack results`    | An array of ack results. (Msg ID + success)                          | [uint32]ackres |

### Examples
- `[9, 0, 0, 0, 1, 1, 0, 0, 0, 1]` -> `[12, 0, 0, 0, 1, 0]`

## NACK

### Direction
Client -> Server
### Description
Works similarly to `ACK`.
### Syntax
##### Request
`[10, <correlation id>, <subscription id>, <message ids>]`  
where:
| name              | description                                                          | type                 | presence |
| ----------------- | ---------------------------------------------------------------------| -------------------- | -------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32               | always   |
| `subscription id` | Subscription ID to identify the subscription.                        | byte                 | always   |
| `msg ids`         | Message ID batch.                                                    | [uint32][uint32]byte | always   |
##### Response
`[13, <correlation id>, <error>, <nack results>]`  
where:
| name             | description                                                          | type           |
| ---------------- | -------------------------------------------------------------------- | -------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32         |
| `error`          | An error.                                                            | string?        |
| `nack results`   | An array of nack results. (Msg ID + success)                         | [uint32]ackres |


### Examples
- `[10, 0, 0, 0, 1, 1, 0, 0, 0, 1]` -> `[13, 0, 0, 0, 1, 0]`

## FETCH

### Direction
Client -> Server
## Description
Client can send a `FETCH` command to retrieve a batch through a configured route. The underlying broker determines whether fetch blocks, returns a partial batch, or returns zero messages. Not all connectors implement `FETCH`; push subscription is the alternative.

On the first `FETCH` request for a `(route, auto commit, header mode)` tuple, the server creates an implicit reader and assigns a `subscription_id`. Subsequent equivalent requests reuse that reader.

## Syntax
##### Request
`[7, <correlation id>, <auto commit>, <route>, <msg response batch len>]`

| name                     | description                                                          | type   |
| ------------------------ | -------------------------------------------------------------------- | ------ |
| `correlation id`         | Correlation ID used to match client request with server response.    | uint32 |
| `auto commit`            | Fetch with auto commit.                                              | bool   |
| `route`                  | Fujin route name resolved by the bound connector.                    | string |
| `msg response batch len` | Maximum number of messages requested.                                | uint32 |

##### Response
`[10, <correlation id>, <error>, <subscription_id>, <msgs>]`  
where:
| name              | description                                                          | type             |
| ----------------- | -------------------------------------------------------------------- | ---------------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32           |
| `error`           | An error.                                                            | string?          |
| `subscription_id` | Subscription ID for ACK/NACK operations (reused across fetches).    | byte             |
| `msgs`            | Message batch.                                                       | [uint32]message  |

## HFETCH

### Direction
Client -> Server
## Description
`FETCH` with header-aware message encoding.

The implicit reader cache is keyed by `(route, auto commit, header mode)`, so `FETCH` and `HFETCH` do not share a reader.

## Syntax
##### Request
`[8, <correlation id>, <auto commit>, <route>, <msg response batch len>]`

| name                     | description                                                          | type   |
| ------------------------ | -------------------------------------------------------------------- | ------ |
| `correlation id`         | Correlation ID used to match client request with server response.    | uint32 |
| `auto commit`            | Fetch with auto commit.                                              | bool   |
| `route`                  | Fujin route name resolved by the bound connector.                    | string |
| `msg response batch len` | Maximum number of messages requested.                                | uint32 |

##### Response
`[11, <correlation id>, <error>, <subscription_id>, <msgs>]`  
where:
| name              | description                                                          | type                                   |
| ----------------- | -------------------------------------------------------------------- | -------------------------------------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32                                 |
| `error`           | An error.                                                            | string?                                |
| `subscription_id` | Subscription ID for ACK/NACK operations (reused across fetches).    | byte                                   |
| `msgs`            | Message with headers batch.                                          | [uint32]type{[uint16]string, message}  |


### Examples
- `[8, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[11, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 5, 104, 101, 108, 108, 111]`
- `[8, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[11, 0, 0, 0, 1, 0, 0, 0, 0, 0]`
- `[8, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[11, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 42, 107, 97, 102, 107, 97, 58, 32, 112, 111, 108, 108, 32, 102, 101, 116, 99, 104, 101, 115, 58, 32, 91, 123, 32, 45, 49, 32, 99, 108, 105, 101, 110, 116, 32, 99, 108, 111, 115, 101, 100, 125, 93]` 
## DISCONNECT

### Direction
Client -> Server
### Description
The client should send `DISCONNECT` request to the server and receive response before closing the stream/connection. Over QUIC, `DISCONNECT` should be sent on each open stream; over TCP, on the connection. The server will close the stream after sending the `DISCONNECT` response.
### Syntax
##### Request
`[14]`
##### Response
`[15]`
### Examples
- `[14]` -> `[15]`

## STOP

### Direction
Server -> Client
### Description
The server can sometimes send `STOP` command to the client, when trying to shutdown gracefully. If the client does not disconnect within the configured response interval, the server will terminate its connection.
### Syntax
##### Request
`[98]`
##### Response
`-`
### Examples
- `[98]` -> `-`
