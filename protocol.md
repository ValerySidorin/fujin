# Fujin protocol

This document provides a brief description of the native Fujin protocol, used for communication between the Fujin server and client. It is a byte-based protocol that supports various patterns. The Fujin server operates as a QUIC server.

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
| Type       | Alias for                          |
| ---------- | ---------------------------------- |
| string     | [uint32]byte                       |
| message    | type{[uint32]byte??, [uint32]byte} |
| ackres     | type{[uint32]byte, bool}           |

## PING
### Direction
Server -> Client
### Description
`PING` and `PONG` implement a simple keep-alive mechanism between the client and server. Once a client establishes a connection to the Fujin server, the server continuously opens a QUIC stream and sends `PING` messages at a configurable interval. If the client fails to respond with a `PONG` message within the configured response interval, the server will terminate its connection. If a connection remains idle for too long, it will be closed.

Since the QUIC protocol supports multiplexing, `PING` messages are sent over a dedicated control streams, separated from messaging ones.
### Syntax
##### Request
`[11]`
##### Response
`[11]`
### Examples
- `[11]` -> `[11]`

## CONNECT

### Direction
Client -> Server
### Description
Before producing messages, the client must open a QUIC stream and send a `CONNECT` command to the server. When using Kafka, `stream id` is required for transactions and must not be equal to `[0, 0, 0, 0]`.
### Syntax
##### Request
 `[1, <stream id>]`  
 where:
 | name       | description                                                                    | type   |
| ----------- | ------------------------------------------------------------------------------ | ------ |
| `stream id` | A unique stream identifier used for transactional message production in Kafka. | uint32 |
##### Response
`-`
### Examples
- `[1, 0, 0, 0, 0]` -> `-`
- `[1, 0, 0, 0, 1]` -> `-`

## PRODUCE

### Direction
Client -> Server
### Description
Sends a message to the specified topic. This must be sent in the same QUIC stream where the `CONNECT` command was previously issued.
### Syntax
##### Request
`[3, <correlation id>, <topic>, <message>]`  
where:
| name             | description                                                               | type                      |
| ---------------- | ------------------------------------------------------------------------- | ------------------------- |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32                    |
| `topic`          | The target topic for the message.                                         | string                    |
| `message`        | The message content.                                                      | [uint32]byte              |
##### Response
`[2, <correlation id>, <error>]`  
where:
| name             | description                                                               | type   |
| ---------------- | ------------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32 |
| `error`     | An error.               | string?   | always   |
### Examples
- `[3, 0, 1, 1, 1, 0, 0, 0, 3, 112, 117, 98, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111]` -> `[2, 0, 1, 1, 1, 0]`

## HPRODUCE

### Direction
Client -> Server
### Description
Sends a message to the specified topic. This must be sent in the same QUIC stream where the `CONNECT` command was previously issued.
### Syntax
##### Request
`[3, <correlation id>, <topic>, <headers>, <message>]`  
where:
| name             | description                                                               | type                      |
| ---------------- | ------------------------------------------------------------------------- | ------------------------- |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32                    |
| `topic`          | The target topic for the message.                                         | string                    |
| `headers`        | Optional headers for the message.                                         | array (uint16) of strings |
| `message`        | The message content.                                                      | [uint32]byte              |
##### Response
`[2, <correlation id>, <error>]`  
where:
| name             | description                                                               | type   |
| ---------------- | ------------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32 |
| `error`     | An error.               | string?   | always   |
### Examples
- `[3, 0, 1, 1, 1, 0, 0, 0, 3, 112, 117, 98, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111]` -> `[2, 0, 1, 1, 1, 0]`


## BEGIN TX
### Direction
Client -> Server

### Description
Begins transaction. Must be send in a QUIC stream, where `CONNECT` command was sent previously. `stream id` arg is required on `CONNECT` when Kafka is used.

### Syntax
##### Request
`[4, <correlation id>]`  
where:
| name             | description                                                          | type   |
| ---------------- | -------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 |
##### Response
`[3, <correlation id>, <error>]`  
where:
| name             | description                                                               | type   |
| ---------------- | ------------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32 |
| `error`     | An error.               | string?   | always   |

### Examples
- `[4, 0, 0, 0, 1]` -> `[3, 0, 0, 0, 1, 0]`

## COMMIT TX
### Direction
Client -> Server

### Description
Commits transaction. Must be send in a QUIC stream, where `CONNECT` command was sent previously. `stream id` arg is required on `CONNECT` when Kafka is used.

### Syntax
##### Request
`[5, <correlation id>]`  
where:
| name             | description                                                          | type   |
| ---------------- | -------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 |
##### Response
`[4, <correlation id>, <error>]`  
where:
| name             | description                                                               | type    |
| ---------------- | ------------------------------------------------------------------------- | ------- |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32  |
| `error`          | An error.                                                                 | string? |

### Examples
- `[5, 0, 0, 0, 1]` -> `[4, 0, 0, 0, 1, 0]`

## ROLLBACK TX
### Direction
Client -> Server

### Description
Rolls back transaction. Must be send in a QUIC stream, where `CONNECT` command was sent previously. `stream id` arg is required on `CONNECT` when Kafka is used.

### Syntax
##### Request
`[6, <correlation id>]`  
where:
| name             | description                                                          | type   |
| ---------------- | -------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 |
##### Response
`[5, <correlation id>, <error>]`  
where:
| name             | description                                                               | type    |
| ---------------- | ------------------------------------------------------------------------- | ------- |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32  |
| `error`          | An error.                                                                 | string? |

### Examples
- `[6, 0, 0, 0, 1]` -> `[5, 0, 0, 0, 1, 0]`

## SUBSCRIBE

### Direction
Client -> Server
### Description
Client initiates a subscription to a topic. Messages will be sent by the server in a stream opened by the client previously. Message distribution is handled by the underlying broker.

### Syntax
##### Request
`[2, <correlation id>, <auto commit>, <topic>]`  
where:
| name             | description                                                          | type   |
| ---------------- | -------------------------------------------------------------------- | ------ |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 |
| `auto commit`    | Connect with auto commit.                                            | bool   |
| `topic`          | Topic to read from.                                                  | string |
##### Response
`[1, <correlation id>, <error>, <subscription id>]`  
where:
| name              | description                                                          |  type   |
| ----------------- | -------------------------------------------------------------------- | ------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32  |
| `error`           | An error.                                                            | string? |
| `subscription id` | Subscription ID.                                                     | byte    |

### Examples
- `[2, 0, 0, 0, 1, 1, 0, 0, 0, 3, 112, 117, 98]` -> `[1, 0, 0, 0, 1, 0, 5]`
## MSG

### Direction
Server -> Client
### Description
A message propagated by the server in a client-opened QUIC stream after issuing `SUBSCRIBE` command.
### Syntax
`[6, <subscription id>, <message>]`
where:
| name                  | description      | type           |
| --------------------- | ---------------- | -------------- |
| `subscription id`     | Subscription ID. | byte           |
| `message`             | Message.         | message        |
### Examples
- `-` -> `[6, 5, 0, 0, 0, 5, 104, 101, 108, 108, 111]`

## HMSG

### Direction
Server -> Client
### Description
A message with headers propagated by the server in a client-opened QUIC stream after issuing `SUBSCRIBE` command.
### Syntax
`[6, <subscription id>, <headers>, <message>]`  
where:
| name                  | description      | type           |
| --------------------- | ---------------- | -------------- |
| `subscription id`     | Subscription ID. | byte           |
| `headers`             | Message headers. | [uint16]string |
| `message`             | Message.         | message        |
### Examples
- `-` -> `[6, 5, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111]`


## ACK

### Direction
Client -> Server
### Description
If auto commit is disabled on the specified topic, the reader must `ACK` each message or message offset. `ACK` rules are dictated by the underlying broker.
### Syntax
##### Request
`[8, <correlation id>, <msg ids>]`  
where:
| name             | description                                                          | type                 |
| ---------------- | ---------------------------------------------------------------------| -------------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32               |
| `msg ids`        | Message ID batch.                                                    | [uint32][uint32]byte |
##### Response
`[8, <correlation id>, <error>, <ack results>]`  
where:
| name             | description                                                          | type           |
| ---------------- | -------------------------------------------------------------------- | -------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32         |
| `error`          | An error.                                                            | string?        |
| `ack results`    | An array of ack results. (Msg ID + success)                          | [uint32]ackres |

### Examples
- `[8, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[8, 0, 0, 0, 1, 0]`

## NACK

### Direction
Client -> Server
### Description
Works similarly to `ACK`.
### Syntax
##### Request
`[9, <correlation id>, <message ids>]`  
where:
| name              | description                                                          | type                 | presence |
| ----------------- | ---------------------------------------------------------------------| -------------------- | -------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32               | always   |
| `msg ids`         | Message ID batch.                                                    | [uint32][uint32]byte | always   |
##### Response
`[9, <correlation id>, <error>, <nack results>]`  
where:
| name             | description                                                          | type           |
| ---------------- | -------------------------------------------------------------------- | -------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32         |
| `error`          | An error.                                                            | string?        |
| `nack results`   | An array of nack results. (Msg ID + success)                         | [uint32]ackres |


### Examples
- `[9, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[9, 0, 0, 0, 1, 0]`

## FETCH

### Direction
Client -> Server
## Description
Client can send a `FETCH` command to the server to retrieve messages from the current stream. The server will respond with a `FETCH` reply containing a batch of messages. The behavior of batch retrieval depends on the underlying broker: some brokers will block until all messages are received (or at least one), while others may return immediately, even if the batch contains zero messages. Not all connectors implement `FETCH`. For those, who are not - subscriber pattern is a preferred way of reading messages.

## Syntax
##### Request
`[7, <correlation id>, <auto commit>, <msg response batch len>]`  
where:
| name                     | description                                                          | type    |
| ------------------------ | -------------------------------------------------------------------- | ------- |
| `correlation id`         | Correlation ID is used to match client request with server response. | uint32  |
| `msg response batch len` | The number of messages the server should send in response.           | uint32  |

##### Response
`[7, <correlation id>, <error>, <msgs>]`  
where:
| name             | description                                                          | type             |
| ---------------- | -------------------------------------------------------------------- | ---------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32           |
| `error`          | An error.                                                            | string?          |
| `msgs`           | Message batch.                                                       | [uint32]message  |

## HFETCH

### Direction
Client -> Server
## Description
`FETCH` with headers support.

## Syntax
##### Request
`[7, <correlation id>, <auto commit>, <msg response batch len>]`  
where:
| name                     | description                                                          | type    |
| ------------------------ | -------------------------------------------------------------------- | ------- |
| `correlation id`         | Correlation ID is used to match client request with server response. | uint32  |
| `msg response batch len` | The number of messages the server should send in response.           | uint32  |

##### Response
`[7, <correlation id>, <error>, <msgs>]`  
where:
| name             | description                                                          | type                                   |
| ---------------- | -------------------------------------------------------------------- | -------------------------------------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32                                 |
| `error`          | An error.                                                            | string?                                |
| `msgs`           | Message with headers batch.                                          | [uint32]type{[uint16]string, message}  |


### Examples
- `[7, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[7, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 5, 104, 101, 108, 108, 111]`
- `[7, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[7, 0, 0, 0, 1, 0, 0, 0, 0, 0]`
- `[7, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[7, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 42, 107, 97, 102, 107, 97, 58, 32, 112, 111, 108, 108, 32, 102, 101, 116, 99, 104, 101, 115, 58, 32, 91, 123, 32, 45, 49, 32, 99, 108, 105, 101, 110, 116, 32, 99, 108, 111, 115, 101, 100, 125, 93]` 
## DISCONNECT

### Direction
Client -> Server
### Description
The client should send `DISCONNECT` request to the server and receive response before closing QUIC streams and connection. `DISCONNECT` should be sent both in writer and reader streams. Server will close QUIC stream after receiving `DISCONNECT` response.
### Syntax
##### Request
`[10]`
##### Response
`[10]`
### Examples
- `[10]` -> `[10]`

## STOP

### Direction
Server -> Client
### Description
The server can sometimes send `STOP` command to the client, when trying to shutdown gracefully. If the client does not disconnect within the configured response interval, the server will terminate its connection.
### Syntax
##### Request
`[12]`
##### Response
`-`
### Examples
- `[12]` -> `-`