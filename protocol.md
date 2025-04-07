# Fujin protocol

This document provides a brief description of the native Fujin protocol, used for communication between the Fujin server and client. It is a byte-based publish/subscribe protocol. The Fujin server operates as a QUIC server.

The Fujin server implements a [zero allocation byte parser](https://youtu.be/ylRKac5kSOk?t=10m46s), inspired by the NATS server, ensuring high speed and efficiency.

## Protocol conventions

**Command as an Array of Bytes with Optional Content**: Each interaction between the client and server consists of a control (protocol) byte array, optionally followed by message content.  
**No Command Delimiters**: The Fujin server receives commands as a plain stream of bytes. Commands are parsed based on their structure.  
**Byte Order**: The Fujin server uses big-endian byte order.  

## Types

Before describing the commands, let's explore the data types used in the Fujin protocol.

| Type   | Length                | Example                                 | Representation              |
|--------|-----------------------|-----------------------------------------| --------------------------- |
| byte   | 1                     | `[12]`                                  | `0x0C`                      |
| uint32 | 4                     | `[0, 0, 1, 1]`                          | `3`                         |
| string | dynamic (len+payload) | `[0, 0, 0, 5, 104, 101, 108, 108, 111]` | `hello`                     |
| bool   | 1                     | `[0]`                                   | `false`                     |  
| dynamic| dynamic               | `[1, 1, 1, 1]`                          | `[1, 1, 1, 1]`              |

## PING
### Direction
Server -> Client
### Description
`PING` and `PONG` implement a simple keep-alive mechanism between the client and server. Once a client establishes a connection to the Fujin server, the server continuously opens a QUIC stream and sends `PING` messages at a configurable interval. If the client fails to respond with a `PONG` message within the configured response interval, the server will terminate its connection. If a connection remains idle for too long, it will be closed.

Since the QUIC protocol supports multiplexing, `PING` messages are sent over a dedicated control streams, separated from messaging ones.
### Syntax
##### Request
`[12]`
##### Response
`[12]`
### Examples
- `[12]` -> `[12]`

## CONNECT WRITER

### Direction
Client -> Server
### Description
Before producing messages, the client must open a QUIC stream and send a `CONNECT WRITER` command to the server. When using Kafka, `writer id` is required for transactions and must not be equal to `[0, 0, 0, 0]`.
### Syntax
##### Request
 `[1, <writer id>]`  
 where:
 | name       | description                                                                    | type   | required |
| ----------- | ------------------------------------------------------------------------------ | ------ | -------- |
| `writer id` | A unique writer identifier used for transactional message production in Kafka. | uint32 | true     |
##### Response
`-`
### Examples
- `[1, 0, 0, 0, 0]` -> `-`
- `[1, 0, 0, 0, 1]` -> `-`

## WRITE

### Direction
Client -> Server
### Description
Sends a message to the specified topic. This must be sent in the same QUIC stream where the `CONNECT WRITER` command was previously issued.
### Syntax
##### Request
`[4, <correlation id>, <topic>, <message>]`  
where:
| name             | description                                                               | type   | required |
| ---------------- | ------------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32 | true     |
| `topic`          | The target topic for the message.                                         | string | true     |
| `message`        | The message content.                                                      | string | true     |
##### Response
`[2, <correlation id>, <error code>, <error payload>]`  
where:
| name             | description                                                               | type   | presence |
| ---------------- | ------------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID used to match the client request with the server response. | uint32 | always   |
| `error code`     | Error code: `0` indicates no error, `1` indicates an error.               | byte   | always   |
| `error payload`  | Error message text, if applicable.                                        | string | optional |
### Examples
- `[4, 0, 1, 1, 1, 0, 0, 0, 3, 112, 117, 98, 0, 0, 0, 5, 104, 101, 108, 108, 111]` -> `[2, 0, 1, 1, 1, 0]`

## BEGIN TX
### Direction
Client -> Server

### Description
Begins transaction. Must be send in a QUIC stream, where `CONNECT WRITER` command was sent previously. `producer id` arg is required on `CONNECT WRITER` when Kafka is used.

### Syntax
##### Request
`[5, <correlation id>]`  
where:
| name             | description                                                          | type   | required |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | true     |
##### Response
`[6, <correlation id>]`  
where:
| name             | description                                                          | type   | presence |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | always   |

### Examples
- `[5, 0, 0, 0, 1]` -> `[6, 0, 0, 0, 1]`

## COMMIT TX
### Direction
Client -> Server

### Description
Commits transaction. Must be send in a QUIC stream, where `CONNECT WRITER` command was sent previously. `producer id` arg is required on `CONNECT WRITER` when Kafka is used.

### Syntax
##### Request
`[6, <correlation id>]`  
where:
| name             | description                                                          | type   | required |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | true     |
##### Response
`[7, <correlation id>]`  
where:
| name             | description                                                          | type   | presence |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | always   |

### Examples
- `[6, 0, 0, 0, 1]` -> `[7, 0, 0, 0, 1]`

## ROLLBACK TX
### Direction
Client -> Server

### Description
Rolls back transaction. Must be send in a QUIC stream, where `CONNECT WRITER` command was sent previously. `producer id` arg is required on `CONNECT WRITER` when Kafka is used.

### Syntax
##### Request
`[7, <correlation id>]`  
where:
| name             | description                                                          | type   | required |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | true     |
##### Response
`[8, <correlation id>]`  
where:
| name             | description                                                          | type   | presence |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | always   |

### Examples
- `[7, 0, 0, 0, 1]` -> `[8, 0, 0, 0, 1]`

## CONNECT READER

### Direction
Client -> Server
### Description
Client opens QUIC stream and initiates a subscription to a topic. Messages will be sent by the server in a stream opened by the client. Message distribution is handled by the underlying broker. There are two types of readers: subscriber (push) and consumer (pull). If the client connects as a subscriber, the server pushes messages into the connection itself. If the client connects as a consumer, the server sends messages on request.

### Syntax
##### Request
`[2, <type>, <topic>]`  
where:
| name    | description                                                     | type   | required |
| ------- | --------------------------------------------------------------- | ------ | -------- |
| `topic` | Topic to read from.                                             | string | true     |
| `type`  | Type of reader: `1` — subscriber (push), `2` — consumer (pull). | byte   | true     |
##### Response
`[1, <is auto commit>, <message meta length>, <error code>, <error payload>]`  
where:
| name                  | description                                          |  type  | presence |
| --------------------- | ---------------------------------------------------- | ------ | -------- |
| `is auto commit`      | Topic is configured as auto commit.                  | bool   | always   |
| `message meta length` | Message meta length. Used to pass in commit request. | byte   | always   |
| `error code`          | Error code. 0 is no error. 1 is error.               | byte   | always   |
| `error payload`       | Error payload text.                                  | string | optional |
### Examples
- `[2, 0, 0, 0, 3, 112, 117, 98, 0]` -> `[1, 0, 0, 0]`
## MSG

### Direction
Server -> Client
### Description
A message propagated by the server in a client-opened reader QUIC stream. Message meta length is returned from `CONNECT READER` response.
### Syntax
`[3, <message meta>, <message payload>]`  
where:
| name                  | description                                                     | type     | presence |
| --------------------- | --------------------------------------------------------------- | -------- | -------- |
| `message meta`        | Message meta. Propagated by server if auto commit is disabled.  | dynamic  | optional |
| `message payload`     | Message payload.                                                | string   | always   |
### Examples
- `-` -> `[3, 0, 0, 0, 5, 104, 101, 108, 108, 111]`

## ACK

### Direction
Client -> Server
### Description
Must be sent in a QUIC stream, where `CONNECT READER` command was sent previously.
If auto commit is disabled on the specified topic, the reader must `ACK` each message or message offset. `ACK` rules are dictated by the underlying broker.
### Syntax
##### Request
`[9, <correlation id>, <message meta>]`  
where:
| name                  | description                                                      | type    | required |
| ----------------- | ---------------------------------------------------------------------| ------- | -------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32  | always   |
| `message meta`    | Message meta.                                                        | dynamic | always   |
##### Response
`[4, <correlation id>, <error code>, <error payload>]`  
where:
| name               | description                                                          | type   | presence |
| ------------------ | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id`   | Correlation ID is used to match client request with server response. | uint32 | always   |
| `error code`       | Error code. 0 is no error. 1 is error.                               | byte   | always   |
| `error payload`    | Error payload text.                                                  | string | optional |

### Examples
- `[9, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[4, 0, 0, 0, 1, 0]`

## NACK

### Direction
Client -> Server
### Description
Works similarly to `ACK`.
### Syntax
##### Request
`[10, <correlation id>, <message meta>]`  
where:
| name                  | description                                                      | type    | required |
| ----------------- | ---------------------------------------------------------------------| ------- | -------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32  | always   |
| `message meta`    | Message meta.                                                        | dynamic | always   |
##### Response
`[5, <correlation id>, <error code>, <error payload>]`  
where:
| name                  | description                                                      | type    | presence |
| ----------------- | -------------------------------------------------------------------- | ------- | -------- |
| `correlation id`  | Correlation ID is used to match client request with server response. | uint32  | always   |
| `error code`      | Error code. 0 is no error. 1 is error.                               | byte    | always   |
| `error payload`   | Error payload text.                                                  | string  | optional |

### Examples
- `[10, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[5, 0, 0, 0, 1, 0]`

## FETCH

### Direction
Client -> Server
## Description
When connected as a consumer, the client must send a FETCH command to the server to retrieve messages from the current reader stream. The server will respond with a FETCH reply containing a batch of messages. The behavior of batch retrieval depends on the underlying broker: some brokers will block until all messages are received, while others may return immediately, even if the batch contains zero messages.

## Syntax
##### Request
`[8, <correlation id>, <num messages in batch>]`  
where:
| name                     | description                                                          | type    | required |
| ------------------------ | -------------------------------------------------------------------- | ------- | -------- |
| `correlation id`         | Correlation ID is used to match client request with server response. | uint32  | always   |
| `num messages in batch`  | The number of messages the server should send in response.           | uint32  | true     |

##### Response
`[6, <correlation id>, <num messages in batch>, <error code>, <error payload>, <batch of messages>]`  
where:
| name                     | description                                                          | type    | required |
| ------------------------ | -------------------------------------------------------------------- | ------- | -------- |
| `correlation id`         | Correlation ID is used to match client request with server response. | uint32  | always   |
| `num messages in batch`  | The number of messages the server should send in response.           | uint32  | true     |
| `error code`             | Error code. 0 is no error. 1 is error.                               | byte    | always   |
| `error payload`          | Error payload text.                                                  | string  | optional |
| `batch of messages`      | Just an array of `MSG`s.                                             | string  | optional |


### Examples
- `[8, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[6, 0, 0, 0, 1, 0, 0, 0, 1, 0, 3, 0, 0, 0, 5, 104, 101, 108, 108, 111]`
- `[8, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[6, 0, 0, 0, 1, 0, 0, 0, 0, 0]`
- `[8, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[6, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 42, 107, 97, 102, 107, 97, 58, 32, 112, 111, 108, 108, 32, 102, 101, 116, 99, 104, 101, 115, 58, 32, 91, 123, 32, 45, 49, 32, 99, 108, 105, 101, 110, 116, 32, 99, 108, 111, 115, 101, 100, 125, 93]` 
## DISCONNECT

### Direction
Client -> Server
### Description
The client should send `DISCONNECT` request to the server and receive response before closing QUIC streams and connection. `DISCONNECT` should be sent both in writer and reader streams. Server will close QUIC stream after receiving `DISCONNECT` response.
### Syntax
##### Request
`[11]`
##### Response
`[9]`
### Examples
- `[11]` -> `[9]`

## STOP

### Direction
Server -> Client
### Description
The server can sometimes send `STOP` command to the client, when trying to shutdown gracefully. If the client does not disconnect within the configured response interval, the server will terminate its connection.
### Syntax
##### Request
`[13]`
##### Response
`-`
### Examples
- `[13]` -> `-`