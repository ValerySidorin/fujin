# Fujin protocol

// TODO:
// Replace CONNECT SUBSCRIBER/CONNECT CONSUMER with CONNECT READER with reader type arg
// Add error to ACK/NACK
// Remove message meta from ACK/NACK response
// Add num messages arg to CONSUME command

Here you can find a brief description of native Fujin protocol, that is used to communicate between the Fujin server and client. It is a byte-based, publish/subscribe protocol. Fujin server is actually a QUIC server.

The Fujin server implements a [zero allocation byte parser](https://youtu.be/ylRKac5kSOk?t=10m46s), inspired by the NATS server, that is fast and efficient.

## Protocol conventions

**Command Array of Bytes with Optional Content**: Each interaction between the client and server consists of a control, or protocol, array of bytes followed, optionally by message content.  
**No Command Delimiters**: The Fujin server recieves Commands as a plain stream of bytes. Commands are splitted based on its structure.  
**Byte Order**: The Fujin server uses Big Endian byte order.  

## Types

Before we can move to Commands description, we should explore types, that are used by the Fujin protocol.

| Type   | Length                | Example                                 | Representation              |
|--------|-----------------------|-----------------------------------------| --------------------------- |
| byte   | 1                     | `[12]`                                  | `12`                        |
| uint32 | 4                     | `[0, 0, 0, 0]`                          | `0`                         |
| string | dynamic (len+payload) | `[0, 0, 0, 5, 104, 101, 108, 108, 111]` | `"hello"`                   |
| bool   | 1                     | `[0]`                                   | `true`                      |  
| dynamic| dynamic               | `[1, 1, 1, 1]`                          | `[1, 1, 1, 1]`              |

## PING
### Direction
Server -> Client
### Description
`PING` and `PONG` implement a simple keep-alive mechanism between client and server. Once a client establishes a connection to the Fujin server, the server will continuously open QUIC stream and send `PING` messages to the client at a configurable interval. If the client fails to respond with a `PONG` message within the configured response interval, the server will terminate its connection. If your connection stays idle for too long, it is cut off.

QUIC protocol is multiplexed by default, so a client will not receive `PING` messages in its messaging streams.
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
Before producing messages, the client must open a QUIC stream and send a `CONNECT WRITER` command to the server. When used with Kafka, `writer id` is required for transactions and must not be equal to `[0, 0, 0, 0]`.
### Syntax
##### Request
 `[1, <writer id>]`
 where:
 | name       | description                                                                   | type   | required |
| ----------- | ----------------------------------------------------------------------------- | ------ | -------- |
| `writer id` | A unique writer identifier, that is used on transactional producing to Kafka. | uint32 | true     |
##### Response
`-`
### Examples
- `[1, 0, 0, 0, 0]` -> `[]`
- `[1, 0, 0, 0, 1]` -> `[]`

## WRITE

### Direction
Client -> Server
### Description
Sends message to a desired topic. Must be send in a QUIC stream, where `CONNECT WRITER` command was sent previously.
### Syntax
##### Request
`[4, <correlation id>, <topic>, <message>]`
where:
| name             | description                                                          | type   | required |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | true     |
| `topic`          | Topic to write to.                                                   | string | true     |
| `message`        | Message to write.                                                    | string | true     |
##### Response
`[2, <correlation id>, <error code>, <error payload>]`
| name             | description                                                          | type   | presence |
| ---------------- | -------------------------------------------------------------------- | ------ | -------- |
| `correlation id` | Correlation ID is used to match client request with server response. | uint32 | always   |
| `error code`     | Error code. 0 is no error.                                           | byte   | always   |
| `error payload`  | Error payload text.                                                  | string | optional |
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
Rollbacks transaction. Must be send in a QUIC stream, where `CONNECT WRITER` command was sent previously. `producer id` arg is required on `CONNECT WRITER` when Kafka is used.

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
Client opens QUIC stream and initiates a subscription to a topic. Messages will be sent by the server in a stream, opened by client. Message distribution is being handled by the underlying broker. There are 2 types of readers: subscriber (push) and consumer (pull). If the client connects as a subscriber, the server pushes messages into the connection itself. If the client connects as a consumer, the server sends messages on request.

### Syntax
##### Request
`[2, <topic>, <type>]`
where:
| name    | description                                     | type   | required |
| ------- | ----------------------------------------------- | ------ | -------- |
| `topic` | Topic to read from.                             | string | true     |
| `type`  | Type of reader. 0 is subscriber, 1 is consumer. | byte   | true     |
##### Response
`[1, <is auto commit>, <message meta length>, <error code>, <error payload>]`
where:
| name                  | description                                          |  type  | presence |
| --------------------- | ---------------------------------------------------- | ------ | -------- |
| `is auto commit`      | Topic is configured as auto commit.                  | bool   | always   |
| `message meta length` | Message meta length. Used to pass in commit request. | byte   | always   |
| `error code`          | Error code. 0 is no error.                           | byte   | always   |
| `error payload`       | Error payload text.                                  | string | optional |
### Examples
- `[2, 0, 0, 0, 3, 112, 117, 98, 0]` -> `[1, 0, 0, 0]`
## MSG

### Direction
Server -> Client
### Description
A message. Propagated by the server in client-opened reader QUIC stream.
### Syntax
`[3, <message meta>, <message payload>]`
where:
| name                  | description                                                     | type     | presence |
| --------------------- | --------------------------------------------------------------- | -------- | -------- |
| `message meta`        | Message meta. Propagated by server if auto commit is disabled.  | dynamic  | optional |
| `message payload`     | Message payload.                                                | string   | always   |
### Examples
- `-` -> `[ 3, 0, 0, 0, 5, 104, 101, 108, 108, 111]`

## ACK

### Direction
Client -> Server
### Description
Must be send in a QUIC stream, where `CONNECT READER` command was sent previously.
If auto commit is disabled on the desired topic, reader must `ACK` each message or message offset. `ACK` rules are dictated by the underlying broker.
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
| `error code`       | Error code. 0 is no error.                                           | byte   | always   |
| `error payload`    | Error payload text.                                                  | string | optional |

### Examples
- `[9, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[4, 0, 0, 0, 1, 0]`

## NACK

### Direction
Client -> Server
### Description
Works similarly with `ACK`.
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
| `error code`      | Error code. 0 is no error.                                           | byte    | always   |
| `error payload`   | Error payload text.                                                  | string  | optional |

### Examples
- `[10, 0, 0, 0, 1, 0, 0, 0, 1]` -> `[5, 0, 0, 0, 1, 0]`

## FETCH

### Direction
Client -> Server
## Description
When connected as a consumer, the client should send to the server `FETCH` command to receive messages is current reader stream.

## Syntax
##### Request
`[8, <num messages in batch>]`
where:
| name                  | description                                                      | type    | required |
| ----------------- | -------------------------------------------------------------------- | ------- | -------- |
| `num messages in batch`  | A number of messages, the server will send to the client in stream. | uint32  | true   |

##### Response
`-`

### Examples
- `[8, 0, 0, 0, 1]` -> `-`

## DISCONNECT

### Direction
Client -> Server
### Description
The client should send `DISCONNECT` request to the server and receive response before closing QUIC streams and connection. `DISCONNECT` should be sent both in writer and reader streams.
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