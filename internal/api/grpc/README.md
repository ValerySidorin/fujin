# Fujin gRPC API

This directory contains the gRPC implementation for the Fujin message broker.

## Overview

The gRPC API provides a standard gRPC interface to the Fujin message broker, implementing the same async request-response pattern as the native protocol. It uses bidirectional streaming with correlation IDs for request-response matching.

## Architecture

The gRPC implementation mirrors the native Fujin protocol architecture:

- **Bidirectional Streaming**: Single `Stream` RPC for all operations
- **Correlation IDs**: Each request has a correlation ID for async response matching
- **Session-based**: Maintains writers and subscribers per session
- **Async Processing**: Non-blocking request handling with callback-based responses
- **Connect First**: Must send Connect request with streamId before other operations

## Build Tags

The gRPC functionality is conditionally compiled using build tags:

- **Without gRPC**: `go build ./...` (default)
- **With gRPC**: `go build -tags grpc ./...`

## Configuration

Add gRPC configuration to your `config.yaml`:

```yaml
grpc:
  disabled: false  # Set to true to disable gRPC server
  addr: ":9091"    # gRPC server address
```

## API

### Bidirectional Stream

All operations are handled through a single bidirectional stream:

```protobuf
service FujinService {
  rpc Stream(stream FujinRequest) returns (stream FujinResponse);
}
```

### Request Types

```protobuf
message FujinRequest {
  oneof request {
    ConnectRequest connect = 1;    // Must be sent first
    ProduceRequest produce = 2;
    SubscribeRequest subscribe = 3;
    AckRequest ack = 4;
    NackRequest nack = 5;
  }
}

message ConnectRequest {
  uint32 correlation_id = 1;
  string stream_id = 2;  // Used as transactional ID for Kafka
}

message ProduceRequest {
  uint32 correlation_id = 1;
  string topic = 2;
  bytes message = 3;
  repeated Header headers = 4;
}

message SubscribeRequest {
  uint32 correlation_id = 1;
  string topic = 2;
  bool auto_commit = 3;
  repeated Header headers = 4;
}

message Header {
  bytes key = 1;
  bytes value = 2;
}
```

### Response Types

```protobuf
message FujinResponse {
  oneof response {
    ConnectResponse connect = 1;
    ProduceResponse produce = 2;
    Message message = 3;
    Empty ack = 4;
    Empty nack = 5;
  }
}

message ConnectResponse {
  uint32 correlation_id = 1;
  string error = 2;
}

message ProduceResponse {
  uint32 correlation_id = 1;
  string error = 2;
}

message Message {
  uint32 correlation_id = 1;
  string topic = 2;
  bytes payload = 3;
  repeated Header headers = 4;
  bytes delivery_id = 5;
}
```

## Usage Example

```go
package main

import (
    "context"
    "log"
    "sync"
    "time"

    fujinv1 "github.com/ValerySidorin/fujin/internal/api/grpc/v1"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    // Connect to the gRPC server
    conn, err := grpc.Dial("localhost:9091", grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    // Create a client
    client := fujinv1.NewFujinServiceClient(conn)

    // Create a context with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Start bidirectional stream
    stream, err := client.Stream(ctx)
    if err != nil {
        log.Fatalf("Failed to start stream: %v", err)
    }

    // Start response receiver goroutine
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        for {
            resp, err := stream.Recv()
            if err != nil {
                log.Printf("Failed to receive response: %v", err)
                return
            }
            handleResponse(resp)
        }
    }()

    // Send Connect request first (REQUIRED)
    correlationID := uint32(1)
    connectReq := &fujinv1.FujinRequest{
        Request: &fujinv1.FujinRequest_Connect{
            Connect: &fujinv1.ConnectRequest{
                CorrelationId: correlationID,
                StreamId:      "my-client-123", // Used as Kafka transactional ID
            },
        },
    }

    if err := stream.Send(connectReq); err != nil {
        log.Fatalf("Failed to send connect request: %v", err)
    }
    correlationID++

    // Wait for connect response
    time.Sleep(500 * time.Millisecond)

    // Now send other requests
    produceReq := &fujinv1.FujinRequest{
        Request: &fujinv1.FujinRequest_Produce{
            Produce: &fujinv1.ProduceRequest{
                CorrelationId: correlationID,
                Topic:         "test-topic",
                Message:       []byte("Hello, Fujin gRPC!"),
                Headers: []*fujinv1.Header{
                    {Key: []byte("content-type"), Value: []byte("text/plain")},
                },
            },
        },
    }

    if err := stream.Send(produceReq); err != nil {
        log.Fatalf("Failed to send produce request: %v", err)
    }

    // Wait for response
    time.Sleep(1 * time.Second)

    // Close the stream
    if err := stream.CloseSend(); err != nil {
        log.Printf("Failed to close send: %v", err)
    }

    // Wait for response receiver to finish
    wg.Wait()
}

func handleResponse(resp *fujinv1.FujinResponse) {
    switch r := resp.Response.(type) {
    case *fujinv1.FujinResponse_Connect:
        connectResp := r.Connect
        if connectResp.Error != "" {
            log.Printf("Connect error (correlation_id=%d): %s", 
                connectResp.CorrelationId, connectResp.Error)
        } else {
            log.Printf("Connect success (correlation_id=%d)", connectResp.CorrelationId)
        }
    case *fujinv1.FujinResponse_Produce:
        produceResp := r.Produce
        if produceResp.Error != "" {
            log.Printf("Produce error (correlation_id=%d): %s", 
                produceResp.CorrelationId, produceResp.Error)
        } else {
            log.Printf("Produce success (correlation_id=%d)", produceResp.CorrelationId)
        }
    }
}
```

## Running the Server

1. Build with gRPC support:
   ```bash
   go build -tags grpc -o fujin ./cmd
   ```

2. Run the server:
   ```bash
   ./fujin config.dev.yaml
   ```

3. The gRPC server will be available on port 9091 (or the configured address).

## Testing

Use the example client in `examples/grpc_client/main.go`:

```bash
go run -tags grpc ./examples/grpc_client
```

## Protocol Flow

### 1. Connect (Required First Step)

Every gRPC stream must start with a Connect request:

```go
connectReq := &fujinv1.FujinRequest{
    Request: &fujinv1.FujinRequest_Connect{
        Connect: &fujinv1.ConnectRequest{
            CorrelationId: 1,
            StreamId:      "my-client-123", // Used as Kafka transactional ID
        },
    },
}
```

The `stream_id` parameter is used as the `writerID` when creating Kafka writers, which becomes the `transactional_id` for Kafka transactions.

### 2. Other Operations

After successful Connect, you can send:
- **Produce**: Send messages to topics
- **Subscribe**: Subscribe to topics for message delivery
- **Ack/Nack**: Acknowledge or reject received messages

### 3. Error Handling

If you try to send any request before Connect:
```
Error: "not connected, send Connect request first"
```

## Architecture Details

### Session Management

- **GRPCSession**: Similar to native protocol handler
- **Stream ID**: Stored from Connect request, used as Kafka transactional ID
- **Writers Cache**: Reuses writers per topic within a session
- **Subscribers**: Manages subscriptions per session
- **Correlation IDs**: Tracks request-response pairs

### Async Processing

- **Non-blocking**: Requests are handled in goroutines
- **Response Channel**: Responses are sent through a buffered channel
- **Callback-based**: Uses the same callback pattern as native protocol

### Protocol Compatibility

- **Headers**: Binary-safe `[][]byte` format matching native protocol
- **Error Handling**: Same error semantics as native protocol
- **Resource Management**: Proper cleanup of writers and subscribers
- **Kafka Integration**: Stream ID becomes transactional ID for Kafka

## Future Enhancements

- Complete Subscribe implementation with message delivery
- Ack/Nack implementation for message acknowledgment
- TLS support for secure connections
- Authentication and authorization
- Metrics and tracing interceptors
- Connection pooling and load balancing
