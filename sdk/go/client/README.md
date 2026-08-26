# Fujin Go Client

The Fujin Go client is maintained with the server in the [Fujin monorepo](https://github.com/fujin-io/fujin). It supports the native Fujin protocol over QUIC and the Fujin gRPC API through the same `Conn`, `Stream`, and `Subscription` interfaces.

## Requirements

- Go `1.25.1` or newer
- Fujin server `v0.5.0` or newer for the native QUIC client

The native protocol uses version-independent QUIC ALPN `fujin`. Every session stream performs mandatory HELLO negotiation before BIND, selects byte wire version `1` (`fujin/1`), and exchanges diagnostic SDK/server build versions. This is incompatible with Fujin servers before `v0.5.0`.

## Install

```bash
go get github.com/fujin-io/fujin/sdk/go/client@latest
```

## Choose a transport

| Transport | Package | Server endpoint | Use when |
| --- | --- | --- | --- |
| Native QUIC | `github.com/fujin-io/fujin/sdk/go/client/fujin` | Fujin QUIC listener, commonly `:4848` | You want the native protocol and QUIC transport. |
| gRPC | `github.com/fujin-io/fujin/sdk/go/client/grpc/v1` | Fujin gRPC listener, commonly `:4849` | Your environment standardizes on gRPC. |

Both return `client.Conn` and expose the same message operations.

## Native QUIC quick start

The server controls its TLS policy. In production, configure a trust store and server name; `InsecureSkipVerify` is appropriate only for local development with a self-signed certificate.

```go
package main

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	client "github.com/fujin-io/fujin/sdk/go/client"
	"github.com/fujin-io/fujin/sdk/go/client/fujin"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := fujin.Dial(ctx, "fujin.example:4848", &tls.Config{
		ServerName: "fujin.example",
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stream, err := conn.Bind(ctx, "orders")
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close(context.Background())

	if err := stream.HProduce(ctx, "events", []byte(`{"id":"42"}`), []client.Header{
		{Key: []byte("content-type"), Value: []byte("application/json")},
	}); err != nil {
		log.Fatal(err)
	}
}
```

`fujin.Dial` sets the required native ALPN value automatically. Optional native connection settings include `fujin.WithTimeout`, `fujin.WithWriteDeadline`, and `fujin.WithLogger`.

## gRPC quick start

Pass ordinary gRPC dial options to `grpc/v1.Dial`. This example uses plaintext for a local server only; use `credentials.NewTLS` for production.

```go
package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/fujin-io/fujin/sdk/go/client/grpc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := v1.Dial("127.0.0.1:4849", slog.Default(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := conn.Bind(ctx, "orders")
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close(context.Background())

	if err := stream.Produce(ctx, "events", []byte("created")); err != nil {
		log.Fatal(err)
	}
}
```

## Bind and route capabilities

A **connector** is selected when calling `Bind`; a **route** is selected for each message operation. The server returns the capabilities of every route in `Stream.Routes()` after a successful bind.

Check capabilities before using optional operations such as transactions, headers, fetch, or manual settlement:

```go
routes := stream.Routes()
orders, ok := routes["events"]
if !ok || !orders.Produce {
	log.Fatal("events is not a produce-capable route")
}
if orders.Transactions {
	if err := stream.BeginTx(ctx, "events"); err != nil {
		log.Fatal(err)
	}
	if err := stream.TxProduce(ctx, []byte("created")); err != nil {
		_ = stream.RollbackTx(ctx)
		log.Fatal(err)
	}
	if err := stream.CommitTx(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`Routes` returns a copy. Modifying its returned map does not change stream state.

## Produce

```go
err := stream.Produce(ctx, "events", []byte("created"))

err = stream.HProduce(ctx, "events", []byte(`{"event":"created"}`), []client.Header{
	{Key: []byte("content-type"), Value: []byte("application/json")},
})
```

Use `HProduce` only when the route advertises `Headers`.

## Transactions

Transactions are scoped to one route. Start one with `BeginTx`, then call `TxProduce` or `TxHProduce`; those methods do not accept a route because the active transaction already determines it.

```go
if err := stream.BeginTx(ctx, "events"); err != nil {
	return err
}
if err := stream.TxHProduce(ctx, []byte("created"), nil); err != nil {
	_ = stream.RollbackTx(ctx)
	return err
}
return stream.CommitTx(ctx)
```

Use transactions only when the route advertises `Transactions`.

## Subscribe, fetch, and settlement

`Subscribe` and `HSubscribe` return a `Subscription`. The `autoSettle` argument controls whether the server settles messages automatically. Set it to `false` only for a route with `ManualSettlement` support, then call `Ack` or `Nack` using the delivered subscription and message IDs.

```go
subscription, err := stream.HSubscribe(ctx, "events", false, func(message client.Message) {
	results, err := stream.Ack(ctx, message.SubscriptionID, message.MessageID)
	if err != nil {
		log.Printf("ack request: %v", err)
		return
	}
	for _, result := range results {
		if result.Error != nil {
			log.Printf("ack %x: %v", result.MessageID, result.Error)
		}
	}
})
if err != nil {
	return err
}
defer subscription.Close(context.Background())
```

For bounded reads, use `Fetch` or `HFetch`:

```go
result, err := stream.Fetch(ctx, "events", true, 100)
if err != nil {
	return err
}
for _, message := range result.Messages {
	log.Printf("%s", message.Payload)
}
```

Check `Subscribe`, `Fetch`, `Headers`, `ManualSettlement`, `AckGranularity`, and `NackEffect` in the route capability profile before relying on those operations.

## Bind metadata and configuration overrides

Pass metadata and connector configuration overrides when binding:

```go
stream, err := conn.Bind(ctx, "orders",
	client.WithMeta(map[string]string{
		"tenant": "acme",
	}),
	client.WithConfigOverrides(map[string]string{
		"routes.events.produce_topic": "orders.created",
	}),
)
```

The server authorizes metadata and validates override paths against its connector configuration. An override that is not explicitly allowed by the server will fail the bind.

## Structured operation errors

Server-side operation failures may be returned as `*client.OperationError`. Inspect the status code, outcome, reason, message, and detail fields instead of matching error text.

```go
err := stream.Produce(ctx, "missing-route", []byte("message"))
var operationErr *client.OperationError
if errors.As(err, &operationErr) {
	log.Printf("code=%d outcome=%d reason=%s details=%v",
		operationErr.Code,
		operationErr.Outcome,
		operationErr.Reason,
		operationErr.Details,
	)
}
```

`OutcomeNotApplied` means the server did not apply the operation. `OutcomeApplied` means the operation was applied even though the caller received an error while completing the request. Treat `OutcomeUnknown` as indeterminate and reconcile according to the connector's delivery guarantees.

## Resource lifecycle

- Call `Subscription.Close` when a subscription is no longer needed.
- Call `Stream.Close` when finished with a bound stream.
- Call `Conn.Close` before application shutdown.
- Use bounded contexts for dialing and application operations. `fujin.WithTimeout` supplies a default native operation timeout when a context has no deadline.

## Examples

Runnable examples are included in this repository:

```bash
go run ./examples/producer
go run ./examples/subscriber
go run ./examples/grpc_client
```

The example addresses, connector names, routes, and TLS settings must match your Fujin server configuration.

## Compatibility verification

The coordinated Fujin release gate exercises both native QUIC and gRPC clients against a built Fujin server:

```bash
# From the Fujin repository root.
make sdk-compat
```

## License

See the repository license.
