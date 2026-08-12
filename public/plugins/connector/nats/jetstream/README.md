# NATS JetStream Connector

Connects to NATS JetStream for at-least-once message delivery with durable consumers, pull-based fetch, and explicit acknowledgments.

**Registered name:** `nats_jetstream`

## Features

- **At-least-once delivery** with explicit Ack/Nack
- **Durable consumers** for persistent subscriptions
- **Pull-based Fetch** for controlled message consumption
- **Push-based Subscribe** via JetStream consumer callbacks
- **Server-side publish acknowledgment** (unlike NATS Core fire-and-forget)
- **Headers** support

## Configuration

```yaml
connectors:
  my_jetstream:
    type: nats_jetstream
    settings:
      common:
        url: "nats://localhost:4222"
        stream: "ORDERS"
      routes:
        order_writer:
          subject: "orders.created"
        order_reader:
          subject: "orders.>"
          consumer: "order-processor"    # durable consumer name (optional)
          ack_wait: "30s"                # ack timeout (default: 30s)
          max_deliver: 5                 # max redeliveries (default: unlimited)
          max_ack_pending: 1000          # max unacked messages (default: 1000)
```

## Settings

### Common

| Setting | Required | Description |
|---------|----------|-------------|
| `url` | yes | NATS server URL |
| `stream` | yes | JetStream stream name |

### Client-specific

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `subject` | yes | | Subject to publish/subscribe |
| `consumer` | no | auto-generated | Durable consumer name |
| `ack_wait` | no | `30s` | Time before unacked message is redelivered |
| `max_deliver` | no | unlimited | Maximum redelivery attempts |
| `max_ack_pending` | no | `1000` | Maximum number of unacknowledged messages |

## Differences from NATS Core

| Feature | `nats_core` | `nats_jetstream` |
|---------|-------------|------------------|
| Delivery guarantee | at-most-once | at-least-once |
| Fetch (pull) | not supported | supported |
| Ack/Nack | not supported | supported |
| Durable consumers | no | yes |
| Publish confirmation | no (fire-and-forget) | yes (server ack) |

## Prerequisites

The JetStream stream must exist before Fujin starts. Create it using the NATS CLI:

```bash
nats stream add ORDERS --subjects "orders.>" --storage file --retention limits
```
