# NATS Core Connector

NATS Core pub/sub connector.

**Registered name:** `nats_core`

## Configuration

```yaml
connectors:
  my_nats:
    type: nats_core
    settings:
      common:
        url: nats://localhost:4222
      routes:
        pub:
          subject: my_subject
        sub:
          subject: my_subject
```
