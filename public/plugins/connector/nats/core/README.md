# NATS Core Connector

NATS Core pub/sub connector.

**Registered name:** `nats_core`

## Fujin route capabilities

Each configured route supports `PRODUCE` with `local_accept` semantics and auto-settle `SUBSCRIBE`. NATS Core does not expose broker acknowledgements, manual settlement, `FETCH`, transactions, or Fujin's lossless binary-header capability. Header-aware Fujin operations are therefore rejected even though the underlying NATS client has its own header representation.


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
