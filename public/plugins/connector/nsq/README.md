# NSQ Connector

NSQ connector.

**Registered name:** `nsq`

## Configuration

```yaml
connectors:
  my_nsq:
    type: nsq
    settings:
      common:
        address: localhost:4150
        addresses: [localhost:4150]
        lookupd_addresses: [localhost:4161]
      routes:
        pub:
          topic: my_topic
        sub:
          topic: my_topic
          channel: my_channel
          max_in_flight: 5
```
