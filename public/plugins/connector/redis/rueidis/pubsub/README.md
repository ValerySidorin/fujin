# Redis/Valkey PubSub Connector

Redis/Valkey PubSub connector using rueidis client.

**Registered name:** `redis_rueidis_pubsub`

## Configuration

```yaml
connectors:
  my_redis_pubsub:
    type: redis_rueidis_pubsub
    settings:
      common:
        init_address: [localhost:6379]
        username: ""
        password: ""
        disable_cache: false
        batch_size: 100
        linger: 10ms
      routes:
        pub:
          channel: my_channel
        sub:
          channels: [my_channel, other_channel]
```
