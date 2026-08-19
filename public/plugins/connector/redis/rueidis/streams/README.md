# Redis/Valkey Streams Connector

Redis/Valkey Streams connector using rueidis client. Supports consumer groups.

**Registered name:** `redis_rueidis_streams`

## Configuration

```yaml
connectors:
  my_redis_streams:
    type: redis_rueidis_streams
    settings:
      common:
        init_address: [localhost:6379]
        batch_size: 100
        linger: 10ms
      routes:
        writer:
          stream: my_stream
          marshaller: json
        reader:
          streams:
            my_stream:
              start_id: "0"
              group_create_id: "0"
          group:
            name: my_group
            consumer: consumer_1
          block: 5s
          count: 100
```
