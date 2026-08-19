# Kafka Connector (franz-go)

Apache Kafka connector using the [franz-go](https://github.com/twmb/franz-go) client.

**Registered name:** `kafka_franz`

## Configuration

```yaml
connectors:
  my_kafka:
    type: kafka_franz
    overridable:
      - routes.*.produce_topic
      - routes.*.consume_topics
      - routes.*.group
    settings:
      common:
        brokers: [localhost:9092, localhost:9093]
        ping_timeout: 5s
        tls:
          enabled: false
          server_cert_pem_path: ./certs/server.pem
          server_key_pem_path: ./certs/server-key.pem
      routes:
        producer:
          produce_topic: my_topic
          allow_auto_topic_creation: true
          linger: 10ms
          max_buffered_records: 10000
          disable_idempotent_write: false
          # transactional_id: my_tx_id
        consumer:
          consume_topics: [my_topic]
          group: my_group
          max_poll_records: 10000
          auto_commit_interval: 5s
          auto_commit_marks: false
          disable_auto_commit: false
          block_rebalance_on_poll: false
          balancers: [cooperative_sticky]
          fetch_isolation_level: read_uncommited
```

## Settings Reference

### Balancers

`sticky`, `cooperative_sticky`, `range`, `round_robin`

### Isolation Levels

`read_uncommited`, `read_commited`
