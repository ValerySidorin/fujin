# RabbitMQ Connector (AMQP 0-9-1)

RabbitMQ connector using AMQP 0-9-1 protocol.

**Registered name:** `rabbitmq_amqp09`

## Configuration

```yaml
connectors:
  my_rabbit:
    type: rabbitmq_amqp09
    settings:
      routes:
        writer:
          conn:
            url: amqp://guest:guest@localhost:5672/
            vhost: /
            heartbeat: 10s
          exchange:
            name: my_exchange
            kind: topic
            durable: true
          queue:
            name: my_queue
            durable: true
          queue_bind:
            routing_key: my.key
          publish:
            mandatory: false
            content_type: application/json
            delivery_mode: 2
        reader:
          conn:
            url: amqp://guest:guest@localhost:5672/
          exchange:
            name: my_exchange
            kind: topic
            durable: true
          queue:
            name: my_queue
            durable: true
          queue_bind:
            routing_key: my.key
          consume:
            consumer: my_consumer
          ack:
            multiple: false
          nack:
            multiple: false
            requeue: true
```
