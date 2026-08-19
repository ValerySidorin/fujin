# Azure Service Bus / ActiveMQ Connector (AMQP 1.0)

Azure Service Bus / ActiveMQ connector using AMQP 1.0 protocol.

**Registered name:** `azure_amqp1`

## Configuration

```yaml
connectors:
  my_azure:
    type: azure_amqp1
    settings:
      routes:
        writer:
          conn:
            addr: amqps://my-namespace.servicebus.windows.net
            idle_timeout: 5m
          sender:
            target: my_queue
            settlement_mode: 0
          send:
            settled: true
        reader:
          conn:
            addr: amqps://my-namespace.servicebus.windows.net
          receiver:
            source: my_queue
            credit: 100
```
