# Fujin

Fujin is a blazingly fast broker connector that bridges your applications with any message broker through a unified, efficient protocol. It translates between a custom byte-based protocol and various messaging systems, aiming to provide a seamless experience regardless of the underlying broker.

> **Note**: This project is currently in active development and is not yet production ready.

## Pitch

Modern distributed systems often need to work with multiple message brokers, each with its own protocol and quirks. Here are some core features:

- **Unified Protocol**: A single, efficient byte-based protocol for all your messaging needs
- **Zero Broker Lock-in**: Switch between brokers without changing your application code
- **Dual Message Delivery Models**:
  - Push (Subscriber): Server pushes messages to clients
  - Pull (Consumer): Clients request messages from server (not for all brokers)
- **Transaction Support**: Atomic message production across multiple topics (not for all brokers)
- **Blazing Speed & Efficiency**: Optimized for processing large volumes of messages efficiently, leveraging techniques like zero-allocation parsing.
- **QUIC Transport**: Reliable, multiplexed communication with built-in security
- **Keep-Alive Mechanism**: Automatic connection health monitoring

## Supported Protocols

- Kafka
- NATS Core
- AMQP 0.9.1 (RabbitMQ)
- AMQP 1.0 (Azure Service Bus)
- RESP (PubSub/Streams)
- NSQ
- MQTT

## Use Cases

Fujin is particularly useful for:

- Microservices architectures integrating diverse messaging backends.
- Systems migrating between message brokers with minimal application changes.
- Applications requiring a unified messaging API across different cloud providers or on-premise systems.
- Scenarios demanding high-throughput message processing.

## When to Use Fujin

Fujin is most valuable in these scenarios:

- **Multiple Broker Environment**: When your system needs to work with different message brokers simultaneously
- **Unified Interface Needed**: When you want a single protocol interface across different brokers

## When Not to Use Fujin

Consider alternatives if:

- **Single Broker**: You're only using one message broker
- **Minimal Layers**: You want to minimize the number of components in your architecture
- **Ultra-Low Latency**: When every microsecond counts and you can't afford additional overhead
- **Broker-Specific Features**: When you need direct access to broker-specific features

## Documentation

- [Protocol Specification](protocol.md)
- [Configuration Guide](config.yaml)

## TODO

- Request/Reply
- Protocol versioning
- Message headers ?

## Contributing

Fujin is completely open-source, feel free to contribute! If you plan to contribute, please see if there's a `CONTRIBUTING.md` file for guidelines, or start by opening an issue to discuss your proposed changes.

## License

This project is licensed under the terms of the MIT License. See the [LICENSE](LICENSE) file for details.
