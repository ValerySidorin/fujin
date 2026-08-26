# Local broker stacks

The Kafka example is split into broker infrastructure and Fujin composition overlays. This keeps
the Kafka cluster reusable by connector tests while making the complete application stack explicit.

## Kafka and Fujin

From the repository root:

```bash
docker compose \
  -f resources/docker-compose.kafka.yaml \
  -f resources/docker-compose.fujin-kafka.yaml \
  up -d --build --wait
```

The equivalent shortcut is `make up-kafka-fujin`.

The stack contains ZooKeeper, three Kafka brokers, and a Fujin image composed from
[`fujin.kafka.build.toml`](fujin.kafka.build.toml). Fujin exposes:

- QUIC native protocol on `localhost:4848/udp`;
- gRPC on `localhost:4849`;
- TCP native protocol on `localhost:4850`;
- HTTP liveness and readiness on `localhost:8080`.

[`assets/config-kafka.yaml`](assets/config-kafka.yaml) binds connector `connector` with produce route
`pub` and consume route `sub`, both backed by Kafka topic `my_pub_topic`.

Start the Go SDK producer first so Kafka creates `my_pub_topic`, then start the consumer in a
separate terminal:

```bash
cd sdk/go/client
go run ./examples/producer
```

```bash
cd sdk/go/client
go run ./examples/consumer
```

Stop the complete stack and remove its volumes:

```bash
docker compose \
  -f resources/docker-compose.kafka.yaml \
  -f resources/docker-compose.fujin-kafka.yaml \
  down -v --remove-orphans
```

The equivalent shortcut is `make down-kafka-fujin`.

To run only Kafka for connector development, continue using `make up-kafka`, `make down-kafka`, or
`make e2e-kafka`.
