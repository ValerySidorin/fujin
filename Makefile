# Makefile for Fujin

APP_NAME := fujin
VERSION ?= $(shell git describe --tags --always --dirty || echo "dev")

ALL_BROKERS = kafka,nats_core,amqp091,amqp10,resp_pubsub,resp_streams,mqtt,nsq

GO_BUILD_TAGS ?= ${ALL_BROKERS}

BENCH_TIME ?= 1000x
BENCH_FUNC ?=

.PHONY: all
all: clean build run

.PHONY: build
build:
	@echo "==> Building ${APP_NAME} (Version: ${VERSION}, Tags: [${GO_BUILD_TAGS}])"
	@mkdir -p bin
	@go build -tags=${GO_BUILD_TAGS} -ldflags "-s -w -X main.Version=${VERSION}" -o bin/${APP_NAME} ./cmd/...

.PHONY: clean
clean:
	@echo "==> Cleaning"
	@rm -rf bin/

.PHONY: run
run:
	@echo "==> Running"
	@./bin/fujin ./config.dev.yaml

.PHONY: test
test:
	@echo "==> Running tests"
	@go test -v -tags=${GO_BUILD_TAGS} ./...

.PHONY: help
help:
	@echo "Fujin Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make build [GO_BUILD_TAGS=\"tag1,tag2\"]  Build binary. Default GO_BUILD_TAGS=\"$(ALL_BROKERS)\"."
	@echo "  make clean                             Remove build artifacts."
	@echo ""
	@echo "Variables:"
	@echo "  VERSION (default: git describe || dev) Version tag for builds."
	@echo "  GO_BUILD_TAGS (default: all brokers)   Comma-separated Go build tags."

# Broker management commands
.PHONY: up-kafka down-kafka up-nats down-nats up-rabbitmq down-rabbitmq up-artemis down-artemis up-emqx down-emqx up-nsq down-nsq

# Kafka
up-kafka:
	docker compose -f resources/docker-compose.fujin.kafka.yaml -f resources/docker-compose.kafka.yaml up -d

down-kafka:
	docker compose -f resources/docker-compose.fujin.kafka.yaml -f resources/docker-compose.kafka.yaml down

# NATS
up-nats_core:
	docker compose -f resources/docker-compose.fujin.nats_core.yaml -f resources/docker-compose.nats_core.yaml up -d

down-nats_core:
	docker compose -f resources/docker-compose.fujin.nats_core.yaml -f resources/docker-compose.nats_core.yaml down

# RabbitMQ
up-amqp091:
	docker compose -f resources/docker-compose.fujin.amqp091.yaml -f resources/docker-compose.rabbitmq.yaml up -d

down-amqp091:
	docker compose -f resources/docker-compose.fujin.amqp091.yaml -f resources/docker-compose.rabbitmq.yaml down

# ActiveMQ Artemis
up-amqp10:
	docker compose -f resources/docker-compose.fujin.amqp10.yaml -f resources/docker-compose.artemis.yaml up -d

down-amqp10:
	docker compose -f resources/docker-compose.fujin.amqp10.yaml -f resources/docker-compose.artemis.yaml down

# EMQX
up-mqtt:
	docker compose -f resources/docker-compose.fujin.mqtt.yaml -f resources/docker-compose.emqx.yaml up -d

down-mqtt:
	docker compose -f resources/docker-compose.fujin.mqtt.yaml -f resources/docker-compose.emqx.yaml down
# Redis (e.g. ValKey)
up-resp_pubsub:
	docker compose -f resources/docker-compose.fujin.resp_pubsub.yaml -f resources/docker-compose.valkey.yaml up -d

down-resp_pubsub:
	docker compose -f resources/docker-compose.fujin.resp_pubsub.yaml -f resources/docker-compose.valkey.yaml down

up-resp_streams:
	docker compose -f resources/docker-compose.fujin.resp_streams.yaml -f resources/docker-compose.valkey.yaml up -d

down-resp_streams:
	docker compose -f resources/docker-compose.fujin.resp_streams.yaml -f resources/docker-compose.valkey.yaml down


# NSQ
up-nsq:
	docker compose -f resources/docker-compose.fujin.nsq.yaml -f resources/docker-compose.nsq.yaml up -d

down-nsq:
	docker compose -f resources/docker-compose.fujin.nsq.yaml -f resources/docker-compose.nsq.yaml down

# Helper command to show all available broker commands
broker-help:
	@echo "Available broker commands:"
	@echo "  make up-kafka       - Start Kafka cluster"
	@echo "  make down-kafka     - Stop Kafka cluster"
	@echo "  make up-nats_core   - Start NATS server"
	@echo "  make down-nats_core - Stop NATS server"
	@echo "  make up-rabbitmq    - Start RabbitMQ server"
	@echo "  make down-rabbitmq  - Stop RabbitMQ server"
	@echo "  make up-artemis     - Start ArtemisMQ server"
	@echo "  make down-artemis   - Stop ArtemisMQ server"
	@echo "  make up-emqx        - Start EMQX server"
	@echo "  make down-emqx      - Stop EMQX server"
	@echo "  make up-redis       - Start Redis server (ValKey)"
	@echo "  make down-redis     - Stop Redis server (ValKey)"
	@echo "  make up-nsq         - Start NSQ cluster"
	@echo "  make down-nsq       - Stop NSQ cluster"

.PHONY: bench
bench:
	@go test -bench=${BENCH_FUNC} -benchtime=${BENCH_TIME} -tags=${GO_BUILD_TAGS} github.com/ValerySidorin/fujin/test