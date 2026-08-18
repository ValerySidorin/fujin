# Makefile for Fujin

APP_NAME := fujin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

FUJIN_PKG := github.com/fujin-io/fujin

ALL_TAGS = fujin,grpc

GO_BUILD_TAGS ?= ${ALL_TAGS}

# Full plugin set for default build.
CONFIGURATORS ?= github.com/fujin-io/fujin/public/plugins/configurator/all
CONNECTORS ?= github.com/fujin-io/fujin/public/plugins/connector/all
TRANSPORTS ?= github.com/fujin-io/fujin/public/plugins/transport/all
BIND_MIDDLEWARES ?= github.com/fujin-io/fujin/public/plugins/middleware/bind/all
CONNECTOR_MIDDLEWARES ?= github.com/fujin-io/fujin/public/plugins/middleware/connector/all

BENCH_TIME ?= 1000000x
BENCH_FUNC ?= Benchmark_Produce_32KBPayload_Nop_TCP

# Detect OS
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
    BINARY_EXT := .exe
    RM := del /Q /F
    RMDIR := rmdir /S /Q
    MKDIR := mkdir
    PATHSEP := \\
	BOOTCONF := set "FUJIN_CONFIGURATOR=yaml" && set "FUJIN_CONFIGURATOR_YAML_PATHS=./config.dev.yaml"
else
    DETECTED_OS := $(shell uname -s)
    BINARY_EXT :=
    RM := rm -f
    RMDIR := rm -rf
    MKDIR := mkdir -p
    PATHSEP := /
	BOOTCONF := export FUJIN_CONFIGURATOR=yaml && export FUJIN_CONFIGURATOR_YAML_PATHS=./config.dev.yaml
endif

BIN_DIR := bin
BINARY := $(BIN_DIR)$(PATHSEP)$(APP_NAME)$(BINARY_EXT)

.PHONY: all
all: clean build run

comma := ,
# Build args (comma-separated)
BUILDER_TR_ARGS := $(foreach c,$(subst $(comma), ,$(TRANSPORTS)),-transport $(c))
BUILDER_CONF_ARGS := $(foreach c,$(subst $(comma), ,$(CONFIGURATORS)),-configurator $(c))
BUILDER_CONN_ARGS := $(foreach c,$(subst $(comma), ,$(CONNECTORS)),-connector $(c))
BUILDER_BIND_M_ARGS := $(foreach c,$(subst $(comma), ,$(BIND_MIDDLEWARES)),-bind-middleware $(c))
BUILDER_CONN_M_ARGS := $(foreach c,$(subst $(comma), ,$(CONNECTOR_MIDDLEWARES)),-connector-middleware $(c))

.PHONY: build
build:
	@echo "==> Building ${APP_NAME} for ${DETECTED_OS} (Version: ${VERSION}, Tags: [${GO_BUILD_TAGS}], Connectors: [${CONNECTORS}])"
	@go run ./cmd/builder \
		-local \
		$(BUILDER_TR_ARGS) \
		$(BUILDER_CONF_ARGS) \
		$(BUILDER_CONN_ARGS) \
		$(BUILDER_BIND_M_ARGS) \
		$(BUILDER_CONN_M_ARGS) \
		-tags "$(GO_BUILD_TAGS)" \
		-ldflags "-X main.Version=$(VERSION)" \
		-output ./$(BINARY)
	@echo "==> Binary created: $(BINARY)"

.PHONY: clean
clean:
	@echo "==> Cleaning"
ifeq ($(OS),Windows_NT)
	@if exist $(BIN_DIR) $(RMDIR) $(BIN_DIR) 2>nul
else
	@$(RMDIR) $(BIN_DIR)
endif

.PHONY: run
run:
	@echo "==> Running"
	@$(BOOTCONF) && $(BINARY)

.PHONY: generate
generate:
	@echo "==> Generating gRPC code"
	@cd public/proto && protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/v1/fujin.proto

.PHONY: test
test:
	@echo "==> Running tests"
	@go test -v -tags=${GO_BUILD_TAGS} ./...

.PHONY: cross-build
cross-build:
	@echo "==> Cross-compiling for Windows"
	@GOOS=windows GOARCH=amd64 go build -tags=${GO_BUILD_TAGS} ./...
	@echo "==> Cross-compiling for Linux"
	@GOOS=linux GOARCH=amd64 go build -tags=${GO_BUILD_TAGS} ./...
	@echo "==> Cross-compiling for Darwin (arm64)"
	@GOOS=darwin GOARCH=arm64 go build -tags=${GO_BUILD_TAGS} ./...
	@echo "==> All platforms OK"

.PHONY: help
help:
	@echo "Fujin Makefile ($(DETECTED_OS))"
	@echo ""
	@echo "Usage:"
	@echo "  make build [GO_BUILD_TAGS=\"tag1,tag2\"]  Build binary. Default GO_BUILD_TAGS=\"$(ALL_TAGS)\"."
	@echo "  make run                                  Run binary."
	@echo "  make clean                                Remove build artifacts."
	@echo "  make test                                 Run all tests."
	@echo "  make cross-build                          Verify cross-compilation (Windows, Linux, Darwin)."
	@echo "  make bench                                Run benchmarks."
	@echo "  make sdk-compat                           Build the server and verify fujin-go over native QUIC and gRPC."
	@echo ""
	@echo "Variables:"
	@echo "  VERSION (default: git describe || dev) Version tag for builds."
	@echo "  GO_BUILD_TAGS (default: $(ALL_TAGS))   Comma-separated Go build tags."
	@echo "  CONFIGURATORS (default: all)           Comma-separated configurator names for builder."
	@echo "  CONNECTORS (default: all)              Comma-separated connector names for builder."
	@echo "  BIND_MIDDLEWARES (default: all)        Comma-separated bind middleware names for builder."
	@echo "  CONNECTOR_MIDDLEWARES (default: all)   Comma-separated connector middleware names for builder."
	@echo "  BOOTCONF (optional)                    Bootstrap config."
	@echo "  FUJIN_GO_ROOT (default: ../fujin-go)    Path to the coordinated Go SDK checkout."
	@echo ""
	@echo "Platform: $(DETECTED_OS)"
	@echo "Binary: $(BINARY)"

# Opt-in CGO ZeroMQ connector.
ZEROMQ_PEBBE_PACKAGE := github.com/fujin-io/fujin/public/plugins/connector/zeromq/pebbe
ZEROMQ_PYTHON ?= python3

.PHONY: build-zeromq-pebbe test-zeromq-pebbe e2e-zeromq-pebbe bench-zeromq-pebbe up-zeromq-pebbe down-zeromq-pebbe
build-zeromq-pebbe:
	@go run ./cmd/builder \
		-local -cgo \
		-tags "$(GO_BUILD_TAGS),zeromq_pebbe" \
		-configurator github.com/fujin-io/fujin/public/plugins/configurator/yaml \
		-connector $(ZEROMQ_PEBBE_PACKAGE) \
		-transport github.com/fujin-io/fujin/public/plugins/transport/all \
		-output ./$(BIN_DIR)/fujin-zeromq-pebbe

test-zeromq-pebbe:
	@go test -race -tags=zeromq_pebbe ./public/plugins/connector ./public/plugins/connector/zeromq/pebbe

e2e-zeromq-pebbe:
	@$(ZEROMQ_PYTHON) -c 'import zmq'
	@FUJIN_ZEROMQ_PYZMQ=1 FUJIN_ZEROMQ_PYTHON="$(ZEROMQ_PYTHON)" go test -tags=zeromq_pebbe -run '^TestPyZMQInteroperabilityMatrix$$' ./public/plugins/connector/zeromq/pebbe

bench-zeromq-pebbe:
	@go test -tags=zeromq_pebbe -run '^$$' -bench '^BenchmarkFujinV1Framing$$' -benchmem ./public/plugins/connector/zeromq/pebbe

up-zeromq-pebbe:
	docker compose -f resources/docker-compose.zeromq-pebbe.yaml up -d --build

down-zeromq-pebbe:
	docker compose -f resources/docker-compose.zeromq-pebbe.yaml down --remove-orphans

# Broker management commands
.PHONY: up-kafka_franz down-kafka_franz up-nats_core down-nats_core up-rabbitmq_amqp09 down-rabbitmq_amqp09 up-azure_amqp1 down-azure_amqp1 up-mqtt_paho down-mqtt_paho up-nsq down-nsq up-zeromq-pebbe down-zeromq-pebbe

# Kafka
up-kafka_franz:
	docker compose -f resources/docker-compose.fujin.kafka_franz.yaml -f resources/docker-compose.kafka.yaml -f resources/docker-compose.observability.yaml up -d

down-kafka_franz:
	docker compose -f resources/docker-compose.fujin.kafka_franz.yaml -f resources/docker-compose.kafka.yaml -f resources/docker-compose.observability.yaml down

# NATS
up-nats_core:
	docker compose -f resources/docker-compose.fujin.nats_core.yaml -f resources/docker-compose.nats_core.yaml -f resources/docker-compose.observability.yaml up -d

down-nats_core:
	docker compose -f resources/docker-compose.fujin.nats_core.yaml -f resources/docker-compose.nats_core.yaml -f resources/docker-compose.observability.yaml down

# RabbitMQ
up-rabbitmq_amqp09:
	docker compose -f resources/docker-compose.fujin.rabbitmq_amqp09.yaml -f resources/docker-compose.rabbitmq.yaml -f resources/docker-compose.observability.yaml up -d

down-rabbitmq_amqp09:
	docker compose -f resources/docker-compose.fujin.rabbitmq_amqp09.yaml -f resources/docker-compose.rabbitmq.yaml -f resources/docker-compose.observability.yaml down

# ActiveMQ Artemis
up-azure_amqp1:
	docker compose -f resources/docker-compose.fujin.azure_amqp1.yaml -f resources/docker-compose.artemis.yaml -f resources/docker-compose.observability.yaml up -d

down-azure_amqp1:
	docker compose -f resources/docker-compose.fujin.azure_amqp1.yaml -f resources/docker-compose.artemis.yaml -f resources/docker-compose.observability.yaml down

# EMQX
up-mqtt_paho:
	docker compose -f resources/docker-compose.fujin.mqtt_paho.yaml -f resources/docker-compose.emqx.yaml -f resources/docker-compose.observability.yaml up -d

down-mqtt_paho:
	docker compose -f resources/docker-compose.fujin.mqtt_paho.yaml -f resources/docker-compose.emqx.yaml -f resources/docker-compose.observability.yaml down
# Redis (e.g. ValKey)
up-redis_rueidis_pubsub:
	docker compose -f resources/docker-compose.fujin.redis_rueidis_pubsub.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml up -d

down-redis_rueidis_pubsub:
	docker compose -f resources/docker-compose.fujin.redis_rueidis_pubsub.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml down

up-redis_rueidis_streams:
	docker compose -f resources/docker-compose.fujin.redis_rueidis_streams.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml up -d

down-redis_rueidis_streams:
	docker compose -f resources/docker-compose.fujin.redis_rueidis_streams.yaml -f resources/docker-compose.valkey.yaml -f resources/docker-compose.observability.yaml down

# NSQ
up-nsq:
	docker compose -f resources/docker-compose.fujin.nsq.yaml -f resources/docker-compose.nsq.yaml -f resources/docker-compose.observability.yaml up -d

down-nsq:
	docker compose -f resources/docker-compose.fujin.nsq.yaml -f resources/docker-compose.nsq.yaml -f resources/docker-compose.observability.yaml down

# Helper command to show all available broker commands
broker-help:
	@echo "Available broker commands:"
	@echo "  make up-kafka         - Start Kafka cluster"
	@echo "  make down-kafka       - Stop Kafka cluster"
	@echo "  make up-nats_core     - Start NATS server"
	@echo "  make down-nats_core   - Stop NATS server"
	@echo "  make up-rabbitmq_amqp09       - Start RabbitMQ (AMQP 0.9.1)"
	@echo "  make down-rabbitmq_amqp09     - Stop RabbitMQ"
	@echo "  make up-azure_amqp1        - Start ActiveMQ Artemis (AMQP 1.0)"
	@echo "  make down-azure_amqp1      - Stop Artemis"
	@echo "  make up-mqtt          - Start EMQX (MQTT)"
	@echo "  make down-mqtt       - Stop EMQX"
	@echo "  make up-resp_pubsub   - Start ValKey (Redis PubSub)"
	@echo "  make down-resp_pubsub - Stop ValKey"
	@echo "  make up-redis_rueidis_streams  - Start ValKey (Redis Rueidis Streams)"
	@echo "  make down-redis_rueidis_streams - Stop ValKey"
	@echo "  make up-nsq           - Start NSQ cluster"
	@echo "  make down-nsq         - Stop NSQ cluster"
	@echo "  make up-zeromq-pebbe   - Build and start Fujin plus a pyzmq fixture container"
	@echo "  make down-zeromq-pebbe - Stop the ZeroMQ fixture stack"

.PHONY: bench
bench:
	@go test -bench=${BENCH_FUNC} -benchtime=${BENCH_TIME} -tags=${GO_BUILD_TAGS} ./test

# Broker-backed E2E tests. Targets set FUJIN_E2E=1 and require Docker.
E2E_TIMEOUT ?= 120s

.PHONY: e2e-kafka_franz e2e-nats_core e2e-nats_jetstream e2e-rabbitmq_amqp09 e2e-azure_amqp1 e2e-redis_rueidis_pubsub e2e-redis_rueidis_streams e2e-mqtt_paho e2e-nsq

e2e-kafka_franz:
	docker compose -f resources/docker-compose.kafka.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_KafkaFranz -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.kafka.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-nats_core:
	docker compose -f resources/docker-compose.nats_core.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_NatsCore -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.nats_core.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-nats_jetstream:
	docker compose -f resources/docker-compose.nats_jetstream.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_NatsJetstream -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.nats_jetstream.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-rabbitmq_amqp09:
	docker compose -f resources/docker-compose.rabbitmq.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_RabbitMQ -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.rabbitmq.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-azure_amqp1:
	docker compose -f resources/docker-compose.artemis.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_AzureAMQP1 -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.artemis.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-redis_rueidis_pubsub:
	docker compose -f resources/docker-compose.valkey.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_RedisPubSub -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.valkey.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-redis_rueidis_streams:
	docker compose -f resources/docker-compose.valkey.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_RedisStreams -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.valkey.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-mqtt_paho:
	docker compose -f resources/docker-compose.emqx.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_MQTT -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.emqx.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

e2e-nsq:
	docker compose -f resources/docker-compose.nsq.yaml up -d --wait
	@status=0; FUJIN_E2E=1 go test -v -tags=${GO_BUILD_TAGS} -run TestE2E_NSQ -timeout ${E2E_TIMEOUT} ./test || status=$$?; cleanup=0; docker compose -f resources/docker-compose.nsq.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

FUJIN_GO_ROOT ?= ../fujin-go

.PHONY: sdk-compat
sdk-compat:
	@$(MAKE) -C "$(FUJIN_GO_ROOT)" compat-server FUJIN_SERVER_ROOT="$(CURDIR)"