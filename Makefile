# Makefile for Fujin

APP_NAME := fujin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
RUST_ROOT := fujin-rs
RUST_FEATURES ?= full
RUST_PROFILE ?= release

GO_BUILD_TAGS ?= fujin,grpc
BENCH_TIME ?= 1000000x
BENCH_FUNC ?= Benchmark_Produce_32KBPayload_Nop_TCP

# Detect OS
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
    BINARY_EXT := .exe
    RM := del /Q /F
    RMDIR := rmdir /S /Q
    MKDIR := mkdir
    COPY := copy /Y
    PATHSEP := \\
    RUNCONF := set "FUJIN_CONFIG=./config.dev.yaml"
    RUST_BUILD_ENV :=
else
    DETECTED_OS := $(shell uname -s)
    BINARY_EXT :=
    RM := rm -f
    RMDIR := rm -rf
    MKDIR := mkdir -p
    COPY := cp
    PATHSEP := /
    RUNCONF := FUJIN_CONFIG=./config.dev.yaml
    ifeq ($(DETECTED_OS),Darwin)
        RUST_BUILD_ENV := CPLUS_INCLUDE_PATH=$(shell xcrun --show-sdk-path)/usr/include/c++/v1
    else
        RUST_BUILD_ENV :=
    endif
endif

BIN_DIR := bin
BINARY := $(BIN_DIR)$(PATHSEP)$(APP_NAME)$(BINARY_EXT)
RUST_BINARY := $(RUST_ROOT)$(PATHSEP)target$(PATHSEP)$(RUST_PROFILE)$(PATHSEP)$(APP_NAME)$(BINARY_EXT)

.PHONY: all
all: clean build run

.PHONY: build
build:
	@echo "==> Building ${APP_NAME} with Rust for ${DETECTED_OS} (Version: ${VERSION}, Features: [${RUST_FEATURES}])"
	@$(RUST_BUILD_ENV) FUJIN_VERSION="$(VERSION)" cargo build --manifest-path $(RUST_ROOT)/Cargo.toml --profile $(RUST_PROFILE) -p fujin --features "$(RUST_FEATURES)"
	@$(MKDIR) $(BIN_DIR)
	@$(COPY) $(RUST_BINARY) $(BINARY)
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
	@$(RUNCONF) $(BINARY)

.PHONY: generate
generate:
	@echo "==> Regenerating Rust gRPC bindings"
	@cargo build --manifest-path $(RUST_ROOT)/Cargo.toml -p fujin-proto

.PHONY: test
test:
	@echo "==> Running Rust tests"
	@$(RUST_BUILD_ENV) cargo test --manifest-path $(RUST_ROOT)/Cargo.toml --workspace --all-features --all-targets

.PHONY: cross-build
cross-build:
	@echo "==> Checking all Rust production features"
	@$(RUST_BUILD_ENV) cargo check --manifest-path $(RUST_ROOT)/Cargo.toml --workspace --all-features --all-targets

.PHONY: go-test-legacy
go-test-legacy:
	@go test -tags=${GO_BUILD_TAGS} ./...

.PHONY: help
help:
	@echo "Fujin Makefile ($(DETECTED_OS))"
	@echo ""
	@echo "Usage:"
	@echo "  make build [RUST_FEATURES=full]       Build the production Rust binary."
	@echo "  make run                              Run with ./config.dev.yaml."
	@echo "  make clean                            Remove packaged binaries."
	@echo "  make test                             Run the complete Rust workspace suite."
	@echo "  make cross-build                      Check all Rust targets and features."
	@echo "  make go-test-legacy                    Run the retained Go comparison suite."
	@echo "  make rust-bench-report                 Generate the Rust benchmark report."
	@echo "  make sdk-compat                        Verify the coordinated Go SDK."
	@echo ""
	@echo "Variables:"
	@echo "  VERSION (default: git describe || dev) Build version."
	@echo "  RUST_FEATURES (default: full)           Cargo feature set."
	@echo "  RUST_PROFILE (default: release)         Cargo profile."
	@echo ""
	@echo "Platform: $(DETECTED_OS)"
	@echo "Binary: $(BINARY)"

# Broker management commands
.PHONY: up-kafka_franz down-kafka_franz up-nats_core down-nats_core up-rabbitmq_amqp09 down-rabbitmq_amqp09 up-azure_amqp1 down-azure_amqp1 up-mqtt_paho down-mqtt_paho up-nsq down-nsq

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

.PHONY: bench-report
bench-report:
	@test/generate_bench_report.sh

.PHONY: rust-bench-report
rust-bench-report:
	@fujin-rs/scripts/generate_bench_report.sh

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