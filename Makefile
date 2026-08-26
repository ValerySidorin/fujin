# Fujin Rust workspace

APP_NAME := fujin
VERSION ?=
RUST_FEATURES ?= full
RUST_PROFILE ?= release
E2E_TIMEOUT ?= 120

ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
    BINARY_EXT := .exe
    RM_TREE := rmdir /S /Q
    MKDIR := mkdir
    COPY := copy /Y
    PATHSEP := \\
    RUNCONF := set "FUJIN_CONFIGURATOR=file" && set "FUJIN_CONFIGURATOR_FILE_PATHS=./config.dev.yaml" &&
    PLATFORM_ENV :=
else
    DETECTED_OS := $(shell uname -s)
    BINARY_EXT :=
    RM_TREE := rm -rf
    MKDIR := mkdir -p
    COPY := cp
    PATHSEP := /
    RUNCONF := FUJIN_CONFIGURATOR=file FUJIN_CONFIGURATOR_FILE_PATHS=./config.dev.yaml
    ifeq ($(DETECTED_OS),Darwin)
        PLATFORM_ENV := CPLUS_INCLUDE_PATH=$(shell xcrun --show-sdk-path)/usr/include/c++/v1
    else
        PLATFORM_ENV :=
    endif
endif

VERSION_ENV := $(if $(VERSION),VERSION="$(VERSION)",)
BIN_DIR := bin
BINARY := $(BIN_DIR)$(PATHSEP)$(APP_NAME)$(BINARY_EXT)
CARGO_BINARY := target$(PATHSEP)$(RUST_PROFILE)$(PATHSEP)$(APP_NAME)$(BINARY_EXT)

.PHONY: all build clean run generate fmt lint check test sdk-test sdk-compat help
all: clean build

build:
	@echo "==> Building $(APP_NAME) with Rust for $(DETECTED_OS) (features: $(RUST_FEATURES))"
	@$(PLATFORM_ENV) $(VERSION_ENV) cargo build --profile $(RUST_PROFILE) -p fujin-app --features "$(RUST_FEATURES)"
	@$(MKDIR) $(BIN_DIR)
	@$(COPY) $(CARGO_BINARY) $(BINARY)
	@echo "==> Binary created: $(BINARY)"

clean:
	@echo "==> Removing packaged binaries"
ifeq ($(OS),Windows_NT)
	@if exist $(BIN_DIR) $(RM_TREE) $(BIN_DIR) 2>nul
else
	@$(RM_TREE) $(BIN_DIR)
endif

run: build
	@$(RUNCONF) $(BINARY)

generate:
	@cargo build -p fujin-grpc-proto
	@$(MAKE) -C sdk/go/client generate

fmt:
	@cargo fmt --all --check

lint:
	@$(PLATFORM_ENV) cargo clippy --workspace --all-features --all-targets -- -D warnings

check:
	@$(PLATFORM_ENV) cargo check --workspace --all-features --all-targets

test:
	@$(PLATFORM_ENV) cargo test --workspace --all-features --all-targets

.PHONY: up-kafka down-kafka up-kafka-fujin down-kafka-fujin e2e-kafka bench bench-report sdk-compat
up-kafka:
	docker compose -f resources/docker-compose.kafka.yaml up -d --wait

down-kafka:
	docker compose -f resources/docker-compose.kafka.yaml down --remove-orphans

up-kafka-fujin:
	docker compose -f resources/docker-compose.kafka.yaml -f resources/docker-compose.fujin-kafka.yaml up -d --build --wait

down-kafka-fujin:
	docker compose -f resources/docker-compose.kafka.yaml -f resources/docker-compose.fujin-kafka.yaml down -v --remove-orphans

e2e-kafka:
	docker compose -f resources/docker-compose.kafka.yaml up -d --wait
	@status=0; FUJIN_KAFKA_E2E=1 cargo test -p fujin-connector-kafka --test kafka_e2e -- --nocapture || status=$$?; cleanup=0; docker compose -f resources/docker-compose.kafka.yaml down --remove-orphans || cleanup=$$?; test $$status -eq 0 || exit $$status; exit $$cleanup

bench:
	@cargo run --release -q -p fujin-bench --bin session-bench --features bench

bench-report:
	@./scripts/generate_bench_report.sh

sdk-test:
	@cd sdk/go/client && go test -race ./...
	@cd sdk/go/embed && go test -race ./...

FUJIN_GO_ROOT ?= sdk/go/client
sdk-compat:
	@$(MAKE) -C "$(FUJIN_GO_ROOT)" compat-server FUJIN_SERVER_ROOT="$(CURDIR)"

help:
	@echo "Fujin Rust workspace ($(DETECTED_OS))"
	@echo ""
	@echo "  make build [VERSION=v0.6.0-alpha.1] [RUST_FEATURES=full]"
	@echo "  make run            Build and run with ./config.dev.yaml"
	@echo "  make fmt            Check rustfmt"
	@echo "  make lint           Run Clippy with warnings denied"
	@echo "  make check          Check all workspace targets and features"
	@echo "  make test           Test all workspace targets and features"
	@echo "  make generate       Regenerate/check protobuf bindings"
	@echo "  make sdk-test       Test both Go SDK modules with the race detector"
	@echo "  make e2e-kafka      Run the broker-backed Kafka contract"
	@echo "  make bench-report   Regenerate bench_report.md"
	@echo "  make sdk-compat     Verify native QUIC and gRPC with the Go client SDK"
