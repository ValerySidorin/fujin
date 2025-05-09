# Makefile for Fujin

APP_NAME := fujin
VERSION ?= $(shell git describe --tags --always --dirty || echo "dev")

ALL_BROKERS = kafka,nats_core,amqp091,amqp10,redis_pubsub,redis_streams,mqtt,nsq

GO_BUILD_TAGS ?= $(ALL_BROKERS)

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
	@./bin/fujin

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