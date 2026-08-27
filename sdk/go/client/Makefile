.PHONY: generate compat-server

FUJIN_SERVER_ROOT ?= ../../..
PROTO_SOURCE := $(FUJIN_SERVER_ROOT)/crates/fujin-grpc-proto/proto/fujin.proto

generate:
	@echo "==> Generating Go messages from $(PROTO_SOURCE)"
	@protoc --proto_path="$(dir $(PROTO_SOURCE))" --go_out=grpc/v1/proto --go_opt=paths=source_relative "$(PROTO_SOURCE)"

compat-server:
	@FUJIN_SERVER_ROOT="$(FUJIN_SERVER_ROOT)" go test -count=1 -run '^TestServerCompatibility$$' .
