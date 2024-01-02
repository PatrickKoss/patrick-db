PWD = $(shell pwd)
PROTO_DIR = proto
PROTOC_VERSION = 25.0
export PATH := $(PWD)/bin:$(PATH)

PROTOC_ZIP = protoc.zip
bin/protoc: ## Download and installs protoc
ifeq ($(shell uname -s), Darwin)
	$(eval TARGET_SYSTEM := "osx-universal_binary")
else
	$(eval TARGET_SYSTEM := "$(shell uname -s)-$(shell uname -m)")
endif
	@[ -e $(@) ] && : || curl -sSfL https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(TARGET_SYSTEM).zip -o $(PROTOC_ZIP) && unzip -o $(PROTOC_ZIP) -d $(PWD) bin/protoc && rm -f $(PROTOC_ZIP)


.PHONY: proto/buf.lock
proto/buf.lock: bin/buf bin/protoc
	@buf mod update $(PROTO_DIR)

protolint: proto/buf.lock bin/protoc-gen-buf-lint ## Lint protobuf files
	@buf format --diff --exit-code

protoformat: proto/buf.lock bin/protoc-gen-buf-lint ## Format protobuf files
	@buf format --write $(PROTO_DIR)

## Generates code from protobuf files
gen-proto: install-proto-tools proto/buf.lock
	@buf generate

build-proto: gen-proto ## Build protobuf files
	@buf build -o $(PROTO_DIR)/server-envoy.pb

install-proto-tools: install-buf install-protoc-gen-buf-breaking install-protoc-gen-buf-lint install-protoc-gen-validate install-protoc-gen-openapi install-protoc-gen-go install-protoc-gen-go-grpc

BUF = bin/buf
install-buf: $(BUF)
$(BUF):
	GOBIN=$(PWD)/bin go install github.com/bufbuild/buf/cmd/buf@latest

PROTOC_GEN_BUF_BREAKING = bin/protoc-gen-buf-breaking
install-protoc-gen-buf-breaking: $(PROTOC_GEN_BUF_BREAKING)
$(PROTOC_GEN_BUF_BREAKING):
	GOBIN=$(PWD)/bin go install github.com/bufbuild/buf/cmd/protoc-gen-buf-breaking@latest

PROTOC_GEN_BUF_LINT = bin/protoc-gen-buf-lint
install-protoc-gen-buf-lint: $(PROTOC_GEN_BUF_LINT)
$(PROTOC_GEN_BUF_LINT):
	GOBIN=$(PWD)/bin go install github.com/bufbuild/buf/cmd/protoc-gen-buf-lint@latest

PROTOC_GEN_VALIDATE = bin/protoc-gen-validate
install-protoc-gen-validate: $(PROTOC_GEN_VALIDATE)
$(PROTOC_GEN_VALIDATE):
	GOBIN=$(PWD)/bin go install github.com/envoyproxy/protoc-gen-validate@latest

PROTOC_GEN_OPENAPI = bin/protoc-gen-openapi
install-protoc-gen-openapi: $(PROTOC_GEN_OPENAPI)
$(PROTOC_GEN_OPENAPI):
	GOBIN=$(PWD)/bin go install github.com/google/gnostic/cmd/protoc-gen-openapi@latest

PROTOC_GEN_GO = bin/protoc-gen-go
install-protoc-gen-go: $(PROTOC_GEN_GO)
$(PROTOC_GEN_GO):
	GOBIN=$(PWD)/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

PROTOC_GEN_GO_GRPC = bin/protoc-gen-go-grpc
install-protoc-gen-go-grpc: $(PROTOC_GEN_GO_GRPC)
$(PROTOC_GEN_GO_GRPC):
	GOBIN=$(PWD)/bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

run-router:
	cargo run --bin router

run-server:
	cargo run --bin server

run-follower:
	cargo run --bin server -- --server-address="[::1]:50053" --server-url="http://[::1]:50053" --storage-file-name=patrick2.db
