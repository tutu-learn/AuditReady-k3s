GO ?= go

BINARY := bin/operator
MOCKSERVER := bin/mockserver

# Runtime configuration for `make run.local`
KUBECONFIG ?=
SERVER_ENDPOINT ?= http://127.0.0.1:8443/audit_ready
SERVER_TOKEN ?=
SERVER_PUBLIC_KEY ?=

.PHONY: all deps build test vet fmt run.local e2e clean

all: deps build

deps:
	$(GO) mod download

build:
	$(GO) build ./...
	mkdir -p bin
	$(GO) build -o $(BINARY) ./cmd/operator
	$(GO) build -o $(MOCKSERVER) ./cmd/mockserver

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .

run.local:
	KUBECONFIG="$(KUBECONFIG)" SERVER_ENDPOINT="$(SERVER_ENDPOINT)" SERVER_TOKEN="$(SERVER_TOKEN)" SERVER_PUBLIC_KEY="$(SERVER_PUBLIC_KEY)" $(GO) run ./cmd/operator

# End-to-end flow against a local kind cluster. Best-effort: this requires
# `kind` and `helm` on PATH, which may not be installed everywhere.
# Steps: create a kind cluster, start the mock control plane, then install
# the chart pointing at it.
e2e:
	kind create cluster --name k8s-agent-e2e || true
	$(GO) run ./cmd/mockserver -addr 0.0.0.0:8443 -token dev-token &
	helm install k8s-agent ./charts/k8s-agent \
		--set server.endpoint="http://host.docker.internal:8443/audit_ready" \
		--set server.publicKey="$(SERVER_PUBLIC_KEY)" \
		--set server.token=dev-token

clean:
	rm -rf bin
