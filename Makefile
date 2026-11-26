# Copyright 2026, Staffbase SE and contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SHELL        := /bin/bash
.SHELLFLAGS  := -ec

REPO_ROOT    := $(shell git rev-parse --show-toplevel)
CLUSTER_NAME := flux-ai-bridge-e2e
CONTEXT      := kind-$(CLUSTER_NAME)
E2E_IMAGE    := flux-ai-bridge:e2e
MANIFESTS    := $(REPO_ROOT)/e2e/manifests
# Throwaway GitHub App key for clusters that never talk to GitHub (mock-mode
# e2e, ct-install). The bridge parses the PEM at startup, so it must be a real
# RSA key. Generated on first use; gitignored.
MOCK_APP_KEY := $(REPO_ROOT)/e2e/fixtures/mock-app-key.pem

# Flux Operator chart that `make flux-install` (e2e-up, ct-install) installs.
# Pinned so a new release can't change CI on its own; keep it in step with the
# flux-operator-mcp sub-chart version in charts/flux-ai-bridge/Chart.yaml.
FLUX_OPERATOR_VERSION ?= 0.63.1

# `make ct-install`: an existing kind cluster, the namespace ct installs every
# release into, and the Git source of the podinfo Kustomization the chart's
# Alert watches. CI sets the source to the pull request's merge ref.
CT_CLUSTER_NAME ?= flux-ai-bridge-ct
CT_CONTEXT      := kind-$(CT_CLUSTER_NAME)
CT_NAMESPACE    ?= flux-ai-bridge
CT_IMAGE_REPO   := flux-ai-bridge
CT_IMAGE_TAG    := ct
CT_GIT_URL      ?= https://github.com/Staffbase/flux-ai-bridge
CT_GIT_REF      ?= refs/heads/main

# Default model used when E2E_LLM=ollama. Two equivalent override knobs:
#   - OLLAMA_MODEL=…  on the make invocation (this variable)
#   - E2E_LLM_MODEL=… in the environment (shell-level, wins if set)
# Either reaches both the bridge (config.llm.model) and the Ollama sub-chart
# (ollama.ollama.models.pull/run) via the e2e values template.
OLLAMA_MODEL ?= qwen2.5-coder:1.5b

# Ollama container image — pre-pulled on the host and kind-loaded so each
# cluster recreation skips the network pull. The tag is auto-derived from the
# bundled chart's appVersion so it always matches what the kubelet will
# actually want; when a sub-chart version in Chart.yaml is bumped and
# `make chart-deps` fetches the new archive, this tracks it on the next
# `make e2e-up`.
#
# OLLAMA_CHART resolves to a single path or empty (firstword handles the
# rare case where stale tarballs from prior dep-update runs linger).
# The e2e-up recipe checks for emptiness before it builds or creates
# anything, and points the user at `make chart-deps` when the archives
# are missing.
OLLAMA_CHART     := $(firstword $(wildcard $(REPO_ROOT)/charts/flux-ai-bridge/charts/ollama-*.tgz))
OLLAMA_VERSION   := $(if $(OLLAMA_CHART),$(shell tar -xzOf $(OLLAMA_CHART) ollama/Chart.yaml 2>/dev/null | awk '/^appVersion:/ {print $$2}'))
OLLAMA_IMAGE     ?= ollama/ollama:$(OLLAMA_VERSION)

# Host directory where the Ollama model cache is persisted across kind cluster
# lifecycles. Survives `make e2e-down && make e2e-up` so the model only pulls
# once per host.
OLLAMA_CACHE_DIR ?= $(HOME)/.cache/flux-ai-bridge/ollama

# Dev image tag for `make image`. Distinct from $(E2E_IMAGE) so a `helm
# install` against the dev image doesn't get accidentally clobbered by an
# `e2e-up` rebuild and vice versa.
DEV_IMAGE ?= flux-ai-bridge:dev

# govulncheck release `make vuln` (and CI) runs. Pinned so a new release
# can't change CI on its own; bump it deliberately. The vulnerability
# database is still fetched fresh on every run.
GOVULNCHECK_VERSION ?= v1.9.0

# `make` with no target still builds (unchanged); `make help` lists everything.
.DEFAULT_GOAL := build

.PHONY: help
help: ## Show this help (all available targets)
	@echo "flux-ai-bridge — available make targets:"
	@echo ""
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all Go packages
	go build ./...

# `make run` runs the bridge locally with `go run`. The config loader
# requires MCP_URL + LLM_PROVIDER + GITHUB_APP_* at minimum; export those
# first or the bridge exits at startup with a clear error. See
# docs/development.md "Running Locally" for the full env-var list.
.PHONY: run
run: ## Run the bridge locally (go run; needs MCP_URL, LLM_PROVIDER, GITHUB_APP_* env)
	go run ./cmd/...

# `make image` builds the local dev container image. Tag is $(DEV_IMAGE)
# (default `flux-ai-bridge:dev`); override with `make image DEV_IMAGE=...`.
# Separate from the e2e flow — e2e-up builds its own `flux-ai-bridge:e2e`
# tag to keep the two image lifecycles independent.
.PHONY: image
image: ## Build the local dev container image (DEV_IMAGE, default flux-ai-bridge:dev)
	docker build -t $(DEV_IMAGE) $(REPO_ROOT)

# `make chart-deps` fetches the sub-charts pinned in Chart.yaml into
# charts/flux-ai-bridge/charts/ (gitignored, like Chart.lock). Helm only
# resolves https chart repositories that are registered, so the two are added
# first; adding them refreshes their index, so the update skips a refresh of
# every other repo. `update` rather than `build`: a local Chart.lock left over
# from before a version bump would make `build` fail. Output goes to stderr
# so `make template | yq …` stays clean.
.PHONY: chart-deps
chart-deps: ## Fetch the chart's sub-charts pinned in Chart.yaml
	@helm repo add otwld https://helm.otwld.com/ --force-update >&2
	@helm dependency update --skip-refresh $(REPO_ROOT)/charts/flux-ai-bridge >&2

# `make template` renders the chart with the minimal `--set` flags that
# satisfy required.values + the GitHub-App config gate. Useful for
# inspecting the rendered Deployment/Secret/etc. without installing.
# Pipe to `less` or `yq` as needed: `make template | yq '.spec.template'`.
.PHONY: template
template: chart-deps ## Render the Helm chart with the minimal required --set flags
	helm template flux-ai-bridge $(REPO_ROOT)/charts/flux-ai-bridge/ \
	  --set credentials.githubApp.appId=1 \
	  --set credentials.githubApp.installationId=1 \
	  --set credentials.githubApp.privateKey=dummy

.PHONY: test
test: ## Run unit tests (go test ./...)
	go test ./...

# Integration test for the Sentinel-backed dedupe Cache: brings up a
# single-node Redis + a Sentinel monitoring it (both with auth) in
# disposable Docker containers, runs the `integration_sentinel`-tagged
# test against them, then tears the containers down.
#
# Uses --network=host (Linux-only) so the test on the host can reach both
# `127.0.0.1:${SENTINEL_TEST_REDIS_PORT}` and
# `127.0.0.1:${SENTINEL_TEST_SENTINEL_PORT}` AND so Sentinel reports the
# master at `127.0.0.1:${SENTINEL_TEST_REDIS_PORT}` (which the test client
# can then connect to). On a docker-bridge network Sentinel would
# advertise the master by its container hostname, which isn't resolvable
# from the host.
#
# Non-standard ports (16379 / 26379) avoid collisions with a host-local
# Redis. Passwords match the defaults baked into
# internal/dedupe/sentinel_integration_test.go — override via env if you
# need to test ACL-style multi-user configs.
SENTINEL_TEST_REDIS_PORT ?= 16379
SENTINEL_TEST_SENTINEL_PORT ?= 26379
SENTINEL_TEST_REDIS_PW ?= fab-redis-pw-integration
SENTINEL_TEST_SENTINEL_PW ?= fab-sentinel-pw-integration
SENTINEL_TEST_REDIS_NAME := fab-test-redis
SENTINEL_TEST_SENTINEL_NAME := fab-test-sentinel
SENTINEL_TEST_CONF_DIR := /tmp/fab-test-sentinel

.PHONY: test-sentinel
test-sentinel: ## Run the Redis Sentinel dedupe integration tests (local stack)
	@command -v docker >/dev/null || { echo "ERROR: docker is required for test-sentinel" >&2; exit 1; }
	@# Tear down any leftovers from a previous interrupted run so we don't
	@# bind onto a container that's running with stale config / passwords.
	@docker rm -f $(SENTINEL_TEST_REDIS_NAME) $(SENTINEL_TEST_SENTINEL_NAME) >/dev/null 2>&1 || true
	@trap '$(MAKE) test-sentinel-down' EXIT INT TERM; \
	  set -e; \
	  echo "==> Starting Redis (password=$(SENTINEL_TEST_REDIS_PW), port=$(SENTINEL_TEST_REDIS_PORT))"; \
	  docker run -d --rm --network=host --name $(SENTINEL_TEST_REDIS_NAME) \
	    redis:7-alpine \
	    redis-server \
	      --port $(SENTINEL_TEST_REDIS_PORT) \
	      --requirepass $(SENTINEL_TEST_REDIS_PW) \
	      --masterauth $(SENTINEL_TEST_REDIS_PW) >/dev/null; \
	  echo "==> Waiting for Redis to accept AUTH"; \
	  for i in $$(seq 1 30); do \
	    if docker exec $(SENTINEL_TEST_REDIS_NAME) redis-cli -p $(SENTINEL_TEST_REDIS_PORT) -a $(SENTINEL_TEST_REDIS_PW) --no-auth-warning ping 2>/dev/null | grep -qx PONG; then \
	      echo "    Redis ready"; \
	      break; \
	    fi; \
	    sleep 0.5; \
	    if [ $$i = 30 ]; then echo "Redis never became ready" >&2; exit 1; fi; \
	  done; \
	  echo "==> Writing sentinel.conf"; \
	  mkdir -p $(SENTINEL_TEST_CONF_DIR); \
	  printf '%s\n' \
	    'port $(SENTINEL_TEST_SENTINEL_PORT)' \
	    'sentinel monitor mymaster 127.0.0.1 $(SENTINEL_TEST_REDIS_PORT) 1' \
	    'sentinel auth-pass mymaster $(SENTINEL_TEST_REDIS_PW)' \
	    'sentinel down-after-milliseconds mymaster 5000' \
	    'sentinel failover-timeout mymaster 10000' \
	    'sentinel parallel-syncs mymaster 1' \
	    'requirepass $(SENTINEL_TEST_SENTINEL_PW)' \
	    'sentinel resolve-hostnames yes' \
	    > $(SENTINEL_TEST_CONF_DIR)/sentinel.conf; \
	  echo "==> Starting Sentinel (password=$(SENTINEL_TEST_SENTINEL_PW), port=$(SENTINEL_TEST_SENTINEL_PORT))"; \
	  docker run -d --rm --network=host --name $(SENTINEL_TEST_SENTINEL_NAME) \
	    -v $(SENTINEL_TEST_CONF_DIR)/sentinel.conf:/etc/redis/sentinel.conf \
	    redis:7-alpine \
	    redis-sentinel /etc/redis/sentinel.conf >/dev/null; \
	  echo "==> Waiting for Sentinel to know the master"; \
	  for i in $$(seq 1 30); do \
	    out=$$(docker exec $(SENTINEL_TEST_SENTINEL_NAME) redis-cli -p $(SENTINEL_TEST_SENTINEL_PORT) -a $(SENTINEL_TEST_SENTINEL_PW) --no-auth-warning sentinel get-master-addr-by-name mymaster 2>/dev/null || true); \
	    if echo "$$out" | grep -q '127.0.0.1'; then \
	      echo "    Sentinel knows master at $$(echo $$out | tr '\n' ' ')"; \
	      break; \
	    fi; \
	    sleep 0.5; \
	    if [ $$i = 30 ]; then echo "Sentinel never discovered the master" >&2; exit 1; fi; \
	  done; \
	  echo "==> Running Go integration test"; \
	  SENTINEL_TEST_SENTINEL_PORT=$(SENTINEL_TEST_SENTINEL_PORT) \
	  SENTINEL_TEST_REDIS_PORT=$(SENTINEL_TEST_REDIS_PORT) \
	  SENTINEL_TEST_REDIS_PASSWORD=$(SENTINEL_TEST_REDIS_PW) \
	  SENTINEL_TEST_SENTINEL_PASSWORD=$(SENTINEL_TEST_SENTINEL_PW) \
	  go test -v -tags integration_sentinel -count=1 -timeout 1m ./internal/dedupe/...

.PHONY: test-sentinel-down
test-sentinel-down: ## Tear down the local Sentinel test stack
	@docker rm -f $(SENTINEL_TEST_REDIS_NAME) $(SENTINEL_TEST_SENTINEL_NAME) >/dev/null 2>&1 || true
	@rm -rf $(SENTINEL_TEST_CONF_DIR) 2>/dev/null || true

.PHONY: vet
vet: ## Run go vet ./...
	go vet ./...

.PHONY: vuln
vuln: ## Report known vulnerabilities in code paths the bridge calls (govulncheck)
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

.PHONY: lint
lint: chart-deps ## Run helm lint on the chart
	helm lint charts/flux-ai-bridge/

# Optional Go linter — same one CI runs. Skip silently if not installed locally
# so `make test-all` doesn't fail on contributors who haven't installed it.
.PHONY: golangci-lint
golangci-lint: ## Run golangci-lint (skipped if not installed)
	@if command -v golangci-lint >/dev/null 2>&1; then \
	  golangci-lint run; \
	else \
	  echo "==> golangci-lint not installed locally (CI runs it); skipping"; \
	fi

# Full pre-PR sanity check: go vet, unit tests, golangci-lint (if installed),
# helm lint, then a complete e2e cycle (cluster setup + bridge install + tests).
# Leaves the cluster running for inspection — `make e2e-down` to remove it.
.PHONY: test-all
test-all: ## Full pre-PR check: vet, unit tests, golangci-lint, helm lint + a full e2e cycle
	@echo "==> [1/6] go vet"
	@$(MAKE) vet
	@echo "==> [2/6] go test (unit)"
	@$(MAKE) test
	@echo "==> [3/6] golangci-lint"
	@$(MAKE) golangci-lint
	@echo "==> [4/6] helm lint"
	@$(MAKE) lint
	@echo "==> [5/6] make e2e-up (provisions kind cluster + installs bridge)"
	@$(MAKE) e2e-up
	@echo "==> [6/6] make e2e (runs Go e2e tests against the cluster)"
	@# Deliberately do NOT teardown the cluster — leave it running so the
	@# operator can `kubectl logs` etc. to investigate. Run `make e2e-down`
	@# manually when you're done.
	@$(MAKE) e2e
	@echo ""
	@echo "==> test-all PASS (cluster left running — \`make e2e-down\` to clean up)"

# `make flux-install` installs the Flux Operator, then Flux through the
# FluxInstance in e2e/manifests/01-flux-instance.yaml, into the kind cluster
# $(CLUSTER_NAME). Shared by e2e-up and ct-install.
.PHONY: flux-install
flux-install: ## Install the Flux Operator and Flux (FluxInstance) into the kind cluster CLUSTER_NAME
	@echo "==> Installing flux-operator $(FLUX_OPERATOR_VERSION) into $(CONTEXT)"
	helm upgrade --install flux-operator \
	  oci://ghcr.io/controlplaneio-fluxcd/charts/flux-operator \
	  --version $(FLUX_OPERATOR_VERSION) \
	  --namespace flux-system \
	  --create-namespace \
	  --wait --timeout 3m \
	  --kube-context $(CONTEXT)
	kubectl --context $(CONTEXT) apply -f $(MANIFESTS)/01-flux-instance.yaml
	@echo "==> Waiting for FluxInstance to be Ready"
	kubectl --context $(CONTEXT) -n flux-system wait \
	  fluxinstance/flux \
	  --for=condition=Ready \
	  --timeout=5m
	@echo "==> Waiting for Flux controllers"
	@for deploy in source-controller kustomize-controller helm-controller notification-controller; do \
	  kubectl --context $(CONTEXT) -n flux-system wait \
	    --for=condition=Available deployment/"$$deploy" \
	    --timeout=5m; \
	done

# `make ct-install` runs `ct install` the way the helm-ci workflow does, against
# an existing kind cluster (create one with
# `kind create cluster --name $(CT_CLUSTER_NAME) --config .github/kind-config.yaml`):
#   1. builds the bridge image from the working tree and loads it into the
#      cluster (the chart then runs the code it ships with);
#   2. installs Flux (flux-install) and a podinfo Kustomization in
#      $(CT_NAMESPACE), so the chart's Alert has events to forward;
#   3. creates the Secrets that ci/extra-env-values.yaml expects to exist, with
#      dummy GitHub App credentials (the bridge starts without reaching GitHub);
#   4. installs the chart once per ci/*-values.yaml into $(CT_NAMESPACE) and
#      waits until every resource is ready. --set gives each install the local
#      image and the dummy App key.
# ct has no context flag, so it gets a kubeconfig holding only this cluster. It
# also gets its own Helm repository list: ct adds the repos from .github/ct.yaml
# and fails when one of those names already points elsewhere.
.PHONY: ct-install
ct-install: ## Install the chart with ct (once per ci/*-values.yaml) into the kind cluster CT_CLUSTER_NAME
	@for cmd in kind kubectl helm docker ct openssl; do \
	  command -v "$$cmd" >/dev/null 2>&1 || { echo "ERROR: $$cmd not found in PATH" >&2; exit 1; }; \
	done
	docker build -t $(CT_IMAGE_REPO):$(CT_IMAGE_TAG) $(REPO_ROOT)
	kind load docker-image $(CT_IMAGE_REPO):$(CT_IMAGE_TAG) --name $(CT_CLUSTER_NAME)
	@$(MAKE) --no-print-directory flux-install CLUSTER_NAME=$(CT_CLUSTER_NAME)
	@if [ ! -f "$(MOCK_APP_KEY)" ]; then \
	  echo "==> Generating throwaway GitHub App key ($(MOCK_APP_KEY))"; \
	  mkdir -p "$(dir $(MOCK_APP_KEY))"; \
	  openssl genrsa -out "$(MOCK_APP_KEY)" 2048 2>/dev/null; \
	fi
	@echo "==> Preparing namespace $(CT_NAMESPACE)"
	kubectl --context $(CT_CONTEXT) create namespace $(CT_NAMESPACE) --dry-run=client -o yaml \
	  | kubectl --context $(CT_CONTEXT) apply -f -
	kubectl --context $(CT_CONTEXT) -n $(CT_NAMESPACE) create secret generic flux-ai-bridge \
	  --from-literal=GITHUB_APP_ID=1 \
	  --from-literal=GITHUB_APP_INSTALLATION_ID=1 \
	  --from-file=GITHUB_APP_PRIVATE_KEY="$(MOCK_APP_KEY)" \
	  --dry-run=client -o yaml \
	  | kubectl --context $(CT_CONTEXT) apply -f -
	kubectl --context $(CT_CONTEXT) -n $(CT_NAMESPACE) create secret generic flux-ai-bridge-llm \
	  --from-literal=api-key=unused \
	  --dry-run=client -o yaml \
	  | kubectl --context $(CT_CONTEXT) apply -f -
	@echo "==> Applying the podinfo Kustomization ($(CT_GIT_URL) $(CT_GIT_REF))"
	@printf '%s\n' \
	  'apiVersion: source.toolkit.fluxcd.io/v1' \
	  'kind: GitRepository' \
	  'metadata:' \
	  '  name: flux-ai-bridge' \
	  '  namespace: $(CT_NAMESPACE)' \
	  'spec:' \
	  '  interval: 10m' \
	  '  url: $(CT_GIT_URL)' \
	  '  ref:' \
	  '    name: $(CT_GIT_REF)' \
	  '---' \
	  'apiVersion: kustomize.toolkit.fluxcd.io/v1' \
	  'kind: Kustomization' \
	  'metadata:' \
	  '  name: podinfo' \
	  '  namespace: $(CT_NAMESPACE)' \
	  'spec:' \
	  '  interval: 10m' \
	  '  path: ./e2e/podinfo-kustomize' \
	  '  prune: true' \
	  '  sourceRef:' \
	  '    kind: GitRepository' \
	  '    name: flux-ai-bridge' \
	  | kubectl --context $(CT_CONTEXT) apply -f -
	kubectl --context $(CT_CONTEXT) -n $(CT_NAMESPACE) wait \
	  kustomization/podinfo \
	  --for=condition=Ready \
	  --timeout=5m \
	|| { kubectl --context $(CT_CONTEXT) -n $(CT_NAMESPACE) get gitrepository,kustomization \
	       -o custom-columns='KIND:.kind,NAME:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,MESSAGE:.status.conditions[?(@.type=="Ready")].message'; \
	     exit 1; }
	@echo "==> Running ct install"
	@tmp=$$(mktemp -d "$${TMPDIR:-/tmp}/ct-install.XXXXXX"); \
	trap 'rm -rf "$$tmp"' EXIT; \
	kind get kubeconfig --name $(CT_CLUSTER_NAME) > "$$tmp/kubeconfig"; \
	cd $(REPO_ROOT) && KUBECONFIG="$$tmp/kubeconfig" \
	  HELM_REPOSITORY_CONFIG="$$tmp/repositories.yaml" \
	  HELM_REPOSITORY_CACHE="$$tmp/repository" \
	  ct install \
	  --config .github/ct.yaml \
	  --charts charts/flux-ai-bridge \
	  --namespace $(CT_NAMESPACE) \
	  --helm-extra-set-args "--set=image.repository=$(CT_IMAGE_REPO) --set=image.tag=$(CT_IMAGE_TAG) --set=image.pullPolicy=Never --set=credentials.githubApp.appId=1 --set=credentials.githubApp.installationId=1 --set-file=credentials.githubApp.privateKey=$(MOCK_APP_KEY)"

# Renders e2e/manifests/03-flux-ai-bridge.yaml.tpl for the current mode
# (E2E_REAL_GITHUB, E2E_LLM, E2E_DEDUPE_PERSISTENCE) and applies it. Shared
# by e2e-up and e2e-update, so a new mode or template variable is added once.
define E2E_RENDER_BRIDGE
@set -e; \
if [ "$$E2E_REAL_GITHUB" = "1" ]; then \
  if [ -z "$$GITHUB_APP_ID" ] || [ -z "$$GITHUB_APP_INSTALLATION_ID" ] || [ -z "$$GITHUB_APP_PRIVATE_KEY" ]; then \
    echo "ERROR: real-GitHub mode requires all of GITHUB_APP_ID + GITHUB_APP_INSTALLATION_ID + GITHUB_APP_PRIVATE_KEY (App auth only — PAT support was removed)." >&2; \
    exit 1; \
  fi; \
  if [ -z "$$GITHUB_REPO" ] && [ -n "$$GITHUB_USER" ] && [ -n "$$GH_REPO" ]; then \
    GITHUB_REPO="$$GITHUB_USER/$$GH_REPO"; \
  fi; \
  E2E_GITHUB_REPO="$$GITHUB_REPO"; \
  E2E_GITHUB_API_URL=""; \
  E2E_GITHUB_API_ALLOW_INSECURE="false"; \
  E2E_PODINFO_GIT_URL="https://github.com/$$GITHUB_REPO.git"; \
  E2E_PODINFO_GIT_REF="$${E2E_PODINFO_GIT_REF:-main}"; \
  E2E_PODINFO_GIT_PROVIDER="github"; \
  E2E_PODINFO_PATH="$${E2E_PODINFO_PATH:-./e2e/podinfo-kustomize}"; \
else \
  E2E_GITHUB_REPO="test-owner/test-repo"; \
  E2E_GITHUB_API_URL="http://mock-github.mock.svc.cluster.local/"; \
  E2E_GITHUB_API_ALLOW_INSECURE="true"; \
  E2E_PODINFO_GIT_URL="http://git-server.git-server.svc.cluster.local/flux-ai-bridge.git"; \
  E2E_PODINFO_GIT_REF="main"; \
  E2E_PODINFO_GIT_PROVIDER="generic"; \
  E2E_PODINFO_PATH="./e2e/podinfo-kustomize"; \
fi; \
case "$$E2E_LLM" in \
  ollama) \
    E2E_LLM_PROVIDER="openai-compatible"; \
    E2E_LLM_MODEL="$${E2E_LLM_MODEL:-$(OLLAMA_MODEL)}"; \
    E2E_ANTHROPIC_API_KEY="unused-with-ollama"; \
    E2E_ANTHROPIC_BASE_URL=""; \
    E2E_OPENAI_BASE_URL=""; \
    E2E_OPENAI_API_KEY="unused-with-ollama"; \
    E2E_OLLAMA_ENABLED="true" ;; \
  azure) \
    E2E_LLM_PROVIDER="openai-compatible"; \
    E2E_LLM_MODEL="$${E2E_LLM_MODEL:-gpt-4o-mini}"; \
    E2E_ANTHROPIC_API_KEY="unused-with-azure"; \
    E2E_ANTHROPIC_BASE_URL=""; \
    E2E_OPENAI_BASE_URL="$${OPENAI_BASE_URL:-$${E2E_OPENAI_BASE_URL:-}}"; \
    E2E_OPENAI_API_KEY="$$OPENAI_API_KEY"; \
    E2E_OLLAMA_ENABLED="false" ;; \
  none) \
    E2E_LLM_PROVIDER="none"; \
    E2E_LLM_MODEL=""; \
    E2E_ANTHROPIC_API_KEY=""; \
    E2E_ANTHROPIC_BASE_URL=""; \
    E2E_OPENAI_BASE_URL=""; \
    E2E_OPENAI_API_KEY=""; \
    E2E_OLLAMA_ENABLED="false" ;; \
  *) \
    E2E_LLM_PROVIDER="claude"; \
    E2E_LLM_MODEL=""; \
    E2E_ANTHROPIC_API_KEY="e2e-fake-anthropic-key"; \
    E2E_ANTHROPIC_BASE_URL="http://mock-llm.mock.svc.cluster.local"; \
    E2E_OPENAI_BASE_URL=""; \
    E2E_OPENAI_API_KEY=""; \
    E2E_OLLAMA_ENABLED="false" ;; \
esac; \
E2E_LOG_LEVEL="$${E2E_LOG_LEVEL:-info}"; \
E2E_KEEP_COMMENT_HISTORY="$${E2E_KEEP_COMMENT_HISTORY:-false}"; \
if [ "$$E2E_REAL_GITHUB" = "1" ]; then \
  E2E_GITHUB_APP_ID="$$GITHUB_APP_ID"; \
  E2E_GITHUB_APP_INSTALLATION_ID="$$GITHUB_APP_INSTALLATION_ID"; \
  E2E_GITHUB_APP_PRIVATE_KEY_INDENTED=$$(printf '%s\n' "$$GITHUB_APP_PRIVATE_KEY" | sed 's/^/    /'); \
else \
  if [ ! -f "$(REPO_ROOT)/e2e/fixtures/mock-app-key.pem" ]; then \
    echo "==> Generating throwaway mock-github App key (e2e/fixtures/mock-app-key.pem)"; \
    mkdir -p "$(REPO_ROOT)/e2e/fixtures"; \
    openssl genrsa -out "$(REPO_ROOT)/e2e/fixtures/mock-app-key.pem" 2048 2>/dev/null; \
  fi; \
  E2E_GITHUB_APP_ID="1"; \
  E2E_GITHUB_APP_INSTALLATION_ID="1"; \
  E2E_GITHUB_APP_PRIVATE_KEY_INDENTED=$$(sed 's/^/    /' "$(REPO_ROOT)/e2e/fixtures/mock-app-key.pem"); \
fi; \
if [ "$$E2E_REAL_GITHUB" = "1" ]; then \
  E2E_PODINFO_GIT_AUTH_SECRET=$$(printf 'apiVersion: v1\nkind: Secret\nmetadata:\n  name: flux-ai-bridge-e2e-podinfo-git-creds\n  namespace: flux-system\ntype: Opaque\nstringData:\n  githubAppID: "%s"\n  githubAppInstallationID: "%s"\n  githubAppPrivateKey: |\n%s\n---' "$$E2E_GITHUB_APP_ID" "$$E2E_GITHUB_APP_INSTALLATION_ID" "$$E2E_GITHUB_APP_PRIVATE_KEY_INDENTED"); \
  E2E_PODINFO_GIT_SECRETREF=$$(printf '  secretRef:\n    name: flux-ai-bridge-e2e-podinfo-git-creds'); \
else \
  E2E_PODINFO_GIT_AUTH_SECRET=""; \
  E2E_PODINFO_GIT_SECRETREF=""; \
fi; \
if [ "$$E2E_DEDUPE_PERSISTENCE" = "1" ]; then \
  E2E_VALKEY_ENABLED="false"; \
  E2E_DEDUPE_PERSISTENCE_ENABLED="true"; \
  E2E_DEDUPE_SENTINEL_ENABLED="true"; \
  E2E_DEDUPE_SENTINEL_ADDRS="valkey-node-0.valkey-headless.valkey.svc.cluster.local:26379,valkey-node-1.valkey-headless.valkey.svc.cluster.local:26379,valkey-node-2.valkey-headless.valkey.svc.cluster.local:26379"; \
  E2E_DEDUPE_SENTINEL_MASTER_NAME="myprimary"; \
  E2E_DEDUPE_PERSISTENCE_USER="bridge"; \
  E2E_DEDUPE_SENTINEL_USER="bridge"; \
  E2E_DEDUPE_PERSISTENCE_EXISTING_SECRET="flux-ai-bridge-dedupe"; \
  E2E_DEDUPE_PERSISTENCE_PASSWORD_KEY="bridge-password"; \
  E2E_DEDUPE_SENTINEL_PASSWORD_KEY="bridge-password"; \
  E2E_WEBHOOK_MAX_CONCURRENT=""; \
else \
  E2E_VALKEY_ENABLED="false"; \
  E2E_DEDUPE_PERSISTENCE_ENABLED="false"; \
  E2E_DEDUPE_SENTINEL_ENABLED="false"; \
  E2E_DEDUPE_SENTINEL_ADDRS=""; \
  E2E_DEDUPE_SENTINEL_MASTER_NAME=""; \
  E2E_DEDUPE_PERSISTENCE_USER=""; \
  E2E_DEDUPE_SENTINEL_USER=""; \
  E2E_DEDUPE_PERSISTENCE_EXISTING_SECRET=""; \
  E2E_DEDUPE_PERSISTENCE_PASSWORD_KEY=""; \
  E2E_DEDUPE_SENTINEL_PASSWORD_KEY=""; \
  E2E_WEBHOOK_MAX_CONCURRENT=""; \
fi; \
export E2E_GITHUB_REPO E2E_GITHUB_API_URL E2E_GITHUB_API_ALLOW_INSECURE \
       E2E_LLM_PROVIDER E2E_LLM_MODEL E2E_ANTHROPIC_API_KEY \
       E2E_ANTHROPIC_BASE_URL E2E_OPENAI_BASE_URL E2E_OPENAI_API_KEY \
       E2E_OLLAMA_ENABLED E2E_LOG_LEVEL E2E_KEEP_COMMENT_HISTORY \
       E2E_VALKEY_ENABLED E2E_DEDUPE_PERSISTENCE_ENABLED \
       E2E_DEDUPE_SENTINEL_ENABLED E2E_DEDUPE_SENTINEL_ADDRS \
       E2E_DEDUPE_SENTINEL_MASTER_NAME \
       E2E_DEDUPE_PERSISTENCE_USER E2E_DEDUPE_SENTINEL_USER \
       E2E_DEDUPE_PERSISTENCE_EXISTING_SECRET E2E_DEDUPE_PERSISTENCE_PASSWORD_KEY \
       E2E_DEDUPE_SENTINEL_PASSWORD_KEY E2E_WEBHOOK_MAX_CONCURRENT \
       E2E_GITHUB_APP_ID E2E_GITHUB_APP_INSTALLATION_ID E2E_GITHUB_APP_PRIVATE_KEY_INDENTED \
       E2E_PODINFO_GIT_URL E2E_PODINFO_GIT_REF E2E_PODINFO_GIT_PROVIDER E2E_PODINFO_PATH \
       E2E_PODINFO_GIT_AUTH_SECRET E2E_PODINFO_GIT_SECRETREF; \
envsubst < $(MANIFESTS)/03-flux-ai-bridge.yaml.tpl \
  | kubectl --context $(CONTEXT) apply -f -
endef

.PHONY: e2e-up
e2e-up: ## Provision the kind e2e cluster and install the bridge + dependencies
	@for cmd in kind kubectl helm docker git envsubst jq; do \
	  command -v "$$cmd" >/dev/null 2>&1 || { echo "ERROR: $$cmd not found in PATH" >&2; exit 1; }; \
	done
	@if [ "$$E2E_REAL_GITHUB" = "1" ]; then \
	  if [ -z "$$GITHUB_APP_ID" ] || [ -z "$$GITHUB_APP_INSTALLATION_ID" ] || [ -z "$$GITHUB_APP_PRIVATE_KEY" ]; then \
	    echo "ERROR: real-GitHub mode requires all of GITHUB_APP_ID + GITHUB_APP_INSTALLATION_ID + GITHUB_APP_PRIVATE_KEY. The bridge is GitHub App only — PAT auth was removed. See docs/e2e-real-github.md." >&2; \
	    exit 1; \
	  fi; \
	  if [ -z "$$GITHUB_REPO" ] && [ -n "$$GITHUB_USER" ] && [ -n "$$GH_REPO" ]; then \
	    GITHUB_REPO="$$GITHUB_USER/$$GH_REPO"; \
	  fi; \
	  case "$$GITHUB_REPO" in \
	    */*) : ;; \
	    *) echo "ERROR: GITHUB_REPO must be in 'owner/repo' form (or set GITHUB_USER + GH_REPO instead). Got: '$$GITHUB_REPO'" >&2; exit 1 ;; \
	  esac; \
	  echo "==> Real GitHub mode: repo=$$GITHUB_REPO auth=App"; \
	else \
	  echo "==> Mock GitHub mode (E2E_REAL_GITHUB=1 to target a real repo)"; \
	fi
	@# Mock-mode bridge still needs a syntactically-valid RSA key — the
	@# ghinstallation library parses the PEM eagerly even when no real
	@# JWTs are ever issued. Generate a throwaway key under
	@# e2e/fixtures/mock-app-key.pem on first run; reuse it on subsequent
	@# runs. The file is gitignored (see .gitignore).
	@if [ "$$E2E_REAL_GITHUB" != "1" ] && [ ! -f "$(REPO_ROOT)/e2e/fixtures/mock-app-key.pem" ]; then \
	  echo "==> Generating throwaway mock-github App key (e2e/fixtures/mock-app-key.pem)"; \
	  mkdir -p "$(REPO_ROOT)/e2e/fixtures"; \
	  openssl genrsa -out "$(REPO_ROOT)/e2e/fixtures/mock-app-key.pem" 2048 2>/dev/null; \
	fi
	@case "$$E2E_LLM" in \
	  ollama) \
	    if [ -z "$(OLLAMA_VERSION)" ]; then \
	      echo "ERROR: could not resolve Ollama image tag from $(REPO_ROOT)/charts/flux-ai-bridge/charts/ollama-*.tgz" >&2; \
	      echo "       Run 'make chart-deps' to fetch the sub-charts pinned in Chart.yaml, then retry." >&2; \
	      exit 1; \
	    fi; \
	    echo "==> Real LLM mode: ollama serving $(OLLAMA_MODEL)" ;; \
	  azure) \
	    : "$${OPENAI_API_KEY:?required when E2E_LLM=azure — see docs/e2e-real-llm.md}"; \
	    echo "==> Real LLM mode: Azure AI Foundry ($${E2E_LLM_MODEL:-gpt-4o-mini})" ;; \
	  none) \
	    echo "==> No-LLM mode: bridge posts kstatus output only (config.llm.provider=none)" ;; \
	  "") \
	    echo "==> Mock LLM mode (E2E_LLM=ollama|azure|none to switch)" ;; \
	  *) \
	    echo "ERROR: unknown E2E_LLM=$$E2E_LLM (valid: ollama, azure, none, or empty)" >&2; \
	    exit 1 ;; \
	esac
	docker build -t $(E2E_IMAGE) $(REPO_ROOT)
	@echo "==> Ensuring Ollama host cache dir exists: $(OLLAMA_CACHE_DIR)"
	@mkdir -p "$(OLLAMA_CACHE_DIR)" && chmod 0777 "$(OLLAMA_CACHE_DIR)"
	@echo "==> Creating kind cluster: $(CLUSTER_NAME)"
	@printf '%s\n' \
	  'kind: Cluster' \
	  'apiVersion: kind.x-k8s.io/v1alpha4' \
	  'nodes:' \
	  '  - role: control-plane' \
	  '    extraMounts:' \
	  '      - hostPath: $(REPO_ROOT)' \
	  '        containerPath: /flux-ai-bridge-src' \
	  '        readOnly: true' \
	  '      - hostPath: $(OLLAMA_CACHE_DIR)' \
	  '        containerPath: /var/lib/ollama-cache' \
	  | kind create cluster --name $(CLUSTER_NAME) --config -
	kind load docker-image $(E2E_IMAGE) --name $(CLUSTER_NAME)
	@# Pre-cache the Ollama image so the kubelet skips the network pull on
	@# every cluster recreation. Three-step dance:
	@#   1. docker pull --platform fetches the platform-specific image.
	@#   2. docker create + commit re-packs as a single-platform local image,
	@#      dropping the multi-arch manifest list and attestation manifests
	@#      that ctr cannot import. Without this step ctr fails with
	@#      "ctr: content digest sha256:...: not found" because attestation
	@#      blobs travel by reference only and aren't pulled locally.
	@#   3. docker save | kind load image-archive lands the flattened image
	@#      in containerd's k8s.io namespace; kubelet then finds it locally.
	@if [ "$$E2E_LLM" = "ollama" ]; then \
	  platform="linux/$$(docker version --format '{{.Server.Arch}}')"; \
	  echo "==> Pre-loading Ollama image into kind node: $(OLLAMA_IMAGE) ($$platform)"; \
	  docker pull --platform "$$platform" $(OLLAMA_IMAGE); \
	  echo "    Flattening multi-arch image to single-platform via docker commit"; \
	  cid=$$(docker create --platform "$$platform" $(OLLAMA_IMAGE)); \
	  docker commit "$$cid" $(OLLAMA_IMAGE) >/dev/null; \
	  docker rm "$$cid" >/dev/null; \
	  tmp=$$(mktemp "$${TMPDIR:-/tmp}/ollama-image.XXXXXX"); \
	  trap 'rm -f "$$tmp"' EXIT; \
	  docker save $(OLLAMA_IMAGE) -o "$$tmp"; \
	  kind load image-archive "$$tmp" --name $(CLUSTER_NAME); \
	fi
	@$(MAKE) --no-print-directory flux-install
	@echo "==> Applying manifests"
	kubectl --context $(CONTEXT) apply -f $(MANIFESTS)/00-git-server.yaml
	@# flux-operator-mcp is now a sub-chart of the bridge (enabled by default
	@# in charts/flux-ai-bridge/values.yaml), so no separate helm install is
	@# needed here. Keeping this comment as a tombstone — if e2e ever needs
	@# a pre-existing MCP server (e.g. to test the disabled-subchart path),
	@# re-instate the helm upgrade --install here AND set
	@# flux-operator-mcp.enabled=false in 03-flux-ai-bridge.yaml.tpl.
	@if [ "$$E2E_LLM" = "ollama" ]; then \
	  echo "==> Provisioning Ollama model cache PV (hostPath → $(OLLAMA_CACHE_DIR))"; \
	  kubectl --context $(CONTEXT) apply -f $(MANIFESTS)/05-ollama-cache.yaml; \
	  echo "    Ollama will be installed by the bridge HelmRelease via the chart's"; \
	  echo "    ollama-helm dependency (ollama.enabled=true in the rendered values)."; \
	fi
	@# Mock servers — applied per-side so the unused mock isn't burning
	@# pod-slots and cluttering `kubectl get pods`. The common file
	@# (Namespace + scripts ConfigMap) is applied whenever at least one
	@# mock is going to live; the LLM and GitHub Deployment+Service pairs
	@# each apply only when their corresponding real-* env is unset.
	@if [ "$$E2E_REAL_GITHUB" = "1" ] && [ -n "$$E2E_LLM" ]; then \
	  echo "==> Skipping mock servers (real GitHub + real LLM)"; \
	else \
	  echo "==> Applying mock common (Namespace + ConfigMap)"; \
	  kubectl --context $(CONTEXT) apply -f $(MANIFESTS)/02-mock-common.yaml; \
	  if [ -z "$$E2E_LLM" ]; then \
	    echo "==> Applying mock-llm"; \
	    kubectl --context $(CONTEXT) apply -f $(MANIFESTS)/02-mock-llm.yaml; \
	  else \
	    echo "==> Skipping mock-llm (E2E_LLM=$$E2E_LLM)"; \
	  fi; \
	  if [ "$$E2E_REAL_GITHUB" != "1" ]; then \
	    echo "==> Applying mock-github"; \
	    kubectl --context $(CONTEXT) apply -f $(MANIFESTS)/02-mock-github.yaml; \
	  else \
	    echo "==> Skipping mock-github (E2E_REAL_GITHUB=1)"; \
	  fi; \
	fi
	@# Bitnami valkey with Sentinel + auth. The bridge HelmRelease below
	@# references the chart-managed `valkey` Secret (key valkey-password),
	@# so the chart MUST be installed before the bridge pod starts or it
	@# crash-loops on a missing Secret reference. We use helm CLI directly
	@# rather than a Flux HelmRelease to keep the e2e bootstrap simple —
	@# this is test infrastructure, not GitOps under test.
	@if [ "$$E2E_DEDUPE_PERSISTENCE" = "1" ]; then \
	  $(MAKE) e2e-valkey-setup; \
	fi
	@# Build the per-mode env vars. GitHub mock/real and LLM mock/real toggle
	@# independently. GitHub auth is App-only (PAT support was removed);
	@# real mode reads the operator's shell GITHUB_APP_* vars, mock mode
	@# uses a generated throwaway PEM under e2e/fixtures.
	$(E2E_RENDER_BRIDGE)
	@echo "==> Waiting for git-server"
	kubectl --context $(CONTEXT) -n git-server wait \
	  --for=condition=Available deployment/git-server \
	  --timeout=2m
	@# Wait only for mock Deployments that were actually applied above.
	@# Each side toggles independently: mock-llm exists iff E2E_LLM is
	@# empty, mock-github exists iff E2E_REAL_GITHUB != 1.
	@deploys=""; \
	if [ -z "$$E2E_LLM" ]; then deploys="$$deploys mock-llm"; fi; \
	if [ "$$E2E_REAL_GITHUB" != "1" ]; then deploys="$$deploys mock-github"; fi; \
	if [ -z "$$deploys" ]; then \
	  echo "==> Skipping mock-server wait (none deployed)"; \
	else \
	  echo "==> Waiting for mock servers:$$deploys"; \
	  for deploy in $$deploys; do \
	    kubectl --context $(CONTEXT) -n mock wait \
	      --for=condition=Available deployment/"$$deploy" \
	      --timeout=2m; \
	  done; \
	fi
	@# E2E_LLM=ollama means the bundled Ollama sub-chart installs. On a cold
	@# host cache the qwen2.5-coder:1.5b model (~1 GB) gets pulled during
	@# the helm install before the release reports Ready. Subsequent runs
	@# reuse the host-backed PV (e2e/manifests/05-ollama-cache.yaml).
	@# Don't read E2E_OLLAMA_ENABLED — that's set/exported inside the
	@# earlier recipe line and is gone by the time this fresh shell runs.
	@status=""; \
	if [ "$$E2E_LLM" = "ollama" ]; then \
	  iters=240; mins=20; \
	else \
	  iters=72; mins=6; \
	fi; \
	echo "==> Waiting for HelmRelease flux-ai-bridge to be ready (max $${mins}m)..."; \
	for i in $$(seq 1 $$iters); do \
	  status=$$(kubectl --context $(CONTEXT) -n flux-system get helmrelease flux-ai-bridge \
	    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo ""); \
	  if [ "$$status" = "True" ]; then \
	    echo "    HelmRelease ready."; \
	    break; \
	  fi; \
	  printf "    (%d/%d) status=%s -- waiting 5s...\n" "$$i" "$$iters" "$${status:-Pending}"; \
	  sleep 5; \
	done; \
	if [ "$${status:-}" != "True" ]; then \
	  echo "ERROR: HelmRelease did not become Ready within $${mins} minutes" >&2; \
	  kubectl --context $(CONTEXT) -n flux-system describe helmrelease flux-ai-bridge >&2; \
	  kubectl --context $(CONTEXT) -n flux-system describe gitrepository flux-ai-bridge-chart >&2; \
	  exit 1; \
	fi
	@# The podinfo GitRepository + Kustomization were applied as part of the
	@# bridge HelmRelease manifest (03-flux-ai-bridge.yaml.tpl).
	@echo "==> Waiting for test Kustomization to reconcile once (so it has an inventory)"
	@status=""; \
	for i in $$(seq 1 60); do \
	  status=$$(kubectl --context $(CONTEXT) -n flux-system get kustomization podinfo \
	    -o jsonpath='{.status.lastAppliedRevision}' 2>/dev/null || echo ""); \
	  if [ -n "$$status" ]; then \
	    echo "    Kustomization reconciled at revision $$status"; \
	    break; \
	  fi; \
	  printf "    (%d/60) waiting for first reconciliation...\n" "$$i"; \
	  sleep 5; \
	done; \
	if [ -z "$${status:-}" ]; then \
	  echo "ERROR: Kustomization podinfo did not reconcile within 5 minutes" >&2; \
	  kubectl --context $(CONTEXT) -n flux-system describe kustomization podinfo >&2; \
	  exit 1; \
	fi
	@echo ""
	@echo "==> E2E cluster ready. Run: make e2e"

.PHONY: e2e
e2e: ## Run the e2e test suite against the running cluster
	@# Restore podinfo to source-of-truth state before testing. If a prior
	@# `make e2e-broken` suspended the Kustomization and patched the in-
	@# cluster HelmRelease, this unsuspends + force-reconciles so
	@# kustomize-controller drift-corrects the HelmRelease back to source.
	@# Idempotent — when nothing is broken, the patch + annotate are
	@# no-ops and the waits return quickly. A failure stops here, so the
	@# healthy assertions never run against broken pods. The Kustomization
	@# keeps a stale Ready=True while suspended, so wait for it to handle
	@# this reconcile request first, then for the podinfo HelmRelease (the
	@# Kustomization doesn't wait for it) and its Deployment.
	@echo "==> Ensuring podinfo Kustomization is unsuspended"
	@kubectl --context $(CONTEXT) -n flux-system patch kustomization/podinfo \
	  --type=merge -p '{"spec":{"suspend":false}}' >/dev/null
	@echo "==> Forcing reconcile to restore source state"
	@set -e; ts=$$(date +%s); \
	kubectl --context $(CONTEXT) -n flux-system annotate --overwrite \
	  kustomization/podinfo reconcile.fluxcd.io/requestedAt=$$ts >/dev/null; \
	echo "==> Waiting for podinfo Kustomization to reconcile and be Ready (max 2m each)"; \
	kubectl --context $(CONTEXT) -n flux-system wait kustomization/podinfo \
	  --for=jsonpath='{.status.lastHandledReconcileAt}'=$$ts --timeout=2m; \
	kubectl --context $(CONTEXT) -n flux-system wait kustomization/podinfo \
	  --for=condition=Ready --timeout=2m
	@echo "==> Waiting for the podinfo HelmRelease and Deployment (max 5m + 2m)"
	@kubectl --context $(CONTEXT) -n podinfo wait helmrelease/podinfo \
	  --for=condition=Ready --timeout=5m
	@kubectl --context $(CONTEXT) -n podinfo rollout status deployment/podinfo --timeout=2m
	@# -count=1 disables Go's test result cache, otherwise repeated `make e2e`
	@# returns "(cached)" without sending any traffic to the live cluster.
	go test -tags e2e -count=1 -timeout 5m ./e2e/...

.PHONY: e2e-broken
e2e-broken: ## Drive podinfo into a failed state (suspend + patch) to exercise the failure path
	@# Drive podinfo into a real failed state by patching the live
	@# HelmRelease in the cluster — no on-disk file edits, so there's no
	@# risk of accidentally committing the broken state.
	@#
	@# Flow:
	@#   1. Suspend the podinfo Kustomization so kustomize-controller's
	@#      drift-correction doesn't immediately revert the patch.
	@#   2. Patch the HelmRelease with a postRenderer that overwrites the
	@#      podinfo container args with `[--foo=bar]`. podinfo refuses to
	@#      start with that argv (unknown flag + no `--port`), so pods
	@#      CrashLoopBackOff.
	@#   3. Force a HelmRelease reconcile so the postRenderer takes effect
	@#      without waiting for the 5m interval.
	@#   4. Wait until pods are visibly unhealthy, then run the test suite.
	@#
	@# `make e2e` reverses everything: unsuspend + force-reconcile →
	@# kustomize-controller re-applies source → postRenderer disappears →
	@# pods recover.
	@echo "==> Suspending podinfo Kustomization (paused drift correction)"
	kubectl --context $(CONTEXT) -n flux-system patch kustomization/podinfo \
	  --type=merge -p '{"spec":{"suspend":true}}'
	@echo "==> Patching HelmRelease with a postRenderer that adds --foo=bar"
	kubectl --context $(CONTEXT) -n podinfo patch helmrelease/podinfo --type=merge -p \
	  '{"spec":{"postRenderers":[{"kustomize":{"patches":[{"target":{"kind":"Deployment","name":"podinfo"},"patch":"apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: podinfo\nspec:\n  template:\n    spec:\n      containers:\n        - name: podinfo\n          args:\n            - --foo=bar"}]}}]}}'
	@echo "==> Forcing HelmRelease reconcile"
	kubectl --context $(CONTEXT) -n podinfo annotate --overwrite \
	  helmrelease/podinfo reconcile.fluxcd.io/requestedAt=$$(date +%s)
	@echo "==> Waiting for podinfo pods to enter CrashLoopBackOff (max 90s)"
	@deadline=$$(( $$(date +%s) + 90 )); broken=""; \
	while [ $$(date +%s) -lt $$deadline ]; do \
	  reasons=$$(kubectl --context $(CONTEXT) -n podinfo get pods \
	    -l app.kubernetes.io/name=podinfo \
	    -o jsonpath='{.items[*].status.containerStatuses[*].state.waiting.reason}' 2>/dev/null); \
	  case "$$reasons" in \
	    *CrashLoopBackOff*|*Error*) \
	      echo "    Podinfo pods unhealthy ($$reasons) — broken state achieved."; \
	      broken=1; \
	      break ;; \
	  esac; \
	  sleep 5; \
	done; \
	if [ -z "$$broken" ]; then \
	  echo "ERROR: no podinfo pod reached CrashLoopBackOff or Error within 90s" >&2; \
	  kubectl --context $(CONTEXT) -n podinfo get pods -l app.kubernetes.io/name=podinfo -o wide >&2 || true; \
	  kubectl --context $(CONTEXT) -n podinfo describe helmrelease/podinfo >&2 || true; \
	  exit 1; \
	fi
	@echo "==> Running e2e tests against broken cluster state (comments must report ❌)"
	E2E_EXPECT_BROKEN=1 go test -tags e2e -count=1 -timeout 15m ./e2e/...
	@echo ""
	@echo "==> e2e-broken done. podinfo Kustomization is suspended + patched in-cluster."
	@echo "==> Run 'make e2e' to unsuspend and restore via drift-correction."

.PHONY: e2e-update
e2e-update: ## Rebuild the bridge image and rollout-restart it in the e2e cluster (clears in-memory dedupe)
	@# Rebuild the bridge image and redeploy into the running e2e cluster.
	@# Skips cluster creation and the slow Flux/MCP/Ollama bootstrap; the
	@# typical inner-loop after editing Go or chart code is:
	@#   make e2e-update && make e2e
	@#
	@# What this does, and why each step is needed:
	@#   1. Build the image. Tag stays $(E2E_IMAGE); only digest changes.
	@#   2. kind load the new image into the node's containerd store.
	@#   3. Restart the git-server pod. Its init container re-tars the
	@#      bind-mounted repo into a fresh git history, so any chart edits
	@#      become visible to Flux source-controller.
	@#   4. Annotate GitRepository, HelmChart and HelmRelease with a reconcile
	@#      timestamp, one after the other, and wait until each has handled
	@#      it, so the release runs the new chart before step 5 restarts it
	@#      (instead of waiting for their poll intervals, 30s / 1m). Fail
	@#      unless each reached the git-server's snapshot commit: the source
	@#      artifact, the chart built from it, and a Ready HelmRelease whose
	@#      latest release deployed that chart version.
	@#   5. Force a deployment rollout. The image tag is unchanged and the
	@#      chart sets pullPolicy=Never, so the kubelet has no signal that
	@#      anything changed unless we tell it to recreate the pods.
	@kind get clusters | grep -qx '$(CLUSTER_NAME)' || { \
	  echo "ERROR: kind cluster $(CLUSTER_NAME) not running. Run 'make e2e-up' first." >&2; \
	  exit 1; \
	}
	docker build -t $(E2E_IMAGE) $(REPO_ROOT)
	kind load docker-image $(E2E_IMAGE) --name $(CLUSTER_NAME)
	@echo "==> Re-snapshotting chart via git-server pod restart"
	kubectl --context $(CONTEXT) -n git-server rollout restart deployment/git-server
	kubectl --context $(CONTEXT) -n git-server rollout status deployment/git-server --timeout=2m
	@# Re-apply (idempotent) the Bitnami Valkey chart when persistence is on.
	@# Lets `make e2e-update` recover from a state where e2e-up was run
	@# without persistence and the operator now wants to enable it; without
	@# this, the bridge would reference a non-existent `valkey` Secret on
	@# the next roll-out.
	@if [ "$$E2E_DEDUPE_PERSISTENCE" = "1" ]; then \
	  $(MAKE) e2e-valkey-setup; \
	fi
	@echo "==> Re-rendering + re-applying bridge HelmRelease + podinfo source/Kustomization"
	$(E2E_RENDER_BRIDGE)
	@# Reconcile source → chart → release in order and wait for each to
	@# handle the request, so the restart below runs against the Deployment
	@# from the new chart and values, not the one before Flux applied them.
	@# Handling a request isn't succeeding (a failed fetch, build or upgrade
	@# keeps the previous artifact or release), so each step is checked
	@# against the commit the git-server serves: the GitRepository artifact
	@# is that commit, the HelmChart was built from it (reconcileStrategy
	@# Revision appends the SHA's first 12 characters to the chart version),
	@# and the HelmRelease is Ready (kubectl wait also checks the condition's
	@# observedGeneration, so the values are current) with that chart version
	@# as its latest, deployed release. Otherwise stop before the restart.
	@echo "==> Reconciling the chart source, the HelmChart and the bridge HelmRelease"
	@set -e; ts=$$(date +%s); \
	fail() { echo "ERROR: $$2" >&2; \
	  kubectl --context $(CONTEXT) -n "$${1%%/*}" get "$${1#*/}" \
	    -o jsonpath='{range .status.conditions[*]}{.type}={.status}: {.message}{"\n"}{end}' >&2 || true; \
	  exit 1; }; \
	sha=$$(kubectl --context $(CONTEXT) -n git-server exec deployment/git-server -c git-http -- \
	  git -C /serve/flux-ai-bridge.git rev-parse 'e2e-test^{commit}'); \
	[ -n "$$sha" ] || { echo "ERROR: the git-server serves no e2e-test commit" >&2; exit 1; }; \
	short=$$(printf '%.12s' "$$sha"); \
	echo "    git-server snapshot: $$sha"; \
	kubectl --context $(CONTEXT) -n flux-system annotate --overwrite \
	  gitrepository/flux-ai-bridge-chart reconcile.fluxcd.io/requestedAt=$$ts; \
	kubectl --context $(CONTEXT) -n flux-system wait gitrepository/flux-ai-bridge-chart \
	  --for=jsonpath='{.status.lastHandledReconcileAt}'=$$ts --timeout=2m; \
	gitrev=$$(kubectl --context $(CONTEXT) -n flux-system get gitrepository/flux-ai-bridge-chart \
	  -o jsonpath='{.status.artifact.revision}'); \
	case "$$gitrev" in *":$$sha") ;; \
	  *) fail flux-system/gitrepository/flux-ai-bridge-chart \
	    "GitRepository flux-ai-bridge-chart is at '$$gitrev', not at the snapshot $$sha:";; esac; \
	chart=$$(kubectl --context $(CONTEXT) -n flux-system get helmrelease/flux-ai-bridge \
	  -o jsonpath='{.status.helmChart}'); \
	chart="$${chart:-flux-system/flux-system-flux-ai-bridge}"; \
	kubectl --context $(CONTEXT) -n "$${chart%%/*}" annotate --overwrite \
	  helmchart/"$${chart#*/}" reconcile.fluxcd.io/requestedAt=$$ts; \
	kubectl --context $(CONTEXT) -n "$${chart%%/*}" wait helmchart/"$${chart#*/}" \
	  --for=jsonpath='{.status.lastHandledReconcileAt}'=$$ts --timeout=2m; \
	built=$$(kubectl --context $(CONTEXT) -n "$${chart%%/*}" get helmchart/"$${chart#*/}" \
	  -o jsonpath='{.status.observedSourceArtifactRevision} {.status.artifact.revision}'); \
	chartrev="$${built#* }"; \
	case "$$built" in "$$gitrev "*"+$$short") ;; \
	  *) fail "$${chart%%/*}/helmchart/$${chart#*/}" \
	    "HelmChart $$chart has '$$built' (source revision, chart version), not a chart built from $$gitrev:";; esac; \
	kubectl --context $(CONTEXT) -n flux-system annotate --overwrite \
	  helmrelease/flux-ai-bridge reconcile.fluxcd.io/requestedAt=$$ts; \
	kubectl --context $(CONTEXT) -n flux-system wait helmrelease/flux-ai-bridge \
	  --for=jsonpath='{.status.lastHandledReconcileAt}'=$$ts --timeout=10m; \
	kubectl --context $(CONTEXT) -n flux-system wait helmrelease/flux-ai-bridge \
	  --for=condition=Ready --timeout=1m || fail flux-system/helmrelease/flux-ai-bridge \
	  "HelmRelease flux-ai-bridge is not Ready after the reconcile, so the chart change wasn't applied:"; \
	release=$$(kubectl --context $(CONTEXT) -n flux-system get helmrelease/flux-ai-bridge \
	  -o jsonpath='{.status.history[0].chartVersion} {.status.history[0].status}'); \
	[ "$$release" = "$$chartrev deployed" ] || fail flux-system/helmrelease/flux-ai-bridge \
	  "HelmRelease flux-ai-bridge's latest release is '$$release' (chart version, status), not '$$chartrev deployed':"; \
	echo "    deployed chart $$chartrev"
	@# Also re-reconcile the test workload Kustomization so edits under
	@# e2e/podinfo-kustomize/ land immediately (and a fresh Flux event
	@# fires at the bridge for downstream test flows that watch for one).
	kubectl --context $(CONTEXT) -n flux-system annotate --overwrite \
	  kustomization/podinfo reconcile.fluxcd.io/requestedAt=$$(date +%s)
	@echo "==> Rolling the bridge deployment to pick up the new image"
	kubectl --context $(CONTEXT) -n flux-system rollout restart deployment/flux-ai-bridge
	kubectl --context $(CONTEXT) -n flux-system rollout status deployment/flux-ai-bridge --timeout=5m
	@echo ""
	@echo "==> e2e-update complete. Run: make e2e"

# Bitnami Valkey + Sentinel + a non-default application ACL user, used
# by e2e-up and e2e-update when E2E_DEDUPE_PERSISTENCE=1. Idempotent —
# helm upgrade is a no-op when values haven't changed, ACL SETUSER is a
# no-op when the user already exists with the same rules.
#
# Topology + credential model (mirrors a typical production setup):
#   - 3 valkey-node pods, each running both valkey-server and a sentinel
#     daemon (quorum=2 for split-brain safety).
#   - One ADMIN password (chart-managed `valkey` Secret, key
#     `valkey-password`) — used by sentinel ↔ valkey master/replica
#     monitoring auth, by sentinel ↔ sentinel inter-daemon ACL, and by
#     the chart's bootstrap probes. The ai-bridge MUST NOT use this.
#   - One APPLICATION user `bridge` with its own password, provisioned
#     via `ACL SETUSER` on every node's valkey-server AND sentinel
#     container (ACL is local to each daemon, not replicated). The ai-
#     bridge authenticates with this user on both legs of SentinelOptions
#     (Username/Password for the master, SentinelUsername/Password for
#     the sentinel daemons — same value here because it's the same
#     application user authenticating against two endpoints).
.PHONY: e2e-valkey-setup
e2e-valkey-setup: ## Set up Valkey + Sentinel and secrets in the e2e cluster for persistence tests
	@echo "==> Installing Bitnami Valkey (Sentinel, 3 nodes, quorum 2)"
	@# Use `--repo` instead of `helm repo add bitnami …` so we don't
	@# pollute the operator's host-level helm repo config. `helm repo
	@# add` persists into ~/.config/helm/repositories.yaml and survives
	@# `make e2e-down` (which only deletes the kind cluster); the
	@# inline --repo URL keeps the chart fetch ephemeral.
	@kubectl --context $(CONTEXT) create namespace valkey --dry-run=client -o yaml | kubectl --context $(CONTEXT) apply -f -
	helm --kube-context $(CONTEXT) upgrade --install valkey valkey \
	  --repo https://charts.bitnami.com/bitnami \
	  --namespace valkey \
	  --values $(MANIFESTS)/../values/valkey-sentinel-values.yaml \
	  --wait --timeout 5m
	@echo "==> Provisioning application ACL user 'bridge' on every node"
	@# ACL is local to each daemon in Redis/Valkey — `ACL SETUSER` on
	@# the master doesn't automatically populate replicas, and sentinel
	@# daemons have their own ACL store entirely separate from the
	@# backend. So we loop over all 3 pods × 2 containers (valkey on
	@# 6379, sentinel on 26379) and create the application user on each.
	@# Without this, a failover that promotes a replica to master would
	@# leave the bridge unable to AUTH against the new master.
	@ADMIN_PW=$$(kubectl --context $(CONTEXT) -n valkey get secret valkey -o jsonpath='{.data.valkey-password}' | base64 -d); \
	for i in 0 1 2; do \
	  echo "    valkey-node-$$i / valkey container"; \
	  kubectl --context $(CONTEXT) -n valkey exec valkey-node-$$i -c valkey -- \
	    valkey-cli -a "$$ADMIN_PW" --no-auth-warning \
	    ACL SETUSER bridge on '>bridge-app-pw-e2e' '~*' '+@all' >/dev/null; \
	  echo "    valkey-node-$$i / sentinel container"; \
	  kubectl --context $(CONTEXT) -n valkey exec valkey-node-$$i -c sentinel -- \
	    valkey-cli -p 26379 -a "$$ADMIN_PW" --no-auth-warning \
	    ACL SETUSER bridge on '>bridge-app-pw-e2e' 'allcommands' 'allchannels' >/dev/null; \
	done
	@echo "==> Creating the bridge's dedupe-creds Secret in flux-system"
	@# Single password for the application user — the bridge sends the
	@# same AUTH on both sentinel and backend connections (different
	@# endpoints, same user). The admin password lives ONLY in the
	@# chart-managed `valkey` Secret in the `valkey` namespace and never
	@# reaches the bridge's pod — this is what the
	@# TestPersistence_AdminCredentialIsolated test asserts.
	@kubectl --context $(CONTEXT) -n flux-system create secret generic flux-ai-bridge-dedupe \
	  --from-literal=bridge-password=bridge-app-pw-e2e \
	  --dry-run=client -o yaml | kubectl --context $(CONTEXT) apply -f -

.PHONY: e2e-persistence
e2e-persistence: ## Run the dedupe-persistence e2e tests
	@# Verifies dedupe state against a real Bitnami Valkey deployment
	@# running with Sentinel + auth (separate password for sentinel
	@# would also be possible; this setup uses one shared password
	@# matching auth.sentinel=true in the chart values).
	@#
	@# Composes the existing e2e-up/e2e-update + e2e flow with
	@# E2E_DEDUPE_PERSISTENCE=1, which:
	@#   - installs Bitnami valkey via helm CLI into the `valkey`
	@#     namespace (replication mode, 1 master + 1 replica, both with
	@#     a sentinel sidecar, single shared password from the chart-
	@#     managed Secret),
	@#   - renders the bridge HelmRelease with config.dedupe.persistence.
	@#     sentinel.enabled=true + auto-derived sentinel addrs pointing at
	@#     the Bitnami chart's headless Service + StatefulSet,
	@#
	@# The Go suite covers, end-to-end against this real
	@# Valkey+Sentinel+auth stack:
	@#   1. TestPersistence_DedupeSurvivesBridgeRestart — the per-PR-head
	@#      mark written through sentinel survives a `kubectl rollout
	@#      restart`, and the replayed event skips the PR as deduped.
	@#   2. TestPersistence_BridgeWritesAreVisibleInValkey — kubectl exec
	@#      valkey-cli against the master pod confirms the mark hit the
	@#      actual keyspace with the 30-day backstop expiry (would catch a
	@#      misrouted sentinel master discovery, a wrong key prefix, etc.).
	@#   3. TestPersistence_AdminCredentialIsolated /
	@#      TestPersistence_QuorumTopology — the ACL user and the
	@#      3-sentinel quorum are wired as configured.
	@#
	@# Requires the cluster to already exist (make e2e-up). If you want
	@# a one-shot, run: `make e2e-down && E2E_DEDUPE_PERSISTENCE=1 make e2e-up && make e2e-persistence`.
	@kind get clusters | grep -qx '$(CLUSTER_NAME)' || { \
	  echo "ERROR: kind cluster $(CLUSTER_NAME) not running. Run 'E2E_DEDUPE_PERSISTENCE=1 make e2e-up' first." >&2; \
	  exit 1; \
	}
	@echo "==> Re-rendering bridge HelmRelease with persistence enabled"
	E2E_DEDUPE_PERSISTENCE=1 $(MAKE) e2e-update
	@echo "==> Running persistence e2e test"
	E2E_DEDUPE_PERSISTENCE=1 go test -tags e2e -count=1 -timeout 5m \
	  -run TestPersistence ./e2e/...

.PHONY: e2e-down
e2e-down: ## Tear down the kind e2e cluster
	kind delete cluster --name $(CLUSTER_NAME)
