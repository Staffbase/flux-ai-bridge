# Developer Guide

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Go | ≥ 1.27 | Build and test |
| Docker | ≥ 24 | Build container image |
| `kubectl` | ≥ 1.25 | Interact with the cluster |
| `helm` | ≥ 3.10 | Lint and install chart |
| `flux` CLI | ≥ 2.0 | Test Flux notification payloads (optional) |

---

## Make targets

Every common dev task is wrapped in a `make` target. Prefer these over running `go` / `helm` / `docker` directly so the canonical flags (build tags, `-count=1`, `--kube-context`, the e2e env-var rendering, etc.) stay in one place.

| Target | What it does |
|---|---|
| `make help` | List all available targets with their one-line descriptions |
| `make build` | Compile-check every package (`go build ./...`) — does NOT produce a binary in the working dir |
| `make run` | Run the bridge locally (`go run ./cmd/...`). Requires env vars — see [Running Locally](#running-locally) below |
| `make test` | Unit tests across every package (`go test ./...`) |
| `make vet` | `go vet ./...` |
| `make golangci-lint` | Optional Go linter (CI runs it; locally skipped silently if not installed) |
| `make chart-deps` | Add the sub-chart repos and `helm dependency update` from the versions pinned in `Chart.yaml` (run by `lint` and `template`) |
| `make lint` | `helm lint charts/flux-ai-bridge/` |
| `make image` | Build the dev container image (tag `flux-ai-bridge:dev`; override with `DEV_IMAGE=...`) |
| `make vuln` | `govulncheck` over the module (also runs in CI) |
| `make template` | `helm template` the chart with the minimum required flags — pipe to `less` / `yq` to inspect |
| `make test-sentinel` | Bring up Redis + Sentinel in disposable Docker containers, run the `integration_sentinel`-tagged tests, tear down |
| `make test-sentinel-down` | Tear down the local Sentinel test stack (when a `test-sentinel` run left it behind) |
| `make test-all` | Full pre-PR sanity check: vet → unit tests → golangci-lint → helm lint → `e2e-up` → `e2e`. Leaves the kind cluster running for follow-up debugging |
| `make e2e-up` | Build image, create kind cluster, install Flux + mocks, deploy bridge via HelmRelease |
| `make e2e` | Run the e2e Go tests against the running cluster |
| `make e2e-broken` | Patch podinfo into a CrashLoopBackOff state and re-run e2e against that |
| `make e2e-update` | Rebuild image + redeploy without recreating the cluster — inner-loop after a code edit |
| `make e2e-valkey-setup` | Set up Valkey + Sentinel and the secrets in the e2e cluster for the persistence tests (run automatically by `E2E_DEDUPE_PERSISTENCE=1 make e2e-up`) |
| `make e2e-persistence` | Run the persistence test suite against Bitnami Valkey + Sentinel (requires `E2E_DEDUPE_PERSISTENCE=1 make e2e-up`) |
| `make e2e-down` | Delete the kind cluster |
| `make flux-install` | Install the Flux Operator (`FLUX_OPERATOR_VERSION`) and Flux through `e2e/manifests/01-flux-instance.yaml` into the kind cluster `CLUSTER_NAME` (run by `e2e-up` and `ct-install`) |
| `make ct-install` | What the `install-chart` CI job runs: build and load the image, install Flux and a podinfo Kustomization, then `ct install` the chart once per `ci/*-values.yaml` with dummy GitHub App credentials. Needs an existing kind cluster `CT_CLUSTER_NAME` (default `flux-ai-bridge-ct`): `kind create cluster --name flux-ai-bridge-ct --config .github/kind-config.yaml` |

Switches that combine with the e2e targets: `E2E_REAL_GITHUB=1` (target a real PR), `E2E_LLM=ollama|azure|none` (real LLM), `E2E_KEEP_COMMENT_HISTORY=true` (don't delete prior comments), `E2E_DEDUPE_PERSISTENCE=1` (Valkey + Sentinel). See [docs/e2e-real-github.md](e2e-real-github.md) and [docs/e2e-real-llm.md](e2e-real-llm.md).

---

## Repository Layout

```
flux-ai-bridge/
├── cmd/
│   └── main.go                   # Entry point — wires config, server, shutdown; registers GET /healthz + /readyz + /metrics
├── internal/
│   ├── config/
│   │   └── config.go             # Env-based configuration loader + validator
│   ├── webhook/
│   │   └── handler.go            # HTTP handler for POST /webhook (HMAC validation, payload parsing)
│   ├── kustomizations/
│   │   └── filter.go             # Glob-based Kustomization-name admission filter (include/exclude)
│   ├── dedupe/
│   │   ├── cache.go              # Per-PR-head admission cache (MarkPRIfNew/ForgetPR), in-memory LRU
│   │   └── redis.go              # Redis/Valkey backend (single-node + Sentinel), SETNX admission
│   ├── fetchcache/
│   │   ├── cache.go              # fetchcache.Cache interface
│   │   ├── lru.go                # In-memory LRU backend
│   │   └── redis.go              # Redis backend (shares the dedupe connection)
│   ├── mcp/
│   │   ├── client.go             # HTTP MCP client for flux-operator-mcp
│   │   └── pool.go               # MCP session pool (one session per in-flight pipeline)
│   ├── readiness/
│   │   └── checker.go            # kstatus evaluation of inventory resources
│   ├── llm/
│   │   ├── interface.go          # Analyzer interface + NewAnalyzer factory
│   │   ├── claude.go             # Anthropic Claude implementation
│   │   └── openai.go             # OpenAI-compatible implementation (Ollama / vLLM / Azure / OpenAI)
│   ├── github/
│   │   └── commenter.go          # Find PR by commit SHA, post/update comment
│   ├── metrics/
│   │   └── metrics.go            # Prometheus counters/gauges
│   ├── skills/
│   │   └── loader.go             # Loads /skills/SKILL.md as the LLM system-prompt prefix
│   └── processor/
│       ├── processor.go          # Pipeline: MCP → HR expansion → readiness → pod snapshot → (LLM | skip-on-healthy | no-LLM) → GitHub
│       └── failure.go            # Failure-context fetcher (events/logs/metrics) + pod kstatus helpers
├── charts/
│   └── flux-ai-bridge/           # Helm chart
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── _helpers.tpl
│           ├── NOTES.txt
│           ├── deployment.yaml
│           ├── service.yaml
│           ├── serviceaccount.yaml
│           ├── secret.yaml
│           ├── secret-dedupe.yaml
│           ├── configmap-skill.yaml
│           ├── flux-provider.yaml
│           ├── flux-alert.yaml
│           ├── flux-hmac-secret.yaml
│           ├── mcp-rbac.yaml
│           ├── networkpolicy.yaml
│           ├── servicemonitor.yaml
│           ├── hpa.yaml
│           └── tests/
│               └── test-connection.yaml
├── e2e/                          # End-to-end tests (kind cluster)
│   ├── e2e_test.go               # Go tests (build tag: e2e); setup is driven by Makefile
│   ├── persistence_test.go       # Dedupe-persistence tests (Valkey + Sentinel; `make e2e-persistence`)
│   ├── values/
│   │   └── valkey-sentinel-values.yaml  # Bitnami Valkey values for the persistence setup
│   └── manifests/
│       ├── 00-git-server.yaml    # git-daemon serving the local repo to Flux
│       ├── 01-flux-instance.yaml # FluxInstance (installs Flux controllers)
│       ├── 02-mock-common.yaml   # Mock namespace + scripts ConfigMap (always when ≥1 mock)
│       ├── 02-mock-llm.yaml      # Mock LLM Deployment + Service (skipped when E2E_LLM is set)
│       ├── 02-mock-github.yaml   # Mock GitHub Deployment + Service (skipped when E2E_REAL_GITHUB=1)
│       ├── 03-flux-ai-bridge.yaml.tpl  # Template: Secret, GitRepository, HelmRelease (rendered by the Makefile)
│       └── 05-ollama-cache.yaml  # Host-backed PV/PVC for Ollama model blobs (E2E_LLM=ollama)
├── docs/                         # Documentation
├── Dockerfile
├── go.mod
└── go.sum
```

Note: there is no RBAC template — the bridge never talks to the Kubernetes API directly (all cluster reads go through `flux-operator-mcp`), so the chart creates no Roles or ClusterRoles.

---

## Building

### Compile-check

```bash
make build
```

Runs `go build ./...` — verifies every package compiles. Does NOT produce a runnable binary; use `make run` for that.

### Run tests

```bash
make test
```

For the full pre-PR sweep (vet + tests + golangci-lint + helm lint + e2e):

```bash
make test-all
```

Unit-test coverage spans thirteen packages — `cmd` plus every `internal/` package (`config`, `dedupe`, `fetchcache`, `github`, `kustomizations`, `llm`, `mcp`, `metrics`, `processor`, `readiness`, `skills`, `webhook`); the e2e suite is on top of that. Highlights:

| Package | What is tested |
|---|---|
| `internal/config` | Env-based loading, all valid/invalid configurations |
| `internal/webhook` | SHA extraction, method/path routing, async processor dispatch |
| `internal/readiness` | Inventory ID parsing, kstatus evaluation via a fake MCP fetcher |
| `internal/processor` | Owner/repo splitting, URL parsing, prompt building, full `Process()` flow with mocked MCP/LLM/GitHub |
| `internal/llm` | Provider selection (`claude`, `openai-compatible`, `none`, unknown, empty). `none` returns a nil analyzer at construction — the processor branches on that to skip the LLM call entirely |

There's no make wrapper for a single-package verbose run — use `go test` directly when you need it:

```bash
go test -v ./internal/processor/...
```

### Build the container image

```bash
make image
```

Builds `flux-ai-bridge:dev`. Override the tag with `DEV_IMAGE=registry.local/foo:bar make image`. The Dockerfile uses a multi-stage build:
- **Builder**: `golang:1.27-alpine` — compiles with `CGO_ENABLED=0` and `-ldflags="-s -w"` for a minimal binary.
- **Runtime**: `gcr.io/distroless/static:nonroot` — no shell, runs as UID 65532.

---

## End-to-End Tests

The E2E suite spins up a real kind cluster and exercises the full request path:
`webhook → flux-operator-mcp → mock LLM → mock GitHub API`.

The MCP server is the real upstream `flux-operator-mcp` Helm chart (with
`transport=http` for streamable HTTP). Anthropic and GitHub are replaced by
in-cluster Python stubs, so no real LLM or GitHub credentials are needed.

### Prerequisites

| Tool | Notes |
|---|---|
| `kind` | ≥ 0.24 |
| `helm` | ≥ 3.10 |
| `docker` | image build + kind load |
| `kubectl` | cluster interaction |
| `git` | tagging HEAD |

### Workflow

```bash
make e2e-up    # build image, create cluster, install Flux + mocks, deploy bridge via HelmRelease
make e2e       # run Go E2E tests (health check + webhook response codes + full pipeline)
make e2e-down  # delete the kind cluster
```

In mock mode (the default), `make e2e-up` auto-generates a throwaway
2048-bit RSA key at `e2e/fixtures/mock-app-key.pem` if one isn't present. The
file is gitignored — never commit a real App key. The mock GitHub server
ignores the key, so any well-formed PEM works.

> Want to run e2e against a **real GitHub PR** instead of the in-cluster mock?
> See [docs/e2e-real-github.md](e2e-real-github.md). One-time sandbox-repo
> setup + a GitHub App installed on it, then
> `E2E_REAL_GITHUB=1 GITHUB_APP_ID=... GITHUB_APP_INSTALLATION_ID=... GITHUB_APP_PRIVATE_KEY="$(cat key.pem)" make e2e-up && make e2e`.
>
> Want to run against a **real LLM** (local Ollama with `qwen2.5-coder:1.5b`)?
> See [docs/e2e-real-llm.md](e2e-real-llm.md). One-flag toggle:
> `E2E_LLM=ollama make e2e-up && make e2e`. Independent of the GitHub toggle.
>
> Want the bridge to **keep the PR comment history** during e2e (so each
> analysis posts as a fresh comment instead of replacing the prior one)?
> Set `E2E_KEEP_COMMENT_HISTORY=true make e2e-up` (or `make e2e-update`).
> Useful when watching how a workload toggles between healthy and broken
> across commits on a real GitHub PR.
>
> Want to **deploy the bridge with a local LLM in production** (not just e2e)?
> See [docs/local-llm.md](local-llm.md). The chart includes an optional Ollama
> sub-chart that gives you a fully self-contained install with one flag:
> `--set ollama.enabled=true`.
>
> Setting up **GitHub App auth**? See [docs/github-app.md](github-app.md) —
> the bridge is App-only, so this is the canonical and only auth doc.
> Includes required permissions, "Selected Repositories" install, and chart wiring.
>
> Want to verify **persistent dedupe** (the optional Valkey sub-chart) actually
> survives a bridge restart? Run `make e2e-persistence` against an existing
> e2e cluster. It re-renders the bridge HelmRelease with `valkey.enabled=true`
> + `config.dedupe.persistence.enabled=true`, then runs `TestPersistence_DedupeSurvivesBridgeRestart`:
> POST a unique-SHA event → rollout-restart the bridge pod → POST the same
> event again → assert the second one was deduped (no re-pipeline, no re-post).
> The test self-skips when persistence isn't wired into the running cluster,
> so it's safe to leave in the default suite.

`make e2e-up` does the following automatically:

1. Builds `flux-ai-bridge:e2e` Docker image.
2. Creates a kind cluster named `flux-ai-bridge-e2e` with the local repo mounted at `/flux-ai-bridge-src`.
3. Loads the image into kind (`pullPolicy: Never`).
4. Installs `flux-operator` via Helm.
5. Applies `FluxInstance` → Flux controllers come up.
6. `flux-operator-mcp` comes up as a sub-chart of the bridge's HelmRelease (step 8; `transport=http`, streamable HTTP on `:9090/mcp`).
7. Deploys two in-cluster mock servers in the `mock` namespace: **mock-llm** (Anthropic Messages API on `:80`) and **mock-github** (GitHub REST API on `:80`). Bridge reaches them via `mock-llm.mock.svc.cluster.local` / `mock-github.mock.svc.cluster.local`.
8. Creates a `GitRepository` pointing to the in-cluster git-server (served from the mounted local repo at the `e2e-test` tag) and a `HelmRelease` that installs the bridge chart wired to the real MCP server and the mock(s).

### The in-cluster git-server ([e2e/manifests/00-git-server.yaml](../e2e/manifests/00-git-server.yaml))

A small `nginx → fcgiwrap → git-http-backend` (Debian) stack serves a single bare repo, `flux-ai-bridge.git`, at `http://git-server.git-server.svc.cluster.local/flux-ai-bridge.git`. It exists so the e2e flow can stay hermetic when neither GitHub nor an external git host is involved. Two consumers reference it:

1. **The bridge chart source — always.** The e2e HelmRelease pulls its chart from a GitRepository (`flux-ai-bridge-chart`, tag `e2e-test`) backed by this server, so the bridge is installed *via Flux* (source-controller + helm-controller) and not via `helm install`. The install path itself is part of what the suite tests.
2. **The podinfo workload source — mock mode only.** When `E2E_REAL_GITHUB` is unset, the rendered `flux-ai-bridge-podinfo` GitRepository also points here, so source-controller reconciles `e2e/podinfo-kustomize/` without leaving the cluster. With `E2E_REAL_GITHUB=1` the URL switches to `github.com/$GITHUB_REPO` and the git-server is only used for the chart.

Two implementation details worth knowing if you ever debug it:

- An init container snapshots the *working tree* of the mounted host repo (hostPath `/flux-ai-bridge-src`, which `kind` binds to the workstation's repo dir) into a fresh `git init` + tag `e2e-test`. That's why uncommitted local changes are picked up by e2e without requiring a commit first.
- The runtime container uses `debian:bookworm-slim`, not Alpine — Alpine's `git` package only ships the dumb-protocol bits, and source-controller (go-git) needs smart-HTTP, which is what `/usr/lib/git-core/git-http-backend` provides on Debian.

### The mock servers ([e2e/manifests/02-mock-{common,llm,github}.yaml](../e2e/manifests/))

Two small Python `http.server` stubs in the `mock` namespace replace the bridge's external dependencies so the suite can run offline and assert *exactly* what the bridge sent:

- **mock-llm** (`mock-llm.mock.svc.cluster.local`) — Anthropic Messages API. Always responds with a fixed stub message; captures every incoming POST body so tests can inspect the bridge's prompt. Skipped when `E2E_LLM` is non-empty.
- **mock-github** (`mock-github.mock.svc.cluster.local`) — GitHub REST API. Returns canned data for the endpoints the bridge calls (PR-lookup, PR file list, blob fetch, comment-list, comment-create, comment-update) and captures every POST/PATCH for assertion. The PR-files / blob-content handlers return a synthetic `namespace.yaml` declaring `Namespace podinfo` so the bridge's inventory-membership filter resolves against the podinfo Kustomization's inventory — without those handlers the filter would silently drop every PR. Skipped when `E2E_REAL_GITHUB=1`.

Each mock toggles independently via its own Deployment+Service file (the mock-server block of the `e2e-up` target in the [Makefile](../Makefile)), so `E2E_LLM=ollama make e2e-up` deploys only mock-github, and `E2E_REAL_GITHUB=1 make e2e-up` deploys only mock-llm. The shared `mock-server-scripts` ConfigMap in `02-mock-common.yaml` holds both Python scripts.

#### What the mock logs look like

Both mocks emit a structured IN/OUT block per call to stdout (visible via `kubectl -n mock logs deploy/mock-llm` or `…/mock-github`). Each call gets a monotonic ordinal `#NNN` that pairs the IN with its OUT, plus a phase/identity tag to disambiguate when multiple pipelines fire in one run:

```
--- IN  POST /v1/messages | #003 flux-system/podinfo Kustomization rev=27168b55 reason=ReconciliationSucceeded ---
{ full request body }
--- OUT 200 /v1/messages | #003 flux-system/podinfo Kustomization rev=27168b55 reason=ReconciliationSucceeded ---
{ full response body }
```

- **mock-llm** parses the bridge's prompt and extracts `<namespace>/<name>`, `<kind>`, `rev=<short-sha>`, and `reason=<event-reason>` into the tag. Falls back to `unparseable-body` if the prompt shape ever changes.
- **mock-github** tags each call with the pipeline *phase* (`pr-lookup`, `comment-list`, `comment-create`, `comment-update`, `app-token`) so you can grep across a noisy run.

The internal `/_dump` and `/_reset` endpoints — used by the Go tests to scrape and clear the captured-request buffer — are suppressed from the log to keep it focused on actual bridge traffic. Output is line-buffered (`flush=True`) so it surfaces in `kubectl logs -f` in real time.

### What the tests verify

| Test | Assert |
|---|---|
| `TestHealthz` | `GET /healthz` → 200, `{"status":"ok"}` |
| `TestWebhookKustomizationAccepted` | Kustomization event → 202 Accepted |
| `TestWebhookKustomizationWithoutRevisionSkipped` | Kustomization event with no revision metadata → 204 No Content (skipped) |
| `TestWebhookNonKustomizationIgnored` | HelmRelease event → 204 No Content |
| `TestWebhookInvalidJSONRejected` | Malformed body → 400 Bad Request |
| `TestPipelineProducesGitHubComment` | Full pipeline in mock mode: a Flux event ends in a comment POST against `mock-github` (skipped when `E2E_REAL_GITHUB=1`) |
| `TestPipelineProducesRealGitHubComment` | Full pipeline against api.github.com: the bridge's marker comment lands on the real sandbox PR (active only with `E2E_REAL_GITHUB=1`) |
| `TestPipelineAttributesPrunedDeletion` | A PR that deletes a resource is attributed from the Kustomization's prune event and gets its comment (mock mode) |
| `TestPipelineDefersUnappliedRevision` | An event for a revision the Kustomization hasn't applied yet is deferred, not analysed |
| `TestPersistence_DedupeSurvivesBridgeRestart` | After a bridge rollout-restart, a replayed event skips the already-analysed PR head as deduped (Valkey-backed; `make e2e-persistence`) |
| `TestPersistence_BridgeWritesAreVisibleInValkey` | The per-PR-head mark lands in the Valkey keyspace with the 30-day backstop expiry, and a replay is deduped |
| `TestPersistence_AdminCredentialIsolated` | The bridge's Valkey ACL user is isolated from the admin credential |
| `TestPersistence_QuorumTopology` | The Sentinel quorum topology is what the values file configures |

The `TestPersistence_*` tests self-skip when persistence isn't wired into the running cluster (see `make e2e-persistence`).

### Troubleshooting

```bash
# Check bridge pod logs
kubectl -n flux-system logs -l app.kubernetes.io/name=flux-ai-bridge --context kind-flux-ai-bridge-e2e

# Check HelmRelease status
kubectl -n flux-system describe helmrelease flux-ai-bridge --context kind-flux-ai-bridge-e2e

# Check GitRepository status
kubectl -n flux-system describe gitrepository flux-ai-bridge-chart --context kind-flux-ai-bridge-e2e
```

---

## Running Locally

You need access to a running `flux-operator-mcp` and a GitHub App installed on the repo you want commented on (see [docs/github-app.md](github-app.md) for App setup). The easiest way to get MCP locally is to port-forward the service from your dev cluster.

### 1. Set environment variables

```bash
export WEBHOOK_PORT=8080
export MCP_URL=http://localhost:9090/mcp
export LLM_PROVIDER=claude
export ANTHROPIC_API_KEY=sk-ant-yourkey
export GITHUB_APP_ID=12345
export GITHUB_APP_INSTALLATION_ID=67890
export GITHUB_APP_PRIVATE_KEY_FILE=$HOME/Downloads/flux-ai-bridge.<id>.private-key.pem
export GITHUB_REPO=myorg/my-infra-repo   # optional
```

### 2. Port-forward flux-operator-mcp

```bash
kubectl -n flux-system port-forward svc/flux-operator-mcp 9090:9090
```

### 3. Run the bridge

```bash
make run
```

### 4. Send a test webhook

```bash
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "involvedObject": {
      "kind": "Kustomization",
      "name": "apps",
      "namespace": "default"
    },
    "severity": "info",
    "reason": "ReconciliationSucceeded",
    "message": "Applied revision: main@sha1:abc1234def5678",
    "metadata": {
      "revision": "main@sha1:abc1234def5678"
    }
  }'
```

Expected response: `202 Accepted`. The pipeline logs go to stdout (JSON).

---

## Adding a New LLM Provider

1. Create `internal/llm/myprovider.go` with a struct that implements:

```go
type Analyzer interface {
    Analyze(ctx context.Context, systemPrefix, userPayload string) (string, error)
}
```

2. Register the provider in `internal/llm/interface.go`'s `NewAnalyzer` factory:

```go
case "my-provider":
    return NewMyProviderAnalyzer(cfg)
```

3. Add required config fields to `internal/config/config.go` and validate them in the `switch cfg.LLMProvider` block. Note the `"none"` case is a no-op — `cmd/main.go` skips `NewAnalyzer` entirely when `LLMProvider == "none"` and leaves the analyzer nil; the processor branches on the nil value. Any new provider you add should fall under the same factory and is expected to return a non-nil analyzer.

4. Update `charts/flux-ai-bridge/values.yaml` and `docs/configuration.md` to document the new provider and any new credentials.

---

## Helm Chart Development

### Lint

```bash
make lint
```

### Dry-run render

```bash
make template               # default rendering
make template | less        # pipe to a pager
make template | yq '.spec'  # filter with yq
```

To render with additional `--set` flags beyond the defaults baked into the target, fall back to raw `helm template`:

```bash
helm template flux-ai-bridge charts/flux-ai-bridge/ \
  --set existingSecret=my-secret \
  --set config.llm.provider=claude \
  | less
```

### Install for local testing (kind/minikube)

No make wrapper today — installing into an arbitrary cluster touches kubeconfig context, image-loading mechanism (kind vs. minikube), and namespace, so it's best done explicitly:

```bash
# Build and load the image
make image
kind load docker-image flux-ai-bridge:dev   # or: minikube image load

# Install
helm install flux-ai-bridge charts/flux-ai-bridge/ \
  --namespace flux-system \
  --create-namespace \
  --set image.repository=flux-ai-bridge \
  --set image.tag=dev \
  --set image.pullPolicy=Never \
  --set existingSecret=flux-ai-bridge \
  --set config.llm.provider=claude
```

For e2e-shaped local installs (kind cluster + Flux + bridge + mocks all together), use `make e2e-up` instead.

---

## Code Style

- Standard Go formatting: `gofmt` / `goimports`.
- No external framework — the HTTP server uses `net/http` from the standard library.
- Errors are returned up the call stack; the processor logs them and does not panic.
- Context propagation: every I/O operation (MCP, LLM, GitHub API) accepts and respects `context.Context`.
- No global state outside of `main.go`.

---

## Dependency Highlights

| Module | Purpose |
|---|---|
| `github.com/anthropics/anthropic-sdk-go` | Claude API client |
| `github.com/openai/openai-go/v3` | OpenAI-compatible client (Ollama, vLLM, Azure, OpenAI) |
| `github.com/google/go-github/v62` | GitHub REST API (PR lookup, comments) |
| `github.com/bradleyfalzon/ghinstallation/v2` | GitHub App installation token minting |
| `sigs.k8s.io/cli-utils/pkg/kstatus/status` | Resource readiness evaluation |
| `github.com/modelcontextprotocol/go-sdk` | HTTP MCP client for flux-operator-mcp |

---

## Release Process

1. Tag the commit: `git tag v0.x.y && git push origin v0.x.y`
2. The `ci` workflow builds and pushes `ghcr.io/staffbase/flux-ai-bridge:v0.x.y`. Pushes to `main` and `dev` also publish `main-<sha>` / `dev-<sha>` tags.
3. Update `charts/flux-ai-bridge/Chart.yaml` `appVersion` to match the new tag.
4. Bump `charts/flux-ai-bridge/Chart.yaml` `version` (semantic versioning for chart changes).
5. On merge to `main`, the `Release Helm Charts` workflow pushes the chart to `oci://ghcr.io/staffbase/charts/flux-ai-bridge`.

Dependabot PRs are merged by the `GITHUB_TOKEN`, and those merges don't trigger the push workflows: they publish no `main` image and no chart. The next merge by a person publishes what its own path filters cover: a `main` image for changes outside `charts/` and `docs/`, a chart release for changes under `charts/`.
