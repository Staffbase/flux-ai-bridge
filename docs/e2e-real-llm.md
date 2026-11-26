# E2E with a real LLM

The default `make e2e` setup uses `mock-llm` — an in-cluster Python stub that
returns a fixed canned response. That's fast, deterministic, and free.

To validate the bridge against a **real LLM**, set `E2E_LLM` to one of:

| Mode | What runs | When to use |
|---|---|---|
| `E2E_LLM=ollama` | bundled Ollama sub-chart in the kind cluster, model defaults to `qwen2.5-coder:1.5b` | self-contained, no external service, free; CPU-bound and outputs are mediocre for diagnostic tasks |
| `E2E_LLM=azure` + `OPENAI_API_KEY=…` | bridge calls Azure AI Foundry (`https://models.inference.ai.azure.com`) | best output quality; needs an API key + per-token cost |
| `E2E_LLM=none` | no LLM at all; bridge posts the [kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus) table directly | smoke-test the install path without provisioning a model; verify the no-LLM code path on every e2e run |
| unset | mock-llm (deterministic stub) | default; what CI runs |

`E2E_LLM` is independent of `E2E_REAL_GITHUB`. Any of the eight combinations
(4 LLM modes × 2 GitHub modes) work.

---

## What stays mocked

| Component | Mock LLM | Ollama | Azure AI | No-LLM |
|---|---|---|---|---|
| `flux-operator-mcp` | real (sub-chart) | real (sub-chart) | real (sub-chart) | real (sub-chart) |
| `mock-llm` (Anthropic) | mocked | bypassed (bridge talks to Ollama) | bypassed (bridge talks to Azure AI Foundry) | not deployed (no LLM call) |
| `mock-github` | mocked | unchanged | unchanged | unchanged |

> **About `flux-operator-mcp` in e2e.** The bridge chart pulls flux-operator-mcp as a bundled Helm sub-chart (`charts/flux-ai-bridge/Chart.yaml`, gated by the `flux-operator-mcp.enabled` value, enabled by default). e2e relies on that default — `make e2e-up` brings MCP up via the sub-chart, with no separate `flux-operator-mcp` install step. The sub-chart's Service name is pinned to `flux-operator-mcp` (via `fullnameOverride`), and with `config.mcp.url` left empty the bridge derives `http://flux-operator-mcp.<release-namespace>.svc.cluster.local:9090/mcp` from it. Production users who run their own pre-existing MCP install can set `flux-operator-mcp.enabled: false` and point `config.mcp.url` at it.

Bridge config switches:

| Setting | Mock | Ollama | Azure AI | No-LLM |
|---|---|---|---|---|
| `llm.provider` | `claude` | `openai-compatible` | `openai-compatible` | `none` |
| `llm.model` | (default) | `qwen2.5-coder:1.5b` (overridable) | `gpt-4o-mini` (overridable) | unset |
| `llm.anthropicBaseUrl` | mock-llm Service | unset | unset | unset |
| `llm.openaiBaseUrl` | unset | auto-derived to in-cluster Ollama Service | unset → bridge defaults to `https://models.inference.ai.azure.com` | unset |
| `OPENAI_API_KEY` (from Secret) | unused | unused (Ollama doesn't auth) | the value you exported as `OPENAI_API_KEY` | unused |
| `ANTHROPIC_API_KEY` | fake | unused | unused | unused |

---

## Mode A — Ollama (self-contained)

### Requirements

| Resource | Minimum |
|---|---|
| Docker / kind | already required by `make e2e-up` |
| Free RAM | ~3 GB for the kind cluster + Ollama + the bridge (qwen2.5-coder:1.5b is ~1 GB on disk, ~1.5 GB loaded) |
| Free disk | ~2 GB for the model (cached at `OLLAMA_CACHE_DIR`, default `~/.cache/flux-ai-bridge/ollama`) |
| Internet | first run only — Ollama pulls the model from its registry, then it lives on the host PV |

Bigger models (`qwen2.5-coder:7b`, `qwen2.5:14b`, `qwen2.5:32b`) need
proportionally more RAM but produce noticeably better diagnostic output. With
64 GB RAM the 14B is a sweet spot.

### Running

```bash
make chart-deps                                     # once per checkout: the Ollama image tag comes from its sub-chart
make e2e-down                                       # fresh cluster
E2E_LLM=ollama make e2e-up                     # default qwen2.5-coder:1.5b
# or pick a bigger model:
E2E_LLM=ollama E2E_LLM_MODEL=qwen2.5-coder:7b make e2e-up
make e2e
```

`make e2e-up` adds these steps when `E2E_LLM=ollama`:

1. Pre-pulls the Ollama container image on the host (one network hit per
   host; cached in Docker) and `kind load`s a single-arch flattened tar.
2. Provisions [e2e/manifests/05-ollama-cache.yaml](../e2e/manifests/05-ollama-cache.yaml)
   — a host-backed PV + PVC so model blobs survive `make e2e-down`.
3. Renders the bridge HelmRelease with `ollama.enabled=true`,
   `llm.provider=openai-compatible`, `llm.model=$E2E_LLM_MODEL`. The
   `ollama-helm` dependency installs as a sub-chart in the `ollama`
   namespace; the bridge's `OPENAI_BASE_URL` auto-derives to the in-cluster
   Service.

The HelmRelease's `spec.timeout` is bumped to 20 min in e2e to cover
cold-cache model pulls.

---

## Mode B — Azure AI Foundry (shared endpoint) OR Azure OpenAI Service (your own tenant)

The bridge auto-detects which Azure API to use based on the URL pattern:

| Variant | `OPENAI_BASE_URL` shape | Auth | Bridge behavior |
|---|---|---|---|
| Azure AI Foundry / GitHub Models | empty (defaults to `https://models.inference.ai.azure.com`) OR explicit `https://models.inference.ai.azure.com` | `Authorization: Bearer <api-key>` | OpenAI-compatible path, model name in body |
| Azure OpenAI Service (deployments API) | `https://<resource>.openai.azure.com/` or `https://<resource>.cognitiveservices.azure.com/` (a trailing `/openai` is fine) | `api-key: <key>` header, URL rewritten to `/openai/deployments/<deployment>/chat/completions?api-version=…` | Azure-specific path via [openai-go's `azure` sub-package](https://pkg.go.dev/github.com/openai/openai-go/v3/azure) |
| Azure OpenAI Service (v1 API) | `https://<resource>.openai.azure.com/openai/v1/` | `Authorization: Bearer <api-key>` | OpenAI-compatible path, model (deployment) name in body |

Both kick in when `E2E_LLM=azure`; only the env vars differ.

The Foundry variant is the dev-friendly entry point: free tier, just needs a
GitHub-issued API key (any classic or fine-grained token the Models catalogue
accepts). Use Azure OpenAI Service when you already have a tenant-scoped
deployment (private model, paid SLA, custom region).

### Requirements

| Resource | Minimum |
|---|---|
| Azure subscription | with Azure AI Foundry access — see [GitHub Models marketplace](https://github.com/marketplace/models) for the dev-friendly entry point |
| API key | a personal or org-level token with read on the model catalogue |
| Internet | every e2e run hits Azure |

### Get a key

**For Azure AI Foundry / GitHub Models** (fastest dev path):

1. Open <https://github.com/marketplace/models>, accept the preview terms.
2. Generate an API key the Models catalogue accepts (any GitHub-issued
   classic or fine-grained token works — no special scopes are required for
   Models access). This token is **only** used as the LLM API key; the bridge
   still authenticates to GitHub via its App.
3. ```bash
   export OPENAI_API_KEY=<your-token>
   # OPENAI_BASE_URL left unset → bridge defaults to models.inference.ai.azure.com
   ```

**For Azure OpenAI Service** (your own tenant):

1. In the Azure portal, go to your Azure OpenAI resource → **Keys and Endpoint**.
2. Copy one of the two keys.
3. ```bash
   export OPENAI_API_KEY=<azure-key>
   export OPENAI_BASE_URL=https://<your-resource>.openai.azure.com/
   export E2E_LLM_MODEL=<your-deployment-name>    # NOT model name; the deployment name from Azure
   ```

The bridge detects an `*.openai.azure.com` / `*.cognitiveservices.azure.com`
host with no path (or just `/openai`) and switches to the
Azure-specific request format (api-key header, URL rewrite to
`/openai/deployments/<name>/chat/completions?api-version=…`). The
`E2E_LLM_MODEL` value here is the **deployment** name you configured in
Azure (often the same as the model, but not always — Azure lets you name
a `gpt-4o` deployment `my-summarizer` for example).

### Running

```bash
make e2e-down
export OPENAI_API_KEY=<your-key>
E2E_LLM=azure make e2e-up
# or pick a different model from the catalogue:
E2E_LLM=azure E2E_LLM_MODEL=gpt-4o make e2e-up
# or target a non-default endpoint (Azure tenant, OpenRouter, vLLM, etc.):
E2E_LLM=azure OPENAI_BASE_URL=https://myorg.openai.azure.com make e2e-up
make e2e
```

Override knobs (all optional):

| env var | default | purpose |
|---|---|---|
| `OPENAI_API_KEY` | required | API key the bridge uses to authenticate |
| `E2E_LLM_MODEL` | `gpt-4o-mini` | model identifier passed to the endpoint |
| `OPENAI_BASE_URL` | unset → bridge default `https://models.inference.ai.azure.com` | full endpoint URL, set for a different Azure tenant or any other OpenAI-compatible provider |

`make e2e-up` with `E2E_LLM=azure`:

1. Skips the Ollama image pre-load + cache PV provisioning.
2. Renders the bridge HelmRelease with `ollama.enabled=false`,
   `llm.provider=openai-compatible`, `llm.model=$E2E_LLM_MODEL` (default
   `gpt-4o-mini`), and `llm.openaiBaseUrl=$OPENAI_BASE_URL` (empty by default,
   so the bridge falls back to its built-in `https://models.inference.ai.azure.com`).
3. Writes `OPENAI_API_KEY` into the bridge's Secret so the openai-compatible
   client authenticates against the chosen endpoint.

Approximate cost: one e2e run is ~5 LLM calls × ~10k input tokens × ~600
output tokens. With GPT-4o-mini that's well under 10¢. With GPT-4o, ~30¢.

---

## Mode C — No LLM at all

`E2E_LLM=none` runs the full pipeline but turns the LLM off
(`config.llm.provider=none`). The bridge posts a deterministic comment built
from the [kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus)
output alone — no API key, no model, no skill prompt, no failure-context fetch.

```bash
E2E_LLM=none make e2e-up
make e2e
```

What this verifies:

- The config-load path accepts `LLM_PROVIDER=none` without requiring an API key.
- `cmd/main.go` skips analyzer construction and the processor branches on the nil `Analyzer`.
- `buildKstatusBody` emits the no-LLM body: a `## Resource Status` table (including HelmRelease-expanded children when `READINESS_EXPAND_HELMRELEASES=true`) and a `## Workload Status` table (one row per pod, labeled with the resolved workload controller — Deployment, StatefulSet, etc.) when `READINESS_INCLUDE_PODS=true`. For non-Current rows the Message column carries the most diagnostic not-True condition as `Reason: Message`; the full `.status.conditions[]` list is not rendered. `formatComment` wraps the body with the canonical header/footer — the resulting comment has the same byte-level shape as the LLM-driven path (per-Kustomization idempotency marker, `# ✅/⚠️/❌ Flux Status` header, `*Analyzed by flux-ai-bridge*` footer).
- The mock-llm Deployment is **not** rolled out, so the `e2e-up` recipe also exercises the "no LLM endpoint" path end-to-end.

Use this for smoke-testing the install path on any host (no external services,
no model pull) and as a regression gate for the kstatus-only comment.

---

## Combining with `E2E_REAL_GITHUB`

The two toggles are independent. To run the bridge against a real GitHub PR
*and* a real LLM:

```bash
export E2E_REAL_GITHUB=1
export GITHUB_REPO=<owner/repo>
# Flux deploys the open test PR's branch (see docs/e2e-real-github.md):
export E2E_PODINFO_GIT_REF=<pr-branch>
# GitHub App credentials (see docs/e2e-real-github.md and docs/github-app.md):
export GITHUB_APP_ID=<...>
export GITHUB_APP_INSTALLATION_ID=<...>
export GITHUB_APP_PRIVATE_KEY="$(cat ~/Downloads/flux-ai-bridge.*.pem)"
# Read by `make e2e` to fetch the posted comment (any token that can read the
# repo's PRs), e.g. export GITHUB_TOKEN=$(gh auth token)
export GITHUB_TOKEN=<token>

# Pick one LLM mode:
export OPENAI_API_KEY=<azure-key>
E2E_LLM=azure make e2e-up
# OR
E2E_LLM=ollama make e2e-up

make e2e
```

The PR will receive a real, model-generated comment.

---

## Switching back to mock mode

```bash
unset E2E_LLM
make e2e-down && make e2e-up && make e2e
```

Both Ollama install AND Azure auth are gated entirely on `E2E_LLM`, so
the default flow remains fast and free.

---

## Troubleshooting

### Ollama: `make e2e-up` hangs at "Installing Ollama"

The chart's `--wait` blocks until the model is pulled. A cold pull of
`qwen2.5-coder:1.5b` is ~1 GB and can take a few minutes on a slow link. The
HelmRelease timeout is 20 min. If it actually fails, check:

```bash
kubectl --context kind-flux-ai-bridge-e2e -n ollama logs deploy/ollama
```

### Ollama: inference times out

Small models on CPU are slow. With qwen2.5-coder:1.5b on a 4-core box each
prompt typically lands in 5–15 s. Bigger models on the same hardware can
take minutes. Either give the test more time or step up to a beefier model
on a beefier host.

### Azure AI Foundry: bridge logs `401 Unauthorized` / `Bad credentials`

Your `OPENAI_API_KEY` is wrong, expired, or your account hasn't accepted
the GitHub Models preview terms. Verify with a direct curl:

```bash
curl -sS https://models.inference.ai.azure.com/chat/completions \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_tokens":4}'
```

A 401 here means the key isn't valid for Models — open
<https://github.com/marketplace/models> and accept the preview terms, then
retry.

### Azure OpenAI Service: bridge logs `401 Unauthorized`

Different cause than Foundry. Verify the key directly against your tenant
(remember the URL shape includes the deployment name + api-version):

```bash
curl -sS "${OPENAI_BASE_URL%/}/openai/deployments/${E2E_LLM_MODEL}/chat/completions?api-version=2024-10-21" \
  -H "api-key: $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"hi"}],"max_tokens":4}'
```

If THAT 401s: key is wrong/expired. If THAT works but the bridge still
401s: the `OPENAI_BASE_URL` reaching the bridge isn't a bare Azure resource
endpoint (the bridge uses OpenAI-compatible Bearer auth unless the host is
`*.openai.azure.com` / `*.cognitiveservices.azure.com` with no path or just
`/openai`). Check with:

```bash
kubectl --context kind-flux-ai-bridge-e2e -n flux-system get deploy/flux-ai-bridge \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="OPENAI_BASE_URL")].value}'
```

### Azure: `404 model not found` / `DeploymentNotFound`

For **Foundry**, the model catalogue uses specific identifiers (e.g.
`gpt-4o-mini`, not `gpt-4-mini`). See <https://github.com/marketplace/models>.

For **Azure OpenAI Service**, the `E2E_LLM_MODEL` value is the
**deployment** name configured in the Azure portal, NOT a generic model
name. List deployments under your resource → **Deployments** in the
portal.

### Comment body fails the "mentions podinfo" check

Some models hallucinate the workload name. For Ollama, switch to a bigger
model:
`E2E_LLM=ollama E2E_LLM_MODEL=qwen2.5-coder:7b make e2e-up`.
For Azure, this almost never happens with `gpt-4o-mini` or larger.

### The bridge logs `unsupported LLM_PROVIDER "<value>"`

The HelmRelease values didn't get the right `llm.provider`. Confirm with:

```bash
kubectl --context kind-flux-ai-bridge-e2e -n flux-system \
  get helmrelease flux-ai-bridge -o yaml | grep -A12 'values:'
```

Should read `provider: openai-compatible` (under `llm:`) for both Ollama and Azure
modes.
