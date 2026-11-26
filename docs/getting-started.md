# Getting Started

This guide walks you through deploying `flux-ai-bridge` to your Kubernetes cluster and connecting it to Flux CD. By default the bridge runs without an LLM (`config.llm.provider=none`) and posts a deterministic kstatus summary on each Flux event — only the GitHub App credentials are mandatory. Adding an LLM (Claude or any OpenAI-compatible endpoint) layers a natural-language analysis on top of the same structured comment; that step is optional and covered in §2.

## Prerequisites

| Requirement | Notes |
|---|---|
| Kubernetes ≥ 1.25 | Any distribution |
| Flux CD ≥ 2.2 | With Notification Controller (the chart's Provider and Alert use `notification.toolkit.fluxcd.io/v1` on Flux ≥ 2.10, else `v1beta3`) |
| [flux-operator-mcp](https://github.com/controlplaneio-fluxcd/flux-operator) | Installed by the bundled sub-chart by default; set `flux-operator-mcp.enabled=false` to point at your own install. Flux does **not** need to be managed by the Flux Operator — `flux bootstrap`, `flux install` or the Helm chart work the same, since the MCP server only reads Kubernetes resources. Only the opt-in `FAILURE_FETCH_FLUX_INSTANCE` needs the Operator's `FluxInstance` |
| GitHub App | Installed on the repos you want commented on. See [docs/github-app.md](github-app.md) for the full setup (App ID, installation ID, private key PEM) |
| Helm ≥ 3.14 | For chart installation |
| `kubectl` with cluster access | |
| *(Optional)* LLM account | Only if you want an LLM-written summary on top of the [kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus) table the bridge produces by default: [Anthropic Claude](https://www.anthropic.com/) or any [OpenAI-compatible endpoint](local-llm.md) (Ollama, vLLM, OpenAI proper, Azure) |

---

## 1. Create the Secret

The bridge authenticates to GitHub as a **GitHub App** — see [docs/github-app.md](github-app.md) for how to create one and capture the App ID, installation ID, and private key. With the chart's default `config.llm.provider=none`, the GitHub App credentials are the only Secret keys required. Create the Secret before installing the chart, then reference it with `existingSecret`. The commands below use `flux-system`, Flux's default namespace. To install into a namespace that doesn't exist yet, create it first (`kubectl create namespace <namespace>`).

```bash
kubectl create secret generic flux-ai-bridge \
  --namespace flux-system \
  --from-literal=GITHUB_APP_ID=12345 \
  --from-literal=GITHUB_APP_INSTALLATION_ID=67890 \
  --from-file=GITHUB_APP_PRIVATE_KEY=./flux-ai-bridge.<id>.private-key.pem
```

> To enable an LLM-written summary, add `ANTHROPIC_API_KEY=…` (for Claude) or `OPENAI_API_KEY=…` (for OpenAI-compatible — may be omitted when the endpoint doesn't authenticate, e.g. local Ollama). The LLM-write-up section below shows the matching `helm upgrade`.

---

## 2. Install the Helm Chart

The chart's sub-charts (flux-operator-mcp, plus the optional Ollama and Valkey) aren't stored in the repository. Fetch the versions pinned in `Chart.yaml` once per checkout (`make chart-deps` does the same) — Helm only resolves chart repositories that are registered:

```bash
helm repo add otwld https://helm.otwld.com/ --force-update
helm dependency update ./charts/flux-ai-bridge
```

### Default (no LLM)

```bash
helm install flux-ai-bridge ./charts/flux-ai-bridge \
  --namespace flux-system \
  --create-namespace \
  --set existingSecret=flux-ai-bridge
```

The bridge will post a deterministic kstatus summary on every Flux event — no LLM, no API key, no per-token cost. See [configuration.md → No-LLM mode](configuration.md#no-llm-mode) for the output shape.

### With Claude

```bash
helm install flux-ai-bridge ./charts/flux-ai-bridge \
  --namespace flux-system \
  --create-namespace \
  --set existingSecret=flux-ai-bridge \
  --set config.llm.provider=claude
```

Requires `ANTHROPIC_API_KEY` in the Secret created above.

### With OpenAI-compatible

```bash
helm install flux-ai-bridge ./charts/flux-ai-bridge \
  --namespace flux-system \
  --create-namespace \
  --set existingSecret=flux-ai-bridge \
  --set config.llm.provider=openai-compatible \
  --set config.llm.openaiBaseUrl=<endpoint> \
  --set config.llm.model=<model>
```

Requires `OPENAI_API_KEY` in the Secret (omit for local Ollama). Set `openaiBaseUrl` to your endpoint, e.g. `https://api.openai.com/v1` for OpenAI proper. Left empty, the bridge uses GitHub Models (`https://models.inference.ai.azure.com`) and sends the key there, unless the bundled Ollama (`ollama.enabled=true`) wires it up.

> The chart creates the Flux `Provider` and `Alert` for you by default (`flux.notification.enabled=true`). Pass `--set flux.notification.enabled=false` to skip them.

### With a values file (recommended for production)

Create `my-values.yaml`:

```yaml
existingSecret: flux-ai-bridge

config:
  llm:
    provider: claude   # or "none" for kstatus-only comments, or "openai-compatible"
  github:
    repo: "myorg/my-infra-repo"   # optional: skips auto-discovery

flux:
  notification:
    enabled: true
    namespace: flux-system
```

Then install:

```bash
helm install flux-ai-bridge ./charts/flux-ai-bridge \
  --namespace flux-system \
  --create-namespace \
  -f my-values.yaml
```

---

## 3. Verify the Deployment

```bash
# Pod is running
kubectl -n flux-system get pods -l app.kubernetes.io/name=flux-ai-bridge

# Health endpoint responds
kubectl -n flux-system port-forward svc/flux-ai-bridge 8080:8080 &
curl http://localhost:8080/healthz
# → {"status":"ok"}

# Run the Helm test
helm test flux-ai-bridge -n flux-system
```

---

## 4. Configure Flux Notification Controller

If you kept the default `flux.notification.enabled=true` the Provider and Alert were already created for you. Skip to step 5.

If you opted out (`flux.notification.enabled=false`), apply them manually. On Flux older than 2.10, use `notification.toolkit.fluxcd.io/v1beta3` instead of `v1`:

```yaml
apiVersion: notification.toolkit.fluxcd.io/v1
kind: Provider
metadata:
  name: flux-ai-bridge
  namespace: flux-system
spec:
  type: generic
  address: "http://flux-ai-bridge.flux-system.svc.cluster.local:8080/webhook"
---
apiVersion: notification.toolkit.fluxcd.io/v1
kind: Alert
metadata:
  name: flux-ai-bridge
  namespace: flux-system
spec:
  providerRef:
    name: flux-ai-bridge
  eventSeverity: info
  eventSources:
    - kind: Kustomization
      name: "*"   # watch all Kustomizations
```

---

## 5. End-to-End Workflow Test

1. Open a PR on your infra repository that changes a Kustomization.
2. Add the `dev` label — [autodev](autodev.md) merges it into your `dev` branch. This needs the autodev workflow set up in that repository and the cluster's Flux source tracking `dev`. Without autodev, point the Flux source at a branch that holds the PR's commits while the PR stays open, such as the PR branch itself.
3. Flux reconciles the dev cluster.
4. The Notification Controller sends a webhook to the bridge.
5. The bridge queries `flux-operator-mcp`, evaluates kstatus on every inventory resource (and, when enabled, on pods + HelmRelease child resources), and posts a comment on the PR. The LLM is called only when something needs analysis — see `LLM_SKIP_ON_HEALTHY` in [configuration.md](configuration.md).

Expected PR comment (no-LLM default, fully healthy). With the chart default (`COMMENT_FAILED_RESOURCES_ONLY=true`) an all-healthy Kustomization collapses to a compact per-Kustomization row; the summary line is led by the Kustomization name:

```markdown
<!-- flux-ai-bridge:flux-system/my-app -->
# ✅ Flux Status

Kustomization `my-app`: All 4 resource(s) and 2 pod(s) reached [kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus) `Current`.

## Resource Status
| Kustomization | Resources | Status |
|---|---|---|
| my-app | 4 | ✅ Current |

## Workload Status
| Kustomization | Pods | Status |
|---|---|---|
| my-app | 2 | ✅ Current |

---
*Analyzed by flux-ai-bridge*
```

(With `COMMENT_FAILED_RESOURCES_ONLY=false` you get the full per-resource/per-pod tables instead; on a failure the comment always switches to those detailed tables, listing only the non-`Current` rows.)

With an LLM configured (`provider=claude` or `openai-compatible`), the LLM's narrative is **appended below** the same `## Resource Status` and `## Workload Status` tables — the model authors `## Kustomization Status` and, on failures, `## Root Cause` + `## Next steps to fix`. The bridge owns the marker, header, kstatus tables, and footer in every mode; the LLM only contributes prose. Data on top, interpretation below. The idempotency marker is per-Kustomization: `<!-- flux-ai-bridge:<namespace>/<name> -->`.

---

## Troubleshooting

### No comment appears on the PR

1. Check bridge logs: `kubectl -n flux-system logs -l app.kubernetes.io/name=flux-ai-bridge`
2. Verify the Flux Alert is healthy: `kubectl -n flux-system get alert flux-ai-bridge`
3. Verify the Flux Provider is healthy: `kubectl -n flux-system get provider flux-ai-bridge`
4. Confirm the GitHub App is installed on the target repo and has **Pull requests: Read**, **Issues: Read and write** and **Contents: Read** — see [docs/github-app.md](github-app.md).
5. If `GITHUB_REPO` is not set, check that the Kustomization's GitRepository has a valid `spec.url`.

### bridge pod is CrashLoopBackOff

Check the startup logs — the most common causes are:

- `MCP_URL is required` — no MCP URL reached the bridge (with the chart, `flux-operator-mcp.enabled=false` and no `config.mcp.url`; the chart now refuses to render that).
- `create mcp client: initial mcp session: …` — `config.mcp.url` is wrong or `flux-operator-mcp` is not reachable (pod not running, or a NetworkPolicy blocking the bridge — e.g. an MCP server outside the release and Flux namespaces needs `networkPolicy.egress.extra`).

Timeouts to GitHub, the LLM or Redis while the pod runs usually mean the chart's NetworkPolicy (on by default) doesn't cover that endpoint: it allows DNS, the Flux and release namespaces, ports 443/6443, the configured Redis ports, and the endpoints set in `config.llm.openaiBaseUrl` / `anthropicBaseUrl` and `config.github.apiUrl`. Name in-cluster endpoints with their `.svc` form (`http://gw.ai.svc.cluster.local:4000`), and add anything else via `networkPolicy.egress.extra`, see [configuration.md](configuration.md).
- `GitHub App config required: set GITHUB_APP_ID + GITHUB_APP_INSTALLATION_ID + GITHUB_APP_PRIVATE_KEY (or GITHUB_APP_PRIVATE_KEY_FILE)` — one or more of the three App env vars is missing from the Secret.
- `ANTHROPIC_API_KEY is required` — Using Claude but the key is absent from the Secret.

### PR not found for commit SHA

The bridge logs and skips when no PR is associated with a commit. This can happen if:

- The commit was not pushed via a PR (direct push to `dev`).
- The GitHub App is not installed on the repo, or the repo is not in "Selected Repositories".
- The repository is not set correctly (`config.github.repo` or auto-discovery failed).
