# Running flux-ai-bridge with a local LLM (Ollama)

The chart bundles an **optional Ollama sub-chart** for zero-credentials
installs: opt in with `ollama.enabled: true` and the chart auto-derives
`OPENAI_BASE_URL` to the in-cluster Ollama Service. You must **also** set
`config.llm.provider: openai-compatible` and `config.llm.model`
(e.g. `qwen2.5-coder:1.5b`, the sub-chart's pre-pulled model) yourself —
enabling the sub-chart only wires the URL, not the provider or model.

It is **off by default** so chart-testing on GitHub-hosted runners doesn't OOM
pulling the model image, and so production installs aren't surprised by a
local LLM Pod. **The bundled Ollama is a smoke-test option, not a production
recommendation** — small local models break the bridge's deterministic
comment format frequently. For production prefer Anthropic Claude
(`config.llm.provider: claude` + an API key) or Azure OpenAI Service
(`openai-compatible` against a hosted endpoint). See the warning block above
the `ollama:` section in [charts/flux-ai-bridge/values.yaml](../charts/flux-ai-bridge/values.yaml) and the
"Trade-offs vs. Claude" table below.

This page explains how to enable the bundled Ollama, how to swap in a stronger
model, and how to point the bridge at an Ollama you already run elsewhere.

---

## Quick start (opt in to the bundled Ollama)

```bash
# once per checkout: fetch the sub-charts pinned in Chart.yaml (or: make chart-deps)
helm repo add otwld https://helm.otwld.com/ --force-update
helm dependency update ./charts/flux-ai-bridge

helm install flux-ai-bridge ./charts/flux-ai-bridge \
  --namespace flux-system \
  --create-namespace \
  --set existingSecret=flux-ai-bridge \
  --set ollama.enabled=true \
  --set config.llm.provider=openai-compatible \
  --set config.llm.model=qwen2.5-coder:1.5b
```

All three LLM-related `--set` flags are needed: `ollama.enabled=true` deploys
the sub-chart and auto-derives `OPENAI_BASE_URL`; the provider and model are
not switched automatically and must be set explicitly.

What this gives you:
- The bridge.
- The `otwld/ollama-helm` chart deployed in the same release, with
  `qwen2.5-coder:1.5b` pre-loaded.
- `OPENAI_BASE_URL` on the bridge is auto-wired to
  `http://ollama.<namespace>.svc.cluster.local:11434/v1` (no extra config).
- The bridge still authenticates to GitHub via its App (see
  [docs/github-app.md](github-app.md)). Ollama itself doesn't authenticate,
  so `OPENAI_API_KEY` can be omitted from the Secret for this provider.

The first reconcile after install takes a few minutes for Ollama to pull the
model. Subsequent calls are fast.

---

## Model choice

`qwen2.5-coder:1.5b` is the default because it's:
- ~1 GB on disk, ~1.5 GB RAM loaded.
- Code-tuned (the bridge's prompt is structured markdown about K8s/Flux).
- Fast on CPU (5–15 s per response on a modern 4-core box).

If you have more headroom (or you find the 1.5b model's analysis too thin):

| Model | Disk | RAM | Notes |
|---|---|---|---|
| `qwen2.5-coder:1.5b` | ~1 GB | ~1.5 GB | Default; viable on tiny nodes. |
| `qwen2.5-coder:7b` | ~4.7 GB | ~5 GB | Much better failure analysis; ~3–5× slower on CPU. |
| `llama3.2:3b` | ~2 GB | ~2.5 GB | Generalist; weaker on code/yaml than Qwen Coder. |
| `gpt-oss:20b` | ~13 GB | ~14 GB | Closer to commercial quality; needs GPU or beefy CPU node. |

To switch models, override both `config.llm.model` and the Ollama pull/run
lists:

```yaml
config:
  llm:
    provider: openai-compatible
    model: qwen2.5-coder:7b

ollama:
  enabled: true
  ollama:
    models:
      pull: [qwen2.5-coder:7b]
      run:  [qwen2.5-coder:7b]
  resources:
    requests:
      cpu: 500m
      memory: 6Gi
    limits:
      memory: 8Gi
```

---

## Persistence: don't re-pull on every restart

By default `ollama.persistentVolume.enabled: false` — the model lives in an
`emptyDir` and is re-pulled when the pod restarts. For anything beyond a
quick demo, enable persistence:

```yaml
ollama:
  enabled: true
  persistentVolume:
    enabled: true
    size: 10Gi
    storageClass: ""   # default StorageClass on the cluster
```

The PV needs to be large enough for every model you might pull (1.5b ≈ 1 GB;
7b ≈ 5 GB). Use a cluster-appropriate `storageClass` — most cloud-managed
K8s offers a sensible default; on bare metal, `local-path-provisioner` works
for single-node setups.

---

## Bring your own Ollama install

If you already have Ollama running somewhere (a workstation, a separate
deployment, a sidecar), set `ollama.enabled: false` (the default) and point
the bridge at it directly:

```yaml
config:
  llm:
    provider: openai-compatible
    model: qwen2.5-coder:1.5b
    openaiBaseUrl: "http://my-ollama.my-ns.svc.cluster.local:11434/v1"
```

The chart's NetworkPolicy (on by default) lets the bridge reach the
`my-ns` namespace because of this URL. Use the `.svc` form for in-cluster
endpoints; a short `my-ollama.my-ns` can't be told apart from an external
hostname.

The bridge speaks the OpenAI `/v1/chat/completions` API; anything that
implements that contract works (Ollama, vLLM, OpenAI proper, Azure
model-inference, etc.).

---

## What the bridge actually sends

Prompt shape (LLM input, ~10–15 k tokens depending on event — the SKILL.md prefix is the bulk):
1. System: bridge preamble (`"You are a Kubernetes GitOps expert…"`) + the inlined SKILL.md methodology.
2. User: structured markdown with the Flux event, the Kustomization status YAML (with `.status.inventory` stripped to save tokens), readiness verdicts as an "X of N reached Current" count plus a line per non-Current resource (`## Resource Readiness`, HelmReleases expanded to their children when `READINESS_EXPAND_HELMRELEASES=true`), the same count+failures shape for pods (`## Workload Readiness`, each pod labeled with its owning controller; gated by `READINESS_INCLUDE_PODS=true`), and on failures the appended failure-context sections (pod logs, namespace Events, inventory conditions).

The pod section alone adds ~200–500 tokens per reconcile (one line per pod in inventory namespaces). On a small CPU-only Ollama this is noticeable but still fits comfortably in the bumped 16k context window the chart sets via `OLLAMA_CONTEXT_LENGTH`.

A 1.5b model is enough to follow this. Smaller models (0.5b range) routinely
hallucinate resource names or skip sections. If you see degenerate comments
(unrelated text, missing sections, or just regurgitated methodology), the
first thing to try is bumping to `qwen2.5-coder:7b`.

---

## Sizing the Ollama pod

The chart sets **no Ollama resources by default** — set them yourself via
`ollama.resources`. A working starting point for the 1.5b model on cluster
nodes that aren't already saturated: `requests: cpu=200m, memory=2Gi;
limits: memory=4Gi`. Each inference uses one CPU core for ~5–15 s.

Rules of thumb:
- **CPU-only:** plan for one inference per worker core. The bridge has a
  bounded concurrency cap (`config.webhook.maxConcurrent`, default 16), but in
  practice multiple concurrent Kustomization events serialize at the LLM
  level — small models on a single CPU core handle one event at a time.
- **GPU:** the `otwld/ollama-helm` chart supports `ollama.gpu.enabled: true`
  and `ollama.gpu.type: nvidia|amd`. With a GPU, even 7b–13b models become
  viable, and concurrency rises with VRAM.

For sustained load (many repos, frequent reconciles), keep the bridge's
per-PR-head dedupe on (the default: each PR is analysed once per its own head
commit; don't disable it via `config.dedupe.ttl: "0"`) — that's the main
defense against blowing through your LLM throughput budget.

---

## Trade-offs vs. Claude

| | Bundled Ollama | Anthropic Claude |
|---|---|---|
| Cost | Free (your hardware) | Per-token API charge |
| Latency | 5–15 s on CPU, 1–3 s on GPU | ~1–3 s |
| Quality | Acceptable for routine events; weaker on novel failures | Strong on every event |
| Privacy | Nothing leaves the cluster | Prompt + payload sent to Anthropic |
| Setup | One Helm flag (`ollama.enabled=true`) | API key |
| Cold start | First reconcile after install pulls the model | Instant |

A defensible mixed setup: Claude in production (best quality), bundled
Ollama in dev / on-prem clusters where egress is restricted.

---

## Troubleshooting

### Bridge logs `unsupported LLM_PROVIDER "<value>"`
You set `config.llm.provider` to something the bridge doesn't know.
Currently valid: `none`, `claude`, or `openai-compatible`. `none` is the chart default and turns the LLM off entirely — the bridge posts a deterministic kstatus comment. See [configuration.md → No-LLM mode](configuration.md#no-llm-mode).

### Bridge comments contain incoherent text or skip sections
The model is too small for the prompt size. Switch to a bigger model
(7b range). If that's not possible, check the prompt size: with skills
inlined it can hit ~15 k tokens, which a 1.5b model handles
poorly.

### Ollama pod stuck in `0/1 Running`
Model pull is slow. The first pull of `qwen2.5-coder:1.5b` is ~1 GB; on a
slow link it takes 5–20 minutes. `kubectl -n <ns> logs deploy/ollama` shows
progress. If repeated runs keep re-pulling, enable persistence (see above).

### `helm install` reports `Progress deadline exceeded`
K8s' default Deployment `progressDeadlineSeconds: 600` trips during slow
model pulls. The release marks `failed` even though the pod recovers and the
model finishes loading. Either install without `--wait`, or patch the
Deployment's `progressDeadlineSeconds` to a higher value after install:

```bash
kubectl -n <ns> patch deployment ollama --type=merge \
  -p '{"spec":{"progressDeadlineSeconds":1800}}'
```
