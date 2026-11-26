# flux-ai-bridge Helm chart

Installs [flux-ai-bridge](https://github.com/Staffbase/flux-ai-bridge): it receives Flux CD notification events, checks
the reconciled resources through `flux-operator-mcp`, and posts a status comment (optionally with an LLM-written
analysis) on the GitHub pull request behind the change.

## Prerequisites

- Kubernetes with Flux CD ≥ 2.2 (Notification Controller enabled)
- Helm ≥ 3.14
- A GitHub App installed on the target repositories ([setup](https://github.com/Staffbase/flux-ai-bridge/blob/main/docs/github-app.md))

## Install

Create the Secret with the GitHub App credentials, then install the chart:

```bash
kubectl --context <your-context> create secret generic flux-ai-bridge \
  --namespace flux-system \
  --from-literal=GITHUB_APP_ID=<app-id> \
  --from-literal=GITHUB_APP_INSTALLATION_ID=<installation-id> \
  --from-file=GITHUB_APP_PRIVATE_KEY=./<app>.private-key.pem

helm install flux-ai-bridge oci://ghcr.io/staffbase/charts/flux-ai-bridge \
  --kube-context <your-context> \
  --namespace flux-system \
  --set existingSecret=flux-ai-bridge
```

The chart creates the Flux `Provider` and `Alert` that send Kustomization events to the bridge. Without further values,
the bridge posts deterministic status comments and calls no LLM.

## Sub-charts

| Sub-chart | Default | Purpose |
|---|---|---|
| `flux-operator-mcp` | enabled | Read-only MCP server the bridge reads cluster state through |
| `valkey` | disabled, **recommended** | Persistent dedupe store, standalone or with Sentinel. Without it, dedupe and caches live in memory: a restart re-runs analyses (and LLM calls), and several replicas can't coordinate. Enable it with `valkey.enabled=true` and `config.dedupe.persistence.enabled=true` |
| `ollama` | disabled | Local LLM for a self-contained install |

## Configuration

Every setting is documented next to its value in
[values.yaml](https://github.com/Staffbase/flux-ai-bridge/blob/main/charts/flux-ai-bridge/values.yaml): what it does, its
default and how it interacts with other values. Start there.

For the background behind the settings, see the repository docs:

- [Getting started](https://github.com/Staffbase/flux-ai-bridge/blob/main/docs/getting-started.md)
- [Configuration reference](https://github.com/Staffbase/flux-ai-bridge/blob/main/docs/configuration.md): every environment variable and the Helm value that sets it
- [The autodev workflow](https://github.com/Staffbase/flux-ai-bridge/blob/main/docs/autodev.md): the recommended way to use the bridge

## License

Apache License 2.0, see [LICENSE](LICENSE).
