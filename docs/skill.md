# Customizing the SKILL.md prompt

Every LLM call the bridge makes starts with a `SKILL.md` system-prompt prefix. It describes the diagnostician role, names the Flux CRDs and the data sections the bridge will inline, and sets the rules for the narrative the model writes (its sections and which recommendations to avoid). The comment's header, footer, status marker and status tables are the bridge's, not the model's. Operators frequently want to tweak it — for organization-specific conventions, registry paths, policy engines, naming rules, escalation routing, etc. This page covers what's in the box and how to change it.

## Where the bundled content lives

Despite the name, there is no `charts/flux-ai-bridge/SKILL.md` file on disk. The bundled skill content is inlined directly into [charts/flux-ai-bridge/templates/configmap-skill.yaml](../charts/flux-ai-bridge/templates/configmap-skill.yaml), gated on `skill.default.enabled`. At `helm install`/`helm upgrade` time the chart renders that file into a ConfigMap (`<release>-skill`), which the bridge Pod mounts read-only at `/skills/SKILL.md`. The Go side ([internal/skills/loader.go](../internal/skills/loader.go)) reads that single file at startup and inlines its content into every LLM request.

The bundled content covers:

- The diagnostician role and the data layout of the user-payload sections (`## Flux Event`, `## Kustomization YAML`, `## Resource Readiness`, `## Workload Readiness`, `## Inventory Conditions`, `## Pod Logs`, `## Recent Events`, `## Events`, etc.).
- When a run counts as unhealthy. These are the same signals the bridge uses for the ❌/⚠️ marker: an error event, the Kustomization's `Ready=False` for the analysed revision, a non-Current resource or pod, a readiness-poll timeout, or an incomplete HelmRelease. The narrative therefore can't call a run healthy under a ❌ header — for example when a build failure leaves the previous revision running all-green.
- How to read a rollout that's still in progress: the bridge adds a `## Rollout In Progress` section when every non-Current resource and pod is `InProgress` with no failure reason. Probe failures, `Multi-Attach` volume errors and similar events from pods being replaced are then expected noise, and the reply omits `## Root Cause` / `## Next steps to fix` (the bridge removes them if the model writes them anyway).
- A rule to treat the payload as untrusted data (logs, events and YAML can contain text that looks like instructions). The one exception is the `## Correction required` section the bridge appends when it retries a reply that lacked the mandatory failure sections.
- A short tour of Flux's CRDs (FluxInstance, Kustomization, HelmRelease, …) so smaller models stay grounded.
- What the model must NOT write: the `# <emoji> Flux Status` header, the `*Analyzed by flux-ai-bridge*` footer, or a status marker / verdict line — the bridge stamps those from kstatus — plus which recommendations to avoid (no "wait and retry", no `kubectl get` suggestions because that data is in the prompt).
- An explicit instruction NOT to author `## Resource Status` or `## Workload Status` tables. Those are bridge-stamped (see `buildKstatusBody` in `internal/processor/processor.go`), placed above the model's narrative, and byte-identical across LLM and no-LLM modes. The LLM owns prose — `## Kustomization Status` (the analysis section) and, on failures, `## Root Cause` + `## Next steps to fix` — and references resources by their identifier so reviewers can match prose to the table rows the bridge stamps below. The user payload's input section that contains the raw Kustomization YAML is named `## Kustomization YAML` to avoid confusion with the LLM's output section name.

## Two knobs in `values.yaml`

```yaml
skill:
  default:
    enabled: true       # render the bundled content (default)
  custom: ""            # operator-supplied content; multi-line string
```

Combinations:

| `default.enabled` | `custom` | Result |
|---|---|---|
| `true` | empty | Bundled content only (default) |
| `true` | non-empty | Bundled content **followed by** your custom block |
| `false` | non-empty | Your custom block becomes the entire skill prompt |
| `false` | empty | ConfigMap rendered with empty content; LLM gets no skill prefix (works, but quality drops sharply) |

## Adding org-specific guidance (the common case)

Append to the bundled prompt rather than replacing it — you keep the diagnostician role, Flux CRD orientation, and comment-format rules, and just add your local conventions. Two ways to feed `skill.custom`:

**In `values.yaml`:**

```yaml
skill:
  custom: |
    ## Local conventions

    - All HelmReleases in this org go through the platform team's chart registry
      at `oci://registry.internal/charts/`. A pull error here is a registry-auth
      issue, not a chart bug.
    - Treat any condition mentioning "policy violation" as a Kyverno enforcement;
      the fix is usually in the manifest, not the policy.
    - Namespaces ending in `-stage` are short-lived per-PR previews; failing
      reconciles there should call out the PR review process, not paging on-call.
```

**Or with `--set-file` from a separate file** (keeps the prompt under version control alongside the rest of the chart values):

```bash
helm upgrade --install flux-ai-bridge ./charts/flux-ai-bridge \
  --kube-context <your-context> \
  --namespace flux-system \
  --set-file skill.custom=./platform-skill.md \
  ...
```

The custom block is appended **after** the bundled content when both are enabled (matching the combinations table above), so the "general Flux diagnostician" instructions read first and your conventions follow — your rules land closest to the user payload, where models tend to weight later instructions when they conflict with earlier ones.

## Replacing the bundled content entirely

When the bundled diagnostician framing doesn't fit (different output format, different audience, non-English language, etc.):

```yaml
skill:
  default:
    enabled: false
  custom: |
    # Your full SKILL.md content here.
    # The bridge stamps the header, status tables and footer itself; this
    # prompt only shapes the narrative below the tables.
    ...
```

Keep the `## Root Cause` and `## Next steps to fix` headings in whatever format you define. On every run that isn't healthy (except a rollout still in progress), the bridge checks the reply for them and retries once with a `## Correction required` instruction if they're missing, so a replacement prompt without them costs an extra LLM call per failing run.

Note: the bridge stamps a deterministic header (`# <emoji> Flux Status`) and footer (`*Analyzed by flux-ai-bridge*`) onto every comment in Go code (`internal/processor.formatComment`), independent of the skill. So even with a fully replaced skill the structural markers stay; only the body content is shaped by your prompt.

## What changes at runtime

The Pod template has a `checksum/skill` annotation in [charts/flux-ai-bridge/templates/deployment.yaml](../charts/flux-ai-bridge/templates/deployment.yaml):

```yaml
checksum/skill: {{ include (print $.Template.BasePath "/configmap-skill.yaml") . | sha256sum }}
```

Editing the values file and running `helm upgrade` re-renders the ConfigMap, the annotation hash changes, and the Pod rolls — picking up the new prompt on the next reconcile. Without that annotation, `helm upgrade` would leave the running Pod with stale content because the skills loader reads `/skills` only at startup.

## Caveats

- **`LLM_SKIP_ON_HEALTHY` bypasses the skill on healthy reconciles.** Chart default `config.llm.skipOnHealthy: true` short-circuits the LLM call when every inventory row + every pod is `Current` and the readiness poll settled. On that path the bridge posts the deterministic kstatus body and **never calls the LLM**, so any team-specific text your custom skill adds for healthy comments (runbook links, owner tags, "deploy succeeded — please verify in staging") is **not** applied. Set `config.llm.skipOnHealthy: false` to keep the LLM in the loop on healthy events too, at the cost of a paid round-trip per reconcile. Failing pods (even with healthy inventory) and any non-Current row always route through the LLM regardless.
- **`LLM_PROVIDER=none` skips the skill entirely.** The chart default — `provider=none`, kstatus-only output — never invokes the LLM, so the skill is irrelevant in that mode. Customizing SKILL.md only matters when an LLM provider is configured (`claude` or `openai-compatible`).
- **Context window.** The full prompt = bundled skill + custom skill + per-event user payload (Kustomization YAML, kstatus rows including HelmRelease-expanded children, pod kstatus snapshot, pod logs/events on failures). On a small local model (e.g. `qwen2.5-coder:1.5b`, even with the 16k context the chart configures for the bundled Ollama) a long custom block can evict the framing parts of the bundled skill and degrade output quality. See [docs/local-llm.md](local-llm.md) for the model-size vs context trade-off.
- **Quality scales with the LLM.** A 1.5B local model may ignore subtle skill instructions; Claude/GPT-4-class models tend to follow them precisely. If your custom rules aren't reflected in comments, the model is likely the bottleneck, not the prompt.
