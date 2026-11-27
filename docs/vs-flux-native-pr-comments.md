# flux-ai-bridge vs. Flux's native PR/MR comment providers

Flux 2.8+ ships built-in PR/MR comment providers (`githubpullrequestcomment`, `gitlabmergerequestcomment`, `giteapullrequestcomment`) — see the [upstream provider docs](https://fluxcd.io/flux/components/notification/providers/#change-request-comments). flux-ai-bridge solves the same shaped problem (post a comment on the originating PR when Flux reconciles a change) but for a different operational profile. This doc spells out the differences so you can pick the right tool — or run both.

## TL;DR

| | Native `githubpullrequestcomment` | flux-ai-bridge |
|---|---|---|
| **PR identity** | Operator must set `event.toolkit.fluxcd.io/change_request: "<PR#>"` (and `commit: <sha>`) on every Flux object | Auto-discovered from the commit SHA — no annotations needed |
| **Comment body** | The raw Flux event message (`"Reconciliation succeeded"`, `"Applied revision: …"`, error text) | Deterministic structural body in every mode: `# <emoji> Flux Status` header → optional summary → `## Resource Status` table (inventory rows, with HelmRelease children folded in one level) → `## Workload Status` table (per-pod, labeled with owning Deployment/StatefulSet/etc.) → `*Analyzed by flux-ai-bridge*` footer. With `provider=claude`/`openai-compatible` the same data is fed to an LLM that appends `## Kustomization Status` (and on failures `## Root Cause` + `## Next steps to fix`) **between the tables and the footer** — the LLM never authors the header, tables, or footer |
| **Comment lifecycle** | One new comment per event → long PRs accumulate a comment trail | Delete-and-recreate per analysis, keyed on a per-Kustomization marker `<!-- flux-ai-bridge:<ns>/<name> -->` → exactly one current comment per `(namespace, kustomization)` on each PR. PRs touching multiple Kustomizations get one comment each, independently maintained |
| **External dependencies** | Notification Controller + a GitHub PAT (or App-issued token) | + flux-operator-mcp + a GitHub App. An LLM is **optional** — chart default `provider=none` posts a deterministic kstatus comment without contacting any model |
| **HA** | Single Notification Controller dispatches | Active-active replicas coordinated via Redis/Valkey SETNX |
| **Failure-context fetching** | Whatever was in the Flux event | Sweeps the inventory's namespaces for failing pods, fetches their last 100 log lines (incl. `--previous` for crashloops), and pulls per-namespace Events filtered to those since the earliest failing pod's creation (so old events from a dead pod don't mislead the LLM). `top pods` and cluster-wide FluxInstance status are opt-in (`FAILURE_FETCH_METRICS` / `FAILURE_FETCH_FLUX_INSTANCE`, both off by default — they produce dense output the LLM tends to under-utilise). |

## The big functional differences

### 1. PR discovery without annotations (the autodev story)

The native provider needs the PR number on the Flux object before the event fires:

```yaml
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: podinfo
  namespace: flux-system
  annotations:
    event.toolkit.fluxcd.io/change_request: "42"
    event.toolkit.fluxcd.io/commit: "0123456789abcdef0123456789abcdef01234567"
spec: ...
```

That works fine for one-PR-per-environment flows. It does **not** work for dev-environment patterns where many PRs land into a shared branch — e.g. [Staffbase/autodev-action](https://github.com/Staffbase/autodev-action), which merges a labeled PR's HEAD into a `dev` branch that Flux reconciles. With that pattern:

- The Flux objects live in the dev branch and are static GitOps configuration — they don't know which PR triggered today's merge.
- Patching `change_request` onto a `Kustomization` from CI on every autodev merge means CI mutates Flux-managed resources, which fights the GitOps invariant ("the cluster matches the branch").
- Even if you did patch it, multiple PRs can be merged together (autodev batches), and a single Flux object can only carry one annotation value.

flux-ai-bridge sidesteps the annotation entirely: at event time it takes the event's revision SHA and asks the GitHub API which PR(s) that commit belongs to. Two layers:

1. Direct: `GET /commits/{sha}/pulls`. Hits when the commit is on a PR head branch.
2. Parent walk: when the commit is a merge commit (autodev merging `feature/X` into `dev`), `parents[1..]` are the merged-in branch tips, which ARE on the PR's head branch — probe each. Walks back to the merge base with the default branch (at most `PARENT_WALK_DEPTH` hops, default 200) so a chain of merge commits batched into one push still resolves, without wandering into history that is already merged.

See [architecture.md "PR-from-commit discovery (two-layer)"](architecture.md) for the implementation. The practical effect: you do **nothing** to the Flux manifests; the bridge resolves the right PR by inspecting commit history at event time.

### 2. Comment content — raw event message vs. curated explanation

The native commenter posts the Flux event's `message` field. That's typically one of:

- `Reconciliation finished, next run in 5m0s` (success — friendly but uninformative)
- `Reconciliation failed: error in object podinfo: …` (failure — terse, often missing the underlying cause)

flux-ai-bridge builds a richer per-event payload and stamps a deterministic structural comment regardless of whether an LLM is configured:

1. **Kustomization status** — the full reconciled inventory and conditions.
2. **kstatus readiness** — every resource in the inventory polled until terminal, so the analysis reflects the *settled* cluster, not mid-rollout. Inventory entries of kind `HelmRelease` are expanded one level: the chart-rendered children (Deployment/Service/etc.) listed in the HR's own `status.inventory` are folded into the readiness set, so the Resource Status table shows the actual workloads the chart created, not just "HelmRelease/foo: Current".
3. **Pod kstatus snapshot** — a scan of the Kustomization's **own** namespace (its `spec.targetNamespace` if set, else its **name** — the convention where Kustomization `grafana` deploys into the `grafana` namespace; default on, gated by `READINESS_INCLUDE_PODS`), with each pod labeled by its owning controller (Deployment/StatefulSet/etc., resolved via ownerReferences past the implementation-detail ReplicaSet).
4. **Failure context** (only when the event is Error or any inventory row isn't Current): pod logs (`--previous` first, falls back to current) and namespace Events filtered to those since the earliest failing pod's creation. `top pods` and cluster-wide FluxInstance status are opt-in toggles, off by default.
5. **Comment body assembled in Go**: header (`# <emoji> Flux Status`), optional one-line summary, `## Resource Status` table, `## Workload Status` table, footer (`*Analyzed by flux-ai-bridge*`). This body is byte-identical across LLM and no-LLM modes — reviewers see the same kstatus rendering at the top regardless of provider.
6. **LLM call (when configured)** — a SKILL.md system prompt frames the LLM as filling an *analysis slot* between the kstatus tables and the footer. It authors `## Kustomization Status` (analysis prose) and, on failures, `## Root Cause` + `## Next steps to fix`. It does NOT author the header, tables, or footer — those are Go-stamped. The chart default `config.llm.provider=none` skips this step entirely and the deterministic body is the whole comment. With an LLM configured, `LLM_SKIP_ON_HEALTHY` (default `true`) further short-circuits the LLM round-trip when every inventory row and every pod is `Current` and the readiness poll settled — saving a paid call on what would have produced a near-identical "all green" comment.

Output with `provider=claude`/`openai-compatible` and a real failure looks like:

```markdown
<!-- flux-ai-bridge:flux-system/podinfo -->
# ❌ Flux Status

Kustomization `podinfo`: 1 of 2 pod(s) failed.

## Resource Status
| Namespace | Kind | Name | Status | Message |
|---|---|---|---|---|
| podinfo | Deployment | podinfo | ⏳ InProgress | 2 of 3 updated replicas available |

## Workload Status
| Namespace | Controller | Pod | Status | Message |
|---|---|---|---|---|
| podinfo | Deployment/podinfo | podinfo-7d4f9b6c8-def34 | ❌ Failed | init container: ConfigMap "podinfo-config" not found |

## Kustomization Status

The `podinfo` Kustomization applied the manifests for commit `abc1234`, but the
init container references `ConfigMap/podinfo-config` which isn't in the
inventory.

## Root Cause

The Deployment's init container expects a ConfigMap that the autodev branch
doesn't include — likely missing from `kustomization.yaml` or an out-of-band
Secret that should be inherited from `main`.

## Next steps to fix

1. Add the ConfigMap to the kustomization resources, or
2. Verify the inheritance from `main` is wired up for this branch.

---
*Analyzed by flux-ai-bridge*
```

With `provider=none` (chart default) the same comment is posted without the `## Kustomization Status` / `## Root Cause` / `## Next steps to fix` blocks — header, tables, and footer are unchanged.

Whether the LLM prose is worth the extra moving parts depends on your audience: developers reviewing a PR usually appreciate the curated diagnosis; a platform operator who already knows Flux can read the kstatus tables directly. Either way, the kstatus tables are the floor — nothing the LLM does removes them.

### 3. Comment lifecycle — delete-and-recreate vs. append

Native: each event posts a new comment. A long-lived PR with 30 reconciles → 30 PR comments. The PR comment timeline gets noisy; the latest verdict is hidden under churn.

flux-ai-bridge: a per-Kustomization hidden HTML marker `<!-- flux-ai-bridge:<namespace>/<name> -->` identifies the bridge's existing comment. When the **cluster state** changes (idempotency is keyed on the deterministic kstatus portion — an LLM re-wording an unchanged state touches nothing), the bridge **creates** a fresh comment and then **deletes** the old one (matching *this Kustomization's* marker only; create-first so the PR never sits without a verdict), so a reviewer scrolling the PR always sees one current verdict per Kustomization at the top of the timeline. The per-Kustomization scoping is what makes the multi-Kustomization-per-PR story work — a PR that touches `apps/api` and `apps/worker` gets two bridge comments, each independently maintained by its own Kustomization's pipeline. The unscoped marker (`<!-- flux-ai-bridge -->`) is also recognized on read.

Opt-in to native-style behavior: `config.github.comments.keepHistory=true` flips the delete-and-recreate to append-only, so each analysis adds a new comment and the PR keeps a chronological trail. Useful when iterating on a workload that toggles healthy/broken and you want to read the history; off by default because the common case ("show me the current verdict") works better when the comment stays at the top.

Per-PR-head dedupe — keyed on `(namespace, kustomization, PR number, PR head SHA)`, governed by `DEDUPE_PR_REEVAL_TTL` (bridge default: analyse each PR-head exactly once; the Helm chart re-checks failed/warning heads every 15 minutes, up to 4 times) — ensures Flux's periodic re-reconciles don't re-trigger the pipeline for an unchanged PR head — so the bridge isn't constantly re-deleting and re-posting either.

### 4. Operational surface

Native:

- Just a `Provider` + `Alert` resource in `flux-system`.
- Auth: a Secret with a GitHub PAT (or App-issued token).
- No external services to host.

flux-ai-bridge:

- A bridge Deployment + Service in `flux-system`.
- Requires `flux-operator-mcp` to be reachable in-cluster (bundled sub-chart, or bring your own).
- An LLM endpoint — Anthropic Claude, OpenAI, Azure OpenAI Service, or any OpenAI-compatible (Ollama for self-hosted).
- A GitHub App (not just a PAT) — see [docs/github-app.md](github-app.md).
- For `replicaCount > 1`: Redis/Valkey for SETNX-based dedupe coordination.

## When to pick which

**Native** is the right fit when:

- You want a low-overhead "did Flux reconcile this PR's commit?" signal.
- Each PR maps cleanly to one Flux object and you can set the `change_request` annotation up front.
- You don't want to introduce an LLM into the feedback path.

**flux-ai-bridge** is the right fit when:

- Multiple PRs flow into a shared dev branch via auto-merge automation (autodev-action) and you can't annotate Flux objects with a PR number.
- You want PR reviewers to see *why* something failed — pod logs and kstatus verdicts rather than `error in object …`.
- Comment-trail noise is a real problem (many reconciles on one PR) and you'd rather see a single current verdict.

**Running both** is fine — they're complementary and use independent `Provider`/`Alert` pairs against the same Notification Controller. The native commenter gives an at-a-glance "Flux saw the commit" within seconds (a new comment per event); the bridge follows up with the curated diagnosis once the inventory has settled (`READINESS_POLL_TIMEOUT` budget, default 3m) and replaces its own previous comment so the verdict stays at the top. If you go this route, scope the native commenter narrowly (e.g. only on `severity: error` alerts) so the comment trail stays manageable.
