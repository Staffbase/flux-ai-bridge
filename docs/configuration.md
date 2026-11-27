# Configuration Reference

Complete reference for all environment variables and Helm chart values.

---

## Environment Variables

The bridge is configured entirely through environment variables. When deployed via Helm, non-sensitive variables are injected directly into the Deployment; sensitive ones come from a Kubernetes Secret.

### Required

| Variable | Description |
|---|---|
| `MCP_URL` | HTTP MCP endpoint of `flux-operator-mcp`. Example: `http://flux-operator-mcp.flux-system.svc.cluster.local:9090/mcp` |
| `LLM_PROVIDER` | LLM backend to use. One of: `claude`, `openai-compatible`, `none`. `none` disables the LLM entirely and posts a deterministic [kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus) summary table — see [No-LLM mode](#no-llm-mode) below |
| `GITHUB_APP_ID` | Numeric App ID of your GitHub App (from `github.com/settings/apps/<your-app>`). See [github-app.md](github-app.md) |
| `GITHUB_APP_INSTALLATION_ID` | Installation ID of the App on your org/account (from the install URL: `/installations/<NUMBER>`) |
| `GITHUB_APP_PRIVATE_KEY` | PEM contents of the App's private key (inline). Set this or `GITHUB_APP_PRIVATE_KEY_FILE`; if both are set, the file wins |

### Conditionally Required

| Variable | Condition | Description |
|---|---|---|
| `GITHUB_APP_PRIVATE_KEY_FILE` | When `GITHUB_APP_PRIVATE_KEY` is not set | Path to the PEM file on disk. Alternative to the inline `_KEY` variant |
| `ANTHROPIC_API_KEY` | `LLM_PROVIDER=claude` | Anthropic API key |
| `OPENAI_API_KEY` | `LLM_PROVIDER=openai-compatible` (when the endpoint authenticates) | OpenAI / OpenAI-compatible API key. Omit when the endpoint accepts unauthenticated calls (e.g. local Ollama) |

### Optional

| Variable | Default | Description |
|---|---|---|
| `WEBHOOK_PORT` | `8080` | TCP port the webhook HTTP server listens on |
| `WEBHOOK_MAX_CONCURRENT` | `16` | Maximum number of webhook events processed concurrently. Further events are accepted and wait for a free slot, up to `WEBHOOK_QUEUE_SIZE` of them; only beyond that are they rejected with `503 Service Unavailable` |
| `WEBHOOK_QUEUE_SIZE` | `512` | How many accepted events may wait for a free processing slot. A new source revision makes every Kustomization that uses it send an event within seconds, so keep it above the number of Kustomizations the bridge watches. Beyond it the bridge answers `503`, which notification-controller retries only a few times within about a minute; after that the event is lost. A waiting event costs a few KB of memory, and its processing deadline starts only once it gets a slot. Must be > 0 (anything else uses the default); values above `1048576` use `1048576` |
| `WEBHOOK_HMAC_SECRET` | unset (HMAC validation disabled; the chart sets it by default) | Shared secret for validating webhook payloads. When set, every `POST /webhook` must carry an `X-Signature: sha256=<hex>` header — an HMAC-SHA256 over the raw request body — so configure the Flux `Provider` as `type: generic-hmac` with the same token. The Helm chart generates the token and wires both sides by default (`flux.notification.hmac.*`). Empty = no signature check, and the bridge logs a warning at startup |
| `LOG_LEVEL` | `info` | One of `debug`, `info`, `warn`, `error`. `debug` emits the full LLM prompt and response — useful for diagnosing analysis quality, noisy in steady state. The debug prompt includes failing pods' log lines, which can hold secrets: treat debug logs as sensitive as the pod logs. |
| `INSTANCE_ID` | unset | Set when bridges in **several clusters** report on the same repositories. It is written into every comment marker (`<!-- flux-ai-bridge@<id>:<ns>/<name> -->`) and into the Redis key prefixes (`flux-ai-bridge:<id>:dedupe:` / `:fetchcache:`), so each instance keeps its own comment per Kustomization and its own dedupe/cache state — without it, same-named Kustomizations in two clusters claim and delete each other's comments. PR labels can't be scoped this way: give each instance its own `PR_LABEL_*` names. A DNS label (lowercase letters, digits, `-`, at most 63 characters). Changing it later orphans comments posted under the old value |
| `SHUTDOWN_TIMEOUT` | `30s` | Total shutdown budget on SIGTERM. In-flight pipelines get it minus a 10s unwind reserve (at most half the timeout) to finish; then they are cancelled and roll back the dedupe marks of PRs they didn't deliver. Keep it below the pod's `terminationGracePeriodSeconds` (chart default 45), or SIGKILL cuts the rollback short and those PR-heads stay deduped until their mark expires. Must be > 0 |
| `LLM_MODEL` | See table below | Override the default model for the selected provider |
| `LLM_MAX_TOKENS` | `2048` | Hard cap on LLM output tokens. Truncations log WARN and increment `flux_ai_bridge_llm_max_tokens_hit_total`. With an Ollama endpoint (port 11434) the cap is sent as `max_tokens`, the only field Ollama reads; other endpoints get `max_completion_tokens` |
| `LLM_SKIP_ON_HEALTHY` | `true` | Short-circuit the LLM call when settled + every inventory row + every scanned pod reached `Current`. The kstatus body fills in the same `## Resource Status` table the LLM would have produced for a happy path — saves a paid round-trip per healthy reconcile (the vast majority in steady state). Failures, in-progress rows, poll timeouts, and any failing pod all keep the LLM in the loop. Set false when SKILL.md adds team-specific text to healthy comments. Ignored when `LLM_PROVIDER=none` |
| `ANTHROPIC_BASE_URL` | unset (Anthropic default) | Optional override of the Anthropic API endpoint (testing / proxy). Helm: `config.llm.anthropicBaseUrl`. Redirects to another origin (or from HTTPS to HTTP) are refused, so the API key stays with the configured endpoint |
| `SKILLS_DIR` | `/skills` | Directory the skill loader reads `SKILL.md` from at startup (the chart mounts the skill ConfigMap there). See [docs/skill.md](skill.md) |
| `GITHUB_REPO` | auto-discovered | GitHub repo in `owner/repo` format. Auto-discovered from the Kustomization's GitRepository when empty |
| `GITHUB_API_URL` | unset (api.github.com) | Optional GitHub Enterprise base URL override. Helm: `config.github.apiUrl`. Use an `https://` URL: an `http://` URL would send the App JWT and installation tokens in clear text, so the bridge refuses it at startup unless `GITHUB_API_ALLOW_INSECURE=true` |
| `GITHUB_API_ALLOW_INSECURE` | `false` | Accept a plain-HTTP `GITHUB_API_URL`. Only for an in-cluster test mock (the e2e harness sets it). Helm: `config.github.allowInsecure` |
| `GITHUB_KEEP_COMMENT_HISTORY` | `false` | When `true`, append every analysis as a fresh comment. The PR ends up with a chronological trail. Useful for iterative testing across commits or for audit trails. Default off keeps **one current comment per (PR, Kustomization)**, delivered idempotently on the **cluster state** (the deterministic kstatus portion — an LLM re-wording an unchanged state is also a no-op): unchanged state leaves the comment strictly untouched (no API write, no notification — an unchanged PR is never re-notified as the source branch advances); a changed state posts the fresh comment first and then deletes the prior one (create-before-delete — the PR never sits without a verdict), so reviewers get a notification + new timestamp for the updated verdict |
| `COMMENT_INCLUDE_SUMMARY` | `true` | Emit an English headline summary line at the top of the comment body (e.g. ``"Kustomization `grafana`: All N resource(s) reached kstatus Current."`` on healthy outcomes, with the kstatus README link embedded). Set `false` to drop the line and have the body go straight from the marker to the Resource Status table. Applies with and without an LLM — the bridge writes this line, the model's narrative goes below the tables |
| `COMMENT_INCLUDE_FOOTER` | `true` | Emit the trailing `*Analyzed by flux-ai-bridge*` attribution byline on every comment. Set `false` to strip it (the hidden per-Kustomization HTML marker `<!-- flux-ai-bridge:<ns>/<name> -->` still scopes idempotency unchanged). Applies to both LLM and no-LLM paths. Controls **only** the attribution byline — the auto-retry policy note on non-green comments (see `DEDUPE_PR_REEVAL_TTL`) is operational info and renders independently of this flag |
| `COMMENT_FAILED_RESOURCES_ONLY` | `false` (bridge default; the **Helm chart sets `true`**) | Compact **both** the `## Resource Status` and `## Workload Status` tables for large inventories / pod-heavy namespaces: a section with failures lists only the failing (non-`Current`) rows (each already carries its namespace in the `Namespace` column, so no separate `Namespace`-kind row is kept); a section where everything is `Current` collapses to a single compact summary row keyed by the Kustomization (`| Kustomization | Resources | Status |`), keeping the table format. The detailed failure table still carries the per-resource Namespace. The overall ✅/⚠️/❌ marker + summary reflect the whole Kustomization inventory + pod set — only the table rows are filtered |
| `COMMENT_ON_SUCCESS` | `true` | Whether to post a comment on a **healthy** outcome (everything `Current`, settled). Set `false` to suppress the "all green" comment — warnings and failures still get one. Independent of `PR_LABEL_ENABLED`: pair `COMMENT_ON_SUCCESS=false` with `PR_LABEL_ENABLED=true` to get a `flux-success` label (for a merge gate) on healthy PRs with **no** comment noise. When a healthy outcome is suppressed but the PR still has a bridge comment from an earlier failing run, that stale comment is removed (cache-guarded — at most one comment lookup per PR and Kustomization when the cache has no record, none after that). Suppressing also skips the LLM call on healthy outcomes |
| `MCP_POOL_SIZE` | matches `WEBHOOK_MAX_CONCURRENT` | Number of MCP sessions in the pool. One session per in-flight pipeline; otherwise concurrent reconciles serialize |
| `DEDUPE_PR_REEVAL_TTL` | unset / `0` (no re-eval; the Helm chart sets `15m`) | Per-PR-head re-evaluation floor. The bridge analyses each PR **once per its own head commit**, so unrelated advances of the Flux source branch don't re-trigger analysis or churn the comment. A positive duration (e.g. `30m`) lets that admission record expire so a later event re-checks current cluster state — used to keep **failed/warning** heads tracked until they resolve (a previously-green head is frozen until a new commit unless `DEDUPE_REEVAL_FAILED_ONLY=false`; with Redis/Valkey the freeze lapses once neither the admission record nor the head's success record is left, see `DEDUPE_REEVAL_FAILED_ONLY`). The comment layer is idempotent, so a re-eval that finds identical state writes nothing. `0`/unset analyses each PR-head exactly once. Enabling it on a running install also migrates admission records written **before** the change (their next event lowers their expiry to the floor via `EXPIRE LT`, also for records that carry the longer `DEDUPE_PERSISTENCE_MAX_TTL` backstop — **requires Redis ≥ 7.0; any Valkey version qualifies** since Valkey forked from Redis 7.2; on older Redis only PR-heads analysed after the change re-evaluate). When set, non-green PR comments carry a one-line note of this policy — e.g. `🔁 Re-evaluated automatically up to 4×, at most once every 15m, until it passes.` (numbers from `DEDUPE_PR_REEVAL_MAX` / this TTL) — shown regardless of `COMMENT_INCLUDE_FOOTER`. Go duration syntax. Negative values are rejected at startup. A positive value shorter than one analysis (`READINESS_POLL_TIMEOUT` + `2m`, the per-event processing budget; `5m` with the defaults) is raised to that, with a warning in the log: with a shorter window a slow failed analysis could release a newer analysis's mark |
| `DEDUPE_REEVAL_FAILED_ONLY` | `true` | Only consulted when `DEDUPE_PR_REEVAL_TTL` is set. **`true`** (default) → a previously-green head is **frozen**: never re-analysed until the PR gets a new commit. Nothing re-wakes it — not a Flux error event, not a pod that crashes afterwards — because in a monorepo where many open PRs share a Kustomization, re-waking green heads on any failure in that Kustomization flips unrelated green PRs whenever a sibling change breaks. Failed/warning heads are still re-checked each window. (Collateral damage from the PR's own change is still caught: the first analysis is whole-inventory, so a PR only reaches green if every dependent was healthy then.) With Redis/Valkey the freeze holds while events keep arriving: only events past the re-eval floor refresh the success record behind it, and it outlives each admission mark by 7 days (or one more `DEDUPE_PR_REEVAL_TTL`, if longer). Once both have expired, the next event analyses the unchanged head as new, which can give it a different verdict. **`false`** → re-evaluate every attributed PR-head each window regardless of prior outcome — this does re-check green heads (catching post-green drift), at the price of re-polling everything and flipping green PRs when unrelated siblings break. |
| `DEDUPE_PR_REEVAL_MAX` | unset / `0` (unlimited; the Helm chart sets `4`) | Only consulted when `DEDUPE_PR_REEVAL_TTL` is set. Caps how many **re-evaluations** a single PR-head gets after its initial analysis — bounding the cost of a deploy that never recovers (otherwise it re-runs the readiness poll + LLM + comment refresh every window for as long as the PR lives). Once the budget is spent the head stops triggering analyses (`flux_ai_bridge_prs_skipped_total{reason="reeval_exhausted"}`) but still **piggybacks** on analyses triggered by sibling PRs of the same Kustomization — its comment keeps tracking the current verdict at zero extra analysis cost; with no siblings it freezes at the last analysis; a **new commit always starts a fresh budget**. **Only re-evals whose analysis concludes failed/warning count** — healthy skips under `DEDUPE_REEVAL_FAILED_ONLY` and re-evals that conclude healthy are both free (a recovery is the news re-eval exists for). The budget therefore caps CONSECUTIVE failing analyses, not the head's lifetime, and with Redis/Valkey persistence it self-cleans after two windows without increments (a quiet head, or a healthy stretch under `failedOnly`) — so a head that later drifts unhealthy gets a fresh analysis and a new budget rather than staying blind forever; the in-memory backend bounds these records by LRU capacity instead, so a spent budget can persist until eviction or restart |
| `DEDUPE_TTL` | unset (no expiry) | Expiry knob for the **in-memory** dedupe backend only — it is **ignored** when Redis/Valkey persistence is configured (`DEDUPE_PERSISTENCE_URL` / `_SENTINEL_ADDRS`). **Unset** → entries never expire (bounded only by LRU capacity, rollback-on-failure, and `DEDUPE_PR_REEVAL_TTL`). **Positive duration** → a coarse cap on every entry's lifetime. **Any zero value** (`0`, `0s`, …) → in-memory dedupe disabled, every event runs the pipeline (the idempotent comment layer still prevents duplicate comments); negative durations are treated like zero (dedupe disabled). Go duration syntax. For per-PR re-evaluation prefer `DEDUPE_PR_REEVAL_TTL` |
| `DEDUPE_PERSISTENCE_URL` | unset (in-memory) | Optional Redis-protocol URL (`redis://host:port/db` or `rediss://` for TLS) for persisting the dedupe cache across bridge restarts. **Single-node mode** — no failover handling; for HA use sentinel mode below. Works with the bundled Valkey sub-chart, external Valkey/Redis, AWS ElastiCache, GCP Memorystore. The dedupe admission **fails closed** on Redis errors: the event is skipped (logged + counted) rather than risking uncoordinated duplicate analysis across replicas. Ignored when `DEDUPE_PERSISTENCE_SENTINEL_ADDRS` is set. A URL the client can't parse stops the bridge at startup instead of falling back to in-memory dedupe (which would break cross-replica coordination); an unreachable server does not: events are skipped until it answers |
| `DEDUPE_PERSISTENCE_USER` | unset | Redis ACL username (Redis 6+) for the **Redis master+replicas** (used in both single-node and sentinel modes). Leave unset for the legacy `default` user |
| `DEDUPE_PERSISTENCE_PASSWORD` | unset | Password for the Redis master+replicas. TrimSpaced at startup — Secret values with trailing newlines won't cause `WRONGPASS` |
| `DEDUPE_PERSISTENCE_SENTINEL_ADDRS` | unset | Comma-separated Sentinel quorum addresses (`h1:26379,h2:26379,h3:26379`). When set, the bridge uses go-redis's FailoverClient and follows master failover automatically. Requires `DEDUPE_PERSISTENCE_MASTER_NAME`. An invalid Sentinel configuration (e.g. a missing master name or port) stops the bridge at startup |
| `DEDUPE_PERSISTENCE_MASTER_NAME` | unset | Sentinel master name (the label in `sentinel monitor <name> …`). Required when sentinel mode is enabled |
| `DEDUPE_PERSISTENCE_DB` | `0` | Logical Redis database to SELECT — **applied in Sentinel mode only**. In single-node mode the database comes from the `/db` path segment of `DEDUPE_PERSISTENCE_URL` instead |
| `DEDUPE_PERSISTENCE_TLS` | `false` | Enable TLS on both sentinel and master connections in sentinel mode. (For single-node mode use `rediss://` in `_URL` instead) |
| `DEDUPE_PERSISTENCE_MAX_TTL` | `720h` (30 days) | Redis/Valkey only: backstop expiry for records that would otherwise never expire — admission marks when `DEDUPE_PR_REEVAL_TTL` is unset and the fetch cache's immutable entries (label records have their own 7-day expiry, refreshed on every event; success outcome records expire 7 days after the head's re-eval window opens, see `DEDUPE_REEVAL_FAILED_ONLY`). Without it they accumulate for every PR-head ever seen until Redis runs out of memory (with `noeviction` or `volatile-*`, keys without an expiry can't be evicted, writes fail, and the bridge fails closed). A PR-head Flux still re-fires after this long is analysed once more. `0` disables the backstop; negative values are rejected |
| `DEDUPE_PERSISTENCE_SENTINEL_USER` | unset | Sentinel-daemon ACL username (often the same value as `_USER` but configured independently because the sentinel ACL file is typically distinct) |
| `DEDUPE_PERSISTENCE_SENTINEL_PASSWORD` | unset | Sentinel-daemon ACL password. TrimSpaced at startup |
| `KUSTOMIZATIONS_INCLUDE` | unset (admit all) | Comma-separated glob patterns (`path.Match` syntax, e.g. `apps-*`) of Kustomization names to process; events for other names are dropped at the webhook. See [Kustomization Filter](#kustomization-filter-configkustomizations) |
| `KUSTOMIZATIONS_EXCLUDE` | unset | Comma-separated glob patterns of Kustomization names to drop, applied after `KUSTOMIZATIONS_INCLUDE` (exclude wins) |
| `PR_REQUIRED_LABELS` | unset (no filter) | Comma-separated list of GitHub PR labels. When set, only PRs carrying at least one of these labels receive a comment. Case-sensitive. Applied AFTER the SHA→PR lookup but BEFORE the per-PR file/blob fetch and LLM call, so unmatched PRs cost zero extra GitHub API spend. Always combined with the inventory-membership filter (a PR must also declare at least one resource in the Kustomization's `status.inventory`) |
| `PR_ATTRIBUTE_PRUNES` | `true` | Attribute a **pure-deletion** PR by what the reconcile **pruned**, not only by what's currently in `status.inventory`. A `removed` file whose declared resource the Kustomization just deleted — read from the kustomize-controller prune event (`Kind/namespace/name deleted`, fetched via MCP) — attributes the PR even though the resource is gone from the inventory, so a clean teardown reads `flux-success` instead of going silently unlabelled. Also covers removing a **whole Kustomization**: the parent that prunes the child CR claims it (the deleted child can't self-report). After attribution the bridge re-checks each pruned resource and keeps the verdict **non-green** if any is still present (`Terminating` finalizer / not-yet-deleted) or unverifiable (`Unknown` MCP error); a `NotFound` re-read is trusted as gone since the prune event already asserted the deletion, unless the read used an `apiVersion` the latest MCP discovery doesn't confirm (a guess for a kind discovery doesn't know, or a cached version a failed or partial refresh didn't list; then the row shows ⚠️ "deletion unverified"). Prune Events whose `kustomize.toolkit.fluxcd.io/revision` annotation names another commit than the analysed one are ignored. The Events read is lazy — only when a candidate PR has a `removed` file whose resource is absent from the inventory — and the failure floor is safe: a missed prune event (e.g. >1h Event TTL) just leaves the PR unlabelled, never wrong. Set `false` to disable (inventory-membership attribution only) |
| `PR_LABEL_ENABLED` | `false` | When true, after posting a comment the bridge reconciles three labels on the PR based on the outcome: ✅ → add success, remove failed + warning; ❌ → add failed, remove success + warning; ⚠️ → add **warning**, remove success + failed. (When a PR no longer touches any tracked Kustomization, **all three** are removed.) Requires the same `Issues: Read & Write` GitHub App permission the bridge already needs for comments. **Multi-Kustomization PRs:** the label is the **aggregate** across every Kustomization attributed to the PR — any one failing → failed; all healthy → success; otherwise → warning — so a healthy event for one Kustomization can't flip the PR green while another is still failing. **Anti-flap:** a `flux-success` aggregate is **held** until every Kustomization the PR is known to touch (its roster, learned across prior commits) has reported for the current head SHA; meanwhile the head is labelled `flux-warning`, so an earlier head's `flux-success` isn't left on an unverified head. If the roster can't be read, `flux-success` is held the same way, and a later event retries the label. `flux-failed`/`flux-warning` are still applied immediately. Without this, each new commit would briefly show `flux-success` (only the fast scopes reported) before flipping to `flux-failed` when a slow scope (e.g. an app-of-apps parent) reports — visible flapping on every push. Aggregation + the roster need the persistent dedupe cache to be correct across replicas; without it, it degrades to per-Kustomization last-write-wins. **Across replicas:** with persistent dedupe configured, label updates for a PR are serialized across replicas by a short lock in the shared Redis/Valkey, so two replicas can't interleave their add/remove calls; a replica that finds the lock held, or can't reach Redis/Valkey to ask for it, skips that update and a later event retries it. One narrow exception remains: the lock is released with two plain commands (no Lua), so a holder that runs to within a round-trip of the 30 s lock TTL can free a lock another replica just took, and a third replica may then reconcile alongside that one (see [caching.md → PR label lock](caching.md#pr-label-lock-cross-replica)). The lock covers the write, not the read of the aggregate, so after each write the replica reads the aggregate again and rewrites the labels if another replica recorded an outcome meanwhile. **Manual removals:** a reviewer deleting a bridge label is respected for the current head (the bridge won't re-add it on re-evals), but every **new commit re-asserts** the labels — the transition cache is head-scoped. Re-adding an already-present label is a silent GitHub no-op, so the per-push write produces no timeline noise. A label write that fails after the comment landed is retried on the next event (the current comment isn't reposted). |
| `PR_LABEL_SUCCESS` | `flux-success` | Name of the label applied on the all-healthy aggregate. Configurable to match an existing merge-gate convention; whatever you pick becomes the contract a downstream Action keys on, so treat it as stable. Only used when `PR_LABEL_ENABLED=true`. Renaming a label (this or the other two) removes the old name from a PR at its next label update, when the PR's outcome record still names it: the record survives restarts only with persistent dedupe (Redis/Valkey), and only the old label the bridge applied is removed. To clear it from every PR at once, including PRs that get no further event, delete the old label in the repository |
| `PR_LABEL_FAILED` | `flux-failed` | Name of the label applied when any Kustomization on the PR fails. Same contract caveat as `PR_LABEL_SUCCESS`. Only used when `PR_LABEL_ENABLED=true` |
| `PR_LABEL_WARNING` | `flux-warning` | Name of the label applied on a ⚠️ warning aggregate (not green, nothing definitively `Failed` — e.g. a readiness-poll timeout, or a Flux child Kustomization stuck on `BuildFailed` that kstatus reports as `InProgress`). **Set this to the same value as `PR_LABEL_FAILED`** to make a warning block a merge gate exactly like a failure — the bridge never removes a label it's also adding, so the shared name doesn't churn. The comment marker is unaffected (a warning still renders ⚠️). Only used when `PR_LABEL_ENABLED=true` |
| `READINESS_POLL_INTERVAL` | `5s` | How often the bridge re-checks the Kustomization's inventory after an event arrives. Lower = quicker reaction to settle, higher MCP load. Must be > 0 |
| `READINESS_POLL_TIMEOUT` | `3m` | How long to wait for every inventory row **and pod** to reach a terminal kstatus before analyzing partial state. One shared budget covers both phases — the inventory poll first, then the pod settle wait inherits the remainder — so a workload mid-rollout (e.g. a freshly-started pod after a HelmRelease image bump) is waited out rather than analysed transiently. Settled pods also wait out `READINESS_POD_QUIET_PERIOD` within this budget. The per-event pipeline budget is `READINESS_POLL_TIMEOUT + 2m` (LLM/GitHub slack). Must be > 0 |
| `READINESS_POD_QUIET_PERIOD` | `60s` (half of `READINESS_POLL_TIMEOUT` when that is under 2m) | How long the scanned namespace must stay quiet — no workload pod appearing, shutting down or disappearing — before settled pods count as settled. Counted from the start of the readiness poll or the last such change, whichever is later; a pod already there at the first scan counts from its `creationTimestamp` (if that is earlier than the scan), so a pod created during the inventory poll counts too. Operators (database operators and the like) create their workload pods only **after** Flux applied the custom resource, and kstatus reads such a resource `Current` as soon as it exists (no conditions → assume Current). Without this window the bridge can find the namespace settled before those pods exist and report healthy. Time spent in the inventory poll counts toward the window, so a slow deploy doesn't pay it twice. A new pod restarts it, even one that is already ready, one created while the inventory poll ran, or one recreated under the same name (a StatefulSet replacing `db-0`); so does a workload pod shutting down, since the replacement only comes once it is gone. A pod first seen in a later scan counts from when the bridge saw it, so clock skew between the API server and the bridge can't hide it; Job pods don't (a frequent CronJob would keep it open, and runs are judged by the newest one anyway). A `Failed` workload pod skips the window, so failures aren't slowed; a failed Job pod doesn't, since it may belong to a superseded CronJob run that is dropped later. Comes out of the shared `READINESS_POLL_TIMEOUT` budget and must be shorter than it; if the budget runs out inside the window the pods count as unsettled, which reads ⚠️ unless a pod failed (fail closed: pods were still appearing). The comment's headline then names the pod change that last restarted the window, since the tables show every pod `Current`. Adds time only when every pod is already settled and no workload pod failed. `0` disables. When unset, the default shrinks to half a poll timeout under 2m, so an existing short `READINESS_POLL_TIMEOUT` keeps working; an explicit value that isn't shorter than the poll timeout is rejected at startup. Ignored when `READINESS_INCLUDE_PODS=false` |
| `READINESS_INCLUDE_PODS` | `true` | When true, take a kstatus snapshot of pods in the **Kustomization's own namespace** (its `spec.targetNamespace` if set, else its name — e.g. Kustomization `grafana` → the `grafana` namespace). NOT every inventory namespace: a Kustomization can own resources (dashboards, alert-rules) across 100+ namespaces while only running its workload in one, so the scan is confined to that one namespace to avoid dumping the whole cluster's unrelated pods. Each pod row is labeled with its workload controller (resolved via `ownerReferences`, chasing one extra hop for Pod → ReplicaSet → Deployment). In no-LLM mode the snapshot renders as a `## Workload Status` table; in LLM mode it's appended to the prompt as `## Workload Readiness`. Surfaces CrashLoopBackOff / ImagePullBackOff / OOMKilled / Pending>2m pods even when the controller (HelmRelease, Deployment) hasn't propagated the failure to its own status yet. The scan is re-run until every pod reaches a terminal kstatus and the namespace has stayed quiet for `READINESS_POD_QUIET_PERIOD`, or the shared `READINESS_POLL_TIMEOUT` elapses, so a just-rolled-out pod is waited out into `Current` (a `Failed` workload pod is terminal and ends the wait at once; a failed Job pod waits out the quiet period). Pods are listed by namespace, NOT filtered by ownership; disable in shared-namespace environments where sibling-Kustomization noise outweighs the diagnostic value. Pod rows are not filtered by ownership; which pods' logs reach the LLM is set by `FAILURE_FETCH_LOGS_SCOPE` |
| `READINESS_EXPAND_HELMRELEASES` | `true` | When true, for every HelmRelease in the Kustomization's inventory the bridge fetches the HelmRelease's own `status.inventory.entries` via MCP and folds those child resources (Deployments, Services, ServiceAccounts, etc.) into the readiness check. Without this, a HelmRelease row shows only opaque controller summaries like "generation N vs observed N-1" — the chart-rendered resources stay invisible. One MCP fetch per HelmRelease; one level only (nested HelmReleases not recursively expanded). Per-HelmRelease fetch failures fall back to "just include the HelmRelease itself" with a WARN log |
| `FAILURE_FETCH_LOGS` | `true` | When true, pull container logs for failing pods on error events (tries `previous=true` first, falls back to current) |
| `FAILURE_FETCH_LOGS_SCOPE` | `owned` | Which failing pods' logs `FAILURE_FETCH_LOGS` sends to the LLM. `owned`: only pods owned by a workload in the Kustomization's inventory (HelmRelease children included), resolved through `ownerReferences` like the pod snapshot (Pod → ReplicaSet → Deployment; StatefulSet, DaemonSet, Job; a CronJob's Job one hop further). A pod whose owner can't be resolved is left out, and the prompt says how many failing pods' logs were not included. `namespace`: any failing pod in the swept namespaces, the behaviour before this setting existed. **The default changed to `owned`**: in a shared namespace the old behaviour put logs of unrelated apps into the prompt, and so to an external LLM. Pod status rows and events are not affected. Any other value is rejected at startup |
| `FAILURE_FETCH_EVENTS` | `true` | When true, pull namespace Events filtered to post-pod-creation timestamps |
| `FAILURE_FETCH_METRICS` | `false` | Opt-in. Pull `top pods` metrics — dense numeric output the LLM often under-utilizes |
| `FAILURE_FETCH_FLUX_INSTANCE` | `false` | Opt-in. Pull cluster-wide Flux Operator status — useful only for cluster-wide controller problems. Needs Flux managed by the Flux Operator (a `FluxInstance`); without one the fetch fails with a WARN log and the section is left out |
| `PARENT_WALK_DEPTH` | `200` | Cap on the merge-parent walk in `FindPRsForCommit`. The walk normally stops earlier, at the merge base with the default branch; the cap bounds it when that can't be determined. Sized for [autodev](https://github.com/Staffbase/autodev-action) batches; raise if your workflow merges more than 200 PRs in one push. See [docs/caching.md](caching.md) |
| `PRS_FOR_COMMIT_TTL` | `5m` | TTL for the PR-lookup caches (`prs-for-commit` + `prs-walk`). The PR↔commit association is immutable; this TTL only bounds staleness of each PR's mutable GitHub **state** (open/closed/merged) and **labels** — fields that change outside the Flux event stream, so the bridge only learns them by re-querying. **Without `PR_REQUIRED_LABELS` (the default), only state matters**, and per-PR-head dedup + idempotent comments already cushion stale state, so the multi-minute default is safe and cuts GitHub calls. **If you set `PR_REQUIRED_LABELS`, lower this (e.g. `60s`)** so a freshly-added label is picked up promptly. Empty/`0` → the 5m default; a **negative** value (e.g. `-1s`) disables the cache entirely (every event re-queries) |
| `COMMIT_TTL` | `1h` | Growth bound on the (immutable) commit-parents cache so the merge-parent walk's ancestors don't accumulate forever in Redis. Not a freshness knob — commit metadata never changes. A negative duration (e.g. `-1s`) restores no-expiry. See [docs/caching.md](caching.md) |
| `FETCH_CACHE_CAPACITY` | `4096` | In-memory LRU bound (entries) for the GitHub-fetch cache. Shared across all fetch-cache kinds (prs-walk/prs-for-commit/commit/pr-files/blobrefs/basefileids/last-comment/pr-outcome). Ignored when the fetchcache is Redis-backed (auto-detected from persistent dedupe; give that Redis a `maxmemory`+eviction policy instead) |

### Default LLM Models

| Provider | Default Model |
|---|---|
| `claude` | `claude-sonnet-4-6` |
| `openai-compatible` | `gpt-4.1` |
| `none` | n/a — LLM disabled |

### Defaults for openai-compatible provider

| Variable | Default |
|---|---|
| `OPENAI_BASE_URL` | `https://models.inference.ai.azure.com` |

### No-LLM mode

`LLM_PROVIDER=none` (Helm: `config.llm.provider=none`) turns the LLM call off completely. The bridge still runs the full reconciliation-readiness poll, but instead of asking a model to summarise the result, it posts a deterministic comment built from the [kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus) output alone — one row per inventory resource, with the same `✅/⚠️/❌` overall marker convention as the LLM-driven comment so PR reviewers see the outcome at a glance.

When `none` is set:

- No API key, model, or base URL is required (`ANTHROPIC_*` / `OPENAI_*` are ignored).
- The skill prompt (`SKILL.md`) is loaded but not used.
- The failure-context fetch (`FAILURE_FETCH_LOGS` / `_EVENTS` / `_METRICS` / `_FLUX_INSTANCE`) is skipped — there is nothing to feed it into.
- The body still surfaces controller-specific diagnostics that don't require an LLM:
  - **`## Workload Status`** — kstatus snapshot of pods in the Kustomization's namespace, each row labeled with its workload controller (Deployment / StatefulSet / DaemonSet / Job, resolved through the Pod → ReplicaSet → Deployment chain for Deployment-owned pods). Gated by `READINESS_INCLUDE_PODS` (default true). Surfaces failing pods (CrashLoopBackOff, ImagePullBackOff, OOMKilled, Pending>2m) that the inventory row often masks during the brief window between failure and controller propagation.

For a non-Current inventory entry the Resource Status row's Message column shows its most diagnostic not-True condition as `Reason: Message` — the Flux `Ready` condition when present (e.g. a HelmRelease's `InstallFailed: helm upgrade failed: …`). After helm-controller remediated a failed release, `Ready` only reports the remediation (`RollbackSucceeded: Helm rollback … succeeded`), so the failure leads instead and the rollback follows as context: `RetriesExceeded: Failed to upgrade after 4 attempt(s); RollbackSucceeded: …`. The full `.status.conditions[]` list is not rendered; operators can `kubectl describe` for it. The complete condition data is collected during the readiness poll and feeds the LLM prompt.

Typical use cases: air-gapped clusters with no LLM endpoint reachable, cost-sensitive setups, smoke-testing the install path without provisioning a model, or anyone who simply prefers the structured readiness table without an LLM-written summary on top.

---

## Helm Chart Values

### Core

| Value | Default | Description |
|---|---|---|
| `terminationGracePeriodSeconds` | `45` | Pod termination grace period; must exceed `config.shutdownTimeout` (bridge default 30s) |
| `replicaCount` | `1` | Number of bridge replicas. Setting `> 1` (or enabling `autoscaling`) requires `config.dedupe.persistence.enabled=true`; the chart fails fast at template time otherwise. The persistence URL itself can come from `valkey.enabled=true` (auto-derived) or an explicit `config.dedupe.persistence.url`. See architecture.md "High availability (multi-replica)" |
| `image.repository` | `ghcr.io/staffbase/flux-ai-bridge` | Container image repository |
| `image.tag` | `""` (chart `appVersion`) | Image tag override |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy |

### Service

| Value | Default | Description |
|---|---|---|
| `service.type` | `ClusterIP` | Kubernetes Service type |
| `service.port` | `8080` | Service port (also sets `WEBHOOK_PORT` in the container) |

### Bridge Configuration (`config.*`)

| Value | Default | Description |
|---|---|---|
| `config.logLevel` | `""` (bridge default `info`) | Log level: `debug`, `info`, `warn`, `error` |
| `config.instanceId` | `""` | Instance ID (`INSTANCE_ID`) for multi-cluster setups; see the env var above |
| `config.shutdownTimeout` | `""` (bridge default `30s`) | Shutdown budget (`SHUTDOWN_TIMEOUT`); keep below `terminationGracePeriodSeconds` |
| `config.webhook.maxConcurrent` | `""` (bridge default `16`) | Max concurrent webhook events; up to `config.webhook.queueSize` more wait for a slot, beyond that 503 |
| `config.webhook.queueSize` | `""` (bridge default `512`) | Events that may wait for a processing slot (`WEBHOOK_QUEUE_SIZE`); keep it above the number of Kustomizations the bridge watches |
| `config.mcp.url` | `""` (bundled sub-chart's Service in the release namespace) | MCP endpoint URL. Empty derives `http://flux-operator-mcp.<release-namespace>.svc.cluster.local:9090/mcp`; required when `flux-operator-mcp.enabled=false` |
| `config.mcp.poolSize` | `""` (matches `webhook.maxConcurrent`) | MCP session pool size |
| `config.llm.provider` | `none` | LLM provider: `none`, `claude`, or `openai-compatible`. **Chart default is `none`**: a fresh `helm install` with only the GitHub App Secret produces a working bridge that posts a deterministic [kstatus](https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus) summary table on every Flux event — see [No-LLM mode](#no-llm-mode). Switch to `claude` (+ `credentials.anthropicApiKey`) for an LLM-written summary on top. Switch to `openai-compatible` (+ `llm.openaiBaseUrl` + `credentials.openaiApiKey`) to use Azure OpenAI, OpenAI proper, OpenRouter, vLLM, or a bundled local Ollama (`ollama.enabled: true`). The API key can also come from `existingSecret` or `extraEnv`, see [Credentials](#credentials) |
| `config.llm.model` | `""` | Model override. Empty = provider default |
| `config.llm.maxTokens` | `""` (bridge default `2048`) | Cap on LLM output tokens |
| `config.llm.skipOnHealthy` | `true` | Short-circuit the LLM call on fully-green reconciles — see `LLM_SKIP_ON_HEALTHY` above |
| `config.llm.anthropicBaseUrl` | `""` | Override Anthropic API base URL (testing / proxy) |
| `config.llm.openaiBaseUrl` | `""` | Override OpenAI-compatible API base URL when `llm.provider=openai-compatible` (e.g. `http://ollama.../v1`) |
| `config.github.repo` | `""` | GitHub repo (`owner/repo`). Empty = auto-discover |
| `config.github.apiUrl` | `""` | Override GitHub API base URL (Enterprise / testing) |
| `config.github.allowInsecure` | `false` | Accept a plain-HTTP `apiUrl` (`GITHUB_API_ALLOW_INSECURE`); test mocks only |
| `config.github.parentWalkDepth` | `""` (bridge default `200`) | Cap on the merge-parent walk (`PARENT_WALK_DEPTH`) — see [caching.md](caching.md) |
| `config.github.cache.capacity` | `""` (bridge default `4096`) | In-memory fetch-cache LRU bound (`FETCH_CACHE_CAPACITY`) — see [caching.md](caching.md) |
| `config.github.cache.commitTTL` | `""` (bridge default `1h`) | Growth bound on the commit-metadata cache (`COMMIT_TTL`) — see [caching.md](caching.md) |
| `config.github.cache.prsForCommitTTL` | `""` (bridge default `5m`) | TTL for the PR-lookup caches (`PRS_FOR_COMMIT_TTL`) — see [caching.md](caching.md) |
| `config.github.comments.onSuccess` | `true` | Whether to comment on a healthy outcome. Set `false` to suppress "all green" comments (warnings/failures still comment). Combine with `labels.enabled: true` for a label-only signal on healthy PRs — see `COMMENT_ON_SUCCESS` above |
| `config.github.comments.keepHistory` | `false` | Skip delete-and-recreate; append each analysis as a new comment. The PR keeps a chronological trail — handy for testing iterations across commits or as an audit trail |
| `config.github.comments.includeSummary` | `true` | Emit the headline summary line above the tables — see `COMMENT_INCLUDE_SUMMARY` above |
| `config.github.comments.includeFooter` | `true` | Emit the `*Analyzed by flux-ai-bridge*` footer — see `COMMENT_INCLUDE_FOOTER` above |
| `config.github.comments.failedResourcesOnly` | `true` | Compact the Resource/Workload Status tables (failed rows only; an all-`Current` section collapses to a one-line summary). **Chart default is `true`** — note this differs from the bridge binary default (`false`); see `COMMENT_FAILED_RESOURCES_ONLY` above |
| `config.dedupe.ttl` | `""` (no expiry) | Dedupe TTL (`DEDUPE_TTL`) — in-memory backend only, ignored when persistence is enabled. Empty/unset = no expiry; any zero value (`0`, `0s`) = disabled; Go duration like `1h`; negative values treated like zero (disabled). The chart refuses a disabling value when `config.github.labels.enabled` is on without persistence: label aggregation needs the dedupe record |
| `config.dedupe.reEval.ttl` | `"15m"` | Per-PR-head **re-evaluation** floor (`DEDUPE_PR_REEVAL_TTL`). The default re-checks **failed/warning** heads every 15 minutes until they resolve, so a warning or failure that resolves on its own (a chart upgrade or rollout that outlasts the readiness poll) gets its final verdict without a new push. Green heads stay frozen until a new commit unless `reEval.failedOnly=false`. `"0"` → a PR is analysed once per its head commit. A positive value shorter than `config.readiness.pollTimeout` + `2m` is raised to that (logged as a warning), so with `pollTimeout` above `13m` the default becomes `pollTimeout` + `2m`. Migrating PR-heads analysed before the ttl was enabled needs Redis ≥ 7.0 on the persistence backend (any Valkey version qualifies, incl. the bundled sub-chart) — see `DEDUPE_PR_REEVAL_TTL` above |
| `config.dedupe.reEval.failedOnly` | `true` | Maps to `DEDUPE_REEVAL_FAILED_ONLY`. Only relevant when `reEval.ttl` is set. `true` (default) re-evaluates only previously-unhealthy PR-heads (healthy deploys skip the re-poll, saving MCP traffic); `false` re-evaluates every attributed PR-head — see `DEDUPE_REEVAL_FAILED_ONLY` above |
| `config.dedupe.reEval.maxRetries` | `4` | Maps to `DEDUPE_PR_REEVAL_MAX`. Only relevant when `reEval.ttl` is set. Caps re-evaluations per PR-head so a never-recovering deploy stops burning readiness polls + LLM calls + comment churn: with the default ttl, about 1h of re-checks per push. A new commit resets the budget; `0` = unlimited — see `DEDUPE_PR_REEVAL_MAX` above |
| `config.dedupe.persistence.enabled` | `true` | Redis-protocol persistent dedupe, so the cache and the PR label roster survive bridge restarts (the flux-* label aggregation needs the roster to be correct). On by default with the bundled Valkey; set it and `valkey.enabled` to `false` for in-memory dedupe |
| `config.dedupe.persistence.maxTTL` | `""` (bridge default `720h`) | Backstop expiry for Redis records without one (`DEDUPE_PERSISTENCE_MAX_TTL`); `"0"` disables it |
| `config.dedupe.persistence.url` | `""` | Redis URL (`redis://...` or `rediss://...`). Empty + `valkey.enabled=true` auto-wires the in-cluster Valkey Service |
| `config.dedupe.persistence.user` | `""` | Plaintext Redis ACL username (Redis 6+). Injected as a plain pod env var — usernames aren't secret, so this lives in chart values regardless of whether the password comes from `existingSecret` or inline. Leave empty for the legacy `default` user |
| `config.dedupe.persistence.password` | `""` | Inline password for the URL above. Stored in a chart-created Secret. Prefer `existingSecret` for production |
| `config.dedupe.persistence.existingSecret` | `""` | Name of a pre-existing Secret holding the dedupe password. The chart looks up the password key via `passwordKey` below — letting you consume a third-party Secret (e.g. one provisioned by an external-secrets operator) without renaming its keys |
| `config.dedupe.persistence.passwordKey` | `"DEDUPE_PERSISTENCE_PASSWORD"` | Key inside `existingSecret` holding the Redis-backend password. Set to `""` to skip password injection (e.g. username-only ACL flows where `user` carries auth) |
| `config.dedupe.persistence.sentinel.enabled` | `false` | Enable Redis Sentinel mode (HA). When true, supersedes the single-node `url` field and the bridge uses go-redis's FailoverClient — connecting to the sentinel quorum and following master failover transparently |
| `config.dedupe.persistence.sentinel.addrs` | `""` | Sentinel quorum addresses, passed straight to go-redis's FailoverClient (`DEDUPE_PERSISTENCE_SENTINEL_ADDRS`). Point at the Redis chart's **headless** Service DNS (returns every sentinel pod — one entry suffices, e.g. `redis-flux-ai-bridge-headless.flux-system.svc.cluster.local:26379`) or a comma-separated list of per-pod FQDNs for maximum robustness. The port lives in each addr. Don't use a plain ClusterIP Service — it returns one VIP and defeats the quorum. Required when `sentinel.enabled=true`. With the bundled Valkey in Sentinel mode (`valkey.replica.enabled` + `valkey.sentinel.enabled`), defaults to its Sentinel headless Service `<valkey>-sentinel-hl.<namespace>.svc.cluster.local:26379` |
| `config.dedupe.persistence.sentinel.masterName` | `""` | Sentinel master label (e.g. Bitnami default `mymaster`). Required when `sentinel.enabled=true`. With the bundled Valkey in Sentinel mode, defaults to its `valkey.sentinel.masterSet` (`mymaster`) |
| `config.dedupe.persistence.sentinel.db` | `0` | Logical Redis database number |
| `config.dedupe.persistence.sentinel.tls` | `false` | Enable TLS on sentinel + master connections |
| `config.dedupe.persistence.sentinel.user` | `""` | Sentinel-daemon ACL username, plaintext (usernames aren't secret) |
| `config.dedupe.persistence.sentinel.password` | `""` | Inline sentinel-daemon ACL password. Stored in the chart-created Secret under `DEDUPE_PERSISTENCE_SENTINEL_PASSWORD`. Prefer `existingSecret` + `sentinel.passwordKey` for production. Also works with the bundled Valkey when it runs Sentinel (`valkey.replica.enabled`, `valkey.sentinel.enabled`, which also needs `valkey.persistence` and `valkey.sentinel.password`); set this password to the same value |
| `config.dedupe.persistence.sentinel.passwordKey` | `""` | Key inside `existingSecret` holding the sentinel password. Empty (default) means don't mount a sentinel password, appropriate when your sentinel quorum is unauthenticated |
| `config.prFilter.requiredLabels` | `[]` | When non-empty, only post analysis to PRs carrying at least one of these labels (OR semantics, case-sensitive). Useful for opting individual PRs into bridge review rather than commenting on every PR a Kustomization happens to be part of — particularly in autodev flows where one merge SHA maps to many PRs. Rendered into `PR_REQUIRED_LABELS` (comma-separated) |
| `config.prFilter.attributePrunes` | `true` | Maps to `PR_ATTRIBUTE_PRUNES`. Attribute pure-deletion PRs by what the reconcile pruned (so a clean teardown reads `flux-success`); only rendered into the Deployment when set to `false`. See `PR_ATTRIBUTE_PRUNES` above for full semantics |
| `config.github.labels.enabled` | `false` | When true, the bridge applies the success/failed labels to every PR, reconciled from the **aggregate** outcome across all Kustomizations on the PR (any failing → failed label), independent of whether a comment was posted. Useful for gating merges with a downstream GitHub Action — see `PR_LABEL_ENABLED` above for full semantics |
| `config.github.labels.names.success` | `flux-success` | Name of the label applied when all Kustomizations on the PR are healthy (`PR_LABEL_SUCCESS`). Whatever you choose is the contract your merge gate keys on |
| `config.github.labels.names.failed` | `flux-failed` | Name of the label applied when any Kustomization on the PR fails (`PR_LABEL_FAILED`) |
| `config.github.labels.names.warning` | `flux-warning` | Name of the label applied on a ⚠️ warning outcome (`PR_LABEL_WARNING`). Set it equal to `names.failed` to make a warning block a merge gate the same way a failure does — colliding names don't churn (the bridge never removes a label it's also adding) |
| `config.readiness.pollInterval` | `""` (bridge default `5s`) | How often the bridge re-checks inventory after an event |
| `config.readiness.pollTimeout` | `""` (bridge default `3m`) | How long to wait for inventory to settle before analyzing |
| `config.readiness.podQuietPeriod` | `""` (bridge default `60s`) | How long the namespace must stay quiet — no workload pod appearing and none shutting down — before settled pods count as settled; `0` disables — see `READINESS_POD_QUIET_PERIOD` above |
| `config.readiness.includePods` | `true` | Snapshot pods in the Kustomization's namespace and render a `## Workload Status` section (each row labeled with its workload controller) — see `READINESS_INCLUDE_PODS` above |
| `config.readiness.expandHelmReleases` | `true` | For each HelmRelease in the Kustomization inventory, fold its own child resources into the readiness check — see `READINESS_EXPAND_HELMRELEASES` above |
| `config.failureAnalysis.fetchLogs` | `true` | Pull pod logs on failure events |
| `config.failureAnalysis.logsScope` | `owned` | Which failing pods' logs are pulled: `owned` (inventory workloads only) or `namespace` — see `FAILURE_FETCH_LOGS_SCOPE` above |
| `config.failureAnalysis.fetchEvents` | `true` | Pull namespace Events on failure events |
| `config.failureAnalysis.fetchMetrics` | `false` | Opt-in: pull `top pods` metrics |
| `config.failureAnalysis.fetchFluxInstance` | `false` | Opt-in: pull cluster-wide Flux Operator status. Needs a `FluxInstance` (Flux managed by the Flux Operator) |
| `valkey.enabled` | `true` | Render the bundled Valkey sub-chart for in-cluster dedupe persistence. Set it to `false` to point at an external Redis-compatible service via `config.dedupe.persistence.url` (or `sentinel`), or together with `config.dedupe.persistence.enabled=false` for in-memory dedupe. The default is a single pod without Sentinel, a single point of failure (dedupe fails closed while it is down); for high availability, use the Sentinel setup (`valkey.replica.enabled`, `valkey.sentinel.enabled`, see `config.dedupe.persistence.sentinel`) |
| `ollama.enabled` | `false` | Render the bundled Ollama sub-chart so `OPENAI_BASE_URL` auto-wires to an in-cluster Ollama (you still set `config.llm.provider=openai-compatible` and `config.llm.model` yourself). Off by default so chart-testing on GitHub-hosted runners doesn't OOM pulling the model image. Enable for self-contained / air-gapped installs — see [local-llm.md](local-llm.md) |
| `flux-operator-mcp.enabled` | `true` | Render the bundled `flux-operator-mcp` sub-chart (Service name pinned to `flux-operator-mcp` via `fullnameOverride`; the bridge derives its MCP URL from it). Its NetworkPolicy is on, so only the release namespace can connect (`flux-operator-mcp.networkPolicy.ingress.namespaces` adds more), and its cluster-admin binding is off in favour of `mcpRbac`. Set `false` to point `config.mcp.url` at a pre-existing install |
| `mcpRbac.create` | `true` | Bind a read-only ClusterRole (`get`/`list`/`watch` on all resources) to the bundled flux-operator-mcp ServiceAccount, replacing the sub-chart's cluster-admin binding. MCP masks Secret values by default. The wildcard also covers subresources such as `nodes/proxy`, whose `get` the kubelet accepts for exec over WebSocket, so guard the MCP ServiceAccount token like an admin credential (or set `mcpRbac.excludeNodeProxy`); set `false` to bind a narrower role yourself |
| `mcpRbac.excludeNodeProxy` | `false` | Opt-in: bind a role that lists API groups instead of the wildcard, so it can leave out `nodes/proxy` (and `pods/exec`, `pods/attach`, `pods/portforward`, `pods/proxy`, `services/proxy`). It reads the core resources the bridge needs (incl. `pods/log` and events), the built-in groups (`apps`, `batch`, `networking.k8s.io`, `policy`, `autoscaling`, `rbac.authorization.k8s.io`, `apiextensions.k8s.io`, `events.k8s.io`, `metrics.k8s.io`, `storage.k8s.io`, `admissionregistration.k8s.io`, `scheduling.k8s.io`, `discovery.k8s.io`, `apiregistration.k8s.io`) and all Flux groups (`*.toolkit.fluxcd.io`, `source.extensions.fluxcd.io`, `fluxcd.controlplane.io`). **Extend it for your CRDs** with `mcpRbac.extraRules`: a group the role misses shows up as unreadable resources in the comment (`Unknown` rows with a `forbidden` error, so the verdict is ⚠️ warning at best) |
| `mcpRbac.extraRules` | `[]` | Extra RBAC rules added to the MCP role, e.g. `[{apiGroups: ["example.com"], resources: ["*"]}]`. Needed with `excludeNodeProxy` for every CRD group in a Kustomization's inventory. Read-only: `verbs` defaults to `get`/`list`/`watch`, and any other verb fails the render |
| `autoscaling.*` | `enabled: false` | HorizontalPodAutoscaler (`minReplicas`/`maxReplicas`/`targetCPUUtilizationPercentage`, optional `targetMemoryUtilizationPercentage`). Enabling it requires `config.dedupe.persistence.enabled=true`, same as `replicaCount > 1` |
| `networkPolicy.*` | `enabled: true` | NetworkPolicy for the bridge pod (inert where the CNI doesn't enforce policies): ingress from the Flux namespace (`networkPolicy.fluxNamespace`, plus the monitoring namespace when `serviceMonitor.enabled`), egress to DNS (`k8s-app: kube-dns`), the Flux namespace, pods in the release namespace (bundled MCP, Valkey, Ollama), an external dedupe Redis (`networkPolicy.egress.redis.ports`, default `6379`; in Sentinel mode also `redis.sentinelPorts`, default `26379`; plus any port in `config.dedupe.persistence.url` / `sentinel.addrs`), the endpoints set in `config.llm.openaiBaseUrl` / `anthropicBaseUrl` and `config.github.apiUrl` (derived from the URL: an in-cluster Service `<name>.<namespace>.svc…` allows its namespace on any port, since the URL names the Service port but the policy matches the pod's; an IP address allows that address on the URL's port; any other host opens the URL's port to any destination unless it's 443/6443), and ports 443/6443. Anything else — an endpoint set through `extraEnv`, an in-cluster Service named without `.svc`, DNS with other labels — goes in `networkPolicy.egress.extra`; `networkPolicy.ingress.extra` adds ingress rules, `networkPolicy.ingress.from` replaces the Flux-namespace source. See the comments in `values.yaml` |
| `serviceMonitor.*` | `enabled: false` | Prometheus Operator ServiceMonitor for `/metrics` (`interval`, `scrapeTimeout`, optional `namespace`/`labels`) |

### Credentials

The bridge authenticates to GitHub via a GitHub App only — there is no token-based path. See [github-app.md](github-app.md) for App setup.

| Value | Default | Description |
|---|---|---|
| `existingSecret` | `""` | Name of a pre-existing Secret. When set, `credentials.*` values are ignored |
| `credentials.githubApp.appId` | `""` | Numeric App ID (chart creates a Secret) |
| `credentials.githubApp.installationId` | `""` | Installation ID (chart creates a Secret) |
| `credentials.githubApp.privateKey` | `""` | PEM contents of the App's private key. Use `--set-file credentials.githubApp.privateKey=./app-key.pem` to load from a file |
| `credentials.anthropicApiKey` | `""` | Anthropic API key (chart creates a Secret, Claude only) |
| `credentials.openaiApiKey` | `""` | OpenAI / OpenAI-compatible API key (chart creates a Secret, `openai-compatible` provider only) |

**Secret key names** (when using `existingSecret`):

| Key | Required for |
|---|---|
| `GITHUB_APP_ID` | Always |
| `GITHUB_APP_INSTALLATION_ID` | Always |
| `GITHUB_APP_PRIVATE_KEY` | Always |
| `ANTHROPIC_API_KEY` | `config.llm.provider=claude`, unless `extraEnv` sets it |
| `OPENAI_API_KEY` | `config.llm.provider=openai-compatible` (when the endpoint authenticates), unless `extraEnv` sets it |

**Keys from other Secrets:** `extraEnv` (default `[]`) adds Kubernetes `EnvVar` entries to the bridge container after
the chart's own. Kubernetes gives `env` priority over `envFrom`, so an entry overrides the same key from the credentials
Secret. Use it to read the LLM API key from a Secret of its own, e.g. one that is rotated apart from the GitHub App
credentials:

```yaml
existingSecret: flux-ai-bridge
extraEnv:
  - name: OPENAI_API_KEY
    valueFrom:
      secretKeyRef:
        name: flux-ai-bridge-llm
        key: api-key
```

Don't repeat a name the chart already sets. Like `existingSecret`, a Secret referenced here needs a reloader to restart
the pod when it changes.

### RBAC

The bridge's ServiceAccount gets **no RBAC**: the bridge never talks to the Kubernetes API directly — all cluster reads go through `flux-operator-mcp`, so it needs no Roles or ClusterRoles. The bundled MCP server does: by default the chart binds it a read-only ClusterRole (`mcpRbac.create`, see above), on every resource or, with `mcpRbac.excludeNodeProxy`, on a list of API groups without `nodes/proxy`. With `flux-operator-mcp.enabled=false` or `mcpRbac.create=false` the chart creates no RBAC resources.

### Flux Notification Integration (`flux.notification.*`)

| Value | Default | Description |
|---|---|---|
| `flux.notification.enabled` | `true` | Create Flux Provider + Alert |
| `flux.notification.namespace` | `""` (release namespace) | Namespace for Provider and Alert |
| `flux.notification.hmac.generate` | `true` | When neither `token` nor `existingSecret` is set, generate a random token: the chart creates the Secret, renders the Provider as `type: generic-hmac`, and the bridge validates each webhook's `X-Signature` (see `WEBHOOK_HMAC_SECRET` above). The token is reused on upgrades via `lookup`; renderers without cluster access (`helm template`, Argo CD) get a new one on every render, so set `token` or `existingSecret` there. `false` (with no token) disables signing |
| `flux.notification.hmac.token` | `""` | Inline HMAC token; the chart creates the Secret |
| `flux.notification.hmac.existingSecret` | `""` | Name of a pre-existing Secret holding the HMAC token under the key `token` (the key name is not configurable — both the bridge and Flux's `generic-hmac` Provider read it). Must exist in the release namespace and, when it differs, in `flux.notification.namespace` |
| `flux.notification.eventSourceNamespaces` | `[]` | Extra namespaces whose Kustomizations the Alert subscribes to. The Alert always covers its own namespace |

The chart-managed HMAC Secret is created in the release namespace (read by the bridge) and, when `flux.notification.namespace` differs, also there (read by the Provider), with the same token.

The Alert always subscribes to ALL Kustomizations (`name: "*"`) in its own namespace plus any listed in `eventSourceNamespaces`. Per-name allow/deny lives bridge-side in `config.kustomizations.{include,exclude}` (see below) — the Alert CRD only supports allowlisting and can't express exclude semantics, so we centralize the decision in one place.

### Kustomization Filter (`config.kustomizations.*`)

| Value | Default | Description |
|---|---|---|
| `config.kustomizations.include` | `[]` | Glob list of Kustomization names to admit (empty = match all) |
| `config.kustomizations.exclude` | `[]` | Glob list of Kustomization names to drop |

Globs use Go's [`path.Match`](https://pkg.go.dev/path#Match) syntax (`*`, `?`, `[abc]`). Compose rule (mirrors Prometheus relabel keep+drop, kustomize, gitignore):

- Both empty → admit everything (default).
- Non-empty `include` → name MUST match at least one include glob.
- Non-empty `exclude` → name MUST NOT match any exclude glob.
- Both populated → must match `include` AND not match `exclude` (deny wins).

```yaml
config:
  kustomizations:
    include:
      - "apps-*"
    exclude:
      - "apps-canary"
```

Filtering runs in-process AFTER the Flux Alert delivers the event and BEFORE dedupe — so dropped events never pollute the dedupe cache (toggling the filter off later wouldn't silently swallow events for SHAs we "saw" only as drops).

Bad globs (e.g. unclosed `[`) make the bridge exit non-zero at startup, and empty patterns fail the Helm render — typos are caught at deploy time, not silently absorbed at runtime. (Set via the env vars directly, empty entries between commas are ignored.)

Drops emit a DEBUG log line and increment `flux_ai_bridge_events_filtered_total{reason="not_in_include"|"in_exclude"}`. Use that counter (not log grep) to verify the filter does what you expect.

### Skill (LLM system prompt) (`skill.*`)

| Value | Default | Description |
|---|---|---|
| `skill.default.enabled` | `true` | Render the bundled diagnostician SKILL.md (inlined in [charts/flux-ai-bridge/templates/configmap-skill.yaml](../charts/flux-ai-bridge/templates/configmap-skill.yaml)). Disable to replace the prompt entirely via `skill.custom` |
| `skill.custom` | `""` | Multi-line string. Appended to the bundled content when `default.enabled=true`; used standalone when `default.enabled=false`. Common pattern: org-specific conventions, registry paths, policy-engine semantics |

The skill content is mounted into the bridge Pod at the skills dir (`SKILLS_DIR`, default `/skills`); the loader reads exactly one file from it — `SKILL.md` — at startup and inlines its content as a system-prompt prefix on every LLM call. Editing either value and running `helm upgrade` rolls the Pod via a `checksum/skill` annotation so the new prompt takes effect on the next reconcile. See [docs/skill.md](skill.md) for a walkthrough, override patterns (`--set-file`, full replacement), and the prompt-size/context-window trade-off on small local models.

### ServiceAccount

| Value | Default | Description |
|---|---|---|
| `serviceAccount.create` | `true` | Create a ServiceAccount |
| `serviceAccount.name` | `""` (generated) | Override the ServiceAccount name |
| `serviceAccount.annotations` | `{}` | Annotations (e.g. for IRSA/Workload Identity) |
| `serviceAccount.automount` | `false` | Mount a ServiceAccount token into the pod. Off: the bridge never calls the Kubernetes API — cluster reads go through flux-operator-mcp |

### Security

| Value | Default | Description |
|---|---|---|
| `podSecurityContext.runAsNonRoot` | `true` | |
| `podSecurityContext.runAsUser` | `65532` | `nonroot` user from distroless image |
| `securityContext.allowPrivilegeEscalation` | `false` | |
| `securityContext.readOnlyRootFilesystem` | `true` | |
| `securityContext.capabilities.drop` | `["ALL"]` | |

### Resources

The chart sets small requests so the pod isn't BestEffort and an HPA has a baseline to compute utilization against. No limits are set by default:

| Value | Default | Description |
|---|---|---|
| `resources.requests.cpu` | `50m` | |
| `resources.requests.memory` | `64Mi` | |
| `resources.limits.memory` | unset (suggested `256Mi`) | No CPU limit |

### Probes

The startup and liveness probes hit `GET /healthz`, the readiness probe hits `GET /readyz`. The startup probe holds the liveness probe until the bridge serves `/healthz` once: startup connects to the MCP server, the dedupe store and GitHub before the HTTP server listens, and on a fresh install those start at the same time. Defaults:

| Value | Default |
|---|---|
| `livenessProbe.initialDelaySeconds` | `5` |
| `livenessProbe.periodSeconds` | `10` |
| `livenessProbe.timeoutSeconds` | `3` |
| `readinessProbe.initialDelaySeconds` | `3` |
| `readinessProbe.periodSeconds` | `5` |
| `readinessProbe.timeoutSeconds` | `3` |
| `startupProbe.periodSeconds` | `5` |
| `startupProbe.timeoutSeconds` | `3` |
| `startupProbe.failureThreshold` | `24` (two minutes) |

---

## Examples

### Minimal — Claude, manual Secret

```yaml
existingSecret: flux-ai-bridge

config:
  llm:
    provider: claude
```

### Minimal — OpenAI-compatible (e.g. Ollama), manual Secret

```yaml
existingSecret: flux-ai-bridge

config:
  llm:
    provider: openai-compatible
    # Without a base URL the bridge calls models.inference.ai.azure.com.
    # With the bundled Ollama (ollama.enabled=true) leave it empty — the
    # chart points it at the in-cluster Service.
    openaiBaseUrl: "http://ollama.llm.svc.cluster.local:11434/v1"
    model: "qwen2.5-coder:7b"
```

### Full production setup

```yaml
replicaCount: 1

image:
  repository: ghcr.io/staffbase/flux-ai-bridge
  tag: "v0.9.10"

existingSecret: flux-ai-bridge-credentials

config:
  llm:
    provider: claude
    model: "claude-opus-5-5"
  github:
    repo: "myorg/infra"
  # Restrict which Kustomizations the bridge processes (optional).
  # The Alert always subscribes to ALL Kustomizations; this glob filter
  # runs in-process. Omit the block entirely to process every Kustomization.
  kustomizations:
    include:
      - "apps"
      - "platform"

flux:
  notification:
    enabled: true
    namespace: flux-system

# The chart sets no default resources — these are suggested values.
resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    memory: 256Mi
```
