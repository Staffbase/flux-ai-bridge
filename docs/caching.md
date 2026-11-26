# Caching

The bridge caches GitHub API responses to stay well under rate limits and keep latency low at scale. Most caches are content-addressed and never invalidate; a small TTL'd cache handles the surfaces with mutable state.

This page documents what is cached, why, how, and how to tune it.

> **Why this matters.** A single [autodev](https://github.com/Staffbase/autodev-action) push merging 200 PRs into a dev branch fires one Flux event whose pipeline can fan out into 1000+ GitHub API calls without caching — exceeding the [App installation rate limit](https://docs.github.com/en/rest/rate-limit) (5,000+/hr, scaling with installation size) within a few pushes. With the caches described here in steady state, the same push lands in roughly 600 calls, and same-SHA re-fires from Flux's reconcile loop short-circuit at the dedupe cache before reaching GitHub at all.

> ⚠️ **Highly recommended: enable persistent (Redis/Valkey) caching.** Caching is always on, but the default is a per-replica **in-memory** LRU that is wiped on every restart and capped at `FETCH_CACHE_CAPACITY` (4096) entries. For any non-trivial deployment — many Kustomizations, a busy GitOps branch, or `replicaCount > 1` — turn on **persistent** caching (`config.dedupe.persistence.enabled=true`, which the fetchcache automatically shares). It's the single most effective thing you can do to stay under GitHub's rate limit: it survives restarts (no cold re-fetch storm after a deploy), is shared across replicas (no per-pod duplication — and it's *required* for `replicaCount > 1`), and removes the in-memory entry cap so a large repo's working set doesn't thrash. Give the Redis instance a `maxmemory` + `allkeys-lru` policy. See [Backends → Redis / Valkey](#redis--valkey-opt-in-persistent-cross-replica). Without it the bridge still works, but on a busy repo with many Kustomizations you are far more likely to hit the rate limit.

## Cache surfaces

The bridge has **eight distinct caches**, each tuned for a different mutability profile (a ninth, `pr-outcome:`, is a tiny per-head transition label cache covered under SetPRLabels — head-scoped so a new commit always re-asserts the labels).

| Cache | Key | TTL | Backend(s) | Saves |
|---|---|---|---|---|
| **Dedupe** | `(namespace, kustomization, PR number, PR head SHA)` | none (LRU 4096; Redis: 30d backstop via `DEDUPE_PERSISTENCE_MAX_TTL`; per-PR re-eval via `DEDUPE_PR_REEVAL_TTL`, capped by `DEDUPE_PR_REEVAL_MAX`) | in-memory or Redis | Entire pipeline on re-fires for an already-analysed PR head (LLM + kstatus + GitHub) |
| **PR changed-files** | `pr-files:<owner>/<repo>/<pr>/<head-sha>` | none (Redis: 30d backstop) | in-memory LRU or Redis | One paginated `GET /pulls/{n}/files` per PR per event in `filterPRs`. Immutable per head SHA. |
| **PR-file resource IDs** | `blobrefs2:<owner>/<repo>/<blob-sha>` | none (Redis: 30d backstop) | in-memory LRU or Redis | One `GET /git/blobs/{sha}` + parse per file. Caches the *derived* inventory IDs, **not** the raw manifest bytes. |
| **Base-file resource IDs** | `basefileids2:<owner>/<repo>/<base-ref>/<path>` | none (Redis: 30d backstop) | in-memory LRU or Redis | One `GET /contents/{path}?ref=<base>` + parse per modified file, to diff an in-place removal (a resource edited out of a still-present file) against the PR base. Immutable per base commit. |
| **Commit metadata** | `commit:<owner>/<repo>/<sha>` | 1h default (`COMMIT_TTL`) | in-memory LRU or Redis | One `GET /commits/{sha}` per ancestor when walking merge parents. **The autodev win.** |
| **Walk stop (merge base)** | `merge-base:<owner>/<repo>/<sha>` | 1h default (`COMMIT_TTL`) | in-memory LRU or Redis | One `GET /compare/{default}...{sha}` per walked source SHA, to stop the parent walk where the default branch's history starts. Only looked up once a walk reaches a second merge commit. |
| **Default branch** | `default-branch:<owner>/<repo>` | 1h default (`COMMIT_TTL`) | in-memory LRU or Redis | One `GET /repos/{owner}/{repo}` for the merge-base lookup. |
| **PR resolution (walk result)** | `prs-walk:<owner>/<repo>/<sha>` | 5m default (`PRS_FOR_COMMIT_TTL`) | in-memory LRU or Redis | The **whole** direct-lookup + merge-parent-walk result for a source SHA. A hit skips the entire traversal — collapses every Kustomization/re-fire at the same dev revision into one resolution. Its TTL is what remains of `PRS_FOR_COMMIT_TTL` for the oldest per-commit lookup it reused, so PR state is never served older than the TTL. |
| **PRs for commit** | `prs-for-commit2:<owner>/<repo>/<sha>` | 5m default (`PRS_FOR_COMMIT_TTL`) | in-memory LRU or Redis | One `GET /commits/{sha}/pulls` per ancestor during the parent walk (the per-commit layer under `prs-walk`). |
| **Last comment ID** | `last-comment:<owner>/<repo>/<pr>/<ns>/<name>` | none (Redis: 30d backstop) | in-memory LRU or Redis | One paginated `GET /issues/{pr}/comments` per PR per event. **The comment-posting win.** |

The dedupe cache is described in [architecture.md → Dedupe cache](architecture.md#dedupe-cache-per-pr-head-processing). The rest — the **fetch cache** — are described below.

Separately, an in-process **source-artifact cache** holds the parsed file tree (paths + `kustomization.yaml` contents; resource bodies are never retained) of each source-controller artifact, keyed by artifact **digest** (`sha256:…`), as an LRU capped at 4 and never persisted. It is consulted during PR attribution to resolve a Kustomization's exact build file set (see [architecture.md → PR attribution](architecture.md#pr-attribution-vs-whole-inventory-reporting)). Because the digest addresses immutable bytes a hit is always correct, and a reconcile storm at one source revision downloads the tarball **once**. This cache saves **source-controller** fetches, not GitHub calls — it's independent of the rate-limit story above and of the persistent backend.

## Why each cache is safe

The mutability story differs per surface; understanding it explains the TTL choices.

### PR changed-files (immutable per head)

`filterPRs` asks each candidate PR "do any of your changed files declare a resource in this Kustomization's inventory?" — which needs the PR's file list (`GET /pulls/{n}/files`). That list only changes when the PR gets new commits, i.e. a new **head SHA**, so it's cached under `pr-files:<owner>/<repo>/<pr>/<head-sha>` with no TTL. This is load-bearing: `filterPRs` runs on *every* event (before the per-PR dedup), and the merge-parent walk can surface many candidates — without the cache that's a fresh paginated file listing per candidate per event. The head SHA comes from the PR object resolved in `FindPRsForCommit`; if it's ever empty the result is fetched but not cached (never key a stale list under a missing head).

### PR-file resource IDs (immutable, derived)

The bridge only needs one thing from a PR's changed files: the set of Kubernetes resource IDs they *declare*, to test against the Kustomization's inventory. So instead of caching the raw file bytes, it fetches the blob, parses it (`ResourceIDsFromYAML`), and caches **only the derived `<ns>_<name>_<group>_<Kind>` ID list** under `blobrefs2:<owner>/<repo>/<blob-sha>`. Git blobs are content-addressed (SHA-1 of the bytes), so the bytes — and therefore the derived IDs — **cannot change**; the entry is valid forever.

Two reasons it caches the derived IDs rather than the content:
- **Space**: a handful of short ID strings instead of a whole manifest file.
- **Safety**: GitOps manifests can carry Secret/ConfigMap bodies. The bridge never copies that content into the cache (and thus into Redis) — only resource coordinates, which aren't sensitive. The raw bytes are fetched transiently on a cache miss and discarded.

Real-world hit pattern: many PRs in a monorepo touch the same root `kustomization.yaml`, `values.yaml`, or workspace metadata files. Each unique blob SHA is fetched + parsed exactly once across all PRs and all events; subsequent reads come from cache. With Redis-backed caching, this survives bridge restarts and is shared across replicas.

### Commit metadata (immutable, TTL-bounded for growth)

A commit's parents are part of the commit object that the SHA addresses. Changing the parents would change the commit's content → change its SHA → it'd be a different commit. So `parents` for a given commit SHA **cannot change** — cache hits are always correct.

This is what makes deep parent-walks affordable (the walk also stops at the merge base with the default branch, so it doesn't descend into already-merged history; see [Walk stop](#walk-stop-merge-base)). For an autodev workflow where dev advances over hundreds of merge commits, the first event walks all ancestors fresh; every subsequent event walks the same ancestors as cache hits. Steady-state cost goes from O(depth) per event to O(new ancestors since the last event).

The entry carries a **1h default TTL** (`CommitTTL`) purely as a *growth bound*, not for freshness: on a continuously-advancing branch the walk would otherwise cache every ancestor forever and grow the Redis keyspace without limit. 1h is far longer than an autodev batch window (the same ancestors are re-hit within minutes), so hit rate stays high while the working set stays bounded to ~1h of distinct commits. Set `COMMIT_TTL` (`config.github.cache.commitTTL`) to a negative duration to restore no-expiry.

### Walk stop (merge base)

The merge base of a commit and the default branch can only move *closer* to the commit as the default branch advances. A cached, older merge base therefore makes a later walk a few hops longer, never shorter — it can't hide an open PR. The default branch name is cached the same way; a stale name only weakens the stop. Both use the commit TTL as a growth bound.

### PRs for commit (mutable state, TTL'd)

`GET /commits/{sha}/pulls` returns PRs whose head branch contains the commit. The PR↔commit association is **immutable** (a commit is on a branch or it isn't, and force-pushing creates new SHAs rather than mutating old ones), but the PR objects' **state** (open/closed/merged) and **labels** are mutable — and crucially they change **on GitHub, outside the Flux event stream**. Flux events only ever say "Kustomization reconciled at commit X"; nothing fires when a reviewer labels a PR or it gets merged. The bridge can only learn those by re-querying, and this TTL is how stale that view is allowed to be before the next lookup refreshes it. (This is the one cache with a freshness, not just growth, motivation.)

**What the TTL actually keeps fresh — and why the default is 5m.** The bridge reads two mutable fields in `filterPRs`: PR **state** (skip closed/merged PRs) and **labels** (the optional `PR_REQUIRED_LABELS` gate).

- **Default config (no required-label gate, no label writing): only state matters.** And per-PR-head dedup + idempotent comment delivery already cushion stale state — a merged PR the bridge already commented on won't be re-touched even if the cache still says "open." So a multi-minute TTL is safe; the only edge cases it lengthens are "comment once on a PR that merged before the bridge ever saw it" and "notice a same-head reopen later." Hence the **5-minute default** — it cuts GitHub calls substantially on busy repos / many Kustomizations reconciling the same dev revision.
- **⚠️ If you use `PR_REQUIRED_LABELS`, lower this (e.g. `60s`).** Now the TTL gates *whether the bridge comments at all*, and a high TTL delays picking up a freshly-added label by up to the TTL. Label-driven workflows want it short.

**Empty results are cached too** (negative caching). The direct lookup at the top of `FindPRsForCommit` is a dev-branch *merge* commit, which is never a PR head, so it always returns empty. Without caching the empty, every Flux event — and every Kustomization reconciling the same dev revision — re-queries the identical SHA and burns the rate limit. The TTL bounds the only case where empty could legitimately flip (a PR opened against that commit); merge-commit empties never flip.

On a busy repo this is the single biggest rate-limit lever after the dedupe cache: with N Kustomizations reconciling the same dev revision, negative caching collapses N identical lookups into one. If you still approach the limit and you **don't** use the label gate, raise `PRS_FOR_COMMIT_TTL` further (the 5m default is conservative — minutes-to-an-hour is fine when only PR state matters); also lower `PARENT_WALK_DEPTH` if your autodev batches are shallow, and/or scope the bridge with `KUSTOMIZATIONS_INCLUDE` so it isn't walking the same dev history for every Kustomization in the cluster.

Override the TTL via `PRS_FOR_COMMIT_TTL` env var (`config.github.cache.prsForCommitTTL` Helm value). Empty/`0` keeps the 5m default; set a **negative** duration (e.g. `-1s`) to disable PR-lookup caching while leaving blobrefs + commit caches on — useful for diagnostics when stale state is suspected.

### Last comment ID (load-bearing for active-active)

When the bridge posts to a PR, it replaces its prior comment for that scope (when comment history is off — `config.github.comments.keepHistory=false`, the default), **creating the fresh comment first and deleting the prior one after** so a crash mid-swap leaves a brief duplicate, never a missing verdict. The naive flow is `ListComments + grep marker + POST new + DELETE matches`. The list call is paginated and can be slow on long-running PRs.

The cache stores the comment ID the bridge created on its last post. On the next post the bridge:

1. Looks up `last-comment:<owner>/<repo>/<pr>/<ns>/<name>` → comment ID + **stability hash** (the deterministic kstatus portion — LLM prose excluded, so re-worded-but-identical state is a full no-op).
2. **Cache hit, unchanged key**: nothing happens — no API call at all.
3. **Cache hit, changed key**: `POST` the fresh comment, then `DELETE` the cached ID. **Skips `ListComments` entirely**.
4. **Cache miss**: full `ListComments + grep marker` — the newest marker comment's **embedded key marker** (`<!-- flux-ai-bridge-key:… -->`) is compared first, so a cold cache with unchanged state keeps the existing comment; otherwise `POST + DELETE` matches (and cache the new comment ID on the way out).

The new comment ID is recorded only after the prior comment is deleted; until then the entry is cleared, so a crash mid-swap leaves a cold cache and the next post sweeps the leftover.

For an autodev push with 200 PRs, that's 200 fewer `ListComments` calls — roughly a third of the comment-posting phase's API budget.

The 404-tolerant fallback handles the active-active race. If a peer replica already deleted the cached comment before our `DELETE` attempt, GitHub returns 404 and the bridge treats that as success (the comment IS gone; the next POST will land cleanly). For any other DELETE error, the cache entry stays cold and the bridge falls back to the full `ListComments` path on the next post.

**Removing a comment on a healthy outcome** (`COMMENT_ON_SUCCESS=false`) uses the same entry: a recorded ID is deleted directly, and afterwards the entry records "no comment" so later healthy events make no API call. Without any entry — after an in-memory restart, or once the Redis backstop expired it — the bridge lists the PR's comments once to find the scope's comment, deletes it, and records "no comment". A stale failure comment therefore can't outlive its cache entry.

**Comment-history mode bypasses this cache** (`config.github.comments.keepHistory=true`). When the operator opts into the append-only comment trail, there's no "previous comment" to delete and the cache would only be dead weight.

## Concurrent resolution (single-flight)

Caching collapses *repeat* and *cross-replica* resolutions, but it can't help the **first** burst. When a new source revision drops, the notification-controller fires **one event per Kustomization within a few seconds** — all before any of them has written the `prs-walk:` entry. Cold-cache, every one of those concurrent events would run the full direct-lookup + merge-parent walk for the *same* SHA: a thundering herd of identical walks, bounded only by `WEBHOOK_MAX_CONCURRENT`. This is the dominant per-revision GitHub-call cost as the bridge widens beyond a single Kustomization.

`FindPRsForCommit` wraps the cache-miss path in a [single-flight group](../internal/github/commenter.go) keyed on `(owner, repo, sha)`. Concurrent callers for the same key coalesce: the **first** runs the walk and warms the cache; the **rest block on it** and reuse its result. The herd collapses to **one walk per new revision** instead of one per Kustomization.

- **Always on, no knob.** It has no tunable trade-off worth exposing — it's most valuable exactly when the cache is cold, so it isn't gated behind the cache TTL either.
- **Latency.** Waiters block for the duration of the one in-flight walk (~1–2s) rather than running their own — roughly the time they'd have spent anyway, and *faster* under load because the single walk runs uncontended instead of N competing for the GitHub client. Resolution is the early step of processing; readiness/LLM/posting still run independently per event afterward, so no PR's analysis is deferred. A waiter stops waiting when its own context ends, and one whose leader was cancelled retries the resolution itself.
- **Failure isolation.** On error the leader `Forget`s the key, so a transient failure isn't pinned onto callers arriving moments later — the next event re-attempts immediately instead of waiting a reconcile cycle.
- **In-process, per replica.** It composes with the (Redis) `prs-walk` cache: single-flight kills the *intra-replica* concurrent herd that the cache can't (the herd races before the first write lands); the shared cache kills the *cross-replica* and *sequential* repeats.

## Backends

Both backends implement the same `fetchcache.Cache` interface — `Get` returns `(nil, false)` on miss or transient backend error; `Put` is best-effort. Cache failures NEVER break the pipeline: a Redis outage degrades gracefully to "always fetch upstream."

### In-memory LRU (default)

[internal/fetchcache/lru.go](../internal/fetchcache/lru.go) — backed by `hashicorp/golang-lru/v2`.

- **Per-replica.** Each bridge pod has its own copy.
- **Lost on restart.** First event after restart pays the full fetch cost for any SHA that was cached before the restart.
- **Capacity bound**: configurable via `FETCH_CACHE_CAPACITY` env var (default 4096 entries, shared across all fetch-cache kinds — prs-walk/prs-for-commit/commit/merge-base/default-branch/pr-files/blobrefs/basefileids/last-comment/pr-outcome).
- **TTL'd entries** are lazily evicted on `Get` after expiry, in addition to the LRU bound.

### Redis / Valkey (opt-in, persistent, cross-replica)

[internal/fetchcache/redis.go](../internal/fetchcache/redis.go) — reuses the same Redis client the dedupe cache uses. When `config.dedupe.persistence.enabled=true` AND the dedupe cache is backed by Redis (single-node or Sentinel), the fetchcache automatically uses the same connection — same auth, same Sentinel quorum, same TLS config, same network. Operators configure Valkey/Redis once for both caches.

- **Cross-replica.** All bridge pods see the same cache entries.
- **Survives restarts.** Cache entries persist as long as the Redis store does.
- **Separate keyspace**: cache keys are prefixed with `flux-ai-bridge:fetchcache:` (vs. `flux-ai-bridge:dedupe:` for the dedupe cache). With `INSTANCE_ID` set, the instance goes after `flux-ai-bridge:`, e.g. `flux-ai-bridge:prod-eu:fetchcache:`.
- **Op timeout 2s**: a stuck Redis doesn't hold the event-processing goroutine longer than that. After 2s the call returns "miss" and the bridge fetches upstream.
- **Set `maxmemory-policy allkeys-lru`** (see [Sizing your Redis](#sizing-your-redis) below — this is the one setting you must not get wrong). Unlike the in-memory LRU, Redis has no built-in entry cap, and the long-lived entries (`blobrefs:`, `pr-files:`, `last-comment:`, dedupe marks, …) only expire after the `DEDUPE_PERSISTENCE_MAX_TTL` backstop (30 days). `allkeys-lru` self-trims under pressure; dedupe correctness is unaffected because an evicted SETNX entry just re-admits one event.

The auto-detection happens in [cmd/main.go `newFetchCache`](../cmd/main.go) via `dedupe.Cache.RedisClient()`:

```go
if client := dedupeCache.RedisClient(); client != nil {
    // keyPrefix is "flux-ai-bridge:[<INSTANCE_ID>:]fetchcache:"; maxTTL is
    // the DEDUPE_PERSISTENCE_MAX_TTL backstop for entries without their own TTL.
    return fetchcache.NewRedis(client, keyPrefix, 2*time.Second, maxTTL)
}
return fetchcache.NewLRU(parseIntEnv("FETCH_CACHE_CAPACITY", 4096))
```

If the operator hasn't enabled dedupe persistence, the dedupe cache is in-memory and `RedisClient()` returns `nil` — the fetchcache falls back to its own in-memory LRU.

### Sizing your Redis

Three different numbers get confused here; only one bounds the cache:

| Number | What it actually limits | Bounds the cache? |
|---|---|---|
| **PVC size** (e.g. 1 GB) | On-disk space for persistence snapshots (RDB/AOF) | ❌ No — Redis is in-memory; the PVC just has to be ≥ the dataset for a save to fit. |
| **Pod memory limit** (e.g. 128Mi) | RAM ceiling; the kernel OOM-kills the Pod past it | Indirectly — it's the hard wall, but Redis doesn't know about it unless you tell it. |
| **`maxmemory`** (e.g. 48mb) | The dataset size Redis will hold before applying its eviction policy | ✅ **Yes — this is the real cache ceiling.** |

So a "1 GB PVC" tells you nothing about cache headroom. The binding limit is `maxmemory`, and what happens when you reach it is decided by `maxmemory-policy`.

**`maxmemory-policy` is the setting you must not get wrong.** Most keyspaces — `blobrefs:`, `pr-files:`, `last-comment:`, `pr-outcome:`, `basefileids:` and the dedupe marks — have no expiry of their own and only expire after the `DEDUPE_PERSISTENCE_MAX_TTL` backstop (default 30 days; `0` disables it). Only `prs-for-commit` (5m) and `commit` (1h) expire sooner. So the dataset grows with every unique PR-blob and PR-scope seen in that window and can reach `maxmemory`. The policy then decides the outcome:

| Policy | At `maxmemory` | Verdict |
|---|---|---|
| `allkeys-lru` | Evicts coldest keys of any kind | ✅ **Required.** Evicted cache key = one extra upstream fetch; evicted dedupe key = one re-admitted event (a possible duplicate comment, absorbed by idempotent delivery). Both harmless. |
| `volatile-lru` / `volatile-ttl` | Evicts only keys **with a TTL** | ⚠️ Works only while the backstop is on (every key then carries a TTL); with `DEDUPE_PERSISTENCE_MAX_TTL=0` the long-lived keys aren't evictable, fill `maxmemory`, and writes fail anyway. |
| `noeviction` (**Redis/Valkey default!**) | **Writes fail** with `OOM command not allowed` | ❌ Outage. The dedupe `SETNX` is fail-closed on write error ([dedupe/redis.go](../internal/dedupe/redis.go) `markIfNewAtomic`), so **every event drops** — the bridge silently stops commenting — until enough keys expire (never, with the backstop disabled). |

**Rules of thumb:**

1. **Always run `allkeys-lru`.** Confirm with `redis-cli CONFIG GET maxmemory-policy`. The bundled Valkey sub-chart now defaults to it (`valkey.extraConfig`); external Redis must set it explicitly.
2. **Set `maxmemory` to ~75% of the Pod memory limit** so Valkey trims before the kernel OOM-kills the Pod (e.g. `maxmemory 96mb` for a 128Mi limit). The ~25% margin covers Redis baseline RSS, fragmentation, and the copy-on-write spike during a background save.
3. **The dataset is small** — single-digit MB at typical scale (it scales with unique PRs/commits/blobs, not event volume). You won't approach even a 48 MB cap normally; `maxmemory` + `allkeys-lru` is insurance, not a tight fit.
4. **Watch `evicted_keys`** (`INFO stats`, or `redis_evicted_keys_total`). Steadily climbing → raise `maxmemory` toward the Pod limit to keep more cache resident (higher hit rate, fewer GitHub/LLM calls) at no extra Pod cost.

## Force-push handling

Force-push is a common worry with cache schemes because it "rewrites history." For the bridge's caches, force-push is a **non-issue** by construction:

- **Blob** and **commit** caches are keyed on content-addressed SHAs. Force-push CAN'T mutate an existing blob or commit — different content always produces a different SHA. Old SHAs are orphaned (no branch refs them anymore) but the cache entries for them remain *correct*. They simply become unused.
- **PRs-for-commit** cache uses the `PRS_FOR_COMMIT_TTL` (5m default). If a force-push removes a commit from a branch, GitHub's API still returns the PR association for that commit (it's still recorded against the PR's commit history). State shifts (merge/close) are picked up on the next miss after the TTL.
- **Last comment ID** cache is keyed by `(PR#, Kustomization scope)` — completely unaffected by branch history rewrites.

Force-pushed commits never appear as parents of new commits (the new commits replace them in the branch graph), so the parent-walk never reaches an orphaned SHA from cache.

## Active-active behavior

With persistent dedupe + persistent fetchcache (both backed by the same Redis), all replicas share the same cache state. The dedupe cache's SETNX ensures only one replica processes each (Kustomization, PR, PR head commit), so cache writes are serialized per cache key by construction.

With in-memory fetchcache + multi-replica deployment, each replica has its own cache and they diverge. The bridge handles divergence gracefully:

- **Blob, commit, prs-for-commit**: divergence wastes one round-trip per replica per SHA (each replica refetches independently the first time). No correctness issue.
- **Last comment ID**: when replica A posts comment 100, replica B's cache doesn't know. When replica B handles the next event for the same scope, its cache miss triggers `ListComments` → finds comment 100 → deletes it → posts comment 101. Replica A's cache still says comment 100 (stale); next time A handles this scope, DELETE 100 returns 404, the bridge treats this as "already gone," and POSTs cleanly. No correctness issue, occasional extra `ListComments` until caches converge.

The chart guard at [charts/flux-ai-bridge/templates/deployment.yaml](../charts/flux-ai-bridge/templates/deployment.yaml) refuses to render `replicaCount > 1` (or autoscaling enabled) without `config.dedupe.persistence.enabled=true`, so the in-memory fetchcache only ever runs in single-replica deployments. The active-active divergence scenarios are a defense-in-depth concern, not the normal path.

## Configuration

Every knob has a sensible default. The first-class env vars and chart values:

| Env var | Chart value | Default | Purpose |
|---|---|---|---|
| `FETCH_CACHE_CAPACITY` | `config.github.cache.capacity` | `4096` | In-memory LRU bound (entries, shared across all fetch-cache kinds). Ignored when Redis-backed. |
| `PARENT_WALK_DEPTH` | `config.github.parentWalkDepth` | `200` | Cap on parent-walk recursion. The walk normally stops earlier, at the merge base with the default branch. Sized for autodev batches; raise if your workflow merges more than 200 PRs in one push. |
| `COMMIT_TTL` | `config.github.cache.commitTTL` | `1h` | Growth bound on the commit-metadata cache; a negative value disables it. |
| `PRS_FOR_COMMIT_TTL` | `config.github.cache.prsForCommitTTL` | `5m` | TTL for the PR-lookup cache. Empty/`0` → the 5m default; a **negative** value (e.g. `-1s`) disables it (blobrefs/commit/last-comment caches stay on). |
| (see dedupe docs) | `config.dedupe.persistence.*` | disabled | When persistent dedupe is enabled, the fetchcache automatically reuses the same Redis client. |
| `DEDUPE_PERSISTENCE_MAX_TTL` | `config.dedupe.persistence.maxTTL` | `720h` | Redis only: expiry for entries written without one (immutable blobs and file lists, last-comment IDs, dedupe marks). `0` disables it. |

### Disabling caching for diagnostics

Set `FETCH_CACHE_CAPACITY=1` to effectively disable all in-memory caching (the LRU will evict immediately after every write). For Redis-backed caching there's no on-the-fly disable; restart the bridge without persistent dedupe to fall back to a tiny in-memory cache. This is intended for diagnostic purposes only.

## Metrics

Hit/miss counters per cache kind:

```prometheus
flux_ai_bridge_fetch_cache_hits_total{kind="prs_walk|pr_files|blobrefs|basefileids|commit|merge_base|default_branch|prs_for_commit|last_comment|pr_outcome"}
flux_ai_bridge_fetch_cache_misses_total{kind="prs_walk|pr_files|blobrefs|basefileids|commit|merge_base|default_branch|prs_for_commit|last_comment|pr_outcome"}
```

Single-flight coalescing (the concurrent herd that never reached a walk):

```prometheus
flux_ai_bridge_prs_for_commit_coalesced_total
```

A rising `coalesced_total` tracks duplicate walks avoided — it grows with the number of Kustomizations sharing a branch. The precise count of *real* walks is the `prs_walk` **miss** counter (recorded only by the single-flight leader); waiters increment neither hit nor miss.

A useful PromQL query for cache effectiveness:

```promql
rate(flux_ai_bridge_fetch_cache_hits_total[5m])
  / (rate(flux_ai_bridge_fetch_cache_hits_total[5m])
   + rate(flux_ai_bridge_fetch_cache_misses_total[5m]))
```

Per-cache-kind expectations in a healthy production deployment with persistent caching:

| Kind | Expected hit rate | If lower, check |
|---|---|---|
| `blobrefs` | 70-95% | Working set may exceed LRU capacity; raise `FETCH_CACHE_CAPACITY` or move to Redis-backed. |
| `commit` | 80-99% (steady state) | First few events after restart are cold; if sustained low, the parent walk is hitting NEW commits every event (unusual). |
| `prs_for_commit` | 40-80% | TTL'd — hit rate depends on how often the same SHA is queried within the TTL window. Low hit rate is normal if events are spaced far apart. |
| `last_comment` | 80-99% (steady state) | Every Kustomization-PR pair pays one miss on first post; subsequent posts to the same scope should hit. Low rate suggests the bridge is constantly seeing new PRs (typical of large autodev orgs). |

GitHub rate-limit headroom is monitored separately:

```prometheus
flux_ai_bridge_github_rate_limit_remaining   # gauge — most recent X-RateLimit-Remaining
flux_ai_bridge_github_rate_limit_hits_total  # counter — typed 403/429 from GitHub
```

If `remaining` is trending down despite high cache hit rates, the bottleneck is one of the **uncacheable** API calls — typically `POST /issues/{pr}/comments` (one per PR per event, unavoidable). The only further savings there would be reducing how many PRs the bridge comments on (label filters, inventory tightening).

### Total request volume (GitHub + LLM)

The cache hit/miss counters only cover the *cached read surfaces*. For the actual outbound request rate to each upstream — the number to compare against your GitHub and LLM provider rate limits — use:

```prometheus
flux_ai_bridge_github_api_requests_total{method,status}   # EVERY request to GitHub (transport layer)
flux_ai_bridge_llm_requests_total{provider,outcome}        # every LLM completion call
```

`github_api_requests_total` is counted below go-github, so it includes uncached reads, comment posts/deletes, label writes, pagination, and App token refreshes — the complete picture that summing cache misses can't give you. `llm_requests_total` is the call-count companion to the token counters (`llm_input_tokens_total` / `llm_output_tokens_total`): tokens are volume, this is request count, with `outcome` separating `success` / `rate_limited` / `error`.

Requests per hour, to compare against the limits:

```promql
sum(rate(flux_ai_bridge_github_api_requests_total[5m])) * 3600   # vs GitHub's 5,000+/hr (scales with installation size)
sum(rate(flux_ai_bridge_llm_requests_total[5m])) * 3600          # vs your provider RPM*60
```

## Tuning for very large autodev batches

If your dev branch regularly absorbs hundreds of PRs in one push:

1. **Raise `PARENT_WALK_DEPTH`** so all PRs in the batch resolve. The default 200 handles batches up to 200 PRs; for larger batches set this to whatever upper bound you expect plus a safety margin (e.g. 500 for 300-PR batches).
2. **Enable persistent dedupe + Redis-backed fetchcache** so:
   - The dedupe cache survives restarts (no re-processing the entire backlog after a deploy).
   - The fetchcache is shared across replicas (no per-replica cold starts).
   - Cache entries persist across batches; the commit-metadata cache is TTL-bounded (1h) so it amortizes the parent walk without growing the keyspace forever — pair it with a Redis `maxmemory`/eviction policy (see Redis backend above).
3. **Watch `flux_ai_bridge_github_rate_limit_remaining`.** If it dips below ~1000 during peak autodev hours, you're near the limit. Combinations that help: raise `PRS_FOR_COMMIT_TTL` above its 5m default (safe to push to tens of minutes when you don't use the `PR_REQUIRED_LABELS` gate — only PR state goes stale, and dedup cushions that), tighter `config.kustomizations.include` to scope the bridge to fewer PRs, or a higher GitHub App rate limit (Enterprise tier).
4. **Don't tune `FETCH_CACHE_CAPACITY` blindly upward** — 4096 covers ~4096 distinct blob + commit + PR-list keys, which is plenty for typical workloads. If you have millions of unique blobs, move to Redis backend rather than enlarging the in-memory LRU; the heap pressure isn't worth it.

## Implementation notes

- The **fetch-cache** layer is **fail-open**. Any backend error (Redis down, AUTH rejected, network timeout) is logged at WARN and the call returns `(nil, false)`. The bridge then fetches upstream. Caching is a performance optimization, not a correctness primitive — a Redis outage degrades throughput but doesn't break the pipeline. (The **dedupe admission** is the opposite: it fails **closed** — a Redis error on `MarkPRIfNew` skips the event rather than risking uncoordinated duplicate analysis; see [architecture.md → Dedupe cache](architecture.md#dedupe-cache-per-pr-head-processing).)
- The cache is **best-effort write**: failed `Put` operations are silently dropped (still logged at WARN). The next `Get` will be a miss and refetch upstream.
- All cache **values are byte slices** at the interface boundary. The `commenter` JSON-encodes structured values (commit parents as `[]string`, PR info as `[]PRInfo`) before `Put` and decodes after `Get`. If a cached value is malformed JSON (corrupted entry, schema drift across versions), the read is treated as a miss and refetched. No wedge state.
- Cache keys are **stable**. The key formats (`blobrefs:`, `commit:`, `prs-for-commit2:`, `last-comment:`) are versioned implicitly — a future schema change would use a new prefix to invalidate the old keyspace without explicit migration.

For the source: [internal/fetchcache/](../internal/fetchcache/) (the cache package itself), [internal/github/commenter.go](../internal/github/commenter.go) (call sites for all four caches), [internal/dedupe/cache.go](../internal/dedupe/cache.go) (`RedisClient()` accessor that lets fetchcache share the dedupe connection), [cmd/main.go](../cmd/main.go) (`newFetchCache`).
