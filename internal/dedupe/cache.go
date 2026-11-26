// Copyright 2026, Staffbase SE and contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dedupe owns the admission record of "we've already analysed this".
// The primary identity is per-PR-head: (namespace, kustomization, PR number,
// PR head SHA) via MarkPRIfNew — a PR is analysed once per its own head
// commit, optionally re-admitted after DEDUPE_PR_REEVAL_TTL. The processor
// (admission + rollback) and main.go (construction)
// share one Cache. The same store also keeps per-PR outcome records and the
// per-PR scope roster used for label aggregation.
//
// Without dedupe, Flux's periodic ReconciliationSucceeded re-fires on every
// Kustomization.spec.interval (typically 1–5 minutes) would cause the bridge
// to re-post the same analysis indefinitely. An admission entry is evicted by:
//
//   - the processor's deferred rollback when delivery failed (ForgetPR)
//   - a rate-limit cooldown lapsing (ExpirePR)
//   - DEDUPE_PR_REEVAL_TTL expiry (per-PR-head re-eval floor)
//   - in-memory backend only: the LRU bound (default capacity 4096) and
//     process restart. The Redis/Valkey backend has NO capacity bound —
//     records the caller wants kept forever get the WithMaxTTL backstop
//     (DEDUPE_PERSISTENCE_MAX_TTL) instead, so Redis can't fill up.
//
// Three configuration modes for the in-memory backend (the Redis backend
// ignores DEDUPE_TTL entirely):
//
//   - **No expiry** (NewNoExpiry, the default in main.go): entries persist
//     until evicted as above.
//   - **TTL** (New with positive ttl): entries expire after ttl. Useful for
//     operators wanting periodic refresh-posts on long-lived PRs.
//   - **Disabled** (New with ttl <= 0): every MarkPRIfNew returns true. Used
//     by tests and by operators setting DEDUPE_TTL=0 (or any zero duration).
package dedupe

import (
	"log/slog"
	"strconv"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	expirable "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/redis/go-redis/v9"
)

// Key is the dedupe identity: one Kustomization, one PR, at one PR head
// commit — the same no matter how many times Flux re-fires its Alert, which
// severity/reason the event carries, or how far the source branch advanced
// underneath an unchanged PR.
//
// Severity is intentionally NOT in the key: the bridge's contract is "one
// analysis per PR head." A pod that crashes 10 min after a healthy analysis
// produces an error event for the same head — that does not trigger a fresh
// analysis until a new commit (or the re-eval floor, if configured). This
// keeps the PR comment stable and tied to deploys.
type Key struct {
	Namespace string
	Name      string
	PR        int
	SHA       string // the PR's own head commit
}

// store abstracts over the expirable and non-expirable LRU implementations
// from hashicorp/golang-lru/v2 so the Cache wrapper doesn't care which it
// holds. The three methods we use match across both types.
type store interface {
	Get(Key) (time.Time, bool)
	Add(Key, time.Time) bool
	Remove(Key) bool
}

// Cache is the only public type. Zero value is unusable; use New or
// NewNoExpiry. A Cache with a nil store is the "disabled" form — every
// method is a safe no-op, MarkPRIfNew always reports true.
//
// Safe for concurrent use.
type Cache struct {
	mu sync.Mutex
	s  store // nil = disabled
	// outcomes is a best-effort side-channel recording the LAST analysis
	// outcome per (ns, name, PR, head) — used ONLY by the re-eval-failed-only
	// optimization to skip re-polling a previously-healthy PR-head. It is NOT a
	// correctness mechanism (the atomic mark above still guarantees
	// at-most-once); a miss just means we re-poll as usual. nil for the
	// Redis-backed Cache (which stores outcomes via redisStore.client) and for
	// the disabled Cache.
	outcomes *lru.Cache[Key, string]
	// labels holds, per PR-head, a map of Kustomization scope ("<ns>/<name>") →
	// last analysis outcome — so the processor can set a PR label reflecting ALL
	// Kustomizations on the PR (any failure → flux-failed; all success →
	// flux-success) instead of last-writer-wins. Best-effort, like outcomes; nil
	// for the Redis-backed Cache (stored via redisStore.client as a HASH) and the
	// disabled Cache. In-memory mode is per-replica, so correct cross-Kustomization
	// aggregation across multiple replicas needs the persistent (Redis) backend.
	labels *lru.Cache[string, map[string]string]
	// reevals counts TTL re-evaluations per (ns, name, PR, head) — consulted by
	// the DEDUPE_PR_REEVAL_MAX cap. Best-effort like outcomes: a lost count just
	// means an extra re-eval, never a correctness issue. nil for the
	// Redis-backed Cache (counted via INCR) and the disabled Cache.
	reevals *lru.Cache[Key, int]
	// cooldowns holds, per admission mark, the time an ExpirePR cooldown lapses
	// — the in-memory stand-in for a Redis key expiry. nil for the Redis-backed
	// Cache (EXPIRE) and the disabled Cache.
	cooldowns *lru.Cache[Key, time.Time]
}

// RedisClient returns the underlying go-redis client when the Cache is
// backed by Redis/Valkey (single-node or Sentinel), or nil for the
// in-memory and disabled forms. Exposed so other packages can share the
// connection without re-reading env vars — specifically the fetchcache
// package reuses this client for the persistent-cache backend so the
// operator only configures Valkey once.
func (c *Cache) RedisClient() *redis.Client {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.s.(*redisStore); ok {
		return r.client
	}
	return nil
}

// New constructs a TTL-bounded Cache. ttl <= 0 returns a disabled cache
// (callers should prefer NewNoExpiry for the commit-driven contract).
// capacity <= 0 falls back to 4096 entries.
func New(ttl time.Duration, capacity int) *Cache {
	if ttl <= 0 {
		return &Cache{}
	}
	if capacity <= 0 {
		capacity = 4096
	}
	outcomes, _ := lru.New[Key, string](capacity)
	labels, _ := lru.New[string, map[string]string](capacity)
	reevals, _ := lru.New[Key, int](capacity)
	cooldowns, _ := lru.New[Key, time.Time](capacity)
	return &Cache{s: expirable.NewLRU[Key, time.Time](capacity, nil, ttl), outcomes: outcomes, labels: labels, reevals: reevals, cooldowns: cooldowns}
}

// NewNoExpiry constructs a Cache whose entries never expire. Eviction is
// driven exclusively by the LRU capacity bound, explicit ForgetPR calls
// (pipeline error → retry next event), ExpirePR cooldowns, and process
// restart.
// This is the default mode and matches the bridge's event-driven contract:
// a SHA is processed exactly once until something real changes.
func NewNoExpiry(capacity int) *Cache {
	if capacity <= 0 {
		capacity = 4096
	}
	c, _ := lru.New[Key, time.Time](capacity)
	outcomes, _ := lru.New[Key, string](capacity)
	labels, _ := lru.New[string, map[string]string](capacity)
	reevals, _ := lru.New[Key, int](capacity)
	cooldowns, _ := lru.New[Key, time.Time](capacity)
	return &Cache{s: c, outcomes: outcomes, labels: labels, reevals: reevals, cooldowns: cooldowns}
}

// WithMaxTTL sets the backstop expiry for Redis-backed records written with
// "never expire" semantics (admission marks without a re-eval floor, outcome
// and label records), so they can't accumulate in Redis forever. A positive
// ttl passed by a caller still wins. d <= 0 disables the backstop. No-op for
// the in-memory backend, which is LRU-bounded and dies with the process. Call
// before the Cache is shared. Returns c for chaining.
func (c *Cache) WithMaxTTL(d time.Duration) *Cache {
	if c == nil {
		return nil
	}
	if r, ok := c.s.(*redisStore); ok {
		r.maxTTL = max(d, 0)
	}
	return c
}

// Available reports whether admission writes can be expected to work. It only
// costs a round-trip after a Redis admission write failed: then it PINGs, and
// a reply clears the failure. Always true for the in-memory and disabled
// forms. The processor checks it before GitHub lookups, so a Redis outage —
// during which every PR would be skipped as dedupe_unhealthy anyway — doesn't
// keep spending GitHub rate limit on PR resolution.
func (c *Cache) Available() bool {
	if c == nil {
		return true
	}
	r, ok := c.s.(*redisStore)
	if !ok || r.lastFailure.Load() == 0 {
		return true
	}
	return r.ping()
}

// MarkPRIfNew is the per-PR-head write-path primitive. It records that PR
// `pr` (in Kustomization scope ns/name) has been analysed at head commit
// `headSHA`, returning true exactly once per (ns, name, pr, headSHA) — so an
// open PR is commented on once per push to its OWN branch, regardless of how
// many times the Flux source revision advances underneath it.
//
// ttl is the re-evaluation floor: 0 keeps the record forever (analyse a given
// PR-head exactly once); a positive ttl lets the record expire so a later
// event re-admits the same PR-head, catching runtime drift (a pod that starts
// crashlooping minutes after a green deploy) without a new commit. The comment
// layer is idempotent, so a re-eval that finds identical state touches nothing.
//
// The floor also applies to marks that predate it, so enabling
// DEDUPE_PR_REEVAL_TTL on a running install picks up already-analysed PR-heads
// instead of leaving them deduped forever: the in-memory backend compares the
// mark's stored timestamp against ttl on every call, and the Redis backend
// attaches the expiry to pre-floor (no-expiry) marks via EXPIRE NX (requires
// Redis ≥ 7.0 / Valkey), so they self-free one window after their next event.
//
// Returns (false, err) when the backend is unreachable (Redis down, auth
// misconfigured): callers MUST fail closed and skip the PR — without dedupe
// coordination there's no at-most-once analysis across replicas, and every
// Flux re-fire would re-run the LLM and re-post. A nil or disabled cache
// reports every PR-head as new (true, nil). Redis enforces atomicity with
// SET NX (cross-replica safe); in memory it's the local mutex.
func (c *Cache) MarkPRIfNew(ns, name string, pr int, headSHA string, ttl time.Duration) (bool, error) {
	if c == nil || c.s == nil {
		return true, nil
	}
	key := Key{Namespace: ns, Name: name, PR: pr, SHA: headSHA}
	if r, ok := c.s.(*redisStore); ok {
		return r.markIfNewAtomic(key, time.Now(), ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, hit := c.s.Get(key); hit {
		// An ExpirePR cooldown replaces the floor: the mark lapses at the
		// cooldown's end, like a Redis key whose expiry EXPIRE rewrote.
		if until, cooling := c.cooldowns.Get(key); cooling {
			if time.Now().Before(until) {
				return false, nil
			}
			c.cooldowns.Remove(key)
			c.s.Add(key, time.Now())
			return true, nil
		}
		// In-memory floor: an expirable LRU already drops entries past its
		// global ttl, but when this cache is non-expirable we enforce the
		// per-call floor here by re-admitting once the prior mark is older
		// than ttl. ttl<=0 means "never re-admit".
		if ttl > 0 && time.Since(t) >= ttl {
			c.s.Add(key, time.Now())
			return true, nil
		}
		return false, nil
	}
	// The cooldown is the mark's expiry, like a Redis EXPIRE: a mark the
	// TTL cache dropped (its global ttl shorter than the cooldown) still
	// dedupes until the cooldown ends, so a rate-limited PR doesn't hit
	// GitHub again before the limit resets.
	if until, cooling := c.cooldowns.Get(key); cooling && time.Now().Before(until) {
		return false, nil
	}
	// A lapsed cooldown must not cut this fresh mark short.
	c.cooldowns.Remove(key)
	c.s.Add(key, time.Now())
	return true, nil
}

// ForgetPR removes a per-PR-head admission record (the MarkPRIfNew inverse).
// Used when the analysis pipeline fails after the mark was committed, so the
// next event re-admits the PR instead of being silently skipped. No-op on a
// nil or disabled cache.
func (c *Cache) ForgetPR(ns, name string, pr int, headSHA string) {
	if c == nil || c.s == nil {
		return
	}
	key := Key{Namespace: ns, Name: name, PR: pr, SHA: headSHA}
	// Redis is its own synchronization point — holding c.mu across the
	// network round-trip (up to opTimeout) would serialize every other
	// dedupe operation behind a slow Valkey. Same bypass as MarkPRIfNew.
	if r, ok := c.s.(*redisStore); ok {
		r.Remove(key)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.s.Remove(key)
	c.cooldowns.Remove(key)
}

// ExpirePR keeps a per-PR-head admission record but makes it lapse after d —
// a cooldown, between ForgetPR (re-admit on the next event) and keeping the
// mark (never re-admit at this head). Used when delivery hit a GitHub rate
// limit: Flux's re-fires stay deduped until the limit has reset, then the
// next event re-admits the PR. d <= 0 is ForgetPR. No-op on a nil or
// disabled cache.
func (c *Cache) ExpirePR(ns, name string, pr int, headSHA string, d time.Duration) {
	if c == nil || c.s == nil {
		return
	}
	if d <= 0 {
		c.ForgetPR(ns, name, pr, headSHA)
		return
	}
	key := Key{Namespace: ns, Name: name, PR: pr, SHA: headSHA}
	if r, ok := c.s.(*redisStore); ok {
		r.expire(key, d)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, hit := c.s.Get(key); hit {
		c.cooldowns.Add(key, time.Now().Add(d))
	}
}

// RecordPROutcome stores the most recent analysis outcome for a (PR, head) so
// a later re-eval can decide whether re-polling is worthwhile (see
// DedupeReEvalFailedOnly). Best-effort: a write failure or eviction only costs
// us the optimization (we re-poll), never correctness. ttl bounds the
// Redis-backed record so orphaned entries self-clean after a PR-head goes
// quiet; the in-memory store ignores ttl (it's LRU-bounded). No-op on a
// nil/disabled cache.
func (c *Cache) RecordPROutcome(ns, name string, pr int, headSHA, outcome string, ttl time.Duration) {
	if c == nil || c.s == nil {
		return
	}
	key := Key{Namespace: ns, Name: name, PR: pr, SHA: headSHA}
	if r, ok := c.s.(*redisStore); ok {
		r.setOutcome(key, outcome, ttl)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outcomes != nil {
		c.outcomes.Add(key, outcome)
	}
}

// LastPROutcome returns the outcome recorded by the last analysis of this
// (PR, head), or ("", false) if none is known (never analysed, evicted,
// expired, or store unreachable). Callers treat "not found" as "must analyse".
func (c *Cache) LastPROutcome(ns, name string, pr int, headSHA string) (string, bool) {
	if c == nil || c.s == nil {
		return "", false
	}
	key := Key{Namespace: ns, Name: name, PR: pr, SHA: headSHA}
	if r, ok := c.s.(*redisStore); ok {
		return r.getOutcome(key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outcomes == nil {
		return "", false
	}
	return c.outcomes.Get(key)
}

// IncrPRReEval bumps and returns this (PR, head)'s re-evaluation count — the
// processor calls it after an analysis whose outcome was failed/warning (and
// on exhausted-skip windows, to keep the streak's record alive); healthy
// analyses don't touch it, so the budget meters FAILING work only. Best-effort
// like the outcome records: on a backend error it returns 0, which callers
// read as "cap not reached, keep going" — the cost profile degrades to the
// uncapped behavior, never blocks an analysis. ttl bounds the Redis record and
// is refreshed on every call, so an actively-failing head keeps its count
// while a quiet or healthy one self-cleans (after which a head that drifts
// unhealthy later gets a fresh budget — consistent with its outcome record
// expiring too). The in-memory store ignores ttl (LRU-bounded). Always 0 on a
// nil/disabled cache.
func (c *Cache) IncrPRReEval(ns, name string, pr int, headSHA string, ttl time.Duration) int {
	if c == nil || c.s == nil {
		return 0
	}
	key := Key{Namespace: ns, Name: name, PR: pr, SHA: headSHA}
	if r, ok := c.s.(*redisStore); ok {
		return r.incrReEval(key, ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reevals == nil {
		return 0
	}
	n, _ := c.reevals.Get(key)
	n++
	c.reevals.Add(key, n)
	return n
}

// PRReEvalCount peeks the (PR, head)'s re-evaluation count without touching
// it — the processor's pre-analysis budget check. 0 on miss/error/disabled
// (fail-open: no count, no cap).
func (c *Cache) PRReEvalCount(ns, name string, pr int, headSHA string) int {
	if c == nil || c.s == nil {
		return 0
	}
	key := Key{Namespace: ns, Name: name, PR: pr, SHA: headSHA}
	if r, ok := c.s.(*redisStore); ok {
		return r.getReEval(key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reevals == nil {
		return 0
	}
	n, _ := c.reevals.Get(key)
	return n
}

// labelStateKey identifies the per-PR-head scope→outcome map. Keyed by
// owner/repo/PR/head so two repos (or two heads of one PR) never share state.
func labelStateKey(owner, repo string, pr int, headSHA string) string {
	return owner + "/" + repo + ":pr" + strconv.Itoa(pr) + ":" + headSHA
}

// PutScopeOutcome records Kustomization `scope`'s latest analysis outcome for a
// PR-head, into the map the label aggregator reads. ttl bounds the Redis-backed
// record (refreshed on each call); the in-memory store is LRU-bounded and
// ignores ttl. No-op on a nil/disabled cache.
func (c *Cache) PutScopeOutcome(owner, repo string, pr int, headSHA, scope, outcome string, ttl time.Duration) {
	if c == nil || c.s == nil {
		return
	}
	key := labelStateKey(owner, repo, pr, headSHA)
	if r, ok := c.s.(*redisStore); ok {
		r.hsetScopeOutcome(key, scope, outcome, ttl)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.labels == nil {
		return
	}
	prev, _ := c.labels.Get(key)
	next := make(map[string]string, len(prev)+1)
	for k, v := range prev {
		next[k] = v
	}
	next[scope] = outcome
	c.labels.Add(key, next)
}

// RemoveScopeOutcome drops a Kustomization scope from a PR-head's map (used when
// a PR no longer touches that Kustomization). Returns true if the scope was
// present, so callers only recompute the label when something actually changed.
func (c *Cache) RemoveScopeOutcome(owner, repo string, pr int, headSHA, scope string) bool {
	if c == nil || c.s == nil {
		return false
	}
	key := labelStateKey(owner, repo, pr, headSHA)
	if r, ok := c.s.(*redisStore); ok {
		return r.hdelScopeOutcome(key, scope)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.labels == nil {
		return false
	}
	prev, ok := c.labels.Get(key)
	if !ok {
		return false
	}
	if _, present := prev[scope]; !present {
		return false
	}
	next := make(map[string]string, len(prev))
	for k, v := range prev {
		if k != scope {
			next[k] = v
		}
	}
	c.labels.Add(key, next)
	return true
}

// ScopeOutcomes returns the PR-head's scope→outcome map (a copy), or an empty
// map. The processor aggregates these into a single label.
func (c *Cache) ScopeOutcomes(owner, repo string, pr int, headSHA string) map[string]string {
	if c == nil || c.s == nil {
		return nil
	}
	key := labelStateKey(owner, repo, pr, headSHA)
	if r, ok := c.s.(*redisStore); ok {
		return r.hgetallScopeOutcomes(key)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.labels == nil {
		return nil
	}
	prev, ok := c.labels.Get(key)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(prev))
	for k, v := range prev {
		out[k] = v
	}
	return out
}

// rosterKey identifies the per-PR (head-INDEPENDENT) set of Kustomization
// scopes the bridge has attributed the PR to. Unlike labelStateKey it carries no
// head SHA, so the roster persists across the PR's commits — a new push must not
// make the bridge forget which Kustomizations the PR is known to touch. The
// processor uses it to decide when a SUCCESS aggregate is "complete" enough to
// label (every known scope has reported for the current head) vs. still partial.
func rosterKey(owner, repo string, pr int) string {
	return owner + "/" + repo + ":pr" + strconv.Itoa(pr) + ":roster"
}

// AddPRScope records that `scope` is one of the Kustomizations this PR touches
// (head-independent roster). ttl bounds the Redis record, refreshed each call;
// the in-memory store is LRU-bounded and ignores ttl. No-op on a nil cache.
func (c *Cache) AddPRScope(owner, repo string, pr int, scope string, ttl time.Duration) {
	if c == nil || c.s == nil {
		return
	}
	key := rosterKey(owner, repo, pr)
	if r, ok := c.s.(*redisStore); ok {
		r.hsetScopeOutcome(key, scope, "1", ttl)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.labels == nil {
		return
	}
	prev, _ := c.labels.Get(key)
	next := make(map[string]string, len(prev)+1)
	for k, v := range prev {
		next[k] = v
	}
	next[scope] = "1"
	c.labels.Add(key, next)
}

// RemovePRScope drops `scope` from the PR roster (when the PR no longer touches
// that Kustomization). Returns true if it was present.
func (c *Cache) RemovePRScope(owner, repo string, pr int, scope string) bool {
	if c == nil || c.s == nil {
		return false
	}
	key := rosterKey(owner, repo, pr)
	if r, ok := c.s.(*redisStore); ok {
		return r.hdelScopeOutcome(key, scope)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.labels == nil {
		return false
	}
	prev, ok := c.labels.Get(key)
	if !ok {
		return false
	}
	if _, present := prev[scope]; !present {
		return false
	}
	next := make(map[string]string, len(prev))
	for k, v := range prev {
		if k != scope {
			next[k] = v
		}
	}
	c.labels.Add(key, next)
	return true
}

// PRScopes returns the PR roster: every Kustomization scope the bridge has
// attributed this PR to, across heads. Order is unspecified. Best-effort: a
// backend read error returns an empty roster (logged); callers that must not
// mistake an unreadable roster for an empty one use ReadPRScopes.
func (c *Cache) PRScopes(owner, repo string, pr int) []string {
	scopes, err := c.ReadPRScopes(owner, repo, pr)
	if err != nil {
		slog.Default().Warn("dedupe: redis HGETALL roster failed, treating as empty (best-effort)",
			"owner", owner, "repo", repo, "pr", pr, "error", err.Error())
	}
	return scopes
}

// ReadPRScopes is PRScopes with the backend read error. Deciding that every
// known scope has reported (the gate for a flux-success label) must fail
// closed on an error: an unreadable roster is not an empty one.
func (c *Cache) ReadPRScopes(owner, repo string, pr int) ([]string, error) {
	if c == nil || c.s == nil {
		return nil, nil
	}
	key := rosterKey(owner, repo, pr)
	var m map[string]string
	if r, ok := c.s.(*redisStore); ok {
		var err error
		if m, err = r.hgetall(key); err != nil {
			return nil, err
		}
	} else {
		c.mu.Lock()
		if c.labels != nil {
			if prev, ok := c.labels.Get(key); ok {
				m = make(map[string]string, len(prev))
				for k, v := range prev {
					m[k] = v
				}
			}
		}
		c.mu.Unlock()
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out, nil
}
