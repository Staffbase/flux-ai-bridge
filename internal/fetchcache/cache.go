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

// Package fetchcache deduplicates expensive GitHub API calls whose results
// are stable across events. The bridge's hot path (per Flux event) can fan
// out into dozens of GitHub calls — most heavily on autodev-style flows
// where a dev branch accumulates many merge commits and the bridge walks
// the parent chain to resolve each PR. Caching those lookups is what makes
// large autodev batches tractable without burning the GitHub rate limit.
//
// Three caches with different mutability profiles share one backend
// implementation:
//
//   - Blob content (`blob:<owner>/<repo>/<sha>` → []byte):
//     Git blobs are content-addressed — the SHA fully identifies the
//     payload, which CANNOT change. Cache forever; never invalidate.
//
//   - Commit metadata (`commit:<owner>/<repo>/<sha>` → JSON-encoded
//     []parentSHA): A commit's parents are immutable once the commit is
//     written. Cache forever. This is the load-bearing cache for autodev:
//     walking the dev-branch parent chain produces one GetCommit call per
//     hop, and the same ancestors are walked on every subsequent merge
//     event. Without caching, deep walks are O(depth × events).
//
//   - PRs-for-commit (`prs-for-commit2:<owner>/<repo>/<sha>` → JSON
//     {"at": <fetch time>, "prs": [...]PRInfo}): The PR numbers a commit belongs to are stable, but state
//     (open/closed/merged) and labels mutate. Cache with a short TTL so
//     same-commit lookups within the TTL window reuse the response, and
//     longer-term changes refresh on the next miss. The bridge's filter
//     logic tolerates briefly-stale state (commenting on a just-closed PR
//     is cosmetic, not harmful).
//
// Backends:
//
//   - In-memory LRU (default): per-replica, lost on restart. Matches the
//     existing dedupe model for single-replica deployments.
//   - Redis/Valkey (opt-in, reuses the dedupe persistence connection):
//     shared across replicas, survives restarts. Same SETNX-class
//     semantics — `Get` on a missing key returns (nil, false, nil).
package fetchcache

import (
	"context"
	"time"
)

// Cache is the byte-oriented interface every backend implements. Callers
// serialize their domain types (PRInfo, parent SHAs) to JSON before Put
// and deserialize after Get; keeping the interface generic lets the same
// backend serve blob/commit/PRs-for-commit caches with different TTL
// semantics encoded by the caller.
//
// Fail-open contract: Get returns (nil, false) on a cache miss OR a
// transient backend error (e.g. Redis down). Callers MUST treat a miss as
// "fetch upstream" rather than "give up" — caching is best-effort
// optimization, not a correctness primitive.
type Cache interface {
	// Get fetches the value for key. Returns (nil, false) on cache miss
	// or backend error.
	Get(ctx context.Context, key string) ([]byte, bool)
	// Put stores value under key with the given TTL. A zero ttl means "no
	// expiry of its own" — the entry lives until evicted (LRU bound for
	// in-memory) or, for Redis, until the DEDUPE_PERSISTENCE_MAX_TTL
	// backstop. Errors are logged but not surfaced; fail-open.
	Put(ctx context.Context, key string, value []byte, ttl time.Duration)
	// Delete removes the entry for key. Used by callers that need a true
	// invalidation primitive (Put with empty bytes leaves a tombstone-
	// shaped row that costs a slot in the LRU and a permanent key in
	// Redis). No-op when the key is absent; errors are logged but not
	// surfaced — same fail-open contract as Put.
	Delete(ctx context.Context, key string)
	// TryLock takes a short-lived exclusive lock named key, for work that
	// must not interleave across replicas (e.g. a PR's label reconcile).
	// ok=false with a nil error means another holder has it: the caller
	// skips its work rather than waiting. A non-nil error means the backend
	// couldn't be asked; the caller decides how to fail. On success the
	// caller must call release once its work is done; the lock also lapses
	// on its own after ttl (which must be positive), so a crashed holder
	// blocks the others for at most ttl.
	//
	// Unlike Get/Put/Delete this is not fail-open: an error is returned, not
	// hidden, because "couldn't ask" and "someone else holds it" call for
	// different handling. Backends that serve a single process (in-memory,
	// disabled) always grant the lock: the process's own mutex already
	// serializes its callers.
	TryLock(ctx context.Context, key string, ttl time.Duration) (release func(), ok bool, err error)
}

// Disabled returns a Cache that misses on every Get and drops every Put.
// Used when the operator opts out of caching entirely (e.g. for
// diagnosing a suspected cache bug) so callers don't need branchy `if
// cache != nil` checks.
func Disabled() Cache { return disabled{} }

type disabled struct{}

func (disabled) Get(context.Context, string) ([]byte, bool)         { return nil, false }
func (disabled) Put(context.Context, string, []byte, time.Duration) {}
func (disabled) Delete(context.Context, string)                     {}

func (disabled) TryLock(context.Context, string, time.Duration) (func(), bool, error) {
	return noopRelease, true, nil
}

// noopRelease is the release func of a lock that exists only in-process.
func noopRelease() {}
