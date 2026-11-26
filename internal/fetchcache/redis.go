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

package fetchcache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/staffbase/flux-ai-bridge/internal/lifecycle"
)

// redisCache is the Redis/Valkey-backed implementation. It reuses the
// existing dedupe persistence connection so operators don't need to
// provision a second store — the fetchcache and dedupe share the same
// Valkey/Sentinel/auth setup. A separate key prefix
// (`flux-ai-bridge:fetchcache:*`) keeps the keyspaces from colliding.
//
// Fail-open: any Redis error (connection dropped, AUTH rejected, network
// timeout) is logged at WARN and Get returns (nil, false). The caller
// then fetches upstream — caching is a perf optimization, not a
// correctness primitive, so a temporary Redis outage degrades gracefully
// to "always fetch" without breaking the pipeline.
type redisCache struct {
	client    redis.UniversalClient
	keyPrefix string
	opTimeout time.Duration
	// maxTTL replaces a "no expiry" (ttl <= 0) Put, so records for PRs and
	// commits nobody will ask about again don't accumulate forever. 0 keeps
	// them indefinitely.
	maxTTL time.Duration
}

// NewRedis wraps an existing go-redis client (single-node or
// FailoverClient for sentinel mode — the bridge's dedupe package owns
// the connection details). keyPrefix is prepended to every cache key so
// the fetchcache keyspace doesn't collide with the dedupe keyspace on
// the same Redis instance. opTimeout caps individual Get/Put calls so a
// stuck Redis doesn't hold the event-processing goroutine. maxTTL is the
// expiry applied to Puts that ask for none (0 = really none).
func NewRedis(client redis.UniversalClient, keyPrefix string, opTimeout, maxTTL time.Duration) Cache {
	if keyPrefix == "" {
		keyPrefix = "flux-ai-bridge:fetchcache:"
	}
	if opTimeout <= 0 {
		opTimeout = 2 * time.Second
	}
	return &redisCache{client: client, keyPrefix: keyPrefix, opTimeout: opTimeout, maxTTL: max(maxTTL, 0)}
}

func (c *redisCache) Get(ctx context.Context, key string) ([]byte, bool) {
	ctx, cancel := context.WithTimeout(ctx, c.opTimeout)
	defer cancel()
	val, err := c.client.Get(ctx, c.keyPrefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false
	}
	if err != nil {
		slog.Default().Warn("fetchcache: redis GET failed, treating as miss",
			"key", key, "error", err.Error())
		return nil, false
	}
	return val, true
}

// Put stores value under key with the given TTL. A zero ttl means "no
// expiry of its own" — entries that are immutable (blobs, per-head file
// lists) or bookkeeping (last comment ID, label outcome) — and gets the
// maxTTL backstop.
//
// Put and Delete detach from the caller's cancellation: they record what a
// GitHub write already did (a new comment's ID, a deleted comment), and
// losing that because the pipeline was cancelled right after the write
// leaves the cache describing a comment that no longer exists. opTimeout
// still bounds them, and shutdown still stops them (see lifecycle), so a
// stalled Redis can't eat the unwind budget.
func (c *redisCache) Put(ctx context.Context, key string, value []byte, ttl time.Duration) {
	ctx, cancel := lifecycle.Detach(ctx, c.opTimeout)
	defer cancel()
	if ttl <= 0 {
		ttl = c.maxTTL
	}
	if err := c.client.Set(ctx, c.keyPrefix+key, value, ttl).Err(); err != nil {
		slog.Default().Warn("fetchcache: redis SET failed",
			"key", key, "error", err.Error())
	}
}

// Delete removes the entry for key. No-op on cache miss; logs and
// returns on backend error (fail-open).
func (c *redisCache) Delete(ctx context.Context, key string) {
	ctx, cancel := lifecycle.Detach(ctx, c.opTimeout)
	defer cancel()
	if err := c.client.Del(ctx, c.keyPrefix+key).Err(); err != nil {
		slog.Default().Warn("fetchcache: redis DEL failed",
			"key", key, "error", err.Error())
	}
}

// TryLock takes the lock with `SET <key> <token> NX PX <ttl>` (go-redis sends
// EX for a whole-second ttl; same effect), where token is random per call. The returned release reads the key (GET) and deletes it
// (DEL) only while it still holds this call's token, so a holder whose lock
// already expired doesn't free the lock another replica took since.
//
// GET-then-DEL is two commands, not one atomic compare-and-delete: that needs
// Lua (EVAL), which production ACLs deny. The race this leaves: the lock
// expires between our GET and our DEL, another replica takes it, and our DEL
// removes the new holder's lock. That needs the holder to run to within a
// round-trip of its TTL, and the damage is bounded by the TTL: at worst a
// third replica may enter while the second still works, which is the
// unserialized behaviour the lock otherwise prevents. Callers keep their work
// well inside the TTL to keep the window shut.
//
// Acquiring honours the caller's context (bounded by the op timeout); release
// detaches from it, like Put and Delete, so a pipeline cancelled after taking
// the lock still frees it instead of blocking the others for the whole TTL.
func (c *redisCache) TryLock(ctx context.Context, key string, ttl time.Duration) (func(), bool, error) {
	if ttl <= 0 {
		return nil, false, fmt.Errorf("fetchcache: lock %q: ttl must be positive, got %s", key, ttl)
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, false, fmt.Errorf("fetchcache: lock %q: token: %w", key, err)
	}
	token := hex.EncodeToString(raw[:])
	full := c.keyPrefix + key

	opCtx, cancel := context.WithTimeout(ctx, c.opTimeout)
	defer cancel()
	ok, err := c.client.SetNX(opCtx, full, token, ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("fetchcache: lock %q: %w", key, err)
	}
	if !ok {
		return nil, false, nil
	}
	release := func() {
		ctx, cancel := lifecycle.Detach(ctx, c.opTimeout)
		defer cancel()
		cur, err := c.client.Get(ctx, full).Result()
		if errors.Is(err, redis.Nil) {
			return // expired; nothing to free
		}
		if err != nil {
			slog.Default().Warn("fetchcache: lock release GET failed; the lock lapses after its TTL",
				"key", key, "error", err.Error())
			return
		}
		if cur != token {
			return // expired and taken by another holder: theirs now
		}
		if err := c.client.Del(ctx, full).Err(); err != nil {
			slog.Default().Warn("fetchcache: lock release DEL failed; the lock lapses after its TTL",
				"key", key, "error", err.Error())
		}
	}
	return release, true, nil
}
