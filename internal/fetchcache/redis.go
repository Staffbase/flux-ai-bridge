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
	"errors"
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
