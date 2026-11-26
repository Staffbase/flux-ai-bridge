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
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// memEntry pairs the cached bytes with an optional expiry. expiry.IsZero()
// means "no expiry" — entries with a zero expiry live until LRU evicts
// them. Pre-computing expiry at Put time avoids per-Get clock math when
// the TTL is zero, which is the common case for blob + commit caches.
type memEntry struct {
	value  []byte
	expiry time.Time
}

// memCache is the in-memory backend. Thread-safe via the LRU's internal
// locking; we add a mutex only to make Put atomic across the get-and-set
// path when the value expired. Capacity is a count of entries, not a
// byte budget — for blob caches that hold large entries an operator can
// reduce the count or move to Redis.
type memCache struct {
	mu  sync.Mutex
	lru *lru.Cache[string, memEntry]
}

// NewLRU builds an in-memory cache holding up to `capacity` entries.
// Capacity is enforced by hashicorp/golang-lru's standard eviction — the
// least-recently-used entry is dropped when the cache is full.
func NewLRU(capacity int) Cache {
	if capacity <= 0 {
		capacity = 4096
	}
	cache, _ := lru.New[string, memEntry](capacity)
	return &memCache{lru: cache}
}

// Get returns (nil, false) on miss OR expired entry (lazily evicted).
// The eager Remove on expiry keeps the cache from holding dead entries
// indefinitely — important when many short-TTL entries land and then
// stop being read.
func (c *memCache) Get(ctx context.Context, key string) ([]byte, bool) {
	entry, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	if !entry.expiry.IsZero() && time.Now().After(entry.expiry) {
		// Re-check under the mutex before evicting: a concurrent Put may
		// have refreshed the entry between the unlocked Get above and
		// here, and an unconditional Remove would delete the fresh value.
		c.mu.Lock()
		if cur, ok := c.lru.Peek(key); ok &&
			!cur.expiry.IsZero() && time.Now().After(cur.expiry) {
			c.lru.Remove(key)
		}
		c.mu.Unlock()
		return nil, false
	}
	return entry.value, true
}

// Put stores value under key. A zero ttl means no expiry. Existing
// entries are overwritten — useful when a TTL'd entry's true value
// changed and the caller wants to refresh.
func (c *memCache) Put(ctx context.Context, key string, value []byte, ttl time.Duration) {
	var expiry time.Time
	if ttl > 0 {
		expiry = time.Now().Add(ttl)
	}
	c.mu.Lock()
	c.lru.Add(key, memEntry{value: value, expiry: expiry})
	c.mu.Unlock()
}

// Delete removes the entry for key. No-op when key is absent. Never fails.
func (c *memCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	c.lru.Remove(key)
	c.mu.Unlock()
	return nil
}

// TryLock always grants the lock: an in-memory cache serves one process, and
// the caller's own mutex already serializes work within it (the in-memory
// backend is only used with a single replica; see docs/caching.md).
func (c *memCache) TryLock(context.Context, string, time.Duration) (func(), bool, error) {
	return noopRelease, true, nil
}
