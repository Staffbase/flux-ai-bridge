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

package fetchcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/staffbase/flux-ai-bridge/internal/fetchcache"
)

// TestLRU_RoundTrip pins the in-memory backend's basic contract:
//   - Put then Get returns the same bytes
//   - Get on an unknown key returns (nil, false)
//   - Put overwrites existing entries
func TestLRU_RoundTrip(t *testing.T) {
	c := fetchcache.NewLRU(16)
	ctx := context.Background()

	if v, ok := c.Get(ctx, "missing"); ok || v != nil {
		t.Fatalf("Get(missing) = (%v, %v), want (nil, false)", v, ok)
	}

	c.Put(ctx, "k", []byte("v1"), 0)
	if v, ok := c.Get(ctx, "k"); !ok || string(v) != "v1" {
		t.Fatalf("Get after Put = (%q, %v), want (v1, true)", v, ok)
	}

	c.Put(ctx, "k", []byte("v2"), 0)
	if v, ok := c.Get(ctx, "k"); !ok || string(v) != "v2" {
		t.Fatalf("Get after overwrite = (%q, %v), want (v2, true)", v, ok)
	}
}

// TestLRU_TTLExpiry verifies the TTL'd cache surface that the
// PR-for-commit cache depends on. Entries with a positive TTL must
// disappear from Get after that TTL has elapsed.
func TestLRU_TTLExpiry(t *testing.T) {
	c := fetchcache.NewLRU(16)
	ctx := context.Background()
	c.Put(ctx, "ttl-key", []byte("v"), 50*time.Millisecond)
	if _, ok := c.Get(ctx, "ttl-key"); !ok {
		t.Fatal("entry must be present immediately after Put")
	}
	time.Sleep(80 * time.Millisecond)
	if v, ok := c.Get(ctx, "ttl-key"); ok {
		t.Fatalf("entry must expire after TTL; got %q", v)
	}
}

// TestLRU_ZeroTTLNoExpiry confirms that ttl=0 means "never expires" —
// this is the blob + commit cache's default since their entries are
// truly immutable (Git content-addressed).
func TestLRU_ZeroTTLNoExpiry(t *testing.T) {
	c := fetchcache.NewLRU(16)
	ctx := context.Background()
	c.Put(ctx, "permanent", []byte("v"), 0)
	time.Sleep(50 * time.Millisecond)
	if _, ok := c.Get(ctx, "permanent"); !ok {
		t.Fatal("entry with ttl=0 must NOT expire on time elapse")
	}
}

// TestLRU_CapacityEviction confirms the LRU eviction kicks in when the
// capacity is exceeded. Tests use capacity=2 and insert 3 entries; the
// oldest must be evicted.
func TestLRU_CapacityEviction(t *testing.T) {
	c := fetchcache.NewLRU(2)
	ctx := context.Background()
	c.Put(ctx, "a", []byte("1"), 0)
	c.Put(ctx, "b", []byte("2"), 0)
	c.Put(ctx, "c", []byte("3"), 0)
	if _, ok := c.Get(ctx, "a"); ok {
		t.Error("entry 'a' must be evicted at capacity=2 after inserting a 3rd entry")
	}
	if _, ok := c.Get(ctx, "b"); !ok {
		t.Error("entry 'b' must remain in cache")
	}
	if _, ok := c.Get(ctx, "c"); !ok {
		t.Error("entry 'c' must remain in cache")
	}
}

// TestDisabled_AlwaysMisses asserts the no-op backend the commenter
// falls back to when no cache is wired. Every Get returns miss; every
// Put and Delete silently succeeds. Callers can use it interchangeably
// with a real cache without nil checks.
func TestDisabled_AlwaysMisses(t *testing.T) {
	c := fetchcache.Disabled()
	ctx := context.Background()
	c.Put(ctx, "k", []byte("v"), time.Hour)
	if v, ok := c.Get(ctx, "k"); ok || v != nil {
		t.Fatalf("Disabled.Get = (%v, %v), want (nil, false)", v, ok)
	}
	if err := c.Delete(ctx, "k"); err != nil { // must not panic
		t.Fatalf("Delete: %v", err)
	}
}

// TestLRU_Delete pins Delete's contract: after Delete, Get returns
// (nil, false). Used by the commenter when a fast-path delete by
// cached ID fails — leaving the stale entry in the cache would replay
// the same failure on every subsequent event.
func TestLRU_Delete(t *testing.T) {
	c := fetchcache.NewLRU(16)
	ctx := context.Background()
	c.Put(ctx, "k", []byte("v"), 0)
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if v, ok := c.Get(ctx, "k"); ok || v != nil {
		t.Fatalf("Get after Delete = (%q, %v), want (nil, false)", v, ok)
	}
	if err := c.Delete(ctx, "missing"); err != nil { // must not panic on absent key
		t.Fatalf("Delete: %v", err)
	}
}

// TestRedis_Delete pins the same contract on the Redis backend so
// shared-Redis deployments don't accumulate tombstone entries.
func TestRedis_Delete(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	c := fetchcache.NewRedis(client, "test:", 500*time.Millisecond, 0)
	ctx := context.Background()
	c.Put(ctx, "k", []byte("v"), 0)
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if v, ok := c.Get(ctx, "k"); ok || v != nil {
		t.Fatalf("Get after Delete = (%q, %v), want (nil, false)", v, ok)
	}
	if err := c.Delete(ctx, "missing"); err != nil { // must not panic on absent key
		t.Fatalf("Delete: %v", err)
	}
}

// A Delete the backend refused must say so: the commenter drops its comment
// and label records with it before a GitHub write, and a record that silently
// survived could later vouch for a state the PR no longer has.
func TestRedis_DeleteReportsBackendError(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	c := fetchcache.NewRedis(client, "test:", 500*time.Millisecond, 0)
	ctx := context.Background()
	c.Put(ctx, "k", []byte("v"), 0)
	if err := c.Delete(ctx, "missing"); err != nil {
		t.Fatalf("Delete of an absent key = %v, want nil", err)
	}
	mr.SetError("ERR boom")
	if err := c.Delete(ctx, "k"); err == nil {
		t.Fatal("Delete with a failing backend returned nil; the entry is still there")
	}
}

// TestRedis_RoundTrip verifies the Redis backend against miniredis (in-
// process RESP server). Same Put/Get contract as the LRU backend.
func TestRedis_RoundTrip(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	c := fetchcache.NewRedis(client, "test:", 500*time.Millisecond, 0)
	ctx := context.Background()

	if v, ok := c.Get(ctx, "missing"); ok || v != nil {
		t.Fatalf("Get(missing) = (%v, %v), want (nil, false)", v, ok)
	}

	c.Put(ctx, "k", []byte("hello"), 0)
	if v, ok := c.Get(ctx, "k"); !ok || string(v) != "hello" {
		t.Fatalf("Get = (%q, %v), want (hello, true)", v, ok)
	}
}

// TestRedis_KeyPrefix proves the prefix scopes the keyspace — two
// fetchcache instances with different prefixes against the same Redis
// don't see each other's entries. This is the load-bearing property
// that lets fetchcache share a Redis instance with the dedupe cache.
func TestRedis_KeyPrefix(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	a := fetchcache.NewRedis(client, "a:", 500*time.Millisecond, 0)
	b := fetchcache.NewRedis(client, "b:", 500*time.Millisecond, 0)
	ctx := context.Background()

	a.Put(ctx, "shared-key", []byte("a-value"), 0)
	b.Put(ctx, "shared-key", []byte("b-value"), 0)

	if v, ok := a.Get(ctx, "shared-key"); !ok || string(v) != "a-value" {
		t.Errorf("cache A read = (%q, %v), want (a-value, true)", v, ok)
	}
	if v, ok := b.Get(ctx, "shared-key"); !ok || string(v) != "b-value" {
		t.Errorf("cache B read = (%q, %v), want (b-value, true)", v, ok)
	}
}

// TestRedis_TTLExpiry covers the TTL'd path on the Redis backend.
// miniredis simulates SET EX semantics; after the TTL passes, GET
// returns nil (cache miss).
func TestRedis_TTLExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	c := fetchcache.NewRedis(client, "test:", 500*time.Millisecond, 0)
	ctx := context.Background()
	c.Put(ctx, "ttl-key", []byte("v"), 100*time.Millisecond)
	if _, ok := c.Get(ctx, "ttl-key"); !ok {
		t.Fatal("entry must be readable immediately after Put with TTL")
	}
	// miniredis exposes FastForward to advance virtual time deterministically
	// rather than relying on wall-clock sleep.
	mr.FastForward(200 * time.Millisecond)
	if v, ok := c.Get(ctx, "ttl-key"); ok {
		t.Fatalf("entry must expire after TTL; got %q", v)
	}
}

// TestRedis_UnreachableFailsOpen pins the fail-open contract: when
// Redis is unreachable, Get returns (nil, false) so callers refetch
// upstream. The bridge MUST NOT crash on a Redis outage; caching is a
// perf optimization, not a correctness primitive.
func TestRedis_UnreachableFailsOpen(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // refused
		DialTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })

	c := fetchcache.NewRedis(client, "test:", 100*time.Millisecond, 0)
	ctx := context.Background()

	if v, ok := c.Get(ctx, "anything"); ok || v != nil {
		t.Fatalf("unreachable Redis Get = (%v, %v), want (nil, false)", v, ok)
	}
	// Put must also fail-open (no panic, no error surfaced).
	c.Put(ctx, "anything", []byte("v"), 0)
}

// TestRedis_MaxTTLBackstop pins the backstop: a Put asking for no expiry
// gets maxTTL, so immutable/bookkeeping entries can't pile up in Redis
// forever, while an explicit TTL is left alone.
func TestRedis_MaxTTLBackstop(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	c := fetchcache.NewRedis(client, "test:", 500*time.Millisecond, time.Hour)
	ctx := context.Background()
	c.Put(ctx, "forever", []byte("v"), 0)
	c.Put(ctx, "short", []byte("v"), time.Minute)
	if got := mr.TTL("test:forever"); got != time.Hour {
		t.Errorf("TTL of a no-expiry Put = %s, want the 1h backstop", got)
	}
	if got := mr.TTL("test:short"); got != time.Minute {
		t.Errorf("TTL of an explicit-TTL Put = %s, want 1m unchanged", got)
	}

	noBackstop := fetchcache.NewRedis(client, "nb:", 500*time.Millisecond, 0)
	noBackstop.Put(ctx, "forever", []byte("v"), 0)
	if got := mr.TTL("nb:forever"); got != 0 {
		t.Errorf("TTL with backstop disabled = %s, want none", got)
	}
}

// TestRedis_WritesSurviveCancelledContext pins that Put and Delete still
// reach Redis after the caller's context is cancelled: they record GitHub
// writes that already happened (a new comment ID, a deleted comment), so
// dropping them on cancellation leaves the cache pointing at the wrong
// comment.
func TestRedis_WritesSurviveCancelledContext(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	c := fetchcache.NewRedis(client, "test:", 500*time.Millisecond, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Put(ctx, "k", []byte("v"), 0)
	if !mr.Exists("test:k") {
		t.Fatal("Put with a cancelled context did not write")
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if mr.Exists("test:k") {
		t.Fatal("Delete with a cancelled context did not delete")
	}
}
