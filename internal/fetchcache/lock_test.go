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

// twoReplicas returns two Redis-backed caches sharing one miniredis, standing
// for two bridge replicas on the same Valkey.
func twoReplicas(t *testing.T) (*miniredis.Miniredis, fetchcache.Cache, fetchcache.Cache) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr,
		fetchcache.NewRedis(client, "test:", 500*time.Millisecond, 0),
		fetchcache.NewRedis(client, "test:", 500*time.Millisecond, 0)
}

// While one replica holds the lock, another can't take it; once released it
// can. The key carries the lock TTL, so a crashed holder can't block forever.
func TestRedis_TryLock_SecondHolderBlockedUntilRelease(t *testing.T) {
	mr, a, b := twoReplicas(t)
	ctx := context.Background()

	releaseA, ok, err := a.TryLock(ctx, "lock:k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first TryLock = (ok %v, err %v), want (true, nil)", ok, err)
	}
	if got := mr.TTL("test:lock:k"); got != time.Minute {
		t.Errorf("lock key TTL = %s, want 1m", got)
	}
	if _, ok, err := b.TryLock(ctx, "lock:k", time.Minute); err != nil || ok {
		t.Fatalf("second TryLock while held = (ok %v, err %v), want (false, nil)", ok, err)
	}

	releaseA()
	if mr.Exists("test:lock:k") {
		t.Fatal("release did not delete the lock key")
	}
	releaseB, ok, err := b.TryLock(ctx, "lock:k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock after release = (ok %v, err %v), want (true, nil)", ok, err)
	}
	releaseB()
}

// A holder whose lock expired must not delete the lock another replica took
// since: release deletes the key only while it still holds this holder's token.
func TestRedis_TryLock_ExpiredLockNotReleasedByOldHolder(t *testing.T) {
	mr, a, b := twoReplicas(t)
	ctx := context.Background()

	releaseA, ok, err := a.TryLock(ctx, "lock:k", time.Second)
	if err != nil || !ok {
		t.Fatalf("first TryLock = (ok %v, err %v), want (true, nil)", ok, err)
	}
	mr.FastForward(2 * time.Second) // A's lock expires
	releaseB, ok, err := b.TryLock(ctx, "lock:k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock after expiry = (ok %v, err %v), want (true, nil)", ok, err)
	}

	releaseA() // late release by the old holder
	if !mr.Exists("test:lock:k") {
		t.Fatal("the old holder's release deleted the new holder's lock")
	}
	if _, ok, _ := a.TryLock(ctx, "lock:k", time.Minute); ok {
		t.Fatal("lock taken while the new holder still holds it")
	}

	releaseB()
	if mr.Exists("test:lock:k") {
		t.Fatal("the current holder's release did not delete its lock")
	}
}

// Release runs detached from the caller's context, like Put and Delete: a
// pipeline cancelled mid-reconcile must still free its lock instead of
// blocking the other replicas for the whole TTL.
func TestRedis_TryLock_ReleaseSurvivesCancelledContext(t *testing.T) {
	mr, a, _ := twoReplicas(t)
	ctx, cancel := context.WithCancel(context.Background())
	release, ok, err := a.TryLock(ctx, "lock:k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryLock = (ok %v, err %v), want (true, nil)", ok, err)
	}
	cancel()
	release()
	if mr.Exists("test:lock:k") {
		t.Fatal("release with a cancelled context did not delete the lock")
	}
}

// A backend error is reported as an error, not as "held by someone else", so
// the caller can tell contention from an outage.
func TestRedis_TryLock_BackendError(t *testing.T) {
	mr, a, _ := twoReplicas(t)
	mr.SetError("ERR boom")
	release, ok, err := a.TryLock(context.Background(), "lock:k", time.Minute)
	if err == nil || ok || release != nil {
		t.Fatalf("TryLock on a failing backend = (release %v, ok %v, err %v), want (nil, false, error)", release != nil, ok, err)
	}
}

func TestRedis_TryLock_RejectsNonPositiveTTL(t *testing.T) {
	mr, a, _ := twoReplicas(t)
	if _, ok, err := a.TryLock(context.Background(), "lock:k", 0); err == nil || ok {
		t.Fatalf("TryLock with ttl 0 = (ok %v, err %v), want (false, error)", ok, err)
	}
	if mr.Exists("test:lock:k") {
		t.Fatal("a rejected TryLock wrote a key without expiry")
	}
}

// The in-memory and disabled backends serve one process, whose own mutex
// already serializes the callers: TryLock always succeeds, even while held.
func TestInProcess_TryLock_AlwaysSucceeds(t *testing.T) {
	for name, c := range map[string]fetchcache.Cache{
		"lru":      fetchcache.NewLRU(16),
		"disabled": fetchcache.Disabled(),
	} {
		ctx := context.Background()
		release1, ok, err := c.TryLock(ctx, "lock:k", time.Minute)
		if err != nil || !ok || release1 == nil {
			t.Fatalf("%s: first TryLock = (ok %v, err %v), want (true, nil)", name, ok, err)
		}
		release2, ok, err := c.TryLock(ctx, "lock:k", time.Minute)
		if err != nil || !ok || release2 == nil {
			t.Fatalf("%s: second TryLock = (ok %v, err %v), want (true, nil)", name, ok, err)
		}
		release1()
		release2()
	}
}
