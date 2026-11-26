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

package github_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/staffbase/flux-ai-bridge/internal/fetchcache"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
)

const (
	labelLockKey  = "fc:pr-outcome:owner/repo/42:lock"
	labelOutcome  = "fc:pr-outcome:owner/repo/42"
	labelLockName = "pr-outcome:owner/repo/42:lock"
)

// labelReplicas returns a miniredis and two Redis-backed fetchcaches on it,
// standing for two bridge replicas sharing one Valkey.
func labelReplicas(t *testing.T) (*miniredis.Miniredis, fetchcache.Cache, fetchcache.Cache) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr,
		fetchcache.NewRedis(client, "fc:", time.Second, 0),
		fetchcache.NewRedis(client, "fc:", time.Second, 0)
}

// labelCallMux counts the label writes on PR 42 and runs onWrite on each.
func labelCallMux(calls *atomic.Int32, onWrite func()) *http.ServeMux {
	mux := http.NewServeMux()
	handle := func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if onWrite != nil {
			onWrite()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{})
	}
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", handle)
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", handle)
	return mux
}

// Two replicas reconciling the same PR's labels at once could interleave
// their add/remove calls and leave conflicting labels. While another replica
// holds the PR's label lock, this one skips: no label write, no outcome
// record (so the next event reconciles), and an error that tells the caller
// the labels weren't reconciled.
func TestSetPRLabels_SkipsWhileAnotherReplicaHoldsTheLock(t *testing.T) {
	mr, replicaA, replicaB := labelReplicas(t)
	ctx := context.Background()

	releaseB, ok, err := replicaB.TryLock(ctx, labelLockName, time.Minute)
	if err != nil || !ok {
		t.Fatalf("replica B TryLock = (ok %v, err %v)", ok, err)
	}

	var calls atomic.Int32
	c := newCommenterWithOptions(t, labelCallMux(&calls, nil), bridgegithub.Options{Cache: replicaA})
	err = c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure)
	if !errors.Is(err, bridgegithub.ErrPRLabelsLocked) {
		t.Fatalf("SetPRLabels while locked: err = %v, want ErrPRLabelsLocked", err)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("label writes while another replica holds the lock = %d, want 0", n)
	}
	if mr.Exists(labelOutcome) {
		t.Error("outcome record written for a skipped reconcile; the next event would skip it")
	}
	if !mr.Exists(labelLockKey) {
		t.Error("the skipped reconcile freed the other replica's lock")
	}

	releaseB()
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure); err != nil {
		t.Fatalf("SetPRLabels after release: %v", err)
	}
	if calls.Load() == 0 {
		t.Error("no label writes once the lock was free")
	}
	if !mr.Exists(labelOutcome) {
		t.Error("outcome not recorded after a reconcile under the lock")
	}
	if mr.Exists(labelLockKey) {
		t.Error("the lock was not released after the reconcile")
	}
}

// The lock covers the label writes themselves, not just the outcome check.
func TestSetPRLabels_HoldsTheLockWhileWriting(t *testing.T) {
	mr, replicaA, _ := labelReplicas(t)
	var calls, unlocked atomic.Int32
	mux := labelCallMux(&calls, func() {
		if !mr.Exists(labelLockKey) {
			unlocked.Add(1)
		}
	})
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: replicaA})
	if err := c.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("SetPRLabels: %v", err)
	}
	if calls.Load() == 0 {
		t.Fatal("no label writes")
	}
	if n := unlocked.Load(); n != 0 {
		t.Errorf("%d label writes ran without the PR's label lock held", n)
	}
}

// An unchanged outcome is a no-op even while another replica holds the lock:
// it needs no label write, so contention must not turn it into a retry.
func TestSetPRLabels_UnchangedOutcomeIgnoresTheLock(t *testing.T) {
	_, replicaA, replicaB := labelReplicas(t)
	ctx := context.Background()
	var calls atomic.Int32
	c := newCommenterWithOptions(t, labelCallMux(&calls, nil), bridgegithub.Options{Cache: replicaA})
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("first SetPRLabels: %v", err)
	}
	before := calls.Load()

	releaseB, ok, err := replicaB.TryLock(ctx, labelLockName, time.Minute)
	if err != nil || !ok {
		t.Fatalf("replica B TryLock = (ok %v, err %v)", ok, err)
	}
	defer releaseB()
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("unchanged SetPRLabels while locked: %v", err)
	}
	if calls.Load() != before {
		t.Error("an unchanged outcome wrote labels")
	}
}

// lockErrCache fails every TryLock and serves the rest from memory.
type lockErrCache struct{ fetchcache.Cache }

func (lockErrCache) TryLock(context.Context, string, time.Duration) (func(), bool, error) {
	return nil, false, errors.New("lock backend unavailable")
}

// A lock that can't be asked for (the backend fails) doesn't let the
// reconcile run unserialized: two replicas both failing to lock could
// interleave their add/remove calls and leave neither outcome label, while
// their outcome records vouch for one. No label changes, no record is
// written, and the error makes the event retry.
func TestSetPRLabels_LockBackendErrorWritesNoLabels(t *testing.T) {
	var calls atomic.Int32
	store := fetchcache.NewLRU(64)
	c := newCommenterWithOptions(t, labelCallMux(&calls, nil), bridgegithub.Options{Cache: lockErrCache{store}})
	err := c.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure)
	if err == nil {
		t.Fatal("SetPRLabels with a failing lock backend returned nil; want an error so the event retries")
	}
	if errors.Is(err, bridgegithub.ErrPRLabelsLocked) {
		t.Errorf("err = %v; a backend failure must not read as a lock held by another replica", err)
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("label writes with a failing lock backend = %d, want 0", n)
	}
	if _, hit := store.Get(context.Background(), "pr-outcome:owner/repo/42"); hit {
		t.Error("outcome record written although no label was reconciled")
	}
}

// With the whole backend down, the outcome record can't be dropped, so no
// label changes: the record would otherwise outlive the reconcile and later
// vouch for labels the PR no longer has. The error makes the event retry.
func TestSetPRLabels_BackendDownWritesNoLabels(t *testing.T) {
	mr, replicaA, _ := labelReplicas(t)
	mr.SetError("ERR boom")
	var calls atomic.Int32
	c := newCommenterWithOptions(t, labelCallMux(&calls, nil), bridgegithub.Options{Cache: replicaA})
	if err := c.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure); err == nil {
		t.Fatal("SetPRLabels with the backend down returned nil; want an error so the event retries")
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("label writes with the backend down = %d, want 0", n)
	}
}
