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

package dedupe_test

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
)

// Test the cache as a state machine: disabled → enabled with empty → entry
// present → entry forgotten → entry expired. Each transition is a method
// call; assertions go through the test-only peek (HasPR).

func TestDisabledCacheIsNoOp(t *testing.T) {
	t.Parallel()
	c := dedupe.New(0, 0)
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("disabled cache must report every event as new")
	}
	if c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("disabled cache must never have entries")
	}
	// Forget on a disabled cache must not panic.
	c.ForgetPR("ns", "k", 1, "sha")
}

func TestMarkIfNewIsAtomic(t *testing.T) {
	t.Parallel()
	c := dedupe.New(time.Minute, 16)
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("first call must be new")
	}
	if markNew(t, c, "ns", "k", "sha") {
		t.Fatal("second call must report duplicate")
	}
	if !c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("entry must be present after MarkPRIfNew")
	}
}

func TestForgetEvicts(t *testing.T) {
	t.Parallel()
	c := dedupe.New(time.Minute, 16)
	markNew(t, c, "ns", "k", "sha")
	c.ForgetPR("ns", "k", 1, "sha")
	if c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("entry must be gone after ForgetPR")
	}
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("MarkPRIfNew after ForgetPR must report new again")
	}
}

func TestMarkPRIfNew_Identity(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)
	first, err := c.MarkPRIfNew("ns", "k", 42, "head1", 0)
	if err != nil || !first {
		t.Fatalf("first MarkPRIfNew = (%v, %v), want (true, nil)", first, err)
	}
	// Same (ns, name, pr, head) → duplicate.
	if dup, _ := c.MarkPRIfNew("ns", "k", 42, "head1", 0); dup {
		t.Error("same PR head must be a duplicate")
	}
	// Same PR, new head → distinct.
	if isNew, _ := c.MarkPRIfNew("ns", "k", 42, "head2", 0); !isNew {
		t.Error("a new PR head must be a distinct key")
	}
	// Different PR number, same head → distinct.
	if isNew, _ := c.MarkPRIfNew("ns", "k", 43, "head1", 0); !isNew {
		t.Error("a different PR number must be a distinct key")
	}
	// ForgetPR re-opens the slot.
	c.ForgetPR("ns", "k", 42, "head1")
	if isNew, _ := c.MarkPRIfNew("ns", "k", 42, "head1", 0); !isNew {
		t.Error("MarkPRIfNew after ForgetPR must report new again")
	}
}

// TestPROutcome_RoundTrip pins the best-effort last-outcome side-channel used
// by re-eval-failed-only: record then read back per (ns, name, pr, head), with
// a miss for an unknown head.
func TestPROutcome_RoundTrip(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)

	if _, seen := c.LastPROutcome("ns", "k", 42, "head1"); seen {
		t.Error("unknown PR head must have no recorded outcome")
	}
	c.RecordPROutcome("ns", "k", 42, "head1", "success", 0)
	if got, seen := c.LastPROutcome("ns", "k", 42, "head1"); !seen || got != "success" {
		t.Errorf("LastPROutcome = (%q, %v), want (\"success\", true)", got, seen)
	}
	// Outcome is per-head: a new head has no record yet.
	if _, seen := c.LastPROutcome("ns", "k", 42, "head2"); seen {
		t.Error("a different head must not inherit the prior head's outcome")
	}
	// Overwrite (e.g. a failed deploy later turns healthy).
	c.RecordPROutcome("ns", "k", 42, "head1", "failure", 0)
	if got, _ := c.LastPROutcome("ns", "k", 42, "head1"); got != "failure" {
		t.Errorf("LastPROutcome after overwrite = %q, want \"failure\"", got)
	}
}

// The per-PR-head label side-channel: record scope outcomes, read them back as
// a map, and remove a scope (reporting whether it was present).
func TestScopeOutcomes_RoundTrip(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)

	if got := c.ScopeOutcomes("o", "r", 42, "h1"); len(got) != 0 {
		t.Errorf("unknown PR head must have no scope outcomes, got %v", got)
	}
	c.PutScopeOutcome("o", "r", 42, "h1", "ns/k1", "failure", 0)
	c.PutScopeOutcome("o", "r", 42, "h1", "ns/k2", "success", 0)
	got := c.ScopeOutcomes("o", "r", 42, "h1")
	if got["ns/k1"] != "failure" || got["ns/k2"] != "success" || len(got) != 2 {
		t.Errorf("scope outcomes = %v, want {ns/k1:failure, ns/k2:success}", got)
	}
	// A different head doesn't share state.
	if got := c.ScopeOutcomes("o", "r", 42, "h2"); len(got) != 0 {
		t.Errorf("a different head must not inherit scope outcomes, got %v", got)
	}
	// Removing a tracked scope reports true; an untracked one false.
	if ok, err := c.RemoveScopeOutcome("o", "r", 42, "h1", "ns/k1"); !ok || err != nil {
		t.Error("removing a tracked scope must report true")
	}
	if ok, err := c.RemoveScopeOutcome("o", "r", 42, "h1", "ns/nope"); ok || err != nil {
		t.Error("removing an untracked scope must report false")
	}
	if got := c.ScopeOutcomes("o", "r", 42, "h1"); len(got) != 1 || got["ns/k2"] != "success" {
		t.Errorf("after removing k1, want only k2; got %v", got)
	}
}

// The PR roster is head-INDEPENDENT: scopes added under any head are visible
// regardless of head, and removal works. This is what lets a new push hold
// flux-success until every previously-known scope reports again.
func TestPRRoster_RoundTrip(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)

	if got := c.PRScopes("o", "r", 42); len(got) != 0 {
		t.Errorf("unknown PR must have an empty roster, got %v", got)
	}
	c.AddPRScope("o", "r", 42, "ns/k1", 0)
	c.AddPRScope("o", "r", 42, "ns/k2", 0)
	c.AddPRScope("o", "r", 42, "ns/k1", 0) // idempotent
	got := map[string]bool{}
	for _, s := range c.PRScopes("o", "r", 42) {
		got[s] = true
	}
	if len(got) != 2 || !got["ns/k1"] || !got["ns/k2"] {
		t.Errorf("roster = %v, want {ns/k1, ns/k2}", got)
	}
	// Roster is not scoped to a head and not shared across PRs.
	if got := c.PRScopes("o", "r", 99); len(got) != 0 {
		t.Errorf("a different PR must not inherit the roster, got %v", got)
	}
	if !c.RemovePRScope("o", "r", 42, "ns/k1") {
		t.Error("removing a tracked roster scope must report true")
	}
	if c.RemovePRScope("o", "r", 42, "ns/nope") {
		t.Error("removing an untracked roster scope must report false")
	}
	if got := c.PRScopes("o", "r", 42); len(got) != 1 || got[0] != "ns/k2" {
		t.Errorf("after removing k1, roster = %v, want [ns/k2]", got)
	}
}

// A pending de-scope marker leaves the roster for every reader but stays
// visible to ReadPRScopeState until RemovePRScope drops it; re-attributing the
// scope replaces it. Both backends behave the same, and the Redis write
// reports a backend error so the caller can keep its retry state.
func TestPRRoster_DescopingMarker(t *testing.T) {
	t.Parallel()
	mem := dedupe.NewNoExpiry(16)
	rc, mr := newRedis(t)
	for name, c := range map[string]*dedupe.Cache{"memory": mem, "redis": rc} {
		c.AddPRScope("o", "r", 42, "ns/k1", time.Hour)
		c.AddPRScope("o", "r", 42, "ns/k2", time.Hour)
		if err := c.MarkPRScopeDescoping("o", "r", 42, "ns/k1", time.Hour); err != nil {
			t.Fatalf("%s: MarkPRScopeDescoping: %v", name, err)
		}
		if got := c.PRScopes("o", "r", 42); len(got) != 1 || got[0] != "ns/k2" {
			t.Errorf("%s: a de-scoping scope must leave the roster; got %v", name, got)
		}
		if listed, descoping, err := c.ReadPRScopeState("o", "r", 42, "ns/k1"); err != nil || listed || !descoping {
			t.Errorf("%s: state = listed %v, descoping %v, err %v; want descoping", name, listed, descoping, err)
		}
		c.AddPRScope("o", "r", 42, "ns/k1", time.Hour)
		if listed, descoping, _ := c.ReadPRScopeState("o", "r", 42, "ns/k1"); !listed || descoping {
			t.Errorf("%s: re-attributing must replace the marker; listed %v, descoping %v", name, listed, descoping)
		}
		_ = c.MarkPRScopeDescoping("o", "r", 42, "ns/k1", time.Hour)
		if !c.RemovePRScope("o", "r", 42, "ns/k1") {
			t.Errorf("%s: RemovePRScope must drop the marker", name)
		}
		if listed, descoping, _ := c.ReadPRScopeState("o", "r", 42, "ns/k1"); listed || descoping {
			t.Errorf("%s: marker still present after RemovePRScope", name)
		}
	}
	mr.SetError("LOADING")
	if err := rc.MarkPRScopeDescoping("o", "r", 42, "ns/k2", time.Hour); err == nil {
		t.Error("a failed marker write must return the backend error")
	}
	if _, _, err := rc.ReadPRScopeState("o", "r", 42, "ns/k2"); err == nil {
		t.Error("a failed roster read must return the backend error")
	}
}

// A nil/disabled cache must no-op the roster without panicking.
func TestPRRoster_NilSafe(t *testing.T) {
	t.Parallel()
	var c *dedupe.Cache
	c.AddPRScope("o", "r", 1, "ns/k", 0) // must not panic
	if got := c.PRScopes("o", "r", 1); got != nil {
		t.Errorf("nil cache roster must be nil, got %v", got)
	}
	if c.RemovePRScope("o", "r", 1, "ns/k") {
		t.Error("nil cache RemovePRScope must report false")
	}
}

// A nil/disabled cache must no-op the outcome side-channel without panicking.
func TestPROutcome_NilSafe(t *testing.T) {
	t.Parallel()
	var c *dedupe.Cache
	c.RecordPROutcome("ns", "k", 1, "h", "success", 0) // must not panic
	if _, seen := c.LastPROutcome("ns", "k", 1, "h"); seen {
		t.Error("nil cache must report no outcome")
	}
}

// TestMarkPRIfNew_TTLFloor pins the per-PR re-evaluation floor on the
// non-expirable in-memory cache: within the ttl the same PR head is a
// duplicate; once the ttl elapses the same head is re-admitted. Fake time
// (synctest) keeps a scheduler pause from expiring the floor before the
// duplicate check.
func TestMarkPRIfNew_TTLFloor(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := dedupe.NewNoExpiry(16)
		const ttl = 30 * time.Millisecond
		if isNew, _ := c.MarkPRIfNew("ns", "k", 7, "head", ttl); !isNew {
			t.Fatal("first mark must be new")
		}
		time.Sleep(ttl - time.Millisecond)
		if dup, _ := c.MarkPRIfNew("ns", "k", 7, "head", ttl); dup {
			t.Fatal("within the ttl floor the same head must be a duplicate")
		}
		time.Sleep(time.Millisecond)
		if isNew, _ := c.MarkPRIfNew("ns", "k", 7, "head", ttl); !isNew {
			t.Fatal("after the ttl floor elapses the same head must be re-admitted")
		}
	})
}

func TestKeyTupleSeparation(t *testing.T) {
	t.Parallel()
	c := dedupe.New(time.Minute, 16)
	markNew(t, c, "ns", "k", "sha1")
	if !markNew(t, c, "ns", "k", "sha2") {
		t.Error("different SHA must be a distinct key")
	}
	if !markNew(t, c, "ns", "other", "sha1") {
		t.Error("different name must be a distinct key")
	}
	if !markNew(t, c, "other", "k", "sha1") {
		t.Error("different namespace must be a distinct key")
	}
}

func TestTTLExpiry(t *testing.T) {
	t.Parallel()
	c := dedupe.New(20*time.Millisecond, 16)
	markNew(t, c, "ns", "k", "sha")
	time.Sleep(80 * time.Millisecond)
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("entry should have expired and report new")
	}
}

// NewNoExpiry is the default in main.go and is the property that lets the
// bridge honour "process once per SHA, never re-fire on Flux's periodic
// reconciles". Even after a sleep longer than any reasonable TTL, the
// entry must still be present.
func TestNoExpiryPersistsBeyondTypicalTTL(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("first call must be new")
	}
	time.Sleep(150 * time.Millisecond)
	if markNew(t, c, "ns", "k", "sha") {
		t.Fatal("no-expiry entry must NOT report as new after a sleep")
	}
	if !c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("entry should still be present")
	}
}

// Explicit Forget on a no-expiry cache still works — pipeline-error retry
// and PR finalize both depend on it.
func TestNoExpiryHonoursForget(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)
	markNew(t, c, "ns", "k", "sha")
	c.ForgetPR("ns", "k", 1, "sha")
	if c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("entry must be gone after ForgetPR")
	}
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("MarkPRIfNew after ForgetPR must report new again")
	}
}

// TestConcurrentMarkIfNew: with N goroutines racing to mark the same key,
// exactly one must win. This is the property the bare Get-then-Add pattern
// in the old webhook code did not have.
func TestConcurrentMarkIfNew(t *testing.T) {
	t.Parallel()
	c := dedupe.New(time.Minute, 16)
	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	winners := make(chan struct{}, goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if markNew(t, c, "ns", "k", "sha") {
				winners <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(winners)
	if got := len(winners); got != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", got)
	}
}

// IncrPRReEval counts per (ns, name, PR, head): independent heads don't share
// a budget, and each call bumps by exactly one. The zero return on a
// nil/disabled cache means "cap never reached" — pinned so a disabled dedupe
// can't accidentally park every PR.
func TestIncrPRReEval_InMemory(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)
	for want := 1; want <= 3; want++ {
		if got := c.IncrPRReEval("ns", "k", 42, "head1", 0); got != want {
			t.Fatalf("IncrPRReEval call %d = %d, want %d", want, got, want)
		}
	}
	if got := c.IncrPRReEval("ns", "k", 42, "head2", 0); got != 1 {
		t.Fatalf("a different head must start its own budget, got %d", got)
	}
	if got := c.IncrPRReEval("ns", "k", 43, "head1", 0); got != 1 {
		t.Fatalf("a different PR must start its own budget, got %d", got)
	}

	var nilCache *dedupe.Cache
	if got := nilCache.IncrPRReEval("ns", "k", 42, "head1", 0); got != 0 {
		t.Fatalf("nil cache must report 0 (cap never reached), got %d", got)
	}
}

// PRReEvalCount is the budget's pre-analysis peek: it must read the counter
// without incrementing or creating it.
func TestPRReEvalCount_PeeksWithoutTouching(t *testing.T) {
	t.Parallel()
	c := dedupe.NewNoExpiry(16)
	if got := c.PRReEvalCount("ns", "k", 42, "h1"); got != 0 {
		t.Fatalf("unseen head must peek 0, got %d", got)
	}
	c.IncrPRReEval("ns", "k", 42, "h1", 0)
	c.IncrPRReEval("ns", "k", 42, "h1", 0)
	for i := 0; i < 3; i++ {
		if got := c.PRReEvalCount("ns", "k", 42, "h1"); got != 2 {
			t.Fatalf("peek #%d = %d, want 2 (peeks must not increment)", i+1, got)
		}
	}
	var nilCache *dedupe.Cache
	if got := nilCache.PRReEvalCount("ns", "k", 42, "h1"); got != 0 {
		t.Fatalf("nil cache must peek 0, got %d", got)
	}
}

// In-memory ExpirePR mirrors the Redis EXPIRE: the mark dedupes until the
// cooldown lapses, re-admits once, and a cooldown never outlives its mark.
// Fake time (synctest) keeps a scheduler pause from lapsing the cooldown
// before the in-cooldown check.
func TestExpirePR_InMemoryCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := dedupe.NewNoExpiry(16)
		if isNew, _ := c.MarkPRIfNew("ns", "k", 7, "head", 0); !isNew {
			t.Fatal("first mark must be new")
		}
		c.ExpirePR("ns", "k", 7, "head", 30*time.Millisecond)
		time.Sleep(29 * time.Millisecond)
		if isNew, _ := c.MarkPRIfNew("ns", "k", 7, "head", 0); isNew {
			t.Fatal("mark must dedupe during the cooldown")
		}
		time.Sleep(time.Millisecond)
		if isNew, _ := c.MarkPRIfNew("ns", "k", 7, "head", 0); !isNew {
			t.Fatal("mark must re-admit once the cooldown lapsed")
		}
		if isNew, _ := c.MarkPRIfNew("ns", "k", 7, "head", 0); isNew {
			t.Fatal("the re-admitted mark must dedupe again (no cooldown left)")
		}

		// A cooldown on an absent mark is ignored, so it can't cut a later
		// fresh mark short.
		c.ExpirePR("ns", "k", 8, "head", time.Millisecond)
		if isNew, _ := c.MarkPRIfNew("ns", "k", 8, "head", 0); !isNew {
			t.Fatal("first mark must be new")
		}
		time.Sleep(5 * time.Millisecond)
		if isNew, _ := c.MarkPRIfNew("ns", "k", 8, "head", 0); isNew {
			t.Fatal("fresh mark must not inherit a cooldown set before it existed")
		}
		if !c.Available() {
			t.Fatal("in-memory cache is always available")
		}
	})
}

// With a TTL cache whose global ttl is shorter than a rate-limit cooldown,
// the cooldown still holds after the mark itself expired: the next event
// must not retry GitHub before the limit resets. Once the cooldown ends,
// the PR is admitted again. Real time, not synctest: the expirable LRU runs
// a cleanup goroutine that never exits.
func TestExpirePR_CooldownOutlivesTTL(t *testing.T) {
	t.Parallel()
	c := dedupe.New(20*time.Millisecond, 16)
	markPRNew(t, c, "ns", "k", 7, "head", 0)
	markPRNew(t, c, "ns", "k", 8, "head", 0)
	c.ExpirePR("ns", "k", 7, "head", time.Hour)
	c.ExpirePR("ns", "k", 8, "head", 40*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // past the ttl and PR 8's cooldown

	if markPRNew(t, c, "ns", "k", 7, "head", 0) {
		t.Fatal("PR re-admitted while its cooldown still runs, because the TTL dropped the mark")
	}
	if !markPRNew(t, c, "ns", "k", 8, "head", 0) {
		t.Fatal("PR must be admitted again once its cooldown ended")
	}
}
