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
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
)

// Wire-protocol-compatible in-memory Redis. Lets us exercise the real
// go-redis client path (SETNX/EXISTS/DEL) without standing up a real
// server in the test environment.
func newRedis(t *testing.T) (*dedupe.Cache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	return c, mr
}

// Same property as the in-memory MarkPRIfNew test: first call wins, second
// returns false. Verifies that SETNX is hooked up correctly.
func TestRedis_MarkIfNewAtomic(t *testing.T) {
	t.Parallel()
	c, _ := newRedis(t)
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("first call must be new")
	}
	if markNew(t, c, "ns", "k", "sha") {
		t.Fatal("second call must dedupe")
	}
	if !c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("entry must be present after MarkPRIfNew")
	}
}

// Forget evicts the entry; subsequent MarkPRIfNew returns new again.
// Critical for the pipeline-error retry path.
func TestRedis_ForgetEvicts(t *testing.T) {
	t.Parallel()
	c, _ := newRedis(t)
	markNew(t, c, "ns", "k", "sha")
	c.ForgetPR("ns", "k", 1, "sha")
	if c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("entry must be gone after ForgetPR")
	}
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("MarkPRIfNew after ForgetPR must report new again")
	}
}

// State must survive what a process restart looks like — close and rebuild
// the client against the same backing store. This is the property that
// "comments aren't rewritten after a bridge restart" depends on.
func TestRedis_StatePersistsAcrossClientReconnect(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	addr := "redis://" + mr.Addr()

	// First "bridge instance" marks the SHA.
	first, err := dedupe.NewRedis(addr, "", "", "", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis #1: %v", err)
	}
	if !markNew(t, first, "ns", "k", "sha") {
		t.Fatal("first bridge: MarkPRIfNew must report new")
	}

	// Second "bridge instance" (post-restart) reconnects and sees the SHA.
	second, err := dedupe.NewRedis(addr, "", "", "", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis #2: %v", err)
	}
	if !second.HasPR("ns", "k", 1, "sha") {
		t.Fatal("post-restart bridge: HasPR must still see the SHA")
	}
	if markNew(t, second, "ns", "k", "sha") {
		t.Fatal("post-restart bridge: MarkPRIfNew must dedupe, not run pipeline")
	}
}

// SETNX is atomic across concurrent callers: with N goroutines racing to
// mark the same key, exactly one must "win". Same property the in-memory
// path proves with a mutex; here go-redis + miniredis provide it via the
// Redis protocol.
func TestRedis_ConcurrentMarkIfNew(t *testing.T) {
	t.Parallel()
	c, _ := newRedis(t)
	const goroutines = 32
	results := make(chan bool, goroutines)
	for range goroutines {
		go func() { results <- markNew(t, c, "ns", "k", "sha") }()
	}
	wins := 0
	for range goroutines {
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("expected exactly 1 winner across %d goroutines, got %d", goroutines, wins)
	}
}

// Fail-closed: when the Redis server is unreachable mid-operation,
// MarkPRIfNew now returns (false, err) so the webhook handler can detect
// the unhealthy dedupe and skip the event entirely — fail-open was the
// previous semantics but produced a much worse mode in practice (every
// Flux re-fire would re-run the LLM and repost the same comment because
// the in-process state doesn't survive across replicas, so dedupe had
// no effect during a Redis outage). Skipping > repeating.
func TestRedis_UnreachableServerFailsClosed(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	// Baseline: MarkPRIfNew works while the server is up.
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("baseline MarkPRIfNew must succeed")
	}

	// Kill the server mid-test to simulate a Valkey outage.
	mr.Close()

	// MarkPRIfNew against the dead server must return an error AND a
	// non-true bool — the caller (webhook handler) treats err != nil as
	// "dedupe unhealthy, fail closed".
	isNew, err := c.MarkPRIfNew("ns", "k", 1, "different-sha", 0)
	if err == nil {
		t.Fatal("MarkPRIfNew against dead server must surface the error (fail-closed contract)")
	}
	if isNew {
		t.Fatal("MarkPRIfNew against dead server must return false (no admission) when err != nil")
	}
	// HasPR continues to return false on a dead server — peek path stays
	// fail-open because it's only used for tests/observability and a
	// stale "false" reading is harmless.
	if c.HasPR("ns", "k", 1, "sha") {
		t.Fatal("HasPR against dead server must return false")
	}
}

// Sanity: the constructor reports a hard parse error rather than swallowing
// it (a typo'd DEDUPE_PERSISTENCE_URL must be visible at startup, not just
// at first event).
func TestRedis_BadURLErrors(t *testing.T) {
	t.Parallel()
	if _, err := dedupe.NewRedis("not a url", "", "", "", 0); err == nil {
		t.Fatal("expected parse error for invalid URL")
	}
}

// Password from constructor args is sent on AUTH and lets the client
// pass a server that's configured with RequireAuth. miniredis doesn't
// implement Redis 6 ACL users, so username goes untested here; sentinel
// + production exercise that path.
func TestRedis_PasswordAccepted(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	mr.RequireAuth("s3cret")
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "s3cret", "", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis with creds: %v", err)
	}
	if !markNew(t, c, "ns", "k", "sha") {
		t.Fatal("MarkPRIfNew with matching password must succeed")
	}
}

// Wrong password → fail-closed contract: MarkPRIfNew surfaces the auth
// error rather than silently degrading. The webhook handler treats this
// as "dedupe unhealthy" and skips the event instead of running the LLM
// + posting comments without cross-replica coordination.
func TestRedis_WrongPasswordFailsClosed(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	mr.RequireAuth("expected")
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "wrong", "", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis constructor returned err: %v", err) // construction itself succeeds; Ping warn-logs
	}
	isNew, err := c.MarkPRIfNew("ns", "k", 1, "sha", 0)
	if err == nil {
		t.Fatal("MarkPRIfNew with wrong password must surface the auth error (fail-closed)")
	}
	if isNew {
		t.Fatal("MarkPRIfNew must return false alongside the error so the webhook handler doesn't double-admit")
	}
}

// NewRedisSentinel rejects empty MasterName / empty Addrs at construction so
// misconfigurations surface at bridge boot rather than at the first event.
// miniredis can't simulate a Sentinel quorum, so the happy-path Sentinel
// connection is exercised by the e2e suite; here we only pin the input-
// validation contract that operators rely on for fail-fast deploys.
func TestRedisSentinel_RejectsEmptyConfig(t *testing.T) {
	t.Parallel()
	if _, err := dedupe.NewRedisSentinel(dedupe.SentinelOptions{Addrs: []string{"s1:26379"}}); err == nil {
		t.Fatal("NewRedisSentinel must reject empty MasterName")
	}
	if _, err := dedupe.NewRedisSentinel(dedupe.SentinelOptions{MasterName: "mymaster"}); err == nil {
		t.Fatal("NewRedisSentinel must reject empty Addrs")
	}
}

// prKey mirrors redisStore.keyOf for the per-PR identity, using an explicit
// keyPrefix passed to NewRedis so the tests can seed/inspect raw entries the
// way a pre-DEDUPE_PR_REEVAL_TTL bridge wrote them. Changing the key layout
// in keyOf intentionally breaks these tests.
func prKey(prefix, ns, name string, pr int, sha string) string {
	return prefix + ns + ":" + name + ":pr" + strconv.Itoa(pr) + ":" + sha
}

// A mark written while the re-eval floor was UNSET has no Redis expiry, and
// SETNX never updates an existing key's TTL — so before this migration path
// existed, enabling DEDUPE_PR_REEVAL_TTL left every already-analysed PR-head
// deduped forever. Pin the fix: the first duplicate event attaches the floor
// (EXPIRE NX) to the persistent mark, the mark self-frees after one window,
// and the next event re-admits the PR-head with a normally-expiring mark.
func TestRedis_ReEvalFloorAdoptsPreFloorMark(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	key := prKey("p:", "ns", "k", 7, "head")
	// Seed what a pre-floor bridge wrote: RFC3339Nano timestamp, no expiry.
	seedMark(t, mr, key, time.Now().Add(-time.Hour).Format(time.RFC3339Nano))

	// The mark still dedupes this event, but the floor is now attached.
	if markPRNew(t, c, "ns", "k", 7, "head", 15*time.Minute) {
		t.Fatal("pre-floor mark must still dedupe the event that adopts it")
	}
	if mr.TTL(key) <= 0 {
		t.Fatal("pre-floor mark must carry the re-eval expiry after its next event")
	}
	// One window later the mark has expired and the next event re-admits.
	mr.FastForward(15*time.Minute + time.Second)
	if !markPRNew(t, c, "ns", "k", 7, "head", 15*time.Minute) {
		t.Fatal("expired pre-floor mark must be re-admitted")
	}
	if mr.TTL(key) <= 0 {
		t.Fatal("re-admitted mark must expire normally, not regress to persistent")
	}
}

// EXPIRE NX must not touch marks that already carry an expiry: a duplicate
// event halfway through the window must NOT push the re-eval out by resetting
// the countdown (Flux re-fires every few minutes — a refreshed TTL would mean
// the floor never elapses on a busy Kustomization).
func TestRedis_ReEvalFloorDoesNotRefreshExistingExpiry(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	key := prKey("p:", "ns", "k", 7, "head")
	if !markPRNew(t, c, "ns", "k", 7, "head", 15*time.Minute) {
		t.Fatal("first call must be new")
	}
	mr.FastForward(10 * time.Minute)
	if markPRNew(t, c, "ns", "k", 7, "head", 15*time.Minute) {
		t.Fatal("duplicate inside the window must dedupe")
	}
	if ttl := mr.TTL(key); ttl > 5*time.Minute {
		t.Fatalf("duplicate event must not reset the countdown: remaining ttl = %v, want <= 5m", ttl)
	}
}

// With the floor disabled (ttl 0) the migration path must stay dormant: a
// pre-floor persistent mark keeps deduping and gains no expiry.
func TestRedis_NoFloorLeavesPreFloorMarkPersistent(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	key := prKey("p:", "ns", "k", 7, "head")
	seedMark(t, mr, key, time.Now().Add(-time.Hour).Format(time.RFC3339Nano))

	if markPRNew(t, c, "ns", "k", 7, "head", 0) {
		t.Fatal("persistent mark must dedupe when the floor is off")
	}
	if mr.TTL(key) != 0 {
		t.Fatalf("floor off must not attach an expiry, got ttl %v", mr.TTL(key))
	}
}

// Replicas racing over the same pre-floor mark: EXPIRE NX is idempotent, so
// nobody is admitted while the mark stands, and after it expires the SETNX
// race has exactly one winner — no duplicate-analysis storm at either stage.
func TestRedis_ConcurrentPreFloorAdoption(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", time.Second)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	key := prKey("p:", "ns", "k", 7, "head")
	seedMark(t, mr, key, time.Now().Add(-time.Hour).Format(time.RFC3339Nano))

	const goroutines = 32
	race := func() int {
		results := make(chan bool, goroutines)
		for range goroutines {
			go func() {
				isNew, err := c.MarkPRIfNew("ns", "k", 7, "head", 15*time.Minute)
				if err != nil {
					t.Errorf("MarkPRIfNew: %v", err)
				}
				results <- isNew
			}()
		}
		wins := 0
		for range goroutines {
			if <-results {
				wins++
			}
		}
		return wins
	}

	if wins := race(); wins != 0 {
		t.Fatalf("standing pre-floor mark must admit nobody, got %d winners", wins)
	}
	if mr.TTL(key) <= 0 {
		t.Fatal("racing adopters must have attached the expiry")
	}
	mr.FastForward(15*time.Minute + time.Second)
	if wins := race(); wins != 1 {
		t.Fatalf("expected exactly 1 winner after the mark expired, got %d", wins)
	}
}

// seedMark writes a raw admission entry the way a pre-floor bridge (or a
// foreign writer) would have, bypassing the Cache API.
func seedMark(t *testing.T, mr *miniredis.Miniredis, key, val string) {
	t.Helper()
	if err := mr.Set(key, val); err != nil {
		t.Fatalf("miniredis Set(%s): %v", key, err)
	}
}

// The Redis-backed re-eval counter increments atomically per head and carries
// a TTL that is refreshed on every call — including calls made while the cap
// is already exceeded — so an actively re-firing head can't shed its spent
// budget by letting the counter expire.
func TestRedis_IncrPRReEvalCountsAndRefreshesTTL(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	key := prKey("p:", "ns", "k", 7, "head") + ":reevals"

	if got := c.IncrPRReEval("ns", "k", 7, "head", 30*time.Minute); got != 1 {
		t.Fatalf("first incr = %d, want 1", got)
	}
	if mr.TTL(key) <= 0 {
		t.Fatal("counter must carry a TTL so quiet heads self-clean")
	}
	mr.FastForward(10 * time.Minute)
	if got := c.IncrPRReEval("ns", "k", 7, "head", 30*time.Minute); got != 2 {
		t.Fatalf("second incr = %d, want 2", got)
	}
	if ttl := mr.TTL(key); ttl <= 20*time.Minute {
		t.Fatalf("each incr must refresh the counter TTL, remaining = %v", ttl)
	}
	// A quiet head's counter expires; a revived head starts a fresh budget.
	mr.FastForward(31 * time.Minute)
	if got := c.IncrPRReEval("ns", "k", 7, "head", 30*time.Minute); got != 1 {
		t.Fatalf("expired counter must restart at 1, got %d", got)
	}
}

// Redis-backed peek: GET without INCR, 0 on missing key.
func TestRedis_PRReEvalCountPeek(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	if got := c.PRReEvalCount("ns", "k", 7, "head"); got != 0 {
		t.Fatalf("missing counter must peek 0, got %d", got)
	}
	c.IncrPRReEval("ns", "k", 7, "head", 30*time.Minute)
	if got := c.PRReEvalCount("ns", "k", 7, "head"); got != 1 {
		t.Fatalf("peek = %d, want 1", got)
	}
	if got := c.PRReEvalCount("ns", "k", 7, "head"); got != 1 {
		t.Fatalf("second peek = %d, want 1 (no increment)", got)
	}
}

// The WithMaxTTL backstop gives every "keep forever" record an expiry — new
// admission marks without a re-eval floor, marks written before the backstop
// existed (attached via EXPIRE NX on their next hit), outcome and label
// records — while an explicit TTL wins.
func TestRedis_MaxTTLBackstop(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	c.WithMaxTTL(time.Hour)

	if !markPRNew(t, c, "ns", "k", 1, "head", 0) {
		t.Fatal("first mark must be new")
	}
	if got := mr.TTL(prKey("p:", "ns", "k", 1, "head")); got != time.Hour {
		t.Errorf("no-floor mark TTL = %s, want the 1h backstop", got)
	}
	if !markPRNew(t, c, "ns", "k", 2, "head", 5*time.Minute) {
		t.Fatal("first mark must be new")
	}
	if got := mr.TTL(prKey("p:", "ns", "k", 2, "head")); got != 5*time.Minute {
		t.Errorf("floored mark TTL = %s, want the 5m floor", got)
	}

	legacy := prKey("p:", "ns", "k", 3, "head")
	seedMark(t, mr, legacy, time.Now().Add(-time.Hour).Format(time.RFC3339Nano))
	if markPRNew(t, c, "ns", "k", 3, "head", 0) {
		t.Fatal("existing mark must dedupe")
	}
	if got := mr.TTL(legacy); got != time.Hour {
		t.Errorf("pre-backstop mark TTL after a hit = %s, want the 1h backstop", got)
	}

	c.RecordPROutcome("ns", "k", 1, "head", "success", 0)
	if got := mr.TTL(prKey("p:", "ns", "k", 1, "head") + ":outcome"); got != time.Hour {
		t.Errorf("outcome record TTL = %s, want the 1h backstop", got)
	}
	c.PutScopeOutcome("o", "r", 1, "head", "ns/k", "success", 0)
	if got := mr.TTL("p:label:o/r:pr1:head"); got != time.Hour {
		t.Errorf("label record TTL = %s, want the 1h backstop", got)
	}
}

// ExpirePR turns a standing mark into a cooldown: Flux re-fires stay deduped
// until it lapses, then the next event re-admits the PR-head.
func TestRedis_ExpirePRCooldown(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	if !markPRNew(t, c, "ns", "k", 7, "head", 0) {
		t.Fatal("first mark must be new")
	}
	c.ExpirePR("ns", "k", 7, "head", 2*time.Minute)
	if got := mr.TTL(prKey("p:", "ns", "k", 7, "head")); got != 2*time.Minute {
		t.Fatalf("cooldown TTL = %s, want 2m", got)
	}
	if markPRNew(t, c, "ns", "k", 7, "head", 0) {
		t.Fatal("mark must dedupe during the cooldown")
	}
	mr.FastForward(2*time.Minute + time.Second)
	if !markPRNew(t, c, "ns", "k", 7, "head", 0) {
		t.Fatal("mark must re-admit once the cooldown lapsed")
	}

	c.ExpirePR("ns", "k", 7, "head", 0)
	if !markPRNew(t, c, "ns", "k", 7, "head", 0) {
		t.Fatal("a zero cooldown must release the mark like ForgetPR")
	}
}

// Available costs nothing while admission writes succeed, reports an outage
// after a failed write, and recovers as soon as the server answers again.
func TestRedis_AvailableProbe(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	if !c.Available() {
		t.Fatal("fresh cache must be available")
	}
	mr.SetError("LOADING simulated outage")
	if _, err := c.MarkPRIfNew("ns", "k", 1, "head", 0); err == nil {
		t.Fatal("MarkPRIfNew must fail during the outage")
	}
	if c.Available() {
		t.Fatal("must report unavailable while the server errors")
	}
	mr.SetError("")
	if !c.Available() {
		t.Fatal("must report available once the server answers")
	}
	if !markPRNew(t, c, "ns", "k", 1, "head", 0) {
		t.Fatal("admission must work again after recovery")
	}
}
