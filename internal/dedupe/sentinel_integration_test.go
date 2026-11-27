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

//go:build integration_sentinel

// Integration test for the Sentinel-backed dedupe Cache. Unlike the rest of
// the dedupe test suite (which uses in-process miniredis), this one needs
// real Redis + Sentinel processes — miniredis doesn't implement the
// SENTINEL command set, so the `NewRedisSentinel` codepath (auth, master
// discovery, failover-aware client) is otherwise impossible to exercise in
// CI-ish fashion.
//
// Build tag `integration_sentinel` keeps it off `go test ./...` by default.
// Run it via `make test-sentinel`, which:
//
//  1. Starts a single-node Redis with `requirepass=<redisPassword>` on
//     port `${SENTINEL_TEST_REDIS_PORT}`.
//  2. Starts a Sentinel monitoring that Redis with
//     `sentinel auth-pass mymaster <redisPassword>` AND
//     `requirepass=<sentinelPassword>` on port `${SENTINEL_TEST_SENTINEL_PORT}`
//     — Sentinel's own ACL is distinct from the backend's, mirroring the
//     Bitnami/production layout the bridge's docs describe.
//  3. Runs this test, which connects via `NewRedisSentinel` with BOTH
//     credential pairs populated and drives every Redis command the bridge
//     issues (SET NX with expiry, EXPIRE LT, the INCR+EXPIRE and
//     HSET+EXPIRE pipelines, EXPIRE, DEL, PING) through the real wire —
//     all plain keyspace commands, no scripting, so they work under ACLs
//     that deny EVAL.
//
// The negative tests use a wrong sentinel password and a wrong Redis
// password: `NewRedisSentinel` still returns a cache (it only logs the auth
// failure), and every admission write then fails, so the bridge fails closed
// at runtime instead of analysing without cross-replica coordination.
package dedupe_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
)

// envOrDefault keeps the test self-contained when run directly with `go
// test -tags integration_sentinel ./internal/dedupe/...` against a
// pre-existing Sentinel — the defaults match the Makefile target's
// docker setup, but operators iterating locally may want to point at a
// different host/port.
func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func sentinelAddrFromEnv() string {
	host := envOrDefault("SENTINEL_TEST_HOST", "127.0.0.1")
	port := envOrDefault("SENTINEL_TEST_SENTINEL_PORT", "26379")
	return fmt.Sprintf("%s:%s", host, port)
}

const (
	defaultMasterName       = "mymaster"
	defaultRedisPassword    = "fab-redis-pw-integration"
	defaultSentinelPassword = "fab-sentinel-pw-integration"
)

func redisPassword() string {
	return envOrDefault("SENTINEL_TEST_REDIS_PASSWORD", defaultRedisPassword)
}

func sentinelPassword() string {
	return envOrDefault("SENTINEL_TEST_SENTINEL_PASSWORD", defaultSentinelPassword)
}

// TestSentinel_HappyPath verifies the load-bearing claim of the
// SentinelOptions doc comment: with both credential pairs populated,
// `NewRedisSentinel` connects, asks the quorum for the master, and routes
// every dedupe operation through with the master's AUTH applied.
func TestSentinel_HappyPath(t *testing.T) {
	cache, err := dedupe.NewRedisSentinel(dedupe.SentinelOptions{
		Addrs:            []string{sentinelAddrFromEnv()},
		MasterName:       defaultMasterName,
		Username:         "", // default user — bitnami/redis-sentinel layout
		Password:         redisPassword(),
		SentinelUsername: "",
		SentinelPassword: sentinelPassword(),
		KeyPrefix:        "fab-test:",
		OpTimeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisSentinel: %v (is sentinel reachable at %s? did `make test-sentinel` start the containers?)", err, sentinelAddrFromEnv())
	}
	cache.WithMaxTTL(time.Hour) // exercise the EXPIRE LT backstop path

	ns, name, pr := "test-ns", "test-name", 42
	head := fmt.Sprintf("sentinel-%d", time.Now().UnixNano())
	t.Cleanup(func() { cache.ForgetPR(ns, name, pr, head) })

	mark := func(label string) bool {
		t.Helper()
		isNew, err := cache.MarkPRIfNew(ns, name, pr, head, 0)
		if err != nil {
			t.Fatalf("MarkPRIfNew (%s): %v", label, err)
		}
		return isNew
	}
	if !mark("first") {
		t.Fatal("first MarkPRIfNew returned false; expected true for a fresh key")
	}
	if mark("second") {
		t.Fatal("second MarkPRIfNew returned true; SET NX must be atomic across calls")
	}

	cache.RecordPROutcome(ns, name, pr, head, "failure", time.Minute)
	if got, ok := cache.LastPROutcome(ns, name, pr, head); !ok || got != "failure" {
		t.Fatalf("LastPROutcome = (%q, %v), want (failure, true)", got, ok)
	}
	if n := cache.IncrPRReEval(ns, name, pr, head, time.Minute); n != 1 {
		t.Fatalf("IncrPRReEval = %d, want 1", n)
	}
	cache.PutScopeOutcome("owner", "repo", pr, head, ns+"/"+name, "success", time.Minute)
	if got := cache.ScopeOutcomes("owner", "repo", pr, head); got[ns+"/"+name] != "success" {
		t.Fatalf("ScopeOutcomes = %v, want %s/%s=success", got, ns, name)
	}

	cache.ExpirePR(ns, name, pr, head, time.Minute)
	if mark("during cooldown") {
		t.Fatal("MarkPRIfNew returned true during an ExpirePR cooldown")
	}
	cache.ForgetPR(ns, name, pr, head)
	if !mark("after ForgetPR") {
		t.Fatal("MarkPRIfNew returned false after ForgetPR; deletion didn't take")
	}
	if !cache.Available() {
		t.Fatal("Available returned false against a healthy Sentinel setup")
	}
}

// TestSentinel_WrongSentinelPasswordFailsClosed asserts the documented
// fail-closed contract: a bad Sentinel password means `MarkPRIfNew` returns
// `(false, err)` and the processor skips the PR rather than
// running the pipeline without cross-replica dedupe coordination. The
// constructor itself succeeds and logs ERROR — matching the
// `TestRedis_WrongPasswordFailsClosed` pattern for the single-node path,
// where startup failures are surfaced via logs but the bridge stays up
// (transient sentinel/redis outages during a rolling restart shouldn't
// crash-loop the bridge; auth misconfigs surface in the ERROR log).
func TestSentinel_WrongSentinelPasswordFailsClosed(t *testing.T) {
	cache, err := dedupe.NewRedisSentinel(dedupe.SentinelOptions{
		Addrs:            []string{sentinelAddrFromEnv()},
		MasterName:       defaultMasterName,
		Password:         redisPassword(),
		SentinelPassword: "this-is-definitely-not-the-sentinel-password",
		OpTimeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisSentinel constructor returned err: %v (should warn-log and return cache)", err)
	}
	isNew, err := cache.MarkPRIfNew("ns", "name", 1, "head", 0)
	if err == nil {
		t.Fatal("MarkPRIfNew with wrong SentinelPassword must surface the auth error (fail-closed)")
	}
	if isNew {
		t.Fatal("MarkPRIfNew must return false alongside the error so the PR isn't admitted")
	}
	t.Logf("got expected auth error: %v", err)
}

// TestSentinel_WrongRedisPasswordFailsClosed asserts the symmetric case:
// sentinel auth succeeds, master is discovered, but the master rejects
// the AUTH command with the wrong backend password. MarkPRIfNew should
// surface that as an error too.
func TestSentinel_WrongRedisPasswordFailsClosed(t *testing.T) {
	cache, err := dedupe.NewRedisSentinel(dedupe.SentinelOptions{
		Addrs:            []string{sentinelAddrFromEnv()},
		MasterName:       defaultMasterName,
		Password:         "this-is-definitely-not-the-redis-password",
		SentinelPassword: sentinelPassword(),
		OpTimeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisSentinel constructor returned err: %v (should warn-log and return cache)", err)
	}
	isNew, err := cache.MarkPRIfNew("ns", "name", 1, "head", 0)
	if err == nil {
		t.Fatal("MarkPRIfNew with wrong backend Password must surface the auth error (fail-closed)")
	}
	if isNew {
		t.Fatal("MarkPRIfNew must return false alongside the error so the PR isn't admitted")
	}
	t.Logf("got expected auth error: %v", err)
}
