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

package processor

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	gh "github.com/google/go-github/v62/github"
	corev1 "k8s.io/api/core/v1"

	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
	"github.com/staffbase/flux-ai-bridge/internal/lifecycle"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
)

func newRedisDedupe(t *testing.T) (*dedupe.Cache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c, err := dedupe.NewRedis("redis://"+mr.Addr(), "", "", "p:", time.Second)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	return c, mr
}

// A GitHub rate limit on delivery puts the PR-head's mark on a cooldown that
// follows GitHub's Retry-After: Flux re-fires during it stay deduped (no
// hammering the limit), and the first event after it re-admits the PR — the
// mark is neither released at once nor kept forever.
func TestProcess_RateLimitedPostCoolsDownMark(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	retryAfter := 3 * time.Minute
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{
		prNumber: 42, prState: "open", prHeadSHA: "prhead111",
		postErr: &gh.AbuseRateLimitError{Message: "secondary rate limit", RetryAfter: &retryAfter},
	}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, cache, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err == nil {
		t.Fatal("expected the rate-limited post to fail the event")
	}
	if got := mr.TTL("p:default:apps:pr42:prhead111"); got != retryAfter {
		t.Fatalf("mark TTL after a rate limit = %s, want the %s Retry-After", got, retryAfter)
	}

	commenterMock.postErr = nil
	if err := proc.Process(context.Background(), event, "srcrev-bbb"); err != nil {
		t.Fatalf("Process during cooldown: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("a re-fire during the cooldown must stay deduped; got %d post attempts", len(commenterMock.posts))
	}

	mr.FastForward(retryAfter + time.Second)
	if err := proc.Process(context.Background(), event, "srcrev-ccc"); err != nil {
		t.Fatalf("Process after cooldown: %v", err)
	}
	if len(commenterMock.posts) != 2 {
		t.Fatalf("the first event after the cooldown must re-admit the PR; got %d post attempts", len(commenterMock.posts))
	}
}

// While the Redis dedupe backend is down every PR would be skipped anyway, so
// after the first failed admission the processor stops before any GitHub
// lookup — and resumes as soon as Redis answers again. The event that first
// hits the outage already reports it (outcome dedupe_error), not a no-op
// success.
func TestProcess_DedupeOutageSkipsGitHubLookups(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, cache, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	mr.SetError("LOADING simulated outage")
	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err == nil || !strings.HasPrefix(err.Error(), "dedupe unavailable:") {
		t.Fatalf("first event during the outage: err = %v, want a dedupe unavailable error", err)
	}
	if commenterMock.findCalls != 1 || len(commenterMock.posts) != 0 {
		t.Fatalf("first event: findCalls=%d posts=%d, want 1 lookup and no post (PR skipped as dedupe_unhealthy)",
			commenterMock.findCalls, len(commenterMock.posts))
	}

	err := proc.Process(context.Background(), event, "srcrev-bbb")
	if err == nil || !strings.HasPrefix(err.Error(), "dedupe unavailable:") {
		t.Fatalf("second event during the outage: err = %v, want a dedupe unavailable error", err)
	}
	if commenterMock.findCalls != 1 {
		t.Fatalf("no GitHub lookup may run while dedupe is down; findCalls = %d", commenterMock.findCalls)
	}

	mr.SetError("")
	if err := proc.Process(context.Background(), event, "srcrev-ccc"); err != nil {
		t.Fatalf("event after recovery: %v", err)
	}
	if commenterMock.findCalls != 2 || len(commenterMock.posts) != 1 {
		t.Fatalf("after recovery: findCalls=%d posts=%d, want the pipeline to run again", commenterMock.findCalls, len(commenterMock.posts))
	}
}

func TestSettleBudget(t *testing.T) {
	const timeout = 3 * time.Minute
	if got := settleBudget(context.Background(), timeout); got != timeout {
		t.Errorf("no deadline: %s, want the full %s", got, timeout)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout+2*time.Minute)
	defer cancel()
	if got := settleBudget(ctx, timeout); got != timeout {
		t.Errorf("ample deadline: %s, want the full %s", got, timeout)
	}

	ctx, cancel = context.WithTimeout(context.Background(), postPollReserve+20*time.Second)
	defer cancel()
	if got := settleBudget(ctx, timeout); got > 20*time.Second || got < 19*time.Second {
		t.Errorf("tight deadline: %s, want about 20s so %s stays free", got, postPollReserve)
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got := settleBudget(ctx, timeout); got != time.Second {
		t.Errorf("deadline inside the reserve: %s, want the 1s floor", got)
	}
}

// Delivery outlives the pipeline deadline (the analysis is already paid for)
// but not a cancellation (shutdown).
func TestDeliveryContext(t *testing.T) {
	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-expired.Done()
	dctx, stop := deliveryContext(expired)
	defer stop()
	time.Sleep(10 * time.Millisecond) // let a (wrong) AfterFunc cancel run
	if err := dctx.Err(); err != nil {
		t.Fatalf("delivery context after the pipeline deadline: %v, want still alive", err)
	}
	if _, ok := dctx.Deadline(); !ok {
		t.Fatal("delivery context must carry its own deadline")
	}

	parent, cancelParent := context.WithCancel(context.Background())
	dctx, stop = deliveryContext(parent)
	defer stop()
	cancelParent()
	select {
	case <-dctx.Done():
		if !errors.Is(dctx.Err(), context.Canceled) {
			t.Fatalf("delivery context err = %v, want Canceled", dctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("cancelling the pipeline (shutdown) must cancel delivery")
	}

	// Deadline first, shutdown later: the expired pipeline context can't
	// report the shutdown, so delivery must follow the attached signal.
	shutdown, stopProcess := context.WithCancel(context.Background())
	defer stopProcess()
	expired, cancel = context.WithTimeout(lifecycle.WithShutdown(context.Background(), shutdown), time.Nanosecond)
	defer cancel()
	<-expired.Done()
	dctx, stop = deliveryContext(expired)
	defer stop()
	time.Sleep(10 * time.Millisecond)
	if err := dctx.Err(); err != nil {
		t.Fatalf("delivery context after the pipeline deadline: %v, want still alive", err)
	}
	stopProcess()
	select {
	case <-dctx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown after the pipeline deadline must cancel delivery")
	}
}

// A green PR-head stays frozen (DEDUPE_REEVAL_FAILED_ONLY) until its next
// commit: its success record must not lapse after 2×TTL like failure records
// do, or a quiet night re-analyses it as new.
func TestProcess_SuccessOutcomeRecordOutlivesReEvalWindow(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	cache.WithMaxTTL(720 * time.Hour)
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none", CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		DedupePRReEvalTTL: 10 * time.Minute, DedupeReEvalFailedOnly: true,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, cache, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}
	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	key := "p:default:apps:pr42:prhead111:outcome"
	if v, err := mr.Get(key); err != nil || v != "success" {
		t.Fatalf("outcome record = %q (%v), want success", v, err)
	}
	if want := 10*time.Minute + labelStateTTL; mr.TTL(key) != want {
		t.Fatalf("success record TTL = %s, want floor + label-state TTL (%s) instead of 2×re-eval (20m) or the 720h backstop", mr.TTL(key), want)
	}
	if got := proc.outcomeRecordTTL("failure"); got != 20*time.Minute {
		t.Fatalf("failure record TTL = %s, want 2×re-eval = 20m", got)
	}
}

// Events inside the re-eval floor are deduped before they can refresh the
// success record, so with a floor longer than labelStateTTL the record must
// still outlive the floor: every event after it has to find the head frozen,
// window after window, not re-analyse it as new.
func TestProcess_SuccessOutcomeRecordOutlivesLongReEvalFloor(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	cache.WithMaxTTL(720 * time.Hour)
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	analyzer := &fakeAnalyzer{result: "## Status\nall good"}
	floor := labelStateTTL + 24*time.Hour
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true, LLMSkipOnHealthy: false,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		DedupePRReEvalTTL: floor, DedupeReEvalFailedOnly: true,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzer, commenterMock, nil, cache, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if analyzer.calls != 1 {
		t.Fatalf("first event must analyse the head; Analyze calls = %d", analyzer.calls)
	}
	// Regular events, half a floor apart: the ones inside the floor are
	// deduped without refreshing anything, the ones past it are frozen skips.
	for i, rev := range []string{"srcrev-bbb", "srcrev-ccc", "srcrev-ddd", "srcrev-eee"} {
		mr.FastForward(floor/2 + time.Second)
		if err := proc.Process(context.Background(), event, rev); err != nil {
			t.Fatalf("Process (event %d): %v", i+2, err)
		}
		if analyzer.calls != 1 {
			t.Fatalf("green head re-analysed at event %d with a %s floor: success record expired first; Analyze calls = %d", i+2, floor, analyzer.calls)
		}
	}
}

// A healthy analysis whose LLM call failed went out as "LLM analysis
// unavailable". It must not freeze the head like a success does
// (DEDUPE_REEVAL_FAILED_ONLY): the next re-eval retries the LLM and replaces
// the degraded comment.
func TestProcess_HealthyLLMFallbackIsReEvaluated(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	analyzer := &fakeAnalyzer{err: errors.New("provider down")}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true, LLMSkipOnHealthy: false,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		DedupePRReEvalTTL: 10 * time.Minute, DedupeReEvalFailedOnly: true,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzer, commenterMock, nil, cache, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !strings.Contains(commenterMock.prCommentBody, "LLM analysis unavailable") {
		t.Fatalf("expected the degraded comment, got %q", commenterMock.prCommentBody)
	}

	analyzer.err, analyzer.result = nil, "## Status\nall good"
	mr.FastForward(10*time.Minute + time.Second) // admission mark expires: re-eval window
	if err := proc.Process(context.Background(), event, "srcrev-bbb"); err != nil {
		t.Fatalf("Process (re-eval): %v", err)
	}
	if analyzer.calls != 2 {
		t.Fatalf("re-eval must retry the LLM after a degraded healthy run; Analyze calls = %d", analyzer.calls)
	}
	if strings.Contains(commenterMock.prCommentBody, "LLM analysis unavailable") {
		t.Fatalf("degraded comment not replaced after the LLM recovered: %q", commenterMock.prCommentBody)
	}
}
