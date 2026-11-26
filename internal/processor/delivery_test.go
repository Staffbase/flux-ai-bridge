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
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	gh "github.com/google/go-github/v62/github"
	corev1 "k8s.io/api/core/v1"

	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
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

// A roster that can't be read is not an empty roster. Taking "no known scopes"
// as "every scope reported" would label a head flux-success from one scope's
// green outcome while other Kustomizations of the PR haven't reported. The
// success is held and the head labelled flux-warning instead, and an error is
// returned so deliver releases the admission mark: kept, it would skip the
// head on later events, and the held success would never be written.
func TestReconcileLabels_UnreadableRosterHoldsSuccess(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	// A roster key of the wrong type makes HGETALL fail (WRONGTYPE) while the
	// per-head outcome hash stays readable.
	if err := mr.Set("p:label:o/r:pr42:roster", "not-a-hash"); err != nil {
		t.Fatalf("seed roster key: %v", err)
	}
	commenterMock := &fakeCommenter{}
	proc := New(&config.Config{PRLabelEnabled: true}, nil, nil, nil, commenterMock, nil, cache, slog.Default())

	if err := proc.reconcileLabels(context.Background(), "o", "r", "ns/fast", "h1", 42, bridgegithub.OutcomeSuccess); err == nil {
		t.Fatal("reconcileLabels must return the roster error so a later event retries the label")
	}
	calls := commenterMock.labelCalls
	if len(calls) != 1 || calls[0].Outcome != bridgegithub.OutcomeWarning {
		t.Fatalf("an unreadable roster must hold flux-success and label the head flux-warning; got %+v", calls)
	}
}

// perPRErrCommenter fails label, comment and comment-removal writes per PR
// number.
type perPRErrCommenter struct {
	*fakeCommenter
	labelErrs, postErrs, removeErrs map[int]error
}

func (c *perPRErrCommenter) SetPRLabels(ctx context.Context, owner, repo string, prNumber int, headSHA string, outcome bridgegithub.Outcome) error {
	if err := c.labelErrs[prNumber]; err != nil {
		return err
	}
	return c.fakeCommenter.SetPRLabels(ctx, owner, repo, prNumber, headSHA, outcome)
}

func (c *perPRErrCommenter) PostOrUpdateCommentKeyed(ctx context.Context, owner, repo, ns, name string, prNumber int, body, stabilityKey string) error {
	if err := c.postErrs[prNumber]; err != nil {
		return err
	}
	return c.fakeCommenter.PostOrUpdateCommentKeyed(ctx, owner, repo, ns, name, prNumber, body, stabilityKey)
}

func (c *perPRErrCommenter) RemoveComment(ctx context.Context, owner, repo, ns, name string, prNumber int) error {
	if err := c.removeErrs[prNumber]; err != nil {
		return err
	}
	return c.fakeCommenter.RemoveComment(ctx, owner, repo, ns, name, prNumber)
}

// labelHookCommenter runs onLabel before each label write.
type labelHookCommenter struct {
	*fakeCommenter
	onLabel func()
}

func (c *labelHookCommenter) SetPRLabels(ctx context.Context, owner, repo string, prNumber int, headSHA string, outcome bridgegithub.Outcome) error {
	if c.onLabel != nil {
		c.onLabel()
	}
	return c.fakeCommenter.SetPRLabels(ctx, owner, repo, prNumber, headSHA, outcome)
}

// A de-scope label write that fails must stay retryable even when the backend
// fails right after it: keeping the retry state may not need a roster write
// after the failed label write, since that write can fail as well. Here Redis
// rejects every command from the failed label write until the event ends; once
// it recovers, the next event retries the write and finishes the de-scope.
func TestProcess_DeScope_FailedLabelWriteRetriedWhenBackendFailsToo(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	owner, repo, pr, head := "owner", "repo", 42, "h-new"
	cache.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	cache.AddPRScope(owner, repo, pr, "default/other", time.Hour)
	cache.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeSuccess), time.Hour)
	cache.PutScopeOutcome(owner, repo, pr, head, "default/other", string(bridgegithub.OutcomeFailure), time.Hour)

	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)} // inventory: default_any__ConfigMap
	fc := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles:  []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:    map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
		labelErr: errors.New("server error"),
	}
	commenterMock := &labelHookCommenter{fakeCommenter: fc, onLabel: func() { mr.SetError("LOADING Redis is loading the dataset in memory") }}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, cache, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "other", Namespace: "default"}}

	_ = proc.Process(context.Background(), event, "srcrev") // the outage may fail the event too
	if len(fc.labelCalls) == 0 {
		t.Fatal("the de-scope did not attempt a label write")
	}

	mr.SetError("")
	commenterMock.onLabel = nil
	fc.labelErr = nil
	before := len(fc.labelCalls)
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process (retry): %v", err)
	}
	if calls := fc.labelCalls[before:]; len(calls) == 0 || calls[len(calls)-1].Outcome != bridgegithub.OutcomeSuccess {
		t.Fatalf("the failed de-scope label write was not retried; label calls on the next event %+v", calls)
	}
	if slices.Contains(cache.PRScopes(owner, repo, pr), "default/other") {
		t.Fatal("default/other still on the roster after the retried de-scope")
	}
	if mr.HGet("p:label:owner/repo:pr42:roster", "default/other") != "" {
		t.Fatal("the pending de-scope marker outlived the retried label write")
	}
}

// A de-scope whose per-head outcome can't be removed must not go on: the label
// rewrite would still count the stale outcome, and dropping the marker after it
// would leave no retry state. The marker stays, and once the outcome can be
// removed, the next event finishes the de-scope.
func TestProcess_DeScope_FailedOutcomeRemovalKeepsMarker(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	owner, repo, pr, head := "owner", "repo", 42, "h-new"
	roster, outcomes := "p:label:owner/repo:pr42:roster", "p:label:owner/repo:pr42:h-new"
	cache.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	cache.AddPRScope(owner, repo, pr, "default/other", time.Hour)
	// A per-head outcome key of the wrong type makes HDEL fail (WRONGTYPE).
	if err := mr.Set(outcomes, "not-a-hash"); err != nil {
		t.Fatalf("seed outcome key: %v", err)
	}

	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)} // inventory: default_any__ConfigMap
	fc := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles: []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, fc, nil, cache, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "other", Namespace: "default"}}

	_ = proc.Process(context.Background(), event, "srcrev")
	if got := mr.HGet(roster, "default/other"); got != "descoping" {
		t.Fatalf("a failed outcome removal must keep the de-scope marker; roster entry = %q", got)
	}

	mr.Del(outcomes)
	cache.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeSuccess), time.Hour)
	cache.PutScopeOutcome(owner, repo, pr, head, "default/other", string(bridgegithub.OutcomeFailure), time.Hour)
	before := len(fc.labelCalls)
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process (retry): %v", err)
	}
	if calls := fc.labelCalls[before:]; len(calls) == 0 || calls[len(calls)-1].Outcome != bridgegithub.OutcomeSuccess {
		t.Fatalf("the retried de-scope did not drop the stale failure; label calls %+v", calls)
	}
	if mr.HGet(roster, "default/other") != "" {
		t.Fatal("the de-scope marker outlived the finished de-scope")
	}
}

// A rate-limited label write on one PR keeps its cooldown when a later PR's
// comment write (post, or removal when suppressed) then fails for another
// reason: returning only that error's zero cooldown would release the first
// PR's mark, and the next event would hit GitHub again before the limit
// resets.
func TestDeliver_LabelCooldownSurvivesLaterCommentError(t *testing.T) {
	retryAfter := 3 * time.Minute
	for _, suppress := range []bool{false, true} {
		commenterMock := &perPRErrCommenter{
			fakeCommenter: &fakeCommenter{},
			labelErrs: map[int]error{1: &gh.AbuseRateLimitError{
				Response:   &http.Response{StatusCode: http.StatusForbidden, Request: httptest.NewRequest(http.MethodPost, "https://api.example.com/labels", nil)},
				Message:    "secondary rate limit",
				RetryAfter: &retryAfter,
			}},
			postErrs:   map[int]error{2: errors.New("server error")},
			removeErrs: map[int]error{2: errors.New("server error")},
		}
		proc := New(&config.Config{PRLabelEnabled: true}, nil, nil, nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
		cooldown, err := proc.deliver(context.Background(), prDelivery{
			owner: "o", repo: "r", ns: "ns", name: "k", body: "b", stabilityKey: "b",
			outcome: bridgegithub.OutcomeSuccess, suppress: suppress,
		}, []bridgegithub.PRInfo{{Number: 1, HeadSHA: "a"}, {Number: 2, HeadSHA: "b"}}, map[int]bool{})
		if err == nil {
			t.Fatalf("suppress=%v: expected PR 2's comment error", suppress)
		}
		if cooldown != retryAfter {
			t.Errorf("suppress=%v: cooldown = %s, want PR 1's %s label Retry-After", suppress, cooldown, retryAfter)
		}
	}
}

// One batch, two deliveries: the self-removal report for PR 42, whose label
// write is rate-limited, then the normal report for PR 43. The second
// delivery's zero cooldown must not overwrite the first one's, or PR 42's mark
// is released instead of cooled down.
func TestProcess_SelfRemovalLabelCooldownSurvivesNormalDelivery(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	retryAfter := 3 * time.Minute
	commenterMock := &perPRErrCommenter{
		fakeCommenter: &fakeCommenter{
			prs: []bridgegithub.PRInfo{
				{Number: 42, HeadSHA: "head42", State: "open"},
				{Number: 43, HeadSHA: "head43", State: "open"},
			},
			filesByPR: map[int][]bridgegithub.PRFile{
				42: {selfRemovalCRDFile, {Path: "clusters/base/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"}},
				43: {selfRemovalCRDFile},
			},
			blobs: map[string][]byte{"crd-blob": []byte(selfRemovalCRD), "ks-blob": []byte(selfRemovalCR)},
		},
		labelErrs: map[int]error{42: &gh.AbuseRateLimitError{
			Response:   &http.Response{StatusCode: http.StatusForbidden, Request: httptest.NewRequest(http.MethodPost, "https://api.example.com/labels", nil)},
			Message:    "secondary rate limit",
			RetryAfter: &retryAfter,
		}},
	}
	mcpMock := &fakeMCPClient{
		data:                 []byte(selfRemovalChild),
		kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(selfRemovalParent)},
		resources:            selfRemovalSources,
	}
	cfg := &config.Config{
		LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, cache, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityError,
		Message:        "kustomization path not found: stat /tmp/x: no such file or directory",
	}
	if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if len(commenterMock.posts) != 2 {
		t.Fatalf("expected both PRs' comments, got %+v", commenterMock.posts)
	}
	if got := mr.TTL("p:flux-system:legacy-crds:pr42:head42"); got != retryAfter {
		t.Fatalf("PR 42 mark TTL after its rate-limited label = %s, want the %s Retry-After", got, retryAfter)
	}
}
