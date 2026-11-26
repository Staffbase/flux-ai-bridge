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
	"log/slog"
	"strings"
	"testing"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
	corev1 "k8s.io/api/core/v1"
)

func TestRevisionMismatch(t *testing.T) {
	cases := []struct {
		applied, analysed string
		want              bool // true = confident mismatch → gate defers
	}{
		{"dev@sha1:abc123", "abc123", false},          // branch-prefixed vs bare sha → same
		{"main@sha1:deadbeef", "deadbeef", false},     //
		{"v1.2.3@sha256:cafe", "cafe", false},         // OCI/helm digest → same
		{"dev@sha1:abc123", "def456", true},           // different commit → mismatch
		{"abc123", "abc123", false},                   // both bare, same
		{"", "abc123", false},                         // blank applied → NOT a mismatch (fall through)
		{"dev@sha1:abc123", "", false},                // blank analysed → NOT a mismatch
		{"dev@sha1:", "abc123", false},                // unparseable applied (empty token) → NOT a mismatch
		{"dev@sha1:abc123", "dev@sha1:abc123", false}, // both prefixed, same
	}
	for _, c := range cases {
		if got := revisionMismatch(c.applied, c.analysed); got != c.want {
			t.Errorf("revisionMismatch(%q,%q) = %v, want %v", c.applied, c.analysed, got, c.want)
		}
	}
}

const notYetAppliedKustomization = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  lastAppliedRevision: dev@sha1:0000000000000000000000000000000000000000
  inventory:
    entries:
      - id: "ns_app__ConfigMap"
`

// lastAppliedRevision ends in ':' → its SHA token is empty → revisionMismatch
// must NOT treat it as a mismatch, so analysis proceeds instead of deferring
// forever on a value that can never compare equal.
const malformedRevisionKustomization = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  lastAppliedRevision: "dev@sha1:"
  inventory:
    entries:
      - id: "ns_app__ConfigMap"
`

func TestProcess_MalformedAppliedRevisionDoesNotDefer(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(malformedRevisionKustomization)}
	commenterMock := &fakeCommenter{
		prNumber: 5,
		prFiles:  []bridgegithub.PRFile{{Path: "app.yaml", BlobSHA: "cm", Status: "modified"}},
		blobs:    map[string][]byte{"cm": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n  namespace: ns\n")},
	}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "app-ks", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "1111111111111111111111111111111111111111"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.owner == "" {
		t.Error("malformed lastAppliedRevision was deferred by the revision gate (no PR lookup)")
	}
	if commenterMock.prCommentBody == "" {
		t.Error("malformed lastAppliedRevision produced no comment despite a matching PR")
	}
}

// The analysed revision isn't the one the Kustomization has applied yet → the
// event is deferred: no PR lookup, no comment, no label. (Regression:
// flux-success before the change rolled out.)
func TestProcess_DefersWhenRevisionNotApplied(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(notYetAppliedKustomization)}
	analyzerMock := &fakeAnalyzer{result: "should not be reached"}
	commenterMock := &fakeCommenter{
		prNumber: 5,
		prFiles:  []bridgegithub.PRFile{{Path: "app.yaml", BlobSHA: "cm", Status: "modified"}},
		blobs:    map[string][]byte{"cm": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n  namespace: ns\n")},
	}
	// LLM enabled (a real provider + a non-nil analyzer) so the assertion below
	// proves the gate defers BEFORE the LLM would have been called.
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", PRLabelEnabled: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "app-ks", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	// Analysed revision differs from the applied one (all-zero) above.
	if err := proc.Process(context.Background(), event, "1111111111111111111111111111111111111111"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.owner != "" {
		t.Errorf("deferred event still looked up PRs (FindPRsForCommit called: owner=%q)", commenterMock.owner)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("deferred event posted a comment: %q", commenterMock.prCommentBody)
	}
	if len(commenterMock.labelCalls) != 0 {
		t.Errorf("deferred event set labels: %v", commenterMock.labelCalls)
	}
	if analyzerMock.calls != 0 {
		t.Errorf("deferred event invoked the LLM: calls=%d", analyzerMock.calls)
	}
}

const appliedKustomization = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  lastAppliedRevision: dev@sha1:2222222222222222222222222222222222222222
  inventory:
    entries:
      - id: "ns_app__ConfigMap"
`

// When the analysed revision matches lastAppliedRevision, analysis proceeds as
// normal — the PR that declares an inventory resource is attributed and gets a
// comment.
func TestProcess_ProceedsWhenRevisionApplied(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(appliedKustomization)}
	commenterMock := &fakeCommenter{
		prNumber: 5,
		prFiles:  []bridgegithub.PRFile{{Path: "app.yaml", BlobSHA: "cm", Status: "modified"}},
		blobs:    map[string][]byte{"cm": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n  namespace: ns\n")},
	}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "app-ks", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	// Analysed revision == the applied one's sha → gate passes.
	if err := proc.Process(context.Background(), event, "2222222222222222222222222222222222222222"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	// The ConfigMap is in the inventory but absent from the (fake) cluster →
	// NotFound → non-green → a comment is posted, proving analysis ran.
	if commenterMock.owner == "" {
		t.Error("applied-revision event did not proceed to PR lookup")
	}
	if commenterMock.prCommentBody == "" {
		t.Error("applied-revision event produced no comment despite a matching PR")
	}
}

// A non-green comment carries the static auto-retry policy footer derived from
// the re-eval config; a run with re-eval disabled does not.
func TestProcess_RetryFooterOnNonGreenComment(t *testing.T) {
	newProc := func(ttl time.Duration, max int) (*Processor, *fakeCommenter) {
		mcpMock := &fakeMCPClient{data: []byte(appliedKustomization)}
		commenterMock := &fakeCommenter{
			prNumber: 5,
			prFiles:  []bridgegithub.PRFile{{Path: "app.yaml", BlobSHA: "cm", Status: "modified"}},
			blobs:    map[string][]byte{"cm": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n  namespace: ns\n")},
		}
		cfg := &config.Config{
			GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
			CommentIncludeFooter: true, CommentOnSuccess: true,
			DedupePRReEvalTTL: ttl, DedupePRReEvalMax: max,
			ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		}
		return New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default()), commenterMock
	}
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "app-ks", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}

	// Re-eval enabled (15m / 4) → the failing comment carries the policy line.
	proc, commenterMock := newProc(15*time.Minute, 4)
	if err := proc.Process(context.Background(), event, "2222222222222222222222222222222222222222"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if !strings.Contains(commenterMock.prCommentBody, "🔁 Re-evaluated automatically up to 4×, at most once every 15m") {
		t.Errorf("non-green comment missing retry footer:\n%s", commenterMock.prCommentBody)
	}

	// Re-eval disabled (ttl=0) → no policy line.
	procOff, commenterOff := newProc(0, 4)
	if err := procOff.Process(context.Background(), event, "2222222222222222222222222222222222222222"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if strings.Contains(commenterOff.prCommentBody, "🔁") {
		t.Errorf("retry footer present despite re-eval disabled:\n%s", commenterOff.prCommentBody)
	}
}

// The revision gate applies ONLY to the success path. A failing reconcile or a
// dependency wait inherently lags lastAppliedRevision (the apply never landed),
// but those are exactly the ❌/⚠️ states the bridge must report — deferring them
// would leave a failing PR silently unlabelled. Both must proceed to analysis
// despite the revision mismatch.
func TestProcess_DoesNotDeferFailureOrWaitEvents(t *testing.T) {
	// notYetAppliedKustomization pins lastAppliedRevision to all-zeros; the
	// analysed sha below differs, so the gate WOULD defer on a healthy event.
	cases := []struct {
		name  string
		event eventv1.Event
	}{
		{
			name: "reconcile failure (severity=error)",
			event: eventv1.Event{
				InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "app-ks", Namespace: "flux-system"},
				Severity:       eventv1.EventSeverityError, Reason: "BuildFailed",
				Message: "kustomize build failed",
			},
		},
		{
			name: "dependency wait (DependencyNotReady)",
			event: eventv1.Event{
				InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "app-ks", Namespace: "flux-system"},
				Severity:       eventv1.EventSeverityError, Reason: dependencyNotReadyReason,
				Message: "dependency 'flux-system/infra' is not ready",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mcpMock := &fakeMCPClient{data: []byte(notYetAppliedKustomization)}
			commenterMock := &fakeCommenter{
				prNumber: 5,
				prFiles:  []bridgegithub.PRFile{{Path: "app.yaml", BlobSHA: "cm", Status: "modified"}},
				blobs:    map[string][]byte{"cm": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n  namespace: ns\n")},
			}
			cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
				ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
			proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
			// Analysed revision differs from the applied (all-zero) one — a healthy
			// event would defer here; a failure/wait event must not.
			if err := proc.Process(context.Background(), tc.event, "1111111111111111111111111111111111111111"); err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if commenterMock.owner == "" {
				t.Error("failure/wait event was deferred by the revision gate (no PR lookup)")
			}
			if commenterMock.prCommentBody == "" {
				t.Error("failure/wait event produced no comment despite a matching PR")
			}
		})
	}
}

// Ready=False describes the controller's last ATTEMPT. When that attempt was a
// different revision, the failure isn't this revision's; a blank attempted
// revision keeps the condition.
func TestReconcileFailureMessage_IgnoresOtherRevisionsAttempt(t *testing.T) {
	var kspec kustomizationSpec
	kspec.Status.Conditions = append(kspec.Status.Conditions, struct {
		Type    string `yaml:"type"`
		Status  string `yaml:"status"`
		Reason  string `yaml:"reason"`
		Message string `yaml:"message"`
	}{Type: "Ready", Status: "False", Reason: "BuildFailed", Message: "kustomize build failed"})
	info := eventv1.Event{Severity: eventv1.EventSeverityInfo}
	for attempted, wantFailed := range map[string]bool{
		"main@sha1:bbb": false, // a later revision failed, not this one
		"main@sha1:aaa": true,  // this revision's own attempt
		"":              true,  // unknown: keep the condition
	} {
		kspec.Status.LastAttemptedRevision = attempted
		failed, _ := reconcileFailureMessage(info, kspec, "aaa")
		if (failed != "") != wantFailed {
			t.Errorf("lastAttemptedRevision=%q: failed=%q, want failed=%v", attempted, failed, wantFailed)
		}
	}
}
