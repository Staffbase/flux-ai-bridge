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
	"testing"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
	bridgesource "github.com/staffbase/flux-ai-bridge/internal/source"
	corev1 "k8s.io/api/core/v1"
)

// fakeScope is a stand-in ScopeResolver: it returns a fixed build file set (or
// an error) regardless of input.
type fakeScope struct {
	files map[string]struct{}
	err   error
}

func (f fakeScope) BuildFiles(_ context.Context, _ bridgesource.Ref, _, _ string) (map[string]struct{}, error) {
	return f.files, f.err
}

func newScopeTestProc(t *testing.T, scope ScopeResolver, commenter *fakeCommenter, analyzer *fakeAnalyzer) *Processor {
	t.Helper()
	mcp := &fakeMCPClient{data: []byte(kustomizationWithInventory)}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcp, readiness.NewChecker(mcp), analyzer, commenter, nil, nil, slog.Default())
	return proc.WithScopeResolver(scope)
}

func scopeTestEvent() eventv1.Event {
	return eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "blackbox-exporter", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}
}

// TestProcess_ScopeGate_ExcludesFileNotInBuild is the exact-attribution fix: a
// PR whose changed file declares a resource that IS in the inventory, but whose
// file is NOT part of this Kustomization's build (it lives in a different
// overlay), must NOT be attributed — even though identity matches.
func TestProcess_ScopeGate_ExcludesFileNotInBuild(t *testing.T) {
	commenter := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/blackbox-exporter/release.yaml", BlobSHA: "blob-blackbox", Status: "modified"},
		},
		blobs: map[string][]byte{"blob-blackbox": []byte(blackboxHelmRelease)},
	}
	analyzer := &fakeAnalyzer{result: "## Status\nok"}
	// Build set does NOT contain the PR's file → gated out.
	scope := fakeScope{files: map[string]struct{}{"apps/other/release.yaml": {}}}

	proc := newScopeTestProc(t, scope, commenter, analyzer)
	if err := proc.Process(context.Background(), scopeTestEvent(), "abc1234"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if commenter.prCommentBody != "" {
		t.Errorf("comment posted for a file outside the build set: %q", commenter.prCommentBody)
	}
	if analyzer.calls != 0 {
		t.Errorf("LLM invoked despite scope gate; calls=%d", analyzer.calls)
	}
}

// TestProcess_ScopeGate_AdmitsFileInBuild is the positive counterpart: when the
// changed file IS in the build set, attribution proceeds as before.
func TestProcess_ScopeGate_AdmitsFileInBuild(t *testing.T) {
	commenter := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/blackbox-exporter/release.yaml", BlobSHA: "blob-blackbox", Status: "modified"},
		},
		blobs: map[string][]byte{"blob-blackbox": []byte(blackboxHelmRelease)},
	}
	analyzer := &fakeAnalyzer{result: "## Status\nok"}
	scope := fakeScope{files: map[string]struct{}{"apps/blackbox-exporter/release.yaml": {}}}

	proc := newScopeTestProc(t, scope, commenter, analyzer)
	if err := proc.Process(context.Background(), scopeTestEvent(), "abc1234"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if commenter.prCommentBody == "" {
		t.Error("expected a comment when the PR file is in the build set")
	}
}

// TestProcess_ScopeGate_AdmitsBuildMemberWithoutInventoryResource is the
// follow-up: a PR whose only change is a build input that declares NO inventory
// resource (a patch, the kustomization.yaml, a generator input) is still
// attributed, because the file is part of this Kustomization's build. Under the
// old identity-only model it would have been dropped.
func TestProcess_ScopeGate_AdmitsBuildMemberWithoutInventoryResource(t *testing.T) {
	commenter := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/web/dev/kustomization.yaml", BlobSHA: "blob-kust", Status: "modified"},
		},
		// Declares a resource NOT in the inventory → identity match alone fails.
		blobs: map[string][]byte{"blob-kust": []byte(unrelatedHelmRelease)},
	}
	analyzer := &fakeAnalyzer{result: "## Status\nok"}
	scope := fakeScope{files: map[string]struct{}{"apps/web/dev/kustomization.yaml": {}}}

	proc := newScopeTestProc(t, scope, commenter, analyzer)
	if err := proc.Process(context.Background(), scopeTestEvent(), "abc1234"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if commenter.prCommentBody == "" {
		t.Error("expected a comment: a build-member file should attribute even without an inventory resource")
	}
}

// TestProcess_ScopeGate_ForeignKustomizationCRNotAttributedToParent guards
// follow-CR-to-target through the build-membership path: an app-of-apps parent's
// build set contains its child Kustomization CR files, but a PR editing only a
// child CR must route to that child (via the child's own reconcile), NOT light
// up the parent — even though the file is in the parent's build.
func TestProcess_ScopeGate_ForeignKustomizationCRNotAttributedToParent(t *testing.T) {
	// A Flux Kustomization CR for a DIFFERENT Kustomization than the event's
	// (event is flux-system/blackbox-exporter; this declares flux-system/child).
	childCR := "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata:\n  name: child\n  namespace: flux-system\n"
	commenter := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "clusters/prod/child-kustomization.yaml", BlobSHA: "blob-child", Status: "modified"},
		},
		blobs: map[string][]byte{"blob-child": []byte(childCR)},
	}
	analyzer := &fakeAnalyzer{result: "## Status\nok"}
	// The child CR file IS in the (parent) Kustomization's build set.
	scope := fakeScope{files: map[string]struct{}{"clusters/prod/child-kustomization.yaml": {}}}

	proc := newScopeTestProc(t, scope, commenter, analyzer)
	if err := proc.Process(context.Background(), scopeTestEvent(), "abc1234"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if commenter.prCommentBody != "" {
		t.Errorf("foreign child Kustomization CR was attributed to the parent: %q", commenter.prCommentBody)
	}
}

// TestProcess_ScopeGate_ErrorSkips covers exact-or-skip: when the build scope
// can't be resolved for this revision (e.g. the artifact was GC'd), the event
// is skipped without posting — a later reconcile retries.
func TestProcess_ScopeGate_ErrorSkips(t *testing.T) {
	commenter := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/blackbox-exporter/release.yaml", BlobSHA: "blob-blackbox", Status: "modified"},
		},
		blobs: map[string][]byte{"blob-blackbox": []byte(blackboxHelmRelease)},
	}
	analyzer := &fakeAnalyzer{result: "## Status\nok"}
	scope := fakeScope{err: bridgesource.ErrRevisionMismatch}

	proc := newScopeTestProc(t, scope, commenter, analyzer)
	if err := proc.Process(context.Background(), scopeTestEvent(), "abc1234"); err != nil {
		t.Fatalf("Process should skip (return nil) on scope error, got: %v", err)
	}
	if commenter.prCommentBody != "" {
		t.Errorf("comment posted despite unresolved build scope: %q", commenter.prCommentBody)
	}
	if analyzer.calls != 0 {
		t.Errorf("LLM invoked despite skip; calls=%d", analyzer.calls)
	}
	// "scope unavailable" must NOT be read as "PR no longer touches this
	// Kustomization": the no-match is non-definitive, so no destructive
	// de-scope (comment/label removal) may fire.
	if len(commenter.removeCalls) != 0 {
		t.Errorf("destructive de-scope fired on a scope-unavailable event: %+v", commenter.removeCalls)
	}
}

// TestProcess_ScopeGate_InPlaceRemovalAttributedDespiteScopeFailure proves that
// when the build scope can't be resolved, DELETIONS are still processed — not
// only `removed` files but also an IN-PLACE removal (a resource edited out of a
// still-present `modified` file). That deletion is attributed from prune
// evidence, which doesn't need the build scope, so the modified file must reach
// the in-place diff even though its present-file attribution is suppressed.
func TestProcess_ScopeGate_InPlaceRemovalAttributedDespiteScopeFailure(t *testing.T) {
	const policyPath = "apps/kyverno/base/policies/require-team-label.yaml"
	basePolicy := []byte("apiVersion: kyverno.io/v1\nkind: ClusterPolicy\nmetadata:\n  name: require-team-label\n")
	commentedOut := []byte("# apiVersion: kyverno.io/v1\n# kind: ClusterPolicy\n# metadata:\n#   name: require-team-label\n")
	mcpMock := &fakeMCPClient{
		data: []byte(emptyInventoryKustomization),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("kyverno", "ClusterPolicy/require-team-label deleted"),
		},
	}
	commenterMock := &fakeCommenter{
		prNumber:  77,
		prBaseSHA: "base-sha", // required for the in-place-removal diff
		prFiles: []bridgegithub.PRFile{
			{Path: policyPath, BlobSHA: "pol-head", Status: "modified"},
		},
		blobs:     map[string][]byte{"pol-head": commentedOut}, // head declares nothing
		baseFiles: map[string][]byte{policyPath: basePolicy},   // base declared the ClusterPolicy
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	// Scope resolution fails (revision mismatch) — present-file attribution is
	// suppressed, but the in-place removal must still be attributed via prune.
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default()).
		WithScopeResolver(fakeScope{err: bridgesource.ErrRevisionMismatch})
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "kyverno", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-inplace"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 77)
	if !ok {
		t.Fatalf("in-place removal was NOT attributed under scope failure; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeSuccess {
		t.Errorf("in-place-removal-under-scope-failure label = %q, want success", got)
	}
}

// The pure-deletion counterpart: a PR whose only file is `removed` is attributed
// from prune evidence alone, so an unresolvable build scope (e.g. the source
// advanced past the event's revision) must not drop it.
func TestProcess_ScopeGate_RemovedFileAttributedDespiteScopeFailure(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte(emptyInventoryKustomization),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("batch-jobs", "CronJob/batch-jobs/nightly-cleanup deleted"),
		},
	}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/batch-jobs/base/nightly-cleanup-cj.yaml", BlobSHA: "cj-blob", Status: "removed"},
		},
		blobs: map[string][]byte{"cj-blob": []byte(deletedCronJobBlob)},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default()).
		WithScopeResolver(fakeScope{err: bridgesource.ErrRevisionMismatch})
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-del"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 42)
	if !ok {
		t.Fatalf("pure-deletion PR was NOT attributed under scope failure; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeSuccess {
		t.Errorf("pure-deletion-under-scope-failure label = %q, want success", got)
	}
}

// A file whose type can't declare a resource (here a script) is still a build
// input when the build uses it — e.g. a configMapGenerator source. A PR that
// only edits it must be attributed, without fetching the blob; the same file
// outside the build must not.
func TestProcess_ScopeGate_SkippableBuildInputAttributes(t *testing.T) {
	for name, tc := range map[string]struct {
		build     map[string]struct{}
		wantPosts bool
	}{
		"in build":     {map[string]struct{}{"apps/blackbox-exporter/scripts/init.sh": {}}, true},
		"not in build": {map[string]struct{}{"apps/other/init.sh": {}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			commenter := &fakeCommenter{
				prNumber: 42,
				prFiles: []bridgegithub.PRFile{
					{Path: "apps/blackbox-exporter/scripts/init.sh", BlobSHA: "blob-script", Status: "modified"},
				},
				blobsErr: errBlobFetched, // any fetch would surface as a non-definitive no-match
			}
			analyzer := &fakeAnalyzer{result: "## Status\nok"}
			proc := newScopeTestProc(t, fakeScope{files: tc.build}, commenter, analyzer)
			if err := proc.Process(context.Background(), scopeTestEvent(), "abc1234"); err != nil {
				t.Fatalf("Process: %v", err)
			}
			if got := commenter.prCommentBody != ""; got != tc.wantPosts {
				t.Fatalf("comment posted = %v, want %v", got, tc.wantPosts)
			}
		})
	}
}

var errBlobFetched = errors.New("blob must not be fetched for a script")
