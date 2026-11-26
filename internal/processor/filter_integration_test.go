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

// kustomizationWithInventory is a fake Kustomization YAML carrying a
// realistic Flux inventory: a HelmRelease in the blackbox-exporter
// namespace and the namespace itself. Tests below assert that PR files
// declaring these resources match while files declaring unrelated
// resources do not — which is the entire bridge-comment-attribution
// contract after the path-based filter was dropped.
const kustomizationWithInventory = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "_blackbox-exporter__Namespace"
      - id: "blackbox-exporter_blackbox_helm.toolkit.fluxcd.io_HelmRelease"
`

// blackboxHelmRelease is the YAML a PR file declaring the matching
// HelmRelease would have. Inventory-ID for this content is
// "blackbox-exporter_blackbox_helm.toolkit.fluxcd.io_HelmRelease" — same
// as the kustomization inventory above, so this file admits the PR.
const blackboxHelmRelease = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: blackbox
  namespace: blackbox-exporter
`

// unrelatedHelmRelease declares a HelmRelease for a different service.
// Its inventory ID is "other-app_other__HelmRelease" (oversimplified
// apiVersion to keep the test focused on tuple membership) — not in the
// kustomization inventory above.
const unrelatedHelmRelease = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: other
  namespace: other-app
`

// TestProcess_SkipsPRWhenFilesNotInInventory regresses the original bug:
// the bridge was posting comments to PRs that didn't change anything the
// Kustomization manages. With inventory-based attribution, a PR whose
// only changes declare resources NOT in the inventory must NOT receive a
// comment, AND the LLM must NOT run (filter is pre-LLM per design grill Q1).
func TestProcess_SkipsPRWhenFilesNotInInventory(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationWithInventory)}
	analyzerMock := &fakeAnalyzer{result: "should not be reached"}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "infra/cert-manager/release.yaml", BlobSHA: "blob-unrelated", Status: "modified"},
		},
		blobs: map[string][]byte{
			"blob-unrelated": []byte(unrelatedHelmRelease),
		},
	}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "blackbox-exporter", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("comment posted to a PR that doesn't declare anything in this Kustomization's inventory. body=%q", commenterMock.prCommentBody)
	}
	if analyzerMock.calls != 0 {
		t.Errorf("LLM invoked despite no PR surviving the inventory filter. calls=%d", analyzerMock.calls)
	}
}

// TestProcess_PostsWhenFileDeclaresInventoryResource is the positive
// counterpart: same Kustomization, but the PR's file content declares a
// resource that's IN the inventory → admit + post.
func TestProcess_PostsWhenFileDeclaresInventoryResource(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationWithInventory)}
	analyzerMock := &fakeAnalyzer{result: "## Status\nAll resources ready."}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/blackbox-exporter/release.yaml", BlobSHA: "blob-blackbox", Status: "modified"},
		},
		blobs: map[string][]byte{
			"blob-blackbox": []byte(blackboxHelmRelease),
		},
	}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "blackbox-exporter", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody == "" {
		t.Error("expected a comment when PR file declares a resource in inventory")
	}
	if commenterMock.prCommentKustomization != "flux-system/blackbox-exporter" {
		t.Errorf("comment scope = %q, want flux-system/blackbox-exporter", commenterMock.prCommentKustomization)
	}
}

// TestProcess_AutodevBatch_OnlyTargetsPRDeclaringInventoryResource is the
// comprehensive regression test mirroring the user's scenario: 3 PRs at one
// autodev SHA, only one PR's files declare a resource the Kustomization
// manages. That one PR — and only that one — gets a comment. The LLM runs
// once.
func TestProcess_AutodevBatch_OnlyTargetsPRDeclaringInventoryResource(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationWithInventory)}
	analyzerMock := &fakeAnalyzer{result: "## Status\nNot healthy.\n## Root Cause\nx\n## Next steps to fix\n1. y"}

	commenterMock := &fakeCommenter{
		prs: []bridgegithub.PRInfo{
			{Number: 101, State: "open"},
			{Number: 102, State: "open"}, // declares the matching HelmRelease
			{Number: 103, State: "open"},
		},
		filesByPR: map[int][]bridgegithub.PRFile{
			101: {{Path: "infra/cert-manager/release.yaml", BlobSHA: "blob-unrelated"}},
			102: {{Path: "apps/blackbox-exporter/release.yaml", BlobSHA: "blob-blackbox"}},
			103: {{Path: "apps/api/deployment.yaml", BlobSHA: "blob-unrelated-2"}},
		},
		blobs: map[string][]byte{
			"blob-unrelated":   []byte(unrelatedHelmRelease),
			"blob-blackbox":    []byte(blackboxHelmRelease),
			"blob-unrelated-2": []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\n  namespace: apps\n"),
		},
	}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "blackbox-exporter", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if analyzerMock.calls != 1 {
		t.Errorf("LLM Analyze() called %d time(s), want 1", analyzerMock.calls)
	}
	if got := len(commenterMock.posts); got != 1 {
		t.Fatalf("PostOrUpdateComment calls = %d, want 1; posts=%+v", got, commenterMock.posts)
	}
	if commenterMock.posts[0].Number != 102 {
		t.Errorf("comment posted on PR %d, want 102 (the only PR declaring an inventory resource)", commenterMock.posts[0].Number)
	}
}

// TestProcess_UmbrellaKustomization_DoesNotMatchHelmReleasePR pins the
// umbrella case: an umbrella Kustomization whose inventory is exclusively
// child Kustomizations must NOT receive a comment when a PR changes a
// HelmRelease (or any non-Kustomization resource). The child Kustomizations
// fire their own events with the right granularity.
//
// This is the inventory-based fix replacing the older path-overlap fix:
// even if the umbrella's spec.path "contains" the PR file by prefix, the
// PR's resources aren't in the umbrella's inventory, so we don't comment.
func TestProcess_UmbrellaKustomization_DoesNotMatchHelmReleasePR(t *testing.T) {
	const umbrellaKustomization = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "flux-system_child-a-crds_kustomize.toolkit.fluxcd.io_Kustomization"
      - id: "flux-system_child-b-crds_kustomize.toolkit.fluxcd.io_Kustomization"
      - id: "flux-system_child-c-crds_kustomize.toolkit.fluxcd.io_Kustomization"
`
	mcpMock := &fakeMCPClient{data: []byte(umbrellaKustomization)}
	analyzerMock := &fakeAnalyzer{result: "should not be reached"}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		// PR changes a HelmRelease — a resource type that doesn't appear
		// in the umbrella's inventory.
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/blackbox-exporter/release.yaml", BlobSHA: "blob-blackbox"},
		},
		blobs: map[string][]byte{
			"blob-blackbox": []byte(blackboxHelmRelease),
		},
	}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "umbrella", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("umbrella Kustomization commented on a PR that doesn't touch any of its inventory entries. body=%q", commenterMock.prCommentBody)
	}
	if analyzerMock.calls != 0 {
		t.Errorf("LLM invoked for an umbrella Kustomization with no matching PR file. calls=%d", analyzerMock.calls)
	}
}

// TestProcess_SkipsPRWithoutRequiredLabel pins the global PR_REQUIRED_LABELS
// gate. A PR that does declare an inventory resource but is missing the
// required label must be filtered out before the LLM call.
func TestProcess_SkipsPRWithoutRequiredLabel(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationWithInventory)}
	analyzerMock := &fakeAnalyzer{result: "should not be reached"}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/blackbox-exporter/release.yaml", BlobSHA: "blob-blackbox"},
		},
		blobs: map[string][]byte{
			"blob-blackbox": []byte(blackboxHelmRelease), // would match inventory
		},
		prLabels: []string{"docs", "wip"}, // missing the required label
	}

	cfg := &config.Config{
		GitHubRepo:            "owner/repo",
		LLMProvider:           "claude",
		RequiredPRLabels:      []string{"flux-review"},
		ReadinessPollInterval: time.Millisecond,
		ReadinessPollTimeout:  10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "blackbox-exporter", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("comment posted to a PR missing the required label. body=%q", commenterMock.prCommentBody)
	}
	if analyzerMock.calls != 0 {
		t.Errorf("LLM invoked despite label gate. calls=%d", analyzerMock.calls)
	}
}

// ── Follow-CR-to-target attribution ──────────────────────────────────────
//
// A PR that edits a Flux Kustomization CR (`kind: Kustomization`) must be
// attributed to the TARGET Kustomization it defines (analysed via the target's
// own reconcile event), NOT to the aggregate parent that merely applies the CR.
// Editing `Kustomization/flux-system/child` should light up `child`, not the
// aggregate `parent` (which owns many child Kustomizations, whose "all children
// Current at once" readiness bar is unrealistic).

// parentAggregateInventory is an aggregate parent whose inventory contains the
// `child` Kustomization OBJECT (the parent applies the CR).
const parentAggregateInventory = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "flux-system_child_kustomize.toolkit.fluxcd.io_Kustomization"
      - id: "flux-system_other-child_kustomize.toolkit.fluxcd.io_Kustomization"
`

// childOwnInventory is the `child` Kustomization's OWN inventory — the workloads
// it manages, NOT the `child` Kustomization object itself.
const childOwnInventory = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "_app__Namespace"
      - id: "app_app_helm.toolkit.fluxcd.io_HelmRelease"
`

// childKustomizationCR is the PR file that DEFINES the `child` Kustomization.
// Inventory ID: "flux-system_child_kustomize.toolkit.fluxcd.io_Kustomization".
const childKustomizationCR = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: child
  namespace: flux-system
`

// The aggregate parent must NOT claim a Kustomization-CR change even though the
// CR object is in its inventory — otherwise the noisy aggregate comment returns.
func TestProcess_KustomizationCR_NotAttributedToParent(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(parentAggregateInventory)}
	analyzerMock := &fakeAnalyzer{result: "should not be reached"}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "clusters/dev/child-kustomization.yaml", BlobSHA: "blob-cr", Status: "modified"},
		},
		blobs: map[string][]byte{"blob-cr": []byte(childKustomizationCR)},
	}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "parent", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("aggregate parent claimed a Kustomization-CR change; it must be attributed to the target. body=%q", commenterMock.prCommentBody)
	}
	if analyzerMock.calls != 0 {
		t.Errorf("LLM ran for the aggregate parent on a CR-only change. calls=%d", analyzerMock.calls)
	}
}

// The target Kustomization must self-match its own CR — even though the CR
// object is NOT in the target's own inventory (a Kustomization doesn't manage
// itself; the parent does). This is the follow-to-target attribution.
func TestProcess_KustomizationCR_AttributedToTargetViaSelfMatch(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(childOwnInventory)}
	analyzerMock := &fakeAnalyzer{result: "## Status\nchild analysed."}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			{Path: "clusters/dev/child-kustomization.yaml", BlobSHA: "blob-cr", Status: "modified"},
		},
		blobs: map[string][]byte{"blob-cr": []byte(childKustomizationCR)},
	}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "child", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentKustomization != "flux-system/child" {
		t.Errorf("CR change not self-attributed to target; scope=%q want flux-system/child", commenterMock.prCommentKustomization)
	}
}

// The same follow-to-target self-match with the build-scope gate on: the
// child's CR lives in its PARENT's build, never in the child's own (spec.path
// ./apps/web), so the gate must not reject the file before the self-match runs.
// An unrelated workload file outside the build still doesn't attribute (see
// TestProcess_ScopeGate_ExcludesFileNotInBuild).
func TestProcess_KustomizationCR_SelfMatchOutsideItsBuild(t *testing.T) {
	for name, tc := range map[string]struct {
		blob string
		want string
	}{
		"own CR":     {blob: childKustomizationCR, want: "flux-system/child"},
		"foreign CR": {blob: strings.Replace(childKustomizationCR, "name: child", "name: sibling", 1)},
		// Declares a resource in the child's inventory, but from outside its
		// build: still not attributed.
		"workload": {blob: "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: app\n  namespace: app\n"},
	} {
		t.Run(name, func(t *testing.T) {
			mcpMock := &fakeMCPClient{data: []byte(childOwnInventory)}
			commenterMock := &fakeCommenter{
				prNumber: 42,
				prFiles:  []bridgegithub.PRFile{{Path: "clusters/dev/child-kustomization.yaml", BlobSHA: "blob-cr", Status: "modified"}},
				blobs:    map[string][]byte{"blob-cr": []byte(tc.blob)},
			}
			cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", CommentOnSuccess: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
			proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default()).
				WithScopeResolver(fakeScope{files: map[string]struct{}{"apps/web/release.yaml": {}}})
			event := eventv1.Event{
				InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "child", Namespace: "flux-system"},
				Severity:       eventv1.EventSeverityInfo,
				Reason:         "ReconciliationSucceeded",
			}
			if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if commenterMock.prCommentKustomization != tc.want {
				t.Errorf("scope=%q, want %q", commenterMock.prCommentKustomization, tc.want)
			}
		})
	}
}
