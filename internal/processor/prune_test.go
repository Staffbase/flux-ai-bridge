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

func TestPrunedSubjectFromLine(t *testing.T) {
	cases := []struct {
		line string
		want string
		ok   bool
	}{
		{"CronJob/batch-jobs/nightly-cleanup deleted", "CronJob/batch-jobs/nightly-cleanup", true},
		{"  ConfigMap/ns/cm deleted  ", "ConfigMap/ns/cm", true},                             // surrounding whitespace
		{"ClusterRole/my-role deleted", "ClusterRole/my-role", true},                         // cluster-scoped (2 tokens)
		{"Kustomization/flux-system/child deleted", "Kustomization/flux-system/child", true}, // case (b)
		{"Service/ns/svc configured", "", false},                                             // not a deletion
		{"CronJob/ns/job unchanged", "", false},                                              // not a deletion
		{"the old job was deleted", "", false},                                               // prose, not a subject
		{"cronjob/ns/name deleted", "", false},                                               // lowercase kind → reject
		{"/ns/name deleted", "", false},                                                      // empty kind token
		{"CronJob/ns/name/extra deleted", "", false},                                         // too many tokens
		{"OnlyKind deleted", "", false},                                                      // single token
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := prunedSubjectFromLine(c.line)
		if ok != c.ok || got != c.want {
			t.Errorf("prunedSubjectFromLine(%q) = (%q, %v), want (%q, %v)", c.line, got, ok, c.want, c.ok)
		}
	}
}

func TestSubjectKeyFromInventoryID(t *testing.T) {
	cases := []struct {
		id   string
		want string
		ok   bool
	}{
		{"batch-jobs_nightly-cleanup_batch_CronJob", "CronJob/batch-jobs/nightly-cleanup", true},
		{"flux-system_child_kustomize.toolkit.fluxcd.io_Kustomization", "Kustomization/flux-system/child", true},
		{"_my-role_rbac.authorization.k8s.io_ClusterRole", "ClusterRole/my-role", true}, // cluster-scoped → no namespace segment
		{"too_few_parts", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := subjectKeyFromInventoryID(c.id)
		if ok != c.ok || got != c.want {
			t.Errorf("subjectKeyFromInventoryID(%q) = (%q, %v), want (%q, %v)", c.id, got, ok, c.want, c.ok)
		}
	}
}

func TestParsePrunedSubjects(t *testing.T) {
	// Two prune events on different Kustomizations, plus an apply (configured)
	// event and a wrong-shape (Warning) event — both must be ignored. Only the
	// subjects deleted by THIS Kustomization (batch-jobs) via a real prune
	// event should survive.
	events := []eventRecord{
		{
			Type:    "Normal",
			Reason:  "ReconciliationSucceeded",
			Message: "CronJob/batch-jobs/nightly-cleanup deleted\nConfigMap/batch-jobs/old-cm deleted",
		},
		{ // same event source but apply, not prune
			Reason:  "ReconciliationSucceeded",
			Message: "ConfigMap/batch-jobs/kept-cm configured",
		},
		{ // a DIFFERENT Kustomization's prune — must not leak in
			Reason:  "ReconciliationSucceeded",
			Message: "Deployment/other/app deleted",
		},
		{ // right Kustomization, but a Warning/non-success event — must NOT match
			Type:    "Warning",
			Reason:  "HealthCheckFailed",
			Message: "ConfigMap/batch-jobs/should-not-leak deleted",
		},
		{ // right object/shape, but emitted by ANOTHER component — must NOT match
			Type:    "Normal",
			Reason:  "ReconciliationSucceeded",
			Message: "ConfigMap/batch-jobs/foreign-emitter deleted",
		},
		{ // right shape and a deleted line, but NO emitter set — an incidental /
			// emitter-less Event must NOT match (rejected by the emitter guard)
			Type:    "Normal",
			Reason:  "ReconciliationSucceeded",
			Message: "ConfigMap/batch-jobs/spoofed deleted",
		},
	}
	events[0].InvolvedObject.Kind = "Kustomization"
	events[0].InvolvedObject.Name = "batch-jobs"
	events[0].Source.Component = "kustomize-controller" // real prune events name this emitter
	events[1].InvolvedObject.Kind = "Kustomization"
	events[1].InvolvedObject.Name = "batch-jobs"
	events[1].Source.Component = "kustomize-controller"
	events[2].InvolvedObject.Kind = "Kustomization"
	events[2].InvolvedObject.Name = "other-kustomization"
	events[2].Source.Component = "kustomize-controller"
	events[3].InvolvedObject.Kind = "Kustomization"
	events[3].InvolvedObject.Name = "batch-jobs"
	events[4].InvolvedObject.Kind = "Kustomization"
	events[4].InvolvedObject.Name = "batch-jobs"
	events[4].Source.Component = "some-other-operator"
	events[5].InvolvedObject.Kind = "Kustomization"
	events[5].InvolvedObject.Name = "batch-jobs"
	// events[5] deliberately leaves both component fields empty.

	got := parsePrunedSubjects(events, "batch-jobs")
	want := map[string]struct{}{
		"CronJob/batch-jobs/nightly-cleanup": {},
		"ConfigMap/batch-jobs/old-cm":        {},
	}
	if len(got) != len(want) {
		t.Fatalf("parsePrunedSubjects size = %d (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("parsePrunedSubjects missing %q; got %v", k, got)
		}
	}
	if _, leaked := got["ConfigMap/batch-jobs/should-not-leak"]; leaked {
		t.Error("a Warning/non-success event's deleted subject leaked into the prune set")
	}
	if _, leaked := got["ConfigMap/batch-jobs/foreign-emitter"]; leaked {
		t.Error("a non-kustomize-controller event's deleted subject leaked into the prune set")
	}
	if _, leaked := got["ConfigMap/batch-jobs/spoofed"]; leaked {
		t.Error("an emitter-less event's deleted subject leaked into the prune set")
	}
}

// A prune during a reconcile carries reason "Progressing": kustomize-controller
// marks Ready Unknown/Progressing before it applies and prunes, and the event
// reason is the Ready reason at emission time. A finalizer prune carries
// whatever Ready said last (e.g. "ArtifactFailed") and isn't a reconcile's
// prune, so it stays out.
func TestParsePrunedSubjects_ProgressingReason(t *testing.T) {
	events := []eventRecord{
		{Type: "Normal", Reason: "Progressing", Message: "Kustomization/flux-system/legacy-app deleted"},
		{Type: "Normal", Reason: "ArtifactFailed", Message: "CustomResourceDefinition/widgets.example.com deleted"},
	}
	for i := range events {
		events[i].InvolvedObject.Kind = "Kustomization"
		events[i].InvolvedObject.Name = "cluster"
		events[i].Source.Component = "kustomize-controller"
	}
	got := parsePrunedSubjects(events, "cluster")
	if _, ok := got["Kustomization/flux-system/legacy-app"]; !ok {
		t.Errorf("Progressing prune event not parsed; got %v", got)
	}
	if _, leaked := got["CustomResourceDefinition/widgets.example.com"]; leaked {
		t.Errorf("finalizer prune event leaked into the prune set; got %v", got)
	}
}

// Only the NEWEST prune Event's subjects count: a stale prune Event still inside
// the namespace's Event-retention window (a prior reconcile's prune) must not
// seed the set, or it would attribute a PR off a deletion THIS reconcile didn't
// make.
func TestParsePrunedSubjects_OnlyNewestEvent(t *testing.T) {
	mk := func(ts, message string) eventRecord {
		var e eventRecord
		e.Type = "Normal"
		e.Reason = "ReconciliationSucceeded"
		e.Source.Component = "kustomize-controller"
		e.LastTimestamp = ts
		e.Message = message
		e.InvolvedObject.Kind = "Kustomization"
		e.InvolvedObject.Name = "app"
		return e
	}
	events := []eventRecord{
		mk("2026-06-18T07:00:00Z", "ConfigMap/app/stale-pruned deleted"), // older reconcile
		mk("2026-06-18T07:30:00Z", "ConfigMap/app/fresh-pruned deleted"), // current reconcile
	}
	got := parsePrunedSubjects(events, "app")
	if _, ok := got["ConfigMap/app/fresh-pruned"]; !ok {
		t.Errorf("newest prune subject missing; got %v", got)
	}
	if _, leaked := got["ConfigMap/app/stale-pruned"]; leaked {
		t.Errorf("a stale prior-reconcile prune subject leaked into the set; got %v", got)
	}
	if len(got) != 1 {
		t.Errorf("expected exactly the newest event's 1 subject, got %v", got)
	}

	// Same-timestamp events (one reconcile reported across two records, or a tie)
	// are unioned, not dropped.
	tie := []eventRecord{
		mk("2026-06-18T07:30:00Z", "ConfigMap/app/a deleted"),
		mk("2026-06-18T07:30:00Z", "ConfigMap/app/b deleted"),
	}
	gotTie := parsePrunedSubjects(tie, "app")
	if len(gotTie) != 2 {
		t.Errorf("equal-timestamp prune events should union; got %v", gotTie)
	}
}

func TestInventoryIDWasPruned(t *testing.T) {
	set := pruneSetProvider(func() map[string]struct{} {
		return map[string]struct{}{"CronJob/batch-jobs/nightly-cleanup": {}}
	})
	if inventoryIDWasPruned(nil, "batch-jobs_nightly-cleanup_batch_CronJob") {
		t.Error("nil provider must report not-pruned")
	}
	if !inventoryIDWasPruned(set, "batch-jobs_nightly-cleanup_batch_CronJob") {
		t.Error("expected pruned match on the CronJob")
	}
	if inventoryIDWasPruned(set, "batch-jobs_other_batch_CronJob") {
		t.Error("unexpected match on a different resource")
	}
	if inventoryIDWasPruned(set, "bad-id") {
		t.Error("unparseable id must not match")
	}
}

// --- integration tests via Process ---

const emptyInventoryKustomization = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries: []
`

const deletedCronJobBlob = `apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-cleanup
  namespace: batch-jobs
`

func eventsListYAML(involvedName, message string) []byte {
	return []byte("items:\n" +
		"  - involvedObject:\n" +
		"      kind: Kustomization\n" +
		"      name: " + involvedName + "\n" +
		"    type: Normal\n" +
		"    reason: Progressing\n" +
		"    source:\n" +
		"      component: kustomize-controller\n" +
		"    message: '" + message + "'\n")
}

func lastLabel(calls []labelCall, pr int) (bridgegithub.Outcome, bool) {
	var out bridgegithub.Outcome
	found := false
	for _, c := range calls {
		if c.Number == pr {
			out = c.Outcome
			found = true
		}
	}
	return out, found
}

// Case (a): a pure-deletion PR whose CronJob is already gone from the inventory
// is attributed by the prune event and earns flux-success over the (now empty)
// remaining inventory.
func TestProcess_AttributesPrunedDeletionPR(t *testing.T) {
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
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-del"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 42)
	if !ok {
		t.Fatalf("expected a label on the pruned deletion PR; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeSuccess {
		t.Errorf("pruned deletion PR label = %q, want success", got)
	}
}

// In-place removal: the resource is removed by EDITING a still-present file
// (here the ClusterPolicy is commented out, so the file is `modified`, not
// `removed`, and its head blob declares nothing). The bridge must diff the
// file's base version against head, see the ClusterPolicy was removed, attribute
// the PR via the prune event, and earn flux-success over the remaining (empty)
// inventory.
func TestProcess_AttributesInPlaceResourceRemoval(t *testing.T) {
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
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "kyverno", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-inplace"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 77)
	if !ok {
		t.Fatalf("expected a label on the in-place-removal PR; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeSuccess {
		t.Errorf("in-place-removal PR label = %q, want success", got)
	}
}

// A head the parser couldn't read completely (the policy now wrapped in a
// List's items) still declares it, as far as anyone can tell: absence from the
// parsed part is no removal, so the base diff must not attribute the PR.
func TestProcess_InPlaceRemovalNotClaimedFromIncompleteHead(t *testing.T) {
	const policyPath = "apps/kyverno/base/policies/require-team-label.yaml"
	basePolicy := []byte("apiVersion: kyverno.io/v1\nkind: ClusterPolicy\nmetadata:\n  name: require-team-label\n")
	wrapped := []byte("apiVersion: v1\nkind: List\nitems:\n- apiVersion: kyverno.io/v1\n  kind: ClusterPolicy\n  metadata:\n    name: require-team-label\n")
	mcpMock := &fakeMCPClient{
		data: []byte(emptyInventoryKustomization),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("kyverno", "ClusterPolicy/require-team-label deleted"),
		},
	}
	commenterMock := &fakeCommenter{
		prNumber:  77,
		prBaseSHA: "base-sha",
		prFiles:   []bridgegithub.PRFile{{Path: policyPath, BlobSHA: "pol-head", Status: "modified"}},
		blobs:     map[string][]byte{"pol-head": wrapped},
		baseFiles: map[string][]byte{policyPath: basePolicy},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "kyverno", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-inplace"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if _, ok := lastLabel(commenterMock.labelCalls, 77); ok {
		t.Errorf("a removal was claimed from an incompletely parsed head; labelCalls=%v", commenterMock.labelCalls)
	}
}

// Gating: an in-place removal with NO prune event in the window is NOT attributed
// (the removed resource isn't confirmed gone), so no label is set — mirrors the
// removed-file gating, proving the base-diff fallback still requires prune
// evidence to attribute.
func TestProcess_InPlaceRemovalNotAttributedWithoutPruneEvent(t *testing.T) {
	const policyPath = "apps/kyverno/base/policies/require-team-label.yaml"
	basePolicy := []byte("apiVersion: kyverno.io/v1\nkind: ClusterPolicy\nmetadata:\n  name: require-team-label\n")
	commentedOut := []byte("# apiVersion: kyverno.io/v1\n# kind: ClusterPolicy\n")
	mcpMock := &fakeMCPClient{
		data:  []byte(emptyInventoryKustomization),
		lists: map[string][]byte{"v1|Event|flux-system": []byte("items: []\n")}, // no prune event
	}
	commenterMock := &fakeCommenter{
		prNumber:  77,
		prBaseSHA: "base-sha",
		prFiles:   []bridgegithub.PRFile{{Path: policyPath, BlobSHA: "pol-head", Status: "modified"}},
		blobs:     map[string][]byte{"pol-head": commentedOut},
		baseFiles: map[string][]byte{policyPath: basePolicy},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "kyverno", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-inplace"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if _, ok := lastLabel(commenterMock.labelCalls, 77); ok {
		t.Errorf("in-place removal without a prune event was attributed; labelCalls=%v", commenterMock.labelCalls)
	}
}

// Mixed PR, attributed via a PRESENT match, that ALSO removes a resource
// in-place (a second file commented out) whose teardown is finalizer-stuck. The
// present match is found FIRST — which used to skip the later modified file — so
// the in-place removal must still be diffed and its still-Terminating resource
// folded into the verdict, keeping it ⚠️ instead of a premature ✅.
func TestProcess_MixedPR_InPlaceRemovalStuckIsWarning(t *testing.T) {
	const inventoryWithKeptCM = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "batch-jobs_kept-cm__ConfigMap"
`
	keptCM := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kept-cm\n  namespace: batch-jobs\n")
	stuckLive := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: stuck-cm
  namespace: batch-jobs
  deletionTimestamp: "2026-06-15T10:00:00Z"
  finalizers:
    - example.com/protect
`)
	mcpMock := &fakeMCPClient{
		data: []byte(inventoryWithKeptCM),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("batch-jobs", "ConfigMap/batch-jobs/stuck-cm deleted"),
		},
		resources: map[string][]byte{
			"v1|ConfigMap|kept-cm|batch-jobs":  keptCM,    // present match resource, healthy
			"v1|ConfigMap|stuck-cm|batch-jobs": stuckLive, // pruned but still Terminating
		},
	}
	commenterMock := &fakeCommenter{
		prNumber:  88,
		prBaseSHA: "base-sha",
		// Present-match file FIRST (would set presentMatch and skip the next
		// file under the old short-circuit), commented-out removal SECOND.
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/batch-jobs/base/kept-cm.yaml", BlobSHA: "kept-head", Status: "modified"},
			{Path: "apps/batch-jobs/base/stuck-cm.yaml", BlobSHA: "stuck-head", Status: "modified"},
		},
		blobs: map[string][]byte{
			"kept-head":  keptCM,                                                   // still declares the kept ConfigMap → present match
			"stuck-head": []byte("# apiVersion: v1\n# kind: ConfigMap\n# stuck\n"), // commented out → empty head
		},
		baseFiles: map[string][]byte{
			"apps/batch-jobs/base/stuck-cm.yaml": []byte(stuckConfigMapBlob), // base declared stuck-cm
		},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-mixed-inplace"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 88)
	if !ok {
		t.Fatalf("expected a label on the mixed PR; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeWarning {
		t.Errorf("mixed PR with a finalizer-stuck in-place removal label = %q, want warning", got)
	}
}

// A multi-doc file that keeps one resource and drops another: the kept one
// attributes the PR, but the dropped one was pruned and is finalizer-stuck.
// With prune candidates in this reconcile the file is diffed anyway, so the
// stuck teardown keeps the verdict ⚠️ instead of a premature ✅.
func TestProcess_MultiDocPartialRemovalStuckIsWarning(t *testing.T) {
	const inventoryWithKeptCM = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "batch-jobs_kept-cm__ConfigMap"
`
	keptCM := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kept-cm\n  namespace: batch-jobs\n"
	stuckLive := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: stuck-cm\n  namespace: batch-jobs\n" +
		"  deletionTimestamp: \"2026-06-15T10:00:00Z\"\n  finalizers:\n    - example.com/protect\n")
	const path = "apps/batch-jobs/base/configmaps.yaml"
	mcpMock := &fakeMCPClient{
		data: []byte(inventoryWithKeptCM),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("batch-jobs", "ConfigMap/batch-jobs/stuck-cm deleted"),
		},
		resources: map[string][]byte{
			"v1|ConfigMap|kept-cm|batch-jobs":  []byte(keptCM),
			"v1|ConfigMap|stuck-cm|batch-jobs": stuckLive,
		},
	}
	commenterMock := &fakeCommenter{
		prNumber:  89,
		prBaseSHA: "base-sha",
		prFiles:   []bridgegithub.PRFile{{Path: path, BlobSHA: "cms-head", Status: "modified"}},
		blobs:     map[string][]byte{"cms-head": []byte(keptCM)},
		baseFiles: map[string][]byte{path: []byte(keptCM + "---\n" + stuckConfigMapBlob)},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-multidoc"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if got, ok := lastLabel(commenterMock.labelCalls, 89); !ok || got != bridgegithub.OutcomeWarning {
		t.Errorf("multi-doc partial removal with a stuck teardown: label = %q (set=%v), want warning", got, ok)
	}
}

// Gating regression: the SAME deletion PR with NO prune event in the window is
// NOT attributed (no label, no comment) — prune attribution must not fire on a
// removed file alone, only on observed evidence the reconcile deleted it.
func TestProcess_DeletionPRNotAttributedWithoutPruneEvent(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:  []byte(emptyInventoryKustomization),
		lists: map[string][]byte{"v1|Event|flux-system": []byte("items: []\n")}, // no prune event
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
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-del"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if _, ok := lastLabel(commenterMock.labelCalls, 42); ok {
		t.Errorf("PR labelled without a prune event; labelCalls=%v", commenterMock.labelCalls)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("comment posted without a prune event: %q", commenterMock.prCommentBody)
	}
}

const deletedChildKustomizationBlob = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: legacy-app
  namespace: flux-system
`

// Case (b): a PR removing a child Kustomization CR is attributed to the PARENT
// that pruned it (the deleted child can't self-report), and the clean teardown
// earns flux-success.
func TestProcess_AttributesKustomizationRemovalToParent(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte(emptyInventoryKustomization),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("cluster", "Kustomization/flux-system/legacy-app deleted"),
		},
	}
	commenterMock := &fakeCommenter{
		prNumber: 7,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/legacy-app/legacy-app-ks.yaml", BlobSHA: "ks-blob", Status: "removed"},
		},
		blobs: map[string][]byte{"ks-blob": []byte(deletedChildKustomizationBlob)},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	// The event is the PARENT app-of-apps reconcile.
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "cluster", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-ks-del"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 7)
	if !ok {
		t.Fatalf("expected the parent to attribute the Kustomization-removal PR; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeSuccess {
		t.Errorf("Kustomization-removal PR label = %q, want success", got)
	}
}

// Regression: once the prune event ages out of the recent-Event window, a PR
// that removed a child Kustomization CR (now gone from the parent's inventory)
// must NOT be de-scoped — its ✅ comment/label, earned on the earlier
// prune-attributed reconcile, must survive. The parent's inventory is non-empty
// here (other children remain), so it's TRUSTED and the destructive cleanup
// would otherwise fire; removedAbsent must protect it.
func TestProcess_RemovedChildKustomizationNotDeScopedAfterPruneWindow(t *testing.T) {
	const parentInventoryWithoutChild = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "flux-system_other-child_kustomize.toolkit.fluxcd.io_Kustomization"
`
	mcpMock := &fakeMCPClient{
		data: []byte(parentInventoryWithoutChild),
		// Prune event has aged out: the namespace lists Events, but none assert
		// the legacy-app deletion → empty prune set.
		lists: map[string][]byte{"v1|Event|flux-system": []byte("items: []\n")},
	}
	commenterMock := &fakeCommenter{
		prNumber: 77,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/legacy-app/legacy-app-ks.yaml", BlobSHA: "ks-blob", Status: "removed"},
		},
		blobs: map[string][]byte{"ks-blob": []byte(deletedChildKustomizationBlob)},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "cluster", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-aged-out"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	for _, c := range commenterMock.removeCalls {
		if c.Number == 77 {
			t.Errorf("removed-child-Kustomization PR was de-scoped (RemoveComment called) after the prune window: %+v", commenterMock.removeCalls)
		}
	}
}

// When a PR has a `modified` file whose head declares NOTHING (a possible
// in-place full removal, e.g. a resource commented out) but NO base ref to diff
// against (e.g. a PRInfo from a pre-upgrade cache that predates BaseSHA), the
// bridge can't confirm the removal, so a "no inventory match" is NOT trustworthy
// enough to destructively de-scope — it must withhold cleanup (non-definitive)
// rather than wipe a possibly-earned ✅. A modified file that still declares
// resources is unaffected (covered by the ordinary de-scope tests).
func TestProcess_NoBaseRefWithEmptyHeadModifiedFileBlocksDeScope(t *testing.T) {
	const inventoryWithOther = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "batch-jobs_other-cm__ConfigMap"
`
	mcpMock := &fakeMCPClient{
		data:  []byte(inventoryWithOther),
		lists: map[string][]byte{"v1|Event|flux-system": []byte("items: []\n")},
	}
	commenterMock := &fakeCommenter{
		prNumber:  55,
		prBaseSHA: "", // no base ref → an in-place full removal can't be diffed
		// A modified file whose head declares NOTHING (commented out) → the
		// ambiguous case the bridge must not de-scope without a base ref.
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/batch-jobs/base/thing.yaml", BlobSHA: "thing-head", Status: "modified"},
		},
		blobs: map[string][]byte{"thing-head": []byte("# apiVersion: v1\n# kind: ConfigMap\n# (commented out)\n")},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-nobase"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	for _, c := range commenterMock.removeCalls {
		if c.Number == 55 {
			t.Errorf("PR was destructively de-scoped despite an undiffable empty-head modified file (no base ref): %+v", commenterMock.removeCalls)
		}
	}
}

const stuckConfigMapBlob = `apiVersion: v1
kind: ConfigMap
metadata:
  name: stuck-cm
  namespace: batch-jobs
`

// A pruned resource still lingering in Terminating (finalizer stuck) must keep
// the verdict non-green: the PR is attributed, but the teardown verification
// folds the Terminating resource in and the label is flux-warning, not success.
func TestProcess_FinalizerStuckTeardownIsWarning(t *testing.T) {
	stuckLive := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: stuck-cm
  namespace: batch-jobs
  deletionTimestamp: "2026-06-15T10:00:00Z"
  finalizers:
    - example.com/protect
`)
	mcpMock := &fakeMCPClient{
		data: []byte(emptyInventoryKustomization),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("batch-jobs", "ConfigMap/batch-jobs/stuck-cm deleted"),
		},
		resources: map[string][]byte{
			// The pruned ConfigMap is still present, mid-deletion.
			"v1|ConfigMap|stuck-cm|batch-jobs": stuckLive,
		},
	}
	commenterMock := &fakeCommenter{
		prNumber: 99,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/batch-jobs/base/stuck-cm.yaml", BlobSHA: "cm-blob", Status: "removed"},
		},
		blobs: map[string][]byte{"cm-blob": []byte(stuckConfigMapBlob)},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-stuck"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 99)
	if !ok {
		t.Fatalf("expected a label on the stuck-teardown PR; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeWarning {
		t.Errorf("finalizer-stuck teardown label = %q, want warning", got)
	}
}

// A pruned resource that is still fully present (reads Current — the delete
// hasn't taken effect) must keep the verdict non-green: the teardown isn't
// complete, so the label is flux-warning, not a premature flux-success.
func TestProcess_StillCurrentPrunedResourceIsWarning(t *testing.T) {
	presentLive := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: present-cm
  namespace: batch-jobs
`)
	mcpMock := &fakeMCPClient{
		data: []byte(emptyInventoryKustomization),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("batch-jobs", "ConfigMap/batch-jobs/present-cm deleted"),
		},
		resources: map[string][]byte{
			// The pruned ConfigMap is still fully present (delete not yet applied).
			"v1|ConfigMap|present-cm|batch-jobs": presentLive,
		},
	}
	commenterMock := &fakeCommenter{
		prNumber: 101,
		prFiles: []bridgegithub.PRFile{
			{Path: "apps/batch-jobs/base/present-cm.yaml", BlobSHA: "cm-blob", Status: "removed"},
		},
		blobs: map[string][]byte{"cm-blob": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: present-cm\n  namespace: batch-jobs\n")},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "batch-jobs", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-present"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 101)
	if !ok {
		t.Fatalf("expected a label on the not-yet-deleted teardown PR; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeWarning {
		t.Errorf("still-Current pruned resource label = %q, want warning", got)
	}
	// The synthetic teardown row must NOT render "✅ Current" (which reads as
	// healthy); it's re-rendered as Terminating with the observed status kept in
	// the message.
	body := commenterMock.prCommentBody
	if strings.Contains(body, "✅ Current") {
		t.Errorf("still-Current pruned resource rendered as a healthy row:\n%s", body)
	}
	if !strings.Contains(body, "pruned but still present") || !strings.Contains(body, "observed Current") {
		t.Errorf("teardown row missing annotation / observed-status note:\n%s", body)
	}
}

// Mixed PR: it touches a still-present inventory resource AND removes a pruned
// resource that's finalizer-stuck. The present-resource match must NOT short-
// circuit away the pruned ID — the stuck teardown still has to be verified, so
// the verdict is flux-warning, not a premature flux-success. (The present file
// is ordered FIRST so the pre-fix early-return would have dropped the pruned ID.)
func TestProcess_MixedPR_PresentMatchDoesNotDropPrunedDeletion(t *testing.T) {
	const mixedKustomization = `
spec:
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  inventory:
    entries:
      - id: "ns_kept__ConfigMap"
`
	stuckLive := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: stuck
  namespace: ns
  deletionTimestamp: "2026-06-16T10:00:00Z"
  finalizers:
    - example.com/protect
`)
	mcpMock := &fakeMCPClient{
		data: []byte(mixedKustomization),
		lists: map[string][]byte{
			"v1|Event|flux-system": eventsListYAML("app-ks", "ConfigMap/ns/stuck deleted"),
		},
		resources: map[string][]byte{
			"v1|ConfigMap|kept|ns":  []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kept\n  namespace: ns\n"),
			"v1|ConfigMap|stuck|ns": stuckLive,
		},
	}
	commenterMock := &fakeCommenter{
		prNumber: 202,
		prFiles: []bridgegithub.PRFile{
			{Path: "kept.yaml", BlobSHA: "kept", Status: "modified"},  // present match, scanned first
			{Path: "stuck.yaml", BlobSHA: "stuck", Status: "removed"}, // pruned + finalizer-stuck
		},
		blobs: map[string][]byte{
			"kept":  []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kept\n  namespace: ns\n"),
			"stuck": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: stuck\n  namespace: ns\n"),
		},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		PRLabelEnabled: true, PRAttributePrunes: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "app-ks", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo, Reason: "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "sha-mixed"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	got, ok := lastLabel(commenterMock.labelCalls, 202)
	if !ok {
		t.Fatalf("expected a label on the mixed PR; labelCalls=%v", commenterMock.labelCalls)
	}
	if got != bridgegithub.OutcomeWarning {
		t.Errorf("mixed PR with finalizer-stuck pruned deletion label = %q, want warning", got)
	}
}
