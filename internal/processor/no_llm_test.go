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
	"fmt"
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

// TestPromptKustomizationYAML_StripsInventory pins the LLM-prompt trim: the
// status.inventory enumeration (token noise with no root-cause signal) is
// removed, the diagnostic fields (spec, status.conditions) are kept, and a
// one-line scale summary replaces the entries.
func TestPromptKustomizationYAML_StripsInventory(t *testing.T) {
	raw := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: grafana
  namespace: flux-system
spec:
  path: ./apps/grafana
  dependsOn:
    - name: kube-prometheus-stack
status:
  lastAppliedRevision: main@sha1:abc123
  conditions:
    - type: Ready
      status: "False"
      reason: HealthCheckFailed
      message: "Health check failed for Deployment/grafana/grafana"
  inventory:
    entries:
      - id: grafana_dashboards__ConfigMap
        v: v1
      - id: kube-system_coredns__ConfigMap
        v: v1
      - id: monitoring_prom__ConfigMap
        v: v1
`
	refs := []string{"grafana_dashboards__ConfigMap", "kube-system_coredns__ConfigMap", "monitoring_prom__ConfigMap"}
	got := promptKustomizationYAML(raw, refs)

	// Inventory IDs must be gone.
	for _, id := range refs {
		if strings.Contains(got, id) {
			t.Errorf("inventory id %q should be stripped from prompt YAML: %q", id, got)
		}
	}
	if strings.Contains(got, "entries:") {
		t.Errorf("inventory entries should be stripped: %q", got)
	}
	// Diagnostic fields must remain.
	for _, want := range []string{"./apps/grafana", "kube-prometheus-stack", "HealthCheckFailed", "Health check failed for Deployment/grafana/grafana", "main@sha1:abc123"} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %q to be retained in prompt YAML: %q", want, got)
		}
	}
	// Scale summary present (3 resources, 3 namespaces).
	if !strings.Contains(got, "status.inventory omitted: 3 resource(s) across 3 namespace(s)") {
		t.Errorf("expected inventory scale summary line: %q", got)
	}
}

// TestPromptKustomizationYAML_FailOpen verifies the helper returns the input
// unchanged when there's nothing to trim or the YAML doesn't parse — a
// slightly bloated prompt beats a broken one.
func TestPromptKustomizationYAML_FailOpen(t *testing.T) {
	noInv := "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nstatus:\n  lastAppliedRevision: x\n"
	if got := promptKustomizationYAML(noInv, nil); got != noInv {
		t.Errorf("no-inventory YAML should pass through unchanged, got: %q", got)
	}
	malformed := "\tthis is: not\n  valid yaml: ["
	if got := promptKustomizationYAML(malformed, nil); got != malformed {
		t.Errorf("malformed YAML should pass through unchanged, got: %q", got)
	}
}

// TestBuildKstatusBody_HealthyPicksGreenMarker exercises the all-Current
// path with the default summary-on configuration. The body opens with the
// ✅ marker (lifted into the header by formatComment) followed by a
// headline summary embedding the kstatus README link, then the Resource
// Status table.
func TestBuildKstatusBody_HealthyPicksGreenMarker(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "apps", Name: "api", Status: "Current", Message: "ready"},
		{Kind: "Deployment", Namespace: "apps", Name: "worker", Status: "Current", Message: "ready"},
	}
	got := buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", nil)
	if !strings.HasPrefix(got, statusEmojiHealthy+"\n") {
		t.Errorf("expected ✅ marker on first line, got: %q", got)
	}
	if !strings.Contains(got, kstatusReadmeURL) {
		t.Errorf("expected kstatus README URL on healthy summary, got: %q", got)
	}
	if !strings.Contains(got, "## Resource Status") {
		t.Errorf("missing Resource Status section: %q", got)
	}
	if !strings.Contains(got, "| apps | Deployment | api | ✅ Current | ready |") {
		t.Errorf("missing healthy api row: %q", got)
	}
}

// TestBuildKstatusBody_AllCurrentSummaryIsReasonAware: with every row Current
// and settled, the headline must not claim an inventory "isn't fully visible
// yet" for a release that merely couldn't be inspected — a fetch/parse
// failure is no evidence of an incomplete inventory.
func TestBuildKstatusBody_AllCurrentSummaryIsReasonAware(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "apps", Name: "api", Status: "Current", Message: "ready"},
	}
	unverified := []pendingHelmRelease{{Name: "apps/app", Reason: helmReleaseReasonUnverified}}
	got := buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", unverified)
	if strings.Contains(got, "isn't fully visible yet") {
		t.Errorf("unverified release must not get the install-pending headline: %q", got)
	}
	if !strings.Contains(got, "a HelmRelease could not be inspected") {
		t.Errorf("expected inspection-failure headline, got: %q", got)
	}

	installPending := []pendingHelmRelease{{Name: "apps/app", Reason: helmReleaseReasonInstallPending}}
	got = buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", installPending)
	if !strings.Contains(got, "isn't fully visible yet") {
		t.Errorf("install-pending release should keep its headline, got: %q", got)
	}
}

// TestBuildKstatusBody_FailedOnly_FiltersTable asserts the failedResourcesOnly
// mode: only non-Current rows are tabled; every Current row — including a
// Current `Namespace` row — is dropped (the namespace is already in each failing
// row's Namespace column, so a dedicated Namespace row is redundant). The
// overall marker still reflects the full inventory.
func TestBuildKstatusBody_FailedOnly_FiltersTable(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Namespace", Namespace: "", Name: "test", Status: "Current", Message: "ok"},
		{Kind: "Deployment", Namespace: "test", Name: "api", Status: "Current", Message: "ready"},
		{Kind: "HelmRelease", Namespace: "test", Name: "podinfo", Status: "Failed", Message: "InstallFailed"},
	}
	got := buildKstatusBody(statuses, nil, false, true, false, true /* failedOnly */, "", "", "", nil)
	if !strings.HasPrefix(got, statusEmojiFailure+"\n") {
		t.Errorf("overall marker must still reflect the full inventory (❌): %q", got)
	}
	if !strings.Contains(got, "| test | HelmRelease | podinfo | ❌ Failed | InstallFailed |") {
		t.Errorf("failed row must be shown: %q", got)
	}
	// The Current Namespace row is now dropped — the namespace shows in the
	// failing rows' Namespace column already.
	if strings.Contains(got, "| Namespace | test |") {
		t.Errorf("Current Namespace row should be dropped in failedOnly mode: %q", got)
	}
	// The healthy Deployment row must also be filtered out.
	if strings.Contains(got, "| test | Deployment | api |") {
		t.Errorf("Current rows must be filtered out in failedOnly mode: %q", got)
	}
}

// TestBuildKstatusBody_FailedOnly_AllCurrentCollapses asserts that when every
// resource is Current the per-resource rows collapse to a single compact
// summary row (table format kept) whose first column is the Kustomization
// (the comment's identity), not a namespace list.
func TestBuildKstatusBody_FailedOnly_AllCurrentCollapses(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Namespace", Namespace: "", Name: "test", Status: "Current", Message: "ok"},
		{Kind: "Deployment", Namespace: "test", Name: "api", Status: "Current", Message: "ready"},
		{Kind: "Service", Namespace: "test", Name: "api", Status: "Current", Message: "ready"},
	}
	got := buildKstatusBody(statuses, nil, false, true, false, true /* failedOnly */, "apps", "", "", nil)
	if !strings.HasPrefix(got, statusEmojiHealthy+"\n") {
		t.Errorf("expected ✅ marker, got: %q", got)
	}
	// Compact summary table (kept the table format) keyed by Kustomization + count.
	if !strings.Contains(got, "| Kustomization | Resources | Status |") {
		t.Errorf("expected the compact summary table header keyed by Kustomization: %q", got)
	}
	if !strings.Contains(got, "| apps | 3 | "+statusEmojiHealthy+" Current |") {
		t.Errorf("expected a one-row summary naming the Kustomization + count: %q", got)
	}
	// The per-resource detail rows must NOT appear.
	if strings.Contains(got, "| test | Deployment | api |") {
		t.Errorf("per-resource rows should be collapsed when all Current: %q", got)
	}
}

// TestBuildKstatusBody_HealthyHeadlineLedByKustomization pins that the healthy
// summary line is led by the Kustomization identity and states the whole-
// inventory count (no per-PR scoping).
func TestBuildKstatusBody_HealthyHeadlineLedByKustomization(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "GrafanaDashboard", Namespace: "grafana", Name: "slo-detail", Status: "Current"},
		{Kind: "GrafanaDashboard", Namespace: "grafana", Name: "slo-overview", Status: "Current"},
	}
	got := buildKstatusBody(statuses, nil, false, true, true /* includeSummary */, false, "grafana", "", "", nil)
	if !strings.Contains(got, "Kustomization `grafana`: All 2 resource(s) reached") {
		t.Errorf("expected headline led by the Kustomization with the full count, got: %q", got)
	}
	if strings.Contains(got, "scoped to this PR") {
		t.Errorf("whole-inventory report must not carry a 'scoped to this PR' note: %q", got)
	}
}

// TestBuildKstatusBody_SummaryRowKeyedByKustomization pins that the compact
// summary row's first column is the Kustomization name regardless of resource
// scoping — including the case the namespace approach couldn't handle, where
// every resource is cluster-scoped (e.g. Kyverno ClusterPolicies have no
// namespace). Empty scope renders "—".
func TestBuildKstatusBody_SummaryRowKeyedByKustomization(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "ClusterPolicy", Namespace: "", Name: "require-emptydir-sizelimit", Status: "Current"},
		{Kind: "ClusterPolicy", Namespace: "", Name: "require-root-filesystem-readonly", Status: "Current"},
	}
	got := buildKstatusBody(statuses, nil, false, true, false, true /* failedOnly */, "kyverno", "", "", nil)
	if !strings.Contains(got, "| kyverno | 2 | "+statusEmojiHealthy+" Current |") {
		t.Errorf("summary row should be keyed by the Kustomization name, got: %q", got)
	}
	if strings.Contains(got, "| — |") {
		t.Errorf("cell should be the Kustomization, not a bare dash, when scope is set: %q", got)
	}

	// Empty scope renders "—" (nothing to show).
	gotNoScope := buildKstatusBody(statuses, nil, false, true, false, true, "", "", "", nil)
	if !strings.Contains(gotNoScope, "| — | 2 | "+statusEmojiHealthy+" Current |") {
		t.Errorf("with no scope the cell should be —, got: %q", gotNoScope)
	}
}

// TestBuildKstatusBody_FailedOnly_WorkloadCompacts asserts the same failed-only
// treatment now applies to the Workload Status table: all-Current pods collapse
// to a compact per-namespace summary row, and a failing pod yields a table of
// only the failing pods.
func TestBuildKstatusBody_FailedOnly_WorkloadCompacts(t *testing.T) {
	inv := []readiness.ResourceStatus{
		{Kind: "Namespace", Namespace: "", Name: "test", Status: "Current", Message: "ok"},
	}
	// All pods Current → compact Workload summary row.
	pods := []readiness.ResourceStatus{
		{Kind: "Pod", Namespace: "test", Name: "podinfo-a", Status: "Current", Controller: "Deployment/podinfo"},
		{Kind: "Pod", Namespace: "test", Name: "podinfo-b", Status: "Current", Controller: "Deployment/podinfo"},
	}
	got := buildKstatusBody(inv, pods, true, true, false, true /* failedOnly */, "monitoring", "", "", nil)
	if !strings.Contains(got, "## Workload Status") {
		t.Fatalf("missing Workload Status section: %q", got)
	}
	if !strings.Contains(got, "| Kustomization | Pods | Status |") || !strings.Contains(got, "| monitoring | 2 | "+statusEmojiHealthy+" Current |") {
		t.Errorf("expected a compact all-Current Workload summary row keyed by Kustomization: %q", got)
	}
	if strings.Contains(got, "podinfo-a") {
		t.Errorf("individual healthy pod rows should be collapsed: %q", got)
	}

	// One failing pod → table of only the failing pod, keyed by the pod's
	// actual namespace (the detailed table keeps per-resource namespace).
	pods2 := []readiness.ResourceStatus{
		{Kind: "Pod", Namespace: "test", Name: "podinfo-a", Status: "Current", Controller: "Deployment/podinfo"},
		{Kind: "Pod", Namespace: "test", Name: "podinfo-b", Status: "Failed", Message: "CrashLoopBackOff", Controller: "Deployment/podinfo"},
	}
	got2 := buildKstatusBody(inv, pods2, true, true, false, true, "monitoring", "", "", nil)
	if !strings.Contains(got2, "| Namespace | Controller | Pod | Status | Message |") {
		t.Errorf("expected the detailed Workload table when a pod fails: %q", got2)
	}
	if !strings.Contains(got2, "podinfo-b") {
		t.Errorf("failing pod must be shown: %q", got2)
	}
	if strings.Contains(got2, "podinfo-a") {
		t.Errorf("healthy pods should be filtered out in failedOnly mode: %q", got2)
	}
}

// TestBuildKstatusBody_FailureWinsOverInProgress asserts the overall
// marker rule (failures win over warnings/healthy) and that the failure
// summary line names the count of failed resources.
func TestBuildKstatusBody_FailureWinsOverInProgress(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "apps", Name: "api", Status: "Failed", Message: "CrashLoopBackOff"},
		{Kind: "Deployment", Namespace: "apps", Name: "worker", Status: "InProgress", Message: "Available=False"},
	}
	got := buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", nil)
	if !strings.HasPrefix(got, statusEmojiFailure+"\n") {
		t.Errorf("expected ❌ marker, got: %q", got)
	}
	if !strings.Contains(got, "1 of 2 resource(s) failed to reconcile.") {
		t.Errorf("expected failure summary, got: %q", got)
	}
}

// TestBuildKstatusBody_TimeoutDowngradesToWarning asserts that a poll
// timeout (settled=false) downgrades an otherwise-all-Current snapshot to
// ⚠️ — kstatus may have settled mid-flight but the terminal-state contract
// was missed.
func TestBuildKstatusBody_TimeoutDowngradesToWarning(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "apps", Name: "api", Status: "Current", Message: "ready"},
	}
	got := buildKstatusBody(statuses, nil, false, false, true, false, "", "", "", nil)
	if !strings.HasPrefix(got, statusEmojiWarning+"\n") {
		t.Errorf("expected ⚠️ marker on poll timeout, got: %q", got)
	}
	if !strings.Contains(got, "Readiness not confirmed before the poll ended") {
		t.Errorf("expected unsettled summary phrasing, got: %q", got)
	}
	if strings.Contains(got, "0 of 1") {
		t.Errorf("summary must not report a timeout over zero pending resources: %q", got)
	}
}

// TestBuildKstatusBody_SummaryOptOut covers the includeSummary=false
// path: body goes straight from the marker line to the Resource Status
// table, with no English summary in between (and no kstatus README link
// in the body either — that's a healthy-summary-only embed).
func TestBuildKstatusBody_SummaryOptOut(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "apps", Name: "api", Status: "Current", Message: "ready"},
	}
	got := buildKstatusBody(statuses, nil, false, true, false, false, "", "", "", nil)
	if !strings.HasPrefix(got, statusEmojiHealthy+"\n\n## Resource Status") {
		t.Errorf("expected body to jump straight from marker to Resource Status, got: %q", got)
	}
	if strings.Contains(got, kstatusReadmeURL) {
		t.Errorf("kstatus README URL should be absent when summary is off, got: %q", got)
	}
	if strings.Contains(got, "reached [kstatus]") {
		t.Errorf("summary phrasing should be omitted: %q", got)
	}
}

// TestBuildKstatusBody_FormatCommentLiftsHeader verifies that the body
// produced by buildKstatusBody is correctly wrapped by formatComment — the
// status marker on the first body line must be lifted into the
// "# <emoji> Flux Status" header, the summary paragraph (carrying the
// kstatus README link) must survive the wrap, and the footer must be
// appended.
func TestBuildKstatusBody_FormatCommentLiftsHeader(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "apps", Name: "api", Status: "Current", Message: "ready"},
	}
	wrapped := formatComment(buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", nil), true, "")
	if !strings.HasPrefix(wrapped, "# ✅ Flux Status\n\n") {
		t.Errorf("expected ✅ header, got: %q", wrapped)
	}
	if !strings.Contains(wrapped, "## Resource Status") {
		t.Errorf("table section missing from wrapped comment: %q", wrapped)
	}
	if !strings.Contains(wrapped, kstatusReadmeURL) {
		t.Errorf("kstatus README link missing from wrapped comment: %q", wrapped)
	}
	if !strings.HasSuffix(wrapped, "---\n*Analyzed by flux-ai-bridge*") {
		t.Errorf("footer missing: %q", wrapped)
	}
}

// TestFormatComment_FooterOptOut asserts that includeFooter=false strips
// the attribution line — header + body remain, no `*Analyzed by …*`
// trailer.
func TestFormatComment_FooterOptOut(t *testing.T) {
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "apps", Name: "api", Status: "Current", Message: "ready"},
	}
	wrapped := formatComment(buildKstatusBody(statuses, nil, false, true, false, false, "", "", "", nil), false, "")
	if !strings.HasPrefix(wrapped, "# ✅ Flux Status\n\n") {
		t.Errorf("expected ✅ header, got: %q", wrapped)
	}
	if strings.Contains(wrapped, "*Analyzed by flux-ai-bridge*") {
		t.Errorf("footer should be omitted when includeFooter=false, got: %q", wrapped)
	}
	// `---` alone matches GFM table separators (`|---|---|---|`); only the
	// standalone footer separator line preceded by a blank line is forbidden.
	if strings.Contains(wrapped, "\n\n---\n") {
		t.Errorf("footer separator line should be omitted: %q", wrapped)
	}
}

// TestFormatComment_FooterToggleIsIdempotent verifies that flipping the
// footer toggle off on a comment that previously had one strips the stale
// footer rather than carrying it through — re-wrapping must always
// produce a single canonical shape.
func TestFormatComment_FooterToggleIsIdempotent(t *testing.T) {
	withFooter := formatComment("✅\n\n## Resource Status", true, "")
	if !strings.HasSuffix(withFooter, "---\n*Analyzed by flux-ai-bridge*") {
		t.Fatalf("setup: expected footer present in baseline: %q", withFooter)
	}
	// Re-wrap with footer off — the original footer must be stripped, not
	// duplicated or left dangling.
	stripped := formatComment(withFooter, false, "")
	if strings.Contains(stripped, "*Analyzed by flux-ai-bridge*") {
		t.Errorf("re-wrap with includeFooter=false should strip prior footer, got: %q", stripped)
	}
}

// TestMarkdownTableCell collapses newlines and escapes pipes so a
// multi-line kstatus message can't break GFM's table layout.
func TestMarkdownTableCell(t *testing.T) {
	cases := map[string]string{
		"":               "",
		"plain":          "plain",
		"with|pipe":      "with\\|pipe",
		"line1\nline2":   "line1 line2",
		"line1\r\nline2": "line1 line2",
		"line1\rline2":   "line1 line2",
		"  spaced  ":     "spaced",
		"two|pipes|here": "two\\|pipes\\|here",
	}
	for in, want := range cases {
		if got := markdownTableCell(in); got != want {
			t.Errorf("markdownTableCell(%q) = %q, want %q", in, got, want)
		}
	}
}

// Only real headings satisfy the failure-section contract: the same headings
// quoted inside a code block give the reviewer no section, so the corrective
// retry must still run.
func TestLLMMissingFailureSections(t *testing.T) {
	for reply, want := range map[string]bool{
		"## Root Cause\nx\n## Next steps to fix\n1. y":                        false,
		"## root cause\nx\n### Next steps\n1. y":                              false,
		"## Kustomization Status\nok":                                         true,
		"## Root Cause\nx":                                                    true,
		"```\n## Root Cause\n## Next steps to fix\n```":                       true,
		"## Kustomization Status\n~~~md\n## Root Cause\n~~~\n## Next steps\n": true,
		"    ## Root Cause\n    ## Next steps to fix":                         true,
	} {
		if got := llmMissingFailureSections(reply); got != want {
			t.Errorf("llmMissingFailureSections(%q) = %v, want %v", reply, got, want)
		}
	}
}

// TestProcess_NoLLM_PostsKstatusComment runs Process end-to-end with a nil
// analyzer (LLM_PROVIDER=none). The posted comment must be the
// buildKstatusBody output wrapped by formatComment — no LLM contact, no
// failure-context fetch, but a real PR comment hits the commenter.
func TestProcess_NoLLM_PostsKstatusComment(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{prNumber: 42}

	// CommentIncludeFooter explicitly set because Go's zero value (`false`)
	// differs from config.Load's documented default (`true`). Inline Config
	// literals don't go through Load, so any test that asserts on the
	// footer presence needs the flag set explicitly.
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", CommentIncludeFooter: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	// nil analyzer is the entire no-LLM signal — main.go leaves it nil
	// when cfg.LLMProvider == "none".
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
		Message:        "Applied revision",
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}

	body := commenterMock.prCommentBody
	if !strings.HasPrefix(body, "# ") || !strings.Contains(body, "Flux Status") {
		t.Errorf("expected canonical header on no-LLM comment, got: %q", body)
	}
	if !strings.Contains(body, "## Resource Status") {
		t.Errorf("expected Resource Status table on no-LLM comment, got: %q", body)
	}
	// The kstatus link is rendered as part of the *healthy* summary only.
	// This test's fakeMCPClient doesn't supply the inventory entry, so
	// the readiness check returns NotFound → failure path → no link.
	// Don't assert on the URL here; TestBuildKstatusBody_FormatCommentLiftsHeader
	// covers the healthy-path link survival.
	if !strings.HasSuffix(body, "---\n*Analyzed by flux-ai-bridge*") {
		t.Errorf("expected footer on no-LLM comment, got: %q", body)
	}
}

// TestBuildKstatusBody_FoldsConditionIntoTable pins the fix for the "Helm
// chart couldn't install (missing repo) and the comment didn't say why"
// report. A non-Current inventory row carries its controller's own root-cause
// condition (Ready=False reason+message) INLINE in the Resource Status table's
// Message column — replacing kstatus's terse "generation N, observed -1"
// summary — so the cause lands even with the LLM disabled, without a separate
// section. The readiness checker already populated Conditions during the poll.
func TestBuildKstatusBody_FoldsConditionIntoTable(t *testing.T) {
	const cause = `HelmChart 'test/test-podinfo' is not ready: failed to get source: HelmRepository.source.toolkit.fluxcd.io "stefanprodan" not found`
	statuses := []readiness.ResourceStatus{
		{
			Kind: "HelmRelease", Namespace: "test", Name: "podinfo",
			Status:  "InProgress",
			Message: "HelmRelease generation is 1, but latest observed generation is -1",
			Conditions: []readiness.Condition{
				{Type: "Ready", Status: "False", Reason: "SourceNotReady", Message: cause},
			},
		},
	}
	got := buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", nil)
	// No separate section anymore — the cause lives in the table.
	if strings.Contains(got, "## Inventory Conditions") {
		t.Errorf("conditions should be folded into the table, not a separate section: %q", got)
	}
	// The condition reason+message replaces the vague kstatus summary in the
	// Message column.
	if !strings.Contains(got, "SourceNotReady: "+cause) {
		t.Errorf("expected the Ready-condition reason+message in the table Message column: %q", got)
	}
	if strings.Contains(got, "but latest observed generation is -1") {
		t.Errorf("the vague kstatus summary should be replaced by the condition message: %q", got)
	}
}

// TestBuildKstatusBody_SkipsHealthyAbnormalTrueCondition covers a HelmRelease
// whose spec moved on (generation 31) while helm-controller hasn't picked it up
// yet: Ready is still True from the previous generation, so the only not-True
// condition is Drifted=False — which is the HEALTHY state of an abnormal-true
// condition. It must not be shown as the row's cause; kstatus's
// generation message is the actual reason the row is InProgress.
func TestBuildKstatusBody_SkipsHealthyAbnormalTrueCondition(t *testing.T) {
	const kstatusMsg = "HelmRelease generation is 31, but latest observed generation is 30"
	statuses := []readiness.ResourceStatus{
		{
			Kind: "HelmRelease", Namespace: "test", Name: "podinfo",
			Status:  "InProgress",
			Message: kstatusMsg,
			Conditions: []readiness.Condition{
				{Type: "Ready", Status: "True", Reason: "UpgradeSucceeded", Message: "Helm upgrade succeeded"},
				{Type: "Drifted", Status: "False", Reason: "NoDriftDetected", Message: "No drift detected against the cluster state"},
				{Type: "Stalled", Status: "False", Reason: "NotStalled", Message: "not stalled"},
				{Type: "Reconciling", Status: "False", Reason: "Idle", Message: "idle"},
				{Type: "Released", Status: "True", Reason: "UpgradeSucceeded", Message: "Helm upgrade succeeded"},
			},
		},
	}
	got := buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", nil)
	for _, healthy := range []string{"NoDriftDetected", "NotStalled", "Idle"} {
		if strings.Contains(got, healthy) {
			t.Errorf("healthy abnormal-true condition %q must not be shown as the row's cause: %q", healthy, got)
		}
	}
	if !strings.Contains(got, kstatusMsg) {
		t.Errorf("expected the kstatus generation message in the table Message column: %q", got)
	}
}

// TestBuildKstatusBody_RolledBackHelmReleaseShowsUpgradeFailure covers a
// HelmRelease whose upgrade failed until retries ran out, after which
// helm-controller rolled back. Flux then writes the rollback outcome into
// Ready (Ready=False/RollbackSucceeded "... succeeded"), which reads like a
// success next to a ❌ Failed status. The Message column must lead with the
// failure (Stalled) and keep the rollback only as trailing context.
func TestBuildKstatusBody_RolledBackHelmReleaseShowsUpgradeFailure(t *testing.T) {
	const rollbackMsg = "Helm rollback to previous release app/app-service.v189 with chart app-chart@0.14.1 succeeded"
	statuses := []readiness.ResourceStatus{
		{
			Kind: "HelmRelease", Namespace: "app", Name: "app-service",
			Status:  "Failed",
			Message: "Failed to upgrade after 4 attempt(s)",
			Conditions: []readiness.Condition{
				{Type: "Stalled", Status: "True", Reason: "RetriesExceeded", Message: "Failed to upgrade after 4 attempt(s)"},
				{Type: "Ready", Status: "False", Reason: "RollbackSucceeded", Message: rollbackMsg},
				{Type: "Released", Status: "False", Reason: "UpgradeFailed", Message: "Helm upgrade failed for release app/app-service with chart app-chart@0.14.1: context deadline exceeded"},
				{Type: "Remediated", Status: "True", Reason: "RollbackSucceeded", Message: rollbackMsg},
				{Type: "Drifted", Status: "False", Reason: "NoDriftDetected", Message: "No drift detected against the cluster state"},
			},
		},
	}
	got := buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", nil)
	// markdownTableCell breaks "@" with a zero-width space so it can't mention.
	want := "| app | HelmRelease | app-service | ❌ Failed | RetriesExceeded: Failed to upgrade after 4 attempt(s); " +
		"RollbackSucceeded: Helm rollback to previous release app/app-service.v189 with chart app-chart@\u200b0.14.1 succeeded |"
	if !strings.Contains(got, want) {
		t.Errorf("expected row %q in body:\n%s", want, got)
	}
}

// TestBuildKstatusBody_RemediatedHelmReleaseCauses covers the remediation
// outcomes besides "retries exhausted": a rollback with retries left (no
// Stalled yet; Reconciling=True repeats the rollback text and comes first, so
// the cause must be picked by type, not list order), an uninstall after a
// failed install, and a remediation with no other unhealthy condition, which
// keeps the Ready text alone.
func TestBuildKstatusBody_RemediatedHelmReleaseCauses(t *testing.T) {
	const rollbackMsg = "Helm rollback to previous release app/app-service.v7 succeeded"
	tests := []struct {
		name  string
		conds []readiness.Condition
		want  string
	}{
		{
			name: "rollback with retries left",
			conds: []readiness.Condition{
				{Type: "Reconciling", Status: "True", Reason: "ProgressingWithRetry", Message: rollbackMsg},
				{Type: "Ready", Status: "False", Reason: "RollbackSucceeded", Message: rollbackMsg},
				{Type: "Released", Status: "False", Reason: "UpgradeFailed", Message: "Helm upgrade failed: context deadline exceeded"},
				{Type: "Remediated", Status: "True", Reason: "RollbackSucceeded", Message: rollbackMsg},
			},
			want: "UpgradeFailed: Helm upgrade failed: context deadline exceeded; RollbackSucceeded: " + rollbackMsg,
		},
		{
			name: "uninstall after failed install",
			conds: []readiness.Condition{
				{Type: "Ready", Status: "False", Reason: "UninstallSucceeded", Message: "Helm uninstall for release app/app-service succeeded"},
				{Type: "Released", Status: "False", Reason: "InstallFailed", Message: "Helm install failed: timeout"},
			},
			want: "InstallFailed: Helm install failed: timeout; UninstallSucceeded: Helm uninstall for release app/app-service succeeded",
		},
		{
			name: "no other cause",
			conds: []readiness.Condition{
				{Type: "Ready", Status: "False", Reason: "RollbackSucceeded", Message: rollbackMsg},
			},
			want: "RollbackSucceeded: " + rollbackMsg,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statuses := []readiness.ResourceStatus{{
				Kind: "HelmRelease", Namespace: "app", Name: "app-service",
				Status: "Failed", Message: "kstatus message", Conditions: tt.conds,
			}}
			got := buildKstatusBody(statuses, nil, false, true, true, false, "", "", "", nil)
			want := "| app | HelmRelease | app-service | ❌ Failed | " + tt.want + " |"
			if !strings.Contains(got, want) {
				t.Errorf("expected row %q in body:\n%s", want, got)
			}
		})
	}
}

// TestNotReadyConditionMessage_AbnormalTruePolarity pins the other side of
// per-type polarity: an abnormal-true condition in its True (or Unknown) state
// IS the problem and must be shown, while a normal condition that is True is
// skipped.
func TestNotReadyConditionMessage_AbnormalTruePolarity(t *testing.T) {
	ready := readiness.Condition{Type: "Ready", Status: "True", Reason: "UpgradeSucceeded", Message: "Helm upgrade succeeded"}
	tests := []struct {
		name string
		cond readiness.Condition
		want string
	}{
		{"stalled true", readiness.Condition{Type: "Stalled", Status: "True", Reason: "InvalidChartReference", Message: "chart not found"}, "InvalidChartReference: chart not found"},
		{"drifted true", readiness.Condition{Type: "Drifted", Status: "True", Reason: "DriftDetected", Message: "Deployment/test/podinfo changed"}, "DriftDetected: Deployment/test/podinfo changed"},
		{"reconciling unknown", readiness.Condition{Type: "Reconciling", Status: "Unknown", Reason: "Progressing", Message: "waiting"}, "Progressing: waiting"},
		{"drifted false", readiness.Condition{Type: "Drifted", Status: "False", Reason: "NoDriftDetected", Message: "no drift"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := notReadyConditionMessage([]readiness.Condition{ready, tt.cond}); got != tt.want {
				t.Errorf("notReadyConditionMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestBuildKstatusBody_RendersWorkloadStatus asserts pod rows appear under
// a "## Workload Status" section with the Controller column populated from
// each pod's ResourceStatus.Controller field. Naked pods (empty Controller)
// render with a "—" placeholder so the column stays aligned.
func TestBuildKstatusBody_RendersWorkloadStatus(t *testing.T) {
	inventory := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "podinfo", Name: "podinfo", Status: "Current", Message: "released"},
	}
	pods := []readiness.ResourceStatus{
		{Kind: "Pod", Namespace: "podinfo", Name: "podinfo-abc", Status: "Current", Message: "all containers ready", Controller: "Deployment/podinfo"},
		{Kind: "Pod", Namespace: "podinfo", Name: "podinfo-def", Status: "Current", Message: "all containers ready", Controller: "Deployment/podinfo"},
		{Kind: "Pod", Namespace: "podinfo", Name: "naked-pod", Status: "Current", Message: "all containers ready"},
	}
	got := buildKstatusBody(inventory, pods, true, true, true, false, "", "", "", nil)
	if !strings.Contains(got, "## Workload Status") {
		t.Errorf("missing Workload Status section: %q", got)
	}
	if !strings.Contains(got, "| podinfo | Deployment/podinfo | podinfo-abc | ✅ Current | all containers ready |") {
		t.Errorf("expected formatted pod row with controller column: %q", got)
	}
	if !strings.Contains(got, "| podinfo | — | naked-pod |") {
		t.Errorf("expected naked-pod row to use \"—\" placeholder in controller column: %q", got)
	}
}

// TestBuildKstatusBody_ScannedButNoPods asserts that when the pod scan ran
// conclusively but found zero pods (podsScanned=true, empty slice), the
// Workload Status section is still emitted with an explicit "No pods running."
// note rather than being omitted — the case flux-operator-mcp surfaces as
// "No resources found" for a namespace with no pods.
func TestBuildKstatusBody_ScannedButNoPods(t *testing.T) {
	inventory := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "test", Name: "podinfo", Status: "Current", Message: "released"},
	}
	got := buildKstatusBody(inventory, nil, true, true, true, false, "", "", "", nil)
	if !strings.Contains(got, "## Workload Status") {
		t.Errorf("expected Workload Status section when scan ran with zero pods: %q", got)
	}
	if !strings.Contains(got, "No pods running.") {
		t.Errorf("expected explicit \"No pods running.\" note: %q", got)
	}
}

// TestBuildKstatusBody_UnscannedOmitsWorkloadStatus asserts the section is
// omitted (not "No pods running") when the scan did not run — so the bridge
// never claims zero pods off data it never fetched.
func TestBuildKstatusBody_UnscannedOmitsWorkloadStatus(t *testing.T) {
	inventory := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "test", Name: "podinfo", Status: "Current", Message: "released"},
	}
	got := buildKstatusBody(inventory, nil, false, true, true, false, "", "", "", nil)
	if strings.Contains(got, "## Workload Status") {
		t.Errorf("Workload Status section should be omitted when scan did not run: %q", got)
	}
}

// TestBuildKstatusBody_FailingPodFlipsToRed asserts a failing pod degrades
// the overall marker to ❌ even when every inventory row is Current — the
// real-world case where a controller hasn't propagated the pod failure
// upward yet.
func TestBuildKstatusBody_FailingPodFlipsToRed(t *testing.T) {
	inventory := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "podinfo", Name: "podinfo", Status: "Current", Message: "released"},
	}
	pods := []readiness.ResourceStatus{
		{Kind: "Pod", Namespace: "podinfo", Name: "podinfo-abc", Status: "Failed", Message: "podinfo: CrashLoopBackOff"},
	}
	got := buildKstatusBody(inventory, pods, true, true, true, false, "", "", "", nil)
	if !strings.HasPrefix(got, statusEmojiFailure+"\n") {
		t.Errorf("expected ❌ marker when a pod is failing: %q", got)
	}
	// Pod's failure shows up in the Workload Status row's Message column.
	if !strings.Contains(got, "podinfo: CrashLoopBackOff") {
		t.Errorf("expected failing pod's reason in Workload Status table: %q", got)
	}
}

// A Running pod whose containers are all ready but whose own Ready condition
// is False (a readiness gate that hasn't passed) isn't Current yet: it would
// otherwise settle the wait and count toward flux-success.
func TestPodKstatus_ReadinessGatePending(t *testing.T) {
	pods, err := parsePodListAll([]byte(`items:
- metadata:
    name: web-0
    namespace: apps
  status:
    phase: Running
    conditions:
    - type: ContainersReady
      status: "True"
    - type: Ready
      status: "False"
      reason: ReadinessGatesNotReady
    containerStatuses:
    - name: app
      ready: true
- metadata:
    name: web-1
    namespace: apps
  status:
    phase: Running
    conditions:
    - type: Ready
      status: "True"
    containerStatuses:
    - name: app
      ready: true
`))
	if err != nil || len(pods) != 2 {
		t.Fatalf("parsePodListAll = %d pods, %v", len(pods), err)
	}
	if got := podKstatus(pods[0], time.Now()); got.Status != "InProgress" {
		t.Errorf("gated pod: Status = %q, want InProgress (msg=%q)", got.Status, got.Message)
	}
	if got := podKstatus(pods[1], time.Now()); got.Status != "Current" {
		t.Errorf("ready pod: Status = %q, want Current (msg=%q)", got.Status, got.Message)
	}
}

// TestPodKstatus_NonFailingClassification exercises the non-failing branches
// (Running+Ready, Running+NotReady, Pending, Succeeded, Unknown). The failing
// branch is already covered by isFailingPod's own tests.
func TestPodKstatus_NonFailingClassification(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name       string
		pod        podInfo
		wantStatus string
	}{
		{
			name: "running and all ready → Current",
			pod: podInfo{
				Metadata: podMetadata{Name: "p1", Namespace: "ns"},
				Status:   podStatus{Phase: "Running", ContainerStatuses: []containerStatus{{Name: "c", Ready: true}}},
			},
			wantStatus: "Current",
		},
		{
			name: "running but a container not ready → InProgress",
			pod: podInfo{
				Metadata: podMetadata{Name: "p2", Namespace: "ns"},
				Status:   podStatus{Phase: "Running", ContainerStatuses: []containerStatus{{Name: "c", Ready: false}}},
			},
			wantStatus: "InProgress",
		},
		{
			name: "Pending (within grace) → InProgress",
			pod: podInfo{
				Metadata: podMetadata{Name: "p3", Namespace: "ns", CreationTimestamp: now.Format(time.RFC3339)},
				Status:   podStatus{Phase: "Pending"},
			},
			wantStatus: "InProgress",
		},
		{
			name: "Succeeded (completed Job) → Current",
			pod: podInfo{
				Metadata: podMetadata{Name: "p4", Namespace: "ns"},
				Status:   podStatus{Phase: "Succeeded"},
			},
			wantStatus: "Current",
		},
		{
			name: "unknown phase → Unknown",
			pod: podInfo{
				Metadata: podMetadata{Name: "p5", Namespace: "ns"},
				Status:   podStatus{Phase: "WhoKnows"},
			},
			wantStatus: "Unknown",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := podKstatus(tc.pod, now)
			if got.Status != tc.wantStatus {
				t.Errorf("Status = %q, want %q (msg=%q)", got.Status, tc.wantStatus, got.Message)
			}
			if got.Kind != "Pod" {
				t.Errorf("Kind = %q, want Pod", got.Kind)
			}
		})
	}
}

// helmReleaseWithInventoryYAML simulates the inventory shape
// flux-operator-mcp renders for HelmReleases: each `.status.history[]`
// entry carries a structured `inventory:` list with apiVersion/kind/name/
// namespace fields. The parser must reconstruct
// `<ns>_<name>_<group>_<kind>` IDs from those fields.
const helmReleaseWithInventoryYAML = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
status:
  history:
    - chartName: app
      status: deployed
      version: 1
      inventory:
        - apiVersion: apps/v1
          kind: Deployment
          name: app
          namespace: apps
        - apiVersion: v1
          kind: Service
          name: app
          namespace: apps
`

// helmReleaseRawEntriesYAML pins the secondary shape — the
// `.status.inventory.entries[].id` format used by kustomize-controller and
// (depending on Flux version) sometimes by HelmRelease's raw spec. The
// parser must accept both shapes interchangeably.
const helmReleaseRawEntriesYAML = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
status:
  inventory:
    entries:
      - id: "apps_app_apps_Deployment"
      - id: "apps_app__Service"
`

// TestExpandHelmReleaseInventories_FoldsChildEntries asserts that a
// HelmRelease ref in the base inventory triggers a child-fetch and the
// returned ref list contains both the HelmRelease itself AND every entry
// from its own status.inventory.
func TestExpandHelmReleaseInventories_FoldsChildEntries(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseWithInventoryYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{
		"apps_app_helm.toolkit.fluxcd.io_HelmRelease",
		"apps_app__ConfigMap",
	}
	got, _ := proc.expandHelmReleaseInventories(context.Background(), base)
	wantContains := []string{
		"apps_app_helm.toolkit.fluxcd.io_HelmRelease", // base
		"apps_app__ConfigMap",                         // base
		"apps_app_apps_Deployment",                    // HR child
		"apps_app__Service",                           // HR child
	}
	for _, w := range wantContains {
		found := false
		for _, r := range got {
			if r == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expanded list missing %q; got %v", w, got)
		}
	}
}

// TestExpandHelmReleaseInventories_DisabledByFlag asserts the feature
// opt-out: ReadinessExpandHelmReleases=false returns the base list
// unchanged, and crucially does NOT make any MCP fetches (otherwise the
// flag wouldn't actually save the round-trip).
func TestExpandHelmReleaseInventories_DisabledByFlag(t *testing.T) {
	mcpMock := &fakeMCPClient{
		err: fmt.Errorf("MCP should not be called when expansion is disabled"),
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: false}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	got, _ := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(got) != 1 || got[0] != base[0] {
		t.Errorf("flag-disabled expansion must return base unchanged, got %v", got)
	}
	if mcpMock.resourceCalls != 0 {
		t.Errorf("flag-disabled expansion made %d MCP reads, want 0", mcpMock.resourceCalls)
	}
}

// TestExpandHelmReleaseInventories_NoHelmReleaseInBase asserts that a
// non-HelmRelease-bearing inventory short-circuits without ever calling
// MCP (no Kind matches HelmRelease).
func TestExpandHelmReleaseInventories_NoHelmReleaseInBase(t *testing.T) {
	mcpMock := &fakeMCPClient{
		err: fmt.Errorf("MCP should not be called when no HelmRelease is in base"),
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app__ConfigMap", "apps_app_apps_Deployment"}
	got, _ := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(got) != len(base) {
		t.Errorf("expected base unchanged, got %v", got)
	}
	if mcpMock.resourceCalls != 0 {
		t.Errorf("HelmRelease-free inventory made %d MCP reads, want 0", mcpMock.resourceCalls)
	}
}

// TestExpandHelmReleaseInventories_AcceptsRawEntriesShape covers the
// secondary shape — `.status.inventory.entries[*].id` direct IDs — so the
// parser stays compatible with whatever flux-operator-mcp version renders
// HelmReleases that way.
func TestExpandHelmReleaseInventories_AcceptsRawEntriesShape(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseRawEntriesYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	got, _ := proc.expandHelmReleaseInventories(context.Background(), []string{
		"apps_app_helm.toolkit.fluxcd.io_HelmRelease",
	})
	wantContains := []string{
		"apps_app_apps_Deployment",
		"apps_app__Service",
	}
	for _, w := range wantContains {
		found := false
		for _, r := range got {
			if r == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("raw-entries shape: expanded list missing %q; got %v", w, got)
		}
	}
}

// TestExpandHelmReleaseInventories_FetchFailureFallsBackToBase asserts the
// graceful-degradation contract: when MCP can't deliver a HelmRelease YAML
// (transient error, RBAC, etc.) we keep the HelmRelease itself in the refs
// and skip its expansion rather than aborting the whole pipeline.
func TestExpandHelmReleaseInventories_FetchFailureFallsBackToBase(t *testing.T) {
	mcpMock := &fakeMCPClient{
		err: fmt.Errorf("simulated MCP failure"),
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	got, _ := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(got) != 1 || got[0] != base[0] {
		t.Errorf("MCP failure should leave base intact, got %v", got)
	}
}

// helmReleaseNoHistoryYAML simulates a HelmRelease that hasn't completed its
// first install yet: no status.history and no status.inventory.entries at
// all, which is exactly what helm-controller reports before it ever records
// a release.
const helmReleaseNoHistoryYAML = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
status:
  observedGeneration: -1
  conditions:
    - type: Ready
      status: Unknown
      reason: Progressing
      message: "Running 'install' action with timeout of 10m0s"
`

// TestExpandHelmReleaseInventories_FlagsNoHistoryAsPending covers the
// mitigation for a HelmRelease still pending its first install: fetched and
// parsed without error, but with zero children because status.history is
// still empty. The caller needs to know this so it can flag the picture as
// incomplete instead of silently reporting a clean, small release.
func TestExpandHelmReleaseInventories_FlagsNoHistoryAsPending(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseNoHistoryYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	got, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(got) != 1 || got[0] != base[0] {
		t.Errorf("expected base unchanged (no children to add), got %v", got)
	}
	if len(pending) != 1 || pending[0].Name != "apps/app" || pending[0].Reason != helmReleaseReasonInstallPending {
		t.Errorf("expected pending = [{apps/app installPending}], got %v", pending)
	}
}

// TestExpandHelmReleaseInventories_ChildrenFoundIsNotPending is the negative
// case for the two prior tests: a HelmRelease with real children (either
// inventory shape) must NOT be reported as pending.
func TestExpandHelmReleaseInventories_ChildrenFoundIsNotPending(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseWithInventoryYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	_, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(pending) != 0 {
		t.Errorf("HelmRelease with children must not be pending, got %v", pending)
	}
}

// helmReleaseCompletedEmptyInventoryYAML simulates a HelmRelease that HAS
// completed a reconcile (status.observedGeneration matches the applied
// generation) whose chart simply renders zero resources — a legitimate, if
// rare, empty release. This must not be confused with a first-install-still-
// pending HelmRelease (see helmReleaseNoHistoryYAML above): both have zero
// extracted children, but only the latter still shows observedGeneration -1.
const helmReleaseCompletedEmptyInventoryYAML = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
status:
  observedGeneration: 1
  history:
    - chartName: app
      status: deployed
      version: 1
      inventory: []
`

// TestExpandHelmReleaseInventories_CompletedEmptyHistoryIsNotPending covers
// the false-positive the naive "zero children" signal alone would produce:
// a HelmRelease with a real (if empty) release history must not be reported
// as still pending its first install.
func TestExpandHelmReleaseInventories_CompletedEmptyHistoryIsNotPending(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseCompletedEmptyInventoryYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	_, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(pending) != 0 {
		t.Errorf("a completed release with an empty inventory must not be pending, got %v", pending)
	}
}

// helmReleaseV2beta1NoHistoryYAML simulates a long-running v2beta1
// HelmRelease: upstream helm-controller marks status.history as "not
// actively used" for that API version, so it can be permanently absent even
// on a release that has reconciled many times over (observedGeneration
// matches the applied generation).
const helmReleaseV2beta1NoHistoryYAML = `apiVersion: helm.toolkit.fluxcd.io/v2beta1
kind: HelmRelease
metadata:
  name: app
  namespace: apps
status:
  observedGeneration: 5
  conditions:
    - type: Ready
      status: "True"
      reason: ReleaseSucceeded
      message: "Release reconciliation succeeded"
`

// TestExpandHelmReleaseInventories_V2beta1NoHistoryIsNotPending covers the
// v2beta1 case explicitly: no status.history at all, but observedGeneration
// shows the release has reconciled — must not be reported as pending.
func TestExpandHelmReleaseInventories_V2beta1NoHistoryIsNotPending(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseV2beta1NoHistoryYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	_, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(pending) != 0 {
		t.Errorf("a reconciled v2beta1 release must not be pending just because it has no status.history, got %v", pending)
	}
}

// helmReleaseDeletingYAML simulates a HelmRelease mid-teardown:
// helm-controller calls ClearHistory() on uninstall well before the object
// is actually removed, so a deleting-but-not-yet-gone release also reads
// "no history" and (per some controller versions) can revert
// observedGeneration — neither of which should be read as a stalled first
// install.
const helmReleaseDeletingYAML = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
  deletionTimestamp: "2026-01-01T00:00:00Z"
status:
  observedGeneration: -1
`

// TestExpandHelmReleaseInventories_DeletingReleaseIsNotPending covers the
// uninstall case explicitly: a HelmRelease being torn down is not a stalled
// first install, regardless of what its history/observedGeneration read.
func TestExpandHelmReleaseInventories_DeletingReleaseIsNotPending(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseDeletingYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	_, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(pending) != 0 {
		t.Errorf("a deleting release must not be reported as pending, got %v", pending)
	}
}

// helmReleaseListWrapperReconciledYAML wraps an already-reconciled HelmRelease
// in a standard Kubernetes `kind: List` envelope that ALSO carries its own
// top-level `metadata` (a real List object always has a ListMeta, even if
// near-empty) — the shape that previously fooled the traversal into
// evaluating the wrapper itself instead of descending into `items`.
const helmReleaseListWrapperReconciledYAML = `apiVersion: v1
kind: List
metadata:
  resourceVersion: ""
items:
  - apiVersion: helm.toolkit.fluxcd.io/v2
    kind: HelmRelease
    metadata:
      name: app
      namespace: apps
    status:
      observedGeneration: 3
`

// TestExpandHelmReleaseInventories_ListWrapperWithMetadataIsNotPending covers
// the review finding: a List envelope's own (typically near-empty) metadata
// must not be mistaken for the wrapped resource's metadata and short-circuit
// evaluation before ever reaching `items`.
func TestExpandHelmReleaseInventories_ListWrapperWithMetadataIsNotPending(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseListWrapperReconciledYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	_, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(pending) != 0 {
		t.Errorf("a reconciled HelmRelease inside a List wrapper with its own metadata must not be pending, got %v", pending)
	}
}

// TestExpandHelmReleaseInventories_FetchFailureIsUnverified: a fetch failure
// tells us nothing about whether the release has completed a reconcile, so
// it fails CLOSED (marked pending, forcing Warning) but tagged
// helmReleaseReasonUnverified — distinct from helmReleaseReasonInstallPending
// — so the downstream note never claims a release is "still installing"
// when all we actually know is that the read failed.
func TestExpandHelmReleaseInventories_FetchFailureIsUnverified(t *testing.T) {
	mcpMock := &fakeMCPClient{
		err: fmt.Errorf("simulated MCP failure"),
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	_, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(pending) != 1 || pending[0].Name != "apps/app" || pending[0].Reason != helmReleaseReasonUnverified {
		t.Errorf("expected pending = [{apps/app unverified}], got %v", pending)
	}
}

// TestExpandHelmReleaseInventories_MalformedYAMLIsUnverified exercises the
// OTHER fail-closed branch: GetKubernetesResource succeeds, but the bytes it
// returns aren't valid YAML, so helmReleaseInventoryIDs itself errors. This
// is distinct from a fetch (transport) failure and wasn't covered by any
// existing test — a change to the parser that silently swallowed this error
// instead of returning it would regress to fail-open here undetected.
func TestExpandHelmReleaseInventories_MalformedYAMLIsUnverified(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte("status: [unterminated"),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	got, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(got) != 1 || got[0] != base[0] {
		t.Errorf("malformed YAML should leave base intact, got %v", got)
	}
	if len(pending) != 1 || pending[0].Name != "apps/app" || pending[0].Reason != helmReleaseReasonUnverified {
		t.Errorf("expected pending = [{apps/app unverified}], got %v", pending)
	}
}

// TestExpandHelmReleaseInventories_RawEntriesShapeIsNotPending: the
// raw-entries inventory shape (status.inventory.entries, no status.history)
// still carries real children and must not be flagged as pending just
// because it lacks a history section.
func TestExpandHelmReleaseInventories_RawEntriesShapeIsNotPending(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": []byte(helmReleaseRawEntriesYAML),
		},
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"}
	_, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(pending) != 0 {
		t.Errorf("raw-entries shape with real children must not be pending, got %v", pending)
	}
}

func TestPendingHelmReleaseNote_Singular(t *testing.T) {
	note := pendingHelmReleaseNote([]pendingHelmRelease{{Name: "apps/web", Reason: helmReleaseReasonInstallPending}})
	if !strings.Contains(note, "HelmRelease `apps/web` has not completed an initial install yet") {
		t.Errorf("unexpected singular note: %s", note)
	}
	if !strings.Contains(note, "ImageUpdateAutomation") {
		t.Errorf("expected note to mention chart-templated resources, got: %s", note)
	}
}

func TestPendingHelmReleaseNote_Plural(t *testing.T) {
	note := pendingHelmReleaseNote([]pendingHelmRelease{
		{Name: "b/b", Reason: helmReleaseReasonInstallPending},
		{Name: "a/a", Reason: helmReleaseReasonInstallPending},
	})
	// Sorted, "have" (not "has") for multiple entries, and "their"/"their"
	// (not "its") to agree with the plural subject.
	if !strings.Contains(note, "HelmReleases `a/a`, `b/b` have not completed an initial install yet") {
		t.Errorf("unexpected plural note: %s", note)
	}
	if !strings.Contains(note, "so their rendered resources aren't in their status") {
		t.Errorf("expected plural pronoun agreement, got: %s", note)
	}
}

// TestPendingHelmReleaseNote_Unverified covers the review finding: a
// fetch/parse failure must render distinct wording from a genuine stalled
// install, since it isn't evidence of one.
func TestPendingHelmReleaseNote_Unverified(t *testing.T) {
	note := pendingHelmReleaseNote([]pendingHelmRelease{{Name: "apps/web", Reason: helmReleaseReasonUnverified}})
	if strings.Contains(note, "has not completed an initial install yet") {
		t.Errorf("unverified note must not claim the release is still installing: %s", note)
	}
	if !strings.Contains(note, "HelmRelease `apps/web` could not be inspected this run") {
		t.Errorf("unexpected unverified note: %s", note)
	}
}

// TestPendingHelmReleaseNote_MixedReasons covers both reasons appearing in
// the same run (e.g. one HelmRelease genuinely still installing, another
// uninspectable because of an unrelated MCP error): each gets its own
// paragraph with its own, accurate wording.
func TestPendingHelmReleaseNote_MixedReasons(t *testing.T) {
	note := pendingHelmReleaseNote([]pendingHelmRelease{
		{Name: "apps/installing", Reason: helmReleaseReasonInstallPending},
		{Name: "apps/unreadable", Reason: helmReleaseReasonUnverified},
	})
	if !strings.Contains(note, "HelmRelease `apps/installing` has not completed an initial install yet") {
		t.Errorf("expected install-pending paragraph, got: %s", note)
	}
	if !strings.Contains(note, "HelmRelease `apps/unreadable` could not be inspected this run") {
		t.Errorf("expected unverified paragraph, got: %s", note)
	}
}

// TestPendingHelmReleaseNote_UnverifiedPlural: the closing clause must read
// correctly for several releases too (no singular/plural pronoun mismatch).
func TestPendingHelmReleaseNote_UnverifiedPlural(t *testing.T) {
	note := pendingHelmReleaseNote([]pendingHelmRelease{
		{Name: "b/b", Reason: helmReleaseReasonUnverified},
		{Name: "a/a", Reason: helmReleaseReasonUnverified},
	})
	if !strings.Contains(note, "HelmReleases `a/a`, `b/b` could not be inspected this run") {
		t.Errorf("unexpected plural unverified note: %s", note)
	}
	if !strings.Contains(note, "not evidence of an incomplete install") {
		t.Errorf("expected number-neutral closing clause, got: %s", note)
	}
}

// TestExpandHelmReleaseInventories_NonFluxHelmReleaseKindIsSkipped: Kinds are
// only unique within an API group. An inventory entry whose Kind is
// "HelmRelease" but whose group isn't helm.toolkit.fluxcd.io must be left
// alone — the fetch is hardwired to the Flux group and would always fail for
// it, wrongly forcing a Warning. The failing mock proves no fetch result is
// consulted.
func TestExpandHelmReleaseInventories_NonFluxHelmReleaseKindIsSkipped(t *testing.T) {
	mcpMock := &fakeMCPClient{
		err: fmt.Errorf("must not be consulted for a non-Flux group"),
	}
	cfg := &config.Config{ReadinessExpandHelmReleases: true}
	proc := New(cfg, mcpMock, nil, nil, nil, nil, nil, slog.Default())

	base := []string{"apps_app_example.com_HelmRelease"}
	got, pending := proc.expandHelmReleaseInventories(context.Background(), base)
	if len(got) != 1 || got[0] != base[0] {
		t.Errorf("expected base unchanged, got %v", got)
	}
	if len(pending) != 0 {
		t.Errorf("a non-Flux HelmRelease kind must not be reported pending, got %v", pending)
	}
}

// healthyConfigMapYAML is what fakeMCPClient.GetKubernetesResource returns
// for the lone inventory entry in kustomizationOneInventoryEntry. kstatus
// considers a ConfigMap Current immediately (no conditions to wait for) so
// the readiness poll settles green and the LLM_SKIP_ON_HEALTHY gate fires.
const healthyConfigMapYAML = `apiVersion: v1
kind: ConfigMap
metadata:
  name: any
  namespace: default
`

// crashlooperPodListYAML is a one-pod list with a container stuck in
// CrashLoopBackOff. Used to exercise the "all inventory Current but a pod
// is failing" branch of the LLM-skip gate.
const crashlooperPodListYAML = `items:
- apiVersion: v1
  kind: Pod
  metadata:
    name: api-fail
    namespace: default
  status:
    phase: Running
    containerStatuses:
    - name: api
      ready: false
      restartCount: 5
      state:
        waiting:
          reason: CrashLoopBackOff
`

// TestProcess_SkipsLLM_OnFullyHealthy asserts that LLM_SKIP_ON_HEALTHY=true
// + every inventory row Current + (with pods disabled) settled poll causes
// the analyzer to NEVER be called. The comment is still posted, built from
// kstatus alone.
func TestProcess_SkipsLLM_OnFullyHealthy(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{result: "should never be invoked"}
	commenterMock := &fakeCommenter{prNumber: 42}

	cfg := &config.Config{
		GitHubRepo:            "owner/repo",
		LLMProvider:           "claude",
		LLMSkipOnHealthy:      true,
		CommentOnSuccess:      true,
		ReadinessIncludePods:  false,
		ReadinessPollInterval: time.Millisecond,
		ReadinessPollTimeout:  10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if analyzerMock.calls != 0 {
		t.Errorf("LLM must NOT be called on fully-healthy state, got %d Analyze call(s)", analyzerMock.calls)
	}
	if !strings.Contains(commenterMock.prCommentBody, "## Resource Status") {
		t.Errorf("expected kstatus-only comment posted, got: %q", commenterMock.prCommentBody)
	}
	// Defensive: the LLM mock's stock result must NOT appear — that would
	// mean the skip silently fell through.
	if strings.Contains(commenterMock.prCommentBody, "should never be invoked") {
		t.Errorf("comment leaked the LLM mock result — skip didn't take effect")
	}
}

// TestProcess_CommentOnSuccessFalse_SuppressesHealthyComment pins the
// comment-policy feature: with COMMENT_ON_SUCCESS=false and a fully-healthy
// outcome, the bridge posts NO comment, attempts to remove any stale prior
// comment (cache-guarded), and STILL reconciles the PR label — so labels work
// without healthy-comment noise. It also asserts the LLM is never called (the
// suppressed path skips the whole body/LLM build).
func TestProcess_CommentOnSuccessFalse_SuppressesHealthyComment(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{result: "should never be invoked"}
	commenterMock := &fakeCommenter{prNumber: 42}

	cfg := &config.Config{
		GitHubRepo:            "owner/repo",
		LLMProvider:           "claude",
		CommentOnSuccess:      false, // suppress healthy comments
		PRLabelEnabled:        true,  // but still label
		ReadinessIncludePods:  false,
		ReadinessPollInterval: time.Millisecond,
		ReadinessPollTimeout:  10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}

	if len(commenterMock.posts) != 0 || commenterMock.prCommentBody != "" {
		t.Errorf("no comment should be posted on healthy outcome when COMMENT_ON_SUCCESS=false; got %q", commenterMock.prCommentBody)
	}
	if len(commenterMock.removeCalls) != 1 || commenterMock.removeCalls[0].Number != 42 {
		t.Errorf("expected RemoveComment to be called once for PR 42 (stale-comment cleanup); got %+v", commenterMock.removeCalls)
	}
	if analyzerMock.calls != 0 {
		t.Errorf("LLM must not be called on a suppressed healthy outcome; got %d", analyzerMock.calls)
	}
	// Label still reconciled, with the success outcome.
	if len(commenterMock.labelCalls) != 1 || commenterMock.labelCalls[0].Outcome != bridgegithub.OutcomeSuccess {
		t.Errorf("expected a flux-success label reconcile despite the suppressed comment; got %+v", commenterMock.labelCalls)
	}
}

// TestProcess_KeepsLLM_WhenSkipDisabled asserts the opt-out: setting
// LLM_SKIP_ON_HEALTHY=false keeps the LLM in the loop even on a fully-green
// state.
func TestProcess_KeepsLLM_WhenSkipDisabled(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{result: "## Status\nAll resources are ready."}
	commenterMock := &fakeCommenter{prNumber: 42}

	cfg := &config.Config{
		GitHubRepo:            "owner/repo",
		LLMProvider:           "claude",
		LLMSkipOnHealthy:      false,
		CommentOnSuccess:      true,
		ReadinessIncludePods:  false,
		ReadinessPollInterval: time.Millisecond,
		ReadinessPollTimeout:  10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if analyzerMock.calls != 1 {
		t.Errorf("LLM must be called when skip is disabled, got %d Analyze call(s)", analyzerMock.calls)
	}
	if !strings.Contains(commenterMock.prCommentBody, "All resources are ready.") {
		t.Errorf("comment should carry LLM result when skip disabled: %q", commenterMock.prCommentBody)
	}
}

// TestProcess_KeepsLLM_WhenPodFailing asserts the pod-aware gate: a healthy
// inventory paired with a CrashLoopBackOff pod must still route through the
// LLM. The pod kstatus snapshot was added specifically to catch this case;
// skipping the LLM would defeat that signal. The pod scan targets the
// Kustomization's namespace (its name, the namespace==name convention) — so the Kustomization
// is named "default", matching the namespace the crashlooping pod lives in.
func TestProcess_KeepsLLM_WhenPodFailing(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
		lists:     map[string][]byte{"v1|Pod|default": []byte(crashlooperPodListYAML)},
	}
	analyzerMock := &fakeAnalyzer{result: "## Status\nA pod is crashlooping.\n## Root Cause\nx\n## Next steps to fix\n1. y"}
	commenterMock := &fakeCommenter{prNumber: 42}

	cfg := &config.Config{
		GitHubRepo:            "owner/repo",
		LLMProvider:           "claude",
		LLMSkipOnHealthy:      true,
		ReadinessIncludePods:  true,
		ReadinessPollInterval: time.Millisecond,
		ReadinessPollTimeout:  10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "default", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if analyzerMock.calls != 1 {
		t.Errorf("LLM must be called when a pod is failing (inventory looked healthy), got %d Analyze call(s)", analyzerMock.calls)
	}
}

// replicaSetWithDeploymentOwnerYAML is what MCP returns for an RS owned by
// a Deployment. The resolver must hop through it and emit
// "Deployment/podinfo" rather than the implementation-detail RS name.
const replicaSetWithDeploymentOwnerYAML = `apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: podinfo-7d4f9b6c8
  namespace: podinfo
  ownerReferences:
    - apiVersion: apps/v1
      kind: Deployment
      name: podinfo
      uid: deadbeef-0000
      controller: true
`

// nakedReplicaSetYAML — an RS without ownerReferences (rare but legal:
// kubectl create rs without a Deployment). The resolver should fall back
// to "ReplicaSet/<name>" rather than emit an empty controller.
const nakedReplicaSetYAML = `apiVersion: apps/v1
kind: ReplicaSet
metadata:
  name: orphan-rs-abc
  namespace: ns
`

// TestResolvePodController_DeploymentViaReplicaSet covers the most common
// case: a pod whose direct owner is a ReplicaSet → resolver fetches the RS
// and surfaces the Deployment.
func TestResolvePodController_DeploymentViaReplicaSet(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"apps/v1|ReplicaSet|podinfo-7d4f9b6c8|podinfo": []byte(replicaSetWithDeploymentOwnerYAML),
		},
	}
	proc := New(&config.Config{}, mcpMock, nil, nil, nil, nil, nil, slog.Default())
	pod := podInfo{
		Metadata: podMetadata{
			Name:      "podinfo-7d4f9b6c8-abc12",
			Namespace: "podinfo",
			OwnerReferences: []ownerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "podinfo-7d4f9b6c8", Controller: true},
			},
		},
	}
	got := proc.resolvePodController(context.Background(), pod, map[string]string{})
	if got != "Deployment/podinfo" {
		t.Errorf("Controller = %q, want Deployment/podinfo", got)
	}
}

// TestResolvePodController_StatefulSetDirect: StatefulSet (and DaemonSet /
// Job) own pods directly — no extra hop, no MCP call needed.
func TestResolvePodController_StatefulSetDirect(t *testing.T) {
	mcpMock := &fakeMCPClient{
		err: fmt.Errorf("MCP must not be called for StatefulSet-owned pods"),
	}
	proc := New(&config.Config{}, mcpMock, nil, nil, nil, nil, nil, slog.Default())
	pod := podInfo{
		Metadata: podMetadata{
			Name:      "postgres-0",
			Namespace: "apps",
			OwnerReferences: []ownerReference{
				{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "postgres", Controller: true},
			},
		},
	}
	got := proc.resolvePodController(context.Background(), pod, map[string]string{})
	if got != "StatefulSet/postgres" {
		t.Errorf("Controller = %q, want StatefulSet/postgres", got)
	}
	if mcpMock.resourceCalls != 0 {
		t.Errorf("StatefulSet-owned pod made %d MCP reads, want 0", mcpMock.resourceCalls)
	}
}

// TestResolvePodController_NakedPod: pod with no ownerReferences (e.g.
// kubectl run / debug pods). Resolver returns "" so the renderer shows
// "—" rather than fabricating a controller.
func TestResolvePodController_NakedPod(t *testing.T) {
	proc := New(&config.Config{}, &fakeMCPClient{}, nil, nil, nil, nil, nil, slog.Default())
	pod := podInfo{Metadata: podMetadata{Name: "debug", Namespace: "ns"}}
	got := proc.resolvePodController(context.Background(), pod, map[string]string{})
	if got != "" {
		t.Errorf("Controller = %q, want empty string for naked pod", got)
	}
}

// TestResolvePodController_ReplicaSetFetchFailureFallsBack: if MCP can't
// deliver the RS YAML we keep going with the ReplicaSet identifier rather
// than dropping the controller column entirely.
func TestResolvePodController_ReplicaSetFetchFailureFallsBack(t *testing.T) {
	mcpMock := &fakeMCPClient{
		err: fmt.Errorf("simulated MCP failure"),
	}
	proc := New(&config.Config{}, mcpMock, nil, nil, nil, nil, nil, slog.Default())
	pod := podInfo{
		Metadata: podMetadata{
			Name:      "api-abc-def",
			Namespace: "apps",
			OwnerReferences: []ownerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api-abc", Controller: true},
			},
		},
	}
	got := proc.resolvePodController(context.Background(), pod, map[string]string{})
	if got != "ReplicaSet/api-abc" {
		t.Errorf("Controller = %q, want fallback ReplicaSet/api-abc", got)
	}
}

// TestResolvePodController_NakedReplicaSet: RS without ownerReferences →
// resolver surfaces the RS itself rather than emitting an empty controller.
func TestResolvePodController_NakedReplicaSet(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"apps/v1|ReplicaSet|orphan-rs-abc|ns": []byte(nakedReplicaSetYAML),
		},
	}
	proc := New(&config.Config{}, mcpMock, nil, nil, nil, nil, nil, slog.Default())
	pod := podInfo{
		Metadata: podMetadata{
			Name:      "orphan-rs-abc-xyz",
			Namespace: "ns",
			OwnerReferences: []ownerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "orphan-rs-abc", Controller: true},
			},
		},
	}
	got := proc.resolvePodController(context.Background(), pod, map[string]string{})
	if got != "ReplicaSet/orphan-rs-abc" {
		t.Errorf("Controller = %q, want ReplicaSet/orphan-rs-abc for naked RS", got)
	}
}

// TestResolvePodController_ReplicaSetCacheReusesPriorLookup: a Deployment
// with N pods all share the same ReplicaSet owner — the cache must dedupe
// the MCP fetch so we don't pay N round-trips for one workload.
func TestResolvePodController_ReplicaSetCacheReusesPriorLookup(t *testing.T) {
	mcpMock := &fakeMCPClient{
		resources: map[string][]byte{
			"apps/v1|ReplicaSet|podinfo-7d4f9b6c8|podinfo": []byte(replicaSetWithDeploymentOwnerYAML),
		},
	}
	proc := New(&config.Config{}, mcpMock, nil, nil, nil, nil, nil, slog.Default())
	cache := map[string]string{}
	for i, name := range []string{"podinfo-7d4f9b6c8-abc", "podinfo-7d4f9b6c8-def", "podinfo-7d4f9b6c8-ghi"} {
		pod := podInfo{
			Metadata: podMetadata{
				Name:      name,
				Namespace: "podinfo",
				OwnerReferences: []ownerReference{
					{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "podinfo-7d4f9b6c8", Controller: true},
				},
			},
		}
		got := proc.resolvePodController(context.Background(), pod, cache)
		if got != "Deployment/podinfo" {
			t.Errorf("pod %d Controller = %q, want Deployment/podinfo", i, got)
		}
	}
	// One entry in the cache for the shared RS, fetched once — proves dedup.
	if len(cache) != 1 {
		t.Errorf("expected cache to dedupe to a single entry, got %d: %v", len(cache), cache)
	}
	if mcpMock.resourceCalls != 1 {
		t.Errorf("three pods of one ReplicaSet made %d MCP reads, want 1", mcpMock.resourceCalls)
	}
}

// A failed ReplicaSet read isn't cached: the next scan retries it, so one
// transient MCP error doesn't hide the Deployment for the whole settle wait.
func TestResolvePodController_ReplicaSetFetchFailureNotCached(t *testing.T) {
	const key = "apps/v1|ReplicaSet|podinfo-7d4f9b6c8|podinfo"
	mcpMock := &fakeMCPClient{
		resources:    map[string][]byte{key: []byte(replicaSetWithDeploymentOwnerYAML)},
		resourceErrs: map[string]int{key: 1},
	}
	proc := New(&config.Config{}, mcpMock, nil, nil, nil, nil, nil, slog.Default())
	pod := podInfo{Metadata: podMetadata{
		Name: "podinfo-7d4f9b6c8-abc", Namespace: "podinfo",
		OwnerReferences: []ownerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "podinfo-7d4f9b6c8", Controller: true}},
	}}
	cache := map[string]string{}
	if got := proc.resolvePodController(context.Background(), pod, cache); got != "ReplicaSet/podinfo-7d4f9b6c8" {
		t.Fatalf("first scan Controller = %q, want the ReplicaSet fallback", got)
	}
	if got := proc.resolvePodController(context.Background(), pod, cache); got != "Deployment/podinfo" {
		t.Errorf("second scan Controller = %q, want Deployment/podinfo after the read recovered", got)
	}
}

// TestPodKstatus_FailingDelegates asserts the failing branch routes through
// isFailingPod — a CrashLoopBackOff pod must report Status=Failed with a
// human-readable message naming the container and reason.
func TestPodKstatus_FailingDelegates(t *testing.T) {
	pod := podInfo{
		Metadata: podMetadata{Name: "crash-1", Namespace: "ns"},
		Status: podStatus{Phase: "Running", ContainerStatuses: []containerStatus{
			{
				Name: "app",
				State: struct {
					Waiting *struct {
						Reason  string `yaml:"reason"`
						Message string `yaml:"message"`
					} `yaml:"waiting,omitempty"`
					Terminated *struct {
						Reason   string `yaml:"reason"`
						ExitCode int    `yaml:"exitCode"`
						Message  string `yaml:"message"`
					} `yaml:"terminated,omitempty"`
				}{Waiting: &struct {
					Reason  string `yaml:"reason"`
					Message string `yaml:"message"`
				}{Reason: "CrashLoopBackOff"}},
			},
		}},
	}
	got := podKstatus(pod, time.Now())
	if got.Status != "Failed" {
		t.Errorf("Status = %q, want Failed", got.Status)
	}
	if !strings.Contains(got.Message, "app") || !strings.Contains(got.Message, "CrashLoopBackOff") {
		t.Errorf("Message %q should mention container + reason", got.Message)
	}
}

// terminatedState builds the anonymous *Terminated struct of a container
// status, for fixtures that set State.Terminated or LastState.Terminated.
func terminatedState(reason string, exitCode int, message string) *struct {
	Reason   string `yaml:"reason"`
	ExitCode int    `yaml:"exitCode"`
	Message  string `yaml:"message"`
} {
	return &struct {
		Reason   string `yaml:"reason"`
		ExitCode int    `yaml:"exitCode"`
		Message  string `yaml:"message"`
	}{Reason: reason, ExitCode: exitCode, Message: message}
}

// TestPodKstatus_FailureMessage pins the Workload Status Message cell for
// failing pods: a terminated container shows its exit code (and termination
// message) instead of Kubernetes' bare "Error" reason, and a crash-looping or
// restarting container shows how its last run ended.
func TestPodKstatus_FailureMessage(t *testing.T) {
	now := time.Now()
	withContainer := func(phase string, mutate func(*containerStatus)) podInfo {
		cs := containerStatus{Name: "seed"}
		mutate(&cs)
		return podInfo{
			Metadata: podMetadata{Name: "job-abc", Namespace: "ns"},
			Status:   podStatus{Phase: phase, ContainerStatuses: []containerStatus{cs}},
		}
	}
	cases := []struct {
		name string
		pod  podInfo
		want string
	}{
		{
			name: "terminated with Error shows the exit code",
			pod: withContainer("Failed", func(cs *containerStatus) {
				cs.State.Terminated = terminatedState("Error", 1, "")
			}),
			want: "container `seed` exited with code 1",
		},
		{
			name: "termination message is appended on one line",
			pod: withContainer("Failed", func(cs *containerStatus) {
				cs.State.Terminated = terminatedState("Error", 2, "ERROR:  canceling statement\n  due to statement timeout\n")
			}),
			want: "container `seed` exited with code 2: ERROR: canceling statement due to statement timeout",
		},
		{
			name: "long termination message is cut",
			pod: withContainer("Failed", func(cs *containerStatus) {
				cs.State.Terminated = terminatedState("Error", 1, strings.Repeat("x", maxTerminationMessageRunes+50))
			}),
			want: "container `seed` exited with code 1: " + strings.Repeat("x", maxTerminationMessageRunes) + "…",
		},
		{
			name: "OOMKilled keeps the reason and adds the exit code",
			pod: withContainer("Failed", func(cs *containerStatus) {
				cs.State.Terminated = terminatedState("OOMKilled", 137, "")
			}),
			want: "container `seed` was OOMKilled (exit code 137)",
		},
		{
			name: "CrashLoopBackOff reports how the last run ended",
			pod: withContainer("Running", func(cs *containerStatus) {
				cs.RestartCount = 3
				cs.State.Waiting = &struct {
					Reason  string `yaml:"reason"`
					Message string `yaml:"message"`
				}{Reason: "CrashLoopBackOff"}
				cs.LastState.Terminated = terminatedState("Error", 1, "")
			}),
			want: "container `seed` CrashLoopBackOff; last run exited with code 1",
		},
		{
			name: "waiting reason without a previous run stays bare",
			pod: withContainer("Pending", func(cs *containerStatus) {
				cs.State.Waiting = &struct {
					Reason  string `yaml:"reason"`
					Message string `yaml:"message"`
				}{Reason: "ImagePullBackOff"}
			}),
			want: "container `seed` ImagePullBackOff",
		},
		{
			name: "restarting mid-cycle reports the previous termination",
			pod: withContainer("Running", func(cs *containerStatus) {
				cs.RestartCount = 1
				cs.LastState.Terminated = terminatedState("OOMKilled", 137, "")
			}),
			want: "container `seed` is restarting after it was OOMKilled (exit code 137)",
		},
		{
			name: "restarting keeps the exit code when the last reason is empty",
			pod: withContainer("Running", func(cs *containerStatus) {
				cs.RestartCount = 1
				cs.LastState.Terminated = terminatedState("", 1, "")
			}),
			want: "container `seed` is restarting after it exited with code 1",
		},
		{
			name: "CrashLoopBackOff keeps the exit code when the last reason is empty",
			pod: withContainer("Running", func(cs *containerStatus) {
				cs.RestartCount = 2
				cs.State.Waiting = &struct {
					Reason  string `yaml:"reason"`
					Message string `yaml:"message"`
				}{Reason: "CrashLoopBackOff"}
				cs.LastState.Terminated = terminatedState("", 3, "")
			}),
			want: "container `seed` CrashLoopBackOff; last run exited with code 3",
		},
		{
			name: "restarting without a recorded termination",
			pod: withContainer("Running", func(cs *containerStatus) {
				cs.RestartCount = 1
			}),
			want: "container `seed` is restarting",
		},
		{
			name: "pod-level failure keeps the bare reason",
			pod: podInfo{
				Metadata: podMetadata{Name: "job-abc", Namespace: "ns"},
				Status:   podStatus{Phase: "Failed"},
			},
			want: "Failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := podKstatus(tc.pod, now)
			if got.Status != "Failed" {
				t.Fatalf("Status = %q, want Failed", got.Status)
			}
			if got.Message != tc.want {
				t.Errorf("Message = %q, want %q", got.Message, tc.want)
			}
		})
	}
}
