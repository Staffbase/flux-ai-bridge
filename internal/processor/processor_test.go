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
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
	corev1 "k8s.io/api/core/v1"
)

func TestSplitOwnerRepo(t *testing.T) {
	tests := []struct {
		input     string
		wantOwner string
		wantRepo  string
		wantError bool
	}{
		{input: "owner/repo", wantOwner: "owner", wantRepo: "repo"},
		{input: "myorg/my-infra", wantOwner: "myorg", wantRepo: "my-infra"},
		{input: "  owner/repo  ", wantOwner: "owner", wantRepo: "repo"},
		{input: "noslash", wantError: true},
		{input: "/repo", wantError: true},
		{input: "owner/", wantError: true},
		{input: "", wantError: true},
		{input: "a/b/c", wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			owner, repo, err := splitOwnerRepo(tc.input)
			if tc.wantError {
				if err == nil {
					t.Errorf("splitOwnerRepo(%q) expected error, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitOwnerRepo(%q) unexpected error: %v", tc.input, err)
			}
			if owner != tc.wantOwner || repo != tc.wantRepo {
				t.Errorf("splitOwnerRepo(%q) = (%q,%q), want (%q,%q)", tc.input, owner, repo, tc.wantOwner, tc.wantRepo)
			}
		})
	}
}

func TestParseGitHubRepoURL(t *testing.T) {
	tests := []struct {
		url       string
		wantOwner string
		wantRepo  string
		wantError bool
	}{
		{url: "https://github.com/owner/repo", wantOwner: "owner", wantRepo: "repo"},
		{url: "https://github.com/owner/repo.git", wantOwner: "owner", wantRepo: "repo"},
		{url: "git@github.com:owner/repo.git", wantOwner: "owner", wantRepo: "repo"},
		{url: "git@github.com:owner/repo", wantOwner: "owner", wantRepo: "repo"},
		{url: "https://gitlab.com/owner/repo", wantError: true},
		{url: "not-a-url", wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.url, func(t *testing.T) {
			owner, repo, err := parseGitHubRepoURL(tc.url)
			if tc.wantError {
				if err == nil {
					t.Errorf("parseGitHubRepoURL(%q) expected error, got nil", tc.url)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseGitHubRepoURL(%q) unexpected error: %v", tc.url, err)
			}
			if owner != tc.wantOwner || repo != tc.wantRepo {
				t.Errorf("parseGitHubRepoURL(%q) = (%q,%q), want (%q,%q)", tc.url, owner, repo, tc.wantOwner, tc.wantRepo)
			}
		})
	}
}

func TestInventoryRefsFromYAML(t *testing.T) {
	t.Run("with entries", func(t *testing.T) {
		yaml := `
status:
  inventory:
    entries:
      - id: "default_nginx_apps_Deployment"
      - id: "default_web__Service"
`
		refs, err := inventoryRefsFromYAML([]byte(yaml))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(refs) != 2 {
			t.Fatalf("expected 2 refs, got %d", len(refs))
		}
		if refs[0] != "default_nginx_apps_Deployment" {
			t.Errorf("refs[0] = %q, want %q", refs[0], "default_nginx_apps_Deployment")
		}
		if refs[1] != "default_web__Service" {
			t.Errorf("refs[1] = %q, want %q", refs[1], "default_web__Service")
		}
	})

	t.Run("empty inventory", func(t *testing.T) {
		yaml := `status: {}`
		refs, err := inventoryRefsFromYAML([]byte(yaml))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(refs) != 0 {
			t.Errorf("expected 0 refs, got %d", len(refs))
		}
	})

	t.Run("skips empty IDs", func(t *testing.T) {
		yaml := `
status:
  inventory:
    entries:
      - id: "default_nginx_apps_Deployment"
      - id: ""
`
		refs, err := inventoryRefsFromYAML([]byte(yaml))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(refs) != 1 {
			t.Errorf("expected 1 ref (empty ID skipped), got %d", len(refs))
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		tabIndented := []byte("\t invalid")
		_, err := inventoryRefsFromYAML(tabIndented)
		if err == nil {
			t.Fatal("expected error for invalid YAML, got nil")
		}
	})
}

// TestBuildPrompt_AllCurrentCollapses pins the token-frugal readiness summary:
// when every resource/pod is Current, the section collapses to a one-line count
// with no per-row enumeration (a 1000-resource Kustomization → one line).
func TestBuildPrompt_AllCurrentCollapses(t *testing.T) {
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "default", Name: "api", Status: "Current", Message: "ready"},
		{Kind: "Service", Namespace: "default", Name: "web", Status: "Current"},
	}
	pods := []readiness.ResourceStatus{
		{Kind: "Pod", Namespace: "default", Name: "api-x", Status: "Current", Controller: "Deployment/api"},
	}
	prompt := buildPrompt(event, "abc1234", "kustomization-yaml", statuses, pods, true)

	if !strings.Contains(prompt, "All 2 resources reached Current.") {
		t.Errorf("expected collapsed resource count line, got: %q", prompt)
	}
	if !strings.Contains(prompt, "All 1 pods reached Current.") {
		t.Errorf("expected collapsed pod count line, got: %q", prompt)
	}
	// No individual Current rows enumerated.
	if strings.Contains(prompt, "Deployment/default/api") || strings.Contains(prompt, "default/api-x (") {
		t.Errorf("all-Current sections must not list individual rows: %q", prompt)
	}
}

// TestBuildPrompt_ReadinessTimeout pins the timeout line: an unsettled poll
// makes the outcome at least a warning even when every row in the snapshot is
// Current, so the payload must say so — the skill's healthy check keys on it.
// A settled poll adds no line.
func TestBuildPrompt_ReadinessTimeout(t *testing.T) {
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "default", Name: "api", Status: "Current", Message: "ready"},
	}
	const line = "Readiness poll timed out"

	unsettled := buildPrompt(event, "abc1234", "kustomization-yaml", statuses, nil, false)
	if !strings.Contains(unsettled, "## Resource Readiness\n"+line) {
		t.Errorf("unsettled poll: Resource Readiness must open with the timeout line, got: %q", unsettled)
	}
	if !strings.Contains(unsettled, "All 1 resources reached Current.") {
		t.Errorf("unsettled poll: count line must still follow the timeout line, got: %q", unsettled)
	}

	if settled := buildPrompt(event, "abc1234", "kustomization-yaml", statuses, nil, true); strings.Contains(settled, line) {
		t.Errorf("settled poll must not carry the timeout line, got: %q", settled)
	}
}

func TestBuildPrompt(t *testing.T) {
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Kustomization",
			Name:      "apps",
			Namespace: "default",
		},
		Severity: eventv1.EventSeverityInfo,
		Reason:   "ReconciliationSucceeded",
		Message:  "Applied revision: main@sha1:abc1234",
	}
	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "default", Name: "api", Status: "Current", Message: "ready"},
		{Kind: "HelmRelease", Namespace: "default", Name: "web", Status: "Failed", Message: "InstallFailed"},
	}

	prompt := buildPrompt(event, "abc1234", "kustomization-yaml", statuses, nil, true)

	// buildPrompt is data-only since the instructions live in SKILL.md.
	// Assert the data sections; do NOT assert "Instructions" here — that section was deliberately
	// removed to keep the skill the single source of truth.
	mustContain := []string{
		"Flux Event",
		"default/apps",
		"ReconciliationSucceeded",
		"abc1234",
		"Kustomization YAML",
		"kustomization-yaml",
		"Resource Readiness",
		"1 of 2 resources reached Current", // count line precedes the failures
		"HelmRelease/default/web: Failed",  // non-Current row IS listed
	}
	for _, want := range mustContain {
		if !strings.Contains(prompt, want) {
			t.Errorf("buildPrompt output missing %q", want)
		}
	}
	// Current rows are summarised away (token frugality) — only the count line
	// represents them, never an individual row.
	if strings.Contains(prompt, "Deployment/default/api") {
		t.Error("buildPrompt should summarise Current rows into the count, not list them individually")
	}
	// Regression guard: the skill is authoritative; buildPrompt MUST NOT
	// emit instruction text. If you find yourself adding back an
	// "## Instructions" header, update SKILL.md instead.
	if strings.Contains(prompt, "## Instructions") {
		t.Error("buildPrompt re-introduced an '## Instructions' section; move guidance to SKILL.md")
	}
}

// formatComment lifts the status marker (✅ / ❌ / ⚠️ or text equivalent)
// out of the LLM body's first line and stamps it into the header itself so
// PR reviewers see the outcome at a glance. The body section starts with
// "## Kustomization Status" — the status line is consumed by the header lift.
// Verify the full canonical shape for each of the three statuses, the
// text-mode markers (which are mapped onto the same emoji set), and the
// idempotence invariant.
func TestFormatComment_HeaderCarriesStatusEmoji(t *testing.T) {
	const footer = "---\n*Analyzed by flux-ai-bridge*"
	bodySections := "## Kustomization Status\nall green"

	cases := []struct {
		name       string
		in         string
		wantHeader string
	}{
		{"emoji healthy", "✅\n\n" + bodySections, "# ✅ Flux Status"},
		{"emoji failure", "❌\n\n" + bodySections, "# ❌ Flux Status"},
		{"emoji warning", "⚠️\n\n" + bodySections, "# ⚠️ Flux Status"},
		{"emoji warning without variation selector", "⚠\n\n" + bodySections, "# ⚠️ Flux Status"},
		{"text success", "Success: clean\n\n" + bodySections, "# ✅ Flux Status"},
		{"text failure", "Failure: pods crashing\n\n" + bodySections, "# ❌ Flux Status"},
		{"text warning", "Warning: in progress\n\n" + bodySections, "# ⚠️ Flux Status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatComment(tc.in, true, "")
			want := tc.wantHeader + "\n\n" + bodySections + "\n\n" + footer
			if got != want {
				t.Errorf("formatComment mismatch:\nwant:\n%q\ngot:\n%q", want, got)
			}
			if again := formatComment(got, true, ""); again != got {
				t.Errorf("not idempotent:\nfirst:\n%q\nsecond:\n%q", got, again)
			}
		})
	}
}

// Defensive: if the LLM ignored the skill and emitted its own header
// and/or footer, formatComment must strip them before re-stamping so the
// final comment has exactly one of each.
func TestFormatComment_StripsLLMEmittedHeaderAndFooter(t *testing.T) {
	const footer = "---\n*Analyzed by flux-ai-bridge*"
	body := "✅\n\n## Kustomization Status\nall green"
	want := "# ✅ Flux Status\n\n## Kustomization Status\nall green\n\n" + footer

	cases := []string{
		"# Flux Status\n\n" + body,                              // old bare header
		"# ✅ Flux Status\n\n## Kustomization Status\nall green", // already-wrapped emoji header (no status line)
		"# ❌ Flux Status\n\n" + body,                            // wrong emoji in pre-existing header
		body + "\n\n" + footer,                                  // footer alone
		"# Flux Status\n\n" + body + "\n\n" + footer,            // both
		"   \n\n" + body + "\n\n   ",                            // surrounding whitespace
	}
	for i, tc := range cases {
		got := formatComment(tc, true, "")
		if got != want {
			t.Errorf("case %d (%q) mismatch:\nwant:\n%q\ngot:\n%q", i, tc, want, got)
		}
		if again := formatComment(got, true, ""); again != got {
			t.Errorf("case %d not idempotent: first=%q second=%q", i, got, again)
		}
	}
}

// Empty input still produces a structurally valid wrapped comment — no
// crashes, header (bare, no emoji since none can be inferred) and footer
// both present, body section empty.
func TestFormatComment_EmptyBody(t *testing.T) {
	got := formatComment("", true, "")
	if !strings.HasPrefix(got, "# Flux Status\n\n") {
		t.Errorf("missing header on empty body: %q", got)
	}
	if !strings.HasSuffix(got, "---\n*Analyzed by flux-ai-bridge*") {
		t.Errorf("missing footer on empty body: %q", got)
	}
}

// If the first line isn't a recognized status marker we preserve the body
// untouched (don't accidentally eat a "## Kustomization Status" header) and
// emit the bare "# Flux Status" header.
func TestFormatComment_UnknownFirstLinePreservesBody(t *testing.T) {
	body := "## Kustomization Status\nthe model forgot the status line"
	got := formatComment(body, true, "")
	want := "# Flux Status\n\n" + body + "\n\n---\n*Analyzed by flux-ai-bridge*"
	if got != want {
		t.Errorf("unknown-first-line case mismatch:\nwant:\n%q\ngot:\n%q", want, got)
	}
}

func TestCompactDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{15 * time.Minute, "15m"},
		{time.Hour, "1h"},
		{90 * time.Minute, "90m"},
		{30 * time.Second, "30s"},
		{200 * time.Second, "200s"}, // 3m20s — must NOT mangle to "3m2"
		{90 * time.Second, "90s"},   // 1m30s — must NOT mangle to "1m3"
		{2 * time.Hour, "2h"},
		{500 * time.Millisecond, "500ms"}, // sub-second falls back to stdlib
	}
	for _, c := range cases {
		if got := compactDuration(c.d); got != c.want {
			t.Errorf("compactDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestRetryPolicyNote(t *testing.T) {
	cases := []struct {
		name    string
		ttl     time.Duration
		max     int
		outcome bridgegithub.Outcome
		want    string
	}{
		{"failure, counted", 15 * time.Minute, 4, bridgegithub.OutcomeFailure,
			"🔁 Re-evaluated automatically up to 4×, at most once every 15m, until it passes."},
		{"warning, counted", 15 * time.Minute, 4, bridgegithub.OutcomeWarning,
			"🔁 Re-evaluated automatically up to 4×, at most once every 15m, until it passes."},
		{"failure, unlimited", 30 * time.Minute, 0, bridgegithub.OutcomeFailure,
			"🔁 Re-evaluated automatically, at most once every 30m, until it passes."},
		{"success → no note", 15 * time.Minute, 4, bridgegithub.OutcomeSuccess, ""},
		{"reeval disabled → no note", 0, 4, bridgegithub.OutcomeFailure, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &config.Config{DedupePRReEvalTTL: c.ttl, DedupePRReEvalMax: c.max}
			if got := retryPolicyNote(cfg, c.outcome); got != c.want {
				t.Errorf("retryPolicyNote = %q, want %q", got, c.want)
			}
		})
	}
}

// The retry note re-wraps cleanly (byte-stable) whether the attribution byline
// is on or off — it's operational info, not branding, so it renders regardless
// of COMMENT_INCLUDE_FOOTER.
func TestFormatComment_RetryNoteIdempotent(t *testing.T) {
	note := "🔁 Re-evaluated automatically up to 4×, at most once every 15m, until it passes."

	// Footer ON: note sits directly under the byline.
	got := formatComment("❌\n\n## Resource Status\nrows", true, note)
	if !strings.HasSuffix(got, "---\n*Analyzed by flux-ai-bridge*\n"+note) {
		t.Fatalf("retry note not rendered under footer:\n%q", got)
	}
	if again := formatComment(got, true, note); again != got {
		t.Errorf("not idempotent with retry note (footer on):\nfirst:\n%q\nsecond:\n%q", got, again)
	}
	// Re-wrapping WITHOUT the note (e.g. the deploy recovered) must drop the old
	// note rather than carry it through.
	recovered := formatComment(got, true, "")
	if strings.Contains(recovered, note) {
		t.Errorf("stale retry note survived a no-note re-wrap:\n%q", recovered)
	}

	// Footer OFF: the byline is suppressed but the retry note still renders
	// (decoupled from COMMENT_INCLUDE_FOOTER), and re-wrapping stays stable.
	noByline := formatComment("❌\n\n## Resource Status\nrows", false, note)
	if strings.Contains(noByline, commentFooter) {
		t.Errorf("byline rendered despite footer off:\n%q", noByline)
	}
	if !strings.HasSuffix(noByline, "\n"+note) {
		t.Fatalf("retry note missing when footer off:\n%q", noByline)
	}
	if again := formatComment(noByline, false, note); again != noByline {
		t.Errorf("not idempotent with retry note (footer off):\nfirst:\n%q\nsecond:\n%q", noByline, again)
	}
}

// formatComment must NOT delete a genuine LLM line that merely starts with the
// 🔁 emoji (e.g. a "Next steps" bullet) — the strip anchors on the bridge's full
// note phrase, not the bare emoji.
func TestFormatComment_PreservesLLMLineStartingWithRetryEmoji(t *testing.T) {
	note := "🔁 Re-evaluated automatically up to 4×, at most once every 15m, until it passes."
	llmTail := "🔁 Restart the deployment to clear the CrashLoopBackOff."
	body := "❌\n\n## Resource Status\nrows\n\n## Next steps to fix\n" + llmTail

	got := formatComment(body, true, note)
	if !strings.Contains(got, llmTail) {
		t.Errorf("LLM line starting with 🔁 was deleted:\n%q", got)
	}
	if !strings.HasSuffix(got, "\n"+note) {
		t.Errorf("retry note not appended after the LLM body:\n%q", got)
	}
	// Re-wrapping is still idempotent: the bridge note is stripped+re-added, the
	// LLM 🔁 line is preserved.
	again := formatComment(got, true, note)
	if again != got {
		t.Errorf("not idempotent with an LLM 🔁 line present:\nfirst:\n%q\nsecond:\n%q", got, again)
	}
	if !strings.Contains(again, llmTail) {
		t.Errorf("LLM 🔁 line lost on re-wrap:\n%q", again)
	}
}

// kustomizationEmptyInventory is a minimal kustomization YAML the test
// processor returns for "GetKustomizationStatus" — no inventory means the
// readiness check has zero rows to fetch, so the test stays focused on the
// post-LLM PR-commenting logic without dragging in per-resource MCP fakes.
//
// Inventory-based PR attribution would skip a PR for an empty-inventory
// Kustomization (nothing to match against), so tests that want to reach
// the LLM/post path use kustomizationOneInventoryEntry below with a
// commenter wired to declare a matching blob.
const kustomizationEmptyInventory = "status: {}"

// kustomizationOneInventoryEntry pairs with a fakeCommenter whose default
// blob declares ConfigMap/default/any — the inventory ID built by the
// processor for that resource is "default_any__ConfigMap", which is what
// this YAML lists. Tests using this fixture get to focus on the post-LLM
// PR-commenting logic without each constructing their own blob+inventory.
const kustomizationOneInventoryEntry = `
status:
  inventory:
    entries:
      - id: "default_any__ConfigMap"
`

func TestProcess_PostsPRComment(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	analyzerMock := &fakeAnalyzer{result: "## Status\nAll resources are ready."}
	commenterMock := &fakeCommenter{prNumber: 42}

	// CommentIncludeFooter explicitly set because Go's zero value (`false`)
	// differs from config.Load's documented default (`true`). Inline Config
	// literals don't go through Load, so any test that asserts on the
	// footer presence needs the flag set explicitly.
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", CommentIncludeFooter: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
		Message:        "Applied revision",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}

	if commenterMock.owner != "owner" || commenterMock.repo != "repo" {
		t.Errorf("owner/repo = %q/%q, want owner/repo", commenterMock.owner, commenterMock.repo)
	}
	// Comment shape (LLM mode): header → bridge marker → bridge summary
	// (CommentIncludeSummary defaults false here since we don't set it) →
	// bridge tables → LLM narrative → footer. Tables on top of narrative
	// so reviewers see kstatus data first.
	//
	// The single inventory entry (`default_any__ConfigMap`) isn't supplied
	// by the fakeMCPClient → NotFound row → ❌ marker. fakeAnalyzer.result
	// has no status marker on its first line, so we just see it as
	// narrative content below the tables.
	wantTable := "## Resource Status\n" +
		"| Namespace | Kind | Name | Status | Message |\n" +
		"|---|---|---|---|---|\n" +
		"| default | ConfigMap | any | ❌ NotFound | resource not found |\n"
	wantBody := "# ❌ Flux Status\n\n" + wantTable + "\n" + analyzerMock.result + "\n\n---\n*Analyzed by flux-ai-bridge*"
	if commenterMock.prCommentBody != wantBody {
		t.Errorf("comment body = %q, want %q", commenterMock.prCommentBody, wantBody)
	}
	if commenterMock.prNumber != 42 {
		t.Errorf("pr number = %d, want 42", commenterMock.prNumber)
	}
}

// helmReleaseReadyNoHistoryYAML is the edge case computeOutcome's
// helmReleasesPending must independently guard against: a HelmRelease
// reporting Ready=True (so the readiness checker's OWN row reads ✅ Current)
// while status.history is still empty (so expandHelmReleaseInventories has
// nothing to expand — its rendered children, e.g. an ImageUpdateAutomation,
// stay invisible). Both reads hit this exact fixture, so this reproduces —
// deterministically, without needing to race two separate MCP calls — the
// scenario flagged in review: the Kustomization's own tables can look
// entirely clean while the inventory view is genuinely incomplete.
const helmReleaseReadyNoHistoryYAML = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: default
status:
  conditions:
    - type: Ready
      status: "True"
      reason: ReconciliationSucceeded
      message: "Release reconciliation succeeded"
`

// TestProcess_PendingHelmRelease_ForcesWarningDespiteCleanTables is the
// Process-level regression test for the review finding that pendingHelmReleases
// only affected the comment TEXT, never computeOutcome/suppression/labels: a
// HelmRelease that reads Current on its own (Ready=True) but has no
// status.history yet must still downgrade the outcome to warning — NOT
// success — so COMMENT_ON_SUCCESS=false can't silently suppress the comment,
// and the PR label can't read flux-success, on a run whose inventory view is
// provably incomplete.
func TestProcess_PendingHelmRelease_ForcesWarningDespiteCleanTables(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte(`
status:
  inventory:
    entries:
      - id: "default_app_helm.toolkit.fluxcd.io_HelmRelease"
`),
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|default": []byte(helmReleaseReadyNoHistoryYAML),
		},
	}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles:  []bridgegithub.PRFile{{Path: "release.yaml", BlobSHA: "hr-blob", Status: "modified"}},
		blobs: map[string][]byte{
			"hr-blob": []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: app\n  namespace: default\n"),
		},
	}
	cfg := &config.Config{
		GitHubRepo:                  "owner/repo",
		ReadinessExpandHelmReleases: true,
		ReadinessPollInterval:       time.Millisecond,
		ReadinessPollTimeout:        10 * time.Millisecond,
		CommentOnSuccess:            false, // the exact setting that would have swallowed the comment entirely under the bug
		PRLabelEnabled:              true,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, nil /* no LLM */, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}

	if commenterMock.prCommentBody == "" {
		t.Fatal("expected a comment to be posted despite CommentOnSuccess=false — a pending HelmRelease means this run is NOT a genuine success")
	}
	if !strings.Contains(commenterMock.prCommentBody, "Incomplete picture") {
		t.Errorf("expected the pending-HelmRelease note in the comment body, got: %s", commenterMock.prCommentBody)
	}
	if !strings.Contains(commenterMock.prCommentBody, "# ⚠️ Flux Status") {
		t.Errorf("expected a warning marker (not success), got: %s", commenterMock.prCommentBody)
	}
	if len(commenterMock.labelCalls) == 0 {
		t.Fatal("expected a label call")
	}
	if got := commenterMock.labelCalls[len(commenterMock.labelCalls)-1].Outcome; got == bridgegithub.OutcomeSuccess {
		t.Errorf("outcome = %v, must not be Success while a HelmRelease's inventory is still incomplete", got)
	}
}

// TestProcess_PendingHelmRelease_KeepsLLMInLoop covers the review finding
// that the pending signal wasn't wired into the LLM path: with
// LLM_SKIP_ON_HEALTHY=true, a pending-only warning (all tables Current,
// settled) must NOT take the "fully healthy" skip — outcome is Warning, not
// Success — and the LLM prompt payload must carry the pending-release
// caveat, so the model can't describe the run as healthy while
// contradicting the deterministic marker/note.
func TestProcess_PendingHelmRelease_KeepsLLMInLoop(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte(`
status:
  inventory:
    entries:
      - id: "default_app_helm.toolkit.fluxcd.io_HelmRelease"
`),
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|app|default": []byte(helmReleaseReadyNoHistoryYAML),
		},
	}
	analyzerMock := &fakeAnalyzer{result: "## Status\nAll good."}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles:  []bridgegithub.PRFile{{Path: "release.yaml", BlobSHA: "hr-blob", Status: "modified"}},
		blobs: map[string][]byte{
			"hr-blob": []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: app\n  namespace: default\n"),
		},
	}
	cfg := &config.Config{
		GitHubRepo:                  "owner/repo",
		LLMProvider:                 "claude",
		LLMSkipOnHealthy:            true,
		ReadinessExpandHelmReleases: true,
		ReadinessPollInterval:       time.Millisecond,
		ReadinessPollTimeout:        10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}

	if analyzerMock.calls == 0 {
		t.Fatal("expected the LLM to be called — a pending HelmRelease is not a healthy skip, despite LLM_SKIP_ON_HEALTHY=true")
	}
	if !strings.Contains(analyzerMock.lastUserPayload, "Incomplete picture") {
		t.Errorf("expected the pending-HelmRelease note in the LLM prompt payload, got: %s", analyzerMock.lastUserPayload)
	}
}

// TestProcess_ReportsWholeInventory pins the whole-Kustomization reporting: the
// Kustomization owns three resources across three namespaces, and the PR only
// changes the one in `grafana` — but the comment must report the ENTIRE
// inventory (all three), because a change can break resources it didn't
// directly touch (collateral failure). Attribution still uses the PR's diff to
// decide the PR earns a comment; the report is the full deploy unit.
func TestProcess_ReportsWholeInventory(t *testing.T) {
	const inventory = `
status:
  inventory:
    entries:
      - id: "grafana_dashboards__ConfigMap"
      - id: "kube-system_coredns__ConfigMap"
      - id: "monitoring_prom__ConfigMap"
`
	mcpMock := &fakeMCPClient{data: []byte(inventory)}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles:  []bridgegithub.PRFile{{Path: "grafana/dash.yaml", BlobSHA: "grafana-blob", Status: "modified"}},
		blobs: map[string][]byte{
			// The PR's changed file declares only the grafana ConfigMap — that's
			// enough for attribution; the report still covers all three.
			"grafana-blob": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: dashboards\n  namespace: grafana\n"),
		},
	}
	// LLM disabled, full table (failedResourcesOnly=false) → every inventory row
	// is rendered. The MCP fake supplies none of them → NotFound rows; the
	// status is irrelevant, what matters is that all three resources appear.
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "grafana", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}

	body := commenterMock.prCommentBody
	if body == "" {
		t.Fatal("expected a comment to be posted")
	}
	// The whole inventory must be reported — including the resources the PR did
	// NOT touch (this is the collateral-failure visibility we want).
	for _, want := range []string{"dashboards", "coredns", "prom"} {
		if !strings.Contains(body, want) {
			t.Errorf("comment must report the whole inventory; missing %q: %q", want, body)
		}
	}
}

// TestProcess_PRLabel_AppliedOnFailureOutcome verifies the
// PRLabelEnabled wiring: with the toggle on and a failure outcome (no
// inventory data found → NotFound row → failure marker), the bridge
// calls SetPRLabels(outcome=failure) on the target PR.
func TestProcess_PRLabel_AppliedOnFailureOutcome(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{prNumber: 42}

	cfg := &config.Config{
		GitHubRepo:            "owner/repo",
		LLMProvider:           "none",
		PRLabelEnabled:        true,
		ReadinessPollInterval: time.Millisecond,
		ReadinessPollTimeout:  10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if len(commenterMock.labelCalls) != 1 {
		t.Fatalf("expected exactly one SetPRLabels call, got %d (%+v)", len(commenterMock.labelCalls), commenterMock.labelCalls)
	}
	got := commenterMock.labelCalls[0]
	if got.Number != 42 || got.Outcome != bridgegithub.OutcomeFailure {
		t.Errorf("got %+v, want {Number: 42, Outcome: failure}", got)
	}
}

// TestProcess_PRLabel_NotCalledWhenDisabled is the negative companion:
// the default config (PRLabelEnabled=false) must never reach the label
// surface. Operators not opting in must see zero API calls to the label
// endpoint regardless of the comment outcome.
func TestProcess_PRLabel_NotCalledWhenDisabled(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{prNumber: 42}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond} // PRLabelEnabled defaults false
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if len(commenterMock.labelCalls) != 0 {
		t.Fatalf("expected no SetPRLabels calls when disabled, got %+v", commenterMock.labelCalls)
	}
}

// TestProcess_PRLabel_FailureIsNonFatal pins the "label failure must not
// tank the pipeline" contract: SetPRLabels returns an error, but the
// comment has already landed (PostOrUpdateComment ran), so Process must
// still return nil.
func TestProcess_PRLabel_FailureIsNonFatal(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		labelErr: errors.New("simulated label API error"),
	}

	cfg := &config.Config{
		GitHubRepo:            "owner/repo",
		LLMProvider:           "none",
		PRLabelEnabled:        true,
		ReadinessPollInterval: time.Millisecond,
		ReadinessPollTimeout:  10 * time.Millisecond,
	}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() must succeed even when label call errors; got %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("expected comment to be posted before label call; got %d posts", len(commenterMock.posts))
	}
}

// A label write that fails after the comment landed must stay retryable: with
// re-eval off, a kept admission mark would skip every later event for this
// head, leaving the label wrong until a new commit. The next event retries the
// label without reposting the (current) comment.
func TestProcess_PRLabel_FailureRetriedOnNextEvent(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{
		prNumber: 42, prState: "open", prHeadSHA: "h1",
		labelErr:      errors.New("simulated label API error"),
		stabilityKeys: map[int]string{},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	commenterMock.labelErr = nil // GitHub recovered
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if n := len(commenterMock.labelCalls); n != 2 || commenterMock.labelCalls[1].Outcome != bridgegithub.OutcomeSuccess {
		t.Fatalf("the failed label must be retried on the next event; got %+v", commenterMock.labelCalls)
	}
	if len(commenterMock.posts) != 1 {
		t.Errorf("the current comment must not be reposted for the label retry; got %d posts", len(commenterMock.posts))
	}
}

// TestProcess_TablesPrecedeLLMNarrative pins the design contract: the
// bridge stamps the kstatus tables on TOP of the comment (matching the
// no-LLM body), and the LLM's narrative is appended BELOW them. Tables on
// top so reviewers see the canonical readiness data first; analysis below
// so it can reference rows the reader can already see.
//
// Also verifies the legacy-marker strip: fakeAnalyzer.result starts with
// "✅\n\n…" — that emoji must NOT appear as a second marker in the body
// (the bridge's marker on line 1 is authoritative).
func TestProcess_TablesPrecedeLLMNarrative(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	analyzerMock := &fakeAnalyzer{result: "✅\n\n## Kustomization Status\nAll good."}
	commenterMock := &fakeCommenter{prNumber: 42}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", CommentIncludeFooter: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	body := commenterMock.prCommentBody

	// LLM narrative section must appear (the LLM still authors prose).
	if !strings.Contains(body, "## Kustomization Status\nAll good.") {
		t.Errorf("LLM analysis missing from posted body: %q", body)
	}
	// Resource Status table comes BEFORE the LLM narrative.
	rsIdx := strings.Index(body, "## Resource Status")
	ksIdx := strings.Index(body, "## Kustomization Status")
	if rsIdx < 0 || ksIdx < 0 {
		t.Fatalf("expected both bridge and LLM sections; got: %q", body)
	}
	if rsIdx >= ksIdx {
		t.Errorf("Resource Status table should appear BEFORE Kustomization Status (tables on top); Resource Status at %d, Kustomization Status at %d; body: %q", rsIdx, ksIdx, body)
	}
	// Same NotFound row the no-LLM path would have produced.
	if !strings.Contains(body, "| default | ConfigMap | any | ❌ NotFound | resource not found |") {
		t.Errorf("expected deterministic NotFound row in Resource Status table: %q", body)
	}
	// Legacy-marker strip: the LLM's "✅" first-line marker must NOT
	// survive into the body — the bridge's marker (the ❌ in the header,
	// from the NotFound row) is the single source of truth.
	if strings.Contains(body, "\n\n✅\n") || strings.HasSuffix(strings.TrimRight(body, " \t\r\n"), "✅") {
		t.Errorf("LLM-emitted ✅ marker should be stripped before appending: %q", body)
	}
}

// A merged PR is dropped by filterPRs, so the bridge reaches "no targets" and
// posts nothing. (Dedup is per-PR-head now; there's no source-SHA entry to
// evict here anymore.)
func TestProcess_MergedPR_SkipsComment(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationEmptyInventory)}
	analyzerMock := &fakeAnalyzer{result: "analysis"}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "closed", merged: true}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "deadbeef"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("PostOrUpdateComment must not be called for a merged PR (got body=%q)", commenterMock.prCommentBody)
	}
}

func TestProcess_ClosedWithoutMerge_SkipsComment(t *testing.T) {
	commenterMock := &fakeCommenter{prNumber: 42, prState: "closed", merged: false}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	mcpMock := &fakeMCPClient{data: []byte(kustomizationEmptyInventory)}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg,
		mcpMock,
		checker, &fakeAnalyzer{result: "analysis"}, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "deadbeef"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("PostOrUpdateComment must not be called for a closed-without-merge PR")
	}
}

// TestProcess_PerPRDedup_SkipsUnchangedHead is the core regression for the
// reported symptom: an open PR must be analysed once per ITS OWN head commit.
// Two events carrying DIFFERENT Flux source revisions but resolving to the
// SAME open PR at the SAME head SHA must produce exactly ONE post — the second
// is deduped before the readiness poll + LLM even run.
func TestProcess_PerPRDedup_SkipsUnchangedHead(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	// Two unrelated source revisions, same PR head.
	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if err := proc.Process(context.Background(), event, "srcrev-bbb"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("expected exactly 1 post for an unchanged PR head across 2 source revisions; got %d", len(commenterMock.posts))
	}
}

// A new PR head SHA must be re-analysed even though the PR number is the same.
func TestProcess_PerPRDedup_ReanalysesOnNewHead(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	ddc := dedupe.NewNoExpiry(16)
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, ddc, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	// PR gets a new commit pushed → new head SHA.
	commenterMock.prHeadSHA = "prhead222"
	if err := proc.Process(context.Background(), event, "srcrev-bbb"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 2 {
		t.Fatalf("expected 2 posts (one per distinct PR head); got %d", len(commenterMock.posts))
	}
}

// With re-eval on and failed-only (the default), a re-eval of a PR-head whose
// last analysis was HEALTHY must be skipped — no second readiness poll, no
// re-post — so healthy deploys don't burn MCP traffic every window.
func TestProcess_ReEvalFailedOnly_SkipsPreviouslyHealthyHead(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: true,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("first run should post the healthy comment; got %d", len(commenterMock.posts))
	}
	time.Sleep(3 * time.Millisecond) // let the per-PR re-eval floor elapse
	if err := proc.Process(context.Background(), event, "srcrev-bbb"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("re-eval of a previously-healthy head must be skipped (no re-post); got %d posts", len(commenterMock.posts))
	}
}

// Control: with failed-only disabled, the same healthy head IS re-evaluated on
// re-eval (re-polled and re-posted) — proving the skip is gated by the flag,
// not an unrelated dedup.
func TestProcess_ReEvalAll_RepostsPreviouslyHealthyHead(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: false,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	if err := proc.Process(context.Background(), event, "srcrev-bbb"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 2 {
		t.Fatalf("with failed-only OFF, a re-eval must re-post even a healthy head; got %d posts", len(commenterMock.posts))
	}
}

func TestAggregatePRLabelOutcome(t *testing.T) {
	f, s, w := string(bridgegithub.OutcomeFailure), string(bridgegithub.OutcomeSuccess), string(bridgegithub.OutcomeWarning)
	cases := []struct {
		name string
		in   map[string]string
		want bridgegithub.Outcome
	}{
		{"all success", map[string]string{"a": s, "b": s}, bridgegithub.OutcomeSuccess},
		{"any failure wins", map[string]string{"a": s, "b": f}, bridgegithub.OutcomeFailure},
		{"failure alone", map[string]string{"a": f}, bridgegithub.OutcomeFailure},
		{"warning blocks success", map[string]string{"a": s, "b": w}, bridgegithub.OutcomeWarning},
		{"warning alone", map[string]string{"a": w}, bridgegithub.OutcomeWarning},
		{"failure beats warning", map[string]string{"a": w, "b": f}, bridgegithub.OutcomeFailure},
		// Empty/nil → OutcomeNone (clear all labels), NOT a warning: a PR with no
		// attributed scopes must shed its labels, not acquire flux-warning.
		{"empty", map[string]string{}, bridgegithub.OutcomeNone},
		{"nil", nil, bridgegithub.OutcomeNone},
	}
	for _, tc := range cases {
		if got := aggregatePRLabelOutcome(tc.in); got != tc.want {
			t.Errorf("%s: aggregatePRLabelOutcome(%v) = %s, want %s", tc.name, tc.in, got, tc.want)
		}
	}
}

// readinessRefsFor drops child Kustomizations from the poll, keeps the parent's
// directly-managed (non-Kustomization) resources, and adds the parent's own
// object when it had children — while leaving a leaf Kustomization untouched.
func TestReadinessRefsFor(t *testing.T) {
	childA := "flux-system_child-a_kustomize.toolkit.fluxcd.io_Kustomization"
	childB := "flux-system_child-b_kustomize.toolkit.fluxcd.io_Kustomization"
	gitRepo := "flux-system_image-automation_source.toolkit.fluxcd.io_GitRepository"
	ns := "_app__Namespace"
	deploy := "app_api_apps_Deployment"
	self := "flux-system_parent_kustomize.toolkit.fluxcd.io_Kustomization"

	counts := func(xs []string) map[string]int {
		m := map[string]int{}
		for _, x := range xs {
			m[x]++
		}
		return m
	}
	eq := func(a, b []string) bool {
		ca, cb := counts(a), counts(b)
		if len(ca) != len(cb) {
			return false
		}
		for k, va := range ca {
			if vb, ok := cb[k]; !ok || vb != va {
				return false
			}
		}
		return true
	}

	// Leaf (no child Kustomizations) → unchanged, no self row added.
	leaf := []string{ns, deploy}
	if got := readinessRefsFor(leaf, "default", "app"); !eq(got, leaf) {
		t.Errorf("leaf: got %v, want unchanged %v", got, leaf)
	}

	// Aggregate → children dropped, GitRepository + Namespace kept, parent's own
	// object added.
	agg := []string{childA, childB, gitRepo, ns}
	got := readinessRefsFor(agg, "flux-system", "parent")
	want := []string{gitRepo, ns, self}
	if !eq(got, want) {
		t.Errorf("aggregate: got %v, want %v", got, want)
	}
	for _, g := range got {
		if g == childA || g == childB {
			t.Errorf("aggregate poll still contains a child Kustomization: %s", g)
		}
	}
}

// The label must reflect ALL Kustomizations on a PR: a healthy event for one
// Kustomization must NOT flip the PR green while another is still failing, and
// the PR only goes green once every Kustomization is green.
func TestReconcileLabels_AggregatesAcrossKustomizations(t *testing.T) {
	commenterMock := &fakeCommenter{}
	cfg := &config.Config{PRLabelEnabled: true}
	proc := New(cfg, nil, nil, nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	ctx := context.Background()
	lastLabel := func() bridgegithub.Outcome {
		return commenterMock.labelCalls[len(commenterMock.labelCalls)-1].Outcome
	}

	// k1 fails on PR 42 (head h1) → flux-failed.
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/k1", "h1", 42, bridgegithub.OutcomeFailure)
	if lastLabel() != bridgegithub.OutcomeFailure {
		t.Fatalf("after k1 fails, label = %s, want failure", lastLabel())
	}
	// k2 succeeds on the same PR → must STAY flux-failed (k1 still red).
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/k2", "h1", 42, bridgegithub.OutcomeSuccess)
	if lastLabel() != bridgegithub.OutcomeFailure {
		t.Fatalf("k2 success must not override k1 failure; label = %s, want failure", lastLabel())
	}
	// k1 recovers → every Kustomization green → flux-success.
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/k1", "h1", 42, bridgegithub.OutcomeSuccess)
	if lastLabel() != bridgegithub.OutcomeSuccess {
		t.Fatalf("after k1 recovers, all-green label = %s, want success", lastLabel())
	}
}

// Anti-flap: on a NEW head, a partial all-green view (only the fast scope has
// reported) must NOT write flux-success — that's the success→failed flap a
// multi-Kustomization PR shows after every push. The PR's roster (learned on the
// prior head) holds success until every known scope reports for the new head; a
// failing scope is still labelled immediately.
func TestReconcileLabels_HoldsSuccessUntilRosterComplete(t *testing.T) {
	commenterMock := &fakeCommenter{}
	cfg := &config.Config{PRLabelEnabled: true}
	proc := New(cfg, nil, nil, nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	ctx := context.Background()

	// Head h1: both Kustomizations report → roster learns {fast, slow}.
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/fast", "h1", 42, bridgegithub.OutcomeSuccess)
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/slow", "h1", 42, bridgegithub.OutcomeSuccess)
	base := len(commenterMock.labelCalls)

	// Head h2 (a new push): the fast scope reports success FIRST. The roster still
	// expects `slow`, so success must be HELD — and h1's flux-success must not
	// stay on h2, which nothing verified yet: the head reads flux-warning.
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/fast", "h2", 42, bridgegithub.OutcomeSuccess)
	if extra := commenterMock.labelCalls[base:]; len(extra) != 1 || extra[0].Outcome != bridgegithub.OutcomeWarning {
		t.Fatalf("partial roster on the new head must write flux-warning, not keep or write flux-success; got %+v", extra)
	}

	// The slow scope then reports a failure → flux-failed written immediately.
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/slow", "h2", 42, bridgegithub.OutcomeFailure)
	if got := commenterMock.labelCalls[len(commenterMock.labelCalls)-1].Outcome; got != bridgegithub.OutcomeFailure {
		t.Fatalf("expected flux-failed written immediately on the new head; got %s", got)
	}
}

// blockingLabelCommenter parks the first SetPRLabels call until released, so a
// test can interleave two label writers. second is closed when a second call
// starts.
type blockingLabelCommenter struct {
	*fakeCommenter
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	release chan struct{}
	second  chan struct{}
}

func (b *blockingLabelCommenter) SetPRLabels(ctx context.Context, owner, repo string, prNumber int, headSHA string, outcome bridgegithub.Outcome) error {
	b.mu.Lock()
	b.calls++
	call := b.calls
	b.mu.Unlock()
	switch call {
	case 1:
		close(b.entered)
		<-b.release
	case 2:
		if b.second != nil {
			close(b.second)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fakeCommenter.SetPRLabels(ctx, owner, repo, prNumber, headSHA, outcome)
}

// Two scopes of one PR head report at once. A worker that read an all-green
// aggregate must not write flux-success after another worker recorded and
// wrote flux-failed: the aggregate is read and written under one per-PR lock.
func TestReconcileLabels_StaleSuccessCannotOverwriteNewerFailure(t *testing.T) {
	commenterMock := &blockingLabelCommenter{fakeCommenter: &fakeCommenter{},
		entered: make(chan struct{}), release: make(chan struct{}), second: make(chan struct{})}
	proc := New(&config.Config{PRLabelEnabled: true}, nil, nil, nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // scope a: success, computed and parked inside the label write
		defer wg.Done()
		_ = proc.reconcileLabels(ctx, "o", "r", "ns/a", "h1", 42, bridgegithub.OutcomeSuccess)
	}()
	<-commenterMock.entered
	go func() { // scope b: failure, recorded while a's write is in flight
		defer wg.Done()
		_ = proc.reconcileLabels(ctx, "o", "r", "ns/b", "h1", 42, bridgegithub.OutcomeFailure)
	}()
	// No sleep: wait until b either writes labels (no lock: fail) or is
	// parked on the label lock inside applyPRLabel.
	for deadline := time.Now().Add(10 * time.Second); !goroutineWaitsOnLock("(*Processor).applyPRLabel"); {
		select {
		case <-commenterMock.second:
			close(commenterMock.release)
			wg.Wait()
			t.Fatal("scope b wrote labels while scope a's write was in flight: the aggregate isn't read and written under one lock")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("scope b neither waited on the label lock nor wrote labels")
		}
		runtime.Gosched()
	}
	close(commenterMock.release)
	wg.Wait()

	calls := commenterMock.labelCalls
	if len(calls) == 0 || calls[len(calls)-1].Outcome != bridgegithub.OutcomeFailure {
		t.Fatalf("a stale flux-success overwrote the newer flux-failed; label calls %+v", calls)
	}
}

// Two replicas (separate processors, one shared outcome store) report scopes of
// one PR head. Replica A reads an all-green aggregate and its write stalls;
// replica B records a failure and writes flux-failed meanwhile. A's stale
// flux-success then lands last, so A must re-read the aggregate after its
// write and correct the labels.
func TestReconcileLabels_StaleSuccessAcrossReplicasIsCorrected(t *testing.T) {
	commenterMock := &blockingLabelCommenter{fakeCommenter: &fakeCommenter{}, entered: make(chan struct{}), release: make(chan struct{})}
	shared := dedupe.NewNoExpiry(16)
	replicaA := New(&config.Config{PRLabelEnabled: true}, nil, nil, nil, commenterMock, nil, shared, slog.Default())
	replicaB := New(&config.Config{PRLabelEnabled: true}, nil, nil, nil, commenterMock, nil, shared, slog.Default())
	ctx := context.Background()

	done := make(chan error, 1)
	go func() { done <- replicaA.reconcileLabels(ctx, "o", "r", "ns/a", "h1", 42, bridgegithub.OutcomeSuccess) }()
	<-commenterMock.entered
	if err := replicaB.reconcileLabels(ctx, "o", "r", "ns/b", "h1", 42, bridgegithub.OutcomeFailure); err != nil {
		t.Fatalf("replica B: %v", err)
	}
	close(commenterMock.release)
	if err := <-done; err != nil {
		t.Fatalf("replica A: %v", err)
	}
	calls := commenterMock.labelCalls
	if len(calls) == 0 || calls[len(calls)-1].Outcome != bridgegithub.OutcomeFailure {
		t.Fatalf("a replica's stale flux-success stayed on the PR after another replica wrote flux-failed; label calls %+v", calls)
	}
}

// goroutineWaitsOnLock reports whether some goroutine is blocked acquiring a
// sync.Mutex directly inside the function named by frame.
func goroutineWaitsOnLock(frame string) bool {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		lines := strings.Split(g, "\n")
		for i, l := range lines {
			if !strings.Contains(l, "Mutex).Lock") {
				continue
			}
			// The frame after Lock (and its file line) is the caller.
			for _, next := range lines[i+1:] {
				if strings.HasPrefix(next, "\t") || strings.Contains(next, "Mutex).") {
					continue
				}
				if strings.Contains(next, frame) {
					return true
				}
				break
			}
		}
	}
	return false
}

// The hold releases on COMPLETENESS, not a timer: once every rostered scope has
// reported success for the head, flux-success is written.
func TestReconcileLabels_WritesSuccessWhenRosterComplete(t *testing.T) {
	commenterMock := &fakeCommenter{}
	cfg := &config.Config{PRLabelEnabled: true}
	proc := New(cfg, nil, nil, nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	ctx := context.Background()

	_ = proc.reconcileLabels(ctx, "o", "r", "ns/fast", "h1", 42, bridgegithub.OutcomeSuccess)
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/slow", "h1", 42, bridgegithub.OutcomeSuccess)
	// New head h2: fast first (held), then slow → now complete → success written.
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/fast", "h2", 42, bridgegithub.OutcomeSuccess)
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/slow", "h2", 42, bridgegithub.OutcomeSuccess)
	if got := commenterMock.labelCalls[len(commenterMock.labelCalls)-1].Outcome; got != bridgegithub.OutcomeSuccess {
		t.Fatalf("expected flux-success once all rostered scopes reported for h2; got %s", got)
	}
}

// De-scoping a Kustomization must shrink the roster so a held success can release:
// a scope the PR no longer touches would otherwise never report for the new head
// and pin the label forever.
func TestReconcileLabels_DeScopeReleasesHeldSuccess(t *testing.T) {
	commenterMock := &fakeCommenter{}
	cfg := &config.Config{PRLabelEnabled: true}
	dd := dedupe.NewNoExpiry(16)
	proc := New(cfg, nil, nil, nil, commenterMock, nil, dd, slog.Default())
	ctx := context.Background()

	// Roster learns {fast, slow}; on head h2 only `fast` reports success → held.
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/fast", "h1", 42, bridgegithub.OutcomeSuccess)
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/slow", "h1", 42, bridgegithub.OutcomeSuccess)
	_ = proc.reconcileLabels(ctx, "o", "r", "ns/fast", "h2", 42, bridgegithub.OutcomeSuccess)
	base := len(commenterMock.labelCalls)

	// `slow` is de-scoped (PR no longer touches it): roster shrinks to {fast},
	// which is now covered → the held success releases.
	dd.RemoveScopeOutcome("o", "r", 42, "h2", "ns/slow")
	dd.RemovePRScope("o", "r", 42, "ns/slow")
	_ = proc.applyPRLabel(ctx, "o", "r", 42, "h2", bridgegithub.OutcomeNone)
	if got := commenterMock.labelCalls[len(commenterMock.labelCalls)-1].Outcome; got != bridgegithub.OutcomeSuccess || len(commenterMock.labelCalls) == base {
		t.Fatalf("expected held flux-success to release after de-scope; calls=%+v", commenterMock.labelCalls)
	}
}

// When a PR stops declaring any resource in this Kustomization's inventory
// (e.g. the workload change was reverted out of the PR), the now-stale comment
// must be removed — a DEFINITIVE no-match triggers the cache-guarded cleanup.
func TestProcess_DeScopedPR_RemovesStaleComment(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)} // inventory: default_any__ConfigMap
	commenterMock := &fakeCommenter{
		prNumber: 42, prState: "open", prHeadSHA: "h1",
		// PR now changes a DIFFERENT resource (default_other__ConfigMap), not in
		// this Kustomization's inventory → definitive no-match.
		prFiles: []bridgegithub.PRFile{{Path: "other.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 0 {
		t.Fatalf("a de-scoped PR must not be posted to; got %d posts", len(commenterMock.posts))
	}
	if len(commenterMock.removeCalls) != 1 {
		t.Fatalf("expected one stale-comment cleanup for the de-scoped PR; got %d", len(commenterMock.removeCalls))
	}
	if got := commenterMock.removeCalls[0]; got.Kustomization != "default/apps" || got.Number != 42 {
		t.Errorf("cleanup targeted wrong scope/PR: %+v", got)
	}
}

// Regression: de-scoping must prune the PR roster even when the de-scoped scope
// has NO outcome recorded on the current head. Otherwise a roster entry learned
// from a PRIOR head (the held-success case) is never covered for the new head
// and holds flux-success indefinitely. The roster removal must not be gated on
// the per-head RemoveScopeOutcome.
func TestProcess_DeScope_ReleasesHeldSuccessWithoutHeadOutcome(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h-new"
	// Roster (learned on a prior head) knows two scopes; on head h-new only
	// default/apps has reported success → flux-success is currently HELD pending
	// default/other, which has NO outcome on this head.
	dd.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	dd.AddPRScope(owner, repo, pr, "default/other", time.Hour)
	dd.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeSuccess), time.Hour)

	// The bridge processes the `other` Kustomization's event; the PR no longer
	// declares anything in `other`'s inventory → a definitive no-match → de-scope.
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)} // inventory: default_any__ConfigMap
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles: []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "other", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	// default/other pruned from the roster → remaining default/apps=success now
	// covers the roster → the held flux-success is released.
	if len(commenterMock.labelCalls) == 0 || commenterMock.labelCalls[len(commenterMock.labelCalls)-1].Outcome != bridgegithub.OutcomeSuccess {
		t.Fatalf("expected held flux-success to release after de-scope pruned the roster; got %+v", commenterMock.labelCalls)
	}
	for _, s := range dd.PRScopes(owner, repo, pr) {
		if s == "default/other" {
			t.Fatalf("default/other still in roster after de-scope")
		}
	}
}

// A de-scope removes the scope's outcome and roster entry, then rewrites the
// labels. When that write fails, the next event must retry it: with both
// entries already gone it would find nothing to remove and never write again,
// leaving flux-success held (or a de-scoped failure) on the PR.
func TestProcess_DeScope_FailedLabelWriteIsRetried(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h-new"
	dd.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	dd.AddPRScope(owner, repo, pr, "default/other", time.Hour)
	dd.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeSuccess), time.Hour)

	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)} // inventory: default_any__ConfigMap
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles:  []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:    map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
		labelErr: errors.New("server error"),
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "other", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	commenterMock.labelErr = nil
	before := len(commenterMock.labelCalls)
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process (retry): %v", err)
	}
	if calls := commenterMock.labelCalls[before:]; len(calls) == 0 || calls[len(calls)-1].Outcome != bridgegithub.OutcomeSuccess {
		t.Fatalf("the failed de-scope label write was not retried; label calls on the next event %+v", calls)
	}
	if slices.Contains(dd.PRScopes(owner, repo, pr), "default/other") {
		t.Fatalf("default/other still in the roster after the retried de-scope")
	}
}

// Regression for visible label flapping on de-scope (observed in practice:
// "added flux-success and removed flux-success"). Scenario: a PR touches
// flux-system and test; a new push REMOVES the test changes. If test's
// de-scope event processes BEFORE flux-system has reported for the new head,
// the per-head scope map is empty and the OutcomeNone fallback would strip
// flux-success — only for flux-system's event to re-add it seconds later.
// The clear must be skipped while the roster still lists pending scopes;
// labels from the prior head stay put until the surviving scope reports.
func TestProcess_DeScope_DoesNotClearLabelsWhileRosterPending(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h-new"
	// Roster (learned on the prior head) knows both scopes. NO outcome is
	// recorded yet for the new head — flux-system hasn't reconciled it.
	dd.AddPRScope(owner, repo, pr, "flux-system/flux-system", time.Hour)
	dd.AddPRScope(owner, repo, pr, "flux-system/test", time.Hour)

	// The bridge processes test's event on the new head; the PR no longer
	// declares anything in test's inventory → definitive no-match → de-scope.
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)} // inventory: default_any__ConfigMap
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles: []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "test", Namespace: "flux-system"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	// The roster must be pruned (test gone)…
	for _, s := range dd.PRScopes(owner, repo, pr) {
		if s == "flux-system/test" {
			t.Fatalf("flux-system/test still in roster after de-scope")
		}
	}
	// …but NO label write may happen: flux-system is still in the roster and
	// hasn't reported for this head, so clearing now would flap the label.
	if len(commenterMock.labelCalls) != 0 {
		t.Fatalf("expected NO label writes while the roster has pending scopes; got %+v", commenterMock.labelCalls)
	}

	// When the LAST scope de-scopes too, the clear must go through — the PR
	// genuinely has nothing left to say.
	dd.RemovePRScope(owner, repo, pr, "flux-system/flux-system")
	_ = proc.applyPRLabel(context.Background(), owner, repo, pr, head, bridgegithub.OutcomeNone)
	if n := len(commenterMock.labelCalls); n != 1 || commenterMock.labelCalls[0].Outcome != bridgegithub.OutcomeNone {
		t.Fatalf("expected exactly one OutcomeNone write once the roster is empty; got %+v", commenterMock.labelCalls)
	}
}

// A blob-fetch error makes the no-match verdict non-definitive — the cleanup
// must NOT fire, so a transient GitHub blip can't delete a still-valid comment.
func TestProcess_DeScopedPR_BlobErrorSuppressesCleanup(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{
		prNumber: 42, prState: "open", prHeadSHA: "h1",
		prFiles:  []bridgegithub.PRFile{{Path: "other.yaml", BlobSHA: "b1", Status: "modified"}},
		blobsErr: errors.New("github blip"), // not a rate limit → treated as no-overlap, non-definitive
	}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.removeCalls) != 0 {
		t.Fatalf("a non-definitive no-match (blob error) must NOT trigger cleanup; got %d removes", len(commenterMock.removeCalls))
	}
}

// A failed post must roll back the per-PR mark so the next event re-admits the
// PR instead of silently skipping it.
func TestProcess_PerPRDedup_RollsBackMarkOnPostFailure(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111", postErr: errors.New("boom")}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "srcrev-aaa"); err == nil {
		t.Fatal("expected first Process to fail (post error)")
	}
	// Recover: post now succeeds. Same PR head — must be RE-ADMITTED because
	// the failed attempt rolled back its mark. The fake records every post
	// ATTEMPT (including the failed first one), so a successful re-admission
	// yields 2 calls; had the mark NOT rolled back, the retry would have been
	// deduped away and the count would stay at 1.
	commenterMock.postErr = nil
	if err := proc.Process(context.Background(), event, "srcrev-bbb"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 2 {
		t.Fatalf("expected the PR to be re-admitted after rollback (2 post attempts); got %d", len(commenterMock.posts))
	}
}

// Per Q4 the commit-comment fallback was dropped. When FindPRsForCommit
// returns no PRs, the bridge logs a warning and ends the pipeline cleanly
// without posting anywhere.
func TestProcess_NoPRFound_SkipsCleanly(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationEmptyInventory)}
	analyzerMock := &fakeAnalyzer{result: "analysis"}
	commenterMock := &fakeCommenter{prNumber: 0}

	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, analyzerMock, commenterMock, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "deadbeef"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if commenterMock.prCommentBody != "" {
		t.Errorf("PostOrUpdateComment was called even though no PR was found")
	}
}

func TestProcess_MCPError(t *testing.T) {
	mcpMock := &fakeMCPClient{err: fmt.Errorf("mcp unavailable")}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	proc := New(cfg, mcpMock, checker, &fakeAnalyzer{}, &fakeCommenter{}, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "abc"); err == nil {
		t.Fatal("expected error from MCP failure, got nil")
	}
}

// TestProcess_AnalyzerErrorFallsBackToKstatus pins the load-bearing
// "post the kstatus body anyway" contract: when the LLM call fails the
// processor MUST NOT return an error. Instead it posts the deterministic
// kstatus body alone (with a visible "LLM analysis unavailable" note)
// and bumps LLMFallbacks so operators can alert on a sustained outage.
//
// The old behavior — return `llm: <wrapped>` — caused the dedupe Forget
// path to fire and Flux's 5-minute re-fire to retry the whole pipeline
// (including the LLM). In the meantime the PR had no comment at all,
// even though the readiness signal the reviewer most needs (the kstatus
// tables) was already computed. The fallback path keeps the PR up to
// date during an LLM outage and the operator sees the failure in both
// logs (ERROR) and metrics (LLMFallbacks).
func TestProcess_AnalyzerErrorFallsBackToKstatus(t *testing.T) {
	metrics.Init()
	before := testutil.ToFloat64(metrics.LLMFallbacks.WithLabelValues("claude"))

	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	analyzerMock := &fakeAnalyzer{err: fmt.Errorf("llm timeout")}
	cfg := &config.Config{GitHubRepo: "owner/repo", LLMProvider: "claude", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	checker := readiness.NewChecker(mcpMock)
	commenter := &fakeCommenter{prNumber: 1}
	proc := New(cfg, mcpMock, checker, analyzerMock, commenter, nil, nil, slog.Default())

	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
	}
	if err := proc.Process(context.Background(), event, "abc"); err != nil {
		t.Fatalf("LLM failure must NOT propagate as a pipeline error; got %v", err)
	}

	// A comment must have been posted (the kstatus body, not the LLM
	// narrative). Verify both the post happened and the body carries the
	// visible "unavailable" note so reviewers know the narrative is
	// intentionally missing.
	if commenter.prCommentBody == "" {
		t.Fatal("expected a kstatus-only comment to be posted; commenter recorded no post")
	}
	if !strings.Contains(commenter.prCommentBody, "LLM analysis unavailable") {
		t.Errorf("posted comment must contain the 'LLM analysis unavailable' marker; got:\n%s", commenter.prCommentBody)
	}

	if got := testutil.ToFloat64(metrics.LLMFallbacks.WithLabelValues("claude")) - before; got != 1 {
		t.Errorf("LLMFallbacks{provider=claude} delta = %v, want 1", got)
	}
}

// fakeMCPClient implements both processor.MCPClient and
// readiness.ResourceFetcher. The processor's GetKustomizationStatus returns
// `data`; the readiness path's GetKubernetesResource consults `resources`
// keyed by "<apiVersion>|<Kind>|<name>|<namespace>" and defaults to NotFound.
type fakeMCPClient struct {
	data []byte
	// dataQueue, when non-empty, takes precedence over data: consumed one
	// entry per GetKustomizationStatus call (the last entry repeats once
	// exhausted) — lets tests return a different inventory on the pruned-row
	// re-read than on the initial fetch.
	dataQueue          [][]byte
	kustomizationCalls int
	err                error
	resources          map[string][]byte // GetKubernetesResource: "<apiVersion>|<Kind>|<name>|<namespace>" → YAML
	// resourceCalls counts GetKubernetesResource calls; resourceErrs, when it
	// has the key, fails that many calls for it before serving resources.
	resourceCalls int
	resourceErrs  map[string]int
	// kustomizationsByName: GetKustomizationStatus "<namespace>/<name>" → YAML,
	// checked before data/dataQueue (e.g. a parent Kustomization's spec).
	kustomizationsByName map[string][]byte
	// kustomizationsByNameQueue: like kustomizationsByName, but consumed one
	// per call (the last entry repeats) and checked first.
	kustomizationsByNameQueue map[string][][]byte
	// kustomizationErr fails GetKustomizationStatus only (e.g. a deleted
	// Kustomization), leaving the other reads working.
	kustomizationErr error
	lists            map[string][]byte // ListKubernetesResources: "<apiVersion>|<Kind>|<namespace>" → YAML
	// listQueue, when it has the key, takes precedence over lists: consumed
	// one entry per call (the last entry repeats) — lets a test change a list
	// between two reads, e.g. a pod that starts failing mid-run.
	listQueue               map[string][][]byte
	listCalls               map[string]int
	events                  string
	eventsErr               error
	eventsNS                []string
	fluxInstance            string
	fluxInstanceErr         error
	fluxInstanceN           int
	logs                    string
	logsErr                 error
	logsCalled              []string
	metrics                 string
	metricsErr              error
	metricsNS               []string
	workloadEvents          string
	workloadEventsTruncated bool
	workloadEventsErr       error
	workloadEventsCalls     []string        // "<kind>/<namespace>/<name>" per GetKubernetesEvents call
	workloadEventsSince     []time.Duration // `since` passed, parallel to workloadEventsCalls
}

func (f *fakeMCPClient) GetKubernetesEvents(_ context.Context, kind, name, namespace string, since time.Duration, _ int) (string, bool, error) {
	f.workloadEventsCalls = append(f.workloadEventsCalls, kind+"/"+namespace+"/"+name)
	f.workloadEventsSince = append(f.workloadEventsSince, since)
	return f.workloadEvents, f.workloadEventsTruncated, f.workloadEventsErr
}

func (f *fakeMCPClient) GetFluxInstance(_ context.Context) (string, error) {
	f.fluxInstanceN++
	return f.fluxInstance, f.fluxInstanceErr
}

func (f *fakeMCPClient) GetKubernetesLogs(_ context.Context, podName, namespace, container string, previous bool) (string, error) {
	suffix := "/" + container
	if previous {
		suffix += "/previous"
	}
	f.logsCalled = append(f.logsCalled, namespace+"/"+podName+suffix)
	return f.logs, f.logsErr
}

func (f *fakeMCPClient) GetKubernetesMetrics(_ context.Context, namespace string) (string, error) {
	f.metricsNS = append(f.metricsNS, namespace)
	return f.metrics, f.metricsErr
}

func (f *fakeMCPClient) GetKubernetesResource(_ context.Context, apiVersion, kind, name, namespace string) ([]byte, error) {
	f.resourceCalls++
	if f.err != nil {
		return nil, f.err
	}
	key := apiVersion + "|" + kind + "|" + name + "|" + namespace
	if f.resourceErrs[key] > 0 {
		f.resourceErrs[key]--
		return nil, fmt.Errorf("transient failure: %s", key)
	}
	if body, ok := f.resources[key]; ok {
		return body, nil
	}
	return nil, fmt.Errorf("not found: %s", key)
}

func (f *fakeMCPClient) GetKustomizationStatus(_ context.Context, name, namespace string) ([]byte, error) {
	if f.kustomizationErr != nil {
		return nil, f.kustomizationErr
	}
	if queue := f.kustomizationsByNameQueue[namespace+"/"+name]; len(queue) > 0 {
		if len(queue) > 1 {
			f.kustomizationsByNameQueue[namespace+"/"+name] = queue[1:]
		}
		return queue[0], nil
	}
	if data, ok := f.kustomizationsByName[namespace+"/"+name]; ok {
		return data, nil
	}
	if len(f.dataQueue) > 0 {
		f.kustomizationCalls++
		return f.dataQueue[min(f.kustomizationCalls-1, len(f.dataQueue)-1)], f.err
	}
	return f.data, f.err
}

func (f *fakeMCPClient) ListKubernetesResources(_ context.Context, apiVersion, kind, namespace string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	// Track Event list calls so existing tests can assert "events fetched
	// for namespace X" semantics now that the failure path uses
	// ListKubernetesResources(kind=Event) instead of GetKubernetesEvents.
	if apiVersion == "v1" && kind == "Event" {
		f.eventsNS = append(f.eventsNS, namespace)
		if f.eventsErr != nil {
			return nil, f.eventsErr
		}
		// Fall through to lists map so tests can supply event YAML there.
	}
	key := apiVersion + "|" + kind + "|" + namespace
	if queue := f.listQueue[key]; len(queue) > 0 {
		if f.listCalls == nil {
			f.listCalls = map[string]int{}
		}
		f.listCalls[key]++
		return queue[min(f.listCalls[key]-1, len(queue)-1)], nil
	}
	if body, ok := f.lists[key]; ok {
		return body, nil
	}
	// Back-compat for old tests that set fake.events (a string) instead of
	// pre-loading lists["v1|Event|<ns>"]: wrap it in a minimal Event YAML
	// so the filter/render pipeline produces some output.
	if apiVersion == "v1" && kind == "Event" && f.events != "" {
		return []byte("apiVersion: v1\nkind: Event\nmetadata:\n  name: legacy-fixture\n  namespace: " + namespace +
			"\ntype: Warning\nreason: LegacyFixture\nmessage: " + f.events + "\n"), nil
	}
	return nil, nil
}

type fakeAnalyzer struct {
	result string
	// results, when non-empty, is consumed one entry per Analyze call (the
	// last entry repeats once exhausted) — lets tests script a non-compliant
	// first reply followed by a corrected retry. Takes precedence over `result`.
	results         []string
	err             error
	calls           int // bumped on every Analyze call; assertions use it to prove pre-LLM filter actually short-circuits
	lastUserPayload string
}

func (f *fakeAnalyzer) Analyze(_ context.Context, _, userPayload string) (string, error) {
	f.calls++
	f.lastUserPayload = userPayload
	if len(f.results) > 0 {
		i := min(f.calls-1, len(f.results)-1)
		return f.results[i], f.err
	}
	return f.result, f.err
}

type fakeCommenter struct {
	merged                 bool
	owner                  string
	postErr                error
	prCommentBody          string
	prCommentKustomization string // "<ns>/<name>" of the most recent post
	prErr                  error
	prFiles                []bridgegithub.PRFile // files returned by ListPRFiles; default = one matching-any blob
	prFilesErr             error
	prLabels               []string
	prNumber               int
	prHeadSHA              string
	prState                string
	repo                   string

	// blobs maps blob-SHA → YAML content for GetBlobContent. Tests set
	// this together with the corresponding prFiles BlobSHA values.
	// Missing entries return ("", nil) which the processor treats as
	// "file declares no resources" → no inventory match.
	blobs    map[string][]byte
	blobsErr error

	// baseFiles maps a changed file's PATH → its YAML content at the PR base
	// (before the PR), for BaseFileResourceIDs. Set together with prBaseSHA so
	// the in-place-removal diff has a base ref to fetch against. Missing entries
	// return no resources (file absent at base / nothing to diff).
	baseFiles    map[string][]byte
	baseFilesErr error
	// prBaseSHA is the single-PR-mode PR base commit (PRInfo.BaseSHA). Must be
	// non-empty for the in-place-removal fallback to run.
	prBaseSHA string

	// stabilityKeys opts a test into stateful comment-currency tracking:
	// when non-nil, PostOrUpdateCommentKeyed records each PR's last
	// "<scope>|<stabilityKey>" here and CommentCurrent matches against it
	// (mirroring the real cache). The default nil means CommentCurrent is
	// always false, preserving legacy tests' every-analysis-posts counts.
	stabilityKeys map[int]string

	// Multi-PR mode (autodev-shaped tests). When `prs` is non-nil it
	// overrides the single-PR fields above. FindPRsForCommit returns the
	// entire slice; ListPRFiles consults `filesByPR` keyed by PR number;
	// PostOrUpdateComment records every (number, scope) pair into `posts`.
	prs          []bridgegithub.PRInfo
	filesByPR    map[int][]bridgegithub.PRFile
	posts        []postRecord
	removeCalls  []postRecord // (scope, PR) pairs RemoveComment was called for
	removeErr    error
	listFileHits []int // PR numbers ListPRFiles was actually called for

	// Label calls captured by SetPRLabels. Tests that care about label
	// reconciliation read this directly. labelErr lets a test simulate a
	// GitHub failure on the label call without affecting the comment path.
	labelCalls []labelCall
	labelErr   error

	// findCalls counts FindPRsForCommit calls (the first GitHub lookup).
	findCalls int
}

type labelCall struct {
	Number  int
	Outcome bridgegithub.Outcome
}

type postRecord struct {
	Kustomization string // "<ns>/<name>"
	Number        int
	Body          string
}

func (f *fakeCommenter) FindPRsForCommit(_ context.Context, owner, repo, _ string) ([]bridgegithub.PRInfo, error) {
	f.findCalls++
	f.owner = owner
	f.repo = repo
	if f.prErr != nil {
		return nil, f.prErr
	}
	if f.prs != nil {
		return append([]bridgegithub.PRInfo(nil), f.prs...), nil
	}
	if f.prNumber == 0 {
		return nil, nil
	}
	state := f.prState
	if state == "" {
		state = "open"
	}
	return []bridgegithub.PRInfo{{Number: f.prNumber, State: state, Merged: f.merged, Labels: f.prLabels, HeadSHA: f.prHeadSHA, BaseSHA: f.prBaseSHA}}, nil
}

func (f *fakeCommenter) ListPRFiles(_ context.Context, _, _ string, prNumber int, _ string) ([]bridgegithub.PRFile, error) {
	f.listFileHits = append(f.listFileHits, prNumber)
	if f.prFilesErr != nil {
		return nil, f.prFilesErr
	}
	if f.filesByPR != nil {
		if files, ok := f.filesByPR[prNumber]; ok {
			return files, nil
		}
		return nil, nil // a PR with zero changed files — treated as no-overlap
	}
	if f.prFiles == nil {
		// Default: single file with a blob SHA that maps to a "matches
		// anything in inventory" payload. Tests that care about real
		// inventory matching pass explicit prFiles + blobs.
		return []bridgegithub.PRFile{{Path: "any.yaml", BlobSHA: "default-blob", Status: "modified"}}, nil
	}
	return f.prFiles, nil
}

// BlobResourceIDs mirrors the real commenter: it derives the declared
// resource IDs from the blob bytes (same fixtures the tests already set up via
// `blobs`), parsing with the production ResourceIDsFromYAML so test inputs and
// real behaviour stay in lockstep.
func (f *fakeCommenter) BlobResourceIDs(_ context.Context, _, _, sha string) ([]string, error) {
	if f.blobsErr != nil {
		return nil, f.blobsErr
	}
	var content []byte
	switch {
	case f.blobs != nil:
		content = f.blobs[sha]
	case sha == "default-blob":
		// Default blob declares a resource named "any" / Kind ConfigMap.
		// Tests that don't care about inventory filtering can put this ID in
		// the inventory to admit the PR by default; tests that DO care
		// override prFiles + blobs to set up specific scenarios.
		content = []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: any\n  namespace: default\n")
	}
	return ResourceIDsFromYAML(content), nil
}

func (f *fakeCommenter) BaseFileResourceIDs(_ context.Context, _, _, path, ref string) ([]string, error) {
	if f.baseFilesErr != nil {
		return nil, f.baseFilesErr
	}
	if ref == "" || f.baseFiles == nil {
		return nil, nil
	}
	return ResourceIDsFromYAML(f.baseFiles[path]), nil
}

func (f *fakeCommenter) PostOrUpdateComment(_ context.Context, _, _, ns, name string, prNumber int, body string) error {
	f.prCommentBody = body
	f.prCommentKustomization = ns + "/" + name
	f.posts = append(f.posts, postRecord{Kustomization: ns + "/" + name, Number: prNumber, Body: body})
	return f.postErr
}

// PostOrUpdateCommentKeyed records the stability key (when the test opted
// into stateful tracking by initialising stabilityKeys) and delegates to the
// plain recorder.
func (f *fakeCommenter) PostOrUpdateCommentKeyed(ctx context.Context, owner, repo, ns, name string, prNumber int, body, stabilityKey string) error {
	if f.stabilityKeys != nil && f.postErr == nil {
		f.stabilityKeys[prNumber] = ns + "/" + name + "|" + stabilityKey
	}
	return f.PostOrUpdateComment(ctx, owner, repo, ns, name, prNumber, body)
}

// CommentCurrent reports true only for tests that initialised stabilityKeys
// (opt-in): the default nil map means "never current", keeping legacy tests'
// post-count semantics where every analysis posts.
func (f *fakeCommenter) CommentCurrent(_ context.Context, _, _, ns, name string, prNumber int, stabilityKey string) bool {
	if f.stabilityKeys == nil {
		return false
	}
	return f.stabilityKeys[prNumber] == ns+"/"+name+"|"+stabilityKey
}

func (f *fakeCommenter) RemoveComment(_ context.Context, _, _, ns, name string, prNumber int) error {
	f.removeCalls = append(f.removeCalls, postRecord{Kustomization: ns + "/" + name, Number: prNumber})
	return f.removeErr
}

func (f *fakeCommenter) SetPRLabels(_ context.Context, _, _ string, prNumber int, _ string, outcome bridgegithub.Outcome) error {
	f.labelCalls = append(f.labelCalls, labelCall{Number: prNumber, Outcome: outcome})
	return f.labelErr
}

// With a re-eval budget set, a FAILING PR-head is analysed once plus at most
// DedupePRReEvalMax failing re-evaluations; further windows are skipped so a
// deploy that never recovers stops burning readiness polls + LLM calls +
// comment churn. The events carry error severity so every analysis concludes
// failure — only failed/warning analyses are metered.
func TestProcess_ReEvalMax_StopsAfterBudgetSpent(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: false,
		DedupePRReEvalMax:      2,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, Message: "still broken",
	}

	for i := range 5 {
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process #%d: %v", i+1, err)
		}
		time.Sleep(3 * time.Millisecond) // let the per-PR re-eval floor elapse
	}
	// 1 initial analysis + 2 budgeted failing re-evals; runs 4 and 5 are skipped.
	if len(commenterMock.posts) != 3 {
		t.Fatalf("want 3 posts (initial + max 2 failing re-evals), got %d", len(commenterMock.posts))
	}

	// A healthy re-eval must NOT burn budget: the budget meters failing
	// analyses only. (Covered structurally: healthy analyses skip the
	// post-analysis increment — see TestProcess_ReEvalMax_HealthyAnalysisIsFree.)
}

// A re-eval whose analysis concludes HEALTHY never burns budget — the budget
// meters failing analyses only. With max=2: failing initial (free), failing
// re-eval (1/2), HEALTHY re-eval (free), failing re-eval (2/2), then
// exhausted. Were the healthy analysis counted, the 4th step would already be
// exhausted and only 3 posts would land.
func TestProcess_ReEvalMax_HealthyAnalysisIsFree(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: false,
		DedupePRReEvalMax:      2,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	base := corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}
	failing := eventv1.Event{InvolvedObject: base, Severity: eventv1.EventSeverityError, Message: "boom"}
	healthy := eventv1.Event{InvolvedObject: base}

	steps := []eventv1.Event{failing, failing, healthy, failing, failing}
	for i, ev := range steps {
		if err := proc.Process(context.Background(), ev, "srcrev"); err != nil {
			t.Fatalf("Process #%d: %v", i+1, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	// initial + failing(1/2) + healthy(free) + failing(2/2); the 5th window
	// is exhausted → 4 posts.
	if len(commenterMock.posts) != 4 {
		t.Fatalf("healthy analyses must not burn budget: want 4 posts, got %d", len(commenterMock.posts))
	}
}

// Control: same setup without the cap keeps re-posting every window — proving
// the cutoff above is the budget, not some other dedup.
func TestProcess_ReEvalMaxUnset_KeepsReEvaluating(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: false,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	for i := range 5 {
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process #%d: %v", i+1, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	if len(commenterMock.posts) != 5 {
		t.Fatalf("without a budget every window must re-post, got %d posts", len(commenterMock.posts))
	}
}

// A new commit (new head SHA) resets the budget: the spent counter belongs to
// the old head, and the push gets a fresh initial analysis plus re-evals.
func TestProcess_ReEvalMax_NewHeadGetsFreshBudget(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "prhead111"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: false,
		DedupePRReEvalMax:      1,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, Message: "still broken",
	}

	for i := range 3 { // initial + 1 failing re-eval + 1 exhausted skip
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process #%d: %v", i+1, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	if len(commenterMock.posts) != 2 {
		t.Fatalf("old head: want 2 posts (initial + 1 re-eval), got %d", len(commenterMock.posts))
	}

	commenterMock.prHeadSHA = "prhead222" // the PR gets a new push
	for i := range 3 {
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process after push #%d: %v", i+1, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	if len(commenterMock.posts) != 4 {
		t.Fatalf("new head must get a fresh budget: want 4 posts total, got %d", len(commenterMock.posts))
	}
}

// Regression for erased failure evidence (observed in practice: flux-failed
// added on the failure event, then comment + label wiped by the retry event
// ~3 minutes later). A failing reconcile's inventory can be missing exactly
// the resources the PR added (apply failure, post-revert prune, transiently
// empty status), so an ERROR event's no-match must NOT trigger the de-scope
// cleanup — the comment stays, the label scope stays, and a later successful
// reconcile confirms (or refutes) the de-scope.
func TestProcess_DeScope_DeferredOnErrorEvent(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h1"
	// The PR previously failed for this scope: outcome + roster recorded.
	dd.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	dd.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeFailure), time.Hour)

	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)} // inventory: default_any__ConfigMap
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		// The PR's file no longer matches the (failing) inventory.
		prFiles: []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, // the Kustomization is failing
	}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.removeCalls) != 0 {
		t.Fatalf("error-event no-match must not remove the comment; got %+v", commenterMock.removeCalls)
	}
	if len(commenterMock.labelCalls) != 0 {
		t.Fatalf("error-event no-match must not touch labels; got %+v", commenterMock.labelCalls)
	}
	if outcomes := dd.ScopeOutcomes(owner, repo, pr, head); outcomes["default/apps"] != string(bridgegithub.OutcomeFailure) {
		t.Fatalf("failure outcome must survive the error event; got %+v", outcomes)
	}
}

// An EMPTY inventory (mid-reconcile status, never-applied Kustomization) makes
// every PR a "no-match" — acting on that would erase comments and labels for
// every attributed PR at once. The cleanup must be deferred even on a
// non-error event.
func TestProcess_DeScope_DeferredOnEmptyInventory(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h1"
	dd.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	dd.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeFailure), time.Hour)

	mcpMock := &fakeMCPClient{data: []byte(kustomizationEmptyInventory)}
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles: []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: any\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.removeCalls) != 0 {
		t.Fatalf("empty-inventory no-match must not remove the comment; got %+v", commenterMock.removeCalls)
	}
	if outcomes := dd.ScopeOutcomes(owner, repo, pr, head); outcomes["default/apps"] != string(bridgegithub.OutcomeFailure) {
		t.Fatalf("failure outcome must survive the empty-inventory event; got %+v", outcomes)
	}
}

// An info event can arrive while the revision is still failing (Ready=False):
// the inventory is then the last good one, which may lack the resource the PR
// adds, so its no-match must not erase the failure comment either.
func TestProcess_DeScope_DeferredOnReadyFalseInfoEvent(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h1"
	dd.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	dd.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeFailure), time.Hour)

	mcpMock := &fakeMCPClient{data: []byte(kustomizationFailingReady)}
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles: []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
	}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.removeCalls) != 0 {
		t.Fatalf("Ready=False no-match must not remove the comment; got %+v", commenterMock.removeCalls)
	}
	if outcomes := dd.ScopeOutcomes(owner, repo, pr, head); outcomes["default/apps"] != string(bridgegithub.OutcomeFailure) {
		t.Fatalf("failure outcome must survive the Ready=False info event; got %+v", outcomes)
	}
}

// A changed file the parser couldn't read completely (here: resources wrapped
// in a List's items) proves nothing by not matching: it may declare the very
// resource in the inventory. Its no-match must not erase the PR's verdict.
func TestProcess_DeScope_DeferredOnIncompleteParse(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h1"
	dd.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	dd.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeFailure), time.Hour)

	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		// A base ref, so the no-base-ref rule for empty heads doesn't decide.
		prBaseSHA: "base-sha",
		prFiles:   []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs: map[string][]byte{"b1": []byte("apiVersion: v1\nkind: List\nitems:\n" +
			"- apiVersion: v1\n  kind: ConfigMap\n  metadata:\n    name: any\n    namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
	}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.removeCalls) != 0 {
		t.Fatalf("an incompletely parsed file's no-match must not remove the comment; got %+v", commenterMock.removeCalls)
	}
	if outcomes := dd.ScopeOutcomes(owner, repo, pr, head); outcomes["default/apps"] != string(bridgegithub.OutcomeFailure) {
		t.Fatalf("failure outcome must survive; got %+v", outcomes)
	}
}

// Control: the same no-match on a healthy (non-error) event with a non-empty
// inventory DOES de-scope — the trust gate must not disable legitimate cleanup.
func TestProcess_DeScope_RunsOnTrustedInventory(t *testing.T) {
	dd := dedupe.NewNoExpiry(16)
	owner, repo, pr, head := "owner", "repo", 42, "h1"
	dd.AddPRScope(owner, repo, pr, "default/apps", time.Hour)
	dd.PutScopeOutcome(owner, repo, pr, head, "default/apps", string(bridgegithub.OutcomeFailure), time.Hour)

	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntry)}
	commenterMock := &fakeCommenter{
		prNumber: pr, prState: "open", prHeadSHA: head,
		prFiles: []bridgegithub.PRFile{{Path: "x.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nope\n  namespace: default\n")},
	}
	cfg := &config.Config{GitHubRepo: owner + "/" + repo, LLMProvider: "none", PRLabelEnabled: true, ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
	}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.removeCalls) != 1 {
		t.Fatalf("trusted-inventory no-match must clean up the stale comment; got %+v", commenterMock.removeCalls)
	}
	if outcomes := dd.ScopeOutcomes(owner, repo, pr, head); len(outcomes) != 0 {
		t.Fatalf("scope outcome must be dropped on a trusted de-scope; got %+v", outcomes)
	}
}

// kustomizationFailingReady models a Kustomization whose own reconcile is
// failing (Ready=False, e.g. a dry-run admission rejection) while its
// inventory — the LAST successfully applied state — still polls all-Current.
const kustomizationFailingReady = `
status:
  inventory:
    entries:
      - id: "default_any__ConfigMap"
  conditions:
    - type: Ready
      status: "False"
      reason: ReconciliationFailed
      message: "dry-run failed (Invalid): admission webhook denied the request"
`

// Regression for the false flux-success observed in practice: the deploy was
// rejected at dry-run (error event), but every LIVE resource was Current (the
// old state), so the bridge reported ✅. An error-severity event must force
// OutcomeFailure regardless of how green the inventory snapshot looks.
func TestProcess_ErrorEvent_HealthyInventoryIsStillFailure(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
		CommentIncludeSummary: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError,
		Reason:         "ReconciliationFailed",
		Message:        "Elasticsearch/backstage/elasticsearch dry-run failed (Invalid): admission webhook denied the request",
	}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("expected one comment, got %d", len(commenterMock.posts))
	}
	body := commenterMock.posts[0].Body
	if !strings.Contains(body, statusEmojiFailure) {
		t.Errorf("comment must carry the failure marker; got %q", body)
	}
	if !strings.Contains(body, "reconciliation failed: Elasticsearch/backstage/elasticsearch dry-run failed") {
		t.Errorf("summary must lead with the reconcile failure; got %q", body)
	}
	if n := len(commenterMock.labelCalls); n == 0 || commenterMock.labelCalls[n-1].Outcome != bridgegithub.OutcomeFailure {
		t.Fatalf("expected flux-failed label; got %+v", commenterMock.labelCalls)
	}
}

// Same protection when the analysis is triggered by a NON-error event while
// the Kustomization's Ready condition is False: the controller's own verdict
// in the fetched status forces failure.
func TestProcess_ReadyConditionFalse_ForcesFailure(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationFailingReady),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
		CommentIncludeSummary: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
	}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("expected one comment, got %d", len(commenterMock.posts))
	}
	if body := commenterMock.posts[0].Body; !strings.Contains(body, "reconciliation failed: dry-run failed") {
		t.Errorf("summary must surface the Ready=False message; got %q", body)
	}
	if n := len(commenterMock.labelCalls); n == 0 || commenterMock.labelCalls[n-1].Outcome != bridgegithub.OutcomeFailure {
		t.Fatalf("expected flux-failed label; got %+v", commenterMock.labelCalls)
	}
}

// Regression for the suppressed retries on the same deploy: the head's
// outcome record said SUCCESS (mis-recorded), so DEDUPE_REEVAL_FAILED_ONLY
// skipped the ReconciliationFailed retry events. An error event is firsthand
// evidence the record is stale — it must be analysed, not skipped.
// A previously-green head is FROZEN until a new commit: even an error event
// for the same head does NOT re-wake it (re-waking green
// heads on Kustomization-level failures flipped unrelated PRs). Genuine
// breakage of a green deploy waits for the PR's next push; a fresh head is
// always analysed.
func TestProcess_ReEvalFailedOnly_GreenHeadFrozenAgainstErrorEvent(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: true,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	base := corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}

	// First analysis: genuinely healthy → success recorded.
	if err := proc.Process(context.Background(), eventv1.Event{InvolvedObject: base}, "srcrev"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("first run should post; got %d", len(commenterMock.posts))
	}
	time.Sleep(3 * time.Millisecond) // let the re-eval floor elapse

	// An error event arrives for the SAME head — it must NOT re-wake the
	// frozen green head (no second post).
	errEvent := eventv1.Event{InvolvedObject: base, Severity: eventv1.EventSeverityError, Message: "dry-run failed"}
	if err := proc.Process(context.Background(), errEvent, "srcrev2"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("green head must stay frozen against an error event; got %d posts", len(commenterMock.posts))
	}
}

// Regression for "intentionally deleted resources read as failures" (observed
// in practice): a resource the PR removes stays in the event-time inventory
// snapshot until Flux prunes it, polls NotFound, and was counted as ❌. The
// pruned-row filter re-reads the inventory and drops NotFound rows whose
// entry is gone — while keeping a NotFound row that the inventory still
// claims (genuinely missing).
func TestDropPrunedRows_DropsPrunedKeepsMissing(t *testing.T) {
	// Fresh inventory (the re-read) lists ONLY keepme: deleteme was pruned.
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntryKeepme)}
	proc := New(&config.Config{}, mcpMock, nil, nil, &fakeCommenter{}, nil, nil, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}
	originalRefs := []string{"default_keepme__ConfigMap", "default_deleteme__ConfigMap"}

	statuses := []readiness.ResourceStatus{
		{Kind: "ConfigMap", Namespace: "default", Name: "keepme", Status: "NotFound"},   // still in inventory → keep (real failure)
		{Kind: "ConfigMap", Namespace: "default", Name: "deleteme", Status: "NotFound"}, // pruned → drop
	}
	got := proc.dropPrunedRows(context.Background(), event, originalRefs, statuses)
	if len(got) != 1 || got[0].Name != "keepme" {
		t.Fatalf("want only the still-inventoried NotFound row kept, got %+v", got)
	}
}

// A NotFound row that never came from the inventory (a HelmRelease-expanded
// child) must NOT be dropped — its absence is a real signal, and the
// inventory re-read can't vouch for it either way.
func TestDropPrunedRows_KeepsNonInventoryRows(t *testing.T) {
	mcpMock := &fakeMCPClient{data: []byte(kustomizationOneInventoryEntryKeepme)}
	proc := New(&config.Config{}, mcpMock, nil, nil, &fakeCommenter{}, nil, nil, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	statuses := []readiness.ResourceStatus{
		{Kind: "Deployment", Namespace: "default", Name: "helm-child", Status: "NotFound"},
	}
	got := proc.dropPrunedRows(context.Background(), event, []string{"default_keepme__ConfigMap"}, statuses)
	if len(got) != 1 {
		t.Fatalf("expanded-child NotFound row must be kept, got %+v", got)
	}
}

// An empty fresh inventory is not trusted: a failing or mid-reconcile
// Kustomization can transiently report no inventory, and trusting it would
// silently drop every genuine NotFound failure at once. Same for a re-read
// error: keep everything (fail-open to current behavior).
func TestDropPrunedRows_UntrustedRereadKeepsRows(t *testing.T) {
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}
	originalRefs := []string{"default_deleteme__ConfigMap"}
	statuses := []readiness.ResourceStatus{
		{Kind: "ConfigMap", Namespace: "default", Name: "deleteme", Status: "NotFound"},
	}

	emptyInv := New(&config.Config{}, &fakeMCPClient{data: []byte(kustomizationEmptyInventory)}, nil, nil, &fakeCommenter{}, nil, nil, slog.Default())
	if got := emptyInv.dropPrunedRows(context.Background(), event, originalRefs, statuses); len(got) != 1 {
		t.Fatalf("empty fresh inventory must keep rows, got %+v", got)
	}

	erroring := New(&config.Config{}, &fakeMCPClient{err: errors.New("mcp down")}, nil, nil, &fakeCommenter{}, nil, nil, slog.Default())
	if got := erroring.dropPrunedRows(context.Background(), event, originalRefs, statuses); len(got) != 1 {
		t.Fatalf("re-read error must keep rows, got %+v", got)
	}
}

// Regression for the comment that buried the root cause in prose
// with no actionable steps: when the outcome is non-healthy and the LLM reply
// omits the SKILL-mandated `## Root Cause` / `## Next steps to fix` sections,
// the bridge retries once with a corrective instruction and uses the
// compliant retry.
func TestProcess_LLMMissingFailureSections_RetriesOnce(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{results: []string{
		"## Kustomization Status\nSomething broke, trust me.",
		"## Kustomization Status\nSomething broke.\n## Root Cause\nthe proof\n## Next steps to fix\n1. fix it",
	}}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	// Error severity → failure outcome despite the healthy inventory.
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, Message: "dry-run failed",
	}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if analyzerMock.calls != 2 {
		t.Fatalf("want exactly one corrective retry (2 calls), got %d", analyzerMock.calls)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("want one post, got %d", len(commenterMock.posts))
	}
	body := commenterMock.posts[0].Body
	if !strings.Contains(body, "## Root Cause") || !strings.Contains(body, "## Next steps to fix") {
		t.Errorf("final comment must carry the retry's compliant sections; got %q", body)
	}
}

// A compliant first reply must not trigger the retry, and a HEALTHY outcome
// must never demand the failure sections at all.
func TestProcess_LLMSectionRetry_NotTriggeredWhenCompliantOrHealthy(t *testing.T) {
	newProc := func(analyzer *fakeAnalyzer, commenter *fakeCommenter) *Processor {
		mcpMock := &fakeMCPClient{
			data:      []byte(kustomizationOneInventoryEntry),
			resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
		}
		cfg := &config.Config{
			GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true,
			ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		}
		return New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzer, commenter, nil, dedupe.NewNoExpiry(16), slog.Default())
	}
	base := corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}

	// Failure outcome + compliant reply → single call.
	compliant := &fakeAnalyzer{result: "## Kustomization Status\nbroken\n## Root Cause\nproof\n## Next steps to fix\n1. fix"}
	if err := newProc(compliant, &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}).Process(context.Background(),
		eventv1.Event{InvolvedObject: base, Severity: eventv1.EventSeverityError, Message: "boom"}, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if compliant.calls != 1 {
		t.Fatalf("compliant reply must not retry, got %d calls", compliant.calls)
	}

	// Healthy outcome + sectionless reply → single call (sections not required).
	healthy := &fakeAnalyzer{result: "## Kustomization Status\nall good"}
	if err := newProc(healthy, &fakeCommenter{prNumber: 43, prState: "open", prHeadSHA: "h2"}).Process(context.Background(),
		eventv1.Event{InvolvedObject: base}, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if healthy.calls != 1 {
		t.Fatalf("healthy outcome must not demand failure sections, got %d calls", healthy.calls)
	}
}

// rolloutDeploymentYAML is a Deployment mid-rollout: 1 of 2 replicas updated,
// no failure condition → kstatus InProgress.
const rolloutDeploymentYAML = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: default
  generation: 2
spec:
  replicas: 2
status:
  observedGeneration: 2
  replicas: 2
  updatedReplicas: 1
  readyReplicas: 1
  availableReplicas: 1
  conditions:
    - type: Progressing
      status: "True"
      reason: ReplicaSetUpdated
`

const kustomizationRolloutInventory = `status:
  inventory:
    entries:
      - id: "default_any__ConfigMap"
      - id: "default_api_apps_Deployment"
`

// A rollout the poll didn't see finish is ⚠️ but not a failure: the payload
// says so, and a reply without Root Cause / Next steps must NOT trigger the
// corrective retry (the model would invent a cause from routine
// pod-replacement events such as probe failures).
func TestProcess_RolloutInProgress_NoForcedRootCause(t *testing.T) {
	mcpMock := &fakeMCPClient{
		// The ConfigMap admits the PR (the fake commenter's default blob). The
		// Deployment sits in the target namespace, so the pod scan covers it.
		data: []byte("spec:\n  targetNamespace: default\n" + kustomizationRolloutInventory),
		resources: map[string][]byte{
			"v1|ConfigMap|any|default":       []byte(healthyConfigMapYAML),
			"apps/v1|Deployment|api|default": []byte(rolloutDeploymentYAML),
		},
		// A successful (empty) pod scan: without one, pod health is unproven.
		lists: map[string][]byte{"v1|Pod|default": []byte("items: []\n")},
	}
	analyzerMock := &fakeAnalyzer{result: "## Kustomization Status\nDeployment/default/api is still rolling out (1/2 updated)."}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true, ReadinessIncludePods: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !strings.Contains(analyzerMock.lastUserPayload, "## Rollout In Progress") {
		t.Errorf("payload must carry the rollout-in-progress note; got %q", analyzerMock.lastUserPayload)
	}
	if analyzerMock.calls != 1 {
		t.Fatalf("an in-progress rollout must not force the failure sections, got %d calls", analyzerMock.calls)
	}
	if len(commenterMock.posts) != 1 || !strings.Contains(commenterMock.posts[0].Body, "⚠️") {
		t.Fatalf("want one ⚠️ comment, got %+v", commenterMock.posts)
	}
}

// A model that ignores the SKILL and writes a Root Cause on a rollout in
// progress gets both failure sections removed; its status paragraph stays.
func TestProcess_RolloutInProgress_StripsFailureSections(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte("spec:\n  targetNamespace: default\n" + kustomizationRolloutInventory),
		resources: map[string][]byte{
			"v1|ConfigMap|any|default":       []byte(healthyConfigMapYAML),
			"apps/v1|Deployment|api|default": []byte(rolloutDeploymentYAML),
		},
		lists: map[string][]byte{"v1|Pod|default": []byte("items: []\n")},
	}
	analyzerMock := &fakeAnalyzer{result: "## Kustomization Status\nDeployment/default/api is still rolling out.\n\n" +
		"## Root Cause\nReadiness probe failed: connection refused\n\n## Next steps to fix\n1. Change the probe"}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true, ReadinessIncludePods: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("want one post, got %d", len(commenterMock.posts))
	}
	body := commenterMock.posts[0].Body
	if strings.Contains(body, "## Root Cause") || strings.Contains(body, "## Next steps") || strings.Contains(body, "Change the probe") {
		t.Errorf("failure sections must be removed on a rollout in progress; got %q", body)
	}
	if !strings.Contains(body, "is still rolling out") {
		t.Errorf("the status paragraph must stay; got %q", body)
	}
}

func TestStripFailureSections(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{
			name: "both sections removed",
			in:   "## Kustomization Status\nrolling out\n\n## Root Cause\nprobe\n\n## Next steps to fix\n1. fix",
			want: "## Kustomization Status\nrolling out",
		},
		{
			name: "a later section is kept",
			in:   "## Kustomization Status\nok\n## Root Cause\nx\n## Notes\nkept",
			want: "## Kustomization Status\nok\n## Notes\nkept",
		},
		{
			name: "heading inside a fence is content",
			in:   "## Kustomization Status\n```\n## Root Cause\n```\nafter",
			want: "## Kustomization Status\n```\n## Root Cause\n```\nafter",
		},
		{
			name: "heading in indented code is content",
			in:   "## Kustomization Status\nok\n\n    ## Root Cause\n\t## Next steps to fix\nafter",
			want: "## Kustomization Status\nok\n\n    ## Root Cause\n\t## Next steps to fix\nafter",
		},
		{
			name: "heading with up to three spaces is a heading",
			in:   "## Kustomization Status\nok\n   ## Root Cause\nx",
			want: "## Kustomization Status\nok",
		},
		{
			name: "a title that only starts with the phrase is kept",
			in:   "## Kustomization Status\nok\n## Next stepstones\nkept",
			want: "## Kustomization Status\nok\n## Next stepstones\nkept",
		},
		{
			name: "tab after the hashes is a heading",
			in:   "## Kustomization Status\nok\n##\tRoot Cause\nx\n##\tNotes\nkept",
			want: "## Kustomization Status\nok\n##\tNotes\nkept",
		},
		{
			name: "subheading inside a failure section is dropped with it",
			in:   "## Kustomization Status\nok\n## Root Cause\n### Evidence\nx\n## Notes\nkept",
			want: "## Kustomization Status\nok\n## Notes\nkept",
		},
		{
			name: "hashes without a separator are text",
			in:   "## Kustomization Status\nok\n##Root Cause\nkept",
			want: "## Kustomization Status\nok\n##Root Cause\nkept",
		},
		{
			name: "case-insensitive heading",
			in:   "## Kustomization Status\nok\n## ROOT CAUSE\nx",
			want: "## Kustomization Status\nok",
		},
		{name: "no sections", in: "## Kustomization Status\nok", want: "## Kustomization Status\nok"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripFailureSections(tt.in); got != tt.want {
				t.Errorf("stripFailureSections =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

// A failing pod found by the failure sweep's own pod list revokes the
// exemption: no "nothing has failed" note next to fresh crash logs, and the
// failure sections are enforced again. Here the pod starts failing between
// the readiness snapshot and the sweep.
func TestProcess_RolloutInProgress_RevokedByFreshFailingPod(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte("spec:\n  targetNamespace: default\n" + kustomizationRolloutInventory),
		resources: map[string][]byte{
			"v1|ConfigMap|any|default":       []byte(healthyConfigMapYAML),
			"apps/v1|Deployment|api|default": []byte(rolloutDeploymentYAML),
		},
		// Snapshot: no pods yet. Failure sweep: a crash-looping pod.
		listQueue: map[string][][]byte{"v1|Pod|default": {[]byte("items: []\n"), []byte(crashlooperPodListYAML)}},
	}
	analyzerMock := &fakeAnalyzer{results: []string{
		"## Kustomization Status\nstill rolling out",
		"## Kustomization Status\nbroken\n## Root Cause\ncrash\n## Next steps to fix\n1. fix",
	}}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true, ReadinessIncludePods: true,
		FailureFetchLogs:      true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if strings.Contains(analyzerMock.lastUserPayload, "## Rollout In Progress") {
		t.Errorf("a freshly failing pod must drop the rollout note; got %q", analyzerMock.lastUserPayload)
	}
	if analyzerMock.calls != 2 {
		t.Fatalf("failure sections must be enforced again (2 calls), got %d", analyzerMock.calls)
	}
}

// stuckStatefulSetYAML is a StatefulSet whose pod was never created:
// 0 of 1 replicas, no condition to say why → kstatus InProgress.
const stuckStatefulSetYAML = `apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: db
  namespace: default
  generation: 2
spec:
  replicas: 1
status:
  observedGeneration: 2
  replicas: 0
  readyReplicas: 0
  updatedReplicas: 0
`

// failedCreateEventsYAML is the statefulset-controller's event when an
// admission webhook rejects the pod.
const failedCreateEventsYAML = `items:
- type: Warning
  reason: FailedCreate
  message: 'create Pod db-0 in StatefulSet db failed error: admission webhook "validate.example.com" denied the request'
  count: 3
  source:
    component: statefulset-controller
  involvedObject:
    kind: StatefulSet
    name: db
`

// A StatefulSet whose pod creation is rejected has no pod to classify and no
// condition naming the failure — only the FailedCreate event. That event
// revokes the exemption.
func TestProcess_RolloutInProgress_RevokedByFailedCreateEvent(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte("spec:\n  targetNamespace: default\nstatus:\n  inventory:\n    entries:\n      - id: \"default_any__ConfigMap\"\n      - id: \"default_db_apps_StatefulSet\"\n"),
		resources: map[string][]byte{
			"v1|ConfigMap|any|default":       []byte(healthyConfigMapYAML),
			"apps/v1|StatefulSet|db|default": []byte(stuckStatefulSetYAML),
		},
		lists: map[string][]byte{
			"v1|Pod|default":   []byte("items: []\n"),
			"v1|Event|default": []byte(failedCreateEventsYAML),
		},
	}
	analyzerMock := &fakeAnalyzer{results: []string{
		"## Kustomization Status\nstill rolling out",
		"## Kustomization Status\nblocked\n## Root Cause\nwebhook\n## Next steps to fix\n1. fix",
	}}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true, ReadinessIncludePods: true,
		FailureFetchEvents:    true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !strings.Contains(analyzerMock.lastUserPayload, "FailedCreate") {
		t.Fatalf("payload must carry the FailedCreate event; got %q", analyzerMock.lastUserPayload)
	}
	if strings.Contains(analyzerMock.lastUserPayload, "## Rollout In Progress") {
		t.Errorf("a FailedCreate event must drop the rollout note; got %q", analyzerMock.lastUserPayload)
	}
	if analyzerMock.calls != 2 {
		t.Fatalf("failure sections must be enforced again (2 calls), got %d", analyzerMock.calls)
	}
}

func TestHasFailedCreateEvent(t *testing.T) {
	eventFor := func(typ, reason, ts, name string) []byte {
		return []byte("items:\n- type: " + typ + "\n  reason: " + reason + "\n  lastTimestamp: \"" + ts +
			"\"\n  source:\n    component: statefulset-controller\n  involvedObject:\n    kind: StatefulSet\n    name: " + name + "\n")
	}
	event := func(typ, reason, ts string) []byte { return eventFor(typ, reason, ts, "db") }
	rollingOut := map[string]bool{"StatefulSet/db": true}
	cutoff := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		body   []byte
		cutoff time.Time
		want   bool
	}{
		{name: "Warning FailedCreate", body: event("Warning", "FailedCreate", "2026-01-01T12:05:00Z"), want: true},
		{name: "after cutoff (included)", body: event("Warning", "FailedCreate", "2026-01-01T12:05:00Z"), cutoff: cutoff, want: true},
		{name: "at cutoff (included)", body: event("Warning", "FailedCreate", "2026-01-01T12:00:00Z"), cutoff: cutoff, want: true},
		{name: "before cutoff", body: event("Warning", "FailedCreate", "2026-01-01T11:00:00Z"), cutoff: cutoff},
		{name: "Normal type", body: event("Normal", "FailedCreate", "2026-01-01T12:05:00Z")},
		{name: "routine rollout warning", body: event("Warning", "Unhealthy", "2026-01-01T12:05:00Z")},
		{name: "empty list", body: []byte("items: []\n")},
		{name: "unrelated workload", body: eventFor("Warning", "FailedCreate", "2026-01-01T12:05:00Z", "other")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasFailedCreateEvent(tt.body, tt.cutoff, rollingOut); got != tt.want {
				t.Errorf("hasFailedCreateEvent = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRolloutInProgress(t *testing.T) {
	inProgress := readiness.ResourceStatus{Kind: "StatefulSet", Namespace: "apps", Name: "db", Status: "InProgress"}
	startingPod := readiness.ResourceStatus{Kind: "Pod", Namespace: "apps", Name: "db-0", Status: "InProgress", Controller: "StatefulSet/db"}
	current := readiness.ResourceStatus{Kind: "ConfigMap", Name: "cfg", Status: "Current"}
	withCond := func(condType, condStatus, reason string) readiness.ResourceStatus {
		r := readiness.ResourceStatus{Kind: "HelmRelease", Namespace: "apps", Name: "db", Status: "InProgress"}
		r.Conditions = []readiness.Condition{{Type: condType, Status: condStatus, Reason: reason}}
		return r
	}
	rows := func(r ...readiness.ResourceStatus) []readiness.ResourceStatus { return r }
	warning := bridgegithub.OutcomeWarning

	tests := []struct {
		name     string
		outcome  bridgegithub.Outcome
		statuses []readiness.ResourceStatus
		pods     []readiness.ResourceStatus
		noScan   bool
		waiting  bool
		pending  bool
		want     bool
	}{
		{name: "InProgress rows and pods", outcome: warning, statuses: rows(current, inProgress), pods: rows(startingPod), want: true},
		{name: "upgrade still running", outcome: warning, statuses: rows(withCond("Ready", "Unknown", "Progressing")), want: true},
		{name: "pod scan failed or disabled", outcome: warning, statuses: rows(inProgress), noScan: true},
		{name: "workload outside the scanned namespace", outcome: warning,
			statuses: rows(readiness.ResourceStatus{Kind: "Deployment", Namespace: "other", Name: "api", Status: "InProgress"})},
		{name: "custom resource kind outside the scanned namespace", outcome: warning,
			statuses: rows(readiness.ResourceStatus{Kind: "Database", Namespace: "other", Name: "db", Status: "InProgress"})},
		// A cluster-scoped custom resource's operator may run its pods in any
		// namespace, none of which was scanned.
		{name: "cluster-scoped row", outcome: warning,
			statuses: rows(readiness.ResourceStatus{Kind: "ClusterDatabase", Name: "db", Status: "InProgress"})},
		{name: "success is not a rollout", outcome: bridgegithub.OutcomeSuccess, statuses: rows(current)},
		{name: "failure is not a rollout", outcome: bridgegithub.OutcomeFailure, statuses: rows(inProgress)},
		{name: "dependency wait", outcome: warning, statuses: rows(current), waiting: true},
		{name: "HelmRelease children unseen", outcome: warning, statuses: rows(current), pending: true},
		{name: "Terminating row", outcome: warning, statuses: rows(readiness.ResourceStatus{Status: "Terminating"})},
		{name: "Unknown pod", outcome: warning, pods: rows(readiness.ResourceStatus{Status: "Unknown"})},
		{name: "Ready=False SourceNotReady", outcome: warning, statuses: rows(withCond("Ready", "False", "SourceNotReady"))},
		{name: "Stalled=True", outcome: warning, statuses: rows(withCond("Stalled", "True", "RetriesExceeded"))},
		{name: "retry after failed upgrade", outcome: warning, statuses: rows(withCond("Reconciling", "True", "ProgressingWithRetry"))},
		{name: "UpgradeFailed reason", outcome: warning, statuses: rows(withCond("Released", "False", "UpgradeFailed"))},
		{name: "FailedCreate reason", outcome: warning, statuses: rows(withCond("ReplicaFailure", "True", "FailedCreate"))},
		{name: "ProgressDeadlineExceeded reason", outcome: warning, statuses: rows(withCond("Progressing", "False", "ProgressDeadlineExceeded"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rolloutInProgress(tt.outcome, tt.statuses, tt.pods, "apps", !tt.noScan, tt.waiting, tt.pending); got != tt.want {
				t.Errorf("rolloutInProgress = %v, want %v", got, tt.want)
			}
		})
	}
}

// A mention the model quotes from a log must reach the PR neutralized: the
// delivered comment carries the reply's prose, not a live @-mention.
func TestProcess_LLMReplyMentionsNeutralized(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{result: "## Kustomization Status\nThe log asks to ping @org/oncall."}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("want one post, got %d", len(commenterMock.posts))
	}
	body := commenterMock.posts[0].Body
	if strings.Contains(body, "@org/oncall") || !strings.Contains(body, "@\u200borg/oncall") {
		t.Errorf("comment must carry the mention neutralized; got %q", body)
	}
}

const kustomizationOneInventoryEntryKeepme = `
status:
  inventory:
    entries:
      - id: "default_keepme__ConfigMap"
`

const kustomizationTwoEntriesKeepmeDeleteme = `
status:
  inventory:
    entries:
      - id: "default_keepme__ConfigMap"
      - id: "default_deleteme__ConfigMap"
`

const keepmeConfigMapYAML = `apiVersion: v1
kind: ConfigMap
metadata:
  name: keepme
  namespace: default
`

// End-to-end regression for the settled flag after a prune (Copilot review on
// the pruned-row filter): NotFound is non-terminal for the readiness poll, so
// a deleted resource holds the poll open until timeout and leaves
// settled=false — and without recomputation the run reads ⚠️ "timed out"
// even after the pruned row is dropped. An intentional deletion whose
// remaining rows are all Current must end ✅ settled.
func TestProcess_PrunedDeletion_EndsHealthyNotTimedOut(t *testing.T) {
	mcpMock := &fakeMCPClient{
		dataQueue: [][]byte{
			[]byte(kustomizationTwoEntriesKeepmeDeleteme), // event-time fetch: deleteme still listed
			[]byte(kustomizationOneInventoryEntryKeepme),  // re-read: deleteme pruned
		},
		// deleteme has no resource entry → GetKubernetesResource "not found"
		// → NotFound row → poll runs to its (short) timeout.
		resources: map[string][]byte{"v1|ConfigMap|keepme|default": []byte(keepmeConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{
		prNumber: 42, prState: "open", prHeadSHA: "h1",
		// The PR declares keepme (still in inventory) and previously declared
		// deleteme — the deletion under test.
		prFiles: []bridgegithub.PRFile{{Path: "keep.yaml", BlobSHA: "b1", Status: "modified"}},
		blobs:   map[string][]byte{"b1": []byte(keepmeConfigMapYAML)},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
		CommentIncludeSummary: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("want one post, got %d", len(commenterMock.posts))
	}
	body := commenterMock.posts[0].Body
	if !strings.Contains(body, statusEmojiHealthy) || strings.Contains(body, statusEmojiWarning) {
		t.Errorf("pruned deletion must read healthy, not timed-out warning; got %q", body)
	}
	if strings.Contains(body, "deleteme") {
		t.Errorf("pruned resource must not appear in the comment; got %q", body)
	}
	if n := len(commenterMock.labelCalls); n == 0 || commenterMock.labelCalls[n-1].Outcome != bridgegithub.OutcomeSuccess {
		t.Fatalf("want flux-success label; got %+v", commenterMock.labelCalls)
	}
}

// A PR that deletes the LAST resources of a Kustomization leaves a genuinely
// empty post-prune inventory. With the fetched object's Ready condition True,
// the controller vouches that the reconcile (including the prune) completed —
// the emptiness is real and the NotFound rows are prunes, not failures.
// Without Ready=True (covered in TestDropPrunedRows_UntrustedRereadKeepsRows)
// the conservative keep-everything path still applies.
func TestDropPrunedRows_TrustsEmptyInventoryWhenReady(t *testing.T) {
	const emptyButReady = `
status:
  inventory:
    entries: []
  conditions:
    - type: Ready
      status: "True"
      reason: ReconciliationSucceeded
`
	mcpMock := &fakeMCPClient{data: []byte(emptyButReady)}
	proc := New(&config.Config{}, mcpMock, nil, nil, &fakeCommenter{}, nil, nil, slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	statuses := []readiness.ResourceStatus{
		{Kind: "ConfigMap", Namespace: "default", Name: "deleteme", Status: "NotFound"},
	}
	got := proc.dropPrunedRows(context.Background(), event, []string{"default_deleteme__ConfigMap"}, statuses)
	if len(got) != 0 {
		t.Fatalf("last-resource deletion with Ready=True must drop the pruned row, got %+v", got)
	}
}

// A green head stays frozen against pod-level runtime drift too: a workload
// that starts failing after a green analysis (no new commit, no error event)
// is NOT re-analysed until the PR is pushed. This is the deliberate trade-off
// for not flipping green PRs when unrelated siblings in the shared
// Kustomization break; the previously-implemented drift
// probe is intentionally gone.
func TestProcess_ReEvalFailedOnly_GreenHeadFrozenAgainstPodDrift(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
		lists:     map[string][]byte{}, // first analysis sees a healthy namespace
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: true,
		ReadinessIncludePods:   true,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "default", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("first run should post the healthy comment; got %d", len(commenterMock.posts))
	}

	// A pod starts crashing — no new commit, no error event, Kustomization
	// still green. The frozen head must NOT be re-analysed.
	mcpMock.lists["v1|Pod|default"] = []byte(crashlooperPodListYAML)
	time.Sleep(3 * time.Millisecond) // let the re-eval floor elapse

	if err := proc.Process(context.Background(), event, "srcrev2"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("green head must stay frozen against pod drift; got %d posts", len(commenterMock.posts))
	}
}

// End-to-end flap regression (a CronJob failing every run): in
// the gap between runs the failed pods are already cleaned, the namespace
// samples clean, and the analysis used to flip back to ✅/flux-success — only
// to flip ❌ again on the next run. With the Job-object fallback the gap
// sample still reads the newest run's Failed condition → stable ❌.
func TestProcess_CronJobGapSample_StaysFailed(t *testing.T) {
	const failedJobYAML = `items:
- apiVersion: batch/v1
  kind: Job
  metadata:
    name: judge-29686335
    namespace: default
    ownerReferences:
    - kind: CronJob
      name: judge
      controller: true
  status:
    conditions:
    - type: Failed
      status: "True"
      message: Job has reached the specified backoff limit
`
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
		// No pod list entry: the namespace samples clean (gap between runs).
		lists: map[string][]byte{"batch/v1|Job|default": []byte(failedJobYAML)},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
		CommentIncludeSummary: true, CommentOnSuccess: true, ReadinessIncludePods: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "default", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("want one post, got %d", len(commenterMock.posts))
	}
	body := commenterMock.posts[0].Body
	if !strings.Contains(body, statusEmojiFailure) || !strings.Contains(body, "backoff limit") {
		t.Errorf("gap sample must stay failed via the Job condition; got %q", body)
	}
	// The failing row is a Job condition, not a pod — the summary must not
	// claim "pod(s) failed" about a namespace with zero pods.
	if !strings.Contains(body, "workload(s) failed") || strings.Contains(body, "pod(s) failed") {
		t.Errorf("summary must say workload(s), not pod(s), for Job-condition failures; got %q", body)
	}
	if n := len(commenterMock.labelCalls); n == 0 || commenterMock.labelCalls[n-1].Outcome != bridgegithub.OutcomeFailure {
		t.Fatalf("want flux-failed despite the clean pod sample; got %+v", commenterMock.labelCalls)
	}
}

// The fallback is a gap-closer, not a substitute for a failed scan: when the
// pod list itself is inconclusive (parse error), "no pod for the newest run"
// is not evidence of cleanup, so no Job-condition rows may be emitted.
func TestProcess_CronJobFallback_SkippedWhenPodScanInconclusive(t *testing.T) {
	const failedJobYAML = `items:
- apiVersion: batch/v1
  kind: Job
  metadata:
    name: judge-29686335
    namespace: default
    ownerReferences:
    - kind: CronJob
      name: judge
      controller: true
  status:
    conditions:
    - type: Failed
      status: "True"
      message: Job has reached the specified backoff limit
`
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
		lists: map[string][]byte{
			"v1|Pod|default":       []byte("\t::: not yaml"), // pod scan fails → inconclusive
			"batch/v1|Job|default": []byte(failedJobYAML),
		},
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		CommentIncludeSummary: true, CommentOnSuccess: true, ReadinessIncludePods: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "default", Namespace: "default"}}

	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("want one post, got %d", len(commenterMock.posts))
	}
	if body := commenterMock.posts[0].Body; strings.Contains(body, "backoff limit") || strings.Contains(body, statusEmojiFailure) {
		t.Errorf("inconclusive pod scan must not admit Job-condition rows; got %q", body)
	}
}

// Regression for the broken comment: the model imitated the
// SKILL slot diagram's box art and wrapped its headings in ┌─┐ │…│ └─┘ —
// rendering stray "table borders" and hiding the ## headings from Markdown.
// The sanitizer must drop pure-decoration lines and unwrap boxed content,
// while leaving real Markdown (ASCII | tables, prose, code) untouched.
func TestStripBoxDecoration(t *testing.T) {
	in := "┌─────────────────────────────┐\n" +
		"│   ## Kustomization Status    │\n" +
		"└─────────────────────────────┘\n" +
		"\n" +
		"All good prose with | a pipe | table row kept.\n" +
		"───\n" +
		"║ ## Root Cause ║\n" +
		"`Failed — deadline exceeded`\n"
	got := stripBoxDecoration(in)
	want := "## Kustomization Status\n" +
		"\n" +
		"All good prose with | a pipe | table row kept.\n" +
		"## Root Cause\n" +
		"`Failed — deadline exceeded`\n"
	if got != want {
		t.Fatalf("stripBoxDecoration:\n got %q\nwant %q", got, want)
	}
	// Decoration-free replies pass through byte-identically (fast path).
	clean := "## Kustomization Status\nfine.\n## Root Cause\nx\n## Next steps to fix\n1. y"
	if stripBoxDecoration(clean) != clean {
		t.Fatal("clean reply must be unchanged")
	}

	// Box-drawing inside fenced code blocks is quoted evidence (log
	// excerpts, tree output), not decoration — it must survive verbatim,
	// while decoration outside the fence is still stripped.
	fenced := "## Root Cause\n" +
		"```\n" +
		"├── values.yaml\n" +
		"│   └── prod.yaml\n" +
		"───────────────\n" +
		"```\n" +
		"┌────────┐\n" +
		"│ boxed  │\n" +
		"└────────┘\n"
	wantFenced := "## Root Cause\n" +
		"```\n" +
		"├── values.yaml\n" +
		"│   └── prod.yaml\n" +
		"───────────────\n" +
		"```\n" +
		"boxed\n"
	if got := stripBoxDecoration(fenced); got != wantFenced {
		t.Fatalf("fenced content must be untouched:\n got %q\nwant %q", got, wantFenced)
	}
}

// Regression for the perpetual comment swaps on a persistently-failing PR
// (observed in practice: every ~15m re-eval re-ran the LLM, the narrative
// came back re-worded, the full-body hash differed, and the comment was
// deleted+recreated — notification churn and visible no-comment gaps). The
// repost decision is keyed on the DETERMINISTIC kstatus body: identical
// state must skip both the LLM round-trip and the repost; a real state
// change still swaps.
func TestProcess_UnchangedState_SkipsLLMAndRepost(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{results: []string{
		"## Kustomization Status\nbroken, wording A\n## Root Cause\nx\n## Next steps to fix\n1. y",
		"## Kustomization Status\nbroken, wording B (re-worded!)\n## Root Cause\nx\n## Next steps to fix\n1. y",
	}}
	commenterMock := &fakeCommenter{
		prNumber: 42, prState: "open", prHeadSHA: "h1",
		stabilityKeys: map[int]string{}, // opt into currency tracking
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true,
		CommentIncludeSummary:  true, // the summary carries the failure message the third phase changes
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: true,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	base := corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}
	failing := eventv1.Event{InvolvedObject: base, Severity: eventv1.EventSeverityError, Message: "dry-run failed"}

	if err := proc.Process(context.Background(), failing, "srcrev1"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if len(commenterMock.posts) != 1 || analyzerMock.calls != 1 {
		t.Fatalf("first run: want 1 post / 1 LLM call, got %d / %d", len(commenterMock.posts), analyzerMock.calls)
	}
	time.Sleep(3 * time.Millisecond) // let the re-eval floor elapse

	// Same failing state on re-eval: the LLM would re-word the analysis, but
	// the deterministic body is identical — no LLM call, no repost, no swap.
	if err := proc.Process(context.Background(), failing, "srcrev2"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("identical state must not repost, got %d posts", len(commenterMock.posts))
	}
	if analyzerMock.calls != 1 {
		t.Fatalf("identical state must skip the LLM, got %d calls", analyzerMock.calls)
	}
	time.Sleep(3 * time.Millisecond)

	// The failure message changes → deterministic body changes → full
	// pipeline runs again and the comment is swapped.
	changed := eventv1.Event{InvolvedObject: base, Severity: eventv1.EventSeverityError, Message: "a different failure"}
	if err := proc.Process(context.Background(), changed, "srcrev3"); err != nil {
		t.Fatalf("third Process: %v", err)
	}
	if len(commenterMock.posts) != 2 || analyzerMock.calls != 2 {
		t.Fatalf("changed state must re-analyse and repost: want 2/2, got %d posts / %d calls", len(commenterMock.posts), analyzerMock.calls)
	}
}

// Regression for a PR flagged ❌/flux-failed while its Kustomization was
// merely QUEUED behind a dependsOn target ("dependency 'flux-system/
// cluster-base' is not ready"). DependencyNotReady is a self-resolving
// ordering state: it must read ⚠️/flux-warning — via the Ready condition on a
// non-error event AND via an error-severity DependencyNotReady event.
func TestProcess_DependencyNotReady_IsWarningNotFailure(t *testing.T) {
	const kustomizationWaitingOnDependency = `
status:
  inventory:
    entries:
      - id: "default_any__ConfigMap"
  conditions:
    - type: Ready
      status: "False"
      reason: DependencyNotReady
      message: "dependency 'flux-system/cluster-base' is not ready"
`
	run := func(t *testing.T, kYAML string, event eventv1.Event) *fakeCommenter {
		t.Helper()
		mcpMock := &fakeMCPClient{
			data:      []byte(kYAML),
			resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
		}
		commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1"}
		cfg := &config.Config{
			GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
			CommentIncludeSummary: true, CommentOnSuccess: true,
			ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		}
		proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(commenterMock.posts) != 1 {
			t.Fatalf("want one post, got %d", len(commenterMock.posts))
		}
		return commenterMock
	}
	assertWarning := func(t *testing.T, c *fakeCommenter) {
		t.Helper()
		body := c.posts[0].Body
		if !strings.Contains(body, statusEmojiWarning) || strings.Contains(body, statusEmojiFailure) {
			t.Errorf("dependency wait must read ⚠️, not ❌; got %q", body)
		}
		if !strings.Contains(body, "waiting on a dependency: dependency 'flux-system/cluster-base' is not ready") {
			t.Errorf("summary must lead with the dependency wait; got %q", body)
		}
		if n := len(c.labelCalls); n == 0 || c.labelCalls[n-1].Outcome != bridgegithub.OutcomeWarning {
			t.Fatalf("want flux-warning label; got %+v", c.labelCalls)
		}
	}
	base := corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}

	// Via the Ready condition on a non-error event.
	assertWarning(t, run(t, kustomizationWaitingOnDependency, eventv1.Event{InvolvedObject: base}))

	// Via an error-severity DependencyNotReady event (healthy fetched status).
	assertWarning(t, run(t, kustomizationOneInventoryEntry, eventv1.Event{
		InvolvedObject: base,
		Severity:       eventv1.EventSeverityError,
		Reason:         "DependencyNotReady",
		Message:        "dependency 'flux-system/cluster-base' is not ready",
	}))

	// A message-less DependencyNotReady error event must get WAITING wording,
	// not the "reconciliation failed" placeholder (Copilot review).
	c := run(t, kustomizationOneInventoryEntry, eventv1.Event{
		InvolvedObject: base,
		Severity:       eventv1.EventSeverityError,
		Reason:         "DependencyNotReady",
	})
	body := c.posts[0].Body
	if !strings.Contains(body, statusEmojiWarning) || strings.Contains(body, "reconciliation failed") {
		t.Errorf("empty-message dependency wait must not carry the failure placeholder; got %q", body)
	}
	if !strings.Contains(body, "waiting on a dependency: a dependsOn target is not ready (DependencyNotReady)") {
		t.Errorf("empty-message dependency wait must use the waiting default; got %q", body)
	}
}

// Two PRs share a Kustomization, one with a spent re-eval budget (observed on
// In practice: the exhausted PR's comment froze on a stale verdict while its sibling
// showed the current one). The cap bounds ANALYSIS cost only: when the
// sibling triggers the analysis anyway, the already-built body must also be
// delivered to the exhausted PR. Alone, the exhausted PR still triggers
// nothing.
func TestProcess_ExhaustedPR_PiggybacksOnSiblingAnalysis(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{
		prs: []bridgegithub.PRInfo{{Number: 42, State: "open", HeadSHA: "h-old"}},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: false,
		DedupePRReEvalMax:      1,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, Message: "still broken",
	}

	// Spend PR 42's budget: initial + 1 failing re-eval, then an exhausted round that
	// must NOT analyse (no sibling to ride on).
	for i := range 3 {
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process #%d: %v", i+1, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	if got := postsTo(commenterMock.posts, 42); got != 2 {
		t.Fatalf("exhausted PR alone: want 2 posts (initial + 1 re-eval), got %d", got)
	}

	// A sibling PR with a fresh budget appears: its analysis runs, and the
	// exhausted PR receives the same body as a piggyback delivery.
	commenterMock.prs = append(commenterMock.prs, bridgegithub.PRInfo{Number: 50, State: "open", HeadSHA: "h-new"})
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process with sibling: %v", err)
	}
	if got := postsTo(commenterMock.posts, 50); got != 1 {
		t.Fatalf("sibling must get its first post, got %d", got)
	}
	if got := postsTo(commenterMock.posts, 42); got != 3 {
		t.Fatalf("exhausted PR must piggyback on the sibling's analysis, got %d posts", got)
	}
}

func postsTo(posts []postRecord, pr int) int {
	n := 0
	for _, p := range posts {
		if p.Number == pr {
			n++
		}
	}
	return n
}

// Regression for degraded comments outliving an LLM outage (Copilot review):
// a kstatus-only fallback comment keyed under the normal stability key made
// CommentCurrent report true on every later re-eval of the unchanged state —
// the LLM was never retried and "analysis unavailable" persisted forever.
// The degraded key forces the retry; recovery swaps in the full comment, and
// a sustained outage still doesn't churn (same degraded key each window).
func TestProcess_LLMOutageComment_SelfHealsOnRecovery(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{
		err:    errors.New("provider down"),
		result: "## Kustomization Status\nbroken\n## Root Cause\nproof\n## Next steps to fix\n1. fix",
	}
	commenterMock := &fakeCommenter{
		prNumber: 42, prState: "open", prHeadSHA: "h1",
		stabilityKeys: map[int]string{}, // opt into currency tracking
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude", CommentOnSuccess: true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: true,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	failing := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, Message: "boom",
	}

	// Outage: the comment lands kstatus-only with the fallback note.
	if err := proc.Process(context.Background(), failing, "srcrev1"); err != nil {
		t.Fatalf("outage Process: %v", err)
	}
	if n := len(commenterMock.posts); n != 1 || !strings.Contains(commenterMock.posts[0].Body, "LLM analysis unavailable") {
		t.Fatalf("outage must post the fallback comment; posts=%d", n)
	}
	time.Sleep(3 * time.Millisecond)

	// Recovery, same cluster state: the degraded key must force an LLM retry
	// and swap in the full narrative.
	analyzerMock.err = nil
	if err := proc.Process(context.Background(), failing, "srcrev2"); err != nil {
		t.Fatalf("recovery Process: %v", err)
	}
	if n := len(commenterMock.posts); n != 2 {
		t.Fatalf("recovery must repost the full comment, got %d posts", n)
	}
	if body := commenterMock.posts[1].Body; strings.Contains(body, "LLM analysis unavailable") || !strings.Contains(body, "## Root Cause") {
		t.Errorf("recovered comment must carry the narrative, not the fallback; got %q", body)
	}
	time.Sleep(3 * time.Millisecond)

	// Steady state after recovery: unchanged state is current again — no
	// further LLM calls or reposts.
	callsBefore := analyzerMock.calls
	if err := proc.Process(context.Background(), failing, "srcrev3"); err != nil {
		t.Fatalf("steady Process: %v", err)
	}
	if len(commenterMock.posts) != 2 || analyzerMock.calls != callsBefore {
		t.Fatalf("unchanged state after recovery must skip LLM and repost; posts=%d calls=%d→%d", len(commenterMock.posts), callsBefore, analyzerMock.calls)
	}
}

// A deterministic PRESENTATION change (here: toggling the footer) must alter
// the stability key so existing comments are refreshed — keying on the
// unformatted body would let the embedded key match forever while the
// rendered comment differs (Copilot review).
func TestProcess_PresentationChange_AltersStabilityKey(t *testing.T) {
	newProc := func(footer bool, commenter *fakeCommenter, dd *dedupe.Cache) *Processor {
		mcpMock := &fakeMCPClient{
			data:      []byte(kustomizationOneInventoryEntry),
			resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
		}
		cfg := &config.Config{
			GitHubRepo: "owner/repo", LLMProvider: "none", CommentOnSuccess: true,
			CommentIncludeFooter:   footer,
			DedupePRReEvalTTL:      time.Millisecond,
			DedupeReEvalFailedOnly: false,
			ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		}
		return New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenter, nil, dd, slog.Default())
	}
	commenterMock := &fakeCommenter{prNumber: 42, prState: "open", prHeadSHA: "h1", stabilityKeys: map[int]string{}}
	dd := dedupe.NewNoExpiry(16)
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	if err := newProc(true, commenterMock, dd).Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	// Same state, same presentation → current → no repost.
	if err := newProc(true, commenterMock, dd).Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("unchanged presentation must not repost, got %d", len(commenterMock.posts))
	}
	time.Sleep(3 * time.Millisecond)
	// Footer toggled: the deterministic OUTPUT changed → key changes → repost.
	if err := newProc(false, commenterMock, dd).Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("third Process: %v", err)
	}
	if len(commenterMock.posts) != 2 {
		t.Fatalf("presentation change must refresh the comment, got %d posts", len(commenterMock.posts))
	}
}

// A pipeline error must roll back PIGGYBACK admissions too (Copilot review):
// the piggyback PR's delivery happens at the end of the failed run, so a
// standing mark would block re-admission until the TTL expires and the PR
// would miss the sibling's retry, staying stale a full extra window.
func TestProcess_PiggybackMarkRolledBackOnPipelineError(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	commenterMock := &fakeCommenter{
		prs: []bridgegithub.PRInfo{{Number: 42, State: "open", HeadSHA: "h-old"}},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "none",
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Minute, // long: re-admission must come from ROLLBACK, not expiry
		DedupeReEvalFailedOnly: false,
		DedupePRReEvalMax:      1,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	dd := dedupe.New(time.Minute, 16) // expirable backend so the per-call floor works
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, Message: "still broken",
	}

	// Exhaust PR 42's budget (initial + 1 failing re-eval). The floor is long,
	// so re-admissions below need explicit ForgetPR rollbacks.
	for i := range 2 {
		dd.ForgetPR("default", "apps", 42, "h-old") // simulate elapsed floor between rounds
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process #%d: %v", i+1, err)
		}
	}
	if got := postsTo(commenterMock.posts, 42); got != 2 {
		t.Fatalf("setup: want 2 posts, got %d", got)
	}

	// A sibling arrives; PR 42 is exhausted → piggyback. The delivery FAILS,
	// so the whole pipeline errors — BOTH marks must roll back.
	commenterMock.prs = append(commenterMock.prs, bridgegithub.PRInfo{Number: 50, State: "open", HeadSHA: "h-new"})
	dd.ForgetPR("default", "apps", 42, "h-old")
	commenterMock.postErr = errors.New("github down")
	if err := proc.Process(context.Background(), event, "srcrev"); err == nil {
		t.Fatal("failed delivery must surface an error")
	}

	// Recovery: WITHOUT rollback, PR 42's standing mark would block its
	// re-admission (floor is 1m) and only the sibling would be delivered.
	// (The fake records even the failed attempt, so assert deltas.)
	commenterMock.postErr = nil
	before42, before50 := postsTo(commenterMock.posts, 42), postsTo(commenterMock.posts, 50)
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("recovery Process: %v", err)
	}
	if got := postsTo(commenterMock.posts, 50); got != before50+1 {
		t.Fatalf("sibling must deliver on recovery, got %d (was %d)", got, before50)
	}
	if got := postsTo(commenterMock.posts, 42); got != before42+1 {
		t.Fatalf("piggyback PR must be re-admitted after the rollback and delivered, got %d (was %d)", got, before42)
	}
}

// Regression for a CronJob-removal PR: it earned ✅/flux-success when its
// deletion was applied, then the NEXT event de-scoped it — the pruned
// resource no longer matched the inventory, so the bridge erased the comment
// and labels one window after awarding them. A removed manifest whose
// resources are ABSENT from the inventory is the deletion succeeding: with
// the scope in the PR's roster the PR stays attributed; without roster
// evidence it isn't attributed, but the destructive cleanup is still skipped.
func TestProcess_DeletionPR_NotDeScopedByItsOwnSuccess(t *testing.T) {
	// Inventory after the prune: only `other` remains — the PR's deleted
	// CronJob is gone.
	const postPruneInventory = `
status:
  inventory:
    entries:
      - id: "default_other__ConfigMap"
`
	deletedCronJobYAML := []byte("apiVersion: batch/v1\nkind: CronJob\nmetadata:\n  name: cleanup\n  namespace: default\n")
	newSetup := func(withRoster bool) (*Processor, *fakeCommenter, *dedupe.Cache) {
		mcpMock := &fakeMCPClient{
			data:      []byte(postPruneInventory),
			resources: map[string][]byte{"v1|ConfigMap|other|default": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n  namespace: default\n")},
		}
		commenterMock := &fakeCommenter{
			prNumber: 42, prState: "open", prHeadSHA: "h1",
			// The PR's lone change is the REMOVED CronJob manifest.
			prFiles: []bridgegithub.PRFile{{Path: "cleanup-cj.yaml", BlobSHA: "b1", Status: "removed"}},
			blobs:   map[string][]byte{"b1": deletedCronJobYAML},
		}
		dd := dedupe.NewNoExpiry(16)
		if withRoster {
			dd.AddPRScope("owner", "repo", 42, "default/apps", time.Hour)
		}
		cfg := &config.Config{
			GitHubRepo: "owner/repo", LLMProvider: "none", PRLabelEnabled: true,
			CommentIncludeSummary: true, CommentOnSuccess: true,
			ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
		}
		return New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil, commenterMock, nil, dd, slog.Default()), commenterMock, dd
	}
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"}}

	// With roster evidence: the PR stays attributed — it gets the (healthy)
	// verdict, no comment removal, no label clearing.
	proc, commenterMock, dd := newSetup(true)
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(commenterMock.posts) != 1 {
		t.Fatalf("deletion PR must stay attributed and receive the verdict, got %d posts", len(commenterMock.posts))
	}
	if len(commenterMock.removeCalls) != 0 {
		t.Fatalf("deletion success must not remove the comment, got %+v", commenterMock.removeCalls)
	}
	if scopes := dd.PRScopes("owner", "repo", 42); len(scopes) == 0 {
		t.Fatal("roster must survive — the PR still owns this scope")
	}

	// Without roster evidence: no attribution (no post), but the destructive
	// cleanup must still be skipped.
	proc2, commenter2, _ := newSetup(false)
	if err := proc2.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("Process (no roster): %v", err)
	}
	if len(commenter2.posts) != 0 {
		t.Fatalf("without roster evidence the PR must not be attributed, got %d posts", len(commenter2.posts))
	}
	if len(commenter2.removeCalls) != 0 {
		t.Fatalf("absence-by-deletion must never trigger cleanup, got %+v", commenter2.removeCalls)
	}
}

// A STALE piggyback comment must block the LLM skip (Copilot review): with
// only toPost PRs voting, the stale piggyback PR was "healed" with a
// kstatus-only body keyed as current — permanently dropping the narrative for
// this state. The healing delivery must carry the narrative, at the cost of
// one LLM call; the next window skips again.
func TestProcess_StalePiggybackComment_BlocksLLMSkip(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data:      []byte(kustomizationOneInventoryEntry),
		resources: map[string][]byte{"v1|ConfigMap|any|default": []byte(healthyConfigMapYAML)},
	}
	analyzerMock := &fakeAnalyzer{result: "## Kustomization Status\nbroken\n## Root Cause\nproof\n## Next steps to fix\n1. fix"}
	commenterMock := &fakeCommenter{
		prs:           []bridgegithub.PRInfo{{Number: 42, State: "open", HeadSHA: "h-old"}},
		stabilityKeys: map[int]string{},
	}
	cfg := &config.Config{
		GitHubRepo: "owner/repo", LLMProvider: "claude",
		CommentOnSuccess:       true,
		DedupePRReEvalTTL:      time.Millisecond,
		DedupeReEvalFailedOnly: false,
		DedupePRReEvalMax:      1,
		ReadinessPollInterval:  time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), analyzerMock, commenterMock, nil, dedupe.NewNoExpiry(16), slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityError, Message: "still broken",
	}

	// Exhaust PR 42 (initial + 1 failing re-eval), then add a fresh sibling so
	// 42 piggybacks from here on.
	for i := range 2 {
		if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
			t.Fatalf("Process #%d: %v", i+1, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
	commenterMock.prs = append(commenterMock.prs, bridgegithub.PRInfo{Number: 50, State: "open", HeadSHA: "h-new"})
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("sibling Process: %v", err)
	}
	time.Sleep(3 * time.Millisecond)

	// PR 42's comment goes stale (e.g. a missed delivery) while the sibling's
	// stays current: the healing delivery must include the narrative. (The
	// sibling's budget is intact — its previous round was the uncounted
	// initial analysis — so it still triggers the analysis 42 rides on.)
	delete(commenterMock.stabilityKeys, 42)
	calls := analyzerMock.calls
	if err := proc.Process(context.Background(), event, "srcrev"); err != nil {
		t.Fatalf("healing Process: %v", err)
	}
	if analyzerMock.calls != calls+1 {
		t.Fatalf("stale piggyback comment must block the LLM skip; calls %d→%d", calls, analyzerMock.calls)
	}
	healed := commenterMock.posts[len(commenterMock.posts)-1]
	if healed.Number != 42 || !strings.Contains(healed.Body, "## Root Cause") {
		t.Fatalf("healing delivery must target PR 42 with the narrative; got %+v", healed.Number)
	}
}
