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

//go:build e2e

package e2e

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	bridgePort = "18080"
	githubPort = "18081"
	llmPort    = "18082"
	bridgeURL  = "http://localhost:" + bridgePort
	githubURL  = "http://localhost:" + githubPort
	llmURL     = "http://localhost:" + llmPort
	namespace  = "flux-system"
	mockNS     = "mock"
	context    = "kind-flux-ai-bridge-e2e"
)

// realGitHub is true when the suite is running against a real GitHub repo.
// Set E2E_REAL_GITHUB=1 plus GITHUB_TOKEN, GITHUB_REPO. The test
// auto-discovers the open PR in the configured repo; only set
// GITHUB_PR_NUMBER + GITHUB_PR_SHA when you have multiple open PRs and need
// to pin a specific one. See docs/e2e-real-github.md.
var realGitHub = os.Getenv("E2E_REAL_GITHUB") == "1"

// realLLM is true when the suite is running against a real LLM. The Makefile
// accepts any non-empty value (typically "1", "ollama", or "azure" — each
// drives a different LLM install path). See docs/e2e-real-llm.md.
//
// "none" is treated as "no LLM at all" rather than "real LLM" — the bridge
// posts a deterministic kstatus comment without contacting any model.
// noLLM gates that branch so assertions skip the mock-llm dump check.
var realLLM = os.Getenv("E2E_LLM") != "" && os.Getenv("E2E_LLM") != "none"
var noLLM = os.Getenv("E2E_LLM") == "none"

// expectBroken is set by `make e2e-broken`, which crash-loops podinfo before
// running the suite: the delivered comment must then report the failure.
var expectBroken = os.Getenv("E2E_EXPECT_BROKEN") == "1"

// podinfoBranch returns the branch label the test fakes onto synthetic Flux
// event payloads, matching the GitRepository ref the cluster's Kustomization
// actually tracks (E2E_PODINFO_GIT_REF). The bridge only uses the SHA half of
// a revision string, so this label is cosmetic — but a wrong label produces
// confusing log lines like `main@sha1:…` when the cluster is actually
// tracking `dev`. Defaults to "main" when unset (matches the Makefile default).
func podinfoBranch() string {
	if ref := os.Getenv("E2E_PODINFO_GIT_REF"); ref != "" {
		return ref
	}
	return "main"
}

var portForwards []*exec.Cmd

func TestMain(m *testing.M) {
	// Bridge lives in flux-system; mock servers live in their own
	// `mock` namespace (see e2e/manifests/02-mock-{common,llm,github}.yaml).
	type forward struct{ ns, mapping string }
	forwards := map[string]forward{
		"svc/flux-ai-bridge": {namespace, bridgePort + ":8080"},
	}
	// mock-github only matters in mock mode (real mode hits api.github.com).
	if !realGitHub {
		forwards["svc/mock-github"] = forward{mockNS, githubPort + ":80"}
	}
	// mock-llm only matters in mock mode. Real mode hits the configured
	// model directly; noLLM mode skips LLM entirely so there's no mock to
	// port-forward.
	if !realLLM && !noLLM {
		forwards["svc/mock-llm"] = forward{mockNS, llmPort + ":80"}
	}
	for target, f := range forwards {
		cmd := exec.Command(
			"kubectl", "port-forward",
			"-n", f.ns,
			target,
			f.mapping,
			"--context", context,
		)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "port-forward %s: %v\n", target, err)
			killAll(portForwards)
			os.Exit(1)
		}
		portForwards = append(portForwards, cmd)
	}

	if !waitHealthy(45 * time.Second) {
		killAll(portForwards)
		fmt.Fprintln(os.Stderr, "bridge did not become healthy within 45s")
		os.Exit(1)
	}

	code := m.Run()
	killAll(portForwards)
	os.Exit(code)
}

func killAll(cmds []*exec.Cmd) {
	for _, c := range cmds {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}
}

func waitHealthy(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(bridgeURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func TestHealthz(t *testing.T) {
	resp, err := http.Get(bridgeURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("expected Content-Type application/json, got %q", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected {\"status\":\"ok\"}, got %v", body)
	}
}

func TestWebhookKustomizationAccepted(t *testing.T) {
	// Skipped in real-GitHub mode: the bridge's webhook handler accepts
	// (202) before spawning the async pipeline goroutine, so the synthetic
	// `aaaa…` SHA below would leak into the user's real repo as an error
	// log when the goroutine tries to look it up via api.github.com.
	// Acceptance behavior is implicitly covered by TestPipelineProducesRealGitHubComment
	// which uses a real SHA and asserts the same 202 prerequisite.
	if realGitHub {
		t.Skip("E2E_REAL_GITHUB=1 — synthetic SHA would error against the real GitHub API; webhook-acceptance assertion is covered by TestPipelineProducesRealGitHubComment")
	}
	payload := kustomizationEvent("podinfo", "flux-system", "ReconciliationSucceeded",
		"Reconciliation finished in 5s",
		podinfoBranch()+"@sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	resp := postWebhook(t, payload)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}
}

func TestWebhookKustomizationWithoutRevisionSkipped(t *testing.T) {
	// No `kustomize.toolkit.fluxcd.io/revision` metadata → bridge skips with 204.
	payload := `{
		"involvedObject": {
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind": "Kustomization",
			"name": "podinfo",
			"namespace": "flux-system"
		},
		"severity": "info",
		"reason": "ReconciliationSucceeded",
		"message": "Reconciliation finished",
		"metadata": {}
	}`

	resp := postWebhook(t, payload)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", resp.StatusCode)
	}
}

func TestWebhookNonKustomizationIgnored(t *testing.T) {
	payload := `{
		"involvedObject": {
			"apiVersion": "helm.toolkit.fluxcd.io/v2",
			"kind": "HelmRelease",
			"name": "my-release",
			"namespace": "flux-system"
		},
		"severity": "info",
		"timestamp": "2026-05-08T12:00:00Z",
		"message": "Release reconciliation succeeded",
		"reason": "ReconciliationSucceeded",
		"reportingController": "helm-controller",
		"reportingInstance": "helm-controller-abc"
	}`

	resp := postWebhook(t, payload)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", resp.StatusCode)
	}
}

func TestWebhookInvalidJSONRejected(t *testing.T) {
	resp := postWebhook(t, `{not valid json}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", resp.StatusCode)
	}
}

// TestPipelineProducesGitHubComment drives the full pipeline against the
// bundled flux-operator-mcp sub-chart (enabled via the bridge HelmRelease)
// and the real `podinfo` Flux Kustomization installed by `make e2e-up`. It
// asserts that the bridge:
//
//  1. Calls the (mock) LLM with a prompt derived from the Kustomization status.
//  2. Calls the (mock) GitHub API to look up the PR for the commit SHA.
//  3. Posts a comment containing the bridge's idempotency marker and the LLM
//     mock's stock response body.
func TestPipelineProducesGitHubComment(t *testing.T) {
	if realGitHub {
		t.Skip("E2E_REAL_GITHUB=1 — running TestPipelineProducesRealGitHubComment instead")
	}
	resetMocks(t)
	// A PR number no earlier run commented on: the bridge doesn't re-post a
	// comment whose state is unchanged, and the cluster's own reconcile
	// events also comment on the default PR #42 — so on #42 this test could
	// find its comment already current and see no POST at all.
	pr := 1000 + int(time.Now().UnixNano()%1_000_000)
	setScenario(t, fmt.Sprintf("pr:%d", pr))

	// Self-isolating: suspend podinfo and pin a unique applied revision so the
	// revision-applied gate passes and the cluster's own reconciles can't dedupe
	// or cache-lock this revision (which would make the test flaky in the suite).
	rev := isolateRevision(t, "podinfo")
	payload := kustomizationEvent("podinfo", "flux-system",
		"ReconciliationSucceeded",
		"Applied revision: "+rev,
		rev)

	resp := postWebhook(t, payload)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	// Wait for the async pipeline to land a POST on the (mock) GitHub.
	// The bridge talks to the mock-github via go-github's Enterprise client,
	// which prefixes paths with /api/v3/.
	expectedPath := fmt.Sprintf("/api/v3/repos/test-owner/test-repo/issues/%d/comments", pr)
	deadline := time.Now().Add(pipelineDeadline())
	var match map[string]any
	for time.Now().Before(deadline) {
		for _, req := range fetchDump(t, githubURL+"/_dump") {
			if req["method"] == "POST" && req["path"] == expectedPath {
				match = req
				break
			}
		}
		if match != nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if match == nil {
		t.Fatalf("bridge did not POST a PR comment to %s within %s", expectedPath, pipelineDeadline())
	}

	body, _ := match["body"].(string)
	assertCommentBody(t, body)
	if expectBroken && !strings.Contains(body, "❌ Flux Status") {
		t.Errorf("podinfo is crash-looping (make e2e-broken) but the comment isn't ❌: %s", body)
	}
	if !realLLM && !noLLM {
		// mock-llm captures the bridge's prompt; verify it was actually called
		// and contained the kustomization context.
		llmCalls := fetchDump(t, llmURL+"/_dump")
		if len(llmCalls) == 0 {
			t.Fatalf("expected at least one call to mock-llm, got 0")
		}
		llmBody, _ := llmCalls[len(llmCalls)-1]["body"].(string)
		if !strings.Contains(llmBody, "Flux Event") || !strings.Contains(llmBody, "podinfo") {
			t.Errorf("LLM prompt missing expected content; got: %s", llmBody)
		}
	}
}

// assertCommentBody validates the bridge's PR comment. The mock-llm path lets
// us assert the exact stock string. The real-LLM path can only assert
// structural properties — actual content varies per model and per run.
// The no-LLM path checks for the deterministic kstatus table + the canonical
// upstream link the bridge embeds in every no-LLM comment.
func assertCommentBody(t *testing.T, body string) {
	t.Helper()
	// Bridge now emits per-Kustomization markers (`<!-- flux-ai-bridge:<ns>/<name> -->`)
	// so the bare prefix is the substring we can rely on.
	if !strings.Contains(body, "<!-- flux-ai-bridge") {
		t.Errorf("comment body missing idempotency marker: %s", body)
	}
	if noLLM {
		// LLM disabled: the comment is the deterministic kstatus body.
		// Assert the canonical structure rather than free-text content.
		if !strings.Contains(body, "## Resource Status") {
			t.Errorf("no-LLM comment missing Resource Status table: %s", body)
		}
		if !strings.Contains(body, "kubernetes-sigs/cli-utils/tree/master/pkg/kstatus") {
			t.Errorf("no-LLM comment missing kstatus README link: %s", body)
		}
		// Workload Status section is gated by READINESS_INCLUDE_PODS
		// (default true). The e2e fixture leaves the default on; expect the
		// section (renamed from "Pod Status" once the per-pod Controller
		// column landed — see internal/processor/processor.go).
		if !strings.Contains(body, "## Workload Status") {
			t.Errorf("no-LLM comment missing Workload Status section: %s", body)
		}
		return
	}
	if realLLM {
		// Loose checks: must be non-trivial markdown that references the workload.
		if len(body) < 80 {
			t.Errorf("comment body suspiciously short (%d bytes): %s", len(body), body)
		}
		if !strings.Contains(strings.ToLower(body), "podinfo") &&
			!strings.Contains(strings.ToLower(body), "kustomization") {
			t.Errorf("comment body doesn't mention the workload or kustomization: %s", body)
		}
		return
	}
	// Mock-LLM path: stock response is deterministic.
	if !strings.Contains(body, "All resources ready (E2E mock)") {
		t.Errorf("comment body missing LLM mock response: %s", body)
	}
}

// TestPipelineProducesRealGitHubComment verifies the bridge comments on the
// configured PR for its selected head, against the real GitHub API and the
// real Flux reconcile of the PR branch. The cluster's Kustomization tracks
// `E2E_PODINFO_GIT_REF`, which must deploy the PR head (the PR branch, or a
// branch that merges it).
//
// Neither the comment nor the labels name the head, and the bridge writes
// nothing for a head it already analysed or a state its comment already
// shows. So the test controls the analysis: once the cluster applied the
// head, it takes the App's marker comments as the baseline and deletes them,
// clears the bridge's dedupe and comment state for the head (restart, plus
// the Valkey keys with persistence), and sends the Flux event for the applied
// revision. It then requires a marker comment from the configured App that
// is not in the baseline. This also makes repeat runs on the same cluster
// work.
//
// Required env: E2E_REAL_GITHUB=1, GITHUB_TOKEN (allowed to delete the
// App's comments on the PR), GITHUB_REPO. Optional: GITHUB_PR_NUMBER /
// GITHUB_PR_SHA (auto-discovered when unset), GITHUB_APP_ID (read from the
// bridge Secret when unset).
func TestPipelineProducesRealGitHubComment(t *testing.T) {
	if !realGitHub {
		t.Skip("E2E_REAL_GITHUB=1 not set; skipping real-GitHub pipeline test")
	}
	cfg := requireRealGitHubEnv(t)
	appID := bridgeAppID(t)

	// The analysis below must be of the selected head: wait until Flux
	// applied it (the head itself, or a merge commit with it as a parent).
	revision := waitAppliedHead(t, cfg)

	comments, err := listPRComments(cfg)
	if err != nil {
		t.Fatalf("list comments on PR #%d: %v", cfg.prNumber, err)
	}
	baseline := map[int64]bool{}
	for _, c := range comments {
		if id, ok := bridgeCommentID(c, appID); ok {
			baseline[id] = true
			if err := deleteComment(cfg, id); err != nil {
				t.Fatalf("delete the App's comment %d on PR #%d (GITHUB_TOKEN needs write access to the PR's comments): %v",
					id, cfg.prNumber, err)
			}
		}
	}
	resetBridgeHead(t, cfg)

	resp := postWebhook(t, kustomizationEvent("podinfo", namespace, "ReconciliationSucceeded",
		"Applied revision: "+revision, revision))
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	deadline := time.Now().Add(pipelineDeadline())
	var found map[string]any
	for found == nil && time.Now().Before(deadline) {
		comments, err := listPRComments(cfg)
		if err != nil {
			t.Logf("listPRComments: %v (will retry)", err)
		}
		for _, c := range comments {
			if id, ok := bridgeCommentID(c, appID); ok && !baseline[id] {
				found = c
				break
			}
		}
		if found == nil {
			time.Sleep(2 * time.Second)
		}
	}
	if found == nil {
		t.Fatalf("App %s posted no new bridge comment for head %s on PR #%d in %s within %s (%d earlier comment(s) deleted as the baseline)",
			appID, cfg.sha, cfg.prNumber, cfg.repo, pipelineDeadline(), len(baseline))
	}

	body, _ := found["body"].(string)
	assertCommentBody(t, body)
}

// bridgeAppID returns the GitHub App ID the bridge posts as: GITHUB_APP_ID,
// or the one in the bridge's credentials Secret.
func bridgeAppID(t *testing.T) string {
	t.Helper()
	if id := strings.TrimSpace(os.Getenv("GITHUB_APP_ID")); id != "" {
		return id
	}
	id := strings.TrimSpace(readBridgeSecretKey(t, "flux-ai-bridge-e2e-creds", "GITHUB_APP_ID"))
	if id == "" {
		t.Fatal("no GitHub App ID: set GITHUB_APP_ID, or check the flux-ai-bridge-e2e-creds Secret")
	}
	return id
}

// waitAppliedHead waits until the podinfo Kustomization applied the PR head
// cfg.sha, or a commit whose parents include it, and returns that revision.
func waitAppliedHead(t *testing.T, cfg realGHConfig) string {
	t.Helper()
	deadline := time.Now().Add(pipelineDeadline())
	for {
		revision := kubectlJSONPath(t, "kustomization", "podinfo", "{.status.lastAppliedRevision}")
		applied := revision[strings.LastIndex(revision, ":")+1:]
		if strings.EqualFold(applied, cfg.sha) {
			return revision
		}
		if applied != "" {
			parents, err := lookupCommitParents(cfg, applied)
			if err != nil {
				t.Logf("look up commit %s: %v (will retry)", applied, err)
			}
			for _, p := range parents {
				if strings.EqualFold(p, cfg.sha) {
					return revision
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("podinfo applied %q, which is neither PR #%d's head %s nor a merge of it, within %s: "+
				"point E2E_PODINFO_GIT_REF at the PR branch", revision, cfg.prNumber, cfg.sha, pipelineDeadline())
		}
		time.Sleep(2 * time.Second)
	}
}

// resetBridgeHead makes the bridge analyse PR cfg.prNumber at cfg.sha again
// and post its comment afresh: a restart clears the in-memory dedupe mark and
// comment cache, and with persistence the same records go from Valkey.
func resetBridgeHead(t *testing.T, cfg realGHConfig) {
	t.Helper()
	if persistenceEnabled(t) {
		valkeyExec(t, "DEL",
			prDedupeKey(namespace, "podinfo", cfg.prNumber, cfg.sha),
			fmt.Sprintf("flux-ai-bridge:fetchcache:last-comment:%s/%d/%s/podinfo", cfg.repo, cfg.prNumber, namespace))
	}
	rolloutRestartBridge(t)
	if err := rebuildPortForward(t); err != nil {
		t.Fatalf("port-forward after restart: %v", err)
	}
	if !waitHealthy(60 * time.Second) {
		t.Fatal("bridge did not become healthy within 60s of restart")
	}
}

type realGHConfig struct {
	token    string
	repo     string // owner/repo
	prNumber int
	sha      string
	apiURL   string // default https://api.github.com
}

func requireRealGitHubEnv(t *testing.T) realGHConfig {
	t.Helper()
	cfg := realGHConfig{
		token:  os.Getenv("GITHUB_TOKEN"),
		repo:   os.Getenv("GITHUB_REPO"),
		sha:    os.Getenv("GITHUB_PR_SHA"),
		apiURL: os.Getenv("GITHUB_API_URL"),
	}
	if cfg.apiURL == "" {
		cfg.apiURL = "https://api.github.com"
	}
	// The helpers send GITHUB_TOKEN on every request: never in cleartext.
	if u, err := url.Parse(cfg.apiURL); err != nil || u.Scheme != "https" || u.Host == "" {
		t.Fatalf("GITHUB_API_URL must be an https:// URL (the token is sent with every request), got %q", cfg.apiURL)
	}
	if cfg.token == "" {
		t.Fatal("GITHUB_TOKEN is required when E2E_REAL_GITHUB=1")
	}
	if cfg.repo == "" || !strings.Contains(cfg.repo, "/") {
		t.Fatal("GITHUB_REPO=owner/repo is required when E2E_REAL_GITHUB=1")
	}

	// PR number + SHA are optional env overrides. Three discovery paths:
	//   - both set: use as-is.
	//   - number set, SHA empty: fetch GET /pulls/<N> for the head SHA.
	//   - both empty: list open PRs; the single open PR's number + head SHA.
	//
	// Keeps single-sandbox-repo setups (the common case) friction-free:
	// create one PR, leave it open, never touch the env again.
	prRaw := os.Getenv("GITHUB_PR_NUMBER")
	if prRaw != "" {
		pr, err := strconv.Atoi(prRaw)
		if err != nil || pr <= 0 {
			t.Fatalf("GITHUB_PR_NUMBER must be a positive integer when set, got %q", prRaw)
		}
		cfg.prNumber = pr
	}
	switch {
	case cfg.prNumber != 0 && cfg.sha != "":
		// both pinned, nothing to do
	case cfg.prNumber != 0 && cfg.sha == "":
		// resolve SHA from pinned PR
		sha, err := lookupPRHeadSHA(cfg, cfg.prNumber)
		if err != nil {
			t.Fatalf("look up head SHA for PR #%d in %s: %v", cfg.prNumber, cfg.repo, err)
		}
		cfg.sha = sha
	default:
		// both empty → auto-discover
		discovered, err := discoverOpenPR(cfg)
		if err != nil {
			t.Fatalf("auto-discover open PR in %s: %v\n"+
				"Set GITHUB_PR_NUMBER (and optionally GITHUB_PR_SHA) to pin a "+
				"specific PR, or open exactly one PR in the sandbox repo.", cfg.repo, err)
		}
		cfg.prNumber = discovered.prNumber
		cfg.sha = discovered.sha
	}
	return cfg
}

// githubClient sends the requests that carry GITHUB_TOKEN. It follows a
// redirect only within the https origin it started on: net/http keeps the
// Authorization header on a redirect to the same host, even from https to
// http.
var githubClient = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if req.URL.Scheme != "https" || req.URL.Host != via[0].URL.Host {
			return fmt.Errorf("refusing redirect to %s: the request carries GITHUB_TOKEN", req.URL.Redacted())
		}
		return nil
	},
}

// lookupCommitParents returns the parent SHAs of commit sha via
// GET /commits/{sha}.
func lookupCommitParents(cfg realGHConfig, sha string) ([]string, error) {
	url := fmt.Sprintf("%s/repos/%s/commits/%s", cfg.apiURL, cfg.repo, sha)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+cfg.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := githubClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get commit: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub returned %s", resp.Status)
	}
	var commit struct {
		Parents []struct {
			Sha string `json:"sha"`
		} `json:"parents"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&commit); err != nil {
		return nil, fmt.Errorf("decode commit response: %w", err)
	}
	parents := make([]string, 0, len(commit.Parents))
	for _, p := range commit.Parents {
		parents = append(parents, p.Sha)
	}
	return parents, nil
}

// bridgeCommentID returns the ID of an issue comment when it carries the
// bridge marker and was posted by the GitHub App appID.
func bridgeCommentID(c map[string]any, appID string) (int64, bool) {
	body, _ := c["body"].(string)
	if !strings.Contains(body, "<!-- flux-ai-bridge") {
		return 0, false
	}
	app, _ := c["performed_via_github_app"].(map[string]any)
	appNum, ok := app["id"].(float64)
	if !ok || strconv.FormatInt(int64(appNum), 10) != appID {
		return 0, false
	}
	id, ok := c["id"].(float64)
	return int64(id), ok
}

// deleteComment deletes issue comment id via DELETE /issues/comments/{id}. A
// comment that is already gone counts as deleted.
func deleteComment(cfg realGHConfig, id int64) error {
	url := fmt.Sprintf("%s/repos/%s/issues/comments/%d", cfg.apiURL, cfg.repo, id)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	req.Header.Set("Authorization", "Bearer "+cfg.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := githubClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("GitHub returned %s", resp.Status)
	}
	return nil
}

// lookupPRHeadSHA fetches a specific PR's head SHA via GET /pulls/{number}.
// Used when GITHUB_PR_NUMBER is pinned but GITHUB_PR_SHA is not.
func lookupPRHeadSHA(cfg realGHConfig, prNumber int) (string, error) {
	url := fmt.Sprintf("%s/repos/%s/pulls/%d", cfg.apiURL, cfg.repo, prNumber)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+cfg.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := githubClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("get PR: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GitHub returned %s", resp.Status)
	}
	var pr struct {
		Head struct {
			Sha string `json:"sha"`
		} `json:"head"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return "", fmt.Errorf("decode PR response: %w", err)
	}
	if pr.Head.Sha == "" {
		return "", fmt.Errorf("PR response has empty head.sha")
	}
	return pr.Head.Sha, nil
}

// discoverOpenPR queries GitHub for open PRs in the configured repo and
// returns the single open PR's number + head SHA. Errors when zero or more
// than one open PR exists — for the disambiguation case, the caller falls
// back to GITHUB_PR_NUMBER.
func discoverOpenPR(cfg realGHConfig) (realGHConfig, error) {
	url := fmt.Sprintf("%s/repos/%s/pulls?state=open&per_page=100", cfg.apiURL, cfg.repo)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+cfg.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := githubClient.Do(req)
	if err != nil {
		return realGHConfig{}, fmt.Errorf("list open PRs: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return realGHConfig{}, fmt.Errorf("list open PRs: GitHub returned %s", resp.Status)
	}
	var prs []struct {
		Number int `json:"number"`
		Head   struct {
			Sha string `json:"sha"`
		} `json:"head"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&prs); err != nil {
		return realGHConfig{}, fmt.Errorf("decode PRs response: %w", err)
	}
	switch len(prs) {
	case 0:
		return realGHConfig{}, fmt.Errorf("no open PRs found")
	case 1:
		return realGHConfig{prNumber: prs[0].Number, sha: prs[0].Head.Sha}, nil
	default:
		nums := make([]string, 0, len(prs))
		for _, p := range prs {
			nums = append(nums, strconv.Itoa(p.Number))
		}
		return realGHConfig{}, fmt.Errorf("%d open PRs found (#%s) — set GITHUB_PR_NUMBER to disambiguate",
			len(prs), strings.Join(nums, ", #"))
	}
}

func listPRComments(cfg realGHConfig) ([]map[string]any, error) {
	// Every page: a check that only saw the first one could miss a comment,
	// or see an older one move up after a deletion.
	var all []map[string]any
	for page := 1; ; page++ {
		url := fmt.Sprintf("%s/repos/%s/issues/%d/comments?per_page=100&page=%d",
			cfg.apiURL, cfg.repo, cfg.prNumber, page)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("Authorization", "Bearer "+cfg.token)
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		resp, err := githubClient.Do(req)
		if err != nil {
			return nil, err
		}
		var out []map[string]any
		if resp.StatusCode/100 != 2 {
			err = fmt.Errorf("status %d listing comments", resp.StatusCode)
		} else {
			err = json.NewDecoder(resp.Body).Decode(&out)
		}
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		all = append(all, out...)
		if len(out) < 100 {
			return all, nil
		}
	}
}

// pipelineDeadline gives the bridge time to finish MCP → LLM → GitHub. A real
// local LLM (small quantized model on kind) can take 10–30 s per inference,
// so we extend the wait significantly when E2E_LLM=ollama.
func pipelineDeadline() time.Duration {
	switch {
	case expectBroken:
		// A crash-looping workload never settles, so the bridge waits out
		// the whole readiness poll (READINESS_POLL_TIMEOUT, 3m) first.
		return 4 * time.Minute
	case realLLM:
		return 3 * time.Minute
	}
	return 60 * time.Second
}

func resetMocks(t *testing.T) {
	t.Helper()
	// /_reset clears the mock's in-memory request log without recycling the
	// pod (which would tear down our port-forward).
	var urls []string
	if !realGitHub {
		urls = append(urls, githubURL+"/_reset")
	}
	if !realLLM && !noLLM {
		urls = append(urls, llmURL+"/_reset")
	}
	for _, url := range urls {
		if err := postReset(url); err != nil {
			t.Fatalf("reset %s: %v", url, err)
		}
	}
}

func postReset(url string) error {
	req, _ := http.NewRequest(http.MethodPost, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func fetchDump(t *testing.T, url string) []map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var out []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Logf("decode dump %s: %v", url, err)
		return nil
	}
	return out
}

// e2eWebhookHMACToken mirrors flux.notification.hmac.token in
// manifests/03-flux-ai-bridge.yaml.tpl.
const e2eWebhookHMACToken = "e2e-webhook-hmac-token"

// postWebhook POSTs body to the bridge, signed the way Flux's generic-hmac
// Provider signs it (`X-Signature: sha256=<hex>`).
func postWebhook(t *testing.T, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, bridgeURL+"/webhook",
		bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	mac := hmac.New(sha256.New, []byte(e2eWebhookHMACToken))
	mac.Write([]byte(body))
	req.Header.Set("X-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /webhook: %v", err)
	}
	return resp
}

func kustomizationEvent(name, ns, reason, message, revision string) string {
	evt := map[string]any{
		"involvedObject": map[string]string{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"name":       name,
			"namespace":  ns,
		},
		"severity":  "info",
		"timestamp": "2026-05-08T12:00:00Z",
		"message":   message,
		"reason":    reason,
		"metadata": map[string]string{
			// Flux Notification Controller stamps the revision under the bare
			// "revision" key (the canonical eventv1.MetaRevisionKey). The
			// bridge skips events without it (returns 204).
			"revision": revision,
		},
		"reportingController": "kustomize-controller",
		"reportingInstance":   "kustomize-controller-abc",
	}
	b, _ := json.Marshal(evt)
	return string(b)
}

// injectPruneEvent creates a Kubernetes Event in the cluster mimicking the one
// kustomize-controller emits when it prunes a resource: involvedObject is the
// Kustomization, and the message is ssa's changeset line `Kind/ns/name deleted`.
// The bridge reads this via the Flux MCP to attribute a pure-deletion PR whose
// removed resource is no longer in the inventory (PR_ATTRIBUTE_PRUNES).
func injectPruneEvent(t *testing.T, ksName, message string) {
	t.Helper()
	name := fmt.Sprintf("e2e-prune-%d", time.Now().UnixNano())
	now := time.Now().UTC().Format("2006-01-02T15:04:05Z")
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Event
metadata:
  name: %s
  namespace: %s
type: Normal
reason: ReconciliationSucceeded
message: %q
involvedObject:
  apiVersion: kustomize.toolkit.fluxcd.io/v1
  kind: Kustomization
  name: %s
  namespace: %s
source:
  component: kustomize-controller
reportingComponent: kustomize-controller
reportingInstance: kustomize-controller-e2e
firstTimestamp: "%s"
lastTimestamp: "%s"
count: 1
`, name, namespace, message, ksName, namespace, now, now)

	cmd := exec.Command("kubectl", "--context", context, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("inject prune event: %v\n%s", err, out)
	}
}

// TestPipelineAttributesPrunedDeletion exercises prune-based attribution
// end-to-end: a pure-deletion PR whose only file removes a CronJob that is
// NOT in podinfo's inventory. The bridge can't match it by inventory membership;
// it must read the kustomize-controller prune event (injected below) and
// attribute the PR by what the reconcile deleted.
//
// Self-isolating: isolateRevision suspends podinfo and pins a UNIQUE applied
// revision, so the revision-applied gate passes while the cluster's own
// reconciles can't cache-lock FindPRsForCommit to #42 or dedupe the SHA — making
// the flip to the prune PR deterministic even within the full suite.
func TestPipelineAttributesPrunedDeletion(t *testing.T) {
	if realGitHub {
		t.Skip("E2E_REAL_GITHUB=1 — prune-attribution fixture is mock-GitHub only")
	}
	resetMocks(t)
	// PR lookups now return the removed-CronJob PR, under a number no earlier
	// run commented on (an unchanged state is never re-posted).
	pr := 2000 + int(time.Now().UnixNano()%1_000_000)
	setScenario(t, fmt.Sprintf("prune:%d", pr))
	rev := isolateRevision(t, "podinfo")

	// The prune event the bridge will read to attribute the prune PR.
	injectPruneEvent(t, "podinfo", "CronJob/podinfo/legacy-cron deleted")

	payload := kustomizationEvent("podinfo", "flux-system",
		"ReconciliationSucceeded",
		"Applied revision: "+rev,
		rev)

	resp := postWebhook(t, payload)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	expectedPath := fmt.Sprintf("/api/v3/repos/test-owner/test-repo/issues/%d/comments", pr)
	deadline := time.Now().Add(pipelineDeadline())
	for time.Now().Before(deadline) {
		for _, req := range fetchDump(t, githubURL+"/_dump") {
			if req["method"] == "POST" && req["path"] == expectedPath {
				return // attributed + commented — success
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("bridge did not attribute the pruned-deletion PR (no POST to %s within %s)", expectedPath, pipelineDeadline())
}

// TestPipelineDefersUnappliedRevision proves the revision-applied gate: an event
// for a revision the Kustomization has NOT applied (a synthetic SHA that never
// matches status.lastAppliedRevision) must be DEFERRED — the bridge logs the
// deferral and runs no analysis, so the kstatus settle budget never counts down
// against a stale snapshot.
func TestPipelineDefersUnappliedRevision(t *testing.T) {
	if realGitHub {
		t.Skip("E2E_REAL_GITHUB=1 — relies on bridge logs in the kind cluster")
	}
	resetMocks(t)

	ref := podinfoBranch()
	sha := fmt.Sprintf("%040x", time.Now().UnixNano()) // not podinfo's applied revision
	payload := kustomizationEvent("podinfo", "flux-system",
		"ReconciliationSucceeded",
		"Applied revision: "+ref+"@sha1:"+sha,
		ref+"@sha1:"+sha)

	resp := postWebhook(t, payload)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	// The bridge must log the deferral for this exact SHA. (Asserting "no comment"
	// would race the cluster's own real podinfo reconciles, which DO carry the
	// applied revision and legitimately comment — so we check the log instead.)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		out, _ := exec.Command("kubectl", "--context", context, "-n", namespace,
			"logs", "deploy/flux-ai-bridge", "--since=40s", "--tail=500").CombinedOutput()
		// One JSON log entry per line: the deferral entry itself must carry
		// this SHA, not just any entry next to some other deferral.
		for line := range strings.SplitSeq(string(out), "\n") {
			if strings.Contains(line, "deferring until") && strings.Contains(line, sha) {
				return // gate fired
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("bridge did not defer the unapplied revision %s within 20s", sha)
}

// isolateRevision makes a synthetic-webhook pipeline test deterministic under
// the revision-applied gate and the build-scope gate. It suspends the
// Kustomization (so the cluster's own reconciles don't compete) and pins a
// UNIQUE status.lastAppliedRevision, which it returns. The revision gate then
// passes (the event carries this exact revision) while the unique SHA can't be
// cache-locked by FindPRsForCommit or deduped by a background reconcile — so a
// scenario flip (e.g. to the prune PR) is reliable even within the full suite.
//
// The bridge only attributes a PR's present files when the source artifact
// is at the event's revision, so the Kustomization's GitRepository gets the
// same synthetic status.artifact.revision (suspended too, so source-controller
// doesn't restore it). The artifact tarball and its digest are unchanged.
// Both objects get their previous suspend state back on cleanup — under
// `make e2e-broken` podinfo is suspended on purpose, and resuming it would let
// kustomize-controller revert the broken state mid-suite.
func isolateRevision(t *testing.T, ksName string) string {
	t.Helper()
	rev := podinfoBranch() + "@sha1:" + fmt.Sprintf("%040x", time.Now().UnixNano())
	src := kubectlJSONPath(t, "kustomization", ksName, "{.spec.sourceRef.name}")
	waitSettled(t, ksName, src)
	suspendForTest(t, "kustomization", ksName)
	if src != "" {
		suspendForTest(t, "gitrepository", src)
	}
	// A reconcile already running when the suspend landed can still write
	// its own revision afterwards; re-apply until both stick.
	for attempt := 0; ; attempt++ {
		if src != "" {
			patchStatus(t, "gitrepository", src, fmt.Sprintf(`{"status":{"artifact":{"revision":%q}}}`, rev))
		}
		patchStatus(t, "kustomization", ksName, fmt.Sprintf(`{"status":{"lastAppliedRevision":%q}}`, rev))
		time.Sleep(time.Second)
		if kubectlJSONPath(t, "kustomization", ksName, "{.status.lastAppliedRevision}") == rev &&
			(src == "" || kubectlJSONPath(t, "gitrepository", src, "{.status.artifact.revision}") == rev) {
			return rev
		}
		if attempt == 10 {
			t.Fatalf("%s revision status kept being overwritten; could not pin %s", ksName, rev)
		}
	}
}

// waitSettled waits until neither the Kustomization nor its GitRepository has
// a reconcile pending. A previous test resuming them, or a reconcile request,
// starts one that would otherwise overwrite the revisions isolateRevision pins
// right after — a GitRepository restored to its real revision then fails the
// bridge's build-scope lookup for the pinned one. Suspended objects don't
// reconcile and count as settled.
func waitSettled(t *testing.T, ksName, src string) {
	t.Helper()
	const fields = `{.spec.suspend}|{.metadata.generation}|{.status.observedGeneration}|` +
		`{.metadata.annotations.reconcile\.fluxcd\.io/requestedAt}|{.status.lastHandledReconcileAt}`
	idle := func(kind, name string) (suspended, ok bool) {
		f := strings.Split(kubectlJSONPath(t, kind, name, fields), "|")
		if len(f) != 5 {
			t.Fatalf("read %s/%s reconcile state: %q", kind, name, f)
		}
		return f[0] == "true", f[0] == "true" || (f[1] == f[2] && f[3] == f[4])
	}
	deadline := time.Now().Add(time.Minute)
	for {
		ksSuspended, ok := idle("kustomization", ksName)
		if ok && src != "" {
			_, ok = idle("gitrepository", src)
			// A source revision change re-triggers the Kustomization without a
			// generation bump; it has caught up once it applied that revision.
			if ok && !ksSuspended {
				ok = kubectlJSONPath(t, "kustomization", ksName, "{.status.lastAppliedRevision}") ==
					kubectlJSONPath(t, "gitrepository", src, "{.status.artifact.revision}")
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s or its source still reconciling after 1m", ksName)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// suspendForTest suspends a Flux object and restores its previous
// spec.suspend on test cleanup.
func suspendForTest(t *testing.T, kind, name string) {
	t.Helper()
	was := kubectlJSONPath(t, kind, name, "{.spec.suspend}") == "true"
	patchSuspendKind(t, kind, name, true)
	t.Cleanup(func() { patchSuspendKind(t, kind, name, was) })
}

// kubectlJSONPath reads one field of a Flux object in the test namespace.
func kubectlJSONPath(t *testing.T, kind, name, path string) string {
	t.Helper()
	out, err := exec.Command("kubectl", "--context", context, "-n", namespace,
		"get", kind, name, "-o", "jsonpath="+path).Output()
	if err != nil {
		t.Fatalf("get %s/%s %s: %v", kind, name, path, err)
	}
	return strings.TrimSpace(string(out))
}

// patchStatus merge-patches the status subresource of a Flux object.
func patchStatus(t *testing.T, kind, name, patch string) {
	t.Helper()
	out, err := exec.Command("kubectl", "--context", context, "-n", namespace,
		"patch", kind, name, "--subresource=status", "--type=merge", "-p", patch).CombinedOutput()
	if err != nil {
		t.Fatalf("patch %s/%s status: %v\n%s", kind, name, err, out)
	}
}

// patchSuspendKind toggles spec.suspend on a Flux object.
func patchSuspendKind(t *testing.T, kind, name string, suspend bool) {
	t.Helper()
	out, err := exec.Command("kubectl", "--context", context, "-n", namespace,
		"patch", kind, name, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"suspend":%t}}`, suspend)).CombinedOutput()
	if err != nil {
		t.Fatalf("patch %s/%s spec.suspend=%t: %v\n%s", kind, name, suspend, err, out)
	}
}

// setScenario flips the mock-github PR-routing scenario ("default" | "prune").
func setScenario(t *testing.T, name string) {
	t.Helper()
	if realGitHub {
		return
	}
	req, _ := http.NewRequest(http.MethodPost, githubURL+"/_scenario", strings.NewReader(name))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("set mock scenario %q: %v", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("set mock scenario %q: status %d", name, resp.StatusCode)
	}
}
