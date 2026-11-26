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
	b64 "encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestPersistence_DedupeSurvivesBridgeRestart verifies the load-bearing
// property of the optional Valkey-backed dedupe: after a bridge restart
// (which wipes the in-memory state), a re-fired event for an already-analysed
// PR head is still skipped rather than re-running the readiness poll, LLM and
// GitHub delivery.
//
// Dedupe is per (Kustomization, PR, PR head commit) and happens inside the
// processor after the PR lookup, so the event must get that far:
// isolateRevision makes the Kustomization report the event's revision as
// applied (passing the revision-applied gate), and mock-github returns PR #42
// with the looked-up commit as its head — a unique dedupe key per run.
//
// Self-skips when persistence isn't enabled in the running cluster, so the
// test is safe to leave in the default e2e suite — the dedicated
// `make e2e-persistence` target is what turns Valkey on. The metrics read
// here are process counters (reset on pod restart), so the replay phase only
// compares post-restart values.
func TestPersistence_DedupeSurvivesBridgeRestart(t *testing.T) {
	if !persistenceEnabled(t) {
		t.Skip("persistence not enabled on the bridge pod; run with `make e2e-persistence`")
	}
	payload, key := analysePRHead(t, "persistence restart test")

	// Restart the bridge pod. The in-memory state is wiped; only the Valkey
	// entry remains.
	t.Logf("restart: rollout-restart bridge to wipe in-memory state")
	rolloutRestartBridge(t)
	if err := rebuildPortForward(t); err != nil {
		t.Fatalf("port-forward after restart: %v", err)
	}
	if !waitHealthy(60 * time.Second) {
		t.Fatal("bridge did not become healthy within 60s of restart")
	}
	if valkeyExec(t, "EXISTS", key) != "1" {
		t.Fatalf("dedupe mark %q gone after the bridge restart", key)
	}

	// Replay: same event, same PR head. Without persistence this would run
	// the pipeline again; with it, MarkPRIfNew finds the Valkey entry.
	t.Logf("replay: POST same event; expect the PR to be skipped as deduped")
	expectDedupedReplay(t, payload)
}

// analysePRHead runs one event through the full pipeline at a fresh revision
// and returns the payload plus the per-PR dedupe key it must leave in Valkey.
// It waits for the pipeline to finish successfully, because a failed run
// releases the mark again.
func analysePRHead(t *testing.T, message string) (payload, key string) {
	t.Helper()
	resetMocks(t)
	rev := isolateRevision(t, "podinfo")
	sha := rev[strings.LastIndex(rev, ":")+1:]
	payload = kustomizationEvent("podinfo", "flux-system", "ReconciliationSucceeded", message, rev)
	key = prDedupeKey("flux-system", "podinfo", 42, sha)
	t.Cleanup(func() { valkeyExec(t, "DEL", key) })

	successBefore := scrapeCounter(t, "flux_ai_bridge_events_processed_total", `outcome="success"`)
	resp := postWebhook(t, payload)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST: status %d, want 202", resp.StatusCode)
	}
	waitFor(t, pipelineDeadline(), "pipeline success", func() bool {
		return scrapeCounter(t, "flux_ai_bridge_events_processed_total", `outcome="success"`) > successBefore
	})
	if valkeyExec(t, "EXISTS", key) != "1" {
		t.Fatalf("dedupe mark %q not in Valkey after a successful analysis", key)
	}
	return payload, key
}

// expectDedupedReplay re-posts payload and waits for the PR to be counted as
// deduped — proof the bridge read the mark back from Valkey.
func expectDedupedReplay(t *testing.T, payload string) {
	t.Helper()
	before := scrapeCounter(t, "flux_ai_bridge_prs_skipped_total", `reason="deduped"`)
	resp := postWebhook(t, payload)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("replay POST: status %d, want 202", resp.StatusCode)
	}
	waitFor(t, 30*time.Second, "deduped skip on replay", func() bool {
		return scrapeCounter(t, "flux_ai_bridge_prs_skipped_total", `reason="deduped"`) > before
	})
}

// waitFor polls cond every 250ms until it holds or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// persistenceEnabled returns true when the running bridge Deployment has
// any dedupe persistence env wired up — either single-node
// (DEDUPE_PERSISTENCE_URL) OR sentinel mode (DEDUPE_PERSISTENCE_SENTINEL_ADDRS).
// The sentinel-mode path is what `make e2e-persistence` configures; we
// keep the URL check so the older single-node bundled-valkey path still
// works for any code that calls in by hand.
func persistenceEnabled(t *testing.T) bool {
	t.Helper()
	for _, envName := range []string{"DEDUPE_PERSISTENCE_URL", "DEDUPE_PERSISTENCE_SENTINEL_ADDRS"} {
		cmd := exec.Command("kubectl", "--context", context, "-n", namespace,
			"get", "deploy/flux-ai-bridge",
			"-o", fmt.Sprintf("jsonpath={.spec.template.spec.containers[0].env[?(@.name=='%s')].value}", envName))
		out, err := cmd.Output()
		if err != nil {
			t.Logf("persistenceEnabled (%s): %v", envName, err)
			continue
		}
		if strings.TrimSpace(string(out)) != "" {
			return true
		}
	}
	return false
}

// valkeyExec runs `redis-cli` inside the valkey-node-0 pod with the
// chart-managed password sourced from the `valkey` Secret. Returns
// stdout (trimmed). The test calls this to assert what the bridge
// actually wrote/deleted through the sentinel connection — observing
// the real datastore rather than only the bridge's outward signals.
//
// Routes via sentinel by talking to the master directly on port 6379
// (Bitnami valkey sentinel mode exposes both ports on the pod). We
// don't go through sentinel here because the test is doing an
// admin/assertion read, not exercising the failover path.
func valkeyExec(t *testing.T, cmdArgs ...string) string {
	t.Helper()
	args := []string{
		"--context", context,
		"-n", "valkey",
		"exec", "valkey-node-0",
		"-c", "valkey",
		"--", "valkey-cli",
		"-a", valkeyPassword(t),
		"--no-auth-warning",
	}
	args = append(args, cmdArgs...)
	cmd := exec.Command("kubectl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl exec valkey-cli %v: %v\n%s", cmdArgs, err, out)
	}
	return strings.TrimSpace(string(out))
}

// valkeyPassword reads the chart-managed admin password (default user)
// from the `valkey` namespace's `valkey` Secret. The test's direct
// kubectl-exec reads against Valkey/Sentinel use the admin user — the
// ACL users the bridge runs as have `+@all` and would also work, but
// the admin user is closer to how an operator would debug a live
// cluster, and reading the Secret from the source namespace avoids
// duplicating credentials into flux-system.
func valkeyPassword(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("kubectl", "--context", context, "-n", "valkey",
		"get", "secret/valkey",
		"-o", "jsonpath={.data.valkey-password}")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("read valkey secret: %v", err)
	}
	decoded, err := base64Decode(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("decode valkey-password: %v", err)
	}
	return decoded
}

func base64Decode(s string) (string, error) {
	b, err := b64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// prDedupeKey constructs the exact key the bridge writes for a given
// (ns, name, PR, PR head). Pinned here so the test fails loudly if the
// bridge's redis store changes its key format — that's a breaking change for
// any operator with pre-existing dedupe state.
func prDedupeKey(ns, name string, pr int, headSHA string) string {
	return fmt.Sprintf("flux-ai-bridge:dedupe:%s:%s:pr%d:%s", ns, name, pr, headSHA)
}

// rolloutRestartBridge runs `kubectl rollout restart` + `rollout status`
// against the bridge Deployment. The existing port-forward dies when the
// pod is replaced; the caller is expected to follow up with
// rebuildPortForward.
func rolloutRestartBridge(t *testing.T) {
	t.Helper()
	for _, args := range [][]string{
		{"--context", context, "-n", namespace, "rollout", "restart", "deploy/flux-ai-bridge"},
		{"--context", context, "-n", namespace, "rollout", "status", "deploy/flux-ai-bridge", "--timeout=2m"},
	} {
		cmd := exec.Command("kubectl", args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
}

// rebuildPortForward replaces the bridge port-forward (the only one this
// test cares about — mock-github/mock-llm forwards survive the bridge
// restart because they target different Services).
//
// We kill any existing kubectl-port-forward process targeting the bridge
// Service before spawning a fresh one, then poll /healthz until it's
// reachable. Mutates the package-level portForwards slice from e2e_test.go.
func rebuildPortForward(t *testing.T) error {
	t.Helper()
	// Kill the existing bridge forward. portForwards is set up by
	// TestMain in the same package; index 0 is the bridge forward
	// (the loop in TestMain inserts svc/flux-ai-bridge first), but
	// we match by inspecting Args defensively in case ordering changes.
	for _, c := range portForwards {
		if c == nil || c.Process == nil {
			continue
		}
		joined := strings.Join(c.Args, " ")
		if strings.Contains(joined, "svc/flux-ai-bridge") {
			_ = c.Process.Kill()
		}
	}

	cmd := exec.Command(
		"kubectl", "port-forward",
		"-n", namespace,
		"svc/flux-ai-bridge",
		bridgePort+":8080",
		"--context", context,
	)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start port-forward: %w", err)
	}
	portForwards = append(portForwards, cmd)

	// Give the port-forward a beat to bind before /healthz polling.
	time.Sleep(500 * time.Millisecond)
	return nil
}

// scrapeCounter pulls /metrics and extracts the value of a Prometheus
// counter with the supplied name + label match. Returns 0 when missing —
// which is also the legitimate "this label combination hasn't been
// observed yet" state, so callers must compare against expectations.
func scrapeCounter(t *testing.T, metric, labelMatch string) int {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(bridgeURL + "/metrics")
	if err != nil {
		t.Logf("scrape /metrics: %v", err)
		return 0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0
	}
	// Format: `flux_ai_bridge_prs_skipped_total{reason="deduped"} 3`
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, metric+"{") {
			continue
		}
		if !strings.Contains(line, labelMatch) {
			continue
		}
		// Counter value is whitespace-separated trailing number.
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		// Counter values can be floats in Prometheus exposition.
		f, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		return int(f)
	}
	return 0
}

// TestPersistence_BridgeWritesAreVisibleInValkey verifies the WRITE + READ
// legs of the bridge ↔ Valkey contract by reading the datastore DIRECTLY
// through `kubectl exec valkey-cli`, so a bug where the bridge reports
// success but never commits to the master (e.g. wrong sentinel master
// discovery) would surface here. It also checks the mark carries the
// DEDUPE_PERSISTENCE_MAX_TTL backstop (30 days by default) instead of
// living forever.
//
// Routes through sentinel + auth implicitly: the bridge's only path to Valkey
// in this configuration is via the FailoverClient asking the quorum for the
// master and authenticating with the application user's Secret.
func TestPersistence_BridgeWritesAreVisibleInValkey(t *testing.T) {
	if !persistenceEnabled(t) {
		t.Skip("persistence not enabled on the bridge pod; run with `make e2e-persistence`")
	}
	payload, key := analysePRHead(t, "persistence write-visibility test")

	ttl, err := strconv.Atoi(valkeyExec(t, "TTL", key))
	if err != nil {
		t.Fatalf("TTL %s: %v", key, err)
	}
	if maxTTL := 30 * 24 * 3600; ttl <= 0 || ttl > maxTTL {
		t.Fatalf("dedupe mark TTL = %ds, want the backstop expiry (0 < ttl <= %ds); -1 means it would never expire", ttl, maxTTL)
	}
	t.Logf("write visible: %s (TTL %ds)", key, ttl)

	// READ leg: a second POST for the same PR head must be deduped.
	expectDedupedReplay(t, payload)
}

// TestPersistence_AdminCredentialIsolated asserts the credential model
// `make e2e-valkey-setup` configures: an ADMIN user (`default`) with one
// password used by sentinel↔valkey and sentinel↔sentinel internal auth,
// and a separate APPLICATION user (`bridge`) used ONLY by the ai-bridge.
//
// Three structural checks + one functional check:
//
//  1. The bridge connects with a non-default ACL user — never `default`.
//  2. The same `bridge` user is used for both sentinel-leg and backend-
//     leg AUTH (typical production pattern: one application identity,
//     two endpoints).
//  3. The bridge's password is NOT the admin password. We read both:
//     the admin from `valkey/valkey` Secret (chart-managed,
//     `valkey-password` key) and the bridge from
//     `flux-system/flux-ai-bridge-dedupe` Secret (`bridge-password`
//     key). If they were equal, the test fails — that'd mean either a
//     misconfig or that the bridge was given admin creds.
//  4. Round-trip analysis against the live cluster still works end-to-end,
//     confirming the `bridge` user has the required ACL grants on the
//     master (SET NX, EXPIRE and reads through sentinel).
func TestPersistence_AdminCredentialIsolated(t *testing.T) {
	if !persistenceEnabled(t) {
		t.Skip("persistence not enabled on the bridge pod; run with `make e2e-persistence`")
	}

	user := bridgeEnvValue(t, "DEDUPE_PERSISTENCE_USER")
	if user == "" || user == "default" {
		t.Fatalf("DEDUPE_PERSISTENCE_USER must be a non-default ACL user (admin); got %q", user)
	}
	sentinelUser := bridgeEnvValue(t, "DEDUPE_PERSISTENCE_SENTINEL_USER")
	if sentinelUser == "" || sentinelUser == "default" {
		t.Fatalf("DEDUPE_PERSISTENCE_SENTINEL_USER must be a non-default ACL user (admin); got %q", sentinelUser)
	}
	// In this setup we deliberately use the SAME application user on
	// both legs (one identity, two endpoints). If you want them
	// separate, see git history for the earlier separate-sentinel-user
	// variant.
	if user != sentinelUser {
		t.Fatalf("expected the same application user on both legs in the e2e-persistence setup; got backend=%q sentinel=%q", user, sentinelUser)
	}

	// Read the admin password (from the chart-managed valkey Secret,
	// which the bridge pod never has access to) and the bridge's
	// application password (from the dedupe Secret in flux-system).
	adminPw := valkeyPassword(t)
	bridgePwSecret := bridgeEnvSecretName(t, "DEDUPE_PERSISTENCE_PASSWORD")
	bridgePwKey := bridgeEnvSecretKey(t, "DEDUPE_PERSISTENCE_PASSWORD")
	if bridgePwSecret == "" || bridgePwKey == "" {
		t.Fatalf("DEDUPE_PERSISTENCE_PASSWORD must be sourced from a Secret; got secret=%q key=%q", bridgePwSecret, bridgePwKey)
	}
	if bridgePwSecret == "valkey" {
		t.Fatalf("bridge must NOT reference the chart-managed `valkey` Secret (which holds the admin password); got %q", bridgePwSecret)
	}
	bridgePw := readBridgeSecretKey(t, bridgePwSecret, bridgePwKey)
	if bridgePw == "" {
		t.Fatal("bridge password is empty — application AUTH would fail")
	}
	if bridgePw == adminPw {
		t.Fatal("bridge password matches the admin password — production hygiene says these MUST differ; the bridge is given application credentials only")
	}
	t.Logf("ACL: application user=%q (Secret=%s, key=%s, %d bytes); admin password isolated in `valkey/valkey` (%d bytes)",
		user, bridgePwSecret, bridgePwKey, len(bridgePw), len(adminPw))

	// Functional cross-check: the application user has the ACL grants to
	// write and read marks on the master through sentinel. If the bridge fell
	// back to default/admin (or the ACL grants were missing), the analysis
	// would never commit its mark.
	analysePRHead(t, "admin-credential-isolated round-trip")
	t.Logf("round-trip OK: mark written via application-user AUTH on both legs")
}

// TestPersistence_QuorumTopology pins the 3-sentinel quorum-2 setup
// `make e2e-valkey-setup` configures. Two assertions:
//
//  1. Three sentinel addrs are wired into the bridge — derived by the
//     chart's auto-derive from sentinel.{headlessService, statefulSet,
//     replicas}. A single-pod setup would fail-over only if the lone
//     sentinel marks itself down (not realistic); 3 sentinels with
//     quorum=2 is the canonical split-brain-safe configuration.
//  2. Every sentinel agrees on the master via `SENTINEL
//     get-primary-addr-by-name`. If the cluster is mid-failover or
//     un-quorate, queries to different sentinels return different
//     answers — the test would catch a setup where the chart's
//     `sentinel.primarySet` and the bridge's masterName diverge.
func TestPersistence_QuorumTopology(t *testing.T) {
	if !persistenceEnabled(t) {
		t.Skip("persistence not enabled on the bridge pod; run with `make e2e-persistence`")
	}
	addrs := bridgeEnvValue(t, "DEDUPE_PERSISTENCE_SENTINEL_ADDRS")
	parts := strings.Split(addrs, ",")
	if len(parts) != 3 {
		t.Fatalf("DEDUPE_PERSISTENCE_SENTINEL_ADDRS should list 3 sentinels (one per StatefulSet pod); got %d: %q", len(parts), addrs)
	}
	masterName := bridgeEnvValue(t, "DEDUPE_PERSISTENCE_MASTER_NAME")
	if masterName == "" {
		t.Fatal("DEDUPE_PERSISTENCE_MASTER_NAME must be set in sentinel mode")
	}

	adminPw := valkeyPassword(t)
	var first string
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("valkey-node-%d", i)
		cmd := exec.Command("kubectl", "--context", context, "-n", "valkey",
			"exec", pod, "-c", "sentinel", "--",
			"valkey-cli", "-p", "26379", "-a", adminPw, "--no-auth-warning",
			"SENTINEL", "get-primary-addr-by-name", masterName)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("sentinel query on %s: %v\n%s", pod, err, out)
		}
		got := strings.TrimSpace(string(out))
		if got == "" {
			t.Fatalf("sentinel on %s did not know master %q (output empty)", pod, masterName)
		}
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("sentinels disagree on master:\n  valkey-node-0: %q\n  %s: %q\n(quorate cluster must converge)", first, pod, got)
		}
	}
	t.Logf("quorum OK: 3 sentinels at %v all agree master=%q is %q", parts, masterName, strings.ReplaceAll(first, "\n", " "))
}

// bridgeEnvValue returns a plain env var value on the bridge Deployment.
// Returns "" if absent or sourced from a Secret (use bridgeEnvSecretKey
// for the secret-keyref case).
func bridgeEnvValue(t *testing.T, name string) string {
	t.Helper()
	cmd := exec.Command("kubectl", "--context", context, "-n", namespace,
		"get", "deploy/flux-ai-bridge",
		"-o", fmt.Sprintf("jsonpath={.spec.template.spec.containers[0].env[?(@.name=='%s')].value}", name))
	out, _ := cmd.Output()
	return strings.TrimSpace(string(out))
}

// bridgeEnvSecretKey returns the `key` field of a valueFrom.secretKeyRef
// env var on the bridge. Empty when the env var is missing or sources
// from a plain value rather than a Secret.
func bridgeEnvSecretKey(t *testing.T, name string) string {
	t.Helper()
	cmd := exec.Command("kubectl", "--context", context, "-n", namespace,
		"get", "deploy/flux-ai-bridge",
		"-o", fmt.Sprintf("jsonpath={.spec.template.spec.containers[0].env[?(@.name=='%s')].valueFrom.secretKeyRef.key}", name))
	out, _ := cmd.Output()
	return strings.TrimSpace(string(out))
}

// bridgeEnvSecretName returns the `name` field of a valueFrom.secretKeyRef
// env var on the bridge. Used together with bridgeEnvSecretKey to read
// the actual password value back via readBridgeSecretKey.
func bridgeEnvSecretName(t *testing.T, name string) string {
	t.Helper()
	cmd := exec.Command("kubectl", "--context", context, "-n", namespace,
		"get", "deploy/flux-ai-bridge",
		"-o", fmt.Sprintf("jsonpath={.spec.template.spec.containers[0].env[?(@.name=='%s')].valueFrom.secretKeyRef.name}", name))
	out, _ := cmd.Output()
	return strings.TrimSpace(string(out))
}

// readBridgeSecretKey reads a key from a Secret in the bridge's
// namespace and base64-decodes it. Used by the ACL-user test to read
// the actual password values back and confirm they differ — without
// this, an operator who accidentally pointed both passwordKeys at the
// same key in a Secret would pass the env-var-key check (different
// keys) but still be using one shared password.
func readBridgeSecretKey(t *testing.T, secretName, key string) string {
	t.Helper()
	cmd := exec.Command("kubectl", "--context", context, "-n", namespace,
		"get", "secret/"+secretName,
		"-o", fmt.Sprintf("jsonpath={.data.%s}", key))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("read secret %s/%s key %s: %v", namespace, secretName, key, err)
	}
	decoded, err := base64Decode(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("decode secret %s/%s key %s: %v", namespace, secretName, key, err)
	}
	return decoded
}
