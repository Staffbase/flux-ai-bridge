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

package config_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/config"
)

// fakeAppPEM is a syntactically-shaped placeholder. config.Load doesn't
// parse the key — that happens in internal/github/commenter.NewCommenter
// — so any non-empty value is fine for verifying validation logic here.
const fakeAppPEM = "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----"

// configEnvVars lists every variable Load reads. setEnv clears them all, so a
// variable exported in the developer's shell can't leak into a fixture;
// TestSetEnvClearsEveryConfigVariable keeps the list complete.
var configEnvVars = []string{
	"WEBHOOK_PORT", "MCP_URL", "LLM_PROVIDER",
	"ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "OPENAI_API_KEY", "OPENAI_BASE_URL",
	"LLM_MODEL", "LLM_MAX_TOKENS", "LLM_SKIP_ON_HEALTHY",
	"GITHUB_APP_ID", "GITHUB_APP_INSTALLATION_ID", "GITHUB_APP_PRIVATE_KEY", "GITHUB_APP_PRIVATE_KEY_FILE",
	"GITHUB_API_URL", "GITHUB_REPO", "GITHUB_KEEP_COMMENT_HISTORY",
	"KUSTOMIZATIONS_INCLUDE", "KUSTOMIZATIONS_EXCLUDE",
	"DEDUPE_PERSISTENCE_MAX_TTL", "SHUTDOWN_TIMEOUT",
	"READINESS_POLL_INTERVAL", "READINESS_POLL_TIMEOUT", "DEDUPE_PR_REEVAL_TTL",
	"DEDUPE_PR_REEVAL_MAX", "DEDUPE_REEVAL_FAILED_ONLY",
	"READINESS_POD_QUIET_PERIOD", "READINESS_EXPAND_HELMRELEASES", "READINESS_INCLUDE_PODS",
	"FAILURE_FETCH_EVENTS", "FAILURE_FETCH_FLUX_INSTANCE", "FAILURE_FETCH_LOGS", "FAILURE_FETCH_METRICS",
	"COMMENT_FAILED_RESOURCES_ONLY", "COMMENT_INCLUDE_FOOTER", "COMMENT_INCLUDE_SUMMARY", "COMMENT_ON_SUCCESS",
	"PR_ATTRIBUTE_PRUNES", "PR_REQUIRED_LABELS",
	"PR_LABEL_ENABLED", "PR_LABEL_SUCCESS", "PR_LABEL_FAILED", "PR_LABEL_WARNING",
	"INSTANCE_ID",
}

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, k := range configEnvVars {
		t.Setenv(k, "")
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

// TestSetEnvClearsEveryConfigVariable fails when Load reads a variable that
// setEnv doesn't clear: a value exported in the shell would then override the
// default a test asserts.
func TestSetEnvClearsEveryConfigVariable(t *testing.T) {
	src, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	cleared := make(map[string]bool, len(configEnvVars))
	for _, k := range configEnvVars {
		cleared[k] = true
	}
	matches := regexp.MustCompile(`(?:Getenv|Env)\("([A-Z0-9_]+)"`).FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatal("found no environment reads in config.go; the pattern is out of date")
	}
	for _, m := range matches {
		if !cleared[m[1]] {
			t.Errorf("Load reads %s but setEnv doesn't clear it; add it to configEnvVars", m[1])
		}
	}
}

// All happy-path fixtures now configure GitHub App auth — there is no
// PAT path in the bridge anymore.
func baseAppEnv() map[string]string {
	return map[string]string{
		"GITHUB_APP_ID":              "12345",
		"GITHUB_APP_INSTALLATION_ID": "67890",
		"GITHUB_APP_PRIVATE_KEY":     fakeAppPEM,
	}
}

func baseClaudeEnv() map[string]string {
	env := baseAppEnv()
	env["MCP_URL"] = "http://mcp:9090/mcp"
	env["LLM_PROVIDER"] = "claude"
	env["ANTHROPIC_API_KEY"] = "sk-ant-test"
	return env
}

func baseOpenAIEnv() map[string]string {
	env := baseAppEnv()
	env["MCP_URL"] = "http://mcp:9090/mcp"
	env["LLM_PROVIDER"] = "openai-compatible"
	env["OPENAI_API_KEY"] = "sk-openai-test"
	return env
}

func TestLoad_Claude_Defaults(t *testing.T) {
	setEnv(t, baseClaudeEnv())
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LLMProvider != "claude" {
		t.Errorf("LLMProvider = %q, want %q", cfg.LLMProvider, "claude")
	}
	if cfg.LLMModel != "claude-sonnet-4-6" {
		t.Errorf("LLMModel = %q, want default claude model", cfg.LLMModel)
	}
	if cfg.WebhookPort != 8080 {
		t.Errorf("WebhookPort = %d, want 8080", cfg.WebhookPort)
	}
	if cfg.MCPURL != "http://mcp:9090/mcp" {
		t.Errorf("MCPURL = %q, unexpected", cfg.MCPURL)
	}
	if cfg.AnthropicAPIKey != "sk-ant-test" {
		t.Errorf("AnthropicAPIKey = %q, unexpected", cfg.AnthropicAPIKey)
	}
}

// A garbled boolean env var must fail startup, not silently flip the flag
// to its default — same fail-fast contract the int/duration parsers have.
func TestLoad_InvalidBoolFailsFast(t *testing.T) {
	env := baseClaudeEnv()
	env["COMMENT_ON_SUCCESS"] = "flase" // typo
	setEnv(t, env)
	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for unparseable boolean env var")
	}
	if !strings.Contains(err.Error(), "COMMENT_ON_SUCCESS") {
		t.Errorf("error should name the offending var, got %v", err)
	}
}

// _FILE variant of the private key is the same shape as _KEY — accept it
// in lieu of an inline PEM (operators sometimes mount the key as a file).
func TestLoad_AppAuthWithPrivateKeyFile(t *testing.T) {
	env := baseClaudeEnv()
	delete(env, "GITHUB_APP_PRIVATE_KEY")
	env["GITHUB_APP_PRIVATE_KEY_FILE"] = "/etc/secrets/app-key.pem"
	setEnv(t, env)
	if _, err := config.Load(); err != nil {
		t.Fatalf("App-only (file variant) should load, got: %v", err)
	}
}

func TestLoad_Claude_ModelOverride(t *testing.T) {
	env := baseClaudeEnv()
	env["LLM_MODEL"] = "claude-opus-4-5"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LLMModel != "claude-opus-4-5" {
		t.Errorf("LLMModel = %q, want %q", cfg.LLMModel, "claude-opus-4-5")
	}
}

// LLM_PROVIDER=none is a valid config that disables the LLM entirely.
// Neither ANTHROPIC_API_KEY nor OPENAI_API_KEY is required — the processor
// builds the PR comment from kstatus output directly.
func TestLoad_None_NoAPIKeyRequired(t *testing.T) {
	env := baseAppEnv()
	env["MCP_URL"] = "http://mcp:9090/mcp"
	env["LLM_PROVIDER"] = "none"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("LLM_PROVIDER=none should load without API key, got: %v", err)
	}
	if cfg.LLMProvider != "none" {
		t.Errorf("LLMProvider = %q, want %q", cfg.LLMProvider, "none")
	}
	if cfg.LLMModel != "" {
		t.Errorf("LLMModel = %q, want empty (no model needed when LLM is off)", cfg.LLMModel)
	}
}

// TestLoad_OpenAI_NoKeyOK pins that openai-compatible loads WITHOUT an API key
// — unauthenticated endpoints (a bundled/self-hosted Ollama, vLLM) accept calls
// with no key, and the OpenAI client tolerates an empty one. The bundled-Ollama
// Helm flow relies on this (it injects OPENAI_API_KEY="").
func TestLoad_OpenAI_NoKeyOK(t *testing.T) {
	env := baseAppEnv()
	env["MCP_URL"] = "http://mcp:9090/mcp"
	env["LLM_PROVIDER"] = "openai-compatible"
	env["OPENAI_BASE_URL"] = "http://ollama.default.svc.cluster.local:11434/v1"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("openai-compatible should load without an API key (unauthenticated endpoint), got: %v", err)
	}
	if cfg.OpenAIAPIKey != "" {
		t.Errorf("OpenAIAPIKey = %q, want empty", cfg.OpenAIAPIKey)
	}
	if cfg.LLMModel != "gpt-4.1" {
		t.Errorf("LLMModel = %q, want default gpt-4.1", cfg.LLMModel)
	}
}

func TestLoad_OpenAI_Defaults(t *testing.T) {
	setEnv(t, baseOpenAIEnv())
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LLMProvider != "openai-compatible" {
		t.Errorf("LLMProvider = %q, want %q", cfg.LLMProvider, "openai-compatible")
	}
	if cfg.LLMModel != "gpt-4.1" {
		t.Errorf("LLMModel = %q, want %q", cfg.LLMModel, "gpt-4.1")
	}
	if cfg.OpenAIAPIKey != "sk-openai-test" {
		t.Errorf("OpenAIAPIKey = %q, want explicit value", cfg.OpenAIAPIKey)
	}
	if cfg.OpenAIBaseURL != "https://models.inference.ai.azure.com" {
		t.Errorf("OpenAIBaseURL = %q, want default", cfg.OpenAIBaseURL)
	}
}

func TestLoad_OpenAI_BaseURLOverride(t *testing.T) {
	env := baseOpenAIEnv()
	env["OPENAI_BASE_URL"] = "https://custom.endpoint.example.com"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.OpenAIBaseURL != "https://custom.endpoint.example.com" {
		t.Errorf("OpenAIBaseURL = %q, want override", cfg.OpenAIBaseURL)
	}
}

func TestLoad_OpenAI_ModelOverride(t *testing.T) {
	env := baseOpenAIEnv()
	env["LLM_MODEL"] = "gpt-4o"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LLMModel != "gpt-4o" {
		t.Errorf("LLMModel = %q, want gpt-4o", cfg.LLMModel)
	}
}

func TestLoad_CustomPort(t *testing.T) {
	env := baseClaudeEnv()
	env["WEBHOOK_PORT"] = "9999"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.WebhookPort != 9999 {
		t.Errorf("WebhookPort = %d, want 9999", cfg.WebhookPort)
	}
}

func TestLoad_GitHubRepo(t *testing.T) {
	env := baseClaudeEnv()
	env["GITHUB_REPO"] = "myorg/myrepo"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GitHubRepo != "myorg/myrepo" {
		t.Errorf("GitHubRepo = %q, want myorg/myrepo", cfg.GitHubRepo)
	}
}

func TestLoad_PRLabelNames(t *testing.T) {
	// Empty by default → the commenter applies its flux-* defaults.
	setEnv(t, baseClaudeEnv())
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PRLabelWarning != "" {
		t.Errorf("PRLabelWarning default = %q, want empty (commenter defaults to flux-warning)", cfg.PRLabelWarning)
	}

	// Explicit overrides, including setting warning == failed.
	env := baseClaudeEnv()
	env["PR_LABEL_SUCCESS"] = "deploy-ok"
	env["PR_LABEL_FAILED"] = "deploy-broken"
	env["PR_LABEL_WARNING"] = "deploy-broken"
	setEnv(t, env)
	cfg, err = config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PRLabelSuccess != "deploy-ok" || cfg.PRLabelFailed != "deploy-broken" || cfg.PRLabelWarning != "deploy-broken" {
		t.Errorf("PR label names = (%q,%q,%q), want (deploy-ok, deploy-broken, deploy-broken)",
			cfg.PRLabelSuccess, cfg.PRLabelFailed, cfg.PRLabelWarning)
	}
}

func TestLoad_Errors(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name: "missing MCP_URL",
			env: mergeEnv(baseAppEnv(), map[string]string{
				"LLM_PROVIDER": "claude", "ANTHROPIC_API_KEY": "key",
			}),
			wantErr: "MCP_URL is required",
		},
		{
			name: "missing LLM_PROVIDER",
			env: mergeEnv(baseAppEnv(), map[string]string{
				"MCP_URL": "http://mcp",
			}),
			wantErr: "LLM_PROVIDER is required",
		},
		{
			// No GitHub App env vars at all → bridge can't authenticate.
			name:    "missing GitHub App config",
			env:     map[string]string{"MCP_URL": "http://mcp", "LLM_PROVIDER": "claude", "ANTHROPIC_API_KEY": "key"},
			wantErr: "GitHub App config required",
		},
		{
			// Partial App config (missing private key) must NOT count as
			// "App configured".
			name: "incomplete GitHub App config (missing private key)",
			env: map[string]string{
				"MCP_URL": "http://mcp", "LLM_PROVIDER": "claude", "ANTHROPIC_API_KEY": "key",
				"GITHUB_APP_ID": "12345", "GITHUB_APP_INSTALLATION_ID": "67890",
			},
			wantErr: "GitHub App config required",
		},
		{
			// Partial App config (missing installation ID) must error too.
			name: "incomplete GitHub App config (missing installation ID)",
			env: map[string]string{
				"MCP_URL": "http://mcp", "LLM_PROVIDER": "claude", "ANTHROPIC_API_KEY": "key",
				"GITHUB_APP_ID": "12345", "GITHUB_APP_PRIVATE_KEY": fakeAppPEM,
			},
			wantErr: "GitHub App config required",
		},
		{
			name: "claude missing ANTHROPIC_API_KEY",
			env: mergeEnv(baseAppEnv(), map[string]string{
				"MCP_URL": "http://mcp", "LLM_PROVIDER": "claude",
			}),
			wantErr: "ANTHROPIC_API_KEY is required",
		},
		{
			name: "unsupported provider",
			env: mergeEnv(baseAppEnv(), map[string]string{
				"MCP_URL": "http://mcp", "LLM_PROVIDER": "openai",
			}),
			wantErr: "unsupported LLM_PROVIDER",
		},
		{
			name: "invalid WEBHOOK_PORT",
			env: mergeEnv(baseClaudeEnv(), map[string]string{
				"WEBHOOK_PORT": "notanumber",
			}),
			wantErr: "invalid WEBHOOK_PORT",
		},
		{
			// Bad glob in KUSTOMIZATIONS_INCLUDE must fail-fast at startup
			// rather than silently treat the typo as "matches nothing."
			name: "invalid KUSTOMIZATIONS_INCLUDE glob",
			env: mergeEnv(baseClaudeEnv(), map[string]string{
				"KUSTOMIZATIONS_INCLUDE": "[unclosed",
			}),
			wantErr: "kustomizations filter",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)
			_, err := config.Load()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestLoad_Claude_AnthropicBaseURL(t *testing.T) {
	env := baseClaudeEnv()
	env["ANTHROPIC_BASE_URL"] = "http://mock-llm:80"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AnthropicBaseURL != "http://mock-llm:80" {
		t.Errorf("AnthropicBaseURL = %q, want %q", cfg.AnthropicBaseURL, "http://mock-llm:80")
	}
}

func TestLoad_KustomizationsFilter(t *testing.T) {
	env := baseClaudeEnv()
	env["KUSTOMIZATIONS_INCLUDE"] = "apps-*, infra-cert-manager"
	env["KUSTOMIZATIONS_EXCLUDE"] = "apps-canary"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantInclude := []string{"apps-*", "infra-cert-manager"}
	wantExclude := []string{"apps-canary"}
	if !equalSlices(cfg.KustomizationsInclude, wantInclude) {
		t.Errorf("KustomizationsInclude = %v, want %v", cfg.KustomizationsInclude, wantInclude)
	}
	if !equalSlices(cfg.KustomizationsExclude, wantExclude) {
		t.Errorf("KustomizationsExclude = %v, want %v", cfg.KustomizationsExclude, wantExclude)
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestLoad_GitHubAPIURL(t *testing.T) {
	env := baseClaudeEnv()
	env["GITHUB_API_URL"] = "http://mock-github:80/"
	setEnv(t, env)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GitHubAPIURL != "http://mock-github:80/" {
		t.Errorf("GitHubAPIURL = %q, want %q", cfg.GitHubAPIURL, "http://mock-github:80/")
	}
}

// mergeEnv returns a new map with keys from b overriding keys from a.
// Lets the table-driven error tests reuse baseAppEnv() while overriding
// one or two fields without mutating the base.
func mergeEnv(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// DEDUPE_PERSISTENCE_MAX_TTL defaults to 30 days and accepts 0 (backstop off);
// SHUTDOWN_TIMEOUT defaults to 30s. Values the bridge can't honour fail at
// startup instead of misbehaving later (a negative TTL makes every Redis SET
// fail; a zero shutdown budget cancels pipelines with no time to roll back).
func TestLoad_DurationValidation(t *testing.T) {
	setEnv(t, baseClaudeEnv())
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DedupePersistenceMaxTTL != 30*24*time.Hour {
		t.Errorf("DedupePersistenceMaxTTL default = %s, want 720h", cfg.DedupePersistenceMaxTTL)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout default = %s, want 30s", cfg.ShutdownTimeout)
	}
	if cfg.ReadinessPodQuietPeriod != time.Minute {
		t.Errorf("ReadinessPodQuietPeriod default = %s, want 1m", cfg.ReadinessPodQuietPeriod)
	}

	setEnv(t, mergeEnv(baseClaudeEnv(), map[string]string{"DEDUPE_PERSISTENCE_MAX_TTL": "0", "SHUTDOWN_TIMEOUT": "50s", "READINESS_POD_QUIET_PERIOD": "0"}))
	cfg, err = config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DedupePersistenceMaxTTL != 0 || cfg.ShutdownTimeout != 50*time.Second || cfg.ReadinessPodQuietPeriod != 0 {
		t.Errorf("overrides = (%s, %s, %s), want (0s, 50s, 0s)", cfg.DedupePersistenceMaxTTL, cfg.ShutdownTimeout, cfg.ReadinessPodQuietPeriod)
	}

	for env, wantErr := range map[string]string{
		"DEDUPE_PERSISTENCE_MAX_TTL=-1h": "DEDUPE_PERSISTENCE_MAX_TTL must be >= 0",
		"SHUTDOWN_TIMEOUT=0s":            "SHUTDOWN_TIMEOUT must be > 0",
		"READINESS_POLL_INTERVAL=0s":     "READINESS_POLL_INTERVAL must be > 0",
		"READINESS_POLL_TIMEOUT=-1m":     "READINESS_POLL_TIMEOUT must be > 0",
		"DEDUPE_PR_REEVAL_TTL=-5m":       "DEDUPE_PR_REEVAL_TTL must be >= 0",
		"READINESS_POD_QUIET_PERIOD=-1s": "READINESS_POD_QUIET_PERIOD must be >= 0",
		"READINESS_POD_QUIET_PERIOD=3m":  "must be shorter than READINESS_POLL_TIMEOUT",
	} {
		k, v, _ := strings.Cut(env, "=")
		setEnv(t, mergeEnv(baseClaudeEnv(), map[string]string{k: v}))
		if _, err := config.Load(); err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("%s: err = %v, want %q", env, err, wantErr)
		}
	}
}

// An existing deployment with a short READINESS_POLL_TIMEOUT that never set
// READINESS_POD_QUIET_PERIOD must keep starting after the upgrade: the
// default shrinks to fit (half the poll timeout) instead of being rejected.
// Only an explicit value that doesn't fit is an error.
func TestLoad_PodQuietPeriodDefaultFitsShortPollTimeout(t *testing.T) {
	setEnv(t, mergeEnv(baseClaudeEnv(), map[string]string{"READINESS_POLL_TIMEOUT": "1m"}))
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load with READINESS_POLL_TIMEOUT=1m and no quiet period: %v", err)
	}
	if cfg.ReadinessPodQuietPeriod != 30*time.Second {
		t.Errorf("ReadinessPodQuietPeriod = %s, want 30s (half of 1m)", cfg.ReadinessPodQuietPeriod)
	}

	setEnv(t, mergeEnv(baseClaudeEnv(), map[string]string{"READINESS_POLL_TIMEOUT": "1m", "READINESS_POD_QUIET_PERIOD": "1m"}))
	if _, err := config.Load(); err == nil || !strings.Contains(err.Error(), "must be shorter than READINESS_POLL_TIMEOUT") {
		t.Errorf("explicit quiet period = poll timeout: err = %v, want the shorter-than error", err)
	}
}

// INSTANCE_ID lands in HTML comment markers and Redis keys, so only DNS
// labels are accepted.
func TestLoad_InstanceID(t *testing.T) {
	for id, ok := range map[string]bool{
		"":                            true,
		"prod-eu1":                    true,
		"a":                           true,
		"Prod":                        false,
		"prod_eu":                     false,
		"-prod":                       false,
		"prod -->":                    false,
		"x" + strings.Repeat("y", 63): false,
	} {
		setEnv(t, mergeEnv(baseClaudeEnv(), map[string]string{"INSTANCE_ID": id}))
		cfg, err := config.Load()
		if ok && (err != nil || cfg.InstanceID != id) {
			t.Errorf("INSTANCE_ID=%q: err=%v, want accepted", id, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "INSTANCE_ID")) {
			t.Errorf("INSTANCE_ID=%q: err=%v, want rejected", id, err)
		}
	}
}
