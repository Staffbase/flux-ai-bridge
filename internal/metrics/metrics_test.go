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

package metrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staffbase/flux-ai-bridge/internal/metrics"
)

// TestMetricsEndpoint exercises the full Prometheus path: register, mutate,
// scrape, parse. The goal is to catch silent regressions where a metric is
// added in code but not registered (so /metrics omits it), so the table lists
// every metric the package defines.
func TestMetricsEndpoint(t *testing.T) {
	metrics.Init()
	// Prometheus omits vec families that have zero samples, so each entry
	// records one sample: that confirms the metric is registered AND
	// scrapable in one shot.
	all := []struct {
		name  string
		touch func()
	}{
		{"flux_ai_bridge_events_processed_total", func() { metrics.EventsProcessed.WithLabelValues("success").Inc() }},
		{"flux_ai_bridge_prs_skipped_total", func() { metrics.PRsSkipped.WithLabelValues("deduped").Inc() }},
		{"flux_ai_bridge_events_filtered_total", func() { metrics.EventsFiltered.WithLabelValues("in_exclude").Inc() }},
		{"flux_ai_bridge_pipeline_duration_seconds", func() { metrics.PipelineDuration.Observe(0.1) }},
		{"flux_ai_bridge_llm_input_tokens_total", func() { metrics.LLMInputTokens.Add(1) }},
		{"flux_ai_bridge_llm_output_tokens_total", func() { metrics.LLMOutputTokens.Add(42) }},
		{"flux_ai_bridge_llm_max_tokens_hit_total", func() { metrics.LLMMaxTokensHit.Inc() }},
		{"flux_ai_bridge_llm_rate_limit_hits_total", func() { metrics.LLMRateLimitHits.WithLabelValues("claude").Inc() }},
		{"flux_ai_bridge_llm_requests_total", func() { metrics.LLMRequests.WithLabelValues("claude", "success").Inc() }},
		{"flux_ai_bridge_llm_fallbacks_total", func() { metrics.LLMFallbacks.WithLabelValues("claude").Inc() }},
		{"flux_ai_bridge_fetch_cache_hits_total", func() { metrics.FetchCacheHits.WithLabelValues("commit").Inc() }},
		{"flux_ai_bridge_fetch_cache_misses_total", func() { metrics.FetchCacheMisses.WithLabelValues("commit").Inc() }},
		{"flux_ai_bridge_github_auth_failures_total", func() { metrics.GitHubAuthFailures.Inc() }},
		{"flux_ai_bridge_github_rate_limit_hits_total", func() { metrics.GitHubRateLimitHits.Inc() }},
		{"flux_ai_bridge_github_rate_limit_remaining", func() { metrics.GitHubRateLimitRemaining.Set(4999) }},
		{"flux_ai_bridge_github_api_requests_total", func() { metrics.GitHubAPIRequests.WithLabelValues("GET", "200").Inc() }},
		{"flux_ai_bridge_mcp_call_duration_seconds", func() { metrics.MCPCallDuration.Observe(0.05) }},
		{"flux_ai_bridge_mcp_pool_size", func() { metrics.MCPPoolSize.Set(3) }},
		{"flux_ai_bridge_mcp_pool_in_use", func() { metrics.MCPPoolInUse.Set(1) }},
		{"flux_ai_bridge_mcp_call_retries_total", func() { metrics.MCPCallRetries.Inc() }},
		{"flux_ai_bridge_prs_for_commit_coalesced_total", func() { metrics.PRsForCommitCoalesced.Inc() }},
	}
	for _, m := range all {
		m.touch()
	}

	srv := httptest.NewServer(metrics.Handler())
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	out := string(body)
	// Match the family's TYPE line, so one name that prefixes another
	// can't stand in for it.
	for _, m := range all {
		if !strings.Contains(out, "\n# TYPE "+m.name+" ") {
			t.Errorf("metrics output missing family %q", m.name)
		}
	}
	// Every bridge family in the output is in the table, so a metric added
	// to register() without a row here fails too.
	listed := make(map[string]bool, len(all))
	for _, m := range all {
		listed[m.name] = true
	}
	for line := range strings.SplitSeq(out, "\n") {
		if name, ok := strings.CutPrefix(line, "# TYPE flux_ai_bridge_"); ok {
			if name = "flux_ai_bridge_" + strings.Fields(name)[0]; !listed[name] {
				t.Errorf("metric family %q is exposed but not listed in this test", name)
			}
		}
	}
	// Negative assertions: dropped metrics MUST NOT reappear in /metrics
	// output. Keeps the trimmed surface stable across future PRs — anyone
	// re-introducing them needs a deliberate update here.
	for _, gone := range []string{
		"flux_ai_bridge_events_received_total",
		"flux_ai_bridge_llm_cache_read_tokens_total",
		"flux_ai_bridge_llm_cache_write_tokens_total",
	} {
		if strings.Contains(out, gone) {
			t.Errorf("metrics output unexpectedly contains dropped metric %q", gone)
		}
	}
}
