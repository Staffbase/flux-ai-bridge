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
// added in code but not registered (so /metrics omits it).
func TestMetricsEndpoint(t *testing.T) {
	metrics.Init()
	// Touch every metric — Prometheus omits vec families that have zero samples.
	// Recording at least one sample per metric confirms it's registered AND
	// scrapable in one shot.
	metrics.EventsProcessed.WithLabelValues("success").Inc()
	metrics.PipelineDuration.Observe(0.1)
	metrics.LLMInputTokens.Add(1)
	metrics.LLMOutputTokens.Add(42)
	metrics.LLMMaxTokensHit.Inc()
	metrics.GitHubRateLimitRemaining.Set(4999)
	metrics.GitHubAuthFailures.Inc()
	metrics.MCPCallDuration.Observe(0.05)
	metrics.MCPPoolSize.Set(3)
	metrics.MCPPoolInUse.Set(1)

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
	for _, want := range []string{
		"flux_ai_bridge_events_processed_total",
		"flux_ai_bridge_pipeline_duration_seconds",
		"flux_ai_bridge_llm_input_tokens_total",
		"flux_ai_bridge_llm_output_tokens_total",
		"flux_ai_bridge_llm_max_tokens_hit_total",
		"flux_ai_bridge_github_rate_limit_remaining",
		"flux_ai_bridge_github_auth_failures_total",
		"flux_ai_bridge_mcp_call_duration_seconds",
		"flux_ai_bridge_mcp_pool_size",
		"flux_ai_bridge_mcp_pool_in_use",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
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
