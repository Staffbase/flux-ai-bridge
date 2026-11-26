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

package llm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	anthropicOption "github.com/anthropics/anthropic-sdk-go/option"
	openai "github.com/openai/openai-go/v3"
	openaiOption "github.com/openai/openai-go/v3/option"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
)

// A 429 returned by the Anthropic API (after the SDK exhausts its 2-retry
// budget) must bump LLMRateLimitHits{provider=claude} so quota alerts have
// a dedicated signal — not be conflated with generic llm_error outcomes.
// Disable SDK retries in the test so the httptest server only sees one
// request and the test runs in milliseconds.
func TestClaudeAnalyzer_Bumps429Counter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`))
	}))
	defer srv.Close()

	a := &ClaudeAnalyzer{
		client: anthropic.NewClient(
			anthropicOption.WithBaseURL(srv.URL),
			anthropicOption.WithAPIKey("test"),
			anthropicOption.WithMaxRetries(0),
		),
		model:     "claude-test",
		maxTokens: 100,
	}

	before := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("claude"))
	_, err := a.Analyze(context.Background(), "", "hello")
	if err == nil {
		t.Fatal("expected error from 429 response, got nil")
	}
	if got := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("claude")) - before; got != 1 {
		t.Errorf("LLMRateLimitHits{claude} delta = %v, want 1; err=%v", got, err)
	}
}

// Non-429 errors (5xx, network blip, schema mismatch, etc.) must NOT bump
// the rate-limit counter — they belong in the generic events_processed
// {outcome=llm_error} bucket. Verifies the classification is tight.
func TestClaudeAnalyzer_500DoesNotBump429Counter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"boom"}}`))
	}))
	defer srv.Close()

	a := &ClaudeAnalyzer{
		client: anthropic.NewClient(
			anthropicOption.WithBaseURL(srv.URL),
			anthropicOption.WithAPIKey("test"),
			anthropicOption.WithMaxRetries(0),
		),
		model:     "claude-test",
		maxTokens: 100,
	}

	before := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("claude"))
	_, err := a.Analyze(context.Background(), "", "hello")
	if err == nil {
		t.Fatal("expected error from 500 response, got nil")
	}
	if got := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("claude")) - before; got != 0 {
		t.Errorf("LLMRateLimitHits{claude} unexpectedly incremented by %v on 500 error; err=%v", got, err)
	}
}

func TestOpenAIAnalyzer_Bumps429Counter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited","type":"rate_limit_exceeded"}}`))
	}))
	defer srv.Close()

	a := &OpenAIAnalyzer{
		client: openai.NewClient(
			openaiOption.WithBaseURL(srv.URL),
			openaiOption.WithAPIKey("test"),
			openaiOption.WithMaxRetries(0),
		),
		model:     "gpt-test",
		maxTokens: 100,
	}

	before := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("openai-compatible"))
	_, err := a.Analyze(context.Background(), "", "hello")
	if err == nil {
		t.Fatal("expected error from 429 response, got nil")
	}
	if got := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("openai-compatible")) - before; got != 1 {
		t.Errorf("LLMRateLimitHits{openai-compatible} delta = %v, want 1; err=%v", got, err)
	}
	// Sanity check that errors.As against *openai.Error actually matched —
	// otherwise the counter would be at delta 0 and the previous assertion
	// would catch it, but a clearer assertion here helps when this test
	// breaks because the SDK changed its error wire shape.
	if !strings.Contains(err.Error(), "openai analyze") {
		t.Errorf("error %q lacks expected analyze prefix", err)
	}
}

func TestOpenAIAnalyzer_500DoesNotBump429Counter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"boom","type":"server_error"}}`))
	}))
	defer srv.Close()

	a := &OpenAIAnalyzer{
		client: openai.NewClient(
			openaiOption.WithBaseURL(srv.URL),
			openaiOption.WithAPIKey("test"),
			openaiOption.WithMaxRetries(0),
		),
		model:     "gpt-test",
		maxTokens: 100,
	}

	before := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("openai-compatible"))
	_, err := a.Analyze(context.Background(), "", "hello")
	if err == nil {
		t.Fatal("expected error from 500 response, got nil")
	}
	if got := testutil.ToFloat64(metrics.LLMRateLimitHits.WithLabelValues("openai-compatible")) - before; got != 0 {
		t.Errorf("LLMRateLimitHits{openai-compatible} unexpectedly incremented by %v on 500 error; err=%v", got, err)
	}
}

// A large LLM_MAX_TOKENS must still produce a request. The SDK refuses
// non-streaming calls whose max_tokens implies a >10-minute generation unless
// the client sets an explicit request timeout, which NewClaudeAnalyzer does.
func TestClaudeAnalyzer_LargeMaxTokensStillSends(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m","type":"message","role":"assistant","model":"claude-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer srv.Close()

	a := NewClaudeAnalyzer(&config.Config{AnthropicAPIKey: "test", AnthropicBaseURL: srv.URL, LLMModel: "claude-test", LLMMaxTokens: 64000})
	out, err := a.Analyze(context.Background(), "", "hello")
	if err != nil {
		t.Fatalf("Analyze with max_tokens=64000: %v", err)
	}
	if hits != 1 || out != "ok" {
		t.Fatalf("hits=%d out=%q, want one request answered with ok", hits, out)
	}
}
