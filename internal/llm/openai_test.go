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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	openai "github.com/openai/openai-go/v3"
	openaiOption "github.com/openai/openai-go/v3/option"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
)

// The output cap must reach the backend in the field it honours: Ollama reads
// only max_tokens, everything else gets max_completion_tokens (OpenAI's
// reasoning models reject max_tokens).
func TestOpenAIAnalyzer_TokenCapField(t *testing.T) {
	for _, tc := range []struct {
		baseURL, wantField, otherField string
	}{
		{"http://ollama.llm.svc.cluster.local:11434/v1", "max_tokens", "max_completion_tokens"},
		{"https://models.inference.ai.azure.com", "max_completion_tokens", "max_tokens"},
		{"http://llm-gateway.example.com:8787", "max_completion_tokens", "max_tokens"},
	} {
		t.Run(tc.baseURL, func(t *testing.T) {
			var body map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&body)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"c","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
			}))
			defer srv.Close()

			// The constructor decides the field from the configured URL; the
			// client is then pointed at the test server.
			a := NewOpenAIAnalyzer(&config.Config{OpenAIBaseURL: tc.baseURL, LLMModel: "m", LLMMaxTokens: 16})
			a.client = openai.NewClient(openaiOption.WithBaseURL(srv.URL), openaiOption.WithMaxRetries(0))

			if _, err := a.Analyze(context.Background(), "", "hello"); err != nil {
				t.Fatalf("Analyze: %v", err)
			}
			if got := body[tc.wantField]; got != float64(16) {
				t.Errorf("%s = %v, want 16; body=%v", tc.wantField, got, body)
			}
			if got, sent := body[tc.otherField]; sent {
				t.Errorf("%s = %v sent too, want it absent", tc.otherField, got)
			}
		})
	}
}

// LLMInputTokens counts uncached input, as the Claude analyzer does
// (Anthropic's input_tokens excludes cache reads). OpenAI's prompt_tokens
// includes the cached part, so only the remainder may be added.
func TestOpenAIAnalyzer_InputTokensExcludeCached(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":3000,"completion_tokens":5,"total_tokens":3005,"prompt_tokens_details":{"cached_tokens":2000}}}`))
	}))
	defer srv.Close()

	a := NewOpenAIAnalyzer(&config.Config{LLMModel: "m"})
	a.client = openai.NewClient(openaiOption.WithBaseURL(srv.URL), openaiOption.WithMaxRetries(0))

	before := testutil.ToFloat64(metrics.LLMInputTokens)
	if _, err := a.Analyze(context.Background(), "", "hello"); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if got := testutil.ToFloat64(metrics.LLMInputTokens) - before; got != 1000 {
		t.Errorf("LLMInputTokens grew by %v, want 1000 (prompt 3000 minus 2000 cached)", got)
	}
}
