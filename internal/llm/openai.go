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
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/azure"
	"github.com/openai/openai-go/v3/option"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
)

// azureOpenAIAPIVersion is the API version sent on every Azure OpenAI
// Service request. Azure rejects requests without it. Newer values may
// unlock more model features; older values stay stable. Bump as needed.
const azureOpenAIAPIVersion = "2024-10-21"

type OpenAIAnalyzer struct {
	client    openai.Client
	model     string
	maxTokens int64
	// legacyMaxTokens sends the output cap as max_tokens instead of
	// max_completion_tokens. Ollama's OpenAI-compatible API reads only
	// max_tokens and ignores the newer field, so without this LLM_MAX_TOKENS
	// would not bound its output. OpenAI's reasoning models reject
	// max_tokens, so every other endpoint keeps max_completion_tokens.
	legacyMaxTokens bool
}

func NewOpenAIAnalyzer(cfg *config.Config) *OpenAIAnalyzer {
	// Two routing shapes share this analyzer:
	//   - generic OpenAI-compatible (OpenAI, Azure AI Foundry, OpenRouter,
	//     Ollama, vLLM, …): Bearer auth, /chat/completions on base URL.
	//   - Azure OpenAI Service deployments API (resource endpoints like
	//     <resource>.openai.azure.com): `api-key` header, URL rewritten to
	//     /openai/deployments/<deployment>/chat/completions?api-version=…,
	//     model name in the body is treated as the *deployment* name.
	//
	// We pick by URL — see azureDeploymentsEndpoint.
	var options []option.RequestOption
	if endpoint := azureDeploymentsEndpoint(cfg.OpenAIBaseURL); endpoint != "" {
		options = append(options,
			azure.WithEndpoint(endpoint, azureOpenAIAPIVersion),
			azure.WithAPIKey(cfg.OpenAIAPIKey),
		)
	} else {
		options = append(options, option.WithAPIKey(cfg.OpenAIAPIKey))
		if cfg.OpenAIBaseURL != "" {
			options = append(options, option.WithBaseURL(cfg.OpenAIBaseURL))
		}
	}
	max := int64(cfg.LLMMaxTokens)
	if max <= 0 {
		max = defaultMaxTokens
	}
	return &OpenAIAnalyzer{
		client:          openai.NewClient(options...),
		model:           cfg.LLMModel,
		maxTokens:       max,
		legacyMaxTokens: isOllamaEndpoint(cfg.OpenAIBaseURL),
	}
}

// ollamaPort is the port Ollama listens on by default, and the one the
// bundled sub-chart and docs/local-llm.md use.
const ollamaPort = "11434"

// isOllamaEndpoint reports whether baseURL addresses an Ollama server, told
// apart by Ollama's default port: the OpenAI-compatible API has no capability
// discovery for the token-limit field.
func isOllamaEndpoint(baseURL string) bool {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	return err == nil && u.Port() == ollamaPort
}

// azureDeploymentsEndpoint returns the resource endpoint to hand to
// azure.WithEndpoint when baseURL addresses the Azure OpenAI deployments API,
// or "" when baseURL is a plain OpenAI-compatible base. Deployments API: a
// *.openai.azure.com or *.cognitiveservices.azure.com host with no path or
// just /openai (the SDK appends /openai/deployments/… itself, so the path is
// dropped). Everything else is plain: the shared Azure AI Foundry / GitHub
// Models endpoint, any other provider, and Azure's /openai/v1 API, which
// speaks the OpenAI protocol directly (Bearer auth, model in the body).
func azureDeploymentsEndpoint(baseURL string) string {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u.Host == "" {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if !strings.HasSuffix(host, ".openai.azure.com") && !strings.HasSuffix(host, ".cognitiveservices.azure.com") {
		return ""
	}
	switch strings.TrimSuffix(u.Path, "/") {
	case "", "/openai":
		return u.Scheme + "://" + u.Host
	}
	return ""
}

// Analyze sends the bridge's preamble + (optional) skill systemPrefix as the
// System message, and userPayload as the User message. The OpenAI-compatible
// API has no equivalent of Anthropic's per-block cache_control — providers
// implementing it (e.g., OpenAI's automatic prompt cache, Azure model
// inference) do it transparently based on prompt similarity. Ollama and
// most self-hosted backends don't cache at all.
func (a *OpenAIAnalyzer) Analyze(ctx context.Context, systemPrefix, userPayload string) (string, error) {
	var sb strings.Builder
	sb.WriteString(baseSystemPrompt)
	if systemPrefix != "" {
		sb.WriteString("\n\n")
		sb.WriteString(systemPrefix)
	}

	params := openai.ChatCompletionNewParams{
		Model: openai.ChatModel(a.model),
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(sb.String()),
			openai.UserMessage(userPayload),
		},
	}
	if a.legacyMaxTokens {
		params.MaxTokens = openai.Int(a.maxTokens)
	} else {
		params.MaxCompletionTokens = openai.Int(a.maxTokens)
	}
	completion, err := a.client.Chat.Completions.New(ctx, params)
	if err != nil {
		// Classify 429 separately from generic LLM errors so quota-exhaustion
		// alerts don't false-positive on schema/parsing failures. The OpenAI
		// SDK has already retried twice with backoff (MaxRetries=2 default);
		// a bubbled 429 means quota, TPM, or RPM cap, not a transient blip.
		// "openai-compatible" covers every backend behind this analyzer
		// (Azure AI Foundry, Azure OpenAI Service, OpenAI direct, OpenRouter,
		// Ollama, vLLM) — the wire shape is the same.
		outcome := "error"
		var apiErr *openai.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusTooManyRequests {
			outcome = "rate_limited"
			metrics.LLMRateLimitHits.WithLabelValues("openai-compatible").Inc()
		}
		metrics.LLMRequests.WithLabelValues("openai-compatible", outcome).Inc()
		return "", fmt.Errorf("openai analyze: %w", err)
	}
	metrics.LLMRequests.WithLabelValues("openai-compatible", "success").Inc()

	// Token metrics. OpenAI-compatible APIs don't expose Anthropic-style cache
	// counters, so cache_read/cache_write stay at zero — that's fine.
	metrics.LLMInputTokens.Add(float64(completion.Usage.PromptTokens))
	metrics.LLMOutputTokens.Add(float64(completion.Usage.CompletionTokens))

	if len(completion.Choices) == 0 {
		return "", fmt.Errorf("openai returned no choices")
	}
	choice := completion.Choices[0]

	// OpenAI signals truncation via finish_reason="length".
	if string(choice.FinishReason) == "length" {
		metrics.LLMMaxTokensHit.Inc()
		slog.Warn("llm response truncated by max_tokens cap",
			"model", a.model,
			"max_tokens", a.maxTokens,
			"output_tokens", completion.Usage.CompletionTokens,
		)
	}

	// Guard empty/whitespace-only output (e.g. content_filter, or a model
	// that delivered everything via fields we don't read). Mirror the Claude
	// path: surface an error so the processor falls back to the kstatus-only
	// comment instead of posting a blank analysis to the PR.
	if strings.TrimSpace(choice.Message.Content) == "" {
		return "", fmt.Errorf("openai returned no text content")
	}
	return choice.Message.Content, nil
}
