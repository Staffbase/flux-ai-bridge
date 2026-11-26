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
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
)

const baseSystemPrompt = "You are a Kubernetes GitOps expert writing concise, actionable GitHub PR comments for Flux deployment events."

const defaultMaxTokens = 2048

// requestTimeout caps one Messages API attempt. Setting it explicitly also
// keeps the SDK from refusing non-streaming requests whose max_tokens implies
// a >10-minute generation (anything above ~21k, or a model's non-streaming
// limit) — without it every call with such an LLM_MAX_TOKENS fails before
// leaving the process. The pipeline's own deadline is far shorter anyway.
const requestTimeout = 10 * time.Minute

type ClaudeAnalyzer struct {
	client    anthropic.Client
	model     string
	maxTokens int64
}

func NewClaudeAnalyzer(cfg *config.Config) *ClaudeAnalyzer {
	opts := []option.RequestOption{
		option.WithAPIKey(cfg.AnthropicAPIKey),
		option.WithRequestTimeout(requestTimeout),
		option.WithHTTPClient(sameOriginHTTPClient()),
	}
	if cfg.AnthropicBaseURL != "" {
		opts = append(opts, option.WithBaseURL(cfg.AnthropicBaseURL))
	}
	max := int64(cfg.LLMMaxTokens)
	if max <= 0 {
		max = defaultMaxTokens
	}
	return &ClaudeAnalyzer{client: anthropic.NewClient(opts...), model: cfg.LLMModel, maxTokens: max}
}

// Analyze sends two blocks as the System role:
//
//  1. The bridge's hardcoded preamble (~20 tokens, constant for the bridge's lifetime).
//  2. systemPrefix from the caller — typically the skill methodology + reference
//     (~3-10k tokens, constant across all events handled by this pod since
//     skills load once at startup).
//
// Both get cache_control: ephemeral so Anthropic caches them; cached reads
// cost ~10% of normal input. userPayload is the per-event content, sent as
// the User message and NEVER cached (it changes every call).
//
// Reference: https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching
func (a *ClaudeAnalyzer) Analyze(ctx context.Context, systemPrefix, userPayload string) (string, error) {
	systemBlocks := []anthropic.TextBlockParam{
		{Text: baseSystemPrompt},
	}
	if systemPrefix != "" {
		systemBlocks = append(systemBlocks, anthropic.TextBlockParam{
			Text: systemPrefix,
			CacheControl: anthropic.CacheControlEphemeralParam{
				Type: "ephemeral",
			},
		})
	} else {
		// Even without a skill, cache the bridge's hardcoded preamble — same
		// reasoning, smaller win.
		systemBlocks[0].CacheControl = anthropic.CacheControlEphemeralParam{Type: "ephemeral"}
	}

	message, err := a.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     anthropic.Model(a.model),
		MaxTokens: a.maxTokens,
		System:    systemBlocks,
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(userPayload)),
		},
	})
	if err != nil {
		// Classify 429 separately from generic LLM errors so quota-exhaustion
		// alerts don't false-positive on "model returned malformed JSON" etc.
		// The Anthropic SDK has already retried twice with backoff at this
		// point (its default MaxRetries=2); a bubbled 429 means quota or TPM
		// cap, not a transient blip.
		outcome := "error"
		var apiErr *anthropic.Error
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusTooManyRequests {
			outcome = "rate_limited"
			metrics.LLMRateLimitHits.WithLabelValues("claude").Inc()
		}
		metrics.LLMRequests.WithLabelValues("claude", outcome).Inc()
		return "", fmt.Errorf("claude analyze: %w", err)
	}
	metrics.LLMRequests.WithLabelValues("claude", "success").Inc()

	// Token metrics. Anthropic's usage struct also exposes prompt-cache
	// counters (CacheReadInputTokens/CacheCreationInputTokens) — those
	// used to feed two dedicated Prometheus counters, but they were
	// dropped to reduce the scrape surface (~55% series cut) since
	// nothing in docs or runbooks referenced them. If cache-effectiveness
	// observability comes back as a need, the usage fields are still
	// available on `message.Usage` for ad-hoc logging.
	metrics.LLMInputTokens.Add(float64(message.Usage.InputTokens))
	metrics.LLMOutputTokens.Add(float64(message.Usage.OutputTokens))

	// Truncation detection — Anthropic sets stop_reason to "max_tokens" when
	// the response was cut off at our cap. Visible only via this counter +
	// the WARN log; the response itself looks normal.
	if string(message.StopReason) == "max_tokens" {
		metrics.LLMMaxTokensHit.Inc()
		slog.Warn("llm response truncated by max_tokens cap",
			"model", a.model,
			"max_tokens", a.maxTokens,
			"output_tokens", message.Usage.OutputTokens,
		)
	}

	var sb strings.Builder
	for _, block := range message.Content {
		if text := strings.TrimSpace(block.Text); text != "" {
			if sb.Len() > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(block.Text)
		}
	}
	if sb.Len() == 0 {
		return "", fmt.Errorf("claude returned no text content")
	}
	return sb.String(), nil
}
