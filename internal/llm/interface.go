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
	"fmt"

	"github.com/staffbase/flux-ai-bridge/internal/config"
)

// Analyzer asks the LLM to produce a PR comment from two pieces:
//
//   - systemPrefix is the static portion of the prompt (the bridge's
//     hardcoded "you are a GitOps expert" preamble + the inlined SKILL.md
//     methodology + the chosen reference). It's byte-identical across calls
//     within a pod lifetime, so providers that support prompt caching
//     (Anthropic) can mark it as cacheable and serve it at ~10% input cost
//     on every call after the first.
//
//   - userPayload is the per-event content (Flux event, kustomization
//     status, resource readiness, optional logs/events for failures). This
//     is NOT cached.
type Analyzer interface {
	Analyze(ctx context.Context, systemPrefix, userPayload string) (string, error)
}

func NewAnalyzer(cfg *config.Config) (Analyzer, error) {
	switch cfg.LLMProvider {
	case "claude":
		return NewClaudeAnalyzer(cfg), nil
	case "openai-compatible":
		return NewOpenAIAnalyzer(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported llm provider %q", cfg.LLMProvider)
	}
}
