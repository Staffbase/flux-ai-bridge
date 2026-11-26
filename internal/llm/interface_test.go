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
	"testing"

	"github.com/staffbase/flux-ai-bridge/internal/config"
)

func TestNewAnalyzer_Claude(t *testing.T) {
	cfg := &config.Config{
		LLMProvider:     "claude",
		AnthropicAPIKey: "sk-ant-test",
		LLMModel:        "claude-test-model",
	}
	a, err := NewAnalyzer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := a.(*ClaudeAnalyzer); !ok {
		t.Errorf("NewAnalyzer(claude) returned %T, want *ClaudeAnalyzer", a)
	}
}

func TestNewAnalyzer_OpenAI(t *testing.T) {
	cfg := &config.Config{
		LLMProvider:  "openai-compatible",
		OpenAIAPIKey: "ghp_test",
		LLMModel:     "gpt-4.1",
	}
	a, err := NewAnalyzer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := a.(*OpenAIAnalyzer); !ok {
		t.Errorf("NewAnalyzer(openai-compatible) returned %T, want *OpenAIAnalyzer", a)
	}
}

func TestNewAnalyzer_Unknown(t *testing.T) {
	cfg := &config.Config{LLMProvider: "openai"}
	_, err := NewAnalyzer(cfg)
	if err == nil {
		t.Fatal("expected error for unknown provider, got nil")
	}
}

func TestNewAnalyzer_Empty(t *testing.T) {
	cfg := &config.Config{LLMProvider: ""}
	_, err := NewAnalyzer(cfg)
	if err == nil {
		t.Fatal("expected error for empty provider, got nil")
	}
}

func TestAzureDeploymentsEndpoint(t *testing.T) {
	for baseURL, want := range map[string]string{
		"https://res.openai.azure.com":                 "https://res.openai.azure.com",
		"https://res.openai.azure.com/":                "https://res.openai.azure.com",
		"https://RES.OpenAI.Azure.com/openai":          "https://RES.OpenAI.Azure.com",
		"https://res.cognitiveservices.azure.com/":     "https://res.cognitiveservices.azure.com",
		"https://res.openai.azure.com/openai/v1":       "", // OpenAI-compatible v1 API
		"https://res.openai.azure.com/openai/v1/":      "",
		"https://models.inference.ai.azure.com":        "", // AI Foundry / GitHub Models
		"https://api.openai.com/v1":                    "",
		"http://ollama.llm.svc.cluster.local:11434/v1": "",
		"https://evil.example.com/?.openai.azure.com":  "", // substring trap
		"": "",
	} {
		if got := azureDeploymentsEndpoint(baseURL); got != want {
			t.Errorf("azureDeploymentsEndpoint(%q) = %q, want %q", baseURL, got, want)
		}
	}
}
