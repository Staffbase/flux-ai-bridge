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
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/staffbase/flux-ai-bridge/internal/config"
)

// redirectingGateway answers every request with a 302 to target, the way a
// misconfigured or compromised LLM gateway could.
func redirectingGateway(t *testing.T, target string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// keyCatcher counts the requests that reach another origin.
func keyCatcher(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// A redirect to another origin must not be followed: Go keeps X-Api-Key on a
// cross-domain redirect, so following it hands the Anthropic key to that host.
func TestClaudeAnalyzer_RefusesCrossOriginRedirect(t *testing.T) {
	other, hits := keyCatcher(t)
	gw := redirectingGateway(t, other.URL)

	a := NewClaudeAnalyzer(&config.Config{AnthropicAPIKey: "secret", AnthropicBaseURL: gw.URL, LLMModel: "claude-test"})
	if _, err := a.Analyze(context.Background(), "", "hello"); err == nil {
		t.Fatal("Analyze followed a cross-origin redirect without error")
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("other origin received %d request(s) carrying the API key, want 0", n)
	}
}

// The OpenAI SDK refuses cross-origin requests in its own transport; this
// pins that an SDK upgrade keeps doing so.
func TestOpenAIAnalyzer_RefusesCrossOriginRedirect(t *testing.T) {
	other, hits := keyCatcher(t)
	gw := redirectingGateway(t, other.URL)

	a := NewOpenAIAnalyzer(&config.Config{OpenAIAPIKey: "secret", OpenAIBaseURL: gw.URL, LLMModel: "gpt-test"})
	if _, err := a.Analyze(context.Background(), "", "hello"); err == nil {
		t.Fatal("Analyze followed a cross-origin redirect without error")
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("other origin received %d request(s), want 0", n)
	}
}

func TestCheckSameOriginRedirect(t *testing.T) {
	req := func(raw string) *http.Request {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		return &http.Request{URL: u}
	}
	from := []*http.Request{req("https://llm.example.com/v1/messages")}
	for target, wantOK := range map[string]bool{
		"https://llm.example.com/v2/messages": true,  // same origin, new path
		"https://LLM.example.com/v1/messages": true,  // host case differs only
		"http://llm.example.com/v1/messages":  false, // HTTPS-to-HTTP downgrade
		"https://other.example.com/v1":        false, // another host
		"https://llm.example.com:8443/v1":     false, // another port
	} {
		err := checkSameOriginRedirect(req(target), from)
		if gotOK := err == nil; gotOK != wantOK {
			t.Errorf("redirect to %s: err = %v, want allowed=%v", target, err, wantOK)
		}
	}
}
