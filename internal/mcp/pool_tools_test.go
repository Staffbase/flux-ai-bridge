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

package mcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	bridgemcp "github.com/staffbase/flux-ai-bridge/internal/mcp"
)

type rpcMsg struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method"`
	Params  json.RawMessage  `json:"params,omitempty"`
}

func mcpHandler(toolText map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var msg rpcMsg
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		if msg.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}

		var result any
		switch msg.Method {
		case "initialize":
			result = map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]any{"name": "test-mcp", "version": "0.0.1"},
			}
		case "tools/call":
			var params struct {
				Name string `json:"name"`
			}
			_ = json.Unmarshal(msg.Params, &params)
			text := toolText[params.Name]
			result = map[string]any{
				"content": []map[string]any{{"type": "text", "text": text}},
				"isError": false,
			}
		default:
			result = map[string]any{}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      msg.ID,
			"result":  result,
		})
	}
}

func newTestPool(t *testing.T, responses map[string]string) *bridgemcp.Pool {
	t.Helper()
	srv := httptest.NewServer(mcpHandler(responses))
	t.Cleanup(srv.Close)
	c, err := bridgemcp.NewPool(context.Background(), srv.URL, 1)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	// Cleanups run last-in first-out, so the pool's sessions close before
	// the server does. Close is idempotent and bounds itself (5s).
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestNewPool_Connect(t *testing.T) {
	_ = newTestPool(t, nil)
}

func TestNewPool_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srv.Close()
	_, err := bridgemcp.NewPool(context.Background(), srv.URL, 1)
	if err == nil {
		t.Fatal("expected error when server returns 500")
	}
}

func TestGetKustomizationStatus(t *testing.T) {
	expected := `{"metadata":{"name":"test-ks"}}`
	c := newTestPool(t, map[string]string{
		"get_kubernetes_resources": expected,
	})
	result, err := c.GetKustomizationStatus(context.Background(), "test-ks", "flux-system")
	if err != nil {
		t.Fatalf("GetKustomizationStatus: %v", err)
	}
	if string(result) != expected {
		t.Errorf("result = %q, want %q", string(result), expected)
	}
}

func TestGetKubernetesLogs(t *testing.T) {
	expected := "pod log line 1\npod log line 2\n"
	c := newTestPool(t, map[string]string{
		"get_kubernetes_logs": expected,
	})
	result, err := c.GetKubernetesLogs(context.Background(), "test-pod", "flux-system", "test-container", false)
	if err != nil {
		t.Fatalf("GetKubernetesLogs: %v", err)
	}
	if result != expected {
		t.Errorf("result = %q, want %q", result, expected)
	}
}

func TestGetKubernetesEvents(t *testing.T) {
	c := newTestPool(t, map[string]string{
		"get_kubernetes_events": "namespace: flux-system\ntotal: 2\ntruncated: false\nevents: |\n  2026-01-01T00:00:00Z Warning BackOff Pod/flux-system/app-abc123 Back-off restarting failed container\n  2026-01-01T00:00:00Z Normal Pulled Pod/flux-system/app-abc123 Successfully pulled image\n",
	})
	result, truncated, err := c.GetKubernetesEvents(context.Background(), "Deployment", "app", "flux-system", 10*time.Minute, 5)
	if err != nil {
		t.Fatalf("GetKubernetesEvents: %v", err)
	}
	if truncated {
		t.Errorf("truncated = true, want false")
	}
	const want = "2026-01-01T00:00:00Z Warning BackOff Pod/flux-system/app-abc123 Back-off restarting failed container\n" +
		"2026-01-01T00:00:00Z Normal Pulled Pod/flux-system/app-abc123 Successfully pulled image"
	if result != want {
		t.Errorf("result = %q, want %q", result, want)
	}
}

func TestGetKubernetesEvents_Truncated(t *testing.T) {
	c := newTestPool(t, map[string]string{
		"get_kubernetes_events": "namespace: flux-system\ntotal: 500\ntruncated: true\nevents: |\n  2026-01-01T00:00:00Z Warning BackOff Pod/flux-system/app-abc123 Back-off restarting failed container\n",
	})
	_, truncated, err := c.GetKubernetesEvents(context.Background(), "Deployment", "app", "flux-system", 0, 5)
	if err != nil {
		t.Fatalf("GetKubernetesEvents: %v", err)
	}
	if !truncated {
		t.Errorf("truncated = false, want true")
	}
}

func TestGetKubernetesEvents_NoneFound(t *testing.T) {
	c := newTestPool(t, map[string]string{
		"get_kubernetes_events": "No events found",
	})
	result, truncated, err := c.GetKubernetesEvents(context.Background(), "Pod", "app-abc123", "flux-system", 0, 0)
	if err != nil {
		t.Fatalf("GetKubernetesEvents: %v", err)
	}
	if result != "" {
		t.Errorf("result = %q, want empty", result)
	}
	if truncated {
		t.Errorf("truncated = true, want false")
	}
}

func TestClose(t *testing.T) {
	c := newTestPool(t, nil)
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
