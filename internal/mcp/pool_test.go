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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bridgemcp "github.com/staffbase/flux-ai-bridge/internal/mcp"
)

// poolMCPHandler returns an MCP-over-streamable-HTTP handler that counts
// how many sessions were initialized (one per "initialize" call) and serves
// `tools/call` with a static response.
func poolMCPHandler(initCount *int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			JSONRPC string           `json:"jsonrpc"`
			ID      *json.RawMessage `json:"id,omitempty"`
			Method  string           `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&msg)

		if msg.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		var result any
		switch msg.Method {
		case "initialize":
			atomic.AddInt64(initCount, 1)
			result = map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]any{"name": "pool-test", "version": "0.0.1"},
			}
		case "tools/call":
			// Simulate non-trivial work so concurrent calls have a chance to overlap.
			time.Sleep(10 * time.Millisecond)
			result = map[string]any{
				"content": []map[string]any{{"type": "text", "text": "ok"}},
				"isError": false,
			}
		case "tools/list":
			result = map[string]any{"tools": []any{}}
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

func TestPool_LazyGrow(t *testing.T) {
	var inits int64
	srv := httptest.NewServer(poolMCPHandler(&inits))
	defer srv.Close()

	pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	// NewPool dials one probe session at startup → 1 init.
	if got := atomic.LoadInt64(&inits); got != 1 {
		t.Fatalf("after NewPool, init count = %d, want 1", got)
	}

	// A single sequential call reuses the probe — no new sessions.
	if _, err := pool.GetKustomizationStatus(context.Background(), "n", "ns"); err != nil {
		t.Fatalf("call 1: %v", err)
	}
	if got := atomic.LoadInt64(&inits); got != 1 {
		t.Fatalf("after 1 sequential call, init count = %d, want 1 (reuse)", got)
	}
}

func TestPool_Concurrent(t *testing.T) {
	var inits int64
	srv := httptest.NewServer(poolMCPHandler(&inits))
	defer srv.Close()

	pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 4)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	const goroutines = 4
	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, err := pool.GetKustomizationStatus(context.Background(), "n", "ns")
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent call: %v", err)
		}
	}

	// 1 startup probe + up to 3 lazy growth (since the probe handles one slot).
	// Lower bound: 1 (everything serialized through probe — unlikely with 10ms sleep).
	// Upper bound: 4.
	got := atomic.LoadInt64(&inits)
	if got < 1 || got > 4 {
		t.Fatalf("init count after concurrent burst = %d, want 1..4", got)
	}
	if got == 1 {
		t.Logf("note: only 1 session created — concurrent calls did not overlap (timing-dependent)")
	}
}

// TestPool_RetriesOnStaleConnection reproduces the production failure where a
// pooled session's idle keep-alive connection is closed by the server, the
// reused POST fails with "rejected by transport: ... EOF", and net/http does
// not retry it. The pool must discard the dead session and retry the call on a
// fresh one, so the caller never sees the transport error.
func TestPool_RetriesOnStaleConnection(t *testing.T) {
	var inits, toolCalls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			JSONRPC string           `json:"jsonrpc"`
			ID      *json.RawMessage `json:"id,omitempty"`
			Method  string           `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&msg)

		if msg.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		writeResult := func(result any) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0", "id": msg.ID, "result": result,
			})
		}
		switch msg.Method {
		case "initialize":
			atomic.AddInt64(&inits, 1)
			writeResult(map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]any{"name": "pool-test", "version": "0.0.1"},
			})
		case "tools/call":
			// First call: simulate a stale keep-alive connection by hijacking
			// and closing without responding, so the client's POST sees EOF.
			if atomic.AddInt64(&toolCalls, 1) == 1 {
				hj, ok := w.(http.Hijacker)
				if !ok {
					t.Errorf("ResponseWriter is not a Hijacker")
					return
				}
				conn, _, err := hj.Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				_ = conn.Close()
				return
			}
			writeResult(map[string]any{
				"content": []map[string]any{{"type": "text", "text": "ok"}},
				"isError": false,
			})
		default:
			writeResult(map[string]any{})
		}
	}))
	defer srv.Close()

	pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 1)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	out, err := pool.GetKustomizationStatus(context.Background(), "n", "ns")
	if err != nil {
		t.Fatalf("call should have recovered via retry, got: %v", err)
	}
	if string(out) != "ok" {
		t.Fatalf("call result = %q, want %q", out, "ok")
	}
	// Probe (1) plus the fresh session dialed for the retry (2).
	if got := atomic.LoadInt64(&inits); got != 2 {
		t.Fatalf("init count = %d, want 2 (probe + retry session)", got)
	}
	if got := atomic.LoadInt64(&toolCalls); got != 2 {
		t.Fatalf("tools/call count = %d, want 2 (failed + retried)", got)
	}
}

func TestPool_PingAfterClose(t *testing.T) {
	var inits int64
	srv := httptest.NewServer(poolMCPHandler(&inits))
	defer srv.Close()

	pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 2)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After Close, further acquires must fail.
	if err := pool.Ping(context.Background()); err == nil {
		t.Fatal("expected Ping to fail after Close")
	}
}

// schemaMCPHandler serves initialize, tools/list (advertising a
// get_kubernetes_logs schema with the property names logProps returns) and
// tools/call. Like flux-operator-mcp v0.61 it rejects argument names the
// schema doesn't list (a tool error); accepted calls' arguments are recorded.
func schemaMCPHandler(logProps func() []string, calls chan<- map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			ID     *json.RawMessage `json:"id,omitempty"`
			Method string           `json:"method"`
			Params struct {
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&msg)
		if msg.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		var result any
		switch msg.Method {
		case "initialize":
			result = map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "schema-test", "version": "0.0.1"},
			}
		case "tools/list":
			props := map[string]any{}
			for _, p := range logProps() {
				props[p] = map[string]any{"type": "string"}
			}
			result = map[string]any{"tools": []any{map[string]any{
				"name":        "get_kubernetes_logs",
				"inputSchema": map[string]any{"type": "object", "properties": props},
			}}}
		case "tools/call":
			known := map[string]bool{}
			for _, p := range logProps() {
				known[p] = true
			}
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": "log line"}}}
			for k := range msg.Params.Arguments {
				if !known[k] {
					result = map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": `validating "arguments": unexpected property ` + k}}}
				}
			}
			if result.(map[string]any)["isError"] == nil {
				calls <- msg.Params.Arguments
			}
		default:
			result = map[string]any{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": result})
	}
}

// flux-operator-mcp v0.61 renamed get_kubernetes_logs' arguments and rejects
// unknown keys; the pool must send whichever names the server advertises.
func TestPool_LogArgsFollowServerSchema(t *testing.T) {
	for name, tc := range map[string]struct {
		props []string
		want  []string
	}{
		"v0.61+":  {[]string{"name", "namespace", "container", "previous"}, []string{"name", "namespace", "container", "previous"}},
		"<=v0.60": {[]string{"pod_name", "pod_namespace", "container_name", "previous"}, []string{"pod_name", "pod_namespace", "container_name", "previous"}},
	} {
		t.Run(name, func(t *testing.T) {
			calls := make(chan map[string]any, 4)
			srv := httptest.NewServer(schemaMCPHandler(func() []string { return tc.props }, calls))
			defer srv.Close()
			pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 1)
			if err != nil {
				t.Fatalf("NewPool: %v", err)
			}
			defer func() { _ = pool.Close(context.Background()) }()

			for range 2 { // second call uses the cached resolution
				if _, err := pool.GetKubernetesLogs(context.Background(), "web-1", "apps", "app", true); err != nil {
					t.Fatalf("GetKubernetesLogs: %v", err)
				}
				args := <-calls
				if len(args) != len(tc.want) {
					t.Fatalf("args = %v, want keys %v", args, tc.want)
				}
				for _, k := range tc.want {
					if _, ok := args[k]; !ok {
						t.Fatalf("args = %v, missing %q", args, k)
					}
				}
			}
		})
	}
}

// A server changed across the v0.61 rename while the bridge runs (upgraded,
// or a rolling update answered the schema lookup from an old pod) rejects the
// cached argument names: the next log call re-reads the schema and succeeds.
func TestPool_LogArgsReResolvedAfterServerChange(t *testing.T) {
	legacy := []string{"pod_name", "pod_namespace", "container_name", "previous"}
	current := []string{"name", "namespace", "container", "previous"}
	var props atomic.Pointer[[]string]
	props.Store(&legacy)
	calls := make(chan map[string]any, 8)
	srv := httptest.NewServer(schemaMCPHandler(func() []string { return *props.Load() }, calls))
	defer srv.Close()
	pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 1)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	if _, err := pool.GetKubernetesLogs(context.Background(), "web-1", "apps", "app", false); err != nil {
		t.Fatalf("GetKubernetesLogs against the old server: %v", err)
	}
	<-calls
	props.Store(&current)
	if _, err := pool.GetKubernetesLogs(context.Background(), "web-1", "apps", "app", false); err != nil {
		t.Fatalf("GetKubernetesLogs after the server changed: %v", err)
	}
	if args := <-calls; args["name"] != "web-1" {
		t.Fatalf("retry used %v, want the server's new argument names", args)
	}
}

// /readyz must not queue behind real work: with every session busy, Ping
// reports healthy at once instead of timing out — but only while MCP calls
// keep succeeding.
func TestPool_PingDoesNotWaitForBusyPool(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			ID     *json.RawMessage `json:"id,omitempty"`
			Method string           `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&msg)
		if msg.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		var result any = map[string]any{}
		switch msg.Method {
		case "initialize":
			result = map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "busy-test", "version": "0.0.1"},
			}
		case "tools/call":
			started <- struct{}{}
			<-release
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": "ok"}}}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": result})
	}))
	defer srv.Close()
	pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 1)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = pool.GetKustomizationStatus(context.Background(), "n", "ns")
	}()
	// Unblock the held call however the test ends, or srv.Close hangs on it.
	defer func() {
		close(release)
		<-done
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping with a busy pool = %v, want nil without waiting", err)
	}
	// Every session stuck for longer than the freshness window: an
	// unresponsive server must not keep the bridge looking ready.
	bridgemcp.SetBusyFreshness(pool, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if err := pool.Ping(ctx); err == nil {
		t.Fatal("Ping with every session stuck past the freshness window = nil, want an error")
	}
}

// A response the SDK can't decode kills that session's connection. The pool
// must discard it rather than hand the dead session to the next caller —
// with a pool of one, that left every later call (and /readyz) failing.
func TestPool_DiscardsSessionAfterUndecodableResponse(t *testing.T) {
	var inits, toolCalls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			ID     *json.RawMessage `json:"id,omitempty"`
			Method string           `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&msg)
		if msg.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		var result any = map[string]any{}
		switch msg.Method {
		case "initialize":
			atomic.AddInt64(&inits, 1)
			result = map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "garbage-test", "version": "0.0.1"},
			}
		case "tools/call":
			if atomic.AddInt64(&toolCalls, 1) == 1 {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {not json\n\n"))
				return
			}
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": "ok"}}}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": result})
	}))
	defer srv.Close()
	pool, err := bridgemcp.NewPool(context.Background(), srv.URL, 1)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, firstErr := pool.GetKustomizationStatus(ctx, "n", "ns")
	out, err := pool.GetKustomizationStatus(ctx, "n", "ns")
	if err != nil {
		t.Fatalf("call after an undecodable response must succeed on a fresh session (first err: %v): %v", firstErr, err)
	}
	if string(out) != "ok" {
		t.Fatalf("out = %q, want ok", out)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping after recovery: %v", err)
	}
}
