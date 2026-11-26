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

package github_test

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// appIdentityMux serves GET /app (answered by appHandler) and a PR whose
// comments carry the same scoped marker from the bridge's App and from another
// App. It records the IDs of deleted comments.
func appIdentityMux(t *testing.T, appHandler http.HandlerFunc) (*http.ServeMux, func() []int64) {
	t.Helper()
	var (
		mu      sync.Mutex
		deleted []int64
	)
	marker := "<!-- flux-ai-bridge:flux-system/podinfo -->\n"
	comments := []any{
		map[string]any{"id": 98, "body": marker + "copied by another app",
			"user": map[string]any{"login": "other-app[bot]", "type": "Bot"}},
		map[string]any{"id": 99, "body": marker + "old text",
			"user": map[string]any{"login": "flux-ai-bridge[bot]", "type": "Bot"}},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/app", appHandler)
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(comments)
		case http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 100, "body": "new"})
		}
	})
	for _, id := range []int64{98, 99} {
		mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/"+strconv.FormatInt(id, 10), func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete {
				mu.Lock()
				deleted = append(deleted, id)
				mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
			}
		})
	}
	return mux, func() []int64 {
		mu.Lock()
		defer mu.Unlock()
		return slices.Sorted(slices.Values(deleted))
	}
}

// With the App's identity known, a comment from another App that copied the
// bridge's marker is not the bridge's: replacing the bridge's comment must
// leave it alone. GET /app is called with the App JWT, not the installation
// token (the endpoint only accepts the JWT).
func TestResolveAppIdentity_OtherAppsCommentIsNotTheBridges(t *testing.T) {
	var auth string
	mux, deleted := appIdentityMux(t, func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 12345, "slug": "flux-ai-bridge", "name": "flux-ai-bridge"})
	})
	c := newCommenter(t, mux)
	ctx := context.Background()

	if err := c.ResolveAppIdentity(ctx); err != nil {
		t.Fatalf("ResolveAppIdentity: %v", err)
	}
	if !strings.HasPrefix(auth, "Bearer ") {
		t.Errorf("GET /app Authorization = %q, want the App JWT (Bearer)", auth)
	}
	if got := c.BotLogin(); got != "flux-ai-bridge[bot]" {
		t.Errorf("BotLogin = %q, want flux-ai-bridge[bot]", got)
	}
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "flux-system", "podinfo", 42, "new text"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}
	if got := deleted(); !slices.Equal(got, []int64{99}) {
		t.Errorf("deleted comments = %v, want only the bridge's [99]", got)
	}
}

// While the App's identity is unknown (GET /app keeps failing), the bridge
// can't tell its own marker comments from another App's copies, so it edits
// and deletes none: the post fails and the event retries.
func TestResolveAppIdentity_UnknownIdentityTouchesNoComments(t *testing.T) {
	mux, deleted := appIdentityMux(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
	})
	c := newCommenter(t, mux)
	ctx := context.Background()

	if err := c.ResolveAppIdentity(ctx); err == nil {
		t.Fatal("ResolveAppIdentity: want an error for a failed GET /app")
	}
	if got := c.BotLogin(); got != "" {
		t.Errorf("BotLogin after a failed lookup = %q, want empty", got)
	}
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "flux-system", "podinfo", 42, "new text"); err == nil {
		t.Fatal("PostOrUpdateComment with the App identity unknown returned nil; want an error so the event retries")
	}
	if err := c.RemoveComment(ctx, "owner", "repo", "flux-system", "podinfo", 42); err == nil {
		t.Fatal("RemoveComment with the App identity unknown returned nil; want an error")
	}
	if got := deleted(); len(got) != 0 {
		t.Errorf("deleted comments = %v, want none while the App identity is unknown", got)
	}
}

// A failed startup lookup is not final: the next comment operation retries
// GET /app, and once it answers, only the bridge's own comment is replaced.
func TestResolveAppIdentity_RetriedOnUse(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	mux, deleted := appIdentityMux(t, func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			http.Error(w, `{"message":"boom"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 12345, "slug": "flux-ai-bridge"})
	})
	c := newCommenter(t, mux)
	ctx := context.Background()
	if err := c.ResolveAppIdentity(ctx); err == nil {
		t.Fatal("ResolveAppIdentity: want an error for a failed GET /app")
	}

	fail.Store(false)
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "flux-system", "podinfo", 42, "new text"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}
	if got := c.BotLogin(); got != "flux-ai-bridge[bot]" {
		t.Errorf("BotLogin after the retried lookup = %q, want flux-ai-bridge[bot]", got)
	}
	if got := deleted(); !slices.Equal(got, []int64{99}) {
		t.Errorf("deleted comments = %v, want only the bridge's [99]", got)
	}
}

// An App response without a slug can't name the bridge's login: treat it as a
// failed lookup rather than matching "[bot]".
func TestResolveAppIdentity_EmptySlugIsAnError(t *testing.T) {
	mux, _ := appIdentityMux(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 12345})
	})
	c := newCommenter(t, mux)
	if err := c.ResolveAppIdentity(context.Background()); err == nil {
		t.Fatal("ResolveAppIdentity: want an error for an App without a slug")
	}
	if got := c.BotLogin(); got != "" {
		t.Errorf("BotLogin = %q, want empty", got)
	}
}
