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
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/fetchcache"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
)

// flakyCache is an in-memory cache whose Delete and Put can be made to fail,
// standing for a Redis that errors between two commands.
type flakyCache struct {
	fetchcache.Cache
	failDelete atomic.Bool
	failPut    atomic.Bool
}

func (c *flakyCache) Delete(ctx context.Context, key string) error {
	if c.failDelete.Load() {
		return errors.New("backend unavailable")
	}
	return c.Cache.Delete(ctx, key)
}

func (c *flakyCache) Put(ctx context.Context, key string, value []byte, ttl time.Duration) {
	if c.failPut.Load() {
		return
	}
	c.Cache.Put(ctx, key, value, ttl)
}

// If the comment record can't be dropped before a swap, the swap must not
// run: with the old record left in place, a failed delete of the superseded
// comment leaves two verdicts on the PR, and a later post of the old state
// trusts the record, skips, and never sweeps the newer comment.
func TestPostOrUpdateComment_UnconfirmedRecordDropSkipsTheSwap(t *testing.T) {
	ctx := context.Background()
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	cache := &flakyCache{Cache: fetchcache.NewLRU(64)}
	c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: cache})
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "state A"); err != nil {
		t.Fatalf("post A: %v", err)
	}

	cache.failDelete.Store(true)
	srv.mu.Lock()
	srv.failDel = true
	srv.mu.Unlock()
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "state B"); err == nil {
		t.Error("post B with an unconfirmed record drop returned nil; want an error so the event retries")
	}

	cache.failDelete.Store(false)
	srv.mu.Lock()
	srv.failDel = false
	srv.mu.Unlock()
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "state A"); err != nil {
		t.Fatalf("post A again: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.live) != 1 {
		t.Fatalf("want one comment (state A), got %d: %v", len(srv.live), srv.live)
	}
	for _, body := range srv.live {
		if !strings.HasSuffix(body, "state A") {
			t.Fatalf("live comment = %q, want state A", body)
		}
	}
}

// A removal whose "no comment" record doesn't land must not leave the old
// record behind: a later post of the removed state would trust it and skip,
// leaving the PR with no comment at all.
func TestRemoveComment_UnrecordedRemovalDoesNotStrandTheOldRecord(t *testing.T) {
	ctx := context.Background()
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	cache := &flakyCache{Cache: fetchcache.NewLRU(64)}
	c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: cache})
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "failing"); err != nil {
		t.Fatalf("post: %v", err)
	}

	cache.failPut.Store(true)
	if err := c.RemoveComment(ctx, "owner", "repo", "apps", "api", 42); err != nil {
		t.Fatalf("RemoveComment: %v", err)
	}
	cache.failPut.Store(false)

	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "failing"); err != nil {
		t.Fatalf("re-post: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.live) != 1 {
		t.Fatalf("want the re-posted comment on the PR, got %d comments", len(srv.live))
	}
}

// labelState is a PR's label set behind the issue-labels API. failRemove
// makes every DELETE fail with a 502.
type labelState struct {
	mu         sync.Mutex
	labels     map[string]bool
	failRemove bool
}

func (s *labelState) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		var add []string
		_ = json.NewDecoder(r.Body).Decode(&add)
		s.mu.Lock()
		for _, l := range add {
			s.labels[l] = true
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		name, _ := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/api/v3/repos/owner/repo/issues/42/labels/"))
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.failRemove {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if !s.labels[name] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		delete(s.labels, name)
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func (s *labelState) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for l := range s.labels {
		out = append(out, l)
	}
	sort.Strings(out)
	return out
}

// If the outcome record can't be dropped before the labels change, the
// labels must not change: a remove that then fails leaves both labels on the
// PR, and a later reconcile of the old outcome trusts the record and skips,
// so both stay.
func TestSetPRLabels_UnconfirmedRecordDropSkipsTheReconcile(t *testing.T) {
	ctx := context.Background()
	state := &labelState{labels: map[string]bool{}}
	cache := &flakyCache{Cache: fetchcache.NewLRU(64)}
	c := newCommenterWithOptions(t, state.mux(), bridgegithub.Options{Cache: cache})
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("success: %v", err)
	}

	cache.failDelete.Store(true)
	state.mu.Lock()
	state.failRemove = true
	state.mu.Unlock()
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure); err == nil {
		t.Error("failure reconcile with an unconfirmed record drop returned nil")
	}

	cache.failDelete.Store(false)
	state.mu.Lock()
	state.failRemove = false
	state.mu.Unlock()
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("success again: %v", err)
	}
	if got := state.names(); len(got) != 1 || got[0] != "flux-success" {
		t.Fatalf("labels = %v, want [flux-success]", got)
	}
}
