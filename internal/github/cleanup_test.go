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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/fetchcache"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
)

// mapCache is a plain in-memory cache whose contents a test can copy, e.g. to
// stand for what a shared Redis holds when a replica dies.
type mapCache struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMapCache(m map[string][]byte) *mapCache {
	if m == nil {
		m = map[string][]byte{}
	}
	return &mapCache{m: m}
}

func (c *mapCache) Get(_ context.Context, key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[key]
	return v, ok
}

func (c *mapCache) Put(_ context.Context, key string, value []byte, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = value
}

func (c *mapCache) Delete(_ context.Context, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, key)
}

// TryLock always grants the lock, like the in-memory backend.
func (c *mapCache) TryLock(context.Context, string, time.Duration) (func(), bool, error) {
	return func() {}, true, nil
}

func (c *mapCache) snapshot() map[string][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][]byte, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}

// commentServer is a tiny issue-comments API: list, create, delete by ID.
// onDelete, when set, runs (under mu) as a DELETE arrives, before it applies.
type commentServer struct {
	mu        sync.Mutex
	live      map[int64]string
	nextID    int64
	deletes   []int64
	failDel   bool
	listCalls int
	onDelete  func()
}

func (s *commentServer) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			s.listCalls++
			var out []map[string]any
			for id, body := range s.live {
				out = append(out, map[string]any{"id": float64(id), "body": body})
			}
			_ = json.NewEncoder(w).Encode(out)
		case http.MethodPost:
			var in struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&in)
			id := s.nextID
			s.nextID++
			s.live[id] = in.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(id), "body": in.Body})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			return
		}
		id, _ := strconv.ParseInt(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], 10, 64)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.onDelete != nil {
			s.onDelete()
		}
		if s.failDel {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		s.deletes = append(s.deletes, id)
		delete(s.live, id)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// A comment ID that left the cache (in-memory restart, or the Redis backstop
// expiring it) must not strand a stale verdict: RemoveComment finds the
// scope's comment by listing once, deletes only that one, and records "no
// comment" so later healthy events cost no API call. A later post replaces
// that record, and removal goes back to deleting by ID.
func TestRemoveComment_ColdCacheFindsScopedComment(t *testing.T) {
	ctx := context.Background()
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	poster := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	if err := poster.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "failing"); err != nil {
		t.Fatalf("post api: %v", err)
	}
	if err := poster.PostOrUpdateComment(ctx, "owner", "repo", "apps", "web", 42, "other scope"); err != nil {
		t.Fatalf("post web: %v", err)
	}

	c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: fetchcache.NewLRU(64)}) // cache lost
	if err := c.RemoveComment(ctx, "owner", "repo", "apps", "api", 42); err != nil {
		t.Fatalf("RemoveComment: %v", err)
	}
	srv.mu.Lock()
	if len(srv.live) != 1 {
		t.Fatalf("want only the other scope's comment left, got %v", srv.live)
	}
	for _, body := range srv.live {
		if !strings.HasSuffix(body, "other scope") {
			t.Fatalf("removed the wrong scope's comment; left: %q", body)
		}
	}
	lists, deletes := srv.listCalls, len(srv.deletes)
	srv.mu.Unlock()

	if err := c.RemoveComment(ctx, "owner", "repo", "apps", "api", 42); err != nil {
		t.Fatalf("second RemoveComment: %v", err)
	}
	srv.mu.Lock()
	if srv.listCalls != lists || len(srv.deletes) != deletes {
		t.Fatalf("a recorded 'no comment' must skip the lookup: lists %d→%d, deletes %d→%d",
			lists, srv.listCalls, deletes, len(srv.deletes))
	}
	srv.mu.Unlock()

	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "failing again"); err != nil {
		t.Fatalf("re-post: %v", err)
	}
	srv.mu.Lock()
	lists = srv.listCalls
	srv.mu.Unlock()
	if err := c.RemoveComment(ctx, "owner", "repo", "apps", "api", 42); err != nil {
		t.Fatalf("RemoveComment after re-post: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.live) != 1 || srv.listCalls != lists {
		t.Fatalf("after a re-post, removal must delete by the recorded ID without listing: live=%v lists %d→%d", srv.live, lists, srv.listCalls)
	}
}

// A cancellation landing after the create of a swap (a shutdown
// mid-delivery) must not leave the superseded comment on the PR, and the
// fresh comment is still recorded.
func TestPostOrUpdateComment_SwapCleanupSurvivesCancellation(t *testing.T) {
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	cache := fetchcache.NewLRU(64)
	c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: cache})

	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, "state A"); err != nil {
		t.Fatalf("first post: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.mu.Lock()
	// Cancel while the predecessor's DELETE is in flight, and hold the
	// response long enough that a request bound to the cancelled context
	// would fail instead of completing.
	srv.onDelete = func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}
	srv.mu.Unlock()
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "state B"); err != nil {
		t.Fatalf("swap: %v", err)
	}

	srv.mu.Lock()
	srv.onDelete = nil
	if len(srv.live) != 1 {
		t.Fatalf("PR must keep exactly one bridge comment after the swap, got %d: %v", len(srv.live), srv.live)
	}
	for _, body := range srv.live {
		if !strings.HasSuffix(body, "state B") {
			t.Fatalf("surviving comment is the superseded one: %q", body)
		}
	}
	lists := srv.listCalls
	srv.mu.Unlock()
	if !c.CommentCurrent(context.Background(), "owner", "repo", "apps", "api", 42, "state B") {
		t.Fatalf("the fresh comment must be recorded after the cleanup (list calls so far: %d)", lists)
	}
}

// A replica that dies between creating the fresh comment and deleting its
// predecessor must not leave a cache record saying the swap finished: the next
// post (from any replica sharing the cache) must sweep the leftover instead of
// skipping the unchanged state.
func TestPostOrUpdateComment_CrashMidSwapIsSweptByNextPost(t *testing.T) {
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	cache := newMapCache(nil)
	c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: cache})
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, "state A"); err != nil {
		t.Fatalf("first post: %v", err)
	}

	// Capture what the shared cache and the PR hold at the moment the
	// predecessor's DELETE goes out — the state a crash right then leaves.
	var cacheAtCrash map[string][]byte
	var liveAtCrash map[int64]string
	srv.mu.Lock()
	srv.onDelete = func() {
		cacheAtCrash = cache.snapshot()
		liveAtCrash = make(map[int64]string, len(srv.live))
		for id, body := range srv.live {
			liveAtCrash[id] = body
		}
	}
	srv.mu.Unlock()
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, "state B"); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if len(liveAtCrash) != 2 {
		t.Fatalf("want both comments live when the DELETE goes out, got %v", liveAtCrash)
	}

	// Restart: the PR still has both comments, the cache what it had then.
	after := &commentServer{live: liveAtCrash, nextID: 100}
	restarted := newCommenterWithOptions(t, after.mux(), bridgegithub.Options{Cache: newMapCache(cacheAtCrash)})
	if err := restarted.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, "state B"); err != nil {
		t.Fatalf("post after restart: %v", err)
	}
	after.mu.Lock()
	defer after.mu.Unlock()
	if len(after.live) != 1 {
		t.Fatalf("the superseded comment survived the restart: %d comments left: %v", len(after.live), after.live)
	}
	for _, body := range after.live {
		if !strings.HasSuffix(body, "state B") {
			t.Fatalf("surviving comment is the superseded one: %q", body)
		}
	}
}

// When the cache is cold and the newest comment is already current, a failed
// delete of an older duplicate must not fail the delivery (the verdict is
// live) — but the cache stays cold, so the next post re-lists and sweeps.
func TestPostOrUpdateComment_CurrentCommentDuplicateDeleteTolerated(t *testing.T) {
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: fetchcache.NewLRU(64)})

	// Seed two identical bridge comments via a cache-less commenter.
	seed := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{KeepCommentHistory: true})
	for range 2 {
		if err := seed.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, "state A"); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	srv.mu.Lock()
	srv.failDel = true
	srv.mu.Unlock()
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, "state A"); err != nil {
		t.Fatalf("a failed duplicate delete must be tolerated: %v", err)
	}

	srv.mu.Lock()
	srv.failDel = false
	lists := srv.listCalls
	srv.mu.Unlock()
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, "state A"); err != nil {
		t.Fatalf("follow-up post: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.listCalls != lists+1 {
		t.Fatalf("cache must stay cold after a failed sweep so the next post re-lists; lists %d → %d", lists, srv.listCalls)
	}
	if len(srv.live) != 1 {
		t.Fatalf("follow-up post must sweep the duplicate; %d comments left", len(srv.live))
	}
}

// Label names with path-significant characters must reach GitHub as one
// escaped path segment — unescaped, `status/ok` deletes `.../labels/status/ok`
// (404, read as "not on the PR", so the stale label stays) and `wip#1` is cut
// at the fragment.
func TestSetPRLabels_EscapesLabelNamesInPath(t *testing.T) {
	var mu sync.Mutex
	var deleted []string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		deleted = append(deleted, strings.TrimPrefix(r.URL.EscapedPath(), "/api/v3/repos/owner/repo/issues/42/labels/"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	cm := newCommenterWithOptions(t, mux, bridgegithub.Options{PRLabelSuccess: "status/ok", PRLabelFailure: "wip#1", PRLabelWarning: "needs look"})
	// OutcomeNone removes all three, so every name goes through the DELETE path.
	if err := cm.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", bridgegithub.OutcomeNone); err != nil {
		t.Fatalf("SetPRLabels: %v", err)
	}
	want := map[string]bool{"status%2Fok": true, "wip%231": true, "needs%20look": true}
	if len(deleted) != len(want) {
		t.Fatalf("DELETE paths = %v, want %v", deleted, want)
	}
	for _, p := range deleted {
		if !want[p] {
			t.Errorf("unexpected DELETE path segment %q; want one of %v", p, want)
		}
	}
}

// A body over GitHub's limit is truncated before posting instead of being
// rejected (422) on every attempt: marker and key header kept, cut on a line
// break, an open code fence closed, and a note appended.
func TestPostOrUpdateComment_TruncatesOversizedBody(t *testing.T) {
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: fetchcache.NewLRU(64)})

	body := "## Resource Status\n```\n" + strings.Repeat("a long log line that keeps going\n", 3000)
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, body); err != nil {
		t.Fatalf("post: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.live) != 1 {
		t.Fatalf("want one comment, got %d", len(srv.live))
	}
	for _, posted := range srv.live {
		if n := len([]rune(posted)); n > 65536 {
			t.Fatalf("posted body has %d characters, over GitHub's 65536 limit", n)
		}
		if !strings.HasPrefix(posted, "<!-- flux-ai-bridge:apps/api -->\n<!-- flux-ai-bridge-key:") {
			t.Fatalf("marker/key header lost: %q", posted[:80])
		}
		if !strings.Contains(posted, "Comment truncated") {
			t.Fatal("truncation note missing")
		}
		if strings.Count(posted, "```")%2 != 0 {
			t.Fatal("truncation left a code fence open")
		}
		kept := posted[:strings.Index(posted, "\n\n*Comment truncated")]
		if kept = strings.TrimSuffix(kept, "\n```"); !strings.HasSuffix(kept, "keeps going") {
			t.Fatalf("cut did not land on a line break; kept text ends %q", kept[len(kept)-20:])
		}
	}
}

// A cut inside a four-backtick or tilde block is closed with the matching
// delimiter, so the truncation note renders as text, not code.
func TestPostOrUpdateComment_TruncationClosesMatchingFence(t *testing.T) {
	for name, fence := range map[string]string{"four backticks": "````", "tildes": "~~~"} {
		t.Run(name, func(t *testing.T) {
			srv := &commentServer{live: map[int64]string{}, nextID: 1}
			c := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
			body := "## Logs\n" + fence + "\n```\n" + strings.Repeat("a long log line that keeps going\n", 3000)
			if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "apps", "api", 42, body); err != nil {
				t.Fatalf("post: %v", err)
			}
			srv.mu.Lock()
			defer srv.mu.Unlock()
			for _, posted := range srv.live {
				kept := posted[:strings.Index(posted, "\n\n*Comment truncated")]
				if !strings.HasSuffix(kept, "keeps going\n"+fence) {
					t.Fatalf("open %s block not closed with %q; kept text ends %q", name, fence, kept[len(kept)-30:])
				}
			}
		})
	}
}

// Two bridges (different clusters, different INSTANCE_IDs) reporting on the
// same repo with a same-named Kustomization must each manage only their own
// comment: a state change on one never swaps or deletes the other's.
func TestPostOrUpdateComment_InstancesKeepSeparateComments(t *testing.T) {
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	east := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{InstanceID: "east"})
	west := newCommenterWithOptions(t, srv.mux(), bridgegithub.Options{InstanceID: "west"})
	ctx := context.Background()

	for _, step := range []struct {
		c    *bridgegithub.Commenter
		body string
	}{{east, "east: state A"}, {west, "west: state A"}, {east, "east: state B"}} {
		if err := step.c.PostOrUpdateComment(ctx, "owner", "repo", "flux-system", "apps", 42, step.body); err != nil {
			t.Fatalf("post %q: %v", step.body, err)
		}
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	var bodies []string
	for _, b := range srv.live {
		bodies = append(bodies, b)
	}
	if len(bodies) != 2 {
		t.Fatalf("want one comment per instance, got %d: %v", len(bodies), bodies)
	}
	joined := strings.Join(bodies, "\n====\n")
	for _, want := range []string{
		"<!-- flux-ai-bridge@east:flux-system/apps -->", "east: state B",
		"<!-- flux-ai-bridge@west:flux-system/apps -->", "west: state A",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in comments:\n%s", want, joined)
		}
	}
}
