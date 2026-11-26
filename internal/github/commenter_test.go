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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/fetchcache"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
)

// All tests now use GitHub App auth because the bridge dropped PAT support.
// ghinstallation parses the PEM eagerly, so the test config needs a real
// RSA key. Generating one per test would be expensive; we cache a single
// 2048-bit key for the whole package.
var (
	testKeyOnce sync.Once
	testKeyPEM  string
)

func appAuth(t *testing.T, baseURL string) bridgegithub.AuthConfig {
	t.Helper()
	testKeyOnce.Do(func() {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("generate test RSA key: %v", err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatalf("marshal test key: %v", err)
		}
		testKeyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	})
	return bridgegithub.AuthConfig{
		AppID:            12345,
		InstallationID:   67890,
		AppPrivateKeyPEM: testKeyPEM,
		BaseURL:          baseURL,
	}
}

func newCommenter(t *testing.T, mux *http.ServeMux) *bridgegithub.Commenter {
	t.Helper()
	// ghinstallation's transport refreshes installation tokens by POSTing
	// to /app/installations/<id>/access_tokens against the BaseURL we set
	// on the AuthConfig. The mock-github tests don't care about the token
	// itself — they just need the endpoint to return 200 with a token-like
	// JSON blob so the transport caches it for ~1h and proceeds to the
	// actual API calls the test cares about.
	// Note: /api/v3 prefix included. The commenter points ghinstallation at
	// the same normalized REST root go-github uses (real GHE serves the App
	// token endpoint under /api/v3 as well).
	mux.HandleFunc("/api/v3/app/installations/67890/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "ghs_test_installation_token",
			"expires_at": "2099-01-01T00:00:00Z",
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := bridgegithub.NewCommenter(appAuth(t, srv.URL+"/"), bridgegithub.Options{})
	if err != nil {
		t.Fatalf("NewCommenter: %v", err)
	}
	return c
}

func TestNewCommenter_NoBaseURL(t *testing.T) {
	_, err := bridgegithub.NewCommenter(appAuth(t, ""), bridgegithub.Options{})
	if err != nil {
		t.Fatalf("unexpected error with empty baseURL: %v", err)
	}
}

func TestNewCommenter_EnterpriseURL(t *testing.T) {
	srv := httptest.NewServer(http.NewServeMux())
	defer srv.Close()
	_, err := bridgegithub.NewCommenter(appAuth(t, srv.URL+"/"), bridgegithub.Options{})
	if err != nil {
		t.Fatalf("NewCommenter with enterprise URL: %v", err)
	}
}

// A redirect to another origin must not carry the installation token there:
// ghinstallation sets Authorization on every hop, after net/http has dropped
// it for the cross-origin request.
func TestNewCommenter_RefusesCrossOriginRedirect(t *testing.T) {
	var leaked atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{})
	}))
	t.Cleanup(other.Close)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/abc123/pulls", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusFound)
	})
	c := newCommenter(t, mux)

	if _, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "abc123"); err == nil {
		t.Error("FindPRsForCommit followed a cross-origin redirect; want an error")
	}
	if n := leaked.Load(); n != 0 {
		t.Fatalf("installation token sent to another origin on %d request(s)", n)
	}
}

// A redirect within the same origin (e.g. a renamed repository) still works.
func TestNewCommenter_FollowsSameOriginRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/abc123/pulls", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/api/v3/repos/owner/renamed/commits/abc123/pulls", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/api/v3/repos/owner/renamed/commits/abc123/pulls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 42, "state": "open", "head": map[string]any{"sha": "abc123"}}})
	})
	serveNonMergeCommit(mux, "abc123")
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "abc123")
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) == 0 || prs[0].Number != 42 {
		t.Fatalf("got %+v, want PR #42 through the same-origin redirect", prs)
	}
}

func TestFindPRsForCommit_EmptySHA(t *testing.T) {
	c, err := bridgegithub.NewCommenter(appAuth(t, ""), bridgegithub.Options{})
	if err != nil {
		t.Fatalf("NewCommenter: %v", err)
	}
	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "")
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 0 {
		t.Fatalf("expected no PRs for empty SHA, got %+v", prs)
	}
}

func TestFindPRsForCommit_ReturnsPRNumber(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/abc123/pulls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 42, "state": "open"}})
	})
	serveNonMergeCommit(mux, "abc123")
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "abc123")
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 1 || prs[0].Number != 42 {
		t.Fatalf("expected [PR#42], got %+v", prs)
	}
}

// serveNonMergeCommit serves sha as a single-parent commit, so the parent
// walk FindPRsForCommit runs after the direct lookup ends right there.
func serveNonMergeCommit(mux *http.ServeMux, sha string) {
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+sha, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "older"}}})
	})
}

// A deployed branch that is also the head of an open PR (a promotion PR from
// the dev branch) must not hide the PRs merged into it: the direct lookup
// returns the promotion PR, and the parent walk still finds the feature PR.
func TestFindPRsForCommit_DirectHitStillWalksMergeParents(t *testing.T) {
	const mergeSHA, featureHead = "merge0001", "feature1"
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+mergeSHA+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 1, "state": "open"}}) // promotion PR
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+mergeSHA, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": mergeSHA, "parents": []map[string]any{{"sha": "dev0000"}, {"sha": featureHead}}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+featureHead+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 7, "state": "open"}, {"number": 1, "state": "open"}})
	})
	serveNonMergeCommit(mux, "dev0000")
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", mergeSHA)
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	var got []int
	for _, pr := range prs {
		got = append(got, pr.Number)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 7 {
		t.Fatalf("PR numbers = %v, want [1 7] (promotion PR, then the merged-in feature PR, each once)", got)
	}
}

// TestFindPRsForCommit_CachesWholeResult pins the whole-resolution cache: a
// second FindPRsForCommit for the same source SHA must make ZERO GitHub calls
// — the direct lookup AND the entire merge-parent walk are skipped via the
// `prs-walk:` entry. This is what collapses N Kustomizations reconciling the
// same dev revision (and Flux re-fires) into a single resolution.
func TestFindPRsForCommit_CachesWholeResult(t *testing.T) {
	const mergeSHA = "merge0001"
	const prHead = "feature1"
	var apiCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/", func(w http.ResponseWriter, r *http.Request) {
		apiCalls++
		w.Header().Set("Content-Type", "application/json")
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/commits/"), "/")
		sha := parts[0]
		if len(parts) == 2 && parts[1] == "pulls" {
			if sha == prHead {
				_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 7, "state": "open"}})
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{}) // merge commit: empty
			return
		}
		// commit metadata: mergeSHA is a 2-parent merge; base is non-merge.
		if sha == mergeSHA {
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "base"}, {"sha": prHead}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "older"}}})
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	prs1, err := c.FindPRsForCommit(ctx, "owner", "repo", mergeSHA)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if len(prs1) != 1 || prs1[0].Number != 7 {
		t.Fatalf("first: got %+v, want [PR#7]", prs1)
	}
	firstCalls := apiCalls
	if firstCalls == 0 {
		t.Fatal("first resolution should make API calls")
	}

	prs2, err := c.FindPRsForCommit(ctx, "owner", "repo", mergeSHA)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(prs2) != 1 || prs2[0].Number != 7 {
		t.Fatalf("second: got %+v, want [PR#7]", prs2)
	}
	if apiCalls != firstCalls {
		t.Errorf("second FindPRsForCommit for the same SHA must make 0 GitHub calls (prs-walk cache); made %d more", apiCalls-firstCalls)
	}
}

// TestFindPRsForCommit_CoalescesConcurrent pins the single-flight behavior:
// when many events for the SAME source revision arrive concurrently with a
// cold cache (the thundering herd of one event per Kustomization on a new
// push), only ONE of them runs the direct-lookup + merge-parent walk; the
// rest block on it and reuse the result. Without coalescing each would walk,
// producing one full set of GitHub calls per caller. We park the leader
// mid-walk on a barrier and release it only once every waiter has checked
// the result cache and missed, so no waiter can be served from a cache the
// leader filled; then assert exactly one walk's worth of API calls and that
// every caller got the same PR.
func TestFindPRsForCommit_CoalescesConcurrent(t *testing.T) {
	const mergeSHA = "merge0001"
	const prHead = "feature1"
	const callers = 12

	var directMergeCalls int32           // hits on the merge commit's /pulls (one per walk)
	leaderEntered := make(chan struct{}) // closed when the first walk reaches the handler
	release := make(chan struct{})       // unblocks the parked leader
	var once sync.Once

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/commits/"), "/")
		sha := parts[0]
		if len(parts) == 2 && parts[1] == "pulls" {
			if sha == prHead {
				_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 7, "state": "open"}})
				return
			}
			// Merge commit direct lookup: empty. This is the first call of
			// the walk — park the leader here so waiters queue behind it.
			atomic.AddInt32(&directMergeCalls, 1)
			once.Do(func() { close(leaderEntered) })
			<-release
			_ = json.NewEncoder(w).Encode([]map[string]any{})
			return
		}
		if sha == mergeSHA {
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "base"}, {"sha": prHead}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "older"}}})
	})

	gets := &walkGets{Cache: fetchcache.NewLRU(64)}
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: gets})
	ctx := context.Background()

	type result struct {
		prs []bridgegithub.PRInfo
		err error
	}
	results := make(chan result, callers)

	// Leader first; wait until it's parked inside the walk.
	go func() {
		prs, err := c.FindPRsForCommit(ctx, "owner", "repo", mergeSHA)
		results <- result{prs, err}
	}()
	select {
	case <-leaderEntered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("leader never entered the walk")
	}

	// Now the rest, while the leader is still parked — they must coalesce.
	for i := 0; i < callers-1; i++ {
		go func() {
			prs, err := c.FindPRsForCommit(ctx, "owner", "repo", mergeSHA)
			results <- result{prs, err}
		}()
	}
	// The leader read the result cache twice (before and inside the single
	// flight), each waiter once: all of them missed while the leader is parked.
	waitFor(t, func() bool { return gets.n.Load() >= callers+1 })
	// Give the waiters a beat to join the single flight before releasing.
	time.Sleep(50 * time.Millisecond)
	close(release)

	for i := 0; i < callers; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("caller %d: %v", i, r.err)
		}
		if len(r.prs) != 1 || r.prs[0].Number != 7 {
			t.Fatalf("caller %d: got %+v, want [PR#7]", i, r.prs)
		}
	}
	if got := atomic.LoadInt32(&directMergeCalls); got != 1 {
		t.Errorf("expected exactly 1 merge-commit walk across %d concurrent callers (single-flight); got %d", callers, got)
	}
}

// walkGets counts reads of the whole-resolution (prs-walk) cache, so a test
// can wait until its callers have checked that cache before it lets a parked
// leader finish.
type walkGets struct {
	fetchcache.Cache
	n atomic.Int32
}

func (c *walkGets) Get(ctx context.Context, key string) ([]byte, bool) {
	if strings.HasPrefix(key, "prs-walk:") {
		c.n.Add(1)
	}
	return c.Cache.Get(ctx, key)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}

// parkedWalkMux serves a merge commit whose second parent is the head of open
// PR #7. The first direct lookup of the merge commit parks until park returns.
func parkedWalkMux(entered chan<- struct{}, park func(*http.Request)) *http.ServeMux {
	const mergeSHA, prHead = "merge0001", "feature1"
	var once sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/commits/"), "/")
		sha := parts[0]
		if len(parts) == 2 && parts[1] == "pulls" {
			if sha == prHead {
				_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 7, "state": "open"}})
				return
			}
			first := false
			once.Do(func() { first = true })
			if first {
				close(entered)
				park(r)
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{})
			return
		}
		if sha == mergeSHA {
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "base"}, {"sha": prHead}}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "older"}}})
	})
	return mux
}

// A caller waiting on another event's resolution stops waiting when its own
// context ends, instead of blocking until the leader's walk finishes.
func TestFindPRsForCommit_WaiterCancelStopsWaiting(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	gets := &walkGets{Cache: fetchcache.NewLRU(64)}
	c := newCommenterWithOptions(t, parkedWalkMux(entered, func(*http.Request) { <-release }), bridgegithub.Options{Cache: gets})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	leaderDone := make(chan error, 1)
	go func() {
		_, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "merge0001")
		leaderDone <- err
	}()
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := c.FindPRsForCommit(ctx, "owner", "repo", "merge0001")
		waiterDone <- err
	}()
	waitFor(t, func() bool { return gets.n.Load() >= 3 })
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter still blocked on the leader's walk")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-leaderDone; err != nil {
		t.Errorf("leader: %v", err)
	}
}

// When the leader's context ends mid-walk, a waiter with time left resolves
// the PRs itself instead of failing with the leader's cancellation.
func TestFindPRsForCommit_WaiterSurvivesLeaderCancel(t *testing.T) {
	entered := make(chan struct{})
	gets := &walkGets{Cache: fetchcache.NewLRU(64)}
	c := newCommenterWithOptions(t, parkedWalkMux(entered, func(r *http.Request) { <-r.Context().Done() }), bridgegithub.Options{Cache: gets})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	leaderDone := make(chan error, 1)
	go func() {
		_, err := c.FindPRsForCommit(leaderCtx, "owner", "repo", "merge0001")
		leaderDone <- err
	}()
	<-entered

	type result struct {
		prs []bridgegithub.PRInfo
		err error
	}
	waiterDone := make(chan result, 1)
	go func() {
		prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "merge0001")
		waiterDone <- result{prs, err}
	}()
	waitFor(t, func() bool { return gets.n.Load() >= 3 })
	time.Sleep(20 * time.Millisecond)
	cancelLeader()

	if err := <-leaderDone; err == nil {
		t.Error("cancelled leader returned no error")
	}
	select {
	case r := <-waiterDone:
		if r.err != nil {
			t.Fatalf("waiter failed with the leader's cancellation: %v", r.err)
		}
		if len(r.prs) != 1 || r.prs[0].Number != 7 {
			t.Fatalf("waiter got %+v, want [PR#7]", r.prs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never returned")
	}
}

// walkPutTTLs records the TTL of every whole-resolution (prs-walk) write.
type walkPutTTLs struct {
	fetchcache.Cache
	mu   sync.Mutex
	ttls []time.Duration
}

func (c *walkPutTTLs) Put(ctx context.Context, key string, value []byte, ttl time.Duration) {
	if strings.HasPrefix(key, "prs-walk:") {
		c.mu.Lock()
		c.ttls = append(c.ttls, ttl)
		c.mu.Unlock()
	}
	c.Cache.Put(ctx, key, value, ttl)
}

// A resolution built from cached per-commit lookups is as old as the oldest
// of them, so its cache entry must expire with that lookup instead of getting
// a fresh full TTL (which would serve PR state up to twice the TTL old).
func TestFindPRsForCommit_WalkEntryExpiresWithItsOldestLookup(t *testing.T) {
	const ttl, wait = 400 * time.Millisecond, 200 * time.Millisecond
	mux := parkedWalkMux(make(chan struct{}), func(*http.Request) {})
	cache := &walkPutTTLs{Cache: fetchcache.NewLRU(64)}
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: cache, PRsForCommitTTL: ttl})
	ctx := context.Background()

	if _, err := c.FindPRsForCommit(ctx, "owner", "repo", "merge0001"); err != nil {
		t.Fatalf("first: %v", err)
	}
	time.Sleep(wait)
	// A new resolution that reuses the per-commit entries cached above.
	cache.Delete(ctx, "prs-walk:owner/repo/merge0001")
	prs, err := c.FindPRsForCommit(ctx, "owner", "repo", "merge0001")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(prs) != 1 || prs[0].Number != 7 {
		t.Fatalf("second: got %+v, want [PR#7]", prs)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.ttls) == 2 && cache.ttls[1] > ttl-wait {
		t.Fatalf("whole-resolution entry cached for %s; its lookups were already %s old, so at most %s is left", cache.ttls[1], wait, ttl-wait)
	}
}

// TestListPRsForCommit_NegativeCaches pins the rate-limit fix: an EMPTY
// `/commits/{sha}/pulls` result (the common case for a dev-branch merge
// commit, which is the direct lookup at the top of FindPRsForCommit) must be
// cached so repeated events for the same SHA — e.g. many Kustomizations
// reconciling the same dev revision — don't re-query GitHub every time and
// exhaust the rate limit. Without negative caching this was one API call per
// event per Kustomization for the identical SHA.
func TestListPRsForCommit_NegativeCaches(t *testing.T) {
	var directCalls int
	const sha = "devmerge01"
	mux := http.NewServeMux()
	// Direct lookup is empty; commit metadata is a non-merge (single parent)
	// so the walk stops immediately without other lookups.
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+sha+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
		directCalls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+sha, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": []map[string]any{{"sha": "base"}}})
	})

	cache := fetchcache.NewLRU(16)
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: cache})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		// Evict the whole-resolution entry so each call reaches the direct
		// lookup and only its own (negative) cache can save the API call.
		cache.Delete(ctx, "prs-walk:owner/repo/"+sha)
		prs, err := c.FindPRsForCommit(ctx, "owner", "repo", sha)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if len(prs) != 0 {
			t.Fatalf("call %d: expected no PRs, got %+v", i, prs)
		}
	}
	if directCalls != 1 {
		t.Errorf("expected the empty /pulls lookup to be cached after the first call; got %d API calls across 5 reads", directCalls)
	}
}

// Autodev-style: dev merge commit isn't on any PR head branch, but parents[1]
// IS. The bridge should walk one level and find the PR there.
func TestFindPRsForCommit_WalksMergeParent(t *testing.T) {
	mux := http.NewServeMux()
	const mergeSHA = "merge0001"
	const prHeadSHA = "feature1"
	// Direct lookup: empty.
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+mergeSHA+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{})
	})
	// Commit metadata: 2 parents, parents[1] is the PR head SHA.
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+mergeSHA, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sha":     mergeSHA,
			"parents": []map[string]any{{"sha": "dev0000"}, {"sha": prHeadSHA}},
		})
	})
	// Parent[1] lookup: returns the open PR.
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+prHeadSHA+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 99, "state": "open"}})
	})
	// dev0000 has 1 parent — stops the walk.
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/dev0000", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sha":     "dev0000",
			"parents": []map[string]any{{"sha": "older"}},
		})
	})
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", mergeSHA)
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 1 || prs[0].Number != 99 {
		t.Fatalf("expected [PR#99], got %+v", prs)
	}
}

// Autodev batches multiple PRs in one run: chain of merge commits where each
// parents[0] is the previous merge and parents[1] is a PR head. Walking the
// chain reveals all three PRs in one call.
func TestFindPRsForCommit_WalksMergeChain(t *testing.T) {
	mux := http.NewServeMux()
	const m3 = "merge3"
	const m2 = "merge2"
	const m1 = "merge1"
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+m3+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+m3, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": m3, "parents": []map[string]any{{"sha": m2}, {"sha": "pr3head"}}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+m2, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": m2, "parents": []map[string]any{{"sha": m1}, {"sha": "pr2head"}}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+m1, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": m1, "parents": []map[string]any{{"sha": "dev0"}, {"sha": "pr1head"}}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/dev0", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": "dev0", "parents": []map[string]any{{"sha": "older"}}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/pr3head/pulls", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 3, "state": "open"}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/pr2head/pulls", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 2, "state": "open"}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/pr1head/pulls", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 1, "state": "open"}})
	})
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", m3)
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	got := map[int]bool{}
	for _, pr := range prs {
		got[pr.Number] = true
	}
	for _, want := range []int{1, 2, 3} {
		if !got[want] {
			t.Errorf("expected PR #%d in result, got %+v", want, prs)
		}
	}
}

// mergeChainMux serves an autodev-style history: dev tip m2 and m1 merge open
// PRs 2 and 1 onto the default branch's tip, which is itself a merge commit
// (the default branch merges with merge commits), followed by one more
// default-branch merge and a plain commit. probed records which side-parent
// tips were looked up.
func mergeChainMux(probed map[string]bool, compare http.HandlerFunc) *http.ServeMux {
	mux := http.NewServeMux()
	commit := func(sha string, parents ...string) {
		mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+sha, func(w http.ResponseWriter, _ *http.Request) {
			ps := make([]map[string]any, 0, len(parents))
			for _, p := range parents {
				ps = append(ps, map[string]any{"sha": p})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"sha": sha, "parents": ps})
		})
	}
	pulls := func(sha string, prs ...map[string]any) {
		mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+sha+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
			probed[sha] = true
			if prs == nil {
				prs = []map[string]any{}
			}
			_ = json.NewEncoder(w).Encode(prs)
		})
	}
	commit("m2", "m1", "pr2head")
	commit("m1", "maintip", "pr1head")
	commit("maintip", "mainold", "merged1head")
	commit("mainold", "plain", "merged0head")
	commit("plain", "root")
	pulls("m2")
	pulls("pr2head", map[string]any{"number": 2, "state": "open"})
	pulls("pr1head", map[string]any{"number": 1, "state": "open"})
	pulls("merged1head", map[string]any{"number": 11, "state": "closed", "merged_at": "2026-01-01T00:00:00Z"})
	pulls("merged0head", map[string]any{"number": 10, "state": "closed", "merged_at": "2026-01-01T00:00:00Z"})
	mux.HandleFunc("/api/v3/repos/owner/repo", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "repo", "default_branch": "main"})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/compare/main...m2", compare)
	return mux
}

// The walk stops at the merge base with the default branch instead of
// continuing through the default branch's own merge commits, whose tips only
// resolve to already-merged PRs.
func TestFindPRsForCommit_WalkStopsAtDefaultBranchMergeBase(t *testing.T) {
	probed := map[string]bool{}
	mux := mergeChainMux(probed, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]any{"sha": "maintip"}})
	})
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "m2")
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 2 || prs[0].Number != 2 || prs[1].Number != 1 {
		t.Fatalf("expected [PR#2 PR#1], got %+v", prs)
	}
	if probed["merged1head"] || probed["merged0head"] {
		t.Errorf("walk probed default-branch history below the merge base: %v", probed)
	}
}

// Without a merge base (unrelated histories, missing permission, API error)
// the walk keeps today's behaviour and runs to the first plain commit.
func TestFindPRsForCommit_WalkWithoutMergeBaseRunsToPlainCommit(t *testing.T) {
	probed := map[string]bool{}
	mux := mergeChainMux(probed, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"No common ancestor"}`, http.StatusNotFound)
	})
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "m2")
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 4 {
		t.Fatalf("expected all 4 PRs down to the plain commit, got %+v", prs)
	}
	if !probed["merged0head"] {
		t.Error("walk stopped early although no merge base was known")
	}
}

// A deployed commit that is already on the default branch (merge base ==
// the commit) has only merged history below it: the walk probes the commit's
// own merged-in tip and stops at the next merge.
func TestFindPRsForCommit_WalkStopsWhenCommitIsOnDefaultBranch(t *testing.T) {
	probed := map[string]bool{}
	mux := mergeChainMux(probed, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]any{"sha": "m2"}})
	})
	c := newCommenter(t, mux)

	if _, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "m2"); err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if !probed["pr2head"] || probed["pr1head"] {
		t.Errorf("expected only the deployed merge's own tip to be probed, got %v", probed)
	}
}

// A single merge on top of a plain commit (a default branch that squash- or
// rebase-merges) ends at the plain commit: no merge-base lookup is made.
func TestFindPRsForCommit_ShortWalkSkipsMergeBaseLookup(t *testing.T) {
	var lookups int
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/m1/pulls", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/m1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": "m1", "parents": []map[string]any{{"sha": "maintip"}, {"sha": "pr1head"}}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/maintip", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": "maintip", "parents": []map[string]any{{"sha": "older"}}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/pr1head/pulls", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 1, "state": "open"}})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo", func(w http.ResponseWriter, _ *http.Request) {
		lookups++
		_ = json.NewEncoder(w).Encode(map[string]any{"default_branch": "main"})
	})
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "m1")
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 1 || prs[0].Number != 1 {
		t.Fatalf("expected [PR#1], got %+v", prs)
	}
	if lookups != 0 {
		t.Errorf("merge-base lookup made for a walk that ends at the first plain commit; repo lookups = %d", lookups)
	}
}

// Plain (non-merge) commit with the direct lookup empty stops the walk
// immediately — no parents to follow.
func TestFindPRsForCommit_NoMergeNoWalk(t *testing.T) {
	mux := http.NewServeMux()
	const sha = "plain1"
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+sha+"/pulls", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/"+sha, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sha":     sha,
			"parents": []map[string]any{{"sha": "p1"}},
		})
	})
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", sha)
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 0 {
		t.Fatalf("expected no PRs for non-merge commit, got %+v", prs)
	}
}

func TestFindPRsForCommit_NoPRs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/abc123/pulls", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]any{})
	})
	// FindPRsForCommit walks parents when the direct lookup is empty; stub a
	// single-parent (non-merge) commit so the walk terminates immediately.
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/abc123", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sha":     "abc123",
			"parents": []map[string]any{{"sha": "older"}},
		})
	})
	c := newCommenter(t, mux)

	prs, err := c.FindPRsForCommit(context.Background(), "owner", "repo", "abc123")
	if err != nil {
		t.Fatalf("FindPRsForCommit: %v", err)
	}
	if len(prs) != 0 {
		t.Fatalf("expected no PRs, got %+v", prs)
	}
}

func TestPostOrUpdateComment_CreatesNew(t *testing.T) {
	var created bool
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode([]any{})
		case http.MethodPost:
			created = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 1, "body": "ok"})
		}
	})
	c := newCommenter(t, mux)

	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "flux-system", "podinfo", 42, "hello"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}
	if !created {
		t.Fatal("expected a new comment to be created")
	}
}

// TestPostOrUpdateComment_PreservesOtherKustomizationMarkers is the
// regression test for the user-reported "PRs having comments of different
// kustomizations" symptom. The bug: a single global marker meant
// kustomization-B's PostOrUpdateComment deleted kustomization-A's prior
// comment on the same PR.
//
// This pins marker scoping: posting for ns/A must NOT touch a comment carrying
// the ns/B marker (or the legacy unscoped marker — those belong to no specific
// Kustomization and we don't have safe attribution). On a changed analysis the
// prior ns/A comment is deleted and recreated (so the reviewer is notified);
// the other markers are left strictly alone.
func TestPostOrUpdateComment_PreservesOtherKustomizationMarkers(t *testing.T) {
	deletedIDs := []int64{}
	var created bool

	// Three prior comments on PR 42:
	//   id=100  → ns/A      (must be deleted+recreated; same scope as the post)
	//   id=101  → ns/B      (must be PRESERVED; different Kustomization)
	//   id=102  → unscoped  (must be PRESERVED; legacy bridge versions)
	priors := []any{
		map[string]any{"id": float64(100), "body": "<!-- flux-ai-bridge:flux-system/a -->\nold A"},
		map[string]any{"id": float64(101), "body": "<!-- flux-ai-bridge:flux-system/b -->\nold B"},
		map[string]any{"id": float64(102), "body": "<!-- flux-ai-bridge -->\nlegacy"},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(priors)
		case http.MethodPost:
			created = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 200, "body": "new"})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		id, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		deletedIDs = append(deletedIDs, id)
		w.WriteHeader(http.StatusNoContent)
	})
	c := newCommenter(t, mux)

	// Post for ns/A.
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "flux-system", "a", 42, "new A"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}
	if !created {
		t.Fatal("expected a fresh ns/A comment to be created")
	}
	if len(deletedIDs) != 1 || deletedIDs[0] != 100 {
		t.Fatalf("expected exactly one delete (id=100, the prior ns/A comment); got %v", deletedIDs)
	}
}

func TestPostOrUpdateComment_RecreatesOnChange(t *testing.T) {
	var deleted, created bool
	// Same-scoped prior comment with a DIFFERENT body must be deleted and a
	// fresh one posted (so the reviewer gets a notification for the update).
	existing := map[string]any{"id": float64(99), "body": "<!-- flux-ai-bridge:flux-system/podinfo -->\nold text"}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode([]any{existing})
		case http.MethodPost:
			created = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 100, "body": "new"})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/99", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		}
	})
	c := newCommenter(t, mux)

	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "flux-system", "podinfo", 42, "new text"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}
	if !deleted {
		t.Fatal("expected the changed prior comment to be deleted")
	}
	if !created {
		t.Fatal("expected a fresh comment to be created after delete")
	}
}

// TestPostOrUpdateComment_SkipsWhenUnchanged is the core regression for the
// reported symptom: on a busy source branch the bridge re-rendered the same
// analysis every time the Flux revision advanced and delete+recreated the PR
// comment, spamming subscribers. With content-hash idempotency, an identical
// body must touch nothing — no edit, no delete, no create.
func TestPostOrUpdateComment_SkipsWhenUnchanged(t *testing.T) {
	marker := "<!-- flux-ai-bridge:flux-system/podinfo -->"
	// The existing comment carries the embedded stability-key marker exactly
	// as the bridge posts it (sha256 of marker+"\n"+stabilityKey; plain
	// PostOrUpdateComment uses the body as its own key).
	sum := sha256.Sum256([]byte(marker + "\nsame body"))
	keyMarker := "<!-- flux-ai-bridge-key:" + hex.EncodeToString(sum[:]) + " -->"
	existing := map[string]any{"id": float64(99), "body": marker + "\n" + keyMarker + "\nsame body"}
	var deleted, created, edited bool

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode([]any{existing})
		case http.MethodPost:
			created = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 100})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/99", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPatch:
			edited = true
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(99)})
		}
	})
	c := newCommenter(t, mux)

	// Body renders to exactly the existing comment's body → no-op.
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "flux-system", "podinfo", 42, "same body"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}
	if edited || deleted || created {
		t.Fatalf("expected a no-op for identical body; edited=%v deleted=%v created=%v", edited, deleted, created)
	}
}

// TestPostOrUpdateComment_Pagination pins two behaviours at once:
//   - the bridge collects ALL matching comment IDs across every page before
//     mutating anything (the GitHub offset-pagination shift bug: deleting
//     while iterating shifts later pages up and skips items);
//   - convergence to a single comment — when the slow path finds duplicate
//     marker comments (left by the legacy delete+recreate era or a race), it
//     keeps the NEWEST (highest ID), edits it in place, and deletes the rest.
//
// The mock implements the real API's shift behaviour: pagination is offset
// based over live state, so deleting page 1 shifts page 2 up. Collecting all
// IDs first is what makes the delete sweep see everything.
func TestPostOrUpdateComment_Pagination(t *testing.T) {
	// 150 comments total, all with the same marker, all initially present.
	// Page size = 100 → page 1: IDs 1-100, page 2: IDs 101-150.
	marker := "<!-- flux-ai-bridge:flux-system/podinfo -->"

	var mu sync.Mutex
	deleted := make(map[int]struct{})
	deletionOrder := []int{}

	// remainingAt returns the current page-1 slice of comments that have
	// NOT been deleted yet — matching the real GitHub API's behavior where
	// pagination is offset-based over the live state, so deleting page 1
	// causes what was page 2 to shift up into page 1.
	remainingAt := func(page int) []map[string]any {
		const pageSize = 100
		mu.Lock()
		defer mu.Unlock()
		var alive []int
		for i := 1; i <= 150; i++ {
			if _, gone := deleted[i]; !gone {
				alive = append(alive, i)
			}
		}
		start := (page - 1) * pageSize
		end := start + pageSize
		if start >= len(alive) {
			return nil
		}
		if end > len(alive) {
			end = len(alive)
		}
		out := make([]map[string]any, 0, end-start)
		for _, id := range alive[start:end] {
			out = append(out, map[string]any{
				"id":   float64(id),
				"body": marker + "\ncomment " + strconv.Itoa(id),
			})
		}
		return out
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			if page == 0 {
				page = 1
			}
			comments := remainingAt(page)
			// Set the next-page Link header only when there's another full
			// page of comments to read — matches go-github's NextPage
			// detection. If the current page is short, we're at the end.
			if len(comments) == 100 {
				w.Header().Set("Link", `<https://api.github.com/api/v3/repos/owner/repo/issues/42/comments?page=`+strconv.Itoa(page+1)+`>; rel="next"`)
			}
			_ = json.NewEncoder(w).Encode(comments)
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 200, "body": "new"})
			return
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		id, _ := strconv.Atoi(parts[len(parts)-1])
		mu.Lock()
		deleted[id] = struct{}{}
		deletionOrder = append(deletionOrder, id)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})

	c := newCommenter(t, mux)
	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "flux-system", "podinfo", 42, "new text"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}

	// All 150 (none of which match the new body) are deleted, then one fresh
	// comment is posted. The buggy interleaved GET+DELETE ordering would only
	// catch the first page; collecting all IDs before deleting sees every one
	// regardless of the server-side offset shift.
	if len(deleted) != 150 {
		t.Errorf("expected 150 deleted comments, got %d (offset-pagination shift bug if <150)", len(deleted))
	}
}

// TestPostOrUpdateComment_SerializesConcurrentPostsForSameScope is the
// regression test for the duplicate-comment race: two goroutines posting
// for the same (namespace, name) scope must not both pass the list-delete
// step before either creates. With the per-scope mutex in place, the
// second goroutine sees the first's comment in its list step and deletes
// it before creating, leaving exactly one current comment on the PR.
//
// The handler mocks the GitHub API with shared state: a slice of comment
// IDs visible to GET, mutated by POST (append) and DELETE (remove). The
// assertion is on the final list length after both goroutines complete —
// before the fix it would be 2; with the fix it must be 1.
func TestPostOrUpdateComment_SerializesConcurrentPostsForSameScope(t *testing.T) {
	var mu sync.Mutex
	comments := []map[string]any{} // shared "PR comments" store
	nextID := int64(100)
	const slowdown = 30 * time.Millisecond // widen the race window

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			// Simulate API latency between list and the caller's subsequent
			// delete/create. Without the per-scope mutex, both goroutines'
			// list calls return [] (no prior comments) and both proceed
			// straight to create, ending with two entries.
			time.Sleep(slowdown)
			mu.Lock()
			snapshot := make([]map[string]any, len(comments))
			copy(snapshot, comments)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(snapshot)
		case http.MethodPost:
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			id := nextID
			nextID++
			comments = append(comments, map[string]any{"id": float64(id), "body": body.Body})
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "body": body.Body})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		// /issues/comments/<id>
		idStr := strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/issues/comments/")
		id, _ := strconv.ParseInt(idStr, 10, 64)
		switch r.Method {
		case http.MethodDelete:
			mu.Lock()
			filtered := comments[:0]
			for _, c := range comments {
				if int64(c["id"].(float64)) != id {
					filtered = append(filtered, c)
				}
			}
			comments = filtered
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPatch:
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			for _, c := range comments {
				if int64(c["id"].(float64)) == id {
					c["body"] = body.Body
				}
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(id), "body": body.Body})
		default:
			http.NotFound(w, r)
		}
	})
	c := newCommenter(t, mux)

	var wg sync.WaitGroup
	wg.Add(2)
	for i, body := range []string{"first analysis", "second analysis"} {
		i, body := i, body
		go func() {
			defer wg.Done()
			// Same Kustomization scope (flux-system/podinfo) for both
			// goroutines — that's the racing case. Tiny stagger so the
			// goroutines don't even start the list call at exactly the
			// same instant (which would just exercise sleep-ordering).
			if i == 1 {
				time.Sleep(slowdown / 2)
			}
			if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "flux-system", "podinfo", 42, body); err != nil {
				t.Errorf("goroutine %d PostOrUpdateComment: %v", i, err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(comments) != 1 {
		t.Fatalf("expected exactly 1 comment after concurrent posts (per-scope mutex should serialize them); got %d: %+v", len(comments), comments)
	}
}

// TestPostOrUpdateComment_KeepHistory_DoesNotDelete verifies that when
// Options.KeepCommentHistory is true the commenter posts a fresh comment
// *without* deleting prior marker-bearing comments. Use case: testing a
// commit toggle (healthy → broken → healthy) and wanting to see the
// chronological trail on the PR.
func TestPostOrUpdateComment_KeepHistory_DoesNotDelete(t *testing.T) {
	var deleted, created bool
	existing := map[string]any{"id": float64(99), "body": "<!-- flux-ai-bridge -->\nold text"}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode([]any{existing})
		case http.MethodPost:
			created = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 100, "body": "new"})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/99", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted = true
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("/api/v3/app/installations/67890/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "ghs_test", "expires_at": "2099-01-01T00:00:00Z"})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c, err := bridgegithub.NewCommenter(appAuth(t, srv.URL+"/"), bridgegithub.Options{KeepCommentHistory: true})
	if err != nil {
		t.Fatalf("NewCommenter: %v", err)
	}

	if err := c.PostOrUpdateComment(context.Background(), "owner", "repo", "flux-system", "podinfo", 42, "new text"); err != nil {
		t.Fatalf("PostOrUpdateComment: %v", err)
	}
	if deleted {
		t.Fatal("KeepCommentHistory=true must NOT delete prior marker comments")
	}
	if !created {
		t.Fatal("expected a fresh comment to be created")
	}
}

// PostCommitComment was dropped in Phase 2 (Q4) — commit-comment fallback
// removed so the GitHub App's permissions can stay at PRs:Read + Issues:Write
// without needing Contents:Write.

func TestNewCommenter_AppMissingPrivateKey(t *testing.T) {
	_, err := bridgegithub.NewCommenter(bridgegithub.AuthConfig{
		AppID:          12345,
		InstallationID: 67890,
		// AppPrivateKeyPEM intentionally empty
	}, bridgegithub.Options{})
	if err == nil {
		t.Fatal("expected error for missing private key, got nil")
	}
}

func TestNewCommenter_NoAuth(t *testing.T) {
	_, err := bridgegithub.NewCommenter(bridgegithub.AuthConfig{}, bridgegithub.Options{})
	if err == nil {
		t.Fatal("expected error for missing App config, got nil")
	}
}

// Partial config: AppID set but no InstallationID. Must still error.
func TestNewCommenter_AppMissingInstallationID(t *testing.T) {
	_, err := bridgegithub.NewCommenter(bridgegithub.AuthConfig{
		AppID:            12345,
		AppPrivateKeyPEM: "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----",
	}, bridgegithub.Options{})
	if err == nil {
		t.Fatal("expected error for missing installation ID, got nil")
	}
}

// newCommenterWithOptions is the cache-aware variant of newCommenter:
// the caller supplies Options (typically with a fetchcache attached) so
// tests can assert that the cache is consulted for read-heavy lookups.
// Otherwise identical to newCommenter — same mock-token endpoint setup.
func newCommenterWithOptions(t *testing.T, mux *http.ServeMux, opts bridgegithub.Options) *bridgegithub.Commenter {
	t.Helper()
	mux.HandleFunc("/api/v3/app/installations/67890/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "ghs_test_installation_token",
			"expires_at": "2099-01-01T00:00:00Z",
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := bridgegithub.NewCommenter(appAuth(t, srv.URL+"/"), opts)
	if err != nil {
		t.Fatalf("NewCommenter: %v", err)
	}
	return c
}

// TestListPRFiles_CachesByHeadSHA pins the per-head file-list cache: repeated
// ListPRFiles for the same (PR, head) must hit the upstream API once. Without
// it, filterPRs re-lists files for every candidate PR on every event — a
// major rate-limit drain on a busy repo. With an empty head SHA the result is
// NOT cached (we never key a stale list under a missing head).
func TestListPRFiles_CachesByHeadSHA(t *testing.T) {
	var apiCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/pulls/42/files", func(w http.ResponseWriter, _ *http.Request) {
		apiCalls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "apps/podinfo/release.yaml", "sha": "blob1", "status": "modified"},
		})
	})
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(16)})
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		files, err := c.ListPRFiles(ctx, "owner", "repo", 42, "headsha1")
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if len(files) != 1 || files[0].BlobSHA != "blob1" {
			t.Fatalf("call %d: files = %+v", i, files)
		}
	}
	if apiCalls != 1 {
		t.Errorf("expected 1 ListFiles API call across 4 reads of the same head; got %d", apiCalls)
	}

	// A different head SHA is a distinct key → one more fetch.
	if _, err := c.ListPRFiles(ctx, "owner", "repo", 42, "headsha2"); err != nil {
		t.Fatalf("new-head call: %v", err)
	}
	if apiCalls != 2 {
		t.Errorf("expected a fresh fetch for a new head SHA; got %d total", apiCalls)
	}

	// Empty head SHA must NOT be served from (or written to) cache.
	before := apiCalls
	for i := 0; i < 2; i++ {
		if _, err := c.ListPRFiles(ctx, "owner", "repo", 42, ""); err != nil {
			t.Fatalf("empty-head call %d: %v", i, err)
		}
	}
	if apiCalls != before+2 {
		t.Errorf("empty head SHA must bypass the cache (2 fetches); got %d (before %d)", apiCalls, before)
	}
}

// A renamed file carries its base path (previous_filename) — also through the
// head-keyed cache — so a base-vs-head diff can read the old path.
func TestListPRFiles_RenamedKeepsPreviousPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/pulls/42/files", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "apps/new.yaml", "previous_filename": "apps/old.yaml", "sha": "blob1", "status": "renamed"},
		})
	})
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(16)})
	for i := 0; i < 2; i++ { // second read is served from the cache
		files, err := c.ListPRFiles(context.Background(), "owner", "repo", 42, "headsha1")
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if len(files) != 1 || files[0].PreviousPath != "apps/old.yaml" {
			t.Fatalf("call %d: files = %+v, want PreviousPath apps/old.yaml", i, files)
		}
	}
}

// A file list cached before PreviousPath existed has renames without it: it
// is refetched instead of served, so the rename's base path is known.
func TestListPRFiles_RefetchesLegacyRenameEntry(t *testing.T) {
	var apiCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/pulls/42/files", func(w http.ResponseWriter, _ *http.Request) {
		apiCalls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "apps/new.yaml", "previous_filename": "apps/old.yaml", "sha": "blob1", "status": "renamed"},
		})
	})
	cache := fetchcache.NewLRU(16)
	cache.Put(context.Background(), "pr-files:owner/repo/42/headsha1",
		[]byte(`[{"Path":"apps/new.yaml","BlobSHA":"blob1","Status":"renamed"}]`), 0)
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: cache})
	files, err := c.ListPRFiles(context.Background(), "owner", "repo", 42, "headsha1")
	if err != nil {
		t.Fatal(err)
	}
	if apiCalls != 1 || len(files) != 1 || files[0].PreviousPath != "apps/old.yaml" {
		t.Errorf("apiCalls=%d files=%+v, want one refetch with PreviousPath apps/old.yaml", apiCalls, files)
	}
}

// TestBlobResourceIDs_CachesDerivedIDs pins the cache behaviour after the
// switch from caching raw blob bytes to caching only the DERIVED resource
// IDs. Five reads of the same blob SHA must hit the upstream API exactly once
// AND parse exactly once — the derived IDs are content-addressed (immutable)
// so subsequent reads come from cache. The raw manifest bytes are never
// stored (only the parser's output is), keeping file contents out of Redis.
func TestBlobResourceIDs_CachesDerivedIDs(t *testing.T) {
	var blobCalls, parseCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/git/blobs/", func(w http.ResponseWriter, r *http.Request) {
		blobCalls++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sha":      "abc123",
			"content":  "aGVsbG8=", // "hello" base64
			"encoding": "base64",
		})
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{
		Cache: fetchcache.NewLRU(64),
		BlobResourceParser: func([]byte) []string {
			parseCalls++
			return []string{"default_app_apps_Deployment"}
		},
	})

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		ids, err := c.BlobResourceIDs(ctx, "owner", "repo", "abc123")
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if len(ids) != 1 || ids[0] != "default_app_apps_Deployment" {
			t.Errorf("call %d: ids = %v, want [default_app_apps_Deployment]", i, ids)
		}
	}
	if blobCalls != 1 {
		t.Errorf("expected 1 upstream blob fetch across 5 cached reads, got %d", blobCalls)
	}
	if parseCalls != 1 {
		t.Errorf("expected the parser to run once (then served from the derived-ID cache), got %d", parseCalls)
	}
}

// BaseFileResourceIDs fetches a file at a git ref (the PR base) via the contents
// API, parses it, and caches the derived IDs by (ref, path). A 404 (file absent
// at base) is an empty result, not an error.
func TestBaseFileResourceIDs(t *testing.T) {
	var contentCalls, parseCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/contents/", func(w http.ResponseWriter, r *http.Request) {
		contentCalls++
		if r.URL.Query().Get("ref") != "base-sha" {
			t.Errorf("expected ref=base-sha, got %q", r.URL.Query().Get("ref"))
		}
		if strings.HasSuffix(r.URL.Path, "/gone.yaml") {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Not Found"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type":     "file",
			"sha":      "basesha123",
			"content":  "aGVsbG8=", // "hello" base64
			"encoding": "base64",
		})
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{
		Cache: fetchcache.NewLRU(64),
		BlobResourceParser: func([]byte) []string {
			parseCalls++
			return []string{"_my-policy_kyverno.io_ClusterPolicy"}
		},
	})
	ctx := context.Background()

	// Present file: parsed once, then served from the (ref,path) cache.
	for i := 0; i < 3; i++ {
		ids, err := c.BaseFileResourceIDs(ctx, "owner", "repo", "policies/p.yaml", "base-sha")
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if len(ids) != 1 || ids[0] != "_my-policy_kyverno.io_ClusterPolicy" {
			t.Errorf("call %d: ids = %v", i, ids)
		}
	}
	if contentCalls != 1 || parseCalls != 1 {
		t.Errorf("expected 1 contents fetch + 1 parse across 3 cached reads, got contents=%d parse=%d", contentCalls, parseCalls)
	}

	// Absent at base (404) → empty, no error.
	ids, err := c.BaseFileResourceIDs(ctx, "owner", "repo", "policies/gone.yaml", "base-sha")
	if err != nil {
		t.Fatalf("404 should not error: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("absent-at-base file should yield no IDs, got %v", ids)
	}

	// Empty ref → no fetch, no IDs.
	if ids, err := c.BaseFileResourceIDs(ctx, "owner", "repo", "policies/p.yaml", ""); err != nil || ids != nil {
		t.Errorf("empty ref: ids=%v err=%v, want (nil, nil)", ids, err)
	}
}

// TestBlobResourceIDs_NoParserErrors guards the misconfiguration: without a
// parser wired, BlobResourceIDs must error rather than silently return no
// declared resources (which would drop every PR from inventory matching).
func TestBlobResourceIDs_NoParserErrors(t *testing.T) {
	mux := http.NewServeMux()
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(8)})
	if _, err := c.BlobResourceIDs(context.Background(), "owner", "repo", "abc123"); err == nil {
		t.Fatal("expected an error when no BlobResourceParser is configured")
	}
}

// TestFindPRsForCommit_CachesMergeWalk pins the autodev win. Walking
// the parent chain twice — same starting SHA — must hit GetCommit
// exactly ONCE per ancestor: every later walk hits the cache for the
// SAME merge ancestors. Without this, autodev with a deep parent chain
// burns one GetCommit call per hop per event (50 events × 50 hops =
// 2500 calls; with the cache, ~50 calls total).
//
// Mock structure: a 3-deep merge chain (head → m1 → m2 → m3 → base).
// Each merge has 2 parents; parents[1] is a feature-branch tip that
// directly resolves to a PR.
func TestFindPRsForCommit_CachesMergeWalk(t *testing.T) {
	commits := map[string]map[string]any{
		"head": {"sha": "head", "parents": []map[string]any{{"sha": "m1"}, {"sha": "feature-a"}}},
		"m1":   {"sha": "m1", "parents": []map[string]any{{"sha": "m2"}, {"sha": "feature-b"}}},
		"m2":   {"sha": "m2", "parents": []map[string]any{{"sha": "m3"}, {"sha": "feature-c"}}},
		"m3":   {"sha": "m3", "parents": []map[string]any{{"sha": "base"}, {"sha": "feature-d"}}},
		"base": {"sha": "base", "parents": []map[string]any{{"sha": "older"}}}, // single parent ⇒ stop
	}

	commitCalls := map[string]int{}
	prCalls := map[string]int{}
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/", func(w http.ResponseWriter, r *http.Request) {
		// Path is /api/v3/repos/owner/repo/commits/{sha} OR
		// /api/v3/repos/owner/repo/commits/{sha}/pulls
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/commits/"), "/")
		sha := parts[0]
		w.Header().Set("Content-Type", "application/json")
		if len(parts) == 2 && parts[1] == "pulls" {
			mu.Lock()
			prCalls[sha]++
			mu.Unlock()
			// Direct lookup: only feature-* SHAs return PRs.
			if strings.HasPrefix(sha, "feature-") {
				num, _ := strconv.Atoi(string(sha[len(sha)-1])) // 'a'..'d' → 97..100 — fine, just need uniqueness
				_ = num
				_ = json.NewEncoder(w).Encode([]map[string]any{
					{"number": int(sha[len(sha)-1]), "state": "open", "labels": []map[string]any{}},
				})
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{})
			return
		}
		// /commits/{sha} → commit metadata
		mu.Lock()
		commitCalls[sha]++
		mu.Unlock()
		body, ok := commits[sha]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	})

	cache := fetchcache.NewLRU(64)
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{
		Cache:           cache,
		ParentWalkDepth: 10,
	})

	ctx := context.Background()

	// First walk: every commit + every parent[1] PR lookup is a miss.
	prs1, err := c.FindPRsForCommit(ctx, "owner", "repo", "head")
	if err != nil {
		t.Fatalf("first walk: %v", err)
	}
	if len(prs1) != 4 {
		t.Fatalf("first walk: got %d PRs, want 4 (feature-a..d)", len(prs1))
	}
	firstCommitCalls := totalInt(commitCalls)
	if firstCommitCalls < 4 {
		t.Fatalf("first walk: expected at least 4 GetCommit calls (head + m1..m3), got %d", firstCommitCalls)
	}

	// Second walk from the SAME head: GetCommit calls must NOT increase
	// (every ancestor is now cached). PR-for-commit calls also reuse the
	// cache (PRsForCommitTTL > 0 by default when Options.Cache is set).
	// The whole-resolution entry is evicted first, so the walk really runs
	// and only the per-commit caches can save the calls.
	cache.Delete(ctx, "prs-walk:owner/repo/head")
	prs2, err := c.FindPRsForCommit(ctx, "owner", "repo", "head")
	if err != nil {
		t.Fatalf("second walk: %v", err)
	}
	if len(prs2) != len(prs1) {
		t.Errorf("second walk PR count = %d, want %d", len(prs2), len(prs1))
	}
	if got := totalInt(commitCalls); got != firstCommitCalls {
		t.Errorf("GetCommit calls increased on second walk: %d → %d (cache miss — autodev would burn one round-trip per hop per event without this)",
			firstCommitCalls, got)
	}
}

func totalInt(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// TestFindPRsForCommit_NoCacheRewalks is the negative counterpart to
// TestFindPRsForCommit_CachesMergeWalk: with no cache wired (the
// commenter falls back to fetchcache.Disabled()) the second walk MUST
// re-fetch every commit. Pins that the previous test's pass isn't a
// tautology — disabling the cache actually changes behavior.
func TestFindPRsForCommit_NoCacheRewalks(t *testing.T) {
	commits := map[string]map[string]any{
		"head": {"sha": "head", "parents": []map[string]any{{"sha": "m1"}, {"sha": "feature-a"}}},
		"m1":   {"sha": "m1", "parents": []map[string]any{{"sha": "base"}, {"sha": "feature-b"}}},
		"base": {"sha": "base", "parents": []map[string]any{{"sha": "older"}}},
	}
	commitCalls := map[string]int{}
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/commits/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/commits/"), "/")
		sha := parts[0]
		w.Header().Set("Content-Type", "application/json")
		if len(parts) == 2 && parts[1] == "pulls" {
			if strings.HasPrefix(sha, "feature-") {
				_ = json.NewEncoder(w).Encode([]map[string]any{
					{"number": int(sha[len(sha)-1]), "state": "open", "labels": []map[string]any{}},
				})
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{})
			return
		}
		mu.Lock()
		commitCalls[sha]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(commits[sha])
	})

	// NO Cache option → falls back to Disabled.
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{
		ParentWalkDepth: 10,
	})

	ctx := context.Background()
	if _, err := c.FindPRsForCommit(ctx, "owner", "repo", "head"); err != nil {
		t.Fatalf("first walk: %v", err)
	}
	first := totalInt(commitCalls)
	if _, err := c.FindPRsForCommit(ctx, "owner", "repo", "head"); err != nil {
		t.Fatalf("second walk: %v", err)
	}
	if got := totalInt(commitCalls); got != 2*first {
		t.Errorf("without cache the second walk must re-fetch every commit (expected %d, got %d). "+
			"If this fails, the cache is somehow on by default — investigate the Options.Cache fallback in NewCommenter.",
			2*first, got)
	}
}

// TestPostOrUpdateComment_LastCommentCacheSkipsListComments is the
// load-bearing regression test for the second-and-subsequent posts to the
// same (PR, Kustomization scope). The first post pays for ListComments; every
// subsequent post MUST skip ListComments and act on the cached comment ID
// directly — delete+recreate when the body changed, and do NOTHING at all
// when it didn't (skip-if-unchanged).
//
// Without this, an autodev batch with 200 PRs × N events burns 200×N
// ListComments calls. With it, only the first event per (PR, scope) lists.
func TestPostOrUpdateComment_LastCommentCacheSkipsListComments(t *testing.T) {
	var listCalls, deleteCalls, createCalls int
	mu := sync.Mutex{}

	marker := "<!-- flux-ai-bridge:apps/api -->"
	live := map[int64]map[string]any{
		7: {"id": float64(7), "body": marker + "\nold"},
	}
	nextID := int64(8)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			mu.Lock()
			listCalls++
			var out []map[string]any
			for _, c := range live {
				out = append(out, c)
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(out)
		case http.MethodPost:
			mu.Lock()
			createCalls++
			id := nextID
			nextID++
			live[id] = map[string]any{"id": float64(id), "body": marker + "\nfresh"}
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(id), "body": marker + "\nfresh"})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		id, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		mu.Lock()
		deleteCalls++
		_, ok := live[id]
		delete(live, id)
		mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{
		Cache: fetchcache.NewLRU(64),
	})

	ctx := context.Background()

	// FIRST post: cache miss → ListComments finds id=7 (different body) →
	// delete it + create a fresh one.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 1"); err != nil {
		t.Fatalf("first post: %v", err)
	}
	if listCalls != 1 {
		t.Errorf("first post: expected 1 ListComments (cache miss), got %d", listCalls)
	}
	if deleteCalls != 1 || createCalls != 1 {
		t.Errorf("first post: expected delete(id=7)+create (delete=1 create=1); got delete=%d create=%d", deleteCalls, createCalls)
	}

	// SECOND post, different body: cache HIT → delete-by-cached-ID + create,
	// no ListComments.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 2"); err != nil {
		t.Fatalf("second post: %v", err)
	}
	if listCalls != 1 {
		t.Errorf("second post: ListComments must be SKIPPED on cache hit; got %d (want 1)", listCalls)
	}
	if createCalls != 2 {
		t.Errorf("second post: expected 2 total creates, got %d", createCalls)
	}

	// THIRD post, IDENTICAL body to the second: cache hit + hash match →
	// no GitHub mutation at all (no list, no delete, no create).
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 2"); err != nil {
		t.Fatalf("third post: %v", err)
	}
	if listCalls != 1 {
		t.Errorf("third post: ListComments must STILL be skipped; got %d (want 1)", listCalls)
	}
	if createCalls != 2 {
		t.Errorf("third post: identical body must be a no-op; expected creates to stay at 2, got %d", createCalls)
	}
}

// TestPostOrUpdateComment_DeleteCache404Tolerant covers the race-recovery
// path: when the cached comment ID returns 404 on DELETE (a peer replica or
// user already removed it), the bridge treats it as "already gone," posts the
// fresh comment, and does NOT fall back to ListComments — nothing of our
// marker remains by the time the 404 returns.
func TestPostOrUpdateComment_DeleteCache404Tolerant(t *testing.T) {
	var listCalls, createCalls, deleteCalls int
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			mu.Lock()
			listCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case http.MethodPost:
			mu.Lock()
			createCalls++
			id := 100 + createCalls
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(id)})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			// Always 404 to simulate "comment already gone when we tried to
			// delete our cached ID."
			mu.Lock()
			deleteCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
		}
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{
		Cache: fetchcache.NewLRU(64),
	})
	ctx := context.Background()

	// First post seeds the cache (empty list → create id=101).
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 1"); err != nil {
		t.Fatalf("first post: %v", err)
	}
	listAfterFirst := listCalls // 1 (cache miss)

	// Second post, different body: cache hit → DELETE returns 404 (already
	// gone) → tolerated → CreateComment, NO ListComments fallback.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 2"); err != nil {
		t.Fatalf("second post (DELETE-404 path): %v", err)
	}
	if deleteCalls != 1 {
		t.Errorf("expected exactly one delete attempt; got %d", deleteCalls)
	}
	if listCalls != listAfterFirst {
		t.Errorf("DELETE-404 must NOT trigger ListComments fallback; got %d total (want %d)", listCalls, listAfterFirst)
	}
	if createCalls != 2 {
		t.Errorf("expected 2 total CreateComment calls (one per post); got %d", createCalls)
	}
}

// TestPostOrUpdateComment_KeepHistoryBypassesLastCommentCache: when
// keepHistory mode is on, the bridge appends comments without deleting.
// The last-comment cache MUST NOT be consulted (there's no DELETE to
// optimise) and the result also MUST NOT pollute the cache (a future
// non-keepHistory operator change would then DELETE the wrong comment).
func TestPostOrUpdateComment_KeepHistoryBypassesLastCommentCache(t *testing.T) {
	var listCalls, deleteCalls, createCalls int
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			mu.Lock()
			listCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case http.MethodPost:
			mu.Lock()
			createCalls++
			id := 500 + createCalls
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(id)})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			deleteCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{
		KeepCommentHistory: true,
		Cache:              fetchcache.NewLRU(64),
	})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body"); err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}
	if listCalls != 0 {
		t.Errorf("keepHistory must skip ListComments; got %d", listCalls)
	}
	if deleteCalls != 0 {
		t.Errorf("keepHistory must NOT delete prior comments; got %d", deleteCalls)
	}
	if createCalls != 3 {
		t.Errorf("expected 3 CreateComment calls (one per post); got %d", createCalls)
	}
}

// TestSetPRLabels covers the three outcomes the bridge produces. For each:
//   - the right label is added via POST /issues/{n}/labels
//   - the opposite label is removed via DELETE /issues/{n}/labels/{label}
//   - a 404 on the remove is swallowed (idempotent across replays)
//
// Recorded request paths are compared against the expected sequence so a
// future refactor that breaks the contract trips this test loudly.
func TestSetPRLabels(t *testing.T) {
	type req struct {
		Method string
		Path   string
		Body   []string // for POST /labels: the labels[] payload
	}
	cases := []struct {
		name    string
		outcome bridgegithub.Outcome
		want    []req
	}{
		{
			name:    "success → add flux-success, remove flux-failed + flux-warning",
			outcome: bridgegithub.OutcomeSuccess,
			want: []req{
				{Method: http.MethodPost, Path: "/api/v3/repos/owner/repo/issues/42/labels", Body: []string{"flux-success"}},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-failed"},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-warning"},
			},
		},
		{
			name:    "failure → add flux-failed, remove flux-success + flux-warning",
			outcome: bridgegithub.OutcomeFailure,
			want: []req{
				{Method: http.MethodPost, Path: "/api/v3/repos/owner/repo/issues/42/labels", Body: []string{"flux-failed"}},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-success"},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-warning"},
			},
		},
		{
			name:    "warning → add flux-warning, remove flux-success + flux-failed",
			outcome: bridgegithub.OutcomeWarning,
			want: []req{
				{Method: http.MethodPost, Path: "/api/v3/repos/owner/repo/issues/42/labels", Body: []string{"flux-warning"}},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-success"},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-failed"},
			},
		},
		{
			name:    "none → remove all three, no add",
			outcome: bridgegithub.OutcomeNone,
			want: []req{
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-success"},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-failed"},
				{Method: http.MethodDelete, Path: "/api/v3/repos/owner/repo/issues/42/labels/flux-warning"},
			},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var got []req
			var mu sync.Mutex
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				// go-github sends a bare JSON array (["flux-success"]).
				var labels []string
				_ = json.NewDecoder(r.Body).Decode(&labels)
				mu.Lock()
				got = append(got, req{Method: r.Method, Path: r.URL.Path, Body: labels})
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode([]any{})
			})
			mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				mu.Lock()
				got = append(got, req{Method: r.Method, Path: r.URL.Path})
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
			})
			cm := newCommenter(t, mux)
			if err := cm.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", c.outcome); err != nil {
				t.Fatalf("SetPRLabels: %v", err)
			}
			if len(got) != len(c.want) {
				t.Fatalf("got %d requests, want %d: %+v", len(got), len(c.want), got)
			}
			for i, w := range c.want {
				if got[i].Method != w.Method || got[i].Path != w.Path {
					t.Errorf("req %d: got %s %s, want %s %s", i, got[i].Method, got[i].Path, w.Method, w.Path)
				}
				if len(w.Body) > 0 {
					if len(got[i].Body) != len(w.Body) || got[i].Body[0] != w.Body[0] {
						t.Errorf("req %d body: got %v, want %v", i, got[i].Body, w.Body)
					}
				}
			}
		})
	}
}

// Custom label names (Options.PRLabelSuccess/PRLabelFailure) must drive the
// add/remove ops instead of the flux-success/flux-failed defaults.
func TestSetPRLabels_CustomNames(t *testing.T) {
	var got []string // "POST <label>" / "DELETE <label>"
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		var labels []string
		_ = json.NewDecoder(r.Body).Decode(&labels)
		mu.Lock()
		for _, l := range labels {
			got = append(got, "POST "+l)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, "DELETE "+strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/issues/42/labels/"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	cm := newCommenterWithOptions(t, mux, bridgegithub.Options{PRLabelSuccess: "deploy-ok", PRLabelFailure: "deploy-broken", PRLabelWarning: "deploy-pending"})
	if err := cm.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure); err != nil {
		t.Fatalf("SetPRLabels: %v", err)
	}
	want := map[string]bool{"POST deploy-broken": true, "DELETE deploy-ok": true, "DELETE deploy-pending": true}
	if len(got) != 3 {
		t.Fatalf("custom-name failure ops = %v, want 3 ops", got)
	}
	for _, op := range got {
		if !want[op] {
			t.Errorf("unexpected op %q; want a subset of POST deploy-broken / DELETE deploy-ok / DELETE deploy-pending", op)
		}
	}
}

// Setting the warning label equal to the failed label must make a warning
// outcome ADD that label (so a merge gate keyed on it blocks) WITHOUT the
// add/remove pair cancelling out — i.e. we never DELETE a label we just added.
func TestSetPRLabels_WarningEqualsFailed_NoSelfRemove(t *testing.T) {
	var got []string // "POST <label>" / "DELETE <label>"
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		var labels []string
		_ = json.NewDecoder(r.Body).Decode(&labels)
		mu.Lock()
		for _, l := range labels {
			got = append(got, "POST "+l)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, "DELETE "+strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/issues/42/labels/"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	// warning == failed == "flux-failed"
	cm := newCommenterWithOptions(t, mux, bridgegithub.Options{PRLabelWarning: "flux-failed"})
	if err := cm.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", bridgegithub.OutcomeWarning); err != nil {
		t.Fatalf("SetPRLabels: %v", err)
	}
	// Expect: POST flux-failed, DELETE flux-success. The failed label must NOT
	// be in the remove set even though the warning→remove list names it.
	for _, op := range got {
		if op == "DELETE flux-failed" {
			t.Fatalf("warning removed the label it was adding (flux-failed); ops = %v", got)
		}
	}
	hasPost, hasDelSuccess := false, false
	for _, op := range got {
		switch op {
		case "POST flux-failed":
			hasPost = true
		case "DELETE flux-success":
			hasDelSuccess = true
		}
	}
	if !hasPost || !hasDelSuccess || len(got) != 2 {
		t.Errorf("warning==failed ops = %v, want exactly POST flux-failed + DELETE flux-success", got)
	}
}

// TestSetPRLabels_TransitionOnly pins the API-call-saving contract:
// when the outcome hasn't changed since the last successful write, the
// bridge MUST NOT call the labels API again. Asserts both:
//   - Two consecutive same-outcome calls → exactly one set of API ops
//   - A subsequent different-outcome call → API hit again, then cached
func TestSetPRLabels_TransitionOnly(t *testing.T) {
	var (
		mu        sync.Mutex
		postCalls int
		delCalls  int
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mu.Lock()
			postCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]any{})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			delCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}
	})

	// Wire a real LRU cache (default newCommenter uses Disabled). The
	// labels endpoint is what we're verifying gets skipped on repeats —
	// the cache is the mechanism.
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	// First call: success outcome → 1 POST (add success) + 2 DELETEs
	// (remove failed + warning). Cache is empty, so this is a clean miss → API.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("call 1: %v", err)
	}
	if postCalls != 1 || delCalls != 2 {
		t.Fatalf("after call 1: post=%d del=%d, want post=1 del=2", postCalls, delCalls)
	}

	// Second call with the SAME outcome — must be a no-op. This is the
	// load-bearing assertion: if the bridge re-issues the API ops on
	// every reconcile, the user pays for it on every Flux event.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("call 2: %v", err)
	}
	if postCalls != 1 || delCalls != 2 {
		t.Errorf("after call 2 (same outcome): post=%d del=%d, want unchanged (1, 2)", postCalls, delCalls)
	}

	// Third call with a DIFFERENT outcome — cache mismatches, so the
	// transition triggers the full reconciliation again (1 POST + 2 DELETEs).
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure); err != nil {
		t.Fatalf("call 3: %v", err)
	}
	if postCalls != 2 || delCalls != 4 {
		t.Errorf("after call 3 (outcome flipped): post=%d del=%d, want post=2 del=4", postCalls, delCalls)
	}

	// Fourth call: failure again — cached, no API.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure); err != nil {
		t.Fatalf("call 4: %v", err)
	}
	if postCalls != 2 || delCalls != 4 {
		t.Errorf("after call 4 (same outcome as 3): post=%d del=%d, want unchanged (2, 4)", postCalls, delCalls)
	}
}

// TestSetPRLabels_DoesNotCacheOnPartialFailure pins the rollback rule:
// if a label op errors mid-way, the cache must NOT be written. The next
// call (even with the same outcome) must hit the API again to retry,
// not return a false-positive "already done" from a stale cache entry.
func TestSetPRLabels_DoesNotCacheOnPartialFailure(t *testing.T) {
	var (
		mu        sync.Mutex
		postCalls int
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mu.Lock()
			postCalls++
			mu.Unlock()
			// First two POSTs fail with 500; once we let one through,
			// the cache writes and the next call is a cache hit.
			if postCalls <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]any{})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusOK)
		}
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	// First two calls error. The cache must remain empty across them.
	for i := 1; i <= 2; i++ {
		if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err == nil {
			t.Fatalf("call %d: expected error from 500, got nil", i)
		}
	}
	// Third call succeeds (POST returns 200) — cache populates.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("call 3: %v", err)
	}
	if postCalls != 3 {
		t.Fatalf("expected 3 POST attempts (cache must NOT short-circuit on prior failures); got %d", postCalls)
	}
	// Fourth call is the steady-state cache hit.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("call 4: %v", err)
	}
	if postCalls != 3 {
		t.Errorf("call 4 after successful write must be a cache hit; got %d POST calls", postCalls)
	}
}

// A transition that adds the new label but fails to remove the old one must
// not leave the PREVIOUS outcome's record in the cache: a later call with that
// outcome would hit it and leave both labels on the PR.
func TestSetPRLabels_PartialTransitionInvalidatesPriorOutcome(t *testing.T) {
	var (
		mu      sync.Mutex
		failDel bool
		ops     []string
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		var names []string
		_ = json.NewDecoder(r.Body).Decode(&names)
		mu.Lock()
		ops = append(ops, "add "+strings.Join(names, ","))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		ops = append(ops, "remove "+strings.TrimPrefix(r.URL.Path, "/api/v3/repos/owner/repo/issues/42/labels/"))
		if failDel {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("seed success: %v", err)
	}
	mu.Lock()
	failDel = true
	mu.Unlock()
	// flux-failed is added, removing flux-success fails.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeFailure); err == nil {
		t.Fatal("failure transition with a failing DELETE must return an error")
	}
	mu.Lock()
	failDel, ops = false, nil
	mu.Unlock()

	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("back to success: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	got := strings.Join(ops, "; ")
	if !strings.Contains(got, "add "+bridgegithub.DefaultPRLabelSuccess) || !strings.Contains(got, "remove "+bridgegithub.DefaultPRLabelFailure) {
		t.Fatalf("success after a partial failure transition must reconcile (add %s, remove %s); got [%s]",
			bridgegithub.DefaultPRLabelSuccess, bridgegithub.DefaultPRLabelFailure, got)
	}
}

// Renaming an outcome label must re-apply the labels at the same head: the
// record written under the old names must not short-circuit the new one.
func TestSetPRLabels_RenamedLabelReappliesAtSameHead(t *testing.T) {
	var (
		mu    sync.Mutex
		added []string
	)
	labelsMux := func() *http.ServeMux {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
			var names []string
			_ = json.NewDecoder(r.Body).Decode(&names)
			mu.Lock()
			added = append(added, names...)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]any{})
		})
		mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		return mux
	}
	shared := fetchcache.NewLRU(64) // stands for Redis across a restart
	ctx := context.Background()

	before := newCommenterWithOptions(t, labelsMux(), bridgegithub.Options{Cache: shared})
	if err := before.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("before rename: %v", err)
	}
	after := newCommenterWithOptions(t, labelsMux(), bridgegithub.Options{Cache: shared, PRLabelSuccess: "deploy-ok"})
	if err := after.SetPRLabels(ctx, "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("after rename: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(added) != 2 || added[1] != "deploy-ok" {
		t.Fatalf("added labels = %v, want the renamed label deploy-ok applied at the same head", added)
	}
}

// TestSetPRLabels_RemoveLabelNotFoundIsSwallowed pins the idempotency
// contract: a 404 on RemoveLabelForIssue means "label wasn't on the PR" —
// the next state-transition replay must not trip on it.
func TestSetPRLabels_RemoveLabelNotFoundIsSwallowed(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]any{})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	c := newCommenter(t, mux)
	if err := c.SetPRLabels(context.Background(), "owner", "repo", 42, "h1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("SetPRLabels must swallow 404 on remove; got %v", err)
	}
}

// TestPostOrUpdateComment_CreateBeforeDelete pins the swap ordering that
// keeps a verdict on the PR at all times: on a body change the FRESH comment
// is posted before the prior one is deleted (a crash between the calls used
// to leave the PR with no comment until the next re-eval window — observed
// during a rolling deploy), and a FAILED create must leave the old comment
// untouched (no delete at all) so the next event retries the whole swap.
func TestPostOrUpdateComment_CreateBeforeDelete(t *testing.T) {
	var ops []string
	var failCreate bool
	live := map[int64]string{}
	nextID := int64(100)
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			mu.Lock()
			ops = append(ops, "list")
			var out []map[string]any
			for id, body := range live {
				out = append(out, map[string]any{"id": float64(id), "body": body})
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(out)
		case http.MethodPost:
			var in struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&in)
			mu.Lock()
			fail := failCreate
			ops = append(ops, "create")
			nextID++
			id := nextID
			if !fail {
				live[id] = in.Body
			}
			mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(id)})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			id, _ := strconv.ParseInt(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], 10, 64)
			mu.Lock()
			ops = append(ops, "delete")
			delete(live, id)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 1"); err != nil {
		t.Fatalf("first post: %v", err)
	}
	mu.Lock()
	ops = nil // only observe the swap
	mu.Unlock()

	// Changed body via the cache-hit fast path: create must precede delete.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 2"); err != nil {
		t.Fatalf("second post: %v", err)
	}
	mu.Lock()
	got := strings.Join(ops, ",")
	mu.Unlock()
	if got != "create,delete" {
		t.Fatalf("swap order must be create-then-delete, got [%s]", got)
	}

	// Failed create: the old comment must NOT be deleted. The cache is left
	// cold (the create may have landed despite the error), so the next
	// attempt lists the comments and retries the full swap.
	mu.Lock()
	ops, failCreate = nil, true
	mu.Unlock()
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 3"); err == nil {
		t.Fatal("failed create must surface an error")
	}
	mu.Lock()
	got = strings.Join(ops, ",")
	ops, failCreate = nil, false
	mu.Unlock()
	if got != "create" {
		t.Fatalf("failed create must not delete the old comment, got [%s]", got)
	}
	// Retry succeeds and swaps in order again, removing the old comment.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 3"); err != nil {
		t.Fatalf("retry post: %v", err)
	}
	mu.Lock()
	got = strings.Join(ops, ",")
	n := len(live)
	mu.Unlock()
	if got != "list,create,delete" {
		t.Fatalf("retry must list, then swap create-then-delete, got [%s]", got)
	}
	if n != 1 {
		t.Fatalf("retry must leave exactly one comment, got %d", n)
	}
}

// TestPostOrUpdateComment_DeleteFailureForcesSweep pins the duplicate-cleanup
// guarantee behind create-before-delete: when the post-create DELETE of the
// prior comment fails (non-404), the cache entry must be dropped so the NEXT
// post takes the list-based slow path and sweeps the orphan — a hot cache
// would short-circuit "unchanged" forever and the duplicate would never go.
func TestPostOrUpdateComment_DeleteFailureForcesSweep(t *testing.T) {
	var listCalls, createCalls, deleteCalls int
	var failDelete bool
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			mu.Lock()
			listCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case http.MethodPost:
			mu.Lock()
			createCalls++
			id := 100 + createCalls
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(id)})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			deleteCalls++
			fail := failDelete
			mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 1"); err != nil {
		t.Fatalf("first post: %v", err)
	}

	// Fast-path swap whose post-create delete fails: tolerated (fresh comment
	// is live), but the cache must be cleared.
	mu.Lock()
	failDelete = true
	mu.Unlock()
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 2"); err != nil {
		t.Fatalf("second post must tolerate the delete failure: %v", err)
	}
	mu.Lock()
	failDelete = false
	listsBefore := listCalls
	mu.Unlock()

	// Same body again: a hot cache would skip silently; the cleared cache
	// must force the list-based slow path that sweeps duplicates.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "body 2"); err != nil {
		t.Fatalf("third post: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if listCalls != listsBefore+1 {
		t.Fatalf("delete failure must force the next post through the slow-path sweep; lists %d → %d", listsBefore, listCalls)
	}
}

// TestPostOrUpdateComment_SlowPathDeleteFailureTolerated pins the slow-path
// half of the tolerance contract: once the fresh comment is created, a failed
// sweep-delete must NOT fail the delivery (the verdict is live — an error
// would roll back the admission mark and re-run the whole analysis just to
// clean a duplicate). The cache is cleared instead, so the next post re-lists
// and finishes the sweep.
func TestPostOrUpdateComment_SlowPathDeleteFailureTolerated(t *testing.T) {
	var listCalls int
	var failDelete bool
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			mu.Lock()
			listCalls++
			mu.Unlock()
			// One pre-existing marker comment with a DIFFERENT body → the
			// slow path swaps: create fresh, then sweep this one.
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": float64(90), "body": "<!-- flux-ai-bridge:apps/api -->\nold body"},
			})
		case http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(101)})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			fail := failDelete
			mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	mu.Lock()
	failDelete = true
	mu.Unlock()
	// Cold cache → slow path → create ok, sweep-delete fails → tolerated.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "new body"); err != nil {
		t.Fatalf("slow-path delete failure must be tolerated (verdict is live): %v", err)
	}
	mu.Lock()
	failDelete = false
	listsBefore := listCalls
	mu.Unlock()
	// Cache was cleared → the next post re-lists and completes the sweep.
	if err := c.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "new body"); err != nil {
		t.Fatalf("follow-up post: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if listCalls != listsBefore+1 {
		t.Fatalf("cleared cache must force a re-list on the next post; lists %d → %d", listsBefore, listCalls)
	}
}

// TestPostOrUpdateCommentKeyed_ColdCacheHonorsEmbeddedKey pins the cold-cache
// half of the stability-key contract: after a cache loss (restart, or the
// deliberate clear following a delete failure), the slow path must recover
// the key from the comment's own hidden marker — an unchanged STATE with
// re-worded LLM prose must NOT swap the comment, while a changed state must.
func TestPostOrUpdateCommentKeyed_ColdCacheHonorsEmbeddedKey(t *testing.T) {
	marker := "<!-- flux-ai-bridge:flux-system/podinfo -->"
	sum := sha256.Sum256([]byte(marker + "\nstable state A"))
	keyMarker := "<!-- flux-ai-bridge-key:" + hex.EncodeToString(sum[:]) + " -->"
	// The PR carries a comment posted under state A with "wording one".
	existing := map[string]any{"id": float64(99), "body": marker + "\n" + keyMarker + "\nwording one"}
	var created, deleted bool
	mu := sync.Mutex{}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/comments", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode([]any{existing})
		case http.MethodPost:
			mu.Lock()
			created = true
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": float64(100)})
		}
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/comments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			deleted = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	// Cold cache, same stability key, different prose → keep the comment.
	if err := c.PostOrUpdateCommentKeyed(ctx, "owner", "repo", "flux-system", "podinfo", 42, "wording two (re-worded)", "stable state A"); err != nil {
		t.Fatalf("keyed post: %v", err)
	}
	mu.Lock()
	if created || deleted {
		mu.Unlock()
		t.Fatalf("unchanged state must not swap on a cold cache; created=%v deleted=%v", created, deleted)
	}
	mu.Unlock()

	// Changed state → swap (create-before-delete).
	if err := c.PostOrUpdateCommentKeyed(ctx, "owner", "repo", "flux-system", "podinfo", 42, "wording three", "different state B"); err != nil {
		t.Fatalf("changed-state post: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !created || !deleted {
		t.Fatalf("changed state must swap; created=%v deleted=%v", created, deleted)
	}
}

// Regression for a PR where a reviewer manually removed flux-success and
// a NEW commit didn't restore it: the transition cache was PR-scoped, so the
// new head's identical aggregate hit the cache and skipped the write. The
// cache entry is head-scoped now: same head + same outcome stays a no-op
// (a manual removal is a deliberate override for that head), but a new head
// always re-asserts the labels.
func TestSetPRLabels_NewHeadReassertsAfterManualRemoval(t *testing.T) {
	var addCalls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			atomic.AddInt32(&addCalls, 1)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	})
	mux.HandleFunc("/api/v3/repos/owner/repo/issues/42/labels/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound) // removes: label wasn't present — fine
	})

	c := newCommenterWithOptions(t, mux, bridgegithub.Options{Cache: fetchcache.NewLRU(64)})
	ctx := context.Background()

	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "head-1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Same head, same outcome: cached no-op — a reviewer's manual removal
	// stands for this head.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "head-1", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("second: %v", err)
	}
	if n := atomic.LoadInt32(&addCalls); n != 1 {
		t.Fatalf("same head must stay cached; add calls = %d, want 1", n)
	}
	// New commit: the cache misses and the labels are re-asserted even though
	// the aggregate outcome is unchanged.
	if err := c.SetPRLabels(ctx, "owner", "repo", 42, "head-2", bridgegithub.OutcomeSuccess); err != nil {
		t.Fatalf("new head: %v", err)
	}
	if n := atomic.LoadInt32(&addCalls); n != 2 {
		t.Fatalf("new head must re-assert labels; add calls = %d, want 2", n)
	}
}
