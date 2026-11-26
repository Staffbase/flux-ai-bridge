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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
)

// parkFirstCreate wraps a replica's view of srv: once armed, the first
// comment create (by any replica) closes parked and is held for hold.
func parkFirstCreate(srv *commentServer, armed, taken *atomic.Bool, parked chan struct{}, hold time.Duration) *http.ServeMux {
	inner := srv.mux()
	outer := http.NewServeMux()
	outer.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/issues/42/comments") &&
			armed.Load() && taken.CompareAndSwap(false, true) {
			close(parked)
			time.Sleep(hold)
		}
		inner.ServeHTTP(w, r)
	})
	return outer
}

// Two replicas posting different states for the same PR and scope at once
// (two heads of one PR, handled by different pods) both read the same
// comment record. Unserialized, both create a comment and both delete the
// same predecessor, so one new comment is never swept and the PR keeps two
// verdicts. The swap must run under a cross-replica lock, so the second
// replica swaps the first one's comment.
func TestPostOrUpdateComment_SerializesSwapsAcrossReplicas(t *testing.T) {
	mr, replicaA, replicaB := labelReplicas(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := &commentServer{live: map[int64]string{}, nextID: 1}
	var armed, taken atomic.Bool
	parked := make(chan struct{})
	a := newCommenterWithOptions(t, parkFirstCreate(srv, &armed, &taken, parked, 300*time.Millisecond), bridgegithub.Options{Cache: replicaA})
	b := newCommenterWithOptions(t, parkFirstCreate(srv, &armed, &taken, parked, 300*time.Millisecond), bridgegithub.Options{Cache: replicaB})

	if err := a.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "state 0"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	armed.Store(true)

	doneA := make(chan error, 1)
	go func() { doneA <- a.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "state 1") }()
	waitClosed(t, parked, "replica A never created its comment")
	doneB := make(chan error, 1)
	go func() { doneB <- b.PostOrUpdateComment(ctx, "owner", "repo", "apps", "api", 42, "state 2") }()

	if err := recvErr(t, doneA, "replica A"); err != nil {
		t.Fatalf("replica A: %v", err)
	}
	if err := recvErr(t, doneB, "replica B"); err != nil {
		t.Fatalf("replica B: %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.live) != 1 {
		t.Fatalf("want one comment after both swaps, got %d: %v", len(srv.live), srv.live)
	}
	for _, body := range srv.live {
		if !strings.HasSuffix(body, "state 2") {
			t.Fatalf("live comment = %q, want the later post (state 2)", body)
		}
	}
	if mr.Exists("fc:last-comment:owner/repo/42/apps/api:lock") {
		t.Error("the comment lock was not released")
	}
}
