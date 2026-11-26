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

package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/lifecycle"
)

// fakeGetter returns canned source-object YAML and counts calls (race-safe).
type fakeGetter struct {
	yaml  string
	err   error
	calls int32
}

func (f *fakeGetter) GetKubernetesResource(_ context.Context, _, _, _, _ string) ([]byte, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.err != nil {
		return nil, f.err
	}
	return []byte(f.yaml), nil
}

func sourceYAML(url, revision, digest string) string {
	return fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nstatus:\n  artifact:\n    url: %s\n    revision: %q\n    digest: %s\n", url, revision, digest)
}

func TestRevisionMatches(t *testing.T) {
	cases := []struct {
		artifact, event string
		want            bool
	}{
		{"dev@sha1:da6a06f4194ded4db3ee543383eb979fe9b5ccd7", "da6a06f4194ded4db3ee543383eb979fe9b5ccd7", true},
		{"dev@sha1:da6a06f4194ded4db3ee543383eb979fe9b5ccd7", "da6a06f4", true}, // short prefix
		{"dev@sha1:da6a06f4194ded4db3ee543383eb979fe9b5ccd7", "5008d2ea3f46dda01d9d479e776257f1e29d7200", false},
		{"v1.2.3@sha1:abcdef1234567", "abcdef1234567", true},
		{"", "abc", false},
	}
	for _, c := range cases {
		if got := revisionMatches(c.artifact, c.event); got != c.want {
			t.Errorf("revisionMatches(%q, %q) = %v, want %v", c.artifact, c.event, got, c.want)
		}
	}
}

func TestResolver_BuildFiles_HappyPathAndCache(t *testing.T) {
	tarball := makeTarGz(t, overlayFixture())
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	getter := &fakeGetter{yaml: sourceYAML(srv.URL, "dev@sha1:deadbeefcafe0001", digestOf(tarball))}
	r := NewResolver(getter, nil, srv.Client())

	set, err := r.BuildFilesWithComponents(context.Background(), Ref{Kind: "GitRepository", Name: "cluster", Namespace: "flux-system"}, "apps/web/dev/r1", Components{}, "deadbeefcafe0001")
	if err != nil {
		t.Fatalf("BuildFilesWithComponents: %v", err)
	}
	if _, ok := set["apps/web/base/helmrelease.yaml"]; !ok {
		t.Error("expected base resource in build set")
	}
	if _, ok := set["apps/web/prod/r1/values.yaml"]; ok {
		t.Error("sibling prod file must not be in dev build set")
	}

	// Second call at the same digest must be served from cache (no second GET).
	if _, err := r.BuildFilesWithComponents(context.Background(), Ref{Kind: "GitRepository", Name: "cluster"}, "apps/web/dev/r1", Components{}, "deadbeefcafe0001"); err != nil {
		t.Fatalf("BuildFilesWithComponents (cached): %v", err)
	}
	if hits != 1 {
		t.Errorf("artifact fetched %d times, want 1 (digest cache)", hits)
	}
}

// TestResolver_BuildFiles_SingleflightCollapsesConcurrentMisses proves that many
// callers racing on the same artifact (a reconcile storm fanning one revision
// across many Kustomization events) trigger exactly one download + parse.
func TestResolver_BuildFiles_SingleflightCollapsesConcurrentMisses(t *testing.T) {
	tarball := makeTarGz(t, overlayFixture())
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		time.Sleep(50 * time.Millisecond) // widen the window so misses overlap
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	getter := &fakeGetter{yaml: sourceYAML(srv.URL, "dev@sha1:deadbeefcafe0001", digestOf(tarball))}
	r := NewResolver(getter, nil, srv.Client())

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = r.BuildFilesWithComponents(context.Background(), Ref{Kind: "GitRepository", Name: "cluster"}, "apps/web/dev/r1", Components{}, "deadbeefcafe0001")
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("BuildFilesWithComponents[%d]: %v", i, e)
		}
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("artifact fetched %d times under concurrent misses, want 1 (singleflight)", got)
	}
}

func TestResolver_BuildFiles_RevisionMismatchSkips(t *testing.T) {
	tarball := makeTarGz(t, overlayFixture())
	fetched := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetched = true
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()

	// Source artifact is at a NEWER revision than the event — the matching bytes
	// are gone. BuildFilesWithComponents must refuse (and never fetch the wrong tarball).
	getter := &fakeGetter{yaml: sourceYAML(srv.URL, "dev@sha1:newrevision999", "sha256:d")}
	r := NewResolver(getter, nil, srv.Client())

	_, err := r.BuildFilesWithComponents(context.Background(), Ref{Kind: "GitRepository", Name: "cluster"}, "apps/web/dev/r1", Components{}, "oldrevision111")
	if err == nil {
		t.Fatal("expected ErrRevisionMismatch, got nil")
	}
	if fetched {
		t.Error("artifact should not be fetched on a revision mismatch")
	}
}

// Without a digest, a source that serves every revision at the same URL must
// not get the tree cached for the previous revision: the new revision's
// build inputs are what attribution needs.
func TestResolver_BuildFiles_DigestlessCacheKeyedByRevision(t *testing.T) {
	revA := makeTarGz(t, map[string]string{"apps/web/kustomization.yaml": "resources:\n  - a.yaml\n", "apps/web/a.yaml": "kind: A\n"})
	revB := makeTarGz(t, map[string]string{"apps/web/kustomization.yaml": "resources:\n  - b.yaml\n", "apps/web/b.yaml": "kind: B\n"})
	var serving atomic.Pointer[[]byte]
	serving.Store(&revA)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(*serving.Load())
	}))
	defer srv.Close()
	url := srv.URL + "/latest.tar.gz"
	getter := &fakeGetter{yaml: sourceYAML(url, "main@sha1:aaaaaaaa00000001", "")}
	r := NewResolver(getter, nil, srv.Client())
	ref := Ref{Kind: "GitRepository", Name: "cluster"}

	if _, err := r.BuildFilesWithComponents(context.Background(), ref, "apps/web", Components{}, "aaaaaaaa00000001"); err != nil {
		t.Fatalf("BuildFilesWithComponents at revision A: %v", err)
	}
	serving.Store(&revB)
	getter.yaml = sourceYAML(url, "main@sha1:bbbbbbbb00000002", "")
	set, err := r.BuildFilesWithComponents(context.Background(), ref, "apps/web", Components{}, "bbbbbbbb00000002")
	if err != nil {
		t.Fatalf("BuildFilesWithComponents at revision B: %v", err)
	}
	if _, ok := set["apps/web/b.yaml"]; !ok {
		t.Fatalf("revision B got a stale tree: set = %v, want apps/web/b.yaml", keys(set))
	}
}

func TestResolver_BuildFiles_NoArtifact(t *testing.T) {
	getter := &fakeGetter{yaml: "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nstatus: {}\n"}
	r := NewResolver(getter, nil, http.DefaultClient)
	if _, err := r.BuildFilesWithComponents(context.Background(), Ref{Kind: "GitRepository", Name: "cluster"}, "apps/web/dev/r1", Components{}, "abc"); err == nil {
		t.Fatal("expected ErrNoArtifact for a source with no ready artifact")
	}
}

// digestOf returns the Flux-style artifact digest of b.
func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// An artifact whose bytes don't match the digest the source advertises is
// rejected rather than parsed: the URL comes from cluster state.
func TestResolver_BuildFiles_RejectsDigestMismatch(t *testing.T) {
	tarball := makeTarGz(t, overlayFixture())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()
	getter := &fakeGetter{yaml: sourceYAML(srv.URL, "dev@sha1:deadbeefcafe0001", "sha256:"+strings.Repeat("0", 64))}
	r := NewResolver(getter, nil, srv.Client())
	_, err := r.BuildFilesWithComponents(context.Background(), Ref{Kind: "GitRepository", Name: "cluster"}, "apps/web/dev/r1", Components{}, "deadbeefcafe0001")
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("err = %v, want ErrDigestMismatch", err)
	}
}

// Shutdown stops the shared download itself, not only the callers waiting on
// it, so no fetch outlives the bridge's drain.
func TestResolver_BuildFiles_ShutdownStopsSharedDownload(t *testing.T) {
	arrived, aborted, testDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(arrived)
		select {
		case <-r.Context().Done(): // the client gave up on the request
			close(aborted)
		case <-testDone: // let srv.Close return if the download never stops
		}
	}))
	defer srv.Close()
	defer close(testDone)
	getter := &fakeGetter{yaml: sourceYAML(srv.URL, "dev@sha1:deadbeefcafe0001", "")}
	r := NewResolver(getter, nil, srv.Client())

	shutdown, stopProcess := context.WithCancel(context.Background())
	defer stopProcess()
	buildErr := make(chan error, 1)
	go func() {
		_, err := r.BuildFilesWithComponents(lifecycle.WithShutdown(context.Background(), shutdown),
			Ref{Kind: "GitRepository", Name: "cluster"}, "apps/web/dev/r1", Components{}, "deadbeefcafe0001")
		buildErr <- err
	}()
	select {
	case <-arrived:
	case err := <-buildErr:
		t.Fatalf("BuildFilesWithComponents returned before requesting the artifact: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the artifact request never arrived")
	}
	stopProcess()
	select {
	case <-aborted:
	case <-time.After(time.Second):
		t.Fatal("shutdown left the shared artifact download running")
	}
}

// A caller whose event was cancelled must not fail the others waiting on the
// same shared download, and must itself return promptly.
func TestResolver_BuildFiles_CancelledLeaderDoesNotFailFollowers(t *testing.T) {
	tarball := makeTarGz(t, overlayFixture())
	arrived, release := make(chan struct{}, 1), make(chan struct{})
	releaseHandler := sync.OnceFunc(func() { close(release) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		<-release
		_, _ = w.Write(tarball)
	}))
	defer srv.Close()
	// Runs before srv.Close: a failed assertion must not leave the handler
	// blocked, or srv.Close waits for it forever.
	defer releaseHandler()
	getter := &fakeGetter{yaml: sourceYAML(srv.URL, "dev@sha1:deadbeefcafe0001", digestOf(tarball))}
	r := NewResolver(getter, nil, srv.Client())
	ref := Ref{Kind: "GitRepository", Name: "cluster"}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := r.BuildFilesWithComponents(leaderCtx, ref, "apps/web/dev/r1", Components{}, "deadbeefcafe0001")
		leaderErr <- err
	}()
	select { // leader is now inside the shared download
	case <-arrived:
	case err := <-leaderErr:
		t.Fatalf("leader returned before requesting the artifact: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the artifact request never arrived")
	}
	followerErr := make(chan error, 1)
	go func() {
		_, err := r.BuildFilesWithComponents(context.Background(), ref, "apps/web/dev/r1", Components{}, "deadbeefcafe0001")
		followerErr <- err
	}()
	time.Sleep(20 * time.Millisecond)

	cancelLeader()
	select {
	case err := <-leaderErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("leader err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled leader kept waiting on the shared download")
	}
	releaseHandler()
	if err := <-followerErr; err != nil {
		t.Fatalf("follower failed because the leader was cancelled: %v", err)
	}
}
