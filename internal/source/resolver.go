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
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"gopkg.in/yaml.v3"

	"github.com/staffbase/flux-ai-bridge/internal/lifecycle"
)

// Ref identifies a Flux source object (a Kustomization's spec.sourceRef).
type Ref struct {
	Kind      string
	Name      string
	Namespace string
}

// ResourceGetter reads a Kubernetes resource's YAML — satisfied by the bridge's
// MCP pool. Kept as a narrow interface so the resolver is testable without MCP.
type ResourceGetter interface {
	GetKubernetesResource(ctx context.Context, apiVersion, kind, name, namespace string) ([]byte, error)
}

var (
	// ErrNoArtifact means the source object has no ready artifact to read.
	ErrNoArtifact = errors.New("source has no ready artifact")
	// ErrRevisionMismatch means the source's current artifact is at a different
	// revision than the event being processed — the exact bytes Flux built from
	// are no longer available (the source advanced and garbage-collected them).
	// The caller skips this event; a later, matching-revision reconcile retries.
	ErrRevisionMismatch = errors.New("source artifact revision does not match event revision")
)

const sourceGroup = "source.toolkit.fluxcd.io"

// Resolver fetches the source-controller artifact for a Kustomization's source
// and resolves which repo files feed that Kustomization's build. Trees are
// cached by artifact digest, so a reconcile storm (many Kustomizations, one
// source revision) downloads the tarball once.
type Resolver struct {
	getter        ResourceGetter
	apiVersionFor func(group, kind string) string // may be nil → defaults used
	httpClient    *http.Client

	// sf collapses concurrent fetches of the same artifact (keyed by digest/url)
	// so a reconcile storm downloads + parses the tarball once.
	sf singleflight.Group

	mu        sync.Mutex
	cache     map[string]*Tree
	order     []string
	maxCached int
}

// NewResolver builds a Resolver. apiVersionFor resolves a (group,kind) to its
// served apiVersion (pass the readiness resolver's APIVersion method); nil falls
// back to built-in defaults for the Flux source kinds.
func NewResolver(getter ResourceGetter, apiVersionFor func(group, kind string) string, httpClient *http.Client) *Resolver {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Resolver{
		getter:        getter,
		apiVersionFor: apiVersionFor,
		httpClient:    httpClient,
		cache:         make(map[string]*Tree),
		maxCached:     4,
	}
}

// BuildFilesWithComponents returns the set of repo files that feed the build
// at specPath with the Kustomization's spec.components (see
// Tree.BuildFileSetWithComponents), for the source named by ref, but only when
// the source's current artifact is at revisionSHA (the event's revision).
// Otherwise it returns ErrRevisionMismatch / ErrNoArtifact and the caller
// skips the event.
func (r *Resolver) BuildFilesWithComponents(ctx context.Context, ref Ref, specPath string, comps Components, revisionSHA string) (map[string]struct{}, error) {
	if ref.Kind == "" || ref.Name == "" {
		return nil, fmt.Errorf("kustomization has no usable sourceRef")
	}
	apiVersion := ""
	if r.apiVersionFor != nil {
		apiVersion = r.apiVersionFor(sourceGroup, ref.Kind)
	}
	if apiVersion == "" {
		apiVersion = defaultSourceAPIVersion(ref.Kind)
	}

	raw, err := r.getter.GetKubernetesResource(ctx, apiVersion, ref.Kind, ref.Name, ref.Namespace)
	if err != nil {
		return nil, fmt.Errorf("read source %s/%s: %w", ref.Kind, ref.Name, err)
	}
	var src sourceStatus
	if err := yaml.Unmarshal(raw, &src); err != nil {
		return nil, fmt.Errorf("parse source status: %w", err)
	}
	art := src.Status.Artifact
	if art.URL == "" || art.Revision == "" {
		return nil, ErrNoArtifact
	}
	if !revisionMatches(art.Revision, revisionSHA) {
		return nil, fmt.Errorf("%w: artifact=%q event=%q", ErrRevisionMismatch, art.Revision, revisionSHA)
	}

	tree, err := r.treeFor(ctx, art.Digest, art.URL, art.Revision)
	if err != nil {
		return nil, err
	}
	return tree.BuildFileSetWithComponents(specPath, comps)
}

// artifactFetchTimeout bounds one artifact download + parse.
const artifactFetchTimeout = 2 * time.Minute

// ErrDigestMismatch reports an artifact whose bytes don't hash to the digest
// the source's status advertises.
var ErrDigestMismatch = errors.New("artifact digest mismatch")

// treeFor returns the parsed artifact tree, cached by digest. Without a digest
// the URL alone doesn't name the content (a server may serve another revision
// at the same URL), so the key is the URL plus the revision the source status
// reports. Concurrent misses for the same key are collapsed via singleflight
// so the tarball is fetched + parsed once.
//
// The shared download runs on its own context (detached from the caller that
// happened to start it, bounded by artifactFetchTimeout): otherwise the first
// caller's cancellation failed every caller waiting on the same artifact.
// Each caller still stops waiting at its own deadline, and shutdown stops the
// download itself (see lifecycle) so it doesn't outlive the drain.
func (r *Resolver) treeFor(ctx context.Context, digest, url, revision string) (*Tree, error) {
	key := digest
	if key == "" {
		key = url + "@" + revision
	}

	if t := r.cached(key); t != nil {
		return t, nil
	}

	ch := r.sf.DoChan(key, func() (any, error) {
		// Re-check the cache: a racing leader may have populated it while we
		// waited to enter singleflight.
		if t := r.cached(key); t != nil {
			return t, nil
		}
		fetchCtx, cancel := lifecycle.Detach(ctx, artifactFetchTimeout)
		defer cancel()
		tree, err := r.fetchTree(fetchCtx, digest, url)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.store(key, tree)
		r.mu.Unlock()
		return tree, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*Tree), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fetchTree downloads and parses the artifact. When the source advertises a
// sha256 digest, the downloaded bytes must match it — the URL comes from the
// cluster's source status, so the content is verified rather than trusted.
func (r *Resolver) fetchTree(ctx context.Context, digest, url string) (*Tree, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build artifact request: %w", err)
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch artifact: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch artifact %s: status %d", url, resp.StatusCode)
	}

	want, verify := strings.CutPrefix(digest, "sha256:")
	hash := sha256.New()
	body := io.Reader(resp.Body)
	if verify {
		body = io.TeeReader(resp.Body, hash)
	}
	tree, err := ReadTarGz(body)
	if err != nil {
		return nil, fmt.Errorf("read artifact: %w", err)
	}
	if verify {
		// The tar reader stops at the end-of-archive marker; hash the rest.
		if _, err := io.Copy(hash, resp.Body); err != nil {
			return nil, fmt.Errorf("read artifact: %w", err)
		}
		if got := hex.EncodeToString(hash.Sum(nil)); got != want {
			return nil, fmt.Errorf("%w: %s: got sha256:%s, want %s", ErrDigestMismatch, url, got, digest)
		}
	}
	return tree, nil
}

// cached returns the cached tree for key, refreshing its LRU recency, or nil.
func (r *Resolver) cached(key string) *Tree {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.cache[key]; ok {
		r.touch(key)
		return t
	}
	return nil
}

// store inserts a tree, evicting the least-recently-used entry past maxCached.
// Caller holds mu.
func (r *Resolver) store(key string, tree *Tree) {
	if _, ok := r.cache[key]; ok {
		return
	}
	r.cache[key] = tree
	r.order = append(r.order, key)
	for len(r.order) > r.maxCached {
		oldest := r.order[0]
		r.order = r.order[1:]
		delete(r.cache, oldest)
	}
}

// touch moves key to the most-recently-used end of order so eviction in store
// is LRU rather than insertion-order FIFO. Caller holds mu.
func (r *Resolver) touch(key string) {
	for i, k := range r.order {
		if k == key {
			r.order = append(r.order[:i], r.order[i+1:]...)
			r.order = append(r.order, key)
			return
		}
	}
}

type sourceStatus struct {
	Status struct {
		Artifact struct {
			URL      string `yaml:"url"`
			Revision string `yaml:"revision"`
			Digest   string `yaml:"digest"`
		} `yaml:"artifact"`
	} `yaml:"status"`
}

// defaultSourceAPIVersion is the fallback served version per Flux source kind
// when discovery isn't available.
func defaultSourceAPIVersion(kind string) string {
	switch kind {
	case "OCIRepository":
		return sourceGroup + "/v1beta2"
	default: // GitRepository, Bucket
		return sourceGroup + "/v1"
	}
}

// revisionMatches reports whether a source artifact revision (e.g.
// "dev@sha1:abcdef…") refers to the same commit as the event's revision SHA.
// Short/long SHA forms are treated as equal when one is a prefix of the other.
func revisionMatches(artifactRevision, eventSHA string) bool {
	a, b := shaToken(artifactRevision), shaToken(eventSHA)
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	if len(a) >= 7 && len(b) >= 7 && (strings.HasPrefix(a, b) || strings.HasPrefix(b, a)) {
		return true
	}
	return false
}

// shaToken extracts the bare commit SHA from a Flux revision string, which may
// be "<branch>@sha1:<sha>", "<tag>@<sha>", or just "<sha>".
func shaToken(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return strings.ToLower(strings.TrimSpace(s[i+1:]))
	}
	if i := strings.LastIndex(s, "@"); i >= 0 {
		return strings.ToLower(strings.TrimSpace(s[i+1:]))
	}
	return strings.ToLower(s)
}
