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

// Package source reads a Flux source-controller artifact (the tarball the
// controller already fetched for a GitRepository / OCIRepository / Bucket) and
// answers, exactly, which repo files participate in a given Kustomization's
// build. It exists so PR attribution can be decided from the same bytes Flux
// reconciled — overlay-correct and layout-independent — instead of guessing
// from file paths or matching bare resource identity.
//
// Memory is deliberately bounded: the artifact is streamed, and only file
// PATHS (tar headers) and the contents of kustomization files are retained.
// Resource manifest bodies (HelmReleases, Deployments, Secrets, …) are skipped
// unread, so the bridge never holds rendered Kubernetes resources in RAM.
package source

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"path"
	"strings"
)

// ErrArtifactTooLarge reports an artifact over maxArtifactBytes decompressed.
var ErrArtifactTooLarge = fmt.Errorf("artifact exceeds %d bytes (decompressed)", maxArtifactBytes)

// Guard rails against a malicious or accidentally huge artifact. The cap is on
// DECOMPRESSED bytes (a gzip-bomb defence), applied across the whole stream.
const (
	maxArtifactBytes      = 64 << 20 // 64 MiB decompressed, total
	maxKustomizationBytes = 1 << 20  // 1 MiB per kustomization file
	maxEntries            = 200_000  // file-count ceiling
)

// kustomizationNames are the recognised kustomization config filenames. A file
// with one of these basenames is the only kind whose CONTENT we read.
var kustomizationNames = map[string]struct{}{
	"kustomization.yaml": {},
	"kustomization.yml":  {},
	"Kustomization":      {},
}

// Tree is the in-memory projection of an artifact: every regular file's path,
// plus the bytes of kustomization files only. It carries no resource bodies.
type Tree struct {
	// paths holds every regular-file path in the artifact (cleaned,
	// slash-separated, repo-root-relative). Used for membership tests and to
	// tell a referenced file from a referenced directory.
	paths map[string]struct{}
	// kustomizations maps a directory path to that directory's kustomization
	// file. The key is the containing directory (cleaned); "." is the repo
	// root. The entry keeps the file's real path so editing the kustomization
	// itself counts as touching the build.
	kustomizations map[string]kustEntry
}

// kustEntry is one directory's kustomization file: its repo-relative path and
// its bytes.
type kustEntry struct {
	path string
	body []byte
}

// HasPath reports whether p (cleaned) is a regular file in the artifact.
func (t *Tree) HasPath(p string) bool {
	_, ok := t.paths[cleanRel(p)]
	return ok
}

// ReadTarGz streams a gzipped tar artifact, retaining only what exact
// attribution needs: the set of file paths and the contents of kustomization
// files. All other entry bodies are skipped without being read into memory.
func ReadTarGz(r io.Reader) (*Tree, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	// Cap decompressed bytes across the whole stream (covers skipped entries
	// too, since tar must decompress to advance past them).
	limited := &io.LimitedReader{R: gz, N: maxArtifactBytes + 1}
	tr := tar.NewReader(limited)
	// The cap cuts the stream mid-entry, which tar reports as an unexpected
	// EOF; name the real cause instead.
	capped := func(err error) error {
		if limited.N <= 0 {
			return ErrArtifactTooLarge
		}
		return err
	}

	t := &Tree{
		paths:          make(map[string]struct{}),
		kustomizations: make(map[string]kustEntry),
	}

	entries := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, capped(fmt.Errorf("tar: %w", err))
		}
		if limited.N <= 0 {
			return nil, ErrArtifactTooLarge
		}
		if entries++; entries > maxEntries {
			return nil, fmt.Errorf("artifact exceeds %d entries", maxEntries)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue // dirs, symlinks, etc. — not build inputs we track
		}
		rel, ok := cleanArtifactPath(hdr.Name)
		if !ok {
			continue // absolute or path-escaping entry: ignore defensively
		}
		t.paths[rel] = struct{}{}

		if _, isKust := kustomizationNames[path.Base(rel)]; !isKust {
			continue // resource body: record the path, skip the content unread
		}
		// Only kustomization files have their bytes read (size-capped).
		body, err := io.ReadAll(io.LimitReader(tr, maxKustomizationBytes+1))
		if err != nil {
			return nil, capped(fmt.Errorf("read %s: %w", rel, err))
		}
		if len(body) > maxKustomizationBytes {
			return nil, fmt.Errorf("%s exceeds %d bytes", rel, maxKustomizationBytes)
		}
		t.kustomizations[path.Dir(rel)] = kustEntry{path: rel, body: body}
	}
	return t, nil
}

// cleanArtifactPath normalises a tar entry name to a repo-root-relative,
// slash-separated path. Returns ok=false for absolute paths or paths that
// escape the root (defence against a crafted archive).
func cleanArtifactPath(name string) (string, bool) {
	name = strings.TrimPrefix(name, "./")
	if name == "" || strings.HasPrefix(name, "/") {
		return "", false
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", false
	}
	return clean, true
}

// cleanRel normalises a path the same way entries are stored, so external
// callers (PR file paths, spec.path) compare consistently. Whitespace is part
// of the name: archive entries, GitHub file names and kustomize references
// all keep it, so trimming it could match a different file.
func cleanRel(p string) string {
	p = strings.TrimPrefix(p, "./")
	p = path.Clean(p)
	return strings.TrimPrefix(p, "/")
}

// CleanPath normalises a repo-relative path to the same form BuildFileSet keys
// use, so callers can test membership of a PR file path in a build set.
func CleanPath(p string) string {
	return cleanRel(p)
}
