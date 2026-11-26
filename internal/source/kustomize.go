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
	"fmt"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// kustomization captures the fields of a kustomization.yaml that reference other
// repo files or directories — every input whose edit changes this build. Fields
// the bridge doesn't need for attribution (namespace, images, replicas, …) are
// ignored.
type kustomization struct {
	Resources             []string    `yaml:"resources"`
	Bases                 []string    `yaml:"bases"` // deprecated, still encountered
	Components            []string    `yaml:"components"`
	CRDs                  []string    `yaml:"crds"`
	Configurations        []string    `yaml:"configurations"`
	PatchesStrategicMerge []string    `yaml:"patchesStrategicMerge"`
	Patches               []patchRef  `yaml:"patches"`
	PatchesJSON6902       []patchRef  `yaml:"patchesJson6902"`
	ConfigMapGenerator    []generator `yaml:"configMapGenerator"`
	SecretGenerator       []generator `yaml:"secretGenerator"`
	// Plugin configs: file or directory paths (inline YAML entries, which
	// contain a newline, are skipped).
	Transformers []string `yaml:"transformers"`
	Generators   []string `yaml:"generators"`
	Validators   []string `yaml:"validators"`
	// Replacement entries are either `path:` references or inline specs.
	Replacements []patchRef `yaml:"replacements"`
	OpenAPI      patchRef   `yaml:"openapi"`
}

type patchRef struct {
	Path string `yaml:"path"`
}

type generator struct {
	Files []string `yaml:"files"`
	Envs  []string `yaml:"envs"`
	Env   string   `yaml:"env"`
}

// dirOrFileRefs returns every relative path this kustomization references. Each
// is later resolved to either a nested kustomize directory (recurse) or a leaf
// file (build input).
func (k kustomization) dirOrFileRefs() []string {
	var refs []string
	refs = append(refs, k.Resources...)
	refs = append(refs, k.Bases...)
	refs = append(refs, k.Components...)
	refs = append(refs, k.CRDs...)
	refs = append(refs, k.Configurations...)
	for _, p := range k.PatchesStrategicMerge {
		if !isInlinePatch(p) {
			refs = append(refs, p)
		}
	}
	for _, p := range k.Patches {
		if p.Path != "" {
			refs = append(refs, p.Path)
		}
	}
	for _, p := range k.PatchesJSON6902 {
		if p.Path != "" {
			refs = append(refs, p.Path)
		}
	}
	for _, g := range append(append([]generator{}, k.ConfigMapGenerator...), k.SecretGenerator...) {
		refs = append(refs, generatorFiles(g)...)
	}
	for _, list := range [][]string{k.Transformers, k.Generators, k.Validators} {
		for _, ref := range list {
			if !strings.Contains(ref, "\n") {
				refs = append(refs, ref)
			}
		}
	}
	for _, r := range k.Replacements {
		if r.Path != "" {
			refs = append(refs, r.Path)
		}
	}
	if k.OpenAPI.Path != "" {
		refs = append(refs, k.OpenAPI.Path)
	}
	return refs
}

// isInlinePatch reports whether a patchesStrategicMerge entry is the patch
// itself rather than a file path. Kustomize accepts both and treats an entry
// that is a YAML object as an inline patch. A path never parses as one.
func isInlinePatch(entry string) bool {
	var obj map[string]any
	return yaml.Unmarshal([]byte(entry), &obj) == nil && len(obj) > 0
}

// generatorFiles extracts the source paths a generator reads. A `files` entry
// may be `key=path` or just `path`; `envs`/`env` are plain paths.
func generatorFiles(g generator) []string {
	var out []string
	for _, f := range g.Files {
		if i := strings.IndexByte(f, '='); i >= 0 {
			f = f[i+1:]
		}
		if f != "" {
			out = append(out, f)
		}
	}
	out = append(out, g.Envs...)
	if g.Env != "" {
		out = append(out, g.Env)
	}
	return out
}

// BuildFileSet resolves, from tree data alone (no I/O), the set of repo files
// that feed the kustomize build rooted at startDir — typically a Kustomization's
// spec.path. The returned set holds repo-root-relative paths; a PR file is part
// of this build iff its cleaned path is in the set.
//
// A startDir without a kustomization file is built the way Flux's
// kustomize-controller generates one for it: every .yaml/.yml file below it,
// except that a subdirectory with its own kustomization file is included as a
// kustomize directory (its build inputs, not every file in it).
//
// Remote references (URLs, git::/oci:: bases) are skipped: they live in another
// repo and can never be one of THIS repo's changed files. Any LOCAL reference
// that can't be resolved is returned as an error, so the caller can skip rather
// than attribute against an incomplete view (the exact-or-skip contract).
func (t *Tree) BuildFileSet(startDir string) (map[string]struct{}, error) {
	return t.BuildFileSetWithComponents(startDir, Components{})
}

// Components are a Flux Kustomization's spec.components: component
// directories, relative to spec.path, that kustomize-controller adds to the
// kustomization it builds there. With IgnoreMissing
// (spec.ignoreMissingComponents) a component the source doesn't contain is
// left out instead of failing the build.
type Components struct {
	Paths         []string
	IgnoreMissing bool
}

// BuildFileSetWithComponents is BuildFileSet for a Kustomization that also
// sets spec.components: the files those components read are build inputs too.
// A component Flux would reject (remote or absolute, outside the source,
// missing without IgnoreMissing, or without a kustomization file) is an error,
// as for any other unresolvable reference.
func (t *Tree) BuildFileSetWithComponents(startDir string, comps Components) (map[string]struct{}, error) {
	start := cleanRel(startDir)
	out := make(map[string]struct{})
	visited := make(map[string]struct{})

	var walk func(dir string) error
	walk = func(dir string) error {
		if _, seen := visited[dir]; seen {
			return nil
		}
		visited[dir] = struct{}{}

		entry, ok := t.kustomizations[dir]
		if !ok {
			return fmt.Errorf("referenced directory %q has no kustomization", dir)
		}
		// Editing the kustomization file itself changes the build.
		out[entry.path] = struct{}{}

		var k kustomization
		if err := yaml.Unmarshal(entry.body, &k); err != nil {
			return fmt.Errorf("parse kustomization in %q: %w", dir, err)
		}

		for _, ref := range k.dirOrFileRefs() {
			// Normalise to the same repo-root-relative form used by CleanPath /
			// HasPath membership checks (strips a leading "./" or "/"), so an
			// absolute or oddly-formed ref can't produce a key that later lookups
			// miss.
			target := cleanRel(path.Join(dir, ref))
			if target == ".." || strings.HasPrefix(target, "../") {
				continue // escapes the repo root — not an in-repo file
			}
			if _, isKustDir := t.kustomizations[target]; isKustDir {
				if err := walk(target); err != nil {
					return err
				}
				continue
			}
			if t.HasPath(target) {
				out[target] = struct{}{} // leaf input: resource / patch / generator / config file
				continue
			}
			// Kustomize loads a local file or directory before it tries a
			// remote base, so only a remote-looking ref that isn't in the
			// tree is remote.
			if isRemoteRef(ref) {
				continue
			}
			return fmt.Errorf("unresolved reference %q from %q", ref, dir)
		}
		return nil
	}

	if _, ok := t.kustomizations[start]; ok {
		if err := walk(start); err != nil {
			return nil, err
		}
	} else {
		files, kustDirs := t.generatedInputs(start)
		if len(files) == 0 && len(kustDirs) == 0 {
			return nil, fmt.Errorf("no kustomization or manifests at %q", start)
		}
		for _, f := range files {
			out[f] = struct{}{}
		}
		for _, d := range kustDirs {
			if err := walk(d); err != nil {
				return nil, err
			}
		}
	}

	for _, c := range comps.Paths {
		if path.IsAbs(c) || isRemoteRef(c) {
			return nil, fmt.Errorf("component %q must be local and relative", c)
		}
		target := cleanRel(path.Join(start, c))
		if target == ".." || strings.HasPrefix(target, "../") {
			return nil, fmt.Errorf("component %q is outside the source", c)
		}
		if comps.IgnoreMissing && !t.exists(target) {
			continue
		}
		if err := walk(target); err != nil {
			return nil, fmt.Errorf("component %q: %w", c, err)
		}
	}
	return out, nil
}

// exists reports whether p is a file or a directory in the tree.
func (t *Tree) exists(p string) bool {
	if _, ok := t.kustomizations[p]; ok || t.HasPath(p) {
		return true
	}
	prefix := p + "/"
	if p == "." {
		prefix = ""
	}
	for f := range t.paths {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	return false
}

// generatedInputs mirrors the kustomization Flux generates for a directory
// without one: the .yaml/.yml files below start, and — in place of the files
// under them — the topmost subdirectories that carry their own kustomization
// file (Flux adds those as resources and doesn't descend into them). Sorted
// for deterministic errors.
//
// No ignore rules apply here: Flux excludes files from generation only via
// the source's ignore rules (.sourceignore, GitRepository spec.ignore), which
// source-controller applies when it builds the artifact — so excluded files
// aren't in t. A Kustomization's spec.ignore is about drift detection (JSON
// Pointer paths inside objects), not files.
func (t *Tree) generatedInputs(start string) (files, kustDirs []string) {
	prefix := start + "/"
	if start == "." {
		prefix = ""
	}
	seen := make(map[string]struct{})
	for p := range t.paths {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		if d, ok := t.topKustomizationDir(start, path.Dir(p)); ok {
			if _, dup := seen[d]; !dup {
				seen[d] = struct{}{}
				kustDirs = append(kustDirs, d)
			}
			continue
		}
		if ext := path.Ext(p); ext == ".yaml" || ext == ".yml" {
			files = append(files, p)
		}
	}
	sort.Strings(files)
	sort.Strings(kustDirs)
	return files, kustDirs
}

// topKustomizationDir returns the topmost directory strictly below start, on
// the way down to dir, that has a kustomization file.
func (t *Tree) topKustomizationDir(start, dir string) (string, bool) {
	top, found := "", false
	for d := dir; d != start && d != "." && d != "/"; d = path.Dir(d) {
		if _, ok := t.kustomizations[d]; ok {
			top, found = d, true
		}
	}
	return top, found
}

// isRemoteRef reports whether a kustomization reference points outside this
// repo (a URL or a kustomize remote-base spec). BuildFileSet asks only after
// the ref didn't resolve inside the tree, the order kustomize uses; a remote
// ref is never one of this repo's changed files, so it's skipped.
func isRemoteRef(ref string) bool {
	return strings.Contains(ref, "://") ||
		strings.HasPrefix(ref, "git::") ||
		strings.HasPrefix(ref, "oci::") ||
		strings.HasPrefix(ref, "github.com/") ||
		strings.HasPrefix(ref, "gitlab.com/") ||
		strings.HasPrefix(ref, "bitbucket.org/")
}
