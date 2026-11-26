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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"sort"
	"strings"
	"testing"
)

// makeTarGz builds an in-memory gzipped tar from path->content. Order is sorted
// for determinism.
func makeTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	for _, p := range paths {
		body := []byte(files[p])
		if err := tw.WriteHeader(&tar.Header{
			Name:     p,
			Mode:     0o644,
			Size:     int64(len(body)),
			Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatalf("write header %s: %v", p, err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatalf("write body %s: %v", p, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// overlayFixture models the layout behind the cross-overlay false positive: a
// shared base plus two sibling environment overlays that both include it.
func overlayFixture() map[string]string {
	return map[string]string{
		"apps/web/base/kustomization.yaml": "resources:\n  - deployment.yaml\n  - helmrelease.yaml\n",
		"apps/web/base/deployment.yaml":    "kind: Deployment\nmetadata:\n  name: web\n",
		"apps/web/base/helmrelease.yaml":   "kind: HelmRelease\nmetadata:\n  name: cache\n  namespace: web\n",

		"apps/web/dev/r1/kustomization.yaml": "resources:\n  - ../../base\npatches:\n  - path: patch.yaml\n",
		"apps/web/dev/r1/patch.yaml":         "kind: HelmRelease\nmetadata:\n  name: cache\n",

		"apps/web/prod/r1/kustomization.yaml": "resources:\n  - ../../base\npatches:\n  - path: values.yaml\n",
		"apps/web/prod/r1/values.yaml":        "kind: HelmRelease\nmetadata:\n  name: cache\n",

		"README.md": "docs",
	}
}

func TestReadTarGz_KeepsPathsDiscardsResourceBodies(t *testing.T) {
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, overlayFixture())))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	// Every file's path is recorded...
	for _, p := range []string{"apps/web/base/deployment.yaml", "apps/web/prod/r1/values.yaml", "README.md"} {
		if !tree.HasPath(p) {
			t.Errorf("path %q not recorded", p)
		}
	}
	// ...but only kustomization files have their bytes retained.
	if len(tree.kustomizations) != 3 {
		t.Errorf("kustomizations retained = %d, want 3 (base, dev/r1, prod/r1)", len(tree.kustomizations))
	}
	if _, ok := tree.kustomizations["apps/web/base"]; !ok {
		t.Errorf("base kustomization not retained")
	}
}

func TestBuildFileSet_DevOverlayExcludesSiblingProd(t *testing.T) {
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, overlayFixture())))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	set, err := tree.BuildFileSetWithComponents("./apps/web/dev/r1", Components{})
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents: %v", err)
	}

	wantIn := []string{
		"apps/web/dev/r1/kustomization.yaml", // the overlay itself
		"apps/web/dev/r1/patch.yaml",         // a patch input
		"apps/web/base/kustomization.yaml",   // included base
		"apps/web/base/deployment.yaml",      // base resource
		"apps/web/base/helmrelease.yaml",     // base resource (the shared-name one)
	}
	for _, p := range wantIn {
		if _, ok := set[p]; !ok {
			t.Errorf("build set missing %q", p)
		}
	}

	// The crux: the sibling prod overlay's files are NOT part of the dev build,
	// even though prod/r1/values.yaml patches the same-named HelmRelease.
	wantOut := []string{
		"apps/web/prod/r1/kustomization.yaml",
		"apps/web/prod/r1/values.yaml",
		"README.md",
	}
	for _, p := range wantOut {
		if _, ok := set[p]; ok {
			t.Errorf("build set unexpectedly contains sibling/unrelated file %q", p)
		}
	}
}

func TestBuildFileSet_MissingStartPathErrors(t *testing.T) {
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, overlayFixture())))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	if _, err := tree.BuildFileSetWithComponents("apps/web/stage/r1", Components{}); err == nil {
		t.Error("expected error for a path with no kustomization, got nil")
	}
}

func TestBuildFileSet_RemoteBaseIgnoredNotError(t *testing.T) {
	files := map[string]string{
		"apps/web/dev/kustomization.yaml":  "resources:\n  - ../base\n  - https://example.com/remote/base\n",
		"apps/web/base/kustomization.yaml": "resources:\n  - deployment.yaml\n",
		"apps/web/base/deployment.yaml":    "kind: Deployment\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	set, err := tree.BuildFileSetWithComponents("apps/web/dev", Components{})
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents should ignore remote bases, got: %v", err)
	}
	if _, ok := set["apps/web/base/deployment.yaml"]; !ok {
		t.Error("local base resource missing from build set")
	}
}

func TestBuildFileSet_UnresolvedLocalRefErrors(t *testing.T) {
	files := map[string]string{
		"apps/web/dev/kustomization.yaml": "resources:\n  - missing.yaml\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	if _, err := tree.BuildFileSetWithComponents("apps/web/dev", Components{}); err == nil {
		t.Error("expected error for an unresolved local reference, got nil")
	}
}

// A Kustomization pointing at plain manifests (no kustomization file) is built
// from the kustomization Flux generates: every .yaml/.yml below the path, with
// subdirectories that have their own kustomization file contributing only
// their build inputs.
func TestBuildFileSet_GeneratedKustomization(t *testing.T) {
	files := map[string]string{
		"apps/plain/deployment.yaml":          "kind: Deployment\n",
		"apps/plain/sub/service.yml":          "kind: Service\n",
		"apps/plain/README.md":                "docs",
		"apps/plain/kust/kustomization.yaml":  "resources:\n  - cm.yaml\n",
		"apps/plain/kust/cm.yaml":             "kind: ConfigMap\n",
		"apps/plain/kust/unused.yaml":         "kind: ConfigMap\n",
		"apps/plain/kust/deeper/ignored.yaml": "kind: Secret\n",
		"apps/other/deployment.yaml":          "kind: Deployment\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	set, err := tree.BuildFileSetWithComponents("./apps/plain", Components{})
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents: %v", err)
	}
	want := []string{
		"apps/plain/deployment.yaml",
		"apps/plain/kust/cm.yaml",
		"apps/plain/kust/kustomization.yaml",
		"apps/plain/sub/service.yml",
	}
	if len(set) != len(want) {
		t.Fatalf("set = %v, want %v", keys(set), want)
	}
	for _, p := range want {
		if _, ok := set[p]; !ok {
			t.Errorf("missing %s; set = %v", p, keys(set))
		}
	}
}

// Plugin configs, replacement sources and the OpenAPI schema are build inputs
// too; inline plugin YAML is not a path.
func TestBuildFileSet_PluginAndReplacementRefs(t *testing.T) {
	files := map[string]string{
		"apps/web/kustomization.yaml": "resources:\n  - deploy.yaml\n" +
			"transformers:\n  - labels.yaml\n  - |\n    apiVersion: builtin\n    kind: LabelTransformer\n" +
			"generators:\n  - gen/\n" +
			"validators:\n  - check.yaml\n" +
			"replacements:\n  - path: repl.yaml\n  - source:\n      kind: ConfigMap\n" +
			"openapi:\n  path: schema.json\n",
		"apps/web/deploy.yaml":            "kind: Deployment\n",
		"apps/web/labels.yaml":            "kind: LabelTransformer\n",
		"apps/web/gen/kustomization.yaml": "resources:\n  - g.yaml\n",
		"apps/web/gen/g.yaml":             "kind: Generator\n",
		"apps/web/check.yaml":             "kind: Validator\n",
		"apps/web/repl.yaml":              "source: {}\n",
		"apps/web/schema.json":            "{}",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	set, err := tree.BuildFileSetWithComponents("apps/web", Components{})
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents: %v", err)
	}
	for p := range files {
		if _, ok := set[p]; !ok {
			t.Errorf("missing %s; set = %v", p, keys(set))
		}
	}
}

// An inline patchesStrategicMerge entry is the patch itself, not a path: it
// must not make the overlay unresolvable. A missing patch file still is.
func TestBuildFileSet_InlineStrategicMergePatch(t *testing.T) {
	files := map[string]string{
		"apps/web/kustomization.yaml": "resources:\n  - deploy.yaml\n" +
			"patchesStrategicMerge:\n  - patch.yaml\n" +
			"  - |-\n    apiVersion: apps/v1\n    kind: Deployment\n    metadata:\n      name: web\n" +
			"  - '{\"apiVersion\": \"apps/v1\", \"kind\": \"Deployment\", \"metadata\": {\"name\": \"web\"}}'\n",
		"apps/web/deploy.yaml": "kind: Deployment\n",
		"apps/web/patch.yaml":  "kind: Deployment\n",
		"apps/bad/kustomization.yaml": "resources:\n  - deploy.yaml\n" +
			"patchesStrategicMerge:\n  - missing.yaml\n",
		"apps/bad/deploy.yaml": "kind: Deployment\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	set, err := tree.BuildFileSetWithComponents("apps/web", Components{})
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents with inline patches: %v", err)
	}
	for _, p := range []string{"apps/web/kustomization.yaml", "apps/web/deploy.yaml", "apps/web/patch.yaml"} {
		if _, ok := set[p]; !ok {
			t.Errorf("missing %s; set = %v", p, keys(set))
		}
	}
	if _, err := tree.BuildFileSetWithComponents("apps/bad", Components{}); err == nil {
		t.Error("a missing patchesStrategicMerge file must still fail the build set")
	}
}

// Kustomize loads a local path before it tries a remote base, so an in-repo
// directory or file whose path looks like a remote host is a build input.
func TestBuildFileSet_LocalPathLookingRemote(t *testing.T) {
	files := map[string]string{
		"apps/web/kustomization.yaml":                     "resources:\n  - github.com/org/base\n  - gitlab.com/cm.yaml\n  - github.com/org/remote?ref=v1\n",
		"apps/web/github.com/org/base/kustomization.yaml": "resources:\n  - deploy.yaml\n",
		"apps/web/github.com/org/base/deploy.yaml":        "kind: Deployment\n",
		"apps/web/gitlab.com/cm.yaml":                     "kind: ConfigMap\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	set, err := tree.BuildFileSetWithComponents("apps/web", Components{})
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents: %v", err)
	}
	for p := range files {
		if _, ok := set[p]; !ok {
			t.Errorf("missing %s; set = %v", p, keys(set))
		}
	}
}

// A file name's whitespace is part of it: a reference to "config.yaml " is
// that file, not its unspaced sibling, and CleanPath keeps the space so a PR
// file name matches the build set.
func TestBuildFileSet_PathWhitespaceIsLiteral(t *testing.T) {
	files := map[string]string{
		"apps/web/kustomization.yaml": "resources:\n  - \"config.yaml \"\n",
		"apps/web/config.yaml ":       "kind: ConfigMap\n",
		"apps/web/config.yaml":        "kind: ConfigMap\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	set, err := tree.BuildFileSetWithComponents("apps/web", Components{})
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents: %v", err)
	}
	if _, ok := set[CleanPath("apps/web/config.yaml ")]; !ok {
		t.Errorf("referenced file with trailing space missing; set = %q", keys(set))
	}
	if _, ok := set["apps/web/config.yaml"]; ok {
		t.Errorf("unreferenced sibling without the space attributed; set = %q", keys(set))
	}
}

// spec.components on the Flux Kustomization adds component directories to the
// build at spec.path: their files are build inputs. A missing component fails
// the build set unless ignoreMissingComponents is set.
func TestBuildFileSetWithComponents(t *testing.T) {
	files := map[string]string{
		"apps/web/dev/kustomization.yaml":     "resources:\n  - deploy.yaml\n",
		"apps/web/dev/deploy.yaml":            "kind: Deployment\n",
		"components/tls/kustomization.yaml":   "kind: Component\npatches:\n  - path: tls.yaml\n",
		"components/tls/tls.yaml":             "kind: Deployment\n",
		"components/other/kustomization.yaml": "kind: Component\n",
		"apps/plain/deploy.yaml":              "kind: Deployment\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}

	// One spec.path with its own kustomization, one Flux generates one for;
	// component paths are relative to spec.path.
	for start, component := range map[string]string{
		"apps/web/dev": "../../../components/tls",
		"apps/plain":   "../../components/tls",
	} {
		set, err := tree.BuildFileSetWithComponents(start, Components{Paths: []string{component}})
		if err != nil {
			t.Fatalf("%s: BuildFileSetWithComponents: %v", start, err)
		}
		for _, p := range []string{"components/tls/kustomization.yaml", "components/tls/tls.yaml"} {
			if _, ok := set[p]; !ok {
				t.Errorf("%s: component file %s missing; set = %v", start, p, keys(set))
			}
		}
		if _, ok := set["components/other/kustomization.yaml"]; ok {
			t.Errorf("%s: unlisted component attributed", start)
		}
	}

	missing := Components{Paths: []string{"../../../components/gone"}}
	if _, err := tree.BuildFileSetWithComponents("apps/web/dev", missing); err == nil {
		t.Error("a missing component must fail the build set without ignoreMissingComponents")
	}
	missing.IgnoreMissing = true
	if _, err := tree.BuildFileSetWithComponents("apps/web/dev", missing); err != nil {
		t.Errorf("ignoreMissingComponents must skip a missing component: %v", err)
	}
	for _, c := range []string{"https://example.com/c", "/components/tls"} {
		if _, err := tree.BuildFileSetWithComponents("apps/web/dev", Components{Paths: []string{c}}); err == nil {
			t.Errorf("component %q must be rejected like Flux does", c)
		}
	}
}

// A spec.path that exists but holds no manifests still builds in Flux: it
// generates an empty kustomization there and adds spec.components. The build
// set is then the components' files. A missing spec.path, or one with no
// build inputs and no components, stays an error.
func TestBuildFileSetWithComponents_ComponentsOnly(t *testing.T) {
	files := map[string]string{
		"apps/empty/README.md":              "docs",
		"components/tls/kustomization.yaml": "kind: Component\npatches:\n  - path: tls.yaml\n",
		"components/tls/tls.yaml":           "kind: Deployment\n",
	}
	tree, err := ReadTarGz(bytes.NewReader(makeTarGz(t, files)))
	if err != nil {
		t.Fatalf("ReadTarGz: %v", err)
	}
	comps := Components{Paths: []string{"../../components/tls"}}
	set, err := tree.BuildFileSetWithComponents("apps/empty", comps)
	if err != nil {
		t.Fatalf("BuildFileSetWithComponents: %v", err)
	}
	for _, p := range []string{"components/tls/kustomization.yaml", "components/tls/tls.yaml"} {
		if _, ok := set[p]; !ok {
			t.Errorf("component file %s missing; set = %v", p, keys(set))
		}
	}
	if _, err := tree.BuildFileSetWithComponents("apps/gone", comps); err == nil {
		t.Error("a missing spec.path must fail the build set, components or not")
	}
	if _, err := tree.BuildFileSetWithComponents("apps/empty", Components{}); err == nil {
		t.Error("a spec.path with no build inputs and no components must fail the build set")
	}
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Hitting the decompression cap mid-entry must report the cap, not the
// "tar: unexpected EOF" the truncated stream produces.
func TestReadTarGz_OversizedReportsCap(t *testing.T) {
	big := makeTarGz(t, map[string]string{"huge.yaml": strings.Repeat("a", maxArtifactBytes+1024)})
	if _, err := ReadTarGz(bytes.NewReader(big)); !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("err = %v, want ErrArtifactTooLarge", err)
	}
}
