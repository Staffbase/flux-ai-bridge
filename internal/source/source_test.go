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
	set, err := tree.BuildFileSet("./apps/web/dev/r1")
	if err != nil {
		t.Fatalf("BuildFileSet: %v", err)
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
	if _, err := tree.BuildFileSet("apps/web/stage/r1"); err == nil {
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
	set, err := tree.BuildFileSet("apps/web/dev")
	if err != nil {
		t.Fatalf("BuildFileSet should ignore remote bases, got: %v", err)
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
	if _, err := tree.BuildFileSet("apps/web/dev"); err == nil {
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
	set, err := tree.BuildFileSet("./apps/plain")
	if err != nil {
		t.Fatalf("BuildFileSet: %v", err)
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
	set, err := tree.BuildFileSet("apps/web")
	if err != nil {
		t.Fatalf("BuildFileSet: %v", err)
	}
	for p := range files {
		if _, ok := set[p]; !ok {
			t.Errorf("missing %s; set = %v", p, keys(set))
		}
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
