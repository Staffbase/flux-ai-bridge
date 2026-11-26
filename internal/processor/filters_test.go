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

package processor

import (
	"testing"
)

// TestResourceIDsFromYAML pins the inventory-ID format the bridge derives
// from PR file content. Format must match kustomize-controller's
// `<namespace>_<name>_<group>_<kind>` (empty group for core types).
func TestResourceIDsFromYAML(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data string
		want []string
	}{
		{
			name: "namespaced HelmRelease",
			data: `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: blackbox
  namespace: blackbox-exporter
`,
			want: []string{"blackbox-exporter_blackbox_helm.toolkit.fluxcd.io_HelmRelease"},
		},
		{
			name: "cluster-scoped Namespace (empty namespace + empty group)",
			data: `apiVersion: v1
kind: Namespace
metadata:
  name: blackbox-exporter
`,
			want: []string{"_blackbox-exporter__Namespace"},
		},
		{
			name: "Flux Kustomization (group != core, namespace set)",
			data: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apperator-crds
  namespace: flux-system
`,
			want: []string{"flux-system_apperator-crds_kustomize.toolkit.fluxcd.io_Kustomization"},
		},
		{
			name: "multi-document YAML",
			data: `apiVersion: v1
kind: Namespace
metadata:
  name: blackbox-exporter
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: blackbox
  namespace: blackbox-exporter
`,
			want: []string{
				"_blackbox-exporter__Namespace",
				"blackbox-exporter_blackbox_helm.toolkit.fluxcd.io_HelmRelease",
			},
		},
		{
			name: "missing required fields (no kind) → skipped",
			data: `apiVersion: v1
metadata:
  name: x
`,
			want: nil,
		},
		{
			name: "non-YAML content is marked unparseable without panic",
			data: "this isn't yaml :)",
			want: []string{unparseableResourceID},
		},
		{
			name: "empty input",
			data: "",
			want: nil,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := ResourceIDsFromYAML([]byte(c.data))
			if len(got) != len(c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, got[i], c.want[i])
				}
			}
		})
	}
}

func TestSkippableFile(t *testing.T) {
	t.Parallel()
	skip := []string{
		"README.md",
		"docs/setup.md",
		"docs/setup.MARKDOWN",
		"docs/screenshot.png",
		".gitignore",
		"go.sum",
		"package-lock.json",
		"CODEOWNERS",
	}
	keep := []string{
		"apps/web/deployment.yaml",
		"clusters/prod/namespace.yaml",
		"kustomization.yaml",
		"release.yml",
		"some/nested/dir/configmap.json",
	}
	for _, p := range skip {
		if !skippableFile(p) {
			t.Errorf("%q should be skippable", p)
		}
	}
	for _, p := range keep {
		if skippableFile(p) {
			t.Errorf("%q should NOT be skippable", p)
		}
	}
}

func TestPRHasRequiredLabel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		required []string
		labels   []string
		want     bool
	}{
		{"empty required = pass-through (filter disabled)", nil, []string{"any"}, true},
		{"required all-empty after trim = pass-through", []string{"", "  "}, []string{"any"}, true},
		{"PR has matching label", []string{"flux-review"}, []string{"flux-review", "bug"}, true},
		{"PR has none of the required labels", []string{"flux-review"}, []string{"bug"}, false},
		{"OR semantics: any one match passes", []string{"a", "b", "c"}, []string{"c"}, true},
		{"case-sensitive: differing case does not match", []string{"Flux-Review"}, []string{"flux-review"}, false},
		{"no PR labels + required configured = fail", []string{"flux-review"}, nil, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := prHasRequiredLabel(c.required, c.labels); got != c.want {
				t.Errorf("required=%v labels=%v got=%v want=%v", c.required, c.labels, got, c.want)
			}
		})
	}
}
