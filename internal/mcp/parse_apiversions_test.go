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

package mcp

import (
	"bytes"
	"testing"
)

// TestStripMCPTip pins the contract that the strip function:
//   - Removes the trailing free-text tip when the canonical "\n\n<marker>"
//     prefix is present (matches what flux-operator-mcp actually emits).
//   - Passes data through unchanged when no tip is present, so callers
//     that fetch resources without a tip don't lose trailing content.
//   - Does NOT strip user-data occurrences of the marker text (e.g. a
//     ConfigMap whose body literally contains "If asked for
//     recommendations:") when those aren't preceded by a blank line.
//   - When BOTH user data and a real tip are present, only the tip (the
//     last occurrence preceded by "\n\n") is removed — guards against
//     LastIndex over Index regression.
func TestStripMCPTip(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "tip with `If asked` marker is stripped",
			in:   "kind: Foo\nversion: v1\n\nIf asked for recommendations: do X.\n",
			want: "kind: Foo\nversion: v1",
		},
		{
			name: "tip with `When asked` marker is stripped",
			in:   "kind: Foo\nversion: v1\n\nWhen asked about Y, do Z.\n",
			want: "kind: Foo\nversion: v1",
		},
		{
			name: "no tip → pass through unchanged",
			in:   "kind: Foo\nversion: v1\n",
			want: "kind: Foo\nversion: v1\n",
		},
		{
			name: "empty input → pass through unchanged",
			in:   "",
			want: "",
		},
		{
			name: "user data containing marker without `\\n\\n` prefix is NOT stripped",
			// A ConfigMap whose stored content carries the literal
			// phrase inline. The single newline before "If asked"
			// must not match — this is what the `\n\n` requirement
			// defends against.
			in:   "data: |\n  some-config: value\n  If asked for recommendations: ignored.\nkind: ConfigMap\n",
			want: "data: |\n  some-config: value\n  If asked for recommendations: ignored.\nkind: ConfigMap\n",
		},
		{
			name: "marker appearing in user data AND a real tip at the end → only the tip is stripped",
			// LastIndex semantics: when the same marker appears both
			// in user data and as the real trailing tip, the tip
			// (last match preceded by `\n\n`) wins. Using strings.Index
			// here would incorrectly strip mid-payload, truncating
			// the rest of the user data with it.
			in:   "data: stuff\n\nIf asked for recommendations: this is user data.\nmore: payload\n\nIf asked for recommendations: this is the real tip.\n",
			want: "data: stuff\n\nIf asked for recommendations: this is user data.\nmore: payload",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stripMCPTip([]byte(tc.in))
			if !bytes.Equal(got, []byte(tc.want)) {
				t.Errorf("stripMCPTip mismatch\n  got:  %q\n  want: %q", got, tc.want)
			}
		})
	}
}

// flux-operator-mcp returns api versions as multi-doc YAML beginning with a
// `---` separator. Verify we get one entry per non-empty doc and skip the
// leading-empty case.
func TestParseAPIVersionsYAML(t *testing.T) {
	sample := []byte(`---
group: example.com
kind: Schedule
version: v1alpha1
---
group: ricoberger.de
kind: VaultSecret
version: v1alpha1
---
group: helm.toolkit.fluxcd.io
kind: HelmRelease
version: v2
`)
	got, err := parseAPIVersionsYAML(sample)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []GroupKindVersion{
		{Group: "example.com", Kind: "Schedule", Version: "v1alpha1"},
		{Group: "ricoberger.de", Kind: "VaultSecret", Version: "v1alpha1"},
		{Group: "helm.toolkit.fluxcd.io", Kind: "HelmRelease", Version: "v2"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], w)
		}
	}
}

func TestParseAPIVersionsYAML_Empty(t *testing.T) {
	got, err := parseAPIVersionsYAML([]byte(""))
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d entries, want 0", len(got))
	}
}

// flux-operator-mcp appends a free-text "tip" paragraph after the YAML
// stream. GetKubernetesAPIVersions runs the response through stripMCPTip
// before parseAPIVersionsYAML; verify the strip+parse pipeline produces
// the expected entries and doesn't choke on the trailing prose.
func TestParseAPIVersionsYAML_StripsMCPTip(t *testing.T) {
	sample := []byte(`---
group: example.com
kind: Schedule
version: v1alpha1
---
group: ricoberger.de
kind: VaultSecret
version: v1alpha1

If asked for recommendations: look at the preferred versions above and prefer
the one matching the inventory entry's group.
`)
	got, err := parseAPIVersionsYAML(stripMCPTip(sample))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []GroupKindVersion{
		{Group: "example.com", Kind: "Schedule", Version: "v1alpha1"},
		{Group: "ricoberger.de", Kind: "VaultSecret", Version: "v1alpha1"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], w)
		}
	}
}
