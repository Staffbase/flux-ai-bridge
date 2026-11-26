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
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/staffbase/flux-ai-bridge/internal/readiness"
)

// The summary cut must not split a multi-byte character: invalid UTF-8 ends
// up in the comment and in its stability key.
func TestSummaryFailureText_CutsOnCharacterBoundary(t *testing.T) {
	msg := strings.Repeat("a", 299) + "ü" + strings.Repeat("b", 50)
	got := summaryFailureText(msg)
	if !utf8.ValidString(got) {
		t.Fatalf("summary is not valid UTF-8: %q", got)
	}
	if want := strings.Repeat("a", 299) + "ü…"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	if got := summaryFailureText("short\n  message"); got != "short message" {
		t.Fatalf("short message = %q, want it whitespace-collapsed and uncut", got)
	}
}

// Controller messages are untrusted (they can quote user input, and a forged
// event carries arbitrary text): no markup, no hidden-comment trick, no
// mentions, and no table breakage.
func TestMarkdownSafeEscaping(t *testing.T) {
	msg := "apply failed <!-- hide --> <img src=x> ping @alice and @org/team | a&b"
	for name, got := range map[string]string{
		"summary": summaryFailureText(msg),
		"cell":    markdownTableCell(msg),
	} {
		for _, bad := range []string{"<!--", "<img", "@alice", "@org/team"} {
			if strings.Contains(got, bad) {
				t.Errorf("%s still contains %q: %q", name, bad, got)
			}
		}
		for _, want := range []string{"&lt;!-- hide --&gt;", "@\u200balice", "a&amp;b"} {
			if !strings.Contains(got, want) {
				t.Errorf("%s missing %q: %q", name, want, got)
			}
		}
	}
	if cell := markdownTableCell("a | b\nc"); cell != `a \| b c` {
		t.Errorf("cell = %q, want pipes escaped and newlines collapsed", cell)
	}
}

// The LLM reply quotes untrusted text, so mentions in its prose must not
// notify anyone. Code stays byte-identical (GitHub doesn't link mentions
// there, and readers copy from it), and so do emails and image digests.
func TestNeutralizeMentions(t *testing.T) {
	const zw = "\u200b"
	for in, want := range map[string]string{
		"ping @alice and @org/team":                "ping @" + zw + "alice and @" + zw + "org/team",
		"@alice at line start, (@bob) in parens":   "@" + zw + "alice at line start, (@" + zw + "bob) in parens",
		"mail ops@example.com, image app@sha256:1": "mail ops@example.com, image app@sha256:1",
		"set `schedule: \"@daily\"` and @carol":    "set `schedule: \"@daily\"` and @" + zw + "carol",
		"``a ` @x`` then @dave":                    "``a ` @x`` then @" + zw + "dave",
		"unmatched ` backtick @erin":               "unmatched ` backtick @" + zw + "erin",
		"```\n@frank in a fence\n```\n@grace":      "```\n@frank in a fence\n```\n@" + zw + "grace",
		"- ~~~\n  @heidi\n  ~~~\nthen @ivan":       "- ~~~\n  @heidi\n  ~~~\nthen @" + zw + "ivan",
		"no mentions here":                         "no mentions here",
	} {
		if got := neutralizeMentions(in); got != want {
			t.Errorf("neutralizeMentions(%q) = %q, want %q", in, got, want)
		}
	}
}

// Fence boundaries come from the shared tracker: a mention inside a
// four-backtick or tilde block — including a ``` line quoted inside it — stays
// untouched, and prose after the block is neutralized again.
func TestNeutralizeMentions_FenceKinds(t *testing.T) {
	in := "````\n```\n@daily\n````\nping @ops\n~~~\n@hourly\n~~~"
	want := "````\n```\n@daily\n````\nping @\u200bops\n~~~\n@hourly\n~~~"
	if got := neutralizeMentions(in); got != want {
		t.Fatalf("neutralizeMentions(%q) = %q, want %q", in, got, want)
	}
}

// Manifest-derived IDs must use the inventory's encoding: cli-utils writes `:`
// in RBAC names as `__`.
func TestResourceIDsFromYAML_RBACColonTranscoding(t *testing.T) {
	got := ResourceIDsFromYAML([]byte("apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: system:metrics-reader\n"))
	if want := "_system__metrics-reader_rbac.authorization.k8s.io_ClusterRole"; len(got) != 1 || got[0] != want {
		t.Fatalf("ids = %v, want [%s]", got, want)
	}
}

// A manifest that leaves its namespace to the build resolves to the namespaced
// inventory entry (or, once pruned, to the prune subject), so attribution,
// prune matching and teardown checks see the real object.
func TestCanonicalID(t *testing.T) {
	inventory := map[string]struct{}{
		"apps_web_apps_Deployment":                                 {},
		"_cluster-admin-ish_rbac.authorization.k8s.io_ClusterRole": {},
		"a_dup_apps_Deployment":                                    {},
		"b_dup_apps_Deployment":                                    {},
	}
	prunes := pruneSetProvider(func() map[string]struct{} {
		return map[string]struct{}{"Deployment/apps/gone": {}}
	})
	for id, want := range map[string]string{
		"_web_apps_Deployment":                                     "apps_web_apps_Deployment",                                 // namespace from the build
		"apps_web_apps_Deployment":                                 "apps_web_apps_Deployment",                                 // already exact
		"_cluster-admin-ish_rbac.authorization.k8s.io_ClusterRole": "_cluster-admin-ish_rbac.authorization.k8s.io_ClusterRole", // cluster-scoped
		"_dup_apps_Deployment":                                     "_dup_apps_Deployment",                                     // ambiguous: left alone
		"_gone_apps_Deployment":                                    "apps_gone_apps_Deployment",                                // pruned: from the prune subject
		"_unknown_apps_Deployment":                                 "_unknown_apps_Deployment",
	} {
		if got := canonicalID(id, inventory, prunes); got != want {
			t.Errorf("canonicalID(%q) = %q, want %q", id, got, want)
		}
	}
	if got := canonicalID("_gone_apps_Deployment", inventory, nil); got != "_gone_apps_Deployment" {
		t.Errorf("without prunes, unknown IDs stay unchanged; got %q", got)
	}
}

// A pending HelmRelease whose row was pruned (the PR deleted it) is no longer
// pending; one whose row is still there stays pending.
func TestWithoutPrunedHelmReleases(t *testing.T) {
	before := map[string]struct{}{"apps/deleted": {}, "apps/installing": {}}
	after := []readiness.ResourceStatus{{Kind: "HelmRelease", Namespace: "apps", Name: "installing"}}
	pending := []pendingHelmRelease{{Name: "apps/deleted"}, {Name: "apps/installing"}, {Name: "apps/not-a-row"}}
	got := withoutPrunedHelmReleases(pending, before, after)
	if len(got) != 2 || got[0].Name != "apps/installing" || got[1].Name != "apps/not-a-row" {
		t.Fatalf("pending = %+v, want installing and not-a-row kept", got)
	}
}
