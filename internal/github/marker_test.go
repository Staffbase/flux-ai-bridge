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

package github

import (
	"testing"

	gh "github.com/google/go-github/v62/github"
)

func TestIsBridgeComment(t *testing.T) {
	marker := commentMarkerFor("apps", "web")
	const self = "flux-ai-bridge[bot]"
	bridge := &gh.User{Type: gh.String("Bot"), Login: gh.String(self)}
	otherBot := &gh.User{Type: gh.String("Bot"), Login: gh.String("other-app[bot]")}
	human := &gh.User{Type: gh.String("User"), Login: gh.String("someone")}
	for name, tc := range map[string]struct {
		comment  *gh.IssueComment
		botLogin string // "" = the App's identity is unknown (lookup failed)
		want     bool
	}{
		"bridge comment":                         {&gh.IssueComment{User: bridge, Body: gh.String(marker + "\n# ✅ Flux Status")}, self, true},
		"bridge login in other case":             {&gh.IssueComment{User: &gh.User{Type: gh.String("Bot"), Login: gh.String("Flux-AI-Bridge[bot]")}, Body: gh.String(marker + "\n")}, self, true},
		"other bot with a copied marker":         {&gh.IssueComment{User: otherBot, Body: gh.String(marker + "\n# ✅ Flux Status")}, self, false},
		"no author info":                         {&gh.IssueComment{Body: gh.String(marker + "\nbody")}, self, false},
		"human quoting the marker":               {&gh.IssueComment{User: human, Body: gh.String(marker + "\nplease look")}, self, false},
		"marker further down the body":           {&gh.IssueComment{User: bridge, Body: gh.String("note\n" + marker + "\n")}, self, false},
		"other scope's marker":                   {&gh.IssueComment{User: bridge, Body: gh.String(commentMarkerFor("apps", "web-canary") + "\n")}, self, false},
		"identity unknown: any bot's marker":     {&gh.IssueComment{User: otherBot, Body: gh.String(marker + "\n")}, "", true},
		"identity unknown: no author info":       {&gh.IssueComment{Body: gh.String(marker + "\nbody")}, "", true},
		"identity unknown: human quoting marker": {&gh.IssueComment{User: human, Body: gh.String(marker + "\n")}, "", false},
	} {
		if got := isBridgeComment(tc.comment, marker, tc.botLogin); got != tc.want {
			t.Errorf("%s: isBridgeComment = %v, want %v", name, got, tc.want)
		}
	}
}
