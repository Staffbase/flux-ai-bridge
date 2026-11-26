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
	bot, human := &gh.User{Type: gh.String("Bot")}, &gh.User{Type: gh.String("User")}
	for name, tc := range map[string]struct {
		comment *gh.IssueComment
		want    bool
	}{
		"bridge comment":               {&gh.IssueComment{User: bot, Body: gh.String(marker + "\n# ✅ Flux Status")}, true},
		"no author info":               {&gh.IssueComment{Body: gh.String(marker + "\nbody")}, true},
		"human quoting the marker":     {&gh.IssueComment{User: human, Body: gh.String(marker + "\nplease look")}, false},
		"marker further down the body": {&gh.IssueComment{User: bot, Body: gh.String("note\n" + marker + "\n")}, false},
		"other scope's marker":         {&gh.IssueComment{User: bot, Body: gh.String(commentMarkerFor("apps", "web-canary") + "\n")}, false},
	} {
		if got := isBridgeComment(tc.comment, marker); got != tc.want {
			t.Errorf("%s: isBridgeComment = %v, want %v", name, got, tc.want)
		}
	}
}
