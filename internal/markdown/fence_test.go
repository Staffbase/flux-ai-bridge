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

package markdown

import (
	"strings"
	"testing"
)

func TestCloseOpenFence(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"balanced":            {"a\n```\nx\n```\nb", "a\n```\nx\n```\nb"},
		"open backticks":      {"a\n```yaml\nx", "a\n```yaml\nx\n```"},
		"open four backticks": {"````\nx\n```\ny", "````\nx\n```\ny\n````"},
		"open tildes":         {"~~~\nx", "~~~\nx\n~~~"},
		"tilde inside ticks":  {"```\n~~~\nx", "```\n~~~\nx\n```"},
		"longer close":        {"```\nx\n`````\nafter", "```\nx\n`````\nafter"},
		"indented 3":          {"   ~~~~\nx", "   ~~~~\nx\n~~~~"},
		"indented 4 is code":  {"    ```\nx", "    ```\nx"},
		"inline backticks":    {"use ```x``` inline", "use ```x``` inline"},
		"backtick info":       {"``` a`b\nx", "``` a`b\nx"},
		"closer with text":    {"```\nx\n``` not a close", "```\nx\n``` not a close\n```"},
		"open in list item":   {"- ~~~\n  code", "- ~~~\n  code\n  ~~~"},
		"open in ordered":     {"10. ```\n    code", "10. ```\n    code\n    ```"},
		// CommonMark: only spaces and tabs may follow a closing fence, so a
		// no-break space leaves the block open.
		"closer with nbsp":   {"~~~\ncode\n~~~ ", "~~~\ncode\n~~~ \n~~~"},
		"closer with tab":    {"~~~\ncode\n~~~ \t", "~~~\ncode\n~~~ \t"},
		"closer with crlf":   {"~~~\r\ncode\r\n~~~\r", "~~~\r\ncode\r\n~~~\r"},
		"open in item cont.": {"- item\n  ~~~\n  code", "- item\n  ~~~\n  code\n  ~~~"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := CloseOpenFence(tc.in); got != tc.want {
				t.Fatalf("CloseOpenFence(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestTrackerListItemFences covers fences opened inside a list item: the
// marker must not hide the opener, or the indented closer is taken for a new
// opener and every following line counts as code (leaving its @mentions
// live in a comment).
func TestTrackerListItemFences(t *testing.T) {
	for name, tc := range map[string]struct {
		in   string
		want []bool
	}{
		"bullet":         {"- ~~~\n  quoted code\n  ~~~\nprose @someone", []bool{true, true, true, false}},
		"star backticks": {"* ```yaml\n  x: 1\n  ```\nprose", []bool{true, true, true, false}},
		"ordered wide":   {"10. ~~~\n    code\n    ~~~\nprose", []bool{true, true, true, false}},
		"list no fence":  {"- item\n  ~~~\n  code\n  ~~~\nprose", []bool{false, true, true, true, false}},
		"thematic break": {"---\nprose", []bool{false, false}},
		// An unclosed fence ends with its list item: an outdented line leaves
		// the item, and is read again outside the fence.
		"item ends":       {"- ~~~\n  code\n\nOutside: ping @ops", []bool{true, true, true, false}},
		"sibling item":    {"- ~~~\n  code\n- ping @ops", []bool{true, true, false}},
		"sibling fence":   {"- ~~~\n  code\n- ```\n  more\n  ```\nprose", []bool{true, true, true, true, true, false}},
		"top level stays": {"~~~\ncode\n\nstill code", []bool{true, true, true, true}},
		// A fence opened on a continuation line of a list item belongs to
		// the item too, and ends with it.
		"cont. item ends":  {"- item\n  ~~~\n  code\n\nOutside: ping @ops", []bool{false, true, true, true, false}},
		"cont. sibling":    {"- item\n  ~~~\n  code\n- ping @ops", []bool{false, true, true, false}},
		"cont. nested":     {"- a\n  - b\n    ~~~\n    code\n  back in a @ops", []bool{false, false, true, true, false}},
		"cont. after gap":  {"- item\n\n  ~~~\n  code\nOutside @ops", []bool{false, false, true, true, false}},
		"item left first":  {"- item\n\nprose\n  ~~~\ncode\n\nstill code", []bool{false, false, false, true, true, true, true}},
		"outdented opener": {"- item\n~~~\ncode\n\nstill code", []bool{false, true, true, true, true}},
		"nbsp line ends":   {"- ~~~\n  code\n \nprose", []bool{true, true, false, false}},
	} {
		t.Run(name, func(t *testing.T) {
			var tr Tracker
			lines := strings.Split(tc.in, "\n")
			for i, line := range lines {
				if got := tr.Line(line); got != tc.want[i] {
					t.Fatalf("line %d %q inside = %v, want %v", i, line, got, tc.want[i])
				}
			}
		})
	}
}

func TestTrackerMarksDelimitersAndContent(t *testing.T) {
	var tr Tracker
	var inside []bool
	for _, line := range strings.Split("text\n````\n```\ncode\n````\nafter", "\n") {
		inside = append(inside, tr.Line(line))
	}
	want := []bool{false, true, true, true, true, false}
	for i := range want {
		if inside[i] != want[i] {
			t.Fatalf("line %d inside = %v, want %v (all: %v)", i, inside[i], want[i], inside)
		}
	}
}
