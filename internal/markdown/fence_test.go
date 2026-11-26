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
		"closer with nbsp":   {"~~~\ncode\n~~~\u00a0", "~~~\ncode\n~~~\u00a0\n~~~"},
		"closer with tab":    {"~~~\ncode\n~~~ \t", "~~~\ncode\n~~~ \t"},
		"closer with crlf":   {"~~~\r\ncode\r\n~~~\r", "~~~\r\ncode\r\n~~~\r"},
		"open in item cont.": {"- item\n  ~~~\n  code", "- item\n  ~~~\n  code\n  ~~~"},
		"trailing newline":   {"```\nx\n", "```\nx\n```"},
		"opener only":        {"text\n~~~", "text\n~~~\n~~~"},
		// A fence the list item already ended is closed: the text after it
		// is outside the block.
		"ended by item": {"- ~~~\n  code\n\nprose", "- ~~~\n  code\n\nprose"},
		// The closer goes to the content column of the item holding the
		// fence: a tab-indented nested item, a block quote.
		"nested item":    {"- a\n  - ```\n    code", "- a\n  - ```\n    code\n    ```"},
		"tab item":       {"-\t```\n\tcode", "-\t```\n\tcode\n    ```"},
		"in block quote": {"> ```\n> code", "> ```\n> code\n> ```"},
		"quoted item":    {"> - ~~~\n>   code", "> - ~~~\n>   code\n>   ~~~"},
		// An HTML comment hides the fence: nothing to close.
		"fence in html comment": {"<!--\n```\n", "<!--\n```\n"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := CloseOpenFence(tc.in); got != tc.want {
				t.Fatalf("CloseOpenFence(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestLineKinds covers which lines count as code or HTML, so that a
// line-based edit (such as mention neutralization) leaves code alone and
// still reaches the prose around it. A fence opened inside a list item ends
// with the item: a fence mistaken for an unclosed one turns every following
// line into code and leaves its @mentions live in a comment. want has one
// letter per line: p prose, c code, h HTML.
func TestLineKinds(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"delimiters and content": {"text\n````\n```\ncode\n````\nafter", "pccccp"},
		"bullet":                 {"- ~~~\n  quoted code\n  ~~~\nprose @someone", "cccp"},
		"star backticks":         {"* ```yaml\n  x: 1\n  ```\nprose", "cccp"},
		"ordered wide":           {"10. ~~~\n    code\n    ~~~\nprose", "cccp"},
		"list no fence":          {"- item\n  ~~~\n  code\n  ~~~\nprose", "pcccp"},
		"thematic break":         {"---\nprose", "pp"},
		// An unclosed fence ends with its list item: an outdented line leaves
		// the item, and is read again outside the fence.
		"item ends":       {"- ~~~\n  code\n\nOutside: ping @ops", "cccp"},
		"sibling item":    {"- ~~~\n  code\n- ping @ops", "ccp"},
		"sibling fence":   {"- ~~~\n  code\n- ```\n  more\n  ```\nprose", "cccccp"},
		"top level stays": {"~~~\ncode\n\nstill code", "cccc"},
		// A fence opened on a continuation line of a list item belongs to
		// the item too, and ends with it.
		"cont. item ends":  {"- item\n  ~~~\n  code\n\nOutside: ping @ops", "pcccp"},
		"cont. sibling":    {"- item\n  ~~~\n  code\n- ping @ops", "pccp"},
		"cont. nested":     {"- a\n  - b\n    ~~~\n    code\n  back in a @ops", "ppccp"},
		"cont. after gap":  {"- item\n\n  ~~~\n  code\nOutside @ops", "ppccp"},
		"item left first":  {"- item\n\nprose\n  ~~~\ncode\n\nstill code", "pppcccc"},
		"outdented opener": {"- item\n~~~\ncode\n\nstill code", "pcccc"},
		"nbsp line ends":   {"- ~~~\n  code\n\u00a0\nprose", "ccpp"},
		// A closer-like line after the item ended opens a new block.
		"closer after item": {"- a\n  - ```\n    code\n  ```\nprose", "pcccp"},

		// Indented code is code; an indented line that continues a paragraph
		// is not. HTML blocks are a kind of their own.
		"indented code": {"para\n\n    code @x\n\nprose", "ppcpp"},
		"lazy indent":   {"para\n    still para", "pp"},
		"html block":    {"<details>\n<summary>@x</summary>\n\nprose", "hhpp"},
		"tab in quote":  {"   >\t~~~\n   >\tcode\n   >\t~~~\nprose", "cccp"},

		// Regressions for CommonMark cases a line-based tracker got wrong.
		// A tab is a four-column tab stop: the tabbed line is content of
		// the list item's fence, and the indented closer closes it.
		"tab content in item": {"- ~~~\n\tcode\n  ~~~\nOutside: ping @ops", "cccp"},
		// A fence in a nested item, its marker beyond three spaces.
		"nested item fence": {"100. outer\n     - ~~~\n       code\n       ~~~\n     ping @ops", "pcccp"},
		"third level fence": {"- a\n  - b\n    - ~~~\n      code\n      ~~~\n    prose @ops", "ppcccp"},
		// Back in the outer item after a nested one: the fence belongs to
		// the outer item and ends with it.
		"fence back in outer item": {"- outer\n  - inner\n    text\n  ~~~\n  code\n\nprose @ops", "pppcccp"},
		// An HTML comment hides a fence inside it.
		"fence in html comment": {"<!--\n```\n-->\nPing @ops", "hhhp"},
		// `- - -` and `* * *` are thematic breaks, not list items, so the
		// fence after them is a top-level one, closed by the indented closer.
		"dash thematic break": {"- - -\n  ~~~\ncode\n  ~~~\nprose @ops", "pcccp"},
		"star thematic break": {"* * *\n  ~~~\ncode\n  ~~~\nprose @ops", "pcccp"},
	} {
		t.Run(name, func(t *testing.T) {
			var got strings.Builder
			for _, k := range LineKinds(tc.in) {
				got.WriteByte("pch"[k])
			}
			if got.String() != tc.want {
				t.Fatalf("LineKinds(%q) = %s, want %s", tc.in, got.String(), tc.want)
			}
		})
	}
}

// FuzzCloseOpenFence checks that CloseOpenFence leaves no fenced block open
// for a line appended after it, and that closing twice changes nothing.
func FuzzCloseOpenFence(f *testing.F) {
	for _, s := range []string{
		"- ~~~\n  code", "> - ```\n>   x", "-\t```\n\tx", "<!--\n```", "- a\n  - ```\n    code\n  ```",
		"1. > ~~~\n   > a", "~~~\n\n", ">* ```", "   >\t~~~", ">  ```\n",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		once := CloseOpenFence(s)
		if twice := CloseOpenFence(once); twice != once {
			t.Fatalf("CloseOpenFence not idempotent: %q -> %q -> %q", s, once, twice)
		}
		lines, blocks := codeBlocks(once + "\nx")
		for _, b := range blocks {
			if b.fence != "" && b.last == len(lines)-1 {
				t.Fatalf("CloseOpenFence(%q) = %q leaves the fence on line %d open", s, once, b.first)
			}
		}
	})
}
