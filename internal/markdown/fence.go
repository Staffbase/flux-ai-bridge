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

// Package markdown tracks fenced code blocks in Markdown text, for the places
// that edit LLM output and comment bodies line by line and must not change
// quoted code, or must close a block that was cut off.
package markdown

import "strings"

// Fence is the delimiter of a fenced code block: its character (` or ~) and
// how many of them open it.
type Fence struct {
	Char byte
	Len  int
}

// String returns the delimiter, e.g. "```" or "~~~~".
func (f Fence) String() string {
	return strings.Repeat(string(f.Char), f.Len)
}

// Tracker follows fenced code blocks line by line, per CommonMark: a block
// opens with at least three backticks or tildes (indented at most three
// spaces) and closes with a run of the same character at least as long and
// nothing after it. A backtick or tilde line that doesn't close the open
// block is part of its content. A fence may also open right after a list
// marker (`- ~~~`); its closer is then indented to the item's content, and
// the block also ends with the item: at the first non-blank line indented
// less than the item's content (an outdented paragraph or a sibling item).
type Tracker struct {
	open   Fence
	inside bool
	indent int // content indent of the list item that opened the block
}

// Line feeds the next line and reports whether it belongs to a fenced block,
// counting the opening and closing delimiter lines as part of it.
func (t *Tracker) Line(line string) bool {
	if t.inside && t.indent > 0 && strings.TrimSpace(line) != "" &&
		len(line)-len(strings.TrimLeft(line, " ")) < t.indent {
		t.inside = false // the list item, and with it the block, ended
	}
	if t.inside {
		f, rest, ok := delimiter(dedent(line, t.indent))
		if ok && f.Char == t.open.Char && f.Len >= t.open.Len && strings.TrimSpace(rest) == "" {
			t.inside = false
		}
		return true
	}
	w := listMarkerWidth(line)
	f, rest, ok := delimiter(line[w:])
	if ok && (f.Char != '`' || !strings.Contains(rest, "`")) {
		t.open, t.inside, t.indent = f, true, w
		return true
	}
	return false
}

// listMarkerWidth returns the width of a list item marker that starts line
// (up to three spaces, then -, * or +, or 1-9 digits and . or ), then one to
// four spaces), or 0 when the line doesn't start a list item.
func listMarkerWidth(line string) int {
	i := len(line) - len(strings.TrimLeft(line, " "))
	if i > 3 || i == len(line) {
		return 0
	}
	switch c := line[i]; {
	case c == '-' || c == '*' || c == '+':
		i++
	case c >= '0' && c <= '9':
		d := i
		for i < len(line) && i-d < 9 && line[i] >= '0' && line[i] <= '9' {
			i++
		}
		if i == len(line) || (line[i] != '.' && line[i] != ')') {
			return 0
		}
		i++
	default:
		return 0
	}
	spaces := len(line[i:]) - len(strings.TrimLeft(line[i:], " "))
	if spaces < 1 || spaces > 4 {
		return 0
	}
	return i + spaces
}

// dedent removes up to n leading spaces from line.
func dedent(line string, n int) string {
	for n > 0 && len(line) > 0 && line[0] == ' ' {
		line, n = line[1:], n-1
	}
	return line
}

// Open returns the delimiter of the block still open after the last line.
func (t *Tracker) Open() (Fence, bool) {
	return t.open, t.inside
}

// CloseOpenFence appends a closing delimiter matching the fenced block s
// leaves open, if any — e.g. for text cut off at a length limit, where the
// open block would otherwise swallow everything appended after it. A block
// inside a list item is closed at the item's content indent, so the closer
// stays inside the item.
func CloseOpenFence(s string) string {
	var t Tracker
	for _, line := range strings.Split(s, "\n") {
		t.Line(line)
	}
	if f, open := t.Open(); open {
		return s + "\n" + strings.Repeat(" ", t.indent) + f.String()
	}
	return s
}

// delimiter reports whether line starts with a fence run: up to three spaces,
// then at least three backticks or tildes. rest is what follows the run.
func delimiter(line string) (f Fence, rest string, ok bool) {
	indent := len(line) - len(strings.TrimLeft(line, " "))
	if indent > 3 || indent == len(line) {
		return Fence{}, "", false
	}
	c := line[indent]
	if c != '`' && c != '~' {
		return Fence{}, "", false
	}
	n := indent
	for n < len(line) && line[n] == c {
		n++
	}
	if n-indent < 3 {
		return Fence{}, "", false
	}
	return Fence{Char: c, Len: n - indent}, line[n:], true
}
