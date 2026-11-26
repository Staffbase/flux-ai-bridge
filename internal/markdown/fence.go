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

// Package markdown finds the code in Markdown text, for the places that edit
// LLM output and comment bodies line by line and must not change quoted code,
// or must close a code block that a length cut left open. It reads the text
// with a CommonMark parser (goldmark), so list items, tab stops, block quotes,
// thematic breaks and HTML blocks are taken as GitHub renders them.
package markdown

import (
	"slices"
	"sort"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
)

// Kind is what a line of Markdown text belongs to.
type Kind uint8

const (
	// Prose is any line outside code and HTML blocks: paragraphs, headings,
	// list items, tables. Code spans inside it are left to the caller.
	Prose Kind = iota
	// Code is a line of a fenced code block, its delimiter lines included,
	// or of an indented code block. GitHub links no mentions there, and
	// readers copy from it.
	Code
	// HTML is a line of a raw HTML block, e.g. a comment or a <details>
	// block. A fence inside it opens no code block. GitHub renders the text
	// in it (and links mentions there), so it is neither prose nor code.
	HTML
)

// codeBlock is a code or HTML block. first and last are line indexes into
// strings.Split(s, "\n"), inclusive.
type codeBlock struct {
	first, last int
	html        bool
	fence       string // the opening delimiter run of a fenced block, else ""
	col         int    // byte offset of the fence in its opening line
	closed      bool   // a fenced block's last line is its closing delimiter
}

// LineKinds reports the Kind of each line of strings.Split(s, "\n").
func LineKinds(s string) []Kind {
	lines, blocks := codeBlocks(s)
	kinds := make([]Kind, len(lines))
	for _, b := range blocks {
		k := Code
		if b.html {
			k = HTML
		}
		for i := b.first; i <= b.last; i++ {
			kinds[i] = k
		}
	}
	return kinds
}

// CloseOpenFence appends a closing delimiter when s ends inside a fenced code
// block that has none — e.g. for text cut off at a length limit, where the
// open block would otherwise swallow everything appended after it. The closer
// repeats the opening run and is indented to the content column of the
// block's container (list item, block quote), so it stays inside it. A block
// its container already ended needs no closer.
func CloseOpenFence(s string) string {
	lines, blocks := codeBlocks(s)
	i := slices.IndexFunc(blocks, func(b codeBlock) bool {
		return b.fence != "" && !b.closed && blankFrom(lines, b.last+1)
	})
	if i < 0 {
		return s
	}
	open := blocks[i]
	sep := "\n" // a trailing newline already ends the last line
	if strings.HasSuffix(s, "\n") {
		sep = ""
	}
	prefix, keep := containerPrefix(lines[open.first][:open.col])
	// The opener may sit up to three spaces beyond its container's content
	// column, and a block quote's optional space can shift that column on
	// the next line. Try the closer from that column on, three columns to
	// either side of the opener's, and let the parser confirm that it closes
	// this block and that a line after it is outside of code. If none does,
	// the container already ended the block (a blank line ends a block quote).
	trailing := len(prefix) - len(strings.TrimRight(prefix, " "))
	for shift := -min(3, trailing, len(prefix)-keep); shift <= 3; shift++ {
		lead := prefix[:len(prefix)+min(shift, 0)] + strings.Repeat(" ", max(shift, 0))
		if closed := s + sep + lead + open.fence; closes(closed, open.first) {
			return closed
		}
	}
	return s
}

// closes reports whether the last line of s closes the fenced block that
// opens on line first, so that a line appended after it is outside of code.
func closes(s string, first int) bool {
	lines, blocks := codeBlocks(s + "\nx")
	at := len(lines) - 2
	for _, b := range blocks {
		if b.last > at || b.first == first && b.fence != "" && (!b.closed || b.last != at) {
			return false
		}
	}
	return true
}

// blankFrom reports whether lines[from:] hold only spaces and tabs.
func blankFrom(lines []string, from int) bool {
	for _, l := range lines[min(from, len(lines)):] {
		if strings.Trim(l, " \t\r") != "" {
			return false
		}
	}
	return true
}

// containerPrefix turns the part of an opening fence line before the fence
// into the prefix of a line in the same container: block quote markers stay,
// list markers become spaces, tabs are expanded to their four-column stops.
// keep is the length of the block quote markers (and the space after the
// last one) that must not be trimmed.
func containerPrefix(lead string) (prefix string, keep int) {
	var b strings.Builder
	for _, c := range lead {
		switch c {
		case '>':
			b.WriteByte('>')
		case '\t':
			b.WriteString(strings.Repeat(" ", 4-b.Len()%4))
		default:
			b.WriteByte(' ')
		}
	}
	prefix = b.String()
	if i := strings.LastIndexByte(prefix, '>'); i >= 0 {
		keep = i + 1
		if keep < len(prefix) {
			keep++ // the space after the marker
		}
	}
	return prefix, keep
}

// codeBlocks parses s and returns its lines and its code-like blocks.
func codeBlocks(s string) ([]string, []codeBlock) {
	lines := strings.Split(s, "\n")
	starts := make([]int, len(lines)) // byte offset of each line
	for i := 1; i < len(lines); i++ {
		starts[i] = starts[i-1] + len(lines[i-1]) + 1
	}
	lineOf := func(off int) int {
		return sort.Search(len(starts), func(i int) bool { return starts[i] > off }) - 1
	}
	// A segment's start can lie past a tab the parser expanded, its end
	// can't: the line of a segment is the line of its last byte.
	segLine := func(seg text.Segment) int {
		if seg.Stop == seg.Start {
			return lineOf(seg.Start)
		}
		return lineOf(seg.Stop - 1)
	}
	segLines := func(segs *text.Segments) (first, last int) {
		return segLine(segs.At(0)), segLine(segs.At(segs.Len() - 1))
	}

	var blocks []codeBlock
	doc := goldmark.DefaultParser().Parse(text.NewReader([]byte(s)))
	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		var b codeBlock
		switch n := n.(type) {
		case *ast.FencedCodeBlock:
			// The parser keeps the content lines, not the delimiter lines.
			// The opener is the line of the node's position (which a tab
			// can shift by up to three bytes, never past the fence's end).
			b.first = lineOf(n.Pos())
			b.col = strings.IndexAny(lines[b.first], "`~")
			opener := lines[b.first][b.col:]
			b.fence = opener[:len(opener)-len(strings.TrimLeft(opener, opener[:1]))]
			b.last = b.first
			if n.Lines().Len() > 0 {
				_, b.last = segLines(n.Lines())
			}
			// The closer is the line after the content, if it is one.
			if next := b.last + 1; next < len(lines) {
				rest, ok := strings.CutPrefix(strings.TrimLeft(lines[next], " \t>"), b.fence)
				if ok && strings.Trim(strings.TrimLeft(rest, b.fence[:1]), " \t\r") == "" {
					b.last, b.closed = next, true
				}
			}
		case *ast.CodeBlock:
			b.first, b.last = segLines(n.Lines())
		case *ast.HTMLBlock:
			b.html = true
			b.first, b.last = segLines(n.Lines())
			if n.HasClosure() {
				b.last = segLine(n.ClosureLine)
			}
		default:
			return ast.WalkContinue, nil
		}
		blocks = append(blocks, b)
		return ast.WalkSkipChildren, nil
	})
	// A delimiter-like line that starts a block of its own didn't close the
	// fence before it: the fence's container (a list item) ended first.
	for i := range blocks {
		b := &blocks[i]
		if b.closed && slices.ContainsFunc(blocks, func(o codeBlock) bool { return o.first == b.last }) {
			b.last, b.closed = b.last-1, false
		}
	}
	return lines, blocks
}
