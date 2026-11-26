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

package skills

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewLoader_MissingDirIsNoOp(t *testing.T) {
	l, err := NewLoader(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := l.Prompt(); got != "" {
		t.Errorf("expected empty prompt from no-op loader, got %q", got)
	}
	if l.Loaded() {
		t.Errorf("expected Loaded()=false when dir is missing")
	}
}

func TestNewLoader_MissingFileIsNoOp(t *testing.T) {
	// Dir exists, but no SKILL.md inside. Mirrors the chart configuration where
	// both skill.default.enabled=false and skill.custom is empty — the ConfigMap
	// is still rendered with no SKILL.md key, so /skills is empty.
	dir := t.TempDir()
	l, err := NewLoader(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := l.Prompt(); got != "" {
		t.Errorf("expected empty prompt, got %q", got)
	}
	if l.Loaded() {
		t.Errorf("expected Loaded()=false when SKILL.md is absent")
	}
}

func TestLoader_ReadsSkillFile(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "SKILL.md"), "SKILL BODY")

	l, err := NewLoader(dir)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	if got := l.Prompt(); got != "SKILL BODY" {
		t.Errorf("Prompt() = %q, want %q", got, "SKILL BODY")
	}
	if !l.Loaded() {
		t.Errorf("expected Loaded()=true after reading SKILL.md")
	}
}

func TestLoader_IgnoresOtherFiles(t *testing.T) {
	// Only SKILL.md is read. Anything else dropped into /skills (e.g. an
	// operator experimenting, leftover files in a custom mount) is ignored.
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "SKILL.md"), "REAL SKILL")
	mustWrite(t, filepath.Join(dir, "extra.md"), "MUST BE IGNORED")
	mustWrite(t, filepath.Join(dir, "notes.txt"), "MUST BE IGNORED")

	l, err := NewLoader(dir)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	if got := l.Prompt(); got != "REAL SKILL" {
		t.Errorf("Prompt() = %q, want exactly the SKILL.md content", got)
	}
}

func TestLoader_PromptIsStableAcrossCalls(t *testing.T) {
	// Anthropic prompt cache wants byte-identical content. Two calls must
	// return the exact same string.
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "SKILL.md"), "STABLE CONTENT")
	l, err := NewLoader(dir)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	first := l.Prompt()
	second := l.Prompt()
	if first != second {
		t.Fatalf("Prompt() returned different content across calls: %q vs %q", first, second)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
