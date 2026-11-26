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

// Package skills loads the SKILL.md system-prompt prefix mounted by the
// Helm chart and exposes it as a single string.
//
// The chart renders one ConfigMap key (`SKILL.md`) and mounts it at
// /skills/SKILL.md. The loader reads that file at startup. Missing file
// or missing directory both yield a no-op loader (empty Prompt) so unit
// tests and minimal configs stay runnable.
//
// Operators wanting to override or extend the prompt edit the chart's
// `skill.default.enabled` / `skill.custom` values — see docs/skill.md.
// The chart's checksum/skill annotation forces a pod restart when the
// ConfigMap content changes.
package skills

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// skillFilename is the single file the loader reads from the mounted dir.
// Matches the ConfigMap key rendered by charts/flux-ai-bridge/templates/configmap-skill.yaml.
const skillFilename = "SKILL.md"

// Loader is the read-only result of loading the SKILL.md at startup.
type Loader struct {
	prompt string // the file content; empty when no file was loaded
	loaded bool   // true when SKILL.md was found and read
}

// NewLoader loads dir/SKILL.md. Missing dir or missing file return a no-op
// loader (Prompt returns "") so the bridge stays runnable in unit tests and
// minimal configs.
func NewLoader(dir string) (*Loader, error) {
	l := &Loader{}

	info, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stat skills dir %q: %w", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("skills path %q is not a directory", dir)
	}

	body, err := os.ReadFile(filepath.Join(dir, skillFilename))
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s/%s: %w", dir, skillFilename, err)
	}
	l.prompt = string(body)
	l.loaded = true
	return l, nil
}

// Prompt returns the SKILL.md content, suitable for inlining into the LLM
// system prompt. Empty when no file was loaded — caller should send the
// prompt without skills in that case.
//
// Byte-identical across calls for the same pod lifetime: required for
// Anthropic's prompt-cache hit rate.
func (l *Loader) Prompt() string {
	if l == nil {
		return ""
	}
	return l.prompt
}

// Loaded reports whether SKILL.md was found and read. Used for startup
// logging — distinguishes "no skill prefix configured" from "tried to load
// and got empty content".
func (l *Loader) Loaded() bool {
	if l == nil {
		return false
	}
	return l.loaded
}
