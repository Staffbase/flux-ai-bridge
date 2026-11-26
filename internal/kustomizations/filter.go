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

// Package kustomizations holds the Kustomization-name admission filter used
// by the webhook handler. Operators configure include/exclude glob lists via
// config.kustomizations.{include,exclude} in the Helm chart, which flow into
// KUSTOMIZATIONS_INCLUDE / KUSTOMIZATIONS_EXCLUDE env vars at runtime.
//
// Filter semantics (matches Prometheus relabel keep+drop, kustomize, gitignore):
//
//   - Empty include + empty exclude  → admit everything.
//   - Non-empty include              → name MUST match at least one include glob.
//   - Non-empty exclude              → name MUST NOT match any exclude glob.
//   - Both populated                 → match include AND not match exclude
//     ("deny wins"); the typical pattern is include=[apps-*] exclude=[apps-canary]
//     which admits apps-frontend, drops apps-canary, drops infra-cert-manager.
//
// Glob syntax is Go's stdlib path.Match: '*' matches any run of non-separator
// chars, '?' matches one, '[abc]' matches a class. Anchored implicitly — the
// pattern must match the entire name. No anchoring confusion, no YAML
// escape hell, no third-party regex flavor.
package kustomizations

import (
	"fmt"
	"path"
	"strings"
)

// Filter decides whether an event for a given Kustomization name should be
// admitted into the bridge pipeline. The zero value (nil-safe) admits
// everything.
type Filter struct {
	include []string
	exclude []string
}

// Reason values returned by Allow for dropped events. Used as the
// `reason` label on flux_ai_bridge_events_filtered_total — keep the set
// small and stable (two values total).
const (
	ReasonNotInInclude = "not_in_include"
	ReasonInExclude    = "in_exclude"
)

// New constructs a Filter from include/exclude glob lists and validates every
// pattern via path.Match against a probe string. Returns an error on the
// first invalid (path.ErrBadPattern) or empty pattern so the bridge fails
// fast at startup rather than silently treating typos as "no match" at
// runtime — see config.Load for the call site.
func New(include, exclude []string) (*Filter, error) {
	if err := validate("include", include); err != nil {
		return nil, err
	}
	if err := validate("exclude", exclude); err != nil {
		return nil, err
	}
	return &Filter{include: include, exclude: exclude}, nil
}

func validate(field string, patterns []string) error {
	for i, p := range patterns {
		if strings.TrimSpace(p) == "" {
			return fmt.Errorf("%s[%d]: empty pattern (set the env var to a non-empty glob, or remove the entry from values.yaml)", field, i)
		}
		if _, err := path.Match(p, "probe"); err != nil {
			return fmt.Errorf("%s[%d] = %q: %w", field, i, p, err)
		}
	}
	return nil
}

// Allow reports whether the event for the named Kustomization should be
// admitted. When false, reason is one of ReasonNotInInclude / ReasonInExclude.
// Nil receiver admits everything (used when the filter is unconfigured).
func (f *Filter) Allow(name string) (bool, string) {
	if f == nil {
		return true, ""
	}
	if len(f.include) > 0 && !matchAny(f.include, name) {
		return false, ReasonNotInInclude
	}
	if len(f.exclude) > 0 && matchAny(f.exclude, name) {
		return false, ReasonInExclude
	}
	return true, ""
}

func matchAny(patterns []string, name string) bool {
	for _, p := range patterns {
		// Validated at construction; path.Match only returns ErrBadPattern
		// on patterns we already rejected, so an error here is impossible.
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}
