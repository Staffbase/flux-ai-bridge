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

package webhook

import (
	"errors"
	"fmt"
	"testing"
)

// classifyOutcome maps processor errors to Prometheus label values. The
// label is the public observable, so behavior we want to pin: given an
// error in the exact shape the processor produces (fmt.Errorf("<phase>: %w",
// inner)), the right metric label comes out. If a phase prefix changes in
// the processor and these tests don't catch it, the dashboards rot
// silently.
func TestClassifyOutcome_Success(t *testing.T) {
	if got := classifyOutcome(nil); got != "success" {
		t.Errorf("classifyOutcome(nil) = %q, want %q", got, "success")
	}
}

func TestClassifyOutcome_PhasePrefixes(t *testing.T) {
	inner := errors.New("boom")
	cases := []struct {
		phase string // prefix the processor wraps with — see internal/processor/processor.go
		want  string
	}{
		// MCP transport / parsing → mcp_error
		{phase: "mcp", want: "mcp_error"},
		{phase: "parse inventory", want: "mcp_error"},

		{phase: "parse kustomization yaml", want: "mcp_error"},

		// GitHub API surface → github_error
		{phase: "find pr", want: "github_error"},
		{phase: "post pr comment", want: "github_error"},
		// resolve repository walks Kustomization→GitRepository→URL; the
		// label says github_error because the user-visible effect is "we
		// couldn't act on GitHub." Pinning that decision here.
		{phase: "resolve repository", want: "github_error"},

		// kstatus evaluation → readiness_error
		{phase: "readiness check", want: "readiness_error"},

		// Redis-backed dedupe down before any GitHub lookup → dedupe_error
		{phase: "dedupe unavailable", want: "dedupe_error"},
	}
	for _, tc := range cases {
		t.Run(tc.phase, func(t *testing.T) {
			err := fmt.Errorf("%s: %w", tc.phase, inner)
			if got := classifyOutcome(err); got != tc.want {
				t.Errorf("classifyOutcome(%q) = %q, want %q", err, got, tc.want)
			}
		})
	}
}

// Unknown / unwrapped errors fall through to other_error. This is the
// catch-all bucket — operators should alert when it's non-zero to discover
// new error shapes the classifier doesn't recognize yet.
func TestClassifyOutcome_UnknownFallsThrough(t *testing.T) {
	if got := classifyOutcome(errors.New("k8s api server timeout")); got != "other_error" {
		t.Errorf("unknown error must classify as other_error, got %q", got)
	}
}
