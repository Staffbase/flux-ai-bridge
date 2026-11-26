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

package kustomizations

import (
	"strings"
	"testing"
)

func TestFilter_EmptyAdmitsAll(t *testing.T) {
	f, err := New(nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{"apps-frontend", "infra-cert-manager", ""} {
		ok, reason := f.Allow(name)
		if !ok {
			t.Errorf("Allow(%q) = false (reason=%q); want admitted", name, reason)
		}
	}
}

func TestFilter_NilReceiverAdmitsAll(t *testing.T) {
	var f *Filter
	if ok, _ := f.Allow("anything"); !ok {
		t.Error("nil receiver should admit everything")
	}
}

func TestFilter_IncludeOnly(t *testing.T) {
	f, err := New([]string{"apps-*"}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cases := map[string]bool{
		"apps-frontend":      true,
		"apps-backend":       true,
		"infra-cert-manager": false,
	}
	for name, want := range cases {
		got, reason := f.Allow(name)
		if got != want {
			t.Errorf("Allow(%q) = %v reason=%q, want %v", name, got, reason, want)
		}
		if !got && reason != ReasonNotInInclude {
			t.Errorf("Allow(%q) drop reason = %q, want %q", name, reason, ReasonNotInInclude)
		}
	}
}

func TestFilter_ExcludeOnlyTreatsEmptyIncludeAsMatchAll(t *testing.T) {
	f, err := New(nil, []string{"infra-*"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if ok, _ := f.Allow("apps-frontend"); !ok {
		t.Error("apps-frontend should be admitted under empty include + infra-* exclude")
	}
	ok, reason := f.Allow("infra-cert-manager")
	if ok {
		t.Error("infra-cert-manager should be dropped by infra-* exclude")
	}
	if reason != ReasonInExclude {
		t.Errorf("drop reason = %q, want %q", reason, ReasonInExclude)
	}
}

// Deny-wins: include carves a broad set, exclude scoops a hole out of it.
// include=[apps-*] exclude=[apps-canary] → apps-frontend admitted,
// apps-canary dropped (by exclude), infra-* dropped (not in include).
func TestFilter_DenyWins(t *testing.T) {
	f, err := New([]string{"apps-*"}, []string{"apps-canary"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cases := []struct {
		name       string
		wantAdmit  bool
		wantReason string
	}{
		{"apps-frontend", true, ""},
		{"apps-canary", false, ReasonInExclude},
		{"infra-cert-manager", false, ReasonNotInInclude},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := f.Allow(tc.name)
			if ok != tc.wantAdmit || reason != tc.wantReason {
				t.Errorf("Allow(%q) = (%v, %q), want (%v, %q)", tc.name, ok, reason, tc.wantAdmit, tc.wantReason)
			}
		})
	}
}

func TestFilter_RejectsBadPattern(t *testing.T) {
	_, err := New([]string{"[unclosed"}, nil)
	if err == nil {
		t.Fatal("expected error for malformed glob pattern")
	}
	if !strings.Contains(err.Error(), "include[0]") {
		t.Errorf("error %q should locate the bad pattern at include[0]", err)
	}
}

func TestFilter_RejectsEmptyPattern(t *testing.T) {
	_, err := New(nil, []string{"infra-*", ""})
	if err == nil {
		t.Fatal("expected error for empty pattern in exclude list")
	}
	if !strings.Contains(err.Error(), "exclude[1]") {
		t.Errorf("error %q should locate the empty pattern at exclude[1]", err)
	}
}

func TestFilter_RejectsWhitespaceOnlyPattern(t *testing.T) {
	_, err := New([]string{"   "}, nil)
	if err == nil {
		t.Fatal("expected error for whitespace-only pattern")
	}
}
