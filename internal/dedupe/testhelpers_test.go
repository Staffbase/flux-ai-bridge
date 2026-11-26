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

package dedupe_test

import (
	"testing"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
)

// markNew admits (ns, name, sha) as PR #1's head with no re-eval floor, so
// tests about the plain admission contract stay short. A backend error fails
// the test rather than reading as a silent "false".
func markNew(t *testing.T, c *dedupe.Cache, ns, name, sha string) bool {
	t.Helper()
	return markPRNew(t, c, ns, name, 1, sha, 0)
}

// markPRNew is markNew's per-PR-head sibling, wrapping Cache.MarkPRIfNew with
// the same fail-fast error contract: a backend error must fail the test, not
// read as a silent "false" that could green-light a broken dedupe path. Not
// safe inside goroutines (t.Fatalf) — concurrent tests check the error inline.
func markPRNew(t *testing.T, c *dedupe.Cache, ns, name string, pr int, sha string, ttl time.Duration) bool {
	t.Helper()
	isNew, err := c.MarkPRIfNew(ns, name, pr, sha, ttl)
	if err != nil {
		t.Fatalf("MarkPRIfNew(%s, %s, %d, %s): %v", ns, name, pr, sha, err)
	}
	return isNew
}
