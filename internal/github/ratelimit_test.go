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

package github

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	gh "github.com/google/go-github/v62/github"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
)

// recordRateLimit must split go-github responses into two buckets: typed
// rate-limit errors go to rate_limit_hits_total, generic 401/403s go to
// auth_failures_total. Conflating these (the pre-fix behavior) makes quota
// alerts false-positive on App-uninstalled and vice versa.
func TestRecordRateLimit_Classification(t *testing.T) {
	tests := []struct {
		name                 string
		resp                 *gh.Response
		err                  error
		wantRateLimitDelta   float64
		wantAuthFailureDelta float64
	}{
		{
			name:                 "RateLimitError (primary) bumps rate_limit_hits, not auth_failures",
			resp:                 &gh.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
			err:                  &gh.RateLimitError{Message: "API rate limit exceeded"},
			wantRateLimitDelta:   1,
			wantAuthFailureDelta: 0,
		},
		{
			name:                 "AbuseRateLimitError (secondary) bumps rate_limit_hits, not auth_failures",
			resp:                 &gh.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
			err:                  &gh.AbuseRateLimitError{Message: "secondary rate limit"},
			wantRateLimitDelta:   1,
			wantAuthFailureDelta: 0,
		},
		{
			name:                 "wrapped RateLimitError still classifies via errors.As",
			resp:                 &gh.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
			err:                  fmt.Errorf("list pull requests with commit: %w", &gh.RateLimitError{}),
			wantRateLimitDelta:   1,
			wantAuthFailureDelta: 0,
		},
		{
			name:                 "plain 401 (PAT revoked / App uninstalled) bumps auth_failures only",
			resp:                 &gh.Response{Response: &http.Response{StatusCode: http.StatusUnauthorized}},
			err:                  errors.New("unauthorized"),
			wantRateLimitDelta:   0,
			wantAuthFailureDelta: 1,
		},
		{
			name:                 "plain 403 with no rate-limit error type bumps auth_failures only",
			resp:                 &gh.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
			err:                  errors.New("forbidden"),
			wantRateLimitDelta:   0,
			wantAuthFailureDelta: 1,
		},
		{
			name:                 "200 OK + nil err bumps neither",
			resp:                 &gh.Response{Response: &http.Response{StatusCode: http.StatusOK}},
			err:                  nil,
			wantRateLimitDelta:   0,
			wantAuthFailureDelta: 0,
		},
		{
			name:                 "nil resp + RateLimitError still bumps rate_limit_hits (transport-level errors)",
			resp:                 nil,
			err:                  &gh.RateLimitError{},
			wantRateLimitDelta:   1,
			wantAuthFailureDelta: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			beforeRL := testutil.ToFloat64(metrics.GitHubRateLimitHits)
			beforeAF := testutil.ToFloat64(metrics.GitHubAuthFailures)

			recordRateLimit(tc.resp, tc.err)

			gotRL := testutil.ToFloat64(metrics.GitHubRateLimitHits) - beforeRL
			gotAF := testutil.ToFloat64(metrics.GitHubAuthFailures) - beforeAF
			if gotRL != tc.wantRateLimitDelta {
				t.Errorf("rate_limit_hits delta = %v, want %v", gotRL, tc.wantRateLimitDelta)
			}
			if gotAF != tc.wantAuthFailureDelta {
				t.Errorf("auth_failures delta = %v, want %v", gotAF, tc.wantAuthFailureDelta)
			}
		})
	}
}

// RateLimitCooldown follows GitHub's own hint (secondary Retry-After, primary
// reset time) within [1m, 1h], defaults to 10m without one, and is 0 for
// anything that isn't a rate limit.
func TestRateLimitCooldown(t *testing.T) {
	retry := func(d time.Duration) *time.Duration { return &d }
	reset := func(d time.Duration) gh.Rate { return gh.Rate{Reset: gh.Timestamp{Time: time.Now().Add(d)}} }
	tests := []struct {
		name     string
		err      error
		min, max time.Duration
	}{
		{"secondary with Retry-After", &gh.AbuseRateLimitError{RetryAfter: retry(3 * time.Minute)}, 3 * time.Minute, 3 * time.Minute},
		{"secondary Retry-After below the floor", &gh.AbuseRateLimitError{RetryAfter: retry(5 * time.Second)}, time.Minute, time.Minute},
		{"secondary without Retry-After", &gh.AbuseRateLimitError{}, 10 * time.Minute, 10 * time.Minute},
		{"primary until reset", &gh.RateLimitError{Rate: reset(20 * time.Minute)}, 19 * time.Minute, 20 * time.Minute},
		{"primary reset beyond the cap", &gh.RateLimitError{Rate: reset(3 * time.Hour)}, time.Hour, time.Hour},
		{"primary without reset", &gh.RateLimitError{}, 10 * time.Minute, 10 * time.Minute},
		{"wrapped", fmt.Errorf("post pr comment: %w", &gh.AbuseRateLimitError{RetryAfter: retry(2 * time.Minute)}), 2 * time.Minute, 2 * time.Minute},
		{"not a rate limit", errors.New("boom"), 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RateLimitCooldown(tt.err); got < tt.min || got > tt.max {
				t.Errorf("RateLimitCooldown = %s, want within [%s, %s]", got, tt.min, tt.max)
			}
		})
	}
}

// A response without rate-limit headers (go-github leaves Rate zeroed, e.g.
// a 502 from GitHub's edge) must not drive the remaining-quota gauge to 0.
func TestRecordRateLimit_IgnoresResponsesWithoutRateHeaders(t *testing.T) {
	recordRateLimit(&gh.Response{Response: &http.Response{StatusCode: http.StatusOK}, Rate: gh.Rate{Limit: 5000, Remaining: 4321}}, nil)
	recordRateLimit(&gh.Response{Response: &http.Response{StatusCode: http.StatusBadGateway}}, errors.New("bad gateway"))
	if got := testutil.ToFloat64(metrics.GitHubRateLimitRemaining); got != 4321 {
		t.Fatalf("remaining gauge = %v, want 4321 kept from the last response that carried headers", got)
	}
}
