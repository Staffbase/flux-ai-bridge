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

package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		raw  string
		want slog.Level
	}{
		{"", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"  info  ", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"Debug", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"ERROR", slog.LevelError},
		{"nonsense", slog.LevelInfo},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			if got := parseLogLevel(tc.raw); got != tc.want {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParseIntEnv(t *testing.T) {
	const key = "FLUX_AI_BRIDGE_TEST_INT"

	tests := []struct {
		name     string
		value    string
		fallback int
		want     int
	}{
		{"unset uses fallback", "", 16, 16},
		{"valid positive", "42", 16, 42},
		{"whitespace trimmed", "  7  ", 16, 7},
		{"non-numeric uses fallback", "abc", 16, 16},
		{"zero uses fallback", "0", 16, 16},
		{"negative uses fallback", "-5", 16, 16},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(key, tc.value)
			if got := parseIntEnv(key, tc.fallback); got != tc.want {
				t.Errorf("parseIntEnv(%q=%q, %d) = %d, want %d", key, tc.value, tc.fallback, got, tc.want)
			}
		})
	}
}

// A configured but unbuildable dedupe persistence must stop startup instead
// of falling back to a per-process in-memory cache: with several replicas,
// each would admit the same PR-head on its own.
func TestNewDedupe_ConfiguredPersistenceFailsClosed(t *testing.T) {
	const secret = "s3cr3t-pw"
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"malformed URL", map[string]string{
			"DEDUPE_PERSISTENCE_URL": "redis://user:" + secret + "@redis.example.com:bad",
		}},
		{"sentinel without master name", map[string]string{
			"DEDUPE_PERSISTENCE_SENTINEL_ADDRS": "sentinel.example.com:26379",
		}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range []string{"DEDUPE_PERSISTENCE_URL", "DEDUPE_PERSISTENCE_SENTINEL_ADDRS", "DEDUPE_PERSISTENCE_MASTER_NAME", "DEDUPE_TTL"} {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			c, err := newDedupe(logger, "test:")
			if err == nil {
				t.Fatalf("newDedupe() = %v, nil; want an error, not an in-memory fallback", c)
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error leaks the persistence password: %v", err)
			}
		})
	}
}

// The Redis URL parse error quotes the raw URL, userinfo included; the
// startup error must not carry it into the log.
func TestNewDedupe_MalformedURLErrorOmitsCredentials(t *testing.T) {
	t.Setenv("DEDUPE_PERSISTENCE_SENTINEL_ADDRS", "")
	t.Setenv("DEDUPE_PERSISTENCE_URL", "redis://user:s3cr3t-pw@redis.example.com:bad")
	_, err := newDedupe(slog.New(slog.NewTextHandler(io.Discard, nil)), "test:")
	if err == nil {
		t.Fatal("newDedupe() returned no error for a malformed URL")
	}
	for _, leak := range []string{"s3cr3t-pw", "user:", "redis://"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("error contains %q: %v", leak, err)
		}
	}
	if !strings.Contains(err.Error(), "port") {
		t.Errorf("error lost the cause (invalid port): %v", err)
	}
}
