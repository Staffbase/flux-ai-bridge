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
	"log/slog"
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
