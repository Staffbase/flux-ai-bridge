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

package mcp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	gosdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSessionErrorClassification(t *testing.T) {
	rejected := fmt.Errorf("calling \"tools/call\": %w: sending: Post \"http://mcp\": EOF", &jsonrpc.Error{Code: -32005, Message: "rejected by transport"})
	tests := []struct {
		name              string
		err               error
		broken, retryable bool
	}{
		{"nil", nil, false, false},
		{"json-rpc error reply", fmt.Errorf("call: %w", &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "unexpected additional properties"}), false, false},
		{"rejected by transport", rejected, true, true},
		{"connection closed", fmt.Errorf("%w: calling %q: client is closing: failed to decode event", gosdk.ErrConnectionClosed, "tools/call"), true, true},
		{"session missing", fmt.Errorf("POST: failed to connect (session ID: x): %w", gosdk.ErrSessionMissing), true, true},
		{"our cancellation", fmt.Errorf("call: %w", context.Canceled), false, false},
		{"our deadline", fmt.Errorf("call: %w", context.DeadlineExceeded), false, false},
		// Former substring false positives: neither is a transport failure.
		{"text mentioning oneOf", errors.New("schema: value must match oneOf"), true, false},
		{"pod hash containing 404", errors.New("pod web-7d4040404-abc not ready"), true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sessionBroken(tt.err); got != tt.broken {
				t.Errorf("sessionBroken = %v, want %v", got, tt.broken)
			}
			if got := retryableCallError(tt.err); got != tt.retryable {
				t.Errorf("retryableCallError = %v, want %v", got, tt.retryable)
			}
		})
	}
}
