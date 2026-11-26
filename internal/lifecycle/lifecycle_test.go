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

package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

type valueKey struct{}

func TestDetach(t *testing.T) {
	shutdown, stopProcess := context.WithCancel(context.Background())
	defer stopProcess()
	parent, cancelParent := context.WithCancel(WithShutdown(context.WithValue(context.Background(), valueKey{}, "v"), shutdown))

	ctx, cancel := Detach(parent, time.Minute)
	defer cancel()
	cancelParent()
	time.Sleep(10 * time.Millisecond)
	if err := ctx.Err(); err != nil {
		t.Fatalf("detached context after the parent was cancelled: %v, want still alive", err)
	}
	if got := ctx.Value(valueKey{}); got != "v" {
		t.Fatalf("detached context lost the parent's values: %v", got)
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("detached context must carry its own deadline")
	}

	stopProcess()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("err = %v, want Canceled", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown must cancel the detached context")
	}
}

func TestDetachWithoutShutdownSignal(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	ctx, cancel := Detach(parent, 20*time.Millisecond)
	defer cancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("detached context: %v, want alive until its timeout", err)
	}
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("detached context must expire after its timeout")
	}
}
