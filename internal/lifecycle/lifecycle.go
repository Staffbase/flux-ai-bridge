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

// Package lifecycle carries the process-wide shutdown signal through request
// contexts. Work that detaches from a request's deadline (delivery of an
// analysis that is already paid for, cleanup after a post) must still stop
// on shutdown, so the request's deferred rollback runs before the process
// exits. Once a request's own deadline has fired, its context can no longer
// report a later shutdown — hence a separate signal.
package lifecycle

import (
	"context"
	"time"
)

type shutdownKey struct{}

// WithShutdown returns ctx carrying shutdown, a context that is cancelled when
// the process starts shutting down.
func WithShutdown(ctx, shutdown context.Context) context.Context {
	return context.WithValue(ctx, shutdownKey{}, shutdown)
}

// Detach returns a context that ignores ctx's deadline and cancellation but
// keeps its values, is cancelled on shutdown (when ctx carries the signal),
// and expires after timeout. Callers must call the returned CancelFunc.
func Detach(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	shutdown, ok := ctx.Value(shutdownKey{}).(context.Context)
	if !ok {
		return dctx, cancel
	}
	stop := context.AfterFunc(shutdown, cancel)
	return dctx, func() {
		stop()
		cancel()
	}
}
