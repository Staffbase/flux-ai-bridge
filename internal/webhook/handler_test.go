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
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/staffbase/flux-ai-bridge/internal/kustomizations"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
	corev1 "k8s.io/api/core/v1"
)

const (
	testSHA    = "0123456789abcdef0123456789abcdef01234567"
	testSHA256 = testSHA + "0123456789abcdef01234567"
)

// Revisions that don't carry a full commit SHA are skipped before the
// processor runs — they can't be looked up on GitHub, and the SHA is spliced
// into GitHub API paths, so path fragments must never get through.
func TestHandler_RejectsNonCommitRevisions(t *testing.T) {
	for _, revision := range []string{
		"",
		"main@sha1:abc1234",               // short
		"main@sha1:" + testSHA + "00",     // 42 chars
		"main@sha1:../../../../orgs/x",    // path injection
		"main@sha1:" + testSHA[:39] + "g", // non-hex
		"main/" + testSHA,                 // pre-v2 format
	} {
		t.Run(revision, func(t *testing.T) {
			proc := newMockProcessor()
			h := newTestHandler(proc)
			rr := postWebhook(h, newKustomizationEvent("apps", "default", revision))
			if rr.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204 (skipped)", rr.Code)
			}
			select {
			case <-proc.called:
				t.Fatal("processor must not run for a revision without a full commit SHA")
			case <-time.After(20 * time.Millisecond):
			}
		})
	}
}

func TestExtractSHA(t *testing.T) {
	tests := []struct {
		revision string
		want     string
	}{
		{revision: "main@sha1:abc123def456", want: "abc123def456"},
		{revision: "main@sha1:  abc123  ", want: "abc123"},
		{revision: "abc123def456", want: "abc123def456"},
		{revision: "", want: ""},
		{revision: "main@sha1:", want: ""},
		{revision: "refs/heads/main@sha1:deadbeef", want: "deadbeef"},
		{revision: "main@sha256:def456abc789", want: "def456abc789"},
		{revision: "fix-sha1:thing@sha256:cafebabe", want: "cafebabe"},
	}
	for _, tc := range tests {
		got := extractSHA(tc.revision)
		if got != tc.want {
			t.Errorf("extractSHA(%q) = %q, want %q", tc.revision, got, tc.want)
		}
	}
}

type mockProcessor struct {
	called chan struct{}
	sha    string
	err    error
}

func newMockProcessor() *mockProcessor {
	return &mockProcessor{called: make(chan struct{}, 1)}
}

func (m *mockProcessor) Process(_ context.Context, _ eventv1.Event, sha string) error {
	m.sha = sha
	select {
	case m.called <- struct{}{}:
	default:
	}
	return m.err
}

func (m *mockProcessor) waitCalled(t *testing.T) {
	t.Helper()
	select {
	case <-m.called:
	case <-time.After(2 * time.Second):
		t.Fatal("processor.Process was not called within timeout")
	}
}

func newKustomizationEvent(name, namespace, revision string) eventv1.Event {
	return eventv1.Event{
		InvolvedObject: corev1Ref("Kustomization", name, namespace),
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
		Message:        "Applied revision: " + revision,
		Metadata: map[string]string{
			eventv1.MetaRevisionKey: revision,
		},
	}
}

func corev1Ref(kind, name, namespace string) corev1ObjRef {
	return corev1ObjRef{Kind: kind, Name: name, Namespace: namespace}
}

type corev1ObjRef = corev1.ObjectReference

func postWebhook(h http.Handler, body any) *httptest.ResponseRecorder {
	data, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func newTestHandler(proc Processor) *Handler {
	return NewHandler(proc, discardLogger(), Options{MaxConcurrent: 4}) // HMAC disabled in unit tests
}

func TestHandler_WrongMethod(t *testing.T) {
	h := newTestHandler(newMockProcessor())
	req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandler_WrongPath(t *testing.T) {
	h := newTestHandler(newMockProcessor())
	req := httptest.NewRequest(http.MethodPost, "/other", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	h := newTestHandler(newMockProcessor())
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString("not-json"))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestHandler_NonKustomizationEvent(t *testing.T) {
	proc := newMockProcessor()
	h := newTestHandler(proc)
	event := eventv1.Event{
		InvolvedObject: corev1Ref("HelmRelease", "my-release", "default"),
		Severity:       eventv1.EventSeverityInfo,
	}
	rr := postWebhook(h, event)
	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
}

func TestHandler_EmptySHASkipped(t *testing.T) {
	proc := newMockProcessor()
	h := newTestHandler(proc)
	event := newKustomizationEvent("apps", "default", "")
	rr := postWebhook(h, event)
	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	select {
	case <-proc.called:
		t.Fatal("processor should not be invoked when SHA is empty")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandler_KustomizationEvent_Accepted(t *testing.T) {
	proc := newMockProcessor()
	h := newTestHandler(proc)
	event := newKustomizationEvent("apps", "default", "main@sha1:0123456789abcdef0123456789abcdef01234567")
	rr := postWebhook(h, event)
	if rr.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusAccepted)
	}
	proc.waitCalled(t)
	if proc.sha != testSHA {
		t.Errorf("sha = %q, want %q", proc.sha, testSHA)
	}
}

func TestHandler_KustomizationEvent_SHAExtracted(t *testing.T) {
	tests := []struct {
		revision string
		wantSHA  string
	}{
		{"main@sha1:" + testSHA, testSHA},
		{testSHA, testSHA},
		{"main@sha1:" + strings.ToUpper(testSHA), testSHA},
		{"main@sha256:" + testSHA256, testSHA256},
	}
	for _, tc := range tests {
		t.Run(tc.revision, func(t *testing.T) {
			proc := newMockProcessor()
			h := newTestHandler(proc)
			event := newKustomizationEvent("apps", "default", tc.revision)
			postWebhook(h, event)
			proc.waitCalled(t)
			if proc.sha != tc.wantSHA {
				t.Errorf("sha = %q, want %q", proc.sha, tc.wantSHA)
			}
		})
	}
}

func TestHandler_BodyTooLarge(t *testing.T) {
	h := newTestHandler(newMockProcessor())
	big := bytes.Repeat([]byte("a"), maxBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(big))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestHandler_HMAC(t *testing.T) {
	const secret = "shared-with-flux-notification-controller"
	proc := newMockProcessor()
	h := NewHandler(proc, discardLogger(), Options{MaxConcurrent: 4, HMACSecret: secret})

	event := newKustomizationEvent("apps", "default", "main@sha1:0123456789abcdef0123456789abcdef01234567")
	body, _ := json.Marshal(event)

	// Compute the correct signature.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	goodSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	send := func(sig string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if sig != "" {
			req.Header.Set("X-Signature", sig)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	t.Run("valid signature accepted", func(t *testing.T) {
		rr := send(goodSig)
		if rr.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", rr.Code)
		}
		proc.waitCalled(t)
	})
	t.Run("missing signature rejected", func(t *testing.T) {
		rr := send("")
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rr.Code)
		}
	})
	t.Run("wrong signature rejected", func(t *testing.T) {
		rr := send("sha256=deadbeef")
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rr.Code)
		}
	})
	t.Run("malformed signature rejected", func(t *testing.T) {
		rr := send("sha1=" + hex.EncodeToString(mac.Sum(nil)))
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401 (wrong algo)", rr.Code)
		}
	})
}

// Dedup is no longer a handler concern — it moved into the processor, keyed
// on each PR's own head commit. The handler-level dedup tests that used to
// live here (same-SHA dedup, dedupe-unhealthy fail-closed, failed-pipeline
// eviction, same-SHA-different-severity) now belong to the processor's
// per-PR dedup tests in internal/processor.

// TestHandler_FilterDrops asserts the Kustomization admission filter drops
// events before the processor is invoked, and ticks the EventsFiltered
// counter with the correct reason label so operators can verify the filter
// via Prometheus instead of grep.
func TestHandler_FilterDrops(t *testing.T) {
	filter, err := kustomizations.New(nil, []string{"infra-*"})
	if err != nil {
		t.Fatalf("New filter: %v", err)
	}
	proc := newMockProcessor()
	h := NewHandler(proc, discardLogger(), Options{MaxConcurrent: 4, Filter: filter})

	// Metrics are package-global; read the baseline so this test doesn't
	// have to assume the counter starts at zero (other tests in the suite
	// may also hit it once filter coverage grows).
	beforeFiltered := testutil.ToFloat64(metrics.EventsFiltered.WithLabelValues(kustomizations.ReasonInExclude))
	beforeSkipped := testutil.ToFloat64(metrics.EventsProcessed.WithLabelValues("skipped"))

	dropped := newKustomizationEvent("infra-cert-manager", "flux-system", "main@sha1:0123456789abcdef0123456789abcdef01234567")
	rr := postWebhook(h, dropped)
	if rr.Code != http.StatusNoContent {
		t.Errorf("filtered event status = %d, want 204", rr.Code)
	}
	select {
	case <-proc.called:
		t.Fatal("processor invoked on filtered event")
	case <-time.After(100 * time.Millisecond):
	}
	if got := testutil.ToFloat64(metrics.EventsFiltered.WithLabelValues(kustomizations.ReasonInExclude)) - beforeFiltered; got != 1 {
		t.Errorf("EventsFiltered{reason=in_exclude} delta = %v, want 1", got)
	}
	// Filter drops also tick EventsProcessed{skipped} so sum(EventsProcessed)
	// stays a complete count of terminal events.
	if got := testutil.ToFloat64(metrics.EventsProcessed.WithLabelValues("skipped")) - beforeSkipped; got != 1 {
		t.Errorf("EventsProcessed{outcome=skipped} delta = %v, want 1 (filter drops must also bump EventsProcessed)", got)
	}

	admitted := newKustomizationEvent("apps-frontend", "flux-system", "main@sha1:0123456789abcdef0123456789abcdef01234567")
	rr = postWebhook(h, admitted)
	if rr.Code != http.StatusAccepted {
		t.Errorf("admitted event status = %d, want 202", rr.Code)
	}
	proc.waitCalled(t)
}

func TestHandler_Wait(t *testing.T) {
	proc := newMockProcessor()
	h := newTestHandler(proc)
	event := newKustomizationEvent("apps", "default", "main@sha1:0123456789abcdef0123456789abcdef01234567")
	postWebhook(h, event)
	proc.waitCalled(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// blockingProcessor holds every Process call until release is closed.
type blockingProcessor struct {
	release chan struct{}
	started chan struct{}
	done    atomic.Int32
}

func (b *blockingProcessor) Process(context.Context, eventv1.Event, string) error {
	b.started <- struct{}{}
	<-b.release
	b.done.Add(1)
	return nil
}

// Events beyond the running slots wait instead of being turned away (Flux
// retries a 503 only briefly); only a full queue answers 503.
func TestHandler_QueuesEventsBeyondConcurrency(t *testing.T) {
	proc := &blockingProcessor{release: make(chan struct{}), started: make(chan struct{}, 8)}
	// Release the workers however the test ends, so a failed assertion
	// doesn't leave them blocked.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(proc.release) }) }
	t.Cleanup(release)
	h := NewHandler(proc, discardLogger(), Options{MaxConcurrent: 1, QueueSize: 3})
	codes := make([]int, 0, 5)
	for range 5 {
		codes = append(codes, postWebhook(h, newKustomizationEvent("apps", "default", "main@sha1:"+testSHA)).Code)
	}
	want := []int{http.StatusAccepted, http.StatusAccepted, http.StatusAccepted, http.StatusAccepted, http.StatusServiceUnavailable}
	for i := range want {
		if codes[i] != want[i] {
			t.Fatalf("responses = %v, want %v (1 running + 3 queued, then 503)", codes, want)
		}
	}
	select {
	case <-proc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("no accepted event started processing")
	}
	select {
	case <-proc.started:
		t.Fatal("a queued event started while the only slot was busy")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := proc.done.Load(); got != 4 {
		t.Fatalf("processed %d events, want all 4 accepted ones", got)
	}
}

// The queue holds the running events plus QueueSize waiting ones; unset, the
// default lets a revision's whole event wave wait instead of answering 503.
func TestNewHandler_QueueSize(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want int
	}{
		{name: "defaults", opts: Options{}, want: 16 + defaultQueueSize},
		{name: "configured", opts: Options{MaxConcurrent: 4, QueueSize: 10}, want: 14},
		{name: "negative uses default", opts: Options{MaxConcurrent: 4, QueueSize: -1}, want: 4 + defaultQueueSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(&countingProcessor{}, discardLogger(), tt.opts)
			if got := cap(h.queue); got != tt.want {
				t.Errorf("queue capacity = %d, want %d", got, tt.want)
			}
		})
	}
}

// countingProcessor counts Process calls.
type countingProcessor struct{ calls atomic.Int32 }

func (c *countingProcessor) Process(context.Context, eventv1.Event, string) error {
	c.calls.Add(1)
	return nil
}

// An event that arrives after Shutdown began gets a 503 (Flux retries it),
// never a 202 for work that won't run, and doesn't Add to the WaitGroup
// Shutdown is waiting on.
func TestShutdown_RejectsEventsAfterShutdown(t *testing.T) {
	proc := &countingProcessor{}
	h := NewHandler(proc, discardLogger(), Options{})
	if err := h.Shutdown(context.Background(), time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	before := testutil.ToFloat64(metrics.EventsProcessed.WithLabelValues("busy"))
	rr := postWebhook(h, newKustomizationEvent("apps", "default", "main@sha1:"+testSHA))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 after shutdown", rr.Code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := proc.calls.Load(); got != 0 {
		t.Fatalf("processor ran %d times after shutdown, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.EventsProcessed.WithLabelValues("busy")) - before; got != 1 {
		t.Errorf("EventsProcessed{outcome=busy} delta = %v, want 1", got)
	}
}

// A queued event whose slot frees up as Shutdown cancels sees both select
// cases ready; it must not start a pipeline on the cancelled context. The
// loop makes a random select pick show up: without the check, about half
// the runs call Process.
func TestHandler_QueuedEventDoesNotStartAfterCancel(t *testing.T) {
	for range 64 {
		proc := &countingProcessor{}
		h := newTestHandler(proc)
		h.cancelBase() // shutdown signal fired; a running slot is free
		if rr := postWebhook(h, newKustomizationEvent("apps", "default", "main@sha1:"+testSHA)); rr.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (admission is still open)", rr.Code)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := h.Wait(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if got := proc.calls.Load(); got != 0 {
			t.Fatalf("processor ran %d times on a cancelled context, want 0", got)
		}
	}
}

// Requests that can't be read or decoded as an event still end in exactly
// one EventsProcessed sample, so sum(EventsProcessed) counts them.
func TestHandler_InvalidRequestsCounted(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
		want int
	}{
		{name: "invalid json", body: []byte("{not json"), want: http.StatusBadRequest},
		{name: "too large", body: bytes.Repeat([]byte("a"), maxBodyBytes+1), want: http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := &countingProcessor{}
			h := newTestHandler(proc)
			before := testutil.ToFloat64(metrics.EventsProcessed.WithLabelValues("invalid"))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(tc.body)))
			if rr.Code != tc.want {
				t.Fatalf("status = %d, want %d", rr.Code, tc.want)
			}
			if got := testutil.ToFloat64(metrics.EventsProcessed.WithLabelValues("invalid")) - before; got != 1 {
				t.Errorf("EventsProcessed{outcome=invalid} delta = %v, want 1", got)
			}
			if got := proc.calls.Load(); got != 0 {
				t.Errorf("processor ran %d times, want 0", got)
			}
		})
	}
}

// A clean drain must still fire the shutdown signal: detached work (an
// artifact download whose caller gave up) isn't tracked by Wait and only
// stops on it.
func TestShutdown_CancelsShutdownSignalAfterCleanDrain(t *testing.T) {
	h := NewHandler(nil, slog.Default(), Options{})
	if err := h.Shutdown(context.Background(), time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if h.baseCtx.Err() == nil {
		t.Fatal("shutdown signal not cancelled after a clean drain")
	}
}
