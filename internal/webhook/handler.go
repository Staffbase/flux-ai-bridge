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
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/kustomizations"
	"github.com/staffbase/flux-ai-bridge/internal/lifecycle"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
)

const (
	maxBodyBytes = 1 << 20 // 1 MiB

	// defaultProcessingTimeout is the fallback when Options.ProcessingTimeout
	// is unset. main.go derives a value from ReadinessPollTimeout + slack so
	// the per-event pipeline budget always exceeds the readiness poll budget.
	defaultProcessingTimeout = 5 * time.Minute

	// defaultQueueSize is how many events may wait for a running slot when
	// Options.QueueSize is unset. Events beyond the running ones wait instead
	// of being turned away: notification-controller retries a 503 only a few
	// times within about a minute, and a new source revision makes every
	// Kustomization that uses it send an event within seconds. The queue
	// must hold such a wave. A waiting event costs one idle goroutine plus
	// the decoded event (a few KB), and its processing deadline starts only
	// once it gets a slot.
	defaultQueueSize = 512

	// maxQueueSize caps QueueSize far above any real burst, so a mistyped
	// setting can't overflow the queue capacity.
	maxQueueSize = 1 << 20
)

type Processor interface {
	Process(ctx context.Context, event eventv1.Event, sha string) error
}

type Handler struct {
	filter            *kustomizations.Filter // nil-safe: nil admits everything
	hmacSecret        []byte                 // nil/empty = HMAC validation disabled
	logger            *slog.Logger
	processingTimeout time.Duration
	processor         Processor
	sem               chan struct{} // running pipelines
	queue             chan struct{} // accepted events: running + waiting for a sem slot
	wg                sync.WaitGroup
	// mu guards closed and orders every wg.Add before Shutdown's Wait:
	// once Shutdown sets closed, no new event is admitted.
	mu     sync.Mutex
	closed bool
	// baseCtx parents every worker goroutine's context so Shutdown can
	// cancel in-flight pipelines that outlive the graceful-wait budget.
	// Without it, killing the process mid-Process would skip the
	// processor's deferred per-PR dedupe rollback — with a persistent
	// (Redis) backend and no TTL, that PR-head would never be re-admitted.
	baseCtx    context.Context
	cancelBase context.CancelFunc
}

// Options configures the webhook handler. Empty hmacSecret disables HMAC
// validation.
type Options struct {
	HMACSecret    string
	MaxConcurrent int
	// QueueSize is how many accepted events may wait for a running slot;
	// beyond that the handler answers 503. Zero or less uses
	// defaultQueueSize; more than maxQueueSize uses maxQueueSize.
	QueueSize int
	// Filter admits/drops events by Kustomization name (config.kustomizations
	// in the chart). Nil admits everything — see internal/kustomizations.
	Filter *kustomizations.Filter
	// ProcessingTimeout caps a single event's full pipeline (resolve repo →
	// MCP → readiness poll → LLM → GitHub). MUST be greater than the
	// readiness poll timeout, otherwise the readiness poll never gets its
	// full budget and every analysis aborts with "context deadline exceeded"
	// just before the workload settles.
	ProcessingTimeout time.Duration
}

// NewHandler creates a webhook handler. Dedup is no longer a handler concern:
// it moved into the processor, which keys it on each PR's own head commit
// (not the Flux source revision) so an unchanged PR isn't re-analysed every
// time the source branch advances. Cross-replica coordination still holds —
// the processor's per-PR SETNX admits exactly one replica per (PR, head).
// Every replica accepts every webhook; no leader-election gate is needed.
func NewHandler(processor Processor, logger *slog.Logger, opts Options) *Handler {
	maxConcurrent := opts.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 16
	}
	queueSize := opts.QueueSize
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	if queueSize > maxQueueSize {
		if logger != nil {
			logger.Warn("WEBHOOK_QUEUE_SIZE is above the maximum; using the maximum",
				"configured", queueSize, "used", maxQueueSize)
		}
		queueSize = maxQueueSize
	}
	// The queue holds the running events plus the waiting ones. Saturate
	// instead of overflowing for an absurd maxConcurrent.
	queueCap := maxConcurrent + queueSize
	if queueCap < maxConcurrent {
		queueCap = math.MaxInt
	}
	processingTimeout := opts.ProcessingTimeout
	if processingTimeout <= 0 {
		processingTimeout = defaultProcessingTimeout
	}
	h := &Handler{
		processor:         processor,
		filter:            opts.Filter,
		logger:            logger,
		processingTimeout: processingTimeout,
		sem:               make(chan struct{}, maxConcurrent),
		queue:             make(chan struct{}, queueCap),
	}
	h.baseCtx, h.cancelBase = context.WithCancel(context.Background())
	if opts.HMACSecret != "" {
		h.hmacSecret = []byte(opts.HMACSecret)
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path != "/webhook" {
		http.NotFound(w, r)
		return
	}

	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		metrics.EventsProcessed.WithLabelValues("invalid").Inc()
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Validate Flux Notification Controller's HMAC signature if configured.
	// Flux signs the raw body with the Secret's `token` field; the signature
	// arrives as `X-Signature: sha256=<hex>`. We compare in constant time.
	if h.hmacSecret != nil {
		if !validateHMAC(r.Header.Get("X-Signature"), body, h.hmacSecret) {
			h.logger.Warn("webhook signature verification failed", "remote", r.RemoteAddr)
			metrics.EventsProcessed.WithLabelValues("unauthorized").Inc()
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	var event eventv1.Event
	if err := json.Unmarshal(body, &event); err != nil {
		metrics.EventsProcessed.WithLabelValues("invalid").Inc()
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if event.InvolvedObject.Kind != "Kustomization" {
		// HelmRelease / HelmChart / Bucket / etc. events can reach the
		// bridge in a few scenarios: an Alert in the cluster is over-
		// broadly subscribed (the chart's Alert is scoped to Kustomization,
		// but operators may have additional Alerts), the e2e suite POSTs
		// synthetic non-Kustomization events to verify they're rejected
		// (TestWebhookNonKustomizationIgnored), or a misrouted webhook
		// landed here. The bridge has nothing to say about non-Kustomization
		// kinds — surface this at DEBUG so steady-state INFO logs only
		// show events the bridge actually processes.
		h.logger.Debug("ignoring non-Kustomization event",
			"kind", event.InvolvedObject.Kind,
			"name", event.InvolvedObject.Name,
			"namespace", event.InvolvedObject.Namespace)
		metrics.EventsProcessed.WithLabelValues("skipped").Inc()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h.logger.Info("received flux event", "kind", event.InvolvedObject.Kind, "name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace, "severity", event.Severity, "reason", event.Reason, "message", event.Message, "metadata", event.Metadata)

	revision := event.Metadata[eventv1.MetaRevisionKey]
	sha := strings.ToLower(extractSHA(revision))
	if !commitSHA.MatchString(sha) {
		h.logger.Info("event revision carries no full commit SHA, skipping",
			"name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace, "revision", revision)
		metrics.EventsProcessed.WithLabelValues("skipped").Inc()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Kustomization-name admission filter (config.kustomizations.{include,exclude}).
	// Dedup is per-PR-head and lives in the processor now, so this filter is
	// the only admission gate at the door.
	if admitted, reason := h.filter.Allow(event.InvolvedObject.Name); !admitted {
		h.logger.Debug("event filtered by kustomizations admission filter",
			"name", event.InvolvedObject.Name,
			"namespace", event.InvolvedObject.Namespace,
			"reason", reason)
		// Two counters fire together by design:
		//   EventsProcessed{outcome="skipped"} keeps the "every terminal event
		//     is counted exactly once in EventsProcessed" invariant — so
		//     sum(EventsProcessed) is the canonical total-events query.
		//   EventsFiltered{reason=…} is the per-reason detail view of the
		//     same drops, scoped to filter outcomes only.
		// Filter drops are therefore a strict subset of skipped; never sum
		// the two metrics — that double-counts.
		metrics.EventsFiltered.WithLabelValues(reason).Inc()
		metrics.EventsProcessed.WithLabelValues("skipped").Inc()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Admission and wg.Add happen under mu, so Shutdown's Wait never races a
	// late Add, and an event arriving after shutdown began gets a 503 (Flux
	// retries it) instead of a 202 for work that would never run.
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		h.logger.Warn("shutting down, dropping event", "name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace)
		metrics.EventsProcessed.WithLabelValues("busy").Inc()
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	select {
	case h.queue <- struct{}{}:
	default:
		h.mu.Unlock()
		h.logger.Warn("event queue full, dropping event", "name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace)
		metrics.EventsProcessed.WithLabelValues("busy").Inc()
		// Dedup marks are only written inside the processor, after the PR
		// lookup, so an event dropped at the door leaves no dedup state to
		// undo — the next Flux re-fire runs it.
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	h.wg.Add(1)
	h.mu.Unlock()

	go func(evt eventv1.Event, revisionSHA string) {
		defer h.wg.Done()
		defer func() { <-h.queue }()
		// Wait for a running slot; the processing deadline starts once we
		// have one. Shutdown (base context cancelled) abandons the wait.
		select {
		case h.sem <- struct{}{}:
		case <-h.baseCtx.Done():
			metrics.EventsProcessed.WithLabelValues("busy").Inc()
			return
		}
		defer func() { <-h.sem }()
		// When a slot frees up as Shutdown cancels, select may pick either
		// ready case: don't start a pipeline on a cancelled context.
		if h.baseCtx.Err() != nil {
			metrics.EventsProcessed.WithLabelValues("busy").Inc()
			return
		}
		// Panic recovery: the LLM client, YAML parser, and go-github can all
		// surface panics under pathological inputs. Without a recover here a
		// single bad event would take down the whole bridge process. Any
		// per-PR dedup marks committed before the panic are rolled back by
		// the processor's deferred cleanup as the stack unwinds, so retries
		// still succeed.
		defer func() {
			if r := recover(); r != nil {
				h.logger.Error("panic in processor goroutine",
					"name", evt.InvolvedObject.Name,
					"namespace", evt.InvolvedObject.Namespace,
					"panic", r)
				metrics.EventsProcessed.WithLabelValues("other_error").Inc()
			}
		}()
		ctx, cancel := context.WithTimeout(h.baseCtx, h.processingTimeout)
		defer cancel()
		// Delivery and cleanup detach from the processing deadline but must
		// still stop on Shutdown — which ctx can't report once its deadline
		// fired first.
		ctx = lifecycle.WithShutdown(ctx, h.baseCtx)
		start := time.Now()
		err := h.processor.Process(ctx, evt, revisionSHA)
		metrics.PipelineDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			h.logger.Error("process event failed", "error", err, "name", evt.InvolvedObject.Name, "namespace", evt.InvolvedObject.Namespace)
			metrics.EventsProcessed.WithLabelValues(classifyOutcome(err)).Inc()
			// Per-PR dedup marks are managed inside Process: rolled back on
			// failure so the PR is re-admitted, or put on a cooldown after a
			// GitHub rate limit so re-fires don't hammer it.
			return
		}
		metrics.EventsProcessed.WithLabelValues("success").Inc()
	}(event, sha)

	w.WriteHeader(http.StatusAccepted)
}

// classifyOutcome maps a processor error to a Prometheus label value. The
// processor wraps errors with `fmt.Errorf("<phase>: %w", ...)` so we look at
// the prefix. Keep this in sync with processor.Process — the test in
// classify_test.go pins each phase to its label.
//
// Rate-limit errors are split out so quota alerts don't false-positive on
// generic GitHub failures (auth / 5xx / DNS) and so dashboards can show
// the abuse-limit cooldown distinctly. The check runs first because a
// rate-limit error can arrive on any GitHub phase (find pr, list files,
// post comment) and the phase prefix would otherwise win.
func classifyOutcome(err error) string {
	if err == nil {
		return "success"
	}
	if bridgegithub.IsRateLimit(err) {
		return "rate_limited"
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "mcp:"),
		strings.HasPrefix(msg, "parse inventory:"),
		strings.HasPrefix(msg, "parse kustomization yaml:"):
		return "mcp_error"
	case strings.HasPrefix(msg, "find pr:"),
		strings.HasPrefix(msg, "post pr comment:"),
		strings.HasPrefix(msg, "remove stale pr comment:"),
		strings.HasPrefix(msg, "resolve repository:"):
		return "github_error"
	case strings.HasPrefix(msg, "readiness check:"):
		return "readiness_error"
	case strings.HasPrefix(msg, "dedupe unavailable:"):
		return "dedupe_error"
	default:
		return "other_error"
	}
}

// Wait blocks until in-flight processing finishes or ctx is done.
func (h *Handler) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops admitting events (later ones get 503) and waits for
// in-flight pipelines like Wait, but when ctx expires first it CANCELS
// them (a pipeline can hold a multi-minute readiness-poll
// budget, far past any sane termination grace period) and waits up to
// unwind for the cancellation to unwind. The unwind matters: the
// processor's deferred cleanup rolls back per-PR dedupe admission marks,
// and with a persistent (Redis) backend an abandoned mark suppresses that
// PR-head's analysis until it expires. ctx's budget plus unwind must fit in
// the pod's termination grace period.
func (h *Handler) Shutdown(ctx context.Context, unwind time.Duration) error {
	// Close admission first: a request still reading its body when the HTTP
	// server's own shutdown gave up must not Add to wg once Wait runs.
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	// Cancel the shutdown signal however the drain ends: detached work it
	// stops (see lifecycle) — e.g. an artifact download whose caller already
	// gave up — isn't tracked by Wait.
	defer h.cancelBase()
	if err := h.Wait(ctx); err == nil {
		return nil
	}
	h.logger.Warn("shutdown budget elapsed; cancelling in-flight pipelines", "unwind", unwind)
	h.cancelBase()
	graceCtx, cancel := context.WithTimeout(context.Background(), unwind)
	defer cancel()
	if err := h.Wait(graceCtx); err != nil {
		return errors.New("in-flight pipelines did not unwind after cancellation")
	}
	return nil
}

// validateHMAC checks an `X-Signature: sha256=<hex>` header against an
// HMAC-SHA256 computed over the raw body using secret. Returns true if the
// signature is present, well-formed, and matches.
func validateHMAC(header string, body []byte, secret []byte) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	want, err := hex.DecodeString(header[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(want, mac.Sum(nil))
}

// commitSHA matches a full git object ID: SHA-1 (40 hex) or SHA-256 (64 hex).
// The SHA ends up in GitHub API paths, so anything else — a short or garbled
// revision, or something like `../../orgs/x` in a forged event — is dropped
// at the door rather than sent to GitHub.
var commitSHA = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

// extractSHA pulls the hex digest out of a Flux revision string. Flux formats
// revisions as `<ref>@<algo>:<hex>` (e.g. `main@sha1:abc123`,
// `main@sha256:def456`); a bare `sha1:<hex>` or `<hex>` is accepted as well.
// Splitting on the LAST `@` first means a ref name that itself contains
// `sha1:` can't confuse the parse, and sha256 repositories are handled the
// same as sha1. The result is not validated; see commitSHA.
func extractSHA(revision string) string {
	rev := strings.TrimSpace(revision)
	if idx := strings.LastIndex(rev, "@"); idx >= 0 {
		rev = rev[idx+1:]
	}
	for _, algo := range []string{"sha1:", "sha256:"} {
		if strings.HasPrefix(rev, algo) {
			return strings.TrimSpace(rev[len(algo):])
		}
	}
	return strings.TrimSpace(rev)
}
