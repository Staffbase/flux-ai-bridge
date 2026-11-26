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

// Package metrics defines all Prometheus instrumentation for the bridge.
//
// Exposed via promhttp.Handler() on /metrics. Counters/histograms/gauges
// are package-level so the rest of the code can record without passing a
// Metrics handle around. Init() must be called once at startup to register
// with the default registry.
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsProcessed records every event that reached a terminal state, labelled
	// by outcome. The handler (and classifyOutcome) writes exactly one of:
	//
	//   admission outcomes (no pipeline ran):
	//     unauthorized     — HMAC validation failed
	//     invalid          — the body couldn't be read (e.g. over 1 MiB) or
	//                        decoded as an event; client gets 400/413
	//     skipped          — event ignored before processing (wrong kind/severity, kustomization filter, etc.)
	//     busy             — event queue full (4× WEBHOOK_MAX_CONCURRENT running
	//                        + waiting); client gets 503. Also an event that
	//                        arrives after shutdown began (503), and a queued
	//                        event abandoned at shutdown
	//
	//   pipeline outcomes (classifyOutcome on the wrapped error):
	//     success          — the pipeline returned no error. Includes no-op
	//                        runs that post nothing: every PR skipped (see
	//                        PRsSkipped), a deferred revision, or an
	//                        unchanged comment
	//     mcp_error        — flux-operator-mcp call, or parsing its answer, failed
	//     readiness_error  — the readiness poll failed (MCP error or cancellation).
	//                        A poll that merely times out is not an error: it
	//                        posts a ⚠️ comment and counts as success.
	//   (LLM failures never fail an event — the kstatus-only comment is posted
	//   instead — so they're counted in llm_fallbacks_total, not here.)
	//     github_error     — PR lookup / comment post / repo resolve failed (non-rate-limit)
	//     rate_limited     — GitHub primary or secondary (abuse) rate limit hit.
	//                        Split out so quota alerts don't false-positive on
	//                        generic GitHub failures; the processor puts the
	//                        undelivered PR marks on a cooldown until the limit
	//                        resets (re-firing only extends it).
	//     dedupe_error     — the Redis/Valkey dedupe backend is unreachable; the
	//                        event is skipped before any GitHub lookup
	//     other_error      — anything not matched by the prefixes above
	//
	// Exactly ONE sample per event, so sum(EventsProcessed) is the canonical
	// total-events query. Per-PR skips inside a pipeline (dedupe hits etc.) are
	// counted in PRsSkipped, never here. Keep this list in sync with
	// internal/webhook/handler.go (the WithLabelValues call sites +
	// classifyOutcome). The classify_test.go suite pins each pipeline-phase
	// prefix to its label.
	EventsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_events_processed_total",
			Help: "Total events that reached terminal state, by outcome.",
		},
		[]string{"outcome"},
	)

	// PRsSkipped counts PRs that a running pipeline decided NOT to analyse,
	// by reason. Per-PR granularity (one event can touch several PRs), which
	// is why these live outside EventsProcessed — an event whose PRs were all
	// skipped still terminates as EventsProcessed{success}. Reasons:
	//
	//     deduped                — this PR-head was already analysed (MarkPRIfNew hit)
	//     dedupe_unhealthy       — dedupe backend errored (Redis down, auth broken);
	//                              the bridge fails closed for this PR to avoid
	//                              uncoordinated duplicate analysis across replicas
	//     reeval_skipped_healthy — TTL re-eval admitted the PR-head but its last
	//                              outcome was healthy and DEDUPE_REEVAL_FAILED_ONLY
	//                              is on, so the re-analysis was skipped
	//     reeval_exhausted       — TTL re-eval admitted the PR-head but it has
	//                              already been re-evaluated DEDUPE_PR_REEVAL_MAX
	//                              times, so its budget is spent
	//     build_scope_unavailable — the Kustomization's exact build file set
	//                              couldn't be resolved from the source artifact
	//                              for this revision, so present-file PRs aren't
	//                              attributed (deletions still are); counted
	//                              once per event
	//     revision_not_applied   — the event's revision is not the one the
	//                              Kustomization has APPLIED (status.lastAppliedRevision
	//                              differs), so analysis is deferred until the
	//                              apply lands and re-fires the event (see the
	//                              revision-applied gate in processor.Process)
	//     self_removal           — every attributed PR deletes the Kustomization's
	//                              own CR and its reconcile is failing/waiting, so
	//                              the readiness poll and LLM are skipped and a
	//                              fixed "being removed" comment is posted;
	//                              counted once per event
	PRsSkipped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_prs_skipped_total",
			Help: "PRs a pipeline run skipped instead of analysing, by reason.",
		},
		[]string{"reason"},
	)

	// EventsFiltered counts events dropped by the Kustomization-name admission
	// filter (config.kustomizations.{include,exclude}). Reason is exactly one
	// of "not_in_include" / "in_exclude". Useful for verifying the filter is
	// doing what the operator expects without grepping DEBUG logs.
	//
	// Filter drops are also counted in EventsProcessed{outcome="skipped"} so
	// sum(EventsProcessed) remains the canonical total-events query — this
	// counter is the per-reason detail view of that subset. Never sum the
	// two metrics: every filter drop appears in both.
	EventsFiltered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_events_filtered_total",
			Help: "Total events dropped by the Kustomization-name include/exclude filter, by reason. Strict subset of EventsProcessed{outcome=\"skipped\"}.",
		},
		[]string{"reason"},
	)

	PipelineDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "flux_ai_bridge_pipeline_duration_seconds",
			Help: "End-to-end pipeline duration (MCP → readiness → LLM → GitHub).",
			// 50ms .. ~410s: the pipeline budget is READINESS_POLL_TIMEOUT (3m
			// default) + 2m, so long runs must not all land in +Inf.
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 14),
		},
	)

	LLMInputTokens = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "flux_ai_bridge_llm_input_tokens_total",
		Help: "Total LLM input tokens (uncached).",
	})

	LLMOutputTokens = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "flux_ai_bridge_llm_output_tokens_total",
		Help: "Total LLM output tokens.",
	})

	LLMMaxTokensHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "flux_ai_bridge_llm_max_tokens_hit_total",
		Help: "Times the LLM response was truncated by hitting MaxTokens (stop_reason=max_tokens).",
	})

	GitHubRateLimitRemaining = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flux_ai_bridge_github_rate_limit_remaining",
		Help: "Most recent X-RateLimit-Remaining value reported by the GitHub API.",
	})

	// GitHubAPIRequests counts EVERY HTTP request the bridge sends to GitHub,
	// measured at the transport layer (below go-github). Unlike summing
	// fetch-cache misses, this captures the full surface: cached AND uncached
	// reads, comment posts/deletes, label writes, pagination follow-ups, and
	// the App installation-token refreshes ghinstallation makes under the
	// hood. The `status` label is the HTTP status code ("200", "403", …) or
	// "error" for transport failures (DNS, connection reset) where no
	// response came back. Divide by an interval for requests/hour to compare
	// against the GitHub rate limit; break down by status to see how much of
	// the budget is going to 403s.
	GitHubAPIRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_github_api_requests_total",
			Help: "Total HTTP requests sent to GitHub, counted at the transport layer, by method and response status.",
		},
		[]string{"method", "status"},
	)

	// GitHubAuthFailures counts 401/403 responses from GitHub that are NOT
	// rate-limit hits (those go to GitHubRateLimitHits instead). Useful for
	// alerting on "GitHub App uninstalled from the org" or "App lost its
	// permissions". Sustained non-zero rate while pipeline events are
	// flowing is the signal.
	GitHubAuthFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "flux_ai_bridge_github_auth_failures_total",
		Help: "Total 401/403 responses from GitHub that are not rate-limit related. Indicates an uninstalled App or missing App permissions.",
	})

	// GitHubRateLimitHits counts GitHub API calls that failed specifically
	// because of primary or secondary (abuse) rate limiting — classified
	// via go-github's typed errors (RateLimitError, AbuseRateLimitError).
	// Separated from GitHubAuthFailures so quota-exhaustion alerts don't
	// false-positive on App-uninstalled, and vice versa.
	GitHubRateLimitHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "flux_ai_bridge_github_rate_limit_hits_total",
		Help: "Total GitHub API calls that hit primary or secondary (abuse) rate limits, classified via go-github typed errors.",
	})

	// LLMRateLimitHits counts LLM API calls that bubbled up a 429 after the
	// SDK's built-in retries (typically 2 retries with backoff) were
	// exhausted. The label distinguishes Anthropic vs OpenAI-compatible
	// endpoints; a sustained non-zero rate on either means you're hitting
	// the provider's quota or TPM/RPM cap.
	LLMRateLimitHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_llm_rate_limit_hits_total",
			Help: "Total LLM API calls that returned 429 after SDK retries were exhausted.",
		},
		[]string{"provider"},
	)

	// LLMRequests counts every LLM completion call the bridge makes, one
	// increment per call after the SDK's internal retries settle. The
	// `provider` label is "claude" or "openai-compatible"; `outcome` is
	// "success" (HTTP 2xx, regardless of downstream content validation),
	// "rate_limited" (bubbled 429 — also counted in LLMRateLimitHits), or
	// "error" (any other failure). This is the call-count companion to the
	// token counters: tokens tell you volume, this tells you how many
	// requests produced it (and how many failed). Divide by an interval for
	// LLM calls/hour.
	LLMRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_llm_requests_total",
			Help: "Total LLM completion calls by provider and outcome (success, rate_limited, error).",
		},
		[]string{"provider", "outcome"},
	)

	// FetchCacheHits / FetchCacheMisses count GitHub-fetch cache outcomes
	// per cache kind. The `kind` label is one of:
	//
	//   - "prs_walk"       — whole PR-resolution result per source SHA (TTL'd)
	//   - "prs_for_commit" — PR-numbers-for-commit (mutable, TTL'd)
	//   - "commit"         — commit parent metadata (immutable)
	//   - "pr_files"       — a PR's changed-file list per head SHA (immutable)
	//   - "blobrefs"       — resource IDs declared by a file blob (immutable)
	//   - "basefileids"    — resource IDs a file declared at the PR base
	//   - "last_comment"   — last bridge comment ID per (PR, K-scope)
	//   - "pr_outcome"     — last outcome written by SetPRLabels per PR;
	//                        a hit means the bridge skipped the labels
	//                        API round-trip because the outcome did not
	//                        change since the previous analysis
	//
	// Hit rate = hits / (hits + misses). Sustained <50% hit rate on the
	// `blobrefs` or `commit` kinds suggests either (a) cache capacity is too
	// small relative to working set (raise the LRU bound or move to
	// Redis-backed) or (b) every event has cache-miss-shaped traffic
	// (deep-walk first event, sparse repeat patterns).
	FetchCacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_fetch_cache_hits_total",
			Help: "GitHub-fetch cache hits per cache kind (prs_walk, prs_for_commit, commit, merge_base, default_branch, pr_files, blobrefs, basefileids, last_comment, pr_outcome).",
		},
		[]string{"kind"},
	)
	FetchCacheMisses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_fetch_cache_misses_total",
			Help: "GitHub-fetch cache misses per cache kind. Misses mean a real GitHub API call was made.",
		},
		[]string{"kind"},
	)

	// LLMFallbacks counts events whose LLM call failed (rate limit, transient
	// network error, auth failure, etc.) so the bridge fell back to a
	// kstatus-only comment. The bridge prefers a partial comment
	// (deterministic kstatus tables, no narrative) over no comment at all so
	// reviewers still see the readiness state during an LLM outage. It counts
	// the fallback decision, before delivery: a fallback comment GitHub then
	// rejects is counted here too, and its event ends as github_error (or
	// rate_limited) in EventsProcessed. Otherwise the event's outcome is still `success` —
	// this counter is the only place an operator can see how often the LLM
	// is silently failing while comments keep landing.
	LLMFallbacks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flux_ai_bridge_llm_fallbacks_total",
			Help: "Total events where the LLM call failed and the bridge fell back to a kstatus-only comment (counted before delivery).",
		},
		[]string{"provider"},
	)

	// MCPCallDuration aggregates latency across all MCP tool calls. The
	// previous {tool} label was the largest single contributor to scrape
	// volume (one full bucket set per tool); the aggregate view is what
	// dashboards and SLO alerts actually plot ("p95 of MCP calls is
	// rising"). For "which tool is slow" drill-down, query the controller
	// logs — every MCP call already logs duration at debug level.
	MCPCallDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "flux_ai_bridge_mcp_call_duration_seconds",
		Help:    "Duration of MCP tool calls, aggregated across all tools.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14), // 5ms .. ~41s
	})

	MCPPoolSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flux_ai_bridge_mcp_pool_size",
		Help: "Current MCP session pool size (live sessions).",
	})

	MCPPoolInUse = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "flux_ai_bridge_mcp_pool_in_use",
		Help: "MCP sessions currently checked out of the pool.",
	})

	// MCPCallRetries counts MCP tool calls retried on a fresh session after a
	// transport-level failure (typically a stale keep-alive connection closed
	// by the server while the pooled session sat idle). A steadily rising
	// value is expected and healthy — it means the retry is absorbing dead
	// connections instead of surfacing them as failed events. A flat zero
	// after deploy would suggest the retry path is never exercised.
	MCPCallRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "flux_ai_bridge_mcp_call_retries_total",
		Help: "MCP tool calls retried on a fresh session after a transport error.",
	})

	// PRsForCommitCoalesced counts FindPRsForCommit resolutions that were
	// served by a concurrent in-flight resolution for the SAME (owner, repo,
	// sha) rather than running their own direct-lookup + merge-parent walk.
	// This is the thundering-herd saving: when a new source revision drops,
	// every Kustomization fires an event at once and all but the first
	// coalesce onto a single walk. A rising value tracks how much duplicate
	// walking (and the GitHub calls behind it) the bridge avoided — it grows
	// with the number of Kustomizations sharing a branch.
	PRsForCommitCoalesced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "flux_ai_bridge_prs_for_commit_coalesced_total",
		Help: "FindPRsForCommit calls served by a concurrent in-flight resolution (single-flight) instead of a fresh walk.",
	})
)

// Init registers all metrics with the default Prometheus registry.
// Idempotent: a second call is a no-op (registering the same collector twice
// would panic in MustRegister).
func Init() {
	initOnce.Do(register)
}

var initOnce sync.Once

func register() {
	prometheus.MustRegister(
		EventsProcessed,
		EventsFiltered,
		PRsSkipped,
		PipelineDuration,
		LLMInputTokens,
		LLMOutputTokens,
		LLMMaxTokensHit,
		LLMRateLimitHits,
		LLMRequests,
		LLMFallbacks,
		FetchCacheHits,
		FetchCacheMisses,
		GitHubAuthFailures,
		GitHubRateLimitHits,
		GitHubRateLimitRemaining,
		GitHubAPIRequests,
		MCPCallDuration,
		MCPPoolSize,
		MCPPoolInUse,
		MCPCallRetries,
		PRsForCommitCoalesced,
	)
}

// Handler returns the HTTP handler that serves /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
