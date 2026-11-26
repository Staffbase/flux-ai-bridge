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

package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/kustomizations"
)

type Config struct {
	AnthropicAPIKey          string
	AnthropicBaseURL         string
	FailureFetchEvents       bool
	FailureFetchFluxInstance bool
	FailureFetchLogs         bool
	FailureFetchMetrics      bool
	GitHubAPIURL             string
	GitHubRepo               string
	// GitHubAPIAllowInsecure permits a plain-HTTP GitHubAPIURL. Without it
	// Load refuses an `http://` URL, which would send the App JWT and the
	// installation tokens unencrypted. Meant for an in-cluster test mock only.
	// Set via GITHUB_API_ALLOW_INSECURE.
	GitHubAPIAllowInsecure bool
	// GitHubKeepCommentHistory disables the default delete-prior-marker
	// step in the commenter. Each analysis is posted as a fresh comment;
	// the PR accumulates a trail. Useful for testing iterations across
	// commits (healthy → broken → healthy) and for audit trails. Default
	// false preserves the "exactly one current comment" idempotency story.
	GitHubKeepCommentHistory bool
	// CommentIncludeSummary controls the headline summary line at the top
	// of the comment body — e.g.
	// "All N inventory resource(s) reached kstatus Current." Default
	// true: the headline gives a one-glance verdict above the tables and
	// (on healthy outcomes) embeds the kstatus README link as a
	// vocabulary pointer. Set false to drop the line and have the body
	// jump straight from the marker to the Resource Status table; the
	// tables alone carry the same data at higher fidelity. Applies with
	// and without an LLM: the bridge writes this line, the model's
	// narrative goes below the tables.
	CommentIncludeSummary bool
	// CommentIncludeFooter controls the trailing
	// "*Analyzed by flux-ai-bridge*" attribution line that `formatComment`
	// appends to every comment. Default true — the attribution line is
	// the canonical marker readers (and grep) use to identify bridge
	// comments at a glance. Set false to strip it; the per-Kustomization
	// `<!-- flux-ai-bridge:<ns>/<name> -->` HTML marker still scopes
	// idempotency unchanged.
	CommentIncludeFooter bool
	// CommentFailedResourcesOnly compacts the `## Resource Status` table for
	// large inventories: only non-`Current` rows are listed (each already shows
	// its namespace in the Namespace column, so no separate `Namespace`-kind row
	// is kept), and when everything is `Current` the table collapses to a
	// one-line "all resources Current" summary. The overall ✅/⚠️/❌ marker and
	// summary still reflect the FULL inventory — only the table rows are filtered.
	// Default false (full table). Set via COMMENT_FAILED_RESOURCES_ONLY. The
	// Workload Status (pod) table is unaffected.
	CommentFailedResourcesOnly bool
	// KustomizationsInclude / KustomizationsExclude are glob lists
	// (path.Match syntax) controlling which Kustomization names are
	// admitted into the bridge pipeline. Both empty = admit all. See
	// internal/kustomizations.Filter for the compose rule.
	KustomizationsInclude []string
	KustomizationsExclude []string
	// RequiredPRLabels is a list of GitHub PR labels. When non-empty, the
	// processor only posts a comment to PRs carrying at least one of these
	// labels — useful for opting individual PRs into review rather than
	// commenting on every PR a Kustomization happens to be part of (autodev
	// batches in particular). Empty (default) disables the filter, matching
	// the previous "comment on every matching PR" behavior.
	RequiredPRLabels []string
	// PRLabelEnabled toggles outcome-driven label reconciliation on each PR
	// the bridge processes (independent of whether a comment was posted — see
	// CommentOnSuccess). When true, the bridge applies exactly one of
	// "flux-success" / "flux-failed" / "flux-warning" (warning =
	// settled-but-not-fully-Current, or readiness poll timed out) and
	// removes the other two. A downstream GitHub Action can gate merges on
	// the presence of "flux-failed" (or the absence of "flux-success").
	// Default false — opt-in feature.
	//
	// PRs touching multiple Kustomizations get an AGGREGATED label: each
	// Kustomization scope's outcome is recorded in the dedupe cache and the
	// worst across scopes wins (failed > warning > success), with a success
	// label held back until every known scope has reported for the current
	// PR head (anti-flap). See processor.aggregatePRLabelOutcome.
	PRLabelEnabled bool
	// PRLabelSuccess / PRLabelFailed / PRLabelWarning are the label names
	// reconciled when PRLabelEnabled is true. Empty falls back to the bridge
	// defaults ("flux-success" / "flux-failed" / "flux-warning"). Set
	// PR_LABEL_SUCCESS / PR_LABEL_FAILED / PR_LABEL_WARNING to match an existing
	// merge-gate convention. Setting PRLabelWarning equal to PRLabelFailed makes
	// a ⚠️ warning apply the failed label (so a gate keyed on it blocks on any
	// non-green outcome); the bridge never removes a label it's also adding, so
	// the shared name doesn't churn.
	PRLabelSuccess string
	PRLabelFailed  string
	PRLabelWarning string
	// PRAttributePrunes lets a pure-deletion PR earn a label by what a reconcile
	// PRUNED, not only by what's currently in status.inventory. A removed file
	// whose declared resource the Kustomization just deleted (read from the
	// controller's prune event: `Kind/namespace/name deleted`) attributes the PR
	// even though the resource is gone from the inventory — so a clean teardown
	// reads flux-success. Also covers removing a whole Kustomization (the parent
	// that prunes the child CR claims it). Default true; the failure floor is
	// safe — a missed prune event just leaves the PR unlabelled, never wrong.
	PRAttributePrunes bool

	// CommentOnSuccess controls whether a comment is posted when the outcome
	// is healthy (everything Current, settled). Default true (comment on every
	// outcome). Set false to suppress the "all green" comment — warnings and
	// failures still get a comment. This is independent of PRLabelEnabled: with
	// labels on and this off, a healthy PR gets a `flux-success` label but no
	// comment (label carries the status, comment noise is dropped). When a
	// success comment is suppressed but the PR already has a bridge comment
	// from an earlier failing run, that stale comment is removed (cache-guarded,
	// so a healthy-from-start PR pays no extra GitHub call). Set via
	// COMMENT_ON_SUCCESS.
	CommentOnSuccess bool
	LLMMaxTokens     int
	LLMModel         string
	LLMProvider      string
	// LLMSkipOnHealthy short-circuits the LLM call when the cluster is
	// fully green: every inventory row reached `Current`, every pod (when
	// included) reached `Current`, and the readiness poll settled cleanly.
	// Comment shape stays byte-identical to the LLM-driven happy-path
	// output — the no-LLM kstatus body fills in the same `## Resource
	// Status` table. The LLM is still called for any non-green state
	// (failures, in-progress rows, poll timeouts) so analysis quality is
	// unchanged where it matters.
	//
	// Default true. Set false when you've customized `SKILL.md` to add
	// team-specific text on healthy comments (runbook links, team tags)
	// that the kstatus-only body wouldn't carry, or simply want every
	// reconcile annotated by the model.
	LLMSkipOnHealthy bool
	MCPURL           string
	OpenAIAPIKey     string
	OpenAIBaseURL    string
	WebhookPort      int

	// ReadinessPollInterval is how often the processor re-checks the
	// Kustomization's inventory after an event arrives. kustomize-controller
	// fires `ReconciliationSucceeded` the moment kubectl-apply returns, well
	// before the underlying workload has actually rolled — polling lets the
	// bridge analyze the *settled* state instead of an in-flight rollout.
	ReadinessPollInterval time.Duration
	// DedupePRReEvalTTL is the per-PR-head re-evaluation floor. The bridge
	// analyses a PR once per its own head commit (so unrelated advances of
	// the Flux source branch don't re-trigger analysis). A positive value
	// lets that admission record expire after the window, so a later event
	// re-admits the same PR-head and the bridge re-checks current cluster
	// state — catching runtime drift (e.g. a pod that starts crashlooping
	// minutes after a green deploy) without a new commit. The comment layer
	// is idempotent, so a re-eval that finds identical state touches nothing.
	// Zero (default) disables re-evaluation: each PR-head is analysed exactly
	// once. A positive value must cover one analysis (ProcessingTimeout), or a
	// slow failed analysis could release a newer analysis's mark; Load
	// rejects shorter values. Set via DEDUPE_PR_REEVAL_TTL (e.g. "30m").
	DedupePRReEvalTTL time.Duration
	// DedupeReEvalFailedOnly narrows re-evaluation (when DedupePRReEvalTTL > 0)
	// to PR-heads whose LAST analysis was NOT healthy. A previously-green deploy
	// is left alone on re-eval — skipping its readiness poll and MCP traffic —
	// while a failed/warning one is re-checked so a fix (or a worsening) is
	// picked up. Default true: re-eval exists to track failures resolving, and
	// re-polling healthy deploys is pure cost. Set DEDUPE_REEVAL_FAILED_ONLY=
	// false to re-evaluate every attributed PR-head regardless of prior outcome
	// (catches a healthy deploy that drifts unhealthy without a new commit, at
	// the price of re-polling everything). No effect when re-eval is disabled.
	DedupeReEvalFailedOnly bool
	// DedupePRReEvalMax caps how many TTL re-evaluations a single PR-head gets
	// (when DedupePRReEvalTTL > 0). After the initial analysis plus this many
	// re-evals, further re-admissions of the same head are skipped — bounding
	// the per-PR cost of a deploy that stays broken forever (each re-eval is a
	// readiness poll + LLM call + comment refresh per window). A new commit
	// (new head SHA) always starts with a fresh budget. The counter rides the
	// same best-effort side-channel as the last-outcome records: on the
	// Redis/Valkey backend it self-cleans (ttl) after a head goes quiet for two
	// windows, and a head re-fired after that gets one fresh analysis and a new
	// budget; the in-memory backend ignores the ttl for these records and
	// bounds them by LRU capacity instead, so there a spent budget persists
	// until eviction or restart. Zero (default) = unlimited re-evals. Set via
	// DEDUPE_PR_REEVAL_MAX (e.g. "8"). No effect when re-eval is disabled.
	DedupePRReEvalMax int
	// DedupePersistenceMaxTTL is the backstop expiry for Redis/Valkey records
	// that would otherwise never expire: admission marks when re-eval is off,
	// outcome/label records, and the fetch cache's immutable entries (blobs,
	// per-head file lists, last-comment IDs). Without it the keyspace grows for
	// every PR-head ever seen until Redis runs out of memory — with
	// `noeviction` or a `volatile-*` policy, keys without an expiry can't be
	// evicted, writes fail, and the bridge (failing closed) goes silent. The
	// cost: a PR-head still re-fired by Flux after this long is analysed once
	// more. Default 720h (30 days); 0 disables the backstop. No effect on the
	// in-memory backend. Set via DEDUPE_PERSISTENCE_MAX_TTL.
	DedupePersistenceMaxTTL time.Duration
	// ShutdownTimeout is the total time the bridge takes to shut down on
	// SIGTERM: in-flight pipelines get it minus a fixed unwind reserve to
	// finish, then they are cancelled and the reserve lets their rollbacks
	// (releasing dedupe marks of PRs that weren't delivered) run. Keep it
	// below the pod's terminationGracePeriodSeconds, or the kubelet's
	// SIGKILL cuts the rollback short and those PR-heads stay deduped until
	// their mark expires. Default 30s. Set via SHUTDOWN_TIMEOUT.
	ShutdownTimeout time.Duration
	// InstanceID distinguishes bridges in different clusters that report on
	// the same repositories. It is written into every comment marker and
	// into the Redis key prefixes, so each instance keeps its own comment per
	// Kustomization and its own dedupe/cache state. A DNS label (lowercase
	// alphanumerics and '-', at most 63 characters). Empty (default) keeps
	// the plain marker and prefixes. Set via INSTANCE_ID.
	InstanceID string
	// ReadinessPollTimeout caps how long Process waits for every inventory
	// row to reach a terminal kstatus (Current, Failed, Terminating).
	// NotFound is re-polled until the timeout (a cold MCP cache can return
	// it transiently). Anything still not terminal when the timeout hits is
	// analyzed as-is.
	ReadinessPollTimeout time.Duration
	// ReadinessExpandHelmReleases, when true, walks every HelmRelease in
	// the Kustomization's inventory and fetches its own
	// `.status.inventory.entries` via MCP. Those child resources are
	// folded into the readiness-check refs so they're evaluated by kstatus
	// alongside the Kustomization-level entries. Without this, a HelmRelease
	// inventory row shows opaque controller summaries like "generation N
	// vs observed N-1" — the actual Deployments/Services it released are
	// invisible. One level only: nested HelmReleases (a HelmRelease whose
	// chart contains another HelmRelease CR) are not recursively expanded.
	//
	// Default true. Set false to skip the extra MCP fetches (1 per
	// HelmRelease) when you don't need that level of detail.
	ReadinessExpandHelmReleases bool

	// ReadinessIncludePods, when true, takes a kstatus snapshot of pods in
	// the Kustomization's OWN namespace (spec.targetNamespace, else its
	// name) and renders a separate "## Workload Status" section in the
	// comment. Useful when a controller (e.g. HelmRelease) reports its
	// inventory row as healthy but the underlying pods are crashing —
	// kstatus's pod check sees the failure immediately. Pods aren't part of
	// the inventory readiness poll loop (they're inherently volatile); they
	// get their own settle wait on the remaining shared budget AFTER the
	// inventory has settled (see waitPodsSettled).
	//
	// Default true. Set false in environments where the namespace contains
	// pods owned by other Kustomizations (we don't filter by ownership) and
	// the noise outweighs the diagnostic value.
	ReadinessIncludePods bool
	// ReadinessPodQuietPeriod is how long the scanned namespace must stay
	// quiet (no workload pod appearing or shutting down) before settled pods
	// count as settled, so the bridge doesn't report healthy before an
	// operator has created its pods. Defaults to 60s, or half a poll timeout
	// under 2m. Must be shorter than ReadinessPollTimeout; 0 disables. Rules:
	// waitPodsSettled and
	// docs/configuration.md (READINESS_POD_QUIET_PERIOD).
	ReadinessPodQuietPeriod time.Duration
}

// processingSlack is what one event's pipeline gets on top of the readiness
// poll: failure-context fetches, the LLM call and comment delivery.
const processingSlack = 2 * time.Minute

// processingTimeout is the per-event pipeline budget for a readiness poll
// timeout. A single derivation keeps the webhook deadline and the re-eval TTL
// floor in step.
func processingTimeout(pollTimeout time.Duration) time.Duration {
	return pollTimeout + processingSlack
}

// ProcessingTimeout caps one event's full pipeline (readiness poll plus
// processingSlack). main.go hands it to the webhook handler.
func (c *Config) ProcessingTimeout() time.Duration {
	return processingTimeout(c.ReadinessPollTimeout)
}

// instanceIDPattern is a DNS label: safe inside an HTML comment marker and a
// Redis key.
var instanceIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func Load() (*Config, error) {
	port, err := intFromEnv("WEBHOOK_PORT", 8080)
	if err != nil {
		return nil, err
	}
	maxTokens, err := intFromEnv("LLM_MAX_TOKENS", 2048)
	if err != nil {
		return nil, err
	}
	pollInterval, err := durationFromEnv("READINESS_POLL_INTERVAL", 5*time.Second)
	if err != nil {
		return nil, err
	}
	pollTimeout, err := durationFromEnv("READINESS_POLL_TIMEOUT", 3*time.Minute)
	if err != nil {
		return nil, err
	}
	prReEvalTTL, err := durationFromEnv("DEDUPE_PR_REEVAL_TTL", 0)
	if err != nil {
		return nil, err
	}
	// Reject values the consumers can't honour instead of letting each one
	// improvise: a non-positive interval turns the pod-settle wait into a
	// tight MCP scan loop, a non-positive timeout shrinks the pipeline budget
	// below the poll itself, and a negative re-eval TTL makes every Redis
	// admission write fail (`ERR invalid expire time`) while the in-memory
	// backend silently treats it as 0.
	if pollInterval <= 0 {
		return nil, fmt.Errorf("READINESS_POLL_INTERVAL must be > 0, got %s", pollInterval)
	}
	if pollTimeout <= 0 {
		return nil, fmt.Errorf("READINESS_POLL_TIMEOUT must be > 0, got %s", pollTimeout)
	}
	if prReEvalTTL < 0 {
		return nil, fmt.Errorf("DEDUPE_PR_REEVAL_TTL must be >= 0, got %s", prReEvalTTL)
	}
	// A re-eval window shorter than one analysis lets a slow failed analysis
	// roll back the admission mark that a newer analysis of the same PR-head
	// took after the window expired. 0 (never re-evaluate) has no window.
	if floor := processingTimeout(pollTimeout); prReEvalTTL > 0 && prReEvalTTL < floor {
		return nil, fmt.Errorf("DEDUPE_PR_REEVAL_TTL (%s) must be 0 or at least READINESS_POLL_TIMEOUT (%s) + %s = %s, the time one analysis may take", prReEvalTTL, pollTimeout, processingSlack, floor)
	}
	// The quiet window comes out of the poll budget: one that doesn't fit
	// would end every healthy analysis unsettled (⚠️). The default shrinks to
	// half a short poll timeout, so a deployment that never set the variable
	// keeps starting; only an explicit value that doesn't fit is rejected.
	podQuietPeriod, err := durationFromEnv("READINESS_POD_QUIET_PERIOD", min(time.Minute, pollTimeout/2))
	if err != nil {
		return nil, err
	}
	if podQuietPeriod < 0 {
		return nil, fmt.Errorf("READINESS_POD_QUIET_PERIOD must be >= 0, got %s", podQuietPeriod)
	}
	if podQuietPeriod >= pollTimeout {
		return nil, fmt.Errorf("READINESS_POD_QUIET_PERIOD (%s) must be shorter than READINESS_POLL_TIMEOUT (%s)", podQuietPeriod, pollTimeout)
	}
	prReEvalMax, err := intFromEnv("DEDUPE_PR_REEVAL_MAX", 0)
	if err != nil {
		return nil, err
	}
	if prReEvalMax < 0 {
		return nil, fmt.Errorf("DEDUPE_PR_REEVAL_MAX must be >= 0, got %d", prReEvalMax)
	}
	persistenceMaxTTL, err := durationFromEnv("DEDUPE_PERSISTENCE_MAX_TTL", 30*24*time.Hour)
	if err != nil {
		return nil, err
	}
	if persistenceMaxTTL < 0 {
		return nil, fmt.Errorf("DEDUPE_PERSISTENCE_MAX_TTL must be >= 0, got %s", persistenceMaxTTL)
	}
	shutdownTimeout, err := durationFromEnv("SHUTDOWN_TIMEOUT", 30*time.Second)
	if err != nil {
		return nil, err
	}
	if shutdownTimeout <= 0 {
		return nil, fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0, got %s", shutdownTimeout)
	}

	// boolEnv accumulates parse errors so the struct literal below stays
	// readable; the first error is returned right after construction.
	var boolErr error
	boolEnv := func(key string, fallback bool) bool {
		v, err := boolFromEnv(key, fallback)
		if err != nil && boolErr == nil {
			boolErr = err
		}
		return v
	}

	cfg := &Config{
		WebhookPort:                 port,
		MCPURL:                      strings.TrimSpace(os.Getenv("MCP_URL")),
		LLMProvider:                 strings.TrimSpace(os.Getenv("LLM_PROVIDER")),
		AnthropicAPIKey:             strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")),
		AnthropicBaseURL:            strings.TrimSpace(os.Getenv("ANTHROPIC_BASE_URL")),
		OpenAIAPIKey:                strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		OpenAIBaseURL:               strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")),
		LLMModel:                    strings.TrimSpace(os.Getenv("LLM_MODEL")),
		LLMMaxTokens:                maxTokens,
		LLMSkipOnHealthy:            boolEnv("LLM_SKIP_ON_HEALTHY", true),
		GitHubAPIURL:                strings.TrimSpace(os.Getenv("GITHUB_API_URL")),
		GitHubRepo:                  strings.TrimSpace(os.Getenv("GITHUB_REPO")),
		GitHubAPIAllowInsecure:      boolEnv("GITHUB_API_ALLOW_INSECURE", false),
		GitHubKeepCommentHistory:    boolEnv("GITHUB_KEEP_COMMENT_HISTORY", false),
		CommentIncludeSummary:       boolEnv("COMMENT_INCLUDE_SUMMARY", true),
		CommentIncludeFooter:        boolEnv("COMMENT_INCLUDE_FOOTER", true),
		CommentFailedResourcesOnly:  boolEnv("COMMENT_FAILED_RESOURCES_ONLY", false),
		FailureFetchLogs:            boolEnv("FAILURE_FETCH_LOGS", true),
		FailureFetchEvents:          boolEnv("FAILURE_FETCH_EVENTS", true),
		FailureFetchMetrics:         boolEnv("FAILURE_FETCH_METRICS", false),
		FailureFetchFluxInstance:    boolEnv("FAILURE_FETCH_FLUX_INSTANCE", false),
		ReadinessPollInterval:       pollInterval,
		DedupePRReEvalTTL:           prReEvalTTL,
		DedupeReEvalFailedOnly:      boolEnv("DEDUPE_REEVAL_FAILED_ONLY", true),
		DedupePRReEvalMax:           prReEvalMax,
		DedupePersistenceMaxTTL:     persistenceMaxTTL,
		ShutdownTimeout:             shutdownTimeout,
		InstanceID:                  strings.TrimSpace(os.Getenv("INSTANCE_ID")),
		ReadinessPollTimeout:        pollTimeout,
		ReadinessIncludePods:        boolEnv("READINESS_INCLUDE_PODS", true),
		ReadinessPodQuietPeriod:     podQuietPeriod,
		ReadinessExpandHelmReleases: boolEnv("READINESS_EXPAND_HELMRELEASES", true),
		KustomizationsInclude:       csvFromEnv("KUSTOMIZATIONS_INCLUDE"),
		KustomizationsExclude:       csvFromEnv("KUSTOMIZATIONS_EXCLUDE"),
		RequiredPRLabels:            csvFromEnv("PR_REQUIRED_LABELS"),
		PRLabelEnabled:              boolEnv("PR_LABEL_ENABLED", false),
		PRLabelSuccess:              strings.TrimSpace(os.Getenv("PR_LABEL_SUCCESS")),
		PRLabelFailed:               strings.TrimSpace(os.Getenv("PR_LABEL_FAILED")),
		PRLabelWarning:              strings.TrimSpace(os.Getenv("PR_LABEL_WARNING")),
		PRAttributePrunes:           boolEnv("PR_ATTRIBUTE_PRUNES", true),
		CommentOnSuccess:            boolEnv("COMMENT_ON_SUCCESS", true),
	}
	if boolErr != nil {
		return nil, boolErr
	}
	if strings.HasPrefix(strings.ToLower(cfg.GitHubAPIURL), "http://") && !cfg.GitHubAPIAllowInsecure {
		return nil, fmt.Errorf("GITHUB_API_URL %q uses plain HTTP, which sends the GitHub App credentials unencrypted; use https:// or set GITHUB_API_ALLOW_INSECURE=true (test mocks only)", cfg.GitHubAPIURL)
	}
	if cfg.InstanceID != "" && !instanceIDPattern.MatchString(cfg.InstanceID) {
		return nil, fmt.Errorf("INSTANCE_ID %q must be a DNS label: lowercase letters, digits and '-', at most 63 characters, starting and ending alphanumeric", cfg.InstanceID)
	}

	// Fail-fast on malformed glob patterns: a silent "no match" on a typo
	// (e.g. unclosed character class) would silently disable a filter the
	// operator thought was active. Validate the patterns here so the bridge
	// exits non-zero at boot rather than at the first event.
	if _, err := kustomizations.New(cfg.KustomizationsInclude, cfg.KustomizationsExclude); err != nil {
		return nil, fmt.Errorf("kustomizations filter: %w", err)
	}

	if cfg.MCPURL == "" {
		return nil, fmt.Errorf("MCP_URL is required")
	}
	if cfg.LLMProvider == "" {
		return nil, fmt.Errorf("LLM_PROVIDER is required")
	}
	// GitHub auth is App-only. PAT support was removed; deployments must
	// configure all three GITHUB_APP_* env vars (consumed directly by
	// internal/github/commenter.AuthFromEnv).
	if !githubAppConfigured() {
		return nil, fmt.Errorf("GitHub App config required: set GITHUB_APP_ID + GITHUB_APP_INSTALLATION_ID + GITHUB_APP_PRIVATE_KEY (or GITHUB_APP_PRIVATE_KEY_FILE)")
	}

	switch cfg.LLMProvider {
	case "claude":
		if cfg.AnthropicAPIKey == "" {
			return nil, fmt.Errorf("ANTHROPIC_API_KEY is required when LLM_PROVIDER=claude")
		}
		if cfg.LLMModel == "" {
			cfg.LLMModel = "claude-sonnet-4-6"
		}
	case "openai-compatible":
		// OPENAI_API_KEY is intentionally NOT required: unauthenticated
		// endpoints (a bundled/self-hosted Ollama, vLLM, etc.) accept calls
		// with no key, and the OpenAI client tolerates an empty key (it just
		// omits the auth header). The trade-off is that a misconfigured
		// authenticated endpoint (e.g. the GitHub Models default below) with a
		// missing key fails at the first LLM call with a 401 instead of at
		// startup — surfaced via the LLMFallbacks metric + an ERROR log, with
		// the deterministic kstatus comment still posted.
		if cfg.OpenAIBaseURL == "" {
			cfg.OpenAIBaseURL = "https://models.inference.ai.azure.com"
		}
		if cfg.LLMModel == "" {
			cfg.LLMModel = "gpt-4.1"
		}
	case "none":
		// LLM disabled: the processor builds the PR comment directly from the
		// kstatus output. No API key or model required, and the failure-context
		// fetchers (logs/events/metrics) are skipped — there is nothing to feed
		// them into.
	default:
		return nil, fmt.Errorf("unsupported LLM_PROVIDER %q (supported: claude, openai-compatible, none)", cfg.LLMProvider)
	}

	return cfg, nil
}

// boolFromEnv parses 1/true/yes/on as true and 0/false/no/off as false
// (case-insensitive). Unset → fallback. Anything else is an error — a typo
// like "flase" silently flipping behavior to the default is exactly the
// fail-late failure mode the int/duration parsers already reject.
func boolFromEnv(key string, fallback bool) (bool, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch raw {
	case "":
		return fallback, nil
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return fallback, fmt.Errorf("invalid %s: %q (use true/false, 1/0, yes/no, or on/off)", key, raw)
}

func intFromEnv(key string, defaultValue int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return value, nil
}

// githubAppConfigured returns true when the operator has supplied the
// three required GitHub App env vars (or the file variant of the private
// key). The bridge's commenter layer (internal/github/commenter.go)
// auto-selects App mode when these are present.
func githubAppConfigured() bool {
	id := strings.TrimSpace(os.Getenv("GITHUB_APP_ID"))
	inst := strings.TrimSpace(os.Getenv("GITHUB_APP_INSTALLATION_ID"))
	pem := strings.TrimSpace(os.Getenv("GITHUB_APP_PRIVATE_KEY"))
	pemFile := strings.TrimSpace(os.Getenv("GITHUB_APP_PRIVATE_KEY_FILE"))
	return id != "" && inst != "" && (pem != "" || pemFile != "")
}

// csvFromEnv splits a comma-separated env var into trimmed, non-empty entries.
// Used for list-valued config (e.g. KUSTOMIZATIONS_INCLUDE). Commas don't
// appear in path.Match globs so no escaping is needed; an unset var yields
// a nil slice (which the filter treats as "match all").
func csvFromEnv(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func durationFromEnv(key string, defaultValue time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return d, nil
}
