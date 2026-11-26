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

package processor

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/lifecycle"
	"github.com/staffbase/flux-ai-bridge/internal/llm"
	"github.com/staffbase/flux-ai-bridge/internal/markdown"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
	"github.com/staffbase/flux-ai-bridge/internal/skills"
	bridgesource "github.com/staffbase/flux-ai-bridge/internal/source"
	"gopkg.in/yaml.v3"
)

type MCPClient interface {
	GetFluxInstance(ctx context.Context) (string, error)
	// Namespace-wide events are fetched via ListKubernetesResources(kind=Event)
	// so we can filter by per-event timestamps and reportingComponent
	// (core-vs-noise classification) client-side; the old text-formatted
	// GetKubernetesEvents helper was removed for that path. GetKubernetesEvents
	// below is the newer, precisely-scoped `get_kubernetes_events` MCP tool
	// (flux-operator-mcp v0.59.0+), used only as a supplementary per-failing-
	// workload fetch — see fetchFailureContext.
	GetKubernetesEvents(ctx context.Context, kind, name, namespace string, since time.Duration, limit int) (events string, truncated bool, err error)
	GetKubernetesLogs(ctx context.Context, podName, namespace, container string, previous bool) (string, error)
	GetKubernetesMetrics(ctx context.Context, namespace string) (string, error)
	GetKubernetesResource(ctx context.Context, apiVersion, kind, name, namespace string) ([]byte, error)
	GetKustomizationStatus(ctx context.Context, name, namespace string) ([]byte, error)
	ListKubernetesResources(ctx context.Context, apiVersion, kind, namespace string) ([]byte, error)
}

type Commenter interface {
	FindPRsForCommit(ctx context.Context, owner, repo, sha string) ([]bridgegithub.PRInfo, error)
	// ListPRFiles returns a PR's changed files. headSHA (the PR's head
	// commit) keys the cache: the file set is immutable per head, so passing
	// it lets the commenter serve repeat calls without a GitHub round-trip.
	ListPRFiles(ctx context.Context, owner, repo string, prNumber int, headSHA string) ([]bridgegithub.PRFile, error)
	// BlobResourceIDs returns the inventory IDs (`<ns>_<name>_<group>_<Kind>`)
	// of the Kubernetes resources DECLARED in a PR file blob. The commenter
	// fetches the blob, parses it with the injected ResourceIDsFromYAML, and
	// caches the resulting IDs — never the raw manifest bytes — so Redis holds
	// only resource coordinates, not (potentially sensitive) file contents.
	BlobResourceIDs(ctx context.Context, owner, repo, sha string) ([]string, error)
	// BaseFileResourceIDs returns the inventory IDs a file declares at ref (a
	// PR's base commit) — its state BEFORE the PR. Diffed against the head-blob
	// IDs to detect an in-place resource removal (a manifest commented out, or a
	// YAML doc deleted while the file persists) that BlobResourceIDs over the
	// head blob can't see.
	BaseFileResourceIDs(ctx context.Context, owner, repo, path, ref string) ([]string, error)
	PostOrUpdateComment(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int, body string) error
	PostOrUpdateCommentKeyed(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int, body, stabilityKey string) error
	CommentCurrent(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int, stabilityKey string) bool
	// RemoveComment deletes the bridge's prior comment for a scope if one is
	// recorded (cache-guarded; no-op + no API call when none is known). Used to
	// clear a stale comment when posting is suppressed on a now-healthy PR.
	RemoveComment(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int) error
	SetPRLabels(ctx context.Context, owner, repo string, prNumber int, headSHA string, outcome bridgegithub.Outcome) error
}

type Processor struct {
	analyzer  llm.Analyzer
	cfg       *config.Config
	checker   *readiness.Checker
	commenter Commenter
	dedupe    *dedupe.Cache // nil-safe; per-PR-head admission (MarkPRIfNew/ForgetPR)
	logger    *slog.Logger
	mcp       MCPClient
	scope     ScopeResolver // nil-safe; exact PR-attribution scope from the source artifact
	skills    *skills.Loader
}

// ScopeResolver resolves the set of repo files that feed a Kustomization's
// kustomize build, from the source-controller artifact at the event's revision.
// A PR file attributes to the Kustomization only if it's in that set. Returns a
// non-nil error (e.g. source.ErrRevisionMismatch) when the exact scope can't be
// determined for this revision, signalling the processor to skip-and-retry.
type ScopeResolver interface {
	BuildFiles(ctx context.Context, ref bridgesource.Ref, specPath, revisionSHA string) (map[string]struct{}, error)
}

// New constructs a Processor. All cluster reads go through MCP — the bridge
// no longer holds a Kubernetes client. The dedupe cache is owned here now
// (the webhook handler no longer dedups): the processor admits each PR once
// per its own head commit via MarkPRIfNew. main.go also shares the cache's
// Redis client with the commenter's fetchcache.
//
// Blob caching used to live here as a per-process LRU; it's now owned by
// the commenter's fetchcache, which can be backed by Redis/Valkey for
// cross-replica sharing and survives bridge restarts.
func New(
	cfg *config.Config,
	mcpClient MCPClient,
	checker *readiness.Checker,
	analyzer llm.Analyzer,
	commenter Commenter,
	skillLoader *skills.Loader,
	dedupeCache *dedupe.Cache,
	logger *slog.Logger,
) *Processor {
	return &Processor{
		analyzer:  analyzer,
		cfg:       cfg,
		checker:   checker,
		commenter: commenter,
		dedupe:    dedupeCache,
		logger:    logger,
		mcp:       mcpClient,
		skills:    skillLoader,
	}
}

// WithScopeResolver enables exact, artifact-based PR attribution. When set, the
// processor resolves each Kustomization's build file set from the
// source-controller artifact and attributes a PR only if a changed file is in
// that set (see ScopeResolver). Left unset, attribution falls back to the prior
// inventory-identity behaviour. Returns the receiver for chaining.
func (p *Processor) WithScopeResolver(s ScopeResolver) *Processor {
	p.scope = s
	return p
}

func (p *Processor) Process(ctx context.Context, event eventv1.Event, sha string) error {
	p.logger.Info("processing flux event", "name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace, "sha", sha)

	p.logger.Debug("calling mcp", "name", event.InvolvedObject.Name)
	kustomizationYAML, err := p.mcp.GetKustomizationStatus(ctx, event.InvolvedObject.Name, event.InvolvedObject.Namespace)
	if err != nil {
		return fmt.Errorf("mcp: %w", err)
	}
	p.logger.Debug("mcp returned", "bytes", len(kustomizationYAML))

	var kspec kustomizationSpec
	if err := yaml.Unmarshal(kustomizationYAML, &kspec); err != nil {
		return fmt.Errorf("parse kustomization yaml: %w", err)
	}

	// Classify the controller's own verdict up front. It guards the
	// revision-applied gate below (a failing/waiting reconcile must NOT be
	// deferred — see there) and is reused for the outcome further down.
	reconcileFailedMsg, reconcileWaitingMsg := reconcileFailureMessage(event, kspec, sha)

	// Revision-applied gate. Only analyse once the Kustomization has APPLIED the
	// revision this event is about. Until status.lastAppliedRevision matches the
	// analysed revision, the inventory/pod snapshot still reflects the
	// PREVIOUSLY-running state, so a green readout would describe the OLD deploy —
	// not the change under review. (Seen in practice: a Flux self-upgrade
	// got flux-success ~11min before the controllers actually rolled, because the
	// apply was gated on a dependsOn target; the snapshot showed the old, healthy
	// controllers.) We DEFER rather than warn: when the apply lands, the
	// controller emits its own ReconciliationSucceeded for that revision, which
	// re-triggers analysis — so the readiness settle poll only ever counts down
	// against the applied revision, and the deferred head isn't frozen (no green
	// was recorded). Only a CONFIDENT mismatch defers (both revisions present and
	// different); a blank/unparseable applied revision falls through unchanged.
	//
	// The gate ONLY applies to the success path. A failing reconcile
	// (severity=error / Ready=False) or a dependency wait (DependencyNotReady)
	// inherently leaves lastAppliedRevision behind the analysed revision — the
	// apply never landed — yet those are exactly the ❌/⚠️ states the bridge must
	// report, not swallow. So when the controller is already signalling a
	// failure/wait we skip the gate and let analysis surface that verdict.
	//
	// Deferral requires a CONFIDENT mismatch (revisionMismatch): both revisions
	// must yield a non-empty SHA token AND differ. A blank or unparseable
	// lastAppliedRevision (e.g. one ending in `:`, which extracts to an empty
	// token) is NOT a mismatch — it falls through to analysis rather than
	// deferring forever on a value that can never "match".
	reconcileHealthy := reconcileFailedMsg == "" && reconcileWaitingMsg == ""
	if reconcileHealthy && revisionMismatch(kspec.Status.LastAppliedRevision, sha) {
		p.logger.Info("analysed revision not yet applied; deferring until the Kustomization applies it",
			"name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace,
			"sha", sha, "applied_revision", kspec.Status.LastAppliedRevision)
		metrics.PRsSkipped.WithLabelValues("revision_not_applied").Inc()
		return nil
	}

	owner, repo, err := p.resolveRepository(ctx, event, kspec)
	if err != nil {
		return fmt.Errorf("resolve repository: %w", err)
	}

	// Parse the Kustomization's inventory up front — both PR attribution
	// (does any PR file declare a resource this Kustomization manages?)
	// and the readiness checker need the same list, and we already have
	// the YAML in hand. The inventory is the ONLY signal PR attribution
	// uses: a PR is relevant to this Kustomization iff at least one of
	// its changed files declares a resource in this inventory.
	refs, err := inventoryRefsFromYAML(kustomizationYAML)
	if err != nil {
		return fmt.Errorf("parse inventory: %w", err)
	}
	p.logger.Debug("inventory parsed", "refs", len(refs))
	inventorySet := make(map[string]struct{}, len(refs))
	for _, r := range refs {
		inventorySet[r] = struct{}{}
	}

	// PR-side filters run BEFORE the LLM call. Stages:
	//   1) Drop closed/merged PRs (cache-hygiene Forget afterwards).
	//   2) Apply the global PR_REQUIRED_LABELS gate.
	//   3) Build-scope gate: keep only changed files that are part of THIS
	//      Kustomization's kustomize build (resolved from the source artifact).
	//   4) Inventory-membership: a build-scoped file that declares a resource in
	//      this Kustomization's inventory (or is itself a build input) attributes.
	// If nothing survives, return without the readiness poll + LLM cost.
	// Autodev batches typically produce ONE matching PR per Kustomization
	// out of N candidates; without this short-circuit the bridge would
	// run the LLM and then post nothing.
	//
	// A Redis-backed dedupe that just failed fails closed — every PR below would
	// be skipped as dedupe_unhealthy — so don't spend GitHub rate limit
	// resolving PRs (up to a full parent walk) for a result we'd discard.
	// Available only costs a PING after a failed admission write.
	if !p.dedupe.Available() {
		return errors.New("dedupe unavailable: admission backend unreachable; skipping GitHub PR lookups until it recovers")
	}
	prs, err := p.commenter.FindPRsForCommit(ctx, owner, repo, sha)
	if err != nil {
		return fmt.Errorf("find pr: %w", err)
	}
	p.logger.Debug("pr lookup done", "count", len(prs))

	// Exact attribution scope. Resolve, from the source-controller artifact Flux
	// actually built, the set of repo files that feed THIS Kustomization's build
	// (its overlay at spec.path plus the bases it includes). A PRESENT-file PR is
	// attributed here only if a changed file is in that set — so a same-named
	// resource in a sibling overlay can't be mis-attributed.
	//
	// If the scope can't be resolved for THIS revision (revision mismatch because
	// the source advanced and GC'd the artifact, a fetch error, or an
	// unresolvable kustomize graph), we set scopeFailed: present-file attribution
	// is suppressed (no overlay-blind fallback — a later reconcile at the matching
	// revision retries), while DELETION PRs still flow through the prune paths,
	// which don't depend on the build scope. We never wholesale-skip the event,
	// so a deletion at an advanced revision isn't lost.
	//
	// nil scope resolver (not configured, e.g. unit tests) leaves buildFiles nil
	// and scopeFailed false, which disables the gate and preserves the prior
	// identity-only behaviour.
	var buildFiles map[string]struct{}
	scopeFailed := false
	if p.scope != nil && len(prs) > 0 {
		ref := bridgesource.Ref{
			Kind:      kspec.Spec.SourceRef.Kind,
			Name:      kspec.Spec.SourceRef.Name,
			Namespace: kspec.Spec.SourceRef.Namespace,
		}
		// sourceRef.kind is optional in Flux and defaults to GitRepository
		// (mirrors resolveRepository); without this an implicit-kind Kustomization
		// would fail scope resolution and suppress all present-file attribution.
		if ref.Kind == "" {
			ref.Kind = "GitRepository"
		}
		if ref.Namespace == "" {
			ref.Namespace = event.InvolvedObject.Namespace
		}
		bf, serr := p.scope.BuildFiles(ctx, ref, kspec.Spec.Path, sha)
		if serr != nil {
			// Couldn't resolve the exact build scope for this revision (source
			// advanced + GC'd the matching artifact, a fetch error, or an
			// unresolvable kustomize graph). We do NOT attribute PRESENT-file PRs
			// without it — there is no overlay-blind fallback, and a later
			// reconcile at the matching revision re-drives them. DELETION PRs are
			// unaffected: their `removed` files don't use the build scope, so they
			// are still attributed from the cluster's prune state below.
			p.logger.Info("build scope unavailable for this revision; present-file attribution suppressed (deletions still processed)",
				"name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace,
				"sha", sha, "path", kspec.Spec.Path, "error", serr.Error())
			metrics.PRsSkipped.WithLabelValues("build_scope_unavailable").Inc()
			scopeFailed = true
		} else {
			buildFiles = bf
		}
	}

	// A no-match verdict is only allowed to ERASE state (stale comment, label
	// scope) when the inventory it was computed against is trustworthy. A
	// failing reconcile's inventory is not: an apply failure leaves resources
	// the PR adds out of it, a post-revert prune drops them, and mid-reconcile
	// the status can be transiently empty — the exact moments the bridge would
	// otherwise erase its own failure evidence (observed in practice:
	// flux-failed added on the failure event, then comment + label wiped by the
	// retry event ~3 minutes later). Cleanup waits for a non-error event with a
	// non-empty inventory; matching itself is unaffected.
	inventoryTrusted := event.Severity != eventv1.EventSeverityError && len(inventorySet) > 0
	// A teardown-shaped failure may come from a PR that only edits this
	// Kustomization's CR out of its parent's manifests — a file outside this
	// build that no other attribution path matches. Resolve the parent's build
	// path once so filterPRs can attribute exactly that (see prEditsOutSelf).
	selfRemovalPath := ""
	if len(prs) > 0 && teardownFailure(reconcileFailedMsg, reconcileWaitingMsg) {
		selfRemovalPath = p.parentBuildPath(ctx, kspec)
	}
	targets, finalized, prunedToVerify, selfRemovalOnly, err := p.filterPRs(ctx, owner, repo, event.InvolvedObject.Namespace, event.InvolvedObject.Name, inventorySet, buildFiles, scopeFailed, inventoryTrusted, selfRemovalPath, prs)
	if err != nil {
		// Surface rate-limit + other unrecoverable GitHub errors so the
		// webhook handler can classify them (rate_limited outcome). No PR
		// is marked yet, so there is nothing to roll back.
		return fmt.Errorf("find pr: %w", err)
	}
	if len(targets) == 0 {
		// No targets means either (a) every candidate PR is closed/merged,
		// or (b) every open PR was filter-dropped by labels/inventory.
		// Dedup is now per-PR-head and either TTL-expiring or naturally
		// irrelevant once a PR finalises (it stops appearing in candidates),
		// so there is no source-SHA entry to evict here anymore.
		p.logger.Info("no open PR matched filters; skipping analysis",
			"owner", owner, "repo", repo, "sha", sha,
			"candidate_prs", len(prs), "finalized", finalized,
			"required_labels", p.cfg.RequiredPRLabels, "inventory_size", len(refs))
		return nil
	}

	// Per-PR-head dedup. A PR is analysed once per ITS OWN head commit, so
	// the comment isn't churned every time the Flux source revision advances
	// underneath an unchanged PR. Narrow targets to those not yet analysed at
	// their current head (or past the DedupePRReEvalTTL floor, which re-admits
	// them so runtime drift on a static PR is still caught). If none survive,
	// we skip the readiness poll + LLM entirely.
	var toPost []bridgegithub.PRInfo
	var marked []bridgegithub.PRInfo
	// piggyback collects PRs whose re-eval budget is spent: they never trigger
	// an analysis, but when a sibling PR does, the already-built body is
	// delivered to them too (see the DEDUPE_PR_REEVAL_MAX branch below).
	var piggyback []bridgegithub.PRInfo
	// wasReEval marks analysis-triggering admissions that are TTL re-evals of
	// an already-analysed head — the only ones the re-eval budget meters, and
	// only when their analysis concludes failed/warning.
	wasReEval := map[int]bool{}
	// admissionErr keeps the last dedupe backend error, so an event that
	// discovers an outage reports it instead of a no-op success.
	var admissionErr error
	for _, pr := range targets {
		isNew, derr := p.dedupe.MarkPRIfNew(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA, p.cfg.DedupePRReEvalTTL)
		if derr != nil {
			admissionErr = derr
			// Dedup backend unhealthy: fail closed for THIS PR (skip) rather
			// than risk uncoordinated duplicate comments across replicas.
			p.logger.Error("dedupe unhealthy; skipping PR to avoid uncoordinated duplicate analysis",
				"owner", owner, "repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA, "error", derr.Error())
			metrics.PRsSkipped.WithLabelValues("dedupe_unhealthy").Inc()
			continue
		}
		if !isNew {
			p.logger.Info("PR already analysed at this head; skipping",
				"owner", owner, "repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA)
			metrics.PRsSkipped.WithLabelValues("deduped").Inc()
			continue
		}
		// Both the failed-only skip and the re-eval cap below key off the
		// head's last-outcome record; fetch it once per admitted head so the
		// Redis backend pays a single GET even when both knobs are on.
		var lastOutcome string
		var outcomeSeen bool
		if p.cfg.DedupePRReEvalTTL > 0 && (p.cfg.DedupeReEvalFailedOnly || p.cfg.DedupePRReEvalMax > 0) {
			lastOutcome, outcomeSeen = p.dedupe.LastPROutcome(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA)
		}
		// Re-eval-failed-only (default when re-eval is on): an isNew admission
		// here is either a fresh PR-head or a TTL re-eval. If we analysed THIS
		// head before AND it was healthy, the head is FROZEN — skipped until a
		// new commit (a new head SHA is a different dedupe key with no success
		// record, so it always re-analyses). Nothing re-wakes a green head: not
		// an error event, not runtime drift. In the autodev model many open PRs
		// share a Kustomization, and re-waking green heads on ANY sibling
		// failure flipped unrelated green PRs to ❌ as the shared Kustomization
		// flapped (a broken policy and an unrelated pod failure). The
		// deliberate trade-off: genuine drift on a green deploy — a pod that
		// crashes minutes after a green analysis — is NOT reflected until the
		// PR gets a new commit. (Collateral damage from the PR's OWN change is
		// still caught: the FIRST analysis is whole-inventory and ungated, so a
		// PR only reaches green if every dependent was healthy then.) A
		// previously failed/warning head (or a never-seen head, no record)
		// falls through and is analysed. The admission mark MarkPRIfNew just
		// set stays (we don't add to `marked`), so it expires after the floor
		// and the next event re-checks → re-skips while the head stays green.
		if p.cfg.DedupeReEvalFailedOnly && p.cfg.DedupePRReEvalTTL > 0 &&
			outcomeSeen && lastOutcome == string(bridgegithub.OutcomeSuccess) {
			p.logger.Info("re-eval skipped: PR-head previously healthy, frozen until a new commit (DEDUPE_REEVAL_FAILED_ONLY)",
				"owner", owner, "repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA)
			metrics.PRsSkipped.WithLabelValues("reeval_skipped_healthy").Inc()
			p.dedupe.RecordPROutcome(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA, lastOutcome, p.outcomeRecordTTL(lastOutcome))
			continue
		}
		// Re-eval cap: the budget meters FAILING analyses only. An admitted head
		// that already has an outcome record is a TTL re-eval (a never-seen
		// head has none — the same heuristic the failed-only skip above uses);
		// its analysis is counted AFTER it runs, and only when the outcome is
		// failed/warning (see the delivery loop) — a healthy analysis is the
		// good news the re-eval exists for and burns nothing. Here we only
		// PEEK: once the count reaches DEDUPE_PR_REEVAL_MAX the head's budget
		// is spent and it stops triggering analyses, bounding the per-PR cost
		// of a deploy that never recovers (readiness poll + LLM + comment
		// churn every window, forever). The exhausted-window increment below
		// keeps the counter's TTL refreshed while the failing streak lasts; on
		// the Redis backend the counter self-cleans (ttl) once the head goes
		// quiet or healthy, after which a later drift gets a fresh analysis
		// and a new budget. The in-memory backend is LRU-bounded instead, so
		// there a spent budget persists until eviction or restart. A new
		// commit (new head SHA) always starts at zero.
		if p.cfg.DedupePRReEvalMax > 0 && p.cfg.DedupePRReEvalTTL > 0 && outcomeSeen {
			if n := p.dedupe.PRReEvalCount(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA); n >= p.cfg.DedupePRReEvalMax {
				p.logger.Info("re-eval budget spent (DEDUPE_PR_REEVAL_MAX); PR won't trigger analysis but piggybacks on siblings",
					"owner", owner, "repo", repo, "pr", pr.Number, "head_sha", pr.HeadSHA,
					"reeval_count", n, "max", p.cfg.DedupePRReEvalMax)
				metrics.PRsSkipped.WithLabelValues("reeval_exhausted").Inc()
				// Refresh the counter (its TTL) so exhaustion holds while the
				// failing streak keeps firing events, and the outcome record so
				// re-eval detection keeps working.
				p.dedupe.IncrPRReEval(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA, p.reEvalRecordTTL())
				p.dedupe.RecordPROutcome(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA, lastOutcome, p.outcomeRecordTTL(lastOutcome))
				// The cap bounds ANALYSIS cost (readiness poll + LLM), not
				// delivery: when ANOTHER PR on this Kustomization still has
				// budget and triggers the analysis below, the body is built
				// anyway — withholding it from this PR would freeze its
				// comment on a stale verdict while a sibling PR shows the
				// current one (observed with two PRs sharing a failing
				// Kustomization). Piggyback PRs never justify an analysis on
				// their own and cost only the (stability-keyed, often no-op)
				// delivery.
				piggyback = append(piggyback, pr)
				// Piggyback PRs join the rollback set: their delivery happens
				// at the end of THIS run, so a pipeline error before it must
				// forget the admission mark like any undelivered PR — else the
				// standing mark blocks re-admission until the TTL expires and
				// the PR misses the sibling's retry, staying stale a full
				// extra window. On success the delivery loop sets keepMark
				// exactly as for analysis-triggering PRs.
				marked = append(marked, pr)
				continue
			}
			// This analysis-triggering admission is a re-eval; remember it so
			// the delivery loop can count it against the budget if the
			// analysis concludes failed/warning.
			wasReEval[pr.Number] = true
		}
		toPost = append(toPost, pr)
		marked = append(marked, pr)
	}
	if len(toPost) == 0 {
		if admissionErr != nil {
			// Nothing ran because the dedupe backend failed, not because every
			// PR was already handled — report it (outcome dedupe_error), like
			// the upfront Available() gate does for later events.
			return fmt.Errorf("dedupe unavailable: PR admission failed: %w", admissionErr)
		}
		p.logger.Debug("no PRs require analysis after per-PR dedup; skipping readiness+LLM",
			"owner", owner, "repo", repo, "candidate_targets", len(targets), "piggyback_only", len(piggyback))
		return nil
	}

	// Roll back any mark we committed but didn't successfully deliver — on a
	// pipeline error, a per-PR post failure, or a panic unwinding through
	// here — so the next event re-admits the PR instead of silently skipping
	// it. `keepMark[pr]` is set for PRs we delivered (including idempotent
	// no-op posts). When delivery hit a GitHub rate limit, the undelivered
	// marks are neither kept nor released but put on a cooldown until the
	// limit resets: releasing them would let the next Flux re-fire hammer
	// GitHub straight back into a longer abuse cooldown, keeping them would
	// leave those PR-heads without a comment until a new commit.
	keepMark := make(map[int]bool, len(toPost))
	var rateLimitCooldown time.Duration
	defer func() {
		for _, pr := range marked {
			switch {
			case keepMark[pr.Number]:
			case rateLimitCooldown > 0:
				p.dedupe.ExpirePR(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA, rateLimitCooldown)
			default:
				p.dedupe.ForgetPR(event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, pr.HeadSHA)
			}
		}
	}()

	// Self-removal: every PR here deletes this Kustomization's own CR, so its
	// parent deletes it and prunes what it manages. Until then it reconciles a
	// revision without its files and fails ("kustomization path not found") or
	// waits on a dependsOn target the PR removed too. That is the deletion
	// working, not a broken deploy: report it as such, without the readiness
	// poll (the inventory is about to be pruned) or the LLM. Reported ✅ because
	// nothing re-reports this scope once the Kustomization is gone — a ❌ or ⚠️
	// would stay on the PR and hold its label for good. Recorded as
	// outcomeRecordSelfRemoval, not success, so while the Kustomization still
	// exists its re-evals keep re-checking (see selfRemovalExpected) instead of
	// freezing the head green.
	if teardownFailure(reconcileFailedMsg, reconcileWaitingMsg) {
		parent, removing, serr := p.selfRemovalExpected(ctx, owner, repo, event, kspec, append(append([]bridgegithub.PRInfo{}, toPost...), piggyback...))
		if serr != nil {
			// Only a rate limit gets here: cool the admission marks down like
			// delivery does, so the next event doesn't hit GitHub again at once.
			rateLimitCooldown = bridgegithub.RateLimitCooldown(serr)
			return fmt.Errorf("self-removal check: %w", serr)
		}
		if len(removing) > 0 {
			p.logger.Info("PR deletes this Kustomization; reconcile failure is expected teardown",
				"name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace,
				"sha", sha, "parent", parent, "prs", len(removing), "reconcile_failure", cmp.Or(reconcileFailedMsg, reconcileWaitingMsg))
			metrics.PRsSkipped.WithLabelValues("self_removal").Inc()
			body := formatComment(buildSelfRemovalBody(event.InvolvedObject.Name, parent, reconcileFailedMsg, reconcileWaitingMsg, deletesResourcesOnDeletion(kspec)), p.cfg.CommentIncludeFooter, "")
			rateLimitCooldown, err = p.deliver(ctx, prDelivery{
				owner: owner, repo: repo,
				ns: event.InvolvedObject.Namespace, name: event.InvolvedObject.Name,
				body: body, stabilityKey: body, outcome: bridgegithub.OutcomeSuccess,
				record: outcomeRecordSelfRemoval, retireScope: true, suppress: !p.cfg.CommentOnSuccess, wasReEval: wasReEval,
			}, append(onlyPRs(toPost, removing), onlyPRs(piggyback, removing)...), keepMark)
			if err != nil {
				return err
			}
			// The batch's other PRs get the normal report below.
			toPost = withoutPRs(toPost, removing)
			piggyback = withoutPRs(piggyback, removing)
		}
		// PRs admitted only for this check don't get the normal report: their
		// match was a hint, not attribution. Their admission marks roll back
		// (no keepMark), so a later event re-checks them.
		toPost = withoutPRs(toPost, selfRemovalOnly)
		piggyback = withoutPRs(piggyback, selfRemovalOnly)
		if len(toPost) == 0 {
			// Nothing left that justifies an analysis (piggyback PRs never do
			// on their own).
			return nil
		}
	}

	// Poll the WHOLE Kustomization inventory until every resource reaches a
	// terminal kstatus, re-expanding HelmRelease inventories on EACH iteration
	// (a HelmRelease's status.history[].inventory[] only populates after
	// helm-controller observes it; expanding per iteration picks up the
	// Deployment/Service/etc. children the moment they appear).
	//
	// We deliberately evaluate the ENTIRE deploy unit, not just the resources a
	// PR changed: a change can break OTHER resources the Kustomization manages
	// (a DB migration failing dependent pods, a server-side-apply conflict, a
	// prune), and scoping the report to the diff would hide exactly those. The
	// comment is therefore the same for every PR that touched this Kustomization
	// — built once below.
	//
	// One shared settle deadline covers this poll AND the pod-settle wait below
	// — a freshly-rolled-out workload (e.g. a HelmRelease image bump) shouldn't
	// be analysed mid-rollout; whatever the inventory poll doesn't consume, the
	// pod wait inherits.
	pollTimeout := settleBudget(ctx, p.cfg.ReadinessPollTimeout)
	if pollTimeout < p.cfg.ReadinessPollTimeout {
		p.logger.Info("readiness poll shortened to leave time for analysis and delivery",
			"name", event.InvolvedObject.Name, "namespace", event.InvolvedObject.Namespace,
			"timeout", pollTimeout, "configured", p.cfg.ReadinessPollTimeout)
	}
	settleDeadline := time.Now().Add(pollTimeout)
	// The pod quiet period counts from here, so the inventory poll's time
	// already counts toward it (see waitPodsSettled).
	analysisStart := time.Now()
	readinessRefs := readinessRefsFor(refs, event.InvolvedObject.Namespace, event.InvolvedObject.Name)
	// pendingHelmReleases is overwritten on every poll iteration by the
	// closure below (WaitUntilSettledFunc calls refsFunc synchronously, never
	// concurrently) — so once the loop exits, it reflects the LAST observed
	// state: whichever HelmReleases still had no visible children at settle
	// time (or at timeout). Surfaced in the comment below.
	var pendingHelmReleases []pendingHelmRelease
	resourceStatuses, settled, err := p.checker.WaitUntilSettledFunc(ctx, func() []string {
		expanded, pending := p.expandHelmReleaseInventories(ctx, readinessRefs)
		pendingHelmReleases = pending
		return expanded
	}, p.cfg.ReadinessPollInterval, pollTimeout)
	if err != nil {
		return fmt.Errorf("readiness check: %w", err)
	}
	if !settled {
		p.logger.Warn("readiness poll timed out before inventory settled, analysing partial state",
			"timeout", pollTimeout, "count", len(resourceStatuses))
	}
	p.logger.Debug("readiness check done", "count", len(resourceStatuses), "settled", settled)

	// A PR that DELETES a resource leaves it listed in the inventory snapshot
	// taken at event time until Flux's prune completes — polling it then reads
	// NotFound, which the marker rules count as ❌. An intentional deletion is
	// not a failure: re-read the inventory once and drop NotFound rows whose
	// entry is gone from the CURRENT inventory (pruned). Rows still listed yet
	// NotFound stay failures — those are genuinely missing.
	helmReleasesBefore := helmReleaseRows(resourceStatuses)
	resourceStatuses = p.dropPrunedRows(ctx, event, refs, resourceStatuses)
	// A HelmRelease the PR deleted was pending only because its children are
	// gone with it; listing it as "could not be inspected" would turn a clean
	// deletion ⚠️. Drop pending entries whose row was just pruned.
	pendingHelmReleases = withoutPrunedHelmReleases(pendingHelmReleases, helmReleasesBefore, resourceStatuses)
	// NotFound is deliberately NON-terminal for the poll (see
	// readiness.allSettled), so pruned rows held the poll open until timeout
	// and left settled=false. With them filtered out, re-evaluate: if every
	// remaining row is terminal, the only unsettlement WAS the prunes and the
	// run must not read ⚠️ "poll timed out". A genuinely-missing NotFound row
	// keeps settled=false — the same ambiguity stance the poll itself takes.
	if !settled && readiness.AllSettled(resourceStatuses) {
		settled = true
		p.logger.Debug("settled recomputed after pruned-row drop: remaining rows all terminal",
			"count", len(resourceStatuses))
	}

	// Wait for PODS to settle too, on the remaining shared budget, confined to
	// the Kustomization's OWN namespace — its spec.targetNamespace if set, else
	// its name (the namespace==name convention where Kustomization "grafana" deploys into
	// the "grafana" namespace). We deliberately do NOT sweep every inventory
	// namespace: a broad Kustomization owns resources (dashboards, alert-rules)
	// in 100+ namespaces but only RUNS its workload in one, so sweeping the lot
	// would dump the whole cluster's unrelated pods.
	//
	// waitPodsSettled re-scans until every pod reaches a terminal kstatus (a
	// Deployment can read Current while its just-created pod is still "Running,
	// containers not all ready" mid-rollout): a starting pod is waited out →
	// Current; a genuinely failing workload pod (isFailingPod → Failed) ends
	// the wait at once; a pod stuck InProgress is reported at the deadline.
	// Otherwise settled pods wait out the pod quiet period, so pods an
	// operator creates after the apply are judged too.
	podNamespace := kspec.Spec.TargetNamespace
	if podNamespace == "" {
		podNamespace = event.InvolvedObject.Name
	}
	podStatuses, podsScanned, podsSettled := p.waitPodsSettled(ctx, []string{podNamespace}, p.cfg.ReadinessPollInterval, time.Until(settleDeadline), analysisStart)
	p.logger.Debug("pod kstatus snapshot", "count", len(podStatuses), "scanned", podsScanned, "settled", podsSettled)
	// Fold the pod result into the overall settled flag: an unsettled pod at the
	// deadline is a timeout just like an unsettled inventory row.
	settled = settled && podsSettled
	// CronJob runs whose pods are already cleaned up are judged by their Job
	// object's conditions instead — a stable verdict that doesn't flap with
	// pod-history pruning (see cronRunFallbackRows). Only when the pod scan
	// SUCCEEDED: "no pod for the newest run" is evidence of cleanup only if we
	// got a conclusive look; after a failed scan the run's pods may simply be
	// invisible, and this stays a gap-closer, not a replacement for the scan.
	// Appended after the pod rows; both Failed and Current are terminal, so
	// settledness is untouched.
	if podsScanned {
		fallback := p.cronRunFallbackRows(ctx, podNamespace, podStatuses)
		podStatuses = append(dropOlderCronRunPods(podStatuses, fallback), fallback...)
	}

	// The inventory/pod snapshot reflects what's RUNNING — the last
	// successfully-applied state, not necessarily the analysed revision. When
	// the Kustomization's own reconcile is failing (dry-run/build/apply), the
	// change never reached the cluster and an all-Current snapshot describes
	// the OLD state — fold the controller's own verdict into the outcome so a
	// rejected deploy can't read ✅ (see reconcileFailureMessage). A dependency
	// WAIT (DependencyNotReady) is a self-resolving ordering state → ⚠️, never ❌.
	// (reconcileFailedMsg / reconcileWaitingMsg were classified up front, before
	// the revision-applied gate.)

	// Finalizer-stuck teardown: a PR attributed by prune is judged on the
	// teardown actually COMPLETING, not just on a Ready Kustomization. Re-check
	// each pruned resource the attributed PR(s) removed — gone (NotFound) is the
	// success we want and contributes no row, but a resource lingering in
	// Terminating (a finalizer blocking deletion) is folded in as a non-Current
	// row so the verdict reads ⚠️ instead of a premature ✅. One bounded pass, no
	// poll: a stuck object stays stuck, and a racy still-Current read self-heals
	// on the next reconcile event rather than flapping a warning here.
	if len(prunedToVerify) > 0 {
		if rows := p.verifyTeardownComplete(ctx, prunedToVerify); len(rows) > 0 {
			resourceStatuses = append(resourceStatuses, rows...)
			settled = false
		}
	}

	outcome := computeOutcome(resourceStatuses, podStatuses, settled, reconcileFailedMsg != "", reconcileWaitingMsg != "", len(pendingHelmReleases) > 0)
	// A ⚠️ that is only an unfinished rollout has no failure to diagnose yet:
	// the LLM is told so explicitly and isn't forced to produce a Root Cause,
	// which it would otherwise invent from routine pod-replacement events.
	inProgress := rolloutInProgress(outcome, resourceStatuses, podStatuses, podNamespace, podsScanned, reconcileWaitingMsg != "", len(pendingHelmReleases) > 0)

	// Static auto-retry policy line for non-green comments (empty on green / when
	// re-eval is disabled). Computed once here and folded into both formatComment
	// calls below so it's part of the stabilityKey — it refreshes a comment only
	// when the outcome crosses the success boundary, never on a same-state retry.
	// Rendered independently of COMMENT_INCLUDE_FOOTER (operational info, not
	// branding).
	retryNote := retryPolicyNote(p.cfg, outcome)

	// Comment policy: suppress the comment on a fully-healthy outcome when
	// COMMENT_ON_SUCCESS is off. Computed up front so a suppressed healthy event
	// also skips the LLM + body build entirely (a cost win on top of
	// LLM_SKIP_ON_HEALTHY). Labels still reconcile per PR below.
	suppress := outcome == bridgegithub.OutcomeSuccess && !p.cfg.CommentOnSuccess

	// Build the comment body ONCE — it reflects the whole Kustomization's status
	// and is identical for every PR that touched this Kustomization. Skipped
	// entirely when suppressing a healthy comment.
	var analysis string
	// stabilityKey is the deterministic kstatus body; commentCurrent marks PRs
	// whose delivered comment already describes this exact state (see below).
	var stabilityKey string
	// llmDegraded: the LLM call failed and the comment went out kstatus-only.
	llmDegraded := false
	commentCurrent := map[int]bool{}
	if !suppress {
		// Decide whether to call the LLM. Two paths bypass it:
		//   1. LLM_PROVIDER=none → analyzer is nil at construction time.
		//   2. LLM_SKIP_ON_HEALTHY=true AND fully green (every inventory row +
		//      every pod reached `Current`, settled poll) → skip the paid
		//      round-trip on what would be a near-identical "all green" comment.
		// A failing pod (even with all-Current inventory) keeps the LLM in.
		// Keyed off `outcome` (already computed above) rather than
		// re-deriving the same conditions inline: it's the single source of
		// truth for "is this actually green", and already folds in
		// helmReleasesPending — a pending HelmRelease must keep the LLM in
		// the loop exactly like any other non-success outcome, not just
		// change the comment text after the fact.
		useLLM := p.analyzer != nil
		if useLLM && p.cfg.LLMSkipOnHealthy && outcome == bridgegithub.OutcomeSuccess {
			useLLM = false
			p.logger.Info("LLM call skipped: all inventory + pods Current and settled",
				"inventory", len(resourceStatuses), "pods", len(podStatuses))
		}

		// Deterministic body first (marker + optional summary + Resource Status
		// + Workload Status); the LLM narrative, if any, is appended below it.
		// pendingHelmReleases (see expandHelmReleaseInventories) both forces
		// at least a warning outcome and appends its own caveat note — a
		// HelmRelease with no visible children yet can otherwise leave the
		// tables looking like a small, complete, all-green release when
		// they're actually blind to whatever the chart renders.
		analysis = buildKstatusBody(resourceStatuses, podStatuses, podsScanned, settled, p.cfg.CommentIncludeSummary, p.cfg.CommentFailedResourcesOnly, event.InvolvedObject.Name, reconcileFailedMsg, reconcileWaitingMsg, pendingHelmReleases)
		// The deterministic body is the comment's STABILITY KEY: the LLM
		// narrative below is non-deterministic (the same broken state gets
		// re-worded on every re-eval window), and keying the repost decision on
		// the full body made every re-eval delete+recreate the comment —
		// notification churn for identical state, and a visible no-comment gap
		// when a pod died mid-swap. PRs whose recorded comment already carries
		// this key are skipped entirely; if EVERY target is current, the LLM
		// round-trip is skipped too (the prose on the PR still describes this
		// exact state). The key is the FORMATTED deterministic output — the
		// same header/footer wrapping the delivered comment carries — so
		// deterministic presentation changes (toggling COMMENT_INCLUDE_FOOTER,
		// future wrapper changes) alter the key and refresh existing comments
		// instead of matching their embedded key forever.
		stabilityKey = formatComment(analysis, p.cfg.CommentIncludeFooter, retryNote)
		allCommentsCurrent := true
		for _, pr := range toPost {
			if p.commenter.CommentCurrent(ctx, owner, repo, event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, stabilityKey) {
				commentCurrent[pr.Number] = true
			} else {
				allCommentsCurrent = false
			}
		}
		// Piggyback PRs vote on the LLM skip too: their delivery shares this
		// body, and a stale piggyback comment (a delivery missed to a
		// transient GitHub error) healed with a kstatus-only body would be
		// keyed CURRENT for this state — permanently dropping the narrative
		// until the state changes. Letting it block the skip costs at most
		// one healing LLM call: the analysis already ran, and once delivered
		// the comment is current and skips again next window.
		for _, pr := range piggyback {
			if p.commenter.CommentCurrent(ctx, owner, repo, event.InvolvedObject.Namespace, event.InvolvedObject.Name, pr.Number, stabilityKey) {
				commentCurrent[pr.Number] = true
			} else {
				allCommentsCurrent = false
			}
		}
		if useLLM && allCommentsCurrent && len(toPost) > 0 {
			useLLM = false
			p.logger.Info("LLM call skipped: cluster state unchanged since the delivered comment(s)",
				"name", event.InvolvedObject.Name, "prs", len(toPost))
		}
		if useLLM {
			skillPrefix := p.skills.Prompt()
			// Strip the (potentially huge) status.inventory enumeration from the
			// Kustomization YAML before it goes into the prompt — it's token
			// noise with no root-cause signal (the readiness tables already
			// carry per-resource status).
			promptYAML := promptKustomizationYAML(string(kustomizationYAML), refs)
			userPayload := buildPrompt(event, sha, promptYAML, resourceStatuses, podStatuses, settled)
			if len(pendingHelmReleases) > 0 {
				// Without this, the model only sees the same all-Current
				// snapshot buildKstatusBody does and can describe the run as
				// fully healthy, contradicting the deterministic warning
				// marker/note it renders alongside.
				userPayload = userPayload + "\n\n" + pendingHelmReleaseNote(pendingHelmReleases)
			}
			extra, hardFailure := p.fetchFailureContext(ctx, event, resourceStatuses, podStatuses)
			// The failure sweep lists pods and events again; a failing pod or
			// a FailedCreate the readiness snapshot couldn't see revokes the
			// rollout exemption.
			if hardFailure {
				inProgress = false
			}
			if inProgress {
				userPayload = userPayload + "\n\n" + rolloutInProgressNote
			}
			if extra != "" {
				userPayload = userPayload + "\n\n" + extra
				p.logger.Debug("failure context appended", "extra_bytes", len(extra))
			}
			// Full prompt at debug level — useful for e2e diffing what the LLM saw.
			p.logger.Debug("llm prompt",
				"skill_prefix_bytes", len(skillPrefix),
				"user_payload_bytes", len(userPayload),
				"skill_prefix", skillPrefix,
				"user_payload", userPayload)
			llmOut, lerr := p.analyzer.Analyze(ctx, skillPrefix, userPayload)
			if lerr != nil {
				// LLM failure is non-fatal: the deterministic kstatus body is
				// already built and carries the readiness signal. Posting
				// kstatus-only beats posting nothing. ERROR level so a sustained
				// LLM outage is visible; LLMFallbacks lets operators alert on it
				// independently of comments still landing.
				p.logger.Error("llm analyze failed; falling back to kstatus-only comment",
					"provider", p.cfg.LLMProvider,
					"name", event.InvolvedObject.Name,
					"namespace", event.InvolvedObject.Namespace,
					"sha", sha,
					"error", lerr.Error())
				metrics.LLMFallbacks.WithLabelValues(p.cfg.LLMProvider).Inc()
				analysis = strings.TrimRight(analysis, " \t\r\n") +
					"\n\n*LLM analysis unavailable; see bridge logs for details.*"
				// A degraded (kstatus-only) comment must NOT claim the normal
				// stability key: keyed identically to a full comment, an
				// unchanged state would report CommentCurrent on every later
				// re-eval — skipping the LLM retry AND the repost — so the
				// "analysis unavailable" comment would outlive the outage
				// indefinitely. A distinct degraded key makes the next re-eval
				// miss the currency check, retry the LLM, and swap in the full
				// comment once the provider recovers; consecutive outage
				// windows produce the SAME degraded key, so a sustained outage
				// still doesn't churn the comment.
				stabilityKey += "\n<llm-degraded>"
				llmDegraded = true
			} else {
				p.logger.Debug("llm returned", "bytes", len(llmOut), "skill_prefix_bytes", len(skillPrefix), "response", llmOut)
				// Append the LLM narrative BELOW the deterministic tables; strip
				// any status marker it emitted (the bridge owns the marker).
				_, llmRest := extractStatusMarker(llmOut)
				llmRest = stripBoxDecoration(strings.TrimSpace(llmRest))
				// SKILL contract enforcement: any non-healthy run must carry
				// `## Root Cause` and `## Next steps to fix`. Models drift —
				// observed in production as a failure analysis collapsed into
				// the `## Kustomization Status` paragraph with no actionable
				// next steps. One corrective retry; if the model still won't
				// comply, post what we got (an imperfect analysis beats none)
				// and leave a WARN trail. A rollout still in progress is exempt:
				// the SKILL tells the model to omit both sections there.
				if llmRest != "" && outcome != bridgegithub.OutcomeSuccess && !inProgress && llmMissingFailureSections(llmRest) {
					p.logger.Warn("llm reply missing required failure sections; retrying once",
						"provider", p.cfg.LLMProvider, "name", event.InvolvedObject.Name)
					retryPayload := userPayload + "\n\n## Correction required\n" +
						"This run is NOT fully healthy, but your previous reply omitted the mandatory failure-only sections. " +
						"Produce your full reply again, this time including BOTH `## Root Cause` (quote the proving evidence) " +
						"and `## Next steps to fix` (numbered, concrete changes to the PR)."
					if retryOut, rerr := p.analyzer.Analyze(ctx, skillPrefix, retryPayload); rerr == nil {
						if _, retryRest := extractStatusMarker(retryOut); strings.TrimSpace(retryRest) != "" {
							llmRest = stripBoxDecoration(strings.TrimSpace(retryRest))
						}
					}
					if llmMissingFailureSections(llmRest) {
						p.logger.Warn("llm reply still missing required failure sections after retry; posting as-is",
							"provider", p.cfg.LLMProvider, "name", event.InvolvedObject.Name)
					}
				}
				// The reverse direction: on a rollout still in progress there is no
				// failure to diagnose, yet models ignore the SKILL and write a
				// Root Cause anyway — observed in production, naming the routine
				// probe failures and volume re-attach of pods being replaced.
				// Drop both sections deterministically.
				if inProgress {
					if stripped := stripFailureSections(llmRest); stripped != llmRest {
						p.logger.Info("llm reply carried failure sections on a rollout in progress; removed",
							"provider", p.cfg.LLMProvider, "name", event.InvolvedObject.Name)
						llmRest = stripped
					}
				}
				if llmRest != "" {
					analysis = strings.TrimRight(analysis, " \t\r\n") + "\n\n" + markdown.CloseOpenFence(neutralizeMentions(llmRest))
				}
			}
		} else if p.analyzer == nil {
			p.logger.Debug("llm disabled; built kstatus-only comment",
				"bytes", len(analysis), "settled", settled,
				"rows", len(resourceStatuses), "pods", len(podStatuses))
		}
		// Stamp the deterministic header (and optionally footer) so the shape
		// stays byte-identical across runs and models.
		analysis = formatComment(analysis, p.cfg.CommentIncludeFooter, retryNote)
	}

	// Deliver to every target PR — including budget-exhausted piggyback PRs,
	// whose delivery rides on the analysis the others paid for. The body is
	// the same whole-Kustomization status; labels reconcile per PR regardless
	// of suppression.
	rateLimitCooldown, err = p.deliver(ctx, prDelivery{
		owner: owner, repo: repo,
		ns: event.InvolvedObject.Namespace, name: event.InvolvedObject.Name,
		body: analysis, stabilityKey: stabilityKey, outcome: outcome,
		suppress: suppress, commentCurrent: commentCurrent, wasReEval: wasReEval,
		llmDegraded: llmDegraded, resources: len(resourceStatuses),
	}, append(append([]bridgegithub.PRInfo{}, toPost...), piggyback...), keepMark)
	return err
}

// prDelivery is one event's verdict, delivered identically to every target PR
// of a Kustomization scope.
type prDelivery struct {
	owner, repo  string
	ns, name     string // the Kustomization scope
	body         string
	stabilityKey string
	outcome      bridgegithub.Outcome
	// suppress: healthy with COMMENT_ON_SUCCESS=false — clear any stale
	// comment instead of posting.
	suppress bool
	// commentCurrent marks PRs whose delivered comment already carries
	// stabilityKey; nil leaves the currency check to the commenter.
	commentCurrent map[int]bool
	// wasReEval marks TTL re-eval admissions, metered against the re-eval
	// budget when the outcome isn't green.
	wasReEval   map[int]bool
	llmDegraded bool
	// record, when set, replaces the outcome in the head's last-outcome
	// record (see outcomeRecordSelfRemoval).
	record string
	// retireScope drops the scope from the PR's head-independent label roster
	// after recording this head's outcome: a Kustomization that is being
	// deleted never reports on a later head, so a later head mustn't wait for
	// it before flux-success.
	retireScope bool
	resources   int // inventory rows, for the delivery log line
}

// deliver posts (or clears) the comment on every PR, then reconciles its
// labels and outcome record. keepMark is set for each delivered PR. On an
// error the returned cooldown is non-zero for a GitHub rate limit, which the
// caller's admission rollback uses (see keepMark in Process).
func (p *Processor) deliver(ctx context.Context, d prDelivery, prs []bridgegithub.PRInfo, keepMark map[int]bool) (time.Duration, error) {
	ctx, cancelDelivery := deliveryContext(ctx)
	defer cancelDelivery()
	owner, repo := d.owner, d.repo
	scope := d.ns + "/" + d.name
	for _, pr := range prs {
		if d.suppress {
			// Healthy + COMMENT_ON_SUCCESS=false: post nothing, and clear any
			// stale comment from an earlier failing run (cache-guarded → a
			// healthy-from-start PR pays no GitHub call).
			if err := p.commenter.RemoveComment(ctx, owner, repo, d.ns, d.name, pr.Number); err != nil {
				return bridgegithub.RateLimitCooldown(err), fmt.Errorf("remove stale pr comment: %w", err)
			}
			p.logger.Info("healthy outcome; comment suppressed (COMMENT_ON_SUCCESS=false)",
				"owner", owner, "repo", repo, "pr", pr.Number,
				"kustomization", scope)
		} else if d.commentCurrent[pr.Number] {
			// The delivered comment already describes this exact state — only
			// the LLM's wording could differ, and identical state needs no
			// re-worded verdict. No GitHub write, no notification, no swap.
			p.logger.Info("PR comment already current for this state; left untouched",
				"owner", owner, "repo", repo, "pr", pr.Number,
				"kustomization", scope)
		} else {
			if err := p.commenter.PostOrUpdateCommentKeyed(ctx, owner, repo, d.ns, d.name, pr.Number, d.body, d.stabilityKey); err != nil {
				// A rate limit puts the undelivered marks on a cooldown (0 for
				// any other error, which the deferred rollback releases); see
				// the keepMark docstring.
				return bridgegithub.RateLimitCooldown(err), fmt.Errorf("post pr comment: %w", err)
			}
			p.logger.Info("delivered PR comment", "owner", owner, "repo", repo, "pr", pr.Number,
				"kustomization", scope,
				"resources", d.resources)
		}
		keepMark[pr.Number] = true
		p.reconcileLabels(ctx, owner, repo, scope, pr.HeadSHA, pr.Number, d.outcome)
		if d.retireScope && p.cfg.PRLabelEnabled {
			p.dedupe.RemovePRScope(owner, repo, pr.Number, scope)
		}
		// Record this head's outcome so a later re-eval can skip re-polling it
		// when it was healthy (DEDUPE_REEVAL_FAILED_ONLY) and detect that the
		// head was analysed before (DEDUPE_PR_REEVAL_MAX counts re-evals only
		// for heads with a record). Only bothered with when re-eval plus at
		// least one consumer is on, since nothing reads it otherwise.
		// A self-removal record is always kept: filterPRs reads it to keep the
		// PR attributed at this head, whatever the re-eval knobs.
		if d.record == outcomeRecordSelfRemoval || (p.cfg.DedupePRReEvalTTL > 0 && (p.cfg.DedupeReEvalFailedOnly || p.cfg.DedupePRReEvalMax > 0)) {
			recorded := cmp.Or(d.record, string(d.outcome))
			if d.llmDegraded && d.outcome == bridgegithub.OutcomeSuccess {
				// Recorded as success, the failed-only gate would freeze the
				// head before the next re-eval reaches the degraded stability
				// key, so "LLM analysis unavailable" would stay until a new
				// commit. A record of its own lets the re-eval retry the LLM.
				recorded = outcomeRecordSuccessNoLLM
			}
			p.dedupe.RecordPROutcome(d.ns, d.name, pr.Number, pr.HeadSHA, recorded, p.outcomeRecordTTL(recorded))
		}
		// Budget accounting happens AFTER the analysis, and only for re-evals
		// that concluded failed/warning: a healthy re-eval is the recovery the
		// re-eval exists to catch (and flips the head back to cost-free
		// failed-only skips), so it never burns budget.
		if d.wasReEval[pr.Number] && d.outcome != bridgegithub.OutcomeSuccess && p.cfg.DedupePRReEvalMax > 0 {
			p.dedupe.IncrPRReEval(d.ns, d.name, pr.Number, pr.HeadSHA, p.reEvalRecordTTL())
		}
	}
	return 0, nil
}

// postPollReserve is the part of the pipeline deadline the readiness poll
// leaves free for what follows it: failure context, the LLM call (plus its
// one corrective retry) and delivery. The webhook handler's deadline is
// READINESS_POLL_TIMEOUT + 2m from arrival, so the poll only gets shortened
// when the lookups before it took longer than the remaining 30s.
const postPollReserve = 90 * time.Second

// deliveryTimeout bounds comment and label delivery for all target PRs.
const deliveryTimeout = 2 * time.Minute

// settleBudget is how long the readiness poll (inventory, then pods) may run:
// timeout, cut short so postPollReserve remains before ctx's deadline. Without
// the cut, a poll that started late runs into the deadline itself and fails
// the event — no comment, marks rolled back, the same overrun on every retry.
// Never below a second, so the poll still takes one snapshot.
func settleBudget(ctx context.Context, timeout time.Duration) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return timeout
	}
	return max(min(timeout, time.Until(deadline)-postPollReserve), time.Second)
}

// deliveryContext detaches delivery from the pipeline deadline: the analysis
// is done (and an LLM call paid for), so a deadline that expired during it
// must not throw the result away and force a full re-run on the next event.
// Cancellation for any other reason still stops delivery, and so does
// shutdown after the deadline already fired (the shutdown signal the handler
// attaches, see lifecycle) — so the rollback runs within the unwind budget.
// Bounded by deliveryTimeout.
func deliveryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	dctx, cancel := lifecycle.Detach(ctx, deliveryTimeout)
	stop := context.AfterFunc(ctx, func() {
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cancel()
		}
	})
	return dctx, func() {
		stop()
		cancel()
	}
}

// outcomeRecordSuccessNoLLM is the last-outcome record of a healthy analysis
// whose LLM call failed. It isn't a final verdict: unlike a success record it
// doesn't freeze the head under DEDUPE_REEVAL_FAILED_ONLY, and it expires like
// a failure record.
const outcomeRecordSuccessNoLLM = "success-without-llm"

// outcomeRecordSelfRemoval is the last-outcome record of a ✅ "being removed"
// report (see the self-removal branch in Process). Like
// outcomeRecordSuccessNoLLM it doesn't freeze the head: if the parent never
// deletes the Kustomization, later re-evals still see its failure.
const outcomeRecordSelfRemoval = "success-self-removal"

// outcomeRecordTTL bounds the last-outcome side-channel records. A failed or
// warning record lives twice the re-eval floor: long enough to outlive the gap
// between re-evals (it's refreshed on every skip), short enough to self-expire
// once a PR-head stops producing events. A success record is what keeps a
// green head frozen until its next commit under DEDUPE_REEVAL_FAILED_ONLY, and
// letting it lapse after a quiet spell (a night) re-analysed the head as if
// new — flipping it ❌ on a sibling PR's failure, the very flapping the freeze
// prevents. Only an admitted event (a frozen skip) rewrites it; events inside
// the re-eval floor are deduped before they reach that refresh. So it outlives
// the admission mark set with it by labelStateTTL, or by a whole floor when
// that is longer: the head stays frozen while events keep arriving less than a
// floor apart, and lapses only when none comes for labelStateTTL after the
// re-eval window opens, far longer than the overnight and weekend gaps between
// events. Not the DEDUPE_PERSISTENCE_MAX_TTL backstop: one record per healthy
// Kustomization × PR-head held for 30 days was most of the Redis keyspace, for
// nothing. Zero (never used) for failed/warning records when re-eval is
// disabled.
func (p *Processor) outcomeRecordTTL(outcome string) time.Duration {
	switch outcome {
	case string(bridgegithub.OutcomeSuccess):
		floor := max(p.cfg.DedupePRReEvalTTL, 0)
		return floor + max(labelStateTTL, floor)
	case outcomeRecordSelfRemoval:
		// filterPRs keeps the PR attributed off this record, so a finalizer
		// failure long after the removal report still reaches the PR: keep it
		// as long as the head's label state.
		return labelStateTTL
	}
	return p.reEvalRecordTTL()
}

// reEvalRecordTTL is the lifetime of records that track a failing streak (the
// re-eval counter, non-success outcomes): twice the re-eval floor, 0 when
// re-eval is disabled.
func (p *Processor) reEvalRecordTTL() time.Duration {
	if p.cfg.DedupePRReEvalTTL <= 0 {
		return 0
	}
	return 2 * p.cfg.DedupePRReEvalTTL
}

// labelStateTTL bounds the per-PR-head scope→outcome records that back label
// aggregation. Refreshed on every event, so it only matters once a PR-head goes
// quiet; generous enough to survive normal reconcile gaps, short enough that a
// stale PR-head's record self-cleans from Redis.
const labelStateTTL = 7 * 24 * time.Hour

// reconcileLabels applies the outcome-driven PR labels when PR_LABEL_ENABLED is
// set. It runs independently of comment delivery — whether the comment was
// posted, left unchanged, or suppressed (COMMENT_ON_SUCCESS=false) — so a
// downstream merge gate keyed on flux-success/flux-failed stays accurate even
// when healthy comments are turned off. Strictly best-effort: a labelling
// hiccup is logged WARN, never returned, so it can't tank a pipeline whose
// comment work already succeeded. Sustained failures (revoked token, repo
// without Issues permission) are visible in the logs.
//
// The label reflects the AGGREGATE across every Kustomization attributed to the
// PR, not just this one: a PR can touch several Kustomizations (separate events,
// separate times), and a merge gate wants "any failing → flux-failed, all green
// → flux-success." We record this Kustomization's outcome into the per-PR-head
// map and label from the aggregate of all recorded scopes — so a later healthy
// event for one Kustomization can't flip the PR green while another is still
// red. scope is "<ns>/<name>"; headSHA scopes the map to the current PR head.
func (p *Processor) reconcileLabels(ctx context.Context, owner, repo, scope, headSHA string, prNumber int, outcome bridgegithub.Outcome) {
	if !p.cfg.PRLabelEnabled {
		return
	}
	p.dedupe.PutScopeOutcome(owner, repo, prNumber, headSHA, scope, string(outcome), labelStateTTL)
	// Add this Kustomization to the PR's head-independent roster, so a later head
	// (a new push) knows this scope is expected before declaring success.
	p.dedupe.AddPRScope(owner, repo, prNumber, scope, labelStateTTL)
	p.applyPRLabel(ctx, owner, repo, prNumber, headSHA, outcome)
}

// applyPRLabel computes the PR-global aggregate from the per-head scope outcomes
// and writes it — EXCEPT a SUCCESS aggregate is held until every Kustomization
// in the PR's roster has reported for THIS head (anti-flap).
//
// Why hold only success: early in a head's life only the fast scopes have
// reported, so an all-green view over that PARTIAL set writes flux-success — then
// flips to flux-failed/flux-warning seconds later when a slow scope (e.g. an
// app-of-apps parent that's perpetually Progressing) reports. A failure or
// warning aggregate, by contrast, is written immediately: a non-green scope that
// HAS reported is real regardless of which scopes are still pending, and surfacing
// bad news promptly is desirable. Success is the only verdict a still-pending
// scope can falsify, so it's the only one that waits for a complete roster.
//
// fallback is this event's own outcome, used when no scopes are recorded (cache
// unavailable) so single-Kustomization labelling still works without persistence;
// an empty roster is trivially "covered", so that path also labels eagerly.
func (p *Processor) applyPRLabel(ctx context.Context, owner, repo string, prNumber int, headSHA string, fallback bridgegithub.Outcome) {
	scopes := p.dedupe.ScopeOutcomes(owner, repo, prNumber, headSHA)
	agg := aggregatePRLabelOutcome(scopes)
	if len(scopes) == 0 {
		agg = fallback
	}
	// OutcomeNone (only reachable from the de-scope path with no outcomes yet
	// recorded for this head) clears ALL labels — correct only when the PR has
	// nothing left to say at all. If the roster still lists other scopes, they
	// just haven't reconciled this head yet; stripping the label now would
	// visibly flap (e.g. flux-success removed here, re-added seconds later when
	// the surviving scope reports). Leave the existing labels in place — the
	// pending scopes assert the right state when they report.
	if agg == bridgegithub.OutcomeNone {
		if roster := p.dedupe.PRScopes(owner, repo, prNumber); len(roster) > 0 {
			p.logger.Debug("skipping label clear: roster still has pending scopes for this PR",
				"owner", owner, "repo", repo, "pr", prNumber, "head_sha", headSHA, "roster", len(roster))
			return
		}
	}
	if agg == bridgegithub.OutcomeSuccess {
		if roster := p.dedupe.PRScopes(owner, repo, prNumber); !headCoversRoster(scopes, roster) {
			p.logger.Info("holding flux-success: not all known scopes have reported for this head",
				"owner", owner, "repo", repo, "pr", prNumber, "head_sha", headSHA,
				"reported", len(scopes), "roster", len(roster))
			return
		}
	}
	if err := p.commenter.SetPRLabels(ctx, owner, repo, prNumber, headSHA, agg); err != nil {
		p.logger.Warn("PR label update failed",
			"owner", owner, "repo", repo, "pr", prNumber, "aggregate", string(agg), "error", err.Error())
		return
	}
	p.logger.Debug("PR labels reconciled",
		"owner", owner, "repo", repo, "pr", prNumber, "aggregate", string(agg))
}

// headCoversRoster reports whether every scope in the PR's roster has recorded an
// outcome for the current head. A roster scope absent from headScopes means that
// Kustomization hasn't reconciled this head yet → a success aggregate is still
// partial. An empty roster is trivially covered.
func headCoversRoster(headScopes map[string]string, roster []string) bool {
	for _, s := range roster {
		if _, ok := headScopes[s]; !ok {
			return false
		}
	}
	return true
}

// aggregatePRLabelOutcome collapses every attributed Kustomization's outcome
// into the single label the PR should carry:
//   - ANY failure            → OutcomeFailure (flux-failed) — a failure anywhere
//     blocks the PR, regardless of other green Kustomizations.
//   - all success            → OutcomeSuccess (flux-success).
//   - otherwise (a warning /
//     in-progress, no failure) → OutcomeWarning (flux-warning) — not green yet,
//     but nothing has definitively failed.
//
// Empty (no scopes tracked, e.g. after the last one is de-scoped) → OutcomeNone,
// which clears ALL bridge labels. This is distinct from OutcomeWarning: a
// genuine warning APPLIES the warning label, but a PR with nothing left to say
// about must shed its labels entirely rather than acquire flux-warning.
func aggregatePRLabelOutcome(scopeOutcomes map[string]string) bridgegithub.Outcome {
	if len(scopeOutcomes) == 0 {
		return bridgegithub.OutcomeNone
	}
	allSuccess := true
	for _, o := range scopeOutcomes {
		if o == string(bridgegithub.OutcomeFailure) {
			return bridgegithub.OutcomeFailure
		}
		if o != string(bridgegithub.OutcomeSuccess) {
			allSuccess = false
		}
	}
	if allSuccess {
		return bridgegithub.OutcomeSuccess
	}
	return bridgegithub.OutcomeWarning
}

// filterPRs narrows the SHA-lookup result to the PRs that should receive a
// comment (open, carrying the required labels, attributed to this
// Kustomization) and also returns the number of finalized PRs (for the log
// line) and the pruned resource IDs whose teardown the caller must verify.
// selfRemovalPath, when set, is the parent's build path for a Kustomization
// whose failure looks like its own teardown: a PR that may edit this
// Kustomization's CR out of a file there (prEditsOutSelf) is admitted too, but
// only for the self-removal check — the returned set marks those PRs, which
// get no normal report (see Process).
// ns/name is the Kustomization scope, used to clean up a now-stale comment
// when a PR no longer touches this inventory (see below).
//
// Attribution (prTouchesInventory) is the load-bearing step: for each PR we
// fetch its changed files, fetch each file's content (cached by blob SHA),
// parse the YAML, and check whether any declared resource is in the
// Kustomization's inventory. A PR earns a comment iff at least one of its
// files declares a resource THIS Kustomization manages. This makes the
// path of the file irrelevant — only what the file declares matters, which
// is the only attribution model that's robust to flat-monorepo layouts
// where every Kustomization shares a similar spec.path.
//
// On per-PR errors (ListPRFiles, BlobResourceIDs), most are logged and
// the PR is treated as "no overlap" — a transient GitHub blip shouldn't
// spam unrelated PRs. Rate-limit errors are an exception: they're
// returned so the webhook handler classifies the event as `rate_limited`.
//
// De-scoping cleanup: a PR that no longer declares ANY resource in this
// inventory (e.g. the workload change was reverted out of the PR) gets its
// prior comment removed, so a stale "❌ failing" verdict for a change that's
// gone doesn't linger. Only on a DEFINITIVE no-match (no blob-fetch errors)
// AND a trusted inventory (inventoryTrusted: non-error event, non-empty
// inventory — a failing reconcile's inventory can be missing exactly the
// resources the PR added, and acting on it would erase the failure evidence),
// and only via the cache-guarded RemoveComment — zero GitHub calls unless a
// comment was actually recorded for this (PR, scope).
func (p *Processor) filterPRs(ctx context.Context, owner, repo, ns, name string, inventory map[string]struct{}, buildFiles map[string]struct{}, scopeFailed bool, inventoryTrusted bool, selfRemovalPath string, prs []bridgegithub.PRInfo) ([]bridgegithub.PRInfo, int, []string, map[int]bool, error) {
	var out []bridgegithub.PRInfo
	var finalized int
	// prunedToVerify accumulates the inventory IDs of resources that attributed
	// PRs removed AND this reconcile pruned — the caller re-checks each to catch
	// a finalizer-stuck teardown (a lingering Terminating object keeps the
	// verdict non-green).
	var prunedToVerify []string
	// selfRemovalOnly marks PRs admitted only by prEditsOutSelf.
	selfRemovalOnly := map[int]bool{}
	// Lazily resolves what THIS reconcile pruned, so a pure-deletion PR can be
	// attributed by its removed resource even after the resource has left the
	// inventory. Memoised + fetched only on first use, so non-deletion PRs pay
	// nothing (see newPruneSetProvider).
	pruneSet := p.newPruneSetProvider(ctx, ns, name)
	for _, pr := range prs {
		if pr.Merged || pr.State == "closed" {
			finalized++
			continue
		}
		if !prHasRequiredLabel(p.cfg.RequiredPRLabels, pr.Labels) {
			p.logger.Debug("PR skipped: required label not present",
				"pr", pr.Number, "labels", pr.Labels, "required", p.cfg.RequiredPRLabels)
			continue
		}
		files, err := p.commenter.ListPRFiles(ctx, owner, repo, pr.Number, pr.HeadSHA)
		if err != nil {
			if bridgegithub.IsRateLimit(err) {
				return nil, 0, nil, nil, err
			}
			p.logger.Warn("ListPRFiles failed; skipping PR (fail-closed to avoid spamming unrelated PRs)",
				"pr", pr.Number, "error", err.Error())
			continue
		}
		matched, definitive, removedAbsent, prunedIDs, err := p.prTouchesInventory(ctx, owner, repo, pr.Number, files, inventory, buildFiles, scopeFailed, ns, name, pruneSet, pr.BaseSHA)
		if err != nil {
			// Only rate-limit errors propagate here — other blob-fetch
			// errors are logged and treated as no-overlap inside
			// prTouchesInventory.
			return nil, 0, nil, nil, err
		}
		if !matched {
			// Verified as this Kustomization's removal at this head earlier
			// (the ✅ "being removed" report): stay attributed, so a later
			// failure — a finalizer that can't clean up — replaces that ✅.
			if last, ok := p.dedupe.LastPROutcome(ns, name, pr.Number, pr.HeadSHA); ok && last == outcomeRecordSelfRemoval {
				matched = true
			}
		}
		if !matched && selfRemovalPath != "" {
			edits, eerr := p.prEditsOutSelf(ctx, owner, repo, pr, files, name, selfRemovalPath)
			if eerr != nil {
				return nil, 0, nil, nil, eerr
			}
			if edits {
				p.logger.Debug("PR may edit this Kustomization's CR out of its parent's manifests; admitted for the self-removal check only",
					"pr", pr.Number, "kustomization", ns+"/"+name)
				matched = true
				selfRemovalOnly[pr.Number] = true
			}
		}
		if matched {
			out = append(out, pr)
			prunedToVerify = append(prunedToVerify, prunedIDs...)
			continue
		}
		if removedAbsent && definitive {
			// The PR DELETES resources and the inventory no longer lists them:
			// that's the deletion having SUCCEEDED, not the PR ceasing to touch
			// this Kustomization — de-scoping here erased a deletion PR's ✅
			// comment and flux-success label one window after it earned them
			// (observed on a CronJob-removal PR). If the roster shows the
			// bridge attributed the PR here while the resource still existed,
			// keep it attributed so verdicts continue like any other PR's;
			// either way, absence-by-deletion is never grounds for the
			// destructive cleanup below.
			inRoster := false
			for _, s := range p.dedupe.PRScopes(owner, repo, pr.Number) {
				if s == ns+"/"+name {
					inRoster = true
					break
				}
			}
			if inRoster {
				p.logger.Debug("PR kept attributed: its deleted resources are pruned from this inventory (deletion in effect)",
					"pr", pr.Number, "kustomization", ns+"/"+name)
				out = append(out, pr)
			} else {
				p.logger.Debug("PR not attributed but de-scope skipped: deleted resources absent from inventory",
					"pr", pr.Number, "kustomization", ns+"/"+name)
			}
			continue
		}
		p.logger.Debug("PR skipped: no changed file declares a resource in this Kustomization's inventory",
			"pr", pr.Number, "file_count", len(files), "inventory_size", len(inventory))
		// De-scoped cleanup: if the PR USED to touch this Kustomization (we
		// posted a comment) but no longer does — e.g. the change to a workload
		// was reverted out of the PR — the old comment is now stale and
		// misleading. Remove it. Guarded three ways: (1) only on a `definitive`
		// no-match, so a transient blob-fetch error can't delete a still-valid
		// comment; (2) only against a TRUSTED inventory (non-error event,
		// non-empty inventory) — a failing reconcile's inventory can be missing
		// exactly the resources the PR added, and erasing the comment + label
		// then would wipe the failure evidence the PR author needs (the
		// cleanup simply waits for the next successful reconcile to confirm);
		// (3) RemoveComment is cache-guarded, so this is a no-op with
		// ZERO GitHub calls unless we actually recorded a comment for this
		// (PR, scope) — the cost is one DELETE only on a real de-scoping.
		// Best-effort: a cleanup failure is logged, never fatal to the filter.
		if definitive && !inventoryTrusted {
			p.logger.Debug("de-scope cleanup deferred: inventory not trustworthy on this event (error severity or empty)",
				"pr", pr.Number, "kustomization", ns+"/"+name)
		}
		if definitive && inventoryTrusted {
			if rerr := p.commenter.RemoveComment(ctx, owner, repo, ns, name, pr.Number); rerr != nil {
				p.logger.Warn("stale comment cleanup failed for de-scoped PR",
					"pr", pr.Number, "kustomization", ns+"/"+name, "error", rerr.Error())
			}
			// Drop this Kustomization from the PR's label aggregate (per-head) AND
			// its roster (head-independent), so a now-removed Kustomization stops
			// forcing flux-failed and stops holding back flux-success.
			//
			// The roster removal must NOT be gated on the per-head outcome: a scope
			// can be in the roster (learned from a prior head) yet have no outcome
			// on THIS head — which is exactly the held-success case — and leaving it
			// in the roster would hold flux-success on the new head indefinitely
			// (its roster entry never gets covered). So remove from both and
			// recompute whenever EITHER changed. Both removes report presence, so
			// a PR that never touched this Kustomization still costs nothing.
			if p.cfg.PRLabelEnabled {
				removedOutcome := p.dedupe.RemoveScopeOutcome(owner, repo, pr.Number, pr.HeadSHA, ns+"/"+name)
				removedRoster := p.dedupe.RemovePRScope(owner, repo, pr.Number, ns+"/"+name)
				if removedOutcome || removedRoster {
					p.applyPRLabel(ctx, owner, repo, pr.Number, pr.HeadSHA, bridgegithub.OutcomeNone)
				}
			}
		}
	}
	return out, finalized, prunedToVerify, selfRemovalOnly, nil
}

// fluxKustomizationGroupKind is the trailing `<group>_<kind>` of a Flux
// Kustomization's inventory ID (`<ns>_<name>_kustomize.toolkit.fluxcd.io_Kustomization`).
const fluxKustomizationGroupKind = "kustomize.toolkit.fluxcd.io_Kustomization"

// readinessRefsFor builds the inventory IDs to POLL for a Kustomization's
// readiness, given its full inventory `refs`.
//
// Child Flux Kustomizations are dropped: an "app-of-apps" parent (e.g. `cluster`
// with 160+ child Kustomizations) shouldn't be judged by polling every child to
// `Current` — each child reconciles on its own schedule and is analysed on its
// own event, and a `wait: false` parent is `Ready` once it has *applied* the
// children, not once they're healthy. Polling them just makes the parent
// perpetually time out into a Warning and dumps 100+ unrelated rows into the
// comment. When any child is dropped, the parent's OWN Kustomization object is
// added instead, so its apply health (`Ready`/`Stalled`) is still evaluated and
// a pure aggregate has a row to report. Non-Kustomization resources the parent
// directly manages (workloads, HelmReleases, GitRepositories, …) are kept and
// polled as before — a broken one the change touched still surfaces.
//
// A leaf Kustomization (no child Kustomizations in its inventory) is returned
// unchanged — no behaviour change for the common case.
func readinessRefsFor(refs []string, eventNamespace, eventName string) []string {
	out := make([]string, 0, len(refs)+1)
	droppedChild := false
	for _, r := range refs {
		if strings.HasSuffix(r, "_"+fluxKustomizationGroupKind) {
			droppedChild = true
			continue
		}
		out = append(out, r)
	}
	if droppedChild {
		out = append(out, eventNamespace+"_"+eventName+"_"+fluxKustomizationGroupKind)
	}
	return out
}

// prTouchesInventory reports whether any of the PR's changed files declares a
// resource THIS Kustomization manages. It's pure attribution — a true means the
// PR earns a comment; the comment then reports the WHOLE Kustomization
// inventory regardless of which resource matched (a PR's change can break other
// resources in the same Kustomization — e.g. a DB change failing dependent
// pods — so we always evaluate the full deploy unit, not just the diff).
//
// Flux Kustomization CRs are attributed to the TARGET, not the parent. A file
// that declares a `Kustomization` object is the *definition* of a Kustomization
// that reconciles (and emits its own events) on its own. Such a file matches
// ONLY the event whose Kustomization IS that object (a self-match: eventNS/Name
// == the declared object), and is deliberately NOT counted as an inventory hit
// for the parent Kustomization that merely applies the CR. Without this, a PR
// editing a child CR lights up the aggregate parent that owns it (an
// app-of-apps Kustomization with many children, whose "all children Current"
// readiness bar is unrealistic) instead of the child itself, where the change
// actually takes effect. Non-Kustomization resources match by inventory
// membership as before.
//
// A present-resource match (self-match on the Kustomization's own CR, or an
// inventory-membership hit) short-circuits — attribution only needs "does
// SOMETHING overlap", so it stops fetching blobs as soon as one does. A
// prune-based match does NOT short-circuit: scanning continues so EVERY pruned ID
// the PR removed is collected (the caller re-verifies each — a finalizer-stuck
// object keeps the verdict non-green).
//
// Returns (matched, definitive, removedAbsent, prunedIDs, err):
//   - matched    — at least one changed file is attributed to THIS Kustomization
//     (inventory membership, a self-match on its own CR, or a prune match).
//   - definitive — true iff EVERY file was evaluated cleanly (no blob-fetch
//     errors). A `false, definitive=true` is a confident "this PR genuinely
//     declares nothing we manage" — the signal filterPRs needs before removing a
//     now-stale comment. A `false, definitive=false` means a blob fetch failed
//     and was treated as no-overlap, so the negative might be spurious; callers
//     must NOT act destructively on it.
//   - removedAbsent — set when a `removed` file declared a workload that's gone
//     from the inventory but was NOT observed in the prune set (deletion in
//     effect without prune evidence). Only meaningful when matched is false;
//     filterPRs uses it with the label roster to avoid de-scoping a deletion PR.
//   - prunedIDs — when matched via prune, the full inventory IDs of the removed
//     resources confirmed in the prune set, for the caller's teardown re-check.
//   - err — set ONLY for rate-limit errors (propagated so the handler classifies
//     the event and skips the dedupe Forget). Other BlobResourceIDs errors are
//     logged, flip definitive to false, and the file is treated as no-overlap.
//
// pruneSet attributes a pure-deletion PR by what THIS reconcile removed: a
// `removed` file whose declared resource the Kustomization just pruned is a
// match even though the resource is gone from the live inventory (case (a)), and
// a `removed` file declaring a child Kustomization the PARENT just pruned is
// attributed to that parent (case (b) — the only place a whole-Kustomization
// teardown can be reported, since the deleted child stops emitting events).
func (p *Processor) prTouchesInventory(ctx context.Context, owner, repo string, prNumber int, files []bridgegithub.PRFile, inventory map[string]struct{}, buildFiles map[string]struct{}, scopeFailed bool, eventNamespace, eventName string, pruneSet pruneSetProvider, baseRef string) (matched, definitive, removedAbsent bool, prunedIDs []string, err error) {
	// selfID is THIS Kustomization's own CR object. A changed file that declares
	// it means the PR edits this Kustomization's definition → attribute here.
	selfID := eventNamespace + "_" + eventName + "_" + fluxKustomizationGroupKind
	definitive = true
	// pruneMatched: a `removed` file whose resource this reconcile pruned.
	// presentMatch: a self-/inventory-match on a resource that's still present.
	// Either is a positive attribution, but NEITHER short-circuits the whole scan
	// — we keep scanning `removed` files to collect EVERY pruned ID the PR
	// removed, so the caller can verify each went away (a finalizer-stuck object
	// keeps the verdict non-green), even in a mixed PR that also touches a present
	// resource. Once attribution is established, non-`removed` files (which can't
	// contribute prunes) are skipped to keep the common non-deletion case cheap.
	pruneMatched := false
	presentMatch := false
	// buildMatch: a non-removed changed file that is part of THIS Kustomization's
	// kustomize build (per the resolved build set). It attributes the PR even when
	// the file declares no inventory resource of its own — a patch, the
	// kustomization.yaml itself, or a generator input file. Only set when a build
	// set is available (buildFiles != nil).
	buildMatch := false
	// modifiedHeadIDs records each `modified` file's head-blob IDs so an in-place
	// removal (a resource edited OUT of a still-present file) can be recovered by
	// a base-vs-head diff after the loop — but only on the unattributed path, so
	// the common edit pays nothing.
	type modifiedFile struct {
		path    string
		headIDs []string
	}
	var modifiedFiles []modifiedFile
	for _, f := range files {
		// Build scope: a present (non-`removed`) changed file may attribute the PR
		// to THIS Kustomization only if it is part of the Kustomization's kustomize
		// build, resolved exactly from the source-controller artifact (the overlay
		// at spec.path plus the bases it includes). This is what stops a
		// prod-overlay edit from being attributed to the dev overlay's same-named
		// resource. `removed` files bypass this entirely: a deletion's file is gone
		// from the artifact tree, so the prune/teardown logic below handles it —
		// and it doesn't depend on the build scope at all.
		//
		// fileInBuild only RECORDS membership; the attribution decision (buildMatch)
		// is made after the id loop so it can honour follow-CR-to-target — a file
		// that only declares a foreign child Kustomization CR must not attribute to
		// this (parent) Kustomization just because the parent's build includes it.
		fileInBuild := false
		if f.Status != "removed" {
			switch {
			case scopeFailed:
				// The build scope couldn't be resolved for this revision. Without
				// it we can't confirm a present file belongs to the build, so it
				// must not attribute by identity/membership (no overlay-blind
				// fallback) — a later reconcile at the matching revision re-drives
				// present-file PRs. Mark the no-match NON-definitive: "scope
				// unavailable" is NOT "PR no longer touches this Kustomization", so
				// it must never drive the destructive de-scope cleanup below.
				//
				// A `modified` file may still carry an IN-PLACE removal (a resource
				// edited out), which is a deletion attributed from prune evidence —
				// that path does not need the build scope. So let modified files
				// fall through to record their head IDs for the in-place diff (the
				// per-id present attribution is short-circuited just below); `added`
				// files have no base to diff and only add present content, so skip.
				definitive = false
				if f.Status != "modified" {
					p.logger.Debug("present file skipped: build scope unavailable for this revision",
						"pr", prNumber, "path", f.Path)
					continue
				}
			case buildFiles != nil:
				if _, inBuild := buildFiles[bridgesource.CleanPath(f.Path)]; !inBuild {
					p.logger.Debug("PR file is not part of this Kustomization's build; not attributing",
						"pr", prNumber, "path", f.Path)
					continue
				}
				fileInBuild = true
			}
		}
		// Files that can't declare a resource (scripts, docs, images, lockfiles)
		// need no blob fetch. Inside the build they still attribute — as a build
		// input, e.g. a configMapGenerator's script — the same way an in-build
		// file declaring no resource does after the id loop; outside it they're
		// ruled out. This runs after the build-scope check, not before it: a
		// generator input like `init.sh` would otherwise never attribute.
		if skippableFile(f.Path) {
			if fileInBuild {
				buildMatch = true
			}
			continue
		}
		// Attribution already known → skip files that can't add to the verdict.
		// `removed` files still scan to collect every pruned ID (case a/b);
		// `modified` files still scan so a fully-removed resource (commented out
		// → empty head blob) is collected for the in-place teardown diff below,
		// even on an already-attributed PR. Other files (added, etc.) are skipped —
		// buildMatch counts as "attributed", so once a build-input file lands the
		// blob fetch for further added files is skipped.
		if (presentMatch || pruneMatched || buildMatch) && f.Status != "removed" && f.Status != "modified" {
			continue
		}
		if f.BlobSHA == "" {
			// Files without a SHA (e.g. deletions in some API responses)
			// can't be fetched; treat as no overlap — but, exactly like a
			// blob-fetch error below, an unevaluated file means a "no
			// match" verdict is no longer trustworthy enough to act on
			// destructively (comment removal / scope de-listing).
			definitive = false
			continue
		}
		ids, berr := p.commenter.BlobResourceIDs(ctx, owner, repo, f.BlobSHA)
		ids, _ = splitParsedIDs(ids)
		if berr != nil {
			if bridgegithub.IsRateLimit(berr) {
				return false, false, false, nil, berr
			}
			p.logger.Warn("BlobResourceIDs failed; treating file as no-overlap",
				"pr", prNumber, "path", f.Path, "sha", f.BlobSHA, "error", berr.Error())
			// Couldn't read this file — the overall "no match" verdict is no
			// longer trustworthy, so block any destructive cleanup.
			definitive = false
			continue
		}
		// Record modified files for the in-place-removal fallback below. (A
		// `removed` file is handled inline; an `added` file has no base version
		// to diff.)
		if f.Status == "modified" {
			modifiedFiles = append(modifiedFiles, modifiedFile{path: f.Path, headIDs: ids})
		}
		// When the build scope is unavailable, a `modified` file reaches here only
		// to feed the in-place-removal diff (deletion via prune evidence, which
		// doesn't need the scope). Its present-file content must NOT attribute the
		// PR by identity/membership, so short-circuit the per-id work below — only
		// prune-based attribution may still happen for this PR. `removed` files
		// never went through the scope gate and carry the prune match itself, so
		// they continue exactly as when the scope resolved.
		if scopeFailed && f.Status != "removed" {
			continue
		}
		// Track what the file declares, for the follow-CR-to-target-aware build
		// membership decision after the loop: declaresForeignCR (a child Flux
		// Kustomization CR that isn't this event's own) vs declaresNonForeign (a
		// workload, or this Kustomization's own CR). A file that declares ONLY
		// foreign CRs must not attribute to this parent by membership.
		declaresForeignCR := false
		declaresNonForeign := false
		declaredWorkload := false
		// Prune subjects only help resolve a removed file's IDs (they're gone
		// from the inventory); don't fetch them for other files.
		var idPrunes pruneSetProvider
		if f.Status == "removed" {
			idPrunes = pruneSet
		}
		for _, id := range ids {
			id = canonicalID(id, inventory, idPrunes)
			// A Flux Kustomization object is attributed to the TARGET it
			// defines, never to a parent that applies it. Self-match when the
			// declared object IS this event's Kustomization; otherwise skip it
			// (the target's own reconcile event will claim the PR).
			if strings.HasSuffix(id, "_"+fluxKustomizationGroupKind) {
				if id == selfID {
					p.logger.Debug("PR matched this Kustomization's own CR (follow-to-target)",
						"pr", prNumber, "path", f.Path, "inventory_id", id)
					presentMatch = true
					declaresNonForeign = true
					continue
				}
				declaresForeignCR = true
				// Case (b): the PR removes a child Kustomization CR that THIS
				// (parent) reconcile just pruned. The deleted child can't
				// self-report, so the parent claims the teardown.
				if f.Status == "removed" && inventoryIDWasPruned(pruneSet, id) {
					p.logger.Debug("PR matched a Kustomization this reconcile pruned (teardown attributed to parent)",
						"pr", prNumber, "path", f.Path, "inventory_id", id)
					pruneMatched = true
					prunedIDs = append(prunedIDs, id)
				}
				// Once the prune event ages out of the recent-Event window the
				// match above stops firing, but a removed child Kustomization
				// that's absent from this inventory is a COMPLETED teardown — not
				// a PR that stopped touching this Kustomization. Flag it here
				// (independent of the prune-window check, and of declaredWorkload
				// below which never trips for a Kustomization id) so the caller's
				// roster protection keeps the deletion PR from being de-scoped by
				// its own success.
				if f.Status == "removed" {
					if _, present := inventory[id]; !present {
						removedAbsent = true
					}
				}
				continue
			}
			declaredWorkload = true
			declaresNonForeign = true
			if _, ok := inventory[id]; ok {
				p.logger.Debug("PR matched inventory entry",
					"pr", prNumber, "path", f.Path, "inventory_id", id)
				presentMatch = true
				continue
			}
			// Case (a): the resource is gone from the inventory because THIS
			// reconcile pruned it AND the PR is what removed it — the deletion
			// took effect, so attribute (the verdict then covers the remaining
			// inventory; an empty inventory with a Ready Kustomization is
			// success). Unlike removedAbsent below, this is a positive match: we
			// have direct evidence (the prune event) that the deletion is ours.
			if f.Status == "removed" && inventoryIDWasPruned(pruneSet, id) {
				p.logger.Debug("PR matched a resource this reconcile pruned (deletion in effect)",
					"pr", prNumber, "path", f.Path, "inventory_id", id)
				pruneMatched = true
				prunedIDs = append(prunedIDs, id)
			}
		}
		// A DELETED manifest whose resources are gone from the inventory is
		// the deletion having TAKEN EFFECT — the cluster reflecting the PR's
		// change, not the PR ceasing to touch this Kustomization. Reported to
		// the caller so a deletion PR isn't de-scoped by its own success.
		if f.Status == "removed" && declaredWorkload {
			removedAbsent = true
		}
		// Build-membership attribution (follow-CR-to-target aware). A present
		// in-build file attributes the PR to THIS Kustomization — even when it
		// declares no inventory resource of its own (a patch, the kustomization.yaml,
		// a generator input: declaresForeignCR and declaresNonForeign both false) —
		// UNLESS it declares ONLY foreign child Kustomization CRs. Those belong to
		// their target child, analysed via the child's own reconcile, not this
		// parent that merely applies them (so an app-of-apps parent isn't lit up by
		// a child-CR edit).
		if fileInBuild && (!declaresForeignCR || declaresNonForeign) {
			buildMatch = true
		}
	}
	// In-place removal diff. A resource removed by EDITING a file (commenting it
	// out, deleting one YAML doc from a still-present file) leaves a `modified`
	// file whose head blob no longer declares it — invisible to the loop above.
	// Diff each modified file's BASE blob against its head: an ID in base but
	// absent from head was removed by this PR, and is treated exactly like a
	// `removed` file's resource (attribute if this reconcile pruned it — case a;
	// flag removedAbsent so a completed teardown isn't de-scoped).
	//
	// Two regimes, both needing a base ref:
	//   - PR NOT yet attributed → diff every modified file (this may be the only
	//     thing that attributes a pure in-place-deletion PR).
	//   - PR already attributed → diff only files whose head declares NOTHING (a
	//     fully removed resource). This catches a finalizer-stuck in-place
	//     teardown in a mixed PR — so its pruned ID lands in prunedIDs and keeps
	//     the verdict non-green — without a base fetch for every ordinary edit.
	//     (A multi-doc file that drops ONE of several resources isn't covered
	//     here; that rarer case stays a known gap.)
	if baseRef != "" {
		attributed := presentMatch || pruneMatched || buildMatch
		for _, mf := range modifiedFiles {
			if attributed && len(mf.headIDs) > 0 {
				continue // already attributed and this file still declares resources
			}
			baseIDs, berr := p.commenter.BaseFileResourceIDs(ctx, owner, repo, mf.path, baseRef)
			baseIDs, _ = splitParsedIDs(baseIDs)
			if berr != nil {
				if bridgegithub.IsRateLimit(berr) {
					return false, false, false, nil, berr
				}
				p.logger.Warn("BaseFileResourceIDs failed; can't diff in-place removal",
					"pr", prNumber, "path", mf.path, "base_ref", baseRef, "error", berr.Error())
				definitive = false
				continue
			}
			headSet := make(map[string]struct{}, len(mf.headIDs))
			for _, id := range mf.headIDs {
				headSet[id] = struct{}{}
			}
			for _, id := range baseIDs {
				if _, stillThere := headSet[id]; stillThere {
					continue // not removed by this edit
				}
				id = canonicalID(id, inventory, pruneSet)
				if id == selfID {
					continue // editing out THIS Kustomization's own CR isn't a child teardown
				}
				if inventoryIDWasPruned(pruneSet, id) {
					p.logger.Debug("PR matched a resource this reconcile pruned (in-place removal)",
						"pr", prNumber, "path", mf.path, "inventory_id", id)
					pruneMatched = true
					prunedIDs = append(prunedIDs, id)
				}
				if _, present := inventory[id]; !present {
					removedAbsent = true
				}
			}
		}
	} else if !presentMatch && !pruneMatched && !buildMatch {
		// No base ref to diff against (rare — e.g. a PRInfo deserialized from a
		// pre-upgrade cache entry that predates BaseSHA), and nothing matched. A
		// modified file whose head declares NOTHING could be an in-place full
		// removal (resource commented out) that we can't confirm without the base
		// version — so a "no match" isn't trustworthy enough to destructively
		// de-scope a deletion PR's earned ✅. Mark non-definitive (same stance as an
		// unreadable blob) so the caller withholds cleanup until a base ref is
		// available. A modified file that still DECLARES resources is not this
		// ambiguous case, so ordinary de-scoping is unaffected.
		for _, mf := range modifiedFiles {
			if len(mf.headIDs) == 0 {
				definitive = false
				break
			}
		}
	}
	// A present-resource match or a prune match is a positive attribution. Return
	// every pruned ID collected so the caller can verify the teardown completed;
	// removedAbsent only matters when nothing matched (the caller checks matched
	// first).
	if presentMatch || pruneMatched || buildMatch {
		return true, definitive, false, prunedIDs, nil
	}
	return false, definitive, removedAbsent, nil, nil
}

func (p *Processor) resolveRepository(ctx context.Context, event eventv1.Event, k kustomizationSpec) (string, string, error) {
	if p.cfg.GitHubRepo != "" {
		return splitOwnerRepo(p.cfg.GitHubRepo)
	}
	url, err := p.sourceURL(ctx, event.InvolvedObject.Namespace, k)
	if err != nil {
		return "", "", err
	}
	return parseGitHubRepoURL(url)
}

// sourceURL returns the spec.url of the source a Kustomization in namespace
// builds from (its spec.sourceRef; kind defaults to GitRepository, namespace
// to the Kustomization's).
func (p *Processor) sourceURL(ctx context.Context, namespace string, k kustomizationSpec) (string, error) {
	// Walk to the GitRepository via spec.sourceRef, parse that, extract spec.url.
	if k.Spec.SourceRef.Name == "" {
		return "", fmt.Errorf("kustomization has no sourceRef")
	}
	sourceNamespace := k.Spec.SourceRef.Namespace
	if sourceNamespace == "" {
		sourceNamespace = namespace
	}
	sourceKind := k.Spec.SourceRef.Kind
	if sourceKind == "" {
		sourceKind = "GitRepository"
	}
	repoYAML, err := p.mcp.GetKubernetesResource(ctx, "source.toolkit.fluxcd.io/v1", sourceKind, k.Spec.SourceRef.Name, sourceNamespace)
	if err != nil {
		return "", fmt.Errorf("get git repository: %w", err)
	}
	var r gitRepositorySpec
	if err := yaml.Unmarshal(repoYAML, &r); err != nil {
		return "", fmt.Errorf("parse git repository yaml: %w", err)
	}
	if r.Spec.URL == "" {
		return "", fmt.Errorf("git repository has no spec.url")
	}
	return r.Spec.URL, nil
}

// ResourceIDsFromYAML parses a PR file's content and returns the
// (namespace, name, group, kind) tuples it declares — in the same format
// the Flux inventory uses (`<namespace>_<name>_<group>_<kind>`). Files
// can contain multiple documents separated by `---`; each is decoded
// independently. Documents that lack apiVersion/kind/name are skipped
// (they're not Kubernetes resources we can attribute).
//
// Returns nil on parse error rather than failing — a corrupt or
// non-YAML file just produces zero resources for attribution. The
// caller treats "zero matches" as "this file doesn't belong to me".
//
// Inventory ID format (kustomize-controller):
//
//	<namespace>_<name>_<group>_<kind>
//
// where group is the part before `/` in apiVersion (empty for core
// types like `v1`). Cluster-scoped resources have empty namespace, so
// the ID begins with a leading `_`.
func ResourceIDsFromYAML(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	type docHeader struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Metadata   struct {
			Name      string `yaml:"name"`
			Namespace string `yaml:"namespace"`
		} `yaml:"metadata"`
		Items yaml.Node `yaml:"items"`
	}
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	var ids []string
	for {
		var doc docHeader
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Parse failure on THIS document. A syntax error leaves
			// yaml.v3's Decoder stuck (every further Decode returns the
			// same error), so stop here rather than loop; documents after
			// a bad one are not read. Treat the rest as "declares no
			// resources" rather than aborting the whole filter: the
			// caller only cares whether SOMETHING in the PR matches
			// the inventory, and a single malformed file shouldn't
			// silently mask matches we already found in earlier docs.
			// The sentinel records that the rest was unread (see
			// splitParsedIDs) for callers that need "does NOT declare X".
			ids = append(ids, unparseableResourceID)
			break
		}
		if strings.HasSuffix(doc.Kind, "List") && doc.Items.Kind == yaml.SequenceNode && len(doc.Items.Content) > 0 {
			// A `kind: List` (or `<Kind>List`) wraps its resources in items,
			// which this header parse doesn't read: mark them unread rather
			// than report the file as declaring nothing. A named document of
			// such a kind (a custom resource) still gets its own ID below.
			ids = append(ids, unparseableResourceID)
		}
		if doc.APIVersion == "" || doc.Kind == "" || doc.Metadata.Name == "" {
			continue
		}
		group := ""
		if idx := strings.IndexByte(doc.APIVersion, '/'); idx >= 0 {
			group = doc.APIVersion[:idx]
		}
		ids = append(ids, inventoryID(doc.Metadata.Namespace, doc.Metadata.Name, group, doc.Kind))
	}
	return ids
}

// unparseableResourceID is appended by ResourceIDsFromYAML when part of a file
// went unread: it stops parsing part-way, or wraps resources in a List's
// items. It can't collide with an inventory ID (those never start with "!").
const unparseableResourceID = "!unparseable"

// splitParsedIDs drops the unparseableResourceID sentinel and reports whether
// the file parsed completely. Attribution ignores completeness (a match found
// before the bad document still counts); an absence check must not.
func splitParsedIDs(ids []string) (clean []string, complete bool) {
	complete = true
	clean = ids[:0:0]
	for _, id := range ids {
		if id == unparseableResourceID {
			complete = false
			continue
		}
		clean = append(clean, id)
	}
	return clean, complete
}

// inventoryID formats an object identity the way Flux's inventory (cli-utils
// ObjMetadata.String) does: `<namespace>_<name>_<group>_<kind>`, with `:` in
// RBAC object names (e.g. `system:metrics-reader`) transcoded to `__`.
// Without the transcoding a PR editing such a Role or binding never matches
// its inventory row.
func inventoryID(namespace, name, group, kind string) string {
	if group == "rbac.authorization.k8s.io" {
		switch kind {
		case "Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding":
			name = strings.ReplaceAll(name, ":", "__")
		}
	}
	return namespace + "_" + name + "_" + group + "_" + kind
}

// canonicalID maps a manifest-derived ID onto the identity the cluster
// recorded. A manifest that leaves metadata.namespace to the build
// (kustomize's `namespace:`, or Flux's spec.targetNamespace) yields an
// empty-namespace ID, while the inventory and prune events carry the
// namespace the build assigned. Such an ID resolves to the single inventory
// entry with the same name, group and kind — or, when pruneSet is given (a
// removed resource is no longer in the inventory), to the single prune
// subject with the same kind and name. Anything else — already exact,
// cluster-scoped, ambiguous or unknown — is returned unchanged.
func canonicalID(id string, inventory map[string]struct{}, pruneSet pruneSetProvider) string {
	if !strings.HasPrefix(id, "_") {
		return id
	}
	if _, exact := inventory[id]; exact {
		return id
	}
	match := ""
	for inv := range inventory {
		ns, ok := strings.CutSuffix(inv, id)
		if !ok || ns == "" || strings.Contains(ns, "_") {
			continue
		}
		if match != "" {
			return id // same name/group/kind in several namespaces
		}
		match = inv
	}
	if match != "" || pruneSet == nil {
		return cmp.Or(match, id)
	}
	ref, err := readiness.ParseInventoryID(id)
	if err != nil {
		return id
	}
	ns := ""
	for subject := range pruneSet() {
		parts := strings.Split(subject, "/") // Kind/namespace/name
		if len(parts) != 3 || parts[0] != ref.Kind || parts[2] != ref.Name {
			continue
		}
		if ns != "" {
			return id
		}
		ns = parts[1]
	}
	if ns == "" {
		return id
	}
	return inventoryID(ns, ref.Name, ref.Group, ref.Kind)
}

// skippableFile reports whether a PR file's content can't define a
// Kubernetes resource (Markdown, scripts, images, dotfiles, lockfiles), so its
// blob needn't be fetched. Such a file can still be a build input (a
// generator's source file); the caller attributes it by build membership.
// Conservative list — false negatives cost only an extra fetch.
func skippableFile(path string) bool {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".md"),
		strings.HasSuffix(lower, ".markdown"),
		strings.HasSuffix(lower, ".txt"),
		strings.HasSuffix(lower, ".sh"),
		strings.HasSuffix(lower, ".png"),
		strings.HasSuffix(lower, ".jpg"),
		strings.HasSuffix(lower, ".jpeg"),
		strings.HasSuffix(lower, ".svg"),
		strings.HasSuffix(lower, ".gif"):
		return true
	}
	base := path
	if idx := strings.LastIndexByte(path, '/'); idx >= 0 {
		base = path[idx+1:]
	}
	switch base {
	case ".gitignore", ".gitattributes", ".editorconfig",
		"LICENSE", "CODEOWNERS", "OWNERS",
		"go.sum", "go.mod", "Cargo.lock", "package-lock.json", "yarn.lock":
		return true
	}
	return false
}

// prHasRequiredLabel returns true when required is empty (filter disabled) or
// the PR carries at least one of the required labels. Comparison is
// case-sensitive — GitHub labels are case-preserving and operators tend to
// configure exact names.
func prHasRequiredLabel(required, prLabels []string) bool {
	if len(required) == 0 {
		return true
	}
	want := make(map[string]struct{}, len(required))
	for _, l := range required {
		if t := strings.TrimSpace(l); t != "" {
			want[t] = struct{}{}
		}
	}
	if len(want) == 0 {
		return true
	}
	for _, l := range prLabels {
		if _, ok := want[l]; ok {
			return true
		}
	}
	return false
}

// kustomizationSpec is the minimal subset of Kustomization YAML the bridge
// needs: the source (repo resolution, build scope), the path and target
// namespace, and the status fields the gates read. The inventory is parsed
// separately (inventoryRefsFromYAML).
type kustomizationSpec struct {
	// Metadata identifies the parent Kustomization (the
	// kustomize.toolkit.fluxcd.io/{name,namespace} labels it stamps on what it
	// applies) and whether it may prune this object (the prune annotation) —
	// see selfRemovalExpected.
	Metadata struct {
		Labels            map[string]string `yaml:"labels"`
		Annotations       map[string]string `yaml:"annotations"`
		DeletionTimestamp string            `yaml:"deletionTimestamp"`
		UID               string            `yaml:"uid"`
	} `yaml:"metadata"`
	Spec struct {
		SourceRef struct {
			Kind      string `yaml:"kind"`
			Name      string `yaml:"name"`
			Namespace string `yaml:"namespace"`
		} `yaml:"sourceRef"`
		// Path is the Kustomization's source path (the overlay it renders,
		// e.g. ./<root>/<component>/<env>/<region>). Resolved against the source
		// artifact to scope PR attribution to this overlay's actual build inputs
		// (see ScopeResolver / internal/source).
		Path string `yaml:"path"`
		// TargetNamespace is Flux's "deploy everything here" override. When set
		// it's the authoritative namespace the Kustomization's workloads live
		// in; when empty (the common case, where resources carry their own
		// namespace) the bridge falls back to the Kustomization name.
		TargetNamespace string `yaml:"targetNamespace"`
		// Prune: whether the Kustomization deletes objects that left its
		// build — a parent with it off never deletes a removed child.
		Prune bool `yaml:"prune"`
		// Suspend: a suspended Kustomization doesn't reconcile (or prune).
		Suspend bool `yaml:"suspend"`
		// DeletionPolicy: what deleting the Kustomization does to the objects
		// it manages (see deletesResourcesOnDeletion).
		DeletionPolicy string `yaml:"deletionPolicy"`
	} `yaml:"spec"`
	// Status.Conditions carries the Kustomization's OWN Ready condition — the
	// controller's verdict on whether the current source revision applied. The
	// inventory/pod snapshot can't see a dry-run/build/apply failure (nothing
	// changed in the cluster, so the LAST GOOD state polls all-Current); this
	// is the signal that catches it.
	Status struct {
		Conditions []struct {
			Type    string `yaml:"type"`
			Status  string `yaml:"status"`
			Reason  string `yaml:"reason"`
			Message string `yaml:"message"`
		} `yaml:"conditions"`
		// LastAppliedRevision is the source revision the controller most
		// recently APPLIED. The bridge only analyses once this matches the
		// revision the event is about — otherwise the inventory/pod snapshot
		// still describes the previously-running state (see the revision-applied
		// gate in Process).
		LastAppliedRevision string `yaml:"lastAppliedRevision"`
		// LastAttemptedRevision is the revision the controller last TRIED;
		// its Ready condition describes that attempt.
		LastAttemptedRevision string `yaml:"lastAttemptedRevision"`
	} `yaml:"status"`
}

type gitRepositorySpec struct {
	Spec struct {
		URL string `yaml:"url"`
	} `yaml:"spec"`
}

type kustomizationSnapshot struct {
	Status struct {
		Inventory struct {
			Entries []struct {
				ID string `yaml:"id"`
			} `yaml:"entries"`
		} `yaml:"inventory"`
	} `yaml:"status"`
}

func inventoryRefsFromYAML(data []byte) ([]string, error) {
	var snapshot kustomizationSnapshot
	if err := yaml.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("unmarshal kustomization yaml: %w", err)
	}
	refs := make([]string, 0, len(snapshot.Status.Inventory.Entries))
	for _, entry := range snapshot.Status.Inventory.Entries {
		if entry.ID != "" {
			refs = append(refs, entry.ID)
		}
	}
	return refs, nil
}

// helmReleaseAPIVersion is hardcoded because the inventory ID format omits
// the version. helm-controller has only ever shipped v2beta1 / v2beta2 / v2;
// fall back to v2 (current stable). If MCP discovery has surfaced a newer
// preferred version it'll route correctly anyway — flux-operator-mcp resolves
// the kind even when the apiVersion doesn't match precisely.
const helmReleaseAPIVersion = "helm.toolkit.fluxcd.io/v2"

// helmReleaseInstallPending reports whether a HelmRelease's status shows NO
// evidence of ever completing a reconcile — the genuine "first install still
// pending" case expandHelmReleaseInventories needs to distinguish from two
// look-alikes that can ALSO present an empty status.history and therefore
// can't use history-presence as the signal:
//
//   - the HelmRelease v2beta1 API, where upstream marks status.history as
//     not actively used, so an otherwise healthy, long-running v2beta1
//     release can have it permanently empty;
//   - a release mid-teardown: helm-controller calls ClearHistory() on
//     uninstall, well before the object is actually removed, so a
//     deleting-but-not-yet-gone HelmRelease also reads "no history".
//
// Both are excluded by using status.observedGeneration instead: it's the
// unreconciled sentinel (-1, or absent) ONLY before the first reconcile ever
// completes, a base Kubernetes convention every API version and lifecycle
// phase sets the same way — unlike status.history. A metadata.deletionTimestamp
// is checked directly on top, since that's the more direct "being torn down,
// not installed" signal.
func helmReleaseInstallPending(data []byte) bool {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc yaml.Node
		if err := dec.Decode(&doc); err != nil {
			return false
		}
		if pending, found := helmReleaseInstallPendingIn(&doc); found {
			return pending
		}
	}
}

// helmReleaseInstallPendingIn walks one parsed document (tolerating the same
// `kind: List` envelope extractInventoryIDs does) and evaluates the first
// resource-shaped node it finds. found is false for a document with neither
// a metadata nor a status mapping (nothing resource-like to report on).
func helmReleaseInstallPendingIn(n *yaml.Node) (pending bool, found bool) {
	if n == nil {
		return false, false
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return helmReleaseInstallPendingIn(n.Content[0])
	}
	if n.Kind != yaml.MappingNode {
		return false, false
	}
	// A `kind: List` envelope is identified by its literal `kind` value, NOT
	// by "no metadata/status at top level" — a real Kubernetes List object
	// carries its own (typically near-empty) ListMeta under `metadata`,
	// which would otherwise be mistaken for the wrapped resource's own
	// metadata and short-circuit evaluation before ever reaching `items`.
	if scalarChildValue(n, "kind") == "List" {
		items := mappingChild(n, "items")
		if items == nil || items.Kind != yaml.SequenceNode {
			return false, false
		}
		for _, item := range items.Content {
			if pending, found := helmReleaseInstallPendingIn(item); found {
				return pending, true
			}
		}
		return false, false
	}
	metadata := mappingChild(n, "metadata")
	status := mappingChild(n, "status")
	if metadata == nil && status == nil {
		return false, false
	}
	if mappingChild(metadata, "deletionTimestamp") != nil {
		return false, true
	}
	og := scalarChildValue(status, "observedGeneration")
	return og == "" || og == "-1", true
}

// helmReleaseInventoryIDs walks an arbitrary YAML payload and extracts every
// `.status.inventory.entries[*].id` value. Uses yaml.Node-based traversal
// (rather than struct unmarshal) so the parser tolerates whatever
// surrounding shape MCP wraps the resource in — extra top-level fields, a
// `kind: List` envelope, multi-document streams, history sections with
// embedded sequences that confuse `yaml.Unmarshal` into a typed struct.
//
// Returns nil for any payload that doesn't expose the path; never errors on
// "no inventory here". Genuine parse failures (malformed YAML) return an
// error so callers can log and fall back to base inventory.
func helmReleaseInventoryIDs(data []byte) ([]string, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var ids []string
	for {
		var doc yaml.Node
		if err := dec.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		ids = append(ids, extractInventoryIDs(&doc)...)
	}
	return ids, nil
}

// extractInventoryIDs descends through a parsed YAML document looking for
// resource references and emits each as a `<namespace>_<name>_<group>_<kind>`
// inventory ID. Three shapes are supported because flux-operator-mcp renders
// HelmReleases differently from how kustomize-controller stores
// Kustomizations:
//
//  1. `.status.inventory.entries[*].id`        — kustomize-controller's
//     native shape (raw IDs).
//  2. `.status.history[*].inventory[*].{...}`  — flux-operator-mcp's
//     rendered HelmRelease shape
//     (each history entry carries
//     a structured object list).
//  3. `.status.inventory[*].{...}`             — same structured shape but
//     attached directly to
//     status (defensive: some
//     controller versions).
//
// Structured shapes carry `apiVersion`, `kind`, `name`, `namespace`. We
// reconstruct the inventory ID from those — `group` is the part before `/`
// in apiVersion (empty for core v1 like Service/ConfigMap).
//
// All shapes are walked even if multiple coexist; the caller dedupes.
func extractInventoryIDs(n *yaml.Node) []string {
	if n == nil {
		return nil
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return extractInventoryIDs(n.Content[0])
	}
	status := mappingChild(n, "status")
	if status == nil {
		// `kind: List` envelope — recurse into items[*].
		if items := mappingChild(n, "items"); items != nil && items.Kind == yaml.SequenceNode {
			var out []string
			for _, item := range items.Content {
				out = append(out, extractInventoryIDs(item)...)
			}
			return out
		}
		return nil
	}
	var ids []string
	// Shape 1: status.inventory.entries[*].id
	if inv := mappingChild(status, "inventory"); inv != nil {
		if entries := mappingChild(inv, "entries"); entries != nil && entries.Kind == yaml.SequenceNode {
			for _, entry := range entries.Content {
				if id := mappingChild(entry, "id"); id != nil && id.Value != "" {
					ids = append(ids, id.Value)
				}
			}
		}
		// Shape 3: status.inventory is itself a sequence of structured refs.
		if inv.Kind == yaml.SequenceNode {
			for _, ref := range inv.Content {
				if id := inventoryIDFromStructuredRef(ref); id != "" {
					ids = append(ids, id)
				}
			}
		}
	}
	// Shape 2: status.history[*].inventory[*].{...}
	if history := mappingChild(status, "history"); history != nil && history.Kind == yaml.SequenceNode {
		for _, hentry := range history.Content {
			hinv := mappingChild(hentry, "inventory")
			if hinv == nil || hinv.Kind != yaml.SequenceNode {
				continue
			}
			for _, ref := range hinv.Content {
				if id := inventoryIDFromStructuredRef(ref); id != "" {
					ids = append(ids, id)
				}
			}
		}
	}
	return ids
}

// inventoryIDFromStructuredRef builds a `<ns>_<name>_<group>_<kind>` ID
// from a mapping with apiVersion/kind/name/namespace fields. Used for the
// MCP-rendered HelmRelease inventory shape where each entry is structured
// rather than pre-encoded. Returns "" for any entry that's missing the
// mandatory kind+name fields.
func inventoryIDFromStructuredRef(n *yaml.Node) string {
	if n == nil || n.Kind != yaml.MappingNode {
		return ""
	}
	apiVersion := scalarChildValue(n, "apiVersion")
	kind := scalarChildValue(n, "kind")
	name := scalarChildValue(n, "name")
	namespace := scalarChildValue(n, "namespace")
	if kind == "" || name == "" {
		return ""
	}
	group := ""
	if idx := strings.IndexByte(apiVersion, '/'); idx >= 0 {
		group = apiVersion[:idx]
	}
	return fmt.Sprintf("%s_%s_%s_%s", namespace, name, group, kind)
}

// scalarChildValue returns the scalar value of `key` in a mapping node, or
// "" if the key is missing or the value isn't a scalar. Convenience helper
// for the structured-ref builder above.
func scalarChildValue(n *yaml.Node, key string) string {
	child := mappingChild(n, key)
	if child == nil || child.Kind != yaml.ScalarNode {
		return ""
	}
	return child.Value
}

// mappingChild returns the value Node for `key` in a mapping, or nil if the
// node isn't a mapping or the key is missing. Mapping nodes store
// keys + values flattened into the Content slice: [k1, v1, k2, v2, …].
func mappingChild(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

// helmReleasePendingReason distinguishes WHY expandHelmReleaseInventories
// couldn't confirm a HelmRelease's children, so pendingHelmReleaseNote can
// state only what's actually known instead of always claiming the release
// is still installing — a fetch/parse failure is evidence of nothing.
type helmReleasePendingReason int

const (
	// helmReleaseReasonInstallPending: fetched and parsed fine, zero
	// children, and no evidence of ever completing a reconcile (see
	// helmReleaseInstallPending) — genuinely looks like a stalled first
	// install.
	helmReleaseReasonInstallPending helmReleasePendingReason = iota
	// helmReleaseReasonUnverified: the HelmRelease itself couldn't be
	// fetched, or its YAML couldn't be parsed. Its install state is simply
	// unknown — NOT evidence that it's still installing.
	helmReleaseReasonUnverified
)

// pendingHelmRelease names one HelmRelease expandHelmReleaseInventories
// couldn't resolve this run, and why — see helmReleasePendingReason.
type pendingHelmRelease struct {
	Name   string // "<namespace>/<name>"
	Reason helmReleasePendingReason
}

// expandHelmReleaseInventories walks the Kustomization's inventory refs and,
// for every HelmRelease, fetches the HR YAML via MCP and folds its own
// `status.inventory.entries` into the returned ref list. The expanded set is
// the base inventory + every HelmRelease's child resources — deduped, so a
// resource that somehow appears in both lists is only checked once.
//
// One level only — a HelmRelease whose chart renders another HelmRelease CR
// is not recursively expanded. Rare in practice and unbounded recursion
// would be a footgun.
//
// Per-HelmRelease MCP failures are logged WARN; the HelmRelease falls back
// to "just include itself" so the readiness loop still evaluates it. We
// never abort the whole pipeline because one HR fetch failed.
//
// The second return value names every HelmRelease expandHelmReleaseInventories
// couldn't confirm this run, tagged with why — NOT every zero-child
// HelmRelease:
//
//   - helmReleaseReasonInstallPending: fetched and parsed WITHOUT error,
//     yielded zero children, AND shows no evidence of ever completing a
//     reconcile (see helmReleaseInstallPending). Most commonly this means
//     helm-controller hasn't completed a first install yet, so
//     status.inventory is still empty: every rendered resource — the
//     workload, AND anything the chart templates itself (an
//     image-automation ImageUpdateAutomation/ImagePolicy, a Job, …) — is
//     invisible to this function until then, so the caller can otherwise
//     present a deceptively clean "N of N current" picture that's actually
//     just blind to N-minus-however-many resources. Deliberately NOT
//     reported for a release that's genuinely done reconciling but whose
//     chart renders nothing, a v2beta1 release (which doesn't populate
//     status.history at all), a release mid-teardown, or an inventory
//     shape this parser doesn't recognise — none of those are a stalled
//     first install.
//   - helmReleaseReasonUnverified: the HelmRelease couldn't be fetched or
//     its YAML couldn't be parsed. A deliberate fail-CLOSED choice — a
//     fetch/parse failure means we don't know whether the release has
//     completed a reconcile, and treating "don't know" as "not pending" is
//     exactly the gap that let a HelmRelease still in the middle of its
//     first install through as a false Success (the fetch raced the
//     object's own first status write). The worst case of treating it as
//     pending is one extra
//     Warning cycle on a one-off MCP blip; the alternative risks a silent
//     false green. But this is NOT itself evidence the release is still
//     installing — it may be long settled and simply unreadable this run,
//     for reasons that can just as easily be persistent (a standing RBAC
//     denial, a chart that always renders unparseable status) as one-off —
//     so callers must not describe it the same way as
//     helmReleaseReasonInstallPending, and must not call it transient.
//
// Returns the base list unchanged (and no pending entries) when the feature
// is disabled or when nothing in the inventory is a HelmRelease.
func (p *Processor) expandHelmReleaseInventories(ctx context.Context, baseRefs []string) ([]string, []pendingHelmRelease) {
	if !p.cfg.ReadinessExpandHelmReleases {
		return baseRefs, nil
	}
	// Fetch the HelmReleases' own inventories CONCURRENTLY (bounded), then
	// assemble in the original ref order so the output stays deterministic
	// (prompt ordering feeds the LLM provider's prompt cache). This runs on
	// EVERY readiness-poll iteration — children appear as a release
	// progresses, so the re-fetch is deliberate — which makes the sequential
	// version the dominant per-iteration cost on HR-heavy Kustomizations
	// (N HRs × one MCP round-trip each, every poll interval). The bound
	// keeps one pipeline from monopolizing the shared MCP session pool.
	children := make([][]string, len(baseRefs))
	pendingEntries := make([]*pendingHelmRelease, len(baseRefs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, ref := range baseRefs {
		parsed, err := readiness.ParseInventoryID(ref)
		// Kinds are only unique within an API group; the fetch below is
		// hardwired to helm.toolkit.fluxcd.io, so another group's
		// "HelmRelease" would always fail it and wrongly force a warning.
		if err != nil || parsed.Kind != "HelmRelease" || parsed.Group != "helm.toolkit.fluxcd.io" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, parsed readiness.InventoryRef) {
			defer wg.Done()
			defer func() { <-sem }()
			hrYAML, err := p.mcp.GetKubernetesResource(ctx, helmReleaseAPIVersion, parsed.Kind, parsed.Name, parsed.Namespace)
			if err != nil {
				// Fail CLOSED on "don't know": a flaky/denied read tells us
				// nothing about whether this HelmRelease ever finished a
				// reconcile, so don't let it imply either way. Marked
				// unverified (not installPending) — it forces the same
				// Warning, but the downstream note must not claim this
				// release is still installing, since it may be long settled.
				p.logger.Warn("inventory expansion: HelmRelease fetch failed; falling back to base inventory for this entry, marked unverified",
					"helmrelease", parsed.Namespace+"/"+parsed.Name, "error", err.Error())
				pendingEntries[i] = &pendingHelmRelease{Name: parsed.Namespace + "/" + parsed.Name, Reason: helmReleaseReasonUnverified}
				return
			}
			subRefs, err := helmReleaseInventoryIDs(hrYAML)
			if err != nil {
				p.logger.Warn("inventory expansion: HelmRelease YAML parse failed, marked unverified",
					"helmrelease", parsed.Namespace+"/"+parsed.Name, "error", err.Error())
				pendingEntries[i] = &pendingHelmRelease{Name: parsed.Namespace + "/" + parsed.Name, Reason: helmReleaseReasonUnverified}
				return
			}
			children[i] = subRefs
			if len(subRefs) > 0 {
				p.logger.Debug("inventory expansion: HelmRelease children added",
					"helmrelease", parsed.Namespace+"/"+parsed.Name, "children", len(subRefs))
			} else if helmReleaseInstallPending(hrYAML) {
				// Zero children AND no evidence of a completed reconcile
				// (see helmReleaseInstallPending): a first-install-still-pending
				// HelmRelease, not a v2beta1 release whose API version doesn't
				// populate status.history, a release mid-teardown, or an
				// inventory shape this parser missed.
				pendingEntries[i] = &pendingHelmRelease{Name: parsed.Namespace + "/" + parsed.Name, Reason: helmReleaseReasonInstallPending}
			}
		}(i, parsed)
	}
	wg.Wait()

	out := make([]string, 0, len(baseRefs))
	seen := make(map[string]struct{}, len(baseRefs))
	add := func(ref string) {
		if _, dup := seen[ref]; dup {
			return
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	var pending []pendingHelmRelease
	for i, ref := range baseRefs {
		add(ref)
		for _, sub := range children[i] {
			add(sub)
		}
		if pendingEntries[i] != nil {
			pending = append(pending, *pendingEntries[i])
		}
	}
	return out, pending
}

// promptKustomizationYAML returns the Kustomization YAML with the (potentially
// huge) status.inventory enumeration stripped out, for embedding in the LLM
// prompt. The inventory is a flat list of `<ns>_<name>_<group>_<kind>` IDs with
// no health, message, or relationship data — it carries zero root-cause signal
// (the scoped Resource Readiness section already lists the relevant resources
// WITH status), and on a cluster-wide Kustomization it can be tens of thousands
// of tokens of pure ID noise. Everything that actually drives analysis — spec
// (path, dependsOn, sourceRef), status.conditions, revisions — is kept.
//
// A one-line comment preserves the scale signal at ~zero token cost. On any
// parse/marshal failure, or when there's no inventory to trim, the original
// YAML is returned unchanged (fail-open: a slightly bloated prompt beats a
// broken one). refs is the already-parsed full inventory, used only for the
// summary counts so we don't re-walk the entries.
func promptKustomizationYAML(raw string, refs []string) string {
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
		return raw
	}
	status, ok := doc["status"].(map[string]any)
	if !ok {
		return raw
	}
	if _, hasInv := status["inventory"]; !hasInv {
		return raw
	}
	delete(status, "inventory")
	out, err := yaml.Marshal(doc)
	if err != nil {
		return raw
	}
	return strings.TrimRight(string(out), "\n") +
		fmt.Sprintf("\n# status.inventory omitted: %d resource(s) across %d namespace(s)\n",
			len(refs), distinctNamespaceCount(refs))
}

// distinctNamespaceCount counts the distinct namespaces a set of inventory IDs
// touches — the namespace field of each namespaced entry, plus the name of any
// cluster-scoped `Namespace` entry. Used only for the prompt's omitted-inventory
// summary line.
func distinctNamespaceCount(refs []string) int {
	set := make(map[string]struct{})
	for _, id := range refs {
		ref, err := readiness.ParseInventoryID(id)
		if err != nil {
			continue
		}
		switch {
		case ref.Namespace != "":
			set[ref.Namespace] = struct{}{}
		case ref.Kind == "Namespace" && ref.Name != "":
			set[ref.Name] = struct{}{}
		}
	}
	return len(set)
}

// buildPrompt emits the per-event user payload — DATA ONLY. All
// instructions / output-format rules / style guidance live in the SKILL.md
// ConfigMap and are sent as the system prompt by analyzer.Analyze. Keeping
// them in one place means we don't have to chase a duplicated set of rules
// across Go code and Helm template every time the comment shape changes.
//
// settled=false (readiness poll timeout) leads the Resource Readiness section
// with a timeout line: the poll timing out makes the outcome at least a
// warning even over an all-Current snapshot, and without the line the model
// would read that snapshot as a healthy run.
func buildPrompt(event eventv1.Event, revision, kustomizationYAML string, statuses, podStatuses []readiness.ResourceStatus, settled bool) string {
	var builder strings.Builder
	builder.WriteString("## Flux Event\n")
	fmt.Fprintf(&builder, "- Resource: %s/%s (%s)\n", event.InvolvedObject.Namespace, event.InvolvedObject.Name, event.InvolvedObject.Kind)
	fmt.Fprintf(&builder, "- Severity: %s\n", event.Severity)
	fmt.Fprintf(&builder, "- Reason: %s\n", event.Reason)
	fmt.Fprintf(&builder, "- Message: %s\n", event.Message)
	fmt.Fprintf(&builder, "- Git Revision: %s\n\n", revision)
	builder.WriteString("## Kustomization YAML\n")
	builder.WriteString(kustomizationYAML)
	// Readiness sections list ONLY the non-Current rows, prefixed with an
	// "X of N reached Current" count. The LLM's job is failure analysis, so
	// enumerating hundreds of healthy rows is pure token cost with no
	// diagnostic value (a broad Kustomization like grafana owns 1000+
	// resources). The human still sees the full per-resource table in the
	// bridge-stamped comment; the model only needs the failures + a count.
	builder.WriteString("\n\n## Resource Readiness\n")
	if !settled {
		builder.WriteString("Readiness poll timed out: at least one resource or pod was still changing state when the bridge stopped waiting, so the rows below are the last snapshot, not a settled result.\n")
	}
	writeReadinessSummary(&builder, "resources", statuses, func(r readiness.ResourceStatus) string {
		line := fmt.Sprintf("- %s/%s/%s: %s — %s", r.Kind, r.Namespace, r.Name, r.Status, r.Message)
		// Hard replica counts for workloads (Deployment/StatefulSet/…): the
		// ground truth the model must use instead of guessing availability from
		// the Deployment `Available` condition. "0/1 ready" is unambiguous.
		if r.Replicas != nil {
			line += " (" + r.Replicas.String() + ")"
		}
		return line + "\n"
	})
	if len(podStatuses) > 0 {
		// Pod kstatus snapshot, each row labeled with its owning workload
		// controller (Deployment/StatefulSet/etc., resolved through
		// ownerReferences so "Deployment/api" shows rather than the
		// implementation-detail ReplicaSet). A pod can be failing here while
		// its controller reads Current (so the controller is summarised away in
		// Resource Readiness above) — the pod row names the controller so it
		// can still be cited.
		builder.WriteString("\n## Workload Readiness\n")
		writeReadinessSummary(&builder, "pods", podStatuses, func(p readiness.ResourceStatus) string {
			ctrl := p.Controller
			if ctrl == "" {
				ctrl = "(naked)"
			}
			return fmt.Sprintf("- %s/%s (%s): %s — %s\n", p.Namespace, p.Name, ctrl, p.Status, p.Message)
		})
	}
	return builder.String()
}

// writeReadinessSummary writes a token-frugal readiness section: a one-line
// "X of N <noun> reached Current" count, then a line (via the formatter) for
// each NON-Current row only. All-Current collapses to just the count line.
// Empty input writes nothing.
func writeReadinessSummary(b *strings.Builder, noun string, statuses []readiness.ResourceStatus, line func(readiness.ResourceStatus) string) {
	total := len(statuses)
	if total == 0 {
		return
	}
	current := 0
	for _, s := range statuses {
		if s.Status == "Current" {
			current++
		}
	}
	if current == total {
		fmt.Fprintf(b, "All %d %s reached Current.\n", total, noun)
		return
	}
	fmt.Fprintf(b, "%d of %d %s reached Current. Not Current:\n", current, total, noun)
	for _, s := range statuses {
		if s.Status != "Current" {
			b.WriteString(line(s))
		}
	}
}

// kstatusReadmeURL is the upstream reference linked from the optional
// healthy-state summary line. Single source of truth so the URL doesn't
// drift between code and Markdown.
const kstatusReadmeURL = "https://github.com/kubernetes-sigs/cli-utils/tree/master/pkg/kstatus"

// pendingHelmReleaseNote renders a caveat for one or more HelmReleases whose
// rendered children weren't visible to expandHelmReleaseInventories this run
// (see its doc comment). Split by helmReleasePendingReason so the wording
// states only what's actually known — a fetch/parse failure is NOT evidence
// a release is still installing, so it gets its own paragraph rather than
// being folded into the "still installing" claim. Deterministic and separate
// from the LLM narrative, so it can't be missed, softened, or mis-worded,
// and it survives an LLM outage (the kstatus-only fallback still carries it).
func pendingHelmReleaseNote(pending []pendingHelmRelease) string {
	var installPending, unverified []string
	for _, p := range pending {
		if p.Reason == helmReleaseReasonUnverified {
			unverified = append(unverified, p.Name)
		} else {
			installPending = append(installPending, p.Name)
		}
	}
	var notes []string
	if len(installPending) > 0 {
		notes = append(notes, installPendingNote(installPending))
	}
	if len(unverified) > 0 {
		notes = append(notes, unverifiedHelmReleaseNote(unverified))
	}
	return strings.Join(notes, "\n\n")
}

// helmReleaseSubject renders the shared "HelmRelease `x`" / "HelmReleases
// `x`, `y`" subject phrase plus the matching pronoun, so every
// pendingHelmReleaseNote variant agrees in number.
func helmReleaseSubject(names []string) (subject, pronoun string) {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted) // deterministic ordering (stability key, prompt cache)
	quoted := make([]string, len(sorted))
	for i, name := range sorted {
		quoted[i] = "`" + name + "`"
	}
	if len(quoted) == 1 {
		return "HelmRelease " + quoted[0], "its"
	}
	return "HelmReleases " + strings.Join(quoted, ", "), "their"
}

// installPendingNote covers helmReleaseReasonInstallPending: fetched and
// parsed fine, but genuinely looks like a stalled first install.
func installPendingNote(names []string) string {
	subject, pronoun := helmReleaseSubject(names)
	verb := "has"
	if len(names) > 1 {
		verb = "have"
	}
	return fmt.Sprintf(
		"> ⚠️ **Incomplete picture**: %s %s not completed an initial install yet, "+
			"so %[3]s rendered resources aren't in %[3]s status and can't be evaluated here — "+
			"this includes anything the chart templates itself (e.g. an image-automation "+
			"ImageUpdateAutomation/ImagePolicy), not just the workload.",
		subject, verb, pronoun)
}

// hasInstallPendingHelmRelease reports whether any entry looks mid-install
// (as opposed to merely uninspectable). Used to pick summary wording.
func hasInstallPendingHelmRelease(pending []pendingHelmRelease) bool {
	for _, p := range pending {
		if p.Reason == helmReleaseReasonInstallPending {
			return true
		}
	}
	return false
}

// unverifiedHelmReleaseNote covers helmReleaseReasonUnverified: the
// HelmRelease itself couldn't be fetched or its content couldn't be parsed
// this run — the wording covers both, since in the parse case the read
// itself succeeded. This says nothing
// about whether the release is still installing — it may be long settled —
// so it must not reuse installPendingNote's wording. Also deliberately does
// NOT call the failure "transient": this path covers a one-off MCP blip, but
// equally a persistent RBAC denial or a chart that renders unparseable
// status every run — either of which would recur indefinitely. Calling it
// transient would hand the reader a false "this'll clear up on its own"
// signal.
func unverifiedHelmReleaseNote(names []string) string {
	subject, pronoun := helmReleaseSubject(names)
	return fmt.Sprintf(
		"> ⚠️ **Incomplete picture**: %s could not be inspected this run, so whether "+
			"%s rendered resources are up to date couldn't be verified — this is a "+
			"fetch or parse failure, not evidence of an incomplete install.",
		subject, pronoun)
}

// buildKstatusBody produces the deterministic comment body — the body
// `formatComment` will wrap with the canonical header + footer. Every
// comment starts with it; with an LLM, the model's narrative is appended
// below. The first line carries a status marker that `extractStatusMarker`
// lifts into the `# ✅/❌/⚠️ Flux Status` header.
//
// Sections emitted (in order):
//  1. Marker on its own line (lifted by formatComment into the header).
//  2. Headline summary as the first body paragraph (includes the kstatus
//     README link on healthy outcomes; failure/warning summaries omit
//     the link to keep the body tight — kstatus vocabulary lives in
//     the docs).
//  3. ## Resource Status — every inventory row.
//  4. ## Workload Status — kstatus snapshot of pods in the Kustomization's
//     own namespace, when scanPodStatuses returned anything. Surfaces
//     failing pods that controllers haven't propagated up to their
//     inventory row yet (a CrashLoopBackOff workload often shows ✅
//     on the Deployment for tens of seconds before the controller
//     flips it).
//
// The full `.status.conditions[]` list of non-Current rows is NOT rendered;
// each such row's Message column carries its most diagnostic not-True
// condition (inventoryRowMessage). The complete condition data feeds the
// LLM prompt via failure.fetchFailureContext.
//
// Overall marker rules (consider inventory AND pods):
//   - any inventory row in Failed/NotFound, OR any pod Failed  → ❌
//   - else any row not Current, OR settled=false               → ⚠️
//   - else (all Current AND settled=true)                       → ✅
//
// `settled=false` (readiness poll timeout) forces at least ⚠️ even if every
// row happens to be Current at the moment of the snapshot, because the
// terminal-state contract was not satisfied.
// scope is the Kustomization name (e.g. "grafana"). It leads the summary line
// ("Kustomization `grafana`: …") and is the value of the compact summary row's
// Kustomization column — a comment is always for one Kustomization, so this is
// its identity, stated once rather than per row. Empty scope (tests) drops the
// prefix and renders "—" in the summary-row cell.
// pendingHelmReleases names the HelmReleases whose rendered children were
// invisible to expandHelmReleaseInventories this run, tagged with why (see
// its doc comment) — it forces at least a warning outcome (never a
// downgrade of a real failure) and, when non-empty, appends a deterministic
// note below the tables so the caveat can't be missed, softened by an LLM,
// or lost when a healthy outcome would otherwise suppress the comment
// entirely (COMMENT_ON_SUCCESS=false).
func buildKstatusBody(statuses, podStatuses []readiness.ResourceStatus, podsScanned, settled, includeSummary, failedOnly bool, scope, reconcileFailedMsg, reconcileWaitingMsg string, pendingHelmReleases []pendingHelmRelease) string {
	invFailed, invNotCurrent := countStatuses(statuses)
	podFailed, podNotCurrent := countStatuses(podStatuses)
	helmReleasesPending := len(pendingHelmReleases) > 0

	// Marker always on the first line so `formatComment` can lift the
	// emoji into the canonical "# <emoji> Flux Status" header. When
	// includeSummary is true, a one-sentence headline becomes the first
	// body paragraph after the marker (and on healthy outcomes embeds
	// the kstatus README link). When false, the body jumps straight to
	// the Resource Status table — the tables alone carry the same data
	// at higher fidelity, and some operators prefer the tighter shape.
	var marker, summary string
	noun := workloadNoun(podStatuses)
	switch computeOutcome(statuses, podStatuses, settled, reconcileFailedMsg != "", reconcileWaitingMsg != "", helmReleasesPending) {
	case bridgegithub.OutcomeFailure:
		marker = statusEmojiFailure
		// A reconcile failure is the root cause and outranks the table counts:
		// the tables describe the last-applied state, which may look entirely
		// green while the analysed revision was rejected.
		if reconcileFailedMsg != "" {
			summary = "reconciliation failed: " + summaryFailureText(reconcileFailedMsg)
		} else {
			summary = failureSummary(invFailed, len(statuses), podFailed, len(podStatuses), noun)
		}
	case bridgegithub.OutcomeWarning:
		marker = statusEmojiWarning
		// A dependency wait outranks the table counts for the same reason a
		// reconcile failure does: it is the root cause of the not-green state,
		// and it self-resolves once the dependsOn target reconciles.
		switch {
		case reconcileWaitingMsg != "":
			summary = "waiting on a dependency: " + summaryFailureText(reconcileWaitingMsg)
		case invNotCurrent == 0 && podNotCurrent == 0 && settled && helmReleasesPending:
			// The tables themselves are all-Current AND settled — the ONLY
			// reason this isn't a success is that a HelmRelease's own
			// children are still invisible. warningSummary's wording assumes
			// at least one not-Current row or an unsettled poll, so calling
			// it here would produce a confusing "all good" summary paired
			// with a ⚠️ marker; state the real reason instead. Reason-aware:
			// "isn't fully visible yet" is only accurate for a release that
			// looks mid-install; a fetch/parse failure is no evidence of an
			// incomplete inventory, and "yet" would imply it's temporary.
			if hasInstallPendingHelmRelease(pendingHelmReleases) {
				summary = "every visible resource is current, but a HelmRelease's own inventory isn't fully visible yet (see note below)"
			} else {
				summary = "every visible resource is current, but a HelmRelease could not be inspected, so its inventory couldn't be verified (see note below)"
			}
		default:
			summary = warningSummary(invNotCurrent, len(statuses), podNotCurrent, len(podStatuses), settled, noun)
		}
	default:
		marker = statusEmojiHealthy
		summary = healthySummary(len(statuses), len(podStatuses), noun)
	}

	// Lead the summary with the Kustomization identity. A comment is always
	// for exactly one Kustomization, so this names it once (e.g. "grafana")
	// rather than repeating it per table row. Empty scope (tests) → no prefix.
	if scope != "" {
		summary = fmt.Sprintf("Kustomization `%s`: %s", scope, summary)
	}

	var b strings.Builder
	if includeSummary {
		fmt.Fprintf(&b, "%s\n\n%s\n\n", marker, summary)
	} else {
		fmt.Fprintf(&b, "%s\n\n", marker)
	}
	b.WriteString(renderStatusTables(statuses, podStatuses, podsScanned, failedOnly, scope))
	if helmReleasesPending {
		b.WriteString("\n\n")
		b.WriteString(pendingHelmReleaseNote(pendingHelmReleases))
	}
	return b.String()
}

// inventoryRowMessage picks the Message-column text for an inventory row. For
// a non-Current row it prefers the resource's own `.status.conditions[]` —
// the controller's actual root cause (e.g. a HelmRelease's "SourceNotReady:
// HelmChart … HelmRepository … not found") — over kstatus's terse summary
// (e.g. "generation 1, observed -1"), which throws that reason away. The
// readiness checker already populated Conditions during the poll, so this is
// free, and it lets the table stand alone without a separate conditions
// section (markdownTableCell escapes the pipe/newline). Current rows and rows
// with no usable condition keep the kstatus message.
func inventoryRowMessage(r readiness.ResourceStatus) string {
	if r.Status == "Current" {
		return r.Message
	}
	if cm := notReadyConditionMessage(r.Conditions); cm != "" {
		return cm
	}
	return r.Message
}

// abnormalTrueConditions are condition types whose True status signals a
// problem (kstatus's "abnormal-true" polarity), so False is their healthy
// state. Drifted is set by Flux's helm-controller; Stalled and Reconciling are
// the kstatus standard conditions Flux CRs publish.
var abnormalTrueConditions = map[string]bool{
	"Drifted":     true,
	"Stalled":     true,
	"Reconciling": true,
}

// helmRemediationReasons are the Ready reasons helm-controller sets after it
// successfully remediated a failed install/upgrade: they describe the
// remediation, not the failure that caused it. RollbackFailed and
// UninstallFailed are deliberately absent: a failed remediation is itself the
// problem worth leading with.
var helmRemediationReasons = map[string]bool{
	"RollbackSucceeded":  true,
	"UninstallSucceeded": true,
}

// conditionSignalsProblem reports whether a condition is in its unhealthy
// state, honouring per-type polarity: abnormal-true conditions signal a problem
// unless False, all others unless True. Unknown counts as a problem either way.
func conditionSignalsProblem(c readiness.Condition) bool {
	if abnormalTrueConditions[c.Type] {
		return c.Status != "False"
	}
	return c.Status != "True"
}

// notReadyConditionMessage formats the most diagnostic unhealthy condition of
// a resource as "Reason: Message" (or whichever parts are present). It prefers
// the Flux `Ready` condition — the canonical "why" on Flux CRs — falling back
// to the first other unhealthy condition. Polarity is per type (see
// conditionSignalsProblem), so e.g. a HelmRelease with a stale Ready=True and
// Drifted=False/NoDriftDetected keeps kstatus's generation message instead of
// showing "no drift" as its cause, while Stalled=True is shown. Returns "" when
// every condition is healthy or none carry text.
//
// Exception: after a failed upgrade/install, helm-controller's remediation
// writes its own outcome into Ready (e.g. Ready=False/RollbackSucceeded "Helm
// rollback ... succeeded"), which reads like a success next to a Failed status.
// Then the failure leads — Stalled (retries exhausted), else Released (the
// failed action) — and the remediation follows as context. The cause is picked
// by type, not list order: with retries left, Reconciling=True comes first but
// only repeats the rollback text.
func notReadyConditionMessage(conds []readiness.Condition) string {
	// pick returns the first unhealthy condition with text of the given type,
	// or of any type when typ is "".
	pick := func(typ string) *readiness.Condition {
		for i := range conds {
			c := &conds[i]
			if !conditionSignalsProblem(*c) || (c.Reason == "" && c.Message == "") {
				continue
			}
			if typ == "" || c.Type == typ {
				return c
			}
		}
		return nil
	}
	if ready := pick("Ready"); ready != nil {
		if helmRemediationReasons[ready.Reason] {
			for _, typ := range []string{"Stalled", "Released"} {
				if cause := pick(typ); cause != nil {
					return formatCondition(*cause) + "; " + formatCondition(*ready)
				}
			}
		}
		return formatCondition(*ready)
	}
	if c := pick(""); c != nil {
		return formatCondition(*c)
	}
	return ""
}

// formatCondition renders a condition as "Reason: Message", or whichever part
// is present.
func formatCondition(c readiness.Condition) string {
	switch {
	case c.Reason != "" && c.Message != "":
		return c.Reason + ": " + c.Message
	case c.Message != "":
		return c.Message
	default:
		return c.Reason
	}
}

// llmMissingFailureSections reports whether an analysis reply for a
// non-healthy run violates the SKILL contract that requires both a
// `## Root Cause` and a `## Next steps to fix` section. Case-insensitive
// heading match; "## Next steps" alone is accepted so minor phrasing drift
// doesn't trigger a pointless retry.
func llmMissingFailureSections(reply string) bool {
	lower := strings.ToLower(reply)
	return !strings.Contains(lower, "## root cause") || !strings.Contains(lower, "## next steps")
}

// stripFailureSections removes the `## Root Cause` and `## Next steps to fix`
// sections — each heading plus everything up to the next `#`/`##` heading —
// from an LLM reply. Headings inside fenced or indented code blocks are
// quoted text, not structure, and don't count: per CommonMark a heading has at
// most three leading spaces, so a tab or four spaces makes it code.
func stripFailureSections(reply string) string {
	lines := strings.Split(reply, "\n")
	out := lines[:0]
	var fences markdown.Tracker
	dropping := false
	for _, line := range lines {
		if !fences.Line(line) {
			if level, title, ok := atxHeading(line); ok && level <= 2 {
				dropping = level == 2 && (strings.HasPrefix(title, "root cause") || strings.HasPrefix(title, "next steps"))
			}
		}
		if !dropping {
			out = append(out, line)
		}
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// atxHeading parses line as a CommonMark ATX heading: at most three leading
// spaces (a tab or more makes it indented code), a run of one to six `#`, then
// a space, a tab or the end of the line. It returns the level and the
// lower-cased title.
func atxHeading(line string) (level int, title string, ok bool) {
	rest := strings.TrimLeft(line, " ")
	if len(line)-len(rest) > 3 {
		return 0, "", false
	}
	level = len(rest) - len(strings.TrimLeft(rest, "#"))
	rest = rest[level:]
	if level < 1 || level > 6 || (rest != "" && rest[0] != ' ' && rest[0] != '\t') {
		return 0, "", false
	}
	return level, strings.ToLower(strings.TrimSpace(rest)), true
}

// boxDrawingRunes is the decoration alphabet stripBoxDecoration removes:
// Unicode box-drawing characters plus the rounded corners.
const boxDrawingRunes = "─│┌┐└┘├┤┬┴┼═║╔╗╚╝╠╣╦╩╬╭╮╰╯"

// stripBoxDecoration undoes a failure mode observed in production: the
// SKILL's slot diagram drew the reply slot with box-drawing characters, and a
// model imitated the art — wrapping its section headings in ┌─┐ │…│ └─┘
// boxes, which renders as stray "table borders" and (worse) hides the `##`
// headings from Markdown. The skill now forbids decoration, but models drift,
// so this strips it deterministically: lines consisting solely of
// box-drawing runes are dropped, and lines wrapped in │…│ / ║…║ are unwrapped
// to their content. Tables (ASCII `|`), prose, and FENCED CODE BLOCKS pass
// through untouched — box-drawing inside ``` / ~~~ fences is quoted evidence
// (log excerpts, tree output), not decoration. Runs BEFORE the
// failure-section check so a boxed heading is judged by its unwrapped form.
func stripBoxDecoration(reply string) string {
	if !strings.ContainsAny(reply, boxDrawingRunes) {
		return reply
	}
	lines := strings.Split(reply, "\n")
	out := lines[:0]
	var fences markdown.Tracker
	for _, line := range lines {
		if fences.Line(line) {
			out = append(out, line)
			continue
		}
		t := strings.TrimSpace(line)
		if t != "" && strings.Trim(t, boxDrawingRunes+" \t") == "" {
			continue // pure decoration: ┌────┐, └────┘, ───, …
		}
		for _, edge := range []string{"│", "║"} {
			if strings.HasPrefix(t, edge) && strings.HasSuffix(t, edge) && len(t) > 2*len(edge) {
				line = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(t, edge), edge))
				break
			}
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// dropPrunedRows filters out inventory rows that read NotFound only because
// Flux pruned them: the readiness poll runs against the inventory snapshot
// taken at event time, so a resource the PR intentionally DELETED stays in
// the polled set (and reads NotFound) even after the reconcile has pruned it
// from the cluster AND from the live inventory. Reporting that as ❌ punishes
// exactly the change the PR author meant to make.
//
// The filter re-reads the Kustomization's inventory once and drops a row iff:
//   - it polls NotFound,
//   - it CAME from the event-time inventory (HelmRelease-expanded children are
//     never inventory entries, so they're exempt — a NotFound child is a real
//     signal), and
//   - the fresh inventory no longer lists it (pruned).
//
// Guards: a re-read failure keeps every row (fail-open to the current
// behavior), and an EMPTY fresh inventory is only trusted when the fetched
// object's own Ready condition is True — a PR deleting the LAST resources of
// a Kustomization legitimately leaves the post-prune inventory empty (and a
// completed reconcile vouches for it), while a failing or mid-reconcile
// Kustomization can transiently report no inventory, where acting on it would
// silently drop every genuine NotFound failure at once.
// Identity matching uses (kind, namespace, name); the inventory ID's group is
// not carried on ResourceStatus rows, and a same-kind+name+namespace clash
// across API groups within one Kustomization is theoretical.
func (p *Processor) dropPrunedRows(ctx context.Context, event eventv1.Event, originalRefs []string, statuses []readiness.ResourceStatus) []readiness.ResourceStatus {
	anyNotFound := false
	for _, r := range statuses {
		if r.Status == "NotFound" {
			anyNotFound = true
			break
		}
	}
	if !anyNotFound {
		return statuses
	}
	key := func(kind, ns, name string) string { return kind + "|" + ns + "|" + name }
	refSet := func(ids []string) map[string]struct{} {
		set := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			if ref, err := readiness.ParseInventoryID(id); err == nil {
				set[key(ref.Kind, ref.Namespace, ref.Name)] = struct{}{}
			}
		}
		return set
	}
	freshYAML, err := p.mcp.GetKustomizationStatus(ctx, event.InvolvedObject.Name, event.InvolvedObject.Namespace)
	if err != nil {
		p.logger.Warn("inventory re-read failed; keeping NotFound rows (best-effort prune detection)",
			"name", event.InvolvedObject.Name, "error", err.Error())
		return statuses
	}
	freshRefs, err := inventoryRefsFromYAML(freshYAML)
	if err != nil {
		p.logger.Warn("inventory re-read unparseable; keeping NotFound rows (best-effort prune detection)",
			"name", event.InvolvedObject.Name, "error", err.Error())
		return statuses
	}
	if len(freshRefs) == 0 && !kustomizationReadyTrue(freshYAML) {
		p.logger.Debug("inventory re-read empty without Ready=True; not trusted for prune detection, keeping NotFound rows",
			"name", event.InvolvedObject.Name)
		return statuses
	}
	original := refSet(originalRefs)
	fresh := refSet(freshRefs)
	// In-place filter, reusing statuses' backing array: the caller replaces
	// its only reference with our return value, so aliasing the input is safe
	// and the kept prefix never overtakes the read cursor.
	kept := statuses[:0]
	var dropped []string
	for _, r := range statuses {
		k := key(r.Kind, r.Namespace, r.Name)
		_, wasInventory := original[k]
		_, stillInventory := fresh[k]
		if r.Status == "NotFound" && wasInventory && !stillInventory {
			dropped = append(dropped, r.Kind+"/"+r.Namespace+"/"+r.Name)
			continue
		}
		kept = append(kept, r)
	}
	if len(dropped) > 0 {
		p.logger.Info("dropped pruned inventory rows: deleted from the Kustomization, not failures",
			"name", event.InvolvedObject.Name, "count", len(dropped), "resources", strings.Join(dropped, ", "))
	}
	return kept
}

// helmReleaseRows returns the "<namespace>/<name>" of every HelmRelease row.
func helmReleaseRows(statuses []readiness.ResourceStatus) map[string]struct{} {
	out := make(map[string]struct{})
	for _, r := range statuses {
		if r.Kind == "HelmRelease" {
			out[r.Namespace+"/"+r.Name] = struct{}{}
		}
	}
	return out
}

// withoutPrunedHelmReleases removes pending HelmReleases whose row was in
// before but is no longer in after (dropPrunedRows removed it as pruned).
func withoutPrunedHelmReleases(pending []pendingHelmRelease, before map[string]struct{}, after []readiness.ResourceStatus) []pendingHelmRelease {
	remaining := helmReleaseRows(after)
	return slices.DeleteFunc(pending, func(h pendingHelmRelease) bool {
		_, wasRow := before[h.Name]
		_, isRow := remaining[h.Name]
		return wasRow && !isRow
	})
}

// kustomizationReadyTrue reports whether the Kustomization YAML's own Ready
// condition is True — i.e. the controller vouches that the last reconcile
// (apply AND prune) completed. Unparseable YAML, no conditions, or
// Ready=False/Unknown all return false (don't vouch).
func kustomizationReadyTrue(data []byte) bool {
	var k kustomizationSpec
	if err := yaml.Unmarshal(data, &k); err != nil {
		return false
	}
	for _, c := range k.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

// reconcileFailureMessage's context: the inventory/pod snapshot polls what's
// RUNNING — the last successfully-applied state. When the new revision fails
// at dry-run/build/apply, nothing in the cluster changes, the old state polls
// all-Current, and without the controller's own verdict the bridge reported ✅
// for a deploy that was rejected outright (observed in practice: an
// admission-webhook dry-run rejection was labelled flux-success, and
// DEDUPE_REEVAL_FAILED_ONLY then skipped every retry because the head was
// "previously healthy"). Two sources, either suffices: the triggering event's
// `error` severity, or the fetched Kustomization's Ready condition being
// False (covers analyses triggered by non-error events while failing).

// dependencyNotReadyReason is Flux's condition/event reason while a
// Kustomization WAITS for `dependsOn` targets — a normal, self-resolving
// ordering state, not a failure.
const dependencyNotReadyReason = "DependencyNotReady"

// reconcileFailureMessage classifies the Kustomization's OWN reconcile state,
// independent of how healthy the live inventory looks. Returns
// (failedMsg, waitingMsg) — at most one non-empty:
//
//   - failedMsg: the reconcile FAILED (error event or Ready=False) — dry-run
//     rejection, build/apply error. The analysed revision did not apply; the
//     outcome must be ❌ however green the (last-good) snapshot polls.
//   - waitingMsg: the reconcile is WAITING on a dependency
//     (reason DependencyNotReady, from either source). The Kustomization
//     hasn't failed — it just hasn't run yet — so this is ⚠️, not ❌: flagging
//     it flux-failed punished every PR behind a slow dependency chain
//     (observed in practice waiting for flux-system/cluster-base).
//
// Both empty: the controller has no complaint; the snapshot speaks.
// revisionMismatch reports a CONFIDENT mismatch between the Kustomization's
// lastAppliedRevision and the analysed revision: both must yield a non-empty
// SHA token AND those tokens must differ. Flux revision strings carry a source
// prefix ("dev@sha1:<sha>", "main@sha1:<sha>", "sha256:<digest>",
// "v1.2.3@sha256:<digest>"); the trailing token is the identity.
//
// A blank or unparseable revision on EITHER side (empty string, or one ending
// in ':' that extracts to an empty token) is deliberately NOT a mismatch — the
// revision gate must fall through to analysis on those rather than deferring
// indefinitely on a value that can never compare equal.
func revisionMismatch(appliedRevision, analysedRevision string) bool {
	a := revisionSHA(appliedRevision)
	b := revisionSHA(analysedRevision)
	return a != "" && b != "" && a != b
}

// revisionSHA extracts the commit/digest token from a Flux revision string —
// the part after the last ':' ("dev@sha1:abc" -> "abc"), or the whole (trimmed)
// string when there's no prefix (a bare SHA, as the webhook hands to Process).
func revisionSHA(rev string) string {
	rev = strings.TrimSpace(rev)
	if i := strings.LastIndexByte(rev, ':'); i >= 0 {
		return rev[i+1:]
	}
	return rev
}

// reconcileFailureMessage returns the controller's own failure (or dependency
// wait) for the analysed revision: from an error event, else from the
// Kustomization's Ready=False condition. That condition describes the LAST
// ATTEMPT, so it is ignored when lastAttemptedRevision confidently names a
// different revision — an event for R1 processed after R2 started failing
// must not pin R2's failure on R1's PRs. A blank or unparseable attempted
// revision keeps the condition (same stance as the revision-applied gate).
func reconcileFailureMessage(event eventv1.Event, kspec kustomizationSpec, analysedRevision string) (failedMsg, waitingMsg string) {
	if event.Severity == eventv1.EventSeverityError {
		msg := strings.TrimSpace(event.Message)
		// Classify by reason BEFORE picking an empty-message placeholder, so
		// a message-less DependencyNotReady event gets waiting wording, not
		// the failure placeholder.
		if event.Reason == dependencyNotReadyReason {
			if msg == "" {
				msg = "a dependsOn target is not ready (" + dependencyNotReadyReason + ")"
			}
			return "", msg
		}
		if msg == "" {
			msg = "reconciliation failed (error event with no message)"
		}
		return msg, ""
	}
	if revisionMismatch(kspec.Status.LastAttemptedRevision, analysedRevision) {
		return "", ""
	}
	for _, c := range kspec.Status.Conditions {
		if c.Type == "Ready" && c.Status == "False" {
			msg := strings.TrimSpace(c.Message)
			if msg == "" {
				msg = c.Reason
			}
			if msg == "" {
				msg = "Ready condition is False"
			}
			if c.Reason == dependencyNotReadyReason {
				return "", msg
			}
			return msg, ""
		}
	}
	return "", ""
}

// summaryFailureText compacts a (potentially multi-line, KB-sized) controller
// failure message into a single summary-friendly line. The full message still
// reaches the LLM prompt via the event payload; the summary only needs the
// lead of the root cause.
func summaryFailureText(msg string) string {
	return markdownSafe(compactLine(msg, 300))
}

// compactLine collapses all whitespace runs (newlines included) in msg to
// single spaces and cuts the result to maxRunes, marking a cut with "…". The
// cut is on a character boundary, so a multi-byte character never ends up
// split into invalid UTF-8. No Markdown escaping: callers escape for where
// the text lands.
func compactLine(msg string, maxRunes int) string {
	msg = strings.Join(strings.Fields(msg), " ")
	if utf8.RuneCountInString(msg) > maxRunes {
		msg = string([]rune(msg)[:maxRunes]) + "…"
	}
	return msg
}

// markdownSafeReplacer backs markdownSafe.
var markdownSafeReplacer = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "@", "@\u200b")

// markdownSafe neutralises untrusted text — controller and kstatus messages,
// which can quote user input, and which a forged webhook event can carry —
// for inline use in the comment. HTML is escaped, so a `<!--` can't hide the
// rest of the comment and no markup gets injected; `@` gets a zero-width
// space after it, so `@user` or `@org/team` in a message notifies nobody.
func markdownSafe(s string) string {
	return markdownSafeReplacer.Replace(s)
}

// mentionPattern matches a GitHub @-mention: `@` at the start or after a
// non-word character, followed by a login character. An email address or an
// image digest (`app@sha256:…`) has a word character before the `@`, doesn't
// match, and isn't linked by GitHub either.
var mentionPattern = regexp.MustCompile(`(^|\W)@(\w)`)

// neutralizeMentions puts a zero-width space after the `@` of every
// @-mention in the LLM reply, as markdownSafe does for controller messages.
// The reply quotes untrusted text (pod logs, event messages, PR-authored
// YAML), and a quoted `@user` or `@org/team` would otherwise notify them from
// the bridge's comment. markdownSafe itself doesn't fit: its HTML escaping
// would break the reply's Markdown (blockquotes, `<`/`>` in code). Fenced
// blocks and code spans are left alone — GitHub doesn't link mentions there,
// and an inserted character would corrupt a quoted value such as a cron
// `@daily` the reader is meant to copy.
func neutralizeMentions(reply string) string {
	if !strings.Contains(reply, "@") {
		return reply
	}
	lines := strings.Split(reply, "\n")
	var fences markdown.Tracker
	for i, line := range lines {
		if fences.Line(line) || !strings.Contains(line, "@") {
			continue
		}
		lines[i] = mapOutsideCodeSpans(line, func(text string) string {
			return mentionPattern.ReplaceAllString(text, "${1}@\u200b${2}")
		})
	}
	return strings.Join(lines, "\n")
}

// mapOutsideCodeSpans applies f to the parts of a Markdown line outside
// inline code spans. A span opens with a run of backticks and closes at the
// next run of the same length; an unmatched run is literal text.
func mapOutsideCodeSpans(line string, f func(string) string) string {
	var b strings.Builder
	text := 0 // start of the pending non-code segment
	for i := 0; i < len(line); {
		if line[i] != '`' {
			i++
			continue
		}
		open := i
		for i < len(line) && line[i] == '`' {
			i++
		}
		n := i - open
		end := backtickRun(line[i:], n)
		if end < 0 {
			continue // unmatched: literal backticks
		}
		b.WriteString(f(line[text:open]))
		i += end + n
		b.WriteString(line[open:i])
		text = i
	}
	b.WriteString(f(line[text:]))
	return b.String()
}

// backtickRun returns the index of the first run of exactly n backticks in
// s, or -1.
func backtickRun(s string, n int) int {
	for i := 0; i < len(s); {
		if s[i] != '`' {
			i++
			continue
		}
		start := i
		for i < len(s) && s[i] == '`' {
			i++
		}
		if i-start == n {
			return start
		}
	}
	return -1
}

// computeOutcome classifies the per-event analysis into one of three
// buckets, matching the marker-selection rules buildKstatusBody applies:
//
//   - any inventory row in Failed/NotFound, OR any pod Failed → failure
//   - else any row not Current, OR settled=false, OR a HelmRelease's
//     children are still invisible (helmReleasesPending)      → warning
//   - else (all Current AND settled=true)                     → success
//
// helmReleasesPending forces at least a warning the same way settled=false
// does: a HelmRelease that hasn't completed its first install yet leaves its
// rendered children (workload AND anything the chart templates itself, e.g.
// an image-automation ImageUpdateAutomation) outside this function's view
// entirely, so an all-Current table can't be trusted as a genuine success —
// it's just blind to part of the release. It never DOWNGRADES a real
// failure, same as settled=false.
//
// Shared with the PR-label reconciliation so the label state mirrors
// the per-comment marker exactly — a comment with ❌ always pairs with
// `flux-failed`, ✅ with `flux-success`, ⚠️ with neither.
func computeOutcome(statuses, podStatuses []readiness.ResourceStatus, settled, reconcileFailed, reconcileWaiting, helmReleasesPending bool) bridgegithub.Outcome {
	invFailed, invNotCurrent := countStatuses(statuses)
	podFailed, podNotCurrent := countStatuses(podStatuses)
	switch {
	case reconcileFailed:
		// The Kustomization's own reconcile failed (dry-run/build/apply) — the
		// analysed revision never reached the cluster, so however green the
		// LIVE inventory polls, the deploy under review is a failure.
		return bridgegithub.OutcomeFailure
	case invFailed > 0 || podFailed > 0:
		return bridgegithub.OutcomeFailure
	case reconcileWaiting || invNotCurrent > 0 || podNotCurrent > 0 || !settled || helmReleasesPending:
		// reconcileWaiting: the Kustomization is queued behind a dependsOn
		// target (DependencyNotReady) — not green yet, but nothing failed;
		// the wait self-resolves when the dependency reconciles.
		return bridgegithub.OutcomeWarning
	default:
		return bridgegithub.OutcomeSuccess
	}
}

// rolloutInProgressNote is appended to the LLM payload when rolloutInProgress
// holds. The SKILL keys its "Rollout in progress" workflow on this heading.
const rolloutInProgressNote = "## Rollout In Progress\n" +
	"Nothing has failed: every non-Current resource and pod above is InProgress with no failure reason, " +
	"and no pod is currently crash-looping, restarting without becoming ready, or stuck Pending past its grace period. " +
	"The rollout had not finished when the bridge stopped waiting."

// rolloutInProgress reports whether a warning outcome is nothing more than a
// rollout the readiness poll didn't see finish: no reconcile wait, no blind
// HelmRelease, a successful pod scan, and every non-Current resource and pod
// reads InProgress without a failure reason.
//
// Pods that restarted, crashed, failed to pull or sat Pending past the grace
// are already Failed (isFailingPod), so they never get here — but only if the
// pods were actually seen (a pod that restarted and recovered to Ready reads
// Current, the same as on a healthy run). A failed or disabled pod scan leaves
// podStatuses empty and proves nothing. The scan only covers podNamespace, so
// any non-Current row in another namespace fails closed: whether its kind
// owns pods (built-in workloads, a CRD whose operator runs pods) can't be told
// from the row, and its pods were never seen. Cluster-scoped rows own no pods.
//
// The condition check catches the InProgress rows that DO carry a failure:
// Ready=False (a normal upgrade or reconcile reads Ready=Unknown; False means
// e.g. UpgradeFailed or SourceNotReady), Stalled=True, a HelmRelease retrying
// (ProgressingWithRetry), or a Deployment with ReplicaFailure / FailedCreate.
// A rollout that later stalls still surfaces: Helm's timeout turns the
// HelmRelease Failed and the next evaluation reports ❌.
func rolloutInProgress(outcome bridgegithub.Outcome, statuses, podStatuses []readiness.ResourceStatus, podNamespace string, podsScanned, reconcileWaiting, helmReleasesPending bool) bool {
	if outcome != bridgegithub.OutcomeWarning || !podsScanned || reconcileWaiting || helmReleasesPending {
		return false
	}
	for _, rows := range [][]readiness.ResourceStatus{statuses, podStatuses} {
		for _, r := range rows {
			if r.Status == "Current" {
				continue
			}
			if r.Status != "InProgress" {
				return false
			}
			if r.Namespace != "" && r.Namespace != podNamespace {
				return false
			}
			for _, c := range r.Conditions {
				if (c.Type == "Ready" && c.Status == "False") || (c.Type == "Stalled" && c.Status == "True") ||
					c.Reason == "ProgressingWithRetry" ||
					strings.HasPrefix(c.Reason, "Failed") || strings.HasSuffix(c.Reason, "Failed") ||
					strings.HasSuffix(c.Reason, "Exceeded") {
					return false
				}
			}
		}
	}
	return true
}

// renderStatusTables produces the deterministic `## Resource Status` and
// (optionally) `## Workload Status` sections from kstatus data. The output
// is identical regardless of LLM mode: the no-LLM path stamps this directly
// after the marker/summary, and the LLM-driven path appends it after the
// model's analysis so reviewers always see the same canonical readiness
// rendering — the LLM can't drift the columns, miss a row, or hallucinate a
// status. Rows in `podStatuses` are expected pre-sorted by
// (namespace, controller, name) (see scanPodStatuses).
//
// `podsScanned` distinguishes the two empty-podStatuses cases: when the scan
// ran and conclusively found zero pods, the Workload Status section is still
// emitted with an explicit "no pods running" note (a workload that declares a
// Deployment but has no pods is a signal reviewers want to see, not a blank).
// When the scan didn't run (pods disabled, over the namespace cap, or a fetch
// failed), the section is omitted entirely — we don't assert "no pods" off data
// we never saw.
//
// `failedOnly` (config.comment.failedResourcesOnly) compacts BOTH the
// Resource Status and Workload Status sections for large inventories /
// pod-heavy namespaces:
//   - when something is non-`Current`, only the failing rows are tabled —
//     same detailed columns as the full table (each row carries its own
//     Namespace column, so no separate `Namespace`-kind rows are kept);
//   - when everything is `Current`, the per-resource/per-pod rows collapse to
//     a single compact "all Current" summary row per section, keeping the
//     table format rather than dropping to prose.
//
// The overall ✅/⚠️/❌ marker and summary still reflect the FULL inventory and
// pod set — only the table rows are filtered.
func renderStatusTables(statuses, podStatuses []readiness.ResourceStatus, podsScanned, failedOnly bool, scope string) string {
	var b strings.Builder

	b.WriteString("## Resource Status\n")
	if failedOnly && !anyNotCurrent(statuses) {
		writeCurrentSummaryRow(&b, "Resources", scope, len(statuses))
	} else {
		shown := statuses
		if failedOnly {
			// Show only the non-Current (failing/in-progress) rows. The
			// namespace of each is already in the row's Namespace column, so we
			// don't keep a separate `Namespace`-kind row for context — it'd be
			// redundant (and with whole-inventory reporting would add one row
			// per managed namespace).
			shown = shown[:0:0]
			for _, r := range statuses {
				if r.Status != "Current" {
					shown = append(shown, r)
				}
			}
		}
		writeResourceRows(&b, shown)
	}

	// "Workload Status" rather than "Pod Status" because each pod is labeled
	// with its workload controller (Deployment/StatefulSet/etc., resolved up
	// the Pod → ReplicaSet → Deployment chain).
	switch {
	case len(podStatuses) == 0 && podsScanned:
		// Scanned conclusively and found zero pods — surface it explicitly so
		// reviewers can tell "workload has no pods" apart from "didn't check".
		b.WriteString("\n## Workload Status\nNo pods running.\n")
	case len(podStatuses) == 0:
		// Scan didn't run (pods disabled / over cap / fetch failed) — omit.
	case failedOnly && !anyNotCurrent(podStatuses):
		b.WriteString("\n## Workload Status\n")
		writeCurrentSummaryRow(&b, "Pods", scope, len(podStatuses))
	default:
		shown := podStatuses
		if failedOnly {
			shown = shown[:0:0]
			for _, r := range podStatuses {
				if r.Status != "Current" {
					shown = append(shown, r)
				}
			}
		}
		b.WriteString("\n## Workload Status\n")
		writePodRows(&b, shown)
	}
	return b.String()
}

// writeCurrentSummaryRow emits a compact one-row "all Current" table for the
// failed-only rendering when nothing in the section is failing: keeps the
// table format but collapses N per-resource/per-pod rows into a single row.
// The first column is the Kustomization (the comment's identity) rather than a
// namespace list — a comment is always for one Kustomization, and the per-row
// namespace is an implementation detail of where those resources happen to
// live (the same Kustomization can scatter resources across many namespaces).
// Empty scope → "—".
func writeCurrentSummaryRow(b *strings.Builder, noun, scope string, count int) {
	cell := scope
	if cell == "" {
		cell = "—"
	}
	fmt.Fprintf(b, "| Kustomization | %s | Status |\n|---|---|---|\n", noun)
	fmt.Fprintf(b, "| %s | %d | %s Current |\n", markdownTableCell(cell), count, statusEmojiHealthy)
}

// writePodRows emits the `## Workload Status` table header and one row per
// workload status row. Almost every row is a Pod; the cron-run fallback contributes Job rows
// (a CronJob run whose pods were already pruned, judged from the Job object —
// see cronRunFallbackRows), labeled with their kind in the Pod column so they
// can't be mistaken for a pod name.
func writePodRows(b *strings.Builder, rows []readiness.ResourceStatus) {
	b.WriteString("| Namespace | Controller | Pod | Status | Message |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, r := range rows {
		name := r.Name
		if r.Kind != "" && r.Kind != "Pod" {
			name = r.Kind + " " + r.Name
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s %s | %s |\n",
			markdownTableCell(r.Namespace),
			markdownTableCell(controllerCell(r.Controller)),
			markdownTableCell(name),
			emojiForStatus(r.Status),
			markdownTableCell(r.Status),
			markdownTableCell(r.Message),
		)
	}
}

// writeResourceRows emits the `## Resource Status` table header and one row
// per status. Shared by the full and failed-only renderings.
func writeResourceRows(b *strings.Builder, rows []readiness.ResourceStatus) {
	b.WriteString("| Namespace | Kind | Name | Status | Message |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, r := range rows {
		fmt.Fprintf(b, "| %s | %s | %s | %s %s | %s |\n",
			markdownTableCell(r.Namespace),
			markdownTableCell(r.Kind),
			markdownTableCell(r.Name),
			emojiForStatus(r.Status),
			markdownTableCell(r.Status),
			markdownTableCell(inventoryRowMessage(r)),
		)
	}
}

// anyNotCurrent reports whether any row is in a non-`Current` kstatus.
func anyNotCurrent(statuses []readiness.ResourceStatus) bool {
	for _, r := range statuses {
		if r.Status != "Current" {
			return true
		}
	}
	return false
}

// controllerCell formats the Controller field for rendering — empty values
// (naked pods with no workload owner) get a visible "—" placeholder so the
// table column stays aligned and the reader knows the omission is intentional.
func controllerCell(controller string) string {
	if controller == "" {
		return "—"
	}
	return controller
}

// countStatuses classifies a status slice into (failed-or-not-found, not-Current).
// `failed` is a subset of `notCurrent`. Used by the overall-marker decision so
// the inventory + pod tallies can be considered jointly.
func countStatuses(rows []readiness.ResourceStatus) (failed, notCurrent int) {
	for _, r := range rows {
		switch r.Status {
		case "Failed", "NotFound":
			failed++
			notCurrent++
		case "Current":
			// healthy
		default:
			notCurrent++
		}
	}
	return failed, notCurrent
}

// failureSummary, warningSummary, and healthySummary compose the optional
// headline summary line emitted when CommentIncludeSummary=true. Each
// branch reflects the overall-marker rule that picked it: failures wins
// over warnings wins over healthy. The healthy summary embeds the kstatus
// README link so a reader scanning the green-path comment has a one-click
// pointer to the upstream vocabulary; failure/warning summaries skip the
// link (the comment is already busy with diagnostics).

func failureSummary(invFailed, invTotal, podFailed, podTotal int, noun string) string {
	switch {
	case invFailed > 0 && podFailed > 0:
		return fmt.Sprintf("%d of %d resource(s) and %d of %d %s(s) failed.",
			invFailed, invTotal, podFailed, podTotal, noun)
	case invFailed > 0:
		return fmt.Sprintf("%d of %d resource(s) failed to reconcile.", invFailed, invTotal)
	default:
		return fmt.Sprintf("%d of %d %s(s) failed.", podFailed, podTotal, noun)
	}
}

// warningSummary states what kept the outcome from green. An unsettled poll
// is reported as "not confirmed", not "timed out": it is also unsettled when
// only pods were still settling, or when a resource the PR deleted was still
// being torn down — and the counts cover pods too, so the line never claims
// a timeout over "0 of N" pending resources.
func warningSummary(invNotCurrent, invTotal, podNotCurrent, podTotal int, settled bool, noun string) string {
	var counts string
	switch {
	case invNotCurrent > 0 && podNotCurrent > 0:
		counts = fmt.Sprintf("%d of %d resource(s) and %d of %d %s(s) not yet `Current`.",
			invNotCurrent, invTotal, podNotCurrent, podTotal, noun)
	case invNotCurrent > 0:
		counts = fmt.Sprintf("%d of %d resource(s) not yet `Current`.", invNotCurrent, invTotal)
	case podNotCurrent > 0 || settled:
		counts = fmt.Sprintf("%d of %d %s(s) not yet `Current`.", podNotCurrent, podTotal, noun)
	}
	switch {
	case settled:
		return counts
	case counts == "":
		return "Readiness not confirmed before the poll ended, although the last snapshot reads `Current`."
	default:
		return "Readiness not confirmed: " + counts
	}
}

// healthySummary renders the all-Current headline for the whole Kustomization
// inventory. The leading "Kustomization `<name>`:" prefix is added by
// buildKstatusBody, so this just states the resource + pod counts.
func healthySummary(invTotal, podTotal int, noun string) string {
	if podTotal > 0 {
		return fmt.Sprintf("All %d resource(s) and %d %s(s) reached [kstatus](%s) `Current`.",
			invTotal, podTotal, noun, kstatusReadmeURL)
	}
	return fmt.Sprintf("All %d resource(s) reached [kstatus](%s) `Current`.", invTotal, kstatusReadmeURL)
}

// workloadNoun names the Workload Status population in the summary line:
// plain "pod" when every row is a Pod — the overwhelmingly common case,
// keeping existing comment bodies byte-stable — and "workload" when the
// cron-run fallback contributed Job rows, so the summary can't claim "N
// pod(s) failed" about a namespace that currently has zero pods (the failure
// came from a Job condition).
func workloadNoun(rows []readiness.ResourceStatus) string {
	for _, r := range rows {
		if r.Kind != "" && r.Kind != "Pod" {
			return "workload"
		}
	}
	return "pod"
}

// emojiForStatus picks a per-row glyph that matches the overall marker
// vocabulary. Terminating gets ⏳ — it's not failed, but it's not Current.
func emojiForStatus(s string) string {
	switch s {
	case "Current":
		return statusEmojiHealthy
	case "Failed", "NotFound":
		return statusEmojiFailure
	case "InProgress", "Terminating":
		return "⏳"
	default:
		return statusEmojiWarning
	}
}

// markdownTableCell escapes pipes and collapses newlines so a multi-line
// kstatus message can't break the table layout. GitHub's GFM renderer takes
// `\|` literally and ignores embedded newlines inside cells; we collapse
// them to ` ` so the line stays one row. Cell text is untrusted too, so it
// also goes through markdownSafe.
func markdownTableCell(s string) string {
	s = markdownSafe(s)
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}

// formatComment stamps the deterministic `# <emoji> Flux Status` header and
// the `*Analyzed by flux-ai-bridge*` footer onto whatever the LLM produced
// so every comment we post has byte-identical wrapping. The status emoji
// (✅ / ❌ / ⚠️) is lifted out of the LLM body's first line and placed in
// the header itself, so PR reviewers see the outcome at a glance without
// scrolling past the heading.
//
// The skill tells the LLM NOT to emit the header/footer and to put the
// status marker on the first body line. formatComment is defensive: it
// strips any pre-existing header/footer the LLM emitted, parses the status
// marker, and re-stamps everything in canonical form.
//
// Idempotent: passing in an already-wrapped comment yields the same output.
const (
	commentHeaderBase = "# Flux Status"
	commentFooter     = "---\n*Analyzed by flux-ai-bridge*"

	// retryNoteMarker prefixes the auto-retry policy line (the 🔁 emoji).
	retryNoteMarker = "🔁 "
	// retryNoteAnchor is the FULL stable leading text of the policy note.
	// stripRetryNote keys on this whole phrase — not just the emoji — so an LLM
	// "Next steps" line that happens to start with 🔁 (e.g. "🔁 Restart the
	// deployment …") is NOT mistaken for the bridge's note and deleted.
	retryNoteAnchor = retryNoteMarker + "Re-evaluated automatically"

	statusEmojiHealthy = "✅"
	statusEmojiFailure = "❌"
	statusEmojiWarning = "⚠️"
)

// retryPolicyNote returns the static auto-retry policy line for a non-green
// comment, or "" when no line should be shown. It's a STATEMENT of policy
// (read from config), not a live countdown: identical across PRs and across
// re-evals, so it adds no per-PR state and never churns the comment on an
// unchanged-state retry (a countdown would have to live in the stabilityKey and
// would force an extra LLM call + edit each retry).
//
// This is OPERATIONAL information (how the deploy will be re-checked), not
// authorship metadata, so formatComment renders it independently of
// COMMENT_INCLUDE_FOOTER — disabling the attribution byline must not also hide
// runtime behaviour.
//
// Empty when the deploy is green (nothing to retry) or re-eval is disabled
// (DEDUPE_PR_REEVAL_TTL == 0). The cadence is worded as a floor ("at most once
// every …") because the TTL is the minimum gap — actual retries are driven by
// Flux re-emitting events, not a guaranteed timer.
func retryPolicyNote(cfg *config.Config, outcome bridgegithub.Outcome) string {
	if cfg == nil || outcome == bridgegithub.OutcomeSuccess || cfg.DedupePRReEvalTTL <= 0 {
		return ""
	}
	every := compactDuration(cfg.DedupePRReEvalTTL)
	if cfg.DedupePRReEvalMax > 0 {
		return fmt.Sprintf("%sRe-evaluated automatically up to %d×, at most once every %s, until it passes.",
			retryNoteMarker, cfg.DedupePRReEvalMax, every)
	}
	return fmt.Sprintf("%sRe-evaluated automatically, at most once every %s, until it passes.",
		retryNoteMarker, every)
}

// compactDuration renders a Duration without trailing zero units, so a 15-minute
// TTL reads "15m" rather than Go's default "15m0s". Only whole hour/minute/second
// values are compacted; anything finer falls back to the stdlib rendering.
func compactDuration(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	default:
		return d.String()
	}
}

func formatComment(body string, includeFooter bool, retryNote string) string {
	body = strings.TrimSpace(body)
	// Look for an emoji embedded in a pre-existing "# <emoji> Flux Status"
	// header BEFORE stripping it; otherwise re-wrapping an already-formatted
	// comment would drop the emoji and produce a bare header on the second
	// pass, breaking idempotence.
	headerEmoji := extractHeaderEmoji(body)
	body = stripLeadingHeader(body)
	// Always strip a pre-existing footer so re-wrapping is idempotent
	// regardless of the current includeFooter setting — if a prior run
	// produced a comment WITH a footer and the operator now flips the
	// flag off, the new comment must not carry the stale footer through.
	// The retry-policy note may sit below the byline (footer on) or stand
	// alone (footer off), so strip it first (anchored on its marker) to expose
	// the canonical footer suffix stripTrailingFooter expects.
	body = stripTrailingFooter(stripRetryNote(body))
	bodyEmoji, rest := extractStatusMarker(body)
	// Body marker wins over header marker: the body is the LLM's actual
	// analysis output, while the header may be the bridge's previous stamp
	// (idempotent re-wrap) OR a misbehaving LLM emitting both. When both
	// are present and disagree, trust the body. When only the header is
	// present (the re-wrap case), the header value carries through.
	emoji := bodyEmoji
	if emoji == "" {
		emoji = headerEmoji
	}
	header := commentHeaderBase
	if emoji != "" {
		header = "# " + emoji + " Flux Status"
	}
	out := header
	if rest != "" {
		out += "\n\n" + rest
	}
	// The attribution byline is gated on includeFooter; the retry-policy note is
	// NOT — it's runtime behaviour, not branding, so it shows whenever present.
	// With the byline it sits directly under it; without, it stands alone.
	if includeFooter {
		out += "\n\n" + commentFooter
		if retryNote != "" {
			out += "\n" + retryNote
		}
	} else if retryNote != "" {
		out += "\n\n" + retryNote
	}
	return out
}

// extractHeaderEmoji returns the status emoji embedded in a leading
// "# <emoji> Flux Status" line, or "" if no such header is present.
// Used for idempotence: a re-wrap must preserve the status the first wrap
// already lifted into the header.
func extractHeaderEmoji(body string) string {
	if !strings.HasPrefix(body, "# ") {
		return ""
	}
	nl := strings.IndexByte(body, '\n')
	end := nl
	if end < 0 {
		end = len(body)
	}
	headerLine := body[2:end] // skip "# "
	if !strings.Contains(headerLine, "Flux Status") {
		return ""
	}
	parts := strings.SplitN(headerLine, " Flux Status", 2)
	candidate := strings.TrimSpace(parts[0])
	switch candidate {
	case statusEmojiHealthy:
		return statusEmojiHealthy
	case statusEmojiFailure:
		return statusEmojiFailure
	case statusEmojiWarning, "⚠":
		return statusEmojiWarning
	}
	return ""
}

// extractStatusMarker reads the first non-empty line of body, classifies
// it as healthy/failure/warning (emoji or text marker), and returns the
// canonical emoji plus the body with that line removed. Returns an empty
// emoji when no marker is detected — the body is then preserved as-is and
// the header falls back to the plain "# Flux Status" form.
func extractStatusMarker(body string) (emoji, rest string) {
	body = strings.TrimLeft(body, " \t\r\n")
	if body == "" {
		return "", ""
	}
	nl := strings.IndexByte(body, '\n')
	var first string
	if nl < 0 {
		first = body
		rest = ""
	} else {
		first = body[:nl]
		rest = strings.TrimLeft(body[nl+1:], " \t\r\n")
	}
	first = strings.TrimSpace(first)
	switch {
	case strings.HasPrefix(first, statusEmojiHealthy):
		return statusEmojiHealthy, rest
	case strings.HasPrefix(first, statusEmojiFailure):
		return statusEmojiFailure, rest
	case strings.HasPrefix(first, statusEmojiWarning),
		// Some renderers/composers strip the U+FE0F variation selector;
		// accept the bare ⚠ too so we don't fall through to "no marker".
		strings.HasPrefix(first, "⚠"):
		return statusEmojiWarning, rest
	}
	lower := strings.ToLower(first)
	switch {
	case strings.HasPrefix(lower, "success:"):
		return statusEmojiHealthy, rest
	case strings.HasPrefix(lower, "failure:"):
		return statusEmojiFailure, rest
	case strings.HasPrefix(lower, "warning:"):
		return statusEmojiWarning, rest
	}
	// No recognized marker — keep the first line in the body and don't
	// inject an emoji into the header.
	return "", body
}

// stripLeadingHeader removes a leading "# … Flux Status" line, whether it
// carries a status emoji or not. Handles both the previous bare form and
// the new emoji-prefixed form so a re-wrap is idempotent.
func stripLeadingHeader(body string) string {
	if !strings.HasPrefix(body, "# ") {
		return body
	}
	nl := strings.IndexByte(body, '\n')
	if nl < 0 {
		// Header line with no body following.
		if strings.Contains(body, "Flux Status") {
			return ""
		}
		return body
	}
	if !strings.Contains(body[:nl], "Flux Status") {
		return body
	}
	return strings.TrimLeft(body[nl+1:], " \t\r\n")
}

// stripRetryNote removes a trailing auto-retry policy line (the final line
// beginning with retryNoteAnchor — the bridge's exact note phrase, so genuine
// LLM prose that merely starts with 🔁 is left intact), idempotently. It runs
// before stripTrailingFooter so the byline strip sees a canonical footer suffix.
// Anchoring on the note phrase — rather than blindly cutting from the "---" rule
// — avoids touching any byline-looking text that might appear mid-body, and
// works whether the note followed the byline (footer on), stood alone (footer
// off), or is the only content (no preceding newline).
func stripRetryNote(body string) string {
	trimmed := strings.TrimRight(body, " \t\r\n")
	start := strings.LastIndexByte(trimmed, '\n') + 1 // 0 when there is no newline
	if strings.HasPrefix(trimmed[start:], retryNoteAnchor) {
		return strings.TrimRight(trimmed[:start], " \t\r\n")
	}
	return trimmed
}

// stripTrailingFooter removes a trailing "---" rule followed by an
// "*Analyzed by flux-ai-bridge*" line if present, idempotently.
func stripTrailingFooter(body string) string {
	trimmed := strings.TrimRight(body, " \t\r\n")
	if !strings.HasSuffix(trimmed, commentFooter) {
		return trimmed
	}
	return strings.TrimRight(trimmed[:len(trimmed)-len(commentFooter)], " \t\r\n")
}

func splitOwnerRepo(value string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid owner/repo %q", value)
	}
	return parts[0], parts[1], nil
}

func parseGitHubRepoURL(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "git@github.com:") {
		return splitOwnerRepo(strings.TrimSuffix(strings.TrimPrefix(raw, "git@github.com:"), ".git"))
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("parse repo url: %w", err)
	}
	if !strings.EqualFold(parsed.Host, "github.com") {
		return "", "", fmt.Errorf("unsupported git host %q", parsed.Host)
	}
	return splitOwnerRepo(strings.TrimSuffix(strings.Trim(parsed.Path, "/"), ".git"))
}
