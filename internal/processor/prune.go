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
	"context"
	"strings"
	"time"

	"github.com/staffbase/flux-ai-bridge/internal/readiness"
)

// kustomizeControllerComponent is the emitter name (Event source.component /
// reportingComponent) on genuine prune events — the only component that prunes
// a Kustomization's inventory.
const kustomizeControllerComponent = "kustomize-controller"

// Reasons a genuine prune event can carry (see parsePrunedSubjects).
const (
	pruneEventReasonProgressing = "Progressing"
	pruneEventReasonSucceeded   = "ReconciliationSucceeded"
)

// kustomizeRevisionAnnotation is the Event annotation in which
// kustomize-controller records the source revision a reconcile ran
// (`<branch>@sha1:<sha>`). Older Flux versions don't set it.
const kustomizeRevisionAnnotation = "kustomize.toolkit.fluxcd.io/revision"

// pruneSetProvider lazily resolves the set of resources THIS Kustomization
// removed in its recent reconcile(s), keyed by ssa "Subject" form
// (`Kind/namespace/name`, or `Kind/name` for cluster-scoped). It is a func so
// the (one) MCP events read is paid only when attribution actually needs it —
// a PR with no deletions never triggers the fetch.
type pruneSetProvider func() map[string]struct{}

// newPruneSetProvider returns a memoised provider over the Kustomization's
// prune events. When PR_ATTRIBUTE_PRUNES is off it resolves to an empty set
// without any MCP call, so attribution falls back to inventory-membership only.
//
// ns is the Kustomization's namespace (where its Events live — flux-system in
// the namespace==name convention) and ksName is the Kustomization name, used to keep only
// events emitted on THIS Kustomization object. revision is the revision under
// analysis: a prune Event recorded for another revision is ignored (see
// parsePrunedSubjects).
func (p *Processor) newPruneSetProvider(ctx context.Context, ns, ksName, revision string) pruneSetProvider {
	var cached map[string]struct{}
	loaded := false
	return func() map[string]struct{} {
		if loaded {
			return cached
		}
		loaded = true
		cached = map[string]struct{}{}
		if !p.cfg.PRAttributePrunes {
			return cached
		}
		body, err := p.mcp.ListKubernetesResources(ctx, "v1", "Event", ns)
		if err != nil {
			// "No resources found" is MCP's phrasing for an empty Event list — a
			// valid result (the namespace simply has no Events right now), not a
			// failure. Treat it as an empty prune set silently, so a sparse-Event
			// namespace doesn't spam WARN. Any other error is a genuine fetch
			// failure: best-effort degrade to inventory-membership attribution.
			if !isNoResourcesFound(err) {
				p.logger.Warn("prune attribution: events fetch failed via MCP",
					"namespace", ns, "kustomization", ksName, "error", err.Error())
			}
			return cached
		}
		cached = parsePrunedSubjects(parseEventList(body), ksName, revision)
		if len(cached) > 0 {
			p.logger.Debug("prune attribution: pruned subjects collected",
				"namespace", ns, "kustomization", ksName, "count", len(cached))
		}
		return cached
	}
}

// parsePrunedSubjects walks a Kustomization's Events and returns the set of
// resource Subjects pruned by its MOST RECENT prune. kustomize-controller emits
// a dedicated event on each prune (severity Info → Kubernetes Event type
// "Normal") whose Message is ssa's `ChangeSet.String()` — one `Subject Action`
// per line, the prune Action being the literal `deleted` (verified against
// kustomize-controller v1.8.4 / fluxcd/pkg/ssa v0.67.4).
//
// Only the freshest qualifying prune Event's subjects are returned, NOT every
// `deleted` line in the namespace's whole Event-retention window. A prune Event
// lingers (~1h TTL) after its reconcile, so accumulating across the window would
// let a prior reconcile's prune seed the set and attribute a later PR — breaking
// the "THIS reconcile pruned it" intent. A single reconcile emits ONE event
// carrying its full changeset, so the latest prune Event is the latest
// reconcile's prune; ties (equal timestamps) are unioned. Events with no
// parseable timestamp (test fixtures / sparse rendering) sort as the zero time,
// so a set with no timestamps at all still resolves to "all of them".
//
// The prune Event must also belong to the analysed revision. kustomize-controller
// annotates its Events with the revision the reconcile ran
// (kustomizeRevisionAnnotation); an Event whose annotation names another commit
// is a different reconcile's prune — an older revision's, still inside the
// retention window, or a newer one's that already ran — and is dropped before
// the freshest is picked. An Event without the annotation (older Flux versions)
// is kept, as is every Event when revision is empty. The commit tokens are
// compared (see revisionSHA), case-insensitively, so `dev@sha1:<sha>` matches the
// bare SHA the webhook hands to Process.
func parsePrunedSubjects(events []eventRecord, ksName, revision string) map[string]struct{} {
	type pruneEvent struct {
		when     time.Time
		subjects []string
	}
	var prunes []pruneEvent
	var newest time.Time
	for _, e := range events {
		if e.InvolvedObject.Name != ksName {
			continue
		}
		if e.InvolvedObject.Kind != "" && e.InvolvedObject.Kind != "Kustomization" {
			continue
		}
		// kustomize-controller emits the prune as an Info-severity (Kubernetes
		// type "Normal") event whose reason is the Ready condition's reason at
		// emission time. reconcile() marks Ready Unknown/"Progressing" before it
		// applies and prunes, so a prune during a reconcile carries "Progressing"
		// (verified against v1.8.4 and on cluster); "ReconciliationSucceeded" is
		// accepted too, for controllers that emit it after marking Ready. Guard on
		// that shape so an unrelated Kustomization event whose message merely
		// contains "… deleted" can't inject a false prune subject. Unset fields
		// are tolerated (test fixtures / sparse MCP rendering); a populated field
		// that doesn't match is dropped.
		if e.Type != "" && e.Type != "Normal" {
			continue
		}
		if e.Reason != "" && e.Reason != pruneEventReasonProgressing && e.Reason != pruneEventReasonSucceeded {
			continue
		}
		// REQUIRE the emitter to identify as kustomize-controller — the only
		// component that prunes a Kustomization's inventory. Events carry the
		// emitter in reportingComponent (modern) or source.component (legacy);
		// real prune Events populate at least source.component (verified on
		// cluster, and the MCP server preserves it). This rejects Events that
		// don't name kustomize-controller — an unrelated component's Event, or an
		// emitter-less stray — so an incidental "… deleted" line can't seed the
		// prune set.
		//
		// NOT a security boundary: Event fields are client-supplied, so anyone
		// with Event-create RBAC in the namespace could forge a matching emitter.
		// This narrows accidental/low-effort matches, not a determined spoof. The
		// failure floor bounds the blast radius — a wrongly-seeded prune only
		// attributes/greens a PR that ALSO removed the matching resource, it never
		// fabricates attribution from the Event alone.
		if e.ReportingComponent != kustomizeControllerComponent && e.Source.Component != kustomizeControllerComponent {
			continue
		}
		if pruneEventOtherRevision(e, revision) {
			continue // another reconcile's prune, not the analysed revision's
		}
		var subjects []string
		for _, line := range strings.Split(e.Message, "\n") {
			if subject, ok := prunedSubjectFromLine(line); ok {
				subjects = append(subjects, subject)
			}
		}
		if len(subjects) == 0 {
			continue // a clean reconcile (no `deleted` lines) — nothing to attribute
		}
		when := e.freshest()
		prunes = append(prunes, pruneEvent{when: when, subjects: subjects})
		if when.After(newest) {
			newest = when
		}
	}
	out := map[string]struct{}{}
	for _, pe := range prunes {
		if !pe.when.Equal(newest) {
			continue // an older reconcile's prune — not what THIS reconcile removed
		}
		for _, s := range pe.subjects {
			out[s] = struct{}{}
		}
	}
	return out
}

// pruneEventOtherRevision reports whether e's revision annotation confidently
// names a commit other than revision: both commit tokens are non-empty and
// differ. A missing or unparseable annotation, or an empty revision, is not a
// mismatch (same stance as revisionMismatch).
func pruneEventOtherRevision(e eventRecord, revision string) bool {
	got := revisionSHA(e.Metadata.Annotations[kustomizeRevisionAnnotation])
	want := revisionSHA(revision)
	return got != "" && want != "" && !strings.EqualFold(got, want)
}

// prunedSubjectFromLine extracts the Subject from a single ssa changeset line
// of the form `Kind/namespace/name deleted` (or `Kind/name deleted` for
// cluster-scoped). It is deliberately strict so an unrelated event message that
// happens to contain the word "deleted" can't be mistaken for a prune entry:
// the line must end in ` deleted`, the subject must be 2 or 3 non-empty
// slash-separated tokens, and the first token (Kind) must look like a Kubernetes
// kind (leading uppercase letter).
func prunedSubjectFromLine(line string) (string, bool) {
	line = strings.TrimSpace(line)
	const suffix = " deleted"
	if !strings.HasSuffix(line, suffix) {
		return "", false
	}
	subject := strings.TrimSpace(strings.TrimSuffix(line, suffix))
	parts := strings.Split(subject, "/")
	if len(parts) < 2 || len(parts) > 3 {
		return "", false
	}
	for _, part := range parts {
		if part == "" {
			return "", false
		}
	}
	if k := parts[0]; k == "" || k[0] < 'A' || k[0] > 'Z' {
		return "", false
	}
	return subject, true
}

// subjectKeyFromInventoryID converts a Flux inventory ID
// (`namespace_name_group_kind`) into the ssa Subject form used by prune events
// (`Kind/namespace/name`, or `Kind/name` when cluster-scoped). The group is
// dropped because the prune Subject carries no API group — matching is on the
// (kind, namespace, name) tuple. Returns false for an unparseable ID.
func subjectKeyFromInventoryID(id string) (string, bool) {
	ref, err := readiness.ParseInventoryID(id)
	if err != nil || ref.Kind == "" || ref.Name == "" {
		return "", false
	}
	if ref.Namespace == "" {
		return ref.Kind + "/" + ref.Name, true
	}
	return ref.Kind + "/" + ref.Namespace + "/" + ref.Name, true
}

// inventoryIDWasPruned reports whether the resource identified by a Flux
// inventory ID is in the prune set (i.e. this Kustomization just deleted it).
func inventoryIDWasPruned(pruneSet pruneSetProvider, id string) bool {
	if pruneSet == nil {
		return false
	}
	subject, ok := subjectKeyFromInventoryID(id)
	if !ok {
		return false
	}
	_, pruned := pruneSet()[subject]
	return pruned
}

// verifyTeardownComplete re-checks each resource an attributed PR removed (and
// this reconcile pruned) and returns synthetic readiness rows for every one that
// is still observably present — so the caller keeps the verdict non-green instead
// of a premature ✅.
//
// The prune event is the **authoritative deletion assertion**: the controller
// already reported it deleted these resources, so the re-check's job is to catch
// the EXCEPTION — an object that's still positively present (a finalizer-stuck
// `Terminating`, or even still `Current` because the delete hasn't applied) — not
// to re-confirm the deletion itself. Accordingly:
//
//   - any present status (`Terminating`/`Current`/`Failed`/`InProgress`) → kept
//     as a lingering row → verdict stays non-green. A still-`Current` object is
//     re-rendered as `Terminating` so its table row doesn't read "✅ Current"
//     (healthy) while representing an incomplete teardown — the observed status
//     is preserved in the row message;
//   - `Unknown` (a genuine MCP read error) → also kept lingering — we couldn't
//     verify, so we don't green on it;
//   - `NotFound` → treated as gone (consistent with the prune assertion), no row
//     — unless the read used a guessed apiVersion (the kind isn't in MCP's
//     discovery cache, see readiness.ResourceStatus.VersionGuessed). Such a
//     NotFound may only mean the bridge asked for a version the cluster doesn't
//     serve, so the resource was never really looked up: it is kept as an
//     `Unknown` row "pruned but deletion unverified (kind not in discovery)".
//
// Known limitation: readiness.Checker cannot distinguish a real 404 from a
// degraded "No resources found" (cold MCP discovery cache) — it maps both to
// `NotFound` (see checkOne). So during *sustained* MCP degradation a still-stuck
// object can read `NotFound` and be treated as gone, allowing a premature
// `flux-success`. This is bounded: the controller did assert the deletion, the
// whole-inventory poll runs alongside this check, and the PR head un-freezes on a
// new commit (re-verifying against fresh state). Extra immediate reads don't help
// — they hit the same cold cache — so we don't pay for them. Genuine MCP errors
// surface as `Unknown` (above) and are handled conservatively.
//
// prunedIDs is de-duplicated first — it's accumulated across every attributed PR
// and changed file, so the same resource can appear more than once. CheckAll
// reports failures as per-row `Unknown` (never an error), so its error return is
// ignored.
func (p *Processor) verifyTeardownComplete(ctx context.Context, prunedIDs []string) []readiness.ResourceStatus {
	if p.checker == nil || len(prunedIDs) == 0 {
		return nil
	}
	rows, _ := p.checker.CheckAll(ctx, dedupeStrings(prunedIDs))
	var lingering []readiness.ResourceStatus
	for _, r := range rows {
		if r.Status == "NotFound" && !r.VersionGuessed {
			continue // gone — consistent with the prune event's deletion assertion
		}
		// Still present, or unverifiable (Unknown = MCP read error, or a
		// NotFound under a guessed apiVersion) → keep non-green. Always annotate
		// so the row's purpose is clear, and preserve the observed kstatus
		// status in the message.
		observed := r.Status
		note := "pruned but still present"
		switch r.Status {
		case "NotFound":
			// Read at a guessed apiVersion: the NotFound doesn't prove the
			// object is gone. Unknown renders ⚠️ like an MCP read error.
			r.Status = "Unknown"
			note = "pruned but deletion unverified (kind not in discovery)"
		case "Unknown":
			// Not presence — we just couldn't verify. Renders ⚠️, which is the
			// right signal, so leave the status as-is with a distinct note.
			note = "pruned but deletion unverified (MCP read error)"
		case "Current":
			// A still-`Current` object renders "✅ Current" in the table and reads
			// as healthy, hiding that teardown hasn't finished. Override the
			// synthetic row to Terminating (⏳) so it matches the ⚠️ verdict; the
			// real observed status is preserved in the message and the log below.
			r.Status = "Terminating"
		}
		msg := note
		if observed != r.Status {
			msg += " (observed " + observed + ")"
		}
		if r.Message != "" {
			msg += " — " + r.Message
		}
		r.Message = msg
		p.logger.Info("teardown verify: pruned resource not confirmed gone, keeping verdict non-green",
			"kind", r.Kind, "namespace", r.Namespace, "name", r.Name, "status", observed)
		lingering = append(lingering, r)
	}
	return lingering
}

// dedupeStrings returns xs with duplicates removed, preserving first-seen order.
func dedupeStrings(xs []string) []string {
	seen := make(map[string]struct{}, len(xs))
	var out []string
	for _, x := range xs {
		if _, dup := seen[x]; dup {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	return out
}
