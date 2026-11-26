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

// Package readiness evaluates the state of every resource in a
// Kustomization's inventory using sigs.k8s.io/cli-utils/pkg/kstatus.
//
// The bridge never reads Kubernetes directly — every resource fetch goes
// through flux-operator-mcp via the ResourceFetcher seam. This keeps the
// bridge ServiceAccount scoped to Flux CRs only; the MCP server holds the
// broad cluster-read RBAC. Callers wire any MCP-backed adapter that returns
// YAML for a given GVK + name + namespace.
package readiness

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	"sigs.k8s.io/yaml"
)

type ResourceStatus struct {
	Kind      string
	Message   string
	Name      string
	Namespace string
	Status    string
	// Conditions is the resource's own `.status.conditions[]` block, lifted
	// out of the unstructured object. Only populated for non-Current rows so
	// the failure-context appender can surface controller-specific reasons
	// (e.g., HelmRelease "Released: False, reason: InstallFailed, …") that
	// kstatus's summary message drops.
	Conditions []Condition
	// Controller is a human-readable identifier for the workload controller
	// that owns this resource — typically "<Kind>/<Name>" (e.g.
	// "Deployment/api"). Only populated for Pod rows produced by the pod
	// kstatus snapshot, resolved one extra hop up the ownerReferences chain
	// for Pod → ReplicaSet → Deployment so the user-facing controller is
	// surfaced rather than the implementation-detail ReplicaSet. Empty for
	// inventory rows and for naked pods (no controller owner).
	Controller string
	// CreatedAt is the pod's metadata.creationTimestamp. Only populated for
	// Pod rows produced by the pod kstatus snapshot; zero when absent or
	// unparseable.
	CreatedAt time.Time
	// VersionGuessed reports that the final read of this row used a guessed
	// apiVersion: the (group, kind) wasn't in MCP's discovery cache, even
	// after the NotFound refresh in checkOne, so the version came from
	// fallbackPreferredVersions or the "v1" default. A NotFound row is also
	// marked when its version came from the cache but the latest discovery
	// attempt didn't confirm it (it failed, came back empty, or left the kind
	// out). A NotFound under such a version may only mean the bridge asked
	// for a version the cluster doesn't serve, so it doesn't prove the
	// resource is gone. The core group ("") is never guessed: it only serves
	// v1.
	VersionGuessed bool
	// Replicas carries the hard replica accounting for workload kinds
	// (Deployment/StatefulSet/ReplicaSet/DaemonSet); nil for everything else.
	// Populated for non-Current rows so the LLM states "0/1 ready" as fact
	// instead of inferring service health from the Deployment `Available`
	// condition — which stays True with zero ready pods when
	// maxUnavailable >= replicas (minAvailable becomes 0). See the SKILL's
	// Workload guidance.
	Replicas *WorkloadReplicas
}

// WorkloadReplicas is the ground-truth replica accounting for a workload
// resource. It exists so failure analysis reports availability from numbers,
// not from the easily-misread `Available` condition.
type WorkloadReplicas struct {
	Desired     int64 // spec.replicas (Deployment/StatefulSet/ReplicaSet) or status.desiredNumberScheduled (DaemonSet)
	Ready       int64
	Available   int64
	Updated     int64
	Unavailable int64
}

// String renders the counts compactly for the LLM prompt, leading with the
// ready/desired ratio — the single most decisive figure ("0/1 ready" tells the
// whole story regardless of what the Available condition claims).
func (w WorkloadReplicas) String() string {
	return fmt.Sprintf("replicas: %d/%d ready, %d available, %d updated, %d unavailable",
		w.Ready, w.Desired, w.Available, w.Updated, w.Unavailable)
}

// Condition is a minimal projection of a Kubernetes status condition. We
// keep only the four fields the LLM actually reads.
type Condition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

// ResourceFetcher is the single seam the readiness check needs. Implementations
// must accept (apiVersion, kind, name, namespace) and return YAML for the
// matching resource. flux-operator-mcp's `get_kubernetes_resources` tool
// satisfies this; tests mock it directly.
type ResourceFetcher interface {
	GetKubernetesResource(ctx context.Context, apiVersion, kind, name, namespace string) ([]byte, error)
}

type Checker struct {
	fetcher  ResourceFetcher
	logger   *slog.Logger // nil-safe: nil → slog.Default()
	resolver *Resolver    // nil-safe: nil → fallback table only
}

func NewChecker(fetcher ResourceFetcher) *Checker {
	return &Checker{fetcher: fetcher}
}

// WithLogger returns a Checker that emits per-resource MCP-failure logs
// against the supplied logger. The fluent API keeps NewChecker's signature
// unchanged for existing call sites and tests that don't care about logs.
func (c *Checker) WithLogger(l *slog.Logger) *Checker {
	c.logger = l
	return c
}

// WithResolver attaches a (group,kind)→apiVersion resolver populated from
// MCP discovery at startup. Without one, the Checker falls back to the
// hardcoded preferredVersions table — which misroutes any non-v1 CRD
// because the Flux Kustomization inventory ID format drops the version.
func (c *Checker) WithResolver(r *Resolver) *Checker {
	c.resolver = r
	return c
}

func (c *Checker) log() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}

// CheckAll evaluates each inventory ref. A failure on a single resource is
// recorded as a status row rather than aborting the whole batch — the goal is
// to give the LLM a complete picture even if some lookups fail.
func (c *Checker) CheckAll(ctx context.Context, refs []string) ([]ResourceStatus, error) {
	results := make([]ResourceStatus, 0, len(refs))
	for _, ref := range refs {
		results = append(results, c.checkOne(ctx, ref))
	}
	return results, nil
}

// WaitUntilSettled re-runs CheckAll every interval until every row reaches a
// terminal kstatus (Current, Failed, Terminating) or timeout fires. NotFound
// is intentionally non-terminal — see allSettled for why (transient MCP
// not-found guard).
// This lets the bridge analyze the *settled* state instead of an in-flight
// rollout: kustomize-controller emits ReconciliationSucceeded the instant
// kubectl-apply returns, well before the underlying workload has rolled.
//
// If timeout elapses with rows still InProgress/Unknown, the most recent
// result is returned along with settled=false so the caller can flag that
// the analysis ran against a non-stable state.
//
// Forwards to WaitUntilSettledFunc with a constant refs supplier.
func (c *Checker) WaitUntilSettled(ctx context.Context, refs []string, interval, timeout time.Duration) (results []ResourceStatus, settled bool, err error) {
	return c.WaitUntilSettledFunc(ctx, func(context.Context) []string { return refs }, interval, timeout)
}

// WaitUntilSettledFunc is the dynamic-refs variant of WaitUntilSettled.
// On each poll iteration it calls `refsFunc(ctx)` to obtain the current ref
// list — useful when the inventory GROWS mid-poll (a HelmRelease populates
// `status.history[].inventory` only after helm-controller observes it,
// so the bridge's HelmRelease-expansion produces zero children for the
// first few seconds and the Service/Deployment rows only become available
// later in the poll).
//
// Settled semantics match WaitUntilSettled: every row in the LATEST
// snapshot must reach a terminal kstatus. If refsFunc grows the list
// mid-poll, the new rows need to settle too before the loop returns
// settled=true. The refs and the rows are separate reads, so a settled
// snapshot only counts when a fresh refsFunc() still returns the same refs:
// a HelmRelease that finishes its install between the two reads can read
// Current while its new children are still missing from the refs.
//
// A cancelled ctx never yields settled=true, and the last sleep stops at the
// deadline instead of running up to a whole interval past it. Each poll's
// reads are bounded by the deadline (at least minPollRead per read): the rows
// and the refs alike, since refsFunc reads too (a HelmRelease expansion is an
// MCP read per HelmRelease), so refsFunc must make its reads with the ctx it
// is given. A row read the timeout cuts short reads Unknown, so the result is
// settled=false with a nil error.
func (c *Checker) WaitUntilSettledFunc(ctx context.Context, refsFunc func(context.Context) []string, interval, timeout time.Duration) (results []ResourceStatus, settled bool, err error) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = 3 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	// readRefs calls refsFunc under the same bound as the row reads.
	readRefs := func() []string {
		readCtx, cancelRead := readContext(ctx, deadline)
		defer cancelRead()
		return refsFunc(readCtx)
	}
	for {
		refs := readRefs()
		// The poll's MCP reads are bounded by the timeout too (but get at
		// least minPollRead), so a slow read can't run the poll past it. A
		// read cut short reads Unknown, which keeps the snapshot unsettled.
		readCtx, cancelRead := readContext(ctx, deadline)
		results, err = c.CheckAll(readCtx, refs)
		cancelRead()
		if err != nil {
			return results, false, err
		}
		// Reads made with a dead context prove nothing (and no refs at all
		// would read as settled).
		if err := ctx.Err(); err != nil {
			return results, false, err
		}
		if allSettled(results) && slices.Equal(readRefs(), refs) {
			return results, true, nil
		}
		if !time.Now().Before(deadline) {
			return results, false, nil
		}
		select {
		case <-ctx.Done():
			return results, false, ctx.Err()
		case <-time.After(min(interval, time.Until(deadline))):
		}
	}
}

// minPollRead is the least time one poll's reads get, even when the timeout
// is already spent, so the poll still takes its one snapshot.
const minPollRead = time.Second

// readContext bounds one read step of the poll by the poll deadline, but
// gives it at least minPollRead.
func readContext(ctx context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if floor := time.Now().Add(minPollRead); floor.After(deadline) {
		deadline = floor
	}
	return context.WithDeadline(ctx, deadline)
}

// allSettled returns true iff every row is in a terminal kstatus. InProgress
// and Unknown count as still-changing so the poll keeps running.
//
// NotFound is deliberately NOT terminal here, even though a genuinely-pruned
// resource never comes back: flux-operator-mcp returns the byte-identical
// "No resources found" both for a real 404 AND for a read it couldn't serve
// while its discovery cache was cold (e.g. just after an OOM restart), so a
// single NotFound is ambiguous. Treating it as still-changing means the poll
// re-fetches it next interval — a transient blip self-heals into the real
// status, while a genuinely-missing resource simply stays NotFound until the
// timeout and is reported as such (settled=false). The cost is that a real
// prune now keeps polling to the timeout instead of settling immediately;
// that's an accepted trade for never emitting a confident "deleted" verdict
// off a flaky MCP read.
func allSettled(results []ResourceStatus) bool {
	for _, r := range results {
		switch r.Status {
		case string(status.CurrentStatus),
			string(status.FailedStatus),
			string(status.TerminatingStatus):
			// settled
		default:
			// InProgress, Unknown, and NotFound all keep the poll running.
			return false
		}
	}
	return true
}

// AllSettled is the exported form of allSettled, for callers (e.g. the
// processor's pod-settle wait) that need the same terminal-kstatus predicate on
// a status slice they assembled themselves. A pod still rolling out reads
// InProgress (non-terminal → keep waiting); a genuinely failing pod reads
// Failed (terminal → stop waiting and report). NotFound stays non-terminal for
// the same ambiguity reason documented on allSettled, though pods sourced from
// a list scan never carry it.
func AllSettled(results []ResourceStatus) bool {
	return allSettled(results)
}

func (c *Checker) checkOne(ctx context.Context, ref string) ResourceStatus {
	parsed, err := ParseInventoryID(ref)
	if err != nil {
		return ResourceStatus{Status: "Unknown", Message: err.Error()}
	}

	apiVersion, known := c.resolver.apiVersionKnown(parsed.Group, parsed.Kind)
	rs := c.fetchResource(ctx, parsed, apiVersion)

	// NotFound may mean we polled at the wrong apiVersion: a GUESSED one (no
	// discovery entry for the group — e.g. a CRD registered after the bridge
	// started), or a cached one the cluster no longer serves (a CRD upgraded
	// from v1beta1 to v1, or an entry a partial discovery response left
	// stale). Refresh discovery (debounced, so at most one call per cooldown
	// however many rows read NotFound) and, if the kind now resolves to a
	// different version, re-poll there before trusting the NotFound.
	if rs.Status == "NotFound" {
		if _, rerr := c.resolver.Refresh(ctx); rerr != nil {
			c.log().Warn("readiness: discovery refresh failed", "group", parsed.Group, "kind", parsed.Kind, "error", rerr.Error())
		}
		if v, nowKnown := c.resolver.apiVersionKnown(parsed.Group, parsed.Kind); nowKnown && v != apiVersion {
			// Discovery now knows the kind at another version: re-poll there.
			c.log().Info("readiness: re-resolved apiVersion after NotFound; retrying",
				"group", parsed.Group, "kind", parsed.Kind, "from", apiVersion, "to", v)
			apiVersion, known = v, true
			rs = c.fetchResource(ctx, parsed, v)
		}
		// A NotFound counts as read at a known version only when the latest
		// discovery attempt lists the kind at the version read. A cache entry
		// alone isn't enough: a failed, empty or partial refresh keeps it,
		// though the cluster may no longer serve that version.
		if rs.Status == "NotFound" {
			known = c.resolver.confirmedVersion(parsed.Group, parsed.Kind, apiVersion)
		}
	}
	// The core group only serves v1, so its "v1" default is no guess.
	rs.VersionGuessed = !known && parsed.Group != ""
	return rs
}

// fetchResource polls one resource at the given apiVersion and maps the result
// to a ResourceStatus (NotFound / Unknown / computed kstatus).
func (c *Checker) fetchResource(ctx context.Context, parsed InventoryRef, apiVersion string) ResourceStatus {
	yamlBytes, err := c.fetcher.GetKubernetesResource(ctx, apiVersion, parsed.Kind, parsed.Name, parsed.Namespace)
	if err != nil {
		// MCP "not found" surfaces as a regular error; the tool doesn't
		// distinguish. flux-operator-mcp emits two phrasings:
		//
		//   - "... not found"               — single-resource GET 404'd
		//   - "No resources found"          — LIST/GET returned empty
		//
		// Crucially, a degraded MCP server (e.g. cold discovery cache right
		// after an OOM restart) returns the SAME "No resources found" for a
		// resource that actually exists. We can't tell the two apart from the
		// message, so we mark NotFound but treat it as NON-terminal in the
		// poll (see allSettled): a transient blip re-resolves to the real
		// status next interval, while a genuinely-missing resource stays
		// NotFound through to the timeout. This avoids emitting a confident
		// "deleted" verdict off a flaky read.
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "not found") || strings.Contains(msg, "no resources found") {
			return ResourceStatus{
				Kind: parsed.Kind, Name: parsed.Name, Namespace: parsed.Namespace,
				Status: "NotFound", Message: "resource not found",
			}
		}
		// Non-not-found = MCP itself misbehaved (session error, RBAC, etc.).
		// Surface as WARN with the exact error string so operators can act on
		// it; the row keeps Status=Unknown so the LLM still sees a complete
		// inventory readout.
		c.log().Warn("readiness: resource fetch failed via MCP",
			"apiVersion", apiVersion, "kind", parsed.Kind,
			"name", parsed.Name, "namespace", parsed.Namespace,
			"tool", "get_kubernetes_resources", "error", err.Error())
		return ResourceStatus{
			Kind: parsed.Kind, Name: parsed.Name, Namespace: parsed.Namespace,
			Status:  "Unknown",
			Message: fmt.Sprintf("get resource: %v", err),
		}
	}

	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(yamlBytes, obj); err != nil {
		return ResourceStatus{
			Kind: parsed.Kind, Name: parsed.Name, Namespace: parsed.Namespace,
			Status:  "Unknown",
			Message: fmt.Sprintf("parse resource yaml: %v", err),
		}
	}

	computed, err := status.Compute(obj)
	if err != nil {
		return ResourceStatus{
			Kind: parsed.Kind, Name: parsed.Name, Namespace: parsed.Namespace,
			Status:  "Unknown",
			Message: fmt.Sprintf("compute status: %v", err),
		}
	}

	rs := ResourceStatus{
		Kind:      parsed.Kind,
		Name:      parsed.Name,
		Namespace: parsed.Namespace,
		Status:    string(computed.Status),
		Message:   computed.Message,
	}
	// kstatus can call a resource Current off a status it has barely
	// written — a HelmRelease fetched moments after creation, before
	// helm-controller's first status write, reports before any conditions
	// or observedGeneration exist at all. kstatus's own generic generation
	// check (checkGeneration in cli-utils' kstatus/status package) only
	// fires when status.observedGeneration is PRESENT; when the field is
	// missing entirely it's a no-op, and — for a CRD with no kstatus-native
	// handler, like HelmRelease — Compute() falls all the way through to
	// "no recognized conditions, assume Current". Confirmed directly
	// against cli-utils v0.37.2's status.Compute(): an object with
	// observedGeneration PRESENT but behind generation already comes back
	// InProgress from kstatus itself (so that shape can never reach this
	// point with rs.Status == Current); the gap this closes is exclusively
	// generation present / observedGeneration absent.
	// Deliberately scoped to HelmRelease, not "any resource with
	// metadata.generation": metadata.generation is API-server-managed and
	// says nothing about whether a given CRD's controller ALSO publishes a
	// top-level status.observedGeneration — some track generation only
	// per-condition (the metav1.Condition.ObservedGeneration convention,
	// e.g. cert-manager's Certificate) and never set the top-level field at
	// all. Applying this generically would misread that absence as
	// eternally stale and hold such a resource at InProgress until timeout.
	// HelmRelease is the one kind this is verified for (Flux's helm
	// controller always sets status.observedGeneration, defaulting to -1
	// pre-reconcile) and the one the original bug report was about.
	if rs.Status == string(status.CurrentStatus) && isHelmRelease(parsed.Kind, apiVersion) {
		if msg, stale := staleObservedGeneration(obj); stale {
			rs.Status = string(status.InProgressStatus)
			rs.Message = msg
		}
	}
	// kstatus reads Ready=True as Current without looking at the condition's
	// own observedGeneration. A resource that tracks generation only per
	// condition (the metav1.Condition convention, e.g. cert-manager's
	// Certificate) therefore reads Current right after a spec change, off the
	// previous generation's verdict, until its controller re-evaluates it.
	// Only an explicitly stale value demotes the row, so a CRD that never
	// sets the field still defers to kstatus.
	if rs.Status == string(status.CurrentStatus) {
		if msg, stale := staleReadyCondition(obj); stale {
			rs.Status = string(status.InProgressStatus)
			rs.Message = msg
		}
	}
	// Extract controller-level conditions only for non-Current rows — they're
	// the only ones where the LLM benefits from the extra context. Skipping
	// the read for Current rows keeps the prompt lean.
	if rs.Status != string(status.CurrentStatus) {
		rs.Conditions = extractConditions(obj)
		rs.Replicas = extractWorkloadReplicas(obj)
	}
	return rs
}

// isHelmRelease reports whether an inventory entry is a HelmRelease, in any
// API version Flux's helm-controller has shipped (v2beta1/v2beta2/v2) —
// matched on group, not the exact version, so a version bump doesn't
// silently disable the staleObservedGeneration guard. Scoping that guard to
// this one kind (see fetchResource's call site) is deliberate: it's the one
// kind verified to always publish a top-level status.observedGeneration.
func isHelmRelease(kind, apiVersion string) bool {
	return kind == "HelmRelease" && strings.HasPrefix(apiVersion, "helm.toolkit.fluxcd.io/")
}

// staleObservedGeneration reports whether a HelmRelease's own generation
// tracking shows helm-controller hasn't caught up to the current spec —
// stale in two distinct shapes:
//
//   - status.observedGeneration is entirely ABSENT: the resource has never
//     been observed by its controller at all. This is the actual gap in
//     kstatus's own generic check (see the call site's comment) — kstatus
//     treats "no evidence" as "assume Current" for a CRD with no
//     kstatus-native handler.
//   - status.observedGeneration is PRESENT but behind metadata.generation:
//     kept as a belt-and-suspenders check even though kstatus's own
//     checkGeneration already demotes this shape to InProgress on its own
//     (confirmed directly against cli-utils v0.37.2), so this function's
//     contract — "a stale observedGeneration is never reported as fine" —
//     doesn't silently depend on that upstream behavior staying unchanged.
//
// Returns stale=false (skip, defer to kstatus) when metadata.generation
// itself is absent — shouldn't happen for a HelmRelease, but a resource
// with no generation at all gives this function nothing to compare against
// either way. Callers MUST additionally gate on isHelmRelease: unlike
// HelmRelease, an arbitrary CRD having metadata.generation does not
// guarantee its controller publishes a matching top-level
// status.observedGeneration (some track generation only per-condition, via
// metav1.Condition.ObservedGeneration) — treating that absence as stale for
// such a resource would misread a genuinely healthy CRD as permanently
// stuck, holding the readiness poll open until timeout.
func staleObservedGeneration(obj *unstructured.Unstructured) (message string, stale bool) {
	generation, genFound, _ := unstructured.NestedInt64(obj.Object, "metadata", "generation")
	if !genFound || generation == 0 {
		return "", false
	}
	observed, obsFound, _ := unstructured.NestedInt64(obj.Object, "status", "observedGeneration")
	if !obsFound {
		return "status.observedGeneration has not been published yet (not yet observed by its controller)", true
	}
	if observed != generation {
		return fmt.Sprintf("status.observedGeneration (%d) has not caught up to metadata.generation (%d) yet", observed, generation), true
	}
	return "", false
}

// staleReadyCondition reports whether the object's Ready condition carries an
// observedGeneration behind metadata.generation. A missing or zero value (the
// unset value of metav1.Condition.ObservedGeneration) is not stale: a
// controller that doesn't track generation per condition would otherwise read
// as stuck forever.
func staleReadyCondition(obj *unstructured.Unstructured) (message string, stale bool) {
	generation := obj.GetGeneration()
	if generation == 0 {
		return "", false
	}
	raw, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return "", false
	}
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok || m["type"] != "Ready" {
			continue
		}
		observed, found, err := unstructured.NestedInt64(m, "observedGeneration")
		if err != nil || !found || observed == 0 || observed >= generation {
			return "", false
		}
		return fmt.Sprintf("Ready condition observedGeneration (%d) has not caught up to metadata.generation (%d) yet", observed, generation), true
	}
	return "", false
}

// extractWorkloadReplicas pulls the hard replica counts off a workload object,
// returning nil for non-workload kinds. Deployment/StatefulSet/ReplicaSet share
// the spec.replicas + status.{ready,available,updated,unavailable}Replicas
// shape; DaemonSet uses its own *NumberScheduled / number* status fields. All
// reads go through NestedInt64, which is safe here because the object was
// decoded via Unstructured's JSON scheme (numbers normalised to int64).
func extractWorkloadReplicas(obj *unstructured.Unstructured) *WorkloadReplicas {
	statusInt := func(field string) int64 {
		v, _, _ := unstructured.NestedInt64(obj.Object, "status", field)
		return v
	}
	switch obj.GetKind() {
	case "Deployment", "StatefulSet", "ReplicaSet":
		// spec.replicas defaults to 1 when omitted; fall back to the observed
		// status count rather than reporting a misleading desired=0.
		desired, found, _ := unstructured.NestedInt64(obj.Object, "spec", "replicas")
		if !found {
			desired = statusInt("replicas")
		}
		return &WorkloadReplicas{
			Desired:     desired,
			Ready:       statusInt("readyReplicas"),
			Available:   statusInt("availableReplicas"),
			Updated:     statusInt("updatedReplicas"),
			Unavailable: statusInt("unavailableReplicas"),
		}
	case "DaemonSet":
		return &WorkloadReplicas{
			Desired:     statusInt("desiredNumberScheduled"),
			Ready:       statusInt("numberReady"),
			Available:   statusInt("numberAvailable"),
			Updated:     statusInt("updatedNumberScheduled"),
			Unavailable: statusInt("numberUnavailable"),
		}
	}
	return nil
}

// extractConditions reads `.status.conditions[]` off an unstructured object
// and projects each entry to our minimal Condition type. Missing or malformed
// conditions are silently skipped — the LLM works fine with a partial list.
func extractConditions(obj *unstructured.Unstructured) []Condition {
	raw, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}
	out := make([]Condition, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		c := Condition{
			Type:    asString(m["type"]),
			Status:  asString(m["status"]),
			Reason:  asString(m["reason"]),
			Message: asString(m["message"]),
		}
		if c.Type == "" {
			continue
		}
		out = append(out, c)
	}
	return out
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

// InventoryRef is a parsed Flux inventory ID. Exported so callers outside
// the readiness package (e.g. processor's HelmRelease-inventory expansion)
// can route on Kind without re-implementing the parser.
type InventoryRef struct {
	Namespace string
	Name      string
	Group     string
	Kind      string
}

// ParseInventoryID splits a Flux inventory ID `<namespace>_<name>_<group>_<kind>`
// into its four parts. The format is documented by kustomize-controller and
// shared by other Flux controllers (helm-controller HelmRelease inventory
// uses the same shape).
//
// A naive `strings.Split(id, "_")` is WRONG: the name field can itself contain
// underscores because cli-utils — the library that PRODUCES these IDs —
// transcodes ":" in RBAC names to "__" (e.g. the ClusterRole `system:evictioner`
// is stored as `_system__evictioner_…`, which splits into six parts, not four).
// We mirror cli-utils' object.ParseObjMetadata: anchor namespace from the front
// and kind+group from the back; the middle is the name, with "__" un-transcoded
// back to ":". This is reimplemented rather than imported because cli-utils'
// pkg/object pulls in cli-runtime + kustomize/kyaml for one function. Keeping it
// in lockstep with the encoder is why those RBAC resources parse correctly here
// instead of degrading to a bogus `Unknown / invalid inventory id` row.
func ParseInventoryID(id string) (InventoryRef, error) {
	invalid := func() (InventoryRef, error) {
		return InventoryRef{}, fmt.Errorf("invalid inventory id %q", id)
	}
	i := strings.Index(id, "_")
	if i < 0 {
		return invalid()
	}
	namespace, rest := id[:i], id[i+1:]
	j := strings.LastIndex(rest, "_")
	if j < 0 {
		return invalid()
	}
	kind := rest[j+1:]
	rest = rest[:j]
	k := strings.LastIndex(rest, "_")
	if k < 0 {
		return invalid()
	}
	group := rest[k+1:]
	name := strings.ReplaceAll(rest[:k], "__", ":")
	// Reject a name that STILL contains a single "_" after un-transcoding. This
	// is not a heuristic of ours — it mirrors cli-utils' own ParseObjMetadata,
	// which ends with the identical `strings.Contains(name, fieldSeparator)` →
	// "too many fields" guard. The invariant holds at the source: the encoder
	// (ObjMetadata.String) escapes only ":" (to "__", and only for RBAC kinds),
	// never "_", so it can never EMIT a name with a literal "_" — and Kubernetes
	// names can't contain "_" regardless (DNS-1123 for workloads/ConfigMaps/…;
	// RBAC path-segment names use ":"). Dropping this check would make us ACCEPT
	// ids cli-utils itself refuses, i.e. diverge from the producer. See the
	// underscore-rejection case in TestParseInventoryID.
	if name == "" || kind == "" || strings.Contains(name, "_") {
		return invalid()
	}
	return InventoryRef{Namespace: namespace, Name: name, Group: group, Kind: kind}, nil
}

// fallbackPreferredVersions is the legacy hardcoded group→version map. It's
// used (a) when no Resolver is wired (test paths, startup-time discovery
// failure) and (b) as the final fallback inside Resolver.APIVersion when a
// (group, kind) wasn't in MCP's discovery cache.
//
// New entries here are a band-aid — the correct fix for any new mismatch is
// to ensure the Resolver gets populated from MCP. The Flux inventory ID
// format omits the version, so this table can only ever be a guess.
var fallbackPreferredVersions = map[string]string{
	"helm.toolkit.fluxcd.io":         "v2",
	"notification.toolkit.fluxcd.io": "v1beta3",
	"image.toolkit.fluxcd.io":        "v1beta2",
	"autoscaling":                    "v2",
}

// Resolver maps (group, kind) → preferred apiVersion ("group/version", or
// just "version" for the core group). Populated from MCP's
// get_kubernetes_api_versions tool at bridge startup so non-v1 CRDs
// (example.com/v1alpha1, ricoberger.de/v1alpha1, …) route to the
// correct GVR. The Flux Kustomization inventory ID format records only the
// group with no version, which is why this lookup is necessary at all.
//
// Nil receiver is safe — falls back to fallbackPreferredVersions (legacy
// hardcoded table) so the Checker still works for v1 / known-Flux groups
// even if discovery couldn't run at startup.
type Resolver struct {
	cacheMu sync.RWMutex
	cache   map[groupKind]string
	// confirmed holds what the latest discovery attempt answered: the startup
	// entries, then each Refresh's response (empty when it failed or came back
	// empty). Unlike cache it isn't merged, so an entry a failed or partial
	// refresh left stale in cache isn't in it. See confirmedVersion.
	confirmed map[groupKind]string

	// refreshFn re-runs MCP discovery (nil → refresh disabled). Used to pick up
	// CRDs registered AFTER startup: a resource whose group the resolver doesn't
	// know is polled at the v1 fallback and reads NotFound, which the Checker
	// uses as the cue to refresh discovery and re-resolve at the real version.
	refreshFn   func(context.Context) ([]VersionEntry, error)
	refreshMu   sync.Mutex
	lastRefresh time.Time
	cooldown    time.Duration
}

type groupKind struct {
	Group string
	Kind  string
}

// VersionEntry is the source-agnostic input shape — mcp.GroupKindVersion
// fits this directly. Kept here (rather than importing the mcp package) so
// the readiness package has no dependency on mcp internals.
type VersionEntry struct {
	Group   string
	Kind    string
	Version string
}

// discoveryRefreshCooldown bounds how often Refresh actually re-runs discovery,
// so a single failing reconcile with many unknown-group NotFound rows triggers
// at most one discovery call.
const discoveryRefreshCooldown = 60 * time.Second

// NewResolver builds a Resolver from the discovery entries returned by MCP.
// Duplicate (group, kind) pairs from the source take the last entry, which
// matches MCP's "preferred version wins" semantics.
func NewResolver(entries []VersionEntry) *Resolver {
	cache := cacheFromEntries(entries)
	return &Resolver{cache: cache, confirmed: cache}
}

func cacheFromEntries(entries []VersionEntry) map[groupKind]string {
	cache := make(map[groupKind]string, len(entries))
	for _, e := range entries {
		cache[groupKind{Group: e.Group, Kind: e.Kind}] = e.Version
	}
	return cache
}

// WithRefresher attaches a discovery refresher (typically wrapping
// mcp.GetKubernetesAPIVersions) so the resolver can pick up CRDs registered
// after startup. Returns the receiver for chaining.
func (r *Resolver) WithRefresher(fn func(context.Context) ([]VersionEntry, error)) *Resolver {
	if r == nil {
		return r
	}
	r.refreshFn = fn
	if r.cooldown == 0 {
		r.cooldown = discoveryRefreshCooldown
	}
	return r
}

// APIVersion returns the cluster's preferred apiVersion for the given
// (group, kind), formatted as "group/version" (or just "version" for the
// core group, which has group=""). Lookup order:
//
//  1. Resolver cache (populated from MCP discovery at startup / Refresh).
//  2. fallbackPreferredVersions (legacy hardcoded per-group table).
//  3. "v1" as a last-resort default.
//
// Step 2 covers the case where discovery couldn't run at startup or a
// brand-new CRD landed after the cache was built. Step 3 is the original
// pre-resolver behavior and exists so resource fetches still attempt
// *something* even with zero discovery data.
func (r *Resolver) APIVersion(group, kind string) string {
	v, _ := r.apiVersionKnown(group, kind)
	return v
}

// apiVersionKnown is APIVersion plus whether the version came from the
// discovery cache (known=true) versus a fallback-table/v1 guess (known=false).
func (r *Resolver) apiVersionKnown(group, kind string) (string, bool) {
	if r != nil {
		r.cacheMu.RLock()
		v, ok := r.cache[groupKind{Group: group, Kind: kind}]
		r.cacheMu.RUnlock()
		if ok {
			return formatAPIVersion(group, v), true
		}
	}
	if v, ok := fallbackPreferredVersions[group]; ok {
		return formatAPIVersion(group, v), false
	}
	return formatAPIVersion(group, "v1"), false
}

// confirmedVersion reports whether the latest discovery attempt (the startup
// discovery, or the last Refresh that ran) listed (group, kind) at apiVersion.
// A cached entry alone doesn't confirm it: a failed, empty or partial refresh
// keeps the old entry in the cache, though the cluster may no longer serve
// that version.
func (r *Resolver) confirmedVersion(group, kind, apiVersion string) bool {
	if r == nil {
		return false
	}
	r.cacheMu.RLock()
	v, ok := r.confirmed[groupKind{Group: group, Kind: kind}]
	r.cacheMu.RUnlock()
	return ok && formatAPIVersion(group, v) == apiVersion
}

// Refresh re-runs MCP discovery and merges the result into the cache,
// debounced to at most once per cooldown. It's a no-op (changed=false) when no
// refresher is wired, the cooldown hasn't elapsed, or discovery came back
// empty. Merging rather than replacing means a degraded MCP server answering
// with an empty or partial list can't wipe known versions — with the cooldown
// then blocking a retry, non-v1 CRDs would resolve to guesses and read
// NotFound until the next refresh. An entry a partial response leaves out
// keeps its old version; if the cluster no longer serves that version, the
// resource reads NotFound, which triggers the next refresh (see checkOne), so
// a complete response corrects it. Such a kept entry is not confirmed (see
// confirmedVersion): the attempt's own response, empty on error, replaces the
// confirmed set. A CRD removed from the cluster lingers in the cache, which is
// harmless: lookups for it fail either way. Concurrent callers serialise on
// refreshMu; the loser sees the updated lastRefresh and returns immediately.
func (r *Resolver) Refresh(ctx context.Context) (changed bool, err error) {
	if r == nil || r.refreshFn == nil {
		return false, nil
	}
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	if !r.lastRefresh.IsZero() && time.Since(r.lastRefresh) < r.cooldown {
		return false, nil
	}
	entries, err := r.refreshFn(ctx)
	r.lastRefresh = time.Now() // stamp even on error so a broken discovery isn't hammered
	if err != nil {
		entries = nil
	}
	fresh := cacheFromEntries(entries)
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	r.confirmed = fresh
	if err != nil {
		return false, err
	}
	if len(fresh) == 0 {
		return false, nil
	}
	merged := make(map[groupKind]string, len(r.cache)+len(fresh))
	maps.Copy(merged, r.cache)
	maps.Copy(merged, fresh)
	r.cache = merged
	return true, nil
}

func formatAPIVersion(group, version string) string {
	if group == "" {
		return version
	}
	return group + "/" + version
}
