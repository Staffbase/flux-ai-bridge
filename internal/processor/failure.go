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
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
	"gopkg.in/yaml.v3"
)

const (
	// maxFailingPods caps how many failing pods' logs we pull into the prompt
	// across all swept namespaces. Beyond this the LLM gets diminishing returns
	// and the prompt token cost climbs faster than insight.
	maxFailingPods = 3
	// maxSweptNamespaces is the hard cap on how many distinct namespaces the
	// pod sweep / events fetch will visit. Namespaces with failing pods or
	// non-Current rows come first, so above the cap the sweep keeps those
	// and drops the rest (logged).
	maxSweptNamespaces = 5
	// maxConditionsRows caps how many non-Current inventory entries we
	// surface conditions for. Prompt growth bound; entries beyond this are
	// summarised by the kstatus one-liner already on every row.
	maxConditionsRows = 10
	// pendingPodGrace is how long a Pod can be in `Pending` before we count
	// it as "failing." Below this we assume it's still in normal startup.
	pendingPodGrace = 2 * time.Minute
	// maxWorkloadEventLines caps the lines get_kubernetes_events returns per
	// failing pod's owning workload. Scoped to one object family (not a whole
	// namespace), so a modest cap is plenty; bounds the pathological case of a
	// workload with many replicas all firing distinct warnings.
	maxWorkloadEventLines = 15
	// eventsSinceMargin pads the `since` cutoff sent to get_kubernetes_events
	// beyond the raw elapsed-time-since-pod-creation. Two reasons: (1) our
	// `since` is computed here, before the MCP round trip, while the server
	// computes its own cutoff from its OWN (later) time.Now() — the elapsed
	// network time would otherwise shift that cutoff past the pod's actual
	// creation moment; (2) Kubernetes timestamps serialize at one-second
	// precision, so a same-second boundary event could round the wrong way.
	// A little slack costs nothing (results are bounded by
	// maxWorkloadEventLines regardless) and prevents silently dropping the
	// pod's very first FailedScheduling/BackOff event — often the most
	// diagnostic one.
	eventsSinceMargin = 30 * time.Second
)

// failingWaitingReasons is the closed set of container `state.waiting.reason`
// values that flag a pod as broken. Anything outside this set (e.g.
// "ContainerCreating") is a transient startup state.
var failingWaitingReasons = map[string]bool{
	"CrashLoopBackOff":           true,
	"ImagePullBackOff":           true,
	"ErrImagePull":               true,
	"CreateContainerConfigError": true,
	"RunContainerError":          true,
	"InvalidImageName":           true,
}

// failingTerminatedReasons flags pods whose container exited non-cleanly.
var failingTerminatedReasons = map[string]bool{
	"Error":     true,
	"OOMKilled": true,
}

// fetchFailureContext gathers extra MCP context when the bridge has reason
// to believe something is wrong:
//
//   - Flux event's severity is Error (ReconciliationFailed / HealthCheckFailed)
//   - Any inventory row's kstatus is not Current (covers cases where the
//     event payload says success but a managed resource is actually broken).
//   - Any pod in the kstatus snapshot is not Current (a CrashLoopBackOff pod
//     often sits under a Deployment that still reads Current — without this
//     leg the LLM would analyse that failure with no logs or events).
//
// The fetch fans out across the namespaces sweepNamespaces picks, capped at
// maxSweptNamespaces. Within that namespace set the bridge pulls:
//
//   - Conditions: each non-Current inventory entry's `.status.conditions[]`,
//     because kstatus's summary line throws away controller-specific reasons
//     (HelmRelease "Released: False, reason: InstallFailed, …" never makes
//     it to the LLM otherwise).
//   - Events: namespace events surface kubelet / scheduler messages
//     (FailedScheduling, OOMKilled, BackOff) AND controller messages
//     (helm-controller install failures live in flux-system).
//   - Failing pods: across all swept namespaces, find up to maxFailingPods
//     pods in CrashLoopBackOff / Error / OOMKilled / stuck-Pending state,
//     pull their last 100 log lines.
//   - Metrics + Flux instance: namespace top output and the FluxInstance,
//     each gated by its FAILURE_FETCH_* toggle. Default off because they
//     produce dense numeric output the LLM tends to under-utilise.
//
// Returns "" only when the event is non-error AND every inventory row AND
// every snapshot pod is Current — i.e. the cluster genuinely looks healthy.
// hardFailure reports failure evidence the earlier pod snapshot may not have
// seen: a failing pod in the sweep's fresh pod list, or a core FailedCreate
// Warning event — pod creation rejected (e.g. by an admission webhook), so
// there's no pod to classify at all.
func (p *Processor) fetchFailureContext(ctx context.Context, event eventv1.Event, statuses, podStatuses []readiness.ResourceStatus) (out string, hardFailure bool) {
	if !shouldFetchFailureContext(event, statuses, podStatuses) {
		return "", false
	}

	var sections []string

	// 1. Inventory conditions — cheap (no extra MCP calls; readiness checker
	//    already populated these for non-Current rows).
	if conds := renderInventoryConditions(statuses); conds != "" {
		sections = append(sections, conds)
	}

	// 2. Determine the namespace set we'll fan out across.
	namespaces := sweepNamespaces(statuses, podStatuses)
	if len(namespaces) > maxSweptNamespaces {
		p.logger.Warn("failure context: too many namespaces to sweep; keeping the first by priority",
			"count", len(namespaces), "cap", maxSweptNamespaces, "dropped", namespaces[maxSweptNamespaces:])
		namespaces = namespaces[:maxSweptNamespaces]
	}

	// 3. Failing pod sweep runs up front. Two reasons:
	//    a) we use the failing pods to build per-namespace cutoffs that
	//       filter out events older than the current pod incarnation — old
	//       events from a previous (dead) pod are noise that misleads the
	//       LLM, especially when the pod has been replaced.
	//    b) the sweep is also what step 4 uses to fetch logs; doing it once
	//       at the top means we don't list pods twice.
	//
	//    With FAILURE_FETCH_LOGS_SCOPE=owned (the default) logs come only from
	//    failing pods a workload in this Kustomization's inventory owns, so in
	//    a shared namespace an unrelated app's logs never reach the prompt (or
	//    an external LLM). The sweep then looks at every failing pod, as owned
	//    ones may come after unrelated ones; `failing` itself — the pods the
	//    events and the hard-failure signal use — stays the first
	//    maxFailingPods, exactly as without the scope.
	ownedLogs := p.cfg.FailureFetchLogs && p.cfg.FailureFetchLogsScope != config.LogsScopeNamespace
	var failing []failingPod
	if p.cfg.FailureFetchEvents || p.cfg.FailureFetchLogs {
		limit := maxFailingPods
		if ownedLogs {
			limit = 0
		}
		failing = p.sweepFailingPods(ctx, namespaces, limit)
	}
	logPods := failing
	withheldLogs := 0
	if ownedLogs {
		logPods, withheldLogs = p.ownedFailingPods(ctx, failing, statuses)
	}
	failing = failing[:min(len(failing), maxFailingPods)]
	cutoffs := earliestFailingPodPerNamespace(failing)

	// 4. Failing pod logs — rendered BEFORE events on purpose. Logs are the
	//    application's own stderr, the most direct evidence of why a container
	//    crashed; events are kubelet/scheduler hints and (when a policy engine
	//    is present) a wall of advisory noise. Leading with logs keeps the
	//    prompt's physical ordering aligned with the SKILL's "prefer logs over
	//    events" instruction — otherwise the noisy events sit on top and can
	//    capture the root-cause framing (e.g. blaming a securityContext audit
	//    finding for a bad container arg).
	//
	//    For each failing pod we ask for the PREVIOUS container's logs first
	//    (kubectl -p equivalent): in a CrashLoop cycle the current container is
	//    just starting up while the previous one held the actual crash output.
	//    If the pod has never crashed (no previous instance) the MCP server
	//    returns an error; we fall back to the current container's logs.
	if p.cfg.FailureFetchLogs {
		for i, fp := range logPods {
			if i >= maxFailingPods {
				break
			}
			if fp.container == "" {
				p.logger.Warn("failure context: cannot fetch logs without container name",
					"pod", fp.namespace+"/"+fp.name)
				continue
			}
			logs, fromPrevious, err := p.fetchPodLogsWithFallback(ctx, fp)
			if err != nil {
				p.logger.Warn("failure context: pod logs fetch failed via MCP",
					"pod", fp.namespace+"/"+fp.name, "container", fp.container,
					"tool", "get_kubernetes_logs", "error", err.Error())
				continue
			}
			logs = strings.TrimSpace(logs)
			variant := "current"
			if fromPrevious {
				variant = "previous"
			}
			header := fmt.Sprintf("## Pod Logs — %s/%s (container: %s, %s; reason: %s)",
				fp.namespace, fp.name, fp.container, variant, fp.reason)
			if logs == "" {
				logs = "<no log output>"
			}
			sections = append(sections, header+"\n\n"+promptFence("", logs))
		}
		if withheldLogs > 0 {
			sections = append(sections, withheldLogsNote(withheldLogs))
		}
	}

	// 4b. Supplementary events for each failing pod's OWNING WORKLOAD, via
	//    MCP's get_kubernetes_events (flux-operator-mcp v0.59.0+) — precisely
	//    scoped to that one object family rather than a namespace-wide dump.
	//    For a Deployment/StatefulSet/DaemonSet/CronJob/Job the server also
	//    resolves and folds in the events of the ReplicaSets/Jobs/pods it
	//    owns, so a sibling replica's FailedScheduling/BackOff surfaces even
	//    though only ONE pod tripped isFailingPod. Falls back to the pod
	//    itself when podStatuses has no resolved Controller (a naked pod) OR
	//    when the resolved controller isn't one of the kinds the MCP tool
	//    expands (e.g. a bare "ReplicaSet/<name>" — resolvePodController's
	//    fallback when the owning Deployment couldn't be fetched, or a naked
	//    ReplicaSet with none): querying an unsupported kind returns only
	//    that object's own events, silently omitting the failing pod's.
	//
	//    This SUPPLEMENTS, not replaces, the namespace-wide sweep in step 5:
	//    the rendered event lines here carry no reportingComponent, so they
	//    can't feed that sweep's core-vs-third-party-noise classification —
	//    swapping it in wholesale would let policy-engine noise flood the
	//    section unfiltered. Grouped by (kind,name,namespace) so several
	//    failing replicas of the same workload cost one call, not one each —
	//    using the OLDEST pod's creation time in the group as the shared
	//    `since` cutoff (plus eventsSinceMargin), since the failing-pod list
	//    isn't age-ordered and keying off whichever pod is encountered first
	//    could clip an older sibling's events. The tool ALSO caps its own
	//    owned-pod resolution at its newest N pods (v0.60.0: 10) — when it
	//    reports that as truncated, every failing pod in the group gets an
	//    individual follow-up query so one buried under >N replicas isn't
	//    silently dropped from its own failure context.
	if p.cfg.FailureFetchEvents {
		type eventTarget struct {
			kind, name, namespace string
			oldest                time.Time
			unbounded             bool // an ungroupable creation timestamp was seen; don't filter by `since`
			pods                  []string
		}
		targets := map[string]*eventTarget{}
		var order []string
		for i, fp := range failing {
			if i >= maxFailingPods {
				break
			}
			kind, name := "Pod", fp.name
			if ctrl := controllerFor(podStatuses, fp.namespace, fp.name); ctrl != "" {
				if k, n, ok := strings.Cut(ctrl, "/"); ok && eventWorkloadKinds[k] {
					kind, name = k, n
				}
			}
			key := kind + "/" + fp.namespace + "/" + name
			t, ok := targets[key]
			if !ok {
				t = &eventTarget{kind: kind, name: name, namespace: fp.namespace}
				targets[key] = t
				order = append(order, key)
			}
			t.pods = append(t.pods, fp.name)
			if t.unbounded {
				continue
			}
			created, perr := time.Parse(time.RFC3339, fp.creationTimestamp)
			if perr != nil || created.IsZero() {
				t.unbounded = true
				continue
			}
			if t.oldest.IsZero() || created.Before(t.oldest) {
				t.oldest = created
			}
		}
		for _, key := range order {
			t := targets[key]
			var since time.Duration
			if !t.unbounded && !t.oldest.IsZero() {
				// sinceCreationTime returns exactly 0 both for "no timestamp"
				// and for a creation time the bridge's clock sees as not yet
				// in the past (clock skew) — that 0 means "no since filter",
				// so the margin must NOT be added unconditionally: doing so
				// would turn "unbounded" into "only the last 30s," clipping
				// events under skew instead of the opposite.
				if raw := sinceCreationTime(t.oldest); raw > 0 {
					since = raw + eventsSinceMargin
				}
			}
			events, truncated, err := p.mcp.GetKubernetesEvents(ctx, t.kind, t.name, t.namespace, since, maxWorkloadEventLines)
			if err != nil {
				p.logger.Warn("failure context: workload events fetch failed via MCP",
					"namespace", t.namespace, "kind", t.kind, "name", t.name, "tool", "get_kubernetes_events", "error", err.Error())
				continue
			}
			if events = strings.TrimSpace(events); events != "" {
				sections = append(sections, fmt.Sprintf("## Recent Events — %s/%s/%s\n\n%s", t.kind, t.namespace, t.name, promptFence("", events)))
			}
			// The tool caps its owned-pod resolution at its own newest-N pods
			// (v0.60.0: 10), sorted newest-first — in a workload with more
			// replicas, an OLDER failing pod (the very one that triggered
			// this fetch) can fall outside that window and be silently
			// dropped from the response above. When truncated, fall back to
			// a direct per-pod query for every failing pod in this group so
			// none of them go uncovered.
			if truncated && t.kind != "Pod" {
				for _, podName := range t.pods {
					podEvents, _, perr := p.mcp.GetKubernetesEvents(ctx, "Pod", podName, t.namespace, since, maxWorkloadEventLines)
					if perr != nil {
						p.logger.Warn("failure context: per-pod events fallback failed via MCP (workload query was truncated)",
							"namespace", t.namespace, "pod", podName, "tool", "get_kubernetes_events", "error", perr.Error())
						continue
					}
					if podEvents = strings.TrimSpace(podEvents); podEvents != "" {
						sections = append(sections, fmt.Sprintf("## Recent Events — Pod/%s/%s (workload query truncated)\n\n%s", t.namespace, podName, promptFence("", podEvents)))
					}
				}
			}
		}
	}

	// 5. Events per namespace, filtered to those at-or-after the earliest
	//    failing pod in that namespace. Drops stale events from prior
	//    pod incarnations that polluted the prompt.
	failedCreate := false
	if p.cfg.FailureFetchEvents {
		for _, ns := range namespaces {
			body, err := p.mcp.ListKubernetesResources(ctx, "v1", "Event", ns)
			if err != nil {
				p.logger.Warn("failure context: events fetch failed via MCP",
					"namespace", ns, "tool", "get_kubernetes_resources", "kind", "Event", "error", err.Error())
				continue
			}
			objects := notCurrentObjects(statuses, ns)
			failedCreate = failedCreate || hasFailedCreateEvent(body, cutoffs[ns], objects)
			rendered := filterAndRenderEvents(body, cutoffs[ns], objects)
			if rendered != "" {
				sections = append(sections, fmt.Sprintf("## Events — %s\n\n%s", ns, promptFence("yaml", rendered)))
			}
		}
	}

	// 6. Namespace metrics (toggle-gated, opt-in — dense output).
	if p.cfg.FailureFetchMetrics {
		for _, ns := range namespaces {
			m, err := p.mcp.GetKubernetesMetrics(ctx, ns)
			if err != nil {
				p.logger.Warn("failure context: metrics fetch failed via MCP",
					"namespace", ns, "tool", "get_kubernetes_metrics", "error", err.Error())
				continue
			}
			if m = strings.TrimSpace(m); m != "" {
				sections = append(sections, fmt.Sprintf("## Resource Metrics — %s\n\n%s", ns, promptFence("", m)))
			}
		}
	}

	// 7. Flux Operator status — cluster-wide, fetched once.
	if p.cfg.FailureFetchFluxInstance {
		if fi, err := p.mcp.GetFluxInstance(ctx); err != nil {
			p.logger.Warn("failure context: flux-instance fetch failed via MCP",
				"tool", "get_flux_instance", "error", err.Error())
		} else if fi = strings.TrimSpace(fi); fi != "" {
			sections = append(sections, "## Flux Operator Status\n\n"+promptFence("yaml", fi))
		}
	}

	return strings.Join(sections, "\n\n"), len(failing) > 0 || failedCreate
}

// hasFailedCreateEvent reports whether the event list holds a core Warning
// FailedCreate at or after cutoff for one of the given objects ("Kind/name"):
// a workload controller still rolling out couldn't create its pods. A
// Deployment surfaces this as a ReplicaFailure condition too, but a
// StatefulSet, DaemonSet or Job only has the event. Events of other objects
// in the namespace (an unrelated workload, a deleted one) don't count.
func hasFailedCreateEvent(yamlStream []byte, cutoff time.Time, objects map[string]bool) bool {
	for _, e := range parseEventList(yamlStream) {
		if !cutoff.IsZero() && e.freshest().Before(cutoff) {
			continue
		}
		if failedCreateFor(e, objects) {
			return true
		}
	}
	return false
}

// failedCreateFor reports whether e is a core Warning FailedCreate for one of
// the given objects ("Kind/name").
func failedCreateFor(e eventRecord, objects map[string]bool) bool {
	return e.Type == "Warning" && e.Reason == "FailedCreate" && e.isCoreKubernetesEvent() &&
		objects[e.InvolvedObject.Kind+"/"+e.InvolvedObject.Name]
}

// notCurrentObjects returns the "Kind/name" keys of the non-Current rows in
// namespace ns.
func notCurrentObjects(statuses []readiness.ResourceStatus, ns string) map[string]bool {
	out := map[string]bool{}
	for _, r := range statuses {
		if r.Namespace == ns && r.Status != "Current" {
			out[r.Kind+"/"+r.Name] = true
		}
	}
	return out
}

// fetchPodLogsWithFallback tries the previous container instance first
// (kubectl logs -p equivalent) and falls back to the current container's
// logs if MCP reports no previous instance exists. Returns the log text,
// whether it came from the previous instance, and any final error after
// the fallback.
func (p *Processor) fetchPodLogsWithFallback(ctx context.Context, fp failingPod) (logs string, fromPrevious bool, err error) {
	logs, err = p.mcp.GetKubernetesLogs(ctx, fp.name, fp.namespace, fp.container, true /* previous */)
	if err == nil {
		return logs, true, nil
	}
	// Common case: very first instance ever — no previous container yet.
	// MCP propagates the kube API's "previous terminated container … not
	// found" string verbatim. Anything that mentions "not found" or
	// "previous" we treat as "no -p available, try current".
	low := strings.ToLower(err.Error())
	if strings.Contains(low, "not found") || strings.Contains(low, "previous") {
		p.logger.Debug("failure context: no previous container, falling back to current",
			"pod", fp.namespace+"/"+fp.name, "container", fp.container, "previous_error", err.Error())
		logs, err = p.mcp.GetKubernetesLogs(ctx, fp.name, fp.namespace, fp.container, false)
		return logs, false, err
	}
	// Some other MCP-level error — surface as-is so the caller can WARN.
	return "", false, err
}

// controllerFor looks up a pod's resolved workload controller (e.g.
// "Deployment/api") from the pod kstatus snapshot, by (namespace, name).
// Returns "" when the pod isn't in the snapshot or has no owner (naked pod)
// — the caller falls back to querying the pod itself.
func controllerFor(podStatuses []readiness.ResourceStatus, namespace, name string) string {
	for _, ps := range podStatuses {
		if ps.Kind == "Pod" && ps.Namespace == namespace && ps.Name == name {
			return ps.Controller
		}
	}
	return ""
}

// eventWorkloadKinds mirrors flux-operator-mcp's own eventWorkloadKinds
// (cmd/mcp/k8s/events.go): the only kinds for which get_kubernetes_events
// resolves and folds in the owned ReplicaSets/Jobs/pods' events server-side.
// Any other controller kind (e.g. a bare "ReplicaSet" — resolvePodController's
// fallback when the owning Deployment couldn't be resolved) must NOT be
// queried directly: the tool would return only that object's own events,
// silently dropping the failing pod's.
var eventWorkloadKinds = map[string]bool{
	"Deployment":  true,
	"StatefulSet": true,
	"DaemonSet":   true,
	"CronJob":     true,
	"Job":         true,
}

// sinceCreationTime returns how long ago t was, or 0 (meaning "no since
// filter") when t is zero or not in the past — get_kubernetes_events rejects
// a non-positive `since` duration.
func sinceCreationTime(t time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}
	if d := time.Since(t); d > 0 {
		return d
	}
	return 0
}

// earliestFailingPodPerNamespace returns the earliest pod-creation time
// among failing pods in each namespace. Used as a per-namespace cutoff for
// the events filter so events emitted before the current pod incarnation
// (i.e. about a previous, replaced pod) are discarded.
func earliestFailingPodPerNamespace(failing []failingPod) map[string]time.Time {
	out := map[string]time.Time{}
	for _, fp := range failing {
		t, err := time.Parse(time.RFC3339, fp.creationTimestamp)
		if err != nil || t.IsZero() {
			continue
		}
		if existing, ok := out[fp.namespace]; !ok || t.Before(existing) {
			out[fp.namespace] = t
		}
	}
	return out
}

// maxNonCoreEvents caps the deduped non-core (third-party) events we surface.
// Policy engines are the worst case: one event per rule per pod, so a
// Deployment with 3 replicas violating 7 rules produces 21 near-identical
// events; deduping by message collapses that to 7, and this cap bounds even
// pathological cases.
const maxNonCoreEvents = 8

// coreEventSources is the bounded, stable set of built-in Kubernetes
// control-plane components that emit genuine workload events: the kubelet, the
// scheduler, and the kube-controller-manager controllers. An event from any of
// these is a real signal regardless of its reason. Third-party tools — policy
// engines, operators, service meshes — report under their own component names
// and therefore fall through to the non-core bucket. This is the allowlist that
// lets us flag "everything that isn't standard Kubernetes" without having to
// know what any particular add-on calls its events.
var coreEventSources = map[string]bool{
	"kubelet":                     true,
	"default-scheduler":           true,
	"kube-scheduler":              true,
	"replicaset-controller":       true,
	"deployment-controller":       true,
	"statefulset-controller":      true,
	"daemonset-controller":        true,
	"job-controller":              true,
	"cronjob-controller":          true,
	"node-controller":             true,
	"persistentvolume-controller": true,
	"persistentvolume-binder":     true,
	"horizontal-pod-autoscaler":   true,
	"endpoint-controller":         true,
	"endpointslice-controller":    true,
	"attachdetach-controller":     true,
	"taint-controller":            true,
	"pod-garbage-collector":       true,
	"endpoint-slice-controller":   true,
}

// coreEventReasons is a safety net for genuine events that arrive with a blank
// source/reportingComponent (some emitters leave both empty). These are the
// built-in kubelet/scheduler/controller reasons; matching one keeps the event
// in the genuine set even when we can't identify the emitter. It does NOT need
// to be exhaustive — the source allowlist catches the common case, and an
// unrecognised genuine event is only demoted (still shown), never dropped.
var coreEventReasons = map[string]bool{
	// kubelet — container/image lifecycle and failures
	"Created": true, "Started": true, "Failed": true, "Killing": true,
	"BackOff": true, "Pulling": true, "Pulled": true, "InspectFailed": true,
	"ErrImageNeverPull": true, "Unhealthy": true, "ProbeWarning": true,
	"OOMKilling": true, "FailedToCreateContainer": true, "FailedToStartContainer": true,
	"FailedCreatePodSandBox": true, "FailedPostStartHook": true, "FailedPreStopHook": true,
	"NetworkNotReady": true, "ExceededGracePeriod": true,
	// kubelet — node/volume
	"NodeReady": true, "NodeNotReady": true, "NodeNotSchedulable": true,
	"Rebooted": true, "Evicted": true, "FailedMount": true, "FailedAttachVolume": true,
	"FailedMapVolume": true, "VolumeResizeFailed": true, "SuccessfulAttachVolume": true,
	// scheduler
	"Scheduled": true, "FailedScheduling": true, "Preempted": true, "FailedPreemption": true,
	"TaintManagerEviction": true,
	// controllers
	"SuccessfulCreate": true, "FailedCreate": true, "SuccessfulDelete": true,
	"FailedDelete": true, "ScalingReplicaSet": true, "FailedSync": true,
	"SuccessfulRescale": true, "FailedGetResourceMetric": true,
	"FailedComputeMetricsReplicas": true, "Completed": true, "BackoffLimitExceeded": true,
	"DeadlineExceeded": true, "FailedDaemonPod": true, "RegisteredNode": true,
	"RemovingNode": true, "DeletingNode": true,
	// persistentvolume
	"ProvisioningFailed": true, "ProvisioningSucceeded": true, "FailedBinding": true,
	"ExternalProvisioning": true, "WaitForFirstConsumer": true, "WaitForPodScheduled": true,
}

// isCoreKubernetesEvent reports whether an event is a genuine Kubernetes
// control-plane signal (kubelet / scheduler / built-in controller) as opposed
// to a third-party event (policy engine, operator, mesh).
//
// Rather than enumerate what every policy engine emits — an open-ended set that
// differs per vendor (Kyverno `PolicyViolation`, Gatekeeper `AuditViolation`, …)
// — we allowlist the bounded, stable set of core Kubernetes emitters and treat
// everything else as non-core. New or unknown add-ons are handled with no code
// change.
//
// Three independent ways to qualify as genuine:
//  1. Emitted by a core component (the primary, stable signal).
//  2. Admission denial — an ENFORCE-mode policy block is a real deploy failure;
//     every engine's denial flows through the admission webhook, so the API
//     message says "admission webhook … denied the request". Pinning it here
//     keeps enforcement causes in the genuine set even though a non-core
//     component reported them.
//  3. A recognised built-in reason — safety net for events whose source fields
//     the emitter left blank.
//
// The failure mode is safe either way: a genuine event we fail to recognise is
// only demoted into the "additional events" section, never dropped.
func (e eventRecord) isCoreKubernetesEvent() bool {
	if coreEventSources[e.ReportingComponent] || coreEventSources[e.Source.Component] {
		return true
	}
	msg := strings.ToLower(e.Message)
	if strings.Contains(msg, "denied the request") || strings.Contains(msg, "admission webhook") {
		return true
	}
	// The reason safety net is for emitters that left both fields blank only:
	// a named third-party controller reusing a built-in reason is not core.
	return e.ReportingComponent == "" && e.Source.Component == "" && coreEventReasons[e.Reason]
}

// filterAndRenderEvents parses an events YAML stream and re-emits the survivors
// as a compact YAML-ish text block, structured so the LLM can tell a genuine
// Kubernetes failure apart from third-party noise:
//
//  1. Stale filter: drop events whose freshest timestamp predates `cutoff` (the
//     earliest failing pod's creation in the namespace) — they describe a previous,
//     replaced pod. Zero cutoff keeps every event. A FailedCreate of one of
//     the notCurrent objects ("Kind/name") is exempt: it is about the
//     workload, not a pod, and another workload's failing pod being newer
//     says nothing about it.
//  2. Classification by emitter, not by guessing vendors: genuine core
//     Kubernetes events (kubelet / scheduler / built-in controllers — see
//     isCoreKubernetesEvent) vs. everything else (third-party tools, most often
//     a policy/admission engine).
//  3. Ordering within core events: Warning before Normal, so the real failure
//     signal (BackOff, Failed, FailedScheduling, Unhealthy) leads rather than
//     being buried under routine Pulled/Created/Started lines.
//  4. Non-core events are kept but segregated under a labelled comment, deduped
//     by message (policy engines repeat per pod per rule) and capped at
//     maxNonCoreEvents — reported as context, never mistaken for the cause.
func filterAndRenderEvents(yamlStream []byte, cutoff time.Time, notCurrent map[string]bool) string {
	events := parseEventList(yamlStream)
	var warnings, normals, nonCore []eventRecord
	for _, e := range events {
		if !cutoff.IsZero() && e.freshest().Before(cutoff) && !failedCreateFor(e, notCurrent) {
			continue
		}
		switch {
		case !e.isCoreKubernetesEvent():
			nonCore = append(nonCore, e)
		case e.Type == "Warning":
			warnings = append(warnings, e)
		default:
			normals = append(normals, e)
		}
	}
	if len(warnings) == 0 && len(normals) == 0 && len(nonCore) == 0 {
		return ""
	}

	var sb strings.Builder
	writeEvent := func(e eventRecord) {
		fmt.Fprintf(&sb, "- type: %s\n  reason: %s\n  count: %d\n  lastTimestamp: %s\n  involvedObject: %s/%s\n  message: %s\n",
			e.Type, e.Reason, e.Count, e.LastTimestamp,
			e.InvolvedObject.Kind, e.InvolvedObject.Name,
			strings.TrimSpace(e.Message))
	}
	for _, e := range append(warnings, normals...) {
		writeEvent(e)
	}
	if len(nonCore) > 0 {
		fmt.Fprintf(&sb, "# additional non-core events (from a third-party controller, likely a policy/admission engine — advisory, usually not the deploy cause)\n")
		seen := map[string]struct{}{}
		shown := 0
		for _, e := range nonCore {
			key := strings.TrimSpace(e.Message)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			if shown >= maxNonCoreEvents {
				continue
			}
			writeEvent(e)
			shown++
		}
		if remaining := len(seen) - shown; remaining > 0 {
			fmt.Fprintf(&sb, "# +%d more distinct non-core event(s)\n", remaining)
		}
	}
	return strings.TrimRight(sb.String(), "\n")
}

// eventRecord is the projection of a v1.Event we need for filtering +
// rendering. Time fields are strings on purpose (yaml.v3 silently fails
// decoding metav1.Time/MicroTime into time.Time) — we parse where needed.
type eventRecord struct {
	Type           string `yaml:"type"`
	Reason         string `yaml:"reason"`
	Message        string `yaml:"message"`
	Count          int    `yaml:"count"`
	EventTime      string `yaml:"eventTime"`
	FirstTimestamp string `yaml:"firstTimestamp"`
	LastTimestamp  string `yaml:"lastTimestamp"`
	// ReportingComponent (modern events) and Source.Component (legacy events)
	// name the emitter. Genuine workload events come from a bounded set of core
	// control-plane components; everything else is third-party (see
	// isCoreKubernetesEvent). Both fields are checked because emitters populate
	// one or the other inconsistently.
	ReportingComponent string `yaml:"reportingComponent"`
	Source             struct {
		Component string `yaml:"component"`
	} `yaml:"source"`
	Metadata struct {
		CreationTimestamp string `yaml:"creationTimestamp"`
		// Annotations carry, on kustomize-controller's Events, the source
		// revision the reconcile ran (kustomizeRevisionAnnotation); prune
		// attribution uses it to ignore another revision's prune.
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	InvolvedObject struct {
		Kind string `yaml:"kind"`
		Name string `yaml:"name"`
		UID  string `yaml:"uid"`
	} `yaml:"involvedObject"`
}

// freshest picks the most recent of the four candidate time fields. Events
// use a mix of legacy (firstTimestamp/lastTimestamp) and modern (eventTime)
// time fields depending on emitter; we want the latest moment associated
// with the event for the cutoff comparison.
func (e eventRecord) freshest() time.Time {
	var best time.Time
	for _, raw := range []string{e.LastTimestamp, e.EventTime, e.FirstTimestamp, e.Metadata.CreationTimestamp} {
		if raw == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			continue
		}
		if t.After(best) {
			best = t
		}
	}
	return best
}

// parseEventList tolerates the same two shapes parsePodList does: a
// kind: List wrapper or a stream of "---"-separated Event docs.
func parseEventList(yamlStream []byte) []eventRecord {
	var wrapper struct {
		Items []eventRecord `yaml:"items"`
	}
	if err := yaml.Unmarshal(yamlStream, &wrapper); err == nil && len(wrapper.Items) > 0 {
		return wrapper.Items
	}
	var out []eventRecord
	decoder := yaml.NewDecoder(bytes.NewReader(yamlStream))
	for {
		var e eventRecord
		if err := decoder.Decode(&e); err != nil {
			break
		}
		if e.Reason == "" && e.Type == "" && e.Message == "" {
			continue
		}
		out = append(out, e)
	}
	return out
}

// shouldFetchFailureContext is the entry-gate for the diagnostic-fetch
// pipeline. The bridge wants to pull failure context whenever something
// looks broken — either Flux has formally given up (severity=Error) or
// kstatus's own evaluation of the inventory disagrees with the event's
// nominal success.
//
// "Non-Current" includes InProgress / Failed / NotFound / Unknown — anything
// that isn't a clean kstatus pass. The trade-off: during legitimate
// transient InProgress windows we'll fetch logs that probably show normal
// startup output. Acceptable; the LLM will report "still progressing"
// rather than miss a real problem.
//
// podStatuses (the pod kstatus snapshot) gets the same treatment as the
// inventory: a failing pod under an all-Current inventory (CrashLoopBackOff
// under a Current Deployment) must trigger the fetch, or the LLM analyses
// the failure blind.
func shouldFetchFailureContext(event eventv1.Event, statuses, podStatuses []readiness.ResourceStatus) bool {
	if event.Severity == eventv1.EventSeverityError {
		return true
	}
	for _, s := range statuses {
		if s.Status != "Current" {
			return true
		}
	}
	for _, s := range podStatuses {
		if s.Status != "Current" {
			return true
		}
	}
	return false
}

// sweepNamespaces returns the deduped namespace list the failure-context
// fetch fans out across, most relevant first: namespaces of NON-Current pods
// in the kstatus snapshot (where a CrashLoopBackOff pod under a Current
// Deployment lives — its logs are the whole point of the sweep), then those
// of non-Current inventory rows. Only when nothing is visibly wrong (an
// error event over an all-Current inventory) does it fall back to every
// inventory namespace. Healthy namespaces don't count otherwise: a broad
// Kustomization with resources in dozens of namespaces must not push the
// one with the failing pod past the cap. The empty namespace (cluster-scoped
// resources) is skipped. Each group is sorted for deterministic prompt
// ordering (helps Anthropic prompt cache stay warm across reconciles).
//
// The Kustomization's OWN namespace is deliberately NOT included
// unconditionally. Most Kustomizations live in `flux-system` alongside the
// flux controllers themselves; sweeping that namespace surfaces ~4 unrelated
// controller pods and a stream of routine flux events that have nothing to
// do with the workload the Kustomization manages. Same-namespace
// Kustomizations (e.g. a Kustomization in `apps` managing a Deployment in
// `apps`) are still covered because their inventory entries carry the same
// namespace, and the pod-snapshot namespace joins whenever a pod there is
// unhealthy.
func sweepNamespaces(statuses, podStatuses []readiness.ResourceStatus) []string {
	var out []string
	seen := map[string]struct{}{}
	addGroup := func(rows []readiness.ResourceStatus, include func(readiness.ResourceStatus) bool) {
		var group []string
		for _, r := range rows {
			if _, dup := seen[r.Namespace]; dup || r.Namespace == "" || !include(r) {
				continue
			}
			seen[r.Namespace] = struct{}{}
			group = append(group, r.Namespace)
		}
		sort.Strings(group)
		out = append(out, group...)
	}
	notCurrent := func(r readiness.ResourceStatus) bool { return r.Status != "Current" }
	addGroup(podStatuses, notCurrent)
	addGroup(statuses, notCurrent)
	if len(out) == 0 {
		addGroup(statuses, func(readiness.ResourceStatus) bool { return true })
	}
	return out
}

// renderInventoryConditions emits a "## Inventory Conditions" section for
// each non-Current inventory entry, surfacing its controller-specific
// `.status.conditions[]` text. Entries without conditions (cluster-scoped
// objects, fetch failures) contribute nothing.
func renderInventoryConditions(statuses []readiness.ResourceStatus) string {
	var sections []string
	for _, s := range statuses {
		if len(sections) >= maxConditionsRows {
			break
		}
		if s.Status == string(readinessCurrent) {
			continue
		}
		if len(s.Conditions) == 0 {
			continue
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "### %s/%s/%s\n\n", s.Kind, s.Namespace, s.Name)
		for _, c := range s.Conditions {
			fmt.Fprintf(&sb, "- **%s**: %s", c.Type, c.Status)
			if c.Reason != "" {
				fmt.Fprintf(&sb, " (reason: %s)", c.Reason)
			}
			if c.Message != "" {
				fmt.Fprintf(&sb, "\n  %s", c.Message)
			}
			sb.WriteByte('\n')
		}
		sections = append(sections, sb.String())
	}
	if len(sections) == 0 {
		return ""
	}
	return "## Inventory Conditions (non-Current entries)\n\n" + strings.Join(sections, "\n")
}

// readinessCurrent shadows readiness's "Current" constant so we can compare
// without importing kstatus directly. Kept in sync with kstatus.CurrentStatus.
const readinessCurrent = "Current"

type failingPod struct {
	namespace string
	name      string
	// container is the name of the failing container we'll request logs for.
	// MCP's get_kubernetes_logs requires this; pods can have multiple
	// containers and the tool will NOT default to "the only one".
	container string
	// reason is the specific signal that flagged this pod as failing
	// (e.g. "CrashLoopBackOff", "OOMKilled", "Pending>2m"). Surfaced in
	// the prompt header so the LLM doesn't have to derive it from logs.
	reason string
	// creationTimestamp is the pod's RFC3339 metadata.creationTimestamp,
	// used to build the per-namespace cutoff for filtering stale events.
	// Events older than the earliest failing pod in a namespace describe a
	// previous (replaced) pod and are dropped.
	creationTimestamp string
	// ownerRefs are the pod's metadata.ownerReferences, for the owned-logs
	// scope (see ownedFailingPods).
	ownerRefs []ownerReference
}

// sweepFailingPods lists pods in each swept namespace and selects up to limit
// pods that satisfy isFailingPod (limit <= 0: all of them). Continues on
// per-namespace MCP errors so a single broken namespace doesn't blackhole the
// whole analysis.
func (p *Processor) sweepFailingPods(ctx context.Context, namespaces []string, limit int) []failingPod {
	p.logger.Debug("failure context: pod sweep starting", "namespaces", namespaces)
	full := func(n int) bool { return limit > 0 && n >= limit }
	out := make([]failingPod, 0, maxFailingPods)
	now := time.Now()
	for _, ns := range namespaces {
		if full(len(out)) {
			break
		}
		body, err := p.mcp.ListKubernetesResources(ctx, "v1", "Pod", ns)
		if err != nil {
			// MCP genuinely failed to list pods in this namespace — surfacing
			// as WARN (not Debug) so operators can correlate missing
			// pod-logs sections in PR comments with the upstream failure.
			p.logger.Warn("failure context: pod list failed via MCP",
				"namespace", ns, "tool", "get_kubernetes_resources", "kind", "Pod", "error", err.Error())
			continue
		}
		pods, err := parsePodList(body)
		if err != nil {
			// MCP returned something we couldn't parse — that's a contract
			// break worth surfacing too.
			p.logger.Warn("failure context: pod list parse failed",
				"namespace", ns, "error", err.Error(), "body_bytes", len(body))
			continue
		}
		p.logger.Debug("failure context: pod list parsed", "namespace", ns, "total_pods", len(pods))
		for _, pod := range pods {
			if full(len(out)) {
				break
			}
			container, reason, failing := isFailingPod(pod, now)
			if !failing {
				continue
			}
			// Pod-level failures (phase=Failed / Pending>2m) come back with
			// container="". Fall back to the first container's name so we
			// can still request logs — single-container pods are by far
			// the common case.
			if container == "" && len(pod.Status.ContainerStatuses) > 0 {
				container = pod.Status.ContainerStatuses[0].Name
			}
			p.logger.Debug("failure context: flagged failing pod",
				"namespace", ns, "pod", pod.Metadata.Name, "container", container, "reason", reason)
			out = append(out, failingPod{
				namespace:         ns,
				name:              pod.Metadata.Name,
				container:         container,
				reason:            reason,
				creationTimestamp: pod.Metadata.CreationTimestamp,
				ownerRefs:         pod.Metadata.OwnerReferences,
			})
		}
	}
	p.logger.Debug("failure context: pod sweep complete", "failing_count", len(out))
	return out
}

// ownedFailingPods keeps the failing pods whose logs FAILURE_FETCH_LOGS_SCOPE=
// owned allows: those owned by a workload in this Kustomization's inventory
// (statuses — the readiness rows, HelmRelease children included). Ownership
// is resolved like the pod scan does it (resolvePodController: Pod →
// ReplicaSet → Deployment; StatefulSet, DaemonSet and Job directly), plus one
// hop Job → CronJob, as a CronJob's runs are never inventory entries
// themselves. It starts from the failing pod's own ownerReferences, never
// from the pod scan's row of the same name: that row may describe an earlier
// pod the name belonged to. A pod the inventory lists by name counts too. A pod whose owner
// can't be resolved (naked, a ReplicaSet lookup that failed, a Job read that
// failed) is left out: fail closed, an unrelated app's logs must not reach the
// prompt on a guess. Returns at most maxFailingPods pods and the number of
// failing pods left out for ownership.
func (p *Processor) ownedFailingPods(ctx context.Context, failing []failingPod, statuses []readiness.ResourceStatus) (owned []failingPod, withheld int) {
	inventory := make(map[string]bool, len(statuses))
	for _, r := range statuses {
		inventory[r.Kind+"/"+r.Namespace+"/"+r.Name] = true
	}
	rsCache := map[string]string{}
	jobOwners := map[string]string{}
	for _, fp := range failing {
		if !p.podOwnedByInventory(ctx, fp, inventory, rsCache, jobOwners) {
			withheld++
			p.logger.Debug("failure context: logs withheld, pod not owned by the inventory",
				"pod", fp.namespace+"/"+fp.name)
			continue
		}
		if len(owned) < maxFailingPods {
			owned = append(owned, fp)
		}
	}
	return owned, withheld
}

// podOwnedByInventory reports whether a workload in the inventory set
// ("Kind/namespace/name") owns the failing pod, resolved from the pod's own
// ownerReferences as the failure sweep read them. jobOwners caches a Job's
// owner ("Kind/name", "" for none) by "namespace/job".
func (p *Processor) podOwnedByInventory(ctx context.Context, fp failingPod, inventory map[string]bool, rsCache, jobOwners map[string]string) bool {
	listed := func(kind, name string) bool { return inventory[kind+"/"+fp.namespace+"/"+name] }
	if listed("Pod", fp.name) {
		return true
	}
	ctrl := p.resolvePodController(ctx, podInfo{Metadata: podMetadata{
		Name: fp.name, Namespace: fp.namespace, OwnerReferences: fp.ownerRefs,
	}}, rsCache)
	kind, name, ok := strings.Cut(ctrl, "/")
	if !ok {
		return false // naked pod: no owner
	}
	if listed(kind, name) {
		return true
	}
	if kind != "Job" {
		return false
	}
	key := fp.namespace + "/" + name
	owner, cached := jobOwners[key]
	if !cached {
		body, err := p.mcp.GetKubernetesResource(ctx, "batch/v1", "Job", name, fp.namespace)
		if err != nil {
			// Not cached: the next pod of this Job retries.
			p.logger.Debug("failure context: Job fetch failed; pod's owner unresolved",
				"job", key, "error", err.Error())
			return false
		}
		// replicaSetController reads any object's controller ownerReference.
		if k, n := replicaSetController(body); k != "" {
			owner = k + "/" + n
		}
		jobOwners[key] = owner
	}
	ownerKind, ownerName, ok := strings.Cut(owner, "/")
	return ok && ownerKind == "CronJob" && listed(ownerKind, ownerName)
}

// withheldLogsNote tells the model that FAILURE_FETCH_LOGS_SCOPE=owned left
// some failing pods' logs out, so it doesn't invent what they would show.
func withheldLogsNote(n int) string {
	return fmt.Sprintf("## Pod Logs — not included\n\n"+
		"Logs of %d failing pod(s) in the swept namespaces were not fetched: no workload in this Kustomization's "+
		"inventory owns them, or their owner could not be resolved. Their pod status and events may still appear; "+
		"do not guess what their logs contain or blame them for this Kustomization without other evidence.", n)
}

// podInfo is the minimal projection of a Kubernetes Pod we need for failure
// detection. The MCP returns full YAML; only these fields drive the
// is-failing decision and the prompt header.
//
// CreationTimestamp is a string (RFC3339) rather than time.Time on purpose:
// yaml.v3's auto time-decoding rejects K8s timestamp strings under some
// edge cases (trailing 'Z' vs explicit offset), and a failed field decode
// fails the WHOLE pod, silently emptying the sweep. Parse explicitly where
// we need a time.
type podInfo struct {
	Metadata podMetadata `yaml:"metadata"`
	Status   podStatus   `yaml:"status"`
}

// podMetadata is a named type (rather than an anonymous struct inline on
// podInfo) so tests can build fixtures with field-name initialization and
// leave OwnerReferences out — every test that pre-dates the controller
// resolver doesn't care about ownerRefs.
type podMetadata struct {
	Name              string           `yaml:"name"`
	Namespace         string           `yaml:"namespace"`
	CreationTimestamp string           `yaml:"creationTimestamp"`
	DeletionTimestamp string           `yaml:"deletionTimestamp"`
	OwnerReferences   []ownerReference `yaml:"ownerReferences"`
}

type podStatus struct {
	Phase             string            `yaml:"phase"`
	Conditions        []podCondition    `yaml:"conditions"`
	ContainerStatuses []containerStatus `yaml:"containerStatuses"`
}

// podCondition is one entry of a pod's status.conditions[]. Only the Ready
// condition is read (see podKstatus).
type podCondition struct {
	Type   string `yaml:"type"`
	Status string `yaml:"status"`
}

// ownerReference is the minimal projection of metadata.ownerReferences[] we
// need to identify the workload controller owning a pod. The `controller`
// flag distinguishes the primary owner (the workload controller) from
// secondary refs (e.g. blockOwnerDeletion-only entries).
type ownerReference struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Name       string `yaml:"name"`
	UID        string `yaml:"uid"`
	Controller bool   `yaml:"controller"`
}

type containerStatus struct {
	Name         string `yaml:"name"`
	Ready        bool   `yaml:"ready"`
	RestartCount int    `yaml:"restartCount"`
	State        struct {
		Waiting *struct {
			Reason  string `yaml:"reason"`
			Message string `yaml:"message"`
		} `yaml:"waiting,omitempty"`
		Terminated *struct {
			Reason   string `yaml:"reason"`
			ExitCode int    `yaml:"exitCode"`
			Message  string `yaml:"message"`
		} `yaml:"terminated,omitempty"`
	} `yaml:"state"`
	// LastState surfaces the prior termination during a CrashLoop cycle's
	// brief Running window: the container has just restarted, current
	// state.terminated is nil, but lastState.terminated still reports
	// the reason for the most recent crash.
	LastState struct {
		Terminated *struct {
			Reason   string `yaml:"reason"`
			ExitCode int    `yaml:"exitCode"`
			Message  string `yaml:"message"`
		} `yaml:"terminated,omitempty"`
	} `yaml:"lastState"`
}

// isFailingPod applies the "loose" criteria from the design grill. Returns
// the name of the failing container (empty for pod-level reasons like
// Pending>2m or phase=Failed), the reason string, and whether the pod is
// failing at all.
//
//   - any container in a known-bad `waiting.reason` (CrashLoopBackOff,
//     ImagePullBackOff, etc.)
//   - any container terminated with Error / OOMKilled
//   - any container that has restarted at least once AND is not currently
//     ready — catches crashlooping pods caught mid-cycle in the brief
//     Running window between two crashes (state.waiting is momentarily
//     nil, but restartCount > 0 + ready=false is unambiguous)
//   - any container whose lastState.terminated reports a failing reason
//     AND is not currently ready — same root case, different signal
//   - phase == Failed
//   - phase == Pending and pod is older than pendingPodGrace
//
// The first match wins for both container and reason; we don't list multiple
// reasons per pod (the LLM gets logs anyway).
//
// phase == Succeeded short-circuits to healthy BEFORE the container checks:
// every container of a completed pod reports ready=false, so a Job pod that
// flaked once and then ran to completion would otherwise trip the
// RestartCount>0 && !Ready branch and read as Failed forever (until GC'd).
// Succeeded means every container exited 0 — definitively not failing.
func isFailingPod(p podInfo, now time.Time) (container, reason string, failing bool) {
	if p.Status.Phase == "Succeeded" {
		return "", "", false
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil && failingWaitingReasons[cs.State.Waiting.Reason] {
			return cs.Name, cs.State.Waiting.Reason, true
		}
		if cs.State.Terminated != nil && failingTerminatedReasons[cs.State.Terminated.Reason] {
			return cs.Name, cs.State.Terminated.Reason, true
		}
		if cs.RestartCount > 0 && !cs.Ready {
			if cs.LastState.Terminated != nil && cs.LastState.Terminated.Reason != "" {
				return cs.Name, "Restarting/" + cs.LastState.Terminated.Reason, true
			}
			return cs.Name, "Restarting", true
		}
	}
	if p.Status.Phase == "Failed" {
		return "", "Failed", true
	}
	if p.Status.Phase == "Pending" {
		if t, err := time.Parse(time.RFC3339, p.Metadata.CreationTimestamp); err == nil {
			if now.Sub(t) > pendingPodGrace {
				return "", "Pending>2m", true
			}
		}
	}
	return "", "", false
}

// scanPodStatuses snapshots pod kstatus across the given namespaces (the
// caller passes the Kustomization's own namespace — see Process). Confining the
// scan to that one namespace, rather than every namespace the inventory touches,
// is deliberate: a Kustomization like grafana owns dashboards/alert-rules in
// 100+ service namespaces but only RUNS workloads in its own namespace, so
// sweeping the lot would dump the whole cluster's (unrelated) pods and cost one
// MCP list per namespace. Independent of the readiness poll — pods are
// inherently volatile and would prevent the inventory loop from ever settling.
//
// Reuses the failure-context primitives (parsePodList + isFailingPod + the
// podInfo projection) so there's one definition of "is this pod failing?".
//
// Returns nil when ReadinessIncludePods is disabled or no namespaces are
// given. MCP failures on individual namespaces are logged WARN and that
// namespace contributes nothing; we never abort the whole scan over one fetch.
//
// Ownership filter: NOT applied. Every pod in the scanned namespace is
// reported, even if owned by a sibling Kustomization sharing the namespace.
// Operators can disable the section via READINESS_INCLUDE_PODS=false.
//
// The second return value, `scanned`, reports whether the snapshot produced a
// definitive picture of every namespace. It is true only when the scan ran AND
// every namespace returned a conclusive answer (a pod list, or
// flux-operator-mcp's "No resources found" — its way of saying zero pods). It
// is false when pods are disabled, no namespaces were given, or ANY namespace
// fetch/parse failed for a non-empty reason. Callers use it to distinguish
// "scanned, found zero pods" (render "no pods running") from "couldn't look"
// (omit the Workload Status section) — see renderStatusTables.
//
// Terminating pods stay out of the rows. The third return value,
// `shuttingDown`, reports whether any of them is not owned by a Job — a
// workload pod on its way out (e.g. a StatefulSet replacing db-0), which the
// pod quiet period must not mistake for a quiet namespace.
// rsCache is the ReplicaSet → Deployment cache keyed by
// "<namespace>/<rs-name>". Many pods of one Deployment share a single
// ReplicaSet owner, and an RS never changes its owning Deployment, so the
// caller owns the map and reuses it across settle-loop iterations — without
// it every 5s re-scan would re-fetch the same immutable mappings via MCP.
func (p *Processor) scanPodStatuses(ctx context.Context, namespaces []string, rsCache map[string]string) ([]readiness.ResourceStatus, bool, bool) {
	if !p.cfg.ReadinessIncludePods {
		return nil, false, false
	}
	if len(namespaces) == 0 {
		return nil, false, false
	}
	// No namespace cap here: the caller passes the Kustomization's own single
	// namespace, so there's nothing to fan out over. (The failure-context sweep
	// over the problem namespaces caps via maxSweptNamespaces.)
	now := time.Now()
	var out []readiness.ResourceStatus
	// Start optimistic; any namespace we can't conclusively read flips this to
	// false so we never claim "no pods running" off incomplete data.
	scanned := true
	shuttingDown := false
	for _, ns := range namespaces {
		body, err := p.mcp.ListKubernetesResources(ctx, "v1", "Pod", ns)
		if err != nil {
			if isNoResourcesFound(err) {
				// Definitive empty: the namespace has zero pods.
				// flux-operator-mcp signals this as a tool error rather than
				// an empty list, so treat it as a successful "no pods" answer
				// rather than a fetch failure — `scanned` stays true.
				p.logger.Debug("pod kstatus snapshot: namespace has no pods", "namespace", ns)
				continue
			}
			scanned = false
			p.logger.Warn("pod kstatus snapshot: list failed via MCP",
				"namespace", ns, "tool", "get_kubernetes_resources", "kind", "Pod", "error", err.Error())
			continue
		}
		pods, err := parsePodListAll(body)
		if err != nil {
			scanned = false
			p.logger.Warn("pod kstatus snapshot: parse failed",
				"namespace", ns, "error", err.Error(), "body_bytes", len(body))
			continue
		}
		for _, pod := range pods {
			if podTerminating(pod) {
				if owner := primaryOwnerRef(pod.Metadata.OwnerReferences); owner == nil || owner.Kind != "Job" {
					shuttingDown = true
				}
				continue
			}
			rs := podKstatus(pod, now)
			rs.Controller = p.resolvePodController(ctx, pod, rsCache)
			if t, err := time.Parse(time.RFC3339, pod.Metadata.CreationTimestamp); err == nil {
				rs.CreatedAt = t
			}
			out = append(out, rs)
		}
	}
	// Superseded CronJob runs are dropped later, once the Job list tells which
	// Jobs a CronJob owns (see dropSupersededJobPods).
	// Sort by (namespace, controller, name) so pods owned by the same
	// workload cluster together in the rendered table. Empty controller
	// (naked pods) sorts first within a namespace by string compare.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].Controller != out[j].Controller {
			return out[i].Controller < out[j].Controller
		}
		return out[i].Name < out[j].Name
	})
	p.logger.Debug("pod kstatus snapshot complete",
		"namespaces", len(namespaces), "pods", len(out), "scanned", scanned)
	return out, scanned, shuttingDown
}

// cronJobSlotPattern matches the Kubernetes CronJob → Job naming contract:
// `<cronjob-name>-<scheduled-time-in-unix-minutes>`. The numeric suffix is the
// schedule slot, monotonically increasing per run (8 digits in this era; ≥5
// required so version-ish suffixes like "-v2" or ordinal "-001" Jobs never
// match).
var cronJobSlotPattern = regexp.MustCompile(`^(.+)-(\d{5,})$`)

// dropSupersededJobPods keeps, per CronJob, only the pods belonging to its
// MOST RECENT Job run. CronJobs retain finished Jobs per
// {successful,failed}JobsHistoryLimit, so the namespace scan sees pods of
// several runs at once — and an old run's failed pod must not outvote a newer
// run that succeeded: the CronJob's CURRENT health is its latest run
// (observed in practice where a lingering CreateContainerConfigError pod
// from a fixed run kept the comment red). Conversely a latest-run failure
// still reads Failed, with the noise of its predecessors' pods removed.
//
// Only Jobs a CronJob owns (cronRuns, from cronRunFallbackRows) are grouped,
// by the naming contract above and per namespace: standalone Jobs that merely
// carry a numeric suffix (e.g. `migrate-20260101`) are separate workloads, and
// a newer one's healthy pod must not hide an older one's failure. Pods whose
// controller isn't such a Job pass through untouched. Filters in place — the
// caller owns the slice.
func dropSupersededJobPods(pods []readiness.ResourceStatus, cronRuns map[string]bool) []readiness.ResourceStatus {
	slotOf := func(r readiness.ResourceStatus) (family string, slot int64, ok bool) {
		jobName, isJob := strings.CutPrefix(r.Controller, "Job/")
		if !isJob || !cronRuns[jobName] {
			return "", 0, false
		}
		m := cronJobSlotPattern.FindStringSubmatch(jobName)
		if m == nil {
			return "", 0, false
		}
		n, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			return "", 0, false
		}
		return r.Namespace + "|" + m[1], n, true
	}
	newest := make(map[string]int64)
	for _, r := range pods {
		if fam, slot, ok := slotOf(r); ok {
			// Explicit presence check rather than relying on the zero
			// default: a slot that parses to 0 (a literal "-00000" suffix)
			// must still register its family, not read as "never seen".
			if cur, seen := newest[fam]; !seen || slot > cur {
				newest[fam] = slot
			}
		}
	}
	if len(newest) == 0 {
		return pods
	}
	kept := pods[:0]
	for _, r := range pods {
		if fam, slot, ok := slotOf(r); ok && slot < newest[fam] {
			continue
		}
		kept = append(kept, r)
	}
	return kept
}

// jobInfo is the slice of a batch/v1 Job the cron-run fallback needs.
type jobInfo struct {
	Metadata struct {
		Name            string           `yaml:"name"`
		Namespace       string           `yaml:"namespace"`
		OwnerReferences []ownerReference `yaml:"ownerReferences"`
	} `yaml:"metadata"`
	Status struct {
		Conditions []struct {
			Type    string `yaml:"type"`
			Status  string `yaml:"status"`
			Reason  string `yaml:"reason"`
			Message string `yaml:"message"`
		} `yaml:"conditions"`
	} `yaml:"status"`
}

// parseJobList mirrors parsePodList for batch/v1 Jobs: a `kind: List` wrapper
// or a bare multi-doc stream.
func parseJobList(yamlStream []byte) ([]jobInfo, error) {
	var wrapper struct {
		Items []jobInfo `yaml:"items"`
	}
	if err := yaml.Unmarshal(yamlStream, &wrapper); err == nil && len(wrapper.Items) > 0 {
		return wrapper.Items, nil
	}
	var out []jobInfo
	decoder := yaml.NewDecoder(bytes.NewReader(yamlStream))
	for {
		var j jobInfo
		err := decoder.Decode(&j)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode job list: %w", err)
		}
		if j.Metadata.Name == "" {
			continue
		}
		out = append(out, j)
	}
}

// cronRunFallbackRows closes the gap the pod scan can't see: a CronJob run
// whose pods are ALREADY GONE. Failed pods are cleaned per
// {successful,failed}JobsHistoryLimit, so a CronJob that fails every run
// oscillates between "failing pod visible" and "namespace samples clean" —
// and the analysis flapped flux-failed ↔ flux-success with it (observed on a
// real deploy). The Job OBJECTS outlive their pods, so for each CronJob family
// whose NEWEST run has no pod in the scan, this emits one synthetic row
// judged from that Job's own conditions: Failed=True → Failed (stable across
// pod cleanup), Complete=True → Current, otherwise InProgress — a run still
// active, or one created after the pod scan, has proven nothing yet, so it
// can't read green. Families whose newest run HAS visible pods are
// skipped: the pod rows already carry the verdict with container-level
// detail — except a Failed=True run none of whose visible pods reads Failed
// (only a succeeded pod is left), which still gets its Failed row. A CronJob run is a Job a CronJob owns and named after it
// (`<cronjob>-<slot>`); the second return value holds their names for
// dropSupersededJobPods. One MCP list per analysis; errors yield no rows and
// no runs — no superseded run is dropped, so an old run's failure stays
// visible rather than being hidden on a guess.
func (p *Processor) cronRunFallbackRows(ctx context.Context, ns string, podRows []readiness.ResourceStatus) ([]readiness.ResourceStatus, map[string]bool) {
	if !p.cfg.ReadinessIncludePods || ns == "" {
		return nil, nil
	}
	body, err := p.mcp.ListKubernetesResources(ctx, "batch/v1", "Job", ns)
	if err != nil {
		if !isNoResourcesFound(err) {
			p.logger.Debug("cron-run fallback: job list failed; relying on pod scan alone",
				"namespace", ns, "error", err.Error())
		}
		return nil, nil
	}
	jobs, err := parseJobList(body)
	if err != nil {
		p.logger.Debug("cron-run fallback: job list unparseable; relying on pod scan alone",
			"namespace", ns, "error", err.Error())
		return nil, nil
	}
	type run struct {
		slot int64
		job  jobInfo
	}
	newest := make(map[string]run)
	cronRuns := map[string]bool{}
	for _, j := range jobs {
		m := cronJobSlotPattern.FindStringSubmatch(j.Metadata.Name)
		if m == nil {
			continue // one-shot Job, not a CronJob run
		}
		if owner := primaryOwnerRef(j.Metadata.OwnerReferences); owner == nil || owner.Kind != "CronJob" || owner.Name != m[1] {
			continue // a standalone Job that merely ends in digits
		}
		slot, perr := strconv.ParseInt(m[2], 10, 64)
		if perr != nil {
			continue
		}
		cronRuns[j.Metadata.Name] = true
		if cur, seen := newest[m[1]]; !seen || slot > cur.slot {
			newest[m[1]] = run{slot: slot, job: j}
		}
	}
	if len(newest) == 0 {
		return nil, cronRuns
	}
	// Families whose newest run already shows pods are judged by those rows —
	// unless the Job failed and none of them reads Failed: a run with several
	// completions can fail (deadline exceeded, its running pods deleted)
	// after one pod succeeded, and that pod alone would read green.
	covered := make(map[string]bool, len(podRows))
	podFailed := make(map[string]bool, len(podRows))
	for _, r := range podRows {
		if jobName, isJob := strings.CutPrefix(r.Controller, "Job/"); isJob {
			covered[jobName] = true
			podFailed[jobName] = podFailed[jobName] || r.Status == "Failed"
		}
	}
	var out []readiness.ResourceStatus
	for fam, r := range newest {
		if covered[r.job.Metadata.Name] && (podFailed[r.job.Metadata.Name] || !jobFailed(r.job)) {
			continue
		}
		row := readiness.ResourceStatus{
			Kind: "Job", Name: r.job.Metadata.Name, Namespace: ns,
			Controller: "CronJob/" + fam,
			Status:     "InProgress", Message: "run not finished; its pods weren't seen",
		}
		for _, c := range r.job.Status.Conditions {
			if c.Type == "Failed" && c.Status == "True" {
				msg := c.Message
				if msg == "" {
					msg = c.Reason
				}
				if msg == "" {
					msg = "Job failed"
				}
				row.Status = "Failed"
				if covered[r.job.Metadata.Name] {
					row.Message = msg + " (no failed pod of the run left)"
				} else {
					row.Message = msg + " (run's pods already removed)"
				}
				break
			}
			if c.Type == "Complete" && c.Status == "True" {
				row.Status = "Current"
				row.Message = "run completed"
			}
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, cronRuns
}

// jobFailed reports whether the Job carries Failed=True.
func jobFailed(j jobInfo) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == "Failed" && c.Status == "True" {
			return true
		}
	}
	return false
}

// dropOlderCronRunPods removes the pod rows of CronJob runs older than a run
// cronRunFallbackRows reported (one whose own pods are already gone). Each
// CronJob is judged by its newest run; without this, an older run's failed
// pod — still around under failedJobsHistoryLimit — read ❌ next to a newer
// run that succeeded. Only pods of CronJob runs (cronRuns) are dropped.
func dropOlderCronRunPods(pods, fallback []readiness.ResourceStatus, cronRuns map[string]bool) []readiness.ResourceStatus {
	newest := make(map[string]int64, len(fallback))
	for _, f := range fallback {
		if fam, slot, ok := cronRunSlot(f.Name); ok {
			newest[fam] = slot
		}
	}
	if len(newest) == 0 {
		return pods
	}
	return slices.DeleteFunc(pods, func(r readiness.ResourceStatus) bool {
		job, isJob := strings.CutPrefix(r.Controller, "Job/")
		if !isJob || !cronRuns[job] {
			return false
		}
		fam, slot, ok := cronRunSlot(job)
		newestSlot, known := newest[fam]
		return ok && known && slot < newestSlot
	})
}

// cronRunSlot splits a CronJob-spawned Job name into family and schedule slot.
func cronRunSlot(jobName string) (family string, slot int64, ok bool) {
	m := cronJobSlotPattern.FindStringSubmatch(jobName)
	if m == nil {
		return "", 0, false
	}
	slot, err := strconv.ParseInt(m[2], 10, 64)
	return m[1], slot, err == nil
}

// waitPodsSettled re-runs the pod kstatus snapshot until every pod reaches a
// terminal kstatus (Current/Failed) and the namespace has stayed quiet for
// the pod quiet period, or the budget elapses, then returns the final
// snapshot. It exists so the bridge doesn't comment on a
// transient mid-rollout pod: a Deployment can report Current while its newly
// created pod is still "Running, containers not all ready" (InProgress), and a
// one-shot snapshot would capture that stale state.
//
// Termination semantics (via readiness.AllSettled): a starting pod (InProgress)
// keeps the loop running, so it's waited out into Current; a genuinely failing
// pod (isFailingPod → Failed) is terminal; a failing workload pod ends the
// wait at once, while a failed Job pod still waits out the quiet period (see
// anyWorkloadPodFailed). A pod stuck InProgress (e.g. Pending/unschedulable) is
// reported when the budget runs out — identical to the old one-shot behaviour.
//
// Quiet period (ReadinessPodQuietPeriod): settled pods alone aren't enough.
// An operator creates its workload pods only after Flux applied the custom
// resource, and kstatus reads that resource Current as soon as it exists, so
// the first scans can see only the old, ready pods. The pods count as settled
// only once the namespace has been quiet for the quiet period, counted from
// quietFrom (the start of the readiness poll, so the inventory poll's time
// counts) or from the last change to the workload pods, whichever is later:
// a pod appearing (one created during the inventory poll or recreated under
// the same name counts), shutting down, or gone since the previous scan (see
// podChanges). Job pods don't count: a frequent CronJob would otherwise keep
// the window open, and its runs are judged by the newest run anyway. A
// Failed workload pod skips the window (a Job pod's failure doesn't; see
// anyWorkloadPodFailed). A window the budget cuts short is NOT settled
// (fail closed): the namespace was still changing.
//
// The budget is the remainder of the shared readiness deadline, and it bounds
// each scan's MCP reads as well as the sleeps. A non-positive budget (the
// inventory poll already consumed it) still yields exactly one snapshot,
// bounded by minPodScan. Pods disabled (or no namespaces to scan) is vacuously settled —
// pods simply aren't part of the verdict. A FAILED scan (MCP fetch/parse
// error → scanned=false) is NOT settled: it's retried on the interval like an
// unsettled pod, and if it never recovers within the budget the result is
// settled=false, so the outcome degrades to a warning instead of reading
// success off pods we never saw — the exact window an MCP degradation would
// otherwise hide a crashlooping rollout in.
//
// quietOpen is set only when the wait ends unsettled with every pod of the
// last scan settled, so the open quiet window was the only thing missing. It
// names the change that last restarted the window, for the comment and the
// LLM prompt; the tables alone show nothing pending then.
func (p *Processor) waitPodsSettled(ctx context.Context, namespaces []string, interval, budget time.Duration, quietFrom time.Time) (pods []readiness.ResourceStatus, scanned, settled bool, quietOpen string) {
	if !p.cfg.ReadinessIncludePods || len(namespaces) == 0 {
		return nil, false, true, ""
	}
	// Shared across iterations: RS → Deployment mappings are immutable, so
	// re-resolving them on every re-scan would be pure repeated MCP traffic.
	rsCache := map[string]string{}
	deadline := time.Now().Add(budget)
	quietPeriod := p.cfg.ReadinessPodQuietPeriod
	changes := podChanges{}
	for {
		var shuttingDown bool
		// The scan's MCP reads (pod list, ReplicaSet lookups) are bounded by
		// the budget too, so a slow read can't eat into the time reserved for
		// analysis; a scan cut short fails and leaves the wait unsettled.
		scanStart := time.Now()
		scanCtx, cancelScan := context.WithDeadline(ctx, laterOf(deadline, scanStart.Add(minPodScan)))
		pods, scanned, shuttingDown = p.scanPodStatuses(scanCtx, namespaces, rsCache)
		cancelScan()
		quietOpen = ""
		if scanned {
			now := time.Now()
			windowStart := quietFrom
			if last := changes.observe(pods, shuttingDown, now); last.After(windowStart) {
				windowStart = last
			}
			// The rows were listed during the scan, so the window must have
			// been quiet long enough when the scan STARTED: a pod created
			// while a slow scan ran is in none of its rows.
			if readiness.AllSettled(pods) {
				if anyWorkloadPodFailed(pods) || scanStart.Sub(windowStart) >= quietPeriod {
					return pods, scanned, true, ""
				}
				quietOpen = quietWindowOpen(changes.lastWhat, windowStart.Equal(quietFrom), quietPeriod)
			}
		}
		if !time.Now().Before(deadline) {
			// Budget exhausted: report the partial snapshot, unsettled.
			return pods, scanned, false, quietOpen
		}
		// Never sleep past the budget: the last scan lands at the deadline
		// instead of up to a whole interval after it.
		select {
		case <-ctx.Done():
			return pods, scanned, false, quietOpen
		case <-time.After(min(interval, time.Until(deadline))):
		}
	}
}

// quietWindowOpen says why settled pods didn't count yet: what last changed
// the namespace, or the start of the readiness poll when nothing changed
// since. No elapsed time: the comment body is the comment's stability key,
// and a seconds count would repost an unchanged state on every re-eval.
func quietWindowOpen(lastChange string, fromPollStart bool, quietPeriod time.Duration) string {
	since := "the readiness poll started"
	if !fromPollStart && lastChange != "" {
		since = lastChange
	}
	return fmt.Sprintf("every pod reads `Current`, but %s less than %s (the pod quiet period) before the last pod scan, so the pods hadn't settled.",
		since, quietPeriod)
}

// minPodScan is the least time one pod scan gets, even when the readiness
// budget is already spent, so the wait still takes its one snapshot.
const minPodScan = time.Second

// laterOf returns the later of two times.
func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// anyWorkloadPodFailed reports whether a pod not owned by a Job is Failed. Such
// a failure decides the verdict, so the quiet period needn't be waited out.
// A Job pod's failure doesn't count: it may belong to a superseded CronJob
// run that Process drops later (newest-run reconciliation), and skipping the
// window on it could then read healthy before late pods were looked for.
func anyWorkloadPodFailed(pods []readiness.ResourceStatus) bool {
	for _, p := range pods {
		if p.Status == "Failed" && !strings.HasPrefix(p.Controller, "Job/") {
			return true
		}
	}
	return false
}

// podChanges tracks when the workload pods (not owned by a Job) last
// changed, for the pod quiet period: a pod appearing, a pod shutting down, or
// a pod gone since the previous scan. A pod's identity includes its creation
// time, so a pod recreated under the same name is a new pod.
type podChanges struct {
	// sightings maps a pod identity to when the bridge first saw it.
	sightings map[string]podSighting
	// prevPresent holds the identities counted in the previous scan.
	prevPresent map[string]bool
	// last is the latest change across all scans so far, so a pod that was
	// seen once and is gone by the next scan still counts.
	last time.Time
	// lastWhat describes the change at last, e.g. "pod `db-0` was created".
	lastWhat string
}

type podSighting struct {
	firstSeen time.Time
	// inFirstScan is true for pods already there at the first scan.
	inFirstScan bool
}

// observe records one successful scan taken at now — its workload pods and
// whether any workload pod was shutting down — and returns when the
// namespace last changed, across all scans so far.
//
// A pod that shows up in a later scan appeared when the bridge first saw it —
// the bridge's own clock, whatever the API server's says. A pod already there
// at the first scan may have appeared during the inventory poll, which only
// its creationTimestamp can tell; it counts from the earlier of that and the
// first scan, so an API server clock running ahead can't push the window
// into the future. A pod shutting down changes the namespace at now (a
// StatefulSet creates the replacement only once the old pod is gone), and so
// does a pod counted in the previous scan and missing now: it may have shut
// down between two scans without either seeing it terminate. Pods without a
// creationTimestamp are skipped. Zero when nothing counts.
func (c *podChanges) observe(pods []readiness.ResourceStatus, shuttingDown bool, now time.Time) time.Time {
	firstScan := c.sightings == nil
	if firstScan {
		c.sightings = map[string]podSighting{}
	}
	current := make(map[string]bool, len(pods))
	for _, p := range pods {
		if strings.HasPrefix(p.Controller, "Job/") || p.CreatedAt.IsZero() {
			continue
		}
		id := p.Namespace + "/" + p.Name + "@" + p.CreatedAt.Format(time.RFC3339)
		current[id] = true
		s, ok := c.sightings[id]
		if !ok {
			s = podSighting{firstSeen: now, inFirstScan: firstScan}
			c.sightings[id] = s
		}
		appeared := s.firstSeen
		if s.inFirstScan && p.CreatedAt.Before(appeared) {
			appeared = p.CreatedAt
		}
		c.record(appeared, fmt.Sprintf("pod `%s` was created", p.Name))
	}
	if shuttingDown {
		c.record(now, "a pod was shutting down")
	}
	for id := range c.prevPresent {
		if !current[id] {
			_, nameAt, _ := strings.Cut(id, "/")
			name, _, _ := strings.Cut(nameAt, "@")
			c.record(now, fmt.Sprintf("pod `%s` was removed", name))
			break
		}
	}
	c.prevPresent = current
	return c.last
}

// record moves the last change forward to t if t is later.
func (c *podChanges) record(t time.Time, what string) {
	if t.After(c.last) {
		c.last, c.lastWhat = t, what
	}
}

// isNoResourcesFound reports whether err is flux-operator-mcp's "No resources
// found" signal — which it returns as a tool error (not an empty list) when a
// namespace contains zero objects of the requested kind. The bridge treats
// this as a definitive empty result rather than a fetch failure, so the
// Workload Status section can say "no pods running" instead of being omitted.
func isNoResourcesFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "No resources found")
}

// resolvePodController walks metadata.ownerReferences to identify the
// user-facing workload controller for a pod. For Deployments, the chain is
// Pod → ReplicaSet → Deployment; we chase the extra hop via MCP so the
// table shows "Deployment/api" rather than the implementation-detail
// "ReplicaSet/api-7d4f9b6c8". For StatefulSet/DaemonSet/Job/etc. the direct
// owner IS the workload controller — no extra fetch needed.
//
// Returns "" for naked pods (no ownerReferences) — the renderer treats
// empty controller as a separate group at the top of the namespace's
// section.
//
// rsCache (keyed by "<namespace>/<rs-name>") dedupes the RS fetch across
// pods of the same Deployment. On MCP fetch failure we fall back to the
// direct owner ("ReplicaSet/<name>") rather than aborting — partial data
// is better than no controller info at all. Only successful lookups are
// cached, so a failed one is retried on the next scan.
func (p *Processor) resolvePodController(ctx context.Context, pod podInfo, rsCache map[string]string) string {
	primary := primaryOwnerRef(pod.Metadata.OwnerReferences)
	if primary == nil {
		return ""
	}
	if primary.Kind != "ReplicaSet" {
		// StatefulSet / DaemonSet / Job / CronJob owner — already the
		// workload controller.
		return primary.Kind + "/" + primary.Name
	}
	cacheKey := pod.Metadata.Namespace + "/" + primary.Name
	if cached, ok := rsCache[cacheKey]; ok {
		return cached
	}
	rsYAML, err := p.mcp.GetKubernetesResource(ctx, "apps/v1", "ReplicaSet", primary.Name, pod.Metadata.Namespace)
	fallback := "ReplicaSet/" + primary.Name
	if err != nil {
		// Not cached: a transient read failure must not pin the fallback for
		// the whole settle wait — the next scan retries the lookup.
		p.logger.Debug("pod controller: ReplicaSet fetch failed; falling back to RS identifier",
			"replicaset", cacheKey, "error", err.Error())
		return fallback
	}
	parentKind, parentName := replicaSetController(rsYAML)
	if parentKind == "" {
		// Naked ReplicaSet (no Deployment) — surface the RS itself.
		rsCache[cacheKey] = fallback
		return fallback
	}
	result := parentKind + "/" + parentName
	rsCache[cacheKey] = result
	return result
}

// primaryOwnerRef returns the first ownerReference flagged controller=true,
// or the first ref if none carries the flag (Kubernetes guarantees at most
// one controller=true entry, but some operators write owner refs without
// setting it).
func primaryOwnerRef(refs []ownerReference) *ownerReference {
	for i := range refs {
		if refs[i].Controller {
			return &refs[i]
		}
	}
	if len(refs) > 0 {
		return &refs[0]
	}
	return nil
}

// replicaSetController extracts the (Kind, Name) of a ReplicaSet's owner —
// typically the parent Deployment. Returns ("", "") for a naked RS or any
// parse failure. Uses the same yaml.Node-based traversal as the HelmRelease
// inventory parser to tolerate envelope variations from MCP.
func replicaSetController(data []byte) (kind, name string) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc yaml.Node
		if err := dec.Decode(&doc); err != nil {
			return "", ""
		}
		if k, n := replicaSetControllerFromNode(&doc); k != "" {
			return k, n
		}
	}
}

func replicaSetControllerFromNode(n *yaml.Node) (kind, name string) {
	if n == nil {
		return "", ""
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return replicaSetControllerFromNode(n.Content[0])
	}
	metadata := mappingChild(n, "metadata")
	if metadata == nil {
		return "", ""
	}
	refs := mappingChild(metadata, "ownerReferences")
	if refs == nil || refs.Kind != yaml.SequenceNode {
		return "", ""
	}
	// Prefer the controller=true entry; fall back to the first ref.
	var fallbackKind, fallbackName string
	for _, ref := range refs.Content {
		k := scalarChildValue(ref, "kind")
		nm := scalarChildValue(ref, "name")
		if k == "" || nm == "" {
			continue
		}
		ctrl := scalarChildValue(ref, "controller")
		if ctrl == "true" {
			return k, nm
		}
		if fallbackKind == "" {
			fallbackKind, fallbackName = k, nm
		}
	}
	return fallbackKind, fallbackName
}

// podKstatus turns a podInfo snapshot into a kstatus-shaped ResourceStatus.
// The "is this pod in trouble?" decision delegates to isFailingPod so the
// failure-context fetcher and the always-on snapshot agree on what counts
// as failing. The non-failing leg is straight Phase + Ready logic that
// mirrors what sigs.k8s.io/cli-utils/pkg/kstatus emits for Pod resources.
//
//   - Phase=Running + all containers Ready  → Current, unless the pod's own
//     Ready condition isn't True (a pending readiness gate) → InProgress
//   - Phase=Succeeded                       → Current (completed Job pods)
//   - Phase=Pending                         → InProgress
//   - anything else                         → Unknown (with Phase as Message)
//
// Conditions is left empty — kstatus's pod check doesn't surface controller
// conditions, and the existing renderer only emits the Inventory Conditions
// section for inventory rows.
func podKstatus(pod podInfo, now time.Time) readiness.ResourceStatus {
	rs := readiness.ResourceStatus{
		Kind:      "Pod",
		Name:      pod.Metadata.Name,
		Namespace: pod.Metadata.Namespace,
	}
	if container, reason, failing := isFailingPod(pod, now); failing {
		rs.Status = "Failed"
		rs.Message = podFailureMessage(pod, container, reason)
		return rs
	}
	switch pod.Status.Phase {
	case "Running":
		ready := true
		for _, cs := range pod.Status.ContainerStatuses {
			if !cs.Ready {
				ready = false
				break
			}
		}
		switch {
		case !ready:
			rs.Status = "InProgress"
			rs.Message = "containers not all ready"
		case !podReadyConditionOK(pod):
			// Ready containers aren't a ready pod: a readiness gate (e.g. a
			// load balancer's target health) keeps the pod's own Ready
			// condition False until it passes.
			rs.Status = "InProgress"
			rs.Message = "pod not ready"
		default:
			rs.Status = "Current"
			rs.Message = "all containers ready"
		}
	case "Succeeded":
		rs.Status = "Current"
		rs.Message = "completed"
	case "Pending":
		rs.Status = "InProgress"
		rs.Message = "pending"
	default:
		rs.Status = "Unknown"
		rs.Message = pod.Status.Phase
	}
	return rs
}

// podReadyConditionOK reports whether the pod's Ready condition, when the pod
// reports one, is True. A Running pod always carries it; a snapshot without
// conditions at all falls back to the container readiness alone.
func podReadyConditionOK(pod podInfo) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return true
}

// maxTerminationMessageRunes caps the termination message quoted in a
// Workload Status cell. The termination message lives only in the pod status
// (pod logs don't carry it unless the pod sets FallbackToLogsOnError), so text
// past the cap reaches neither the comment nor the LLM prompt.
const maxTerminationMessageRunes = 200

// podFailureMessage renders the Message cell for a pod isFailingPod flagged.
// Kubernetes' terminated reason "Error" only says the process exited non-zero,
// which the Failed status already conveys, so a terminated container reports
// its exit code instead, plus its termination message when it wrote one
// (usually the actual error). A container that is crash-looping or restarting
// reports how its last run ended. Pod-level reasons (container == "", e.g.
// phase=Failed, Pending>2m) pass through unchanged.
func podFailureMessage(pod podInfo, container, reason string) string {
	if container == "" {
		return reason
	}
	name := "container `" + container + "`"
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != container {
			continue
		}
		if t := cs.State.Terminated; t != nil && t.Reason == reason {
			return name + " " + terminationText(t.Reason, t.ExitCode, t.Message)
		}
		last := cs.LastState.Terminated
		if strings.HasPrefix(reason, "Restarting") {
			if last == nil {
				return name + " is restarting"
			}
			return name + " is restarting after it " + terminationText(last.Reason, last.ExitCode, last.Message)
		}
		if last != nil {
			return name + " " + reason + "; last run " + terminationText(last.Reason, last.ExitCode, last.Message)
		}
		break
	}
	return name + " " + reason
}

// terminationText phrases a container termination as a verb phrase, e.g.
// "exited with code 1" or "was OOMKilled (exit code 137)", followed by the
// container's termination message when it wrote one.
func terminationText(reason string, exitCode int, message string) string {
	var s string
	switch reason {
	case "OOMKilled":
		s = fmt.Sprintf("was OOMKilled (exit code %d)", exitCode)
	case "Error", "":
		s = fmt.Sprintf("exited with code %d", exitCode)
	default:
		s = fmt.Sprintf("exited with code %d (%s)", exitCode, reason)
	}
	if msg := compactLine(message, maxTerminationMessageRunes); msg != "" {
		s += ": " + msg
	}
	return s
}

// parsePodList tolerates both shapes the MCP tool can return: a `kind: List`
// wrapper with `.items` or a stream of `---`-separated Pod docs. The MCP
// server's documented contract doesn't fix which one; we accept either.
//
// Pods already being deleted (metadata.deletionTimestamp set) are dropped:
// during a rollout the old ReplicaSet's pods shut down — not ready, often
// with restarts or exit code 143 from SIGTERM — and would otherwise read as
// failing, turning a healthy rollout ❌ and pulling their logs into the
// failure context. They're going away; the pods replacing them are judged.
//
// A body that is not valid YAML returns an error — callers flip their
// `scanned` flag on it so the bridge never asserts "no pods running" off a
// response it couldn't actually read.
func parsePodList(yamlStream []byte) ([]podInfo, error) {
	pods, err := parsePodListAll(yamlStream)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(pods, podTerminating), nil
}

// parsePodListAll is parsePodList without the terminating-pod filter, for
// callers that need to know about pods on their way out.
func parsePodListAll(yamlStream []byte) ([]podInfo, error) {
	var wrapper struct {
		Items []podInfo `yaml:"items"`
	}
	if err := yaml.Unmarshal(yamlStream, &wrapper); err == nil && len(wrapper.Items) > 0 {
		return wrapper.Items, nil
	}
	var out []podInfo
	decoder := yaml.NewDecoder(bytes.NewReader(yamlStream))
	for {
		var p podInfo
		err := decoder.Decode(&p)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode pod list: %w", err)
		}
		if p.Metadata.Name == "" {
			continue
		}
		out = append(out, p)
	}
}

// podTerminating reports whether the pod is being deleted.
func podTerminating(p podInfo) bool {
	return p.Metadata.DeletionTimestamp != ""
}

// promptFence wraps cluster-sourced text (pod logs, events) in a code fence
// longer than any backtick run inside it. A log line carrying ``` would
// otherwise close a plain fence early, and whatever followed — text an app
// could have logged on purpose — would read as prompt instructions rather
// than quoted data.
func promptFence(lang, content string) string {
	longest, run := 0, 0
	for _, r := range content {
		if r == '`' {
			run++
			longest = max(longest, run)
		} else {
			run = 0
		}
	}
	fence := strings.Repeat("`", max(3, longest+1))
	return fence + lang + "\n" + content + "\n" + fence
}
