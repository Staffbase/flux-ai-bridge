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

package readiness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseInventoryID(t *testing.T) {
	tests := []struct {
		id        string
		want      InventoryRef
		wantError bool
	}{
		{
			id:   "default_nginx_apps_Deployment",
			want: InventoryRef{Namespace: "default", Name: "nginx", Group: "apps", Kind: "Deployment"},
		},
		{
			id:   "kube-system_coredns__ConfigMap",
			want: InventoryRef{Namespace: "kube-system", Name: "coredns", Group: "", Kind: "ConfigMap"},
		},
		{
			id:   "production_my-app_apps_StatefulSet",
			want: InventoryRef{Namespace: "production", Name: "my-app", Group: "apps", Kind: "StatefulSet"},
		},
		// RBAC names transcode ":" to "__" (cli-utils colonTranscoded), so the
		// name field contains underscores and a naive 4-way split mis-counts the
		// fields. These are the exact shapes that surfaced as bogus "invalid
		// inventory id" rows on a VPA Kustomization.
		{
			id:   "_system__evictioner_rbac.authorization.k8s.io_ClusterRole",
			want: InventoryRef{Namespace: "", Name: "system:evictioner", Group: "rbac.authorization.k8s.io", Kind: "ClusterRole"},
		},
		{
			id:   "_system__vpa-status-reader-binding_rbac.authorization.k8s.io_ClusterRoleBinding",
			want: InventoryRef{Namespace: "", Name: "system:vpa-status-reader-binding", Group: "rbac.authorization.k8s.io", Kind: "ClusterRoleBinding"},
		},
		{
			id:   "vertical-pod-autoscaler_system__leader-locking-vpa-recommender_rbac.authorization.k8s.io_Role",
			want: InventoryRef{Namespace: "vertical-pod-autoscaler", Name: "system:leader-locking-vpa-recommender", Group: "rbac.authorization.k8s.io", Kind: "Role"},
		},
		{id: "missing_parts", wantError: true},
		{id: "", wantError: true},
		{id: "a_b_c_d_extra", wantError: true},
		// A literal "_" left in the name after un-transcoding is rejected ON
		// PURPOSE — it mirrors cli-utils' own "too many fields" guard. The
		// encoder never emits "_" in a name (it escapes only ":" → "__"), and
		// K8s names can't contain "_" anyway, so this id can't come from Flux;
		// accepting it would diverge from the producer. Here a Deployment named
		// "foo_bar" round-trips to a name with a stray "_" → invalid.
		{id: "ns_foo_bar_apps_Deployment", wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			got, err := ParseInventoryID(tc.id)
			if tc.wantError {
				if err == nil {
					t.Errorf("ParseInventoryID(%q) expected error, got nil", tc.id)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseInventoryID(%q) unexpected error: %v", tc.id, err)
			}
			if got != tc.want {
				t.Errorf("ParseInventoryID(%q) = %+v, want %+v", tc.id, got, tc.want)
			}
		})
	}
}

// Resolver.APIVersion: cache hit (from MCP discovery) wins; cache miss
// falls through to the legacy hardcoded table; final fallback is v1.
// Verifying all three legs of the lookup keeps the misroute that the bridge
// previously suffered (example.com/v1 vs /v1alpha1) from
// regressing silently.
func TestResolver_APIVersion(t *testing.T) {
	r := NewResolver([]VersionEntry{
		{Group: "example.com", Kind: "Schedule", Version: "v1alpha1"},
		{Group: "secrets.example.com", Kind: "VaultSecret", Version: "v1alpha1"},
	})
	tests := []struct {
		group string
		kind  string
		want  string
		why   string
	}{
		{"example.com", "Schedule", "example.com/v1alpha1", "cache hit"},
		{"secrets.example.com", "VaultSecret", "secrets.example.com/v1alpha1", "cache hit"},
		{"", "Pod", "v1", "core group → bare version"},
		{"apps", "Deployment", "apps/v1", "v1 default"},
		// Hardcoded fallback table — covered when discovery doesn't include them.
		{"helm.toolkit.fluxcd.io", "HelmRelease", "helm.toolkit.fluxcd.io/v2", "fallback table"},
		{"autoscaling", "HorizontalPodAutoscaler", "autoscaling/v2", "fallback table"},
		// Unknown group + unknown kind → v1 default.
		{"madeup.example.com", "Thing", "madeup.example.com/v1", "v1 default"},
	}
	for _, tc := range tests {
		t.Run(tc.group+"/"+tc.kind, func(t *testing.T) {
			if got := r.APIVersion(tc.group, tc.kind); got != tc.want {
				t.Errorf("APIVersion(%q, %q) = %q, want %q (%s)", tc.group, tc.kind, got, tc.want, tc.why)
			}
		})
	}
}

// Nil resolver must still work — the Checker is constructed without one in
// tests and during startup-discovery failure. It should behave exactly like
// the pre-resolver code: hardcoded table + v1 fallback.
func TestResolver_NilFallsBackToHardcodedTable(t *testing.T) {
	var r *Resolver
	if got := r.APIVersion("helm.toolkit.fluxcd.io", "HelmRelease"); got != "helm.toolkit.fluxcd.io/v2" {
		t.Errorf("nil resolver hardcoded fallback = %q, want helm.toolkit.fluxcd.io/v2", got)
	}
	if got := r.APIVersion("apps", "Deployment"); got != "apps/v1" {
		t.Errorf("nil resolver v1 default = %q, want apps/v1", got)
	}
}

// fakeFetcher mimics the MCP-backed ResourceFetcher. It returns YAML bytes
// keyed by (apiVersion, kind, name, namespace). Tests configure it inline.
type fakeFetcher struct {
	// resources is the response map. Key shape: "apiVersion|Kind|name|namespace".
	// Missing keys produce a "not found" error so the Checker treats the row
	// as NotFound (matching kstatus terminal-deleted state).
	resources map[string]string
	// err overrides everything when non-nil — used to test the Unknown path.
	err error
}

func (f *fakeFetcher) key(apiVersion, kind, name, namespace string) string {
	return apiVersion + "|" + kind + "|" + name + "|" + namespace
}

func (f *fakeFetcher) GetKubernetesResource(_ context.Context, apiVersion, kind, name, namespace string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	if body, ok := f.resources[f.key(apiVersion, kind, name, namespace)]; ok {
		return []byte(body), nil
	}
	return nil, fmt.Errorf("not found: %s/%s/%s in %s", apiVersion, kind, name, namespace)
}

func TestCheckAll_Empty(t *testing.T) {
	checker := NewChecker(&fakeFetcher{})
	results, err := checker.CheckAll(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestCheckAll_NotFound(t *testing.T) {
	checker := NewChecker(&fakeFetcher{})
	results, err := checker.CheckAll(context.Background(), []string{"default_ghost_apps_Deployment"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if r.Kind != "Deployment" || r.Name != "ghost" || r.Namespace != "default" {
		t.Errorf("unexpected ref fields: %+v", r)
	}
	if r.Status != "NotFound" {
		t.Errorf("status = %q, want %q", r.Status, "NotFound")
	}
}

func TestCheckAll_ExistingDeployment(t *testing.T) {
	deployYAML := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: default
  generation: 1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
status:
  observedGeneration: 1
  replicas: 1
  updatedReplicas: 1
  readyReplicas: 1
  availableReplicas: 1
  conditions:
    - type: Available
      status: "True"
      reason: MinimumReplicasAvailable
`
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|api|default": deployYAML,
	}}
	checker := NewChecker(fake)
	results, err := checker.CheckAll(context.Background(), []string{"default_api_apps_Deployment"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if r.Kind != "Deployment" || r.Name != "api" || r.Namespace != "default" {
		t.Errorf("unexpected ref fields: %+v", r)
	}
	if r.Status == "" || r.Status == "Unknown" {
		t.Errorf("status = %q, want a real kstatus value", r.Status)
	}
}

// TestCheckAll_HelmReleaseWithNoStatusYetIsNotCurrent is the regression test
// for the race this package's staleObservedGeneration guards against: a
// HelmRelease fetched moments after creation, before helm-controller's first
// status write, has metadata.generation set but NO status block at all.
// kstatus's own Compute() (confirmed directly against cli-utils v0.37.2) has
// no recognized condition or observedGeneration to act on in this shape, so
// it falls through to its generic "assume Current" default — exactly the
// false-green this guards against.
func TestCheckAll_HelmReleaseWithNoStatusYetIsNotCurrent(t *testing.T) {
	hrYAML := `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
  generation: 1
`
	fake := &fakeFetcher{resources: map[string]string{
		"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": hrYAML,
	}}
	checker := NewChecker(fake)
	results, err := checker.CheckAll(context.Background(), []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	r := results[0]
	if r.Status == "Current" {
		t.Errorf("a HelmRelease with no status at all must not read Current, got status=%q message=%q", r.Status, r.Message)
	}
	if r.Status != "InProgress" {
		t.Errorf("status = %q, want InProgress", r.Status)
	}
	if !strings.Contains(r.Message, "observedGeneration") {
		t.Errorf("message = %q, want it to explain the missing observedGeneration", r.Message)
	}
}

// TestCheckAll_HelmReleaseObservedGenerationBehindIsNotCurrent covers the
// present-but-stale shape: status.observedGeneration exists but hasn't
// caught up to metadata.generation. kstatus's own checkGeneration already
// demotes this to InProgress before staleObservedGeneration ever runs
// (confirmed against cli-utils v0.37.2) — this test pins that behavior so a
// future cli-utils upgrade that weakens it is caught here, not in
// production.
func TestCheckAll_HelmReleaseObservedGenerationBehindIsNotCurrent(t *testing.T) {
	hrYAML := `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
  generation: 2
status:
  observedGeneration: -1
  conditions:
    - type: Ready
      status: Unknown
      reason: Progressing
      message: "Running 'install' action with timeout of 10m0s"
`
	fake := &fakeFetcher{resources: map[string]string{
		"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": hrYAML,
	}}
	checker := NewChecker(fake)
	results, err := checker.CheckAll(context.Background(), []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if r := results[0]; r.Status != "InProgress" {
		t.Errorf("status = %q, want InProgress, message=%q", r.Status, r.Message)
	}
}

// TestCheckAll_ReconciledHelmReleaseStaysCurrent is the negative case: a
// HelmRelease whose observedGeneration has caught up to generation, with a
// True Ready condition, must still read Current — staleObservedGeneration
// must not demote a genuinely healthy, fully-reconciled release.
func TestCheckAll_ReconciledHelmReleaseStaysCurrent(t *testing.T) {
	hrYAML := `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
  generation: 4
status:
  observedGeneration: 4
  conditions:
    - type: Ready
      status: "True"
      reason: InstallSucceeded
      message: "Helm install succeeded"
`
	fake := &fakeFetcher{resources: map[string]string{
		"helm.toolkit.fluxcd.io/v2|HelmRelease|app|apps": hrYAML,
	}}
	checker := NewChecker(fake)
	results, err := checker.CheckAll(context.Background(), []string{"apps_app_helm.toolkit.fluxcd.io_HelmRelease"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if r := results[0]; r.Status != "Current" {
		t.Errorf("a reconciled HelmRelease must read Current, got status=%q message=%q", r.Status, r.Message)
	}
}

// TestCheckAll_ConfigMapWithoutGenerationStaysCurrent pins the exclusion
// this guard relies on: kinds with no metadata.generation at all (confirmed
// live: ConfigMap, Secret, Namespace, ServiceAccount, Service, HPA, PVC)
// must never be touched by staleObservedGeneration, since they have no
// generation field for it to reason about in the first place.
func TestCheckAll_ConfigMapWithoutGenerationStaysCurrent(t *testing.T) {
	cmYAML := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: apps
data:
  key: value
`
	fake := &fakeFetcher{resources: map[string]string{
		"v1|ConfigMap|app-config|apps": cmYAML,
	}}
	checker := NewChecker(fake)
	results, err := checker.CheckAll(context.Background(), []string{"apps_app-config__ConfigMap"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if r := results[0]; r.Status != "Current" {
		t.Errorf("a ConfigMap without generation tracking must read Current, got status=%q message=%q", r.Status, r.Message)
	}
}

// TestCheckAll_NonHelmReleaseCRDWithoutTopLevelObservedGenerationStaysCurrent
// is the regression test for the review finding that staleObservedGeneration
// must be scoped to HelmRelease specifically: an arbitrary CRD CAN have
// API-server-managed metadata.generation while its controller tracks
// generation only per-condition (the metav1.Condition.ObservedGeneration
// convention — e.g. cert-manager's Certificate) and never sets a top-level
// status.observedGeneration at all. Applying the "absent == stale" rule to
// such a resource would misread a genuinely healthy, Ready=True CRD as
// permanently stuck, holding the readiness poll open until timeout.
func TestCheckAll_NonHelmReleaseCRDWithoutTopLevelObservedGenerationStaysCurrent(t *testing.T) {
	certYAML := `
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: app-tls
  namespace: apps
  generation: 3
status:
  conditions:
    - type: Ready
      status: "True"
      reason: Ready
      message: Certificate is up to date and has not expired
      observedGeneration: 3
`
	fake := &fakeFetcher{resources: map[string]string{
		"cert-manager.io/v1|Certificate|app-tls|apps": certYAML,
	}}
	checker := NewChecker(fake)
	results, err := checker.CheckAll(context.Background(), []string{"apps_app-tls_cert-manager.io_Certificate"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if r := results[0]; r.Status != "Current" {
		t.Errorf("a Ready, non-HelmRelease CRD without a top-level status.observedGeneration must stay Current, got status=%q message=%q", r.Status, r.Message)
	}
}

// kstatus reads Ready=True as Current without looking at the condition's own
// observedGeneration. Right after a spec change, a CRD that tracks generation
// per condition still carries the previous generation's Ready=True until its
// controller re-evaluates it: that must not read Current. A condition that
// doesn't set the field (or sets the unset value 0) still defers to kstatus.
func TestCheckAll_ReadyConditionFromOlderGeneration(t *testing.T) {
	for _, tc := range []struct {
		name       string
		observed   string
		wantStatus string
	}{
		{name: "older generation", observed: "\n      observedGeneration: 3", wantStatus: "InProgress"},
		{name: "current generation", observed: "\n      observedGeneration: 4", wantStatus: "Current"},
		{name: "unset", observed: "", wantStatus: "Current"},
		{name: "zero", observed: "\n      observedGeneration: 0", wantStatus: "Current"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			certYAML := `
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: app-tls
  namespace: apps
  generation: 4
status:
  conditions:
    - type: Ready
      status: "True"
      reason: Ready` + tc.observed + "\n"
			fake := &fakeFetcher{resources: map[string]string{
				"cert-manager.io/v1|Certificate|app-tls|apps": certYAML,
			}}
			results, err := NewChecker(fake).CheckAll(context.Background(), []string{"apps_app-tls_cert-manager.io_Certificate"})
			if err != nil {
				t.Fatalf("CheckAll: %v", err)
			}
			if r := results[0]; r.Status != tc.wantStatus {
				t.Errorf("status = %q (message %q), want %q", r.Status, r.Message, tc.wantStatus)
			}
		})
	}
}

// Covers a single-replica Deployment stuck rolling out a crashing
// pod. Available stays True (maxUnavailable=1 → minAvailable=0) while zero pods
// are ready. The bridge must surface the hard counts so the LLM reports "0/1
// ready" instead of guessing that "older replicas remain ready".
func TestCheckAll_StuckDeployment_ExtractsReplicaCounts(t *testing.T) {
	deployYAML := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: podinfo
  namespace: test
  generation: 8
spec:
  replicas: 1
  strategy:
    rollingUpdate:
      maxUnavailable: 1
  selector:
    matchLabels:
      app: podinfo
status:
  observedGeneration: 8
  replicas: 1
  updatedReplicas: 1
  unavailableReplicas: 1
  conditions:
    - type: Available
      status: "True"
      reason: MinimumReplicasAvailable
      message: Deployment has minimum availability.
    - type: Progressing
      status: "False"
      reason: ProgressDeadlineExceeded
      message: ReplicaSet "podinfo-67bdddfc9b" has timed out progressing.
`
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|podinfo|test": deployYAML,
	}}
	results, err := NewChecker(fake).CheckAll(context.Background(), []string{"test_podinfo_apps_Deployment"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r := results[0]
	if r.Status == "Current" {
		t.Fatalf("expected a non-Current status for a stuck rollout, got Current")
	}
	if r.Replicas == nil {
		t.Fatalf("expected replica counts to be populated for a non-Current Deployment")
	}
	if r.Replicas.Desired != 1 || r.Replicas.Ready != 0 || r.Replicas.Unavailable != 1 || r.Replicas.Updated != 1 {
		t.Errorf("replica counts wrong: %+v", r.Replicas)
	}
	if got, want := r.Replicas.String(), "replicas: 0/1 ready, 0 available, 1 updated, 1 unavailable"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// Non-workload kinds (and Current workloads) carry no replica counts — the
// field stays nil so the prompt line is unchanged for them.
func TestCheckAll_NonWorkload_NoReplicaCounts(t *testing.T) {
	cmYAML := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
  namespace: default
data:
  k: v
`
	fake := &fakeFetcher{resources: map[string]string{"v1|ConfigMap|cfg|default": cmYAML}}
	results, err := NewChecker(fake).CheckAll(context.Background(), []string{"default_cfg__ConfigMap"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Replicas != nil {
		t.Errorf("ConfigMap must not carry replica counts: %+v", results[0].Replicas)
	}
}

func TestCheckAll_MultipleRefs(t *testing.T) {
	svcYAML := `
apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: default
spec:
  clusterIP: 10.0.0.1
  ports:
    - port: 80
`
	fake := &fakeFetcher{resources: map[string]string{
		"v1|Service|web|default": svcYAML,
	}}
	checker := NewChecker(fake)
	refs := []string{
		"default_api_apps_Deployment", // missing
		"default_web__Service",        // present
	}
	results, err := checker.CheckAll(context.Background(), refs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Status != "NotFound" {
		t.Errorf("first result (missing Deployment) status = %q, want NotFound", results[0].Status)
	}
	if results[1].Status == "" {
		t.Error("second result (existing Service) status should not be empty")
	}
}

func TestCheckAll_InvalidRef(t *testing.T) {
	checker := NewChecker(&fakeFetcher{})
	results, err := checker.CheckAll(context.Background(), []string{"bad-ref"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "Unknown" {
		t.Errorf("status = %q, want Unknown", results[0].Status)
	}
}

func TestCheckAll_FetcherErrorIsUnknown(t *testing.T) {
	// A non-"not found" error means MCP itself blew up — we surface it as
	// Unknown so the LLM sees the row but doesn't conclude the resource is
	// deleted. AND it should log WARN with the exact upstream error so
	// operators can correlate the missing-data symptom with the root cause.
	fake := &fakeFetcher{err: fmt.Errorf("mcp session lost")}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	checker := NewChecker(fake).WithLogger(logger)
	results, err := checker.CheckAll(context.Background(), []string{"default_api_apps_Deployment"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Status != "Unknown" {
		t.Errorf("status = %q, want Unknown", results[0].Status)
	}
	if !strings.Contains(results[0].Message, "mcp session lost") {
		t.Errorf("message should preserve the upstream error, got %q", results[0].Message)
	}
	logs := buf.String()
	if !strings.Contains(logs, "readiness: resource fetch failed via MCP") {
		t.Errorf("expected WARN log with MCP failure marker, got: %s", logs)
	}
	if !strings.Contains(logs, "mcp session lost") {
		t.Errorf("expected exact upstream error string in log, got: %s", logs)
	}
}

// "not found" is a meaningful state, not an operational failure. We do NOT
// want to spam WARN logs every time the inventory contains a row whose
// resource was deleted in-flight — kstatus already classifies it as
// NotFound and the LLM gets the picture from the row alone.
func TestCheckAll_NotFoundDoesNotLog(t *testing.T) {
	fake := &fakeFetcher{err: fmt.Errorf("Resource not found in namespace default")}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	checker := NewChecker(fake).WithLogger(logger)
	results, _ := checker.CheckAll(context.Background(), []string{"default_api_apps_Deployment"})
	if results[0].Status != "NotFound" {
		t.Errorf("status = %q, want NotFound", results[0].Status)
	}
	if buf.Len() != 0 {
		t.Errorf("not-found is expected; no log should fire, got: %s", buf.String())
	}
}

// flux-operator-mcp returns "No resources found" (note: not "not found")
// when its underlying list returns empty — typically because the GVR isn't
// in the discovery cache (uninstalled CRD) or the named instance simply
// doesn't exist. Substring "not found" is absent, but the semantics match
// NotFound and we don't want WARN-log spam for it.
func TestCheckAll_NoResourcesFoundTreatedAsNotFound(t *testing.T) {
	fake := &fakeFetcher{err: fmt.Errorf("mcp tool get_kubernetes_resources returned error: No resources found")}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	checker := NewChecker(fake).WithLogger(logger)
	results, _ := checker.CheckAll(context.Background(), []string{"app_app_example.com_Schedule"})
	if results[0].Status != "NotFound" {
		t.Errorf("status = %q, want NotFound", results[0].Status)
	}
	if buf.Len() != 0 {
		t.Errorf("no WARN should fire for 'No resources found', got: %s", buf.String())
	}
}

// readyDeploymentYAML and inProgressDeploymentYAML are the two states a
// Deployment passes through during a rolling update. Used by the
// WaitUntilSettled tests below — first poll returns the in-progress YAML,
// later polls return the ready YAML so the loop exercises both branches.
const readyDeploymentYAML = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: default
  generation: 1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
status:
  observedGeneration: 1
  replicas: 1
  updatedReplicas: 1
  readyReplicas: 1
  availableReplicas: 1
  conditions:
    - type: Available
      status: "True"
      reason: MinimumReplicasAvailable
`

const inProgressDeploymentYAML = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: default
  generation: 2
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
status:
  observedGeneration: 1
  replicas: 1
  updatedReplicas: 0
  readyReplicas: 0
  availableReplicas: 0
  conditions:
    - type: Progressing
      status: "True"
      reason: ReplicaSetUpdated
`

// switchingFetcher returns inProgressDeploymentYAML for the first `n` calls
// and readyDeploymentYAML afterwards. Models a real rollout where the
// Deployment gradually settles between polls.
type switchingFetcher struct {
	inProgressCalls int
	calls           atomic.Int32
}

func (s *switchingFetcher) GetKubernetesResource(_ context.Context, _, _, _, _ string) ([]byte, error) {
	n := s.calls.Add(1)
	if int(n) <= s.inProgressCalls {
		return []byte(inProgressDeploymentYAML), nil
	}
	return []byte(readyDeploymentYAML), nil
}

func TestWaitUntilSettled_ImmediatelyCurrent(t *testing.T) {
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|api|default": readyDeploymentYAML,
	}}
	checker := NewChecker(fake)
	results, settled, err := checker.WaitUntilSettled(
		context.Background(),
		[]string{"default_api_apps_Deployment"},
		10*time.Millisecond, 1*time.Second,
	)
	if err != nil {
		t.Fatalf("WaitUntilSettled: %v", err)
	}
	if !settled {
		t.Fatalf("expected settled=true on already-Current row")
	}
	if results[0].Status != "Current" {
		t.Errorf("status = %q, want Current", results[0].Status)
	}
}

func TestWaitUntilSettled_PollsThenSettles(t *testing.T) {
	s := &switchingFetcher{inProgressCalls: 2}
	checker := NewChecker(s)
	start := time.Now()
	results, settled, err := checker.WaitUntilSettled(
		context.Background(),
		[]string{"default_api_apps_Deployment"},
		20*time.Millisecond, 2*time.Second,
	)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("WaitUntilSettled: %v", err)
	}
	if !settled {
		t.Fatalf("expected settled=true after rollout completes")
	}
	if results[0].Status != "Current" {
		t.Errorf("status = %q, want Current after settle", results[0].Status)
	}
	// At least 2 sleeps of 20ms — be lax to avoid flakes on slow CI.
	if elapsed < 30*time.Millisecond {
		t.Errorf("elapsed = %s, expected >= 30ms (must have slept between polls)", elapsed)
	}
	if got := s.calls.Load(); got < 3 {
		t.Errorf("fetcher called %d times, want >=3 (2 InProgress + 1 Current)", got)
	}
}

// notFoundThenReadyFetcher returns flux-operator-mcp's ambiguous
// "No resources found" error for the first `notFoundCalls` calls, then the
// real (Current) resource. Models a degraded MCP server (cold discovery cache
// after an OOM restart) that recovers mid-poll.
type notFoundThenReadyFetcher struct {
	notFoundCalls int
	calls         atomic.Int32
}

func (f *notFoundThenReadyFetcher) GetKubernetesResource(_ context.Context, _, _, _, _ string) ([]byte, error) {
	if int(f.calls.Add(1)) <= f.notFoundCalls {
		return nil, errors.New("No resources found")
	}
	return []byte(readyDeploymentYAML), nil
}

// A transient NotFound (degraded MCP) must NOT settle the poll on a false
// "deleted" verdict — the row must re-resolve to its real status on a later
// interval once MCP recovers.
func TestWaitUntilSettled_TransientNotFoundRecovers(t *testing.T) {
	f := &notFoundThenReadyFetcher{notFoundCalls: 2}
	checker := NewChecker(f)
	results, settled, err := checker.WaitUntilSettled(
		context.Background(),
		[]string{"default_api_apps_Deployment"},
		20*time.Millisecond, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("WaitUntilSettled: %v", err)
	}
	if !settled {
		t.Fatalf("expected settled=true once the transient NotFound cleared")
	}
	if results[0].Status != "Current" {
		t.Errorf("status = %q, want Current (must not settle on the transient NotFound)", results[0].Status)
	}
	if got := f.calls.Load(); got < 3 {
		t.Errorf("fetcher called %d times, want >=3 (2 NotFound + 1 Current) — poll must have re-checked", got)
	}
}

// A genuinely-missing resource stays NotFound and the poll runs to the
// timeout (settled=false), still reporting NotFound so the caller analyses it.
func TestWaitUntilSettled_PersistentNotFoundDoesNotSettle(t *testing.T) {
	fake := &fakeFetcher{} // empty → every get is "not found"
	checker := NewChecker(fake)
	results, settled, err := checker.WaitUntilSettled(
		context.Background(),
		[]string{"default_api_apps_Deployment"},
		20*time.Millisecond, 80*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("WaitUntilSettled: %v", err)
	}
	if settled {
		t.Fatalf("expected settled=false: a persistent NotFound is non-terminal now")
	}
	if len(results) != 1 || results[0].Status != "NotFound" {
		t.Fatalf("expected the row reported as NotFound at timeout, got %+v", results)
	}
}

func TestWaitUntilSettled_TimesOut(t *testing.T) {
	// Never-settles fetcher — always InProgress.
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|api|default": inProgressDeploymentYAML,
	}}
	checker := NewChecker(fake)
	results, settled, err := checker.WaitUntilSettled(
		context.Background(),
		[]string{"default_api_apps_Deployment"},
		20*time.Millisecond, 80*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("WaitUntilSettled: %v", err)
	}
	if settled {
		t.Fatalf("expected settled=false on timeout, got true")
	}
	// Caller still gets a result row so it can analyse the partial state.
	if len(results) != 1 {
		t.Fatalf("expected 1 result row even on timeout, got %d", len(results))
	}
}

// TestWaitUntilSettledFunc_RefsGrowMidPoll mirrors the production fix for
// the HelmRelease-children case: when refs APPEAR mid-poll (because the
// caller's expansion picks up a HelmRelease's children only AFTER
// helm-controller has populated status.history), the new rows must show
// up in the final result set, and settled must wait for them too.
//
// Setup:
//   - switchingFetcher returns InProgress for the first 2 fetches across
//     all resources combined, then Current.
//   - refsFunc returns [A] for the first 2 iterations, [A, B] thereafter,
//     simulating an HR's inventory populating after a couple of polls.
//
// Iteration math:
//
//	Iter 1: refs=[A]. fetch #1 → InProgress. Not settled. Sleep.
//	Iter 2: refs=[A]. fetch #2 → InProgress. Not settled. Sleep.
//	Iter 3: refs=[A,B]. fetch #3 + #4 → both Current. Settled.
//
// Asserts: settled=true, 2 rows in the final result, refsFunc called >=3.
func TestWaitUntilSettledFunc_RefsGrowMidPoll(t *testing.T) {
	s := &switchingFetcher{inProgressCalls: 2}
	checker := NewChecker(s)

	var refsFuncCalls atomic.Int32
	refsFunc := func(context.Context) []string {
		n := refsFuncCalls.Add(1)
		if n <= 2 {
			return []string{"default_a_apps_Deployment"}
		}
		return []string{"default_a_apps_Deployment", "default_b_apps_Deployment"}
	}

	results, settled, err := checker.WaitUntilSettledFunc(
		context.Background(),
		refsFunc,
		20*time.Millisecond, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("WaitUntilSettledFunc: %v", err)
	}
	if !settled {
		t.Fatalf("expected settled=true once the grown ref set is all Current")
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows in the settled result (refs grew from 1 to 2 mid-poll), got %d: %v", len(results), results)
	}
	for i, r := range results {
		if r.Status != "Current" {
			t.Errorf("results[%d].Status = %q, want Current", i, r.Status)
		}
	}
	if got := refsFuncCalls.Load(); got < 3 {
		t.Errorf("refsFunc called %d times, want >= 3 (2 InProgress + 1 Current)", got)
	}
}

// A cancelled caller never gets settled=true, even when the snapshot (read
// by a fetcher that ignores ctx) is all Current, or when there are no refs.
func TestWaitUntilSettled_ContextCancelled(t *testing.T) {
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|api|default": readyDeploymentYAML,
	}}
	checker := NewChecker(fake)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, refs := range [][]string{{"default_api_apps_Deployment"}, nil} {
		_, settled, err := checker.WaitUntilSettled(ctx, refs, 50*time.Millisecond, 5*time.Second)
		if settled || !errors.Is(err, context.Canceled) {
			t.Errorf("refs %v: settled=%v err=%v, want settled=false and context.Canceled", refs, settled, err)
		}
	}
}

// The last sleep stops at the deadline: an interval longer than the timeout
// must not hold the poll a whole interval past it.
func TestWaitUntilSettled_SleepStopsAtDeadline(t *testing.T) {
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|api|default": inProgressDeploymentYAML,
	}}
	start := time.Now()
	_, settled, err := NewChecker(fake).WaitUntilSettled(context.Background(),
		[]string{"default_api_apps_Deployment"}, 10*time.Second, 50*time.Millisecond)
	if err != nil || settled {
		t.Fatalf("settled=%v err=%v, want an unsettled timeout", settled, err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("poll returned after %s, want shortly after the 50ms timeout", elapsed)
	}
}

// slowFetcher returns readyDeploymentYAML after delay, or the ctx error if
// ctx ends first, like an MCP call bound to its context.
type slowFetcher struct{ delay time.Duration }

func (s slowFetcher) GetKubernetesResource(ctx context.Context, _, _, _, _ string) ([]byte, error) {
	select {
	case <-time.After(s.delay):
		return []byte(readyDeploymentYAML), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// The poll's reads are bounded by its timeout (with a short floor), not just
// its sleeps: a read slower than the timeout is cut short, reads Unknown, and
// the poll returns unsettled without an error instead of running past the
// timeout and reporting what the slow read eventually returned.
func TestWaitUntilSettled_ReadsBoundedByTimeout(t *testing.T) {
	start := time.Now()
	results, settled, err := NewChecker(slowFetcher{delay: 5 * time.Second}).WaitUntilSettled(
		context.Background(), []string{"default_api_apps_Deployment"}, 10*time.Millisecond, 50*time.Millisecond)
	elapsed := time.Since(start)
	if err != nil || settled {
		t.Fatalf("settled=%v err=%v, want settled=false and no error", settled, err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("poll returned after %s, want the read cut at the 1s floor", elapsed)
	}
	if len(results) != 1 || results[0].Status != "Unknown" {
		t.Errorf("results = %+v, want one Unknown row for the cut read", results)
	}
}

// The refs read is bounded like the row reads: a refsFunc that reads slowly
// (a HelmRelease expansion over MCP) gets a ctx that ends at the poll
// deadline (at least the 1s floor), so it can't run the poll past the timeout.
func TestWaitUntilSettledFunc_RefsReadBoundedByTimeout(t *testing.T) {
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|api|default": inProgressDeploymentYAML,
	}}
	refsFunc := func(ctx context.Context) []string {
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
		}
		return []string{"default_api_apps_Deployment"}
	}
	start := time.Now()
	_, settled, err := NewChecker(fake).WaitUntilSettledFunc(context.Background(), refsFunc, 10*time.Millisecond, 50*time.Millisecond)
	if err != nil || settled {
		t.Fatalf("settled=%v err=%v, want settled=false and no error", settled, err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("poll returned after %s, want the refs read cut at the 1s floor", elapsed)
	}
}

// The refs and the rows are separate reads. When the refs change between
// them (a HelmRelease finished its install and now lists a child the refs
// read missed), an all-Current snapshot of the old refs is not settled.
func TestWaitUntilSettledFunc_RefsChangedAfterCheck(t *testing.T) {
	fake := &fakeFetcher{resources: map[string]string{
		"apps/v1|Deployment|a|default": readyDeploymentYAML,
		"apps/v1|Deployment|b|default": readyDeploymentYAML,
	}}
	var calls atomic.Int32
	refsFunc := func(context.Context) []string {
		if calls.Add(1) == 1 {
			return []string{"default_a_apps_Deployment"}
		}
		return []string{"default_a_apps_Deployment", "default_b_apps_Deployment"}
	}
	results, settled, err := NewChecker(fake).WaitUntilSettledFunc(context.Background(), refsFunc, 10*time.Millisecond, 2*time.Second)
	if err != nil || !settled {
		t.Fatalf("settled=%v err=%v, want settled", settled, err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d rows, want 2: the settled snapshot must cover the refs read after it: %v", len(results), results)
	}
}

// TestCheckOne_RefreshesDiscoveryOnUnknownVersionNotFound covers a CRD that was
// registered AFTER the bridge built its discovery cache: the resolver doesn't
// know the group, polls at the v1 fallback, reads NotFound — then refreshes
// discovery and re-polls at the real served version, resolving the resource.
func TestCheckOne_RefreshesDiscoveryOnUnknownVersionNotFound(t *testing.T) {
	widget := "apiVersion: example.com/v1beta1\nkind: Widget\nmetadata:\n  name: w1\n  namespace: ns1\n"
	fetcher := &fakeFetcher{resources: map[string]string{
		// Only served at the REAL version; the v1 guess misses.
		"example.com/v1beta1|Widget|w1|ns1": widget,
	}}
	refreshed := 0
	resolver := NewResolver(nil).WithRefresher(func(_ context.Context) ([]VersionEntry, error) {
		refreshed++
		return []VersionEntry{{Group: "example.com", Kind: "Widget", Version: "v1beta1"}}, nil
	})
	checker := NewChecker(fetcher).WithResolver(resolver)

	results, err := checker.CheckAll(context.Background(), []string{"ns1_w1_example.com_Widget"})
	if err != nil {
		t.Fatalf("CheckAll: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("want 1 result, got %d", len(results))
	}
	if results[0].Status == "NotFound" {
		t.Errorf("resource still NotFound after discovery refresh; want it resolved at v1beta1 (status=%q)", results[0].Status)
	}
	if refreshed == 0 {
		t.Error("expected a discovery refresh to be triggered by the unknown-version NotFound")
	}
}

// TestCheckOne_GenuinelyMissingStaysNotFound proves the refresh doesn't fabricate
// a verdict: when the resource is absent at its real version too, the retry still
// reads NotFound.
func TestCheckOne_GenuinelyMissingStaysNotFound(t *testing.T) {
	fetcher := &fakeFetcher{resources: map[string]string{}} // absent at every version
	resolver := NewResolver(nil).WithRefresher(func(_ context.Context) ([]VersionEntry, error) {
		return []VersionEntry{{Group: "example.com", Kind: "Widget", Version: "v1beta1"}}, nil
	})
	checker := NewChecker(fetcher).WithResolver(resolver)

	results, err := checker.CheckAll(context.Background(), []string{"ns1_w1_example.com_Widget"})
	if err != nil {
		t.Fatalf("CheckAll: %v", err)
	}
	if results[0].Status != "NotFound" {
		t.Errorf("genuinely-missing resource should stay NotFound, got %q", results[0].Status)
	}
}

// A kind discovery doesn't know is read at a guessed apiVersion. Its NotFound
// may only mean the cluster doesn't serve that version, so the row says the
// version was guessed; a kind discovery knows, or one it learns on the
// NotFound refresh, and the core group (v1 only) are not guesses.
func TestCheckOne_ReportsGuessedVersion(t *testing.T) {
	cases := []struct {
		name     string
		resolver *Resolver
		ref      string
		want     bool
	}{
		{"unknown kind, no refresher", NewResolver(nil), "ns1_w1_example.com_Widget", true},
		{"no resolver at all", nil, "ns1_w1_example.com_Widget", true},
		{"fallback-table group", NewResolver(nil), "ns1_hr_helm.toolkit.fluxcd.io_HelmRelease", true},
		{"refresh still doesn't know the kind", NewResolver(nil).WithRefresher(func(context.Context) ([]VersionEntry, error) {
			return []VersionEntry{{Group: "other.io", Kind: "Gadget", Version: "v1"}}, nil
		}), "ns1_w1_example.com_Widget", true},
		{"known from discovery", NewResolver([]VersionEntry{{Group: "example.com", Kind: "Widget", Version: "v1beta1"}}), "ns1_w1_example.com_Widget", false},
		{"learned on the NotFound refresh", NewResolver(nil).WithRefresher(func(context.Context) ([]VersionEntry, error) {
			return []VersionEntry{{Group: "example.com", Kind: "Widget", Version: "v1"}}, nil
		}), "ns1_w1_example.com_Widget", false},
		{"core group", nil, "ns1_cm__ConfigMap", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			checker := NewChecker(&fakeFetcher{resources: map[string]string{}}).WithResolver(c.resolver)
			results, err := checker.CheckAll(context.Background(), []string{c.ref})
			if err != nil {
				t.Fatalf("CheckAll: %v", err)
			}
			if results[0].Status != "NotFound" {
				t.Fatalf("status = %q, want NotFound", results[0].Status)
			}
			if results[0].VersionGuessed != c.want {
				t.Errorf("VersionGuessed = %v, want %v", results[0].VersionGuessed, c.want)
			}
		})
	}
}

// A CRD upgraded after startup (v1beta1 → v1) is cached at a version the
// cluster no longer serves. Its NotFound triggers a discovery refresh like a
// guessed version's does, and the re-poll at v1 resolves it. When the first
// refresh is partial and leaves the kind out, the next one (after the
// cooldown) still corrects it.
func TestCheckOne_RefreshesDiscoveryOnStaleKnownVersion(t *testing.T) {
	widget := "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w1\n  namespace: ns1\n"
	fetcher := &fakeFetcher{resources: map[string]string{"example.com/v1|Widget|w1|ns1": widget}}
	answers := [][]VersionEntry{
		{{Group: "other.io", Kind: "Gadget", Version: "v1"}}, // partial: Widget missing
		{{Group: "example.com", Kind: "Widget", Version: "v1"}},
	}
	refreshed := 0
	resolver := NewResolver([]VersionEntry{{Group: "example.com", Kind: "Widget", Version: "v1beta1"}}).
		WithRefresher(func(_ context.Context) ([]VersionEntry, error) {
			a := answers[min(refreshed, len(answers)-1)]
			refreshed++
			return a, nil
		})
	resolver.cooldown = 0
	checker := NewChecker(fetcher).WithResolver(resolver)
	refs := []string{"ns1_w1_example.com_Widget"}

	results, err := checker.CheckAll(context.Background(), refs)
	if err != nil {
		t.Fatalf("CheckAll: %v", err)
	}
	if results[0].Status != "NotFound" || refreshed != 1 {
		t.Fatalf("first check: status=%q refreshes=%d, want NotFound after one (partial) refresh", results[0].Status, refreshed)
	}
	results, err = checker.CheckAll(context.Background(), refs)
	if err != nil {
		t.Fatalf("CheckAll: %v", err)
	}
	if results[0].Status == "NotFound" {
		t.Fatalf("still NotFound after a complete discovery; the stale v1beta1 entry was kept")
	}
}

// A degraded MCP answering discovery with nothing (or only part of the API
// surface) must not wipe versions the resolver already knows.
func TestResolverRefresh_KeepsKnownVersionsOnEmptyOrPartialDiscovery(t *testing.T) {
	answers := [][]VersionEntry{
		nil,
		{{Group: "example.com", Kind: "Widget", Version: "v2"}},
	}
	calls := 0
	resolver := NewResolver([]VersionEntry{{Group: "helm.toolkit.fluxcd.io", Kind: "HelmRelease", Version: "v2"}}).
		WithRefresher(func(_ context.Context) ([]VersionEntry, error) {
			a := answers[calls]
			calls++
			return a, nil
		})
	resolver.cooldown = 0

	if changed, err := resolver.Refresh(context.Background()); err != nil || changed {
		t.Fatalf("empty discovery: changed=%v err=%v, want a no-op", changed, err)
	}
	if v, known := resolver.apiVersionKnown("helm.toolkit.fluxcd.io", "HelmRelease"); !known || v != "helm.toolkit.fluxcd.io/v2" {
		t.Fatalf("after empty discovery: %q known=%v, want the cached v2", v, known)
	}
	if changed, err := resolver.Refresh(context.Background()); err != nil || !changed {
		t.Fatalf("partial discovery: changed=%v err=%v, want an update", changed, err)
	}
	for _, gk := range [][2]string{{"helm.toolkit.fluxcd.io", "HelmRelease"}, {"example.com", "Widget"}} {
		if _, known := resolver.apiVersionKnown(gk[0], gk[1]); !known {
			t.Errorf("%s/%s lost after a partial discovery", gk[0], gk[1])
		}
	}
}

// A kind discovery once knew at a version the cluster may no longer serve
// (a CRD upgraded v1beta1 → v1) reads NotFound there. When the refresh that
// NotFound triggers fails, or answers without the kind, nothing confirms the
// cached version is still served, so the NotFound doesn't prove the resource
// is gone: the row is marked VersionGuessed, like a read at a guessed version.
// A refresh that lists the kind at the version read confirms it.
func TestCheckOne_UnconfirmedCachedVersionIsGuessed(t *testing.T) {
	cached := []VersionEntry{{Group: "example.com", Kind: "Widget", Version: "v1beta1"}}
	cases := []struct {
		name    string
		refresh func(context.Context) ([]VersionEntry, error)
		want    bool
	}{
		{"refresh fails", func(context.Context) ([]VersionEntry, error) {
			return nil, errors.New("discovery unavailable")
		}, true},
		{"refresh answers nothing", func(context.Context) ([]VersionEntry, error) {
			return nil, nil
		}, true},
		{"refresh leaves the kind out", func(context.Context) ([]VersionEntry, error) {
			return []VersionEntry{{Group: "other.io", Kind: "Gadget", Version: "v1"}}, nil
		}, true},
		{"refresh confirms the version", func(context.Context) ([]VersionEntry, error) {
			return cached, nil
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resolver := NewResolver(cached).WithRefresher(c.refresh)
			checker := NewChecker(&fakeFetcher{resources: map[string]string{}}).WithResolver(resolver)
			results, err := checker.CheckAll(context.Background(), []string{"ns1_w1_example.com_Widget"})
			if err != nil {
				t.Fatalf("CheckAll: %v", err)
			}
			if results[0].Status != "NotFound" {
				t.Fatalf("status = %q, want NotFound", results[0].Status)
			}
			if results[0].VersionGuessed != c.want {
				t.Errorf("VersionGuessed = %v, want %v", results[0].VersionGuessed, c.want)
			}
		})
	}
}

// Within the refresh cooldown no new discovery runs, so the latest attempt
// decides: after a failed refresh, a NotFound at a cached version stays
// unconfirmed until a later refresh lists it.
func TestCheckOne_FailedRefreshLeavesCachedVersionUnconfirmedDuringCooldown(t *testing.T) {
	cached := []VersionEntry{{Group: "example.com", Kind: "Widget", Version: "v1beta1"}}
	resolver := NewResolver(cached).WithRefresher(func(context.Context) ([]VersionEntry, error) {
		return nil, errors.New("discovery unavailable")
	})
	checker := NewChecker(&fakeFetcher{resources: map[string]string{}}).WithResolver(resolver)
	for i := range 2 { // the second check falls inside the cooldown
		results, err := checker.CheckAll(context.Background(), []string{"ns1_w1_example.com_Widget"})
		if err != nil {
			t.Fatalf("CheckAll #%d: %v", i, err)
		}
		if !results[0].VersionGuessed {
			t.Errorf("check #%d: VersionGuessed = false, want true (no discovery confirmed the cached version)", i)
		}
	}
}

// scriptedFetcher answers the nth read (counting from 1) with an InProgress
// Deployment, except the reads listed in block: those wait for ctx to end and
// fail with its error, like an MCP call cut short by the poll deadline. Each
// read takes delay first. The poll reads one row at a time, so a plain
// counter is enough.
type scriptedFetcher struct {
	block map[int]bool
	delay time.Duration
	calls int
}

func (s *scriptedFetcher) GetKubernetesResource(ctx context.Context, _, _, _, _ string) ([]byte, error) {
	s.calls++
	if s.block[s.calls] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-time.After(s.delay):
		return []byte(inProgressDeploymentYAML), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A row the deadline cuts short keeps what the previous poll read for it, so
// the result shows the last state seen instead of a timeout error. A row cut
// short with no previous read stays Unknown.
func TestWaitUntilSettled_CutRowKeepsPreviousPoll(t *testing.T) {
	refs := []string{"default_api_apps_Deployment", "default_worker_apps_Deployment"}
	// Poll 1 reads both rows (#1, #2); poll 2 reads api (#3) and is cut on
	// worker (#4).
	fetcher := &scriptedFetcher{block: map[int]bool{4: true}}
	results, settled, err := NewChecker(fetcher).WaitUntilSettled(context.Background(), refs,
		10*time.Millisecond, 200*time.Millisecond)
	if err != nil || settled {
		t.Fatalf("settled=%v err=%v, want settled=false and no error", settled, err)
	}
	if fetcher.calls != 4 {
		t.Fatalf("reads = %d, want 4 (two polls, the second cut short)", fetcher.calls)
	}
	for i, r := range results {
		if r.Status != "InProgress" {
			t.Errorf("row %d (%s) = %s %q, want InProgress kept from the previous poll", i, r.Name, r.Status, r.Message)
		}
	}

	// Cut short in the first poll: nothing to keep.
	fetcher = &scriptedFetcher{block: map[int]bool{2: true}}
	results, _, _ = NewChecker(fetcher).WaitUntilSettled(context.Background(), refs,
		10*time.Millisecond, 50*time.Millisecond)
	if results[1].Status != "Unknown" {
		t.Errorf("row cut in the first poll = %s, want Unknown", results[1].Status)
	}
}

// The poll doesn't start a poll that can't finish by the deadline, judged by
// how long the previous one took: it reports the previous poll instead of
// one the deadline would cut short.
func TestWaitUntilSettled_NoPollThatCannotFinish(t *testing.T) {
	fetcher := &scriptedFetcher{delay: 60 * time.Millisecond}
	start := time.Now()
	results, settled, err := NewChecker(fetcher).WaitUntilSettled(context.Background(),
		[]string{"default_api_apps_Deployment"}, 10*time.Millisecond, 100*time.Millisecond)
	if err != nil || settled {
		t.Fatalf("settled=%v err=%v, want settled=false and no error", settled, err)
	}
	if fetcher.calls != 1 {
		t.Errorf("reads = %d, want 1: a second 60ms poll can't finish in the 40ms left", fetcher.calls)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("poll returned after %s, want before the 100ms deadline", elapsed)
	}
	if len(results) != 1 || results[0].Status != "InProgress" {
		t.Errorf("results = %+v, want the first poll's InProgress row", results)
	}
}
