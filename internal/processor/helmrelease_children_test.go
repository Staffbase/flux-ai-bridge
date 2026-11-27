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
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"github.com/staffbase/flux-ai-bridge/internal/config"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
	corev1 "k8s.io/api/core/v1"
)

// helmReleaseDBNewInventory is HelmRelease apps/db after an upgrade that
// renamed the chart's ConfigMap db-init-old to db-init.
const helmReleaseDBNewInventory = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: db
  namespace: apps
status:
  inventory:
    entries:
      - id: "apps_db_apps_StatefulSet"
      - id: "apps_db-init__ConfigMap"
`

// helmReleaseDBOldInventory is HelmRelease apps/db while the upgrade still
// runs: helm-controller writes the new inventory only when the release ends,
// so it still lists the ConfigMap the new chart version no longer renders.
const helmReleaseDBOldInventory = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: db
  namespace: apps
status:
  inventory:
    entries:
      - id: "apps_db_apps_StatefulSet"
      - id: "apps_db-init-old__ConfigMap"
`

const helmReleaseDBKey = "helm.toolkit.fluxcd.io/v2|HelmRelease|db|apps"

func helmReleaseChildrenProcessor(mcp *fakeMCPClient) *Processor {
	return New(&config.Config{ReadinessExpandHelmReleases: true}, mcp, nil, nil, &fakeCommenter{}, nil, nil, slog.Default())
}

func dbParents() map[string]string {
	return map[string]string{
		rowKey("StatefulSet", "apps", "db"):        "apps/db",
		rowKey("ConfigMap", "apps", "db-init-old"): "apps/db",
	}
}

// An upgrade still running: the HelmRelease's inventory is the previous
// release's, so an object the new chart version removed or renamed reads
// NotFound. That is the upgrade in progress, not a failure: the row reads
// InProgress (⚠️), not NotFound (❌).
func TestSettleHelmReleaseChildren_UpgradeInProgressReadsInProgress(t *testing.T) {
	proc := helmReleaseChildrenProcessor(&fakeMCPClient{resources: map[string][]byte{helmReleaseDBKey: []byte(helmReleaseDBOldInventory)}})
	statuses := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "apps", Name: "db", Status: "InProgress"},
		{Kind: "StatefulSet", Namespace: "apps", Name: "db", Status: "InProgress"},
		{Kind: "ConfigMap", Namespace: "apps", Name: "db-init-old", Status: "NotFound", Message: "resource not found"},
	}
	got := proc.settleHelmReleaseChildren(context.Background(), statuses, dbParents())
	if len(got) != 3 {
		t.Fatalf("want all 3 rows kept, got %+v", got)
	}
	cm := got[2]
	if cm.Status != "InProgress" {
		t.Fatalf("NotFound child of a reconciling HelmRelease must read InProgress, got %q", cm.Status)
	}
	if cm.Message != "not found while HelmRelease apps/db is still reconciling" {
		t.Errorf("unexpected message %q", cm.Message)
	}
	if failed, _ := countStatuses(got); failed != 0 {
		t.Errorf("no row may count as failed, got %d", failed)
	}
}

// The upgrade finished between the poll's inventory read and its row reads:
// the fresh HelmRelease inventory no longer lists the object, so the release
// removed it on purpose and the row is dropped.
func TestSettleHelmReleaseChildren_RemovedByReleaseIsDropped(t *testing.T) {
	proc := helmReleaseChildrenProcessor(&fakeMCPClient{resources: map[string][]byte{helmReleaseDBKey: []byte(helmReleaseDBNewInventory)}})
	statuses := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "apps", Name: "db", Status: "Current"},
		{Kind: "StatefulSet", Namespace: "apps", Name: "db", Status: "Current"},
		{Kind: "ConfigMap", Namespace: "apps", Name: "db-init-old", Status: "NotFound"},
	}
	got := proc.settleHelmReleaseChildren(context.Background(), statuses, dbParents())
	if len(got) != 2 {
		t.Fatalf("want the removed ConfigMap dropped, got %+v", got)
	}
	for _, r := range got {
		if r.Name == "db-init-old" {
			t.Fatalf("removed ConfigMap still listed: %+v", got)
		}
	}
}

// A settled HelmRelease that still lists the object: the object is genuinely
// missing and stays a failure.
func TestSettleHelmReleaseChildren_MissingChildOfSettledReleaseStaysNotFound(t *testing.T) {
	proc := helmReleaseChildrenProcessor(&fakeMCPClient{resources: map[string][]byte{helmReleaseDBKey: []byte(helmReleaseDBOldInventory)}})
	statuses := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "apps", Name: "db", Status: "Current"},
		{Kind: "ConfigMap", Namespace: "apps", Name: "db-init-old", Status: "NotFound"},
	}
	got := proc.settleHelmReleaseChildren(context.Background(), statuses, dbParents())
	if len(got) != 2 || got[1].Status != "NotFound" {
		t.Fatalf("missing child of a settled HelmRelease must stay NotFound, got %+v", got)
	}
}

// A failed release is a failure on its own: its missing children stay
// NotFound rather than softening to InProgress.
func TestSettleHelmReleaseChildren_FailedReleaseKeepsNotFound(t *testing.T) {
	proc := helmReleaseChildrenProcessor(&fakeMCPClient{resources: map[string][]byte{helmReleaseDBKey: []byte(helmReleaseDBOldInventory)}})
	statuses := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "apps", Name: "db", Status: "Failed"},
		{Kind: "ConfigMap", Namespace: "apps", Name: "db-init-old", Status: "NotFound"},
	}
	got := proc.settleHelmReleaseChildren(context.Background(), statuses, dbParents())
	if got[1].Status != "NotFound" {
		t.Fatalf("child of a failed HelmRelease must stay NotFound, got %+v", got)
	}
}

// When the HelmRelease can't be re-read, nothing is dropped; a reconciling
// release still turns its missing children InProgress, a settled one leaves
// them NotFound.
func TestSettleHelmReleaseChildren_RereadFailure(t *testing.T) {
	statusesFor := func(hrStatus string) []readiness.ResourceStatus {
		return []readiness.ResourceStatus{
			{Kind: "HelmRelease", Namespace: "apps", Name: "db", Status: hrStatus},
			{Kind: "ConfigMap", Namespace: "apps", Name: "db-init-old", Status: "NotFound"},
		}
	}
	proc := helmReleaseChildrenProcessor(&fakeMCPClient{err: errors.New("mcp down")})
	if got := proc.settleHelmReleaseChildren(context.Background(), statusesFor("InProgress"), dbParents()); len(got) != 2 || got[1].Status != "InProgress" {
		t.Fatalf("reconciling release, re-read failed: want InProgress row kept, got %+v", got)
	}
	if got := proc.settleHelmReleaseChildren(context.Background(), statusesFor("Current"), dbParents()); len(got) != 2 || got[1].Status != "NotFound" {
		t.Fatalf("settled release, re-read failed: want NotFound row kept, got %+v", got)
	}
}

// NotFound rows that didn't come from a HelmRelease's inventory are left to
// dropPrunedRows and the marker rules, and nothing is read for them.
func TestSettleHelmReleaseChildren_IgnoresOtherRows(t *testing.T) {
	mcp := &fakeMCPClient{err: errors.New("must not be called")}
	proc := helmReleaseChildrenProcessor(mcp)
	statuses := []readiness.ResourceStatus{
		{Kind: "HelmRelease", Namespace: "apps", Name: "db", Status: "InProgress"},
		{Kind: "ConfigMap", Namespace: "apps", Name: "standalone", Status: "NotFound"},
	}
	got := proc.settleHelmReleaseChildren(context.Background(), statuses, dbParents())
	if len(got) != 2 || got[1].Status != "NotFound" {
		t.Fatalf("non-child NotFound row must be untouched, got %+v", got)
	}
	if mcp.resourceCalls != 0 {
		t.Errorf("made %d MCP reads, want 0", mcp.resourceCalls)
	}
}

// The expansion reports which HelmRelease listed each child, so the rows can
// be traced back after the poll.
func TestExpandHelmReleases_ReportsParents(t *testing.T) {
	proc := helmReleaseChildrenProcessor(&fakeMCPClient{resources: map[string][]byte{helmReleaseDBKey: []byte(helmReleaseDBOldInventory)}})
	_, _, parents := proc.expandHelmReleases(context.Background(), []string{"apps_db_helm.toolkit.fluxcd.io_HelmRelease", "apps_other__ConfigMap"})
	want := map[string]string{
		rowKey("StatefulSet", "apps", "db"):        "apps/db",
		rowKey("ConfigMap", "apps", "db-init-old"): "apps/db",
	}
	if len(parents) != len(want) {
		t.Fatalf("parents = %v, want %v", parents, want)
	}
	for k, v := range want {
		if parents[k] != v {
			t.Errorf("parents[%q] = %q, want %q", k, parents[k], v)
		}
	}
}

// helmReleaseUpgradingYAML is HelmRelease default/db in the middle of an
// upgrade: Reconciling, Ready Unknown, and an inventory that is still the
// previous release's, listing a ConfigMap the new chart version renamed.
const helmReleaseUpgradingYAML = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: db
  namespace: default
  generation: 2
status:
  observedGeneration: 1
  conditions:
    - type: Reconciling
      status: "True"
      reason: Progressing
      message: "Running 'upgrade' action with timeout of 15m0s"
    - type: Ready
      status: Unknown
      reason: Progressing
      message: "Running 'upgrade' action with timeout of 15m0s"
  inventory:
    entries:
      - id: "default_db-init-old__ConfigMap"
`

// A chart upgrade still in progress when the readiness poll ends reads ⚠️,
// not ❌: the ConfigMap the previous release's inventory still lists is
// NotFound because the new chart version renamed it.
func TestProcess_HelmReleaseUpgradeInProgress_IsWarningNotFailure(t *testing.T) {
	mcpMock := &fakeMCPClient{
		data: []byte(`
status:
  inventory:
    entries:
      - id: "default_db_helm.toolkit.fluxcd.io_HelmRelease"
`),
		resources: map[string][]byte{
			"helm.toolkit.fluxcd.io/v2|HelmRelease|db|default": []byte(helmReleaseUpgradingYAML),
		},
	}
	commenterMock := &fakeCommenter{
		prNumber: 42,
		prFiles:  []bridgegithub.PRFile{{Path: "release.yaml", BlobSHA: "hr-blob", Status: "modified"}},
		blobs: map[string][]byte{
			"hr-blob": []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata:\n  name: db\n  namespace: default\n"),
		},
	}
	cfg := &config.Config{
		GitHubRepo:                  "owner/repo",
		ReadinessExpandHelmReleases: true,
		ReadinessPollInterval:       time.Millisecond,
		ReadinessPollTimeout:        10 * time.Millisecond,
		PRLabelEnabled:              true,
	}
	proc := New(cfg, mcpMock, readiness.NewChecker(mcpMock), nil /* no LLM */, commenterMock, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "apps", Namespace: "default"},
		Severity:       eventv1.EventSeverityInfo,
		Reason:         "ReconciliationSucceeded",
	}

	if err := proc.Process(context.Background(), event, "abc1234"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	body := commenterMock.prCommentBody
	if !strings.Contains(body, "# ⚠️ Flux Status") {
		t.Fatalf("want a warning marker, got: %s", body)
	}
	if !strings.Contains(body, "| default | ConfigMap | db-init-old | ⏳ InProgress | not found while HelmRelease default/db is still reconciling |") {
		t.Errorf("want the renamed ConfigMap as InProgress, got: %s", body)
	}
	if len(commenterMock.labelCalls) == 0 {
		t.Fatal("expected a label call")
	}
	if got := commenterMock.labelCalls[len(commenterMock.labelCalls)-1].Outcome; got != bridgegithub.OutcomeWarning {
		t.Errorf("label outcome = %v, want warning", got)
	}
}
