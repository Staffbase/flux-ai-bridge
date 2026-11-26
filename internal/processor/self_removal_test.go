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
	"slices"
	"strings"
	"testing"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	gh "github.com/google/go-github/v62/github"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"

	"github.com/staffbase/flux-ai-bridge/internal/config"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
)

// selfRemovalChild is a child Kustomization whose PR deleted both its CR and
// its source path: the reconcile at the new revision fails with "path not
// found" while the parent hasn't deleted it yet.
const selfRemovalChild = `
metadata:
  name: legacy-crds
  namespace: flux-system
  deletionTimestamp: "2026-10-27T08:02:45Z"
  labels:
    kustomize.toolkit.fluxcd.io/name: cluster-base
    kustomize.toolkit.fluxcd.io/namespace: flux-system
spec:
  path: ./apps/legacy/crds
  prune: true
  sourceRef:
    name: flux-system
    kind: GitRepository
status:
  lastAttemptedRevision: main@sha1:sha-teardown
  conditions:
    - type: Ready
      status: "False"
      reason: ArtifactFailed
      message: "kustomization path not found: stat /tmp/kustomization-1/apps/legacy/crds: no such file or directory"
  inventory:
    entries:
      - id: "_widgets.example.com_apiextensions.k8s.io_CustomResourceDefinition"
`

const selfRemovalParent = `
metadata:
  name: cluster-base
  namespace: flux-system
spec:
  path: ./clusters/base
  prune: true
  sourceRef:
    kind: GitRepository
    name: cluster-base
status:
  lastAttemptedRevision: main@sha1:sha-teardown
  conditions:
    - type: Ready
      status: "True"
      reason: ReconciliationSucceeded
  inventory:
    entries:
      - id: "flux-system_other-app_kustomize.toolkit.fluxcd.io_Kustomization"
`

// selfRemovalChildEntry is the child's entry in its parent's inventory.
const selfRemovalChildEntry = "      - id: \"flux-system_legacy-crds_kustomize.toolkit.fluxcd.io_Kustomization\"\n"

const selfRemovalCR = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: legacy-crds
  namespace: flux-system
`

const selfRemovalCRNoNamespace = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: legacy-crds
`

const selfRemovalSiblingCR = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: other-app
  namespace: flux-system
`

// The child and its parent read the same repository through two
// GitRepositories (e.g. different branches).
var selfRemovalSources = map[string][]byte{
	"source.toolkit.fluxcd.io/v1|GitRepository|flux-system|flux-system":  []byte("spec:\n  url: https://github.com/owner/repo\n"),
	"source.toolkit.fluxcd.io/v1|GitRepository|cluster-base|flux-system": []byte("spec:\n  url: ssh://git@github.com/owner/repo\n"),
	"source.toolkit.fluxcd.io/v1|GitRepository|elsewhere|flux-system":    []byte("spec:\n  url: https://github.com/owner/other-repo\n"),
}

const selfRemovalCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
`

// Every scenario's PR also deletes the CRD file, which attributes it to the
// child through inventory membership.
var selfRemovalCRDFile = bridgegithub.PRFile{Path: "apps/legacy/crds/widgets-crd.yaml", BlobSHA: "crd-blob", Status: "removed"}

func TestProcess_SelfRemoval(t *testing.T) {
	removedCR := bridgegithub.PRFile{Path: "clusters/base/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"}
	for name, tc := range map[string]struct {
		files     []bridgegithub.PRFile
		blobs     map[string]string
		baseFiles map[string]string
		child     string // child Kustomization YAML (default selfRemovalChild)
		parent    string // parent Kustomization YAML (default selfRemovalParent)
		repo      string // GITHUB_REPO override (default none)
		wantRemov bool
	}{
		"CR and path deleted": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": selfRemovalCR},
			wantRemov: true,
		},
		"CR inherits its namespace from the build": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": selfRemovalCRNoNamespace},
			wantRemov: true,
		},
		"CR edited out of a multi-document file": {
			files: []bridgegithub.PRFile{{Path: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "modified"}},
			blobs: map[string]string{"apps-head": selfRemovalSiblingCR},
			baseFiles: map[string]string{
				"clusters/base/apps.yaml": selfRemovalSiblingCR + "---\n" + selfRemovalCR,
			},
			wantRemov: true,
		},
		"CR edited out of a renamed file": {
			files: []bridgegithub.PRFile{{Path: "clusters/base/apps-renamed.yaml", PreviousPath: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "renamed"}},
			blobs: map[string]string{"apps-head": selfRemovalSiblingCR},
			baseFiles: map[string]string{
				"clusters/base/apps.yaml": selfRemovalSiblingCR + "---\n" + selfRemovalCR,
			},
			wantRemov: true,
		},
		"renamed file without its previous path": {
			files: []bridgegithub.PRFile{removedCR, {Path: "clusters/base/apps-renamed.yaml", BlobSHA: "apps-head", Status: "renamed"}},
			blobs: map[string]string{"ks-blob": selfRemovalCR, "apps-head": selfRemovalSiblingCR},
		},
		"CR moved into a .txt resource under the parent": {
			files: []bridgegithub.PRFile{
				removedCR,
				{Path: "clusters/base/resources.txt", BlobSHA: "ks-blob", Status: "added"},
			},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
		},
		"CR moved to another namespace the build rewrites": {
			files: []bridgegithub.PRFile{
				removedCR,
				{Path: "clusters/base/legacy/ks.yaml", BlobSHA: "ks-default", Status: "added"},
			},
			blobs: map[string]string{
				"ks-blob":    selfRemovalCR,
				"ks-default": strings.Replace(selfRemovalCR, "namespace: flux-system", "namespace: default", 1),
			},
		},
		"CR re-added under a name the build prefixes": {
			files: []bridgegithub.PRFile{
				removedCR,
				{Path: "clusters/base/legacy/ks.yaml", BlobSHA: "ks-crds", Status: "added"},
			},
			blobs: map[string]string{
				"ks-blob": selfRemovalCR,
				"ks-crds": strings.Replace(selfRemovalCR, "name: legacy-crds", "name: crds", 1),
			},
		},
		"removed CR in a namespace the parent's targetNamespace rewrites": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": strings.Replace(selfRemovalCR, "namespace: flux-system", "namespace: default", 1)},
			parent:    strings.Replace(selfRemovalParent, "prune: true", "prune: true\n  targetNamespace: flux-system", 1),
			wantRemov: true,
		},
		"removed CR in a namespace the parent's kustomization.yaml rewrites": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": strings.Replace(selfRemovalCR, "namespace: flux-system", "namespace: default", 1)},
			wantRemov: true,
		},
		"removed CR with a templated namespace holding underscores": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": strings.Replace(selfRemovalCR, "namespace: flux-system", "namespace: ${FLUX_NS_NAME}", 1)},
			wantRemov: true,
		},
		"removed CR the build prefixes": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": strings.Replace(selfRemovalCR, "name: legacy-crds", "name: crds", 1)},
			wantRemov: true,
		},
		"CR moved under a templated name": {
			files: []bridgegithub.PRFile{
				removedCR,
				{Path: "clusters/base/legacy/ks.yaml", BlobSHA: "ks-tmpl", Status: "added"},
			},
			blobs: map[string]string{
				"ks-blob": selfRemovalCR,
				"ks-tmpl": strings.Replace(selfRemovalCR, "name: legacy-crds", "name: ${CHILD_NAME}", 1),
			},
		},
		"removed CR under a templated name": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": strings.Replace(selfRemovalCR, "name: legacy-crds", "name: ${CHILD_NAME}", 1)},
			wantRemov: true,
		},
		"parent builds from the source root": {
			files:     []bridgegithub.PRFile{{Path: "legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"}},
			blobs:     map[string]string{"ks-blob": selfRemovalCR},
			parent:    strings.Replace(selfRemovalParent, "  path: ./clusters/base\n", "", 1),
			wantRemov: true,
		},
		"last child removed leaves a Ready parent's inventory empty": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": selfRemovalCR},
			parent:    strings.Replace(selfRemovalParent, "  inventory:\n    entries:\n      - id: \"flux-system_other-app_kustomize.toolkit.fluxcd.io_Kustomization\"\n", "  inventory:\n    entries: []\n", 1),
			wantRemov: true,
		},
		"empty inventory from a parent that isn't Ready": {
			files: []bridgegithub.PRFile{removedCR},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
			parent: strings.Replace(strings.Replace(selfRemovalParent, "  inventory:\n    entries:\n      - id: \"flux-system_other-app_kustomize.toolkit.fluxcd.io_Kustomization\"\n", "  inventory:\n    entries: []\n", 1),
				"status: \"True\"\n      reason: ReconciliationSucceeded", "status: \"Unknown\"\n      reason: Progressing", 1),
		},
		"child isn't being deleted": {
			files: []bridgegithub.PRFile{removedCR},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
			child: strings.Replace(selfRemovalChild, "  deletionTimestamp: \"2026-10-27T08:02:45Z\"\n", "", 1),
		},
		"parent still lists the child": {
			files:  []bridgegithub.PRFile{removedCR},
			blobs:  map[string]string{"ks-blob": selfRemovalCR},
			parent: selfRemovalParent + selfRemovalChildEntry,
		},
		"CR moved to another file": {
			files: []bridgegithub.PRFile{
				removedCR,
				{Path: "clusters/base/legacy/ks.yaml", BlobSHA: "ks-blob", Status: "added"},
			},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
		},
		"only the path deleted": {
			files: nil,
		},
		"CR deleted from another parent's overlay": {
			files: []bridgegithub.PRFile{{Path: "clusters/other/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"}},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
		},
		"CR moved into a List": {
			files: []bridgegithub.PRFile{
				removedCR,
				{Path: "clusters/base/list.yaml", BlobSHA: "list-blob", Status: "added"},
			},
			blobs: map[string]string{
				"ks-blob":   selfRemovalCR,
				"list-blob": "apiVersion: v1\nkind: List\nitems:\n  - apiVersion: kustomize.toolkit.fluxcd.io/v1\n    kind: Kustomization\n    metadata:\n      name: legacy-crds\n      namespace: flux-system\n",
			},
		},
		"a head file doesn't parse": {
			files: []bridgegithub.PRFile{
				removedCR,
				{Path: "clusters/base/broken.yaml", BlobSHA: "broken-blob", Status: "added"},
			},
			blobs: map[string]string{"ks-blob": selfRemovalCR, "broken-blob": "kind: [unclosed\n"},
		},
		"no Flux parent": {
			files: []bridgegithub.PRFile{removedCR},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
			child: strings.Replace(selfRemovalChild, "kustomize.toolkit.fluxcd.io/name: cluster-base", "app.kubernetes.io/name: legacy", 1),
		},
		"child opts out of pruning": {
			files: []bridgegithub.PRFile{removedCR},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
			child: strings.Replace(selfRemovalChild, "  labels:\n", "  annotations:\n    kustomize.toolkit.fluxcd.io/prune: disabled\n  labels:\n", 1),
		},
		"child excluded by a reconcile-disabled label": {
			files: []bridgegithub.PRFile{removedCR},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
			child: strings.Replace(selfRemovalChild, "  labels:\n", "  labels:\n    kustomize.toolkit.fluxcd.io/reconcile: Disabled\n", 1),
		},
		"parent builds from another repository despite GITHUB_REPO": {
			files:  []bridgegithub.PRFile{removedCR},
			blobs:  map[string]string{"ks-blob": selfRemovalCR},
			parent: strings.Replace(selfRemovalParent, "name: cluster-base\nstatus", "name: elsewhere\nstatus", 1),
			repo:   "owner/repo",
		},
		"removal with GITHUB_REPO set": {
			files:     []bridgegithub.PRFile{removedCR},
			blobs:     map[string]string{"ks-blob": selfRemovalCR},
			repo:      "owner/repo",
			wantRemov: true,
		},
		"parent doesn't prune": {
			files:  []bridgegithub.PRFile{removedCR},
			blobs:  map[string]string{"ks-blob": selfRemovalCR},
			parent: strings.Replace(selfRemovalParent, "prune: true", "prune: false", 1),
		},
		"parent is suspended": {
			files:  []bridgegithub.PRFile{removedCR},
			blobs:  map[string]string{"ks-blob": selfRemovalCR},
			parent: strings.Replace(selfRemovalParent, "prune: true", "prune: true\n  suspend: true", 1),
		},
		"parent builds from another repository": {
			files:  []bridgegithub.PRFile{removedCR},
			blobs:  map[string]string{"ks-blob": selfRemovalCR},
			parent: strings.Replace(selfRemovalParent, "name: cluster-base\nstatus", "name: elsewhere\nstatus", 1),
		},
		"parent fails at an older revision": {
			files: []bridgegithub.PRFile{removedCR},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
			parent: strings.Replace(strings.Replace(strings.Replace(selfRemovalParent, `status: "True"`, `status: "False"`, 1),
				"reason: ReconciliationSucceeded", "reason: BuildFailed", 1), "sha1:sha-teardown", "sha1:sha-older", 1),
		},
		"parent fails at this revision": {
			files: []bridgegithub.PRFile{removedCR},
			blobs: map[string]string{"ks-blob": selfRemovalCR},
			parent: strings.Replace(strings.Replace(selfRemovalParent, `status: "True"`, `status: "False"`, 1),
				"reason: ReconciliationSucceeded", "reason: BuildFailed", 1),
		},
	} {
		t.Run(name, func(t *testing.T) {
			blobs := map[string][]byte{"crd-blob": []byte(selfRemovalCRD)}
			for k, v := range tc.blobs {
				blobs[k] = []byte(v)
			}
			baseFiles := map[string][]byte{}
			for k, v := range tc.baseFiles {
				baseFiles[k] = []byte(v)
			}
			commenter := &fakeCommenter{
				prNumber:  42,
				prBaseSHA: "base-sha",
				prFiles:   append([]bridgegithub.PRFile{selfRemovalCRDFile}, tc.files...),
				blobs:     blobs,
				baseFiles: baseFiles,
			}
			child, parent := tc.child, tc.parent
			if child == "" {
				child = selfRemovalChild
			}
			if parent == "" {
				parent = selfRemovalParent
			}
			mcp := &fakeMCPClient{
				data:                 []byte(child),
				kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(parent)},
				resources:            selfRemovalSources,
			}
			analyzer := &fakeAnalyzer{result: "❌\n\n## Root Cause\npath missing\n\n## Next steps to fix\n1. Fix the path."}
			cfg := &config.Config{
				GitHubRepo: tc.repo, LLMProvider: "claude",
				PRLabelEnabled: true, CommentOnSuccess: true,
				ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
			}
			proc := New(cfg, mcp, readiness.NewChecker(mcp), analyzer, commenter, nil, nil, slog.Default())
			event := eventv1.Event{
				InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
				Severity:       eventv1.EventSeverityError,
				Reason:         "ArtifactFailed",
				Message:        "kustomization path not found: stat /tmp/kustomization-1/apps/legacy/crds: no such file or directory",
			}
			if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
				t.Fatalf("Process() error: %v", err)
			}

			body := commenter.prCommentBody
			label, labelled := lastLabel(commenter.labelCalls, 42)
			if !tc.wantRemov {
				if strings.Contains(body, "is being removed by this PR") {
					t.Errorf("reported as a removal:\n%s", body)
				}
				if !labelled || label != bridgegithub.OutcomeFailure {
					t.Errorf("label = %q (set=%v), want failure", label, labelled)
				}
				return
			}
			if !strings.HasPrefix(body, "# ✅ Flux Status") {
				t.Errorf("comment header is not ✅:\n%s", body)
			}
			for _, want := range []string{"is being removed by this PR", "kustomization path not found", "`flux-system/cluster-base`"} {
				if !strings.Contains(body, want) {
					t.Errorf("comment misses %q:\n%s", want, body)
				}
			}
			if analyzer.calls != 0 {
				t.Errorf("LLM called %d times for an expected teardown, want 0", analyzer.calls)
			}
			if !labelled || label != bridgegithub.OutcomeSuccess {
				t.Errorf("label = %q (set=%v), want success", label, labelled)
			}
		})
	}
}

func TestResourceIDsFromYAML_MarksUnparseableRest(t *testing.T) {
	ids, complete := splitParsedIDs(ResourceIDsFromYAML([]byte(selfRemovalCRD + "---\nkind: [unclosed\n")))
	if complete {
		t.Error("a file that stops parsing part-way reported as complete")
	}
	if len(ids) != 1 || ids[0] != "_widgets.example.com_apiextensions.k8s.io_CustomResourceDefinition" {
		t.Errorf("ids = %v, want the CRD parsed before the bad document", ids)
	}
	if _, complete := splitParsedIDs(ResourceIDsFromYAML([]byte(selfRemovalCRD))); !complete {
		t.Error("a well-formed file reported as incomplete")
	}
}

func TestPathWithin(t *testing.T) {
	for _, tc := range []struct {
		path, dir string
		want      bool
	}{
		{"clusters/base/a.yaml", "./clusters/base", true},
		{"clusters/base/sub/a.yaml", "clusters/base/", true},
		{"clusters/base-other/a.yaml", "./clusters/base", false},
		{"clusters/other/a.yaml", "./clusters/base", false},
		{"a.yaml", "./", true},
	} {
		if got := pathWithin(tc.path, tc.dir); got != tc.want {
			t.Errorf("pathWithin(%q, %q) = %v, want %v", tc.path, tc.dir, got, tc.want)
		}
	}
}

// A rate limit inside the self-removal scan (here the base-file fetch that
// attribution skipped) cools the admission mark down like a delivery rate
// limit, instead of releasing it for the next event to hit GitHub again.
func TestProcess_SelfRemoval_RateLimitCoolsDownMark(t *testing.T) {
	cache, mr := newRedisDedupe(t)
	retryAfter := 3 * time.Minute
	commenter := &fakeCommenter{
		prNumber: 42, prHeadSHA: "prhead111", prBaseSHA: "base-sha",
		prFiles: []bridgegithub.PRFile{
			selfRemovalCRDFile,
			{Path: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "modified"},
		},
		blobs: map[string][]byte{
			"crd-blob":  []byte(selfRemovalCRD),
			"apps-head": []byte(selfRemovalSiblingCR),
		},
		baseFilesErr: &gh.AbuseRateLimitError{Message: "secondary rate limit", RetryAfter: &retryAfter},
	}
	mcp := &fakeMCPClient{
		data:                 []byte(selfRemovalChild),
		kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(selfRemovalParent)},
		resources:            selfRemovalSources,
	}
	cfg := &config.Config{LLMProvider: "none", ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond}
	proc := New(cfg, mcp, readiness.NewChecker(mcp), nil, commenter, nil, cache, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityError,
		Message:        "kustomization path not found",
	}
	if err := proc.Process(context.Background(), event, "sha-teardown"); err == nil {
		t.Fatal("expected the rate-limited self-removal check to fail the event")
	}
	if got := mr.TTL("p:flux-system:legacy-crds:pr42:prhead111"); got != retryAfter {
		t.Fatalf("mark TTL after a rate limit = %s, want the %s Retry-After", got, retryAfter)
	}
}

// GitHub lists at most prFilesCap files per PR; a listing that long may hide
// the file that still declares the CR, so it keeps the normal report.
func TestPRRemovesSelf_CappedListingIsUnsure(t *testing.T) {
	files := []bridgegithub.PRFile{{Path: "clusters/base/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"}}
	for len(files) < prFilesCap {
		files = append(files, bridgegithub.PRFile{Path: "docs/note.md", Status: "modified"})
	}
	commenter := &fakeCommenter{
		prNumber: 42, prFiles: files,
		blobs: map[string][]byte{"ks-blob": []byte(selfRemovalCR)},
	}
	mcp := &fakeMCPClient{
		data:                 []byte(selfRemovalChild),
		kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(selfRemovalParent)},
		resources:            selfRemovalSources,
	}
	proc := New(&config.Config{LLMProvider: "none"}, mcp, readiness.NewChecker(mcp), nil, commenter, nil, nil, slog.Default())
	var kspec kustomizationSpec
	if err := yaml.Unmarshal([]byte(selfRemovalChild), &kspec); err != nil {
		t.Fatal(err)
	}
	event := eventv1.Event{InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"}}
	_, expected, err := proc.selfRemovalExpected(context.Background(), "owner", "repo", event, kspec,
		[]bridgegithub.PRInfo{{Number: 42}})
	if err != nil || expected[42] {
		t.Errorf("selfRemovalExpected on a capped listing = %v (err %v), want PR 42 not verified", expected, err)
	}
	commenter.prFiles = files[:1]
	if _, expected, _ := proc.selfRemovalExpected(context.Background(), "owner", "repo", event, kspec,
		[]bridgegithub.PRInfo{{Number: 42}}); !expected[42] {
		t.Error("control: the same removal below the cap should be expected")
	}
}

// A non-List resource may carry its own top-level `items` (a mapping here):
// it keeps its ID, and the documents after it are still read.
func TestResourceIDsFromYAML_NonListItemsKept(t *testing.T) {
	data := "apiVersion: example.com/v1\nkind: Catalog\nmetadata:\n  name: shop\n  namespace: apps\nitems:\n  featured: true\n---\n" + selfRemovalCRD
	ids, complete := splitParsedIDs(ResourceIDsFromYAML([]byte(data)))
	want := []string{"apps_shop_example.com_Catalog", "_widgets.example.com_apiextensions.k8s.io_CustomResourceDefinition"}
	if !complete || len(ids) != 2 || ids[0] != want[0] || ids[1] != want[1] {
		t.Errorf("ids = %v (complete %v), want %v (complete)", ids, complete, want)
	}
}

func TestBuildSelfRemovalBody_ResourceWording(t *testing.T) {
	for policy, want := range map[string]bool{"": true, "MirrorPrune": true, "Delete": true, "WaitForTermination": true, "Orphan": false} {
		var k kustomizationSpec
		k.Spec.Prune = true
		k.Spec.DeletionPolicy = policy
		if got := deletesResourcesOnDeletion(k); got != want {
			t.Errorf("deletesResourcesOnDeletion(prune, %q) = %v, want %v", policy, got, want)
		}
	}
	var noPrune kustomizationSpec
	if deletesResourcesOnDeletion(noPrune) {
		t.Error("MirrorPrune without spec.prune must keep the resources")
	}
	var suspended kustomizationSpec
	suspended.Spec.Prune, suspended.Spec.DeletionPolicy, suspended.Spec.Suspend = true, "Delete", true
	if deletesResourcesOnDeletion(suspended) {
		t.Error("a suspended Kustomization's finalizer deletes nothing")
	}
	if body := buildSelfRemovalBody("legacy-crds", "flux-system/cluster-base", "path not found", "", false); !strings.Contains(body, "stay on the cluster") {
		t.Errorf("orphaning body doesn't say the resources stay:\n%s", body)
	}
	// Flux skips objects excluded from pruning (the prune or reconcile key set
	// to disabled) even then, so the body mustn't promise every one goes.
	if body := buildSelfRemovalBody("legacy-crds", "flux-system/cluster-base", "path not found", "", true); !strings.Contains(body, "also prunes the resources it manages, except those excluded from pruning") {
		t.Errorf("deleting body doesn't describe the pruning policy:\n%s", body)
	}
}

// The parent usually reconciles the removal moments after the child fails:
// the check waits, within the readiness budget, for its inventory to drop the
// child.
func TestProcess_SelfRemoval_WaitsForParentToDropChild(t *testing.T) {
	commenter := &fakeCommenter{
		prNumber: 42,
		prFiles: []bridgegithub.PRFile{
			selfRemovalCRDFile,
			{Path: "clusters/base/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"},
		},
		blobs: map[string][]byte{"crd-blob": []byte(selfRemovalCRD), "ks-blob": []byte(selfRemovalCR)},
	}
	withChild := []byte(selfRemovalParent + selfRemovalChildEntry)
	mcp := &fakeMCPClient{
		data: []byte(selfRemovalChild),
		kustomizationsByNameQueue: map[string][][]byte{
			"flux-system/cluster-base": {withChild, withChild, []byte(selfRemovalParent)},
		},
		resources: selfRemovalSources,
	}
	cfg := &config.Config{
		LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: time.Second,
	}
	proc := New(cfg, mcp, readiness.NewChecker(mcp), nil, commenter, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityError,
		Message:        "kustomization path not found",
	}
	if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if !strings.Contains(commenter.prCommentBody, "is being removed by this PR") {
		t.Errorf("removal not reported once the parent dropped the child:\n%s", commenter.prCommentBody)
	}
}

// A custom resource of a `*List` kind can carry its own items and a name: it
// keeps its ID, and the unread items still mark the file incomplete.
func TestResourceIDsFromYAML_NamedListKindKeepsID(t *testing.T) {
	data := "apiVersion: example.com/v1\nkind: AllowList\nmetadata:\n  name: egress\n  namespace: apps\nitems:\n  - host: example.org\n"
	ids, complete := splitParsedIDs(ResourceIDsFromYAML([]byte(data)))
	if complete {
		t.Error("unread items reported as complete")
	}
	if len(ids) != 1 || ids[0] != "apps_egress_example.com_AllowList" {
		t.Errorf("ids = %v, want the AllowList's own ID", ids)
	}
}

// The parent must still be able to prune when it drops the child: a snapshot
// that turned Ready=False (a failed prune), stopped pruning or got suspended
// while the check waited doesn't count, nor does one that switched to another
// build than the PR was judged against. The child is terminating in every
// case, so only the parent gate decides; a Ready, pruning parent is the
// positive control.
func TestParentDroppedChild_RechecksParentGates(t *testing.T) {
	pruneFailed := strings.Replace(strings.Replace(selfRemovalParent, `status: "True"`, `status: "False"`, 1),
		"reason: ReconciliationSucceeded", "reason: PruneFailed", 1)
	var attributed kustomizationSpec
	if err := yaml.Unmarshal([]byte(selfRemovalParent), &attributed); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		snapshot string
		want     bool
	}{
		"ready, pruning parent": {snapshot: selfRemovalParent, want: true},
		"prune failed":          {snapshot: pruneFailed},
		"prune disabled":        {snapshot: strings.Replace(selfRemovalParent, "prune: true", "prune: false", 1)},
		"parent suspended":      {snapshot: strings.Replace(selfRemovalParent, "prune: true", "prune: true\n  suspend: true", 1)},
		"path switched":         {snapshot: strings.Replace(selfRemovalParent, "path: ./clusters/base", "path: ./clusters/other", 1)},
		"source switched":       {snapshot: strings.Replace(selfRemovalParent, "name: cluster-base\nstatus", "name: elsewhere\nstatus", 1)},
		"components changed":    {snapshot: strings.Replace(selfRemovalParent, "prune: true", "prune: true\n  components:\n    - ../components/extra", 1)},
		"missing components ignored": {snapshot: strings.Replace(selfRemovalParent, "prune: true",
			"prune: true\n  ignoreMissingComponents: true", 1)},
	} {
		mcp := &fakeMCPClient{data: []byte(selfRemovalChild), kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(tc.snapshot)}}
		proc := New(&config.Config{ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 5 * time.Millisecond},
			mcp, readiness.NewChecker(mcp), nil, &fakeCommenter{}, nil, nil, slog.Default())
		if got := proc.parentDroppedChild(context.Background(), attributed, "cluster-base", "flux-system", "legacy-crds", "flux-system", "child-uid"); got != tc.want {
			t.Errorf("%s: parentDroppedChild = %v, want %v", name, got, tc.want)
		}
	}
}

// A parent that switches its build between the PR file scan and the
// inventory poll renders another build: its dropping the child doesn't prove
// this PR removed it, so the normal report stays.
func TestProcess_SelfRemoval_ParentSwitchedBuildDuringCheck(t *testing.T) {
	parentListing := selfRemovalParent + selfRemovalChildEntry // still lists the child
	switched := strings.Replace(selfRemovalParent, "path: ./clusters/base", "path: ./clusters/other", 1)
	commenter := &fakeCommenter{
		prNumber:  42,
		prBaseSHA: "base-sha",
		prFiles:   []bridgegithub.PRFile{selfRemovalCRDFile, {Path: "clusters/base/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"}},
		blobs:     map[string][]byte{"crd-blob": []byte(selfRemovalCRD), "ks-blob": []byte(selfRemovalCR)},
	}
	mcp := &fakeMCPClient{
		data: []byte(selfRemovalChild),
		kustomizationsByNameQueue: map[string][][]byte{
			"flux-system/cluster-base": {[]byte(parentListing), []byte(parentListing), []byte(switched)},
		},
		resources: selfRemovalSources,
	}
	cfg := &config.Config{
		LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 50 * time.Millisecond,
	}
	proc := New(cfg, mcp, readiness.NewChecker(mcp), nil, commenter, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityError,
		Reason:         "ArtifactFailed",
		Message:        "kustomization path not found: stat /tmp/kustomization-1/apps/legacy/crds: no such file or directory",
	}
	if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if strings.Contains(commenter.prCommentBody, "is being removed by this PR") {
		t.Errorf("a parent that switched builds mid-check confirmed the removal:\n%s", commenter.prCommentBody)
	}
}

// Only the failures a removal causes qualify: the deleted source path or a
// dependency wait. A failed finalizer keeps the child alive and stays a
// failure.
func TestTeardownFailure(t *testing.T) {
	for _, tc := range []struct {
		failed, waiting string
		want            bool
	}{
		{"kustomization path not found: stat /tmp/x: no such file or directory", "", true},
		{"", "dependency 'flux-system/legacy-db' is not ready", true},
		{"pruning for deleted resource failed", "", false},
		{"kustomize build failed: accumulating resources", "", false},
	} {
		if got := teardownFailure(tc.failed, tc.waiting); got != tc.want {
			t.Errorf("teardownFailure(%q, %q) = %v, want %v", tc.failed, tc.waiting, got, tc.want)
		}
	}
}

// A child that's already gone counts as deleted: a 404, or MCP's ambiguous
// "No resources found" backed by a served Kustomization list without it. A
// live one without a deletionTimestamp doesn't, nor does a read error, a
// replacement under the same name, or an Event list that can't be read.
func TestChildDeleting(t *testing.T) {
	finalizerEvent := []byte("items:\n  - involvedObject:\n      kind: Kustomization\n      name: legacy-crds\n    type: Warning\n    reason: ArtifactFailed\n    message: pruning for deleted resource failed\n")
	notFound := errors.New(`kustomizations.kustomize.toolkit.fluxcd.io "legacy-crds" not found`)
	noResources := errors.New("No resources found in flux-system namespace.")
	siblingOnly := []byte("items:\n- metadata:\n    name: cluster-base\n")
	for name, tc := range map[string]struct {
		data      string
		err       error
		listed    []byte // the namespace's Kustomization list
		events    []byte
		eventsErr error
		want      bool
	}{
		"terminating, finalizer failed": {data: selfRemovalChild, events: finalizerEvent},
		"gone, reported as not found":   {err: notFound, want: true},
		// The UID filter alone decides: a non-retryable failure counts even
		// once the child is gone, unless it was an earlier object's.
		"gone, cleanup failure of an earlier object with the same name": {err: notFound,
			events: []byte(strings.Replace(strings.Replace(string(finalizerEvent), "name: legacy-crds\n", "name: legacy-crds\n      uid: old-uid\n", 1),
				"pruning for deleted resource failed", "unable to prune objects", 1)), want: true},
		"gone after a retried cleanup failure recovered": {err: noResources, listed: siblingOnly,
			events: finalizerEvent, want: true},
		"gone after a cleanup failure": {err: noResources, listed: siblingOnly,
			events: []byte(strings.Replace(string(finalizerEvent), "pruning for deleted resource failed", "unable to prune objects", 1))},
		"terminating, Ready=False for another reason": {data: strings.Replace(selfRemovalChild,
			"kustomization path not found: stat /tmp/kustomization-1/apps/legacy/crds: no such file or directory", "health check failed", 1)},
		"gone":                                  {err: noResources, listed: siblingOnly, want: true},
		"No resources found, list still has it": {err: noResources, listed: []byte("items:\n- metadata:\n    name: cluster-base\n- metadata:\n    name: legacy-crds\n")},
		"No resources found, no list":           {err: noResources},
		"terminating":                           {data: selfRemovalChild, want: true},
		"terminating replacement with another uid": {data: strings.Replace(selfRemovalChild, "  name: legacy-crds\n", "  name: legacy-crds\n  uid: new-uid\n", 1)},
		"terminating, events unreadable":           {data: selfRemovalChild, eventsErr: noResources},
		"live":                                     {data: strings.Replace(selfRemovalChild, "  deletionTimestamp: \"2026-10-27T08:02:45Z\"\n", "", 1)},
		"read error":                               {err: errors.New("connection refused")},
	} {
		mcp := &fakeMCPClient{data: []byte(tc.data), kustomizationErr: tc.err, eventsErr: tc.eventsErr, lists: map[string][]byte{
			"v1|Event|flux-system": tc.events,
			"kustomize.toolkit.fluxcd.io/v1|Kustomization|flux-system": tc.listed,
		}}
		proc := New(&config.Config{}, mcp, readiness.NewChecker(mcp), nil, &fakeCommenter{}, nil, nil, slog.Default())
		if got := proc.childDeleting(context.Background(), "legacy-crds", "flux-system", "child-uid"); got != tc.want {
			t.Errorf("%s: childDeleting = %v, want %v", name, got, tc.want)
		}
	}
}

// A PR that only edits the child's CR out of a shared parent file, with no
// other change to the child, is attributed to it for the self-removal check —
// whether or not the child's build scope resolves, also through a rename — but
// only for a file under the parent's path, never a sibling overlay's.
func TestProcess_SelfRemoval_InPlaceOnlyRemoval(t *testing.T) {
	base := map[string][]byte{
		"clusters/base/apps.yaml":  []byte(selfRemovalSiblingCR + "---\n" + selfRemovalCR),
		"clusters/other/apps.yaml": []byte(selfRemovalSiblingCR + "---\n" + selfRemovalCR),
		"clusters/base/prefixed.yaml": []byte(selfRemovalSiblingCR + "---\n" +
			strings.Replace(selfRemovalCR, "name: legacy-crds", "name: crds", 1)),
	}
	for name, tc := range map[string]struct {
		file    bridgegithub.PRFile
		scope   ScopeResolver
		parent  string // default selfRemovalParent
		noBase  bool   // PR without a base SHA
		removed bool
	}{
		"removed file without a PR base SHA": {
			file:    bridgegithub.PRFile{Path: "clusters/base/crds-ks.yaml", BlobSHA: "crds-blob", Status: "removed"},
			scope:   fakeScope{err: errors.New("kustomization path not found")},
			noBase:  true,
			removed: true,
		},
		"removed file with a name the parent build prefixes": {
			file:    bridgegithub.PRFile{Path: "clusters/base/crds-ks.yaml", BlobSHA: "crds-blob", Status: "removed"},
			scope:   fakeScope{err: errors.New("kustomization path not found")},
			removed: true,
		},
		"name the parent build prefixes": {
			file:    bridgegithub.PRFile{Path: "clusters/base/prefixed.yaml", BlobSHA: "apps-head", Status: "modified"},
			scope:   fakeScope{err: errors.New("kustomization path not found")},
			removed: true,
		},
		"parent from another repository": {
			file:   bridgegithub.PRFile{Path: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "modified"},
			scope:  fakeScope{err: errors.New("kustomization path not found")},
			parent: strings.Replace(selfRemovalParent, "name: cluster-base\nstatus", "name: elsewhere\nstatus", 1),
		},
		"scope unavailable": {
			file:    bridgegithub.PRFile{Path: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "modified"},
			scope:   fakeScope{err: errors.New("kustomization path not found")},
			removed: true,
		},
		"scope resolves without the parent file": {
			file:    bridgegithub.PRFile{Path: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "modified"},
			scope:   fakeScope{files: map[string]struct{}{"apps/legacy/crds/kustomization.yaml": {}}},
			removed: true,
		},
		"renamed file": {
			file:    bridgegithub.PRFile{Path: "clusters/base/apps-renamed.yaml", PreviousPath: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "renamed"},
			scope:   fakeScope{err: errors.New("kustomization path not found")},
			removed: true,
		},
		"sibling overlay": {
			file:  bridgegithub.PRFile{Path: "clusters/other/apps.yaml", BlobSHA: "apps-head", Status: "modified"},
			scope: fakeScope{err: errors.New("kustomization path not found")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			baseSHA := "base-sha"
			if tc.noBase {
				baseSHA = ""
			}
			commenter := &fakeCommenter{
				prNumber: 42, prBaseSHA: baseSHA,
				prFiles: []bridgegithub.PRFile{tc.file},
				blobs: map[string][]byte{
					"apps-head": []byte(selfRemovalSiblingCR),
					"crds-blob": []byte(strings.Replace(selfRemovalCR, "name: legacy-crds", "name: crds", 1)),
				},
				baseFiles: base,
			}
			parent := tc.parent
			if parent == "" {
				parent = selfRemovalParent
			}
			mcp := &fakeMCPClient{
				data:                 []byte(selfRemovalChild),
				kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(parent)},
				resources:            selfRemovalSources,
			}
			cfg := &config.Config{
				LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
				ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
			}
			proc := New(cfg, mcp, readiness.NewChecker(mcp), nil, commenter, nil, nil, slog.Default()).WithScopeResolver(tc.scope)
			event := eventv1.Event{
				InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
				Severity:       eventv1.EventSeverityError,
				Message:        "kustomization path not found: stat /tmp/x: no such file or directory",
			}
			if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
				t.Fatalf("Process() error: %v", err)
			}
			if got := strings.Contains(commenter.prCommentBody, "is being removed by this PR"); got != tc.removed {
				t.Errorf("removal reported = %v, want %v; comment:\n%s", got, tc.removed, commenter.prCommentBody)
			}
			if !tc.removed && (commenter.prCommentBody != "" || len(commenter.labelCalls) > 0) {
				t.Errorf("a PR not confirmed as the removal got the child's report: labels=%v comment:\n%s",
					commenter.labelCalls, commenter.prCommentBody)
			}
		})
	}
}

// A PR verified as the removal (✅) stays attributed at its head: when the
// child's finalizer then fails, the next re-eval replaces the ✅ with the
// failure, even for an in-place-only removal nothing else attributes.
func TestProcess_SelfRemoval_LaterFinalizerFailureReplacesSuccess(t *testing.T) {
	for name, failedOnly := range map[string]bool{"failed-only re-eval": true, "plain re-eval": false} {
		t.Run(name, func(t *testing.T) { testLaterFinalizerFailureReplacesSuccess(t, failedOnly) })
	}
}

func testLaterFinalizerFailureReplacesSuccess(t *testing.T, failedOnly bool) {
	cache, mr := newRedisDedupe(t)
	commenter := &fakeCommenter{
		prNumber: 42, prHeadSHA: "prhead111", prBaseSHA: "base-sha",
		prFiles:   []bridgegithub.PRFile{{Path: "clusters/base/apps.yaml", BlobSHA: "apps-head", Status: "modified"}},
		blobs:     map[string][]byte{"apps-head": []byte(selfRemovalSiblingCR)},
		baseFiles: map[string][]byte{"clusters/base/apps.yaml": []byte(selfRemovalSiblingCR + "---\n" + selfRemovalCR)},
	}
	mcp := &fakeMCPClient{
		data:                 []byte(selfRemovalChild),
		kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(selfRemovalParent)},
		resources:            selfRemovalSources,
	}
	cfg := &config.Config{
		LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
		DedupePRReEvalTTL: time.Minute, DedupeReEvalFailedOnly: failedOnly,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcp, readiness.NewChecker(mcp), nil, commenter, nil, cache, slog.Default()).
		WithScopeResolver(fakeScope{err: errors.New("kustomization path not found")})
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityError,
		Message:        "kustomization path not found: stat /tmp/x: no such file or directory",
	}
	if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
		t.Fatalf("first Process() error: %v", err)
	}
	if label, _ := lastLabel(commenter.labelCalls, 42); !strings.Contains(commenter.prCommentBody, "is being removed by this PR") || label != bridgegithub.OutcomeSuccess {
		t.Fatalf("first event: label=%q comment:\n%s", label, commenter.prCommentBody)
	}

	mr.FastForward(time.Hour) // many re-eval windows: the verified-removal record must outlive them
	event.Message = "pruning for deleted resource failed"
	if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
		t.Fatalf("second Process() error: %v", err)
	}
	if strings.Contains(commenter.prCommentBody, "is being removed by this PR") {
		t.Errorf("the finalizer failure didn't replace the removal report:\n%s", commenter.prCommentBody)
	}
	if label, _ := lastLabel(commenter.labelCalls, 42); label != bridgegithub.OutcomeFailure {
		t.Errorf("label after the finalizer failure = %q, want failure", label)
	}
}

// In a batch, a PR that only deletes one of the child's resources doesn't
// hold back the PR that deletes the child: each is judged on its own, the
// deleting PR gets the removal report and the other the normal one.
func TestProcess_SelfRemoval_BatchSplitsVerdicts(t *testing.T) {
	commenter := &fakeCommenter{
		prs: []bridgegithub.PRInfo{
			{Number: 42, HeadSHA: "head42", State: "open"},
			{Number: 43, HeadSHA: "head43", State: "open"},
		},
		filesByPR: map[int][]bridgegithub.PRFile{
			42: {selfRemovalCRDFile, {Path: "clusters/base/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"}},
			43: {selfRemovalCRDFile},
		},
		blobs: map[string][]byte{"crd-blob": []byte(selfRemovalCRD), "ks-blob": []byte(selfRemovalCR)},
	}
	mcp := &fakeMCPClient{
		data:                 []byte(selfRemovalChild),
		kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(selfRemovalParent)},
		resources:            selfRemovalSources,
	}
	cfg := &config.Config{
		LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcp, readiness.NewChecker(mcp), nil, commenter, nil, nil, slog.Default())
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityError,
		Message:        "kustomization path not found: stat /tmp/x: no such file or directory",
	}
	if err := proc.Process(context.Background(), event, "sha-teardown"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	bodies := map[int]string{}
	for _, post := range commenter.posts {
		bodies[post.Number] = post.Body
	}
	if !strings.Contains(bodies[42], "is being removed by this PR") {
		t.Errorf("PR 42 deletes the child but didn't get the removal report:\n%s", bodies[42])
	}
	if bodies[43] == "" || strings.Contains(bodies[43], "is being removed by this PR") {
		t.Errorf("PR 43 only deletes a resource and should get the normal report:\n%s", bodies[43])
	}
}

// A removed child never reports on a later PR head, so the removal report
// takes its scope off the PR's label roster: a later head whose remaining
// scopes are green gets flux-success instead of waiting for the child.
func TestProcess_SelfRemoval_RetiresScopeFromLabelRoster(t *testing.T) {
	cache, _ := newRedisDedupe(t)
	commenter := &fakeCommenter{
		prNumber: 42, prHeadSHA: "h1",
		prFiles: []bridgegithub.PRFile{
			selfRemovalCRDFile,
			{Path: "clusters/base/legacy-crds-ks.yaml", BlobSHA: "ks-blob", Status: "removed"},
		},
		blobs: map[string][]byte{"crd-blob": []byte(selfRemovalCRD), "ks-blob": []byte(selfRemovalCR)},
	}
	mcp := &fakeMCPClient{
		data:                 []byte(selfRemovalChild),
		kustomizationsByName: map[string][]byte{"flux-system/cluster-base": []byte(selfRemovalParent)},
		resources:            selfRemovalSources,
	}
	cfg := &config.Config{
		LLMProvider: "none", PRLabelEnabled: true, CommentOnSuccess: true,
		ReadinessPollInterval: time.Millisecond, ReadinessPollTimeout: 10 * time.Millisecond,
	}
	proc := New(cfg, mcp, readiness.NewChecker(mcp), nil, commenter, nil, cache, slog.Default())
	ctx := context.Background()
	// A sibling Kustomization of the same PR fails on head h1.
	_ = proc.reconcileLabels(ctx, "owner", "repo", "flux-system/other", "h1", 42, bridgegithub.OutcomeFailure)
	event := eventv1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Kustomization", Name: "legacy-crds", Namespace: "flux-system"},
		Severity:       eventv1.EventSeverityError,
		Message:        "kustomization path not found: stat /tmp/x: no such file or directory",
	}
	if err := proc.Process(ctx, event, "sha-teardown"); err != nil {
		t.Fatalf("Process() error: %v", err)
	}
	if slices.Contains(cache.PRScopes("owner", "repo", 42), "flux-system/legacy-crds") {
		t.Errorf("removed child still in the label roster: %v", cache.PRScopes("owner", "repo", 42))
	}
	// A new head fixes the sibling; the removed child never reports again.
	_ = proc.reconcileLabels(ctx, "owner", "repo", "flux-system/other", "h2", 42, bridgegithub.OutcomeSuccess)
	if label, _ := lastLabel(commenter.labelCalls, 42); label != bridgegithub.OutcomeSuccess {
		t.Errorf("label on the new head = %q, want success (the removed child mustn't hold it)", label)
	}
}
