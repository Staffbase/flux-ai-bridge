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
	"slices"
	"strings"
	"time"

	eventv1 "github.com/fluxcd/pkg/apis/event/v1beta1"
	"gopkg.in/yaml.v3"

	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	bridgesource "github.com/staffbase/flux-ai-bridge/internal/source"
)

// Labels and annotation kustomize-controller uses on the objects it applies.
const (
	parentNameLabel      = "kustomize.toolkit.fluxcd.io/name"
	parentNamespaceLabel = "kustomize.toolkit.fluxcd.io/namespace"
	pruneKey             = "kustomize.toolkit.fluxcd.io/prune"
	reconcileKey         = "kustomize.toolkit.fluxcd.io/reconcile"
)

// teardownFailure reports whether the child's failure is one its removal
// causes: its source path is gone ("kustomization path not found"), or it waits
// on a dependsOn target the PR removed too. Anything else — notably a failed
// finalizer ("pruning for deleted resource failed"), which keeps the child
// from going away — is a real failure and keeps the normal report.
func teardownFailure(reconcileFailedMsg, reconcileWaitingMsg string) bool {
	if reconcileFailedMsg == "" {
		return reconcileWaitingMsg != ""
	}
	return strings.Contains(reconcileFailedMsg, "kustomization path not found")
}

// prFilesCap is the most files GitHub lists for a pull request. A listing
// that long may be cut short, so it can't prove the CR is declared nowhere.
const prFilesCap = 3000

// selfRemovalExpected reports which PRs this Kustomization's failing reconcile
// is the expected deletion for: the PRs that delete its CR from its parent's
// build, when that parent is deleting it. It returns the parent's
// `namespace/name` for the comment and the set of those PRs (nil: none). Each condition fails closed —
// anything it can't confirm keeps the normal report:
//
//   - a Flux parent applied it (the kustomize.toolkit.fluxcd.io/{name,
//     namespace} labels) — a hand-applied Kustomization isn't deleted when its
//     file goes away;
//   - the parent prunes (spec.prune), isn't suspended (a suspended parent
//     keeps its last Ready but never reconciles), and this object isn't
//     excluded from pruning (see pruneExcluded);
//   - the parent builds from the same repository as this Kustomization — the
//     PR's files say nothing about a parent built from elsewhere (the branch
//     may differ, e.g. a separate branch that feeds shared bases). Compared on
//     the sources themselves, not the GITHUB_REPO reporting override;
//   - the parent isn't failing (Ready=False) at any revision — a broken or
//     blocked parent (a malformed manifest, a dependency wait) never prunes
//     the child;
//   - the PR removes the CR from a file under the parent's spec.path and
//     declares it nowhere else (see prRemovesSelf), judged on a file listing
//     below GitHub's cap — per PR;
//   - the parent's applied inventory no longer lists this Kustomization
//     (see parentDroppedChild) — the authoritative check, since only the
//     rendered build knows every transform the file checks approximate.
//
// Only a rate-limit error is returned; every other failure is a "no".
func (p *Processor) selfRemovalExpected(ctx context.Context, owner, repo string, event eventv1.Event, kspec kustomizationSpec, prs []bridgegithub.PRInfo) (string, map[int]bool, error) {
	parentName := kspec.Metadata.Labels[parentNameLabel]
	parentNS := kspec.Metadata.Labels[parentNamespaceLabel]
	if parentName == "" || parentNS == "" || len(prs) == 0 {
		return "", nil, nil
	}
	if pruneExcluded(kspec) {
		return "", nil, nil
	}
	parent := parentNS + "/" + parentName
	parentYAML, err := p.mcp.GetKustomizationStatus(ctx, parentName, parentNS)
	if err != nil {
		p.logger.Info("self-removal check: parent Kustomization unreadable; keeping the normal report",
			"name", event.InvolvedObject.Name, "parent", parent, "error", err.Error())
		return "", nil, nil
	}
	var pspec kustomizationSpec
	if err := yaml.Unmarshal(parentYAML, &pspec); err != nil || !parentCanPrune(pspec) {
		return "", nil, nil
	}
	childURL, err := p.sourceURL(ctx, event.InvolvedObject.Namespace, kspec)
	if err != nil {
		return "", nil, nil
	}
	parentURL, err := p.sourceURL(ctx, parentNS, pspec)
	if err != nil || !sameRepository(childURL, parentURL) {
		return "", nil, nil
	}
	parentPath := bridgesource.CleanPath(pspec.Spec.Path) // "" (source root) → "."
	// Each PR is judged on its own: in a batch, a PR that only touches the
	// child's workloads mustn't hold back the PR that deletes the child.
	removing := map[int]bool{}
	for _, pr := range prs {
		files, err := p.commenter.ListPRFiles(ctx, owner, repo, pr.Number, pr.HeadSHA)
		if err != nil {
			if bridgegithub.IsRateLimit(err) {
				return "", nil, err
			}
			continue
		}
		if len(files) >= prFilesCap {
			continue
		}
		removes, err := p.prRemovesSelf(ctx, owner, repo, pr, files, event.InvolvedObject.Name, parentPath)
		if err != nil {
			return "", nil, err
		}
		if removes {
			removing[pr.Number] = true
		}
	}
	if len(removing) == 0 {
		return "", nil, nil
	}
	if !p.parentDroppedChild(ctx, pspec, parentName, parentNS, event.InvolvedObject.Name, event.InvolvedObject.Namespace, kspec.Metadata.UID) {
		p.logger.Info("self-removal check: parent still lists this Kustomization, or it isn't being deleted; keeping the normal report",
			"name", event.InvolvedObject.Name, "parent", parent)
		return "", nil, nil
	}
	return parent, removing, nil
}

// parentDroppedChild polls, within the readiness budget, until the parent
// Kustomization's status.inventory no longer lists the child AND the child
// itself is being deleted (metadata.deletionTimestamp set) or gone — Flux
// saves the parent's new inventory even when the prune's DELETE fails, so
// inventory absence alone isn't deletion. The inventory is
// what the parent's build actually rendered and applied, so it settles every
// identity transform a file-level check can only guess at (namePrefix,
// nameSuffix, postBuild substitution, namespace rewrites). The parent usually
// reconciles the same push within seconds of the child failing; if it still
// lists the child when the budget runs out (or its inventory can't be read, or
// is empty while the parent isn't Ready), the answer is "no". attributed is
// the parent snapshot the PR's files were judged against: a parent recreated,
// or switched to another source or path since, renders another build, whose
// dropping the child says nothing about this PR.
func (p *Processor) parentDroppedChild(ctx context.Context, attributed kustomizationSpec, parentName, parentNS, childName, childNS, childUID string) bool {
	childID := childNS + "_" + childName + "_" + fluxKustomizationGroupKind
	deadline := time.Now().Add(settleBudget(ctx, p.cfg.ReadinessPollTimeout))
	for {
		if data, err := p.mcp.GetKustomizationStatus(ctx, parentName, parentNS); err == nil {
			// Each snapshot must still pass the parent gates: a parent that
			// turned Ready=False (e.g. PruneFailed), stopped pruning or got
			// suspended while we waited proves nothing by dropping the child.
			var pspec kustomizationSpec
			if yaml.Unmarshal(data, &pspec) != nil || !parentCanPrune(pspec) || !sameParentBuild(attributed, pspec) {
				return false
			}
			// An empty inventory counts only from a Ready parent: pruning its
			// last child empties it, but so can a parent mid-reconcile.
			if refs, err := inventoryRefsFromYAML(data); err == nil && !slices.Contains(refs, childID) &&
				(len(refs) > 0 || kustomizationReadyTrue(data)) && p.childDeleting(ctx, childName, childNS, childUID) {
				return true
			}
		}
		if !time.Now().Before(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(p.cfg.ReadinessPollInterval):
		}
	}
}

// sameParentBuild reports whether two snapshots of a parent Kustomization are
// the same object (uid, when both carry one) building the same source and path.
func sameParentBuild(a, b kustomizationSpec) bool {
	if a.Metadata.UID != "" && b.Metadata.UID != "" && a.Metadata.UID != b.Metadata.UID {
		return false
	}
	return a.Spec.SourceRef == b.Spec.SourceRef &&
		bridgesource.CleanPath(a.Spec.Path) == bridgesource.CleanPath(b.Spec.Path)
}

// parentBuildPath returns the repo path the Kustomization's Flux parent builds
// from ("." for the source root), or "" when there is no readable parent.
func (p *Processor) parentBuildPath(ctx context.Context, kspec kustomizationSpec) string {
	parentName := kspec.Metadata.Labels[parentNameLabel]
	parentNS := kspec.Metadata.Labels[parentNamespaceLabel]
	if parentName == "" || parentNS == "" {
		return ""
	}
	data, err := p.mcp.GetKustomizationStatus(ctx, parentName, parentNS)
	if err != nil {
		return ""
	}
	var pspec kustomizationSpec
	if yaml.Unmarshal(data, &pspec) != nil {
		return ""
	}
	return bridgesource.CleanPath(pspec.Spec.Path)
}

// prEditsOutSelf reports whether the PR may remove the Kustomization name from
// a file under parentPath: a `removed` file declared a Kustomization that
// could render as it (see couldBeSelf), or a `modified` (or `renamed`, read at
// its previous path) one did at the PR base and no longer does at the head.
// Exact identities are already attributed elsewhere; this catches inherited
// namespaces and transformed names. It only
// admits the PR to the self-removal check, which verifies the removal; Process
// gives such a PR no other report. Only rate-limit errors are returned;
// anything unreadable is a "no".
func (p *Processor) prEditsOutSelf(ctx context.Context, owner, repo string, pr bridgegithub.PRInfo, files []bridgegithub.PRFile, name, parentPath string) (bool, error) {
	for _, f := range files {
		basePath := f.Path
		switch f.Status {
		case "removed":
			if !pathWithin(f.Path, parentPath) {
				continue
			}
			ids, err := p.commenter.BlobResourceIDs(ctx, owner, repo, f.BlobSHA)
			if err != nil {
				return false, rateLimitOnly(err)
			}
			if ids, _ = splitParsedIDs(ids); couldBeSelf(ids, name) {
				return true, nil
			}
			continue
		case "modified":
		case "renamed":
			if f.PreviousPath == "" {
				continue
			}
			basePath = f.PreviousPath
		default:
			continue
		}
		// A removed file reads its old content from its blob; an edit needs
		// the PR base to diff against.
		if pr.BaseSHA == "" || !pathWithin(basePath, parentPath) {
			continue
		}
		baseIDs, err := p.commenter.BaseFileResourceIDs(ctx, owner, repo, basePath, pr.BaseSHA)
		if err != nil {
			return false, rateLimitOnly(err)
		}
		if baseIDs, _ = splitParsedIDs(baseIDs); !couldBeSelf(baseIDs, name) {
			continue
		}
		headIDs, err := p.commenter.BlobResourceIDs(ctx, owner, repo, f.BlobSHA)
		if err != nil {
			return false, rateLimitOnly(err)
		}
		if headIDs, _ = splitParsedIDs(headIDs); !couldBeSelf(headIDs, name) {
			return true, nil
		}
	}
	return false, nil
}

// prRemovesSelf reports whether the PR deletes the Kustomization ns/name from
// the parent build rooted at parentPath: a file under parentPath declared its
// CR before the PR (a `removed` file, or a `modified` or `renamed` one at the
// PR base) and no file at the PR head declares it any more (a move is not a
// removal). The parent's build may rewrite identities (namePrefix/nameSuffix,
// postBuild ${VAR}, namespace transformers), so on both sides any Kustomization
// in any namespace whose name is part of this one's, or templated, counts; on
// the removal side parentDroppedChild then confirms the rendered child is gone.
//
// Unlike attribution, this scans every file regardless of build scope, and a
// file it can't read — or can't fully parse, which could hide the CR — makes
// the answer "no". Under parentPath no file is skipped by extension: kustomize
// loads a referenced resource file whatever its name. Only rate-limit errors
// are returned.
func (p *Processor) prRemovesSelf(ctx context.Context, owner, repo string, pr bridgegithub.PRInfo, files []bridgegithub.PRFile, name, parentPath string) (bool, error) {
	removed := false
	for _, f := range files {
		basePath := f.Path
		if f.Status == "renamed" {
			if f.PreviousPath == "" {
				return false, nil // pre-PreviousPath cache entry: can't diff it
			}
			basePath = f.PreviousPath
		}
		inParent := pathWithin(f.Path, parentPath)
		baseInParent := pathWithin(basePath, parentPath)
		if skippableFile(f.Path) && !inParent && !baseInParent {
			continue
		}
		if f.Status == "removed" {
			if !inParent {
				continue
			}
			ids, err := p.commenter.BlobResourceIDs(ctx, owner, repo, f.BlobSHA)
			if err != nil {
				return false, rateLimitOnly(err)
			}
			if ids, _ = splitParsedIDs(ids); couldBeSelf(ids, name) {
				removed = true
			}
			continue
		}
		if f.BlobSHA == "" {
			return false, nil
		}
		ids, err := p.commenter.BlobResourceIDs(ctx, owner, repo, f.BlobSHA)
		if err != nil {
			return false, rateLimitOnly(err)
		}
		ids, complete := splitParsedIDs(ids)
		if !complete || couldBeSelf(ids, name) {
			return false, nil
		}
		if (f.Status != "modified" && f.Status != "renamed") || !baseInParent || pr.BaseSHA == "" {
			continue
		}
		baseIDs, err := p.commenter.BaseFileResourceIDs(ctx, owner, repo, basePath, pr.BaseSHA)
		if err != nil {
			return false, rateLimitOnly(err)
		}
		if baseIDs, _ = splitParsedIDs(baseIDs); couldBeSelf(baseIDs, name) {
			removed = true
		}
	}
	return removed, nil
}

// parentCanPrune reports whether a parent Kustomization snapshot will prune a
// removed child: spec.prune on, not suspended (a suspended parent keeps its
// last Ready but never reconciles), and not Ready=False at any revision (a
// broken or blocked parent — a malformed manifest, a dependency wait, a failed
// prune — prunes nothing). An empty analysed revision makes
// reconcileFailureMessage read Ready whatever revision it describes.
func parentCanPrune(k kustomizationSpec) bool {
	if !k.Spec.Prune || k.Spec.Suspend {
		return false
	}
	failed, waiting := reconcileFailureMessage(eventv1.Event{}, k, "")
	return failed == "" && waiting == ""
}

// finalizerFailureMessages are the error events kustomize-controller's
// finalizer emits when it can't clean up a deleted Kustomization (v1.8.4).
var finalizerFailureMessages = []string{
	retryableFinalizerFailure,
	"failed to wait for resources termination",
	"unable to prune objects",
}

// retryableFinalizerFailure keeps the finalizer and is retried: once the
// child is gone, the cleanup it reported has since succeeded. The other two
// let the finalizer go regardless, so they count even after the child is gone.
const retryableFinalizerFailure = "pruning for deleted resource failed"

// childDeleting reports whether the child Kustomization is gone, or being
// deleted (metadata.deletionTimestamp set), without a failure that blocks or
// skips its cleanup: a terminating child's fresh Ready must not be False for a
// reason other than the teardown itself (see teardownFailure), and — gone or
// terminating — it must have no finalizer failure event (v1.8.4 drops the
// finalizer even after "unable to prune objects", leaving resources behind).
// A read error other than "not found" is no evidence. MCP's "No resources
// found" is ambiguous — a degraded server (cold discovery cache) says the same
// about an object that exists — so it counts as gone only when a list of the
// namespace's Kustomizations succeeds and lacks the child; for the Event list
// it proves nothing either, so the check fails closed.
// uid (from the first snapshot) is the object the PR was attributed against:
// a live object with another uid is a replacement under the same name, whose
// deletion says nothing about it; events of an earlier object are skipped.
// Events or a uid that don't carry one still count.
func (p *Processor) childDeleting(ctx context.Context, name, namespace, uid string) bool {
	data, err := p.mcp.GetKustomizationStatus(ctx, name, namespace)
	gone := err != nil
	switch {
	case gone:
		msg := strings.ToLower(err.Error())
		switch {
		case strings.Contains(msg, "not found"):
		case strings.Contains(msg, "no resources found"):
			if !p.kustomizationListedAbsent(ctx, name, namespace) {
				return false
			}
		default:
			return false
		}
	default:
		var k kustomizationSpec
		if yaml.Unmarshal(data, &k) != nil || k.Metadata.DeletionTimestamp == "" {
			return false
		}
		if uid != "" && k.Metadata.UID != "" && k.Metadata.UID != uid {
			return false
		}
		if failed, waiting := reconcileFailureMessage(eventv1.Event{}, k, ""); (failed != "" || waiting != "") && !teardownFailure(failed, waiting) {
			return false
		}
	}
	body, err := p.mcp.ListKubernetesResources(ctx, "v1", "Event", namespace)
	if err != nil {
		return false
	}
	for _, e := range parseEventList(body) {
		if e.InvolvedObject.Name != name || (e.InvolvedObject.Kind != "" && e.InvolvedObject.Kind != "Kustomization") ||
			(uid != "" && e.InvolvedObject.UID != "" && e.InvolvedObject.UID != uid) {
			continue
		}
		for _, m := range finalizerFailureMessages {
			if strings.Contains(e.Message, m) && (!gone || m != retryableFinalizerFailure) {
				return false
			}
		}
	}
	return true
}

// kustomizationListedAbsent reports whether a list of the namespace's
// Kustomizations succeeds, holds at least one, and doesn't hold name — a
// served answer that the named object is gone. An empty or failed list proves
// nothing.
func (p *Processor) kustomizationListedAbsent(ctx context.Context, name, namespace string) bool {
	body, err := p.mcp.ListKubernetesResources(ctx, "kustomize.toolkit.fluxcd.io/v1", "Kustomization", namespace)
	if err != nil {
		return false
	}
	type object struct {
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
	}
	// A `kind: List` wrapper or a bare multi-doc stream, like the other lists.
	var list struct {
		Items []object `yaml:"items"`
	}
	if yaml.Unmarshal(body, &list) != nil || len(list.Items) == 0 {
		list.Items = nil
		dec := yaml.NewDecoder(bytes.NewReader(body))
		for {
			var o object
			if err := dec.Decode(&o); err != nil {
				if !errors.Is(err, io.EOF) {
					return false
				}
				break
			}
			list.Items = append(list.Items, o)
		}
	}
	if len(list.Items) == 0 {
		return false
	}
	for _, it := range list.Items {
		if it.Metadata.Name == name || it.Metadata.Name == "" {
			return false
		}
	}
	return true
}

// deletesResourcesOnDeletion reports whether deleting the Kustomization also
// deletes the objects it manages, per spec.deletionPolicy: MirrorPrune (the
// default) follows spec.prune, Delete and WaitForTermination always delete,
// Orphan never does. A suspended Kustomization's finalizer deletes nothing.
func deletesResourcesOnDeletion(k kustomizationSpec) bool {
	if k.Spec.Suspend {
		return false
	}
	switch k.Spec.DeletionPolicy {
	case "Delete", "WaitForTermination":
		return true
	case "Orphan":
		return false
	default:
		return k.Spec.Prune
	}
}

// pruneExcluded reports whether kustomize-controller skips this object when
// pruning: the prune or reconcile key set to "disabled" (any case) as a label
// or an annotation.
func pruneExcluded(k kustomizationSpec) bool {
	for _, m := range []map[string]string{k.Metadata.Labels, k.Metadata.Annotations} {
		for _, key := range []string{pruneKey, reconcileKey} {
			if strings.EqualFold(m[key], "disabled") {
				return true
			}
		}
	}
	return false
}

// sameRepository reports whether two source URLs name the same repository:
// the same GitHub owner/repo whatever the URL form (https, ssh, .git), else
// the same URL ignoring case, a trailing slash and ".git".
func sameRepository(a, b string) bool {
	ao, ar, aerr := parseGitHubRepoURL(a)
	bo, br, berr := parseGitHubRepoURL(b)
	if aerr == nil && berr == nil {
		return strings.EqualFold(ao, bo) && strings.EqualFold(ar, br)
	}
	norm := func(u string) string {
		return strings.TrimSuffix(strings.TrimSuffix(strings.ToLower(strings.TrimSpace(u)), "/"), ".git")
	}
	return norm(a) == norm(b)
}

// couldBeSelf reports whether ids hold a Flux Kustomization, in any
// namespace, that a parent build could render as the Kustomization name: its
// name is part of name (namePrefix/nameSuffix) or templated (a postBuild
// ${VAR}). The split between namespace and name is ambiguous once a templated
// value holds "_", so every split is tried.
func couldBeSelf(ids []string, name string) bool {
	for _, id := range ids {
		rest, ok := strings.CutSuffix(id, "_"+fluxKustomizationGroupKind)
		if !ok {
			continue
		}
		for i := range len(rest) {
			if rest[i] != '_' {
				continue
			}
			if idName := rest[i+1:]; idName != "" && (strings.Contains(name, idName) || strings.Contains(idName, "${")) {
				return true
			}
		}
	}
	return false
}

// withoutPRs returns prs minus those in drop.
func withoutPRs(prs []bridgegithub.PRInfo, drop map[int]bool) []bridgegithub.PRInfo {
	var out []bridgegithub.PRInfo
	for _, pr := range prs {
		if !drop[pr.Number] {
			out = append(out, pr)
		}
	}
	return out
}

// onlyPRs returns the prs in keep.
func onlyPRs(prs []bridgegithub.PRInfo, keep map[int]bool) []bridgegithub.PRInfo {
	var out []bridgegithub.PRInfo
	for _, pr := range prs {
		if keep[pr.Number] {
			out = append(out, pr)
		}
	}
	return out
}

// pathWithin reports whether the repo file path lies under dir (both
// repo-relative; "." is the repo root).
func pathWithin(path, dir string) bool {
	path, dir = bridgesource.CleanPath(path), bridgesource.CleanPath(dir)
	return dir == "." || strings.HasPrefix(path, dir+"/")
}

// rateLimitOnly passes a GitHub rate-limit error through and drops any other.
func rateLimitOnly(err error) error {
	if bridgegithub.IsRateLimit(err) {
		return err
	}
	return nil
}

// buildSelfRemovalBody is the comment body for a Kustomization whose own CR
// the PR deletes (see the self-removal branch in Process): the reconcile
// failure is named, then explained as expected teardown.
func buildSelfRemovalBody(name, parent, reconcileFailedMsg, reconcileWaitingMsg string, deletesResources bool) string {
	var b strings.Builder
	b.WriteString(statusEmojiHealthy + "\n\n")
	fmt.Fprintf(&b, "🗑️ Kustomization `%s` is being removed by this PR.\n\n", name)
	if reconcileFailedMsg != "" {
		fmt.Fprintf(&b, "Its last reconcile failed: %s\n\n", summaryFailureText(reconcileFailedMsg))
	} else {
		fmt.Fprintf(&b, "Its last reconcile is waiting on a dependency: %s\n\n", summaryFailureText(reconcileWaitingMsg))
	}
	fmt.Fprintf(&b, "This is expected. The PR deletes this Kustomization's definition from its parent Kustomization `%s`, "+
		"which prunes, so the parent deletes this Kustomization. ", parent)
	if deletesResources {
		b.WriteString("Its deletion also prunes the resources it manages, except those excluded from pruning. ")
	} else {
		b.WriteString("The resources it manages stay on the cluster (its `spec.prune`, `spec.deletionPolicy` or `spec.suspend` keeps them). ")
	}
	b.WriteString("Until then it reconciles a revision that no longer has its files.\n\n")
	b.WriteString("No action needed. If it is still on the cluster after the parent's next reconcile, check the parent's status.")
	return b.String()
}
