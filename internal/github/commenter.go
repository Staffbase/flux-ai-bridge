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

// Package github wraps the go-github client with GitHub App authentication
// and exposes only the methods the processor needs. PAT auth was removed
// in favour of App-only: bridge installs are expected to authenticate as
// an installed GitHub App (so they get bounded permissions, no per-user
// PAT rotation, and bot-style comment attribution).
package github

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bradleyfalzon/ghinstallation/v2"
	gh "github.com/google/go-github/v62/github"
	"github.com/staffbase/flux-ai-bridge/internal/fetchcache"
	"github.com/staffbase/flux-ai-bridge/internal/lifecycle"
	"github.com/staffbase/flux-ai-bridge/internal/markdown"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
	"golang.org/x/sync/singleflight"
)

const (
	// commentMarkerPrefix is the leading token shared by every flux-ai-bridge
	// comment. Bridge comments are now scoped per Kustomization
	// (`<!-- flux-ai-bridge:<namespace>/<name> -->`) so a PR that touches
	// multiple Kustomizations gets one sticky comment per Kustomization
	// instead of having later analyses delete earlier ones. Comment matching
	// is done on the FULL scoped marker (strings.Contains against
	// commentMarkerFor's output), so legacy unscoped comments
	// (`<!-- flux-ai-bridge -->`) posted by previous bridge versions are
	// deliberately left untouched — preserved like any other non-matching
	// comment (pinned by TestPostOrUpdateComment_PreservesOtherKustomizationMarkers).
	commentMarkerPrefix = "<!-- flux-ai-bridge"
	requestTimeout      = 30 * time.Second
)

// commentMarkerFor returns the marker for a specific Kustomization. Empty
// namespace+name (callers that haven't migrated yet) fall back to the
// legacy unscoped marker — kept only so the constant is reachable from a
// single helper.
func commentMarkerFor(namespace, name string) string {
	if namespace == "" && name == "" {
		return commentMarkerPrefix + " -->"
	}
	return commentMarkerPrefix + ":" + namespace + "/" + name + " -->"
}

// markerFor returns the hidden marker that identifies this bridge's comment
// for a Kustomization. With an instance ID it names the instance as well
// (`<!-- flux-ai-bridge@<instance>:<ns>/<name> -->`), so bridges in different
// clusters that report on the same repo — and may well have a same-named
// Kustomization — never claim, swap or delete each other's comments.
func (c *Commenter) markerFor(namespace, name string) string {
	if c.instanceID == "" {
		return commentMarkerFor(namespace, name)
	}
	return commentMarkerPrefix + "@" + c.instanceID + ":" + namespace + "/" + name + " -->"
}

// keyMarkerPrefix opens the hidden stability-key marker embedded in every
// delivered comment (`<!-- flux-ai-bridge-key:<sha256-hex> -->`). It lets a
// cold cache recover the key the comment was posted under — see
// PostOrUpdateCommentKeyed.
const keyMarkerPrefix = "<!-- flux-ai-bridge-key:"

// commentKeyOf extracts the embedded stability hash from a comment body, or
// "" when absent (legacy comments posted before the marker existed, or
// foreign content).
func commentKeyOf(body string) string {
	start := strings.Index(body, keyMarkerPrefix)
	if start < 0 {
		return ""
	}
	rest := body[start+len(keyMarkerPrefix):]
	end := strings.Index(rest, " -->")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

type Commenter struct {
	client *gh.Client
	// keepHistory, when true, makes PostOrUpdateComment skip the delete-prior
	// step and just create a fresh comment. Useful for testing (you want to
	// see the sequence of analyses as a commit toggles between healthy and
	// broken) and for audit trails. Default false preserves the
	// delete-and-recreate idempotency story.
	keepHistory bool
	// instanceID scopes the comment marker; see markerFor.
	instanceID string
	// cache holds the fetchcache backing the read-heavy GitHub lookups
	// (blob content, commit metadata, listPRsForCommit). Disabled() by
	// default so any zero-value Commenter (in tests, or constructed
	// without a cache) silently degrades to "always fetch."
	cache fetchcache.Cache
	// parentWalkDepth caps walkMergeParents recursion. Autodev batches
	// regularly exceed the previous hardcoded 10; configurable so deep
	// batches don't drop PRs from the resolved set.
	parentWalkDepth int
	// prsForCommitTTL bounds how stale the PR-list-for-commit cache can
	// be. The PR NUMBERS are stable but state (open/closed) and labels
	// mutate; a short TTL trades a small staleness window for far fewer
	// API calls during autodev walks where the same parent SHAs are
	// queried repeatedly across consecutive events.
	prsForCommitTTL time.Duration
	// blobParser extracts the declared Kubernetes resource IDs from a PR
	// file's raw bytes. Injected (rather than imported) to avoid a github →
	// processor dependency. BlobResourceIDs caches its OUTPUT, so the raw
	// manifest bytes never enter the fetchcache/Redis. nil → BlobResourceIDs
	// errors (a misconfiguration; main.go always wires it).
	blobParser func([]byte) []string
	// commitTTL bounds the otherwise-immutable commit-parents cache so the
	// no-expiry entries don't grow unbounded in Redis across a busy branch's
	// merge-parent walks. 0 = no expiry.
	commitTTL time.Duration
	// labelSuccess / labelFailure / labelWarning are the PR label names
	// reconciled by SetPRLabels. Configurable so operators can match an existing
	// merge-gate convention; default to DefaultPRLabelSuccess /
	// DefaultPRLabelFailure / DefaultPRLabelWarning. Setting two of them to the
	// SAME string is supported (e.g. labelWarning == labelFailure to make a
	// warning block a merge gate the same way a failure does): SetPRLabels never
	// removes a name it is also adding, so colliding names don't churn.
	labelSuccess string
	labelFailure string
	labelWarning string
	// postLocks serializes PostOrUpdateComment per
	// (kustomizationNamespace, kustomizationName) scope. Two goroutines
	// processing different SHAs of the SAME Kustomization would otherwise
	// both list-and-delete before either creates — they'd each see the
	// post-list state as "no prior bridge comment" and both create,
	// leaving duplicates on the PR. Keyed on Kustomization scope rather
	// than PR because the marker is scope-keyed: distinct Kustomizations
	// posting to the same PR have distinct markers and don't race on the
	// delete step.
	//
	// Map grows once per unique (ns, name) and is never cleaned — bounded
	// by the number of Kustomizations in the cluster (typically < 1000),
	// not by event volume. Not a memory concern.
	postLocksMu sync.Mutex
	postLocks   map[string]*sync.Mutex
	// labelLocks serializes SetPRLabels per (owner, repo, PR). Two events
	// (different Kustomizations on the same PR) reconciling concurrently
	// would otherwise interleave their add/remove calls — A adds success,
	// B adds failure, A removes failure, B removes success — leaving the
	// PR with NO outcome label, while both then cache their outcome so the
	// transition-only short-circuit never re-asserts it. Serializing makes
	// each reconcile atomic per PR within this replica. Replicas are NOT
	// serialized against each other: the same interleaving across two
	// replicas can leave the PR without an outcome label, and the shared
	// record then skips the repair until the outcome or the head changes.
	//
	// STRIPED, not per-key: unlike postLocks (bounded by the cluster's
	// Kustomization count), PR numbers grow without bound over a deployment's
	// lifetime, so a grow-only map would be a slow leak. The key hashes to
	// one of a fixed set of mutexes — constant memory; two different PRs
	// landing on the same stripe merely serialize against each other, which
	// is harmless (SetPRLabels is a handful of fast GitHub calls).
	labelLocks [64]sync.Mutex
	// prsForCommitSF coalesces concurrent FindPRsForCommit resolutions for
	// the same (owner, repo, sha). When a new source revision drops, the
	// notification-controller fires one event per Kustomization within a
	// few seconds — before any of them has populated the prs-walk cache.
	// Without coalescing, up to WEBHOOK_MAX_CONCURRENT of them each run the
	// full direct-lookup + merge-parent walk (the "thundering herd"); with
	// it, the first runs the walk and the rest wait for and reuse its
	// result. This is the dominant per-revision GitHub-call cost as the
	// bridge widens beyond a single Kustomization, so it's always on (no
	// knob): it has no downside worth tuning. It's in-process per replica;
	// the (Redis) prs-walk cache handles cross-replica + sequential reuse.
	prsForCommitSF singleflight.Group
}

// Options configures Commenter behavior. Auth lives in AuthConfig; this
// struct holds runtime knobs that don't touch credentials.
type Options struct {
	// KeepCommentHistory disables the delete-prior-marker-comments step.
	// Each analysis is posted as a new comment; the PR accumulates a trail.
	KeepCommentHistory bool
	// Cache backs the read-heavy GitHub lookups (blob content, commit
	// metadata, PRs-for-commit). Nil falls back to fetchcache.Disabled()
	// — the commenter still works but pays a fresh API call on every
	// lookup. Production deployments wire either an in-memory LRU or a
	// Redis-backed instance from cmd/main.go.
	Cache fetchcache.Cache
	// ParentWalkDepth caps the merge-parent recursion in
	// FindPRsForCommit. Default 200 (defaultParentWalkDepth) covers autodev
	// batches with up to 200 PRs merged into one dev-branch push. Bumping
	// this is cheap when
	// Cache is on (subsequent events hit cached commit metadata); raise
	// it for organizations with deeper autodev batches.
	ParentWalkDepth int
	// PRsForCommitTTL is the cache TTL for the PR-list-per-commit-SHA
	// lookup. 0 = default 5m (defaultPRsForCommitTTL). The PR NUMBERS are
	// stable; state mutates. A negative value disables caching for this
	// surface specifically (the commit-metadata + blob-refs caches stay on).
	PRsForCommitTTL time.Duration
	// BlobResourceParser extracts the declared Kubernetes resource IDs from a
	// PR file's raw bytes (main.go wires processor.ResourceIDsFromYAML). It's
	// what lets BlobResourceIDs cache the small, non-sensitive ID list instead
	// of the raw manifest. Required for BlobResourceIDs to function.
	BlobResourceParser func([]byte) []string
	// CommitTTL bounds the commit-parents cache. Commit metadata is immutable
	// so this only caps unbounded growth (the merge-parent walk caches every
	// ancestor it touches). Default 1h when a cache is set; negative disables
	// the bound (no expiry).
	CommitTTL time.Duration
	// PRLabelSuccess / PRLabelFailure / PRLabelWarning override the outcome
	// label names SetPRLabels reconciles. Empty → DefaultPRLabelSuccess /
	// DefaultPRLabelFailure / DefaultPRLabelWarning.
	PRLabelSuccess string
	PRLabelFailure string
	PRLabelWarning string
	// InstanceID, when set, is written into every comment marker so bridges
	// in different clusters reporting on the same repo keep separate
	// comments (see markerFor). Empty keeps the plain marker.
	InstanceID string
}

// AuthConfig collects the GitHub App credentials. Exactly one of
// AppPrivateKeyPEM or AppPrivateKeyFile must be set; the other two ID
// fields are always required.
type AuthConfig struct {
	AppID             int64  // GitHub App ID (numeric)
	AppPrivateKeyFile string // Path to the App's PEM private key
	AppPrivateKeyPEM  string // PEM contents inlined directly
	BaseURL           string // GitHub Enterprise / mock — empty = api.github.com
	InstallationID    int64  // Installation ID for the target org/user
}

// AuthFromEnv reads the standard GitHub App env vars used by main.go.
// Returns a config the caller hands to NewCommenter. githubAPIURL routes
// requests at GitHub Enterprise / mock-github when non-empty.
func AuthFromEnv(githubAPIURL string) (AuthConfig, error) {
	cfg := AuthConfig{BaseURL: githubAPIURL}
	if v := os.Getenv("GITHUB_APP_ID"); v != "" {
		id, err := parseInt64(v)
		if err != nil {
			return cfg, fmt.Errorf("GITHUB_APP_ID: %w", err)
		}
		cfg.AppID = id
	}
	if v := os.Getenv("GITHUB_APP_INSTALLATION_ID"); v != "" {
		id, err := parseInt64(v)
		if err != nil {
			return cfg, fmt.Errorf("GITHUB_APP_INSTALLATION_ID: %w", err)
		}
		cfg.InstallationID = id
	}
	cfg.AppPrivateKeyPEM = os.Getenv("GITHUB_APP_PRIVATE_KEY")
	cfg.AppPrivateKeyFile = os.Getenv("GITHUB_APP_PRIVATE_KEY_FILE")
	return cfg, nil
}

// validate ensures all required App fields are present.
func (c AuthConfig) validate() error {
	if c.AppID == 0 || c.InstallationID == 0 || (c.AppPrivateKeyPEM == "" && c.AppPrivateKeyFile == "") {
		return fmt.Errorf("GitHub App config incomplete: need GITHUB_APP_ID + GITHUB_APP_INSTALLATION_ID + GITHUB_APP_PRIVATE_KEY (or _FILE)")
	}
	return nil
}

// NewCommenter constructs a Commenter using GitHub App auth. The PEM is
// parsed eagerly by ghinstallation, so misconfigured keys fail at startup
// rather than at first API call.
func NewCommenter(cfg AuthConfig, opts Options) (*Commenter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	tr, err := newAppTransport(cfg)
	if err != nil {
		return nil, fmt.Errorf("github app transport: %w", err)
	}
	httpClient := &http.Client{Transport: tr, Timeout: requestTimeout, CheckRedirect: sameOriginRedirect}
	ghClient := gh.NewClient(httpClient)
	if cfg.BaseURL != "" {
		var err error
		ghClient, err = ghClient.WithEnterpriseURLs(cfg.BaseURL, cfg.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("github client with enterprise urls: %w", err)
		}
		// ghinstallation's installation-token refresh goes to api.github.com
		// by default. Point it at the SAME normalized REST root go-github
		// uses (WithEnterpriseURLs appends /api/v3/ for a bare GHE host) —
		// real GHE serves /app/installations/{id}/access_tokens under
		// /api/v3 too, so a raw host here would 404 token issuance while
		// REST calls succeed.
		tr.BaseURL = strings.TrimSuffix(ghClient.BaseURL.String(), "/")
	}
	cache := opts.Cache
	if cache == nil {
		cache = fetchcache.Disabled()
	}
	depth := opts.ParentWalkDepth
	if depth <= 0 {
		depth = defaultParentWalkDepth
	}
	prsTTL := opts.PRsForCommitTTL
	if prsTTL < 0 {
		prsTTL = 0
	} else if prsTTL == 0 && opts.Cache != nil {
		// Caller provided a cache but didn't pick a TTL — apply the
		// default so they get sensible cache behavior out of the box.
		// Explicit `PRsForCommitTTL: -1` (or a negative value) disables
		// PRs-for-commit caching while leaving blob+commit caches on.
		prsTTL = defaultPRsForCommitTTL
	}
	commitTTL := opts.CommitTTL
	if commitTTL < 0 {
		commitTTL = 0
	} else if commitTTL == 0 && opts.Cache != nil {
		commitTTL = defaultCommitTTL
	}
	labelSuccess := opts.PRLabelSuccess
	if labelSuccess == "" {
		labelSuccess = DefaultPRLabelSuccess
	}
	labelFailure := opts.PRLabelFailure
	if labelFailure == "" {
		labelFailure = DefaultPRLabelFailure
	}
	labelWarning := opts.PRLabelWarning
	if labelWarning == "" {
		labelWarning = DefaultPRLabelWarning
	}
	return &Commenter{
		client:          ghClient,
		keepHistory:     opts.KeepCommentHistory,
		instanceID:      opts.InstanceID,
		cache:           cache,
		parentWalkDepth: depth,
		prsForCommitTTL: prsTTL,
		blobParser:      opts.BlobResourceParser,
		commitTTL:       commitTTL,
		labelSuccess:    labelSuccess,
		labelFailure:    labelFailure,
		labelWarning:    labelWarning,
		postLocks:       make(map[string]*sync.Mutex),
	}, nil
}

// maxRedirects matches net/http's default redirect limit.
const maxRedirects = 10

// sameOriginRedirect refuses a redirect that leaves the origin (scheme, host
// and port) of the original request. net/http drops the Authorization header
// on such a hop, but ghinstallation sets it again in RoundTrip for every
// request, so following the redirect would hand the installation token to
// the other origin.
func sameOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	orig := via[0].URL
	if req.URL.Scheme != orig.Scheme || req.URL.Host != orig.Host {
		return fmt.Errorf("refusing redirect from %s://%s to another origin %s://%s",
			orig.Scheme, orig.Host, req.URL.Scheme, req.URL.Host)
	}
	return nil
}

// lockForScope returns the per-(namespace, name) mutex used to serialize
// PostOrUpdateComment. Lazily initialized so we don't pre-allocate a mutex
// for every theoretical Kustomization scope.
func (c *Commenter) lockForScope(namespace, name string) *sync.Mutex {
	key := namespace + "/" + name
	c.postLocksMu.Lock()
	defer c.postLocksMu.Unlock()
	m, ok := c.postLocks[key]
	if !ok {
		m = &sync.Mutex{}
		c.postLocks[key] = m
	}
	return m
}

// lockForPRLabels returns the stripe mutex serializing SetPRLabels for an
// (owner, repo, PR). Same PR always maps to the same stripe; see the
// labelLocks field doc for why this is striped rather than per-key.
func (c *Commenter) lockForPRLabels(owner, repo string, prNumber int) *sync.Mutex {
	h := fnv.New32a()
	// hash.Hash.Write never returns an error; discard explicitly for errcheck.
	_, _ = fmt.Fprintf(h, "%s/%s#%d", owner, repo, prNumber)
	return &c.labelLocks[h.Sum32()%uint32(len(c.labelLocks))]
}

// countingTransport wraps an http.RoundTripper to count every request sent
// to GitHub. It sits at the BASE of the ghinstallation transport (rather than
// wrapping it) so it observes not just the high-level API calls but also the
// App installation-token refreshes ghinstallation issues internally — i.e.
// every actual HTTP request to GitHub, which is what the rate limit counts.
type countingTransport struct {
	base http.RoundTripper
}

func (t *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	status := "error" // transport-level failure: no response (DNS, reset, timeout)
	if resp != nil {
		status = strconv.Itoa(resp.StatusCode)
	}
	metrics.GitHubAPIRequests.WithLabelValues(req.Method, status).Inc()
	return resp, err
}

func newAppTransport(cfg AuthConfig) (*ghinstallation.Transport, error) {
	var (
		tr  *ghinstallation.Transport
		err error
	)
	base := &countingTransport{base: http.DefaultTransport}
	if cfg.AppPrivateKeyFile != "" {
		tr, err = ghinstallation.NewKeyFromFile(base, cfg.AppID, cfg.InstallationID, cfg.AppPrivateKeyFile)
	} else {
		tr, err = ghinstallation.New(base, cfg.AppID, cfg.InstallationID, []byte(cfg.AppPrivateKeyPEM))
	}
	if err != nil {
		return nil, err
	}
	return tr, nil
}

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

// recordRateLimit pulls the rate-limit headers from a go-github response and
// classifies errors into two buckets so quota alerts don't false-positive on
// auth issues (and vice versa):
//
//   - flux_ai_bridge_github_rate_limit_hits_total — bumped when err is a
//     *github.RateLimitError (primary) or *github.AbuseRateLimitError
//     (secondary / abuse).
//   - flux_ai_bridge_github_auth_failures_total — bumped on any other
//     401/403 (revoked PAT, uninstalled App, etc.). go-github surfaces
//     401/403 as a *github.ErrorResponse — err is non-nil but represents
//     the authoritative auth failure, so we count it.
//
// Called from every API method so the remaining-quota gauge reflects the
// most recent observation, regardless of error.
func recordRateLimit(resp *gh.Response, err error) {
	if resp == nil {
		// Some go-github errors (e.g. transport-level) come back with a nil
		// Response; we still want to classify the error type if possible.
		if err != nil && isRateLimitError(err) {
			metrics.GitHubRateLimitHits.Inc()
		}
		return
	}
	// Only responses that carry rate-limit headers update the gauge; go-github
	// leaves Rate zeroed otherwise (e.g. a 502 from GitHub's edge), and
	// recording that 0 fires a false "quota exhausted" alert.
	if resp.Rate.Limit > 0 {
		metrics.GitHubRateLimitRemaining.Set(float64(resp.Rate.Remaining))
	}
	if err != nil && isRateLimitError(err) {
		metrics.GitHubRateLimitHits.Inc()
		return
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		metrics.GitHubAuthFailures.Inc()
	}
}

// isRateLimitError matches go-github's two typed rate-limit errors. Both
// are unwrap-safe with errors.As, so wrapped errors from callers still
// classify correctly.
func isRateLimitError(err error) bool {
	var rateLimitErr *gh.RateLimitError
	var abuseLimitErr *gh.AbuseRateLimitError
	return errors.As(err, &rateLimitErr) || errors.As(err, &abuseLimitErr)
}

// IsRateLimit reports whether err is a GitHub primary or secondary rate-limit
// error, even when wrapped through several layers of `fmt.Errorf("...: %w", …)`.
// Exposed so callers (the webhook handler, the processor) can short-circuit
// without retrying — re-firing the same SHA at a rate-limited GitHub only
// extends the cooldown. The processor holds the dedupe entry for
// RateLimitCooldown(err) so Flux's reconcile-interval re-fires don't keep
// hammering, then lets the next event retry.
func IsRateLimit(err error) bool {
	return isRateLimitError(err)
}

// Rate-limit cooldown bounds. Secondary limits without a Retry-After ask
// clients to wait "at least a minute" and back off further on repeats.
const (
	minRateLimitCooldown     = time.Minute
	maxRateLimitCooldown     = time.Hour
	defaultRateLimitCooldown = 10 * time.Minute
)

// RateLimitCooldown returns how long to stop retrying after err: the
// secondary limit's Retry-After, or the time until the primary limit resets,
// clamped to [1m, 1h]; 10m when GitHub gave no hint. 0 if err is not a rate
// limit.
func RateLimitCooldown(err error) time.Duration {
	var d time.Duration
	var rateLimitErr *gh.RateLimitError
	var abuseLimitErr *gh.AbuseRateLimitError
	switch {
	case errors.As(err, &abuseLimitErr):
		if abuseLimitErr.RetryAfter != nil {
			d = *abuseLimitErr.RetryAfter
		}
	case errors.As(err, &rateLimitErr):
		if !rateLimitErr.Rate.Reset.IsZero() {
			d = time.Until(rateLimitErr.Rate.Reset.Time)
		}
	default:
		return 0
	}
	if d <= 0 {
		d = defaultRateLimitCooldown
	}
	return min(max(d, minRateLimitCooldown), maxRateLimitCooldown)
}

// PRInfo is the minimum the bridge needs to know about a found pull request.
// Labels carries the PR's GitHub labels so the processor can apply the
// required-labels gate (config.prFilter.requiredLabels) without a second
// round-trip.
type PRInfo struct {
	Labels []string
	Merged bool
	Number int
	State  string // "open" or "closed"
	// HeadSHA is the PR's own head commit. It is the dedup identity for
	// per-PR analysis: a PR is analysed once per HeadSHA, so unrelated
	// advances of the Flux source branch don't trigger re-analysis. Empty
	// when the GitHub API didn't surface a head (shouldn't happen for the
	// list-by-commit path, which always returns full PR objects).
	HeadSHA string
	// BaseSHA is the commit the PR targets (its base branch tip). Used to
	// fetch a changed file's BEFORE-state so an in-place resource removal (a
	// manifest commented out / a YAML doc deleted from a still-present file)
	// can be diffed against the head version. Empty when the API didn't
	// surface a base.
	BaseSHA string
}

// defaultParentWalkDepth caps the parent chain when no override is set.
// Sized for production autodev (https://github.com/Staffbase/autodev-action)
// — batches of 50+ PRs merged into a dev branch in one push are common, and at any cap
// below the actual batch size the bridge silently drops the older PRs
// from the resolved set. Sized generously at 200 so unusually deep
// batches still resolve; raising the cap is cheap with the commit-
// metadata cache wired up (each event walks at most depth-many NEW
// hops, and all already-walked ancestors are cache hits forever).
// Operators can tune via PARENT_WALK_DEPTH env if their workflow
// exceeds 200. The walk normally stops earlier, at the merge base with the
// default branch; the cap matters when that can't be determined.
const defaultParentWalkDepth = 200

// defaultPRsForCommitTTL bounds staleness on the PR-lookup caches
// (prs-for-commit + prs-walk). The PR↔commit association is immutable; the
// ONLY thing this TTL keeps fresh is each PR's mutable GitHub state
// (open/closed/merged) and labels — which change outside the Flux event
// stream, so the bridge can only learn them by re-querying.
//
// 5 minutes is the default because in the out-of-the-box config there is no
// required-label gate and no label writing, so only PR state matters — and
// per-PR-head dedup + idempotent comment delivery already cushion stale state
// (a merged PR the bridge already commented on won't be re-touched). A
// multi-minute TTL therefore cuts GitHub calls substantially on busy repos
// and clusters where many Kustomizations reconcile the same dev revision,
// while keeping merge/close detection reasonably prompt.
//
// Operators who DO use PR_REQUIRED_LABELS should LOWER this (e.g. 60s): the
// label gate decides whether to comment, and a higher TTL delays noticing a
// freshly-added label by up to the TTL.
const defaultPRsForCommitTTL = 5 * time.Minute

// defaultCommitTTL bounds the commit-parents cache. The data is immutable, so
// this is purely a growth cap: the merge-parent walk caches every ancestor it
// touches, and on a busy branch that would otherwise accumulate forever in
// Redis. 1h is far longer than an autodev batch window (the walk re-hits the
// same ancestors within minutes, so hit rate stays high) yet bounds the
// working set to roughly one hour of distinct commits.
const defaultCommitTTL = time.Hour

// FindPRsForCommit returns every pull request the bridge should comment on
// for a given commit SHA. Two layers:
//
//  1. Direct: GET /commits/{sha}/pulls — finds PRs whose head branch contains
//     the SHA (covers fast-forward merges + still-open feature branches).
//  2. Parent walk: if the commit is a merge commit with N parents,
//     parents[1..N-1] are the "merged-in" branch tips. We probe
//     each via the same /commits/.../pulls endpoint to find the open PR.
//     Then we recurse along parents[0] up to c.parentWalkDepth so a single
//     autodev batch (sequential merge commits) reveals every involved PR,
//     stopping early at the merge base with the default branch (history
//     already merged there only yields merged PRs).
//
// This is required for workflows where a CI action merges PRs into a
// non-default branch (autodev-action style,
// https://github.com/Staffbase/autodev-action): the merge commit
// itself doesn't appear on any PR's head, so the direct API returns empty
// even though there's clearly an open PR to comment on.
func (c *Commenter) FindPRsForCommit(ctx context.Context, owner, repo, sha string) ([]PRInfo, error) {
	if sha == "" {
		return nil, nil
	}
	// Whole-resolution cache, keyed only by (owner, repo, sha) — the result
	// doesn't depend on the Kustomization, so every event for the same source
	// revision (all Kustomizations, all re-fires) shares one entry. This sits
	// ABOVE the per-commit listPRsForCommit/getCommitParents caches: a hit
	// skips the entire direct-lookup + parent-walk traversal. It also survives
	// in-memory LRU pressure better than the dozens of per-commit entries a
	// deep walk produces (one durable key vs many evictable ones), which
	// matters when the bridge tracks many Kustomizations off one busy branch.
	// Same short TTL as the PR-lookup cache (the result carries mutable PR
	// state/labels), including the empty case (negative cache).
	cacheKey := "prs-walk:" + owner + "/" + repo + "/" + sha
	if cached, hit := c.cachedPRsForCommit(ctx, cacheKey); hit {
		metrics.FetchCacheHits.WithLabelValues("prs_walk").Inc()
		return cached, nil
	}
	// Coalesce the cache-miss path: a burst of concurrent events for the same
	// revision (all Kustomizations on a new push) collapses to ONE walk
	// instead of one per event. The leader resolves + warms the cache; the
	// waiters block on it and reuse the result. On error we Forget the key so
	// a transient failure doesn't get pinned onto callers arriving moments
	// later — the next event re-attempts immediately rather than waiting a
	// reconcile cycle. (singleflight already drops the entry once the leader
	// returns; Forget only matters for the tiny window between failure and
	// return, but it makes the "flaky failure" recovery deterministic.)
	//
	// The walk runs under the leader's context. Each waiter stops waiting
	// when its own context ends, and a waiter whose leader was cancelled
	// (its pipeline deadline) retries instead of failing with the leader's
	// cancellation while it still has time of its own.
	for attempt := 1; ; attempt++ {
		v, shared, led, err := c.findPRsForCommitShared(ctx, owner, repo, sha, cacheKey)
		if err != nil && !led && ctx.Err() == nil && attempt < maxCoalescedAttempts &&
			(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			continue
		}
		if err != nil {
			return nil, err
		}
		// shared is true for every caller of a coalesced batch, INCLUDING the
		// leader that actually walked — so this slightly over-counts (by the one
		// leader per batch). That's an acceptable imprecision for a trend metric;
		// the precise "real walks" figure is the prs_walk miss counter above.
		if shared {
			metrics.PRsForCommitCoalesced.Inc()
		}
		return v, nil
	}
}

// maxCoalescedAttempts bounds how often a waiter re-joins (or starts) a
// resolution after the one it waited on was cancelled.
const maxCoalescedAttempts = 3

// findPRsForCommitShared runs or joins the single-flight resolution for
// cacheKey. led reports whether this caller ran the walk itself. A caller
// whose context ends stops waiting and returns its own context error.
func (c *Commenter) findPRsForCommitShared(ctx context.Context, owner, repo, sha, cacheKey string) (prs []PRInfo, shared, led bool, err error) {
	ch := c.prsForCommitSF.DoChan(cacheKey, func() (any, error) {
		led = true
		// Double-checked read: a leader that just finished may have warmed
		// the cache and had its single-flight entry cleared before this
		// goroutine arrived. Re-reading here avoids a redundant walk in that
		// narrow window (and is what keeps misses ≈ real walks).
		if cached, hit := c.cachedPRsForCommit(ctx, cacheKey); hit {
			metrics.FetchCacheHits.WithLabelValues("prs_walk").Inc()
			return cached, nil
		}
		// Reached the real resolution: this is a genuine cache miss / walk.
		// Recorded here (leader only) so the miss count tracks actual GitHub
		// work, not the waiters that coalesce onto it.
		metrics.FetchCacheMisses.WithLabelValues("prs_walk").Inc()
		var obs observedAt
		result, err := c.resolvePRsForCommit(ctx, owner, repo, sha, &obs)
		if err != nil {
			c.prsForCommitSF.Forget(cacheKey)
			return nil, err
		}
		// The result is as old as the oldest per-commit lookup it reused, so
		// it expires with that one: re-caching it for a full TTL would serve
		// PR state up to twice the TTL old.
		if ttl := obs.remaining(c.prsForCommitTTL); ttl > 0 {
			if encoded, mErr := json.Marshal(result); mErr == nil {
				c.cache.Put(ctx, cacheKey, encoded, ttl)
			}
		}
		return result, nil
	})
	select {
	case <-ctx.Done():
		return nil, false, false, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Shared, led, r.Err
		}
		return r.Val.([]PRInfo), r.Shared, led, nil
	}
}

// cachedPRsForCommit reads and decodes the whole-resolution prs-walk cache
// entry. Returns (nil, false) when caching is off, the key is absent, or the
// stored bytes don't decode. Factored out so FindPRsForCommit can check the
// cache both before and inside the single-flight critical section without
// duplicating the decode.
func (c *Commenter) cachedPRsForCommit(ctx context.Context, cacheKey string) ([]PRInfo, bool) {
	if c.prsForCommitTTL <= 0 {
		return nil, false
	}
	data, hit := c.cache.Get(ctx, cacheKey)
	if !hit {
		return nil, false
	}
	var cached []PRInfo
	if err := json.Unmarshal(data, &cached); err != nil {
		return nil, false
	}
	return cached, true
}

// resolvePRsForCommit is the uncached direct-lookup + merge-parent-walk
// resolution behind FindPRsForCommit's result cache.
//
// The walk runs even when the direct lookup finds PRs: a deployed branch can
// itself be the head of an open PR (a promotion PR from the dev branch), and
// stopping at that PR would hide the PRs merged into the branch. For a
// non-merge commit the walk ends after one (cached) commit lookup.
func (c *Commenter) resolvePRsForCommit(ctx context.Context, owner, repo, sha string, obs *observedAt) ([]PRInfo, error) {
	direct, err := c.listPRsForCommit(ctx, owner, repo, sha, obs)
	if err != nil {
		return nil, err
	}
	walked, err := c.walkMergeParents(ctx, owner, repo, sha, obs)
	if err != nil {
		return nil, err
	}
	if len(direct) == 0 {
		return walked, nil
	}
	seen := make(map[int]struct{}, len(direct))
	for _, pr := range direct {
		seen[pr.Number] = struct{}{}
	}
	out := direct
	for _, pr := range walked {
		if _, dup := seen[pr.Number]; !dup {
			seen[pr.Number] = struct{}{}
			out = append(out, pr)
		}
	}
	return out, nil
}

// observedAt tracks when the oldest PR lookup a resolution used was fetched
// from GitHub, cached or not.
type observedAt struct{ oldest time.Time }

func (o *observedAt) add(t time.Time) {
	if o.oldest.IsZero() || t.Before(o.oldest) {
		o.oldest = t
	}
}

// remaining is what is left of ttl for data observed at o.oldest; ttl itself
// when nothing was observed.
func (o *observedAt) remaining(ttl time.Duration) time.Duration {
	if o.oldest.IsZero() {
		return ttl
	}
	return ttl - time.Since(o.oldest)
}

// prsForCommitEntry is the cached form of one commit's PR lookup. At is when
// it was fetched from GitHub; a result built from it must not outlive it.
type prsForCommitEntry struct {
	At  time.Time `json:"at"`
	PRs []PRInfo  `json:"prs"`
}

func (c *Commenter) listPRsForCommit(ctx context.Context, owner, repo, sha string, obs *observedAt) ([]PRInfo, error) {
	// Cache lookup before the API call. The PR NUMBERS a commit belongs
	// to are stable, but state (open/closed/merged) + labels can mutate,
	// so the cache uses a short TTL (default 5m) — long enough to
	// amortize the dozens of repeated lookups autodev does across one
	// batch window, short enough that operators don't see noticeably
	// stale state in normal flows.
	// The "2" marks entries that carry their fetch time: a new prefix, not a
	// format change under the old one, so replicas on either version during a
	// rollout don't keep overwriting each other's entries.
	cacheKey := "prs-for-commit2:" + owner + "/" + repo + "/" + sha
	if c.prsForCommitTTL > 0 {
		if data, hit := c.cache.Get(ctx, cacheKey); hit {
			var cached prsForCommitEntry
			if err := json.Unmarshal(data, &cached); err == nil && !cached.At.IsZero() {
				metrics.FetchCacheHits.WithLabelValues("prs_for_commit").Inc()
				obs.add(cached.At)
				return cached.PRs, nil
			}
		}
		metrics.FetchCacheMisses.WithLabelValues("prs_for_commit").Inc()
	}
	fetchedAt := time.Now()
	// Paginate: a single commit can be the head of arbitrarily many PRs
	// (cherry-pick targets, dependabot batches, autodev fanouts). The
	// default page size is 30 — without an explicit loop we'd silently
	// truncate, which produces "phantom missing PR" symptoms downstream
	// in the parent-walk + inventory filter.
	opt := &gh.ListOptions{PerPage: 100}
	var out []PRInfo
	for {
		prs, resp, err := c.client.PullRequests.ListPullRequestsWithCommit(ctx, owner, repo, sha, opt)
		recordRateLimit(resp, err)
		if err != nil {
			return nil, fmt.Errorf("list pull requests with commit: %w", err)
		}
		for _, pr := range prs {
			labels := make([]string, 0, len(pr.Labels))
			for _, l := range pr.Labels {
				if name := l.GetName(); name != "" {
					labels = append(labels, name)
				}
			}
			out = append(out, PRInfo{
				Labels: labels,
				// List-form PR objects (GET /commits/{sha}/pulls) carry
				// merged_at but never the `merged` boolean, so GetMerged()
				// would be false even for merged PRs. Derive it.
				Merged:  pr.MergedAt != nil,
				Number:  pr.GetNumber(),
				State:   pr.GetState(),
				HeadSHA: pr.GetHead().GetSHA(),
				BaseSHA: pr.GetBase().GetSHA(),
			})
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	if c.prsForCommitTTL > 0 {
		// Cache the result — INCLUDING the empty case. The direct lookup of a
		// dev-branch merge commit (the top of FindPRsForCommit) is always
		// empty (merge commits aren't a PR head), so without negative caching
		// every event for every Kustomization re-queries the same SHA and
		// burns the GitHub rate limit — on a busy repo with many
		// Kustomizations reconciling the same revision this dominates the
		// call budget. The TTL (default 5m) bounds the one case where empty
		// could legitimately flip to non-empty (a PR opened against the
		// commit); merge-commit empties never flip at all.
		if encoded, err := json.Marshal(prsForCommitEntry{At: fetchedAt, PRs: out}); err == nil {
			c.cache.Put(ctx, cacheKey, encoded, c.prsForCommitTTL)
		}
	}
	obs.add(fetchedAt)
	return out, nil
}

// walkMergeParents walks back from sha along parents[0] (the "into" branch
// history), probing parents[1..] of each merge commit for associated PRs.
// Stops at a non-merge commit (no PRs to find further back), at the merge
// base with the default branch, or at the depth cap.
//
// The merge-base stop matters when the default branch itself uses merge
// commits: an autodev dev branch is the default branch's tip plus one merge
// per labeled PR, and without the stop the walk would continue through the
// default branch's own merge history up to the depth cap. Everything there
// is already merged — GitHub resolves those tips to merged PRs, which the
// processor drops — so stopping loses nothing. It is looked up lazily, only
// once the chain reaches a second merge commit, and when it can't be
// determined the walk runs to the depth cap as before.
//
// Caching: each commit's parents are looked up once and reused across
// every subsequent event that walks the same chain — critical for
// autodev where consecutive dev-branch advances re-walk the same
// ancestors. Commit metadata is truly immutable; the entries' TTL
// (COMMIT_TTL, default 1h) only bounds the cache's growth.
func (c *Commenter) walkMergeParents(ctx context.Context, owner, repo, sha string, obs *observedAt) ([]PRInfo, error) {
	seen := map[int]struct{}{}
	var out []PRInfo
	current := sha
	mergeBase, mergeBaseKnown := "", false
	for i := 0; i < c.parentWalkDepth; i++ {
		parents, err := c.getCommitParents(ctx, owner, repo, current)
		if err != nil {
			return out, fmt.Errorf("get commit %s: %w", current, err)
		}
		if len(parents) < 2 {
			return out, nil
		}
		if i > 0 {
			if !mergeBaseKnown {
				mergeBase, mergeBaseKnown = c.defaultBranchMergeBase(ctx, owner, repo, sha), true
			}
			// sha itself on the default branch (merge base == sha): every
			// ancestor is too.
			if mergeBase != "" && (current == mergeBase || mergeBase == sha) {
				return out, nil
			}
		}
		for _, headSHA := range parents[1:] {
			if headSHA == "" {
				continue
			}
			prs, err := c.listPRsForCommit(ctx, owner, repo, headSHA, obs)
			if err != nil {
				return out, err
			}
			for _, pr := range prs {
				if _, dup := seen[pr.Number]; dup {
					continue
				}
				seen[pr.Number] = struct{}{}
				out = append(out, pr)
			}
		}
		current = parents[0]
		if current == "" {
			return out, nil
		}
	}
	return out, nil
}

// defaultBranchMergeBase returns the merge base of sha and the repository's
// default branch, or "" when it can't be determined (unrelated histories, an
// API error) — the walk then runs without the stop. Cached per sha with the
// commit TTL: as the default branch advances the true merge base can only
// move closer to sha, so a cached older one makes the walk a few hops longer,
// never shorter.
func (c *Commenter) defaultBranchMergeBase(ctx context.Context, owner, repo, sha string) string {
	cacheKey := "merge-base:" + owner + "/" + repo + "/" + sha
	if data, hit := c.cache.Get(ctx, cacheKey); hit {
		metrics.FetchCacheHits.WithLabelValues("merge_base").Inc()
		return string(data)
	}
	metrics.FetchCacheMisses.WithLabelValues("merge_base").Inc()
	branch, err := c.defaultBranch(ctx, owner, repo)
	if err != nil {
		slog.Default().Debug("parent walk: default branch unknown; walking without a merge-base stop",
			"owner", owner, "repo", repo, "error", err.Error())
		return ""
	}
	// One commit per page keeps the response small; merge_base_commit is
	// present regardless.
	cmp, resp, err := c.client.Repositories.CompareCommits(ctx, owner, repo, branch, sha, &gh.ListOptions{PerPage: 1})
	recordRateLimit(resp, err)
	if err != nil {
		slog.Default().Debug("parent walk: no merge base with the default branch; walking without a stop",
			"owner", owner, "repo", repo, "branch", branch, "sha", sha, "error", err.Error())
		return ""
	}
	base := cmp.GetMergeBaseCommit().GetSHA()
	if base != "" {
		c.cache.Put(ctx, cacheKey, []byte(base), c.commitTTL)
	}
	return base
}

// defaultBranch returns the repository's default branch name, cached with the
// commit TTL (it changes rarely, and a stale name only weakens the walk stop).
func (c *Commenter) defaultBranch(ctx context.Context, owner, repo string) (string, error) {
	cacheKey := "default-branch:" + owner + "/" + repo
	if data, hit := c.cache.Get(ctx, cacheKey); hit {
		metrics.FetchCacheHits.WithLabelValues("default_branch").Inc()
		return string(data), nil
	}
	metrics.FetchCacheMisses.WithLabelValues("default_branch").Inc()
	r, resp, err := c.client.Repositories.Get(ctx, owner, repo)
	recordRateLimit(resp, err)
	if err != nil {
		return "", err
	}
	branch := r.GetDefaultBranch()
	if branch == "" {
		return "", errors.New("repository reports no default branch")
	}
	c.cache.Put(ctx, cacheKey, []byte(branch), c.commitTTL)
	return branch, nil
}

// getCommitParents returns the SHAs of the given commit's parents (in
// order — parents[0] is the "into" branch tip, parents[1..] are merged-in
// branch tips for merge commits). Goes through the fetchcache:
// commit-metadata is immutable so cache hits are always safe; entries carry
// a coarse TTL (commitTTL) only to bound unbounded growth, not for freshness.
//
// On cache miss we hit `GET /repos/{owner}/{repo}/commits/{sha}` and
// cache the parents. Errors propagate to the caller — caching is
// best-effort, and a transient cache failure shouldn't be confused with
// a real GitHub failure.
func (c *Commenter) getCommitParents(ctx context.Context, owner, repo, sha string) ([]string, error) {
	cacheKey := "commit:" + owner + "/" + repo + "/" + sha
	if data, hit := c.cache.Get(ctx, cacheKey); hit {
		var parents []string
		if err := json.Unmarshal(data, &parents); err == nil {
			metrics.FetchCacheHits.WithLabelValues("commit").Inc()
			return parents, nil
		}
		// Malformed cache entry — log via metric and refetch. Treating
		// this as a miss avoids a wedge where bad data permanently
		// breaks lookups for one SHA.
	}
	metrics.FetchCacheMisses.WithLabelValues("commit").Inc()
	commit, resp, err := c.client.Repositories.GetCommit(ctx, owner, repo, sha, nil)
	recordRateLimit(resp, err)
	if err != nil {
		return nil, err
	}
	parents := make([]string, 0, len(commit.Parents))
	for _, p := range commit.Parents {
		parents = append(parents, p.GetSHA())
	}
	if encoded, err := json.Marshal(parents); err == nil {
		// Immutable data, but TTL-bounded so the walk's ancestors don't
		// accumulate forever in Redis (see commitTTL / defaultCommitTTL).
		c.cache.Put(ctx, cacheKey, encoded, c.commitTTL)
	}
	return parents, nil
}

// PostOrUpdateComment delivers the bridge's analysis to a PR. The comment is
// scoped per Kustomization via the (namespace, name) pair: the marker becomes
// `<!-- flux-ai-bridge:<ns>/<name> -->`, and the delete-prior step only
// removes comments carrying the SAME scoped marker. A PR that touches three
// Kustomizations therefore gets three sticky comments, one per Kustomization,
// each independently refreshed.
//
// Legacy unscoped comments (`<!-- flux-ai-bridge -->`) posted by earlier
// bridge versions are NOT touched here — they belonged to no specific
// Kustomization, so we can't safely attribute and replace them. Operators
// upgrading can delete them by hand once.
//
// When keepHistory is true the delete-prior step is skipped entirely and
// every analysis is posted as a fresh comment. The PR ends up with a trail
// of comments — useful when iterating on a workload that toggles between
// healthy and broken across commits, because the chronological sequence is
// preserved on the PR.
func (c *Commenter) PostOrUpdateComment(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int, body string) error {
	return c.PostOrUpdateCommentKeyed(ctx, owner, repo, kustomizationNamespace, kustomizationName, prNumber, body, body)
}

// PostOrUpdateCommentKeyed is PostOrUpdateComment with an explicit STABILITY
// KEY: the skip-if-unchanged decision hashes stabilityKey instead of the full
// body. The processor passes the deterministic kstatus portion as the key, so
// an LLM that re-words an identical cluster state on every re-eval doesn't
// cause a delete+recreate swap (notification churn, and pre-create-first a
// visible no-comment gap) — the existing comment, whose prose is still valid
// for the unchanged state, stays put. A real state change alters the key and
// swaps as before. PostOrUpdateComment passes the body as its own key,
// preserving exact-body semantics for callers without a deterministic core.
func (c *Commenter) PostOrUpdateCommentKeyed(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int, body, stabilityKey string) error {
	// Serialize concurrent posts for the same Kustomization scope. Without
	// this, two goroutines processing different SHAs of the same Kustomization
	// can both pass the list-then-delete step before either creates, leaving
	// duplicate comments on the PR. See postLocks docstring on Commenter.
	lock := c.lockForScope(kustomizationNamespace, kustomizationName)
	lock.Lock()
	defer lock.Unlock()
	marker := c.markerFor(kustomizationNamespace, kustomizationName)
	hash := bodyHash(marker + "\n" + stabilityKey)
	// The stability hash is ALSO embedded in the comment as a hidden marker:
	// the cache is the fast path, but it can be lost (restart without
	// persistence, or deliberately cleared after a post-create delete
	// failure) — and the key is not recoverable from the rendered body (the
	// LLM prose isn't part of it). Without the embedded copy, a cold cache
	// re-worded-but-same-state post would swap the comment, defeating the
	// stability-key contract exactly when the cache can't vouch. The slow
	// path reads it back via commentKeyOf.
	commentBody := truncateComment(marker + "\n" + keyMarkerPrefix + hash + " -->\n" + body)

	if c.keepHistory {
		// History mode: always append a fresh comment, never edit or delete,
		// so the PR keeps the full chronological trail of analyses.
		_, resp, err := c.client.Issues.CreateComment(ctx, owner, repo, prNumber, &gh.IssueComment{Body: gh.String(commentBody)})
		recordRateLimit(resp, err)
		if err != nil {
			return fmt.Errorf("create comment: %w", err)
		}
		return nil
	}

	cacheKey := lastCommentCacheKey(owner, repo, prNumber, kustomizationNamespace, kustomizationName)

	// Fast path: skip-if-unchanged. We remember the comment we last delivered
	// for this (PR, scope) AND the hash of its body. If the new body hashes
	// identically, the comment on the PR is already correct — do nothing. No
	// GitHub call, no notification, no churn. This is the steady-state path on
	// a busy source branch where the Flux revision advances constantly but the
	// rendered analysis is unchanged.
	if cached, hit := c.cache.Get(ctx, cacheKey); hit {
		metrics.FetchCacheHits.WithLabelValues("last_comment").Inc()
		var lc lastComment
		if err := json.Unmarshal(cached, &lc); err == nil && lc.ID != 0 {
			if lc.Hash == hash {
				slog.Default().Debug("PR comment unchanged; skipping repost",
					"owner", owner, "repo", repo, "pr", prNumber,
					"scope", kustomizationNamespace+"/"+kustomizationName)
				return nil
			}
			// Body changed: post the FRESH comment FIRST, then delete the
			// prior one by its cached ID. Create-before-delete closes the gap
			// where a crash or pod termination between the two calls (observed
			// during a rolling deploy: the admission mark was already
			// committed, so nothing re-posted until the next re-eval window)
			// left the PR with NO comment while its labels still asserted a
			// verdict — the worst case is now a duplicate (when the delete
			// below fails, or the process dies before it), swept by the next
			// post for this scope, instead of a missing comment. Recreating
			// (rather than editing in place) gives the reviewer a notification
			// and a new timestamp for the updated verdict — GitHub sends
			// nothing on an edit. This skips the paginated ListComments on the
			// hot path.
			//
			// The new comment is recorded only once its predecessor is gone
			// (createComment clears the record first): a crash in between
			// leaves the cache cold, so the next post lists the comments and
			// sweeps the leftover instead of trusting a record that says the
			// swap finished.
			newID, cerr := c.createComment(ctx, owner, repo, prNumber, commentBody, cacheKey)
			if cerr != nil {
				// The old comment is untouched and the cache cold — the next
				// event lists the comments and retries the swap.
				return cerr
			}
			cctx, cancel := cleanupContext(ctx)
			defer cancel()
			delResp, derr := c.client.Issues.DeleteComment(cctx, owner, repo, lc.ID)
			recordRateLimit(delResp, derr)
			if derr != nil && (delResp == nil || delResp.StatusCode != http.StatusNotFound) {
				// The fresh comment is live, but its prior version is now an
				// orphan. Leave the cache cold so the NEXT post takes the slow
				// path and its list-based sweep removes the leftover — with
				// the cache hot, an unchanged stability key would
				// short-circuit forever and the duplicate would never be
				// cleaned.
				slog.Default().Warn("prior PR comment delete failed; cache left cold so the next post sweeps the duplicate",
					"owner", owner, "repo", repo, "pr", prNumber, "comment_id", lc.ID, "error", derr.Error())
				return nil
			}
			c.recordComment(cctx, cacheKey, newID, hash)
			return nil
		}
	} else {
		metrics.FetchCacheMisses.WithLabelValues("last_comment").Inc()
	}

	// Slow path: no usable cache. List the PR's comments and find ours by
	// marker. If the newest one already matches the desired body the comment
	// is current — skip (and clean up any older duplicates). Otherwise delete
	// every matching comment and post a fresh one. Duplicates only arise from
	// cross-replica races or the legacy era; steady state has exactly one.
	mine, err := c.listScopedComments(ctx, owner, repo, prNumber, marker)
	if err != nil {
		return err
	}

	if len(mine) == 0 {
		newID, err := c.createComment(ctx, owner, repo, prNumber, commentBody, cacheKey)
		if err != nil {
			return err
		}
		c.recordComment(ctx, cacheKey, newID, hash)
		return nil
	}

	// Newest = highest ID (GitHub issue-comment IDs are monotonic).
	keep := mine[0]
	for _, cm := range mine[1:] {
		if cm.GetID() > keep.GetID() {
			keep = cm
		}
	}

	if keep.GetBody() == commentBody || commentKeyOf(keep.GetBody()) == hash {
		// Already correct — the cache was merely cold. The first clause
		// matches byte-identical bodies; the second recovers the stability
		// key from the comment's own hidden marker, so an unchanged STATE
		// with re-worded LLM prose doesn't swap (the existing prose still
		// describes this exact state). Delete older duplicates, record the
		// kept comment, and skip the recreate. A failed duplicate delete is
		// tolerated like the swap's below — the verdict is live, and failing
		// the delivery would re-run the analysis to clean a leftover — but
		// the cache stays cold so the next post re-sweeps.
		swept := true
		for _, cm := range mine {
			if cm.GetID() == keep.GetID() {
				continue
			}
			delResp, err := c.client.Issues.DeleteComment(ctx, owner, repo, cm.GetID())
			recordRateLimit(delResp, err)
			if err != nil && (delResp == nil || delResp.StatusCode != http.StatusNotFound) {
				swept = false
				slog.Default().Warn("duplicate PR comment delete failed; cache left cold so the next post re-sweeps",
					"owner", owner, "repo", repo, "pr", prNumber, "comment_id", cm.GetID(), "error", err.Error())
			}
		}
		if swept {
			c.cache.Put(ctx, cacheKey, encodeLastComment(keep.GetID(), hash), 0)
		}
		return nil
	}

	// Changed: post the fresh comment FIRST (create-before-delete — same
	// missing-comment rationale as the fast path), then delete every prior
	// marker comment. Delete failures after the create are TOLERATED, exactly
	// like the fast path: the verdict comment is already live, so failing the
	// delivery would abort label reconciliation and roll back the admission
	// mark — re-running the whole analysis (LLM included) just to clean a
	// duplicate. Instead: leave the cache cold so the next post re-enters this
	// sweep, warn, and report success. As on the fast path, the new comment
	// is recorded only after every prior one is gone.
	newID, err := c.createComment(ctx, owner, repo, prNumber, commentBody, cacheKey)
	if err != nil {
		return err
	}
	cctx, cancel := cleanupContext(ctx)
	defer cancel()
	swept := true
	for _, cm := range mine {
		delResp, err := c.client.Issues.DeleteComment(cctx, owner, repo, cm.GetID())
		recordRateLimit(delResp, err)
		if err != nil && (delResp == nil || delResp.StatusCode != http.StatusNotFound) {
			swept = false
			slog.Default().Warn("prior PR comment delete failed during sweep; cache left cold so the next post re-sweeps",
				"owner", owner, "repo", repo, "pr", prNumber, "comment_id", cm.GetID(), "error", err.Error())
		}
	}
	if swept {
		c.recordComment(cctx, cacheKey, newID, hash)
	}
	return nil
}

// CommentCurrent reports whether the comment last delivered for (PR, scope)
// was posted under the SAME stability key — i.e. the cluster state the
// processor is about to describe is the one already on the PR. Cache-only
// (zero GitHub calls): a miss (cold cache, never posted, evicted) reports
// false and the caller proceeds with the full pipeline. The processor uses
// this BEFORE the LLM call so an unchanged state skips the paid round-trip,
// not just the repost.
func (c *Commenter) CommentCurrent(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int, stabilityKey string) bool {
	if c.keepHistory {
		return false // history mode always appends; nothing is "current"
	}
	cacheKey := lastCommentCacheKey(owner, repo, prNumber, kustomizationNamespace, kustomizationName)
	cached, hit := c.cache.Get(ctx, cacheKey)
	if !hit {
		return false
	}
	var lc lastComment
	if err := json.Unmarshal(cached, &lc); err != nil || lc.ID == 0 {
		return false
	}
	marker := c.markerFor(kustomizationNamespace, kustomizationName)
	return lc.Hash == bodyHash(marker+"\n"+stabilityKey)
}

// RemoveComment deletes the bridge's prior comment for a Kustomization scope.
// It's used when comment posting is suppressed (COMMENT_ON_SUCCESS=false on a
// now-healthy PR) but a stale comment from an earlier failing run may still be
// on the PR.
//
// Cache-guarded: a recorded comment ID → one DELETE; a recorded "no comment"
// → no API call at all, which keeps the healthy path free of GitHub round
// trips. Only without any record — the cache was lost (in-memory restart) or
// the entry expired (the Redis backstop) — does it list the PR's comments
// once to find the scope's comment, and then records "no comment" so the
// next healthy events skip the lookup. Without that fallback a stale failure
// comment would outlive its cache entry and stay on the PR for good.
//
// keepHistory mode is a no-op: that mode intentionally never deletes.
func (c *Commenter) RemoveComment(ctx context.Context, owner, repo, kustomizationNamespace, kustomizationName string, prNumber int) error {
	if c.keepHistory {
		return nil
	}
	lock := c.lockForScope(kustomizationNamespace, kustomizationName)
	lock.Lock()
	defer lock.Unlock()
	cacheKey := lastCommentCacheKey(owner, repo, prNumber, kustomizationNamespace, kustomizationName)
	var ids []int64
	var lc lastComment
	if cached, hit := c.cache.Get(ctx, cacheKey); hit && json.Unmarshal(cached, &lc) == nil && (lc.ID != 0 || lc.None) {
		metrics.FetchCacheHits.WithLabelValues("last_comment").Inc()
		if lc.None {
			return nil
		}
		ids = []int64{lc.ID}
	} else {
		metrics.FetchCacheMisses.WithLabelValues("last_comment").Inc()
		mine, err := c.listScopedComments(ctx, owner, repo, prNumber, c.markerFor(kustomizationNamespace, kustomizationName))
		if err != nil {
			return err
		}
		for _, cm := range mine {
			ids = append(ids, cm.GetID())
		}
	}
	for _, id := range ids {
		delResp, derr := c.client.Issues.DeleteComment(ctx, owner, repo, id)
		recordRateLimit(delResp, derr)
		// A 404 means it's already gone (peer/user deleted it) — that's success.
		if derr != nil && (delResp == nil || delResp.StatusCode != http.StatusNotFound) {
			return fmt.Errorf("remove comment %d: %w", id, derr)
		}
	}
	c.cache.Put(ctx, cacheKey, encodeNoComment(), 0)
	return nil
}

// listScopedComments returns the bridge's comments for one scope's marker on a
// PR, across all pages.
func (c *Commenter) listScopedComments(ctx context.Context, owner, repo string, prNumber int, marker string) ([]*gh.IssueComment, error) {
	var mine []*gh.IssueComment
	opt := &gh.IssueListCommentsOptions{ListOptions: gh.ListOptions{PerPage: 100}}
	for {
		comments, resp, err := c.client.Issues.ListComments(ctx, owner, repo, prNumber, opt)
		recordRateLimit(resp, err)
		if err != nil {
			return nil, fmt.Errorf("list comments: %w", err)
		}
		for _, comment := range comments {
			if isBridgeComment(comment, marker) {
				mine = append(mine, comment)
			}
		}
		if resp == nil || resp.NextPage == 0 {
			return mine, nil
		}
		opt.Page = resp.NextPage
	}
}

// maxCommentRunes keeps comment bodies under GitHub's 65,536-character limit,
// with headroom for characters GitHub may count twice (e.g. emoji). Larger
// bodies are rejected with a 422, which would fail the delivery — and roll
// back the dedupe mark, so every later event re-ran the whole analysis
// (LLM call included) only to be rejected again.
const maxCommentRunes = 64000

// fenceCloseReserve is room kept for the delimiter that closes a code block
// the cut left open — a newline plus a fence, which is rarely over 4 long.
const fenceCloseReserve = 16

// truncationNote replaces whatever truncateComment cuts off.
const truncationNote = "\n\n*Comment truncated: it exceeded GitHub's comment size limit. See the bridge logs for the full analysis.*\n"

// truncateComment cuts body to fit maxCommentRunes. The cut lands on the last
// line break before the limit (no half table row), closes a code fence the
// cut left open, and appends truncationNote. The marker and key header at
// the top are always kept. Bodies within the limit are returned unchanged.
func truncateComment(body string) string {
	if utf8.RuneCountInString(body) <= maxCommentRunes {
		return body
	}
	room := maxCommentRunes - utf8.RuneCountInString(truncationNote) - fenceCloseReserve
	kept := string([]rune(body)[:room])
	if i := strings.LastIndexByte(kept, '\n'); i > 0 {
		kept = kept[:i]
	}
	kept = markdown.CloseOpenFence(kept)
	slog.Default().Warn("PR comment exceeds GitHub's size limit; truncated",
		"runes", utf8.RuneCountInString(body), "limit", maxCommentRunes)
	return kept + truncationNote
}

// isBridgeComment reports whether comment is one the bridge wrote for this
// marker's scope: the marker is the body's first line (the bridge always puts
// it there) and the author isn't a human account. A plain substring match
// would also claim — and delete — a user's comment that quotes the marker, or
// one whose text echoes it further down.
func isBridgeComment(comment *gh.IssueComment, marker string) bool {
	if comment.GetUser().GetType() == "User" {
		return false
	}
	return strings.HasPrefix(comment.GetBody(), marker+"\n")
}

// cleanupTimeout bounds the deletes that follow a successful create.
const cleanupTimeout = 30 * time.Second

// cleanupContext detaches post-create cleanup from the caller's cancellation.
// Once the fresh comment is live, deleting its predecessor is what keeps the
// PR from showing two verdicts; a cancellation landing between the two calls
// (the pipeline deadline mid-swap) would otherwise leave the stale one
// behind — and with the PR-head's dedupe mark kept, no later post at that
// head sweeps it. Shutdown still stops it (see lifecycle), so the event's
// rollback runs before the process exits; the leftover is then swept by the
// next post for this scope.
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return lifecycle.Detach(ctx, cleanupTimeout)
}

// createComment posts a fresh comment and returns its ID. It drops the
// scope's cached record first: until the caller records the new comment (see
// recordComment), the record must not claim a state the PR doesn't have — a
// stale ID, or "no comment" (which would make RemoveComment skip the new
// one). A cold cache sends the next post or removal to the list-based path.
func (c *Commenter) createComment(ctx context.Context, owner, repo string, prNumber int, commentBody, cacheKey string) (int64, error) {
	c.cache.Delete(ctx, cacheKey)
	newComment, resp, err := c.client.Issues.CreateComment(ctx, owner, repo, prNumber, &gh.IssueComment{Body: gh.String(commentBody)})
	recordRateLimit(resp, err)
	if err != nil {
		return 0, fmt.Errorf("create comment: %w", err)
	}
	return newComment.GetID(), nil
}

// recordComment records the comment now live for this scope (its ID + body
// hash) so the next post can skip-if-unchanged and delete-by-ID on the fast
// path.
func (c *Commenter) recordComment(ctx context.Context, cacheKey string, id int64, hash string) {
	if id != 0 {
		c.cache.Put(ctx, cacheKey, encodeLastComment(id, hash), 0)
	}
}

// lastComment is the cached record of the comment the bridge last delivered
// for a (PR, Kustomization scope): its GitHub ID plus the hash of the body we
// wrote. The hash lets the next post short-circuit to "unchanged, do nothing"
// without a GitHub round-trip.
type lastComment struct {
	ID   int64  `json:"id"`
	Hash string `json:"hash"`
	// None records that the scope has no comment on the PR (RemoveComment
	// looked or deleted it), so later healthy events skip the lookup. Posting
	// overwrites it with the new comment's ID.
	None bool `json:"none,omitempty"`
}

func encodeLastComment(id int64, hash string) []byte {
	b, _ := json.Marshal(lastComment{ID: id, Hash: hash})
	return b
}

func encodeNoComment() []byte {
	b, _ := json.Marshal(lastComment{None: true})
	return b
}

// bodyHash is the content fingerprint used for skip-if-unchanged. SHA-256 is
// overkill for collision resistance here but it's cheap and unambiguous.
func bodyHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// lastCommentCacheKey is the cache key for "the comment ID this bridge
// last posted for (PR, Kustomization scope)". Format kept stable so a
// chart upgrade doesn't silently invalidate every operator's cache.
func lastCommentCacheKey(owner, repo string, prNumber int, ns, name string) string {
	return "last-comment:" + owner + "/" + repo + "/" + strconv.Itoa(prNumber) + "/" + ns + "/" + name
}

// Default outcome label names. Used when Options.PRLabelSuccess/PRLabelFailure
// are empty. Configurable per install so operators can match an existing
// merge-gate convention; whatever names are chosen become the contract a
// downstream GitHub Action keys on, so changing them later silently breaks
// branch-protection rules — treat as a contract once set.
const (
	DefaultPRLabelSuccess = "flux-success"
	DefaultPRLabelFailure = "flux-failed"
	DefaultPRLabelWarning = "flux-warning"
)

// Outcome is the bridge's classification of a Kustomization analysis.
// Mirrors the marker emoji vocabulary in processor.buildKstatusBody
// (✅ / ❌ / ⚠️) but as an enum the GitHub layer can switch on without
// importing the processor.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
	OutcomeWarning Outcome = "warning"
	// OutcomeNone means "remove all bridge labels" — used when a PR no longer
	// has any attributed Kustomization (e.g. its last touching scope was
	// de-scoped). Distinct from OutcomeWarning, which now applies the warning
	// label rather than clearing.
	OutcomeNone Outcome = "none"
)

// SetPRLabels reconciles the bridge's outcome labels on a single PR. Each
// outcome adds its own label and removes the other two:
//
//   - OutcomeSuccess → add `flux-success`,  remove `flux-failed` + `flux-warning`
//   - OutcomeFailure → add `flux-failed`,   remove `flux-success` + `flux-warning`
//   - OutcomeWarning → add `flux-warning`,  remove `flux-success` + `flux-failed`
//   - OutcomeNone    → remove ALL three (the PR no longer has any attributed
//     Kustomization — clear stale state)
//
// Colliding names are safe: a name in the add set is never removed, so
// configuring labelWarning == labelFailure makes a warning apply the failed
// label (and a merge gate keyed on it blocks) WITHOUT the add/remove pair
// cancelling out. Empty names are skipped.
//
// GitHub's POST /issues/{n}/labels is server-side idempotent (existing
// labels are not duplicated) and auto-creates labels that don't exist
// on the repo. DELETE /issues/{n}/labels/{label} returns 404 when the
// label isn't present; we swallow that as a no-op.
//
// Transition-only writes: the last outcome successfully written per PR
// is cached in the same fetchcache the commenter already uses (Redis
// when wired for cross-replica sharing, in-memory LRU otherwise). A
// subsequent call with the same outcome returns without any GitHub API
// traffic — the common steady-state case is a stream of healthy
// reconciles that all map to OutcomeSuccess, and we don't want to burn
// labels-API round-trips re-asserting state that already matches. The
// cache is updated only after every label op succeeds, so a partial
// failure leaves the slot empty and the next call retries.
//
// Cache key is PR-global (no Kustomization scope) because the LABEL is
// PR-global. Two Kustomizations that disagree about the outcome on the
// same PR will alternately overwrite — last-write-wins — and each
// disagreement bypasses the cache (different outcome → cache mismatch
// → API call). Manual edits in the GitHub UI are NOT detected within a
// head: a reviewer who removes the bridge's label has deliberately
// overridden it, and that stands for the current head. The cache entry
// is scoped to (PR, head SHA), so a NEW COMMIT always re-asserts the
// labels — observed in practice where a manually-removed flux-success
// was never restored because the cache was PR-scoped and the new head's
// identical aggregate hit it.
//
// Any other API error is returned to the caller. The caller (processor)
// treats label failures as non-fatal — the comment has already been
// posted at that point, so a label hiccup shouldn't tank the pipeline.
//
// Permissions: requires GitHub App `Issues: Read & Write` (PRs are
// issues for the label API). This is the same permission the comment
// surface already needs.
func (c *Commenter) SetPRLabels(ctx context.Context, owner, repo string, prNumber int, headSHA string, outcome Outcome) error {
	mu := c.lockForPRLabels(owner, repo, prNumber)
	mu.Lock()
	defer mu.Unlock()

	cacheKey := prOutcomeCacheKey(owner, repo, prNumber)
	record := c.prOutcomeRecord(headSHA, outcome)
	cached, hit := c.cache.Get(ctx, cacheKey)
	if hit && string(cached) == record {
		metrics.FetchCacheHits.WithLabelValues("pr_outcome").Inc()
		return nil
	}
	metrics.FetchCacheMisses.WithLabelValues("pr_outcome").Inc()

	var add, remove []string
	switch outcome {
	case OutcomeSuccess:
		add = []string{c.labelSuccess}
		remove = []string{c.labelFailure, c.labelWarning}
	case OutcomeFailure:
		add = []string{c.labelFailure}
		remove = []string{c.labelSuccess, c.labelWarning}
	case OutcomeWarning:
		add = []string{c.labelWarning}
		remove = []string{c.labelSuccess, c.labelFailure}
	case OutcomeNone:
		remove = []string{c.labelSuccess, c.labelFailure, c.labelWarning}
	default:
		return fmt.Errorf("unknown outcome %q", outcome)
	}
	// Collision-safe reconcile: drop empty names, and never remove a label we
	// are also adding (operators may set two label names equal — e.g.
	// labelWarning == labelFailure — so a warning blocks the same gate a failure
	// does; without this the add/remove pair would cancel out into a no-op).
	add = dedupeNonEmpty(add, nil)
	addSet := make(map[string]struct{}, len(add))
	for _, l := range add {
		addSet[l] = struct{}{}
	}
	remove = dedupeNonEmpty(remove, addSet)

	// The record describes the labels this reconcile is about to change. Drop
	// it before the first mutation: if a later step fails, a call with the old
	// outcome must reconcile again rather than trust a record the PR no
	// longer matches (e.g. both the old and the new label on it).
	if hit {
		c.cache.Delete(ctx, cacheKey)
	}
	if len(add) > 0 {
		_, resp, err := c.client.Issues.AddLabelsToIssue(ctx, owner, repo, prNumber, add)
		recordRateLimit(resp, err)
		if err != nil {
			return fmt.Errorf("add labels %v: %w", add, err)
		}
	}
	for _, label := range remove {
		// go-github puts the label into the URL path verbatim, so `status/failed`
		// would hit `.../labels/status/failed` and `a#b` `.../labels/a` — both
		// 404, read below as "not on the PR", leaving the stale label in place.
		resp, err := c.client.Issues.RemoveLabelForIssue(ctx, owner, repo, prNumber, url.PathEscape(label))
		recordRateLimit(resp, err)
		if err != nil {
			// 404 = label wasn't on the PR; treat as no-op so the
			// success/failure transitions stay idempotent across replays.
			if resp != nil && resp.StatusCode == http.StatusNotFound {
				continue
			}
			return fmt.Errorf("remove label %q: %w", label, err)
		}
	}
	// Record the new state ONLY after every label op succeeded; partial
	// failure leaves the cache empty so the next call retries the full
	// reconciliation rather than skipping based on a half-applied state.
	// Head-scoped value: a new commit misses and re-asserts the labels.
	c.cache.Put(ctx, cacheKey, []byte(record), 0)
	return nil
}

// prOutcomeRecord is the cached value for an outcome written at a head. It
// names the configured labels too, so renaming one (a new merge-gate label)
// re-applies the labels at the same head instead of hitting a record written
// under the old names.
func (c *Commenter) prOutcomeRecord(headSHA string, outcome Outcome) string {
	return fmt.Sprintf("%s|%s|%q|%q|%q", headSHA, outcome, c.labelSuccess, c.labelFailure, c.labelWarning)
}

// dedupeNonEmpty returns in with empty strings dropped, duplicates collapsed,
// and (when exclude is non-nil) any member of exclude removed. Order is
// preserved. Used by SetPRLabels so the remove set never contains a label the
// add set already carries (exclude = the add set) and so a label configured to
// the same name twice doesn't produce a duplicate API call.
func dedupeNonEmpty(in []string, exclude map[string]struct{}) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		if exclude != nil {
			if _, skip := exclude[s]; skip {
				continue
			}
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// prOutcomeCacheKey is the cache key for "the outcome this bridge last
// wrote to this PR". PR-global (no Kustomization scope) because the
// label is PR-global.
func prOutcomeCacheKey(owner, repo string, prNumber int) string {
	return "pr-outcome:" + owner + "/" + repo + "/" + strconv.Itoa(prNumber)
}

// PRFile is the per-file PR data the processor needs for inventory-based
// attribution. Path is repo-root-relative; BlobSHA is the file's git blob
// SHA at the PR's head (suitable for content-addressed fetching via
// GetBlobContent without first having to look up the PR's head commit).
// Status is "added" / "modified" / "removed" / "renamed" — we don't act
// on it today but the field is preserved for future attribution rules
// (e.g. "removed files map to inventory entries scheduled for deletion").
type PRFile struct {
	Path    string
	BlobSHA string
	Status  string
	// PreviousPath is the file's path at the PR base when Status is
	// "renamed" (GitHub's previous_filename). Empty in file lists cached
	// before it was recorded.
	PreviousPath string `json:",omitempty"`
}

// ListPRFiles returns each file changed by a pull request, paginated.
// Used by the processor to decide whether a PR's changes affect any
// resource the Kustomization manages — see processor.filterPRs for the
// inventory-membership rule. Large PRs paginate (the GitHub per-page max
// is 100; the API hard-caps at 3000 files per PR).
func (c *Commenter) ListPRFiles(ctx context.Context, owner, repo string, prNumber int, headSHA string) ([]PRFile, error) {
	// A PR's changed-file set is fixed for a given head commit — it only
	// changes when the PR gets new commits (→ new head SHA). So cache it
	// keyed by head, immutable (no TTL). This matters a lot: filterPRs calls
	// this for every candidate PR on EVERY event (before the per-PR dedup),
	// and the parent walk can surface many candidates — without the cache,
	// each is a fresh paginated GET /pulls/{n}/files per event, a major
	// chunk of the rate-limit budget on a busy repo. We only cache when the
	// head SHA is known (empty → always fetch, never cache a stale list).
	cacheKey := ""
	if headSHA != "" {
		cacheKey = "pr-files:" + owner + "/" + repo + "/" + strconv.Itoa(prNumber) + "/" + headSHA
		if data, hit := c.cache.Get(ctx, cacheKey); hit {
			var cached []PRFile
			if err := json.Unmarshal(data, &cached); err == nil && !missingPreviousPath(cached) {
				metrics.FetchCacheHits.WithLabelValues("pr_files").Inc()
				return cached, nil
			}
		}
		metrics.FetchCacheMisses.WithLabelValues("pr_files").Inc()
	}
	var out []PRFile
	opt := &gh.ListOptions{PerPage: 100}
	for {
		files, resp, err := c.client.PullRequests.ListFiles(ctx, owner, repo, prNumber, opt)
		recordRateLimit(resp, err)
		if err != nil {
			return nil, fmt.Errorf("list pr files: %w", err)
		}
		for _, f := range files {
			if name := f.GetFilename(); name != "" {
				out = append(out, PRFile{
					Path:         name,
					BlobSHA:      f.GetSHA(),
					Status:       f.GetStatus(),
					PreviousPath: f.GetPreviousFilename(),
				})
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	if cacheKey != "" {
		if encoded, err := json.Marshal(out); err == nil {
			c.cache.Put(ctx, cacheKey, encoded, 0) // immutable per head → no TTL
		}
	}
	return out, nil
}

// missingPreviousPath reports whether a cached file list predates PreviousPath:
// a rename without it. Such an entry is refetched rather than served.
func missingPreviousPath(files []PRFile) bool {
	for _, f := range files {
		if f.Status == "renamed" && f.PreviousPath == "" {
			return true
		}
	}
	return false
}

// BlobResourceIDs returns the inventory IDs of the Kubernetes resources
// declared in a PR file blob, going through the fetchcache. It is the only
// blob entry point the pipeline uses (PR ↔ inventory matching).
//
// We cache the DERIVED IDs (`blobrefs2:<owner>/<repo>/<sha>`), never the raw
// manifest bytes. Two wins over the old raw-blob cache:
//   - space: a few short `<ns>_<name>_<group>_<Kind>` strings instead of a
//     full file;
//   - safety: GitOps manifests can carry Secret/ConfigMap bodies, and we no
//     longer copy that content into Redis — only resource coordinates, which
//     aren't sensitive.
//
// The blob SHA is content-addressed (immutable), so the derived IDs are
// immutable too; the cache entry never needs a TTL. On a cache miss the raw
// blob is fetched and parsed via the injected blobParser, then discarded.
func (c *Commenter) BlobResourceIDs(ctx context.Context, owner, repo, sha string) ([]string, error) {
	if sha == "" {
		return nil, nil
	}
	if c.blobParser == nil {
		return nil, fmt.Errorf("blob resource parser not configured")
	}
	// "2": entries now carry the parser's unparseable sentinel; older ones
	// don't, so they must not be read back.
	cacheKey := "blobrefs2:" + owner + "/" + repo + "/" + sha
	if data, hit := c.cache.Get(ctx, cacheKey); hit {
		var ids []string
		if err := json.Unmarshal(data, &ids); err == nil {
			metrics.FetchCacheHits.WithLabelValues("blobrefs").Inc()
			return ids, nil
		}
		// Malformed entry — treat as a miss and refetch rather than wedge.
	}
	metrics.FetchCacheMisses.WithLabelValues("blobrefs").Inc()
	content, err := c.getBlob(ctx, owner, repo, sha)
	if err != nil {
		return nil, err
	}
	ids := c.blobParser(content)
	if encoded, err := json.Marshal(ids); err == nil {
		c.cache.Put(ctx, cacheKey, encoded, 0) // derived from immutable blob → no TTL
	}
	return ids, nil
}

// BaseFileResourceIDs returns the inventory IDs declared in a file as it exists
// at ref (a PR's base commit) — the file's state BEFORE the PR. The caller
// diffs this against the head-blob IDs to find resources a `modified` file
// removed in place (commented out, or a YAML doc deleted while the file lives
// on), which BlobResourceIDs over the head blob alone can't see.
//
// ref is an immutable commit SHA, so (ref, path) → IDs is immutable and cached
// without a TTL, like BlobResourceIDs. A file absent at ref (added by the PR,
// or the path never existed) yields an empty set, not an error — there is
// simply nothing to diff. Parsing reuses the injected blobParser, so the raw
// manifest bytes are never cached.
func (c *Commenter) BaseFileResourceIDs(ctx context.Context, owner, repo, path, ref string) ([]string, error) {
	if ref == "" || path == "" {
		return nil, nil
	}
	if c.blobParser == nil {
		return nil, fmt.Errorf("blob resource parser not configured")
	}
	cacheKey := "basefileids2:" + owner + "/" + repo + "/" + ref + "/" + path // "2": see BlobResourceIDs
	if data, hit := c.cache.Get(ctx, cacheKey); hit {
		var ids []string
		if err := json.Unmarshal(data, &ids); err == nil {
			metrics.FetchCacheHits.WithLabelValues("basefileids").Inc()
			return ids, nil
		}
		// Malformed entry — treat as a miss and refetch rather than wedge.
	}
	metrics.FetchCacheMisses.WithLabelValues("basefileids").Inc()
	content, found, err := c.getContentsAtRef(ctx, owner, repo, path, ref)
	if err != nil {
		return nil, err
	}
	var ids []string
	if found {
		ids = c.blobParser(content)
	}
	if encoded, err := json.Marshal(ids); err == nil {
		c.cache.Put(ctx, cacheKey, encoded, 0) // ref is an immutable commit SHA → no TTL
	}
	return ids, nil
}

// getContentsAtRef fetches a single file's decoded bytes at a git ref. found is
// false (with nil error) when the path doesn't resolve to a file at ref — a 404
// (absent) or a directory — so callers treat "nothing there" distinctly from a
// genuine fetch error.
func (c *Commenter) getContentsAtRef(ctx context.Context, owner, repo, path, ref string) (content []byte, found bool, err error) {
	fileContent, _, resp, err := c.client.Repositories.GetContents(ctx, owner, repo, path, &gh.RepositoryContentGetOptions{Ref: ref})
	recordRateLimit(resp, err)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, false, nil // absent at base → nothing to diff
		}
		return nil, false, fmt.Errorf("get contents %s@%s: %w", path, ref, err)
	}
	if fileContent == nil {
		return nil, false, nil // a directory, not a file
	}
	// GetContent decodes base64 per the content's Encoding field.
	decoded, derr := fileContent.GetContent()
	if derr != nil {
		return nil, false, fmt.Errorf("decode contents %s@%s: %w", path, ref, derr)
	}
	return []byte(decoded), true, nil
}

// getBlob fetches the decoded bytes of a git blob by SHA. Not cached — the
// raw content is only ever consumed transiently by BlobResourceIDs, which
// caches the derived IDs instead. go-github returns the API's encoded
// content as-is, so a "base64" blob is decoded here.
func (c *Commenter) getBlob(ctx context.Context, owner, repo, sha string) ([]byte, error) {
	blob, resp, err := c.client.Git.GetBlob(ctx, owner, repo, sha)
	recordRateLimit(resp, err)
	if err != nil {
		return nil, fmt.Errorf("get blob %s: %w", sha, err)
	}
	if blob == nil {
		return nil, nil
	}
	content := blob.GetContent()
	if blob.GetEncoding() == "base64" {
		decoded, derr := base64.StdEncoding.DecodeString(content)
		if derr != nil {
			return nil, fmt.Errorf("decode blob %s: %w", sha, derr)
		}
		return decoded, nil
	}
	return []byte(content), nil
}
