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

package mcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	gosdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
	"gopkg.in/yaml.v3"
)

// Pool is a lazy-grown pool of MCP sessions, bounded by size. The first N
// callers each get a freshly-initialized session; subsequent callers reuse
// returned sessions. If a session is reported broken on release, it's closed
// and the next acquirer creates a fresh one in its place.
type Pool struct {
	clientImp *gosdk.Implementation
	closedCh  chan struct{}
	closing   bool
	created   int        // total sessions currently alive (idle + in-use)
	idle      []*Session // available sessions
	maxSize   int
	mu        sync.Mutex
	serverURL string

	// semaphore: capacity == maxSize. Acquire takes a slot before creating
	// or reusing; Release returns it.
	sem chan struct{}

	// logArgs caches the get_kubernetes_logs argument names this server
	// expects (see resolveLogArgs); nil until resolved.
	logArgs *logArgNames

	// lastOK is when an MCP round trip last succeeded (UnixNano), and
	// busyFreshness how recent it must be for Ping to call a full pool
	// healthy (see Ping).
	lastOK        atomic.Int64
	busyFreshness time.Duration
}

// defaultBusyFreshness: with every session taken, Ping reports healthy only
// if some MCP call succeeded within this window.
const defaultBusyFreshness = time.Minute

// Session wraps a single MCP client session. Callers should not retain a
// reference after Release.
type Session struct {
	inner *gosdk.ClientSession
}

// errPoolBusy is returned by a non-waiting acquire when every slot is taken.
var errPoolBusy = errors.New("mcp pool busy")

// Version is the bridge build version stamped into every MCP `initialize`
// handshake (visible to the MCP server in its logs). Set via the linker
// flag `-X github.com/staffbase/flux-ai-bridge/internal/mcp.Version=v1.2.3`
// in the release workflow. Defaults to "dev" for local builds.
var Version = "dev"

// NewPool initializes the pool and verifies one working session at startup
// (so misconfiguration fails fast). The probe session goes into the idle
// list rather than being discarded.
func NewPool(ctx context.Context, serverURL string, maxSize int) (*Pool, error) {
	if maxSize <= 0 {
		maxSize = 1
	}
	p := &Pool{
		serverURL: serverURL,
		maxSize:   maxSize,
		clientImp: &gosdk.Implementation{Name: "flux-ai-bridge", Version: Version},
		closedCh:  make(chan struct{}),
		sem:       make(chan struct{}, maxSize),

		busyFreshness: defaultBusyFreshness,
	}
	// Pre-create one session to validate connectivity at startup.
	probe, err := p.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("initial mcp session: %w", err)
	}
	p.mu.Lock()
	p.idle = append(p.idle, probe)
	p.created++
	p.mu.Unlock()
	p.lastOK.Store(time.Now().UnixNano())
	metrics.MCPPoolSize.Set(float64(p.created))
	return p, nil
}

func (p *Pool) dial(ctx context.Context) (*Session, error) {
	client := gosdk.NewClient(p.clientImp, nil)
	// MaxEventSize is set explicitly: the streamable transport treats 0 as
	// "no limit" (not the documented default), so one huge response — a
	// cluster-wide resource list — would be buffered whole.
	inner, err := client.Connect(ctx, &gosdk.StreamableClientTransport{Endpoint: p.serverURL, MaxEventSize: gosdk.DefaultMaxEventSize}, nil)
	if err != nil {
		return nil, fmt.Errorf("connect to mcp server: %w", err)
	}
	return &Session{inner: inner}, nil
}

// acquire blocks until a session can be reused or created. It returns the
// session and a release function the caller MUST call exactly once.
func (p *Pool) acquire(ctx context.Context) (*Session, func(error), error) {
	return p.acquireSlot(ctx, true)
}

// acquireSlot is acquire; with wait=false it returns errPoolBusy instead of
// waiting when every slot is taken.
func (p *Pool) acquireSlot(ctx context.Context, wait bool) (*Session, func(error), error) {
	if wait {
		select {
		case p.sem <- struct{}{}:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	} else {
		select {
		case p.sem <- struct{}{}:
		default:
			return nil, nil, errPoolBusy
		}
	}
	metrics.MCPPoolInUse.Set(float64(len(p.sem)))

	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		<-p.sem
		metrics.MCPPoolInUse.Set(float64(len(p.sem)))
		return nil, nil, errors.New("mcp pool closed")
	}
	var sess *Session
	if n := len(p.idle); n > 0 {
		sess = p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
	} else {
		p.created++
		metrics.MCPPoolSize.Set(float64(p.created))
		p.mu.Unlock()
		var err error
		sess, err = p.dial(ctx)
		if err != nil {
			p.mu.Lock()
			p.created--
			size := p.created
			p.mu.Unlock()
			metrics.MCPPoolSize.Set(float64(size))
			<-p.sem
			metrics.MCPPoolInUse.Set(float64(len(p.sem)))
			return nil, nil, err
		}
	}

	release := func(callErr error) {
		p.release(sess, callErr)
	}
	return sess, release, nil
}

func (p *Pool) release(sess *Session, callErr error) {
	defer func() {
		<-p.sem
		metrics.MCPPoolInUse.Set(float64(len(p.sem)))
	}()
	if sess == nil {
		return
	}
	if callErr == nil {
		p.lastOK.Store(time.Now().UnixNano())
	}
	broken := sessionBroken(callErr)

	p.mu.Lock()
	defer p.mu.Unlock()
	if broken || p.closing {
		// Discard broken session: close it, drop from pool census.
		go func() {
			_ = sess.inner.Close()
		}()
		p.created--
		metrics.MCPPoolSize.Set(float64(p.created))
		return
	}
	p.idle = append(p.idle, sess)
}

// rejectedByTransport matches the SDK's internal jsonrpc2.ErrRejected (code
// -32005) via jsonrpc.Error's code-based Is. The SDK wraps a request in it
// when it was not processed but the connection survives: a send failure
// (e.g. EOF on a stale keep-alive connection), a transient 5xx/429, or a
// protocol error the server answered over HTTP.
var rejectedByTransport = &jsonrpc.Error{Code: -32005}

// retryableCallError is the retry predicate for a failed MCP call: the
// request was rejected by the transport, the connection is closed, or the
// server no longer knows our Mcp-Session-Id (typically after a restart).
// A rejected call may have reached the server (5xx/429), so retrying is safe
// only because every tool the bridge calls is read-only. Typed checks only:
// substring matching on error text misfired both ways ("oneOf" matched
// "EOF", a pod hash matched "404", and a dead connection worded differently
// matched nothing).
func retryableCallError(err error) bool {
	return errors.Is(err, rejectedByTransport) ||
		errors.Is(err, gosdk.ErrConnectionClosed) ||
		errors.Is(err, gosdk.ErrSessionMissing)
}

// sessionBroken reports whether a call error leaves the session unusable,
// so release discards it instead of returning it to the idle stack. Only two
// outcomes keep it: a JSON-RPC error reply (the server answered over a
// working session) and our own cancellation. Everything else — including
// errors this code doesn't know — discards it: re-dialing costs one
// handshake, while a dead session back in the stack fails every caller that
// pops it (with a pool of one, the bridge and /readyz until restart).
func sessionBroken(err error) bool {
	switch {
	case err == nil:
		return false
	case retryableCallError(err):
		return true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	}
	var rpcErr *jsonrpc.Error
	return !errors.As(err, &rpcErr)
}

// GetKubernetesResource fetches a single Kubernetes resource by GVK + name +
// namespace via MCP. Namespace empty = cluster-scoped resource. The MCP tool
// returns YAML (with a trailing free-text "tip" section that callers strip
// via stripMCPTip in the processor).
//
// This is the single way the bridge reads cluster state — controller-runtime
// client.Get was removed in favour of "all cluster reads go through MCP" so
// the bridge SA can be scoped to Flux CRs only and operators have a single
// permission boundary to audit.
func (p *Pool) GetKubernetesResource(ctx context.Context, apiVersion, kind, name, namespace string) ([]byte, error) {
	args := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"name":       name,
	}
	if namespace != "" {
		args["namespace"] = namespace
	}
	text, err := p.callTextTool(ctx, "get_kubernetes_resources", args)
	if err != nil {
		return nil, err
	}
	return stripMCPTip([]byte(text)), nil
}

// ListKubernetesResources lists every resource of the given GVK in a
// namespace (or cluster-wide when namespace is empty). Returns YAML with a
// "---" separator between items, per the MCP tool's contract. Callers parse
// the YAML stream and filter client-side — the MCP tool does not accept a
// labelSelector argument today.
func (p *Pool) ListKubernetesResources(ctx context.Context, apiVersion, kind, namespace string) ([]byte, error) {
	args := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
	}
	if namespace != "" {
		args["namespace"] = namespace
	}
	text, err := p.callTextTool(ctx, "get_kubernetes_resources", args)
	if err != nil {
		return nil, err
	}
	return stripMCPTip([]byte(text)), nil
}

// stripMCPTip removes the trailing free-text "tip" section that
// flux-operator-mcp appends to YAML responses. The tip is human guidance
// for an LLM; it breaks YAML parsers, so we trim it at the Pool boundary
// instead of every caller having to remember.
// See: github.com/controlplaneio-fluxcd/flux-operator/cmd/mcp/toolbox/get_resource.go.
var mcpTipMarkers = []string{
	"\n\nIf asked for recommendations:",
	"\n\nWhen asked",
}

func stripMCPTip(data []byte) []byte {
	s := string(data)
	for _, marker := range mcpTipMarkers {
		if idx := strings.LastIndex(s, marker); idx >= 0 {
			return data[:idx]
		}
	}
	return data
}

// GetKustomizationStatus is a back-compat shim — the processor's
// inventory parsing uses it. New code should call GetKubernetesResource
// directly.
func (p *Pool) GetKustomizationStatus(ctx context.Context, name, namespace string) ([]byte, error) {
	return p.GetKubernetesResource(ctx, "kustomize.toolkit.fluxcd.io/v1", "Kustomization", name, namespace)
}

// GroupKindVersion is one entry in the cluster's discovery output: the
// preferred apiVersion currently exposed for a given (group, kind). Used
// by readiness.Resolver to pick the right version when the Kustomization
// inventory only carries the group (Flux inventory IDs drop the version).
type GroupKindVersion struct {
	Group   string
	Kind    string
	Version string
}

// GetKubernetesAPIVersions calls the MCP `get_kubernetes_api_versions` tool
// and parses the multi-doc YAML response into one entry per (group, kind).
// The bridge populates a (group,kind)→version cache from this at startup
// so non-v1 CRDs (e.g. example.com/v1alpha1) resolve correctly
// instead of silently misrouting to v1.
//
// flux-operator-mcp appends a free-text "tip" section to its YAML responses
// (same as get_kubernetes_resources) which would otherwise cause the YAML
// decoder to fail mid-stream and yield zero entries. stripMCPTip removes
// it before decoding.
func (p *Pool) GetKubernetesAPIVersions(ctx context.Context) ([]GroupKindVersion, error) {
	text, err := p.callTextTool(ctx, "get_kubernetes_api_versions", map[string]any{})
	if err != nil {
		return nil, err
	}
	return parseAPIVersionsYAML(stripMCPTip([]byte(text)))
}

func parseAPIVersionsYAML(data []byte) ([]GroupKindVersion, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var out []GroupKindVersion
	for {
		var entry GroupKindVersion
		if err := dec.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, fmt.Errorf("decode api versions yaml: %w", err)
		}
		// Skip empty separator documents — yaml.v3 emits the zero value for
		// stray `---` blocks (which the MCP output starts with).
		if entry.Kind == "" {
			continue
		}
		out = append(out, entry)
	}
}

// GetKubernetesLogs fetches a container's logs via MCP's get_kubernetes_logs
// tool. previous=true is the equivalent of `kubectl logs -p`, which is what
// you want for a crashlooping pod: the current container instance is just
// spinning up while the prior one held the actual crash output. The argument
// names depend on the server version (see resolveLogArgs).
func (p *Pool) GetKubernetesLogs(ctx context.Context, podName, namespace, container string, previous bool) (string, error) {
	args := func(names logArgNames) map[string]any {
		a := map[string]any{
			names.name:      podName,
			names.namespace: namespace,
			names.container: container,
		}
		if previous {
			a["previous"] = true
		}
		return a
	}
	names := p.resolveLogArgs(ctx)
	out, err := p.callTextTool(ctx, "get_kubernetes_logs", args(names))
	if err == nil || ctx.Err() != nil {
		return out, err
	}
	// The server may have changed since the names were resolved — upgraded
	// across the v0.61 rename, or a rolling update answered from an old pod
	// first — and now rejects them. Re-read its schema once and retry if the
	// names differ; a genuine log error (no such container) keeps the same
	// names and is returned as is.
	p.mu.Lock()
	p.logArgs = nil
	p.mu.Unlock()
	if fresh := p.resolveLogArgs(ctx); fresh != names {
		return p.callTextTool(ctx, "get_kubernetes_logs", args(fresh))
	}
	return out, err
}

// logArgNames are the get_kubernetes_logs argument names for pod, namespace
// and container.
type logArgNames struct{ name, namespace, container string }

var (
	// flux-operator-mcp up to v0.60: snake_case, container required.
	legacyLogArgs = logArgNames{name: "pod_name", namespace: "pod_namespace", container: "container_name"}
	// v0.61 renamed them (and rejects unknown keys, so the old names fail).
	currentLogArgs = logArgNames{name: "name", namespace: "namespace", container: "container"}
)

// resolveLogArgs reads the get_kubernetes_logs input schema and picks the
// matching argument names, so the bridge works against MCP servers on either
// side of the v0.61 rename. The result is cached until a log call fails (see
// GetKubernetesLogs). Until a lookup succeeds it falls back to the legacy
// names and tries again on the next call.
func (p *Pool) resolveLogArgs(ctx context.Context) logArgNames {
	p.mu.Lock()
	if p.logArgs != nil {
		names := *p.logArgs
		p.mu.Unlock()
		return names
	}
	p.mu.Unlock()

	names, resolved := legacyLogArgs, false
	_ = p.withRetry(ctx, func() (bool, error) {
		sess, release, err := p.acquire(ctx)
		if err != nil {
			return false, err
		}
		var callErr error
		for tool, err := range sess.inner.Tools(ctx, nil) {
			if err != nil {
				callErr = err
				break
			}
			if tool.Name != "get_kubernetes_logs" {
				continue
			}
			props, _ := schemaProperties(tool.InputSchema)
			_, hasLegacy := props[legacyLogArgs.name]
			_, hasCurrent := props[currentLogArgs.name]
			if hasCurrent && !hasLegacy {
				names = currentLogArgs
			}
			resolved = props != nil
			break
		}
		release(callErr)
		return retryableCallError(callErr), callErr
	})
	if resolved {
		p.mu.Lock()
		p.logArgs = &names
		p.mu.Unlock()
	}
	return names
}

// schemaProperties returns a JSON-schema object's "properties" map, as the
// SDK decodes it on the client side (generic JSON).
func schemaProperties(schema any) (map[string]any, bool) {
	m, ok := schema.(map[string]any)
	if !ok {
		return nil, false
	}
	props, ok := m["properties"].(map[string]any)
	return props, ok
}

// GetKubernetesMetrics fetches resource utilization (top-pods style output)
// for a namespace via MCP's `get_kubernetes_metrics` tool. Opt-in failure
// context — defaults off because raw CPU/mem numbers are hard for the LLM
// to interpret without alerting baselines.
//
// MCP schema (from upstream toolbox/get_metrics.go):
//
//	pod_namespace  string  (REQUIRED)
//	pod_name       string  (optional — omitted = all pods in namespace)
//	pod_selector   string  (optional)
//	limit          number  (optional, default 100)
//
// Note the snake_case keys — sending `namespace` would silently fail upstream
// schema validation (we caught the same bug in GetKubernetesLogs).
func (p *Pool) GetKubernetesMetrics(ctx context.Context, namespace string) (string, error) {
	return p.callTextTool(ctx, "get_kubernetes_metrics", map[string]any{
		"pod_namespace": namespace,
	})
}

// GetFluxInstance fetches the Flux Operator's FluxInstance resource via MCP's
// `get_flux_instance` tool. Useful for diagnosing cluster-wide Flux issues
// (controller OOMs, distribution skew) that surface as Kustomization
// failures but aren't caused by the Kustomization itself.
func (p *Pool) GetFluxInstance(ctx context.Context) (string, error) {
	return p.callTextTool(ctx, "get_flux_instance", map[string]any{})
}

// GetKubernetesEvents fetches events for one involved object via MCP's
// `get_kubernetes_events` tool (added upstream in flux-operator-mcp v0.59.0).
// For kind Deployment/StatefulSet/DaemonSet/CronJob/Job the server also
// resolves and folds in the events of the ReplicaSets/Jobs/pods it owns —
// but caps that expansion at its own newest-N pods (v0.60.0: 10), so a
// workload with more replicas can come back truncated; the caller decides
// whether that matters (e.g. falling back to a direct per-pod query). since,
// when non-zero, is sent as a Go duration string so only events newer than
// it are returned; zero omits the filter. limit bounds how many
// (newest-first) lines come back; <= 0 leaves the server default (100).
//
// The returned text is the tool's pre-rendered event lines — one per matched
// event, newest first, repeated identical events already collapsed
// server-side into a single line with an "xN" count — or "" when there are
// none. Unlike ListKubernetesResources(kind="Event"), the rendered line does
// NOT carry reportingComponent/source.component, so it can't feed the
// core-vs-third-party-noise classification ListKubernetesResources-based
// callers do; use this only where that classification isn't needed.
//
// truncated reports the tool's own `truncated` flag: either the workload's
// owned-pod resolution was capped, or the matched-event count exceeded limit
// (or the server's hard cap).
func (p *Pool) GetKubernetesEvents(ctx context.Context, kind, name, namespace string, since time.Duration, limit int) (events string, truncated bool, err error) {
	args := map[string]any{
		"kind":      kind,
		"name":      name,
		"namespace": namespace,
	}
	if since > 0 {
		args["since"] = since.String()
	}
	if limit > 0 {
		args["limit"] = limit
	}
	text, err := p.callTextTool(ctx, "get_kubernetes_events", args)
	if err != nil {
		return "", false, err
	}
	// "No events found" is the tool's plain-text sentinel for zero matches
	// (see upstream toolbox/get_events.go) — not YAML, so decode it specially
	// rather than failing yaml.Unmarshal on it.
	if strings.TrimSpace(text) == "No events found" {
		return "", false, nil
	}
	var result struct {
		Events    string `yaml:"events"`
		Truncated bool   `yaml:"truncated"`
	}
	if err := yaml.Unmarshal([]byte(text), &result); err != nil {
		return "", false, fmt.Errorf("decode get_kubernetes_events result: %w", err)
	}
	return strings.TrimRight(result.Events, "\n"), result.Truncated, nil
}

// Ping borrows a session and issues a lightweight call to verify
// connectivity. Used by /readyz. It never waits for a slot: queueing behind
// real work would time the probe out and pull the pod out of the Service
// exactly when it is busiest. A full pool counts as healthy while some MCP
// call succeeded recently (busyFreshness), not when every session is stuck.
func (p *Pool) Ping(ctx context.Context) error {
	return p.withRetry(ctx, func() (bool, error) {
		sess, release, err := p.acquireSlot(ctx, false)
		if errors.Is(err, errPoolBusy) {
			// A full pool is healthy only while calls keep completing: if
			// every session is stuck on an unresponsive server, nothing has
			// succeeded for a while and the bridge shouldn't look ready.
			if since := time.Since(time.Unix(0, p.lastOK.Load())); since > p.busyFreshness {
				return false, fmt.Errorf("mcp pool busy and no MCP call succeeded for %s", since.Round(time.Second))
			}
			return false, nil
		}
		if err != nil {
			return false, err
		}
		// ListTools is the canonical cheap MCP roundtrip.
		_, callErr := sess.inner.ListTools(ctx, &gosdk.ListToolsParams{})
		release(callErr)
		return retryableCallError(callErr), callErr
	})
}

// Close drains the pool, closing all idle sessions. In-flight callers see
// "mcp pool closed" on their next acquire.
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil
	}
	p.closing = true
	toClose := p.idle
	p.idle = nil
	p.created -= len(toClose)
	size := p.created
	p.mu.Unlock()
	metrics.MCPPoolSize.Set(float64(size))

	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		for _, s := range toClose {
			_ = s.inner.Close()
		}
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-closeCtx.Done():
		return closeCtx.Err()
	}
}

// maxCallAttempts bounds in-process retries within a single logical MCP call.
// Two attempts is enough to absorb a stale pooled connection: the first
// attempt's failure discards the dead session (see release), so the retry's
// acquire dials a fresh one. Transport-level errors only (see
// retryableCallError) — tool-level errors and bad responses are not retried.
const maxCallAttempts = 2

// withRetry runs fn up to maxCallAttempts times, retrying only when fn reports
// a retryable transport error and the caller's context is still alive. The
// failed attempt has already released (and thereby discarded) its broken
// session; before retrying, the remaining idle sessions are flushed too —
// pooled sessions go stale together (server restart, LB idle-timeout), so
// the session a retry would otherwise pop from the idle stack is almost
// certainly as dead as the one that just failed. Flushing forces the retry's
// acquire to dial a fresh connection.
func (p *Pool) withRetry(ctx context.Context, fn func() (retryable bool, err error)) error {
	var lastErr error
	for attempt := 1; attempt <= maxCallAttempts; attempt++ {
		retryable, err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable || attempt == maxCallAttempts || ctx.Err() != nil {
			break
		}
		p.flushIdle()
		metrics.MCPCallRetries.Inc()
	}
	return lastErr
}

// flushIdle closes and discards every idle session, leaving in-flight ones
// untouched. Used between retry attempts after a transport-level failure;
// discarded capacity is re-dialed on demand by the next acquire.
func (p *Pool) flushIdle() {
	p.mu.Lock()
	stale := p.idle
	p.idle = nil
	p.created -= len(stale)
	size := p.created
	p.mu.Unlock()
	metrics.MCPPoolSize.Set(float64(size))
	for _, s := range stale {
		go func(s *Session) { _ = s.inner.Close() }(s)
	}
}

func (p *Pool) callTextTool(ctx context.Context, name string, args map[string]any) (string, error) {
	var text string
	err := p.withRetry(ctx, func() (bool, error) {
		out, retryable, err := p.callTextToolOnce(ctx, name, args)
		text = out
		return retryable, err
	})
	return text, err
}

// callTextToolOnce performs a single MCP tool call. The bool return reports
// whether a non-nil error is a retryable transport failure (safe to retry on
// a fresh session); tool-level errors and malformed responses are not.
func (p *Pool) callTextToolOnce(ctx context.Context, name string, args map[string]any) (string, bool, error) {
	sess, release, err := p.acquire(ctx)
	if err != nil {
		return "", false, err
	}
	start := time.Now()
	result, callErr := sess.inner.CallTool(ctx, &gosdk.CallToolParams{Name: name, Arguments: args})
	metrics.MCPCallDuration.Observe(time.Since(start).Seconds())
	release(callErr)
	if callErr != nil {
		return "", retryableCallError(callErr), fmt.Errorf("call mcp tool %s: %w", name, callErr)
	}
	if result.IsError {
		// MCP servers put the human-readable error message in result.Content
		// as TextContent when IsError is true. Surface it instead of an
		// opaque "tool returned error" — the underlying message is what
		// callers need to diagnose ("pod has no running container",
		// "permission denied", etc.).
		if msg := errorMessage(result.Content); msg != "" {
			return "", false, fmt.Errorf("mcp tool %s returned error: %s", name, msg)
		}
		return "", false, fmt.Errorf("mcp tool %s returned error (no detail)", name)
	}
	if len(result.Content) == 0 {
		return "", false, fmt.Errorf("mcp tool %s returned no content", name)
	}
	text, ok := result.Content[0].(*gosdk.TextContent)
	if !ok {
		return "", false, fmt.Errorf("mcp tool %s returned unsupported content %T", name, result.Content[0])
	}
	return text.Text, false, nil
}

// errorMessage extracts the human-readable text from an MCP error response.
// Joins all TextContent entries with "; " so multi-segment errors stay
// readable. Returns "" when no text is present (shouldn't happen in
// practice but kept defensive).
func errorMessage(content []gosdk.Content) string {
	var parts []string
	for _, c := range content {
		if t, ok := c.(*gosdk.TextContent); ok && t.Text != "" {
			parts = append(parts, t.Text)
		}
	}
	return strings.Join(parts, "; ")
}
