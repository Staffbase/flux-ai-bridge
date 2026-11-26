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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	bridgeconfig "github.com/staffbase/flux-ai-bridge/internal/config"
	"github.com/staffbase/flux-ai-bridge/internal/dedupe"
	"github.com/staffbase/flux-ai-bridge/internal/fetchcache"
	bridgegithub "github.com/staffbase/flux-ai-bridge/internal/github"
	"github.com/staffbase/flux-ai-bridge/internal/kustomizations"
	"github.com/staffbase/flux-ai-bridge/internal/llm"
	bridgemcp "github.com/staffbase/flux-ai-bridge/internal/mcp"
	"github.com/staffbase/flux-ai-bridge/internal/metrics"
	"github.com/staffbase/flux-ai-bridge/internal/processor"
	"github.com/staffbase/flux-ai-bridge/internal/readiness"
	"github.com/staffbase/flux-ai-bridge/internal/skills"
	bridgesource "github.com/staffbase/flux-ai-bridge/internal/source"
	"github.com/staffbase/flux-ai-bridge/internal/webhook"
)

// shutdownUnwind is the share of SHUTDOWN_TIMEOUT reserved for cancelled
// pipelines to run their rollbacks (capped at half the timeout).
const shutdownUnwind = 10 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLogLevel(os.Getenv("LOG_LEVEL"))}))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("startup failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	metrics.Init()

	cfg, err := bridgeconfig.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// The bridge does not read Kubernetes resources directly. All cluster
	// reads (Flux CRs, inventory resources, pods/workloads for failure
	// context) go through flux-operator-mcp, so we never need a typed
	// Kubernetes client or a RESTMapper.

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startupCancel()
	mcpPoolSize := parseIntEnv("MCP_POOL_SIZE", parseIntEnv("WEBHOOK_MAX_CONCURRENT", 16))
	mcpClient, err := bridgemcp.NewPool(startupCtx, cfg.MCPURL, mcpPoolSize)
	if err != nil {
		return fmt.Errorf("create mcp client: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mcpClient.Close(closeCtx); err != nil {
			logger.Error("failed to close mcp client", "error", err)
		}
	}()

	// Build the (group,kind)→apiVersion resolver from MCP discovery so non-v1
	// CRDs (e.g. example.com/v1alpha1) route to the correct GVR.
	// Flux Kustomization inventory IDs omit the version, so without discovery
	// the bridge would guess "v1" and every non-v1 CRD would silently misroute.
	// On discovery failure, log loudly and continue without a resolver — the
	// Checker's hardcoded fallback table covers Flux's own CRDs, which is
	// enough for the bridge to stay functional until the next restart picks
	// up discovery.
	resolver, err := buildResolver(startupCtx, mcpClient)
	if err != nil {
		logger.Warn("discovery: get_kubernetes_api_versions failed; falling back to hardcoded preferred-versions table — non-v1 CRDs may misroute until next restart",
			"error", err.Error())
	}
	checker := readiness.NewChecker(mcpClient).WithLogger(logger).WithResolver(resolver)
	// LLM_PROVIDER=none keeps analyzer nil so the processor builds the PR
	// comment from kstatus output alone — no API call, no API key required.
	var analyzer llm.Analyzer
	if cfg.LLMProvider != "none" {
		analyzer, err = llm.NewAnalyzer(cfg)
		if err != nil {
			return fmt.Errorf("create llm analyzer: %w", err)
		}
	} else {
		logger.Info("LLM disabled (LLM_PROVIDER=none); PR comments will contain kstatus output only")
	}
	authCfg, err := bridgegithub.AuthFromEnv(cfg.GitHubAPIURL)
	if err != nil {
		return fmt.Errorf("github auth config: %w", err)
	}
	// Dedupe is constructed BEFORE the commenter so the fetchcache can
	// reuse the same Redis/Valkey connection. With in-memory dedupe the
	// fetchcache falls back to its own in-memory LRU; with Redis-backed
	// dedupe the fetchcache shares the connection and gets persistence
	// + cross-replica sharing for free.
	// An instance ID keeps bridges from different clusters apart when they
	// share a Redis: each gets its own dedupe and fetch-cache keyspace.
	keyPrefix := "flux-ai-bridge:"
	if cfg.InstanceID != "" {
		keyPrefix += cfg.InstanceID + ":"
	}
	dedupeCache, err := newDedupe(logger, keyPrefix+"dedupe:")
	if err != nil {
		return err
	}
	dedupeCache = dedupeCache.WithMaxTTL(cfg.DedupePersistenceMaxTTL)
	fetchCache := newFetchCache(dedupeCache, keyPrefix+"fetchcache:", cfg.DedupePersistenceMaxTTL, logger)
	commenter, err := bridgegithub.NewCommenter(authCfg, bridgegithub.Options{
		KeepCommentHistory: cfg.GitHubKeepCommentHistory,
		InstanceID:         cfg.InstanceID,
		Cache:              fetchCache,
		// Unset/0 values fall back to the commenter's defaults.
		ParentWalkDepth: parseIntEnv("PARENT_WALK_DEPTH", 0),
		PRsForCommitTTL: parseDurationEnv("PRS_FOR_COMMIT_TTL", 0),
		// Growth bound on the immutable commit-parents cache (default 1h
		// inside the commenter; a negative duration like "-1s" disables it).
		CommitTTL: parseDurationEnv("COMMIT_TTL", 0),
		// Lets the commenter cache the DERIVED resource IDs of a PR file
		// instead of its raw manifest bytes — keeps file contents (potential
		// Secret/ConfigMap bodies) out of the fetchcache/Redis.
		BlobResourceParser: processor.ResourceIDsFromYAML,
		// Configurable outcome-label names (empty → flux-success / flux-failed /
		// flux-warning). Set warning == failed to make a warning block a gate.
		PRLabelSuccess: cfg.PRLabelSuccess,
		PRLabelFailure: cfg.PRLabelFailed,
		PRLabelWarning: cfg.PRLabelWarning,
	})
	if err != nil {
		return fmt.Errorf("create github commenter: %w", err)
	}
	skillDir := os.Getenv("SKILLS_DIR")
	if skillDir == "" {
		skillDir = "/skills"
	}
	skillLoader, err := skills.NewLoader(skillDir)
	if err != nil {
		return fmt.Errorf("load skills from %s: %w", skillDir, err)
	}
	logger.Info("skills loaded", "dir", skillDir, "loaded", skillLoader.Loaded())

	// dedupeCache is constructed above (before the commenter) so the
	// fetchcache can share its Redis/Valkey client. DEDUPE_TTL semantics:
	//   - unset (default)        → no expiry. Entries live until the LRU
	//                               bound, a pipeline-error rollback, or a
	//                               restart: one analysis per PR head.
	//   - positive duration      → entries expire after that duration.
	//                               Use this when you want periodic refresh
	//                               posts (e.g. DEDUPE_TTL=1h).
	//   - "0"                    → dedupe disabled entirely.

	proc := processor.New(cfg, mcpClient, checker, analyzer, commenter, skillLoader, dedupeCache, logger)
	// Exact PR attribution: resolve each Kustomization's build file set from the
	// source-controller artifact Flux actually reconciled (overlay-correct,
	// layout-independent), reusing the discovery resolver for source apiVersions.
	proc = proc.WithScopeResolver(bridgesource.NewResolver(mcpClient, resolver.APIVersion, &http.Client{Timeout: 2 * time.Minute}))
	// Per-event pipeline budget = readiness poll budget + 2m slack for
	// MCP/LLM/GitHub calls. Wrapping the pipeline in a smaller ctx than the
	// readiness poll itself would cause every poll-until-settled to abort
	// with "context deadline exceeded" just before the workload converged.
	// Multi-replica coordination is via the dedupe cache (Redis/Valkey SETNX
	// is atomic across pods). Every replica accepts every webhook; the
	// processor admits each (Kustomization, PR, PR head) on exactly one
	// replica, before the readiness poll and LLM call. See
	// charts/flux-ai-bridge/values.yaml `valkey` and
	// `config.dedupe.persistence` for the invariant the chart enforces.

	// Build the Kustomization-name admission filter. Patterns were validated
	// in config.Load (fail-fast on bad globs), so an error here can only mean
	// the validator drifted from this constructor.
	filter, err := kustomizations.New(cfg.KustomizationsInclude, cfg.KustomizationsExclude)
	if err != nil {
		return fmt.Errorf("kustomizations filter: %w", err)
	}
	if len(cfg.KustomizationsInclude) > 0 || len(cfg.KustomizationsExclude) > 0 {
		logger.Info("kustomizations filter active",
			"include", cfg.KustomizationsInclude,
			"exclude", cfg.KustomizationsExclude)
	}

	if cfg.InstanceID != "" && cfg.PRLabelEnabled &&
		cfg.PRLabelSuccess == "" && cfg.PRLabelFailed == "" && cfg.PRLabelWarning == "" {
		// Comments and Redis state are scoped by INSTANCE_ID; labels are
		// repository-wide and can't be.
		logger.Info("PR labels use the shared default names; if other instances label PRs of the same repositories, give each its own PR_LABEL_* names, or they overwrite each other's verdict",
			"instance_id", cfg.InstanceID)
	}
	if os.Getenv("LLM_STYLE") != "" {
		logger.Warn("LLM_STYLE is no longer supported and is ignored; comments always use ✅/❌/⚠️ markers")
	}
	hmacSecret := os.Getenv("WEBHOOK_HMAC_SECRET")
	if hmacSecret == "" {
		logger.Warn("WEBHOOK_HMAC_SECRET is not set; /webhook accepts unsigned events from anyone who can reach it")
	}
	handler := webhook.NewHandler(proc, logger, webhook.Options{
		MaxConcurrent:     parseIntEnv("WEBHOOK_MAX_CONCURRENT", 16),
		HMACSecret:        hmacSecret,
		Filter:            filter,
		ProcessingTimeout: cfg.ReadinessPollTimeout + 2*time.Minute,
	})

	mux := http.NewServeMux()
	mux.Handle("/webhook", handler)
	mux.Handle("/metrics", metrics.Handler())
	// Liveness: process is up. Static 200; deliberately no external checks
	// (we don't want Anthropic/GitHub blips to pod-bounce the bridge).
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	// Readiness: the bridge can do useful work (MCP reachable). Every replica
	// reports Ready so the Service keeps all endpoints in its loadbalancer
	// and `kubectl get pods` never shows "0/1 Ready" (which fires alerts).
	// Coordination across replicas is active-active via the persistent
	// dedupe cache (Redis/Valkey SETNX) — every replica accepts every
	// webhook and the SETNX admits exactly one per (ns, name, PR, PR head).
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		w.Header().Set("Content-Type", "application/json")
		if err := mcpClient.Ping(ctx); err != nil {
			logger.Warn("readiness probe failed", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unready","reason":"mcp"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.WebhookPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// Bodies are at most 1 MiB and every handler answers immediately
		// (the pipeline runs asynchronously), so a client trickling a body
		// or a response can't hold a connection open.
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  2 * time.Minute,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting webhook server", "port", cfg.WebhookPort)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down", "signal", sig.String())
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	}

	// SHUTDOWN_TIMEOUT is the whole budget: in-flight pipelines get it minus
	// the unwind reserve to finish on their own, then they're cancelled and
	// the reserve covers their rollbacks — all before the kubelet's SIGKILL.
	unwind := min(shutdownUnwind, cfg.ShutdownTimeout/2)
	drainCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout-unwind)
	defer cancel()
	if err := server.Shutdown(drainCtx); err != nil {
		logger.Error("failed to shutdown http server", "error", err)
	}
	if err := handler.Shutdown(drainCtx, unwind); err != nil {
		logger.Error("in-flight webhook processing did not finish", "error", err)
	}
	return nil
}

func parseLogLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func parseIntEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

// parseDurationEnv reads a duration string (e.g. "60s", "5m") from env
// and returns it. Empty/unparseable env yields the fallback. Used for
// cache TTL knobs where empty = "use the package default" rather than
// "disable the cache."
func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return v
}

// newFetchCache builds the GitHub-fetch cache used by the commenter for
// blob content, commit metadata, and listPRsForCommit. The backend
// follows the dedupe configuration:
//
//   - When dedupeCache is backed by Redis/Valkey (single-node or
//     Sentinel), the fetchcache shares that connection — persistent,
//     cross-replica, survives restart. Same auth + same connection
//     pool the operator already configured for dedupe.
//   - Otherwise (in-memory dedupe), the fetchcache uses its own
//     in-memory LRU bounded by FETCH_CACHE_CAPACITY (default 4096).
//
// Capacity is per-cache-kind across blob/commit/prs-for-commit
// keyspaces sharing one LRU — operators can tune via env if their
// working set exceeds the default.
func newFetchCache(dedupeCache *dedupe.Cache, keyPrefix string, maxTTL time.Duration, logger *slog.Logger) fetchcache.Cache {
	if client := dedupeCache.RedisClient(); client != nil {
		logger.Info("fetchcache: using shared Redis/Valkey connection from dedupe persistence")
		return fetchcache.NewRedis(client, keyPrefix, 2*time.Second, maxTTL)
	}
	capacity := parseIntEnv("FETCH_CACHE_CAPACITY", 4096)
	logger.Info("fetchcache: using in-memory LRU", "capacity", capacity)
	return fetchcache.NewLRU(capacity)
}

// newDedupe picks the dedupe backend based on env vars:
//
//   - DEDUPE_PERSISTENCE_SENTINEL_ADDRS set → Redis Sentinel HA backend.
//     Requires DEDUPE_PERSISTENCE_MASTER_NAME. Backend creds come from
//     DEDUPE_PERSISTENCE_USER + _PASSWORD; sentinel-daemon creds from
//     DEDUPE_PERSISTENCE_SENTINEL_USER + _SENTINEL_PASSWORD (often the
//     same values but configured independently because the sentinel ACL
//     file is typically distinct). DEDUPE_PERSISTENCE_TLS=true enables
//     TLS on both legs. The bridge transparently follows master failover.
//   - DEDUPE_PERSISTENCE_URL set            → single-node Redis/Valkey.
//     Auth user+password from DEDUPE_PERSISTENCE_USER + _PASSWORD; unset
//     means unauthenticated. No failover handling — pointing at a
//     specific master pod will break when sentinel promotes a new one.
//   - neither set                            → in-memory backend, picked
//     by DEDUPE_TTL:
//   - unset (default)  → NewNoExpiry
//   - "0"              → New(0, …) (disabled)
//   - positive value   → New(d, …) (TTL)
//
// Credential env vars are TrimSpaced because operators frequently create
// Kubernetes Secrets from files that carry a trailing newline; sending
// "<pass>\n" to Redis AUTH returns WRONGPASS even though the password is
// "correct."
//
// A Redis construction error (a malformed URL, a missing master name) is
// returned and stops startup: falling back to a per-process in-memory cache
// would let every replica admit the same PR-head on its own, which breaks
// fail closed. A Valkey that is merely down at startup does not reach this
// path: the client is still built, and admission fails closed per call
// until the server answers. Auth errors at startup are logged at ERROR (not
// WARN) by the dedupe package so the operator notices.
func newDedupe(logger *slog.Logger, keyPrefix string) (*dedupe.Cache, error) {
	const capacity = 4096
	username := strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_USER"))
	password := strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_PASSWORD"))

	if rawAddrs := strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_SENTINEL_ADDRS")); rawAddrs != "" {
		var addrs []string
		for _, a := range strings.Split(rawAddrs, ",") {
			if t := strings.TrimSpace(a); t != "" {
				addrs = append(addrs, t)
			}
		}
		opts := dedupe.SentinelOptions{
			Addrs:            addrs,
			MasterName:       strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_MASTER_NAME")),
			DB:               parseIntEnv("DEDUPE_PERSISTENCE_DB", 0),
			Username:         username,
			Password:         password,
			SentinelUsername: strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_SENTINEL_USER")),
			SentinelPassword: strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_SENTINEL_PASSWORD")),
			UseTLS:           strings.EqualFold(strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_TLS")), "true"),
			KeyPrefix:        keyPrefix,
		}
		c, err := dedupe.NewRedisSentinel(opts)
		if err != nil {
			return nil, fmt.Errorf("dedupe: DEDUPE_PERSISTENCE_SENTINEL_ADDRS is set but the sentinel client could not be built: %w", err)
		}
		logger.Info("dedupe: sentinel persistence enabled",
			"master", opts.MasterName, "sentinels", len(opts.Addrs), "tls", opts.UseTLS)
		return c, nil
	}
	if url := strings.TrimSpace(os.Getenv("DEDUPE_PERSISTENCE_URL")); url != "" {
		c, err := dedupe.NewRedis(url, username, password, keyPrefix, 0)
		if err != nil {
			return nil, fmt.Errorf("dedupe: DEDUPE_PERSISTENCE_URL is set but the client could not be built: %w", withoutURL(err))
		}
		logger.Info("dedupe: persistence enabled", "url_host", redactedHost(url))
		return c, nil
	}
	raw := strings.TrimSpace(os.Getenv("DEDUPE_TTL"))
	if raw == "" {
		return dedupe.NewNoExpiry(capacity), nil
	}
	if raw == "0" {
		return dedupe.New(0, capacity), nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		// Garbled value → behave like unset rather than crash.
		logger.Warn("dedupe: DEDUPE_TTL unparseable; treating as unset (no expiry)", "value", raw)
		return dedupe.NewNoExpiry(capacity), nil
	}
	if d <= 0 {
		// "0s" (or any zero/negative duration) means the same as "0":
		// dedupe disabled. Previously only the literal "0" disabled it and
		// "0s" silently flipped to no-expiry — the exact opposite.
		return dedupe.New(0, capacity), nil
	}
	return dedupe.New(d, capacity), nil
}

// withoutURL drops the raw URL from a URL parse error. url.Error quotes the
// whole input, userinfo included, so a malformed DEDUPE_PERSISTENCE_URL
// would put the Redis password into the startup log. The inner error still
// names what is wrong (e.g. an invalid port).
func withoutURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// buildResolver populates the readiness Resolver from MCP discovery. Returns
// a nil resolver alongside the error on failure — callers can still pass nil
// to WithResolver and the Checker will fall back to its hardcoded table. The
// returned resolver carries a refresher so it can pick up CRDs registered after
// startup (re-running discovery on demand, debounced).
func buildResolver(ctx context.Context, mcpClient *bridgemcp.Pool) (*readiness.Resolver, error) {
	fetch := func(ctx context.Context) ([]readiness.VersionEntry, error) {
		entries, err := mcpClient.GetKubernetesAPIVersions(ctx)
		if err != nil {
			return nil, err
		}
		converted := make([]readiness.VersionEntry, len(entries))
		for i, e := range entries {
			converted[i] = readiness.VersionEntry{Group: e.Group, Kind: e.Kind, Version: e.Version}
		}
		return converted, nil
	}
	entries, err := fetch(ctx)
	if err != nil {
		return nil, err
	}
	return readiness.NewResolver(entries).WithRefresher(fetch), nil
}

// redactedHost strips userinfo from a redis URL so a log line never leaks
// the password. Returns the raw URL on parse failure (best-effort).
func redactedHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<unparseable>"
	}
	u.User = nil
	return u.String()
}
