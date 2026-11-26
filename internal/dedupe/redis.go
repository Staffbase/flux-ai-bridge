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

// Redis/Valkey-backed dedupe backend. Implements the same contract as the
// in-memory store (MarkPRIfNew / ForgetPR / ExpirePR and the side records)
// with plain keyspace commands, so multiple bridge replicas — and the same
// replica across restarts — share dedupe state.
//
// Wire-protocol compatible with Valkey, Redis, AWS ElastiCache, GCP
// Memorystore, and any other RESP server. The bridge chart bundles Valkey
// as an optional sub-chart for the self-contained install case; users with
// existing Redis-family infrastructure point DEDUPE_PERSISTENCE_URL at it.
//
// Fail-CLOSED: Redis errors surface to the caller, and the webhook handler
// / processor skip the event (or PR) rather than risk uncoordinated
// duplicate analysis and comments across replicas — pinned by
// TestRedis_UnreachableServerFailsClosed. During a Valkey outage the bridge
// goes quiet (no comments) instead of duplicating; the error path logs via
// the package's slog default so operators can spot the outage without
// inspecting metrics.
package dedupe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisStore satisfies the package's internal store interface using a Redis
// client. Cache's methods take Redis-specific paths (markIfNewAtomic,
// expire, …) instead of the generic Get-then-Add the in-memory LRU uses.
type redisStore struct {
	client *redis.Client
	// keyPrefix scopes our entries away from anything else sharing the
	// Valkey instance. Default "flux-ai-bridge:dedupe:" — configurable
	// via the constructor for multi-tenant clusters.
	keyPrefix string
	// opTimeout caps the wait on any single Redis call. Keep small so a
	// stuck Valkey doesn't block the event-processing goroutine for long.
	opTimeout time.Duration
	// maxTTL is the backstop expiry for records a caller asks to keep
	// forever (ttl <= 0): admission marks with no re-eval floor, outcome and
	// label records with re-eval off. Without it they accumulate for every
	// PR-head ever seen until Redis runs out of memory — and a server with
	// `noeviction` or a `volatile-*` policy can't evict keys that carry no
	// expiry, so writes start failing and dedupe fails closed. Zero keeps
	// the historic never-expire behavior. Set via Cache.WithMaxTTL.
	maxTTL time.Duration
	// lastFailure is the UnixNano time of the last failed admission write,
	// 0 once one succeeds again. Backs Cache.Available.
	lastFailure atomic.Int64
}

// bound applies the maxTTL backstop to a "never expire" ttl. A positive ttl
// is the caller's explicit choice and passes through unchanged.
func (r *redisStore) bound(ttl time.Duration) time.Duration {
	if ttl > 0 {
		return ttl
	}
	return r.maxTTL
}

// keyOf encodes the dedupe Key as "<prefix><ns>:<name>:pr<n>:<sha>". Kubernetes
// names can't contain ':', so the segments can't collide.
func (r *redisStore) keyOf(k Key) string {
	return r.keyPrefix + k.Namespace + ":" + k.Name + ":pr" + strconv.Itoa(k.PR) + ":" + k.SHA
}

func (r *redisStore) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), r.opTimeout)
}

// Get returns (timestamp, exists), reporting "not present" on a Redis error
// (logged). Part of the store interface; Cache's admission path uses
// markIfNewAtomic instead, which fails closed.
func (r *redisStore) Get(k Key) (time.Time, bool) {
	ctx, cancel := r.ctx()
	defer cancel()
	val, err := r.client.Get(ctx, r.keyOf(k)).Result()
	if err == redis.Nil {
		return time.Time{}, false
	}
	if err != nil {
		slog.Default().Warn("dedupe: redis GET failed, treating as miss (fail-open)",
			"namespace", k.Namespace, "name", k.Name, "sha", k.SHA, "error", err.Error())
		return time.Time{}, false
	}
	// Stored as RFC3339; if parse fails we treat the entry as present
	// (the timestamp value is informational, presence is what matters).
	if t, perr := time.Parse(time.RFC3339Nano, val); perr == nil {
		return t, true
	}
	return time.Time{}, true
}

// Add stores the key unconditionally, returning false on a Redis error
// (logged). Part of the store interface only — Cache never takes the
// non-atomic Get-then-Add path for Redis (see markIfNewAtomic).
func (r *redisStore) Add(k Key, t time.Time) bool {
	ctx, cancel := r.ctx()
	defer cancel()
	if err := r.client.Set(ctx, r.keyOf(k), t.Format(time.RFC3339Nano), r.bound(0)).Err(); err != nil {
		slog.Default().Warn("dedupe: redis SET failed",
			"namespace", k.Namespace, "name", k.Name, "sha", k.SHA, "error", err.Error())
		return false
	}
	return true
}

func (r *redisStore) Remove(k Key) bool {
	ctx, cancel := r.ctx()
	defer cancel()
	n, err := r.client.Del(ctx, r.keyOf(k)).Result()
	if err != nil {
		slog.Default().Warn("dedupe: redis DEL failed",
			"namespace", k.Namespace, "name", k.Name, "sha", k.SHA, "error", err.Error())
		return false
	}
	return n > 0
}

// ping reports whether the server answers, and clears lastFailure when it
// does so admission writes are attempted again.
func (r *redisStore) ping() bool {
	ctx, cancel := r.ctx()
	defer cancel()
	if err := r.client.Ping(ctx).Err(); err != nil {
		r.lastFailure.Store(time.Now().UnixNano())
		return false
	}
	r.lastFailure.Store(0)
	return true
}

// expire sets (or replaces) the record's expiry. Plain EXPIRE — no NX — so it
// also shortens a longer TTL; used for rate-limit cooldowns.
func (r *redisStore) expire(k Key, d time.Duration) {
	ctx, cancel := r.ctx()
	defer cancel()
	if err := r.client.Expire(ctx, r.keyOf(k), d).Err(); err != nil {
		slog.Default().Warn("dedupe: redis EXPIRE failed; mark keeps its previous expiry",
			"namespace", k.Namespace, "name", k.Name, "pr", k.PR, "sha", k.SHA, "error", err.Error())
	}
}

// outcomeKeyOf derives the last-outcome side-channel key from the admission
// key, kept in a distinct ":outcome" namespace so it never collides with the
// admission mark at the same coordinates.
func (r *redisStore) outcomeKeyOf(k Key) string { return r.keyOf(k) + ":outcome" }

// setOutcome records the last analysis outcome for a (PR, head). Best-effort:
// a failure is logged and swallowed because this only feeds the
// re-eval-failed-only optimization, never correctness. A positive ttl lets the
// record self-expire once the PR-head stops producing events; otherwise the
// maxTTL backstop applies.
func (r *redisStore) setOutcome(k Key, outcome string, ttl time.Duration) {
	ctx, cancel := r.ctx()
	defer cancel()
	if err := r.client.Set(ctx, r.outcomeKeyOf(k), outcome, r.bound(ttl)).Err(); err != nil {
		slog.Default().Warn("dedupe: redis SET outcome failed (best-effort; re-eval will re-poll)",
			"namespace", k.Namespace, "name", k.Name, "pr", k.PR, "sha", k.SHA, "error", err.Error())
	}
}

// getOutcome returns the recorded outcome for a (PR, head), or ("", false) on
// miss/error — both of which the caller treats as "must analyse".
func (r *redisStore) getOutcome(k Key) (string, bool) {
	ctx, cancel := r.ctx()
	defer cancel()
	v, err := r.client.Get(ctx, r.outcomeKeyOf(k)).Result()
	if err == redis.Nil {
		return "", false
	}
	if err != nil {
		slog.Default().Warn("dedupe: redis GET outcome failed, treating as miss (fail-open; re-eval will re-poll)",
			"namespace", k.Namespace, "name", k.Name, "pr", k.PR, "sha", k.SHA, "error", err.Error())
		return "", false
	}
	return v, true
}

// reevalKeyOf derives the re-eval counter key from the admission key, in its
// own ":reevals" namespace alongside the ":outcome" side-channel.
func (r *redisStore) reevalKeyOf(k Key) string { return r.keyOf(k) + ":reevals" }

// getReEval peeks the re-eval counter without incrementing or refreshing.
// 0 on miss or error (fail-open: no count, no cap).
func (r *redisStore) getReEval(k Key) int {
	ctx, cancel := r.ctx()
	defer cancel()
	v, err := r.client.Get(ctx, r.reevalKeyOf(k)).Int()
	if err != nil {
		if err != redis.Nil {
			slog.Default().Warn("dedupe: redis GET re-eval counter failed, treating as 0 (best-effort)",
				"namespace", k.Namespace, "name", k.Name, "pr", k.PR, "sha", k.SHA, "error", err.Error())
		}
		return 0
	}
	return v
}

// incrReEval bumps the (PR, head) re-eval counter and returns the new value.
// INCR + EXPIRE in a pipeline — both plain keyspace commands (no Lua; see
// markIfNewAtomic for why that matters here). The EXPIRE is refreshed on
// every call so an active head keeps counting across windows — including
// while the cap is already exceeded, otherwise the counter would expire
// mid-skip-phase and hand the head a fresh budget. Best-effort: on error
// returns 0, which the caller treats as "cap not reached" (degrades to
// uncapped re-eval, never blocks analysis).
func (r *redisStore) incrReEval(k Key, ttl time.Duration) int {
	ctx, cancel := r.ctx()
	defer cancel()
	rk := r.reevalKeyOf(k)
	pipe := r.client.Pipeline()
	incr := pipe.Incr(ctx, rk)
	if ttl = r.bound(ttl); ttl > 0 {
		pipe.Expire(ctx, rk, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Default().Warn("dedupe: redis INCR re-eval counter failed (best-effort; cap not enforced this event)",
			"namespace", k.Namespace, "name", k.Name, "pr", k.PR, "sha", k.SHA, "error", err.Error())
		return 0
	}
	return int(incr.Val())
}

// label-state HASH: one key per PR-head ("<prefix>label:<key>"), one field per
// Kustomization scope, value = last outcome. Backs the cross-Kustomization label
// aggregation. Best-effort: errors are logged and swallowed (labels degrade to
// last-known, never block the pipeline).
func (r *redisStore) labelKeyOf(key string) string { return r.keyPrefix + "label:" + key }

func (r *redisStore) hsetScopeOutcome(key, field, outcome string, ttl time.Duration) {
	if err := r.hset(key, field, outcome, ttl); err != nil {
		slog.Default().Warn("dedupe: redis HSET label-state failed (best-effort)",
			"key", key, "scope", field, "error", err.Error())
	}
}

// hset is hsetScopeOutcome with the backend error, for callers that must
// not carry on when the write didn't land.
func (r *redisStore) hset(key, field, value string, ttl time.Duration) error {
	ctx, cancel := r.ctx()
	defer cancel()
	rk := r.labelKeyOf(key)
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, rk, field, value)
	if ttl = r.bound(ttl); ttl > 0 {
		pipe.Expire(ctx, rk, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *redisStore) hdelScopeOutcome(key, field string) (bool, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	n, err := r.client.HDel(ctx, r.labelKeyOf(key), field).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (r *redisStore) hgetallScopeOutcomes(key string) map[string]string {
	m, err := r.hgetall(key)
	if err != nil {
		slog.Default().Warn("dedupe: redis HGETALL label-state failed, treating as empty (best-effort)",
			"key", key, "error", err.Error())
		return nil
	}
	return m
}

func (r *redisStore) hgetall(key string) (map[string]string, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	return r.client.HGetAll(ctx, r.labelKeyOf(key)).Result()
}

// markIfNewAtomic is the Redis-specific atomic check-and-set used by the
// Cache wrapper to avoid a Get-then-Set race across replicas. SET NX is
// the canonical RESP primitive for this.
//
// Returns (true, nil) if the key was NOT present (i.e. this caller "wins"
// the admission slot), (false, nil) if the key was already present
// (duplicate), and (false, err) when the Redis op fails (network, auth,
// timeout). The Cache wrapper surfaces this error to the processor, which
// then fails CLOSED — the previous fail-open behavior had a much
// worse failure mode: with Redis broken, every Flux re-fire (~12/hour)
// re-ran the LLM and reposted the same comment because in-process dedupe
// state was the only thing left, and that doesn't survive across
// replicas. Closed > open here: the bridge stops processing during a
// Redis outage, recovers automatically when Redis comes back.
// ttl bounds how long the admission record survives: 0 means "no re-eval
// floor" (the commit-driven contract) and falls back to the maxTTL backstop, a
// positive value makes the key auto-expire so the next identical event after
// the window is admitted again (the per-PR re-evaluation floor — see
// Cache.MarkPRIfNew).
//
// When the effective ttl is positive and the key already exists WITHOUT an
// expiry, it is attached via EXPIRE NX. Such keys are marks written before the
// floor or the backstop was configured (SETNX never updates an existing key's
// TTL) — left alone they'd dedupe forever, so PRs analysed before an operator
// enabled DEDUPE_PR_REEVAL_TTL would never re-evaluate at their current head,
// and marks from before the backstop would never be reclaimed. With the expiry
// attached, the mark self-frees after one window and the next event re-admits
// via SETNX.
// Deliberately NOT a Lua compare-and-swap on the stored timestamp (which would
// re-admit on the next event instead of one window later): managed Redis ACLs
// commonly deny EVAL/EVALSHA — seen in production as "NOPERM ... has no
// permissions to run the 'evalsha' command" — while EXPIRE is part of the
// plain keyspace command set. NX keeps it idempotent across racing replicas
// and leaves marks that already carry an expiry untouched. The NX modifier
// requires Redis ≥ 7.0 (any Valkey version qualifies — Valkey forked from
// Redis 7.2); on an older server the call fails and is WARN-logged, leaving
// pre-floor marks deduping forever — the normal TTL-based re-eval is
// unaffected, since SETNX sets the expiry itself.
func (r *redisStore) markIfNewAtomic(k Key, t time.Time, ttl time.Duration) (bool, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	key := r.keyOf(k)
	ttl = r.bound(ttl)
	ok, err := r.client.SetNX(ctx, key, t.Format(time.RFC3339Nano), ttl).Result()
	if err != nil {
		r.lastFailure.Store(time.Now().UnixNano())
		// Log here at WARN so operators have an event-level trail of
		// failures even when the caller doesn't surface every single
		// per-event error. The webhook handler is the place that
		// classifies + metricises + skips.
		slog.Default().Warn("dedupe: redis SETNX failed; webhook will fail closed on this event",
			"namespace", k.Namespace, "name", k.Name, "sha", k.SHA, "error", err.Error())
		return false, err
	}
	r.lastFailure.Store(0)
	if ok || ttl <= 0 {
		return ok, nil
	}
	// Best-effort: the admission answer is already "duplicate" either way; a
	// failure here (an ACL that also denies EXPIRE, or a pre-7.0 Redis that
	// doesn't know the NX modifier) only delays the pre-floor migration, so
	// log loudly and keep the event flowing rather than failing closed.
	if eerr := r.client.ExpireNX(ctx, key, ttl).Err(); eerr != nil {
		slog.Default().Warn("dedupe: redis EXPIRE NX for re-eval floor failed; pre-floor mark keeps deduping (best-effort; requires Redis 7+/Valkey)",
			"namespace", k.Namespace, "name", k.Name, "sha", k.SHA, "error", eerr.Error())
	}
	return false, nil
}

// NewRedis constructs a Cache backed by a Redis/Valkey/RESP-compatible
// server. url is a redis:// or rediss:// URL (the latter for TLS). username
// and password are optional; pass "" for unauthenticated Valkey (the bundled
// sub-chart's default). When non-empty they override anything embedded in the
// URL — useful when credentials come from a Secret separate from the URL.
// The function pings the server once to surface obvious errors early;
// admission writes fail closed on per-call errors (see markIfNewAtomic).
//
// keyPrefix scopes our entries; defaults to "flux-ai-bridge:dedupe:" when
// empty. opTimeout caps individual Redis calls; defaults to 2s when zero.
//
// This is the single-node path. For HA see NewRedisSentinel.
func NewRedis(url, username, password, keyPrefix string, opTimeout time.Duration) (*Cache, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	if username != "" {
		opt.Username = username
	}
	if password != "" {
		opt.Password = password
	}
	// Without this go-redis ignores the context deadline during socket I/O,
	// so opTimeout wouldn't cap a call on a stalled connection.
	opt.ContextTimeoutEnabled = true
	return finishRedisCache(redis.NewClient(opt), keyPrefix, opTimeout)
}

// SentinelOptions configures a Redis Sentinel-backed Cache. Sentinel sits in
// front of a Redis master+replicas set and handles failover; the go-redis
// FailoverClient connects to the Sentinel quorum, asks "which node is master
// for <MasterName>?", and routes every command to the answer. On master
// failover (when down-after-milliseconds elapses and the quorum elects a new
// master) the client transparently re-routes to the new master without the
// bridge having to reconnect.
//
// Two distinct credential pairs because Sentinel and the Redis backend may
// have separate ACL files / users in practice (and frequently do — Bitnami's
// chart wires them that way by default):
//
//   - Username/Password apply to the Redis master + replicas (AUTH user pass).
//   - SentinelUsername/SentinelPassword apply to the Sentinel daemons
//     themselves (AUTH user pass on the sentinel connection). Leave both
//     empty if your sentinel quorum is unauthenticated.
//
// UseTLS enables TLS on both the sentinel and master connections. Sentinel
// doesn't have separate rediss:// / redis-sentinel:// URL schemes the way
// single-node does, so it's a flag rather than a URL scheme.
type SentinelOptions struct {
	Addrs            []string // ["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"]
	MasterName       string   // the sentinel `mymaster` label
	DB               int      // logical Redis DB; 0 unless you cohabit with other workloads
	Username         string   // Redis backend ACL user (the master+replicas)
	Password         string   // Redis backend ACL password
	SentinelUsername string   // sentinel daemon ACL user (often the same value, but distinct)
	SentinelPassword string   // sentinel daemon ACL password
	UseTLS           bool
	KeyPrefix        string
	OpTimeout        time.Duration
}

// NewRedisSentinel constructs a Cache backed by a Redis Sentinel cluster.
// See SentinelOptions for the credential model. Like NewRedis this pings
// once at construction and logs (rather than crash-loops) on transient
// reachability errors — but it elevates permanent auth-class errors to
// ERROR, since the bridge then fails closed and posts nothing until the
// credentials are fixed.
func NewRedisSentinel(opts SentinelOptions) (*Cache, error) {
	if opts.MasterName == "" {
		return nil, errors.New("sentinel: MasterName is required")
	}
	if len(opts.Addrs) == 0 {
		return nil, errors.New("sentinel: at least one address is required in Addrs")
	}
	// Per-entry validation. Empty strings sneak in when a Helm value
	// renders as `""` (e.g. an unset list-item interpolation); go-redis
	// then fails at first command with an opaque error. Each entry must
	// be `host:port` form so we catch missing ports here too (Sentinel's
	// default 26379 isn't auto-applied by go-redis).
	for i, a := range opts.Addrs {
		t := strings.TrimSpace(a)
		if t == "" {
			return nil, fmt.Errorf("sentinel: Addrs[%d] is empty", i)
		}
		if !strings.Contains(t, ":") {
			return nil, fmt.Errorf("sentinel: Addrs[%d] %q is missing a port (expected host:port, typically host:26379)", i, t)
		}
		opts.Addrs[i] = t
	}
	fo := &redis.FailoverOptions{
		ContextTimeoutEnabled: true, // honour opTimeout during socket I/O (see NewRedis)
		MasterName:            opts.MasterName,
		SentinelAddrs:         opts.Addrs,
		DB:                    opts.DB,
		Username:              opts.Username,
		Password:              opts.Password,
		SentinelUsername:      opts.SentinelUsername,
		SentinelPassword:      opts.SentinelPassword,
	}
	if opts.UseTLS {
		// MinVersion: TLS 1.2 matches Redis 6+'s baseline. Cert verification
		// uses the system roots; operators with private CAs should mount
		// them into the pod's trust store rather than disabling verify here.
		fo.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return finishRedisCache(redis.NewFailoverClient(fo), opts.KeyPrefix, opts.OpTimeout)
}

// finishRedisCache pings the freshly-constructed client and wraps it in a
// Cache. Shared between NewRedis and NewRedisSentinel so the startup-error
// classification (auth vs network) is identical regardless of topology.
func finishRedisCache(client *redis.Client, keyPrefix string, opTimeout time.Duration) (*Cache, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		// Two classes of failure with very different ergonomics:
		//   - Auth / permission errors won't fix themselves; the bridge fails
		//     closed (events are skipped while dedupe errors), so a
		//     misconfigured Secret means NO comments until it's fixed — and
		//     the operator can't tell from the bridge's healthz. Log at ERROR
		//     with a pointer to the likely cause (Secret contents, ACL config).
		//   - Network errors (refused, timeout, DNS) are typically transient
		//     during a rolling restart of the Redis/Sentinel stack. WARN and
		//     let the bridge keep running.
		if isAuthError(err) {
			slog.Default().Error("dedupe: redis AUTH rejected at startup; bridge will fail closed (events skipped) until this is fixed. Check that DEDUPE_PERSISTENCE_USER / DEDUPE_PERSISTENCE_PASSWORD (and SENTINEL_* counterparts in sentinel mode) match the Redis/Sentinel ACL config exactly — a trailing newline in the Secret value is the most common cause",
				"error", err.Error())
		} else {
			slog.Default().Warn("dedupe: redis Ping failed at startup; events will be skipped (fail-closed) until the server is reachable",
				"error", err.Error())
		}
	}
	if keyPrefix == "" {
		keyPrefix = "flux-ai-bridge:dedupe:"
	}
	if opTimeout <= 0 {
		opTimeout = 2 * time.Second
	}
	return &Cache{s: &redisStore{client: client, keyPrefix: keyPrefix, opTimeout: opTimeout}}, nil
}

// isAuthError matches the auth-class error replies Redis returns when ACL
// rejects a connection. Both servers and the go-redis client surface these
// as plain strings on the underlying error, so prefix matching on the
// canonical reply codes is the most portable way to classify them.
//
// The "disabled user" reply from Redis is `-ERR User <name> is disabled`,
// with the username sitting between "User" and "is disabled" — so the
// literal "USER IS DISABLED" substring never matches. We check the
// trailing "IS DISABLED" alone, which is stable regardless of the user
// name. WRONGPASS / NOAUTH / NOPERM are stable codes used across Redis
// 6 and 7.
func isAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToUpper(err.Error())
	switch {
	case strings.Contains(msg, "WRONGPASS"):
		return true
	case strings.Contains(msg, "NOAUTH"):
		return true
	case strings.Contains(msg, "NOPERM"):
		return true
	case strings.Contains(msg, "IS DISABLED"):
		return true
	}
	return false
}
