---
# Rendered by `make e2e-up` via envsubst. The Makefile sets all of these
# either to mock-mode defaults or, when E2E_REAL_GITHUB / E2E_LLM is set,
# to real-mode values.
#
# Variables:
#   E2E_ANTHROPIC_API_KEY  — Anthropic key (mock value unless real Anthropic is used)
#   E2E_GITHUB_REPO        — owner/repo passed as bridge config.github.repo
#   E2E_GITHUB_API_URL     — bridge config.github.apiUrl (empty = default api.github.com)
#   E2E_GITHUB_API_ALLOW_INSECURE — bridge config.github.allowInsecure ("true" for the plain-HTTP mock)
#   E2E_KEEP_COMMENT_HISTORY — bridge config.github.comments.keepHistory ("true"/"false"; default "false")
#   E2E_LLM_PROVIDER       — bridge config.llm.provider (claude | openai-compatible)
#   E2E_LLM_MODEL          — bridge config.llm.model (provider-specific name)
#   E2E_ANTHROPIC_BASE_URL — bridge config.llm.anthropicBaseUrl (empty = api.anthropic.com)
#   E2E_OPENAI_BASE_URL    — bridge config.llm.openaiBaseUrl (empty = auto-derive when ollama.enabled)
#   E2E_OLLAMA_ENABLED     — "true" to install the bundled Ollama sub-chart, "false" otherwise
#   E2E_OPENAI_API_KEY     — only used by the Azure AI mode (E2E_LLM=azure); empty otherwise
#   E2E_PODINFO_GIT_URL    — GitRepository URL for the podinfo test workload
#                            (mock: in-cluster git-server; real-GitHub: github.com/$GITHUB_REPO)
#   E2E_PODINFO_GIT_REF    — branch the podinfo GitRepository tracks (typically "main")
#   E2E_PODINFO_GIT_USER   — username for source-controller HTTPS auth (empty in mock mode)
#   E2E_PODINFO_GIT_PASS   — token for source-controller HTTPS auth (empty in mock mode)
#   E2E_PODINFO_PATH       — Kustomization spec.path (typically "./e2e/podinfo-kustomize")
apiVersion: v1
kind: Secret
metadata:
  name: flux-ai-bridge-e2e-creds
  namespace: flux-system
type: Opaque
stringData:
  # GitHub App credentials — the bridge has no PAT auth path. In mock-github
  # mode the Makefile fills these with throwaway values (App ID 1, a
  # generated RSA key under e2e/fixtures/mock-app-key.pem); in real-GitHub
  # mode they come from the operator's shell GITHUB_APP_* env vars. The
  # ghinstallation library in the bridge parses the PEM eagerly at startup,
  # which is why the throwaway key has to be a real RSA key — even though
  # mock-github never verifies any JWTs it issues.
  GITHUB_APP_ID: "${E2E_GITHUB_APP_ID}"
  GITHUB_APP_INSTALLATION_ID: "${E2E_GITHUB_APP_INSTALLATION_ID}"
  GITHUB_APP_PRIVATE_KEY: |
${E2E_GITHUB_APP_PRIVATE_KEY_INDENTED}
  ANTHROPIC_API_KEY: "${E2E_ANTHROPIC_API_KEY}"
  OPENAI_API_KEY: "${E2E_OPENAI_API_KEY}"
---
# GitRepository that source-controller uses to pull the BRIDGE CHART code
# (not the test workload). Always points at the in-cluster git-server
# because the chart lives in this repo, not in the user's sandbox.
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-ai-bridge-chart
  namespace: flux-system
spec:
  interval: 30s
  url: http://git-server.git-server.svc.cluster.local/flux-ai-bridge.git
  ref:
    tag: e2e-test
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: flux-ai-bridge
  namespace: flux-system
spec:
  interval: 1m
  # helm-controller's default install timeout is 5m. With the bundled Ollama
  # sub-chart, the first `e2e-up` on a cold host cache pulls the ~200 MB
  # ollama image AND a ~1 GB model before the Deployment reports Ready.
  # 20m matches the Makefile's e2e-up wait window when E2E_OLLAMA_ENABLED=1.
  # Without Ollama the install finishes in seconds; the bigger timeout
  # costs nothing.
  timeout: 20m
  chart:
    spec:
      chart: charts/flux-ai-bridge
      sourceRef:
        kind: GitRepository
        name: flux-ai-bridge-chart
        namespace: flux-system
      interval: 30s
      # Re-package the chart whenever the GitRepository's source SHA
      # changes, NOT only on Chart.yaml version bumps. e2e-update
      # snapshots the host working tree into a fresh git commit each
      # time, so the source SHA changes constantly while Chart.yaml
      # stays at 0.1.0. Without this, source-controller serves a stale
      # tar (the chart artifact it packaged on first install) and edits
      # to charts/flux-ai-bridge/templates/ never reach helm-controller.
      reconcileStrategy: Revision
  values:
    image:
      repository: flux-ai-bridge
      tag: e2e
      pullPolicy: Never
    existingSecret: flux-ai-bridge-e2e-creds
    # The chart's NetworkPolicy is on by default and kind's CNI enforces it.
    # It allows the `mock` namespace because config.llm.anthropicBaseUrl and
    # config.github.apiUrl point there (mock mode), so e2e exercises the
    # URL-derived rules. The tests reach the bridge via kubectl port-forward,
    # which NetworkPolicy doesn't filter.
    config:
      # Log level for the bridge in e2e. Default info; override with
      # `E2E_LOG_LEVEL=debug make e2e-up` (or e2e-update) when you need
      # the prompt + response bodies in
      # `kubectl -n flux-system logs deploy/flux-ai-bridge`.
      logLevel: "${E2E_LOG_LEVEL}"
      # WEBHOOK_MAX_CONCURRENT: how many pipelines run at once (events
      # beyond that queue). Empty = leave the bridge default (16) in place.
      webhook:
        maxConcurrent: "${E2E_WEBHOOK_MAX_CONCURRENT}"
      # The tests inject events once podinfo is already healthy, so the
      # default 60s pod quiet period would only eat into the 60s pipeline
      # deadline. 5s still runs the quiet-period path on every healthy event.
      readiness:
        podQuietPeriod: "5s"
      llm:
        provider: "${E2E_LLM_PROVIDER}"
        model: "${E2E_LLM_MODEL}"
        anthropicBaseUrl: "${E2E_ANTHROPIC_BASE_URL}"
        openaiBaseUrl: "${E2E_OPENAI_BASE_URL}"
        # The bridge defaults to true (skip the LLM on fully-green
        # reconciles). e2e exercises the full LLM pipeline against
        # mock-llm / Ollama / Azure — keep the LLM in the loop so those
        # assertions stay meaningful. The no-LLM path is covered separately
        # by E2E_LLM=none.
        skipOnHealthy: false
      github:
        repo: "${E2E_GITHUB_REPO}"
        apiUrl: "${E2E_GITHUB_API_URL}"
        # The mock GitHub speaks plain HTTP, which the bridge refuses
        # unless allowInsecure is set; real-GitHub mode keeps it false.
        allowInsecure: ${E2E_GITHUB_API_ALLOW_INSECURE}
        comments:
          # When true, the bridge appends each new comment instead of
          # deleting+recreating. Useful when iterating on a workload that
          # toggles healthy/broken so the PR keeps a chronological trail.
          # Toggle with `E2E_KEEP_COMMENT_HISTORY=true make e2e-update`.
          keepHistory: ${E2E_KEEP_COMMENT_HISTORY}
      # Persistent dedupe — gated by E2E_DEDUPE_PERSISTENCE_ENABLED. When
      # enabled, the Makefile also installs a Bitnami Valkey chart with
      # Sentinel + auth (e2e/values/valkey-sentinel-values.yaml) into the
      # `valkey` namespace, and the sentinel block below points the
      # bridge at it. The `make e2e-persistence` target wires both up so
      # the persistence Go test can verify dedupe survives a bridge
      # restart AND that the bridge talks to a real Valkey+Sentinel
      # backend with credentials.
      dedupe:
        persistence:
          enabled: ${E2E_DEDUPE_PERSISTENCE_ENABLED}
          # Non-default ACL user on the Valkey backend. Created at
          # `make e2e-up` time by the e2e-valkey-setup target via
          # `ACL SETUSER bridge on >bridge-redis-pw-e2e ...`. Empty
          # would fall back to the legacy `default` user with
          # requirepass-style AUTH — we explicitly DON'T do that here so
          # the test exercises the bridge's AUTH <user> <pass> path.
          user: "${E2E_DEDUPE_PERSISTENCE_USER}"
          # existingSecret holds DISTINCT passwords for the backend and
          # the sentinel daemons (Bitnami's auth.sentinel=true uses one
          # shared password — we override that with a separate Secret
          # the e2e-valkey-setup target creates with two independent
          # keys, so the bridge sends different AUTH credentials on
          # each leg).
          existingSecret: "${E2E_DEDUPE_PERSISTENCE_EXISTING_SECRET}"
          passwordKey: "${E2E_DEDUPE_PERSISTENCE_PASSWORD_KEY}"
          sentinel:
            enabled: ${E2E_DEDUPE_SENTINEL_ENABLED}
            # Sentinel quorum addresses, comma-separated. Built by the
            # e2e Makefile from the Bitnami valkey chart's per-pod DNS
            # (fullnameOverride=valkey → valkey-node-{0,1,2}.valkey-headless).
            addrs: "${E2E_DEDUPE_SENTINEL_ADDRS}"
            masterName: "${E2E_DEDUPE_SENTINEL_MASTER_NAME}"
            # Sentinel daemon ACL user — distinct from the backend user
            # above. e2e-valkey-setup runs `ACL SETUSER sentinel-bridge`
            # on each sentinel; the bridge connects with this user when
            # asking sentinel for the master address.
            user: "${E2E_DEDUPE_SENTINEL_USER}"
            # Different KEY in the same Secret = different password than
            # the backend leg above. This is what makes "separate
            # sentinel user + different password" meaningful — the
            # bridge MUST send the correct one to each side.
            passwordKey: "${E2E_DEDUPE_SENTINEL_PASSWORD_KEY}"
    # flux.notification.enabled is true by default — the rendered Provider +
    # Alert wire the in-cluster Notification Controller to the bridge webhook.
    # The HMAC token is pinned (instead of chart-generated) so the events
    # e2e_test.go injects directly via postWebhook can be signed with the
    # same value — keep it in sync with e2eWebhookHMACToken there.
    flux:
      notification:
        hmac:
          token: "e2e-webhook-hmac-token"
    # Bundled Ollama sub-chart. enabled=true installs ollama-helm alongside
    # the bridge; the bridge's OPENAI_BASE_URL auto-wires to the in-cluster
    # ollama Service. The cache PVC + namespace were provisioned by
    # 05-ollama-cache.yaml.
    ollama:
      enabled: ${E2E_OLLAMA_ENABLED}
      # Deploy ollama into its own namespace so operators can scope policies
      # at it without touching flux-system. The bridge chart's deployment.yaml
      # consults this to auto-derive OPENAI_BASE_URL pointing at the right
      # cluster-DNS name.
      namespaceOverride: ollama
      # Override the Ollama sub-chart's models.pull/run lists so e2e can
      # exercise a stronger model than the bridge chart's default
      # (qwen2.5-coder:1.5b). Set via OLLAMA_MODEL on the make invocation,
      # e.g. `make e2e-up E2E_LLM=ollama OLLAMA_MODEL=qwen2.5-coder:7b`.
      ollama:
        models:
          pull:
            - "${E2E_LLM_MODEL}"
          run:
            - "${E2E_LLM_MODEL}"
      # Loosen the bridge chart's conservative Ollama resources defaults
      # (requests=2Gi, limits=4Gi) for e2e — they're too tight for the
      # 7B/14B/32B models you can swap in via E2E_LLM_MODEL.
      #
      # Why explicit numbers and not `{}` or `null`: Helm's deep-merge keeps
      # parent-chart defaults when the override is empty or null, so we have
      # to fully replace each leaf. 32Gi is well above what any qwen2.5
      # model up to 32B uses on disk + working set; on a typical
      # docker-desktop kind setup that's beyond Docker's own memory cap
      # anyway, so it functions as "effectively unlimited."
      persistentVolume:
        enabled: true
        existingClaim: ollama-cache
    # flux-operator-mcp sub-chart is enabled by default in the bridge chart,
    # so e2e doesn't override it. config.mcp.url stays empty, so the bridge
    # derives the sub-chart's Service URL in its release namespace.
    # Bundled Valkey sub-chart for persistent dedupe state. Disabled by
    # default in e2e — `make e2e-persistence` flips this on via
    # E2E_VALKEY_ENABLED=true so the persistence test can assert that
    # dedupe entries survive a bridge restart.
    valkey:
      enabled: ${E2E_VALKEY_ENABLED}
---
# Source-controller auth secret for the podinfo GitRepository below.
# Only rendered (and referenced from the GitRepository) in real-GitHub
# mode. In mock mode the in-cluster git-server is anonymous and the new
# source-controller validates "secret has githubApp keys ⇒ provider must
# be github" — so we must NOT carry an unused App secret when provider is
# "generic". The Makefile sets E2E_PODINFO_GIT_AUTH_SECRET / _SECRETREF
# to render or strip these blocks per mode.
#
# Keys match the fluxcd/pkg/auth/githubapp constants source-controller
# expects when spec.provider=github.
${E2E_PODINFO_GIT_AUTH_SECRET}
# GitRepository that source-controller uses to pull the PODINFO TEST
# WORKLOAD (not the bridge chart). Points at the in-cluster git-server
# in mock mode, at github.com/$GITHUB_REPO when E2E_REAL_GITHUB=1.
# provider="github" (real mode) enables App auth — source-controller
# reads the Secret's githubApp* keys and issues an installation token.
# provider="generic" (mock mode) — Flux now rejects empty provider, and
# we drop the secretRef entirely so source-controller does an anonymous
# clone from the in-cluster git-server.
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-ai-bridge-podinfo
  namespace: flux-system
spec:
  interval: 30s
  url: "${E2E_PODINFO_GIT_URL}"
  ref:
    branch: "${E2E_PODINFO_GIT_REF}"
  provider: "${E2E_PODINFO_GIT_PROVIDER}"
${E2E_PODINFO_GIT_SECRETREF}
---
# Real Flux Kustomization that drives the e2e pipeline. Scoped to one app
# (podinfo) so the bridge events the test fires are about exactly one
# Kustomization. The sourceRef tracks the podinfo GitRepository above, so
# real-GitHub mode and mock mode both reach the right manifests with no
# Kustomization-spec change.
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: podinfo
  namespace: flux-system
spec:
  interval: 5m
  path: "${E2E_PODINFO_PATH}"
  prune: true
  # podinfo comes up in ~1 minute on a warm cluster. A short Kustomization
  # spec.timeout means a broken HelmRelease (e.g. via `make e2e-broken`)
  # surfaces a Failed event within ~2m instead of dragging out for 5m, so
  # the e2e suite can assert on failure analysis quickly.
  timeout: 2m
  sourceRef:
    kind: GitRepository
    name: flux-ai-bridge-podinfo
