# AGENTS.md

`flux-ai-bridge` is a Go service. It receives Flux notification-controller events, reads cluster state through
`flux-operator-mcp`, and posts one status comment per Kustomization on the GitHub PR behind the change. With
`PR_LABEL_ENABLED`, it also sets one `flux-success` / `flux-warning` / `flux-failed` label per PR, aggregated across
every attributed Kustomization: any failing → failed, all green → success. An LLM summary is optional.

## Docs

The docs are the source of truth. Read the relevant one before you change behaviour, and update it in the same change.

- [docs/architecture.md](docs/architecture.md): the request flow (numbered steps), dedupe, readiness and comment
  delivery. Read it before you touch `internal/processor`, `internal/readiness`, `internal/dedupe` or
  `internal/github`.
- [docs/configuration.md](docs/configuration.md): every env var and its Helm value. A new setting goes here, in
  `charts/flux-ai-bridge/values.yaml` and in `templates/deployment.yaml`.
- [docs/development.md](docs/development.md): make targets, e2e harness, mocks, adding an LLM provider.
- [docs/autodev.md](docs/autodev.md): the recommended autodev workflow. Keep its workflow example in sync with
  `.github/workflows/autodev.yml` (same actions and pins, minus the dev image dispatch).
- [docs/caching.md](docs/caching.md), [docs/skill.md](docs/skill.md), [docs/github-app.md](docs/github-app.md),
  [docs/local-llm.md](docs/local-llm.md): their topics only.

## Commands

Use `make` targets (`make help`). They carry the canonical flags.

- Before you hand over a change: `make vet test golangci-lint lint`. Also run `make vuln` after a dependency change.
- For behaviour that only shows in a cluster: `make e2e-up && make e2e`. After a code edit, `make e2e-update` reloads
  the image without recreating the cluster. The e2e git-server snapshots the working tree, so uncommitted changes are
  tested.
- Build-tagged tests: `e2e` (kind cluster) and `integration_sentinel` (`make test-sentinel`, Docker). Run `go vet`
  with these tags too when you touch those files.
- Fetch sub-charts with `make chart-deps`. `Chart.lock` and `charts/*` stay gitignored. Pin exact sub-chart versions in
  `Chart.yaml`.

## Cluster safety

Pin the cluster on every command that targets one: `kubectl --context`, `helm --kube-context`. This includes commands
that test code shells out to. Never rely on the kubeconfig current-context. The e2e cluster is
`kind-flux-ai-bridge-e2e`.

## Design rules

- **Flux only.** The bridge supports Flux CD. Flux itself does not need to be managed by the Flux Operator. Only
  `flux-operator-mcp` and the opt-in `FAILURE_FETCH_FLUX_INSTANCE` use Operator objects.
- **All cluster reads go through MCP.** The bridge has no Kubernetes client and no RBAC of its own. The MCP server is
  bound to a read-only role. Its own secret masking protects secrets. Grant it access by role, never by a list of API
  groups: such a list breaks each time a CRD is added to the cluster. The exception is the opt-in
  `mcpRbac.excludeNodeProxy` role, which lists groups so it can leave out `nodes/proxy`; users extend it for their
  CRDs with `mcpRbac.extraRules`.
- **The verdict is deterministic.** The emoji, label and tables come from kstatus. The LLM writes only the prose
  between the header and the footer. Comment idempotency keys on the kstatus part (the stability key), so LLM rewording
  never replaces a comment. A change to the verdict logic needs a matching update to the SKILL prompt in
  `charts/flux-ai-bridge/templates/configmap-skill.yaml`, and the reverse.
- **Fail closed, never green by default.** A row that has not been observed, a revision that has not been applied, or
  a failing reconcile must not produce `flux-success`. When in doubt, the outcome is ⚠️ warning. A non-`Current` row
  outside the pod-scanned namespace never counts as a harmless rollout in progress (`rolloutInProgress`); healthy rows
  in other namespaces are fine. A PR that deletes resources is attributed through prune events and is not a failure.
- **Redis/Valkey: plain keyspace commands only** (`SET NX`, `EXPIRE NX`, `GET`, `DEL`, `HSET`). Production ACL users
  may not run `EVAL` / `EVALSHA`, so do not use Lua. `EXPIRE NX` needs Redis 7 or later. Dedupe fails closed: on a
  backend error the PR is skipped.
- **The chart ships secure defaults.** NetworkPolicies for the bridge and the bundled MCP are on by default, and their
  ports can be configured (Redis 6379, Sentinel 26379). Bridge egress is derived from the configured URLs (LLM base
  URL, GitHub API URL, external dedupe backend). The chart does not add policies for the optional Valkey and Ollama
  sub-charts. The bridge's egress rules still allow traffic to them.
- **Flux condition types are listed in the code** (e.g. `abnormalTrueConditions` in `internal/processor/processor.go`),
  not imported from a Flux or kstatus package. This keeps all condition handling in one style.

## Tests

- Every fix gets a regression test that fails without the fix. Prove this, for example with `go test -overlay` or by
  stashing the fix.
- e2e scenarios must survive repeat runs on the same cluster. Use unique PR numbers per run (see `setScenario` in
  `e2e/e2e_test.go`).
- Write invisible characters as escapes (`\u200b`), not as literals. staticcheck ST1018 rejects the literals.

## Open source

This repository is open source under Apache 2.0 ([LICENSE](LICENSE)). Everything in it, and every commit, branch, PR
and issue, is public.

- **License header (mandatory):** every source file starts with the Apache 2.0 header from
  [CONTRIBUTING.md](CONTRIBUTING.md), copied unchanged, in the file's comment syntax: `//` for Go, `#` for
  Dockerfile, Makefile and shell. This includes every new file, test files too; a change that adds a source file
  without the header is not done. In Go, a blank line separates it from the package doc comment, and a `//go:build`
  line follows the header.
- **Generic placeholders** in code, comments, tests, fixtures and docs: `<component>/<env>/<region>`, `web` / `api`,
  `dev` / `prod`, `r1` / `r2`, `example.com`. Do not use real service, cluster, region, registry or repository names,
  and use no customer or personal data.
- **Describe the scenario** that motivated a change ("a Flux self-upgrade gated on a `dependsOn` target"). Do not cite
  the PR, issue or repository where it was observed.
- **Commits, branches and PRs:** conventional-commit scopes without ticket keys (`fix(processor): ...`), descriptive
  branch names, and no links to internal tools. PR descriptions follow
  [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md).
- Exception: "autodev" stays. Link it to <https://github.com/Staffbase/autodev-action> where it is introduced.

## Helm chart

Document chart settings in `charts/flux-ai-bridge/values.yaml`, next to the value they describe: what it does, its
default, and how it interacts with other values. Templates carry no documentation comments. When a template needs a
comment (a non-obvious guard or derivation), keep it to one or two lines and leave the explanation to `values.yaml`.

Keys in `values.yaml` and `ci/*.yaml` are sorted alphabetically at every level; list items keep their order, and a
key's comment moves with it. The sections of the sub-charts (`flux-operator-mcp`, `ollama`, `valkey`) stay at the
bottom, after the chart's own keys, sorted among themselves. `Chart.yaml` keeps Helm's field order.

## Chart releases

`ct lint` checks that the chart version goes up. A change under `charts/` bumps `version` in `Chart.yaml`, as a
separate commit: `chore(chart): bump chart to X.Y.Z, appVersion vA.B.C`. `appVersion` matches the image tag of the
release.
