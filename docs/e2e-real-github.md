# E2E against a real GitHub repository

The default `make e2e` setup runs the full pipeline against in-cluster mocks
(`mock-github`, `mock-llm`). That's fast, hermetic, and free.

If you want the bridge to talk to **api.github.com** instead — to validate the
end-to-end behavior on a real PR — set `E2E_REAL_GITHUB=1` and provide a few
env vars. The LLM stays mocked unless you also set `E2E_LLM` (see
[e2e-real-llm.md](e2e-real-llm.md)).

---

## What stays mocked

| Component | Mock mode | Real-GitHub mode |
|---|---|---|
| `flux-operator-mcp` | real (bridge sub-chart) | real |
| `mock-llm` (Anthropic) | mocked | mocked unless `E2E_LLM` is set |
| `mock-github` | mocked | **bypassed** — bridge talks to api.github.com |

Only the GitHub side is real; the LLM stays the in-cluster Python stub (so
test runs are deterministic and free) unless `E2E_LLM=ollama|azure|none` is
set — see [e2e-real-llm.md](e2e-real-llm.md).

---

## One-time setup

You need a sandbox repo, an open PR, and a GitHub App installed on that sandbox repo. `make e2e-up` in real-GitHub mode requires `GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, and `GITHUB_APP_PRIVATE_KEY` to be set in the operator's shell — there is no token-based path. The same App credential covers both the bridge's PR-commenting AND source-controller's cloning of the test repo (via `provider: github` + App-issued installation tokens).

### 1. Create the sandbox repo

```bash
# (replace MYUSER below with your GitHub login or org)
export GITHUB_USER=MYUSER
export GH_REPO=flux-ai-bridge-sandbox

gh repo create "${GITHUB_USER}/${GH_REPO}" --public --description "Sandbox for flux-ai-bridge e2e"
git clone "git@github.com:${GITHUB_USER}/${GH_REPO}.git" /tmp/${GH_REPO}
cd /tmp/${GH_REPO}
```

### 2. Seed it with the test workload

Copy the same workload `make e2e-up` uses inside the cluster, keeping the
same path layout (`e2e/podinfo-kustomize/`) so the bridge's templated
Kustomization path matches both modes:

```bash
mkdir -p e2e/podinfo-kustomize
cp -r /path/to/flux-ai-bridge/e2e/podinfo-kustomize/* e2e/podinfo-kustomize/
git add .
git commit -m "Initial: podinfo + kustomization"
git push origin main
```

You should now have on `main`:

```
e2e/podinfo-kustomize/
├── helmrepo.yaml
├── kustomization.yaml
├── namespace.yaml
└── podinfo.yaml
```

If you prefer a different directory layout, set `E2E_PODINFO_PATH=./<dir>`
before `make e2e-up` so the rendered Kustomization's `spec.path` matches.

### 3. Open a PR that bumps podinfo's CPU resources

```bash
git checkout -b bump-podinfo-cpu
# tweak the CPU request — anything that produces a real diff
sed -i 's/cpu: 10m/cpu: 20m/' e2e/podinfo-kustomize/podinfo.yaml
git add e2e/podinfo-kustomize/podinfo.yaml
git commit -m "Bump podinfo CPU request to 20m"
git push -u origin bump-podinfo-cpu

gh pr create --base main --head bump-podinfo-cpu \
  --title "Bump podinfo CPU to 20m" \
  --body "Test PR for flux-ai-bridge e2e."
```

Note: source-controller pulls from the PR's **base branch** (`main` by
default). Merge the PR into `main` (or override `E2E_PODINFO_GIT_REF` to
target a different long-lived branch like `dev`) for Flux to see the
changes.

### 4. Keep the PR open

The e2e test auto-discovers the single open PR in `${GITHUB_USER}/${GH_REPO}` —
no need to capture the number/SHA. Just keep it open between runs.

If you ever need multiple open PRs in the same sandbox (e.g. testing
parallel comments), set `GITHUB_PR_NUMBER` to pick a specific one; the test
will use `GET /pulls/<N>` to fetch its head SHA on its own.

### 5. Set up the GitHub App

Create a new App at [github.com/settings/apps/new](https://github.com/settings/apps/new):

| Setting | Value |
|---|---|
| Homepage URL | (any — `https://github.com/${GITHUB_USER}` is fine) |
| Webhook | unchecked (not used) |
| **Repository permissions** | **Pull requests: Read**, **Issues: Read and write**, **Contents: Read**, **Metadata: Read (auto)** |

`Issues: Read and write` is what lets the App post PR comments (PR comments are issue comments on GitHub). `Contents: Read` is what the bridge needs to read commits and changed files on a private repository (see [github-app.md](github-app.md)); it also lets source-controller clone the sandbox repo with the same App credentials.

After creating it: generate a private key (downloads a `.pem`), then install the App on your sandbox repo and note the **Installation ID** (URL ends with `/installations/<NUMBER>`).

Export the three required env vars before running `make e2e-up`:

```bash
export GITHUB_APP_ID=<App ID from the App page>
export GITHUB_APP_INSTALLATION_ID=<Installation ID>
export GITHUB_APP_PRIVATE_KEY="$(cat ~/Downloads/flux-ai-bridge.*.pem)"
```

The Makefile uses the same App credentials for both the bridge AND source-controller (via `provider: github`, App-issued installation tokens). See [docs/github-app.md](github-app.md) for the full App setup walkthrough.

---

## Running the e2e suite against the real PR

```bash
export E2E_REAL_GITHUB=1
export GITHUB_REPO=${GITHUB_USER}/${GH_REPO}

# Required (App-only auth):
export GITHUB_APP_ID=<...>
export GITHUB_APP_INSTALLATION_ID=<...>
export GITHUB_APP_PRIVATE_KEY="$(cat ~/Downloads/flux-ai-bridge.*.pem)"
# make e2e-up errors out if any of the three are missing.

# Required by `make e2e` (not the bridge): the test reads the posted comment
# back through the API with this token. Any token that can read the repo's
# PRs and comments works, e.g. `export GITHUB_TOKEN=$(gh auth token)`.
export GITHUB_TOKEN=<token>

make e2e-down      # if a previous cluster is around
make e2e-up        # builds, boots kind, installs Flux and the bridge (with its MCP sub-chart)
make e2e           # runs Go tests; TestPipelineProducesRealGitHubComment is now active
```

The test discovers the single open PR in `GITHUB_REPO` automatically (calls
`GET /repos/.../pulls?state=open`). If you keep multiple open PRs in the
sandbox repo for some reason, pin one with `export GITHUB_PR_NUMBER=42` —
the head SHA is then fetched from `GET /pulls/42`. `GITHUB_PR_SHA` is an
optional final override; setting it alongside `GITHUB_PR_NUMBER` skips the
fetch.

What happens:

1. `make e2e-up` renders [e2e/manifests/03-flux-ai-bridge.yaml.tpl](../e2e/manifests/03-flux-ai-bridge.yaml.tpl) with:
   - bridge `config.github.repo=${GITHUB_REPO}`, `config.github.apiUrl=""` (defaults to api.github.com), plus the App credentials in the bridge Secret: `GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY`.
   - **a second GitRepository** (`flux-ai-bridge-podinfo`) pointing at `https://github.com/${GITHUB_REPO}.git`, branch `main` (or `E2E_PODINFO_GIT_REF`). Auth shape: `provider: github` with `githubAppID`, `githubAppInstallationID`, `githubAppPrivateKey` Secret keys — source-controller mints installation tokens on the fly using the same App credentials.
   - **the podinfo Kustomization** with `sourceRef: flux-ai-bridge-podinfo` and `path: ./e2e/podinfo-kustomize` (or `E2E_PODINFO_PATH`)
   - The bridge's own chart code keeps coming from the in-cluster git-server — only the test workload moves to GitHub.
2. `make e2e` runs the suite. `TestPipelineProducesGitHubComment` is **skipped** (it would target the mock); `TestPipelineProducesRealGitHubComment` becomes active.
3. The test relies on the **real Flux reconcile pipeline** — source-controller fetches the configured branch, kustomize-controller reconciles, Notification Controller forwards the event to the bridge, which posts the comment. The test simply:
   - auto-discovers the open PR (or uses `GITHUB_PR_NUMBER` / `GITHUB_PR_SHA` when pinned)
   - polls `GET /repos/${GITHUB_USER}/${GH_REPO}/issues/${PR}/comments` for up to 90 s
   - asserts a comment with the `<!-- flux-ai-bridge` marker exists and has the expected structure

   No prior-comment deletion, no mock-llm reset — those were mock-mode artifacts. Mock-mode coverage of the synthetic-webhook path is retained in `TestPipelineProducesGitHubComment`.

When it passes, refresh the PR in your browser — you'll see the bridge's
comment.

---

## Cleaning up

The bridge's comment marker is idempotent: a subsequent reconcile **updates** the existing comment rather than spamming the PR (delete-and-recreate on each post, keyed on the per-Kustomization marker).

**Repeat-run caveat:** the bridge's in-memory dedupe persists across `make e2e` invocations against the same long-running cluster. The PR head SHA doesn't change between runs, so the bridge skips reprocessing on the second `make e2e` and the test would see only the comment from the first run. To force a fresh post:

```bash
make e2e-update  # rebuilds + rollout-restarts the bridge → clears in-memory dedupe
make e2e
```

For a totally clean slate, `make e2e-down && make e2e-up && make e2e`.

To tear everything down:

```bash
make e2e-down
```

The sandbox repo / PR can be reused indefinitely. If you want a totally fresh
PR (e.g. to test against a different SHA), just push a new commit to
`bump-podinfo-cpu` and update `GITHUB_PR_SHA`.

---

## Switching back to mock mode

```bash
unset E2E_REAL_GITHUB
make e2e-down && make e2e-up && make e2e
```

`mock-github` is **not** deployed when `E2E_REAL_GITHUB=1` — the Makefile
skips its manifest entirely in real-GitHub mode. Switching back therefore
needs the `make e2e-up` re-run above (with the env var unset) so mock-github
gets deployed and the bridge is re-rendered to point at it instead of
`api.github.com`.

---

## Troubleshooting

### "bridge did not post a comment within 90s"

- Check the bridge logs: `kubectl --context kind-flux-ai-bridge-e2e -n flux-system logs deploy/flux-ai-bridge --tail=50`. Look for `process event failed`.
- A `403` from GitHub is almost always a scope issue. Confirm the App installation covers the sandbox repo AND the App permissions include **Pull requests: Read**, **Issues: Read and write**, and **Contents: Read**.
- A `404` on the commit-to-PR lookup means the SHA is not on an open PR. Re-check `GITHUB_PR_SHA` with `gh pr view`.

### Comment shows up but with the mock LLM text

That's expected — `mock-llm` stays in the loop by default. To run a real LLM,
set `E2E_LLM=ollama` or `E2E_LLM=azure` as well (see
[e2e-real-llm.md](e2e-real-llm.md)).

### Rate limits

The real-GitHub test does at most ~5 GitHub API calls per run. A GitHub App installation token gets at least 5,000 requests/hour, so you'd have to run `make e2e` thousands of times an hour to come close.
