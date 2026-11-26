# Authenticating the bridge to GitHub via a GitHub App

The bridge authenticates to GitHub **exclusively as a GitHub App**. This is the only supported mode — there is no token-based path. The App approach gives bounded, audit-friendly permissions, hourly auto-rotation of installation tokens, no per-user token rotation chores, and bot-style comment attribution (the comments are posted by the App, not as the user whose token was supplied).

The bridge expects all three of these env vars at startup:

- `GITHUB_APP_ID`
- `GITHUB_APP_INSTALLATION_ID`
- `GITHUB_APP_PRIVATE_KEY` (PEM contents inline) **or** `GITHUB_APP_PRIVATE_KEY_FILE` (path to the PEM on disk)

Partial App config (e.g., App ID set but no private key) fails at startup with a clear error.

This guide is the canonical walkthrough for App setup.

---

## 1. Create the GitHub App

Go to:

- **Personal account:** https://github.com/settings/apps/new
- **Organization:** https://github.com/organizations/`<org>`/settings/apps/new

Fill in:

| Field | Value |
|---|---|
| GitHub App name | `flux-ai-bridge` (must be unique across GitHub) |
| Homepage URL | https://github.com/`<your>`/flux-ai-bridge |
| **Webhook → Active** | **Uncheck** (the bridge does not receive GitHub webhooks) |
| Webhook URL | leave empty |

### Repository permissions

Set **only** these — the bridge requests no more:

| Permission | Access |
|---|---|
| **Pull requests** | **Read-only** (find the PRs behind a commit, list their changed files) |
| **Issues** | **Read and write** (PR comments and labels are issue comments/labels on GitHub) |
| **Contents** | **Read-only** (read commits and compare them with the default branch for the merge-parent walk, and the changed files to see which resources they declare) |
| Metadata | Read-only (mandatory, auto-granted) |

Nothing is written to the repository itself — Contents is read-only. Without
it, PR lookups on a **private** repository fail on the commit and file reads,
so no PR is ever attributed (public repositories happen to work without it).
If a commit has no associated PR, the bridge logs and skips (rare in a GitOps
flow — `ListPullRequestsWithCommit` returns merged PRs too).

### How the bridge recognises its own comments

At startup the bridge calls `GET /app` once, signed with the App's JWT (the
same key it mints installation tokens with), and reads the App's slug. GitHub
posts the App's comments as `<slug>[bot]`, so from then on the bridge treats a
PR comment as its own only when it starts with the bridge's marker **and** is
authored by `<slug>[bot]`. A comment by another bot or App that copies the
marker is never edited, kept as current, or deleted as the bridge's.

`GET /app` needs **no extra permission**: any App can read its own metadata
with its JWT. If the call fails (network, a GitHub outage), the bridge logs a
`WARN` (`GitHub App identity lookup failed; retrying on each comment update ...`)
and starts anyway. Each comment update retries the lookup; while it keeps
failing, the update fails and no comment is edited or deleted, so the event
is retried. On success it logs `GitHub App identity resolved` with the
`bot_login`.

### Account & organization permissions

Leave everything unchecked.

### Subscribe to events

Leave all unchecked. The bridge does not consume GitHub webhooks.

### Where can this GitHub App be installed?

- **`Only on this account`** for org-scoped Apps.
- `Any account` is fine for a personal App you intend to share.

Click **Create GitHub App**.

---

## 2. Capture the App ID

On the App's settings page (right after creation), copy the **App ID** at the
top of the General tab. Save it — you'll set `GITHUB_APP_ID` to this value.

---

## 3. Generate a private key

Scroll to **Private keys** → **Generate a private key**. A `.pem` file
downloads. Keep it safe — this is the App's identity.

Use this file directly with `--set-file`, or copy its contents inline; either
way it becomes `GITHUB_APP_PRIVATE_KEY` in the bridge's Secret.

---

## 4. Install the App on your repositories

On the App settings page → **Install App** → choose your account/org.

### Repository access — **choose "Only select repositories"**

"All repositories" works but the App can then comment on *every* repo in the
org, including ones nothing to do with GitOps. "Only select repositories"
scopes the blast radius to the repos you explicitly authorize.

Add each repo that has a Flux Kustomization pointing at it. **Adding a new
repo later:** repeat this step, no bridge change needed.

Click **Install**.

---

## 5. Capture the Installation ID

After installing, the browser URL becomes:

```
https://github.com/organizations/<org>/settings/installations/<installation-id>
```

Copy that trailing number — `GITHUB_APP_INSTALLATION_ID`.

---

## 6. Configure the bridge

The commands below install from a repository checkout; run `make chart-deps` once first to fetch the sub-charts pinned in `Chart.yaml` (see [getting-started.md](getting-started.md#2-install-the-helm-chart) for the plain Helm commands).

### Option A — chart-managed Secret (quick start)

```bash
helm install flux-ai-bridge ./charts/flux-ai-bridge \
  --namespace flux-system \
  --create-namespace \
  --set credentials.githubApp.appId=<APP_ID> \
  --set credentials.githubApp.installationId=<INSTALLATION_ID> \
  --set-file credentials.githubApp.privateKey=./flux-ai-bridge.<id>.private-key.pem \
  --set credentials.anthropicApiKey=$ANTHROPIC_API_KEY \
  --set config.llm.provider=claude
```

The chart renders a Secret with `GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`,
and `GITHUB_APP_PRIVATE_KEY`. The bridge auto-detects App mode at startup.

### Option B — external Secret (recommended for production)

Create the Secret yourself (or via Sealed Secrets / External Secrets / SOPS):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: flux-ai-bridge-credentials
  namespace: flux-system
type: Opaque
stringData:
  GITHUB_APP_ID: "12345"
  GITHUB_APP_INSTALLATION_ID: "67890"
  GITHUB_APP_PRIVATE_KEY: |
    -----BEGIN RSA PRIVATE KEY-----
    MIIEpAIBAAKCAQEA...
    -----END RSA PRIVATE KEY-----
  ANTHROPIC_API_KEY: "sk-ant-..."
```

Then point the chart at it:

```bash
helm install flux-ai-bridge ./charts/flux-ai-bridge \
  --namespace flux-system \
  --set existingSecret=flux-ai-bridge-credentials \
  --set config.llm.provider=claude
```

---

## 7. Verify

Tail bridge logs at the next Flux event. App-mode startup is silent (no
log line announcing it); the proof is that GitHub API calls succeed.

Increment-driven verification:

- `kubectl --context <your-context> -n flux-system port-forward svc/flux-ai-bridge 8080:8080`
  then `curl http://localhost:8080/metrics | grep flux_ai_bridge_github_`.
  After a Flux event, `flux_ai_bridge_github_api_requests_total` shows 2xx
  statuses and `flux_ai_bridge_github_auth_failures_total` stays at 0. Don't
  rely on `flux_ai_bridge_github_rate_limit_remaining` alone: a 401 or 403
  carries rate-limit headers too, so the gauge also moves when the App lacks a
  permission.
- The surest proof is the bridge's comment on the PR.

---

## Troubleshooting

### `GitHub App config required: set GITHUB_APP_ID + GITHUB_APP_INSTALLATION_ID + GITHUB_APP_PRIVATE_KEY (or GITHUB_APP_PRIVATE_KEY_FILE)`

You set one or two of the three App env vars but not all three. The bridge
refuses to start rather than running in an undefined state.

### `403 Resource not accessible by integration` on comment posts

The App is installed but doesn't have **Issues: Write** on the target repo.
Re-check the App's permission set; if you adjusted permissions after
install, the org admin has to **accept the new permissions** in the
installation settings.

### `404 Not Found` on `GET /repos/.../commits/.../pulls`

The App is installed but the repo is not in "Selected Repositories." Add it
via the App's installation settings.

### Bridge logs `no open PR matched filters; skipping analysis`

No open PR behind the commit was attributed to this Kustomization. Either
the commit isn't associated with any open PR (`candidate_prs=0` in the log
line — usually a direct push to the deploy branch), every candidate was
filtered out (required labels, or its changed files don't feed this
Kustomization's build), or they're all closed/merged (`finalized`). The
bridge intentionally posts nothing in this case.

### Rotating the private key

GitHub lets you generate a new key without invalidating the old one. Plan:

1. Generate new private key in the App settings.
2. Put the new key where the chart reads it:
   - **Option A (chart-managed Secret):** run `helm upgrade` with the new key,
     for example `helm upgrade flux-ai-bridge ./charts/flux-ai-bridge
     --kube-context <your-context> --namespace flux-system
     --reset-then-reuse-values --set-file credentials.githubApp.privateKey=<new-key.pem>`.
     Don't edit the Secret by hand: the next `helm upgrade` would restore the
     old key from the Helm values. The chart rolls the pods when the Secret
     changes.
   - **Option B (external Secret):** update the Secret in your secret manager
     (or `kubectl --context <your-context> edit secret ...`), then restart the
     bridge: `kubectl --context <your-context> -n flux-system rollout restart
     deployment/flux-ai-bridge`.
3. Wait until every replica runs with the new key:
   `kubectl --context <your-context> -n flux-system rollout status
   deployment/flux-ai-bridge`. Go on only when it reports success.
4. Delete the old key in the App settings.

The bridge reads the private key only at startup, and uses it to mint and
renew its installation tokens. Every replica must restart (step 3) before
you delete the old key (step 4); a replica still running on the old key
can't renew its token once that key is gone.
