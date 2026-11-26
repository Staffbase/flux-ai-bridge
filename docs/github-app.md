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

- `kubectl -n flux-system port-forward svc/flux-ai-bridge 8080:8080` then
  `curl http://localhost:8080/metrics | grep github_rate_limit`. A real
  installation token gets at least 5,000 requests/hour (more for installations with many repositories or users, up to 12,500; 15,000 on GitHub Enterprise Cloud). The remaining-budget gauge
  is the quickest tell that auth is wired up correctly.

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
2. Update the bridge's Secret (`kubectl edit secret ...` or via your
   external secret manager).
3. Restart the bridge pod (kill + let K8s reschedule, or `kubectl rollout
   restart deployment/flux-ai-bridge`).
4. Delete the old key in the App settings.

The bridge reads the private key only at startup, and uses it to mint and
renew its installation tokens. Every replica must restart (step 3) before
you delete the old key (step 4); a replica still running on the old key
can't renew its token once that key is gone.
