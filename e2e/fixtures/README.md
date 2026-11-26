# e2e fixtures

## `mock-app-key.pem` (generated, gitignored)

The bridge always uses GitHub App auth (no PAT support). At startup the
`github.com/bradleyfalzon/ghinstallation` library parses the App's private
key PEM eagerly, so the bridge cannot construct its GitHub client without
a syntactically valid RSA key — even when targeting mock-github, which
doesn't actually verify any signatures.

To keep mock-mode e2e zero-touch, `make e2e-up` generates a throwaway
2048-bit RSA key into `e2e/fixtures/mock-app-key.pem` on every run if the
file doesn't exist yet. The file is gitignored and has no security value:
it never sees a real GitHub installation. Delete it whenever you like;
the next `make e2e-up` will regenerate one.

Real-GitHub mode (`E2E_REAL_GITHUB=1`) ignores this file entirely and
uses `GITHUB_APP_PRIVATE_KEY` from the operator's shell environment.
