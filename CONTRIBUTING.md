# Contributing

Thanks for your interest in flux-ai-bridge. Bug reports, feature ideas and pull requests are welcome.

## Before you start

- For a bug or feature request, open an issue with one of the templates first, so we can agree on the approach.
- Every pull request must follow the [pull request template](.github/PULL_REQUEST_TEMPLATE.md) and pass CI.
- The [Developer Guide](docs/development.md) explains how to build, test and run the bridge locally.
- [AGENTS.md](AGENTS.md) lists the coding conventions for this repository (they apply to humans as well as AI agents).

## License headers

Every source file starts with the Apache 2.0 license header. For Go files:

```go
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
```

Use the comment syntax of the file type (e.g. `#` for Dockerfiles and Makefiles).

## Contributor License Agreement

Staffbase uses CLAs, or Contributor License Agreements, to manage contributions to Free and Open Source Software projects that it hosts.

Staffbase uses two versions of the CLA:

- An **Individual CLA** for individual contributors. This can be signed through CLA Assistant when submitting a commit in GitHub: comment `I have read the CLA Document and I hereby sign the CLA` on your pull request.
- A **Corporate CLA**, for companies that would like to sign one CLA for their contributors. Please contact opensource(at)staffbase.com in this case.

Staffbase’s two CLAs are based on the Apache Software Foundation CLAs that are widely in use for various free and open source software projects. Our changes are generally limited to replacing Staffbase as the licensee, minor formatting changes and headings added for readability, and to comply with our CLA process integrated into GitHub or our processes around signature and management of the software project.

A pull request can only be merged after its author has signed the [CLA](CLA.md).
