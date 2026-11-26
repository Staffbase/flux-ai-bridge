# Copyright 2026, Staffbase SE and contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Base images are pinned by digest; Dependabot (docker ecosystem) bumps the
# digest along with the tag.
FROM golang:1.27-alpine@sha256:f92b6ef800e499660581efdabdf25d9d817a9d124eaf900924f0504e7e27e12d AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# VERSION is stamped into the binary via -ldflags so the MCP server logs the
# bridge version on every initialize. Defaults to "dev" when not set — pass
# it with `--build-arg VERSION=<tag>` (the CI workflows don't yet).
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/staffbase/flux-ai-bridge/internal/mcp.Version=${VERSION}" \
    -o flux-ai-bridge ./cmd/...

FROM gcr.io/distroless/static:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931
# The source label links the GHCR package to this repository.
LABEL org.opencontainers.image.source="https://github.com/Staffbase/flux-ai-bridge" \
      org.opencontainers.image.description="Posts Flux CD deployment status as GitHub PR comments" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=builder --chown=65532:65532 --chmod=0555 /app/flux-ai-bridge /flux-ai-bridge
COPY --chmod=0444 LICENSE /licenses/LICENSE
USER 65532:65532
ENTRYPOINT ["/flux-ai-bridge"]
