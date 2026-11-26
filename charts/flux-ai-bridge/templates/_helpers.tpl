{{/*
Expand the name of the chart.
*/}}
{{- define "flux-ai-bridge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "flux-ai-bridge.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "flux-ai-bridge.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "flux-ai-bridge.labels" -}}
helm.sh/chart: {{ include "flux-ai-bridge.chart" . }}
{{ include "flux-ai-bridge.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "flux-ai-bridge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flux-ai-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "flux-ai-bridge.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "flux-ai-bridge.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Resolve the Secret name — either the user-supplied existingSecret or the chart-managed one.
*/}}
{{- define "flux-ai-bridge.secretName" -}}
{{- if .Values.existingSecret }}
{{- .Values.existingSecret }}
{{- else }}
{{- include "flux-ai-bridge.fullname" . }}
{{- end }}
{{- end }}

{{/*
Namespace holding the Flux Provider and Alert (and therefore the Secret the
Provider signs with).
*/}}
{{- define "flux-ai-bridge.notificationNamespace" -}}
{{- .Values.flux.notification.namespace | default .Release.Namespace }}
{{- end }}

{{/*
apiVersion for a Flux notification kind (Alert, Provider). Flux 2.10 promoted
both to notification.toolkit.fluxcd.io/v1; older Flux serves only v1beta3.
Use v1 when the cluster serves it, else v1beta3. Helm reads the cluster's API
versions on install/upgrade; an offline `helm template` sees none and renders
v1beta3 unless given `--api-versions notification.toolkit.fluxcd.io/v1/<Kind>`.
Usage: include "flux-ai-bridge.notificationAPIVersion" (dict "ctx" . "kind" "Alert")
*/}}
{{- define "flux-ai-bridge.notificationAPIVersion" -}}
{{- if .ctx.Capabilities.APIVersions.Has (printf "notification.toolkit.fluxcd.io/v1/%s" .kind) -}}
notification.toolkit.fluxcd.io/v1
{{- else -}}
notification.toolkit.fluxcd.io/v1beta3
{{- end -}}
{{- end }}

{{/*
Resolve how the webhook HMAC token is sourced:
  "existing"  — flux.notification.hmac.existingSecret names a Secret you manage (key `token`)
  "inline"    — flux.notification.hmac.token is set; the chart creates the Secret
  "generate"  — neither is set, flux.notification.enabled and hmac.generate are true;
                the chart creates the Secret with a random token (kept stable across
                upgrades via lookup)
  ""          — HMAC disabled
*/}}
{{- define "flux-ai-bridge.hmacMode" -}}
{{- with .Values.flux.notification }}
{{- if .hmac.existingSecret }}existing
{{- else if .hmac.token }}inline
{{- else if and .enabled (ne (toString .hmac.generate) "false") }}generate
{{- end }}
{{- end }}
{{- end }}

{{/*
Resolve the HMAC secret name, or empty string when HMAC is disabled; callers
must guard with `if`.
*/}}
{{- define "flux-ai-bridge.hmacSecretName" -}}
{{- $mode := include "flux-ai-bridge.hmacMode" . }}
{{- if eq $mode "existing" }}
{{- .Values.flux.notification.hmac.existingSecret }}
{{- else if $mode }}
{{- printf "%s-webhook-hmac" (include "flux-ai-bridge.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Webhook HMAC token for the "inline" and "generate" modes, empty otherwise. A
generated token is reused from the existing Secret via `lookup`, so upgrades
keep it. Without cluster access (helm template, Argo CD) a new one is drawn —
once per render and cached, so the Secret and the Deployment's checksum see
the same value and the pod rolls together with the Secret instead of keeping
the old token.
*/}}
{{- define "flux-ai-bridge.hmacToken" -}}
{{- $mode := include "flux-ai-bridge.hmacMode" . }}
{{- $hmac := .Values.flux.notification.hmac }}
{{- if eq $mode "inline" }}
{{- $hmac.token }}
{{- else if eq $mode "generate" }}
{{- if not (hasKey $hmac "_renderedToken") }}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "flux-ai-bridge.hmacSecretName" .) }}
{{- if and $existing $existing.data (hasKey $existing.data "token") }}
{{- $_ := set $hmac "_renderedToken" (index $existing.data "token" | b64dec) }}
{{- else }}
{{- $_ := set $hmac "_renderedToken" (randAlphaNum 64) }}
{{- end }}
{{- end }}
{{- index $hmac "_renderedToken" }}
{{- end }}
{{- end }}

{{/*
NetworkPolicy egress rule for an endpoint the bridge calls, derived from its
URL (config.llm.openaiBaseUrl / anthropicBaseUrl, config.github.apiUrl).
Takes a dict with url, label and root (the top-level context). Renders
nothing when the policy's other rules already cover the endpoint:
  - in-cluster Service (<name>.<namespace>.svc[.<cluster domain>]): its
    namespace, on any port. The URL names the Service port, but a
    NetworkPolicy matches the pod's target port, which can differ. Nothing
    for the release or Flux namespace, which are allowed already.
  - single-label host: a Service in the release namespace, allowed already.
  - IP address: that address on the URL's port.
  - other hostname: the URL's port to any destination, since a
    NetworkPolicy can't select hostnames. Nothing for 443/6443.
The port is the URL's own, else 443 for https and 80 for http.
*/}}
{{- define "flux-ai-bridge.urlEgressRule" -}}
{{- $root := .root }}
{{- $u := urlParse .url }}
{{- $host := $u.hostname }}
{{- $port := "" }}
{{- with regexFind ":[0-9]+$" $u.host }}{{ $port = trimPrefix ":" . }}{{ end }}
{{- if not $port }}{{ $port = ternary "443" "80" (eq $u.scheme "https") }}{{ end }}
{{- $allowed := list $root.Release.Namespace ($root.Values.networkPolicy.fluxNamespace | default "flux-system") }}
{{- if regexMatch "^[^.]+\\.[^.]+\\.svc(\\.|$)" $host }}
{{- $ns := index (splitList "." $host) 1 }}
{{- if not (has $ns $allowed) }}
# {{ .label }}: Service in namespace {{ $ns }}; any port, the policy matches the pod's port, not the Service's
- to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: {{ $ns }}
{{- end }}
{{- else if or (not $host) (has $port (list "443" "6443")) }}
{{- /* covered by the 443/6443 rule */}}
{{- else if or (regexMatch "^[0-9]+(\\.[0-9]+){3}$" $host) (contains ":" $host) }}
# {{ .label }}: {{ $host }} port {{ $port }}
- to:
    - ipBlock:
        cidr: {{ $host }}/{{ ternary "128" "32" (contains ":" $host) }}
  ports:
    - protocol: TCP
      port: {{ $port }}
{{- else if not (contains "." $host) }}
{{- /* single-label host: a Service in the release namespace, allowed already */}}
{{- else }}
# {{ .label }}: {{ $host }} port {{ $port }}, to any destination (a NetworkPolicy can't select hostnames)
- ports:
    - protocol: TCP
      port: {{ $port }}
{{- end }}
{{- end }}

{{/*
MCP endpoint the bridge calls. An explicit config.mcp.url wins; otherwise the
bundled flux-operator-mcp sub-chart's Service in the release namespace.
*/}}
{{- define "flux-ai-bridge.mcpURL" -}}
{{- $mcp := index .Values "flux-operator-mcp" | default dict }}
{{- if .Values.config.mcp.url }}
{{- .Values.config.mcp.url }}
{{- else if $mcp.enabled }}
{{- printf "http://%s.%s.svc.cluster.local:9090/mcp" ($mcp.fullnameOverride | default "flux-operator-mcp") .Release.Namespace }}
{{- end }}
{{- end }}
