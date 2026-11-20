{{/*
Expand the name of the chart.
*/}}
{{- define "ksml.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "ksml.fullname" -}}
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
{{- define "ksml.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ksml.labels" -}}
helm.sh/chart: {{ include "ksml.chart" . }}
{{ include "ksml.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ksml.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ksml.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "ksml.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ksml.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Extra container env from .Values.env. The templates add it after the chart's own env, so entries can reference those with $(NAME).
*/}}
{{- define "ksml.env" -}}
{{- range .Values.env }}
{{- if has .name (list "KSML_K8S_POD_NAME" "KSML_GROUP_INSTANCE_ID" "LOGBACK_CONFIGURATION_FILE") }}
{{- fail (printf "env: %s is set by the chart and cannot be overridden" .name) }}
{{- end }}
{{- end }}
{{- with .Values.env }}
{{- toYaml . }}
{{- end }}
{{- end }}
