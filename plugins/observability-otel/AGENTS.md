# OpenTelemetry Observation Guidance

- Telemetry is a derived observation. It never selects Plans, authorizes work,
  admits transitions, supplies replay data, or becomes a second state store.
- Retain exact Run, Plan, occurrence, command, and effect identities as fields.
  Do not emit Resource contents, prompts, model text, credentials, URLs with
  tokens, or arbitrary state blobs.
- Build on `tracing`, `tracing-opentelemetry`, and the official OTLP exporter.
  The application composes the returned layer and owns global subscriber setup.
- Exporter failure must not change semantic results. Shutdown/flush errors are
  operational evidence for the application, not Run transition failures.
- Keep observation kinds closed and validate bounded attributes before emitting.
