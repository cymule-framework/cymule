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
- Use the closed `CymuleObservation` identity envelope. Do not add arbitrary
  trace attributes, payloads, URLs, prompts, error messages, or credentials.
- Exact Run, Plan, occurrence, command, effect, wait, and evolution identities
  belong in traces only. Metric dimensions are restricted to closed outcomes;
  never add an identity or provider-supplied value as a label.
- Preserve the provider pair as application-owned state. The plugin may create
  official SDK batch/periodic processors, but it must never install a global
  tracer, meter, or tracing subscriber.
- A full telemetry queue drops spans instead of delaying semantic work. Keep
  exporter failure, backpressure, force-flush, shutdown-flush, and shutdown
  failure covered by recording/fault exporter contract tests.
