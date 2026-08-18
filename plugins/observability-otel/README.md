# Cymule OpenTelemetry

`cymule-observability-otel` emits bounded, identity-rich Cymule observations
through `tracing` and provides an official OTLP trace layer.

```sh
cargo add cymule-observability-otel
```

The application composes the returned layer into its subscriber; this crate
does not install global state. Observations retain Run, Plan, occurrence,
command, effect and evidence identities, but never Resource contents, prompts,
credentials, or provider payloads. Telemetry remains a rebuildable view and
cannot authorize or alter execution.
