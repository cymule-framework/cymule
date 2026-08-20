# Cymule OpenTelemetry

`cymule-observability-otel` exports bounded Cymule traces and
low-cardinality metrics through the official OpenTelemetry SDK and OTLP HTTP
exporters.

```sh
cargo add cymule-observability-otel
```

The application composes the returned layer into its subscriber; this crate
does not install global state. `OtelPipeline::observer` creates the bounded
observer used at framework boundaries, while `force_flush` and `shutdown`
return exporter failures as operational evidence.

Exact Run, Plan, occurrence, command, effect, wait, and evolution identities
appear in traces only. The observation contract has no arbitrary attribute,
payload, URL, prompt, error-message, or credential field. Metrics use closed
outcomes as their only dimension:

- Run, occurrence, effect, wait, and evolution outcome counters;
- active Run, active occurrence, and ready-work backlog gauges;
- claim, reconciliation, and wait duration histograms.

Export failure, a full span queue, or shutdown failure can lose telemetry, but
cannot authorize work, alter a durable outcome, or become replay data. Queue
backpressure therefore drops observation data instead of blocking semantic
execution. Graceful shutdown flushes both trace and metric providers when the
exporters are available.
