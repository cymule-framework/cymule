//! Recording-exporter contract tests for Cymule telemetry.

use std::time::Duration;

use cymule_observability_otel::{
    CymuleObservation, DurationKind, ObservationErrorKind, ObservationKind, ObservationOutcome,
    OtelConfig, OtelPipeline, RuntimeGauges,
};
use opentelemetry::trace::Status;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use opentelemetry_sdk::trace::InMemorySpanExporter;
use tracing_subscriber::layer::SubscriberExt;

fn observation(kind: ObservationKind, outcome: ObservationOutcome) -> CymuleObservation {
    CymuleObservation {
        kind,
        outcome,
        run_id: Some("run:exact".to_owned()),
        plan_id: "sha256:plan-exact".to_owned(),
        occurrence_id: Some("occurrence:exact".to_owned()),
        command_id: Some("command:exact".to_owned()),
        effect_id: (kind == ObservationKind::Effect).then(|| "effect:exact".to_owned()),
        wait_id: (kind == ObservationKind::Wait).then(|| "wait:exact".to_owned()),
        evolution_id: (kind == ObservationKind::Evolution).then(|| "evolution:exact".to_owned()),
    }
}

#[test]
fn recording_exporters_capture_parent_child_identity_error_and_low_cardinality_metrics() {
    let trace_exporter = InMemorySpanExporter::default();
    let metric_exporter = InMemoryMetricExporter::default();
    let config = OtelConfig {
        metric_export_interval: Duration::from_hours(1),
        ..OtelConfig::default()
    };
    let pipeline =
        OtelPipeline::from_exporters(&config, trace_exporter.clone(), metric_exporter.clone())
            .expect("recording pipeline");
    let observer = pipeline.observer();
    let subscriber = tracing_subscriber::registry().with(pipeline.layer());

    tracing::subscriber::with_default(subscriber, || {
        let run = observation(ObservationKind::Run, ObservationOutcome::Succeeded);
        let run_span = observer.span(&run).expect("run span");
        {
            let _run_guard = run_span.enter();
            let effect = observation(ObservationKind::Effect, ObservationOutcome::Unknown);
            let effect_span = observer.span(&effect).expect("effect span");
            observer.record_error_on(&effect_span, ObservationErrorKind::UnknownWorld);
        }
        observer.record_on(&run_span);

        observer.record_gauges(RuntimeGauges {
            active_runs: 1,
            active_occurrences: 2,
            backlog: 3,
        });
        observer.record_duration(
            DurationKind::Claim,
            Duration::from_millis(25),
            ObservationOutcome::Succeeded,
        );
        observer.record_duration(
            DurationKind::Reconcile,
            Duration::from_millis(50),
            ObservationOutcome::Reconciled,
        );
        observer.record_duration(
            DurationKind::Wait,
            Duration::from_millis(75),
            ObservationOutcome::Activated,
        );
    });

    pipeline.force_flush().expect("recording exporters flush");
    let spans = trace_exporter
        .get_finished_spans()
        .expect("recorded spans are readable");
    assert_eq!(spans.len(), 2);
    let run = spans.iter().find(|span| span.name == "run").expect("run");
    let effect = spans
        .iter()
        .find(|span| span.name == "effect")
        .expect("effect");
    assert_eq!(effect.parent_span_id, run.span_context.span_id());
    assert_eq!(
        attribute(effect, "cymule.run_id").as_deref(),
        Some("run:exact")
    );
    assert_eq!(
        attribute(effect, "cymule.plan_id").as_deref(),
        Some("sha256:plan-exact")
    );
    assert_eq!(
        attribute(effect, "cymule.occurrence_id").as_deref(),
        Some("occurrence:exact")
    );
    assert_eq!(
        attribute(effect, "cymule.effect_id").as_deref(),
        Some("effect:exact")
    );
    assert!(matches!(effect.status, Status::Error { .. }));
    assert!(effect.events.iter().any(|event| {
        event.attributes.iter().any(|attribute| {
            attribute.key.as_str() == "error.kind" && attribute.value.to_string() == "unknown_world"
        })
    }));
    assert_eq!(run.status, Status::Ok);

    let metrics = metric_exporter
        .get_finished_metrics()
        .expect("recorded metrics are readable");
    let rendered = format!("{metrics:?}");
    for name in [
        "cymule.run.outcomes",
        "cymule.effect.outcomes",
        "cymule.run.active",
        "cymule.occurrence.active",
        "cymule.work.backlog",
        "cymule.claim.duration",
        "cymule.reconcile.duration",
        "cymule.wait.duration",
    ] {
        assert!(rendered.contains(name), "missing metric {name}");
    }
    for high_cardinality_value in [
        "run:exact",
        "sha256:plan-exact",
        "occurrence:exact",
        "effect:exact",
        "command:exact",
    ] {
        assert!(
            !rendered.contains(high_cardinality_value),
            "identity leaked into metric labels: {high_cardinality_value}"
        );
    }

    pipeline.shutdown().expect("recording pipeline shuts down");
}

#[test]
fn observation_contract_has_no_payload_or_secret_escape_hatch() {
    let encoded = serde_json::json!({
        "kind": "effect",
        "outcome": "failed",
        "run_id": "run:one",
        "plan_id": "sha256:plan",
        "occurrence_id": "occurrence:one",
        "command_id": null,
        "effect_id": "effect:one",
        "wait_id": null,
        "evolution_id": null,
        "payload": "do-not-export",
        "authorization": "Bearer do-not-export"
    });
    assert!(serde_json::from_value::<CymuleObservation>(encoded).is_err());

    let mut missing_effect = observation(ObservationKind::Effect, ObservationOutcome::Failed);
    missing_effect.effect_id = None;
    assert!(missing_effect.validate().is_err());
}

#[test]
fn otlp_pipeline_builds_without_installing_a_global_subscriber() {
    let pipeline = OtelPipeline::build(&OtelConfig::default()).expect("pipeline builds");
    let subscriber = tracing_subscriber::registry().with(pipeline.layer());
    tracing::subscriber::with_default(subscriber, || {
        let observer = pipeline.observer();
        observer
            .record(&observation(
                ObservationKind::Run,
                ObservationOutcome::Succeeded,
            ))
            .expect("pipeline composes");
    });
    // No collector is running, so shutdown may report an operational export
    // failure. That is deliberately independent of this test's semantic work.
    let _ = pipeline.shutdown();
}

fn attribute(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find_map(|attribute| (attribute.key.as_str() == key).then(|| attribute.value.to_string()))
}
