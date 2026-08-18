//! Observation validation and OTLP pipeline construction tests.

use std::collections::BTreeMap;

use cymule_observability_otel::{
    CymuleObservation, ObservationKind, OtelConfig, OtelObserver, OtelPipeline,
};
use tracing_subscriber::layer::SubscriberExt;

#[test]
fn bounded_observation_records_without_becoming_authority() {
    let observation = CymuleObservation {
        kind: ObservationKind::Effect,
        run_id: Some("run:one".to_owned()),
        plan_id: Some("sha256:plan".to_owned()),
        occurrence_id: Some("occurrence:one".to_owned()),
        operation_id: Some("effect:one".to_owned()),
        outcome: "unknown".to_owned(),
        attributes: BTreeMap::from([("binding".to_owned(), "plugin:one".to_owned())]),
    };
    OtelObserver
        .record(&observation)
        .expect("observation records");
    let mut invalid = observation;
    invalid
        .attributes
        .insert("payload".to_owned(), "x".repeat(1025));
    assert!(OtelObserver.record(&invalid).is_err());
}

#[test]
fn otlp_pipeline_is_composable_and_not_global() {
    let pipeline = OtelPipeline::build(&OtelConfig::default()).expect("pipeline builds");
    let subscriber = tracing_subscriber::registry().with(pipeline.layer());
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "cymule", "pipeline composes");
    });
    pipeline.shutdown().expect("pipeline shuts down");
}
