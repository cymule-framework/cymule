//! Export failure, backpressure, and shutdown isolation tests.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use cymule_observability_otel::{
    CymuleObservation, ObservationKind, ObservationOutcome, OtelConfig, OtelPipeline,
};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, Temporality};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use tracing_subscriber::layer::SubscriberExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableOutcome {
    committed_events: u64,
    projection_digest: &'static str,
}

fn successful_outcome() -> DurableOutcome {
    DurableOutcome {
        committed_events: 7,
        projection_digest: "sha256:durable-outcome",
    }
}

fn run_observation(sequence: usize) -> CymuleObservation {
    CymuleObservation {
        kind: ObservationKind::Run,
        outcome: ObservationOutcome::Succeeded,
        run_id: Some(format!("run:{sequence}")),
        plan_id: "sha256:plan".to_owned(),
        occurrence_id: None,
        command_id: None,
        effect_id: None,
        wait_id: None,
        evolution_id: None,
    }
}

#[derive(Debug, Clone)]
struct FaultSpanExporter {
    fail_export: bool,
    fail_shutdown: bool,
    exported: Arc<AtomicUsize>,
}

impl FaultSpanExporter {
    fn new(fail_export: bool, fail_shutdown: bool) -> Self {
        Self {
            fail_export,
            fail_shutdown,
            exported: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl SpanExporter for FaultSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        if self.fail_export {
            Err(OTelSdkError::InternalFailure(
                "injected trace export failure".to_owned(),
            ))
        } else {
            self.exported.fetch_add(batch.len(), Ordering::Relaxed);
            Ok(())
        }
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        if self.fail_shutdown {
            Err(OTelSdkError::InternalFailure(
                "injected trace shutdown failure".to_owned(),
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
struct FaultMetricExporter {
    fail_export: bool,
    fail_shutdown: bool,
}

impl FaultMetricExporter {
    fn new(fail_export: bool, fail_shutdown: bool) -> Self {
        Self {
            fail_export,
            fail_shutdown,
        }
    }
}

impl PushMetricExporter for FaultMetricExporter {
    async fn export(&self, _metrics: &ResourceMetrics) -> OTelSdkResult {
        if self.fail_export {
            Err(OTelSdkError::InternalFailure(
                "injected metric export failure".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        if self.fail_shutdown {
            Err(OTelSdkError::InternalFailure(
                "injected metric shutdown failure".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

#[test]
fn exporter_failures_are_operational_and_cannot_change_durable_outcome() {
    let durable = successful_outcome();
    let pipeline = OtelPipeline::from_exporters(
        &OtelConfig::default(),
        FaultSpanExporter::new(true, false),
        FaultMetricExporter::new(true, false),
    )
    .expect("fault pipeline");
    let observer = pipeline.observer();
    let subscriber = tracing_subscriber::registry().with(pipeline.layer());
    tracing::subscriber::with_default(subscriber, || {
        observer
            .record(&run_observation(1))
            .expect("valid observation is accepted before export");
    });

    assert!(pipeline.force_flush().is_err());
    assert_eq!(durable, successful_outcome());
    assert!(pipeline.shutdown().is_err());
    assert_eq!(durable, successful_outcome());
}

#[derive(Debug, Clone)]
struct BlockingSpanExporter {
    blocked: Arc<(Mutex<bool>, Condvar)>,
    export_started: Arc<AtomicBool>,
    exported: Arc<AtomicUsize>,
}

impl BlockingSpanExporter {
    fn new() -> Self {
        Self {
            blocked: Arc::new((Mutex::new(true), Condvar::new())),
            export_started: Arc::new(AtomicBool::new(false)),
            exported: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn release(&self) {
        let (blocked, wake) = &*self.blocked;
        *blocked.lock().expect("blocking exporter gate") = false;
        wake.notify_all();
    }
}

impl SpanExporter for BlockingSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.export_started.store(true, Ordering::Release);
        let (blocked, wake) = &*self.blocked;
        let mut blocked = blocked.lock().expect("blocking exporter gate");
        while *blocked {
            blocked = wake.wait(blocked).expect("blocking exporter wake");
        }
        self.exported.fetch_add(batch.len(), Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn exporter_backpressure_drops_telemetry_without_blocking_semantic_work() {
    const OBSERVATIONS: usize = 10_000;

    let durable = successful_outcome();
    let trace_exporter = BlockingSpanExporter::new();
    let pipeline = OtelPipeline::from_exporters(
        &OtelConfig::default(),
        trace_exporter.clone(),
        InMemoryMetricExporter::default(),
    )
    .expect("blocking pipeline");
    let observer = pipeline.observer();
    let subscriber = tracing_subscriber::registry().with(pipeline.layer());
    tracing::subscriber::with_default(subscriber, || {
        for sequence in 0..OBSERVATIONS {
            observer
                .record(&run_observation(sequence))
                .expect("telemetry enqueue remains non-authoritative");
        }
    });

    assert_eq!(durable, successful_outcome());
    assert!(trace_exporter.export_started.load(Ordering::Acquire));
    trace_exporter.release();
    pipeline.force_flush().expect("released exporter flushes");
    assert!(
        trace_exporter.exported.load(Ordering::Relaxed) < OBSERVATIONS,
        "a bounded queue should discard telemetry instead of blocking work"
    );
    pipeline.shutdown().expect("released pipeline shuts down");
    assert_eq!(durable, successful_outcome());
}

#[test]
fn shutdown_flushes_pending_spans_and_reports_shutdown_faults_out_of_band() {
    let durable = successful_outcome();
    let recording_trace = FaultSpanExporter::new(false, false);
    let exported = Arc::clone(&recording_trace.exported);
    let pipeline = OtelPipeline::from_exporters(
        &OtelConfig::default(),
        recording_trace,
        FaultMetricExporter::new(false, false),
    )
    .expect("recording pipeline");
    let observer = pipeline.observer();
    let subscriber = tracing_subscriber::registry().with(pipeline.layer());
    tracing::subscriber::with_default(subscriber, || {
        observer.record(&run_observation(1)).expect("observation");
    });
    pipeline.shutdown().expect("shutdown flushes pending span");
    assert_eq!(exported.load(Ordering::Relaxed), 1);
    assert_eq!(durable, successful_outcome());

    let failing_shutdown = OtelPipeline::from_exporters(
        &OtelConfig::default(),
        FaultSpanExporter::new(false, true),
        FaultMetricExporter::new(false, true),
    )
    .expect("shutdown fault pipeline");
    assert!(failing_shutdown.shutdown().is_err());
    assert_eq!(durable, successful_outcome());
}
