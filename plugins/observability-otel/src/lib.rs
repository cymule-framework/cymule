//! OpenTelemetry observation adapter for Cymule.
//!
//! This crate deliberately contains no execution decisions. It translates
//! validated, bounded observations into traces and low-cardinality metrics.

use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider, SpanExporter};
use serde::{Deserialize, Serialize};
use tracing::Level;
use tracing::field;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::registry::LookupSpan;

/// One closed observation category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationKind {
    /// A Run lifecycle observation.
    Run,
    /// A component occurrence observation.
    Component,
    /// An effect preparation, dispatch, or reconciliation observation.
    Effect,
    /// A wait registration or activation observation.
    Wait,
    /// A virtual-work occurrence observation.
    VirtualWork,
    /// A live-evolution decision or transition observation.
    Evolution,
}

impl ObservationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Component => "component",
            Self::Effect => "effect",
            Self::Wait => "wait",
            Self::VirtualWork => "virtual_work",
            Self::Evolution => "evolution",
        }
    }
}

/// A bounded outcome used as the only metric dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationOutcome {
    /// Work was admitted or started.
    Started,
    /// Work completed successfully.
    Succeeded,
    /// Work ended in a known failure.
    Failed,
    /// Work was cancelled.
    Cancelled,
    /// Work exceeded its declared deadline.
    TimedOut,
    /// The external result is ambiguous and requires reconciliation.
    Unknown,
    /// An ambiguous result was reconciled.
    Reconciled,
    /// A wait was parked.
    Parked,
    /// A wait activation was admitted.
    Activated,
    /// Evolution promoted future work.
    Promoted,
    /// Evolution rolled future work back.
    RolledBack,
    /// Admission rejected the candidate.
    Rejected,
}

impl ObservationOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Unknown => "unknown",
            Self::Reconciled => "reconciled",
            Self::Parked => "parked",
            Self::Activated => "activated",
            Self::Promoted => "promoted",
            Self::RolledBack => "rolled_back",
            Self::Rejected => "rejected",
        }
    }
}

/// A closed error category. No error message or payload is exported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationErrorKind {
    /// Input or output violated a declared contract.
    Contract,
    /// Admission or authority rejected an operation.
    Admission,
    /// A plugin returned an expected failure.
    Plugin,
    /// A runtime defect terminated the operation.
    Defect,
    /// A provider or substrate failed.
    Substrate,
    /// The operation was cancelled.
    Cancelled,
    /// The operation timed out.
    TimedOut,
    /// The external result is ambiguous.
    UnknownWorld,
}

impl ObservationErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Contract => "contract",
            Self::Admission => "admission",
            Self::Plugin => "plugin",
            Self::Defect => "defect",
            Self::Substrate => "substrate",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::UnknownWorld => "unknown_world",
        }
    }
}

/// Exact identities attached to trace data only.
///
/// Identifiers never become metric labels. Provider payloads, arbitrary
/// attributes, URLs, prompts, and credentials have no field in this contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CymuleObservation {
    /// Observation category.
    pub kind: ObservationKind,
    /// Stable, low-cardinality outcome.
    pub outcome: ObservationOutcome,
    /// Exact Run identity when the observation belongs to a Run.
    pub run_id: Option<String>,
    /// Exact immutable Plan identity.
    pub plan_id: String,
    /// Exact component or virtual-work occurrence identity.
    pub occurrence_id: Option<String>,
    /// Exact command identity when the observation follows a command.
    pub command_id: Option<String>,
    /// Exact effect identity for an effect observation.
    pub effect_id: Option<String>,
    /// Exact wait identity for a wait observation.
    pub wait_id: Option<String>,
    /// Exact evolution decision identity for an evolution observation.
    pub evolution_id: Option<String>,
}

impl CymuleObservation {
    /// Validate the identity-rich, payload-free observation envelope.
    pub fn validate(&self) -> Result<(), ObservationContractError> {
        validate_identity("plan_id", Some(self.plan_id.as_str()))?;
        validate_identity("run_id", self.run_id.as_deref())?;
        validate_identity("occurrence_id", self.occurrence_id.as_deref())?;
        validate_identity("command_id", self.command_id.as_deref())?;
        validate_identity("effect_id", self.effect_id.as_deref())?;
        validate_identity("wait_id", self.wait_id.as_deref())?;
        validate_identity("evolution_id", self.evolution_id.as_deref())?;

        match self.kind {
            ObservationKind::Run => require(self.run_id.as_deref(), "run_id"),
            ObservationKind::Component | ObservationKind::VirtualWork => {
                require(self.run_id.as_deref(), "run_id")?;
                require(self.occurrence_id.as_deref(), "occurrence_id")
            }
            ObservationKind::Effect => {
                require(self.run_id.as_deref(), "run_id")?;
                require(self.occurrence_id.as_deref(), "occurrence_id")?;
                require(self.effect_id.as_deref(), "effect_id")
            }
            ObservationKind::Wait => {
                require(self.run_id.as_deref(), "run_id")?;
                require(self.occurrence_id.as_deref(), "occurrence_id")?;
                require(self.wait_id.as_deref(), "wait_id")
            }
            ObservationKind::Evolution => require(self.evolution_id.as_deref(), "evolution_id"),
        }
    }
}

fn require(value: Option<&str>, field_name: &'static str) -> Result<(), ObservationContractError> {
    if value.is_some() {
        Ok(())
    } else {
        Err(ObservationContractError::MissingIdentity(field_name))
    }
}

fn validate_identity(
    field_name: &'static str,
    value: Option<&str>,
) -> Result<(), ObservationContractError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        Err(ObservationContractError::InvalidIdentity(field_name))
    } else {
        Ok(())
    }
}

/// A validation failure before telemetry is emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationContractError {
    /// A category-required identity is absent.
    MissingIdentity(&'static str),
    /// An identity is empty, over the bound, or contains a control character.
    InvalidIdentity(&'static str),
}

impl std::fmt::Display for ObservationContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingIdentity(field) => write!(formatter, "missing observation {field}"),
            Self::InvalidIdentity(field) => write!(formatter, "invalid observation {field}"),
        }
    }
}

impl std::error::Error for ObservationContractError {}

#[derive(Clone)]
struct CymuleMetrics {
    run_outcomes: Counter<u64>,
    occurrence_outcomes: Counter<u64>,
    effect_outcomes: Counter<u64>,
    wait_outcomes: Counter<u64>,
    evolution_outcomes: Counter<u64>,
    active_runs: Gauge<u64>,
    active_occurrences: Gauge<u64>,
    backlog: Gauge<u64>,
    claim_duration: Histogram<f64>,
    reconcile_duration: Histogram<f64>,
    wait_duration: Histogram<f64>,
}

impl CymuleMetrics {
    fn new(provider: &SdkMeterProvider) -> Self {
        let meter = provider.meter("cymule");
        Self {
            run_outcomes: meter.u64_counter("cymule.run.outcomes").build(),
            occurrence_outcomes: meter.u64_counter("cymule.occurrence.outcomes").build(),
            effect_outcomes: meter.u64_counter("cymule.effect.outcomes").build(),
            wait_outcomes: meter.u64_counter("cymule.wait.outcomes").build(),
            evolution_outcomes: meter.u64_counter("cymule.evolution.outcomes").build(),
            active_runs: meter.u64_gauge("cymule.run.active").build(),
            active_occurrences: meter.u64_gauge("cymule.occurrence.active").build(),
            backlog: meter.u64_gauge("cymule.work.backlog").build(),
            claim_duration: meter
                .f64_histogram("cymule.claim.duration")
                .with_unit("s")
                .build(),
            reconcile_duration: meter
                .f64_histogram("cymule.reconcile.duration")
                .with_unit("s")
                .build(),
            wait_duration: meter
                .f64_histogram("cymule.wait.duration")
                .with_unit("s")
                .build(),
        }
    }

    fn record_outcome(&self, observation: &CymuleObservation) {
        let labels = [KeyValue::new("outcome", observation.outcome.as_str())];
        match observation.kind {
            ObservationKind::Run => self.run_outcomes.add(1, &labels),
            ObservationKind::Component | ObservationKind::VirtualWork => {
                self.occurrence_outcomes.add(1, &labels);
            }
            ObservationKind::Effect => self.effect_outcomes.add(1, &labels),
            ObservationKind::Wait => self.wait_outcomes.add(1, &labels),
            ObservationKind::Evolution => self.evolution_outcomes.add(1, &labels),
        }
    }
}

/// Current derived gauges. These values are never execution authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeGauges {
    /// Current active Run projection.
    pub active_runs: u64,
    /// Current active occurrence projection.
    pub active_occurrences: u64,
    /// Current materialized ready-work projection.
    pub backlog: u64,
}

/// One closed operation-duration category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurationKind {
    /// Time spent claiming work.
    Claim,
    /// Time spent reconciling an ambiguous effect.
    Reconcile,
    /// Time spent parked on a wait.
    Wait,
}

/// Stateless with respect to Cymule semantics; owns only `OTel` instruments.
#[derive(Clone)]
pub struct OtelObserver {
    metrics: CymuleMetrics,
}

/// A validated span bound to the exact observation that created it.
pub struct OtelObservationSpan {
    span: tracing::Span,
    observation: CymuleObservation,
}

impl OtelObservationSpan {
    /// Enter this span so subsequently created spans become its children.
    pub fn enter(&self) -> tracing::span::Entered<'_> {
        self.span.enter()
    }
}

impl OtelObserver {
    fn new(provider: &SdkMeterProvider) -> Self {
        Self {
            metrics: CymuleMetrics::new(provider),
        }
    }

    /// Create a trace span under the caller's current subscriber context.
    ///
    /// The returned span is not entered automatically. Entering a parent span
    /// before calling this method produces the expected parent-child topology.
    pub fn span(
        &self,
        observation: &CymuleObservation,
    ) -> Result<OtelObservationSpan, ObservationContractError> {
        observation.validate()?;
        let span = tracing::span!(
            target: "cymule",
            Level::INFO,
            "cymule.operation",
            otel.name = observation.kind.as_str(),
            otel.status_code = field::Empty,
            cymule.kind = observation.kind.as_str(),
            cymule.outcome = observation.outcome.as_str(),
            cymule.run_id = observation.run_id.as_deref().unwrap_or(""),
            cymule.plan_id = observation.plan_id.as_str(),
            cymule.occurrence_id = observation.occurrence_id.as_deref().unwrap_or(""),
            cymule.command_id = observation.command_id.as_deref().unwrap_or(""),
            cymule.effect_id = observation.effect_id.as_deref().unwrap_or(""),
            cymule.wait_id = observation.wait_id.as_deref().unwrap_or(""),
            cymule.evolution_id = observation.evolution_id.as_deref().unwrap_or(""),
        );
        Ok(OtelObservationSpan {
            span,
            observation: observation.clone(),
        })
    }

    /// Record a successful or non-error outcome on an existing span.
    pub fn record_on(&self, operation: &OtelObservationSpan) {
        operation.span.record("otel.status_code", "OK");
        tracing::event!(
            target: "cymule",
            parent: &operation.span,
            Level::INFO,
            cymule.kind = operation.observation.kind.as_str(),
            cymule.outcome = operation.observation.outcome.as_str(),
            "cymule outcome"
        );
        self.metrics.record_outcome(&operation.observation);
    }

    /// Record a failed outcome without exporting an error message or payload.
    pub fn record_error_on(&self, operation: &OtelObservationSpan, error: ObservationErrorKind) {
        operation.span.record("otel.status_code", "ERROR");
        tracing::event!(
            target: "cymule",
            parent: &operation.span,
            Level::ERROR,
            error.kind = error.as_str(),
            cymule.kind = operation.observation.kind.as_str(),
            cymule.outcome = operation.observation.outcome.as_str(),
            "cymule operation failed"
        );
        self.metrics.record_outcome(&operation.observation);
    }

    /// Emit a complete one-shot observation as one span and one outcome event.
    pub fn record(&self, observation: &CymuleObservation) -> Result<(), ObservationContractError> {
        let operation = self.span(observation)?;
        self.record_on(&operation);
        Ok(())
    }

    /// Record current active/backlog projections without identity labels.
    pub fn record_gauges(&self, gauges: RuntimeGauges) {
        self.metrics.active_runs.record(gauges.active_runs, &[]);
        self.metrics
            .active_occurrences
            .record(gauges.active_occurrences, &[]);
        self.metrics.backlog.record(gauges.backlog, &[]);
    }

    /// Record a bounded duration with only the closed outcome dimension.
    pub fn record_duration(
        &self,
        kind: DurationKind,
        duration: Duration,
        outcome: ObservationOutcome,
    ) {
        let labels = [KeyValue::new("outcome", outcome.as_str())];
        let seconds = duration.as_secs_f64();
        match kind {
            DurationKind::Claim => self.metrics.claim_duration.record(seconds, &labels),
            DurationKind::Reconcile => self.metrics.reconcile_duration.record(seconds, &labels),
            DurationKind::Wait => self.metrics.wait_duration.record(seconds, &labels),
        }
    }
}

/// OTLP HTTP exporter configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtelConfig {
    /// OTLP HTTP endpoint, normally a Collector.
    pub endpoint: String,
    /// OpenTelemetry service name.
    pub service_name: String,
    /// Export timeout.
    pub timeout: Duration,
    /// Metrics export interval.
    pub metric_export_interval: Duration,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:4318".to_owned(),
            service_name: "cymule".to_owned(),
            timeout: Duration::from_secs(5),
            metric_export_interval: Duration::from_mins(1),
        }
    }
}

/// Trace and metric provider guard; applications own subscriber installation.
#[derive(Debug)]
pub struct OtelPipeline {
    trace_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
}

impl OtelPipeline {
    /// Build OTLP/HTTP trace and metric providers without installing globals.
    pub fn build(config: &OtelConfig) -> Result<Self, String> {
        validate_config(config)?;
        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .build()
            .map_err(|error| error.to_string())?;
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .build()
            .map_err(|error| error.to_string())?;
        Self::from_exporters(config, trace_exporter, metric_exporter)
    }

    /// Compose providers from application-selected official `OTel` exporters.
    ///
    /// This constructor exists for alternate transports and deterministic
    /// recording/fault exporters; exporters remain observation-only.
    pub fn from_exporters<T, M>(
        config: &OtelConfig,
        trace_exporter: T,
        metric_exporter: M,
    ) -> Result<Self, String>
    where
        T: SpanExporter + 'static,
        M: PushMetricExporter + 'static,
    {
        validate_config(config)?;
        let resource = Resource::builder()
            .with_service_name(config.service_name.clone())
            .build();
        let trace_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(trace_exporter)
            .build();
        let metric_reader = opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
            .with_interval(config.metric_export_interval)
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(metric_reader)
            .build();
        Ok(Self {
            trace_provider,
            meter_provider,
        })
    }

    /// Build the bounded observer backed by this pipeline's meter provider.
    pub fn observer(&self) -> OtelObserver {
        OtelObserver::new(&self.meter_provider)
    }

    /// Build a composable tracing layer backed by this provider.
    pub fn layer<S>(&self) -> OpenTelemetryLayer<S, SdkTracer>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_opentelemetry::layer().with_tracer(self.trace_provider.tracer("cymule"))
    }

    /// Flush all trace and metric exporters.
    pub fn force_flush(&self) -> Result<(), String> {
        combine_results(
            self.trace_provider.force_flush(),
            self.meter_provider.force_flush(),
        )
    }

    /// Flush and stop every provider, reporting operational failures only.
    pub fn shutdown(self) -> Result<(), String> {
        combine_results(
            self.trace_provider.shutdown(),
            self.meter_provider.shutdown(),
        )
    }
}

fn validate_config(config: &OtelConfig) -> Result<(), String> {
    if config.endpoint.is_empty()
        || config.service_name.is_empty()
        || config.timeout.is_zero()
        || config.metric_export_interval.is_zero()
    {
        Err("OTLP configuration is invalid".to_owned())
    } else {
        Ok(())
    }
}

fn combine_results(
    trace: opentelemetry_sdk::error::OTelSdkResult,
    metrics: opentelemetry_sdk::error::OTelSdkResult,
) -> Result<(), String> {
    match (trace, metrics) {
        (Ok(()), Ok(())) => Ok(()),
        (trace, metrics) => Err(format!(
            "telemetry provider failure: trace={:?}, metrics={:?}",
            trace.err(),
            metrics.err()
        )),
    }
}
