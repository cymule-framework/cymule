//! OpenTelemetry observation adapter for Cymule.

use std::collections::BTreeMap;
use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use serde::{Deserialize, Serialize};
use tracing::Level;
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

/// Bounded non-authoritative telemetry record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CymuleObservation {
    /// Observation category.
    pub kind: ObservationKind,
    /// Exact Run identity when applicable.
    pub run_id: Option<String>,
    /// Exact immutable Plan identity when applicable.
    pub plan_id: Option<String>,
    /// Exact occurrence identity when applicable.
    pub occurrence_id: Option<String>,
    /// Exact command/effect/activation identity when applicable.
    pub operation_id: Option<String>,
    /// Stable lifecycle or outcome name.
    pub outcome: String,
    /// Bounded scalar attributes; never payload content.
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

impl CymuleObservation {
    /// Validate the safe observation envelope.
    pub fn validate(&self) -> Result<(), String> {
        if self.outcome.is_empty()
            || self.outcome.len() > 128
            || self.outcome.chars().any(char::is_control)
        {
            return Err("observation outcome is invalid".to_owned());
        }
        for identity in [
            self.run_id.as_deref(),
            self.plan_id.as_deref(),
            self.occurrence_id.as_deref(),
            self.operation_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if identity.is_empty() || identity.len() > 512 || identity.chars().any(char::is_control)
            {
                return Err("observation identity is invalid".to_owned());
            }
        }
        if self.attributes.len() > 64
            || self.attributes.iter().any(|(key, value)| {
                key.is_empty()
                    || key.len() > 128
                    || value.len() > 1024
                    || key.chars().any(char::is_control)
                    || value.chars().any(char::is_control)
            })
        {
            return Err("observation attributes are invalid".to_owned());
        }
        Ok(())
    }
}

/// Stateless emitter into the application's active `tracing` subscriber.
#[derive(Debug, Clone, Copy, Default)]
pub struct OtelObserver;

impl OtelObserver {
    /// Emit one validated derived observation.
    pub fn record(&self, observation: &CymuleObservation) -> Result<(), String> {
        observation.validate()?;
        let kind = format!("{:?}", observation.kind).to_lowercase();
        tracing::event!(
            target: "cymule",
            Level::INFO,
            cymule.kind = kind.as_str(),
            cymule.run_id = observation.run_id.as_deref().unwrap_or(""),
            cymule.plan_id = observation.plan_id.as_deref().unwrap_or(""),
            cymule.occurrence_id = observation.occurrence_id.as_deref().unwrap_or(""),
            cymule.operation_id = observation.operation_id.as_deref().unwrap_or(""),
            cymule.outcome = observation.outcome.as_str(),
            cymule.attributes = ?observation.attributes,
            "cymule observation"
        );
        Ok(())
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
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:4318".to_owned(),
            service_name: "cymule".to_owned(),
            timeout: Duration::from_secs(5),
        }
    }
}

/// Tracer-provider guard; applications should flush it during graceful shutdown.
#[derive(Debug, Clone)]
pub struct OtelPipeline {
    provider: SdkTracerProvider,
}

impl OtelPipeline {
    /// Build an OTLP/HTTP trace provider without installing global state.
    pub fn build(config: &OtelConfig) -> Result<Self, String> {
        if config.endpoint.is_empty() || config.service_name.is_empty() || config.timeout.is_zero()
        {
            return Err("OTLP configuration is invalid".to_owned());
        }
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.timeout)
            .build()
            .map_err(|error| error.to_string())?;
        let resource = Resource::builder()
            .with_service_name(config.service_name.clone())
            .build();
        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_simple_exporter(exporter)
            .build();
        Ok(Self { provider })
    }

    /// Build a composable tracing layer backed by this provider.
    pub fn layer<S>(&self) -> OpenTelemetryLayer<S, SdkTracer>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_opentelemetry::layer().with_tracer(self.provider.tracer("cymule"))
    }

    /// Flush and stop the provider.
    pub fn shutdown(self) -> Result<(), String> {
        self.provider.shutdown().map_err(|error| error.to_string())
    }
}
