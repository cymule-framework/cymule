//! Reusable application layer for the durable evaluation campaign example.

/// Campaign orchestration, reports, and evolution commands.
pub mod campaign;
mod evolution;
mod model;
/// Deterministic process-plugin implementation used by the local quick start.
pub mod plugin;
mod source;

pub use campaign::{CampaignOptions, CampaignReport, EvolutionReport, FaultPoint, RunDisposition};
pub use model::{CaseOutput, EvaluationCase, Prediction, Score};
pub use source::parse_suite;
