use std::fmt::{Display, Formatter};

/// Result type for live evolution.
pub type EvolutionResult<T> = std::result::Result<T, EvolutionError>;

/// Stable live-evolution errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionError {
    /// Plan, patch, rollout, or migration data is malformed.
    Validation(String),
    /// An executable Plan contract failed admission or value validation.
    Contract(cymule_runtime::ContractViolation),
    /// A referenced Plan or decision is absent.
    NotFound(String),
    /// A DAG, pin, rollout, or migration transition conflicts with history.
    Conflict(String),
}

impl Display for EvolutionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::Contract(error) => write!(formatter, "contract_violation: {error}"),
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
        }
    }
}

impl std::error::Error for EvolutionError {}

impl From<cymule_core::CoreError> for EvolutionError {
    fn from(error: cymule_core::CoreError) -> Self {
        Self::Validation(error.to_string())
    }
}

impl From<cymule_runtime::ContractViolation> for EvolutionError {
    fn from(error: cymule_runtime::ContractViolation) -> Self {
        Self::Contract(error)
    }
}

impl From<cymule_runtime::PlanAdmissionError> for EvolutionError {
    fn from(error: cymule_runtime::PlanAdmissionError) -> Self {
        match error {
            cymule_runtime::PlanAdmissionError::Core(error) => Self::from(error),
            cymule_runtime::PlanAdmissionError::Contract(error) => Self::Contract(error),
        }
    }
}
