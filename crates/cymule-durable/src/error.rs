use std::fmt::{Display, Formatter};

/// Result type for durable coordination.
pub type DurableResult<T> = std::result::Result<T, DurableError>;

/// Stable durable-store and coordination errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableError {
    /// Stored or proposed state is malformed.
    Validation(String),
    /// A conditional write observed another committed revision.
    Conflict {
        /// Caller-observed revision.
        expected: Option<String>,
        /// Current durable revision.
        current: Option<String>,
    },
    /// A requested durable object does not exist.
    NotFound(String),
    /// A legal state-machine edge was not available.
    IllegalTransition(String),
    /// A concrete substrate failed.
    Substrate(String),
    /// Canonical encoding or kernel restoration failed.
    Encoding(String),
}

impl Display for DurableError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::Conflict { expected, current } => {
                write!(
                    formatter,
                    "revision_conflict: expected {expected:?}, current {current:?}"
                )
            }
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::IllegalTransition(message) => {
                write!(formatter, "illegal_transition: {message}")
            }
            Self::Substrate(message) => write!(formatter, "substrate_failed: {message}"),
            Self::Encoding(message) => write!(formatter, "encoding_failed: {message}"),
        }
    }
}

impl std::error::Error for DurableError {}

impl From<cymule_core::CoreError> for DurableError {
    fn from(error: cymule_core::CoreError) -> Self {
        Self::Encoding(error.to_string())
    }
}

impl From<serde_json::Error> for DurableError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(error.to_string())
    }
}
