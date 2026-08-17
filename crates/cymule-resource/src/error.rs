use std::fmt::{Display, Formatter};

/// Result type for resource contracts.
pub type ResourceResult<T> = std::result::Result<T, ResourceError>;

/// Stable resource contract errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceError {
    /// A descriptor, request, or response is malformed.
    Validation(String),
    /// A stable identity was reused with different semantics.
    Conflict(String),
    /// A referenced resource, Run, or transfer is absent.
    NotFound(String),
    /// A resolver or store adapter failed.
    Substrate(String),
    /// Durable handoff state could not be committed or replayed.
    Persistence(String),
    /// Retrieved bytes or immutable-version evidence did not match.
    Integrity(String),
}

impl Display for ResourceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::Substrate(message) => write!(formatter, "substrate_failed: {message}"),
            Self::Persistence(message) => write!(formatter, "persistence_failed: {message}"),
            Self::Integrity(message) => write!(formatter, "integrity_failed: {message}"),
        }
    }
}

impl std::error::Error for ResourceError {}
