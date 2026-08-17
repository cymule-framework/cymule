use std::fmt::{Display, Formatter};

/// Result type for virtual-work scheduling.
pub type VirtualResult<T> = std::result::Result<T, VirtualError>;

/// Stable virtual-work errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VirtualError {
    /// Region, cursor, item, or limits are malformed.
    Validation(String),
    /// A referenced region or work item is absent.
    NotFound(String),
    /// A claim or completion transition is stale or illegal.
    Conflict(String),
    /// A source adapter failed.
    Source(String),
    /// M1 durable checkpoint or CAS failed.
    Durable(String),
}

impl Display for VirtualError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::Source(message) => write!(formatter, "source_failed: {message}"),
            Self::Durable(message) => write!(formatter, "durable_failed: {message}"),
        }
    }
}

impl std::error::Error for VirtualError {}
