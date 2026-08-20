use std::fmt::{Display, Formatter};

/// Result type for resource contracts.
pub type ResourceResult<T> = std::result::Result<T, ResourceError>;

/// Stable local JSON Schema issue at a typed Artifact boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceSchemaIssue {
    /// Exact immutable codec that rejected the value.
    pub codec_id: String,
    /// JSON Pointer to the rejected instance location.
    pub instance_path: String,
    /// JSON Pointer to the rejecting schema keyword.
    pub schema_path: String,
    /// Human-readable validator detail; paths above remain the stable fields.
    pub message: String,
}

/// Stable resource contract errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceError {
    /// A descriptor, request, or response is malformed.
    Validation(String),
    /// A typed Artifact value violates its registered schema contract.
    Schema(ResourceSchemaIssue),
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
            Self::Schema(issue) => write!(
                formatter,
                "schema_failed: codec={} instance={} schema={}: {}",
                issue.codec_id, issue.instance_path, issue.schema_path, issue.message
            ),
            Self::Conflict(message) => write!(formatter, "conflict: {message}"),
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::Substrate(message) => write!(formatter, "substrate_failed: {message}"),
            Self::Persistence(message) => write!(formatter, "persistence_failed: {message}"),
            Self::Integrity(message) => write!(formatter, "integrity_failed: {message}"),
        }
    }
}

impl std::error::Error for ResourceError {}
