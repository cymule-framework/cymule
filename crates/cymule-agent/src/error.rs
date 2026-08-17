use std::fmt::{Display, Formatter};

/// Result type for agent interaction contracts.
pub type AgentResult<T> = std::result::Result<T, AgentError>;

/// Stable agent interaction errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentError {
    /// An update or request was malformed.
    Validation(String),
    /// A session transition was not legal.
    IllegalTransition(String),
    /// A referenced interaction object was missing.
    NotFound(String),
    /// A host adapter failed.
    Host(String),
}

impl Display for AgentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::IllegalTransition(message) => {
                write!(formatter, "illegal_transition: {message}")
            }
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::Host(message) => write!(formatter, "host_failed: {message}"),
        }
    }
}

impl std::error::Error for AgentError {}
