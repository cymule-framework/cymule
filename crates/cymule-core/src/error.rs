use std::fmt::{Display, Formatter};

/// Result type used by the semantic kernel.
pub type Result<T> = std::result::Result<T, CoreError>;

/// Stable semantic error categories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    /// An object failed semantic validation.
    Validation(String),
    /// A referenced object does not exist.
    NotFound(String),
    /// An immutable identity did not match its content.
    IdentityMismatch(String),
    /// A state-machine transition is not legal.
    IllegalTransition(String),
    /// A command ID was reused with different semantics.
    CommandReuse(String),
    /// A causal parent is missing or the event graph is cyclic.
    Causal(String),
    /// Canonical encoding failed.
    Encoding(String),
}

impl CoreError {
    /// Stable machine-readable error code.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Validation(_) => "validation_failed",
            Self::NotFound(_) => "not_found",
            Self::IdentityMismatch(_) => "identity_mismatch",
            Self::IllegalTransition(_) => "illegal_transition",
            Self::CommandReuse(_) => "command_id_reused",
            Self::Causal(_) => "causal_error",
            Self::Encoding(_) => "encoding_failed",
        }
    }
}

impl Display for CoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Validation(message)
            | Self::NotFound(message)
            | Self::IdentityMismatch(message)
            | Self::IllegalTransition(message)
            | Self::CommandReuse(message)
            | Self::Causal(message)
            | Self::Encoding(message) => message,
        };
        write!(formatter, "{}: {message}", self.code())
    }
}

impl std::error::Error for CoreError {}

impl From<serde_json::Error> for CoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(error.to_string())
    }
}
