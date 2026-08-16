use std::fmt::{Display, Formatter};

/// Runtime result type.
pub type RuntimeResult<T> = std::result::Result<T, RuntimeError>;

/// Stable embedded-runtime errors.
#[derive(Debug)]
pub enum RuntimeError {
    /// Trusted semantic kernel rejected an operation.
    Core(cymule_core::CoreError),
    /// Plugin protocol or behavior was invalid.
    Plugin(String),
    /// IR execution reached a durable wait unsupported by one-shot execution.
    Suspended(String),
    /// Local process I/O failed.
    Io(String),
    /// JSON encoding failed.
    Encoding(String),
}

impl Display for RuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(error) => Display::fmt(error, formatter),
            Self::Plugin(message) => write!(formatter, "plugin_error: {message}"),
            Self::Suspended(message) => write!(formatter, "run_suspended: {message}"),
            Self::Io(message) => write!(formatter, "io_error: {message}"),
            Self::Encoding(message) => write!(formatter, "encoding_error: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<cymule_core::CoreError> for RuntimeError {
    fn from(error: cymule_core::CoreError) -> Self {
        Self::Core(error)
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for RuntimeError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(error.to_string())
    }
}
