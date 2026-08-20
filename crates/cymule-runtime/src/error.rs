use std::fmt::{Display, Formatter};

/// Runtime result type.
pub type RuntimeResult<T> = std::result::Result<T, RuntimeError>;

/// Stable embedded-runtime errors.
#[derive(Debug)]
pub enum RuntimeError {
    /// Trusted semantic kernel rejected an operation.
    Core(cymule_core::CoreError),
    /// An executable Plan contract failed admission or value validation.
    Contract(crate::ContractViolation),
    /// Plugin protocol or behavior was invalid.
    PluginDefect {
        /// Stable host-owned defect code.
        code: String,
        /// Human-readable defect summary.
        message: String,
    },
    /// IR execution reached a durable wait unsupported by one-shot execution.
    Suspended(String),
    /// A concrete process or I/O substrate failed.
    Substrate {
        /// Stable substrate failure code.
        code: String,
        /// Human-readable failure summary.
        message: String,
    },
    /// JSON encoding failed.
    Encoding(String),
}

impl RuntimeError {
    /// Construct a host-classified plugin protocol defect.
    pub(crate) fn plugin_defect(message: impl Into<String>) -> Self {
        Self::PluginDefect {
            code: "plugin_protocol_violation".to_owned(),
            message: message.into(),
        }
    }

    /// Construct a process substrate failure.
    pub(crate) fn substrate(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Substrate {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl Display for RuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Core(error) => Display::fmt(error, formatter),
            Self::Contract(error) => write!(formatter, "contract_violation: {error}"),
            Self::PluginDefect { code, message } => {
                write!(formatter, "plugin_defect: {code}: {message}")
            }
            Self::Suspended(message) => write!(formatter, "run_suspended: {message}"),
            Self::Substrate { code, message } => {
                write!(formatter, "substrate_failed: {code}: {message}")
            }
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

impl From<crate::ContractViolation> for RuntimeError {
    fn from(error: crate::ContractViolation) -> Self {
        Self::Contract(error)
    }
}

impl From<crate::PlanAdmissionError> for RuntimeError {
    fn from(error: crate::PlanAdmissionError) -> Self {
        match error {
            crate::PlanAdmissionError::Core(error) => Self::Core(error),
            crate::PlanAdmissionError::Contract(error) => Self::Contract(error),
        }
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(error: std::io::Error) -> Self {
        Self::Substrate {
            code: "process_io_failed".to_owned(),
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for RuntimeError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(error.to_string())
    }
}
