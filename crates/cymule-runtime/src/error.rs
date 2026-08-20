use std::fmt::{Display, Formatter};

use crate::{PluginExpectedFailure, SuspensionBoundary};

/// Runtime result type.
pub type RuntimeResult<T> = std::result::Result<T, RuntimeError>;

/// Stable embedded-runtime errors.
#[derive(Debug)]
pub enum RuntimeError {
    /// Trusted semantic kernel rejected an operation.
    Core(cymule_core::CoreError),
    /// An executable Plan contract failed admission or value validation.
    Contract(crate::ContractViolation),
    /// A component returned an explicit application failure value.
    ExpectedPluginFailure(PluginExpectedFailure),
    /// Plugin protocol or behavior was invalid.
    PluginDefect {
        /// Stable host-owned defect code.
        code: String,
        /// Human-readable defect summary.
        message: String,
    },
    /// IR execution reached a durable wait unsupported by one-shot execution.
    Suspended(SuspensionBoundary),
    /// Prepared explicit effects require a caller-owned durable release control.
    ReleaseRequired {
        /// Exact prepared intent identities that remain unreleased.
        intent_ids: Vec<String>,
    },
    /// A concrete process or I/O substrate failed.
    Substrate {
        /// Stable substrate failure code.
        code: String,
        /// Human-readable failure summary.
        message: String,
    },
    /// A bounded plugin invocation exceeded its admitted deadline.
    TimedOut {
        /// Stable timeout code.
        code: String,
        /// Human-readable timeout summary.
        message: String,
    },
    /// Dispatch may have changed the external world without an observation.
    UnknownWorld {
        /// Stable ambiguity code.
        code: String,
        /// Human-readable ambiguity summary.
        message: String,
    },
    /// JSON encoding failed.
    Encoding(String),
}

impl RuntimeError {
    /// Construct a host-classified plugin protocol defect.
    pub fn plugin_defect(message: impl Into<String>) -> Self {
        Self::PluginDefect {
            code: "plugin_protocol_violation".to_owned(),
            message: message.into(),
        }
    }

    /// Construct a process substrate failure.
    pub fn substrate(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Substrate {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Construct a bounded invocation timeout.
    pub fn timed_out(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::TimedOut {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Construct an ambiguous external-world outcome.
    pub fn unknown_world(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::UnknownWorld {
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
            Self::ExpectedPluginFailure(error) => {
                write!(
                    formatter,
                    "expected_plugin_failure: {}: {}",
                    error.code, error.message
                )
            }
            Self::PluginDefect { code, message } => {
                write!(formatter, "plugin_defect: {code}: {message}")
            }
            Self::Suspended(boundary) => write!(
                formatter,
                "run_suspended: wait site {} reached binding {:?}",
                boundary.site_id, boundary.result_bind
            ),
            Self::ReleaseRequired { intent_ids } => write!(
                formatter,
                "effect_release_required: {}",
                intent_ids.join(",")
            ),
            Self::Substrate { code, message } => {
                write!(formatter, "substrate_failed: {code}: {message}")
            }
            Self::TimedOut { code, message } => write!(formatter, "timed_out: {code}: {message}"),
            Self::UnknownWorld { code, message } => {
                write!(formatter, "unknown_world: {code}: {message}")
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

impl From<crate::CompositionError> for RuntimeError {
    fn from(error: crate::CompositionError) -> Self {
        Self::PluginDefect {
            code: "execution_binding_rejected".to_owned(),
            message: error.to_string(),
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
