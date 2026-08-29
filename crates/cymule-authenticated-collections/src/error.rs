use std::error::Error;
use std::fmt;

/// Closed provider-conflict evidence preserved through collection I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderConflict {
    /// One exact compare-and-set revision expectation was stale.
    Revision {
        /// Caller-observed revision.
        expected: Option<String>,
        /// Current provider revision.
        current: Option<String>,
    },
    /// One immutable provider history identity was reused incompatibly.
    History {
        /// Stable provider-owned conflict code.
        code: String,
        /// Human-readable provider detail.
        message: String,
    },
}

/// Structured provider failure preserved without parsing display text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderFailure {
    /// The provider rejected caller-authored input.
    Validation {
        /// Human-readable provider detail.
        message: String,
    },
    /// Persisted provider state contradicted its own authority.
    Integrity {
        /// Stable provider-owned integrity code.
        code: String,
        /// Human-readable provider detail.
        message: String,
    },
    /// A provider revision or immutable-history expectation was stale.
    Conflict {
        /// Exact closed conflict evidence.
        evidence: ProviderConflict,
    },
    /// The provider substrate failed independently of collection semantics.
    Substrate {
        /// Stable provider-owned substrate code.
        code: String,
        /// Human-readable provider detail.
        message: String,
    },
}

impl fmt::Display for ProviderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation { message } => write!(formatter, "validation: {message}"),
            Self::Integrity { code, message } => write!(formatter, "integrity {code}: {message}"),
            Self::Conflict {
                evidence: ProviderConflict::Revision { expected, current },
            } => write!(
                formatter,
                "revision conflict: expected {expected:?}, current {current:?}"
            ),
            Self::Conflict {
                evidence: ProviderConflict::History { code, message },
            } => write!(formatter, "history conflict {code}: {message}"),
            Self::Substrate { code, message } => write!(formatter, "substrate {code}: {message}"),
        }
    }
}

/// Failure produced while validating or applying an authenticated collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectionError {
    /// A caller-authored shape is outside the closed collection contract.
    Validation(String),
    /// Authenticated bytes, paths, counts, or roots contradict each other.
    Integrity {
        /// Stable closed diagnostic code.
        code: &'static str,
        /// Human-readable failure detail.
        message: String,
    },
    /// An immutable node required by an authenticated root is unavailable.
    MissingObject(String),
    /// An exact mutation expectation does not match its authenticated parent.
    Conflict(String),
    /// The provider could not resolve an immutable collection object.
    Provider(ProviderFailure),
}

impl fmt::Display for CollectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation: {message}"),
            Self::Integrity { code, message } => write!(formatter, "integrity {code}: {message}"),
            Self::MissingObject(object_id) => {
                write!(formatter, "missing immutable collection object {object_id}")
            }
            Self::Conflict(message) => write!(formatter, "mutation conflict: {message}"),
            Self::Provider(failure) => write!(formatter, "collection provider: {failure}"),
        }
    }
}

impl Error for CollectionError {}

/// Result produced by authenticated collection operations.
pub type Result<T> = std::result::Result<T, CollectionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_failures_preserve_closed_category_evidence() {
        let failures = [
            ProviderFailure::Validation {
                message: "invalid request".to_owned(),
            },
            ProviderFailure::Integrity {
                code: "corrupt_node".to_owned(),
                message: "node is corrupt".to_owned(),
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::Revision {
                    expected: Some("old".to_owned()),
                    current: Some("new".to_owned()),
                },
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::History {
                    code: "command_reuse".to_owned(),
                    message: "command differs".to_owned(),
                },
            },
            ProviderFailure::Substrate {
                code: "io_failed".to_owned(),
                message: "provider unavailable".to_owned(),
            },
        ];

        for failure in failures {
            let error = CollectionError::Provider(failure.clone());
            assert_eq!(error, CollectionError::Provider(failure));
            assert!(error.to_string().starts_with("collection provider: "));
        }
    }
}
