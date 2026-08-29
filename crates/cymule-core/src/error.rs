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
    /// A causal source is missing, a cut is not closed, or the event graph is cyclic.
    Causal(String),
    /// One exact keyed value or bounded index page was not supplied to a
    /// local pinned reduction.
    PinnedReadSetIncomplete {
        /// Closed persistent family expected by the reducer.
        family: &'static str,
        /// Exact key which must be resolved under the pinned family root.
        key: String,
    },
    /// A Scope closure cannot fit the complete bounded atomic-batch witness.
    PagedScopeRequired {
        /// Owning Run.
        run_id: String,
        /// Scope whose complete source exceeds the inline bound.
        scope_id: String,
        /// Exact source cardinality.
        entries: u64,
    },
    /// Exact replay requires an archived command proof resolved outside Core.
    ArchivedCommandReplayRequired {
        /// Requested command identity.
        command_id: String,
        /// Current content-addressed archive head.
        archive_head: String,
        /// Current cumulative command-index root.
        command_index_root: String,
    },
    /// Canonical provider failure retained without reclassification or parsing.
    /// Core performs no provider I/O; this preserves an upstream typed failure.
    CollectionProviderFailure(cymule_authenticated_collections::ProviderFailure),
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
            Self::PinnedReadSetIncomplete { .. } => "pinned_read_set_incomplete",
            Self::PagedScopeRequired { .. } => "paged_scope_required",
            Self::ArchivedCommandReplayRequired { .. } => "archived_command_replay_required",
            Self::CollectionProviderFailure(_) => "collection_provider_failed",
            Self::Encoding(_) => "encoding_failed",
        }
    }
}

impl Display for CoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::CollectionProviderFailure(failure) => {
                return write!(formatter, "{}: {failure}", self.code());
            }
            Self::Validation(message)
            | Self::NotFound(message)
            | Self::IdentityMismatch(message)
            | Self::IllegalTransition(message)
            | Self::CommandReuse(message)
            | Self::Causal(message)
            | Self::Encoding(message) => message,
            Self::PinnedReadSetIncomplete { family, key } => {
                return write!(
                    formatter,
                    "{}: exact {family} key {key} was not supplied",
                    self.code()
                );
            }
            Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => {
                return write!(
                    formatter,
                    "{}: Run {run_id} Scope {scope_id} has {entries} entries and requires the paged protocol",
                    self.code()
                );
            }
            Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => {
                return write!(
                    formatter,
                    "{}: command {command_id} requires proof from archive {archive_head} at index {command_index_root}",
                    self.code()
                );
            }
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

impl From<cymule_authenticated_collections::CollectionError> for CoreError {
    fn from(error: cymule_authenticated_collections::CollectionError) -> Self {
        use cymule_authenticated_collections::CollectionError;

        match error {
            CollectionError::Validation(message) => Self::Validation(message),
            CollectionError::Integrity { code, message } => {
                Self::IdentityMismatch(format!("{code}: {message}"))
            }
            CollectionError::MissingObject(object_id) => Self::IdentityMismatch(format!(
                "authenticated collection proof is missing node {object_id}"
            )),
            CollectionError::Conflict(message) => Self::IllegalTransition(message),
            CollectionError::Provider(failure) => Self::CollectionProviderFailure(failure),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_authenticated_collections::{CollectionError, ProviderConflict, ProviderFailure};

    #[test]
    fn collection_provider_failures_retain_category_code_and_conflict_evidence() {
        let failures = [
            ProviderFailure::Validation {
                message: "invalid provider input".to_owned(),
            },
            ProviderFailure::Integrity {
                code: "provider_digest_mismatch".to_owned(),
                message: "bad bytes".to_owned(),
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::Revision {
                    expected: Some("expected".to_owned()),
                    current: None,
                },
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::History {
                    code: "immutable_reuse".to_owned(),
                    message: "different record".to_owned(),
                },
            },
            ProviderFailure::Substrate {
                code: "disk_unavailable".to_owned(),
                message: "temporarily unavailable".to_owned(),
            },
        ];
        for failure in failures {
            let error = CoreError::from(CollectionError::Provider(failure.clone()));
            assert_eq!(error, CoreError::CollectionProviderFailure(failure));
            assert_eq!(error.code(), "collection_provider_failed");
            assert!(!matches!(error, CoreError::IdentityMismatch(_)));
        }
    }
}
