use std::fmt::{Display, Formatter};

/// Result type for provider-neutral durable protocol values.
pub type DurableProtocolResult<T> = std::result::Result<T, DurableProtocolError>;

/// Stable failures produced by pure durable protocol verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableProtocolError {
    /// A closed wire value is malformed or internally inconsistent.
    Validation(String),
    /// A referenced provider-neutral authority is absent.
    NotFound {
        /// Human-readable description of the exact missing authority.
        message: String,
    },
    /// A proposed provider-neutral transition is not legal.
    IllegalTransition(String),
    /// An exact Scope closure requires the bounded paged preparation path.
    PagedScopeRequired {
        /// Owning Run.
        run_id: String,
        /// Scope requiring pagination.
        scope_id: String,
        /// Exact source cardinality.
        entries: u64,
    },
    /// An immutable command identity was reused with different semantics.
    Conflict {
        /// Stable machine-readable conflict code.
        code: String,
        /// Human-readable conflict summary.
        message: String,
    },
    /// Causal or pinned-read evidence contradicted exact protocol authority.
    Integrity {
        /// Stable machine-readable integrity code.
        code: String,
        /// Human-readable integrity summary.
        message: String,
    },
    /// A content-derived identity does not match its complete preimage.
    IdentityMismatch(String),
    /// Exact command replay requires the retained cold Machine archive.
    ArchivedCommandReplayRequired {
        /// Requested canonical command identity.
        command_id: String,
        /// Current content-addressed archive head.
        archive_head: String,
        /// Current cumulative archived-command index root.
        command_index_root: String,
    },
    /// Lossless canonical collection-provider failure, not a proof corruption.
    CollectionProviderFailure(cymule_authenticated_collections::ProviderFailure),
    /// Canonical encoding or identity derivation failed.
    Encoding(String),
}

impl Display for DurableProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollectionProviderFailure(failure) => {
                write!(formatter, "collection_provider_failed: {failure}")
            }
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::NotFound { message } => write!(formatter, "not_found: {message}"),
            Self::IllegalTransition(message) => {
                write!(formatter, "illegal_transition: {message}")
            }
            Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => write!(
                formatter,
                "paged_scope_required: Run {run_id} Scope {scope_id} has {entries} entries"
            ),
            Self::Conflict { code, message } => {
                write!(formatter, "conflict: {code}: {message}")
            }
            Self::Integrity { code, message } => {
                write!(formatter, "integrity_failed: {code}: {message}")
            }
            Self::IdentityMismatch(message) => {
                write!(formatter, "identity_mismatch: {message}")
            }
            Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => write!(
                formatter,
                "archived_command_replay_required: command {command_id} requires proof from archive {archive_head} at index {command_index_root}"
            ),
            Self::Encoding(message) => write!(formatter, "encoding_failed: {message}"),
        }
    }
}

impl std::error::Error for DurableProtocolError {}

impl From<cymule_core::CoreError> for DurableProtocolError {
    fn from(error: cymule_core::CoreError) -> Self {
        match error {
            cymule_core::CoreError::CollectionProviderFailure(failure) => {
                Self::CollectionProviderFailure(failure)
            }
            cymule_core::CoreError::Validation(message) => Self::Validation(message),
            cymule_core::CoreError::NotFound(message) => Self::NotFound { message },
            cymule_core::CoreError::IllegalTransition(message) => Self::IllegalTransition(message),
            cymule_core::CoreError::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            },
            cymule_core::CoreError::CommandReuse(message) => Self::Conflict {
                code: "command_id_reused".to_owned(),
                message,
            },
            cymule_core::CoreError::Causal(message) => Self::Integrity {
                code: "causal_error".to_owned(),
                message,
            },
            cymule_core::CoreError::PinnedReadSetIncomplete { family, key } => Self::Integrity {
                code: "pinned_read_set_incomplete".to_owned(),
                message: format!("exact {family} key {key} was not supplied"),
            },
            cymule_core::CoreError::IdentityMismatch(message) => Self::IdentityMismatch(message),
            cymule_core::CoreError::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            },
            cymule_core::CoreError::Encoding(message) => Self::Encoding(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_provider_failure_is_lossless_across_core_boundary() {
        use cymule_authenticated_collections::{ProviderConflict, ProviderFailure};
        for failure in [
            ProviderFailure::Validation {
                message: "invalid".to_owned(),
            },
            ProviderFailure::Integrity {
                code: "digest_mismatch".to_owned(),
                message: "bad bytes".to_owned(),
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::Revision {
                    expected: None,
                    current: Some("current".to_owned()),
                },
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::History {
                    code: "immutable_reuse".to_owned(),
                    message: "reused".to_owned(),
                },
            },
            ProviderFailure::Substrate {
                code: "provider_unavailable".to_owned(),
                message: "unavailable".to_owned(),
            },
        ] {
            assert_eq!(
                DurableProtocolError::from(cymule_core::CoreError::CollectionProviderFailure(
                    failure.clone()
                )),
                DurableProtocolError::CollectionProviderFailure(failure)
            );
        }
    }

    #[test]
    fn paged_scope_requirement_preserves_its_exact_target() {
        assert_eq!(
            DurableProtocolError::from(cymule_core::CoreError::PagedScopeRequired {
                run_id: "run:paged".to_owned(),
                scope_id: "scope:paged".to_owned(),
                entries: 257,
            }),
            DurableProtocolError::PagedScopeRequired {
                run_id: "run:paged".to_owned(),
                scope_id: "scope:paged".to_owned(),
                entries: 257,
            }
        );
    }

    #[test]
    fn core_failures_preserve_closed_protocol_categories_and_fields() {
        assert_eq!(
            DurableProtocolError::from(cymule_core::CoreError::NotFound("missing".to_owned())),
            DurableProtocolError::NotFound {
                message: "missing".to_owned(),
            }
        );
        assert_eq!(
            DurableProtocolError::from(cymule_core::CoreError::IllegalTransition(
                "closed".to_owned(),
            )),
            DurableProtocolError::IllegalTransition("closed".to_owned())
        );
        assert_eq!(
            DurableProtocolError::from(cymule_core::CoreError::CommandReuse("reused".to_owned())),
            DurableProtocolError::Conflict {
                code: "command_id_reused".to_owned(),
                message: "reused".to_owned(),
            }
        );
        assert_eq!(
            DurableProtocolError::from(cymule_core::CoreError::Causal("cycle".to_owned())),
            DurableProtocolError::Integrity {
                code: "causal_error".to_owned(),
                message: "cycle".to_owned(),
            }
        );
        assert_eq!(
            DurableProtocolError::from(cymule_core::CoreError::PinnedReadSetIncomplete {
                family: "Run",
                key: "run:test".to_owned(),
            }),
            DurableProtocolError::Integrity {
                code: "pinned_read_set_incomplete".to_owned(),
                message: "exact Run key run:test was not supplied".to_owned(),
            }
        );
        assert_eq!(
            DurableProtocolError::from(cymule_core::CoreError::ArchivedCommandReplayRequired {
                command_id: "command:test".to_owned(),
                archive_head: "archive:test".to_owned(),
                command_index_root: "index:test".to_owned(),
            }),
            DurableProtocolError::ArchivedCommandReplayRequired {
                command_id: "command:test".to_owned(),
                archive_head: "archive:test".to_owned(),
                command_index_root: "index:test".to_owned(),
            }
        );
    }
}
