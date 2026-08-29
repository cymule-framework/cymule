use std::fmt::{Display, Formatter};

/// Result type for closed cross-profile persistence values.
pub type ProtocolResult<T> = std::result::Result<T, ProtocolError>;

/// Closed failure categories shared by pure profile contracts and their exact
/// provider or persistence boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// A closed wire value is malformed.
    Validation(String),
    /// A proposed transition is structurally impossible.
    IllegalTransition(String),
    /// An exact Scope closure needs paged preparation rather than an inline batch.
    PagedScopeRequired {
        /// Owning Run.
        run_id: String,
        /// Scope requiring pagination.
        scope_id: String,
        /// Exact source cardinality.
        entries: u64,
    },
    /// A stable semantic identity or optimistic authority was reused with
    /// different content.
    Conflict {
        /// Stable machine-readable conflict code.
        code: String,
        /// Human-readable conflict summary.
        message: String,
    },
    /// A referenced profile-owned authority is absent.
    NotFound {
        /// Human-readable description of the exact missing authority.
        message: String,
    },
    /// A profile provider or substrate failed before producing semantic
    /// authority.
    Substrate {
        /// Stable machine-readable substrate code.
        code: String,
        /// Human-readable substrate summary.
        message: String,
    },
    /// Profile-owned durable state could not be committed or replayed.
    Persistence {
        /// Stable machine-readable persistence code.
        code: String,
        /// Human-readable persistence summary.
        message: String,
    },
    /// A mutating profile operation may have committed, but its authoritative
    /// receipt was not observed and the caller must reconcile before retrying.
    CommitOutcomeUnknown {
        /// Human-readable reconciliation context.
        message: String,
    },
    /// Exact command replay requires the retained cold Machine archive.
    ArchivedCommandReplayRequired {
        /// Requested canonical command identity.
        command_id: String,
        /// Current content-addressed archive head.
        archive_head: String,
        /// Current cumulative archived-command index root.
        command_index_root: String,
    },
    /// Retrieved immutable bytes, proofs, or content identities disagreed.
    Integrity {
        /// Stable machine-readable integrity code.
        code: String,
        /// Human-readable integrity summary.
        message: String,
    },
    /// A content-derived identity does not match its complete preimage.
    IdentityMismatch(String),
    /// A JSON Schema contract rejected a typed value.
    Contract(cymule_runtime::ContractViolation),
    /// Lossless canonical collection-provider failure, not a proof corruption.
    CollectionProviderFailure(cymule_authenticated_collections::ProviderFailure),
    /// Canonical encoding or identity derivation failed.
    Encoding(String),
}

impl Display for ProtocolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollectionProviderFailure(failure) => {
                write!(formatter, "collection_provider_failed: {failure}")
            }
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
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
            Self::NotFound { message } => write!(formatter, "not_found: {message}"),
            Self::Substrate { code, message } => {
                write!(formatter, "substrate_failed: {code}: {message}")
            }
            Self::Persistence { code, message } => {
                write!(formatter, "persistence_failed: {code}: {message}")
            }
            Self::CommitOutcomeUnknown { message } => {
                write!(formatter, "commit_outcome_unknown: {message}")
            }
            Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => write!(
                formatter,
                "archived_command_replay_required: command {command_id} requires proof from archive {archive_head} at index {command_index_root}"
            ),
            Self::Integrity { code, message } => {
                write!(formatter, "integrity_failed: {code}: {message}")
            }
            Self::IdentityMismatch(message) => {
                write!(formatter, "identity_mismatch: {message}")
            }
            Self::Contract(error) => write!(formatter, "contract_violation: {error}"),
            Self::Encoding(message) => write!(formatter, "encoding_failed: {message}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<cymule_core::CoreError> for ProtocolError {
    fn from(error: cymule_core::CoreError) -> Self {
        match error {
            cymule_core::CoreError::CollectionProviderFailure(failure) => {
                Self::CollectionProviderFailure(failure)
            }
            cymule_core::CoreError::Validation(message) => Self::Validation(message),
            cymule_core::CoreError::NotFound(message) => Self::NotFound { message },
            cymule_core::CoreError::IdentityMismatch(message) => Self::IdentityMismatch(message),
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

impl From<cymule_runtime::ContractViolation> for ProtocolError {
    fn from(error: cymule_runtime::ContractViolation) -> Self {
        Self::Contract(error)
    }
}

impl From<cymule_durable_protocol::DurableProtocolError> for ProtocolError {
    fn from(error: cymule_durable_protocol::DurableProtocolError) -> Self {
        match error {
            cymule_durable_protocol::DurableProtocolError::CollectionProviderFailure(failure) => {
                Self::CollectionProviderFailure(failure)
            }
            cymule_durable_protocol::DurableProtocolError::Validation(message) => {
                Self::Validation(message)
            }
            cymule_durable_protocol::DurableProtocolError::NotFound { message } => {
                Self::NotFound { message }
            }
            cymule_durable_protocol::DurableProtocolError::IllegalTransition(message) => {
                Self::IllegalTransition(message)
            }
            cymule_durable_protocol::DurableProtocolError::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            },
            cymule_durable_protocol::DurableProtocolError::Conflict { code, message } => {
                Self::Conflict { code, message }
            }
            cymule_durable_protocol::DurableProtocolError::Integrity { code, message } => {
                Self::Integrity { code, message }
            }
            cymule_durable_protocol::DurableProtocolError::IdentityMismatch(message) => {
                Self::IdentityMismatch(message)
            }
            cymule_durable_protocol::DurableProtocolError::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            },
            cymule_durable_protocol::DurableProtocolError::Encoding(message) => {
                Self::Encoding(message)
            }
        }
    }
}

impl From<serde_json::Error> for ProtocolError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_provider_failure_keeps_the_same_fields_across_both_protocol_paths() {
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
                    expected: Some("old".to_owned()),
                    current: Some("new".to_owned()),
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
            let direct = ProtocolError::from(cymule_core::CoreError::CollectionProviderFailure(
                failure.clone(),
            ));
            let shared = ProtocolError::from(
                cymule_durable_protocol::DurableProtocolError::CollectionProviderFailure(
                    failure.clone(),
                ),
            );
            assert_eq!(direct, ProtocolError::CollectionProviderFailure(failure));
            assert_eq!(direct, shared);
        }
    }

    #[test]
    fn paged_scope_requirement_keeps_the_same_fields_across_protocol_layers() {
        let direct = ProtocolError::from(cymule_core::CoreError::PagedScopeRequired {
            run_id: "run:paged".to_owned(),
            scope_id: "scope:paged".to_owned(),
            entries: 257,
        });
        let shared = ProtocolError::from(
            cymule_durable_protocol::DurableProtocolError::PagedScopeRequired {
                run_id: "run:paged".to_owned(),
                scope_id: "scope:paged".to_owned(),
                entries: 257,
            },
        );
        assert_eq!(direct, shared);
        assert_eq!(
            direct,
            ProtocolError::PagedScopeRequired {
                run_id: "run:paged".to_owned(),
                scope_id: "scope:paged".to_owned(),
                entries: 257,
            }
        );
    }

    #[test]
    fn core_failures_preserve_profile_categories_and_stable_codes() {
        assert_eq!(
            ProtocolError::from(cymule_core::CoreError::NotFound("missing".to_owned())),
            ProtocolError::NotFound {
                message: "missing".to_owned(),
            }
        );
        assert_eq!(
            ProtocolError::from(cymule_core::CoreError::CommandReuse("reused".to_owned())),
            ProtocolError::Conflict {
                code: "command_id_reused".to_owned(),
                message: "reused".to_owned(),
            }
        );
        assert_eq!(
            ProtocolError::from(cymule_core::CoreError::Causal("cycle".to_owned())),
            ProtocolError::Integrity {
                code: "causal_error".to_owned(),
                message: "cycle".to_owned(),
            }
        );
        assert_eq!(
            ProtocolError::from(cymule_core::CoreError::ArchivedCommandReplayRequired {
                command_id: "command:test".to_owned(),
                archive_head: "archive:test".to_owned(),
                command_index_root: "index:test".to_owned(),
            }),
            ProtocolError::ArchivedCommandReplayRequired {
                command_id: "command:test".to_owned(),
                archive_head: "archive:test".to_owned(),
                command_index_root: "index:test".to_owned(),
            }
        );
    }

    #[test]
    fn durable_protocol_failures_preserve_profile_categories_and_fields() {
        assert_eq!(
            ProtocolError::from(cymule_durable_protocol::DurableProtocolError::Conflict {
                code: "command_id_reused".to_owned(),
                message: "reused".to_owned(),
            }),
            ProtocolError::Conflict {
                code: "command_id_reused".to_owned(),
                message: "reused".to_owned(),
            }
        );
        assert_eq!(
            ProtocolError::from(cymule_durable_protocol::DurableProtocolError::Integrity {
                code: "causal_error".to_owned(),
                message: "cycle".to_owned(),
            }),
            ProtocolError::Integrity {
                code: "causal_error".to_owned(),
                message: "cycle".to_owned(),
            }
        );
        assert_eq!(
            ProtocolError::from(
                cymule_durable_protocol::DurableProtocolError::ArchivedCommandReplayRequired {
                    command_id: "command:test".to_owned(),
                    archive_head: "archive:test".to_owned(),
                    command_index_root: "index:test".to_owned(),
                },
            ),
            ProtocolError::ArchivedCommandReplayRequired {
                command_id: "command:test".to_owned(),
                archive_head: "archive:test".to_owned(),
                command_index_root: "index:test".to_owned(),
            }
        );
    }
}
