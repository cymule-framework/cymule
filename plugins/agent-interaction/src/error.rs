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
    /// The requested Scope closure requires the paged preparation protocol.
    PagedScopeRequired {
        /// Owning Run.
        run_id: String,
        /// Scope requiring pagination.
        scope_id: String,
        /// Exact source cardinality.
        entries: u64,
    },
    /// A referenced interaction object was missing.
    NotFound(String),
    /// A host adapter failed.
    Host(String),
    /// A stable profile identity or optimistic authority conflicted.
    Conflict {
        /// Stable conflict code.
        code: String,
        /// Human-readable conflict summary.
        message: String,
    },
    /// Another live durable execution claim owns the requested Run.
    Busy {
        /// Contended Run identity.
        run_id: String,
        /// Current execution owner.
        owner: String,
        /// Current execution fence.
        fence: u64,
    },
    /// A profile provider or substrate failed before producing authority.
    Substrate {
        /// Stable substrate failure code.
        code: String,
        /// Human-readable substrate failure summary.
        message: String,
    },
    /// Durable interaction storage failed or was temporarily unavailable.
    Persistence {
        /// Stable persistence-layer failure code.
        code: String,
        /// Human-readable persistence failure summary.
        message: String,
    },
    /// A selected runtime implementation violated its closed protocol.
    RuntimeDefect {
        /// Stable runtime-defect code.
        code: String,
        /// Human-readable runtime-defect summary.
        message: String,
    },
    /// Retrieved immutable profile evidence disagreed with its identity.
    Integrity {
        /// Stable integrity failure code.
        code: String,
        /// Human-readable integrity failure summary.
        message: String,
    },
    /// Canonical encoding failed after semantic validation.
    Encoding {
        /// Human-readable encoding failure summary.
        message: String,
    },
    /// The owning durable transition may have committed, but its receipt was lost.
    CommitOutcomeUnknown {
        /// Human-readable detail about the uncertain durable commit response.
        message: String,
    },
    /// A started host call has no authoritative terminal outcome.
    HostOutcomeUnknown {
        /// Exact occurrence which must be reconciled before redispatch.
        occurrence_id: String,
    },
    /// An exact Effect intent requires binding-pinned reconciliation.
    ReconciliationRequired {
        /// Original semantic Effect intent.
        intent_id: String,
    },
    /// Exact command replay requires the retained cold Machine archive.
    ArchivedCommandReplayRequired {
        /// Requested canonical command identity.
        command_id: String,
        /// Current content-addressed archive head.
        archive_head: String,
        /// Current archived-command index root.
        command_index_root: String,
    },
    /// The invocation owner cancelled work before ambiguous dispatch.
    Cancelled {
        /// Stable cancellation code.
        code: String,
        /// Human-readable cancellation summary.
        message: String,
    },
    /// Work timed out before ambiguous external dispatch.
    TimedOut {
        /// Stable timeout code.
        code: String,
        /// Human-readable timeout summary.
        message: String,
    },
    /// A started or ambiguous host occurrence requires adapter-owned recovery.
    /// Durable reconciliation and archived replay use the typed variants above.
    RecoveryRequired(String),
}

impl Display for AgentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
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
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::Host(message) => write!(formatter, "host_failed: {message}"),
            Self::Conflict { code, message } => {
                write!(formatter, "conflict: {code}: {message}")
            }
            Self::Busy {
                run_id,
                owner,
                fence,
            } => write!(
                formatter,
                "execution_busy: Run {run_id} is owned by {owner} at fence {fence}"
            ),
            Self::Substrate { code, message } => {
                write!(formatter, "substrate_failed: {code}: {message}")
            }
            Self::Persistence { code, message } => {
                write!(formatter, "persistence_failed: {code}: {message}")
            }
            Self::RuntimeDefect { code, message } => {
                write!(formatter, "runtime_defect: {code}: {message}")
            }
            Self::Integrity { code, message } => {
                write!(formatter, "integrity_failed: {code}: {message}")
            }
            Self::Encoding { message } => write!(formatter, "encoding_failed: {message}"),
            Self::CommitOutcomeUnknown { message } => {
                write!(formatter, "commit_outcome_unknown: {message}")
            }
            Self::HostOutcomeUnknown { occurrence_id } => write!(
                formatter,
                "host_outcome_unknown: occurrence {occurrence_id} requires reconciliation"
            ),
            Self::ReconciliationRequired { intent_id } => write!(
                formatter,
                "reconciliation_required: Effect {intent_id} remains unknown"
            ),
            Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => write!(
                formatter,
                "archived_command_replay_required: command {command_id} requires proof from archive {archive_head} at index {command_index_root}"
            ),
            Self::Cancelled { code, message } => {
                write!(formatter, "cancelled: {code}: {message}")
            }
            Self::TimedOut { code, message } => {
                write!(formatter, "timed_out: {code}: {message}")
            }
            Self::RecoveryRequired(message) => {
                write!(formatter, "recovery_required: {message}")
            }
        }
    }
}

impl std::error::Error for AgentError {}

impl AgentError {
    pub(crate) fn persistence(code: &str, message: impl Into<String>) -> Self {
        Self::Persistence {
            code: code.to_owned(),
            message: message.into(),
        }
    }
}

impl From<cymule_profile_protocol::ProtocolError> for AgentError {
    fn from(error: cymule_profile_protocol::ProtocolError) -> Self {
        match error {
            cymule_profile_protocol::ProtocolError::IllegalTransition(message) => {
                Self::IllegalTransition(message)
            }
            cymule_profile_protocol::ProtocolError::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            },
            error @ cymule_profile_protocol::ProtocolError::CollectionProviderFailure(_) => {
                Self::from(cymule_durable::DurableError::from(error))
            }
            cymule_profile_protocol::ProtocolError::Conflict { code, message } => {
                Self::Conflict { code, message }
            }
            cymule_profile_protocol::ProtocolError::NotFound { message } => Self::NotFound(message),
            cymule_profile_protocol::ProtocolError::Substrate { code, message } => {
                Self::Substrate { code, message }
            }
            cymule_profile_protocol::ProtocolError::Persistence { code, message } => {
                Self::Persistence { code, message }
            }
            cymule_profile_protocol::ProtocolError::CommitOutcomeUnknown { message } => {
                Self::CommitOutcomeUnknown { message }
            }
            cymule_profile_protocol::ProtocolError::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            },
            cymule_profile_protocol::ProtocolError::Integrity { code, message } => {
                Self::Integrity { code, message }
            }
            cymule_profile_protocol::ProtocolError::IdentityMismatch(message) => Self::Integrity {
                code: "agent_protocol_identity_mismatch".to_owned(),
                message,
            },
            cymule_profile_protocol::ProtocolError::Encoding(message) => Self::Encoding { message },
            cymule_profile_protocol::ProtocolError::Validation(message) => {
                Self::Validation(message)
            }
            cymule_profile_protocol::ProtocolError::Contract(error) => {
                Self::Validation(error.to_string())
            }
        }
    }
}

impl From<cymule_durable::DurableError> for AgentError {
    fn from(error: cymule_durable::DurableError) -> Self {
        use cymule_durable::DurableError;

        match error {
            DurableError::Validation(message) => Self::Validation(message),
            DurableError::Contract(error) => Self::Validation(error.to_string()),
            DurableError::NotFound(message) => Self::NotFound(message),
            DurableError::IllegalTransition(message) => Self::IllegalTransition(message),
            DurableError::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            },
            DurableError::CommitOutcomeUnknown { message } => {
                Self::CommitOutcomeUnknown { message }
            }
            DurableError::ReconciliationRequired { intent_id } => {
                Self::ReconciliationRequired { intent_id }
            }
            DurableError::Conflict { expected, current } => Self::Conflict {
                code: "revision_conflict".to_owned(),
                message: format!("expected revision {expected:?}, current revision {current:?}"),
            },
            DurableError::Busy {
                run_id,
                owner,
                fence,
            } => Self::Busy {
                run_id,
                owner,
                fence,
            },
            DurableError::Substrate { code, message } => Self::Substrate { code, message },
            DurableError::Persistence { code, message } => Self::Persistence { code, message },
            DurableError::RuntimeDefect { code, message } => Self::RuntimeDefect { code, message },
            DurableError::Integrity { code, message } => Self::Integrity { code, message },
            DurableError::HistoryConflict { code, message } => Self::Conflict { code, message },
            DurableError::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            },
            DurableError::Cancelled { code, message } => Self::Cancelled { code, message },
            DurableError::TimedOut { code, message } => Self::TimedOut { code, message },
            DurableError::Encoding(message) => Self::Encoding { message },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_unknown_commit_outcome_remains_typed() {
        let error = AgentError::from(cymule_durable::DurableError::CommitOutcomeUnknown {
            message: "receipt response was lost".to_owned(),
        });

        assert_eq!(
            error,
            AgentError::CommitOutcomeUnknown {
                message: "receipt response was lost".to_owned(),
            }
        );
    }

    #[test]
    fn durable_substrate_code_remains_structured() {
        let error = AgentError::from(cymule_durable::DurableError::Substrate {
            code: "store_object_read_failed".to_owned(),
            message: "object provider unavailable".to_owned(),
        });

        assert_eq!(
            error,
            AgentError::Substrate {
                code: "store_object_read_failed".to_owned(),
                message: "object provider unavailable".to_owned(),
            }
        );
    }

    #[test]
    fn durable_persistence_code_remains_structured() {
        let error = AgentError::from(cymule_durable::DurableError::Persistence {
            code: "agent_state_write_failed".to_owned(),
            message: "state root write failed".to_owned(),
        });

        assert_eq!(
            error,
            AgentError::Persistence {
                code: "agent_state_write_failed".to_owned(),
                message: "state root write failed".to_owned(),
            }
        );
    }

    #[test]
    fn durable_defect_integrity_history_and_encoding_keep_categories() {
        let cases = [
            (
                cymule_durable::DurableError::RuntimeDefect {
                    code: "agent_provider_defect".to_owned(),
                    message: "provider returned an invalid product".to_owned(),
                },
                AgentError::RuntimeDefect {
                    code: "agent_provider_defect".to_owned(),
                    message: "provider returned an invalid product".to_owned(),
                },
            ),
            (
                cymule_durable::DurableError::Integrity {
                    code: "agent_receipt_mismatch".to_owned(),
                    message: "receipt content differs".to_owned(),
                },
                AgentError::Integrity {
                    code: "agent_receipt_mismatch".to_owned(),
                    message: "receipt content differs".to_owned(),
                },
            ),
            (
                cymule_durable::DurableError::HistoryConflict {
                    code: "agent_command_reused".to_owned(),
                    message: "command identity has different content".to_owned(),
                },
                AgentError::Conflict {
                    code: "agent_command_reused".to_owned(),
                    message: "command identity has different content".to_owned(),
                },
            ),
            (
                cymule_durable::DurableError::Encoding("canonical JSON failed".to_owned()),
                AgentError::Encoding {
                    message: "canonical JSON failed".to_owned(),
                },
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(AgentError::from(source), expected);
        }
    }

    #[test]
    fn durable_revision_conflict_and_busy_owner_use_explicit_fields() {
        let conflict = AgentError::from(cymule_durable::DurableError::Conflict {
            expected: Some("revision:expected".to_owned()),
            current: Some("revision:current".to_owned()),
        });
        assert_eq!(
            conflict,
            AgentError::Conflict {
                code: "revision_conflict".to_owned(),
                message: "expected revision Some(\"revision:expected\"), current revision Some(\"revision:current\")"
                    .to_owned(),
            }
        );

        let busy = AgentError::from(cymule_durable::DurableError::Busy {
            run_id: "run:busy".to_owned(),
            owner: "worker:owner".to_owned(),
            fence: 17,
        });
        assert_eq!(
            busy,
            AgentError::Busy {
                run_id: "run:busy".to_owned(),
                owner: "worker:owner".to_owned(),
                fence: 17,
            }
        );
    }

    #[test]
    fn durable_recovery_boundaries_keep_exact_authority_fields() {
        let reconciliation =
            AgentError::from(cymule_durable::DurableError::ReconciliationRequired {
                intent_id: "effect:intent".to_owned(),
            });
        assert_eq!(
            reconciliation,
            AgentError::ReconciliationRequired {
                intent_id: "effect:intent".to_owned(),
            }
        );

        let archived = AgentError::from(
            cymule_durable::DurableError::ArchivedCommandReplayRequired {
                command_id: "command:archived".to_owned(),
                archive_head: "archive:head".to_owned(),
                command_index_root: "archive:index".to_owned(),
            },
        );
        assert_eq!(
            archived,
            AgentError::ArchivedCommandReplayRequired {
                command_id: "command:archived".to_owned(),
                archive_head: "archive:head".to_owned(),
                command_index_root: "archive:index".to_owned(),
            }
        );
    }

    #[test]
    fn durable_cancellation_and_timeout_keep_terminal_codes() {
        let cases = [
            (
                cymule_durable::DurableError::Cancelled {
                    code: "agent_cancelled".to_owned(),
                    message: "owner cancelled before dispatch".to_owned(),
                },
                AgentError::Cancelled {
                    code: "agent_cancelled".to_owned(),
                    message: "owner cancelled before dispatch".to_owned(),
                },
            ),
            (
                cymule_durable::DurableError::TimedOut {
                    code: "agent_timed_out".to_owned(),
                    message: "deadline elapsed before dispatch".to_owned(),
                },
                AgentError::TimedOut {
                    code: "agent_timed_out".to_owned(),
                    message: "deadline elapsed before dispatch".to_owned(),
                },
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(AgentError::from(source), expected);
        }
    }

    #[test]
    fn protocol_structured_categories_and_details_remain_exact() {
        let cases = [
            (
                cymule_profile_protocol::ProtocolError::Conflict {
                    code: "agent_alias_conflict".to_owned(),
                    message: "alias has different content".to_owned(),
                },
                AgentError::Conflict {
                    code: "agent_alias_conflict".to_owned(),
                    message: "alias has different content".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::Substrate {
                    code: "agent_provider_unavailable".to_owned(),
                    message: "provider did not answer".to_owned(),
                },
                AgentError::Substrate {
                    code: "agent_provider_unavailable".to_owned(),
                    message: "provider did not answer".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::NotFound {
                    message: "message current is absent".to_owned(),
                },
                AgentError::NotFound("message current is absent".to_owned()),
            ),
            (
                cymule_profile_protocol::ProtocolError::Persistence {
                    code: "agent_state_write_failed".to_owned(),
                    message: "state root write failed".to_owned(),
                },
                AgentError::Persistence {
                    code: "agent_state_write_failed".to_owned(),
                    message: "state root write failed".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::CommitOutcomeUnknown {
                    message: "receipt response was lost".to_owned(),
                },
                AgentError::CommitOutcomeUnknown {
                    message: "receipt response was lost".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::ArchivedCommandReplayRequired {
                    command_id: "command:archived".to_owned(),
                    archive_head: "archive:head".to_owned(),
                    command_index_root: "archive:index".to_owned(),
                },
                AgentError::ArchivedCommandReplayRequired {
                    command_id: "command:archived".to_owned(),
                    archive_head: "archive:head".to_owned(),
                    command_index_root: "archive:index".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::Integrity {
                    code: "agent_resource_digest_mismatch".to_owned(),
                    message: "resource digest differs".to_owned(),
                },
                AgentError::Integrity {
                    code: "agent_resource_digest_mismatch".to_owned(),
                    message: "resource digest differs".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::IdentityMismatch(
                    "message head differs".to_owned(),
                ),
                AgentError::Integrity {
                    code: "agent_protocol_identity_mismatch".to_owned(),
                    message: "message head differs".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::Encoding(
                    "canonical JSON failed".to_owned(),
                ),
                AgentError::Encoding {
                    message: "canonical JSON failed".to_owned(),
                },
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(AgentError::from(source), expected);
        }
    }
}
