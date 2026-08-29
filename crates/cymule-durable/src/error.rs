use std::fmt::{Display, Formatter};

/// Result type for durable coordination.
pub type DurableResult<T> = std::result::Result<T, DurableError>;

/// Stable durable-store and coordination errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableError {
    /// Stored or proposed state is malformed.
    Validation(String),
    /// An executable Plan contract failed admission or value validation.
    Contract(cymule_runtime::ContractViolation),
    /// A conditional write observed another committed revision.
    Conflict {
        /// Caller-observed revision.
        expected: Option<String>,
        /// Current durable revision.
        current: Option<String>,
    },
    /// Another unexpired durable execution claim owns the Run.
    Busy {
        /// Contended Run.
        run_id: String,
        /// Current exact driver identity.
        owner: String,
        /// Current execution fence.
        fence: u64,
    },
    /// The provider atomically closed late dispatch admission but could not
    /// yet prove one terminal world outcome.
    ReconciliationRequired {
        /// Original semantic Effect intent.
        intent_id: String,
    },
    /// A requested durable object does not exist.
    NotFound(String),
    /// A legal state-machine edge was not available.
    IllegalTransition(String),
    /// One inline atomic batch requires an independently paged Scope closure.
    PagedScopeRequired {
        /// Owning Run.
        run_id: String,
        /// Exact Scope whose source exceeds the inline bound.
        scope_id: String,
        /// Authenticated source cardinality.
        entries: u64,
    },
    /// A concrete substrate failed.
    Substrate {
        /// Stable substrate-owned failure code.
        code: String,
        /// Human-readable substrate failure summary.
        message: String,
    },
    /// A profile persistence authority failed with a stable owning code.
    Persistence {
        /// Stable persistence-owned failure code.
        code: String,
        /// Human-readable persistence failure summary.
        message: String,
    },
    /// The store may have committed a head transition, but no authoritative
    /// commit receipt reached the caller.
    CommitOutcomeUnknown {
        /// Human-readable detail about the lost or uncertain commit response.
        message: String,
    },
    /// A selected implementation violated its runtime protocol.
    RuntimeDefect {
        /// Stable defect code.
        code: String,
        /// Human-readable defect summary.
        message: String,
    },
    /// Canonical identity, causal closure, or encoding evidence is corrupted.
    Integrity {
        /// Stable kernel integrity code.
        code: String,
        /// Human-readable integrity summary.
        message: String,
    },
    /// An idempotency identity conflicts with immutable command history.
    HistoryConflict {
        /// Stable history-conflict code.
        code: String,
        /// Human-readable conflict summary.
        message: String,
    },
    /// Exact command replay requires a proof from the retained cold Machine
    /// command archive.
    ArchivedCommandReplayRequired {
        /// Requested canonical command identity.
        command_id: String,
        /// Current content-addressed archive head.
        archive_head: String,
        /// Current cumulative archived-command index root.
        command_index_root: String,
    },
    /// The invocation owner explicitly cancelled work before any ambiguous
    /// external-world outcome existed.
    Cancelled {
        /// Stable cancellation code.
        code: String,
        /// Human-readable cancellation summary.
        message: String,
    },
    /// An operation timed out before any external dispatch began.
    TimedOut {
        /// Stable timeout code.
        code: String,
        /// Human-readable timeout summary.
        message: String,
    },
    /// Canonical encoding or kernel restoration failed.
    Encoding(String),
}

impl Display for DurableError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::Contract(error) => write!(formatter, "contract_violation: {error}"),
            Self::Conflict { expected, current } => {
                write!(
                    formatter,
                    "revision_conflict: expected {expected:?}, current {current:?}"
                )
            }
            Self::Busy {
                run_id,
                owner,
                fence,
            } => write!(
                formatter,
                "execution_busy: Run {run_id} is owned by {owner} at fence {fence}"
            ),
            Self::ReconciliationRequired { intent_id } => write!(
                formatter,
                "reconciliation_required: Effect {intent_id} remains unknown"
            ),
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::IllegalTransition(message) => {
                write!(formatter, "illegal_transition: {message}")
            }
            Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => write!(
                formatter,
                "paged_scope_required: Run {run_id} Scope {scope_id} has {entries} entries and requires a standalone paged closure"
            ),
            Self::Substrate { code, message } => {
                write!(formatter, "substrate_failed: {code}: {message}")
            }
            Self::Persistence { code, message } => {
                write!(formatter, "persistence_failed: {code}: {message}")
            }
            Self::CommitOutcomeUnknown { message } => {
                write!(formatter, "commit_outcome_unknown: {message}")
            }
            Self::RuntimeDefect { code, message } => {
                write!(formatter, "runtime_defect: {code}: {message}")
            }
            Self::Integrity { code, message } => {
                write!(formatter, "integrity_failed: {code}: {message}")
            }
            Self::HistoryConflict { code, message } => {
                write!(formatter, "history_conflict: {code}: {message}")
            }
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
            Self::TimedOut { code, message } => write!(formatter, "timed_out: {code}: {message}"),
            Self::Encoding(message) => write!(formatter, "encoding_failed: {message}"),
        }
    }
}

impl std::error::Error for DurableError {}

impl From<cymule_authenticated_collections::CollectionError> for DurableError {
    fn from(error: cymule_authenticated_collections::CollectionError) -> Self {
        use cymule_authenticated_collections::{
            CollectionError, ProviderConflict, ProviderFailure,
        };

        match error {
            CollectionError::Validation(message) => Self::Validation(message),
            CollectionError::Integrity { code, message } => Self::Integrity {
                code: code.to_owned(),
                message,
            },
            CollectionError::MissingObject(object_id) => Self::Integrity {
                code: "state_root_collection_object_missing".to_owned(),
                message: format!(
                    "referenced authenticated collection object {object_id} does not exist"
                ),
            },
            CollectionError::Conflict(message) => Self::HistoryConflict {
                code: "authenticated_collection_conflict".to_owned(),
                message,
            },
            CollectionError::Provider(ProviderFailure::Validation { message }) => {
                Self::Validation(message)
            }
            CollectionError::Provider(ProviderFailure::Integrity { code, message }) => {
                Self::Integrity { code, message }
            }
            CollectionError::Provider(ProviderFailure::Conflict {
                evidence: ProviderConflict::Revision { expected, current },
            }) => Self::Conflict { expected, current },
            CollectionError::Provider(ProviderFailure::Conflict {
                evidence: ProviderConflict::History { code, message },
            }) => Self::HistoryConflict { code, message },
            CollectionError::Provider(ProviderFailure::Substrate { code, message }) => {
                Self::Substrate { code, message }
            }
        }
    }
}

impl From<cymule_durable_protocol::DurableProtocolError> for DurableError {
    fn from(error: cymule_durable_protocol::DurableProtocolError) -> Self {
        match error {
            cymule_durable_protocol::DurableProtocolError::CollectionProviderFailure(failure) => {
                Self::from(cymule_authenticated_collections::CollectionError::Provider(
                    failure,
                ))
            }
            cymule_durable_protocol::DurableProtocolError::Validation(message) => {
                Self::Validation(message)
            }
            cymule_durable_protocol::DurableProtocolError::NotFound { message } => {
                Self::NotFound(message)
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
                Self::HistoryConflict { code, message }
            }
            cymule_durable_protocol::DurableProtocolError::Integrity { code, message } => {
                Self::Integrity { code, message }
            }
            cymule_durable_protocol::DurableProtocolError::IdentityMismatch(message) => {
                Self::Integrity {
                    code: "durable_protocol_identity_mismatch".to_owned(),
                    message,
                }
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

impl From<cymule_profile_protocol::ProtocolError> for DurableError {
    fn from(error: cymule_profile_protocol::ProtocolError) -> Self {
        match error {
            cymule_profile_protocol::ProtocolError::CollectionProviderFailure(failure) => {
                Self::from(cymule_authenticated_collections::CollectionError::Provider(
                    failure,
                ))
            }
            cymule_profile_protocol::ProtocolError::Validation(message) => {
                Self::Validation(message)
            }
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
            cymule_profile_protocol::ProtocolError::Conflict { code, message } => {
                Self::HistoryConflict { code, message }
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
                code: "profile_protocol_identity_mismatch".to_owned(),
                message,
            },
            cymule_profile_protocol::ProtocolError::Contract(error) => Self::Contract(error),
            cymule_profile_protocol::ProtocolError::Encoding(message) => Self::Encoding(message),
        }
    }
}

impl From<cymule_profile_protocol::resource::ResourceError> for DurableError {
    fn from(error: cymule_profile_protocol::resource::ResourceError) -> Self {
        use cymule_profile_protocol::resource::ResourceError;

        match error {
            ResourceError::Validation(message) => Self::Validation(message),
            ResourceError::Schema(issue) => Self::Validation(format!(
                "Resource contract {} rejected instance {} at schema {}",
                issue.contract_id, issue.instance_path, issue.schema_path
            )),
            ResourceError::Conflict { code, message } => Self::HistoryConflict { code, message },
            ResourceError::NotFound(message) => Self::NotFound(message),
            ResourceError::Substrate { code, message } => Self::Substrate { code, message },
            ResourceError::Persistence { code, message } => Self::Persistence { code, message },
            ResourceError::CommitOutcomeUnknown { message } => {
                Self::CommitOutcomeUnknown { message }
            }
            ResourceError::Integrity { code, message } => Self::Integrity { code, message },
        }
    }
}

impl From<cymule_profile_protocol::evolution::EvolutionError> for DurableError {
    fn from(error: cymule_profile_protocol::evolution::EvolutionError) -> Self {
        use cymule_profile_protocol::evolution::EvolutionError;

        match error {
            EvolutionError::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            },
            EvolutionError::CollectionProviderFailure(failure) => Self::from(
                cymule_authenticated_collections::CollectionError::Provider(failure),
            ),
            EvolutionError::Validation(message) => Self::Validation(message),
            EvolutionError::Contract(error) => Self::Contract(error),
            EvolutionError::NotFound(message) => Self::NotFound(message),
            EvolutionError::ReadRequired {
                family,
                storage_key,
            } => Self::RuntimeDefect {
                code: "evolution_read_set_incomplete".to_owned(),
                message: format!(
                    "framework Evolution reducer omitted exact {family:?} authority for key {storage_key}"
                ),
            },
            EvolutionError::Conflict(message) => Self::HistoryConflict {
                code: "evolution_history_conflict".to_owned(),
                message,
            },
            EvolutionError::PluginDefect { code, message } => Self::RuntimeDefect { code, message },
            EvolutionError::Cancelled { code, message } => Self::Cancelled { code, message },
            EvolutionError::TimedOut { code, message } => Self::TimedOut { code, message },
            EvolutionError::Integrity { code, message } => Self::Integrity { code, message },
            EvolutionError::Substrate { code, message } => Self::Substrate { code, message },
        }
    }
}

impl From<cymule_core::CoreError> for DurableError {
    fn from(error: cymule_core::CoreError) -> Self {
        use cymule_core::CoreError;

        match error {
            CoreError::CollectionProviderFailure(failure) => Self::from(
                cymule_authenticated_collections::CollectionError::Provider(failure),
            ),
            CoreError::Validation(message) => Self::Validation(message),
            CoreError::NotFound(message) => Self::NotFound(message),
            CoreError::IllegalTransition(message) => Self::IllegalTransition(message),
            CoreError::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            } => Self::PagedScopeRequired {
                run_id,
                scope_id,
                entries,
            },
            CoreError::PinnedReadSetIncomplete { family, key } => Self::RuntimeDefect {
                code: "pinned_read_set_incomplete".to_owned(),
                message: format!(
                    "framework pinned reducer omitted exact {family} authority for key {key}"
                ),
            },
            error @ CoreError::CommandReuse(_) => Self::HistoryConflict {
                code: error.code().to_owned(),
                message: error.to_string(),
            },
            CoreError::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            } => Self::ArchivedCommandReplayRequired {
                command_id,
                archive_head,
                command_index_root,
            },
            error @ (CoreError::IdentityMismatch(_)
            | CoreError::Causal(_)
            | CoreError::Encoding(_)) => Self::Integrity {
                code: error.code().to_owned(),
                message: error.to_string(),
            },
        }
    }
}

impl From<serde_json::Error> for DurableError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encoding(error.to_string())
    }
}

impl From<cymule_runtime::ContractViolation> for DurableError {
    fn from(error: cymule_runtime::ContractViolation) -> Self {
        Self::Contract(error)
    }
}

impl From<cymule_runtime::PlanAdmissionError> for DurableError {
    fn from(error: cymule_runtime::PlanAdmissionError) -> Self {
        match error {
            cymule_runtime::PlanAdmissionError::Core(error) => Self::from(error),
            cymule_runtime::PlanAdmissionError::Contract(error) => Self::Contract(error),
        }
    }
}

impl From<cymule_runtime::CompositionError> for DurableError {
    fn from(error: cymule_runtime::CompositionError) -> Self {
        Self::RuntimeDefect {
            code: error.code().to_owned(),
            message: error.message(),
        }
    }
}

impl From<cymule_runtime::RuntimeError> for DurableError {
    fn from(error: cymule_runtime::RuntimeError) -> Self {
        match error {
            cymule_runtime::RuntimeError::Core(error) => Self::from(error),
            cymule_runtime::RuntimeError::Contract(error) => Self::Contract(error),
            cymule_runtime::RuntimeError::Composition(error) => Self::from(*error),
            cymule_runtime::RuntimeError::PluginDefect { code, message }
            | cymule_runtime::RuntimeError::UnknownWorld { code, message } => {
                Self::RuntimeDefect { code, message }
            }
            cymule_runtime::RuntimeError::Substrate { code, message } => {
                Self::Substrate { code, message }
            }
            cymule_runtime::RuntimeError::Cancelled { code, message } => {
                Self::Cancelled { code, message }
            }
            cymule_runtime::RuntimeError::TimedOut { code, message } => {
                Self::TimedOut { code, message }
            }
            cymule_runtime::RuntimeError::ExpectedPluginFailure(error) => Self::RuntimeDefect {
                code: error.code,
                message: error.message,
            },
            cymule_runtime::RuntimeError::Suspended(boundary) => Self::RuntimeDefect {
                code: "unexpected_runtime_suspension".to_owned(),
                message: format!(
                    "runtime suspension at site {} escaped its typed durable boundary",
                    boundary.site_id
                ),
            },
            cymule_runtime::RuntimeError::ReleaseRequired { intent_ids } => Self::RuntimeDefect {
                code: "unexpected_runtime_release_required".to_owned(),
                message: format!(
                    "runtime release requirement escaped its typed durable boundary for intents {}",
                    intent_ids.join(",")
                ),
            },
            cymule_runtime::RuntimeError::Encoding(message) => Self::Integrity {
                code: "runtime_encoding_failed".to_owned(),
                message,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_authenticated_collections::{CollectionError, ProviderConflict, ProviderFailure};

    #[test]
    fn authenticated_collection_provider_failures_preserve_categories_and_codes() {
        let cases = [
            (
                ProviderFailure::Validation {
                    message: "invalid key".to_owned(),
                },
                DurableError::Validation("invalid key".to_owned()),
            ),
            (
                ProviderFailure::Integrity {
                    code: "forged_node".to_owned(),
                    message: "node mismatch".to_owned(),
                },
                DurableError::Integrity {
                    code: "forged_node".to_owned(),
                    message: "node mismatch".to_owned(),
                },
            ),
            (
                ProviderFailure::Conflict {
                    evidence: ProviderConflict::Revision {
                        expected: Some("sha256:expected".to_owned()),
                        current: Some("sha256:current".to_owned()),
                    },
                },
                DurableError::Conflict {
                    expected: Some("sha256:expected".to_owned()),
                    current: Some("sha256:current".to_owned()),
                },
            ),
            (
                ProviderFailure::Conflict {
                    evidence: ProviderConflict::History {
                        code: "identity_reused".to_owned(),
                        message: "history mismatch".to_owned(),
                    },
                },
                DurableError::HistoryConflict {
                    code: "identity_reused".to_owned(),
                    message: "history mismatch".to_owned(),
                },
            ),
            (
                ProviderFailure::Substrate {
                    code: "store_unavailable".to_owned(),
                    message: "offline".to_owned(),
                },
                DurableError::Substrate {
                    code: "store_unavailable".to_owned(),
                    message: "offline".to_owned(),
                },
            ),
        ];
        for (failure, expected) in cases {
            assert_eq!(
                DurableError::from(cymule_core::CoreError::CollectionProviderFailure(
                    failure.clone()
                )),
                expected
            );
            assert_eq!(
                DurableError::from(
                    cymule_durable_protocol::DurableProtocolError::CollectionProviderFailure(
                        failure.clone()
                    )
                ),
                expected
            );
            assert_eq!(
                DurableError::from(
                    cymule_profile_protocol::ProtocolError::CollectionProviderFailure(
                        failure.clone()
                    )
                ),
                expected
            );
            assert_eq!(
                DurableError::from(
                    cymule_profile_protocol::evolution::EvolutionError::CollectionProviderFailure(
                        failure.clone()
                    )
                ),
                expected
            );
            assert_eq!(
                DurableError::from(CollectionError::Provider(failure)),
                expected
            );
        }
    }

    #[test]
    fn paged_scope_required_preserves_the_exact_owner_and_source_cardinality() {
        let run_id = "run:paged-error".to_owned();
        let scope_id = "scope:root".to_owned();
        let entries = 257;
        let expected = DurableError::PagedScopeRequired {
            run_id: run_id.clone(),
            scope_id: scope_id.clone(),
            entries,
        };
        let errors = [
            DurableError::from(cymule_core::CoreError::PagedScopeRequired {
                run_id: run_id.clone(),
                scope_id: scope_id.clone(),
                entries,
            }),
            DurableError::from(
                cymule_durable_protocol::DurableProtocolError::PagedScopeRequired {
                    run_id: run_id.clone(),
                    scope_id: scope_id.clone(),
                    entries,
                },
            ),
            DurableError::from(cymule_profile_protocol::ProtocolError::PagedScopeRequired {
                run_id: run_id.clone(),
                scope_id: scope_id.clone(),
                entries,
            }),
            DurableError::from(
                cymule_profile_protocol::evolution::EvolutionError::PagedScopeRequired {
                    run_id,
                    scope_id,
                    entries,
                },
            ),
        ];
        for error in errors {
            assert_eq!(error, expected);
        }
    }

    #[test]
    fn missing_required_collection_object_is_integrity_not_a_semantic_key_miss() {
        assert!(
            matches!(DurableError::from(CollectionError::MissingObject("node:missing".to_owned())), DurableError::Integrity { code, .. } if code == "state_root_collection_object_missing")
        );
    }

    #[test]
    fn profile_protocol_failures_preserve_persistence_and_terminal_categories() {
        let cases = [
            (
                cymule_profile_protocol::ProtocolError::Persistence {
                    code: "profile_store_failed".to_owned(),
                    message: "write failed".to_owned(),
                },
                DurableError::Persistence {
                    code: "profile_store_failed".to_owned(),
                    message: "write failed".to_owned(),
                },
            ),
            (
                cymule_profile_protocol::ProtocolError::NotFound {
                    message: "missing profile leaf".to_owned(),
                },
                DurableError::NotFound("missing profile leaf".to_owned()),
            ),
            (
                cymule_profile_protocol::ProtocolError::CommitOutcomeUnknown {
                    message: "receipt was lost".to_owned(),
                },
                DurableError::CommitOutcomeUnknown {
                    message: "receipt was lost".to_owned(),
                },
            ),
        ];
        for (source, expected) in cases {
            assert_eq!(DurableError::from(source), expected);
        }
    }

    #[test]
    fn runtime_composition_failure_preserves_typed_code_and_message() {
        let source = cymule_runtime::CompositionError::ManifestMismatch;
        let expected = DurableError::RuntimeDefect {
            code: source.code().to_owned(),
            message: source.message(),
        };
        assert_eq!(DurableError::from(source.clone()), expected);
        assert_eq!(
            DurableError::from(cymule_runtime::RuntimeError::Composition(Box::new(source))),
            expected
        );
    }

    #[test]
    fn runtime_defect_and_unknown_world_preserve_their_code_and_message() {
        let code = "provider_failure".to_owned();
        let message = "retained provider detail".to_owned();
        let expected = DurableError::RuntimeDefect {
            code: code.clone(),
            message: message.clone(),
        };
        let errors = [
            cymule_runtime::RuntimeError::PluginDefect {
                code: code.clone(),
                message: message.clone(),
            },
            cymule_runtime::RuntimeError::UnknownWorld { code, message },
        ];
        for error in errors {
            assert_eq!(DurableError::from(error), expected);
        }
    }

    #[test]
    fn protocol_archive_replay_fields_are_never_flattened() {
        let expected = DurableError::ArchivedCommandReplayRequired {
            command_id: "command:archived".to_owned(),
            archive_head: "sha256:archive".to_owned(),
            command_index_root: "sha256:index".to_owned(),
        };
        assert_eq!(
            DurableError::from(
                cymule_durable_protocol::DurableProtocolError::ArchivedCommandReplayRequired {
                    command_id: "command:archived".to_owned(),
                    archive_head: "sha256:archive".to_owned(),
                    command_index_root: "sha256:index".to_owned(),
                }
            ),
            expected
        );
        assert_eq!(
            DurableError::from(
                cymule_profile_protocol::ProtocolError::ArchivedCommandReplayRequired {
                    command_id: "command:archived".to_owned(),
                    archive_head: "sha256:archive".to_owned(),
                    command_index_root: "sha256:index".to_owned(),
                }
            ),
            expected
        );
    }
}
