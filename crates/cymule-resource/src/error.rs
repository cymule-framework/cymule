pub use cymule_profile_protocol::resource::{ResourceError, ResourceResult, ResourceSchemaIssue};

pub(crate) fn durable_resource_error(error: cymule_durable::DurableError) -> ResourceError {
    match error {
        cymule_durable::DurableError::IllegalTransition(message) => ResourceError::Conflict {
            code: "illegal_transition".to_owned(),
            message,
        },
        cymule_durable::DurableError::PagedScopeRequired {
            run_id,
            scope_id,
            entries,
        } => ResourceError::Conflict {
            code: "paged_scope_required".to_owned(),
            message: format!(
                "Run {run_id} Scope {scope_id} has {entries} entries and requires paged preparation"
            ),
        },
        cymule_durable::DurableError::HistoryConflict { code, message } => {
            ResourceError::Conflict { code, message }
        }
        cymule_durable::DurableError::Conflict { expected, current } => ResourceError::Conflict {
            code: "revision_conflict".to_owned(),
            message: format!("expected {expected:?}, current {current:?}"),
        },
        cymule_durable::DurableError::Busy {
            run_id,
            owner,
            fence,
        } => ResourceError::Conflict {
            code: "execution_busy".to_owned(),
            message: format!("Run {run_id} is owned by {owner} at fence {fence}"),
        },
        cymule_durable::DurableError::ReconciliationRequired { intent_id } => {
            ResourceError::Conflict {
                code: "reconciliation_required".to_owned(),
                message: format!("Effect {intent_id} remains unknown"),
            }
        }
        cymule_durable::DurableError::Validation(message) => ResourceError::Validation(message),
        cymule_durable::DurableError::Contract(error) => {
            ResourceError::Validation(format!("contract_violation: {error}"))
        }
        cymule_durable::DurableError::NotFound(message) => ResourceError::NotFound(message),
        cymule_durable::DurableError::Substrate { code, message } => {
            ResourceError::Substrate { code, message }
        }
        cymule_durable::DurableError::Persistence { code, message }
        | cymule_durable::DurableError::Cancelled { code, message }
        | cymule_durable::DurableError::TimedOut { code, message } => {
            ResourceError::Persistence { code, message }
        }
        cymule_durable::DurableError::Integrity { code, message }
        | cymule_durable::DurableError::RuntimeDefect { code, message } => {
            ResourceError::Integrity { code, message }
        }
        cymule_durable::DurableError::Encoding(message) => ResourceError::Integrity {
            code: "encoding_failed".to_owned(),
            message,
        },
        cymule_durable::DurableError::CommitOutcomeUnknown { message } => {
            ResourceError::CommitOutcomeUnknown { message }
        }
        cymule_durable::DurableError::ArchivedCommandReplayRequired {
            command_id,
            archive_head,
            command_index_root,
        } => ResourceError::Persistence {
            code: "archived_command_replay_required".to_owned(),
            message: format!(
                "command {command_id} requires proof from archive {archive_head} at index {command_index_root}"
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_conflict_categories_remain_resource_conflicts() {
        let cases = [
            (
                cymule_durable::DurableError::Conflict {
                    expected: Some("sha256:expected".to_owned()),
                    current: None,
                },
                "revision_conflict",
            ),
            (
                cymule_durable::DurableError::IllegalTransition("closed edge".to_owned()),
                "illegal_transition",
            ),
            (
                cymule_durable::DurableError::HistoryConflict {
                    code: "history_code".to_owned(),
                    message: "history detail".to_owned(),
                },
                "history_code",
            ),
            (
                cymule_durable::DurableError::Busy {
                    run_id: "run:busy".to_owned(),
                    owner: "driver:busy".to_owned(),
                    fence: 7,
                },
                "execution_busy",
            ),
            (
                cymule_durable::DurableError::ReconciliationRequired {
                    intent_id:
                        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_owned(),
                },
                "reconciliation_required",
            ),
        ];
        for (error, stable_code) in cases {
            let ResourceError::Conflict { code, .. } = durable_resource_error(error) else {
                panic!("Durable conflict escaped the Resource conflict category");
            };
            assert_eq!(code, stable_code);
        }
    }

    #[test]
    fn durable_validation_and_not_found_remain_exact_resource_categories() {
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::Validation(
                "invalid command".to_owned(),
            )),
            ResourceError::Validation("invalid command".to_owned())
        );
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::NotFound(
                "missing receipt".to_owned(),
            )),
            ResourceError::NotFound("missing receipt".to_owned())
        );
    }

    #[test]
    fn durable_structured_failures_preserve_resource_categories_and_codes() {
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::Substrate {
                code: "store_unavailable".to_owned(),
                message: "offline".to_owned(),
            }),
            ResourceError::Substrate {
                code: "store_unavailable".to_owned(),
                message: "offline".to_owned(),
            }
        );
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::RuntimeDefect {
                code: "bad_runtime".to_owned(),
                message: "wrong receipt".to_owned(),
            }),
            ResourceError::Integrity {
                code: "bad_runtime".to_owned(),
                message: "wrong receipt".to_owned(),
            }
        );
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::Persistence {
                code: "durable_receipt_unavailable".to_owned(),
                message: "receipt read failed".to_owned(),
            }),
            ResourceError::Persistence {
                code: "durable_receipt_unavailable".to_owned(),
                message: "receipt read failed".to_owned(),
            }
        );
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::Integrity {
                code: "bad_digest".to_owned(),
                message: "mismatch".to_owned(),
            }),
            ResourceError::Integrity {
                code: "bad_digest".to_owned(),
                message: "mismatch".to_owned(),
            }
        );
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::Encoding(
                "invalid canonical bytes".to_owned()
            )),
            ResourceError::Integrity {
                code: "encoding_failed".to_owned(),
                message: "invalid canonical bytes".to_owned(),
            }
        );
    }

    #[test]
    fn durable_contract_unknown_and_terminal_failures_keep_their_boundary() {
        let contract = cymule_runtime::ContractViolation {
            phase: cymule_runtime::ContractPhase::Execution,
            target: cymule_runtime::ContractTarget {
                boundary: cymule_runtime::ContractBoundary::Component,
                id: "component.resource".to_owned(),
                side: cymule_runtime::ContractSide::Output,
            },
            issues: Vec::new(),
        };
        assert!(matches!(
            durable_resource_error(cymule_durable::DurableError::Contract(contract)),
            ResourceError::Validation(message) if message.starts_with("contract_violation:")
        ));
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::CommitOutcomeUnknown {
                message: "receipt lost".to_owned(),
            }),
            ResourceError::CommitOutcomeUnknown {
                message: "receipt lost".to_owned(),
            }
        );
        assert!(matches!(
            durable_resource_error(
                cymule_durable::DurableError::ArchivedCommandReplayRequired {
                    command_id: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                    archive_head:
                        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                            .to_owned(),
                    command_index_root:
                        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                            .to_owned(),
                }
            ),
            ResourceError::Persistence { code, .. }
                if code == "archived_command_replay_required"
        ));
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::Cancelled {
                code: "cancelled_by_owner".to_owned(),
                message: "stopped".to_owned(),
            }),
            ResourceError::Persistence {
                code: "cancelled_by_owner".to_owned(),
                message: "stopped".to_owned(),
            }
        );
        assert_eq!(
            durable_resource_error(cymule_durable::DurableError::TimedOut {
                code: "clock_timeout".to_owned(),
                message: "expired".to_owned(),
            }),
            ResourceError::Persistence {
                code: "clock_timeout".to_owned(),
                message: "expired".to_owned(),
            }
        );
    }
}
