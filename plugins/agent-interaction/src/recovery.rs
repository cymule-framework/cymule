use cymule_profile_protocol::agent::{
    AgentHostOccurrence, AgentHostOccurrenceState, AgentHostRequest, AgentOccurrenceQuery,
    AgentOccurrenceResolution, ContentBlock,
};

use crate::{
    AgentError, AgentHost, AgentPersistence, AgentResult, interaction::persist_occurrence,
};

/// Explicit recovery operations for prepared, started, or unknown host calls.
pub struct AgentRecoveryController;

const CONTEXT_COMPLETION_UNVERIFIED: &str =
    "context recovery completion lacks the original pinned message-reader evidence";

impl AgentRecoveryController {
    /// Query the original host binding and persist an authoritative resolution.
    ///
    /// # Errors
    ///
    /// Returns an error when the occurrence is missing, M1-owned, only
    /// prepared, bound to another implementation, still ambiguous, or cannot
    /// persist its resolution.
    pub fn reconcile<H: AgentHost, P: AgentPersistence>(
        host: &mut H,
        persistence: &mut P,
        session_id: &str,
        occurrence_id: &str,
    ) -> AgentResult<AgentHostOccurrence> {
        let current = load_occurrence(persistence, session_id, occurrence_id)?;
        if current.request.is_m1_workspace() {
            return Err(AgentError::RecoveryRequired(format!(
                "M1 workspace occurrence {} requires WorkspaceScopeController reconciliation",
                current.occurrence_id
            )));
        }
        if current.is_terminal() {
            return Ok(current);
        }
        if current.state == AgentHostOccurrenceState::Prepared {
            return Err(AgentError::IllegalTransition(format!(
                "prepared occurrence {} was never dispatched; cancel it instead",
                current.occurrence_id
            )));
        }
        let active_binding = host.bind_occurrence(&current.request)?;
        active_binding.verify()?;
        if active_binding != current.occurrence_binding {
            return Err(AgentError::RecoveryRequired(format!(
                "host occurrence {} requires its exact retained implementation binding",
                current.occurrence_id
            )));
        }
        match host.reconcile_occurrence(&current)? {
            AgentOccurrenceResolution::Completed { response } => {
                if matches!(&current.request, AgentHostRequest::Context(_)) {
                    let occurrence_id = current.occurrence_id.clone();
                    if current.state == AgentHostOccurrenceState::Started {
                        persist_occurrence(
                            persistence,
                            current.mark_unknown(CONTEXT_COMPLETION_UNVERIFIED)?,
                        )?;
                    }
                    return Err(AgentError::HostOutcomeUnknown { occurrence_id });
                }
                persist_occurrence(persistence, current.complete(response)?)
            }
            AgentOccurrenceResolution::NotApplied { evidence } => {
                persist_occurrence(persistence, current.mark_not_applied(evidence)?)
            }
            AgentOccurrenceResolution::Unknown { evidence } => {
                let occurrence_id = current.occurrence_id.clone();
                persist_occurrence(
                    persistence,
                    current.mark_unknown_with_evidence(
                        "reconciliation could not determine the original outcome",
                        evidence,
                    )?,
                )?;
                Err(AgentError::HostOutcomeUnknown { occurrence_id })
            }
        }
    }

    /// Cancel a prepared occurrence that is proven never to have started.
    ///
    /// # Errors
    ///
    /// Returns an error when the occurrence is missing, M1-owned, may already
    /// have started, or its not-applied evidence cannot be persisted.
    pub fn cancel_prepared<P: AgentPersistence>(
        persistence: &mut P,
        session_id: &str,
        occurrence_id: &str,
        evidence: Vec<ContentBlock>,
    ) -> AgentResult<AgentHostOccurrence> {
        let current = load_occurrence(persistence, session_id, occurrence_id)?;
        if current.request.is_m1_workspace() {
            return Err(AgentError::RecoveryRequired(format!(
                "M1 workspace occurrence {} requires WorkspaceScopeController recovery",
                current.occurrence_id
            )));
        }
        if current.is_terminal() {
            return Ok(current);
        }
        if current.state != AgentHostOccurrenceState::Prepared {
            return Err(AgentError::IllegalTransition(format!(
                "occurrence {} may have started and cannot be cancelled without reconciliation",
                current.occurrence_id
            )));
        }
        persist_occurrence(persistence, current.mark_not_applied(evidence)?)
    }
}

fn load_occurrence<P: AgentPersistence>(
    persistence: &mut P,
    session_id: &str,
    occurrence_id: &str,
) -> AgentResult<AgentHostOccurrence> {
    persistence
        .read_agent_occurrence(&AgentOccurrenceQuery {
            session_id: session_id.to_owned(),
            occurrence_id: occurrence_id.to_owned(),
            expected_revision: None,
        })?
        .current
        .map(|current| current.occurrence)
        .ok_or_else(|| {
            AgentError::NotFound(format!(
                "host occurrence {occurrence_id} does not exist in Session {session_id}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use cymule_profile_protocol::agent::{
        AgentCommand, AgentCommandAction, AgentContextMessageRef, AgentContextScanLimits,
        AgentHostBinding, AgentHostRequest, AgentHostResponse, AgentMessage, AgentMessageQuery,
        AgentOccurrenceResolution, AgentSessionCurrent, AgentSessionQuery, AgentState, AgentUpdate,
        ContentBlock, ContextRequest, ContextSnapshot, ElicitationRequest, ElicitationResponse,
        MessageRole, ModelRequest, ModelResponse, PermissionRequest, PermissionResponse,
        ToolRequest, ToolResponse, WorkspaceChange, WorkspaceReceipt,
    };
    use serde_json::json;

    use super::*;
    use crate::{AgentHost, AgentMessageReader, EphemeralAgentPersistence};

    const SESSION_ID: &str = "session:recovery";
    const OCCURRENCE_ID: &str = "occurrence:recovery";
    const BINDING_ID: &str = "binding:recovery/1";

    fn request() -> AgentHostRequest {
        AgentHostRequest::Tool(ToolRequest {
            tool_call_id: "tool:recovery".to_owned(),
            operation: "test.recover".to_owned(),
            input: json!({"value": 1}),
        })
    }

    fn binding() -> AgentHostBinding {
        AgentHostBinding::standalone("host:recovery/1", BINDING_ID)
            .expect("recovery binding constructs")
    }

    fn prepare(persistence: &mut EphemeralAgentPersistence) -> AgentHostOccurrence {
        prepare_request(persistence, request())
    }

    fn prepare_request(
        persistence: &mut EphemeralAgentPersistence,
        request: AgentHostRequest,
    ) -> AgentHostOccurrence {
        let occurrence =
            AgentHostOccurrence::prepare(OCCURRENCE_ID, SESSION_ID, request, binding())
                .expect("recovery occurrence prepares");
        persist_occurrence(persistence, occurrence).expect("prepared occurrence persists")
    }

    fn seeded_context_session(persistence: &mut EphemeralAgentPersistence) -> AgentSessionCurrent {
        let mut revision = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: SESSION_ID.to_owned(),
                expected_revision: None,
            })
            .expect("initial Context Session absence reads")
            .revision;
        for index in 0..2_u64 {
            let command = AgentCommand::new(
                revision,
                AgentCommandAction::SessionUpdate {
                    session_id: SESSION_ID.to_owned(),
                    update: AgentUpdate::Message {
                        update_id: format!("update:recovery-context:{index}"),
                        message: AgentMessage {
                            message_id: format!("message:recovery-context:{index}"),
                            role: MessageRole::Agent,
                            content: vec![ContentBlock::Text {
                                text: format!("context {index}"),
                            }],
                        },
                    },
                },
            )
            .expect("Context seed command seals");
            revision = persistence
                .commit_agent(&command)
                .expect("Context seed command commits")
                .observed_revision;
        }
        persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: SESSION_ID.to_owned(),
                expected_revision: Some(revision),
            })
            .expect("seeded Context Session reads")
            .current
            .expect("seeded Context Session exists")
    }

    fn context_request(session: &AgentSessionCurrent) -> AgentHostRequest {
        AgentHostRequest::Context(ContextRequest {
            session_id: session.session_id.clone(),
            source_message_head: session.message_head.clone(),
            source_message_count: session.message_count,
            budget: session.message_count,
            scan_limits: AgentContextScanLimits {
                max_entries: session.message_count,
                max_canonical_bytes: cymule_profile_protocol::agent::MAX_AGENT_CONTEXT_SCAN_BYTES,
            },
        })
    }

    fn context_response(
        source_message_head: Option<String>,
        source_message_count: u64,
    ) -> AgentHostResponse {
        AgentHostResponse::Context(ContextSnapshot {
            snapshot_id: "snapshot:recovery-context".to_owned(),
            source_message_head,
            source_message_count,
            selected_messages: Vec::new(),
            content: Vec::new(),
            occurrence_binding: BINDING_ID.to_owned(),
        })
    }

    fn session(persistence: &mut EphemeralAgentPersistence) -> (String, AgentState, u64) {
        let read = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: SESSION_ID.to_owned(),
                expected_revision: None,
            })
            .expect("recovery Session reads");
        let current = read.current.expect("recovery Session exists");
        (
            read.revision,
            current.state,
            current.unresolved_occurrence_count,
        )
    }

    #[derive(Default)]
    struct RecoveryHost {
        bind_calls: usize,
        reconcile_calls: usize,
        resolutions: Vec<AgentOccurrenceResolution>,
    }

    impl AgentHost for RecoveryHost {
        fn bind_occurrence(
            &mut self,
            _request: &AgentHostRequest,
        ) -> AgentResult<AgentHostBinding> {
            self.bind_calls += 1;
            Ok(binding())
        }

        fn select_context(
            &mut self,
            _request: ContextRequest,
            _messages: &mut dyn AgentMessageReader,
        ) -> AgentResult<ContextSnapshot> {
            Err(AgentError::Host("unexpected context dispatch".to_owned()))
        }

        fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
            Err(AgentError::Host("unexpected model dispatch".to_owned()))
        }

        fn request_permission(
            &mut self,
            _request: PermissionRequest,
        ) -> AgentResult<PermissionResponse> {
            Err(AgentError::Host(
                "unexpected permission dispatch".to_owned(),
            ))
        }

        fn invoke_tool(&mut self, _request: ToolRequest) -> AgentResult<ToolResponse> {
            Err(AgentError::Host("unexpected tool redispatch".to_owned()))
        }

        fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
            Err(AgentError::Host(
                "unexpected elicitation dispatch".to_owned(),
            ))
        }

        fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
            Err(AgentError::Host("unexpected workspace dispatch".to_owned()))
        }

        fn reconcile_occurrence(
            &mut self,
            _occurrence: &AgentHostOccurrence,
        ) -> AgentResult<AgentOccurrenceResolution> {
            self.reconcile_calls += 1;
            if !self.resolutions.is_empty() {
                return Ok(self.resolutions.remove(0));
            }
            Ok(AgentOccurrenceResolution::Completed {
                response: AgentHostResponse::Tool(ToolResponse {
                    tool_call_id: "tool:recovery".to_owned(),
                    content: vec![ContentBlock::Text {
                        text: "recovered".to_owned(),
                    }],
                    occurrence_binding: BINDING_ID.to_owned(),
                }),
            })
        }
    }

    #[test]
    fn prepared_cancel_is_terminal_and_replay_has_zero_writes() {
        let mut persistence = EphemeralAgentPersistence::default();
        prepare(&mut persistence);
        assert_eq!(session(&mut persistence).2, 1);

        let cancelled = AgentRecoveryController::cancel_prepared(
            &mut persistence,
            SESSION_ID,
            OCCURRENCE_ID,
            vec![ContentBlock::Text {
                text: "dispatch never began".to_owned(),
            }],
        )
        .expect("prepared occurrence cancels with evidence");
        assert_eq!(cancelled.state, AgentHostOccurrenceState::NotApplied);
        let after_cancel = session(&mut persistence);
        assert_eq!(after_cancel.2, 0);

        let replay = AgentRecoveryController::cancel_prepared(
            &mut persistence,
            SESSION_ID,
            OCCURRENCE_ID,
            Vec::new(),
        )
        .expect("terminal cancellation replays without another transition");
        assert_eq!(replay, cancelled);
        assert_eq!(session(&mut persistence), after_cancel);
    }

    #[test]
    fn started_reconcile_uses_exact_binding_once_and_never_redispatches() {
        let mut persistence = EphemeralAgentPersistence::default();
        let prepared = prepare(&mut persistence);
        persist_occurrence(
            &mut persistence,
            prepared.start().expect("prepared occurrence starts"),
        )
        .expect("started occurrence persists");

        let mut host = RecoveryHost::default();
        let completed = AgentRecoveryController::reconcile(
            &mut host,
            &mut persistence,
            SESSION_ID,
            OCCURRENCE_ID,
        )
        .expect("started occurrence reconciles through its retained binding");
        assert_eq!(completed.state, AgentHostOccurrenceState::Completed);
        assert_eq!(host.bind_calls, 1);
        assert_eq!(host.reconcile_calls, 1);
        let after_complete = session(&mut persistence);
        assert_eq!(after_complete.2, 0);

        let replay = AgentRecoveryController::reconcile(
            &mut host,
            &mut persistence,
            SESSION_ID,
            OCCURRENCE_ID,
        )
        .expect("terminal reconciliation returns its retained occurrence");
        assert_eq!(replay, completed);
        assert_eq!(host.bind_calls, 1);
        assert_eq!(host.reconcile_calls, 1);
        assert_eq!(session(&mut persistence), after_complete);
    }

    #[test]
    fn unknown_reconciliation_appends_new_evidence_and_replays_duplicates_after_reopen() {
        let mut persistence = EphemeralAgentPersistence::default();
        let prepared = prepare(&mut persistence);
        persist_occurrence(
            &mut persistence,
            prepared.start().expect("prepared occurrence starts"),
        )
        .expect("started occurrence persists");
        let first_evidence = vec![ContentBlock::Text {
            text: "provider has no terminal receipt".to_owned(),
        }];
        let second_evidence = vec![ContentBlock::Text {
            text: "readback remains inconclusive".to_owned(),
        }];
        let mut host = RecoveryHost {
            resolutions: vec![
                AgentOccurrenceResolution::Unknown {
                    evidence: first_evidence.clone(),
                },
                AgentOccurrenceResolution::Unknown {
                    evidence: first_evidence,
                },
                AgentOccurrenceResolution::Unknown {
                    evidence: second_evidence,
                },
            ],
            ..RecoveryHost::default()
        };

        let mut revisions = Vec::new();
        for _ in 0..3 {
            assert_eq!(
                AgentRecoveryController::reconcile(
                    &mut host,
                    &mut persistence,
                    SESSION_ID,
                    OCCURRENCE_ID,
                ),
                Err(AgentError::HostOutcomeUnknown {
                    occurrence_id: OCCURRENCE_ID.to_owned(),
                })
            );
            revisions.push(session(&mut persistence).0);
        }
        assert_eq!(revisions[0], revisions[1]);
        assert_ne!(revisions[1], revisions[2]);
        let after = load_occurrence(&mut persistence, SESSION_ID, OCCURRENCE_ID)
            .expect("unknown occurrence reopens");
        assert_eq!(after.state, AgentHostOccurrenceState::Unknown);
        assert_eq!(after.recovery_observations.len(), 2);
        assert_eq!(
            after.recovery_observations[0].evidence[0],
            ContentBlock::Text {
                text: "provider has no terminal receipt".to_owned(),
            }
        );
        assert_eq!(
            after.recovery_observations[1].evidence[0],
            ContentBlock::Text {
                text: "readback remains inconclusive".to_owned(),
            }
        );
        assert_eq!(host.reconcile_calls, 3);

        let mut reopened = persistence.clone();
        assert_eq!(
            load_occurrence(&mut reopened, SESSION_ID, OCCURRENCE_ID)
                .expect("reopened persistence retains all observations"),
            after
        );
    }

    #[test]
    fn context_completed_recovery_stays_unknown_without_a_completion_write() {
        let mut persistence = EphemeralAgentPersistence::default();
        let source = seeded_context_session(&mut persistence);
        let prepared = prepare_request(&mut persistence, context_request(&source));
        let started = persist_occurrence(
            &mut persistence,
            prepared.start().expect("Context occurrence starts"),
        )
        .expect("Started Context occurrence persists");
        let selected = persistence
            .read_agent_message(&AgentMessageQuery {
                session_id: source.session_id.clone(),
                message_id: "message:recovery-context:0".to_owned(),
                expected_revision: None,
            })
            .expect("older exact Context message reads through the public capability")
            .current
            .expect("older exact Context message exists");
        let unproven = AgentHostResponse::Context(ContextSnapshot {
            snapshot_id: "snapshot:recovery-context-unproven".to_owned(),
            source_message_head: source.message_head.clone(),
            source_message_count: source.message_count,
            selected_messages: vec![
                AgentContextMessageRef::from_current(&selected)
                    .expect("real older message derives a valid Context reference"),
            ],
            content: selected.message.content,
            occurrence_binding: BINDING_ID.to_owned(),
        });
        assert_eq!(
            started
                .complete(unproven.clone())
                .expect("pure profile shape cannot detect missing reader delivery")
                .state,
            AgentHostOccurrenceState::Completed
        );
        let mut host = RecoveryHost {
            resolutions: vec![
                AgentOccurrenceResolution::Completed {
                    response: unproven.clone(),
                },
                AgentOccurrenceResolution::Completed { response: unproven },
            ],
            ..RecoveryHost::default()
        };

        assert_eq!(
            AgentRecoveryController::reconcile(
                &mut host,
                &mut persistence,
                SESSION_ID,
                OCCURRENCE_ID,
            ),
            Err(AgentError::HostOutcomeUnknown {
                occurrence_id: OCCURRENCE_ID.to_owned(),
            })
        );
        let unknown = load_occurrence(&mut persistence, SESSION_ID, OCCURRENCE_ID)
            .expect("Context occurrence remains unresolved");
        assert_eq!(unknown.state, AgentHostOccurrenceState::Unknown);
        assert_eq!(
            unknown.failure.as_deref(),
            Some(CONTEXT_COMPLETION_UNVERIFIED)
        );
        assert!(unknown.response.is_none());
        let after_unknown = session(&mut persistence);

        assert_eq!(
            AgentRecoveryController::reconcile(
                &mut host,
                &mut persistence,
                SESSION_ID,
                OCCURRENCE_ID,
            ),
            Err(AgentError::HostOutcomeUnknown {
                occurrence_id: OCCURRENCE_ID.to_owned(),
            })
        );
        assert_eq!(
            load_occurrence(&mut persistence, SESSION_ID, OCCURRENCE_ID)
                .expect("repeated forged completion leaves the occurrence unchanged"),
            unknown
        );
        assert_eq!(session(&mut persistence), after_unknown);
        assert_eq!(host.bind_calls, 2);
        assert_eq!(host.reconcile_calls, 2);
    }

    #[test]
    fn terminal_context_completion_replays_without_reconciliation_or_writes() {
        let mut persistence = EphemeralAgentPersistence::default();
        let source = seeded_context_session(&mut persistence);
        let prepared = prepare_request(&mut persistence, context_request(&source));
        let started = persist_occurrence(
            &mut persistence,
            prepared.start().expect("Context occurrence starts"),
        )
        .expect("Started Context occurrence persists");
        let completed = persist_occurrence(
            &mut persistence,
            started
                .complete(context_response(
                    source.message_head.clone(),
                    source.message_count,
                ))
                .expect("valid Context response completes"),
        )
        .expect("Completed Context occurrence persists");
        let before = session(&mut persistence);
        let mut host = RecoveryHost::default();

        assert_eq!(
            AgentRecoveryController::reconcile(
                &mut host,
                &mut persistence,
                SESSION_ID,
                OCCURRENCE_ID,
            )
            .expect("lost completion acknowledgement replays the terminal current"),
            completed
        );
        assert_eq!(session(&mut persistence), before);
        assert_eq!(host.bind_calls, 0);
        assert_eq!(host.reconcile_calls, 0);
    }

    #[test]
    fn context_not_applied_recovery_remains_terminal() {
        let mut persistence = EphemeralAgentPersistence::default();
        let source = seeded_context_session(&mut persistence);
        let prepared = prepare_request(&mut persistence, context_request(&source));
        persist_occurrence(
            &mut persistence,
            prepared.start().expect("Context occurrence starts"),
        )
        .expect("Started Context occurrence persists");
        let mut host = RecoveryHost {
            resolutions: vec![AgentOccurrenceResolution::NotApplied {
                evidence: vec![ContentBlock::Text {
                    text: "provider proves selection never ran".to_owned(),
                }],
            }],
            ..RecoveryHost::default()
        };

        let not_applied = AgentRecoveryController::reconcile(
            &mut host,
            &mut persistence,
            SESSION_ID,
            OCCURRENCE_ID,
        )
        .expect("Context NotApplied proof remains admissible");
        assert_eq!(not_applied.state, AgentHostOccurrenceState::NotApplied);
        assert_eq!(session(&mut persistence).2, 0);
        assert_eq!(host.bind_calls, 1);
        assert_eq!(host.reconcile_calls, 1);
    }
}
