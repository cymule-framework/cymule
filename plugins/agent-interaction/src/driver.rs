use cymule_core::MAX_EXACT_INTEGER;
use cymule_profile_protocol::agent::{
    AgentCommand, AgentCommandAction, AgentCommandOutcome, AgentHostOccurrenceState,
    AgentHostRequest, AgentHostResponse, AgentMessage, AgentOccurrencePageQuery,
    AgentSessionCurrent, AgentSessionQuery, AgentState, AgentToolDerivedPurpose, AgentUpdate,
    ContentBlock, ContextRequest, ContextSnapshot, ElicitationRequest, ElicitationResponse,
    MAX_AGENT_CONTEXT_SCAN_BYTES, MAX_AGENT_CONTEXT_SCAN_ENTRIES, MAX_AGENT_PAGE_BYTES,
    MessageRole, ModelRequest, ModelResponse, PermissionDecision, PermissionRequest,
    PermissionResponse, SessionStopReason, ToolCall, ToolCallStatus, ToolRequest, ToolResponse,
    WorkspaceChange, WorkspaceHostRequest, WorkspaceReceipt, agent_tool_derived_id,
};

use crate::{
    AgentError, AgentHost, AgentPersistence, AgentResult, interaction::execute_interaction,
};

/// Hard bound on model/provider rounds performed by one reference-driver turn.
pub const MAX_AGENT_MODEL_ROUNDS: u32 = 64;

/// Bounded synchronous reference turn driver over one replaceable Agent host.
///
/// This convenience driver keeps only bounded Session metadata. Messages and
/// occurrences are always read through exact or paged persistence queries.
#[must_use]
pub struct AgentTurnDriver<H, P> {
    host: H,
    persistence: P,
    session: AgentSessionCurrent,
    session_persisted: bool,
    revision: String,
    sequence: u64,
    max_model_rounds: u32,
}

impl<H: AgentHost, P: AgentPersistence> AgentTurnDriver<H, P> {
    /// Open a new driver after proving the Session key is absent at one exact revision.
    ///
    /// # Errors
    ///
    /// Returns an error when Session metadata cannot be read, the Session
    /// already exists, or its canonical genesis metadata cannot be constructed.
    pub fn open(session_id: impl Into<String>, host: H, mut persistence: P) -> AgentResult<Self> {
        let session_id = session_id.into();
        let read = persistence.read_agent_session(&AgentSessionQuery {
            session_id: session_id.clone(),
            expected_revision: None,
        })?;
        if read.current.is_some() {
            return Err(AgentError::IllegalTransition(format!(
                "Agent Session {session_id} already exists; resume it instead"
            )));
        }
        Self::from_session(
            host,
            persistence,
            AgentSessionCurrent::new(session_id)?,
            read.revision,
            false,
        )
    }

    /// Restore an existing driver from one explicit closed persistence capability.
    ///
    /// # Errors
    ///
    /// Returns an error when Session metadata is absent or cannot be read, its
    /// sequence is exhausted, or unfinished foreground/provider work requires recovery.
    pub fn resume(session_id: impl Into<String>, host: H, mut persistence: P) -> AgentResult<Self> {
        let session_id = session_id.into();
        let read = persistence.read_agent_session(&AgentSessionQuery {
            session_id: session_id.clone(),
            expected_revision: None,
        })?;
        let session = read.current.ok_or_else(|| {
            AgentError::NotFound(format!(
                "Agent Session {session_id} does not exist; open it instead"
            ))
        })?;
        Self::from_session(host, persistence, session, read.revision, true)
    }

    fn from_session(
        host: H,
        persistence: P,
        session: AgentSessionCurrent,
        revision: String,
        session_persisted: bool,
    ) -> AgentResult<Self> {
        if session.latest_update_sequence >= MAX_EXACT_INTEGER {
            return Err(AgentError::Validation(
                "Agent update sequence is exhausted".to_owned(),
            ));
        }
        let mut driver = Self {
            host,
            persistence,
            sequence: session.latest_update_sequence,
            session,
            session_persisted,
            revision,
            max_model_rounds: 16,
        };
        if driver.session_persisted {
            driver.ensure_recovery_clear()?;
        }
        if driver.session.state == AgentState::Running {
            return Err(AgentError::RecoveryRequired(
                "foreground turn control is incomplete; recover or close it before resuming"
                    .to_owned(),
            ));
        }
        Ok(driver)
    }

    /// Set the bounded number of model rounds in one foreground turn.
    ///
    /// # Errors
    ///
    /// Returns an error unless `rounds` is within `1..=MAX_AGENT_MODEL_ROUNDS`.
    pub fn with_max_model_rounds(mut self, rounds: u32) -> AgentResult<Self> {
        validate_model_rounds(rounds)?;
        self.max_model_rounds = rounds;
        Ok(self)
    }

    /// Run one prompt through context, model, permission, and tool interfaces.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session is not idle, input is invalid,
    /// unresolved work requires recovery, any durable/provider transition
    /// fails, or the model exceeds the configured round bound.
    pub fn run_turn(
        &mut self,
        user_message: AgentMessage,
        tools: &[String],
        context_budget: u64,
    ) -> AgentResult<&AgentSessionCurrent> {
        self.ensure_recovery_clear()?;
        if self.session.state != AgentState::Idle {
            return Err(AgentError::IllegalTransition(format!(
                "run_turn requires an idle Session, found {:?}",
                self.session.state
            )));
        }
        if user_message.role != MessageRole::User {
            return Err(AgentError::Validation(
                "run_turn requires a user message".to_owned(),
            ));
        }
        AgentUpdate::Message {
            update_id: "preflight:user-message".to_owned(),
            message: user_message.clone(),
        }
        .validate_content()?;
        self.begin_turn(user_message)?;
        for _ in 0..self.max_model_rounds {
            if self.run_model_round(tools, context_budget)? {
                return Ok(&self.session);
            }
        }
        let update_id = self.next_id("model-round-limit")?;
        self.apply(AgentUpdate::State {
            update_id,
            state: AgentState::Idle,
            stop_reason: Some(SessionStopReason::Error),
        })?;
        Err(AgentError::IllegalTransition(format!(
            "model exceeded {} rounds",
            self.max_model_rounds
        )))
    }

    fn begin_turn(&mut self, user_message: AgentMessage) -> AgentResult<()> {
        let update_id = self.next_id("user-message")?;
        self.apply(AgentUpdate::Message {
            update_id,
            message: user_message,
        })?;
        let update_id = self.next_id("running")?;
        self.apply(AgentUpdate::State {
            update_id,
            state: AgentState::Running,
            stop_reason: None,
        })
    }

    fn run_model_round(&mut self, tools: &[String], context_budget: u64) -> AgentResult<bool> {
        let context = self.select_context(ContextRequest {
            session_id: self.session.session_id.clone(),
            source_message_head: self.session.message_head.clone(),
            source_message_count: self.session.message_count,
            budget: context_budget,
            scan_limits: cymule_profile_protocol::agent::AgentContextScanLimits {
                max_entries: MAX_AGENT_CONTEXT_SCAN_ENTRIES,
                max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
            },
        })?;
        let response = self.invoke_model(ModelRequest {
            session_id: self.session.session_id.clone(),
            context,
            tools: tools.to_owned(),
        })?;
        if self.session.usage.as_ref() != Some(&response.usage) {
            let update_id = self.next_id("usage")?;
            self.apply(AgentUpdate::Usage {
                update_id,
                usage: response.usage.clone(),
            })?;
        }
        let update_id = self.next_id("agent-message")?;
        self.apply(AgentUpdate::Message {
            update_id,
            message: response.message,
        })?;
        if response.tool_requests.is_empty() {
            let update_id = self.next_id("idle")?;
            self.apply(AgentUpdate::State {
                update_id,
                state: AgentState::Idle,
                stop_reason: Some(SessionStopReason::EndTurn),
            })?;
            return Ok(true);
        }
        for request in response.tool_requests {
            self.run_tool_request(&request)?;
        }
        Ok(false)
    }

    fn run_tool_request(&mut self, request: &ToolRequest) -> AgentResult<()> {
        let permission_id = agent_tool_derived_id(
            &self.session.session_id,
            &request.tool_call_id,
            AgentToolDerivedPurpose::PermissionRequest,
        )?;
        let message_id = agent_tool_derived_id(
            &self.session.session_id,
            &request.tool_call_id,
            AgentToolDerivedPurpose::ToolMessage,
        )?;
        self.update_tool(request, ToolCallStatus::Pending, None)?;
        self.update_tool(request, ToolCallStatus::AwaitingPermission, None)?;
        let permission = self.request_permission(PermissionRequest {
            request_id: permission_id,
            tool: request.clone(),
            options: vec![PermissionDecision::AllowOnce, PermissionDecision::Deny],
        })?;
        if permission.decision == PermissionDecision::Deny {
            self.update_tool(request, ToolCallStatus::Cancelled, None)?;
            let update_id = self.next_id("tool-denied")?;
            return self.apply(AgentUpdate::Message {
                update_id,
                message: AgentMessage {
                    message_id,
                    role: MessageRole::Tool,
                    content: vec![ContentBlock::Text {
                        text: "permission denied".to_owned(),
                    }],
                },
            });
        }
        self.update_tool(request, ToolCallStatus::InProgress, None)?;
        let tool_response = self.invoke_tool(request.clone())?;
        if tool_response.tool_call_id != request.tool_call_id {
            return Err(AgentError::Validation(
                "tool response identity does not match request".to_owned(),
            ));
        }
        self.update_tool(
            request,
            ToolCallStatus::Completed,
            Some(tool_response.content.clone()),
        )?;
        let update_id = self.next_id("tool-message")?;
        self.apply(AgentUpdate::Message {
            update_id,
            message: AgentMessage {
                message_id,
                role: MessageRole::Tool,
                content: tool_response.content,
            },
        })
    }

    /// Current bounded Session metadata.
    pub const fn session(&self) -> &AgentSessionCurrent {
        &self.session
    }

    /// Consume the driver and return its host and bounded Session metadata.
    pub fn into_parts(self) -> (H, AgentSessionCurrent) {
        (self.host, self.session)
    }

    /// Consume the driver and return its host, persistence, and bounded metadata.
    pub fn into_persistence_parts(self) -> (H, P, AgentSessionCurrent) {
        (self.host, self.persistence, self.session)
    }

    /// Execute and durably record a typed standalone elicitation occurrence.
    ///
    /// # Errors
    ///
    /// Returns an error when occurrence persistence, provider execution, or
    /// terminal response persistence fails.
    pub fn elicit(&mut self, request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        let AgentHostResponse::Elicitation(response) =
            self.execute_host_call("elicitation", AgentHostRequest::Elicitation(request))?
        else {
            return Err(unexpected_host_response("elicitation"));
        };
        Ok(response)
    }

    /// Execute and durably record a standalone workspace overlay occurrence.
    ///
    /// # Errors
    ///
    /// Returns an error when occurrence persistence, provider execution, or
    /// terminal receipt persistence fails.
    pub fn apply_workspace(&mut self, change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        let AgentHostResponse::Workspace(response) = self.execute_host_call(
            "workspace",
            AgentHostRequest::Workspace(WorkspaceHostRequest::standalone(change)?),
        )?
        else {
            return Err(unexpected_host_response("workspace"));
        };
        Ok(response)
    }

    fn update_tool(
        &mut self,
        request: &ToolRequest,
        status: ToolCallStatus,
        output: Option<Vec<ContentBlock>>,
    ) -> AgentResult<()> {
        let update_id = self.next_id("tool")?;
        self.apply(AgentUpdate::Tool {
            update_id,
            tool: ToolCall {
                tool_call_id: request.tool_call_id.clone(),
                operation: request.operation.clone(),
                status,
                input: request.input.clone(),
                output,
                locations: Vec::new(),
            },
        })
    }

    fn apply(&mut self, update: AgentUpdate) -> AgentResult<()> {
        let command = AgentCommand::new(
            self.revision.clone(),
            AgentCommandAction::SessionUpdate {
                session_id: self.session.session_id.clone(),
                update,
            },
        )?;
        let commit = self.persistence.commit_agent(&command)?;
        commit.verify_for(&command)?;
        let AgentCommandOutcome::Session(postcondition) = commit.receipt.outcome else {
            return Err(AgentError::persistence(
                "agent_session_outcome_mismatch",
                "Agent Session command returned a different typed outcome",
            ));
        };
        self.session = postcondition.session;
        self.session_persisted = true;
        self.revision = commit.observed_revision;
        self.sequence = self.session.latest_update_sequence;
        Ok(())
    }

    fn select_context(&mut self, request: ContextRequest) -> AgentResult<ContextSnapshot> {
        let AgentHostResponse::Context(response) =
            self.execute_host_call("context", AgentHostRequest::Context(request))?
        else {
            return Err(unexpected_host_response("context"));
        };
        Ok(response)
    }

    fn invoke_model(&mut self, request: ModelRequest) -> AgentResult<ModelResponse> {
        let AgentHostResponse::Model(response) =
            self.execute_host_call("model", AgentHostRequest::Model(request))?
        else {
            return Err(unexpected_host_response("model"));
        };
        Ok(response)
    }

    fn request_permission(
        &mut self,
        request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        let AgentHostResponse::Permission(response) =
            self.execute_host_call("permission", AgentHostRequest::Permission(request))?
        else {
            return Err(unexpected_host_response("permission"));
        };
        Ok(response)
    }

    fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse> {
        let AgentHostResponse::Tool(response) =
            self.execute_host_call("tool", AgentHostRequest::Tool(request))?
        else {
            return Err(unexpected_host_response("tool"));
        };
        Ok(response)
    }

    fn execute_host_call(
        &mut self,
        kind: &str,
        request: AgentHostRequest,
    ) -> AgentResult<AgentHostResponse> {
        self.ensure_recovery_clear()?;
        let occurrence_id = format!(
            "occurrence:{kind}:{}",
            self.session.next_occurrence_sequence
        );
        let mut opening_revision = (!self.session_persisted).then(|| self.revision.clone());
        let result = execute_interaction(
            &mut self.host,
            &mut self.persistence,
            &self.session.session_id,
            &occurrence_id,
            &mut opening_revision,
            request,
        );
        if opening_revision.is_none() {
            self.session_persisted = true;
        }
        let response = result?;
        self.refresh_session()?;
        Ok(response)
    }

    fn ensure_recovery_clear(&mut self) -> AgentResult<()> {
        if !self.session_persisted {
            return Ok(());
        }
        self.refresh_session()?;
        if self.session.unresolved_occurrence_count == 0 {
            return Ok(());
        }
        let read = self
            .persistence
            .read_agent_occurrences(&AgentOccurrencePageQuery {
                session_id: self.session.session_id.clone(),
                index_generation: self.session.unresolved_occurrence_generation.clone(),
                after_ordinal: None,
                max_entries: 1,
                max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
                expected_revision: Some(self.revision.clone()),
            })?;
        let unsettled = read.page.entries.first().ok_or_else(|| {
            AgentError::persistence(
                "agent_unresolved_index_missing",
                "Session unresolved count has no matching occurrence index entry",
            )
        })?;
        if unsettled.occurrence.state == AgentHostOccurrenceState::Unknown {
            return Err(AgentError::HostOutcomeUnknown {
                occurrence_id: unsettled.occurrence.occurrence_id.clone(),
            });
        }
        Err(AgentError::RecoveryRequired(format!(
            "host occurrence {} is {:?}; reconcile or cancel it before continuing",
            unsettled.occurrence.occurrence_id, unsettled.occurrence.state
        )))
    }

    fn refresh_session(&mut self) -> AgentResult<()> {
        let read = self.persistence.read_agent_session(&AgentSessionQuery {
            session_id: self.session.session_id.clone(),
            expected_revision: None,
        })?;
        self.session = read.current.ok_or_else(|| {
            AgentError::persistence(
                "agent_session_current_missing",
                format!(
                    "Agent Session {} disappeared from its durable revision",
                    self.session.session_id
                ),
            )
        })?;
        self.revision = read.revision;
        self.sequence = self.session.latest_update_sequence;
        Ok(())
    }

    fn next_id(&mut self, kind: &str) -> AgentResult<String> {
        let next = self.sequence.checked_add(1).ok_or_else(|| {
            AgentError::Validation("Agent update sequence is exhausted".to_owned())
        })?;
        if next > MAX_EXACT_INTEGER {
            return Err(AgentError::Validation(
                "Agent update sequence is exhausted".to_owned(),
            ));
        }
        self.sequence = next;
        Ok(format!("update:{kind}:{}", self.sequence))
    }
}

fn unexpected_host_response(kind: &str) -> AgentError {
    AgentError::persistence(
        "agent_host_response_mismatch",
        format!("Agent {kind} interaction returned a different response kind"),
    )
}

fn validate_model_rounds(rounds: u32) -> AgentResult<()> {
    if rounds == 0 || rounds > MAX_AGENT_MODEL_ROUNDS {
        return Err(AgentError::Validation(format!(
            "Agent model rounds must be within 1..={MAX_AGENT_MODEL_ROUNDS}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::interaction::tests::{BoundaryPersistence, CommitBoundary, CommitPause};
    use cymule_profile_protocol::agent::{
        AgentHostBinding, AgentHostOccurrence, AgentOccurrenceQuery, AgentOccurrenceResolution,
        AgentPlan, AgentPlanEntry, ElicitationRequest, ElicitationResponse, ModelRequest,
        ModelResponse, PermissionRequest, PermissionResponse, PlanEntryStatus, ToolRequest,
        ToolResponse, WorkspaceChange, WorkspaceReceipt,
    };

    #[derive(Default)]
    struct NeverHost {
        binding_calls: usize,
        elicitation_calls: usize,
        elicitation_enabled: bool,
        driver_events: Option<mpsc::Sender<DriverEvent>>,
        worker: usize,
        started_tool_failure: bool,
        tool_flow_decision: Option<PermissionDecision>,
        permission_request_ids: Vec<String>,
        tool_calls: usize,
    }

    impl AgentHost for NeverHost {
        fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<AgentHostBinding> {
            self.binding_calls += 1;
            if self.elicitation_enabled && matches!(request, AgentHostRequest::Elicitation(_)) {
                return AgentHostBinding::standalone("host:elicitation/1", "binding:elicitation/1")
                    .map_err(Into::into);
            }
            if self.tool_flow_decision.is_some()
                && matches!(
                    request,
                    AgentHostRequest::Permission(_) | AgentHostRequest::Tool(_)
                )
            {
                return AgentHostBinding::standalone("host:tool-flow/1", "binding:tool-flow/1")
                    .map_err(Into::into);
            }
            if self.started_tool_failure && matches!(request, AgentHostRequest::Tool(_)) {
                return AgentHostBinding::standalone(
                    "host:started-failure/1",
                    "binding:started-failure/1",
                )
                .map_err(Into::into);
            }
            Err(AgentError::Host("unexpected host binding".to_owned()))
        }

        fn select_context(
            &mut self,
            _request: ContextRequest,
            _messages: &mut dyn crate::AgentMessageReader,
        ) -> AgentResult<ContextSnapshot> {
            Err(AgentError::Host("unexpected context selection".to_owned()))
        }

        fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
            Err(AgentError::Host("unexpected model invocation".to_owned()))
        }

        fn request_permission(
            &mut self,
            request: PermissionRequest,
        ) -> AgentResult<PermissionResponse> {
            if let Some(decision) = self.tool_flow_decision {
                self.permission_request_ids.push(request.request_id);
                return Ok(PermissionResponse {
                    decision,
                    occurrence_binding: "binding:tool-flow/1".to_owned(),
                });
            }
            Err(AgentError::Host("unexpected permission request".to_owned()))
        }

        fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse> {
            if self.started_tool_failure {
                return Err(AgentError::TimedOut {
                    code: "host_timeout".to_owned(),
                    message: "host deadline elapsed".to_owned(),
                });
            }
            if self.tool_flow_decision == Some(PermissionDecision::AllowOnce) {
                self.tool_calls += 1;
                return Ok(ToolResponse {
                    tool_call_id: request.tool_call_id,
                    content: vec![ContentBlock::Text {
                        text: "completed".to_owned(),
                    }],
                    occurrence_binding: "binding:tool-flow/1".to_owned(),
                });
            }
            Err(AgentError::Host("unexpected tool invocation".to_owned()))
        }

        fn elicit(&mut self, request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
            if self.elicitation_enabled {
                self.elicitation_calls += 1;
                if let Some(events) = &self.driver_events {
                    let (resume, wait) = mpsc::channel();
                    events
                        .send(DriverEvent::Dispatched {
                            worker: self.worker,
                            resume,
                        })
                        .unwrap();
                    wait.recv_timeout(Duration::from_secs(10))
                        .expect("test scheduler releases the exact provider call");
                }
                return Ok(ElicitationResponse {
                    request_id: request.request_id,
                    accepted: true,
                    value: Some(serde_json::json!({"answer": "yes"})),
                    occurrence_binding: "binding:elicitation/1".to_owned(),
                });
            }
            Err(AgentError::Host("unexpected elicitation".to_owned()))
        }

        fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
            Err(AgentError::Host(
                "unexpected workspace operation".to_owned(),
            ))
        }

        fn reconcile_occurrence(
            &mut self,
            _occurrence: &AgentHostOccurrence,
        ) -> AgentResult<AgentOccurrenceResolution> {
            Err(AgentError::Host("unexpected reconciliation".to_owned()))
        }
    }

    #[test]
    fn model_round_bound_is_exact() {
        assert!(validate_model_rounds(0).is_err());
        assert!(validate_model_rounds(1).is_ok());
        assert!(validate_model_rounds(MAX_AGENT_MODEL_ROUNDS).is_ok());
        assert!(validate_model_rounds(MAX_AGENT_MODEL_ROUNDS + 1).is_err());
    }

    #[test]
    fn driver_does_not_dispatch_from_a_replayed_started_acknowledgement() {
        let persistence = BoundaryPersistence::new(CommitBoundary::CompetingStarted);
        let mut driver = AgentTurnDriver::open(
            "session:driver-fresh-started",
            NeverHost {
                elicitation_enabled: true,
                ..NeverHost::default()
            },
            persistence,
        )
        .unwrap();
        let error = driver
            .elicit(driver_elicitation())
            .expect_err("a same-head Started replay cannot grant dispatch");
        assert!(matches!(error, AgentError::RecoveryRequired(_)));
        let (host, _, _) = driver.into_persistence_parts();
        assert_eq!(host.elicitation_calls, 0);
    }

    #[test]
    fn driver_lost_prepared_and_started_acknowledgements_require_recovery_without_dispatch() {
        for state in [
            AgentHostOccurrenceState::Prepared,
            AgentHostOccurrenceState::Started,
        ] {
            let persistence = BoundaryPersistence::new(CommitBoundary::LostAcknowledgement(state));
            let mut observer = persistence.inner.clone();
            let mut driver = AgentTurnDriver::open(
                "session:driver-fresh-started",
                NeverHost {
                    elicitation_enabled: true,
                    ..NeverHost::default()
                },
                persistence,
            )
            .unwrap();
            assert!(matches!(
                driver.elicit(driver_elicitation()),
                Err(AgentError::CommitOutcomeUnknown { .. })
            ));
            let query = AgentOccurrenceQuery {
                session_id: "session:driver-fresh-started".to_owned(),
                occurrence_id: "occurrence:elicitation:0".to_owned(),
                expected_revision: None,
            };
            let before = observer.read_agent_occurrence(&query).unwrap();
            assert_eq!(before.current.as_ref().unwrap().occurrence.state, state);
            let retry = driver.elicit(driver_elicitation());
            if state == AgentHostOccurrenceState::Prepared {
                assert!(
                    matches!(retry, Err(AgentError::Persistence { code, .. }) if code == "ephemeral_agent_revision_conflict")
                );
            } else {
                assert!(matches!(retry, Err(AgentError::RecoveryRequired(_))));
            }
            assert_eq!(observer.read_agent_occurrence(&query).unwrap(), before);
            let (host, persistence, _) = driver.into_persistence_parts();
            assert_eq!(host.binding_calls, 1);
            assert_eq!(host.elicitation_calls, 0);
            assert!(matches!(
                AgentTurnDriver::resume(
                    "session:driver-fresh-started",
                    NeverHost::default(),
                    persistence
                ),
                Err(AgentError::RecoveryRequired(_))
            ));
        }
    }

    fn create_competing_session(persistence: &mut crate::EphemeralAgentPersistence) {
        let source = persistence
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:driver-fresh-started".to_owned(),
                expected_revision: None,
            })
            .unwrap();
        assert!(source.current.is_none());
        let command = AgentCommand::new(
            source.revision,
            AgentCommandAction::SessionUpdate {
                session_id: "session:driver-fresh-started".to_owned(),
                update: AgentUpdate::Plan {
                    update_id: "update:competing-session".to_owned(),
                    plan: AgentPlan {
                        plan_id: "plan:competing-session".to_owned(),
                        entries: Vec::new(),
                    },
                },
            },
        )
        .unwrap();
        persistence.commit_agent(&command).unwrap();
    }

    #[test]
    fn driver_open_absence_pin_cannot_join_a_concurrently_created_session() {
        let persistence = crate::EphemeralAgentPersistence::default();
        let mut writer = persistence.clone();
        let mut driver = AgentTurnDriver::open(
            "session:driver-fresh-started",
            NeverHost {
                elicitation_enabled: true,
                ..NeverHost::default()
            },
            persistence,
        )
        .unwrap();
        create_competing_session(&mut writer);
        let query = AgentSessionQuery {
            session_id: "session:driver-fresh-started".to_owned(),
            expected_revision: None,
        };
        let before = writer.read_agent_session(&query).unwrap();
        assert!(matches!(
            driver.elicit(driver_elicitation()),
            Err(AgentError::Persistence { code, .. }) if code == "ephemeral_agent_revision_conflict"
        ));
        assert_eq!(writer.read_agent_session(&query).unwrap(), before);
        let (host, _, _) = driver.into_persistence_parts();
        assert_eq!(host.binding_calls, 0);
        assert_eq!(host.elicitation_calls, 0);
    }

    #[test]
    fn driver_unknown_initial_commit_keeps_its_absence_pin_before_another_session_appears() {
        let persistence = BoundaryPersistence::new(CommitBoundary::UncertainPreparedWithoutWrite);
        let mut writer = persistence.inner.clone();
        let mut driver = AgentTurnDriver::open(
            "session:driver-fresh-started",
            NeverHost {
                elicitation_enabled: true,
                ..NeverHost::default()
            },
            persistence,
        )
        .unwrap();
        assert!(matches!(
            driver.elicit(driver_elicitation()),
            Err(AgentError::CommitOutcomeUnknown { .. })
        ));
        create_competing_session(&mut writer);
        let query = AgentSessionQuery {
            session_id: "session:driver-fresh-started".to_owned(),
            expected_revision: None,
        };
        let before = writer.read_agent_session(&query).unwrap();
        assert!(matches!(
            driver.elicit(driver_elicitation()),
            Err(AgentError::Persistence { code, .. }) if code == "ephemeral_agent_revision_conflict"
        ));
        assert_eq!(writer.read_agent_session(&query).unwrap(), before);
        let (host, _, _) = driver.into_persistence_parts();
        assert_eq!(host.binding_calls, 1);
        assert_eq!(host.elicitation_calls, 0);
    }

    fn driver_elicitation() -> ElicitationRequest {
        ElicitationRequest {
            request_id: "elicitation:driver-fresh-started".to_owned(),
            prompt: vec![ContentBlock::Text {
                text: "Continue?".to_owned(),
            }],
            schema: serde_json::json!({"type": "object"}),
        }
    }

    enum DriverEvent {
        Dispatched {
            worker: usize,
            resume: mpsc::Sender<()>,
        },
        Finished {
            worker: usize,
            result: AgentResult<ElicitationResponse>,
        },
    }

    fn paused_driver(
        worker: usize,
        persistence: crate::EphemeralAgentPersistence,
        commits: mpsc::Sender<CommitPause>,
        events: mpsc::Sender<DriverEvent>,
    ) -> AgentTurnDriver<NeverHost, BoundaryPersistence> {
        AgentTurnDriver::open(
            "session:driver-fresh-started",
            NeverHost {
                elicitation_enabled: true,
                driver_events: Some(events),
                worker,
                ..NeverHost::default()
            },
            BoundaryPersistence::interleaved(persistence, commits),
        )
        .unwrap()
    }

    fn commit_pause(
        commits: &mpsc::Receiver<CommitPause>,
        expected: AgentHostOccurrenceState,
    ) -> CommitPause {
        let pause = commits.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(matches!(
            &pause.command.action,
            AgentCommandAction::Occurrence { occurrence } if occurrence.state == expected
        ));
        pause
    }

    fn finished_driver(
        events: &mpsc::Receiver<DriverEvent>,
        expected: usize,
    ) -> AgentResult<ElicitationResponse> {
        let DriverEvent::Finished { worker, result } =
            events.recv_timeout(Duration::from_secs(10)).unwrap()
        else {
            panic!("driver must finish without a second provider call");
        };
        assert_eq!(worker, expected);
        result
    }

    fn run_paused_driver(
        mut driver: AgentTurnDriver<NeverHost, BoundaryPersistence>,
        worker: usize,
        events: &mpsc::Sender<DriverEvent>,
    ) -> usize {
        let result = driver.elicit(driver_elicitation());
        let (host, _, _) = driver.into_persistence_parts();
        events
            .send(DriverEvent::Finished { worker, result })
            .unwrap();
        host.elicitation_calls
    }

    #[derive(Clone, Copy)]
    enum StartedRace {
        RetainedCurrent,
        CommandReplay,
    }

    #[test]
    fn two_public_drivers_at_the_same_started_command_dispatch_only_once() {
        assert_two_drivers_dispatch_once(StartedRace::CommandReplay);
    }

    #[test]
    fn two_public_drivers_reading_retained_started_state_dispatch_only_once() {
        assert_two_drivers_dispatch_once(StartedRace::RetainedCurrent);
    }

    fn assert_two_drivers_dispatch_once(race: StartedRace) {
        let mut observer = crate::EphemeralAgentPersistence::default();
        let (first_commit_tx, first_commits) = mpsc::channel();
        let (second_commit_tx, second_commits) = mpsc::channel();
        let (event_tx, events) = mpsc::channel();
        let first = paused_driver(0, observer.clone(), first_commit_tx, event_tx.clone());
        let second = paused_driver(1, observer.clone(), second_commit_tx, event_tx.clone());
        let (begin_second, second_ready) = mpsc::channel();
        let (calls, first_result, second_result) = std::thread::scope(|scope| {
            let first_events = event_tx.clone();
            let first = scope.spawn(move || run_paused_driver(first, 0, &first_events));
            let second = scope.spawn(move || {
                second_ready.recv_timeout(Duration::from_secs(10)).unwrap();
                run_paused_driver(second, 1, &event_tx)
            });
            let first_prepare = commit_pause(&first_commits, AgentHostOccurrenceState::Prepared);
            begin_second.send(()).unwrap();
            let second_prepare = commit_pause(&second_commits, AgentHostOccurrenceState::Prepared);
            assert_eq!(first_prepare.command, second_prepare.command);
            first_prepare.resume.send(()).unwrap();
            let first_start = commit_pause(&first_commits, AgentHostOccurrenceState::Started);
            let resume_second = match race {
                StartedRace::RetainedCurrent => second_prepare.resume,
                StartedRace::CommandReplay => {
                    second_prepare.resume.send(()).unwrap();
                    let second_start =
                        commit_pause(&second_commits, AgentHostOccurrenceState::Started);
                    assert_eq!(first_start.command, second_start.command);
                    second_start.resume
                }
            };
            first_start.resume.send(()).unwrap();
            let DriverEvent::Dispatched {
                worker: 0,
                resume: finish_first,
            } = events.recv_timeout(Duration::from_secs(10)).unwrap()
            else {
                panic!("the fresh winner must dispatch first");
            };
            resume_second.send(()).unwrap();
            let second_result = match events.recv_timeout(Duration::from_secs(10)).unwrap() {
                DriverEvent::Finished { worker: 1, result } => result,
                DriverEvent::Dispatched { worker: 1, resume } => {
                    resume.send(()).unwrap();
                    finished_driver(&events, 1)
                }
                _ => panic!("the second driver must resolve its same-command replay"),
            };
            finish_first.send(()).unwrap();
            let first_result = finished_driver(&events, 0);
            (
                first.join().unwrap() + second.join().unwrap(),
                first_result,
                second_result,
            )
        });
        assert_eq!(
            calls, 1,
            "only the freshly acknowledged Started caller may dispatch"
        );
        assert!(first_result.is_ok());
        assert!(matches!(
            second_result,
            Err(AgentError::RecoveryRequired(_))
        ));
        let current = observer
            .read_agent_occurrence(&AgentOccurrenceQuery {
                session_id: "session:driver-fresh-started".to_owned(),
                occurrence_id: "occurrence:elicitation:0".to_owned(),
                expected_revision: None,
            })
            .unwrap()
            .current
            .unwrap();
        assert_eq!(
            current.occurrence.state,
            AgentHostOccurrenceState::Completed
        );
    }

    #[test]
    fn reference_driver_returns_occurrence_unknown_after_started_host_failure() {
        let persistence = crate::EphemeralAgentPersistence::default();
        let mut observer = persistence.clone();
        let mut driver = AgentTurnDriver::open(
            "session:driver-host-unknown",
            NeverHost {
                started_tool_failure: true,
                ..NeverHost::default()
            },
            persistence,
        )
        .expect("driver opens over an absent Session");
        let error = driver
            .invoke_tool(ToolRequest {
                tool_call_id: "tool:driver-host-unknown".to_owned(),
                operation: "test.write".to_owned(),
                input: serde_json::json!({}),
            })
            .expect_err("a Started host timeout has no terminal world outcome");
        assert_eq!(
            error,
            AgentError::HostOutcomeUnknown {
                occurrence_id: "occurrence:tool:0".to_owned(),
            }
        );
        let current = observer
            .read_agent_occurrence(&AgentOccurrenceQuery {
                session_id: "session:driver-host-unknown".to_owned(),
                occurrence_id: "occurrence:tool:0".to_owned(),
                expected_revision: None,
            })
            .expect("driver occurrence reads")
            .current
            .expect("driver occurrence remains durable");
        assert_eq!(current.occurrence.state, AgentHostOccurrenceState::Unknown);
        assert!(current.occurrence.failure.is_some());
    }

    #[test]
    fn maximum_tool_identity_completes_allow_and_deny_without_truncation() {
        use cymule_profile_protocol::agent::{AgentMessageQuery, AgentToolQuery};

        for decision in [PermissionDecision::AllowOnce, PermissionDecision::Deny] {
            let session_id = "界".repeat(512);
            let request = ToolRequest {
                tool_call_id: "🧪".repeat(512),
                operation: "test.read".to_owned(),
                input: serde_json::json!({"path": "README.md"}),
            };
            let permission_id = agent_tool_derived_id(
                &session_id,
                &request.tool_call_id,
                AgentToolDerivedPurpose::PermissionRequest,
            )
            .expect("maximum permission identity derives");
            let message_id = agent_tool_derived_id(
                &session_id,
                &request.tool_call_id,
                AgentToolDerivedPurpose::ToolMessage,
            )
            .expect("maximum message identity derives");
            let mut driver = AgentTurnDriver::open(
                &session_id,
                NeverHost {
                    tool_flow_decision: Some(decision),
                    ..NeverHost::default()
                },
                crate::EphemeralAgentPersistence::default(),
            )
            .expect("maximum Session opens");
            driver
                .run_tool_request(&request)
                .expect("maximum Tool reaches its terminal update");
            let (host, mut persistence, session) = driver.into_persistence_parts();
            assert_eq!(host.permission_request_ids, [permission_id]);
            assert_eq!(
                host.tool_calls,
                usize::from(decision == PermissionDecision::AllowOnce)
            );
            assert_eq!(session.message_count, 1);
            assert_eq!(session.unresolved_occurrence_count, 0);
            let tool = persistence
                .read_agent_tool(&AgentToolQuery {
                    session_id: session_id.clone(),
                    tool_call_id: request.tool_call_id.clone(),
                    expected_revision: None,
                })
                .expect("exact Tool reads")
                .current
                .expect("Tool is retained");
            assert_eq!(tool.tool.tool_call_id, request.tool_call_id);
            assert_eq!(
                tool.tool.status,
                if decision == PermissionDecision::AllowOnce {
                    ToolCallStatus::Completed
                } else {
                    ToolCallStatus::Cancelled
                }
            );
            let resumed = AgentTurnDriver::resume(&session_id, NeverHost::default(), persistence)
                .expect("completed Tool permits an explicit driver reopen");
            let (_, mut persistence, resumed_session) = resumed.into_persistence_parts();
            assert_eq!(resumed_session, session);
            let message = persistence
                .read_agent_message(&AgentMessageQuery {
                    session_id,
                    message_id: message_id.clone(),
                    expected_revision: None,
                })
                .expect("exact result message reads after reopen")
                .current
                .expect("result message is retained");
            assert_eq!(message.message.message_id, message_id);
            assert_eq!(message.message.role, MessageRole::Tool);
        }
    }

    #[test]
    fn open_and_resume_have_disjoint_session_presence_contracts() {
        let persistence = crate::EphemeralAgentPersistence::default();
        assert!(matches!(
            AgentTurnDriver::resume(
                "session:explicit-open",
                NeverHost::default(),
                persistence.clone(),
            ),
            Err(AgentError::NotFound(_))
        ));

        let mut driver = AgentTurnDriver::open(
            "session:explicit-open",
            NeverHost::default(),
            persistence.clone(),
        )
        .expect("absent Session opens from an explicit genesis");
        driver
            .apply(AgentUpdate::Plan {
                update_id: "update:explicit-open:1".to_owned(),
                plan: AgentPlan {
                    plan_id: "plan:explicit-open".to_owned(),
                    entries: vec![AgentPlanEntry {
                        entry_id: "entry:explicit-open".to_owned(),
                        content: "persist the explicit genesis".to_owned(),
                        status: PlanEntryStatus::Pending,
                    }],
                },
            })
            .expect("first command commits over the exact absent-key revision");
        let (_, persistence, _) = driver.into_persistence_parts();

        assert!(matches!(
            AgentTurnDriver::open(
                "session:explicit-open",
                NeverHost::default(),
                persistence.clone(),
            ),
            Err(AgentError::IllegalTransition(_))
        ));
        let _resumed =
            AgentTurnDriver::resume("session:explicit-open", NeverHost::default(), persistence)
                .expect("persisted Session resumes without a genesis fallback");
    }
}
