use cymule_profile_protocol::agent::{
    AgentCommand, AgentCommandAction, AgentCommandOutcome, AgentHostOccurrence,
    AgentHostOccurrenceState, AgentHostRequest, AgentHostResponse, AgentOccurrenceQuery,
    AgentSessionQuery,
};

use crate::{AgentError, AgentHost, AgentPersistence, AgentResult, PinnedAgentMessageReader};

enum OccurrenceAdmission {
    Fresh(AgentHostOccurrence),
    Retained(AgentHostOccurrence),
}

impl OccurrenceAdmission {
    fn into_occurrence(self) -> AgentHostOccurrence {
        match self {
            Self::Fresh(occurrence) | Self::Retained(occurrence) => occurrence,
        }
    }
}

/// Provider-neutral controller for individually identified Agent interactions.
///
/// The controller retains no Session-wide occurrence cache. Every decision is
/// made from one exact revision-pinned occurrence read and one closed command.
#[must_use]
pub struct AgentInteractionController<H, P> {
    host: H,
    persistence: P,
    session_id: String,
    opening_revision: Option<String>,
}

impl<H: AgentHost, P: AgentPersistence> AgentInteractionController<H, P> {
    /// Open a controller after proving the Session key is absent at one exact revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session identity is invalid, its current
    /// revision cannot be read, or the Session already exists.
    pub fn open(session_id: impl Into<String>, host: H, mut persistence: P) -> AgentResult<Self> {
        let session_id = session_id.into();
        let read =
            persistence.read_agent_session(&cymule_profile_protocol::agent::AgentSessionQuery {
                session_id: session_id.clone(),
                expected_revision: None,
            })?;
        if read.current.is_some() {
            return Err(AgentError::IllegalTransition(format!(
                "Agent Session {session_id} already exists; resume it instead"
            )));
        }
        Ok(Self {
            host,
            persistence,
            session_id,
            opening_revision: Some(read.revision),
        })
    }

    /// Resume a controller after proving the Session key exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the Session identity is invalid, its current
    /// revision cannot be read, or the Session is absent.
    pub fn resume(session_id: impl Into<String>, host: H, mut persistence: P) -> AgentResult<Self> {
        let session_id = session_id.into();
        let read =
            persistence.read_agent_session(&cymule_profile_protocol::agent::AgentSessionQuery {
                session_id: session_id.clone(),
                expected_revision: None,
            })?;
        if read.current.is_none() {
            return Err(AgentError::NotFound(format!(
                "Agent Session {session_id} does not exist; open it instead"
            )));
        }
        Ok(Self {
            host,
            persistence,
            session_id,
            opening_revision: None,
        })
    }

    /// Execute a newly identified interaction or return its retained response.
    ///
    /// Prepared and Started are committed before provider dispatch. Only this
    /// call's verified fresh Started acknowledgement permits dispatch; an
    /// existing or replayed Started occurrence requires recovery. A provider
    /// result is then committed as Completed or Unknown. Once Started is
    /// durable, every ordinary host error becomes [`AgentError::HostOutcomeUnknown`]
    /// for that exact occurrence; the pre-dispatch error is not returned as if
    /// it proved a world outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when the occurrence is invalid or conflicts with an
    /// existing request, persistence cannot establish its lifecycle, provider
    /// execution fails, or recovery is required.
    pub fn execute(
        &mut self,
        occurrence_id: impl Into<String>,
        request: AgentHostRequest,
    ) -> AgentResult<AgentHostResponse> {
        let occurrence_id = occurrence_id.into();
        execute_interaction(
            &mut self.host,
            &mut self.persistence,
            &self.session_id,
            &occurrence_id,
            &mut self.opening_revision,
            request,
        )
    }

    /// Read the latest exact occurrence current without invoking the host.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact occurrence read cannot be verified.
    pub fn occurrence(&mut self, occurrence_id: &str) -> AgentResult<Option<AgentHostOccurrence>> {
        load_occurrence(&mut self.persistence, &self.session_id, occurrence_id)
    }

    /// Consume the controller and return its host and persistence capability.
    pub fn into_parts(self) -> (H, P) {
        (self.host, self.persistence)
    }
}

/// One shared lifecycle for standalone controllers and the reference driver.
/// Only this invocation's freshly acknowledged Started commit reaches dispatch.
pub(crate) fn execute_interaction<H: AgentHost, P: AgentPersistence>(
    host: &mut H,
    persistence: &mut P,
    session_id: &str,
    occurrence_id: &str,
    opening_revision: &mut Option<String>,
    request: AgentHostRequest,
) -> AgentResult<AgentHostResponse> {
    if occurrence_id.is_empty() {
        return Err(AgentError::Validation(
            "agent interaction requires an occurrence identity".to_owned(),
        ));
    }
    request.validate_for_session(session_id)?;
    if request.is_m1_workspace() {
        return Err(AgentError::Validation(
            "M1-owned workspace requests require WorkspaceScopeController".to_owned(),
        ));
    }
    if let Some(revision) = opening_revision.as_ref() {
        let read = persistence.read_agent_session(&AgentSessionQuery {
            session_id: session_id.to_owned(),
            expected_revision: Some(revision.clone()),
        })?;
        if read.current.is_some() {
            return Err(AgentError::IllegalTransition(format!(
                "Agent Session {session_id} appeared after open admission",
            )));
        }
    }
    if let Some(existing) = load_occurrence(persistence, session_id, occurrence_id)? {
        return retained_response_for(&existing, &request);
    }

    let occurrence_binding = host.bind_occurrence(&request)?;
    let proposed = AgentHostOccurrence::prepare(
        occurrence_id,
        session_id,
        request.clone(),
        occurrence_binding,
    )?;
    let prepared = admit_prepared(persistence, &proposed, opening_revision)?.into_occurrence();
    if prepared != proposed {
        return retained_response_for(&prepared, &request);
    }
    let started = match admit_occurrence(persistence, &prepared.start()?, Some(&prepared))? {
        OccurrenceAdmission::Fresh(started) => started,
        OccurrenceAdmission::Retained(_) => {
            let current =
                load_occurrence(persistence, session_id, occurrence_id)?.ok_or_else(|| {
                    AgentError::persistence(
                        "agent_occurrence_admission_missing",
                        "retained occurrence disappeared after its Started admission",
                    )
                })?;
            return retained_response_for(&current, &request);
        }
    };

    match dispatch(host, persistence, request) {
        Ok(response) => {
            let completed = persist_occurrence(persistence, started.complete(response)?)?;
            retained_response(&completed)
        }
        Err(error) => {
            persist_occurrence(persistence, started.mark_unknown(error.to_string())?)?;
            Err(AgentError::HostOutcomeUnknown {
                occurrence_id: started.occurrence_id,
            })
        }
    }
}

fn admit_prepared<P: AgentPersistence>(
    persistence: &mut P,
    prepared: &AgentHostOccurrence,
    opening_revision: &mut Option<String>,
) -> AgentResult<OccurrenceAdmission> {
    let Some(revision) = opening_revision.clone() else {
        return admit_occurrence(persistence, prepared, None);
    };
    let admitted = commit_occurrence(persistence, prepared.clone(), revision);
    if admitted.is_ok() {
        *opening_revision = None;
    }
    admitted
}

fn load_occurrence<P: AgentPersistence>(
    persistence: &mut P,
    session_id: &str,
    occurrence_id: &str,
) -> AgentResult<Option<AgentHostOccurrence>> {
    let read = persistence.read_agent_occurrence(&AgentOccurrenceQuery {
        session_id: session_id.to_owned(),
        occurrence_id: occurrence_id.to_owned(),
        expected_revision: None,
    })?;
    Ok(read.current.map(|current| current.occurrence))
}

fn admit_occurrence<P: AgentPersistence>(
    persistence: &mut P,
    proposed: &AgentHostOccurrence,
    expected: Option<&AgentHostOccurrence>,
) -> AgentResult<OccurrenceAdmission> {
    let read = persistence.read_agent_occurrence(&AgentOccurrenceQuery {
        session_id: proposed.session_id.clone(),
        occurrence_id: proposed.occurrence_id.clone(),
        expected_revision: None,
    })?;
    let current = read.current.map(|current| current.occurrence);
    if current.as_ref() != expected {
        return current.map(OccurrenceAdmission::Retained).ok_or_else(|| {
            AgentError::persistence(
                "agent_occurrence_admission_missing",
                "the exact Prepared occurrence disappeared before Started admission",
            )
        });
    }
    commit_occurrence(persistence, proposed.clone(), read.revision)
}

pub(crate) fn persist_occurrence<P: AgentPersistence>(
    persistence: &mut P,
    occurrence: AgentHostOccurrence,
) -> AgentResult<AgentHostOccurrence> {
    let read = persistence.read_agent_occurrence(&AgentOccurrenceQuery {
        session_id: occurrence.session_id.clone(),
        occurrence_id: occurrence.occurrence_id.clone(),
        expected_revision: None,
    })?;
    if let Some(current) = &read.current
        && current.occurrence == occurrence
    {
        return Ok(occurrence);
    }
    commit_occurrence(persistence, occurrence, read.revision)
        .map(OccurrenceAdmission::into_occurrence)
}

fn commit_occurrence<P: AgentPersistence>(
    persistence: &mut P,
    occurrence: AgentHostOccurrence,
    source_revision: String,
) -> AgentResult<OccurrenceAdmission> {
    let command = AgentCommand::new(
        source_revision,
        AgentCommandAction::Occurrence {
            occurrence: Box::new(occurrence),
        },
    )?;
    let commit = persistence.commit_agent(&command)?;
    commit.verify_for(&command)?;
    let fresh = commit.committed_revision.is_some();
    let AgentCommandOutcome::Occurrence(postcondition) = commit.receipt.outcome else {
        return Err(AgentError::persistence(
            "agent_occurrence_outcome_mismatch",
            "Agent occurrence command returned a different typed outcome",
        ));
    };
    Ok(if fresh {
        OccurrenceAdmission::Fresh(postcondition.current.occurrence)
    } else {
        OccurrenceAdmission::Retained(postcondition.current.occurrence)
    })
}

fn retained_response_for(
    occurrence: &AgentHostOccurrence,
    request: &AgentHostRequest,
) -> AgentResult<AgentHostResponse> {
    if &occurrence.request != request {
        return Err(AgentError::IllegalTransition(format!(
            "host occurrence {} was reused with a different request",
            occurrence.occurrence_id,
        )));
    }
    retained_response(occurrence)
}

fn retained_response(occurrence: &AgentHostOccurrence) -> AgentResult<AgentHostResponse> {
    match occurrence.state {
        AgentHostOccurrenceState::Completed => occurrence.response.clone().ok_or_else(|| {
            AgentError::persistence(
                "agent_occurrence_response_missing",
                format!(
                    "completed host occurrence {} has no retained response",
                    occurrence.occurrence_id
                ),
            )
        }),
        AgentHostOccurrenceState::NotApplied => Err(AgentError::RecoveryRequired(format!(
            "host occurrence {} is not_applied; admit a separate replacement identity or terminate the caller-owned loop",
            occurrence.occurrence_id
        ))),
        AgentHostOccurrenceState::Unknown => Err(AgentError::HostOutcomeUnknown {
            occurrence_id: occurrence.occurrence_id.clone(),
        }),
        state @ (AgentHostOccurrenceState::Prepared | AgentHostOccurrenceState::Started) => {
            Err(AgentError::RecoveryRequired(format!(
                "host occurrence {} is {state:?}; reconcile or cancel it before reuse",
                occurrence.occurrence_id
            )))
        }
    }
}

fn dispatch<H: AgentHost, P: AgentPersistence>(
    host: &mut H,
    persistence: &mut P,
    request: AgentHostRequest,
) -> AgentResult<AgentHostResponse> {
    match request {
        AgentHostRequest::Context(request) => {
            let mut messages = PinnedAgentMessageReader::new(persistence, &request)?;
            let response = host.select_context(request, &mut messages)?;
            messages.verify_snapshot(&response)?;
            Ok(AgentHostResponse::Context(response))
        }
        AgentHostRequest::Model(request) => {
            host.invoke_model(request).map(AgentHostResponse::Model)
        }
        AgentHostRequest::Permission(request) => host
            .request_permission(request)
            .map(AgentHostResponse::Permission),
        AgentHostRequest::Tool(request) => host.invoke_tool(request).map(AgentHostResponse::Tool),
        AgentHostRequest::Elicitation(request) => {
            host.elicit(request).map(AgentHostResponse::Elicitation)
        }
        AgentHostRequest::Workspace(request) => host
            .apply_workspace(request.change().clone())
            .map(AgentHostResponse::Workspace),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use cymule_profile_protocol::agent as protocol;
    use cymule_profile_protocol::agent::{
        AgentContextMessageRef, AgentContextScanLimits, AgentHostBinding, AgentSessionQuery,
        AgentState, AgentUpdate, ContentBlock, ContextRequest, ContextSnapshot, ElicitationRequest,
        ElicitationResponse, MAX_AGENT_CONTEXT_SCAN_BYTES, ModelRequest, ModelResponse,
        PermissionRequest, PermissionResponse, ToolRequest, ToolResponse, WorkspaceChange,
        WorkspaceReceipt,
    };
    use serde_json::json;

    use super::*;
    use crate::{AgentMessageReader, EphemeralAgentPersistence};

    pub(crate) enum CommitBoundary {
        CompetingStarted,
        LostAcknowledgement(AgentHostOccurrenceState),
        UncertainPreparedWithoutWrite,
    }

    pub(crate) struct BoundaryPersistence {
        pub(crate) inner: EphemeralAgentPersistence,
        boundary: Option<CommitBoundary>,
        admitted: Option<(AgentCommand, protocol::AgentCommit)>,
        commit_pauses: Option<mpsc::Sender<CommitPause>>,
    }

    pub(crate) struct CommitPause {
        pub(crate) command: AgentCommand,
        pub(crate) resume: mpsc::Sender<()>,
    }

    impl BoundaryPersistence {
        pub(crate) fn new(boundary: CommitBoundary) -> Self {
            Self {
                inner: EphemeralAgentPersistence::default(),
                boundary: Some(boundary),
                admitted: None,
                commit_pauses: None,
            }
        }

        pub(crate) fn interleaved(
            inner: EphemeralAgentPersistence,
            commit_pauses: mpsc::Sender<CommitPause>,
        ) -> Self {
            Self {
                inner,
                boundary: None,
                admitted: None,
                commit_pauses: Some(commit_pauses),
            }
        }
    }

    impl AgentPersistence for BoundaryPersistence {
        fn commit_agent(&mut self, command: &AgentCommand) -> AgentResult<protocol::AgentCommit> {
            if let Some(pauses) = &self.commit_pauses
                && matches!(
                    &command.action,
                    AgentCommandAction::Occurrence { occurrence }
                        if matches!(occurrence.state, AgentHostOccurrenceState::Prepared | AgentHostOccurrenceState::Started)
                )
            {
                let (resume, wait) = mpsc::channel();
                pauses
                    .send(CommitPause {
                        command: command.clone(),
                        resume,
                    })
                    .unwrap();
                wait.recv_timeout(Duration::from_secs(10))
                    .expect("test scheduler releases the exact commit boundary");
            }
            let target = match &self.boundary {
                Some(CommitBoundary::CompetingStarted) => AgentHostOccurrenceState::Started,
                Some(CommitBoundary::LostAcknowledgement(state)) => *state,
                Some(CommitBoundary::UncertainPreparedWithoutWrite) => {
                    AgentHostOccurrenceState::Prepared
                }
                None => return self.inner.commit_agent(command),
            };
            if !matches!(
                &command.action,
                AgentCommandAction::Occurrence { occurrence } if occurrence.state == target
            ) {
                return self.inner.commit_agent(command);
            }
            let boundary = self.boundary.take().expect("one targeted boundary exists");
            if matches!(boundary, CommitBoundary::UncertainPreparedWithoutWrite) {
                return Err(AgentError::CommitOutcomeUnknown {
                    message: "the Prepared acknowledgement is unknown; this attempt wrote nothing"
                        .to_owned(),
                });
            }
            // A separate public writer accepts the exact command between the
            // controller's source read and its commit, or its response is lost.
            // Both cases retain real reducer output without raw state injection.
            let mut writer = self.inner.clone();
            let admitted = writer.commit_agent(command)?;
            assert_eq!(
                admitted.committed_revision.as_ref(),
                Some(&admitted.observed_revision),
            );
            self.admitted = Some((command.clone(), admitted));
            match boundary {
                CommitBoundary::CompetingStarted => self.inner.commit_agent(command),
                CommitBoundary::LostAcknowledgement(_)
                | CommitBoundary::UncertainPreparedWithoutWrite => {
                    Err(AgentError::CommitOutcomeUnknown {
                        message:
                            "the exact Agent command committed but its acknowledgement was lost"
                                .to_owned(),
                    })
                }
            }
        }

        fn finalize_agent_stream(
            &mut self,
            command: &AgentCommand,
        ) -> AgentResult<protocol::AgentStreamFinalizeOutcome> {
            self.inner.finalize_agent_stream(command)
        }

        fn reconcile_agent_stream(
            &mut self,
            command: &AgentCommand,
            intent: &protocol::AgentStreamPublicationIntent,
        ) -> AgentResult<protocol::AgentStreamFinalizeOutcome> {
            self.inner.reconcile_agent_stream(command, intent)
        }

        fn commit_agent_workspace(
            &mut self,
            command: &AgentCommand,
        ) -> AgentResult<protocol::AgentWorkspaceCommitOutcome> {
            self.inner.commit_agent_workspace(command)
        }

        fn read_agent_session(
            &mut self,
            query: &AgentSessionQuery,
        ) -> AgentResult<protocol::AgentSessionRead> {
            self.inner.read_agent_session(query)
        }

        fn read_agent_messages(
            &mut self,
            query: &protocol::AgentMessagePageQuery,
        ) -> AgentResult<protocol::AgentMessagePageRead> {
            self.inner.read_agent_messages(query)
        }

        fn read_agent_message(
            &mut self,
            query: &protocol::AgentMessageQuery,
        ) -> AgentResult<protocol::AgentMessageRead> {
            self.inner.read_agent_message(query)
        }

        fn read_agent_tool(
            &mut self,
            query: &protocol::AgentToolQuery,
        ) -> AgentResult<protocol::AgentToolRead> {
            self.inner.read_agent_tool(query)
        }

        fn read_agent_elicitation(
            &mut self,
            query: &protocol::AgentElicitationQuery,
        ) -> AgentResult<protocol::AgentElicitationRead> {
            self.inner.read_agent_elicitation(query)
        }

        fn read_agent_occurrence(
            &mut self,
            query: &AgentOccurrenceQuery,
        ) -> AgentResult<protocol::AgentOccurrenceRead> {
            self.inner.read_agent_occurrence(query)
        }

        fn read_agent_occurrences(
            &mut self,
            query: &protocol::AgentOccurrencePageQuery,
        ) -> AgentResult<protocol::AgentOccurrencePageRead> {
            self.inner.read_agent_occurrences(query)
        }

        fn read_agent_stream(
            &mut self,
            query: &protocol::AgentStreamQuery,
        ) -> AgentResult<protocol::AgentStreamRead> {
            self.inner.read_agent_stream(query)
        }

        fn read_agent_workspace_admission(
            &mut self,
            query: &protocol::AgentWorkspaceAdmissionQuery,
        ) -> AgentResult<protocol::AgentWorkspaceAdmissionRead> {
            self.inner.read_agent_workspace_admission(query)
        }
    }

    #[derive(Default)]
    struct CountingHost {
        binding_calls: usize,
        context_calls: usize,
        context_acceptances: usize,
        forge_context_ordinal: bool,
        tool_calls: usize,
        tool_error: Option<AgentError>,
    }

    impl AgentHost for CountingHost {
        fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<AgentHostBinding> {
            self.binding_calls += 1;
            match request {
                AgentHostRequest::Context(_) => {
                    AgentHostBinding::standalone("host:test/1", "binding:context/1")
                        .map_err(Into::into)
                }
                AgentHostRequest::Tool(_) => {
                    AgentHostBinding::standalone("host:test/1", "binding:tool/1")
                        .map_err(Into::into)
                }
                _ => panic!("CountingHost received an unsupported request"),
            }
        }

        fn select_context(
            &mut self,
            request: ContextRequest,
            messages: &mut dyn AgentMessageReader,
        ) -> AgentResult<ContextSnapshot> {
            self.context_calls += 1;
            let page = messages
                .read_previous(1)?
                .ok_or_else(|| AgentError::Host("expected one context message".to_owned()))?;
            let current = page
                .page
                .entries
                .last()
                .ok_or_else(|| AgentError::Host("expected one context message".to_owned()))?;
            let mut selected = AgentContextMessageRef::from_current(current)?;
            if self.forge_context_ordinal {
                assert_eq!(selected.index, 1);
                selected.index = 0;
            }
            self.context_acceptances += 1;
            Ok(ContextSnapshot {
                snapshot_id: "snapshot:controller-context".to_owned(),
                source_message_head: request.source_message_head,
                source_message_count: request.source_message_count,
                selected_messages: vec![selected],
                content: current.message.content.clone(),
                occurrence_binding: "binding:context/1".to_owned(),
            })
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

        fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse> {
            self.tool_calls += 1;
            if let Some(error) = self.tool_error.clone() {
                return Err(error);
            }
            Ok(ToolResponse {
                tool_call_id: request.tool_call_id,
                content: vec![ContentBlock::Json {
                    value: json!({"result": "ok"}),
                }],
                occurrence_binding: "binding:tool/1".to_owned(),
            })
        }

        fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
            Err(AgentError::Host(
                "unexpected elicitation dispatch".to_owned(),
            ))
        }

        fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
            Err(AgentError::Host("unexpected workspace dispatch".to_owned()))
        }
    }

    fn boundary_request() -> AgentHostRequest {
        AgentHostRequest::Tool(ToolRequest {
            tool_call_id: "tool:commit-boundary".to_owned(),
            operation: "test.write".to_owned(),
            input: json!({"path": "README.md"}),
        })
    }

    #[test]
    fn same_started_command_interleaving_never_redispatches_from_same_head_replay() {
        let persistence = BoundaryPersistence::new(CommitBoundary::CompetingStarted);
        let mut controller = AgentInteractionController::open(
            "session:commit-boundary",
            CountingHost::default(),
            persistence,
        )
        .expect("controller opens before the competing Started write");
        let result = controller.execute("occurrence:commit-boundary", boundary_request());
        assert!(matches!(result, Err(AgentError::RecoveryRequired(_))));
        let occurrence = controller
            .occurrence("occurrence:commit-boundary")
            .unwrap()
            .unwrap();
        assert_eq!(occurrence.state, AgentHostOccurrenceState::Started);
        let (host, mut persistence) = controller.into_parts();
        assert_eq!(host.binding_calls, 1);
        assert_eq!(host.tool_calls, 0);

        let (command, first) = persistence.admitted.take().unwrap();
        let replay = persistence.inner.commit_agent(&command).unwrap();
        assert_eq!(replay.observed_revision, first.observed_revision);
        assert_eq!(replay.receipt, first.receipt);
        assert_eq!(replay.committed_revision, None);
        // A source/result difference exists for both invocations. It cannot
        // distinguish the fresh winner from this same-command replay.
        assert_ne!(command.source_revision, replay.observed_revision);
        let mut reopened = AgentInteractionController::resume(
            "session:commit-boundary",
            CountingHost::default(),
            persistence.inner,
        )
        .unwrap();
        assert!(matches!(
            reopened.execute("occurrence:commit-boundary", boundary_request()),
            Err(AgentError::RecoveryRequired(_))
        ));
        assert_eq!(
            reopened.occurrence("occurrence:commit-boundary").unwrap(),
            Some(occurrence)
        );
        let (reopened_host, _) = reopened.into_parts();
        assert_eq!(reopened_host.binding_calls, 0);
        assert_eq!(reopened_host.tool_calls, 0);
    }

    #[test]
    fn lost_prepared_started_and_completed_acknowledgements_never_repeat_provider_work() {
        for state in [
            AgentHostOccurrenceState::Prepared,
            AgentHostOccurrenceState::Started,
            AgentHostOccurrenceState::Completed,
        ] {
            assert_lost_acknowledgement(state);
        }
    }

    fn assert_lost_acknowledgement(state: AgentHostOccurrenceState) {
        let persistence = BoundaryPersistence::new(CommitBoundary::LostAcknowledgement(state));
        let mut observer = persistence.inner.clone();
        let mut controller = AgentInteractionController::open(
            "session:commit-boundary",
            CountingHost::default(),
            persistence,
        )
        .unwrap();
        assert!(matches!(
            controller.execute("occurrence:commit-boundary", boundary_request()),
            Err(AgentError::CommitOutcomeUnknown { .. })
        ));
        let query = AgentOccurrenceQuery {
            session_id: "session:commit-boundary".to_owned(),
            occurrence_id: "occurrence:commit-boundary".to_owned(),
            expected_revision: None,
        };
        let before = observer.read_agent_occurrence(&query).unwrap();
        let occurrence = &before.current.as_ref().unwrap().occurrence;
        assert_eq!(occurrence.state, state);

        let retry = controller.execute("occurrence:commit-boundary", boundary_request());
        if state == AgentHostOccurrenceState::Completed {
            assert_eq!(retry.unwrap(), occurrence.response.clone().unwrap());
        } else if state == AgentHostOccurrenceState::Prepared {
            assert!(matches!(
                retry,
                Err(AgentError::Persistence { code, .. }) if code == "ephemeral_agent_revision_conflict"
            ));
        } else {
            assert!(matches!(retry, Err(AgentError::RecoveryRequired(_))));
        }
        let (host, mut persistence) = controller.into_parts();
        assert_eq!(host.binding_calls, 1);
        assert_eq!(
            host.tool_calls,
            usize::from(state == AgentHostOccurrenceState::Completed)
        );
        let (command, admitted) = persistence.admitted.take().unwrap();
        let replay = persistence.inner.commit_agent(&command).unwrap();
        assert_eq!(replay.receipt, admitted.receipt);
        assert_eq!(replay.observed_revision, admitted.observed_revision);
        assert_eq!(replay.committed_revision, None);

        let mut reopened = AgentInteractionController::resume(
            "session:commit-boundary",
            CountingHost::default(),
            persistence.inner,
        )
        .unwrap();
        let retry = reopened.execute("occurrence:commit-boundary", boundary_request());
        if state == AgentHostOccurrenceState::Completed {
            assert_eq!(retry.unwrap(), occurrence.response.clone().unwrap());
        } else {
            assert!(matches!(retry, Err(AgentError::RecoveryRequired(_))));
        }
        assert_eq!(observer.read_agent_occurrence(&query).unwrap(), before);
        let (reopened_host, _) = reopened.into_parts();
        assert_eq!(reopened_host.binding_calls, 0);
        assert_eq!(reopened_host.tool_calls, 0);
    }

    #[test]
    fn completed_occurrence_replays_without_dispatch_and_conflict_has_zero_side_effects() {
        let persistence = EphemeralAgentPersistence::default();
        let mut observer = persistence.clone();
        assert!(matches!(
            AgentInteractionController::resume(
                "session:occurrence-replay",
                CountingHost::default(),
                persistence.clone(),
            ),
            Err(AgentError::NotFound(_))
        ));
        let mut controller = AgentInteractionController::open(
            "session:occurrence-replay",
            CountingHost::default(),
            persistence,
        )
        .expect("controller opens a new Session over explicit persistence");
        let request = AgentHostRequest::Tool(ToolRequest {
            tool_call_id: "tool:occurrence-replay".to_owned(),
            operation: "test.read".to_owned(),
            input: json!({"path": "README.md"}),
        });

        let first = controller
            .execute("occurrence:replay", request.clone())
            .expect("first occurrence completes");
        let replay = controller
            .execute("occurrence:replay", request.clone())
            .expect("completed occurrence replays its retained response");
        assert_eq!(replay, first);
        assert!(matches!(
            AgentInteractionController::open(
                "session:occurrence-replay",
                CountingHost::default(),
                observer.clone(),
            ),
            Err(AgentError::IllegalTransition(_))
        ));

        let before_conflict = observer
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:occurrence-replay".to_owned(),
                expected_revision: None,
            })
            .expect("Session read before conflict succeeds");
        let conflict = controller
            .execute(
                "occurrence:replay",
                AgentHostRequest::Tool(ToolRequest {
                    tool_call_id: "tool:occurrence-replay".to_owned(),
                    operation: "test.read".to_owned(),
                    input: json!({"path": "different.md"}),
                }),
            )
            .expect_err("same occurrence identity cannot admit another request");
        assert!(matches!(conflict, AgentError::IllegalTransition(_)));
        let after_conflict = observer
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:occurrence-replay".to_owned(),
                expected_revision: None,
            })
            .expect("Session read after conflict succeeds");
        assert_eq!(after_conflict, before_conflict);

        let (host, _) = controller.into_parts();
        assert_eq!(host.binding_calls, 1);
        assert_eq!(host.tool_calls, 1);
    }

    #[test]
    fn open_revision_drift_fails_before_binding_or_dispatch() {
        let persistence = EphemeralAgentPersistence::default();
        let mut writer = persistence.clone();
        let mut controller = AgentInteractionController::open(
            "session:open-drift",
            CountingHost::default(),
            persistence,
        )
        .expect("controller pins one absent Session revision");
        let initial = writer
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:open-drift".to_owned(),
                expected_revision: None,
            })
            .expect("initial Session absence reads");
        let advance = AgentCommand::new(
            initial.revision,
            AgentCommandAction::SessionUpdate {
                session_id: "session:open-drift".to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:open-drift:1".to_owned(),
                    state: AgentState::Running,
                    stop_reason: None,
                },
            },
        )
        .expect("competing Session command seals");
        writer
            .commit_agent(&advance)
            .expect("competing Session command advances the head");

        let error = controller
            .execute(
                "occurrence:open-drift",
                AgentHostRequest::Tool(ToolRequest {
                    tool_call_id: "tool:open-drift".to_owned(),
                    operation: "test.read".to_owned(),
                    input: json!({}),
                }),
            )
            .expect_err("stale open admission fails before provider authority");
        assert!(matches!(error, AgentError::Persistence { .. }));
        let (host, _) = controller.into_parts();
        assert_eq!(host.binding_calls, 0);
        assert_eq!(host.tool_calls, 0);
    }

    #[test]
    fn started_host_timeout_and_cancellation_return_only_occurrence_unknown() {
        let failures = [
            AgentError::TimedOut {
                code: "host_timeout".to_owned(),
                message: "host deadline elapsed".to_owned(),
            },
            AgentError::Cancelled {
                code: "host_cancelled".to_owned(),
                message: "host invocation was cancelled".to_owned(),
            },
        ];
        for (index, failure) in failures.into_iter().enumerate() {
            let failure_message = failure.to_string();
            let session_id = format!("session:host-unknown:{index}");
            let occurrence_id = format!("occurrence:host-unknown:{index}");
            let persistence = EphemeralAgentPersistence::default();
            let observer = persistence.clone();
            let mut controller = AgentInteractionController::open(
                session_id.clone(),
                CountingHost {
                    tool_error: Some(failure.clone()),
                    ..CountingHost::default()
                },
                persistence,
            )
            .expect("host failure controller opens");
            let result = controller
                .execute(
                    occurrence_id.clone(),
                    AgentHostRequest::Tool(ToolRequest {
                        tool_call_id: format!("tool:host-unknown:{index}"),
                        operation: "test.write".to_owned(),
                        input: json!({}),
                    }),
                )
                .expect_err("a Started host failure has an unknown world outcome");
            assert_eq!(
                result,
                AgentError::HostOutcomeUnknown {
                    occurrence_id: occurrence_id.clone(),
                }
            );
            let current = controller
                .occurrence(&occurrence_id)
                .expect("unknown occurrence reads")
                .expect("unknown occurrence remains durable");
            assert_eq!(current.state, AgentHostOccurrenceState::Unknown);
            assert_eq!(current.failure.as_deref(), Some(failure_message.as_str()));

            let mut reopened =
                AgentInteractionController::resume(session_id, CountingHost::default(), observer)
                    .expect("unknown occurrence Session reopens");
            assert_eq!(
                reopened
                    .occurrence(&occurrence_id)
                    .expect("reopened occurrence reads"),
                Some(current)
            );
        }
    }

    #[test]
    fn context_host_cannot_admit_an_unread_cross_page_reference() {
        let persistence = EphemeralAgentPersistence::default();
        let mut writer = persistence.clone();
        let mut revision = writer
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:context-origin".to_owned(),
                expected_revision: None,
            })
            .expect("initial Session absence reads")
            .revision;
        for index in 0..2_u64 {
            let command = AgentCommand::new(
                revision,
                AgentCommandAction::SessionUpdate {
                    session_id: "session:context-origin".to_owned(),
                    update: AgentUpdate::Message {
                        update_id: format!("update:context-origin:{index}"),
                        message: cymule_profile_protocol::agent::AgentMessage {
                            message_id: format!("message:context-origin:{index}"),
                            role: cymule_profile_protocol::agent::MessageRole::Agent,
                            content: vec![ContentBlock::Text {
                                text: format!("context {index}"),
                            }],
                        },
                    },
                },
            )
            .expect("context message command seals");
            revision = writer
                .commit_agent(&command)
                .expect("context message command commits")
                .observed_revision;
        }
        let session = writer
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:context-origin".to_owned(),
                expected_revision: Some(revision),
            })
            .expect("seeded Session reads")
            .current
            .expect("seeded Session exists");
        let host = CountingHost {
            forge_context_ordinal: true,
            ..CountingHost::default()
        };
        let mut controller =
            AgentInteractionController::resume(session.session_id.clone(), host, persistence)
                .expect("controller resumes the seeded Session");
        let error = controller
            .execute(
                "occurrence:context-origin",
                AgentHostRequest::Context(ContextRequest {
                    session_id: session.session_id,
                    source_message_head: session.message_head,
                    source_message_count: session.message_count,
                    budget: 1,
                    scan_limits: AgentContextScanLimits {
                        max_entries: 1,
                        max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
                    },
                }),
            )
            .expect_err("an unread older-page reference must fail before response admission");
        assert!(matches!(
            error,
            AgentError::HostOutcomeUnknown { ref occurrence_id }
                if occurrence_id == "occurrence:context-origin"
        ));
        let occurrence = controller
            .occurrence("occurrence:context-origin")
            .expect("occurrence read succeeds")
            .expect("started occurrence remains durable");
        assert_eq!(occurrence.state, AgentHostOccurrenceState::Unknown);
        assert!(occurrence.response.is_none());
        let (host, _) = controller.into_parts();
        assert_eq!(host.binding_calls, 1);
        assert_eq!(host.context_calls, 1);
        assert_eq!(host.tool_calls, 0);
    }

    #[test]
    fn wrong_retained_context_head_cannot_produce_a_selected_snapshot() {
        let persistence = EphemeralAgentPersistence::default();
        let mut writer = persistence.clone();
        let mut revision = writer
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:context-wrong-prefix".to_owned(),
                expected_revision: None,
            })
            .expect("initial Context Session absence reads")
            .revision;
        let mut retained = None;
        for index in 0..3_u64 {
            let command = AgentCommand::new(
                revision,
                AgentCommandAction::SessionUpdate {
                    session_id: "session:context-wrong-prefix".to_owned(),
                    update: AgentUpdate::Message {
                        update_id: format!("update:context-wrong-prefix:{index}"),
                        message: cymule_profile_protocol::agent::AgentMessage {
                            message_id: format!("message:context-wrong-prefix:{index}"),
                            role: cymule_profile_protocol::agent::MessageRole::Agent,
                            content: vec![ContentBlock::Text {
                                text: format!("context {index}"),
                            }],
                        },
                    },
                },
            )
            .expect("Context message command seals");
            revision = writer
                .commit_agent(&command)
                .expect("Context message command commits")
                .observed_revision;
            if index == 1 {
                retained = writer
                    .read_agent_session(&AgentSessionQuery {
                        session_id: "session:context-wrong-prefix".to_owned(),
                        expected_revision: Some(revision.clone()),
                    })
                    .expect("retained Context source reads")
                    .current;
            }
        }
        let retained = retained.expect("two-message Context source exists before append");
        let mut host = CountingHost::default();
        let mut persistence = persistence;
        let error = dispatch(
            &mut host,
            &mut persistence,
            AgentHostRequest::Context(ContextRequest {
                session_id: retained.session_id,
                source_message_head: Some("sha256:".to_owned() + &"f".repeat(64)),
                source_message_count: retained.message_count,
                budget: 1,
                scan_limits: AgentContextScanLimits {
                    max_entries: 1,
                    max_canonical_bytes: MAX_AGENT_CONTEXT_SCAN_BYTES,
                },
            }),
        )
        .expect_err("wrong retained head fails during the first exact page read");
        assert!(matches!(
            error,
            AgentError::Persistence { ref code, .. }
                if code == "agent_message_source_stale"
        ));
        assert_eq!(host.context_calls, 1);
        assert_eq!(host.context_acceptances, 0);
    }
}
