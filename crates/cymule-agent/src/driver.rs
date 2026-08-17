use crate::{
    AgentError, AgentHost, AgentHostOccurrence, AgentHostRequest, AgentHostResponse, AgentJournal,
    AgentMessage, AgentOccurrenceStore, AgentResult, AgentSession, AgentState, AgentUpdate,
    ContentBlock, ContextRequest, ContextSnapshot, ElicitationRequest, ElicitationResponse,
    MessageRole, ModelRequest, ModelResponse, NoopAgentJournal, PermissionDecision,
    PermissionRequest, PermissionResponse, SessionStopReason, ToolCall, ToolCallStatus,
    ToolRequest, ToolResponse, WorkspaceChange, WorkspaceReceipt,
};

/// Synchronous reference turn driver over one replaceable `AgentHost`.
#[must_use]
pub struct AgentTurnDriver<H, J = NoopAgentJournal> {
    host: H,
    journal: J,
    session: AgentSession,
    sequence: u64,
    occurrence_sequence: u64,
    max_model_rounds: u32,
}

impl<H: AgentHost> AgentTurnDriver<H, NoopAgentJournal> {
    /// Create a driver for one Session.
    pub fn new(session_id: impl Into<String>, host: H) -> Self {
        Self {
            host,
            journal: NoopAgentJournal,
            session: AgentSession::new(session_id),
            sequence: 0,
            occurrence_sequence: 0,
            max_model_rounds: 16,
        }
    }
}

impl<H: AgentHost, J: AgentJournal + AgentOccurrenceStore> AgentTurnDriver<H, J> {
    /// Restore a durable driver by replaying one Session journal.
    pub fn resume(session_id: impl Into<String>, host: H, mut journal: J) -> AgentResult<Self> {
        let session_id = session_id.into();
        let updates = journal.load(&session_id)?;
        let occurrences = journal.load_occurrences(&session_id)?;
        let sequence = updates
            .iter()
            .filter_map(|update| update.update_id().rsplit(':').next()?.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        let session = AgentSession::replay(session_id, updates)?;
        if let Some(unsettled) = occurrences
            .iter()
            .find(|occurrence| !occurrence.is_terminal())
        {
            return Err(AgentError::RecoveryRequired(format!(
                "host occurrence {} is {:?}; reconcile or cancel it before resuming",
                unsettled.occurrence_id, unsettled.state
            )));
        }
        if session.state == AgentState::Running && !occurrences.is_empty() {
            return Err(AgentError::RecoveryRequired(
                "foreground turn control is incomplete; consume retained occurrence responses before resuming"
                    .to_owned(),
            ));
        }
        let occurrence_sequence = occurrences
            .iter()
            .filter_map(|occurrence| {
                occurrence
                    .occurrence_id
                    .rsplit(':')
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
            .max()
            .unwrap_or(0);
        Ok(Self {
            host,
            journal,
            session,
            sequence,
            occurrence_sequence,
            max_model_rounds: 16,
        })
    }

    /// Set the bounded number of model rounds in one foreground turn.
    pub const fn with_max_model_rounds(mut self, rounds: u32) -> Self {
        self.max_model_rounds = rounds;
        self
    }

    /// Run one prompt through context, model, permission, and tool interfaces.
    pub fn run_turn(
        &mut self,
        user_message: AgentMessage,
        tools: &[String],
        context_budget: u64,
    ) -> AgentResult<&AgentSession> {
        if user_message.role != MessageRole::User {
            return Err(AgentError::Validation(
                "run_turn requires a user message".to_owned(),
            ));
        }
        let update_id = self.next_id("user-message");
        self.apply(&AgentUpdate::Message {
            update_id,
            message: user_message,
        })?;
        let update_id = self.next_id("running");
        self.apply(&AgentUpdate::State {
            update_id,
            state: AgentState::Running,
            stop_reason: None,
        })?;

        for _ in 0..self.max_model_rounds {
            let context = self.select_context(ContextRequest {
                session_id: self.session.session_id.clone(),
                messages: self.session.ordered_messages().cloned().collect(),
                budget: context_budget,
            })?;
            let response = self.invoke_model(ModelRequest {
                session_id: self.session.session_id.clone(),
                context,
                tools: tools.to_owned(),
            })?;
            let update_id = self.next_id("usage");
            self.apply(&AgentUpdate::Usage {
                update_id,
                usage: response.usage,
            })?;
            let update_id = self.next_id("agent-message");
            self.apply(&AgentUpdate::Message {
                update_id,
                message: response.message,
            })?;

            if response.tool_requests.is_empty() {
                let update_id = self.next_id("idle");
                self.apply(&AgentUpdate::State {
                    update_id,
                    state: AgentState::Idle,
                    stop_reason: Some(SessionStopReason::EndTurn),
                })?;
                return Ok(&self.session);
            }

            for request in response.tool_requests {
                self.update_tool(&request, ToolCallStatus::Pending, None)?;
                self.update_tool(&request, ToolCallStatus::AwaitingPermission, None)?;
                let permission = self.request_permission(PermissionRequest {
                    request_id: format!("permission:{}", request.tool_call_id),
                    tool: request.clone(),
                    options: vec!["allow_once".to_owned(), "deny".to_owned()],
                })?;
                if permission.decision == PermissionDecision::Deny {
                    self.update_tool(&request, ToolCallStatus::Cancelled, None)?;
                    let update_id = self.next_id("tool-denied");
                    self.apply(&AgentUpdate::Message {
                        update_id,
                        message: AgentMessage {
                            message_id: format!("message:tool:{}", request.tool_call_id),
                            role: MessageRole::Tool,
                            content: vec![ContentBlock::Text {
                                text: "permission denied".to_owned(),
                            }],
                        },
                    })?;
                    continue;
                }
                self.update_tool(&request, ToolCallStatus::InProgress, None)?;
                let tool_response = self.invoke_tool(request.clone())?;
                if tool_response.tool_call_id != request.tool_call_id {
                    return Err(AgentError::Validation(
                        "tool response identity does not match request".to_owned(),
                    ));
                }
                self.update_tool(
                    &request,
                    ToolCallStatus::Completed,
                    Some(tool_response.content.clone()),
                )?;
                let update_id = self.next_id("tool-message");
                self.apply(&AgentUpdate::Message {
                    update_id,
                    message: AgentMessage {
                        message_id: format!("message:tool:{}", request.tool_call_id),
                        role: MessageRole::Tool,
                        content: tool_response.content,
                    },
                })?;
            }
        }
        Err(AgentError::IllegalTransition(format!(
            "model exceeded {} rounds",
            self.max_model_rounds
        )))
    }

    /// Current Session projection.
    pub const fn session(&self) -> &AgentSession {
        &self.session
    }

    /// Consume the driver and return its host and Session.
    pub fn into_parts(self) -> (H, AgentSession) {
        (self.host, self.session)
    }

    /// Consume the driver and return its host, journal, and Session.
    pub fn into_durable_parts(self) -> (H, J, AgentSession) {
        (self.host, self.journal, self.session)
    }

    /// Execute and durably record a typed elicitation occurrence.
    pub fn elicit(&mut self, request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        let started = self.begin_host_call(
            "elicitation",
            AgentHostRequest::Elicitation(request.clone()),
        )?;
        match self.host.elicit(request) {
            Ok(response) => {
                self.complete_host_call(
                    &started,
                    AgentHostResponse::Elicitation(response.clone()),
                )?;
                Ok(response)
            }
            Err(error) => {
                self.unknown_host_call(&started, &error)?;
                Err(error)
            }
        }
    }

    /// Execute and durably record a workspace overlay occurrence.
    pub fn apply_workspace(&mut self, change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        let started =
            self.begin_host_call("workspace", AgentHostRequest::Workspace(change.clone()))?;
        match self.host.apply_workspace(change) {
            Ok(response) => {
                self.complete_host_call(&started, AgentHostResponse::Workspace(response.clone()))?;
                Ok(response)
            }
            Err(error) => {
                self.unknown_host_call(&started, &error)?;
                Err(error)
            }
        }
    }

    fn update_tool(
        &mut self,
        request: &crate::ToolRequest,
        status: ToolCallStatus,
        output: Option<Vec<ContentBlock>>,
    ) -> AgentResult<()> {
        let update_id = self.next_id("tool");
        self.apply(&AgentUpdate::Tool {
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

    fn apply(&mut self, update: &AgentUpdate) -> AgentResult<()> {
        let mut next = self.session.clone();
        next.apply(update.clone())?;
        self.journal.append(&self.session.session_id, update)?;
        self.session = next;
        Ok(())
    }

    fn select_context(&mut self, request: ContextRequest) -> AgentResult<ContextSnapshot> {
        let started =
            self.begin_host_call("context", AgentHostRequest::Context(request.clone()))?;
        match self.host.select_context(request) {
            Ok(response) => {
                self.complete_host_call(&started, AgentHostResponse::Context(response.clone()))?;
                Ok(response)
            }
            Err(error) => {
                self.unknown_host_call(&started, &error)?;
                Err(error)
            }
        }
    }

    fn invoke_model(&mut self, request: ModelRequest) -> AgentResult<ModelResponse> {
        let started = self.begin_host_call("model", AgentHostRequest::Model(request.clone()))?;
        match self.host.invoke_model(request) {
            Ok(response) => {
                self.complete_host_call(&started, AgentHostResponse::Model(response.clone()))?;
                Ok(response)
            }
            Err(error) => {
                self.unknown_host_call(&started, &error)?;
                Err(error)
            }
        }
    }

    fn request_permission(
        &mut self,
        request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        let started =
            self.begin_host_call("permission", AgentHostRequest::Permission(request.clone()))?;
        match self.host.request_permission(request) {
            Ok(response) => {
                self.complete_host_call(&started, AgentHostResponse::Permission(response.clone()))?;
                Ok(response)
            }
            Err(error) => {
                self.unknown_host_call(&started, &error)?;
                Err(error)
            }
        }
    }

    fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse> {
        let started = self.begin_host_call("tool", AgentHostRequest::Tool(request.clone()))?;
        match self.host.invoke_tool(request) {
            Ok(response) => {
                self.complete_host_call(&started, AgentHostResponse::Tool(response.clone()))?;
                Ok(response)
            }
            Err(error) => {
                self.unknown_host_call(&started, &error)?;
                Err(error)
            }
        }
    }

    fn begin_host_call(
        &mut self,
        kind: &str,
        request: AgentHostRequest,
    ) -> AgentResult<AgentHostOccurrence> {
        self.occurrence_sequence += 1;
        let occurrence_binding = self.host.bind_occurrence(&request)?;
        let prepared = AgentHostOccurrence::prepare(
            format!("occurrence:{kind}:{}", self.occurrence_sequence),
            self.session.session_id.clone(),
            request,
            occurrence_binding,
        )?;
        self.journal.record_occurrence(&prepared)?;
        let started = prepared.start()?;
        self.journal.record_occurrence(&started)?;
        Ok(started)
    }

    fn complete_host_call(
        &mut self,
        started: &AgentHostOccurrence,
        response: AgentHostResponse,
    ) -> AgentResult<()> {
        let completed = started.complete(response)?;
        self.journal.record_occurrence(&completed)
    }

    fn unknown_host_call(
        &mut self,
        started: &AgentHostOccurrence,
        error: &AgentError,
    ) -> AgentResult<()> {
        let unknown = started.mark_unknown(error.to_string())?;
        self.journal.record_occurrence(&unknown)
    }

    fn next_id(&mut self, kind: &str) -> String {
        self.sequence += 1;
        format!("update:{kind}:{}", self.sequence)
    }
}
