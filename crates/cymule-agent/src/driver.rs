use crate::{
    AgentError, AgentHost, AgentMessage, AgentResult, AgentSession, AgentState, AgentUpdate,
    ContentBlock, ContextRequest, MessageRole, ModelRequest, PermissionDecision, PermissionRequest,
    SessionStopReason, ToolCall, ToolCallStatus,
};

/// Synchronous reference turn driver over one replaceable `AgentHost`.
#[must_use]
pub struct AgentTurnDriver<H> {
    host: H,
    session: AgentSession,
    sequence: u64,
    max_model_rounds: u32,
}

impl<H: AgentHost> AgentTurnDriver<H> {
    /// Create a driver for one Session.
    pub fn new(session_id: impl Into<String>, host: H) -> Self {
        Self {
            host,
            session: AgentSession::new(session_id),
            sequence: 0,
            max_model_rounds: 16,
        }
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
        self.apply(AgentUpdate::Message {
            update_id,
            message: user_message,
        })?;
        let update_id = self.next_id("running");
        self.apply(AgentUpdate::State {
            update_id,
            state: AgentState::Running,
            stop_reason: None,
        })?;

        for _ in 0..self.max_model_rounds {
            let context = self.host.select_context(ContextRequest {
                session_id: self.session.session_id.clone(),
                messages: self.session.ordered_messages().cloned().collect(),
                budget: context_budget,
            })?;
            let response = self.host.invoke_model(ModelRequest {
                session_id: self.session.session_id.clone(),
                context,
                tools: tools.to_owned(),
            })?;
            let update_id = self.next_id("usage");
            self.apply(AgentUpdate::Usage {
                update_id,
                usage: response.usage,
            })?;
            let update_id = self.next_id("agent-message");
            self.apply(AgentUpdate::Message {
                update_id,
                message: response.message,
            })?;

            if response.tool_requests.is_empty() {
                let update_id = self.next_id("idle");
                self.apply(AgentUpdate::State {
                    update_id,
                    state: AgentState::Idle,
                    stop_reason: Some(SessionStopReason::EndTurn),
                })?;
                return Ok(&self.session);
            }

            for request in response.tool_requests {
                self.update_tool(&request, ToolCallStatus::Pending, None)?;
                self.update_tool(&request, ToolCallStatus::AwaitingPermission, None)?;
                let decision = self.host.request_permission(PermissionRequest {
                    request_id: format!("permission:{}", request.tool_call_id),
                    tool: request.clone(),
                    options: vec!["allow_once".to_owned(), "deny".to_owned()],
                })?;
                if decision == PermissionDecision::Deny {
                    self.update_tool(&request, ToolCallStatus::Cancelled, None)?;
                    let update_id = self.next_id("tool-denied");
                    self.apply(AgentUpdate::Message {
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
                let tool_response = self.host.invoke_tool(request.clone())?;
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
                self.apply(AgentUpdate::Message {
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

    fn update_tool(
        &mut self,
        request: &crate::ToolRequest,
        status: ToolCallStatus,
        output: Option<Vec<ContentBlock>>,
    ) -> AgentResult<()> {
        let update_id = self.next_id("tool");
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
        self.session.apply(update)
    }

    fn next_id(&mut self, kind: &str) -> String {
        self.sequence += 1;
        format!("update:{kind}:{}", self.sequence)
    }
}
