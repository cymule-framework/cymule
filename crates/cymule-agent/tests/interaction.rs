//! End-to-end agent interaction and reducer tests.

use cymule_agent::{
    AgentError, AgentHost, AgentMessage, AgentResult, AgentSession, AgentState, AgentTurnDriver,
    AgentUpdate, ContentBlock, ContextRequest, ContextSnapshot, ElicitationRequest,
    ElicitationResponse, MessageRole, ModelRequest, ModelResponse, PermissionDecision,
    PermissionRequest, SessionStopReason, ToolCall, ToolCallStatus, ToolRequest, ToolResponse,
    Usage, WorkspaceChange, WorkspaceReceipt,
};
use serde_json::json;

#[derive(Default)]
struct FakeHost {
    model_rounds: usize,
    tool_calls: usize,
}

impl AgentHost for FakeHost {
    fn select_context(&mut self, request: ContextRequest) -> AgentResult<ContextSnapshot> {
        Ok(ContextSnapshot {
            snapshot_id: format!("context:{}", request.messages.len()),
            content: request
                .messages
                .into_iter()
                .flat_map(|message| message.content)
                .collect(),
            occurrence_binding: "binding:context/1".to_owned(),
        })
    }

    fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
        self.model_rounds += 1;
        if self.model_rounds == 1 {
            Ok(ModelResponse {
                message: message("message:agent:1", MessageRole::Agent, "Checking the file"),
                tool_requests: vec![ToolRequest {
                    tool_call_id: "tool:read".to_owned(),
                    operation: "workspace.read".to_owned(),
                    input: json!({"path": "README.md"}),
                }],
                occurrence_binding: "binding:model/1".to_owned(),
                usage: usage(10),
            })
        } else {
            Ok(ModelResponse {
                message: message("message:agent:2", MessageRole::Agent, "README is present"),
                tool_requests: Vec::new(),
                occurrence_binding: "binding:model/1".to_owned(),
                usage: usage(20),
            })
        }
    }

    fn request_permission(
        &mut self,
        _request: PermissionRequest,
    ) -> AgentResult<PermissionDecision> {
        Ok(PermissionDecision::AllowOnce)
    }

    fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse> {
        self.tool_calls += 1;
        Ok(ToolResponse {
            tool_call_id: request.tool_call_id,
            content: vec![ContentBlock::Json {
                value: json!({"exists": true}),
            }],
            occurrence_binding: "binding:tool/1".to_owned(),
        })
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        Err(AgentError::Host("not used".to_owned()))
    }
}

fn message(id: &str, role: MessageRole, text: &str) -> AgentMessage {
    AgentMessage {
        message_id: id.to_owned(),
        role,
        content: vec![ContentBlock::Text {
            text: text.to_owned(),
        }],
    }
}

fn usage(used: u64) -> Usage {
    Usage {
        used,
        capacity: 100,
        cost: None,
    }
}

#[test]
fn turn_runs_context_model_permission_tool_and_final_model() {
    let mut driver = AgentTurnDriver::new("session:1", FakeHost::default());
    let session = driver
        .run_turn(
            message("message:user:1", MessageRole::User, "Inspect the README"),
            &["workspace.read".to_owned()],
            100,
        )
        .expect("turn completes");
    assert_eq!(session.state, AgentState::Idle);
    assert_eq!(session.ordered_messages().count(), 4);
    assert_eq!(session.tools["tool:read"].status, ToolCallStatus::Completed);
    assert_eq!(session.usage.as_ref().expect("usage").used, 20);
    let (host, _) = driver.into_parts();
    assert_eq!(host.model_rounds, 2);
    assert_eq!(host.tool_calls, 1);
}

#[test]
fn update_id_reuse_and_illegal_tool_jump_fail_closed() {
    let mut session = AgentSession::new("session:2");
    let running = AgentUpdate::State {
        update_id: "update:1".to_owned(),
        state: AgentState::Running,
        stop_reason: None,
    };
    session.apply(running.clone()).expect("state applies");
    session.apply(running).expect("retry is idempotent");
    assert!(matches!(
        session.apply(AgentUpdate::State {
            update_id: "update:1".to_owned(),
            state: AgentState::Idle,
            stop_reason: Some(SessionStopReason::EndTurn),
        }),
        Err(AgentError::IllegalTransition(_))
    ));

    session
        .apply(AgentUpdate::Tool {
            update_id: "update:tool:1".to_owned(),
            tool: ToolCall {
                tool_call_id: "tool:1".to_owned(),
                operation: "test".to_owned(),
                status: ToolCallStatus::Pending,
                input: json!({}),
                output: None,
                locations: Vec::new(),
            },
        })
        .expect("pending tool applies");
    assert!(matches!(
        session.apply(AgentUpdate::Tool {
            update_id: "update:tool:2".to_owned(),
            tool: ToolCall {
                tool_call_id: "tool:1".to_owned(),
                operation: "test".to_owned(),
                status: ToolCallStatus::Completed,
                input: json!({}),
                output: None,
                locations: Vec::new(),
            },
        }),
        Err(AgentError::IllegalTransition(_))
    ));
}
