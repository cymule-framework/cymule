//! Partitioned host-kind failure, refusal, and cancellation conformance.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use cymule_agent::{
    AgentError, AgentHost, AgentHostOccurrenceState, AgentHostRequest, AgentInteractionController,
    AgentMessage, AgentOccurrenceResolution, AgentRecoveryController, AgentResult, AgentSession,
    AgentState, AgentTurnDriver, AgentUpdate, ContentBlock, ContextRequest, ContextSnapshot,
    ElicitationRequest, ElicitationResponse, MemoryAgentJournal, MessageRole, ModelRequest,
    ModelResponse, PermissionDecision, PermissionRequest, PermissionResponse, SessionStopReason,
    ToolRequest, ToolResponse, Usage, WorkspaceChange, WorkspaceReceipt,
};
use cymule_core::ArtifactRef;
use serde_json::json;

#[derive(Clone)]
struct FailingHost {
    dispatches: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
}

impl FailingHost {
    fn fail<T>(&self) -> AgentResult<T> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        Err(AgentError::Host(
            "host failed after the dispatch boundary".to_owned(),
        ))
    }
}

impl AgentHost for FailingHost {
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<String> {
        Ok(format!("binding:{:?}/1", request.kind()).to_lowercase())
    }

    fn select_context(&mut self, _request: ContextRequest) -> AgentResult<ContextSnapshot> {
        self.fail()
    }

    fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
        self.fail()
    }

    fn request_permission(
        &mut self,
        _request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        self.fail()
    }

    fn invoke_tool(&mut self, _request: ToolRequest) -> AgentResult<ToolResponse> {
        self.fail()
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        self.fail()
    }

    fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        self.fail()
    }

    fn reconcile_occurrence(
        &mut self,
        _occurrence: &cymule_agent::AgentHostOccurrence,
    ) -> AgentResult<AgentOccurrenceResolution> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        Ok(AgentOccurrenceResolution::Unknown {
            evidence: vec![ContentBlock::Text {
                text: "provider remains ambiguous".to_owned(),
            }],
        })
    }
}

fn requests() -> Vec<AgentHostRequest> {
    let tool = ToolRequest {
        tool_call_id: "tool:matrix".to_owned(),
        operation: "workspace.read".to_owned(),
        input: json!({"path": "README.md"}),
    };
    let context = ContextSnapshot {
        snapshot_id: "context:matrix".to_owned(),
        content: Vec::new(),
        occurrence_binding: "binding:context-input/1".to_owned(),
    };
    vec![
        AgentHostRequest::Context(ContextRequest {
            session_id: "session:matrix".to_owned(),
            messages: Vec::new(),
            budget: 100,
        }),
        AgentHostRequest::Model(ModelRequest {
            session_id: "session:matrix".to_owned(),
            context,
            tools: vec!["workspace.read".to_owned()],
        }),
        AgentHostRequest::Permission(PermissionRequest {
            request_id: "permission:matrix".to_owned(),
            tool: tool.clone(),
            options: vec!["allow_once".to_owned(), "deny".to_owned()],
        }),
        AgentHostRequest::Tool(tool),
        AgentHostRequest::Elicitation(ElicitationRequest {
            request_id: "input:matrix".to_owned(),
            schema: json!({"type": "string"}),
            prompt: vec![ContentBlock::Text {
                text: "Continue?".to_owned(),
            }],
        }),
        AgentHostRequest::Workspace(WorkspaceChange {
            change_id: "workspace:matrix".to_owned(),
            overlay: ArtifactRef {
                identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
                artifact_id: format!("sha256:{}", "1".repeat(64)),
                kind: "workspace/overlay".to_owned(),
            },
            commit: true,
        }),
    ]
}

#[test]
fn every_host_kind_becomes_unknown_and_never_redispatches_after_failure() {
    for (index, request) in requests().into_iter().enumerate() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let reconciliations = Arc::new(AtomicUsize::new(0));
        let journal = MemoryAgentJournal::default();
        let occurrence_id = format!("occurrence:failure:{index}");
        let host = FailingHost {
            dispatches: Arc::clone(&dispatches),
            reconciliations: Arc::clone(&reconciliations),
        };
        let mut controller =
            AgentInteractionController::resume("session:matrix", host, journal.clone())
                .expect("controller opens");
        assert!(matches!(
            controller.execute(&occurrence_id, request.clone()),
            Err(AgentError::Host(_))
        ));
        assert_eq!(
            controller
                .occurrence(&occurrence_id)
                .expect("occurrence exists")
                .state,
            AgentHostOccurrenceState::Unknown
        );
        assert!(matches!(
            controller.execute(&occurrence_id, request),
            Err(AgentError::RecoveryRequired(_))
        ));
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        let (_, mut journal) = controller.into_parts();
        let mut recovery_host = FailingHost {
            dispatches: Arc::clone(&dispatches),
            reconciliations: Arc::clone(&reconciliations),
        };
        assert!(matches!(
            AgentRecoveryController::reconcile(
                &mut recovery_host,
                &mut journal,
                "session:matrix",
                &occurrence_id,
            ),
            Err(AgentError::RecoveryRequired(_))
        ));
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(reconciliations.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn every_caller_stop_reason_is_a_durable_session_projection() {
    for (index, reason) in [
        SessionStopReason::EndTurn,
        SessionStopReason::Cancelled,
        SessionStopReason::Refusal,
        SessionStopReason::Error,
    ]
    .into_iter()
    .enumerate()
    {
        let mut session = AgentSession::new(format!("session:stop:{index}"));
        session
            .apply(AgentUpdate::State {
                update_id: format!("update:running:{index}"),
                state: AgentState::Running,
                stop_reason: None,
            })
            .expect("Session starts");
        session
            .apply(AgentUpdate::State {
                update_id: format!("update:stopped:{index}"),
                state: AgentState::Idle,
                stop_reason: Some(reason),
            })
            .expect("caller-selected stop reason records");
        assert_eq!(session.state, AgentState::Idle);
    }
}

struct DenyingHost {
    model_round: usize,
    tool_dispatches: Arc<AtomicUsize>,
}

impl AgentHost for DenyingHost {
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<String> {
        Ok(format!("binding:{:?}/1", request.kind()).to_lowercase())
    }

    fn select_context(&mut self, _request: ContextRequest) -> AgentResult<ContextSnapshot> {
        Ok(ContextSnapshot {
            snapshot_id: "context:denial".to_owned(),
            content: Vec::new(),
            occurrence_binding: "binding:context/1".to_owned(),
        })
    }

    fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
        self.model_round += 1;
        Ok(ModelResponse {
            message: AgentMessage {
                message_id: format!("message:agent:{}", self.model_round),
                role: MessageRole::Agent,
                content: vec![ContentBlock::Text {
                    text: "bounded response".to_owned(),
                }],
            },
            tool_requests: (self.model_round == 1)
                .then(|| ToolRequest {
                    tool_call_id: "tool:denied".to_owned(),
                    operation: "workspace.write".to_owned(),
                    input: json!({}),
                })
                .into_iter()
                .collect(),
            occurrence_binding: "binding:model/1".to_owned(),
            usage: Usage {
                used: self.model_round as u64,
                capacity: 100,
                cost: None,
            },
        })
    }

    fn request_permission(
        &mut self,
        _request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        Ok(PermissionResponse {
            decision: PermissionDecision::Deny,
            occurrence_binding: "binding:permission/1".to_owned(),
        })
    }

    fn invoke_tool(&mut self, _request: ToolRequest) -> AgentResult<ToolResponse> {
        self.tool_dispatches.fetch_add(1, Ordering::SeqCst);
        Err(AgentError::Host("denied tool was dispatched".to_owned()))
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        Err(AgentError::Host("not used".to_owned()))
    }
}

#[test]
fn permission_refusal_is_terminal_for_the_tool_and_never_dispatches_it() {
    let tool_dispatches = Arc::new(AtomicUsize::new(0));
    let host = DenyingHost {
        model_round: 0,
        tool_dispatches: Arc::clone(&tool_dispatches),
    };
    let mut driver = AgentTurnDriver::resume("session:denial", host, MemoryAgentJournal::default())
        .expect("driver opens");
    let session = driver
        .run_turn(
            AgentMessage {
                message_id: "message:user:denial".to_owned(),
                role: MessageRole::User,
                content: vec![ContentBlock::Text {
                    text: "change the workspace".to_owned(),
                }],
            },
            &["workspace.write".to_owned()],
            100,
        )
        .expect("caller loop continues after refusal");
    assert_eq!(
        session.tools["tool:denied"].status,
        cymule_agent::ToolCallStatus::Cancelled
    );
    assert_eq!(tool_dispatches.load(Ordering::SeqCst), 0);
}
