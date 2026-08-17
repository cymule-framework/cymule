//! End-to-end agent interaction and reducer tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_agent::{
    AgentError, AgentHost, AgentHostOccurrenceState, AgentHostRequest, AgentInputController,
    AgentJournal, AgentMessage, AgentOccurrenceStore, AgentResult, AgentSession, AgentState,
    AgentTurnDriver, AgentUpdate, ContentBlock, ContextRequest, ContextSnapshot,
    ElicitationRequest, ElicitationResponse, MemoryAgentJournal, MessageRole, ModelRequest,
    ModelResponse, PermissionDecision, PermissionRequest, PermissionResponse, SessionStopReason,
    ToolCall, ToolCallStatus, ToolRequest, ToolResponse, Usage, WorkspaceChange, WorkspaceReceipt,
};
use cymule_core::{ArtifactRef, Machine, ROOT_SCOPE_ID, canonical_digest};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, FrameState, MemoryStore, WaitState,
};
use serde_json::json;

#[derive(Default)]
struct FakeHost {
    model_rounds: usize,
    tool_calls: usize,
}

impl AgentHost for FakeHost {
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<String> {
        let kind = match request {
            AgentHostRequest::Context(_) => "context",
            AgentHostRequest::Model(_) => "model",
            AgentHostRequest::Permission(_) => "permission",
            AgentHostRequest::Tool(_) => "tool",
            AgentHostRequest::Elicitation(_) => "elicitation",
            AgentHostRequest::Workspace(_) => "workspace",
        };
        Ok(format!("binding:{kind}/1"))
    }

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
    ) -> AgentResult<PermissionResponse> {
        Ok(PermissionResponse {
            decision: PermissionDecision::AllowOnce,
            occurrence_binding: "binding:permission/1".to_owned(),
        })
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

    fn elicit(&mut self, request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Ok(ElicitationResponse {
            request_id: request.request_id,
            accepted: true,
            value: Some(json!({"answer": "yes"})),
            occurrence_binding: "binding:elicitation/1".to_owned(),
        })
    }

    fn apply_workspace(&mut self, change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        Ok(WorkspaceReceipt {
            change_id: change.change_id,
            committed: change.commit,
            evidence: change.overlay,
            occurrence_binding: "binding:workspace/1".to_owned(),
        })
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

fn agent_continuation(run_id: &str) -> Continuation {
    Continuation {
        run_id: run_id.to_owned(),
        plan_id: "plan:agent-test".to_owned(),
        binding_context: "binding:agent-test/1".to_owned(),
        frames: vec![FrameState {
            invocation_id: "agent-turn".to_owned(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: None,
        wait_set: BTreeSet::new(),
        scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Ready,
    }
}

#[test]
fn frozen_agent_occurrence_fixture_matches_the_rust_contract() {
    let occurrence: cymule_agent::AgentHostOccurrence = serde_json::from_str(include_str!(
        "../../../tests/fixtures/agent-occurrence.json"
    ))
    .expect("fixture deserializes");
    assert_eq!(
        occurrence.request_digest,
        canonical_digest(&occurrence.request).expect("request hashes")
    );
    occurrence
        .validate()
        .expect("fixture is semantically valid");
    assert_eq!(occurrence.state, AgentHostOccurrenceState::Completed);
}

#[test]
fn host_occurrence_rejects_mismatched_response_identity() {
    let prepared = cymule_agent::AgentHostOccurrence::prepare(
        "occurrence:tool:mismatch",
        "session:mismatch",
        AgentHostRequest::Tool(ToolRequest {
            tool_call_id: "tool:expected".to_owned(),
            operation: "workspace.read".to_owned(),
            input: json!({}),
        }),
        "binding:tool/1",
    )
    .expect("occurrence prepares");
    let started = prepared.start().expect("occurrence starts");
    assert!(matches!(
        started.complete(cymule_agent::AgentHostResponse::Tool(ToolResponse {
            tool_call_id: "tool:different".to_owned(),
            content: Vec::new(),
            occurrence_binding: "binding:tool/1".to_owned(),
        })),
        Err(AgentError::Validation(_))
    ));
    assert!(matches!(
        started.complete(cymule_agent::AgentHostResponse::Tool(ToolResponse {
            tool_call_id: "tool:expected".to_owned(),
            content: Vec::new(),
            occurrence_binding: "binding:tool/different".to_owned(),
        })),
        Err(AgentError::Validation(_))
    ));
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

#[test]
fn durable_turn_reopens_to_the_same_projection() {
    let journal = MemoryAgentJournal::default();
    let mut driver =
        AgentTurnDriver::resume("session:durable", FakeHost::default(), journal.clone())
            .expect("empty journal opens");
    driver
        .run_turn(
            message(
                "message:user:durable",
                MessageRole::User,
                "Inspect the README",
            ),
            &["workspace.read".to_owned()],
            100,
        )
        .expect("durable turn completes");
    let expected = driver.session().clone();
    drop(driver);

    let mut inspection = journal.clone();
    let occurrences = inspection
        .load_occurrences("session:durable")
        .expect("occurrences replay");
    assert_eq!(occurrences.len(), 6);
    assert!(
        occurrences
            .iter()
            .all(|occurrence| occurrence.state == AgentHostOccurrenceState::Completed)
    );
    assert!(
        occurrences
            .iter()
            .all(|occurrence| !occurrence.occurrence_binding.is_empty())
    );

    let reopened = AgentTurnDriver::resume("session:durable", FakeHost::default(), journal)
        .expect("journal replays");
    assert_eq!(reopened.session(), &expected);
    assert_eq!(reopened.session().ordered_messages().count(), 4);
    assert_eq!(
        reopened.session().tools["tool:read"].status,
        ToolCallStatus::Completed
    );
}

#[test]
fn m1_cas_journal_reopens_the_agent_projection() {
    let store = MemoryStore::new();
    let coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("store initializes");
    let mut driver = AgentTurnDriver::resume("session:m1", FakeHost::default(), coordinator)
        .expect("M1 journal opens");
    driver
        .run_turn(
            message("message:user:m1", MessageRole::User, "Inspect the README"),
            &["workspace.read".to_owned()],
            100,
        )
        .expect("M1-backed turn completes");
    let expected = driver.session().clone();
    drop(driver);

    let coordinator = DurableCoordinator::open(store).expect("M1 store reopens");
    let reopened = AgentTurnDriver::resume("session:m1", FakeHost::default(), coordinator)
        .expect("M1 journal replays");
    assert_eq!(reopened.session(), &expected);
}

#[test]
fn durable_input_wait_suspends_and_resumes_atomically_across_reopen() {
    let mut machine = Machine::new();
    let result = machine.put_artifact("agent/input", br#"{"answer":"yes"}"#.to_vec());
    let second_result = machine.put_artifact("agent/input", br#"{"details":"ready"}"#.to_vec());
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(agent_continuation("run:agent-input"))
        .expect("continuation persists");

    let request = ElicitationRequest {
        request_id: "elicitation:approval".to_owned(),
        schema: json!({"type": "object", "required": ["answer"]}),
        prompt: vec![ContentBlock::Text {
            text: "Continue?".to_owned(),
        }],
    };
    let suspended = AgentInputController::suspend(
        &mut coordinator,
        "session:agent-input",
        "run:agent-input",
        request.clone(),
    )
    .expect("input wait suspends");
    assert_eq!(suspended.session.state, AgentState::RequiresAction);
    assert!(
        suspended.session.elicitations["elicitation:approval"]
            .response
            .is_none()
    );
    assert_eq!(
        coordinator.state().expect("state").waits[&suspended.wait_id].state,
        WaitState::Pending
    );
    assert_eq!(
        coordinator.state().expect("state").continuations["run:agent-input"].status,
        ContinuationStatus::Waiting
    );
    let retry = AgentInputController::suspend(
        &mut coordinator,
        "session:agent-input",
        "run:agent-input",
        request,
    )
    .expect("suspension retry is idempotent");
    assert_eq!(retry.wait_id, suspended.wait_id);
    let second = AgentInputController::suspend(
        &mut coordinator,
        "session:agent-input",
        "run:agent-input",
        ElicitationRequest {
            request_id: "elicitation:details".to_owned(),
            schema: json!({"type": "object", "required": ["details"]}),
            prompt: vec![ContentBlock::Text {
                text: "Provide details".to_owned(),
            }],
        },
    )
    .expect("second input wait suspends");
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store.clone()).expect("store reopens");
    let response = ElicitationResponse {
        request_id: "elicitation:approval".to_owned(),
        accepted: true,
        value: Some(json!({"answer": "yes"})),
        occurrence_binding: "binding:human-input/1".to_owned(),
    };
    let completed = AgentInputController::complete(
        &mut reopened,
        "session:agent-input",
        &suspended.wait_id,
        result.clone(),
        response.clone(),
    )
    .expect("input completion resumes");
    assert_eq!(completed.session.state, AgentState::RequiresAction);
    assert_eq!(
        completed.session.elicitations["elicitation:approval"].response,
        Some(response.clone())
    );
    assert_eq!(
        reopened.state().expect("state").waits[&suspended.wait_id].state,
        WaitState::Completed
    );
    assert_eq!(
        reopened.state().expect("state").continuations["run:agent-input"].status,
        ContinuationStatus::Waiting
    );
    let second_response = ElicitationResponse {
        request_id: "elicitation:details".to_owned(),
        accepted: true,
        value: Some(json!({"details": "ready"})),
        occurrence_binding: "binding:human-input/1".to_owned(),
    };
    let all_completed = AgentInputController::complete(
        &mut reopened,
        "session:agent-input",
        &second.wait_id,
        second_result.clone(),
        second_response.clone(),
    )
    .expect("last input completion resumes");
    assert_eq!(all_completed.session.state, AgentState::Running);
    assert_eq!(
        reopened.state().expect("state").continuations["run:agent-input"].status,
        ContinuationStatus::Ready
    );
    AgentInputController::complete(
        &mut reopened,
        "session:agent-input",
        &second.wait_id,
        second_result,
        second_response,
    )
    .expect("completion retry is idempotent");
    let revision_before_old_retry = reopened.revision().expect("revision exists").to_owned();
    let old_retry = AgentInputController::complete(
        &mut reopened,
        "session:agent-input",
        &suspended.wait_id,
        result,
        response,
    )
    .expect("older completion retry is read-only and idempotent");
    assert_eq!(old_retry.revision, revision_before_old_retry);
    drop(reopened);

    let coordinator = DurableCoordinator::open(store).expect("store reopens again");
    let driver = AgentTurnDriver::resume("session:agent-input", FakeHost::default(), coordinator)
        .expect("completed input Session replays");
    assert_eq!(driver.session().state, AgentState::Running);
}

#[test]
fn stale_input_checkpoint_writes_neither_wait_nor_agent_update() {
    let machine = Machine::new();
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    current
        .put_continuation(agent_continuation("run:stale-input"))
        .expect("continuation persists");
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    current
        .acquire_lease("test:advance", "worker", 0, 10)
        .expect("current revision advances");

    assert!(matches!(
        AgentInputController::suspend(
            &mut stale,
            "session:stale-input",
            "run:stale-input",
            ElicitationRequest {
                request_id: "elicitation:stale".to_owned(),
                schema: json!({"type": "string"}),
                prompt: Vec::new(),
            },
        ),
        Err(AgentError::Persistence(_))
    ));
    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    assert!(reopened.state().expect("state").waits.is_empty());
    assert!(
        AgentJournal::load(&mut reopened, "session:stale-input")
            .expect("journal loads")
            .is_empty()
    );
}

#[test]
fn elicitation_and_workspace_calls_are_pinned_occurrences() {
    let journal = MemoryAgentJournal::default();
    let mut driver =
        AgentTurnDriver::resume("session:surfaces", FakeHost::default(), journal.clone())
            .expect("journal opens");
    let response = driver
        .elicit(ElicitationRequest {
            request_id: "elicitation:1".to_owned(),
            schema: json!({"type": "object"}),
            prompt: vec![ContentBlock::Text {
                text: "Continue?".to_owned(),
            }],
        })
        .expect("elicitation completes");
    assert!(response.accepted);
    let receipt = driver
        .apply_workspace(WorkspaceChange {
            change_id: "workspace:1".to_owned(),
            overlay: ArtifactRef {
                artifact_id: "sha256:overlay".to_owned(),
                kind: "workspace/overlay".to_owned(),
            },
            commit: true,
        })
        .expect("workspace change completes");
    assert!(receipt.committed);
    drop(driver);

    let mut inspection = journal;
    let occurrences = inspection
        .load_occurrences("session:surfaces")
        .expect("occurrences replay");
    assert_eq!(occurrences.len(), 2);
    assert!(
        occurrences
            .iter()
            .all(|occurrence| occurrence.state == AgentHostOccurrenceState::Completed)
    );
    assert!(
        occurrences
            .iter()
            .any(|occurrence| matches!(&occurrence.request, AgentHostRequest::Elicitation(_)))
    );
    assert!(
        occurrences
            .iter()
            .any(|occurrence| matches!(&occurrence.request, AgentHostRequest::Workspace(_)))
    );
}

#[derive(Default)]
struct FailingToolHost;

impl AgentHost for FailingToolHost {
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<String> {
        let kind = match request {
            AgentHostRequest::Context(_) => "context",
            AgentHostRequest::Model(_) => "model",
            AgentHostRequest::Permission(_) => "permission",
            AgentHostRequest::Tool(_) => "tool",
            AgentHostRequest::Elicitation(_) => "elicitation",
            AgentHostRequest::Workspace(_) => "workspace",
        };
        Ok(format!("binding:{kind}/1"))
    }

    fn select_context(&mut self, _request: ContextRequest) -> AgentResult<ContextSnapshot> {
        Ok(ContextSnapshot {
            snapshot_id: "context:crash".to_owned(),
            content: Vec::new(),
            occurrence_binding: "binding:context/1".to_owned(),
        })
    }

    fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
        Ok(ModelResponse {
            message: message("message:agent:crash", MessageRole::Agent, "Calling a tool"),
            tool_requests: vec![ToolRequest {
                tool_call_id: "tool:crash".to_owned(),
                operation: "workspace.read".to_owned(),
                input: json!({"path": "README.md"}),
            }],
            occurrence_binding: "binding:model/1".to_owned(),
            usage: usage(10),
        })
    }

    fn request_permission(
        &mut self,
        _request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        Ok(PermissionResponse {
            decision: PermissionDecision::AllowOnce,
            occurrence_binding: "binding:permission/1".to_owned(),
        })
    }

    fn invoke_tool(&mut self, _request: ToolRequest) -> AgentResult<ToolResponse> {
        Err(AgentError::Host("simulated process loss".to_owned()))
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        Err(AgentError::Host("not used".to_owned()))
    }
}

#[test]
fn host_failure_preserves_the_last_accepted_interaction_state() {
    let mut journal = MemoryAgentJournal::default();
    let mut driver = AgentTurnDriver::resume("session:crash", FailingToolHost, journal.clone())
        .expect("journal opens");
    assert!(matches!(
        driver.run_turn(
            message("message:user:crash", MessageRole::User, "Read the file"),
            &["workspace.read".to_owned()],
            100,
        ),
        Err(AgentError::Host(_))
    ));
    drop(driver);

    let session = AgentSession::replay(
        "session:crash",
        journal.load("session:crash").expect("updates replay"),
    )
    .expect("partial projection rebuilds");
    assert_eq!(session.state, AgentState::Running);
    assert_eq!(
        session.tools["tool:crash"].status,
        ToolCallStatus::InProgress
    );
    assert_eq!(session.ordered_messages().count(), 2);
    let occurrences = journal
        .load_occurrences("session:crash")
        .expect("occurrences replay");
    assert!(occurrences.iter().any(|occurrence| {
        occurrence.state == AgentHostOccurrenceState::Unknown
            && occurrence.occurrence_id.starts_with("occurrence:tool:")
            && occurrence.occurrence_binding == "binding:tool/1"
    }));
    assert!(matches!(
        AgentTurnDriver::resume("session:crash", FailingToolHost, journal),
        Err(AgentError::RecoveryRequired(_))
    ));
}

#[derive(Default)]
struct RejectingJournal;

impl AgentJournal for RejectingJournal {
    fn load(&mut self, _session_id: &str) -> AgentResult<Vec<AgentUpdate>> {
        Ok(Vec::new())
    }

    fn append(&mut self, _session_id: &str, _update: &AgentUpdate) -> AgentResult<()> {
        Err(AgentError::Persistence("unavailable".to_owned()))
    }
}

impl AgentOccurrenceStore for RejectingJournal {
    fn load_occurrences(
        &mut self,
        _session_id: &str,
    ) -> AgentResult<Vec<cymule_agent::AgentHostOccurrence>> {
        Ok(Vec::new())
    }

    fn record_occurrence(
        &mut self,
        _occurrence: &cymule_agent::AgentHostOccurrence,
    ) -> AgentResult<()> {
        Err(AgentError::Persistence("unavailable".to_owned()))
    }
}

#[test]
fn failed_append_does_not_advance_the_session_projection() {
    let mut driver =
        AgentTurnDriver::resume("session:unavailable", FakeHost::default(), RejectingJournal)
            .expect("empty journal loads");
    assert!(matches!(
        driver.run_turn(
            message("message:user:unavailable", MessageRole::User, "Inspect"),
            &[],
            100,
        ),
        Err(AgentError::Persistence(_))
    ));
    assert_eq!(driver.session().state, AgentState::Idle);
    assert_eq!(driver.session().ordered_messages().count(), 0);
}

#[test]
fn journal_rejects_conflicting_reuse_without_an_extra_record() {
    let mut journal = MemoryAgentJournal::default();
    let running = AgentUpdate::State {
        update_id: "update:journal:1".to_owned(),
        state: AgentState::Running,
        stop_reason: None,
    };
    journal
        .append("session:journal", &running)
        .expect("first append succeeds");
    journal
        .append("session:journal", &running)
        .expect("same append is idempotent");
    assert!(matches!(
        journal.append(
            "session:journal",
            &AgentUpdate::State {
                update_id: "update:journal:1".to_owned(),
                state: AgentState::Idle,
                stop_reason: Some(SessionStopReason::EndTurn),
            },
        ),
        Err(AgentError::IllegalTransition(_))
    ));
    assert_eq!(
        journal.load("session:journal").expect("load succeeds"),
        vec![running]
    );
}

#[derive(Clone, Default)]
struct FailAfterToolResultJournal {
    inner: MemoryAgentJournal,
}

impl AgentJournal for FailAfterToolResultJournal {
    fn load(&mut self, session_id: &str) -> AgentResult<Vec<AgentUpdate>> {
        self.inner.load(session_id)
    }

    fn append(&mut self, session_id: &str, update: &AgentUpdate) -> AgentResult<()> {
        self.inner.append(session_id, update)
    }
}

impl AgentOccurrenceStore for FailAfterToolResultJournal {
    fn load_occurrences(
        &mut self,
        session_id: &str,
    ) -> AgentResult<Vec<cymule_agent::AgentHostOccurrence>> {
        self.inner.load_occurrences(session_id)
    }

    fn record_occurrence(
        &mut self,
        occurrence: &cymule_agent::AgentHostOccurrence,
    ) -> AgentResult<()> {
        if occurrence.state == AgentHostOccurrenceState::Completed
            && matches!(&occurrence.request, AgentHostRequest::Tool(_))
        {
            return Err(AgentError::Persistence(
                "simulated loss before tool receipt commit".to_owned(),
            ));
        }
        self.inner.record_occurrence(occurrence)
    }
}

#[test]
fn tool_result_without_a_durable_receipt_is_never_redispatched() {
    let journal = FailAfterToolResultJournal::default();
    let mut driver =
        AgentTurnDriver::resume("session:receipt-loss", FakeHost::default(), journal.clone())
            .expect("journal opens");
    assert!(matches!(
        driver.run_turn(
            message(
                "message:user:receipt-loss",
                MessageRole::User,
                "Inspect the README",
            ),
            &["workspace.read".to_owned()],
            100,
        ),
        Err(AgentError::Persistence(_))
    ));
    let (host, _, _) = driver.into_durable_parts();
    assert_eq!(host.tool_calls, 1);

    assert!(matches!(
        AgentTurnDriver::resume("session:receipt-loss", FakeHost::default(), journal),
        Err(AgentError::RecoveryRequired(_))
    ));
}
