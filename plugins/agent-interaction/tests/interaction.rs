//! End-to-end agent interaction and reducer tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use cymule_agent::{
    AgentError, AgentHost, AgentHostOccurrenceState, AgentHostRequest, AgentInputController,
    AgentInteractionController, AgentJournal, AgentMessage, AgentOccurrenceResolution,
    AgentOccurrenceStore, AgentRecoveryController, AgentResult, AgentSession, AgentState,
    AgentTurnDriver, AgentUpdate, ContentBlock, ContextRequest, ContextSnapshot,
    ElicitationRequest, ElicitationResponse, MemoryAgentJournal, MessageRole, ModelRequest,
    ModelResponse, PermissionDecision, PermissionRequest, PermissionResponse, SessionStopReason,
    ToolCall, ToolCallStatus, ToolRequest, ToolResponse, Usage, WorkspaceChange, WorkspaceReceipt,
    WorkspaceScopeController, WorkspaceScopeRequest,
};
use cymule_core::{
    ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, Definition, DispatchPolicy,
    EffectContract, EffectProfile, Expression, Machine, MutationKind, PlanCandidate, ROOT_SCOPE_ID,
    ReconciliationMode, Region, ScopeStatus, WorldOutcome, canonical_digest,
};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, FrameState, JournalRecord, MemoryStore,
    OutboxState, WaitState,
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

struct WorkspaceTestHost {
    applies: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
    fail_apply: bool,
    reconcile_not_applied: bool,
}

impl WorkspaceTestHost {
    fn new(fail_apply: bool) -> Self {
        Self {
            applies: Arc::new(AtomicUsize::new(0)),
            reconciliations: Arc::new(AtomicUsize::new(0)),
            fail_apply,
            reconcile_not_applied: false,
        }
    }

    fn with_not_applied_reconciliation(mut self) -> Self {
        self.reconcile_not_applied = true;
        self
    }
}

impl AgentHost for WorkspaceTestHost {
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<String> {
        if !matches!(request, AgentHostRequest::Workspace(_)) {
            return Err(AgentError::Host(
                "workspace host received another request".to_owned(),
            ));
        }
        Ok("binding:workspace-scope/1".to_owned())
    }

    fn select_context(&mut self, _request: ContextRequest) -> AgentResult<ContextSnapshot> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn request_permission(
        &mut self,
        _request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn invoke_tool(&mut self, _request: ToolRequest) -> AgentResult<ToolResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn apply_workspace(&mut self, change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        if self.fail_apply {
            return Err(AgentError::Host(
                "simulated workspace failure after dispatch".to_owned(),
            ));
        }
        Ok(WorkspaceReceipt {
            change_id: change.change_id,
            committed: change.commit,
            evidence: change.overlay,
            occurrence_binding: "binding:workspace-scope/1".to_owned(),
        })
    }

    fn reconcile_occurrence(
        &mut self,
        occurrence: &cymule_agent::AgentHostOccurrence,
    ) -> AgentResult<AgentOccurrenceResolution> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        let AgentHostRequest::Workspace(change) = &occurrence.request else {
            return Err(AgentError::Host(
                "workspace reconciliation received another request".to_owned(),
            ));
        };
        if self.reconcile_not_applied {
            return Ok(AgentOccurrenceResolution::NotApplied {
                evidence: vec![ContentBlock::Text {
                    text: "provider proved the workspace change did not apply".to_owned(),
                }],
            });
        }
        Ok(AgentOccurrenceResolution::Completed {
            response: cymule_agent::AgentHostResponse::Workspace(WorkspaceReceipt {
                change_id: change.change_id.clone(),
                committed: change.commit,
                evidence: change.overlay.clone(),
                occurrence_binding: occurrence.occurrence_binding.clone(),
            }),
        })
    }
}

struct RacingWorkspaceHost {
    store: MemoryStore,
    applies: Arc<AtomicUsize>,
}

impl AgentHost for RacingWorkspaceHost {
    fn bind_occurrence(&mut self, _request: &AgentHostRequest) -> AgentResult<String> {
        Ok("binding:workspace-scope/1".to_owned())
    }

    fn select_context(&mut self, _request: ContextRequest) -> AgentResult<ContextSnapshot> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn request_permission(
        &mut self,
        _request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn invoke_tool(&mut self, _request: ToolRequest) -> AgentResult<ToolResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn apply_workspace(&mut self, change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        let mut competing = DurableCoordinator::open(self.store.clone())
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        competing
            .append_journal_record(
                "fault:workspace-race",
                JournalRecord::new(
                    "fault:advance-revision",
                    "fault.workspace-race/1",
                    json!({"advanced": true}),
                )
                .map_err(|error| AgentError::Persistence(error.to_string()))?,
            )
            .map_err(|error| AgentError::Persistence(error.to_string()))?;
        Ok(WorkspaceReceipt {
            change_id: change.change_id,
            committed: change.commit,
            evidence: change.overlay,
            occurrence_binding: "binding:workspace-scope/1".to_owned(),
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
            definition_id: "agent-turn".to_owned(),
            invocation_id: "agent-turn".to_owned(),
            input: cymule_core::artifact_ref("test/input", b"agent test input")
                .expect("test input reference derives"),
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

fn install_agent_input(machine: &mut Machine) {
    machine
        .put_artifact("test/input", b"agent test input".to_vec())
        .expect("test input stores");
}

fn workspace_machine(run_id: &str) -> (Machine, String, ArtifactRef) {
    let mut machine = Machine::new();
    install_agent_input(&mut machine);
    let plan = cymule_core::seal_plan(PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "agent_workspace_scope".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: vec![EffectContract {
            id: "workspace.commit".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Mutating,
                dispatch: DispatchPolicy::OnScopeCommit,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
            requirements: BTreeMap::new(),
        }],
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: Vec::new(),
                result: Expression::Literal { value: json!(null) },
            },
        }],
        metadata: BTreeMap::new(),
    })
    .expect("workspace Plan seals");
    machine.insert_plan(plan.clone()).expect("Plan inserts");
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: format!("command:start:{run_id}"),
            actor: "actor:test".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: "binding:workspace-runtime/1".to_owned(),
            },
        })
        .expect("workspace Run starts");
    let overlay = machine
        .put_artifact("workspace/overlay", b"prepared overlay".to_vec())
        .expect("Artifact stores");
    (machine, plan.plan_id, overlay)
}

fn workspace_request(
    run_id: &str,
    session_id: &str,
    occurrence_id: &str,
    overlay: ArtifactRef,
) -> WorkspaceScopeRequest {
    WorkspaceScopeRequest {
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        scope_id: ROOT_SCOPE_ID.to_owned(),
        occurrence_id: occurrence_id.to_owned(),
        change_id: format!("change:{run_id}"),
        overlay,
        operation: "workspace.commit".to_owned(),
        invocation_id: "main".to_owned(),
        site_id: "workspace.finalize".to_owned(),
        occurrence_key: "primary".to_owned(),
    }
}

fn workspace_coordinator(
    store: MemoryStore,
    run_id: &str,
) -> (DurableCoordinator<MemoryStore>, WorkspaceScopeRequest) {
    let (machine, plan_id, overlay) = workspace_machine(run_id);
    let mut coordinator = DurableCoordinator::open(store)
        .expect("workspace store opens")
        .initialize(&machine)
        .expect("workspace store initializes");
    let mut continuation = agent_continuation(run_id);
    continuation.plan_id = plan_id;
    "binding:workspace-runtime/1".clone_into(&mut continuation.binding_context);
    coordinator
        .put_continuation(continuation)
        .expect("workspace Continuation persists");
    let request = workspace_request(
        run_id,
        &format!("session:{run_id}"),
        &format!("occurrence:{run_id}"),
        overlay,
    );
    (coordinator, request)
}

#[test]
fn frozen_agent_occurrence_fixture_matches_the_rust_contract() {
    let occurrence: cymule_agent::AgentHostOccurrence =
        serde_json::from_str(include_str!("fixtures/agent-occurrence.json"))
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
fn interaction_controller_replays_a_retained_response_through_m1_without_redispatch() {
    let store = MemoryStore::new();
    let coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("store initializes");
    let request = AgentHostRequest::Tool(ToolRequest {
        tool_call_id: "tool:interaction".to_owned(),
        operation: "workspace.read".to_owned(),
        input: json!({"path": "README.md"}),
    });
    let mut controller =
        AgentInteractionController::resume("session:interaction", FakeHost::default(), coordinator)
            .expect("interaction controller opens");
    let first = controller
        .execute("occurrence:caller-owned:tool:1", request.clone())
        .expect("first interaction completes");
    let (first_host, coordinator) = controller.into_parts();
    assert_eq!(first_host.tool_calls, 1);
    drop(coordinator);

    let coordinator = DurableCoordinator::open(store).expect("store reopens");
    let mut replay =
        AgentInteractionController::resume("session:interaction", FakeHost::default(), coordinator)
            .expect("interaction controller reopens");
    let retained = replay
        .execute("occurrence:caller-owned:tool:1", request.clone())
        .expect("retained response replays");
    assert_eq!(retained, first);
    assert!(matches!(
        replay.execute(
            "occurrence:caller-owned:tool:1",
            AgentHostRequest::Tool(ToolRequest {
                tool_call_id: "tool:interaction".to_owned(),
                operation: "workspace.write".to_owned(),
                input: json!({"path": "README.md"}),
            }),
        ),
        Err(AgentError::IllegalTransition(_))
    ));
    let (replay_host, _) = replay.into_parts();
    assert_eq!(replay_host.tool_calls, 0);
}

#[test]
fn workspace_commit_transfers_and_resolves_one_scope_obligation() {
    let store = MemoryStore::new();
    let (mut coordinator, request) = workspace_coordinator(store.clone(), "run:workspace-commit");
    let mut host = WorkspaceTestHost::new(false);
    let applies = host.applies.clone();
    let checkpoint = WorkspaceScopeController::commit(&mut coordinator, &mut host, &request)
        .expect("workspace commit completes");
    assert!(
        checkpoint
            .receipt
            .as_ref()
            .is_some_and(|receipt| receipt.committed)
    );
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    let intent_id = checkpoint.effect_intent_id.expect("commit has an intent");
    let obligation_id = checkpoint.obligation_id.expect("commit has an obligation");
    let state = coordinator.state().expect("durable state");
    let run = &state.machine.events.last().expect("events exist").run_id;
    assert_eq!(run, "run:workspace-commit");
    let machine = coordinator.restore_machine().expect("Machine restores");
    let run = &machine.projection().runs["run:workspace-commit"];
    assert_eq!(
        run.scopes[ROOT_SCOPE_ID].status,
        ScopeStatus::ClosedCommitted
    );
    assert_eq!(run.effects[&intent_id].outcome, WorldOutcome::Applied);
    assert!(run.obligations[&obligation_id].resolved);
    assert_eq!(state.outbox[&intent_id].state, OutboxState::Applied);
    assert!(
        state.continuations["run:workspace-commit"]
            .scope_stack
            .is_empty()
    );
    assert!(
        state.continuations["run:workspace-commit"]
            .effect_obligations
            .is_empty()
    );
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("workspace store reopens");
    let mut replay_host = WorkspaceTestHost::new(false);
    let replay_applies = replay_host.applies.clone();
    let replay = WorkspaceScopeController::commit(&mut reopened, &mut replay_host, &request)
        .expect("completed workspace commit replays");
    assert_eq!(replay.receipt, checkpoint.receipt);
    assert_eq!(replay_applies.load(Ordering::SeqCst), 0);
    assert!(matches!(
        WorkspaceScopeController::abort(&mut reopened, &mut replay_host, &request),
        Err(AgentError::IllegalTransition(_))
    ));
}

#[test]
fn workspace_overlay_requires_an_exact_canonical_artifact_before_journaling() {
    for changed_kind in [false, true] {
        let run_id = if changed_kind {
            "run:workspace-forged-kind"
        } else {
            "run:workspace-missing-artifact"
        };
        let (mut coordinator, mut request) = workspace_coordinator(MemoryStore::new(), run_id);
        if changed_kind {
            request.overlay.kind = "workspace/forged".to_owned();
        } else {
            request.overlay.artifact_id = format!("sha256:{}", "f".repeat(64));
        }
        let revision = coordinator.revision().expect("revision exists").to_owned();
        let mut host = WorkspaceTestHost::new(false);
        let applies = host.applies.clone();
        assert!(matches!(
            WorkspaceScopeController::commit(&mut coordinator, &mut host, &request),
            Err(AgentError::NotFound(_))
        ));
        assert_eq!(applies.load(Ordering::SeqCst), 0);
        assert_eq!(coordinator.revision(), Some(revision.as_str()));
        assert!(
            AgentOccurrenceStore::load_occurrences(&mut coordinator, &request.session_id)
                .expect("occurrence journal remains readable")
                .is_empty()
        );
    }
}

#[test]
fn ambiguous_workspace_commit_reconciles_without_redispatch() {
    let store = MemoryStore::new();
    let (mut coordinator, request) =
        workspace_coordinator(store.clone(), "run:workspace-reconcile");
    let mut failing = WorkspaceTestHost::new(true);
    let applies = failing.applies.clone();
    assert!(matches!(
        WorkspaceScopeController::commit(&mut coordinator, &mut failing, &request),
        Err(AgentError::Host(_))
    ));
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    let state = coordinator.state().expect("durable state");
    assert!(
        state
            .outbox
            .values()
            .any(|dispatch| dispatch.state == OutboxState::Unknown)
    );
    let machine = coordinator.restore_machine().expect("Machine restores");
    let run = &machine.projection().runs["run:workspace-reconcile"];
    assert!(
        run.obligations
            .values()
            .any(|obligation| !obligation.resolved)
    );
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("workspace store reopens");
    let mut recovery = WorkspaceTestHost::new(false);
    let recovered_applies = recovery.applies.clone();
    let reconciliations = recovery.reconciliations.clone();
    let checkpoint = WorkspaceScopeController::reconcile(&mut reopened, &mut recovery, &request)
        .expect("workspace commit reconciles");
    assert!(
        checkpoint
            .receipt
            .as_ref()
            .is_some_and(|receipt| receipt.committed)
    );
    assert_eq!(recovered_applies.load(Ordering::SeqCst), 0);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 1);
    let state = reopened.state().expect("durable state");
    assert!(
        state
            .outbox
            .values()
            .any(|dispatch| dispatch.state == OutboxState::Applied)
    );
    let machine = reopened.restore_machine().expect("Machine restores");
    assert!(
        machine.projection().runs["run:workspace-reconcile"]
            .obligations
            .values()
            .all(|obligation| obligation.resolved)
    );
}

#[test]
fn workspace_commit_not_applied_evidence_resolves_without_claiming_a_receipt() {
    let store = MemoryStore::new();
    let (mut coordinator, request) =
        workspace_coordinator(store.clone(), "run:workspace-not-applied");
    let mut failing = WorkspaceTestHost::new(true);
    assert!(WorkspaceScopeController::commit(&mut coordinator, &mut failing, &request).is_err());
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("workspace store reopens");
    let mut recovery = WorkspaceTestHost::new(false).with_not_applied_reconciliation();
    let checkpoint = WorkspaceScopeController::reconcile(&mut reopened, &mut recovery, &request)
        .expect("not-applied evidence settles the original commit");
    assert!(checkpoint.receipt.is_none());
    assert_eq!(
        checkpoint.occurrence.state,
        AgentHostOccurrenceState::NotApplied
    );
    let intent_id = checkpoint.effect_intent_id.expect("commit has an intent");
    assert_eq!(
        reopened.state().expect("state").outbox[&intent_id].state,
        OutboxState::NotApplied
    );
    let machine = reopened.restore_machine().expect("Machine restores");
    assert_eq!(
        machine.projection().runs["run:workspace-not-applied"].effects[&intent_id].outcome,
        WorldOutcome::NotApplied
    );
    assert!(
        machine.projection().runs["run:workspace-not-applied"]
            .obligations
            .values()
            .all(|obligation| obligation.resolved)
    );
}

#[test]
fn workspace_abort_closes_scope_only_after_a_retained_non_commit_receipt() {
    let store = MemoryStore::new();
    let (mut coordinator, request) = workspace_coordinator(store.clone(), "run:workspace-abort");
    let mut host = WorkspaceTestHost::new(false);
    let applies = host.applies.clone();
    let checkpoint = WorkspaceScopeController::abort(&mut coordinator, &mut host, &request)
        .expect("workspace abort completes");
    assert!(
        checkpoint
            .receipt
            .as_ref()
            .is_some_and(|receipt| !receipt.committed)
    );
    assert!(checkpoint.effect_intent_id.is_none());
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    let machine = coordinator.restore_machine().expect("Machine restores");
    let run = &machine.projection().runs["run:workspace-abort"];
    assert_eq!(run.scopes[ROOT_SCOPE_ID].status, ScopeStatus::ClosedAborted);
    assert!(run.obligations.is_empty());
    assert!(coordinator.state().expect("state").outbox.is_empty());
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("workspace store reopens");
    let mut replay_host = WorkspaceTestHost::new(false);
    let replay_applies = replay_host.applies.clone();
    WorkspaceScopeController::abort(&mut reopened, &mut replay_host, &request)
        .expect("completed workspace abort replays");
    assert_eq!(replay_applies.load(Ordering::SeqCst), 0);
}

#[test]
fn lost_workspace_completion_receipt_is_reconciled_from_the_started_claim() {
    let store = MemoryStore::new();
    let (mut coordinator, request) =
        workspace_coordinator(store.clone(), "run:workspace-receipt-loss");
    let applies = Arc::new(AtomicUsize::new(0));
    let mut racing = RacingWorkspaceHost {
        store: store.clone(),
        applies: applies.clone(),
    };
    assert!(matches!(
        WorkspaceScopeController::commit(&mut coordinator, &mut racing, &request),
        Err(AgentError::Persistence(_))
    ));
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("workspace store reopens");
    let occurrences =
        AgentOccurrenceStore::load_occurrences(&mut reopened, "session:run:workspace-receipt-loss")
            .expect("occurrence replays");
    assert_eq!(occurrences[0].state, AgentHostOccurrenceState::Started);
    let mut recovery = WorkspaceTestHost::new(false);
    let recovery_applies = recovery.applies.clone();
    WorkspaceScopeController::reconcile(&mut reopened, &mut recovery, &request)
        .expect("lost completion receipt reconciles");
    assert_eq!(recovery_applies.load(Ordering::SeqCst), 0);
    assert!(
        reopened
            .restore_machine()
            .expect("Machine restores")
            .projection()
            .runs["run:workspace-receipt-loss"]
            .obligations
            .values()
            .all(|obligation| obligation.resolved)
    );
}

#[test]
fn durable_input_wait_suspends_and_resumes_atomically_across_reopen() {
    let mut machine = Machine::new();
    install_agent_input(&mut machine);
    let result = machine
        .put_artifact("agent/input", br#"{"answer":"yes"}"#.to_vec())
        .expect("Artifact stores");
    let second_result = machine
        .put_artifact("agent/input", br#"{"details":"ready"}"#.to_vec())
        .expect("Artifact stores");
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
        schema: json!({
            "$defs": {"answer": {"type": "string"}},
            "type": "object",
            "properties": {"answer": {"$ref": "#/$defs/answer"}},
            "required": ["answer"],
            "additionalProperties": false
        }),
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
fn input_schema_and_external_references_fail_before_suspension() {
    let mut machine = Machine::new();
    install_agent_input(&mut machine);
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(agent_continuation("run:invalid-schema"))
        .expect("continuation persists");
    let revision = coordinator.revision().expect("revision exists").to_owned();

    for (request_id, schema) in [
        ("elicitation:invalid-schema", json!({"type": 42})),
        (
            "elicitation:external-schema",
            json!({"$ref": "https://schemas.example.invalid/input.json"}),
        ),
    ] {
        assert!(matches!(
            AgentInputController::suspend(
                &mut coordinator,
                "session:invalid-schema",
                "run:invalid-schema",
                ElicitationRequest {
                    request_id: request_id.to_owned(),
                    schema,
                    prompt: Vec::new(),
                },
            ),
            Err(AgentError::Validation(_))
        ));
        assert_eq!(coordinator.revision(), Some(revision.as_str()));
        assert!(coordinator.state().expect("state").waits.is_empty());
        assert!(
            AgentJournal::load(&mut coordinator, "session:invalid-schema")
                .expect("journal loads")
                .is_empty()
        );
    }
}

#[test]
fn invalid_completed_input_leaves_wait_and_session_pending() {
    let mut machine = Machine::new();
    install_agent_input(&mut machine);
    let result = machine
        .put_artifact("agent/input", br#"{"answer":42}"#.to_vec())
        .expect("Artifact stores");
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(agent_continuation("run:invalid-input"))
        .expect("continuation persists");
    let suspended = AgentInputController::suspend(
        &mut coordinator,
        "session:invalid-input",
        "run:invalid-input",
        ElicitationRequest {
            request_id: "elicitation:invalid-input".to_owned(),
            schema: json!({
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": false
            }),
            prompt: Vec::new(),
        },
    )
    .expect("input suspends");
    let revision = coordinator.revision().expect("revision exists").to_owned();
    let record_count = coordinator
        .journal_records("session:invalid-input")
        .expect("journal loads")
        .len();

    assert!(matches!(
        AgentInputController::complete(
            &mut coordinator,
            "session:invalid-input",
            &suspended.wait_id,
            result,
            ElicitationResponse {
                request_id: "elicitation:invalid-input".to_owned(),
                accepted: true,
                value: Some(json!({"answer": 42})),
                occurrence_binding: "binding:human-input/1".to_owned(),
            },
        ),
        Err(AgentError::Validation(_))
    ));
    assert_eq!(coordinator.revision(), Some(revision.as_str()));
    assert_eq!(
        coordinator.state().expect("state").waits[&suspended.wait_id].state,
        WaitState::Pending
    );
    assert_eq!(
        coordinator.state().expect("state").continuations["run:invalid-input"].status,
        ContinuationStatus::Waiting
    );
    assert_eq!(
        coordinator
            .journal_records("session:invalid-input")
            .expect("journal loads")
            .len(),
        record_count
    );
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    let session = AgentSession::replay(
        "session:invalid-input",
        AgentJournal::load(&mut reopened, "session:invalid-input").expect("journal replays"),
    )
    .expect("Session replays");
    assert_eq!(session.state, AgentState::RequiresAction);
    assert!(
        session.elicitations["elicitation:invalid-input"]
            .response
            .is_none()
    );
}

#[test]
fn declined_input_completes_without_an_instance_value() {
    let mut machine = Machine::new();
    install_agent_input(&mut machine);
    let result = machine
        .put_artifact("agent/input-declined", b"null".to_vec())
        .expect("Artifact stores");
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(agent_continuation("run:declined-input"))
        .expect("continuation persists");
    let suspended = AgentInputController::suspend(
        &mut coordinator,
        "session:declined-input",
        "run:declined-input",
        ElicitationRequest {
            request_id: "elicitation:declined-input".to_owned(),
            schema: json!({"type": "string", "minLength": 1}),
            prompt: Vec::new(),
        },
    )
    .expect("input suspends");
    let completed = AgentInputController::complete(
        &mut coordinator,
        "session:declined-input",
        &suspended.wait_id,
        result,
        ElicitationResponse {
            request_id: "elicitation:declined-input".to_owned(),
            accepted: false,
            value: None,
            occurrence_binding: "binding:human-input/1".to_owned(),
        },
    )
    .expect("decline completes without a value");

    assert_eq!(completed.session.state, AgentState::Running);
    assert_eq!(
        coordinator.state().expect("state").waits[&suspended.wait_id].state,
        WaitState::Completed
    );
    assert_eq!(
        coordinator.state().expect("state").continuations["run:declined-input"].status,
        ContinuationStatus::Ready
    );
}

#[test]
fn stale_input_checkpoint_writes_neither_wait_nor_agent_update() {
    let mut machine = Machine::new();
    install_agent_input(&mut machine);
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
                identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
                artifact_id: format!("sha256:{}", "a".repeat(64)),
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

#[derive(Clone, Default)]
struct FailingToolHost {
    dispatches: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
}

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
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        Err(AgentError::Host("simulated process loss".to_owned()))
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn reconcile_occurrence(
        &mut self,
        occurrence: &cymule_agent::AgentHostOccurrence,
    ) -> AgentResult<AgentOccurrenceResolution> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        let AgentHostRequest::Tool(request) = &occurrence.request else {
            return Ok(AgentOccurrenceResolution::Unknown {
                evidence: vec![ContentBlock::Text {
                    text: "unsupported occurrence kind".to_owned(),
                }],
            });
        };
        Ok(AgentOccurrenceResolution::Completed {
            response: cymule_agent::AgentHostResponse::Tool(ToolResponse {
                tool_call_id: request.tool_call_id.clone(),
                content: vec![ContentBlock::Json {
                    value: json!({"exists": true, "recovered": true}),
                }],
                occurrence_binding: occurrence.occurrence_binding.clone(),
            }),
        })
    }
}

#[test]
fn interaction_controller_consumes_reconciled_response_without_owning_the_agent_loop() {
    let mut journal = MemoryAgentJournal::default();
    let host = FailingToolHost::default();
    let dispatches = host.dispatches.clone();
    let reconciliations = host.reconciliations.clone();
    let request = AgentHostRequest::Tool(ToolRequest {
        tool_call_id: "tool:caller-loop".to_owned(),
        operation: "workspace.read".to_owned(),
        input: json!({"path": "README.md"}),
    });
    let mut controller =
        AgentInteractionController::resume("session:caller-loop", host, journal.clone())
            .expect("interaction controller opens");
    assert!(matches!(
        controller.execute("occurrence:caller-loop:1", request.clone()),
        Err(AgentError::Host(_))
    ));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    drop(controller);

    let mut blocked = AgentInteractionController::resume(
        "session:caller-loop",
        FailingToolHost {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
        },
        journal.clone(),
    )
    .expect("controller reopens with unknown occurrence");
    assert!(matches!(
        blocked.execute("occurrence:caller-loop:1", request.clone()),
        Err(AgentError::RecoveryRequired(_))
    ));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    drop(blocked);

    let mut recovery_host = FailingToolHost {
        dispatches: dispatches.clone(),
        reconciliations: reconciliations.clone(),
    };
    AgentRecoveryController::reconcile(
        &mut recovery_host,
        &mut journal,
        "session:caller-loop",
        "occurrence:caller-loop:1",
    )
    .expect("original occurrence reconciles");

    let mut resumed = AgentInteractionController::resume(
        "session:caller-loop",
        FailingToolHost {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
        },
        journal,
    )
    .expect("controller reopens after reconciliation");
    let response = resumed
        .execute("occurrence:caller-loop:1", request)
        .expect("reconciled response is consumed");
    assert!(matches!(response, cymule_agent::AgentHostResponse::Tool(_)));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 1);
}

#[test]
fn host_failure_preserves_the_last_accepted_interaction_state() {
    let mut journal = MemoryAgentJournal::default();
    let host = FailingToolHost::default();
    let dispatches = host.dispatches.clone();
    let reconciliations = host.reconciliations.clone();
    let mut driver =
        AgentTurnDriver::resume("session:crash", host, journal.clone()).expect("journal opens");
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
    let unknown = occurrences
        .iter()
        .find(|occurrence| {
            occurrence.state == AgentHostOccurrenceState::Unknown
                && occurrence.occurrence_id.starts_with("occurrence:tool:")
                && occurrence.occurrence_binding == "binding:tool/1"
        })
        .expect("unknown tool occurrence exists");
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert!(matches!(
        AgentTurnDriver::resume("session:crash", FailingToolHost::default(), journal.clone()),
        Err(AgentError::RecoveryRequired(_))
    ));
    let mut recovery_host = FailingToolHost {
        dispatches: dispatches.clone(),
        reconciliations: reconciliations.clone(),
    };
    let recovered = AgentRecoveryController::reconcile(
        &mut recovery_host,
        &mut journal,
        "session:crash",
        &unknown.occurrence_id,
    )
    .expect("original tool occurrence reconciles");
    assert_eq!(recovered.state, AgentHostOccurrenceState::Completed);
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 1);
    assert!(matches!(
        AgentTurnDriver::resume("session:crash", FailingToolHost::default(), journal),
        Err(AgentError::RecoveryRequired(_))
    ));
}

#[test]
fn prepared_occurrence_cancels_only_with_not_applied_evidence() {
    let mut journal = MemoryAgentJournal::default();
    let prepared = cymule_agent::AgentHostOccurrence::prepare(
        "occurrence:tool:cancel",
        "session:cancel",
        AgentHostRequest::Tool(ToolRequest {
            tool_call_id: "tool:cancel".to_owned(),
            operation: "workspace.read".to_owned(),
            input: json!({}),
        }),
        "binding:tool/1",
    )
    .expect("occurrence prepares");
    journal
        .record_occurrence(&prepared)
        .expect("prepared occurrence persists");
    assert!(matches!(
        AgentTurnDriver::resume("session:cancel", FakeHost::default(), journal.clone()),
        Err(AgentError::RecoveryRequired(_))
    ));
    assert!(matches!(
        AgentRecoveryController::cancel_prepared(
            &mut journal,
            "session:cancel",
            "occurrence:tool:cancel",
            Vec::new(),
        ),
        Err(AgentError::Validation(_))
    ));
    let cancelled = AgentRecoveryController::cancel_prepared(
        &mut journal,
        "session:cancel",
        "occurrence:tool:cancel",
        vec![ContentBlock::Text {
            text: "dispatch boundary was never entered".to_owned(),
        }],
    )
    .expect("prepared occurrence settles not applied");
    assert_eq!(cancelled.state, AgentHostOccurrenceState::NotApplied);
    let _driver = AgentTurnDriver::resume("session:cancel", FakeHost::default(), journal)
        .expect("terminal not-applied occurrence no longer blocks idle Session");
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
fn interaction_controller_does_not_redispatch_when_the_completion_receipt_is_lost() {
    let journal = FailAfterToolResultJournal::default();
    let request = AgentHostRequest::Tool(ToolRequest {
        tool_call_id: "tool:receipt-loss-controller".to_owned(),
        operation: "workspace.read".to_owned(),
        input: json!({"path": "README.md"}),
    });
    let mut controller = AgentInteractionController::resume(
        "session:receipt-loss-controller",
        FakeHost::default(),
        journal.clone(),
    )
    .expect("interaction controller opens");
    assert!(matches!(
        controller.execute("occurrence:receipt-loss-controller:1", request.clone()),
        Err(AgentError::Persistence(_))
    ));
    let (host, journal) = controller.into_parts();
    assert_eq!(host.tool_calls, 1);

    let mut reopened = AgentInteractionController::resume(
        "session:receipt-loss-controller",
        FakeHost::default(),
        journal,
    )
    .expect("controller reopens with a started occurrence");
    assert!(matches!(
        reopened.execute("occurrence:receipt-loss-controller:1", request),
        Err(AgentError::RecoveryRequired(_))
    ));
    let (host, _) = reopened.into_parts();
    assert_eq!(host.tool_calls, 0);
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
