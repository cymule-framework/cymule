//! Restart-level resumable interpreter tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cymule_core::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    MutationKind, Operation, PlanCandidate, ReconciliationMode, ReconciliationResolution, Region,
    ScopeMode, Step, WaitSpec, WorldOutcome,
};
use cymule_durable::{
    DURABLE_CONTROL_VERSION, DriveOutcome, DurableBoundary, DurableCommand, DurableError,
    DurableResponse, DurableResult, DurableRuntimeControl, DurableState, DurableStore, MemoryStore,
    OutboxState, ResumableRuntime, StoreCommit, StoredState, WaitActivationSource,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation,
    PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::json;

struct CountingPlugin {
    calls: Arc<AtomicUsize>,
}

struct CrashAfterApplyPlugin {
    dispatches: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
    crash_after_apply: bool,
    unknown_reconciliations: usize,
}

fn open_runtime<S: DurableStore, P: PluginHost>(
    store: S,
    mut plugin: P,
) -> DurableResult<ResumableRuntime<S, P>> {
    let manifest = plugin
        .describe()
        .map_err(|error| DurableError::Substrate(error.to_string()))?;
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:4444444444444444444444444444444444444444444444444444444444444444",
    )
    .map_err(|error| DurableError::Validation(error.to_string()))?;
    ResumableRuntime::open(store, plugin, binding)
}

#[derive(Debug, Clone, Copy)]
enum ReceiptLossStage {
    Enqueue,
    ScopeCommit,
    Claim,
    Applied,
    Unknown,
    EagerBinding,
    RunComplete,
}

#[derive(Clone)]
struct StageReceiptLossStore {
    inner: MemoryStore,
    stage: ReceiptLossStage,
    lost: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy)]
enum CasFaultTiming {
    BeforeCommit,
    AfterCommit,
}

#[derive(Debug)]
struct CasFaultControl {
    calls: AtomicUsize,
    fail_at: AtomicUsize,
}

#[derive(Clone)]
struct CasFaultStore {
    inner: MemoryStore,
    timing: CasFaultTiming,
    control: Arc<CasFaultControl>,
}

#[derive(Clone)]
struct ArmableLostReceiptStore {
    inner: MemoryStore,
    lose_next: Arc<AtomicBool>,
}

impl ArmableLostReceiptStore {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            lose_next: Arc::new(AtomicBool::new(false)),
        }
    }

    fn arm(&self) {
        self.lose_next.store(true, Ordering::SeqCst);
    }
}

impl DurableStore for ArmableLostReceiptStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if self.lose_next.swap(false, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated lost multi-Run creation receipt".to_owned(),
            ));
        }
        Ok(commit)
    }
}

impl CasFaultStore {
    fn new(timing: CasFaultTiming, fail_at: usize) -> Self {
        Self {
            inner: MemoryStore::new(),
            timing,
            control: Arc::new(CasFaultControl {
                calls: AtomicUsize::new(0),
                fail_at: AtomicUsize::new(fail_at),
            }),
        }
    }

    fn calls(&self) -> usize {
        self.control.calls.load(Ordering::SeqCst)
    }

    fn disable_fault(&self) {
        self.control.fail_at.store(0, Ordering::SeqCst);
    }

    fn should_fail(&self, call: usize) -> bool {
        self.control
            .fail_at
            .compare_exchange(call, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

impl DurableStore for CasFaultStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let call = self.control.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if matches!(self.timing, CasFaultTiming::BeforeCommit) && self.should_fail(call) {
            return Err(DurableError::Substrate(format!(
                "simulated I/O failure before CAS {call}"
            )));
        }
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if matches!(self.timing, CasFaultTiming::AfterCommit) && self.should_fail(call) {
            return Err(DurableError::Substrate(format!(
                "simulated lost acknowledgement after CAS {call}"
            )));
        }
        Ok(commit)
    }
}

impl DurableStore for StageReceiptLossStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        let reached = next.outbox.values().any(|dispatch| {
            matches!(
                (self.stage, dispatch.state),
                (ReceiptLossStage::Enqueue, OutboxState::Pending)
                    | (ReceiptLossStage::Claim, OutboxState::Claimed)
                    | (ReceiptLossStage::Applied, OutboxState::Applied)
                    | (ReceiptLossStage::Unknown, OutboxState::Unknown)
            )
        }) || matches!(self.stage, ReceiptLossStage::ScopeCommit)
            && next
                .outbox
                .values()
                .any(|dispatch| dispatch.state == OutboxState::Pending)
            && next.machine.events.last().is_some_and(|event| {
                matches!(
                    &event.payload,
                    cymule_core::EventPayload::ScopeCommitted { .. }
                )
            })
            || matches!(self.stage, ReceiptLossStage::EagerBinding)
                && next
                    .outbox
                    .values()
                    .any(|dispatch| dispatch.state == OutboxState::Applied)
                && next.continuations.values().any(|continuation| {
                    continuation
                        .frames
                        .last()
                        .is_some_and(|frame| frame.next_step > 0)
                })
            || matches!(self.stage, ReceiptLossStage::RunComplete)
                && next.continuations.values().any(|continuation| {
                    continuation.status == cymule_durable::ContinuationStatus::Completed
                });
        if reached && !self.lost.swap(true, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated receipt loss after durable effect stage".to_owned(),
            ));
        }
        Ok(commit)
    }
}

struct StagePlugin {
    prepares: Arc<AtomicUsize>,
    dispatches: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
    dispatch_outcome: WorldOutcome,
    reconciliation: ReconciliationResolution,
    lose_first_prepare_response: bool,
}

struct SweepPlugin {
    dispatches: Arc<AtomicUsize>,
    reconciliations: Arc<AtomicUsize>,
}

impl PluginHost for SweepPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "cas-fault-sweep@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::from([(
                        "test.capture".to_owned(),
                        PluginEffect {
                            implementation_revision: "1".to_owned(),
                            can_reconcile: true,
                        },
                    )]),
                },
            }),
            PluginRequest::PrepareEffect { .. } => Ok(PluginResponse::Prepared),
            PluginRequest::DispatchEffect { input, .. } => {
                self.dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::EffectResult {
                    outcome: WorldOutcome::Applied,
                    value: Some(input),
                })
            }
            PluginRequest::ReconcileEffect { input, .. } => {
                self.reconciliations.fetch_add(1, Ordering::SeqCst);
                let applied = self.dispatches.load(Ordering::SeqCst) > 0;
                Ok(PluginResponse::ReconciliationResult {
                    resolution: if applied {
                        ReconciliationResolution::ResolvedApplied
                    } else {
                        ReconciliationResolution::ResolvedNotApplied
                    },
                    value: applied.then_some(input),
                })
            }
            request @ PluginRequest::Call { .. } => Err(RuntimeError::PluginDefect {
                code: "unsupported_test_request".to_owned(),
                message: format!("unsupported CAS sweep request: {request:?}"),
            }),
        }
    }
}

impl PluginHost for StagePlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "effect-stage-test@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::from([(
                        "test.capture".to_owned(),
                        PluginEffect {
                            implementation_revision: "1".to_owned(),
                            can_reconcile: true,
                        },
                    )]),
                },
            }),
            PluginRequest::PrepareEffect { .. } => {
                let attempt = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
                if self.lose_first_prepare_response && attempt == 1 {
                    return Err(RuntimeError::Substrate {
                        code: "simulated_response_loss".to_owned(),
                        message: "simulated lost response after external prepare".to_owned(),
                    });
                }
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::DispatchEffect { input, .. } => {
                self.dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::EffectResult {
                    outcome: self.dispatch_outcome,
                    value: (self.dispatch_outcome == WorldOutcome::Applied).then_some(input),
                })
            }
            PluginRequest::ReconcileEffect { input, .. } => {
                self.reconciliations.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::ReconciliationResult {
                    resolution: self.reconciliation,
                    value: (self.reconciliation == ReconciliationResolution::ResolvedApplied)
                        .then_some(input),
                })
            }
            request @ PluginRequest::Call { .. } => Err(RuntimeError::PluginDefect {
                code: "unsupported_test_request".to_owned(),
                message: format!("unsupported stage test request: {request:?}"),
            }),
        }
    }
}

impl PluginHost for CrashAfterApplyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "effect-recovery-test@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::from([(
                        "test.capture".to_owned(),
                        PluginEffect {
                            implementation_revision: "1".to_owned(),
                            can_reconcile: true,
                        },
                    )]),
                },
            }),
            PluginRequest::PrepareEffect { .. } => Ok(PluginResponse::Prepared),
            PluginRequest::DispatchEffect { .. } => {
                self.dispatches.fetch_add(1, Ordering::SeqCst);
                if self.crash_after_apply {
                    Err(RuntimeError::Substrate {
                        code: "simulated_process_crash".to_owned(),
                        message: "simulated crash after provider application".to_owned(),
                    })
                } else {
                    Err(RuntimeError::PluginDefect {
                        code: "duplicate_test_dispatch".to_owned(),
                        message: "recovery must not redispatch the original intent".to_owned(),
                    })
                }
            }
            PluginRequest::ReconcileEffect { input, .. } => {
                let attempt = self.reconciliations.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(PluginResponse::ReconciliationResult {
                    resolution: if attempt <= self.unknown_reconciliations {
                        cymule_core::ReconciliationResolution::StillUnknown
                    } else {
                        cymule_core::ReconciliationResolution::ResolvedApplied
                    },
                    value: (attempt > self.unknown_reconciliations).then_some(input),
                })
            }
            request @ PluginRequest::Call { .. } => Err(RuntimeError::PluginDefect {
                code: "unsupported_test_request".to_owned(),
                message: format!("unsupported effect recovery request: {request:?}"),
            }),
        }
    }
}

impl PluginHost for CountingPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "resume-test@1".to_owned(),
                    components: BTreeMap::from([(
                        "test.greet".to_owned(),
                        PluginOperation {
                            implementation_revision: "1".to_owned(),
                        },
                    )]),
                    effects: BTreeMap::new(),
                },
            }),
            PluginRequest::Call { component, input } if component == "test.greet" => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::CallResult {
                    value: json!({"greeting": format!("Hello, {}!", input["name"].as_str().unwrap())}),
                })
            }
            request => Err(RuntimeError::PluginDefect {
                code: "unsupported_test_request".to_owned(),
                message: format!("unsupported resume test request: {request:?}"),
            }),
        }
    }
}

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "resume_after_input".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.greet".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            requirements: BTreeMap::new(),
        }],
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![
                    Step {
                        id: "call.greet".to_owned(),
                        operation: Operation::Call {
                            component: "test.greet".to_owned(),
                            input: Expression::Input,
                            bind: Some("greeting".to_owned()),
                        },
                    },
                    Step {
                        id: "wait.approval".to_owned(),
                        operation: Operation::Wait {
                            wait: WaitSpec::Input {
                                correlation: "approval".to_owned(),
                                schema: json!({"type": "boolean"}),
                            },
                        },
                    },
                ],
                result: Expression::Binding {
                    name: "greeting".to_owned(),
                },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn nested_wait_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "nested_resume_after_input".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.greet".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            requirements: BTreeMap::new(),
        }],
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "scope.nested".to_owned(),
                    operation: Operation::Scope {
                        mode: ScopeMode::Transactional,
                        body: Box::new(Region {
                            steps: vec![
                                Step {
                                    id: "call.nested-greet".to_owned(),
                                    operation: Operation::Call {
                                        component: "test.greet".to_owned(),
                                        input: Expression::Input,
                                        bind: Some("greeting".to_owned()),
                                    },
                                },
                                Step {
                                    id: "wait.nested-approval".to_owned(),
                                    operation: Operation::Wait {
                                        wait: WaitSpec::Input {
                                            correlation: "nested-approval".to_owned(),
                                            schema: json!({}),
                                        },
                                    },
                                },
                            ],
                            result: Expression::Binding {
                                name: "greeting".to_owned(),
                            },
                        }),
                        bind: Some("nested_result".to_owned()),
                    },
                }],
                result: Expression::Binding {
                    name: "nested_result".to_owned(),
                },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn invoked_wait_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "invoked_resume_after_input".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.greet".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            requirements: BTreeMap::new(),
        }],
        effects: Vec::new(),
        definitions: vec![
            Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: vec![Step {
                        id: "invoke.review".to_owned(),
                        operation: Operation::Invoke {
                            definition: "review".to_owned(),
                            input: Expression::Input,
                            bind: Some("review_result".to_owned()),
                        },
                    }],
                    result: Expression::Binding {
                        name: "review_result".to_owned(),
                    },
                },
            },
            Definition {
                id: "review".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: vec![
                        Step {
                            id: "call.review-greet".to_owned(),
                            operation: Operation::Call {
                                component: "test.greet".to_owned(),
                                input: Expression::Input,
                                bind: Some("greeting".to_owned()),
                            },
                        },
                        Step {
                            id: "wait.review-approval".to_owned(),
                            operation: Operation::Wait {
                                wait: WaitSpec::Input {
                                    correlation: "review-approval".to_owned(),
                                    schema: json!({}),
                                },
                            },
                        },
                    ],
                    result: Expression::Binding {
                        name: "greeting".to_owned(),
                    },
                },
            },
        ],
        metadata: BTreeMap::new(),
    }
}

fn effect_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "recover_unknown_effect".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: vec![EffectContract {
            id: "test.capture".to_owned(),
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
                steps: vec![Step {
                    id: "effect.capture".to_owned(),
                    operation: Operation::Effect {
                        effect: "test.capture".to_owned(),
                        input: Expression::Input,
                        occurrence: "primary".to_owned(),
                        bind: None,
                    },
                }],
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn nested_effect_candidate() -> PlanCandidate {
    let mut candidate = effect_candidate();
    "nested_commit_gated_effect".clone_into(&mut candidate.name);
    candidate.definitions[0].body = Region {
        steps: vec![Step {
            id: "scope.effect".to_owned(),
            operation: Operation::Scope {
                mode: ScopeMode::Transactional,
                body: Box::new(Region {
                    steps: vec![Step {
                        id: "effect.nested-capture".to_owned(),
                        operation: Operation::Effect {
                            effect: "test.capture".to_owned(),
                            input: Expression::Input,
                            occurrence: "nested".to_owned(),
                            bind: None,
                        },
                    }],
                    result: Expression::Input,
                }),
                bind: Some("nested_result".to_owned()),
            },
        }],
        result: Expression::Binding {
            name: "nested_result".to_owned(),
        },
    };
    candidate
}

fn eager_effect_candidate() -> PlanCandidate {
    let mut candidate = effect_candidate();
    "eager_observation".clone_into(&mut candidate.name);
    candidate.effects[0].profile.mutation = MutationKind::Observational;
    candidate.effects[0].profile.dispatch = DispatchPolicy::Eager;
    candidate.definitions[0].body.steps[0] = Step {
        id: "effect.observe".to_owned(),
        operation: Operation::Effect {
            effect: "test.capture".to_owned(),
            input: Expression::Input,
            occurrence: "observation".to_owned(),
            bind: Some("observed".to_owned()),
        },
    };
    candidate.definitions[0].body.result = Expression::Binding {
        name: "observed".to_owned(),
    };
    candidate
}

fn explicit_effect_candidate() -> PlanCandidate {
    let mut candidate = effect_candidate();
    "explicit_release".clone_into(&mut candidate.name);
    candidate.effects[0].profile.dispatch = DispatchPolicy::Explicit;
    candidate
}

fn external_wait_candidate(name: &str, wait: WaitSpec) -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: name.to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "wait.external".to_owned(),
                    operation: Operation::Wait { wait },
                }],
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

#[test]
fn process_reopen_resumes_after_wait_without_reinvoking_component() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime opens");
    let DriveOutcome::Suspended { wait_id } = runtime
        .start(candidate(), &json!({"name": "Ada"}), "run:resume")
        .expect("run reaches wait")
    else {
        panic!("run should suspend");
    };
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let (store, _) = runtime.into_parts();
    let mut reopened = open_runtime(
        store,
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime reopens");
    let DriveOutcome::Completed(result) = reopened
        .complete_wait(&wait_id, &json!(true))
        .expect("run resumes and completes")
    else {
        panic!("run should complete");
    };
    assert_eq!(result.value, json!({"greeting": "Hello, Ada!"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened
            .coordinator()
            .state()
            .expect("state")
            .component_occurrences
            .len(),
        1
    );
}

#[test]
fn one_durable_domain_runs_multiple_runs_and_replays_start_exactly() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime opens");

    let DriveOutcome::Suspended {
        wait_id: first_wait,
    } = runtime
        .start(candidate(), &json!({"name": "Ada"}), "run:one")
        .expect("first Run suspends")
    else {
        panic!("first Run should suspend");
    };
    let first_revision = runtime
        .coordinator()
        .revision()
        .expect("first revision")
        .to_owned();
    let DriveOutcome::Suspended { wait_id: replayed } = runtime
        .start(candidate(), &json!({"name": "Ada"}), "run:one")
        .expect("identical start replays current boundary")
    else {
        panic!("start replay should remain suspended");
    };
    assert_eq!(replayed, first_wait);
    assert_eq!(
        runtime.coordinator().revision(),
        Some(first_revision.as_str())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    assert!(matches!(
        runtime.start(candidate(), &json!({"name": "Changed"}), "run:one"),
        Err(DurableError::IllegalTransition(message)) if message.contains("different input")
    ));
    let mut changed_plan = candidate();
    changed_plan.name = "different_plan".to_owned();
    assert!(matches!(
        runtime.start(changed_plan, &json!({"name": "Ada"}), "run:one"),
        Err(DurableError::IllegalTransition(message)) if message.contains("different Plan")
    ));
    assert_eq!(
        runtime.coordinator().revision(),
        Some(first_revision.as_str())
    );

    let DriveOutcome::Suspended {
        wait_id: second_wait,
    } = runtime
        .start(candidate(), &json!({"name": "Grace"}), "run:two")
        .expect("second Run shares the durable domain")
    else {
        panic!("second Run should suspend");
    };
    assert_ne!(second_wait, first_wait);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        runtime
            .coordinator()
            .state()
            .expect("state")
            .continuations
            .len(),
        2
    );

    let DriveOutcome::Completed(second) = runtime
        .complete_wait(&second_wait, &json!(true))
        .expect("second Run completes")
    else {
        panic!("second Run should complete");
    };
    let DriveOutcome::Completed(first) = runtime
        .complete_wait(&first_wait, &json!(true))
        .expect("first Run completes")
    else {
        panic!("first Run should complete");
    };
    assert_eq!(second.value, json!({"greeting": "Hello, Grace!"}));
    assert_eq!(first.value, json!({"greeting": "Hello, Ada!"}));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let machine = runtime
        .coordinator()
        .restore_machine()
        .expect("Machine restores");
    assert_eq!(machine.projection().runs.len(), 2);
    machine.verify_replay().expect("multi-Run history replays");
}

#[test]
fn second_run_creation_reopens_after_lost_cas_receipt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let store = ArmableLostReceiptStore::new();
    let mut runtime = open_runtime(
        store.clone(),
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime opens");
    runtime
        .start(candidate(), &json!({"name": "Ada"}), "run:existing")
        .expect("existing Run reaches wait");
    store.arm();
    assert!(matches!(
        runtime.start(candidate(), &json!({"name": "Grace"}), "run:lost-receipt"),
        Err(DurableError::Substrate(message)) if message.contains("lost multi-Run")
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut reopened = open_runtime(
        store,
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("committed second Run reopens");
    let DriveOutcome::Suspended { wait_id } = reopened
        .start(candidate(), &json!({"name": "Grace"}), "run:lost-receipt")
        .expect("identical start resumes committed Run")
    else {
        panic!("reopened second Run should suspend");
    };
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let state = reopened.coordinator().state().expect("state");
    assert_eq!(state.continuations.len(), 2);
    assert_eq!(state.waits[&wait_id].run_id, "run:lost-receipt");
    let machine = reopened
        .coordinator()
        .restore_machine()
        .expect("Machine restores");
    assert_eq!(machine.projection().runs.len(), 2);
    assert_eq!(
        machine
            .events()
            .filter(|event| matches!(event.payload, cymule_core::EventPayload::RunStarted { .. }))
            .count(),
        2
    );
    machine.verify_replay().expect("multi-Run history replays");
}

#[test]
fn nested_scope_wait_reopens_from_region_path_without_reinvoking_component() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime opens");
    let DriveOutcome::Suspended { wait_id } = runtime
        .start(
            nested_wait_candidate(),
            &json!({"name": "Ada"}),
            "run:nested-resume",
        )
        .expect("nested Run suspends")
    else {
        panic!("nested Run should suspend");
    };
    let continuation =
        &runtime.coordinator().state().expect("state").continuations["run:nested-resume"];
    assert_eq!(continuation.frames.len(), 2);
    assert_eq!(continuation.frames[1].region_path, vec![0]);
    assert_eq!(continuation.scope_stack.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (store, _) = runtime.into_parts();

    let mut reopened = open_runtime(
        store,
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime reopens");
    let DriveOutcome::Completed(result) = reopened
        .complete_wait(&wait_id, &json!({"approved": true}))
        .expect("nested Run resumes")
    else {
        panic!("nested Run should complete");
    };
    assert_eq!(result.value, json!({"greeting": "Hello, Ada!"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let restored = reopened.coordinator().state().expect("state");
    assert_eq!(restored.continuations["run:nested-resume"].frames.len(), 1);
    assert_eq!(
        restored.continuations["run:nested-resume"].scope_stack,
        vec![cymule_core::ROOT_SCOPE_ID.to_owned()]
    );
}

#[test]
fn invoked_definition_wait_reopens_without_reinvoking_completed_component() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime opens");
    let run_id = "run:invoked-resume";
    let DriveOutcome::Suspended { wait_id } = runtime
        .start(invoked_wait_candidate(), &json!({"name": "Ada"}), run_id)
        .expect("invoked Run suspends")
    else {
        panic!("invoked Run should suspend");
    };
    let continuation = &runtime.coordinator().state().expect("state").continuations[run_id];
    assert_eq!(continuation.frames.len(), 2);
    assert_eq!(continuation.frames[0].definition_id, "main");
    assert_eq!(continuation.frames[1].definition_id, "review");
    assert!(continuation.frames[1].invocation_id.starts_with("sha256:"));
    assert_eq!(continuation.scope_stack.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let (store, _) = runtime.into_parts();
    let mut reopened = open_runtime(
        store,
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime reopens");
    let DriveOutcome::Completed(result) = reopened
        .complete_wait(&wait_id, &json!({"approved": true}))
        .expect("invoked Run resumes")
    else {
        panic!("invoked Run should complete");
    };
    assert_eq!(result.value, json!({"greeting": "Hello, Ada!"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened.coordinator().state().expect("state").continuations[run_id]
            .frames
            .len(),
        1
    );
}

#[test]
fn durable_control_drives_and_queries_one_domain_across_reopen() {
    let store = MemoryStore::new();
    let plugin = || SweepPlugin {
        dispatches: Arc::new(AtomicUsize::new(0)),
        reconciliations: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = open_runtime(store.clone(), plugin()).expect("runtime opens");
    let mut control = DurableRuntimeControl::new(runtime);
    let candidate = external_wait_candidate(
        "durable_control_signal",
        WaitSpec::Signal {
            key: "signal:control".to_owned(),
            consume_once: true,
        },
    );
    let input = json!({"message": "durable control"});
    let response = control
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:control".to_owned(),
            candidate: candidate.clone(),
            input: input.clone(),
        })
        .expect("Run starts");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("signal Run must suspend");
    };
    let DurableResponse::Run { run: Some(view) } = control
        .submit(DurableCommand::QueryRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            query_id: "query:control:waiting".to_owned(),
            run_id: "run:control".to_owned(),
        })
        .expect("Run queries")
    else {
        panic!("Run query must exist");
    };
    assert_eq!(
        view.continuation.status,
        cymule_durable::ContinuationStatus::Waiting
    );
    drop(control);

    let runtime = open_runtime(store, plugin()).expect("runtime reopens");
    let mut reopened = DurableRuntimeControl::new(runtime);
    assert_eq!(
        reopened
            .submit(DurableCommand::ActivateWait {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                activation_id: "activation:control".to_owned(),
                source: WaitActivationSource::Signal {
                    key: "signal:control".to_owned(),
                },
                wait_ids: BTreeSet::from([wait_id]),
                value: json!({"approved": true}),
            })
            .expect("wait activates"),
        DurableResponse::WaitActivated {
            ready_run_ids: BTreeSet::from(["run:control".to_owned()]),
        }
    );
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Completed { result },
    } = reopened
        .submit(DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:control".to_owned(),
        })
        .expect("Run resumes")
    else {
        panic!("resumed Run must complete");
    };
    assert_eq!(result.value, input);
    let replay = reopened
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:control".to_owned(),
            candidate,
            input,
        })
        .expect("identical start replays terminal result");
    assert!(matches!(
        replay,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. }
        }
    ));
    let DurableResponse::Domain { domain } = reopened
        .submit(DurableCommand::QueryDomain {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            query_id: "query:control:domain".to_owned(),
        })
        .expect("domain queries")
    else {
        panic!("domain query must return a domain view");
    };
    assert_eq!(domain.run_ids, ["run:control"]);

    let malformed = json!({
        "type": "resume_run",
        "control_version": DURABLE_CONTROL_VERSION,
        "run_id": "run:control",
        "provider": "must-not-enter-control"
    });
    assert!(serde_json::from_value::<DurableCommand>(malformed).is_err());
}

#[test]
fn every_run_cas_boundary_recovers_from_io_failure_or_lost_acknowledgement() {
    let baseline_store = CasFaultStore::new(CasFaultTiming::BeforeCommit, 0);
    let baseline_probe = baseline_store.clone();
    let mut baseline = open_runtime(
        baseline_store,
        SweepPlugin {
            dispatches: Arc::new(AtomicUsize::new(0)),
            reconciliations: Arc::new(AtomicUsize::new(0)),
        },
    )
    .expect("baseline runtime opens");
    assert!(matches!(
        baseline.start(
            effect_candidate(),
            &json!({"value": "baseline"}),
            "run:cas-sweep-baseline",
        ),
        Ok(DriveOutcome::Completed(_))
    ));
    let boundary_count = baseline_probe.calls();
    assert!(
        boundary_count >= 5,
        "effect Run should cross every durable stage"
    );

    for timing in [CasFaultTiming::BeforeCommit, CasFaultTiming::AfterCommit] {
        for fail_at in 1..=boundary_count {
            let store = CasFaultStore::new(timing, fail_at);
            let store_probe = store.clone();
            let dispatches = Arc::new(AtomicUsize::new(0));
            let reconciliations = Arc::new(AtomicUsize::new(0));
            let plugin = || SweepPlugin {
                dispatches: dispatches.clone(),
                reconciliations: reconciliations.clone(),
            };
            let run_id = format!("run:cas-sweep:{timing:?}:{fail_at}");
            let input = json!({"failure": fail_at, "timing": format!("{timing:?}")});
            let mut runtime = open_runtime(store, plugin()).expect("fault runtime opens");
            runtime
                .start(effect_candidate(), &input, &run_id)
                .expect_err("selected CAS boundary must fail once");
            store_probe.disable_fault();

            let mut durable_probe = store_probe.clone();
            let persisted = durable_probe.load().expect("durable state loads");
            if let Some(stored) = &persisted {
                stored.verify().expect("stored revision remains valid");
                assert!(
                    stored.state.continuations.contains_key(&run_id),
                    "a committed Run must never exist without its Continuation"
                );
            }

            let mut reopened = open_runtime(store_probe, plugin()).expect("faulted store reopens");
            let outcome = if persisted.is_some() {
                reopened.resume(&run_id)
            } else {
                reopened.start(effect_candidate(), &input, &run_id)
            }
            .expect("recovery converges");
            let DriveOutcome::Completed(result) = outcome else {
                panic!("faulted Run should complete after recovery");
            };
            assert_eq!(result.value, input);
            assert!(dispatches.load(Ordering::SeqCst) <= 1);
            assert!(reconciliations.load(Ordering::SeqCst) <= 1);

            let state = reopened
                .coordinator()
                .state()
                .expect("state remains readable");
            state
                .validate()
                .expect("recovered state passes integrity check");
            assert_eq!(
                state.continuations[&run_id].status,
                cymule_durable::ContinuationStatus::Completed
            );
            assert!(state.outbox.values().all(|dispatch| matches!(
                dispatch.state,
                OutboxState::Applied | OutboxState::NotApplied
            )));
            reopened
                .coordinator()
                .restore_machine()
                .expect("semantic projection replays after every fault");
        }
    }
}

#[test]
fn nested_effect_stays_staged_until_its_scope_commit_survives_reopen() {
    for (index, stage) in [ReceiptLossStage::Enqueue, ReceiptLossStage::ScopeCommit]
        .into_iter()
        .enumerate()
    {
        let lost = Arc::new(AtomicBool::new(false));
        let store = StageReceiptLossStore {
            inner: MemoryStore::new(),
            stage,
            lost: lost.clone(),
        };
        let prepares = Arc::new(AtomicUsize::new(0));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let reconciliations = Arc::new(AtomicUsize::new(0));
        let plugin = || StagePlugin {
            prepares: prepares.clone(),
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            dispatch_outcome: WorldOutcome::Applied,
            reconciliation: ReconciliationResolution::ResolvedApplied,
            lose_first_prepare_response: false,
        };
        let run_id = format!("run:nested-effect-receipt-loss:{index}");
        let mut runtime = open_runtime(store, plugin()).expect("runtime opens");
        assert!(
            runtime
                .start(nested_effect_candidate(), &json!({"value": index}), &run_id,)
                .is_err()
        );
        assert!(
            lost.load(Ordering::SeqCst),
            "receipt loss stage {stage:?} was not reached"
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);

        let (store, _) = runtime.into_parts();
        let mut reopened = open_runtime(store, plugin()).expect("runtime reopens");
        let durable = reopened.coordinator().state().expect("state");
        let dispatch = durable.outbox.values().next().expect("staged effect");
        assert_eq!(dispatch.state, OutboxState::Pending);
        let machine = reopened
            .coordinator()
            .restore_machine()
            .expect("machine restores");
        let effect = &machine.projection().runs[&run_id].effects[&dispatch.intent_id];
        let effect_scope = &machine.projection().runs[&run_id].scopes[&effect.scope_id];
        assert_eq!(
            effect_scope.status,
            if matches!(stage, ReceiptLossStage::Enqueue) {
                cymule_core::ScopeStatus::Open
            } else {
                cymule_core::ScopeStatus::ClosedCommitted
            }
        );

        let DriveOutcome::Completed(result) = reopened
            .resume(&run_id)
            .expect("nested effect recovery completes")
        else {
            panic!("nested effect Run should complete");
        };
        assert_eq!(result.value, json!({"value": index}));
        assert_eq!(prepares.load(Ordering::SeqCst), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn eager_observation_binds_result_before_scope_commit_across_receipt_loss() {
    let cases = [
        (
            ReceiptLossStage::Claim,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            0,
            1,
        ),
        (
            ReceiptLossStage::Applied,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            1,
            0,
        ),
        (
            ReceiptLossStage::Unknown,
            WorldOutcome::Unknown,
            ReconciliationResolution::ResolvedApplied,
            1,
            1,
        ),
        (
            ReceiptLossStage::EagerBinding,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            1,
            0,
        ),
    ];
    for (
        index,
        (stage, dispatch_outcome, reconciliation, expected_dispatches, expected_reconciliations),
    ) in cases.into_iter().enumerate()
    {
        let lost = Arc::new(AtomicBool::new(false));
        let store = StageReceiptLossStore {
            inner: MemoryStore::new(),
            stage,
            lost: lost.clone(),
        };
        let prepares = Arc::new(AtomicUsize::new(0));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let reconciliations = Arc::new(AtomicUsize::new(0));
        let plugin = || StagePlugin {
            prepares: prepares.clone(),
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            dispatch_outcome,
            reconciliation,
            lose_first_prepare_response: false,
        };
        let run_id = format!("run:eager-receipt-loss:{index}");
        let input = json!({"value": index});
        let mut runtime = open_runtime(store, plugin()).expect("runtime opens");
        let error = runtime
            .start(eager_effect_candidate(), &input, &run_id)
            .expect_err("injected eager receipt loss stops the Run");
        assert!(
            lost.load(Ordering::SeqCst),
            "eager receipt loss stage {stage:?} was not reached; got {error:?}"
        );

        let (store, _) = runtime.into_parts();
        let mut reopened = open_runtime(store, plugin()).expect("runtime reopens");
        let machine = reopened
            .coordinator()
            .restore_machine()
            .expect("machine restores");
        assert_eq!(
            machine.projection().runs[&run_id].scopes[cymule_core::ROOT_SCOPE_ID].status,
            cymule_core::ScopeStatus::Open
        );
        let DriveOutcome::Completed(result) = reopened
            .resume(&run_id)
            .expect("eager observation recovery completes")
        else {
            panic!("eager observation should complete");
        };
        assert_eq!(result.value, input);
        assert_eq!(prepares.load(Ordering::SeqCst), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), expected_dispatches);
        assert_eq!(
            reconciliations.load(Ordering::SeqCst),
            expected_reconciliations
        );
    }
}

#[test]
fn explicit_effect_waits_for_release_and_replays_release_after_receipt_loss() {
    let cases = [
        (
            ReceiptLossStage::Claim,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            0,
            1,
        ),
        (
            ReceiptLossStage::Applied,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            1,
            0,
        ),
        (
            ReceiptLossStage::RunComplete,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            1,
            0,
        ),
    ];
    for (
        index,
        (stage, dispatch_outcome, reconciliation, expected_dispatches, expected_reconciliations),
    ) in cases.into_iter().enumerate()
    {
        let lost = Arc::new(AtomicBool::new(false));
        let store = StageReceiptLossStore {
            inner: MemoryStore::new(),
            stage,
            lost: lost.clone(),
        };
        let prepares = Arc::new(AtomicUsize::new(0));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let reconciliations = Arc::new(AtomicUsize::new(0));
        let plugin = || StagePlugin {
            prepares: prepares.clone(),
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            dispatch_outcome,
            reconciliation,
            lose_first_prepare_response: false,
        };
        let run_id = format!("run:explicit-receipt-loss:{index}");
        let input = json!({"value": index});
        let mut runtime = open_runtime(store, plugin()).expect("runtime opens");
        let DriveOutcome::ReleaseRequired { intent_ids } = runtime
            .start(explicit_effect_candidate(), &input, &run_id)
            .expect("explicit effect stages")
        else {
            panic!("explicit effect should await release");
        };
        assert_eq!(intent_ids.len(), 1);
        let intent_id = intent_ids.into_iter().next().expect("intent");
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert!(matches!(
            runtime.resume(&run_id).expect("resume remains stable"),
            DriveOutcome::ReleaseRequired { .. }
        ));
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert!(runtime.release_effect(&intent_id).is_err());
        assert!(lost.load(Ordering::SeqCst));

        let (store, _) = runtime.into_parts();
        let mut reopened = open_runtime(store, plugin()).expect("runtime reopens");
        let DriveOutcome::Completed(result) = reopened
            .release_effect(&intent_id)
            .expect("explicit release recovery completes")
        else {
            panic!("released effect should complete");
        };
        assert_eq!(result.value, input);
        assert_eq!(prepares.load(Ordering::SeqCst), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), expected_dispatches);
        assert_eq!(
            reconciliations.load(Ordering::SeqCst),
            expected_reconciliations
        );
        let DriveOutcome::Completed(replayed) = reopened
            .release_effect(&intent_id)
            .expect("completed release replays")
        else {
            panic!("completed release should replay its Result");
        };
        assert_eq!(replayed, result);
        assert_eq!(dispatches.load(Ordering::SeqCst), expected_dispatches);
    }
}

#[test]
fn identified_signal_and_timer_activations_resume_after_process_reopen() {
    let cases = [
        (
            "run:signal-activation",
            external_wait_candidate(
                "signal_activation",
                WaitSpec::Signal {
                    key: "signal:continue".to_owned(),
                    consume_once: true,
                },
            ),
            WaitActivationSource::Signal {
                key: "signal:continue".to_owned(),
            },
        ),
        (
            "run:timer-activation",
            external_wait_candidate(
                "timer_activation",
                WaitSpec::Timer {
                    timer_id: "timer:continue".to_owned(),
                },
            ),
            WaitActivationSource::Timer {
                timer_id: "timer:continue".to_owned(),
            },
        ),
    ];

    for (run_id, candidate, source) in cases {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut runtime = open_runtime(
            MemoryStore::new(),
            CountingPlugin {
                calls: calls.clone(),
            },
        )
        .expect("runtime opens");
        let DriveOutcome::Suspended { wait_id } = runtime
            .start(candidate, &json!({"case": run_id}), run_id)
            .expect("run reaches external wait")
        else {
            panic!("run should suspend");
        };
        let ready_runs = runtime
            .admit_wait_activation(
                format!("activation:{run_id}"),
                source.clone(),
                BTreeSet::from([wait_id.clone()]),
                &json!({"delivered": true}),
            )
            .expect("activation commits");
        assert_eq!(ready_runs, BTreeSet::from([run_id.to_owned()]));

        let (store, _) = runtime.into_parts();
        let mut reopened = open_runtime(
            store,
            CountingPlugin {
                calls: calls.clone(),
            },
        )
        .expect("runtime reopens");
        let DriveOutcome::Completed(result) =
            reopened.resume(run_id).expect("activated run resumes")
        else {
            panic!("activated run should complete");
        };
        assert_eq!(result.value, json!({"case": run_id}));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            reopened
                .admit_wait_activation(
                    format!("activation:{run_id}"),
                    source,
                    BTreeSet::from([wait_id]),
                    &json!({"delivered": true}),
                )
                .expect("completed activation redelivery is retained")
                .is_empty()
        );
    }
}

#[test]
fn crash_after_provider_application_reconciles_without_redispatch() {
    let dispatches = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        MemoryStore::new(),
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: true,
            unknown_reconciliations: 0,
        },
    )
    .expect("runtime opens");
    assert!(
        runtime
            .start(
                effect_candidate(),
                &json!({"value": 1}),
                "run:effect-recovery"
            )
            .is_err()
    );
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);

    let (store, _) = runtime.into_parts();
    let mut reopened = open_runtime(
        store,
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: false,
            unknown_reconciliations: 0,
        },
    )
    .expect("runtime reopens");
    let DriveOutcome::Completed(result) = reopened
        .resume("run:effect-recovery")
        .expect("recovery reconciles and completes")
    else {
        panic!("recovered Run should complete");
    };
    assert_eq!(result.value, json!({"value": 1}));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 1);
}

#[test]
fn recovery_survives_lost_unknown_receipt_after_provider_crash() {
    let lost = Arc::new(AtomicBool::new(false));
    let store = StageReceiptLossStore {
        inner: MemoryStore::new(),
        stage: ReceiptLossStage::Unknown,
        lost: lost.clone(),
    };
    let dispatches = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        store,
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: true,
            unknown_reconciliations: 0,
        },
    )
    .expect("runtime opens");
    assert!(
        runtime
            .start(
                effect_candidate(),
                &json!({"value": "compound-fault"}),
                "run:compound-effect-recovery",
            )
            .is_err()
    );
    assert!(!lost.load(Ordering::SeqCst));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);

    let (store, _) = runtime.into_parts();
    let mut first_recovery = open_runtime(
        store,
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: false,
            unknown_reconciliations: 0,
        },
    )
    .expect("first recovery opens");
    assert!(
        first_recovery
            .resume("run:compound-effect-recovery")
            .is_err()
    );
    assert!(lost.load(Ordering::SeqCst));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);

    let (store, _) = first_recovery.into_parts();
    let mut second_recovery = open_runtime(
        store,
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: false,
            unknown_reconciliations: 0,
        },
    )
    .expect("second recovery opens");
    let DriveOutcome::Completed(result) = second_recovery
        .resume("run:compound-effect-recovery")
        .expect("second recovery reconciles")
    else {
        panic!("compound recovery should complete");
    };
    assert_eq!(result.value, json!({"value": "compound-fault"}));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 1);
}

#[test]
fn unknown_outbox_reconciles_after_another_process_reopen_without_redispatch() {
    let dispatches = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        MemoryStore::new(),
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: true,
            unknown_reconciliations: 1,
        },
    )
    .expect("runtime opens");
    assert!(
        runtime
            .start(
                effect_candidate(),
                &json!({"value": 2}),
                "run:repeated-effect-recovery",
            )
            .is_err()
    );
    let (store, _) = runtime.into_parts();
    let mut first_recovery = open_runtime(
        store,
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: false,
            unknown_reconciliations: 1,
        },
    )
    .expect("first recovery opens");
    assert!(matches!(
        first_recovery
            .resume("run:repeated-effect-recovery")
            .expect("first reconciliation is durable"),
        DriveOutcome::ReconciliationRequired { .. }
    ));
    assert!(
        first_recovery
            .coordinator()
            .state()
            .expect("state")
            .outbox
            .values()
            .any(|dispatch| dispatch.state == cymule_durable::OutboxState::Unknown)
    );

    let (store, _) = first_recovery.into_parts();
    let mut second_recovery = open_runtime(
        store,
        CrashAfterApplyPlugin {
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            crash_after_apply: false,
            unknown_reconciliations: 1,
        },
    )
    .expect("second recovery opens");
    let DriveOutcome::Completed(result) = second_recovery
        .resume("run:repeated-effect-recovery")
        .expect("second reconciliation completes")
    else {
        panic!("reconciled Run should complete");
    };
    assert_eq!(result.value, json!({"value": 2}));
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 2);
}

#[test]
fn effect_stage_receipt_loss_reopens_without_duplicate_world_effects() {
    let cases = [
        (
            ReceiptLossStage::Enqueue,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            1,
            0,
        ),
        (
            ReceiptLossStage::Claim,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedNotApplied,
            0,
            1,
        ),
        (
            ReceiptLossStage::ScopeCommit,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            1,
            0,
        ),
        (
            ReceiptLossStage::Applied,
            WorldOutcome::Applied,
            ReconciliationResolution::ResolvedApplied,
            1,
            0,
        ),
        (
            ReceiptLossStage::Unknown,
            WorldOutcome::Unknown,
            ReconciliationResolution::ResolvedApplied,
            1,
            1,
        ),
    ];
    for (
        index,
        (stage, dispatch_outcome, reconciliation, expected_dispatches, expected_reconciliations),
    ) in cases.into_iter().enumerate()
    {
        let lost = Arc::new(AtomicBool::new(false));
        let store = StageReceiptLossStore {
            inner: MemoryStore::new(),
            stage,
            lost: lost.clone(),
        };
        let prepares = Arc::new(AtomicUsize::new(0));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let reconciliations = Arc::new(AtomicUsize::new(0));
        let plugin = || StagePlugin {
            prepares: prepares.clone(),
            dispatches: dispatches.clone(),
            reconciliations: reconciliations.clone(),
            dispatch_outcome,
            reconciliation,
            lose_first_prepare_response: false,
        };
        let run_id = format!("run:effect-stage-receipt-loss:{index}");
        let mut runtime = open_runtime(store.clone(), plugin()).expect("runtime opens");
        assert!(
            runtime
                .start(effect_candidate(), &json!({"value": index}), &run_id)
                .is_err()
        );
        assert!(lost.load(Ordering::SeqCst));
        let (store, _) = runtime.into_parts();
        let mut reopened = open_runtime(store, plugin()).expect("runtime reopens");
        let DriveOutcome::Completed(result) = reopened
            .resume(&run_id)
            .expect("receipt-loss recovery completes")
        else {
            panic!("receipt-loss recovery should complete");
        };
        assert_eq!(result.value, json!({"value": index}));
        assert_eq!(prepares.load(Ordering::SeqCst), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), expected_dispatches);
        assert_eq!(
            reconciliations.load(Ordering::SeqCst),
            expected_reconciliations
        );
    }
}

#[test]
fn lost_prepare_response_reuses_the_same_intent_before_dispatch() {
    let prepares = Arc::new(AtomicUsize::new(0));
    let dispatches = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let plugin = || StagePlugin {
        prepares: prepares.clone(),
        dispatches: dispatches.clone(),
        reconciliations: reconciliations.clone(),
        dispatch_outcome: WorldOutcome::Applied,
        reconciliation: ReconciliationResolution::ResolvedApplied,
        lose_first_prepare_response: true,
    };
    let mut runtime = open_runtime(MemoryStore::new(), plugin()).expect("runtime opens");
    assert!(
        runtime
            .start(
                effect_candidate(),
                &json!({"value": "prepared-once"}),
                "run:lost-prepare-response",
            )
            .is_err()
    );
    assert_eq!(prepares.load(Ordering::SeqCst), 1);
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    let (store, _) = runtime.into_parts();

    let mut reopened = open_runtime(store, plugin()).expect("runtime reopens");
    let DriveOutcome::Completed(result) = reopened
        .resume("run:lost-prepare-response")
        .expect("idempotent prepare retry completes")
    else {
        panic!("prepare retry should complete");
    };
    assert_eq!(result.value, json!({"value": "prepared-once"}));
    assert_eq!(prepares.load(Ordering::SeqCst), 2);
    assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(reconciliations.load(Ordering::SeqCst), 0);
}

#[test]
fn reopen_preserves_the_historical_execution_binding_artifact() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = open_runtime(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime opens");
    let DriveOutcome::Suspended { .. } = runtime
        .start(candidate(), &json!({"name": "Cymule"}), "run:binding-pin")
        .expect("Run suspends")
    else {
        panic!("Run must suspend at its input wait")
    };
    let machine = runtime
        .coordinator()
        .restore_machine()
        .expect("Machine restores");
    let continuation = runtime
        .coordinator()
        .state()
        .expect("state exists")
        .continuations["run:binding-pin"]
        .clone();
    let reference = cymule_core::ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: continuation.binding_context.clone(),
        kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
    };
    assert!(machine.artifact(&reference).is_some());
    let run = &machine.projection().runs["run:binding-pin"];
    assert_eq!(run.initial_binding_context, reference.artifact_id);
    assert_eq!(run.current_binding_context, reference.artifact_id);
    assert_eq!(
        run.attempts.values().next().unwrap().occurrence_binding,
        reference.artifact_id
    );

    let (store, _) = runtime.into_parts();
    let mut changed_plugin = CountingPlugin {
        calls: calls.clone(),
    };
    let manifest = changed_plugin.describe().expect("plugin describes");
    let changed_binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:5555555555555555555555555555555555555555555555555555555555555555",
    )
    .expect("changed binding is independently valid");
    let mut changed_runtime =
        ResumableRuntime::open(store, changed_plugin, changed_binding).expect("domain reopens");
    assert!(matches!(
        changed_runtime.resume("run:binding-pin"),
        Err(DurableError::IllegalTransition(message)) if message.contains("is pinned")
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
