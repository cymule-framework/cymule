//! Restart-level resumable interpreter tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cymule_core::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    MutationKind, Operation, PlanCandidate, ReconciliationMode, Region, Step, WaitSpec,
};
use cymule_durable::{DriveOutcome, MemoryStore, ResumableRuntime};
use cymule_runtime::{
    PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
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
                    Err(RuntimeError::Io(
                        "simulated crash after provider application".to_owned(),
                    ))
                } else {
                    Err(RuntimeError::Plugin(
                        "recovery must not redispatch the original intent".to_owned(),
                    ))
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
            request @ PluginRequest::Call { .. } => Err(RuntimeError::Plugin(format!(
                "unsupported effect recovery request: {request:?}"
            ))),
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
            request => Err(RuntimeError::Plugin(format!(
                "unsupported resume test request: {request:?}"
            ))),
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

#[test]
fn process_reopen_resumes_after_wait_without_reinvoking_component() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = ResumableRuntime::open(
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
    let mut reopened = ResumableRuntime::open(
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
fn crash_after_provider_application_reconciles_without_redispatch() {
    let dispatches = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let mut runtime = ResumableRuntime::open(
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
    let mut reopened = ResumableRuntime::open(
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
fn unknown_outbox_reconciles_after_another_process_reopen_without_redispatch() {
    let dispatches = Arc::new(AtomicUsize::new(0));
    let reconciliations = Arc::new(AtomicUsize::new(0));
    let mut runtime = ResumableRuntime::open(
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
    let mut first_recovery = ResumableRuntime::open(
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
    let mut second_recovery = ResumableRuntime::open(
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
