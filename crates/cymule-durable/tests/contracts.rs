//! Durable executor contract-boundary conformance.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cymule_core::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    IR_VERSION, MutationKind, Operation, PlanCandidate, ReconciliationMode, Region, Step, WaitSpec,
    WorldOutcome,
};
use cymule_durable::{
    DriveOutcome, DurableError, DurableStore, MemoryStore, OutboxState, ResumableRuntime, WaitState,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation,
    PluginRequest, PluginResponse, RuntimeResult,
};
use serde_json::{Value, json};

#[derive(Default)]
struct Counts {
    calls: AtomicUsize,
    prepares: AtomicUsize,
    dispatches: AtomicUsize,
}

struct Plugin {
    counts: Arc<Counts>,
    component_output: Value,
    effect_output: Value,
}

impl PluginHost for Plugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "durable-contract-plugin@1".to_owned(),
                    components: BTreeMap::from([(
                        "example.component".to_owned(),
                        PluginOperation {
                            implementation_revision: "1".to_owned(),
                        },
                    )]),
                    effects: BTreeMap::from([(
                        "example.effect".to_owned(),
                        PluginEffect {
                            implementation_revision: "1".to_owned(),
                            can_reconcile: true,
                        },
                    )]),
                },
            }),
            PluginRequest::Call { .. } => {
                self.counts.calls.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::CallResult {
                    value: self.component_output.clone(),
                })
            }
            PluginRequest::PrepareEffect { .. } => {
                self.counts.prepares.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::DispatchEffect { .. } => {
                self.counts.dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::EffectResult {
                    outcome: WorldOutcome::Applied,
                    value: Some(self.effect_output.clone()),
                })
            }
            PluginRequest::ReconcileEffect { .. } => {
                panic!("test effect never becomes ambiguous")
            }
        }
    }
}

fn runtime(
    store: MemoryStore,
    counts: Arc<Counts>,
    component_output: Value,
    effect_output: Value,
) -> ResumableRuntime<MemoryStore, Plugin> {
    let mut plugin = Plugin {
        counts,
        component_output,
        effect_output,
    };
    let manifest = plugin.describe().expect("test plugin describes");
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .expect("test binding is admitted");
    ResumableRuntime::open(store, plugin, binding).expect("runtime opens")
}

fn base_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: IR_VERSION.to_owned(),
        name: "durable_contract".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["request"],
                "properties": {"request": {"type": "string"}},
                "additionalProperties": false
            }),
            output_schema: json!({}),
            body: Region {
                steps: Vec::new(),
                result: Expression::Literal { value: Value::Null },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn component_candidate(input: Value, output: Value, argument: Value) -> PlanCandidate {
    let mut candidate = base_candidate();
    candidate.components.push(ComponentContract {
        id: "example.component".to_owned(),
        input_schema: input,
        output_schema: output,
        requirements: BTreeMap::new(),
    });
    candidate.definitions[0].body.steps.push(Step {
        id: "call.component".to_owned(),
        operation: Operation::Call {
            component: "example.component".to_owned(),
            input: Expression::Literal { value: argument },
            bind: Some("component_result".to_owned()),
        },
    });
    candidate.definitions[0].body.result = Expression::Binding {
        name: "component_result".to_owned(),
    };
    candidate
}

fn wait_candidate() -> PlanCandidate {
    let mut candidate = base_candidate();
    candidate.definitions[0].output_schema = json!({"type": "null"});
    candidate.definitions[0].body.steps.push(Step {
        id: "wait.approval".to_owned(),
        operation: Operation::Wait {
            wait: WaitSpec::Input {
                correlation: "approval".to_owned(),
                schema: json!({
                    "type": "object",
                    "required": ["approved"],
                    "properties": {"approved": {"type": "boolean"}},
                    "additionalProperties": false
                }),
            },
            bind: Some("approval".to_owned()),
        },
    });
    candidate
}

fn effect_candidate() -> PlanCandidate {
    let mut candidate = base_candidate();
    candidate.definitions[0].output_schema = json!({"type": "boolean"});
    candidate.effects.push(EffectContract {
        id: "example.effect".to_owned(),
        input_schema: json!({"type": "integer"}),
        output_schema: json!({"type": "string"}),
        profile: EffectProfile {
            mutation: MutationKind::Observational,
            dispatch: DispatchPolicy::Eager,
            reconciliation: ReconciliationMode::Queryable,
            keyed_idempotency: true,
            irreversible: false,
        },
        requirements: BTreeMap::new(),
    });
    candidate.definitions[0].body.steps.push(Step {
        id: "effect.observe".to_owned(),
        operation: Operation::Effect {
            effect: "example.effect".to_owned(),
            input: Expression::Literal { value: json!(7) },
            occurrence: "primary".to_owned(),
            bind: Some("effect_result".to_owned()),
        },
    });
    candidate.definitions[0].body.result = Expression::Literal { value: json!(true) };
    candidate
}

fn invocation_candidate() -> PlanCandidate {
    let mut candidate = base_candidate();
    candidate.definitions.push(Definition {
        id: "worker".to_owned(),
        input_schema: json!({"type": "integer"}),
        output_schema: json!({"type": "null"}),
        body: Region {
            steps: Vec::new(),
            result: Expression::Literal { value: Value::Null },
        },
    });
    candidate.definitions[0].body.steps.push(Step {
        id: "invoke.worker".to_owned(),
        operation: Operation::Invoke {
            definition: "worker".to_owned(),
            input: Expression::Literal {
                value: json!("bad"),
            },
            bind: Some("worker_result".to_owned()),
        },
    });
    candidate
}

#[test]
fn invalid_run_input_does_not_create_durable_state() {
    let store = MemoryStore::new();
    let mut readback = store.clone();
    let counts = Arc::new(Counts::default());
    let mut runtime = runtime(store, counts.clone(), json!("ok"), json!("ok"));

    let error = runtime
        .start(base_candidate(), &json!({"request": 7}), "run:bad-input")
        .expect_err("entry input must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert!(readback.load().expect("store loads").is_none());
    assert_eq!(counts.calls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.prepares.load(Ordering::SeqCst), 0);
}

#[test]
fn component_contract_failures_do_not_create_occurrences() {
    let input_counts = Arc::new(Counts::default());
    let mut input_runtime = runtime(
        MemoryStore::new(),
        input_counts.clone(),
        json!("ok"),
        json!("ok"),
    );
    let error = input_runtime
        .start(
            component_candidate(json!({"type": "integer"}), json!({}), json!("bad")),
            &json!({"request": "run"}),
            "run:bad-component-input",
        )
        .expect_err("component input must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert_eq!(input_counts.calls.load(Ordering::SeqCst), 0);
    assert!(
        input_runtime
            .coordinator()
            .state()
            .expect("state loads")
            .component_occurrences
            .is_empty()
    );

    let output_counts = Arc::new(Counts::default());
    let mut output_runtime = runtime(
        MemoryStore::new(),
        output_counts.clone(),
        json!(7),
        json!("ok"),
    );
    let error = output_runtime
        .start(
            component_candidate(
                json!({"type": "integer"}),
                json!({"type": "string"}),
                json!(7),
            ),
            &json!({"request": "run"}),
            "run:bad-component-output",
        )
        .expect_err("component output must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert_eq!(output_counts.calls.load(Ordering::SeqCst), 1);
    assert!(
        output_runtime
            .coordinator()
            .state()
            .expect("state loads")
            .component_occurrences
            .is_empty()
    );
}

#[test]
fn invalid_wait_completion_does_not_advance_revision_or_write_result() {
    let counts = Arc::new(Counts::default());
    let mut runtime = runtime(MemoryStore::new(), counts, json!("ok"), json!("ok"));
    let DriveOutcome::Suspended { wait_id } = runtime
        .start(wait_candidate(), &json!({"request": "run"}), "run:wait")
        .expect("Run parks")
    else {
        panic!("Run must suspend")
    };
    let revision = runtime.coordinator().revision().map(str::to_owned);
    let artifact_count = runtime
        .coordinator()
        .restore_machine()
        .expect("Machine restores")
        .snapshot()
        .artifacts
        .len();

    let error = runtime
        .complete_wait(&wait_id, &json!({"approved": "yes"}))
        .expect_err("wait result must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert_eq!(runtime.coordinator().revision(), revision.as_deref());
    assert_eq!(
        runtime
            .coordinator()
            .restore_machine()
            .expect("Machine restores")
            .snapshot()
            .artifacts
            .len(),
        artifact_count
    );
    let wait = &runtime.coordinator().state().expect("state loads").waits[&wait_id];
    assert_eq!(wait.state, WaitState::Pending);
    assert!(wait.result.is_none());
}

#[test]
fn invalid_effect_output_is_never_settled_or_recorded() {
    let counts = Arc::new(Counts::default());
    let mut runtime = runtime(MemoryStore::new(), counts.clone(), json!("ok"), json!(7));
    let error = runtime
        .start(
            effect_candidate(),
            &json!({"request": "run"}),
            "run:effect-output",
        )
        .expect_err("effect output must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert_eq!(counts.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(counts.dispatches.load(Ordering::SeqCst), 1);
    let state = runtime.coordinator().state().expect("state loads");
    let dispatch = state.outbox.values().next().expect("outbox claim exists");
    assert_eq!(dispatch.state, OutboxState::Claimed);
    assert!(dispatch.result.is_none());
}

#[test]
fn definition_and_effect_inputs_fail_before_child_or_effect_state() {
    let invocation_counts = Arc::new(Counts::default());
    let mut invocation_runtime = runtime(
        MemoryStore::new(),
        invocation_counts,
        json!("ok"),
        json!("ok"),
    );
    let error = invocation_runtime
        .start(
            invocation_candidate(),
            &json!({"request": "run"}),
            "run:invoke-input",
        )
        .expect_err("invocation input must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert_eq!(
        invocation_runtime
            .coordinator()
            .state()
            .expect("state loads")
            .continuations["run:invoke-input"]
            .frames
            .len(),
        1
    );

    let effect_counts = Arc::new(Counts::default());
    let mut effect_runtime = runtime(
        MemoryStore::new(),
        effect_counts.clone(),
        json!("ok"),
        json!("ok"),
    );
    let mut candidate = effect_candidate();
    candidate.effects[0].input_schema = json!({"type": "string"});
    let error = effect_runtime
        .start(candidate, &json!({"request": "run"}), "run:effect-input")
        .expect_err("effect input must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert_eq!(effect_counts.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(effect_counts.dispatches.load(Ordering::SeqCst), 0);
    assert!(
        effect_runtime
            .coordinator()
            .state()
            .expect("state loads")
            .outbox
            .is_empty()
    );
}

#[test]
fn invalid_definition_result_never_completes_the_run() {
    let counts = Arc::new(Counts::default());
    let mut runtime = runtime(MemoryStore::new(), counts, json!("ok"), json!("ok"));
    let mut candidate = base_candidate();
    candidate.definitions[0].output_schema = json!({"type": "string"});
    let error = runtime
        .start(candidate, &json!({"request": "run"}), "run:bad-result")
        .expect_err("definition result must fail");
    assert!(matches!(error, DurableError::Contract(_)));
    assert!(
        runtime
            .coordinator()
            .restore_machine()
            .expect("Machine restores")
            .projection()
            .runs["run:bad-result"]
            .result
            .is_none()
    );
}
