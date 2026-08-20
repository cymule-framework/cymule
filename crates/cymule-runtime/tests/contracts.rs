//! Executable Plan contract compiler conformance.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cymule_core::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    IR_VERSION, MutationKind, Operation, PlanCandidate, ReconciliationMode, Region, ScopeMode,
    Step, WaitSpec, seal_plan,
};
use cymule_runtime::{
    ContractBoundary, ContractPhase, ContractSide, ContractTarget, ContractValidator,
    EmbeddedRuntime, EngineContractSide, EngineFailure, EngineFailureCategory, EnginePhase,
    ExecutionBinding, PLUGIN_VERSION, PlanContracts, PluginEffect, PluginHost, PluginManifest,
    PluginOperation, PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::{Value, json};

fn closed_object(properties: &Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: IR_VERSION.to_owned(),
        name: "contract_boundaries".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "example.component".to_owned(),
            input_schema: closed_object(&json!({"name": {"type": "string"}}), &["name"]),
            output_schema: json!({"type": "integer"}),
            requirements: BTreeMap::new(),
        }],
        effects: vec![EffectContract {
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
        }],
        definitions: vec![
            Definition {
                id: "main".to_owned(),
                input_schema: closed_object(&json!({"request": {"type": "string"}}), &["request"]),
                output_schema: json!({"type": "boolean"}),
                body: Region {
                    steps: vec![
                        Step {
                            id: "call.component".to_owned(),
                            operation: Operation::Call {
                                component: "example.component".to_owned(),
                                input: Expression::Object {
                                    fields: BTreeMap::from([(
                                        "name".to_owned(),
                                        Expression::Literal {
                                            value: json!("Cymule"),
                                        },
                                    )]),
                                },
                                bind: Some("component_result".to_owned()),
                            },
                        },
                        Step {
                            id: "invoke.worker".to_owned(),
                            operation: Operation::Invoke {
                                definition: "worker".to_owned(),
                                input: Expression::Array {
                                    items: vec![Expression::Literal { value: json!(1) }],
                                },
                                bind: Some("worker_result".to_owned()),
                            },
                        },
                        Step {
                            id: "scope.wait".to_owned(),
                            operation: Operation::Scope {
                                mode: ScopeMode::Transactional,
                                body: Box::new(Region {
                                    steps: vec![Step {
                                        id: "wait.approval".to_owned(),
                                        operation: Operation::Wait {
                                            wait: WaitSpec::Input {
                                                correlation: "approval".to_owned(),
                                                schema: closed_object(
                                                    &json!({"approved": {"const": true}}),
                                                    &["approved"],
                                                ),
                                            },
                                            bind: Some("approval".to_owned()),
                                        },
                                    }],
                                    result: Expression::Literal { value: Value::Null },
                                }),
                                bind: Some("wait_scope".to_owned()),
                            },
                        },
                        Step {
                            id: "effect.observe".to_owned(),
                            operation: Operation::Effect {
                                effect: "example.effect".to_owned(),
                                input: Expression::Literal { value: json!(7) },
                                occurrence: "primary".to_owned(),
                                bind: Some("effect_result".to_owned()),
                            },
                        },
                    ],
                    result: Expression::Literal { value: json!(true) },
                },
            },
            Definition {
                id: "worker".to_owned(),
                input_schema: json!({
                    "type": "array",
                    "prefixItems": [{"type": "integer"}],
                    "items": false
                }),
                output_schema: closed_object(&json!({"done": {"const": true}}), &["done"]),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Object {
                        fields: BTreeMap::from([(
                            "done".to_owned(),
                            Expression::Literal { value: json!(true) },
                        )]),
                    },
                },
            },
        ],
        metadata: BTreeMap::new(),
    }
}

#[derive(Default)]
struct PluginCounts {
    describe: AtomicUsize,
    call: AtomicUsize,
    prepare: AtomicUsize,
    dispatch: AtomicUsize,
}

struct ContractPlugin {
    counts: Arc<PluginCounts>,
    component_output: Value,
    effect_output: Value,
}

fn plugin_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "contract-plugin@1".to_owned(),
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
    }
}

impl PluginHost for ContractPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => {
                self.counts.describe.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::Manifest {
                    manifest: plugin_manifest(),
                })
            }
            PluginRequest::Call { .. } => {
                self.counts.call.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::CallResult {
                    value: self.component_output.clone(),
                })
            }
            PluginRequest::PrepareEffect { .. } => {
                self.counts.prepare.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::DispatchEffect { .. } => {
                self.counts.dispatch.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::EffectResult {
                    outcome: cymule_core::WorldOutcome::Applied,
                    value: Some(self.effect_output.clone()),
                })
            }
            PluginRequest::ReconcileEffect { .. } => {
                panic!("test effects do not become ambiguous")
            }
        }
    }
}

fn runtime(
    counts: Arc<PluginCounts>,
    component_output: Value,
    effect_output: Value,
) -> EmbeddedRuntime<ContractPlugin> {
    let binding = ExecutionBinding::for_local_process(
        &plugin_manifest(),
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .expect("test binding is admitted");
    EmbeddedRuntime::new(
        ContractPlugin {
            counts,
            component_output,
            effect_output,
        },
        binding,
    )
    .expect("runtime opens with exact binding")
}

fn effect_only_candidate(input: Value, output: Value) -> PlanCandidate {
    let mut plan = candidate();
    plan.components.clear();
    plan.effects[0].input_schema = input;
    plan.effects[0].output_schema = output;
    plan.definitions
        .retain(|definition| definition.id == "main");
    plan.definitions[0].body.steps = vec![Step {
        id: "effect.observe".to_owned(),
        operation: Operation::Effect {
            effect: "example.effect".to_owned(),
            input: Expression::Literal { value: json!(7) },
            occurrence: "primary".to_owned(),
            bind: Some("effect_result".to_owned()),
        },
    }];
    plan
}

#[test]
fn compiles_every_plan_contract_under_draft_2020_12() {
    let contracts = PlanContracts::compile(&candidate()).expect("all schemas compile");

    contracts
        .validate_definition_input("main", &json!({"request": "run"}))
        .expect("entry input validates");
    contracts
        .validate_definition_input("worker", &json!([1]))
        .expect("prefixItems has Draft 2020-12 semantics");
    contracts
        .validate_definition_output("worker", &json!({"done": true}))
        .expect("definition output validates");
    contracts
        .validate_component_input("example.component", &json!({"name": "Cymule"}))
        .expect("component input validates");
    contracts
        .validate_component_output("example.component", &json!(42))
        .expect("component output validates");
    contracts
        .validate_effect_input("example.effect", &json!(42))
        .expect("effect input validates");
    contracts
        .validate_effect_output("example.effect", &json!("observed"))
        .expect("effect output validates");
    contracts
        .validate_wait_input("wait.approval", &json!({"approved": true}))
        .expect("nested typed wait validates");
}

#[test]
fn malformed_schemas_fail_admission_at_the_exact_boundary() {
    let cases = [
        (ContractBoundary::Definition, "main", ContractSide::Input, 0),
        (
            ContractBoundary::Definition,
            "main",
            ContractSide::Output,
            1,
        ),
        (
            ContractBoundary::Component,
            "example.component",
            ContractSide::Input,
            2,
        ),
        (
            ContractBoundary::Component,
            "example.component",
            ContractSide::Output,
            3,
        ),
        (
            ContractBoundary::Effect,
            "example.effect",
            ContractSide::Input,
            4,
        ),
        (
            ContractBoundary::Effect,
            "example.effect",
            ContractSide::Output,
            5,
        ),
        (
            ContractBoundary::Wait,
            "wait.approval",
            ContractSide::Input,
            6,
        ),
    ];

    for (boundary, id, side, selector) in cases {
        let mut plan = candidate();
        let malformed = json!({"type": 42});
        match selector {
            0 => plan.definitions[0].input_schema = malformed,
            1 => plan.definitions[0].output_schema = malformed,
            2 => plan.components[0].input_schema = malformed,
            3 => plan.components[0].output_schema = malformed,
            4 => plan.effects[0].input_schema = malformed,
            5 => plan.effects[0].output_schema = malformed,
            6 => {
                let Operation::Scope { body, .. } =
                    &mut plan.definitions[0].body.steps[2].operation
                else {
                    panic!("fixture owns a nested scope")
                };
                let Operation::Wait {
                    wait: WaitSpec::Input { schema, .. },
                    ..
                } = &mut body.steps[0].operation
                else {
                    panic!("fixture owns a typed wait")
                };
                *schema = malformed;
            }
            _ => unreachable!(),
        }
        let Err(error) = PlanContracts::compile(&plan) else {
            panic!("malformed schema must fail admission")
        };
        assert_eq!(error.phase, ContractPhase::Admission);
        assert_eq!(error.target.boundary, boundary);
        assert_eq!(error.target.id, id);
        assert_eq!(error.target.side, side);
        assert!(!error.issues.is_empty());
    }
}

#[test]
fn external_references_are_rejected_without_resolution() {
    for keyword in ["$ref", "$dynamicRef"] {
        let schema = json!({
            "$defs": {
                "nested": {
                    keyword: "https://schemas.example.invalid/ambient.json"
                }
            },
            "$ref": "#/$defs/nested"
        });
        let Err(error) = ContractValidator::compile(
            ContractTarget {
                boundary: ContractBoundary::Definition,
                id: "external".to_owned(),
                side: ContractSide::Input,
            },
            &schema,
        ) else {
            panic!("external reference must fail admission")
        };
        assert_eq!(error.phase, ContractPhase::Admission);
        assert!(error.issues[0].message.contains("retriev"), "{error:?}");
    }
}

#[test]
fn schema_dialect_is_exact_and_ref_shaped_instance_data_remains_legal() {
    let target = ContractTarget {
        boundary: ContractBoundary::Definition,
        id: "dialect".to_owned(),
        side: ContractSide::Input,
    };
    let wrong_draft = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "string"
    });
    let error = ContractValidator::compile(target.clone(), &wrong_draft)
        .err()
        .expect("another dialect must fail admission");
    assert_eq!(error.phase, ContractPhase::Admission);
    assert_eq!(error.issues[0].instance_path, "/$schema");

    let data_schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "const": {"$ref": "ordinary application data"}
    });
    ContractValidator::compile(target, &data_schema)
        .expect("a ref-shaped field inside const is instance data")
        .validate(&json!({"$ref": "ordinary application data"}))
        .expect("const instance validates");
}

#[test]
fn violations_are_structured_masked_and_reject_unknown_instance_fields() {
    let contracts = PlanContracts::compile(&candidate()).expect("schemas compile");
    let error = contracts
        .validate_component_input(
            "example.component",
            &json!({"name": 7, "secret_unknown": "must-not-appear"}),
        )
        .expect_err("type and additional property both violate the contract");

    assert_eq!(error.phase, ContractPhase::Execution);
    assert_eq!(error.target.boundary, ContractBoundary::Component);
    assert_eq!(error.target.id, "example.component");
    assert_eq!(error.target.side, ContractSide::Input);
    assert!(error.issues.len() >= 2);
    assert!(
        error
            .issues
            .iter()
            .any(|issue| issue.instance_path == "/name")
    );
    assert!(
        error
            .issues
            .iter()
            .all(|issue| !issue.message.contains("must-not-appear"))
    );
}

#[test]
fn missing_contract_selection_fails_closed_with_a_typed_target() {
    let contracts = PlanContracts::compile(&candidate()).expect("schemas compile");
    let error = contracts
        .validate_definition_output("missing", &Value::Null)
        .expect_err("unknown target must fail");

    assert_eq!(error.phase, ContractPhase::Execution);
    assert_eq!(error.target.boundary, ContractBoundary::Definition);
    assert_eq!(error.target.id, "missing");
    assert_eq!(error.target.side, ContractSide::Output);
    assert_eq!(error.issues.len(), 1);
}

#[test]
fn compilation_never_normalizes_or_weakens_plan_identity() {
    let original = candidate();
    let before = original.clone();
    PlanContracts::compile(&original).expect("schemas compile");
    assert_eq!(original, before);

    let first = seal_plan(original).expect("original Plan seals");
    let mut changed = candidate();
    changed.components[0].input_schema["properties"]["name"]["minLength"] = json!(1);
    PlanContracts::compile(&changed).expect("changed schema compiles");
    let second = seal_plan(changed).expect("changed Plan seals");
    assert_ne!(first.plan_id, second.plan_id);
}

#[test]
fn embedded_wait_returns_typed_boundary_without_a_continuation() {
    let plan = seal_plan(candidate()).expect("Plan seals");
    let outcome = runtime(Arc::new(PluginCounts::default()), json!(1), json!("ok"))
        .execute(plan.clone(), &json!({"request": "run"}), "run:suspended")
        .expect("Embedded execution reaches a semantic boundary");
    let cymule_runtime::ExecutionOutcome::Suspended { suspension } = outcome else {
        panic!("wait must return a typed suspension")
    };
    assert_eq!(suspension.run_id, "run:suspended");
    assert_eq!(suspension.plan_id, plan.plan_id);
    assert_eq!(suspension.definition_id, "main");
    assert_eq!(suspension.site_id, "wait.approval");
    assert_eq!(suspension.result_bind.as_deref(), Some("approval"));
    assert!(
        !serde_json::to_value(suspension)
            .expect("boundary serializes")
            .as_object()
            .expect("boundary is an object")
            .contains_key("continuation")
    );
}

#[test]
fn invalid_run_input_has_zero_plugin_calls_and_zero_machine_mutation() {
    let plan = seal_plan(candidate()).expect("Plan admits");
    let counts = Arc::new(PluginCounts::default());
    let mut runtime = runtime(counts.clone(), json!(42), json!(42));
    let error = runtime
        .execute(plan, &json!({"request": 7}), "run:invalid-input")
        .expect_err("Run input must fail its entry contract");

    assert!(matches!(error, RuntimeError::Contract(_)));
    assert_eq!(counts.describe.load(Ordering::SeqCst), 1);
    assert_eq!(counts.call.load(Ordering::SeqCst), 0);
    let snapshot = runtime.machine().snapshot();
    assert!(snapshot.plans.is_empty());
    assert!(snapshot.artifacts.is_empty());
    assert!(snapshot.events.is_empty());
}

#[test]
fn unmet_plan_requirements_are_rejected_before_run_creation_or_dispatch() {
    let mut candidate = candidate();
    candidate.components[0].requirements =
        BTreeMap::from([("isolation.level".to_owned(), "sandbox".to_owned())]);
    candidate.definitions[0].body.steps.truncate(1);
    candidate.definitions[0].body.result = Expression::Literal { value: json!(true) };
    let plan = seal_plan(candidate).expect("Plan admits structurally");
    let counts = Arc::new(PluginCounts::default());
    let mut runtime = runtime(counts.clone(), json!(42), json!("unused"));

    let error = runtime
        .execute(plan, &json!({"request": "run"}), "run:requirements")
        .expect_err("unmatched provider requirements fail admission");

    assert!(matches!(
        error,
        RuntimeError::PluginDefect { ref code, .. } if code == "execution_binding_rejected"
    ));
    assert_eq!(counts.call.load(Ordering::SeqCst), 0);
    let snapshot = runtime.machine().snapshot();
    assert!(snapshot.plans.is_empty());
    assert!(snapshot.artifacts.is_empty());
    assert!(snapshot.events.is_empty());
}

#[test]
fn component_input_fails_before_call_and_effect_input_before_prepare() {
    let mut component_plan = candidate();
    let Operation::Call { input, .. } = &mut component_plan.definitions[0].body.steps[0].operation
    else {
        panic!("fixture begins with a component call")
    };
    *input = Expression::Literal {
        value: json!({"name": 7}),
    };
    let counts = Arc::new(PluginCounts::default());
    let mut component_runtime = runtime(counts.clone(), json!(42), json!(42));
    let error = component_runtime
        .execute(
            seal_plan(component_plan).expect("Plan admits"),
            &json!({"request": "run"}),
            "run:component-input",
        )
        .expect_err("component input must fail");
    assert!(matches!(error, RuntimeError::Contract(_)));
    assert_eq!(counts.call.load(Ordering::SeqCst), 0);

    let mut effect_plan = effect_only_candidate(json!({"type": "string"}), json!({}));
    let Operation::Effect { input, .. } = &mut effect_plan.definitions[0].body.steps[0].operation
    else {
        panic!("fixture contains one effect")
    };
    *input = Expression::Literal { value: json!(7) };
    let effect_counts = Arc::new(PluginCounts::default());
    let mut effect_runtime = runtime(effect_counts.clone(), json!(42), json!(42));
    let error = effect_runtime
        .execute(
            seal_plan(effect_plan).expect("Plan admits"),
            &json!({"request": "run"}),
            "run:effect-input",
        )
        .expect_err("effect input must fail");
    assert!(matches!(error, RuntimeError::Contract(_)));
    assert_eq!(effect_counts.prepare.load(Ordering::SeqCst), 0);
    assert_eq!(effect_counts.dispatch.load(Ordering::SeqCst), 0);
    assert!(
        effect_runtime.machine().projection().runs["run:effect-input"]
            .effects
            .is_empty()
    );
}

#[test]
fn output_failures_never_bind_or_record_terminal_results() {
    let counts = Arc::new(PluginCounts::default());
    let mut component_runtime = runtime(counts.clone(), json!("not-an-integer"), json!(42));
    let error = component_runtime
        .execute(
            seal_plan(candidate()).expect("Plan admits"),
            &json!({"request": "run"}),
            "run:component-output",
        )
        .expect_err("component output must fail");
    assert!(matches!(error, RuntimeError::Contract(_)));
    assert_eq!(counts.call.load(Ordering::SeqCst), 1);
    assert!(
        component_runtime.machine().projection().runs["run:component-output"]
            .result
            .is_none()
    );

    let effect_plan = effect_only_candidate(json!({"type": "integer"}), json!({"type": "string"}));
    let effect_counts = Arc::new(PluginCounts::default());
    let mut effect_runtime = runtime(effect_counts.clone(), json!(42), json!(42));
    let error = effect_runtime
        .execute(
            seal_plan(effect_plan).expect("Plan admits"),
            &json!({"request": "run"}),
            "run:effect-output",
        )
        .expect_err("effect output must fail");
    assert!(matches!(error, RuntimeError::Contract(_)));
    assert_eq!(effect_counts.dispatch.load(Ordering::SeqCst), 1);
    let effect = effect_runtime.machine().projection().runs["run:effect-output"]
        .effects
        .values()
        .next()
        .expect("effect intent exists");
    assert_eq!(effect.outcome, cymule_core::WorldOutcome::Applied);
}

#[test]
fn contract_violation_projects_to_engine_failure_without_losing_paths() {
    let contracts = PlanContracts::compile(&candidate()).expect("schemas compile");
    let violation = contracts
        .validate_component_input("example.component", &json!({"name": 7}))
        .expect_err("component input must fail");
    let failure = EngineFailure::from_contract_violation(&violation, EnginePhase::PluginCall);

    assert_eq!(failure.category, EngineFailureCategory::ContractViolation);
    assert_eq!(
        failure.contract.as_deref(),
        Some("component:example.component")
    );
    assert_eq!(failure.contract_side, Some(EngineContractSide::Input));
    assert_eq!(failure.path.as_deref(), Some("/name"));
    assert_eq!(failure.issues.len(), violation.issues.len());
    assert_eq!(
        failure.issues[0].schema_path.as_deref(),
        Some(violation.issues[0].schema_path.as_str())
    );
    failure.verify().expect("projected failure is wire-valid");
}
