//! Executable Plan contract compiler conformance.

use std::collections::BTreeMap;

use cymule_core::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    IR_VERSION, MutationKind, Operation, PlanCandidate, ReconciliationMode, Region, ScopeMode,
    Step, WaitSpec,
};
use cymule_runtime::{
    ContractBoundary, ContractPhase, ContractSide, ContractTarget, ContractValidator, PlanContracts,
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
        assert!(
            error.issues[0]
                .message
                .contains("external schema reference")
        );
        assert!(error.issues[0].instance_path.contains("$defs"));
    }
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

    let first = original.seal().expect("original Plan seals");
    let mut changed = candidate();
    changed.components[0].input_schema["properties"]["name"]["minLength"] = json!(1);
    PlanContracts::compile(&changed).expect("changed schema compiles");
    let second = changed.seal().expect("changed Plan seals");
    assert_ne!(first.plan_id, second.plan_id);
}
