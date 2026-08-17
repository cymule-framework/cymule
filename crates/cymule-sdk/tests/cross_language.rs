//! Rust SDK side of the shared cross-language end-to-end scenario.

use std::collections::BTreeMap;
use std::env;

use cymule_sdk::{
    CliEngine, DispatchPolicy, EffectProfile, Engine, Expression, FlowBuilder, MutationKind,
    ReconciliationMode, ResourceCandidate, WaitActivation, WorkOccurrence, WorkResolutionCommand,
};
use serde_json::json;

#[test]
fn rust_candidate_seals_and_executes_through_the_cli() {
    let (Ok(engine_path), Ok(plugin_path), Ok(expected_plan_id)) = (
        env::var("CYMULE_BIN"),
        env::var("CYMULE_TEST_PLUGIN"),
        env::var("CYMULE_EXPECTED_PLAN_ID"),
    ) else {
        return;
    };
    let candidate = FlowBuilder::new("cross_language_echo", json!({}), json!({}))
        .component("test.echo", json!({}), json!({}))
        .effect_contract(
            "test.capture",
            json!({}),
            json!({}),
            EffectProfile {
                mutation: MutationKind::Mutating,
                dispatch: DispatchPolicy::OnScopeCommit,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
        )
        .call("call.echo", "test.echo", Expression::Input, "echoed")
        .effect(
            "effect.capture",
            "test.capture",
            Expression::Binding {
                name: "echoed".to_owned(),
            },
            "primary",
        )
        .finish(Expression::Binding {
            name: "echoed".to_owned(),
        });

    let engine = CliEngine::new(engine_path);
    let plan = engine.seal(&candidate).expect("candidate seals");
    assert_eq!(plan.plan_id, expected_plan_id);
    let input = json!({"message": "hello from Rust"});
    let result = engine
        .run(&plan, &input, plugin_path.as_ref(), "run:rust-e2e")
        .expect("plan executes");
    assert_eq!(result.value, input);
    assert_eq!(result.effects.len(), 1);
}

#[test]
fn rust_resource_seals_through_the_cli() {
    let (Ok(engine_path), Ok(expected_resource_id)) = (
        env::var("CYMULE_BIN"),
        env::var("CYMULE_EXPECTED_RESOURCE_ID"),
    ) else {
        return;
    };
    let mut candidate = ResourceCandidate::text("shared cross-run resource");
    candidate.annotations = BTreeMap::from([(
        "purpose".to_owned(),
        "cross-language-conformance".to_owned(),
    )]);
    let resource = CliEngine::new(engine_path)
        .seal_resource(&candidate)
        .expect("Resource Candidate seals");
    assert_eq!(resource.resource_id, expected_resource_id);
}

#[test]
fn rust_wait_activation_validates_through_the_cli() {
    let Ok(engine_path) = env::var("CYMULE_BIN") else {
        return;
    };
    let activation: WaitActivation =
        serde_json::from_str(include_str!("../../../tests/fixtures/wait-activation.json"))
            .expect("wait activation fixture deserializes");
    let verified = CliEngine::new(engine_path)
        .verify_wait_activation(&activation)
        .expect("wait activation verifies");
    assert_eq!(verified, activation);
}

#[test]
fn rust_virtual_work_query_and_control_fixtures_are_typed() {
    let occurrence: WorkOccurrence = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-work-occurrence.json"
    ))
    .expect("virtual work occurrence deserializes");
    assert_eq!(occurrence.work_id, "work:fixture");
    let command: WorkResolutionCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-work-control.json"
    ))
    .expect("virtual work control deserializes");
    assert_eq!(command.work_id, occurrence.work_id);
    assert_eq!(command.epoch, occurrence.epoch);
}
