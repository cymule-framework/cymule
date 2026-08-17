//! Rust SDK side of the shared cross-language end-to-end scenario.

use std::collections::BTreeMap;
use std::env;

use cymule_sdk::{
    CliEngine, DispatchPolicy, EffectProfile, Engine, Expression, FlowBuilder, MutationKind,
    Operation, ReconciliationMode, Region, RegionMigrationCommand, ResourceCandidate, Step,
    VirtualClaimCommand, VirtualCompactionCommand, VirtualLeaseRenewalCommand,
    VirtualRecoveryCommand, VirtualRehydrationCommand, VirtualRunWeightCommand, WaitActivation,
    WorkOccurrence, WorkResolutionCommand,
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
        .definition(
            "echo_subflow",
            json!({}),
            json!({}),
            Region {
                steps: vec![Step {
                    id: "call.echo".to_owned(),
                    operation: Operation::Call {
                        component: "test.echo".to_owned(),
                        input: Expression::Input,
                        bind: Some("echoed".to_owned()),
                    },
                }],
                result: Expression::Binding {
                    name: "echoed".to_owned(),
                },
            },
        )
        .invoke(
            "invoke.echo-subflow",
            "echo_subflow",
            Expression::Input,
            "echoed",
        )
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
    let migration: RegionMigrationCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-region-migration-control.json"
    ))
    .expect("virtual region migration control deserializes");
    assert_eq!(migration.plan.expected_sources.len(), 1);
    assert_eq!(migration.plan.targets.len(), 2);
    let compaction: VirtualCompactionCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-compaction-control.json"
    ))
    .expect("virtual compaction control deserializes");
    assert_eq!(compaction.region_id, occurrence.region_id);
    let rehydration: VirtualRehydrationCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-rehydration-control.json"
    ))
    .expect("virtual rehydration control deserializes");
    assert!(
        rehydration
            .occurrence_ids
            .contains(&occurrence.occurrence_id)
    );
    let claim: VirtualClaimCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-claim-control.json"
    ))
    .expect("virtual claim control deserializes");
    assert_eq!(claim.owner, occurrence.owner);
    let renewal: VirtualLeaseRenewalCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-lease-renewal-control.json"
    ))
    .expect("virtual lease renewal control deserializes");
    assert_eq!(renewal.work_id, occurrence.work_id);
    let recovery: VirtualRecoveryCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-recovery-control.json"
    ))
    .expect("virtual recovery control deserializes");
    assert_eq!(recovery.expected_epoch, occurrence.epoch);
    let run_weight: VirtualRunWeightCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/virtual-run-weight-control.json"
    ))
    .expect("virtual Run weight control deserializes");
    assert_eq!(run_weight.weight, 3);
}
