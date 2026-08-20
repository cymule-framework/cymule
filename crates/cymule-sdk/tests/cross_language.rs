//! Rust SDK side of the shared cross-language end-to-end scenario.

use std::collections::BTreeMap;
use std::env;
use std::path::Path;

use cymule::{
    CliEngine, DispatchPolicy, DurableCommand, EffectProfile, Engine, EngineFailure,
    EvolutionCommand, Expression, FlowBuilder, LiveEvolutionCommand, MutationKind, Operation,
    PlanCandidate, ReconciliationMode, Region, RegionMigrationCommand, ResourceCandidate, Step,
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
fn rust_durable_control_validates_through_the_cli() {
    let (Ok(engine_path), Ok(fixture_path)) = (
        env::var("CYMULE_BIN"),
        env::var("CYMULE_DURABLE_CONTROL_FIXTURE"),
    ) else {
        return;
    };
    let command: DurableCommand =
        serde_json::from_str(&std::fs::read_to_string(fixture_path).expect("fixture reads"))
            .expect("durable command deserializes");
    command.verify().expect("durable command verifies locally");
    assert_eq!(
        CliEngine::new(engine_path)
            .verify_durable_command(&command)
            .expect("Rust engine verifies M1 command"),
        command
    );
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

#[test]
fn rust_evolution_control_validates_through_the_cli() {
    let (Ok(engine_path), Ok(fixture_path), Ok(restart_path)) = (
        env::var("CYMULE_BIN"),
        env::var("CYMULE_EVOLUTION_CONTROL_FIXTURE"),
        env::var("CYMULE_EVOLUTION_RESTART_FIXTURE"),
    ) else {
        return;
    };
    let command: EvolutionCommand =
        serde_json::from_str(&std::fs::read_to_string(fixture_path).expect("fixture reads"))
            .expect("evolution command deserializes");
    command.verify().expect("command verifies locally");
    assert_eq!(
        CliEngine::new(&engine_path)
            .verify_evolution_command(&command)
            .expect("Rust engine verifies M4 command"),
        command
    );
    let restart: EvolutionCommand =
        serde_json::from_str(&std::fs::read_to_string(restart_path).expect("fixture reads"))
            .expect("restart command deserializes");
    restart.verify().expect("restart command verifies locally");
    assert_eq!(
        CliEngine::new(&engine_path)
            .verify_evolution_command(&restart)
            .expect("Rust engine verifies restart command"),
        restart
    );
}

#[test]
fn rust_unified_live_evolution_validates_through_the_cli() {
    let (Ok(engine_path), Ok(fixture_path)) = (
        env::var("CYMULE_BIN"),
        env::var("CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE"),
    ) else {
        return;
    };
    let command: LiveEvolutionCommand =
        serde_json::from_str(&std::fs::read_to_string(fixture_path).expect("fixture reads"))
            .expect("live-evolution command deserializes");
    command.verify().expect("command verifies locally");
    assert_eq!(
        CliEngine::new(&engine_path)
            .verify_live_evolution_command(&command)
            .expect("Rust engine verifies live-evolution command"),
        command
    );
}

#[test]
fn rust_engine_preserves_structured_negative_outcomes() {
    let (Ok(engine_path), Ok(fixture_path)) = (
        env::var("CYMULE_BIN"),
        env::var("CYMULE_ENGINE_FAILURE_FIXTURE"),
    ) else {
        return;
    };
    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_path).expect("Engine failure fixture reads"),
    )
    .expect("Engine failure fixture parses");
    let engine = CliEngine::new(&engine_path);
    let mut invalid: PlanCandidate = serde_json::from_str(include_str!(
        "../../../tests/fixtures/cross-language-plan.json"
    ))
    .expect("candidate parses");
    invalid.ir_version = "cymule.ir/unsupported".to_owned();
    assert_failure(
        &engine.seal(&invalid).expect_err("invalid version fails"),
        &fixture["cases"]["invalid_plan_version"],
    );

    let candidate: PlanCandidate = serde_json::from_str(include_str!(
        "../../../tests/fixtures/cross-language-plan.json"
    ))
    .expect("candidate parses");
    let plan = engine.seal(&candidate).expect("valid candidate seals");
    assert_failure(
        &engine
            .run(
                &plan,
                &json!({"message": "plugin defect"}),
                engine_path.as_ref(),
                "run:rust-plugin-defect",
            )
            .expect_err("invalid plugin process fails"),
        &fixture["cases"]["plugin_defect"],
    );
    assert_failure(
        &engine
            .run(
                &plan,
                &json!({"message": "substrate"}),
                Path::new("/cymule-conformance/missing-plugin"),
                "run:rust-substrate-failure",
            )
            .expect_err("missing plugin substrate fails"),
        &fixture["cases"]["substrate_failure"],
    );
}

fn assert_failure(failure: &EngineFailure, expected: &serde_json::Value) {
    let category = serde_json::to_value(failure.category).expect("category serializes");
    let phase = serde_json::to_value(failure.phase).expect("phase serializes");
    let retry = serde_json::to_value(failure.retry_disposition).expect("retry serializes");
    assert_eq!(category, expected["category"]);
    assert_eq!(phase, expected["phase"]);
    assert_eq!(failure.code.as_ref(), expected["code"].as_str().unwrap());
    assert_eq!(
        retry,
        expected
            .get("retry_disposition")
            .cloned()
            .unwrap_or_default()
    );
}
