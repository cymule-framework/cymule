//! Rust SDK side of the shared cross-language end-to-end scenario.

use std::collections::BTreeMap;
use std::env;
use std::path::Path;

use cymule::{
    CliEngine, DURABLE_CONTROL_VERSION, DispatchPolicy, DurableCommand, DurableEngine,
    DurableResponse, EffectProfile, Engine, EngineClockTarget, EngineDurableTarget, EngineFailure,
    EnginePluginTarget, EngineProcessConfig, EngineStoreTarget, EvolutionCommand,
    ExecutionClaimRequest, Expression, FlowBuilder, LIVE_EVOLUTION_CONTROL_VERSION,
    LiveEvolutionCommand, LiveEvolutionOutcome, MutationKind, Operation, PlanCandidate,
    ReconciliationMode, Region, RegionMigrationCommand, ResourceCandidate, Step,
    VirtualClaimCommand, VirtualCompactionCommand, VirtualLeaseRenewalCommand,
    VirtualRecoveryCommand, VirtualRehydrationCommand, VirtualRunWeightCommand, WaitActivation,
    WorkOccurrence, WorkResolutionCommand,
};
use cymule_core::COMPONENT_OUTPUT_ARTIFACT_KIND;
use serde_json::json;

const TEST_EFFECT_LEDGER_PATH_ENV: &str = "CYMULE_TEST_EFFECT_LEDGER_PATH";
const REQUIRE_CONFORMANCE_ENV: &str = "CYMULE_RUST_SDK_CONFORMANCE_REQUIRED";

fn conformance_env<const N: usize>(test_name: &str, names: [&str; N]) -> Option<[String; N]> {
    let required = match env::var(REQUIRE_CONFORMANCE_ENV) {
        Ok(value) if value == "1" => true,
        Ok(value) => panic!("{REQUIRE_CONFORMANCE_ENV} must be exactly 1, received {value:?}"),
        Err(env::VarError::NotPresent) => false,
        Err(error) => panic!("{REQUIRE_CONFORMANCE_ENV} is not valid Unicode: {error}"),
    };
    let mut missing = Vec::new();
    let values = names.map(|name| match env::var(name) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => {
            missing.push(name);
            None
        }
        Err(error) => panic!("{name} is not valid Unicode: {error}"),
    });
    if !missing.is_empty() {
        assert!(
            !required,
            "{test_name} requires conformance environment variables: {}",
            missing.join(", ")
        );
        eprintln!(
            "skipped {test_name}: set {REQUIRE_CONFORMANCE_ENV}=1 through scripts/verify-sdk.sh to require {}",
            missing.join(", ")
        );
        return None;
    }
    Some(values.map(|value| value.expect("missing values returned above")))
}

fn explicit_conformance_skip() {}

fn process_target(executable: impl Into<String>, effect_ledger: &Path) -> EnginePluginTarget {
    assert!(effect_ledger.is_absolute());
    let executable = executable.into();
    assert!(Path::new(&executable).is_absolute());
    EnginePluginTarget::process(EngineProcessConfig {
        executable,
        arguments: Vec::new(),
        environment: BTreeMap::from([(
            TEST_EFFECT_LEDGER_PATH_ENV.to_owned(),
            effect_ledger.display().to_string(),
        )]),
        working_directory: None,
        runtime_closure: BTreeMap::from([(
            "component-runtime".to_owned(),
            format!("sha256:{}", "a".repeat(64)),
        )]),
        timeout_ms: 60_000,
        message_limit: 8 * 1024 * 1024,
        closure_limit: 64 * 1024 * 1024,
    })
}

#[test]
fn rust_candidate_seals_and_executes_through_the_cli() {
    let Some([engine_path, plugin_path, expected_plan_id]) = conformance_env(
        "rust_candidate_seals_and_executes_through_the_cli",
        [
            "CYMULE_BIN",
            "CYMULE_TEST_PLUGIN",
            "CYMULE_EXPECTED_PLAN_ID",
        ],
    ) else {
        return explicit_conformance_skip();
    };
    let candidate = cross_language_candidate();

    let domain = tempfile::tempdir().expect("temporary durable domain");
    let effect_ledger = domain.path().join("effect-settlement.sqlite3");
    let plugin = process_target(plugin_path.clone(), &effect_ledger);
    let engine = CliEngine::new(&engine_path);
    let plan = engine.seal(&candidate).expect("candidate seals");
    assert_eq!(plan.plan_id, expected_plan_id);
    let input = json!({"message": "hello from Rust"});
    let result = engine
        .run(&plan, &input, &plugin, "run:rust-e2e")
        .expect("plan executes")
        .into_completed()
        .expect("plan completes");
    assert_eq!(result.value, input);
    assert_eq!(result.effects.len(), 1);

    let store = EngineStoreTarget::sqlite(
        domain.path().join("domain.sqlite").display().to_string(),
        "sdk-rust",
    );
    let clock = EngineClockTarget::sqlite(
        domain.path().join("clock.sqlite").display().to_string(),
        "clock:sdk-rust",
        "sha256:9999999999999999999999999999999999999999999999999999999999999999",
    );
    let durable = DurableEngine::from_transport(CliEngine::new(&engine_path), store.clone())
        .with_executor(plugin)
        .with_clock(clock);
    let run_id = "run:rust-durable-e2e";
    let first_clock = durable
        .observe_clock(run_id)
        .expect("first Clock receipt is issued");
    let later_clock = durable
        .observe_clock(run_id)
        .expect("later Clock receipt is issued");
    assert_eq!(first_clock.source_id, later_clock.source_id);
    assert_eq!(first_clock.source_generation, later_clock.source_generation);
    assert_eq!(first_clock.scope, later_clock.scope);
    assert_ne!(first_clock.observation_id, later_clock.observation_id);
    let response = durable
        .start(
            run_id,
            candidate.clone(),
            input,
            ExecutionClaimRequest {
                owner: "driver:sdk-rust".to_owned(),
                clock: later_clock,
                ttl: 10,
            },
        )
        .expect("durable Run starts through the CLI");
    assert!(matches!(response, DurableResponse::RunBoundary { .. }));
    assert!(
        DurableEngine::from_transport(CliEngine::new(&engine_path), store)
            .run_current("run:rust-durable-e2e", None)
            .expect("Run-current query succeeds")
            .current
            .is_some()
    );
    let evolved = durable
        .evolve(&LiveEvolutionCommand::PublishDefinition {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "evolve:rust:publish".to_owned(),
            logical_ref: "definition:rust:echo".to_owned(),
            definition: candidate.definitions[0].clone(),
            references: Vec::new(),
        })
        .expect("live evolution checkpoints through the CLI");
    assert_eq!(
        evolved.receipt.command.evolution_id,
        "cymule.sdk.live-evolution"
    );
    assert!(matches!(
        evolved.receipt.outcome,
        LiveEvolutionOutcome::DefinitionPublished { .. }
    ));
}

fn cross_language_candidate() -> PlanCandidate {
    FlowBuilder::new("cross_language_echo", json!({}), json!({}))
        .component(
            "test.echo",
            json!({}),
            json!({}),
            COMPONENT_OUTPUT_ARTIFACT_KIND,
            BTreeMap::new(),
        )
        .effect_contract(
            "test.capture",
            json!({}),
            json!({}),
            EffectProfile {
                mutation: MutationKind::Observational,
                dispatch: DispatchPolicy::Eager,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
            BTreeMap::new(),
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
            Some("observed".to_owned()),
        )
        .scope(
            "scope.finalize",
            Region {
                steps: Vec::new(),
                result: Expression::Literal { value: json!(null) },
            },
            "scope_result",
        )
        .finish(Expression::Binding {
            name: "echoed".to_owned(),
        })
}

#[test]
fn rust_rejects_malicious_nested_engine_success() {
    let Some([engine_path]) = conformance_env(
        "rust_rejects_malicious_nested_engine_success",
        ["CYMULE_MALICIOUS_ENGINE"],
    ) else {
        return explicit_conformance_skip();
    };
    let error = CliEngine::new(engine_path)
        .execute_durable(
            &EngineDurableTarget::query(EngineStoreTarget::directory("unused")),
            &DurableCommand::RunIndexPage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                expected_revision: None,
                cursor: None,
                limit: 1,
                max_canonical_bytes: 1024,
            },
        )
        .expect_err("forged nested Continuation must fail closed");
    assert_eq!(error.code.as_ref(), "invalid_engine_response");
    assert_eq!(
        error.category,
        cymule::EngineFailureCategory::TransportFailure
    );
}

#[test]
fn rust_classifies_unsupported_engine_protocol_by_mutation_authority() {
    let Some([engine_path]) = conformance_env(
        "rust_classifies_unsupported_engine_protocol_by_mutation_authority",
        ["CYMULE_UNSUPPORTED_ENGINE"],
    ) else {
        return explicit_conformance_skip();
    };
    let engine = CliEngine::new(engine_path);
    let candidate =
        FlowBuilder::new("unsupported_protocol", json!({}), json!({})).finish(Expression::Input);
    let read = engine
        .seal(&candidate)
        .expect_err("Engine v4 cannot settle a read request");
    assert_eq!(
        read.category,
        cymule::EngineFailureCategory::ContractViolation
    );
    assert_eq!(read.code.as_ref(), "unsupported_engine_protocol");
    assert_eq!(
        read.retry_disposition,
        Some(cymule::EngineRetryDisposition::Never)
    );

    let mutation = engine
        .observe_clock(
            &EngineClockTarget::sqlite(
                "/tmp/cymule-unsupported-clock.sqlite",
                "clock:unsupported-protocol",
                "sha256:4444444444444444444444444444444444444444444444444444444444444444",
            ),
            "run:unsupported-protocol",
        )
        .expect_err("Engine v4 cannot settle a mutating request");
    assert_eq!(
        mutation.category,
        cymule::EngineFailureCategory::UnknownWorldOutcome
    );
    assert_eq!(mutation.code.as_ref(), "unsupported_engine_protocol");
    assert_eq!(
        mutation.retry_disposition,
        Some(cymule::EngineRetryDisposition::Reconcile)
    );
}

#[test]
fn rust_rejects_malicious_effect_boundary_success() {
    let Some([engine_path, malicious_path]) = conformance_env(
        "rust_rejects_malicious_effect_boundary_success",
        ["CYMULE_BIN", "CYMULE_MALICIOUS_EFFECT_ENGINE"],
    ) else {
        return explicit_conformance_skip();
    };
    let candidate = FlowBuilder::new("malicious_effect_boundary", json!({}), json!({}))
        .finish(Expression::Input);
    let plan = CliEngine::new(engine_path)
        .seal(&candidate)
        .expect("malicious-boundary test Plan seals");
    let domain = tempfile::tempdir().expect("temporary malicious-boundary domain");
    let plugin = process_target(
        malicious_path.clone(),
        &domain.path().join("effect-settlement.sqlite3"),
    );

    let error = CliEngine::new(malicious_path)
        .run(
            &plan,
            &json!({"message": "must fail closed"}),
            &plugin,
            "run:rust-malicious-effect-boundary",
        )
        .expect_err("forged release-required identities must fail closed");
    assert_eq!(error.code.as_ref(), "invalid_engine_response");
    assert_eq!(
        error.category,
        cymule::EngineFailureCategory::UnknownWorldOutcome
    );
    assert_eq!(
        error.retry_disposition,
        Some(cymule::EngineRetryDisposition::Reconcile)
    );
}

#[test]
fn rust_resource_seals_through_the_cli() {
    let Some([engine_path, expected_resource_id]) = conformance_env(
        "rust_resource_seals_through_the_cli",
        ["CYMULE_BIN", "CYMULE_EXPECTED_RESOURCE_ID"],
    ) else {
        return explicit_conformance_skip();
    };
    let mut candidate = ResourceCandidate::text("shared cross-run resource");
    candidate.annotations = BTreeMap::from([(
        "purpose".to_owned(),
        "cross-language-conformance".to_owned(),
    )]);
    let resource = CliEngine::new(&engine_path)
        .seal_resource(&candidate)
        .expect("Resource Candidate seals");
    assert_eq!(resource.resource_id, expected_resource_id);

    let vendor = ResourceCandidate {
        resource_version: "cymule.resource/4".to_owned(),
        shape: cymule::ResourceShape::Object,
        media_type: "application/vnd.cymule.resource+json".to_owned(),
        inline: None,
        integrity: cymule::ResourceIntegrity::Content {
            digest: format!("sha256:{}", "1".repeat(64)),
            size: 0,
        },
        manifest: None,
        annotations: BTreeMap::new(),
    };
    let vendor_resource = CliEngine::new(&engine_path)
        .seal_resource(&vendor)
        .expect("vendor Resource Candidate seals");
    assert_eq!(vendor_resource.resource_version, "cymule.resource/4");
    assert_eq!(vendor_resource.media_type, vendor.media_type);
}

#[test]
fn rust_wait_activation_validates_through_the_cli() {
    let Some([engine_path]) = conformance_env(
        "rust_wait_activation_validates_through_the_cli",
        ["CYMULE_BIN"],
    ) else {
        return explicit_conformance_skip();
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
    let Some([engine_path, fixture_path]) = conformance_env(
        "rust_durable_control_validates_through_the_cli",
        ["CYMULE_BIN", "CYMULE_DURABLE_CONTROL_FIXTURE"],
    ) else {
        return explicit_conformance_skip();
    };
    let command: DurableCommand =
        serde_json::from_str(&std::fs::read_to_string(fixture_path).expect("fixture reads"))
            .expect("durable command deserializes");
    command.verify().expect("durable command verifies locally");
    assert!(matches!(
        &command,
        DurableCommand::TakeoverRun {
            run_id,
            expected_fence: 7,
            execution,
            ..
        } if run_id == "run:cross-language"
            && execution.owner == "driver:cross-language"
            && execution.ttl == 30
            && execution.clock.source_id == "clock:cross-language"
    ));
    assert_eq!(
        CliEngine::new(engine_path)
            .verify_durable_command(&command)
            .expect("Rust engine verifies M1 command"),
        command
    );
}

#[test]
fn rust_cancel_control_matches_the_shared_fixture() {
    let command: DurableCommand = serde_json::from_str(include_str!(
        "../../../tests/fixtures/durable-cancel-control.json"
    ))
    .expect("cancel fixture deserializes");
    assert!(matches!(
        &command,
        DurableCommand::CancelRun {
            cancellation_id,
            run_id,
            ..
        } if cancellation_id == "cancel:cross-language"
            && run_id == "run:cross-language"
    ));
    command.verify().expect("cancel command verifies locally");
    let Some([engine_path]) = conformance_env(
        "rust_cancel_control_matches_the_shared_fixture",
        ["CYMULE_BIN"],
    ) else {
        return explicit_conformance_skip();
    };
    assert_eq!(
        CliEngine::new(engine_path)
            .verify_durable_command(&command)
            .expect("Rust engine verifies cancel command"),
        command
    );
}

#[test]
fn rust_terminal_boundaries_match_the_shared_fixture() {
    let responses: Vec<DurableResponse> = serde_json::from_str(include_str!(
        "../../../tests/fixtures/durable-terminal-responses.json"
    ))
    .expect("terminal responses deserialize");
    assert!(matches!(
        &responses[0],
        DurableResponse::RunBoundary {
            boundary: cymule::DurableBoundary::Failed { failure },
        } if failure.code == "fixture_failure"
    ));
    assert!(matches!(
        &responses[1],
        DurableResponse::RunCancelled { receipt }
            if receipt.command.cancellation_id == "cancel:fixture"
                && receipt.command.run_id == "run:fixture:cancelled"
                && receipt.command.reason == json!({"code": "fixture_cancelled"})
                && matches!(
                    &receipt.boundary,
                    cymule::DurableBoundary::Cancelled { .. }
                )
    ));
    assert!(matches!(
        &responses[2],
        DurableResponse::RunBoundary {
            boundary: cymule::DurableBoundary::EffectNotApplied { intent_id },
        } if intent_id
            == "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    ));
    assert!(matches!(
        &responses[3],
        DurableResponse::RunBoundary {
            boundary: cymule::DurableBoundary::EffectUnavailable { intent_id },
        } if intent_id
            == "sha256:982a836f8dcb860b0eedabf0fd133bc2f966992526e2703316cba497f929e03b"
    ));
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
    compaction
        .verify()
        .expect("Rust-authored compaction command identity verifies");
    assert_eq!(
        compaction.work_ids,
        std::collections::BTreeSet::from([occurrence.work_id.clone()])
    );
    assert_eq!(
        compaction.occurrence_ids,
        std::collections::BTreeSet::from([occurrence.occurrence_id.clone()])
    );
    assert!(compaction.archived_command_ids.is_empty());
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
    let Some([engine_path, fixture_path, restart_path]) = conformance_env(
        "rust_evolution_control_validates_through_the_cli",
        [
            "CYMULE_BIN",
            "CYMULE_EVOLUTION_CONTROL_FIXTURE",
            "CYMULE_EVOLUTION_RESTART_FIXTURE",
        ],
    ) else {
        return explicit_conformance_skip();
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
    let Some([engine_path, fixture_path]) = conformance_env(
        "rust_unified_live_evolution_validates_through_the_cli",
        ["CYMULE_BIN", "CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE"],
    ) else {
        return explicit_conformance_skip();
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
    let Some([engine_path, plugin_path, fixture_path]) = conformance_env(
        "rust_engine_preserves_structured_negative_outcomes",
        [
            "CYMULE_BIN",
            "CYMULE_TEST_PLUGIN",
            "CYMULE_ENGINE_FAILURE_FIXTURE",
        ],
    ) else {
        return explicit_conformance_skip();
    };
    let fixture: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_path).expect("Engine failure fixture reads"),
    )
    .expect("Engine failure fixture parses");
    let process_domain = tempfile::tempdir().expect("temporary process configuration domain");
    let effect_ledger = process_domain.path().join("effect-settlement.sqlite3");
    let plugin = process_target(plugin_path, &effect_ledger);
    let missing_plugin = process_target(
        "/cymule-conformance/missing-plugin".to_owned(),
        &effect_ledger,
    );
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
                &json!({"simulate": "expected_failure"}),
                &plugin,
                "run:rust-expected-failure",
            )
            .expect_err("declared application failure remains expected"),
        &fixture["cases"]["expected_plugin_failure"],
    );
    assert_failure(
        &engine
            .run(
                &plan,
                &json!({"simulate": "protocol_defect"}),
                &plugin,
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
                &missing_plugin,
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
