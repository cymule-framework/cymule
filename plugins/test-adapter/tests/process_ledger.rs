//! Real process-boundary conformance for the test adapter's provider ledger.

use std::collections::BTreeMap;
use std::path::Path;

use cymule_executor_process::{ProcessExecutor, ProcessExecutorConfig};
use cymule_runtime::{
    EffectProviderAttempt, EffectReconciliationDecision, ExecutionBinding, PluginHost,
    PluginRequest, PluginResponse,
};
use serde_json::json;

const EFFECT_LEDGER_PATH_ENV: &str = "CYMULE_TEST_EFFECT_LEDGER_PATH";
const LEDGER_INTENT_ID: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const IN_FLIGHT_INTENT_ID: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

fn process_config(executable: impl AsRef<Path>) -> ProcessExecutorConfig {
    ProcessExecutorConfig::new(
        executable,
        BTreeMap::from([(
            "test-adapter-runtime".to_owned(),
            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
        )]),
    )
}

#[test]
fn adapter_rejects_missing_provider_ledger_configuration() {
    let executable = env!("CARGO_BIN_EXE_cymule-test-adapter");
    let mut executor =
        ProcessExecutor::new(process_config(executable)).expect("test adapter executable seals");
    assert!(matches!(
        executor.describe(),
        Err(cymule_runtime::RuntimeError::PluginDefect { code, .. })
            if code == "plugin_process_failed"
    ));
}

#[test]
fn component_protocol_defect_is_a_zero_exit_mismatched_response() {
    let directory = tempfile::tempdir().expect("ledger directory creates");
    let executable = env!("CARGO_BIN_EXE_cymule-test-adapter");
    let mut config = process_config(executable);
    config.environment.insert(
        EFFECT_LEDGER_PATH_ENV.to_owned(),
        directory
            .path()
            .join("effect-settlement.sqlite3")
            .to_string_lossy()
            .into_owned(),
    );
    let mut executor = ProcessExecutor::new(config).expect("test adapter executor configures");
    let failure = executor
        .invoke(PluginRequest::Call {
            component: "test.echo".to_owned(),
            input: json!({"simulate": "protocol_defect"}),
        })
        .expect_err("mismatched successful response is a protocol defect");
    assert!(matches!(
        failure,
        cymule_runtime::RuntimeError::PluginDefect { code, .. }
            if code == "plugin_protocol_violation"
    ));
}

#[test]
fn fresh_children_share_one_explicit_provider_settlement_ledger() {
    let directory = tempfile::tempdir().expect("ledger directory creates");
    let ledger = directory.path().join("effect-settlement.sqlite3");
    let executable = env!("CARGO_BIN_EXE_cymule-test-adapter");
    let mut config = process_config(executable);
    config.environment.insert(
        EFFECT_LEDGER_PATH_ENV.to_owned(),
        ledger.to_string_lossy().into_owned(),
    );

    let mut first_executor =
        ProcessExecutor::new(config.clone()).expect("explicit ledger config seals");
    let manifest = first_executor.describe().expect("test adapter describes");
    let first_revision = first_executor.implementation_revision().to_owned();
    let first_binding = ExecutionBinding::for_local_process(&manifest, &first_revision)
        .expect("explicit ledger binding seals");
    let mut other_config = config.clone();
    other_config.environment.insert(
        EFFECT_LEDGER_PATH_ENV.to_owned(),
        directory
            .path()
            .join("other-effect-settlement.sqlite3")
            .to_string_lossy()
            .into_owned(),
    );
    let other_revision = ProcessExecutor::new(other_config)
        .expect("other ledger config seals")
        .implementation_revision()
        .to_owned();
    let other_binding = ExecutionBinding::for_local_process(&manifest, other_revision)
        .expect("other ledger binding seals");
    assert_ne!(
        other_binding
            .artifact_ref()
            .expect("other binding identifies"),
        first_binding
            .artifact_ref()
            .expect("first binding identifies"),
        "the explicit provider ledger locator participates in the binding revision"
    );

    let mut executor = ProcessExecutor::new(config).expect("test adapter executor configures");
    let intent_id = LEDGER_INTENT_ID;
    let attempt = EffectProviderAttempt::new(intent_id, "owner:fresh-child-ledger", 1)
        .expect("provider attempt derives");
    let input = json!({"value": "never-apply-late"});

    assert!(matches!(
        executor
            .invoke(PluginRequest::ReconcileEffect {
                operation: "test.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt: attempt.clone(),
                decision: EffectReconciliationDecision::ResolveNotApplied,
                resolution_value: None,
                input: input.clone(),
            })
            .expect("first fresh child closes dispatch with a tombstone"),
        PluginResponse::ReconciliationResult {
            resolution: cymule_core::ReconciliationResolution::ResolvedNotApplied,
            value: None,
            ..
        }
    ));

    assert!(matches!(
        executor
            .invoke(PluginRequest::DispatchEffect {
                operation: "test.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt: attempt.clone(),
                input: input.clone(),
            })
            .expect("second fresh child obeys the retained tombstone"),
        PluginResponse::EffectResult {
            outcome: cymule_core::WorldOutcome::NotApplied,
            value: None,
            ..
        }
    ));

    assert!(matches!(
        executor
            .invoke(PluginRequest::ReconcileEffect {
                operation: "test.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt,
                decision: EffectReconciliationDecision::Observe,
                resolution_value: None,
                input,
            })
            .expect("third fresh child observes the same terminal truth"),
        PluginResponse::ReconciliationResult {
            resolution: cymule_core::ReconciliationResolution::ResolvedNotApplied,
            value: None,
            ..
        }
    ));
}

#[test]
fn in_flight_dispatch_cannot_be_rewritten_as_not_applied() {
    let directory = tempfile::tempdir().expect("ledger directory creates");
    let ledger = directory.path().join("effect-settlement.sqlite3");
    let executable = env!("CARGO_BIN_EXE_cymule-test-adapter");
    let mut config = process_config(executable);
    config.environment.insert(
        EFFECT_LEDGER_PATH_ENV.to_owned(),
        ledger.to_string_lossy().into_owned(),
    );
    let mut executor = ProcessExecutor::new(config).expect("test adapter executor configures");
    let intent_id = IN_FLIGHT_INTENT_ID;
    let attempt = EffectProviderAttempt::new(intent_id, "owner:fresh-child-in-flight", 1)
        .expect("provider attempt derives");
    let input = json!({"simulate": "unknown", "value": "ambiguous"});

    assert!(matches!(
        executor
            .invoke(PluginRequest::DispatchEffect {
                operation: "test.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt: attempt.clone(),
                input: input.clone(),
            })
            .expect("first fresh child retains dispatch admission"),
        PluginResponse::EffectResult {
            outcome: cymule_core::WorldOutcome::Unknown,
            ..
        }
    ));
    assert!(matches!(
        executor
            .invoke(PluginRequest::ReconcileEffect {
                operation: "test.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt: attempt.clone(),
                decision: EffectReconciliationDecision::ResolveNotApplied,
                resolution_value: None,
                input: input.clone(),
            })
            .expect("second fresh child cannot overwrite in-flight truth"),
        PluginResponse::ReconciliationResult {
            resolution: cymule_core::ReconciliationResolution::StillUnknown,
            ..
        }
    ));
    assert!(matches!(
        executor
            .invoke(PluginRequest::DispatchEffect {
                operation: "test.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt,
                input,
            })
            .expect("third fresh child sees the same in-flight truth"),
        PluginResponse::EffectResult {
            outcome: cymule_core::WorldOutcome::Unknown,
            ..
        }
    ));
}
