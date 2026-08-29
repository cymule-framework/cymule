//! Deterministic external adapter used by Cymule conformance tests.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use cymule_core::{
    ArtifactRecord, ReconciliationResolution, WorldOutcome, canonical_bytes, canonical_digest,
    decode_json,
};
use cymule_durable_protocol::ContinuationStatus;
use cymule_evolution::{
    EvolutionPluginFailure, EvolutionPluginRequest, EvolutionPluginRequestEnvelope,
    EvolutionPluginResponse, EvolutionPluginResponseEnvelope, MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES,
    MigrationAdapterDescriptor, MigrationCapabilityChange, MigrationOutput, MigrationPreservation,
    MigrationStateCoverage, ShadowBindingMode, ShadowDriverDescriptor, ShadowEffectMode,
    ShadowOutput, decode_evolution_plugin_request,
};
use cymule_runtime::{
    EffectProviderAttempt, EffectReconciliationDecision, PLUGIN_VERSION, PluginEffect,
    PluginExpectedFailure, PluginManifest, PluginOperation, PluginRequest, PluginResponse,
    decode_plugin_request, decode_strict_json_value,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

const EFFECT_LEDGER_PATH_ENV: &str = "CYMULE_TEST_EFFECT_LEDGER_PATH";
const TEST_MIGRATION_FROM_PLAN: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const TEST_MIGRATION_TO_PLAN: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const TEST_MIGRATION_PLAN_EDGE: &str =
    "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const TEST_MIGRATION_COMPATIBILITY: &str =
    "sha256:4444444444444444444444444444444444444444444444444444444444444444";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    io::stdin()
        .take(u64::try_from(MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES + 1)?)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES {
        return Err("test adapter input exceeds the largest admitted process protocol".into());
    }
    let value = decode_strict_json_value(&bytes)?;
    if value.get("evolution_plugin_protocol").is_some()
        || value.get("implementation_revision").is_some()
        || value.get("request").is_some()
    {
        let request = decode_evolution_plugin_request(&bytes)?;
        let response = evolution(request).unwrap_or_else(|error| {
            EvolutionPluginResponseEnvelope::failure(EvolutionPluginFailure::Substrate {
                code: "test_adapter_failed".to_owned(),
                message: bounded_failure_message(&error.to_string()),
            })
        });
        println!("{}", serde_json::to_string(&response)?);
        return Ok(());
    }
    let ledger_path = effect_ledger_path()?;
    let request = decode_plugin_request(&bytes)?;
    let admitted_request = request.clone();
    if matches!(
        &request,
        PluginRequest::Call { component, input }
            if component == "test.echo"
                && input.get("simulate").and_then(serde_json::Value::as_str)
                    == Some("protocol_defect")
    ) {
        // Deliberately return a strict, successfully written plugin/3 value
        // whose response variant cannot satisfy the admitted Call request.
        // The process exits zero so the host must classify protocol authority,
        // not conflate this with process-substrate failure.
        println!("{}", serde_json::to_string(&PluginResponse::Prepared)?);
        return Ok(());
    }
    let response = match request {
        PluginRequest::Describe => PluginResponse::Manifest {
            manifest: PluginManifest {
                plugin_version: PLUGIN_VERSION.to_owned(),
                implementation_id: "test-adapter@2".to_owned(),
                components: BTreeMap::from([(
                    "test.echo".to_owned(),
                    PluginOperation {
                        implementation_revision: "2".to_owned(),
                    },
                )]),
                effects: BTreeMap::from([(
                    "test.capture".to_owned(),
                    PluginEffect {
                        implementation_revision: "2".to_owned(),
                        can_reconcile: true,
                    },
                )]),
            },
        },
        PluginRequest::Call { component, input }
            if component == "test.echo"
                && input.get("simulate").and_then(serde_json::Value::as_str)
                    == Some("expected_failure") =>
        {
            PluginResponse::ExpectedFailure {
                error: PluginExpectedFailure {
                    code: "evaluation_rejected".to_owned(),
                    message: "the test evaluation was rejected".to_owned(),
                },
            }
        }
        PluginRequest::Call { component, input } if component == "test.echo" => {
            PluginResponse::CallResult { value: input }
        }
        PluginRequest::PrepareEffect { operation, .. } if operation == "test.capture" => {
            PluginResponse::Prepared
        }
        PluginRequest::DispatchEffect {
            operation,
            intent_id,
            attempt,
            input,
            ..
        } if operation == "test.capture" => {
            let (outcome, value) = dispatch_effect(&ledger_path, &intent_id, &attempt, &input)?;
            PluginResponse::EffectResult {
                attempt,
                outcome,
                value,
            }
        }
        PluginRequest::ReconcileEffect {
            operation,
            intent_id,
            attempt,
            decision,
            resolution_value,
            input,
            ..
        } if operation == "test.capture" => {
            let (resolution, value) = reconcile_effect(
                &ledger_path,
                &intent_id,
                &attempt,
                decision,
                resolution_value.as_ref(),
                &input,
            )?;
            PluginResponse::ReconciliationResult {
                attempt,
                resolution,
                value,
            }
        }
        request => PluginResponse::Defect {
            code: "unsupported_request".to_owned(),
            message: format!("unsupported test request: {request:?}"),
        },
    };
    response.verify_for(&admitted_request)?;
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}

fn bounded_failure_message(message: &str) -> String {
    let message = message.chars().take(2000).collect::<String>();
    if message.is_empty() {
        "test adapter operation failed".to_owned()
    } else {
        message
    }
}

fn effect_ledger_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = std::env::var_os(EFFECT_LEDGER_PATH_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing explicit {EFFECT_LEDGER_PATH_ENV}"))?;
    if !path.is_absolute() {
        return Err(format!("{EFFECT_LEDGER_PATH_ENV} must be an absolute path").into());
    }
    Ok(path)
}

fn open_effect_ledger(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    let connection = Connection::open(path)?;
    connection.execute_batch(
        "PRAGMA busy_timeout = 0;
         CREATE TABLE IF NOT EXISTS effect_settlement (
            intent_id TEXT PRIMARY KEY NOT NULL,
            attempt_json BLOB NOT NULL,
            input_json BLOB NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('dispatching','applied','not_applied')),
            result_json BLOB
         ) STRICT;",
    )?;
    Ok(connection)
}

type EffectLedgerRow = (Vec<u8>, Vec<u8>, String, Option<Vec<u8>>);

fn read_effect_row(
    transaction: &rusqlite::Transaction<'_>,
    intent_id: &str,
) -> rusqlite::Result<Option<EffectLedgerRow>> {
    transaction
        .query_row(
            "SELECT attempt_json, input_json, state, result_json
             FROM effect_settlement WHERE intent_id = ?1",
            [intent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
}

fn verify_effect_row(
    row: &EffectLedgerRow,
    attempt: &[u8],
    input: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    if row.0 != attempt || row.1 != input {
        return Err("effect intent was reused outside its exact provider attempt".into());
    }
    Ok(())
}

fn decode_result(
    bytes: Option<&[u8]>,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    bytes.map(decode_json).transpose().map_err(Into::into)
}

fn dispatch_effect(
    ledger_path: &Path,
    intent_id: &str,
    attempt: &EffectProviderAttempt,
    input: &serde_json::Value,
) -> Result<(WorldOutcome, Option<serde_json::Value>), Box<dyn std::error::Error>> {
    let attempt_bytes = cymule_core::canonical_bytes(attempt)?;
    let input_bytes = cymule_core::canonical_bytes(input)?;
    let mut connection = open_effect_ledger(ledger_path)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let existing = read_effect_row(&transaction, intent_id)?;
    if let Some(row) = existing {
        verify_effect_row(&row, &attempt_bytes, &input_bytes)?;
        let outcome = match row.2.as_str() {
            "applied" => (WorldOutcome::Applied, decode_result(row.3.as_deref())?),
            "not_applied" => (WorldOutcome::NotApplied, None),
            "dispatching" => (WorldOutcome::Unknown, None),
            _ => return Err("effect settlement ledger contains an invalid state".into()),
        };
        transaction.commit()?;
        return Ok(outcome);
    }

    // First-dispatch admission is provider authority and must be durable before
    // any simulated world mutation. A crash after this commit therefore leaves
    // an honest Dispatching record which reconciliation cannot rewrite as
    // NotApplied.
    transaction.execute(
        "INSERT INTO effect_settlement(
            intent_id, attempt_json, input_json, state, result_json
         ) VALUES (?1, ?2, ?3, 'dispatching', NULL)",
        params![intent_id, attempt_bytes, input_bytes],
    )?;
    transaction.commit()?;

    if input.get("simulate").and_then(serde_json::Value::as_str) == Some("unknown") {
        return Ok((WorldOutcome::Unknown, None));
    }

    let result_bytes = cymule_core::canonical_bytes(input)?;
    let mut connection = open_effect_ledger(ledger_path)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row = read_effect_row(&transaction, intent_id)?
        .ok_or("effect dispatch admission disappeared before settlement")?;
    verify_effect_row(&row, &attempt_bytes, &input_bytes)?;
    if row.2 != "dispatching" {
        return Err("effect dispatch admission changed before settlement".into());
    }
    transaction.execute(
        "UPDATE effect_settlement SET state = 'applied', result_json = ?2
         WHERE intent_id = ?1 AND state = 'dispatching'",
        params![intent_id, result_bytes],
    )?;
    transaction.commit()?;
    Ok((WorldOutcome::Applied, Some(input.clone())))
}

fn reconcile_effect(
    ledger_path: &Path,
    intent_id: &str,
    attempt: &EffectProviderAttempt,
    decision: EffectReconciliationDecision,
    resolution_value: Option<&serde_json::Value>,
    input: &serde_json::Value,
) -> Result<(ReconciliationResolution, Option<serde_json::Value>), Box<dyn std::error::Error>> {
    let attempt_bytes = cymule_core::canonical_bytes(attempt)?;
    let input_bytes = cymule_core::canonical_bytes(input)?;
    let mut connection = open_effect_ledger(ledger_path)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let existing = read_effect_row(&transaction, intent_id)?;
    let (resolution, value) = if let Some(row) = existing {
        verify_effect_row(&row, &attempt_bytes, &input_bytes)?;
        match row.2.as_str() {
            "dispatching" => (ReconciliationResolution::StillUnknown, None),
            "applied" => (
                ReconciliationResolution::ResolvedApplied,
                decode_result(row.3.as_deref())?,
            ),
            "not_applied" => (ReconciliationResolution::ResolvedNotApplied, None),
            _ => return Err("effect settlement ledger contains an invalid state".into()),
        }
    } else {
        let (state, resolution, result) = match decision {
            EffectReconciliationDecision::ResolveApplied => (
                "applied",
                ReconciliationResolution::ResolvedApplied,
                resolution_value.cloned(),
            ),
            EffectReconciliationDecision::Observe
            | EffectReconciliationDecision::ResolveNotApplied => (
                "not_applied",
                ReconciliationResolution::ResolvedNotApplied,
                None,
            ),
        };
        let result_bytes = result
            .as_ref()
            .map(cymule_core::canonical_bytes)
            .transpose()?;
        transaction.execute(
            "INSERT INTO effect_settlement(
                intent_id, attempt_json, input_json, state, result_json
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![intent_id, attempt_bytes, input_bytes, state, result_bytes],
        )?;
        (resolution, result)
    };
    transaction.commit()?;
    Ok((resolution, value))
}

fn evolution(
    envelope: EvolutionPluginRequestEnvelope,
) -> Result<EvolutionPluginResponseEnvelope, Box<dyn std::error::Error>> {
    let response = match envelope.request {
        EvolutionPluginRequest::DescribeMigration {} => {
            EvolutionPluginResponse::MigrationDescriptor {
                descriptor: MigrationAdapterDescriptor {
                    adapter_id: "test.migration".to_owned(),
                    adapter_revision: envelope.implementation_revision,
                    from_plan: TEST_MIGRATION_FROM_PLAN.to_owned(),
                    to_plan: TEST_MIGRATION_TO_PLAN.to_owned(),
                    plan_edge_id: TEST_MIGRATION_PLAN_EDGE.to_owned(),
                    compatibility_id: TEST_MIGRATION_COMPATIBILITY.to_owned(),
                    from_schema: "schema:test-from".to_owned(),
                    to_schema: "schema:test-to".to_owned(),
                    state_coverage: MigrationStateCoverage::TotalReachableState,
                    failure_and_cancellation: MigrationPreservation::Preserved,
                    budget_and_ownership: MigrationPreservation::Preserved,
                    authority_and_effects: MigrationCapabilityChange::NoWidening,
                },
            }
        }
        EvolutionPluginRequest::Migrate { request } => {
            if request.intent.from_plan != TEST_MIGRATION_FROM_PLAN
                || request.intent.to_plan != TEST_MIGRATION_TO_PLAN
                || request.intent.plan_edge_id != TEST_MIGRATION_PLAN_EDGE
                || request.intent.compatibility_id != TEST_MIGRATION_COMPATIBILITY
            {
                return Err("migration request is outside the adapter's pinned descriptor".into());
            }
            request.source_continuation.verify_wire()?;
            if request.source_continuation.run_id != request.intent.run_id
                || request.source_continuation.plan_id != request.intent.from_plan
                || request.source_continuation.binding_context != request.source_binding.artifact_id
                || request.source_continuation.epoch != request.intent.expected_source_epoch
            {
                return Err("migration request disagrees with its source Continuation".into());
            }
            let state = artifact_record(
                "cymule.test/migration-state/1",
                canonical_bytes(&("migrated", request.intent.migration_id.as_str()))?,
            )?;
            let evidence = artifact_record(
                "cymule.test/migration-evidence/1",
                canonical_bytes(&("migration-evidence", request.as_ref()))?,
            )?;
            let mut continuation = request.source_continuation.clone();
            continuation.plan_id.clone_from(&request.intent.to_plan);
            continuation
                .binding_context
                .clone_from(&request.target_binding.artifact_id);
            for frame in &mut continuation.frames {
                frame.invocation_id = cymule_core::plan_invocation_id(
                    &request.intent.run_id,
                    &request.intent.to_plan,
                    &frame.definition_id,
                    &frame.invocation_path,
                )?;
                frame.next_step = 0;
            }
            continuation.epoch = request
                .intent
                .expected_source_epoch
                .checked_add(1)
                .filter(|epoch| *epoch <= cymule_core::MAX_EXACT_INTEGER)
                .ok_or("migration target epoch exceeds the shared exact range")?;
            continuation.execution_claim = None;
            continuation.status = ContinuationStatus::Ready;
            continuation.state = Some(state.reference.clone());
            continuation.verify_wire()?;
            EvolutionPluginResponse::Migrated {
                output: Box::new(MigrationOutput {
                    continuation,
                    artifacts: vec![state],
                    evidence,
                }),
            }
        }
        EvolutionPluginRequest::DescribeShadow {} => EvolutionPluginResponse::ShadowDescriptor {
            descriptor: ShadowDriverDescriptor {
                driver_id: "test.shadow".to_owned(),
                driver_revision: envelope.implementation_revision,
                target_effects: ShadowEffectMode::SuppressedOrSimulated,
                occurrence_bindings: ShadowBindingMode::Pinned,
            },
        },
        EvolutionPluginRequest::ExecuteShadow { request } => {
            request.input.validate()?;
            if request.primary_plan == request.shadow_plan {
                return Err("shadow request requires distinct primary and shadow Plans".into());
            }
            let primary_digest = canonical_digest(&(
                "primary",
                request.primary_plan.as_str(),
                &request.input,
                request.comparison_policy.as_str(),
            ))?;
            let equivalent = request.comparison_policy != "policy:inequivalent";
            let shadow_digest = if equivalent {
                primary_digest.clone()
            } else {
                canonical_digest(&(
                    "shadow",
                    request.shadow_plan.as_str(),
                    &request.input,
                    request.comparison_policy.as_str(),
                ))?
            };
            let evidence = artifact_record(
                "cymule.test/shadow-evidence/1",
                canonical_bytes(&(request, &primary_digest, &shadow_digest, equivalent))?,
            )?;
            EvolutionPluginResponse::ShadowExecuted {
                output: ShadowOutput {
                    primary_digest,
                    shadow_digest,
                    equivalent,
                    evidence,
                },
            }
        }
    };
    Ok(EvolutionPluginResponseEnvelope::success(response))
}

fn artifact_record(
    kind: &str,
    bytes: Vec<u8>,
) -> Result<ArtifactRecord, Box<dyn std::error::Error>> {
    Ok(ArtifactRecord {
        reference: cymule_core::artifact_ref(kind, &bytes)?,
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use cymule_durable_protocol::{Continuation, FrameState};
    use cymule_evolution::{EvolutionPluginMigrationRequest, MigrationRequest, ShadowRequest};

    const REVISION: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn attempt(intent_id: &str) -> EffectProviderAttempt {
        EffectProviderAttempt::new(intent_id, "test:provider", 1).unwrap()
    }

    fn valid_envelope(request: EvolutionPluginRequest) -> EvolutionPluginRequestEnvelope {
        EvolutionPluginRequestEnvelope::new(REVISION, request)
    }

    fn migration_request() -> EvolutionPluginRequest {
        let run_id = "run:test-adapter-migration";
        let source_binding =
            cymule_core::artifact_ref(cymule_runtime::EXECUTION_BINDING_VERSION, b"source binding")
                .unwrap();
        let target_binding =
            cymule_core::artifact_ref(cymule_runtime::EXECUTION_BINDING_VERSION, b"target binding")
                .unwrap();
        let state =
            cymule_core::artifact_ref("cymule.test/migration-input/1", b"source state").unwrap();
        let frame_input =
            cymule_core::artifact_ref("cymule.test/frame-input/1", b"frame input").unwrap();
        let invocation_id =
            cymule_core::plan_invocation_id(run_id, TEST_MIGRATION_FROM_PLAN, "main", &[]).unwrap();
        EvolutionPluginRequest::Migrate {
            request: Box::new(EvolutionPluginMigrationRequest {
                intent: MigrationRequest {
                    migration_id: "migration:test-adapter".to_owned(),
                    run_id: run_id.to_owned(),
                    from_plan: TEST_MIGRATION_FROM_PLAN.to_owned(),
                    to_plan: TEST_MIGRATION_TO_PLAN.to_owned(),
                    plan_edge_id: TEST_MIGRATION_PLAN_EDGE.to_owned(),
                    compatibility_id: TEST_MIGRATION_COMPATIBILITY.to_owned(),
                    expected_source_epoch: 1,
                    adapter_id: "test.migration".to_owned(),
                    adapter_revision: REVISION.to_owned(),
                },
                source_witness_id: format!("sha256:{}", "5".repeat(64)),
                source_continuation: Continuation {
                    continuation_version: cymule_durable_protocol::CONTINUATION_STATE_VERSION
                        .to_owned(),
                    run_id: run_id.to_owned(),
                    plan_id: TEST_MIGRATION_FROM_PLAN.to_owned(),
                    binding_context: source_binding.artifact_id.clone(),
                    frames: vec![FrameState {
                        definition_id: "main".to_owned(),
                        invocation_id,
                        invocation_path: Vec::new(),
                        scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                        input: frame_input,
                        region_path: Vec::new(),
                        next_step: 0,
                        locals: BTreeMap::new(),
                    }],
                    state: Some(state.clone()),
                    wait_set: BTreeSet::new(),
                    scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
                    epoch: 1,
                    execution_fence: 1,
                    execution_claim: None,
                    status: ContinuationStatus::Ready,
                },
                input_state: state,
                source_binding,
                target_binding,
            }),
        }
    }

    fn shadow_request(policy: &str) -> EvolutionPluginRequest {
        EvolutionPluginRequest::ExecuteShadow {
            request: Box::new(ShadowRequest {
                comparison_id: "comparison:test-adapter".to_owned(),
                decision_id: "decision:test-adapter".to_owned(),
                subject: "occurrence:test-adapter".to_owned(),
                primary_plan: format!("sha256:{}", "6".repeat(64)),
                shadow_plan: format!("sha256:{}", "7".repeat(64)),
                input: cymule_core::artifact_ref("cymule.test/shadow-input/1", b"shadow input")
                    .unwrap(),
                driver_id: "test.shadow".to_owned(),
                driver_revision: REVISION.to_owned(),
                comparison_policy: policy.to_owned(),
            }),
        }
    }

    #[test]
    fn evolution_ingress_rejects_wrong_fields_unsafe_numbers_duplicates_and_versions() {
        for input in [
            format!(
                r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/3","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow","wrong":true}}}}"#
            ),
            format!(
                r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/3","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow"}},"unsafe":9007199254740992.0}}"#
            ),
            format!(
                r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/3","implementation_revision":"{REVISION}","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow"}}}}"#
            ),
            format!(
                r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/1","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow"}}}}"#
            ),
            format!(
                r#"{{"implementation_revision":"{REVISION}","request":{{"type":"describe_shadow"}}}}"#
            ),
        ] {
            assert!(
                decode_evolution_plugin_request(input.as_bytes()).is_err(),
                "malformed request was admitted: {input}"
            );
        }
    }

    #[test]
    fn evolution_descriptor_and_execution_paths_are_real_typed_operations() {
        let descriptor = evolution(valid_envelope(EvolutionPluginRequest::DescribeMigration {}))
            .unwrap()
            .into_result()
            .unwrap();
        assert!(matches!(
            descriptor,
            EvolutionPluginResponse::MigrationDescriptor { descriptor }
                if descriptor.plan_edge_id == TEST_MIGRATION_PLAN_EDGE
                    && descriptor.compatibility_id == TEST_MIGRATION_COMPATIBILITY
        ));

        let migrated = evolution(valid_envelope(migration_request()))
            .unwrap()
            .into_result()
            .unwrap();
        let EvolutionPluginResponse::Migrated { output } = migrated else {
            panic!("migration request returned the wrong variant");
        };
        assert_eq!(output.continuation.plan_id, TEST_MIGRATION_TO_PLAN);
        assert_eq!(output.continuation.epoch, 2);
        assert_eq!(output.continuation.status, ContinuationStatus::Ready);
        assert!(output.continuation.execution_claim.is_none());
        assert_eq!(output.artifacts.len(), 1);
        for record in output.artifacts.iter().chain([&output.evidence]) {
            assert_eq!(
                record.reference,
                cymule_core::artifact_ref(record.reference.kind.as_str(), &record.bytes).unwrap()
            );
        }

        for (policy, expected) in [("policy:exact", true), ("policy:inequivalent", false)] {
            let shadow = evolution(valid_envelope(shadow_request(policy)))
                .unwrap()
                .into_result()
                .unwrap();
            let EvolutionPluginResponse::ShadowExecuted { output } = shadow else {
                panic!("shadow request returned the wrong variant");
            };
            assert_eq!(output.equivalent, expected);
            assert_eq!(
                output.primary_digest == output.shadow_digest,
                expected,
                "digest equality must match the deterministic comparison result"
            );
            assert_eq!(
                output.evidence.reference,
                cymule_core::artifact_ref(
                    output.evidence.reference.kind.as_str(),
                    &output.evidence.bytes,
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn not_applied_tombstone_rejects_every_late_dispatch() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = directory.path().join("effect.sqlite3");
        let intent_id = cymule_core::content_id(
            "cymule.test-effect-intent/1",
            &("tombstone", std::process::id()),
        )
        .unwrap();
        let input = serde_json::json!({"value": "never-apply"});
        let (resolution, value) = reconcile_effect(
            &ledger,
            &intent_id,
            &attempt(&intent_id),
            EffectReconciliationDecision::ResolveNotApplied,
            None,
            &input,
        )
        .unwrap();
        assert_eq!(resolution, ReconciliationResolution::ResolvedNotApplied);
        assert!(value.is_none());

        for _ in 0..2 {
            assert_eq!(
                dispatch_effect(&ledger, &intent_id, &attempt(&intent_id), &input).unwrap(),
                (WorldOutcome::NotApplied, None)
            );
        }
    }

    #[test]
    fn applied_dispatch_wins_over_a_later_not_applied_request() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = directory.path().join("effect.sqlite3");
        let intent_id = cymule_core::content_id(
            "cymule.test-effect-intent/1",
            &("applied", std::process::id()),
        )
        .unwrap();
        let input = serde_json::json!({"value": "applied"});
        assert_eq!(
            dispatch_effect(&ledger, &intent_id, &attempt(&intent_id), &input).unwrap(),
            (WorldOutcome::Applied, Some(input.clone()))
        );
        assert_eq!(
            reconcile_effect(
                &ledger,
                &intent_id,
                &attempt(&intent_id),
                EffectReconciliationDecision::ResolveNotApplied,
                None,
                &input,
            )
            .unwrap(),
            (ReconciliationResolution::ResolvedApplied, Some(input))
        );
    }

    #[test]
    fn resultless_applied_resolution_is_persisted_without_synthesizing_input() {
        let directory = tempfile::tempdir().unwrap();
        let ledger = directory.path().join("effect.sqlite3");
        let intent_id = cymule_core::content_id(
            "cymule.test-effect-intent/1",
            &("resultless-applied", std::process::id()),
        )
        .unwrap();
        let input = serde_json::json!({"value": "not-a-result"});

        for decision in [
            EffectReconciliationDecision::ResolveApplied,
            EffectReconciliationDecision::Observe,
        ] {
            assert_eq!(
                reconcile_effect(
                    &ledger,
                    &intent_id,
                    &attempt(&intent_id),
                    decision,
                    None,
                    &input,
                )
                .unwrap(),
                (ReconciliationResolution::ResolvedApplied, None)
            );
        }
    }

    #[test]
    fn provider_ledger_never_waits_on_sqlite_writer_contention() {
        let directory = tempfile::tempdir().unwrap();
        let connection = open_effect_ledger(&directory.path().join("effect.sqlite3")).unwrap();
        let busy_timeout_ms: i64 = connection
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(busy_timeout_ms, 0);
    }
}
