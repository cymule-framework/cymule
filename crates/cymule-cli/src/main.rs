//! Command-line and JSON RPC transport for the Cymule engine.

use std::collections::BTreeMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

use cymule_clock_system::SqliteClock;
use cymule_core::{PlanCandidate, SealedPlan, seal_plan};
use cymule_directory_store::DirectoryStore;
use cymule_durable::{
    DurableCommand, DurableProviderControl, DurableResponse, DurableRuntimeControl, DurableStore,
    DurableStoreControl, GcReceipt, StoreBatch, StoreCommit, StoreHead, StoreStats,
};
use cymule_durable_protocol::{ClockObservationRef, WaitActivation, execution_clock_scope};
use cymule_evolution::{
    EvolutionCommand, EvolutionCommit, EvolutionError, EvolutionPersistenceCommand,
    EvolutionPluginMigrationRequest, EvolutionPluginRequest, EvolutionPluginRequestEnvelope,
    EvolutionPluginResponse, EvolutionProviders, EvolutionReceiptQuery, EvolutionResult,
    LiveEvolutionCommand, MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES, MigrationAdapter,
    MigrationAdapterDescriptor, MigrationAdapterRequest, MigrationOutput, NoEvolutionProviders,
    ShadowDriver, ShadowDriverDescriptor, ShadowOutput, ShadowRequest,
    decode_evolution_plugin_response,
};
use cymule_executor_process::{ProcessCancellation, ProcessExecutor, ProcessExecutorConfig};
use cymule_resource::{ResourceCandidate, ResourceHandle};
use cymule_runtime::{
    ENGINE_CLOCK_SYSTEM_PROVIDER, ENGINE_DIRECTORY_STORE_PROVIDER,
    ENGINE_PROCESS_EXECUTOR_PROVIDER, ENGINE_PROTOCOL_VERSION, ENGINE_SQLITE_STORE_PROVIDER,
    EmbeddedRuntime, EngineClockTarget, EngineContractSide, EngineDurableTarget, EngineFailure,
    EngineFailureCategory, EngineIssue, EnginePhase, EnginePluginTarget, EngineRequestEnvelope,
    EngineResponseEnvelope, EngineRetryDisposition, ExecutionBinding, ExecutionBindingAdmission,
    ExecutionOutcome, PluginHost, decode_strict_json_value, validate_json_member_presence,
    verify_execution_request, verify_plan,
};
use cymule_store_sqlite::SqliteStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EngineRequest {
    Seal {
        candidate: PlanCandidate,
    },
    Verify {
        plan: SealedPlan,
    },
    SealResource {
        candidate: ResourceCandidate,
    },
    VerifyWaitActivation {
        activation: WaitActivation,
    },
    VerifyDurableCommand {
        command: DurableCommand,
    },
    ObserveClock {
        target: EngineClockTarget,
        run_id: String,
    },
    VerifyEvolutionCommand {
        command: EvolutionCommand,
    },
    VerifyLiveEvolutionCommand {
        command: LiveEvolutionCommand,
    },
    ExecuteDurable {
        target: EngineDurableTarget,
        command: DurableCommand,
    },
    ExecuteLiveEvolution {
        target: cymule_runtime::EngineEvolutionTarget,
        evolution_id: String,
        command: LiveEvolutionCommand,
    },
    Run {
        plan: SealedPlan,
        input: Value,
        plugin: EnginePluginTarget,
        run_id: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EngineResponse {
    Sealed { plan: SealedPlan },
    SealedResource { resource: ResourceHandle },
    VerifiedWaitActivation { activation: WaitActivation },
    VerifiedDurableCommand { command: DurableCommand },
    ClockObserved { observation: ClockObservationRef },
    VerifiedEvolutionCommand { command: EvolutionCommand },
    VerifiedLiveEvolutionCommand { command: LiveEvolutionCommand },
    ExecutionBoundary { execution: ExecutionOutcome },
    DurableExecuted { response: DurableResponse },
    LiveEvolutionExecuted { commit: Box<EvolutionCommit> },
    Verified,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    match arguments.first().map(String::as_str) {
        Some("rpc") => rpc(),
        Some("seal") => {
            let candidate: PlanCandidate = read_path(argument_value(&arguments, "--input")?)?;
            let plan = seal_plan(candidate)?;
            print_json(&plan)
        }
        Some("verify") => {
            let plan: SealedPlan = read_path(argument_value(&arguments, "--plan")?)?;
            verify_plan(&plan)?;
            println!("verified {}", plan.plan_id);
            Ok(())
        }
        Some("resource") if arguments.get(1).map(String::as_str) == Some("seal") => {
            let candidate: ResourceCandidate = read_path(argument_value(&arguments, "--input")?)?;
            print_json(&candidate.seal()?)
        }
        Some("wait-activation") if arguments.get(1).map(String::as_str) == Some("verify") => {
            let activation: WaitActivation = read_path(argument_value(&arguments, "--input")?)?;
            activation.verify()?;
            print_json(&activation)
        }
        Some("durable-command") if arguments.get(1).map(String::as_str) == Some("verify") => {
            let command: DurableCommand = read_path(argument_value(&arguments, "--input")?)?;
            command.verify()?;
            print_json(&command)
        }
        Some("evolution-command") if arguments.get(1).map(String::as_str) == Some("verify") => {
            let command: EvolutionCommand = read_path(argument_value(&arguments, "--input")?)?;
            command.verify()?;
            print_json(&command)
        }
        Some("live-evolution-command")
            if arguments.get(1).map(String::as_str) == Some("verify") =>
        {
            let command: LiveEvolutionCommand =
                read_path(argument_value(&arguments, "--input")?)?;
            command.verify()?;
            print_json(&command)
        }
        Some("run") => {
            let plan: SealedPlan = read_path(argument_value(&arguments, "--plan")?)?;
            let input: Value = read_path(argument_value(&arguments, "--input")?)?;
            let plugin: EnginePluginTarget =
                read_path(argument_value(&arguments, "--plugin-target")?)?;
            let run_id = argument_value(&arguments, "--run-id")?;
            verify_execution_request(&plan, &input, run_id)?;
            admit_local_process_target(&plugin, EnginePhase::ExecutePlan)?;
            let mut runtime = local_process_runtime(&plugin, None)?;
            let result = runtime.execute(plan, &input, run_id)?;
            print_json(&result)
        }
        _ => Err(
            "usage: cymule <rpc|seal|verify|run|resource seal|wait-activation verify|durable-command verify|evolution-command verify|live-evolution-command verify> [options]; run requires --plan, --input, --plugin-target, and --run-id"
                .into(),
        ),
    }
}

fn rpc() -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = install_cancellation()?;
    let mut input = Vec::new();
    let response = match io::stdin().read_to_end(&mut input) {
        Ok(_) => decode_and_execute_request_with_cancellation(&input, Some(&cancellation)),
        Err(error) => Err(EngineFailure::transport(
            "engine_read_failed",
            error.to_string(),
        )),
    };
    let response = match response {
        Ok((request, response)) => EngineResponseEnvelope::success(request, response),
        Err(error) => {
            EngineResponseEnvelope::<Value, EngineResponse>::failure(error.into_wire_failure())
        }
    };
    print_json(&response)
}

#[cfg(test)]
fn decode_and_execute_request(input: &[u8]) -> Result<(Value, EngineResponse), EngineFailure> {
    decode_and_execute_request_with_cancellation(input, None)
}

fn decode_and_execute_request_with_cancellation(
    input: &[u8],
    cancellation: Option<&ProcessCancellation>,
) -> Result<(Value, EngineResponse), EngineFailure> {
    let raw_envelope = decode_strict_json_value(input).map_err(|error| {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::DecodeRequest,
            "invalid_engine_request",
            error,
        );
        failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        failure
    })?;
    let envelope: EngineRequestEnvelope<Value> =
        serde_json::from_value(raw_envelope).map_err(|error| {
            let mut failure = EngineFailure::new(
                EngineFailureCategory::Validation,
                EnginePhase::DecodeRequest,
                "invalid_engine_request",
                error.to_string(),
            );
            failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
            failure
        })?;
    if envelope.engine_protocol != ENGINE_PROTOCOL_VERSION {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::ContractViolation,
            EnginePhase::ValidateRequest,
            "unsupported_engine_protocol",
            format!(
                "expected {ENGINE_PROTOCOL_VERSION}, received {:?}",
                envelope.engine_protocol
            ),
        );
        failure.contract = Some(ENGINE_PROTOCOL_VERSION.into());
        failure.retry_disposition = Some(EngineRetryDisposition::Never);
        return Err(failure);
    }
    let retained_request = envelope.request;
    let request: EngineRequest =
        serde_json::from_value(retained_request.clone()).map_err(|error| {
            let mut failure = EngineFailure::new(
                EngineFailureCategory::Validation,
                EnginePhase::DecodeRequest,
                "invalid_engine_request",
                error.to_string(),
            );
            failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
            failure
        })?;
    let normalized_request = serde_json::to_value(&request).map_err(|error| {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ValidateRequest,
            "invalid_engine_request",
            error.to_string(),
        );
        failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        failure
    })?;
    validate_json_member_presence(&retained_request, &normalized_request).map_err(|error| {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ValidateRequest,
            "invalid_engine_request",
            error,
        );
        failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        failure
    })?;
    let response = execute_engine_request(request, cancellation)?;
    Ok((normalized_request, response))
}

fn execute_engine_request(
    request: EngineRequest,
    cancellation: Option<&ProcessCancellation>,
) -> Result<EngineResponse, EngineFailure> {
    Ok(match request {
        EngineRequest::Seal { candidate } => EngineResponse::Sealed {
            plan: seal_plan(candidate).map_err(|error| {
                EngineFailure::from_runtime(error.into(), EnginePhase::SealPlan)
            })?,
        },
        EngineRequest::Verify { plan } => {
            verify_plan(&plan).map_err(|error| {
                EngineFailure::from_runtime(error.into(), EnginePhase::VerifyPlan)
            })?;
            EngineResponse::Verified
        }
        EngineRequest::SealResource { candidate } => EngineResponse::SealedResource {
            resource: candidate
                .seal()
                .map_err(|error| map_resource_error(&error))?,
        },
        EngineRequest::VerifyWaitActivation { activation } => {
            activation.verify().map_err(|error| {
                map_durable_error(
                    &cymule_durable::DurableError::from(error),
                    EnginePhase::VerifyWaitActivation,
                )
            })?;
            EngineResponse::VerifiedWaitActivation { activation }
        }
        EngineRequest::VerifyDurableCommand { command } => {
            command
                .verify()
                .map_err(|error| map_durable_error(&error, EnginePhase::VerifyDurableCommand))?;
            EngineResponse::VerifiedDurableCommand { command }
        }
        EngineRequest::ObserveClock { target, run_id } => EngineResponse::ClockObserved {
            observation: observe_clock(&target, &run_id)?,
        },
        EngineRequest::VerifyEvolutionCommand { command } => {
            command.verify().map_err(|error| {
                map_evolution_error(&error, EnginePhase::VerifyEvolutionCommand)
            })?;
            EngineResponse::VerifiedEvolutionCommand { command }
        }
        EngineRequest::VerifyLiveEvolutionCommand { command } => {
            command.verify().map_err(|error| {
                map_evolution_error(&error, EnginePhase::VerifyLiveEvolutionCommand)
            })?;
            EngineResponse::VerifiedLiveEvolutionCommand { command }
        }
        EngineRequest::ExecuteDurable { target, command } => EngineResponse::DurableExecuted {
            response: execute_durable(&target, command, cancellation.cloned())?,
        },
        EngineRequest::ExecuteLiveEvolution {
            target,
            evolution_id,
            command,
        } => EngineResponse::LiveEvolutionExecuted {
            commit: Box::new(execute_live_evolution(
                &target,
                &evolution_id,
                command,
                cancellation.cloned(),
            )?),
        },
        EngineRequest::Run {
            plan,
            input,
            plugin,
            run_id,
        } => {
            verify_execution_request(&plan, &input, &run_id)
                .map_err(|error| EngineFailure::from_runtime(error, EnginePhase::ExecutePlan))?;
            admit_local_process_target(&plugin, EnginePhase::ExecutePlan)?;
            let mut runtime = local_process_runtime(&plugin, cancellation.cloned())
                .map_err(|error| EngineFailure::from_runtime(error, EnginePhase::ExecutePlan))?;
            EngineResponse::ExecutionBoundary {
                execution: runtime.execute(plan, &input, run_id).map_err(|error| {
                    EngineFailure::from_runtime(error, EnginePhase::ExecutePlan)
                })?,
            }
        }
    })
}

fn execute_durable(
    target: &EngineDurableTarget,
    command: DurableCommand,
    cancellation: Option<ProcessCancellation>,
) -> Result<DurableResponse, EngineFailure> {
    target.verify()?;
    command
        .verify()
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
    let admitted = admit_durable_request(target, &command)?;
    if command.is_read_only() {
        let store = open_store_read_only(&admitted.store)
            .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
        let response = DurableStoreControl::open(store)
            .and_then(|mut control| control.submit(command.clone()))
            .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
        response
            .verify_query_for(&command)
            .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
        return Ok(response);
    }
    if matches!(command, DurableCommand::ResolveEffect { .. }) {
        let store = open_existing_store_read_only(&admitted.store)
            .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?
            .ok_or_else(|| {
                map_durable_error(
                    &cymule_durable::DurableError::NotFound(
                        "durable store does not exist".to_owned(),
                    ),
                    EnginePhase::ExecuteDurable,
                )
            })?;
        let mut control = DurableStoreControl::open(store)
            .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
        if let Some(response) = control
            .replay_effect_resolution(&command)
            .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?
        {
            return Ok(response);
        }
    }
    match admitted.capability {
        DurableCapability::StoreOnly => {
            let store = open_store(&admitted.store)
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
            DurableStoreControl::open(store)
                .and_then(|mut control| control.submit(command))
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))
        }
        DurableCapability::ProviderOnly { executor } => {
            let admission = prepare_durable_admission(executor, cancellation)
                .map_err(|error| map_durable_pre_execution_error(&error))?;
            let store = open_store(&admitted.store)
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
            DurableProviderControl::open(store, admission)
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?
                .submit(command)
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))
        }
        DurableCapability::Execution { executor, clock } => {
            let execution = prepare_durable_execution(executor, &clock, cancellation)
                .map_err(|error| map_durable_pre_execution_error(&error))?;
            reject_store_clock_alias(&admitted.store, &clock.authority)?;
            let store = open_store(&admitted.store)
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
            reject_store_clock_alias(&admitted.store, &clock.authority)?;
            DurableRuntimeControl::open(store, execution.admission, execution.clock)
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?
                .submit(command)
                .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))
        }
    }
}

fn execute_live_evolution(
    target: &cymule_runtime::EngineEvolutionTarget,
    evolution_id: &str,
    command: LiveEvolutionCommand,
    cancellation: Option<ProcessCancellation>,
) -> Result<EvolutionCommit, EngineFailure> {
    target.verify()?;
    command
        .verify()
        .map_err(|error| map_evolution_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    admit_evolution_provider_selection(target, &command)?;
    let store_target = admit_store_target(&target.store, EnginePhase::ExecuteLiveEvolution)?;
    let persistence = EvolutionPersistenceCommand::new(evolution_id, command)
        .map_err(|error| map_evolution_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    if let Some(commit) = read_exact_evolution_replay(&store_target, &persistence)? {
        return Ok(commit);
    }
    require_fresh_evolution_provider_selection(target, &persistence.command)?;
    let mut providers = CliEvolutionProviders::new(target, cancellation);
    let store = open_store(&store_target)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let mut control = DurableStoreControl::open(store)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    control
        .evolution(&mut providers)
        .commit(&persistence)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))
}

fn read_exact_evolution_replay(
    store_target: &AdmittedStoreTarget<'_>,
    command: &EvolutionPersistenceCommand,
) -> Result<Option<EvolutionCommit>, EngineFailure> {
    let mut store = match open_existing_store_read_only(store_target) {
        Ok(Some(store)) => store,
        Ok(None) | Err(cymule_durable::DurableError::NotFound(_)) => return Ok(None),
        Err(error) => {
            return Err(map_durable_error(&error, EnginePhase::ExecuteLiveEvolution));
        }
    };
    if store
        .load_head()
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?
        .is_none()
    {
        return Ok(None);
    }
    let mut control = DurableStoreControl::open(store)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let query = EvolutionReceiptQuery {
        evolution_id: command.evolution_id.clone(),
        command_id: command.command.command_id().to_owned(),
        expected_revision: None,
    };
    let read = control
        .evolution(&mut NoEvolutionProviders)
        .read_receipt(&query)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let Some(receipt) = read.receipt else {
        return Ok(None);
    };
    if receipt.command != *command {
        return Err(map_durable_error(
            &cymule_durable::DurableError::HistoryConflict {
                code: "evolution_command_reused".to_owned(),
                message: format!(
                    "Evolution command {} was reused with different semantics",
                    command.command.command_id()
                ),
            },
            EnginePhase::ExecuteLiveEvolution,
        ));
    }
    let commit = EvolutionCommit {
        observed_revision: read.observed_revision,
        committed_revision: None,
        receipt,
    };
    commit
        .verify_for(command)
        .map_err(|error| map_evolution_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    Ok(Some(commit))
}

fn require_fresh_evolution_provider_selection(
    target: &cymule_runtime::EngineEvolutionTarget,
    command: &LiveEvolutionCommand,
) -> Result<(), EngineFailure> {
    match command {
        LiveEvolutionCommand::Apply { command, .. } => match command.as_ref() {
            EvolutionCommand::Migrate { request, .. }
                if target.migration_adapter.is_none()
                    || !target
                        .target_execution_bindings
                        .contains_key(&request.to_plan) =>
            {
                Err(evolution_target_failure(
                    "missing_migration_provider_target",
                    "fresh migration requires its exact target execution binding and adapter",
                ))
            }
            EvolutionCommand::Shadow { .. } if target.shadow_driver.is_none() => {
                Err(evolution_target_failure(
                    "missing_shadow_provider_target",
                    "fresh shadow command requires its exact driver",
                ))
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

fn admit_evolution_provider_selection(
    target: &cymule_runtime::EngineEvolutionTarget,
    command: &LiveEvolutionCommand,
) -> Result<(), EngineFailure> {
    match command {
        LiveEvolutionCommand::Apply { command, .. } => match command.as_ref() {
            EvolutionCommand::Migrate { request, .. } => {
                if target.shadow_driver.is_some() {
                    return Err(evolution_target_failure(
                        "unexpected_shadow_driver",
                        "migration does not accept a shadow driver",
                    ));
                }
                let binding = target.target_execution_bindings.get(&request.to_plan);
                match (binding, target.migration_adapter.as_ref()) {
                    (None, None) if target.target_execution_bindings.is_empty() => Ok(()),
                    (Some(binding), Some(adapter)) => {
                        if adapter.adapter_id != request.adapter_id
                            || adapter.adapter_revision != request.adapter_revision
                        {
                            return Err(evolution_target_failure(
                                "migration_adapter_mismatch",
                                "migration target does not match the command's exact adapter identity and revision",
                            ));
                        }
                        admit_local_process_target(binding, EnginePhase::ExecuteLiveEvolution)?;
                        admit_local_evolution_process_target(&adapter.process)?;
                        Ok(())
                    }
                    (None, Some(_)) if !target.target_execution_bindings.is_empty() => {
                        Err(evolution_target_failure(
                            "migration_target_binding_mismatch",
                            "migration target registry must contain only the command's exact target Plan",
                        ))
                    }
                    (None, Some(_)) | (Some(_), None) => Err(evolution_target_failure(
                        "incomplete_migration_provider_target",
                        "migration provider configuration must contain both the exact target execution binding and adapter",
                    )),
                    (None, None) => Err(evolution_target_failure(
                        "migration_target_binding_mismatch",
                        "migration target registry must contain only the command's exact target Plan",
                    )),
                }
            }
            EvolutionCommand::Shadow { request, .. } => {
                if target.migration_adapter.is_some()
                    || !target.target_execution_bindings.is_empty()
                {
                    return Err(evolution_target_failure(
                        "unexpected_shadow_provider_capability",
                        "shadow execution does not accept migration adapter or target binding capabilities",
                    ));
                }
                let Some(driver) = &target.shadow_driver else {
                    return Ok(());
                };
                if driver.driver_id != request.driver_id
                    || driver.driver_revision != request.driver_revision
                {
                    return Err(evolution_target_failure(
                        "shadow_driver_mismatch",
                        "shadow target does not match the command's exact driver identity and revision",
                    ));
                }
                admit_local_evolution_process_target(&driver.process)?;
                Ok(())
            }
            EvolutionCommand::ApplyPatch { .. }
            | EvolutionCommand::SetRollout { .. }
            | EvolutionCommand::SelectOccurrence { .. }
            | EvolutionCommand::RestartUnderNewPlan { .. }
            | EvolutionCommand::Observe { .. }
            | EvolutionCommand::ApplyGate { .. } => require_no_evolution_provider(target),
        },
        LiveEvolutionCommand::PublishDefinition { .. }
        | LiveEvolutionCommand::RegisterTemplate { .. }
        | LiveEvolutionCommand::PublishAndRelink { .. } => require_no_evolution_provider(target),
    }
}

fn require_no_evolution_provider(
    target: &cymule_runtime::EngineEvolutionTarget,
) -> Result<(), EngineFailure> {
    if !target.target_execution_bindings.is_empty()
        || target.migration_adapter.is_some()
        || target.shadow_driver.is_some()
    {
        return Err(evolution_target_failure(
            "unexpected_evolution_provider",
            "this live-evolution command does not invoke an external provider",
        ));
    }
    Ok(())
}

fn admit_local_evolution_process_target(target: &EnginePluginTarget) -> Result<(), EngineFailure> {
    if target.provider != ENGINE_PROCESS_EXECUTOR_PROVIDER {
        return Err(evolution_target_failure(
            "unsupported_evolution_provider",
            format!("unsupported evolution provider {}", target.provider),
        ));
    }
    Ok(())
}

fn evolution_target_failure(code: &'static str, message: impl Into<String>) -> EngineFailure {
    request_validation_failure(EnginePhase::ExecuteLiveEvolution, code, message)
}

struct ProcessEvolutionProvider {
    process: ProcessExecutor,
}

impl ProcessEvolutionProvider {
    fn open(
        target: &EnginePluginTarget,
        cancellation: Option<ProcessCancellation>,
    ) -> EvolutionResult<Self> {
        let config =
            process_executor_config(target, cancellation).map_err(evolution_process_error)?;
        let process = ProcessExecutor::new(config).map_err(evolution_process_error)?;
        verify_process_target_revision(target, &process)
            .map_err(|message| EvolutionError::Validation(message.to_owned()))?;
        Ok(Self { process })
    }

    fn invoke(&self, request: EvolutionPluginRequest) -> EvolutionResult<EvolutionPluginResponse> {
        let request =
            EvolutionPluginRequestEnvelope::new(self.process.implementation_revision(), request)
                .into_verified()?;
        let request =
            serde_json::to_vec(&request).map_err(|error| EvolutionError::PluginDefect {
                code: "invalid_evolution_process_encoding".to_owned(),
                message: error.to_string(),
            })?;
        if request.len() > MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES {
            return Err(EvolutionError::Validation(format!(
                "evolution plugin request uses {} raw bytes, above the {MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES} byte bound",
                request.len()
            )));
        }
        let response = self
            .process
            .invoke_evolution_bytes(&request)
            .map_err(evolution_process_error)?;
        decode_evolution_plugin_response(&response)?.into_result()
    }
}

struct ProcessMigrationAdapter {
    adapter_id: String,
    adapter_revision: String,
    provider: ProcessEvolutionProvider,
}

impl MigrationAdapter for ProcessMigrationAdapter {
    fn describe(&mut self) -> EvolutionResult<MigrationAdapterDescriptor> {
        match self
            .provider
            .invoke(EvolutionPluginRequest::DescribeMigration {})?
        {
            EvolutionPluginResponse::MigrationDescriptor { descriptor } => Ok(descriptor),
            EvolutionPluginResponse::Migrated { .. }
            | EvolutionPluginResponse::ShadowDescriptor { .. }
            | EvolutionPluginResponse::ShadowExecuted { .. } => Err(unexpected_evolution_response(
                "migration provider returned the wrong descriptor response",
            )),
        }
    }

    fn migrate(&mut self, request: &MigrationAdapterRequest) -> EvolutionResult<MigrationOutput> {
        let request = EvolutionPluginMigrationRequest::from_adapter_request(request);
        match self.provider.invoke(EvolutionPluginRequest::Migrate {
            request: Box::new(request),
        })? {
            EvolutionPluginResponse::Migrated { output } => Ok(*output),
            EvolutionPluginResponse::MigrationDescriptor { .. }
            | EvolutionPluginResponse::ShadowDescriptor { .. }
            | EvolutionPluginResponse::ShadowExecuted { .. } => Err(unexpected_evolution_response(
                "migration provider returned the wrong execution response",
            )),
        }
    }
}

struct ProcessShadowDriver {
    driver_id: String,
    driver_revision: String,
    provider: ProcessEvolutionProvider,
}

impl ShadowDriver for ProcessShadowDriver {
    fn describe(&mut self) -> EvolutionResult<ShadowDriverDescriptor> {
        match self
            .provider
            .invoke(EvolutionPluginRequest::DescribeShadow {})?
        {
            EvolutionPluginResponse::ShadowDescriptor { descriptor } => Ok(descriptor),
            EvolutionPluginResponse::MigrationDescriptor { .. }
            | EvolutionPluginResponse::Migrated { .. }
            | EvolutionPluginResponse::ShadowExecuted { .. } => Err(unexpected_evolution_response(
                "shadow provider returned the wrong descriptor response",
            )),
        }
    }

    fn execute(&mut self, request: &ShadowRequest) -> EvolutionResult<ShadowOutput> {
        match self
            .provider
            .invoke(EvolutionPluginRequest::ExecuteShadow {
                request: Box::new(request.clone()),
            })? {
            EvolutionPluginResponse::ShadowExecuted { output } => Ok(output),
            EvolutionPluginResponse::MigrationDescriptor { .. }
            | EvolutionPluginResponse::Migrated { .. }
            | EvolutionPluginResponse::ShadowDescriptor { .. } => {
                Err(unexpected_evolution_response(
                    "shadow provider returned the wrong execution response",
                ))
            }
        }
    }
}

struct CliEvolutionProviders {
    target_execution_bindings: BTreeMap<String, EnginePluginTarget>,
    captured_execution_bindings: BTreeMap<String, ExecutionBinding>,
    migration_target: Option<cymule_runtime::EngineMigrationProviderTarget>,
    migration_adapter: Option<ProcessMigrationAdapter>,
    shadow_target: Option<cymule_runtime::EngineShadowProviderTarget>,
    shadow_driver: Option<ProcessShadowDriver>,
    cancellation: Option<ProcessCancellation>,
}

impl CliEvolutionProviders {
    fn new(
        target: &cymule_runtime::EngineEvolutionTarget,
        cancellation: Option<ProcessCancellation>,
    ) -> Self {
        Self {
            target_execution_bindings: target.target_execution_bindings.clone(),
            captured_execution_bindings: BTreeMap::new(),
            migration_target: target.migration_adapter.clone(),
            migration_adapter: None,
            shadow_target: target.shadow_driver.clone(),
            shadow_driver: None,
            cancellation,
        }
    }
}

impl EvolutionProviders for CliEvolutionProviders {
    fn target_execution_binding(&mut self, plan_id: &str) -> EvolutionResult<ExecutionBinding> {
        if let Some(binding) = self.captured_execution_bindings.get(plan_id) {
            return Ok(binding.clone());
        }
        let target = self
            .target_execution_bindings
            .get(plan_id)
            .cloned()
            .ok_or_else(|| {
                EvolutionError::NotFound(format!(
                    "target execution binding for Plan {plan_id} is not registered"
                ))
            })?;
        let config = process_executor_config(&target, self.cancellation.clone())
            .map_err(evolution_process_error)?;
        let mut plugin = ProcessExecutor::new(config).map_err(evolution_process_error)?;
        verify_process_target_revision(&target, &plugin)
            .map_err(|message| EvolutionError::Validation(message.to_owned()))?;
        let implementation_revision = plugin.implementation_revision().to_owned();
        let manifest = plugin.describe().map_err(evolution_process_error)?;
        let binding = ExecutionBinding::for_local_process(&manifest, implementation_revision)
            .map_err(|error| EvolutionError::PluginDefect {
                code: error.code().to_owned(),
                message: error.message(),
            })?;
        self.captured_execution_bindings
            .insert(plan_id.to_owned(), binding.clone());
        Ok(binding)
    }

    fn migration_adapter(
        &mut self,
        adapter_id: &str,
        adapter_revision: &str,
    ) -> EvolutionResult<&mut dyn MigrationAdapter> {
        if self.migration_adapter.is_none() {
            let target = self
                .migration_target
                .as_ref()
                .filter(|target| {
                    target.adapter_id == adapter_id && target.adapter_revision == adapter_revision
                })
                .cloned()
                .ok_or_else(|| {
                    EvolutionError::NotFound(format!(
                        "migration adapter {adapter_id}@{adapter_revision} is not registered"
                    ))
                })?;
            let provider =
                ProcessEvolutionProvider::open(&target.process, self.cancellation.clone())?;
            self.migration_adapter = Some(ProcessMigrationAdapter {
                adapter_id: target.adapter_id,
                adapter_revision: target.adapter_revision,
                provider,
            });
        }
        match &mut self.migration_adapter {
            Some(adapter)
                if adapter.adapter_id == adapter_id
                    && adapter.adapter_revision == adapter_revision =>
            {
                Ok(adapter)
            }
            Some(_) | None => Err(EvolutionError::NotFound(format!(
                "migration adapter {adapter_id}@{adapter_revision} is not registered"
            ))),
        }
    }

    fn shadow_driver(
        &mut self,
        driver_id: &str,
        driver_revision: &str,
    ) -> EvolutionResult<&mut dyn ShadowDriver> {
        if self.shadow_driver.is_none() {
            let target = self
                .shadow_target
                .as_ref()
                .filter(|target| {
                    target.driver_id == driver_id && target.driver_revision == driver_revision
                })
                .cloned()
                .ok_or_else(|| {
                    EvolutionError::NotFound(format!(
                        "shadow driver {driver_id}@{driver_revision} is not registered"
                    ))
                })?;
            let provider =
                ProcessEvolutionProvider::open(&target.process, self.cancellation.clone())?;
            self.shadow_driver = Some(ProcessShadowDriver {
                driver_id: target.driver_id,
                driver_revision: target.driver_revision,
                provider,
            });
        }
        match &mut self.shadow_driver {
            Some(driver)
                if driver.driver_id == driver_id && driver.driver_revision == driver_revision =>
            {
                Ok(driver)
            }
            Some(_) | None => Err(EvolutionError::NotFound(format!(
                "shadow driver {driver_id}@{driver_revision} is not registered"
            ))),
        }
    }
}

fn unexpected_evolution_response(message: &'static str) -> EvolutionError {
    EvolutionError::PluginDefect {
        code: "unexpected_evolution_plugin_response".to_owned(),
        message: message.to_owned(),
    }
}

fn evolution_process_error(error: cymule_runtime::RuntimeError) -> EvolutionError {
    use cymule_runtime::RuntimeError;

    match error {
        RuntimeError::Core(error) => EvolutionError::from(error),
        RuntimeError::Contract(error) => EvolutionError::Contract(error),
        RuntimeError::Composition(error) => EvolutionError::PluginDefect {
            code: error.code().to_owned(),
            message: error.message(),
        },
        RuntimeError::ExpectedPluginFailure(error) => EvolutionError::PluginDefect {
            code: "unexpected_evolution_expected_failure".to_owned(),
            message: error.message,
        },
        RuntimeError::PluginDefect { code, message }
        | RuntimeError::UnknownWorld { code, message } => {
            EvolutionError::PluginDefect { code, message }
        }
        RuntimeError::Suspended(_) => EvolutionError::PluginDefect {
            code: "evolution_process_suspended".to_owned(),
            message: "evolution provider returned a one-shot suspension boundary".to_owned(),
        },
        RuntimeError::ReleaseRequired { .. } => EvolutionError::PluginDefect {
            code: "evolution_process_release_required".to_owned(),
            message: "evolution provider attempted to release an external effect".to_owned(),
        },
        RuntimeError::Substrate { code, message } => EvolutionError::Substrate { code, message },
        RuntimeError::Cancelled { code, message } => EvolutionError::Cancelled { code, message },
        RuntimeError::TimedOut { code, message } => EvolutionError::TimedOut { code, message },
        RuntimeError::Encoding(message) => EvolutionError::PluginDefect {
            code: "invalid_evolution_process_encoding".to_owned(),
            message,
        },
    }
}

struct AdmittedDurableRequest<'a> {
    store: AdmittedStoreTarget<'a>,
    capability: DurableCapability<'a>,
}

enum DurableCapability<'a> {
    StoreOnly,
    ProviderOnly {
        executor: &'a EnginePluginTarget,
    },
    Execution {
        executor: &'a EnginePluginTarget,
        clock: AdmittedClockTarget<'a>,
    },
}

struct AdmittedClockTarget<'a> {
    target: &'a EngineClockTarget,
    authority: SqliteAuthorityPath,
}

struct SqliteAuthorityPath {
    location: PathBuf,
    footprint: [PathBuf; 4],
}

struct PreparedDurableExecution {
    admission: ExecutionBindingAdmission<ProcessExecutor>,
    clock: SqliteClock,
}

fn admit_durable_request<'a>(
    target: &'a EngineDurableTarget,
    command: &DurableCommand,
) -> Result<AdmittedDurableRequest<'a>, EngineFailure> {
    let store = admit_store_target(&target.store, EnginePhase::ExecuteDurable)?;
    let executor = match (command.requires_executor(), target.executor.as_ref()) {
        (true, Some(executor)) => Some(executor),
        (true, None) => {
            return Err(durable_target_failure(
                "missing_execution_provider",
                "durable command requires an execution provider",
            ));
        }
        (false, Some(_)) => {
            return Err(durable_target_failure(
                "unexpected_execution_provider",
                "durable command does not accept an execution provider",
            ));
        }
        (false, None) => None,
    };
    let clock = match (command.requires_clock(), target.clock.as_ref()) {
        (true, Some(clock)) => Some(clock),
        (true, None) => {
            return Err(durable_target_failure(
                "missing_clock_provider",
                "durable execution requires a persistence-backed Clock provider",
            ));
        }
        (false, Some(_)) => {
            return Err(durable_target_failure(
                "unexpected_clock_provider",
                "durable command does not accept a Clock provider",
            ));
        }
        (false, None) => None,
    };
    let Some(executor) = executor else {
        return Ok(AdmittedDurableRequest {
            store,
            capability: DurableCapability::StoreOnly,
        });
    };
    if executor.provider != ENGINE_PROCESS_EXECUTOR_PROVIDER {
        return Err(durable_target_failure(
            "unsupported_execution_provider",
            format!("unsupported execution provider {}", executor.provider),
        ));
    }
    let Some(clock) = clock else {
        return Ok(AdmittedDurableRequest {
            store,
            capability: DurableCapability::ProviderOnly { executor },
        });
    };
    if clock.provider != ENGINE_CLOCK_SYSTEM_PROVIDER {
        return Err(durable_target_failure(
            "unsupported_clock_provider",
            format!("unsupported Clock provider {}", clock.provider),
        ));
    }
    let clock = AdmittedClockTarget {
        target: clock,
        authority: resolve_sqlite_authority_path(&clock.location, EnginePhase::ExecuteDurable)?,
    };
    reject_store_clock_alias(&store, &clock.authority)?;
    Ok(AdmittedDurableRequest {
        store,
        capability: DurableCapability::Execution { executor, clock },
    })
}

fn durable_target_failure(code: &'static str, message: impl Into<String>) -> EngineFailure {
    request_validation_failure(EnginePhase::ExecuteDurable, code, message)
}

fn request_validation_failure(
    phase: EnginePhase,
    code: &'static str,
    message: impl Into<String>,
) -> EngineFailure {
    let mut failure = EngineFailure::new(EngineFailureCategory::Validation, phase, code, message);
    failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
    failure
}

fn observe_clock(
    target: &EngineClockTarget,
    run_id: &str,
) -> Result<ClockObservationRef, EngineFailure> {
    target.verify()?;
    if target.provider != ENGINE_CLOCK_SYSTEM_PROVIDER {
        return Err(request_validation_failure(
            EnginePhase::ObserveClock,
            "unsupported_clock_provider",
            format!("unsupported Clock provider {}", target.provider),
        ));
    }
    let scope = execution_clock_scope(run_id).map_err(|error| {
        map_durable_error(
            &cymule_durable::DurableError::from(error),
            EnginePhase::ObserveClock,
        )
    })?;
    let authority = resolve_sqlite_authority_path(&target.location, EnginePhase::ObserveClock)?;
    let mut clock = SqliteClock::open(
        &authority.location,
        &target.source_id,
        &target.source_generation,
    )
    .map_err(|error| map_durable_error(&error, EnginePhase::ObserveClock))?;
    clock
        .observe(&scope)
        .map(|observation| observation.reference())
        .map_err(|error| map_durable_error(&error, EnginePhase::ObserveClock))
}

fn prepare_durable_admission(
    executor: &EnginePluginTarget,
    cancellation: Option<ProcessCancellation>,
) -> cymule_durable::DurableResult<ExecutionBindingAdmission<ProcessExecutor>> {
    let config = process_executor_config(executor, cancellation)
        .map_err(cymule_durable::DurableError::from)?;
    let plugin = ProcessExecutor::new(config).map_err(cymule_durable::DurableError::from)?;
    verify_process_target_revision(executor, &plugin)
        .map_err(|message| cymule_durable::DurableError::Validation(message.to_owned()))?;
    let implementation_revision = plugin.implementation_revision().to_owned();
    ExecutionBindingAdmission::for_local_process(plugin, implementation_revision)
        .map_err(cymule_durable::DurableError::from)
}

fn prepare_durable_execution(
    executor: &EnginePluginTarget,
    clock: &AdmittedClockTarget<'_>,
    cancellation: Option<ProcessCancellation>,
) -> cymule_durable::DurableResult<PreparedDurableExecution> {
    let admission = prepare_durable_admission(executor, cancellation)?;
    let clock = SqliteClock::open(
        &clock.authority.location,
        &clock.target.source_id,
        &clock.target.source_generation,
    )?;
    Ok(PreparedDurableExecution { admission, clock })
}

fn process_executor_config(
    target: &EnginePluginTarget,
    cancellation: Option<ProcessCancellation>,
) -> cymule_runtime::RuntimeResult<ProcessExecutorConfig> {
    let message_limit = usize::try_from(target.process.message_limit).map_err(|_| {
        cymule_runtime::RuntimeError::plugin_defect(
            "process message limit does not fit the host address space",
        )
    })?;
    let closure_limit = usize::try_from(target.process.closure_limit).map_err(|_| {
        cymule_runtime::RuntimeError::plugin_defect(
            "process closure limit does not fit the host address space",
        )
    })?;
    Ok(ProcessExecutorConfig {
        executable: PathBuf::from(&target.process.executable),
        arguments: target.process.arguments.clone(),
        working_directory: target.process.working_directory.as_ref().map(PathBuf::from),
        environment: target.process.environment.clone(),
        runtime_closure: target.process.runtime_closure.clone(),
        timeout: Duration::from_millis(target.process.timeout_ms),
        message_limit,
        closure_limit,
        cancellation,
    })
}

fn admit_local_process_target(
    target: &EnginePluginTarget,
    phase: EnginePhase,
) -> Result<(), EngineFailure> {
    target.verify()?;
    if target.provider != ENGINE_PROCESS_EXECUTOR_PROVIDER {
        return Err(request_validation_failure(
            phase,
            "unsupported_execution_provider",
            format!("unsupported execution provider {}", target.provider),
        ));
    }
    Ok(())
}

fn reject_store_clock_alias(
    store: &AdmittedStoreTarget<'_>,
    clock: &SqliteAuthorityPath,
) -> Result<(), EngineFailure> {
    let conflict = match store {
        AdmittedStoreTarget::Directory { location } => {
            let mut conflict = false;
            for path in &clock.footprint {
                if filesystem_path_is_same_or_descendant(
                    path,
                    location,
                    EnginePhase::ExecuteDurable,
                )? || filesystem_path_is_same_or_descendant(
                    location,
                    path,
                    EnginePhase::ExecuteDurable,
                )? {
                    conflict = true;
                    break;
                }
            }
            conflict
        }
        AdmittedStoreTarget::Sqlite { authority, .. } => {
            let mut conflict = false;
            for store_path in &authority.footprint {
                for clock_path in &clock.footprint {
                    if filesystem_paths_equivalent(
                        store_path,
                        clock_path,
                        EnginePhase::ExecuteDurable,
                    )? {
                        conflict = true;
                        break;
                    }
                }
                if conflict {
                    break;
                }
            }
            conflict
        }
    };
    if conflict {
        Err(store_clock_conflict())
    } else {
        Ok(())
    }
}

fn resolve_sqlite_authority_path(
    location: &str,
    phase: EnginePhase,
) -> Result<SqliteAuthorityPath, EngineFailure> {
    let location = resolve_authority_path(location, phase)?;
    let [base, wal, shm, journal] = sqlite_authority_footprint(&location);
    let footprint = [
        base,
        stable_path_identity(&wal, phase)?,
        stable_path_identity(&shm, phase)?,
        stable_path_identity(&journal, phase)?,
    ];
    Ok(SqliteAuthorityPath {
        location,
        footprint,
    })
}

fn sqlite_authority_footprint(location: &Path) -> [PathBuf; 4] {
    let suffixed = |suffix: &str| {
        let mut value = location.as_os_str().to_os_string();
        value.push(suffix);
        PathBuf::from(value)
    };
    [
        location.to_path_buf(),
        suffixed("-wal"),
        suffixed("-shm"),
        suffixed("-journal"),
    ]
}

fn store_clock_conflict() -> EngineFailure {
    request_validation_failure(
        EnginePhase::ExecuteDurable,
        "store_clock_location_conflict",
        "durable Store and Clock authorities require distinct stable filesystem locations",
    )
}

fn resolve_authority_path(location: &str, phase: EnginePhase) -> Result<PathBuf, EngineFailure> {
    if location.contains('\0') {
        return Err(request_validation_failure(
            phase,
            "invalid_authority_path",
            "Store and Clock filesystem locations cannot contain NUL",
        ));
    }
    let input = Path::new(location);
    let absolute = if input.is_absolute() {
        input.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| path_identity_failure(&error, phase))?
            .join(input)
    };
    stable_path_identity(&absolute, phase)
}

fn stable_path_identity(path: &Path, phase: EnginePhase) -> Result<PathBuf, EngineFailure> {
    stable_path_parts(path, phase).map(|parts| parts.location)
}

struct StablePathParts {
    location: PathBuf,
    existing_anchor: PathBuf,
    unresolved: Vec<OsString>,
}

fn stable_path_parts(path: &Path, phase: EnginePhase) -> Result<StablePathParts, EngineFailure> {
    let mut existing = path;
    let mut suffix = Vec::new();
    loop {
        match fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = existing.file_name().ok_or_else(|| {
                    path_identity_failure(
                        &io::Error::new(
                            io::ErrorKind::NotFound,
                            "filesystem path has no resolvable ancestor",
                        ),
                        phase,
                    )
                })?;
                suffix.push(name.to_os_string());
                existing = existing.parent().ok_or_else(|| {
                    path_identity_failure(
                        &io::Error::new(
                            io::ErrorKind::NotFound,
                            "filesystem path has no resolvable ancestor",
                        ),
                        phase,
                    )
                })?;
            }
            Err(error) => return Err(path_identity_failure(&error, phase)),
        }
    }
    let existing_anchor =
        fs::canonicalize(existing).map_err(|error| path_identity_failure(&error, phase))?;
    let mut identity = existing_anchor.clone();
    for component in suffix.into_iter().rev() {
        identity.push(component);
    }
    let unresolved = identity
        .strip_prefix(&existing_anchor)
        .expect("stable path is rooted at its canonical existing ancestor")
        .components()
        .map(|component| component.as_os_str().to_os_string())
        .collect();
    Ok(StablePathParts {
        location: identity,
        existing_anchor,
        unresolved,
    })
}

fn same_existing_file(
    left: &Path,
    right: &Path,
    phase: EnginePhase,
) -> Result<bool, EngineFailure> {
    let metadata = |path: &Path| match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(path_identity_failure(&error, phase)),
    };
    if metadata(left)?.is_none() || metadata(right)?.is_none() {
        return Ok(false);
    }
    same_file::is_same_file(left, right).map_err(|error| path_identity_failure(&error, phase))
}

fn filesystem_paths_equivalent(
    left: &Path,
    right: &Path,
    phase: EnginePhase,
) -> Result<bool, EngineFailure> {
    if left == right || same_existing_file(left, right, phase)? {
        return Ok(true);
    }
    let left = stable_path_parts(left, phase)?;
    let right = stable_path_parts(right, phase)?;
    if left.existing_anchor != right.existing_anchor
        && !same_existing_file(&left.existing_anchor, &right.existing_anchor, phase)?
    {
        return Ok(false);
    }
    component_sequences_equal(
        &left.unresolved,
        &right.unresolved,
        |left_component, right_component| {
            filesystem_components_equivalent(
                &left.existing_anchor,
                left_component,
                right_component,
                phase,
            )
        },
    )
}

fn filesystem_path_is_same_or_descendant(
    candidate: &Path,
    ancestor: &Path,
    phase: EnginePhase,
) -> Result<bool, EngineFailure> {
    if filesystem_paths_equivalent(candidate, ancestor, phase)? {
        return Ok(true);
    }
    let candidate = stable_path_parts(candidate, phase)?;
    let ancestor = stable_path_parts(ancestor, phase)?;
    if ancestor.unresolved.is_empty() && candidate.existing_anchor.starts_with(&ancestor.location) {
        return Ok(true);
    }
    if candidate.existing_anchor != ancestor.existing_anchor
        && !same_existing_file(&candidate.existing_anchor, &ancestor.existing_anchor, phase)?
    {
        return Ok(false);
    }
    component_sequence_is_prefix(
        &ancestor.unresolved,
        &candidate.unresolved,
        |left, right| {
            filesystem_components_equivalent(&candidate.existing_anchor, left, right, phase)
        },
    )
}

fn component_sequences_equal<E>(
    left: &[OsString],
    right: &[OsString],
    mut equivalent: impl FnMut(&OsStr, &OsStr) -> Result<bool, E>,
) -> Result<bool, E> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        if !equivalent(left, right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn component_sequence_is_prefix<E>(
    prefix: &[OsString],
    complete: &[OsString],
    mut equivalent: impl FnMut(&OsStr, &OsStr) -> Result<bool, E>,
) -> Result<bool, E> {
    if prefix.len() > complete.len() {
        return Ok(false);
    }
    for (left, right) in prefix.iter().zip(complete) {
        if !equivalent(left, right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn filesystem_components_equivalent(
    existing_parent: &Path,
    left: &OsStr,
    right: &OsStr,
    phase: EnginePhase,
) -> Result<bool, EngineFailure> {
    if left == right {
        return Ok(true);
    }
    let probe = tempfile::Builder::new()
        .prefix(".cymule-authority-probe-")
        .tempdir_in(existing_parent)
        .map_err(|error| path_identity_failure(&error, phase))?;
    let comparison = (|| {
        let first = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(probe.path().join(left))?;
        let equivalent = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(probe.path().join(right))
        {
            Ok(second) => {
                drop(second);
                false
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => true,
            Err(error) => return Err(error),
        };
        drop(first);
        Ok(equivalent)
    })();
    let cleanup = probe.close();
    let equivalent = comparison.map_err(|error| path_identity_failure(&error, phase))?;
    cleanup.map_err(|error| path_identity_failure(&error, phase))?;
    Ok(equivalent)
}

fn path_identity_failure(error: &io::Error, phase: EnginePhase) -> EngineFailure {
    let mut failure = EngineFailure::new(
        EngineFailureCategory::SubstrateFailure,
        phase,
        "authority_path_identity_failed",
        error.to_string(),
    );
    failure.retry_disposition = Some(EngineRetryDisposition::RetrySameRequest);
    failure
}

enum CliStore {
    Directory(DirectoryStore),
    Sqlite(SqliteStore),
}

enum AdmittedStoreTarget<'a> {
    Directory {
        location: PathBuf,
    },
    Sqlite {
        authority: SqliteAuthorityPath,
        domain: &'a str,
    },
}

impl DurableStore for CliStore {
    fn load_head(&mut self) -> cymule_durable::DurableResult<Option<StoreHead>> {
        match self {
            Self::Directory(store) => store.load_head(),
            Self::Sqlite(store) => store.load_head(),
        }
    }

    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> cymule_durable::DurableResult<Option<cymule_durable::StateRootManifest>> {
        match self {
            Self::Directory(store) => store.load_state_root_manifest(manifest_id),
            Self::Sqlite(store) => store.load_state_root_manifest(manifest_id),
        }
    }

    fn with_state_root_resolver<T>(
        &mut self,
        current: &cymule_durable::StateRootManifest,
        read: impl FnOnce(
            &mut dyn cymule_durable::StateRootResolver,
        ) -> cymule_durable::DurableResult<T>,
    ) -> cymule_durable::DurableResult<T> {
        match self {
            Self::Directory(store) => store.with_state_root_resolver(current, read),
            Self::Sqlite(store) => store.with_state_root_resolver(current, read),
        }
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> cymule_durable::DurableResult<cymule_durable::ApplicationJournalPrefix> {
        match self {
            Self::Directory(store) => store.application_journal_prefix(manifest, journal_id, count),
            Self::Sqlite(store) => store.application_journal_prefix(manifest, journal_id, count),
        }
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> cymule_durable::DurableResult<Option<cymule_durable::JournalRecordManifest>> {
        match self {
            Self::Directory(store) => {
                store.application_journal_record_manifest(manifest, journal_id, record_id)
            }
            Self::Sqlite(store) => {
                store.application_journal_record_manifest(manifest, journal_id, record_id)
            }
        }
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        replacement_id: &str,
    ) -> cymule_durable::DurableResult<
        Option<cymule_durable::ApplicationJournalPrefixReplacementAuthority>,
    > {
        match self {
            Self::Directory(store) => {
                store.application_journal_prefix_replacement_authority(manifest, replacement_id)
            }
            Self::Sqlite(store) => {
                store.application_journal_prefix_replacement_authority(manifest, replacement_id)
            }
        }
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        coupling_id: &str,
    ) -> cymule_durable::DurableResult<Option<cymule_durable::CoupledCheckpointReceipt>> {
        match self {
            Self::Directory(store) => store.coupled_checkpoint_receipt(manifest, coupling_id),
            Self::Sqlite(store) => store.coupled_checkpoint_receipt(manifest, coupling_id),
        }
    }

    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> cymule_durable::DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
        match self {
            Self::Directory(store) => store.load_machine_command_archive_segment(segment_id),
            Self::Sqlite(store) => store.load_machine_command_archive_segment(segment_id),
        }
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> cymule_durable::DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
        match self {
            Self::Directory(store) => store.load_machine_command_archive_entry(entry_id),
            Self::Sqlite(store) => store.load_machine_command_archive_entry(entry_id),
        }
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> cymule_durable::DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
        match self {
            Self::Directory(store) => store.load_machine_command_archive_batch(batch_id),
            Self::Sqlite(store) => store.load_machine_command_archive_batch(batch_id),
        }
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> cymule_durable::DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
        match self {
            Self::Directory(store) => store.load_machine_command_index_node(node_id),
            Self::Sqlite(store) => store.load_machine_command_index_node(node_id),
        }
    }

    fn lookup_machine_command_archive(
        &mut self,
        anchor: &cymule_core::MachineBaseAnchor,
        command_id: &str,
    ) -> cymule_durable::DurableResult<cymule_core::MachineCommandArchiveLookup> {
        match self {
            Self::Directory(store) => store.lookup_machine_command_archive(anchor, command_id),
            Self::Sqlite(store) => store.lookup_machine_command_archive(anchor, command_id),
        }
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> cymule_durable::DurableResult<StoreCommit> {
        match self {
            Self::Directory(store) => store.compare_and_commit(expected, batch),
            Self::Sqlite(store) => store.compare_and_commit(expected, batch),
        }
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &cymule_durable::StoreReclamation,
    ) -> cymule_durable::DurableResult<GcReceipt> {
        match self {
            Self::Directory(store) => store.reconcile_cold_reclamation(request),
            Self::Sqlite(store) => store.reconcile_cold_reclamation(request),
        }
    }

    fn advance_cold_reclamation(
        &mut self,
        request: &cymule_durable::StoreReclamation,
    ) -> cymule_durable::DurableResult<GcReceipt> {
        match self {
            Self::Directory(store) => store.advance_cold_reclamation(request),
            Self::Sqlite(store) => store.advance_cold_reclamation(request),
        }
    }

    fn stats(&self) -> cymule_durable::DurableResult<StoreStats> {
        match self {
            Self::Directory(store) => store.stats(),
            Self::Sqlite(store) => store.stats(),
        }
    }
}

fn admit_store_target(
    target: &cymule_runtime::EngineStoreTarget,
    phase: EnginePhase,
) -> Result<AdmittedStoreTarget<'_>, EngineFailure> {
    match target.provider.as_str() {
        ENGINE_DIRECTORY_STORE_PROVIDER if target.domain.is_none() => {
            Ok(AdmittedStoreTarget::Directory {
                location: resolve_authority_path(&target.location, phase)?,
            })
        }
        ENGINE_SQLITE_STORE_PROVIDER => {
            let domain = target.domain.as_ref().ok_or_else(|| {
                request_validation_failure(
                    phase,
                    "missing_store_domain",
                    "SQLite store target requires a domain",
                )
            })?;
            Ok(AdmittedStoreTarget::Sqlite {
                authority: resolve_sqlite_authority_path(&target.location, phase)?,
                domain,
            })
        }
        ENGINE_DIRECTORY_STORE_PROVIDER => Err(request_validation_failure(
            phase,
            "unexpected_store_domain",
            "directory store target must not contain a domain",
        )),
        provider => Err(request_validation_failure(
            phase,
            "unsupported_store_provider",
            format!(
                "the CLI Engine does not provide store {provider}; select a custom Engine transport"
            ),
        )),
    }
}

fn open_store(target: &AdmittedStoreTarget<'_>) -> cymule_durable::DurableResult<CliStore> {
    match target {
        AdmittedStoreTarget::Directory { location } => {
            DirectoryStore::open(location).map(CliStore::Directory)
        }
        AdmittedStoreTarget::Sqlite { authority, domain } => {
            SqliteStore::open(&authority.location, *domain).map(CliStore::Sqlite)
        }
    }
}

fn open_store_read_only(
    target: &AdmittedStoreTarget<'_>,
) -> cymule_durable::DurableResult<CliStore> {
    match target {
        AdmittedStoreTarget::Directory { location } => {
            DirectoryStore::open_read_only(location).map(CliStore::Directory)
        }
        AdmittedStoreTarget::Sqlite { authority, domain } => {
            SqliteStore::open_read_only(&authority.location, *domain).map(CliStore::Sqlite)
        }
    }
}

fn open_existing_store_read_only(
    target: &AdmittedStoreTarget<'_>,
) -> cymule_durable::DurableResult<Option<CliStore>> {
    let location = match target {
        AdmittedStoreTarget::Directory { location } => location,
        AdmittedStoreTarget::Sqlite { authority, .. } => &authority.location,
    };
    match fs::metadata(location) {
        Ok(_) => open_store_read_only(target).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(cymule_durable::DurableError::Substrate {
            code: "store_location_metadata_failed".to_owned(),
            message: error.to_string(),
        }),
    }
}

fn local_process_runtime(
    target: &EnginePluginTarget,
    cancellation: Option<ProcessCancellation>,
) -> cymule_runtime::RuntimeResult<EmbeddedRuntime<ProcessExecutor>> {
    let config = process_executor_config(target, cancellation)?;
    let mut plugin = ProcessExecutor::new(config)?;
    verify_process_target_revision(target, &plugin).map_err(|message| {
        cymule_runtime::RuntimeError::Core(cymule_core::CoreError::Validation(message.to_owned()))
    })?;
    let implementation_revision = plugin.implementation_revision().to_owned();
    let manifest = plugin.describe()?;
    let binding = ExecutionBinding::for_local_process(&manifest, implementation_revision)?;
    EmbeddedRuntime::new(plugin, binding)
}

fn verify_process_target_revision(
    target: &EnginePluginTarget,
    executor: &ProcessExecutor,
) -> Result<(), &'static str> {
    if target
        .revision
        .as_deref()
        .is_some_and(|expected| expected != executor.implementation_revision())
    {
        return Err("plugin target revision does not match the sealed executable bytes");
    }
    Ok(())
}

fn install_cancellation() -> Result<ProcessCancellation, Box<dyn std::error::Error>> {
    let cancellation = ProcessCancellation::new()?;
    for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        cancellation.register_signal(signal)?;
    }
    Ok(cancellation)
}

fn map_resource_error(error: &cymule_resource::ResourceError) -> EngineFailure {
    use cymule_resource::ResourceError;

    if let ResourceError::Schema(issue) = error {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::ContractViolation,
            EnginePhase::SealResource,
            "resource_schema_violation",
            "typed Artifact value violates its declared schema",
        );
        failure.contract = Some(issue.contract_id.clone().into());
        failure.contract_side = Some(EngineContractSide::Input);
        failure.path = Some(issue.instance_path.clone().into());
        failure.issues = vec![EngineIssue {
            code: "schema_violation".into(),
            message: "value does not satisfy the Artifact type contract".into(),
            path: Some(issue.instance_path.clone().into()),
            schema_path: Some(issue.schema_path.clone().into()),
        }];
        failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        return failure;
    }

    let (category, code, retry) = match &error {
        ResourceError::Validation(_) => (
            EngineFailureCategory::Validation,
            "resource_validation_failed",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        ResourceError::Conflict { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::Conflict,
                EnginePhase::SealResource,
                code,
                message,
                EngineRetryDisposition::Never,
            );
        }
        ResourceError::NotFound(_) => (EngineFailureCategory::NotFound, "resource_not_found", None),
        ResourceError::CommitOutcomeUnknown { .. } => (
            EngineFailureCategory::UnknownWorldOutcome,
            "unknown_world_outcome",
            Some(EngineRetryDisposition::Reconcile),
        ),
        ResourceError::Substrate { code, message }
        | ResourceError::Persistence { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::SubstrateFailure,
                EnginePhase::SealResource,
                code,
                message,
                EngineRetryDisposition::RetrySameRequest,
            );
        }
        ResourceError::Integrity { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::ContractViolation,
                EnginePhase::SealResource,
                code,
                message,
                EngineRetryDisposition::Never,
            );
        }
        ResourceError::Schema(_) => unreachable!("schema errors return above"),
    };
    let mut failure =
        EngineFailure::new(category, EnginePhase::SealResource, code, error.to_string());
    failure.retry_disposition = retry;
    failure
}

fn structured_engine_failure(
    category: EngineFailureCategory,
    phase: EnginePhase,
    code: &str,
    message: &str,
    retry: EngineRetryDisposition,
) -> EngineFailure {
    let mut failure = EngineFailure::new(category, phase, code.to_owned(), message.to_owned());
    failure.retry_disposition = Some(retry);
    failure
}

fn map_structured_durable_error(
    error: &cymule_durable::DurableError,
    phase: EnginePhase,
) -> Option<EngineFailure> {
    use cymule_durable::DurableError;

    let (category, code, message, retry) = match error {
        DurableError::Contract(error) => {
            return Some(EngineFailure::from_contract_violation(error, phase));
        }
        DurableError::Substrate { code, message } | DurableError::Persistence { code, message } => {
            (
                EngineFailureCategory::SubstrateFailure,
                code,
                message,
                EngineRetryDisposition::RetrySameRequest,
            )
        }
        DurableError::RuntimeDefect { code, message } => (
            EngineFailureCategory::PluginDefect,
            code,
            message,
            EngineRetryDisposition::Never,
        ),
        DurableError::Integrity { code, message } => (
            EngineFailureCategory::ContractViolation,
            code,
            message,
            EngineRetryDisposition::Never,
        ),
        DurableError::HistoryConflict { code, message } => (
            EngineFailureCategory::Conflict,
            code,
            message,
            EngineRetryDisposition::Never,
        ),
        DurableError::Cancelled { code, message } => (
            EngineFailureCategory::Cancelled,
            code,
            message,
            EngineRetryDisposition::Never,
        ),
        DurableError::TimedOut { code, message } => (
            EngineFailureCategory::TimedOut,
            code,
            message,
            EngineRetryDisposition::RefreshAndRetry,
        ),
        _ => return None,
    };
    Some(structured_engine_failure(
        category, phase, code, message, retry,
    ))
}

fn map_durable_error(error: &cymule_durable::DurableError, phase: EnginePhase) -> EngineFailure {
    use cymule_durable::DurableError;

    if let Some(failure) = map_structured_durable_error(error, phase) {
        return failure;
    }

    let (category, code, retry) = match &error {
        DurableError::Validation(_) => (
            EngineFailureCategory::Validation,
            "durable_command_validation_failed",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        DurableError::Encoding(_) => (
            EngineFailureCategory::ContractViolation,
            "durable_integrity_failed",
            Some(EngineRetryDisposition::Never),
        ),
        DurableError::Conflict { .. } => (
            EngineFailureCategory::Conflict,
            "durable_revision_conflict",
            Some(EngineRetryDisposition::RefreshAndRetry),
        ),
        DurableError::Busy { .. } => (
            EngineFailureCategory::Conflict,
            "durable_execution_busy",
            Some(EngineRetryDisposition::RefreshAndRetry),
        ),
        DurableError::ReconciliationRequired { .. } => (
            EngineFailureCategory::UnknownWorldOutcome,
            "effect_reconciliation_required",
            Some(EngineRetryDisposition::Reconcile),
        ),
        DurableError::NotFound(_) => (
            EngineFailureCategory::NotFound,
            "durable_object_not_found",
            None,
        ),
        DurableError::IllegalTransition(_) => (
            EngineFailureCategory::AdmissionDenied,
            "durable_transition_denied",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        DurableError::PagedScopeRequired { .. } => (
            EngineFailureCategory::AdmissionDenied,
            "paged_scope_required",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        DurableError::CommitOutcomeUnknown { .. } => (
            EngineFailureCategory::UnknownWorldOutcome,
            "durable_commit_outcome_unknown",
            Some(EngineRetryDisposition::Reconcile),
        ),
        // This requires archive-aware correction, never retry-same-request.
        DurableError::ArchivedCommandReplayRequired { .. } => (
            EngineFailureCategory::AdmissionDenied,
            "archived_command_replay_required",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        DurableError::Contract(_)
        | DurableError::Substrate { .. }
        | DurableError::Persistence { .. }
        | DurableError::RuntimeDefect { .. }
        | DurableError::Integrity { .. }
        | DurableError::HistoryConflict { .. }
        | DurableError::Cancelled { .. }
        | DurableError::TimedOut { .. } => unreachable!("structured errors return above"),
    };
    let mut failure = EngineFailure::new(category, phase, code, error.to_string());
    failure.retry_disposition = retry;
    failure
}

fn map_durable_pre_execution_error(error: &cymule_durable::DurableError) -> EngineFailure {
    if let cymule_durable::DurableError::TimedOut { code, message } = error {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::TimedOut,
            EnginePhase::ExecuteDurable,
            code,
            message,
        );
        failure.retry_disposition = Some(EngineRetryDisposition::RetrySameRequest);
        return failure;
    }
    map_durable_error(error, EnginePhase::ExecuteDurable)
}

fn map_evolution_error(
    error: &cymule_evolution::EvolutionError,
    phase: EnginePhase,
) -> EngineFailure {
    use cymule_evolution::EvolutionError;

    let (category, code, retry) = match &error {
        EvolutionError::Contract(error) => {
            return EngineFailure::from_contract_violation(error, phase);
        }
        EvolutionError::PagedScopeRequired { .. } => (
            EngineFailureCategory::AdmissionDenied,
            "paged_scope_required",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        EvolutionError::CollectionProviderFailure(failure) => {
            return EngineFailure::from_core(
                &cymule_core::CoreError::CollectionProviderFailure(failure.clone()),
                phase,
            );
        }
        EvolutionError::Validation(_) => (
            EngineFailureCategory::Validation,
            "evolution_command_validation_failed",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        EvolutionError::NotFound(_) => (
            EngineFailureCategory::NotFound,
            "evolution_object_not_found",
            None,
        ),
        EvolutionError::ReadRequired { .. } => (
            EngineFailureCategory::ContractViolation,
            "evolution_read_set_incomplete",
            Some(EngineRetryDisposition::Never),
        ),
        EvolutionError::Conflict(_) => (
            EngineFailureCategory::Conflict,
            "evolution_conflict",
            Some(EngineRetryDisposition::Never),
        ),
        EvolutionError::Cancelled { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::Cancelled,
                phase,
                code,
                message,
                EngineRetryDisposition::Never,
            );
        }
        EvolutionError::TimedOut { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::TimedOut,
                phase,
                code,
                message,
                EngineRetryDisposition::RetrySameRequest,
            );
        }
        EvolutionError::Integrity { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::ContractViolation,
                phase,
                code,
                message,
                EngineRetryDisposition::Never,
            );
        }
        EvolutionError::PluginDefect { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::PluginDefect,
                phase,
                code,
                message,
                EngineRetryDisposition::Never,
            );
        }
        EvolutionError::Substrate { code, message } => {
            return structured_engine_failure(
                EngineFailureCategory::SubstrateFailure,
                phase,
                code,
                message,
                EngineRetryDisposition::RetrySameRequest,
            );
        }
    };
    let mut failure = EngineFailure::new(category, phase, code, error.to_string());
    failure.retry_disposition = retry;
    failure
}

fn argument_value<'a>(arguments: &'a [String], flag: &str) -> Result<&'a str, String> {
    arguments
        .windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
        .ok_or_else(|| format!("missing required option {flag}"))
}

fn read_path<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, Box<dyn std::error::Error>> {
    let bytes = if path == "-" {
        let mut bytes = Vec::new();
        io::stdin().read_to_end(&mut bytes)?;
        bytes
    } else {
        fs::read(Path::new(path))?
    };
    let value = decode_strict_json_value(&bytes)?;
    Ok(serde_json::from_value(value)?)
}

fn print_json<T: Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        DurableCapability, EngineRequest, EngineResponse, ProcessCancellation,
        ProcessEvolutionProvider, ProcessExecutor, admit_durable_request,
        admit_evolution_provider_selection, admit_local_process_target, admit_store_target,
        component_sequence_is_prefix, component_sequences_equal, decode_and_execute_request,
        execute_durable, execute_live_evolution, filesystem_components_equivalent,
        map_durable_error, map_evolution_error, map_resource_error, observe_clock, open_store,
        process_executor_config, reject_store_clock_alias,
    };
    use cymule_core::{Definition, Expression, Region};
    use cymule_directory_store::DirectoryStore;
    use cymule_durable::{
        DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableError, DurableResponse,
        DurableRunItem, DurableRunItemSelector, DurableStoreControl,
        MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES, MAX_DURABLE_QUERY_PAGE_BYTES, OutboxState,
    };
    use cymule_durable_protocol::{
        CLOCK_OBSERVATION_VERSION, ClockObservationRef, ExecutionClaimRequest,
    };
    use cymule_evolution::{
        EVOLUTION_CONTROL_VERSION, EvolutionCommand, EvolutionError, EvolutionPersistenceCommand,
        EvolutionPluginFailure, LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand,
        MigrationRequest, NoEvolutionProviders, RolloutDecision, RolloutMode, ShadowRequest,
    };
    use cymule_resource::ResourceError;
    use cymule_runtime::{
        EngineDurableTarget, EngineEvolutionTarget, EngineFailure, EngineFailureCategory,
        EngineMigrationProviderTarget, EnginePhase, EnginePluginTarget, EngineProcessConfig,
        EngineRequestEnvelope, EngineResponseEnvelope, EngineRetryDisposition, EngineStoreTarget,
    };
    use serde_json::Value;
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn test_path(name: &str) -> PathBuf {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "cymule-cli-{name}-{}-{sequence}",
            std::process::id()
        ))
    }

    fn process_target(location: impl Into<String>) -> EnginePluginTarget {
        EnginePluginTarget::process(process_config(location))
    }

    fn pinned_process_target(
        location: impl Into<String>,
        revision: impl Into<String>,
    ) -> EnginePluginTarget {
        EnginePluginTarget::pinned_process(process_config(location), revision)
    }

    fn process_config(location: impl Into<String>) -> EngineProcessConfig {
        let location = PathBuf::from(location.into());
        let executable = if location.is_absolute() {
            location
        } else {
            std::env::current_dir()
                .expect("test process current directory resolves")
                .join(location)
        };
        let effect_ledger = executable.with_extension("effect-ledger.sqlite3");
        EngineProcessConfig {
            executable: executable.to_string_lossy().into_owned(),
            arguments: Vec::new(),
            environment: std::collections::BTreeMap::from([(
                "CYMULE_TEST_EFFECT_LEDGER_PATH".to_owned(),
                effect_ledger.to_string_lossy().into_owned(),
            )]),
            working_directory: None,
            runtime_closure: std::collections::BTreeMap::from([(
                "test-adapter-runtime".to_owned(),
                "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                    .to_owned(),
            )]),
            timeout_ms: 60_000,
            message_limit: 8 * 1024 * 1024,
            closure_limit: 64 * 1024 * 1024,
        }
    }

    fn definition_publication(command_id: &str) -> LiveEvolutionCommand {
        LiveEvolutionCommand::PublishDefinition {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: command_id.to_owned(),
            logical_ref: "definition.cli-replay".to_owned(),
            definition: Definition {
                id: "published".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Literal {
                        value: serde_json::json!({"status": "published"}),
                    },
                },
            },
            references: Vec::new(),
        }
    }

    fn provider_free_migration_command(command_id: &str) -> LiveEvolutionCommand {
        let exact = |domain: &str| cymule_core::content_id(domain, &()).expect("identity derives");
        LiveEvolutionCommand::Apply {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: command_id.to_owned(),
            template_id: "template:provider-free-preflight".to_owned(),
            command: Box::new(EvolutionCommand::Migrate {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: format!("{command_id}:migration"),
                request: Box::new(MigrationRequest {
                    migration_id: format!("{command_id}:request"),
                    run_id: "run:provider-free-preflight".to_owned(),
                    from_plan: exact("cymule.cli-test-provider-free-from-plan/1"),
                    to_plan: exact("cymule.cli-test-provider-free-to-plan/1"),
                    plan_edge_id: exact("cymule.cli-test-provider-free-edge/1"),
                    compatibility_id: exact("cymule.cli-test-provider-free-compatibility/1"),
                    expected_source_epoch: 1,
                    adapter_id: "adapter:provider-free-preflight".to_owned(),
                    adapter_revision: exact("cymule.cli-test-provider-free-adapter/1"),
                }),
            }),
        }
    }

    fn provider_free_shadow_command(command_id: &str) -> LiveEvolutionCommand {
        let exact = |domain: &str| cymule_core::content_id(domain, &()).expect("identity derives");
        LiveEvolutionCommand::Apply {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: command_id.to_owned(),
            template_id: "template:provider-free-shadow".to_owned(),
            command: Box::new(EvolutionCommand::Shadow {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: format!("{command_id}:shadow"),
                request: ShadowRequest {
                    comparison_id: format!("{command_id}:comparison"),
                    decision_id: "decision:provider-free-shadow".to_owned(),
                    subject: "run:provider-free-shadow".to_owned(),
                    primary_plan: exact("cymule.cli-test-provider-free-primary-plan/1"),
                    shadow_plan: exact("cymule.cli-test-provider-free-shadow-plan/1"),
                    driver_id: "driver:provider-free-shadow".to_owned(),
                    driver_revision: exact("cymule.cli-test-provider-free-shadow-driver/1"),
                    input: cymule_core::artifact_ref(
                        cymule_core::RUN_INPUT_ARTIFACT_KIND,
                        b"provider-free shadow input",
                    )
                    .expect("shadow input identity derives"),
                    comparison_policy: "policy:provider-free-shadow".to_owned(),
                },
            }),
        }
    }

    #[cfg(unix)]
    fn process_evolution_provider(
        name: &str,
        script: &str,
    ) -> (tempfile::TempDir, ProcessEvolutionProvider) {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("provider directory creates");
        let executable = directory.path().join(format!("{name}.sh"));
        fs::write(&executable, script).expect("provider script writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("provider script is executable");
        let mut process = process_config(executable.to_string_lossy().into_owned());
        process.message_limit = cymule_runtime::EVOLUTION_PLUGIN_MESSAGE_LIMIT as u64;
        let unpinned = EnginePluginTarget::process(process.clone());
        let captured = ProcessExecutor::new(
            process_executor_config(&unpinned, None).expect("provider config admits"),
        )
        .expect("provider closure captures");
        let target = EnginePluginTarget::pinned_process(
            process,
            captured.implementation_revision().to_owned(),
        );
        let provider =
            ProcessEvolutionProvider::open(&target, None).expect("exact process provider opens");
        (directory, provider)
    }

    fn execution_command() -> DurableCommand {
        DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:authority-admission".to_owned(),
            execution: ExecutionClaimRequest {
                owner: "driver:authority-admission".to_owned(),
                clock: ClockObservationRef {
                    clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
                    observation_id: format!("sha256:{}", "1".repeat(64)),
                    source_id: "clock:authority-admission".to_owned(),
                    source_generation: format!("sha256:{}", "2".repeat(64)),
                    scope: "scope:authority-admission".to_owned(),
                },
                ttl: 1,
            },
        }
    }

    fn resolution_command() -> DurableCommand {
        DurableCommand::ResolveEffect {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            resolution_id: "resolution:authority-admission".to_owned(),
            run_id: "run:authority-admission".to_owned(),
            intent_id: "effect:authority-admission".to_owned(),
            execution_binding: cymule_core::artifact_ref(
                cymule_runtime::EXECUTION_BINDING_VERSION,
                b"authority admission binding",
            )
            .expect("execution binding reference derives"),
            occurrence_binding: "binding:authority-admission".to_owned(),
            claim_owner: "dispatcher:authority-admission".to_owned(),
            claim_epoch: 1,
            resolution: cymule_core::ReconciliationResolution::ResolvedNotApplied,
            value: None,
        }
    }

    fn run_index_query() -> DurableCommand {
        DurableCommand::RunIndexPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            expected_revision: None,
            cursor: None,
            limit: 1,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        }
    }

    fn run_current_query(run_id: impl Into<String>) -> DurableCommand {
        DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.into(),
            expected_revision: None,
        }
    }

    fn effect_query(run_id: impl Into<String>, intent_id: impl Into<String>) -> DurableCommand {
        DurableCommand::RunItem {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.into(),
            expected_revision: None,
            selector: DurableRunItemSelector::Effect {
                intent_id: intent_id.into(),
            },
            max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
        }
    }

    fn embedded_plan(input_schema: Value) -> cymule_core::SealedPlan {
        cymule_core::seal_plan(cymule_core::PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "cli-preflight".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![cymule_core::Definition {
                id: "main".to_owned(),
                input_schema,
                output_schema: serde_json::json!({}),
                body: cymule_core::Region {
                    steps: Vec::new(),
                    result: cymule_core::Expression::Literal { value: Value::Null },
                },
            }],
            metadata: std::collections::BTreeMap::new(),
        })
        .expect("test Plan seals")
    }

    fn ambiguous_effect_candidate() -> cymule_core::PlanCandidate {
        cymule_core::PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "cli_store_only_effect_resolution".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: vec![cymule_core::EffectContract {
                id: "test.effect".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                profile: cymule_core::EffectProfile {
                    mutation: cymule_core::MutationKind::Mutating,
                    dispatch: cymule_core::DispatchPolicy::OnScopeCommit,
                    reconciliation: cymule_core::ReconciliationMode::Queryable,
                    keyed_idempotency: true,
                    irreversible: false,
                },
                requirements: std::collections::BTreeMap::new(),
            }],
            definitions: vec![cymule_core::Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: cymule_core::Region {
                    steps: vec![cymule_core::Step {
                        id: "effect.ambiguous".to_owned(),
                        operation: cymule_core::Operation::Effect {
                            effect: "test.effect".to_owned(),
                            input: cymule_core::Expression::Input,
                            occurrence: "primary".to_owned(),
                            bind: None,
                        },
                    }],
                    result: cymule_core::Expression::Literal { value: Value::Null },
                },
            }],
            metadata: std::collections::BTreeMap::new(),
        }
    }

    fn assert_sqlite_files_absent(path: &std::path::Path) {
        assert!(!path.exists());
        assert!(!PathBuf::from(format!("{}-wal", path.display())).exists());
        assert!(!PathBuf::from(format!("{}-shm", path.display())).exists());
        assert!(!PathBuf::from(format!("{}-journal", path.display())).exists());
    }

    #[test]
    fn rpc_rejects_nested_duplicate_plan_members_before_typed_decode() {
        let error = decode_and_execute_request(
            br#"{"engine_protocol":"cymule.engine/4","request":{"type":"seal","candidate":{"ir_version":"cymule.ir/3","ir_version":"changed"}}}"#,
        )
        .expect_err("duplicate Plan member is rejected");
        assert_eq!(error.code.as_ref(), "invalid_engine_request");
        assert!(error.message.contains("duplicate JSON object"));
    }

    #[test]
    fn rpc_rejects_integers_outside_the_shared_sdk_domain() {
        let error = decode_and_execute_request(
            br#"{"engine_protocol":"cymule.engine/4","request":{"type":"verify_durable_command","command":{"type":"activate_wait","control_version":"cymule.durable-control/3","activation_id":"activation:test","source":{"source":"signal","key":"signal:test"},"wait_ids":["wait:test"],"value":9007199254740992}}}"#,
        )
        .expect_err("unsafe integer is rejected before typed decode");
        assert_eq!(error.code.as_ref(), "invalid_engine_request");
        assert!(error.message.contains("exact cross-language range"));
    }

    #[test]
    fn rpc_normalizes_mathematical_integer_tokens_before_typed_decode_and_echo() {
        for lexeme in ["1.0", "1e0"] {
            let input = format!(
                r#"{{"engine_protocol":"cymule.engine/4","request":{{"type":"verify_evolution_command","command":{{"control_version":"cymule.evolution-control/5","command_id":"command:mathematical-integer","operation":"apply_gate","gate":{{"gate_id":"gate:mathematical-integer","decision_id":"decision:mathematical-integer","min_target_observations":{lexeme},"max_target_failures":0,"min_equivalent_shadows":0,"max_inequivalent_shadows":0}},"next_decision_id":"decision:mathematical-integer-next"}}}}}}"#
            );
            let (echo, response) = decode_and_execute_request(input.as_bytes())
                .expect("safe mathematical integer is admitted");
            assert!(matches!(
                response,
                EngineResponse::VerifiedEvolutionCommand { .. }
            ));
            assert_eq!(
                echo["command"]["gate"]["min_target_observations"],
                serde_json::json!(1)
            );
            assert!(
                !serde_json::to_string(&echo).unwrap().contains(lexeme),
                "success echo must use the normalized integer representation"
            );
        }

        let fractional = br#"{"engine_protocol":"cymule.engine/4","request":{"type":"verify_evolution_command","command":{"control_version":"cymule.evolution-control/5","command_id":"command:fractional-integer","operation":"apply_gate","gate":{"gate_id":"gate:fractional-integer","decision_id":"decision:fractional-integer","min_target_observations":1.5,"max_target_failures":0,"min_equivalent_shadows":0,"max_inequivalent_shadows":0},"next_decision_id":"decision:fractional-integer-next"}}}"#;
        let error = decode_and_execute_request(fractional)
            .expect_err("fractional value is not a typed integer");
        assert_eq!(error.code.as_ref(), "invalid_engine_request");
    }

    #[test]
    fn rpc_v4_rejects_v3_without_transport_fallback() {
        let error = decode_and_execute_request(
            br#"{"engine_protocol":"cymule.engine/3","request":{"type":"observe_clock","target":{"provider":"cymule.clock-system/2","location":"unused.sqlite","source_id":"clock:test","source_generation":"sha256:1111111111111111111111111111111111111111111111111111111111111111"},"run_id":"run:test"}}"#,
        )
        .expect_err("Engine v3 is not a supported fallback generation");
        assert_eq!(error.code.as_ref(), "unsupported_engine_protocol");
        assert_eq!(error.contract.as_deref(), Some("cymule.engine/4"));
        assert_eq!(error.retry_disposition, Some(EngineRetryDisposition::Never));
    }

    #[test]
    fn rpc_rejects_superseded_ir_v2_without_a_compatibility_reader() {
        let mut candidate = embedded_plan(serde_json::json!({})).candidate;
        candidate.ir_version = "cymule.ir/2".to_owned();
        let request = EngineRequest::Seal { candidate };
        let input = serde_json::to_vec(&EngineRequestEnvelope::new(request))
            .expect("legacy IR request serializes");

        let failure = decode_and_execute_request(&input)
            .expect_err("superseded IR generation is rejected exactly");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.phase, EnginePhase::SealPlan);
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
    }

    #[test]
    fn rpc_rejects_missing_required_plan_members_instead_of_synthesizing_defaults() {
        let candidate = serde_json::to_value(embedded_plan(serde_json::json!({})).candidate)
            .expect("candidate serializes");
        for member in ["components", "effects", "metadata"] {
            let mut malformed = candidate.clone();
            malformed
                .as_object_mut()
                .expect("candidate is an object")
                .remove(member);
            let input = serde_json::to_vec(&serde_json::json!({
                "engine_protocol": cymule_runtime::ENGINE_PROTOCOL_VERSION,
                "request": {"type": "seal", "candidate": malformed},
            }))
            .expect("malformed request serializes");
            let failure = decode_and_execute_request(&input)
                .expect_err("schema-required Plan members cannot be defaulted");
            assert_eq!(failure.code.as_ref(), "invalid_engine_request");
            assert_eq!(failure.phase, EnginePhase::DecodeRequest);
        }
    }

    #[cfg(unix)]
    #[test]
    fn pinned_run_revision_is_checked_before_plugin_process_io() {
        use std::os::unix::fs::PermissionsExt;

        let directory = test_path("pinned-run-revision");
        fs::create_dir(&directory).expect("fixture directory creates");
        let marker = directory.join("invoked");
        let executable = directory.join("plugin.sh");
        fs::write(
            &executable,
            format!("#!/bin/sh\nprintf invoked > '{}'\n", marker.display()),
        )
        .expect("fixture process writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("fixture process is executable");
        let request = EngineRequest::Run {
            plan: embedded_plan(serde_json::json!({ "type": "string" })),
            input: Value::String("valid".to_owned()),
            plugin: pinned_process_target(
                executable.to_string_lossy().into_owned(),
                format!("sha256:{}", "0".repeat(64)),
            ),
            run_id: "run:pinned-revision".to_owned(),
        };
        let input =
            serde_json::to_vec(&EngineRequestEnvelope::new(request)).expect("request serializes");

        let failure = decode_and_execute_request(&input).expect_err("wrong revision fails closed");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(!marker.exists(), "revision admission precedes Describe I/O");
        fs::remove_dir_all(directory).expect("fixture directory removes");
    }

    #[test]
    fn successful_rpc_echoes_the_strictly_decoded_inner_request() {
        let command = LiveEvolutionCommand::Apply {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:verify-live-rollout".to_owned(),
            template_id: "template:test".to_owned(),
            command: Box::new(EvolutionCommand::SetRollout {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "command:verify-rollout".to_owned(),
                decision: RolloutDecision {
                    decision_id: "decision:verify".to_owned(),
                    fallback_plan: format!("sha256:{}", "1".repeat(64)),
                    target_plan: format!("sha256:{}", "2".repeat(64)),
                    mode: RolloutMode::Active,
                },
            }),
        };
        let request = EngineRequest::VerifyLiveEvolutionCommand { command };
        let input = serde_json::to_vec(&EngineRequestEnvelope::new(request.clone()))
            .expect("request serializes");

        let (echoed, response) =
            decode_and_execute_request(&input).expect("request verifies successfully");
        assert_eq!(
            echoed,
            serde_json::to_value(request).expect("request serializes")
        );
        assert!(matches!(
            response,
            EngineResponse::VerifiedLiveEvolutionCommand { .. }
        ));
    }

    #[test]
    fn live_evolution_provider_selection_is_exact_before_any_provider_or_store_io() {
        let exact = |domain: &str| cymule_core::content_id(domain, &()).expect("identity derives");
        let adapter_revision = exact("cymule.cli-test-adapter/1");
        let target_revision = exact("cymule.cli-test-target-binding/1");
        let from_plan = exact("cymule.cli-test-from-plan/1");
        let to_plan = exact("cymule.cli-test-to-plan/1");
        let command = LiveEvolutionCommand::Apply {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:engine-migrate".to_owned(),
            template_id: "template:test".to_owned(),
            command: Box::new(EvolutionCommand::Migrate {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "command:migrate".to_owned(),
                request: Box::new(MigrationRequest {
                    migration_id: "migration:test".to_owned(),
                    run_id: "run:test".to_owned(),
                    from_plan,
                    to_plan: to_plan.clone(),
                    plan_edge_id: exact("cymule.cli-test-plan-edge/1"),
                    compatibility_id: exact("cymule.cli-test-compatibility/1"),
                    expected_source_epoch: 1,
                    adapter_id: "adapter:test".to_owned(),
                    adapter_revision: adapter_revision.clone(),
                }),
            }),
        };
        let mut process = pinned_process_target("/must/not/be/opened", &adapter_revision);
        process.process.message_limit = cymule_runtime::EVOLUTION_PLUGIN_MESSAGE_LIMIT as u64;
        let exact_target = EngineEvolutionTarget {
            store: EngineStoreTarget::directory("/must/not/be/opened-store"),
            target_execution_bindings: std::collections::BTreeMap::from([(
                to_plan,
                pinned_process_target("/must/not/be-opened-target", target_revision),
            )]),
            migration_adapter: Some(EngineMigrationProviderTarget {
                adapter_id: "adapter:test".to_owned(),
                adapter_revision: adapter_revision.clone(),
                process,
            }),
            shadow_driver: None,
        };
        admit_evolution_provider_selection(&exact_target, &command)
            .expect("exact command and provider selection match without I/O");

        let provider_free = EngineEvolutionTarget::new(EngineStoreTarget::directory(
            "/must/not/be-opened-replay-store",
        ));
        admit_evolution_provider_selection(&provider_free, &command)
            .expect("an exact retained replay requires no provider target");

        let mut incomplete = exact_target.clone();
        incomplete.migration_adapter = None;
        assert_eq!(
            admit_evolution_provider_selection(&incomplete, &command)
                .expect_err("partial migration capability is rejected")
                .code
                .as_ref(),
            "incomplete_migration_provider_target"
        );

        let mut wrong_plan = exact_target.clone();
        let binding = wrong_plan
            .target_execution_bindings
            .pop_first()
            .expect("target binding exists")
            .1;
        wrong_plan
            .target_execution_bindings
            .insert(exact("cymule.cli-test-wrong-target-plan/1"), binding);
        assert_eq!(
            admit_evolution_provider_selection(&wrong_plan, &command)
                .expect_err("another target Plan is rejected")
                .code
                .as_ref(),
            "migration_target_binding_mismatch"
        );

        let mut mismatched = exact_target;
        mismatched
            .migration_adapter
            .as_mut()
            .expect("adapter exists")
            .adapter_revision = exact("cymule.cli-test-other-adapter/1");
        assert_eq!(
            admit_evolution_provider_selection(&mismatched, &command)
                .expect_err("adapter revision must match the command")
                .code
                .as_ref(),
            "migration_adapter_mismatch"
        );
    }

    #[test]
    fn exact_evolution_replay_uses_the_existing_store_read_only() {
        let store_path = test_path("evolution-read-only-replay");
        let evolution_id = "evolution:cli-read-only-replay";
        let command = definition_publication("command:cli-read-only-replay");
        let persistence = EvolutionPersistenceCommand::new(evolution_id, command.clone())
            .expect("Evolution persistence command seals");
        let store = DirectoryStore::open(&store_path).expect("fixture Store opens");
        let mut control =
            DurableStoreControl::initialize(store).expect("fixture Store initializes");
        let first = control
            .evolution(&mut NoEvolutionProviders)
            .commit(&persistence)
            .expect("fixture Evolution command commits");
        drop(control);

        fs::remove_file(store_path.join("head.lock")).expect("head lock residue removes");
        fs::remove_file(store_path.join("objects.lock")).expect("object lock residue removes");
        let target = EngineEvolutionTarget::new(EngineStoreTarget::directory(
            store_path.to_string_lossy().into_owned(),
        ));
        let replay = execute_live_evolution(&target, evolution_id, command, None);
        let head_lock_exists = store_path.join("head.lock").exists();
        let object_lock_exists = store_path.join("objects.lock").exists();
        fs::remove_dir_all(&store_path).expect("fixture Store removes");

        let replay = replay.expect("exact Evolution command replays");
        assert_eq!(replay.committed_revision, None);
        assert_eq!(replay.receipt, first.receipt);
        assert!(
            !head_lock_exists,
            "read-only replay must not recreate head.lock"
        );
        assert!(
            !object_lock_exists,
            "read-only replay must not recreate objects.lock"
        );
    }

    #[test]
    fn provider_free_fresh_migration_rejects_before_store_creation() {
        let parent = test_path("provider-free-fresh-migration");
        fs::create_dir(&parent).expect("fixture parent creates");
        let store_path = parent.join("store");
        let target = EngineEvolutionTarget::new(EngineStoreTarget::directory(
            store_path.to_string_lossy().into_owned(),
        ));
        let error = execute_live_evolution(
            &target,
            "evolution:provider-free-fresh",
            provider_free_migration_command("command:provider-free-fresh"),
            None,
        )
        .expect_err("fresh migration requires its exact providers");
        let store_exists = store_path.exists();
        fs::remove_dir_all(parent).expect("fixture parent removes");

        assert_eq!(error.category, EngineFailureCategory::Validation);
        assert_eq!(error.code.as_ref(), "missing_migration_provider_target");
        assert_eq!(
            error.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(
            !store_exists,
            "rejected fresh command must not create a Store"
        );
    }

    #[test]
    fn provider_free_fresh_shadow_rejects_before_store_creation() {
        let parent = test_path("provider-free-fresh-shadow");
        fs::create_dir(&parent).expect("fixture parent creates");
        let store_path = parent.join("store");
        let target = EngineEvolutionTarget::new(EngineStoreTarget::directory(
            store_path.to_string_lossy().into_owned(),
        ));
        let error = execute_live_evolution(
            &target,
            "evolution:provider-free-shadow",
            provider_free_shadow_command("command:provider-free-shadow"),
            None,
        )
        .expect_err("fresh shadow command requires its exact driver");
        let store_exists = store_path.exists();
        fs::remove_dir_all(parent).expect("fixture parent removes");

        assert_eq!(error.category, EngineFailureCategory::Validation);
        assert_eq!(error.code.as_ref(), "missing_shadow_provider_target");
        assert_eq!(
            error.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(
            !store_exists,
            "rejected fresh command must not create a Store"
        );
    }

    #[cfg(unix)]
    #[test]
    fn invalid_embedded_run_rejects_before_plugin_describe() {
        use std::os::unix::fs::PermissionsExt;

        let directory = test_path("embedded-preflight-plugin");
        fs::create_dir(&directory).expect("fixture directory creates");
        let marker = directory.join("invoked");
        let executable = directory.join("plugin.sh");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf invoked > '{}'\nexit 1\n",
                marker.display()
            ),
        )
        .expect("fixture process writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("fixture process is executable");

        let valid_plan = embedded_plan(serde_json::json!({ "type": "string" }));
        let mut wrong_identity = valid_plan.clone();
        wrong_identity.plan_id = format!("sha256:{}", "0".repeat(64));
        for (plan, input, run_id) in [
            (
                wrong_identity,
                Value::String("valid".to_owned()),
                "run:valid",
            ),
            (valid_plan.clone(), Value::Null, "run:valid"),
            (
                valid_plan,
                Value::String("valid".to_owned()),
                "run:\u{0085}forged",
            ),
        ] {
            let request = EngineRequest::Run {
                plan,
                input,
                plugin: process_target(executable.to_string_lossy().into_owned()),
                run_id: run_id.to_owned(),
            };
            let input = serde_json::to_vec(&EngineRequestEnvelope::new(request))
                .expect("request serializes");
            decode_and_execute_request(&input)
                .expect_err("invalid execution request fails before plugin construction");
            assert!(
                !marker.exists(),
                "Plan, input, and Run admission must precede plugin describe"
            );
        }

        fs::remove_dir_all(directory).expect("fixture directory removes");
    }

    #[test]
    fn embedded_run_accepts_only_the_exact_process_provider() {
        let mut target = process_target(
            test_path("unsupported-embedded-provider")
                .to_string_lossy()
                .into_owned(),
        );
        target.provider = "example.process-lookalike/1".to_owned();

        let failure = admit_local_process_target(&target, EnginePhase::ExecutePlan)
            .expect_err("a non-process provider is never interpreted as a local process");
        assert_eq!(failure.code.as_ref(), "unsupported_execution_provider");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
    }

    #[test]
    fn rpc_bounds_more_than_one_hundred_contract_issues_inside_one_v4_failure() {
        let all_of = (0..101)
            .map(|index| serde_json::json!({"required": [format!("field_{index}")]}))
            .collect::<Vec<_>>();
        let request = EngineRequest::Run {
            plan: embedded_plan(serde_json::json!({
                "type": "object",
                "allOf": all_of,
            })),
            input: serde_json::json!({}),
            plugin: process_target("must-not-be-opened"),
            run_id: "run:bounded-contract-issues".to_owned(),
        };
        let input =
            serde_json::to_vec(&EngineRequestEnvelope::new(request)).expect("request serializes");

        let failure = decode_and_execute_request(&input)
            .expect_err("invalid input returns one bounded contract failure");
        assert_eq!(failure.category, EngineFailureCategory::ContractViolation);
        assert_eq!(failure.issues.len(), 100);
        assert_eq!(
            failure
                .issues
                .last()
                .expect("summary issue exists")
                .code
                .as_ref(),
            "contract_issues_omitted"
        );
        failure.verify().expect("bounded projection is valid v4");
        let envelope = EngineResponseEnvelope::<Value, EngineResponse>::failure(failure);
        let wire = serde_json::to_value(envelope).expect("failure envelope serializes");
        assert_eq!(wire["outcome"], "failure");
        assert_eq!(wire["engine_protocol"], "cymule.engine/4");
        assert_eq!(wire["error"]["issues"].as_array().unwrap().len(), 100);
    }

    #[test]
    fn invalid_internal_failure_is_reprojected_as_one_valid_v4_envelope() {
        let invalid = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ExecutePlan,
            "INVALID FAILURE CODE",
            "",
        );
        let projected = invalid.into_wire_failure();
        assert_eq!(projected.category, EngineFailureCategory::PluginDefect);
        assert_eq!(projected.code.as_ref(), "engine_failure_projection_invalid");
        projected
            .verify()
            .expect("terminal wire projection is valid");
        let wire = serde_json::to_value(EngineResponseEnvelope::<Value, EngineResponse>::failure(
            projected,
        ))
        .expect("terminal failure envelope serializes");
        assert_eq!(wire["outcome"], "failure");
        assert!(wire.get("request").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn rpc_projects_owner_cancellation_as_cancelled_without_retry() {
        use std::os::unix::fs::PermissionsExt;

        let directory = test_path("cancelled-run-plugin");
        fs::create_dir(&directory).expect("fixture directory creates");
        let marker = directory.join("invoked");
        let executable = directory.join("plugin.sh");
        fs::write(
            &executable,
            format!("#!/bin/sh\nprintf invoked > '{}'\n", marker.display()),
        )
        .expect("fixture process writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("fixture process is executable");
        let request = EngineRequest::Run {
            plan: embedded_plan(serde_json::json!({ "type": "string" })),
            input: Value::String("valid".to_owned()),
            plugin: process_target(executable.to_string_lossy().into_owned()),
            run_id: "run:cancelled".to_owned(),
        };
        let input =
            serde_json::to_vec(&EngineRequestEnvelope::new(request)).expect("request serializes");
        let cancellation = ProcessCancellation::new().expect("cancellation authority creates");
        cancellation.cancel();

        let failure =
            super::decode_and_execute_request_with_cancellation(&input, Some(&cancellation))
                .expect_err("owner cancellation returns one typed Engine failure");
        assert_eq!(failure.category, EngineFailureCategory::Cancelled);
        assert_eq!(failure.code.as_ref(), "process_invocation_cancelled");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::Never)
        );
        assert!(
            !marker.exists(),
            "pre-cancelled request never starts the plugin"
        );

        fs::remove_dir_all(directory).expect("fixture directory removes");
    }

    #[test]
    fn rpc_rejects_removed_live_members_before_verification() {
        let command = LiveEvolutionCommand::Apply {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:verify-presence".to_owned(),
            template_id: "template:test".to_owned(),
            command: Box::new(EvolutionCommand::SetRollout {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "command:rollout-presence".to_owned(),
                decision: RolloutDecision {
                    decision_id: "decision:presence".to_owned(),
                    fallback_plan: format!("sha256:{}", "1".repeat(64)),
                    target_plan: format!("sha256:{}", "2".repeat(64)),
                    mode: RolloutMode::Active,
                },
            }),
        };
        let request = serde_json::to_value(EngineRequest::VerifyLiveEvolutionCommand {
            command: command.clone(),
        })
        .expect("request serializes");
        assert!(request["command"].get("safe_point").is_none());
        let input = serde_json::to_vec(&EngineRequestEnvelope::new(request.clone()))
            .expect("request envelope serializes");
        let (echo, _) = decode_and_execute_request(&input).expect("current request verifies");
        assert_eq!(echo, request);

        let mut removed_member = request;
        removed_member["command"]["safe_point"] = Value::Null;
        let null_input = serde_json::to_vec(&EngineRequestEnvelope::new(removed_member))
            .expect("request envelope serializes");
        let failure = decode_and_execute_request(&null_input)
            .expect_err("removed member is rejected before semantic verification");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.phase, EnginePhase::DecodeRequest);
        assert_eq!(failure.code.as_ref(), "invalid_engine_request");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(failure.message.contains("safe_point"));
    }

    #[test]
    fn rpc_rejects_explicit_null_for_omission_only_read_target_before_store_io() {
        let path = test_path("correlated-null-target");
        let request = serde_json::json!({
            "type": "execute_durable",
            "target": {
                "store": {
                    "provider": "cymule.directory-store/5",
                    "location": path.to_string_lossy(),
                },
                "executor": null,
            },
            "command": {
                "type": "run_index_page",
                "control_version": DURABLE_CONTROL_VERSION,
                "expected_revision": null,
                "cursor": null,
                "limit": 1,
                "max_canonical_bytes": MAX_DURABLE_QUERY_PAGE_BYTES,
            },
        });
        let input = serde_json::to_vec(&EngineRequestEnvelope::new(request.clone()))
            .expect("request envelope serializes");

        let failure = decode_and_execute_request(&input)
            .expect_err("explicit null is rejected before opening the store");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.phase, EnginePhase::ValidateRequest);
        assert_eq!(failure.code.as_ref(), "invalid_engine_request");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(failure.message.contains("/target/executor"));
        assert!(!path.exists(), "request rejection performs no store I/O");
    }

    #[cfg(unix)]
    #[test]
    fn store_provider_generations_dispatch_exactly_and_reject_predecessors_before_io() {
        let directory_path = test_path("provider-generation-directory");
        let directory_target =
            EngineStoreTarget::directory(directory_path.to_string_lossy().into_owned());
        let admitted = admit_store_target(&directory_target, EnginePhase::ExecuteDurable)
            .expect("directory /5 target admits");
        drop(open_store(&admitted).expect("directory /5 dispatch opens"));
        let marker: Value = cymule_core::decode_json(
            &fs::read(directory_path.join("store-meta.json"))
                .expect("directory generation marker reads"),
        )
        .expect("directory generation marker decodes");
        assert_eq!(
            marker["schema_version"],
            Value::String("cymule.directory-store/5".to_owned())
        );

        let sqlite_path = test_path("provider-generation-sqlite").with_extension("sqlite");
        let sqlite_target = EngineStoreTarget::sqlite(
            sqlite_path.to_string_lossy().into_owned(),
            "domain:provider-generation",
        );
        let admitted = admit_store_target(&sqlite_target, EnginePhase::ExecuteDurable)
            .expect("SQLite /6 target admits");
        drop(open_store(&admitted).expect("SQLite /6 dispatch opens"));
        cymule_store_sqlite::SqliteStore::open_read_only(
            &sqlite_path,
            "domain:provider-generation",
        )
        .expect("SQLite /6 physical generation reopens");

        for (provider, domain, location) in [
            (
                "cymule.directory-store/4",
                None,
                test_path("rejected-directory-v4"),
            ),
            (
                "cymule.directory-store/3",
                None,
                test_path("rejected-directory-v3"),
            ),
            (
                "cymule.sqlite-store/5",
                Some("domain:rejected".to_owned()),
                test_path("rejected-sqlite-v5").with_extension("sqlite"),
            ),
            (
                "cymule.sqlite-store/4",
                Some("domain:rejected".to_owned()),
                test_path("rejected-sqlite-v4").with_extension("sqlite"),
            ),
        ] {
            let target = EngineStoreTarget {
                provider: provider.to_owned(),
                location: location.to_string_lossy().into_owned(),
                domain,
            };
            let Err(failure) = admit_store_target(&target, EnginePhase::ExecuteDurable) else {
                panic!("superseded selector was admitted")
            };
            assert_eq!(failure.code.as_ref(), "unsupported_store_provider");
            let request = EngineRequest::ExecuteDurable {
                target: EngineDurableTarget::query(target),
                command: run_index_query(),
            };
            let input = serde_json::to_vec(&EngineRequestEnvelope::new(
                serde_json::to_value(request).expect("request serializes"),
            ))
            .expect("request envelope serializes");
            let failure = decode_and_execute_request(&input)
                .expect_err("Engine ingress rejects the old selector");
            assert_eq!(failure.code.as_ref(), "unsupported_store_provider");
            assert!(!location.exists());
            assert!(!PathBuf::from(format!("{}-wal", location.display())).exists());
            assert!(!PathBuf::from(format!("{}-shm", location.display())).exists());
        }

        fs::remove_dir_all(directory_path).expect("directory fixture removes");
        for path in [
            sqlite_path.clone(),
            PathBuf::from(format!("{}-wal", sqlite_path.display())),
            PathBuf::from(format!("{}-shm", sqlite_path.display())),
            PathBuf::from(format!("{}-journal", sqlite_path.display())),
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("SQLite fixture cleanup failed: {error}"),
            }
        }
    }

    #[test]
    fn superseded_store_selectors_reject_before_any_store_io() {
        for (provider, domain, location) in [
            (
                "cymule.directory-store/4",
                None,
                test_path("rejected-directory-v4-only"),
            ),
            (
                "cymule.directory-store/3",
                None,
                test_path("rejected-directory-v3-only"),
            ),
            (
                "cymule.sqlite-store/5",
                Some("domain:rejected".to_owned()),
                test_path("rejected-sqlite-v5-only").with_extension("sqlite"),
            ),
            (
                "cymule.sqlite-store/4",
                Some("domain:rejected".to_owned()),
                test_path("rejected-sqlite-v4-only").with_extension("sqlite"),
            ),
        ] {
            let target = EngineStoreTarget {
                provider: provider.to_owned(),
                location: location.to_string_lossy().into_owned(),
                domain,
            };
            let request = EngineRequest::ExecuteDurable {
                target: EngineDurableTarget::query(target),
                command: run_index_query(),
            };
            let input = serde_json::to_vec(&EngineRequestEnvelope::new(
                serde_json::to_value(request).expect("request serializes"),
            ))
            .expect("request envelope serializes");
            let failure = decode_and_execute_request(&input)
                .expect_err("Engine ingress rejects the old selector");
            assert_eq!(failure.code.as_ref(), "unsupported_store_provider");
            assert!(!location.exists());
            assert!(!PathBuf::from(format!("{}-wal", location.display())).exists());
            assert!(!PathBuf::from(format!("{}-shm", location.display())).exists());
        }
    }

    #[test]
    fn durable_command_capabilities_require_the_exact_executor_clock_cartesian_product() {
        let commands = [
            ("store-only", run_index_query(), false, false),
            ("provider-only", resolution_command(), true, false),
            ("execution", execution_command(), true, true),
        ];
        for (class, command, expected_executor, expected_clock) in commands {
            for executor_present in [false, true] {
                for clock_present in [false, true] {
                    let suffix = format!("{class}-{executor_present}-{clock_present}");
                    let store_path = test_path(&format!("authority-cartesian-store-{suffix}"));
                    let clock_path = test_path(&format!("authority-cartesian-clock-{suffix}"))
                        .with_extension("sqlite");
                    let target = EngineDurableTarget {
                        store: EngineStoreTarget::directory(
                            store_path.to_string_lossy().into_owned(),
                        ),
                        executor: executor_present.then(|| {
                            process_target(
                                test_path(&format!("authority-cartesian-plugin-{suffix}"))
                                    .to_string_lossy()
                                    .into_owned(),
                            )
                        }),
                        clock: clock_present.then(|| {
                            cymule_runtime::EngineClockTarget::sqlite(
                                clock_path.to_string_lossy().into_owned(),
                                format!("clock:{suffix}"),
                                format!("sha256:{}", "7".repeat(64)),
                            )
                        }),
                    };
                    let exact =
                        executor_present == expected_executor && clock_present == expected_clock;
                    assert_eq!(
                        admit_durable_request(&target, &command).is_ok(),
                        exact,
                        "{class} capability mismatch for executor={executor_present} clock={clock_present}"
                    );
                    if !exact {
                        let failure = execute_durable(&target, command.clone(), None)
                            .expect_err("non-exact capability shape is rejected");
                        assert_eq!(failure.category, EngineFailureCategory::Validation);
                        assert_eq!(
                            failure.retry_disposition,
                            Some(EngineRetryDisposition::CorrectAndRetry)
                        );
                        assert!(!store_path.exists());
                        assert_sqlite_files_absent(&clock_path);
                    }
                }
            }
        }
    }

    #[test]
    fn missing_executor_rejects_before_directory_or_sqlite_store_io() {
        let directory_path = test_path("missing-executor-directory");
        let sqlite_path = test_path("missing-executor-sqlite").with_extension("sqlite");
        for (store, path, sqlite) in [
            (
                EngineStoreTarget::directory(directory_path.to_string_lossy().into_owned()),
                directory_path,
                false,
            ),
            (
                EngineStoreTarget::sqlite(
                    sqlite_path.to_string_lossy().into_owned(),
                    "domain:missing-executor",
                ),
                sqlite_path,
                true,
            ),
        ] {
            let clock_path = test_path("missing-executor-clock").with_extension("sqlite");
            let target = EngineDurableTarget {
                store,
                executor: None,
                clock: Some(cymule_runtime::EngineClockTarget::sqlite(
                    clock_path.to_string_lossy().into_owned(),
                    "clock:missing-executor",
                    format!("sha256:{}", "3".repeat(64)),
                )),
            };

            let failure = execute_durable(&target, execution_command(), None)
                .expect_err("missing executor is rejected at target admission");
            assert_eq!(failure.category, EngineFailureCategory::Validation);
            assert_eq!(failure.code.as_ref(), "missing_execution_provider");
            assert_eq!(
                failure.retry_disposition,
                Some(EngineRetryDisposition::CorrectAndRetry)
            );
            if sqlite {
                assert_sqlite_files_absent(&path);
            } else {
                assert!(!path.exists());
            }
            assert_sqlite_files_absent(&clock_path);
        }
    }

    #[test]
    fn missing_clock_rejects_before_directory_or_sqlite_store_io() {
        let directory_path = test_path("missing-clock-directory");
        let sqlite_path = test_path("missing-clock-sqlite").with_extension("sqlite");
        for (store, path, sqlite) in [
            (
                EngineStoreTarget::directory(directory_path.to_string_lossy().into_owned()),
                directory_path,
                false,
            ),
            (
                EngineStoreTarget::sqlite(
                    sqlite_path.to_string_lossy().into_owned(),
                    "domain:missing-clock",
                ),
                sqlite_path,
                true,
            ),
        ] {
            let target = EngineDurableTarget {
                store,
                executor: Some(process_target("unused-process")),
                clock: None,
            };

            let failure = execute_durable(&target, execution_command(), None)
                .expect_err("missing Clock is rejected at target admission");
            assert_eq!(failure.category, EngineFailureCategory::Validation);
            assert_eq!(failure.code.as_ref(), "missing_clock_provider");
            assert_eq!(
                failure.retry_disposition,
                Some(EngineRetryDisposition::CorrectAndRetry)
            );
            if sqlite {
                assert_sqlite_files_absent(&path);
            } else {
                assert!(!path.exists());
            }
        }
    }

    #[test]
    fn directory_store_subtree_is_rejected_as_clock_authority_before_io() {
        let root = test_path("directory-store-clock-subtree");
        fs::create_dir(&root).expect("existing Store root creates");
        let clock = root.join("head.json");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::directory(root.to_string_lossy().into_owned()),
            process_target("must-not-be-opened"),
            cymule_runtime::EngineClockTarget::sqlite(
                clock.to_string_lossy().into_owned(),
                "clock:directory-subtree",
                format!("sha256:{}", "d".repeat(64)),
            ),
        );

        let failure = execute_durable(&target, execution_command(), None)
            .expect_err("Directory Store owns its complete subtree");
        assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
        assert!(
            fs::read_dir(&root)
                .expect("fixture root reads")
                .next()
                .is_none(),
            "rejection must not create Clock or Directory Store files"
        );
        fs::remove_dir(root).expect("empty fixture root removes");
    }

    #[test]
    fn sqlite_store_and_clock_sidecar_footprints_are_rejected_before_io() {
        let root = test_path("sqlite-store-clock-sidecars");
        fs::create_dir(&root).expect("authority parent creates");
        for (case, store, clock) in [
            (
                "store-base-clock-wal",
                root.join("first.sqlite"),
                root.join("first.sqlite-wal"),
            ),
            (
                "store-wal-clock-base",
                root.join("second.sqlite-wal"),
                root.join("second.sqlite"),
            ),
            (
                "store-base-clock-journal",
                root.join("third.sqlite"),
                root.join("third.sqlite-journal"),
            ),
            (
                "store-base-clock-shm",
                root.join("fourth.sqlite"),
                root.join("fourth.sqlite-shm"),
            ),
            (
                "store-shm-clock-base",
                root.join("fifth.sqlite-shm"),
                root.join("fifth.sqlite"),
            ),
            (
                "store-journal-clock-base",
                root.join("sixth.sqlite-journal"),
                root.join("sixth.sqlite"),
            ),
        ] {
            let target = EngineDurableTarget::execute(
                EngineStoreTarget::sqlite(
                    store.to_string_lossy().into_owned(),
                    format!("domain:{case}"),
                ),
                process_target("must-not-be-opened"),
                cymule_runtime::EngineClockTarget::sqlite(
                    clock.to_string_lossy().into_owned(),
                    format!("clock:{case}"),
                    format!("sha256:{}", "e".repeat(64)),
                ),
            );

            let failure = execute_durable(&target, execution_command(), None)
                .expect_err("SQLite base and sidecars are one owned footprint");
            assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
            assert_sqlite_files_absent(&store);
            assert_sqlite_files_absent(&clock);
        }
        fs::remove_dir(root).expect("empty authority parent removes");
    }

    #[test]
    fn store_and_clock_lexical_aliases_are_rejected_before_authority_io() {
        let root = test_path("store-clock-lexical-alias");
        let discarded = root.join("discarded");
        fs::create_dir_all(&discarded).expect("lexical alias parent creates");
        let canonical = root.join("authority.sqlite");
        let lexical_alias = discarded.join("..").join("authority.sqlite");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::sqlite(
                lexical_alias.to_string_lossy().into_owned(),
                "domain:path-alias",
            ),
            process_target("unused-process"),
            cymule_runtime::EngineClockTarget::sqlite(
                canonical.to_string_lossy().into_owned(),
                "clock:path-alias",
                format!("sha256:{}", "d".repeat(64)),
            ),
        );

        let failure = execute_durable(&target, execution_command(), None)
            .expect_err("one filesystem authority cannot be both Store and Clock");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert_sqlite_files_absent(&canonical);
        fs::remove_dir_all(root).expect("lexical alias fixture removes");
    }

    #[test]
    fn identical_store_and_clock_location_creates_neither_authority() {
        let location = test_path("store-clock-identical").with_extension("sqlite");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::sqlite(
                location.to_string_lossy().into_owned(),
                "domain:identical-authority",
            ),
            process_target("unused-process"),
            cymule_runtime::EngineClockTarget::sqlite(
                location.to_string_lossy().into_owned(),
                "clock:identical-authority",
                format!("sha256:{}", "f".repeat(64)),
            ),
        );

        let failure = execute_durable(&target, execution_command(), None)
            .expect_err("identical Store and Clock location is rejected pre-I/O");
        assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
        assert_sqlite_files_absent(&location);
    }

    #[cfg(unix)]
    #[test]
    fn store_and_clock_existing_canonical_aliases_are_rejected_without_creating_authority() {
        use std::os::unix::fs::symlink;

        let root = test_path("store-clock-canonical-alias");
        let real = root.join("real");
        let alias = root.join("alias");
        fs::create_dir_all(&real).expect("canonical parent creates");
        symlink(&real, &alias).expect("directory alias creates");
        let store_path = alias.join("authority.sqlite");
        let clock_path = real.join("authority.sqlite");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::sqlite(
                store_path.to_string_lossy().into_owned(),
                "domain:canonical-alias",
            ),
            process_target("unused-process"),
            cymule_runtime::EngineClockTarget::sqlite(
                clock_path.to_string_lossy().into_owned(),
                "clock:canonical-alias",
                format!("sha256:{}", "e".repeat(64)),
            ),
        );

        let failure = execute_durable(&target, execution_command(), None)
            .expect_err("canonical filesystem aliases are one authority location");
        assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
        assert_sqlite_files_absent(&clock_path);
        fs::remove_dir_all(root).expect("alias fixture removes");
    }

    #[test]
    fn fresh_case_and_normalization_aliases_are_rejected_by_the_host_volume_before_io() {
        let root = test_path("store-clock-volume-semantics");
        fs::create_dir(&root).expect("authority parent creates");
        for (case, store_name, clock_name) in [
            ("case", "Store.sqlite", "store.sqlite"),
            ("normalization", "caf\u{e9}.sqlite", "cafe\u{301}.sqlite"),
        ] {
            if !filesystem_components_equivalent(
                &root,
                OsStr::new(store_name),
                OsStr::new(clock_name),
                EnginePhase::ExecuteDurable,
            )
            .expect("host volume component semantics probe succeeds")
            {
                continue;
            }
            let store = root.join(store_name);
            let clock = root.join(clock_name);
            let target = EngineDurableTarget::execute(
                EngineStoreTarget::sqlite(
                    store.to_string_lossy().into_owned(),
                    format!("domain:{case}"),
                ),
                process_target("must-not-be-opened"),
                cymule_runtime::EngineClockTarget::sqlite(
                    clock.to_string_lossy().into_owned(),
                    format!("clock:{case}"),
                    format!("sha256:{}", "a".repeat(64)),
                ),
            );

            let failure = execute_durable(&target, execution_command(), None)
                .expect_err("the host filesystem decides that both fresh names alias");
            assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
            assert_sqlite_files_absent(&store);
            assert_sqlite_files_absent(&clock);
        }
        assert!(
            fs::read_dir(&root)
                .expect("authority parent reads")
                .next()
                .is_none(),
            "component probes and rejected requests leave no files"
        );
        fs::remove_dir(root).expect("empty authority parent removes");
    }

    #[test]
    fn windows_style_component_model_detects_fresh_authority_alias_and_subtree() {
        let store = [OsString::from("Store")];
        let same_clock = [OsString::from("store")];
        let nested_clock = [OsString::from("STORE"), OsString::from("clock.sqlite")];
        let windows_equivalent = |left: &OsStr, right: &OsStr| {
            Ok::<_, ()>(
                left.to_string_lossy()
                    .eq_ignore_ascii_case(&right.to_string_lossy()),
            )
        };

        assert!(
            component_sequences_equal(&store, &same_clock, windows_equivalent)
                .expect("infallible Windows model")
        );
        assert!(
            component_sequence_is_prefix(&store, &nested_clock, windows_equivalent)
                .expect("infallible Windows model")
        );
    }

    #[test]
    fn existing_store_and_clock_hardlinks_are_one_cross_platform_file_identity() {
        let root = test_path("store-clock-hardlink-alias");
        fs::create_dir(&root).expect("authority parent creates");
        let store = root.join("store.sqlite");
        let clock = root.join("clock.sqlite");
        fs::write(&store, b"preexisting authority bytes").expect("Store fixture writes");
        fs::hard_link(&store, &clock).expect("Clock hardlink fixture creates");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::sqlite(
                store.to_string_lossy().into_owned(),
                "domain:hardlink-alias",
            ),
            process_target("must-not-be-opened"),
            cymule_runtime::EngineClockTarget::sqlite(
                clock.to_string_lossy().into_owned(),
                "clock:hardlink-alias",
                format!("sha256:{}", "b".repeat(64)),
            ),
        );

        let failure = execute_durable(&target, execution_command(), None)
            .expect_err("hardlinks identify one physical authority");
        assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
        assert_eq!(
            fs::read(&store).expect("Store fixture remains readable"),
            b"preexisting authority bytes"
        );
        fs::remove_dir_all(root).expect("hardlink fixture removes");
    }

    #[cfg(unix)]
    #[test]
    fn identity_reread_rejects_an_alias_introduced_after_clock_open() {
        let root = test_path("store-clock-open-race");
        fs::create_dir(&root).expect("authority parent creates");
        let store_path = root.join("store.sqlite");
        let clock_path = root.join("clock.sqlite");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::sqlite(
                store_path.to_string_lossy().into_owned(),
                "domain:open-race",
            ),
            process_target("not-materialized-by-admission"),
            cymule_runtime::EngineClockTarget::sqlite(
                clock_path.to_string_lossy().into_owned(),
                "clock:open-race",
                format!("sha256:{}", "c".repeat(64)),
            ),
        );
        let admitted = admit_durable_request(&target, &execution_command())
            .expect("distinct fresh authorities admit");
        let DurableCapability::Execution { clock, .. } = &admitted.capability else {
            panic!("execution command admits a Clock capability");
        };
        let opened_clock = cymule_clock_system::SqliteClock::open(
            &clock.authority.location,
            &clock.target.source_id,
            &clock.target.source_generation,
        )
        .expect("Clock authority opens");
        fs::hard_link(&clock.authority.location, &store_path)
            .expect("concurrent hardlink alias creates after admission");

        let failure = reject_store_clock_alias(&admitted.store, &clock.authority)
            .expect_err("stable identity reread rejects the post-admission alias");
        assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
        drop(opened_clock);
        fs::remove_dir_all(root).expect("open-race fixture removes");
    }

    #[cfg(unix)]
    #[test]
    fn directory_store_rejects_an_existing_clock_sidecar_alias_into_its_subtree() {
        use std::os::unix::fs::symlink;

        let root = test_path("directory-store-clock-sidecar-alias");
        let store = root.join("store");
        fs::create_dir_all(&store).expect("Directory Store fixture creates");
        let owned = store.join("owned.sqlite-bytes");
        fs::write(&owned, b"owned by the Directory Store")
            .expect("Directory Store fixture file writes");
        let clock = root.join("clock.sqlite");
        let clock_wal = PathBuf::from(format!("{}-wal", clock.display()));
        symlink(&owned, &clock_wal).expect("Clock WAL alias creates");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::directory(store.to_string_lossy().into_owned()),
            process_target("must-not-be-opened"),
            cymule_runtime::EngineClockTarget::sqlite(
                clock.to_string_lossy().into_owned(),
                "clock:directory-sidecar-alias",
                format!("sha256:{}", "e".repeat(64)),
            ),
        );

        let failure = execute_durable(&target, execution_command(), None)
            .expect_err("Clock sidecar aliases cannot enter a Directory Store subtree");
        assert_eq!(failure.code.as_ref(), "store_clock_location_conflict");
        assert!(!clock.exists(), "Clock base must not be created");
        assert_eq!(
            fs::read(&owned).expect("Directory Store fixture remains readable"),
            b"owned by the Directory Store",
        );
        fs::remove_dir_all(root).expect("sidecar alias fixture removes");
    }

    #[cfg(unix)]
    #[test]
    fn rejected_execution_implementation_leaves_directory_and_sqlite_stores_absent() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = test_path("execution-authority-fixture");
        fs::create_dir(&fixture).expect("fixture directory creates");
        let executable = fixture.join("plugin.sh");
        fs::write(&executable, "#!/bin/sh\nexit 1\n").expect("fixture executable writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("fixture executable is executable");
        for (case, executor) in [
            (
                "missing-bytes",
                process_target(
                    test_path("missing-execution-provider")
                        .to_string_lossy()
                        .into_owned(),
                ),
            ),
            (
                "wrong-revision",
                pinned_process_target(
                    executable.to_string_lossy().into_owned(),
                    format!("sha256:{}", "0".repeat(64)),
                ),
            ),
        ] {
            let directory_path = test_path(&format!("{case}-directory-store"));
            let sqlite_path = test_path(&format!("{case}-sqlite-store")).with_extension("sqlite");
            for (store, path, sqlite) in [
                (
                    EngineStoreTarget::directory(directory_path.to_string_lossy().into_owned()),
                    directory_path,
                    false,
                ),
                (
                    EngineStoreTarget::sqlite(
                        sqlite_path.to_string_lossy().into_owned(),
                        format!("domain:{case}"),
                    ),
                    sqlite_path,
                    true,
                ),
            ] {
                let clock_path = test_path(&format!("{case}-clock")).with_extension("sqlite");
                let target = EngineDurableTarget::execute(
                    store,
                    executor.clone(),
                    cymule_runtime::EngineClockTarget::sqlite(
                        clock_path.to_string_lossy().into_owned(),
                        format!("clock:{case}"),
                        format!("sha256:{}", "7".repeat(64)),
                    ),
                );

                execute_durable(&target, execution_command(), None)
                    .expect_err("invalid execution implementation fails before store open");
                if sqlite {
                    assert_sqlite_files_absent(&path);
                } else {
                    assert!(!path.exists());
                }
                assert_sqlite_files_absent(&clock_path);
            }
        }
        fs::remove_dir_all(fixture).expect("fixture directory removes");
    }

    #[cfg(unix)]
    #[test]
    fn rejected_clock_authority_leaves_directory_and_sqlite_stores_absent() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = test_path("clock-authority-fixture");
        fs::create_dir(&fixture).expect("fixture directory creates");
        let executable = fixture.join("plugin.sh");
        fs::write(
            &executable,
            r#"#!/bin/sh
/bin/cat >/dev/null
printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"process:clock-admission","components":{},"effects":{}}}'
"#,
        )
        .expect("fixture executable writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("fixture executable is executable");
        let clock_path = fixture.join("clock.sqlite");
        drop(
            cymule_store_sqlite::SqliteStore::open(&clock_path, "domain:foreign-clock")
                .expect("foreign Store authority initializes"),
        );

        let directory_path = test_path("wrong-clock-directory-store");
        let sqlite_path = test_path("wrong-clock-sqlite-store").with_extension("sqlite");
        for (store, path, sqlite) in [
            (
                EngineStoreTarget::directory(directory_path.to_string_lossy().into_owned()),
                directory_path,
                false,
            ),
            (
                EngineStoreTarget::sqlite(
                    sqlite_path.to_string_lossy().into_owned(),
                    "domain:wrong-clock",
                ),
                sqlite_path,
                true,
            ),
        ] {
            let target = EngineDurableTarget::execute(
                store,
                process_target(executable.to_string_lossy().into_owned()),
                cymule_runtime::EngineClockTarget::sqlite(
                    clock_path.to_string_lossy().into_owned(),
                    "clock:foreign-authority",
                    format!("sha256:{}", "c".repeat(64)),
                ),
            );
            execute_durable(&target, execution_command(), None)
                .expect_err("foreign Clock authority fails before store open");
            if sqlite {
                assert_sqlite_files_absent(&path);
            } else {
                assert!(!path.exists());
            }
        }
        fs::remove_dir_all(fixture).expect("fixture directory removes");
    }

    #[cfg(unix)]
    #[test]
    fn durable_open_consumes_the_single_manifest_admission_without_redescribe() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = test_path("single-manifest-admission");
        fs::create_dir(&fixture).expect("fixture directory creates");
        let counter = fixture.join("describe-count");
        let executable = fixture.join("plugin.sh");
        fs::write(
            &executable,
            format!(
                r#"#!/bin/sh
request=$(/bin/cat)
case "$request" in
  *'"type":"describe"'*) ;;
  *) exit 9 ;;
esac
count=0
test ! -f '{counter}' || count=$(/bin/cat '{counter}')
count=$((count + 1))
printf '%s' "$count" > '{counter}'
if test "$count" -eq 1; then
  implementation='process:first-manifest'
else
  implementation='process:forged-second-manifest'
fi
printf '%s' "{{\"type\":\"manifest\",\"manifest\":{{\"plugin_version\":\"cymule.plugin/3\",\"implementation_id\":\"$implementation\",\"components\":{{}},\"effects\":{{}}}}}}"
"#,
                counter = counter.display(),
            ),
        )
        .expect("fixture executable writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("fixture executable is executable");
        let store = test_path("single-manifest-store");
        let clock = fixture.join("clock.sqlite");
        let target = EngineDurableTarget::execute(
            EngineStoreTarget::directory(store.to_string_lossy().into_owned()),
            process_target(executable.to_string_lossy().into_owned()),
            cymule_runtime::EngineClockTarget::sqlite(
                clock.to_string_lossy().into_owned(),
                "clock:single-manifest",
                format!("sha256:{}", "9".repeat(64)),
            ),
        );

        let failure = execute_durable(&target, execution_command(), None)
            .expect_err("empty Store has no Run to resume");
        assert_eq!(failure.category, EngineFailureCategory::NotFound);
        assert_eq!(
            fs::read_to_string(&counter).expect("Describe counter reads"),
            "1",
            "runtime open must consume admission without a second provider Describe"
        );

        fs::remove_dir_all(store).expect("Store fixture removes");
        fs::remove_dir_all(fixture).expect("provider fixture removes");
    }

    #[cfg(unix)]
    struct UnknownEffectFixture {
        root: PathBuf,
        invocations: PathBuf,
        executable: PathBuf,
        run_id: &'static str,
        store: EngineStoreTarget,
        intent_id: String,
        provider_invocations_before_store_only: String,
    }

    #[cfg(unix)]
    fn start_unknown_effect_fixture() -> UnknownEffectFixture {
        use std::os::unix::fs::PermissionsExt;

        let fixture = test_path("store-only-effect-resolution");
        fs::create_dir(&fixture).expect("fixture directory creates");
        let invocations = fixture.join("provider-invocations");
        let executable = fixture.join("plugin.sh");
        fs::write(
            &executable,
            format!(
                r#"#!/bin/sh
request=$(/bin/cat)
attempt=$(printf '%s' "$request" | /usr/bin/sed -E 's/.*"attempt":(\{{[^}}]*\}}).*/\1/')
count=0
test ! -f '{invocations}' || count=$(/bin/cat '{invocations}')
count=$((count + 1))
printf '%s' "$count" > '{invocations}'
case "$request" in
  *'"type":"describe"'*)
    printf '%s' '{{"type":"manifest","manifest":{{"plugin_version":"cymule.plugin/3","implementation_id":"process:store-only-resolution","components":{{}},"effects":{{"test.effect":{{"implementation_revision":"1","can_reconcile":true}}}}}}}}'
    ;;
  *'"type":"prepare_effect"'*)
    printf '%s' '{{"type":"prepared"}}'
    ;;
  *'"type":"dispatch_effect"'*)
    printf '%s' "{{\"type\":\"effect_result\",\"attempt\":$attempt,\"outcome\":\"unknown\",\"value\":null}}"
    ;;
  *'"type":"reconcile_effect"'*)
    printf '%s' "{{\"type\":\"reconciliation_result\",\"attempt\":$attempt,\"resolution\":\"resolved_not_applied\",\"value\":null}}"
    ;;
  *) exit 19 ;;
esac
"#,
                invocations = invocations.display(),
            ),
        )
        .expect("fixture executable writes");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("fixture executable is executable");

        let run_id = "run:store-only-resolution";
        let store_path = fixture.join("store");
        let clock_path = fixture.join("clock.sqlite");
        let store = EngineStoreTarget::directory(store_path.to_string_lossy().into_owned());
        let clock = cymule_runtime::EngineClockTarget::sqlite(
            clock_path.to_string_lossy().into_owned(),
            "clock:store-only-resolution",
            format!("sha256:{}", "a".repeat(64)),
        );
        let observation = observe_clock(&clock, run_id).expect("Clock observation is issued");
        let started = execute_durable(
            &EngineDurableTarget::execute(
                store.clone(),
                process_target(executable.to_string_lossy().into_owned()),
                clock,
            ),
            DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.to_owned(),
                candidate: ambiguous_effect_candidate(),
                input: serde_json::json!({"mutation": "ambiguous"}),
                execution: ExecutionClaimRequest {
                    owner: "driver:store-only-resolution".to_owned(),
                    clock: observation,
                    ttl: 10,
                },
            },
            None,
        )
        .expect("ambiguous dispatch reaches a durable boundary");
        let DurableResponse::RunBoundary {
            boundary: DurableBoundary::ReconciliationRequired { intent_id },
        } = started
        else {
            panic!("dispatch failure must retain one reconciliation boundary")
        };
        let provider_invocations_before_store_only =
            fs::read_to_string(&invocations).expect("provider counter reads");

        UnknownEffectFixture {
            root: fixture,
            invocations,
            executable,
            run_id,
            store,
            intent_id,
            provider_invocations_before_store_only,
        }
    }

    #[cfg(unix)]
    fn cancel_unknown_effect(
        fixture: &UnknownEffectFixture,
    ) -> cymule_runtime::EngineDurableTarget {
        let store_only = EngineDurableTarget::query(fixture.store.clone());
        let cancel_request = EngineRequest::ExecuteDurable {
            target: store_only.clone(),
            command: DurableCommand::CancelRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                cancellation_id: "cancel:store-only-resolution".to_owned(),
                run_id: fixture.run_id.to_owned(),
                reason: serde_json::json!({"code": "operator_cancelled"}),
            },
        };
        let cancel_input = serde_json::to_vec(&EngineRequestEnvelope::new(cancel_request))
            .expect("cancellation request serializes");
        let (_, cancel_response) =
            decode_and_execute_request(&cancel_input).expect("store-only cancellation executes");
        let EngineResponse::DurableExecuted {
            response: cancellation @ DurableResponse::RunCancelled { .. },
        } = cancel_response
        else {
            panic!("Engine must return a typed RunCancelled response")
        };
        cancellation
            .verify()
            .expect("cancellation receipt verifies");
        let cancellation_wire = serde_json::to_value(EngineResponse::DurableExecuted {
            response: cancellation,
        })
        .expect("cancellation response serializes");
        assert_eq!(cancellation_wire["response"]["type"], "run_cancelled");
        assert_eq!(
            cancellation_wire["response"]["receipt"]["boundary"]["status"],
            "cancelled"
        );
        store_only
    }

    #[cfg(unix)]
    fn load_unknown_effect(
        store_only: &EngineDurableTarget,
        run_id: &str,
        intent_id: String,
    ) -> cymule_durable::EffectDispatch {
        let queried = execute_durable(store_only, effect_query(run_id, intent_id), None)
            .expect("cancelled Run queries without a provider");
        let DurableResponse::RunItem {
            item: Some(item), ..
        } = queried
        else {
            panic!("cancelled Run query must return its retained Effect")
        };
        let DurableRunItem::Effect { effect } = *item else {
            panic!("exact Effect query must return an Effect leaf")
        };
        assert_eq!(effect.state, OutboxState::Unknown);
        *effect
    }

    #[cfg(unix)]
    #[test]
    fn cancelled_unknown_effect_resolves_through_provider_only_engine_control() {
        let fixture = start_unknown_effect_fixture();
        let store_only = cancel_unknown_effect(&fixture);
        let UnknownEffectFixture {
            root,
            invocations,
            executable,
            run_id,
            store,
            intent_id,
            provider_invocations_before_store_only,
        } = fixture;

        let effect = load_unknown_effect(&store_only, run_id, intent_id.clone());
        let claim_owner = effect
            .claim_owner
            .clone()
            .expect("ambiguous dispatch retains claim owner");
        let resolve_request = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::resolve(
                store.clone(),
                process_target(executable.to_string_lossy().into_owned()),
            ),
            command: DurableCommand::ResolveEffect {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                resolution_id: "resolution:store-only".to_owned(),
                run_id: run_id.to_owned(),
                intent_id,
                execution_binding: effect.execution_binding.clone(),
                occurrence_binding: effect.occurrence_binding.clone(),
                claim_owner,
                claim_epoch: effect.claim_epoch,
                resolution: cymule_core::ReconciliationResolution::ResolvedNotApplied,
                value: None,
            },
        };
        let resolve_input = serde_json::to_vec(&EngineRequestEnvelope::new(resolve_request))
            .expect("resolution request serializes");
        let (_, resolve_response) =
            decode_and_execute_request(&resolve_input).expect("provider-only resolution executes");
        let EngineResponse::DurableExecuted {
            response: resolution @ DurableResponse::EffectResolved { .. },
        } = resolve_response
        else {
            panic!("Engine must return a typed EffectResolved response")
        };
        resolution.verify().expect("resolution receipt verifies");
        let resolution_wire = serde_json::to_value(EngineResponse::DurableExecuted {
            response: resolution,
        })
        .expect("resolution response serializes");
        assert_eq!(resolution_wire["response"]["type"], "effect_resolved");
        assert!(
            resolution_wire["response"]["receipt"]
                .get("world_settlement")
                .is_none(),
            "Effect resolution receipts do not duplicate mutable Run settlement"
        );
        let provider_invocations_after_resolution =
            fs::read_to_string(&invocations).expect("provider counter reads");
        fs::remove_file(&executable).expect("historical provider is removed after commit");
        let (_, replay_response) = decode_and_execute_request(&resolve_input)
            .expect("lost-ack retry replays without the removed provider");
        let replay_wire =
            serde_json::to_value(replay_response).expect("replayed resolution response serializes");
        assert_eq!(replay_wire, resolution_wire);
        assert_eq!(
            fs::read_to_string(&invocations).expect("provider counter reads"),
            provider_invocations_after_resolution,
            "exact replay must not construct or invoke the removed provider"
        );
        let settled = execute_durable(&store_only, run_current_query(run_id), None)
            .expect("settled cancelled Run queries");
        let DurableResponse::RunCurrent {
            current: Some(settled_run),
            ..
        } = settled
        else {
            panic!("settled cancelled Run remains queryable")
        };
        assert!(matches!(
            &settled_run.execution_status,
            cymule_core::RunExecutionStatus::Cancelled { .. }
        ));
        assert_eq!(
            settled_run.world_settlement,
            cymule_core::WorldSettlementStatus::Settled
        );
        assert_ne!(
            fs::read_to_string(&invocations).expect("provider counter reads"),
            provider_invocations_before_store_only,
            "terminal resolution must cross the exact historical provider"
        );

        fs::remove_dir_all(root).expect("fixture directory removes");
    }

    #[test]
    fn invalid_clock_scope_rejects_before_sqlite_clock_io() {
        for run_id in ["", "run:\u{0085}forged"] {
            let path = test_path("invalid-clock-scope").with_extension("sqlite");
            let target = cymule_runtime::EngineClockTarget::sqlite(
                path.to_string_lossy().into_owned(),
                "clock:scope-admission",
                format!("sha256:{}", "4".repeat(64)),
            );

            let failure = observe_clock(&target, run_id)
                .expect_err("invalid Run identity is rejected before Clock initialization");
            assert_eq!(failure.category, EngineFailureCategory::Validation);
            assert_eq!(failure.phase, EnginePhase::ObserveClock);
            assert_eq!(
                failure.retry_disposition,
                Some(EngineRetryDisposition::CorrectAndRetry)
            );
            assert_sqlite_files_absent(&path);
        }
    }

    #[test]
    fn evolution_plugin_failure_is_bounded_before_engine_projection() {
        EvolutionPluginFailure::Substrate {
            code: "provider_unavailable".to_owned(),
            message: "🧭".repeat(2000),
        }
        .verify()
        .expect("bounded multi-byte failure is valid");

        for failure in [
            EvolutionPluginFailure::Substrate {
                code: "INVALID".to_owned(),
                message: "invalid code".to_owned(),
            },
            EvolutionPluginFailure::Substrate {
                code: "2provider".to_owned(),
                message: "invalid leading digit".to_owned(),
            },
            EvolutionPluginFailure::Substrate {
                code: "_provider".to_owned(),
                message: "invalid leading underscore".to_owned(),
            },
            EvolutionPluginFailure::Substrate {
                code: format!("a{}", "b".repeat(200)),
                message: "overlong ASCII code".to_owned(),
            },
            EvolutionPluginFailure::Substrate {
                code: "provider_unavailable".to_owned(),
                message: String::new(),
            },
            EvolutionPluginFailure::Substrate {
                code: "provider_unavailable".to_owned(),
                message: "🧭".repeat(2001),
            },
        ] {
            assert!(matches!(
                failure.verify(),
                Err(EvolutionError::PluginDefect { .. })
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn production_evolution_process_enforces_the_fixed_bound_and_strict_decoder() {
        let response = r#"{"outcome":"failure","evolution_plugin_protocol":"cymule.evolution-plugin/3","error":{"category":"substrate","code":"provider_unavailable","message":"unavailable"}}"#;
        let script = |body: &str, total: usize| {
            assert!(body.len() <= total);
            assert!(!body.contains('\''));
            let padding = total - body.len();
            format!(
                "#!/bin/sh\n/bin/cat >/dev/null\nprintf '%s' '{body}'\n/bin/dd if=/dev/zero bs={padding} count=1 2>/dev/null | /usr/bin/tr '\\000' ' '\n"
            )
        };
        let limit = cymule_evolution::MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES;
        let (_exact_directory, exact) =
            process_evolution_provider("exact-response", &script(response, limit));
        assert!(matches!(
            exact.invoke(cymule_evolution::EvolutionPluginRequest::DescribeMigration {}),
            Err(EvolutionError::Substrate { code, .. }) if code == "provider_unavailable"
        ));

        let (_oversized_directory, oversized) =
            process_evolution_provider("oversized-response", &script(response, limit + 1));
        assert!(matches!(
            oversized.invoke(cymule_evolution::EvolutionPluginRequest::DescribeMigration {}),
            Err(EvolutionError::PluginDefect { code, .. })
                if code == "plugin_output_limit_exceeded"
        ));

        for (name, malformed) in [
            (
                "duplicate-response",
                r#"{"outcome":"failure","evolution_plugin_protocol":"cymule.evolution-plugin/3","evolution_plugin_protocol":"cymule.evolution-plugin/3","error":{"category":"substrate","code":"provider_unavailable","message":"unavailable"}}"#,
            ),
            (
                "unknown-response",
                r#"{"outcome":"failure","evolution_plugin_protocol":"cymule.evolution-plugin/3","error":{"category":"substrate","code":"provider_unavailable","message":"unavailable","unknown":true}}"#,
            ),
            (
                "unsafe-number-response",
                r#"{"outcome":"failure","evolution_plugin_protocol":"cymule.evolution-plugin/3","error":{"category":"substrate","code":"provider_unavailable","message":"unavailable","unsafe":9007199254740992}}"#,
            ),
        ] {
            let malformed_script =
                format!("#!/bin/sh\n/bin/cat >/dev/null\nprintf '%s' '{malformed}'\n");
            let (_directory, provider) = process_evolution_provider(name, &malformed_script);
            assert!(matches!(
                provider.invoke(cymule_evolution::EvolutionPluginRequest::DescribeMigration {}),
                Err(EvolutionError::Validation(_))
            ));
        }
    }

    #[test]
    fn resource_and_durable_commit_outcomes_map_only_to_reconciliation() {
        let resource = map_resource_error(&ResourceError::CommitOutcomeUnknown {
            message: "missing Resource commit receipt".to_owned(),
        });
        assert_eq!(
            resource.category,
            EngineFailureCategory::UnknownWorldOutcome
        );
        assert_eq!(resource.code.as_ref(), "unknown_world_outcome");
        assert!(resource.message.contains("missing Resource commit receipt"));
        assert_eq!(
            resource.retry_disposition,
            Some(EngineRetryDisposition::Reconcile)
        );

        let durable = map_durable_error(
            &DurableError::CommitOutcomeUnknown {
                message: "missing durable commit receipt".to_owned(),
            },
            EnginePhase::ExecuteDurable,
        );
        assert_eq!(durable.category, EngineFailureCategory::UnknownWorldOutcome);
        assert_eq!(durable.code.as_ref(), "durable_commit_outcome_unknown");
        assert_eq!(
            durable.retry_disposition,
            Some(EngineRetryDisposition::Reconcile)
        );
    }

    #[test]
    fn structured_profile_failures_preserve_exact_code_and_message() {
        let persistence = map_durable_error(
            &DurableError::Persistence {
                code: "evolution_receipt_write_failed".to_owned(),
                message: "the profile receipt could not be written".to_owned(),
            },
            EnginePhase::ExecuteLiveEvolution,
        );
        assert_eq!(
            persistence.category,
            EngineFailureCategory::SubstrateFailure
        );
        assert_eq!(persistence.code.as_ref(), "evolution_receipt_write_failed");
        assert_eq!(
            persistence.message.as_ref(),
            "the profile receipt could not be written"
        );
        assert_eq!(
            persistence.retry_disposition,
            Some(EngineRetryDisposition::RetrySameRequest)
        );

        let integrity = map_evolution_error(
            &EvolutionError::Integrity {
                code: "evolution_receipt_invalid".to_owned(),
                message: "the retained receipt is malformed".to_owned(),
            },
            EnginePhase::ExecuteLiveEvolution,
        );
        assert_eq!(integrity.category, EngineFailureCategory::ContractViolation);
        assert_eq!(integrity.code.as_ref(), "evolution_receipt_invalid");
        assert_eq!(
            integrity.message.as_ref(),
            "the retained receipt is malformed"
        );
        assert_eq!(
            integrity.retry_disposition,
            Some(EngineRetryDisposition::Never)
        );
    }

    #[test]
    fn archived_durable_command_replay_requires_archive_aware_correction() {
        let failure = map_durable_error(
            &DurableError::ArchivedCommandReplayRequired {
                command_id: "command:archived".to_owned(),
                archive_head:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                command_index_root:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
            },
            EnginePhase::ExecuteDurable,
        );
        assert_eq!(failure.category, EngineFailureCategory::AdmissionDenied);
        assert_eq!(failure.code.as_ref(), "archived_command_replay_required");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(failure.message.contains("command:archived"));
        assert!(failure.message.contains("sha256:aaaaaaaa"));
        failure
            .verify()
            .expect("archived durable replay projects as a valid Engine admission failure");
    }

    #[test]
    fn paged_durable_scope_requires_a_corrected_command_without_same_request_retry() {
        let failure = map_durable_error(
            &DurableError::PagedScopeRequired {
                run_id: "run:paged-scope".to_owned(),
                scope_id: "scope:paged-scope".to_owned(),
                entries: 4_097,
            },
            EnginePhase::ExecuteDurable,
        );
        assert_eq!(failure.category, EngineFailureCategory::AdmissionDenied);
        assert_eq!(failure.phase, EnginePhase::ExecuteDurable);
        assert_eq!(failure.code.as_ref(), "paged_scope_required");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(failure.message.contains("run:paged-scope"));
        assert!(failure.message.contains("scope:paged-scope"));
        failure
            .verify()
            .expect("paged scope remains a typed Engine admission boundary");
    }

    #[test]
    fn evolution_preserves_paged_scope_and_typed_collection_provider_failures() {
        use cymule_authenticated_collections::{ProviderConflict, ProviderFailure};
        let paged = map_evolution_error(
            &EvolutionError::PagedScopeRequired {
                run_id: "run:evolution-paged".to_owned(),
                scope_id: "scope:root".to_owned(),
                entries: 4_097,
            },
            EnginePhase::ExecuteLiveEvolution,
        );
        assert_eq!(paged.category, EngineFailureCategory::AdmissionDenied);
        assert_eq!(paged.code.as_ref(), "paged_scope_required");
        assert_eq!(
            paged.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        paged.verify().expect("paged Evolution admission is valid");
        for provider in [
            ProviderFailure::Validation {
                message: "bad provider input".to_owned(),
            },
            ProviderFailure::Integrity {
                code: "provider_corrupt".to_owned(),
                message: "bad bytes".to_owned(),
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::Revision {
                    expected: Some("old".to_owned()),
                    current: Some("new".to_owned()),
                },
            },
            ProviderFailure::Conflict {
                evidence: ProviderConflict::History {
                    code: "provider_reuse".to_owned(),
                    message: "different history".to_owned(),
                },
            },
            ProviderFailure::Substrate {
                code: "provider_io".to_owned(),
                message: "read failed".to_owned(),
            },
        ] {
            let expected = EngineFailure::from_core(
                &cymule_core::CoreError::CollectionProviderFailure(provider.clone()),
                EnginePhase::ExecuteLiveEvolution,
            );
            let actual = map_evolution_error(
                &EvolutionError::CollectionProviderFailure(provider),
                EnginePhase::ExecuteLiveEvolution,
            );
            assert_eq!(actual, expected);
            actual
                .verify()
                .expect("Evolution uses the same typed provider projection");
        }
    }

    #[test]
    fn durable_runtime_cancellation_remains_typed_through_engine_projection() {
        let durable = DurableError::from(cymule_runtime::RuntimeError::cancelled(
            "process_invocation_cancelled",
            "the owning Engine cancelled work",
        ));
        assert!(matches!(durable, DurableError::Cancelled { .. }));

        let failure = map_durable_error(&durable, EnginePhase::ExecuteDurable);
        assert_eq!(failure.category, EngineFailureCategory::Cancelled);
        assert_eq!(failure.code.as_ref(), "process_invocation_cancelled");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::Never)
        );
        failure
            .verify()
            .expect("typed cancellation is a valid Engine failure");
    }

    #[test]
    fn durable_timeout_recovery_distinguishes_preflight_from_persisted_attempt() {
        let timeout = DurableError::TimedOut {
            code: "process_response_timed_out".to_owned(),
            message: "provider response deadline elapsed".to_owned(),
        };

        let preflight = super::map_durable_pre_execution_error(&timeout);
        assert_eq!(preflight.category, EngineFailureCategory::TimedOut);
        assert_eq!(
            preflight.retry_disposition,
            Some(EngineRetryDisposition::RetrySameRequest)
        );
        preflight
            .verify()
            .expect("preflight timeout permits the identical request");

        let persisted = map_durable_error(&timeout, EnginePhase::ExecuteDurable);
        assert_eq!(persisted.category, EngineFailureCategory::TimedOut);
        assert_eq!(
            persisted.retry_disposition,
            Some(EngineRetryDisposition::RefreshAndRetry)
        );
        persisted
            .verify()
            .expect("persisted attempt timeout requires refreshed takeover authority");

        let interrupted_component = DurableError::TimedOut {
            code: "component_invocation_interrupted".to_owned(),
            message: "provider connection closed after Attempt admission".to_owned(),
        };
        let interrupted = map_durable_error(&interrupted_component, EnginePhase::ExecuteDurable);
        assert_eq!(interrupted.category, EngineFailureCategory::TimedOut);
        assert_eq!(
            interrupted.retry_disposition,
            Some(EngineRetryDisposition::RefreshAndRetry)
        );
    }

    #[test]
    fn core_errors_keep_their_durable_recovery_class() {
        let cases = [
            (
                cymule_core::CoreError::Validation("invalid request".to_owned()),
                EngineFailureCategory::Validation,
                Some(EngineRetryDisposition::CorrectAndRetry),
            ),
            (
                cymule_core::CoreError::IllegalTransition("illegal edge".to_owned()),
                EngineFailureCategory::AdmissionDenied,
                Some(EngineRetryDisposition::CorrectAndRetry),
            ),
            (
                cymule_core::CoreError::IdentityMismatch("corrupt identity".to_owned()),
                EngineFailureCategory::ContractViolation,
                Some(EngineRetryDisposition::Never),
            ),
            (
                cymule_core::CoreError::Causal("missing parent".to_owned()),
                EngineFailureCategory::ContractViolation,
                Some(EngineRetryDisposition::Never),
            ),
            (
                cymule_core::CoreError::CommandReuse("different semantics".to_owned()),
                EngineFailureCategory::Conflict,
                Some(EngineRetryDisposition::Never),
            ),
        ];

        for (core, category, retry) in cases {
            let durable = DurableError::from(core);
            let failure = map_durable_error(&durable, EnginePhase::ExecuteDurable);
            assert_eq!(failure.category, category);
            assert_eq!(failure.retry_disposition, retry);
            failure.verify().expect("typed durable projection is valid");
        }
    }

    #[test]
    fn durable_queries_leave_directory_lock_and_staging_residue_untouched() {
        let path = test_path("read-only-directory");
        let control = DurableStoreControl::initialize(
            DirectoryStore::open(&path).expect("directory store opens"),
        )
        .expect("store-only domain initializes");
        drop(control);
        fs::remove_file(path.join("head.lock")).expect("writer lock residue removes");
        fs::remove_file(path.join("objects.lock")).expect("object lock residue removes");
        let head_residue = path.join("head.next");
        let object_residue = path.join("state-root-objects").join("cli-query.next");
        fs::write(&head_residue, b"head residue").expect("head residue writes");
        fs::write(&object_residue, b"object residue").expect("object residue writes");
        let target = EngineDurableTarget::query(EngineStoreTarget::directory(
            path.to_string_lossy().into_owned(),
        ));

        let domain =
            execute_durable(&target, run_index_query(), None).expect("Run-index query succeeds");
        assert!(matches!(domain, DurableResponse::RunIndexPage { .. }));
        let run = execute_durable(&target, run_current_query("run:not-present"), None)
            .expect("Run-current query succeeds");
        assert!(matches!(
            run,
            DurableResponse::RunCurrent { current: None, .. }
        ));

        assert_eq!(
            fs::read(&head_residue).expect("head residue remains"),
            b"head residue"
        );
        assert_eq!(
            fs::read(&object_residue).expect("object residue remains"),
            b"object residue"
        );
        assert!(!path.join("head.lock").exists());
        assert!(!path.join("objects.lock").exists());
        fs::remove_dir_all(path).expect("test directory removes");
    }

    #[test]
    fn durable_queries_leave_missing_store_targets_absent() {
        let directory_path = test_path("missing-directory");
        let sqlite_path = test_path("missing-sqlite").with_extension("sqlite");
        let commands = [run_index_query(), run_current_query("run:missing")];
        for command in commands.iter().cloned() {
            let target = EngineDurableTarget::query(EngineStoreTarget::directory(
                directory_path.to_string_lossy().into_owned(),
            ));
            execute_durable(&target, command, None)
                .expect_err("missing directory query fails without initialization");
            assert!(!directory_path.exists());
        }
        for command in commands {
            let target = EngineDurableTarget::query(EngineStoreTarget::sqlite(
                sqlite_path.to_string_lossy().into_owned(),
                "domain:missing",
            ));
            execute_durable(&target, command, None)
                .expect_err("missing SQLite query fails without initialization");
            assert!(!sqlite_path.exists());
            assert!(!PathBuf::from(format!("{}-wal", sqlite_path.display())).exists());
            assert!(!PathBuf::from(format!("{}-shm", sqlite_path.display())).exists());
        }
    }
}
