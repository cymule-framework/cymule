//! Command-line and JSON RPC transport for the Cymule engine.

use std::env;
use std::fs;
use std::io::{self, Read};
use std::path::Path;

use cymule_core::{PlanCandidate, SealedPlan, decode_json, seal_plan};
use cymule_directory_store::DirectoryStore;
use cymule_durable::{
    DurableCommand, DurableCoordinator, DurableResponse, DurableRuntimeControl, ResumableRuntime,
    WaitActivation,
};
use cymule_evolution::{
    DurableLiveEvolutionController, EvolutionCommand, EvolutionError, EvolutionResult,
    LiveEvolutionCommand, LiveEvolutionResponse, MigrationAdapter, MigrationAdapterDescriptor,
    MigrationOutput, MigrationRequest, ShadowDriver, ShadowDriverDescriptor, ShadowOutput,
    ShadowRequest,
};
use cymule_executor_process::{ProcessExecutor, ProcessExecutorConfig};
use cymule_resource::{ResourceCandidate, ResourceHandle};
use cymule_runtime::{
    ENGINE_PROTOCOL_VERSION, EmbeddedRuntime, EngineContractSide, EngineFailure,
    EngineFailureCategory, EngineIssue, EnginePhase, EngineRequestEnvelope, EngineResponseEnvelope,
    EngineRetryDisposition, ExecutionBinding, ExecutionOutcome, PluginHost, verify_plan,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
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
    VerifyEvolutionCommand {
        command: EvolutionCommand,
    },
    VerifyLiveEvolutionCommand {
        command: LiveEvolutionCommand,
    },
    ExecuteDurable {
        store: String,
        plugin: String,
        command: DurableCommand,
    },
    ExecuteLiveEvolution {
        store: String,
        journal_id: String,
        command: LiveEvolutionCommand,
    },
    Run {
        plan: SealedPlan,
        input: Value,
        plugin: String,
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
    VerifiedEvolutionCommand { command: EvolutionCommand },
    VerifiedLiveEvolutionCommand { command: LiveEvolutionCommand },
    ExecutionBoundary { execution: ExecutionOutcome },
    DurableExecuted { response: DurableResponse },
    LiveEvolutionExecuted { response: LiveEvolutionResponse },
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
            let plugin = argument_value(&arguments, "--plugin")?;
            let run_id = argument_value(&arguments, "--run-id")?;
            let mut runtime = local_process_runtime(plugin)?;
            let result = runtime.execute(plan, &input, run_id)?;
            print_json(&result)
        }
        _ => Err(
            "usage: cymule <rpc|seal|verify|run|resource seal|wait-activation verify|durable-command verify|evolution-command verify|live-evolution-command verify> [options]"
                .into(),
        ),
    }
}

fn rpc() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = Vec::new();
    let response = match io::stdin().read_to_end(&mut input) {
        Ok(_) => decode_and_execute_request(&input),
        Err(error) => Err(EngineFailure::transport(
            "engine_read_failed",
            error.to_string(),
        )),
    };
    print_json(&match response {
        Ok(response) => EngineResponseEnvelope::success(response),
        Err(error) => EngineResponseEnvelope::<EngineResponse>::failure(error),
    })
}

fn decode_and_execute_request(input: &[u8]) -> Result<EngineResponse, EngineFailure> {
    let envelope: EngineRequestEnvelope<EngineRequest> = decode_json(input).map_err(|error| {
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
    let response = match envelope.request {
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
            activation
                .verify()
                .map_err(|error| map_durable_error(&error, EnginePhase::VerifyWaitActivation))?;
            EngineResponse::VerifiedWaitActivation { activation }
        }
        EngineRequest::VerifyDurableCommand { command } => {
            command
                .verify()
                .map_err(|error| map_durable_error(&error, EnginePhase::VerifyDurableCommand))?;
            EngineResponse::VerifiedDurableCommand { command }
        }
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
        EngineRequest::ExecuteDurable {
            store,
            plugin,
            command,
        } => EngineResponse::DurableExecuted {
            response: execute_durable(&store, &plugin, command)?,
        },
        EngineRequest::ExecuteLiveEvolution {
            store,
            journal_id,
            command,
        } => EngineResponse::LiveEvolutionExecuted {
            response: execute_live_evolution(&store, &journal_id, command)?,
        },
        EngineRequest::Run {
            plan,
            input,
            plugin,
            run_id,
        } => {
            let mut runtime = local_process_runtime(&plugin)
                .map_err(|error| EngineFailure::from_runtime(error, EnginePhase::ExecutePlan))?;
            EngineResponse::ExecutionBoundary {
                execution: runtime.execute(plan, &input, run_id).map_err(|error| {
                    EngineFailure::from_runtime(error, EnginePhase::ExecutePlan)
                })?,
            }
        }
    };
    Ok(response)
}

fn execute_durable(
    store: &str,
    plugin: &str,
    command: DurableCommand,
) -> Result<DurableResponse, EngineFailure> {
    let runtime = local_durable_runtime(store, plugin)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
    DurableRuntimeControl::new(runtime)
        .submit(command)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))
}

fn local_durable_runtime(
    store: &str,
    executable: &str,
) -> cymule_durable::DurableResult<ResumableRuntime<DirectoryStore, ProcessExecutor>> {
    let store = DirectoryStore::open(store)?;
    let mut plugin = ProcessExecutor::new(ProcessExecutorConfig::new(executable))
        .map_err(|error| cymule_durable::DurableError::Substrate(error.to_string()))?;
    let implementation_revision = plugin.implementation_revision().to_owned();
    let manifest = plugin
        .describe()
        .map_err(|error| cymule_durable::DurableError::Substrate(error.to_string()))?;
    let binding = ExecutionBinding::for_local_process(&manifest, implementation_revision)
        .map_err(cymule_durable::DurableError::from)?;
    ResumableRuntime::open(store, plugin, binding)
}

fn execute_live_evolution(
    store: &str,
    journal_id: &str,
    command: LiveEvolutionCommand,
) -> Result<LiveEvolutionResponse, EngineFailure> {
    let store = DirectoryStore::open(store)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let mut coordinator = DurableCoordinator::open(store)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let mut controller = DurableLiveEvolutionController::load(&coordinator, journal_id)
        .map_err(|error| map_evolution_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let mut unsupported = UnsupportedEvolutionPlugin;
    DurableLiveEvolutionController::submit(
        &mut coordinator,
        &mut controller,
        journal_id,
        command,
        &mut unsupported,
        &mut UnsupportedEvolutionPlugin,
    )
    .map_err(|error| map_evolution_error(&error, EnginePhase::ExecuteLiveEvolution))
}

struct UnsupportedEvolutionPlugin;

impl MigrationAdapter for UnsupportedEvolutionPlugin {
    fn describe(&mut self) -> EvolutionResult<MigrationAdapterDescriptor> {
        Err(EvolutionError::Validation(
            "the local CLI transport has no migration adapter binding".to_owned(),
        ))
    }

    fn migrate(&mut self, _request: &MigrationRequest) -> EvolutionResult<MigrationOutput> {
        Err(EvolutionError::Validation(
            "the local CLI transport has no migration adapter binding".to_owned(),
        ))
    }
}

impl ShadowDriver for UnsupportedEvolutionPlugin {
    fn describe(&mut self) -> EvolutionResult<ShadowDriverDescriptor> {
        Err(EvolutionError::Validation(
            "the local CLI transport has no shadow driver binding".to_owned(),
        ))
    }

    fn execute(&mut self, _request: &ShadowRequest) -> EvolutionResult<ShadowOutput> {
        Err(EvolutionError::Validation(
            "the local CLI transport has no shadow driver binding".to_owned(),
        ))
    }
}

fn local_process_runtime(
    executable: impl AsRef<Path>,
) -> cymule_runtime::RuntimeResult<EmbeddedRuntime<ProcessExecutor>> {
    let mut plugin = ProcessExecutor::new(ProcessExecutorConfig::new(executable))?;
    let implementation_revision = plugin.implementation_revision().to_owned();
    let manifest = plugin.describe()?;
    let binding = ExecutionBinding::for_local_process(&manifest, implementation_revision)?;
    EmbeddedRuntime::new(plugin, binding)
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
        ResourceError::Conflict(_) => (
            EngineFailureCategory::Conflict,
            "resource_conflict",
            Some(EngineRetryDisposition::Never),
        ),
        ResourceError::NotFound(_) => (EngineFailureCategory::NotFound, "resource_not_found", None),
        ResourceError::Substrate(_) | ResourceError::Persistence(_) => (
            EngineFailureCategory::SubstrateFailure,
            "resource_substrate_failed",
            Some(EngineRetryDisposition::RetrySameRequest),
        ),
        ResourceError::Integrity(_) => (
            EngineFailureCategory::ContractViolation,
            "resource_integrity_failed",
            Some(EngineRetryDisposition::Never),
        ),
        ResourceError::Schema(_) => unreachable!("schema errors return above"),
    };
    let mut failure =
        EngineFailure::new(category, EnginePhase::SealResource, code, error.to_string());
    failure.retry_disposition = retry;
    failure
}

fn map_durable_error(error: &cymule_durable::DurableError, phase: EnginePhase) -> EngineFailure {
    use cymule_durable::DurableError;

    let (category, code, retry) = match &error {
        DurableError::Contract(error) => {
            return EngineFailure::from_contract_violation(error, phase);
        }
        DurableError::Validation(_) | DurableError::Encoding(_) => (
            EngineFailureCategory::Validation,
            "durable_command_validation_failed",
            Some(EngineRetryDisposition::CorrectAndRetry),
        ),
        DurableError::Conflict { .. } => (
            EngineFailureCategory::Conflict,
            "durable_revision_conflict",
            Some(EngineRetryDisposition::RefreshAndRetry),
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
        DurableError::Substrate(_) => (
            EngineFailureCategory::SubstrateFailure,
            "durable_substrate_failed",
            Some(EngineRetryDisposition::RetrySameRequest),
        ),
    };
    let mut failure = EngineFailure::new(category, phase, code, error.to_string());
    failure.retry_disposition = retry;
    failure
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
        EvolutionError::Conflict(_) => (
            EngineFailureCategory::Conflict,
            "evolution_conflict",
            Some(EngineRetryDisposition::Never),
        ),
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
    Ok(decode_json(&bytes)?)
}

fn print_json<T: Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::decode_and_execute_request;

    #[test]
    fn rpc_rejects_nested_duplicate_plan_members_before_typed_decode() {
        let error = decode_and_execute_request(
            br#"{"engine_protocol":"cymule.engine/2","request":{"type":"seal","candidate":{"ir_version":"cymule.ir/2","ir_version":"changed"}}}"#,
        )
        .expect_err("duplicate Plan member is rejected");
        assert_eq!(error.code.as_ref(), "invalid_engine_request");
        assert!(error.message.contains("duplicate JSON object member"));
    }
}
