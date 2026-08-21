//! Command-line and JSON RPC transport for the Cymule engine.

use std::env;
use std::fs;
use std::io::{self, Read};
use std::path::Path;

use cymule_core::{PlanCandidate, SealedPlan, decode_json, seal_plan};
use cymule_directory_store::DirectoryStore;
use cymule_durable::{
    DurableCommand, DurableCoordinator, DurableQueryControl, DurableResponse,
    DurableRuntimeControl, DurableStore, GcReceipt, ResumableRuntime, StoreBatch, StoreCommit,
    StoreHead, StoreStats, StoredState, WaitActivation,
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
    ENGINE_DIRECTORY_STORE_PROVIDER, ENGINE_PROCESS_EXECUTOR_PROVIDER, ENGINE_PROTOCOL_VERSION,
    ENGINE_SQLITE_STORE_PROVIDER, EVOLUTION_PLUGIN_PROTOCOL_VERSION, EmbeddedRuntime,
    EngineContractSide, EngineDurableTarget, EngineEvolutionTarget, EngineFailure,
    EngineFailureCategory, EngineIssue, EnginePhase, EnginePluginTarget, EngineRequestEnvelope,
    EngineResponseEnvelope, EngineRetryDisposition, ExecutionBinding, ExecutionOutcome, PluginHost,
    validate_strict_json, verify_plan,
};
use cymule_store_sqlite::SqliteStore;
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
        target: EngineDurableTarget,
        command: DurableCommand,
    },
    ExecuteLiveEvolution {
        target: EngineEvolutionTarget,
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
    validate_strict_json(input).map_err(|error| {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::DecodeRequest,
            "invalid_engine_request",
            error,
        );
        failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        failure
    })?;
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
        EngineRequest::ExecuteDurable { target, command } => EngineResponse::DurableExecuted {
            response: execute_durable(&target, command)?,
        },
        EngineRequest::ExecuteLiveEvolution {
            target,
            journal_id,
            command,
        } => EngineResponse::LiveEvolutionExecuted {
            response: execute_live_evolution(&target, &journal_id, command)?,
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
    target: &EngineDurableTarget,
    command: DurableCommand,
) -> Result<DurableResponse, EngineFailure> {
    target.verify()?;
    command
        .verify()
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
    let store = open_store(&target.store)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
    if command.is_query() {
        return DurableQueryControl::open(store)
            .and_then(|mut control| control.submit(command))
            .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable));
    }
    let executor = target.executor.as_ref().ok_or_else(|| {
        EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ExecuteDurable,
            "missing_execution_provider",
            "durable mutation requires an execution provider",
        )
    })?;
    let runtime = local_durable_runtime(store, executor)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))?;
    DurableRuntimeControl::new(runtime)
        .submit(command)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteDurable))
}

fn local_durable_runtime(
    store: CliStore,
    target: &EnginePluginTarget,
) -> cymule_durable::DurableResult<ResumableRuntime<CliStore, ProcessExecutor>> {
    if target.provider != ENGINE_PROCESS_EXECUTOR_PROVIDER {
        return Err(cymule_durable::DurableError::Validation(format!(
            "unsupported execution provider {}",
            target.provider
        )));
    }
    let mut plugin = ProcessExecutor::new(ProcessExecutorConfig::new(&target.location))
        .map_err(|error| cymule_durable::DurableError::Substrate(error.to_string()))?;
    if target
        .revision
        .as_deref()
        .is_some_and(|expected| expected != plugin.implementation_revision())
    {
        return Err(cymule_durable::DurableError::Validation(
            "execution provider revision does not match the sealed executable bytes".to_owned(),
        ));
    }
    let implementation_revision = plugin.implementation_revision().to_owned();
    let manifest = plugin
        .describe()
        .map_err(|error| cymule_durable::DurableError::Substrate(error.to_string()))?;
    let binding = ExecutionBinding::for_local_process(&manifest, implementation_revision)
        .map_err(cymule_durable::DurableError::from)?;
    ResumableRuntime::open(store, plugin, binding)
}

fn execute_live_evolution(
    target: &EngineEvolutionTarget,
    journal_id: &str,
    command: LiveEvolutionCommand,
) -> Result<LiveEvolutionResponse, EngineFailure> {
    target.verify()?;
    let store = open_store(&target.store)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let mut coordinator = DurableCoordinator::open(store)
        .map_err(|error| map_durable_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let mut controller = DurableLiveEvolutionController::load(&coordinator, journal_id)
        .map_err(|error| map_evolution_error(&error, EnginePhase::ExecuteLiveEvolution))?;
    let mut migration = ProcessEvolutionPlugin::optional(target.migration.as_ref())?;
    let mut shadow = ProcessEvolutionPlugin::optional(target.shadow.as_ref())?;
    DurableLiveEvolutionController::submit(
        &mut coordinator,
        &mut controller,
        journal_id,
        command,
        &mut migration,
        &mut shadow,
    )
    .map_err(|error| map_evolution_error(&error, EnginePhase::ExecuteLiveEvolution))
}

enum CliStore {
    Directory(DirectoryStore),
    Sqlite(SqliteStore),
}

impl DurableStore for CliStore {
    fn load(&mut self) -> cymule_durable::DurableResult<Option<StoredState>> {
        match self {
            Self::Directory(store) => store.load(),
            Self::Sqlite(store) => store.load(),
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

    fn reclaim_cold(&mut self, expected: &StoreHead) -> cymule_durable::DurableResult<GcReceipt> {
        match self {
            Self::Directory(store) => store.reclaim_cold(expected),
            Self::Sqlite(store) => store.reclaim_cold(expected),
        }
    }

    fn stats(&self) -> cymule_durable::DurableResult<StoreStats> {
        match self {
            Self::Directory(store) => store.stats(),
            Self::Sqlite(store) => store.stats(),
        }
    }
}

fn open_store(
    target: &cymule_runtime::EngineStoreTarget,
) -> cymule_durable::DurableResult<CliStore> {
    match target.provider.as_str() {
        ENGINE_DIRECTORY_STORE_PROVIDER if target.domain.is_none() => {
            DirectoryStore::open(&target.location).map(CliStore::Directory)
        }
        ENGINE_SQLITE_STORE_PROVIDER => {
            let domain = target.domain.as_ref().ok_or_else(|| {
                cymule_durable::DurableError::Validation(
                    "SQLite store target requires a domain".to_owned(),
                )
            })?;
            SqliteStore::open(&target.location, domain).map(CliStore::Sqlite)
        }
        ENGINE_DIRECTORY_STORE_PROVIDER => Err(cymule_durable::DurableError::Validation(
            "directory store target must not contain a domain".to_owned(),
        )),
        provider => Err(cymule_durable::DurableError::Validation(format!(
            "the CLI Engine does not provide store {provider}; select a custom Engine transport"
        ))),
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EvolutionPluginRequest<'a> {
    DescribeMigration,
    Migrate { request: &'a MigrationRequest },
    DescribeShadow,
    ExecuteShadow { request: &'a ShadowRequest },
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct EvolutionPluginRequestEnvelope<'a> {
    evolution_plugin_protocol: &'static str,
    implementation_revision: &'a str,
    request: EvolutionPluginRequest<'a>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum EvolutionPluginResponseEnvelope {
    Success {
        evolution_plugin_protocol: String,
        response: EvolutionPluginResponse,
    },
    Failure {
        evolution_plugin_protocol: String,
        error: EvolutionPluginFailure,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EvolutionPluginResponse {
    MigrationDescriptor {
        descriptor: MigrationAdapterDescriptor,
    },
    Migrated {
        output: MigrationOutput,
    },
    ShadowDescriptor {
        descriptor: ShadowDriverDescriptor,
    },
    ShadowExecuted {
        output: ShadowOutput,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionPluginFailure {
    code: String,
    message: String,
}

struct ProcessEvolutionPlugin {
    process: Option<ProcessExecutor>,
}

impl ProcessEvolutionPlugin {
    fn optional(target: Option<&EnginePluginTarget>) -> Result<Self, EngineFailure> {
        let Some(target) = target else {
            return Ok(Self { process: None });
        };
        if target.provider != ENGINE_PROCESS_EXECUTOR_PROVIDER {
            return Err(EngineFailure::new(
                EngineFailureCategory::Validation,
                EnginePhase::ExecuteLiveEvolution,
                "unsupported_evolution_plugin_provider",
                format!("unsupported evolution plugin provider {}", target.provider),
            ));
        }
        let process = ProcessExecutor::new(ProcessExecutorConfig::new(&target.location)).map_err(
            |error| EngineFailure::from_runtime(error, EnginePhase::ExecuteLiveEvolution),
        )?;
        if target.revision.as_deref() != Some(process.implementation_revision()) {
            return Err(EngineFailure::new(
                EngineFailureCategory::AdmissionDenied,
                EnginePhase::ExecuteLiveEvolution,
                "evolution_plugin_revision_mismatch",
                "evolution plugin revision does not match the sealed executable bytes",
            ));
        }
        Ok(Self {
            process: Some(process),
        })
    }

    fn invoke(
        &self,
        request: EvolutionPluginRequest<'_>,
    ) -> EvolutionResult<EvolutionPluginResponse> {
        let process = self.process.as_ref().ok_or_else(|| {
            EvolutionError::Validation(
                "this evolution operation requires a pinned plugin".to_owned(),
            )
        })?;
        let envelope: EvolutionPluginResponseEnvelope = process
            .invoke_json(&EvolutionPluginRequestEnvelope {
                evolution_plugin_protocol: EVOLUTION_PLUGIN_PROTOCOL_VERSION,
                implementation_revision: process.implementation_revision(),
                request,
            })
            .map_err(|error| EvolutionError::Substrate(error.to_string()))?;
        match envelope {
            EvolutionPluginResponseEnvelope::Success {
                evolution_plugin_protocol,
                response,
            } => {
                if evolution_plugin_protocol != EVOLUTION_PLUGIN_PROTOCOL_VERSION {
                    return Err(EvolutionError::Validation(
                        "evolution plugin returned an unsupported protocol version".to_owned(),
                    ));
                }
                Ok(response)
            }
            EvolutionPluginResponseEnvelope::Failure {
                evolution_plugin_protocol,
                error,
            } => {
                if evolution_plugin_protocol != EVOLUTION_PLUGIN_PROTOCOL_VERSION {
                    return Err(EvolutionError::Validation(
                        "evolution plugin returned an unsupported protocol version".to_owned(),
                    ));
                }
                Err(EvolutionError::Substrate(format!(
                    "{}: {}",
                    error.code, error.message
                )))
            }
        }
    }

    fn sealed_revision(&self) -> EvolutionResult<&str> {
        self.process
            .as_ref()
            .map(ProcessExecutor::implementation_revision)
            .ok_or_else(|| {
                EvolutionError::Validation(
                    "this evolution operation requires a pinned plugin".to_owned(),
                )
            })
    }
}

impl MigrationAdapter for ProcessEvolutionPlugin {
    fn describe(&mut self) -> EvolutionResult<MigrationAdapterDescriptor> {
        match self.invoke(EvolutionPluginRequest::DescribeMigration)? {
            EvolutionPluginResponse::MigrationDescriptor { descriptor }
                if descriptor.adapter_revision == self.sealed_revision()? =>
            {
                Ok(descriptor)
            }
            EvolutionPluginResponse::MigrationDescriptor { .. } => Err(EvolutionError::Conflict(
                "migration descriptor revision does not match the sealed plugin".to_owned(),
            )),
            _ => Err(EvolutionError::Validation(
                "evolution plugin returned the wrong migration response variant".to_owned(),
            )),
        }
    }

    fn migrate(&mut self, request: &MigrationRequest) -> EvolutionResult<MigrationOutput> {
        match self.invoke(EvolutionPluginRequest::Migrate { request })? {
            EvolutionPluginResponse::Migrated { output } => Ok(output),
            _ => Err(EvolutionError::Validation(
                "evolution plugin returned the wrong migration response variant".to_owned(),
            )),
        }
    }
}

impl ShadowDriver for ProcessEvolutionPlugin {
    fn describe(&mut self) -> EvolutionResult<ShadowDriverDescriptor> {
        match self.invoke(EvolutionPluginRequest::DescribeShadow)? {
            EvolutionPluginResponse::ShadowDescriptor { descriptor }
                if descriptor.driver_revision == self.sealed_revision()? =>
            {
                Ok(descriptor)
            }
            EvolutionPluginResponse::ShadowDescriptor { .. } => Err(EvolutionError::Conflict(
                "shadow descriptor revision does not match the sealed plugin".to_owned(),
            )),
            _ => Err(EvolutionError::Validation(
                "evolution plugin returned the wrong shadow response variant".to_owned(),
            )),
        }
    }

    fn execute(&mut self, request: &ShadowRequest) -> EvolutionResult<ShadowOutput> {
        match self.invoke(EvolutionPluginRequest::ExecuteShadow { request })? {
            EvolutionPluginResponse::ShadowExecuted { output } => Ok(output),
            _ => Err(EvolutionError::Validation(
                "evolution plugin returned the wrong shadow response variant".to_owned(),
            )),
        }
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
        EvolutionError::Substrate(_) => (
            EngineFailureCategory::SubstrateFailure,
            "evolution_plugin_substrate_failed",
            Some(EngineRetryDisposition::RetrySameRequest),
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
    validate_strict_json(&bytes)?;
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

    #[test]
    fn rpc_rejects_integers_outside_the_shared_sdk_domain() {
        let error = decode_and_execute_request(
            br#"{"engine_protocol":"cymule.engine/2","request":{"type":"verify_durable_command","command":{"type":"activate_wait","control_version":"cymule.durable-control/1","activation_id":"activation:test","source":{"source":"signal","key":"signal:test"},"wait_ids":["wait:test"],"value":9007199254740992}}}"#,
        )
        .expect_err("unsafe integer is rejected before typed decode");
        assert_eq!(error.code.as_ref(), "invalid_engine_request");
        assert!(error.message.contains("shared JSON range"));
    }
}
