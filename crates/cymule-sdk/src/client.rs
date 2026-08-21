use std::collections::BTreeSet;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use cymule_core::{PlanCandidate, SealedPlan, decode_json};
use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableCommand, DurableResponse, DurableRunView, WaitActivation,
    WaitActivationSource,
};
use cymule_evolution::{EvolutionCommand, LiveEvolutionCommand, LiveEvolutionResponse};
use cymule_resource::{ResourceCandidate, ResourceHandle};
use cymule_runtime::{
    EngineDurableTarget, EngineEvolutionTarget, EngineFailure, EngineFailureCategory, EnginePhase,
    EnginePluginTarget, EngineRequestEnvelope, EngineResponseEnvelope, EngineResult,
    EngineStoreTarget, ExecutionOutcome, validate_strict_json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Engine operations shared by SDK transports.
pub trait Engine {
    /// Validate and seal a candidate with the trusted Rust kernel.
    fn seal(&self, candidate: &PlanCandidate) -> EngineResult<SealedPlan>;
    /// Validate and seal a provider-neutral Resource Candidate.
    fn seal_resource(&self, candidate: &ResourceCandidate) -> EngineResult<ResourceHandle>;
    /// Validate a provider-neutral signal or timer activation record.
    fn verify_wait_activation(&self, activation: &WaitActivation) -> EngineResult<WaitActivation>;
    /// Validate one closed, versioned M1 control envelope.
    fn verify_durable_command(&self, command: &DurableCommand) -> EngineResult<DurableCommand>;
    /// Validate one closed, versioned M4 control envelope.
    fn verify_evolution_command(
        &self,
        command: &EvolutionCommand,
    ) -> EngineResult<EvolutionCommand>;
    /// Validate one complete unified live-evolution control envelope.
    fn verify_live_evolution_command(
        &self,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<LiveEvolutionCommand>;
    /// Submit one stateful command to a durable Rust authority.
    fn execute_durable(
        &self,
        target: &EngineDurableTarget,
        command: &DurableCommand,
    ) -> EngineResult<DurableResponse>;
    /// Submit one stateful live-evolution command to its durable journal.
    fn execute_live_evolution(
        &self,
        target: &EngineEvolutionTarget,
        journal_id: &str,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<LiveEvolutionResponse>;
    /// Execute a sealed plan through a selected plugin realization.
    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &Path,
        run_id: &str,
    ) -> EngineResult<ExecutionOutcome>;
}

/// CLI-backed Engine transport used for cross-language parity.
#[derive(Debug, Clone)]
pub struct CliEngine {
    executable: PathBuf,
    timeout: Duration,
    cancelled: Arc<AtomicBool>,
}

impl CliEngine {
    /// Create a CLI engine transport.
    pub fn new(executable: impl AsRef<Path>) -> Self {
        Self {
            executable: executable.as_ref().to_path_buf(),
            timeout: Duration::from_secs(30),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Override the bounded response deadline.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Bind an externally controlled cancellation flag.
    #[must_use]
    pub fn with_cancellation(mut self, cancelled: Arc<AtomicBool>) -> Self {
        self.cancelled = cancelled;
        self
    }

    fn request(&self, request: &EngineRequest) -> EngineResult<EngineResponse> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(interrupted_failure(request, "cancelled", false));
        }
        let encoded_request =
            serde_json::to_vec(&EngineRequestEnvelope::new(request)).map_err(|error| {
                EngineFailure::transport("request_encoding_failed", error.to_string())
            })?;
        validate_strict_json(&encoded_request)
            .map_err(|error| EngineFailure::transport("request_encoding_failed", error))?;
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            EngineFailure::transport("invalid_timeout", "timeout exceeds the clock range")
        })?;
        let mut command = Command::new(&self.executable);
        command
            .arg("rpc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .map_err(|error| EngineFailure::transport("engine_start_failed", error.to_string()))?;
        let stdin = child.stdin.take().ok_or_else(|| {
            terminate_engine(&mut child);
            response_loss_failure(request, "engine_stdin_unavailable")
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            terminate_engine(&mut child);
            response_loss_failure(request, "engine_stdout_unavailable")
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            terminate_engine(&mut child);
            response_loss_failure(request, "engine_stderr_unavailable")
        })?;
        let (status, stdout) = std::thread::scope(|scope| {
            let input = scope.spawn(|| write_engine_input(stdin, &encoded_request));
            let output = scope.spawn(|| read_engine_stream(stdout));
            let diagnostic = scope.spawn(|| read_engine_stream(stderr));
            let status = loop {
                if self.cancelled.load(Ordering::Acquire) {
                    terminate_engine(&mut child);
                    break Err(interrupted_failure(request, "cancelled", true));
                }
                if Instant::now() >= deadline {
                    terminate_engine(&mut child);
                    break Err(interrupted_failure(request, "timed_out", true));
                }
                match child.try_wait() {
                    Ok(Some(status)) => break Ok(status),
                    Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                    Err(_) => {
                        terminate_engine(&mut child);
                        break Err(response_loss_failure(request, "engine_wait_failed"));
                    }
                }
            };
            let input = input
                .join()
                .map_err(|_| ())
                .and_then(|value| value.map_err(|_| ()));
            let output = output
                .join()
                .map_err(|_| ())
                .and_then(|value| value.map_err(|_| ()));
            let diagnostic = diagnostic
                .join()
                .map_err(|_| ())
                .and_then(|value| value.map_err(|_| ()));
            if input.is_err() || output.is_err() || diagnostic.is_err() {
                return Err(response_loss_failure(request, "engine_io_failed"));
            }
            status.map(|status| (status, output.expect("checked output")))
        })?;
        if !status.success() {
            return Err(response_loss_failure(request, "engine_process_failed"));
        }
        validate_strict_json(&stdout)
            .map_err(|_| response_loss_failure(request, "invalid_engine_response"))?;
        let envelope: EngineResponseEnvelope<EngineResponse> = decode_json(&stdout)
            .map_err(|_| response_loss_failure(request, "invalid_engine_response"))?;
        envelope.into_result()
    }

    fn seal_resource(&self, candidate: &ResourceCandidate) -> EngineResult<ResourceHandle> {
        match self.request(&EngineRequest::SealResource {
            candidate: candidate.clone(),
        })? {
            EngineResponse::SealedResource { resource } => Ok(resource),
            response => Err(unexpected_response("sealed_resource", &response)),
        }
    }

    fn verify_wait_activation(&self, activation: &WaitActivation) -> EngineResult<WaitActivation> {
        match self.request(&EngineRequest::VerifyWaitActivation {
            activation: activation.clone(),
        })? {
            EngineResponse::VerifiedWaitActivation { activation } => Ok(activation),
            response => Err(unexpected_response("verified_wait_activation", &response)),
        }
    }

    fn verify_durable_command(&self, command: &DurableCommand) -> EngineResult<DurableCommand> {
        match self.request(&EngineRequest::VerifyDurableCommand {
            command: command.clone(),
        })? {
            EngineResponse::VerifiedDurableCommand { command } => Ok(command),
            response => Err(unexpected_response("verified_durable_command", &response)),
        }
    }

    fn verify_evolution_command(
        &self,
        command: &EvolutionCommand,
    ) -> EngineResult<EvolutionCommand> {
        match self.request(&EngineRequest::VerifyEvolutionCommand {
            command: command.clone(),
        })? {
            EngineResponse::VerifiedEvolutionCommand { command } => Ok(command),
            response => Err(unexpected_response("verified_evolution_command", &response)),
        }
    }

    fn verify_live_evolution_command(
        &self,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<LiveEvolutionCommand> {
        match self.request(&EngineRequest::VerifyLiveEvolutionCommand {
            command: command.clone(),
        })? {
            EngineResponse::VerifiedLiveEvolutionCommand { command } => Ok(command),
            response => Err(unexpected_response(
                "verified_live_evolution_command",
                &response,
            )),
        }
    }
}

const ENGINE_STREAM_LIMIT: usize = 16 * 1024 * 1024;

fn write_engine_input(mut stdin: std::process::ChildStdin, input: &[u8]) -> std::io::Result<()> {
    stdin.write_all(input)
}

fn read_engine_stream(mut stream: impl Read) -> std::io::Result<Vec<u8>> {
    let mut retained = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if retained.len() <= ENGINE_STREAM_LIMIT {
            let remaining = ENGINE_STREAM_LIMIT
                .saturating_add(1)
                .saturating_sub(retained.len());
            retained.extend_from_slice(&buffer[..read.min(remaining)]);
        }
    }
    if retained.len() > ENGINE_STREAM_LIMIT {
        return Err(std::io::Error::other(
            "Engine stream exceeded the byte limit",
        ));
    }
    Ok(retained)
}

fn terminate_engine(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        let _ = Command::new("/bin/kill")
            .arg("-TERM")
            .arg(format!("-{}", child.id()))
            .status();
        let grace = Instant::now() + Duration::from_secs(2);
        while Instant::now() < grace {
            if child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = Command::new("/bin/kill")
            .arg("-KILL")
            .arg(format!("-{}", child.id()))
            .status();
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn response_loss_failure(request: &EngineRequest, code: &str) -> EngineFailure {
    if request_is_mutating(request) {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::UnknownWorldOutcome,
            EnginePhase::Transport,
            code,
            "the Engine response was unavailable after a mutating request began",
        );
        failure.retry_disposition = Some(cymule_runtime::EngineRetryDisposition::Reconcile);
        failure
    } else {
        EngineFailure::transport(code, "the Engine response was unavailable")
    }
}

fn request_is_mutating(request: &EngineRequest) -> bool {
    match request {
        EngineRequest::Run { .. } | EngineRequest::ExecuteLiveEvolution { .. } => true,
        EngineRequest::ExecuteDurable { command, .. } => !command.is_query(),
        _ => false,
    }
}

fn interrupted_failure(request: &EngineRequest, kind: &str, began: bool) -> EngineFailure {
    let mutating = request_is_mutating(request);
    if began && mutating {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::UnknownWorldOutcome,
            EnginePhase::Transport,
            format!("engine_response_{kind}"),
            format!("the Engine response was {kind} after a mutating request began"),
        );
        failure.retry_disposition = Some(cymule_runtime::EngineRetryDisposition::Reconcile);
        return failure;
    }
    EngineFailure::new(
        if kind == "timed_out" {
            EngineFailureCategory::TimedOut
        } else {
            EngineFailureCategory::Cancelled
        },
        EnginePhase::Transport,
        format!("engine_response_{kind}"),
        format!("the Engine response was {kind}"),
    )
}

impl Engine for CliEngine {
    fn seal(&self, candidate: &PlanCandidate) -> EngineResult<SealedPlan> {
        match self.request(&EngineRequest::Seal {
            candidate: candidate.clone(),
        })? {
            EngineResponse::Sealed { plan } => Ok(plan),
            response => Err(unexpected_response("sealed", &response)),
        }
    }

    fn seal_resource(&self, candidate: &ResourceCandidate) -> EngineResult<ResourceHandle> {
        CliEngine::seal_resource(self, candidate)
    }

    fn verify_wait_activation(&self, activation: &WaitActivation) -> EngineResult<WaitActivation> {
        CliEngine::verify_wait_activation(self, activation)
    }

    fn verify_durable_command(&self, command: &DurableCommand) -> EngineResult<DurableCommand> {
        CliEngine::verify_durable_command(self, command)
    }

    fn verify_evolution_command(
        &self,
        command: &EvolutionCommand,
    ) -> EngineResult<EvolutionCommand> {
        CliEngine::verify_evolution_command(self, command)
    }

    fn verify_live_evolution_command(
        &self,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<LiveEvolutionCommand> {
        CliEngine::verify_live_evolution_command(self, command)
    }

    fn execute_durable(
        &self,
        target: &EngineDurableTarget,
        command: &DurableCommand,
    ) -> EngineResult<DurableResponse> {
        match self.request(&EngineRequest::ExecuteDurable {
            target: target.clone(),
            command: command.clone(),
        })? {
            EngineResponse::DurableExecuted { response } => Ok(response),
            response => Err(unexpected_response("durable_executed", &response)),
        }
    }

    fn execute_live_evolution(
        &self,
        target: &EngineEvolutionTarget,
        journal_id: &str,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<LiveEvolutionResponse> {
        match self.request(&EngineRequest::ExecuteLiveEvolution {
            target: target.clone(),
            journal_id: journal_id.to_owned(),
            command: command.clone(),
        })? {
            EngineResponse::LiveEvolutionExecuted { response } => Ok(response),
            response => Err(unexpected_response("live_evolution_executed", &response)),
        }
    }

    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &Path,
        run_id: &str,
    ) -> EngineResult<ExecutionOutcome> {
        match self.request(&EngineRequest::Run {
            plan: plan.clone(),
            input: input.clone(),
            plugin: plugin.display().to_string(),
            run_id: run_id.to_owned(),
        })? {
            EngineResponse::ExecutionBoundary { execution } => Ok(execution),
            response => Err(unexpected_response("execution_boundary", &response)),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EngineRequest {
    Seal {
        candidate: PlanCandidate,
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

#[derive(Debug, Clone, Deserialize)]
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

/// High-level provider-neutral durable Run client over an Engine transport.
#[derive(Debug, Clone)]
pub struct DurableEngine<E = CliEngine> {
    transport: E,
    store: EngineStoreTarget,
    executor: Option<EnginePluginTarget>,
    migration: Option<EnginePluginTarget>,
    shadow: Option<EnginePluginTarget>,
    evolution_journal: String,
}

impl DurableEngine<CliEngine> {
    /// Bind one CLI transport, directory domain, and sealed process executor.
    pub fn new(
        executable: impl AsRef<Path>,
        store: impl AsRef<Path>,
        plugin: impl AsRef<Path>,
    ) -> Self {
        Self {
            transport: CliEngine::new(executable),
            store: EngineStoreTarget::directory(store.as_ref().display().to_string()),
            executor: Some(EnginePluginTarget::process(
                plugin.as_ref().display().to_string(),
            )),
            migration: None,
            shadow: None,
            evolution_journal: "cymule.sdk.live-evolution".to_owned(),
        }
    }

    /// Override the complete high-level durable request deadline.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.transport = self.transport.with_timeout(timeout);
        self
    }

    /// Bind an externally controlled high-level durable cancellation flag.
    #[must_use]
    pub fn with_cancellation(mut self, cancelled: Arc<AtomicBool>) -> Self {
        self.transport = self.transport.with_cancellation(cancelled);
        self
    }
}

impl<E: Engine> DurableEngine<E> {
    /// Bind any Engine transport to provider-neutral durable targets.
    pub fn from_transport(transport: E, store: EngineStoreTarget) -> Self {
        Self {
            transport,
            store,
            executor: None,
            migration: None,
            shadow: None,
            evolution_journal: "cymule.sdk.live-evolution".to_owned(),
        }
    }

    /// Select the execution provider used by mutating Run commands.
    #[must_use]
    pub fn with_executor(mut self, executor: EnginePluginTarget) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Select one exact migration adapter.
    #[must_use]
    pub fn with_migration_plugin(mut self, plugin: EnginePluginTarget) -> Self {
        self.migration = Some(plugin);
        self
    }

    /// Select one exact shadow driver.
    #[must_use]
    pub fn with_shadow_plugin(mut self, plugin: EnginePluginTarget) -> Self {
        self.shadow = Some(plugin);
        self
    }

    /// Select the durable live-evolution journal identity.
    #[must_use]
    pub fn with_evolution_journal(mut self, journal: impl Into<String>) -> Self {
        self.evolution_journal = journal.into();
        self
    }

    /// Start an idempotent Run and drive it to the next durable boundary.
    pub fn start(
        &self,
        run_id: impl Into<String>,
        candidate: PlanCandidate,
        input: Value,
    ) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.into(),
            candidate,
            input,
        })
    }

    /// Read one Run without reducing durable state.
    pub fn get(&self, run_id: &str) -> EngineResult<Option<Box<DurableRunView>>> {
        match self.submit(&DurableCommand::QueryRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            query_id: format!("sdk:get:{run_id}"),
            run_id: run_id.to_owned(),
        })? {
            DurableResponse::Run { run } => Ok(run),
            response => Err(unexpected_durable_response("run", &response)),
        }
    }

    /// Resume one ready Run.
    pub fn resume(&self, run_id: impl Into<String>) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.into(),
        })
    }

    /// Admit one identified signal delivery without selecting targets locally.
    pub fn signal(
        &self,
        activation_id: impl Into<String>,
        key: impl Into<String>,
        wait_ids: impl IntoIterator<Item = String>,
        value: Value,
    ) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: activation_id.into(),
            source: WaitActivationSource::Signal { key: key.into() },
            wait_ids: wait_ids.into_iter().collect::<BTreeSet<_>>(),
            value,
        })
    }

    /// Release one explicit effect intent after its owning scope committed.
    pub fn release(&self, intent_id: impl Into<String>) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::ReleaseEffect {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            intent_id: intent_id.into(),
        })
    }

    /// Submit one atomic live-evolution command to the same durable domain.
    pub fn evolve(&self, command: &LiveEvolutionCommand) -> EngineResult<LiveEvolutionResponse> {
        self.transport.execute_live_evolution(
            &EngineEvolutionTarget {
                store: self.store.clone(),
                migration: self.migration.clone(),
                shadow: self.shadow.clone(),
            },
            &self.evolution_journal,
            command,
        )
    }

    fn submit(&self, command: &DurableCommand) -> EngineResult<DurableResponse> {
        let target = if command.is_query() {
            EngineDurableTarget::query(self.store.clone())
        } else {
            EngineDurableTarget {
                store: self.store.clone(),
                executor: self.executor.clone(),
            }
        };
        self.transport.execute_durable(&target, command)
    }
}

fn unexpected_durable_response(expected: &str, response: &DurableResponse) -> EngineFailure {
    EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "unexpected_engine_response",
        format!("expected durable {expected}, received {response:?}"),
    )
}

fn unexpected_response(expected: &str, response: &EngineResponse) -> EngineFailure {
    EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "unexpected_engine_response",
        format!("expected {expected}, received {response:?}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures")
            .join(name)
    }

    fn empty_candidate() -> PlanCandidate {
        PlanCandidate {
            ir_version: "cymule.ir/2".to_owned(),
            name: "transport".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: Vec::new(),
            metadata: std::collections::BTreeMap::default(),
        }
    }

    #[test]
    fn interruption_classification_preserves_mutation_uncertainty() {
        let candidate = empty_candidate();
        let mutation = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::execute(
                EngineStoreTarget::directory("domain"),
                EnginePluginTarget::process("plugin"),
            ),
            command: DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run:test".to_owned(),
                candidate,
                input: Value::Null,
            },
        };
        let lost = interrupted_failure(&mutation, "timed_out", true);
        assert_eq!(lost.category, EngineFailureCategory::UnknownWorldOutcome);
        assert_eq!(
            lost.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile)
        );

        let query = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command: DurableCommand::QueryDomain {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                query_id: "query:test".to_owned(),
            },
        };
        assert_eq!(
            interrupted_failure(&query, "cancelled", true).category,
            EngineFailureCategory::Cancelled
        );
    }

    #[cfg(unix)]
    #[test]
    fn large_engine_response_is_drained_before_child_exit() {
        let engine =
            CliEngine::new(fixture("large-response-engine")).with_timeout(Duration::from_secs(2));
        let plan = engine
            .seal(&empty_candidate())
            .expect("large response completes");
        assert_eq!(plan.candidate.metadata["large"].len(), 262_144);
    }

    #[cfg(unix)]
    #[test]
    fn mutating_response_loss_requires_reconciliation() {
        let engine = CliEngine::new(fixture("response-loss-engine"));
        let error = engine
            .execute_durable(
                &EngineDurableTarget::execute(
                    EngineStoreTarget::directory("unused"),
                    EnginePluginTarget::process("unused"),
                ),
                &DurableCommand::StartRun {
                    control_version: DURABLE_CONTROL_VERSION.to_owned(),
                    run_id: "run:response-loss".to_owned(),
                    candidate: empty_candidate(),
                    input: Value::Null,
                },
            )
            .expect_err("missing mutating response is ambiguous");
        assert_eq!(error.category, EngineFailureCategory::UnknownWorldOutcome);
        assert_eq!(
            error.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile)
        );
    }
}
