use std::collections::BTreeSet;
use std::io::Write;
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
    EngineFailure, EngineFailureCategory, EnginePhase, EngineRequestEnvelope,
    EngineResponseEnvelope, EngineResult, ExecutionOutcome, validate_strict_json,
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
        store: &Path,
        plugin: &Path,
        command: &DurableCommand,
    ) -> EngineResult<DurableResponse>;
    /// Submit one stateful live-evolution command to its durable journal.
    fn execute_live_evolution(
        &self,
        store: &Path,
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
        let mut child = Command::new(&self.executable)
            .arg("rpc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| EngineFailure::transport("engine_start_failed", error.to_string()))?;
        let encoded_request =
            serde_json::to_vec(&EngineRequestEnvelope::new(request)).map_err(|error| {
                EngineFailure::transport("request_encoding_failed", error.to_string())
            })?;
        validate_strict_json(&encoded_request)
            .map_err(|error| EngineFailure::transport("request_encoding_failed", error))?;
        child
            .stdin
            .take()
            .ok_or_else(|| {
                EngineFailure::transport("engine_stdin_unavailable", "CLI stdin was not captured")
            })?
            .write_all(&encoded_request)
            .map_err(|error| EngineFailure::transport("engine_write_failed", error.to_string()))?;
        let deadline = Instant::now() + self.timeout;
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(interrupted_failure(request, "cancelled", true));
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(interrupted_failure(request, "timed_out", true));
            }
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(error) => {
                    return Err(EngineFailure::transport(
                        "engine_wait_failed",
                        error.to_string(),
                    ));
                }
            }
        }
        let output = child
            .wait_with_output()
            .map_err(|error| EngineFailure::transport("engine_wait_failed", error.to_string()))?;
        if !output.status.success() {
            return Err(EngineFailure::transport(
                "engine_process_failed",
                format!(
                    "engine exited without a protocol response ({})",
                    output.status
                ),
            ));
        }
        let envelope: EngineResponseEnvelope<EngineResponse> = decode_json(&output.stdout)
            .map_err(|error| {
                EngineFailure::transport("invalid_engine_response", error.to_string())
            })?;
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

fn interrupted_failure(request: &EngineRequest, kind: &str, began: bool) -> EngineFailure {
    let mutating = matches!(
        request,
        EngineRequest::Run { .. }
            | EngineRequest::ExecuteLiveEvolution { .. }
            | EngineRequest::ExecuteDurable {
                command: DurableCommand::StartRun { .. }
                    | DurableCommand::ResumeRun { .. }
                    | DurableCommand::ActivateWait { .. }
                    | DurableCommand::ReleaseEffect { .. },
                ..
            }
    );
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
        store: &Path,
        plugin: &Path,
        command: &DurableCommand,
    ) -> EngineResult<DurableResponse> {
        match self.request(&EngineRequest::ExecuteDurable {
            store: store.display().to_string(),
            plugin: plugin.display().to_string(),
            command: command.clone(),
        })? {
            EngineResponse::DurableExecuted { response } => Ok(response),
            response => Err(unexpected_response("durable_executed", &response)),
        }
    }

    fn execute_live_evolution(
        &self,
        store: &Path,
        journal_id: &str,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<LiveEvolutionResponse> {
        match self.request(&EngineRequest::ExecuteLiveEvolution {
            store: store.display().to_string(),
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
pub struct DurableEngine {
    transport: CliEngine,
    store: PathBuf,
    plugin: PathBuf,
    evolution_journal: String,
}

impl DurableEngine {
    /// Bind one CLI transport, durable domain, and immutable process plugin.
    pub fn new(
        executable: impl AsRef<Path>,
        store: impl AsRef<Path>,
        plugin: impl AsRef<Path>,
    ) -> Self {
        Self {
            transport: CliEngine::new(executable),
            store: store.as_ref().to_path_buf(),
            plugin: plugin.as_ref().to_path_buf(),
            evolution_journal: "cymule.sdk.live-evolution".to_owned(),
        }
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
        self.transport
            .execute_live_evolution(&self.store, &self.evolution_journal, command)
    }

    fn submit(&self, command: &DurableCommand) -> EngineResult<DurableResponse> {
        self.transport
            .execute_durable(&self.store, &self.plugin, command)
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

    #[test]
    fn interruption_classification_preserves_mutation_uncertainty() {
        let candidate = PlanCandidate {
            ir_version: "cymule.ir/2".to_owned(),
            name: "classification".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: Vec::new(),
            metadata: std::collections::BTreeMap::default(),
        };
        let mutation = EngineRequest::ExecuteDurable {
            store: "domain".to_owned(),
            plugin: "plugin".to_owned(),
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
            store: "domain".to_owned(),
            plugin: "plugin".to_owned(),
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
}
