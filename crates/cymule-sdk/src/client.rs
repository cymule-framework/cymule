use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use cymule_core::{PlanCandidate, SealedPlan, decode_json};
use cymule_durable::{DurableCommand, WaitActivation};
use cymule_evolution::{EvolutionCommand, LiveEvolutionCommand};
use cymule_resource::{ResourceCandidate, ResourceHandle};
use cymule_runtime::{
    EngineFailure, EngineFailureCategory, EnginePhase, EngineRequestEnvelope,
    EngineResponseEnvelope, EngineResult, ExecutionResult,
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
    /// Execute a sealed plan through a selected plugin realization.
    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &Path,
        run_id: &str,
    ) -> EngineResult<ExecutionResult>;
}

/// CLI-backed Engine transport used for cross-language parity.
#[derive(Debug, Clone)]
pub struct CliEngine {
    executable: PathBuf,
}

impl CliEngine {
    /// Create a CLI engine transport.
    pub fn new(executable: impl AsRef<Path>) -> Self {
        Self {
            executable: executable.as_ref().to_path_buf(),
        }
    }

    fn request(&self, request: &EngineRequest) -> EngineResult<EngineResponse> {
        let mut child = Command::new(&self.executable)
            .arg("rpc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| EngineFailure::transport("engine_start_failed", error.to_string()))?;
        child
            .stdin
            .take()
            .ok_or_else(|| {
                EngineFailure::transport("engine_stdin_unavailable", "CLI stdin was not captured")
            })?
            .write_all(
                &serde_json::to_vec(&EngineRequestEnvelope::new(request)).map_err(|error| {
                    EngineFailure::transport("request_encoding_failed", error.to_string())
                })?,
            )
            .map_err(|error| EngineFailure::transport("engine_write_failed", error.to_string()))?;
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

    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &Path,
        run_id: &str,
    ) -> EngineResult<ExecutionResult> {
        match self.request(&EngineRequest::Run {
            plan: plan.clone(),
            input: input.clone(),
            plugin: plugin.display().to_string(),
            run_id: run_id.to_owned(),
        })? {
            EngineResponse::Executed { result } => Ok(result),
            response => Err(unexpected_response("executed", &response)),
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
    Executed { result: ExecutionResult },
    Verified,
}

fn unexpected_response(expected: &str, response: &EngineResponse) -> EngineFailure {
    EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "unexpected_engine_response",
        format!("expected {expected}, received {response:?}"),
    )
}
