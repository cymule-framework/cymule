use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use cymule_core::{CoreError, PlanCandidate, SealedPlan};
use cymule_durable::WaitActivation;
use cymule_evolution::EvolutionCommand;
use cymule_resource::{ResourceCandidate, ResourceHandle};
use cymule_runtime::ExecutionResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Engine operations shared by SDK transports.
pub trait Engine {
    /// Validate and seal a candidate with the trusted Rust kernel.
    fn seal(&self, candidate: &PlanCandidate) -> Result<SealedPlan, CoreError>;
    /// Validate and seal a provider-neutral Resource Candidate.
    fn seal_resource(&self, candidate: &ResourceCandidate) -> Result<ResourceHandle, CoreError>;
    /// Validate a provider-neutral signal or timer activation record.
    fn verify_wait_activation(
        &self,
        activation: &WaitActivation,
    ) -> Result<WaitActivation, CoreError>;
    /// Validate one closed, versioned M4 control envelope.
    fn verify_evolution_command(
        &self,
        command: &EvolutionCommand,
    ) -> Result<EvolutionCommand, CoreError>;
    /// Execute a sealed plan through a selected plugin realization.
    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &Path,
        run_id: &str,
    ) -> Result<ExecutionResult, CoreError>;
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

    fn request(&self, request: &EngineRequest) -> Result<EngineResponse, CoreError> {
        let mut child = Command::new(&self.executable)
            .arg("rpc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| CoreError::Encoding(error.to_string()))?;
        child
            .stdin
            .take()
            .ok_or_else(|| CoreError::Encoding("CLI stdin was not captured".to_owned()))?
            .write_all(&serde_json::to_vec(request)?)
            .map_err(|error| CoreError::Encoding(error.to_string()))?;
        let output = child
            .wait_with_output()
            .map_err(|error| CoreError::Encoding(error.to_string()))?;
        if !output.status.success() {
            return Err(CoreError::Validation(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        serde_json::from_slice(&output.stdout).map_err(Into::into)
    }

    fn seal_resource(&self, candidate: &ResourceCandidate) -> Result<ResourceHandle, CoreError> {
        match self.request(&EngineRequest::SealResource {
            candidate: candidate.clone(),
        })? {
            EngineResponse::SealedResource { resource } => Ok(resource),
            response => Err(CoreError::Validation(format!(
                "CLI returned unexpected response {response:?}"
            ))),
        }
    }

    fn verify_wait_activation(
        &self,
        activation: &WaitActivation,
    ) -> Result<WaitActivation, CoreError> {
        match self.request(&EngineRequest::VerifyWaitActivation {
            activation: activation.clone(),
        })? {
            EngineResponse::VerifiedWaitActivation { activation } => Ok(activation),
            response => Err(CoreError::Validation(format!(
                "CLI returned unexpected response {response:?}"
            ))),
        }
    }

    fn verify_evolution_command(
        &self,
        command: &EvolutionCommand,
    ) -> Result<EvolutionCommand, CoreError> {
        match self.request(&EngineRequest::VerifyEvolutionCommand {
            command: command.clone(),
        })? {
            EngineResponse::VerifiedEvolutionCommand { command } => Ok(command),
            response => Err(CoreError::Validation(format!(
                "CLI returned unexpected response {response:?}"
            ))),
        }
    }
}

impl Engine for CliEngine {
    fn seal(&self, candidate: &PlanCandidate) -> Result<SealedPlan, CoreError> {
        match self.request(&EngineRequest::Seal {
            candidate: candidate.clone(),
        })? {
            EngineResponse::Sealed { plan } => Ok(plan),
            response => Err(CoreError::Validation(format!(
                "CLI returned unexpected response {response:?}"
            ))),
        }
    }

    fn seal_resource(&self, candidate: &ResourceCandidate) -> Result<ResourceHandle, CoreError> {
        CliEngine::seal_resource(self, candidate)
    }

    fn verify_wait_activation(
        &self,
        activation: &WaitActivation,
    ) -> Result<WaitActivation, CoreError> {
        CliEngine::verify_wait_activation(self, activation)
    }

    fn verify_evolution_command(
        &self,
        command: &EvolutionCommand,
    ) -> Result<EvolutionCommand, CoreError> {
        CliEngine::verify_evolution_command(self, command)
    }

    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &Path,
        run_id: &str,
    ) -> Result<ExecutionResult, CoreError> {
        match self.request(&EngineRequest::Run {
            plan: plan.clone(),
            input: input.clone(),
            plugin: plugin.display().to_string(),
            run_id: run_id.to_owned(),
        })? {
            EngineResponse::Executed { result } => Ok(result),
            response => Err(CoreError::Validation(format!(
                "CLI returned unexpected response {response:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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
    VerifyEvolutionCommand {
        command: EvolutionCommand,
    },
    Run {
        plan: SealedPlan,
        input: Value,
        plugin: String,
        run_id: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EngineResponse {
    Sealed { plan: SealedPlan },
    SealedResource { resource: ResourceHandle },
    VerifiedWaitActivation { activation: WaitActivation },
    VerifiedEvolutionCommand { command: EvolutionCommand },
    Executed { result: ExecutionResult },
    Verified,
}
