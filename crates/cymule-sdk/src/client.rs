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
    CancellationCommand, DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand,
    DurablePageCursor, DurableResponse, DurableRunAttemptPage, DurableRunCurrent,
    DurableRunEffectPage, DurableRunIndexPage, DurableRunItem, DurableRunItemSelector,
    DurableRunOccurrencePage, DurableRunWaitPage, EffectResolutionCommand,
};
use cymule_durable_protocol::{
    ClockObservationRef, ClockObservationResult, ExecutionClaimRequest, WaitActivation,
    WaitActivationSource,
};
use cymule_evolution::{EvolutionCommand, EvolutionCommit, LiveEvolutionCommand};
use cymule_resource::{ResourceCandidate, ResourceHandle};
use cymule_runtime::{
    EngineClockTarget, EngineContractSide, EngineDurableTarget, EngineEvolutionTarget,
    EngineFailure, EngineFailureCategory, EngineMigrationProviderTarget, EnginePhase,
    EnginePluginTarget, EngineRequestEnvelope, EngineResponseEnvelope, EngineResult,
    EngineRetryDisposition, EngineShadowProviderTarget, EngineStoreTarget, ExecutionOutcome,
    MAX_ENGINE_REQUEST_BYTES, validate_json_typed_roundtrip, validate_strict_json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Closed limits and continuation authority for one bounded durable query page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurablePageQueryOptions {
    /// Optional exact revision precondition.
    pub expected_revision: Option<String>,
    /// Optional authenticated continuation returned by the preceding page.
    pub cursor: Option<DurablePageCursor>,
    /// Maximum number of summaries to return.
    pub limit: u32,
    /// Maximum canonical response bytes accepted by the caller.
    pub max_canonical_bytes: u64,
}

/// Closed selector and byte authority for one exact Run-owned durable leaf read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableRunItemQuery {
    /// Owning Run.
    pub run_id: String,
    /// Optional exact revision precondition.
    pub expected_revision: Option<String>,
    /// Exact typed item selector.
    pub selector: DurableRunItemSelector,
    /// Maximum canonical response bytes accepted by the caller.
    pub max_canonical_bytes: u64,
}

/// Revision/root-pinned result of one bounded Run-current read.
#[derive(Debug, Clone, PartialEq)]
pub struct DurableRunCurrentRead {
    /// Exact revision observed by the durable authority.
    pub observed_revision: String,
    /// Canonical digest of the complete source `StateRoot`.
    pub source_root: String,
    /// Current projection, absent only when the Run does not exist.
    pub current: Option<Box<DurableRunCurrent>>,
}

/// Revision/root-pinned result of one exact Run-owned durable leaf read.
#[derive(Debug, Clone, PartialEq)]
pub struct DurableRunItemRead {
    /// Owning Run.
    pub run_id: String,
    /// Exact revision observed by the durable authority.
    pub observed_revision: String,
    /// Canonical digest of the complete source `StateRoot`.
    pub source_root: String,
    /// Exact leaf, absent only when the selected identity does not exist.
    pub item: Option<Box<DurableRunItem>>,
}

/// Engine operations shared by SDK transports.
pub trait Engine {
    /// Validate and seal a candidate with the trusted Rust kernel.
    ///
    /// # Errors
    /// Returns validation, Engine, or transport failures, including an invalid sealed response.
    fn seal(&self, candidate: &PlanCandidate) -> EngineResult<SealedPlan>;
    /// Validate and seal a provider-neutral Resource Candidate.
    ///
    /// # Errors
    /// Returns invalid-candidate, Engine, or response-integrity failures.
    fn seal_resource(&self, candidate: &ResourceCandidate) -> EngineResult<ResourceHandle>;
    /// Validate a provider-neutral signal or timer activation record.
    ///
    /// # Errors
    /// Returns malformed-activation, Engine, or mismatched-response failures.
    fn verify_wait_activation(&self, activation: &WaitActivation) -> EngineResult<WaitActivation>;
    /// Validate one closed, versioned M1 control envelope.
    ///
    /// # Errors
    /// Returns command-validation, Engine, or mismatched-response failures.
    fn verify_durable_command(&self, command: &DurableCommand) -> EngineResult<DurableCommand>;
    /// Issue one exact retained logical Clock observation for a Run.
    ///
    /// # Errors
    /// Returns invalid Clock/Run input, issuance, or response-loss failures.
    fn observe_clock(
        &self,
        target: &EngineClockTarget,
        run_id: &str,
    ) -> EngineResult<ClockObservationResult>;
    /// Validate one closed, versioned M4 control envelope.
    ///
    /// # Errors
    /// Returns command-validation, Engine, or mismatched-response failures.
    fn verify_evolution_command(
        &self,
        command: &EvolutionCommand,
    ) -> EngineResult<EvolutionCommand>;
    /// Validate one complete unified live-evolution control envelope.
    ///
    /// # Errors
    /// Returns invalid live intent, Engine, or mismatched-response failures.
    fn verify_live_evolution_command(
        &self,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<LiveEvolutionCommand>;
    /// Submit one stateful command to a durable Rust authority.
    ///
    /// # Errors
    /// Returns validation, admission, or transport failures. An uncertain mutation
    /// response retains its explicit reconciliation disposition.
    fn execute_durable(
        &self,
        target: &EngineDurableTarget,
        command: &DurableCommand,
    ) -> EngineResult<DurableResponse>;
    /// Submit one stateful live-evolution command and return its semantic commit.
    ///
    /// # Errors
    /// Returns invalid intent/provider input, rejected admission, or invalid/lost
    /// commit responses without inferring replay permission.
    fn execute_live_evolution(
        &self,
        target: &EngineEvolutionTarget,
        evolution_id: &str,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<EvolutionCommit>;
    /// Execute a sealed plan through one complete process-backed plugin target.
    ///
    /// # Errors
    /// Returns Plan/input/provider validation, execution, or response-loss failures.
    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &EnginePluginTarget,
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
        let (encoded_request, sent_inner, sent_request) = snapshot_cli_request(request)?;
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            local_request_failure("invalid_timeout", "timeout exceeds the clock range")
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
            response_loss_failure(&sent_request, "engine_stdin_unavailable")
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            terminate_engine(&mut child);
            response_loss_failure(&sent_request, "engine_stdout_unavailable")
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            terminate_engine(&mut child);
            response_loss_failure(&sent_request, "engine_stderr_unavailable")
        })?;
        let input_done = AtomicBool::new(false);
        let output_done = AtomicBool::new(false);
        let diagnostic_done = AtomicBool::new(false);
        let (status, stdout, input_failed) = std::thread::scope(|scope| {
            let input_complete = &input_done;
            let input_bytes = &encoded_request;
            let input = scope.spawn(move || {
                let result = write_engine_input(stdin, input_bytes);
                input_complete.store(true, Ordering::Release);
                result
            });
            let output_complete = &output_done;
            let output = scope.spawn(move || {
                let result = read_engine_stream(stdout);
                output_complete.store(true, Ordering::Release);
                result
            });
            let diagnostic_complete = &diagnostic_done;
            let diagnostic = scope.spawn(move || {
                let result = read_engine_stream(stderr);
                diagnostic_complete.store(true, Ordering::Release);
                result
            });
            let status = self.wait_for_engine_completion(
                &mut child,
                &sent_request,
                deadline,
                [&input_done, &output_done, &diagnostic_done],
            );
            let input_failed = input
                .join()
                .map_err(|_| ())
                .and_then(|value| value.map_err(|_| ()))
                .is_err();
            let output = output
                .join()
                .map_err(|_| ())
                .and_then(|value| value.map_err(|_| ()));
            let diagnostic = diagnostic
                .join()
                .map_err(|_| ())
                .and_then(|value| value.map_err(|_| ()));
            if output.is_err() || diagnostic.is_err() {
                return Err(response_loss_failure(&sent_request, "engine_io_failed"));
            }
            status.map(|status| (status, output.expect("checked output"), input_failed))
        })?;
        if !status.success() {
            return Err(response_loss_failure(
                &sent_request,
                "engine_process_failed",
            ));
        }
        admit_cli_output(&sent_request, &sent_inner, &stdout, input_failed)
    }

    fn wait_for_engine_completion(
        &self,
        child: &mut std::process::Child,
        request: &EngineRequest,
        deadline: Instant,
        streams_done: [&AtomicBool; 3],
    ) -> EngineResult<std::process::ExitStatus> {
        let mut exited = None;
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                terminate_engine(child);
                return Err(interrupted_failure(request, "cancelled", true));
            }
            if Instant::now() >= deadline {
                terminate_engine(child);
                return Err(interrupted_failure(request, "timed_out", true));
            }
            if exited.is_none() {
                match child.try_wait() {
                    Ok(Some(status)) if !status.success() => {
                        terminate_engine(child);
                        return Err(response_loss_failure(request, "engine_process_failed"));
                    }
                    Ok(Some(status)) => exited = Some(status),
                    Ok(None) => {}
                    Err(_) => {
                        terminate_engine(child);
                        return Err(response_loss_failure(request, "engine_wait_failed"));
                    }
                }
            }
            if streams_done.iter().all(|done| done.load(Ordering::Acquire))
                && let Some(status) = exited
            {
                return Ok(status);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
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
            EngineResponse::VerifiedWaitActivation { activation } => {
                activation.verify().map_err(|error| {
                    invalid_engine_payload("wait activation", &error.to_string())
                })?;
                Ok(activation)
            }
            response => Err(unexpected_response("verified_wait_activation", &response)),
        }
    }

    fn verify_durable_command(&self, command: &DurableCommand) -> EngineResult<DurableCommand> {
        match self.request(&EngineRequest::VerifyDurableCommand {
            command: command.clone(),
        })? {
            EngineResponse::VerifiedDurableCommand { command } => {
                command.verify().map_err(|error| {
                    invalid_engine_payload("durable command", &error.to_string())
                })?;
                Ok(command)
            }
            response => Err(unexpected_response("verified_durable_command", &response)),
        }
    }

    fn observe_clock(
        &self,
        target: &EngineClockTarget,
        run_id: &str,
    ) -> EngineResult<ClockObservationResult> {
        match self.request(&EngineRequest::ObserveClock {
            target: target.clone(),
            run_id: run_id.to_owned(),
        })? {
            EngineResponse::ClockObserved { result } => Ok(result),
            response => Err(unexpected_response("clock_observed", &response)),
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

fn snapshot_cli_request(request: &EngineRequest) -> EngineResult<(Vec<u8>, Value, EngineRequest)> {
    let encoded = serde_json::to_vec(&EngineRequestEnvelope::new(request))
        .map_err(|error| local_request_failure("request_encoding_failed", error))?;
    if encoded.len() > MAX_ENGINE_REQUEST_BYTES {
        return Err(local_request_failure(
            "engine_request_too_large",
            format!("complete Engine request exceeds {MAX_ENGINE_REQUEST_BYTES} UTF-8 bytes"),
        ));
    }
    validate_strict_json(&encoded)
        .map_err(|error| local_request_failure("request_encoding_failed", error))?;
    let envelope: EngineRequestEnvelope<Value> = decode_json(&encoded)
        .map_err(|error| local_request_failure("request_encoding_failed", error))?;
    let mut inner = envelope.request;
    normalize_mathematical_integers(&mut inner);
    let typed: EngineRequest = serde_json::from_value(inner.clone())
        .map_err(|error| local_request_failure("request_encoding_failed", error))?;
    let normalized = serde_json::to_value(&typed)
        .map_err(|error| local_request_failure("request_encoding_failed", error))?;
    validate_json_typed_roundtrip(&inner, &normalized)
        .map_err(|error| local_request_failure("request_encoding_failed", error))?;
    Ok((encoded, inner, typed))
}

fn admit_cli_output(
    sent_request: &EngineRequest,
    sent_inner: &Value,
    stdout: &[u8],
    input_failed: bool,
) -> EngineResult<EngineResponse> {
    validate_strict_json(stdout)
        .map_err(|_| response_loss_failure(sent_request, "invalid_engine_response"))?;
    let raw_envelope: Value = decode_json(stdout)
        .map_err(|_| response_loss_failure(sent_request, "invalid_engine_response"))?;
    if input_failed && raw_envelope.get("outcome").and_then(Value::as_str) != Some("failure") {
        return Err(response_loss_failure(
            sent_request,
            "engine_request_incomplete",
        ));
    }
    admit_raw_engine_response(sent_request, sent_inner, &raw_envelope)
}

const ENGINE_STREAM_LIMIT: usize = 16 * 1024 * 1024;

fn write_engine_input(mut stdin: std::process::ChildStdin, input: &[u8]) -> std::io::Result<()> {
    stdin.write_all(input)
}

fn read_engine_stream(mut stream: impl Read) -> std::io::Result<Vec<u8>> {
    let mut retained = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    let detection_limit = ENGINE_STREAM_LIMIT
        .checked_add(1)
        .expect("Engine stream limit must leave room for one overflow byte");
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if retained.len() < detection_limit {
            let remaining = detection_limit
                .checked_sub(retained.len())
                .expect("retained Engine stream bytes cannot exceed the detection limit");
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
        // The direct child PID is also the process-group ID. Always signal the
        // group, even after `try_wait` observed the direct child exit, because
        // a descendant can still own one of the transport pipes.
        let _ = Command::new("/bin/kill")
            .arg("-KILL")
            .arg(format!("-{}", child.id()))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
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

fn local_request_failure(code: &str, message: impl std::fmt::Display) -> EngineFailure {
    let mut failure = EngineFailure::new(
        EngineFailureCategory::Validation,
        EnginePhase::ValidateRequest,
        code,
        message.to_string(),
    );
    failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
    failure
}

fn unsupported_engine_protocol_failure(request: &EngineRequest, received: &str) -> EngineFailure {
    if request_is_mutating(request) {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::UnknownWorldOutcome,
            EnginePhase::Transport,
            "unsupported_engine_protocol",
            format!(
                "expected {}, received {received:?} after a mutating request began",
                cymule_runtime::ENGINE_PROTOCOL_VERSION
            ),
        );
        failure.retry_disposition = Some(EngineRetryDisposition::Reconcile);
        return failure;
    }
    let mut failure = EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "unsupported_engine_protocol",
        format!(
            "expected {}, received {received:?}",
            cymule_runtime::ENGINE_PROTOCOL_VERSION
        ),
    );
    failure.contract = Some(cymule_runtime::ENGINE_PROTOCOL_VERSION.into());
    failure.contract_side = Some(EngineContractSide::Schema);
    failure.retry_disposition = Some(EngineRetryDisposition::Never);
    failure
}

fn normalize_mathematical_integers(value: &mut Value) {
    match value {
        Value::Number(number) if number.as_i64().is_none() && number.as_u64().is_none() => {
            let Some(float) = number.as_f64() else {
                return;
            };
            if !float.is_finite() || float.fract() != 0.0 || float.abs() > 9_007_199_254_740_991.0 {
                return;
            }
            *number = format!("{float:.0}")
                .parse()
                .expect("a finite safe mathematical integer formats as a JSON integer");
        }
        Value::Array(values) => {
            for value in values {
                normalize_mathematical_integers(value);
            }
        }
        Value::Object(members) => {
            for value in members.values_mut() {
                normalize_mathematical_integers(value);
            }
        }
        _ => {}
    }
}

fn invalid_success_response(request: &EngineRequest, message: &str) -> EngineFailure {
    invalid_typed_response(request_is_mutating(request), "success response", message)
}

fn invalid_typed_response(mutating: bool, kind: &str, message: &str) -> EngineFailure {
    if mutating {
        let mut failure = EngineFailure::new(
            EngineFailureCategory::UnknownWorldOutcome,
            EnginePhase::Transport,
            "invalid_engine_response",
            format!(
                "the Engine returned an invalid {kind} after a mutating request began: {message}"
            ),
        );
        failure.retry_disposition = Some(cymule_runtime::EngineRetryDisposition::Reconcile);
        failure
    } else {
        invalid_engine_payload(kind, message)
    }
}

fn request_is_mutating(request: &EngineRequest) -> bool {
    match request {
        EngineRequest::Run { .. }
        | EngineRequest::ObserveClock { .. }
        | EngineRequest::ExecuteLiveEvolution { .. } => true,
        EngineRequest::ExecuteDurable { command, .. } => durable_command_is_mutating(command),
        _ => false,
    }
}

fn durable_command_is_mutating(command: &DurableCommand) -> bool {
    !command.is_read_only()
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
    let mut failure = EngineFailure::new(
        if kind == "timed_out" {
            EngineFailureCategory::TimedOut
        } else {
            EngineFailureCategory::Cancelled
        },
        EnginePhase::Transport,
        format!("engine_response_{kind}"),
        format!("the Engine response was {kind}"),
    );
    failure.retry_disposition = Some(if kind == "timed_out" {
        cymule_runtime::EngineRetryDisposition::RetrySameRequest
    } else {
        cymule_runtime::EngineRetryDisposition::Never
    });
    failure
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

    fn observe_clock(
        &self,
        target: &EngineClockTarget,
        run_id: &str,
    ) -> EngineResult<ClockObservationResult> {
        CliEngine::observe_clock(self, target, run_id)
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
        let expected_start_plan = match command {
            DurableCommand::StartRun { candidate, .. } => Some(self.seal(candidate)?),
            _ => None,
        };
        let response = match self.request(&EngineRequest::ExecuteDurable {
            target: target.clone(),
            command: command.clone(),
        })? {
            EngineResponse::DurableExecuted { response } => response,
            response => return Err(unexpected_response("durable_executed", &response)),
        };
        if let (
            Some(plan),
            DurableCommand::StartRun { run_id, .. },
            DurableResponse::RunBoundary { boundary },
        ) = (expected_start_plan.as_ref(), command, &response)
        {
            verify_durable_boundary_run(boundary, run_id, Some(&plan.plan_id))
                .map_err(|error| invalid_typed_response(true, "durable Start response", &error))?;
        }
        Ok(response)
    }

    fn execute_live_evolution(
        &self,
        target: &EngineEvolutionTarget,
        evolution_id: &str,
        command: &LiveEvolutionCommand,
    ) -> EngineResult<EvolutionCommit> {
        verify_live_evolution_preflight(command)?;
        verify_evolution_target_preflight(target, command)?;
        match self.request(&EngineRequest::ExecuteLiveEvolution {
            target: target.clone(),
            evolution_id: evolution_id.to_owned(),
            command: command.clone(),
        })? {
            EngineResponse::LiveEvolutionExecuted { commit } => Ok(*commit),
            response => Err(unexpected_response("live_evolution_executed", &response)),
        }
    }

    fn run(
        &self,
        plan: &SealedPlan,
        input: &Value,
        plugin: &EnginePluginTarget,
        run_id: &str,
    ) -> EngineResult<ExecutionOutcome> {
        match self.request(&EngineRequest::Run {
            plan: plan.clone(),
            input: input.clone(),
            plugin: plugin.clone(),
            run_id: run_id.to_owned(),
        })? {
            EngineResponse::ExecutionBoundary { execution } => Ok(execution),
            response => Err(unexpected_response("execution_boundary", &response)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        target: EngineEvolutionTarget,
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

fn admit_engine_response(
    request: &EngineRequest,
    sent_inner: &Value,
    envelope: EngineResponseEnvelope<Value, EngineResponse>,
) -> EngineResult<EngineResponse> {
    let protocol = match &envelope {
        EngineResponseEnvelope::Success {
            engine_protocol, ..
        }
        | EngineResponseEnvelope::Failure {
            engine_protocol, ..
        } => engine_protocol,
    };
    if protocol != cymule_runtime::ENGINE_PROTOCOL_VERSION {
        return Err(unsupported_engine_protocol_failure(request, protocol));
    }
    let (echoed, response) = match envelope {
        success @ EngineResponseEnvelope::Success { .. } => success
            .into_result()
            .map_err(|error| invalid_success_response(request, &error.to_string()))?,
        EngineResponseEnvelope::Failure { error, .. } => {
            error.verify().map_err(|validation| {
                invalid_typed_response(
                    request_is_mutating(request),
                    "failure response",
                    &validation.to_string(),
                )
            })?;
            return Err(error);
        }
    };
    if echoed != *sent_inner {
        return Err(invalid_success_response(
            request,
            "success response request echo does not match the exact submitted request",
        ));
    }
    response
        .verify_for(request)
        .map_err(|error| invalid_success_response(request, &error))?;
    Ok(response)
}

fn admit_raw_engine_response(
    request: &EngineRequest,
    sent_inner: &Value,
    raw_envelope: &Value,
) -> EngineResult<EngineResponse> {
    if let Some(protocol) = raw_envelope
        .as_object()
        .and_then(|envelope| envelope.get("engine_protocol"))
        .and_then(Value::as_str)
        && protocol != cymule_runtime::ENGINE_PROTOCOL_VERSION
    {
        return Err(unsupported_engine_protocol_failure(request, protocol));
    }
    let mut normalized_raw = raw_envelope.clone();
    normalize_mathematical_integers(&mut normalized_raw);
    let envelope: EngineResponseEnvelope<Value, EngineResponse> =
        serde_json::from_value(normalized_raw.clone()).map_err(|error| {
            invalid_typed_response(
                request_is_mutating(request),
                "response envelope",
                &error.to_string(),
            )
        })?;
    let normalized_envelope = serde_json::to_value(&envelope).map_err(|error| {
        invalid_typed_response(
            request_is_mutating(request),
            "response envelope",
            &error.to_string(),
        )
    })?;
    validate_json_typed_roundtrip(&normalized_raw, &normalized_envelope).map_err(|error| {
        invalid_typed_response(request_is_mutating(request), "response envelope", &error)
    })?;
    admit_engine_response(request, sent_inner, envelope)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EngineResponse {
    Sealed { plan: SealedPlan },
    SealedResource { resource: ResourceHandle },
    VerifiedWaitActivation { activation: WaitActivation },
    VerifiedDurableCommand { command: DurableCommand },
    ClockObserved { result: ClockObservationResult },
    VerifiedEvolutionCommand { command: EvolutionCommand },
    VerifiedLiveEvolutionCommand { command: LiveEvolutionCommand },
    ExecutionBoundary { execution: ExecutionOutcome },
    DurableExecuted { response: DurableResponse },
    LiveEvolutionExecuted { commit: Box<EvolutionCommit> },
    Verified,
}

impl EngineResponse {
    fn verify_for(&self, request: &EngineRequest) -> Result<(), String> {
        match (request, self) {
            (EngineRequest::Seal { candidate }, Self::Sealed { plan }) => {
                plan.verify().map_err(|error| error.to_string())?;
                verify_exact_payload("sealed Plan candidate", candidate, &plan.candidate)
            }
            (EngineRequest::SealResource { candidate }, Self::SealedResource { resource }) => {
                resource.verify().map_err(|error| error.to_string())?;
                verify_resource_candidate(candidate, resource)
            }
            (
                EngineRequest::VerifyWaitActivation {
                    activation: requested,
                },
                Self::VerifiedWaitActivation { activation },
            ) => {
                activation.verify().map_err(|error| error.to_string())?;
                verify_exact_payload("wait activation", requested, activation)
            }
            (
                EngineRequest::VerifyDurableCommand { command: requested },
                Self::VerifiedDurableCommand { command },
            ) => {
                command.verify().map_err(|error| error.to_string())?;
                verify_exact_payload("durable command", requested, command)
            }
            (EngineRequest::ObserveClock { target, run_id }, Self::ClockObserved { result }) => {
                verify_clock_observation(target, run_id, result)
            }
            (
                EngineRequest::VerifyEvolutionCommand { command: requested },
                Self::VerifiedEvolutionCommand { command },
            ) => {
                command.verify().map_err(|error| error.to_string())?;
                verify_exact_payload("evolution command", requested, command)
            }
            (
                EngineRequest::VerifyLiveEvolutionCommand { command: requested },
                Self::VerifiedLiveEvolutionCommand { command },
            ) => {
                command.verify().map_err(|error| error.to_string())?;
                verify_exact_payload("live-evolution command", requested, command)
            }
            (EngineRequest::ExecuteDurable { command, .. }, Self::DurableExecuted { response }) => {
                verify_durable_response(command, response)
            }
            (
                EngineRequest::ExecuteLiveEvolution {
                    evolution_id,
                    command,
                    ..
                },
                Self::LiveEvolutionExecuted { commit },
            ) => verify_evolution_commit(evolution_id, command, commit),
            (EngineRequest::Run { plan, run_id, .. }, Self::ExecutionBoundary { execution }) => {
                plan.verify().map_err(|error| error.to_string())?;
                verify_execution_outcome(execution, plan, run_id)
            }
            _ => Err(format!(
                "success response does not match request: received {self:?}"
            )),
        }
    }
}

fn verify_resource_candidate(
    candidate: &ResourceCandidate,
    resource: &ResourceHandle,
) -> Result<(), String> {
    if candidate.resource_version != resource.resource_version
        || candidate.shape != resource.shape
        || candidate.media_type != resource.media_type
        || candidate.inline != resource.inline
        || candidate.integrity != resource.integrity
        || candidate.manifest != resource.manifest
        || candidate.annotations != resource.annotations
    {
        return Err("sealed Resource descriptor does not match the requested candidate".to_owned());
    }
    Ok(())
}

fn verify_exact_payload<T: PartialEq>(
    kind: &str,
    requested: &T,
    returned: &T,
) -> Result<(), String> {
    if returned != requested {
        return Err(format!(
            "verified {kind} payload does not match the requested object"
        ));
    }
    Ok(())
}

fn verify_durable_response(
    command: &DurableCommand,
    response: &DurableResponse,
) -> Result<(), String> {
    command.verify().map_err(|error| error.to_string())?;
    response.verify_wire().map_err(|error| error.to_string())?;
    if command.is_read_only() {
        return response
            .verify_query_for(command)
            .map_err(|error| error.to_string());
    }
    match (command, response) {
        (DurableCommand::StartRun { run_id, .. }, DurableResponse::RunBoundary { boundary }) => {
            verify_durable_boundary_run(boundary, run_id, None)
        }
        (
            DurableCommand::ResumeRun { run_id, .. } | DurableCommand::TakeoverRun { run_id, .. },
            DurableResponse::RunBoundary { boundary },
        ) => verify_durable_boundary_run(boundary, run_id, None),
        (
            DurableCommand::CancelRun {
                cancellation_id,
                run_id,
                reason,
                ..
            },
            DurableResponse::RunCancelled { receipt },
        ) if receipt.command
            == (CancellationCommand {
                cancellation_id: cancellation_id.clone(),
                run_id: run_id.clone(),
                reason: reason.clone(),
            }) =>
        {
            Ok(())
        }
        (
            DurableCommand::ResolveEffect {
                resolution_id,
                run_id,
                intent_id,
                execution_binding,
                occurrence_binding,
                claim_owner,
                claim_epoch,
                resolution,
                value,
                ..
            },
            DurableResponse::EffectResolved { receipt },
        ) if receipt.command
            == (EffectResolutionCommand {
                resolution_id: resolution_id.clone(),
                run_id: run_id.clone(),
                intent_id: intent_id.clone(),
                execution_binding: execution_binding.clone(),
                occurrence_binding: occurrence_binding.clone(),
                claim_owner: claim_owner.clone(),
                claim_epoch: *claim_epoch,
                resolution: *resolution,
                value: value.clone(),
            }) =>
        {
            Ok(())
        }
        (
            DurableCommand::ReleaseEffect { intent_id, .. },
            DurableResponse::RunBoundary { boundary },
        ) if match boundary {
            DurableBoundary::ReconciliationRequired {
                intent_id: returned,
            }
            | DurableBoundary::EffectUnavailable {
                intent_id: returned,
            }
            | DurableBoundary::EffectNotApplied {
                intent_id: returned,
            } => returned == intent_id,
            DurableBoundary::ReleaseRequired { intent_ids } => intent_ids.contains(intent_id),
            _ => true,
        } =>
        {
            Ok(())
        }
        (
            DurableCommand::ActivateWait {
                activation_id,
                source,
                wait_ids,
                ..
            },
            DurableResponse::WaitActivated { receipt },
        ) if receipt.activation.activation_id == *activation_id
            && receipt.activation.source == *source
            && receipt.activation.wait_ids == *wait_ids =>
        {
            Ok(())
        }
        _ => Err("durable response variant does not match its command".to_owned()),
    }
}

fn verify_durable_boundary_run(
    boundary: &DurableBoundary,
    run_id: &str,
    plan_id: Option<&str>,
) -> Result<(), String> {
    if let DurableBoundary::Completed { result } = boundary {
        verify_completed_result(result, run_id, plan_id)?;
    }
    Ok(())
}

fn verify_evolution_commit(
    evolution_id: &str,
    command: &LiveEvolutionCommand,
    commit: &EvolutionCommit,
) -> Result<(), String> {
    commit
        .verify_for(&commit.receipt.command)
        .map_err(|error| error.to_string())?;
    if commit.receipt.command.evolution_id != evolution_id
        || commit.receipt.command.command != *command
    {
        return Err(
            "evolution commit does not match the requested authority and command".to_owned(),
        );
    }
    Ok(())
}

fn verify_live_evolution_preflight(command: &LiveEvolutionCommand) -> EngineResult<()> {
    command.verify().map_err(|error| {
        if let cymule_evolution::EvolutionError::Contract(contract) = &error {
            return EngineFailure::from_contract_violation(
                contract,
                EnginePhase::VerifyLiveEvolutionCommand,
            );
        }
        let mut failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::VerifyLiveEvolutionCommand,
            "evolution_command_validation_failed",
            error.to_string(),
        );
        failure.retry_disposition = Some(cymule_runtime::EngineRetryDisposition::CorrectAndRetry);
        failure
    })
}

fn verify_evolution_target_preflight(
    target: &EngineEvolutionTarget,
    command: &LiveEvolutionCommand,
) -> EngineResult<()> {
    target.verify()?;
    let valid = match command {
        LiveEvolutionCommand::Apply { command, .. } => match command.as_ref() {
            EvolutionCommand::Migrate { request, .. } => {
                target.shadow_driver.is_none()
                    && match (
                        target.target_execution_bindings.get(&request.to_plan),
                        target.migration_adapter.as_ref(),
                    ) {
                        (None, None) => target.target_execution_bindings.is_empty(),
                        (Some(_), Some(adapter)) => {
                            target.target_execution_bindings.len() == 1
                                && adapter.adapter_id == request.adapter_id
                                && adapter.adapter_revision == request.adapter_revision
                                && adapter.process.revision.as_deref()
                                    == Some(request.adapter_revision.as_str())
                        }
                        _ => false,
                    }
            }
            EvolutionCommand::Shadow { request, .. } => {
                target.migration_adapter.is_none()
                    && target.target_execution_bindings.is_empty()
                    && target.shadow_driver.as_ref().is_none_or(|driver| {
                        driver.driver_id == request.driver_id
                            && driver.driver_revision == request.driver_revision
                            && driver.process.revision.as_deref()
                                == Some(request.driver_revision.as_str())
                    })
            }
            _ => {
                target.migration_adapter.is_none()
                    && target.shadow_driver.is_none()
                    && target.target_execution_bindings.is_empty()
            }
        },
        _ => {
            target.migration_adapter.is_none()
                && target.shadow_driver.is_none()
                && target.target_execution_bindings.is_empty()
        }
    };
    if valid {
        return Ok(());
    }
    let mut failure = EngineFailure::new(
        EngineFailureCategory::Validation,
        EnginePhase::ExecuteLiveEvolution,
        "evolution_target_validation_failed",
        "live-evolution provider target does not match the semantic command",
    );
    failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
    Err(failure)
}

fn verify_durable_preflight(command: &DurableCommand) -> EngineResult<()> {
    command.verify().map_err(|error| {
        if let cymule_durable::DurableError::Contract(contract) = &error {
            return EngineFailure::from_contract_violation(
                contract,
                EnginePhase::VerifyDurableCommand,
            );
        }
        let mut failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::VerifyDurableCommand,
            "durable_command_validation_failed",
            error.to_string(),
        );
        failure.retry_disposition = Some(cymule_runtime::EngineRetryDisposition::CorrectAndRetry);
        failure
    })
}

fn verify_clock_observation(
    target: &EngineClockTarget,
    run_id: &str,
    result: &ClockObservationResult,
) -> Result<(), String> {
    result.verify().map_err(|error| error.to_string())?;
    if result.run_id != run_id
        || result.observation.source_id != target.source_id
        || result.observation.source_generation != target.source_generation
    {
        return Err("Clock observation does not match the requested authority".to_owned());
    }
    Ok(())
}

fn verify_execution_outcome(
    outcome: &ExecutionOutcome,
    plan: &SealedPlan,
    run_id: &str,
) -> Result<(), String> {
    let plan_id = plan.plan_id.as_str();
    match outcome {
        ExecutionOutcome::Completed { result } => {
            verify_completed_result(result, run_id, Some(plan_id))?;
            cymule_runtime::PlanContracts::compile(&plan.candidate)
                .map_err(|error| error.to_string())?
                .validate_definition_output(&plan.candidate.entry, &result.value)
                .map_err(|error| error.to_string())?;
        }
        ExecutionOutcome::Suspended { suspension } => {
            if suspension.run_id != run_id
                || suspension.plan_id != plan_id
                || ![
                    suspension.definition_id.as_str(),
                    suspension.site_id.as_str(),
                ]
                .into_iter()
                .all(valid_wire_identity)
                || !is_content_id(&suspension.invocation_id)
                || suspension
                    .result_bind
                    .as_deref()
                    .is_some_and(|binding| !valid_wire_identity(binding))
            {
                return Err("suspended execution boundary is malformed".to_owned());
            }
            let definition = plan
                .candidate
                .definitions
                .iter()
                .find(|definition| definition.id == suspension.definition_id)
                .ok_or_else(|| "suspended definition is absent from the Plan".to_owned())?;
            let step = find_step(&definition.body, &suspension.site_id)
                .ok_or_else(|| "suspended wait site is absent from the Plan".to_owned())?;
            match &step.operation {
                cymule_core::Operation::Wait { wait, bind }
                    if wait == &suspension.wait && bind == &suspension.result_bind => {}
                _ => return Err("suspended boundary does not match its Plan wait site".to_owned()),
            }
            let wait_identity = match &suspension.wait {
                cymule_core::WaitSpec::Signal { key, .. } => key,
                cymule_core::WaitSpec::Timer { timer_id } => timer_id,
                cymule_core::WaitSpec::Input { correlation, .. } => correlation,
            };
            if !valid_wire_identity(wait_identity) {
                return Err("suspended execution wait is malformed".to_owned());
            }
        }
        ExecutionOutcome::ReleaseRequired { release } => {
            if release.run_id != run_id
                || release.plan_id != plan_id
                || release.intent_ids.is_empty()
                || !strictly_ordered_content_ids(&release.intent_ids)
            {
                return Err("release-required execution boundary is malformed".to_owned());
            }
        }
        ExecutionOutcome::ReconciliationRequired { reconciliation } => {
            if reconciliation.run_id != run_id
                || reconciliation.plan_id != plan_id
                || !is_content_id(&reconciliation.intent_id)
            {
                return Err("reconciliation execution boundary is malformed".to_owned());
            }
        }
    }
    Ok(())
}

fn verify_completed_result(
    result: &cymule_runtime::ExecutionResult,
    run_id: &str,
    plan_id: Option<&str>,
) -> Result<(), String> {
    if result.run_id != run_id
        || !is_content_id(&result.plan_id)
        || plan_id.is_some_and(|expected| result.plan_id != expected)
        || !is_digest(&result.projection_digest)
        || !is_precondition_token(&result.precondition_token)
        || !strictly_ordered_content_ids(&result.effects)
    {
        return Err("completed execution boundary is malformed".to_owned());
    }
    Ok(())
}

fn is_precondition_token(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("pre:") else {
        return false;
    };
    let Some((epoch, event_id)) = rest.split_once(':') else {
        return false;
    };
    let Ok(epoch_value) = epoch.parse::<u64>() else {
        return false;
    };
    epoch_value <= cymule_core::MAX_EXACT_INTEGER
        && epoch == epoch_value.to_string()
        && is_content_id(event_id)
}

fn find_step<'a>(region: &'a cymule_core::Region, site_id: &str) -> Option<&'a cymule_core::Step> {
    for step in &region.steps {
        if step.id == site_id {
            return Some(step);
        }
        if let cymule_core::Operation::Scope { body, .. } = &step.operation
            && let Some(found) = find_step(body, site_id)
        {
            return Some(found);
        }
    }
    None
}

fn strictly_ordered_content_ids(values: &[String]) -> bool {
    values.iter().all(|value| is_content_id(value))
        && values.windows(2).all(|pair| pair[0] < pair[1])
}

fn is_content_id(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(is_digest)
}

fn valid_wire_identity(value: &str) -> bool {
    !value.is_empty() && value.chars().count() <= 512 && !value.chars().any(char::is_control)
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// High-level provider-neutral durable Run client over an Engine transport.
#[derive(Debug, Clone)]
pub struct DurableEngine<E = CliEngine> {
    transport: E,
    store: EngineStoreTarget,
    executor: Option<EnginePluginTarget>,
    clock: Option<EngineClockTarget>,
    migration_adapter: Option<EngineMigrationProviderTarget>,
    shadow_driver: Option<EngineShadowProviderTarget>,
    target_execution_bindings: std::collections::BTreeMap<String, EnginePluginTarget>,
    evolution_id: String,
}

impl DurableEngine<CliEngine> {
    /// Bind one CLI transport, directory domain, and complete process executor target.
    pub fn new(
        executable: impl AsRef<Path>,
        store: impl AsRef<Path>,
        executor: EnginePluginTarget,
        clock: EngineClockTarget,
    ) -> Self {
        Self {
            transport: CliEngine::new(executable),
            store: EngineStoreTarget::directory(store.as_ref().display().to_string()),
            executor: Some(executor),
            clock: Some(clock),
            migration_adapter: None,
            shadow_driver: None,
            target_execution_bindings: std::collections::BTreeMap::new(),
            evolution_id: "cymule.sdk.live-evolution".to_owned(),
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
            clock: None,
            migration_adapter: None,
            shadow_driver: None,
            target_execution_bindings: std::collections::BTreeMap::new(),
            evolution_id: "cymule.sdk.live-evolution".to_owned(),
        }
    }

    /// Select the execution provider used by mutating Run commands.
    #[must_use]
    pub fn with_executor(mut self, executor: EnginePluginTarget) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Select the exact persistence-backed Clock authority used by mutations.
    #[must_use]
    pub fn with_clock(mut self, clock: EngineClockTarget) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Select one exact migration adapter.
    #[must_use]
    pub fn with_migration_adapter(mut self, adapter: EngineMigrationProviderTarget) -> Self {
        self.migration_adapter = Some(adapter);
        self
    }

    /// Select one exact shadow driver.
    #[must_use]
    pub fn with_shadow_driver(mut self, driver: EngineShadowProviderTarget) -> Self {
        self.shadow_driver = Some(driver);
        self
    }

    /// Bind one exact target Plan to its revision-pinned ordinary executor.
    #[must_use]
    pub fn with_target_execution_binding(
        mut self,
        plan_id: impl Into<String>,
        target: EnginePluginTarget,
    ) -> Self {
        self.target_execution_bindings
            .insert(plan_id.into(), target);
        self
    }

    /// Select the durable live-evolution authority identity.
    #[must_use]
    pub fn with_evolution_id(mut self, evolution_id: impl Into<String>) -> Self {
        self.evolution_id = evolution_id.into();
        self
    }

    /// Issue one exact retained Clock reference for a later execution command.
    ///
    /// # Errors
    /// Returns a missing/invalid Clock configuration, issuance failure, or a
    /// response that does not match the selected Clock authority.
    pub fn observe_clock(&self, run_id: &str) -> EngineResult<ClockObservationRef> {
        let clock = self.clock.as_ref().ok_or_else(|| {
            let mut failure = EngineFailure::new(
                EngineFailureCategory::Validation,
                EnginePhase::ObserveClock,
                "missing_clock_provider",
                "durable Clock observation requires an exact Clock target",
            );
            failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
            failure
        })?;
        if !valid_wire_identity(run_id) {
            return Err(facade_validation_failure(
                EnginePhase::ObserveClock,
                "invalid_run_identity",
                "Clock observation Run identity must contain 1..=512 non-control Unicode scalars",
            ));
        }
        clock.verify()?;
        let result = self.transport.observe_clock(clock, run_id)?;
        verify_clock_observation(clock, run_id, &result)
            .map_err(|error| invalid_typed_response(true, "Clock observation reference", &error))?;
        Ok(result.observation)
    }

    /// Start an idempotent Run and drive it to the next durable boundary.
    ///
    /// # Errors
    /// Returns invalid candidate/execution input, rejected admission, or an
    /// invalid/lost response. Uncertain mutations require reconciliation.
    pub fn start(
        &self,
        run_id: impl Into<String>,
        candidate: PlanCandidate,
        input: Value,
        execution: ExecutionClaimRequest,
    ) -> EngineResult<DurableResponse> {
        let run_id = run_id.into();
        self.require_durable_capabilities(true, true)?;
        let plan = self.transport.seal(&candidate)?;
        let response = self.submit(&DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.clone(),
            candidate,
            input,
            execution,
        })?;
        let DurableResponse::RunBoundary { boundary } = &response else {
            return Err(unexpected_durable_response("start boundary", &response));
        };
        verify_durable_boundary_run(boundary, &run_id, Some(&plan.plan_id))
            .map_err(|error| invalid_typed_response(true, "durable Start response", &error))?;
        Ok(response)
    }

    /// Read one revision-pinned page of the domain Run index.
    ///
    /// # Errors
    /// Returns invalid query bounds/cursors, unavailable revision, or malformed
    /// and failed Engine read responses.
    pub fn run_index_page(
        &self,
        options: DurablePageQueryOptions,
    ) -> EngineResult<DurableRunIndexPage> {
        match self.submit(&DurableCommand::RunIndexPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            expected_revision: options.expected_revision,
            cursor: options.cursor,
            limit: options.limit,
            max_canonical_bytes: options.max_canonical_bytes,
        })? {
            DurableResponse::RunIndexPage { page } => Ok(page),
            response => Err(unexpected_durable_response("Run-index page", &response)),
        }
    }

    /// Read one Run's bounded semantic current projection.
    ///
    /// # Errors
    /// Returns invalid Run/revision input or malformed and failed Engine reads.
    pub fn run_current(
        &self,
        run_id: impl Into<String>,
        expected_revision: Option<String>,
    ) -> EngineResult<DurableRunCurrentRead> {
        match self.submit(&DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.into(),
            expected_revision,
        })? {
            DurableResponse::RunCurrent {
                observed_revision,
                source_root,
                current,
            } => Ok(DurableRunCurrentRead {
                observed_revision,
                source_root,
                current,
            }),
            response => Err(unexpected_durable_response("Run-current", &response)),
        }
    }

    /// Read one revision-pinned page of a Run's waits.
    ///
    /// # Errors
    /// Returns invalid Run/query bounds, stale cursors/revisions, or malformed
    /// and failed Engine read responses.
    pub fn run_wait_page(
        &self,
        run_id: impl Into<String>,
        options: DurablePageQueryOptions,
    ) -> EngineResult<DurableRunWaitPage> {
        let run_id = run_id.into();
        match self.submit(&DurableCommand::RunWaitPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id,
            expected_revision: options.expected_revision,
            cursor: options.cursor,
            limit: options.limit,
            max_canonical_bytes: options.max_canonical_bytes,
        })? {
            DurableResponse::RunWaitPage { page, .. } => Ok(page),
            response => Err(unexpected_durable_response("Run-wait page", &response)),
        }
    }

    /// Read one revision-pinned page of a Run's Effects.
    ///
    /// # Errors
    /// Returns invalid Run/query bounds, stale cursors/revisions, or malformed
    /// and failed Engine read responses.
    pub fn run_effect_page(
        &self,
        run_id: impl Into<String>,
        options: DurablePageQueryOptions,
    ) -> EngineResult<DurableRunEffectPage> {
        let run_id = run_id.into();
        match self.submit(&DurableCommand::RunEffectPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id,
            expected_revision: options.expected_revision,
            cursor: options.cursor,
            limit: options.limit,
            max_canonical_bytes: options.max_canonical_bytes,
        })? {
            DurableResponse::RunEffectPage { page, .. } => Ok(page),
            response => Err(unexpected_durable_response("Run-Effect page", &response)),
        }
    }

    /// Read one revision-pinned page of a Run's component occurrences.
    ///
    /// # Errors
    /// Returns invalid Run/query bounds, stale cursors/revisions, or malformed
    /// and failed Engine read responses.
    pub fn run_occurrence_page(
        &self,
        run_id: impl Into<String>,
        options: DurablePageQueryOptions,
    ) -> EngineResult<DurableRunOccurrencePage> {
        let run_id = run_id.into();
        match self.submit(&DurableCommand::RunOccurrencePage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id,
            expected_revision: options.expected_revision,
            cursor: options.cursor,
            limit: options.limit,
            max_canonical_bytes: options.max_canonical_bytes,
        })? {
            DurableResponse::RunOccurrencePage { page, .. } => Ok(page),
            response => Err(unexpected_durable_response(
                "Run-occurrence page",
                &response,
            )),
        }
    }

    /// Read one revision-pinned page of a Run's provider Attempts.
    ///
    /// # Errors
    /// Returns invalid Run/query bounds, stale cursors/revisions, or malformed
    /// and failed Engine read responses.
    pub fn run_attempt_page(
        &self,
        run_id: impl Into<String>,
        options: DurablePageQueryOptions,
    ) -> EngineResult<DurableRunAttemptPage> {
        let run_id = run_id.into();
        match self.submit(&DurableCommand::RunAttemptPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id,
            expected_revision: options.expected_revision,
            cursor: options.cursor,
            limit: options.limit,
            max_canonical_bytes: options.max_canonical_bytes,
        })? {
            DurableResponse::RunAttemptPage { page, .. } => Ok(page),
            response => Err(unexpected_durable_response("Run-Attempt page", &response)),
        }
    }

    /// Read one complete Run-owned typed leaf by exact identity.
    ///
    /// # Errors
    /// Returns invalid selector/budget input, unavailable revision, or malformed
    /// and failed Engine reads; an authenticated absent item is not an error.
    pub fn run_item(&self, query: DurableRunItemQuery) -> EngineResult<DurableRunItemRead> {
        match self.submit(&DurableCommand::RunItem {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: query.run_id,
            expected_revision: query.expected_revision,
            selector: query.selector,
            max_canonical_bytes: query.max_canonical_bytes,
        })? {
            DurableResponse::RunItem {
                run_id,
                observed_revision,
                source_root,
                item,
            } => Ok(DurableRunItemRead {
                run_id,
                observed_revision,
                source_root,
                item,
            }),
            response => Err(unexpected_durable_response("Run item", &response)),
        }
    }

    /// Resume one ready Run.
    ///
    /// # Errors
    /// Returns invalid execution input, rejected resume admission, or invalid/lost
    /// mutation responses with the Engine's exact recovery disposition.
    pub fn resume(
        &self,
        run_id: impl Into<String>,
        execution: ExecutionClaimRequest,
    ) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.into(),
            execution,
        })
    }

    /// Explicitly take over one expired persisted Running Run.
    ///
    /// # Errors
    /// Returns invalid execution input, stale fences or rejected takeover,
    /// and invalid/lost mutation responses without inferring retry safety.
    pub fn takeover(
        &self,
        run_id: impl Into<String>,
        expected_fence: u64,
        execution: ExecutionClaimRequest,
    ) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::TakeoverRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.into(),
            expected_fence,
            execution,
        })
    }

    /// Admit one identified signal delivery without selecting targets locally.
    ///
    /// # Errors
    /// Returns invalid activation input, rejected admission, or invalid/lost
    /// mutation responses with the Engine's exact recovery disposition.
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
    ///
    /// # Errors
    /// Returns invalid intent/execution input, rejected release, or invalid/lost
    /// mutation responses with the Engine's exact recovery disposition.
    pub fn release(
        &self,
        intent_id: impl Into<String>,
        execution: ExecutionClaimRequest,
    ) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::ReleaseEffect {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            intent_id: intent_id.into(),
            execution,
        })
    }

    /// Terminally resolve one retained unknown-world Effect under its exact
    /// historical binding and dispatch fence, without provider redispatch.
    ///
    /// # Errors
    /// Returns invalid resolution input, stale binding/fence or rejected
    /// resolution, and invalid/lost mutation responses requiring reconciliation.
    pub fn resolve_effect(
        &self,
        command: EffectResolutionCommand,
    ) -> EngineResult<DurableResponse> {
        let EffectResolutionCommand {
            resolution_id,
            run_id,
            intent_id,
            execution_binding,
            occurrence_binding,
            claim_owner,
            claim_epoch,
            resolution,
            value,
        } = command;
        self.submit(&DurableCommand::ResolveEffect {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            resolution_id,
            run_id,
            intent_id,
            execution_binding,
            occurrence_binding,
            claim_owner,
            claim_epoch,
            resolution,
            value,
        })
    }

    /// Cancel one Run without requiring a live execution provider.
    ///
    /// # Errors
    /// Returns invalid cancellation input, rejected admission, or invalid/lost
    /// mutation responses with the Engine's exact recovery disposition.
    pub fn cancel(
        &self,
        cancellation_id: impl Into<String>,
        run_id: impl Into<String>,
        reason: Value,
    ) -> EngineResult<DurableResponse> {
        self.submit(&DurableCommand::CancelRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            cancellation_id: cancellation_id.into(),
            run_id: run_id.into(),
            reason,
        })
    }

    /// Submit one atomic live-evolution command and return its semantic commit.
    ///
    /// # Errors
    /// Returns invalid M4/provider input, rejected admission, or an invalid/lost
    /// commit. A mismatched commit is an uncertain mutation, not permission to retry.
    pub fn evolve(&self, command: &LiveEvolutionCommand) -> EngineResult<EvolutionCommit> {
        verify_live_evolution_preflight(command)?;
        if self.evolution_id.is_empty()
            || self.evolution_id.chars().count() > 256
            || self.evolution_id.chars().any(char::is_control)
        {
            return Err(facade_validation_failure(
                EnginePhase::ExecuteLiveEvolution,
                "invalid_evolution_identity",
                "evolution identity must contain 1..=256 non-control Unicode scalars",
            ));
        }
        let target = self.evolution_target(command);
        verify_evolution_target_preflight(&target, command)?;
        let commit = self
            .transport
            .execute_live_evolution(&target, &self.evolution_id, command)?;
        verify_evolution_commit(&self.evolution_id, command, &commit)
            .map_err(|error| invalid_typed_response(true, "live-evolution commit", &error))?;
        Ok(commit)
    }

    fn evolution_target(&self, command: &LiveEvolutionCommand) -> EngineEvolutionTarget {
        let mut target_execution_bindings = std::collections::BTreeMap::new();
        let (migration_adapter, shadow_driver) = match command {
            LiveEvolutionCommand::Apply { command, .. } => match command.as_ref() {
                EvolutionCommand::Migrate { request, .. } => {
                    if let Some(target) = self.target_execution_bindings.get(&request.to_plan) {
                        target_execution_bindings.insert(request.to_plan.clone(), target.clone());
                    }
                    (self.migration_adapter.clone(), None)
                }
                EvolutionCommand::Shadow { .. } => (None, self.shadow_driver.clone()),
                _ => (None, None),
            },
            _ => (None, None),
        };
        EngineEvolutionTarget {
            store: self.store.clone(),
            migration_adapter,
            shadow_driver,
            target_execution_bindings,
        }
    }

    fn submit(&self, command: &DurableCommand) -> EngineResult<DurableResponse> {
        verify_durable_preflight(command)?;
        self.require_durable_capabilities(command.requires_executor(), command.requires_clock())?;
        let target = EngineDurableTarget {
            store: self.store.clone(),
            executor: command
                .requires_executor()
                .then(|| self.executor.clone())
                .flatten(),
            clock: command
                .requires_clock()
                .then(|| self.clock.clone())
                .flatten(),
        };
        target.verify()?;
        let response = self.transport.execute_durable(&target, command)?;
        verify_durable_response(command, &response).map_err(|error| {
            invalid_typed_response(
                durable_command_is_mutating(command),
                "durable response",
                &error,
            )
        })?;
        Ok(response)
    }

    fn require_durable_capabilities(
        &self,
        requires_executor: bool,
        requires_clock: bool,
    ) -> EngineResult<()> {
        if requires_executor && self.executor.is_none() {
            return Err(missing_durable_capability(
                "missing_execution_provider",
                "durable command requires an execution provider",
            ));
        }
        if requires_clock && self.clock.is_none() {
            return Err(missing_durable_capability(
                "missing_clock_provider",
                "durable execution requires a persistence-backed Clock provider",
            ));
        }
        Ok(())
    }
}

fn missing_durable_capability(code: &'static str, message: &'static str) -> EngineFailure {
    let mut failure = EngineFailure::new(
        EngineFailureCategory::Validation,
        EnginePhase::ExecuteDurable,
        code,
        message,
    );
    failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
    failure
}

fn facade_validation_failure(
    phase: EnginePhase,
    code: &'static str,
    message: &'static str,
) -> EngineFailure {
    let mut failure = EngineFailure::new(EngineFailureCategory::Validation, phase, code, message);
    failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
    failure
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

fn invalid_engine_payload(kind: &str, message: &str) -> EngineFailure {
    EngineFailure::new(
        EngineFailureCategory::TransportFailure,
        EnginePhase::Transport,
        "invalid_engine_response",
        format!("Engine returned an invalid {kind}: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_core::ReconciliationResolution;
    use cymule_runtime::EngineProcessConfig;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;

    #[test]
    fn engine_stream_limit_retains_one_exact_overflow_byte() {
        let exact = read_engine_stream(
            std::io::repeat(b'x').take(u64::try_from(ENGINE_STREAM_LIMIT).unwrap()),
        )
        .expect("the exact Engine stream limit must be admitted");
        assert_eq!(exact.len(), ENGINE_STREAM_LIMIT);

        let overflow = u64::try_from(ENGINE_STREAM_LIMIT)
            .unwrap()
            .checked_add(1)
            .unwrap();
        let error = read_engine_stream(std::io::repeat(b'x').take(overflow))
            .expect_err("one byte beyond the Engine stream limit must be rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(error.to_string(), "Engine stream exceeded the byte limit");
    }

    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures")
            .join(name)
    }

    fn empty_candidate() -> PlanCandidate {
        PlanCandidate {
            ir_version: "cymule.ir/3".to_owned(),
            name: "transport".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![cymule_core::Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: cymule_core::Region {
                    steps: Vec::new(),
                    result: cymule_core::Expression::Literal { value: Value::Null },
                },
            }],
            metadata: std::collections::BTreeMap::default(),
        }
    }

    fn clock_target() -> EngineClockTarget {
        EngineClockTarget::sqlite(
            "clock",
            "clock:sdk-client-test",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
    }

    fn process_target(executable: impl Into<String>) -> EnginePluginTarget {
        let executable = PathBuf::from(executable.into());
        let executable = if executable.is_absolute() {
            executable
        } else {
            std::env::current_dir()
                .expect("test process current directory resolves")
                .join(executable)
        };
        let effect_ledger = executable.with_extension("effect-ledger.sqlite3");
        EnginePluginTarget::process(EngineProcessConfig {
            executable: executable.to_string_lossy().into_owned(),
            arguments: Vec::new(),
            environment: std::collections::BTreeMap::from([(
                "CYMULE_TEST_EFFECT_LEDGER_PATH".to_owned(),
                effect_ledger.to_string_lossy().into_owned(),
            )]),
            working_directory: None,
            runtime_closure: std::collections::BTreeMap::from([(
                "component-runtime".to_owned(),
                format!("sha256:{}", "a".repeat(64)),
            )]),
            timeout_ms: 60_000,
            message_limit: 8 * 1024 * 1024,
            closure_limit: 64 * 1024 * 1024,
        })
    }

    fn execution() -> ExecutionClaimRequest {
        ExecutionClaimRequest {
            owner: "driver:sdk-client-test".to_owned(),
            clock: ClockObservationRef {
                clock_version: crate::CLOCK_OBSERVATION_VERSION.to_owned(),
                observation_id:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                source_id: "clock:sdk-client-test".to_owned(),
                source_generation:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                scope: "scope:sdk-client-test".to_owned(),
            },
            ttl: 10,
        }
    }

    fn semantic_artifact(kind: &str, value: &str) -> cymule_core::ArtifactRef {
        cymule_core::artifact_ref(kind, value.as_bytes()).expect("test Artifact reference derives")
    }

    fn execution_binding_ref(value: &str) -> cymule_core::ArtifactRef {
        semantic_artifact(cymule_runtime::EXECUTION_BINDING_VERSION, value)
    }

    fn reusable_definition(version: &str) -> cymule_core::Definition {
        cymule_core::Definition {
            id: "review".to_owned(),
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            body: cymule_core::Region {
                steps: Vec::new(),
                result: cymule_core::Expression::Literal {
                    value: serde_json::json!({"version": version}),
                },
            },
        }
    }

    fn versioned_plan(version: &str) -> SealedPlan {
        let mut candidate = empty_candidate();
        candidate.name = format!("receipt_{version}");
        candidate.definitions[0].body.result = cymule_core::Expression::Literal {
            value: serde_json::json!({"version": version}),
        };
        candidate
            .metadata
            .insert("version".to_owned(), version.to_owned());
        cymule_core::seal_plan(candidate).expect("test Plan seals")
    }

    fn run_current_fixture(run_id: &str) -> DurableRunCurrent {
        let plan = versioned_plan("query-boundary");
        let execution_binding = execution_binding_ref("query execution binding");
        DurableRunCurrent {
            run_id: run_id.to_owned(),
            plan_id: plan.plan_id,
            execution_binding,
            continuation_status: cymule_durable_protocol::ContinuationStatus::Ready,
            epoch: 0,
            execution_fence: 0,
            result: None,
            execution_status: cymule_core::RunExecutionStatus::Active,
            world_settlement: cymule_core::WorldSettlementStatus::Settled,
        }
    }

    fn query_revision() -> String {
        format!(
            "sha256:{}",
            cymule_core::sha256_bytes(b"query boundary revision")
        )
    }

    fn query_source_root() -> String {
        cymule_core::sha256_bytes(b"query StateRoot")
    }

    fn missing_run_current_response() -> DurableResponse {
        DurableResponse::RunCurrent {
            observed_revision: query_revision(),
            source_root: query_source_root(),
            current: None,
        }
    }

    fn live_command_fixture() -> LiveEvolutionCommand {
        LiveEvolutionCommand::PublishDefinition {
            control_version: cymule_evolution::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:definition:wire".to_owned(),
            logical_ref: "subflow:review".to_owned(),
            definition: reusable_definition("v1"),
            references: Vec::new(),
        }
    }

    fn migration_command_fixture() -> LiveEvolutionCommand {
        let command = LiveEvolutionCommand::Apply {
            control_version: cymule_evolution::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:live-migrate:wire".to_owned(),
            template_id: "template:wire".to_owned(),
            command: Box::new(EvolutionCommand::Migrate {
                control_version: cymule_evolution::EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "command:migrate:wire".to_owned(),
                request: Box::new(cymule_evolution::MigrationRequest {
                    migration_id: "migration:wire".to_owned(),
                    run_id: "run:wire".to_owned(),
                    from_plan: format!("sha256:{}", "1".repeat(64)),
                    to_plan: format!("sha256:{}", "2".repeat(64)),
                    plan_edge_id: format!("sha256:{}", "3".repeat(64)),
                    compatibility_id: format!("sha256:{}", "4".repeat(64)),
                    expected_source_epoch: 0,
                    adapter_id: "adapter:wire".to_owned(),
                    adapter_revision: format!("sha256:{}", "5".repeat(64)),
                }),
            }),
        };
        command
            .verify()
            .expect("migration command fixture verifies");
        command
    }

    fn shadow_command_fixture() -> LiveEvolutionCommand {
        let command = LiveEvolutionCommand::Apply {
            control_version: cymule_evolution::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:live-shadow:wire".to_owned(),
            template_id: "template:wire".to_owned(),
            command: Box::new(EvolutionCommand::Shadow {
                control_version: cymule_evolution::EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "command:shadow:wire".to_owned(),
                request: cymule_evolution::ShadowRequest {
                    comparison_id: "comparison:wire".to_owned(),
                    decision_id: "decision:wire".to_owned(),
                    subject: "run:wire".to_owned(),
                    primary_plan: format!("sha256:{}", "6".repeat(64)),
                    shadow_plan: format!("sha256:{}", "7".repeat(64)),
                    driver_id: "driver:wire".to_owned(),
                    driver_revision: format!("sha256:{}", "8".repeat(64)),
                    input: semantic_artifact("cymule.input/1", "shadow input"),
                    comparison_policy: "policy:wire".to_owned(),
                },
            }),
        };
        command.verify().expect("shadow command fixture verifies");
        command
    }

    fn evolution_commit_fixture(
        evolution_id: &str,
        command: &LiveEvolutionCommand,
    ) -> EvolutionCommit {
        let persistence =
            cymule_evolution::EvolutionPersistenceCommand::new(evolution_id, command.clone())
                .expect("semantic command seals");
        let mut control =
            cymule_durable::DurableStoreControl::initialize(cymule_durable::MemoryStore::new())
                .expect("public durable authority initializes");
        let commit = control
            .evolution(&mut cymule_evolution::NoEvolutionProviders)
            .commit(&persistence)
            .expect("public Evolution authority resolves reads and commits the fixture");
        commit
            .verify_for(&persistence)
            .expect("test evolution commit is valid");
        commit
    }

    fn live_request(command: LiveEvolutionCommand) -> EngineRequest {
        EngineRequest::ExecuteLiveEvolution {
            target: EngineEvolutionTarget {
                store: EngineStoreTarget::directory("domain"),
                migration_adapter: None,
                shadow_driver: None,
                target_execution_bindings: std::collections::BTreeMap::new(),
            },
            evolution_id: "evolution:wire".to_owned(),
            command,
        }
    }
    #[derive(Clone)]
    struct PreflightProbe {
        invoked: Arc<AtomicBool>,
    }

    impl Engine for PreflightProbe {
        fn seal(&self, _candidate: &PlanCandidate) -> EngineResult<SealedPlan> {
            unreachable!("preflight probe only accepts live evolution")
        }

        fn seal_resource(&self, _candidate: &ResourceCandidate) -> EngineResult<ResourceHandle> {
            unreachable!("preflight probe only accepts live evolution")
        }

        fn verify_wait_activation(
            &self,
            _activation: &WaitActivation,
        ) -> EngineResult<WaitActivation> {
            unreachable!("preflight probe only accepts live evolution")
        }

        fn verify_durable_command(
            &self,
            _command: &DurableCommand,
        ) -> EngineResult<DurableCommand> {
            unreachable!("preflight probe only accepts live evolution")
        }

        fn observe_clock(
            &self,
            _target: &EngineClockTarget,
            _run_id: &str,
        ) -> EngineResult<ClockObservationResult> {
            self.invoked.store(true, Ordering::Release);
            Err(EngineFailure::transport(
                "preflight_probe_invoked",
                "invalid Clock request reached the transport",
            ))
        }

        fn verify_evolution_command(
            &self,
            _command: &EvolutionCommand,
        ) -> EngineResult<EvolutionCommand> {
            unreachable!("preflight probe only accepts live evolution")
        }

        fn verify_live_evolution_command(
            &self,
            _command: &LiveEvolutionCommand,
        ) -> EngineResult<LiveEvolutionCommand> {
            unreachable!("preflight probe only accepts live evolution")
        }

        fn execute_durable(
            &self,
            _target: &EngineDurableTarget,
            _command: &DurableCommand,
        ) -> EngineResult<DurableResponse> {
            self.invoked.store(true, Ordering::Release);
            Err(EngineFailure::transport(
                "preflight_probe_invoked",
                "invalid durable request reached the transport",
            ))
        }

        fn execute_live_evolution(
            &self,
            _target: &EngineEvolutionTarget,
            _evolution_id: &str,
            _command: &LiveEvolutionCommand,
        ) -> EngineResult<EvolutionCommit> {
            self.invoked.store(true, Ordering::Release);
            Err(EngineFailure::transport(
                "preflight_probe_invoked",
                "invalid command reached the mutation transport",
            ))
        }

        fn run(
            &self,
            _plan: &SealedPlan,
            _input: &Value,
            _plugin: &EnginePluginTarget,
            _run_id: &str,
        ) -> EngineResult<ExecutionOutcome> {
            unreachable!("preflight probe only accepts live evolution")
        }
    }

    #[derive(Clone)]
    struct DurableQueryProbe {
        requests: Arc<Mutex<Vec<(EngineDurableTarget, DurableCommand)>>>,
        response: DurableResponse,
    }

    impl Engine for DurableQueryProbe {
        fn seal(&self, candidate: &PlanCandidate) -> EngineResult<SealedPlan> {
            cymule_core::seal_plan(candidate.clone())
                .map_err(|error| local_request_failure("plan_seal_failed", error))
        }

        fn seal_resource(&self, _candidate: &ResourceCandidate) -> EngineResult<ResourceHandle> {
            unreachable!("durable query probe only accepts Run queries")
        }

        fn verify_wait_activation(
            &self,
            _activation: &WaitActivation,
        ) -> EngineResult<WaitActivation> {
            unreachable!("durable query probe only accepts Run queries")
        }

        fn verify_durable_command(&self, command: &DurableCommand) -> EngineResult<DurableCommand> {
            command
                .verify()
                .map_err(|error| local_request_failure("durable_command_invalid", error))?;
            Ok(command.clone())
        }

        fn observe_clock(
            &self,
            _target: &EngineClockTarget,
            _run_id: &str,
        ) -> EngineResult<ClockObservationResult> {
            unreachable!("durable query probe only accepts Run queries")
        }

        fn verify_evolution_command(
            &self,
            _command: &EvolutionCommand,
        ) -> EngineResult<EvolutionCommand> {
            unreachable!("durable query probe only accepts Run queries")
        }

        fn verify_live_evolution_command(
            &self,
            _command: &LiveEvolutionCommand,
        ) -> EngineResult<LiveEvolutionCommand> {
            unreachable!("durable query probe only accepts Run queries")
        }

        fn execute_durable(
            &self,
            target: &EngineDurableTarget,
            command: &DurableCommand,
        ) -> EngineResult<DurableResponse> {
            self.requests
                .lock()
                .expect("query probe request lock remains available")
                .push((target.clone(), command.clone()));
            Ok(self.response.clone())
        }

        fn execute_live_evolution(
            &self,
            _target: &EngineEvolutionTarget,
            _evolution_id: &str,
            _command: &LiveEvolutionCommand,
        ) -> EngineResult<EvolutionCommit> {
            unreachable!("durable query probe only accepts Run queries")
        }

        fn run(
            &self,
            _plan: &SealedPlan,
            _input: &Value,
            _plugin: &EnginePluginTarget,
            _run_id: &str,
        ) -> EngineResult<ExecutionOutcome> {
            unreachable!("durable query probe only accepts Run queries")
        }
    }

    fn assert_local_live_preflight_failure(failure: &EngineFailure) {
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.phase, EnginePhase::VerifyLiveEvolutionCommand);
        assert_eq!(failure.code.as_ref(), "evolution_command_validation_failed");
        assert_eq!(
            failure.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::CorrectAndRetry)
        );
    }

    fn assert_wrong_engine_echo(
        request: &EngineRequest,
        echoed: EngineRequest,
        response: EngineResponse,
        expected_category: EngineFailureCategory,
        expected_retry: Option<cymule_runtime::EngineRetryDisposition>,
    ) {
        let sent_inner = serde_json::to_value(request).expect("submitted request serializes");
        let echoed_inner = serde_json::to_value(echoed).expect("echoed request serializes");
        let failure = admit_engine_response(
            request,
            &sent_inner,
            EngineResponseEnvelope::success(echoed_inner, response),
        )
        .expect_err("wrong Engine request echo is rejected before its payload");
        assert_eq!(failure.category, expected_category);
        assert_eq!(failure.code.as_ref(), "invalid_engine_response");
        assert_eq!(failure.retry_disposition, expected_retry);
        assert!(failure.message.contains("request echo"));
    }

    #[test]
    fn engine_success_requires_exact_request_echo_before_payload_validation() {
        let seal_candidate = empty_candidate();
        let seal = EngineRequest::Seal {
            candidate: seal_candidate.clone(),
        };
        let mut different_candidate = empty_candidate();
        different_candidate.name = "different request".to_owned();
        assert_wrong_engine_echo(
            &seal,
            EngineRequest::Seal {
                candidate: different_candidate,
            },
            EngineResponse::Sealed {
                plan: cymule_core::seal_plan(seal_candidate).expect("echo test Plan seals"),
            },
            EngineFailureCategory::TransportFailure,
            None,
        );

        assert_execution_request_echoes();

        let cancellation_reason = serde_json::json!({"reason": "requested"});
        let cancel_command = DurableCommand::CancelRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            cancellation_id: "cancel:echo".to_owned(),
            run_id: "run:echo".to_owned(),
            reason: cancellation_reason.clone(),
        };
        let cancel = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command: cancel_command.clone(),
        };
        let mut different_cancel = cancel_command;
        let DurableCommand::CancelRun {
            cancellation_id, ..
        } = &mut different_cancel
        else {
            unreachable!("constructed cancellation command")
        };
        *cancellation_id = "cancel:different".to_owned();
        assert_wrong_engine_echo(
            &cancel,
            EngineRequest::ExecuteDurable {
                target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
                command: different_cancel,
            },
            EngineResponse::DurableExecuted {
                response: DurableResponse::RunCancelled {
                    receipt: {
                        let reason_ref = cymule_core::artifact_ref(
                            cymule_durable::CANCELLATION_REASON_ARTIFACT_KIND,
                            &cymule_core::canonical_bytes(&cancellation_reason)
                                .expect("cancellation reason encodes"),
                        )
                        .expect("cancellation reason reference derives");
                        cymule_durable::CancellationReceipt {
                            receipt_version: cymule_durable::RUN_CANCELLATION_RECEIPT_VERSION
                                .to_owned(),
                            command: CancellationCommand {
                                cancellation_id: "cancel:echo".to_owned(),
                                run_id: "run:echo".to_owned(),
                                reason: cancellation_reason,
                            },
                            boundary: DurableBoundary::Cancelled { reason: reason_ref },
                            receipt_id: "4".repeat(64),
                        }
                    },
                },
            },
            EngineFailureCategory::UnknownWorldOutcome,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile),
        );

        let live_command = live_command_fixture();
        let live = live_request(live_command.clone());
        let mut different_live = live_command.clone();
        let LiveEvolutionCommand::PublishDefinition { command_id, .. } = &mut different_live else {
            unreachable!("fixture has a publication command")
        };
        *command_id = "command:different-echo".to_owned();
        let EngineRequest::ExecuteLiveEvolution {
            target,
            evolution_id,
            ..
        } = &live
        else {
            unreachable!("constructed live request")
        };
        assert_wrong_engine_echo(
            &live,
            EngineRequest::ExecuteLiveEvolution {
                target: target.clone(),
                evolution_id: evolution_id.clone(),
                command: different_live,
            },
            EngineResponse::LiveEvolutionExecuted {
                commit: Box::new(evolution_commit_fixture(evolution_id, &live_command)),
            },
            EngineFailureCategory::UnknownWorldOutcome,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile),
        );

        assert_exact_echo_still_validates_response(&seal);
    }

    fn assert_exact_echo_still_validates_response(seal: &EngineRequest) {
        let seal_inner = serde_json::to_value(seal).expect("Seal request serializes");
        let payload_failure = admit_engine_response(
            seal,
            &seal_inner,
            EngineResponseEnvelope::success(seal_inner.clone(), EngineResponse::Verified),
        )
        .expect_err("an exact echo still validates its success payload");
        assert_eq!(
            payload_failure.category,
            EngineFailureCategory::TransportFailure
        );
        assert_eq!(payload_failure.code.as_ref(), "invalid_engine_response");
        assert!(!payload_failure.message.contains("request echo"));
    }

    fn assert_execution_request_echoes() {
        let clock_target = clock_target();
        let clock = EngineRequest::ObserveClock {
            target: clock_target.clone(),
            run_id: "run:echo".to_owned(),
        };
        assert_wrong_engine_echo(
            &clock,
            EngineRequest::ObserveClock {
                target: clock_target.clone(),
                run_id: "run:different".to_owned(),
            },
            EngineResponse::ClockObserved {
                result: ClockObservationResult {
                    run_id: "run:echo".to_owned(),
                    observation: ClockObservationRef {
                    clock_version: crate::CLOCK_OBSERVATION_VERSION.to_owned(),
                    observation_id:
                        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                            .to_owned(),
                    source_id: clock_target.source_id,
                    source_generation: clock_target.source_generation,
                    scope:
                        "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                            .to_owned(),
                    },
                },
            },
            EngineFailureCategory::UnknownWorldOutcome,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile),
        );

        let run_plan = cymule_core::seal_plan(empty_candidate()).expect("echo Run Plan seals");
        let mut run_plugin = process_target("plugin");
        run_plugin
            .process
            .environment
            .insert("LEDGER".to_owned(), "/authority/ledger.sqlite3".to_owned());
        let run = EngineRequest::Run {
            plan: run_plan.clone(),
            input: Value::Null,
            plugin: run_plugin.clone(),
            run_id: "run:echo-process-config".to_owned(),
        };
        let mut different_run_plugin = run_plugin;
        different_run_plugin
            .process
            .environment
            .insert("LEDGER".to_owned(), "/different/ledger.sqlite3".to_owned());
        assert_wrong_engine_echo(
            &run,
            EngineRequest::Run {
                plan: run_plan,
                input: Value::Null,
                plugin: different_run_plugin,
                run_id: "run:echo-process-config".to_owned(),
            },
            EngineResponse::Verified,
            EngineFailureCategory::UnknownWorldOutcome,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile),
        );
    }

    fn verification_fixtures() -> (
        WaitActivation,
        DurableCommand,
        EvolutionCommand,
        LiveEvolutionCommand,
    ) {
        let activation: WaitActivation =
            serde_json::from_str(include_str!("../../../tests/fixtures/wait-activation.json"))
                .expect("wait activation fixture decodes");
        let durable: DurableCommand =
            serde_json::from_str(include_str!("../../../tests/fixtures/durable-control.json"))
                .expect("durable command fixture decodes");
        let evolution: EvolutionCommand = serde_json::from_str(include_str!(
            "../../../tests/fixtures/evolution-control.json"
        ))
        .expect("evolution command fixture decodes");
        let live: LiveEvolutionCommand = serde_json::from_str(include_str!(
            "../../../tests/fixtures/live-evolution-control.json"
        ))
        .expect("live-evolution command fixture decodes");
        activation.verify().expect("activation fixture verifies");
        durable.verify().expect("durable fixture verifies");
        evolution.verify().expect("evolution fixture verifies");
        live.verify().expect("live-evolution fixture verifies");
        (activation, durable, evolution, live)
    }

    fn assert_verify_payload_mismatch(request: &EngineRequest, response: EngineResponse) {
        let sent_inner = serde_json::to_value(request).expect("verification request serializes");
        let raw_response = serde_json::to_value(EngineResponseEnvelope::success(
            sent_inner.clone(),
            response,
        ))
        .expect("verification response serializes");
        let failure = admit_raw_engine_response(request, &sent_inner, &raw_response)
            .expect_err("same-tag response cannot return a different valid object");
        assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
        assert_eq!(failure.code.as_ref(), "invalid_engine_response");
        assert_eq!(failure.retry_disposition, None);
        assert!(
            failure
                .message
                .contains("does not match the requested object")
        );
    }

    #[test]
    fn verified_wait_activation_must_equal_the_request_payload() {
        let (activation, _, _, _) = verification_fixtures();
        let mut different_activation = activation.clone();
        different_activation.activation_id = "activation:different".to_owned();
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyWaitActivation {
                activation: activation.clone(),
            },
            EngineResponse::VerifiedWaitActivation {
                activation: different_activation,
            },
        );

        let mut alternate_activation = activation.clone();
        alternate_activation.source = WaitActivationSource::Timer {
            timer_id: "timer:different-source".to_owned(),
        };
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyWaitActivation { activation },
            EngineResponse::VerifiedWaitActivation {
                activation: alternate_activation,
            },
        );
    }

    #[test]
    fn verified_durable_command_must_equal_the_request_payload() {
        let (_, durable, _, _) = verification_fixtures();
        let mut different_durable = durable.clone();
        let DurableCommand::TakeoverRun { expected_fence, .. } = &mut different_durable else {
            panic!("durable fixture is a takeover command")
        };
        *expected_fence += 1;
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyDurableCommand {
                command: durable.clone(),
            },
            EngineResponse::VerifiedDurableCommand {
                command: different_durable,
            },
        );

        let alternate_durable: DurableCommand = serde_json::from_str(include_str!(
            "../../../tests/fixtures/durable-cancel-control.json"
        ))
        .expect("alternate durable command fixture decodes");
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyDurableCommand { command: durable },
            EngineResponse::VerifiedDurableCommand {
                command: alternate_durable,
            },
        );
    }

    #[test]
    fn verified_evolution_command_must_equal_the_request_payload() {
        let (_, _, evolution, _) = verification_fixtures();
        let mut different_evolution = evolution.clone();
        let EvolutionCommand::ApplyGate {
            next_decision_id, ..
        } = &mut different_evolution
        else {
            panic!("evolution fixture is an apply-gate command")
        };
        *next_decision_id = "rollout:fixture:different".to_owned();
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyEvolutionCommand {
                command: evolution.clone(),
            },
            EngineResponse::VerifiedEvolutionCommand {
                command: different_evolution,
            },
        );

        let alternate_evolution = EvolutionCommand::SetRollout {
            control_version: cymule_evolution::EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:evolution:alternate".to_owned(),
            decision: cymule_evolution::RolloutDecision {
                decision_id: "rollout:alternate".to_owned(),
                fallback_plan:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                target_plan:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
                mode: cymule_evolution::RolloutMode::Shadow,
            },
        };
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyEvolutionCommand { command: evolution },
            EngineResponse::VerifiedEvolutionCommand {
                command: alternate_evolution,
            },
        );
    }

    #[test]
    fn verified_live_evolution_command_must_equal_the_request_payload() {
        let (_, _, _, live) = verification_fixtures();
        let mut different_live = live.clone();
        let LiveEvolutionCommand::Apply { command_id, .. } = &mut different_live else {
            panic!("live-evolution fixture is an apply command")
        };
        *command_id = "command:live-evolution:different".to_owned();
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyLiveEvolutionCommand {
                command: live.clone(),
            },
            EngineResponse::VerifiedLiveEvolutionCommand {
                command: different_live,
            },
        );

        let alternate_live = LiveEvolutionCommand::PublishDefinition {
            control_version: cymule_evolution::LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "command:alternate-live".to_owned(),
            logical_ref: "subflow:alternate-live".to_owned(),
            definition: reusable_definition("alternate-live"),
            references: Vec::new(),
        };
        assert_verify_payload_mismatch(
            &EngineRequest::VerifyLiveEvolutionCommand { command: live },
            EngineResponse::VerifiedLiveEvolutionCommand {
                command: alternate_live,
            },
        );
    }

    #[test]
    fn verify_success_response_variants_are_bound_to_the_request_operation() {
        let (activation, durable, evolution, live) = verification_fixtures();

        let cases = [
            (
                EngineRequest::VerifyWaitActivation {
                    activation: activation.clone(),
                },
                EngineResponse::VerifiedDurableCommand {
                    command: durable.clone(),
                },
            ),
            (
                EngineRequest::VerifyDurableCommand {
                    command: durable.clone(),
                },
                EngineResponse::VerifiedEvolutionCommand {
                    command: evolution.clone(),
                },
            ),
            (
                EngineRequest::VerifyEvolutionCommand {
                    command: evolution.clone(),
                },
                EngineResponse::VerifiedLiveEvolutionCommand {
                    command: live.clone(),
                },
            ),
            (
                EngineRequest::VerifyLiveEvolutionCommand { command: live },
                EngineResponse::VerifiedWaitActivation { activation },
            ),
        ];
        for (request, response) in cases {
            let error = response
                .verify_for(&request)
                .expect_err("response operation variant must match its request");
            assert!(error.contains("success response does not match request"));
        }
    }

    #[test]
    fn engine_evolution_target_preserves_required_nullable_provider_members() {
        let request = live_request(live_command_fixture());
        let encoded = serde_json::to_value(&request).expect("live request serializes");
        assert_eq!(encoded["target"]["migration_adapter"], Value::Null);
        assert_eq!(encoded["target"]["shadow_driver"], Value::Null);

        let mut missing = encoded;
        missing["target"]
            .as_object_mut()
            .expect("target is an object")
            .remove("migration_adapter");
        serde_json::from_value::<EngineRequest>(missing)
            .expect_err("required-nullable provider selection cannot be omitted");
    }
    #[test]
    fn sdk_rejects_erased_optional_nulls_in_failure_responses() {
        let read_request = EngineRequest::VerifyLiveEvolutionCommand {
            command: live_command_fixture(),
        };
        let read_inner = serde_json::to_value(&read_request).expect("read request serializes");
        let mut remote_failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ValidateRequest,
            "rejected_request",
            "request was rejected",
        );
        remote_failure.issues.push(cymule_runtime::EngineIssue {
            code: "invalid_member".into(),
            message: "member is invalid".into(),
            path: None,
            schema_path: None,
        });
        let mut raw_failure = serde_json::to_value(
            EngineResponseEnvelope::<Value, EngineResponse>::failure(remote_failure),
        )
        .expect("failure serializes");
        raw_failure["error"]["issues"][0]["path"] = Value::Null;
        serde_json::from_value::<EngineResponseEnvelope<Value, EngineResponse>>(
            raw_failure.clone(),
        )
        .expect("typed failure decoding alone would erase the nested explicit null");
        let failure = admit_raw_engine_response(&read_request, &read_inner, &raw_failure)
            .expect_err("nested failure optional null is rejected");
        assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
        assert_eq!(failure.code.as_ref(), "invalid_engine_response");
        assert_eq!(failure.retry_disposition, None);
        assert!(failure.message.contains("/error/issues/0/path"));
    }
    #[test]
    fn sdk_presence_admission_preserves_required_nullable_response_members() {
        let request = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command: DurableCommand::RunCurrent {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run:missing".to_owned(),
                expected_revision: None,
            },
        };
        let sent_inner = serde_json::to_value(&request).expect("request serializes");
        let raw = serde_json::to_value(EngineResponseEnvelope::success(
            sent_inner.clone(),
            EngineResponse::DurableExecuted {
                response: DurableResponse::RunCurrent {
                    observed_revision: query_revision(),
                    source_root: query_source_root(),
                    current: None,
                },
            },
        ))
        .expect("response serializes");
        assert_eq!(raw["response"]["response"]["current"], Value::Null);
        let admitted = admit_raw_engine_response(&request, &sent_inner, &raw)
            .expect("required nullable Run-current result remains admitted");
        assert!(matches!(
            admitted,
            EngineResponse::DurableExecuted {
                response: DurableResponse::RunCurrent { current: None, .. }
            }
        ));

        let mut missing = raw;
        missing["response"]["response"]
            .as_object_mut()
            .expect("durable response is an object")
            .remove("current");
        let failure = admit_raw_engine_response(&request, &sent_inner, &missing)
            .expect_err("missing required nullable Run-current result is rejected");
        assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
    }

    #[test]
    fn typed_response_array_collapse_and_reorder_fail_with_request_authority() {
        let first = format!("sha256:{}", "a".repeat(64));
        let second = format!("sha256:{}", "b".repeat(64));
        let command = DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: "activation:typed-roundtrip".to_owned(),
            source: WaitActivationSource::Signal {
                key: "signal:typed-roundtrip".to_owned(),
            },
            wait_ids: BTreeSet::from([first.clone(), second.clone()]),
            value: serde_json::json!({"accepted": true}),
        };

        let read = EngineRequest::VerifyDurableCommand {
            command: command.clone(),
        };
        let read_inner = serde_json::to_value(&read).expect("read request serializes");
        let read_response = serde_json::to_value(EngineResponseEnvelope::success(
            read_inner.clone(),
            EngineResponse::VerifiedDurableCommand {
                command: command.clone(),
            },
        ))
        .expect("read response serializes");

        let activation = WaitActivation {
            activation_version: cymule_durable_protocol::WAIT_ACTIVATION_VERSION.to_owned(),
            activation_id: "activation:typed-roundtrip".to_owned(),
            source: WaitActivationSource::Signal {
                key: "signal:typed-roundtrip".to_owned(),
            },
            wait_ids: BTreeSet::from([first.clone(), second.clone()]),
            result: semantic_artifact(
                cymule_durable_protocol::WAIT_RESULT_ARTIFACT_KIND,
                "typed roundtrip result",
            ),
        };
        let mutation = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command,
        };
        let mutation_inner = serde_json::to_value(&mutation).expect("mutation request serializes");
        let mutation_response = serde_json::to_value(EngineResponseEnvelope::success(
            mutation_inner.clone(),
            EngineResponse::DurableExecuted {
                response: DurableResponse::WaitActivated {
                    receipt: cymule_durable_protocol::WaitActivationReceipt {
                        receipt_version: cymule_durable_protocol::WAIT_ACTIVATION_RECEIPT_VERSION
                            .to_owned(),
                        activation,
                        applied_wait_ids: BTreeSet::new(),
                        ready_run_ids: BTreeSet::new(),
                    },
                },
            },
        ))
        .expect("mutation response serializes");

        for malformed in [
            serde_json::json!([first.clone(), first.clone(), second.clone()]),
            serde_json::json!([second.clone(), first.clone()]),
        ] {
            let mut raw_read = read_response.clone();
            raw_read["response"]["command"]["wait_ids"] = malformed.clone();
            let failure = admit_raw_engine_response(&read, &read_inner, &raw_read)
                .expect_err("read response array normalization must fail closed");
            assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
            assert_eq!(failure.code.as_ref(), "invalid_engine_response");
            assert_eq!(failure.retry_disposition, None);

            let mut raw_mutation = mutation_response.clone();
            raw_mutation["response"]["response"]["receipt"]["activation"]["wait_ids"] = malformed;
            let failure = admit_raw_engine_response(&mutation, &mutation_inner, &raw_mutation)
                .expect_err("mutation response array normalization must require reconciliation");
            assert_eq!(failure.category, EngineFailureCategory::UnknownWorldOutcome);
            assert_eq!(failure.code.as_ref(), "invalid_engine_response");
            assert_eq!(
                failure.retry_disposition,
                Some(EngineRetryDisposition::Reconcile)
            );
        }
    }

    #[test]
    fn durable_run_current_accepts_a_512_scalar_run_response() {
        let run_id = "🦀".repeat(512);
        assert_eq!(run_id.chars().count(), 512);
        assert!(valid_wire_identity(&run_id));
        let expected = run_current_fixture(&run_id);
        expected.verify().expect("boundary Run view verifies");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let durable = DurableEngine::from_transport(
            DurableQueryProbe {
                requests: Arc::clone(&requests),
                response: DurableResponse::RunCurrent {
                    observed_revision: query_revision(),
                    source_root: query_source_root(),
                    current: Some(Box::new(expected.clone())),
                },
            },
            EngineStoreTarget::directory("domain"),
        );

        let returned = durable
            .run_current(&run_id, None)
            .expect("512-scalar Run query succeeds");
        assert_eq!(returned.current.as_deref(), Some(&expected));
        assert_eq!(returned.observed_revision, query_revision());
        assert_eq!(returned.source_root, query_source_root());

        let requests = requests
            .lock()
            .expect("query probe request lock remains available");
        assert!(matches!(
            requests.as_slice(),
            [(target, DurableCommand::RunCurrent {
                run_id: queried_run,
                expected_revision: None,
                ..
            })] if target.executor.is_none()
                && target.clock.is_none()
                && queried_run == &run_id
        ));
    }

    #[test]
    fn non_completed_run_current_cannot_forge_a_terminal_result() {
        let mut view = run_current_fixture("run:forged-result");
        view.result = Some(semantic_artifact("cymule.result/1", "forged"));
        assert!(view.verify().is_err());

        for (status, continuation_status) in [
            (
                cymule_core::RunExecutionStatus::Failed {
                    failure: cymule_core::RunFailure {
                        class: cymule_core::RunFailureClass::RuntimeDefect,
                        code: "test_failure".to_owned(),
                        detail: semantic_artifact("cymule.failure/1", "failure"),
                    },
                },
                cymule_durable_protocol::ContinuationStatus::Failed,
            ),
            (
                cymule_core::RunExecutionStatus::Cancelled {
                    reason: semantic_artifact("cymule.cancellation/1", "cancelled"),
                },
                cymule_durable_protocol::ContinuationStatus::Cancelled,
            ),
        ] {
            let mut terminal = view.clone();
            terminal.execution_status = status;
            terminal.continuation_status = continuation_status;
            assert!(terminal.verify().is_err());
        }
    }

    #[test]
    fn durable_run_current_rejects_513_scalars_and_controls_before_transport() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let durable = DurableEngine::from_transport(
            DurableQueryProbe {
                requests: Arc::clone(&requests),
                response: DurableResponse::RunCurrent {
                    observed_revision: query_revision(),
                    source_root: query_source_root(),
                    current: None,
                },
            },
            EngineStoreTarget::directory("domain"),
        );
        let too_long = "🦀".repeat(513);
        assert!(!valid_wire_identity(&too_long));

        for invalid_run in [too_long, "run:\u{0000}control".to_owned()] {
            let failure = durable
                .run_current(&invalid_run, None)
                .expect_err("invalid Run identity is rejected locally");
            assert_eq!(failure.category, EngineFailureCategory::Validation);
            assert_eq!(failure.phase, EnginePhase::VerifyDurableCommand);
            assert_eq!(failure.code.as_ref(), "durable_command_validation_failed");
            assert_eq!(
                failure.retry_disposition,
                Some(cymule_runtime::EngineRetryDisposition::CorrectAndRetry)
            );
        }
        assert!(
            requests
                .lock()
                .expect("query probe request lock remains available")
                .is_empty(),
            "invalid command must not reach the transport"
        );
    }

    #[test]
    fn missing_clock_configuration_is_a_correctable_validation_failure() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let durable = DurableEngine::from_transport(
            DurableQueryProbe {
                requests: Arc::clone(&requests),
                response: missing_run_current_response(),
            },
            EngineStoreTarget::directory("domain"),
        );
        let failure = durable
            .observe_clock("run:missing-clock")
            .expect_err("Clock observation requires configured authority");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.phase, EnginePhase::ObserveClock);
        assert_eq!(failure.code.as_ref(), "missing_clock_provider");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(
            requests
                .lock()
                .expect("query probe request lock remains available")
                .is_empty(),
            "missing Clock authority must not reach the transport"
        );
    }

    #[test]
    fn missing_durable_capabilities_never_reach_a_custom_mutation_transport() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let probe = || DurableQueryProbe {
            requests: Arc::clone(&requests),
            response: missing_run_current_response(),
        };
        let assert_missing = |failure: EngineFailure, code: &str| {
            assert_eq!(failure.category, EngineFailureCategory::Validation);
            assert_eq!(failure.phase, EnginePhase::ExecuteDurable);
            assert_eq!(failure.code.as_ref(), code);
            assert_eq!(
                failure.retry_disposition,
                Some(EngineRetryDisposition::CorrectAndRetry)
            );
        };

        let missing_both =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"));
        assert_missing(
            missing_both
                .resume("run:missing-executor", execution())
                .expect_err("execution requires an executor"),
            "missing_execution_provider",
        );

        let missing_clock =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"))
                .with_executor(process_target("plugin"));
        assert_missing(
            missing_clock
                .resume("run:missing-clock", execution())
                .expect_err("execution requires a Clock"),
            "missing_clock_provider",
        );

        let missing_effect_provider =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"));
        assert_missing(
            missing_effect_provider
                .resolve_effect(EffectResolutionCommand {
                    resolution_id: "resolution:missing-provider".to_owned(),
                    run_id: "run:missing-provider".to_owned(),
                    intent_id: format!("sha256:{}", "c".repeat(64)),
                    execution_binding: execution_binding_ref("missing provider binding"),
                    occurrence_binding: format!("sha256:{}", "d".repeat(64)),
                    claim_owner: "owner:missing-provider".to_owned(),
                    claim_epoch: 1,
                    resolution: ReconciliationResolution::ResolvedNotApplied,
                    value: None,
                })
                .expect_err("Effect resolution requires its historical executor"),
            "missing_execution_provider",
        );

        assert!(
            requests
                .lock()
                .expect("request probe remains available")
                .is_empty(),
            "missing durable capability must fail before custom transport"
        );
    }

    #[test]
    fn invalid_configured_facade_targets_never_reach_custom_transport() {
        let invoked = Arc::new(AtomicBool::new(false));
        let probe = || PreflightProbe {
            invoked: Arc::clone(&invoked),
        };
        let invalid_store = EngineStoreTarget {
            provider: String::new(),
            location: "domain".to_owned(),
            domain: None,
        };
        let failure = DurableEngine::from_transport(probe(), invalid_store)
            .run_current("run:invalid-store", None)
            .expect_err("invalid Store target must fail before custom transport");
        assert_eq!(failure.category, EngineFailureCategory::Validation);

        let mut invalid_executor = process_target("executor");
        invalid_executor.process.executable = "relative-executable".to_owned();
        let failure =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"))
                .with_executor(invalid_executor)
                .with_clock(clock_target())
                .resume("run:invalid-executor", execution())
                .expect_err("invalid executor target must fail before custom transport");
        assert_eq!(failure.category, EngineFailureCategory::Validation);

        let mut invalid_clock = clock_target();
        invalid_clock.source_generation = format!("sha256:{}", "A".repeat(64));
        let durable =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"))
                .with_clock(invalid_clock);
        let failure = durable
            .observe_clock("run:invalid-clock")
            .expect_err("invalid Clock target must fail before custom transport");
        assert_eq!(failure.category, EngineFailureCategory::Validation);

        let durable =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"))
                .with_clock(clock_target());
        let failure = durable
            .observe_clock("")
            .expect_err("invalid Clock Run must fail before custom transport");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.code.as_ref(), "invalid_run_identity");

        let failure =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"))
                .with_evolution_id("")
                .evolve(&live_command_fixture())
                .expect_err("invalid evolution identity must fail before custom transport");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.code.as_ref(), "invalid_evolution_identity");

        let migration = migration_command_fixture();
        let LiveEvolutionCommand::Apply { command, .. } = &migration else {
            unreachable!("migration fixture is an Apply command")
        };
        let EvolutionCommand::Migrate { request, .. } = command.as_ref() else {
            unreachable!("migration fixture contains Migrate")
        };
        let mut process = process_target("migration");
        process.revision = Some(request.adapter_revision.clone());
        process.process.message_limit = 16 * 1024 * 1024;
        let failure =
            DurableEngine::from_transport(probe(), EngineStoreTarget::directory("domain"))
                .with_migration_adapter(EngineMigrationProviderTarget {
                    adapter_id: request.adapter_id.clone(),
                    adapter_revision: request.adapter_revision.clone(),
                    process,
                })
                .evolve(&migration)
                .expect_err("adapter-only migration target must fail before custom transport");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.code.as_ref(), "evolution_target_validation_failed");
        assert!(
            !invoked.load(Ordering::Acquire),
            "invalid configured target reached the custom transport"
        );
    }

    #[test]
    fn start_missing_capabilities_fails_before_seal_transport() {
        let invoked = Arc::new(AtomicBool::new(false));
        let durable = DurableEngine::from_transport(
            PreflightProbe {
                invoked: Arc::clone(&invoked),
            },
            EngineStoreTarget::directory("domain"),
        );
        let failure = durable
            .start(
                "run:missing-start-capabilities",
                empty_candidate(),
                Value::Null,
                execution(),
            )
            .expect_err("Start requires local executor and Clock configuration");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.phase, EnginePhase::ExecuteDurable);
        assert_eq!(failure.code.as_ref(), "missing_execution_provider");
        assert!(!invoked.load(Ordering::Acquire));
    }

    #[test]
    fn durable_engine_targets_match_each_command_capability_exactly() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let executor = process_target("plugin");
        let clock = clock_target();
        let durable = DurableEngine::from_transport(
            DurableQueryProbe {
                requests: Arc::clone(&requests),
                response: missing_run_current_response(),
            },
            EngineStoreTarget::directory("domain"),
        )
        .with_executor(executor.clone())
        .with_clock(clock.clone());

        durable
            .run_current("run:target-shape", None)
            .expect("query response matches");
        durable
            .resolve_effect(EffectResolutionCommand {
                resolution_id: "resolution:target-shape".to_owned(),
                run_id: "run:target-shape".to_owned(),
                intent_id:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_owned(),
                execution_binding: execution_binding_ref("target shape binding"),
                occurrence_binding:
                    "sha256:edededededededededededededededededededededededededededededededed"
                        .to_owned(),
                claim_owner: "owner:target-shape".to_owned(),
                claim_epoch: 1,
                resolution: ReconciliationResolution::ResolvedNotApplied,
                value: None,
            })
            .expect_err("probe returns a query response after capturing resolution target");
        durable
            .resume("run:target-shape", execution())
            .expect_err("probe returns a query response after capturing execution target");
        durable
            .cancel(
                "cancel:target-shape",
                "run:target-shape",
                serde_json::json!({"reason": "target shape"}),
            )
            .expect_err("probe returns a query response after capturing cancellation target");

        let requests = requests
            .lock()
            .expect("target-shape probe request lock remains available");
        assert_eq!(requests.len(), 4);
        for (target, command) in requests.iter() {
            assert_eq!(
                target.executor.is_some(),
                command.requires_executor(),
                "executor target presence must exactly match {command:?}",
            );
            assert_eq!(
                target.clock.is_some(),
                command.requires_clock(),
                "Clock target presence must exactly match {command:?}",
            );
        }
        assert!(matches!(
            &requests[1],
            (
                EngineDurableTarget {
                    executor: Some(returned_executor),
                    clock: None,
                    ..
                },
                DurableCommand::ResolveEffect { .. }
            ) if returned_executor == &executor
        ));
        assert!(matches!(
            &requests[2],
            (
                EngineDurableTarget {
                    executor: Some(returned_executor),
                    clock: Some(returned_clock),
                    ..
                },
                DurableCommand::ResumeRun { .. }
            ) if returned_executor == &executor && returned_clock == &clock
        ));
        assert!(matches!(
            &requests[3],
            (
                EngineDurableTarget {
                    executor: None,
                    clock: None,
                    ..
                },
                DurableCommand::CancelRun { .. }
            )
        ));
    }

    #[test]
    fn durable_engine_selects_only_the_live_provider_required_by_the_command() {
        let migration_command = migration_command_fixture();
        let LiveEvolutionCommand::Apply {
            command: migration, ..
        } = &migration_command
        else {
            unreachable!("migration fixture is an Apply command")
        };
        let EvolutionCommand::Migrate { request, .. } = migration.as_ref() else {
            unreachable!("migration fixture contains Migrate")
        };
        let migration_adapter = EngineMigrationProviderTarget {
            adapter_id: request.adapter_id.clone(),
            adapter_revision: request.adapter_revision.clone(),
            process: {
                let mut target = process_target("migration");
                target.revision = Some(request.adapter_revision.clone());
                target.process.message_limit = 16 * 1024 * 1024;
                target
            },
        };

        let shadow_command = shadow_command_fixture();
        let LiveEvolutionCommand::Apply {
            command: shadow, ..
        } = &shadow_command
        else {
            unreachable!("shadow fixture is an Apply command")
        };
        let EvolutionCommand::Shadow { request, .. } = shadow.as_ref() else {
            unreachable!("shadow fixture contains Shadow")
        };
        let shadow_driver = EngineShadowProviderTarget {
            driver_id: request.driver_id.clone(),
            driver_revision: request.driver_revision.clone(),
            process: {
                let mut target = process_target("shadow");
                target.revision = Some(request.driver_revision.clone());
                target.process.message_limit = 16 * 1024 * 1024;
                target
            },
        };

        let durable = DurableEngine::from_transport(
            DurableQueryProbe {
                requests: Arc::new(Mutex::new(Vec::new())),
                response: missing_run_current_response(),
            },
            EngineStoreTarget::directory("domain"),
        )
        .with_migration_adapter(migration_adapter.clone())
        .with_shadow_driver(shadow_driver.clone());

        for (command, expected_migration, expected_shadow) in [
            (live_command_fixture(), false, false),
            (migration_command, true, false),
            (shadow_command, false, true),
        ] {
            let target = durable.evolution_target(&command);
            assert_eq!(
                target.migration_adapter.as_ref(),
                expected_migration.then_some(&migration_adapter)
            );
            assert_eq!(
                target.shadow_driver.as_ref(),
                expected_shadow.then_some(&shadow_driver)
            );
        }
    }
    #[test]
    fn invalid_live_commands_fail_before_cli_or_custom_transport_mutation() {
        let mut invalid = live_command_fixture();
        let LiveEvolutionCommand::PublishDefinition { command_id, .. } = &mut invalid else {
            unreachable!("fixture has a publication command")
        };
        command_id.clear();
        let target = EngineEvolutionTarget {
            store: EngineStoreTarget::directory("domain"),
            migration_adapter: None,
            shadow_driver: None,
            target_execution_bindings: std::collections::BTreeMap::new(),
        };
        let cli_failure = CliEngine::new("/definitely/missing/cymule")
            .execute_live_evolution(&target, "evolution:wire", &invalid)
            .expect_err("invalid command is rejected before the CLI starts");
        assert_local_live_preflight_failure(&cli_failure);

        let invoked = Arc::new(AtomicBool::new(false));
        let durable = DurableEngine::from_transport(
            PreflightProbe {
                invoked: Arc::clone(&invoked),
            },
            EngineStoreTarget::directory("domain"),
        );
        let facade_failure = durable
            .evolve(&invalid)
            .expect_err("invalid command is rejected before a custom transport runs");
        assert_local_live_preflight_failure(&facade_failure);
        assert!(!invoked.load(Ordering::Acquire));
    }

    #[test]
    fn cli_preflight_admits_provider_free_retained_evolution_replay() {
        for command in [migration_command_fixture(), shadow_command_fixture()] {
            let target = EngineEvolutionTarget::new(EngineStoreTarget::directory("domain"));
            verify_evolution_target_preflight(&target, &command)
                .expect("provider-free retained replay must reach the CLI exact-alias read");
        }
    }

    #[test]
    fn evolution_commit_preserves_required_nullable_revision_and_exact_command() {
        let command = live_command_fixture();
        let commit = evolution_commit_fixture("evolution:wire", &command);
        verify_evolution_commit("evolution:wire", &command, &commit)
            .expect("terminal Evolution commit verifies");

        let encoded = serde_json::to_value(&commit).expect("Evolution commit serializes");
        assert!(encoded.get("committed_revision").is_some());
        let mut missing = encoded;
        missing
            .as_object_mut()
            .expect("Evolution commit is an object")
            .remove("committed_revision");
        serde_json::from_value::<EvolutionCommit>(missing)
            .expect_err("committed_revision is required even when nullable");
    }

    #[test]
    fn evolution_commit_rejects_changed_authority_or_semantic_command() {
        let command = live_command_fixture();
        let commit = evolution_commit_fixture("evolution:wire", &command);

        assert!(
            verify_evolution_commit("evolution:other", &command, &commit).is_err(),
            "a different evolution authority cannot accept the commit",
        );

        let mut changed = command;
        let LiveEvolutionCommand::PublishDefinition { command_id, .. } = &mut changed else {
            unreachable!("fixture has a publication command")
        };
        *command_id = "command:definition:other".to_owned();
        changed
            .verify()
            .expect("changed semantic command remains valid");
        assert!(
            verify_evolution_commit("evolution:wire", &changed, &commit).is_err(),
            "a different semantic command cannot accept the commit",
        );
    }
    #[test]
    fn rust_cli_requests_use_the_runtime_engine_v5_constant() {
        assert_eq!(cymule_runtime::ENGINE_PROTOCOL_VERSION, "cymule.engine/5");
        let request = EngineRequest::Seal {
            candidate: empty_candidate(),
        };
        let value = serde_json::to_value(EngineRequestEnvelope::new(&request))
            .expect("request envelope serializes");
        assert_eq!(
            value["engine_protocol"],
            Value::String(cymule_runtime::ENGINE_PROTOCOL_VERSION.to_owned())
        );
        let legacy_bytes = serde_json::to_vec(&serde_json::json!({
            "engine_protocol": "cymule.engine/4",
            "outcome": "success",
            "request": request,
            "response": {"type": "verified"}
        }))
        .expect("legacy envelope serializes");
        let legacy: EngineResponseEnvelope<Value, EngineResponse> = decode_json(&legacy_bytes)
            .expect("closed legacy envelope decodes before version admission");
        let failure = legacy
            .into_result()
            .expect_err("Engine v4 is not a compatible transport generation");
        assert_eq!(failure.code.as_ref(), "unsupported_engine_protocol");
        assert_eq!(
            failure.contract.as_deref(),
            Some(cymule_runtime::ENGINE_PROTOCOL_VERSION)
        );

        let read = EngineRequest::Seal {
            candidate: empty_candidate(),
        };
        let read_inner = serde_json::to_value(&read).expect("read request serializes");
        let failure = admit_engine_response(
            &read,
            &read_inner,
            EngineResponseEnvelope::Success {
                engine_protocol: "cymule.engine/4".to_owned(),
                request: read_inner.clone(),
                response: EngineResponse::Verified,
            },
        )
        .expect_err("a legacy success cannot settle a read-only request");
        assert_eq!(failure.category, EngineFailureCategory::ContractViolation);
        assert_eq!(failure.code.as_ref(), "unsupported_engine_protocol");
        assert_eq!(
            failure.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Never)
        );
        assert_eq!(
            failure.contract.as_deref(),
            Some(cymule_runtime::ENGINE_PROTOCOL_VERSION)
        );

        let mutation = EngineRequest::ObserveClock {
            target: clock_target(),
            run_id: "run:legacy-failure".to_owned(),
        };
        let legacy_failure: EngineResponseEnvelope<Value, EngineResponse> =
            EngineResponseEnvelope::Failure {
                engine_protocol: "cymule.engine/4".to_owned(),
                error: EngineFailure::transport("legacy_failure", "legacy failure envelope"),
            };
        let failure = admit_engine_response(&mutation, &Value::Null, legacy_failure)
            .expect_err("a legacy failure cannot settle a mutating request");
        assert_eq!(failure.category, EngineFailureCategory::UnknownWorldOutcome);
        assert_eq!(failure.code.as_ref(), "unsupported_engine_protocol");
        assert_eq!(
            failure.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile)
        );
    }

    #[test]
    fn mathematical_integer_response_tokens_normalize_before_typed_admission() {
        let command: EvolutionCommand = serde_json::from_str(include_str!(
            "../../../tests/fixtures/evolution-control.json"
        ))
        .expect("evolution fixture decodes");
        let request = EngineRequest::VerifyEvolutionCommand {
            command: command.clone(),
        };
        let sent_inner = serde_json::to_value(&request).expect("request serializes");
        let mut raw = serde_json::to_value(EngineResponseEnvelope::success(
            sent_inner.clone(),
            EngineResponse::VerifiedEvolutionCommand { command },
        ))
        .expect("response serializes");
        for root in ["request", "response"] {
            raw[root]["command"]["gate"]["min_target_observations"] = serde_json::Value::Number(
                serde_json::Number::from_f64(3.0).expect("finite number"),
            );
        }
        assert!(
            admit_raw_engine_response(&request, &sent_inner, &raw).is_ok(),
            "mathematical integer tokens are accepted independent of lexical form"
        );

        raw["response"]["command"]["gate"]["min_target_observations"] =
            serde_json::Value::Number(serde_json::Number::from_f64(3.5).expect("finite number"));
        let failure = admit_raw_engine_response(&request, &sent_inner, &raw)
            .expect_err("a non-integral token cannot populate an integer field");
        assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
        assert_eq!(failure.code.as_ref(), "invalid_engine_response");
    }

    #[test]
    fn failure_envelope_validation_uses_the_request_mutation_authority() {
        let invalid_failure = || {
            EngineFailure::new(
                EngineFailureCategory::Validation,
                EnginePhase::ValidateRequest,
                "invalid_remote_failure",
                "the remote failure omitted its required retry disposition",
            )
        };
        let read = EngineRequest::Seal {
            candidate: empty_candidate(),
        };
        let read_inner = serde_json::to_value(&read).expect("read request serializes");
        let read_failure = admit_engine_response(
            &read,
            &read_inner,
            EngineResponseEnvelope::Failure {
                engine_protocol: cymule_runtime::ENGINE_PROTOCOL_VERSION.to_owned(),
                error: invalid_failure(),
            },
        )
        .expect_err("a semantically invalid read failure is not an Engine result");
        assert_eq!(
            read_failure.category,
            EngineFailureCategory::TransportFailure
        );
        assert_eq!(read_failure.code.as_ref(), "invalid_engine_response");
        assert_eq!(read_failure.retry_disposition, None);
        read_failure
            .verify()
            .expect("the synthesized read failure is wire-valid");

        let mutation = EngineRequest::ObserveClock {
            target: clock_target(),
            run_id: "run:invalid-failure".to_owned(),
        };
        let mutation_inner = serde_json::to_value(&mutation).expect("mutation request serializes");
        let mutation_failure = admit_engine_response(
            &mutation,
            &mutation_inner,
            EngineResponseEnvelope::Failure {
                engine_protocol: cymule_runtime::ENGINE_PROTOCOL_VERSION.to_owned(),
                error: invalid_failure(),
            },
        )
        .expect_err("an invalid failure cannot settle a mutating request");
        assert_eq!(
            mutation_failure.category,
            EngineFailureCategory::UnknownWorldOutcome
        );
        assert_eq!(mutation_failure.code.as_ref(), "invalid_engine_response");
        assert_eq!(
            mutation_failure.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile)
        );
        mutation_failure
            .verify()
            .expect("the synthesized mutation failure is wire-valid");

        let mut remote = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ValidateRequest,
            "remote_validation",
            "the exact valid remote failure is preserved",
        );
        remote.retry_disposition = Some(cymule_runtime::EngineRetryDisposition::CorrectAndRetry);
        let preserved = admit_engine_response(
            &read,
            &read_inner,
            EngineResponseEnvelope::Failure {
                engine_protocol: cymule_runtime::ENGINE_PROTOCOL_VERSION.to_owned(),
                error: remote.clone(),
            },
        )
        .expect_err("a valid remote failure remains the operation result");
        assert_eq!(preserved.category, remote.category);
        assert_eq!(preserved.phase, remote.phase);
        assert_eq!(preserved.code, remote.code);
        assert_eq!(preserved.message, remote.message);
        assert_eq!(preserved.retry_disposition, remote.retry_disposition);
    }

    #[test]
    fn seal_success_requires_the_exact_requested_plan_or_resource_semantics() {
        let mut different_plan_candidate = empty_candidate();
        different_plan_candidate.name = "engine_owned_sealed_plan".to_owned();
        let different_plan = cymule_core::seal_plan(different_plan_candidate)
            .expect("different returned Plan self-verifies");
        let seal = EngineRequest::Seal {
            candidate: empty_candidate(),
        };
        let seal_inner = serde_json::to_value(&seal).expect("Seal request serializes");
        admit_engine_response(
            &seal,
            &seal_inner,
            EngineResponseEnvelope::success(
                seal_inner.clone(),
                EngineResponse::Sealed {
                    plan: different_plan,
                },
            ),
        )
        .expect_err("a self-valid but unrequested Plan is rejected");

        let different = ResourceCandidate::text("different resource")
            .seal()
            .expect("different returned Resource self-verifies");
        let resource_request = EngineRequest::SealResource {
            candidate: ResourceCandidate::text("requested resource"),
        };
        let resource_inner =
            serde_json::to_value(&resource_request).expect("Resource request serializes");
        admit_engine_response(
            &resource_request,
            &resource_inner,
            EngineResponseEnvelope::success(
                resource_inner.clone(),
                EngineResponse::SealedResource {
                    resource: different,
                },
            ),
        )
        .expect_err("a self-valid but unrequested Resource is rejected");
    }

    #[test]
    fn sealed_resource_response_rejects_split_manifest_authority() {
        #[derive(serde::Serialize)]
        struct ResourceIdentity<'a> {
            resource_version: &'a str,
            shape: cymule_resource::ResourceShape,
            media_type: &'a str,
            inline: Option<&'a cymule_resource::InlineData>,
            integrity: &'a cymule_resource::ResourceIntegrity,
            manifest: Option<&'a cymule_resource::ResourceManifestDescriptor>,
            annotations: &'a std::collections::BTreeMap<String, String>,
        }

        let entry = |name: &str, value: &str| cymule_resource::ResourceManifestEntry {
            name: name.to_owned(),
            resource: ResourceCandidate::text(value)
                .seal()
                .expect("child Resource seals"),
        };
        let first = cymule_resource::SealedResourceManifest::seal(vec![entry("a", "first")])
            .expect("first manifest seals");
        let second = cymule_resource::SealedResourceManifest::seal(vec![entry("b", "second")])
            .expect("second manifest seals");
        let requested = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: cymule_resource::ResourceShape::Directory,
            media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
            inline: None,
            integrity: cymule_resource::ResourceIntegrity::Content {
                digest: first.descriptor.digest.clone(),
                size: first.descriptor.size,
            },
            manifest: Some(first.descriptor.clone()),
            annotations: std::collections::BTreeMap::new(),
        };
        requested.validate().expect("requested manifest is valid");

        let mut mixed = first.descriptor;
        mixed.root_digest = second.descriptor.root_digest;
        let integrity = cymule_resource::ResourceIntegrity::Content {
            digest: mixed.digest.clone(),
            size: mixed.size,
        };
        let annotations = std::collections::BTreeMap::new();
        let resource_id = cymule_core::content_id(
            cymule_resource::RESOURCE_VERSION,
            &ResourceIdentity {
                resource_version: cymule_resource::RESOURCE_VERSION,
                shape: cymule_resource::ResourceShape::Directory,
                media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE,
                inline: None,
                integrity: &integrity,
                manifest: Some(&mixed),
                annotations: &annotations,
            },
        )
        .expect("outer Resource identity recomputes");
        let malicious = ResourceHandle {
            resource_id,
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: cymule_resource::ResourceShape::Directory,
            media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
            inline: None,
            integrity,
            manifest: Some(mixed),
            annotations,
        };
        let request = EngineRequest::SealResource {
            candidate: requested,
        };
        assert!(
            EngineResponse::SealedResource {
                resource: malicious
            }
            .verify_for(&request)
            .is_err()
        );
    }

    #[test]
    fn durable_start_binds_completion_to_the_same_engine_seal_authority() {
        let candidate = empty_candidate();
        let expected_plan = cymule_core::seal_plan(candidate.clone()).expect("candidate seals");
        let forged_plan_id = format!("sha256:{}", "7".repeat(64));
        assert_ne!(forged_plan_id, expected_plan.plan_id);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let durable = DurableEngine::from_transport(
            DurableQueryProbe {
                requests,
                response: DurableResponse::RunBoundary {
                    boundary: DurableBoundary::Completed {
                        result: cymule_runtime::ExecutionResult {
                            run_id: "run:start-plan-binding".to_owned(),
                            plan_id: forged_plan_id,
                            value: Value::Null,
                            projection_digest: "8".repeat(64),
                            precondition_token: format!("pre:1:sha256:{}", "9".repeat(64)),
                            effects: Vec::new(),
                        },
                    },
                },
            },
            EngineStoreTarget::directory("domain"),
        )
        .with_executor(process_target("plugin"))
        .with_clock(clock_target());

        let failure = durable
            .start(
                "run:start-plan-binding",
                candidate,
                Value::Null,
                execution(),
            )
            .expect_err("a completed Start cannot return another Plan");
        assert_eq!(failure.code.as_ref(), "invalid_engine_response");
    }

    #[cfg(unix)]
    #[test]
    fn applied_effect_summaries_fail_closed_at_cli_ingress() {
        let fixture: Value = cymule_core::decode_json(include_bytes!(
            "../../../tests/fixtures/applied-effect-summary.json"
        ))
        .expect("shared canonical-null summary is strict JSON");
        let run_id = fixture["run_id"].as_str().expect("summary owns a Run");
        let target = EngineDurableTarget::query(EngineStoreTarget::directory("unused"));
        let command = DurableCommand::RunEffectPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: None,
            cursor: None,
            limit: 1,
            max_canonical_bytes: 1024 * 1024,
        };
        let request = EngineRequest::ExecuteDurable {
            target: target.clone(),
            command: command.clone(),
        };

        for (state, retain_result, accepted) in [
            ("applied", true, true),
            ("applied", false, false),
            ("not_applied", false, true),
            ("not_applied", true, false),
        ] {
            let mut summary = fixture.clone();
            summary["state"] = Value::from(state);
            if !retain_result {
                summary["result"] = Value::Null;
            }
            let response = serde_json::json!({
                "type": "run_effect_page", "run_id": run_id,
                "page": {
                    "observed_revision": query_revision(), "source_root": query_source_root(),
                    "items": [summary], "next_cursor": null,
                },
            });
            let envelope = serde_json::json!({
                "engine_protocol": cymule_runtime::ENGINE_PROTOCOL_VERSION, "outcome": "success",
                "request": request,
                "response": {"type": "durable_executed", "response": response},
            });
            let directory = tempfile::tempdir().expect("isolated Engine fixture directory");
            let executable = directory.path().join("summary-engine");
            std::fs::write(
                &executable,
                "#!/bin/sh\n/bin/cat >/dev/null\n/bin/cat \"$0.json\"\n",
            )
            .expect("Engine fixture writes");
            std::fs::write(
                executable.with_extension("json"),
                serde_json::to_vec(&envelope).expect("response envelope encodes"),
            )
            .expect("Engine response writes");
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
                .expect("Engine fixture becomes executable");

            let outcome = CliEngine::new(&executable).execute_durable(&target, &command);
            if accepted {
                let actual = outcome.expect("valid summary passes real CLI admission");
                assert_eq!(serde_json::to_value(actual).unwrap(), response);
            } else {
                let failure = outcome.expect_err("invalid summary fails real CLI admission");
                assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
                assert_eq!(failure.code.as_ref(), "invalid_engine_response");
                assert_eq!(failure.retry_disposition, None);
            }
        }
    }

    #[test]
    fn read_only_nested_validation_failure_is_an_invalid_response() {
        let mut current = run_current_fixture("run:wire");
        current.execution_status = cymule_core::RunExecutionStatus::Completed;
        let response = DurableResponse::RunCurrent {
            observed_revision: query_revision(),
            source_root: query_source_root(),
            current: Some(Box::new(current)),
        };
        let request = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command: DurableCommand::RunCurrent {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run:wire".to_owned(),
                expected_revision: None,
            },
        };
        let error = EngineResponse::DurableExecuted { response }
            .verify_for(&request)
            .expect_err("malformed query response is rejected");
        let failure = invalid_success_response(&request, &error);
        assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
        assert_eq!(failure.code.as_ref(), "invalid_engine_response");
        assert_eq!(failure.retry_disposition, None);
    }

    #[test]
    fn request_aware_success_rejects_wrong_cancellation_reason_and_result_contract() {
        let requested_reason = serde_json::json!({"reason": "requested"});
        let cancellation = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command: DurableCommand::CancelRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                cancellation_id: "cancel:wire".to_owned(),
                run_id: "run:wire".to_owned(),
                reason: requested_reason,
            },
        };
        let different_reason = serde_json::json!({"reason": "different"});
        let wrong_reason = cymule_core::artifact_ref(
            cymule_durable::CANCELLATION_REASON_ARTIFACT_KIND,
            &cymule_core::canonical_bytes(&different_reason).expect("wrong reason encodes"),
        )
        .expect("wrong reason reference derives");
        assert!(
            EngineResponse::DurableExecuted {
                response: DurableResponse::RunCancelled {
                    receipt: cymule_durable::CancellationReceipt {
                        receipt_version: cymule_durable::RUN_CANCELLATION_RECEIPT_VERSION
                            .to_owned(),
                        command: CancellationCommand {
                            cancellation_id: "cancel:wire".to_owned(),
                            run_id: "run:wire".to_owned(),
                            reason: different_reason,
                        },
                        boundary: DurableBoundary::Cancelled {
                            reason: wrong_reason,
                        },
                        receipt_id: "0".repeat(64),
                    },
                },
            }
            .verify_for(&cancellation)
            .is_err()
        );

        let execution_binding = execution_binding_ref("effect resolution binding");
        let requested_value = serde_json::json!({"requested": "applied"});
        let resolution = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::resolve(
                EngineStoreTarget::directory("domain"),
                process_target("plugin"),
            ),
            command: DurableCommand::ResolveEffect {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                resolution_id: "resolution:wire".to_owned(),
                run_id: "run:wire".to_owned(),
                intent_id:
                    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                        .to_owned(),
                execution_binding: execution_binding.clone(),
                occurrence_binding:
                    "sha256:abababababababababababababababababababababababababababababababab"
                        .to_owned(),
                claim_owner: "driver:wire".to_owned(),
                claim_epoch: 3,
                resolution: ReconciliationResolution::ResolvedApplied,
                value: Some(requested_value.clone()),
            },
        };
        assert!(
            EngineResponse::DurableExecuted {
                response: DurableResponse::EffectResolved {
                    receipt: cymule_durable::EffectResolutionReceipt {
                        receipt_version: cymule_durable::EFFECT_RESOLUTION_RECEIPT_VERSION
                            .to_owned(),
                        command: EffectResolutionCommand {
                            resolution_id: "resolution:wire".to_owned(),
                            run_id: "run:wire".to_owned(),
                            intent_id:
                                "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                                    .to_owned(),
                            execution_binding,
                            occurrence_binding:
                                "sha256:abababababababababababababababababababababababababababababababab"
                                    .to_owned(),
                            claim_owner: "driver:wire".to_owned(),
                            claim_epoch: 3,
                            resolution: ReconciliationResolution::ResolvedApplied,
                            value: Some(requested_value),
                        },
                        actual_resolution: ReconciliationResolution::ResolvedNotApplied,
                        actual_value: None,
                        result: None,
                        receipt_id: "1".repeat(64),
                    },
                },
            }
            .verify_for(&resolution)
            .is_ok()
        );

        assert_execution_result_contract();
    }

    fn assert_execution_result_contract() {
        let mut candidate = empty_candidate();
        candidate.definitions[0].output_schema = serde_json::json!({"type": "string"});
        let plan = cymule_core::seal_plan(candidate).expect("result-contract Plan seals");
        let run = EngineRequest::Run {
            plan: plan.clone(),
            input: Value::Null,
            plugin: process_target("plugin"),
            run_id: "run:result-contract".to_owned(),
        };
        let invalid_result = EngineResponse::ExecutionBoundary {
            execution: ExecutionOutcome::Completed {
                result: cymule_runtime::ExecutionResult {
                    run_id: "run:result-contract".to_owned(),
                    plan_id: plan.plan_id,
                    value: Value::Null,
                    projection_digest: "a".repeat(64),
                    precondition_token: "precondition:wire".to_owned(),
                    effects: Vec::new(),
                },
            },
        };
        assert!(invalid_result.verify_for(&run).is_err());
    }

    #[test]
    fn durable_receipts_bind_rust_owned_artifacts_without_sdk_hashing() {
        let reason = serde_json::json!({"reason": "opaque-authority"});
        let reason_ref = cymule_core::ArtifactRef {
            identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
            artifact_id: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            kind: cymule_durable::CANCELLATION_REASON_ARTIFACT_KIND.to_owned(),
        };
        let cancellation = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command: DurableCommand::CancelRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                cancellation_id: "cancel:opaque".to_owned(),
                run_id: "run:opaque".to_owned(),
                reason: reason.clone(),
            },
        };
        EngineResponse::DurableExecuted {
            response: DurableResponse::RunCancelled {
                receipt: cymule_durable::CancellationReceipt {
                    receipt_version: cymule_durable::RUN_CANCELLATION_RECEIPT_VERSION.to_owned(),
                    command: CancellationCommand {
                        cancellation_id: "cancel:opaque".to_owned(),
                        run_id: "run:opaque".to_owned(),
                        reason,
                    },
                    boundary: DurableBoundary::Cancelled { reason: reason_ref },
                    receipt_id: "2".repeat(64),
                },
            },
        }
        .verify_for(&cancellation)
        .expect("SDK binds the opaque Rust-owned cancellation reference");

        let execution_binding = execution_binding_ref("opaque resolution binding");
        let value = serde_json::json!({"output": "opaque-authority"});
        let result = cymule_core::ArtifactRef {
            identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
            artifact_id: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            kind: "cymule.effect-result/1".to_owned(),
        };
        let resolution = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::resolve(
                EngineStoreTarget::directory("domain"),
                process_target("plugin"),
            ),
            command: DurableCommand::ResolveEffect {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                resolution_id: "resolution:opaque".to_owned(),
                run_id: "run:opaque".to_owned(),
                intent_id:
                    "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                        .to_owned(),
                execution_binding: execution_binding.clone(),
                occurrence_binding:
                    "sha256:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
                        .to_owned(),
                claim_owner: "driver:opaque".to_owned(),
                claim_epoch: 4,
                resolution: ReconciliationResolution::ResolvedApplied,
                value: Some(value.clone()),
            },
        };
        EngineResponse::DurableExecuted {
            response: DurableResponse::EffectResolved {
                receipt: cymule_durable::EffectResolutionReceipt {
                    receipt_version: cymule_durable::EFFECT_RESOLUTION_RECEIPT_VERSION.to_owned(),
                    command: EffectResolutionCommand {
                        resolution_id: "resolution:opaque".to_owned(),
                        run_id: "run:opaque".to_owned(),
                        intent_id:
                            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                                .to_owned(),
                        execution_binding,
                        occurrence_binding:
                            "sha256:cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
                                .to_owned(),
                        claim_owner: "driver:opaque".to_owned(),
                        claim_epoch: 4,
                        resolution: ReconciliationResolution::ResolvedApplied,
                        value: Some(value.clone()),
                    },
                    actual_resolution: ReconciliationResolution::ResolvedApplied,
                    actual_value: Some(value),
                    result: Some(result),
                    receipt_id: "3".repeat(64),
                },
            },
        }
        .verify_for(&resolution)
        .expect("SDK binds the opaque Rust-owned Effect result reference");
    }

    #[test]
    fn wait_activation_receipt_binds_the_submitted_delivery_selection() {
        let command = DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: "activation:receipt-binding".to_owned(),
            source: WaitActivationSource::Signal {
                key: "signal:receipt-binding".to_owned(),
            },
            wait_ids: BTreeSet::from([
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ]),
            value: serde_json::json!({"accepted": true}),
        };
        let activation = WaitActivation {
            activation_version: cymule_durable_protocol::WAIT_ACTIVATION_VERSION.to_owned(),
            activation_id: "activation:receipt-binding".to_owned(),
            source: WaitActivationSource::Signal {
                key: "signal:receipt-binding".to_owned(),
            },
            wait_ids: BTreeSet::from([
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ]),
            result: semantic_artifact(
                cymule_durable_protocol::WAIT_RESULT_ARTIFACT_KIND,
                "wait activation result",
            ),
        };
        let request = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command,
        };
        let response_for = |activation| EngineResponse::DurableExecuted {
            response: DurableResponse::WaitActivated {
                receipt: cymule_durable_protocol::WaitActivationReceipt {
                    receipt_version: cymule_durable_protocol::WAIT_ACTIVATION_RECEIPT_VERSION
                        .to_owned(),
                    activation,
                    applied_wait_ids: BTreeSet::new(),
                    ready_run_ids: BTreeSet::new(),
                },
            },
        };
        response_for(activation.clone())
            .verify_for(&request)
            .expect("the exact activation receipt matches its command");

        let mut wrong_id = activation.clone();
        wrong_id.activation_id = "activation:forged".to_owned();
        assert!(response_for(wrong_id).verify_for(&request).is_err());

        let mut wrong_source = activation.clone();
        wrong_source.source = WaitActivationSource::Timer {
            timer_id: "timer:forged".to_owned(),
        };
        assert!(response_for(wrong_source).verify_for(&request).is_err());

        let mut wrong_waits = activation;
        wrong_waits.wait_ids = BTreeSet::from([
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ]);
        assert!(response_for(wrong_waits).verify_for(&request).is_err());
    }

    #[test]
    fn release_effect_boundaries_remain_bound_to_the_requested_intent() {
        let requested = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let other = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let request = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::execute(
                EngineStoreTarget::directory("domain"),
                process_target("plugin"),
                clock_target(),
            ),
            command: DurableCommand::ReleaseEffect {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                intent_id: requested.to_owned(),
                execution: execution(),
            },
        };
        for boundary in [
            DurableBoundary::ReconciliationRequired {
                intent_id: other.to_owned(),
            },
            DurableBoundary::EffectUnavailable {
                intent_id: other.to_owned(),
            },
            DurableBoundary::EffectNotApplied {
                intent_id: other.to_owned(),
            },
            DurableBoundary::ReleaseRequired {
                intent_ids: BTreeSet::from([other.to_owned()]),
            },
        ] {
            assert!(
                EngineResponse::DurableExecuted {
                    response: DurableResponse::RunBoundary { boundary },
                }
                .verify_for(&request)
                .is_err()
            );
        }
    }

    #[test]
    fn completed_results_require_exact_content_and_precondition_identities() {
        let result = cymule_runtime::ExecutionResult {
            run_id: "run:completed-wire".to_owned(),
            plan_id:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            value: Value::Null,
            projection_digest: "b".repeat(64),
            precondition_token:
                "pre:9007199254740991:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_owned(),
            effects: vec![
                "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    .to_owned(),
            ],
        };
        verify_completed_result(&result, "run:completed-wire", Some(&result.plan_id))
            .expect("closed completed result verifies");

        for invalid in [
            cymule_runtime::ExecutionResult {
                plan_id: "A".repeat(64),
                ..result.clone()
            },
            cymule_runtime::ExecutionResult {
                projection_digest: format!("sha256:{}", "b".repeat(64)),
                ..result.clone()
            },
            cymule_runtime::ExecutionResult {
                precondition_token: format!("pre:9007199254740992:sha256:{}", "c".repeat(64)),
                ..result.clone()
            },
            cymule_runtime::ExecutionResult {
                precondition_token: format!("pre:01:sha256:{}", "c".repeat(64)),
                ..result.clone()
            },
            cymule_runtime::ExecutionResult {
                effects: vec![format!("sha256:{}", "D".repeat(64))],
                ..result.clone()
            },
        ] {
            assert!(
                verify_completed_result(&invalid, "run:completed-wire", None).is_err(),
                "forged completed result must fail closed: {invalid:?}"
            );
        }
    }

    #[test]
    fn every_mutating_success_boundary_preserves_unknown_outcome_on_malformed_payload() {
        let plan = cymule_core::seal_plan(empty_candidate()).expect("test Plan seals");
        let run_request = EngineRequest::Run {
            plan: plan.clone(),
            input: Value::Null,
            plugin: process_target("plugin"),
            run_id: "run:wire".to_owned(),
        };
        let run_response = EngineResponse::ExecutionBoundary {
            execution: ExecutionOutcome::Completed {
                result: cymule_runtime::ExecutionResult {
                    run_id: "run:foreign".to_owned(),
                    plan_id: plan.plan_id.clone(),
                    value: Value::Null,
                    projection_digest: "a".repeat(64),
                    precondition_token: "pre:1:event:wire".to_owned(),
                    effects: Vec::new(),
                },
            },
        };

        let clock_request = EngineRequest::ObserveClock {
            target: clock_target(),
            run_id: "run:wire".to_owned(),
        };
        let valid_clock_response = EngineResponse::ClockObserved {
            result: ClockObservationResult {
                run_id: "run:wire".to_owned(),
                observation: ClockObservationRef {
                    clock_version: crate::CLOCK_OBSERVATION_VERSION.to_owned(),
                    observation_id:
                        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                            .to_owned(),
                    source_id: "clock:sdk-client-test".to_owned(),
                    source_generation:
                        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_owned(),
                    scope: cymule_durable_protocol::execution_clock_scope("run:wire")
                        .expect("Run Clock scope derives"),
                },
            },
        };
        valid_clock_response
            .verify_for(&clock_request)
            .expect("matching Clock observation verifies");
        let clock_response = EngineResponse::ClockObserved {
            result: ClockObservationResult {
                run_id: "run:foreign".to_owned(),
                observation: ClockObservationRef {
                    clock_version: crate::CLOCK_OBSERVATION_VERSION.to_owned(),
                    observation_id:
                        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                            .to_owned(),
                    source_id: "clock:foreign".to_owned(),
                    source_generation:
                        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_owned(),
                    scope: "run:foreign".to_owned(),
                },
            },
        };

        let durable_request = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::execute(
                EngineStoreTarget::directory("domain"),
                process_target("plugin"),
                clock_target(),
            ),
            command: DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run:wire".to_owned(),
                candidate: empty_candidate(),
                input: Value::Null,
                execution: execution(),
            },
        };

        let evolution_command = live_command_fixture();
        let evolution_request = live_request(evolution_command.clone());
        let mut invalid_commit = evolution_commit_fixture("evolution:wire", &evolution_command);
        invalid_commit.receipt.command.evolution_id = "evolution:foreign".to_owned();
        let evolution_response = EngineResponse::LiveEvolutionExecuted {
            commit: Box::new(invalid_commit),
        };

        for (request, response) in [
            (run_request, run_response),
            (clock_request, clock_response),
            (durable_request, EngineResponse::Verified),
            (evolution_request, evolution_response),
        ] {
            let message = response
                .verify_for(&request)
                .expect_err("malformed typed success is rejected");
            let failure = invalid_success_response(&request, &message);
            assert_eq!(failure.category, EngineFailureCategory::UnknownWorldOutcome);
            assert_eq!(failure.code.as_ref(), "invalid_engine_response");
            assert_eq!(
                failure.retry_disposition,
                Some(cymule_runtime::EngineRetryDisposition::Reconcile)
            );
        }
    }

    #[test]
    fn malformed_seal_success_remains_an_ordinary_invalid_response() {
        let candidate = empty_candidate();
        let request = EngineRequest::Seal {
            candidate: candidate.clone(),
        };
        let response = EngineResponse::Sealed {
            plan: SealedPlan {
                plan_id: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_owned(),
                candidate,
            },
        };
        let message = response
            .verify_for(&request)
            .expect_err("forged sealed Plan is rejected");
        let failure = invalid_success_response(&request, &message);
        assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
        assert_eq!(failure.code.as_ref(), "invalid_engine_response");
        assert_eq!(failure.retry_disposition, None);
    }

    #[test]
    fn local_interruption_classification_matches_the_closed_retry_matrix() {
        let candidate = empty_candidate();
        let mutation = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::execute(
                EngineStoreTarget::directory("domain"),
                process_target("plugin"),
                clock_target(),
            ),
            command: DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: "run:test".to_owned(),
                candidate,
                input: Value::Null,
                execution: execution(),
            },
        };
        let pre_spawn = interrupted_failure(&mutation, "cancelled", false);
        assert_eq!(pre_spawn.category, EngineFailureCategory::Cancelled);
        assert_eq!(
            pre_spawn.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Never)
        );
        pre_spawn
            .verify()
            .expect("pre-spawn cancellation is wire-valid");

        for kind in ["timed_out", "cancelled"] {
            let lost = interrupted_failure(&mutation, kind, true);
            assert_eq!(lost.category, EngineFailureCategory::UnknownWorldOutcome);
            assert_eq!(
                lost.retry_disposition,
                Some(cymule_runtime::EngineRetryDisposition::Reconcile)
            );
            lost.verify().expect("mutating response loss is wire-valid");
        }

        let clock_loss = interrupted_failure(
            &EngineRequest::ObserveClock {
                target: clock_target(),
                run_id: "run:clock-response-loss".to_owned(),
            },
            "timed_out",
            true,
        );
        assert_eq!(
            clock_loss.category,
            EngineFailureCategory::UnknownWorldOutcome
        );
        assert_eq!(
            clock_loss.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Reconcile)
        );
        clock_loss
            .verify()
            .expect("Clock response loss is wire-valid");

        let query = EngineRequest::ExecuteDurable {
            target: EngineDurableTarget::query(EngineStoreTarget::directory("domain")),
            command: DurableCommand::RunIndexPage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                expected_revision: None,
                cursor: None,
                limit: 1,
                max_canonical_bytes: 1024,
            },
        };
        let read_timeout = interrupted_failure(&query, "timed_out", true);
        assert_eq!(read_timeout.category, EngineFailureCategory::TimedOut);
        assert_eq!(
            read_timeout.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::RetrySameRequest)
        );
        assert_ne!(
            read_timeout.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::RefreshAndRetry)
        );
        read_timeout
            .verify()
            .expect("read-only local timeout is wire-valid");
        let read_cancel = interrupted_failure(&query, "cancelled", true);
        assert_eq!(read_cancel.category, EngineFailureCategory::Cancelled);
        assert_eq!(
            read_cancel.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Never)
        );
        read_cancel
            .verify()
            .expect("read-only cancellation is wire-valid");
    }

    #[cfg(unix)]
    #[test]
    fn pre_spawn_cancellation_is_terminal_without_starting_the_engine() {
        let cancelled = Arc::new(AtomicBool::new(true));
        let error = CliEngine::new("/definitely/missing/cymule")
            .with_cancellation(cancelled)
            .execute_durable(
                &EngineDurableTarget::execute(
                    EngineStoreTarget::directory("unused"),
                    process_target("unused"),
                    clock_target(),
                ),
                &DurableCommand::StartRun {
                    control_version: DURABLE_CONTROL_VERSION.to_owned(),
                    run_id: "run:pre-cancelled".to_owned(),
                    candidate: empty_candidate(),
                    input: Value::Null,
                    execution: execution(),
                },
            )
            .expect_err("pre-signalled cancellation wins before process spawn");
        assert_eq!(error.category, EngineFailureCategory::Cancelled);
        assert_eq!(
            error.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Never)
        );
        error
            .verify()
            .expect("pre-spawn cancellation is wire-valid");
    }

    #[cfg(unix)]
    #[test]
    fn strict_request_rejection_is_local_validation_before_engine_spawn() {
        let mut candidate = empty_candidate();
        candidate.definitions[0].input_schema = serde_json::json!({
            "unsafe_integer": serde_json::Value::Number(
                serde_json::Number::from_f64(9_007_199_254_740_992.0)
                    .expect("finite unsafe integer")
            )
        });
        let error = CliEngine::new("/definitely/missing/cymule")
            .seal(&candidate)
            .expect_err("strict local JSON validation rejects the request before spawn");
        assert_eq!(error.category, EngineFailureCategory::Validation);
        assert_eq!(error.code.as_ref(), "request_encoding_failed");
        assert_eq!(
            error.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::CorrectAndRetry)
        );
        error
            .verify()
            .expect("local validation failure is wire-valid");
    }

    #[cfg(unix)]
    #[test]
    fn engine_request_exact_limit_preserves_early_failure_and_max_plus_one_never_spawns() {
        let directory = tempfile::tempdir().expect("isolated Engine fixture directory");
        let executable = directory.path().join("early-failure-engine");
        let marker = directory.path().join("spawned");
        let mut remote_failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ValidateRequest,
            "fixture_rejected",
            "fixture rejected the request",
        );
        remote_failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        let output = serde_json::to_string(
            &EngineResponseEnvelope::<Value, EngineResponse>::failure(remote_failure),
        )
        .expect("failure envelope serializes");
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\n: > {:?}\nprintf '%s' '{}'\n",
                marker.display().to_string(),
                output,
            ),
        )
        .expect("Engine fixture writes");
        let mut permissions = std::fs::metadata(&executable)
            .expect("Engine fixture metadata reads")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&executable, permissions)
            .expect("Engine fixture becomes executable");

        let mut candidate = empty_candidate();
        candidate
            .metadata
            .insert("padding".to_owned(), String::new());
        let base = serde_json::to_vec(&EngineRequestEnvelope::new(&EngineRequest::Seal {
            candidate: candidate.clone(),
        }))
        .expect("base request serializes");
        assert!(base.len() < MAX_ENGINE_REQUEST_BYTES);
        candidate.metadata.insert(
            "padding".to_owned(),
            "x".repeat(MAX_ENGINE_REQUEST_BYTES - base.len()),
        );

        let engine = CliEngine::new(&executable).with_timeout(Duration::from_secs(5));
        let failure = engine
            .seal(&candidate)
            .expect_err("exact-bound request reaches the early-failure Engine");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.code.as_ref(), "fixture_rejected");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(
            marker.is_file(),
            "exact-bound request did not start the Engine"
        );
        std::fs::remove_file(&marker).expect("spawn marker resets");

        candidate
            .metadata
            .get_mut("padding")
            .expect("padding remains present")
            .push('x');
        let failure = engine
            .seal(&candidate)
            .expect_err("max-plus-one request is rejected before spawn");
        assert_eq!(failure.category, EngineFailureCategory::Validation);
        assert_eq!(failure.code.as_ref(), "engine_request_too_large");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        assert!(!marker.exists(), "max-plus-one request started the Engine");
    }

    #[cfg(unix)]
    #[test]
    fn early_stdin_close_cannot_forge_read_or_mutation_success() {
        let directory = tempfile::tempdir().expect("isolated Engine fixture directory");
        let write_engine = |name: &str, output: &str| {
            let executable = directory.path().join(name);
            std::fs::write(&executable, format!("#!/bin/sh\nprintf '%s' '{output}'\n"))
                .expect("Engine fixture writes");
            let mut permissions = std::fs::metadata(&executable)
                .expect("Engine fixture metadata reads")
                .permissions();
            permissions.set_mode(0o700);
            std::fs::set_permissions(&executable, permissions)
                .expect("Engine fixture becomes executable");
            executable
        };

        let mut read_candidate = empty_candidate();
        read_candidate
            .metadata
            .insert("padding".to_owned(), "x".repeat(2 * 1024 * 1024));
        let read_plan = cymule_core::seal_plan(read_candidate.clone()).expect("read Plan seals");
        let read_request = EngineRequest::Seal {
            candidate: read_candidate.clone(),
        };
        let read_inner = serde_json::to_value(&read_request).expect("read request serializes");
        let read_output = serde_json::to_string(&EngineResponseEnvelope::success(
            read_inner,
            EngineResponse::Sealed { plan: read_plan },
        ))
        .expect("read success serializes");
        assert!(read_output.len() < ENGINE_STREAM_LIMIT);
        let read_engine = write_engine("forged-read-success", &read_output);
        let failure = CliEngine::new(read_engine)
            .with_timeout(Duration::from_secs(5))
            .seal(&read_candidate)
            .expect_err("an incomplete read request cannot accept a forged success");
        assert_eq!(failure.category, EngineFailureCategory::TransportFailure);
        assert_eq!(failure.code.as_ref(), "engine_request_incomplete");
        assert_eq!(failure.retry_disposition, None);

        let plan = cymule_core::seal_plan(empty_candidate()).expect("mutation Plan seals");
        let run_id = "run:forged-early-success";
        let input = Value::String("x".repeat(2 * 1024 * 1024));
        let mutation = EngineRequest::Run {
            plan: plan.clone(),
            input: input.clone(),
            plugin: process_target("plugin"),
            run_id: run_id.to_owned(),
        };
        let mutation_inner = serde_json::to_value(&mutation).expect("mutation request serializes");
        let mutation_output = serde_json::to_string(&EngineResponseEnvelope::success(
            mutation_inner,
            EngineResponse::ExecutionBoundary {
                execution: ExecutionOutcome::Completed {
                    result: cymule_runtime::ExecutionResult {
                        run_id: run_id.to_owned(),
                        plan_id: plan.plan_id.clone(),
                        value: Value::Null,
                        projection_digest: "a".repeat(64),
                        precondition_token: format!("pre:1:sha256:{}", "b".repeat(64)),
                        effects: Vec::new(),
                    },
                },
            },
        ))
        .expect("mutation success serializes");
        assert!(mutation_output.len() < ENGINE_STREAM_LIMIT);
        let mutation_engine = write_engine("forged-mutation-success", &mutation_output);
        let failure = CliEngine::new(mutation_engine)
            .with_timeout(Duration::from_secs(5))
            .run(&plan, &input, &process_target("plugin"), run_id)
            .expect_err("an incomplete mutation request cannot accept a forged success");
        assert_eq!(failure.category, EngineFailureCategory::UnknownWorldOutcome);
        assert_eq!(failure.code.as_ref(), "engine_request_incomplete");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::Reconcile)
        );
    }

    #[cfg(unix)]
    #[test]
    fn read_only_engine_timeout_retries_the_same_request() {
        let error = CliEngine::new(fixture("slow-engine"))
            .with_timeout(Duration::from_millis(20))
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
            .expect_err("read-only request reaches its local response deadline");
        assert_eq!(error.category, EngineFailureCategory::TimedOut);
        assert_eq!(
            error.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::RetrySameRequest)
        );
        error
            .verify()
            .expect("read-only local timeout is wire-valid");
    }

    #[cfg(unix)]
    #[test]
    fn direct_child_exit_cannot_leave_a_descendant_holding_transport_pipes() {
        let directory = tempfile::tempdir().expect("temporary Engine fixture directory");
        let executable = directory.path().join("descendant-pipe-engine");
        let child_pid_path = directory.path().join("child.pid");
        let descendant_pid_path = directory.path().join("descendant.pid");
        let release_path = directory.path().join("release");
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > {:?}\n/bin/sleep 60 &\nprintf '%s' \"$!\" > {:?}\nwhile [ ! -e {:?} ]; do /bin/sleep 0.01; done\nexit 0\n",
                child_pid_path.display().to_string(),
                descendant_pid_path.display().to_string(),
                release_path.display().to_string(),
            ),
        )
        .expect("Engine fixture writes");
        let mut permissions = std::fs::metadata(&executable)
            .expect("Engine fixture metadata reads")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&executable, permissions)
            .expect("Engine fixture becomes executable");

        let process_exists = |pid: u32| {
            Command::new("/bin/kill")
                .arg("-0")
                .arg(pid.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let (result, descendant) = std::thread::scope(|scope| {
            let engine = CliEngine::new(&executable)
                .with_timeout(Duration::from_secs(30))
                .with_cancellation(cancelled.clone());
            let candidate = empty_candidate();
            let request = scope.spawn(move || engine.seal(&candidate));

            let ready_deadline = Instant::now() + Duration::from_secs(5);
            while !child_pid_path.is_file() || !descendant_pid_path.is_file() {
                assert!(
                    Instant::now() < ready_deadline,
                    "Engine fixture did not publish its ready marker"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            let child = std::fs::read_to_string(&child_pid_path)
                .expect("direct child PID was retained")
                .parse::<u32>()
                .expect("direct child PID is numeric");
            let descendant = std::fs::read_to_string(&descendant_pid_path)
                .expect("descendant PID was retained")
                .parse::<u32>()
                .expect("descendant PID is numeric");

            std::fs::write(&release_path, b"release").expect("direct child is released");
            let exit_deadline = Instant::now() + Duration::from_secs(5);
            while process_exists(child) {
                assert!(
                    Instant::now() < exit_deadline,
                    "Engine direct child {child} did not exit after release"
                );
                std::thread::sleep(Duration::from_millis(5));
            }

            let cancelled_at = Instant::now();
            cancelled.store(true, Ordering::Release);
            let result = request
                .join()
                .expect("Engine request thread does not panic");
            assert!(
                cancelled_at.elapsed() < Duration::from_secs(2),
                "transport did not react to cancellation after direct-child exit"
            );
            (result, descendant)
        });
        let error = result.expect_err(
            "cancellation terminates a descendant that retained pipes after direct-child exit",
        );
        assert_eq!(error.category, EngineFailureCategory::Cancelled);
        assert_eq!(
            error.retry_disposition,
            Some(cymule_runtime::EngineRetryDisposition::Never)
        );

        let reaped_before = Instant::now();
        while process_exists(descendant) {
            assert!(
                reaped_before.elapsed() < Duration::from_secs(2),
                "Engine descendant {descendant} survived process-group termination"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn clock_success_binds_the_exact_requested_run_scope() {
        let request = EngineRequest::ObserveClock {
            target: clock_target(),
            run_id: "run:clock-scope".to_owned(),
        };
        let response = |run_id: &str, scope: String| EngineResponse::ClockObserved {
            result: ClockObservationResult {
                run_id: run_id.to_owned(),
                observation: ClockObservationRef {
                    clock_version: crate::CLOCK_OBSERVATION_VERSION.to_owned(),
                    observation_id: format!("sha256:{}", "d".repeat(64)),
                    source_id: "clock:sdk-client-test".to_owned(),
                    source_generation: format!("sha256:{}", "a".repeat(64)),
                    scope,
                },
            },
        };
        response(
            "run:clock-scope",
            cymule_durable_protocol::execution_clock_scope("run:clock-scope")
                .expect("requested Run Clock scope derives"),
        )
        .verify_for(&request)
        .expect("exact Run Clock scope verifies");
        assert!(
            response(
                "run:foreign",
                cymule_durable_protocol::execution_clock_scope("run:foreign")
                    .expect("foreign Run Clock scope derives"),
            )
            .verify_for(&request)
            .is_err(),
            "Clock success must reject another Run scope"
        );
    }

    #[cfg(unix)]
    #[test]
    fn large_engine_response_is_drained_before_child_exit() {
        let engine =
            CliEngine::new(fixture("large-response-engine")).with_timeout(Duration::from_secs(2));
        let error = engine
            .seal(&empty_candidate())
            .expect_err("large forged response is drained, then rejected");
        assert_eq!(error.category, EngineFailureCategory::TransportFailure);
        assert_eq!(error.code.as_ref(), "invalid_engine_response");
    }

    #[cfg(unix)]
    #[test]
    fn mutating_response_loss_requires_reconciliation() {
        let engine = CliEngine::new(fixture("response-loss-engine"));
        let error = engine
            .execute_durable(
                &EngineDurableTarget::execute(
                    EngineStoreTarget::directory("unused"),
                    process_target("unused"),
                    clock_target(),
                ),
                &DurableCommand::ResumeRun {
                    control_version: DURABLE_CONTROL_VERSION.to_owned(),
                    run_id: "run:response-loss".to_owned(),
                    execution: execution(),
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
