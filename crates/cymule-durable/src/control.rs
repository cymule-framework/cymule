use std::collections::BTreeSet;

use cymule_core::{ArtifactRef, PlanCandidate};
use cymule_runtime::{ExecutionResult, PluginHost};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Continuation, DriveOutcome, DurableError, DurableResult, DurableStore, EffectDispatch,
    ResumableRuntime, WaitActivationSource, WaitCondition,
};

/// Frozen provider-neutral M1 control protocol version.
pub const DURABLE_CONTROL_VERSION: &str = "cymule.durable-control/1";

/// Closed stateful control and query union for one durable domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableCommand {
    /// Seal, create, and drive one Run to its next durable boundary.
    StartRun {
        /// Protocol version.
        control_version: String,
        /// Stable Run identity and idempotency key.
        run_id: String,
        /// Candidate sealed by the trusted Rust runtime.
        candidate: PlanCandidate,
        /// Immutable initial input.
        input: Value,
    },
    /// Resume one existing ready or running Run.
    ResumeRun {
        /// Protocol version.
        control_version: String,
        /// Existing Run identity.
        run_id: String,
    },
    /// Admit one identified external signal or timer observation.
    ActivateWait {
        /// Protocol version.
        control_version: String,
        /// Stable transport delivery and deduplication identity.
        activation_id: String,
        /// Plan-declared activation source.
        source: WaitActivationSource,
        /// Exact targets selected from the parked index.
        wait_ids: BTreeSet<String>,
        /// Typed result sealed to an Artifact by Rust before admission.
        value: Value,
    },
    /// Explicitly release one prepared effect after its scope committed.
    ReleaseEffect {
        /// Protocol version.
        control_version: String,
        /// Structural effect intent identity.
        intent_id: String,
    },
    /// Query one Run without mutation.
    QueryRun {
        /// Protocol version.
        control_version: String,
        /// Stable query identity for transport tracing.
        query_id: String,
        /// Run to inspect.
        run_id: String,
    },
    /// Query the complete Run index of one durable domain without mutation.
    QueryDomain {
        /// Protocol version.
        control_version: String,
        /// Stable query identity for transport tracing.
        query_id: String,
    },
}

impl DurableCommand {
    /// Validate the closed command independently of current durable state.
    pub fn verify(&self) -> DurableResult<()> {
        let version = match self {
            Self::StartRun {
                control_version, ..
            }
            | Self::ResumeRun {
                control_version, ..
            }
            | Self::ActivateWait {
                control_version, ..
            }
            | Self::ReleaseEffect {
                control_version, ..
            }
            | Self::QueryRun {
                control_version, ..
            }
            | Self::QueryDomain {
                control_version, ..
            } => control_version,
        };
        if version != DURABLE_CONTROL_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable control version {version}"
            )));
        }
        match self {
            Self::StartRun {
                run_id, candidate, ..
            } => {
                validate_identity("Run", run_id)?;
                candidate.clone().seal()?;
            }
            Self::ResumeRun { run_id, .. } => validate_identity("Run", run_id)?,
            Self::ActivateWait {
                activation_id,
                source,
                wait_ids,
                ..
            } => {
                validate_identity("activation", activation_id)?;
                source.verify()?;
                if wait_ids.is_empty() || wait_ids.len() > crate::MAX_WAIT_DELIVERY_TARGETS {
                    return Err(DurableError::Validation(format!(
                        "wait activation target count must be 1..={}",
                        crate::MAX_WAIT_DELIVERY_TARGETS
                    )));
                }
                for wait_id in wait_ids {
                    validate_identity("wait", wait_id)?;
                }
            }
            Self::ReleaseEffect { intent_id, .. } => {
                validate_identity("effect intent", intent_id)?;
            }
            Self::QueryRun {
                query_id, run_id, ..
            } => {
                validate_identity("query", query_id)?;
                validate_identity("Run", run_id)?;
            }
            Self::QueryDomain { query_id, .. } => validate_identity("query", query_id)?,
        }
        Ok(())
    }
}

/// Serializable Run boundary returned by mutation controls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableBoundary {
    /// The Run parked at one durable wait.
    Suspended {
        /// Stable wait identity.
        wait_id: String,
    },
    /// An effect remains ambiguous and requires reconciliation.
    ReconciliationRequired {
        /// Original structural effect intent.
        intent_id: String,
    },
    /// One or more explicit effects require caller release.
    ReleaseRequired {
        /// Stable structural effect intents.
        intent_ids: BTreeSet<String>,
    },
    /// The Run committed its terminal result.
    Completed {
        /// Full execution result and replay evidence.
        result: ExecutionResult,
    },
}

impl From<DriveOutcome> for DurableBoundary {
    fn from(outcome: DriveOutcome) -> Self {
        match outcome {
            DriveOutcome::Suspended { wait_id } => Self::Suspended { wait_id },
            DriveOutcome::ReconciliationRequired { intent_id } => {
                Self::ReconciliationRequired { intent_id }
            }
            DriveOutcome::ReleaseRequired { intent_ids } => Self::ReleaseRequired { intent_ids },
            DriveOutcome::Completed(result) => Self::Completed { result },
        }
    }
}

/// Exact query projection for one Run under the current domain revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableRunView {
    /// Store-owned complete-state revision.
    pub revision: String,
    /// Complete resumable Continuation.
    pub continuation: Continuation,
    /// This Run's durable waits in stable identity order.
    pub waits: Vec<WaitCondition>,
    /// This Run's durable outbox entries in stable intent order.
    pub effects: Vec<EffectDispatch>,
    /// Optional canonical terminal result Artifact.
    pub result: Option<ArtifactRef>,
}

/// Read-only index of one durable domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableDomainView {
    /// Current store revision, absent before the first Run.
    pub revision: Option<String>,
    /// Stable Run identities currently retained by the domain.
    pub run_ids: Vec<String>,
}

/// Closed response union for the M1 control protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableResponse {
    /// Run mutation reached a durable execution boundary.
    RunBoundary {
        /// Boundary receipt.
        boundary: DurableBoundary,
    },
    /// An identified wait delivery was admitted atomically.
    WaitActivated {
        /// Runs that are now ready for a separately fenced resume.
        ready_run_ids: BTreeSet<String>,
    },
    /// One Run query result.
    Run {
        /// Exact view, or absent when the Run does not exist.
        run: Option<Box<DurableRunView>>,
    },
    /// Whole-domain Run index.
    Domain {
        /// Current read-only domain view.
        domain: DurableDomainView,
    },
}

/// Stateful Rust admission authority for one durable runtime.
pub struct DurableRuntimeControl<S, P> {
    runtime: ResumableRuntime<S, P>,
}

impl<S: DurableStore, P: PluginHost> DurableRuntimeControl<S, P> {
    /// Wrap an opened resumable runtime.
    pub const fn new(runtime: ResumableRuntime<S, P>) -> Self {
        Self { runtime }
    }

    /// Submit one verified command to the Rust authority.
    pub fn submit(&mut self, command: DurableCommand) -> DurableResult<DurableResponse> {
        command.verify()?;
        match command {
            DurableCommand::StartRun {
                run_id,
                candidate,
                input,
                ..
            } => Ok(DurableResponse::RunBoundary {
                boundary: self.runtime.start(candidate, &input, run_id)?.into(),
            }),
            DurableCommand::ResumeRun { run_id, .. } => Ok(DurableResponse::RunBoundary {
                boundary: self.runtime.resume(&run_id)?.into(),
            }),
            DurableCommand::ActivateWait {
                activation_id,
                source,
                wait_ids,
                value,
                ..
            } => Ok(DurableResponse::WaitActivated {
                ready_run_ids: self.runtime.admit_wait_activation(
                    activation_id,
                    source,
                    wait_ids,
                    &value,
                )?,
            }),
            DurableCommand::ReleaseEffect { intent_id, .. } => Ok(DurableResponse::RunBoundary {
                boundary: self.runtime.release_effect(&intent_id)?.into(),
            }),
            DurableCommand::QueryRun { run_id, .. } => Ok(DurableResponse::Run {
                run: query_run(&self.runtime, &run_id)?.map(Box::new),
            }),
            DurableCommand::QueryDomain { .. } => Ok(DurableResponse::Domain {
                domain: query_domain(&self.runtime)?,
            }),
        }
    }

    /// Borrow the underlying runtime for adapter-specific orchestration.
    pub const fn runtime(&self) -> &ResumableRuntime<S, P> {
        &self.runtime
    }

    /// Consume the controller and return the runtime.
    pub fn into_runtime(self) -> ResumableRuntime<S, P> {
        self.runtime
    }
}

fn query_domain<S: DurableStore, P: PluginHost>(
    runtime: &ResumableRuntime<S, P>,
) -> DurableResult<DurableDomainView> {
    let Some(revision) = runtime.coordinator().revision() else {
        return Ok(DurableDomainView {
            revision: None,
            run_ids: Vec::new(),
        });
    };
    Ok(DurableDomainView {
        revision: Some(revision.to_owned()),
        run_ids: runtime
            .coordinator()
            .state()?
            .continuations
            .keys()
            .cloned()
            .collect(),
    })
}

fn query_run<S: DurableStore, P: PluginHost>(
    runtime: &ResumableRuntime<S, P>,
    run_id: &str,
) -> DurableResult<Option<DurableRunView>> {
    let Some(revision) = runtime.coordinator().revision() else {
        return Ok(None);
    };
    let state = runtime.coordinator().state()?;
    let Some(continuation) = state.continuations.get(run_id).cloned() else {
        return Ok(None);
    };
    let machine = runtime.coordinator().restore_machine()?;
    let result = machine
        .projection()
        .runs
        .get(run_id)
        .and_then(|run| run.result.clone());
    Ok(Some(DurableRunView {
        revision: revision.to_owned(),
        continuation,
        waits: state
            .waits
            .values()
            .filter(|wait| wait.run_id == run_id)
            .cloned()
            .collect(),
        effects: state
            .outbox
            .values()
            .filter(|effect| effect.run_id == run_id)
            .cloned()
            .collect(),
        result,
    }))
}

fn validate_identity(kind: &str, value: &str) -> DurableResult<()> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(DurableError::Validation(format!(
            "durable {kind} identity must contain 1..=512 printable characters"
        )));
    }
    Ok(())
}
