use cymule_durable::{
    DurableCoordinator, DurableStore, JournalBatch, JournalRecord, WaitKind, WaitState,
};
use serde::{Deserialize, Serialize};

use crate::{ResourceError, ResourceHandle, ResourceResult};

/// Frozen Run-to-Run handoff record version.
pub const RESOURCE_HANDOFF_VERSION: &str = "cymule.resource-handoff/1";
/// Frozen handoff-to-input activation record version.
pub const RESOURCE_HANDOFF_ACTIVATION_VERSION: &str = "cymule.resource-handoff-activation/1";

/// One durable resource transfer between two Runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoff {
    /// Handoff wire version.
    pub handoff_version: String,
    /// Caller-supplied idempotency identity.
    pub transfer_id: String,
    /// Producing Run.
    pub from_run: String,
    /// Consuming Run.
    pub to_run: String,
    /// Stable target state/output slot.
    pub slot: String,
    /// Provider-neutral resource handle.
    pub resource: ResourceHandle,
}

/// Durable evidence that one handoff completed one target input wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffActivation {
    /// Activation wire version.
    pub activation_version: String,
    /// Stable activation/idempotency identity.
    pub activation_id: String,
    /// Source transfer identity.
    pub transfer_id: String,
    /// Consuming Run.
    pub to_run: String,
    /// Exact completed input wait.
    pub wait_id: String,
    /// Artifact containing the canonical Resource Handle.
    pub result: cymule_core::ArtifactRef,
}

impl ResourceHandoff {
    /// Validate stable identities and the embedded Resource.
    ///
    /// # Errors
    ///
    /// Returns an error for empty identities, a self-transfer, or invalid
    /// resource descriptor.
    pub fn validate(&self) -> ResourceResult<()> {
        if self.handoff_version != RESOURCE_HANDOFF_VERSION {
            return Err(ResourceError::Validation(format!(
                "unsupported resource handoff version {:?}",
                self.handoff_version
            )));
        }
        for (kind, value) in [
            ("transfer", self.transfer_id.as_str()),
            ("source Run", self.from_run.as_str()),
            ("target Run", self.to_run.as_str()),
            ("slot", self.slot.as_str()),
        ] {
            if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
                return Err(ResourceError::Validation(format!(
                    "resource {kind} identity must contain 1..=512 non-control characters"
                )));
            }
        }
        if self.from_run == self.to_run {
            return Err(ResourceError::Validation(
                "resource handoff requires distinct source and target Runs".to_owned(),
            ));
        }
        self.resource.verify()
    }
}

/// M1-backed Run-to-Run resource handoff operations.
pub struct ResourceHandoffController;

impl ResourceHandoffController {
    /// Commit one idempotent handoff to the target Run's typed journal.
    ///
    /// # Errors
    ///
    /// Returns an error when either Run is absent, the handoff is invalid, its
    /// transfer ID conflicts, or the M1 CAS fails.
    pub fn transfer<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        handoff: &ResourceHandoff,
    ) -> ResourceResult<String> {
        let _ = ensure_admissible(coordinator, handoff)?;
        let payload = serde_json::to_value(handoff)
            .map_err(|error| ResourceError::Persistence(error.to_string()))?;
        let record = JournalRecord::new(&handoff.transfer_id, RESOURCE_HANDOFF_VERSION, payload)
            .map_err(|error| ResourceError::Persistence(error.to_string()))?;
        coordinator
            .append_journal_record(&handoff_journal_id(&handoff.to_run), record)
            .map_err(|error| match error {
                cymule_durable::DurableError::IllegalTransition(message) => {
                    ResourceError::Conflict(message)
                }
                other => ResourceError::Persistence(other.to_string()),
            })
    }

    /// Atomically record a handoff and complete one matching target input wait.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid transfer, a non-input or mismatched wait,
    /// conflicting activation identity, unrelated Machine mutation, or M1 CAS
    /// failure.
    pub fn activate_input<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        handoff: &ResourceHandoff,
        wait_id: &str,
    ) -> ResourceResult<ResourceHandoffActivation> {
        let mut machine = ensure_admissible(coordinator, handoff)?;
        let wait = coordinator
            .state()
            .map_err(|error| persistence(&error))?
            .waits
            .get(wait_id)
            .ok_or_else(|| ResourceError::NotFound(format!("input wait {wait_id} is missing")))?;
        if wait.run_id != handoff.to_run {
            return Err(ResourceError::Validation(
                "resource handoff wait belongs to another Run".to_owned(),
            ));
        }
        match &wait.kind {
            WaitKind::Input { correlation, .. } if correlation == &handoff.slot => {}
            WaitKind::Input { .. } => {
                return Err(ResourceError::Validation(
                    "resource handoff slot does not match input correlation".to_owned(),
                ));
            }
            _ => {
                return Err(ResourceError::Validation(
                    "resource handoff can activate only an input wait".to_owned(),
                ));
            }
        }
        if wait.state == WaitState::Cancelled {
            return Err(ResourceError::Conflict(
                "resource handoff input wait is cancelled".to_owned(),
            ));
        }
        let result = machine
            .put_artifact(
                "cymule.resource-handoff-input/1",
                cymule_core::canonical_bytes(&handoff.resource)
                    .map_err(|error| ResourceError::Persistence(error.to_string()))?,
            )
            .map_err(|error| ResourceError::Persistence(error.to_string()))?;
        let activation = ResourceHandoffActivation {
            activation_version: RESOURCE_HANDOFF_ACTIVATION_VERSION.to_owned(),
            activation_id: format!("activation:{}:{wait_id}", handoff.transfer_id),
            transfer_id: handoff.transfer_id.clone(),
            to_run: handoff.to_run.clone(),
            wait_id: wait_id.to_owned(),
            result: result.clone(),
        };
        let handoff_record = JournalRecord::new(
            &handoff.transfer_id,
            RESOURCE_HANDOFF_VERSION,
            serde_json::to_value(handoff)
                .map_err(|error| ResourceError::Persistence(error.to_string()))?,
        )
        .map_err(|error| persistence(&error))?;
        let activation_record = JournalRecord::new(
            &activation.activation_id,
            RESOURCE_HANDOFF_ACTIVATION_VERSION,
            serde_json::to_value(&activation)
                .map_err(|error| ResourceError::Persistence(error.to_string()))?,
        )
        .map_err(|error| persistence(&error))?;
        coordinator
            .checkpoint_input_wait_journals(
                &machine,
                &result,
                wait_id,
                &[
                    JournalBatch {
                        journal_id: handoff_journal_id(&handoff.to_run),
                        records: vec![handoff_record],
                    },
                    JournalBatch {
                        journal_id: activation_journal_id(&handoff.to_run),
                        records: vec![activation_record],
                    },
                ],
            )
            .map_err(map_durable_error)?;
        Ok(activation)
    }

    /// Replay every incoming handoff for one target Run.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported/tampered records or persistence failure.
    pub fn incoming<S: DurableStore>(
        coordinator: &DurableCoordinator<S>,
        to_run: &str,
    ) -> ResourceResult<Vec<ResourceHandoff>> {
        coordinator
            .journal_records(&handoff_journal_id(to_run))
            .map_err(|error| ResourceError::Persistence(error.to_string()))?
            .iter()
            .map(|record| {
                if record.schema != RESOURCE_HANDOFF_VERSION {
                    return Err(ResourceError::Persistence(format!(
                        "resource handoff {} has unsupported schema {}",
                        record.record_id, record.schema
                    )));
                }
                record
                    .verify()
                    .map_err(|error| ResourceError::Persistence(error.to_string()))?;
                let handoff: ResourceHandoff = serde_json::from_value(record.payload.clone())
                    .map_err(|error| ResourceError::Persistence(error.to_string()))?;
                handoff.validate()?;
                if handoff.transfer_id != record.record_id || handoff.to_run != to_run {
                    return Err(ResourceError::Persistence(format!(
                        "resource handoff record {} has mismatched identity or target",
                        record.record_id
                    )));
                }
                Ok(handoff)
            })
            .collect()
    }
}

fn ensure_admissible<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    handoff: &ResourceHandoff,
) -> ResourceResult<cymule_core::Machine> {
    handoff.validate()?;
    let machine = coordinator
        .restore_machine()
        .map_err(|error| persistence(&error))?;
    for run_id in [&handoff.from_run, &handoff.to_run] {
        if !machine.projection().runs.contains_key(run_id) {
            return Err(ResourceError::NotFound(format!(
                "resource handoff Run {run_id} does not exist"
            )));
        }
    }
    let target = &machine.projection().runs[&handoff.to_run];
    if !matches!(
        target.status,
        cymule_core::RunStatus::Active | cymule_core::RunStatus::Waiting
    ) {
        return Err(ResourceError::Validation(format!(
            "target Run {} cannot accept resource handoffs in state {:?}",
            handoff.to_run, target.status
        )));
    }
    for existing in ResourceHandoffController::incoming(coordinator, &handoff.to_run)? {
        if existing.slot == handoff.slot && existing != *handoff {
            return Err(ResourceError::Conflict(format!(
                "target Run {} slot {} already has transfer {}",
                handoff.to_run, handoff.slot, existing.transfer_id
            )));
        }
    }
    Ok(machine)
}

fn persistence(error: &cymule_durable::DurableError) -> ResourceError {
    ResourceError::Persistence(error.to_string())
}

fn map_durable_error(error: cymule_durable::DurableError) -> ResourceError {
    match error {
        cymule_durable::DurableError::IllegalTransition(message)
        | cymule_durable::DurableError::Conflict {
            expected: _,
            current: Some(message),
        } => ResourceError::Conflict(message),
        other => ResourceError::Persistence(other.to_string()),
    }
}

fn handoff_journal_id(to_run: &str) -> String {
    format!("cymule.resources/{to_run}")
}

fn activation_journal_id(to_run: &str) -> String {
    format!("cymule.resource-activations/{to_run}")
}
