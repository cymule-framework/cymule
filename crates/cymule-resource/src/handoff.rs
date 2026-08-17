use cymule_durable::{DurableCoordinator, DurableStore, JournalRecord};
use serde::{Deserialize, Serialize};

use crate::{ResourceError, ResourceHandle, ResourceResult};

/// Frozen Run-to-Run handoff record version.
pub const RESOURCE_HANDOFF_VERSION: &str = "cymule.resource-handoff/1";

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
        handoff.validate()?;
        let machine = coordinator
            .restore_machine()
            .map_err(|error| ResourceError::Persistence(error.to_string()))?;
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
        for existing in Self::incoming(coordinator, &handoff.to_run)? {
            if existing.slot == handoff.slot && existing != *handoff {
                return Err(ResourceError::Conflict(format!(
                    "target Run {} slot {} already has transfer {}",
                    handoff.to_run, handoff.slot, existing.transfer_id
                )));
            }
        }
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

fn handoff_journal_id(to_run: &str) -> String {
    format!("cymule.resources/{to_run}")
}
