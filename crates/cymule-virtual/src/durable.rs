use std::collections::BTreeSet;

use cymule_core::Machine;
use cymule_durable::{
    DurableCoordinator, DurableStore, JournalBatch, JournalRecord, WaitActivation,
};
use serde::{Deserialize, Serialize};

use crate::{
    ClaimedWork, FrontierLimits, RegionSource, VIRTUAL_WORK_CONTROL_VERSION, VirtualError,
    VirtualResult, VirtualScheduler, VirtualSnapshot, WorkOccurrence, WorkResolution,
    WorkResolutionCommand,
};

/// Versioned M3 scheduler checkpoint stored in an M1 application journal.
pub const VIRTUAL_CHECKPOINT_SCHEMA: &str = "cymule.virtual-checkpoint/1";

/// One full bounded scheduler checkpoint with explicit journal lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCheckpoint {
    /// Checkpoint schema and semantic version.
    pub checkpoint_version: String,
    /// Stable caller-supplied idempotency identity.
    pub checkpoint_id: String,
    /// Previous checkpoint record in this exact journal.
    pub parent_checkpoint: Option<String>,
    /// Portable bounded scheduler state, including every region cursor.
    pub snapshot: VirtualSnapshot,
    /// Control command committed by this checkpoint, when any.
    #[serde(default)]
    pub control: Option<WorkResolutionCommand>,
    /// Occurrence receipt returned by the control command.
    #[serde(default)]
    pub receipt_occurrence_id: Option<String>,
}

/// M1 journal integration for the M3 virtual scheduler.
pub struct DurableVirtualController;

impl DurableVirtualController {
    /// Rebuild the scheduler from an ordered M1 application journal.
    pub fn load<S: DurableStore>(
        coordinator: &DurableCoordinator<S>,
        journal_id: &str,
        limits: FrontierLimits,
    ) -> VirtualResult<VirtualScheduler> {
        let records = coordinator
            .journal_records(journal_id)
            .map_err(durable_error)?;
        if records.is_empty() {
            return VirtualScheduler::new(limits);
        }
        let mut parent = None;
        let mut restored = None;
        for record in records {
            let checkpoint = decode_checkpoint(record)?;
            if checkpoint.parent_checkpoint != parent {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} has a discontinuous parent",
                    checkpoint.checkpoint_id
                )));
            }
            restored = Some(VirtualScheduler::restore(limits, checkpoint.snapshot)?);
            parent = Some(checkpoint.checkpoint_id);
        }
        restored.ok_or_else(|| {
            VirtualError::Validation("virtual checkpoint journal did not restore".to_owned())
        })
    }

    /// Persist the current scheduler snapshot under one idempotent checkpoint.
    pub fn checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &VirtualScheduler,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> VirtualResult<String> {
        let record = checkpoint_record(coordinator, scheduler, journal_id, checkpoint_id)?;
        coordinator
            .append_journal_record(journal_id, record)
            .map_err(durable_error)
    }

    /// Materialize one bounded source page and commit its cursor plus frontier
    /// in one M1 journal record.
    ///
    /// The in-memory scheduler rolls back when validation or CAS fails. Source
    /// adapters must return the same page for the same immutable cursor so a
    /// caller can reopen and retry after an unknown acknowledgement.
    pub fn fill_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        source: &mut impl RegionSource,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> VirtualResult<usize> {
        let before = scheduler.clone();
        let added = match scheduler.fill(source) {
            Ok(added) => added,
            Err(error) => {
                *scheduler = before;
                return Err(error);
            }
        };
        if let Err(error) = Self::checkpoint(coordinator, scheduler, journal_id, checkpoint_id) {
            *scheduler = before;
            return Err(error);
        }
        Ok(added)
    }

    /// Claim one bounded work item and persist its binding-pinned running
    /// occurrence before a worker may execute it.
    pub fn claim_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        owner: &str,
        occurrence_binding: &str,
        capabilities: &BTreeSet<String>,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> VirtualResult<Option<ClaimedWork>> {
        let before = scheduler.clone();
        let claim = scheduler.claim(owner, occurrence_binding, capabilities)?;
        let Some(claim) = claim else {
            return Ok(None);
        };
        if let Err(error) = Self::checkpoint(coordinator, scheduler, journal_id, checkpoint_id) {
            *scheduler = before;
            return Err(error);
        }
        Ok(Some(claim))
    }

    /// Apply one provider-neutral idempotent work-resolution command and
    /// atomically checkpoint its output or evidence Artifacts.
    pub fn resolve_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        machine: &Machine,
        command: &WorkResolutionCommand,
        journal_id: &str,
    ) -> VirtualResult<WorkOccurrence> {
        if command.control_version != VIRTUAL_WORK_CONTROL_VERSION
            || command.command_id.is_empty()
            || command.work_id.is_empty()
            || command.owner.is_empty()
            || command.epoch == 0
        {
            return Err(VirtualError::Validation(
                "virtual work control command is malformed".to_owned(),
            ));
        }
        if let Some(receipt) = replay_control_receipt(coordinator, scheduler, journal_id, command)?
        {
            return Ok(receipt);
        }
        let before = scheduler.clone();
        let occurrence = scheduler.resolve(
            &command.work_id,
            &command.owner,
            command.epoch,
            &command.resolution,
        )?;
        let record = match control_checkpoint_record(
            coordinator,
            scheduler,
            journal_id,
            command,
            &occurrence.occurrence_id,
        ) {
            Ok(record) => record,
            Err(error) => {
                *scheduler = before;
                return Err(error);
            }
        };
        let batch = JournalBatch {
            journal_id: journal_id.to_owned(),
            records: vec![record],
        };
        if let Err(error) = coordinator.checkpoint_artifact_journals(
            machine,
            &resolution_artifacts(&command.resolution),
            &[batch],
        ) {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(occurrence)
    }

    /// Atomically admit one M1 wait activation and publish the exact M3 parked
    /// work wake-up produced by its selected wait IDs.
    pub fn activate_and_wake<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        machine: &Machine,
        activation: WaitActivation,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> VirtualResult<usize> {
        let before = scheduler.clone();
        let woken = scheduler.wake_activation(&activation);
        let record = match checkpoint_record(coordinator, scheduler, journal_id, checkpoint_id) {
            Ok(record) => record,
            Err(error) => {
                *scheduler = before;
                return Err(error);
            }
        };
        let batch = JournalBatch {
            journal_id: journal_id.to_owned(),
            records: vec![record],
        };
        if let Err(error) =
            coordinator.checkpoint_wait_activation_journals(machine, activation, &[batch])
        {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(woken)
    }
}

fn checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    checkpoint_id: &str,
) -> VirtualResult<JournalRecord> {
    if checkpoint_id.is_empty() {
        return Err(VirtualError::Validation(
            "virtual checkpoint identity must not be empty".to_owned(),
        ));
    }
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    if let Some(existing) = records
        .iter()
        .find(|record| record.record_id == checkpoint_id)
    {
        if records.last().map(|record| record.record_id.as_str()) != Some(checkpoint_id) {
            return Err(VirtualError::Conflict(format!(
                "virtual checkpoint {checkpoint_id} is not the current journal head"
            )));
        }
        let checkpoint = decode_checkpoint(existing)?;
        if checkpoint.control.is_some() || checkpoint.snapshot != scheduler.snapshot() {
            return Err(VirtualError::Conflict(format!(
                "virtual checkpoint {checkpoint_id} already has different state"
            )));
        }
        return Ok(existing.clone());
    }
    let checkpoint = VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: checkpoint_id.to_owned(),
        parent_checkpoint: records.last().map(|record| record.record_id.clone()),
        snapshot: scheduler.snapshot(),
        control: None,
        receipt_occurrence_id: None,
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(checkpoint_id, VIRTUAL_CHECKPOINT_SCHEMA, payload).map_err(durable_error)
}

fn control_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &WorkResolutionCommand,
    occurrence_id: &str,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    if records
        .iter()
        .any(|record| record.record_id == command.command_id)
    {
        return Err(VirtualError::Conflict(format!(
            "virtual work command {} already exists without a replayable receipt",
            command.command_id
        )));
    }
    let checkpoint = VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: command.command_id.clone(),
        parent_checkpoint: records.last().map(|record| record.record_id.clone()),
        snapshot: scheduler.snapshot(),
        control: Some(command.clone()),
        receipt_occurrence_id: Some(occurrence_id.to_owned()),
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(&command.command_id, VIRTUAL_CHECKPOINT_SCHEMA, payload)
        .map_err(durable_error)
}

fn replay_control_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &WorkResolutionCommand,
) -> VirtualResult<Option<WorkOccurrence>> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    let Some(record) = records
        .iter()
        .find(|record| record.record_id == command.command_id)
    else {
        return Ok(None);
    };
    let checkpoint = decode_checkpoint(record)?;
    if checkpoint.control.as_ref() != Some(command) {
        return Err(VirtualError::Conflict(format!(
            "virtual work command {} was reused with different semantics",
            command.command_id
        )));
    }
    let occurrence_id = checkpoint.receipt_occurrence_id.ok_or_else(|| {
        VirtualError::Validation(format!(
            "virtual work command {} has no occurrence receipt",
            command.command_id
        ))
    })?;
    let occurrence = scheduler.occurrence(&occurrence_id).ok_or_else(|| {
        VirtualError::Validation(format!(
            "virtual work command {} receipt occurrence is unavailable",
            command.command_id
        ))
    })?;
    Ok(Some(occurrence.clone()))
}

fn decode_checkpoint(record: &JournalRecord) -> VirtualResult<VirtualCheckpoint> {
    if record.schema != VIRTUAL_CHECKPOINT_SCHEMA {
        return Err(VirtualError::Validation(format!(
            "virtual checkpoint {} uses schema {}",
            record.record_id, record.schema
        )));
    }
    let checkpoint: VirtualCheckpoint = serde_json::from_value(record.payload.clone())
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    if checkpoint.checkpoint_version != VIRTUAL_CHECKPOINT_SCHEMA
        || checkpoint.checkpoint_id != record.record_id
    {
        return Err(VirtualError::Validation(format!(
            "virtual checkpoint {} identity or version does not match its record",
            record.record_id
        )));
    }
    match (&checkpoint.control, &checkpoint.receipt_occurrence_id) {
        (None, None) => {}
        (Some(command), Some(occurrence_id)) => {
            let occurrence = checkpoint
                .snapshot
                .occurrences
                .get(occurrence_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} control receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_WORK_CONTROL_VERSION
                || command.command_id != record.record_id
                || !control_matches_occurrence(command, occurrence)
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} control receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial control receipt",
                record.record_id
            )));
        }
    }
    Ok(checkpoint)
}

fn durable_error(error: impl std::fmt::Display) -> VirtualError {
    VirtualError::Durable(error.to_string())
}

fn resolution_artifacts(resolution: &WorkResolution) -> BTreeSet<cymule_core::ArtifactRef> {
    match resolution {
        WorkResolution::Succeeded { result } => BTreeSet::from([result.clone()]),
        WorkResolution::Retry { error, .. } | WorkResolution::Failed { error } => {
            BTreeSet::from([error.clone()])
        }
        WorkResolution::Cancelled { reason } => BTreeSet::from([reason.clone()]),
        WorkResolution::Parked { .. } => BTreeSet::new(),
    }
}

fn control_matches_occurrence(
    command: &WorkResolutionCommand,
    occurrence: &WorkOccurrence,
) -> bool {
    if command.work_id != occurrence.work_id
        || command.owner != occurrence.owner
        || command.epoch != occurrence.epoch
    {
        return false;
    }
    match &command.resolution {
        WorkResolution::Succeeded { result } => {
            occurrence.state == crate::WorkOccurrenceState::Succeeded
                && occurrence.result.as_ref() == Some(result)
                && occurrence.error.is_none()
                && occurrence.next_reason.is_none()
        }
        WorkResolution::Retry { error, next_reason } => {
            occurrence.state == crate::WorkOccurrenceState::RetryScheduled
                && occurrence.result.is_none()
                && occurrence.error.as_ref() == Some(error)
                && occurrence.next_reason == *next_reason
        }
        WorkResolution::Parked { reason } => {
            occurrence.state == crate::WorkOccurrenceState::Parked
                && occurrence.result.is_none()
                && occurrence.error.is_none()
                && occurrence.next_reason.as_ref() == Some(reason)
        }
        WorkResolution::Failed { error } => {
            occurrence.state == crate::WorkOccurrenceState::Failed
                && occurrence.result.is_none()
                && occurrence.error.as_ref() == Some(error)
                && occurrence.next_reason.is_none()
        }
        WorkResolution::Cancelled { reason } => {
            occurrence.state == crate::WorkOccurrenceState::Cancelled
                && occurrence.result.is_none()
                && occurrence.error.as_ref() == Some(reason)
                && occurrence.next_reason.is_none()
        }
    }
}
