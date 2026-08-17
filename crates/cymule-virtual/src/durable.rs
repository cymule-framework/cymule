use cymule_core::Machine;
use cymule_durable::{
    DurableCoordinator, DurableStore, JournalBatch, JournalRecord, WaitActivation,
};
use serde::{Deserialize, Serialize};

use crate::{
    FrontierLimits, RegionSource, VirtualError, VirtualResult, VirtualScheduler, VirtualSnapshot,
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
        if checkpoint.snapshot != scheduler.snapshot() {
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
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(checkpoint_id, VIRTUAL_CHECKPOINT_SCHEMA, payload).map_err(durable_error)
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
    Ok(checkpoint)
}

fn durable_error(error: impl std::fmt::Display) -> VirtualError {
    VirtualError::Durable(error.to_string())
}
