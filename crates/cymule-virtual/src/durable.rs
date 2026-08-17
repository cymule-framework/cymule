use std::collections::BTreeSet;

use cymule_core::Machine;
use cymule_durable::{
    AuthorityLease, DurableCoordinator, DurableStore, JournalBatch, JournalRecord, WaitActivation,
};
use serde::{Deserialize, Serialize};

use crate::{
    ClaimedWork, FrontierLimits, RegionMigrationCommand, RegionMigrationReceipt, RegionMigrator,
    RegionSource, VIRTUAL_CLAIM_CONTROL_VERSION, VIRTUAL_COMPACTION_CONTROL_VERSION,
    VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION, VIRTUAL_RECOVERY_CONTROL_VERSION,
    VIRTUAL_REGION_MIGRATION_CONTROL_VERSION, VIRTUAL_REHYDRATION_CONTROL_VERSION,
    VIRTUAL_RUN_WEIGHT_CONTROL_VERSION, VIRTUAL_WORK_CONTROL_VERSION, VirtualArchive,
    VirtualClaimCommand, VirtualClaimLease, VirtualClaimReceipt, VirtualCompactionCommand,
    VirtualCompactionReceipt, VirtualError, VirtualLeaseRenewalCommand, VirtualLeaseRenewalReceipt,
    VirtualRecoveryCommand, VirtualRecoveryReceipt, VirtualRehydrationCommand,
    VirtualRehydrationReceipt, VirtualResult, VirtualRunWeightCommand, VirtualRunWeightReceipt,
    VirtualScheduler, VirtualSnapshot, WorkOccurrence, WorkResolution, WorkResolutionCommand,
    virtual_archive_record,
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
    /// Region migration command committed by this checkpoint, when any.
    #[serde(default)]
    pub migration_control: Option<RegionMigrationCommand>,
    /// Migration receipt returned by the region control command.
    #[serde(default)]
    pub receipt_migration_id: Option<String>,
    /// Region compaction command committed by this checkpoint, when any.
    #[serde(default)]
    pub compaction_control: Option<VirtualCompactionCommand>,
    /// Compaction certificate returned by the control command.
    #[serde(default)]
    pub receipt_compaction_id: Option<String>,
    /// Partial rehydration command committed by this checkpoint, when any.
    #[serde(default)]
    pub rehydration_control: Option<VirtualRehydrationCommand>,
    /// Rehydration command receipt identity.
    #[serde(default)]
    pub receipt_rehydration_id: Option<String>,
    /// Worker-slot claim command committed by this checkpoint, when any.
    #[serde(default)]
    pub claim_control: Option<VirtualClaimCommand>,
    /// Claim command receipt identity.
    #[serde(default)]
    pub receipt_claim_id: Option<String>,
    /// Active-claim lease renewal command, when any.
    #[serde(default)]
    pub lease_renewal_control: Option<VirtualLeaseRenewalCommand>,
    /// Lease renewal receipt identity.
    #[serde(default)]
    pub receipt_lease_renewal_id: Option<String>,
    /// Expired-claim recovery command, when any.
    #[serde(default)]
    pub recovery_control: Option<VirtualRecoveryCommand>,
    /// Recovery receipt identity.
    #[serde(default)]
    pub receipt_recovery_id: Option<String>,
    /// Future Run scheduling-weight update command, when any.
    #[serde(default)]
    pub run_weight_control: Option<VirtualRunWeightCommand>,
    /// Run weight update receipt identity.
    #[serde(default)]
    pub receipt_run_weight_id: Option<String>,
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

    /// Claim at most one item through a fenced worker capacity slot and commit
    /// the lease plus scheduler receipt in one M1 CAS revision.
    pub fn claim_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        command: &VirtualClaimCommand,
        journal_id: &str,
    ) -> VirtualResult<VirtualClaimReceipt> {
        if let Some(receipt) = replay_claim_receipt(coordinator, scheduler, journal_id, command)? {
            return Ok(receipt);
        }
        let authority = coordinator
            .preview_lease(
                &command.slot_id,
                &command.owner,
                command.logical_now,
                command.lease_ttl,
            )
            .map_err(durable_error)?;
        let lease = virtual_lease(&authority);
        let before = scheduler.clone();
        let receipt = scheduler.claim_command(command, &lease)?;
        let record = match claim_checkpoint_record(coordinator, scheduler, journal_id, command) {
            Ok(record) => record,
            Err(error) => {
                *scheduler = before;
                return Err(error);
            }
        };
        let result = if receipt.claim.is_some() {
            coordinator.checkpoint_lease_journals(
                &authority,
                command.logical_now,
                command.lease_ttl,
                &[JournalBatch {
                    journal_id: journal_id.to_owned(),
                    records: vec![record],
                }],
            )
        } else {
            coordinator.append_journal_record(journal_id, record)
        };
        if let Err(error) = result {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(receipt)
    }

    /// Renew one active claim and its worker capacity slot in the same M1 CAS
    /// revision.
    pub fn renew_claim_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        command: &VirtualLeaseRenewalCommand,
        journal_id: &str,
    ) -> VirtualResult<VirtualLeaseRenewalReceipt> {
        if let Some(receipt) =
            replay_lease_renewal_receipt(coordinator, scheduler, journal_id, command)?
        {
            return Ok(receipt);
        }
        let active = scheduler
            .active_claim(&command.work_id)
            .ok_or_else(|| {
                VirtualError::NotFound(format!("active work {} is missing", command.work_id))
            })?
            .clone();
        if active.owner != command.owner
            || active.epoch != command.epoch
            || active.lease.epoch != command.expected_lease_epoch
        {
            return Err(VirtualError::Conflict(format!(
                "stale lease renewal for {}",
                command.work_id
            )));
        }
        let authority = coordinator
            .preview_lease(
                &active.lease.resource,
                &command.owner,
                command.logical_now,
                command.lease_ttl,
            )
            .map_err(durable_error)?;
        let lease = virtual_lease(&authority);
        let before = scheduler.clone();
        let receipt = scheduler.renew_claim(command, &lease)?;
        let record =
            match lease_renewal_checkpoint_record(coordinator, scheduler, journal_id, command) {
                Ok(record) => record,
                Err(error) => {
                    *scheduler = before;
                    return Err(error);
                }
            };
        if let Err(error) = coordinator.checkpoint_lease_journals(
            &authority,
            command.logical_now,
            command.lease_ttl,
            &[JournalBatch {
                journal_id: journal_id.to_owned(),
                records: vec![record],
            }],
        ) {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(receipt)
    }

    /// Apply one explicit disposition after the active capacity-slot lease has
    /// expired, then checkpoint the fence before another worker may claim it.
    pub fn recover_expired_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        machine: &Machine,
        command: &VirtualRecoveryCommand,
        journal_id: &str,
    ) -> VirtualResult<VirtualRecoveryReceipt> {
        if let Some(receipt) = replay_recovery_receipt(coordinator, scheduler, journal_id, command)?
        {
            return Ok(receipt);
        }
        let active = scheduler
            .active_claim(&command.work_id)
            .ok_or_else(|| {
                VirtualError::NotFound(format!("active work {} is missing", command.work_id))
            })?
            .clone();
        let authority = coordinator
            .state()
            .map_err(durable_error)?
            .leases
            .get(&active.lease.resource)
            .ok_or_else(|| {
                VirtualError::Conflict(format!(
                    "active work {} has no durable capacity-slot lease",
                    command.work_id
                ))
            })?;
        if virtual_lease(authority) != active.lease || authority.expires_at > command.observed_at {
            return Err(VirtualError::Conflict(format!(
                "active work {} lease is not expired under the durable fence",
                command.work_id
            )));
        }
        let before = scheduler.clone();
        let receipt = scheduler.recover_expired(command)?;
        let record = match recovery_checkpoint_record(coordinator, scheduler, journal_id, command) {
            Ok(record) => record,
            Err(error) => {
                *scheduler = before;
                return Err(error);
            }
        };
        if let Err(error) = coordinator.checkpoint_artifact_journals(
            machine,
            &resolution_artifacts(&command.resolution),
            &[JournalBatch {
                journal_id: journal_id.to_owned(),
                records: vec![record],
            }],
        ) {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(receipt)
    }

    /// Update one Run's future weighted scheduling share and retain an
    /// idempotent receipt in the M1 virtual journal.
    pub fn set_run_weight_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        command: &VirtualRunWeightCommand,
        journal_id: &str,
    ) -> VirtualResult<VirtualRunWeightReceipt> {
        if let Some(receipt) =
            replay_run_weight_receipt(coordinator, scheduler, journal_id, command)?
        {
            return Ok(receipt);
        }
        let before = scheduler.clone();
        let receipt = scheduler.set_run_weight_command(command)?;
        let record = match run_weight_checkpoint_record(coordinator, scheduler, journal_id, command)
        {
            Ok(record) => record,
            Err(error) => {
                *scheduler = before;
                return Err(error);
            }
        };
        if let Err(error) = coordinator.append_journal_record(journal_id, record) {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(receipt)
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
            || command.expected_lease_epoch == 0
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
        let occurrence = scheduler.resolve_fenced(
            &command.work_id,
            &command.owner,
            command.epoch,
            command.expected_lease_epoch,
            command.observed_at,
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

    /// Apply one adapter-produced split/merge command and atomically checkpoint
    /// coverage evidence, source retirement, targets, and receipt.
    pub fn migrate_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        migrator: &mut impl RegionMigrator,
        machine: &Machine,
        command: &RegionMigrationCommand,
        journal_id: &str,
    ) -> VirtualResult<RegionMigrationReceipt> {
        if command.control_version != VIRTUAL_REGION_MIGRATION_CONTROL_VERSION
            || command.command_id.is_empty()
            || command.plan.migration_id.is_empty()
        {
            return Err(VirtualError::Validation(
                "virtual region migration command is malformed".to_owned(),
            ));
        }
        if let Some(receipt) =
            replay_migration_receipt(coordinator, scheduler, journal_id, command)?
        {
            return Ok(receipt);
        }
        let before = scheduler.clone();
        let receipt = scheduler.migrate(migrator, &command.plan)?;
        let record = match migration_checkpoint_record(coordinator, scheduler, journal_id, command)
        {
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
            &BTreeSet::from([command.plan.coverage_evidence.clone()]),
            &[batch],
        ) {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(receipt)
    }

    /// Archive one completed region and atomically checkpoint its verified
    /// certificate plus manifest Artifact through the M1 journal CAS.
    pub fn compact_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        archive: &mut impl VirtualArchive,
        machine: &mut Machine,
        command: &VirtualCompactionCommand,
        journal_id: &str,
    ) -> VirtualResult<VirtualCompactionReceipt> {
        if let Some(receipt) =
            replay_compaction_receipt(coordinator, scheduler, journal_id, command)?
        {
            return Ok(receipt);
        }
        let journal_head = coordinator
            .journal_records(journal_id)
            .map_err(durable_error)?
            .last()
            .map(|record| record.record_id.clone())
            .ok_or_else(|| {
                VirtualError::Validation(
                    "durable compaction requires an existing virtual checkpoint".to_owned(),
                )
            })?;
        if !command.source_causal_cut.contains(&journal_head) {
            return Err(VirtualError::Conflict(format!(
                "compaction causal cut does not include current checkpoint {journal_head}"
            )));
        }
        let scheduler_before = scheduler.clone();
        let machine_before = machine.clone();
        let receipt = scheduler.compact(archive, command)?;
        let bytes = match archive.get(&receipt.certificate.rehydration_manifest) {
            Ok(bytes) => bytes,
            Err(error) => {
                *scheduler = scheduler_before;
                return Err(error);
            }
        };
        let manifest: crate::VirtualArchiveManifest = match serde_json::from_slice(&bytes) {
            Ok(manifest) => manifest,
            Err(error) => {
                *scheduler = scheduler_before;
                return Err(VirtualError::Source(error.to_string()));
            }
        };
        let record = match virtual_archive_record(&manifest) {
            Ok(record) => record,
            Err(error) => {
                *scheduler = scheduler_before;
                return Err(error);
            }
        };
        if record.reference != receipt.certificate.rehydration_manifest || record.bytes != bytes {
            *scheduler = scheduler_before;
            return Err(VirtualError::Source(
                "archive manifest changed before its durable checkpoint".to_owned(),
            ));
        }
        let stored = machine.put_artifact(record.reference.kind.clone(), record.bytes);
        if stored != record.reference {
            *scheduler = scheduler_before;
            *machine = machine_before;
            return Err(VirtualError::Validation(
                "archive manifest Artifact identity is inconsistent".to_owned(),
            ));
        }
        let checkpoint = match compaction_checkpoint_record(
            coordinator,
            scheduler,
            journal_id,
            command,
            &receipt.certificate.certificate_id,
        ) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                *scheduler = scheduler_before;
                *machine = machine_before;
                return Err(error);
            }
        };
        let batch = JournalBatch {
            journal_id: journal_id.to_owned(),
            records: vec![checkpoint],
        };
        if let Err(error) = coordinator.checkpoint_artifact_journals(
            machine,
            &BTreeSet::from([receipt.certificate.rehydration_manifest.clone()]),
            &[batch],
        ) {
            *scheduler = scheduler_before;
            *machine = machine_before;
            return Err(durable_error(error));
        }
        Ok(receipt)
    }

    /// Restore selected archived occurrence records and checkpoint the exact
    /// selection through the M1 journal CAS.
    pub fn rehydrate_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        archive: &mut impl VirtualArchive,
        command: &VirtualRehydrationCommand,
        journal_id: &str,
    ) -> VirtualResult<VirtualRehydrationReceipt> {
        if let Some(receipt) =
            replay_rehydration_receipt(coordinator, scheduler, journal_id, command)?
        {
            return Ok(receipt);
        }
        let before = scheduler.clone();
        let receipt = scheduler.rehydrate(archive, command)?;
        let record =
            match rehydration_checkpoint_record(coordinator, scheduler, journal_id, command) {
                Ok(record) => record,
                Err(error) => {
                    *scheduler = before;
                    return Err(error);
                }
            };
        if let Err(error) = coordinator.append_journal_record(journal_id, record) {
            *scheduler = before;
            return Err(durable_error(error));
        }
        Ok(receipt)
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
        if checkpoint.control.is_some()
            || checkpoint.migration_control.is_some()
            || checkpoint.compaction_control.is_some()
            || checkpoint.rehydration_control.is_some()
            || checkpoint.claim_control.is_some()
            || checkpoint.lease_renewal_control.is_some()
            || checkpoint.recovery_control.is_some()
            || checkpoint.run_weight_control.is_some()
            || checkpoint.snapshot != scheduler.snapshot()
        {
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
        migration_control: None,
        receipt_migration_id: None,
        compaction_control: None,
        receipt_compaction_id: None,
        rehydration_control: None,
        receipt_rehydration_id: None,
        claim_control: None,
        receipt_claim_id: None,
        lease_renewal_control: None,
        receipt_lease_renewal_id: None,
        recovery_control: None,
        receipt_recovery_id: None,
        run_weight_control: None,
        receipt_run_weight_id: None,
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
        migration_control: None,
        receipt_migration_id: None,
        compaction_control: None,
        receipt_compaction_id: None,
        rehydration_control: None,
        receipt_rehydration_id: None,
        claim_control: None,
        receipt_claim_id: None,
        lease_renewal_control: None,
        receipt_lease_renewal_id: None,
        recovery_control: None,
        receipt_recovery_id: None,
        run_weight_control: None,
        receipt_run_weight_id: None,
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

fn migration_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &RegionMigrationCommand,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    if records
        .iter()
        .any(|record| record.record_id == command.command_id)
    {
        return Err(VirtualError::Conflict(format!(
            "virtual region migration command {} already exists without a replayable receipt",
            command.command_id
        )));
    }
    let checkpoint = VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: command.command_id.clone(),
        parent_checkpoint: records.last().map(|record| record.record_id.clone()),
        snapshot: scheduler.snapshot(),
        control: None,
        receipt_occurrence_id: None,
        migration_control: Some(command.clone()),
        receipt_migration_id: Some(command.plan.migration_id.clone()),
        compaction_control: None,
        receipt_compaction_id: None,
        rehydration_control: None,
        receipt_rehydration_id: None,
        claim_control: None,
        receipt_claim_id: None,
        lease_renewal_control: None,
        receipt_lease_renewal_id: None,
        recovery_control: None,
        receipt_recovery_id: None,
        run_weight_control: None,
        receipt_run_weight_id: None,
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(&command.command_id, VIRTUAL_CHECKPOINT_SCHEMA, payload)
        .map_err(durable_error)
}

fn replay_migration_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &RegionMigrationCommand,
) -> VirtualResult<Option<RegionMigrationReceipt>> {
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
    if checkpoint.migration_control.as_ref() != Some(command) {
        return Err(VirtualError::Conflict(format!(
            "virtual region migration command {} was reused with different semantics",
            command.command_id
        )));
    }
    let migration_id = checkpoint.receipt_migration_id.ok_or_else(|| {
        VirtualError::Validation(format!(
            "virtual region migration command {} has no receipt",
            command.command_id
        ))
    })?;
    let receipt = scheduler.migration(&migration_id).ok_or_else(|| {
        VirtualError::Validation(format!(
            "virtual region migration command {} receipt is unavailable",
            command.command_id
        ))
    })?;
    Ok(Some(receipt.clone()))
}

fn compaction_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualCompactionCommand,
    certificate_id: &str,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    if records
        .iter()
        .any(|record| record.record_id == command.command_id)
    {
        return Err(VirtualError::Conflict(format!(
            "virtual compaction command {} already exists without a replayable receipt",
            command.command_id
        )));
    }
    let checkpoint = VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: command.command_id.clone(),
        parent_checkpoint: records.last().map(|record| record.record_id.clone()),
        snapshot: scheduler.snapshot(),
        control: None,
        receipt_occurrence_id: None,
        migration_control: None,
        receipt_migration_id: None,
        compaction_control: Some(command.clone()),
        receipt_compaction_id: Some(certificate_id.to_owned()),
        rehydration_control: None,
        receipt_rehydration_id: None,
        claim_control: None,
        receipt_claim_id: None,
        lease_renewal_control: None,
        receipt_lease_renewal_id: None,
        recovery_control: None,
        receipt_recovery_id: None,
        run_weight_control: None,
        receipt_run_weight_id: None,
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(&command.command_id, VIRTUAL_CHECKPOINT_SCHEMA, payload)
        .map_err(durable_error)
}

fn replay_compaction_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualCompactionCommand,
) -> VirtualResult<Option<VirtualCompactionReceipt>> {
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
    if checkpoint.compaction_control.as_ref() != Some(command) {
        return Err(VirtualError::Conflict(format!(
            "virtual compaction command {} was reused with different semantics",
            command.command_id
        )));
    }
    let certificate_id = checkpoint.receipt_compaction_id.ok_or_else(|| {
        VirtualError::Validation(format!(
            "virtual compaction command {} has no certificate receipt",
            command.command_id
        ))
    })?;
    let receipt = scheduler
        .snapshot()
        .compaction_receipts
        .get(&command.command_id)
        .filter(|receipt| receipt.certificate.certificate_id == certificate_id)
        .cloned()
        .ok_or_else(|| {
            VirtualError::Validation(format!(
                "virtual compaction command {} receipt is unavailable",
                command.command_id
            ))
        })?;
    Ok(Some(receipt))
}

fn rehydration_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualRehydrationCommand,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    if records
        .iter()
        .any(|record| record.record_id == command.command_id)
    {
        return Err(VirtualError::Conflict(format!(
            "virtual rehydration command {} already exists without a replayable receipt",
            command.command_id
        )));
    }
    let checkpoint = VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: command.command_id.clone(),
        parent_checkpoint: records.last().map(|record| record.record_id.clone()),
        snapshot: scheduler.snapshot(),
        control: None,
        receipt_occurrence_id: None,
        migration_control: None,
        receipt_migration_id: None,
        compaction_control: None,
        receipt_compaction_id: None,
        rehydration_control: Some(command.clone()),
        receipt_rehydration_id: Some(command.command_id.clone()),
        claim_control: None,
        receipt_claim_id: None,
        lease_renewal_control: None,
        receipt_lease_renewal_id: None,
        recovery_control: None,
        receipt_recovery_id: None,
        run_weight_control: None,
        receipt_run_weight_id: None,
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(&command.command_id, VIRTUAL_CHECKPOINT_SCHEMA, payload)
        .map_err(durable_error)
}

fn replay_rehydration_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualRehydrationCommand,
) -> VirtualResult<Option<VirtualRehydrationReceipt>> {
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
    if checkpoint.rehydration_control.as_ref() != Some(command)
        || checkpoint.receipt_rehydration_id.as_deref() != Some(command.command_id.as_str())
    {
        return Err(VirtualError::Conflict(format!(
            "virtual rehydration command {} was reused with different semantics",
            command.command_id
        )));
    }
    let receipt = scheduler
        .snapshot()
        .rehydration_receipts
        .get(&command.command_id)
        .cloned()
        .ok_or_else(|| {
            VirtualError::Validation(format!(
                "virtual rehydration command {} receipt is unavailable",
                command.command_id
            ))
        })?;
    Ok(Some(receipt))
}

fn claim_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualClaimCommand,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    reject_existing_control_record(records, &command.command_id, "virtual claim")?;
    scheduling_checkpoint_record(
        records.last().map(|record| record.record_id.clone()),
        scheduler,
        &command.command_id,
        Some(command.clone()),
        None,
        None,
    )
}

fn replay_claim_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualClaimCommand,
) -> VirtualResult<Option<VirtualClaimReceipt>> {
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
    if checkpoint.claim_control.as_ref() != Some(command)
        || checkpoint.receipt_claim_id.as_deref() != Some(command.command_id.as_str())
    {
        return Err(VirtualError::Conflict(format!(
            "virtual claim command {} was reused with different semantics",
            command.command_id
        )));
    }
    let receipt = scheduler
        .snapshot()
        .claim_receipts
        .get(&command.command_id)
        .cloned()
        .ok_or_else(|| {
            VirtualError::Validation(format!(
                "virtual claim command {} receipt is unavailable",
                command.command_id
            ))
        })?;
    Ok(Some(receipt))
}

fn lease_renewal_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualLeaseRenewalCommand,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    reject_existing_control_record(records, &command.command_id, "virtual lease renewal")?;
    scheduling_checkpoint_record(
        records.last().map(|record| record.record_id.clone()),
        scheduler,
        &command.command_id,
        None,
        Some(command.clone()),
        None,
    )
}

fn replay_lease_renewal_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualLeaseRenewalCommand,
) -> VirtualResult<Option<VirtualLeaseRenewalReceipt>> {
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
    if checkpoint.lease_renewal_control.as_ref() != Some(command)
        || checkpoint.receipt_lease_renewal_id.as_deref() != Some(command.command_id.as_str())
    {
        return Err(VirtualError::Conflict(format!(
            "virtual lease renewal command {} was reused with different semantics",
            command.command_id
        )));
    }
    let receipt = scheduler
        .snapshot()
        .lease_renewal_receipts
        .get(&command.command_id)
        .cloned()
        .ok_or_else(|| {
            VirtualError::Validation(format!(
                "virtual lease renewal command {} receipt is unavailable",
                command.command_id
            ))
        })?;
    Ok(Some(receipt))
}

fn recovery_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualRecoveryCommand,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    reject_existing_control_record(records, &command.command_id, "virtual recovery")?;
    scheduling_checkpoint_record(
        records.last().map(|record| record.record_id.clone()),
        scheduler,
        &command.command_id,
        None,
        None,
        Some(command.clone()),
    )
}

fn replay_recovery_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualRecoveryCommand,
) -> VirtualResult<Option<VirtualRecoveryReceipt>> {
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
    if checkpoint.recovery_control.as_ref() != Some(command)
        || checkpoint.receipt_recovery_id.as_deref() != Some(command.command_id.as_str())
    {
        return Err(VirtualError::Conflict(format!(
            "virtual recovery command {} was reused with different semantics",
            command.command_id
        )));
    }
    let receipt = scheduler
        .snapshot()
        .recovery_receipts
        .get(&command.command_id)
        .cloned()
        .ok_or_else(|| {
            VirtualError::Validation(format!(
                "virtual recovery command {} receipt is unavailable",
                command.command_id
            ))
        })?;
    Ok(Some(receipt))
}

fn run_weight_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualRunWeightCommand,
) -> VirtualResult<JournalRecord> {
    let records = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?;
    reject_existing_control_record(records, &command.command_id, "virtual Run weight")?;
    let checkpoint = VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: command.command_id.clone(),
        parent_checkpoint: records.last().map(|record| record.record_id.clone()),
        snapshot: scheduler.snapshot(),
        control: None,
        receipt_occurrence_id: None,
        migration_control: None,
        receipt_migration_id: None,
        compaction_control: None,
        receipt_compaction_id: None,
        rehydration_control: None,
        receipt_rehydration_id: None,
        claim_control: None,
        receipt_claim_id: None,
        lease_renewal_control: None,
        receipt_lease_renewal_id: None,
        recovery_control: None,
        receipt_recovery_id: None,
        run_weight_control: Some(command.clone()),
        receipt_run_weight_id: Some(command.command_id.clone()),
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(&command.command_id, VIRTUAL_CHECKPOINT_SCHEMA, payload)
        .map_err(durable_error)
}

fn replay_run_weight_receipt<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command: &VirtualRunWeightCommand,
) -> VirtualResult<Option<VirtualRunWeightReceipt>> {
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
    if checkpoint.run_weight_control.as_ref() != Some(command)
        || checkpoint.receipt_run_weight_id.as_deref() != Some(command.command_id.as_str())
    {
        return Err(VirtualError::Conflict(format!(
            "virtual Run weight command {} was reused with different semantics",
            command.command_id
        )));
    }
    let receipt = scheduler
        .snapshot()
        .run_weight_receipts
        .get(&command.command_id)
        .cloned()
        .ok_or_else(|| {
            VirtualError::Validation(format!(
                "virtual Run weight command {} receipt is unavailable",
                command.command_id
            ))
        })?;
    Ok(Some(receipt))
}

fn scheduling_checkpoint_record(
    parent_checkpoint: Option<String>,
    scheduler: &VirtualScheduler,
    command_id: &str,
    claim_control: Option<VirtualClaimCommand>,
    lease_renewal_control: Option<VirtualLeaseRenewalCommand>,
    recovery_control: Option<VirtualRecoveryCommand>,
) -> VirtualResult<JournalRecord> {
    let checkpoint = VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: command_id.to_owned(),
        parent_checkpoint,
        snapshot: scheduler.snapshot(),
        control: None,
        receipt_occurrence_id: None,
        migration_control: None,
        receipt_migration_id: None,
        compaction_control: None,
        receipt_compaction_id: None,
        rehydration_control: None,
        receipt_rehydration_id: None,
        receipt_claim_id: claim_control.as_ref().map(|_| command_id.to_owned()),
        claim_control,
        receipt_lease_renewal_id: lease_renewal_control
            .as_ref()
            .map(|_| command_id.to_owned()),
        lease_renewal_control,
        receipt_recovery_id: recovery_control.as_ref().map(|_| command_id.to_owned()),
        recovery_control,
        run_weight_control: None,
        receipt_run_weight_id: None,
    };
    let payload = serde_json::to_value(checkpoint)
        .map_err(|error| VirtualError::Validation(error.to_string()))?;
    JournalRecord::new(command_id, VIRTUAL_CHECKPOINT_SCHEMA, payload).map_err(durable_error)
}

fn reject_existing_control_record(
    records: &[JournalRecord],
    command_id: &str,
    family: &str,
) -> VirtualResult<()> {
    if records.iter().any(|record| record.record_id == command_id) {
        return Err(VirtualError::Conflict(format!(
            "{family} command {command_id} already exists without a replayable receipt"
        )));
    }
    Ok(())
}

fn virtual_lease(lease: &AuthorityLease) -> VirtualClaimLease {
    VirtualClaimLease {
        resource: lease.resource.clone(),
        owner: lease.owner.clone(),
        epoch: lease.epoch,
        expires_at: lease.expires_at,
    }
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
    let control_count = [
        checkpoint.control.is_some(),
        checkpoint.migration_control.is_some(),
        checkpoint.compaction_control.is_some(),
        checkpoint.rehydration_control.is_some(),
        checkpoint.claim_control.is_some(),
        checkpoint.lease_renewal_control.is_some(),
        checkpoint.recovery_control.is_some(),
        checkpoint.run_weight_control.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if control_count > 1 {
        return Err(VirtualError::Validation(format!(
            "virtual checkpoint {} contains multiple control commands",
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
    match (
        &checkpoint.migration_control,
        &checkpoint.receipt_migration_id,
    ) {
        (None, None) => {}
        (Some(command), Some(migration_id)) => {
            let receipt = checkpoint
                .snapshot
                .migrations
                .get(migration_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} migration receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_REGION_MIGRATION_CONTROL_VERSION
                || command.command_id != record.record_id
                || command.plan.migration_id != *migration_id
                || receipt.plan != command.plan
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} migration receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial migration receipt",
                record.record_id
            )));
        }
    }
    match (
        &checkpoint.compaction_control,
        &checkpoint.receipt_compaction_id,
    ) {
        (None, None) => {}
        (Some(command), Some(certificate_id)) => {
            let receipt = checkpoint
                .snapshot
                .compaction_receipts
                .get(&command.command_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} compaction receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_COMPACTION_CONTROL_VERSION
                || command.command_id != record.record_id
                || receipt.command != *command
                || receipt.certificate.certificate_id != *certificate_id
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} compaction receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial compaction receipt",
                record.record_id
            )));
        }
    }
    match (
        &checkpoint.rehydration_control,
        &checkpoint.receipt_rehydration_id,
    ) {
        (None, None) => {}
        (Some(command), Some(receipt_id)) => {
            let receipt = checkpoint
                .snapshot
                .rehydration_receipts
                .get(receipt_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} rehydration receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_REHYDRATION_CONTROL_VERSION
                || command.command_id != record.record_id
                || receipt_id != &command.command_id
                || receipt.command != *command
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} rehydration receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial rehydration receipt",
                record.record_id
            )));
        }
    }
    match (&checkpoint.claim_control, &checkpoint.receipt_claim_id) {
        (None, None) => {}
        (Some(command), Some(receipt_id)) => {
            let receipt = checkpoint
                .snapshot
                .claim_receipts
                .get(receipt_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} claim receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_CLAIM_CONTROL_VERSION
                || command.command_id != record.record_id
                || receipt_id != &command.command_id
                || receipt.command != *command
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} claim receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial claim receipt",
                record.record_id
            )));
        }
    }
    match (
        &checkpoint.lease_renewal_control,
        &checkpoint.receipt_lease_renewal_id,
    ) {
        (None, None) => {}
        (Some(command), Some(receipt_id)) => {
            let receipt = checkpoint
                .snapshot
                .lease_renewal_receipts
                .get(receipt_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} lease renewal receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION
                || command.command_id != record.record_id
                || receipt_id != &command.command_id
                || receipt.command != *command
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} lease renewal receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial lease renewal receipt",
                record.record_id
            )));
        }
    }
    match (
        &checkpoint.recovery_control,
        &checkpoint.receipt_recovery_id,
    ) {
        (None, None) => {}
        (Some(command), Some(receipt_id)) => {
            let receipt = checkpoint
                .snapshot
                .recovery_receipts
                .get(receipt_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} recovery receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_RECOVERY_CONTROL_VERSION
                || command.command_id != record.record_id
                || receipt_id != &command.command_id
                || receipt.command != *command
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} recovery receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial recovery receipt",
                record.record_id
            )));
        }
    }
    match (
        &checkpoint.run_weight_control,
        &checkpoint.receipt_run_weight_id,
    ) {
        (None, None) => {}
        (Some(command), Some(receipt_id)) => {
            let receipt = checkpoint
                .snapshot
                .run_weight_receipts
                .get(receipt_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "virtual checkpoint {} Run weight receipt is missing",
                        record.record_id
                    ))
                })?;
            if command.control_version != VIRTUAL_RUN_WEIGHT_CONTROL_VERSION
                || command.command_id != record.record_id
                || receipt_id != &command.command_id
                || receipt.command != *command
            {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} Run weight receipt does not match its command",
                    record.record_id
                )));
            }
        }
        _ => {
            return Err(VirtualError::Validation(format!(
                "virtual checkpoint {} has a partial Run weight receipt",
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
        || command.expected_lease_epoch != occurrence.lease_epoch
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
