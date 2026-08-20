use std::collections::{BTreeMap, BTreeSet, VecDeque};

use cymule_core::Machine;
use cymule_durable::{
    AuthorityLease, DurableCoordinator, DurableStore, JournalBatch, JournalRecord, WaitActivation,
};
use serde::{Deserialize, Serialize};

use crate::{
    ClaimedWork, CompactedWorkIndex, FrontierLimits, ParkedWork, RegionMigrationCommand,
    RegionMigrationReceipt, RegionMigrator, RegionSource, SchedulingPolicy,
    VIRTUAL_CLAIM_CONTROL_VERSION, VIRTUAL_COMPACTION_CONTROL_VERSION,
    VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION, VIRTUAL_RECOVERY_CONTROL_VERSION,
    VIRTUAL_REGION_MIGRATION_CONTROL_VERSION, VIRTUAL_REHYDRATION_CONTROL_VERSION,
    VIRTUAL_RUN_WEIGHT_CONTROL_VERSION, VIRTUAL_WORK_CONTROL_VERSION, VirtualArchive,
    VirtualClaimCommand, VirtualClaimLease, VirtualClaimReceipt, VirtualCompactionCertificate,
    VirtualCompactionCommand, VirtualCompactionReceipt, VirtualError, VirtualLeaseRenewalCommand,
    VirtualLeaseRenewalReceipt, VirtualRecoveryCommand, VirtualRecoveryReceipt, VirtualRegion,
    VirtualRehydrationCommand, VirtualRehydrationReceipt, VirtualResult, VirtualRunWeightCommand,
    VirtualRunWeightReceipt, VirtualScheduler, VirtualSnapshot, WorkItem, WorkOccurrence,
    WorkResolution, WorkResolutionCommand,
};

/// Versioned M3 scheduler checkpoint stored in an M1 application journal.
pub const VIRTUAL_CHECKPOINT_SCHEMA: &str = "cymule.virtual-checkpoint/2";
/// Hard encoded-size bound for one authenticated M3 journal delta.
pub const MAX_VIRTUAL_CHECKPOINT_DELTA_BYTES: usize = 4 * 1024 * 1024;

/// Incremental changes to one string-keyed scheduler map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapDelta<T> {
    /// New or changed values keyed by canonical scheduler identity.
    pub upsert: BTreeMap<String, T>,
    /// Existing identities removed by this transition.
    pub remove: BTreeSet<String>,
}

/// Incremental changes to one scheduler identity set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetDelta {
    /// Identities added by this transition.
    pub add: BTreeSet<String>,
    /// Existing identities removed by this transition.
    pub remove: BTreeSet<String>,
}

/// One bounded scheduler transition encoded without repeating prior state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualDelta {
    /// Resulting frozen scheduling policy.
    pub scheduling_policy: SchedulingPolicy,
    /// Region mutations.
    pub regions: MapDelta<VirtualRegion>,
    /// Per-Run ready-frontier mutations.
    pub ready: MapDelta<VecDeque<WorkItem>>,
    /// Active-claim mutations.
    pub active: MapDelta<ClaimedWork>,
    /// Parked-work mutations.
    pub parked: MapDelta<ParkedWork>,
    /// Materialized-identity mutations.
    pub known: SetDelta,
    /// Resulting Run fairness cursor.
    pub last_run: Option<String>,
    /// Resulting region visibility cursor.
    pub last_region: Option<String>,
    /// Work claim-epoch mutations.
    pub claim_epochs: MapDelta<u64>,
    /// Occurrence mutations.
    pub occurrences: MapDelta<WorkOccurrence>,
    /// Run weight mutations.
    pub run_weights: MapDelta<u32>,
    /// Run deficit mutations.
    pub run_deficits: MapDelta<u64>,
    /// Resulting successful dispatch sequence.
    pub dispatch_sequence: u64,
    /// Ready-age mutations.
    pub ready_since: MapDelta<u64>,
    /// Retired-region mutations.
    pub retired_regions: MapDelta<String>,
    /// Migration-receipt mutations.
    pub migrations: MapDelta<RegionMigrationReceipt>,
    /// Cold-history certificate mutations.
    pub compactions: MapDelta<VirtualCompactionCertificate>,
    /// Compaction-receipt mutations.
    pub compaction_receipts: MapDelta<VirtualCompactionReceipt>,
    /// Terminal compacted-work index mutations.
    pub compacted_work: MapDelta<CompactedWorkIndex>,
    /// Region-to-certificate mutations.
    pub compacted_regions: MapDelta<String>,
    /// Rehydration-receipt mutations.
    pub rehydration_receipts: MapDelta<VirtualRehydrationReceipt>,
    /// Claim-receipt mutations.
    pub claim_receipts: MapDelta<VirtualClaimReceipt>,
    /// Lease-renewal-receipt mutations.
    pub lease_renewal_receipts: MapDelta<VirtualLeaseRenewalReceipt>,
    /// Recovery-receipt mutations.
    pub recovery_receipts: MapDelta<VirtualRecoveryReceipt>,
    /// Run-weight-receipt mutations.
    pub run_weight_receipts: MapDelta<VirtualRunWeightReceipt>,
}

/// One content-addressed bounded scheduler delta with explicit journal lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualCheckpoint {
    /// Checkpoint schema and semantic version.
    pub checkpoint_version: String,
    /// Stable caller-supplied idempotency identity.
    pub checkpoint_id: String,
    /// Previous checkpoint record in this exact journal.
    pub parent_checkpoint: Option<String>,
    /// Authenticated transition head to which `delta` applies.
    pub parent_state_digest: String,
    /// Authenticated transition head after `delta` is applied.
    pub state_digest: String,
    /// Content identity of the exact incremental delta.
    pub delta_digest: String,
    /// Incremental scheduler mutation; prior snapshots are never repeated.
    pub delta: VirtualDelta,
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

impl VirtualDelta {
    fn between(before: &VirtualSnapshot, after: &VirtualSnapshot) -> Self {
        Self {
            scheduling_policy: after.scheduling_policy,
            regions: map_delta(&before.regions, &after.regions),
            ready: map_delta(&before.ready, &after.ready),
            active: map_delta(&before.active, &after.active),
            parked: map_delta(&before.parked, &after.parked),
            known: set_delta(&before.known, &after.known),
            last_run: after.last_run.clone(),
            last_region: after.last_region.clone(),
            claim_epochs: map_delta(&before.claim_epochs, &after.claim_epochs),
            occurrences: map_delta(&before.occurrences, &after.occurrences),
            run_weights: map_delta(&before.run_weights, &after.run_weights),
            run_deficits: map_delta(&before.run_deficits, &after.run_deficits),
            dispatch_sequence: after.dispatch_sequence,
            ready_since: map_delta(&before.ready_since, &after.ready_since),
            retired_regions: map_delta(&before.retired_regions, &after.retired_regions),
            migrations: map_delta(&before.migrations, &after.migrations),
            compactions: map_delta(&before.compactions, &after.compactions),
            compaction_receipts: map_delta(&before.compaction_receipts, &after.compaction_receipts),
            compacted_work: map_delta(&before.compacted_work, &after.compacted_work),
            compacted_regions: map_delta(&before.compacted_regions, &after.compacted_regions),
            rehydration_receipts: map_delta(
                &before.rehydration_receipts,
                &after.rehydration_receipts,
            ),
            claim_receipts: map_delta(&before.claim_receipts, &after.claim_receipts),
            lease_renewal_receipts: map_delta(
                &before.lease_renewal_receipts,
                &after.lease_renewal_receipts,
            ),
            recovery_receipts: map_delta(&before.recovery_receipts, &after.recovery_receipts),
            run_weight_receipts: map_delta(&before.run_weight_receipts, &after.run_weight_receipts),
        }
    }

    fn apply(&self, snapshot: &mut VirtualSnapshot) -> VirtualResult<()> {
        snapshot.scheduling_policy = self.scheduling_policy;
        apply_map_delta(&mut snapshot.regions, &self.regions, "regions")?;
        apply_map_delta(&mut snapshot.ready, &self.ready, "ready")?;
        apply_map_delta(&mut snapshot.active, &self.active, "active")?;
        apply_map_delta(&mut snapshot.parked, &self.parked, "parked")?;
        apply_set_delta(&mut snapshot.known, &self.known, "known")?;
        snapshot.last_run.clone_from(&self.last_run);
        snapshot.last_region.clone_from(&self.last_region);
        apply_map_delta(
            &mut snapshot.claim_epochs,
            &self.claim_epochs,
            "claim_epochs",
        )?;
        apply_map_delta(&mut snapshot.occurrences, &self.occurrences, "occurrences")?;
        apply_map_delta(&mut snapshot.run_weights, &self.run_weights, "run_weights")?;
        apply_map_delta(
            &mut snapshot.run_deficits,
            &self.run_deficits,
            "run_deficits",
        )?;
        snapshot.dispatch_sequence = self.dispatch_sequence;
        apply_map_delta(&mut snapshot.ready_since, &self.ready_since, "ready_since")?;
        apply_map_delta(
            &mut snapshot.retired_regions,
            &self.retired_regions,
            "retired_regions",
        )?;
        apply_map_delta(&mut snapshot.migrations, &self.migrations, "migrations")?;
        apply_map_delta(&mut snapshot.compactions, &self.compactions, "compactions")?;
        apply_map_delta(
            &mut snapshot.compaction_receipts,
            &self.compaction_receipts,
            "compaction_receipts",
        )?;
        apply_map_delta(
            &mut snapshot.compacted_work,
            &self.compacted_work,
            "compacted_work",
        )?;
        apply_map_delta(
            &mut snapshot.compacted_regions,
            &self.compacted_regions,
            "compacted_regions",
        )?;
        apply_map_delta(
            &mut snapshot.rehydration_receipts,
            &self.rehydration_receipts,
            "rehydration_receipts",
        )?;
        apply_map_delta(
            &mut snapshot.claim_receipts,
            &self.claim_receipts,
            "claim_receipts",
        )?;
        apply_map_delta(
            &mut snapshot.lease_renewal_receipts,
            &self.lease_renewal_receipts,
            "lease_renewal_receipts",
        )?;
        apply_map_delta(
            &mut snapshot.recovery_receipts,
            &self.recovery_receipts,
            "recovery_receipts",
        )?;
        apply_map_delta(
            &mut snapshot.run_weight_receipts,
            &self.run_weight_receipts,
            "run_weight_receipts",
        )?;
        snapshot.parked_index.clear();
        Ok(())
    }
}

fn map_delta<T: Clone + PartialEq>(
    before: &BTreeMap<String, T>,
    after: &BTreeMap<String, T>,
) -> MapDelta<T> {
    MapDelta {
        upsert: after
            .iter()
            .filter(|(key, value)| before.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        remove: before
            .keys()
            .filter(|key| !after.contains_key(*key))
            .cloned()
            .collect(),
    }
}

fn set_delta(before: &BTreeSet<String>, after: &BTreeSet<String>) -> SetDelta {
    SetDelta {
        add: after.difference(before).cloned().collect(),
        remove: before.difference(after).cloned().collect(),
    }
}

fn apply_map_delta<T: Clone + PartialEq>(
    target: &mut BTreeMap<String, T>,
    delta: &MapDelta<T>,
    field: &str,
) -> VirtualResult<()> {
    if delta
        .upsert
        .keys()
        .any(|identity| delta.remove.contains(identity))
    {
        return Err(VirtualError::Validation(format!(
            "virtual delta {field} upserts and removes the same identity"
        )));
    }
    for identity in &delta.remove {
        if target.remove(identity).is_none() {
            return Err(VirtualError::Validation(format!(
                "virtual delta {field} removes missing identity {identity}"
            )));
        }
    }
    for (identity, value) in &delta.upsert {
        if target.get(identity) == Some(value) {
            return Err(VirtualError::Validation(format!(
                "virtual delta {field} redundantly upserts identity {identity}"
            )));
        }
        target.insert(identity.clone(), value.clone());
    }
    Ok(())
}

fn apply_set_delta(
    target: &mut BTreeSet<String>,
    delta: &SetDelta,
    field: &str,
) -> VirtualResult<()> {
    if !delta.add.is_disjoint(&delta.remove) {
        return Err(VirtualError::Validation(format!(
            "virtual delta {field} adds and removes the same identity"
        )));
    }
    for identity in &delta.remove {
        if !target.remove(identity) {
            return Err(VirtualError::Validation(format!(
                "virtual delta {field} removes missing identity {identity}"
            )));
        }
    }
    for identity in &delta.add {
        if !target.insert(identity.clone()) {
            return Err(VirtualError::Validation(format!(
                "virtual delta {field} redundantly adds identity {identity}"
            )));
        }
    }
    Ok(())
}

/// M1 journal integration for the M3 virtual scheduler.
pub struct DurableVirtualController;

/// Inputs for one embedded binding-pinned claim checkpoint.
#[derive(Clone, Copy)]
pub struct VirtualClaimCheckpoint<'a> {
    /// Worker identity.
    pub owner: &'a str,
    /// Exact semantic Plan.
    pub plan_id: &'a str,
    /// Exact immutable execution binding.
    pub occurrence_binding: &'a str,
    /// Worker capabilities used for selection.
    pub capabilities: &'a BTreeSet<String>,
    /// Owning virtual journal.
    pub journal_id: &'a str,
    /// Stable checkpoint identity.
    pub checkpoint_id: &'a str,
}

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
        let mut state_digest = virtual_genesis_digest()?;
        let mut snapshot = VirtualScheduler::new(limits)?.snapshot();
        for record in records {
            let checkpoint = decode_checkpoint(record)?;
            if checkpoint.parent_checkpoint != parent {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} has a discontinuous parent",
                    checkpoint.checkpoint_id
                )));
            }
            if checkpoint.parent_state_digest != state_digest {
                return Err(VirtualError::Validation(format!(
                    "virtual checkpoint {} has a discontinuous authenticated state parent",
                    checkpoint.checkpoint_id
                )));
            }
            checkpoint.delta.apply(&mut snapshot)?;
            validate_checkpoint_receipt(&checkpoint, &snapshot)?;
            state_digest.clone_from(&checkpoint.state_digest);
            parent = Some(checkpoint.checkpoint_id);
        }
        let mut restored = VirtualScheduler::restore(limits, snapshot)?;
        restored.mark_checkpoint(
            parent.expect("non-empty virtual journal has a checkpoint head"),
            state_digest,
        );
        Ok(restored)
    }

    /// Persist the current scheduler snapshot under one idempotent checkpoint.
    pub fn checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        journal_id: &str,
        checkpoint_id: &str,
    ) -> VirtualResult<String> {
        let record = checkpoint_record(coordinator, scheduler, journal_id, checkpoint_id)?;
        let head = checkpoint_head(&record)?;
        let revision = coordinator
            .append_journal_record(journal_id, record)
            .map_err(durable_error)?;
        scheduler.mark_checkpoint(head.0, head.1);
        Ok(revision)
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
        request: VirtualClaimCheckpoint<'_>,
    ) -> VirtualResult<Option<ClaimedWork>> {
        let VirtualClaimCheckpoint {
            owner,
            plan_id,
            occurrence_binding,
            capabilities,
            journal_id,
            checkpoint_id,
        } = request;
        let before = scheduler.clone();
        let claim = scheduler.claim(owner, plan_id, occurrence_binding, capabilities)?;
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
        Self::claim_command_and_checkpoint_with_journals(
            coordinator,
            scheduler,
            command,
            journal_id,
            &[],
        )
    }

    /// Claim one item and atomically append additional higher-profile records.
    ///
    /// This is the cross-profile admission seam used when immutable version
    /// selection and the fenced worker claim must become visible together.
    pub fn claim_command_and_checkpoint_with_journals<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        command: &VirtualClaimCommand,
        journal_id: &str,
        additional: &[JournalBatch],
    ) -> VirtualResult<VirtualClaimReceipt> {
        if let Some(receipt) = replay_claim_receipt(coordinator, scheduler, journal_id, command)? {
            ensure_journal_batches_retained(coordinator, additional)?;
            return Ok(receipt);
        }
        if additional
            .iter()
            .any(|batch| batch.journal_id == journal_id)
        {
            return Err(VirtualError::Validation(format!(
                "additional journal batch repeats virtual journal {journal_id}"
            )));
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
        let mut batches = Vec::with_capacity(additional.len() + 1);
        batches.push(JournalBatch {
            journal_id: journal_id.to_owned(),
            records: vec![record],
        });
        batches.extend_from_slice(additional);
        let result = if receipt.claim.is_some() {
            coordinator.checkpoint_lease_journals(
                &authority,
                command.logical_now,
                command.lease_ttl,
                &batches,
            )
        } else {
            coordinator.checkpoint_journals(&batches)
        };
        if let Err(error) = result {
            *scheduler = before;
            return Err(durable_error(error));
        }
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
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
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
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
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
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
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
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
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
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
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
        Ok(receipt)
    }

    /// Archive one completed region and atomically checkpoint its verified
    /// cold Resource descriptor through the M1 journal CAS.
    pub fn compact_command_and_checkpoint<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        scheduler: &mut VirtualScheduler,
        archive: &mut impl VirtualArchive,
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
        let receipt = scheduler.compact(archive, command)?;
        let bytes = match archive.get(&receipt.certificate.rehydration_manifest) {
            Ok(bytes) => bytes,
            Err(error) => {
                *scheduler = scheduler_before;
                return Err(error);
            }
        };
        let manifest: crate::VirtualArchiveManifest = match cymule_core::decode_json(&bytes) {
            Ok(manifest) => manifest,
            Err(error) => {
                *scheduler = scheduler_before;
                return Err(VirtualError::Source(error.to_string()));
            }
        };
        let object = match crate::virtual_archive_record(&manifest) {
            Ok(object) => object,
            Err(error) => {
                *scheduler = scheduler_before;
                return Err(error);
            }
        };
        if object.descriptor != receipt.certificate.rehydration_manifest || object.bytes != bytes {
            *scheduler = scheduler_before;
            return Err(VirtualError::Source(
                "archive manifest changed before its durable checkpoint".to_owned(),
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
                return Err(error);
            }
        };
        let batch = JournalBatch {
            journal_id: journal_id.to_owned(),
            records: vec![checkpoint],
        };
        if let Err(error) = coordinator.checkpoint_journals(&[batch]) {
            *scheduler = scheduler_before;
            return Err(durable_error(error));
        }
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
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
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
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
        mark_current_checkpoint(coordinator, scheduler, journal_id)?;
        Ok(woken)
    }
}

fn ensure_journal_batches_retained<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    batches: &[JournalBatch],
) -> VirtualResult<()> {
    for batch in batches {
        let retained = coordinator
            .journal_records(&batch.journal_id)
            .map_err(durable_error)?;
        for requested in &batch.records {
            match retained
                .iter()
                .find(|record| record.record_id == requested.record_id)
            {
                Some(existing) if existing == requested => {}
                Some(_) => {
                    return Err(VirtualError::Conflict(format!(
                        "journal record {} has conflicting retained content",
                        requested.record_id
                    )));
                }
                None => {
                    return Err(VirtualError::Conflict(format!(
                        "replayed virtual claim is missing coupled journal record {}",
                        requested.record_id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn new_checkpoint(
    records: &[JournalRecord],
    before: &VirtualScheduler,
    after: &VirtualScheduler,
    checkpoint_id: &str,
) -> VirtualResult<VirtualCheckpoint> {
    let parent_checkpoint = records.last().map(|record| record.record_id.clone());
    let parent_state_digest = match records.last() {
        Some(record) => decode_checkpoint(record)?.state_digest,
        None => virtual_genesis_digest()?,
    };
    let before_snapshot = before.snapshot();
    let after_snapshot = after.snapshot();
    let delta = VirtualDelta::between(&before_snapshot, &after_snapshot);
    let mut applied = before_snapshot;
    delta.apply(&mut applied)?;
    if serde_json::to_value(&applied).map_err(validation_error)?
        != serde_json::to_value(&after_snapshot).map_err(validation_error)?
    {
        return Err(VirtualError::Validation(
            "virtual delta does not reproduce the proposed scheduler state".to_owned(),
        ));
    }
    let delta_bytes = cymule_core::canonical_bytes(&delta).map_err(validation_error)?;
    if delta_bytes.len() > MAX_VIRTUAL_CHECKPOINT_DELTA_BYTES {
        return Err(VirtualError::Validation(format!(
            "virtual checkpoint delta is {} bytes, above the {} byte bound",
            delta_bytes.len(),
            MAX_VIRTUAL_CHECKPOINT_DELTA_BYTES
        )));
    }
    let delta_digest = cymule_core::canonical_digest(&delta).map_err(validation_error)?;
    let state_digest = cymule_core::canonical_digest(&(
        VIRTUAL_CHECKPOINT_SCHEMA,
        parent_state_digest.as_str(),
        delta_digest.as_str(),
    ))
    .map_err(validation_error)?;
    Ok(VirtualCheckpoint {
        checkpoint_version: VIRTUAL_CHECKPOINT_SCHEMA.to_owned(),
        checkpoint_id: checkpoint_id.to_owned(),
        parent_checkpoint,
        parent_state_digest,
        state_digest,
        delta_digest,
        delta,
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
    })
}

fn new_checkpoint_for_scheduler<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    records: &[JournalRecord],
    scheduler: &VirtualScheduler,
    journal_id: &str,
    checkpoint_id: &str,
) -> VirtualResult<VirtualCheckpoint> {
    let before = if let Some(anchor) = scheduler.checkpoint_anchor() {
        let current = records.last().ok_or_else(|| {
            VirtualError::Conflict(
                "virtual scheduler checkpoint anchor has no durable journal head".to_owned(),
            )
        })?;
        let current_checkpoint = decode_checkpoint(current)?;
        if current.record_id != anchor.checkpoint_id
            || current_checkpoint.state_digest != anchor.state_digest
        {
            return Err(VirtualError::Conflict(
                "virtual scheduler checkpoint anchor is stale".to_owned(),
            ));
        }
        VirtualScheduler::restore(scheduler.limits(), (*anchor.snapshot).clone())?
    } else if records.is_empty() {
        VirtualScheduler::new(scheduler.limits())?
    } else {
        DurableVirtualController::load(coordinator, journal_id, scheduler.limits()).map_err(
            |_| {
                VirtualError::Conflict(
                    "virtual scheduler with durable history must be loaded through its journal"
                        .to_owned(),
                )
            },
        )?
    };
    new_checkpoint(records, &before, scheduler, checkpoint_id)
}

fn encode_checkpoint(checkpoint: VirtualCheckpoint) -> VirtualResult<JournalRecord> {
    let checkpoint_id = checkpoint.checkpoint_id.clone();
    let payload = serde_json::to_value(checkpoint).map_err(validation_error)?;
    JournalRecord::new(checkpoint_id, VIRTUAL_CHECKPOINT_SCHEMA, payload).map_err(durable_error)
}

fn checkpoint_head(record: &JournalRecord) -> VirtualResult<(String, String)> {
    let checkpoint = decode_checkpoint(record)?;
    Ok((checkpoint.checkpoint_id, checkpoint.state_digest))
}

fn mark_current_checkpoint<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    scheduler: &mut VirtualScheduler,
    journal_id: &str,
) -> VirtualResult<()> {
    let record = coordinator
        .journal_records(journal_id)
        .map_err(durable_error)?
        .last()
        .ok_or_else(|| {
            VirtualError::Validation(
                "committed virtual transition has no journal checkpoint".to_owned(),
            )
        })?;
    let head = checkpoint_head(record)?;
    scheduler.mark_checkpoint(head.0, head.1);
    Ok(())
}

fn virtual_genesis_digest() -> VirtualResult<String> {
    cymule_core::canonical_digest(&(VIRTUAL_CHECKPOINT_SCHEMA, "genesis")).map_err(validation_error)
}

fn validation_error(error: impl std::fmt::Display) -> VirtualError {
    VirtualError::Validation(error.to_string())
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
            || DurableVirtualController::load(coordinator, journal_id, scheduler.limits())?
                .snapshot()
                != scheduler.snapshot()
        {
            return Err(VirtualError::Conflict(format!(
                "virtual checkpoint {checkpoint_id} already has different state"
            )));
        }
        return Ok(existing.clone());
    }
    encode_checkpoint(new_checkpoint_for_scheduler(
        coordinator,
        records,
        scheduler,
        journal_id,
        checkpoint_id,
    )?)
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
    let mut checkpoint = new_checkpoint_for_scheduler(
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
    )?;
    checkpoint.control = Some(command.clone());
    checkpoint.receipt_occurrence_id = Some(occurrence_id.to_owned());
    encode_checkpoint(checkpoint)
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
    let mut checkpoint = new_checkpoint_for_scheduler(
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
    )?;
    checkpoint.migration_control = Some(command.clone());
    checkpoint.receipt_migration_id = Some(command.plan.migration_id.clone());
    encode_checkpoint(checkpoint)
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
    let mut checkpoint = new_checkpoint_for_scheduler(
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
    )?;
    checkpoint.compaction_control = Some(command.clone());
    checkpoint.receipt_compaction_id = Some(certificate_id.to_owned());
    encode_checkpoint(checkpoint)
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
    let mut checkpoint = new_checkpoint_for_scheduler(
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
    )?;
    checkpoint.rehydration_control = Some(command.clone());
    checkpoint.receipt_rehydration_id = Some(command.command_id.clone());
    encode_checkpoint(checkpoint)
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
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
        SchedulingControls {
            claim: Some(command.clone()),
            lease_renewal: None,
            recovery: None,
        },
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
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
        SchedulingControls {
            claim: None,
            lease_renewal: Some(command.clone()),
            recovery: None,
        },
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
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
        SchedulingControls {
            claim: None,
            lease_renewal: None,
            recovery: Some(command.clone()),
        },
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
    let mut checkpoint = new_checkpoint_for_scheduler(
        coordinator,
        records,
        scheduler,
        journal_id,
        &command.command_id,
    )?;
    checkpoint.run_weight_control = Some(command.clone());
    checkpoint.receipt_run_weight_id = Some(command.command_id.clone());
    encode_checkpoint(checkpoint)
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

struct SchedulingControls {
    claim: Option<VirtualClaimCommand>,
    lease_renewal: Option<VirtualLeaseRenewalCommand>,
    recovery: Option<VirtualRecoveryCommand>,
}

fn scheduling_checkpoint_record<S: DurableStore>(
    coordinator: &DurableCoordinator<S>,
    records: &[JournalRecord],
    scheduler: &VirtualScheduler,
    journal_id: &str,
    command_id: &str,
    controls: SchedulingControls,
) -> VirtualResult<JournalRecord> {
    let mut checkpoint =
        new_checkpoint_for_scheduler(coordinator, records, scheduler, journal_id, command_id)?;
    checkpoint.receipt_claim_id = controls.claim.as_ref().map(|_| command_id.to_owned());
    checkpoint.claim_control = controls.claim;
    checkpoint.receipt_lease_renewal_id = controls
        .lease_renewal
        .as_ref()
        .map(|_| command_id.to_owned());
    checkpoint.lease_renewal_control = controls.lease_renewal;
    checkpoint.receipt_recovery_id = controls.recovery.as_ref().map(|_| command_id.to_owned());
    checkpoint.recovery_control = controls.recovery;
    encode_checkpoint(checkpoint)
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
    let delta_bytes = cymule_core::canonical_bytes(&checkpoint.delta).map_err(validation_error)?;
    if delta_bytes.len() > MAX_VIRTUAL_CHECKPOINT_DELTA_BYTES {
        return Err(VirtualError::Validation(format!(
            "virtual checkpoint {} delta exceeds the encoded size bound",
            record.record_id
        )));
    }
    let delta_digest =
        cymule_core::canonical_digest(&checkpoint.delta).map_err(validation_error)?;
    let state_digest = cymule_core::canonical_digest(&(
        VIRTUAL_CHECKPOINT_SCHEMA,
        checkpoint.parent_state_digest.as_str(),
        delta_digest.as_str(),
    ))
    .map_err(validation_error)?;
    if checkpoint.delta_digest != delta_digest || checkpoint.state_digest != state_digest {
        return Err(VirtualError::Validation(format!(
            "virtual checkpoint {} has an invalid delta or authenticated state digest",
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
    Ok(checkpoint)
}

fn validate_checkpoint_receipt(
    checkpoint: &VirtualCheckpoint,
    snapshot: &VirtualSnapshot,
) -> VirtualResult<()> {
    struct RecordIdentity<'a> {
        record_id: &'a str,
    }
    let record = RecordIdentity {
        record_id: checkpoint.checkpoint_id.as_str(),
    };
    match (&checkpoint.control, &checkpoint.receipt_occurrence_id) {
        (None, None) => {}
        (Some(command), Some(occurrence_id)) => {
            let occurrence = snapshot.occurrences.get(occurrence_id).ok_or_else(|| {
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
            let receipt = snapshot.migrations.get(migration_id).ok_or_else(|| {
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
            let receipt = snapshot
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
            let receipt = snapshot
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
            let receipt = snapshot.claim_receipts.get(receipt_id).ok_or_else(|| {
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
            let receipt = snapshot
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
            let receipt = snapshot.recovery_receipts.get(receipt_id).ok_or_else(|| {
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
            let receipt = snapshot
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
    Ok(())
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
