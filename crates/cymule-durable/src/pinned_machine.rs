//! Exact-load bridge between the pinned Core reducer and `StateRoot` storage.
//!
//! This module is deliberately provider-neutral. Every read and physical
//! mutation is resolved beneath one immutable manifest snapshot; the returned
//! stage contains no Store CAS capability and can be published only by the
//! owning coordinator together with its exact M1 sidecar transition.

use std::collections::{BTreeMap, BTreeSet};

use cymule_authenticated_collections::{
    MAX_PAGE_BYTES, MapPosition, MapRoot, prove_log_range, prove_map_range, verify_log_range,
    verify_map_range,
};
use cymule_core::durable_internal::{
    MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES, MachineCompactionIntent,
    MachineInlineScopeReadRequirement, MachineMaterialAdmission, MachineMaterialParentReads,
    MachinePagedFinalizeInputs, MachinePagedReadInputs, MachinePagedTransitionAction,
    MachinePagedTransitionCurrent, MachinePagedTransitionPhase, MachinePhysicalRoot,
    MachinePinnedBatchCommand, MachinePinnedBatchPrecondition, MachinePinnedCommandProof,
    MachinePinnedRunLookup, MachinePreparedRootMutation, MachineRunCurrent, MachineRunIndexPage,
    MachineRunLogPage, MachineRunLogSelector, MachineRunReadInputs, MachineRunRootUpdate,
    MachineScopeCurrent, MachineStartRunMaterial, MachineTypedRootMutation,
    PinnedMachineBatchTransition, PinnedMachineCommandPreparation, PinnedMachineFreshPreparation,
    PinnedMachineRunPreparation, PinnedMachineTransition, PreparedMachineMaterialAdmission,
    PreparedPinnedCommandBatch, PreparedPinnedMachineCompaction, PreparedPinnedMachineTransition,
    pinned_paged_log_selector, pinned_paged_obligation_read, prepare_machine_material_admission,
    prepare_pinned_command, prepare_pinned_command_batch, prepare_pinned_compaction,
    prepare_pinned_transition_final, prepare_pinned_transition_page,
    verify_pinned_command_batch_replay,
};
use cymule_core::{
    ArtifactRecord, Command, CommandEnvelope, EffectIntentIdentityInput,
    MachineCommandArchiveLookup, SealedPlan, effect_intent_id, effect_obligation_id,
};

use super::{
    ObjectOverlay, StateRootLeafKind, StateRootManifest, StateRootResolver, StateRootValue, map_get,
};
use crate::{DurableError, DurableResult};

pub(crate) const PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN: &str =
    "cymule.pinned-machine-state-root-stage/1";

/// All-or-none outcome of one frozen ordered command batch.
pub(crate) enum PinnedMachineBatchOutcome {
    /// Complete exact replay; no `StateRoot` successor is authorized.
    Replay(PinnedMachineBatchReplay),
    /// Complete fresh batch staged under one overlay and one future CAS.
    Staged(PinnedMachineStagedMutation),
    /// Sole paged command reserved its persisted transition.
    PagedBegin(PinnedMachineStagedMutation),
    /// Sole paged command was already reserved and must continue.
    Pending(MachinePagedTransitionCurrent),
    /// One or more command identities require Store-owned cold lookup only
    /// after hot/pending authority proved absent.
    NeedsArchive(PinnedMachineBatchArchiveRequest),
    /// Every command proof resolved to one archived batch. Only this opaque
    /// token authorizes loading that exact cold batch record.
    NeedsArchivedBatch(PinnedMachineArchivedBatchRequest),
}

pub(crate) struct PinnedMachineBatchReplay {
    pub(crate) batch_id: String,
    pub(crate) batch_receipt_id: String,
    pub(crate) receipts: Vec<cymule_core::CommandReceipt>,
}

/// One already admitted terminal intent and its bounded staged material.
pub(crate) struct PinnedTerminalRecovery {
    pub(crate) transition: MachinePagedTransitionCurrent,
    pub(crate) material: MachineMaterialAdmission,
}

pub(crate) struct PinnedMachineBatchArchiveRequest {
    manifest_id: String,
    anchor: cymule_core::MachineBaseAnchor,
    commands: Vec<MachinePinnedBatchCommand>,
    material: Option<MachineMaterialAdmission>,
    start_material: Option<MachineStartRunMaterial>,
    command_ids: Vec<String>,
    local_authority: String,
}

pub(crate) struct PinnedMachineArchivedBatchRequest {
    manifest_id: String,
    batch_id: String,
    commands: Vec<MachinePinnedBatchCommand>,
    archived_entries: Vec<cymule_core::MachineCommandArchiveEntry>,
    material_digest: Option<String>,
    local_authority: String,
}

/// One autonomous continuation step for a retained K-page command.
pub(crate) enum PinnedMachinePagedOutcome {
    /// One bounded source page advanced; reopen the committed successor before
    /// preparing another page.
    Progress(PinnedMachineStagedMutation),
    /// Every source page was already complete and the final semantic command
    /// admission was staged.
    Final(PinnedMachineStagedMutation),
    /// Finalization needs current-root archive non-membership after all hot
    /// pending authority was already resolved.
    NeedsArchive(PinnedMachinePagedArchiveRequest),
}

/// Opaque second phase for finalizing a retained paged command.
pub(crate) struct PinnedMachinePagedArchiveRequest {
    manifest_id: String,
    anchor: cymule_core::MachineBaseAnchor,
    transition: MachinePagedTransitionCurrent,
    local_authority: String,
}

impl PinnedMachinePagedArchiveRequest {
    pub(crate) fn anchor(&self) -> &cymule_core::MachineBaseAnchor {
        &self.anchor
    }

    pub(crate) fn command_id(&self) -> &str {
        &self.transition.command_id
    }

    pub(crate) fn finish<R: StateRootResolver + ?Sized>(
        self,
        manifest: &StateRootManifest,
        lookup: MachineCommandArchiveLookup,
        resolver: &mut R,
    ) -> DurableResult<PinnedMachinePagedOutcome> {
        manifest.verify()?;
        super::ensure_resolver_pinned(manifest, resolver)?;
        if manifest.manifest_id != self.manifest_id
            || manifest.machine_base_anchor.as_ref() != Some(&self.anchor)
            || paged_archive_request_authority(manifest, &self.anchor, &self.transition)?
                != self.local_authority
        {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_paged_archive_request_stale".to_owned(),
                message: "paged Machine archive request does not match the pinned manifest"
                    .to_owned(),
            });
        }
        let overlay = ObjectOverlay::new(resolver);
        stage_paged_final(manifest, &self.transition, Some(lookup), overlay)
            .map(PinnedMachinePagedOutcome::Final)
    }
}

impl PinnedMachineBatchArchiveRequest {
    pub(crate) fn anchor(&self) -> &cymule_core::MachineBaseAnchor {
        &self.anchor
    }

    pub(crate) fn command_ids(&self) -> &[String] {
        &self.command_ids
    }

    pub(crate) fn finish<R: StateRootResolver + ?Sized>(
        self,
        manifest: &StateRootManifest,
        lookups: Vec<MachineCommandArchiveLookup>,
        resolver: &mut R,
    ) -> DurableResult<PinnedMachineBatchOutcome> {
        manifest.verify()?;
        super::ensure_resolver_pinned(manifest, resolver)?;
        if manifest.manifest_id != self.manifest_id
            || manifest.machine_base_anchor.as_ref() != Some(&self.anchor)
            || lookups.len() != self.command_ids.len()
            || batch_archive_request_authority(
                manifest,
                &self.anchor,
                &self.commands,
                self.material.as_ref(),
                self.start_material.as_ref(),
                &self.command_ids,
            )? != self.local_authority
        {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_batch_archive_request_stale".to_owned(),
                message: "Machine batch archive request does not match the pinned manifest"
                    .to_owned(),
            });
        }
        let ArchivedBatchMembers {
            proofs,
            entries: archived_entries,
        } = resolve_archived_batch_members(manifest, &self.command_ids, lookups)?;
        let member_count = archived_entries.len();
        if member_count == 0 {
            let overlay = ObjectOverlay::new(resolver);
            return prepare_fresh_command_batch(
                manifest,
                self.commands,
                self.material,
                self.start_material,
                proofs,
                overlay,
            );
        }
        if member_count != self.command_ids.len() {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_batch_partial_archive".to_owned(),
                message: "atomic Machine batch is split between archived and absent members"
                    .to_owned(),
            });
        }
        let batch_id = archived_entries
            .first()
            .map(|entry| entry.command.batch_id.clone())
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_cold_batch_member_missing".to_owned(),
                message: "archived Machine batch lookup returned no member".to_owned(),
            })?;
        if archived_entries
            .iter()
            .any(|entry| entry.command.batch_id != batch_id)
        {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_cold_batch_membership_split".to_owned(),
                message: "archived Machine commands belong to different atomic batches".to_owned(),
            });
        }
        let material_digest = self
            .material
            .as_ref()
            .map(|value| value.material_digest().to_owned());
        let local_authority = archived_batch_request_authority(
            manifest,
            &batch_id,
            &self.commands,
            &archived_entries,
            material_digest.as_deref(),
        )?;
        Ok(PinnedMachineBatchOutcome::NeedsArchivedBatch(
            PinnedMachineArchivedBatchRequest {
                manifest_id: self.manifest_id,
                batch_id,
                commands: self.commands,
                archived_entries,
                material_digest,
                local_authority,
            },
        ))
    }
}

struct ArchivedBatchMembers {
    proofs: BTreeMap<String, MachinePinnedCommandProof>,
    entries: Vec<cymule_core::MachineCommandArchiveEntry>,
}

fn resolve_archived_batch_members(
    manifest: &StateRootManifest,
    command_ids: &[String],
    lookups: Vec<MachineCommandArchiveLookup>,
) -> DurableResult<ArchivedBatchMembers> {
    if lookups.len() != command_ids.len() {
        return Err(DurableError::Integrity {
            code: "state_root_machine_batch_archive_lookup_count_mismatch".to_owned(),
            message: "Machine batch archive lookup changed its exact member count".to_owned(),
        });
    }
    let mut proofs = BTreeMap::new();
    let mut archived_entries = Vec::new();
    for (command_id, lookup) in command_ids.iter().zip(lookups) {
        match lookup {
            MachineCommandArchiveLookup::NonMember { index_proof } => {
                proofs.insert(
                    command_id.clone(),
                    MachinePinnedCommandProof::vacant(index_proof),
                );
            }
            MachineCommandArchiveLookup::Member { index_proof, entry } => {
                if entry.command.envelope.command_id != *command_id {
                    return Err(DurableError::Integrity {
                        code: "state_root_machine_batch_archive_member_mismatch".to_owned(),
                        message: format!(
                            "archived batch lookup for {command_id} returned another command"
                        ),
                    });
                }
                let proof = MachinePinnedCommandProof::archived(index_proof, (*entry).clone());
                if !matches!(
                    prepare_pinned_command(
                        manifest.machine_frontier(),
                        &proof,
                        entry.command.envelope.clone(),
                    )?,
                    PinnedMachineCommandPreparation::Replay(_)
                ) {
                    return Err(DurableError::Integrity {
                        code: "state_root_machine_archived_batch_member_not_replayed".to_owned(),
                        message: "archived batch member did not prove exact retained admission"
                            .to_owned(),
                    });
                }
                archived_entries.push(*entry);
            }
        }
    }
    Ok(ArchivedBatchMembers {
        proofs,
        entries: archived_entries,
    })
}

impl PinnedMachineArchivedBatchRequest {
    /// Exact cold batch identity that the Store may now load.
    pub(crate) fn batch_id(&self) -> &str {
        &self.batch_id
    }

    /// Consume the sole cold batch record and close full-member replay.
    pub(crate) fn finish<R: StateRootResolver + ?Sized>(
        self,
        manifest: &StateRootManifest,
        batch: cymule_core::durable_internal::MachineCommandBatchRecord,
        resolver: &mut R,
    ) -> DurableResult<PinnedMachineBatchOutcome> {
        manifest.verify()?;
        super::ensure_resolver_pinned(manifest, resolver)?;
        if manifest.manifest_id != self.manifest_id
            || batch.batch_id != self.batch_id
            || archived_batch_request_authority(
                manifest,
                &self.batch_id,
                &self.commands,
                &self.archived_entries,
                self.material_digest.as_deref(),
            )? != self.local_authority
        {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_archived_batch_request_stale".to_owned(),
                message: "archived Machine batch request changed pinned authority".to_owned(),
            });
        }
        for entry in &self.archived_entries {
            batch.verify_entry(entry)?;
        }
        let records = self
            .archived_entries
            .into_iter()
            .map(|entry| entry.command)
            .collect::<Vec<_>>();
        let receipts = verify_pinned_command_batch_replay(
            &batch,
            &self.commands,
            &records,
            self.material_digest.as_deref(),
        )?;
        Ok(PinnedMachineBatchOutcome::Replay(
            PinnedMachineBatchReplay {
                batch_id: batch.batch_id,
                batch_receipt_id: batch.batch_receipt_id,
                receipts,
            },
        ))
    }
}

/// Material roots and the real Core batch before the owning profile receipt
/// exists. This preparation has no finish or Store publication capability.
pub(crate) struct PinnedMachinePreparedMaterial {
    parent_manifest: String,
    prepared: PreparedMachineMaterialAdmission,
    roots: super::StateRoots,
    machine_base_anchor: Option<cymule_core::MachineBaseAnchor>,
    pending: BTreeMap<String, super::StateRootObject>,
}

impl PinnedMachinePreparedMaterial {
    /// Borrow the real material-only Core batch for the outer receipt builder.
    pub(crate) fn transition(&self) -> &PreparedMachineMaterialAdmission {
        &self.prepared
    }

    /// Bind the actual completed profile receipt before the stage can enter
    /// the `StateRoot` successor. The batch never depends on this later receipt.
    pub(crate) fn bind_outer_receipt(
        self,
        outer_receipt_digest: &str,
    ) -> DurableResult<PinnedMachineStagedMutation> {
        cymule_core::validate_content_id("profile material receipt", outer_receipt_digest)?;
        let stage_digest = cymule_core::canonical_digest(&(
            PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
            self.parent_manifest.as_str(),
            "material",
            &self.prepared.source_command_id,
            &self.prepared.material_digest,
            outer_receipt_digest,
            &self.prepared.frontier,
            &self.prepared.delta,
        ))?;
        Ok(PinnedMachineStagedMutation {
            parent_manifest: self.parent_manifest,
            stage_digest,
            transition: PinnedMachineStageTransition::Material {
                prepared: Box::new(self.prepared),
                outer_receipt_digest: outer_receipt_digest.to_owned(),
            },
            roots: self.roots,
            machine_base_anchor: self.machine_base_anchor,
            pending: self.pending,
        })
    }
}

/// Opaque no-CAS `StateRoot` stage produced only by this bridge.
pub(crate) struct PinnedMachineStagedMutation {
    pub(super) parent_manifest: String,
    pub(super) stage_digest: String,
    pub(super) transition: PinnedMachineStageTransition,
    pub(super) roots: super::StateRoots,
    pub(super) machine_base_anchor: Option<cymule_core::MachineBaseAnchor>,
    pub(super) pending: BTreeMap<String, super::StateRootObject>,
}

/// Closed Core result represented by one staged manifest successor.
pub(crate) enum PinnedMachineStageTransition {
    /// Initial persisted reservation/fence for a K-page command.
    PagedBegin(Box<cymule_core::durable_internal::PinnedMachinePagedBegin>),
    /// One bounded page progressed under the retained reservation.
    PagedProgress(Box<cymule_core::durable_internal::PinnedMachinePagedProgress>),
    /// Immutable Machine material admitted for one outer profile receipt.
    Material {
        prepared: Box<PreparedMachineMaterialAdmission>,
        outer_receipt_digest: String,
    },
    /// Complete all-or-none ordered command batch.
    Batch(Box<PinnedMachineBatchTransition>),
    /// Explicit offline maintenance over one exact, fully verified Core source.
    Compaction {
        prepared: Box<PreparedPinnedMachineCompaction>,
        receipt: Box<crate::HistoryCompactionReceipt>,
    },
}

impl PinnedMachineStageTransition {
    /// Immutable archive objects derive only from the verified Core result.
    pub(crate) fn archive_segments(&self) -> &[cymule_core::MachineCommandArchiveSegment] {
        match self {
            Self::Compaction { prepared, .. } => {
                std::slice::from_ref(&prepared.compaction().archive_segment)
            }
            Self::PagedBegin(_)
            | Self::PagedProgress(_)
            | Self::Material { .. }
            | Self::Batch(_) => &[],
        }
    }
}

impl PinnedMachineStagedMutation {
    /// Exact maintenance receipt frozen with the Core-produced causal cut.
    pub(crate) fn compaction_receipt(&self) -> Option<&crate::HistoryCompactionReceipt> {
        let PinnedMachineStageTransition::Compaction { receipt, .. } = &self.transition else {
            return None;
        };
        Some(receipt.as_ref())
    }

    /// Exact aggregate Core batch transition when this stage was prepared as
    /// one all-or-none command batch. Callers may derive closed profile
    /// sidecars from this semantic result before finishing the sole `StateRoot`
    /// successor, but cannot construct or mutate the batch itself.
    pub(crate) fn batch_transition(&self) -> Option<&PinnedMachineBatchTransition> {
        let PinnedMachineStageTransition::Batch(batch) = &self.transition else {
            return None;
        };
        Some(batch.as_ref())
    }

    /// Merge this exact Machine stage with optional M1 sidecar operations and
    /// produce one immutable-object/manifest successor. This method does not
    /// publish the Store head.
    pub(crate) fn finish<R: StateRootResolver + ?Sized>(
        self,
        current: &StateRootManifest,
        sidecar: Option<&crate::DurableDelta>,
        resolver: &mut R,
    ) -> DurableResult<PinnedMachinePreparedCommit> {
        if self.parent_manifest != current.manifest_id {
            return Err(DurableError::HistoryConflict {
                code: "state_root_pinned_stage_parent_mismatch".to_owned(),
                message: "pinned Machine stage does not extend the supplied manifest".to_owned(),
            });
        }
        verify_stage_sidecar(&self.transition, sidecar)?;
        let (machine_frontier, machine_root_delta) = match &self.transition {
            PinnedMachineStageTransition::PagedBegin(stage) => (stage.frontier.clone(), None),
            PinnedMachineStageTransition::PagedProgress(stage) => (stage.frontier.clone(), None),
            PinnedMachineStageTransition::Material { prepared, .. } => {
                (prepared.frontier.clone(), Some(prepared.delta.clone()))
            }
            PinnedMachineStageTransition::Batch(batch) => {
                (batch.frontier.clone(), Some(batch.machine.clone()))
            }
            PinnedMachineStageTransition::Compaction { prepared, .. } => (
                prepared.frontier().clone(),
                Some(prepared.root_delta().clone()),
            ),
        };
        let compaction_summary =
            if let PinnedMachineStageTransition::Compaction { prepared, .. } = &self.transition {
                Some(crate::MachineCompactionSummary::from(prepared.compaction()))
            } else {
                None
            };
        let state_root_transition = super::finish_pinned_machine_stage(
            current,
            sidecar,
            super::PinnedStateRootStageParts {
                stage_digest: self.stage_digest.clone(),
                machine_root_delta,
                machine_frontier,
                machine_base_anchor: self.machine_base_anchor,
                compaction_summary,
                roots: self.roots,
                pending: self.pending,
            },
            resolver,
        )?;
        Ok(PinnedMachinePreparedCommit {
            stage_digest: self.stage_digest,
            transition: self.transition,
            state_root_transition,
        })
    }
}

fn verify_stage_sidecar(
    transition: &PinnedMachineStageTransition,
    sidecar: Option<&crate::DurableDelta>,
) -> DurableResult<()> {
    let batch = match transition {
        PinnedMachineStageTransition::Batch(batch) => batch,
        PinnedMachineStageTransition::Material { .. } => {
            if sidecar.is_none_or(|delta| delta.operations().is_empty()) {
                return Err(DurableError::Validation(
                    "Machine material admission requires its outer profile sidecar".to_owned(),
                ));
            }
            return Ok(());
        }
        PinnedMachineStageTransition::Compaction { receipt, .. } => {
            return verify_compaction_sidecar(receipt, sidecar);
        }
        PinnedMachineStageTransition::PagedBegin(_)
        | PinnedMachineStageTransition::PagedProgress(_) => {
            if sidecar.is_some_and(|delta| !delta.operations().is_empty()) {
                return Err(DurableError::Validation(
                    "paged reservation/progress cannot carry M1 sidecar mutations".to_owned(),
                ));
            }
            return Ok(());
        }
    };
    if !batch
        .machine
        .commands
        .values()
        .any(|record| matches!(record.envelope.command, Command::StartRun { .. }))
    {
        return Ok(());
    }
    let ([step], [receipt]) = (batch.steps.as_slice(), batch.batch.receipts.as_slice()) else {
        return Err(DurableError::Integrity {
            code: "state_root_start_run_batch_not_singleton".to_owned(),
            message: "StartRun requires one complete exact command batch".to_owned(),
        });
    };
    let record = batch
        .machine
        .commands
        .get(&receipt.command_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_pinned_stage_command_missing".to_owned(),
            message: "StartRun batch lacks its exact command record".to_owned(),
        })?;
    verify_start_run_sidecar(record, step.run.as_ref(), sidecar)
}

fn verify_compaction_sidecar(
    receipt: &crate::HistoryCompactionReceipt,
    sidecar: Option<&crate::DurableDelta>,
) -> DurableResult<()> {
    let Some(delta) = sidecar else {
        return Err(DurableError::Validation(
            "Machine history compaction requires its exact receipt in the same CAS".to_owned(),
        ));
    };
    let [crate::DurableOperation::PutHistoryCompaction { value }] = delta.operations() else {
        return Err(DurableError::Validation(
            "Machine history compaction requires exactly its one typed receipt sidecar".to_owned(),
        ));
    };
    if value != receipt {
        return Err(DurableError::Integrity {
            code: "state_root_machine_compaction_receipt_mismatch".to_owned(),
            message: "Machine history compaction sidecar changed the frozen receipt".to_owned(),
        });
    }
    Ok(())
}

struct StartRunSidecars<'a> {
    continuation: &'a crate::Continuation,
    clock: &'a crate::ClockObservation,
    current: &'a crate::DurableRunCurrent,
}

fn start_run_sidecars(
    sidecar: Option<&crate::DurableDelta>,
) -> DurableResult<StartRunSidecars<'_>> {
    let delta = sidecar.ok_or_else(|| {
        DurableError::Validation(
            "StartRun requires its initial Continuation in the same StateRoot successor".to_owned(),
        )
    })?;
    let mut continuations = Vec::new();
    let mut clocks = Vec::new();
    let mut currents = Vec::new();
    for operation in delta.operations() {
        match operation {
            crate::DurableOperation::PutContinuation { value } => continuations.push(value),
            crate::DurableOperation::PutClockObservation { value } => clocks.push(value),
            crate::DurableOperation::PutRunCurrent { value } => currents.push(value),
            _ => {
                return Err(DurableError::Validation(
                    "StartRun sidecar contains an unrelated durable mutation".to_owned(),
                ));
            }
        }
    }
    if delta.operations().len() != 3
        || continuations.len() != 1
        || clocks.len() != 1
        || currents.len() != 1
    {
        return Err(DurableError::Validation(
            "StartRun requires exactly one Running Continuation, Clock receipt, and Run current sidecar".to_owned(),
        ));
    }
    Ok(StartRunSidecars {
        continuation: continuations[0],
        clock: clocks[0],
        current: currents[0],
    })
}

fn verify_start_run_sidecar(
    record: &cymule_core::ArchivedCommandRecord,
    run: Option<&cymule_core::durable_internal::MachineRunDelta>,
    sidecar: Option<&crate::DurableDelta>,
) -> DurableResult<()> {
    let Command::StartRun {
        plan_id,
        binding_context,
        input,
        material_digest,
        initial_attempt,
    } = &record.envelope.command
    else {
        return Ok(());
    };
    cymule_core::validate_content_id("StartRun material", material_digest)?;
    let StartRunSidecars {
        continuation,
        clock,
        current,
    } = start_run_sidecars(sidecar)?;
    continuation.verify_wire()?;
    clock.verify()?;
    current.verify()?;
    let claim = continuation
        .execution_claim
        .as_ref()
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_start_run_claim_missing".to_owned(),
            message: "StartRun Running Continuation has no execution claim".to_owned(),
        })?;
    if continuation.run_id != record.envelope.run_id
        || continuation.plan_id != *plan_id
        || continuation.binding_context != *binding_context
        || continuation.epoch != 0
        || continuation.execution_fence != initial_attempt.execution_fence
        || continuation.status != crate::ContinuationStatus::Running
        || continuation.state.as_ref() != Some(input)
        || continuation.frames.first().map(|frame| &frame.input) != Some(input)
        || claim.run_id != continuation.run_id
        || claim.continuation_id != initial_attempt.continuation_id
        || claim.continuation_attempt_id != initial_attempt.attempt_id
        || claim.fence != initial_attempt.execution_fence
        || claim.plan_id != *plan_id
        || claim.execution_binding_ref.artifact_id != *binding_context
        || claim.execution_binding_ref.kind != cymule_core::EXECUTION_BINDING_ARTIFACT_KIND
        || clock.reference() != claim.clock_observation_ref
        || clock.logical_time != claim.logical_acquired_at
        || current.run_id != continuation.run_id
        || current.plan_id != *plan_id
        || current.execution_binding.artifact_id != *binding_context
        || current.continuation_status != crate::ContinuationStatus::Running
        || current.epoch != 0
        || current.execution_fence != initial_attempt.execution_fence
    {
        return Err(DurableError::Integrity {
            code: "state_root_start_run_continuation_mismatch".to_owned(),
            message: "StartRun Running Continuation, first Attempt, Clock, Run current, and immutable material disagree"
                .to_owned(),
        });
    }
    if run.is_none_or(|run| {
        run.result_current.active_attempt_id.as_deref() != Some(initial_attempt.attempt_id.as_str())
            || run
                .attempts
                .get(&initial_attempt.attempt_id)
                .is_none_or(|attempt| {
                    !attempt.active
                        || attempt.continuation_id != initial_attempt.continuation_id
                        || attempt.continuation_epoch != initial_attempt.continuation_epoch
                        || attempt.execution_fence != initial_attempt.execution_fence
                })
    }) {
        return Err(DurableError::Integrity {
            code: "state_root_start_run_attempt_mismatch".to_owned(),
            message: "StartRun Core current does not retain its exact active first Attempt"
                .to_owned(),
        });
    }
    Ok(())
}

/// Fully composed, still-unpublished Store input. `StoreBatch` consumes this
/// opaque value together with the exact expected head.
pub(crate) struct PinnedMachinePreparedCommit {
    stage_digest: String,
    transition: PinnedMachineStageTransition,
    state_root_transition: super::StateRootTransition,
}

impl PinnedMachinePreparedCommit {
    pub(crate) fn stage_digest(&self) -> &str {
        &self.stage_digest
    }

    pub(crate) fn into_parts(self) -> (PinnedMachineStageTransition, super::StateRootTransition) {
        (self.transition, self.state_root_transition)
    }
}

/// Exact resolver view pinned to one authenticated `StateRoot` manifest.
pub(crate) struct PinnedMachineView<'a, R: StateRootResolver + ?Sized> {
    manifest: &'a StateRootManifest,
    resolver: &'a mut R,
}

impl<'a, R: StateRootResolver + ?Sized> PinnedMachineView<'a, R> {
    fn verify_continuation_artifact_closure(
        &mut self,
        continuation: &crate::Continuation,
    ) -> DurableResult<()> {
        let mut references = BTreeSet::new();
        if let Some(state) = &continuation.state {
            references.insert(state.clone());
        }
        for frame in &continuation.frames {
            references.insert(frame.input.clone());
            references.extend(frame.locals.values().cloned());
        }
        for reference in references {
            reference.validate()?;
            let retained =
                self.artifact(&reference.artifact_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "state_root_continuation_artifact_missing".to_owned(),
                        message: format!(
                            "Continuation {} references missing Artifact {}",
                            continuation.run_id, reference.artifact_id
                        ),
                    })?;
            if retained.reference != reference {
                return Err(DurableError::Integrity {
                    code: "state_root_continuation_artifact_reference_mismatch".to_owned(),
                    message: format!(
                        "Continuation {} Artifact {} changed its exact reference",
                        continuation.run_id, reference.artifact_id
                    ),
                });
            }
        }
        Ok(())
    }

    /// Open one no-fallback exact-load view.
    pub(crate) fn open(
        manifest: &'a StateRootManifest,
        resolver: &'a mut R,
    ) -> DurableResult<Self> {
        manifest.verify()?;
        super::ensure_resolver_pinned(manifest, resolver)?;
        Ok(Self { manifest, resolver })
    }

    /// Resolve one exact Run current or authenticated absence.
    pub(crate) fn run_current(&mut self, run_id: &str) -> DurableResult<Option<MachineRunCurrent>> {
        cymule_core::validate_identity("Machine Run", run_id)?;
        let mut overlay = ObjectOverlay::new(self.resolver);
        load_run_current_from_root(&self.manifest.machine_frontier.runs, run_id, &mut overlay)
    }

    /// Resolve only the exact admitted terminal command owned by this Run's
    /// fence. Scope work is not a terminal recovery operation.
    pub(crate) fn pending_terminal_recovery(
        &mut self,
        run_id: &str,
    ) -> DurableResult<Option<PinnedTerminalRecovery>> {
        let Some(run) = self.run_current(run_id)? else {
            return Ok(None);
        };
        let cymule_core::durable_internal::MachineRunReducerState::Transitioning { transition_id } =
            &run.reducer_state
        else {
            return Ok(None);
        };
        let mut overlay = ObjectOverlay::new(self.resolver);
        let transition = load_paged_transition(
            &self.manifest.machine_frontier.paged_transitions,
            transition_id,
            &mut overlay,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_terminal_transition_missing".to_owned(),
            message: format!("Run {run_id} lost its admitted terminal transition"),
        })?;
        if transition.run_id != run_id {
            return Err(DurableError::Integrity {
                code: "state_root_terminal_transition_owner_mismatch".to_owned(),
                message: "terminal transition escaped its exact Run fence".to_owned(),
            });
        }
        if !matches!(
            transition.action,
            MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun
        ) {
            return Ok(None);
        }
        super::verify_pending_terminal_sidecars(self.manifest, &transition, &mut overlay)?;
        let material =
            load_paged_material_admission(&transition, &mut overlay)?.ok_or_else(|| {
                DurableError::Integrity {
                    code: "state_root_terminal_material_missing".to_owned(),
                    message: "admitted terminal command has no frozen semantic material".to_owned(),
                }
            })?;
        Ok(Some(PinnedTerminalRecovery {
            transition,
            material,
        }))
    }

    /// Resolve one exact sealed Plan or authenticated absence.
    pub(crate) fn plan(&mut self, plan_id: &str) -> DurableResult<Option<SealedPlan>> {
        let plan: Option<SealedPlan> = load_leaf(
            &self.manifest.roots.machine_plans,
            plan_id,
            StateRootLeafKind::MachinePlan,
            self.resolver,
        )?;
        if let Some(plan) = &plan {
            plan.verify()?;
            if plan.plan_id != plan_id {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_plan_key_mismatch".to_owned(),
                    message: format!("Machine Plan key {plan_id} changed identity"),
                });
            }
        }
        Ok(plan)
    }

    /// Resolve one exact immutable Artifact or authenticated absence.
    pub(crate) fn artifact(&mut self, artifact_id: &str) -> DurableResult<Option<ArtifactRecord>> {
        let artifact: Option<ArtifactRecord> = load_leaf(
            &self.manifest.roots.machine_artifacts,
            artifact_id,
            StateRootLeafKind::MachineArtifact,
            self.resolver,
        )?;
        if let Some(artifact) = &artifact {
            artifact.validate()?;
            if artifact.reference.artifact_id != artifact_id {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_artifact_key_mismatch".to_owned(),
                    message: format!("Machine Artifact key {artifact_id} changed identity"),
                });
            }
        }
        Ok(artifact)
    }

    /// Resolve one exact Scope current and its authenticated lexical witness.
    pub(crate) fn scope_current(
        &mut self,
        run: &MachineRunCurrent,
        scope_id: &str,
    ) -> DurableResult<Option<PinnedMachineScopeRead>> {
        let mut overlay = ObjectOverlay::new(self.resolver);
        load_scope_current_from_root(&run.children.scopes, scope_id, &mut overlay)
    }

    /// Resolve one exact Effect current or authenticated absence.
    pub(crate) fn effect_current(
        &mut self,
        run: &MachineRunCurrent,
        intent_id: &str,
    ) -> DurableResult<Option<cymule_core::EffectProjection>> {
        let effect: Option<cymule_core::EffectProjection> = load_leaf(
            &run.children.effects,
            intent_id,
            StateRootLeafKind::MachineEffect,
            self.resolver,
        )?;
        if effect
            .as_ref()
            .is_some_and(|effect| effect.intent_id != intent_id)
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_effect_key_mismatch".to_owned(),
                message: format!("Machine Effect key {intent_id} changed identity"),
            });
        }
        Ok(effect)
    }

    /// Resolve one exact Attempt current or authenticated absence.
    pub(crate) fn attempt_current(
        &mut self,
        run: &MachineRunCurrent,
        attempt_id: &str,
    ) -> DurableResult<Option<cymule_core::AttemptProjection>> {
        let attempt: Option<cymule_core::AttemptProjection> = load_leaf(
            &run.children.attempts,
            attempt_id,
            StateRootLeafKind::MachineAttempt,
            self.resolver,
        )?;
        if attempt
            .as_ref()
            .is_some_and(|value| value.attempt_id != attempt_id)
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_attempt_key_mismatch".to_owned(),
                message: format!("Machine Attempt key {attempt_id} changed identity"),
            });
        }
        Ok(attempt)
    }

    /// Prepare one frozen ordered all-or-none command batch. Generic material
    /// is admitted before command reduction inside the same overlay and CAS.
    pub(crate) fn prepare_command_batch(
        &mut self,
        commands: Vec<MachinePinnedBatchCommand>,
        material: Option<MachineMaterialAdmission>,
    ) -> DurableResult<PinnedMachineBatchOutcome> {
        if commands
            .iter()
            .any(|entry| matches!(entry.command, Command::StartRun { .. }))
        {
            return Err(DurableError::Validation(
                "StartRun batch requires its closed MachineStartRunMaterial".to_owned(),
            ));
        }
        self.prepare_command_batch_internal(commands, material, None)
    }

    /// Size-one `StartRun` batch entry used by the typed Run-creation façade.
    pub(crate) fn prepare_start_run_batch(
        &mut self,
        command: MachinePinnedBatchCommand,
        material: MachineStartRunMaterial,
    ) -> DurableResult<PinnedMachineBatchOutcome> {
        if !matches!(command.command, Command::StartRun { .. })
            || !matches!(
                command.precondition,
                MachinePinnedBatchPrecondition::Parent(None)
            )
        {
            return Err(DurableError::Validation(
                "StartRun batch entry has the wrong command or parent precondition".to_owned(),
            ));
        }
        let generic = material.admission().clone();
        self.prepare_command_batch_internal(vec![command], Some(generic), Some(material))
    }

    fn prepare_command_batch_internal(
        &mut self,
        commands: Vec<MachinePinnedBatchCommand>,
        material: Option<MachineMaterialAdmission>,
        start_material: Option<MachineStartRunMaterial>,
    ) -> DurableResult<PinnedMachineBatchOutcome> {
        if commands.is_empty()
            || commands.len() > cymule_core::durable_internal::MAX_PINNED_COMMAND_BATCH_COMMANDS
        {
            return Err(DurableError::Validation(
                "pinned Machine command batch is empty or exceeds its exact command bound"
                    .to_owned(),
            ));
        }
        let mut overlay = ObjectOverlay::new(self.resolver);
        if let Some(replay) =
            load_hot_batch_replay(self.manifest, &commands, material.as_ref(), &mut overlay)?
        {
            return Ok(replay);
        }
        let command_ids = commands
            .iter()
            .map(|command| command.command_id.clone())
            .collect::<Vec<_>>();
        if let Some(anchor) = &self.manifest.machine_base_anchor {
            let local_authority = batch_archive_request_authority(
                self.manifest,
                anchor,
                &commands,
                material.as_ref(),
                start_material.as_ref(),
                &command_ids,
            )?;
            return Ok(PinnedMachineBatchOutcome::NeedsArchive(
                PinnedMachineBatchArchiveRequest {
                    manifest_id: self.manifest.manifest_id.clone(),
                    anchor: anchor.clone(),
                    commands,
                    material,
                    start_material,
                    command_ids,
                    local_authority,
                },
            ));
        }
        let proofs = command_ids
            .iter()
            .map(|command_id| {
                Ok((
                    command_id.clone(),
                    MachinePinnedCommandProof::vacant(
                        cymule_core::MachineCommandIndexProof::empty_nonmembership(command_id)?,
                    ),
                ))
            })
            .collect::<DurableResult<BTreeMap<_, _>>>()?;
        prepare_fresh_command_batch(
            self.manifest,
            commands,
            material,
            start_material,
            proofs,
            overlay,
        )
    }

    /// Stage one bounded immutable Plan/Artifact admission for an outer
    /// profile receipt. The returned stage has no command/Event authority and
    /// can publish only with that profile's non-empty sidecar in the same CAS.
    pub(crate) fn prepare_material_admission(
        &mut self,
        material: &MachineMaterialAdmission,
        outer_receipt_digest: &str,
    ) -> DurableResult<PinnedMachineStagedMutation> {
        self.prepare_material(material)?
            .bind_outer_receipt(outer_receipt_digest)
    }

    /// Prepare material and its real Core batch before building the outer
    /// receipt. Publication remains unavailable until that receipt is bound.
    pub(crate) fn prepare_material(
        &mut self,
        material: &MachineMaterialAdmission,
    ) -> DurableResult<PinnedMachinePreparedMaterial> {
        let mut overlay = ObjectOverlay::new(self.resolver);
        let plan_keys = material
            .plans()
            .iter()
            .map(|plan| plan.plan_id.clone())
            .collect();
        let artifact_keys = material
            .artifacts()
            .iter()
            .map(|artifact| artifact.reference.artifact_id.clone())
            .collect();
        let reads = MachineMaterialParentReads::new(
            load_leaf_set(
                &self.manifest.roots.machine_plans,
                plan_keys,
                StateRootLeafKind::MachinePlan,
                &mut overlay,
            )?,
            load_leaf_set(
                &self.manifest.roots.machine_artifacts,
                artifact_keys,
                StateRootLeafKind::MachineArtifact,
                &mut overlay,
            )?,
        );
        let prepared =
            prepare_machine_material_admission(self.manifest.machine_frontier(), material, &reads)?;
        if prepared.source_command_id != material.source_command_id()
            || prepared.material_digest != material.material_digest()
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_material_preparation_mismatch".to_owned(),
                message: "Core material preparation changed source or digest".to_owned(),
            });
        }
        let mut roots = self.manifest.roots().clone();
        let mut machine_base_anchor = self.manifest.machine_base_anchor.clone();
        super::apply_machine_root_delta(
            &mut roots,
            &prepared.delta,
            self.manifest.machine_frontier(),
            &prepared.frontier,
            &mut machine_base_anchor,
            &mut overlay,
        )?;
        Ok(PinnedMachinePreparedMaterial {
            parent_manifest: self.manifest.manifest_id.clone(),
            prepared,
            roots,
            machine_base_anchor,
            pending: overlay.into_pending(),
        })
    }

    /// Explicit offline maintenance. Only this named operation loads the
    /// complete pinned Core source; ordinary queries and mutations remain lazy.
    pub(crate) fn prepare_history_compaction(
        &mut self,
        request: &crate::HistoryCompactionRequest,
    ) -> DurableResult<PinnedMachineStagedMutation> {
        request.verify()?;
        let intent = match request.kind {
            crate::HistoryCompactionKind::EventPrefix => MachineCompactionIntent::EventPrefix {
                retain_suffix: usize::try_from(request.requested_suffix)
                    .map_err(|error| DurableError::Validation(error.to_string()))?,
            },
            crate::HistoryCompactionKind::EventFreeAdmissions => {
                MachineCompactionIntent::EventFreeAdmissions
            }
        };
        if request.expected_revision != self.manifest.revision {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_compaction_source_stale".to_owned(),
                message: "Machine history compaction requires its exact source revision".to_owned(),
            });
        }
        super::ensure_machine_compaction_source(self.manifest, self.resolver)?;
        if super::load_history_compaction_receipt(
            self.manifest,
            self.resolver,
            &request.compaction_id,
        )?
        .is_some()
        {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_compaction_identity_reuse".to_owned(),
                message: "retained Machine history compaction must resolve as exact replay"
                    .to_owned(),
            });
        }
        let parent_compaction =
            super::load_parent_history_compaction_receipt(self.manifest, self.resolver)?
                .map(|receipt| receipt.compaction_id);
        let source = super::load_machine_compaction_source(self.manifest, self.resolver)?;
        let prepared = prepare_pinned_compaction(self.manifest.machine_frontier(), source, intent)?;
        let receipt = crate::HistoryCompactionReceipt {
            compaction_version: crate::HISTORY_COMPACTION_VERSION.to_owned(),
            compaction_id: request.compaction_id.clone(),
            parent_compaction,
            kind: request.kind,
            source_revision: request.expected_revision.clone(),
            requested_suffix: request.requested_suffix,
            result: crate::MachineCompactionSummary::from(prepared.compaction()),
        };
        receipt.verify()?;
        stage_history_compaction(
            self.manifest,
            prepared,
            receipt,
            ObjectOverlay::new(self.resolver),
        )
    }

    /// Continue one exact retained paged transition by either one bounded page
    /// or its single final publication.
    pub(crate) fn continue_paged(
        &mut self,
        transition: &MachinePagedTransitionCurrent,
    ) -> DurableResult<PinnedMachinePagedOutcome> {
        transition.verify()?;
        if transition.parent_revision == self.manifest.revision {
            return Err(DurableError::Integrity {
                code: "state_root_machine_paged_unpublished_parent".to_owned(),
                message: "paged transition cannot continue from its pre-reservation manifest"
                    .to_owned(),
            });
        }
        let mut overlay = ObjectOverlay::new(self.resolver);
        let retained = load_paged_transition(
            &self.manifest.machine_frontier.paged_transitions,
            &transition.transition_id,
            &mut overlay,
        )?
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "Machine paged transition {} does not exist",
                transition.transition_id
            ))
        })?;
        if &retained != transition {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_paged_transition_stale".to_owned(),
                message: format!(
                    "Machine paged transition {} changed under the current manifest",
                    transition.transition_id
                ),
            });
        }
        match transition.phase {
            MachinePagedTransitionPhase::Finalize => {
                if let Some(anchor) = &self.manifest.machine_base_anchor {
                    let local_authority =
                        paged_archive_request_authority(self.manifest, anchor, transition)?;
                    Ok(PinnedMachinePagedOutcome::NeedsArchive(
                        PinnedMachinePagedArchiveRequest {
                            manifest_id: self.manifest.manifest_id.clone(),
                            anchor: anchor.clone(),
                            transition: transition.clone(),
                            local_authority,
                        },
                    ))
                } else {
                    stage_paged_final(self.manifest, transition, None, overlay)
                        .map(PinnedMachinePagedOutcome::Final)
                }
            }
            MachinePagedTransitionPhase::Effects | MachinePagedTransitionPhase::Scopes => {
                stage_paged_progress(self.manifest, transition, overlay)
                    .map(PinnedMachinePagedOutcome::Progress)
            }
        }
    }
}

/// Exact Scope leaf plus its non-invertible lexical witness.
pub(crate) struct PinnedMachineScopeRead {
    pub(crate) current: MachineScopeCurrent,
    pub(crate) invocation_path: Vec<cymule_core::InvocationPathSegment>,
    pub(crate) region_path: Vec<usize>,
}

/// Exact Run/Continuation authority and the sole profile-owned migration safe
/// point derived from it.
pub(crate) struct PinnedMigrationSafePointRead {
    pub(crate) source: PinnedRunExecutionMaterial,
    pub(crate) safe_point: cymule_profile_protocol::evolution::MigrationSafePoint,
}

/// Exact current execution material shared by higher-profile source
/// admission. Every member was resolved under one manifest revision.
pub(crate) struct PinnedRunExecutionMaterial {
    pub(crate) run: MachineRunCurrent,
    pub(crate) plan: SealedPlan,
    pub(crate) binding: ArtifactRecord,
    pub(crate) continuation: crate::Continuation,
}

/// O(1) authenticated provider-attempt frontier for one component occurrence.
/// The inductive `StateRoot` transition validator owns predecessor-chain
/// continuity; ordinary callers load only the current occurrence and its exact
/// latest Attempt.
pub(crate) struct PinnedComponentAttemptFrontier {
    pub(crate) occurrence: crate::ComponentOccurrence,
    pub(crate) latest_attempt: crate::OperationAttempt,
}

pub(super) fn validate_component_attempt_frontier(
    occurrence: &crate::ComponentOccurrence,
    latest: &crate::OperationAttempt,
) -> DurableResult<()> {
    use crate::{ComponentOccurrenceState, OperationAttemptState};

    if latest.attempt_id != occurrence.latest_attempt_id
        || latest.occurrence_id != occurrence.occurrence_id
        || latest.run_id != occurrence.run_id
        || latest.attempt_ordinal != occurrence.attempt_count
        || latest.operation_occurrence_binding != occurrence.occurrence_binding
        || !matches!(
            (occurrence.state, latest.state),
            (
                ComponentOccurrenceState::Pending,
                OperationAttemptState::Running | OperationAttemptState::Superseded
            ) | (
                ComponentOccurrenceState::Completed,
                OperationAttemptState::Completed
            )
        )
        || (occurrence.state == ComponentOccurrenceState::Completed
            && latest.outcome != occurrence.outcome)
    {
        return Err(DurableError::Integrity {
            code: "state_root_component_attempt_frontier_mismatch".to_owned(),
            message: format!(
                "component occurrence {} and its latest provider Attempt disagree",
                occurrence.occurrence_id
            ),
        });
    }
    Ok(())
}

impl<R: StateRootResolver + ?Sized> PinnedMachineView<'_, R> {
    /// Resolve one exact component occurrence or authenticated absence.
    pub(crate) fn component_occurrence(
        &mut self,
        occurrence_id: &str,
    ) -> DurableResult<Option<crate::ComponentOccurrence>> {
        cymule_core::validate_content_id("component occurrence", occurrence_id)?;
        let occurrence: Option<crate::ComponentOccurrence> = load_leaf(
            &self.manifest.roots.component_occurrences,
            occurrence_id,
            StateRootLeafKind::ComponentOccurrence,
            self.resolver,
        )?;
        if let Some(occurrence) = &occurrence {
            occurrence.verify()?;
            if occurrence.occurrence_id != occurrence_id {
                return Err(DurableError::Integrity {
                    code: "state_root_component_occurrence_key_mismatch".to_owned(),
                    message: format!("component occurrence key {occurrence_id} changed identity"),
                });
            }
        }
        Ok(occurrence)
    }

    /// Resolve one exact provider Attempt or authenticated absence.
    pub(crate) fn operation_attempt(
        &mut self,
        attempt_id: &str,
    ) -> DurableResult<Option<crate::OperationAttempt>> {
        cymule_core::validate_content_id("operation Attempt", attempt_id)?;
        let attempt: Option<crate::OperationAttempt> = load_leaf(
            &self.manifest.roots.operation_attempts,
            attempt_id,
            StateRootLeafKind::OperationAttempt,
            self.resolver,
        )?;
        if let Some(attempt) = &attempt {
            attempt.verify()?;
            if attempt.attempt_id != attempt_id {
                return Err(DurableError::Integrity {
                    code: "state_root_operation_attempt_key_mismatch".to_owned(),
                    message: format!("operation Attempt key {attempt_id} changed identity"),
                });
            }
        }
        Ok(attempt)
    }

    /// Resolve the sole current provider-attempt frontier without scanning the
    /// per-Run attempt-order query log.
    pub(crate) fn component_attempt_frontier(
        &mut self,
        occurrence_id: &str,
    ) -> DurableResult<Option<PinnedComponentAttemptFrontier>> {
        let Some(occurrence) = self.component_occurrence(occurrence_id)? else {
            return Ok(None);
        };
        let latest_attempt = self
            .operation_attempt(&occurrence.latest_attempt_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_component_latest_attempt_missing".to_owned(),
                message: format!(
                    "component occurrence {occurrence_id} references a missing latest Attempt"
                ),
            })?;
        validate_component_attempt_frontier(&occurrence, &latest_attempt)?;
        Ok(Some(PinnedComponentAttemptFrontier {
            occurrence,
            latest_attempt,
        }))
    }

    /// Resolve the exact current Run, Plan, execution binding, and Continuation
    /// without materializing unrelated Machine or M1 families.
    pub(crate) fn run_execution_material(
        &mut self,
        run_id: &str,
    ) -> DurableResult<PinnedRunExecutionMaterial> {
        let run = self.run_current(run_id)?.ok_or_else(|| {
            DurableError::NotFound(format!("Machine Run {run_id} does not exist"))
        })?;
        let plan = self
            .plan(&run.current_plan)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_run_plan_missing".to_owned(),
                message: format!(
                    "Machine Run {run_id} references missing Plan {}",
                    run.current_plan
                ),
            })?;
        let binding = self
            .artifact(&run.current_binding_context)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_run_binding_missing".to_owned(),
                message: format!(
                    "Machine Run {run_id} references missing binding {}",
                    run.current_binding_context
                ),
            })?;
        if binding.reference.kind != cymule_core::EXECUTION_BINDING_ARTIFACT_KIND {
            return Err(DurableError::Integrity {
                code: "state_root_machine_run_binding_kind_mismatch".to_owned(),
                message: format!("Machine Run {run_id} binding has the wrong Artifact kind"),
            });
        }
        let continuation = super::load_continuation(self.manifest, self.resolver, run_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_run_continuation_missing".to_owned(),
                message: format!("Machine Run {run_id} has no exact Continuation"),
            })?;
        if continuation.run_id != run.run_id
            || continuation.plan_id != run.current_plan
            || continuation.binding_context != run.current_binding_context
            || continuation.epoch != run.epoch
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_run_execution_material_mismatch".to_owned(),
                message: format!("Machine Run {run_id}, Plan, binding, and Continuation disagree"),
            });
        }
        Ok(PinnedRunExecutionMaterial {
            run,
            plan,
            binding,
            continuation,
        })
    }

    /// Read and close one migration-safe exact-head cut without inventing a
    /// second Durable quiescence receipt.
    pub(crate) fn migration_safe_point(
        &mut self,
        run_id: &str,
    ) -> DurableResult<PinnedMigrationSafePointRead> {
        let material = self.run_execution_material(run_id)?;
        let run = &material.run;
        let continuation = &material.continuation;
        self.verify_continuation_artifact_closure(continuation)?;
        let current_membership = super::state_map_get(
            &self.manifest.roots.run_query_indexes,
            run_id,
            self.resolver,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "run_query_indexes_missing".to_owned(),
            message: format!("Run {run_id} has no current-membership descriptor"),
        })?
        .decode_run_query_indexes(run_id)?;
        if !matches!(
            run.reducer_state,
            cymule_core::durable_internal::MachineRunReducerState::Ready
        ) || !matches!(
            run.execution_status,
            cymule_core::RunExecutionStatus::Active
        ) || run.active_attempt_id.is_some()
            || run.world_settlement != cymule_core::WorldSettlementStatus::Settled
            || run.current_plan != continuation.plan_id
            || run.current_binding_context != continuation.binding_context
            || run.epoch != continuation.epoch
            || continuation.status != crate::ContinuationStatus::Ready
            || continuation.execution_claim.is_some()
            || !continuation.wait_set.is_empty()
            || continuation.frames.is_empty()
            || continuation.state.is_none()
            || continuation.scope_stack.len() != 1
            || continuation.scope_stack.first().map(String::as_str)
                != Some(cymule_core::ROOT_SCOPE_ID)
            || current_membership.pending_waits.entries != 0
            || current_membership.active_effects.entries != 0
            || current_membership.active_leases.entries != 0
        {
            return Err(DurableError::IllegalTransition(format!(
                "Run {run_id} is not at an exact migration-safe cut"
            )));
        }
        let safe_point = cymule_profile_protocol::evolution::MigrationSafePoint::new(
            self.manifest.revision.clone(),
            continuation,
        )?;
        Ok(PinnedMigrationSafePointRead {
            source: material,
            safe_point,
        })
    }
}

/// Exact at-most-two-page Virtual `ActiveRegions` selection. The selected typed
/// leaf is verified before this proof is returned.
pub(crate) struct VirtualActiveRegionSelectionRead {
    pub(crate) proof: cymule_profile_protocol::virtual_work::VirtualActiveRegionSelectionProof,
}

/// Build the terminal first-plus-optional-wrap `ActiveRegions` proof directly
/// from the pinned `StateRoot` map. No scan or third page is representable.
pub(crate) fn load_virtual_active_region_selection<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    current: &cymule_profile_protocol::virtual_work::VirtualCurrent,
    resolver: &mut R,
) -> DurableResult<VirtualActiveRegionSelectionRead> {
    use cymule_profile_protocol::virtual_work::{
        VirtualActiveRegionSelectionProof, VirtualStateFamily, VirtualStateLeaf,
        virtual_active_region_key, virtual_state_root_id,
    };

    manifest.verify()?;
    super::ensure_resolver_pinned(manifest, resolver)?;
    let root = &manifest.roots.virtual_work.active_regions;
    let source_root_id = virtual_state_root_id(
        VirtualStateFamily::ActiveRegions,
        root.node.as_deref(),
        root.entries,
    )?;
    if source_root_id != current.body.roots.active_regions
        || root.entries != current.body.counts.active_regions
    {
        return Err(DurableError::Integrity {
            code: "state_root_virtual_active_regions_current_mismatch".to_owned(),
            message: "Virtual current does not bind the pinned ActiveRegions map".to_owned(),
        });
    }
    let after_storage_key = current
        .body
        .frontier
        .last_region
        .as_deref()
        .map(|region_id| virtual_active_region_key(&current.body.scheduler_id, region_id))
        .transpose()?;
    let mut overlay = ObjectOverlay::new(resolver);
    let first = load_virtual_active_region_page(
        root,
        &source_root_id,
        after_storage_key.as_deref(),
        &mut overlay,
    )?;
    let wrapped =
        if first.storage_keys().is_empty() && after_storage_key.is_some() && root.entries != 0 {
            Some(load_virtual_active_region_page(
                root,
                &source_root_id,
                None,
                &mut overlay,
            )?)
        } else {
            None
        };
    let proof = VirtualActiveRegionSelectionProof::from_authenticated_pages(
        current,
        &first,
        wrapped.as_ref(),
    )?;
    let _ = proof
        .selected_storage_key()
        .map(|storage_key| {
            let value = map_get(root, storage_key, &mut overlay)?.ok_or_else(|| {
                DurableError::Integrity {
                    code: "state_root_virtual_selected_region_missing".to_owned(),
                    message: format!(
                        "Virtual selected ActiveRegions key {storage_key} does not exist"
                    ),
                }
            })?;
            let leaf: VirtualStateLeaf = value.decode(StateRootLeafKind::VirtualStateLeaf)?;
            leaf.verify()?;
            if leaf.family() != VirtualStateFamily::ActiveRegions
                || leaf.scheduler_id() != current.body.scheduler_id
                || leaf.storage_key()? != storage_key
            {
                return Err(DurableError::Integrity {
                    code: "state_root_virtual_selected_region_mismatch".to_owned(),
                    message:
                        "Virtual selected ActiveRegions leaf changed family, scheduler, or key"
                            .to_owned(),
                });
            }
            Ok(leaf)
        })
        .transpose()?;
    Ok(VirtualActiveRegionSelectionRead { proof })
}

fn load_virtual_active_region_page<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    source_root_id: &str,
    after_storage_key: Option<&str>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<cymule_profile_protocol::virtual_work::VirtualActiveRegionPage> {
    let after = after_storage_key.map(MapPosition::for_key).transpose()?;
    let proof = prove_map_range(root, after.as_ref(), 1, MAX_PAGE_BYTES, overlay)?;
    let verified = verify_map_range(root, after.as_ref(), 1, MAX_PAGE_BYTES, &proof)?;
    let storage_keys = verified
        .entries()
        .iter()
        .map(|(position, _)| position.key().to_owned())
        .collect();
    cymule_profile_protocol::virtual_work::VirtualActiveRegionPage::from_authenticated_range(
        source_root_id.to_owned(),
        root.entries,
        after_storage_key.map(str::to_owned),
        storage_keys,
        verified.has_more(),
    )
    .map_err(Into::into)
}

fn load_leaf<T, R>(
    root: &MapRoot,
    key: &str,
    kind: StateRootLeafKind,
    resolver: &mut R,
) -> DurableResult<Option<T>>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
    R: StateRootResolver + ?Sized,
{
    let mut overlay = ObjectOverlay::new(resolver);
    map_get(root, key, &mut overlay)?
        .map(|value| value.decode(kind))
        .transpose()
}

fn load_run_current_from_root<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    run_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<MachineRunCurrent>> {
    let Some(value) = map_get(root, run_id, overlay)? else {
        return Ok(None);
    };
    let StateRootValue::MachineRunCurrent { current } = value else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_run_value_kind_mismatch".to_owned(),
            message: format!("Machine Run {run_id} is not a typed Run-current descriptor"),
        });
    };
    if current.run_id != run_id {
        return Err(DurableError::Integrity {
            code: "state_root_machine_run_key_mismatch".to_owned(),
            message: format!("Machine Run key {run_id} changed identity"),
        });
    }
    Ok(Some(*current))
}

fn load_scope_current_from_root<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    scope_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<PinnedMachineScopeRead>> {
    let Some(value) = map_get(root, scope_id, overlay)? else {
        return Ok(None);
    };
    let StateRootValue::MachineScopeCurrent {
        current,
        invocation_path,
        region_path,
    } = value
    else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_scope_value_kind_mismatch".to_owned(),
            message: format!("Machine Scope {scope_id} is not a typed Scope-current descriptor"),
        });
    };
    if current.scope_id != scope_id {
        return Err(DurableError::Integrity {
            code: "state_root_machine_scope_key_mismatch".to_owned(),
            message: format!("Machine Scope key {scope_id} changed identity"),
        });
    }
    Ok(Some(PinnedMachineScopeRead {
        current: *current,
        invocation_path,
        region_path,
    }))
}

fn load_paged_transition<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    transition_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<MachinePagedTransitionCurrent>> {
    let Some(value) = map_get(root, transition_id, overlay)? else {
        return Ok(None);
    };
    let StateRootValue::MachinePagedTransitionCurrent { current } = value else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_paged_value_kind_mismatch".to_owned(),
            message: format!("Machine paged transition {transition_id} has the wrong typed value"),
        });
    };
    if current.transition_id != transition_id {
        return Err(DurableError::Integrity {
            code: "state_root_machine_paged_key_mismatch".to_owned(),
            message: format!("Machine paged transition key {transition_id} changed identity"),
        });
    }
    Ok(Some(*current))
}

fn load_hot_batch_replay<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    commands: &[MachinePinnedBatchCommand],
    material: Option<&MachineMaterialAdmission>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<PinnedMachineBatchOutcome>> {
    let mut pending = Vec::new();
    let mut records = Vec::new();
    for command in commands {
        if map_get(
            &manifest.machine_frontier.pending_commands,
            &command.command_id,
            overlay,
        )?
        .is_some()
        {
            pending.push(command.command_id.clone());
        }
        if let Some(value) = map_get(
            &manifest.roots.machine_commands,
            &command.command_id,
            overlay,
        )? {
            let StateRootValue::MachineCommandCurrent {
                record,
                admission,
                index_proof,
                ..
            } = value
            else {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_command_value_kind_mismatch".to_owned(),
                    message: format!(
                        "Machine hot command {} has the wrong value kind",
                        command.command_id
                    ),
                });
            };
            let proof =
                MachinePinnedCommandProof::retained((*record).clone(), *admission, *index_proof);
            if !matches!(
                prepare_pinned_command(
                    manifest.machine_frontier(),
                    &proof,
                    record.envelope.clone(),
                )?,
                PinnedMachineCommandPreparation::Replay(_)
            ) {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_hot_batch_member_not_replayed".to_owned(),
                    message: "hot batch member did not prove exact retained admission".to_owned(),
                });
            }
            records.push(*record);
        }
    }
    if !pending.is_empty() {
        if commands.len() != 1 || pending.len() != 1 || !records.is_empty() {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_batch_partial_pending".to_owned(),
                message: "atomic Machine batch is split across pending or hot authority".to_owned(),
            });
        }
        let transition = load_pending_batch_member(manifest, &commands[0].command_id, overlay)?;
        transition.verify_batch_replay(
            commands,
            material.map(MachineMaterialAdmission::material_digest),
        )?;
        return Ok(Some(PinnedMachineBatchOutcome::Pending(transition)));
    }
    if records.is_empty() {
        return Ok(None);
    }
    if records.len() != commands.len() {
        return Err(DurableError::HistoryConflict {
            code: "state_root_machine_batch_partial_hot_replay".to_owned(),
            message: "atomic Machine batch is split between hot and absent members".to_owned(),
        });
    }
    let batch_id = records[0].batch_id.clone();
    if records.iter().any(|record| record.batch_id != batch_id) {
        return Err(DurableError::HistoryConflict {
            code: "state_root_machine_batch_hot_membership_split".to_owned(),
            message: "requested Machine batch members belong to different batches".to_owned(),
        });
    }
    let batch: cymule_core::durable_internal::MachineCommandBatchRecord =
        map_get(&manifest.roots.machine_command_batches, &batch_id, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_command_batch_missing".to_owned(),
                message: format!("Machine hot batch {batch_id} is missing"),
            })?
            .decode(StateRootLeafKind::MachineCommandBatch)?;
    let receipts = verify_pinned_command_batch_replay(
        &batch,
        commands,
        &records,
        material.map(MachineMaterialAdmission::material_digest),
    )?;
    Ok(Some(PinnedMachineBatchOutcome::Replay(
        PinnedMachineBatchReplay {
            batch_id: batch.batch_id,
            batch_receipt_id: batch.batch_receipt_id,
            receipts,
        },
    )))
}

fn load_pending_batch_member<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    command_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MachinePagedTransitionCurrent> {
    let value = map_get(
        &manifest.machine_frontier.pending_commands,
        command_id,
        overlay,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "state_root_machine_pending_command_missing".to_owned(),
        message: "pending batch lost its exact command reservation".to_owned(),
    })?;
    let StateRootValue::MachinePendingCommand {
        command_id: retained_command,
        transition_id,
    } = value
    else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_pending_value_kind_mismatch".to_owned(),
            message: "pending batch command has the wrong descriptor".to_owned(),
        });
    };
    let transition = load_paged_transition(
        &manifest.machine_frontier.paged_transitions,
        &transition_id,
        overlay,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "state_root_machine_pending_transition_missing".to_owned(),
        message: "pending batch has no exact persisted transition".to_owned(),
    })?;
    if retained_command != command_id || transition.command_id != command_id {
        return Err(DurableError::Integrity {
            code: "state_root_machine_pending_transition_mismatch".to_owned(),
            message: "pending command and transition changed their exact owner".to_owned(),
        });
    }
    Ok(transition)
}

fn paged_archive_request_authority(
    manifest: &StateRootManifest,
    anchor: &cymule_core::MachineBaseAnchor,
    transition: &MachinePagedTransitionCurrent,
) -> DurableResult<String> {
    cymule_core::canonical_digest(&(
        PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
        "paged_archive_request",
        manifest.manifest_id(),
        anchor,
        transition,
    ))
    .map_err(Into::into)
}

fn batch_archive_request_authority(
    manifest: &StateRootManifest,
    anchor: &cymule_core::MachineBaseAnchor,
    commands: &[MachinePinnedBatchCommand],
    material: Option<&MachineMaterialAdmission>,
    start_material: Option<&MachineStartRunMaterial>,
    command_ids: &[String],
) -> DurableResult<String> {
    cymule_core::canonical_digest(&(
        PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
        "batch_archive_request",
        manifest.manifest_id(),
        anchor,
        commands,
        material.map(MachineMaterialAdmission::material_digest),
        start_material.map(MachineStartRunMaterial::material_digest),
        command_ids,
    ))
    .map_err(Into::into)
}

fn archived_batch_request_authority(
    manifest: &StateRootManifest,
    batch_id: &str,
    commands: &[MachinePinnedBatchCommand],
    entries: &[cymule_core::MachineCommandArchiveEntry],
    material_digest: Option<&str>,
) -> DurableResult<String> {
    cymule_core::canonical_digest(&(
        PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
        "archived_batch_request",
        manifest.manifest_id(),
        batch_id,
        commands,
        entries,
        material_digest,
    ))
    .map_err(Into::into)
}

fn load_material_parent_reads<R: StateRootResolver + ?Sized>(
    roots: &super::StateRoots,
    material: &MachineMaterialAdmission,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MachineMaterialParentReads> {
    let plan_keys = material
        .plans()
        .iter()
        .map(|plan| plan.plan_id.clone())
        .collect();
    let artifact_keys = material
        .artifacts()
        .iter()
        .map(|artifact| artifact.reference.artifact_id.clone())
        .collect();
    Ok(MachineMaterialParentReads::new(
        load_leaf_set(
            &roots.machine_plans,
            plan_keys,
            StateRootLeafKind::MachinePlan,
            overlay,
        )?,
        load_leaf_set(
            &roots.machine_artifacts,
            artifact_keys,
            StateRootLeafKind::MachineArtifact,
            overlay,
        )?,
    ))
}

fn prepare_fresh_command_batch<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    commands: Vec<MachinePinnedBatchCommand>,
    material: Option<MachineMaterialAdmission>,
    mut start_material: Option<MachineStartRunMaterial>,
    mut proofs: BTreeMap<String, MachinePinnedCommandProof>,
    mut overlay: ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineBatchOutcome> {
    let material_input = material
        .map(|value| {
            load_material_parent_reads(&manifest.roots, &value, &mut overlay)
                .map(|reads| (value, reads))
        })
        .transpose()?;
    let mut prepared =
        prepare_pinned_command_batch(manifest.machine_frontier(), commands, material_input)?;
    let mut working_roots = manifest.roots().clone();
    let mut working_anchor = manifest.machine_base_anchor.clone();
    if let Some(delta) = prepared.material_delta() {
        super::apply_machine_root_delta(
            &mut working_roots,
            delta,
            manifest.machine_frontier(),
            prepared.material_frontier(),
            &mut working_anchor,
            &mut overlay,
        )?;
    }
    while prepared.next_command().is_some() {
        let envelope = prepared.next_envelope()?;
        let proof = proofs
            .remove(&envelope.command_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_batch_command_proof_missing".to_owned(),
                message: format!(
                    "Machine batch command {} has no exact command-index proof",
                    envelope.command_id
                ),
            })?;
        let member_start_material = if matches!(envelope.command, Command::StartRun { .. }) {
            start_material.take()
        } else {
            None
        };
        let fresh = prepare_batch_member(
            &mut prepared,
            &envelope,
            &proof,
            &working_roots,
            manifest.revision(),
            member_start_material,
            &mut overlay,
        )?;
        let step = match fresh {
            PinnedMachineFreshPreparation::Prepared(step) => *step,
            PinnedMachineFreshPreparation::PagedBegin(begin) => {
                let material = prepared.into_paged_begin(*begin)?;
                let roots =
                    apply_prepared_roots(material.root_mutations()?, &envelope, &mut overlay)?;
                let begin = material.finish(roots)?;
                return stage_paged_begin(manifest, &envelope, begin, overlay)
                    .map(PinnedMachineBatchOutcome::PagedBegin);
            }
        };
        let transition = finish_prepared_command_dag(step, &envelope, &mut overlay)?;
        prepared = prepared.accept_step(transition)?;
    }
    if !proofs.is_empty() {
        return Err(DurableError::Integrity {
            code: "state_root_machine_batch_unused_command_proof".to_owned(),
            message: "Machine batch left an unrelated command-index proof".to_owned(),
        });
    }
    stage_finished_command_batch(manifest, prepared.finish()?, None, overlay)
        .map(PinnedMachineBatchOutcome::Staged)
}

fn prepare_batch_member<R: StateRootResolver + ?Sized>(
    prepared: &mut PreparedPinnedCommandBatch,
    envelope: &CommandEnvelope,
    proof: &MachinePinnedCommandProof,
    roots: &super::StateRoots,
    revision: &str,
    start_material: Option<MachineStartRunMaterial>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineFreshPreparation> {
    let PinnedMachineCommandPreparation::Lookup(lookup) = prepared.prepare_next(proof)? else {
        return Err(DurableError::HistoryConflict {
            code: "state_root_machine_fresh_batch_command_not_fresh".to_owned(),
            message: format!(
                "fresh Machine batch command {} resolved as replay or pending",
                envelope.command_id
            ),
        });
    };
    let current_frontier = prepared.current_frontier().clone();
    let run = load_run_current_from_root(&current_frontier.runs, &envelope.run_id, overlay)?;
    let PinnedMachineRunPreparation::Reads(read) =
        lookup.resolve_run(MachinePinnedRunLookup::new(
            revision.to_owned(),
            envelope.run_id.clone(),
            current_frontier.runs.clone(),
            run.clone(),
        ))?
    else {
        return Err(DurableError::HistoryConflict {
            code: "state_root_machine_batch_stale_precondition".to_owned(),
            message: format!(
                "Machine batch command {} has a stale parent precondition",
                envelope.command_id
            ),
        });
    };
    let mut inputs = load_command_read_inputs(
        roots,
        &current_frontier,
        revision,
        envelope,
        run,
        start_material,
        overlay,
    )?;
    if matches!(
        envelope.command,
        Command::CommitScope { .. }
            | Command::AbortScope { .. }
            | Command::FailRun { .. }
            | Command::CancelRun { .. }
    ) && let Some(material) = prepared.proposed_material()
    {
        supply_proposed_material_reads(&mut inputs, material)?;
    }
    if let Command::CommitScope { scope_id } | Command::AbortScope { scope_id } = &envelope.command
    {
        let scope = inputs
            .scopes
            .get(scope_id)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                DurableError::NotFound(format!("Machine Scope {scope_id} does not exist"))
            })?;
        if let Some(requirement) = read.inline_scope_read_requirement(scope)? {
            supply_inline_scope_reads(&mut inputs, &envelope.command, &requirement, overlay)?;
        }
    }
    read.prepare(inputs).map_err(Into::into)
}

fn supply_inline_scope_reads<R: StateRootResolver + ?Sized>(
    inputs: &mut MachineRunReadInputs,
    command: &Command,
    requirement: &MachineInlineScopeReadRequirement,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let run = inputs.run.as_ref().ok_or_else(|| DurableError::Integrity {
        code: "state_root_inline_scope_run_missing".to_owned(),
        message: "inline Scope closure requires its exact Run current".to_owned(),
    })?;
    let scope = inputs
        .scopes
        .get(&requirement.scope_id)
        .and_then(Option::as_ref)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_inline_scope_current_missing".to_owned(),
            message: "inline Scope closure requires its exact target current".to_owned(),
        })?;
    if let Some(parent_id) = &scope.parent_scope {
        let parent = load_required_scope(run, parent_id, overlay)?;
        inputs
            .scopes
            .insert(parent_id.clone(), Some(parent.current));
    }
    let proof = prove_map_range(
        &requirement.index_root,
        None,
        MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
        MAX_PAGE_BYTES,
        overlay,
    )?;
    let index = MachineRunIndexPage::verify_proof(
        inputs.run_id.clone(),
        requirement.index_selector.clone(),
        &requirement.index_root,
        None,
        &proof,
    )?;
    let log = load_machine_log_page(
        &inputs.run_id,
        requirement.log_selector.clone(),
        &requirement.log_root,
        0,
        overlay,
    )?;
    let effects = index.entries().iter().cloned().collect::<BTreeSet<_>>();
    inputs.effects = load_leaf_set(
        &run.children.effects,
        effects.clone(),
        StateRootLeafKind::MachineEffect,
        overlay,
    )?;
    if matches!(command, Command::CommitScope { .. }) {
        let obligations = effects
            .iter()
            .map(|intent| effect_obligation_id(intent))
            .collect::<Result<BTreeSet<_>, _>>()?;
        inputs.obligations = load_leaf_set(
            &run.children.obligations,
            obligations,
            StateRootLeafKind::MachineObligation,
            overlay,
        )?;
    }
    inputs.index_pages.push(index);
    inputs.log_pages.push(log);
    Ok(())
}

fn load_machine_log_page<R: StateRootResolver + ?Sized>(
    run_id: &str,
    selector: MachineRunLogSelector,
    root: &cymule_authenticated_collections::LogRoot,
    start: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MachineRunLogPage> {
    let proof = prove_log_range(
        root,
        start,
        MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
        MAX_PAGE_BYTES,
        overlay,
    )?;
    let verified = verify_log_range(
        root,
        start,
        MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
        MAX_PAGE_BYTES,
        &proof,
    )?;
    let mut entries = Vec::with_capacity(verified.values().len());
    for value_id in verified.values() {
        let StateRootValue::MachineOrderEntry {
            run_id: owner,
            selector: retained,
            entry,
        } = overlay.load_value(value_id)?
        else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_order_value_kind_mismatch".to_owned(),
                message: "Machine order page contains another typed value".to_owned(),
            });
        };
        if owner != run_id || retained != selector {
            return Err(DurableError::Integrity {
                code: "state_root_machine_order_owner_mismatch".to_owned(),
                message: "Machine order page changed its exact Run or selector".to_owned(),
            });
        }
        entries.push(entry);
    }
    MachineRunLogPage::verify_proof(run_id.to_owned(), selector, root, start, entries, &proof)
        .map_err(Into::into)
}

fn supply_proposed_material_reads(
    inputs: &mut MachineRunReadInputs,
    material: &MachineMaterialAdmission,
) -> DurableResult<()> {
    for plan in material.plans() {
        if let Some(read) = inputs.plans.get_mut(&plan.plan_id) {
            if read.as_ref().is_some_and(|retained| retained != plan) {
                return Err(DurableError::Integrity {
                    code: "state_root_paged_material_plan_conflict".to_owned(),
                    message: format!(
                        "paged material Plan {} changed retained bytes",
                        plan.plan_id
                    ),
                });
            }
            *read = Some(plan.clone());
        }
    }
    for artifact in material.artifacts() {
        if let Some(read) = inputs.artifacts.get_mut(&artifact.reference.artifact_id) {
            if read.as_ref().is_some_and(|retained| retained != artifact) {
                return Err(DurableError::Integrity {
                    code: "state_root_paged_material_artifact_conflict".to_owned(),
                    message: format!(
                        "paged material Artifact {} changed retained bytes",
                        artifact.reference.artifact_id
                    ),
                });
            }
            *read = Some(artifact.clone());
        }
    }
    Ok(())
}

fn stage_history_compaction<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    prepared: PreparedPinnedMachineCompaction,
    receipt: crate::HistoryCompactionReceipt,
    mut overlay: ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineStagedMutation> {
    let mut roots = manifest.roots().clone();
    let mut machine_base_anchor = manifest.machine_base_anchor.clone();
    super::apply_machine_root_delta(
        &mut roots,
        prepared.root_delta(),
        manifest.machine_frontier(),
        prepared.frontier(),
        &mut machine_base_anchor,
        &mut overlay,
    )?;
    let stage_digest = cymule_core::canonical_digest(&(
        PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
        manifest.manifest_id(),
        "history_compaction",
        &receipt,
        prepared.frontier(),
    ))?;
    Ok(PinnedMachineStagedMutation {
        parent_manifest: manifest.manifest_id.clone(),
        stage_digest,
        transition: PinnedMachineStageTransition::Compaction {
            prepared: Box::new(prepared),
            receipt: Box::new(receipt),
        },
        roots,
        machine_base_anchor,
        pending: overlay.into_pending(),
    })
}

fn stage_finished_command_batch<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    batch: PinnedMachineBatchTransition,
    terminal: Option<&MachinePagedTransitionCurrent>,
    mut overlay: ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineStagedMutation> {
    let mut roots = manifest.roots().clone();
    let mut machine_base_anchor = manifest.machine_base_anchor.clone();
    super::apply_machine_root_delta(
        &mut roots,
        &batch.machine,
        manifest.machine_frontier(),
        &batch.frontier,
        &mut machine_base_anchor,
        &mut overlay,
    )?;
    if let Some(transition) = terminal {
        let result = batch
            .steps
            .last()
            .and_then(|step| step.run.as_ref())
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_terminal_batch_run_missing".to_owned(),
                message: "paged terminal batch lost its exact final Run current".to_owned(),
            })?;
        super::finish_terminal_sidecars(
            manifest,
            transition,
            &result.result_current,
            &mut roots,
            &mut overlay,
        )?;
    }
    let stage_digest = cymule_core::canonical_digest(&(
        PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
        manifest.manifest_id(),
        "batch",
        &batch,
    ))?;
    Ok(PinnedMachineStagedMutation {
        parent_manifest: manifest.manifest_id.clone(),
        stage_digest,
        transition: PinnedMachineStageTransition::Batch(Box::new(batch)),
        roots,
        machine_base_anchor,
        pending: overlay.into_pending(),
    })
}

#[derive(Default)]
struct CommandReadKeys {
    plans: BTreeSet<String>,
    artifacts: BTreeSet<String>,
    scopes: BTreeSet<String>,
    locations: BTreeSet<String>,
    effects: BTreeSet<String>,
    obligations: BTreeSet<String>,
    attempts: BTreeSet<String>,
    facts: BTreeSet<String>,
}

impl CommandReadKeys {
    fn add_start_material(
        &mut self,
        command: &Command,
        material: Option<&MachineStartRunMaterial>,
    ) -> DurableResult<()> {
        let Command::StartRun {
            plan_id,
            binding_context,
            input,
            material_digest,
            initial_attempt,
        } = command
        else {
            return Err(DurableError::Validation(
                "initial material requires StartRun".to_owned(),
            ));
        };
        cymule_core::validate_content_id("StartRun material", material_digest)?;
        initial_attempt.verify(binding_context)?;
        if material.map(MachineStartRunMaterial::material_digest) != Some(material_digest.as_str())
        {
            return Err(DurableError::Integrity {
                code: "state_root_start_run_material_mismatch".to_owned(),
                message: "StartRun material does not match its exact command digest".to_owned(),
            });
        }
        self.plans.insert(plan_id.clone());
        self.artifacts
            .extend([binding_context.clone(), input.artifact_id.clone()]);
        Ok(())
    }

    fn add_proposed_effect(
        &mut self,
        command: &Command,
        run: &MachineRunCurrent,
    ) -> DurableResult<()> {
        let Command::ProposeEffect {
            scope_id,
            invocation_id,
            invocation_path,
            site_id,
            occurrence,
            args,
            execution_binding,
            ..
        } = command
        else {
            return Err(DurableError::Validation(
                "Effect material requires ProposeEffect".to_owned(),
            ));
        };
        self.plans.insert(run.current_plan.clone());
        self.scopes.insert(scope_id.clone());
        self.locations.insert(scope_id.clone());
        for segment in invocation_path {
            self.scopes.insert(segment.scope_id.clone());
            self.locations.insert(segment.scope_id.clone());
        }
        self.artifacts.extend([
            args.artifact_id.clone(),
            execution_binding.artifact_id.clone(),
        ]);
        self.effects
            .insert(effect_intent_id(&EffectIntentIdentityInput {
                run_id: &run.run_id,
                plan_id: &run.current_plan,
                invocation_id,
                site_id,
                scope_id,
                occurrence,
                args,
                effect_schema_version: cymule_core::EFFECT_SCHEMA_VERSION,
            })?);
        Ok(())
    }
}

fn command_read_keys<R: StateRootResolver + ?Sized>(
    command: &Command,
    live: Option<&MachineRunCurrent>,
    start_material: Option<&MachineStartRunMaterial>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<CommandReadKeys> {
    let mut keys = CommandReadKeys::default();
    match command {
        Command::StartRun { .. } => keys.add_start_material(command, start_material)?,
        Command::BeginAttempt { attempt_id, .. } | Command::YieldAttempt { attempt_id, .. } => {
            keys.attempts.insert(attempt_id.clone());
        }
        Command::AdvanceEpoch => {
            if let Some(attempt_id) = live.and_then(|run| run.active_attempt_id.clone()) {
                keys.attempts.insert(attempt_id);
            }
        }
        Command::OpenScope {
            scope_id,
            parent_scope,
            invocation_path,
            ..
        } => {
            let live = require_live_run(live)?;
            keys.plans.insert(live.current_plan.clone());
            keys.scopes.extend([scope_id.clone(), parent_scope.clone()]);
            keys.locations.insert(parent_scope.clone());
            for segment in invocation_path {
                keys.scopes.insert(segment.scope_id.clone());
                keys.locations.insert(segment.scope_id.clone());
            }
        }
        Command::ProposeEffect { .. } => {
            keys.add_proposed_effect(command, require_live_run(live)?)?;
        }
        Command::TransitionEffect { intent_id, .. } => {
            let live = require_live_run(live)?;
            keys.effects.insert(intent_id.clone());
            let effect: cymule_core::EffectProjection = load_required_leaf(
                &live.children.effects,
                intent_id,
                StateRootLeafKind::MachineEffect,
                overlay,
                "Machine Effect",
            )?;
            keys.scopes.insert(effect.scope_id.clone());
            let scope = load_required_scope(live, &effect.scope_id, overlay)?;
            if effect.profile.mutation == cymule_core::MutationKind::Mutating
                && scope.current.status == cymule_core::ScopeStatus::ClosedCommitted
            {
                keys.obligations.insert(effect_obligation_id(intent_id)?);
            }
        }
        Command::CommitScope { scope_id } | Command::AbortScope { scope_id } => {
            keys.scopes.insert(scope_id.clone());
        }
        Command::UpdateBinding { binding_context } => {
            keys.artifacts.insert(binding_context.clone());
        }
        Command::MigrateRun {
            from_plan,
            to_plan,
            from_binding,
            to_binding,
            ..
        } => {
            keys.plans.extend([from_plan.clone(), to_plan.clone()]);
            keys.artifacts
                .extend([from_binding.clone(), to_binding.clone()]);
        }
        Command::RecordFact { key, .. } => {
            keys.facts.insert(key.clone());
        }
        Command::CompleteRun { result } => {
            if let Some(result) = result {
                keys.artifacts.insert(result.artifact_id.clone());
            }
        }
        Command::FailRun { failure } => {
            keys.artifacts.insert(failure.detail.artifact_id.clone());
        }
        Command::CancelRun { reason } => {
            keys.artifacts.insert(reason.artifact_id.clone());
        }
    }

    Ok(keys)
}

struct LoadedScopeInputs {
    scopes: BTreeMap<String, Option<MachineScopeCurrent>>,
    locations: BTreeMap<String, cymule_core::durable_internal::MachineScopeLocationWitness>,
}

fn load_scope_inputs<R: StateRootResolver + ?Sized>(
    live: Option<&MachineRunCurrent>,
    scopes: BTreeSet<String>,
    locations: &BTreeSet<String>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LoadedScopeInputs> {
    let mut scope_values = BTreeMap::new();
    let mut scope_locations = BTreeMap::new();
    if let Some(live) = live {
        for scope_id in scopes {
            let value = load_scope_current_from_root(&live.children.scopes, &scope_id, overlay)?;
            if locations.contains(&scope_id) {
                let exact = value.as_ref().ok_or_else(|| {
                    DurableError::NotFound(format!("Machine Scope {scope_id} does not exist"))
                })?;
                scope_locations.insert(
                    scope_id.clone(),
                    cymule_core::durable_internal::MachineScopeLocationWitness::new(
                        scope_id.clone(),
                        exact.invocation_path.clone(),
                        exact.region_path.clone(),
                    )?,
                );
            }
            scope_values.insert(scope_id, value.map(|value| value.current));
        }
    } else {
        for scope_id in scopes {
            scope_values.insert(scope_id, None);
        }
    }

    Ok(LoadedScopeInputs {
        scopes: scope_values,
        locations: scope_locations,
    })
}

fn load_command_read_inputs<R: StateRootResolver + ?Sized>(
    roots: &super::StateRoots,
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    revision: &str,
    envelope: &CommandEnvelope,
    run: Option<MachineRunCurrent>,
    start_material: Option<MachineStartRunMaterial>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MachineRunReadInputs> {
    let keys = command_read_keys(
        &envelope.command,
        run.as_ref(),
        start_material.as_ref(),
        overlay,
    )?;
    let live = run.as_ref();
    let LoadedScopeInputs {
        scopes: scope_values,
        locations: scope_locations,
    } = load_scope_inputs(live, keys.scopes, &keys.locations, overlay)?;
    let read_plans = load_leaf_set(
        &roots.machine_plans,
        keys.plans,
        StateRootLeafKind::MachinePlan,
        overlay,
    )?;
    let read_artifacts = load_leaf_set(
        &roots.machine_artifacts,
        keys.artifacts,
        StateRootLeafKind::MachineArtifact,
        overlay,
    )?;
    let read_effects = match live {
        Some(live) => load_leaf_set(
            &live.children.effects,
            keys.effects,
            StateRootLeafKind::MachineEffect,
            overlay,
        )?,
        None => empty_absence_set(keys.effects),
    };
    let read_obligations = match live {
        Some(live) => load_leaf_set(
            &live.children.obligations,
            keys.obligations,
            StateRootLeafKind::MachineObligation,
            overlay,
        )?,
        None => empty_absence_set(keys.obligations),
    };
    let read_attempts = match live {
        Some(live) => load_leaf_set(
            &live.children.attempts,
            keys.attempts,
            StateRootLeafKind::MachineAttempt,
            overlay,
        )?,
        None => empty_absence_set(keys.attempts),
    };
    let read_facts = load_leaf_set(
        &frontier.facts,
        keys.facts,
        StateRootLeafKind::MachineFact,
        overlay,
    )?;

    let needs_empty = matches!(
        envelope.command,
        Command::StartRun { .. } | Command::OpenScope { .. }
    );
    Ok(MachineRunReadInputs {
        machine_revision: revision.to_owned(),
        run_id: envelope.run_id.clone(),
        runs_root: frontier.runs.clone(),
        facts_root: frontier.facts.clone(),
        run,
        new_run_empty_root: needs_empty.then(MapRoot::empty),
        new_run_empty_log: needs_empty.then(cymule_authenticated_collections::LogRoot::empty),
        plans: read_plans,
        artifacts: read_artifacts,
        scopes: scope_values,
        scope_locations,
        effects: read_effects,
        obligations: read_obligations,
        attempts: read_attempts,
        facts: read_facts,
        start_material,
        index_pages: Vec::new(),
        log_pages: Vec::new(),
    })
}

fn require_live_run(run: Option<&MachineRunCurrent>) -> DurableResult<&MachineRunCurrent> {
    run.ok_or_else(|| {
        DurableError::NotFound("Machine command target Run does not exist".to_owned())
    })
}

fn load_required_scope<R: StateRootResolver + ?Sized>(
    run: &MachineRunCurrent,
    scope_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineScopeRead> {
    load_scope_current_from_root(&run.children.scopes, scope_id, overlay)?
        .ok_or_else(|| DurableError::NotFound(format!("Machine Scope {scope_id} does not exist")))
}

fn load_required_leaf<T, R>(
    root: &MapRoot,
    key: &str,
    kind: StateRootLeafKind,
    overlay: &mut ObjectOverlay<'_, R>,
    label: &str,
) -> DurableResult<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
    R: StateRootResolver + ?Sized,
{
    map_get(root, key, overlay)?
        .map(|value| value.decode(kind))
        .transpose()?
        .ok_or_else(|| DurableError::NotFound(format!("{label} {key} does not exist")))
}

fn load_leaf_set<T, R>(
    root: &MapRoot,
    keys: BTreeSet<String>,
    kind: StateRootLeafKind,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<BTreeMap<String, Option<T>>>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
    R: StateRootResolver + ?Sized,
{
    keys.into_iter()
        .map(|key| {
            let value = map_get(root, &key, overlay)?
                .map(|value| value.decode(kind))
                .transpose()?;
            Ok((key, value))
        })
        .collect()
}

fn empty_absence_set<T>(keys: BTreeSet<String>) -> BTreeMap<String, Option<T>> {
    keys.into_iter().map(|key| (key, None)).collect()
}

fn stage_paged_progress<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    transition: &MachinePagedTransitionCurrent,
    mut overlay: ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineStagedMutation> {
    let live_run = load_run_current_from_root(
        &manifest.machine_frontier.runs,
        &transition.run_id,
        &mut overlay,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!("Machine Run {} does not exist", transition.run_id))
    })?;
    let inputs = load_paged_progress_inputs(live_run, transition, &mut overlay)?;
    let prepared =
        prepare_pinned_transition_page(manifest.machine_frontier(), transition, &inputs)?;
    let shadow_updates = apply_prepared_roots(
        prepared.shadow_root_mutations()?,
        &transition.envelope,
        &mut overlay,
    )?;
    let progress = prepared.finish_shadow_roots(shadow_updates)?;
    let transition_update = apply_prepared_root(
        progress.transition_root_mutation()?,
        &transition.envelope,
        &mut overlay,
    )?;
    let result = progress.finish(transition_update)?;
    let mut roots = manifest.roots().clone();
    super::advance_terminal_sidecars(manifest, transition, &result, &mut roots, &mut overlay)?;
    let stage_digest = cymule_core::canonical_digest(&(
        PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
        manifest.manifest_id(),
        "paged_progress",
        &result,
    ))?;
    Ok(PinnedMachineStagedMutation {
        parent_manifest: manifest.manifest_id.clone(),
        stage_digest,
        transition: PinnedMachineStageTransition::PagedProgress(Box::new(result)),
        roots,
        machine_base_anchor: manifest.machine_base_anchor.clone(),
        pending: overlay.into_pending(),
    })
}

fn load_paged_progress_inputs<R: StateRootResolver + ?Sized>(
    live_run: MachineRunCurrent,
    transition: &MachinePagedTransitionCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MachinePagedReadInputs> {
    let selector = pinned_paged_log_selector(transition)?;
    let source = match transition.phase {
        MachinePagedTransitionPhase::Effects => &transition.effect_source,
        MachinePagedTransitionPhase::Scopes => &transition.scope_source,
        MachinePagedTransitionPhase::Finalize => {
            return Err(DurableError::IllegalTransition(
                "final Machine paged transition has no source page".to_owned(),
            ));
        }
    };
    let page = load_machine_log_page(
        &transition.run_id,
        selector,
        source,
        transition.next_index,
        overlay,
    )?;
    let entries = page.entries().to_vec();
    if entries.is_empty() {
        return Err(DurableError::Integrity {
            code: "state_root_machine_paged_empty_nonterminal_page".to_owned(),
            message: format!(
                "Machine paged transition {} has no value at its nonterminal cursor",
                transition.transition_id
            ),
        });
    }

    let mut scopes = BTreeMap::new();
    let mut effects = BTreeMap::new();
    let mut obligations = BTreeMap::new();
    match transition.phase {
        MachinePagedTransitionPhase::Effects => {
            for intent_id in entries {
                let effect: cymule_core::EffectProjection = load_required_leaf(
                    &transition.shadow.children.effects,
                    &intent_id,
                    StateRootLeafKind::MachineEffect,
                    overlay,
                    "Machine Effect",
                )?;
                let scope = load_required_scope_from_root(
                    &transition.shadow.children.scopes,
                    &effect.scope_id,
                    overlay,
                )?;
                if let Some(obligation_id) =
                    pinned_paged_obligation_read(transition, &effect, &scope.current)?
                {
                    let value = map_get(
                        &transition.shadow.children.obligations,
                        &obligation_id,
                        overlay,
                    )?
                    .map(|value| value.decode(StateRootLeafKind::MachineObligation))
                    .transpose()?;
                    obligations.insert(obligation_id, value);
                }
                scopes.insert(effect.scope_id.clone(), scope.current);
                effects.insert(intent_id, effect);
            }
        }
        MachinePagedTransitionPhase::Scopes => {
            for scope_id in entries {
                let scope = load_required_scope_from_root(
                    &transition.shadow.children.scopes,
                    &scope_id,
                    overlay,
                )?;
                scopes.insert(scope_id, scope.current);
            }
        }
        MachinePagedTransitionPhase::Finalize => unreachable!("phase was rejected above"),
    }
    Ok(MachinePagedReadInputs::new(
        live_run,
        page,
        scopes,
        effects,
        obligations,
    ))
}

fn stage_paged_final<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    transition: &MachinePagedTransitionCurrent,
    archive_lookup: Option<MachineCommandArchiveLookup>,
    mut overlay: ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineStagedMutation> {
    let live_run = load_run_current_from_root(
        &manifest.machine_frontier.runs,
        &transition.run_id,
        &mut overlay,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!("Machine Run {} does not exist", transition.run_id))
    })?;
    let mut scopes = BTreeMap::new();
    match &transition.action {
        MachinePagedTransitionAction::CommitScope { scope_id }
        | MachinePagedTransitionAction::AbortScope { scope_id } => {
            let target = load_required_scope_from_root(
                &transition.shadow.children.scopes,
                scope_id,
                &mut overlay,
            )?;
            if let Some(parent_id) = &target.current.parent_scope {
                let parent = load_required_scope_from_root(
                    &transition.shadow.children.scopes,
                    parent_id,
                    &mut overlay,
                )?;
                scopes.insert(parent_id.clone(), parent.current);
            }
            scopes.insert(scope_id.clone(), target.current);
        }
        MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun => {}
    }
    let active_attempt = match &transition.action {
        MachinePagedTransitionAction::FailRun | MachinePagedTransitionAction::CancelRun => live_run
            .active_attempt_id
            .as_ref()
            .map(|attempt_id| {
                load_required_leaf(
                    &transition.shadow.children.attempts,
                    attempt_id,
                    StateRootLeafKind::MachineAttempt,
                    &mut overlay,
                    "Machine Attempt",
                )
            })
            .transpose()?,
        MachinePagedTransitionAction::CommitScope { .. }
        | MachinePagedTransitionAction::AbortScope { .. } => None,
    };
    let command_index_proof =
        load_current_command_nonmembership(manifest, &transition.command_id, archive_lookup)?;
    let material = load_paged_material_admission(transition, &mut overlay)?
        .map(|material| {
            load_material_parent_reads(&manifest.roots, &material, &mut overlay)
                .map(|reads| (material, reads))
        })
        .transpose()?;
    let inputs = MachinePagedFinalizeInputs::new(
        live_run,
        scopes,
        active_attempt,
        command_index_proof,
        material,
    );
    let prepared =
        prepare_pinned_transition_final(manifest.machine_frontier(), transition, inputs)?;
    let shadow_updates = apply_prepared_roots(
        prepared.shadow_root_mutations()?,
        &transition.envelope,
        &mut overlay,
    )?;
    let publish = prepared.finish_shadow_roots(shadow_updates)?;
    let root_updates = apply_prepared_roots(
        publish.root_mutations()?,
        &transition.envelope,
        &mut overlay,
    )?;
    stage_finished_command_batch(
        manifest,
        publish.finish(root_updates)?,
        Some(transition),
        overlay,
    )
}

/// Reconstruct only the bounded proposal rooted by one retained paged command.
/// The frozen batch lists every permitted semantic identity, so this never
/// scans a global material map or treats unlisted pending material as input.
pub(super) fn load_paged_material_admission<R: StateRootResolver + ?Sized>(
    transition: &MachinePagedTransitionCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<MachineMaterialAdmission>> {
    transition.verify()?;
    let Some(digest) = &transition.batch_manifest.material_digest else {
        return Ok(None);
    };
    let source = transition
        .batch_manifest
        .material_source
        .as_ref()
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_paged_material_source_missing".to_owned(),
            message: "pending material digest has no frozen source manifest".to_owned(),
        })?;
    let mut plans = Vec::new();
    for plan_id in &source.plan_ids {
        let value =
            map_get(&transition.staged_material.plans, plan_id, overlay)?.ok_or_else(|| {
                DurableError::Integrity {
                    code: "state_root_paged_material_plan_missing".to_owned(),
                    message: format!("pending material Plan {plan_id} is missing"),
                }
            })?;
        let plan: SealedPlan = value.decode(StateRootLeafKind::MachinePlan)?;
        plan.verify()?;
        if plan.plan_id != *plan_id {
            return Err(DurableError::Integrity {
                code: "state_root_paged_material_plan_key_mismatch".to_owned(),
                message: "pending material Plan changed its frozen identity".to_owned(),
            });
        }
        plans.push(plan);
    }
    let mut artifacts = Vec::new();
    for reference in &source.artifacts {
        let value = map_get(
            &transition.staged_material.artifacts,
            &reference.artifact_id,
            overlay,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_paged_material_artifact_missing".to_owned(),
            message: format!(
                "pending material Artifact {} is missing",
                reference.artifact_id
            ),
        })?;
        let artifact: ArtifactRecord = value.decode(StateRootLeafKind::MachineArtifact)?;
        artifact.validate()?;
        if artifact.reference != *reference {
            return Err(DurableError::Integrity {
                code: "state_root_paged_material_artifact_key_mismatch".to_owned(),
                message: "pending material Artifact changed its exact frozen reference".to_owned(),
            });
        }
        artifacts.push(artifact);
    }
    if u64::try_from(plans.len()).ok() != Some(transition.staged_material.plans.entries)
        || u64::try_from(artifacts.len()).ok() != Some(transition.staged_material.artifacts.entries)
    {
        return Err(DurableError::Integrity {
            code: "state_root_paged_material_closure_mismatch".to_owned(),
            message: "pending material contains an identity outside its frozen batch".to_owned(),
        });
    }
    let material =
        MachineMaterialAdmission::new(source.source_command_id.clone(), plans, artifacts)?;
    if material.material_digest() != digest {
        return Err(DurableError::Integrity {
            code: "state_root_paged_material_digest_mismatch".to_owned(),
            message: "pending material records do not reproduce their frozen batch digest"
                .to_owned(),
        });
    }
    Ok(Some(material))
}

fn load_required_scope_from_root<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    scope_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineScopeRead> {
    load_scope_current_from_root(root, scope_id, overlay)?
        .ok_or_else(|| DurableError::NotFound(format!("Machine Scope {scope_id} does not exist")))
}

fn load_current_command_nonmembership(
    manifest: &StateRootManifest,
    command_id: &str,
    archive_lookup: Option<MachineCommandArchiveLookup>,
) -> DurableResult<cymule_core::MachineCommandIndexProof> {
    match (manifest.machine_base_anchor.as_ref(), archive_lookup) {
        (None, None) => cymule_core::MachineCommandIndexProof::empty_nonmembership(command_id)
            .map_err(Into::into),
        (None, Some(_)) => Err(DurableError::Validation(
            "uncompacted paged finalization carried archive authority".to_owned(),
        )),
        (Some(_), None) => Err(DurableError::Validation(
            "compacted paged finalization requires Store-owned archive non-membership".to_owned(),
        )),
        (Some(anchor), Some(MachineCommandArchiveLookup::NonMember { index_proof })) => {
            if anchor.command_index_root != manifest.machine_frontier.command_index_root {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_archive_frontier_mismatch".to_owned(),
                    message: "Machine archive root does not match the pinned frontier".to_owned(),
                });
            }
            Ok(index_proof)
        }
        (Some(_), Some(MachineCommandArchiveLookup::Member { .. })) => {
            Err(DurableError::HistoryConflict {
                code: "state_root_machine_pending_command_archived".to_owned(),
                message: format!(
                    "pending Machine command {command_id} already exists in the current archive"
                ),
            })
        }
    }
}

fn finish_prepared_command_dag<R: StateRootResolver + ?Sized>(
    prepared: PreparedPinnedMachineTransition,
    envelope: &CommandEnvelope,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineTransition> {
    let mut witnesses = BTreeMap::new();
    for event in prepared.events()? {
        if let cymule_core::EventPayload::ScopeOpened {
            scope_id,
            invocation_path,
            region_path,
            ..
        } = &event.payload
            && witnesses
                .insert(
                    scope_id.clone(),
                    ScopeInsertionWitness {
                        invocation_path: invocation_path.clone(),
                        region_path: region_path.clone(),
                    },
                )
                .is_some()
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_scope_witness_repeated".to_owned(),
                message: "Core Scope insertion repeats a lexical witness".to_owned(),
            });
        }
    }
    let scope_updates = apply_prepared_roots(&prepared.scope_root_mutations()?, envelope, overlay)?;
    let run = prepared.finish_scope_roots(scope_updates)?;
    let run_updates = apply_prepared_roots_with_scope_witnesses(
        &run.run_root_mutations()?,
        envelope,
        &witnesses,
        overlay,
    )?;
    let global = run.finish_run_roots(run_updates)?;
    let global_updates = apply_prepared_roots(&global.global_root_mutations()?, envelope, overlay)?;
    global.finish(global_updates).map_err(Into::into)
}

fn stage_paged_begin<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    envelope: &CommandEnvelope,
    prepared: cymule_core::durable_internal::PreparedPinnedPagedBegin,
    mut overlay: ObjectOverlay<'_, R>,
) -> DurableResult<PinnedMachineStagedMutation> {
    let updates = apply_prepared_roots(prepared.root_mutations()?, envelope, &mut overlay)?;
    let result = prepared.finish(updates)?;
    let mut roots = manifest.roots().clone();
    super::begin_terminal_sidecars(manifest, &result, &mut roots, &mut overlay)?;
    let stage_digest = cymule_core::canonical_digest(&(
        PINNED_MACHINE_STATE_ROOT_STAGE_DOMAIN,
        manifest.manifest_id(),
        "paged_begin",
        &result,
    ))?;
    Ok(PinnedMachineStagedMutation {
        parent_manifest: manifest.manifest_id.clone(),
        stage_digest,
        transition: PinnedMachineStageTransition::PagedBegin(Box::new(result)),
        roots,
        machine_base_anchor: manifest.machine_base_anchor.clone(),
        pending: overlay.into_pending(),
    })
}

fn apply_prepared_roots<R: StateRootResolver + ?Sized>(
    plans: &[MachinePreparedRootMutation],
    envelope: &CommandEnvelope,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Vec<MachineRunRootUpdate>> {
    apply_prepared_roots_with_scope_witnesses(plans, envelope, &BTreeMap::new(), overlay)
}

struct ScopeInsertionWitness {
    invocation_path: Vec<cymule_core::InvocationPathSegment>,
    region_path: Vec<usize>,
}

fn apply_prepared_roots_with_scope_witnesses<R: StateRootResolver + ?Sized>(
    plans: &[MachinePreparedRootMutation],
    envelope: &CommandEnvelope,
    witnesses: &BTreeMap<String, ScopeInsertionWitness>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Vec<MachineRunRootUpdate>> {
    plans
        .iter()
        .map(|plan| apply_prepared_root_with_scope_witnesses(plan, envelope, witnesses, overlay))
        .collect()
}

fn apply_prepared_root<R: StateRootResolver + ?Sized>(
    plan: &MachinePreparedRootMutation,
    envelope: &CommandEnvelope,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MachineRunRootUpdate> {
    apply_prepared_root_with_scope_witnesses(plan, envelope, &BTreeMap::new(), overlay)
}

fn apply_prepared_root_with_scope_witnesses<R: StateRootResolver + ?Sized>(
    plan: &MachinePreparedRootMutation,
    envelope: &CommandEnvelope,
    witnesses: &BTreeMap<String, ScopeInsertionWitness>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MachineRunRootUpdate> {
    let result = match (plan.parent(), plan.typed()) {
        (MachinePhysicalRoot::Map(parent), typed) => MachinePhysicalRoot::Map(
            apply_machine_map_mutation(parent, typed, envelope, witnesses, overlay)?,
        ),
        (MachinePhysicalRoot::Log(parent), typed) => MachinePhysicalRoot::Log(
            apply_machine_log_mutation(parent, typed, envelope, overlay)?,
        ),
    };
    if match &result {
        MachinePhysicalRoot::Map(root) => root.entries,
        MachinePhysicalRoot::Log(root) => root.len,
    } != plan.expected_count()
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_prepared_count_mismatch".to_owned(),
            message: format!(
                "Machine root {:?} result count differs from its Core preparation",
                plan.target()
            ),
        });
    }
    Ok(plan.bind_result(result))
}

fn apply_machine_map_mutation<R: StateRootResolver + ?Sized>(
    parent: &MapRoot,
    typed: &MachineTypedRootMutation,
    envelope: &CommandEnvelope,
    witnesses: &BTreeMap<String, ScopeInsertionWitness>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MapRoot> {
    let mut root = parent.clone();
    match typed {
        MachineTypedRootMutation::PutMaterialPlans(values) => {
            put_material_plans(&mut root, values, overlay)?;
        }
        MachineTypedRootMutation::PutMaterialArtifacts(values) => {
            put_material_artifacts(&mut root, values, overlay)?;
        }
        MachineTypedRootMutation::PutRuns(values) => {
            put_machine_runs(&mut root, values, overlay)?;
        }
        MachineTypedRootMutation::PutScopes(values) => {
            put_machine_scopes(&mut root, values, envelope, witnesses, overlay)?;
        }
        MachineTypedRootMutation::PutEffects(values) => {
            put_machine_leaf_values(&mut root, values, StateRootLeafKind::MachineEffect, overlay)?;
        }
        MachineTypedRootMutation::PutObligations(values) => put_machine_leaf_values(
            &mut root,
            values,
            StateRootLeafKind::MachineObligation,
            overlay,
        )?,
        MachineTypedRootMutation::PutAttempts(values) => put_machine_leaf_values(
            &mut root,
            values,
            StateRootLeafKind::MachineAttempt,
            overlay,
        )?,
        MachineTypedRootMutation::PutFacts(values) => {
            put_machine_facts(&mut root, values, overlay)?;
        }
        MachineTypedRootMutation::UpdateMembership(deltas) => {
            update_machine_membership(&mut root, deltas, envelope, overlay)?;
        }
        MachineTypedRootMutation::ReserveCommand {
            command_id,
            transition_id,
        } => {
            reserve_machine_command(&mut root, command_id, transition_id, overlay)?;
        }
        MachineTypedRootMutation::PutPagedTransition(current) => {
            root = super::map_put(
                &root,
                &current.transition_id,
                StateRootValue::machine_paged_transition_current((**current).clone())?,
                overlay,
            )?;
        }
        MachineTypedRootMutation::RemoveCommandReservation {
            command_id,
            transition_id,
        } => {
            root = remove_exact_map_value(
                &root,
                command_id,
                &StateRootValue::machine_pending_command(
                    command_id.clone(),
                    transition_id.clone(),
                )?,
                overlay,
            )?;
        }
        MachineTypedRootMutation::RemovePagedTransition {
            transition_id,
            transition_digest,
        } => remove_machine_paged_transition(&mut root, transition_id, transition_digest, overlay)?,
        MachineTypedRootMutation::AppendLog(_) => {
            return Err(DurableError::Integrity {
                code: "state_root_machine_root_kind_mismatch".to_owned(),
                message: "Machine log append targeted a map root".to_owned(),
            });
        }
    }
    Ok(root)
}

fn put_machine_runs<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    values: &BTreeMap<String, MachineRunCurrent>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (key, current) in values {
        *root = super::map_put(
            root,
            key,
            StateRootValue::machine_run_current(current.clone())?,
            overlay,
        )?;
    }
    Ok(())
}

fn put_machine_facts<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    values: &BTreeMap<String, String>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (key, value) in values {
        if map_get(root, key, overlay)?.is_some() {
            return Err(DurableError::HistoryConflict {
                code: "state_root_machine_fact_reuse".to_owned(),
                message: format!("Machine fact {key} already exists"),
            });
        }
        *root = super::map_put(
            root,
            key,
            StateRootValue::encode(StateRootLeafKind::MachineFact, value)?,
            overlay,
        )?;
    }
    Ok(())
}

fn reserve_machine_command<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    command_id: &str,
    transition_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    if map_get(root, command_id, overlay)?.is_some() {
        return Err(DurableError::HistoryConflict {
            code: "state_root_machine_pending_command_reuse".to_owned(),
            message: format!("Machine pending command {command_id} already exists"),
        });
    }
    *root = super::map_put(
        root,
        command_id,
        StateRootValue::machine_pending_command(command_id.to_owned(), transition_id.to_owned())?,
        overlay,
    )?;
    Ok(())
}

fn put_material_plans<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    values: &BTreeMap<String, SealedPlan>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (key, plan) in values {
        plan.verify()?;
        if plan.plan_id != *key {
            return Err(DurableError::Integrity {
                code: "state_root_paged_material_plan_key_mismatch".to_owned(),
                message: "pending material Plan changed its exact storage key".to_owned(),
            });
        }
        super::insert_immutable_typed_value(
            root,
            key,
            StateRootLeafKind::MachinePlan,
            plan,
            "pending Machine Plan",
            overlay,
        )?;
    }
    Ok(())
}

fn put_material_artifacts<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    values: &BTreeMap<String, ArtifactRecord>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (key, artifact) in values {
        artifact.validate()?;
        if artifact.reference.artifact_id != *key {
            return Err(DurableError::Integrity {
                code: "state_root_paged_material_artifact_key_mismatch".to_owned(),
                message: "pending material Artifact changed its exact storage key".to_owned(),
            });
        }
        super::insert_immutable_typed_value(
            root,
            key,
            StateRootLeafKind::MachineArtifact,
            artifact,
            "pending Machine Artifact",
            overlay,
        )?;
    }
    Ok(())
}

fn put_machine_scopes<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    values: &BTreeMap<String, MachineScopeCurrent>,
    envelope: &CommandEnvelope,
    witnesses: &BTreeMap<String, ScopeInsertionWitness>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (key, current) in values {
        let retained = map_get(root, key, overlay)?;
        let (invocation_path, region_path) = match retained {
            Some(StateRootValue::MachineScopeCurrent {
                current: retained,
                invocation_path,
                region_path,
            }) if retained.scope_id == *key => (invocation_path, region_path),
            Some(_) => {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_scope_value_kind_mismatch".to_owned(),
                    message: format!("Machine Scope {key} is not its typed current descriptor"),
                });
            }
            None => new_scope_witness(envelope, key, witnesses)?,
        };
        *root = super::map_put(
            root,
            key,
            StateRootValue::machine_scope_current(current.clone(), invocation_path, region_path)?,
            overlay,
        )?;
    }
    Ok(())
}

fn update_machine_membership<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    deltas: &[cymule_core::durable_internal::MachineRunIndexMembershipDelta],
    envelope: &CommandEnvelope,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for delta in deltas {
        for entry in &delta.removed {
            let value = StateRootValue::machine_index_membership(
                envelope.run_id.clone(),
                delta.selector.clone(),
                entry.clone(),
            )?;
            *root = remove_exact_map_value(root, entry, &value, overlay)?;
        }
        for entry in &delta.inserted {
            if map_get(root, entry, overlay)?.is_some() {
                return Err(DurableError::HistoryConflict {
                    code: "state_root_machine_index_member_reuse".to_owned(),
                    message: format!("Machine index member {entry} already exists"),
                });
            }
            *root = super::map_put(
                root,
                entry,
                StateRootValue::machine_index_membership(
                    envelope.run_id.clone(),
                    delta.selector.clone(),
                    entry.clone(),
                )?,
                overlay,
            )?;
        }
    }
    Ok(())
}

fn remove_machine_paged_transition<R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    transition_id: &str,
    transition_digest: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let retained =
        map_get(root, transition_id, overlay)?.ok_or_else(|| DurableError::HistoryConflict {
            code: "state_root_machine_paged_transition_missing".to_owned(),
            message: format!("Machine paged transition {transition_id} does not exist"),
        })?;
    let StateRootValue::MachinePagedTransitionCurrent { current } = &retained else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_paged_value_kind_mismatch".to_owned(),
            message: format!("Machine paged transition {transition_id} has the wrong typed value"),
        });
    };
    if cymule_core::canonical_digest(current.as_ref())? != transition_digest {
        return Err(DurableError::Integrity {
            code: "state_root_machine_paged_transition_digest_mismatch".to_owned(),
            message: format!("Machine paged transition {transition_id} changed before removal"),
        });
    }
    *root = remove_exact_map_value(root, transition_id, &retained, overlay)?;
    Ok(())
}

fn put_machine_leaf_values<T: serde::Serialize, R: StateRootResolver + ?Sized>(
    root: &mut MapRoot,
    values: &BTreeMap<String, T>,
    kind: StateRootLeafKind,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (key, value) in values {
        *root = super::map_put(root, key, StateRootValue::encode(kind, value)?, overlay)?;
    }
    Ok(())
}

fn apply_machine_log_mutation<R: StateRootResolver + ?Sized>(
    parent: &cymule_authenticated_collections::LogRoot,
    typed: &MachineTypedRootMutation,
    envelope: &CommandEnvelope,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<cymule_authenticated_collections::LogRoot> {
    let MachineTypedRootMutation::AppendLog(deltas) = typed else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_root_kind_mismatch".to_owned(),
            message: "Machine map mutation targeted a log root".to_owned(),
        });
    };
    let mut root = parent.clone();
    for delta in deltas {
        let values = delta
            .values
            .iter()
            .map(|entry| {
                StateRootValue::machine_order_entry(
                    envelope.run_id.clone(),
                    delta.selector.clone(),
                    entry.clone(),
                )
            })
            .collect::<DurableResult<Vec<_>>>()?;
        root = super::log_append(&root, &values, overlay)?;
    }
    Ok(root)
}

fn new_scope_witness(
    envelope: &CommandEnvelope,
    scope_id: &str,
    witnesses: &BTreeMap<String, ScopeInsertionWitness>,
) -> DurableResult<(Vec<cymule_core::InvocationPathSegment>, Vec<usize>)> {
    if scope_id == cymule_core::ROOT_SCOPE_ID
        && matches!(envelope.command, Command::StartRun { .. })
    {
        return Ok((Vec::new(), Vec::new()));
    }
    match &envelope.command {
        Command::OpenScope {
            scope_id: opened, ..
        } if opened == scope_id => witnesses
            .get(scope_id)
            .map(|witness| (witness.invocation_path.clone(), witness.region_path.clone()))
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_scope_witness_missing".to_owned(),
                message: format!(
                    "Machine Scope {scope_id} has no Core-admitted ScopeOpened witness"
                ),
            }),
        _ => Err(DurableError::Integrity {
            code: "state_root_machine_scope_witness_missing".to_owned(),
            message: format!(
                "Machine Scope {scope_id} was inserted without its exact lexical witness"
            ),
        }),
    }
}

fn remove_exact_map_value<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    expected: &StateRootValue,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MapRoot> {
    let retained = map_get(root, key, overlay)?.ok_or_else(|| DurableError::HistoryConflict {
        code: "state_root_machine_remove_missing".to_owned(),
        message: format!("Machine map key {key} does not exist"),
    })?;
    if &retained != expected {
        return Err(DurableError::Integrity {
            code: "state_root_machine_remove_value_mismatch".to_owned(),
            message: format!("Machine map key {key} changed before removal"),
        });
    }
    super::map_remove(root, key, overlay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_core::durable_internal::MachineAuthorityFrontier;
    use cymule_profile_protocol::virtual_work::{
        FrontierLimits, SchedulingPolicy, VIRTUAL_ACTIVE_REGION_CURRENT_VERSION,
        VirtualActiveRegionCurrent, VirtualArchiveBinding, VirtualCurrent, VirtualCurrentBody,
        VirtualCurrentCounts, VirtualCurrentDraft, VirtualFrontierCurrent, VirtualStateFamily,
        VirtualStateLeaf, VirtualStateRoots, virtual_active_region_key,
        virtual_command_index_empty_root, virtual_state_root_id, virtual_work_index_empty_root,
    };

    #[derive(Default)]
    struct TestResolver {
        pinned: String,
        objects: BTreeMap<String, super::super::StateRootObject>,
        loads: usize,
        map_node_loads: usize,
        value_loads: usize,
    }

    impl TestResolver {
        fn insert_all(&mut self, objects: impl IntoIterator<Item = super::super::StateRootObject>) {
            for object in objects {
                self.objects.insert(object.object_id().to_owned(), object);
            }
        }
    }

    impl StateRootResolver for TestResolver {
        fn pinned_manifest_id(&self) -> &str {
            &self.pinned
        }

        fn load_state_root_object(
            &mut self,
            object_id: &str,
        ) -> DurableResult<Option<super::super::StateRootObject>> {
            self.loads += 1;
            let object = self.objects.get(object_id).cloned();
            match &object {
                Some(super::super::StateRootObject::MapNode(_)) => self.map_node_loads += 1,
                Some(super::super::StateRootObject::Value(_)) => self.value_loads += 1,
                _ => {}
            }
            Ok(object)
        }
    }

    fn empty_virtual_root(family: VirtualStateFamily) -> String {
        virtual_state_root_id(family, None, 0).expect("empty Virtual root derives")
    }

    fn active_region_fixture(
        region_ids: &[&str],
        last_region: Option<&str>,
    ) -> (StateRootManifest, TestResolver, VirtualCurrent, Vec<String>) {
        let scheduler_id = "scheduler:active-region-test";
        let mut empty = super::super::EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let mut values = BTreeMap::new();
        let mut storage_keys = Vec::new();
        for region_id in region_ids {
            let leaf = VirtualStateLeaf::ActiveRegions(VirtualActiveRegionCurrent {
                leaf_version: VIRTUAL_ACTIVE_REGION_CURRENT_VERSION.to_owned(),
                scheduler_id: scheduler_id.to_owned(),
                region_id: (*region_id).to_owned(),
            });
            leaf.verify().expect("active-region leaf verifies");
            let key = leaf.storage_key().expect("active-region key derives");
            storage_keys.push(key.clone());
            values.insert(key, leaf);
        }
        let active_regions = super::super::build_typed_map(
            StateRootLeafKind::VirtualStateLeaf,
            values,
            &mut overlay,
        )
        .expect("ActiveRegions map builds");
        let current = active_region_current(scheduler_id, region_ids, last_region, &active_regions);
        let mut roots = super::super::StateRoots::empty();
        roots.virtual_work.active_regions = active_regions;
        let frontier = MachineAuthorityFrontier::genesis(
            MapRoot::empty(),
            MapRoot::empty(),
            MapRoot::empty(),
            MapRoot::empty(),
        )
        .expect("Machine frontier derives");
        let revision = super::super::derive_genesis_revision(super::super::DurableRevisionState {
            durable_version: crate::DURABLE_STATE_VERSION,
            machine_snapshot_version: cymule_core::MachineSnapshot::VERSION,
            machine_frontier: &frontier,
            machine_base_anchor: None,
            roots: &roots,
        })
        .expect("manifest revision derives");
        let manifest = StateRootManifest::new(
            super::super::StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("manifest seals");
        let objects = overlay.finish(&manifest).expect("object closure seals");
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(objects);
        storage_keys.sort_by_key(|key| {
            cymule_authenticated_collections::map_key_hash(key).expect("storage-key hash derives")
        });
        (manifest, resolver, current, storage_keys)
    }

    fn active_region_current(
        scheduler_id: &str,
        region_ids: &[&str],
        last_region: Option<&str>,
        active_regions: &MapRoot,
    ) -> VirtualCurrent {
        let active_root_id = virtual_state_root_id(
            VirtualStateFamily::ActiveRegions,
            active_regions.node.as_deref(),
            active_regions.entries,
        )
        .expect("ActiveRegions semantic root derives");
        let virtual_roots = VirtualStateRoots {
            regions: virtual_state_root_id(
                VirtualStateFamily::Regions,
                active_regions.node.as_deref(),
                active_regions.entries,
            )
            .expect("Regions semantic root derives"),
            active_regions: active_root_id,
            parked: empty_virtual_root(VirtualStateFamily::Parked),
            parked_index: empty_virtual_root(VirtualStateFamily::ParkedIndex),
            work: empty_virtual_root(VirtualStateFamily::Work),
            occurrences: empty_virtual_root(VirtualStateFamily::Occurrences),
            runs: empty_virtual_root(VirtualStateFamily::Runs),
            migrations: empty_virtual_root(VirtualStateFamily::Migrations),
            certificates: empty_virtual_root(VirtualStateFamily::Certificates),
        };
        let count = u64::try_from(region_ids.len()).expect("region count fits u64");
        let body = VirtualCurrentBody::new(
            VirtualCurrentDraft {
                scheduler_id: scheduler_id.to_owned(),
                limits: FrontierLimits {
                    max_materialized: 16,
                    max_active: 4,
                    max_active_per_run: 1,
                    materialize_batch: 1,
                },
                scheduling_policy: SchedulingPolicy::default(),
                archive: VirtualArchiveBinding::new("archive:test", "revision:test")
                    .expect("archive binding verifies"),
                frontier: VirtualFrontierCurrent {
                    ready: BTreeMap::new(),
                    active: BTreeMap::new(),
                    dispatch_sequence: 0,
                    ready_since: BTreeMap::new(),
                    wait_activations: BTreeMap::new(),
                    last_run: None,
                    last_region: last_region.map(str::to_owned),
                },
                archived_work_index_root_digest: virtual_work_index_empty_root(),
                archived_command_index_root_digest: virtual_command_index_empty_root(),
                counts: VirtualCurrentCounts {
                    regions: count,
                    active_regions: count,
                    parked: 0,
                    hot_work: 0,
                    hot_occurrences: 0,
                    runs: 0,
                    migrations: 0,
                    certificates: 0,
                },
            },
            virtual_roots,
        )
        .expect("Virtual current body seals");
        VirtualCurrent::new(
            body,
            cymule_core::content_id("cymule.test.virtual-receipt/1", &region_ids)
                .expect("receipt identity derives"),
        )
        .expect("Virtual current seals")
    }

    fn region_for_key(current: &VirtualCurrent, key: &str) -> &'static str {
        ["region:a", "region:b", "region:c"]
            .into_iter()
            .find(|region| {
                virtual_active_region_key(&current.body.scheduler_id, region)
                    .is_ok_and(|candidate| candidate == key)
            })
            .expect("region key resolves")
    }

    #[test]
    fn virtual_active_region_selection_uses_one_suffix_page() {
        let (_, _, probe, ordered) =
            active_region_fixture(&["region:a", "region:b", "region:c"], None);
        let cursor_region = region_for_key(&probe, &ordered[0]);
        let (manifest, mut resolver, current, ordered) =
            active_region_fixture(&["region:a", "region:b", "region:c"], Some(cursor_region));
        let read = load_virtual_active_region_selection(&manifest, &current, &mut resolver)
            .expect("suffix selection verifies");
        assert_eq!(read.proof.authenticated_page_count(), 1);
        assert_eq!(read.proof.selected_storage_key(), Some(ordered[1].as_str()));
        assert_eq!(resolver.value_loads, 1);
        assert!(resolver.map_node_loads < 24);
        assert!(resolver.loads < 32, "selection must remain bounded");
    }

    #[test]
    fn virtual_active_region_selection_wraps_once_from_terminal_suffix() {
        let (_, _, probe, ordered) =
            active_region_fixture(&["region:a", "region:b", "region:c"], None);
        let last_key = ordered.last().expect("active regions exist");
        let last_region = region_for_key(&probe, last_key);
        let (manifest, mut resolver, current, ordered) =
            active_region_fixture(&["region:a", "region:b", "region:c"], Some(last_region));
        let read = load_virtual_active_region_selection(&manifest, &current, &mut resolver)
            .expect("wrapped selection verifies");
        assert_eq!(read.proof.authenticated_page_count(), 2);
        assert_eq!(
            read.proof.selected_storage_key(),
            ordered.first().map(String::as_str)
        );
        assert_eq!(resolver.value_loads, 1);
        assert!(resolver.map_node_loads < 32);
        assert!(resolver.loads < 48, "wrap must use at most two ranges");
    }

    #[test]
    fn virtual_active_region_empty_and_wrong_root_fail_closed() {
        let (manifest, mut resolver, current, _) = active_region_fixture(&[], None);
        let read = load_virtual_active_region_selection(&manifest, &current, &mut resolver)
            .expect("empty selection verifies");
        assert_eq!(read.proof.authenticated_page_count(), 1);
        assert!(read.proof.selected_storage_key().is_none());
        assert_eq!(resolver.value_loads, 0);
        assert_eq!(resolver.map_node_loads, 0);

        let (manifest, mut resolver, mut wrong, _) = active_region_fixture(&["region:a"], None);
        wrong.body.roots.active_regions =
            cymule_core::content_id("cymule.test.wrong-active-root/1", &())
                .expect("wrong root derives");
        assert!(matches!(
            load_virtual_active_region_selection(&manifest, &wrong, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_virtual_active_regions_current_mismatch"
        ));
    }

    #[test]
    fn virtual_active_region_nonmember_cursor_cannot_skip_authenticated_prefix() {
        let (manifest, mut resolver, current, _) =
            active_region_fixture(&["region:a", "region:b"], Some("region:not-active"));
        assert!(load_virtual_active_region_selection(&manifest, &current, &mut resolver).is_err());
    }
}
