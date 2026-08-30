use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, TryLockError};

#[cfg(test)]
use crate::{ApplicationJournalPrefixReplacementReceipt, JournalRecord};
use crate::{
    ComponentOccurrence, Continuation, CoordinationLease, DurableError, DurableResult,
    DurableState, EffectDispatch, HistoryCompactionReceipt, OperationAttempt, StoredState,
    WaitActivationReceipt, WaitCondition,
};
use cymule_core::{canonical_digest, content_id};
use serde::{Deserialize, Serialize};

/// Small mutable storage-head schema.
pub const STORE_HEAD_VERSION: &str = "cymule.durable-head/2";
/// Cold-reclamation receipt schema.
pub const GC_RECEIPT_VERSION: &str = "cymule.durable-gc-receipt/2";
/// Physical-head fencing token generation.
pub const PHYSICAL_TOKEN_VERSION: &str = "cymule.durable-physical-token/2";
/// Maximum canonical encoded size of the mutable Store head.
pub const MAX_STORE_HEAD_BYTES: usize = 16 * 1024;
/// Maximum canonical encoded size of one physical GC receipt.
pub const MAX_GC_RECEIPT_BYTES: usize = 32 * 1024 * 1024;
/// Maximum sorted object identities reclaimed by one physical GC generation.
pub const MAX_GC_RECLAIMED_OBJECTS: usize = 262_144;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Bounded mutable pointer committed by compare-and-swap.
pub struct StoreHead {
    /// Versioned head shape.
    pub head_version: String,
    /// Canonical semantic state revision.
    pub revision: String,
    /// Exact fixed state-root manifest for the complete current projection.
    pub state_root_manifest_id: String,
    /// Exact trusted Core compacted-base anchor, absent before Machine history
    /// compaction exists.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub machine_base_anchor: Option<cymule_core::MachineBaseAnchor>,
    /// Monotonic physical commit sequence.
    pub sequence: u64,
    /// Monotonic physical-only reclamation sequence.
    pub gc_sequence: u64,
    /// Exact physical generation token. Semantic commits and GC both advance it.
    pub physical_token: String,
    /// Latest cold-reclamation receipt.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub gc_receipt: Option<String>,
}

impl StoreHead {
    /// Verify the bounded versioned shape.
    ///
    /// # Errors
    /// Returns an error for unsupported versions, malformed identities,
    /// invalid anchors, or size and exact-integer bound violations.
    pub fn verify(&self) -> DurableResult<()> {
        if cymule_core::canonical_bytes(self)?.len() > MAX_STORE_HEAD_BYTES {
            return Err(DurableError::Validation(format!(
                "durable head exceeds {MAX_STORE_HEAD_BYTES} canonical bytes"
            )));
        }
        if self.head_version != STORE_HEAD_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable head version {:?}",
                self.head_version
            )));
        }
        cymule_core::validate_content_id("durable head revision", &self.revision)?;
        cymule_core::validate_content_id(
            "durable head state-root manifest",
            &self.state_root_manifest_id,
        )?;
        verify_machine_base_anchor_shape(self.machine_base_anchor.as_ref())?;
        cymule_core::validate_content_id("durable head physical token", &self.physical_token)?;
        if let Some(receipt_id) = &self.gc_receipt {
            cymule_core::validate_content_id("durable head GC receipt", receipt_id)?;
        }
        if self.sequence > crate::MAX_EXACT_INTEGER
            || self.gc_sequence > crate::MAX_EXACT_INTEGER
            || (self.gc_sequence == 0 && self.gc_receipt.is_some())
        {
            return Err(DurableError::Validation(
                "durable head sequence exceeds the exact integer range".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Opaque coordinator-issued capability for one exact physical reclamation
/// operation.
///
/// Store adapters can inspect the expected head while servicing a request but
/// application callers cannot construct a reclamation authority from a public
/// [`StoreHead`].
#[derive(Debug)]
pub struct StoreReclamation {
    expected: StoreHead,
}

impl StoreReclamation {
    pub(crate) fn new(expected: &StoreHead) -> DurableResult<Self> {
        expected.verify()?;
        Ok(Self {
            expected: expected.clone(),
        })
    }

    /// Return the exact current head fenced by this capability.
    pub fn expected_head(&self) -> &StoreHead {
        &self.expected
    }
}

fn verify_machine_base_anchor_shape(
    anchor: Option<&cymule_core::MachineBaseAnchor>,
) -> DurableResult<()> {
    if let Some(anchor) = anchor {
        anchor.verify()?;
    }
    Ok(())
}

fn derive_physical_token(
    parent_token: Option<&str>,
    state_root_manifest_id: &str,
    sequence: u64,
    gc_sequence: u64,
    archive_object_ids: &[String],
    gc_receipt: Option<&str>,
) -> DurableResult<String> {
    content_id(
        PHYSICAL_TOKEN_VERSION,
        &(
            parent_token,
            state_root_manifest_id,
            sequence,
            gc_sequence,
            archive_object_ids,
            gc_receipt,
        ),
    )
    .map_err(Into::into)
}

/// Closed provider-neutral mutations admitted by one M1 commit.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum DurableOperation {
    /// Insert or replace one Continuation.
    PutContinuation {
        /// Exact next Continuation.
        value: Continuation,
    },
    /// Insert or replace one bounded Run query current.
    PutRunCurrent {
        /// Exact semantic current projected from the same Machine and
        /// Continuation transition.
        value: crate::DurableRunCurrent,
    },
    /// Insert or replace one wait.
    PutWait {
        /// Exact next wait record.
        value: WaitCondition,
    },
    /// Insert one identified activation receipt.
    PutWaitActivation {
        /// Exact activation receipt.
        value: WaitActivationReceipt,
    },
    /// Insert or replace one coordination lease.
    PutLease {
        /// Exact next lease.
        value: CoordinationLease,
    },
    /// Insert or replace one Effect dispatch.
    PutOutbox {
        /// Exact next outbox record.
        value: EffectDispatch,
    },
    /// Insert one component occurrence.
    PutComponentOccurrence {
        /// Immutable occurrence record.
        value: ComponentOccurrence,
    },
    /// Insert or advance one provider Attempt.
    PutOperationAttempt {
        /// Exact next provider Attempt.
        value: OperationAttempt,
    },
    /// Insert one immutable logical Clock observation.
    PutClockObservation {
        /// Content-backed Clock evidence.
        value: crate::ClockObservation,
    },
    /// Remove one hot Clock receipt after no active execution claim references
    /// it. The selected Clock substrate remains the cold historical authority.
    RemoveClockObservation {
        /// Exact observation identity being reclaimed from hot state.
        observation_id: String,
    },
    /// Insert one Machine-history compaction receipt.
    PutHistoryCompaction {
        /// Authenticated compaction receipt.
        value: HistoryCompactionReceipt,
    },
    /// Insert the exact typed receipt for one semantic Run cancellation.
    PutCancellationReceipt {
        /// Immutable normalized cancellation authority.
        value: crate::CancellationReceipt,
    },
    /// Insert the exact typed receipt for one terminal Effect resolution.
    PutEffectResolutionReceipt {
        /// Immutable normalized resolution authority.
        value: crate::EffectResolutionReceipt,
    },
    /// Append exact records to one typed application journal.
    #[cfg(test)]
    AppendJournal {
        /// Stable journal identity.
        journal_id: String,
        /// Exact new record suffix.
        records: Vec<JournalRecord>,
    },
    /// Replace one exact authenticated index-zero journal prefix with bounded
    /// typed base records and retain the latest replacement receipt.
    #[cfg(test)]
    ReplaceJournalPrefix {
        /// Complete normalized command and immutable receipt.
        receipt: ApplicationJournalPrefixReplacementReceipt,
    },
    /// Insert one complete higher-profile journal plus M1 checkpoint receipt.
    PutCoupledCheckpointReceipt {
        /// Content-addressed complete coupling authority.
        value: crate::CoupledCheckpointReceipt,
    },
    /// Insert one immutable closed Resource command receipt under every exact
    /// authority alias derived from that receipt.
    PutResourceCommandReceipt {
        /// Complete closed Resource command receipt.
        value: cymule_profile_protocol::resource::ResourceCommandReceipt,
    },
    /// Insert or replace one keyed physical Resource-retention projection.
    PutResourceRetentionCurrent {
        /// Exact current projection.
        value: cymule_profile_protocol::resource::ResourceRetentionCurrent,
    },
    /// Insert or replace one keyed exact Resource-pin projection.
    PutResourcePinCurrent {
        /// Exact current projection.
        value: cymule_profile_protocol::resource::ResourcePinCurrent,
    },
    /// Insert or replace one keyed Resource-deletion projection.
    PutResourceDeleteCurrent {
        /// Exact current projection.
        value: cymule_profile_protocol::resource::ResourceDeleteCurrent,
    },
    /// Insert one immutable keyed Resource-handoff authority.
    PutResourceHandoffCurrent {
        /// Exact current authority.
        value: cymule_profile_protocol::resource::ResourceHandoffCurrent,
    },
    /// Insert one immutable target-slot Resource-handoff entry.
    PutResourceHandoffSlot {
        /// Exact position-bound slot entry.
        value: cymule_profile_protocol::resource::ResourceHandoffIndexEntry,
    },
    /// Insert one immutable keyed Resource-handoff activation authority.
    PutResourceHandoffActivationCurrent {
        /// Exact current authority.
        value: cymule_profile_protocol::resource::ResourceHandoffActivationCurrent,
    },
    /// Append one exact position-bound target Resource-handoff entry.
    AppendResourceHandoffIndex {
        /// Exact position-bound index entry.
        value: cymule_profile_protocol::resource::ResourceHandoffIndexEntry,
    },
    /// Append one exact position-bound target Resource activation entry.
    AppendResourceHandoffActivationIndex {
        /// Exact position-bound activation index entry.
        value: cymule_profile_protocol::resource::ResourceHandoffActivationIndexEntry,
    },
    /// Insert one immutable closed Agent persistence command.
    PutAgentCommand {
        /// Exact semantic command.
        value: cymule_profile_protocol::agent::AgentCommand,
    },
    /// Insert one immutable closed Agent persistence receipt.
    PutAgentCommandReceipt {
        /// Exact semantic receipt.
        value: Box<cymule_profile_protocol::agent::AgentCommandReceipt>,
    },
    /// Insert one immutable Durable-private Agent input suspension receipt.
    PutAgentInputSuspensionReceipt {
        /// Exact M1 Wait/Continuation suspension authority.
        value: crate::model::AgentInputSuspensionReceipt,
    },
    /// Insert one immutable Durable-private Agent input completion receipt.
    PutAgentInputCompletionReceipt {
        /// Exact M1 result/Wait/Continuation completion authority.
        value: crate::model::AgentInputCompletionReceipt,
    },
    /// Insert or replace one bounded Agent Session current.
    PutAgentSessionCurrent {
        /// Exact Session metadata.
        value: cymule_profile_protocol::agent::AgentSessionCurrent,
    },
    /// Insert one immutable Agent update alias.
    PutAgentUpdateCurrent {
        /// Exact update idempotency authority.
        value: cymule_profile_protocol::agent::AgentUpdateCurrent,
    },
    /// Insert one immutable Agent message and append its Session order exactly once.
    PutAgentMessageCurrent {
        /// Exact message and order authority.
        value: cymule_profile_protocol::agent::AgentMessageCurrent,
    },
    /// Insert or replace one Agent tool current.
    PutAgentToolCurrent {
        /// Exact tool-call projection.
        value: cymule_profile_protocol::agent::AgentToolCurrent,
    },
    /// Apply one exact generation successor in the independent Agent target-claim family.
    ApplyAgentTargetClaim {
        /// Exact before/after claim transition.
        value: cymule_profile_protocol::agent::AgentTargetClaimTransition,
    },
    /// Insert or replace one Agent elicitation current.
    PutAgentElicitationCurrent {
        /// Exact elicitation projection.
        value: cymule_profile_protocol::agent::AgentElicitationCurrent,
    },
    /// Insert or replace one Agent occurrence and maintain the unresolved index.
    PutAgentOccurrenceCurrent {
        /// Exact occurrence projection.
        value: cymule_profile_protocol::agent::AgentOccurrenceCurrent,
    },
    /// Insert or replace one Agent stream and maintain the open-stream index.
    PutAgentStreamCurrent {
        /// Exact stream projection.
        value: cymule_profile_protocol::agent::AgentStreamCurrent,
    },
    /// Apply one exact external-publication reservation phase transition.
    ApplyAgentStreamPublicationTransition {
        /// Exact retained stream before the reservation mutation.
        source: cymule_profile_protocol::agent::AgentStreamCurrent,
        /// Exact legal reservation successor.
        current: cymule_profile_protocol::agent::AgentStreamCurrent,
    },
    /// Insert one immutable Agent stream chunk.
    PutAgentStreamChunkCurrent {
        /// Exact chunk projection.
        value: cymule_profile_protocol::agent::AgentStreamChunkCurrent,
    },
    /// Insert or replace one bounded Evolution scalar current.
    PutEvolutionCurrent {
        /// Exact partition current.
        value: cymule_profile_protocol::evolution::EvolutionCurrent,
    },
    /// Insert one immutable Evolution command alias.
    PutEvolutionCommandAlias {
        /// Exact command replay alias.
        value: cymule_profile_protocol::evolution::EvolutionCommandAlias,
    },
    /// Insert one immutable Evolution semantic receipt.
    PutEvolutionPersistenceReceipt {
        /// Exact semantic receipt.
        value: cymule_profile_protocol::evolution::EvolutionPersistenceReceipt,
    },
    /// Insert or replace one normalized Evolution state leaf.
    PutEvolutionMutation {
        /// Exact typed normalized leaf.
        value: cymule_profile_protocol::evolution::EvolutionMutation,
    },
    /// Insert or replace one bounded Virtual scheduler current.
    PutVirtualCurrent {
        /// Exact scheduler current.
        value: cymule_profile_protocol::virtual_work::VirtualCurrent,
    },
    /// Insert one immutable Virtual semantic receipt.
    PutVirtualPersistenceReceipt {
        /// Exact semantic receipt.
        value: Box<cymule_profile_protocol::virtual_work::VirtualPersistenceReceipt>,
    },
    /// Apply one exact before/after normalized Virtual leaf mutation.
    ApplyVirtualMutation {
        /// Exact typed leaf transition.
        value: cymule_profile_protocol::virtual_work::VirtualStateMutation,
    },
    /// Insert one immutable cross-profile Resource catalog record.
    PutResourceCatalogRecord {
        /// Exact catalog record.
        value: cymule_profile_protocol::resource::ResourceCatalogRecord,
    },
}

/// Ordered typed mutation set for one semantic transition.
#[derive(Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableDelta {
    /// Closed operations applied atomically in order.
    operations: Vec<DurableOperation>,
}

impl std::fmt::Debug for DurableDelta {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableDelta")
            .finish_non_exhaustive()
    }
}

impl DurableDelta {
    /// Construct a non-empty delta.
    pub(crate) fn new(operations: Vec<DurableOperation>) -> DurableResult<Self> {
        if operations.is_empty() {
            return Err(DurableError::Validation(
                "durable delta is empty".to_owned(),
            ));
        }
        Ok(Self { operations })
    }

    /// Borrow the admitted closed operation sequence inside the coordinator.
    pub(crate) fn operations(&self) -> &[DurableOperation] {
        &self.operations
    }
}

fn verify_prepared_state_root_delta_transition(
    current_head: &StoreHead,
    delta: &DurableDelta,
    transition: &crate::StateRootTransition,
) -> DurableResult<()> {
    transition.manifest.verify()?;
    let delta_digest = canonical_digest(delta)?;
    let machine_base_anchor = current_head.machine_base_anchor.clone();
    if transition.parent_manifest.as_deref() != Some(current_head.state_root_manifest_id.as_str())
        || transition.delta_digest.as_deref() != Some(delta_digest.as_str())
        || transition.manifest.sequence
            != current_head.sequence.checked_add(1).ok_or_else(|| {
                DurableError::Validation("state-root sequence overflowed".to_owned())
            })?
        || transition.manifest.machine_base_anchor != machine_base_anchor
    {
        return Err(DurableError::Integrity {
            code: "durable_state_root_transition_mismatch".to_owned(),
            message: "state-root transition does not bind the exact head, delta, sequence, and Machine anchor"
                .to_owned(),
        });
    }
    Ok(())
}

fn verify_prepared_pinned_state_root_transition(
    current_head: &StoreHead,
    stage_digest: &str,
    sidecar: Option<&DurableDelta>,
    transition: &crate::StateRootTransition,
) -> DurableResult<()> {
    if stage_digest.len() != 64
        || !stage_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(DurableError::Validation(
            "pinned Machine stage is not a canonical SHA-256 digest".to_owned(),
        ));
    }
    transition.manifest.verify()?;
    let sidecar_digest = sidecar.map(canonical_digest).transpose()?;
    let expected_delta = match sidecar_digest {
        Some(sidecar_digest) => canonical_digest(&(
            crate::state_root::PINNED_MACHINE_SIDECAR_TRANSITION_DOMAIN,
            stage_digest,
            sidecar_digest,
        ))?,
        None => stage_digest.to_owned(),
    };
    if transition.parent_manifest.as_deref() != Some(current_head.state_root_manifest_id.as_str())
        || transition.delta_digest.as_deref() != Some(expected_delta.as_str())
        || transition.manifest.sequence
            != current_head.sequence.checked_add(1).ok_or_else(|| {
                DurableError::Validation("state-root sequence overflowed".to_owned())
            })?
    {
        return Err(DurableError::Integrity {
            code: "durable_pinned_state_root_transition_mismatch".to_owned(),
            message: "pinned StateRoot transition does not bind its exact head, stage, sidecar, and sequence"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_machine_command_archive_batch(
    current: Option<&cymule_core::MachineBaseAnchor>,
    next: Option<&cymule_core::MachineBaseAnchor>,
    segments: &[cymule_core::MachineCommandArchiveSegment],
) -> DurableResult<()> {
    if current == next {
        if segments.is_empty() {
            return Ok(());
        }
        return Err(DurableError::Validation(
            "unchanged Machine base cannot publish command archive segments".to_owned(),
        ));
    }
    let next = next.ok_or_else(|| {
        DurableError::Validation("Machine command archive transition removed its anchor".to_owned())
    })?;
    if segments.is_empty() {
        return Err(DurableError::Validation(
            "Machine base advance requires its independent command archive segment".to_owned(),
        ));
    }
    let mut parent_segment = current.map(|anchor| anchor.archive_head.clone());
    let mut parent_count = current.map_or(0, |anchor| anchor.archive_count);
    let mut parent_event_count = current.map_or(0, |anchor| anchor.archive_event_count);
    let mut archive_batch_count = current.map_or(0, |anchor| anchor.archive_batch_count);
    let mut parent_admission_head = current.and_then(|anchor| anchor.admission_head.clone());
    let mut parent_command_index_root = current.map_or_else(
        cymule_core::MachineCommandIndexProof::empty_root,
        |anchor| Ok(anchor.command_index_root.clone()),
    )?;
    let mut identities = BTreeSet::new();
    for segment in segments {
        segment.verify()?;
        let header = &segment.header;
        if !identities.insert(&header.segment_id)
            || header.parent_segment != parent_segment
            || header.parent_count != parent_count
            || header.parent_event_count != parent_event_count
            || header.parent_admission_head != parent_admission_head
            || header.parent_command_index_root != parent_command_index_root
        {
            return Err(DurableError::Integrity {
                code: "machine_command_archive_batch_lineage_mismatch".to_owned(),
                message: format!(
                    "Machine command archive segment {} does not extend the exact Store head",
                    header.segment_id
                ),
            });
        }
        parent_segment = Some(header.segment_id.clone());
        parent_count = header.result_count;
        parent_event_count = header.result_event_count;
        archive_batch_count = archive_batch_count
            .checked_add(header.batch_count)
            .ok_or_else(|| {
                DurableError::Validation(
                    "Machine archive cumulative batch count overflowed".to_owned(),
                )
            })?;
        parent_admission_head.clone_from(&header.result_admission_head);
        parent_command_index_root.clone_from(&header.result_command_index_root);
    }
    if parent_segment.as_deref() != Some(next.archive_head.as_str())
        || parent_count != next.archive_count
        || parent_event_count != next.archive_event_count
        || archive_batch_count != next.archive_batch_count
        || parent_admission_head != next.admission_head
        || parent_command_index_root != next.command_index_root
    {
        return Err(DurableError::Integrity {
            code: "machine_command_archive_batch_anchor_mismatch".to_owned(),
            message: "Machine command archive batch does not close at the next base anchor"
                .to_owned(),
        });
    }
    Ok(())
}

fn derive_machine_command_archive_objects(
    segments: &[cymule_core::MachineCommandArchiveSegment],
) -> DurableResult<Vec<cymule_core::MachineCommandArchiveObject>> {
    let mut objects = BTreeMap::new();
    for segment in segments {
        for object in segment.persistence_objects()? {
            let identity = object.identity()?;
            match objects.get(&identity) {
                Some(existing) if existing != &object => {
                    return Err(DurableError::Integrity {
                        code: "machine_command_archive_object_identity_conflict".to_owned(),
                        message: format!(
                            "Machine command archive object {identity} has conflicting content"
                        ),
                    });
                }
                Some(_) => {}
                None => {
                    objects.insert(identity, object);
                }
            }
        }
    }
    Ok(objects.into_values().collect())
}

#[derive(Debug, Clone, PartialEq)]
/// Immutable objects and exact next head for one atomic commit.
pub struct StoreBatch {
    /// Transient semantic delta used only for validation and local cache
    /// advancement; absent only for initialization and never persisted as a
    /// second projection authority.
    delta: Option<DurableDelta>,
    /// Opaque Core/`StateRoot` stage digest for an exact-load pinned Machine
    /// successor. Present for pinned commands and paged progress, including
    /// stages with no M1 sidecar delta.
    pinned_machine_stage_digest: Option<String>,
    /// Initialization-only admitted materialization; never persisted as a
    /// competing projection authority.
    initial_state: Option<DurableState>,
    /// Exact persistent-root transition published by this batch.
    state_root_transition: crate::StateRootTransition,
    /// Independent immutable Core command-archive objects introduced by this
    /// transition.
    machine_command_archive_segments: Vec<cymule_core::MachineCommandArchiveSegment>,
    /// Complete immutable segment/entry/index-node object set introduced by
    /// the same compaction transition.
    machine_command_archive_objects: Vec<cymule_core::MachineCommandArchiveObject>,
    /// Exact next small head.
    head: StoreHead,
}

impl StoreBatch {
    /// Exact immutable persistent-root object batch.
    pub fn state_root_transition(&self) -> &crate::StateRootTransition {
        &self.state_root_transition
    }
    /// Independent immutable Machine command-archive objects introduced by
    /// this batch.
    pub fn machine_command_archive_segments(&self) -> &[cymule_core::MachineCommandArchiveSegment] {
        &self.machine_command_archive_segments
    }
    /// Complete immutable objects inserted into the shared command-archive
    /// namespace by this batch.
    pub fn machine_command_archive_objects(&self) -> &[cymule_core::MachineCommandArchiveObject] {
        &self.machine_command_archive_objects
    }
    /// Exact next small CAS head.
    pub fn head(&self) -> &StoreHead {
        &self.head
    }
    /// Materialize one initialized fault-test fixture before publication.
    /// Runtime transitions use the exact pinned `StateRoot` lowering instead.
    #[cfg(test)]
    pub(crate) fn project(&self, current: Option<&StoredState>) -> DurableResult<StoredState> {
        self.verify_against(current.map(|stored| &stored.head))?;
        let state = match current {
            None => self.initial_state.clone().ok_or_else(|| {
                DurableError::Validation(
                    "initialization requires its admitted transient state".to_owned(),
                )
            })?,
            Some(_) => return Err(DurableError::Validation(
                "transition projection is owned by the state-root manifest and live coordinator"
                    .to_owned(),
            )),
        };
        Ok(StoredState {
            revision: self.head.revision.clone(),
            state,
            state_root_manifest: self.state_root_transition.manifest.clone(),
            head: self.head.clone(),
        })
    }

    /// Verify that one successful Store receipt acknowledges this exact batch.
    pub(crate) fn verify_commit(&self, commit: &StoreCommit) -> DurableResult<()> {
        if commit.head != self.head || commit.revision != self.head.revision {
            return Err(DurableError::CommitOutcomeUnknown {
                message: "Store reported success without an exact batch receipt; reopen authority before retry"
                    .to_owned(),
            });
        }
        Ok(())
    }

    /// Build sequence-zero authority from coordinator-owned complete state.
    pub(crate) fn initialize_state(state: DurableState) -> DurableResult<Self> {
        Self::initialize_admitted(state)
    }

    /// Build sequence-zero authority for the coordinator-derived first Run.
    /// The caller is crate-private so Store adapters cannot bypass atomic
    /// Run/Continuation/claim admission.
    pub(crate) fn initialize_admitted(state: DurableState) -> DurableResult<Self> {
        let state_root_transition = crate::StateRootManifest::genesis(&state)?;
        state_root_transition.verify(None)?;
        let physical_token = derive_physical_token(
            None,
            &state_root_transition.manifest.manifest_id,
            0,
            0,
            &[],
            None,
        )?;
        let head = StoreHead {
            head_version: STORE_HEAD_VERSION.to_owned(),
            revision: state_root_transition.manifest.revision.clone(),
            state_root_manifest_id: state_root_transition.manifest.manifest_id.clone(),
            machine_base_anchor: state_root_transition.manifest.machine_base_anchor.clone(),
            sequence: 0,
            gc_sequence: 0,
            physical_token,
            gc_receipt: None,
        };
        head.verify()?;
        Ok(Self {
            delta: None,
            pinned_machine_stage_digest: None,
            initial_state: Some(state),
            state_root_transition,
            machine_command_archive_segments: Vec::new(),
            machine_command_archive_objects: Vec::new(),
            head,
        })
    }

    /// Assemble a transition from an already validated incremental Machine and
    /// sidecar transaction. This constructor reads only the small current head
    /// and delta metadata. A complete materialized state is required only at
    /// the explicit bounded checkpoint/compaction boundary.
    pub(crate) fn transition_prepared(
        current_revision: &str,
        current_head: &StoreHead,
        delta: DurableDelta,
        state_root_transition: crate::StateRootTransition,
        machine_command_archive_segments: Vec<cymule_core::MachineCommandArchiveSegment>,
    ) -> DurableResult<Self> {
        current_head.verify()?;
        if current_revision != current_head.revision {
            return Err(DurableError::Integrity {
                code: "durable_prepared_base_revision_mismatch".to_owned(),
                message: "prepared durable transition does not match its Store head".to_owned(),
            });
        }
        verify_prepared_state_root_delta_transition(current_head, &delta, &state_root_transition)?;
        let sequence = current_head.sequence.checked_add(1).ok_or_else(|| {
            DurableError::Validation("durable head sequence overflowed".to_owned())
        })?;
        let machine_base_anchor = current_head.machine_base_anchor.clone();
        validate_machine_command_archive_batch(
            current_head.machine_base_anchor.as_ref(),
            machine_base_anchor.as_ref(),
            &machine_command_archive_segments,
        )?;
        let machine_command_archive_objects =
            derive_machine_command_archive_objects(&machine_command_archive_segments)?;
        let archive_ids = machine_command_archive_objects
            .iter()
            .map(cymule_core::MachineCommandArchiveObject::identity)
            .collect::<cymule_core::Result<Vec<_>>>()?;
        let physical_token = derive_physical_token(
            Some(&current_head.physical_token),
            &state_root_transition.manifest.manifest_id,
            sequence,
            current_head.gc_sequence,
            &archive_ids,
            None,
        )?;
        let head = StoreHead {
            head_version: STORE_HEAD_VERSION.to_owned(),
            revision: state_root_transition.manifest.revision.clone(),
            state_root_manifest_id: state_root_transition.manifest.manifest_id.clone(),
            machine_base_anchor,
            sequence,
            gc_sequence: current_head.gc_sequence,
            physical_token,
            gc_receipt: None,
        };
        head.verify()?;
        Ok(Self {
            delta: Some(delta),
            pinned_machine_stage_digest: None,
            initial_state: None,
            state_root_transition,
            machine_command_archive_segments,
            machine_command_archive_objects,
            head,
        })
    }

    /// Assemble one exact-load pinned Machine stage, optionally coupled to a
    /// closed M1/profile sidecar delta, without materializing `DurableState`.
    pub(crate) fn transition_pinned(
        current_revision: &str,
        current_head: &StoreHead,
        stage_digest: String,
        sidecar: Option<DurableDelta>,
        state_root_transition: crate::StateRootTransition,
        machine_command_archive_segments: Vec<cymule_core::MachineCommandArchiveSegment>,
    ) -> DurableResult<Self> {
        current_head.verify()?;
        if current_revision != current_head.revision {
            return Err(DurableError::Integrity {
                code: "durable_pinned_base_revision_mismatch".to_owned(),
                message: "pinned Machine stage does not match its Store head".to_owned(),
            });
        }
        verify_prepared_pinned_state_root_transition(
            current_head,
            &stage_digest,
            sidecar.as_ref(),
            &state_root_transition,
        )?;
        let sequence = current_head.sequence.checked_add(1).ok_or_else(|| {
            DurableError::Validation("durable head sequence overflowed".to_owned())
        })?;
        let machine_base_anchor = state_root_transition.manifest.machine_base_anchor.clone();
        validate_machine_command_archive_batch(
            current_head.machine_base_anchor.as_ref(),
            machine_base_anchor.as_ref(),
            &machine_command_archive_segments,
        )?;
        let machine_command_archive_objects =
            derive_machine_command_archive_objects(&machine_command_archive_segments)?;
        let archive_ids = machine_command_archive_objects
            .iter()
            .map(cymule_core::MachineCommandArchiveObject::identity)
            .collect::<cymule_core::Result<Vec<_>>>()?;
        let physical_token = derive_physical_token(
            Some(&current_head.physical_token),
            &state_root_transition.manifest.manifest_id,
            sequence,
            current_head.gc_sequence,
            &archive_ids,
            None,
        )?;
        let head = StoreHead {
            head_version: STORE_HEAD_VERSION.to_owned(),
            revision: state_root_transition.manifest.revision.clone(),
            state_root_manifest_id: state_root_transition.manifest.manifest_id.clone(),
            machine_base_anchor,
            sequence,
            gc_sequence: current_head.gc_sequence,
            physical_token,
            gc_receipt: None,
        };
        head.verify()?;
        Ok(Self {
            delta: sidecar,
            pinned_machine_stage_digest: Some(stage_digest),
            initial_state: None,
            state_root_transition,
            machine_command_archive_segments,
            machine_command_archive_objects,
            head,
        })
    }

    /// Verify atomic contents against the exact current projection.
    ///
    /// # Errors
    /// Returns an error when the batch, transition, immutable archive objects,
    /// or physical successor do not agree with the supplied current head.
    pub fn verify_against(&self, current: Option<&StoreHead>) -> DurableResult<()> {
        self.head.verify()?;
        if self.head.gc_receipt.is_some() {
            return Err(DurableError::Validation(
                "normal StoreBatch cannot publish a GC receipt".to_owned(),
            ));
        }
        match current {
            None => self.verify_initial_authority(),
            Some(current) => self.verify_successor_authority(current),
        }
    }

    fn verify_initial_authority(&self) -> DurableResult<()> {
        if self.delta.is_some()
            || self.pinned_machine_stage_digest.is_some()
            || self.initial_state.is_none()
            || !self.machine_command_archive_segments.is_empty()
            || !self.machine_command_archive_objects.is_empty()
            || self.head.sequence != 0
            || self.head.gc_sequence != 0
        {
            return Err(DurableError::Validation(
                "initialization must contain only sequence-zero state-root authority".to_owned(),
            ));
        }
        self.state_root_transition.verify(None)?;
        if self.state_root_transition.manifest.manifest_id != self.head.state_root_manifest_id
            || self.state_root_transition.manifest.revision != self.head.revision
            || self.state_root_transition.manifest.machine_base_anchor
                != self.head.machine_base_anchor
            || self.head.physical_token
                != derive_physical_token(None, &self.head.state_root_manifest_id, 0, 0, &[], None)?
        {
            return Err(DurableError::Validation(
                "initial StateRoot does not match durable head".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_successor_authority(&self, current: &StoreHead) -> DurableResult<()> {
        if self.head.sequence
            != current.sequence.checked_add(1).ok_or_else(|| {
                DurableError::Validation("durable head sequence overflowed".to_owned())
            })?
        {
            return Err(DurableError::Validation(
                "next durable head sequence does not extend current head".to_owned(),
            ));
        }
        match self.pinned_machine_stage_digest.as_deref() {
            Some(stage_digest) => verify_prepared_pinned_state_root_transition(
                current,
                stage_digest,
                self.delta.as_ref(),
                &self.state_root_transition,
            )?,
            None => verify_prepared_state_root_delta_transition(
                current,
                self.delta.as_ref().ok_or_else(|| {
                    DurableError::Validation("transition requires its transient delta".to_owned())
                })?,
                &self.state_root_transition,
            )?,
        }
        if self.state_root_transition.manifest.manifest_id != self.head.state_root_manifest_id
            || self.state_root_transition.manifest.revision != self.head.revision
        {
            return Err(DurableError::Integrity {
                code: "durable_head_state_root_mismatch".to_owned(),
                message: "next durable head does not match its exact state-root manifest"
                    .to_owned(),
            });
        }
        self.verify_successor_archive(current)?;
        self.verify_successor_physical_token(current)
    }

    fn verify_successor_archive(&self, current: &StoreHead) -> DurableResult<()> {
        let expected_anchor = if self.pinned_machine_stage_digest.is_some() {
            self.state_root_transition
                .manifest
                .machine_base_anchor
                .clone()
        } else {
            current.machine_base_anchor.clone()
        };
        if self.head.machine_base_anchor != expected_anchor {
            return Err(DurableError::Integrity {
                code: "durable_head_machine_anchor_mismatch".to_owned(),
                message: "next durable head does not retain the exact Machine base anchor"
                    .to_owned(),
            });
        }
        validate_machine_command_archive_batch(
            current.machine_base_anchor.as_ref(),
            self.head.machine_base_anchor.as_ref(),
            &self.machine_command_archive_segments,
        )?;
        let expected_objects =
            derive_machine_command_archive_objects(&self.machine_command_archive_segments)?;
        if self.machine_command_archive_objects != expected_objects {
            return Err(DurableError::Integrity {
                code: "machine_command_archive_derived_objects_mismatch".to_owned(),
                message: "Store batch command archive objects do not match its exact segments"
                    .to_owned(),
            });
        }
        Ok(())
    }

    fn verify_successor_physical_token(&self, current: &StoreHead) -> DurableResult<()> {
        if self.initial_state.is_some()
            || self.head.gc_sequence != current.gc_sequence
            || self.head.physical_token
                != derive_physical_token(
                    Some(&current.physical_token),
                    &self.head.state_root_manifest_id,
                    self.head.sequence,
                    self.head.gc_sequence,
                    &self
                        .machine_command_archive_objects
                        .iter()
                        .map(cymule_core::MachineCommandArchiveObject::identity)
                        .collect::<cymule_core::Result<Vec<_>>>()?,
                    None,
                )?
        {
            return Err(DurableError::Integrity {
                code: "durable_head_physical_token_mismatch".to_owned(),
                message: "next durable head physical token does not bind its exact object batch"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Content-addressed evidence for one cold-object reclamation.
pub struct GcReceipt {
    /// Versioned receipt shape.
    pub receipt_version: String,
    /// Content identity of the receipt.
    pub receipt_id: String,
    /// Semantic revision preserved by reclamation.
    pub revision: String,
    /// State-root manifest kept as semantic reopen authority.
    pub retained_state_root: String,
    /// Exact physical token before reclamation.
    pub parent_physical_token: String,
    /// Exact physical token after reclamation.
    pub result_physical_token: String,
    /// Monotonic physical-only GC sequence after reclamation.
    pub gc_sequence: u64,
    /// Semantic commit sequence preserved by reclamation.
    pub sequence: u64,
    /// Digest of sorted reclaimed object identities.
    pub reclaimed_digest: String,
    /// Exact sorted immutable object identities authorized for deletion.
    pub reclaimed_ids: BTreeSet<String>,
    /// Number of reclaimed objects.
    pub reclaimed_objects: u64,
    /// Exact candidate identities left for a later GC generation.
    pub remaining_objects: u64,
}

impl GcReceipt {
    /// Build a receipt from one provider-audited, lexicographically first
    /// bounded page and the exact number of candidates left in the same pinned
    /// inventory snapshot.
    ///
    /// # Errors
    /// Returns an error for an invalid source head, malformed object identities,
    /// an inconsistent page, or a size or exact-integer bound violation.
    pub fn new_bounded(
        head: &StoreHead,
        reclaimed: BTreeSet<String>,
        remaining_objects: u64,
    ) -> DurableResult<Self> {
        head.verify()?;
        if reclaimed.len() > MAX_GC_RECLAIMED_OBJECTS
            || (reclaimed.is_empty() && remaining_objects != 0)
            || remaining_objects > crate::MAX_EXACT_INTEGER
        {
            return Err(DurableError::Validation(format!(
                "GC receipt requires at most {MAX_GC_RECLAIMED_OBJECTS} reclaimed identities and a closed remaining count"
            )));
        }
        for object_id in &reclaimed {
            cymule_core::validate_content_id("GC reclaimed object", object_id)?;
        }
        let reclaimed_digest = canonical_digest(&reclaimed)?;
        let reclaimed_objects = u64::try_from(reclaimed.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if reclaimed_objects
            .checked_add(remaining_objects)
            .is_none_or(|total| total > crate::MAX_EXACT_INTEGER)
        {
            return Err(DurableError::Validation(
                "GC candidate count exceeds the exact integer range".to_owned(),
            ));
        }
        let gc_sequence = head
            .gc_sequence
            .checked_add(1)
            .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| DurableError::Validation("GC sequence overflowed".to_owned()))?;
        let receipt_id = content_id(
            GC_RECEIPT_VERSION,
            &(
                head.revision.as_str(),
                head.state_root_manifest_id.as_str(),
                head.physical_token.as_str(),
                head.sequence,
                gc_sequence,
                &reclaimed_digest,
                &reclaimed,
                reclaimed_objects,
                remaining_objects,
            ),
        )?;
        let result_physical_token = derive_physical_token(
            Some(&head.physical_token),
            &head.state_root_manifest_id,
            head.sequence,
            gc_sequence,
            &[],
            Some(&receipt_id),
        )?;
        let receipt = Self {
            receipt_version: GC_RECEIPT_VERSION.to_owned(),
            receipt_id,
            revision: head.revision.clone(),
            retained_state_root: head.state_root_manifest_id.clone(),
            parent_physical_token: head.physical_token.clone(),
            result_physical_token,
            gc_sequence,
            sequence: head.sequence,
            reclaimed_digest,
            reclaimed_ids: reclaimed,
            reclaimed_objects,
            remaining_objects,
        };
        receipt.verify_identity()?;
        Ok(receipt)
    }

    /// Verify receipt identity and its retained head boundary.
    ///
    /// # Errors
    /// Returns an error if the receipt is invalid or does not exactly bind the
    /// supplied retained head and physical generation.
    pub fn verify_for(&self, head: &StoreHead) -> DurableResult<()> {
        self.verify_identity()?;
        if self.receipt_version != GC_RECEIPT_VERSION
            || self.revision != head.revision
            || self.retained_state_root != head.state_root_manifest_id
            || self.result_physical_token != head.physical_token
            || self.gc_sequence != head.gc_sequence
            || self.sequence != head.sequence
            || head.gc_receipt.as_deref() != Some(self.receipt_id.as_str())
        {
            return Err(DurableError::Validation(
                "GC receipt does not match the retained durable head".to_owned(),
            ));
        }
        Ok(())
    }

    /// Derive the exact acknowledged physical successor without requiring it
    /// to remain the Store's current head after another writer advances.
    pub(crate) fn successor_head(&self, before: &StoreHead) -> DurableResult<StoreHead> {
        before.verify()?;
        let next_gc_sequence = before
            .gc_sequence
            .checked_add(1)
            .filter(|sequence| *sequence <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| DurableError::Validation("GC sequence overflowed".to_owned()))?;
        if self.parent_physical_token != before.physical_token
            || self.gc_sequence != next_gc_sequence
        {
            return Err(DurableError::Integrity {
                code: "gc_receipt_successor_mismatch".to_owned(),
                message: "GC acknowledgement does not extend the exact prior physical head"
                    .to_owned(),
            });
        }
        let mut next = before.clone();
        next.gc_sequence = self.gc_sequence;
        next.physical_token.clone_from(&self.result_physical_token);
        next.gc_receipt = Some(self.receipt_id.clone());
        next.verify()?;
        self.verify_for(&next)?;
        Ok(next)
    }

    /// Verify the receipt's standalone content identity and result token.
    ///
    /// # Errors
    /// Returns an error for malformed, oversized, or inconsistent receipt
    /// content, including an invalid derived physical token.
    pub fn verify_identity(&self) -> DurableResult<()> {
        if cymule_core::canonical_bytes(self)?.len() > MAX_GC_RECEIPT_BYTES {
            return Err(DurableError::Validation(format!(
                "GC receipt exceeds {MAX_GC_RECEIPT_BYTES} canonical bytes"
            )));
        }
        if self.receipt_version != GC_RECEIPT_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported GC receipt version {:?}",
                self.receipt_version
            )));
        }
        for (kind, identity) in [
            ("GC receipt", self.receipt_id.as_str()),
            ("GC semantic revision", self.revision.as_str()),
            ("GC retained StateRoot", self.retained_state_root.as_str()),
            (
                "GC parent physical token",
                self.parent_physical_token.as_str(),
            ),
            (
                "GC result physical token",
                self.result_physical_token.as_str(),
            ),
        ] {
            cymule_core::validate_content_id(kind, identity)?;
        }
        if self.sequence > crate::MAX_EXACT_INTEGER
            || self.gc_sequence == 0
            || self.gc_sequence > crate::MAX_EXACT_INTEGER
        {
            return Err(DurableError::Validation(
                "GC receipt sequence is outside the exact range".to_owned(),
            ));
        }
        if self.reclaimed_ids.len() > MAX_GC_RECLAIMED_OBJECTS {
            return Err(DurableError::Validation(format!(
                "GC receipt exceeds {MAX_GC_RECLAIMED_OBJECTS} reclaimed identities"
            )));
        }
        for object_id in &self.reclaimed_ids {
            cymule_core::validate_content_id("GC reclaimed object", object_id)?;
        }
        let expected = content_id(
            GC_RECEIPT_VERSION,
            &(
                self.revision.as_str(),
                self.retained_state_root.as_str(),
                self.parent_physical_token.as_str(),
                self.sequence,
                self.gc_sequence,
                self.reclaimed_digest.as_str(),
                &self.reclaimed_ids,
                self.reclaimed_objects,
                self.remaining_objects,
            ),
        )?;
        let expected_digest = canonical_digest(&self.reclaimed_ids)?;
        let expected_count = u64::try_from(self.reclaimed_ids.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let expected_token = derive_physical_token(
            Some(&self.parent_physical_token),
            &self.retained_state_root,
            self.sequence,
            self.gc_sequence,
            &[],
            Some(&self.receipt_id),
        )?;
        if self.receipt_id != expected
            || self.reclaimed_digest != expected_digest
            || self.reclaimed_objects != expected_count
            || self.remaining_objects > crate::MAX_EXACT_INTEGER
            || self
                .reclaimed_objects
                .checked_add(self.remaining_objects)
                .is_none_or(|total| total > crate::MAX_EXACT_INTEGER)
            || (self.reclaimed_ids.is_empty() && self.remaining_objects != 0)
            || self.result_physical_token != expected_token
        {
            return Err(DurableError::Validation(format!(
                "GC receipt identity {} does not match {expected}",
                self.receipt_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Provider-neutral physical counts and reopen cost.
pub struct StoreStats {
    /// Retained immutable state-root objects.
    pub state_root_objects: u64,
    /// Retained independent Machine command-archive segments.
    pub machine_command_archive_segments: u64,
    /// Retained independently addressable archived command entries.
    pub machine_command_archive_entries: u64,
    /// Retained complete atomic Machine command-batch records.
    pub machine_command_archive_batches: u64,
    /// Retained immutable sparse command-index nodes.
    pub machine_command_index_nodes: u64,
    /// Retained reclamation receipts.
    pub gc_receipts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Successful small-head CAS receipt.
pub struct StoreCommit {
    /// Newly committed semantic revision.
    pub revision: String,
    /// Newly committed physical head.
    pub head: StoreHead,
}

/// Provider-neutral `StateRoot` single-domain store.
pub trait DurableStore {
    /// Load and authenticate only the bounded mutable Store head.
    ///
    /// Ordinary reopen calls this method and then resolves the exact fixed
    /// manifest named by the head. Implementations must not load the potentially
    /// large head-pinned physical GC receipt or traverse `StateRoot` object
    /// families, Machine history, archive inventory, or application journals.
    ///
    /// # Errors
    /// Returns an error if storage cannot be read or the retained head is invalid.
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>>;
    /// Load the exact fixed manifest pinned by the current head.
    ///
    /// # Errors
    /// Returns an error for storage failure or a malformed or mismatched manifest.
    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> DurableResult<Option<crate::StateRootManifest>>;
    /// Execute one read or Durable-owned lowering against an exact pinned
    /// immutable-object snapshot.
    ///
    /// The callback is framework code. A Store supplies only the resolver and
    /// may neither construct nor reinterpret a [`crate::StateRootTransition`].
    ///
    /// # Errors
    /// Returns an error if the pin is stale, an exact object is unavailable or
    /// corrupt, or the framework callback rejects the resolved state.
    fn with_state_root_resolver<T>(
        &mut self,
        current: &crate::StateRootManifest,
        read: impl FnOnce(&mut dyn crate::StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T>;
    /// Explicitly materialize and authenticate the complete current projection
    /// for an offline audit.
    ///
    /// Runtime open, closed commands, and ordinary queries must never call
    /// this method. They operate from the bounded head/manifest pair and exact
    /// keyed proofs instead. Keeping the full traversal explicitly named makes
    /// accidental O(all-domain-history) reopen visible at every call site.
    ///
    /// # Errors
    /// Returns an error for unavailable or corrupt reachable authority, invalid
    /// reconstructed state, a stale pin, or a terminal receipt closure mismatch.
    fn load_full_audit(&mut self) -> DurableResult<Option<StoredState>> {
        let Some(head) = self.load_head()? else {
            return Ok(None);
        };
        let manifest = self
            .load_state_root_manifest(&head.state_root_manifest_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "store_head_manifest_missing".to_owned(),
                message: format!(
                    "Store head references missing StateRoot manifest {}",
                    head.state_root_manifest_id
                ),
            })?;
        let state = self.with_state_root_resolver(&manifest, |resolver| {
            // The M1 materialized view intentionally omits normalized profile
            // families. Explicit audit must still authenticate their complete
            // physical closure before accepting the current projection.
            crate::reachable_state_root_objects(&manifest, resolver).map_err(
                |error| match error {
                    DurableError::NotFound(message) => DurableError::Integrity {
                        code: "state_root_audit_reachable_object_missing".to_owned(),
                        message,
                    },
                    other => other,
                },
            )?;
            manifest.materialize(resolver)
        })?;
        let stored = StoredState {
            revision: manifest.revision.clone(),
            state,
            state_root_manifest: manifest,
            head,
        };
        stored.verify()?;
        audit_cold_machine_objects(self, &stored)?;
        audit_terminal_receipt_commands(self, &stored)?;
        Ok(Some(stored))
    }
    /// Read one exact rooted application-journal prefix in O(log N).
    ///
    /// # Errors
    /// Returns an error if the pin or prefix is invalid or its proof is unavailable.
    fn application_journal_prefix(
        &mut self,
        manifest: &crate::StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<crate::ApplicationJournalPrefix>;
    /// Resolve one all-ever journal-record manifest through the exact current
    /// `StateRoot` without materializing cumulative history.
    ///
    /// # Errors
    /// Returns an error if the pin is stale or retained membership evidence is
    /// unavailable, malformed, or inconsistent with the requested record.
    fn application_journal_record_manifest(
        &mut self,
        manifest: &crate::StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<crate::JournalRecordManifest>>;
    /// Resolve one cumulative journal prefix-replacement authority through the
    /// exact current `StateRoot`.
    ///
    /// # Errors
    /// Returns an error for a stale pin, storage failure, or invalid replacement
    /// authority or membership evidence.
    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &crate::StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<crate::ApplicationJournalPrefixReplacementAuthority>>;
    /// Resolve one complete coupled-checkpoint receipt through the exact
    /// current `StateRoot`, including its referenced record-manifest closure.
    ///
    /// # Errors
    /// Returns an error if the pin is stale or the receipt and its exact
    /// referenced authority cannot be authenticated.
    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &crate::StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<crate::CoupledCheckpointReceipt>>;
    /// Load one independent immutable Machine command-archive segment by its
    /// content identity. Normal state reopen never invokes this method.
    ///
    /// # Errors
    /// Returns an error for storage failure or invalid retained segment content.
    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>>;
    /// Load one archived command entry by its exact content identity.
    ///
    /// # Errors
    /// Returns an error for storage failure or invalid retained entry content.
    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>>;
    /// Load one complete archived atomic command batch by its stable batch ID.
    ///
    /// # Errors
    /// Returns an error for storage failure or inconsistent batch identity or content.
    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>>;
    /// Load one immutable archived-command sparse-map node by content identity.
    ///
    /// # Errors
    /// Returns an error for storage failure or invalid retained node content.
    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>>;
    /// Resolve one exact current-root archived-command membership or
    /// non-membership lookup without scanning the archive segment chain.
    ///
    /// # Errors
    /// Returns an error for a stale anchor or missing, malformed, or inconsistent
    /// membership evidence or archived command content.
    fn lookup_machine_command_archive(
        &mut self,
        anchor: &cymule_core::MachineBaseAnchor,
        command_id: &str,
    ) -> DurableResult<cymule_core::MachineCommandArchiveLookup> {
        anchor.verify()?;
        let mut store_error = None;
        let index_proof = cymule_core::resolve_machine_command_index_proof(
            &anchor.command_index_root,
            command_id,
            |node_id| match self.load_machine_command_index_node(node_id) {
                Ok(value) => Ok(value),
                Err(error) => {
                    store_error = Some(error);
                    Err(cymule_core::CoreError::NotFound(
                        "Machine command index Store lookup failed".to_owned(),
                    ))
                }
            },
        );
        if let Some(error) = store_error {
            return Err(error);
        }
        let index_proof = index_proof?;
        let lookup = match index_proof.value.as_ref() {
            None => cymule_core::MachineCommandArchiveLookup::NonMember { index_proof },
            Some(value) => {
                let entry = self
                    .load_machine_command_archive_entry(&value.archive_entry_digest)?
                    .ok_or_else(|| {
                        DurableError::NotFound(format!(
                            "Machine command archive entry {} does not exist",
                            value.archive_entry_digest
                        ))
                    })?;
                if entry.identity()? != value.archive_entry_digest {
                    return Err(DurableError::Integrity {
                        code: "machine_command_archive_entry_identity_mismatch".to_owned(),
                        message: format!(
                            "Machine command archive entry {} does not match its Store locator",
                            value.archive_entry_digest
                        ),
                    });
                }
                cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof,
                    entry: Box::new(entry),
                }
            }
        };
        Ok(lookup)
    }
    /// Atomically insert immutable objects and compare-and-swap the small head.
    /// A provider that may have published the requested head without returning
    /// its receipt must return [`DurableError::CommitOutcomeUnknown`]; callers
    /// reconcile by reopening authority and never retry the request blindly.
    /// Every successful receipt must echo the exact batch head and semantic
    /// revision; a stale or foreign success receipt is also an unknown commit
    /// outcome, never authority to continue execution.
    ///
    /// # Errors
    /// Returns a conflict for a changed head, an integrity or validation error
    /// for rejected contents, or an unknown-outcome error when publication may
    /// have succeeded without an exact acknowledged receipt.
    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit>;
    /// Reconcile the exact reclamation receipt pinned by the current head.
    ///
    /// This operation never publishes another head. The opaque request is
    /// created only by the coordinator from its verified current head. A Store
    /// loads that head's exact pinned receipt and idempotently completes only
    /// the deletion page authorized by it.
    ///
    /// # Errors
    /// Returns an error for a stale head, missing or invalid retained authority,
    /// or failure to complete the exact authorized deletion page.
    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<GcReceipt>;
    /// Explicitly publish the next bounded cold-reclamation generation.
    ///
    /// The opaque request is created only by the coordinator from its verified
    /// current head. Unlike reconciliation, this operation always selects a
    /// fresh inventory page (or clean-inventory fence), advances the physical
    /// head by one exact CAS, and returns the new head-pinned receipt. The
    /// current head-pinned receipt is mandatory and other receipt-family
    /// objects have bounded lexical priority over ordinary immutable-object
    /// candidates.
    ///
    /// # Errors
    /// Returns an error for stale authority, invalid or unavailable reachable
    /// objects, an inconsistent candidate inventory, or storage failure.
    fn advance_cold_reclamation(&mut self, request: &StoreReclamation) -> DurableResult<GcReceipt>;
    /// Return physical object counts and bounded reopen work.
    ///
    /// # Errors
    /// Returns an error if the Store cannot read a consistent physical inventory.
    fn stats(&self) -> DurableResult<StoreStats>;
}

fn audit_cold_machine_objects<S: DurableStore + ?Sized>(
    store: &mut S,
    stored: &StoredState,
) -> DurableResult<()> {
    let Some(anchor) = stored.head.machine_base_anchor.as_ref() else {
        return Ok(());
    };
    require_current_manifest(store, &stored.state_root_manifest)?;
    let segments = load_machine_command_archive(anchor, |identity| {
        store.load_machine_command_archive_segment(identity)
    })
    .map_err(cold_archive_audit_error)?;
    let mut entries = BTreeMap::new();
    let mut batch_ids = BTreeSet::new();
    for segment in segments {
        audit_cold_segment_objects(store, &segment, &mut entries, &mut batch_ids)?;
    }
    let indexed = reachable_machine_command_index_objects(&anchor.command_index_root, |identity| {
        let node = store.load_machine_command_index_node(identity)?;
        if let Some(cymule_core::MachineCommandIndexNode::Member {
            command_id, value, ..
        }) = &node
            && entries.get(command_id) != Some(value)
        {
            return Err(DurableError::Integrity {
                code: "cold_archive_audit_index_member_mismatch".to_owned(),
                message: "cumulative command index changed its exact command, admission, or entry"
                    .to_owned(),
            });
        }
        Ok(node)
    })
    .map_err(cold_archive_audit_error)?;
    let entry_ids = entries
        .values()
        .map(|entry| entry.archive_entry_digest.clone())
        .collect::<BTreeSet<_>>();
    if indexed.archive_entry_ids != entry_ids
        || entry_ids.len() != entries.len()
        || u64::try_from(entries.len()).ok() != Some(anchor.archive_count)
        || u64::try_from(batch_ids.len()).ok() != Some(anchor.archive_batch_count)
    {
        return Err(DurableError::Integrity {
            code: "cold_archive_audit_index_not_closed".to_owned(),
            message: "cold archive entries, batches, and cumulative index do not close at the pinned anchor"
                .to_owned(),
        });
    }
    require_current_manifest(store, &stored.state_root_manifest)
}

fn audit_cold_segment_objects<S: DurableStore + ?Sized>(
    store: &mut S,
    segment: &cymule_core::MachineCommandArchiveSegment,
    entries: &mut BTreeMap<String, cymule_core::MachineCommandIndexValue>,
    batch_ids: &mut BTreeSet<String>,
) -> DurableResult<()> {
    for declared in &segment.entries {
        let identity = declared.identity()?;
        let retained = require_cold_audit_object(
            store.load_machine_command_archive_entry(&identity),
            "command entry",
            &identity,
        )?;
        if retained != *declared {
            return Err(DurableError::Integrity {
                code: "cold_archive_audit_entry_mismatch".to_owned(),
                message: format!("independent command entry {identity} differs from its archive"),
            });
        }
        let command_id = declared.command.envelope.command_id.clone();
        let value = cymule_core::MachineCommandIndexValue {
            admission_id: declared.admission.admission_id.clone(),
            archive_entry_digest: identity,
        };
        if entries.insert(command_id, value).is_some() {
            return Err(DurableError::Integrity {
                code: "cold_archive_audit_command_repeated".to_owned(),
                message: "cold archive chain repeats a command identity".to_owned(),
            });
        }
    }
    for declared in &segment.batches {
        let retained = require_cold_audit_object(
            store.load_machine_command_archive_batch(&declared.batch_id),
            "command batch",
            &declared.batch_id,
        )?;
        if retained != *declared || !batch_ids.insert(declared.batch_id.clone()) {
            return Err(DurableError::Integrity {
                code: "cold_archive_audit_batch_mismatch".to_owned(),
                message: format!(
                    "independent batch {} differs from its archive or repeats a prior admission",
                    declared.batch_id
                ),
            });
        }
    }
    Ok(())
}

fn require_cold_audit_object<T>(
    read: DurableResult<Option<T>>,
    kind: &str,
    identity: &str,
) -> DurableResult<T> {
    read.map_err(cold_archive_audit_error)?
        .ok_or_else(|| DurableError::Integrity {
            code: "cold_archive_audit_object_missing".to_owned(),
            message: format!("pinned cold archive lost required {kind} {identity}"),
        })
}

fn cold_archive_audit_error(error: DurableError) -> DurableError {
    match error {
        DurableError::NotFound(message) => DurableError::Integrity {
            code: "cold_archive_audit_object_missing".to_owned(),
            message,
        },
        other => other,
    }
}

fn audit_terminal_receipt_commands<S: DurableStore + ?Sized>(
    store: &mut S,
    stored: &StoredState,
) -> DurableResult<()> {
    for receipt in stored.state.cancellation_receipts.values() {
        let (entry, batch) = load_pinned_machine_command(
            store,
            &stored.state_root_manifest,
            &receipt.command.cancellation_id,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "terminal_receipt_command_missing".to_owned(),
            message: "cancellation receipt has no exact retained Core command".to_owned(),
        })?;
        crate::model::validate_cancellation_receipt_command(receipt, &entry, &batch)?;
    }
    for receipt in stored.state.effect_resolution_receipts.values() {
        let (entry, batch) = load_pinned_machine_command(
            store,
            &stored.state_root_manifest,
            &receipt.command.resolution_id,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "terminal_receipt_command_missing".to_owned(),
            message: "Effect resolution receipt has no exact retained Core command".to_owned(),
        })?;
        crate::model::validate_effect_resolution_receipt_command(receipt, &entry, &batch)?;
    }
    Ok(())
}

/// Resolve one command and its batch by exact hot or archived authority.
/// No Event scan or complete Machine/Store materialization is permitted here.
pub(crate) fn load_pinned_machine_command<S: DurableStore + ?Sized>(
    store: &mut S,
    manifest: &crate::StateRootManifest,
    command_id: &str,
) -> DurableResult<
    Option<(
        cymule_core::MachineCommandArchiveEntry,
        cymule_core::MachineCommandBatchRecord,
    )>,
> {
    cymule_core::validate_identity("pinned Machine command", command_id)?;
    manifest.verify()?;
    let hot = store.with_state_root_resolver(manifest, |resolver| {
        crate::state_root::load_hot_machine_command_entry(manifest, resolver, command_id)
    })?;
    let result = if let Some(command) = hot {
        Some(command)
    } else if let Some(anchor) = manifest.machine_base_anchor() {
        load_archived_machine_command(store, anchor, command_id)?
    } else {
        None
    };
    if let Some((entry, batch)) = &result {
        batch.verify_entry(entry)?;
        if entry.command.envelope.command_id != command_id {
            return Err(DurableError::Integrity {
                code: "pinned_machine_command_identity_mismatch".to_owned(),
                message: "pinned Machine lookup returned another command".to_owned(),
            });
        }
    }
    require_current_manifest(store, manifest)?;
    Ok(result)
}

fn load_archived_machine_command<S: DurableStore + ?Sized>(
    store: &mut S,
    anchor: &cymule_core::MachineBaseAnchor,
    command_id: &str,
) -> DurableResult<
    Option<(
        cymule_core::MachineCommandArchiveEntry,
        cymule_core::MachineCommandBatchRecord,
    )>,
> {
    match store.lookup_machine_command_archive(anchor, command_id)? {
        cymule_core::MachineCommandArchiveLookup::NonMember { index_proof } => {
            index_proof.verify(&anchor.command_index_root)?;
            if index_proof.command_id != command_id || index_proof.value.is_some() {
                return Err(DurableError::Integrity {
                    code: "pinned_machine_command_absence_mismatch".to_owned(),
                    message: "archived command absence changed its exact key or membership"
                        .to_owned(),
                });
            }
            Ok(None)
        }
        cymule_core::MachineCommandArchiveLookup::Member { index_proof, entry } => {
            index_proof.verify(&anchor.command_index_root)?;
            entry.verify()?;
            let entry_id = entry.identity()?;
            if index_proof.command_id != command_id
                || entry.command.envelope.command_id != command_id
                || !index_proof.value.as_ref().is_some_and(|value| {
                    value.admission_id == entry.admission.admission_id
                        && value.archive_entry_digest == entry_id
                })
            {
                return Err(DurableError::Integrity {
                    code: "pinned_machine_command_archive_mismatch".to_owned(),
                    message: "archived command changed its exact current-root membership"
                        .to_owned(),
                });
            }
            let batch = store
                .load_machine_command_archive_batch(&entry.command.batch_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "pinned_machine_command_batch_missing".to_owned(),
                    message: "archived command has no retained immutable batch".to_owned(),
                })?;
            batch.verify_entry(&entry)?;
            Ok(Some((*entry, batch)))
        }
    }
}

fn require_current_manifest<S: DurableStore + ?Sized>(
    store: &mut S,
    manifest: &crate::StateRootManifest,
) -> DurableResult<()> {
    let head = store.load_head()?;
    if head.as_ref().is_none_or(|head| {
        head.state_root_manifest_id != manifest.manifest_id()
            || head.revision != manifest.revision()
    }) {
        return Err(DurableError::Conflict {
            expected: Some(manifest.revision().to_owned()),
            current: head.map(|head| head.revision),
        });
    }
    let observed = store
        .load_state_root_manifest(manifest.manifest_id())?
        .ok_or_else(|| DurableError::Integrity {
            code: "pinned_machine_command_manifest_missing".to_owned(),
            message: "command readback lost its exact pinned manifest".to_owned(),
        })?;
    if observed != *manifest {
        return Err(DurableError::Integrity {
            code: "pinned_machine_command_manifest_mismatch".to_owned(),
            message: "command readback changed its exact pinned manifest".to_owned(),
        });
    }
    Ok(())
}

impl<T: DurableStore + ?Sized> DurableStore for &mut T {
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
        (**self).load_head()
    }

    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> DurableResult<Option<crate::StateRootManifest>> {
        (**self).load_state_root_manifest(manifest_id)
    }

    fn with_state_root_resolver<U>(
        &mut self,
        current: &crate::StateRootManifest,
        read: impl FnOnce(&mut dyn crate::StateRootResolver) -> DurableResult<U>,
    ) -> DurableResult<U> {
        (**self).with_state_root_resolver(current, read)
    }

    fn load_full_audit(&mut self) -> DurableResult<Option<StoredState>> {
        (**self).load_full_audit()
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &crate::StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<crate::ApplicationJournalPrefix> {
        (**self).application_journal_prefix(manifest, journal_id, count)
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &crate::StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<crate::JournalRecordManifest>> {
        (**self).application_journal_record_manifest(manifest, journal_id, record_id)
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &crate::StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<crate::ApplicationJournalPrefixReplacementAuthority>> {
        (**self).application_journal_prefix_replacement_authority(manifest, replacement_id)
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &crate::StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<crate::CoupledCheckpointReceipt>> {
        (**self).coupled_checkpoint_receipt(manifest, coupling_id)
    }

    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
        (**self).load_machine_command_archive_segment(segment_id)
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
        (**self).load_machine_command_archive_entry(entry_id)
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
        (**self).load_machine_command_archive_batch(batch_id)
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
        (**self).load_machine_command_index_node(node_id)
    }

    fn lookup_machine_command_archive(
        &mut self,
        anchor: &cymule_core::MachineBaseAnchor,
        command_id: &str,
    ) -> DurableResult<cymule_core::MachineCommandArchiveLookup> {
        (**self).lookup_machine_command_archive(anchor, command_id)
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        (**self).compare_and_commit(expected, batch)
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<GcReceipt> {
        (**self).reconcile_cold_reclamation(request)
    }

    fn advance_cold_reclamation(&mut self, request: &StoreReclamation) -> DurableResult<GcReceipt> {
        (**self).advance_cold_reclamation(request)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        (**self).stats()
    }
}

#[derive(Debug, Clone, Default)]
struct MemoryDomain {
    head: Option<StoreHead>,
    state_root_objects: BTreeMap<String, crate::StateRootObject>,
    machine_command_archive_segments: BTreeMap<String, cymule_core::MachineCommandArchiveSegment>,
    machine_command_archive_entries: BTreeMap<String, cymule_core::MachineCommandArchiveEntry>,
    machine_command_archive_batches: BTreeMap<String, cymule_core::MachineCommandBatchRecord>,
    machine_command_archive_batch_index: BTreeMap<String, String>,
    machine_command_index_nodes: BTreeMap<String, cymule_core::MachineCommandIndexNode>,
    receipts: BTreeMap<String, GcReceipt>,
}

struct MemoryStateRootResolver<'a> {
    manifest_id: String,
    objects: &'a BTreeMap<String, crate::StateRootObject>,
}

impl crate::StateRootResolver for MemoryStateRootResolver<'_> {
    fn pinned_manifest_id(&self) -> &str {
        &self.manifest_id
    }

    fn load_state_root_object(
        &mut self,
        object_id: &str,
    ) -> DurableResult<Option<crate::StateRootObject>> {
        Ok(self.objects.get(object_id).cloned())
    }
}

fn memory_gc_receipt_for_head<'a>(
    domain: &'a MemoryDomain,
    head: &StoreHead,
) -> DurableResult<Option<&'a GcReceipt>> {
    head.verify()?;
    let Some(receipt_id) = head.gc_receipt.as_deref() else {
        return Ok(None);
    };
    let receipt = domain
        .receipts
        .get(receipt_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "memory_gc_receipt_missing".to_owned(),
            message: format!("head-pinned GC receipt {receipt_id} does not exist"),
        })?;
    receipt
        .verify_for(head)
        .map_err(|error| DurableError::Integrity {
            code: "memory_gc_receipt_head_mismatch".to_owned(),
            message: format!("head-pinned GC receipt is invalid: {error}"),
        })?;
    Ok(Some(receipt))
}

fn require_memory_manifest_matches_head(
    manifest: &crate::StateRootManifest,
    head: &StoreHead,
) -> DurableResult<()> {
    if manifest.manifest_id != head.state_root_manifest_id
        || manifest.revision != head.revision
        || manifest.sequence != head.sequence
        || manifest.machine_base_anchor != head.machine_base_anchor
    {
        return Err(DurableError::Integrity {
            code: "memory_state_root_head_mismatch".to_owned(),
            message: "memory Store head does not match its exact StateRoot manifest".to_owned(),
        });
    }
    Ok(())
}

fn current_memory_manifest<'a>(
    domain: &'a MemoryDomain,
    requested: &crate::StateRootManifest,
) -> DurableResult<&'a crate::StateRootManifest> {
    requested.verify()?;
    let head = domain.head.as_ref().ok_or_else(|| DurableError::Conflict {
        expected: Some(requested.manifest_id.clone()),
        current: None,
    })?;
    if head.state_root_manifest_id != requested.manifest_id || head.revision != requested.revision {
        return Err(DurableError::Conflict {
            expected: Some(requested.manifest_id.clone()),
            current: Some(head.state_root_manifest_id.clone()),
        });
    }
    let physical = match domain.state_root_objects.get(&requested.manifest_id) {
        Some(crate::StateRootObject::Manifest(manifest)) => manifest,
        Some(_) => {
            return Err(DurableError::Integrity {
                code: "memory_state_root_manifest_kind_mismatch".to_owned(),
                message: format!(
                    "state-root manifest locator {} resolves to another object kind",
                    requested.manifest_id
                ),
            });
        }
        None => {
            return Err(DurableError::NotFound(format!(
                "state-root manifest {} does not exist",
                requested.manifest_id
            )));
        }
    };
    require_memory_manifest_matches_head(physical, head)?;
    if physical != requested {
        return Err(DurableError::Integrity {
            code: "memory_state_root_manifest_snapshot_mismatch".to_owned(),
            message: "requested StateRoot manifest does not equal current physical authority"
                .to_owned(),
        });
    }
    Ok(physical)
}

fn insert_memory_namespace<'a>(
    identities: &mut BTreeMap<&'a str, &'static str>,
    identity: &'a str,
    family: &'static str,
) -> DurableResult<()> {
    if let Some(previous) = identities.insert(identity, family) {
        return Err(DurableError::Integrity {
            code: "memory_physical_identity_alias".to_owned(),
            message: format!(
                "physical identity {identity} aliases {previous} and {family} families"
            ),
        });
    }
    Ok(())
}

fn verify_memory_physical_namespaces(domain: &MemoryDomain) -> DurableResult<()> {
    let mut identities = BTreeMap::<&str, &'static str>::new();
    for (identity, object) in &domain.state_root_objects {
        object.verify()?;
        if identity != object.object_id() {
            return Err(DurableError::Integrity {
                code: "memory_state_root_object_locator".to_owned(),
                message: format!(
                    "state-root object locator {identity} does not match {}",
                    object.object_id()
                ),
            });
        }
        insert_memory_namespace(&mut identities, identity, "state_root")?;
    }
    for (identity, segment) in &domain.machine_command_archive_segments {
        segment.verify()?;
        if identity != &segment.header.segment_id {
            return Err(DurableError::Integrity {
                code: "memory_archive_segment_locator".to_owned(),
                message: format!(
                    "archive segment locator {identity} does not match {}",
                    segment.header.segment_id
                ),
            });
        }
        insert_memory_namespace(&mut identities, identity, "archive_segment")?;
    }
    for (identity, entry) in &domain.machine_command_archive_entries {
        let actual = entry.identity()?;
        if identity != &actual {
            return Err(DurableError::Integrity {
                code: "memory_archive_entry_locator".to_owned(),
                message: format!("archive entry locator {identity} does not match {actual}"),
            });
        }
        insert_memory_namespace(&mut identities, identity, "archive_entry")?;
    }
    for (identity, batch) in &domain.machine_command_archive_batches {
        batch.verify()?;
        if identity != &batch.batch_receipt_id
            || domain
                .machine_command_archive_batch_index
                .get(&batch.batch_id)
                != Some(identity)
        {
            return Err(DurableError::Integrity {
                code: "memory_archive_batch_locator".to_owned(),
                message: format!(
                    "archive batch {} does not match its receipt locator or stable index",
                    batch.batch_id
                ),
            });
        }
        insert_memory_namespace(&mut identities, identity, "archive_batch")?;
    }
    if domain.machine_command_archive_batch_index.len()
        != domain.machine_command_archive_batches.len()
    {
        return Err(DurableError::Integrity {
            code: "memory_archive_batch_index_cardinality".to_owned(),
            message: "archive batch index does not exactly cover retained batch records".to_owned(),
        });
    }
    for (identity, node) in &domain.machine_command_index_nodes {
        let actual = node.identity()?;
        if identity != actual {
            return Err(DurableError::Integrity {
                code: "memory_command_index_locator".to_owned(),
                message: format!("command-index locator {identity} does not match {actual}"),
            });
        }
        insert_memory_namespace(&mut identities, identity, "command_index")?;
    }
    for (identity, receipt) in &domain.receipts {
        receipt.verify_identity()?;
        if identity != &receipt.receipt_id {
            return Err(DurableError::Integrity {
                code: "memory_gc_receipt_locator".to_owned(),
                message: format!(
                    "GC receipt locator {identity} does not match {}",
                    receipt.receipt_id
                ),
            });
        }
        insert_memory_namespace(&mut identities, identity, "gc_receipt")?;
    }
    Ok(())
}

fn memory_archive_reachable_ids(
    domain: &MemoryDomain,
    anchor: &cymule_core::MachineBaseAnchor,
) -> DurableResult<BTreeSet<String>> {
    let segments = load_machine_command_archive(anchor, |id| {
        Ok(domain.machine_command_archive_segments.get(id).cloned())
    })?;
    let mut reachable = BTreeSet::new();
    for segment in segments {
        reachable.insert(segment.header.segment_id);
        for declared in &segment.batches {
            let receipt_id = domain
                .machine_command_archive_batch_index
                .get(&declared.batch_id)
                .ok_or_else(|| DurableError::Integrity {
                    code: "memory_archive_reachable_batch_missing".to_owned(),
                    message: format!(
                        "reachable archive batch {} has no stable index",
                        declared.batch_id
                    ),
                })?;
            let retained = domain
                .machine_command_archive_batches
                .get(receipt_id)
                .ok_or_else(|| DurableError::Integrity {
                    code: "memory_archive_reachable_batch_missing".to_owned(),
                    message: format!(
                        "reachable archive batch {} lost its independent receipt {receipt_id}",
                        declared.batch_id
                    ),
                })?;
            // The archive loader authenticated the complete declared batch.
            // Its embedded value is evidence, never a missing-object fallback.
            if receipt_id != &declared.batch_receipt_id || retained != declared {
                return Err(DurableError::Integrity {
                    code: "memory_archive_segment_batch_mismatch".to_owned(),
                    message: format!(
                        "independent batch {} differs from its reachable archive segment",
                        declared.batch_id
                    ),
                });
            }
            reachable.insert(receipt_id.clone());
        }
    }
    Ok(reachable)
}

fn memory_semantic_reachable_ids(
    domain: &MemoryDomain,
    head: &StoreHead,
) -> DurableResult<BTreeSet<String>> {
    let manifest = match domain.state_root_objects.get(&head.state_root_manifest_id) {
        Some(crate::StateRootObject::Manifest(manifest)) => manifest,
        Some(_) => {
            return Err(DurableError::Integrity {
                code: "memory_state_root_manifest_kind_mismatch".to_owned(),
                message: "state-root manifest locator resolves to another object kind".to_owned(),
            });
        }
        None => {
            return Err(DurableError::NotFound(format!(
                "state-root manifest {} does not exist",
                head.state_root_manifest_id
            )));
        }
    };
    require_memory_manifest_matches_head(manifest, head)?;
    let mut root_resolver = MemoryStateRootResolver {
        manifest_id: manifest.manifest_id.clone(),
        objects: &domain.state_root_objects,
    };
    let mut reachable = crate::reachable_state_root_objects(manifest, &mut root_resolver)?;
    let reachable_archives = head
        .machine_base_anchor
        .as_ref()
        .map(|anchor| memory_archive_reachable_ids(domain, anchor))
        .transpose()?
        .unwrap_or_default();
    let reachable_index = head
        .machine_base_anchor
        .as_ref()
        .map(|anchor| {
            reachable_machine_command_index_objects(&anchor.command_index_root, |id| {
                Ok(domain.machine_command_index_nodes.get(id).cloned())
            })
        })
        .transpose()?
        .unwrap_or_default();
    for entry_id in &reachable_index.archive_entry_ids {
        let entry = domain
            .machine_command_archive_entries
            .get(entry_id)
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "Machine command archive entry {entry_id} does not exist"
                ))
            })?;
        if entry.identity()? != *entry_id {
            return Err(DurableError::Integrity {
                code: "machine_command_archive_entry_identity_mismatch".to_owned(),
                message: format!(
                    "Machine command archive entry {entry_id} does not match its Store locator"
                ),
            });
        }
        let batch_receipt_id = domain
            .machine_command_archive_batch_index
            .get(&entry.command.batch_id)
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "Machine command batch {} does not exist",
                    entry.command.batch_id
                ))
            })?;
        let batch = domain
            .machine_command_archive_batches
            .get(batch_receipt_id)
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "Machine command batch receipt {batch_receipt_id} does not exist"
                ))
            })?;
        batch.verify()?;
        if batch.batch_id != entry.command.batch_id || batch.batch_receipt_id != *batch_receipt_id {
            return Err(DurableError::Integrity {
                code: "machine_command_archive_batch_identity_mismatch".to_owned(),
                message: format!(
                    "Machine command batch {} does not match its Store locator",
                    entry.command.batch_id
                ),
            });
        }
        reachable.insert(batch_receipt_id.clone());
    }
    reachable.extend(reachable_archives);
    reachable.extend(reachable_index.node_ids);
    reachable.extend(reachable_index.archive_entry_ids);
    Ok(reachable)
}

fn verify_memory_reclamation_page(
    domain: &MemoryDomain,
    head: &StoreHead,
    receipt: &GcReceipt,
) -> DurableResult<()> {
    receipt.verify_for(head)?;
    verify_memory_physical_namespaces(domain)?;
    let reachable = memory_semantic_reachable_ids(domain, head)?;
    if receipt
        .reclaimed_ids
        .iter()
        .any(|identity| reachable.contains(identity))
        || head
            .gc_receipt
            .as_ref()
            .is_some_and(|identity| receipt.reclaimed_ids.contains(identity))
    {
        return Err(DurableError::Integrity {
            code: "memory_gc_reachable_object".to_owned(),
            message: "GC receipt authorizes deletion of current reachable authority".to_owned(),
        });
    }
    Ok(())
}

fn delete_memory_reclamation_page(domain: &mut MemoryDomain, receipt: &GcReceipt) {
    let reclaimed = &receipt.reclaimed_ids;
    domain
        .state_root_objects
        .retain(|id, _| !reclaimed.contains(id));
    domain
        .machine_command_archive_segments
        .retain(|id, _| !reclaimed.contains(id));
    domain
        .machine_command_archive_entries
        .retain(|id, _| !reclaimed.contains(id));
    domain
        .machine_command_archive_batches
        .retain(|id, _| !reclaimed.contains(id));
    domain
        .machine_command_archive_batch_index
        .retain(|_, receipt_id| {
            domain
                .machine_command_archive_batches
                .contains_key(receipt_id)
        });
    domain
        .machine_command_index_nodes
        .retain(|id, _| !reclaimed.contains(id));
    domain.receipts.retain(|id, _| !reclaimed.contains(id));
}

#[derive(Debug, Clone, Default)]
/// In-memory `StateRoot` reference store for conformance and fault injection.
pub struct MemoryStore {
    current: Arc<Mutex<MemoryDomain>>,
}

impl MemoryStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn remove_agent_command_value_for_test(
        &mut self,
        command_id: &str,
    ) -> DurableResult<()> {
        let mut domain = lock_memory(&self.current, None)?;
        let object_id = domain
            .state_root_objects
            .iter()
            .find_map(|(object_id, object)| match object {
                crate::StateRootObject::Value(value)
                    if matches!(
                        &value.value,
                        crate::StateRootValue::Leaf {
                            kind: crate::StateRootLeafKind::AgentCommand,
                            canonical_json,
                        } if cymule_core::decode_json::<cymule_profile_protocol::agent::AgentCommand>(
                            canonical_json.as_bytes(),
                        )
                        .is_ok_and(|command| command.command_id == command_id)
                    ) =>
                {
                    Some(object_id.clone())
                }
                _ => None,
            })
            .ok_or_else(|| DurableError::NotFound(format!("Agent command {command_id} is missing")))?;
        domain.state_root_objects.remove(&object_id);
        Ok(())
    }
}

impl DurableStore for MemoryStore {
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
        let domain = lock_memory(&self.current, None)?;
        let Some(head) = domain.head.clone() else {
            return Ok(None);
        };
        head.verify()?;
        Ok(Some(head))
    }

    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> DurableResult<Option<crate::StateRootManifest>> {
        let domain = lock_memory(&self.current, None)?;
        let Some(head) = domain.head.as_ref() else {
            return Ok(None);
        };
        if head.state_root_manifest_id != manifest_id {
            return Ok(None);
        }
        match domain.state_root_objects.get(manifest_id) {
            Some(crate::StateRootObject::Manifest(manifest)) => {
                require_memory_manifest_matches_head(manifest, head)?;
                Ok(Some(manifest.clone()))
            }
            Some(_) => Err(DurableError::Integrity {
                code: "memory_state_root_manifest_kind_mismatch".to_owned(),
                message: format!(
                    "state-root manifest locator {manifest_id} resolves to another object kind"
                ),
            }),
            None => Err(DurableError::Integrity {
                code: "memory_state_root_manifest_missing".to_owned(),
                message: format!("head-pinned state-root manifest {manifest_id} does not exist"),
            }),
        }
    }

    fn with_state_root_resolver<T>(
        &mut self,
        current: &crate::StateRootManifest,
        read: impl FnOnce(&mut dyn crate::StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T> {
        let domain = lock_memory(&self.current, None)?;
        let physical = current_memory_manifest(&domain, current)?;
        let mut resolver = MemoryStateRootResolver {
            manifest_id: physical.manifest_id.clone(),
            objects: &domain.state_root_objects,
        };
        read(&mut resolver)
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &crate::StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<crate::ApplicationJournalPrefix> {
        let domain = lock_memory(&self.current, None)?;
        let physical = current_memory_manifest(&domain, manifest)?;
        let mut resolver = MemoryStateRootResolver {
            manifest_id: physical.manifest_id.clone(),
            objects: &domain.state_root_objects,
        };
        crate::state_root::load_application_journal_prefix(
            physical,
            &mut resolver,
            journal_id,
            count,
        )
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &crate::StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<crate::JournalRecordManifest>> {
        let domain = lock_memory(&self.current, None)?;
        let physical = current_memory_manifest(&domain, manifest)?;
        let mut resolver = MemoryStateRootResolver {
            manifest_id: physical.manifest_id.clone(),
            objects: &domain.state_root_objects,
        };
        crate::state_root::load_application_journal_record_manifest(
            physical,
            &mut resolver,
            journal_id,
            record_id,
        )
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &crate::StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<crate::ApplicationJournalPrefixReplacementAuthority>> {
        let domain = lock_memory(&self.current, None)?;
        let physical = current_memory_manifest(&domain, manifest)?;
        let mut resolver = MemoryStateRootResolver {
            manifest_id: physical.manifest_id.clone(),
            objects: &domain.state_root_objects,
        };
        crate::state_root::load_application_journal_prefix_replacement_authority(
            physical,
            &mut resolver,
            replacement_id,
        )
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &crate::StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<crate::CoupledCheckpointReceipt>> {
        let domain = lock_memory(&self.current, None)?;
        let physical = current_memory_manifest(&domain, manifest)?;
        let mut resolver = MemoryStateRootResolver {
            manifest_id: physical.manifest_id.clone(),
            objects: &domain.state_root_objects,
        };
        crate::state_root::load_coupled_checkpoint_receipt(physical, &mut resolver, coupling_id)
    }

    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
        let domain = lock_memory(&self.current, None)?;
        Ok(domain
            .machine_command_archive_segments
            .get(segment_id)
            .cloned())
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
        cymule_core::validate_content_id("Machine command batch", batch_id)?;
        let domain = lock_memory(&self.current, None)?;
        let Some(receipt_id) = domain.machine_command_archive_batch_index.get(batch_id) else {
            return Ok(None);
        };
        let batch = domain
            .machine_command_archive_batches
            .get(receipt_id)
            .cloned()
            .ok_or_else(|| DurableError::Integrity {
                code: "memory_archive_batch_index_dangling".to_owned(),
                message: format!(
                    "Machine command batch {batch_id} points to missing receipt {receipt_id}"
                ),
            })?;
        batch.verify()?;
        if batch.batch_id != batch_id || batch.batch_receipt_id != *receipt_id {
            return Err(DurableError::Integrity {
                code: "memory_archive_batch_identity_mismatch".to_owned(),
                message: format!(
                    "Machine command batch {batch_id} changed its stable or receipt identity"
                ),
            });
        }
        Ok(Some(batch))
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
        let domain = lock_memory(&self.current, None)?;
        Ok(domain
            .machine_command_archive_entries
            .get(entry_id)
            .cloned())
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
        let domain = lock_memory(&self.current, None)?;
        Ok(domain.machine_command_index_nodes.get(node_id).cloned())
    }

    fn lookup_machine_command_archive(
        &mut self,
        anchor: &cymule_core::MachineBaseAnchor,
        command_id: &str,
    ) -> DurableResult<cymule_core::MachineCommandArchiveLookup> {
        anchor.verify()?;
        let domain = lock_memory(&self.current, None)?;
        let head = domain.head.as_ref().ok_or_else(|| DurableError::Conflict {
            expected: Some(anchor.anchor_id.clone()),
            current: None,
        })?;
        if head.machine_base_anchor.as_ref() != Some(anchor) {
            return Err(DurableError::Conflict {
                expected: Some(anchor.anchor_id.clone()),
                current: head
                    .machine_base_anchor
                    .as_ref()
                    .map(|current| current.anchor_id.clone()),
            });
        }
        let manifest = match domain.state_root_objects.get(&head.state_root_manifest_id) {
            Some(crate::StateRootObject::Manifest(manifest)) => manifest,
            Some(_) => {
                return Err(DurableError::Integrity {
                    code: "memory_state_root_manifest_kind_mismatch".to_owned(),
                    message: "state-root manifest locator resolves to another object kind"
                        .to_owned(),
                });
            }
            None => {
                return Err(DurableError::Integrity {
                    code: "memory_state_root_manifest_missing".to_owned(),
                    message: format!(
                        "head-pinned state-root manifest {} does not exist",
                        head.state_root_manifest_id
                    ),
                });
            }
        };
        require_memory_manifest_matches_head(manifest, head)?;
        let index_proof = cymule_core::resolve_machine_command_index_proof(
            &anchor.command_index_root,
            command_id,
            |node_id| Ok(domain.machine_command_index_nodes.get(node_id).cloned()),
        )?;
        match index_proof.value.as_ref() {
            None => Ok(cymule_core::MachineCommandArchiveLookup::NonMember { index_proof }),
            Some(value) => {
                let entry = domain
                    .machine_command_archive_entries
                    .get(&value.archive_entry_digest)
                    .cloned()
                    .ok_or_else(|| {
                        DurableError::NotFound(format!(
                            "Machine command archive entry {} does not exist",
                            value.archive_entry_digest
                        ))
                    })?;
                if entry.identity()? != value.archive_entry_digest {
                    return Err(DurableError::Integrity {
                        code: "machine_command_archive_entry_identity_mismatch".to_owned(),
                        message: format!(
                            "Machine command archive entry {} does not match its Store locator",
                            value.archive_entry_digest
                        ),
                    });
                }
                Ok(cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof,
                    entry: Box::new(entry),
                })
            }
        }
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let mut domain = lock_memory(&self.current, expected)?;
        if expected != domain.head.as_ref() {
            return Err(conflict(expected, domain.head.as_ref()));
        }
        batch.verify_against(domain.head.as_ref())?;
        let parent_manifest = expected
            .map(|head| {
                domain
                    .state_root_objects
                    .get(&head.state_root_manifest_id)
                    .ok_or_else(|| {
                        DurableError::NotFound(format!(
                            "state-root manifest {} does not exist",
                            head.state_root_manifest_id
                        ))
                    })
                    .and_then(|object| match object {
                        crate::StateRootObject::Manifest(manifest) => Ok(manifest),
                        _ => Err(DurableError::Integrity {
                            code: "memory_state_root_manifest_kind_mismatch".to_owned(),
                            message: "state-root manifest locator resolves to another object kind"
                                .to_owned(),
                        }),
                    })
            })
            .transpose()?;
        batch.state_root_transition.verify(parent_manifest)?;
        for object in &batch.state_root_transition.objects {
            if let Some(existing) = domain.state_root_objects.get(object.object_id())
                && existing != object
            {
                return Err(DurableError::Integrity {
                    code: "memory_state_root_object_identity_conflict".to_owned(),
                    message: format!(
                        "state-root object {} has conflicting immutable bytes",
                        object.object_id()
                    ),
                });
            }
        }
        verify_memory_archive_objects(&domain, &batch.machine_command_archive_objects)?;
        for object in &batch.state_root_transition.objects {
            domain
                .state_root_objects
                .entry(object.object_id().to_owned())
                .or_insert_with(|| object.clone());
        }
        insert_verified_memory_archive_objects(
            &mut domain,
            &batch.machine_command_archive_objects,
        )?;
        domain.head = Some(batch.head.clone());
        Ok(StoreCommit {
            revision: batch.head.revision.clone(),
            head: batch.head.clone(),
        })
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<GcReceipt> {
        let expected = request.expected_head();
        let mut domain = lock_memory(&self.current, Some(expected))?;
        if domain.head.as_ref() != Some(expected) {
            return Err(conflict(Some(expected), domain.head.as_ref()));
        }
        let receipt = memory_gc_receipt_for_head(&domain, expected)?
            .ok_or_else(|| {
                DurableError::Validation(
                    "cold-reclamation reconciliation requires a head-pinned receipt".to_owned(),
                )
            })?
            .clone();
        verify_memory_reclamation_page(&domain, expected, &receipt)?;
        delete_memory_reclamation_page(&mut domain, &receipt);
        if !domain.receipts.contains_key(&receipt.receipt_id) {
            return Err(DurableError::Integrity {
                code: "memory_gc_current_receipt_deleted".to_owned(),
                message: "GC reconciliation deleted its current head-pinned receipt".to_owned(),
            });
        }
        Ok(receipt)
    }

    fn advance_cold_reclamation(&mut self, request: &StoreReclamation) -> DurableResult<GcReceipt> {
        let expected = request.expected_head();
        let mut domain = lock_memory(&self.current, Some(expected))?;
        if domain.head.as_ref() != Some(expected) {
            return Err(conflict(Some(expected), domain.head.as_ref()));
        }
        expected.verify()?;
        if let Some(receipt) = memory_gc_receipt_for_head(&domain, expected)?.cloned() {
            verify_memory_reclamation_page(&domain, expected, &receipt)?;
            delete_memory_reclamation_page(&mut domain, &receipt);
        }
        verify_memory_physical_namespaces(&domain)?;
        if expected
            .gc_receipt
            .as_ref()
            .is_some_and(|receipt_id| !domain.receipts.contains_key(receipt_id))
        {
            return Err(DurableError::Integrity {
                code: "memory_gc_receipt_inventory".to_owned(),
                message: "memory Store is missing its head-pinned predecessor GC receipt"
                    .to_owned(),
            });
        }
        let reachable = memory_semantic_reachable_ids(&domain, expected)?;
        let candidates = memory_reclamation_candidates(&domain, &reachable);
        let candidate_count = u64::try_from(candidates.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if candidate_count > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "GC candidate count exceeds the exact integer range".to_owned(),
            ));
        }
        let mut reclaimed = BTreeSet::new();
        if let Some(receipt_id) = expected.gc_receipt.as_ref() {
            if !candidates.contains(receipt_id) {
                return Err(DurableError::Integrity {
                    code: "memory_gc_receipt_inventory".to_owned(),
                    message: "head-pinned GC receipt is absent from reclamation candidates"
                        .to_owned(),
                });
            }
            reclaimed.insert(receipt_id.clone());
        }
        for identity in domain.receipts.keys() {
            if reclaimed.len() == MAX_GC_RECLAIMED_OBJECTS {
                break;
            }
            reclaimed.insert(identity.clone());
        }
        for identity in &candidates {
            if reclaimed.len() == MAX_GC_RECLAIMED_OBJECTS {
                break;
            }
            reclaimed.insert(identity.clone());
        }
        let remaining_objects = candidate_count
            .checked_sub(
                u64::try_from(reclaimed.len())
                    .map_err(|error| DurableError::Validation(error.to_string()))?,
            )
            .ok_or_else(|| DurableError::Integrity {
                code: "memory_gc_candidate_count".to_owned(),
                message: "GC reclaimed page exceeds its exact candidate inventory".to_owned(),
            })?;
        let receipt = GcReceipt::new_bounded(expected, reclaimed, remaining_objects)?;
        delete_memory_reclamation_page(&mut domain, &receipt);
        if receipt.remaining_objects == 0 && !domain.receipts.is_empty() {
            return Err(DurableError::Integrity {
                code: "memory_gc_receipt_inventory".to_owned(),
                message: "terminal GC generation did not reclaim every predecessor receipt"
                    .to_owned(),
            });
        }
        let head = receipt.successor_head(expected)?;
        domain
            .receipts
            .insert(receipt.receipt_id.clone(), receipt.clone());
        domain.head = Some(head);
        Ok(receipt)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        let domain = lock_memory(&self.current, None)?;
        Ok(StoreStats {
            state_root_objects: domain.state_root_objects.len() as u64,
            machine_command_archive_segments: domain.machine_command_archive_segments.len() as u64,
            machine_command_archive_entries: domain.machine_command_archive_entries.len() as u64,
            machine_command_archive_batches: domain.machine_command_archive_batches.len() as u64,
            machine_command_index_nodes: domain.machine_command_index_nodes.len() as u64,
            gc_receipts: domain.receipts.len() as u64,
        })
    }
}

fn verify_memory_archive_objects(
    domain: &MemoryDomain,
    objects: &[cymule_core::MachineCommandArchiveObject],
) -> DurableResult<()> {
    for object in objects {
        match object {
            cymule_core::MachineCommandArchiveObject::Segment(segment) => {
                verify_immutable(
                    &domain.machine_command_archive_segments,
                    Some(segment),
                    |value| &value.header.segment_id,
                )?;
            }
            cymule_core::MachineCommandArchiveObject::Entry(entry) => {
                let identity = entry.identity()?;
                match domain.machine_command_archive_entries.get(&identity) {
                    Some(existing) if existing != entry.as_ref() => {
                        return Err(DurableError::Integrity {
                            code: "machine_command_archive_entry_identity_conflict".to_owned(),
                            message: format!(
                                "Machine command archive entry {identity} has conflicting content"
                            ),
                        });
                    }
                    Some(_) | None => {}
                }
            }
            cymule_core::MachineCommandArchiveObject::Batch(batch) => {
                batch.verify()?;
                let identity = batch.batch_receipt_id.clone();
                match domain.machine_command_archive_batches.get(&identity) {
                    Some(existing) if existing != batch.as_ref() => {
                        return Err(DurableError::Integrity {
                            code: "machine_command_archive_batch_identity_conflict".to_owned(),
                            message: format!(
                                "Machine command batch {} has conflicting content",
                                batch.batch_id
                            ),
                        });
                    }
                    Some(_) | None => {}
                }
                if domain
                    .machine_command_archive_batch_index
                    .get(&batch.batch_id)
                    .is_some_and(|existing| existing != &identity)
                {
                    return Err(DurableError::Integrity {
                        code: "machine_command_archive_batch_index_conflict".to_owned(),
                        message: format!(
                            "Machine command batch {} has another receipt identity",
                            batch.batch_id
                        ),
                    });
                }
            }
            cymule_core::MachineCommandArchiveObject::CommandIndexNode(node) => {
                let identity = node.identity()?.to_owned();
                match domain.machine_command_index_nodes.get(&identity) {
                    Some(existing) if existing != node => {
                        return Err(DurableError::Integrity {
                            code: "machine_command_index_node_identity_conflict".to_owned(),
                            message: format!(
                                "Machine command index node {identity} has conflicting content"
                            ),
                        });
                    }
                    Some(_) | None => {}
                }
            }
        }
    }
    Ok(())
}

fn insert_verified_memory_archive_objects(
    domain: &mut MemoryDomain,
    objects: &[cymule_core::MachineCommandArchiveObject],
) -> DurableResult<()> {
    for object in objects {
        match object {
            cymule_core::MachineCommandArchiveObject::Segment(segment) => {
                domain
                    .machine_command_archive_segments
                    .entry(segment.header.segment_id.clone())
                    .or_insert_with(|| segment.as_ref().clone());
            }
            cymule_core::MachineCommandArchiveObject::Entry(entry) => {
                let identity = entry.identity()?;
                domain
                    .machine_command_archive_entries
                    .entry(identity)
                    .or_insert_with(|| entry.as_ref().clone());
            }
            cymule_core::MachineCommandArchiveObject::Batch(batch) => {
                let identity = batch.batch_receipt_id.clone();
                domain
                    .machine_command_archive_batch_index
                    .entry(batch.batch_id.clone())
                    .or_insert_with(|| identity.clone());
                domain
                    .machine_command_archive_batches
                    .entry(identity)
                    .or_insert_with(|| batch.as_ref().clone());
            }
            cymule_core::MachineCommandArchiveObject::CommandIndexNode(node) => {
                let identity = node.identity()?.to_owned();
                domain
                    .machine_command_index_nodes
                    .entry(identity)
                    .or_insert_with(|| node.clone());
            }
        }
    }
    Ok(())
}

fn memory_reclamation_candidates(
    domain: &MemoryDomain,
    reachable: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut candidates = domain
        .state_root_objects
        .keys()
        .filter(|identity| !reachable.contains(*identity))
        .cloned()
        .collect::<BTreeSet<_>>();
    candidates.extend(
        domain
            .machine_command_archive_segments
            .keys()
            .filter(|identity| !reachable.contains(*identity))
            .cloned(),
    );
    candidates.extend(
        domain
            .machine_command_archive_entries
            .keys()
            .filter(|identity| !reachable.contains(*identity))
            .cloned(),
    );
    candidates.extend(
        domain
            .machine_command_archive_batches
            .keys()
            .filter(|identity| !reachable.contains(*identity))
            .cloned(),
    );
    candidates.extend(
        domain
            .machine_command_index_nodes
            .keys()
            .filter(|identity| !reachable.contains(*identity))
            .cloned(),
    );
    candidates.extend(domain.receipts.keys().cloned());
    candidates
}

fn lock_memory<'a>(
    memory: &'a Mutex<MemoryDomain>,
    expected: Option<&StoreHead>,
) -> DurableResult<std::sync::MutexGuard<'a, MemoryDomain>> {
    match memory.try_lock() {
        Ok(value) => Ok(value),
        Err(TryLockError::WouldBlock) => Err(DurableError::Conflict {
            expected: expected.map(|head| head.revision.clone()),
            current: Some("memory-store-writer-active".to_owned()),
        }),
        Err(TryLockError::Poisoned(error)) => Err(DurableError::Substrate {
            code: "memory_store_lock_poisoned".to_owned(),
            message: error.to_string(),
        }),
    }
}

fn verify_immutable<T: PartialEq>(
    values: &BTreeMap<String, T>,
    value: Option<&T>,
    identity: impl Fn(&T) -> &String,
) -> DurableResult<()> {
    let Some(value) = value else { return Ok(()) };
    let id = identity(value);
    match values.get(id) {
        Some(existing) if existing != value => Err(DurableError::Validation(format!(
            "immutable durable object {id} has conflicting bytes"
        ))),
        Some(_) | None => Ok(()),
    }
}

fn conflict(expected: Option<&StoreHead>, current: Option<&StoreHead>) -> DurableError {
    DurableError::Conflict {
        expected: expected.map(|head| head.revision.clone()),
        current: current.map(|head| head.revision.clone()),
    }
}

/// Load and verify the complete independent Machine command-archive chain for
/// one exact Store-pinned base anchor. Normal state reopen does not call this
/// function.
///
/// # Errors
/// Returns an error for invalid anchors or missing, corrupt, or inconsistent
/// archive segments and their authenticated command closure.
pub fn load_machine_command_archive(
    anchor: &cymule_core::MachineBaseAnchor,
    mut segment: impl FnMut(&str) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>>,
) -> DurableResult<Vec<cymule_core::MachineCommandArchiveSegment>> {
    anchor.verify()?;
    let mut reverse = Vec::new();
    let mut cursor = Some(anchor.archive_head.clone());
    let mut expected_count = anchor.archive_count;
    let mut expected_event_count = anchor.archive_event_count;
    let mut expected_batch_count = anchor.archive_batch_count;
    let mut expected_admission_head = anchor.admission_head.clone();
    let mut expected_command_index_root = anchor.command_index_root.clone();
    while let Some(segment_id) = cursor.take() {
        let value = segment(&segment_id)?.ok_or_else(|| {
            DurableError::NotFound(format!(
                "Machine command archive segment {segment_id} does not exist"
            ))
        })?;
        value.verify()?;
        let header = &value.header;
        if header.segment_id != segment_id
            || header.result_count != expected_count
            || header.result_event_count != expected_event_count
            || expected_admission_head != header.result_admission_head
            || header.result_command_index_root != expected_command_index_root
        {
            return Err(DurableError::Integrity {
                code: "machine_command_archive_chain_mismatch".to_owned(),
                message: format!(
                    "Machine command archive segment {segment_id} does not match its descendant or Store anchor"
                ),
            });
        }
        expected_count = header.parent_count;
        expected_event_count = header.parent_event_count;
        expected_batch_count = expected_batch_count
            .checked_sub(header.batch_count)
            .ok_or_else(|| DurableError::Integrity {
                code: "machine_command_archive_batch_count_underflow".to_owned(),
                message: "Machine command archive exceeds the pinned cumulative batch count"
                    .to_owned(),
            })?;
        expected_admission_head.clone_from(&header.parent_admission_head);
        expected_command_index_root.clone_from(&header.parent_command_index_root);
        cursor.clone_from(&header.parent_segment);
        reverse.push(value);
    }
    if expected_count != 0
        || expected_event_count != 0
        || expected_batch_count != 0
        || expected_admission_head.is_some()
        || expected_command_index_root != cymule_core::MachineCommandIndexProof::empty_root()?
    {
        return Err(DurableError::Integrity {
            code: "machine_command_archive_genesis_mismatch".to_owned(),
            message: "Machine command archive chain does not terminate at genesis".to_owned(),
        });
    }
    reverse.reverse();
    Ok(reverse)
}

/// Return every Store object identity reachable from one exact Machine command
/// archive head. Explicit GC uses this set; normal reopen does not traverse it.
///
/// # Errors
/// Returns an error if any reachable archive segment or its closure is unavailable
/// or cannot be authenticated against the requested head.
pub fn reachable_machine_command_archive_ids(
    anchor: &cymule_core::MachineBaseAnchor,
    segment: impl FnMut(&str) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>>,
) -> DurableResult<BTreeSet<String>> {
    Ok(load_machine_command_archive(anchor, segment)?
        .into_iter()
        .map(|segment| segment.header.segment_id)
        .collect())
}

/// Immutable Store objects reachable from one exact archived-command sparse
/// map root. Empty subtrees are implicit and never require physical objects.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MachineCommandIndexReachability {
    /// Content identities of every reachable branch/member node.
    pub node_ids: BTreeSet<String>,
    /// Content identities of every archived command entry referenced by a
    /// reachable member leaf.
    pub archive_entry_ids: BTreeSet<String>,
}

/// Traverse and authenticate the sparse command-index objects reachable from
/// one current root. This is the explicit GC path; normal command lookup reads
/// at most one node per fixed tree level.
///
/// # Errors
/// Returns an error for an invalid root or a missing, malformed, or inconsistent
/// reachable sparse-index node.
pub fn reachable_machine_command_index_objects(
    root: &str,
    mut node: impl FnMut(&str) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>>,
) -> DurableResult<MachineCommandIndexReachability> {
    let mut reachable = MachineCommandIndexReachability::default();
    let mut visited = BTreeSet::new();
    let mut pending = vec![(0_usize, root.to_owned())];
    while let Some((depth, node_id)) = pending.pop() {
        let encoded_depth =
            u16::try_from(depth).map_err(|error| DurableError::Validation(error.to_string()))?;
        if node_id == cymule_core::MachineCommandIndexProof::empty_hash(encoded_depth)? {
            continue;
        }
        if !visited.insert((depth, node_id.clone())) {
            continue;
        }
        let value = node(&node_id)?.ok_or_else(|| {
            DurableError::NotFound(format!(
                "Machine command index node {node_id} does not exist"
            ))
        })?;
        if value.identity()? != node_id {
            return Err(DurableError::Integrity {
                code: "machine_command_index_node_identity_mismatch".to_owned(),
                message: format!(
                    "Machine command index node {node_id} does not match its Store locator"
                ),
            });
        }
        reachable.node_ids.insert(node_id);
        match value {
            cymule_core::MachineCommandIndexNode::Branch {
                depth: node_depth,
                left,
                right,
                ..
            } if usize::from(node_depth) == depth && depth < 256 => {
                let child_depth = depth.checked_add(1).ok_or_else(|| {
                    DurableError::Validation(
                        "Machine command index traversal depth overflowed".to_owned(),
                    )
                })?;
                pending.push((child_depth, left));
                pending.push((child_depth, right));
            }
            cymule_core::MachineCommandIndexNode::Member { value, .. } if depth == 256 => {
                reachable
                    .archive_entry_ids
                    .insert(value.archive_entry_digest);
            }
            _ => {
                return Err(DurableError::Integrity {
                    code: "machine_command_index_node_depth_mismatch".to_owned(),
                    message: "Machine command index node kind does not match its exact tree depth"
                        .to_owned(),
                });
            }
        }
    }
    Ok(reachable)
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_core::Machine;

    fn material_archive_fixture() -> DurableResult<(
        cymule_core::MachineBaseAnchor,
        cymule_core::MachineCommandArchiveSegment,
    )> {
        use cymule_core::durable_internal::{
            MachineAuthorityFrontier, MachineMaterialAdmission, MachineMaterialParentReads,
            prepare_machine_material_admission,
        };
        let mut frontier = MachineAuthorityFrontier::genesis(
            cymule_authenticated_collections::MapRoot::empty(),
            cymule_authenticated_collections::MapRoot::empty(),
            cymule_authenticated_collections::MapRoot::empty(),
            cymule_authenticated_collections::MapRoot::empty(),
        )?;
        let mut snapshot = Machine::new().snapshot();
        for index in 0..2 {
            let bytes = format!("material-{index}").into_bytes();
            let artifact = cymule_core::ArtifactRecord {
                reference: cymule_core::artifact_ref("test.archive-material/1", &bytes)?,
                bytes,
            };
            let reads = MachineMaterialParentReads::new(
                BTreeMap::new(),
                BTreeMap::from([(artifact.reference.artifact_id.clone(), None)]),
            );
            let material = MachineMaterialAdmission::new(
                format!("material:batch-count:{index}"),
                Vec::new(),
                vec![artifact],
            )?;
            let prepared = prepare_machine_material_admission(&frontier, &material, &reads)?;
            snapshot
                .artifacts
                .extend(prepared.delta.artifacts.into_values());
            snapshot
                .batches
                .extend(prepared.delta.batches.into_values());
            frontier = prepared.frontier;
        }
        let mut machine = Machine::restore(snapshot)?;
        let compaction = machine.compact_event_free_admissions()?;
        let anchor = machine.base_anchor()?.ok_or_else(|| {
            DurableError::Validation("material archive fixture has no base anchor".to_owned())
        })?;
        assert!(compaction.archive_segment.entries.is_empty());
        assert_eq!(compaction.archive_segment.header.batch_count, 2);
        assert_eq!(anchor.archive_batch_count, 2);
        Ok((anchor, compaction.archive_segment))
    }

    fn reseal_archive_batch_count(
        anchor: &cymule_core::MachineBaseAnchor,
        batch_count: u64,
    ) -> DurableResult<cymule_core::MachineBaseAnchor> {
        let mut forged = anchor.clone();
        forged.archive_batch_count = batch_count;
        let mut preimage = serde_json::to_value(&forged)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        preimage
            .as_object_mut()
            .ok_or_else(|| DurableError::Validation("anchor must encode as an object".to_owned()))?
            .remove("anchor_id");
        forged.anchor_id = content_id(cymule_core::MachineBaseAnchor::VERSION, &preimage)?;
        forged.verify()?;
        Ok(forged)
    }

    #[test]
    fn archive_batch_count_binds_zero_command_material_segments() -> DurableResult<()> {
        let (anchor, segment) = material_archive_fixture()?;
        validate_machine_command_archive_batch(
            None,
            Some(&anchor),
            std::slice::from_ref(&segment),
        )?;
        for count in [1, 3] {
            let forged = reseal_archive_batch_count(&anchor, count)?;
            assert!(matches!(
                validate_machine_command_archive_batch(None, Some(&forged), std::slice::from_ref(&segment)),
                Err(DurableError::Integrity { code, .. })
                    if code == "machine_command_archive_batch_anchor_mismatch"
            ));
        }
        Ok(())
    }

    #[test]
    fn archived_batch_reader_and_gc_reject_resealed_count_mismatches() -> DurableResult<()> {
        let (anchor, segment) = material_archive_fixture()?;
        let lookup =
            |identity: &str| Ok((identity == segment.header.segment_id).then(|| segment.clone()));
        let loaded = load_machine_command_archive(&anchor, lookup)?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.first(), Some(&segment));
        for (count, expected_code) in [
            (1, "machine_command_archive_batch_count_underflow"),
            (3, "machine_command_archive_genesis_mismatch"),
        ] {
            let forged = reseal_archive_batch_count(&anchor, count)?;
            assert!(matches!(
                load_machine_command_archive(&forged, lookup),
                Err(DurableError::Integrity { code, .. }) if code == expected_code
            ));
            assert!(matches!(
                reachable_machine_command_archive_ids(&forged, lookup),
                Err(DurableError::Integrity { code, .. }) if code == expected_code
            ));
        }
        Ok(())
    }

    #[test]
    fn memory_archive_reachability_keeps_material_batches_and_rejects_missing_or_aliased_records()
    -> DurableResult<()> {
        let (anchor, segment) = material_archive_fixture()?;
        let mut domain = MemoryDomain::default();
        domain
            .machine_command_archive_segments
            .insert(segment.header.segment_id.clone(), segment.clone());
        for batch in &segment.batches {
            domain
                .machine_command_archive_batch_index
                .insert(batch.batch_id.clone(), batch.batch_receipt_id.clone());
            domain
                .machine_command_archive_batches
                .insert(batch.batch_receipt_id.clone(), batch.clone());
        }
        let reachable = memory_archive_reachable_ids(&domain, &anchor)?;
        assert!(reachable.contains(&segment.header.segment_id));
        assert!(
            segment
                .batches
                .iter()
                .all(|batch| reachable.contains(&batch.batch_receipt_id))
        );
        let [first, second] = segment.batches.as_slice() else {
            return Err(DurableError::Validation(
                "material archive fixture requires two complete batches".to_owned(),
            ));
        };
        let removed = domain
            .machine_command_archive_batches
            .remove(&first.batch_receipt_id)
            .ok_or_else(|| DurableError::Validation("fixture batch is missing".to_owned()))?;
        assert!(matches!(
            memory_archive_reachable_ids(&domain, &anchor),
            Err(DurableError::Integrity { code, .. })
                if code == "memory_archive_reachable_batch_missing"
        ));
        domain
            .machine_command_archive_batches
            .insert(first.batch_receipt_id.clone(), removed);
        domain
            .machine_command_archive_batch_index
            .insert(first.batch_id.clone(), second.batch_receipt_id.clone());
        assert!(matches!(
            memory_archive_reachable_ids(&domain, &anchor),
            Err(DurableError::Integrity { code, .. })
                if code == "memory_archive_segment_batch_mismatch"
        ));
        Ok(())
    }

    #[test]
    fn full_audit_rejects_missing_independent_cold_entry() -> DurableResult<()> {
        let (mut store, head, command_id) = compacted_command_store()?;
        let entry_id = {
            let domain = lock_memory(&store.current, None)?;
            domain
                .machine_command_archive_entries
                .iter()
                .find(|(_, entry)| entry.command.envelope.command_id == command_id)
                .map(|(identity, _)| identity.clone())
                .ok_or_else(|| DurableError::Validation("fixture entry is missing".to_owned()))?
        };
        assert!(
            lock_memory(&store.current, None)?
                .machine_command_archive_entries
                .remove(&entry_id)
                .is_some()
        );
        assert_cold_audit_rejects_without_commit(&mut store, &head)
    }

    #[test]
    fn full_audit_rejects_missing_independent_cold_batch() -> DurableResult<()> {
        let (mut store, head, _) = compacted_command_store()?;
        let receipt_id = lock_memory(&store.current, None)?
            .machine_command_archive_batches
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| DurableError::Validation("fixture batch is missing".to_owned()))?;
        assert!(
            lock_memory(&store.current, None)?
                .machine_command_archive_batches
                .remove(&receipt_id)
                .is_some()
        );
        assert_cold_audit_rejects_without_commit(&mut store, &head)
    }

    #[test]
    fn full_audit_rejects_missing_cold_command_index_node() -> DurableResult<()> {
        let (mut store, head, _) = compacted_command_store()?;
        let anchor = head.machine_base_anchor.as_ref().ok_or_else(|| {
            DurableError::Validation("fixture has no pinned archive anchor".to_owned())
        })?;
        assert!(
            lock_memory(&store.current, None)?
                .machine_command_index_nodes
                .remove(&anchor.command_index_root)
                .is_some()
        );
        assert_cold_audit_rejects_without_commit(&mut store, &head)
    }

    #[test]
    fn full_audit_rejects_foreign_independent_cold_entry() -> DurableResult<()> {
        let (mut store, head, command_id) = compacted_command_store()?;
        {
            let mut domain = lock_memory(&store.current, None)?;
            let identity = domain
                .machine_command_archive_entries
                .iter()
                .find(|(_, entry)| entry.command.envelope.command_id == command_id)
                .map(|(identity, _)| identity.clone())
                .ok_or_else(|| DurableError::Validation("fixture entry is missing".to_owned()))?;
            let foreign = domain
                .machine_command_archive_entries
                .values()
                .find(|entry| entry.command.envelope.command_id != command_id)
                .cloned()
                .ok_or_else(|| {
                    DurableError::Validation("fixture needs another entry".to_owned())
                })?;
            foreign.verify()?;
            domain
                .machine_command_archive_entries
                .insert(identity, foreign);
        }
        assert!(matches!(
            store.load_full_audit(),
            Err(DurableError::Integrity { code, .. }) if code == "cold_archive_audit_entry_mismatch"
        ));
        assert_eq!(store.load_head()?.as_ref(), Some(&head));
        Ok(())
    }

    #[test]
    fn cold_audit_preserves_provider_error_categories() {
        for error in [
            DurableError::Substrate {
                code: "provider_io_failed".to_owned(),
                message: "storage unavailable".to_owned(),
            },
            DurableError::Integrity {
                code: "provider_integrity_failed".to_owned(),
                message: "provider rejected immutable bytes".to_owned(),
            },
            DurableError::Conflict {
                expected: Some("before".to_owned()),
                current: Some("after".to_owned()),
            },
            DurableError::HistoryConflict {
                code: "provider_history_changed".to_owned(),
                message: "history differs".to_owned(),
            },
            DurableError::Validation("provider rejected a read".to_owned()),
        ] {
            assert_eq!(
                require_cold_audit_object::<()>(Err(error.clone()), "entry", "identity"),
                Err(error)
            );
        }
        assert!(matches!(
            require_cold_audit_object::<()>(Ok(None), "entry", "identity"),
            Err(DurableError::Integrity { .. })
        ));
        assert!(matches!(
            cold_archive_audit_error(DurableError::NotFound("required cold node".to_owned())),
            DurableError::Integrity { .. }
        ));
    }

    fn assert_cold_audit_rejects_without_commit(
        store: &mut MemoryStore,
        head: &StoreHead,
    ) -> DurableResult<()> {
        assert!(matches!(
            store.load_full_audit(),
            Err(DurableError::Integrity { .. })
        ));
        assert_eq!(store.load_head()?.as_ref(), Some(head));
        Ok(())
    }

    fn compacted_command_store() -> DurableResult<(MemoryStore, StoreHead, String)> {
        let mut store = completed_fixture_store()?;
        let source = store.load_head()?.ok_or_else(|| {
            DurableError::Validation("completed fixture has no Store head".to_owned())
        })?;
        let mut maintenance = crate::DurableStoreControl::open(store)?;
        let receipt = maintenance.compact_machine_history(&crate::HistoryCompactionRequest {
            compaction_id: "compaction:store-index".to_owned(),
            expected_revision: source.revision,
            kind: crate::HistoryCompactionKind::EventPrefix,
            requested_suffix: 0,
        })?;
        store = maintenance.into_store();
        let head = store.load_head()?.ok_or_else(|| {
            DurableError::Validation("compacted fixture has no Store head".to_owned())
        })?;
        let anchor = head.machine_base_anchor.as_ref().ok_or_else(|| {
            DurableError::Validation("compacted fixture has no pinned base anchor".to_owned())
        })?;
        let segment = store
            .load_machine_command_archive_segment(&anchor.archive_head)?
            .ok_or_else(|| {
                DurableError::Validation("compacted fixture lost its exact archive".to_owned())
            })?;
        assert_eq!(receipt.result.archive_segment, segment.header);
        let command_id = segment
            .entries
            .first()
            .map(|entry| entry.command.envelope.command_id.clone())
            .ok_or_else(|| {
                DurableError::Validation("completed fixture archived no command".to_owned())
            })?;
        assert!(store.load_full_audit()?.is_some());
        Ok((store, head, command_id))
    }

    struct ArchiveFixtureHost;

    fn archive_fixture_manifest() -> cymule_runtime::PluginManifest {
        cymule_runtime::PluginManifest {
            plugin_version: cymule_runtime::PLUGIN_VERSION.to_owned(),
            implementation_id: "store-archive-fixture".to_owned(),
            components: BTreeMap::new(),
            effects: BTreeMap::new(),
        }
    }

    impl cymule_runtime::PluginHost for ArchiveFixtureHost {
        fn invoke(
            &mut self,
            request: cymule_runtime::PluginRequest,
        ) -> cymule_runtime::RuntimeResult<cymule_runtime::PluginResponse> {
            match request {
                cymule_runtime::PluginRequest::Describe => {
                    Ok(cymule_runtime::PluginResponse::Manifest {
                        manifest: archive_fixture_manifest(),
                    })
                }
                _ => Err(cymule_runtime::RuntimeError::PluginDefect {
                    code: "unexpected_archive_fixture_request".to_owned(),
                    message: "operation-free archive fixture cannot execute provider work"
                        .to_owned(),
                }),
            }
        }
    }

    struct ArchiveFixtureClock(crate::ClockObservation);

    impl crate::ClockObservationAuthority for ArchiveFixtureClock {
        fn resolve(
            &mut self,
            reference: &cymule_durable_protocol::ClockObservationRef,
        ) -> DurableResult<crate::ClockObservation> {
            if self.0.reference() != *reference {
                return Err(DurableError::Validation(
                    "archive fixture Clock did not issue this reference".to_owned(),
                ));
            }
            Ok(self.0.clone())
        }
    }

    impl crate::ExecutionClockAuthority for ArchiveFixtureClock {
        fn with_current_head(
            &mut self,
            reference: &cymule_durable_protocol::ClockObservationRef,
            commit: &mut dyn FnMut(&crate::ClockObservation) -> DurableResult<()>,
        ) -> DurableResult<()> {
            let observation = <Self as crate::ClockObservationAuthority>::resolve(self, reference)?;
            commit(&observation)
        }
    }

    fn archive_fixture_observation(run_id: &str) -> DurableResult<crate::ClockObservation> {
        let source_id = "clock:store-archive-fixture".to_owned();
        let source_generation = content_id("test.store-archive-clock/1", &())?;
        let scope = cymule_durable_protocol::execution_clock_scope(run_id)?;
        let observation = crate::ClockObservation {
            clock_version: cymule_durable_protocol::CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id: cymule_durable_protocol::clock_observation_id(
                &source_id,
                &source_generation,
                &scope,
                1,
                0,
            )?,
            source_id,
            source_generation,
            scope,
            logical_time: 1,
            observed_unix_ms: 0,
        };
        observation.verify()?;
        Ok(observation)
    }

    fn archive_fixture_candidate() -> cymule_core::PlanCandidate {
        cymule_core::PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "store_archive_fixture".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![cymule_core::Definition {
                id: "main".to_owned(),
                input_schema: serde_json::Value::Bool(true),
                output_schema: serde_json::Value::Bool(true),
                body: cymule_core::Region {
                    steps: Vec::new(),
                    result: cymule_core::Expression::Input,
                },
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn completed_fixture_store() -> DurableResult<MemoryStore> {
        let run_id = "run:store-archive-fixture";
        let observation = archive_fixture_observation(run_id)?;
        let execution = crate::ExecutionClaimRequest {
            owner: "executor:store-archive-fixture".to_owned(),
            clock: observation.reference(),
            ttl: 10,
        };
        let binding = cymule_runtime::ExecutionBinding::for_local_process(
            &archive_fixture_manifest(),
            content_id("test.store-archive-runtime/1", &())?,
        )?;
        let admission =
            cymule_runtime::ExecutionBindingAdmission::admit(ArchiveFixtureHost, binding)?;
        let mut runtime = crate::DurableRuntimeControl::open(
            MemoryStore::new(),
            admission,
            ArchiveFixtureClock(observation),
        )?;
        let response = runtime.submit(crate::DurableCommand::StartRun {
            control_version: crate::DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: archive_fixture_candidate(),
            input: serde_json::Value::Null,
            execution,
        })?;
        assert!(matches!(
            response,
            crate::DurableResponse::RunBoundary {
                boundary: crate::DurableBoundary::Completed { .. },
            }
        ));
        Ok(runtime.into_parts().0)
    }

    #[test]
    fn store_batch_rejects_a_foreign_self_consistent_revision() {
        let initial = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let stored = initial.project(None).expect("initial state projects");
        let record = JournalRecord::new(
            "record:segment-base",
            "test.segment-base/1",
            serde_json::json!({"value": 1}),
        )
        .expect("journal record seals");
        let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:segment-base".to_owned(),
            records: vec![record],
        }])
        .expect("delta seals");
        let mut store = MemoryStore::new();
        store
            .compare_and_commit(None, &initial)
            .expect("initial roots commit");
        let transition = store
            .with_state_root_resolver(&stored.state_root_manifest, |resolver| {
                stored.state_root_manifest.apply(&delta, resolver)
            })
            .expect("state-root transition prepares");
        let batch = StoreBatch::transition_prepared(
            &stored.revision,
            &stored.head,
            delta,
            transition,
            Vec::new(),
        )
        .expect("transition batch seals");
        let mut forged_batch = batch.clone();
        forged_batch.head.revision =
            content_id("cymule.test-alternate-durable-revision/1", &"alternate")
                .expect("alternate revision derives");
        assert!(matches!(
            forged_batch.verify_against(Some(&stored.head)),
            Err(DurableError::Integrity { code, .. })
                if code == "durable_head_state_root_mismatch"
        ));
    }

    #[test]
    fn store_head_requires_explicit_nullable_machine_anchor() {
        let batch = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut head = serde_json::to_value(batch.head()).expect("head encodes");
        head.as_object_mut()
            .expect("head is an object")
            .remove("machine_base_anchor");
        assert!(serde_json::from_value::<StoreHead>(head).is_err());
    }

    #[test]
    fn gc_advances_only_the_physical_head_generation() {
        let batch = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut store = MemoryStore::new();
        let committed = store
            .compare_and_commit(None, &batch)
            .expect("initial roots commit");
        let receipt = store
            .advance_cold_reclamation(&StoreReclamation::new(&committed.head).unwrap())
            .expect("GC commits a physical generation");
        let reopened = store
            .load_full_audit()
            .expect("Store loads")
            .expect("state exists");
        assert_eq!(reopened.revision, committed.revision);
        assert_eq!(
            reopened.head.state_root_manifest_id,
            committed.head.state_root_manifest_id
        );
        assert_eq!(reopened.head.sequence, committed.head.sequence);
        assert_eq!(reopened.head.gc_sequence, committed.head.gc_sequence + 1);
        assert_ne!(reopened.head.physical_token, committed.head.physical_token);
        receipt
            .verify_for(&reopened.head)
            .expect("GC receipt binds physical head");
        let replay = store
            .reconcile_cold_reclamation(&StoreReclamation::new(&reopened.head).unwrap())
            .expect("same GC head replays its exact receipt");
        assert_eq!(replay, receipt);
        assert_eq!(
            store
                .load_full_audit()
                .expect("Store reloads")
                .expect("state exists")
                .head,
            reopened.head,
            "GC replay must not publish another physical generation"
        );
    }

    #[test]
    fn nonterminal_gc_lost_ack_reconciles_without_advancing() {
        let batch = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut store = MemoryStore::new();
        let committed = store
            .compare_and_commit(None, &batch)
            .expect("initial roots commit");
        let stored = store
            .load_full_audit()
            .expect("Store loads")
            .expect("state exists");
        let record = JournalRecord::new(
            "record:gc-nonterminal-orphan",
            "test.gc-nonterminal-orphan/1",
            serde_json::json!({"value": 1}),
        )
        .expect("journal record seals");
        let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:gc-nonterminal".to_owned(),
            records: vec![record],
        }])
        .expect("delta seals");
        let transition = store
            .with_state_root_resolver(&stored.state_root_manifest, |resolver| {
                stored.state_root_manifest.apply(&delta, resolver)
            })
            .expect("transition prepares");
        let orphan = crate::StateRootObject::Manifest(transition.manifest().clone());
        let orphan_id = orphan.object_id().to_owned();
        let mut already_reclaimed = BTreeSet::new();
        already_reclaimed.insert(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );
        let receipt = GcReceipt::new_bounded(&committed.head, already_reclaimed, 1)
            .expect("nonterminal receipt seals");
        let mut published_head = committed.head.clone();
        published_head.gc_sequence = receipt.gc_sequence;
        published_head
            .physical_token
            .clone_from(&receipt.result_physical_token);
        published_head.gc_receipt = Some(receipt.receipt_id.clone());
        receipt
            .verify_for(&published_head)
            .expect("published receipt binds head");
        {
            let mut domain = lock_memory(&store.current, None).expect("memory Store locks");
            domain.state_root_objects.insert(orphan_id.clone(), orphan);
            domain
                .receipts
                .insert(receipt.receipt_id.clone(), receipt.clone());
            domain.head = Some(published_head.clone());
        }

        let reconciled = store
            .reconcile_cold_reclamation(&StoreReclamation::new(&published_head).unwrap())
            .expect("lost acknowledgement reconciles");
        assert_eq!(reconciled, receipt);
        assert_eq!(reconciled.remaining_objects, 1);
        assert_eq!(
            store
                .load_full_audit()
                .expect("Store reloads")
                .expect("state exists")
                .head,
            published_head,
            "reconciliation must never select the next page"
        );
        assert!(
            lock_memory(&store.current, None)
                .expect("memory Store locks")
                .state_root_objects
                .contains_key(&orphan_id),
            "reconciliation must leave remainder candidates untouched"
        );

        let next = store
            .advance_cold_reclamation(&StoreReclamation::new(&published_head).unwrap())
            .expect("caller explicitly advances the remainder");
        assert_eq!(next.remaining_objects, 0);
        assert!(next.reclaimed_ids.contains(&receipt.receipt_id));
        assert!(next.reclaimed_ids.contains(&orphan_id));
        assert_eq!(store.stats().expect("stats read").gc_receipts, 1);
    }

    #[test]
    fn post_receipt_orphan_requires_an_explicit_new_gc_cycle() {
        let batch = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut store = MemoryStore::new();
        let committed = store
            .compare_and_commit(None, &batch)
            .expect("initial roots commit");
        let first = store
            .advance_cold_reclamation(&StoreReclamation::new(&committed.head).unwrap())
            .expect("clean inventory fence commits");
        assert_eq!(first.remaining_objects, 0);
        let after_first = store
            .load_full_audit()
            .expect("Store loads")
            .expect("state exists");
        let record = JournalRecord::new(
            "record:post-receipt-orphan",
            "test.post-receipt-orphan/1",
            serde_json::json!({"value": 1}),
        )
        .expect("journal record seals");
        let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:post-receipt-orphan".to_owned(),
            records: vec![record],
        }])
        .expect("delta seals");
        let transition = store
            .with_state_root_resolver(&after_first.state_root_manifest, |resolver| {
                after_first.state_root_manifest.apply(&delta, resolver)
            })
            .expect("transition prepares");
        let orphan = crate::StateRootObject::Manifest(transition.manifest().clone());
        let orphan_id = orphan.object_id().to_owned();
        lock_memory(&store.current, None)
            .expect("memory Store locks")
            .state_root_objects
            .insert(orphan_id.clone(), orphan);

        let replay = store
            .reconcile_cold_reclamation(&StoreReclamation::new(&after_first.head).unwrap())
            .expect("current receipt reconciles");
        assert_eq!(replay, first);
        assert!(
            lock_memory(&store.current, None)
                .expect("memory Store locks")
                .state_root_objects
                .contains_key(&orphan_id),
            "receipt replay cannot collect a post-receipt orphan"
        );

        let second = store
            .advance_cold_reclamation(&StoreReclamation::new(&after_first.head).unwrap())
            .expect("explicit new cycle commits");
        assert!(second.reclaimed_ids.contains(&first.receipt_id));
        assert!(second.reclaimed_ids.contains(&orphan_id));
        assert_eq!(second.remaining_objects, 0);
        assert_eq!(store.stats().expect("stats read").gc_receipts, 1);
    }

    #[test]
    fn memory_state_root_reads_reject_stale_manifests() {
        let initial = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut store = MemoryStore::new();
        store
            .compare_and_commit(None, &initial)
            .expect("initial roots commit");
        let before = store
            .load_full_audit()
            .expect("Store loads")
            .expect("state exists");
        let record = JournalRecord::new(
            "record:current-manifest-only",
            "test.current-manifest-only/1",
            serde_json::json!({"value": 1}),
        )
        .expect("journal record seals");
        let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:current-manifest-only".to_owned(),
            records: vec![record],
        }])
        .expect("delta seals");
        let transition = store
            .with_state_root_resolver(&before.state_root_manifest, |resolver| {
                before.state_root_manifest.apply(&delta, resolver)
            })
            .expect("transition prepares");
        let successor = StoreBatch::transition_prepared(
            &before.revision,
            &before.head,
            delta,
            transition,
            Vec::new(),
        )
        .expect("successor batch seals");
        store
            .compare_and_commit(Some(&before.head), &successor)
            .expect("successor commits");

        assert!(
            store
                .load_state_root_manifest(&before.state_root_manifest.manifest_id)
                .expect("stale lookup is bounded")
                .is_none()
        );
        assert!(matches!(
            store.with_state_root_resolver(&before.state_root_manifest, |resolver| {
                before.state_root_manifest.materialize(resolver)
            }),
            Err(DurableError::Conflict { .. })
        ));
        assert!(matches!(
            store.application_journal_prefix(
                &before.state_root_manifest,
                "journal:current-manifest-only",
                0,
            ),
            Err(DurableError::Conflict { .. })
        ));
        assert!(matches!(
            store.with_state_root_resolver(&before.state_root_manifest, |resolver| {
                crate::state_root::preview_application_journal_replacement(
                    &before.state_root_manifest,
                    resolver,
                    "journal:current-manifest-only",
                    0,
                    &[],
                )
            }),
            Err(DurableError::Conflict { .. })
        ));
    }

    #[test]
    fn memory_semantic_commit_does_not_read_a_missing_previous_gc_receipt() {
        let initial = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut store = MemoryStore::new();
        let initial_commit = store
            .compare_and_commit(None, &initial)
            .expect("initial roots commit");
        store
            .advance_cold_reclamation(&StoreReclamation::new(&initial_commit.head).unwrap())
            .expect("GC generation commits");
        let current = store
            .load_full_audit()
            .expect("Store loads")
            .expect("state exists");
        let record = JournalRecord::new(
            "record:missing-current-gc-receipt",
            "test.missing-current-gc-receipt/1",
            serde_json::json!({"value": 1}),
        )
        .expect("journal record seals");
        let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:missing-current-gc-receipt".to_owned(),
            records: vec![record],
        }])
        .expect("delta seals");
        let transition = store
            .with_state_root_resolver(&current.state_root_manifest, |resolver| {
                current.state_root_manifest.apply(&delta, resolver)
            })
            .expect("transition prepares while receipt exists");
        let successor = StoreBatch::transition_prepared(
            &current.revision,
            &current.head,
            delta,
            transition,
            Vec::new(),
        )
        .expect("successor batch seals");
        let receipt_id = current
            .head
            .gc_receipt
            .as_ref()
            .expect("current head pins a receipt")
            .clone();
        lock_memory(&store.current, None)
            .expect("memory Store locks")
            .receipts
            .remove(&receipt_id);

        let commit = store
            .compare_and_commit(Some(&current.head), &successor)
            .expect("semantic CAS is independent of the previous physical GC receipt bytes");
        assert_eq!(commit.head.gc_sequence, current.head.gc_sequence);
        assert!(commit.head.gc_receipt.is_none());
    }

    #[test]
    fn semantic_successor_allows_gc_to_reclaim_the_previous_receipt() {
        let initial = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut store = MemoryStore::new();
        let initial_commit = store
            .compare_and_commit(None, &initial)
            .expect("initial roots commit");
        let first_receipt = store
            .advance_cold_reclamation(&StoreReclamation::new(&initial_commit.head).unwrap())
            .expect("first GC commits");
        let after_gc = store
            .load_full_audit()
            .expect("Store loads")
            .expect("state exists");

        let record = JournalRecord::new(
            "record:gc-receipt-successor",
            "test.gc-receipt-successor/1",
            serde_json::json!({"value": 1}),
        )
        .expect("journal record seals");
        let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:gc-receipt-successor".to_owned(),
            records: vec![record],
        }])
        .expect("delta seals");
        let transition = store
            .with_state_root_resolver(&after_gc.state_root_manifest, |resolver| {
                after_gc.state_root_manifest.apply(&delta, resolver)
            })
            .expect("state-root transition prepares");
        let successor = StoreBatch::transition_prepared(
            &after_gc.revision,
            &after_gc.head,
            delta,
            transition,
            Vec::new(),
        )
        .expect("successor batch seals");
        let successor_commit = store
            .compare_and_commit(Some(&after_gc.head), &successor)
            .expect("semantic successor commits");
        assert!(successor_commit.head.gc_receipt.is_none());

        let second_receipt = store
            .advance_cold_reclamation(&StoreReclamation::new(&successor_commit.head).unwrap())
            .expect("successor GC commits");
        assert_ne!(second_receipt.receipt_id, first_receipt.receipt_id);
        let domain = lock_memory(&store.current, None).expect("memory Store locks");
        assert!(!domain.receipts.contains_key(&first_receipt.receipt_id));
        assert_eq!(
            domain.receipts.get(&second_receipt.receipt_id),
            Some(&second_receipt)
        );
    }

    #[test]
    fn gc_receipt_successor_preserves_semantics_and_requires_one_generation() -> DurableResult<()> {
        let initial = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))?;
        let before = initial.head();
        let receipt = GcReceipt::new_bounded(before, BTreeSet::new(), 0)?;
        let next = receipt.successor_head(before)?;
        assert_eq!(next.revision, before.revision);
        assert_eq!(next.sequence, before.sequence);
        assert_eq!(next.state_root_manifest_id, before.state_root_manifest_id);
        assert_eq!(next.machine_base_anchor, before.machine_base_anchor);
        assert_eq!(next.gc_sequence, before.gc_sequence + 1);
        assert_eq!(next.gc_receipt.as_ref(), Some(&receipt.receipt_id));
        assert_eq!(next.physical_token, receipt.result_physical_token);

        let mut later = before.clone();
        later.gc_sequence += 1;
        let skipped = GcReceipt::new_bounded(&later, BTreeSet::new(), 0)?;
        skipped.verify_identity()?;
        assert!(matches!(
            skipped.successor_head(before),
            Err(DurableError::Integrity { code, .. }) if code == "gc_receipt_successor_mismatch"
        ));
        let mut foreign = before.clone();
        foreign.revision = content_id("test.gc-other-revision/1", &())?;
        let other = GcReceipt::new_bounded(&foreign, BTreeSet::new(), 0)?;
        other.verify_identity()?;
        assert!(other.successor_head(before).is_err());
        later.gc_sequence = crate::MAX_EXACT_INTEGER;
        assert!(receipt.successor_head(&later).is_err());
        Ok(())
    }

    #[test]
    fn gc_receipt_identity_binds_the_complete_physical_transition() {
        let batch = StoreBatch::initialize_state(DurableState::new(Machine::new().snapshot()))
            .expect("initial batch seals");
        let mut reclaimed = BTreeSet::new();
        reclaimed.insert(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );
        let receipt =
            GcReceipt::new_bounded(batch.head(), reclaimed.clone(), 0).expect("GC receipt seals");
        receipt
            .verify_identity()
            .expect("standalone GC receipt verifies");
        let paged = GcReceipt::new_bounded(batch.head(), reclaimed.clone(), 1)
            .expect("a smaller deterministic page may retain exact remaining work");
        assert_eq!(paged.reclaimed_objects, 1);
        assert_eq!(paged.remaining_objects, 1);
        let empty = GcReceipt::new_bounded(batch.head(), BTreeSet::new(), 0)
            .expect("a clean inventory has an explicit completion receipt");
        empty
            .verify_identity()
            .expect("empty completion receipt verifies");

        let mut legacy = receipt.clone();
        legacy.receipt_version = "cymule.durable-gc-receipt/1".to_owned();
        assert!(matches!(
            legacy.verify_identity(),
            Err(DurableError::Validation(message))
                if message.contains("unsupported GC receipt version")
        ));

        for forged in [
            {
                let mut value = receipt.clone();
                value.sequence += 1;
                value
            },
            {
                let mut value = receipt.clone();
                value.gc_sequence += 1;
                value
            },
            {
                let mut value = receipt.clone();
                value.parent_physical_token =
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned();
                value
            },
            {
                let mut value = receipt.clone();
                value.result_physical_token =
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_owned();
                value
            },
            {
                let mut value = receipt.clone();
                value.remaining_objects = 1;
                value
            },
            {
                let mut value = receipt.clone();
                value.remaining_objects = crate::MAX_EXACT_INTEGER;
                value
            },
        ] {
            assert!(forged.verify_identity().is_err());
        }
    }

    #[test]
    fn memory_archive_lookup_rejects_head_metadata_that_disagrees_with_its_manifest()
    -> DurableResult<()> {
        let (mut store, head, command_id) = compacted_command_store()?;
        let anchor = head
            .machine_base_anchor
            .clone()
            .expect("compaction pins an anchor");
        lock_memory(&store.current, None)
            .expect("memory Store locks")
            .head
            .as_mut()
            .expect("head exists")
            .sequence += 1;

        assert!(matches!(
            store.lookup_machine_command_archive(&anchor, &command_id),
            Err(DurableError::Integrity { code, .. }) if code == "memory_state_root_head_mismatch"
        ));
        Ok(())
    }

    #[test]
    fn command_index_lookup_and_gc_fail_closed_on_missing_objects() -> DurableResult<()> {
        let (mut entry_store, entry_head, command_id) = compacted_command_store()?;
        let anchor = entry_head
            .machine_base_anchor
            .as_ref()
            .expect("compaction pins an anchor");
        let lookup = entry_store
            .lookup_machine_command_archive(anchor, &command_id)
            .expect("membership lookup resolves");
        let entry_id = match lookup {
            cymule_core::MachineCommandArchiveLookup::Member { index_proof, .. } => {
                index_proof
                    .value
                    .expect("membership carries a value")
                    .archive_entry_digest
            }
            cymule_core::MachineCommandArchiveLookup::NonMember { .. } => {
                panic!("archived command must be a member")
            }
        };
        lock_memory(&entry_store.current, None)
            .expect("memory Store locks")
            .machine_command_archive_entries
            .remove(&entry_id);
        assert!(matches!(
            entry_store.lookup_machine_command_archive(anchor, &command_id),
            Err(DurableError::NotFound(_))
        ));
        assert!(matches!(
            entry_store.advance_cold_reclamation(&StoreReclamation::new(&entry_head).unwrap()),
            Err(DurableError::NotFound(_))
        ));

        let (mut node_store, node_head, command_id) = compacted_command_store()?;
        let anchor = node_head
            .machine_base_anchor
            .as_ref()
            .expect("compaction pins an anchor");
        lock_memory(&node_store.current, None)
            .expect("memory Store locks")
            .machine_command_index_nodes
            .remove(&anchor.command_index_root);
        assert!(matches!(
            node_store.lookup_machine_command_archive(anchor, &command_id),
            Err(DurableError::NotFound(_))
        ));
        assert!(matches!(
            node_store.advance_cold_reclamation(&StoreReclamation::new(&node_head).unwrap()),
            Err(DurableError::NotFound(_))
        ));
        Ok(())
    }
}
