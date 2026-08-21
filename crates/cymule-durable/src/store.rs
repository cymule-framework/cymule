use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, TryLockError};

use crate::{
    AuthorityLease, ComponentOccurrence, Continuation, DurableError, DurableResult, DurableState,
    EffectDispatch, HistoryCompactionReceipt, JournalRecord, SnapshotRecord, StoredState,
    WaitActivation, WaitCondition,
};
use cymule_core::{canonical_digest, content_id};
use serde::{Deserialize, Serialize};

/// Small mutable storage-head schema.
pub const STORE_HEAD_VERSION: &str = "cymule.durable-head/1";
/// Immutable state-delta segment schema.
pub const STATE_SEGMENT_VERSION: &str = "cymule.durable-segment/2";
/// Authenticated projection-checkpoint schema.
pub const STATE_CHECKPOINT_VERSION: &str = "cymule.durable-checkpoint/1";
/// Cold-reclamation receipt schema.
pub const GC_RECEIPT_VERSION: &str = "cymule.durable-gc-receipt/1";
/// Maximum delta suffix length admitted by the contract.
pub const MAX_HOT_SEGMENTS: u32 = 32;
/// Maximum authenticated checkpoint packs between materialized bases.
pub const MAX_CHECKPOINT_PACKS: u32 = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Bounded mutable pointer committed by compare-and-swap.
pub struct StoreHead {
    /// Versioned head shape.
    pub head_version: String,
    /// Canonical semantic state revision.
    pub revision: String,
    /// Current authenticated checkpoint.
    pub checkpoint_id: String,
    /// Manifest-pack depth since the latest materialized base.
    pub checkpoint_depth: u32,
    /// Latest post-checkpoint delta.
    pub suffix_head: Option<String>,
    /// Exact bounded suffix length.
    pub suffix_len: u32,
    /// Monotonic physical commit sequence.
    pub sequence: u64,
    /// Latest cold-reclamation receipt.
    pub gc_receipt: Option<String>,
}

impl StoreHead {
    /// Verify the bounded versioned shape.
    pub fn verify(&self) -> DurableResult<()> {
        if self.head_version != STORE_HEAD_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable head version {:?}",
                self.head_version
            )));
        }
        if self.revision.is_empty() || self.checkpoint_id.is_empty() {
            return Err(DurableError::Validation(
                "durable head requires revision and checkpoint identities".to_owned(),
            ));
        }
        if self.suffix_len >= MAX_HOT_SEGMENTS
            || self.checkpoint_depth >= MAX_CHECKPOINT_PACKS
            || (self.suffix_len == 0) != self.suffix_head.is_none()
        {
            return Err(DurableError::Validation(
                "durable head suffix is inconsistent or exceeds its bound".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Closed provider-neutral mutations admitted by one M1 commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableOperation {
    /// Apply one incremental canonical Machine transition.
    ApplyMachine {
        /// Exact semantic delta admitted by the core.
        delta: cymule_core::MachineDelta,
    },
    /// Insert or replace one Continuation.
    PutContinuation {
        /// Exact next Continuation.
        value: Continuation,
    },
    /// Insert or replace one wait.
    PutWait {
        /// Exact next wait record.
        value: WaitCondition,
    },
    /// Insert one identified activation receipt.
    PutWaitActivation {
        /// Exact activation receipt.
        value: WaitActivation,
    },
    /// Insert or replace one authority lease.
    PutLease {
        /// Exact next lease.
        value: AuthorityLease,
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
    /// Insert one portable snapshot record.
    PutSnapshot {
        /// Portable snapshot metadata.
        value: SnapshotRecord,
    },
    /// Insert one Machine-history compaction receipt.
    PutHistoryCompaction {
        /// Authenticated compaction receipt.
        value: HistoryCompactionReceipt,
    },
    /// Append exact records to one typed application journal.
    AppendJournal {
        /// Stable journal identity.
        journal_id: String,
        /// Exact new record suffix.
        records: Vec<JournalRecord>,
    },
}

/// Ordered typed mutation set for one semantic transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableDelta {
    /// Closed operations applied atomically in order.
    pub operations: Vec<DurableOperation>,
}

impl DurableDelta {
    /// Construct a non-empty delta.
    pub fn new(operations: Vec<DurableOperation>) -> DurableResult<Self> {
        if operations.is_empty() {
            return Err(DurableError::Validation(
                "durable delta is empty".to_owned(),
            ));
        }
        Ok(Self { operations })
    }

    /// Apply the already-admitted delta to an in-memory materialized projection.
    pub fn apply(&self, state: &mut DurableState) -> DurableResult<()> {
        for operation in &self.operations {
            match operation {
                DurableOperation::ApplyMachine { delta } => state.machine.apply_delta(delta)?,
                DurableOperation::PutContinuation { value } => {
                    state
                        .continuations
                        .insert(value.run_id.clone(), value.clone());
                }
                DurableOperation::PutWait { value } => {
                    state.waits.insert(value.wait_id.clone(), value.clone());
                }
                DurableOperation::PutWaitActivation { value } => {
                    state
                        .wait_activations
                        .insert(value.activation_id.clone(), value.clone());
                }
                DurableOperation::PutLease { value } => {
                    state.leases.insert(value.resource.clone(), value.clone());
                }
                DurableOperation::PutOutbox { value } => {
                    state.outbox.insert(value.intent_id.clone(), value.clone());
                }
                DurableOperation::PutComponentOccurrence { value } => {
                    state
                        .component_occurrences
                        .insert(value.occurrence_id.clone(), value.clone());
                }
                DurableOperation::PutSnapshot { value } => {
                    state
                        .snapshots
                        .insert(value.snapshot_id.clone(), value.clone());
                }
                DurableOperation::PutHistoryCompaction { value } => {
                    state
                        .history_compactions
                        .insert(value.compaction_id.clone(), value.clone());
                }
                DurableOperation::AppendJournal {
                    journal_id,
                    records,
                } => {
                    state
                        .application_journals
                        .entry(journal_id.clone())
                        .or_default()
                        .extend(records.iter().cloned());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_against(
        &self,
        state: &DurableState,
        current_machine: &cymule_core::Machine,
    ) -> DurableResult<cymule_core::Machine> {
        let mut machine = current_machine.clone();
        for operation in &self.operations {
            if let DurableOperation::ApplyMachine { delta } = operation {
                machine.apply_delta(delta)?;
            }
        }
        for operation in &self.operations {
            match operation {
                DurableOperation::PutContinuation { value } => {
                    crate::model::validate_continuation_artifacts(&machine, value)?;
                }
                DurableOperation::PutWait { value } => {
                    let continuation = self
                        .operations
                        .iter()
                        .rev()
                        .find_map(|operation| match operation {
                            DurableOperation::PutContinuation {
                                value: continuation,
                            } if continuation.run_id == value.run_id => Some(continuation),
                            _ => None,
                        })
                        .or_else(|| state.continuations.get(&value.run_id))
                        .ok_or_else(|| {
                            DurableError::Validation(format!(
                                "wait {} has no owning Continuation",
                                value.wait_id
                            ))
                        })?;
                    crate::model::validate_wait_artifacts(&machine, continuation, value)?;
                }
                DurableOperation::PutOutbox { value } => {
                    crate::model::validate_dispatch_artifacts(&machine, value)?;
                }
                DurableOperation::PutComponentOccurrence { value } => {
                    let target = self
                        .operations
                        .iter()
                        .rev()
                        .find_map(|operation| match operation {
                            DurableOperation::PutContinuation { value: continuation }
                                if continuation.run_id == value.run_id => Some(continuation),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            DurableError::Validation(
                                "component occurrence requires its post-call Continuation in the same delta"
                                    .to_owned(),
                            )
                        })?;
                    let source = state.continuations.get(&value.run_id).ok_or_else(|| {
                        DurableError::Validation(
                            "component occurrence requires its source Continuation".to_owned(),
                        )
                    })?;
                    let current_machine = cymule_core::Machine::restore(state.machine.clone())?;
                    let derived = crate::coordinator::derive_component_occurrence(
                        &current_machine,
                        &machine,
                        source,
                        target,
                        &value.input,
                        &value.output,
                    )?;
                    if &derived != value {
                        return Err(DurableError::Validation(
                            "component occurrence is not the derived atomic call transition"
                                .to_owned(),
                        ));
                    }
                    crate::model::require_artifact(
                        &machine,
                        &value.input,
                        "component occurrence input",
                    )?;
                    crate::model::require_artifact(
                        &machine,
                        &value.output,
                        "component occurrence output",
                    )?;
                }
                DurableOperation::PutWaitActivation { value } => {
                    value.verify()?;
                    crate::model::require_artifact(
                        &machine,
                        &value.result,
                        "wait activation result",
                    )?;
                }
                DurableOperation::PutHistoryCompaction { value } => value.verify()?,
                DurableOperation::AppendJournal {
                    journal_id,
                    records,
                } => {
                    let existing = state
                        .application_journals
                        .get(journal_id)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    let mut ids = BTreeSet::new();
                    for record in records {
                        record.verify()?;
                        if !ids.insert(&record.record_id)
                            || existing
                                .iter()
                                .any(|value| value.record_id == record.record_id)
                        {
                            return Err(DurableError::IllegalTransition(format!(
                                "application journal {journal_id} repeats record {}",
                                record.record_id
                            )));
                        }
                    }
                }
                DurableOperation::ApplyMachine { .. }
                | DurableOperation::PutLease { .. }
                | DurableOperation::PutSnapshot { .. } => {}
            }
        }
        Ok(machine)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable content-addressed transition between semantic revisions.
pub struct StateSegment {
    /// Versioned segment shape.
    pub segment_version: String,
    /// Content identity of the remaining fields.
    pub segment_id: String,
    /// Monotonic physical commit sequence.
    pub sequence: u64,
    /// Previous segment in the physical lineage.
    pub parent_segment: Option<String>,
    /// Required semantic base revision.
    pub base_revision: String,
    /// Resulting semantic revision.
    pub revision: String,
    /// Minimal recursive state change.
    pub delta: DurableDelta,
}

impl StateSegment {
    fn next(current: &StoredState, delta: DurableDelta) -> DurableResult<Self> {
        let sequence = current.head.sequence.checked_add(1).ok_or_else(|| {
            DurableError::Validation("durable segment sequence overflowed".to_owned())
        })?;
        let parent_segment = current.suffix_head_or_checkpoint_segment();
        let revision = canonical_digest(&(current.revision.as_str(), &delta))?;
        let segment_id = segment_identity(
            sequence,
            parent_segment.as_deref(),
            &current.revision,
            &revision,
            &delta,
        )?;
        Ok(Self {
            segment_version: STATE_SEGMENT_VERSION.to_owned(),
            segment_id,
            sequence,
            parent_segment,
            base_revision: current.revision.clone(),
            revision,
            delta,
        })
    }

    /// Verify the segment's content identity.
    pub fn verify(&self) -> DurableResult<()> {
        if self.segment_version != STATE_SEGMENT_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable segment version {:?}",
                self.segment_version
            )));
        }
        let expected = segment_identity(
            self.sequence,
            self.parent_segment.as_deref(),
            &self.base_revision,
            &self.revision,
            &self.delta,
        )?;
        if self.segment_id != expected {
            return Err(DurableError::Validation(format!(
                "durable segment identity {} does not match {expected}",
                self.segment_id
            )));
        }
        Ok(())
    }

    /// Apply and authenticate the delta against its exact base.
    pub fn apply(&self, current: &DurableState) -> DurableResult<DurableState> {
        self.verify()?;
        let mut next = current.clone();
        self.delta.apply(&mut next)?;
        Ok(next)
    }
}

fn segment_identity(
    sequence: u64,
    parent_segment: Option<&str>,
    base_revision: &str,
    revision: &str,
    delta: &DurableDelta,
) -> DurableResult<String> {
    content_id(
        STATE_SEGMENT_VERSION,
        &(sequence, parent_segment, base_revision, revision, delta),
    )
    .map_err(Into::into)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Authenticated complete projection used to bound suffix replay.
pub struct StateCheckpoint {
    /// Versioned checkpoint shape.
    pub checkpoint_version: String,
    /// Content identity of the remaining fields.
    pub checkpoint_id: String,
    /// Previous checkpoint retained as cold lineage.
    pub parent_checkpoint: Option<String>,
    /// Latest segment incorporated in this projection.
    pub covered_segment: Option<String>,
    /// Physical sequence incorporated in this projection.
    pub sequence: u64,
    /// Canonical semantic state revision.
    pub revision: String,
    /// Complete authenticated hot projection.
    pub state: Option<DurableState>,
}

impl StateCheckpoint {
    /// Build a content-addressed checkpoint.
    pub fn new(
        parent_checkpoint: Option<String>,
        covered_segment: Option<String>,
        sequence: u64,
        state: DurableState,
    ) -> DurableResult<Self> {
        let revision = state.revision()?;
        Self::for_revision(
            parent_checkpoint,
            covered_segment,
            sequence,
            revision,
            Some(state),
        )
    }

    /// Create a checkpoint for an already authenticated incremental revision.
    pub fn for_revision(
        parent_checkpoint: Option<String>,
        covered_segment: Option<String>,
        sequence: u64,
        revision: String,
        state: Option<DurableState>,
    ) -> DurableResult<Self> {
        if let Some(state) = &state {
            state.validate()?;
        } else if parent_checkpoint.is_none() || covered_segment.is_none() || sequence == 0 {
            return Err(DurableError::Validation(
                "delta checkpoint requires parent and covered segment identities".to_owned(),
            ));
        }
        let checkpoint_id = checkpoint_identity(
            parent_checkpoint.as_deref(),
            covered_segment.as_deref(),
            sequence,
            &revision,
            state.as_ref(),
        )?;
        Ok(Self {
            checkpoint_version: STATE_CHECKPOINT_VERSION.to_owned(),
            checkpoint_id,
            parent_checkpoint,
            covered_segment,
            sequence,
            revision,
            state,
        })
    }

    /// Verify checkpoint identity and projection revision.
    pub fn verify(&self) -> DurableResult<()> {
        if self.checkpoint_version != STATE_CHECKPOINT_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable checkpoint version {:?}",
                self.checkpoint_version
            )));
        }
        if let Some(state) = &self.state {
            state.validate()?;
        } else if self.parent_checkpoint.is_none() || self.covered_segment.is_none() {
            return Err(DurableError::Validation(
                "delta checkpoint is missing its parent or covered segment".to_owned(),
            ));
        }
        let expected = checkpoint_identity(
            self.parent_checkpoint.as_deref(),
            self.covered_segment.as_deref(),
            self.sequence,
            &self.revision,
            self.state.as_ref(),
        )?;
        if self.checkpoint_id != expected {
            return Err(DurableError::Validation(format!(
                "durable checkpoint identity {} does not match {expected}",
                self.checkpoint_id
            )));
        }
        Ok(())
    }
}

fn checkpoint_identity(
    parent_checkpoint: Option<&str>,
    covered_segment: Option<&str>,
    sequence: u64,
    revision: &str,
    state: Option<&DurableState>,
) -> DurableResult<String> {
    content_id(
        STATE_CHECKPOINT_VERSION,
        &(
            parent_checkpoint,
            covered_segment,
            sequence,
            revision,
            state,
        ),
    )
    .map_err(Into::into)
}

#[derive(Debug, Clone, PartialEq)]
/// Immutable objects and exact next head for one atomic commit.
pub struct StoreBatch {
    /// Delta for a transition; absent only for initialization.
    segment: Option<StateSegment>,
    /// Checkpoint for initialization or suffix rotation.
    checkpoint: Option<StateCheckpoint>,
    /// Exact next small head.
    head: StoreHead,
}

impl StoreBatch {
    /// Immutable delta segment, absent only during initialization.
    pub fn segment(&self) -> Option<&StateSegment> {
        self.segment.as_ref()
    }
    /// Checkpoint created by initialization or rotation.
    pub fn checkpoint(&self) -> Option<&StateCheckpoint> {
        self.checkpoint.as_ref()
    }
    /// Exact next small CAS head.
    pub fn head(&self) -> &StoreHead {
        &self.head
    }
    /// Advance one caller-owned materialized projection after this exact batch commits.
    pub fn apply_committed(
        &self,
        current: &mut StoredState,
        commit: &StoreCommit,
    ) -> DurableResult<()> {
        if commit.head != self.head || commit.revision != self.head.revision {
            return Err(DurableError::Validation(
                "store commit does not acknowledge this exact batch".to_owned(),
            ));
        }
        let segment = self.segment.as_ref().ok_or_else(|| {
            DurableError::Validation("initialization batch has no prior projection".to_owned())
        })?;
        segment.delta.apply(&mut current.state)?;
        current.revision.clone_from(&commit.revision);
        current.head.clone_from(&commit.head);
        if let Some(checkpoint) = &self.checkpoint {
            current
                .checkpoint_covered_segment
                .clone_from(&checkpoint.covered_segment);
        }
        Ok(())
    }
    /// Build the sequence-zero checkpoint and head.
    pub fn initialize(state: DurableState) -> DurableResult<Self> {
        let checkpoint = StateCheckpoint::new(None, None, 0, state)?;
        let head = StoreHead {
            head_version: STORE_HEAD_VERSION.to_owned(),
            revision: checkpoint.revision.clone(),
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            checkpoint_depth: 0,
            suffix_head: None,
            suffix_len: 0,
            sequence: 0,
            gc_receipt: None,
        };
        Ok(Self {
            segment: None,
            checkpoint: Some(checkpoint),
            head,
        })
    }

    /// Build one delta transition and rotate at the suffix bound.
    pub fn transition(current: &StoredState, delta: DurableDelta) -> DurableResult<Self> {
        let segment = StateSegment::next(current, delta)?;
        let suffix_len = current.head.suffix_len.checked_add(1).ok_or_else(|| {
            DurableError::Validation("durable suffix length overflowed".to_owned())
        })?;
        if suffix_len >= MAX_HOT_SEGMENTS {
            let checkpoint_depth =
                current
                    .head
                    .checkpoint_depth
                    .checked_add(1)
                    .ok_or_else(|| {
                        DurableError::Validation("checkpoint depth overflowed".to_owned())
                    })?;
            let (checkpoint, checkpoint_depth) = if checkpoint_depth >= MAX_CHECKPOINT_PACKS {
                let mut materialized = current.state.clone();
                segment.delta.apply(&mut materialized)?;
                (
                    StateCheckpoint::for_revision(
                        None,
                        Some(segment.segment_id.clone()),
                        segment.sequence,
                        segment.revision.clone(),
                        Some(materialized),
                    )?,
                    0,
                )
            } else {
                (
                    StateCheckpoint::for_revision(
                        Some(current.head.checkpoint_id.clone()),
                        Some(segment.segment_id.clone()),
                        segment.sequence,
                        segment.revision.clone(),
                        None,
                    )?,
                    checkpoint_depth,
                )
            };
            let head = StoreHead {
                head_version: STORE_HEAD_VERSION.to_owned(),
                revision: segment.revision.clone(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                checkpoint_depth,
                suffix_head: None,
                suffix_len: 0,
                sequence: segment.sequence,
                gc_receipt: None,
            };
            Ok(Self {
                segment: Some(segment),
                checkpoint: Some(checkpoint),
                head,
            })
        } else {
            let head = StoreHead {
                head_version: STORE_HEAD_VERSION.to_owned(),
                revision: segment.revision.clone(),
                checkpoint_id: current.head.checkpoint_id.clone(),
                checkpoint_depth: current.head.checkpoint_depth,
                suffix_head: Some(segment.segment_id.clone()),
                suffix_len,
                sequence: segment.sequence,
                gc_receipt: None,
            };
            Ok(Self {
                segment: Some(segment),
                checkpoint: None,
                head,
            })
        }
    }

    /// Verify atomic contents against the exact current projection.
    pub fn verify_against(&self, current: Option<&StoreHead>) -> DurableResult<()> {
        self.head.verify()?;
        if self.head.gc_receipt.is_some() {
            return Err(DurableError::Validation(
                "normal StoreBatch cannot publish a GC receipt".to_owned(),
            ));
        }
        match current {
            None => {
                let checkpoint = self.checkpoint.as_ref().ok_or_else(|| {
                    DurableError::Validation("initialization requires a checkpoint".to_owned())
                })?;
                if self.segment.is_some()
                    || self.head.sequence != 0
                    || self.head.suffix_len != 0
                    || self.head.checkpoint_depth != 0
                {
                    return Err(DurableError::Validation(
                        "initialization must contain only a sequence-zero checkpoint".to_owned(),
                    ));
                }
                checkpoint.verify()?;
                if checkpoint.checkpoint_id != self.head.checkpoint_id
                    || checkpoint.revision != self.head.revision
                {
                    return Err(DurableError::Validation(
                        "initial checkpoint does not match durable head".to_owned(),
                    ));
                }
                Ok(())
            }
            Some(current) => {
                let segment = self.segment.as_ref().ok_or_else(|| {
                    DurableError::Validation("transition requires a segment".to_owned())
                })?;
                if segment.sequence != current.sequence + 1
                    || segment.base_revision != current.revision
                    || current.suffix_head.is_some()
                        && segment.parent_segment != current.suffix_head
                {
                    return Err(DurableError::Validation(
                        "segment lineage does not match current head".to_owned(),
                    ));
                }
                if self.head.revision != segment.revision || self.head.sequence != segment.sequence
                {
                    return Err(DurableError::Validation(
                        "segment does not match next head".to_owned(),
                    ));
                }
                match &self.checkpoint {
                    Some(checkpoint) => {
                        checkpoint.verify()?;
                        let materialized = checkpoint.state.is_some();
                        let expected_depth = if materialized {
                            0
                        } else {
                            current.checkpoint_depth + 1
                        };
                        if (!materialized
                            && checkpoint.parent_checkpoint.as_deref()
                                != Some(current.checkpoint_id.as_str()))
                            || (materialized && checkpoint.parent_checkpoint.is_some())
                            || checkpoint.covered_segment.as_deref()
                                != Some(segment.segment_id.as_str())
                            || self.head.checkpoint_id != checkpoint.checkpoint_id
                            || self.head.checkpoint_depth != expected_depth
                            || self.head.suffix_len != 0
                            || self.head.suffix_head.is_some()
                        {
                            return Err(DurableError::Validation(
                                "rotated checkpoint does not match segment and head".to_owned(),
                            ));
                        }
                    }
                    None => {
                        if self.head.checkpoint_id != current.checkpoint_id
                            || self.head.checkpoint_depth != current.checkpoint_depth
                            || self.head.suffix_head.as_deref() != Some(segment.segment_id.as_str())
                            || self.head.suffix_len != current.suffix_len + 1
                        {
                            return Err(DurableError::Validation(
                                "hot suffix head does not match segment".to_owned(),
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
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
    /// Checkpoint kept as reopen authority.
    pub retained_checkpoint: String,
    /// Digest of sorted reclaimed object identities.
    pub reclaimed_digest: String,
    /// Exact sorted immutable object identities authorized for deletion.
    pub reclaimed_ids: BTreeSet<String>,
    /// Number of reclaimed objects.
    pub reclaimed_objects: u64,
}

impl GcReceipt {
    /// Build a receipt over an exact reclaimed identity set.
    pub fn new(head: &StoreHead, reclaimed: &BTreeSet<String>) -> DurableResult<Self> {
        let reclaimed_digest = canonical_digest(reclaimed)?;
        let reclaimed_objects = u64::try_from(reclaimed.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let receipt_id = content_id(
            GC_RECEIPT_VERSION,
            &(
                head.revision.as_str(),
                head.checkpoint_id.as_str(),
                &reclaimed_digest,
                reclaimed,
                reclaimed_objects,
            ),
        )?;
        Ok(Self {
            receipt_version: GC_RECEIPT_VERSION.to_owned(),
            receipt_id,
            revision: head.revision.clone(),
            retained_checkpoint: head.checkpoint_id.clone(),
            reclaimed_digest,
            reclaimed_ids: reclaimed.clone(),
            reclaimed_objects,
        })
    }

    /// Verify receipt identity and its retained head boundary.
    pub fn verify_for(&self, head: &StoreHead) -> DurableResult<()> {
        if self.receipt_version != GC_RECEIPT_VERSION
            || self.revision != head.revision
            || self.retained_checkpoint != head.checkpoint_id
        {
            return Err(DurableError::Validation(
                "GC receipt does not match the retained durable head".to_owned(),
            ));
        }
        let expected = content_id(
            GC_RECEIPT_VERSION,
            &(
                self.revision.as_str(),
                self.retained_checkpoint.as_str(),
                self.reclaimed_digest.as_str(),
                &self.reclaimed_ids,
                self.reclaimed_objects,
            ),
        )?;
        let expected_digest = canonical_digest(&self.reclaimed_ids)?;
        let expected_count = u64::try_from(self.reclaimed_ids.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if self.receipt_id != expected
            || self.reclaimed_digest != expected_digest
            || self.reclaimed_objects != expected_count
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
    /// Retained checkpoints.
    pub checkpoints: u64,
    /// Retained segments.
    pub segments: u64,
    /// Segments read by the latest reopen.
    pub reopened_segments: u32,
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

/// Provider-neutral segmented single-domain store.
pub trait DurableStore {
    /// Reconstruct and authenticate the current checkpoint plus bounded suffix.
    fn load(&mut self) -> DurableResult<Option<StoredState>>;
    /// Atomically insert immutable objects and compare-and-swap the small head.
    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit>;
    /// Reclaim objects older than the current checkpoint and retain a receipt.
    fn reclaim_cold(&mut self, _expected: &StoreHead) -> DurableResult<GcReceipt> {
        Err(DurableError::Validation(
            "durable store does not implement cold reclamation".to_owned(),
        ))
    }
    /// Return physical object counts and bounded reopen work.
    fn stats(&self) -> DurableResult<StoreStats> {
        Ok(StoreStats::default())
    }
}

#[derive(Debug, Clone, Default)]
struct MemoryDomain {
    head: Option<StoreHead>,
    checkpoints: BTreeMap<String, StateCheckpoint>,
    segments: BTreeMap<String, StateSegment>,
    receipts: BTreeMap<String, GcReceipt>,
    reopened_segments: u32,
}

#[derive(Debug, Clone, Default)]
/// In-memory segmented reference store for conformance and fault injection.
pub struct MemoryStore {
    current: Arc<Mutex<MemoryDomain>>,
}

impl MemoryStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl DurableStore for MemoryStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        let mut domain = lock_memory(&self.current, None)?;
        let Some(head) = domain.head.clone() else {
            return Ok(None);
        };
        if let Some(receipt_id) = &head.gc_receipt {
            domain
                .receipts
                .get(receipt_id)
                .ok_or_else(|| {
                    DurableError::NotFound(format!("GC receipt {receipt_id} does not exist"))
                })?
                .verify_for(&head)?;
        }
        let (stored, reopened) = restore(
            &head,
            |id| Ok(domain.checkpoints.get(id).cloned()),
            |id| Ok(domain.segments.get(id).cloned()),
        )?;
        domain.reopened_segments = reopened;
        Ok(Some(stored))
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
        insert_immutable(&mut domain.segments, batch.segment.as_ref(), |v| {
            &v.segment_id
        })?;
        insert_immutable(&mut domain.checkpoints, batch.checkpoint.as_ref(), |v| {
            &v.checkpoint_id
        })?;
        domain.head = Some(batch.head.clone());
        Ok(StoreCommit {
            revision: batch.head.revision.clone(),
            head: batch.head.clone(),
        })
    }

    fn reclaim_cold(&mut self, expected: &StoreHead) -> DurableResult<GcReceipt> {
        let mut domain = lock_memory(&self.current, Some(expected))?;
        if domain.head.as_ref() != Some(expected) {
            return Err(conflict(Some(expected), domain.head.as_ref()));
        }
        let stored = restore(
            expected,
            |id| Ok(domain.checkpoints.get(id).cloned()),
            |id| Ok(domain.segments.get(id).cloned()),
        )?
        .0;
        let checkpoint = StateCheckpoint::for_revision(
            None,
            None,
            expected.sequence,
            expected.revision.clone(),
            Some(stored.state),
        )?;
        let mut head = expected.clone();
        head.checkpoint_id.clone_from(&checkpoint.checkpoint_id);
        head.checkpoint_depth = 0;
        head.suffix_head = None;
        head.suffix_len = 0;
        let mut reclaimed = domain.checkpoints.keys().cloned().collect::<BTreeSet<_>>();
        reclaimed.extend(domain.segments.keys().cloned());
        reclaimed.remove(&checkpoint.checkpoint_id);
        domain.checkpoints.clear();
        domain.segments.clear();
        domain
            .checkpoints
            .insert(checkpoint.checkpoint_id.clone(), checkpoint);
        let receipt = GcReceipt::new(&head, &reclaimed)?;
        domain
            .receipts
            .insert(receipt.receipt_id.clone(), receipt.clone());
        head.gc_receipt = Some(receipt.receipt_id.clone());
        domain.head = Some(head);
        Ok(receipt)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        let domain = lock_memory(&self.current, None)?;
        Ok(StoreStats {
            checkpoints: domain.checkpoints.len() as u64,
            segments: domain.segments.len() as u64,
            reopened_segments: domain.reopened_segments,
            gc_receipts: domain.receipts.len() as u64,
        })
    }
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
        Err(TryLockError::Poisoned(error)) => Err(DurableError::Substrate(error.to_string())),
    }
}

fn insert_immutable<T: Clone + PartialEq>(
    values: &mut BTreeMap<String, T>,
    value: Option<&T>,
    identity: impl Fn(&T) -> &String,
) -> DurableResult<()> {
    let Some(value) = value else { return Ok(()) };
    let id = identity(value);
    match values.get(id) {
        Some(existing) if existing == value => Ok(()),
        Some(_) => Err(DurableError::Validation(format!(
            "immutable durable object {id} has conflicting bytes"
        ))),
        None => {
            values.insert(id.clone(), value.clone());
            Ok(())
        }
    }
}

fn conflict(expected: Option<&StoreHead>, current: Option<&StoreHead>) -> DurableError {
    DurableError::Conflict {
        expected: expected.map(|head| head.revision.clone()),
        current: current.map(|head| head.revision.clone()),
    }
}

/// Restore and authenticate one checkpoint plus its bounded suffix.
pub fn restore(
    head: &StoreHead,
    mut checkpoint: impl FnMut(&str) -> DurableResult<Option<StateCheckpoint>>,
    mut segment: impl FnMut(&str) -> DurableResult<Option<StateSegment>>,
) -> DurableResult<(StoredState, u32)> {
    head.verify()?;
    let latest = checkpoint(&head.checkpoint_id)?.ok_or_else(|| {
        DurableError::NotFound(format!(
            "durable checkpoint {} does not exist",
            head.checkpoint_id
        ))
    })?;
    latest.verify()?;
    let mut checkpoints = vec![latest];
    while checkpoints
        .last()
        .is_some_and(|value| value.state.is_none())
    {
        if checkpoints.len() >= MAX_CHECKPOINT_PACKS as usize {
            return Err(DurableError::Validation(
                "checkpoint manifest lineage exceeds its reopen bound".to_owned(),
            ));
        }
        let parent_id = checkpoints
            .last()
            .and_then(|value| value.parent_checkpoint.as_deref())
            .ok_or_else(|| DurableError::Validation("checkpoint lineage has no base".to_owned()))?;
        let parent = checkpoint(parent_id)?.ok_or_else(|| {
            DurableError::NotFound(format!("durable checkpoint {parent_id} does not exist"))
        })?;
        parent.verify()?;
        checkpoints.push(parent);
    }
    checkpoints.reverse();
    if u32::try_from(checkpoints.len().saturating_sub(1))
        .map_err(|error| DurableError::Validation(error.to_string()))?
        != head.checkpoint_depth
    {
        return Err(DurableError::Validation(
            "durable head checkpoint depth does not match its manifest lineage".to_owned(),
        ));
    }
    let mut state = checkpoints[0]
        .state
        .clone()
        .ok_or_else(|| DurableError::Validation("checkpoint base has no state".to_owned()))?;
    let mut revision = checkpoints[0].revision.clone();
    let mut covered = checkpoints[0].covered_segment.clone();
    let mut reopened_segment_count = 0usize;
    for value in checkpoints.iter().skip(1) {
        let ids = segment_ids_between(
            value.covered_segment.clone(),
            covered.as_deref(),
            MAX_HOT_SEGMENTS,
            &mut segment,
        )?;
        reopened_segment_count =
            reopened_segment_count
                .checked_add(ids.len())
                .ok_or_else(|| {
                    DurableError::Validation("reopen segment count overflowed".to_owned())
                })?;
        apply_segment_ids(&ids, &mut state, &mut revision, &mut segment)?;
        if revision != value.revision {
            return Err(DurableError::Validation(
                "checkpoint revision does not match its segment pack".to_owned(),
            ));
        }
        covered.clone_from(&value.covered_segment);
    }
    let ids = segment_ids_between(
        if head.suffix_len == 0 {
            covered.clone()
        } else {
            head.suffix_head.clone()
        },
        covered.as_deref(),
        MAX_HOT_SEGMENTS,
        &mut segment,
    )?;
    if ids.len() != head.suffix_len as usize {
        return Err(DurableError::Validation(
            "durable head suffix length does not match its lineage".to_owned(),
        ));
    }
    reopened_segment_count = reopened_segment_count
        .checked_add(ids.len())
        .ok_or_else(|| DurableError::Validation("reopen segment count overflowed".to_owned()))?;
    apply_segment_ids(&ids, &mut state, &mut revision, &mut segment)?;
    state.validate()?;
    if revision != head.revision {
        return Err(DurableError::Validation(
            "durable head revision does not match checkpoint plus suffix".to_owned(),
        ));
    }
    let reopened_segments = u32::try_from(reopened_segment_count)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    Ok((
        StoredState {
            revision: head.revision.clone(),
            state,
            head: head.clone(),
            checkpoint_covered_segment: covered,
        },
        reopened_segments,
    ))
}

fn segment_ids_between(
    start: Option<String>,
    stop: Option<&str>,
    bound: u32,
    segment: &mut impl FnMut(&str) -> DurableResult<Option<StateSegment>>,
) -> DurableResult<Vec<String>> {
    let mut reverse = Vec::new();
    let mut cursor = start;
    while cursor.as_deref() != stop {
        if reverse.len() >= bound as usize {
            return Err(DurableError::Validation(
                "durable suffix exceeds the bounded reopen limit".to_owned(),
            ));
        }
        let id = cursor.ok_or_else(|| {
            DurableError::Validation("durable suffix does not connect to its checkpoint".to_owned())
        })?;
        let value = segment(&id)?.ok_or_else(|| {
            DurableError::NotFound(format!("durable segment {id} does not exist"))
        })?;
        value.verify()?;
        cursor = value.parent_segment;
        reverse.push(id);
    }
    reverse.reverse();
    Ok(reverse)
}

fn apply_segment_ids(
    ids: &[String],
    state: &mut DurableState,
    revision: &mut String,
    segment: &mut impl FnMut(&str) -> DurableResult<Option<StateSegment>>,
) -> DurableResult<()> {
    for id in ids {
        let value = segment(id)?.ok_or_else(|| {
            DurableError::NotFound(format!("durable segment {id} does not exist"))
        })?;
        if value.base_revision != *revision {
            return Err(DurableError::Validation(format!(
                "durable segment {id} revision chain is discontinuous"
            )));
        }
        value.delta.apply(state)?;
        revision.clone_from(&value.revision);
    }
    Ok(())
}
