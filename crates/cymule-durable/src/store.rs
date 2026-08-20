use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, TryLockError};

use cymule_core::{canonical_digest, content_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{DurableError, DurableResult, DurableState, StoredState};

/// Small mutable storage-head schema.
pub const STORE_HEAD_VERSION: &str = "cymule.durable-head/1";
/// Immutable state-delta segment schema.
pub const STATE_SEGMENT_VERSION: &str = "cymule.durable-segment/1";
/// Authenticated projection-checkpoint schema.
pub const STATE_CHECKPOINT_VERSION: &str = "cymule.durable-checkpoint/1";
/// Cold-reclamation receipt schema.
pub const GC_RECEIPT_VERSION: &str = "cymule.durable-gc-receipt/1";
/// Maximum delta suffix length admitted by the contract.
pub const MAX_HOT_SEGMENTS: u32 = 32;

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
            || (self.suffix_len == 0) != self.suffix_head.is_none()
        {
            return Err(DurableError::Validation(
                "durable head suffix is inconsistent or exceeds its bound".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
/// Deterministic recursive JSON state delta.
pub enum JsonDelta {
    /// Replace one value.
    Replace {
        /// Exact replacement value.
        value: Value,
    },
    /// Change selected fields and remove selected keys.
    Object {
        /// Changed or inserted fields.
        fields: BTreeMap<String, JsonDelta>,
        /// Removed fields.
        removed: BTreeSet<String>,
    },
    /// Append values to an authenticated unchanged array prefix.
    Append {
        /// Required array length before append.
        base_len: u64,
        /// Values appended in order.
        values: Vec<Value>,
    },
}

impl JsonDelta {
    fn between(current: &Value, next: &Value) -> Option<Self> {
        if current == next {
            return None;
        }
        match (current, next) {
            (Value::Object(current), Value::Object(next)) => {
                let fields = next
                    .iter()
                    .filter_map(|(key, value)| match current.get(key) {
                        Some(old) => Self::between(old, value).map(|delta| (key.clone(), delta)),
                        None => Some((
                            key.clone(),
                            Self::Replace {
                                value: value.clone(),
                            },
                        )),
                    })
                    .collect();
                let removed = current
                    .keys()
                    .filter(|key| !next.contains_key(*key))
                    .cloned()
                    .collect();
                Some(Self::Object { fields, removed })
            }
            (Value::Array(current), Value::Array(next))
                if next.len() > current.len() && next.starts_with(current) =>
            {
                Some(Self::Append {
                    base_len: u64::try_from(current.len()).expect("array length fits u64"),
                    values: next[current.len()..].to_vec(),
                })
            }
            _ => Some(Self::Replace {
                value: next.clone(),
            }),
        }
    }

    fn apply(&self, target: &mut Value) -> DurableResult<()> {
        match self {
            Self::Replace { value } => *target = value.clone(),
            Self::Object { fields, removed } => {
                let object = target.as_object_mut().ok_or_else(|| {
                    DurableError::Validation("object delta targets a non-object".to_owned())
                })?;
                for key in removed {
                    if object.remove(key).is_none() {
                        return Err(DurableError::Validation(format!(
                            "object delta removes missing field {key:?}"
                        )));
                    }
                }
                for (key, delta) in fields {
                    match object.get_mut(key) {
                        Some(value) => delta.apply(value)?,
                        None => match delta {
                            Self::Replace { value } => {
                                object.insert(key.clone(), value.clone());
                            }
                            _ => {
                                return Err(DurableError::Validation(format!(
                                    "nested delta targets missing field {key:?}"
                                )));
                            }
                        },
                    }
                }
            }
            Self::Append { base_len, values } => {
                let array = target.as_array_mut().ok_or_else(|| {
                    DurableError::Validation("append delta targets a non-array".to_owned())
                })?;
                if u64::try_from(array.len())
                    .map_err(|error| DurableError::Validation(error.to_string()))?
                    != *base_len
                {
                    return Err(DurableError::Validation(
                        "append delta base length does not match".to_owned(),
                    ));
                }
                array.extend(values.iter().cloned());
            }
        }
        Ok(())
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
    pub delta: JsonDelta,
}

impl StateSegment {
    fn between(current: &StoredState, next: &DurableState) -> DurableResult<Option<Self>> {
        let current_json = serde_json::to_value(&current.state)?;
        let next_json = serde_json::to_value(next)?;
        let Some(delta) = JsonDelta::between(&current_json, &next_json) else {
            return Ok(None);
        };
        let sequence = current.head.sequence.checked_add(1).ok_or_else(|| {
            DurableError::Validation("durable segment sequence overflowed".to_owned())
        })?;
        let parent_segment = current.suffix_head_or_checkpoint_segment();
        let revision = next.revision()?;
        let segment_id = segment_identity(
            sequence,
            parent_segment.as_deref(),
            &current.revision,
            &revision,
            &delta,
        )?;
        Ok(Some(Self {
            segment_version: STATE_SEGMENT_VERSION.to_owned(),
            segment_id,
            sequence,
            parent_segment,
            base_revision: current.revision.clone(),
            revision,
            delta,
        }))
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
        if current.revision()? != self.base_revision {
            return Err(DurableError::Validation(format!(
                "segment {} base revision does not match its projection",
                self.segment_id
            )));
        }
        let mut value = serde_json::to_value(current)?;
        self.delta.apply(&mut value)?;
        let next: DurableState = serde_json::from_value(value)?;
        if next.revision()? != self.revision {
            return Err(DurableError::Validation(format!(
                "segment {} result revision does not match its delta",
                self.segment_id
            )));
        }
        Ok(next)
    }
}

fn segment_identity(
    sequence: u64,
    parent_segment: Option<&str>,
    base_revision: &str,
    revision: &str,
    delta: &JsonDelta,
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
    pub state: DurableState,
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
        let checkpoint_id = checkpoint_identity(
            parent_checkpoint.as_deref(),
            covered_segment.as_deref(),
            sequence,
            &revision,
            &state,
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
        if self.state.revision()? != self.revision {
            return Err(DurableError::Validation(
                "durable checkpoint revision does not match its projection".to_owned(),
            ));
        }
        let expected = checkpoint_identity(
            self.parent_checkpoint.as_deref(),
            self.covered_segment.as_deref(),
            self.sequence,
            &self.revision,
            &self.state,
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
    state: &DurableState,
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
    pub segment: Option<StateSegment>,
    /// Checkpoint for initialization or suffix rotation.
    pub checkpoint: Option<StateCheckpoint>,
    /// Exact next small head.
    pub head: StoreHead,
}

impl StoreBatch {
    /// Build the sequence-zero checkpoint and head.
    pub fn initialize(state: DurableState) -> DurableResult<Self> {
        let checkpoint = StateCheckpoint::new(None, None, 0, state)?;
        let head = StoreHead {
            head_version: STORE_HEAD_VERSION.to_owned(),
            revision: checkpoint.revision.clone(),
            checkpoint_id: checkpoint.checkpoint_id.clone(),
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
    pub fn transition(current: &StoredState, next: DurableState) -> DurableResult<Option<Self>> {
        let Some(segment) = StateSegment::between(current, &next)? else {
            return Ok(None);
        };
        let suffix_len = current.head.suffix_len.checked_add(1).ok_or_else(|| {
            DurableError::Validation("durable suffix length overflowed".to_owned())
        })?;
        if suffix_len >= MAX_HOT_SEGMENTS {
            let checkpoint = StateCheckpoint::new(
                Some(current.head.checkpoint_id.clone()),
                Some(segment.segment_id.clone()),
                segment.sequence,
                next,
            )?;
            let head = StoreHead {
                head_version: STORE_HEAD_VERSION.to_owned(),
                revision: segment.revision.clone(),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                suffix_head: None,
                suffix_len: 0,
                sequence: segment.sequence,
                gc_receipt: None,
            };
            Ok(Some(Self {
                segment: Some(segment),
                checkpoint: Some(checkpoint),
                head,
            }))
        } else {
            let head = StoreHead {
                head_version: STORE_HEAD_VERSION.to_owned(),
                revision: segment.revision.clone(),
                checkpoint_id: current.head.checkpoint_id.clone(),
                suffix_head: Some(segment.segment_id.clone()),
                suffix_len,
                sequence: segment.sequence,
                gc_receipt: None,
            };
            Ok(Some(Self {
                segment: Some(segment),
                checkpoint: None,
                head,
            }))
        }
    }

    /// Verify atomic contents against the exact current projection.
    pub fn verify_against(&self, current: Option<&StoredState>) -> DurableResult<DurableState> {
        self.head.verify()?;
        match current {
            None => {
                let checkpoint = self.checkpoint.as_ref().ok_or_else(|| {
                    DurableError::Validation("initialization requires a checkpoint".to_owned())
                })?;
                if self.segment.is_some() || self.head.sequence != 0 || self.head.suffix_len != 0 {
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
                Ok(checkpoint.state.clone())
            }
            Some(current) => {
                let segment = self.segment.as_ref().ok_or_else(|| {
                    DurableError::Validation("transition requires a segment".to_owned())
                })?;
                if segment.sequence != current.head.sequence + 1
                    || segment.parent_segment != current.suffix_head_or_checkpoint_segment()
                {
                    return Err(DurableError::Validation(
                        "segment lineage does not match current head".to_owned(),
                    ));
                }
                let next = segment.apply(&current.state)?;
                if self.head.revision != segment.revision || self.head.sequence != segment.sequence
                {
                    return Err(DurableError::Validation(
                        "segment does not match next head".to_owned(),
                    ));
                }
                match &self.checkpoint {
                    Some(checkpoint) => {
                        checkpoint.verify()?;
                        if checkpoint.state != next
                            || checkpoint.parent_checkpoint.as_deref()
                                != Some(current.head.checkpoint_id.as_str())
                            || checkpoint.covered_segment.as_deref()
                                != Some(segment.segment_id.as_str())
                            || self.head.checkpoint_id != checkpoint.checkpoint_id
                            || self.head.suffix_len != 0
                            || self.head.suffix_head.is_some()
                        {
                            return Err(DurableError::Validation(
                                "rotated checkpoint does not match segment and head".to_owned(),
                            ));
                        }
                    }
                    None => {
                        if self.head.checkpoint_id != current.head.checkpoint_id
                            || self.head.suffix_head.as_deref() != Some(segment.segment_id.as_str())
                            || self.head.suffix_len != current.head.suffix_len + 1
                        {
                            return Err(DurableError::Validation(
                                "hot suffix head does not match segment".to_owned(),
                            ));
                        }
                    }
                }
                Ok(next)
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
                reclaimed_objects,
            ),
        )?;
        Ok(Self {
            receipt_version: GC_RECEIPT_VERSION.to_owned(),
            receipt_id,
            revision: head.revision.clone(),
            retained_checkpoint: head.checkpoint_id.clone(),
            reclaimed_digest,
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
                self.reclaimed_objects,
            ),
        )?;
        if self.receipt_id != expected {
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
            |id| domain.checkpoints.get(id).cloned(),
            |id| domain.segments.get(id).cloned(),
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
        let current = match &domain.head {
            Some(head) => Some(
                restore(
                    head,
                    |id| domain.checkpoints.get(id).cloned(),
                    |id| domain.segments.get(id).cloned(),
                )?
                .0,
            ),
            None => None,
        };
        batch.verify_against(current.as_ref())?;
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
        let mut reclaimed = BTreeSet::new();
        domain.checkpoints.retain(|id, _| {
            let keep = id == &expected.checkpoint_id;
            if !keep {
                reclaimed.insert(id.clone());
            }
            keep
        });
        let checkpoint = domain
            .checkpoints
            .get(&expected.checkpoint_id)
            .expect("retained checkpoint exists")
            .clone();
        let suffix = suffix_ids(expected, &checkpoint, |id| domain.segments.get(id).cloned())?;
        domain.segments.retain(|id, _| {
            let keep = suffix.contains(id);
            if !keep {
                reclaimed.insert(id.clone());
            }
            keep
        });
        let receipt = GcReceipt::new(expected, &reclaimed)?;
        domain
            .receipts
            .insert(receipt.receipt_id.clone(), receipt.clone());
        let mut head = expected.clone();
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
    mut checkpoint: impl FnMut(&str) -> Option<StateCheckpoint>,
    mut segment: impl FnMut(&str) -> Option<StateSegment>,
) -> DurableResult<(StoredState, u32)> {
    head.verify()?;
    let checkpoint = checkpoint(&head.checkpoint_id).ok_or_else(|| {
        DurableError::NotFound(format!(
            "durable checkpoint {} does not exist",
            head.checkpoint_id
        ))
    })?;
    checkpoint.verify()?;
    let ids = suffix_ids(head, &checkpoint, |id| segment(id))?;
    let mut state = checkpoint.state.clone();
    for id in &ids {
        let value = segment(id).ok_or_else(|| {
            DurableError::NotFound(format!("durable segment {id} does not exist"))
        })?;
        state = value.apply(&state)?;
    }
    if state.revision()? != head.revision {
        return Err(DurableError::Validation(
            "durable head revision does not match checkpoint plus suffix".to_owned(),
        ));
    }
    let reopened_segments =
        u32::try_from(ids.len()).map_err(|error| DurableError::Validation(error.to_string()))?;
    Ok((
        StoredState {
            revision: head.revision.clone(),
            state,
            head: head.clone(),
            checkpoint_covered_segment: checkpoint.covered_segment,
        },
        reopened_segments,
    ))
}

fn suffix_ids(
    head: &StoreHead,
    checkpoint: &StateCheckpoint,
    mut segment: impl FnMut(&str) -> Option<StateSegment>,
) -> DurableResult<Vec<String>> {
    if head.suffix_len == 0 {
        return Ok(Vec::new());
    }
    let mut reverse = Vec::new();
    let mut cursor = head.suffix_head.clone();
    while cursor.as_deref() != checkpoint.covered_segment.as_deref() {
        if reverse.len() >= MAX_HOT_SEGMENTS as usize {
            return Err(DurableError::Validation(
                "durable suffix exceeds the bounded reopen limit".to_owned(),
            ));
        }
        let id = cursor.ok_or_else(|| {
            DurableError::Validation("durable suffix does not connect to its checkpoint".to_owned())
        })?;
        let value = segment(&id).ok_or_else(|| {
            DurableError::NotFound(format!("durable segment {id} does not exist"))
        })?;
        value.verify()?;
        cursor = value.parent_segment;
        reverse.push(id);
    }
    reverse.reverse();
    if reverse.len() != head.suffix_len as usize {
        return Err(DurableError::Validation(
            "durable head suffix length does not match its lineage".to_owned(),
        ));
    }
    Ok(reverse)
}
