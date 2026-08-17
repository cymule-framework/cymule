use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRef, MachineSnapshot, canonical_digest};
use serde::{Deserialize, Serialize};

use crate::{DurableError, DurableResult};

/// Durable profile state version.
pub const DURABLE_STATE_VERSION: &str = "cymule.durable-state/1";

/// Complete provider-neutral state committed by one single-domain CAS write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableState {
    /// Durable state schema version.
    pub durable_version: String,
    /// Canonical semantic-machine inputs.
    pub machine: MachineSnapshot,
    /// Resumable continuation per Run.
    pub continuations: BTreeMap<String, Continuation>,
    /// Wait registrations keyed by stable wait ID.
    pub waits: BTreeMap<String, WaitCondition>,
    /// Fenced authority leases keyed by coordination resource.
    pub leases: BTreeMap<String, AuthorityLease>,
    /// Effect dispatch outbox keyed by structural intent ID.
    pub outbox: BTreeMap<String, EffectDispatch>,
    /// Canonical component results keyed by occurrence ID.
    pub component_occurrences: BTreeMap<String, ComponentOccurrence>,
    /// Portable snapshots keyed by snapshot ID.
    pub snapshots: BTreeMap<String, SnapshotRecord>,
    /// Typed application journals keyed by stable journal identity.
    ///
    /// This extension seam lets higher profiles share the same CAS authority
    /// without teaching M1 their domain types. Each record is self-validating,
    /// and the owning profile must validate its typed payload while replaying.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub application_journals: BTreeMap<String, Vec<JournalRecord>>,
}

impl DurableState {
    /// Construct an empty state around a semantic Machine snapshot.
    pub fn new(machine: MachineSnapshot) -> Self {
        Self {
            durable_version: DURABLE_STATE_VERSION.to_owned(),
            machine,
            continuations: BTreeMap::new(),
            waits: BTreeMap::new(),
            leases: BTreeMap::new(),
            outbox: BTreeMap::new(),
            component_occurrences: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            application_journals: BTreeMap::new(),
        }
    }

    /// Validate references and stable identities before persistence.
    pub fn validate(&self) -> DurableResult<()> {
        if self.durable_version != DURABLE_STATE_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable state version {:?}",
                self.durable_version
            )));
        }
        cymule_core::Machine::restore(self.machine.clone())?;
        for (run_id, continuation) in &self.continuations {
            if &continuation.run_id != run_id {
                return Err(DurableError::Validation(format!(
                    "continuation key {run_id} does not match its Run"
                )));
            }
        }
        for (wait_id, wait) in &self.waits {
            if &wait.wait_id != wait_id {
                return Err(DurableError::Validation(format!(
                    "wait key {wait_id} does not match its identity"
                )));
            }
        }
        for (journal_id, records) in &self.application_journals {
            if journal_id.is_empty() {
                return Err(DurableError::Validation(
                    "application journal identity must not be empty".to_owned(),
                ));
            }
            let mut record_ids = BTreeSet::new();
            for record in records {
                record.verify()?;
                if !record_ids.insert(&record.record_id) {
                    return Err(DurableError::Validation(format!(
                        "application journal {journal_id} repeats record {}",
                        record.record_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Canonical revision digest used by stores for compare-and-swap.
    pub fn revision(&self) -> DurableResult<String> {
        self.validate()?;
        canonical_digest(self).map_err(Into::into)
    }
}

/// Self-validating typed record stored in one higher-profile journal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalRecord {
    /// Stable idempotency identity within the owning journal.
    pub record_id: String,
    /// Versioned payload schema or semantic domain.
    pub schema: String,
    /// Typed payload encoded as canonical JSON.
    pub payload: serde_json::Value,
    /// Canonical digest of `schema` and `payload`.
    pub content_digest: String,
}

/// One journal and its records in an atomic multi-journal checkpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalBatch {
    /// Stable higher-profile journal identity.
    pub journal_id: String,
    /// Ordered idempotent records to append.
    pub records: Vec<JournalRecord>,
}

impl JournalRecord {
    /// Construct a record with a verified content digest.
    pub fn new(
        record_id: impl Into<String>,
        schema: impl Into<String>,
        payload: serde_json::Value,
    ) -> DurableResult<Self> {
        let schema = schema.into();
        let content_digest = canonical_digest(&(schema.as_str(), &payload))?;
        let record = Self {
            record_id: record_id.into(),
            schema,
            payload,
            content_digest,
        };
        record.verify()?;
        Ok(record)
    }

    /// Verify identity, schema, and content digest.
    pub fn verify(&self) -> DurableResult<()> {
        if self.record_id.is_empty() {
            return Err(DurableError::Validation(
                "journal record identity must not be empty".to_owned(),
            ));
        }
        if self.schema.is_empty() {
            return Err(DurableError::Validation(
                "journal record schema must not be empty".to_owned(),
            ));
        }
        let expected = canonical_digest(&(self.schema.as_str(), &self.payload))?;
        if self.content_digest != expected {
            return Err(DurableError::Validation(format!(
                "journal record {} digest does not match its payload",
                self.record_id
            )));
        }
        Ok(())
    }
}

/// State plus its store-owned revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredState {
    /// Canonical revision of `state`.
    pub revision: String,
    /// Complete committed state.
    pub state: DurableState,
}

impl StoredState {
    /// Validate that the revision matches the complete state.
    pub fn verify(&self) -> DurableResult<()> {
        let expected = self.state.revision()?;
        if self.revision != expected {
            return Err(DurableError::Validation(format!(
                "stored revision {} does not match {expected}",
                self.revision
            )));
        }
        Ok(())
    }
}

/// Complete versioned effectful continuation at a semantic safe point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    /// Stable Run identity.
    pub run_id: String,
    /// Current Plan identity.
    pub plan_id: String,
    /// Future-default Binding Context.
    pub binding_context: String,
    /// Logical interpreter stack.
    pub frames: Vec<FrameState>,
    /// Current typed state artifact, when present.
    pub state: Option<ArtifactRef>,
    /// Active wait IDs.
    pub wait_set: BTreeSet<String>,
    /// Open scope stack from root to current.
    pub scope_stack: Vec<String>,
    /// Effect obligations carried beyond scope closure.
    pub effect_obligations: BTreeSet<String>,
    /// Authority lease resources required to resume.
    pub authority_leases: BTreeSet<String>,
    /// Provider-neutral budget balances.
    pub budget: BTreeMap<String, u64>,
    /// Maximal causal frontier.
    pub causal_frontier: BTreeSet<String>,
    /// Attempt fencing epoch.
    pub epoch: u64,
    /// Continuation lifecycle.
    pub status: ContinuationStatus,
}

/// Logical interpreter frame without process-memory references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameState {
    /// Definition or invocation identity.
    pub invocation_id: String,
    /// Nested region indices from the definition root.
    pub region_path: Vec<usize>,
    /// Next stable step index.
    pub next_step: usize,
    /// Typed local bindings stored as immutable artifacts.
    pub locals: BTreeMap<String, ArtifactRef>,
}

/// Continuation lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationStatus {
    /// Ready for a fenced Attempt.
    Ready,
    /// Blocked on one or more durable waits.
    Waiting,
    /// An Attempt currently holds the continuation.
    Running,
    /// Terminal state has been committed.
    Completed,
}

/// Durable wait registration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitCondition {
    /// Stable wait identity.
    pub wait_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Wait semantics.
    pub kind: WaitKind,
    /// Whether only one completion may win.
    pub consume_once: bool,
    /// Wait lifecycle.
    pub state: WaitState,
    /// Completion artifact when resolved.
    pub result: Option<ArtifactRef>,
}

/// Provider-neutral wait kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitKind {
    /// External signal key.
    Signal {
        /// Correlation key supplied by an external signal producer.
        key: String,
    },
    /// Logical timer identity resolved by a clock substrate.
    Timer {
        /// Stable timer identity.
        timer_id: String,
    },
    /// Typed user or external input.
    Input {
        /// Stable input correlation key.
        correlation: String,
        /// JSON Schema for the completed input artifact.
        schema: serde_json::Value,
    },
}

/// Wait lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitState {
    /// Completion may still be admitted.
    Pending,
    /// One authoritative completion was recorded.
    Completed,
    /// The wait was cancelled before completion.
    Cancelled,
}

/// Fenced authority lease. Time values are supplied by a Clock substrate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityLease {
    /// Coordinated resource key.
    pub resource: String,
    /// Current lease owner.
    pub owner: String,
    /// Monotonically increasing fencing epoch.
    pub epoch: u64,
    /// Logical expiry supplied by a Clock substrate.
    pub expires_at: u64,
}

/// Durable effect outbox entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectDispatch {
    /// Structural effect intent identity.
    pub intent_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Abstract effect operation.
    pub operation: String,
    /// Immutable input artifact.
    pub input: ArtifactRef,
    /// Pinned adapter binding.
    pub occurrence_binding: String,
    /// Outbox lifecycle state.
    pub state: OutboxState,
    /// Fencing epoch of the current claim.
    pub claim_epoch: u64,
    /// Current claim owner.
    pub claim_owner: Option<String>,
    /// Optional authoritative result artifact.
    pub result: Option<ArtifactRef>,
}

/// Effect outbox lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxState {
    /// Ready to be claimed.
    Pending,
    /// Held by one fenced dispatcher.
    Claimed,
    /// External application was observed.
    Applied,
    /// External non-application was observed.
    NotApplied,
    /// Dispatch occurred but the world result is ambiguous.
    Unknown,
}

/// Recorded nondeterministic component result for exact execution replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentOccurrence {
    /// Structural component occurrence identity.
    pub occurrence_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Stable Plan site.
    pub site_id: String,
    /// Abstract component operation.
    pub component: String,
    /// Canonical input artifact.
    pub input: ArtifactRef,
    /// Canonical output artifact.
    pub output: ArtifactRef,
    /// Pinned implementation binding.
    pub occurrence_binding: String,
    /// Concrete implementation revision.
    pub implementation_revision: String,
}

/// Portable checkpoint/savepoint metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRecord {
    /// Content-addressed snapshot identity.
    pub snapshot_id: String,
    /// Durable revision summarized by the snapshot.
    pub source_revision: String,
    /// Causally closed frontier.
    pub causal_frontier: BTreeSet<String>,
    /// Continuations included in the snapshot.
    pub continuation_ids: BTreeSet<String>,
    /// Obligations that must survive compaction.
    pub unresolved_obligations: BTreeSet<String>,
    /// Historical bindings required for interpretation.
    pub occurrence_bindings: BTreeSet<String>,
}
