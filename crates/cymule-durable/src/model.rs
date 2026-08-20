use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ArtifactRef, Machine, MachineCompaction, MachineSnapshot, Operation, Region, canonical_digest,
    content_id,
};
use serde::{Deserialize, Serialize};

use crate::{DurableError, DurableResult};

/// Durable profile state version.
pub const DURABLE_STATE_VERSION: &str = "cymule.durable-state/2";
/// Identified external wait activation version.
pub const WAIT_ACTIVATION_VERSION: &str = "cymule.wait-activation/1";
/// Canonical Machine history-compaction receipt version.
pub const HISTORY_COMPACTION_VERSION: &str = "cymule.history-compaction/1";

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
    /// Admitted signal and timer activations keyed by external activation ID.
    ///
    /// The record is the consume-once and idempotency authority for substrate
    /// redelivery. A concrete signal or timer plugin may deliver the same
    /// activation repeatedly, but it cannot reinterpret its source, targets,
    /// or result.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub wait_activations: BTreeMap<String, WaitActivation>,
    /// Fenced authority leases keyed by coordination resource.
    pub leases: BTreeMap<String, AuthorityLease>,
    /// Effect dispatch outbox keyed by structural intent ID.
    pub outbox: BTreeMap<String, EffectDispatch>,
    /// Canonical component results keyed by occurrence ID.
    pub component_occurrences: BTreeMap<String, ComponentOccurrence>,
    /// Portable snapshots keyed by snapshot ID.
    pub snapshots: BTreeMap<String, SnapshotRecord>,
    /// Idempotent canonical Event-prefix compactions keyed by command identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub history_compactions: BTreeMap<String, HistoryCompactionReceipt>,
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
            wait_activations: BTreeMap::new(),
            leases: BTreeMap::new(),
            outbox: BTreeMap::new(),
            component_occurrences: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            history_compactions: BTreeMap::new(),
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
        let machine = Machine::restore(self.machine.clone())?;
        for (run_id, continuation) in &self.continuations {
            if &continuation.run_id != run_id {
                return Err(DurableError::Validation(format!(
                    "continuation key {run_id} does not match its Run"
                )));
            }
            validate_continuation_artifacts(&machine, continuation)?;
        }
        for (wait_id, wait) in &self.waits {
            if &wait.wait_id != wait_id {
                return Err(DurableError::Validation(format!(
                    "wait key {wait_id} does not match its identity"
                )));
            }
            let continuation = self.continuations.get(&wait.run_id).ok_or_else(|| {
                DurableError::Validation(format!(
                    "wait {wait_id} references missing continuation {}",
                    wait.run_id
                ))
            })?;
            validate_wait_artifacts(&machine, continuation, wait)?;
        }
        let mut activated_waits = BTreeSet::new();
        for (activation_id, activation) in &self.wait_activations {
            if &activation.activation_id != activation_id {
                return Err(DurableError::Validation(format!(
                    "wait activation key {activation_id} does not match its identity"
                )));
            }
            activation.verify()?;
            require_artifact(
                &machine,
                &activation.result,
                &format!("wait activation {activation_id} result"),
            )?;
            let mut consume_once_targets = 0usize;
            for wait_id in &activation.wait_ids {
                if !activated_waits.insert(wait_id) {
                    return Err(DurableError::Validation(format!(
                        "wait {wait_id} is completed by more than one activation"
                    )));
                }
                let wait = self.waits.get(wait_id).ok_or_else(|| {
                    DurableError::Validation(format!(
                        "wait activation {activation_id} references missing wait {wait_id}"
                    ))
                })?;
                activation.source.ensure_matches(wait)?;
                if wait.state != WaitState::Completed
                    || wait.result.as_ref() != Some(&activation.result)
                {
                    return Err(DurableError::Validation(format!(
                        "wait activation {activation_id} is not reflected by completed wait {wait_id}"
                    )));
                }
                let continuation = self.continuations.get(&wait.run_id).ok_or_else(|| {
                    DurableError::Validation(format!(
                        "wait activation {activation_id} references missing continuation {}",
                        wait.run_id
                    ))
                })?;
                if continuation.wait_set.contains(wait_id) {
                    return Err(DurableError::Validation(format!(
                        "wait activation {activation_id} left completed wait {wait_id} pending on its Continuation"
                    )));
                }
                if wait.consume_once {
                    consume_once_targets += 1;
                }
            }
            activation
                .source
                .validate_target_cardinality(activation.wait_ids.len(), consume_once_targets)?;
        }
        for (resource, lease) in &self.leases {
            if &lease.resource != resource {
                return Err(DurableError::Validation(format!(
                    "lease key {resource} does not match its resource"
                )));
            }
        }
        for (intent_id, dispatch) in &self.outbox {
            if &dispatch.intent_id != intent_id {
                return Err(DurableError::Validation(format!(
                    "outbox key {intent_id} does not match its Effect intent"
                )));
            }
            validate_dispatch_artifacts(&machine, dispatch)?;
        }
        for (occurrence_id, occurrence) in &self.component_occurrences {
            if &occurrence.occurrence_id != occurrence_id {
                return Err(DurableError::Validation(format!(
                    "component occurrence key {occurrence_id} does not match its identity"
                )));
            }
            require_artifact(
                &machine,
                &occurrence.input,
                &format!("component occurrence {occurrence_id} input"),
            )?;
            require_artifact(
                &machine,
                &occurrence.output,
                &format!("component occurrence {occurrence_id} output"),
            )?;
        }
        for (snapshot_id, snapshot) in &self.snapshots {
            if &snapshot.snapshot_id != snapshot_id {
                return Err(DurableError::Validation(format!(
                    "snapshot key {snapshot_id} does not match its identity"
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
        let mut compactions: Vec<&HistoryCompactionReceipt> =
            self.history_compactions.values().collect();
        compactions.sort_by_key(|receipt| receipt.result.compacted_events);
        let mut expected_parent = None;
        let mut previous_count = 0;
        for receipt in &compactions {
            receipt.verify()?;
            if self.history_compactions.get(&receipt.compaction_id) != Some(*receipt) {
                return Err(DurableError::Validation(format!(
                    "history compaction key {} does not match its receipt",
                    receipt.compaction_id
                )));
            }
            if receipt.parent_compaction != expected_parent
                || receipt.result.compacted_events <= previous_count
            {
                return Err(DurableError::Validation(
                    "history compaction lineage is discontinuous".to_owned(),
                ));
            }
            expected_parent = Some(receipt.compaction_id.clone());
            previous_count = receipt.result.compacted_events;
        }
        if let Some(latest) = compactions.last() {
            let base = self.machine.base.as_ref().ok_or_else(|| {
                DurableError::Validation(
                    "history compaction exists without a Machine base snapshot".to_owned(),
                )
            })?;
            let base_id = content_id("cymule.machine-base/1", base)?;
            let compacted_events = u64::try_from(base.compacted_events.len())
                .map_err(|error| DurableError::Validation(error.to_string()))?;
            let retained_events = u64::try_from(self.machine.events.len())
                .map_err(|error| DurableError::Validation(error.to_string()))?;
            if latest.result.base_id != base_id
                || latest.result.compacted_events != compacted_events
                || latest.result.retained_events > retained_events
                || latest.result.projection_digest != base.projection_digest
            {
                return Err(DurableError::Validation(
                    "latest history compaction does not match the Machine snapshot".to_owned(),
                ));
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

fn validate_continuation_artifacts(
    machine: &Machine,
    continuation: &Continuation,
) -> DurableResult<()> {
    match continuation.status {
        ContinuationStatus::Ready
        | ContinuationStatus::Waiting
        | ContinuationStatus::Running
        | ContinuationStatus::Completed => {}
    }
    if let Some(state) = &continuation.state {
        require_artifact(
            machine,
            state,
            &format!("Continuation {} state", continuation.run_id),
        )?;
    }
    for (index, frame) in continuation.frames.iter().enumerate() {
        require_artifact(
            machine,
            &frame.input,
            &format!("Continuation {} frame {index} input", continuation.run_id),
        )?;
        for (name, local) in &frame.locals {
            require_artifact(
                machine,
                local,
                &format!(
                    "Continuation {} frame {index} local {name}",
                    continuation.run_id
                ),
            )?;
        }
    }
    Ok(())
}

fn validate_wait_artifacts(
    machine: &Machine,
    continuation: &Continuation,
    wait: &WaitCondition,
) -> DurableResult<()> {
    match &wait.kind {
        WaitKind::Signal { key } if key.is_empty() => {
            return Err(DurableError::Validation(format!(
                "wait {} signal key must not be empty",
                wait.wait_id
            )));
        }
        WaitKind::Timer { timer_id } if timer_id.is_empty() => {
            return Err(DurableError::Validation(format!(
                "wait {} timer identity must not be empty",
                wait.wait_id
            )));
        }
        WaitKind::Input { correlation, .. } if correlation.is_empty() => {
            return Err(DurableError::Validation(format!(
                "wait {} input correlation must not be empty",
                wait.wait_id
            )));
        }
        WaitKind::Signal { .. } | WaitKind::Timer { .. } | WaitKind::Input { .. } => {}
    }
    match wait.state {
        WaitState::Pending | WaitState::Completed | WaitState::Cancelled => {}
    }
    if let Some(result) = &wait.result {
        require_artifact(machine, result, &format!("wait {} result", wait.wait_id))?;
    }
    let owner = &wait.owner;
    if owner.invocation_id.is_empty()
        || owner.definition_id.is_empty()
        || owner.site_id.is_empty()
        || owner.bind.as_ref().is_some_and(String::is_empty)
    {
        return Err(DurableError::Validation(format!(
            "wait {} owner is incomplete",
            wait.wait_id
        )));
    }
    let frame = continuation.frames.iter().find(|frame| {
        frame.invocation_id == owner.invocation_id
            && frame.definition_id == owner.definition_id
            && frame.region_path == owner.region_path
    });
    let plan = machine.plan(&continuation.plan_id).ok_or_else(|| {
        DurableError::Validation(format!(
            "wait {} owning Plan {} is missing",
            wait.wait_id, continuation.plan_id
        ))
    })?;
    let definition = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == owner.definition_id)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "wait {} owning definition {} is missing",
                wait.wait_id, owner.definition_id
            ))
        })?;
    let region = region_at_path(&definition.body, &owner.region_path)?;
    let step = region.steps.get(owner.step_index).ok_or_else(|| {
        DurableError::Validation(format!("wait {} owning step is missing", wait.wait_id))
    })?;
    if step.id != owner.site_id
        || !matches!(&step.operation, Operation::Wait { bind, .. } if bind == &owner.bind)
    {
        return Err(DurableError::Validation(format!(
            "wait {} owner does not match its Plan site",
            wait.wait_id
        )));
    }
    match (wait.state, wait.result.as_ref(), frame, owner.bind.as_ref()) {
        (WaitState::Pending, None, Some(frame), bind)
            if frame.next_step == owner.step_index + 1
                && bind.is_none_or(|bind| !frame.locals.contains_key(bind)) => {}
        (WaitState::Completed, Some(result), Some(frame), Some(bind))
            if frame.next_step > owner.step_index && frame.locals.get(bind) == Some(result) => {}
        (WaitState::Completed, Some(_), Some(frame), None)
            if frame.next_step > owner.step_index => {}
        (WaitState::Completed, Some(_), None, _) => {}
        (WaitState::Cancelled, None, Some(frame), bind)
            if frame.next_step > owner.step_index
                && bind.is_none_or(|bind| !frame.locals.contains_key(bind)) => {}
        (WaitState::Cancelled, None, None, _) => {}
        _ => {
            return Err(DurableError::Validation(format!(
                "wait {} owner is not reflected by its frame",
                wait.wait_id
            )));
        }
    }
    Ok(())
}

fn region_at_path<'a>(root: &'a Region, path: &[usize]) -> DurableResult<&'a Region> {
    let mut region = root;
    for index in path {
        let step = region.steps.get(*index).ok_or_else(|| {
            DurableError::Validation("wait result binding Region path is invalid".to_owned())
        })?;
        let Operation::Scope { body, .. } = &step.operation else {
            return Err(DurableError::Validation(
                "wait result binding Region path crosses a non-scope step".to_owned(),
            ));
        };
        region = body;
    }
    Ok(region)
}

fn validate_dispatch_artifacts(machine: &Machine, dispatch: &EffectDispatch) -> DurableResult<()> {
    match dispatch.state {
        OutboxState::Pending
        | OutboxState::Claimed
        | OutboxState::Applied
        | OutboxState::NotApplied
        | OutboxState::Unknown => {}
    }
    require_artifact(
        machine,
        &dispatch.input,
        &format!("Effect intent {} input", dispatch.intent_id),
    )?;
    if let Some(result) = &dispatch.result {
        require_artifact(
            machine,
            result,
            &format!("Effect intent {} result", dispatch.intent_id),
        )?;
    }
    Ok(())
}

fn require_artifact(machine: &Machine, reference: &ArtifactRef, owner: &str) -> DurableResult<()> {
    reference
        .validate()
        .map_err(|error| DurableError::Validation(format!("{owner}: {error}")))?;
    if machine.artifact(reference).is_none() {
        return Err(DurableError::Validation(format!(
            "{owner} Artifact {} is missing from the canonical Machine",
            reference.artifact_id
        )));
    }
    Ok(())
}

/// One idempotent M1 Event-prefix compaction receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCompactionReceipt {
    /// Receipt schema and semantic version.
    pub compaction_version: String,
    /// Stable command/idempotency identity.
    pub compaction_id: String,
    /// Previous compaction in the cumulative base lineage.
    pub parent_compaction: Option<String>,
    /// Durable revision from which compaction was computed.
    pub source_revision: String,
    /// Requested full suffix length.
    pub requested_suffix: u64,
    /// Canonical Machine compaction evidence.
    pub result: MachineCompaction,
}

impl HistoryCompactionReceipt {
    /// Verify stable identities and bounded result metadata.
    pub fn verify(&self) -> DurableResult<()> {
        if self.compaction_version != HISTORY_COMPACTION_VERSION
            || self.compaction_id.is_empty()
            || self.source_revision.len() != 64
            || !self
                .source_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || !self.result.base_id.starts_with("sha256:")
            || self.result.compacted_events == 0
            || self.result.retained_events != self.requested_suffix
            || self.result.causal_frontier.is_empty()
        {
            return Err(DurableError::Validation(
                "history compaction receipt is malformed".to_owned(),
            ));
        }
        Ok(())
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
    /// Definition resolved inside the immutable Plan.
    pub definition_id: String,
    /// Structural materialized invocation identity.
    pub invocation_id: String,
    /// Entry-rooted invoke path proving the dynamic invocation.
    pub invocation_path: Vec<cymule_core::InvocationPathSegment>,
    /// Typed invocation input Artifact.
    pub input: ArtifactRef,
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
    /// Exact Plan frame and site that owns this wait.
    pub owner: WaitOwner,
    /// Wait lifecycle.
    pub state: WaitState,
    /// Completion artifact when resolved.
    pub result: Option<ArtifactRef>,
}

/// Exact Plan frame and site that owns one durable wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitOwner {
    /// Structural invocation that owns the local.
    pub invocation_id: String,
    /// Definition containing the wait operation.
    pub definition_id: String,
    /// Stable wait operation site.
    pub site_id: String,
    /// Nested Region path within the definition.
    pub region_path: Vec<usize>,
    /// Wait step index within that Region.
    pub step_index: usize,
    /// Optional local binding name declared by the wait operation.
    pub bind: Option<String>,
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

/// One externally identified signal or timer delivery admitted by M1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitActivation {
    /// Activation schema and semantic version.
    pub activation_version: String,
    /// Stable external delivery identity used for redelivery deduplication.
    pub activation_id: String,
    /// Expected signal or timer source.
    pub source: WaitActivationSource,
    /// Exact pending waits selected by a scheduler or parked-wait index.
    pub wait_ids: BTreeSet<String>,
    /// Immutable typed completion result shared by every selected wait.
    pub result: ArtifactRef,
}

impl WaitActivation {
    /// Construct and validate an identified wait activation.
    pub fn new(
        activation_id: impl Into<String>,
        source: WaitActivationSource,
        wait_ids: BTreeSet<String>,
        result: ArtifactRef,
    ) -> DurableResult<Self> {
        let activation = Self {
            activation_version: WAIT_ACTIVATION_VERSION.to_owned(),
            activation_id: activation_id.into(),
            source,
            wait_ids,
            result,
        };
        activation.verify()?;
        Ok(activation)
    }

    /// Validate versioned shape independently of the referenced durable state.
    pub fn verify(&self) -> DurableResult<()> {
        if self.activation_version != WAIT_ACTIVATION_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported wait activation version {:?}",
                self.activation_version
            )));
        }
        if self.activation_id.is_empty() {
            return Err(DurableError::Validation(
                "wait activation identity must not be empty".to_owned(),
            ));
        }
        if self.wait_ids.is_empty() {
            return Err(DurableError::Validation(
                "wait activation must target at least one wait".to_owned(),
            ));
        }
        if self.wait_ids.iter().any(String::is_empty) {
            return Err(DurableError::Validation(
                "wait activation target identity must not be empty".to_owned(),
            ));
        }
        self.result
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        self.source.verify()
    }
}

/// Provider-neutral source identity for one external wait activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitActivationSource {
    /// One durable signal delivery under a correlation key.
    Signal {
        /// Correlation key declared by the waiting Plan.
        key: String,
    },
    /// One logical timer firing supplied by a clock plugin.
    Timer {
        /// Stable timer identity declared by the waiting Plan.
        timer_id: String,
    },
}

impl WaitActivationSource {
    /// Validate the closed source kind and its declared identity.
    pub fn verify(&self) -> DurableResult<()> {
        let identity = match self {
            Self::Signal { key } => key,
            Self::Timer { timer_id } => timer_id,
        };
        if identity.is_empty() {
            return Err(DurableError::Validation(
                "wait activation source identity must not be empty".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_matches(&self, wait: &WaitCondition) -> DurableResult<()> {
        let matches = match (self, &wait.kind) {
            (Self::Signal { key }, WaitKind::Signal { key: expected }) => key == expected,
            (Self::Timer { timer_id }, WaitKind::Timer { timer_id: expected }) => {
                timer_id == expected
            }
            _ => false,
        };
        if !matches {
            return Err(DurableError::Validation(format!(
                "activation source does not match wait {}",
                wait.wait_id
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_target_cardinality(
        &self,
        target_count: usize,
        consume_once_targets: usize,
    ) -> DurableResult<()> {
        match self {
            Self::Signal { .. } if consume_once_targets <= 1 => Ok(()),
            Self::Signal { .. } => Err(DurableError::Validation(
                "one signal activation cannot consume more than one consume-once wait".to_owned(),
            )),
            Self::Timer { .. } if target_count == 1 => Ok(()),
            Self::Timer { .. } => Err(DurableError::Validation(
                "one timer activation must target exactly one wait".to_owned(),
            )),
        }
    }
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
