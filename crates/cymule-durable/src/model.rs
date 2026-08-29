use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ops::Index,
};

use cymule_core::{
    ArtifactRecord, ArtifactRef, Command, EffectTransition, Expression, Machine, MachineSnapshot,
    Operation, ReconciliationResolution, Region, SealedPlan, WorldOutcome, canonical_digest,
    content_id, validate_failure_code,
};
use cymule_profile_protocol::agent::{
    self as agent_protocol, AgentCommand, AgentCommandAction, AgentWorkspaceCommand,
    AgentWorkspaceCommandPhase, WorkspaceScopeCheckpoint,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::{DurableError, DurableResult};
pub(crate) use cymule_durable_protocol::{
    Continuation, ContinuationStatus, WaitActivationReceipt, WaitActivationSource, WaitOwner,
    continuation_id,
};

/// Durable profile state version.
pub const DURABLE_STATE_VERSION: &str = "cymule.durable-state/7";
/// Durable provider-attempt version.
pub const OPERATION_ATTEMPT_VERSION: &str = "cymule.operation-attempt/2";
/// Durable legacy component-occurrence version.
pub const COMPONENT_OCCURRENCE_VERSION: &str = "cymule.component-occurrence/4";
/// Canonical Machine history-compaction receipt version.
pub const HISTORY_COMPACTION_VERSION: &str = "cymule.history-compaction/2";
/// Constant-size descriptor for one authenticated application-journal prefix.
pub const APPLICATION_JOURNAL_PREFIX_VERSION: &str = "cymule.application-journal-prefix/1";
/// Immutable receipt for one authenticated application-journal prefix replacement.
pub const APPLICATION_JOURNAL_PREFIX_REPLACEMENT_RECEIPT_VERSION: &str =
    "cymule.application-journal-prefix-replacement-receipt/2";
/// Payload-free cumulative authority for an application-journal prefix replacement.
pub const APPLICATION_JOURNAL_PREFIX_REPLACEMENT_AUTHORITY_VERSION: &str =
    "cymule.application-journal-prefix-replacement-authority/2";
/// Immutable receipt for one higher-profile journal plus M1 wait/effect CAS.
pub const COUPLED_CHECKPOINT_RECEIPT_VERSION: &str = "cymule.coupled-checkpoint-receipt/3";
/// Immutable exact receipt binding an Agent suspension to an existing M1 input Wait.
pub(crate) const AGENT_INPUT_SUSPENSION_RECEIPT_VERSION: &str =
    "cymule.agent-input-suspension-receipt/1";
/// Immutable exact receipt binding an Agent response to the completed M1 input Wait.
pub(crate) const AGENT_INPUT_COMPLETION_RECEIPT_VERSION: &str =
    "cymule.agent-input-completion-receipt/1";
/// Independent canonical byte bound for a payload-free coupled receipt.
pub const MAX_COUPLED_CHECKPOINT_RECEIPT_BYTES: usize = 1024 * 1024;
/// Agent workspace receipts may retain one complete bounded Continuation plus
/// their exact single-Effect neighborhood, within one physical `StateRoot` leaf.
pub const MAX_AGENT_WORKSPACE_CHECKPOINT_RECEIPT_BYTES: usize = crate::MAX_STATE_ROOT_LEAF_BYTES;
const COUPLED_CHECKPOINT_KEY_DOMAIN: &str = "cymule.coupled-checkpoint-key/1";
const AGENT_INPUT_SUSPENSION_KEY_DOMAIN: &str = "cymule.agent-input-suspension-key/1";
const AGENT_INPUT_COMPLETION_KEY_DOMAIN: &str = "cymule.agent-input-completion-key/1";
/// Maximum canonical bytes admitted for one application-journal record.
pub const MAX_APPLICATION_JOURNAL_RECORD_BYTES: usize = 4 * 1024 * 1024;
/// Maximum typed base records admitted by one prefix replacement.
pub const MAX_APPLICATION_JOURNAL_REPLACEMENT_RECORDS: usize = 16;
pub(crate) const COMPONENT_INPUT_ARTIFACT_KIND: &str = "cymule.component-input/1";
pub(crate) const EFFECT_RESULT_ARTIFACT_KIND: &str = "cymule.effect-result/1";
pub(crate) const INVOCATION_INPUT_ARTIFACT_KIND: &str = "cymule.invocation-input/1";
pub(crate) const INVOCATION_RESULT_ARTIFACT_KIND: &str = "cymule.invocation-result/1";
pub(crate) const SCOPE_RESULT_ARTIFACT_KIND: &str = "cymule.scope-result/1";
pub(crate) const WAIT_ID_DOMAIN: &str = "cymule.wait/2";
const SNAPSHOT_ID_DOMAIN: &str = "cymule.snapshot/2";
pub(crate) const TRANSPORT_REQUEST_ID_DOMAIN: &str = "cymule.transport-request/1";
pub(crate) const DERIVED_COMMAND_ID_DOMAIN: &str = "cymule.durable-command-id/1";
pub(crate) const DURABLE_RUNTIME_ACTOR: &str = "actor:durable-runtime";

/// Decode one exact immutable Artifact selected by its complete reference.
pub(crate) fn decode_artifact_value(
    expected: &ArtifactRef,
    record: &ArtifactRecord,
) -> DurableResult<serde_json::Value> {
    record.validate()?;
    if &record.reference != expected {
        return Err(DurableError::Integrity {
            code: "artifact_reference_mismatch".to_owned(),
            message: format!(
                "Artifact {} changed its exact kind or identity",
                expected.artifact_id
            ),
        });
    }
    cymule_core::decode_json(&record.bytes).map_err(Into::into)
}

/// Evaluate one sealed expression using only an exact-reference Artifact loader.
pub(crate) fn evaluate_expression_with<F>(
    expression: &Expression,
    input: &serde_json::Value,
    locals: &BTreeMap<String, ArtifactRef>,
    load: &mut F,
) -> DurableResult<serde_json::Value>
where
    F: FnMut(&ArtifactRef) -> DurableResult<ArtifactRecord>,
{
    match expression {
        Expression::Input => Ok(input.clone()),
        Expression::Literal { value } => Ok(value.clone()),
        Expression::Binding { name } => {
            let reference = locals.get(name).ok_or_else(|| DurableError::Integrity {
                code: "expression_binding_missing".to_owned(),
                message: format!("sealed expression references missing binding {name}"),
            })?;
            let record = load(reference)?;
            decode_artifact_value(reference, &record)
        }
        Expression::Object { fields } => fields
            .iter()
            .map(|(name, expression)| {
                evaluate_expression_with(expression, input, locals, load)
                    .map(|value| (name.clone(), value))
            })
            .collect::<DurableResult<serde_json::Map<String, serde_json::Value>>>()
            .map(serde_json::Value::Object),
        Expression::Array { items } => items
            .iter()
            .map(|expression| evaluate_expression_with(expression, input, locals, load))
            .collect::<DurableResult<Vec<_>>>()
            .map(serde_json::Value::Array),
    }
}

pub(crate) fn journal_wait_coupling_id(wait_id: &str) -> DurableResult<String> {
    validate_sha256_identity("journal wait", wait_id)?;
    content_id(COUPLED_CHECKPOINT_KEY_DOMAIN, &("journal_wait", wait_id)).map_err(Into::into)
}

pub(crate) fn input_wait_coupling_id(wait_id: &str) -> DurableResult<String> {
    validate_sha256_identity("input wait", wait_id)?;
    content_id(
        COUPLED_CHECKPOINT_KEY_DOMAIN,
        &("input_wait_journals", wait_id),
    )
    .map_err(Into::into)
}

pub(crate) fn agent_input_suspension_key(wait_id: &str) -> DurableResult<String> {
    validate_sha256_identity("Agent input suspension Wait", wait_id)?;
    content_id(AGENT_INPUT_SUSPENSION_KEY_DOMAIN, &wait_id).map_err(Into::into)
}

pub(crate) fn agent_input_completion_key(wait_id: &str) -> DurableResult<String> {
    validate_sha256_identity("Agent input completion Wait", wait_id)?;
    content_id(AGENT_INPUT_COMPLETION_KEY_DOMAIN, &wait_id).map_err(Into::into)
}

pub(crate) fn resource_handoff_input_coupling_id(activation_id: &str) -> DurableResult<String> {
    validate_sha256_identity("resource handoff activation", activation_id)?;
    content_id(
        COUPLED_CHECKPOINT_KEY_DOMAIN,
        &("resource_handoff_input", activation_id),
    )
    .map_err(Into::into)
}

pub(crate) fn agent_workspace_coupling_id(agent_command_id: &str) -> DurableResult<String> {
    validate_sha256_identity("Agent workspace command", agent_command_id)?;
    content_id(
        COUPLED_CHECKPOINT_KEY_DOMAIN,
        &("agent_workspace", agent_command_id),
    )
    .map_err(Into::into)
}

/// The workspace witness uses the existing typed Continuation generation as
/// its content-ID domain, never a raw canonical-JSON digest or a second alias.
pub(crate) fn agent_workspace_continuation_digest(
    continuation: &Continuation,
) -> DurableResult<String> {
    continuation.verify_wire()?;
    content_id(
        cymule_durable_protocol::CONTINUATION_STATE_VERSION,
        continuation,
    )
    .map_err(Into::into)
}

/// Derive the exact Plan-structural identity of one durable wait occurrence.
///
/// # Errors
/// Returns an error if the structural identity cannot be canonically encoded.
pub fn derive_wait_id(
    run_id: &str,
    plan_id: &str,
    invocation_id: &str,
    site_id: &str,
) -> DurableResult<String> {
    content_id(WAIT_ID_DOMAIN, &(run_id, plan_id, invocation_id, site_id)).map_err(Into::into)
}

/// Closed internal command purposes used when caller-authored identities are
/// inputs to a canonical Machine command identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DerivedCommandOperation {
    AdvanceContinuation,
    AdvanceContinuationEpoch,
    AuthorizeEffect,
    BeginContinuationAttempt,
    CommitRootScope,
    CommitScope,
    CompleteRun,
    FailRun,
    MarkEffectUnavailable,
    ObserveEffect,
    OpenScope,
    PrepareEffect,
    ProposeEffect,
    ReconcileEffect,
    StartEffectDispatch,
    StartRun,
    YieldContinuationAttempt,
}

#[derive(Serialize)]
struct DerivedCommandIdPreimage<'a, T> {
    operation: DerivedCommandOperation,
    semantics: &'a T,
}

/// Derive one bounded, domain-separated Machine command identity from its
/// complete semantic inputs. Caller identities are hashed as values and are
/// never truncated, escaped, or concatenated into the public command ID.
pub(crate) fn derived_command_id<T: Serialize>(
    operation: DerivedCommandOperation,
    semantics: &T,
) -> DurableResult<String> {
    content_id(
        DERIVED_COMMAND_ID_DOMAIN,
        &DerivedCommandIdPreimage {
            operation,
            semantics,
        },
    )
    .map_err(Into::into)
}

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
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "deserialize_non_empty_map_if_present"
    )]
    pub wait_activations: BTreeMap<String, WaitActivationReceipt>,
    /// Fenced coordination leases keyed by coordination resource.
    pub leases: BTreeMap<String, CoordinationLease>,
    /// Effect dispatch outbox keyed by structural intent ID.
    pub outbox: BTreeMap<String, EffectDispatch>,
    /// Canonical component results keyed by occurrence ID.
    pub component_occurrences: BTreeMap<String, ComponentOccurrence>,
    /// Provider Attempts keyed by stable Attempt identity.
    pub operation_attempts: BTreeMap<String, OperationAttempt>,
    /// Content-backed logical Clock observations used by execution claims.
    pub clock_observations: BTreeMap<String, crate::ClockObservation>,
    /// Portable snapshots keyed by snapshot ID.
    pub snapshots: BTreeMap<String, SnapshotRecord>,
    /// Idempotent canonical Event-prefix compactions keyed by command identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub history_compactions: BTreeMap<String, HistoryCompactionReceipt>,
    /// Exact all-ever semantic Run-cancellation receipts, keyed by command ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cancellation_receipts: BTreeMap<String, crate::CancellationReceipt>,
    /// Exact all-ever terminal Effect-resolution receipts, keyed by command ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub effect_resolution_receipts: BTreeMap<String, crate::EffectResolutionReceipt>,
    /// Typed application journals keyed by stable journal identity.
    ///
    /// This extension seam lets higher profiles share the same CAS authority
    /// without teaching M1 their domain types. Each record is self-validating,
    /// and the owning profile must validate its typed payload while replaying.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub application_journals: BTreeMap<String, ApplicationJournal>,
    /// Latest bounded authenticated prefix-replacement receipt per application
    /// journal. Older payload-free authorities remain in the `StateRoot` history
    /// map and resolve only by exact replacement identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub application_journal_prefix_replacements:
        BTreeMap<String, ApplicationJournalPrefixReplacementReceipt>,
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
            operation_attempts: BTreeMap::new(),
            clock_observations: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            history_compactions: BTreeMap::new(),
            cancellation_receipts: BTreeMap::new(),
            effect_resolution_receipts: BTreeMap::new(),
            application_journals: BTreeMap::new(),
            application_journal_prefix_replacements: BTreeMap::new(),
        }
    }

    /// Validate references and stable identities before persistence.
    ///
    /// # Errors
    /// Returns an error if Machine restoration or any retained typed authority,
    /// Artifact reference, identity, or lifecycle closure is invalid.
    pub fn validate(&self) -> DurableResult<()> {
        self.validate_with_anchor(None)
    }

    pub(crate) fn validate_anchored(
        &self,
        anchor: Option<&cymule_core::MachineBaseAnchor>,
    ) -> DurableResult<()> {
        self.validate_with_anchor(anchor)
    }

    fn validate_with_anchor(
        &self,
        anchor: Option<&cymule_core::MachineBaseAnchor>,
    ) -> DurableResult<()> {
        let machine = match anchor {
            Some(anchor) => Machine::restore_anchored(self.machine.clone(), anchor)?,
            None => Machine::restore(self.machine.clone())?,
        };
        self.validate_restored_machine(&machine)
    }

    pub(crate) fn validate_restored_machine(&self, machine: &Machine) -> DurableResult<()> {
        if self.durable_version != DURABLE_STATE_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable state version {:?}",
                self.durable_version
            )));
        }
        validate_effect_outbox_closure(machine, &self.outbox)?;
        self.validate_retained_continuations(machine)?;
        self.validate_retained_waits(machine)?;
        self.validate_retained_wait_activations(machine)?;
        self.validate_retained_dispatches(machine)?;
        self.validate_retained_component_occurrences(machine)?;
        self.validate_retained_operation_attempts(machine)?;
        self.validate_retained_clocks_and_snapshots(machine)?;
        self.validate_retained_journals()?;
        self.validate_terminal_receipts(machine)?;
        self.validate_retained_compactions()?;
        Ok(())
    }

    fn validate_retained_continuations(&self, machine: &Machine) -> DurableResult<()> {
        for (run_id, continuation) in &self.continuations {
            if &continuation.run_id != run_id {
                return Err(DurableError::Validation(format!(
                    "continuation key {run_id} does not match its Run"
                )));
            }
            continuation.verify_wire()?;
            validate_continuation_artifacts(machine, continuation, &self.outbox)?;
            let run = machine.projection().runs.get(run_id).ok_or_else(|| {
                DurableError::Validation(format!("Continuation {run_id} has no Machine Run"))
            })?;
            let active_attempts: Vec<&cymule_core::AttemptProjection> = run
                .attempts
                .values()
                .filter(|attempt| attempt.active)
                .collect();
            match (&continuation.status, &continuation.execution_claim) {
                (ContinuationStatus::Running, Some(claim)) => {
                    claim.verify(continuation, &self.clock_observations)?;
                    require_artifact(
                        machine,
                        &claim.execution_binding_ref,
                        &format!("execution claim {} binding", claim.continuation_attempt_id),
                    )?;
                    let attempt = run
                        .attempts
                        .get(&claim.continuation_attempt_id)
                        .ok_or_else(|| {
                            DurableError::Validation(format!(
                                "execution claim {} has no Machine Attempt",
                                claim.continuation_attempt_id
                            ))
                        })?;
                    if active_attempts.len() != 1
                        || !attempt.active
                        || attempt.continuation_id != claim.continuation_id
                        || attempt.occurrence_binding != claim.execution_binding_ref.artifact_id
                        || attempt.continuation_epoch != continuation.epoch
                        || attempt.execution_fence != claim.fence
                    {
                        return Err(DurableError::Validation(format!(
                            "execution claim {} does not match its active Machine Attempt",
                            claim.continuation_attempt_id
                        )));
                    }
                }
                (ContinuationStatus::Running, None) => {
                    return Err(DurableError::Validation(format!(
                        "running Continuation {run_id} requires an execution claim"
                    )));
                }
                (_, Some(_)) => {
                    return Err(DurableError::Validation(format!(
                        "non-running Continuation {run_id} cannot retain an execution claim"
                    )));
                }
                (_, None) if active_attempts.is_empty() => {}
                (_, None) => {
                    return Err(DurableError::Validation(format!(
                        "non-running Continuation {run_id} retained an active Machine Attempt"
                    )));
                }
            }
        }
        for run_id in machine.projection().runs.keys() {
            if !self.continuations.contains_key(run_id) {
                return Err(DurableError::Validation(format!(
                    "Machine Run {run_id} has no durable Continuation"
                )));
            }
        }
        Ok(())
    }

    fn validate_retained_waits(&self, machine: &Machine) -> DurableResult<()> {
        for (wait_id, wait) in &self.waits {
            if &wait.wait_id != wait_id {
                return Err(DurableError::Validation(format!(
                    "wait key {wait_id} does not match its identity"
                )));
            }
            wait.verify_wire()?;
            let continuation = self.continuations.get(&wait.run_id).ok_or_else(|| {
                DurableError::Validation(format!(
                    "wait {wait_id} references missing continuation {}",
                    wait.run_id
                ))
            })?;
            validate_wait_artifacts(machine, continuation, wait)?;
            let retained_by_continuation = continuation.wait_set.contains(wait_id);
            match wait.state {
                WaitState::Pending
                    if retained_by_continuation
                        && continuation.status == ContinuationStatus::Waiting => {}
                WaitState::Completed | WaitState::Cancelled if !retained_by_continuation => {}
                _ => {
                    return Err(DurableError::Validation(format!(
                        "wait {wait_id} state is inconsistent with Continuation {}",
                        continuation.run_id
                    )));
                }
            }
        }
        for (run_id, continuation) in &self.continuations {
            let inconsistent_waiting = match continuation.status {
                ContinuationStatus::Waiting => continuation.wait_set.is_empty(),
                _ => !continuation.wait_set.is_empty(),
            };
            if inconsistent_waiting {
                return Err(DurableError::Validation(format!(
                    "Continuation {run_id} waiting status does not match its wait set"
                )));
            }
            for wait_id in &continuation.wait_set {
                let wait = self.waits.get(wait_id).ok_or_else(|| {
                    DurableError::Validation(format!(
                        "Continuation {run_id} references missing wait {wait_id}"
                    ))
                })?;
                if wait.run_id != *run_id || wait.state != WaitState::Pending {
                    return Err(DurableError::Validation(format!(
                        "Continuation {run_id} wait {wait_id} is not its own pending wait"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_retained_wait_activations(&self, machine: &Machine) -> DurableResult<()> {
        let mut activated_waits = BTreeSet::new();
        for (activation_id, receipt) in &self.wait_activations {
            receipt.verify()?;
            let activation = &receipt.activation;
            if &activation.activation_id != activation_id {
                return Err(DurableError::Validation(format!(
                    "wait activation key {activation_id} does not match its identity"
                )));
            }
            activation.verify()?;
            require_artifact(
                machine,
                &activation.result,
                &format!("wait activation {activation_id} result"),
            )?;
            let mut consume_once_targets = 0usize;
            let mut applied_run_ids = BTreeSet::new();
            for wait_id in &activation.wait_ids {
                let applied = receipt.applied_wait_ids.contains(wait_id);
                if applied && !activated_waits.insert(wait_id) {
                    return Err(DurableError::Validation(format!(
                        "wait {wait_id} is completed by more than one activation"
                    )));
                }
                let wait = self.waits.get(wait_id).ok_or_else(|| {
                    DurableError::Validation(format!(
                        "wait activation {activation_id} references missing wait {wait_id}"
                    ))
                })?;
                if applied {
                    applied_run_ids.insert(wait.run_id.clone());
                }
                ensure_wait_activation_source_matches(&activation.source, wait)?;
                match (applied, wait.state) {
                    (true, WaitState::Completed)
                        if wait.result.as_ref() == Some(&activation.result) => {}
                    (false, WaitState::Completed | WaitState::Cancelled) => {}
                    _ => {
                        return Err(DurableError::Validation(format!(
                            "wait activation {activation_id} target outcome is not reflected by wait {wait_id}"
                        )));
                    }
                }
                let continuation = self.continuations.get(&wait.run_id).ok_or_else(|| {
                    DurableError::Validation(format!(
                        "wait activation {activation_id} references missing continuation {}",
                        wait.run_id
                    ))
                })?;
                if continuation.wait_set.contains(wait_id) {
                    return Err(DurableError::Validation(format!(
                        "wait activation {activation_id} left terminal wait {wait_id} pending on its Continuation"
                    )));
                }
                if wait.consume_once {
                    consume_once_targets += 1;
                }
            }
            activation
                .source
                .validate_target_cardinality(activation.wait_ids.len(), consume_once_targets)?;
            if !receipt.ready_run_ids.is_subset(&applied_run_ids) {
                return Err(DurableError::Validation(format!(
                    "wait activation {activation_id} ready Runs are not owned by applied targets"
                )));
            }
        }
        for (wait_id, wait) in &self.waits {
            if wait.state == WaitState::Completed
                && matches!(wait.kind, WaitKind::Signal { .. } | WaitKind::Timer { .. })
                && !activated_waits.contains(wait_id)
            {
                return Err(DurableError::Validation(format!(
                    "completed external wait {wait_id} has no unique applied activation receipt"
                )));
            }
        }
        Ok(())
    }

    fn validate_retained_dispatches(&self, machine: &Machine) -> DurableResult<()> {
        for (resource, lease) in &self.leases {
            lease.verify()?;
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
            dispatch.verify_wire()?;
            validate_dispatch_artifacts(machine, dispatch)?;
        }
        Ok(())
    }

    fn validate_retained_component_occurrences(&self, machine: &Machine) -> DurableResult<()> {
        for (occurrence_id, occurrence) in &self.component_occurrences {
            if &occurrence.occurrence_id != occurrence_id {
                return Err(DurableError::Validation(format!(
                    "component occurrence key {occurrence_id} does not match its identity"
                )));
            }
            require_artifact(
                machine,
                &occurrence.input,
                &format!("component occurrence {occurrence_id} input"),
            )?;
            occurrence.verify()?;
            validate_component_occurrence_authority(machine, occurrence)?;
            if let Some(outcome) = &occurrence.outcome {
                validate_component_outcome(
                    machine,
                    outcome,
                    &format!("component occurrence {occurrence_id}"),
                )?;
                if let ComponentOutcome::ExpectedFailure { code, detail } = outcome {
                    let record = machine.artifact(detail).ok_or_else(|| {
                        DurableError::Validation(format!(
                            "component occurrence {occurrence_id} failure detail is missing"
                        ))
                    })?;
                    let declared: cymule_runtime::PluginExpectedFailure =
                        cymule_core::decode_json(&record.bytes)?;
                    declared.verify()?;
                    let run = machine
                        .projection()
                        .runs
                        .get(&occurrence.run_id)
                        .ok_or_else(|| {
                            DurableError::Validation(format!(
                                "component occurrence {occurrence_id} Run is missing"
                            ))
                        })?;
                    if declared.code != *code
                        || !matches!(
                            &run.execution_status,
                            cymule_core::RunExecutionStatus::Failed { failure }
                                if failure.class == cymule_core::RunFailureClass::DeclaredFailure
                                    && failure.code == *code
                                    && failure.detail == *detail
                        )
                    {
                        return Err(DurableError::Validation(format!(
                            "component occurrence {occurrence_id} failure does not match Run authority"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_retained_operation_attempts(&self, machine: &Machine) -> DurableResult<()> {
        let mut attempts_by_occurrence: BTreeMap<&str, Vec<&OperationAttempt>> = BTreeMap::new();
        for (attempt_id, attempt) in &self.operation_attempts {
            if &attempt.attempt_id != attempt_id {
                return Err(DurableError::Validation(format!(
                    "operation Attempt key {attempt_id} does not match its identity"
                )));
            }
            attempt.verify()?;
            let occurrence = self
                .component_occurrences
                .get(&attempt.occurrence_id)
                .ok_or_else(|| {
                    DurableError::Validation(format!(
                        "operation Attempt {attempt_id} references missing occurrence {}",
                        attempt.occurrence_id
                    ))
                })?;
            if attempt.run_id != occurrence.run_id {
                return Err(DurableError::Validation(format!(
                    "operation Attempt {attempt_id} escaped its occurrence Run"
                )));
            }
            match attempt.state {
                OperationAttemptState::Running => {
                    let claim = self
                        .continuations
                        .get(&attempt.run_id)
                        .and_then(|continuation| continuation.execution_claim.as_ref())
                        .ok_or_else(|| {
                            DurableError::Validation(format!(
                                "running operation Attempt {attempt_id} has no active execution claim"
                            ))
                        })?;
                    if attempt.execution_claim_fence != claim.fence
                        || attempt.continuation_attempt_id != claim.continuation_attempt_id
                    {
                        return Err(DurableError::Validation(format!(
                            "running operation Attempt {attempt_id} is fenced"
                        )));
                    }
                }
                OperationAttemptState::Completed => {
                    if occurrence.state != ComponentOccurrenceState::Completed
                        || attempt.outcome != occurrence.outcome
                    {
                        return Err(DurableError::Validation(format!(
                            "completed operation Attempt {attempt_id} does not match its occurrence"
                        )));
                    }
                }
                OperationAttemptState::Superseded => {}
            }
            if let Some(outcome) = &attempt.outcome {
                validate_component_outcome(
                    machine,
                    outcome,
                    &format!("operation Attempt {attempt_id}"),
                )?;
            }
            attempts_by_occurrence
                .entry(&attempt.occurrence_id)
                .or_default()
                .push(attempt);
        }
        for occurrence in self.component_occurrences.values() {
            let Some(attempts) = attempts_by_occurrence.get_mut(occurrence.occurrence_id.as_str())
            else {
                return Err(DurableError::Validation(format!(
                    "component occurrence {} Attempt history is not closed",
                    occurrence.occurrence_id
                )));
            };
            let continuation_status = self
                .continuations
                .get(&occurrence.run_id)
                .ok_or_else(|| {
                    DurableError::Validation(format!(
                        "component occurrence {} has no Continuation",
                        occurrence.occurrence_id
                    ))
                })?
                .status;
            validate_operation_attempt_history(occurrence, attempts, continuation_status)?;
        }
        Ok(())
    }

    fn validate_retained_clocks_and_snapshots(&self, machine: &Machine) -> DurableResult<()> {
        for (evidence_id, observation) in &self.clock_observations {
            observation.verify()?;
            if &observation.observation_id != evidence_id {
                return Err(DurableError::Validation(format!(
                    "Clock observation key {evidence_id} does not match its content"
                )));
            }
        }
        for (snapshot_id, snapshot) in &self.snapshots {
            if &snapshot.snapshot_id != snapshot_id {
                return Err(DurableError::Validation(format!(
                    "snapshot key {snapshot_id} does not match its identity"
                )));
            }
            snapshot.verify()?;
            if !snapshot
                .continuation_ids
                .iter()
                .all(|run_id| self.continuations.contains_key(run_id))
            {
                return Err(DurableError::Validation(format!(
                    "snapshot {snapshot_id} references a missing Continuation"
                )));
            }
            let retained_obligations = machine
                .projection()
                .runs
                .values()
                .flat_map(|run| run.obligations.keys())
                .collect::<BTreeSet<_>>();
            if !snapshot
                .unresolved_obligations
                .iter()
                .all(|obligation_id| retained_obligations.contains(obligation_id))
            {
                return Err(DurableError::Validation(format!(
                    "snapshot {snapshot_id} references a missing obligation"
                )));
            }
            let retained_bindings = machine
                .projection()
                .runs
                .values()
                .flat_map(|run| run.effects.values())
                .map(|effect| &effect.occurrence_binding)
                .chain(
                    self.component_occurrences
                        .values()
                        .map(|occurrence| &occurrence.occurrence_binding),
                )
                .collect::<BTreeSet<_>>();
            if !snapshot
                .occurrence_bindings
                .iter()
                .all(|binding| retained_bindings.contains(binding))
            {
                return Err(DurableError::Validation(format!(
                    "snapshot {snapshot_id} references a missing occurrence binding"
                )));
            }
        }
        Ok(())
    }

    fn validate_retained_journals(&self) -> DurableResult<()> {
        for (journal_id, records) in &self.application_journals {
            validate_wire_non_empty("application journal identity", journal_id)?;
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
        for (journal_id, receipt) in &self.application_journal_prefix_replacements {
            receipt.verify()?;
            if &receipt.replacement.journal_id != journal_id {
                return Err(DurableError::Integrity {
                    code: "application_journal_prefix_receipt_key_mismatch".to_owned(),
                    message: format!(
                        "application journal prefix receipt {} is stored under {journal_id}",
                        receipt.replacement.replacement_id
                    ),
                });
            }
            let records = self.application_journals.get(journal_id).ok_or_else(|| {
                DurableError::Integrity {
                    code: "application_journal_prefix_receipt_journal_missing".to_owned(),
                    message: format!(
                        "application journal prefix receipt {} has no journal {journal_id}",
                        receipt.replacement.replacement_id
                    ),
                }
            })?;
            if records.len() < receipt.replacement.replacement.len()
                || receipt
                    .replacement
                    .replacement
                    .iter()
                    .enumerate()
                    .any(|(index, expected)| records.get(index) != Some(expected))
            {
                return Err(DurableError::Integrity {
                    code: "application_journal_prefix_receipt_not_current".to_owned(),
                    message: format!(
                        "application journal {journal_id} does not retain replacement {} at its prefix",
                        receipt.replacement.replacement_id
                    ),
                });
            }
        }
        Ok(())
    }

    fn validate_retained_compactions(&self) -> DurableResult<()> {
        let compactions = self.history_compaction_chain()?;
        let mut expected_parent = None;
        let mut expected_parent_segment = None;
        let mut expected_parent_count = 0;
        let mut expected_parent_event_count = 0;
        let mut expected_parent_admission_head = None;
        let mut expected_parent_command_index_root =
            cymule_core::MachineCommandIndexProof::empty_root()?;
        for receipt in &compactions {
            receipt.verify()?;
            let header = &receipt.result.archive_segment;
            if self.history_compactions.get(&receipt.compaction_id) != Some(*receipt) {
                return Err(DurableError::Validation(format!(
                    "history compaction key {} does not match its receipt",
                    receipt.compaction_id
                )));
            }
            if receipt.parent_compaction != expected_parent
                || header.parent_segment != expected_parent_segment
                || header.parent_count != expected_parent_count
                || header.parent_event_count != expected_parent_event_count
                || header.parent_admission_head != expected_parent_admission_head
                || header.parent_command_index_root != expected_parent_command_index_root
                || header.result_count < expected_parent_count
            {
                return Err(DurableError::Validation(
                    "history compaction lineage is discontinuous".to_owned(),
                ));
            }
            expected_parent = Some(receipt.compaction_id.clone());
            expected_parent_segment = Some(header.segment_id.clone());
            expected_parent_count = header.result_count;
            expected_parent_event_count = header.result_event_count;
            expected_parent_admission_head.clone_from(&header.result_admission_head);
            expected_parent_command_index_root.clone_from(&header.result_command_index_root);
        }
        if self.machine.base.is_some() == compactions.is_empty() {
            return Err(DurableError::Validation(
                "Machine base and history-compaction receipt lineage must appear together"
                    .to_owned(),
            ));
        }
        if let Some(latest) = compactions.last() {
            let base = self.machine.base.as_ref().ok_or_else(|| {
                DurableError::Validation(
                    "history compaction exists without a Machine base snapshot".to_owned(),
                )
            })?;
            let base_id = base.identity()?;
            let compacted_events = base.archive_event_count;
            let retained_events = u64::try_from(self.machine.events.len())
                .map_err(|error| DurableError::Validation(error.to_string()))?;
            if latest.result.base_id != base_id
                || latest.result.compacted_events != compacted_events
                || latest.result.retained_events > retained_events
                || latest.result.projection_digest != base.projection_digest
                || latest.result.archive_segment.segment_id != base.archive_head
                || latest.result.archive_segment.result_count != base.archive_count
                || latest.result.archive_segment.result_event_count != base.archive_event_count
                || latest.result.archive_segment.result_admission_head != base.admission_head
                || latest.result.archive_segment.result_command_index_root
                    != base.command_index_root
            {
                return Err(DurableError::Validation(
                    "latest history compaction does not match the Machine snapshot".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn history_compaction_chain(&self) -> DurableResult<Vec<&HistoryCompactionReceipt>> {
        let mut by_parent = BTreeMap::new();
        for receipt in self.history_compactions.values() {
            if by_parent
                .insert(receipt.parent_compaction.as_deref(), receipt)
                .is_some()
            {
                return Err(DurableError::Integrity {
                    code: "history_compaction_lineage_fork".to_owned(),
                    message: "history compaction receipts have more than one child of a parent"
                        .to_owned(),
                });
            }
        }
        let mut ordered = Vec::with_capacity(by_parent.len());
        let mut parent = None;
        while let Some(receipt) = by_parent.remove(&parent) {
            parent = Some(receipt.compaction_id.as_str());
            ordered.push(receipt);
        }
        if !by_parent.is_empty() {
            return Err(DurableError::Integrity {
                code: "history_compaction_lineage_not_closed".to_owned(),
                message: "history compaction receipts contain a cycle or missing parent".to_owned(),
            });
        }
        Ok(ordered)
    }

    fn validate_terminal_receipts(&self, machine: &Machine) -> DurableResult<()> {
        let mut cancellation_runs = BTreeSet::new();
        for (command_id, receipt) in &self.cancellation_receipts {
            let run = machine
                .projection()
                .runs
                .get(&receipt.command.run_id)
                .ok_or_else(|| DurableError::Integrity {
                    code: "run_cancellation_receipt_run_missing".to_owned(),
                    message: format!(
                        "cancellation receipt {command_id} references missing Run {}",
                        receipt.command.run_id
                    ),
                })?;
            if command_id != &receipt.command.cancellation_id
                || !cancellation_runs.insert(receipt.command.run_id.as_str())
            {
                return Err(DurableError::Integrity {
                    code: "run_cancellation_receipt_not_closed".to_owned(),
                    message: format!("cancellation receipt {command_id} changed its key or Run"),
                });
            }
            validate_cancellation_receipt_closure(receipt, &run.run_id, &run.execution_status)?;
            if let crate::DurableBoundary::Cancelled { reason } = &receipt.boundary {
                require_artifact(machine, reason, "Run cancellation receipt reason")?;
            }
        }
        for run in machine.projection().runs.values() {
            if matches!(
                run.execution_status,
                cymule_core::RunExecutionStatus::Cancelled { .. }
            ) && !cancellation_runs.contains(run.run_id.as_str())
            {
                return Err(DurableError::Integrity {
                    code: "cancelled_run_receipt_missing".to_owned(),
                    message: format!("cancelled Run {} has no immutable receipt", run.run_id),
                });
            }
        }
        let mut resolved_intents = BTreeSet::new();
        for (command_id, receipt) in &self.effect_resolution_receipts {
            let dispatch = self.outbox.get(&receipt.command.intent_id).ok_or_else(|| {
                DurableError::Integrity {
                    code: "effect_resolution_receipt_intent_missing".to_owned(),
                    message: format!(
                        "resolution receipt {command_id} references missing Effect {}",
                        receipt.command.intent_id
                    ),
                }
            })?;
            if command_id != &receipt.command.resolution_id
                || !resolved_intents.insert(receipt.command.intent_id.as_str())
            {
                return Err(DurableError::Integrity {
                    code: "effect_resolution_receipt_not_closed".to_owned(),
                    message: format!("resolution receipt {command_id} changed its key or intent"),
                });
            }
            validate_effect_resolution_receipt_closure(receipt, dispatch)?;
            require_artifact(
                machine,
                &receipt.command.execution_binding,
                "Effect receipt binding",
            )?;
            if let Some(result) = &receipt.result {
                require_artifact(machine, result, "Effect resolution receipt result")?;
            }
        }
        Ok(())
    }

    /// Canonical revision digest used by stores for compare-and-swap.
    ///
    /// # Errors
    /// Returns an error if the state is invalid or cannot be canonically encoded.
    pub fn revision(&self) -> DurableResult<String> {
        self.validate()?;
        canonical_digest(self).map_err(Into::into)
    }
}

/// Validate the receipt against the exact Core Run selected by its typed owner.
pub(crate) fn validate_cancellation_receipt_closure(
    receipt: &crate::CancellationReceipt,
    run_id: &str,
    execution_status: &cymule_core::RunExecutionStatus,
) -> DurableResult<()> {
    receipt.verify().map_err(|error| DurableError::Integrity {
        code: "run_cancellation_receipt_invalid".to_owned(),
        message: error.to_string(),
    })?;
    if receipt.command.run_id != run_id
        || !matches!(
            (execution_status, &receipt.boundary),
            (
                cymule_core::RunExecutionStatus::Cancelled { reason },
                crate::DurableBoundary::Cancelled { reason: retained },
            ) if reason == retained
        )
    {
        return Err(DurableError::Integrity {
            code: "run_cancellation_receipt_not_closed".to_owned(),
            message: format!(
                "cancellation receipt {} does not close its exact Run",
                receipt.command.cancellation_id
            ),
        });
    }
    Ok(())
}

/// Validate a terminal resolution against its immutable original dispatch pin.
pub(crate) fn validate_effect_resolution_receipt_closure(
    receipt: &crate::EffectResolutionReceipt,
    dispatch: &EffectDispatch,
) -> DurableResult<()> {
    receipt.verify().map_err(|error| DurableError::Integrity {
        code: "effect_resolution_receipt_invalid".to_owned(),
        message: error.to_string(),
    })?;
    let expected_state = match receipt.actual_resolution {
        cymule_core::ReconciliationResolution::ResolvedApplied => OutboxState::Applied,
        cymule_core::ReconciliationResolution::ResolvedNotApplied => OutboxState::NotApplied,
        cymule_core::ReconciliationResolution::StillUnknown
        | cymule_core::ReconciliationResolution::GovernanceRequired => {
            return Err(DurableError::Integrity {
                code: "effect_resolution_receipt_nonterminal".to_owned(),
                message: "Effect resolution receipt retained a nonterminal outcome".to_owned(),
            });
        }
    };
    if dispatch.intent_id != receipt.command.intent_id
        || dispatch.run_id != receipt.command.run_id
        || dispatch.execution_binding != receipt.command.execution_binding
        || dispatch.occurrence_binding != receipt.command.occurrence_binding
        || dispatch.claim_owner.as_deref() != Some(receipt.command.claim_owner.as_str())
        || dispatch.claim_epoch != receipt.command.claim_epoch
        || dispatch.state != expected_state
        || dispatch.reconciliation != cymule_core::ReconciliationState::Resolved
        || dispatch.result != receipt.result
        || receipt.result.is_some() != (expected_state == OutboxState::Applied)
    {
        return Err(DurableError::Integrity {
            code: "effect_resolution_receipt_not_closed".to_owned(),
            message: format!(
                "resolution receipt {} does not close its exact Effect",
                receipt.command.resolution_id
            ),
        });
    }
    Ok(())
}

/// Bind cancellation identity and reason to its exact immutable Core batch.
pub(crate) fn validate_cancellation_receipt_command(
    receipt: &crate::CancellationReceipt,
    entry: &cymule_core::MachineCommandArchiveEntry,
    batch: &cymule_core::MachineCommandBatchRecord,
) -> DurableResult<()> {
    receipt.verify()?;
    let crate::DurableBoundary::Cancelled { reason } = &receipt.boundary else {
        return Err(DurableError::Validation(
            "cancellation receipt is not terminal".to_owned(),
        ));
    };
    validate_terminal_receipt_command(
        &receipt.command.cancellation_id,
        &receipt.command.run_id,
        &cymule_core::Command::CancelRun {
            reason: reason.clone(),
        },
        std::slice::from_ref(reason),
        entry,
        batch,
    )
}

/// Bind the provider's actual settlement and output to the same Core batch.
pub(crate) fn validate_effect_resolution_receipt_command(
    receipt: &crate::EffectResolutionReceipt,
    entry: &cymule_core::MachineCommandArchiveEntry,
    batch: &cymule_core::MachineCommandBatchRecord,
) -> DurableResult<()> {
    receipt.verify()?;
    validate_terminal_receipt_command(
        &receipt.command.resolution_id,
        &receipt.command.run_id,
        &cymule_core::Command::TransitionEffect {
            intent_id: receipt.command.intent_id.clone(),
            transition: cymule_core::EffectTransition::Reconcile(receipt.actual_resolution),
        },
        receipt.result.as_slice(),
        entry,
        batch,
    )
}

fn validate_terminal_receipt_command(
    command_id: &str,
    run_id: &str,
    command: &cymule_core::Command,
    artifacts: &[ArtifactRef],
    entry: &cymule_core::MachineCommandArchiveEntry,
    batch: &cymule_core::MachineCommandBatchRecord,
) -> DurableResult<()> {
    batch.verify_entry(entry)?;
    let record = &entry.command;
    if record.envelope.command_id != command_id
        || record.envelope.run_id != run_id
        || record.envelope.actor != DURABLE_RUNTIME_ACTOR
        || record.envelope.command != *command
        || record.receipt.status != cymule_core::CommandReceiptStatus::Applied
        || record.receipt.event_ids.len() != 1
        || batch.members.len() != 1
        || !batch.plan_ids.is_empty()
        || batch.artifacts != artifacts
    {
        return Err(DurableError::Integrity {
            code: "terminal_receipt_command_mismatch".to_owned(),
            message: format!(
                "terminal receipt {command_id} does not match its exact Core command and material"
            ),
        });
    }
    Ok(())
}

pub(crate) fn validate_continuation_artifacts(
    machine: &Machine,
    continuation: &Continuation,
    outbox: &BTreeMap<String, EffectDispatch>,
) -> DurableResult<()> {
    let run = validate_continuation_run(machine, continuation)?;
    if matches!(
        continuation.status,
        ContinuationStatus::Ready | ContinuationStatus::Waiting | ContinuationStatus::Running
    ) && continuation.frames.is_empty()
    {
        return Err(DurableError::Validation(format!(
            "active Continuation {} requires an execution frame",
            continuation.run_id
        )));
    }
    if let Some(state) = &continuation.state {
        require_artifact(
            machine,
            state,
            &format!("Continuation {} state", continuation.run_id),
        )?;
    }
    let plan = machine.plan(&continuation.plan_id).ok_or_else(|| {
        DurableError::Validation(format!(
            "Continuation {} references missing Plan {}",
            continuation.run_id, continuation.plan_id
        ))
    })?;
    validate_continuation_plan_frames(plan, continuation)?;
    let (frame_scope, closed_boundary_scope) =
        validate_continuation_frame_scopes(machine, continuation, outbox)?;
    validate_continuation_scope_stack(
        run,
        continuation,
        frame_scope.as_deref(),
        closed_boundary_scope.is_some(),
    )
}

fn validate_continuation_run<'a>(
    machine: &'a Machine,
    continuation: &Continuation,
) -> DurableResult<&'a cymule_core::RunProjection> {
    if continuation.epoch > crate::MAX_EXACT_INTEGER
        || continuation.execution_fence > crate::MAX_EXACT_INTEGER
    {
        return Err(DurableError::Validation(format!(
            "Continuation {} epoch or execution fence exceeds the exact cross-language integer range",
            continuation.run_id
        )));
    }
    match continuation.status {
        ContinuationStatus::Ready
        | ContinuationStatus::Waiting
        | ContinuationStatus::Running
        | ContinuationStatus::Completed
        | ContinuationStatus::Failed
        | ContinuationStatus::Cancelled => {}
    }
    let run = machine
        .projection()
        .runs
        .get(&continuation.run_id)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "Continuation {} references a missing Machine Run",
                continuation.run_id
            ))
        })?;
    if continuation.plan_id != run.current_plan
        || continuation.binding_context != run.current_binding_context
        || continuation.epoch != run.epoch
    {
        return Err(DurableError::Validation(format!(
            "Continuation {} Plan, binding, or epoch does not match its Machine Run",
            continuation.run_id
        )));
    }
    let status_matches = match continuation.status {
        ContinuationStatus::Ready | ContinuationStatus::Waiting | ContinuationStatus::Running => {
            run.execution_status == cymule_core::RunExecutionStatus::Active
        }
        ContinuationStatus::Completed => {
            run.execution_status == cymule_core::RunExecutionStatus::Completed
        }
        ContinuationStatus::Failed => matches!(
            &run.execution_status,
            cymule_core::RunExecutionStatus::Failed { .. }
        ),
        ContinuationStatus::Cancelled => matches!(
            &run.execution_status,
            cymule_core::RunExecutionStatus::Cancelled { .. }
        ),
    };
    if !status_matches {
        return Err(DurableError::Validation(format!(
            "Continuation {} terminal status does not match its Machine Run",
            continuation.run_id
        )));
    }
    match &run.execution_status {
        cymule_core::RunExecutionStatus::Failed { failure } => require_artifact(
            machine,
            &failure.detail,
            &format!("Run {} failure detail", continuation.run_id),
        )?,
        cymule_core::RunExecutionStatus::Cancelled { reason } => require_artifact(
            machine,
            reason,
            &format!("Run {} cancellation reason", continuation.run_id),
        )?,
        cymule_core::RunExecutionStatus::Active | cymule_core::RunExecutionStatus::Completed => {}
    }
    Ok(run)
}

fn validate_continuation_frame_scopes(
    machine: &Machine,
    continuation: &Continuation,
    outbox: &BTreeMap<String, EffectDispatch>,
) -> DurableResult<(Option<String>, Option<String>)> {
    let resumable = matches!(
        continuation.status,
        ContinuationStatus::Ready | ContinuationStatus::Waiting | ContinuationStatus::Running
    );
    let mut frame_scope = None;
    let mut closed_boundary_scope = None;
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
        let location = cymule_core::ExecutionFrameLocation {
            run_id: &continuation.run_id,
            plan_id: &continuation.plan_id,
            invocation_id: &frame.invocation_id,
            invocation_path: &frame.invocation_path,
            definition_id: &frame.definition_id,
            region_path: &frame.region_path,
            scope_id: &frame.scope_id,
            next_step: frame.next_step,
        };
        let current_frame = cymule_core::ResumableExecutionFrame {
            location,
            binding_context: &continuation.binding_context,
            epoch: continuation.epoch,
        };
        let closed_boundary = cymule_core::ClosedExecutionBoundary {
            frame: current_frame,
            frame_count: continuation.frames.len(),
            scope_stack: &continuation.scope_stack,
            wait_count: continuation.wait_set.len(),
            disposition: match continuation.status {
                ContinuationStatus::Running => cymule_core::ClosedBoundaryDisposition::Running,
                ContinuationStatus::Ready
                | ContinuationStatus::Completed
                | ContinuationStatus::Failed
                | ContinuationStatus::Cancelled => cymule_core::ClosedBoundaryDisposition::Ready,
                ContinuationStatus::Waiting => cymule_core::ClosedBoundaryDisposition::Waiting,
            },
            has_execution_claim: continuation.execution_claim.is_some(),
        };
        let scope_id = if resumable {
            validate_resumable_continuation_scope(
                machine,
                continuation,
                &closed_boundary,
                outbox,
                &mut closed_boundary_scope,
            )?
        } else {
            machine.validate_historical_execution_location(&location)?
        };
        frame_scope = Some(scope_id);
    }
    Ok((frame_scope, closed_boundary_scope))
}

fn validate_resumable_continuation_scope(
    machine: &Machine,
    continuation: &Continuation,
    closed_boundary: &cymule_core::ClosedExecutionBoundary<'_>,
    outbox: &BTreeMap<String, EffectDispatch>,
    closed_boundary_scope: &mut Option<String>,
) -> DurableResult<String> {
    let scope = match machine.validate_effect_boundary_frame(closed_boundary) {
        Ok(boundary) => {
            let outbox_intents = outbox
                .values()
                .filter(|dispatch| {
                    dispatch.run_id == continuation.run_id
                        && matches!(
                            dispatch.state,
                            OutboxState::Pending | OutboxState::Claimed | OutboxState::Unknown
                        )
                })
                .map(|dispatch| dispatch.intent_id.clone())
                .collect::<BTreeSet<_>>();
            if boundary.intent_ids != outbox_intents {
                return Err(DurableError::Integrity {
                    code: "effect_boundary_outbox_set_mismatch".to_owned(),
                    message: format!(
                        "Run {} Effect boundary intents do not match its exact nonterminal outbox set",
                        continuation.run_id
                    ),
                });
            }
            *closed_boundary_scope = Some(boundary.scope_id.clone());
            boundary.scope_id
        }
        Err(effect_error) => match machine.validate_post_effect_ready_frame(closed_boundary) {
            Ok(scope_id) => {
                if outbox.values().any(|dispatch| {
                    dispatch.run_id == continuation.run_id
                        && matches!(
                            dispatch.state,
                            OutboxState::Pending | OutboxState::Claimed | OutboxState::Unknown
                        )
                }) {
                    return Err(DurableError::Integrity {
                        code: "post_effect_ready_outbox_not_terminal".to_owned(),
                        message: format!(
                            "Run {} post-Effect Ready boundary retains nonterminal outbox work",
                            continuation.run_id
                        ),
                    });
                }
                *closed_boundary_scope = Some(scope_id.clone());
                scope_id
            }
            Err(post_effect_error) => {
                match machine.validate_completion_boundary_frame(closed_boundary) {
                        Ok(scope_id) => {
                            *closed_boundary_scope = Some(scope_id.clone());
                            scope_id
                        }
                        Err(completion_error) => machine
                            .validate_resumable_execution_frame(&closed_boundary.frame)
                            .map_err(|resume_error| {
                                DurableError::Validation(format!(
                                    "active frame is neither resumable nor a typed Effect/post-Effect/completion boundary: {resume_error}; {effect_error}; {post_effect_error}; {completion_error}"
                                ))
                            })?,
                    }
            }
        },
    };
    Ok(scope)
}

fn validate_continuation_scope_stack(
    run: &cymule_core::RunProjection,
    continuation: &Continuation,
    frame_scope: Option<&str>,
    has_closed_boundary: bool,
) -> DurableResult<()> {
    let resumable = matches!(
        continuation.status,
        ContinuationStatus::Ready | ContinuationStatus::Waiting | ContinuationStatus::Running
    );
    if continuation.scope_stack.first().map(String::as_str) != Some(cymule_core::ROOT_SCOPE_ID) {
        return Err(DurableError::Validation(format!(
            "Continuation {} scope stack must begin at root",
            continuation.run_id
        )));
    }
    for (index, scope_id) in continuation.scope_stack.iter().enumerate() {
        let scope = run.scopes.get(scope_id).ok_or_else(|| {
            DurableError::Validation(format!(
                "Continuation {} scope stack references missing scope {scope_id}",
                continuation.run_id
            ))
        })?;
        if resumable
            && scope.status != cymule_core::ScopeStatus::Open
            && !(has_closed_boundary && scope.status == cymule_core::ScopeStatus::ClosedCommitted)
        {
            return Err(DurableError::Validation(format!(
                "active Continuation {} scope stack contains closed scope {scope_id}",
                continuation.run_id
            )));
        }
        if index > 0 && scope.parent_scope.as_ref() != continuation.scope_stack.get(index - 1) {
            return Err(DurableError::Validation(format!(
                "Continuation {} scope stack is not a parent-child lineage",
                continuation.run_id
            )));
        }
    }
    if let Some(frame_scope) = frame_scope
        && continuation.scope_stack.last().map(String::as_str) != Some(frame_scope)
    {
        return Err(DurableError::Validation(format!(
            "Continuation {} active frame is outside its scope stack",
            continuation.run_id
        )));
    }
    Ok(())
}

/// Validate that every persisted frame is at an exact Plan location and that
/// each adjacent child is owned by the parent frame's current structured step.
///
/// # Errors
/// Returns an error if a frame has a foreign identity, an invalid program
/// counter, or a parent-child relationship absent from the sealed Plan.
pub fn validate_continuation_plan_frames(
    plan: &SealedPlan,
    continuation: &Continuation,
) -> DurableResult<()> {
    for (index, frame) in continuation.frames.iter().enumerate() {
        let expected_invocation = cymule_core::plan_invocation_id(
            &continuation.run_id,
            &plan.plan_id,
            &plan.candidate.entry,
            &frame.invocation_path,
        )?;
        if frame.invocation_id != expected_invocation {
            return Err(DurableError::Validation(format!(
                "Continuation {} frame {index} has an invalid invocation identity",
                continuation.run_id
            )));
        }
        let definition = plan
            .candidate
            .definitions
            .iter()
            .find(|definition| definition.id == frame.definition_id)
            .ok_or_else(|| {
                DurableError::Validation(format!(
                    "Continuation {} frame {index} references missing definition {}",
                    continuation.run_id, frame.definition_id
                ))
            })?;
        let region = region_at_path(&definition.body, &frame.region_path)?;
        if frame.next_step > region.steps.len() {
            return Err(DurableError::Validation(format!(
                "Continuation {} frame {index} program counter is outside its Region",
                continuation.run_id
            )));
        }
        if index == 0 {
            if frame.definition_id != plan.candidate.entry
                || !frame.invocation_path.is_empty()
                || !frame.region_path.is_empty()
            {
                return Err(DurableError::Validation(format!(
                    "Continuation {} first frame is not the entry invocation",
                    continuation.run_id
                )));
            }
            continue;
        }

        validate_continuation_parent_frame(
            plan,
            &continuation.run_id,
            index,
            &continuation.frames[index - 1],
            frame,
        )?;
    }
    Ok(())
}

fn validate_continuation_parent_frame(
    plan: &SealedPlan,
    run_id: &str,
    index: usize,
    parent: &cymule_durable_protocol::FrameState,
    frame: &cymule_durable_protocol::FrameState,
) -> DurableResult<()> {
    let parent_definition = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == parent.definition_id)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "Continuation {run_id} parent frame references missing definition {}",
                parent.definition_id
            ))
        })?;
    let parent_region = region_at_path(&parent_definition.body, &parent.region_path)?;
    let parent_step = parent_region.steps.get(parent.next_step).ok_or_else(|| {
        DurableError::Validation(format!(
            "Continuation {run_id} parent frame does not point at its active child step"
        ))
    })?;
    match &parent_step.operation {
        Operation::Scope { .. } => {
            let mut expected_path = parent.region_path.clone();
            expected_path.push(parent.next_step);
            if frame.invocation_path != parent.invocation_path
                || frame.invocation_id != parent.invocation_id
                || frame.definition_id != parent.definition_id
                || frame.region_path != expected_path
            {
                return Err(DurableError::Validation(format!(
                    "Continuation {run_id} scope frame {index} is not owned by its parent step"
                )));
            }
        }
        Operation::Invoke { definition, .. } => {
            let Some(segment) = frame.invocation_path.last() else {
                return Err(DurableError::Validation(format!(
                    "Continuation {run_id} invoked frame {index} has no call-site segment"
                )));
            };
            if frame.invocation_path.len() != parent.invocation_path.len() + 1
                || !frame.invocation_path.starts_with(&parent.invocation_path)
                || segment.site_id != parent_step.id
                || segment.region_path != parent.region_path
                || segment.scope_id != parent.scope_id
                || frame.definition_id != *definition
                || !frame.region_path.is_empty()
            {
                return Err(DurableError::Validation(format!(
                    "Continuation {run_id} invoked frame {index} is not owned by its parent step"
                )));
            }
        }
        _ => {
            return Err(DurableError::Validation(format!(
                "Continuation {run_id} parent frame points at a non-structured child step"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_wait_artifacts(
    machine: &Machine,
    continuation: &Continuation,
    wait: &WaitCondition,
) -> DurableResult<()> {
    match &wait.kind {
        WaitKind::Signal { key } => validate_wire_non_empty("signal key", key)?,
        WaitKind::Timer { timer_id } => validate_wire_non_empty("timer identity", timer_id)?,
        WaitKind::Input { correlation, .. } => {
            validate_wire_non_empty("input correlation", correlation)?;
        }
    }
    match wait.state {
        WaitState::Pending | WaitState::Completed | WaitState::Cancelled => {}
    }
    if let Some(result) = &wait.result {
        require_artifact(machine, result, &format!("wait {} result", wait.wait_id))?;
    }
    let owner = &wait.owner;
    for (kind, identity) in [
        ("wait owner invocation", owner.invocation_id.as_str()),
        ("wait owner definition", owner.definition_id.as_str()),
        ("wait owner site", owner.site_id.as_str()),
    ] {
        validate_wire_non_empty(kind, identity)?;
    }
    if let Some(bind) = &owner.bind {
        validate_wire_non_empty("wait owner bind", bind)?;
    }
    let frame = continuation.frames.iter().find(|frame| {
        frame.invocation_id == owner.invocation_id
            && frame.definition_id == owner.definition_id
            && frame.region_path == owner.region_path
    });
    validate_wait_plan_owner(machine, continuation, wait)?;
    match (wait.state, wait.result.as_ref(), frame, owner.bind.as_ref()) {
        (WaitState::Pending, None, Some(frame), bind)
            if frame.next_step == owner.step_index + 1
                && bind.is_none_or(|bind| !frame.locals.contains_key(bind)) => {}
        (WaitState::Completed, Some(result), Some(frame), Some(bind))
            if frame.next_step > owner.step_index && frame.locals.get(bind) == Some(result) => {}
        (WaitState::Completed, Some(_), Some(frame), None)
            if frame.next_step > owner.step_index => {}
        (WaitState::Completed, Some(_), None, _) | (WaitState::Cancelled, None, None, _) => {}
        (WaitState::Cancelled, None, Some(frame), bind)
            if frame.next_step > owner.step_index
                && bind.is_none_or(|bind| !frame.locals.contains_key(bind)) => {}
        _ => {
            return Err(DurableError::Validation(format!(
                "wait {} owner is not reflected by its frame",
                wait.wait_id
            )));
        }
    }
    Ok(())
}

fn validate_wait_plan_owner(
    machine: &Machine,
    continuation: &Continuation,
    wait: &WaitCondition,
) -> DurableResult<()> {
    let owner = &wait.owner;
    let origin_plan_id = retained_wait_origin_plan(machine, &continuation.plan_id, wait)?;
    let plan = machine.plan(origin_plan_id).ok_or_else(|| {
        DurableError::Validation(format!(
            "wait {} owning Plan {} is missing",
            wait.wait_id, origin_plan_id
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
    let Operation::Wait {
        wait: planned_wait,
        bind: operation_result_binding,
    } = &step.operation
    else {
        return Err(DurableError::Validation(format!(
            "wait {} owner does not match its Plan site",
            wait.wait_id
        )));
    };
    let (materialized_kind, materialized_consume_once) = match planned_wait {
        cymule_core::WaitSpec::Signal { key, consume_once } => {
            (WaitKind::Signal { key: key.clone() }, *consume_once)
        }
        cymule_core::WaitSpec::Timer { timer_id } => (
            WaitKind::Timer {
                timer_id: timer_id.clone(),
            },
            false,
        ),
        cymule_core::WaitSpec::Input {
            correlation,
            schema,
        } => (
            WaitKind::Input {
                correlation: correlation.clone(),
                schema: schema.clone(),
            },
            false,
        ),
    };
    let planned_wait_id = derive_wait_id(
        &wait.run_id,
        origin_plan_id,
        &owner.invocation_id,
        &owner.site_id,
    )?;
    if step.id != owner.site_id
        || operation_result_binding != &owner.bind
        || wait.wait_id != planned_wait_id
        || wait.kind != materialized_kind
        || wait.consume_once != materialized_consume_once
    {
        return Err(DurableError::Validation(format!(
            "wait {} semantics do not match its Plan site",
            wait.wait_id
        )));
    }
    Ok(())
}

/// Offline retained-state audit resolves terminal Wait origins from the Run's
/// authenticated Plan lineage. Pending work remains bound to the current Plan.
fn retained_wait_origin_plan<'a>(
    machine: &'a Machine,
    current_plan_id: &'a str,
    wait: &WaitCondition,
) -> DurableResult<&'a str> {
    if wait.state == WaitState::Pending {
        return Ok(current_plan_id);
    }
    let run = machine.projection().runs.get(&wait.run_id).ok_or_else(|| {
        DurableError::Validation(format!("wait {} owning Run is missing", wait.wait_id))
    })?;
    let mut origin = None;
    let plans = run
        .plan_lineage
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for plan_id in plans {
        let candidate = derive_wait_id(
            &wait.run_id,
            plan_id,
            &wait.owner.invocation_id,
            &wait.owner.site_id,
        )?;
        if candidate == wait.wait_id && origin.replace(plan_id).is_some() {
            return Err(DurableError::Validation(format!(
                "wait {} has ambiguous origins in its Run Plan lineage",
                wait.wait_id
            )));
        }
    }
    origin.ok_or_else(|| {
        DurableError::Validation(format!(
            "wait {} has no origin in its Run's admitted Plan lineage",
            wait.wait_id
        ))
    })
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

fn validate_effect_outbox_closure(
    machine: &Machine,
    outbox: &BTreeMap<String, EffectDispatch>,
) -> DurableResult<()> {
    let mut machine_effect_intents = BTreeSet::new();
    for run in machine.projection().runs.values() {
        for intent_id in run.effects.keys() {
            if !machine_effect_intents.insert(intent_id) {
                return Err(DurableError::Integrity {
                    code: "machine_effect_intent_not_unique".to_owned(),
                    message: format!(
                        "Machine Effect intent {intent_id} appears under more than one Run"
                    ),
                });
            }
            let dispatch = outbox
                .get(intent_id)
                .ok_or_else(|| DurableError::Integrity {
                    code: "machine_effect_outbox_missing".to_owned(),
                    message: format!(
                        "Machine Effect intent {intent_id} in Run {} has no durable outbox entry",
                        run.run_id
                    ),
                })?;
            if dispatch.run_id != run.run_id {
                return Err(DurableError::Integrity {
                    code: "machine_effect_outbox_run_mismatch".to_owned(),
                    message: format!(
                        "Machine Effect intent {intent_id} outbox escaped Run {}",
                        run.run_id
                    ),
                });
            }
        }
    }
    if machine_effect_intents.len() != outbox.len() {
        return Err(DurableError::Integrity {
            code: "machine_effect_outbox_set_mismatch".to_owned(),
            message: "Machine Effect and durable outbox intent sets are not exact".to_owned(),
        });
    }
    Ok(())
}

/// Project the exact Core result onto an already derived outbox state.
pub(crate) fn synchronize_pinned_effect_projection(
    effect: &cymule_core::EffectProjection,
    dispatch: &mut EffectDispatch,
) -> DurableResult<()> {
    if effect.intent_id != dispatch.intent_id
        || effect.origin_plan_id != dispatch.origin_plan_id
        || effect.operation != dispatch.operation
        || effect.args != dispatch.input
        || effect.execution_binding != dispatch.execution_binding
        || effect.occurrence_binding != dispatch.occurrence_binding
        || !matches!(
            (dispatch.state, effect.phase, effect.outcome),
            (
                OutboxState::Pending,
                cymule_core::EffectPhase::Prepared | cymule_core::EffectPhase::ReleaseAuthorized,
                WorldOutcome::Unobserved
            ) | (
                OutboxState::Claimed,
                cymule_core::EffectPhase::DispatchStarted,
                WorldOutcome::Unobserved
            ) | (
                OutboxState::Applied,
                cymule_core::EffectPhase::DispatchStarted,
                WorldOutcome::Applied
            ) | (
                OutboxState::NotApplied,
                cymule_core::EffectPhase::DispatchStarted,
                WorldOutcome::NotApplied
            ) | (
                OutboxState::Unknown,
                cymule_core::EffectPhase::DispatchStarted,
                WorldOutcome::Unknown
            ) | (
                OutboxState::CancelledBeforeRelease,
                cymule_core::EffectPhase::CancelledBeforeRelease,
                WorldOutcome::NotApplied
            )
        )
    {
        return Err(DurableError::Integrity {
            code: "pinned_effect_outbox_transition_mismatch".to_owned(),
            message: format!(
                "Effect {} and outbox transition disagree",
                dispatch.intent_id
            ),
        });
    }
    dispatch.execution_availability = effect.execution_availability;
    dispatch.reconciliation = effect.reconciliation;
    Ok(())
}

pub(crate) fn validate_dispatch_effect_projection(
    effect: &cymule_core::EffectProjection,
    dispatch: &EffectDispatch,
) -> DurableResult<()> {
    if effect.intent_id != dispatch.intent_id
        || effect.origin_plan_id != dispatch.origin_plan_id
        || effect.operation != dispatch.operation
        || effect.args != dispatch.input
        || effect.execution_binding != dispatch.execution_binding
        || effect.occurrence_binding != dispatch.occurrence_binding
        || effect.execution_availability != dispatch.execution_availability
        || effect.reconciliation != dispatch.reconciliation
    {
        return Err(DurableError::Validation(format!(
            "Effect intent {} outbox origin does not match its Machine occurrence",
            dispatch.intent_id
        )));
    }
    let state_matches = match dispatch.state {
        OutboxState::Pending => {
            matches!(
                effect.phase,
                cymule_core::EffectPhase::Prepared | cymule_core::EffectPhase::ReleaseAuthorized
            ) && effect.outcome == cymule_core::WorldOutcome::Unobserved
        }
        OutboxState::Claimed => {
            effect.phase == cymule_core::EffectPhase::DispatchStarted
                && effect.outcome == cymule_core::WorldOutcome::Unobserved
        }
        OutboxState::Applied => effect.outcome == cymule_core::WorldOutcome::Applied,
        OutboxState::NotApplied => effect.outcome == cymule_core::WorldOutcome::NotApplied,
        OutboxState::Unknown => effect.outcome == cymule_core::WorldOutcome::Unknown,
        OutboxState::CancelledBeforeRelease => {
            effect.phase == cymule_core::EffectPhase::CancelledBeforeRelease
                && matches!(
                    effect.outcome,
                    cymule_core::WorldOutcome::Unobserved | cymule_core::WorldOutcome::NotApplied
                )
        }
    };
    if !state_matches {
        return Err(DurableError::Validation(format!(
            "Effect intent {} outbox state disagrees with its Machine occurrence",
            dispatch.intent_id
        )));
    }
    Ok(())
}

pub(crate) fn validate_dispatch_artifacts(
    machine: &Machine,
    dispatch: &EffectDispatch,
) -> DurableResult<()> {
    match dispatch.state {
        OutboxState::Pending
        | OutboxState::Claimed
        | OutboxState::Applied
        | OutboxState::NotApplied
        | OutboxState::Unknown
        | OutboxState::CancelledBeforeRelease => {}
    }
    let run = machine
        .projection()
        .runs
        .get(&dispatch.run_id)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "Effect intent {} references missing Run {}",
                dispatch.intent_id, dispatch.run_id
            ))
        })?;
    let effect = run.effects.get(&dispatch.intent_id).ok_or_else(|| {
        DurableError::Validation(format!(
            "Effect intent {} is missing from its Machine Run",
            dispatch.intent_id
        ))
    })?;
    validate_dispatch_effect_projection(effect, dispatch)?;
    if dispatch.execution_binding.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
        return Err(DurableError::Validation(format!(
            "Effect intent {} execution binding has the wrong Artifact kind",
            dispatch.intent_id
        )));
    }
    let origin_plan = machine.plan(&dispatch.origin_plan_id).ok_or_else(|| {
        DurableError::Validation(format!(
            "Effect intent {} origin Plan {} is missing",
            dispatch.intent_id, dispatch.origin_plan_id
        ))
    })?;
    let binding_record = machine
        .artifact(&dispatch.execution_binding)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "Effect intent {} execution binding Artifact is missing",
                dispatch.intent_id
            ))
        })?;
    let binding = cymule_runtime::ExecutionBinding::decode(&binding_record.bytes)?;
    if binding.artifact_ref()? != dispatch.execution_binding {
        return Err(DurableError::Validation(format!(
            "Effect intent {} execution binding identity is invalid",
            dispatch.intent_id
        )));
    }
    binding.admit_plan(origin_plan)?;
    if binding.occurrence_binding(
        cymule_runtime::ExecutionOperationKind::Effect,
        &dispatch.operation,
    )? != dispatch.occurrence_binding
    {
        return Err(DurableError::Validation(format!(
            "Effect intent {} operation binding is not derived from its origin execution binding",
            dispatch.intent_id
        )));
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
    if dispatch.state == OutboxState::CancelledBeforeRelease {
        let run = machine
            .projection()
            .runs
            .get(&dispatch.run_id)
            .ok_or_else(|| DurableError::NotFound(format!("Run {} is missing", dispatch.run_id)))?;
        let effect = run.effects.get(&dispatch.intent_id).ok_or_else(|| {
            DurableError::NotFound(format!("effect {} is missing", dispatch.intent_id))
        })?;
        if effect.phase != cymule_core::EffectPhase::CancelledBeforeRelease
            || dispatch.claim_epoch != 0
            || dispatch.claim_owner.is_some()
            || dispatch.result.is_some()
        {
            return Err(DurableError::Validation(format!(
                "Effect intent {} has an invalid cancellation disposition",
                dispatch.intent_id
            )));
        }
    }
    Ok(())
}

pub(crate) fn require_artifact(
    machine: &Machine,
    reference: &ArtifactRef,
    owner: &str,
) -> DurableResult<()> {
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

/// One idempotent M1 Machine-history compaction receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCompactionReceipt {
    /// Receipt schema and semantic version.
    pub compaction_version: String,
    /// Stable command/idempotency identity.
    pub compaction_id: String,
    /// Previous compaction in the cumulative base lineage.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_compaction: Option<String>,
    /// Closed compaction operation that produced this archive segment.
    pub kind: HistoryCompactionKind,
    /// Durable revision from which compaction was computed.
    pub source_revision: String,
    /// Requested full suffix length.
    pub requested_suffix: u64,
    /// Canonical Machine compaction evidence.
    pub result: MachineCompactionSummary,
}

/// Closed Machine-history compaction operations owned by M1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryCompactionKind {
    /// Move a non-empty causal Event prefix and every admission through its cut.
    EventPrefix,
    /// Move a non-empty Event-free tail of conflict admissions and material-only batches.
    EventFreeAdmissions,
}

/// Bounded durable projection of one Core Machine compaction result. The full
/// command archive segment is stored independently by the Store batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCompactionSummary {
    /// Content-addressed base snapshot identity.
    pub base_id: String,
    /// Cumulative number of compacted Event identities.
    pub compacted_events: u64,
    /// Number of full suffix Events retained for resume.
    pub retained_events: u64,
    /// Causal frontier connecting the base to retained execution.
    pub causal_frontier: BTreeSet<String>,
    /// Authenticated base projection digest.
    pub projection_digest: String,
    /// Header of the independently stored command archive segment.
    pub archive_segment: cymule_core::MachineCommandArchiveSegmentHeader,
}

impl From<&cymule_core::MachineCompaction> for MachineCompactionSummary {
    fn from(result: &cymule_core::MachineCompaction) -> Self {
        Self {
            base_id: result.base_id.clone(),
            compacted_events: result.compacted_events,
            retained_events: result.retained_events,
            causal_frontier: result.causal_frontier.clone(),
            projection_digest: result.projection_digest.clone(),
            archive_segment: result.archive_segment.header.clone(),
        }
    }
}

impl HistoryCompactionReceipt {
    /// Verify stable identities and bounded result metadata.
    ///
    /// # Errors
    /// Returns an error for malformed identities, unsupported metadata, or an
    /// inconsistent compaction result and archive boundary.
    pub fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty("history compaction", &self.compaction_id)?;
        if let Some(parent) = &self.parent_compaction {
            validate_wire_non_empty("parent history compaction", parent)?;
        }
        cymule_core::validate_content_id(
            "history compaction source revision",
            &self.source_revision,
        )?;
        cymule_core::validate_content_id("history compaction base", &self.result.base_id)?;
        validate_lower_hex_digest(
            "history compaction projection digest",
            &self.result.projection_digest,
        )?;
        for event_id in &self.result.causal_frontier {
            cymule_core::validate_content_id("history compaction frontier Event", event_id)?;
        }
        if self.compaction_version != HISTORY_COMPACTION_VERSION
            || self.requested_suffix > cymule_core::MAX_EXACT_INTEGER
            || self.result.retained_events != self.requested_suffix
            || self.result.archive_segment.verify().is_err()
            || self.result.archive_segment.result_event_count != self.result.compacted_events
        {
            return Err(DurableError::Validation(
                "history compaction receipt is malformed".to_owned(),
            ));
        }
        let kind_matches = match self.kind {
            HistoryCompactionKind::EventPrefix => {
                self.result.compacted_events > 0
                    && self.result.archive_segment.event_count > 0
                    && !self.result.causal_frontier.is_empty()
            }
            HistoryCompactionKind::EventFreeAdmissions => {
                self.requested_suffix == 0
                    && self.result.retained_events == 0
                    && self.result.archive_segment.event_count == 0
            }
        };
        if !kind_matches {
            return Err(DurableError::Validation(
                "history compaction receipt kind does not match its archive segment".to_owned(),
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

/// Ordered active records for one application journal.
///
/// The wire representation is the same JSON sequence previously used by the
/// journal field. Mutation stays inside the durable coordinator so callers
/// cannot bypass append or authenticated prefix-replacement admission.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ApplicationJournal {
    order: VecDeque<String>,
    records: BTreeMap<String, Box<JournalRecord>>,
}

/// Read-only ordered iterator over the active records of one application
/// journal.
pub struct ApplicationJournalIter<'a> {
    order: std::collections::vec_deque::Iter<'a, String>,
    records: &'a BTreeMap<String, Box<JournalRecord>>,
}

impl<'a> Iterator for ApplicationJournalIter<'a> {
    type Item = &'a JournalRecord;

    fn next(&mut self) -> Option<Self::Item> {
        self.order
            .next()
            .map(|record_id| self.records[record_id].as_ref())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.order.size_hint()
    }
}

impl DoubleEndedIterator for ApplicationJournalIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.order
            .next_back()
            .map(|record_id| self.records[record_id].as_ref())
    }
}

impl ExactSizeIterator for ApplicationJournalIter<'_> {}
impl std::iter::FusedIterator for ApplicationJournalIter<'_> {}

impl Serialize for ApplicationJournal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.iter().collect::<Vec<_>>().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ApplicationJournal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let records = Vec::<JournalRecord>::deserialize(deserializer)?;
        Self::try_from_records(records).map_err(D::Error::custom)
    }
}

impl ApplicationJournal {
    /// Return the number of active records.
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Return whether the active journal is empty.
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Iterate over active records in journal order.
    pub fn iter(&self) -> ApplicationJournalIter<'_> {
        ApplicationJournalIter {
            order: self.order.iter(),
            records: &self.records,
        }
    }

    /// Return the active record at `index`.
    pub fn get(&self, index: usize) -> Option<&JournalRecord> {
        self.order
            .get(index)
            .and_then(|record_id| self.records.get(record_id).map(Box::as_ref))
    }

    /// Return the first active record.
    pub fn front(&self) -> Option<&JournalRecord> {
        self.order
            .front()
            .and_then(|record_id| self.records.get(record_id).map(Box::as_ref))
    }

    /// Return the final active record.
    pub fn back(&self) -> Option<&JournalRecord> {
        self.order
            .back()
            .and_then(|record_id| self.records.get(record_id).map(Box::as_ref))
    }

    /// Return the final active record.
    pub fn last(&self) -> Option<&JournalRecord> {
        self.back()
    }

    /// Materialize an explicit read-only snapshot of the active records.
    pub fn to_vec(&self) -> Vec<JournalRecord> {
        self.iter().cloned().collect()
    }

    /// Resolve one exact active record without scanning the journal or falling
    /// back to all-ever compacted history.
    pub fn active_record(&self, record_id: &str) -> Option<&JournalRecord> {
        self.records.get(record_id).map(Box::as_ref)
    }

    pub(crate) fn try_from_records(records: Vec<JournalRecord>) -> DurableResult<Self> {
        let mut journal = Self::default();
        for record in records {
            journal.append(record)?;
        }
        Ok(journal)
    }

    pub(crate) fn append(&mut self, record: JournalRecord) -> DurableResult<()> {
        record.verify()?;
        if self.records.contains_key(&record.record_id) {
            return Err(DurableError::Validation(format!(
                "application journal repeats active record {}",
                record.record_id
            )));
        }
        self.order.push_back(record.record_id.clone());
        self.records
            .insert(record.record_id.clone(), Box::new(record));
        Ok(())
    }
}

impl Index<usize> for ApplicationJournal {
    type Output = JournalRecord;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index)
            .expect("application journal index is outside the active journal")
    }
}

impl<'a> IntoIterator for &'a ApplicationJournal {
    type Item = &'a JournalRecord;
    type IntoIter = ApplicationJournalIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// One journal and its records in an atomic multi-journal checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalBatch {
    /// Stable higher-profile journal identity.
    pub journal_id: String,
    /// Ordered idempotent records to append.
    pub records: Vec<JournalRecord>,
}

#[cfg(test)]
impl JournalBatch {
    fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty("coupled application journal", &self.journal_id)?;
        if self.records.is_empty() {
            return Err(DurableError::Validation(format!(
                "coupled application journal {} has no records",
                self.journal_id
            )));
        }
        let mut record_ids = BTreeSet::new();
        for record in &self.records {
            record.verify()?;
            if !record_ids.insert(&record.record_id) {
                return Err(DurableError::Validation(format!(
                    "coupled application journal {} repeats record {}",
                    self.journal_id, record.record_id
                )));
            }
        }
        Ok(())
    }
}

/// Payload-free immutable authority for one journal record retained by a
/// coupled receipt or compacted-record history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalRecordManifest {
    /// Stable record identity.
    pub record_id: String,
    /// Exact typed payload schema.
    pub schema: String,
    /// Canonical digest of schema plus payload.
    pub content_digest: String,
    /// Canonical digest of the complete record including its identity.
    pub record_digest: String,
}

impl JournalRecordManifest {
    pub(crate) fn from_record(record: &JournalRecord) -> DurableResult<Self> {
        record.verify()?;
        let manifest = Self {
            record_id: record.record_id.clone(),
            schema: record.schema.clone(),
            content_digest: record.content_digest.clone(),
            record_digest: canonical_digest(record)?,
        };
        manifest.verify()?;
        Ok(manifest)
    }

    /// Verify the closed payload-free record authority.
    ///
    /// # Errors
    /// Returns an error for malformed identities, digests, or encoded-size bounds.
    pub fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty("journal manifest record", &self.record_id)?;
        validate_wire_non_empty("journal manifest schema", &self.schema)?;
        validate_lower_hex_digest("journal manifest content digest", &self.content_digest)?;
        validate_lower_hex_digest("journal manifest record digest", &self.record_digest)
    }
}

/// Ordered payload-free manifest of one exact journal batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalBatchManifest {
    /// Stable journal identity.
    pub journal_id: String,
    /// Exact ordered record manifests.
    pub records: Vec<JournalRecordManifest>,
}

impl JournalBatchManifest {
    #[cfg(test)]
    pub(crate) fn from_batch(batch: &JournalBatch) -> DurableResult<Self> {
        batch.verify()?;
        let manifest = Self {
            journal_id: batch.journal_id.clone(),
            records: batch
                .records
                .iter()
                .map(JournalRecordManifest::from_record)
                .collect::<DurableResult<_>>()?,
        };
        manifest.verify()?;
        Ok(manifest)
    }

    /// Verify one ordered non-empty batch manifest.
    ///
    /// # Errors
    /// Returns an error for an invalid journal identity, empty record set, or
    /// invalid or repeated record manifests.
    pub fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty("journal manifest", &self.journal_id)?;
        if self.records.is_empty() {
            return Err(DurableError::Validation(format!(
                "journal manifest {} has no records",
                self.journal_id
            )));
        }
        let mut ids = BTreeSet::new();
        for record in &self.records {
            record.verify()?;
            if !ids.insert(&record.record_id) {
                return Err(DurableError::Validation(format!(
                    "journal manifest {} repeats record {}",
                    self.journal_id, record.record_id
                )));
            }
        }
        Ok(())
    }
}

/// Exact M1 authority before and after one outer Agent workspace command.
///
/// This receipt contains neither the final Agent receipt nor its physical
/// `StateRoot`, so the M1 and Agent receipts can be committed without a cycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWorkspaceCheckpoint {
    /// Exact outer Agent command, not one of its internal Core member IDs.
    pub agent_command_id: String,
    /// Exact owning Run.
    pub run_id: String,
    /// Exact owning workspace Scope.
    pub scope_id: String,
    /// Exact Agent host occurrence.
    pub occurrence_id: String,
    /// Terminal M1 phase performed for this outer command.
    pub phase: cymule_profile_protocol::agent::AgentWorkspaceCommandPhase,
    /// Core authority immediately before the coupled transition.
    pub source_machine_authority_root: String,
    /// Core authority immediately after the coupled transition.
    pub machine_authority_root: String,
    /// Exact real Core batch, explicitly null when this command changes no M1 state.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub core_batch_id: Option<String>,
    /// Exact terminal receipt for that same batch, explicitly null with its ID.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub core_batch_receipt_id: Option<String>,
    /// Exact issued observation consumed under the `StartEffect` dispatch guard.
    /// Other phases retain null and resolve this history through the start receipt.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub dispatch_clock: Option<cymule_durable_protocol::ClockObservation>,
    /// Source Continuation content ID under `CONTINUATION_STATE_VERSION`.
    pub source_continuation_digest: String,
    /// Complete exact target Continuation.
    pub continuation: Box<Continuation>,
    /// Target Continuation content ID under `CONTINUATION_STATE_VERSION`.
    pub continuation_digest: String,
    /// Exact Core Effect before the transition; absent for a new Effect or abort.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub effect_before: Option<cymule_core::EffectProjection>,
    /// Exact Core Effect afterward; absent for abort commands.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub effect_after: Option<cymule_core::EffectProjection>,
    /// Exact outbox row before the transition.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub outbox_before: Option<EffectDispatch>,
    /// Exact outbox row after the transition.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub outbox_after: Option<EffectDispatch>,
    /// Exact single-intent dispatch lease before the transition.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub lease_before: Option<CoordinationLease>,
    /// Exact single-intent dispatch lease after the transition.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub lease_after: Option<CoordinationLease>,
}

impl AgentWorkspaceCheckpoint {
    /// Validate the bounded local closure. The `StateRoot` owner separately
    /// proves these exact source/target values and the referenced real batch.
    ///
    /// # Errors
    /// Returns an error if the phase, exact authority pair, Continuation digest,
    /// dispatch Clock, or single-Effect neighborhood is malformed or inconsistent.
    pub fn verify(&self) -> DurableResult<()> {
        validate_sha256_identity("Agent workspace command", &self.agent_command_id)?;
        validate_wire_non_empty("Agent workspace Run", &self.run_id)?;
        validate_wire_non_empty("Agent workspace Scope", &self.scope_id)?;
        validate_wire_non_empty("Agent workspace occurrence", &self.occurrence_id)?;
        validate_lower_hex_digest(
            "Agent workspace source Core root",
            &self.source_machine_authority_root,
        )?;
        validate_lower_hex_digest(
            "Agent workspace result Core root",
            &self.machine_authority_root,
        )?;
        validate_sha256_identity(
            "Agent workspace source Continuation",
            &self.source_continuation_digest,
        )?;
        validate_sha256_identity(
            "Agent workspace target Continuation",
            &self.continuation_digest,
        )?;
        if self.continuation.run_id != self.run_id
            || agent_workspace_continuation_digest(&self.continuation)? != self.continuation_digest
        {
            return Err(DurableError::Integrity {
                code: "agent_workspace_continuation_mismatch".to_owned(),
                message: "Agent workspace receipt changed its exact target Continuation".to_owned(),
            });
        }
        match (&self.core_batch_id, &self.core_batch_receipt_id) {
            (Some(batch_id), Some(receipt_id)) => {
                validate_sha256_identity("Agent workspace Core batch", batch_id)?;
                validate_sha256_identity("Agent workspace Core batch receipt", receipt_id)?;
                if self.source_machine_authority_root == self.machine_authority_root {
                    return Err(DurableError::Validation(
                        "Agent workspace Core batch must advance its exact Core authority"
                            .to_owned(),
                    ));
                }
            }
            (None, None) => self.verify_no_core_change()?,
            _ => {
                return Err(DurableError::Validation(
                    "Agent workspace batch and receipt IDs must be present or null together"
                        .to_owned(),
                ));
            }
        }
        self.verify_neighborhood(
            self.effect_before.as_ref(),
            self.outbox_before.as_ref(),
            self.lease_before.as_ref(),
        )?;
        self.verify_neighborhood(
            self.effect_after.as_ref(),
            self.outbox_after.as_ref(),
            self.lease_after.as_ref(),
        )?;
        if let (Some(before), Some(after)) = (&self.effect_before, &self.effect_after) {
            let mut retained = before.clone();
            retained.phase = after.phase;
            retained.outcome = after.outcome;
            retained.reconciliation = after.reconciliation;
            retained.execution_availability = after.execution_availability;
            if retained != *after {
                return Err(DurableError::Integrity {
                    code: "agent_workspace_effect_origin_changed".to_owned(),
                    message: "Agent workspace receipt changed an immutable Effect origin"
                        .to_owned(),
                });
            }
        }
        self.verify_dispatch_clock()?;
        self.verify_phase()
    }

    fn verify_dispatch_clock(&self) -> DurableResult<()> {
        use cymule_profile_protocol::agent::AgentWorkspaceCommandPhase as Phase;
        match (&self.dispatch_clock, self.phase) {
            (Some(clock), Phase::StartEffectDispatch) => {
                clock.verify()?;
                let lease = self
                    .lease_after
                    .as_ref()
                    .ok_or_else(|| DurableError::Integrity {
                        code: "agent_workspace_dispatch_clock_lease_missing".to_owned(),
                        message: "Agent workspace dispatch Clock has no exact result lease"
                            .to_owned(),
                    })?;
                if clock.scope != cymule_durable_protocol::execution_clock_scope(&self.run_id)?
                    || lease.expires_at <= clock.logical_time
                {
                    return Err(DurableError::Integrity {
                        code: "agent_workspace_dispatch_clock_mismatch".to_owned(),
                        message: "Agent workspace dispatch Clock changed its Run scope or positive lease interval".to_owned(),
                    });
                }
                Ok(())
            }
            (None, phase) if phase != Phase::StartEffectDispatch => Ok(()),
            _ => Err(DurableError::Integrity {
                code: "agent_workspace_dispatch_clock_presence_mismatch".to_owned(),
                message:
                    "only Agent workspace StartEffect retains its exact dispatch Clock receipt"
                        .to_owned(),
            }),
        }
    }

    fn verify_no_core_change(&self) -> DurableResult<()> {
        use cymule_profile_protocol::agent::AgentWorkspaceCommandPhase as Phase;
        if !matches!(
            self.phase,
            Phase::StartAbortDispatch
                | Phase::SettleAbortNotApplied
                | Phase::SettleAbortUnknown
                | Phase::SettleEffectUnknown
        ) || self.source_machine_authority_root != self.machine_authority_root
            || !self.m1_values_unchanged()
        {
            return Err(DurableError::Integrity {
                code: "agent_workspace_no_core_change_mismatch".to_owned(),
                message: "Agent workspace without a Core batch must preserve every exact M1 value"
                    .to_owned(),
            });
        }
        Ok(())
    }

    fn m1_values_unchanged(&self) -> bool {
        self.source_continuation_digest == self.continuation_digest
            && self.effect_before == self.effect_after
            && self.outbox_before == self.outbox_after
            && self.lease_before == self.lease_after
    }

    fn verify_neighborhood(
        &self,
        effect: Option<&cymule_core::EffectProjection>,
        outbox: Option<&EffectDispatch>,
        lease: Option<&CoordinationLease>,
    ) -> DurableResult<()> {
        let (Some(effect), Some(outbox)) = (effect, outbox) else {
            return if effect.is_none() && outbox.is_none() && lease.is_none() {
                Ok(())
            } else {
                Err(DurableError::Integrity {
                    code: "agent_workspace_partial_effect_neighborhood".to_owned(),
                    message: "Agent workspace receipt has an incomplete Effect/outbox neighborhood"
                        .to_owned(),
                })
            };
        };
        outbox.verify_wire()?;
        verify_workspace_effect_path(effect)?;
        effect.args.validate()?;
        effect.execution_binding.validate()?;
        validate_sha256_identity("Agent workspace origin Plan", &effect.origin_plan_id)?;
        validate_sha256_identity("Agent workspace invocation", &effect.invocation_id)?;
        let expected_intent =
            cymule_core::effect_intent_id(&cymule_core::EffectIntentIdentityInput {
                run_id: &self.run_id,
                plan_id: &effect.origin_plan_id,
                invocation_id: &effect.invocation_id,
                site_id: &effect.site_id,
                scope_id: &effect.scope_id,
                occurrence: &effect.occurrence,
                args: &effect.args,
                effect_schema_version: &effect.effect_schema_version,
            })?;
        if outbox.run_id != self.run_id
            || effect.scope_id != self.scope_id
            || effect.intent_id != expected_intent
            || effect.effect_schema_version != cymule_core::EFFECT_SCHEMA_VERSION
        {
            return Err(DurableError::Integrity {
                code: "agent_workspace_effect_owner_mismatch".to_owned(),
                message: "Agent workspace receipt changed its one structural Effect owner"
                    .to_owned(),
            });
        }
        validate_dispatch_effect_projection(effect, outbox)?;
        match (lease, outbox.claim_owner.as_deref()) {
            (Some(lease), Some(owner)) => {
                lease.verify()?;
                if lease.resource != effect.intent_id
                    || lease.owner != owner
                    || lease.epoch != outbox.claim_epoch
                {
                    return Err(DurableError::Integrity {
                        code: "agent_workspace_lease_mismatch".to_owned(),
                        message:
                            "Agent workspace lease differs from its exact Effect dispatch claim"
                                .to_owned(),
                    });
                }
            }
            (None, None) => {}
            _ => {
                return Err(DurableError::Integrity {
                    code: "agent_workspace_lease_presence_mismatch".to_owned(),
                    message: "Agent workspace dispatch claim and lease must be retained together"
                        .to_owned(),
                });
            }
        }
        Ok(())
    }

    fn verify_phase(&self) -> DurableResult<()> {
        use cymule_profile_protocol::agent::AgentWorkspaceCommandPhase as Phase;
        let expected_outbox = match self.phase {
            Phase::StartEffectDispatch => Some(OutboxState::Claimed),
            Phase::SettleEffectApplied => Some(OutboxState::Applied),
            Phase::SettleEffectNotApplied => Some(OutboxState::NotApplied),
            Phase::SettleEffectUnknown => Some(OutboxState::Unknown),
            Phase::StartAbortDispatch
            | Phase::SettleAbortApplied
            | Phase::SettleAbortNotApplied
            | Phase::SettleAbortUnknown => None,
            Phase::ProposeEffect
            | Phase::PrepareEffect
            | Phase::CommitScope
            | Phase::AuthorizeEffect => {
                return Err(DurableError::Validation(
                    "Agent workspace checkpoint must name the outer command's terminal phase"
                        .to_owned(),
                ));
            }
        };
        if let Some(expected) = expected_outbox {
            let outbox = self
                .outbox_after
                .as_ref()
                .ok_or_else(|| DurableError::Integrity {
                    code: "agent_workspace_result_outbox_missing".to_owned(),
                    message: "Agent workspace Effect phase has no result outbox".to_owned(),
                })?;
            if outbox.state != expected
                || (expected == OutboxState::Applied && outbox.result.is_none())
                || (self.phase != Phase::StartEffectDispatch
                    && (self.effect_before.is_none() || self.lease_before != self.lease_after))
                || (self.phase == Phase::SettleEffectUnknown
                    && self
                        .outbox_before
                        .as_ref()
                        .is_some_and(|before| before.state == OutboxState::Unknown)
                    && !self.m1_values_unchanged())
            {
                return Err(DurableError::Integrity {
                    code: "agent_workspace_result_phase_mismatch".to_owned(),
                    message:
                        "Agent workspace phase does not match its retained Effect outcome and lease"
                            .to_owned(),
                });
            }
        } else if self.effect_before.is_some()
            || self.effect_after.is_some()
            || self.outbox_before.is_some()
            || self.outbox_after.is_some()
            || self.lease_before.is_some()
            || self.lease_after.is_some()
            || (self.phase == Phase::StartAbortDispatch && self.core_batch_id.is_some())
            || (matches!(
                self.phase,
                Phase::SettleAbortNotApplied | Phase::SettleAbortUnknown
            ) && !self.m1_values_unchanged())
        {
            return Err(DurableError::Integrity {
                code: "agent_workspace_abort_neighborhood_mismatch".to_owned(),
                message: "Agent workspace abort cannot carry an Effect neighborhood or an unrelated Core batch".to_owned(),
            });
        }
        Ok(())
    }
}

pub(crate) fn verify_workspace_receipt_link(
    command: &AgentCommand,
    workspace: &AgentWorkspaceCommand,
    agent: &WorkspaceScopeCheckpoint,
    coupled: &CoupledCheckpointReceipt,
    checkpoint: &AgentWorkspaceCheckpoint,
) -> DurableResult<()> {
    command.verify()?;
    workspace.verify()?;
    agent.verify_for(workspace)?;
    coupled.verify()?;
    checkpoint.verify()?;
    if !matches!(&command.action, AgentCommandAction::Workspace(retained) if retained.as_ref() == workspace)
        || !matches!(&coupled.checkpoint, CoupledCheckpoint::AgentWorkspace { checkpoint: retained }
            if retained.as_ref() == checkpoint)
    {
        return Err(workspace_integrity(
            "agent_workspace_m1_action_mismatch",
            "workspace link changed its outer action or exact coupled checkpoint",
        ));
    }
    let witness = &agent.m1;
    if checkpoint.agent_command_id != command.command_id
        || checkpoint.run_id != workspace.request().run_id
        || checkpoint.scope_id != workspace.request().scope_id
        || checkpoint.occurrence_id != workspace.request().occurrence_id
        || checkpoint.phase != workspace.phase_for(&agent.occurrence.current.occurrence)?
        || witness.m1_receipt_id != coupled.receipt_id
        || witness.continuation_digest != checkpoint.continuation_digest
        || witness.phase != checkpoint.phase
        || witness.effect_intent_id.as_deref()
            != checkpoint
                .effect_after
                .as_ref()
                .map(|effect| effect.intent_id.as_str())
    {
        return Err(workspace_integrity(
            "agent_workspace_m1_origin_mismatch",
            "Agent workspace witness does not equal its real typed M1 receipt",
        ));
    }
    if let AgentWorkspaceCommand::StartEffect {
        request,
        execution_binding,
        operation_occurrence_binding,
        effect_intent_id,
    } = workspace
    {
        let clock = checkpoint.dispatch_clock.as_ref().ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_dispatch_clock_missing",
                "workspace Start lost its issued Clock observation",
            )
        })?;
        let requested = request.dispatch_lease.as_ref().ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_dispatch_request_missing",
                "workspace Start lost its dispatch lease request",
            )
        })?;
        let lease = checkpoint.lease_after.as_ref().ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_dispatch_lease_missing",
                "workspace Start lost its dispatch lease",
            )
        })?;
        let effect = checkpoint.effect_after.as_ref().ok_or_else(|| {
            workspace_integrity(
                "agent_workspace_dispatch_effect_missing",
                "workspace Start lost its exact Effect",
            )
        })?;
        if clock.reference() != requested.clock
            || lease.owner != requested.owner
            || clock.logical_time.checked_add(requested.ttl) != Some(lease.expires_at)
            || effect.execution_binding != *execution_binding
            || effect.occurrence_binding != *operation_occurrence_binding
            || effect.intent_id != *effect_intent_id
        {
            return Err(workspace_integrity(
                "agent_workspace_dispatch_authority_mismatch",
                "workspace Start changed Clock, TTL, owner, or exact Effect binding",
            ));
        }
    }
    Ok(())
}

pub(crate) fn verify_workspace_batch_link(
    checkpoint: &AgentWorkspaceCheckpoint,
    batch: &cymule_core::MachineCommandBatchRecord,
) -> DurableResult<()> {
    batch.verify()?;
    if checkpoint.core_batch_id.as_deref() != Some(batch.batch_id.as_str())
        || checkpoint.core_batch_receipt_id.as_deref() != Some(batch.batch_receipt_id.as_str())
        || checkpoint.source_machine_authority_root != batch.admission_parent_authority_root
        || checkpoint.machine_authority_root != batch.result_authority_root
    {
        return Err(workspace_integrity(
            "agent_workspace_batch_origin_mismatch",
            "workspace M1 receipt points at another Core batch or authority transition",
        ));
    }
    Ok(())
}

/// Verify the complete ordered Core command matrix and the allowed material
/// references. The Store owner additionally resolves exact Artifact bytes and
/// proves which references were absent at the recorded source revision.
pub(crate) fn validate_workspace_checkpoint_batch(
    command: &AgentCommand,
    workspace: &AgentWorkspaceCommand,
    agent: &WorkspaceScopeCheckpoint,
    checkpoint: &AgentWorkspaceCheckpoint,
    batch: &cymule_core::MachineCommandBatchRecord,
    entries: &[cymule_core::MachineCommandArchiveEntry],
) -> DurableResult<()> {
    command.verify()?;
    agent.verify_for(workspace)?;
    checkpoint.verify()?;
    verify_workspace_batch_link(checkpoint, batch)?;
    if checkpoint.agent_command_id != command.command_id
        || !matches!(&command.action, AgentCommandAction::Workspace(retained) if retained.as_ref() == workspace)
        || checkpoint.phase != workspace.phase_for(&agent.occurrence.current.occurrence)?
        || checkpoint.run_id != workspace.request().run_id
        || checkpoint.scope_id != workspace.request().scope_id
        || checkpoint.occurrence_id != workspace.request().occurrence_id
        || !batch.plan_ids.is_empty()
    {
        return Err(workspace_integrity(
            "agent_workspace_batch_action_mismatch",
            "workspace batch changed its outer Agent action or admitted a Plan",
        ));
    }
    let expected = workspace_checkpoint_commands(workspace, checkpoint)?;
    if entries.len() != expected.len()
        || batch.members.len() != expected.len()
        || batch.receipts.len() != expected.len()
    {
        return Err(workspace_integrity(
            "agent_workspace_core_batch_mismatch",
            "workspace batch differs from its complete ordered Core command set",
        ));
    }
    for (position, ((phase, expected_command), entry)) in expected.iter().zip(entries).enumerate() {
        batch.verify_entry(entry)?;
        let expected_id = agent_protocol::agent_workspace_command_id(workspace.request(), *phase)?;
        if entry.command.envelope.command_id != expected_id
            || entry.command.envelope.run_id != workspace.request().run_id
            || entry.command.envelope.actor != DURABLE_RUNTIME_ACTOR
            || entry.command.envelope.command != *expected_command
            || usize::try_from(entry.command.batch_position).ok() != Some(position)
            || entry.command.receipt.status != cymule_core::CommandReceiptStatus::Applied
        {
            return Err(workspace_integrity(
                "agent_workspace_core_batch_mismatch",
                "workspace batch changed a phase, member ID, actor, command, or Applied receipt",
            ));
        }
    }
    if checkpoint.phase == AgentWorkspaceCommandPhase::SettleEffectApplied
        && !checkpoint
            .outbox_after
            .as_ref()
            .and_then(|outbox| outbox.result.as_ref())
            .is_some_and(|result| batch.artifacts.contains(result))
    {
        return Err(workspace_integrity(
            "agent_workspace_effect_result_material_missing",
            "workspace Applied result is not bound by its exact Core batch material",
        ));
    }
    let references = batch
        .material_source
        .as_ref()
        .map_or(&[][..], |source| source.artifacts.as_slice());
    validate_workspace_material_references(workspace, agent, checkpoint, references)?;
    let Some(source) = &batch.material_source else {
        return if expected.is_empty() {
            Err(workspace_integrity(
                "agent_workspace_empty_material_batch",
                "workspace material-only batch has no actual material source",
            ))
        } else {
            Ok(())
        };
    };
    if source.source_command_id != command.command_id
        || !source.plan_ids.is_empty()
        || source.artifacts.is_empty()
        || (expected.is_empty()
            && (!batch.event_ids.is_empty() || !checkpoint.m1_values_unchanged()))
    {
        return Err(workspace_integrity(
            "agent_workspace_material_source_mismatch",
            "workspace material source changed its outer command, Artifact-only input, or unchanged M1 values",
        ));
    }
    Ok(())
}

fn validate_workspace_material_references(
    workspace: &AgentWorkspaceCommand,
    agent: &WorkspaceScopeCheckpoint,
    checkpoint: &AgentWorkspaceCheckpoint,
    artifacts: &[ArtifactRef],
) -> DurableResult<()> {
    let occurrence = &agent.occurrence.current.occurrence;
    let mut allowed = BTreeSet::new();
    let observes = matches!(
        workspace,
        AgentWorkspaceCommand::SettleEffect { .. } | AgentWorkspaceCommand::SettleAbort { .. }
    );
    if let Some(receipt) = &agent.receipt {
        allowed.insert(receipt.evidence.clone());
        if matches!(workspace, AgentWorkspaceCommand::SettleEffect { .. }) {
            let result = cymule_core::artifact_ref(
                EFFECT_RESULT_ARTIFACT_KIND,
                &cymule_core::canonical_bytes(receipt)?,
            )?;
            if checkpoint
                .outbox_after
                .as_ref()
                .and_then(|outbox| outbox.result.as_ref())
                != Some(&result)
            {
                return Err(workspace_integrity(
                    "agent_workspace_effect_result_mismatch",
                    "Applied workspace did not retain its canonical provider receipt result",
                ));
            }
            allowed.insert(result);
        }
    } else if observes && let Some(observation) = occurrence.recovery_observations.last() {
        for block in &observation.evidence {
            if let agent_protocol::ContentBlock::Artifact { artifact } = block {
                allowed.insert(artifact.clone());
            }
        }
    }
    let scope_results = artifacts
        .iter()
        .filter(|artifact| artifact.kind == SCOPE_RESULT_ARTIFACT_KIND)
        .count();
    let is_start = matches!(workspace, AgentWorkspaceCommand::StartEffect { .. });
    if scope_results > usize::from(is_start)
        || artifacts.iter().any(|artifact| {
            !(allowed.contains(artifact) || is_start && artifact.kind == SCOPE_RESULT_ARTIFACT_KIND)
        })
    {
        return Err(workspace_integrity(
            "agent_workspace_material_reference_mismatch",
            "workspace batch contains material outside its exact typed observation and derived result",
        ));
    }
    Ok(())
}

pub(crate) fn workspace_checkpoint_commands(
    workspace: &AgentWorkspaceCommand,
    checkpoint: &AgentWorkspaceCheckpoint,
) -> DurableResult<Vec<(AgentWorkspaceCommandPhase, Command)>> {
    use AgentWorkspaceCommandPhase as Phase;
    let request = workspace.request();
    match checkpoint.phase {
        Phase::StartEffectDispatch => workspace_start_effect_commands(workspace, checkpoint),
        Phase::SettleAbortApplied => Ok(vec![(
            Phase::SettleAbortApplied,
            Command::AbortScope {
                scope_id: request.scope_id.clone(),
            },
        )]),
        Phase::SettleEffectApplied | Phase::SettleEffectNotApplied | Phase::SettleEffectUnknown => {
            let before = checkpoint.outbox_before.as_ref().ok_or_else(|| {
                workspace_integrity(
                    "agent_workspace_source_outbox_missing",
                    "workspace settlement receipt has no source outbox",
                )
            })?;
            if checkpoint.phase == Phase::SettleEffectUnknown
                && before.state == OutboxState::Unknown
            {
                return Ok(Vec::new());
            }
            let (world, reconciled) = match checkpoint.phase {
                Phase::SettleEffectApplied => (
                    WorldOutcome::Applied,
                    ReconciliationResolution::ResolvedApplied,
                ),
                Phase::SettleEffectNotApplied => (
                    WorldOutcome::NotApplied,
                    ReconciliationResolution::ResolvedNotApplied,
                ),
                Phase::SettleEffectUnknown => (
                    WorldOutcome::Unknown,
                    ReconciliationResolution::StillUnknown,
                ),
                _ => unreachable!(),
            };
            let transition = match before.state {
                OutboxState::Claimed => EffectTransition::Observe(world),
                OutboxState::Unknown => EffectTransition::Reconcile(reconciled),
                _ => {
                    return Err(workspace_integrity(
                        "agent_workspace_source_outbox_terminal",
                        "workspace settlement receipt has a terminal source outbox",
                    ));
                }
            };
            Ok(vec![(
                checkpoint.phase,
                Command::TransitionEffect {
                    intent_id: before.intent_id.clone(),
                    transition,
                },
            )])
        }
        Phase::StartAbortDispatch | Phase::SettleAbortNotApplied | Phase::SettleAbortUnknown => {
            Ok(Vec::new())
        }
        Phase::ProposeEffect
        | Phase::PrepareEffect
        | Phase::CommitScope
        | Phase::AuthorizeEffect => Err(workspace_integrity(
            "agent_workspace_intermediate_checkpoint_phase",
            "workspace receipt retained an intermediate Core phase",
        )),
    }
}

fn workspace_start_effect_commands(
    workspace: &AgentWorkspaceCommand,
    checkpoint: &AgentWorkspaceCheckpoint,
) -> DurableResult<Vec<(AgentWorkspaceCommandPhase, Command)>> {
    use AgentWorkspaceCommandPhase as Phase;
    let request = workspace.request();
    let effect = checkpoint.effect_after.as_ref().ok_or_else(|| {
        workspace_integrity(
            "agent_workspace_effect_missing",
            "workspace Start receipt has no Effect",
        )
    })?;
    Ok(vec![
        (
            Phase::ProposeEffect,
            Command::ProposeEffect {
                scope_id: effect.scope_id.clone(),
                invocation_id: effect.invocation_id.clone(),
                invocation_path: effect.invocation_path.clone(),
                definition_id: effect.definition_id.clone(),
                region_path: effect.region_path.clone(),
                site_id: effect.site_id.clone(),
                occurrence: effect.occurrence.clone(),
                operation: effect.operation.clone(),
                args: effect.args.clone(),
                execution_binding: effect.execution_binding.clone(),
                occurrence_binding: effect.occurrence_binding.clone(),
            },
        ),
        (
            Phase::PrepareEffect,
            Command::TransitionEffect {
                intent_id: effect.intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        ),
        (
            Phase::CommitScope,
            Command::CommitScope {
                scope_id: request.scope_id.clone(),
            },
        ),
        (
            Phase::AuthorizeEffect,
            Command::TransitionEffect {
                intent_id: effect.intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        ),
        (
            Phase::StartEffectDispatch,
            Command::TransitionEffect {
                intent_id: effect.intent_id.clone(),
                transition: EffectTransition::StartDispatch,
            },
        ),
    ])
}

fn workspace_integrity(code: &str, message: impl Into<String>) -> DurableError {
    DurableError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn verify_workspace_effect_path(effect: &cymule_core::EffectProjection) -> DurableResult<()> {
    for (label, value) in [
        ("workspace Effect definition", &effect.definition_id),
        ("workspace Effect site", &effect.site_id),
        ("workspace Effect occurrence", &effect.occurrence),
        ("workspace Effect operation", &effect.operation),
    ] {
        validate_wire_non_empty(label, value)?;
    }
    if effect.invocation_path.len() > cymule_durable_protocol::MAX_FRAME_INVOCATION_DEPTH
        || effect.region_path.len() > cymule_durable_protocol::MAX_REGION_PATH_DEPTH
    {
        return Err(DurableError::Validation(
            "workspace Effect path exceeds one bounded Continuation frame".to_owned(),
        ));
    }
    validate_wire_indices("workspace Effect Region", &effect.region_path)?;
    let mut items = effect.invocation_path.len() + effect.region_path.len();
    let mut scalars = effect.definition_id.chars().count()
        + effect.invocation_id.chars().count()
        + effect.scope_id.chars().count();
    for segment in &effect.invocation_path {
        validate_wire_non_empty("workspace invocation site", &segment.site_id)?;
        validate_wire_non_empty("workspace invocation scope", &segment.scope_id)?;
        if segment.region_path.len() > cymule_durable_protocol::MAX_REGION_PATH_DEPTH {
            return Err(DurableError::Validation(
                "workspace invocation Region is too deep".to_owned(),
            ));
        }
        validate_wire_indices("workspace invocation Region", &segment.region_path)?;
        items += segment.region_path.len();
        scalars += segment.site_id.chars().count() + segment.scope_id.chars().count();
    }
    if items > cymule_durable_protocol::MAX_CONTINUATION_AGGREGATE_ITEMS
        || scalars > cymule_durable_protocol::MAX_CONTINUATION_IDENTITY_SCALARS
    {
        return Err(DurableError::Validation(
            "workspace Effect path cannot be a subset of one verified Continuation".to_owned(),
        ));
    }
    Ok(())
}

/// Closed complete semantics of one typed higher-profile M1 boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoupledCheckpoint {
    /// Exact Agent workspace coupling without a generic journal or receipt cycle.
    AgentWorkspace {
        /// Bounded source/result authority for the outer Agent command.
        checkpoint: Box<AgentWorkspaceCheckpoint>,
    },
    /// Register one journal-owned pending wait.
    JournalWait {
        /// Complete wait semantics.
        wait: WaitCondition,
        /// Exact owning journal batch.
        journal: JournalBatchManifest,
    },
    /// Enqueue one Effect with its Continuation and journal checkpoint.
    JournalEffectEnqueue {
        /// Incrementally authenticated result Machine authority root.
        machine_authority_root: String,
        /// Exact post-transition Continuation.
        continuation: Box<Continuation>,
        /// Exact pending dispatch.
        dispatch: Box<EffectDispatch>,
        /// Exact owning journal batch.
        journal: JournalBatchManifest,
    },
    /// Settle one claimed Effect with its Continuation and journal checkpoint.
    JournalEffectSettlement {
        /// Incrementally authenticated result Machine authority root.
        machine_authority_root: String,
        /// Exact post-transition Continuation.
        continuation: Box<Continuation>,
        /// Structural Effect intent.
        intent_id: String,
        /// Original claim owner.
        owner: String,
        /// Original claim epoch.
        lease_epoch: u64,
        /// Closed outbox outcome.
        outcome: OutboxState,
        /// Optional canonical Effect result.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        result: Option<ArtifactRef>,
        /// Exact owning journal batch.
        journal: JournalBatchManifest,
    },
    /// Admit one identified wait activation with all coupled journals.
    WaitActivationJournals {
        /// Incrementally authenticated result Machine authority root.
        machine_authority_root: String,
        /// Exact activation selection and winner receipt.
        activation: Box<WaitActivationReceipt>,
        /// Exact non-empty higher-profile journal batches.
        journals: Vec<JournalBatchManifest>,
    },
    /// Publish an input Artifact, complete its wait, and append journals.
    InputWaitJournals {
        /// Incrementally authenticated result Machine authority root.
        machine_authority_root: String,
        /// Exact input-suspension receipt consumed by this terminal completion.
        suspension_receipt_id: String,
        /// Exact input result Artifact.
        result: ArtifactRef,
        /// Stable wait identity.
        wait_id: String,
        /// Exact non-empty higher-profile journal batches.
        journals: Vec<JournalBatchManifest>,
    },
    /// Complete one resource handoff into its exact target input Wait. The
    /// source transfer receipt owns the handoff authority and target index;
    /// this checkpoint adds only the activation authority and reference index.
    ResourceHandoffInput {
        /// Incrementally authenticated result Machine authority root.
        machine_authority_root: String,
        /// Stable source transfer identity.
        transfer_id: String,
        /// Content identity of the exact activation semantics.
        activation_id: String,
        /// Exact closed Resource command that owns this activation.
        resource_command_id: String,
        /// Content identity of the retained typed Resource transfer receipt.
        source_receipt_id: String,
        /// Exact target Run.
        run_id: String,
        /// Exact structural owner of the target input Wait.
        owner: WaitOwner,
        /// Exact target input Wait.
        wait_id: String,
        /// Canonical Resource Handle Artifact delivered to the Wait.
        result: ArtifactRef,
        /// Digest of the exact resulting target Continuation.
        continuation_digest: String,
    },
    /// Atomically append one complete provider-neutral multi-journal
    /// transition, including caller-owned source/result projection revisions.
    JournalSet {
        /// Stable higher-profile transition/session/stream identity.
        coupling_key: String,
        /// Exact M1 source revision consumed by the CAS.
        source_revision: String,
        /// Canonical higher-profile result projection content identity.
        result_revision: String,
        /// Complete non-empty unique journal batches.
        manifest: Vec<JournalBatchManifest>,
    },
}

impl CoupledCheckpoint {
    fn verify(&self) -> DurableResult<()> {
        let journals = match self {
            Self::AgentWorkspace { checkpoint } => {
                checkpoint.verify()?;
                Vec::new()
            }
            Self::JournalWait { wait, journal } => {
                wait.verify_wire()?;
                vec![journal]
            }
            Self::JournalEffectEnqueue {
                machine_authority_root,
                continuation,
                dispatch,
                journal,
            } => {
                validate_lower_hex_digest(
                    "coupled Machine authority root",
                    machine_authority_root,
                )?;
                continuation.verify_wire()?;
                dispatch.verify_wire()?;
                vec![journal]
            }
            Self::JournalEffectSettlement { journal, .. } => {
                self.verify_effect_settlement()?;
                vec![journal]
            }
            Self::WaitActivationJournals {
                machine_authority_root,
                activation,
                journals,
            } => {
                validate_lower_hex_digest(
                    "coupled Machine authority root",
                    machine_authority_root,
                )?;
                activation.verify()?;
                if journals.is_empty() {
                    return Err(DurableError::Validation(
                        "coupled wait activation requires a journal".to_owned(),
                    ));
                }
                journals.iter().collect()
            }
            Self::InputWaitJournals {
                machine_authority_root,
                suspension_receipt_id,
                result,
                wait_id,
                journals,
            } => {
                validate_lower_hex_digest(
                    "coupled Machine authority root",
                    machine_authority_root,
                )?;
                validate_sha256_identity(
                    "coupled input suspension receipt",
                    suspension_receipt_id,
                )?;
                result
                    .validate()
                    .map_err(|error| DurableError::Validation(error.to_string()))?;
                validate_sha256_identity("coupled input wait", wait_id)?;
                if journals.len() != 1 {
                    return Err(DurableError::Validation(
                        "coupled input wait requires exactly one owning journal".to_owned(),
                    ));
                }
                journals.iter().collect()
            }
            Self::ResourceHandoffInput { .. } => {
                self.verify_resource_handoff_input()?;
                Vec::new()
            }
            Self::JournalSet {
                coupling_key,
                source_revision,
                result_revision,
                manifest,
            } => {
                validate_sha256_identity("coupled journal-set key", coupling_key)?;
                cymule_core::validate_content_id(
                    "coupled journal-set source revision",
                    source_revision,
                )?;
                validate_sha256_identity("coupled journal-set result revision", result_revision)?;
                if manifest.is_empty() {
                    return Err(DurableError::Validation(
                        "coupled journal set requires at least one journal".to_owned(),
                    ));
                }
                manifest.iter().collect()
            }
        };
        Self::verify_journal_manifests(&journals)
    }

    fn verify_journal_manifests(journals: &[&JournalBatchManifest]) -> DurableResult<()> {
        let mut journal_ids = BTreeSet::new();
        for journal in journals {
            journal.verify()?;
            if !journal_ids.insert(&journal.journal_id) {
                return Err(DurableError::Validation(format!(
                    "coupled checkpoint repeats journal {}",
                    journal.journal_id
                )));
            }
        }
        Ok(())
    }

    fn verify_effect_settlement(&self) -> DurableResult<()> {
        let Self::JournalEffectSettlement {
            machine_authority_root,
            continuation,
            intent_id,
            owner,
            lease_epoch,
            outcome,
            result,
            ..
        } = self
        else {
            return Err(DurableError::Validation(
                "coupled checkpoint verifier received another authority kind".to_owned(),
            ));
        };
        validate_lower_hex_digest("coupled Machine authority root", machine_authority_root)?;
        continuation.verify_wire()?;
        validate_sha256_identity("coupled Effect intent", intent_id)?;
        validate_wire_non_empty("coupled Effect owner", owner)?;
        if *lease_epoch == 0 || *lease_epoch > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "coupled Effect lease epoch is invalid".to_owned(),
            ));
        }
        if !matches!(
            outcome,
            OutboxState::Applied | OutboxState::NotApplied | OutboxState::Unknown
        ) {
            return Err(DurableError::Validation(
                "coupled Effect settlement has a non-settlement outcome".to_owned(),
            ));
        }
        if let Some(result) = result {
            result
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string()))?;
        }
        Ok(())
    }

    fn verify_resource_handoff_input(&self) -> DurableResult<()> {
        let Self::ResourceHandoffInput {
            machine_authority_root,
            transfer_id,
            activation_id,
            resource_command_id,
            source_receipt_id,
            run_id,
            owner,
            wait_id,
            result,
            continuation_digest,
        } = self
        else {
            return Err(DurableError::Validation(
                "coupled checkpoint verifier received another authority kind".to_owned(),
            ));
        };
        validate_lower_hex_digest(
            "resource handoff Machine authority root",
            machine_authority_root,
        )?;
        validate_wire_non_empty("resource handoff transfer", transfer_id)?;
        validate_sha256_identity("resource handoff activation", activation_id)?;
        validate_sha256_identity("resource handoff command", resource_command_id)?;
        validate_sha256_identity("resource handoff source receipt", source_receipt_id)?;
        validate_wire_non_empty("resource handoff target Run", run_id)?;
        validate_wire_non_empty("resource handoff owner invocation", &owner.invocation_id)?;
        validate_wire_non_empty("resource handoff owner definition", &owner.definition_id)?;
        validate_wire_non_empty("resource handoff owner site", &owner.site_id)?;
        validate_wire_indices("resource handoff owner Region path", &owner.region_path)?;
        if u64::try_from(owner.step_index).map_or(true, |value| value > crate::MAX_EXACT_INTEGER) {
            return Err(DurableError::Validation(
                "resource handoff owner step exceeds the exact integer range".to_owned(),
            ));
        }
        if let Some(bind) = &owner.bind {
            validate_wire_non_empty("resource handoff owner bind", bind)?;
        }
        validate_sha256_identity("resource handoff input wait", wait_id)?;
        result
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        validate_lower_hex_digest(
            "resource handoff resulting Continuation",
            continuation_digest,
        )?;
        Ok(())
    }

    fn coupling_id(&self) -> DurableResult<String> {
        match self {
            Self::AgentWorkspace { checkpoint } => {
                agent_workspace_coupling_id(&checkpoint.agent_command_id)
            }
            Self::JournalWait { wait, .. } => journal_wait_coupling_id(&wait.wait_id),
            Self::JournalEffectEnqueue { dispatch, .. } => Ok(content_id(
                COUPLED_CHECKPOINT_KEY_DOMAIN,
                &("journal_effect_enqueue", dispatch.intent_id.as_str()),
            )?),
            Self::JournalEffectSettlement {
                intent_id,
                lease_epoch,
                outcome,
                ..
            } => Ok(content_id(
                COUPLED_CHECKPOINT_KEY_DOMAIN,
                &(
                    "journal_effect_settlement",
                    intent_id.as_str(),
                    lease_epoch,
                    outcome,
                ),
            )?),
            Self::WaitActivationJournals { activation, .. } => Ok(content_id(
                COUPLED_CHECKPOINT_KEY_DOMAIN,
                &(
                    "wait_activation_journals",
                    activation.activation.activation_id.as_str(),
                ),
            )?),
            Self::InputWaitJournals { wait_id, .. } => input_wait_coupling_id(wait_id),
            Self::ResourceHandoffInput { activation_id, .. } => {
                resource_handoff_input_coupling_id(activation_id)
            }
            Self::JournalSet { coupling_key, .. } => Ok(coupling_key.clone()),
        }
    }

    fn manifests(&self) -> Vec<&JournalBatchManifest> {
        match self {
            Self::JournalWait { journal, .. }
            | Self::JournalEffectEnqueue { journal, .. }
            | Self::JournalEffectSettlement { journal, .. } => vec![journal],
            Self::WaitActivationJournals { journals, .. }
            | Self::InputWaitJournals { journals, .. }
            | Self::JournalSet {
                manifest: journals, ..
            } => journals.iter().collect(),
            Self::ResourceHandoffInput { .. } | Self::AgentWorkspace { .. } => Vec::new(),
        }
    }
}

/// Immutable content-addressed acknowledgement for one coupled checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoupledCheckpointReceipt {
    /// Frozen receipt generation.
    pub receipt_version: String,
    /// Stable operation/subject idempotency key.
    pub coupling_id: String,
    /// Complete typed checkpoint semantics.
    pub checkpoint: CoupledCheckpoint,
    /// Content identity of the complete checkpoint semantics.
    pub receipt_id: String,
}

impl CoupledCheckpointReceipt {
    pub(crate) fn new(checkpoint: CoupledCheckpoint) -> DurableResult<Self> {
        checkpoint.verify()?;
        let coupling_id = checkpoint.coupling_id()?;
        let receipt_id = content_id(COUPLED_CHECKPOINT_RECEIPT_VERSION, &checkpoint)?;
        let receipt = Self {
            receipt_version: COUPLED_CHECKPOINT_RECEIPT_VERSION.to_owned(),
            coupling_id,
            checkpoint,
            receipt_id,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify the complete typed receipt and both content identities.
    ///
    /// # Errors
    /// Returns an error for an unsupported or oversized receipt, invalid checkpoint,
    /// or mismatched coupling and receipt content identities.
    pub fn verify(&self) -> DurableResult<()> {
        if self.receipt_version != COUPLED_CHECKPOINT_RECEIPT_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported coupled checkpoint receipt version {}",
                self.receipt_version
            )));
        }
        self.checkpoint.verify()?;
        if self.coupling_id != self.checkpoint.coupling_id()?
            || self.receipt_id != content_id(COUPLED_CHECKPOINT_RECEIPT_VERSION, &self.checkpoint)?
        {
            return Err(DurableError::Integrity {
                code: "coupled_checkpoint_receipt_identity_mismatch".to_owned(),
                message: format!(
                    "coupled checkpoint receipt {} does not match its complete semantics",
                    self.receipt_id
                ),
            });
        }
        let byte_limit = if matches!(self.checkpoint, CoupledCheckpoint::AgentWorkspace { .. }) {
            MAX_AGENT_WORKSPACE_CHECKPOINT_RECEIPT_BYTES
        } else {
            MAX_COUPLED_CHECKPOINT_RECEIPT_BYTES
        };
        if cymule_core::canonical_bytes(self)?.len() > byte_limit {
            return Err(DurableError::Validation(format!(
                "coupled checkpoint receipt exceeds {byte_limit} canonical bytes"
            )));
        }
        Ok(())
    }

    pub(crate) fn manifests(&self) -> Vec<&JournalBatchManifest> {
        self.checkpoint.manifests()
    }
}

/// Bounded identity projection of one application-journal record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationJournalRecordRef {
    /// Stable record identity within its journal.
    pub record_id: String,
    /// Canonical digest of the record schema and payload.
    pub content_digest: String,
}

impl ApplicationJournalRecordRef {
    fn from_record(record: &JournalRecord) -> Self {
        Self {
            record_id: record.record_id.clone(),
            content_digest: record.content_digest.clone(),
        }
    }

    /// Validate one bounded record reference.
    ///
    /// # Errors
    /// Returns an error for an empty record identity or malformed content digest.
    pub fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty("application journal record", &self.record_id)?;
        validate_lower_hex_digest(
            "application journal record content digest",
            &self.content_digest,
        )
    }
}

/// Constant-size authentication of one exact current journal prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationJournalPrefix {
    /// Prefix descriptor schema.
    pub prefix_version: String,
    /// Exact number of records covered from index zero.
    pub record_count: u64,
    /// First covered record identity and content digest.
    pub first: ApplicationJournalRecordRef,
    /// Last covered record identity and content digest.
    pub last: ApplicationJournalRecordRef,
    /// Exact current-manifest, history-authenticated AVL-rope root supplied by
    /// `StateRoot` evidence.
    pub ordered_root: String,
}

impl ApplicationJournalPrefix {
    /// Construct one constant-size prefix descriptor from evidence already
    /// authenticated by the canonical persistent-log implementation.
    pub(crate) fn from_state_log_evidence(
        record_count: u64,
        first: &JournalRecord,
        last: &JournalRecord,
        ordered_root: String,
    ) -> DurableResult<Self> {
        first.verify()?;
        last.verify()?;
        let prefix = Self {
            prefix_version: APPLICATION_JOURNAL_PREFIX_VERSION.to_owned(),
            record_count,
            first: ApplicationJournalRecordRef::from_record(first),
            last: ApplicationJournalRecordRef::from_record(last),
            ordered_root,
        };
        prefix.verify()?;
        Ok(prefix)
    }

    /// Validate the bounded descriptor independently of current journal state.
    ///
    /// # Errors
    /// Returns an error for invalid version, count, root, or endpoint identities.
    pub fn verify(&self) -> DurableResult<()> {
        if self.prefix_version != APPLICATION_JOURNAL_PREFIX_VERSION
            || self.record_count == 0
            || self.record_count > crate::MAX_EXACT_INTEGER
        {
            return Err(DurableError::Validation(
                "application journal prefix record count is invalid".to_owned(),
            ));
        }
        self.first.verify()?;
        self.last.verify()?;
        if (self.record_count == 1) != (self.first == self.last) {
            return Err(DurableError::Validation(
                "application journal prefix endpoints do not match its record count".to_owned(),
            ));
        }
        cymule_core::validate_content_id(
            "application journal prefix ordered root",
            &self.ordered_root,
        )
        .map_err(Into::into)
    }
}

/// Complete normalized command for replacing one exact authenticated prefix
/// with a bounded typed base.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationJournalPrefixReplacement {
    /// Stable command and idempotency identity.
    pub replacement_id: String,
    /// Journal whose index-zero prefix is replaced.
    pub journal_id: String,
    /// Latest replacement receipt for this journal, if one exists.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub parent_replacement_id: Option<String>,
    /// Exact current prefix expected before replacement.
    pub expected_prefix: ApplicationJournalPrefix,
    /// Bounded typed base records inserted before the retained suffix.
    pub replacement: Vec<JournalRecord>,
}

impl ApplicationJournalPrefixReplacement {
    /// Validate the normalized command independently of current journal state.
    ///
    /// # Errors
    /// Returns an error if the command, expected prefix, or bounded replacement
    /// records are invalid or contain conflicting record identities.
    pub fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty(
            "application journal prefix replacement",
            &self.replacement_id,
        )?;
        validate_wire_non_empty("application journal identity", &self.journal_id)?;
        if let Some(parent) = &self.parent_replacement_id {
            validate_wire_non_empty("parent journal prefix replacement", parent)?;
            if parent == &self.replacement_id {
                return Err(DurableError::Validation(
                    "application journal prefix replacement cannot parent itself".to_owned(),
                ));
            }
        }
        self.expected_prefix.verify()?;
        if self.replacement.is_empty()
            || self.replacement.len() > MAX_APPLICATION_JOURNAL_REPLACEMENT_RECORDS
            || self.expected_prefix.record_count
                < u64::try_from(self.replacement.len())
                    .map_err(|error| DurableError::Validation(error.to_string()))?
        {
            return Err(DurableError::Validation(format!(
                "application journal replacement must not expand its prefix and must contain 1..={MAX_APPLICATION_JOURNAL_REPLACEMENT_RECORDS} records"
            )));
        }
        let mut ids = BTreeSet::new();
        for record in &self.replacement {
            record.verify()?;
            if !ids.insert(&record.record_id) {
                return Err(DurableError::Validation(format!(
                    "application journal replacement repeats record {}",
                    record.record_id
                )));
            }
        }
        Ok(())
    }
}

/// Latest immutable M1 receipt for one application-journal prefix replacement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationJournalPrefixReplacementReceipt {
    /// Frozen receipt generation.
    pub receipt_version: String,
    /// Complete normalized prefix-replacement command.
    pub replacement: ApplicationJournalPrefixReplacement,
    /// Exact complete journal descriptor after prefix replacement.
    pub result: ApplicationJournalPrefix,
    /// Stable content identity of every preceding receipt field.
    pub receipt_id: String,
}

/// Payload-free cumulative authority for an old prefix-replacement identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationJournalPrefixReplacementAuthority {
    /// Frozen cumulative-authority generation.
    pub authority_version: String,
    /// Stable replacement command identity.
    pub replacement_id: String,
    /// Owning application journal.
    pub journal_id: String,
    /// Content identity of the immutable receipt.
    pub receipt_id: String,
    /// Canonical digest of the complete replacement command.
    pub replacement_digest: String,
    /// Exact complete journal descriptor after the replacement.
    pub result: ApplicationJournalPrefix,
}

impl ApplicationJournalPrefixReplacementAuthority {
    pub(crate) fn new(receipt: &ApplicationJournalPrefixReplacementReceipt) -> DurableResult<Self> {
        receipt.verify()?;
        let authority = Self {
            authority_version: APPLICATION_JOURNAL_PREFIX_REPLACEMENT_AUTHORITY_VERSION.to_owned(),
            replacement_id: receipt.replacement.replacement_id.clone(),
            journal_id: receipt.replacement.journal_id.clone(),
            receipt_id: receipt.receipt_id.clone(),
            replacement_digest: canonical_digest(&receipt.replacement)?,
            result: receipt.result.clone(),
        };
        authority.verify()?;
        Ok(authority)
    }

    /// Verify the payload-free cumulative identity authority.
    ///
    /// # Errors
    /// Returns an error for unsupported authority metadata, malformed identities,
    /// or a receipt identity inconsistent with the retained result descriptor.
    pub fn verify(&self) -> DurableResult<()> {
        if self.authority_version != APPLICATION_JOURNAL_PREFIX_REPLACEMENT_AUTHORITY_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported application journal prefix replacement authority version {}",
                self.authority_version
            )));
        }
        validate_wire_non_empty("journal prefix replacement", &self.replacement_id)?;
        validate_wire_non_empty("application journal", &self.journal_id)?;
        cymule_core::validate_content_id("journal prefix replacement receipt", &self.receipt_id)?;
        validate_lower_hex_digest(
            "journal prefix replacement command digest",
            &self.replacement_digest,
        )?;
        self.result.verify()?;
        let expected_receipt_id = application_journal_prefix_replacement_receipt_id(
            &self.replacement_digest,
            &self.result,
        )?;
        if self.receipt_id != expected_receipt_id {
            return Err(DurableError::Integrity {
                code: "application_journal_replacement_authority_receipt_mismatch".to_owned(),
                message: format!(
                    "application journal prefix replacement authority {} does not bind its receipt",
                    self.replacement_id
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn matches(
        &self,
        receipt: &ApplicationJournalPrefixReplacementReceipt,
    ) -> DurableResult<bool> {
        self.verify()?;
        Ok(self == &Self::new(receipt)?)
    }
}

impl ApplicationJournalPrefixReplacementReceipt {
    /// Construct and authenticate one replacement receipt before its CAS.
    ///
    /// # Errors
    /// Returns an error if the replacement command or result descriptor is invalid
    /// or their derived receipt cannot be authenticated.
    pub fn new(
        replacement: ApplicationJournalPrefixReplacement,
        result: ApplicationJournalPrefix,
    ) -> DurableResult<Self> {
        replacement.verify()?;
        result.verify()?;
        let mut receipt = Self {
            receipt_version: APPLICATION_JOURNAL_PREFIX_REPLACEMENT_RECEIPT_VERSION.to_owned(),
            replacement,
            result,
            receipt_id: String::new(),
        };
        receipt.receipt_id = receipt.content_id()?;
        receipt.verify()?;
        Ok(receipt)
    }

    /// Validate the normalized command and immutable receipt identity.
    ///
    /// # Errors
    /// Returns an error if the command, result endpoints, or derived receipt
    /// identity are invalid or inconsistent.
    pub fn verify(&self) -> DurableResult<()> {
        if self.receipt_version != APPLICATION_JOURNAL_PREFIX_REPLACEMENT_RECEIPT_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported application journal prefix replacement receipt version {}",
                self.receipt_version
            )));
        }
        self.replacement.verify()?;
        self.result.verify()?;
        let replacement_count = u64::try_from(self.replacement.replacement.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let (first, last) = self
            .replacement
            .replacement
            .first()
            .zip(self.replacement.replacement.last())
            .ok_or_else(|| {
                DurableError::Validation(
                    "application journal replacement requires non-empty records".to_owned(),
                )
            })?;
        let replacement_first = ApplicationJournalRecordRef::from_record(first);
        let replacement_last = ApplicationJournalRecordRef::from_record(last);
        if self.result.record_count < replacement_count
            || self.result.first != replacement_first
            || (self.result.record_count == replacement_count
                && self.result.last != replacement_last)
        {
            return Err(DurableError::Validation(
                "application journal prefix replacement result descriptor is inconsistent"
                    .to_owned(),
            ));
        }
        cymule_core::validate_content_id(
            "application journal prefix replacement receipt",
            &self.receipt_id,
        )?;
        if self.receipt_id != self.content_id()? {
            return Err(DurableError::Integrity {
                code: "application_journal_prefix_receipt_identity_mismatch".to_owned(),
                message: format!(
                    "application journal prefix replacement receipt {} is invalid",
                    self.replacement.replacement_id
                ),
            });
        }
        Ok(())
    }

    fn content_id(&self) -> DurableResult<String> {
        let replacement_digest = canonical_digest(&self.replacement)?;
        application_journal_prefix_replacement_receipt_id(&replacement_digest, &self.result)
    }
}

fn application_journal_prefix_replacement_receipt_id(
    replacement_digest: &str,
    result: &ApplicationJournalPrefix,
) -> DurableResult<String> {
    validate_lower_hex_digest(
        "application journal prefix replacement command digest",
        replacement_digest,
    )?;
    result.verify()?;
    content_id(
        APPLICATION_JOURNAL_PREFIX_REPLACEMENT_RECEIPT_VERSION,
        &(replacement_digest, result),
    )
    .map_err(Into::into)
}

impl JournalRecord {
    /// Construct a record with a verified content digest.
    ///
    /// # Errors
    /// Returns an error for an invalid identity, schema, or payload encoding.
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
    ///
    /// # Errors
    /// Returns an error for invalid identity or schema, conflicting content digest,
    /// or a record exceeding the canonical byte bound.
    pub fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty("journal record identity", &self.record_id)?;
        validate_wire_non_empty("journal record schema", &self.schema)?;
        let expected = canonical_digest(&(self.schema.as_str(), &self.payload))?;
        if self.content_digest != expected {
            return Err(DurableError::Validation(format!(
                "journal record {} digest does not match its payload",
                self.record_id
            )));
        }
        let encoded = cymule_core::canonical_bytes(&(self.schema.as_str(), &self.payload))?;
        if encoded.len() > MAX_APPLICATION_JOURNAL_RECORD_BYTES {
            return Err(DurableError::Validation(format!(
                "application journal record {} exceeds the {MAX_APPLICATION_JOURNAL_RECORD_BYTES}-byte canonical bound",
                self.record_id
            )));
        }
        Ok(())
    }
}

fn validate_lower_hex_digest(kind: &str, value: &str) -> DurableResult<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(DurableError::Validation(format!(
            "{kind} must be a lowercase SHA-256 digest"
        )))
    }
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn deserialize_non_empty_map_if_present<'de, D, K, V>(
    deserializer: D,
) -> Result<BTreeMap<K, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    K: Ord + Deserialize<'de>,
    V: Deserialize<'de>,
{
    let values = BTreeMap::<K, V>::deserialize(deserializer)?;
    if values.is_empty() {
        return Err(serde::de::Error::custom(
            "wait_activations must be omitted when empty",
        ));
    }
    Ok(values)
}

/// State plus its store-owned revision.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredState {
    /// Canonical revision of `state`.
    pub revision: String,
    /// Bounded active runtime projection of the committed `StateRoot`.
    pub state: DurableState,
    /// Exact complete persistent-root manifest for `state`.
    pub state_root_manifest: crate::StateRootManifest,
    /// Small physical CAS head fencing the exact `StateRoot` manifest and
    /// physical generation.
    pub head: crate::StoreHead,
}

impl StoredState {
    /// Validate that the revision and active projection match the pinned root.
    ///
    /// # Errors
    /// Returns an error if the head, manifest, revision, or restored typed state
    /// cannot be authenticated against the exact pinned authority.
    pub fn verify(&self) -> DurableResult<()> {
        self.verify_and_restore().map(|_| ())
    }

    pub(crate) fn verify_and_restore(&self) -> DurableResult<Machine> {
        let restored = match self.head.machine_base_anchor.as_ref() {
            Some(anchor) => Machine::restore_anchored(self.state.machine.clone(), anchor)?,
            None => Machine::restore(self.state.machine.clone())?,
        };
        self.verify_with_machine(&restored)?;
        Ok(restored)
    }

    pub(crate) fn verify_with_machine(&self, machine: &Machine) -> DurableResult<()> {
        self.head.verify()?;
        if self.revision != self.head.revision {
            return Err(DurableError::Validation(format!(
                "stored revision {} does not match head {}",
                self.revision, self.head.revision
            )));
        }
        self.state_root_manifest.verify()?;
        if self.state_root_manifest.manifest_id != self.head.state_root_manifest_id
            || self.state_root_manifest.revision != self.revision
            || self.state_root_manifest.sequence != self.head.sequence
            || self.state_root_manifest.machine_base_anchor != self.head.machine_base_anchor
        {
            return Err(DurableError::Integrity {
                code: "stored_state_root_manifest_mismatch".to_owned(),
                message: "StoredState does not match its StoreHead state-root manifest".to_owned(),
            });
        }
        if machine.snapshot() != self.state.machine {
            return Err(DurableError::Integrity {
                code: "stored_machine_snapshot_mismatch".to_owned(),
                message: "caller-supplied restored Machine does not match StoredState".to_owned(),
            });
        }
        self.state.validate_restored_machine(machine)?;
        if machine.base_anchor()? != self.head.machine_base_anchor {
            return Err(DurableError::Integrity {
                code: "stored_machine_base_anchor_mismatch".to_owned(),
                message: "StoredState Machine does not match its StoreHead base anchor".to_owned(),
            });
        }
        Ok(())
    }
}

impl WaitCondition {
    /// Validate the complete self-contained wait wire shape.
    ///
    /// # Errors
    /// Returns an error for malformed ownership, identity, indices, or wait state
    /// and result combinations.
    pub fn verify_wire(&self) -> DurableResult<()> {
        validate_sha256_identity("wait", &self.wait_id)?;
        validate_wire_non_empty("wait Run", &self.run_id)?;
        validate_wire_non_empty("wait owner invocation", &self.owner.invocation_id)?;
        validate_wire_non_empty("wait owner definition", &self.owner.definition_id)?;
        validate_wire_non_empty("wait owner site", &self.owner.site_id)?;
        validate_wire_indices("wait owner Region path", &self.owner.region_path)?;
        if u64::try_from(self.owner.step_index)
            .map_or(true, |value| value > crate::MAX_EXACT_INTEGER)
        {
            return Err(DurableError::Validation(
                "wait owner step exceeds the exact integer range".to_owned(),
            ));
        }
        if let Some(bind) = &self.owner.bind {
            validate_wire_non_empty("wait owner bind", bind)?;
        }
        match &self.kind {
            WaitKind::Signal { key } => validate_wire_non_empty("signal key", key)?,
            WaitKind::Timer { timer_id } => validate_wire_non_empty("timer", timer_id)?,
            WaitKind::Input {
                correlation,
                schema,
            } => {
                validate_wire_non_empty("input correlation", correlation)?;
                cymule_runtime::ContractValidator::compile(
                    cymule_runtime::ContractTarget::wait(&self.wait_id),
                    schema,
                )?;
            }
        }
        match (&self.state, &self.result) {
            (WaitState::Completed, Some(result)) => result
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string())),
            (WaitState::Pending | WaitState::Cancelled, None) => Ok(()),
            _ => Err(DurableError::Validation(
                "wait lifecycle does not match its result".to_owned(),
            )),
        }
    }
}

impl EffectDispatch {
    /// Validate the complete self-contained outbox wire shape.
    ///
    /// # Errors
    /// Returns an error for invalid Effect identity, bindings, paths, lease metadata,
    /// or inconsistent dispatch state and terminal result.
    pub fn verify_wire(&self) -> DurableResult<()> {
        validate_sha256_identity("effect intent", &self.intent_id)?;
        validate_wire_non_empty("effect Run", &self.run_id)?;
        validate_wire_non_empty("effect operation", &self.operation)?;
        validate_sha256_identity("effect occurrence binding", &self.occurrence_binding)?;
        self.input
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        self.execution_binding
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if self.execution_binding.kind != cymule_runtime::EXECUTION_BINDING_VERSION {
            return Err(DurableError::Validation(
                "effect execution binding has the wrong Artifact kind".to_owned(),
            ));
        }
        if self.claim_epoch > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "effect claim epoch exceeds the exact integer range".to_owned(),
            ));
        }
        if let Some(owner) = &self.claim_owner {
            validate_wire_non_empty("effect claim owner", owner)?;
        }
        if let Some(result) = &self.result {
            result
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string()))?;
        }
        let reconciliation_matches = match self.state {
            OutboxState::Pending | OutboxState::Claimed => {
                self.reconciliation == cymule_core::ReconciliationState::NotRequired
            }
            OutboxState::Applied | OutboxState::NotApplied => matches!(
                self.reconciliation,
                cymule_core::ReconciliationState::NotRequired
                    | cymule_core::ReconciliationState::Resolved
            ),
            OutboxState::Unknown => matches!(
                self.reconciliation,
                cymule_core::ReconciliationState::Pending
                    | cymule_core::ReconciliationState::GovernanceRequired
            ),
            OutboxState::CancelledBeforeRelease => {
                self.reconciliation == cymule_core::ReconciliationState::Resolved
            }
        };
        if !reconciliation_matches {
            return Err(DurableError::Validation(
                "effect outbox state and reconciliation axis disagree".to_owned(),
            ));
        }
        if self.state == OutboxState::CancelledBeforeRelease
            && (self.claim_epoch != 0 || self.claim_owner.is_some() || self.result.is_some())
        {
            return Err(DurableError::Validation(
                "cancelled effect dispatch retains claim or result state".to_owned(),
            ));
        }
        let claim_matches = match self.state {
            OutboxState::Pending | OutboxState::CancelledBeforeRelease => {
                self.claim_epoch == 0 && self.claim_owner.is_none()
            }
            OutboxState::Claimed
            | OutboxState::Applied
            | OutboxState::NotApplied
            | OutboxState::Unknown => self.claim_epoch > 0 && self.claim_owner.is_some(),
        };
        if !claim_matches
            || matches!(
                self.state,
                OutboxState::Pending | OutboxState::Claimed | OutboxState::Unknown
            ) && self.result.is_some()
            || matches!(self.state, OutboxState::Pending | OutboxState::Claimed)
                && self.execution_availability
                    != cymule_core::EffectExecutionAvailability::Available
            || self.state == OutboxState::NotApplied && self.result.is_some()
        {
            return Err(DurableError::Validation(
                "effect outbox lifecycle does not match its claim or result".to_owned(),
            ));
        }
        Ok(())
    }
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
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result: Option<ArtifactRef>,
}

/// Provider-neutral wait kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitState {
    /// Completion may still be admitted.
    Pending,
    /// One authoritative completion was recorded.
    Completed,
    /// The wait was cancelled before completion.
    Cancelled,
}

/// Durable-private receipt proving that one Agent command observed an already
/// pending M1 input Wait and its exact Waiting Continuation in the same CAS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentInputSuspensionReceipt {
    pub(crate) receipt_version: String,
    pub(crate) receipt_id: String,
    pub(crate) agent_command_id: String,
    pub(crate) wait: WaitCondition,
    pub(crate) continuation_digest: String,
}

impl AgentInputSuspensionReceipt {
    pub(crate) fn new(
        agent_command_id: &str,
        wait: WaitCondition,
        continuation: &Continuation,
    ) -> DurableResult<Self> {
        let continuation_digest = canonical_digest(continuation)?;
        let receipt_id = content_id(
            AGENT_INPUT_SUSPENSION_RECEIPT_VERSION,
            &(agent_command_id, &wait, continuation_digest.as_str()),
        )?;
        let receipt = Self {
            receipt_version: AGENT_INPUT_SUSPENSION_RECEIPT_VERSION.to_owned(),
            receipt_id,
            agent_command_id: agent_command_id.to_owned(),
            wait,
            continuation_digest,
        };
        receipt.verify()?;
        receipt.verify_continuation(continuation)?;
        Ok(receipt)
    }

    pub(crate) fn verify(&self) -> DurableResult<()> {
        if self.receipt_version != AGENT_INPUT_SUSPENSION_RECEIPT_VERSION {
            return Err(DurableError::Validation(
                "Agent input suspension receipt has an unsupported version".to_owned(),
            ));
        }
        cymule_core::validate_content_id("Agent input command", &self.agent_command_id)?;
        cymule_core::validate_content_id("Agent input suspension receipt", &self.receipt_id)?;
        self.wait.verify_wire()?;
        if !matches!(self.wait.kind, WaitKind::Input { .. })
            || self.wait.state != WaitState::Pending
            || self.wait.result.is_some()
        {
            return Err(DurableError::Validation(
                "Agent input suspension must retain one pending input Wait".to_owned(),
            ));
        }
        validate_lower_hex_digest(
            "Agent input suspension Continuation",
            &self.continuation_digest,
        )?;
        let expected = content_id(
            AGENT_INPUT_SUSPENSION_RECEIPT_VERSION,
            &(
                self.agent_command_id.as_str(),
                &self.wait,
                self.continuation_digest.as_str(),
            ),
        )?;
        if self.receipt_id != expected {
            return Err(DurableError::Validation(
                "Agent input suspension receipt identity does not match its content".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn verify_continuation(&self, continuation: &Continuation) -> DurableResult<()> {
        self.verify()?;
        continuation.verify_wire()?;
        if continuation.run_id != self.wait.run_id
            || continuation.status != ContinuationStatus::Waiting
            || !continuation.wait_set.contains(&self.wait.wait_id)
            || canonical_digest(continuation)? != self.continuation_digest
        {
            return Err(DurableError::Validation(
                "Agent input suspension does not match its exact Waiting Continuation".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Durable-private receipt proving that one Agent response, result Artifact,
/// completed M1 input Wait, and resulting Continuation were committed together.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentInputCompletionReceipt {
    pub(crate) receipt_version: String,
    pub(crate) receipt_id: String,
    pub(crate) agent_command_id: String,
    pub(crate) suspension_receipt_id: String,
    pub(crate) wait: WaitCondition,
    pub(crate) result: ArtifactRef,
    pub(crate) continuation_digest: String,
}

impl AgentInputCompletionReceipt {
    pub(crate) fn new(
        agent_command_id: &str,
        suspension_receipt_id: &str,
        wait: WaitCondition,
        result: ArtifactRef,
        continuation: &Continuation,
    ) -> DurableResult<Self> {
        let continuation_digest = canonical_digest(continuation)?;
        let receipt_id = content_id(
            AGENT_INPUT_COMPLETION_RECEIPT_VERSION,
            &(
                agent_command_id,
                suspension_receipt_id,
                &wait,
                &result,
                continuation_digest.as_str(),
            ),
        )?;
        let receipt = Self {
            receipt_version: AGENT_INPUT_COMPLETION_RECEIPT_VERSION.to_owned(),
            receipt_id,
            agent_command_id: agent_command_id.to_owned(),
            suspension_receipt_id: suspension_receipt_id.to_owned(),
            wait,
            result,
            continuation_digest,
        };
        receipt.verify()?;
        receipt.verify_continuation(continuation)?;
        Ok(receipt)
    }

    pub(crate) fn verify(&self) -> DurableResult<()> {
        if self.receipt_version != AGENT_INPUT_COMPLETION_RECEIPT_VERSION {
            return Err(DurableError::Validation(
                "Agent input completion receipt has an unsupported version".to_owned(),
            ));
        }
        cymule_core::validate_content_id("Agent input command", &self.agent_command_id)?;
        cymule_core::validate_content_id(
            "Agent input suspension receipt",
            &self.suspension_receipt_id,
        )?;
        cymule_core::validate_content_id("Agent input completion receipt", &self.receipt_id)?;
        self.wait.verify_wire()?;
        self.result
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if !matches!(self.wait.kind, WaitKind::Input { .. })
            || self.wait.state != WaitState::Completed
            || self.wait.result.as_ref() != Some(&self.result)
        {
            return Err(DurableError::Validation(
                "Agent input completion must retain its exact completed input Wait and result"
                    .to_owned(),
            ));
        }
        validate_lower_hex_digest(
            "Agent input completion Continuation",
            &self.continuation_digest,
        )?;
        let expected = content_id(
            AGENT_INPUT_COMPLETION_RECEIPT_VERSION,
            &(
                self.agent_command_id.as_str(),
                self.suspension_receipt_id.as_str(),
                &self.wait,
                &self.result,
                self.continuation_digest.as_str(),
            ),
        )?;
        if self.receipt_id != expected {
            return Err(DurableError::Validation(
                "Agent input completion receipt identity does not match its content".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn verify_continuation(&self, continuation: &Continuation) -> DurableResult<()> {
        self.verify()?;
        continuation.verify_wire()?;
        if continuation.run_id != self.wait.run_id
            || continuation.wait_set.contains(&self.wait.wait_id)
            || !matches!(
                continuation.status,
                ContinuationStatus::Ready | ContinuationStatus::Waiting
            )
            || canonical_digest(continuation)? != self.continuation_digest
        {
            return Err(DurableError::Validation(
                "Agent input completion does not match its exact resulting Continuation".to_owned(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn ensure_wait_activation_source_matches(
    source: &WaitActivationSource,
    wait: &WaitCondition,
) -> DurableResult<()> {
    let matches = match (source, &wait.kind) {
        (WaitActivationSource::Signal { key }, WaitKind::Signal { key: expected }) => {
            key == expected
        }
        (WaitActivationSource::Timer { timer_id }, WaitKind::Timer { timer_id: expected }) => {
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

/// Fenced coordination lease. Time values are supplied by a Clock substrate.
///
/// This record owns only one coordination resource and its fencing epoch. It
/// does not grant capability authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinationLease {
    /// Coordinated resource key.
    pub resource: String,
    /// Current lease owner.
    pub owner: String,
    /// Monotonically increasing fencing epoch.
    pub epoch: u64,
    /// Logical expiry supplied by a Clock substrate.
    pub expires_at: u64,
}

impl CoordinationLease {
    /// Validate the closed coordination record before persistence or restore.
    ///
    /// # Errors
    /// Returns an error for empty resource or owner identities, an invalid fence,
    /// or a timestamp exceeding the exact cross-language integer range.
    pub fn verify(&self) -> DurableResult<()> {
        validate_wire_non_empty("coordination lease resource", &self.resource)?;
        validate_wire_non_empty("coordination lease owner", &self.owner)?;
        if self.epoch == 0
            || self.epoch > crate::MAX_EXACT_INTEGER
            || self.expires_at > crate::MAX_EXACT_INTEGER
        {
            return Err(DurableError::Validation(
                "coordination lease identity, epoch, or expiry is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Durable effect outbox entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectDispatch {
    /// Structural effect intent identity.
    pub intent_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Immutable Plan which defined this occurrence.
    pub origin_plan_id: String,
    /// Abstract effect operation.
    pub operation: String,
    /// Immutable input artifact.
    pub input: ArtifactRef,
    /// Exact executable binding Artifact selected for this occurrence.
    pub execution_binding: ArtifactRef,
    /// Pinned adapter binding.
    pub occurrence_binding: String,
    /// Availability of the exact pinned implementation.
    pub execution_availability: cymule_core::EffectExecutionAvailability,
    /// Canonical reconciliation axis projected from the semantic Effect.
    pub reconciliation: cymule_core::ReconciliationState,
    /// Outbox lifecycle state.
    pub state: OutboxState,
    /// Fencing epoch of the current claim.
    pub claim_epoch: u64,
    /// Current claim owner.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub claim_owner: Option<String>,
    /// Optional authoritative result artifact.
    #[serde(deserialize_with = "deserialize_required_nullable")]
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
    /// Execution terminated before this intent began dispatch.
    CancelledBeforeRelease,
}

/// Closed durable component outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum ComponentOutcome {
    /// The component returned its declared output value.
    Succeeded {
        /// Canonical output Artifact.
        output: ArtifactRef,
    },
    /// The component returned a declared application failure.
    ExpectedFailure {
        /// Stable application-owned failure code.
        code: String,
        /// Canonical failure detail Artifact.
        detail: ArtifactRef,
    },
}

impl ComponentOutcome {
    /// Validate the closed outcome independently of Artifact retention.
    ///
    /// # Errors
    /// Returns an error for malformed Artifact references or declared failure codes.
    pub fn verify_wire(&self) -> DurableResult<()> {
        match self {
            Self::Succeeded { output } => output
                .validate()
                .map_err(|error| DurableError::Validation(error.to_string())),
            Self::ExpectedFailure { code, detail } => {
                validate_failure_code(code)
                    .map_err(|error| DurableError::Validation(error.to_string()))?;
                detail
                    .validate()
                    .map_err(|error| DurableError::Validation(error.to_string()))
            }
        }
    }
}

/// Recorded nondeterministic component result for exact execution replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentOccurrence {
    /// Frozen semantic occurrence version.
    pub occurrence_version: String,
    /// Structural component occurrence identity.
    pub occurrence_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Exact semantic Plan interpreted for this occurrence.
    pub plan_id: String,
    /// Exact execution-binding Artifact pinned by the Run.
    pub binding_context: String,
    /// Structural invocation which owns the call site.
    pub invocation_id: String,
    /// Entry-rooted invocation path.
    pub invocation_path: Vec<cymule_core::InvocationPathSegment>,
    /// Definition containing the call site.
    pub definition_id: String,
    /// Nested Region path containing the call site.
    pub region_path: Vec<usize>,
    /// Stable Plan site.
    pub site_id: String,
    /// Exact step index of the call site.
    pub step_index: usize,
    /// Abstract component operation.
    pub component: String,
    /// Canonical input artifact.
    pub input: ArtifactRef,
    /// Closed successful or declared-failure outcome after completion.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub outcome: Option<ComponentOutcome>,
    /// Pinned implementation binding.
    pub occurrence_binding: String,
    /// Concrete implementation revision.
    pub implementation_revision: String,
    /// Monotonic number of provider Attempts admitted for this occurrence.
    pub attempt_count: u64,
    /// Exact latest Attempt; this is the sole O(1) provider-attempt frontier.
    pub latest_attempt_id: String,
    /// Digest of the exact post-call Continuation committed atomically with
    /// completion.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub continuation_digest: Option<String>,
    /// Closed occurrence lifecycle.
    pub state: ComponentOccurrenceState,
}

#[derive(Serialize)]
struct ComponentOccurrenceIdPreimage<'a> {
    run_id: &'a str,
    plan_id: &'a str,
    invocation_id: &'a str,
    scope_id: &'a str,
    definition_id: &'a str,
    region_path: &'a [usize],
    site_id: &'a str,
    input: &'a ArtifactRef,
    component: &'a str,
}

pub(crate) fn component_occurrence_id(occurrence: &ComponentOccurrence) -> DurableResult<String> {
    let scope_id = component_occurrence_scope_id(occurrence)?;
    content_id(
        COMPONENT_OCCURRENCE_VERSION,
        &ComponentOccurrenceIdPreimage {
            run_id: &occurrence.run_id,
            plan_id: &occurrence.plan_id,
            invocation_id: &occurrence.invocation_id,
            scope_id: &scope_id,
            definition_id: &occurrence.definition_id,
            region_path: &occurrence.region_path,
            site_id: &occurrence.site_id,
            input: &occurrence.input,
            component: &occurrence.component,
        },
    )
    .map_err(Into::into)
}

fn component_occurrence_scope_id(occurrence: &ComponentOccurrence) -> DurableResult<String> {
    if occurrence.region_path.is_empty() {
        return Ok(occurrence
            .invocation_path
            .last()
            .map_or(cymule_core::ROOT_SCOPE_ID, |segment| {
                segment.scope_id.as_str()
            })
            .to_owned());
    }
    cymule_core::plan_scope_id(
        &occurrence.run_id,
        &occurrence.plan_id,
        &occurrence.invocation_id,
        &occurrence.definition_id,
        &occurrence.region_path,
    )
    .map_err(Into::into)
}

fn validate_component_occurrence_authority(
    machine: &Machine,
    occurrence: &ComponentOccurrence,
) -> DurableResult<()> {
    let plan = machine.plan(&occurrence.plan_id).ok_or_else(|| {
        DurableError::Validation(format!(
            "component occurrence {} Plan is missing",
            occurrence.occurrence_id
        ))
    })?;
    let scope_id = component_occurrence_scope_id(occurrence)?;
    machine.validate_historical_execution_location(&cymule_core::ExecutionFrameLocation {
        run_id: &occurrence.run_id,
        plan_id: &occurrence.plan_id,
        invocation_id: &occurrence.invocation_id,
        invocation_path: &occurrence.invocation_path,
        definition_id: &occurrence.definition_id,
        region_path: &occurrence.region_path,
        scope_id: &scope_id,
        next_step: occurrence.step_index,
    })?;
    let definition = plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == occurrence.definition_id)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "component occurrence {} definition is missing",
                occurrence.occurrence_id
            ))
        })?;
    let step = region_at_path(&definition.body, &occurrence.region_path)?
        .steps
        .get(occurrence.step_index)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "component occurrence {} step is missing",
                occurrence.occurrence_id
            ))
        })?;
    let Operation::Call { component, .. } = &step.operation else {
        return Err(DurableError::Validation(format!(
            "component occurrence {} site is not a component call",
            occurrence.occurrence_id
        )));
    };
    if step.id != occurrence.site_id || component != &occurrence.component {
        return Err(DurableError::Validation(format!(
            "component occurrence {} does not match its Plan site",
            occurrence.occurrence_id
        )));
    }
    let binding_ref = ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: occurrence.binding_context.clone(),
        kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
    };
    let binding_record = machine.artifact(&binding_ref).ok_or_else(|| {
        DurableError::Validation(format!(
            "component occurrence {} execution binding is missing",
            occurrence.occurrence_id
        ))
    })?;
    let binding = cymule_runtime::ExecutionBinding::decode(&binding_record.bytes)?;
    if binding.artifact_ref()? != binding_ref {
        return Err(DurableError::Validation(format!(
            "component occurrence {} execution binding identity is invalid",
            occurrence.occurrence_id
        )));
    }
    binding.admit_plan(plan)?;
    let selected = binding
        .components
        .get(&occurrence.component)
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "component occurrence {} operation is unbound",
                occurrence.occurrence_id
            ))
        })?;
    if binding.occurrence_binding(
        cymule_runtime::ExecutionOperationKind::Component,
        &occurrence.component,
    )? != occurrence.occurrence_binding
        || selected.operation_revision != occurrence.implementation_revision
    {
        return Err(DurableError::Validation(format!(
            "component occurrence {} implementation pin is invalid",
            occurrence.occurrence_id
        )));
    }
    let input_record = machine.artifact(&occurrence.input).ok_or_else(|| {
        DurableError::Validation(format!(
            "component occurrence {} input Artifact is missing",
            occurrence.occurrence_id
        ))
    })?;
    let input = decode_artifact_value(&occurrence.input, input_record)?;
    cymule_runtime::PlanContracts::compile(&plan.candidate)?
        .validate_component_input(&occurrence.component, &input)?;
    Ok(())
}

impl ComponentOccurrence {
    /// Verify the versioned closed occurrence shape.
    ///
    /// # Errors
    /// Returns an error for invalid identities, bindings, paths, attempt metadata,
    /// or inconsistent lifecycle, outcome, and Continuation digest.
    pub fn verify(&self) -> DurableResult<()> {
        for (kind, identity) in [
            ("component occurrence", self.occurrence_id.as_str()),
            ("component Run", self.run_id.as_str()),
            ("component Plan", self.plan_id.as_str()),
            ("component binding context", self.binding_context.as_str()),
            ("component invocation", self.invocation_id.as_str()),
            ("component site", self.site_id.as_str()),
            ("component operation", self.component.as_str()),
            (
                "component implementation revision",
                self.implementation_revision.as_str(),
            ),
        ] {
            validate_wire_non_empty(kind, identity)?;
        }
        validate_sha256_identity("component occurrence binding", &self.occurrence_binding)?;
        if self.occurrence_version != COMPONENT_OCCURRENCE_VERSION {
            return Err(DurableError::Validation(
                "component occurrence identity or binding is invalid".to_owned(),
            ));
        }
        if self.attempt_count == 0 || self.attempt_count > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(format!(
                "component occurrence {} Attempt count is invalid",
                self.occurrence_id
            )));
        }
        cymule_core::validate_content_id(
            "component occurrence latest Attempt",
            &self.latest_attempt_id,
        )?;
        self.input
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        validate_wire_indices("component occurrence Region path", &self.region_path)?;
        if u64::try_from(self.step_index).map_or(true, |value| value > crate::MAX_EXACT_INTEGER) {
            return Err(DurableError::Validation(
                "component occurrence step exceeds the exact integer range".to_owned(),
            ));
        }
        for segment in &self.invocation_path {
            validate_wire_non_empty("component invocation site", &segment.site_id)?;
            validate_wire_non_empty("component invocation scope", &segment.scope_id)?;
            validate_wire_indices("component invocation Region path", &segment.region_path)?;
        }
        if component_occurrence_id(self)? != self.occurrence_id {
            return Err(DurableError::Validation(format!(
                "component occurrence {} identity does not match its typed preimage",
                self.occurrence_id
            )));
        }
        if let Some(outcome) = &self.outcome {
            outcome.verify_wire()?;
        }
        match (
            self.state,
            self.outcome.as_ref(),
            self.continuation_digest.as_deref(),
        ) {
            (ComponentOccurrenceState::Pending, None, None) => Ok(()),
            (ComponentOccurrenceState::Completed, Some(_), Some(digest)) => {
                if digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                {
                    Ok(())
                } else {
                    Err(DurableError::Validation(format!(
                        "component occurrence {} Continuation digest is invalid",
                        self.occurrence_id
                    )))
                }
            }
            _ => Err(DurableError::Validation(format!(
                "component occurrence {} lifecycle is inconsistent",
                self.occurrence_id
            ))),
        }
    }
}

/// Closed legacy component-occurrence lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentOccurrenceState {
    /// Admitted before provider I/O; no result is durable yet.
    Pending,
    /// Result and post-call Continuation committed together.
    Completed,
}

/// One provider invocation Attempt under an exact execution-claim fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationAttempt {
    /// Frozen Attempt version.
    pub attempt_version: String,
    /// Content-addressed Attempt identity.
    pub attempt_id: String,
    /// Stable semantic occurrence identity.
    pub occurrence_id: String,
    /// Owning Run.
    pub run_id: String,
    /// Monotonic Attempt ordinal within the occurrence.
    pub attempt_ordinal: u64,
    /// Exact predecessor in this occurrence's Attempt chain.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub previous_attempt_id: Option<String>,
    /// Current continuation Attempt that admitted provider I/O.
    pub continuation_attempt_id: String,
    /// Execution owner which admitted provider I/O.
    pub execution_claim_owner: String,
    /// Execution-claim fence that admitted provider I/O.
    pub execution_claim_fence: u64,
    /// Runtime-derived concrete Component occurrence binding.
    pub operation_occurrence_binding: String,
    /// Attempt-specific transport request identity.
    pub transport_request_id: String,
    /// Closed Attempt lifecycle.
    pub state: OperationAttemptState,
    /// Provider success or declared failure after terminal checkpoint.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub outcome: Option<ComponentOutcome>,
}

pub(crate) struct OperationAttemptIdentity<'a> {
    pub(crate) occurrence_id: &'a str,
    pub(crate) attempt_ordinal: u64,
    pub(crate) previous_attempt_id: Option<&'a str>,
    pub(crate) run_id: &'a str,
    pub(crate) continuation_attempt_id: &'a str,
    pub(crate) execution_claim_owner: &'a str,
    pub(crate) execution_claim_fence: u64,
    pub(crate) operation_occurrence_binding: &'a str,
}

pub(crate) fn operation_attempt_id(
    identity: &OperationAttemptIdentity<'_>,
) -> DurableResult<String> {
    content_id(
        OPERATION_ATTEMPT_VERSION,
        &(
            identity.occurrence_id,
            identity.attempt_ordinal,
            identity.previous_attempt_id,
            identity.run_id,
            identity.continuation_attempt_id,
            identity.execution_claim_owner,
            identity.execution_claim_fence,
            identity.operation_occurrence_binding,
        ),
    )
    .map_err(Into::into)
}

impl OperationAttempt {
    /// Verify the content identities and closed lifecycle.
    ///
    /// # Errors
    /// Returns an error for invalid content identities, execution fencing metadata,
    /// or inconsistent attempt lifecycle and outcome.
    pub fn verify(&self) -> DurableResult<()> {
        for (kind, identity) in [
            ("operation Attempt", self.attempt_id.as_str()),
            ("operation occurrence", self.occurrence_id.as_str()),
            ("operation Run", self.run_id.as_str()),
            (
                "continuation Attempt",
                self.continuation_attempt_id.as_str(),
            ),
            ("execution claim owner", self.execution_claim_owner.as_str()),
            (
                "operation occurrence binding",
                self.operation_occurrence_binding.as_str(),
            ),
            ("transport request", self.transport_request_id.as_str()),
        ] {
            validate_wire_non_empty(kind, identity)?;
        }
        if self.attempt_version != OPERATION_ATTEMPT_VERSION
            || self.attempt_ordinal == 0
            || self.attempt_ordinal > crate::MAX_EXACT_INTEGER
            || self.execution_claim_fence == 0
            || self.execution_claim_fence > crate::MAX_EXACT_INTEGER
        {
            return Err(DurableError::Validation(
                "operation Attempt identity or fence is invalid".to_owned(),
            ));
        }
        if let Some(previous) = &self.previous_attempt_id {
            cymule_core::validate_content_id("previous operation Attempt", previous)?;
        }
        if (self.attempt_ordinal == 1) != self.previous_attempt_id.is_none() {
            return Err(DurableError::Validation(
                "operation Attempt predecessor does not match its ordinal".to_owned(),
            ));
        }
        let expected = operation_attempt_id(&OperationAttemptIdentity {
            occurrence_id: &self.occurrence_id,
            attempt_ordinal: self.attempt_ordinal,
            previous_attempt_id: self.previous_attempt_id.as_deref(),
            run_id: &self.run_id,
            continuation_attempt_id: &self.continuation_attempt_id,
            execution_claim_owner: &self.execution_claim_owner,
            execution_claim_fence: self.execution_claim_fence,
            operation_occurrence_binding: &self.operation_occurrence_binding,
        })?;
        let expected_transport = content_id(
            TRANSPORT_REQUEST_ID_DOMAIN,
            &(expected.as_str(), self.continuation_attempt_id.as_str()),
        )?;
        if self.attempt_id != expected || self.transport_request_id != expected_transport {
            return Err(DurableError::Validation(
                "operation Attempt content identity is invalid".to_owned(),
            ));
        }
        if let Some(outcome) = &self.outcome {
            outcome.verify_wire()?;
        }
        match self.state {
            OperationAttemptState::Completed if self.outcome.is_some() => Ok(()),
            OperationAttemptState::Running | OperationAttemptState::Superseded
                if self.outcome.is_none() =>
            {
                Ok(())
            }
            _ => Err(DurableError::Validation(format!(
                "operation Attempt {} lifecycle is inconsistent",
                self.attempt_id
            ))),
        }
    }
}

pub(crate) fn validate_operation_attempt_history(
    occurrence: &ComponentOccurrence,
    attempts: &mut Vec<&OperationAttempt>,
    continuation_status: ContinuationStatus,
) -> DurableResult<()> {
    attempts.sort_by_key(|attempt| attempt.attempt_ordinal);
    let Some((&last_attempt, prior_attempts)) = attempts.split_last() else {
        return Err(DurableError::Validation(format!(
            "component occurrence {} Attempt history is not closed",
            occurrence.occurrence_id
        )));
    };
    let ordinals_are_contiguous = attempts
        .iter()
        .enumerate()
        .all(|(index, attempt)| attempt.attempt_ordinal == index as u64 + 1);
    let running_count = attempts
        .iter()
        .filter(|attempt| attempt.state == OperationAttemptState::Running)
        .count();
    let completed_count = attempts
        .iter()
        .filter(|attempt| attempt.state == OperationAttemptState::Completed)
        .count();
    let lifecycle_is_closed = match occurrence.state {
        ComponentOccurrenceState::Pending => {
            completed_count == 0
                && ((running_count == 1
                    && last_attempt.state == OperationAttemptState::Running
                    && prior_attempts
                        .iter()
                        .all(|attempt| attempt.state == OperationAttemptState::Superseded))
                    || (matches!(
                        continuation_status,
                        ContinuationStatus::Running
                            | ContinuationStatus::Failed
                            | ContinuationStatus::Cancelled
                    ) && !attempts.is_empty()
                        && attempts
                            .iter()
                            .all(|attempt| attempt.state == OperationAttemptState::Superseded)))
        }
        ComponentOccurrenceState::Completed => {
            running_count == 0
                && completed_count == 1
                && last_attempt.state == OperationAttemptState::Completed
                && last_attempt.outcome == occurrence.outcome
                && prior_attempts
                    .iter()
                    .all(|attempt| attempt.state == OperationAttemptState::Superseded)
        }
    };
    if !ordinals_are_contiguous || running_count > 1 || !lifecycle_is_closed {
        return Err(DurableError::Validation(format!(
            "component occurrence {} Attempt history is not closed",
            occurrence.occurrence_id
        )));
    }
    Ok(())
}

/// Closed provider Attempt lifecycle for legacy component calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationAttemptState {
    /// Provider I/O is admitted and may be in flight.
    Running,
    /// Provider output and semantic occurrence completion committed together.
    Completed,
    /// Explicit takeover fenced this in-flight Attempt before completion.
    Superseded,
}

fn validate_component_outcome(
    machine: &Machine,
    outcome: &ComponentOutcome,
    owner: &str,
) -> DurableResult<()> {
    match outcome {
        ComponentOutcome::Succeeded { output } => {
            require_artifact(machine, output, &format!("{owner} output"))
        }
        ComponentOutcome::ExpectedFailure { code, detail } => {
            validate_failure_code(code)
                .map_err(|error| DurableError::Validation(error.to_string()))?;
            require_artifact(machine, detail, &format!("{owner} failure detail"))
        }
    }
}

pub(crate) fn validate_wire_non_empty(kind: &str, value: &str) -> DurableResult<()> {
    cymule_core::validate_identity(kind, value).map_err(Into::into)
}

pub(crate) fn validate_sha256_identity(kind: &str, value: &str) -> DurableResult<()> {
    validate_wire_non_empty(kind, value)?;
    let valid = value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    });
    if valid {
        Ok(())
    } else {
        Err(DurableError::Validation(format!(
            "{kind} must be an exact lowercase SHA-256 content identity"
        )))
    }
}

fn validate_wire_indices(kind: &str, values: &[usize]) -> DurableResult<()> {
    if values
        .iter()
        .any(|value| u64::try_from(*value).map_or(true, |value| value > crate::MAX_EXACT_INTEGER))
    {
        return Err(DurableError::Validation(format!(
            "{kind} exceeds the exact integer range"
        )));
    }
    Ok(())
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

#[derive(Serialize)]
struct SnapshotIdPreimage<'a> {
    source_revision: &'a str,
    causal_frontier: &'a BTreeSet<String>,
    continuation_ids: &'a BTreeSet<String>,
    unresolved_obligations: &'a BTreeSet<String>,
    occurrence_bindings: &'a BTreeSet<String>,
}

impl SnapshotRecord {
    /// Construct content-addressed snapshot metadata from one exact durable
    /// revision and its complete authority projection.
    ///
    /// # Errors
    /// Returns an error if the source revision or any retained frontier identity is
    /// invalid or the snapshot identity cannot be canonically derived.
    pub fn new(
        source_revision: String,
        causal_frontier: BTreeSet<String>,
        continuation_ids: BTreeSet<String>,
        unresolved_obligations: BTreeSet<String>,
        occurrence_bindings: BTreeSet<String>,
    ) -> DurableResult<Self> {
        let mut record = Self {
            snapshot_id: String::new(),
            source_revision,
            causal_frontier,
            continuation_ids,
            unresolved_obligations,
            occurrence_bindings,
        };
        record.snapshot_id = record.expected_id()?;
        record.verify()?;
        Ok(record)
    }

    /// Verify every metadata identity and the content-addressed snapshot ID.
    ///
    /// # Errors
    /// Returns an error for invalid metadata identities or a snapshot ID that does
    /// not match the complete typed preimage.
    pub fn verify(&self) -> DurableResult<()> {
        cymule_core::validate_content_id("snapshot source revision", &self.source_revision)?;
        for event_id in &self.causal_frontier {
            validate_sha256_identity("snapshot causal frontier event", event_id)?;
        }
        for run_id in &self.continuation_ids {
            validate_wire_non_empty("snapshot Continuation", run_id)?;
        }
        for obligation_id in &self.unresolved_obligations {
            validate_sha256_identity("snapshot unresolved obligation", obligation_id)?;
        }
        for binding in &self.occurrence_bindings {
            validate_sha256_identity("snapshot occurrence binding", binding)?;
        }
        let expected = self.expected_id()?;
        if self.snapshot_id != expected {
            return Err(DurableError::Integrity {
                code: "snapshot_identity_mismatch".to_owned(),
                message: format!(
                    "snapshot {} does not match its complete metadata",
                    self.snapshot_id
                ),
            });
        }
        Ok(())
    }

    fn expected_id(&self) -> DurableResult<String> {
        content_id(
            SNAPSHOT_ID_DOMAIN,
            &SnapshotIdPreimage {
                source_revision: &self.source_revision,
                causal_frontier: &self.causal_frontier,
                continuation_ids: &self.continuation_ids,
                unresolved_obligations: &self.unresolved_obligations,
                occurrence_bindings: &self.occurrence_bindings,
            },
        )
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use cymule_profile_protocol::agent::AgentWorkspaceCommandPhase as WorkspacePhase;

    fn workspace_checkpoint() -> DurableResult<AgentWorkspaceCheckpoint> {
        let run_id = "run:workspace-checkpoint".to_owned();
        let plan_id = content_id("test.workspace-checkpoint/1", &"plan")?;
        let input = receipt_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, &json!({}))?.reference;
        let binding =
            receipt_artifact(cymule_core::EXECUTION_BINDING_ARTIFACT_KIND, &json!({}))?.reference;
        let invocation_id = cymule_core::plan_invocation_id(&run_id, &plan_id, "main", &[])?;
        let scope_id = cymule_core::plan_scope_id(&run_id, &plan_id, &invocation_id, "main", &[0])?;
        let root_frame = cymule_durable_protocol::FrameState {
            definition_id: "main".to_owned(),
            invocation_id,
            invocation_path: Vec::new(),
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            input: input.clone(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        };
        let mut child_frame = root_frame.clone();
        child_frame.scope_id.clone_from(&scope_id);
        child_frame.region_path = vec![0];
        let continuation = Box::new(Continuation {
            continuation_version: cymule_durable_protocol::CONTINUATION_STATE_VERSION.to_owned(),
            run_id: run_id.clone(),
            plan_id,
            binding_context: binding.artifact_id,
            frames: vec![root_frame, child_frame],
            state: Some(input),
            wait_set: BTreeSet::new(),
            scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned(), scope_id.clone()],
            epoch: 0,
            execution_fence: 0,
            execution_claim: None,
            status: ContinuationStatus::Ready,
        });
        let continuation_digest = agent_workspace_continuation_digest(&continuation)?;
        Ok(AgentWorkspaceCheckpoint {
            agent_command_id: content_id("test.workspace-checkpoint/1", &"outer-command")?,
            run_id,
            scope_id,
            occurrence_id: "occurrence:workspace".to_owned(),
            phase: WorkspacePhase::StartAbortDispatch,
            source_machine_authority_root: "1".repeat(64),
            machine_authority_root: "1".repeat(64),
            core_batch_id: None,
            core_batch_receipt_id: None,
            dispatch_clock: None,
            source_continuation_digest: continuation_digest.clone(),
            continuation,
            continuation_digest,
            effect_before: None,
            effect_after: None,
            outbox_before: None,
            outbox_after: None,
            lease_before: None,
            lease_after: None,
        })
    }

    fn install_workspace_effect(checkpoint: &mut AgentWorkspaceCheckpoint) -> DurableResult<()> {
        let frame = checkpoint.continuation.frames.last().ok_or_else(|| {
            DurableError::NotFound("workspace fixture frame is absent".to_owned())
        })?;
        let args = receipt_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, &json!({}))?.reference;
        let mut effect = cymule_core::EffectProjection {
            intent_id: String::new(),
            origin_plan_id: checkpoint.continuation.plan_id.clone(),
            scope_id: checkpoint.scope_id.clone(),
            invocation_id: frame.invocation_id.clone(),
            invocation_path: frame.invocation_path.clone(),
            definition_id: frame.definition_id.clone(),
            region_path: frame.region_path.clone(),
            site_id: "effect".to_owned(),
            occurrence: "workspace".to_owned(),
            effect_schema_version: cymule_core::EFFECT_SCHEMA_VERSION.to_owned(),
            operation: "test.workspace".to_owned(),
            profile: workspace_effect_profile(),
            args,
            execution_binding: ArtifactRef {
                identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
                artifact_id: checkpoint.continuation.binding_context.clone(),
                kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
            },
            occurrence_binding: content_id("test.workspace-checkpoint/1", &"binding")?,
            execution_availability: cymule_core::EffectExecutionAvailability::Available,
            phase: cymule_core::EffectPhase::DispatchStarted,
            outcome: cymule_core::WorldOutcome::Unknown,
            reconciliation: cymule_core::ReconciliationState::Pending,
        };
        effect.intent_id =
            cymule_core::effect_intent_id(&cymule_core::EffectIntentIdentityInput {
                run_id: &checkpoint.run_id,
                plan_id: &effect.origin_plan_id,
                invocation_id: &effect.invocation_id,
                site_id: &effect.site_id,
                scope_id: &effect.scope_id,
                occurrence: &effect.occurrence,
                args: &effect.args,
                effect_schema_version: &effect.effect_schema_version,
            })?;
        let lease = CoordinationLease {
            resource: effect.intent_id.clone(),
            owner: "driver:workspace".to_owned(),
            epoch: 1,
            expires_at: 20,
        };
        let outbox = EffectDispatch {
            intent_id: effect.intent_id.clone(),
            run_id: checkpoint.run_id.clone(),
            origin_plan_id: effect.origin_plan_id.clone(),
            operation: effect.operation.clone(),
            input: effect.args.clone(),
            execution_binding: effect.execution_binding.clone(),
            occurrence_binding: effect.occurrence_binding.clone(),
            execution_availability: effect.execution_availability,
            reconciliation: effect.reconciliation,
            state: OutboxState::Unknown,
            claim_epoch: lease.epoch,
            claim_owner: Some(lease.owner.clone()),
            result: None,
        };
        checkpoint.phase = WorkspacePhase::SettleEffectUnknown;
        checkpoint.effect_before = Some(effect.clone());
        checkpoint.effect_after = Some(effect);
        checkpoint.outbox_before = Some(outbox.clone());
        checkpoint.outbox_after = Some(outbox);
        checkpoint.lease_before = Some(lease.clone());
        checkpoint.lease_after = Some(lease);
        checkpoint.verify()
    }

    fn workspace_effect_profile() -> cymule_core::EffectProfile {
        cymule_core::EffectProfile {
            mutation: cymule_core::MutationKind::Mutating,
            dispatch: cymule_core::DispatchPolicy::OnScopeCommit,
            reconciliation: cymule_core::ReconciliationMode::Queryable,
            keyed_idempotency: true,
            irreversible: false,
        }
    }

    #[test]
    fn workspace_no_core_checkpoint_requires_exact_unchanged_m1_values() -> DurableResult<()> {
        let checkpoint = workspace_checkpoint()?;
        checkpoint.verify()?;
        let scenarios: [fn(&mut AgentWorkspaceCheckpoint); 5] = [
            |value| value.machine_authority_root = "2".repeat(64),
            |value| value.core_batch_id = Some(value.agent_command_id.clone()),
            |value| {
                value.core_batch_id = Some(value.agent_command_id.clone());
                value.core_batch_receipt_id = Some(value.agent_command_id.clone());
            },
            |value| value.phase = WorkspacePhase::SettleAbortApplied,
            |value| value.phase = WorkspacePhase::ProposeEffect,
        ];
        for alter in scenarios {
            let mut candidate = checkpoint.clone();
            alter(&mut candidate);
            assert!(candidate.verify().is_err());
        }
        let mut advanced = checkpoint;
        advanced.continuation.epoch += 1;
        advanced.continuation_digest = agent_workspace_continuation_digest(&advanced.continuation)?;
        assert!(
            matches!(advanced.verify(), Err(DurableError::Integrity { code, .. })
            if code == "agent_workspace_no_core_change_mismatch")
        );
        Ok(())
    }

    #[test]
    fn workspace_continuation_digest_has_one_existing_typed_domain() -> DurableResult<()> {
        let mut checkpoint = workspace_checkpoint()?;
        assert_eq!(
            checkpoint.continuation_digest,
            content_id(
                cymule_durable_protocol::CONTINUATION_STATE_VERSION,
                &checkpoint.continuation,
            )?
        );
        let raw = canonical_digest(&checkpoint.continuation)?;
        assert_ne!(checkpoint.continuation_digest, raw);
        checkpoint.source_continuation_digest.clone_from(&raw);
        checkpoint.continuation_digest = raw;
        assert!(checkpoint.verify().is_err());
        Ok(())
    }

    #[test]
    fn workspace_receipt_key_uses_outer_command_and_binds_owner_fields() -> DurableResult<()> {
        let checkpoint = workspace_checkpoint()?;
        let mut receipt = CoupledCheckpointReceipt::new(CoupledCheckpoint::AgentWorkspace {
            checkpoint: Box::new(checkpoint.clone()),
        })?;
        assert!(receipt.manifests().is_empty());
        assert_eq!(
            receipt.coupling_id,
            content_id(
                COUPLED_CHECKPOINT_KEY_DOMAIN,
                &("agent_workspace", checkpoint.agent_command_id.as_str()),
            )?
        );
        let encoded = cymule_core::canonical_bytes(&receipt)?;
        let reopened: CoupledCheckpointReceipt = cymule_core::decode_json(&encoded)?;
        assert_eq!(reopened, receipt);
        reopened.verify()?;
        let CoupledCheckpoint::AgentWorkspace { checkpoint } = &mut receipt.checkpoint else {
            panic!("workspace receipt has another variant")
        };
        checkpoint.occurrence_id = "occurrence:another-owner".to_owned();
        assert!(
            matches!(receipt.verify(), Err(DurableError::Integrity { code, .. })
            if code == "coupled_checkpoint_receipt_identity_mismatch")
        );
        Ok(())
    }

    #[test]
    fn workspace_effect_checkpoint_rejects_a_self_consistent_foreign_lease() -> DurableResult<()> {
        let mut checkpoint = workspace_checkpoint()?;
        install_workspace_effect(&mut checkpoint)?;
        let foreign = content_id("test.workspace-checkpoint/1", &"foreign-intent")?;
        for lease in [&mut checkpoint.lease_before, &mut checkpoint.lease_after] {
            let Some(lease) = lease else {
                panic!("workspace fixture lease is absent")
            };
            lease.resource.clone_from(&foreign);
        }
        assert!(
            matches!(checkpoint.verify(), Err(DurableError::Integrity { code, .. })
            if code == "agent_workspace_lease_mismatch")
        );
        Ok(())
    }

    #[test]
    fn workspace_material_only_checkpoint_preserves_its_m1_neighborhood() -> DurableResult<()> {
        let mut checkpoint = workspace_checkpoint()?;
        checkpoint.phase = WorkspacePhase::SettleAbortUnknown;
        checkpoint.core_batch_id = Some(content_id(
            "test.workspace-checkpoint/1",
            &"material-batch",
        )?);
        checkpoint.core_batch_receipt_id = Some(content_id(
            "test.workspace-checkpoint/1",
            &"material-receipt",
        )?);
        checkpoint.machine_authority_root = "2".repeat(64);
        checkpoint.verify()?;
        checkpoint.continuation.epoch += 1;
        checkpoint.continuation_digest =
            agent_workspace_continuation_digest(&checkpoint.continuation)?;
        assert!(checkpoint.verify().is_err());
        Ok(())
    }

    #[test]
    fn workspace_checkpoint_optional_authorities_are_required_nullable() -> DurableResult<()> {
        let checkpoint = workspace_checkpoint()?;
        checkpoint.verify()?;
        let Value::Object(fields) = serde_json::to_value(checkpoint)? else {
            panic!("workspace checkpoint is not an object")
        };
        for field in [
            "core_batch_id",
            "core_batch_receipt_id",
            "dispatch_clock",
            "effect_before",
            "effect_after",
            "outbox_before",
            "outbox_after",
            "lease_before",
            "lease_after",
        ] {
            assert_eq!(fields.get(field), Some(&Value::Null));
            let mut missing = fields.clone();
            missing.remove(field);
            assert!(
                serde_json::from_value::<AgentWorkspaceCheckpoint>(Value::Object(missing)).is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn shared_workspace_checkpoint_fixture_verifies_against_rust() -> DurableResult<()> {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/harness/fixtures/agent-workspace-checkpoint.json"
        ));
        let checkpoint: AgentWorkspaceCheckpoint = cymule_core::decode_json(bytes)?;
        checkpoint.verify()?;
        assert_eq!(checkpoint, workspace_checkpoint()?);
        Ok(())
    }

    #[test]
    fn workspace_dispatch_retains_exact_clock_scope_and_positive_lease() -> DurableResult<()> {
        let mut checkpoint = workspace_checkpoint()?;
        install_workspace_effect(&mut checkpoint)?;
        checkpoint.phase = WorkspacePhase::StartEffectDispatch;
        checkpoint.effect_before = None;
        checkpoint.outbox_before = None;
        checkpoint.lease_before = None;
        let Some(effect) = &mut checkpoint.effect_after else {
            panic!("fixture Effect is absent")
        };
        effect.outcome = cymule_core::WorldOutcome::Unobserved;
        effect.reconciliation = cymule_core::ReconciliationState::NotRequired;
        let Some(outbox) = &mut checkpoint.outbox_after else {
            panic!("fixture outbox is absent")
        };
        outbox.state = OutboxState::Claimed;
        outbox.reconciliation = cymule_core::ReconciliationState::NotRequired;
        checkpoint.core_batch_id = Some(content_id(
            "test.workspace-checkpoint/1",
            &"dispatch-batch",
        )?);
        checkpoint.core_batch_receipt_id = Some(content_id(
            "test.workspace-checkpoint/1",
            &"dispatch-receipt",
        )?);
        checkpoint.machine_authority_root = "2".repeat(64);
        let source_id = "clock:workspace-checkpoint".to_owned();
        let source_generation = content_id("test.workspace-checkpoint/1", &"clock-generation")?;
        let scope = cymule_durable_protocol::execution_clock_scope(&checkpoint.run_id)?;
        checkpoint.dispatch_clock = Some(cymule_durable_protocol::ClockObservation {
            clock_version: cymule_durable_protocol::CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id: cymule_durable_protocol::clock_observation_id(
                &source_id,
                &source_generation,
                &scope,
                10,
                1000,
            )?,
            source_id,
            source_generation,
            scope,
            logical_time: 10,
            observed_unix_ms: 1000,
        });
        checkpoint.verify()?;
        let mut missing = checkpoint.clone();
        missing.dispatch_clock = None;
        assert!(
            matches!(missing.verify(), Err(DurableError::Integrity { code, .. })
            if code == "agent_workspace_dispatch_clock_presence_mismatch")
        );
        let mut foreign = checkpoint.clone();
        let Some(clock) = &mut foreign.dispatch_clock else {
            panic!("fixture Clock is absent")
        };
        clock.scope = cymule_durable_protocol::execution_clock_scope("run:foreign-clock")?;
        clock.observation_id = cymule_durable_protocol::clock_observation_id(
            &clock.source_id,
            &clock.source_generation,
            &clock.scope,
            clock.logical_time,
            clock.observed_unix_ms,
        )?;
        assert!(
            matches!(foreign.verify(), Err(DurableError::Integrity { code, .. })
            if code == "agent_workspace_dispatch_clock_mismatch")
        );
        let Some(lease) = &mut checkpoint.lease_after else {
            panic!("fixture lease is absent")
        };
        lease.expires_at = 10;
        assert!(
            matches!(checkpoint.verify(), Err(DurableError::Integrity { code, .. })
            if code == "agent_workspace_dispatch_clock_mismatch")
        );
        Ok(())
    }

    fn workspace_invoke_site(index: usize) -> DurableResult<String> {
        let alphabet = b"abcdefghijklmnopqrstuvwxyz";
        let first = alphabet.get(index / alphabet.len()).ok_or_else(|| {
            DurableError::Validation("large fixture exhausts its unique sites".to_owned())
        })?;
        let second = alphabet.get(index % alphabet.len()).ok_or_else(|| {
            DurableError::Validation("large fixture has an invalid site index".to_owned())
        })?;
        Ok(format!("{}{}", char::from(*first), char::from(*second)))
    }

    fn large_workspace_plan() -> DurableResult<SealedPlan> {
        const DEPTH: usize = 180;
        let names = (0..DEPTH)
            .map(|index| format!("{}{:03}", "f".repeat(157), index))
            .collect::<Vec<_>>();
        let mut definitions = Vec::new();
        for (index, name) in names.iter().enumerate() {
            let body = if let Some(next) = names.get(index + 1) {
                Region {
                    steps: vec![cymule_core::Step {
                        id: workspace_invoke_site(index)?,
                        operation: cymule_core::Operation::Invoke {
                            definition: next.clone(),
                            input: Expression::Input,
                            bind: Some("child".to_owned()),
                        },
                    }],
                    result: Expression::Binding {
                        name: "child".to_owned(),
                    },
                }
            } else {
                Region {
                    steps: vec![cymule_core::Step {
                        id: "effect".to_owned(),
                        operation: cymule_core::Operation::Effect {
                            effect: "test.workspace".to_owned(),
                            input: Expression::Input,
                            occurrence: "workspace".to_owned(),
                            bind: None,
                        },
                    }],
                    result: Expression::Input,
                }
            };
            definitions.push(cymule_core::Definition {
                id: name.clone(),
                input_schema: json!({}),
                output_schema: json!({}),
                body,
            });
        }
        let entry = names
            .first()
            .ok_or_else(|| DurableError::Validation("large fixture has no entry".to_owned()))?
            .clone();
        cymule_core::seal_plan(cymule_core::PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "large_workspace_receipt".to_owned(),
            entry,
            components: Vec::new(),
            effects: vec![cymule_core::EffectContract {
                id: "test.workspace".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                profile: workspace_effect_profile(),
                requirements: BTreeMap::new(),
            }],
            definitions,
            metadata: BTreeMap::new(),
        })
        .map_err(Into::into)
    }

    fn install_large_workspace_continuation(
        checkpoint: &mut AgentWorkspaceCheckpoint,
        plan: &SealedPlan,
    ) -> DurableResult<()> {
        checkpoint.scope_id = cymule_core::ROOT_SCOPE_ID.to_owned();
        checkpoint.continuation.plan_id.clone_from(&plan.plan_id);
        checkpoint.continuation.scope_stack = vec![checkpoint.scope_id.clone()];
        checkpoint.continuation.frames.clear();
        let mut path = Vec::new();
        for (index, definition) in plan.candidate.definitions.iter().enumerate() {
            let input_kind = if index == 0 {
                cymule_core::RUN_INPUT_ARTIFACT_KIND
            } else {
                INVOCATION_INPUT_ARTIFACT_KIND
            };
            checkpoint
                .continuation
                .frames
                .push(cymule_durable_protocol::FrameState {
                    definition_id: definition.id.clone(),
                    invocation_id: cymule_core::plan_invocation_id(
                        &checkpoint.run_id,
                        &plan.plan_id,
                        &plan.candidate.entry,
                        &path,
                    )?,
                    invocation_path: path.clone(),
                    scope_id: checkpoint.scope_id.clone(),
                    input: receipt_artifact(input_kind, &json!({}))?.reference,
                    region_path: Vec::new(),
                    next_step: 0,
                    locals: BTreeMap::new(),
                });
            path.push(cymule_core::InvocationPathSegment {
                site_id: workspace_invoke_site(index)?,
                scope_id: checkpoint.scope_id.clone(),
                region_path: Vec::new(),
            });
        }
        checkpoint.continuation.verify_wire()?;
        validate_continuation_plan_frames(plan, &checkpoint.continuation)?;
        checkpoint.continuation_digest =
            agent_workspace_continuation_digest(&checkpoint.continuation)?;
        checkpoint
            .source_continuation_digest
            .clone_from(&checkpoint.continuation_digest);
        Ok(())
    }

    #[test]
    fn workspace_receipt_keeps_large_verified_continuation_and_both_neighbors() -> DurableResult<()>
    {
        let plan = large_workspace_plan()?;
        let mut checkpoint = workspace_checkpoint()?;
        install_large_workspace_continuation(&mut checkpoint, &plan)?;
        install_workspace_effect(&mut checkpoint)?;
        let continuation_bytes = cymule_core::canonical_bytes(&checkpoint.continuation)?;
        assert!(continuation_bytes.len() <= cymule_durable_protocol::MAX_CONTINUATION_WIRE_BYTES);
        let receipt = CoupledCheckpointReceipt::new(CoupledCheckpoint::AgentWorkspace {
            checkpoint: Box::new(checkpoint),
        })?;
        let encoded = cymule_core::canonical_bytes(&receipt)?;
        assert!(encoded.len() > MAX_COUPLED_CHECKPOINT_RECEIPT_BYTES);
        // One Effect's paths are a verified source frame subset: four UTF-8
        // bytes per allowed identity scalar, at most 17 bytes per exact index,
        // and 64 JSON framing bytes per invocation segment. 64 KiB covers its
        // bounded non-path metadata; another 64 KiB covers both outboxes,
        // both leases, Clock evidence, and the complete receipt envelope.
        let effect_bound = 4 * cymule_durable_protocol::MAX_CONTINUATION_IDENTITY_SCALARS
            + 17 * cymule_durable_protocol::MAX_CONTINUATION_AGGREGATE_ITEMS
            + 64 * cymule_durable_protocol::MAX_FRAME_INVOCATION_DEPTH
            + 64 * 1024;
        let full_bound =
            cymule_durable_protocol::MAX_CONTINUATION_WIRE_BYTES + 2 * effect_bound + 64 * 1024;
        assert!(
            encoded.len() <= full_bound
                && full_bound <= MAX_AGENT_WORKSPACE_CHECKPOINT_RECEIPT_BYTES
        );
        let reopened: CoupledCheckpointReceipt = cymule_core::decode_json(&encoded)?;
        reopened.verify()?;
        assert_eq!(reopened, receipt);
        Ok(())
    }

    struct ReceiptCommandFixture {
        machine: Machine,
        receipt: crate::CancellationReceipt,
        entry: cymule_core::MachineCommandArchiveEntry,
        batch: cymule_core::MachineCommandBatchRecord,
    }

    fn receipt_artifact(kind: &str, value: &Value) -> DurableResult<ArtifactRecord> {
        let bytes = cymule_core::canonical_bytes(value)?;
        Ok(ArtifactRecord {
            reference: cymule_core::artifact_ref(kind, &bytes)?,
            bytes,
        })
    }

    fn receipt_command_fixture() -> DurableResult<ReceiptCommandFixture> {
        let run_id = "run:terminal-receipt";
        let plan = cymule_core::seal_plan(cymule_core::PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "terminal_receipt".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![cymule_core::Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Input,
                },
            }],
            metadata: BTreeMap::new(),
        })?;
        let input = receipt_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, &json!({}))?;
        let binding = receipt_artifact(cymule_core::EXECUTION_BINDING_ARTIFACT_KIND, &json!({}))?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            "start:terminal-receipt".to_owned(),
            vec![plan.clone()],
            vec![input.clone(), binding.clone()],
        )?;
        let mut machine = Machine::new();
        machine.insert_plan(plan.clone())?;
        machine.put_artifact(input.reference.kind.clone(), input.bytes)?;
        machine.put_artifact(binding.reference.kind.clone(), binding.bytes)?;
        let started = machine.submit(cymule_core::CommandEnvelope {
            command_version: cymule_core::COMMAND_VERSION.to_owned(),
            command_id: "start:terminal-receipt".to_owned(),
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition: None,
            command: cymule_core::Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.reference.artifact_id.clone(),
                input: input.reference,
                material_digest: material.material_digest().to_owned(),
                initial_attempt: cymule_core::InitialAttemptSpec {
                    attempt_id: content_id("test.receipt-identity/1", &"attempt")?,
                    continuation_id: content_id("test.receipt-identity/1", &"continuation")?,
                    occurrence_binding: binding.reference.artifact_id,
                    continuation_epoch: 0,
                    execution_fence: 1,
                },
            },
        })?;
        assert_eq!(started.status, cymule_core::CommandReceiptStatus::Applied);
        let command = crate::CancellationCommand {
            cancellation_id: "cancel:terminal-receipt".to_owned(),
            run_id: run_id.to_owned(),
            reason: json!({"reason": "terminal-audit"}),
        };
        let reason = receipt_artifact(crate::CANCELLATION_REASON_ARTIFACT_KIND, &command.reason)?;
        machine.put_artifact(reason.reference.kind.clone(), reason.bytes)?;
        let cancelled = machine.submit(cymule_core::CommandEnvelope {
            command_version: cymule_core::COMMAND_VERSION.to_owned(),
            command_id: command.cancellation_id.clone(),
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition: machine
                .projection()
                .runs
                .get(run_id)
                .map(cymule_core::RunProjection::precondition_token),
            command: cymule_core::Command::CancelRun {
                reason: reason.reference.clone(),
            },
        })?;
        assert_eq!(cancelled.status, cymule_core::CommandReceiptStatus::Applied);
        let receipt = crate::CancellationReceipt::new(command, reason.reference)?;
        let entry = machine
            .replay_entries()?
            .into_iter()
            .find(|entry| entry.command.envelope.command_id == receipt.command.cancellation_id)
            .ok_or_else(|| {
                DurableError::NotFound("test cancellation command is absent".to_owned())
            })?;
        let batch = machine
            .snapshot()
            .batches
            .into_iter()
            .find(|batch| batch.batch_id == entry.command.batch_id)
            .ok_or_else(|| {
                DurableError::NotFound("test cancellation batch is absent".to_owned())
            })?;
        Ok(ReceiptCommandFixture {
            machine,
            receipt,
            entry,
            batch,
        })
    }

    fn resolution_receipt_fixture()
    -> DurableResult<(crate::EffectResolutionReceipt, EffectDispatch)> {
        let result = receipt_artifact(EFFECT_RESULT_ARTIFACT_KIND, &json!({"result": "retained"}))?;
        let command = crate::EffectResolutionCommand {
            resolution_id: "resolve:terminal-receipt".to_owned(),
            run_id: "run:terminal-receipt".to_owned(),
            intent_id: content_id("test.receipt-identity/1", &"intent")?,
            execution_binding: receipt_artifact(
                cymule_runtime::EXECUTION_BINDING_VERSION,
                &json!({}),
            )?
            .reference,
            occurrence_binding: content_id("test.receipt-identity/1", &"effect-binding")?,
            claim_owner: "driver:original".to_owned(),
            claim_epoch: 7,
            resolution: cymule_core::ReconciliationResolution::ResolvedNotApplied,
            value: None,
        };
        let receipt = crate::EffectResolutionReceipt::new(
            command,
            cymule_core::ReconciliationResolution::ResolvedApplied,
            Some(json!({"result": "retained"})),
            Some(result.reference),
        )?;
        let dispatch = EffectDispatch {
            intent_id: receipt.command.intent_id.clone(),
            run_id: receipt.command.run_id.clone(),
            origin_plan_id: content_id("test.receipt-identity/1", &"plan")?,
            operation: "test.observe".to_owned(),
            input: receipt_artifact("test.input/1", &json!({}))?.reference,
            execution_binding: receipt.command.execution_binding.clone(),
            occurrence_binding: receipt.command.occurrence_binding.clone(),
            execution_availability: cymule_core::EffectExecutionAvailability::Available,
            reconciliation: cymule_core::ReconciliationState::Resolved,
            state: OutboxState::Applied,
            claim_epoch: receipt.command.claim_epoch,
            claim_owner: Some(receipt.command.claim_owner.clone()),
            result: receipt.result.clone(),
        };
        dispatch.verify_wire()?;
        Ok((receipt, dispatch))
    }

    #[test]
    fn cancellation_receipt_resealed_command_id_cannot_borrow_a_terminal_run() -> DurableResult<()>
    {
        let fixture = receipt_command_fixture()?;
        validate_cancellation_receipt_command(&fixture.receipt, &fixture.entry, &fixture.batch)?;
        let mut command = fixture.receipt.command.clone();
        command.cancellation_id = "cancel:renamed".to_owned();
        let crate::DurableBoundary::Cancelled { reason } = &fixture.receipt.boundary else {
            panic!("fixture cancellation has another boundary")
        };
        let renamed = crate::CancellationReceipt::new(command, reason.clone())?;
        assert!(matches!(
            validate_cancellation_receipt_command(&renamed, &fixture.entry, &fixture.batch),
            Err(DurableError::Integrity { code, .. }) if code == "terminal_receipt_command_mismatch"
        ));
        Ok(())
    }

    #[test]
    fn terminal_receipt_command_binds_its_exact_batch_artifacts() -> DurableResult<()> {
        let fixture = receipt_command_fixture()?;
        let other = receipt_artifact(
            crate::CANCELLATION_REASON_ARTIFACT_KIND,
            &json!({"reason": "other"}),
        )?;
        assert!(matches!(
            validate_terminal_receipt_command(
                &fixture.receipt.command.cancellation_id,
                &fixture.receipt.command.run_id,
                &fixture.entry.command.envelope.command,
                &[other.reference],
                &fixture.entry,
                &fixture.batch,
            ),
            Err(DurableError::Integrity { code, .. }) if code == "terminal_receipt_command_mismatch"
        ));
        Ok(())
    }

    #[test]
    fn cancellation_receipt_command_remains_valid_after_history_compaction() -> DurableResult<()> {
        let mut fixture = receipt_command_fixture()?;
        let compacted = fixture.machine.compact_event_history(0)?;
        assert!(fixture.machine.snapshot().events.is_empty());
        let entry = compacted
            .archive_segment
            .entries
            .iter()
            .find(|entry| {
                entry.command.envelope.command_id == fixture.receipt.command.cancellation_id
            })
            .ok_or_else(|| {
                DurableError::NotFound("compacted cancellation entry is absent".to_owned())
            })?;
        let batch = compacted
            .archive_segment
            .batches
            .iter()
            .find(|batch| batch.batch_id == entry.command.batch_id)
            .ok_or_else(|| {
                DurableError::NotFound("compacted cancellation batch is absent".to_owned())
            })?;
        validate_cancellation_receipt_command(&fixture.receipt, entry, batch)
    }

    #[test]
    fn cancellation_audit_requires_typed_receipt_and_rejects_duplicate_subjects()
    -> DurableResult<()> {
        let fixture = receipt_command_fixture()?;
        let mut state = DurableState::new(fixture.machine.snapshot());
        let record = JournalRecord::new(
            &fixture.receipt.command.cancellation_id,
            crate::RUN_CANCELLATION_RECEIPT_VERSION,
            serde_json::to_value(&fixture.receipt)?,
        )?;
        state
            .application_journals
            .entry("journal:run-cancellation-receipts".to_owned())
            .or_default()
            .append(record)?;
        assert!(matches!(
            state.validate_terminal_receipts(&fixture.machine),
            Err(DurableError::Integrity { code, .. }) if code == "cancelled_run_receipt_missing"
        ));
        state.cancellation_receipts.insert(
            fixture.receipt.command.cancellation_id.clone(),
            fixture.receipt.clone(),
        );
        state.validate_terminal_receipts(&fixture.machine)?;
        let mut command = fixture.receipt.command.clone();
        command.cancellation_id = "cancel:duplicate-subject".to_owned();
        let crate::DurableBoundary::Cancelled { reason } = &fixture.receipt.boundary else {
            panic!("fixture cancellation has another boundary")
        };
        let duplicate = crate::CancellationReceipt::new(command, reason.clone())?;
        state
            .cancellation_receipts
            .insert(duplicate.command.cancellation_id.clone(), duplicate);
        assert!(matches!(
            state.validate_terminal_receipts(&fixture.machine),
            Err(DurableError::Integrity { code, .. }) if code == "run_cancellation_receipt_not_closed"
        ));
        Ok(())
    }

    #[test]
    fn effect_resolution_receipt_binds_original_claim_and_resolved_axis() -> DurableResult<()> {
        let (receipt, dispatch) = resolution_receipt_fixture()?;
        validate_effect_resolution_receipt_closure(&receipt, &dispatch)?;
        let scenarios: [fn(&mut EffectDispatch); 4] = [
            |value| value.claim_epoch += 1,
            |value| value.claim_owner = Some("driver:other".to_owned()),
            |value| value.run_id = "run:other".to_owned(),
            |value| value.reconciliation = cymule_core::ReconciliationState::NotRequired,
        ];
        for alter in scenarios {
            let mut candidate = dispatch.clone();
            alter(&mut candidate);
            candidate.verify_wire()?;
            assert!(matches!(
                validate_effect_resolution_receipt_closure(&receipt, &candidate),
                Err(DurableError::Integrity { code, .. }) if code == "effect_resolution_receipt_not_closed"
            ));
        }
        Ok(())
    }

    #[test]
    fn effect_resolution_receipt_preserves_provider_actual_outcome() -> DurableResult<()> {
        let (receipt, dispatch) = resolution_receipt_fixture()?;
        assert_ne!(receipt.command.resolution, receipt.actual_resolution);
        assert!(receipt.command.value.is_none());
        assert!(receipt.actual_value.is_some());
        validate_effect_resolution_receipt_closure(&receipt, &dispatch)
    }

    fn journal_record(record_id: &str, value: u64) -> JournalRecord {
        JournalRecord::new(
            record_id,
            "test.application-journal/1",
            json!({"value": value}),
        )
        .expect("test journal record is valid")
    }

    fn prefix_from_records(records: &[JournalRecord]) -> ApplicationJournalPrefix {
        let first = records.first().expect("test prefix is non-empty");
        let last = records.last().expect("test prefix is non-empty");
        let ordered_root =
            crate::state_root::application_journal_ordered_root_from_records(records)
                .expect("StateRoot authenticates the test prefix");
        ApplicationJournalPrefix::from_state_log_evidence(
            u64::try_from(records.len()).expect("test prefix length fits u64"),
            first,
            last,
            ordered_root,
        )
        .expect("authenticated StateRoot evidence constructs a prefix")
    }

    fn replacement_receipt() -> ApplicationJournalPrefixReplacementReceipt {
        let source = [
            journal_record("record:source:1", 1),
            journal_record("record:source:2", 2),
        ];
        let replacement = journal_record("record:base", 3);
        let retained = journal_record("record:retained", 4);
        ApplicationJournalPrefixReplacementReceipt::new(
            ApplicationJournalPrefixReplacement {
                replacement_id: "replacement:1".to_owned(),
                journal_id: "journal:1".to_owned(),
                parent_replacement_id: Some("replacement:0".to_owned()),
                expected_prefix: prefix_from_records(&source),
                replacement: vec![replacement.clone()],
            },
            prefix_from_records(&[replacement, retained]),
        )
        .expect("test receipt seals")
    }

    #[test]
    fn application_journal_preserves_sequence_wire_and_read_contract() {
        let source = vec![journal_record("record:1", 1), journal_record("record:2", 2)];
        let expected_wire = serde_json::to_value(&source).expect("record sequence serializes");
        let mut journal: ApplicationJournal =
            serde_json::from_value(expected_wire.clone()).expect("sequence wire deserializes");

        assert_eq!(journal.len(), 2);
        assert!(!journal.is_empty());
        assert_eq!(journal.front(), source.first());
        assert_eq!(journal.back(), source.last());
        assert_eq!(journal.last(), source.last());
        assert_eq!(journal.get(1), source.get(1));
        assert_eq!(&journal[0], &source[0]);
        assert_eq!(
            journal.iter().collect::<Vec<_>>(),
            source.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            (&journal).into_iter().collect::<Vec<_>>(),
            source.iter().collect::<Vec<_>>()
        );
        assert_eq!(journal.to_vec(), source);
        assert_eq!(
            serde_json::to_value(&journal).expect("journal serializes"),
            expected_wire,
            "the newtype must retain the existing JSON-sequence wire"
        );

        let appended = journal_record("record:3", 3);
        journal
            .append(appended.clone())
            .expect("unique record appends");
        assert_eq!(journal.back(), Some(&appended));

        assert!(
            serde_json::from_value::<ApplicationJournal>(serde_json::json!([source[0], source[0]]))
                .is_err(),
            "duplicate active identities must fail while decoding the wire sequence"
        );
    }

    #[test]
    fn terminal_wait_origin_cannot_borrow_an_unrelated_admitted_plan() -> DurableResult<()> {
        let mut fixture = receipt_command_fixture()?;
        let run_id = fixture.receipt.command.run_id.clone();
        let current_plan = fixture.machine.projection().runs[&run_id]
            .current_plan
            .clone();
        let mut candidate = fixture
            .machine
            .plan(&current_plan)
            .ok_or_else(|| DurableError::NotFound("fixture Plan is missing".to_owned()))?
            .candidate
            .clone();
        "foreign_wait_origin".clone_into(&mut candidate.name);
        candidate.definitions[0].body.steps.push(cymule_core::Step {
            id: "foreign.wait".to_owned(),
            operation: Operation::Wait {
                wait: cymule_core::WaitSpec::Signal {
                    key: "signal:foreign".to_owned(),
                    consume_once: false,
                },
                bind: None,
            },
        });
        let foreign = cymule_core::seal_plan(candidate)?;
        fixture.machine.insert_plan(foreign.clone())?;
        let owner = WaitOwner {
            invocation_id: "invocation:foreign".to_owned(),
            definition_id: "main".to_owned(),
            site_id: "foreign.wait".to_owned(),
            region_path: Vec::new(),
            step_index: 0,
            bind: None,
        };
        let wait = WaitCondition {
            wait_id: derive_wait_id(
                &run_id,
                &foreign.plan_id,
                &owner.invocation_id,
                &owner.site_id,
            )?,
            run_id,
            kind: WaitKind::Signal {
                key: "signal:foreign".to_owned(),
            },
            consume_once: false,
            owner,
            state: WaitState::Cancelled,
            result: None,
        };
        wait.verify_wire()?;
        assert!(
            matches!(retained_wait_origin_plan(&fixture.machine, &current_plan, &wait),
            Err(DurableError::Validation(message)) if message.contains("admitted Plan lineage"))
        );
        Ok(())
    }

    #[test]
    fn history_compaction_parent_is_required_nullable_on_wire() -> DurableResult<()> {
        let mut fixture = receipt_command_fixture()?;
        let compacted = fixture.machine.compact_event_history(0)?;
        let receipt = HistoryCompactionReceipt {
            compaction_version: HISTORY_COMPACTION_VERSION.to_owned(),
            compaction_id: "compaction:wire".to_owned(),
            parent_compaction: None,
            kind: HistoryCompactionKind::EventPrefix,
            source_revision: content_id("test.history-receipt/1", &"source")?,
            requested_suffix: 0,
            result: MachineCompactionSummary::from(&compacted),
        };
        receipt.verify()?;
        let Value::Object(mut wire) = serde_json::to_value(&receipt)? else {
            panic!("history compaction receipt is not an object")
        };
        assert_eq!(wire.get("parent_compaction"), Some(&Value::Null));
        let decoded: HistoryCompactionReceipt =
            serde_json::from_value(Value::Object(wire.clone()))?;
        assert_eq!(decoded, receipt);
        wire.remove("parent_compaction");
        assert!(serde_json::from_value::<HistoryCompactionReceipt>(Value::Object(wire)).is_err());
        Ok(())
    }

    #[test]
    fn durable_state_validates_only_the_active_journal_projection() {
        let record = journal_record("record:active", 1);
        let mut state = DurableState::new(Machine::new().snapshot());
        state.application_journals.insert(
            "journal:1".to_owned(),
            ApplicationJournal::try_from_records(vec![record.clone()])
                .expect("unique journal seals"),
        );
        state
            .validate()
            .expect("active journal validation does not require an all-ever cache");

        assert!(matches!(
            ApplicationJournal::try_from_records(vec![record.clone(), record]),
            Err(DurableError::Validation(message)) if message.contains("repeats active record")
        ));
    }

    fn assert_receipt_field_is_bound(
        receipt: &ApplicationJournalPrefixReplacementReceipt,
        mutate: impl FnOnce(&mut ApplicationJournalPrefixReplacementReceipt),
    ) {
        let replacement_digest =
            canonical_digest(&receipt.replacement).expect("replacement digest derives");
        let original_preimage = canonical_digest(&(replacement_digest, &receipt.result))
            .expect("receipt preimage digest derives");
        let mut tampered = receipt.clone();
        mutate(&mut tampered);
        let tampered_replacement_digest =
            canonical_digest(&tampered.replacement).expect("tampered replacement digest derives");
        assert_ne!(
            canonical_digest(&(tampered_replacement_digest, &tampered.result))
                .expect("tampered receipt preimage digest derives"),
            original_preimage,
            "the receipt preimage must bind every transition field"
        );
        assert!(
            tampered.verify().is_err(),
            "a receipt cannot retain its old identity after a bound field changes"
        );
    }

    fn assert_authority_field_is_bound(
        authority: &ApplicationJournalPrefixReplacementAuthority,
        mutate: impl FnOnce(&mut ApplicationJournalPrefixReplacementAuthority),
    ) {
        let original_digest = canonical_digest(authority).expect("authority digest derives");
        let mut tampered = authority.clone();
        mutate(&mut tampered);
        assert_ne!(
            canonical_digest(&tampered).expect("tampered authority digest derives"),
            original_digest,
            "StateRoot value identity must bind every cumulative-authority field"
        );
        assert!(
            tampered.verify().is_err(),
            "receipt ID must reject changed command or result evidence"
        );
    }

    #[test]
    fn application_journal_prefix_has_one_strict_terminal_shape() {
        let prefix =
            prefix_from_records(&[journal_record("record:1", 1), journal_record("record:2", 2)]);
        prefix.verify().expect("terminal prefix verifies");

        let Value::Object(fields) = serde_json::to_value(&prefix).expect("prefix serializes")
        else {
            panic!("prefix must serialize as an object");
        };
        assert_eq!(
            fields.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "first",
                "last",
                "ordered_root",
                "prefix_version",
                "record_count",
            ])
        );

        for required in [
            "prefix_version",
            "record_count",
            "first",
            "last",
            "ordered_root",
        ] {
            let mut missing = fields.clone();
            missing.remove(required);
            assert!(
                serde_json::from_value::<ApplicationJournalPrefix>(Value::Object(missing)).is_err(),
                "{required} must be required"
            );
        }

        let mut legacy = fields.clone();
        legacy.insert("ordered_digest".to_owned(), Value::String("0".repeat(64)));
        assert!(
            serde_json::from_value::<ApplicationJournalPrefix>(Value::Object(legacy)).is_err(),
            "the superseded ordered_digest shape must fail closed"
        );

        let mut old_version = prefix;
        old_version.prefix_version = "cymule.application-journal-prefix/0".to_owned();
        assert!(old_version.verify().is_err());
    }

    #[test]
    fn application_journal_prefix_requires_consistent_authenticated_endpoints() {
        let first = journal_record("record:1", 1);
        let last = journal_record("record:2", 2);
        let root = crate::state_root::application_journal_ordered_root_from_records(&[
            first.clone(),
            last.clone(),
        ])
        .expect("StateRoot authenticates the test records");

        assert!(
            ApplicationJournalPrefix::from_state_log_evidence(0, &first, &last, root.clone(),)
                .is_err()
        );
        assert!(
            ApplicationJournalPrefix::from_state_log_evidence(1, &first, &last, root.clone(),)
                .is_err()
        );
        assert!(
            ApplicationJournalPrefix::from_state_log_evidence(2, &first, &first, root).is_err()
        );
    }

    #[test]
    fn replacement_receipt_binds_source_journal_and_both_log_roots() {
        let receipt = replacement_receipt();
        receipt.verify().expect("terminal receipt verifies");
        assert_eq!(
            receipt.receipt_version,
            APPLICATION_JOURNAL_PREFIX_REPLACEMENT_RECEIPT_VERSION
        );

        let alternate = journal_record("record:alternate", 9);
        let alternate_ref = ApplicationJournalRecordRef::from_record(&alternate);
        let alternate_root = prefix_from_records(std::slice::from_ref(&alternate)).ordered_root;

        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.replacement_id = "replacement:other".to_owned();
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.journal_id = "journal:other".to_owned();
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.parent_replacement_id = Some("replacement:other-parent".to_owned());
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.replacement = vec![alternate.clone()];
        });

        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.expected_prefix.prefix_version =
                "cymule.application-journal-prefix/0".to_owned();
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.expected_prefix.record_count += 1;
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.expected_prefix.first = alternate_ref.clone();
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.expected_prefix.last = alternate_ref.clone();
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.replacement.expected_prefix.ordered_root = alternate_root.clone();
        });

        assert_receipt_field_is_bound(&receipt, |value| {
            value.result.prefix_version = "cymule.application-journal-prefix/0".to_owned();
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.result.record_count += 1;
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.result.first = alternate_ref.clone();
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.result.last = alternate_ref;
        });
        assert_receipt_field_is_bound(&receipt, |value| {
            value.result.ordered_root = alternate_root;
        });
    }

    #[test]
    fn replacement_receipt_rejects_superseded_or_open_shapes() {
        let receipt = replacement_receipt();
        let mut value = serde_json::to_value(&receipt).expect("receipt serializes");
        value["receipt_version"] =
            Value::String("cymule.application-journal-prefix-replacement-receipt/1".to_owned());
        let old: ApplicationJournalPrefixReplacementReceipt =
            serde_json::from_value(value).expect("old version retains the same JSON shape");
        assert!(old.verify().is_err());

        let mut missing_result = serde_json::to_value(&receipt).expect("receipt serializes");
        missing_result
            .as_object_mut()
            .expect("receipt is an object")
            .remove("result");
        assert!(
            serde_json::from_value::<ApplicationJournalPrefixReplacementReceipt>(missing_result)
                .is_err(),
            "the v1 result-free receipt shape must fail closed"
        );

        let mut open_shape = serde_json::to_value(&receipt).expect("receipt serializes");
        open_shape["source_ordered_digest"] = Value::String("0".repeat(64));
        assert!(
            serde_json::from_value::<ApplicationJournalPrefixReplacementReceipt>(open_shape)
                .is_err(),
            "unknown legacy receipt fields must fail closed"
        );
    }

    #[test]
    fn replacement_authority_reconstructs_lost_ack_receipt_from_constant_evidence() {
        let receipt = replacement_receipt();
        let authority = ApplicationJournalPrefixReplacementAuthority::new(&receipt)
            .expect("terminal authority constructs");
        authority.verify().expect("terminal authority verifies");
        assert_eq!(
            authority.authority_version,
            APPLICATION_JOURNAL_PREFIX_REPLACEMENT_AUTHORITY_VERSION
        );
        assert_eq!(authority.result, receipt.result);

        let reconstructed = ApplicationJournalPrefixReplacementReceipt::new(
            receipt.replacement.clone(),
            authority.result.clone(),
        )
        .expect("caller command and retained result reconstruct the receipt");
        assert_eq!(reconstructed, receipt);
        assert!(
            authority
                .matches(&reconstructed)
                .expect("reconstructed receipt comparison succeeds")
        );

        let alternate_result = prefix_from_records(&[
            journal_record("record:alternate:1", 10),
            journal_record("record:alternate:2", 11),
        ]);
        let alternate_receipt = ApplicationJournalPrefixReplacementReceipt::new(
            receipt.replacement.clone(),
            alternate_result.clone(),
        )
        .expect_err("an unrelated result cannot satisfy replacement-prefix semantics");
        assert!(matches!(alternate_receipt, DurableError::Validation(_)));

        assert_authority_field_is_bound(&authority, |value| {
            value.replacement_digest = "0".repeat(64);
        });
        assert_authority_field_is_bound(&authority, |value| {
            value.receipt_id =
                content_id("test.receipt/1", &"other").expect("alternate receipt identity derives");
        });
        assert_authority_field_is_bound(&authority, |value| {
            value.result = alternate_result;
        });
    }

    #[test]
    fn replacement_authority_rejects_old_or_incomplete_shapes() {
        let receipt = replacement_receipt();
        let authority = ApplicationJournalPrefixReplacementAuthority::new(&receipt)
            .expect("terminal authority constructs");
        let Value::Object(fields) = serde_json::to_value(&authority).expect("authority serializes")
        else {
            panic!("authority must serialize as an object");
        };
        assert_eq!(
            fields.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "authority_version",
                "journal_id",
                "receipt_id",
                "replacement_digest",
                "replacement_id",
                "result",
            ])
        );

        for required in ["authority_version", "result"] {
            let mut missing = fields.clone();
            missing.remove(required);
            assert!(
                serde_json::from_value::<ApplicationJournalPrefixReplacementAuthority>(
                    Value::Object(missing)
                )
                .is_err(),
                "the pre-v2 authority shape without {required} must fail closed"
            );
        }

        let mut old_version = authority;
        old_version.authority_version =
            "cymule.application-journal-prefix-replacement-authority/1".to_owned();
        assert!(old_version.verify().is_err());
    }

    fn assert_required_nullable_fields<T>(value: &T, required: &[&str])
    where
        T: Serialize + serde::de::DeserializeOwned,
    {
        let Value::Object(fields) = serde_json::to_value(value).expect("value serializes") else {
            panic!("required-nullable value must serialize as an object");
        };
        for field in required {
            assert_eq!(
                fields.get(*field),
                Some(&Value::Null),
                "{field} must serialize explicit null"
            );
            serde_json::from_value::<T>(Value::Object(fields.clone()))
                .expect("explicit null decodes");
            let mut missing = fields.clone();
            missing.remove(*field);
            assert!(
                serde_json::from_value::<T>(Value::Object(missing)).is_err(),
                "missing required-nullable field {field} must fail closed"
            );
        }
    }

    #[test]
    fn durable_optional_references_are_required_nullable_on_wire() {
        let artifact = cymule_core::artifact_ref("test.external-reference/1", b"null")
            .expect("test Artifact reference derives");
        let owner = WaitOwner {
            invocation_id: "invocation:required-nullable".to_owned(),
            definition_id: "definition:required-nullable".to_owned(),
            site_id: "site:required-nullable".to_owned(),
            region_path: Vec::new(),
            step_index: 0,
            bind: None,
        };
        assert_required_nullable_fields(&owner, &["bind"]);
        let wait = WaitCondition {
            wait_id: "wait:required-nullable".to_owned(),
            run_id: "run:required-nullable".to_owned(),
            kind: WaitKind::Signal {
                key: "signal:required-nullable".to_owned(),
            },
            consume_once: true,
            owner,
            state: WaitState::Pending,
            result: None,
        };
        assert_required_nullable_fields(&wait, &["result"]);
        let dispatch = EffectDispatch {
            intent_id: content_id("test.effect-intent/1", &"required-nullable")
                .expect("test Effect intent derives"),
            run_id: "run:required-nullable".to_owned(),
            origin_plan_id: "plan:required-nullable".to_owned(),
            operation: "effect:required-nullable".to_owned(),
            input: artifact.clone(),
            execution_binding: artifact.clone(),
            occurrence_binding: "binding:required-nullable".to_owned(),
            execution_availability: cymule_core::EffectExecutionAvailability::Available,
            reconciliation: cymule_core::ReconciliationState::NotRequired,
            state: OutboxState::Pending,
            claim_epoch: 0,
            claim_owner: None,
            result: None,
        };
        assert_required_nullable_fields(&dispatch, &["claim_owner", "result"]);
        let continuation = Continuation {
            continuation_version: cymule_durable_protocol::CONTINUATION_STATE_VERSION.to_owned(),
            run_id: "run:required-nullable".to_owned(),
            plan_id: "plan:required-nullable".to_owned(),
            binding_context: artifact.artifact_id.clone(),
            frames: Vec::new(),
            state: None,
            wait_set: BTreeSet::new(),
            scope_stack: Vec::new(),
            epoch: 0,
            execution_fence: 0,
            execution_claim: None,
            status: ContinuationStatus::Ready,
        };
        assert_required_nullable_fields(&continuation, &["state", "execution_claim"]);
        let occurrence = ComponentOccurrence {
            occurrence_version: COMPONENT_OCCURRENCE_VERSION.to_owned(),
            occurrence_id: "occurrence:required-nullable".to_owned(),
            run_id: "run:required-nullable".to_owned(),
            plan_id: "plan:required-nullable".to_owned(),
            binding_context: artifact.artifact_id.clone(),
            invocation_id: "invocation:required-nullable".to_owned(),
            invocation_path: Vec::new(),
            definition_id: "definition:required-nullable".to_owned(),
            region_path: Vec::new(),
            site_id: "site:required-nullable".to_owned(),
            step_index: 0,
            component: "component:required-nullable".to_owned(),
            input: artifact.clone(),
            outcome: None,
            occurrence_binding: content_id("test.occurrence-binding/1", &"required-nullable")
                .expect("test occurrence binding derives"),
            implementation_revision: "revision:required-nullable".to_owned(),
            attempt_count: 1,
            latest_attempt_id: content_id("test.operation-attempt/1", &"required-nullable")
                .expect("test Attempt derives"),
            continuation_digest: None,
            state: ComponentOccurrenceState::Pending,
        };
        assert_required_nullable_fields(&occurrence, &["outcome", "continuation_digest"]);
        let attempt = OperationAttempt {
            attempt_version: OPERATION_ATTEMPT_VERSION.to_owned(),
            attempt_id: "attempt:required-nullable".to_owned(),
            occurrence_id: occurrence.occurrence_id,
            run_id: "run:required-nullable".to_owned(),
            attempt_ordinal: 1,
            previous_attempt_id: None,
            continuation_attempt_id: "continuation-attempt:required-nullable".to_owned(),
            execution_claim_owner: "owner:required-nullable".to_owned(),
            execution_claim_fence: 1,
            operation_occurrence_binding: "binding:required-nullable".to_owned(),
            transport_request_id: "transport:required-nullable".to_owned(),
            state: OperationAttemptState::Running,
            outcome: None,
        };
        assert_required_nullable_fields(&attempt, &["previous_attempt_id", "outcome"]);
    }
}
