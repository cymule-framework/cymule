use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::sha256_bytes;
use crate::{CoreError, Result, canonical_digest, content_id};

/// Semantic specification version.
pub const SEMANTIC_VERSION: &str = "cymule.semantic/1";
/// Canonical event version.
pub const EVENT_VERSION: &str = "cymule.event/1";
/// Public command version.
pub const COMMAND_VERSION: &str = "cymule.command/1";
/// Stable root scope identifier within every Run.
pub const ROOT_SCOPE_ID: &str = "scope:root";

/// Immutable artifact reference.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    /// Content-addressed identity.
    pub artifact_id: String,
    /// Stable artifact type.
    pub kind: String,
}

/// Immutable artifact bytes held by the embedded store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRecord {
    /// Reference derived from type and bytes.
    pub reference: ArtifactRef,
    /// Immutable bytes.
    pub bytes: Vec<u8>,
}

/// Derive the canonical content reference for immutable typed bytes.
///
/// This is the sole implementation of the `cymule.artifact/1` identity
/// preimage. Stores and typed codec layers must call it rather than duplicate
/// the domain separator or framing.
pub fn artifact_ref(kind: impl Into<String>, bytes: &[u8]) -> ArtifactRef {
    let kind = kind.into();
    let mut preimage = Vec::with_capacity(kind.len() + bytes.len() + 20);
    preimage.extend_from_slice(b"cymule.artifact/1\0");
    preimage.extend_from_slice(kind.as_bytes());
    preimage.push(0);
    preimage.extend_from_slice(bytes);
    ArtifactRef {
        artifact_id: format!("sha256:{}", sha256_bytes(&preimage)),
        kind,
    }
}

/// A preconditioned, idempotent command proposal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandEnvelope {
    /// Command protocol version.
    pub command_version: String,
    /// Caller-generated idempotency identity.
    pub command_id: String,
    /// Authenticated actor reference. The embedded profile treats it as opaque.
    pub actor: String,
    /// Target Run identity.
    pub run_id: String,
    /// Token from the caller's current Run view.
    #[serde(default)]
    pub expected_precondition: Option<String>,
    /// Typed semantic proposal.
    pub command: Command,
}

/// Commands admitted by the semantic kernel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    /// Start a new Run under an immutable plan and future-default binding context.
    StartRun {
        /// Sealed plan ID.
        plan_id: String,
        /// Default realization context for future occurrences.
        binding_context: String,
    },
    /// Begin a fenced Attempt.
    BeginAttempt {
        /// Attempt identity.
        attempt_id: String,
        /// Continuation identity.
        continuation_id: String,
        /// Occurrence-level immutable binding.
        occurrence_binding: String,
        /// Expected continuation epoch.
        epoch: u64,
    },
    /// Yield the active Attempt at a safe point.
    YieldAttempt {
        /// Attempt identity.
        attempt_id: String,
        /// Expected continuation epoch.
        epoch: u64,
    },
    /// Advance the Run epoch and fence prior attempts.
    AdvanceEpoch,
    /// Open a nested state/evidence scope.
    OpenScope {
        /// New scope identity.
        scope_id: String,
        /// Existing parent scope.
        parent_scope: String,
    },
    /// Propose a structurally identified external effect.
    ProposeEffect {
        /// Owning open scope.
        scope_id: String,
        /// Invocation identity.
        invocation_id: String,
        /// Stable IR effect site.
        site_id: String,
        /// Intentional occurrence key.
        occurrence: String,
        /// Abstract effect operation.
        operation: String,
        /// Canonical argument artifact.
        args: ArtifactRef,
        /// Immutable occurrence binding.
        occurrence_binding: String,
    },
    /// Advance one axis of an existing effect.
    TransitionEffect {
        /// Structural intent identity.
        intent_id: String,
        /// Legal next transition.
        transition: EffectTransition,
    },
    /// Commit internal scope state and transfer unresolved effect obligations.
    CommitScope {
        /// Scope identity.
        scope_id: String,
    },
    /// Abort a scope before release.
    AbortScope {
        /// Scope identity.
        scope_id: String,
    },
    /// Change the realization default for future occurrences.
    UpdateBinding {
        /// New immutable Binding Context reference.
        binding_context: String,
    },
    /// Append an independent immutable fact for causal conformance tests.
    RecordFact {
        /// Stable logical fact key.
        key: String,
        /// Immutable value digest or reference.
        value: String,
    },
    /// Finish a Run after required obligations have settled.
    CompleteRun {
        /// Optional typed Result artifact.
        #[serde(default)]
        result: Option<ArtifactRef>,
    },
}

/// Legal effect transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum EffectTransition {
    /// Preparation completed.
    Prepare,
    /// Governing policy authorized release.
    AuthorizeRelease,
    /// External dispatch began.
    StartDispatch,
    /// Record the best authoritative world observation currently available.
    Observe(WorldOutcome),
    /// Reconcile an earlier unknown outcome.
    Reconcile(ReconciliationResolution),
}

/// A canonical event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    /// Content-addressed event identity.
    pub event_id: String,
    /// Event schema version.
    pub event_version: String,
    /// Admitted command identity.
    pub command_id: String,
    /// Canonical command semantics digest.
    pub command_hash: String,
    /// Run identity.
    pub run_id: String,
    /// Explicit causal parents.
    pub parents: Vec<String>,
    /// Logical point/predicate reads.
    pub reads: BTreeSet<String>,
    /// Logical writes.
    pub writes: BTreeSet<String>,
    /// Optional non-monotone coordination domain.
    #[serde(default)]
    pub coordination_key: Option<String>,
    /// Trusted semantic transition.
    pub payload: EventPayload,
}

#[derive(Serialize)]
struct EventPreimage<'a> {
    event_version: &'a str,
    command_id: &'a str,
    command_hash: &'a str,
    run_id: &'a str,
    parents: &'a [String],
    reads: &'a BTreeSet<String>,
    writes: &'a BTreeSet<String>,
    coordination_key: &'a Option<String>,
    payload: &'a EventPayload,
}

impl Event {
    /// Construct and content-address a trusted event.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_id: String,
        command_hash: String,
        run_id: String,
        mut parents: Vec<String>,
        reads: BTreeSet<String>,
        writes: BTreeSet<String>,
        coordination_key: Option<String>,
        payload: EventPayload,
    ) -> Result<Self> {
        parents.sort();
        parents.dedup();
        let preimage = EventPreimage {
            event_version: EVENT_VERSION,
            command_id: &command_id,
            command_hash: &command_hash,
            run_id: &run_id,
            parents: &parents,
            reads: &reads,
            writes: &writes,
            coordination_key: &coordination_key,
            payload: &payload,
        };
        let event_id = content_id("cymule.event/1", &preimage)?;
        Ok(Self {
            event_id,
            event_version: EVENT_VERSION.to_owned(),
            command_id,
            command_hash,
            run_id,
            parents,
            reads,
            writes,
            coordination_key,
            payload,
        })
    }

    /// Verify event schema and content identity.
    pub fn verify(&self) -> Result<()> {
        if self.event_version != EVENT_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported event version {:?}",
                self.event_version
            )));
        }
        let preimage = EventPreimage {
            event_version: &self.event_version,
            command_id: &self.command_id,
            command_hash: &self.command_hash,
            run_id: &self.run_id,
            parents: &self.parents,
            reads: &self.reads,
            writes: &self.writes,
            coordination_key: &self.coordination_key,
            payload: &self.payload,
        };
        let expected = content_id("cymule.event/1", &preimage)?;
        if expected != self.event_id {
            return Err(CoreError::IdentityMismatch(format!(
                "event ID {} does not match {expected}",
                self.event_id
            )));
        }
        Ok(())
    }
}

/// Canonical transition payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventPayload {
    /// A Run became canonical.
    RunStarted {
        /// Plan identity.
        plan_id: String,
        /// Future-default realization context.
        binding_context: String,
    },
    /// An Attempt received a fenced lease.
    AttemptStarted {
        /// Attempt identity.
        attempt_id: String,
        /// Continuation identity.
        continuation_id: String,
        /// Pinned occurrence binding.
        occurrence_binding: String,
        /// Pinned epoch.
        epoch: u64,
    },
    /// An Attempt yielded at a safe point.
    AttemptYielded {
        /// Attempt identity.
        attempt_id: String,
        /// Pinned epoch.
        epoch: u64,
    },
    /// The Run epoch advanced.
    EpochAdvanced {
        /// New epoch.
        epoch: u64,
    },
    /// A nested scope opened.
    ScopeOpened {
        /// Scope identity.
        scope_id: String,
        /// Parent scope identity.
        parent_scope: String,
    },
    /// An effect was admitted with an immutable occurrence binding.
    EffectProposed {
        /// Structural intent identity.
        intent_id: String,
        /// Owning scope.
        scope_id: String,
        /// Abstract operation ID.
        operation: String,
        /// Whether it mutates the world.
        mutating: bool,
        /// Canonical argument artifact.
        args: ArtifactRef,
        /// Pinned occurrence binding.
        occurrence_binding: String,
    },
    /// One effect state axis advanced.
    EffectTransitioned {
        /// Structural intent identity.
        intent_id: String,
        /// Transition.
        transition: EffectTransition,
    },
    /// Internal scope state committed and obligations transferred.
    ScopeCommitted {
        /// Scope identity.
        scope_id: String,
        /// Deterministically derived obligations.
        obligations: Vec<ObligationProjection>,
    },
    /// A scope aborted before effect release.
    ScopeAborted {
        /// Scope identity.
        scope_id: String,
    },
    /// Future occurrences use a new default binding context.
    BindingUpdated {
        /// Previous context.
        previous: String,
        /// New context.
        current: String,
    },
    /// An append-only fact was recorded.
    FactRecorded {
        /// Logical fact key.
        key: String,
        /// Immutable value.
        value: String,
    },
    /// A Run entered a terminal completed state.
    RunCompleted {
        /// Optional Result artifact.
        result: Option<ArtifactRef>,
    },
}

/// Public command outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandReceiptStatus {
    /// A canonical event was admitted.
    Applied,
    /// The caller's precondition was stale.
    Conflict,
}

/// Durable command receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandReceipt {
    /// Command identity.
    pub command_id: String,
    /// Stable status.
    pub status: CommandReceiptStatus,
    /// Admitted event when applied.
    #[serde(default)]
    pub event_id: Option<String>,
    /// Stable structured error code for a conflict.
    #[serde(default)]
    pub error_code: Option<String>,
    /// Human-readable explanation.
    #[serde(default)]
    pub message: Option<String>,
    /// Caller-provided token.
    #[serde(default)]
    pub observed_precondition: Option<String>,
    /// Current token after application or conflict.
    #[serde(default)]
    pub current_precondition: Option<String>,
}

/// Rebuildable full projection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    /// Run projections.
    pub runs: BTreeMap<String, RunProjection>,
    /// Append-only facts used by explainability and conformance.
    pub facts: BTreeMap<String, String>,
}

/// Rebuildable Run projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunProjection {
    /// Run identity.
    pub run_id: String,
    /// Initial immutable plan.
    pub initial_plan: String,
    /// Current semantic plan. Plan migration is not implemented in profile 1.
    pub current_plan: String,
    /// Initial realization default.
    pub initial_binding_context: String,
    /// Future-occurrence realization default.
    pub current_binding_context: String,
    /// Fencing epoch.
    pub epoch: u64,
    /// Public Run status.
    pub status: RunStatus,
    /// Scope projections.
    pub scopes: BTreeMap<String, ScopeProjection>,
    /// Effect projections.
    pub effects: BTreeMap<String, EffectProjection>,
    /// Outstanding or settled obligations.
    pub obligations: BTreeMap<String, ObligationProjection>,
    /// Attempt projections.
    pub attempts: BTreeMap<String, AttemptProjection>,
    /// Optional terminal Result.
    pub result: Option<ArtifactRef>,
    /// Last applied event in the embedded linear frontier.
    pub last_event: String,
}

impl RunProjection {
    /// Current stale-action protection token.
    pub fn precondition_token(&self) -> String {
        format!("pre:{}:{}", self.epoch, self.last_event)
    }
}

/// Public Run status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// New work may be admitted.
    Active,
    /// Waiting for a durable condition.
    Waiting,
    /// Terminal successful state.
    Completed,
    /// Terminal failure state.
    Failed,
    /// Terminal cancellation state.
    Cancelled,
}

/// Scope projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeProjection {
    /// Scope identity.
    pub scope_id: String,
    /// Optional parent.
    pub parent_scope: Option<String>,
    /// Scope lifecycle state.
    pub status: ScopeStatus,
    /// Effects admitted in the scope.
    pub intents: BTreeSet<String>,
}

/// Scope lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeStatus {
    /// Scope accepts work.
    Open,
    /// Internal state/evidence was committed and the scope is closed.
    ClosedCommitted,
    /// Overlay and unreleased mutation were discarded.
    ClosedAborted,
}

/// Effect control phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectPhase {
    /// Intent was admitted.
    Admitted,
    /// Payload and evidence were prepared.
    Prepared,
    /// Governing policy authorized release.
    ReleaseAuthorized,
    /// External dispatch began.
    DispatchStarted,
    /// Unreleased effect was cancelled by scope abort.
    CancelledBeforeRelease,
}

/// Observed external-world outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorldOutcome {
    /// No external-world observation exists yet.
    Unobserved,
    /// The external action occurred.
    Applied,
    /// The external action did not occur.
    NotApplied,
    /// Dispatch occurred but the outcome cannot currently be determined.
    Unknown,
}

/// Reconciliation result transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationResolution {
    /// Observation proved the action occurred.
    ResolvedApplied,
    /// Observation proved the action did not occur.
    ResolvedNotApplied,
    /// The outcome remains unknown and may be queried again.
    StillUnknown,
    /// Automatic resolution is unavailable.
    GovernanceRequired,
}

/// Reconciliation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationState {
    /// No reconciliation is required for the observed outcome.
    NotRequired,
    /// Unknown outcome awaits reconciliation.
    Pending,
    /// Unknown outcome was authoritatively resolved.
    Resolved,
    /// Governance must decide how to proceed.
    GovernanceRequired,
}

/// Effect projection with immutable occurrence binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectProjection {
    /// Structural intent identity.
    pub intent_id: String,
    /// Owning scope.
    pub scope_id: String,
    /// Abstract operation.
    pub operation: String,
    /// External mutation flag.
    pub mutating: bool,
    /// Canonical arguments.
    pub args: ArtifactRef,
    /// Immutable historical realization.
    pub occurrence_binding: String,
    /// Control phase.
    pub phase: EffectPhase,
    /// World outcome.
    pub outcome: WorldOutcome,
    /// Reconciliation state.
    pub reconciliation: ReconciliationState,
}

/// Scope-transferred world-effect obligation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObligationProjection {
    /// Deterministic obligation identity.
    pub obligation_id: String,
    /// Effect intent.
    pub intent_id: String,
    /// Whether this obligation blocks normal Run completion.
    pub blocking: bool,
    /// Whether an authoritative terminal outcome is known.
    pub resolved: bool,
}

/// Fenced attempt projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptProjection {
    /// Attempt identity.
    pub attempt_id: String,
    /// Continuation identity.
    pub continuation_id: String,
    /// Immutable occurrence binding.
    pub occurrence_binding: String,
    /// Fencing epoch.
    pub epoch: u64,
    /// Whether the Attempt currently owns execution.
    pub active: bool,
}

/// Explicit exact-replay capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplayAvailability {
    /// All required canonical inputs are available.
    Exact,
    /// State projections remain available but complete nondeterminism does not.
    ProjectionOnly {
        /// Missing or redacted references.
        missing: Vec<String>,
    },
    /// Even the requested projection cannot be reconstructed.
    Unavailable {
        /// Stable explanation.
        reason: String,
    },
}

/// Compaction witness. The Embedded profile constructs and validates this type
/// but does not delete canonical history automatically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionCertificate {
    /// Causally closed source frontier.
    pub source_frontier: Vec<String>,
    /// Digest of the summarized projection.
    pub projection_digest: String,
    /// Unresolved obligations preserved by the summary.
    pub unresolved_obligations: Vec<String>,
    /// Historical occurrence bindings retained for interpretation.
    pub occurrence_bindings: Vec<String>,
    /// Resulting replay capability.
    pub replay_availability: ReplayAvailability,
}

impl Projection {
    /// Apply one verified canonical event.
    pub fn apply_event(&mut self, event: &Event) -> Result<()> {
        match &event.payload {
            EventPayload::RunStarted {
                plan_id,
                binding_context,
            } => {
                if self.runs.contains_key(&event.run_id) {
                    return Err(CoreError::IllegalTransition(format!(
                        "Run {} already exists",
                        event.run_id
                    )));
                }
                let mut scopes = BTreeMap::new();
                scopes.insert(
                    ROOT_SCOPE_ID.to_owned(),
                    ScopeProjection {
                        scope_id: ROOT_SCOPE_ID.to_owned(),
                        parent_scope: None,
                        status: ScopeStatus::Open,
                        intents: BTreeSet::new(),
                    },
                );
                self.runs.insert(
                    event.run_id.clone(),
                    RunProjection {
                        run_id: event.run_id.clone(),
                        initial_plan: plan_id.clone(),
                        current_plan: plan_id.clone(),
                        initial_binding_context: binding_context.clone(),
                        current_binding_context: binding_context.clone(),
                        epoch: 0,
                        status: RunStatus::Active,
                        scopes,
                        effects: BTreeMap::new(),
                        obligations: BTreeMap::new(),
                        attempts: BTreeMap::new(),
                        result: None,
                        last_event: event.event_id.clone(),
                    },
                );
                return Ok(());
            }
            EventPayload::FactRecorded { key, value } => {
                if let Some(existing) = self.facts.get(key) {
                    if existing != value {
                        return Err(CoreError::IllegalTransition(format!(
                            "fact {key:?} already has a different value"
                        )));
                    }
                } else {
                    self.facts.insert(key.clone(), value.clone());
                }
            }
            _ => {}
        }

        let run = self
            .runs
            .get_mut(&event.run_id)
            .ok_or_else(|| CoreError::NotFound(format!("Run {} does not exist", event.run_id)))?;
        if run.status == RunStatus::Completed {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} is already completed",
                event.run_id
            )));
        }

        match &event.payload {
            EventPayload::RunStarted { .. } | EventPayload::FactRecorded { .. } => {}
            EventPayload::AttemptStarted {
                attempt_id,
                continuation_id,
                occurrence_binding,
                epoch,
            } => {
                if *epoch != run.epoch {
                    return Err(CoreError::IllegalTransition(format!(
                        "attempt epoch {epoch} does not match Run epoch {}",
                        run.epoch
                    )));
                }
                if run.attempts.contains_key(attempt_id) {
                    return Err(CoreError::IllegalTransition(format!(
                        "attempt {attempt_id} already exists"
                    )));
                }
                run.attempts.insert(
                    attempt_id.clone(),
                    AttemptProjection {
                        attempt_id: attempt_id.clone(),
                        continuation_id: continuation_id.clone(),
                        occurrence_binding: occurrence_binding.clone(),
                        epoch: *epoch,
                        active: true,
                    },
                );
            }
            EventPayload::AttemptYielded { attempt_id, epoch } => {
                let attempt = run.attempts.get_mut(attempt_id).ok_or_else(|| {
                    CoreError::NotFound(format!("attempt {attempt_id} does not exist"))
                })?;
                if !attempt.active || attempt.epoch != *epoch {
                    return Err(CoreError::IllegalTransition(format!(
                        "attempt {attempt_id} is stale or inactive"
                    )));
                }
                attempt.active = false;
            }
            EventPayload::EpochAdvanced { epoch } => {
                if *epoch != run.epoch + 1 {
                    return Err(CoreError::IllegalTransition(format!(
                        "epoch must advance from {} to {}; received {epoch}",
                        run.epoch,
                        run.epoch + 1
                    )));
                }
                run.epoch = *epoch;
                for attempt in run.attempts.values_mut() {
                    attempt.active = false;
                }
            }
            EventPayload::ScopeOpened {
                scope_id,
                parent_scope,
            } => {
                if run.scopes.contains_key(scope_id) {
                    return Err(CoreError::IllegalTransition(format!(
                        "scope {scope_id} already exists"
                    )));
                }
                let parent = run.scopes.get(parent_scope).ok_or_else(|| {
                    CoreError::NotFound(format!("parent scope {parent_scope} does not exist"))
                })?;
                if parent.status != ScopeStatus::Open {
                    return Err(CoreError::IllegalTransition(format!(
                        "parent scope {parent_scope} is not open"
                    )));
                }
                run.scopes.insert(
                    scope_id.clone(),
                    ScopeProjection {
                        scope_id: scope_id.clone(),
                        parent_scope: Some(parent_scope.clone()),
                        status: ScopeStatus::Open,
                        intents: BTreeSet::new(),
                    },
                );
            }
            EventPayload::EffectProposed {
                intent_id,
                scope_id,
                operation,
                mutating,
                args,
                occurrence_binding,
            } => {
                if run.effects.contains_key(intent_id) {
                    return Err(CoreError::IllegalTransition(format!(
                        "effect intent {intent_id} already exists"
                    )));
                }
                let scope = run.scopes.get_mut(scope_id).ok_or_else(|| {
                    CoreError::NotFound(format!("scope {scope_id} does not exist"))
                })?;
                if scope.status != ScopeStatus::Open {
                    return Err(CoreError::IllegalTransition(format!(
                        "scope {scope_id} is not open"
                    )));
                }
                scope.intents.insert(intent_id.clone());
                run.effects.insert(
                    intent_id.clone(),
                    EffectProjection {
                        intent_id: intent_id.clone(),
                        scope_id: scope_id.clone(),
                        operation: operation.clone(),
                        mutating: *mutating,
                        args: args.clone(),
                        occurrence_binding: occurrence_binding.clone(),
                        phase: EffectPhase::Admitted,
                        outcome: WorldOutcome::Unobserved,
                        reconciliation: ReconciliationState::NotRequired,
                    },
                );
            }
            EventPayload::EffectTransitioned {
                intent_id,
                transition,
            } => {
                let effect = run.effects.get_mut(intent_id).ok_or_else(|| {
                    CoreError::NotFound(format!("effect {intent_id} does not exist"))
                })?;
                apply_effect_transition(effect, transition)?;
                update_obligation(run, intent_id);
            }
            EventPayload::ScopeCommitted {
                scope_id,
                obligations,
            } => {
                let scope = run.scopes.get_mut(scope_id).ok_or_else(|| {
                    CoreError::NotFound(format!("scope {scope_id} does not exist"))
                })?;
                if scope.status != ScopeStatus::Open {
                    return Err(CoreError::IllegalTransition(format!(
                        "scope {scope_id} is not open"
                    )));
                }
                scope.status = ScopeStatus::ClosedCommitted;
                for obligation in obligations {
                    if !scope.intents.contains(&obligation.intent_id) {
                        return Err(CoreError::IllegalTransition(format!(
                            "obligation {} does not belong to scope {scope_id}",
                            obligation.obligation_id
                        )));
                    }
                    if run
                        .obligations
                        .insert(obligation.obligation_id.clone(), obligation.clone())
                        .is_some()
                    {
                        return Err(CoreError::IllegalTransition(format!(
                            "obligation {} already exists",
                            obligation.obligation_id
                        )));
                    }
                }
            }
            EventPayload::ScopeAborted { scope_id } => {
                let scope = run.scopes.get_mut(scope_id).ok_or_else(|| {
                    CoreError::NotFound(format!("scope {scope_id} does not exist"))
                })?;
                if scope.status != ScopeStatus::Open {
                    return Err(CoreError::IllegalTransition(format!(
                        "scope {scope_id} is not open"
                    )));
                }
                for intent_id in &scope.intents {
                    let effect = run.effects.get_mut(intent_id).ok_or_else(|| {
                        CoreError::NotFound(format!("effect {intent_id} does not exist"))
                    })?;
                    if matches!(
                        effect.phase,
                        EffectPhase::ReleaseAuthorized | EffectPhase::DispatchStarted
                    ) {
                        return Err(CoreError::IllegalTransition(format!(
                            "scope {scope_id} cannot abort after effect release"
                        )));
                    }
                    effect.phase = EffectPhase::CancelledBeforeRelease;
                }
                scope.status = ScopeStatus::ClosedAborted;
            }
            EventPayload::BindingUpdated { previous, current } => {
                if &run.current_binding_context != previous {
                    return Err(CoreError::IllegalTransition(format!(
                        "binding context changed from expected {previous}"
                    )));
                }
                run.current_binding_context.clone_from(current);
            }
            EventPayload::RunCompleted { result } => {
                let unresolved = run
                    .obligations
                    .values()
                    .any(|obligation| obligation.blocking && !obligation.resolved);
                if unresolved {
                    return Err(CoreError::IllegalTransition(
                        "Run has unresolved blocking effect obligations".to_owned(),
                    ));
                }
                if run
                    .scopes
                    .values()
                    .any(|scope| scope.status == ScopeStatus::Open)
                {
                    return Err(CoreError::IllegalTransition(
                        "Run has an open scope".to_owned(),
                    ));
                }
                run.status = RunStatus::Completed;
                run.result.clone_from(result);
            }
        }
        run.last_event.clone_from(&event.event_id);
        Ok(())
    }

    /// Deterministic digest of the complete rebuildable projection.
    pub fn digest(&self) -> Result<String> {
        canonical_digest(self)
    }
}

fn apply_effect_transition(
    effect: &mut EffectProjection,
    transition: &EffectTransition,
) -> Result<()> {
    match transition {
        EffectTransition::Prepare if effect.phase == EffectPhase::Admitted => {
            effect.phase = EffectPhase::Prepared;
        }
        EffectTransition::AuthorizeRelease if effect.phase == EffectPhase::Prepared => {
            effect.phase = EffectPhase::ReleaseAuthorized;
        }
        EffectTransition::StartDispatch if effect.phase == EffectPhase::ReleaseAuthorized => {
            effect.phase = EffectPhase::DispatchStarted;
        }
        EffectTransition::Observe(outcome)
            if effect.phase == EffectPhase::DispatchStarted
                && effect.outcome == WorldOutcome::Unobserved
                && *outcome != WorldOutcome::Unobserved =>
        {
            effect.outcome = *outcome;
            effect.reconciliation = if *outcome == WorldOutcome::Unknown {
                ReconciliationState::Pending
            } else {
                ReconciliationState::NotRequired
            };
        }
        EffectTransition::Reconcile(resolution)
            if effect.phase == EffectPhase::DispatchStarted
                && effect.outcome == WorldOutcome::Unknown =>
        {
            match resolution {
                ReconciliationResolution::ResolvedApplied => {
                    effect.outcome = WorldOutcome::Applied;
                    effect.reconciliation = ReconciliationState::Resolved;
                }
                ReconciliationResolution::ResolvedNotApplied => {
                    effect.outcome = WorldOutcome::NotApplied;
                    effect.reconciliation = ReconciliationState::Resolved;
                }
                ReconciliationResolution::StillUnknown => {
                    effect.reconciliation = ReconciliationState::Pending;
                }
                ReconciliationResolution::GovernanceRequired => {
                    effect.reconciliation = ReconciliationState::GovernanceRequired;
                }
            }
        }
        _ => {
            return Err(CoreError::IllegalTransition(format!(
                "illegal effect transition {transition:?} from phase {:?}, outcome {:?}, reconciliation {:?}",
                effect.phase, effect.outcome, effect.reconciliation
            )));
        }
    }
    Ok(())
}

fn update_obligation(run: &mut RunProjection, intent_id: &str) {
    let resolved = run.effects.get(intent_id).is_some_and(|effect| {
        matches!(
            effect.outcome,
            WorldOutcome::Applied | WorldOutcome::NotApplied
        )
    });
    for obligation in run
        .obligations
        .values_mut()
        .filter(|obligation| obligation.intent_id == intent_id)
    {
        obligation.resolved = resolved;
    }
}

/// Derive the structural identity of one intentional effect occurrence.
#[allow(clippy::too_many_arguments)]
pub fn effect_intent_id(
    run_id: &str,
    invocation_id: &str,
    site_id: &str,
    scope_id: &str,
    scope_epoch: u64,
    occurrence: &str,
    args: &ArtifactRef,
    effect_schema_version: &str,
) -> Result<String> {
    #[derive(Serialize)]
    struct IntentPreimage<'a> {
        run_id: &'a str,
        invocation_id: &'a str,
        site_id: &'a str,
        scope_id: &'a str,
        scope_epoch: u64,
        occurrence: &'a str,
        args: &'a ArtifactRef,
        effect_schema_version: &'a str,
    }
    content_id(
        "cymule.effect-intent/1",
        &IntentPreimage {
            run_id,
            invocation_id,
            site_id,
            scope_id,
            scope_epoch,
            occurrence,
            args,
            effect_schema_version,
        },
    )
}

/// Derive a stable obligation identity from an intent.
pub fn effect_obligation_id(intent_id: &str) -> Result<String> {
    content_id("cymule.effect-obligation/1", &intent_id)
}
