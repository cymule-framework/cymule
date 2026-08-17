use std::collections::{BTreeMap, BTreeSet};

use crate::ir::{EffectContract, MutationKind};
use crate::model::{effect_intent_id, effect_obligation_id};
use crate::{
    ArtifactRecord, ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, CommandReceipt,
    CommandReceiptStatus, CoreError, Event, EventPayload, ObligationProjection, PlanCandidate,
    Projection, ReplayAvailability, Result, SealedPlan, WorldOutcome, canonical_digest, content_id,
    sha256_bytes,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandRecord {
    semantic_hash: String,
    receipt: CommandReceipt,
}

/// One verified canonical base projection replacing a causally closed prefix.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineBaseSnapshot {
    /// Content digest of all compacted prefix events and any prior base.
    pub prefix_digest: String,
    /// Exact identities in the compacted causal prefix.
    pub compacted_event_ids: BTreeSet<String>,
    /// Projection after applying the complete compacted prefix.
    pub projection: Projection,
    /// Digest that authenticates the retained projection bytes.
    pub projection_digest: String,
}

impl MachineBaseSnapshot {
    fn verify(&self) -> Result<()> {
        if !self.prefix_digest.starts_with("sha256:")
            || self.prefix_digest.len() != "sha256:".len() + 64
            || self.compacted_event_ids.is_empty()
            || self.compacted_event_ids.iter().any(String::is_empty)
        {
            return Err(CoreError::Validation(
                "machine base snapshot has malformed prefix evidence".to_owned(),
            ));
        }
        let expected = self.projection.digest()?;
        if self.projection_digest != expected {
            return Err(CoreError::IdentityMismatch(format!(
                "machine base projection digest {} does not match {expected}",
                self.projection_digest
            )));
        }
        for run in self.projection.runs.values() {
            if !self.compacted_event_ids.contains(&run.last_event) {
                return Err(CoreError::Causal(format!(
                    "machine base Run {} ends at an unretained event {}",
                    run.run_id, run.last_event
                )));
            }
        }
        Ok(())
    }
}

/// Portable evidence returned after compacting one canonical event prefix.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCompaction {
    /// Content-addressed base snapshot identity.
    pub base_id: String,
    /// Cumulative number of compacted event identities.
    pub compacted_events: u64,
    /// Number of full suffix Events retained for resume.
    pub retained_events: u64,
    /// Causal frontier connecting the base to retained execution.
    pub causal_frontier: BTreeSet<String>,
    /// Authenticated base projection digest.
    pub projection_digest: String,
}

/// Portable, provider-neutral snapshot of canonical inputs and optional base.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineSnapshot {
    /// Snapshot schema version.
    pub snapshot_version: String,
    /// Sealed Plans in content-ID order.
    pub plans: Vec<SealedPlan>,
    /// Immutable Artifacts in content-ID order.
    pub artifacts: Vec<ArtifactRecord>,
    /// Optional verified projection for a compacted causal prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<MachineBaseSnapshot>,
    /// Canonical suffix Events in admitted causal order.
    pub events: Vec<Event>,
    /// Command semantic hashes and receipts for idempotent recovery.
    commands: BTreeMap<String, CommandRecord>,
}

impl MachineSnapshot {
    /// Current snapshot schema version.
    pub const VERSION: &'static str = "cymule.machine-snapshot/2";

    /// Content digest used by conditional durable-store writes.
    pub fn digest(&self) -> Result<String> {
        canonical_digest(self)
    }

    /// Stable content digests for idempotent command records, keyed by command
    /// identity. Durable layers use this to validate an exact canonical delta
    /// without exposing the private command-record representation.
    pub fn command_digests(&self) -> Result<BTreeMap<String, String>> {
        self.commands
            .iter()
            .map(|(command_id, record)| {
                canonical_digest(record).map(|digest| (command_id.clone(), digest))
            })
            .collect()
    }
}

/// In-memory reference machine for the Semantic Interpreter and Embedded
/// profiles. All ambient I/O belongs in higher-level runtime crates.
#[derive(Debug, Clone, Default)]
pub struct Machine {
    plans: BTreeMap<String, SealedPlan>,
    artifacts: BTreeMap<String, ArtifactRecord>,
    events: BTreeMap<String, Event>,
    event_order: Vec<String>,
    base: Option<MachineBaseSnapshot>,
    compacted_event_ids: BTreeSet<String>,
    projection: Projection,
    commands: BTreeMap<String, CommandRecord>,
}

impl Machine {
    /// Create an empty semantic machine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Export canonical inputs for durable persistence.
    pub fn snapshot(&self) -> MachineSnapshot {
        MachineSnapshot {
            snapshot_version: MachineSnapshot::VERSION.to_owned(),
            plans: self.plans.values().cloned().collect(),
            artifacts: self.artifacts.values().cloned().collect(),
            base: self.base.clone(),
            events: self.events().cloned().collect(),
            commands: self.commands.clone(),
        }
    }

    /// Restore a Machine and deterministically rebuild all projections.
    pub fn restore(snapshot: MachineSnapshot) -> Result<Self> {
        if snapshot.snapshot_version != MachineSnapshot::VERSION
            && snapshot.snapshot_version != "cymule.machine-snapshot/1"
        {
            return Err(CoreError::Validation(format!(
                "unsupported machine snapshot version {:?}",
                snapshot.snapshot_version
            )));
        }
        let mut machine = Self::new();
        for plan in snapshot.plans {
            machine.insert_plan(plan)?;
        }
        for artifact in snapshot.artifacts {
            let restored = machine.put_artifact(artifact.reference.kind.clone(), artifact.bytes);
            if restored != artifact.reference {
                return Err(CoreError::IdentityMismatch(format!(
                    "artifact ID {} does not match its bytes",
                    artifact.reference.artifact_id
                )));
            }
        }
        if let Some(base) = snapshot.base {
            base.verify()?;
            machine.projection = base.projection.clone();
            machine
                .compacted_event_ids
                .clone_from(&base.compacted_event_ids);
            machine.base = Some(base);
        }
        for event in snapshot.events {
            machine.append_event(event)?;
        }
        for (command_id, record) in &snapshot.commands {
            if record.receipt.command_id != *command_id {
                return Err(CoreError::IdentityMismatch(format!(
                    "command snapshot key {command_id} does not match its receipt"
                )));
            }
            if let Some(event_id) = &record.receipt.event_id
                && !machine.events.contains_key(event_id)
                && !machine.compacted_event_ids.contains(event_id)
            {
                return Err(CoreError::NotFound(format!(
                    "command {command_id} references missing event {event_id}"
                )));
            }
        }
        machine.commands = snapshot.commands;
        machine.verify_replay()?;
        Ok(machine)
    }

    /// Compact a causally closed event prefix and retain a full suffix.
    pub fn compact_event_history(&mut self, retain_suffix: usize) -> Result<MachineCompaction> {
        if retain_suffix >= self.event_order.len() {
            return Err(CoreError::Validation(
                "event compaction must remove at least one retained Event".to_owned(),
            ));
        }
        let cut = self.event_order.len() - retain_suffix;
        let prefix_ids = self.event_order[..cut].to_vec();
        let prefix: Vec<Event> = prefix_ids
            .iter()
            .map(|event_id| {
                self.events
                    .get(event_id)
                    .expect("event order references existing Event")
                    .clone()
            })
            .collect();
        let mut projection = self
            .base
            .as_ref()
            .map(|base| base.projection.clone())
            .unwrap_or_default();
        for event in &prefix {
            projection.apply_event(event)?;
        }
        let prior_digest = self.base.as_ref().map(|base| base.prefix_digest.as_str());
        let prefix_digest = content_id("cymule.machine-event-prefix/1", &(prior_digest, &prefix))?;
        let mut compacted_event_ids = self.compacted_event_ids.clone();
        compacted_event_ids.extend(prefix_ids.iter().cloned());
        let projection_digest = projection.digest()?;
        let base = MachineBaseSnapshot {
            prefix_digest,
            compacted_event_ids: compacted_event_ids.clone(),
            projection,
            projection_digest: projection_digest.clone(),
        };
        base.verify()?;
        let base_id = content_id("cymule.machine-base/1", &base)?;
        for event_id in &prefix_ids {
            self.events.remove(event_id);
        }
        self.event_order.drain(..cut);
        self.compacted_event_ids = compacted_event_ids;
        self.base = Some(base);
        let causal_frontier = self.compaction_frontier();
        Ok(MachineCompaction {
            base_id,
            compacted_events: u64::try_from(self.compacted_event_ids.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            retained_events: u64::try_from(self.event_order.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            causal_frontier,
            projection_digest,
        })
    }

    /// Validate, seal, and store a Plan Candidate.
    pub fn seal_plan(&mut self, candidate: PlanCandidate) -> Result<SealedPlan> {
        let plan = candidate.seal()?;
        self.insert_plan(plan.clone())?;
        Ok(plan)
    }

    /// Insert and verify an already sealed plan.
    pub fn insert_plan(&mut self, plan: SealedPlan) -> Result<()> {
        plan.verify()?;
        if let Some(existing) = self.plans.get(&plan.plan_id) {
            if existing != &plan {
                return Err(CoreError::IdentityMismatch(format!(
                    "plan {} already exists with different content",
                    plan.plan_id
                )));
            }
            return Ok(());
        }
        self.plans.insert(plan.plan_id.clone(), plan);
        Ok(())
    }

    /// Read a sealed plan.
    pub fn plan(&self, plan_id: &str) -> Option<&SealedPlan> {
        self.plans.get(plan_id)
    }

    /// Store immutable typed bytes and return their content reference.
    pub fn put_artifact(&mut self, kind: impl Into<String>, bytes: Vec<u8>) -> ArtifactRef {
        let kind = kind.into();
        let mut preimage = Vec::with_capacity(kind.len() + bytes.len() + 20);
        preimage.extend_from_slice(b"cymule.artifact/1\0");
        preimage.extend_from_slice(kind.as_bytes());
        preimage.push(0);
        preimage.extend_from_slice(&bytes);
        let reference = ArtifactRef {
            artifact_id: format!("sha256:{}", sha256_bytes(&preimage)),
            kind,
        };
        self.artifacts
            .entry(reference.artifact_id.clone())
            .or_insert_with(|| ArtifactRecord {
                reference: reference.clone(),
                bytes,
            });
        reference
    }

    /// Read an immutable artifact.
    pub fn artifact(&self, reference: &ArtifactRef) -> Option<&ArtifactRecord> {
        self.artifacts
            .get(&reference.artifact_id)
            .filter(|record| record.reference.kind == reference.kind)
    }

    /// Remove an artifact to exercise retention behavior in conformance tests.
    pub fn remove_artifact_for_test(&mut self, artifact_id: &str) -> Option<ArtifactRecord> {
        self.artifacts.remove(artifact_id)
    }

    /// Classify replay capability for a required artifact set.
    pub fn replay_availability(&self, required: &[ArtifactRef]) -> ReplayAvailability {
        let missing: Vec<String> = required
            .iter()
            .filter(|reference| self.artifact(reference).is_none())
            .map(|reference| reference.artifact_id.clone())
            .collect();
        if missing.is_empty() {
            ReplayAvailability::Exact
        } else if self.events.is_empty() {
            ReplayAvailability::Unavailable {
                reason: "canonical event history is unavailable".to_owned(),
            }
        } else {
            ReplayAvailability::ProjectionOnly { missing }
        }
    }

    /// Admit an idempotent command and reduce its canonical event.
    pub fn submit(&mut self, envelope: CommandEnvelope) -> Result<CommandReceipt> {
        validate_envelope(&envelope)?;
        let semantic_hash = canonical_digest(&envelope)?;
        if let Some(record) = self.commands.get(&envelope.command_id) {
            if record.semantic_hash == semantic_hash {
                return Ok(record.receipt.clone());
            }
            return Err(CoreError::CommandReuse(format!(
                "command ID {} was already used with different semantics",
                envelope.command_id
            )));
        }

        let current_precondition = self
            .projection
            .runs
            .get(&envelope.run_id)
            .map(crate::RunProjection::precondition_token);
        if !matches!(envelope.command, Command::StartRun { .. }) {
            let observed = envelope.expected_precondition.clone().ok_or_else(|| {
                CoreError::Validation("mutating commands require expected_precondition".to_owned())
            })?;
            if Some(&observed) != current_precondition.as_ref() {
                let receipt = CommandReceipt {
                    command_id: envelope.command_id.clone(),
                    status: CommandReceiptStatus::Conflict,
                    event_id: None,
                    error_code: Some("stale_action".to_owned()),
                    message: Some("the Run changed after the caller's view".to_owned()),
                    observed_precondition: Some(observed),
                    current_precondition,
                };
                self.commands.insert(
                    envelope.command_id,
                    CommandRecord {
                        semantic_hash,
                        receipt: receipt.clone(),
                    },
                );
                return Ok(receipt);
            }
        }

        let payload = self.admit_command(&envelope)?;
        let (reads, writes, coordination_key) = footprints(&envelope.run_id, &payload);
        let parents = self
            .projection
            .runs
            .get(&envelope.run_id)
            .map_or_else(Vec::new, |run| vec![run.last_event.clone()]);
        let event = Event::new(
            envelope.command_id.clone(),
            semantic_hash.clone(),
            envelope.run_id.clone(),
            parents,
            reads,
            writes,
            coordination_key,
            payload,
        )?;
        self.append_event(event.clone())?;
        let receipt = CommandReceipt {
            command_id: envelope.command_id.clone(),
            status: CommandReceiptStatus::Applied,
            event_id: Some(event.event_id.clone()),
            error_code: None,
            message: None,
            observed_precondition: envelope.expected_precondition,
            current_precondition: self
                .projection
                .runs
                .get(&envelope.run_id)
                .map(crate::RunProjection::precondition_token),
        };
        self.commands.insert(
            envelope.command_id,
            CommandRecord {
                semantic_hash,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    /// Append a trusted event after identity, parent, and transition validation.
    pub fn append_event(&mut self, event: Event) -> Result<()> {
        event.verify()?;
        if self.compacted_event_ids.contains(&event.event_id) {
            return Err(CoreError::IdentityMismatch(format!(
                "event {} belongs to the compacted prefix",
                event.event_id
            )));
        }
        if let Some(existing) = self.events.get(&event.event_id) {
            if existing == &event {
                return Ok(());
            }
            return Err(CoreError::IdentityMismatch(format!(
                "event {} already exists with different content",
                event.event_id
            )));
        }
        for parent in &event.parents {
            if !self.events.contains_key(parent) && !self.compacted_event_ids.contains(parent) {
                return Err(CoreError::Causal(format!(
                    "event {} references missing parent {parent}",
                    event.event_id
                )));
            }
        }
        let mut next = self.projection.clone();
        next.apply_event(&event)?;
        self.projection = next;
        self.event_order.push(event.event_id.clone());
        self.events.insert(event.event_id.clone(), event);
        Ok(())
    }

    /// Current rebuildable projection.
    pub const fn projection(&self) -> &Projection {
        &self.projection
    }

    /// Events in admission order.
    pub fn events(&self) -> impl Iterator<Item = &Event> {
        self.event_order
            .iter()
            .filter_map(|event_id| self.events.get(event_id))
    }

    /// Rebuild a projection from an unordered causal event set.
    pub fn replay(events: impl IntoIterator<Item = Event>) -> Result<Projection> {
        let mut remaining = BTreeMap::new();
        for event in events {
            event.verify()?;
            if let Some(existing) = remaining.insert(event.event_id.clone(), event.clone())
                && existing != event
            {
                return Err(CoreError::IdentityMismatch(format!(
                    "duplicate event ID {} has different content",
                    event.event_id
                )));
            }
        }
        for event in remaining.values() {
            for parent in &event.parents {
                if !remaining.contains_key(parent) {
                    return Err(CoreError::Causal(format!(
                        "event {} references missing parent {parent}",
                        event.event_id
                    )));
                }
            }
        }

        let mut projection = Projection::default();
        let mut applied = BTreeSet::new();
        while !remaining.is_empty() {
            let ready_id = remaining
                .iter()
                .find(|(_, event)| event.parents.iter().all(|parent| applied.contains(parent)))
                .map(|(event_id, _)| event_id.clone())
                .ok_or_else(|| CoreError::Causal("event graph contains a cycle".to_owned()))?;
            let event = remaining.remove(&ready_id).ok_or_else(|| {
                CoreError::Causal("ready event disappeared during replay".to_owned())
            })?;
            projection.apply_event(&event)?;
            applied.insert(ready_id);
        }
        Ok(projection)
    }

    /// Replay all current events and verify the digest matches the live projection.
    pub fn verify_replay(&self) -> Result<()> {
        let mut replayed = self
            .base
            .as_ref()
            .map(|base| base.projection.clone())
            .unwrap_or_default();
        let mut applied = self.compacted_event_ids.clone();
        for event in self.events() {
            if !event.parents.iter().all(|parent| applied.contains(parent)) {
                return Err(CoreError::Causal(format!(
                    "event {} has a parent outside the compacted base and suffix",
                    event.event_id
                )));
            }
            replayed.apply_event(event)?;
            applied.insert(event.event_id.clone());
        }
        let current_digest = self.projection.digest()?;
        let replayed_digest = replayed.digest()?;
        if current_digest != replayed_digest {
            return Err(CoreError::IdentityMismatch(format!(
                "projection digest {current_digest} does not match replay {replayed_digest}"
            )));
        }
        Ok(())
    }

    fn compaction_frontier(&self) -> BTreeSet<String> {
        let mut frontier: BTreeSet<String> = self
            .events()
            .flat_map(|event| event.parents.iter())
            .filter(|parent| self.compacted_event_ids.contains(*parent))
            .cloned()
            .collect();
        if let Some(base) = &self.base {
            frontier.extend(
                base.projection
                    .runs
                    .values()
                    .map(|run| run.last_event.clone()),
            );
        }
        frontier
    }

    fn admit_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::StartRun {
                plan_id,
                binding_context,
            } => {
                if self.projection.runs.contains_key(&envelope.run_id) {
                    return Err(CoreError::IllegalTransition(format!(
                        "Run {} already exists",
                        envelope.run_id
                    )));
                }
                if !self.plans.contains_key(plan_id) {
                    return Err(CoreError::NotFound(format!(
                        "plan {plan_id} does not exist"
                    )));
                }
                Ok(EventPayload::RunStarted {
                    plan_id: plan_id.clone(),
                    binding_context: binding_context.clone(),
                })
            }
            Command::BeginAttempt {
                attempt_id,
                continuation_id,
                occurrence_binding,
                epoch,
            } => Ok(EventPayload::AttemptStarted {
                attempt_id: attempt_id.clone(),
                continuation_id: continuation_id.clone(),
                occurrence_binding: occurrence_binding.clone(),
                epoch: *epoch,
            }),
            Command::YieldAttempt { attempt_id, epoch } => Ok(EventPayload::AttemptYielded {
                attempt_id: attempt_id.clone(),
                epoch: *epoch,
            }),
            Command::AdvanceEpoch => {
                let run = self.run(&envelope.run_id)?;
                Ok(EventPayload::EpochAdvanced {
                    epoch: run.epoch + 1,
                })
            }
            Command::OpenScope {
                scope_id,
                parent_scope,
            } => Ok(EventPayload::ScopeOpened {
                scope_id: scope_id.clone(),
                parent_scope: parent_scope.clone(),
            }),
            Command::ProposeEffect {
                scope_id,
                invocation_id,
                site_id,
                occurrence,
                operation,
                args,
                occurrence_binding,
            } => {
                if self.artifact(args).is_none() {
                    return Err(CoreError::NotFound(format!(
                        "effect argument artifact {} does not exist",
                        args.artifact_id
                    )));
                }
                let run = self.run(&envelope.run_id)?;
                let contract = self.effect_contract(&run.current_plan, operation)?;
                let intent_id = effect_intent_id(
                    &envelope.run_id,
                    invocation_id,
                    site_id,
                    scope_id,
                    run.epoch,
                    occurrence,
                    args,
                    "cymule.effect-schema/1",
                )?;
                Ok(EventPayload::EffectProposed {
                    intent_id,
                    scope_id: scope_id.clone(),
                    operation: operation.clone(),
                    mutating: contract.profile.mutation == MutationKind::Mutating,
                    args: args.clone(),
                    occurrence_binding: occurrence_binding.clone(),
                })
            }
            Command::TransitionEffect {
                intent_id,
                transition,
            } => Ok(EventPayload::EffectTransitioned {
                intent_id: intent_id.clone(),
                transition: transition.clone(),
            }),
            Command::CommitScope { scope_id } => {
                let run = self.run(&envelope.run_id)?;
                let scope = run.scopes.get(scope_id).ok_or_else(|| {
                    CoreError::NotFound(format!("scope {scope_id} does not exist"))
                })?;
                let mut obligations = Vec::new();
                for intent_id in &scope.intents {
                    let effect = run.effects.get(intent_id).ok_or_else(|| {
                        CoreError::NotFound(format!("effect {intent_id} does not exist"))
                    })?;
                    if effect.mutating {
                        obligations.push(ObligationProjection {
                            obligation_id: effect_obligation_id(intent_id)?,
                            intent_id: intent_id.clone(),
                            blocking: true,
                            resolved: matches!(
                                effect.outcome,
                                WorldOutcome::Applied | WorldOutcome::NotApplied
                            ),
                        });
                    }
                }
                obligations.sort_by(|left, right| left.obligation_id.cmp(&right.obligation_id));
                Ok(EventPayload::ScopeCommitted {
                    scope_id: scope_id.clone(),
                    obligations,
                })
            }
            Command::AbortScope { scope_id } => Ok(EventPayload::ScopeAborted {
                scope_id: scope_id.clone(),
            }),
            Command::UpdateBinding { binding_context } => {
                let run = self.run(&envelope.run_id)?;
                Ok(EventPayload::BindingUpdated {
                    previous: run.current_binding_context.clone(),
                    current: binding_context.clone(),
                })
            }
            Command::RecordFact { key, value } => Ok(EventPayload::FactRecorded {
                key: key.clone(),
                value: value.clone(),
            }),
            Command::CompleteRun { result } => {
                if let Some(reference) = result
                    && self.artifact(reference).is_none()
                {
                    return Err(CoreError::NotFound(format!(
                        "Result artifact {} does not exist",
                        reference.artifact_id
                    )));
                }
                Ok(EventPayload::RunCompleted {
                    result: result.clone(),
                })
            }
        }
    }

    fn run(&self, run_id: &str) -> Result<&crate::RunProjection> {
        self.projection
            .runs
            .get(run_id)
            .ok_or_else(|| CoreError::NotFound(format!("Run {run_id} does not exist")))
    }

    fn effect_contract(&self, plan_id: &str, operation: &str) -> Result<&EffectContract> {
        self.plans
            .get(plan_id)
            .and_then(|plan| {
                plan.candidate
                    .effects
                    .iter()
                    .find(|contract| contract.id == operation)
            })
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "effect operation {operation} does not exist in plan {plan_id}"
                ))
            })
    }
}

fn validate_envelope(envelope: &CommandEnvelope) -> Result<()> {
    if envelope.command_version != COMMAND_VERSION {
        return Err(CoreError::Validation(format!(
            "unsupported command version {:?}",
            envelope.command_version
        )));
    }
    for (kind, value) in [
        ("command ID", envelope.command_id.as_str()),
        ("actor", envelope.actor.as_str()),
        ("Run ID", envelope.run_id.as_str()),
    ] {
        if value.is_empty() || value.len() > 200 {
            return Err(CoreError::Validation(format!(
                "{kind} must contain 1..=200 characters"
            )));
        }
    }
    Ok(())
}

fn footprints(
    run_id: &str,
    payload: &EventPayload,
) -> (BTreeSet<String>, BTreeSet<String>, Option<String>) {
    let run_key = format!("run:{run_id}");
    let mut reads = BTreeSet::new();
    let mut writes = BTreeSet::new();
    let coordination_key = match payload {
        EventPayload::RunStarted { .. } => {
            writes.insert(run_key.clone());
            Some(run_key)
        }
        EventPayload::FactRecorded { key, .. } => {
            writes.insert(format!("fact:{run_id}:{key}"));
            None
        }
        EventPayload::EffectProposed { intent_id, .. }
        | EventPayload::EffectTransitioned { intent_id, .. } => {
            reads.insert(run_key);
            let key = format!("effect:{run_id}:{intent_id}");
            writes.insert(key.clone());
            Some(key)
        }
        EventPayload::ScopeOpened { scope_id, .. }
        | EventPayload::ScopeCommitted { scope_id, .. }
        | EventPayload::ScopeAborted { scope_id } => {
            reads.insert(run_key);
            let key = format!("scope:{run_id}:{scope_id}");
            writes.insert(key.clone());
            Some(key)
        }
        EventPayload::AttemptStarted { attempt_id, .. }
        | EventPayload::AttemptYielded { attempt_id, .. } => {
            reads.insert(run_key);
            let key = format!("attempt:{run_id}:{attempt_id}");
            writes.insert(key.clone());
            Some(key)
        }
        EventPayload::EpochAdvanced { .. }
        | EventPayload::BindingUpdated { .. }
        | EventPayload::RunCompleted { .. } => {
            reads.insert(run_key.clone());
            writes.insert(run_key.clone());
            Some(run_key)
        }
    };
    (reads, writes, coordination_key)
}
