use std::collections::{BTreeMap, BTreeSet};

use crate::ir::{EffectContract, MutationKind, Operation, PlanCandidate, Region};
use crate::model::{effect_intent_id, effect_obligation_id};
use crate::{
    ArtifactRecord, ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, CommandReceipt,
    CommandReceiptStatus, CoreError, Event, EventPayload, ObligationProjection, Projection,
    ReplayAvailability, Result, SealedPlan, WorldOutcome, artifact_ref, canonical_digest,
    content_id,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandRecord {
    semantic_hash: String,
    receipt: CommandReceipt,
}

const MACHINE_PREFIX_VERSION: &str = "cymule.machine-prefix/2";

/// Authenticated command/Event evidence retained after an Event body compacts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactedEventEvidence {
    /// Content-addressed Event identity.
    pub event_id: String,
    /// Command that admitted the Event.
    pub command_id: String,
    /// Canonical command semantic hash copied from the Event.
    pub command_hash: String,
    /// Canonical digest of the complete retained command record and receipt.
    pub command_record_digest: String,
}

#[derive(serde::Serialize)]
struct MachinePrefixPreimage<'a> {
    prefix_version: &'static str,
    compacted_events: &'a [CompactedEventEvidence],
    projection_digest: &'a str,
}

/// One verified canonical base projection replacing a causally closed prefix.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineBaseSnapshot {
    /// Recomputable digest of the complete compacted evidence and projection.
    pub prefix_digest: String,
    /// Exact cumulative compacted Event and command evidence in admission order.
    pub compacted_events: Vec<CompactedEventEvidence>,
    /// Projection after applying the complete compacted prefix.
    pub projection: Projection,
    /// Digest that authenticates the retained projection bytes.
    pub projection_digest: String,
}

impl MachineBaseSnapshot {
    fn verify(&self) -> Result<()> {
        if !is_sha256_id(&self.prefix_digest) || self.compacted_events.is_empty() {
            return Err(CoreError::Validation(
                "machine base snapshot has malformed prefix evidence".to_owned(),
            ));
        }
        let mut event_ids = BTreeSet::new();
        let mut command_ids = BTreeSet::new();
        for evidence in &self.compacted_events {
            if !is_sha256_id(&evidence.event_id)
                || evidence.command_id.is_empty()
                || evidence.command_hash.len() != 64
                || evidence.command_record_digest.len() != 64
                || !evidence.command_hash.bytes().all(is_lower_hex)
                || !evidence.command_record_digest.bytes().all(is_lower_hex)
                || !event_ids.insert(evidence.event_id.clone())
                || !command_ids.insert(evidence.command_id.clone())
            {
                return Err(CoreError::Validation(
                    "machine base snapshot has malformed compacted Event evidence".to_owned(),
                ));
            }
        }
        let expected = self.projection.digest()?;
        if self.projection_digest != expected {
            return Err(CoreError::IdentityMismatch(format!(
                "machine base projection digest {} does not match {expected}",
                self.projection_digest
            )));
        }
        let expected_prefix = machine_prefix_digest(&self.compacted_events, &expected)?;
        if self.prefix_digest != expected_prefix {
            return Err(CoreError::IdentityMismatch(format!(
                "machine prefix digest {} does not match {expected_prefix}",
                self.prefix_digest
            )));
        }
        for run in self.projection.runs.values() {
            if !event_ids.contains(&run.last_event) {
                return Err(CoreError::Causal(format!(
                    "machine base Run {} ends at an unretained event {}",
                    run.run_id, run.last_event
                )));
            }
        }
        Ok(())
    }

    fn event_ids(&self) -> BTreeSet<String> {
        self.compacted_events
            .iter()
            .map(|evidence| evidence.event_id.clone())
            .collect()
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
    pub const VERSION: &'static str = "cymule.machine-snapshot/5";

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
        if snapshot.snapshot_version != MachineSnapshot::VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported machine snapshot version {:?}",
                snapshot.snapshot_version
            )));
        }
        let MachineSnapshot {
            snapshot_version: _,
            plans,
            artifacts,
            base,
            events,
            commands,
        } = snapshot;
        if let Some(base) = &base {
            base.verify()?;
        }
        let mut machine = Self::new();
        for plan in plans {
            machine.insert_plan(plan)?;
        }
        for artifact in artifacts {
            artifact.reference.validate()?;
            let restored = machine.put_artifact(artifact.reference.kind.clone(), artifact.bytes)?;
            if restored != artifact.reference {
                return Err(CoreError::IdentityMismatch(format!(
                    "artifact ID {} does not match its bytes",
                    artifact.reference.artifact_id
                )));
            }
        }
        if let Some(base) = base {
            machine.projection = base.projection.clone();
            machine.compacted_event_ids = base.event_ids();
            machine.base = Some(base);
        }
        for event in events.iter().cloned() {
            machine.append_event(event)?;
        }
        verify_command_event_closure(&events, machine.base.as_ref(), &commands)?;
        machine.commands = commands;
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
        let mut compacted_events = self
            .base
            .as_ref()
            .map(|base| base.compacted_events.clone())
            .unwrap_or_default();
        for event in &prefix {
            let record = self.commands.get(&event.command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "compacted event {} has no command record",
                    event.event_id
                ))
            })?;
            if record.semantic_hash != event.command_hash
                || record.receipt.status != CommandReceiptStatus::Applied
                || record.receipt.event_id.as_deref() != Some(event.event_id.as_str())
            {
                return Err(CoreError::IdentityMismatch(format!(
                    "compacted event {} does not match command {}",
                    event.event_id, event.command_id
                )));
            }
            compacted_events.push(CompactedEventEvidence {
                event_id: event.event_id.clone(),
                command_id: event.command_id.clone(),
                command_hash: event.command_hash.clone(),
                command_record_digest: canonical_digest(record)?,
            });
        }
        let compacted_event_ids = compacted_events
            .iter()
            .map(|evidence| evidence.event_id.clone())
            .collect();
        let projection_digest = projection.digest()?;
        let prefix_digest = machine_prefix_digest(&compacted_events, &projection_digest)?;
        let base = MachineBaseSnapshot {
            prefix_digest,
            compacted_events,
            projection,
            projection_digest: projection_digest.clone(),
        };
        base.verify()?;
        let base_id = content_id("cymule.machine-base/2", &base)?;
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
    pub fn put_artifact(&mut self, kind: impl Into<String>, bytes: Vec<u8>) -> Result<ArtifactRef> {
        let reference = artifact_ref(kind, &bytes)?;
        self.artifacts
            .entry(reference.artifact_id.clone())
            .or_insert_with(|| ArtifactRecord {
                reference: reference.clone(),
                bytes,
            });
        Ok(reference)
    }

    /// Read an immutable artifact.
    pub fn artifact(&self, reference: &ArtifactRef) -> Option<&ArtifactRecord> {
        self.artifacts
            .get(&reference.artifact_id)
            .filter(|record| record.reference == *reference)
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
        match &event.payload {
            EventPayload::RunStarted { .. } if !event.parents.is_empty() => {
                return Err(CoreError::Causal(format!(
                    "Run start event {} must not have a causal parent",
                    event.event_id
                )));
            }
            EventPayload::RunStarted { .. } => {}
            _ => {
                let run = self.projection.runs.get(&event.run_id).ok_or_else(|| {
                    CoreError::NotFound(format!("Run {} does not exist", event.run_id))
                })?;
                if !event.parents.contains(&run.last_event) {
                    return Err(CoreError::Causal(format!(
                        "event {} does not extend Run {} causal frontier {}",
                        event.event_id, event.run_id, run.last_event
                    )));
                }
            }
        }
        self.validate_effect_proposal(&event)?;
        let mut next = self.projection.clone();
        next.apply_event(&event)?;
        verify_event_footprint(&event)?;
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
            verify_event_footprint(&event)?;
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
                let plan = self.plans.get(&run.current_plan).ok_or_else(|| {
                    CoreError::NotFound(format!("plan {} does not exist", run.current_plan))
                })?;
                let scope = run.scopes.get(scope_id).ok_or_else(|| {
                    CoreError::NotFound(format!("scope {scope_id} does not exist"))
                })?;
                if scope.status != crate::ScopeStatus::Open {
                    return Err(CoreError::IllegalTransition(format!(
                        "scope {scope_id} is not open"
                    )));
                }
                if invocation_id.is_empty() || invocation_id.len() > 200 {
                    return Err(CoreError::Validation(
                        "effect invocation ID must contain 1..=200 characters".to_owned(),
                    ));
                }
                let (declared_operation, declared_occurrence) =
                    reachable_effect_site(&plan.candidate, site_id).ok_or_else(|| {
                        CoreError::NotFound(format!(
                            "effect site {site_id} is not reachable from Plan entry {}",
                            plan.candidate.entry
                        ))
                    })?;
                if declared_operation != operation || declared_occurrence != occurrence {
                    return Err(CoreError::Validation(format!(
                        "effect site {site_id} declares operation {declared_operation} and occurrence {declared_occurrence}, not {operation} and {occurrence}"
                    )));
                }
                let contract = self.effect_contract(&run.current_plan, declared_operation)?;
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
                    invocation_id: invocation_id.clone(),
                    site_id: site_id.clone(),
                    occurrence: occurrence.clone(),
                    scope_epoch: run.epoch,
                    effect_schema_version: "cymule.effect-schema/1".to_owned(),
                    operation: operation.clone(),
                    profile: contract.profile.clone(),
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
                    if effect.profile.mutation == MutationKind::Mutating {
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

    fn validate_effect_proposal(&self, event: &Event) -> Result<()> {
        let EventPayload::EffectProposed {
            site_id,
            occurrence,
            operation,
            profile,
            ..
        } = &event.payload
        else {
            return Ok(());
        };
        let run = self.run(&event.run_id)?;
        let plan = self.plans.get(&run.current_plan).ok_or_else(|| {
            CoreError::NotFound(format!("plan {} does not exist", run.current_plan))
        })?;
        let (declared_operation, declared_occurrence) =
            reachable_effect_site(&plan.candidate, site_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "effect site {site_id} is not reachable from Plan entry {}",
                    plan.candidate.entry
                ))
            })?;
        if declared_operation != operation || declared_occurrence != occurrence {
            return Err(CoreError::Validation(format!(
                "effect site {site_id} does not match its declared operation and occurrence"
            )));
        }
        let contract = self.effect_contract(&run.current_plan, operation)?;
        if &contract.profile != profile {
            return Err(CoreError::Validation(format!(
                "effect site {site_id} does not match its Plan-declared profile"
            )));
        }
        Ok(())
    }
}

fn reachable_effect_site<'a>(
    candidate: &'a PlanCandidate,
    site_id: &str,
) -> Option<(&'a str, &'a str)> {
    let definitions: BTreeMap<&str, &crate::Definition> = candidate
        .definitions
        .iter()
        .map(|definition| (definition.id.as_str(), definition))
        .collect();
    let mut pending = vec![candidate.entry.as_str()];
    let mut visited = BTreeSet::new();
    while let Some(definition_id) = pending.pop() {
        if !visited.insert(definition_id) {
            continue;
        }
        let definition = definitions.get(definition_id)?;
        if let Some(found) = reachable_effect_in_region(&definition.body, site_id, &mut pending) {
            return Some(found);
        }
    }
    None
}

fn reachable_effect_in_region<'a>(
    region: &'a Region,
    site_id: &str,
    invoked_definitions: &mut Vec<&'a str>,
) -> Option<(&'a str, &'a str)> {
    for step in &region.steps {
        match &step.operation {
            Operation::Effect {
                effect, occurrence, ..
            } if step.id == site_id => return Some((effect, occurrence)),
            Operation::Invoke { definition, .. } => {
                invoked_definitions.push(definition);
            }
            Operation::Scope { body, .. } => {
                if let Some(found) = reachable_effect_in_region(body, site_id, invoked_definitions)
                {
                    return Some(found);
                }
            }
            _ => {}
        }
    }
    None
}

fn verify_command_event_closure(
    events: &[Event],
    base: Option<&MachineBaseSnapshot>,
    commands: &BTreeMap<String, CommandRecord>,
) -> Result<()> {
    let retained_events: BTreeMap<&str, &Event> = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect();
    let compacted_events: BTreeMap<&str, &CompactedEventEvidence> = base
        .map(|base| {
            base.compacted_events
                .iter()
                .map(|evidence| (evidence.event_id.as_str(), evidence))
                .collect()
        })
        .unwrap_or_default();
    let mut receipt_by_event = BTreeMap::new();

    for (command_id, record) in commands {
        if record.receipt.command_id != *command_id {
            return Err(CoreError::IdentityMismatch(format!(
                "command snapshot key {command_id} does not match its receipt"
            )));
        }
        match (&record.receipt.status, &record.receipt.event_id) {
            (CommandReceiptStatus::Applied, Some(event_id))
                if record.receipt.error_code.is_none() && record.receipt.message.is_none() =>
            {
                if let Some(prior_command) = receipt_by_event.insert(event_id.clone(), command_id) {
                    return Err(CoreError::IdentityMismatch(format!(
                        "event {event_id} is claimed by commands {prior_command} and {command_id}"
                    )));
                }
                if let Some(event) = retained_events.get(event_id.as_str()) {
                    if event.command_id != *command_id || event.command_hash != record.semantic_hash
                    {
                        return Err(CoreError::IdentityMismatch(format!(
                            "command {command_id} does not match retained event {event_id}"
                        )));
                    }
                } else if let Some(evidence) = compacted_events.get(event_id.as_str()) {
                    let record_digest = canonical_digest(record)?;
                    if evidence.command_id != *command_id
                        || evidence.command_hash != record.semantic_hash
                        || evidence.command_record_digest != record_digest
                    {
                        return Err(CoreError::IdentityMismatch(format!(
                            "command {command_id} does not match compacted event {event_id}"
                        )));
                    }
                } else {
                    return Err(CoreError::NotFound(format!(
                        "command {command_id} references missing event {event_id}"
                    )));
                }
            }
            (CommandReceiptStatus::Applied, _) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "applied command {command_id} must have one event and no error"
                )));
            }
            (CommandReceiptStatus::Conflict, Some(event_id)) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "conflicting command {command_id} claims event {event_id}"
                )));
            }
            (CommandReceiptStatus::Conflict, None)
                if record
                    .receipt
                    .error_code
                    .as_deref()
                    .is_some_and(|code| !code.is_empty()) => {}
            (CommandReceiptStatus::Conflict, None) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "conflicting command {command_id} has no typed error"
                )));
            }
        }
    }

    for event in events {
        if !receipt_by_event.contains_key(&event.event_id) {
            return Err(CoreError::NotFound(format!(
                "event {} has no command receipt",
                event.event_id
            )));
        }
    }
    for event_id in compacted_events.keys() {
        if !receipt_by_event.contains_key(*event_id) {
            return Err(CoreError::NotFound(format!(
                "compacted event {event_id} has no command receipt"
            )));
        }
    }
    Ok(())
}

fn machine_prefix_digest(
    compacted_events: &[CompactedEventEvidence],
    projection_digest: &str,
) -> Result<String> {
    content_id(
        MACHINE_PREFIX_VERSION,
        &MachinePrefixPreimage {
            prefix_version: MACHINE_PREFIX_VERSION,
            compacted_events,
            projection_digest,
        },
    )
}

fn is_sha256_id(value: &str) -> bool {
    value.len() == "sha256:".len() + 64
        && value.starts_with("sha256:")
        && value["sha256:".len()..].bytes().all(is_lower_hex)
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

fn verify_event_footprint(event: &Event) -> Result<()> {
    let (reads, writes, coordination_key) = footprints(&event.run_id, &event.payload);
    if event.reads != reads || event.writes != writes || event.coordination_key != coordination_key
    {
        return Err(CoreError::IdentityMismatch(format!(
            "event {} does not match its semantic footprint",
            event.event_id
        )));
    }
    Ok(())
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
            let key = format!("fact:{key}");
            reads.insert(key.clone());
            writes.insert(key.clone());
            Some(key)
        }
        EventPayload::EffectProposed {
            intent_id,
            scope_id,
            ..
        } => {
            reads.insert(run_key);
            let effect_key = format!("effect:{run_id}:{intent_id}");
            let scope_key = format!("scope:{run_id}:{scope_id}");
            let tree_key = format!("scope-tree:{run_id}");
            reads.insert(scope_key.clone());
            writes.insert(effect_key);
            writes.insert(scope_key.clone());
            writes.insert(tree_key.clone());
            Some(tree_key)
        }
        EventPayload::EffectTransitioned { intent_id, .. } => {
            reads.insert(run_key);
            let effect_key = format!("effect:{run_id}:{intent_id}");
            reads.insert(effect_key.clone());
            writes.insert(effect_key.clone());
            Some(effect_key)
        }
        EventPayload::ScopeOpened {
            scope_id,
            parent_scope,
        } => {
            reads.insert(run_key);
            let parent_key = format!("scope:{run_id}:{parent_scope}");
            let child_key = format!("scope:{run_id}:{scope_id}");
            let tree_key = format!("scope-tree:{run_id}");
            reads.insert(parent_key.clone());
            writes.insert(parent_key.clone());
            writes.insert(child_key);
            writes.insert(tree_key.clone());
            Some(tree_key)
        }
        EventPayload::ScopeCommitted { scope_id, .. } | EventPayload::ScopeAborted { scope_id } => {
            reads.insert(run_key);
            let scope_key = format!("scope:{run_id}:{scope_id}");
            let tree_key = format!("scope-tree:{run_id}");
            reads.insert(scope_key.clone());
            writes.insert(scope_key.clone());
            writes.insert(tree_key.clone());
            Some(tree_key)
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
