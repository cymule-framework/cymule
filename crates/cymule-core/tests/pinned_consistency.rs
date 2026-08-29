//! Public pinned-reducer regressions backed by real authenticated Map/Log nodes.

use std::collections::BTreeMap;

use cymule_authenticated_collections as collections;
use cymule_core::durable_internal::{
    MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES, MachineAuthorityFrontier, MachineLogRoot,
    MachineMapRoot, MachineMaterialAdmission, MachineMaterialParentReads,
    MachinePagedFinalizeInputs, MachinePagedReadInputs, MachinePagedTransitionCurrent,
    MachinePagedTransitionPhase, MachinePhysicalRoot, MachinePinnedBatchCommand,
    MachinePinnedBatchPrecondition, MachinePinnedCommandProof, MachinePinnedRunLookup,
    MachinePreparedRootMutation, MachineRunCurrent, MachineRunIndexMembershipDelta,
    MachineRunLogAppendDelta, MachineRunLogPage, MachineRunLogSelector, MachineRunReadInputs,
    MachineRunReducerState, MachineRunRootUpdate, MachineRunRootUpdateTarget, MachineScopeCurrent,
    MachineStartRunMaterial, MachineTypedRootMutation, PinnedMachineBatchTransition,
    PinnedMachineCommandPreparation, PinnedMachineFreshPreparation, PinnedMachineRunPreparation,
    PinnedMachineTransition, PreparedPinnedCommandBatch, PreparedPinnedMachineTransition,
    PreparedPinnedPagedFinalize, machine_index_membership_value_id, machine_order_entry_value_id,
    prepare_machine_material_admission, prepare_pinned_command, prepare_pinned_command_batch,
    prepare_pinned_transition_final, prepare_pinned_transition_page,
    verify_pinned_command_batch_replay,
};
use cymule_core::{
    ArtifactRecord, AttemptProjection, Command, CommandEnvelope, CommandReceiptStatus, CoreError,
    DECLARED_FAILURE_ARTIFACT_KIND, EXECUTION_BINDING_ARTIFACT_KIND, InitialAttemptSpec,
    MAX_ARTIFACT_BYTES, MAX_ARTIFACT_RECORD_CANONICAL_BYTES, Machine, MachineCommandArchiveEntry,
    MachineCommandIndexProof, MachineRootDelta, MachineRootParts, MachineSnapshot, PlanCandidate,
    Projection, ROOT_SCOPE_ID, RUN_INPUT_ARTIFACT_KIND, RunExecutionStatus, RunFailure,
    RunFailureClass, ScopeStatus, SealedPlan, artifact_ref, canonical_bytes, canonical_digest,
    content_id, decode_json, seal_plan,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const RUN: &str = "run:pinned-consistency";
const ACTOR: &str = "actor:pinned-consistency";

fn id(label: &str) -> String {
    content_id("test.pinned-consistency/1", &label).expect("test identity derives")
}

fn plan(name: &str) -> SealedPlan {
    let mut candidate: PlanCandidate = decode_json(
        br#"{
          "ir_version":"cymule.ir/3","name":"source","entry":"main",
          "components":[],"effects":[],"definitions":[{"id":"main",
          "input_schema":{},"output_schema":{},
          "body":{"steps":[],"result":{"kind":"input"}}}],"metadata":{}
        }"#,
    )
    .expect("test Plan parses");
    name.clone_into(&mut candidate.name);
    seal_plan(candidate).expect("test Plan seals")
}

fn artifact(kind: &str, bytes: &[u8]) -> ArtifactRecord {
    ArtifactRecord {
        reference: artifact_ref(kind, bytes).expect("test Artifact identity derives"),
        bytes: bytes.to_vec(),
    }
}

/// An immutable test object store; every read follows an authenticated root.
#[derive(Default, Serialize, Deserialize)]
struct Objects {
    maps: BTreeMap<String, Vec<u8>>,
    logs: BTreeMap<String, Vec<u8>>,
    payloads: BTreeMap<String, Vec<u8>>,
    log_values: BTreeMap<String, String>,
}

impl collections::CollectionResolver for Objects {
    fn load_map_node(
        &mut self,
        object_id: &str,
    ) -> collections::Result<Option<collections::MapNode>> {
        self.maps
            .get(object_id)
            .map(|bytes| collections::decode_map_node(bytes))
            .transpose()
    }

    fn load_log_node(
        &mut self,
        object_id: &str,
    ) -> collections::Result<Option<collections::LogNode>> {
        self.logs
            .get(object_id)
            .map(|bytes| collections::decode_log_node(bytes))
            .transpose()
    }
}

impl Objects {
    fn map_value(&mut self, root: &MachineMapRoot, key: &str) -> Option<String> {
        let proof = collections::prove_map_exact(root, key, self).expect("exact Map proof");
        collections::verify_map_exact(root, key, &proof)
            .expect("exact Map proof verifies")
            .value()
            .map(str::to_owned)
    }

    fn read<T: DeserializeOwned + Serialize>(
        &mut self,
        root: &MachineMapRoot,
        key: &str,
    ) -> Option<T> {
        self.map_value(root, key).map(|object_id| {
            let value: T = decode_json(&self.payloads[&object_id]).expect("typed leaf reopens");
            assert_eq!(object_id, value_id(&value));
            value
        })
    }

    fn map_apply(
        &mut self,
        root: &MachineMapRoot,
        changes: &[collections::MapMutation],
    ) -> MachineMapRoot {
        if changes.is_empty() {
            return root.clone();
        }
        let applied = collections::apply_map_mutations(root, changes, self).expect("Map applies");
        let result = applied.verified().result().clone();
        let (_, nodes) = applied.into_parts();
        self.maps.extend(nodes.into_iter().map(|node| {
            (
                node.object_id.clone(),
                canonical_bytes(&node).expect("Map node serializes"),
            )
        }));
        result
    }

    fn put<T: Serialize>(
        &mut self,
        root: &MachineMapRoot,
        values: &BTreeMap<String, T>,
    ) -> MachineMapRoot {
        let mut changes = Vec::new();
        for (key, value) in values {
            let object_id = value_id(value);
            self.payloads.insert(
                object_id.clone(),
                canonical_bytes(value).expect("leaf bytes encode"),
            );
            match self.map_value(root, key) {
                Some(previous) if previous == object_id => {}
                Some(previous) => {
                    changes.push(collections::MapMutation::replace(key, previous, object_id));
                }
                None => changes.push(collections::MapMutation::insert(key, object_id)),
            }
        }
        self.map_apply(root, &changes)
    }

    fn membership(
        &mut self,
        root: &MachineMapRoot,
        deltas: &[MachineRunIndexMembershipDelta],
    ) -> MachineMapRoot {
        let mut result = root.clone();
        for delta in deltas {
            let mut changes = Vec::new();
            for key in &delta.inserted {
                let value = machine_index_membership_value_id(RUN, &delta.selector, key)
                    .expect("index value derives");
                changes.push(collections::MapMutation::insert(key, value));
            }
            for key in &delta.removed {
                let value = machine_index_membership_value_id(RUN, &delta.selector, key)
                    .expect("index value derives");
                changes.push(collections::MapMutation::remove(key, value));
            }
            result = self.map_apply(&result, &changes);
        }
        result
    }

    fn append(
        &mut self,
        root: &MachineLogRoot,
        deltas: &[MachineRunLogAppendDelta],
    ) -> MachineLogRoot {
        let mut values = Vec::new();
        for delta in deltas {
            for entry in &delta.values {
                let object_id = machine_order_entry_value_id(RUN, &delta.selector, entry)
                    .expect("order value derives");
                self.log_values.insert(object_id.clone(), entry.clone());
                values.push(object_id);
            }
        }
        let applied = collections::apply_log_mutations(
            root,
            &[collections::LogMutation::append(values)],
            self,
        )
        .expect("Log appends");
        let result = applied.verified().result().clone();
        let (_, nodes) = applied.into_parts();
        self.logs.extend(nodes.into_iter().map(|node| {
            (
                node.object_id.clone(),
                canonical_bytes(&node).expect("Log node serializes"),
            )
        }));
        result
    }

    fn map_mutation(&mut self, mutation: &MachinePreparedRootMutation) -> MachineMapRoot {
        let MachinePhysicalRoot::Map(parent) = mutation.parent() else {
            panic!("Map mutation requires a Map parent")
        };
        match mutation.typed() {
            MachineTypedRootMutation::PutRuns(values) => self.put(parent, values),
            MachineTypedRootMutation::PutScopes(values) => self.put(parent, values),
            MachineTypedRootMutation::PutAttempts(values) => self.put(parent, values),
            MachineTypedRootMutation::PutEffects(values) => self.put(parent, values),
            MachineTypedRootMutation::PutObligations(values) => self.put(parent, values),
            MachineTypedRootMutation::PutFacts(values) => self.put(parent, values),
            MachineTypedRootMutation::PutMaterialPlans(values) => self.put(parent, values),
            MachineTypedRootMutation::PutMaterialArtifacts(values) => self.put(parent, values),
            MachineTypedRootMutation::UpdateMembership(deltas) => self.membership(parent, deltas),
            MachineTypedRootMutation::ReserveCommand {
                command_id,
                transition_id,
            } => self.map_apply(
                parent,
                &[collections::MapMutation::insert(command_id, transition_id)],
            ),
            MachineTypedRootMutation::PutPagedTransition(transition) => self.put(
                parent,
                &BTreeMap::from([(transition.transition_id.clone(), transition.as_ref())]),
            ),
            MachineTypedRootMutation::RemoveCommandReservation {
                command_id,
                transition_id,
            } => self.map_apply(
                parent,
                &[collections::MapMutation::remove(command_id, transition_id)],
            ),
            MachineTypedRootMutation::RemovePagedTransition {
                transition_id,
                transition_digest,
            } => {
                let transition: MachinePagedTransitionCurrent = self
                    .read(parent, transition_id)
                    .expect("removed transition exists");
                assert_eq!(
                    &canonical_digest(&transition).expect("transition digest"),
                    transition_digest
                );
                self.map_apply(
                    parent,
                    &[collections::MapMutation::remove(
                        transition_id,
                        value_id(&transition),
                    )],
                )
            }
            MachineTypedRootMutation::AppendLog(_) => panic!("Log operation on a Map"),
        }
    }

    fn apply(&mut self, mutation: &MachinePreparedRootMutation) -> MachineRunRootUpdate {
        let result = if let MachineTypedRootMutation::AppendLog(deltas) = mutation.typed() {
            let MachinePhysicalRoot::Log(parent) = mutation.parent() else {
                panic!("Log mutation requires a Log parent")
            };
            let root = self.append(parent, deltas);
            assert_eq!(root.len, mutation.expected_count());
            MachinePhysicalRoot::Log(root)
        } else {
            let root = self.map_mutation(mutation);
            assert_eq!(root.entries, mutation.expected_count());
            MachinePhysicalRoot::Map(root)
        };
        mutation.bind_result(result)
    }

    fn apply_all(
        &mut self,
        mutations: &[MachinePreparedRootMutation],
    ) -> Vec<MachineRunRootUpdate> {
        mutations
            .iter()
            .map(|mutation| self.apply(mutation))
            .collect()
    }

    fn finish(&mut self, prepared: PreparedPinnedMachineTransition) -> PinnedMachineTransition {
        let updates = self.apply_all(&prepared.scope_root_mutations().expect("Scope mutations"));
        let prepared = prepared
            .finish_scope_roots(updates)
            .expect("Scope roots bind");
        let updates = self.apply_all(&prepared.run_root_mutations().expect("Run mutations"));
        let prepared = prepared.finish_run_roots(updates).expect("Run roots bind");
        let updates = self.apply_all(&prepared.global_root_mutations().expect("global mutations"));
        prepared.finish(updates).expect("global roots bind")
    }

    fn scope_page(&mut self, transition: &MachinePagedTransitionCurrent) -> MachineRunLogPage {
        let root = &transition.scope_source;
        let start = transition.next_index;
        let limit = MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES;
        let proof =
            collections::prove_log_range(root, start, limit, collections::MAX_PAGE_BYTES, self)
                .expect("Scope Log range proof");
        let verified =
            collections::verify_log_range(root, start, limit, collections::MAX_PAGE_BYTES, &proof)
                .expect("Scope Log range verifies");
        let entries = verified
            .values()
            .iter()
            .map(|object_id| self.log_values[object_id].clone())
            .collect();
        MachineRunLogPage::verify_proof(
            RUN.to_owned(),
            MachineRunLogSelector::Scopes,
            root,
            start,
            entries,
            &proof,
        )
        .expect("typed Scope order verifies")
    }
}

fn value_id<T: Serialize>(value: &T) -> String {
    content_id("test.pinned-consistency-value/1", value).expect("typed leaf identity derives")
}

#[derive(Serialize, Deserialize)]
struct Harness {
    frontier: MachineAuthorityFrontier,
    objects: Objects,
    parts: MachineRootParts,
}

struct PreparedCommand {
    batch: PreparedPinnedCommandBatch,
    fresh: PinnedMachineFreshPreparation,
    envelope: CommandEnvelope,
    generic: Machine,
}

impl Harness {
    fn new() -> Self {
        Self {
            frontier: MachineAuthorityFrontier::genesis(
                MachineMapRoot::empty(),
                MachineMapRoot::empty(),
                MachineMapRoot::empty(),
                MachineMapRoot::empty(),
            )
            .expect("genesis frontier"),
            objects: Objects::default(),
            parts: Machine::default()
                .snapshot()
                .root_parts()
                .expect("genesis parts"),
        }
    }

    fn current(&mut self) -> Option<MachineRunCurrent> {
        self.objects.read(&self.frontier.runs, RUN)
    }

    fn reopen(&mut self) {
        let bytes = canonical_bytes(self).expect("durable test state serializes");
        *self = decode_json(&bytes).expect("durable test state reopens");
        self.frontier.verify().expect("reopened frontier verifies");
        if let Some(current) = self.current() {
            current.verify().expect("reopened Run current verifies");
        }
    }

    fn generic(&self) -> Machine {
        let snapshot =
            MachineSnapshot::from_root_parts(self.parts.clone()).expect("root parts close");
        Machine::restore(snapshot).expect("complete source restores")
    }

    fn entries(&self) -> Vec<MachineCommandArchiveEntry> {
        self.parts
            .admissions
            .iter()
            .map(|admission| MachineCommandArchiveEntry {
                admission: admission.clone(),
                command: self.parts.commands[&admission.command_id].clone(),
                events: self
                    .parts
                    .events
                    .iter()
                    .filter(|event| event.command_id == admission.command_id)
                    .cloned()
                    .collect(),
            })
            .collect()
    }

    fn replay(&self) -> Projection {
        Machine::replay(
            self.parts.plans.values().cloned(),
            self.parts.artifacts.values().cloned(),
            self.parts
                .batch_admission_order
                .iter()
                .map(|batch_id| self.parts.batches[batch_id].clone()),
            self.entries(),
        )
        .expect("complete exact batch replay")
    }

    fn append_delta(&mut self, delta: &MachineRootDelta) {
        assert!(delta.removed_event_ids.is_empty());
        assert!(delta.removed_admission_ids.is_empty());
        assert!(delta.removed_command_ids.is_empty());
        assert!(delta.removed_batch_ids.is_empty());
        assert!(delta.removed_command_index_proof_ids.is_empty());
        self.parts.plans.extend(delta.plans.clone());
        self.parts
            .plan_admission_order
            .extend(delta.plan_admission_order.clone());
        self.parts.artifacts.extend(delta.artifacts.clone());
        self.parts
            .artifact_admission_order
            .extend(delta.artifact_admission_order.clone());
        self.parts.batches.extend(delta.batches.clone());
        self.parts
            .batch_admission_order
            .extend(delta.batch_admission_order.clone());
        self.parts.events.extend(delta.events.clone());
        self.parts.admissions.extend(delta.admissions.clone());
        self.parts.commands.extend(delta.commands.clone());
        self.parts
            .command_index_proofs
            .extend(delta.command_index_proofs.clone());
    }

    fn material(&mut self, source: &str, plans: Vec<SealedPlan>, artifacts: Vec<ArtifactRecord>) {
        let material = MachineMaterialAdmission::new(source.to_owned(), plans, artifacts)
            .expect("material proposal");
        let reads = MachineMaterialParentReads::new(
            material
                .plans()
                .iter()
                .map(|plan| {
                    (
                        plan.plan_id.clone(),
                        self.parts.plans.get(&plan.plan_id).cloned(),
                    )
                })
                .collect(),
            material
                .artifacts()
                .iter()
                .map(|artifact| {
                    let key = &artifact.reference.artifact_id;
                    (key.clone(), self.parts.artifacts.get(key).cloned())
                })
                .collect(),
        );
        let prepared = prepare_machine_material_admission(&self.frontier, &material, &reads)
            .expect("material admission");
        assert_eq!(
            prepared.delta.parent_authority_root,
            self.frontier.authority_root
        );
        self.append_delta(&prepared.delta);
        self.frontier = prepared.frontier;
    }

    fn prepare(
        &mut self,
        command_id: &str,
        command: Command,
        start: Option<MachineStartRunMaterial>,
    ) -> PreparedCommand {
        let mut generic = self.generic();
        if let Some(material) = &start {
            for plan in material.admission().plans() {
                generic
                    .insert_plan(plan.clone())
                    .expect("generic Plan stages");
            }
            for artifact in material.admission().artifacts() {
                let reference = generic
                    .put_artifact(artifact.reference.kind.clone(), artifact.bytes.clone())
                    .expect("generic Artifact stages");
                assert_eq!(reference, artifact.reference);
            }
        }
        let precondition = self.current().map(|run| run.precondition_token());
        let mut batch = prepare_pinned_command_batch(
            &self.frontier,
            vec![MachinePinnedBatchCommand {
                command_id: command_id.to_owned(),
                actor: ACTOR.to_owned(),
                run_id: RUN.to_owned(),
                precondition: MachinePinnedBatchPrecondition::Parent(precondition),
                command,
            }],
            None,
        )
        .expect("batch prepares");
        let envelope = batch.next_envelope().expect("exact batch envelope");
        let proof = MachinePinnedCommandProof::vacant(
            MachineCommandIndexProof::empty_nonmembership(command_id).expect("absence proof"),
        );
        let PinnedMachineCommandPreparation::Lookup(lookup) =
            batch.prepare_next(&proof).expect("fresh command lookup")
        else {
            panic!("new command needs its Run")
        };
        let frontier = batch.current_frontier();
        let revision = id(command_id);
        let current = self.current();
        let PinnedMachineRunPreparation::Reads(read) = lookup
            .resolve_run(MachinePinnedRunLookup::new(
                revision.clone(),
                RUN.to_owned(),
                frontier.runs.clone(),
                current.clone(),
            ))
            .expect("exact Run resolves")
        else {
            panic!("fresh Run lookup needs typed reads")
        };
        let mut input = empty_inputs(frontier, revision, current, start);
        self.command_inputs(&envelope.command, &mut input);
        PreparedCommand {
            batch,
            fresh: read.prepare(input).expect("fresh command prepares"),
            envelope,
            generic,
        }
    }

    fn command_inputs(&mut self, command: &Command, input: &mut MachineRunReadInputs) {
        match command {
            Command::StartRun {
                plan_id,
                binding_context,
                input: artifact,
                ..
            } => {
                input.new_run_empty_root = Some(MachineMapRoot::empty());
                input.new_run_empty_log = Some(MachineLogRoot::empty());
                input
                    .plans
                    .insert(plan_id.clone(), self.parts.plans.get(plan_id).cloned());
                for key in [binding_context, &artifact.artifact_id] {
                    input
                        .artifacts
                        .insert(key.clone(), self.parts.artifacts.get(key).cloned());
                }
            }
            Command::YieldAttempt { attempt_id, .. } => {
                let root = &input.run.as_ref().expect("existing Run").children.attempts;
                input
                    .attempts
                    .insert(attempt_id.clone(), self.objects.read(root, attempt_id));
            }
            Command::MigrateRun {
                from_plan,
                to_plan,
                from_binding,
                to_binding,
                ..
            } => {
                for key in [from_plan, to_plan] {
                    input
                        .plans
                        .insert(key.clone(), self.parts.plans.get(key).cloned());
                }
                for key in [from_binding, to_binding] {
                    input
                        .artifacts
                        .insert(key.clone(), self.parts.artifacts.get(key).cloned());
                }
            }
            Command::CancelRun { reason } => {
                input.artifacts.insert(
                    reason.artifact_id.clone(),
                    self.parts.artifacts.get(&reason.artifact_id).cloned(),
                );
            }
            Command::FailRun { failure } => {
                input.artifacts.insert(
                    failure.detail.artifact_id.clone(),
                    self.parts
                        .artifacts
                        .get(&failure.detail.artifact_id)
                        .cloned(),
                );
            }
            other => panic!("unsupported test command {other:?}"),
        }
    }

    fn admit(&mut self, result: &PinnedMachineBatchTransition) {
        assert_eq!(
            result.machine.parent_authority_root,
            self.frontier.authority_root
        );
        assert_eq!(
            result.machine.result_authority_root,
            result.frontier.authority_root
        );
        assert_eq!(
            result.batch.result_authority_root,
            result.frontier.authority_root
        );
        self.append_delta(&result.machine);
        self.frontier = result.frontier.clone();
    }

    fn commit(&mut self, prepared: PreparedCommand) -> PinnedMachineBatchTransition {
        let PreparedCommand {
            batch,
            fresh,
            envelope,
            mut generic,
        } = prepared;
        let intrinsic_start = matches!(envelope.command, Command::StartRun { .. });
        let receipt = generic.submit(envelope).expect("generic command admits");
        let PinnedMachineFreshPreparation::Prepared(step) = fresh else {
            panic!("ordinary command expected")
        };
        let finished = self.objects.finish(*step);
        let result = batch
            .accept_step(finished)
            .expect("batch accepts step")
            .finish()
            .expect("batch finalizes");
        assert_eq!(result.batch.receipts, [receipt]);
        self.admit(&result);
        // Generic singleton StartRun retains an explicit outer material source;
        // this pinned fixture uses the intrinsic source, so its batch differs.
        // Every later command starts from the exact same pinned batch history.
        if !intrinsic_start {
            self.assert_same_authority(&generic);
        }
        self.assert_matches(&generic);
        result
    }

    fn assert_same_authority(&self, generic: &Machine) {
        assert_eq!(
            self.frontier.authority_root,
            generic.authority_root().expect("generic root")
        );
    }

    fn assert_matches(&mut self, generic: &Machine) {
        self.assert_same_authority(&self.generic());
        assert_eq!(self.frontier.projection_root, generic.projection_root());
        assert_eq!(&self.replay(), generic.projection());
        let current = self.current().expect("Run current exists");
        let run = &generic.projection().runs[RUN];
        assert_eq!(current.epoch, run.epoch);
        assert_eq!(current.current_plan, run.current_plan);
        assert_eq!(current.current_binding_context, run.current_binding_context);
        assert_eq!(current.execution_status, run.execution_status);
        assert_eq!(current.precondition_token(), run.precondition_token());
        for (key, expected) in &run.attempts {
            assert_eq!(
                self.objects
                    .read::<AttemptProjection>(&current.children.attempts, key),
                Some(expected.clone())
            );
        }
    }

    fn start(&mut self, bytes: &[u8]) -> PinnedMachineBatchTransition {
        let plan = plan("source");
        let binding = artifact(EXECUTION_BINDING_ARTIFACT_KIND, b"source-binding");
        let input = artifact(RUN_INPUT_ARTIFACT_KIND, bytes);
        let material = MachineStartRunMaterial::new(
            "start".to_owned(),
            plan.clone(),
            binding.clone(),
            input.clone(),
        )
        .expect("StartRun material");
        let command = Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: binding.reference.artifact_id.clone(),
            input: input.reference,
            material_digest: material.material_digest().to_owned(),
            initial_attempt: InitialAttemptSpec {
                attempt_id: id("attempt"),
                continuation_id: id("continuation"),
                occurrence_binding: binding.reference.artifact_id,
                continuation_epoch: 0,
                execution_fence: 1,
            },
        };
        let prepared = self.prepare("start", command, Some(material));
        self.commit(prepared)
    }
}

fn empty_inputs(
    frontier: &MachineAuthorityFrontier,
    revision: String,
    run: Option<MachineRunCurrent>,
    start_material: Option<MachineStartRunMaterial>,
) -> MachineRunReadInputs {
    MachineRunReadInputs {
        machine_revision: revision,
        run_id: RUN.to_owned(),
        runs_root: frontier.runs.clone(),
        facts_root: frontier.facts.clone(),
        run,
        new_run_empty_root: None,
        new_run_empty_log: None,
        plans: BTreeMap::new(),
        artifacts: BTreeMap::new(),
        scopes: BTreeMap::new(),
        scope_locations: BTreeMap::new(),
        effects: BTreeMap::new(),
        obligations: BTreeMap::new(),
        attempts: BTreeMap::new(),
        facts: BTreeMap::new(),
        start_material,
        index_pages: Vec::new(),
        log_pages: Vec::new(),
    }
}

#[derive(Clone, Copy)]
enum Termination {
    Cancel,
    Fail,
}

impl Termination {
    fn command(self, harness: &mut Harness) -> Command {
        let kind = match self {
            Self::Cancel => "test.cancel-reason/1",
            Self::Fail => DECLARED_FAILURE_ARTIFACT_KIND,
        };
        let evidence = artifact(kind, b"{}");
        harness.material("terminal-material", Vec::new(), vec![evidence.clone()]);
        match self {
            Self::Cancel => Command::CancelRun {
                reason: evidence.reference,
            },
            Self::Fail => Command::FailRun {
                failure: RunFailure {
                    class: RunFailureClass::DeclaredFailure,
                    code: "expected".to_owned(),
                    detail: evidence.reference,
                },
            },
        }
    }
}

struct TerminalCase {
    harness: Harness,
    envelope: CommandEnvelope,
    generic: Machine,
    transition: MachinePagedTransitionCurrent,
}

impl TerminalCase {
    fn begin(action: Termination) -> Self {
        let mut harness = Harness::new();
        harness.start(b"{}");
        let command = action.command(&mut harness);
        let PreparedCommand {
            batch,
            fresh,
            envelope,
            generic,
        } = harness.prepare("terminate", command, None);
        let source = harness.frontier.clone();
        let PinnedMachineFreshPreparation::PagedBegin(begin) = fresh else {
            panic!("Run termination must enter the formal paged protocol")
        };
        let material = batch
            .into_paged_begin(*begin)
            .expect("paged batch material");
        let updates = harness
            .objects
            .apply_all(material.root_mutations().expect("material roots"));
        let begin = material.finish(updates).expect("paged material roots bind");
        let updates = harness
            .objects
            .apply_all(begin.root_mutations().expect("begin roots"));
        let begin = begin.finish(updates).expect("begin roots bind");
        assert_eq!(begin.transition.phase, MachinePagedTransitionPhase::Scopes);
        assert_eq!(begin.frontier.authority_root, source.authority_root);
        assert_eq!(begin.frontier.event_count, source.event_count);
        assert_eq!(begin.frontier.batch_count, source.batch_count);
        assert_eq!(begin.frontier.pending_commands.entries, 1);
        assert_eq!(begin.frontier.paged_transitions.entries, 1);
        harness.frontier = begin.frontier;
        harness.reopen();
        let mut case = Self {
            harness,
            envelope,
            generic,
            transition: begin.transition,
        };
        case.reload_pending();
        assert_eq!(case.harness.current(), Some(begin.fenced_run));
        case.assert_live_attempt_active();
        case
    }

    fn reload_pending(&mut self) {
        let transition_id = self
            .harness
            .objects
            .map_value(
                &self.harness.frontier.pending_commands,
                &self.envelope.command_id,
            )
            .expect("authenticated command reservation");
        let transition: MachinePagedTransitionCurrent = self
            .harness
            .objects
            .read(&self.harness.frontier.paged_transitions, &transition_id)
            .expect("authenticated pending transition");
        transition.verify().expect("pending transition verifies");
        assert_eq!(transition, self.transition);
        let before = canonical_bytes(&self.harness).expect("before pending retry");
        let result = prepare_pinned_command(
            &self.harness.frontier,
            &MachinePinnedCommandProof::pending(transition.clone()),
            self.envelope.clone(),
        )
        .expect("same pending command resolves");
        let PinnedMachineCommandPreparation::Pending(replayed) = result else {
            panic!("same command must resume its original transition before reading a Run")
        };
        assert_eq!(*replayed, transition);
        assert_eq!(
            before,
            canonical_bytes(&self.harness).expect("after pending retry")
        );
        self.transition = transition;
    }

    fn assert_live_attempt_active(&mut self) {
        let current = self.harness.current().expect("live Run");
        assert_eq!(current.execution_status, RunExecutionStatus::Active);
        assert_eq!(current.epoch, 0);
        assert_eq!(current.active_attempt_id, Some(id("attempt")));
        assert!(matches!(
            current.reducer_state,
            MachineRunReducerState::Transitioning { .. }
        ));
        let attempt: AttemptProjection = self
            .harness
            .objects
            .read(&current.children.attempts, &id("attempt"))
            .expect("live Attempt");
        assert!(attempt.active);
    }

    fn page_scopes(&mut self) {
        let before = self.harness.frontier.clone();
        let parts = self.harness.parts.clone();
        let page = self.harness.objects.scope_page(&self.transition);
        assert_eq!(page.entries(), [ROOT_SCOPE_ID]);
        let scopes = page
            .entries()
            .iter()
            .map(|key| {
                let scope: MachineScopeCurrent = self
                    .harness
                    .objects
                    .read(&self.transition.shadow.children.scopes, key)
                    .expect("authenticated shadow Scope");
                (key.clone(), scope)
            })
            .collect();
        let inputs = MachinePagedReadInputs::new(
            self.harness.current().expect("fenced Run"),
            page,
            scopes,
            BTreeMap::new(),
            BTreeMap::new(),
        );
        let prepared = prepare_pinned_transition_page(&before, &self.transition, &inputs)
            .expect("Scope page prepares");
        let updates = self
            .harness
            .objects
            .apply_all(prepared.shadow_root_mutations().expect("shadow roots"));
        let prepared = prepared
            .finish_shadow_roots(updates)
            .expect("shadow roots bind");
        let update = self
            .harness
            .objects
            .apply(prepared.transition_root_mutation().expect("cursor root"));
        let progress = prepared.finish(update).expect("cursor root binds");
        assert_eq!(
            progress.transition.phase,
            MachinePagedTransitionPhase::Finalize
        );
        assert_eq!(progress.transition.processed_count, 1);
        assert_eq!(progress.frontier.authority_root, before.authority_root);
        assert_eq!(progress.frontier.event_count, before.event_count);
        assert_eq!(progress.frontier.batch_count, before.batch_count);
        assert_eq!(self.harness.parts, parts);
        self.harness.frontier = progress.frontier;
        self.transition = progress.transition;
        self.harness.reopen();
        self.reload_pending();
        self.assert_live_attempt_active();
        let shadow_scope: MachineScopeCurrent = self
            .harness
            .objects
            .read(&self.transition.shadow.children.scopes, ROOT_SCOPE_ID)
            .expect("final shadow Scope");
        assert_eq!(shadow_scope.status, ScopeStatus::ClosedAborted);
        let live = self.harness.current().expect("live Run");
        let live_scope: MachineScopeCurrent = self
            .harness
            .objects
            .read(&live.children.scopes, ROOT_SCOPE_ID)
            .expect("live Scope");
        assert_eq!(live_scope.status, ScopeStatus::Open);
    }

    fn active_attempt(&mut self) -> AttemptProjection {
        let live = self.harness.current().expect("fenced Run");
        self.harness
            .objects
            .read(&live.children.attempts, &id("attempt"))
            .expect("active Attempt leaf")
    }

    fn prepare_final(
        &mut self,
        attempt: Option<AttemptProjection>,
    ) -> cymule_core::Result<PreparedPinnedPagedFinalize> {
        let input = MachinePagedFinalizeInputs::new(
            self.harness.current().expect("fenced Run"),
            BTreeMap::new(),
            attempt,
            MachineCommandIndexProof::empty_nonmembership(&self.envelope.command_id)
                .expect("final non-membership proof"),
            None,
        );
        prepare_pinned_transition_final(&self.harness.frontier, &self.transition, input)
    }

    fn finalize(&mut self) -> PinnedMachineBatchTransition {
        let attempt = self.active_attempt();
        let prepared = self
            .prepare_final(Some(attempt))
            .expect("active Attempt finalization prepares");
        let plans = prepared
            .shadow_root_mutations()
            .expect("final shadow roots");
        let attempt_plan = plans
            .iter()
            .find(|plan| plan.target() == &MachineRunRootUpdateTarget::Attempts)
            .expect("finalization must persist the terminal Attempt");
        let MachineTypedRootMutation::PutAttempts(attempts) = attempt_plan.typed() else {
            panic!("Attempt root must be a typed PutAttempts")
        };
        assert_eq!(attempts.len(), 1);
        assert!(!attempts[&id("attempt")].active);
        assert_eq!(attempt_plan.expected_count(), 1);
        assert_eq!(
            attempt_plan.parent(),
            &MachinePhysicalRoot::Map(self.transition.shadow.children.attempts.clone())
        );
        let updates = self.harness.objects.apply_all(plans);
        let prepared = prepared
            .finish_shadow_roots(updates)
            .expect("final Attempt roots bind");
        self.assert_live_attempt_active();
        let updates = self
            .harness
            .objects
            .apply_all(prepared.root_mutations().expect("publish roots"));
        let result = prepared.finish(updates).expect("terminal roots publish");
        assert_eq!(
            result.batch.batch_id,
            self.transition.batch_manifest.batch_id
        );
        assert_eq!(
            result.batch.parent_authority_root,
            self.transition.batch_manifest.parent_authority_root
        );
        let receipt = self
            .generic
            .submit(self.envelope.clone())
            .expect("generic termination admits");
        assert_eq!(result.batch.receipts, [receipt]);
        self.harness.admit(&result);
        self.harness.reopen();
        self.assert_terminal(&result);
        self.harness.assert_same_authority(&self.generic);
        self.harness.assert_matches(&self.generic);
        result
    }

    fn assert_terminal(&mut self, result: &PinnedMachineBatchTransition) {
        assert_eq!(
            self.harness.frontier.pending_commands,
            MachineMapRoot::empty()
        );
        assert_eq!(
            self.harness.frontier.paged_transitions,
            MachineMapRoot::empty()
        );
        assert_eq!(self.harness.frontier.event_count, 3);
        assert_eq!(self.harness.frontier.batch_count, 3);
        let run = self.harness.current().expect("terminal Run");
        assert_eq!(run.epoch, 1);
        assert_eq!(run.active_attempt_id, None);
        assert_eq!(run.reducer_state, MachineRunReducerState::Ready);
        assert_eq!(run.indexes.open_scopes, MachineMapRoot::empty());
        let attempt: AttemptProjection = self
            .harness
            .objects
            .read(&run.children.attempts, &id("attempt"))
            .expect("terminal Attempt");
        assert!(!attempt.active);
        assert!(attempt.continuation_epoch < run.epoch);
        assert_eq!(attempt.execution_fence, 1);
        let scope: MachineScopeCurrent = self
            .harness
            .objects
            .read(&run.children.scopes, ROOT_SCOPE_ID)
            .expect("terminal root Scope");
        assert_eq!(scope.status, ScopeStatus::ClosedAborted);
        assert_eq!(result.machine.events.len(), 1);
        assert_eq!(
            result.batch.receipts[0].status,
            CommandReceiptStatus::Applied
        );
        assert_eq!(
            result.batch.receipts[0].current_precondition,
            Some(run.precondition_token())
        );
    }

    fn assert_lost_response_replay(&mut self, result: &PinnedMachineBatchTransition) {
        let before = canonical_bytes(&self.harness).expect("before lost-response retry");
        let record = self.harness.parts.commands[&self.envelope.command_id].clone();
        let proof = MachinePinnedCommandProof::retained(
            record.clone(),
            result.machine.admissions[0].clone(),
            self.harness.parts.command_index_proofs[&self.envelope.command_id].clone(),
        );
        let prepared =
            prepare_pinned_command(&self.harness.frontier, &proof, self.envelope.clone())
                .expect("retained command resolves");
        let PinnedMachineCommandPreparation::Replay(replay) = prepared else {
            panic!("an admitted retry must return before reading the Run")
        };
        assert_eq!(replay.receipt, result.batch.receipts[0]);
        assert_eq!(replay.frontier, self.harness.frontier);
        let request = MachinePinnedBatchCommand {
            command_id: self.envelope.command_id.clone(),
            actor: self.envelope.actor.clone(),
            run_id: self.envelope.run_id.clone(),
            precondition: MachinePinnedBatchPrecondition::Parent(
                self.envelope.expected_precondition.clone(),
            ),
            command: self.envelope.command.clone(),
        };
        let receipts =
            verify_pinned_command_batch_replay(&result.batch, &[request], &[record], None)
                .expect("the original complete batch replays");
        assert_eq!(receipts, result.batch.receipts);
        let mut changed = self.envelope.clone();
        "actor:other".clone_into(&mut changed.actor);
        assert!(matches!(
            prepare_pinned_command(&self.harness.frontier, &proof, changed),
            Err(CoreError::CommandReuse(_))
        ));
        assert_eq!(
            before,
            canonical_bytes(&self.harness).expect("after lost-response retry")
        );
    }
}

#[test]
fn pinned_migration_advances_epoch_and_matches_generic_admission_and_replay() {
    let mut harness = Harness::new();
    let start = harness.start(b"{}");
    assert_eq!(start.batch.event_ids.len(), 2);
    harness.reopen();
    let prepared = harness.prepare(
        "yield",
        Command::YieldAttempt {
            attempt_id: id("attempt"),
            continuation_epoch: 0,
            execution_fence: 1,
        },
        None,
    );
    harness.commit(prepared);
    harness.reopen();
    let target = plan("target");
    let binding = artifact(EXECUTION_BINDING_ARTIFACT_KIND, b"target-binding");
    harness.material(
        "migration-material",
        vec![target.clone()],
        vec![binding.clone()],
    );
    harness.reopen();
    let source = harness.current().expect("source Run");
    assert_eq!(source.epoch, 0);
    assert_eq!(source.active_attempt_id, None);
    assert_eq!(harness.replay().runs[RUN].epoch, 0);
    let prepared = harness.prepare(
        "migrate",
        Command::MigrateRun {
            from_plan: source.current_plan,
            to_plan: target.plan_id.clone(),
            from_binding: source.current_binding_context,
            to_binding: binding.reference.artifact_id.clone(),
            safe_point_id: id("safe-point"),
            target_epoch: 1,
            target_continuation_digest: canonical_digest(&"target-continuation")
                .expect("target digest"),
        },
        None,
    );
    let result = harness.commit(prepared);
    harness.reopen();
    let current = harness.current().expect("migrated Run");
    assert_eq!(current.epoch, 1);
    assert_eq!(current.current_plan, target.plan_id);
    assert_eq!(
        current.current_binding_context,
        binding.reference.artifact_id
    );
    assert_eq!(current.plan_lineage_count, 2);
    assert_eq!(current.binding_lineage_count, 2);
    assert_eq!(
        result.batch.receipts[0].current_precondition,
        Some(current.precondition_token())
    );
    assert_eq!(harness.replay().runs[RUN].epoch, 1);
}

#[test]
fn pinned_cancellation_finalizes_active_attempt_after_every_pending_reopen() {
    let mut case = TerminalCase::begin(Termination::Cancel);
    case.page_scopes();
    case.finalize();
    assert!(matches!(
        case.harness
            .current()
            .expect("cancelled Run")
            .execution_status,
        RunExecutionStatus::Cancelled { .. }
    ));
}

#[test]
fn pinned_failure_finalizes_active_attempt_after_every_pending_reopen() {
    let mut case = TerminalCase::begin(Termination::Fail);
    case.page_scopes();
    case.finalize();
    assert!(matches!(
        case.harness.current().expect("failed Run").execution_status,
        RunExecutionStatus::Failed { .. }
    ));
}

#[test]
fn paged_termination_rejects_missing_or_wrong_active_attempt_without_writes() {
    for action in [Termination::Cancel, Termination::Fail] {
        let mut case = TerminalCase::begin(action);
        case.page_scopes();
        let correct = case.active_attempt();
        let mut wrong_identity = correct.clone();
        wrong_identity.attempt_id = id("other-attempt");
        let mut inactive = correct;
        inactive.active = false;
        let before = canonical_bytes(&case.harness).expect("before rejected finalization");
        for invalid in [None, Some(wrong_identity), Some(inactive)] {
            assert!(matches!(
                case.prepare_final(invalid),
                Err(CoreError::PinnedReadSetIncomplete {
                    family: "Machine paged final active Attempt",
                    ..
                })
            ));
            assert_eq!(
                before,
                canonical_bytes(&case.harness).expect("after rejected finalization")
            );
        }
        case.finalize();
    }
}

#[test]
fn paged_termination_replays_original_pending_and_admitted_batch_without_writes() {
    for action in [Termination::Cancel, Termination::Fail] {
        let mut case = TerminalCase::begin(action);
        case.page_scopes();
        let result = case.finalize();
        case.assert_lost_response_replay(&result);
    }
}

#[test]
fn five_mebibyte_canonical_input_admits_start_and_exact_batch_replay() {
    let bytes = canonical_bytes(&"a".repeat(5 * 1024 * 1024)).expect("canonical JSON string");
    let value: serde_json::Value = decode_json(&bytes).expect("strict canonical input");
    assert_eq!(canonical_bytes(&value).expect("input reencodes"), bytes);
    let input = artifact(RUN_INPUT_ARTIFACT_KIND, &bytes);
    input
        .validate()
        .expect("large input remains within Artifact bounds");
    assert!(input.bytes.len() < MAX_ARTIFACT_BYTES);
    assert!(
        canonical_bytes(&input).expect("encoded input").len() < MAX_ARTIFACT_RECORD_CANONICAL_BYTES
    );
    let mut harness = Harness::new();
    let result = harness.start(&bytes);
    assert_eq!(result.batch.event_ids.len(), 2);
    assert_eq!(result.batch.receipts[0].event_ids, result.batch.event_ids);
    assert_eq!(harness.parts.artifacts[&input.reference.artifact_id], input);
    harness.reopen();
    let projection = harness.replay();
    assert_eq!(projection.runs[RUN].epoch, 0);
    assert!(projection.runs[RUN].attempts[&id("attempt")].active);
    assert_eq!(
        result.batch.result_authority_root,
        harness.generic().authority_root().expect("reopened root")
    );
}
