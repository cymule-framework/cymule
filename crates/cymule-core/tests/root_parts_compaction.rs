//! Public regression tests for batch-root closure and causally closed compaction.

use std::collections::BTreeMap;

use cymule_core::durable_internal::{
    MachineAuthorityFrontier, MachineCompactionIntent, MachineLogRoot, MachineMapRoot,
    MachineMaterialAdmission, MachineMaterialParentReads, MachinePagedFinalizeInputs,
    MachinePhysicalRoot, MachinePinnedBatchCommand, MachinePinnedBatchPrecondition,
    MachinePinnedCommandProof, MachinePinnedRunLookup, MachinePreparedRootMutation,
    MachineRunCurrent, MachineRunReadInputs, MachineRunRootUpdate, MachineScopeCurrent,
    MachineStartRunMaterial, PinnedMachineBatchTransition, PinnedMachineCommandPreparation,
    PinnedMachineFreshPreparation, PinnedMachinePagedBegin, PinnedMachineRunPreparation,
    PinnedMachineTransition, PreparedMachineMaterialAdmission, PreparedPinnedMachineCompaction,
    PreparedPinnedMachineTransition, prepare_machine_material_admission, prepare_pinned_command,
    prepare_pinned_command_batch, prepare_pinned_compaction, prepare_pinned_transition_final,
};
use cymule_core::{
    ArtifactRecord, Command, CoreError, EXECUTION_BINDING_ARTIFACT_KIND, InitialAttemptSpec,
    Machine, MachineCommandArchiveSegment, MachineCommandIndexProof, MachineDelta,
    MachineRootDelta, MachineRootParts, MachineSnapshot, PlanCandidate, ROOT_SCOPE_ID,
    RUN_INPUT_ARTIFACT_KIND, SealedPlan, artifact_ref, content_id, decode_json, seal_plan,
};

const RUN_A: &str = "run:cut-a";
const RUN_B: &str = "run:cut-b";

fn id(label: &str) -> String {
    content_id("test.compaction-causality/1", &label).expect("test identity derives")
}

fn artifact(kind: &str, bytes: &[u8]) -> ArtifactRecord {
    ArtifactRecord {
        reference: artifact_ref(kind, bytes).expect("test Artifact derives"),
        bytes: bytes.to_vec(),
    }
}

fn genesis_frontier() -> MachineAuthorityFrontier {
    MachineAuthorityFrontier::genesis(
        MachineMapRoot::empty(),
        MachineMapRoot::empty(),
        MachineMapRoot::empty(),
        MachineMapRoot::empty(),
    )
    .expect("genesis frontier")
}

fn material_at(
    frontier: &MachineAuthorityFrontier,
    label: &str,
) -> PreparedMachineMaterialAdmission {
    let material = MachineMaterialAdmission::new(
        format!("profile:{label}"),
        Vec::new(),
        vec![artifact("test.material/1", label.as_bytes())],
    )
    .expect("material proposal");
    let reads = MachineMaterialParentReads::new(
        BTreeMap::new(),
        material
            .artifacts()
            .iter()
            .map(|record| (record.reference.artifact_id.clone(), None))
            .collect(),
    );
    prepare_machine_material_admission(frontier, &material, &reads).expect("material batch")
}

fn physical_result(plan: &MachinePreparedRootMutation) -> MachineRunRootUpdate {
    let count = plan.expected_count();
    let node = (count != 0).then(|| {
        content_id(
            "test.compaction-physical/1",
            &(plan.target(), plan.mutation_digest(), count),
        )
        .expect("test physical root derives")
    });
    let result = match plan.parent() {
        MachinePhysicalRoot::Map(_) => MachinePhysicalRoot::Map(MachineMapRoot {
            node,
            entries: count,
        }),
        MachinePhysicalRoot::Log(_) => MachinePhysicalRoot::Log(MachineLogRoot {
            ordered_root: node
                .clone()
                .unwrap_or_else(|| MachineLogRoot::empty().ordered_root),
            node,
            len: count,
            height: u8::from(count != 0),
        }),
    };
    plan.bind_result(result)
}

fn finish(prepared: PreparedPinnedMachineTransition) -> PinnedMachineTransition {
    let updates = prepared
        .scope_root_mutations()
        .expect("Scope roots")
        .iter()
        .map(physical_result)
        .collect();
    let run = prepared
        .finish_scope_roots(updates)
        .expect("Scope roots bind");
    let updates = run
        .run_root_mutations()
        .expect("Run roots")
        .iter()
        .map(physical_result)
        .collect();
    let global = run.finish_run_roots(updates).expect("Run roots bind");
    let updates = global
        .global_root_mutations()
        .expect("global roots")
        .iter()
        .map(physical_result)
        .collect();
    global.finish(updates).expect("global roots bind")
}

fn empty_inputs(
    frontier: &MachineAuthorityFrontier,
    run_id: &str,
    run: Option<&MachineRunCurrent>,
    revision: &str,
) -> MachineRunReadInputs {
    MachineRunReadInputs {
        machine_revision: id(revision),
        run_id: run_id.to_owned(),
        runs_root: frontier.runs.clone(),
        facts_root: frontier.facts.clone(),
        run: run.cloned(),
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
        start_material: None,
        index_pages: Vec::new(),
        log_pages: Vec::new(),
    }
}

struct Seed {
    plan: SealedPlan,
    binding: ArtifactRecord,
    input: ArtifactRecord,
}

impl Seed {
    fn new() -> Self {
        let candidate: PlanCandidate = decode_json(br#"{"ir_version":"cymule.ir/3","name":"cut_test","entry":"main","components":[],"effects":[],"definitions":[{"id":"main","input_schema":true,"output_schema":true,"body":{"steps":[],"result":{"kind":"input"}}}],"metadata":{}}"#)
            .expect("test Plan decodes");
        Self {
            plan: seal_plan(candidate).expect("test Plan seals"),
            binding: artifact(EXECUTION_BINDING_ARTIFACT_KIND, b"test binding"),
            input: artifact(RUN_INPUT_ARTIFACT_KIND, b"{}"),
        }
    }

    fn start(
        &self,
        frontier: &MachineAuthorityFrontier,
        run_id: &str,
        reused: bool,
    ) -> PinnedMachineBatchTransition {
        let command_id = format!("command:start:{run_id}");
        let material = MachineStartRunMaterial::new(
            command_id.clone(),
            self.plan.clone(),
            self.binding.clone(),
            self.input.clone(),
        )
        .expect("start material");
        let command = MachinePinnedBatchCommand {
            command_id: command_id.clone(),
            actor: "actor:cut-test".to_owned(),
            run_id: run_id.to_owned(),
            precondition: MachinePinnedBatchPrecondition::Parent(None),
            command: Command::StartRun {
                plan_id: self.plan.plan_id.clone(),
                binding_context: self.binding.reference.artifact_id.clone(),
                input: self.input.reference.clone(),
                material_digest: material.material_digest().to_owned(),
                initial_attempt: InitialAttemptSpec {
                    attempt_id: id(&format!("attempt:{run_id}")),
                    continuation_id: id(&format!("continuation:{run_id}")),
                    occurrence_binding: self.binding.reference.artifact_id.clone(),
                    continuation_epoch: 0,
                    execution_fence: 1,
                },
            },
        };
        let batch =
            prepare_pinned_command_batch(frontier, vec![command], None).expect("start batch");
        let source = batch.current_frontier();
        let envelope = batch.next_envelope().expect("start envelope");
        let proof = MachinePinnedCommandProof::vacant(
            MachineCommandIndexProof::empty_nonmembership(&command_id).expect("absence"),
        );
        let PinnedMachineCommandPreparation::Lookup(lookup) =
            prepare_pinned_command(source, &proof, envelope).expect("start lookup")
        else {
            panic!("fresh lookup")
        };
        let PinnedMachineRunPreparation::Reads(read) = lookup
            .resolve_run(MachinePinnedRunLookup::new(
                id(&command_id),
                run_id.to_owned(),
                source.runs.clone(),
                None,
            ))
            .expect("Run absence")
        else {
            panic!("fresh reads")
        };
        let mut inputs = empty_inputs(source, run_id, None, &command_id);
        inputs.new_run_empty_root = Some(MachineMapRoot::empty());
        inputs.new_run_empty_log = Some(MachineLogRoot::empty());
        inputs
            .plans
            .insert(self.plan.plan_id.clone(), reused.then(|| self.plan.clone()));
        inputs.artifacts.insert(
            self.binding.reference.artifact_id.clone(),
            reused.then(|| self.binding.clone()),
        );
        inputs.artifacts.insert(
            self.input.reference.artifact_id.clone(),
            reused.then(|| self.input.clone()),
        );
        inputs.start_material = Some(material);
        let PinnedMachineFreshPreparation::Prepared(prepared) =
            read.prepare(inputs).expect("start reduces")
        else {
            panic!("start is not paged")
        };
        batch
            .accept_step(finish(*prepared))
            .expect("start step")
            .finish()
            .expect("start batch closes")
    }
}

fn append_parts(parts: &mut MachineRootParts, delta: &MachineRootDelta) {
    assert!(delta.removed_event_ids.is_empty());
    parts.plans.extend(delta.plans.clone());
    parts
        .plan_admission_order
        .extend(delta.plan_admission_order.clone());
    parts.artifacts.extend(delta.artifacts.clone());
    parts
        .artifact_admission_order
        .extend(delta.artifact_admission_order.clone());
    parts.batches.extend(delta.batches.clone());
    parts
        .batch_admission_order
        .extend(delta.batch_admission_order.clone());
    parts.events.extend(delta.events.clone());
    parts.admissions.extend(delta.admissions.clone());
    parts.commands.extend(delta.commands.clone());
    parts
        .command_index_proofs
        .extend(delta.command_index_proofs.clone());
}

struct History {
    seed: Seed,
    parts: MachineRootParts,
    frontier: MachineAuthorityFrontier,
    run_a: MachineRunCurrent,
    scope_a: MachineScopeCurrent,
}

impl History {
    fn new() -> Self {
        Self::start_from(
            Machine::new()
                .snapshot()
                .root_parts()
                .expect("empty root parts"),
            &genesis_frontier(),
        )
    }

    fn start_from(mut parts: MachineRootParts, frontier: &MachineAuthorityFrontier) -> Self {
        let seed = Seed::new();
        let start = seed.start(frontier, RUN_A, false);
        append_parts(&mut parts, &start.machine);
        let run = start.steps[0].run.as_ref().expect("Run delta");
        Self {
            seed,
            parts,
            frontier: start.frontier,
            run_a: run.result_current.clone(),
            scope_a: run.scopes[ROOT_SCOPE_ID].clone(),
        }
    }

    fn material(&mut self, label: &str) {
        let admitted = material_at(&self.frontier, label);
        append_parts(&mut self.parts, &admitted.delta);
        self.frontier = admitted.frontier;
    }

    fn another_run(&mut self) {
        let start = self.seed.start(&self.frontier, RUN_B, true);
        append_parts(&mut self.parts, &start.machine);
        self.frontier = start.frontier;
    }

    fn record_fact(&mut self, key: &str) {
        let command_id = format!("command:{key}");
        let mut batch = prepare_pinned_command_batch(
            &self.frontier,
            vec![MachinePinnedBatchCommand {
                command_id: command_id.clone(),
                actor: "actor:cut-test".to_owned(),
                run_id: RUN_A.to_owned(),
                precondition: MachinePinnedBatchPrecondition::Parent(Some(
                    self.run_a.precondition_token(),
                )),
                command: Command::RecordFact {
                    key: key.to_owned(),
                    value: id(key),
                },
            }],
            None,
        )
        .expect("fact batch");
        let proof = MachinePinnedCommandProof::vacant(
            MachineCommandIndexProof::empty_nonmembership(&command_id).expect("fact absence"),
        );
        let PinnedMachineCommandPreparation::Lookup(lookup) =
            batch.prepare_next(&proof).expect("fact lookup")
        else {
            panic!("fresh fact lookup")
        };
        let PinnedMachineRunPreparation::Reads(read) = lookup
            .resolve_run(MachinePinnedRunLookup::new(
                id(&command_id),
                RUN_A.to_owned(),
                self.frontier.runs.clone(),
                Some(self.run_a.clone()),
            ))
            .expect("fact Run lookup")
        else {
            panic!("fresh fact reads")
        };
        let mut inputs = empty_inputs(
            batch.current_frontier(),
            RUN_A,
            Some(&self.run_a),
            &command_id,
        );
        inputs.facts.insert(key.to_owned(), None);
        let PinnedMachineFreshPreparation::Prepared(prepared) =
            read.prepare(inputs).expect("fact reduces")
        else {
            panic!("fact cannot page")
        };
        let admitted = batch
            .accept_step(finish(*prepared))
            .expect("fact step")
            .finish()
            .expect("fact batch closes");
        self.run_a = admitted.steps[0]
            .run
            .as_ref()
            .expect("fact Run")
            .result_current
            .clone();
        append_parts(&mut self.parts, &admitted.machine);
        self.frontier = admitted.frontier;
    }

    fn begin(&mut self) -> PinnedMachinePagedBegin {
        let command_id = "command:close-a";
        let batch = prepare_pinned_command_batch(
            &self.frontier,
            vec![MachinePinnedBatchCommand {
                command_id: command_id.to_owned(),
                actor: "actor:cut-test".to_owned(),
                run_id: RUN_A.to_owned(),
                precondition: MachinePinnedBatchPrecondition::Parent(Some(
                    self.run_a.precondition_token(),
                )),
                command: Command::CommitScope {
                    scope_id: ROOT_SCOPE_ID.to_owned(),
                },
            }],
            None,
        )
        .expect("closure batch");
        let proof = MachinePinnedCommandProof::vacant(
            MachineCommandIndexProof::empty_nonmembership(command_id).expect("absence"),
        );
        let PinnedMachineCommandPreparation::Lookup(lookup) = prepare_pinned_command(
            batch.current_frontier(),
            &proof,
            batch.next_envelope().expect("closure envelope"),
        )
        .expect("closure lookup") else {
            panic!("lookup")
        };
        let PinnedMachineRunPreparation::Reads(read) = lookup
            .resolve_run(MachinePinnedRunLookup::new(
                id(command_id),
                RUN_A.to_owned(),
                self.frontier.runs.clone(),
                Some(self.run_a.clone()),
            ))
            .expect("closure Run")
        else {
            panic!("reads")
        };
        let mut inputs = empty_inputs(&self.frontier, RUN_A, Some(&self.run_a), command_id);
        inputs
            .scopes
            .insert(ROOT_SCOPE_ID.to_owned(), Some(self.scope_a.clone()));
        let PinnedMachineFreshPreparation::PagedBegin(prepared) =
            read.prepare(inputs).expect("closure prepares")
        else {
            panic!("explicit paged preparation")
        };
        let staged = batch
            .into_paged_begin(*prepared)
            .expect("frozen batch binds");
        let updates = staged
            .root_mutations()
            .expect("staging roots")
            .iter()
            .map(physical_result)
            .collect();
        let begin = staged.finish(updates).expect("staging binds");
        let updates = begin
            .root_mutations()
            .expect("begin roots")
            .iter()
            .map(physical_result)
            .collect();
        let begin = begin.finish(updates).expect("begin binds");
        self.frontier.clone_from(&begin.frontier);
        begin
    }

    fn close(&mut self, begin: &PinnedMachinePagedBegin) {
        let inputs = MachinePagedFinalizeInputs::new(
            begin.fenced_run.clone(),
            BTreeMap::from([(ROOT_SCOPE_ID.to_owned(), self.scope_a.clone())]),
            None,
            MachineCommandIndexProof::empty_nonmembership("command:close-a").expect("absence"),
            None,
        );
        let prepared = prepare_pinned_transition_final(&self.frontier, &begin.transition, inputs)
            .expect("final prepares");
        let updates = prepared
            .shadow_root_mutations()
            .expect("final shadow roots")
            .iter()
            .map(physical_result)
            .collect();
        let publish = prepared.finish_shadow_roots(updates).expect("shadow binds");
        let updates = publish
            .root_mutations()
            .expect("publish roots")
            .iter()
            .map(physical_result)
            .collect();
        let closed = publish.finish(updates).expect("final publishes");
        assert_eq!(
            closed.batch.parent_authority_root,
            begin.transition.batch_manifest.parent_authority_root
        );
        assert_eq!(
            closed.batch.admission_parent_authority_root,
            self.frontier.authority_root
        );
        append_parts(&mut self.parts, &closed.machine);
        self.frontier = closed.frontier;
    }

    fn machine(&self) -> Machine {
        let snapshot = MachineSnapshot::from_root_parts(self.parts.clone())
            .expect("public root parts restore");
        match snapshot.base_anchor.as_ref() {
            Some(anchor) => Machine::restore_anchored(snapshot.clone(), anchor),
            None => Machine::restore(snapshot),
        }
        .expect("Machine restores")
    }

    fn adopt_compaction(&mut self, machine: &Machine) {
        assert_eq!(
            self.frontier.authority_root,
            machine.authority_root().expect("authority")
        );
        self.parts = machine
            .snapshot()
            .root_parts()
            .expect("compacted root parts");
        let anchor = machine
            .base_anchor()
            .expect("anchor accessor")
            .expect("base anchor");
        self.frontier.base_anchor_id = Some(anchor.anchor_id);
        self.frontier.command_index_root = anchor.command_index_root;
        self.frontier
            .verify()
            .expect("frontier adopts exact physical anchor");
    }
}

fn assert_restores(machine: &Machine, segments: &[MachineCommandArchiveSegment]) -> Machine {
    let snapshot = machine.snapshot();
    let anchor = machine
        .base_anchor()
        .expect("anchor accessor")
        .expect("compacted anchor");
    let raw = Machine::restore_with_archive(snapshot.clone(), segments.iter().cloned())
        .expect("raw archive audit");
    let hot = Machine::restore_anchored(snapshot, &anchor).expect("exact-anchor restore");
    assert_eq!(raw.snapshot(), hot.snapshot());
    assert_eq!(
        hot.authority_root().expect("restored authority"),
        machine.authority_root().expect("live authority")
    );
    hot
}

#[test]
fn material_interleave_keeps_the_frozen_source_available_after_event_compaction() {
    let mut history = History::new();
    let begin = history.begin();
    history.material("between-begin-and-final");
    history.close(&begin);
    let mut machine = history.machine();
    let segment = machine
        .compact_event_history(1)
        .expect("complete close batch remains hot")
        .archive_segment;
    assert_eq!(
        segment.batches.len(),
        1,
        "Event cut must not sweep later material batches"
    );
    let mut restored = assert_restores(&machine, std::slice::from_ref(&segment));
    let second = restored
        .compact_event_history(0)
        .expect("replayed suffix can compact completely")
        .archive_segment;
    let _ = assert_restores(&restored, &[segment, second]);
}

fn assert_source_cut_rejected(machine: &mut Machine, retain_suffix: usize) {
    let snapshot = machine.snapshot();
    let root = machine.authority_root().expect("original authority");
    assert!(
        matches!(machine.compact_event_history(retain_suffix), Err(CoreError::Causal(message))
        if message.contains("discards frozen source"))
    );
    assert_eq!(
        machine.snapshot(),
        snapshot,
        "rejected cut publishes no mutation"
    );
    assert_eq!(
        machine.authority_root().expect("authority after rejection"),
        root
    );
}

#[test]
fn another_run_cut_cannot_discard_a_retained_paged_source() {
    let mut history = History::new();
    let begin = history.begin();
    history.another_run();
    history.close(&begin);
    let mut machine = history.machine();
    assert_source_cut_rejected(&mut machine, 1);
    let first = machine
        .compact_event_history(3)
        .expect("cut before other Run retains its ancestor")
        .archive_segment;
    let mut replayed = assert_restores(&machine, std::slice::from_ref(&first));
    assert_source_cut_rejected(&mut replayed, 1);
    let second = replayed
        .compact_event_history(0)
        .expect("complete suffix cut")
        .archive_segment;
    let _ = assert_restores(&replayed, &[first, second]);
}

#[test]
fn material_and_other_run_interleave_remain_closed_across_repeated_cuts() {
    let mut history = History::new();
    let begin = history.begin();
    history.material("before-other-run");
    history.another_run();
    history.material("after-other-run");
    history.close(&begin);
    let mut machine = history.machine();
    assert_source_cut_rejected(&mut machine, 1);
    let first = machine
        .compact_event_history(3)
        .expect("safe complete prefix")
        .archive_segment;
    assert_eq!(first.batches.len(), 1);
    let mut replayed = assert_restores(&machine, std::slice::from_ref(&first));
    assert_source_cut_rejected(&mut replayed, 1);
    let second = replayed
        .compact_event_history(0)
        .expect("all retained dependencies move cold together")
        .archive_segment;
    let _ = assert_restores(&replayed, &[first, second]);
}

#[test]
fn zero_event_cuts_and_later_paged_sources_survive_full_and_anchored_replay() {
    let material = material_at(&genesis_frontier(), "before-runs");
    let mut parts = Machine::new()
        .snapshot()
        .root_parts()
        .expect("genesis parts");
    append_parts(&mut parts, &material.delta);
    let mut initial =
        Machine::restore(MachineSnapshot::from_root_parts(parts).expect("material parts"))
            .expect("material Machine");
    let zero = initial
        .compact_event_free_admissions()
        .expect("zero-event cut")
        .archive_segment;
    assert!(zero.entries.is_empty());
    let _ = assert_restores(&initial, std::slice::from_ref(&zero));
    let anchor = initial
        .base_anchor()
        .expect("anchor accessor")
        .expect("material anchor");
    let mut frontier = material.frontier;
    frontier.base_anchor_id = Some(anchor.anchor_id);
    frontier.command_index_root = anchor.command_index_root;
    let mut history = History::start_from(
        initial.snapshot().root_parts().expect("anchored parts"),
        &frontier,
    );
    let begin = history.begin();
    history.material("during-paging");
    history.close(&begin);
    let mut machine = history.machine();
    let event = machine
        .compact_event_history(1)
        .expect("source-retaining Event cut")
        .archive_segment;
    let mut archives = vec![zero, event];
    machine = assert_restores(&machine, &archives);
    archives.push(
        machine
            .compact_event_history(0)
            .expect("replayed complete suffix")
            .archive_segment,
    );
    let _ = assert_restores(&machine, &archives);
    history.adopt_compaction(&machine);
    history.material("after-complete-cut");
    let mut material_tail = history.machine();
    archives.push(
        material_tail
            .compact_event_free_admissions()
            .expect("later zero-event cut")
            .archive_segment,
    );
    let _ = assert_restores(&material_tail, &archives);
}

#[test]
fn compaction_delta_can_append_a_new_batch_without_repeating_the_suffix() {
    let mut history = History::new();
    let begin = history.begin();
    history.material("retained-material");
    history.close(&begin);
    let mut machine = history.machine();
    let previous = machine.snapshot();
    let segment = machine
        .compact_event_history(1)
        .expect("safe cut")
        .archive_segment;
    history.adopt_compaction(&machine);
    history.material("new-batch-after-cut");
    let next = history.machine().snapshot();
    let delta = MachineDelta::between_compaction(&previous, &next, &segment)
        .expect("valid cut plus new batch");
    let mut materialized = previous.clone();
    materialized
        .apply_compaction_delta(&delta, &segment)
        .expect("snapshot applies mixed delta");
    let mut live = Machine::restore(previous).expect("parent restores");
    live.apply_compaction_delta(&delta, &segment)
        .expect("live Machine applies mixed delta");
    assert_eq!(materialized, next);
    assert_eq!(live.snapshot(), next);
}

fn without_closure(mut parts: MachineRootParts) -> MachineSnapshot {
    let command = parts
        .commands
        .remove("command:close-a")
        .expect("closure command exists");
    parts.command_index_proofs.remove("command:close-a");
    parts
        .admissions
        .retain(|entry| entry.command_id != "command:close-a");
    parts
        .events
        .retain(|event| event.command_id != "command:close-a");
    parts.batches.remove(&command.batch_id);
    parts
        .batch_admission_order
        .retain(|id| id != &command.batch_id);
    MachineSnapshot::from_root_parts(parts).expect("material-only prefix is valid")
}

fn forge_zero_cut_for_retained_closure(
    machine: &Machine,
) -> (MachineDelta, MachineCommandArchiveSegment) {
    let current = machine.snapshot();
    let parts = current.root_parts().expect("current parts");
    let before = without_closure(parts.clone());
    let mut prefix = Machine::restore_anchored(
        before.clone(),
        before.base_anchor.as_ref().expect("prefix anchor"),
    )
    .expect("prefix restores");
    let segment = prefix
        .compact_event_free_admissions()
        .expect("prefix zero-event cut")
        .archive_segment;
    assert!(segment.entries.is_empty());
    let delta = MachineDelta::between_compaction(&before, &prefix.snapshot(), &segment)
        .expect("valid prefix cut delta");
    let mut wire = serde_json::to_value(delta).expect("delta encodes");
    let authority = machine.authority_root().expect("whole parent authority");
    wire["parent_snapshot_digest"] = serde_json::json!(authority);
    wire["result_snapshot_digest"] = serde_json::json!(authority);
    wire["command_index_proofs"] =
        serde_json::to_value(parts.command_index_proofs).expect("retained proofs encode");
    (
        serde_json::from_value(wire).expect("forged delta has a valid closed shape"),
        segment,
    )
}

#[test]
fn serialized_zero_event_delta_cannot_bypass_prepared_compaction_authority() {
    let mut history = History::new();
    let begin = history.begin();
    history.material("zero-cut-between-source-and-final");
    history.close(&begin);
    let mut machine = history.machine();
    machine.compact_event_history(1).expect("safe initial cut");
    let original = machine.snapshot();
    let (delta, segment) = forge_zero_cut_for_retained_closure(&machine);
    let result = machine.apply_compaction_delta(&delta, &segment);
    assert!(
        matches!(&result, Err(CoreError::Validation(message))
        if message.contains("exact locally derived transition")),
        "unexpected cut result: {result:?}"
    );
    assert_eq!(machine.snapshot(), original);
    let mut snapshot = original.clone();
    let result_anchor = delta
        .base_anchor
        .as_ref()
        .expect("forged cut retains exact target anchor");
    let result = snapshot.apply_compaction_delta_anchored(&delta, result_anchor, &segment);
    assert!(
        matches!(&result, Err(CoreError::Validation(message))
        if message.contains("exact locally derived transition")),
        "unexpected snapshot cut result: {result:?}"
    );
    assert_eq!(snapshot, original);
}

fn assert_bad_parts(parts: MachineRootParts, label: &str) {
    let result = std::panic::catch_unwind(|| MachineSnapshot::from_root_parts(parts));
    assert!(result.is_ok(), "{label} panicked");
    assert!(
        result.expect("checked no panic").is_err(),
        "{label} was silently accepted"
    );
}

#[test]
fn root_parts_require_exact_unique_batch_order_and_key_identity() {
    let mut history = History::new();
    history.material("parts-material");
    let original = history.parts;
    let first = original.batch_admission_order[0].clone();
    let mut duplicate = original.clone();
    duplicate.batch_admission_order.push(first.clone());
    assert_bad_parts(duplicate, "duplicate batch order");
    let mut missing = original.clone();
    missing.batches.remove(&first);
    assert_bad_parts(missing, "order points to a missing batch");
    let mut omitted = original.clone();
    omitted.batch_admission_order.remove(0);
    assert_bad_parts(omitted, "unlisted retained batch");
    let mut aliased = original.clone();
    let batch = aliased.batches.remove(&first).expect("batch exists");
    let alias = id("batch-key-alias");
    aliased.batches.insert(alias.clone(), batch);
    aliased.batch_admission_order[0] = alias;
    assert_bad_parts(
        aliased,
        "key/order alias cannot normalize a record identity",
    );
    let mut reversed = original.clone();
    reversed.batch_admission_order.reverse();
    assert_bad_parts(
        reversed,
        "batch causal order cannot be sorted or normalized",
    );
    let mut extra = Machine::new()
        .snapshot()
        .root_parts()
        .expect("genesis parts");
    extra
        .batches
        .insert(first.clone(), original.batches[&first].clone());
    assert_bad_parts(
        extra,
        "an unlisted batch cannot disappear from empty root parts",
    );
}

#[test]
fn root_parts_verify_complete_batch_records_and_command_membership() {
    let original = History::new().parts;
    let batch_id = original.batch_admission_order[0].clone();
    let command_id = original.batches[&batch_id].members[0].command_id.clone();
    let mut bad_receipt = original.clone();
    bad_receipt
        .batches
        .get_mut(&batch_id)
        .expect("batch")
        .batch_receipt_id = id("forged-batch-receipt");
    assert_bad_parts(bad_receipt, "invalid batch record receipt");
    let mut bad_member = original.clone();
    bad_member
        .batches
        .get_mut(&batch_id)
        .expect("batch")
        .members[0]
        .position = 1;
    assert_bad_parts(bad_member, "invalid batch member position");
    let mut no_command = original.clone();
    no_command.commands.remove(&command_id);
    assert_bad_parts(no_command, "missing batch command");
    let mut no_admission = original.clone();
    no_admission.admissions.clear();
    assert_bad_parts(no_admission, "missing batch admission");
    for index in 0..original.events.len() {
        let mut no_event = original.clone();
        no_event.events.remove(index);
        assert_bad_parts(no_event, "missing StartRun Event member");
    }
    let mut no_proof = original.clone();
    no_proof.command_index_proofs.remove(&command_id);
    assert_bad_parts(no_proof, "missing command non-membership proof");
    let mut duplicated = MachineSnapshot::from_root_parts(original).expect("positive snapshot");
    duplicated.batches.push(duplicated.batches[0].clone());
    assert!(
        duplicated.root_parts().is_err(),
        "export must reject repeated batches too"
    );
}

#[test]
fn root_parts_roundtrip_genesis_ordinary_material_and_compacted_suffix() {
    let genesis = Machine::new()
        .snapshot()
        .root_parts()
        .expect("genesis parts");
    assert_eq!(
        MachineSnapshot::from_root_parts(genesis.clone())
            .expect("genesis restores")
            .root_parts()
            .expect("genesis exports"),
        genesis
    );
    let material = material_at(&genesis_frontier(), "material-only-parts");
    let mut material_parts = genesis;
    append_parts(&mut material_parts, &material.delta);
    assert!(material_parts.commands.is_empty());
    assert_eq!(material_parts.batches.len(), 1);
    assert_eq!(
        MachineSnapshot::from_root_parts(material_parts.clone())
            .expect("material-only restores")
            .root_parts()
            .expect("material-only exports"),
        material_parts
    );
    let mut history = History::new();
    let begin = history.begin();
    history.material("roundtrip-hot-material");
    history.close(&begin);
    let ordinary = history.parts.clone();
    assert_eq!(
        MachineSnapshot::from_root_parts(ordinary.clone())
            .expect("ordinary restores")
            .root_parts()
            .expect("ordinary exports"),
        ordinary
    );
    let mut machine = history.machine();
    machine.compact_event_history(1).expect("safe suffix cut");
    let compacted = machine.snapshot().root_parts().expect("compacted exports");
    assert_eq!(
        MachineSnapshot::from_root_parts(compacted.clone())
            .expect("compacted restores")
            .root_parts()
            .expect("compacted reexports"),
        compacted
    );
}

fn material_only_compaction_fixture() -> (
    MachineSnapshot,
    MachineSnapshot,
    MachineCommandArchiveSegment,
) {
    let mut parts = Machine::new()
        .snapshot()
        .root_parts()
        .expect("genesis parts");
    let mut frontier = genesis_frontier();
    for label in ["first-cold-material", "second-cold-material"] {
        let mut candidate = Seed::new().plan.candidate;
        label.clone_into(&mut candidate.name);
        let material = MachineMaterialAdmission::new(
            format!("profile:{label}"),
            vec![seal_plan(candidate).expect("distinct material Plan seals")],
            vec![artifact("test.cold-material/1", label.as_bytes())],
        )
        .expect("material admission derives");
        let reads = MachineMaterialParentReads::new(
            material
                .plans()
                .iter()
                .map(|plan| (plan.plan_id.clone(), None))
                .collect(),
            material
                .artifacts()
                .iter()
                .map(|record| (record.reference.artifact_id.clone(), None))
                .collect(),
        );
        let admitted = prepare_machine_material_admission(&frontier, &material, &reads)
            .expect("public material preparation admits the exact batch");
        append_parts(&mut parts, &admitted.delta);
        frontier = admitted.frontier;
    }
    let before = MachineSnapshot::from_root_parts(parts).expect("material source restores");
    let mut machine = Machine::restore(before.clone()).expect("material Machine restores");
    let compaction = machine
        .compact_event_free_admissions()
        .expect("complete material-only history compacts");
    let cold = machine.snapshot();
    assert!(cold.batches.is_empty());
    assert!(cold.events.is_empty());
    assert!(cold.admissions.is_empty());
    assert_eq!(cold.plans.len(), 2);
    assert_eq!(cold.artifacts.len(), 2);
    let base = cold.base.as_ref().expect("cold snapshot has a base");
    assert_eq!(base.plan_count, 2);
    assert_eq!(base.artifact_count, 2);
    (before, cold, compaction.archive_segment)
}

#[test]
fn anchored_cold_restore_authenticates_the_exact_material_admission_order() {
    let (_, cold, segment) = material_only_compaction_fixture();
    let anchor = cold
        .base_anchor
        .as_ref()
        .expect("cold snapshot has an anchor");
    let original = Machine::restore_anchored(cold.clone(), anchor).expect("exact cold restore");
    assert_eq!(
        Machine::restore_with_archive(cold.clone(), [segment.clone()])
            .expect("complete archive agrees")
            .authority_root()
            .expect("archive authority derives"),
        original
            .authority_root()
            .expect("anchored authority derives")
    );
    for plans in [true, false] {
        let mut reordered = cold.clone();
        let kind = if plans {
            reordered.plans.swap(0, 1);
            "Plan"
        } else {
            reordered.artifacts.swap(0, 1);
            "Artifact"
        };
        assert_eq!(reordered.base, cold.base);
        assert_eq!(reordered.base_anchor, cold.base_anchor);
        let error = Machine::restore_anchored(reordered.clone(), anchor)
            .expect_err("an exact anchor cannot admit another material order");
        assert!(matches!(error, CoreError::IdentityMismatch(message)
            if message == format!("Machine base {kind} admission commitment or count does not match restored material")));
        assert!(Machine::restore_with_archive(reordered, [segment.clone()]).is_err());
    }
}

fn rebind_material_base_anchor(snapshot: &mut MachineSnapshot) {
    let base = snapshot.base.as_ref().expect("material base exists");
    let anchor = snapshot
        .base_anchor
        .as_mut()
        .expect("material anchor exists");
    anchor.base_id = base.identity().expect("changed base remains shape-valid");
    let mut preimage = serde_json::to_value(&*anchor).expect("anchor serializes");
    preimage
        .as_object_mut()
        .expect("anchor is an object")
        .remove("anchor_id");
    anchor.anchor_id = content_id(cymule_core::MachineBaseAnchor::VERSION, &preimage)
        .expect("exact changed anchor derives");
    anchor
        .verify()
        .expect("changed anchor passes its identity check");
}

#[test]
fn cold_restore_checks_material_commitments_and_counts_against_the_actual_prefix() {
    let (_, cold, segment) = material_only_compaction_fixture();
    for (plans, count) in [(true, false), (false, false), (true, true), (false, true)] {
        let mut changed = cold.clone();
        let base = changed.base.as_mut().expect("material base exists");
        let kind = if plans { "Plan" } else { "Artifact" };
        match (plans, count) {
            (true, false) => base.plan_admission_commitment = id("wrong-plan-commitment"),
            (false, false) => base.artifact_admission_commitment = id("wrong-artifact-commitment"),
            (true, true) => base.plan_count -= 1,
            (false, true) => base.artifact_count -= 1,
        }
        rebind_material_base_anchor(&mut changed);
        let anchor = changed.base_anchor.as_ref().expect("changed anchor exists");
        for result in [
            Machine::restore_with_archive(changed.clone(), [segment.clone()]),
            Machine::restore_anchored(changed.clone(), anchor),
        ] {
            let error = result.expect_err("material evidence must reach its claimed base");
            assert!(matches!(error, CoreError::IdentityMismatch(message)
                if message == format!("Machine base {kind} admission commitment or count does not match restored material")));
        }
    }
}

#[test]
fn compaction_delta_rejects_legacy_generations_before_authority_admission() {
    let (before, cold, segment) = material_only_compaction_fixture();
    let delta = MachineDelta::between_compaction(&before, &cold, &segment)
        .expect("nonempty public compaction delta derives");
    assert!(!delta.is_empty());
    for generation in 1..=5 {
        let mut legacy = delta.clone();
        legacy.delta_version = format!("cymule.machine-delta/{generation}");
        let mut machine = Machine::restore(before.clone()).expect("parent Machine restores");
        let mut snapshot = before.clone();
        for result in [
            machine.apply_compaction_delta(&legacy, &segment),
            snapshot.apply_compaction_delta(&legacy, &segment),
        ] {
            assert!(matches!(result, Err(CoreError::Validation(message))
                if message.starts_with("unsupported machine delta version")));
        }
        assert_eq!(machine.snapshot(), before);
        assert_eq!(snapshot, before);
    }
}

#[test]
fn compaction_delta_archive_failure_is_transactional_after_material_assembly() {
    let mut history = History::new();
    let before = history.machine().snapshot();
    let mut compacted = history.machine();
    let segment = compacted
        .compact_event_history(0)
        .expect("existing command history compacts")
        .archive_segment;
    history.adopt_compaction(&compacted);
    history.material("material-after-compaction");
    let after = history.machine().snapshot();
    let delta = MachineDelta::between_compaction(&before, &after, &segment)
        .expect("public compaction delta carries a new material batch");
    assert_eq!(delta.artifacts.len(), 1);
    assert_eq!(delta.batches.len(), 1);
    let (_, _, foreign_segment) = material_only_compaction_fixture();
    foreign_segment
        .verify()
        .expect("foreign segment is independently valid");
    assert_ne!(foreign_segment.header, segment.header);
    let mut machine = Machine::restore(before.clone()).expect("parent Machine restores");
    let mut snapshot = before.clone();
    let before_bytes = cymule_core::canonical_bytes(&before).expect("parent bytes encode");
    for result in [
        machine.apply_compaction_delta(&delta, &foreign_segment),
        snapshot.apply_compaction_delta(&delta, &foreign_segment),
    ] {
        assert!(matches!(result, Err(CoreError::IdentityMismatch(message))
            if message == "Machine compaction segment header does not match its delta"));
    }
    assert_eq!(machine.snapshot(), before);
    assert_eq!(
        cymule_core::canonical_bytes(&snapshot).expect("rejected snapshot encodes"),
        before_bytes,
        "archive rejection after staged material assembly publishes no partial values"
    );
    machine
        .apply_compaction_delta(&delta, &segment)
        .expect("exact retry applies");
    snapshot
        .apply_compaction_delta(&delta, &segment)
        .expect("exact snapshot retry applies");
    assert_eq!(machine.snapshot(), after);
    assert_eq!(snapshot, after);
    assert!(machine.apply_compaction_delta(&delta, &segment).is_err());
    assert!(snapshot.apply_compaction_delta(&delta, &segment).is_err());
    assert_eq!(machine.snapshot(), after);
    assert_eq!(snapshot, after);
}

#[test]
fn missing_root_parts_batch_order_returns_an_error_not_a_panic() {
    let mut parts = Machine::new()
        .snapshot()
        .root_parts()
        .expect("genesis parts");
    parts.batch_admission_order.push(id("missing-batch"));
    let result = std::panic::catch_unwind(|| MachineSnapshot::from_root_parts(parts));
    assert!(result.is_ok(), "untrusted batch order must never panic");
    assert!(matches!(
        result.expect("no panic"),
        Err(CoreError::IdentityMismatch(_))
    ));
}

fn assert_prepared_compaction(
    frontier: &MachineAuthorityFrontier,
    parts: MachineRootParts,
    intent: MachineCompactionIntent,
) -> (PreparedPinnedMachineCompaction, Machine) {
    let prepared = prepare_pinned_compaction(frontier, parts.clone(), intent)
        .expect("offline preparation succeeds");
    let previous = MachineSnapshot::from_root_parts(parts).expect("source snapshot");
    let mut machine = match previous.base_anchor.as_ref() {
        Some(anchor) => Machine::restore_anchored(previous.clone(), anchor),
        None => Machine::restore(previous.clone()),
    }
    .expect("source restores");
    let expected = match intent {
        MachineCompactionIntent::EventPrefix { retain_suffix } => {
            machine.compact_event_history(retain_suffix)
        }
        MachineCompactionIntent::EventFreeAdmissions => machine.compact_event_free_admissions(),
    }
    .expect("reference cut succeeds");
    let delta =
        MachineDelta::between_compaction(&previous, &machine.snapshot(), &expected.archive_segment)
            .expect("exact reference delta");
    assert_eq!(prepared.compaction(), &expected);
    assert_eq!(
        prepared.root_delta(),
        &delta.root_delta().expect("physical delta")
    );
    let mut unchanged = prepared.frontier().clone();
    unchanged
        .base_anchor_id
        .clone_from(&frontier.base_anchor_id);
    unchanged
        .command_index_root
        .clone_from(&frontier.command_index_root);
    assert_eq!(
        &unchanged, frontier,
        "only physical base and command-index identities may change"
    );
    assert_eq!(
        prepared.frontier().authority_root,
        machine.authority_root().expect("result authority")
    );
    (prepared, machine)
}

#[test]
fn offline_preparation_preserves_authority_and_exact_delta_across_replayed_cuts() {
    let mut history = History::new();
    let begin = history.begin();
    history.material("offline-before-other-run");
    history.another_run();
    history.material("offline-after-other-run");
    history.close(&begin);
    let (first, machine) = assert_prepared_compaction(
        &history.frontier,
        history.parts.clone(),
        MachineCompactionIntent::EventPrefix { retain_suffix: 3 },
    );
    let segment = first.compaction().archive_segment.clone();
    let restored = assert_restores(&machine, std::slice::from_ref(&segment));
    let (second, machine) = assert_prepared_compaction(
        first.frontier(),
        restored.snapshot().root_parts().expect("replayed parts"),
        MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
    );
    let _ = assert_restores(
        &machine,
        &[segment, second.compaction().archive_segment.clone()],
    );
}

#[test]
fn offline_event_free_preparation_archives_material_without_fabricating_commands() {
    let admitted = material_at(&genesis_frontier(), "offline-genesis-material");
    let mut parts = Machine::new().snapshot().root_parts().expect("empty parts");
    append_parts(&mut parts, &admitted.delta);
    let (first, machine) = assert_prepared_compaction(
        &admitted.frontier,
        parts,
        MachineCompactionIntent::EventFreeAdmissions,
    );
    let first_segment = first.compaction().archive_segment.clone();
    assert!(first_segment.entries.is_empty());
    assert!(first.root_delta().removed_event_ids.is_empty());
    assert!(first.root_delta().removed_admission_ids.is_empty());
    assert_eq!(first.root_delta().removed_batch_ids.len(), 1);
    assert_eq!(first.frontier().admission_head, None);
    assert_eq!(first.frontier().batch_count, 1);
    let restored = assert_restores(&machine, std::slice::from_ref(&first_segment));
    let second_material = material_at(first.frontier(), "offline-later-material");
    let mut parts = restored.snapshot().root_parts().expect("restored parts");
    append_parts(&mut parts, &second_material.delta);
    let (second, machine) = assert_prepared_compaction(
        &second_material.frontier,
        parts,
        MachineCompactionIntent::EventFreeAdmissions,
    );
    assert_eq!(second.frontier().admission_head, None);
    assert_eq!(second.frontier().batch_count, 2);
    let _ = assert_restores(
        &machine,
        &[first_segment, second.compaction().archive_segment.clone()],
    );
}

#[test]
fn offline_preparation_rejects_a_cut_that_loses_a_retained_frozen_source() {
    let mut history = History::new();
    let begin = history.begin();
    history.material("offline-unsafe-source");
    history.another_run();
    history.close(&begin);
    let source = history.parts.clone();
    let frontier = history.frontier.clone();
    let result = prepare_pinned_compaction(
        &frontier,
        source.clone(),
        MachineCompactionIntent::EventPrefix { retain_suffix: 1 },
    );
    assert!(matches!(result, Err(CoreError::Causal(message))
        if message.contains("discards frozen source")));
    assert_eq!(history.parts, source);
    assert_eq!(history.frontier, frontier);
    let _ = assert_prepared_compaction(
        &frontier,
        source,
        MachineCompactionIntent::EventPrefix { retain_suffix: 3 },
    );
}

#[test]
fn offline_preparation_requires_no_pending_paged_command() {
    let mut history = History::new();
    let _pending = history.begin();
    history.material("offline-pending-material");
    history.another_run();
    assert_eq!(history.frontier.pending_commands.entries, 1);
    assert_eq!(history.frontier.paged_transitions.entries, 1);
    let result = prepare_pinned_compaction(
        &history.frontier,
        history.parts.clone(),
        MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
    );
    assert!(matches!(result, Err(CoreError::Causal(message))
        if message.contains("no pending paged commands")));
}

#[test]
fn offline_preparation_rejects_missing_parts_and_a_different_semantic_source() {
    let mut history = History::new();
    let source_frontier = history.frontier.clone();
    let mut missing = history.parts.clone();
    missing
        .batch_admission_order
        .push(id("offline-missing-batch"));
    let result = std::panic::catch_unwind(|| {
        prepare_pinned_compaction(
            &source_frontier,
            missing,
            MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
        )
    });
    assert!(matches!(result, Ok(Err(CoreError::IdentityMismatch(_)))));
    history.material("offline-wrong-source");
    let result = prepare_pinned_compaction(
        &source_frontier,
        history.parts,
        MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
    );
    assert!(matches!(result, Err(CoreError::IdentityMismatch(message))
        if message.contains("pinned semantic frontier")));
}

#[test]
fn offline_preparation_pins_the_exact_base_and_command_index_without_cold_fallback() {
    let mut history = History::new();
    let begin = history.begin();
    history.material("offline-anchored-source");
    history.close(&begin);
    let (first, machine) = assert_prepared_compaction(
        &history.frontier,
        history.parts,
        MachineCompactionIntent::EventPrefix { retain_suffix: 1 },
    );
    let original = machine.snapshot().root_parts().expect("anchored parts");
    let mut wrong_index = first.frontier().clone();
    wrong_index.command_index_root = id("offline-wrong-index");
    wrong_index
        .verify()
        .expect("physical index is independently pinned");
    for frontier in [&wrong_index, &genesis_frontier()] {
        let result = prepare_pinned_compaction(
            frontier,
            original.clone(),
            MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
        );
        assert!(matches!(result, Err(CoreError::IdentityMismatch(message))
            if message.contains("pinned base anchor or command index")));
    }
    let mut wrong_base = original;
    wrong_base.base.as_mut().expect("base").prefix_digest = id("wrong-prefix");
    let result = prepare_pinned_compaction(
        first.frontier(),
        wrong_base,
        MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
    );
    assert!(matches!(result, Err(CoreError::IdentityMismatch(_))));
}

#[test]
fn offline_preparation_rejects_empty_or_split_event_cuts_and_non_event_free_tails() {
    let history = History::new();
    for intent in [
        MachineCompactionIntent::EventPrefix { retain_suffix: 2 },
        MachineCompactionIntent::EventPrefix { retain_suffix: 1 },
        MachineCompactionIntent::EventFreeAdmissions,
    ] {
        assert!(
            prepare_pinned_compaction(&history.frontier, history.parts.clone(), intent).is_err()
        );
    }
    let genesis = Machine::new().snapshot().root_parts().expect("empty parts");
    assert!(matches!(
        prepare_pinned_compaction(
            &genesis_frontier(),
            genesis,
            MachineCompactionIntent::EventFreeAdmissions,
        ),
        Err(CoreError::Validation(_))
    ));
}

#[test]
fn offline_source_rejects_run_root_cardinality_not_bound_by_the_semantic_digest() {
    let history = History::new();
    assert_eq!(history.frontier.runs.entries, 1);
    for runs in [
        MachineMapRoot::empty(),
        MachineMapRoot {
            node: Some(id("extra-run-root")),
            entries: 2,
        },
    ] {
        let mut frontier = history.frontier.clone();
        frontier.runs = runs;
        frontier
            .verify()
            .expect("shape-valid independently pinned root");
        assert_eq!(frontier.authority_root, history.frontier.authority_root);
        let result = prepare_pinned_compaction(
            &frontier,
            history.parts.clone(),
            MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
        );
        assert!(
            matches!(result, Err(CoreError::IdentityMismatch(message))
            if message.contains("Run count")),
            "inconsistent normalized Run root must be rejected"
        );
    }
    let _ = assert_prepared_compaction(
        &history.frontier,
        history.parts,
        MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
    );
}

#[test]
fn offline_source_rejects_fact_root_cardinality_not_bound_by_the_semantic_digest() {
    let mut history = History::new();
    history.record_fact("fact:source-count");
    assert_eq!(history.frontier.facts.entries, 1);
    for facts in [
        MachineMapRoot::empty(),
        MachineMapRoot {
            node: Some(id("extra-fact-root")),
            entries: 2,
        },
    ] {
        let mut frontier = history.frontier.clone();
        frontier.facts = facts;
        frontier
            .verify()
            .expect("shape-valid independently pinned root");
        assert_eq!(frontier.authority_root, history.frontier.authority_root);
        let result = prepare_pinned_compaction(
            &frontier,
            history.parts.clone(),
            MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
        );
        assert!(
            matches!(result, Err(CoreError::IdentityMismatch(message))
            if message.contains("Fact count")),
            "inconsistent normalized Fact root must be rejected"
        );
    }
    let _ = assert_prepared_compaction(
        &history.frontier,
        history.parts,
        MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
    );
}

#[test]
fn offline_source_counts_include_anchored_runs_and_facts_before_event_free_rotation() {
    let mut history = History::new();
    history.record_fact("fact:anchored-count-one");
    history.record_fact("fact:anchored-count-two");
    history.another_run();
    history.material("anchored-count-material-tail");
    let (first, machine) = assert_prepared_compaction(
        &history.frontier,
        history.parts,
        MachineCompactionIntent::EventPrefix { retain_suffix: 0 },
    );
    let source = machine.snapshot().root_parts().expect("anchored source");
    assert!(source.events.is_empty());
    let projection = &source.base.as_ref().expect("trusted base").projection;
    assert_eq!(projection.runs.len(), 2);
    assert_eq!(projection.facts.len(), 2);
    for (kind, change) in [("Run", true), ("Fact", false)] {
        let mut frontier = first.frontier().clone();
        if change {
            frontier.runs.entries = 1;
        } else {
            frontier.facts.entries = 1;
        }
        frontier.verify().expect("shape-valid undercount");
        let result = prepare_pinned_compaction(
            &frontier,
            source.clone(),
            MachineCompactionIntent::EventFreeAdmissions,
        );
        assert!(matches!(result, Err(CoreError::IdentityMismatch(message))
            if message.contains(&format!("{kind} count"))));
    }
    let (second, machine) = assert_prepared_compaction(
        first.frontier(),
        source,
        MachineCompactionIntent::EventFreeAdmissions,
    );
    let _ = assert_restores(
        &machine,
        &[
            first.compaction().archive_segment.clone(),
            second.compaction().archive_segment.clone(),
        ],
    );
}

#[test]
fn offline_material_only_source_rejects_extra_normalized_run_and_fact_entries() {
    let admitted = material_at(&genesis_frontier(), "empty-projection-counts");
    let mut source = Machine::new()
        .snapshot()
        .root_parts()
        .expect("empty source");
    append_parts(&mut source, &admitted.delta);
    for (kind, change) in [("Run", true), ("Fact", false)] {
        let mut frontier = admitted.frontier.clone();
        let extra = MachineMapRoot {
            node: Some(id(kind)),
            entries: 1,
        };
        if change {
            frontier.runs = extra;
        } else {
            frontier.facts = extra;
        }
        frontier.verify().expect("shape-valid extra entry");
        let result = prepare_pinned_compaction(
            &frontier,
            source.clone(),
            MachineCompactionIntent::EventFreeAdmissions,
        );
        assert!(matches!(result, Err(CoreError::IdentityMismatch(message))
            if message.contains(&format!("{kind} count"))));
    }
    let _ = assert_prepared_compaction(
        &admitted.frontier,
        source,
        MachineCompactionIntent::EventFreeAdmissions,
    );
}
