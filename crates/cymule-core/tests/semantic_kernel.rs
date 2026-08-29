//! Fault-oriented semantic kernel conformance tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ArtifactRef, COMMAND_VERSION, COMPONENT_OUTPUT_ARTIFACT_KIND, Command, CommandEnvelope,
    CommandReceiptStatus, ComponentContract, CoreError, Definition, DispatchPolicy, EffectContract,
    EffectIntentIdentityInput, EffectPhase, EffectProfile, EffectTransition, Event, EventContent,
    EventPayload, Expression, MAX_EXACT_INTEGER, Machine, MutationKind, Operation, PlanCandidate,
    ReconciliationMode, ReconciliationResolution, ReconciliationState, Region, ReplayAvailability,
    RunExecutionStatus, RunFailure, RunFailureClass, ScopeStatus, SealedPlan, Step, WorldOutcome,
    WorldSettlementStatus, effect_intent_id, effect_obligation_id,
};
use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use serde_json::json;

#[test]
fn artifact_kind_public_maximum_preserves_the_exact_ascii_boundary() {
    let maximal = "a".repeat(cymule_core::MAX_ARTIFACT_KIND_BYTES - 2) + "/1";
    assert_eq!(maximal.len(), 255);
    cymule_core::validate_artifact_kind(&maximal).expect("maximum ASCII kind remains valid");
    let reference = cymule_core::artifact_ref(&maximal, b"bounded kind").expect("Artifact kind");
    reference.validate().expect("maximum reference validates");
    assert!(cymule_core::validate_artifact_kind(&(maximal + "a")).is_err());
    assert!(cymule_core::validate_artifact_kind("non-ascii-\u{00e9}/1").is_err());
}

fn test_execution_binding(machine: &mut Machine, label: &str) -> ArtifactRef {
    let input = machine
        .put_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, b"{}".to_vec())
        .expect("test Run input stores");
    assert_eq!(input, test_run_input_ref());
    machine
        .put_artifact(
            cymule_core::EXECUTION_BINDING_ARTIFACT_KIND,
            label.as_bytes().to_vec(),
        )
        .expect("test execution binding stores")
}

fn test_run_input_ref() -> ArtifactRef {
    cymule_core::artifact_ref(cymule_core::RUN_INPUT_ARTIFACT_KIND, b"{}")
        .expect("test Run input derives")
}

fn test_content_id(label: &str) -> String {
    cymule_core::content_id("cymule.test-identity/1", &label).expect("test identity derives")
}

fn test_fact_value(label: &str) -> String {
    cymule_core::content_id("cymule.test-fact-value/1", &label).expect("test fact value derives")
}

fn event_with_payload(event: &Event, payload: EventPayload) -> Event {
    Event::new(EventContent {
        command_id: event.command_id.clone(),
        command_hash: event.command_hash.clone(),
        run_id: event.run_id.clone(),
        parents: event.parents.clone(),
        reads: event.reads.clone(),
        writes: event.writes.clone(),
        coordination_key: event.coordination_key.clone(),
        payload,
    })
    .expect("replacement payload remains content-addressable")
}

fn replay_with_machine_authority(
    machine: &Machine,
    events: impl IntoIterator<Item = Event>,
) -> Result<cymule_core::Projection, CoreError> {
    let snapshot = machine.snapshot();
    let entries = machine
        .replay_entries()?
        .into_iter()
        .map(|entry| (entry.admission.command_id.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut selected = BTreeMap::new();
    let mut event_ids = BTreeSet::new();
    let mut positions = BTreeSet::new();
    for event in events {
        if !event_ids.insert(event.event_id.clone()) {
            return Err(CoreError::Validation(
                "replay repeats an Event identity".to_owned(),
            ));
        }
        let retained = entries.get(&event.command_id).ok_or_else(|| {
            CoreError::NotFound(format!("Event {} has no command admission", event.event_id))
        })?;
        let entry = selected
            .entry(event.command_id.clone())
            .or_insert_with(|| retained.clone());
        let position = entry
            .events
            .iter()
            .position(|retained| {
                std::mem::discriminant(&retained.payload) == std::mem::discriminant(&event.payload)
            })
            .ok_or_else(|| {
                CoreError::NotFound(format!("Event {} has no command admission", event.event_id))
            })?;
        if !positions.insert((event.command_id.clone(), position)) {
            return Err(CoreError::IdentityMismatch(
                "replay repeats a command Event position".to_owned(),
            ));
        }
        entry.events[position] = event;
    }
    let batch_ids = selected
        .values()
        .map(|entry| entry.command.batch_id.clone())
        .collect::<BTreeSet<_>>();
    let batches = snapshot
        .batches
        .into_iter()
        .filter(|batch| batch_ids.contains(&batch.batch_id))
        .collect::<Vec<_>>();
    if batches.len() != batch_ids.len() {
        return Err(CoreError::NotFound(
            "selected replay batch is missing".to_owned(),
        ));
    }
    for batch in &batches {
        for member in &batch.members {
            let entry = entries.get(&member.command_id).ok_or_else(|| {
                CoreError::NotFound(format!("batch member {} is missing", member.command_id))
            })?;
            selected
                .entry(member.command_id.clone())
                .or_insert_with(|| entry.clone());
        }
    }
    Machine::replay(
        snapshot.plans,
        snapshot.artifacts,
        batches,
        selected.into_values(),
    )
}

fn submit_new_with_archive(
    machine: &mut Machine,
    archive_segments: &[cymule_core::MachineCommandArchiveSegment],
    envelope: CommandEnvelope,
) -> Result<cymule_core::CommandReceipt, CoreError> {
    if machine.base_anchor()?.is_none() {
        return machine.submit(envelope);
    }
    let lookup =
        new_archive_nonmembership_lookup(machine, archive_segments, envelope.command_id.as_str())?;
    machine.submit_with_archive_lookup(envelope, lookup)
}

fn new_archive_nonmembership_lookup(
    machine: &Machine,
    archive_segments: &[cymule_core::MachineCommandArchiveSegment],
    command_id: &str,
) -> Result<cymule_core::MachineCommandArchiveLookup, CoreError> {
    let anchor = machine.base_anchor()?.ok_or_else(|| {
        CoreError::NotFound("archive non-membership lookup requires a compacted base".to_owned())
    })?;
    let nodes = archive_segments
        .iter()
        .map(cymule_core::MachineCommandArchiveSegment::command_index_nodes)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .map(|node| Ok((node.identity()?.to_owned(), node)))
        .collect::<Result<BTreeMap<_, _>, CoreError>>()?;
    let proof = cymule_core::resolve_machine_command_index_proof(
        &anchor.command_index_root,
        command_id,
        |node_id| Ok(nodes.get(node_id).cloned()),
    )?;
    Ok(cymule_core::MachineCommandArchiveLookup::NonMember { index_proof: proof })
}

#[derive(serde::Serialize)]
struct MachinePrefixPreimageForTest<'a> {
    prefix_version: &'static str,
    archive_head: &'a str,
    archive_count: u64,
    archive_event_count: u64,
    admission_head: Option<&'a str>,
    command_index_root: &'a str,
    projection_digest: &'a str,
    projection_root: &'a str,
}

#[derive(serde::Serialize)]
struct MachineBaseAnchorPreimageForTest<'a> {
    anchor_version: &'static str,
    base_id: &'a str,
    archive_head: &'a str,
    archive_count: u64,
    archive_event_count: u64,
    archive_batch_count: u64,
    admission_head: Option<&'a str>,
    command_index_root: &'a str,
    prefix_digest: &'a str,
    projection_digest: &'a str,
    projection_root: &'a str,
}

#[derive(serde::Serialize)]
struct CommandAdmissionPreimageForTest<'a> {
    admission_version: &'static str,
    sequence: u64,
    parent_admission: &'a Option<String>,
    command_id: &'a str,
    semantic_hash: &'a str,
    command_record_digest: &'a str,
    batch_id: &'a str,
    batch_position: u32,
    batch_len: u32,
    before_projection_digest: &'a str,
    after_projection_digest: &'a str,
    status: CommandReceiptStatus,
    event_ids: &'a [String],
}

fn reauthenticate_compacted_base(snapshot: &mut cymule_core::MachineSnapshot) {
    let base = snapshot.base.as_mut().expect("compacted base exists");
    base.projection_digest = base
        .projection
        .digest()
        .expect("tampered projection digest recomputes");
    base.prefix_digest = cymule_core::content_id(
        "cymule.machine-prefix/4",
        &MachinePrefixPreimageForTest {
            prefix_version: "cymule.machine-prefix/4",
            archive_head: &base.archive_head,
            archive_count: base.archive_count,
            archive_event_count: base.archive_event_count,
            admission_head: base.admission_head.as_deref(),
            command_index_root: &base.command_index_root,
            projection_digest: &base.projection_digest,
            projection_root: &base.projection_root,
        },
    )
    .expect("tampered prefix digest recomputes");
}

fn reauthenticate_compacted_base_and_anchor(snapshot: &mut cymule_core::MachineSnapshot) {
    reauthenticate_compacted_base(snapshot);
    let base = snapshot.base.as_ref().expect("compacted base exists");
    let base_id = base.identity().expect("tampered base identity recomputes");
    let anchor = snapshot
        .base_anchor
        .as_mut()
        .expect("compacted base anchor exists");
    anchor.base_id = base_id;
    anchor.archive_head.clone_from(&base.archive_head);
    anchor.archive_count = base.archive_count;
    anchor.archive_event_count = base.archive_event_count;
    anchor.archive_batch_count = base.batch_count;
    anchor.admission_head.clone_from(&base.admission_head);
    anchor
        .command_index_root
        .clone_from(&base.command_index_root);
    anchor.prefix_digest.clone_from(&base.prefix_digest);
    anchor.projection_digest.clone_from(&base.projection_digest);
    anchor.anchor_id = cymule_core::content_id(
        cymule_core::MachineBaseAnchor::VERSION,
        &MachineBaseAnchorPreimageForTest {
            anchor_version: cymule_core::MachineBaseAnchor::VERSION,
            base_id: &anchor.base_id,
            archive_head: &anchor.archive_head,
            archive_count: anchor.archive_count,
            archive_event_count: anchor.archive_event_count,
            archive_batch_count: anchor.archive_batch_count,
            admission_head: anchor.admission_head.as_deref(),
            command_index_root: &anchor.command_index_root,
            prefix_digest: &anchor.prefix_digest,
            projection_digest: &anchor.projection_digest,
            projection_root: &anchor.projection_root,
        },
    )
    .expect("tampered base anchor identity recomputes");
}

fn reauthenticate_last_command_admission(snapshot: &mut cymule_core::MachineSnapshot) {
    let command_id = snapshot
        .admissions
        .last()
        .expect("CommandAdmission exists")
        .command_id
        .clone();
    let record_digest =
        snapshot.command_digests().expect("command records hash")[&command_id].clone();
    let admission = snapshot
        .admissions
        .last_mut()
        .expect("CommandAdmission exists");
    admission.command_record_digest = record_digest;
    admission.admission_id = cymule_core::content_id(
        cymule_core::COMMAND_ADMISSION_VERSION,
        &CommandAdmissionPreimageForTest {
            admission_version: cymule_core::COMMAND_ADMISSION_VERSION,
            sequence: admission.sequence,
            parent_admission: &admission.parent_admission,
            command_id: &admission.command_id,
            semantic_hash: &admission.semantic_hash,
            command_record_digest: &admission.command_record_digest,
            batch_id: &admission.batch_id,
            batch_position: admission.batch_position,
            batch_len: admission.batch_len,
            before_projection_digest: &admission.before_projection_digest,
            after_projection_digest: &admission.after_projection_digest,
            status: admission.status,
            event_ids: &admission.event_ids,
        },
    )
    .expect("CommandAdmission identity recomputes");
}

fn assert_forged_migration_payload_rejected(
    authority: &Machine,
    before_migration: &Machine,
    migrated: &Event,
    run_id: &str,
    payload: EventPayload,
) {
    let EventPayload::RunMigrated {
        from_plan,
        to_plan,
        from_binding,
        to_binding,
        safe_point_id,
        target_epoch,
        target_continuation_digest,
    } = &payload
    else {
        panic!("forged payload remains a migration");
    };
    let mut command_target = before_migration.clone();
    assert!(matches!(
        command_target.submit(envelope(
            &command_target,
            90,
            run_id,
            Command::MigrateRun {
                from_plan: from_plan.clone(),
                to_plan: to_plan.clone(),
                from_binding: from_binding.clone(),
                to_binding: to_binding.clone(),
                safe_point_id: safe_point_id.clone(),
                target_epoch: *target_epoch,
                target_continuation_digest: target_continuation_digest.clone(),
            },
        )),
        Err(CoreError::Validation(_) | CoreError::IllegalTransition(_))
    ));

    let forged = Event::new(EventContent {
        command_id: migrated.command_id.clone(),
        command_hash: migrated.command_hash.clone(),
        run_id: migrated.run_id.clone(),
        parents: migrated.parents.clone(),
        reads: migrated.reads.clone(),
        writes: migrated.writes.clone(),
        coordination_key: migrated.coordination_key.clone(),
        payload,
    })
    .expect("forged migration Event remains content-addressable");
    assert!(matches!(
        replay_with_machine_authority(
            authority,
            authority.events().map(|event| {
                if event.event_id == migrated.event_id {
                    forged.clone()
                } else {
                    event.clone()
                }
            }),
        ),
        Err(CoreError::Validation(_)
            | CoreError::IllegalTransition(_)
            | CoreError::IdentityMismatch(_))
    ));

    let mut forged_snapshot =
        serde_json::to_value(authority.snapshot()).expect("legal migration snapshot encodes");
    let migrated_index = authority
        .events()
        .position(|event| event.event_id == migrated.event_id)
        .expect("migration Event is retained");
    forged_snapshot["events"][migrated_index] =
        serde_json::to_value(&forged).expect("forged Event encodes");
    forged_snapshot["commands"][&migrated.command_id]["receipt"]["event_ids"][0] =
        json!(forged.event_id);
    let admission = forged_snapshot["admissions"]
        .as_array_mut()
        .expect("snapshot admissions are ordered")
        .iter_mut()
        .find(|admission| admission["command_id"] == migrated.command_id)
        .expect("migration admission exists");
    admission["event_ids"][0] = json!(forged.event_id);
    let forged_snapshot =
        serde_json::from_value(forged_snapshot).expect("forged snapshot shape decodes");
    let error = Machine::restore(forged_snapshot).expect_err("forged migration cannot restore");
    assert!(
        matches!(
            error,
            CoreError::Validation(_)
                | CoreError::IdentityMismatch(_)
                | CoreError::IllegalTransition(_)
        ),
        "unexpected forged migration error: {error:?}"
    );
}

#[test]
fn artifact_v2_identity_is_closed_length_prefixed_and_golden() {
    let reference = cymule_core::artifact_ref("example/state", b"durable")
        .expect("closed Artifact kind derives");
    assert_eq!(reference.identity_version, "cymule.artifact/2");
    assert_eq!(
        reference.artifact_id,
        "sha256:db4f17f110d4fcb3afb606e7fdce996fc12f4cb9966e0e4956636f6294083250"
    );
    assert!(cymule_core::artifact_ref("example/state\0suffix", b"durable").is_err());
    assert!(cymule_core::artifact_ref("unversioned", b"durable").is_err());
    assert_ne!(
        cymule_core::artifact_ref("example/a", b"b\0c").expect("left derives"),
        cymule_core::artifact_ref("example/a-b", b"c").expect("right derives")
    );

    let mut machine = Machine::new();
    machine
        .put_artifact("example/state", b"durable".to_vec())
        .expect("Artifact stores");
    assert!(
        machine.snapshot().artifacts.is_empty(),
        "unadmitted material remains staged"
    );
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "artifact-identity");
    machine
        .submit(envelope(
            &machine,
            1,
            "run:artifact-identity",
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run admits its exact material");
    let mut snapshot = machine.snapshot();
    snapshot
        .artifacts
        .iter_mut()
        .find(|record| record.reference == binding)
        .expect("admitted binding is retained")
        .reference
        .identity_version = "cymule.artifact/1".to_owned();
    assert!(matches!(
        Machine::restore(snapshot),
        Err(CoreError::Validation(_))
    ));
}

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "semantic_kernel_test".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.echo".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            output_artifact_kind: COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
            requirements: BTreeMap::new(),
        }],
        effects: vec![EffectContract {
            id: "test.capture".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Mutating,
                dispatch: DispatchPolicy::OnScopeCommit,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
            requirements: BTreeMap::new(),
        }],
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "call.echo".to_owned(),
                    operation: Operation::Call {
                        component: "test.echo".to_owned(),
                        input: Expression::Input,
                        bind: Some("echoed".to_owned()),
                    },
                }],
                result: Expression::Binding {
                    name: "echoed".to_owned(),
                },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

#[test]
fn legacy_ir_v2_is_rejected_without_a_reader_or_shape_fallback() {
    assert_eq!(cymule_core::IR_VERSION, "cymule.ir/3");
    let mut legacy = candidate();
    legacy.ir_version = "cymule.ir/2".to_owned();
    assert!(matches!(
        cymule_core::seal_plan(legacy),
        Err(CoreError::Validation(message))
            if message.contains("unsupported IR version")
                && message.contains("cymule.ir/2")
                && message.contains("cymule.ir/3")
    ));
}

fn seal_for_kernel(candidate: PlanCandidate) -> Result<SealedPlan, CoreError> {
    cymule_core::seal_plan(candidate)
}

fn insert_plan(machine: &mut Machine, candidate: PlanCandidate) -> SealedPlan {
    let plan = seal_for_kernel(candidate).expect("kernel test candidate validates");
    machine
        .insert_plan(plan.clone())
        .expect("kernel test Plan inserts");
    plan
}

fn placeholder_initial_attempt() -> cymule_core::InitialAttemptSpec {
    cymule_core::InitialAttemptSpec {
        attempt_id: test_content_id("initial-attempt"),
        continuation_id: test_content_id("initial-continuation"),
        occurrence_binding: test_content_id("initial-binding"),
        continuation_epoch: 0,
        execution_fence: 1,
    }
}

fn envelope(machine: &Machine, sequence: u64, run_id: &str, command: Command) -> CommandEnvelope {
    let mut envelope = CommandEnvelope {
        command_version: COMMAND_VERSION.to_owned(),
        command_id: format!("command:{sequence}"),
        actor: "test:actor".to_owned(),
        run_id: run_id.to_owned(),
        expected_precondition: machine
            .projection()
            .runs
            .get(run_id)
            .map(cymule_core::RunProjection::precondition_token),
        command,
    };
    bind_start_material(machine, &mut envelope);
    envelope
}

fn bind_start_material(machine: &Machine, envelope: &mut CommandEnvelope) {
    if let Command::StartRun {
        plan_id,
        binding_context,
        input,
        material_digest,
        initial_attempt,
    } = &mut envelope.command
    {
        initial_attempt.attempt_id = cymule_core::content_id(
            "cymule.test.initial-attempt/1",
            &(envelope.run_id.as_str(), envelope.command_id.as_str()),
        )
        .expect("initial Attempt derives");
        initial_attempt.continuation_id = cymule_core::content_id(
            "cymule.test.initial-continuation/1",
            &(envelope.run_id.as_str(), envelope.command_id.as_str()),
        )
        .expect("initial Continuation derives");
        initial_attempt
            .occurrence_binding
            .clone_from(binding_context);
        let binding_ref = ArtifactRef {
            identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
            artifact_id: binding_context.clone(),
            kind: cymule_core::EXECUTION_BINDING_ARTIFACT_KIND.to_owned(),
        };
        if let (Some(plan), Some(binding), Some(input_record)) = (
            machine.plan(plan_id),
            machine.artifact(&binding_ref),
            machine.artifact(input),
        ) {
            let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
                envelope.command_id.clone(),
                vec![plan.clone()],
                vec![binding.clone(), input_record.clone()],
            )
            .expect("StartRun material derives");
            material_digest.clone_from(&material.material_digest().to_owned());
        }
    }
}

fn initial_attempt(machine: &Machine, run_id: &str) -> cymule_core::AttemptProjection {
    let run = &machine.projection().runs[run_id];
    assert_eq!(
        run.attempts.len(),
        1,
        "StartRun owns exactly one initial Attempt"
    );
    let attempt = run
        .attempts
        .values()
        .next()
        .expect("StartRun has an initial Attempt")
        .clone();
    assert!(
        attempt.active,
        "the initial Attempt remains active until explicitly yielded"
    );
    attempt
}

fn revision_candidate(revision: &str) -> PlanCandidate {
    let mut plan = candidate();
    plan.metadata
        .insert("revision".to_owned(), revision.to_owned());
    plan
}

fn staging_machine_fixture(run_id: &str) -> (Machine, SealedPlan, ArtifactRef) {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "staging-initial");
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("staging fixture starts with exactly its initial material");
    (machine, plan, binding)
}

#[test]
fn future_staged_material_is_readable_without_changing_committed_authority() {
    let (mut machine, _, _) = staging_machine_fixture("run:future-staging");
    let before = machine.snapshot();
    let authority = machine
        .authority_root()
        .expect("committed authority derives");
    let future = insert_plan(&mut machine, revision_candidate("future-only"));
    let artifact = machine
        .put_artifact("test/staged-future", b"future".to_vec())
        .expect("future Artifact stages");
    machine
        .insert_plan(future.clone())
        .expect("identical future Plan is idempotent");
    assert_eq!(
        machine
            .put_artifact("test/staged-future", b"future".to_vec())
            .expect("identical Artifact stages"),
        artifact
    );
    assert_eq!(machine.plan(&future.plan_id), Some(&future));
    assert_eq!(
        machine
            .artifact(&artifact)
            .expect("future bytes remain readable")
            .bytes,
        b"future"
    );
    assert_eq!(machine.snapshot(), before);
    assert_eq!(
        machine.authority_root().expect("staging has no authority"),
        authority
    );
    let reopened = Machine::restore(before).expect("committed snapshot reopens");
    assert!(reopened.plan(&future.plan_id).is_none());
    assert!(reopened.artifact(&artifact).is_none());
}

#[test]
fn a_command_admits_only_its_exact_staged_plan_and_artifact_members() {
    let run_id = "run:exact-staged-members";
    let (mut machine, source, source_binding) = staging_machine_fixture(run_id);
    let selected = insert_plan(&mut machine, revision_candidate("selected"));
    let selected_binding = machine
        .put_artifact(
            cymule_core::EXECUTION_BINDING_ARTIFACT_KIND,
            b"selected-binding".to_vec(),
        )
        .expect("selected binding stages");
    let future = insert_plan(&mut machine, revision_candidate("not-selected"));
    let future_artifact = machine
        .put_artifact("test/staged-future", b"not-selected".to_vec())
        .expect("unrelated future Artifact stages");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("the exact migration fixture explicitly yields its initial Attempt");
    let before = machine.snapshot();
    assert_eq!(before.plans.as_slice(), std::slice::from_ref(&source));
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::MigrateRun {
                from_plan: source.plan_id.clone(),
                to_plan: selected.plan_id.clone(),
                from_binding: source_binding.artifact_id,
                to_binding: selected_binding.artifact_id.clone(),
                safe_point_id: test_content_id("exact-staged-members-safe-point"),
                target_epoch: 1,
                target_continuation_digest: "a".repeat(64),
            },
        ))
        .expect("migration admits only its selected Plan and binding");
    let admitted = machine.snapshot();
    assert_eq!(admitted.plans, [source, selected]);
    assert!(admitted.artifacts.starts_with(&before.artifacts));
    let added = admitted.artifacts[before.artifacts.len()..]
        .iter()
        .map(|record| record.reference.clone())
        .collect::<Vec<_>>();
    assert_eq!(added, [selected_binding]);
    assert_eq!(machine.plan(&future.plan_id), Some(&future));
    assert_eq!(
        machine
            .artifact(&future_artifact)
            .expect("future Artifact remains staged")
            .bytes,
        b"not-selected"
    );
    assert!(
        !admitted
            .artifacts
            .iter()
            .any(|record| record.reference == future_artifact)
    );
    machine
        .verify_replay()
        .expect("unselected staged material is absent from canonical replay");
}

#[test]
fn rejected_commands_restore_exact_staging_for_retry() {
    let run_id = "run:staging-rollback";
    let (mut machine, source, source_binding) = staging_machine_fixture(run_id);
    let future = insert_plan(&mut machine, revision_candidate("retained-stage"));
    let binding = machine
        .put_artifact(
            cymule_core::EXECUTION_BINDING_ARTIFACT_KIND,
            b"retry-binding".to_vec(),
        )
        .expect("future binding stages before admission");
    let result = machine
        .put_artifact("test/staged-result", b"held".to_vec())
        .expect("result stages");
    assert_rejected_completion_preserves_staging(&mut machine, run_id, &result);
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("migration rollback fixture explicitly yields initial execution");
    let before = machine.snapshot();
    let authority = machine.authority_root().expect("parent authority derives");
    let update = envelope(
        &machine,
        91,
        run_id,
        Command::MigrateRun {
            from_plan: source.plan_id,
            to_plan: future.plan_id.clone(),
            from_binding: source_binding.artifact_id,
            to_binding: binding.artifact_id.clone(),
            safe_point_id: test_content_id("staging-rollback-safe-point"),
            target_epoch: 1,
            target_continuation_digest: "b".repeat(64),
        },
    );
    let mut illegal = update.clone();
    illegal.command_id = "command:invalid-staged-migration".to_owned();
    let Command::MigrateRun { target_epoch, .. } = &mut illegal.command else {
        panic!("fixture is a migration");
    };
    *target_epoch = 2;
    assert!(matches!(
        machine.submit(illegal),
        Err(CoreError::IllegalTransition(message))
            if message == "Run migration target epoch is not the exact next epoch"
    ));
    assert_eq!(machine.snapshot(), before);
    assert_eq!(
        machine
            .authority_root()
            .expect("failed admission restores exact parent"),
        authority
    );
    assert_eq!(machine.plan(&future.plan_id), Some(&future));
    assert_eq!(
        machine
            .artifact(&binding)
            .expect("consumed preexisting stage is restored")
            .bytes,
        b"retry-binding"
    );
    assert!(machine.artifact(&result).is_some());
    let receipt = machine
        .submit(update.clone())
        .expect("the retained staged material admits its exact migration");
    assert_eq!(receipt.status, CommandReceiptStatus::Applied);
    let committed = machine.snapshot();
    assert_eq!(
        machine
            .submit(update)
            .expect("retry returns the same receipt"),
        receipt
    );
    assert_eq!(machine.snapshot(), committed);
    assert_eq!(machine.plan(&future.plan_id), Some(&future));
    assert!(machine.artifact(&result).is_some());
}

fn assert_rejected_completion_preserves_staging(
    machine: &mut Machine,
    run_id: &str,
    result: &ArtifactRef,
) {
    let before = machine.snapshot();
    let authority = machine
        .authority_root()
        .expect("rejected command parent derives");
    let illegal = envelope(
        machine,
        90,
        run_id,
        Command::CompleteRun {
            result: Some(result.clone()),
        },
    );
    for _ in 0..2 {
        assert!(matches!(
            machine.submit(illegal.clone()),
            Err(CoreError::IllegalTransition(message)) if message.contains("Attempt remains active")
        ));
        assert_eq!(machine.snapshot(), before);
        assert_eq!(
            machine
                .authority_root()
                .expect("illegal command rolls back material"),
            authority
        );
        assert_eq!(
            machine
                .artifact(result)
                .expect("rejected material is staged again")
                .bytes,
            b"held"
        );
    }
}

#[test]
fn repeated_compaction_preserves_authority_and_exact_batches_with_a_hot_suffix() {
    let run_id = "run:compaction-authority-stability";
    let (mut machine, _, _) = staging_machine_fixture(run_id);
    for sequence in 2..=5 {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::RecordFact {
                    key: format!("stable:{sequence}"),
                    value: test_fact_value(&format!("stable:{sequence}")),
                },
            ))
            .expect("ordered fact admits before compaction");
    }
    let authority = machine
        .authority_root()
        .expect("complete history authority derives");
    let original_batches = machine.snapshot().batches;
    let projection = machine.projection().clone();
    let first = machine
        .compact_event_history(3)
        .expect("first prefix compacts with three hot Events");
    assert_eq!(first.retained_events, 3);
    assert_eq!(
        machine
            .authority_root()
            .expect("first compacted authority derives"),
        authority
    );
    let second = machine
        .compact_event_history(1)
        .expect("second prefix compacts with one hot Event");
    assert_eq!(second.retained_events, 1);
    assert_eq!(
        machine
            .authority_root()
            .expect("second compacted authority derives"),
        authority
    );
    let snapshot = machine.snapshot();
    let retained_batches = first
        .archive_segment
        .batches
        .iter()
        .chain(&second.archive_segment.batches)
        .chain(&snapshot.batches)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        retained_batches, original_batches,
        "manifest/source/result identities and all receipt bytes are unchanged"
    );
    let segments = [
        first.archive_segment.clone(),
        second.archive_segment.clone(),
    ];
    let restored = Machine::restore_with_archive(snapshot.clone(), segments)
        .expect("complete two-segment audit restores");
    assert_eq!(restored.projection(), &projection);
    assert_eq!(
        restored
            .authority_root()
            .expect("archived replay authority derives"),
        authority
    );
    let anchor = machine
        .base_anchor()
        .expect("anchor resolves")
        .expect("compaction retains an anchor");
    let anchored = Machine::restore_anchored(snapshot.clone(), &anchor)
        .expect("exact anchor restores hot suffix only");
    assert_eq!(anchored.projection(), &projection);
    assert_eq!(
        anchored
            .authority_root()
            .expect("anchored authority derives"),
        authority
    );
    let mut wrong_anchor = anchor;
    wrong_anchor.prefix_digest = test_content_id("forged-compacted-prefix");
    assert!(Machine::restore_anchored(snapshot.clone(), &wrong_anchor).is_err());
    let mut wrong_segment = first.archive_segment;
    wrong_segment.entries[0].command.receipt.event_ids[0] =
        test_content_id("forged-archived-Event");
    assert!(
        Machine::restore_with_archive(snapshot, [wrong_segment, second.archive_segment]).is_err()
    );
}

fn assert_required_nullable_fields<T>(value: &serde_json::Value, fields: &[&str])
where
    T: serde::de::DeserializeOwned,
{
    for field in fields {
        let mut explicit_null = value.clone();
        explicit_null
            .as_object_mut()
            .expect("required-nullable wire value is an object")
            .insert((*field).to_owned(), serde_json::Value::Null);
        if let Err(error) = serde_json::from_value::<T>(explicit_null) {
            panic!("required-nullable field {field} rejected explicit null: {error}");
        }

        let mut missing = value.clone();
        missing
            .as_object_mut()
            .expect("required-nullable wire value is an object")
            .remove(*field);
        match serde_json::from_value::<T>(missing) {
            Err(error) => assert!(
                error.to_string().contains("missing field"),
                "required-nullable field {field} failed for an unrelated reason: {error}"
            ),
            Ok(_) => panic!("required-nullable field {field} accepted an absent member"),
        }
    }
}

#[test]
fn core_required_nullable_wire_members_reject_absence_and_accept_null() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let execution_binding = test_execution_binding(&mut machine, "required-nullable");
    let run_id = "run:required-nullable";
    let start = envelope(
        &machine,
        1,
        run_id,
        Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: execution_binding.artifact_id,
            input: test_run_input_ref(),
            material_digest: String::new(),
            initial_attempt: placeholder_initial_attempt(),
        },
    );

    assert_required_nullable_fields::<CommandEnvelope>(
        &serde_json::to_value(&start).expect("CommandEnvelope encodes"),
        &["expected_precondition"],
    );
    assert_required_nullable_fields::<Command>(
        &serde_json::to_value(Command::CompleteRun { result: None })
            .expect("CompleteRun command encodes"),
        &["result"],
    );
    assert_required_nullable_fields::<EventPayload>(
        &serde_json::to_value(EventPayload::RunCompleted { result: None })
            .expect("RunCompleted payload encodes"),
        &["result"],
    );

    let receipt = machine.submit(start).expect("Run starts");
    let event = machine.events().next().expect("start Event exists");
    let run = &machine.projection().runs[run_id];
    let root_scope = &run.scopes[cymule_core::ROOT_SCOPE_ID];

    assert_required_nullable_fields::<Event>(
        &serde_json::to_value(event).expect("Event encodes"),
        &["coordination_key"],
    );
    assert_required_nullable_fields::<cymule_core::CommandReceipt>(
        &serde_json::to_value(receipt).expect("CommandReceipt encodes"),
        &[
            "error_code",
            "message",
            "observed_precondition",
            "current_precondition",
        ],
    );
    assert_required_nullable_fields::<cymule_core::RunProjection>(
        &serde_json::to_value(run).expect("RunProjection encodes"),
        &["result"],
    );
    assert_required_nullable_fields::<cymule_core::ScopeProjection>(
        &serde_json::to_value(root_scope).expect("ScopeProjection encodes"),
        &["parent_scope", "site_id"],
    );
}

fn observational_unknown_machine(run_id: &str) -> (Machine, String) {
    let mut observational = candidate();
    observational.effects[0].profile.mutation = MutationKind::Observational;
    observational.effects[0].profile.dispatch = DispatchPolicy::Eager;
    observational.definitions[0].body = Region {
        steps: vec![Step {
            id: "effect.capture".to_owned(),
            operation: Operation::Effect {
                effect: "test.capture".to_owned(),
                input: Expression::Input,
                occurrence: "capture".to_owned(),
                bind: None,
            },
        }],
        result: Expression::Literal { value: json!(null) },
    };

    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, observational);
    let execution_binding = test_execution_binding(&mut machine, "observational-unknown");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("effect arguments store");
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let invocation_id = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id,
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "effect.capture".to_owned(),
                occurrence: "capture".to_owned(),
                operation: "test.capture".to_owned(),
                args,
                execution_binding,
                occurrence_binding: test_content_id("binding:observational-unknown/1"),
            },
        ))
        .expect("observational Effect admits");
    let intent_id = machine.projection().runs[run_id]
        .effects
        .keys()
        .next()
        .expect("observational Effect exists")
        .clone();
    for (sequence, transition) in [
        (3, EffectTransition::Prepare),
        (4, EffectTransition::AuthorizeRelease),
        (5, EffectTransition::StartDispatch),
        (6, EffectTransition::Observe(WorldOutcome::Unknown)),
    ] {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::TransitionEffect {
                    intent_id: intent_id.clone(),
                    transition,
                },
            ))
            .expect("observational Effect reaches unknown world state");
    }
    machine
        .submit(envelope(
            &machine,
            7,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("scope commits without a mutating obligation");
    (machine, intent_id)
}

fn root_effect_candidate() -> PlanCandidate {
    let mut value = candidate();
    value.definitions[0].body = Region {
        steps: vec![Step {
            id: "effect.capture".to_owned(),
            operation: Operation::Effect {
                effect: "test.capture".to_owned(),
                input: Expression::Input,
                occurrence: "capture".to_owned(),
                bind: None,
            },
        }],
        result: Expression::Literal { value: json!(null) },
    };
    value
}

fn propose_root_effect(
    machine: &mut Machine,
    sequence: u64,
    run_id: &str,
    args: ArtifactRef,
    execution_binding: ArtifactRef,
) -> String {
    let run = &machine.projection().runs[run_id];
    let entry_definition = machine
        .plan(&run.current_plan)
        .expect("current effect Plan exists")
        .candidate
        .entry
        .clone();
    let invocation_id =
        cymule_core::plan_invocation_id(run_id, &run.current_plan, &entry_definition, &[])
            .expect("root invocation derives");
    machine
        .submit(envelope(
            machine,
            sequence,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id,
                invocation_path: Vec::new(),
                definition_id: entry_definition,
                region_path: Vec::new(),
                site_id: "effect.capture".to_owned(),
                occurrence: "capture".to_owned(),
                operation: "test.capture".to_owned(),
                args,
                execution_binding,
                occurrence_binding: test_content_id("binding:root-effect/1"),
            },
        ))
        .expect("root Effect proposes");
    machine.projection().runs[run_id]
        .effects
        .keys()
        .last()
        .expect("root Effect exists")
        .clone()
}

#[test]
fn effect_args_share_one_strict_kind_canonical_schema_authority() {
    let fixture = strict_effect_args_fixture();
    assert_invalid_effect_args_events(&fixture);
    assert_invalid_effect_args_anchored_restore(fixture);
}

struct StrictEffectArgsFixture {
    machine: Machine,
    run_id: &'static str,
    execution_binding: ArtifactRef,
    start: Event,
    valid_event: Event,
    wrong_kind: ArtifactRef,
    duplicate: ArtifactRef,
    schema_invalid: ArtifactRef,
}

fn strict_effect_args_fixture() -> StrictEffectArgsFixture {
    let mut machine = Machine::new();
    let mut effect_candidate = root_effect_candidate();
    effect_candidate.effects[0].input_schema = json!({
        "type": "object",
        "required": ["value"],
        "properties": {"value": {"type": "integer"}},
        "additionalProperties": false
    });
    let plan = insert_plan(&mut machine, effect_candidate);
    let execution_binding = test_execution_binding(&mut machine, "strict-effect-args");
    let run_id = "run:strict-effect-args";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let start = machine.events().next().expect("Run start exists").clone();
    let admitted = machine.snapshot();
    let [
        wrong_kind,
        duplicate,
        noncanonical,
        schema_invalid,
        unsafe_integer,
        non_json,
    ] = invalid_effect_argument_artifacts(&mut machine);
    for (sequence, args) in [
        (2, wrong_kind.clone()),
        (3, duplicate.clone()),
        (4, noncanonical),
        (5, schema_invalid.clone()),
        (6, unsafe_integer),
        (7, non_json),
    ] {
        assert!(
            machine
                .submit(strict_effect_args_command(
                    &machine,
                    sequence,
                    run_id,
                    &execution_binding,
                    args
                ))
                .is_err(),
            "invalid Effect args at sequence {sequence} must fail closed"
        );
        assert_eq!(
            machine.snapshot(),
            admitted,
            "rejection admits no material or Events"
        );
    }

    let valid = machine
        .put_artifact(
            cymule_core::EFFECT_ARGS_ARTIFACT_KIND,
            br#"{"value":1}"#.to_vec(),
        )
        .expect("valid args store");
    machine
        .submit(strict_effect_args_command(
            &machine,
            8,
            run_id,
            &execution_binding,
            valid,
        ))
        .expect("strict canonical schema-valid args admit");
    let valid_event = machine
        .events()
        .last()
        .expect("Effect Event exists")
        .clone();
    StrictEffectArgsFixture {
        machine,
        run_id,
        execution_binding,
        start,
        valid_event,
        wrong_kind,
        duplicate,
        schema_invalid,
    }
}

fn invalid_effect_argument_artifacts(machine: &mut Machine) -> [ArtifactRef; 6] {
    let wrong_kind = machine
        .put_artifact("test.effect-args/1", br#"{"value":1}"#.to_vec())
        .expect("wrong-kind Artifact stores");
    let duplicate = machine
        .put_artifact(
            cymule_core::EFFECT_ARGS_ARTIFACT_KIND,
            br#"{"value":1,"value":2}"#.to_vec(),
        )
        .expect("duplicate-key Artifact stores");
    let noncanonical = machine
        .put_artifact(
            cymule_core::EFFECT_ARGS_ARTIFACT_KIND,
            br#"{ "value": 1 }"#.to_vec(),
        )
        .expect("noncanonical Artifact stores");
    let schema_invalid = machine
        .put_artifact(
            cymule_core::EFFECT_ARGS_ARTIFACT_KIND,
            br#"{"value":"wrong"}"#.to_vec(),
        )
        .expect("schema-invalid Artifact stores");
    let unsafe_integer = machine
        .put_artifact(
            cymule_core::EFFECT_ARGS_ARTIFACT_KIND,
            br#"{"value":9007199254740992}"#.to_vec(),
        )
        .expect("unsafe-integer Artifact stores");
    let non_json = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"not-json".to_vec())
        .expect("non-JSON Artifact stores");
    [
        wrong_kind,
        duplicate,
        noncanonical,
        schema_invalid,
        unsafe_integer,
        non_json,
    ]
}

fn strict_effect_args_command(
    machine: &Machine,
    sequence: u64,
    run_id: &str,
    execution_binding: &ArtifactRef,
    args: ArtifactRef,
) -> CommandEnvelope {
    let run = &machine.projection().runs[run_id];
    let invocation_id = cymule_core::plan_invocation_id(run_id, &run.current_plan, "main", &[])
        .expect("root invocation derives");
    envelope(
        machine,
        sequence,
        run_id,
        Command::ProposeEffect {
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            invocation_id,
            invocation_path: Vec::new(),
            definition_id: "main".to_owned(),
            region_path: Vec::new(),
            site_id: "effect.capture".to_owned(),
            occurrence: "capture".to_owned(),
            operation: "test.capture".to_owned(),
            args,
            execution_binding: execution_binding.clone(),
            occurrence_binding: test_content_id("binding:strict-effect-args/1"),
        },
    )
}

fn assert_invalid_effect_args_events(fixture: &StrictEffectArgsFixture) {
    let StrictEffectArgsFixture {
        machine,
        start,
        valid_event,
        wrong_kind,
        duplicate,
        ..
    } = fixture;
    let mut wrong_kind_payload = valid_event.payload.clone();
    let EventPayload::EffectProposed { args, .. } = &mut wrong_kind_payload else {
        panic!("Effect Event payload exists");
    };
    **args = wrong_kind.clone();
    let wrong_kind_event = Event::new(EventContent {
        command_id: valid_event.command_id.clone(),
        command_hash: valid_event.command_hash.clone(),
        run_id: valid_event.run_id.clone(),
        parents: valid_event.parents.clone(),
        reads: valid_event.reads.clone(),
        writes: valid_event.writes.clone(),
        coordination_key: valid_event.coordination_key.clone(),
        payload: wrong_kind_payload,
    })
    .expect("wrong-kind Event remains content-addressable");
    assert!(matches!(
        wrong_kind_event.verify(),
        Err(CoreError::Validation(message)) if message.contains("exact kind")
    ));

    let mut duplicate_payload = valid_event.payload.clone();
    let EventPayload::EffectProposed { args, .. } = &mut duplicate_payload else {
        panic!("Effect Event payload exists");
    };
    **args = duplicate.clone();
    let duplicate_event = Event::new(EventContent {
        command_id: valid_event.command_id.clone(),
        command_hash: valid_event.command_hash.clone(),
        run_id: valid_event.run_id.clone(),
        parents: valid_event.parents.clone(),
        reads: valid_event.reads.clone(),
        writes: valid_event.writes.clone(),
        coordination_key: valid_event.coordination_key.clone(),
        payload: duplicate_payload,
    })
    .expect("duplicate-key Event remains content-addressable");
    assert!(replay_with_machine_authority(machine, [start.clone(), duplicate_event]).is_err());
}

fn assert_invalid_effect_args_anchored_restore(fixture: StrictEffectArgsFixture) {
    let StrictEffectArgsFixture {
        mut machine,
        run_id,
        execution_binding,
        schema_invalid,
        ..
    } = fixture;
    // Retain these bytes through an unrelated opaque-evidence command so the
    // anchored probe below isolates the Effect Plan schema, not missing storage.
    let evidence_run = "run:strict-effect-args-evidence";
    machine
        .submit(envelope(
            &machine,
            90,
            evidence_run,
            Command::StartRun {
                plan_id: machine.projection().runs[run_id].current_plan.clone(),
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("opaque-evidence Run starts");
    machine
        .submit(envelope(
            &machine,
            91,
            evidence_run,
            Command::CancelRun {
                reason: schema_invalid.clone(),
            },
        ))
        .expect("schema-invalid Effect arguments remain legal opaque evidence");
    let compaction = machine
        .compact_event_history(0)
        .expect("valid Effect history compacts");
    let mut forged_base = machine.snapshot();
    let run = forged_base
        .base
        .as_mut()
        .expect("compacted base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists");
    let old_intent = run.effects.keys().next().expect("Effect exists").clone();
    let mut effect = run.effects.remove(&old_intent).expect("Effect removes");
    effect.args = schema_invalid;
    let forged_intent = effect_intent_id(&EffectIntentIdentityInput {
        run_id,
        plan_id: &effect.origin_plan_id,
        invocation_id: &effect.invocation_id,
        site_id: &effect.site_id,
        scope_id: &effect.scope_id,
        occurrence: &effect.occurrence,
        args: &effect.args,
        effect_schema_version: &effect.effect_schema_version,
    })
    .expect("schema-invalid args still derive a structural intent");
    effect.intent_id.clone_from(&forged_intent);
    let scope = run
        .scopes
        .get_mut(cymule_core::ROOT_SCOPE_ID)
        .expect("root scope exists");
    scope.intents.remove(&old_intent);
    scope.intents.insert(forged_intent.clone());
    let intent_position = scope
        .intent_order
        .iter()
        .position(|intent_id| intent_id == &old_intent)
        .expect("root Effect appears in proposal order");
    scope.intent_order[intent_position].clone_from(&forged_intent);
    run.effects.insert(forged_intent, effect);
    reauthenticate_compacted_base_and_anchor(&mut forged_base);
    let forged_anchor = forged_base
        .base_anchor
        .clone()
        .expect("forged exact anchor exists");
    assert!(matches!(
        Machine::restore_anchored(forged_base.clone(), &forged_anchor),
        Err(CoreError::Validation(message)) if message.contains("Plan schema")
    ));
    assert!(Machine::restore_with_archive(forged_base, [compaction.archive_segment]).is_err());
}

#[test]
fn attempt_epoch_and_fence_reject_cross_language_integer_overflow() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "exact-attempt-numbers");
    let run_id = "run:exact-attempt-numbers";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before invalid BeginAttempt numeric probes");
    let before_invalid = machine.snapshot();

    for (index, (epoch, fence)) in [
        (MAX_EXACT_INTEGER + 1, 1),
        (0, 0),
        (0, MAX_EXACT_INTEGER + 1),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(matches!(
            machine.submit(envelope(
                &machine,
                2 + u64::try_from(index).expect("small test index fits"),
                run_id,
                Command::BeginAttempt {
                    attempt_id: test_content_id(&format!("attempt:{epoch}:{fence}")),
                    continuation_id: test_content_id(&format!("continuation:{run_id}")),
                    occurrence_binding: test_content_id("binding:v1"),
                    continuation_epoch: epoch,
                    execution_fence: fence,
                },
            )),
            Err(CoreError::Validation(_) | CoreError::Encoding(_))
        ));
    }
    assert_eq!(machine.snapshot(), before_invalid);
}

#[test]
fn plan_identity_is_canonical_and_tamper_evident() {
    let first = seal_for_kernel(candidate()).expect("candidate seals");
    let mut reordered = candidate();
    reordered.metadata.insert("z".to_owned(), "last".to_owned());
    reordered
        .metadata
        .insert("a".to_owned(), "first".to_owned());
    let reordered = seal_for_kernel(reordered).expect("candidate seals");
    let mut same = candidate();
    same.metadata.insert("a".to_owned(), "first".to_owned());
    same.metadata.insert("z".to_owned(), "last".to_owned());
    assert_eq!(
        reordered.plan_id,
        seal_for_kernel(same).expect("candidate seals").plan_id
    );

    let mut tampered = first;
    tampered.candidate.name = "tampered".to_owned();
    assert!(matches!(
        tampered.verify(),
        Err(CoreError::IdentityMismatch(_))
    ));

    let mut invalid = candidate();
    let Operation::Call { component, .. } = &mut invalid.definitions[0].body.steps[0].operation
    else {
        panic!("fixture call exists");
    };
    *component = "missing.component".to_owned();
    assert!(matches!(
        seal_for_kernel(invalid),
        Err(CoreError::Validation(_))
    ));
}

#[test]
fn plan_numbers_are_normalized_and_unsafe_integers_never_reach_identity() {
    let mut integer = candidate();
    integer.definitions[0].body.result = Expression::Literal { value: json!(1) };
    let integer = seal_for_kernel(integer).expect("safe integer Plan seals");

    let mut integral_float = candidate();
    integral_float.definitions[0].body.result = Expression::Literal { value: json!(1.0) };
    let integral_float = seal_for_kernel(integral_float).expect("integral float Plan seals");
    assert_eq!(integral_float.plan_id, integer.plan_id);
    assert_eq!(integral_float.candidate, integer.candidate);

    let mut fractional = candidate();
    fractional.definitions[0].body.result = Expression::Literal { value: json!(1.25) };
    let fractional = seal_for_kernel(fractional).expect("finite fractional Plan seals");
    let Expression::Literal { value } = &fractional.candidate.definitions[0].body.result else {
        panic!("fractional literal remains a literal");
    };
    assert_eq!(value.as_f64(), Some(1.25));

    for unsafe_integer in [9_007_199_254_740_992_u64, 9_007_199_254_740_993_u64] {
        let mut plan = candidate();
        plan.definitions[0].body.result = Expression::Literal {
            value: json!({"nested": [unsafe_integer]}),
        };
        assert!(matches!(
            seal_for_kernel(plan),
            Err(CoreError::Encoding(message))
                if message.contains("exact cross-language range")
        ));
    }

    let unsafe_value = json!(9_007_199_254_740_992_u64);
    let mut component_schema = candidate();
    component_schema.components[0].input_schema = json!({"minimum": unsafe_value.clone()});
    assert!(seal_for_kernel(component_schema).is_err());

    let mut effect_schema = candidate();
    effect_schema.effects[0].input_schema = json!({"minimum": unsafe_value.clone()});
    assert!(seal_for_kernel(effect_schema).is_err());

    let mut definition_schema = candidate();
    definition_schema.definitions[0].output_schema = json!({"minimum": unsafe_value.clone()});
    assert!(seal_for_kernel(definition_schema).is_err());

    let mut wait_schema = candidate();
    wait_schema.definitions[0].body.steps.push(Step {
        id: "wait.unsafe-schema".to_owned(),
        operation: Operation::Wait {
            wait: cymule_core::WaitSpec::Input {
                correlation: "unsafe-schema".to_owned(),
                schema: json!({"minimum": unsafe_value}),
            },
            bind: None,
        },
    });
    assert!(seal_for_kernel(wait_schema).is_err());

    let mut safe_boundaries = candidate();
    safe_boundaries.components[0].input_schema = json!({
        "minimum": -9_007_199_254_740_991_i64,
        "maximum": 9_007_199_254_740_991_u64
    });
    seal_for_kernel(safe_boundaries).expect("safe integer boundaries seal");

    let mut noncanonical = integer.clone();
    noncanonical.candidate.definitions[0].body.result = Expression::Literal { value: json!(1.0) };
    assert!(matches!(
        noncanonical.verify(),
        Err(CoreError::Validation(message))
            if message.contains("canonical JSON number forms")
    ));
}

#[test]
fn flattened_step_and_closed_ir_unions_reject_unknown_members() {
    let mut value = serde_json::to_value(candidate()).expect("candidate encodes");
    value["definitions"][0]["body"]["steps"][0]["unexpected"] = json!(true);
    assert!(serde_json::from_value::<PlanCandidate>(value).is_err());

    let mut expression = json!({"kind": "literal", "value": null, "unexpected": true});
    assert!(serde_json::from_value::<Expression>(expression.clone()).is_err());
    expression
        .as_object_mut()
        .expect("object")
        .remove("unexpected");
    assert!(serde_json::from_value::<Expression>(expression).is_ok());

    let scope = json!({
        "op": "scope",
        "body": {"steps": [], "result": {"kind": "literal", "value": null}}
    });
    assert!(serde_json::from_value::<Operation>(scope.clone()).is_ok());
    for legacy_mode in ["transactional", "speculative"] {
        let mut legacy = scope.clone();
        legacy["mode"] = json!(legacy_mode);
        assert!(serde_json::from_value::<Operation>(legacy).is_err());
    }
}

#[test]
fn public_validation_errors_and_effect_policy_boundaries_are_stable() {
    assert_public_error_codes_and_plan_identity();
    assert_effect_result_binding_policy();
    assert_nested_effect_result_binding_policy();
    assert_definition_wait_and_binding_validation();
}

fn assert_public_error_codes_and_plan_identity() {
    let cases = [
        (
            CoreError::Validation("v".to_owned()),
            "validation_failed: v",
        ),
        (CoreError::NotFound("n".to_owned()), "not_found: n"),
        (
            CoreError::IdentityMismatch("i".to_owned()),
            "identity_mismatch: i",
        ),
        (
            CoreError::IllegalTransition("t".to_owned()),
            "illegal_transition: t",
        ),
        (
            CoreError::CommandReuse("r".to_owned()),
            "command_id_reused: r",
        ),
        (CoreError::Causal("c".to_owned()), "causal_error: c"),
        (CoreError::Encoding("e".to_owned()), "encoding_failed: e"),
    ];
    for (error, expected) in cases {
        assert_eq!(error.to_string(), expected);
        assert_eq!(error.code(), expected.split(':').next().expect("code"));
    }

    let mut invalid_id = candidate();
    invalid_id.name.clear();
    assert!(matches!(
        invalid_id.validate(),
        Err(CoreError::Validation(message)) if message.contains("plan name")
    ));
}

fn assert_effect_result_binding_policy() {
    let mut mutating_eager = candidate();
    mutating_eager.effects[0].profile.dispatch = DispatchPolicy::Eager;
    assert!(matches!(
        mutating_eager.validate(),
        Err(CoreError::Validation(message)) if message.contains("cannot use eager")
    ));

    let mut observational_eager = candidate();
    observational_eager.effects[0].profile.mutation = MutationKind::Observational;
    observational_eager.effects[0].profile.dispatch = DispatchPolicy::Eager;
    observational_eager
        .validate()
        .expect("observational eager effect is legal");
    candidate()
        .validate()
        .expect("mutating commit-gated effect is legal");

    for (mutation, dispatch) in [
        (MutationKind::Mutating, DispatchPolicy::OnScopeCommit),
        (MutationKind::Mutating, DispatchPolicy::Explicit),
        (MutationKind::Observational, DispatchPolicy::OnScopeCommit),
        (MutationKind::Observational, DispatchPolicy::Explicit),
    ] {
        let mut bound_deferred = candidate();
        bound_deferred.effects[0].profile.mutation = mutation;
        bound_deferred.effects[0].profile.dispatch = dispatch;
        bound_deferred.definitions[0].body.steps.push(Step {
            id: "effect.bound-deferred".to_owned(),
            operation: Operation::Effect {
                effect: "test.capture".to_owned(),
                input: Expression::Input,
                occurrence: "bound-deferred".to_owned(),
                bind: Some("forbidden_result".to_owned()),
            },
        });
        assert!(matches!(
            bound_deferred.validate(),
            Err(CoreError::Validation(message))
                if message.contains("may bind only for observational eager dispatch")
        ));
    }

    let mut bound_observation = candidate();
    bound_observation.effects[0].profile.mutation = MutationKind::Observational;
    bound_observation.effects[0].profile.dispatch = DispatchPolicy::Eager;
    bound_observation.definitions[0].body.steps.push(Step {
        id: "effect.bound-observation".to_owned(),
        operation: Operation::Effect {
            effect: "test.capture".to_owned(),
            input: Expression::Input,
            occurrence: "bound-observation".to_owned(),
            bind: Some("observed".to_owned()),
        },
    });
    bound_observation
        .validate()
        .expect("an eager observation may bind its settled result");
}

fn assert_nested_effect_result_binding_policy() {
    let mut nested_definition = candidate();
    nested_definition.definitions.push(Definition {
        id: "nested".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: vec![Step {
                id: "scope.bound-deferred".to_owned(),
                operation: Operation::Scope {
                    body: Box::new(Region {
                        steps: vec![Step {
                            id: "effect.nested-bound-deferred".to_owned(),
                            operation: Operation::Effect {
                                effect: "test.capture".to_owned(),
                                input: Expression::Input,
                                occurrence: "nested-bound-deferred".to_owned(),
                                bind: Some("forbidden_nested_result".to_owned()),
                            },
                        }],
                        result: Expression::Input,
                    }),
                    bind: None,
                },
            }],
            result: Expression::Input,
        },
    });
    assert!(matches!(
        nested_definition.validate(),
        Err(CoreError::Validation(message))
            if message.contains("effect.nested-bound-deferred")
                && message.contains("may bind only for observational eager dispatch")
    ));
}

fn assert_definition_wait_and_binding_validation() {
    let mut unknown_effect = candidate();
    unknown_effect.definitions[0].body.steps.push(Step {
        id: "effect.unknown".to_owned(),
        operation: Operation::Effect {
            effect: "missing.effect".to_owned(),
            input: Expression::Input,
            occurrence: "first".to_owned(),
            bind: None,
        },
    });
    assert!(matches!(
        unknown_effect.validate(),
        Err(CoreError::Validation(message)) if message.contains("unknown effect")
    ));

    let mut unknown_definition = candidate();
    unknown_definition.definitions[0].body.steps.push(Step {
        id: "invoke.unknown".to_owned(),
        operation: Operation::Invoke {
            definition: "missing.definition".to_owned(),
            input: Expression::Input,
            bind: Some("invoked".to_owned()),
        },
    });
    assert!(matches!(
        unknown_definition.validate(),
        Err(CoreError::Validation(message)) if message.contains("unknown definition")
    ));

    let mut invalid_wait = candidate();
    invalid_wait.definitions[0].body.steps.push(Step {
        id: "wait.invalid".to_owned(),
        operation: Operation::Wait {
            wait: cymule_core::WaitSpec::Signal {
                key: String::new(),
                consume_once: true,
            },
            bind: Some("wait_result".to_owned()),
        },
    });
    assert!(matches!(
        invalid_wait.validate(),
        Err(CoreError::Validation(message)) if message.contains("signal key")
    ));

    let mut ignored_wait_result = candidate();
    ignored_wait_result.definitions[0].body.steps.push(Step {
        id: "wait.ignored".to_owned(),
        operation: Operation::Wait {
            wait: cymule_core::WaitSpec::Signal {
                key: "signal:ignored".to_owned(),
                consume_once: false,
            },
            bind: None,
        },
    });
    ignored_wait_result
        .validate()
        .expect("wait results may be intentionally ignored");

    let mut undefined_binding = candidate();
    undefined_binding.definitions[0].body.result = Expression::Binding {
        name: "missing".to_owned(),
    };
    assert!(matches!(
        undefined_binding.validate(),
        Err(CoreError::Validation(message)) if message.contains("undefined binding")
    ));

    let mut invalid_schema = candidate();
    invalid_schema.definitions[0].input_schema = json!(42);
    assert!(matches!(
        invalid_schema.validate(),
        Err(CoreError::Validation(message)) if message.contains("schema must")
    ));
}

#[test]
fn effect_admission_requires_an_exact_entry_reachable_site_tuple() {
    let candidate = effect_site_candidate();
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate);
    let run_id = "run:effect-site";
    let execution_binding = test_execution_binding(&mut machine, "effect-site");
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let root_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let args = machine
        .put_artifact("cymule.effect-args/1", b"{}".to_vec())
        .expect("arguments store");
    let propose = |scope_id: &str, invocation_id: &str, site_id: &str, occurrence: &str| {
        Command::ProposeEffect {
            scope_id: scope_id.to_owned(),
            invocation_id: invocation_id.to_owned(),
            invocation_path: Vec::new(),
            definition_id: "main".to_owned(),
            region_path: Vec::new(),
            site_id: site_id.to_owned(),
            occurrence: occurrence.to_owned(),
            operation: "test.capture".to_owned(),
            args: args.clone(),
            execution_binding: execution_binding.clone(),
            occurrence_binding: test_content_id("binding:effect/v1"),
        }
    };
    assert_dynamic_effect_scope_authority(
        &mut machine,
        run_id,
        &root_invocation,
        &args,
        &execution_binding,
        &propose,
    );
    assert_invalid_root_effect_sites(&mut machine, run_id, &root_invocation, &propose);
    assert_admitted_effect_profile_authority(&mut machine, run_id, &root_invocation, &propose);
}

fn effect_site_candidate() -> PlanCandidate {
    let mut candidate = candidate();
    candidate.definitions[0].body.steps.push(Step {
        id: "effect.reachable".to_owned(),
        operation: Operation::Effect {
            effect: "test.capture".to_owned(),
            input: Expression::Input,
            occurrence: "reachable".to_owned(),
            bind: None,
        },
    });
    candidate.definitions[0].body.steps.push(Step {
        id: "invoke.unreachable".to_owned(),
        operation: Operation::Invoke {
            definition: "unreachable".to_owned(),
            input: Expression::Input,
            bind: None,
        },
    });
    candidate.definitions[0].body.steps.push(Step {
        id: "scope.other".to_owned(),
        operation: Operation::Scope {
            body: Box::new(Region {
                steps: Vec::new(),
                result: Expression::Input,
            }),
            bind: None,
        },
    });
    candidate.definitions.push(Definition {
        id: "unreachable".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: vec![Step {
                id: "effect.unreachable".to_owned(),
                operation: Operation::Effect {
                    effect: "test.capture".to_owned(),
                    input: Expression::Input,
                    occurrence: "unreachable".to_owned(),
                    bind: None,
                },
            }],
            result: Expression::Input,
        },
    });
    candidate
}

fn assert_dynamic_effect_scope_authority(
    machine: &mut Machine,
    run_id: &str,
    root_invocation: &str,
    args: &ArtifactRef,
    execution_binding: &ArtifactRef,
    propose: &impl Fn(&str, &str, &str, &str) -> Command,
) {
    assert!(matches!(
        machine.submit(envelope(
            machine,
            2,
            run_id,
            propose(
                cymule_core::ROOT_SCOPE_ID,
                root_invocation,
                "effect.unreachable",
                "unreachable",
            ),
        )),
        Err(CoreError::NotFound(_))
    ));
    let invocation_path = vec![cymule_core::InvocationPathSegment {
        site_id: "invoke.unreachable".to_owned(),
        region_path: Vec::new(),
        scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
    }];
    let run = &machine.projection().runs[run_id];
    let invoked_id =
        cymule_core::plan_invocation_id(run_id, &run.current_plan, "main", &invocation_path)
            .expect("invocation hashes");
    machine
        .submit(envelope(
            machine,
            51,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: invoked_id,
                invocation_path,
                definition_id: "unreachable".to_owned(),
                region_path: Vec::new(),
                site_id: "effect.unreachable".to_owned(),
                occurrence: "unreachable".to_owned(),
                operation: "test.capture".to_owned(),
                args: args.clone(),
                execution_binding: execution_binding.clone(),
                occurrence_binding: test_content_id("binding:effect/v1"),
            },
        ))
        .expect("invoked site admits only with its exact dynamic invocation path");
    let run = &machine.projection().runs[run_id];
    let unrelated_scope =
        cymule_core::plan_scope_id(run_id, &run.current_plan, root_invocation, "main", &[3])
            .expect("scope identity derives");
    machine
        .submit(envelope(
            machine,
            52,
            run_id,
            Command::OpenScope {
                scope_id: unrelated_scope.clone(),
                parent_scope: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: root_invocation.to_owned(),
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "scope.other".to_owned(),
            },
        ))
        .expect("unrelated lexical scope opens");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            53,
            run_id,
            propose(
                &unrelated_scope,
                root_invocation,
                "effect.reachable",
                "reachable",
            ),
        )),
        Err(CoreError::Validation(message)) if message.contains("lexical scope")
    ));
}

fn assert_invalid_root_effect_sites(
    machine: &mut Machine,
    run_id: &str,
    root_invocation: &str,
    propose: &impl Fn(&str, &str, &str, &str) -> Command,
) {
    assert!(matches!(
        machine.submit(envelope(
            machine,
            3,
            run_id,
            propose(
                cymule_core::ROOT_SCOPE_ID,
                "main",
                "effect.reachable",
                "wrong",
            ),
        )),
        Err(CoreError::Validation(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            4,
            run_id,
            propose(
                cymule_core::ROOT_SCOPE_ID,
                "",
                "effect.reachable",
                "reachable",
            ),
        )),
        Err(CoreError::Validation(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            5,
            run_id,
            propose(
                "scope:missing",
                root_invocation,
                "effect.reachable",
                "reachable",
            ),
        )),
        Err(CoreError::NotFound(_))
    ));
}

fn assert_admitted_effect_profile_authority(
    machine: &mut Machine,
    run_id: &str,
    root_invocation: &str,
    propose: &impl Fn(&str, &str, &str, &str) -> Command,
) {
    machine
        .submit(envelope(
            machine,
            6,
            run_id,
            propose(
                cymule_core::ROOT_SCOPE_ID,
                root_invocation,
                "effect.reachable",
                "reachable",
            ),
        ))
        .expect("exact reachable effect tuple admits");
    let proposed = machine.events().last().expect("admitted proposal exists");
    let mut payload = proposed.payload.clone();
    let EventPayload::EffectProposed { profile, .. } = &mut payload else {
        panic!("admitted Event is an Effect proposal");
    };
    profile.irreversible = true;
    let wrong_profile = event_with_payload(proposed, payload);
    let wrong_profile_history = machine.events().map(|event| {
        if event.event_id == proposed.event_id {
            wrong_profile.clone()
        } else {
            event.clone()
        }
    });
    assert!(matches!(
        replay_with_machine_authority(machine, wrong_profile_history),
        Err(CoreError::Validation(_) | CoreError::IdentityMismatch(_))
    ));

    let effect = machine.projection().runs[run_id]
        .effects
        .values()
        .find(|effect| effect.site_id == "effect.reachable")
        .expect("effect exists");
    assert_eq!(effect.invocation_id, root_invocation);
    assert_eq!(effect.site_id, "effect.reachable");
    assert_eq!(effect.occurrence, "reachable");
    assert_eq!(effect.profile.dispatch, DispatchPolicy::OnScopeCommit);
    assert_eq!(effect.profile.reconciliation, ReconciliationMode::Queryable);
    machine
        .verify_replay()
        .expect("profile rules replay exactly");
}

#[test]
fn event_only_replay_rejects_forged_effect_plan_binding_and_schema_authority() {
    let mut effect_candidate = candidate();
    effect_candidate.definitions[0].body = Region {
        steps: vec![Step {
            id: "effect.replay".to_owned(),
            operation: Operation::Effect {
                effect: "test.capture".to_owned(),
                input: Expression::Input,
                occurrence: "once".to_owned(),
                bind: None,
            },
        }],
        result: Expression::Literal { value: json!(null) },
    };
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, effect_candidate);
    let run_id = "run:effect-replay-authority";
    let execution_binding = test_execution_binding(&mut machine, "effect-replay-authority");
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let run = &machine.projection().runs[run_id];
    let invocation_id = run.scopes[cymule_core::ROOT_SCOPE_ID].invocation_id.clone();
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("effect arguments store");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id,
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "effect.replay".to_owned(),
                occurrence: "once".to_owned(),
                operation: "test.capture".to_owned(),
                args,
                execution_binding,
                occurrence_binding: test_content_id("binding:effect/replay@1"),
            },
        ))
        .expect("effect proposal admits");
    let events = machine.events().cloned().collect::<Vec<_>>();
    let [started, attempted, proposed] = events.as_slice() else {
        panic!("fixture contains the StartRun pair and one effect proposal Event");
    };
    assert!(matches!(
        attempted.payload,
        EventPayload::AttemptStarted { .. }
    ));
    assert_eq!(started.command_id, attempted.command_id);
    let authority = machine.snapshot();
    let entries = machine.replay_entries().expect("replay entries export");
    let started_entry = entries.first().expect("start admission exists").clone();
    assert!(matches!(
        Machine::replay(
            Vec::<SealedPlan>::new(),
            authority.artifacts.clone(),
            [authority.batches[0].clone()],
            [started_entry.clone()],
        ),
        Err(CoreError::NotFound(_))
    ));
    assert!(matches!(
        Machine::replay(
            authority.plans.clone(),
            Vec::<cymule_core::ArtifactRecord>::new(),
            [authority.batches[0].clone()],
            [started_entry],
        ),
        Err(CoreError::NotFound(_))
    ));
    assert_forged_effect_event_authority(&machine, started, proposed);
}

fn assert_forged_effect_event_authority(machine: &Machine, started: &Event, proposed: &Event) {
    let rebuild = |payload| {
        Event::new(EventContent {
            command_id: proposed.command_id.clone(),
            command_hash: proposed.command_hash.clone(),
            run_id: proposed.run_id.clone(),
            parents: proposed.parents.clone(),
            reads: proposed.reads.clone(),
            writes: proposed.writes.clone(),
            coordination_key: proposed.coordination_key.clone(),
            payload,
        })
        .expect("tampered Event remains content-addressable")
    };

    let mut wrong_plan = proposed.payload.clone();
    let EventPayload::EffectProposed { origin_plan_id, .. } = &mut wrong_plan else {
        panic!("fixture payload should be an effect proposal");
    };
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        .clone_into(origin_plan_id);
    assert!(matches!(
        replay_with_machine_authority(machine, [started.clone(), rebuild(wrong_plan)]),
        Err(CoreError::NotFound(_) | CoreError::Validation(_) | CoreError::IdentityMismatch(_))
    ));

    let mut wrong_binding = proposed.payload.clone();
    let EventPayload::EffectProposed {
        execution_binding, ..
    } = &mut wrong_binding
    else {
        panic!("fixture payload should be an effect proposal");
    };
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        .clone_into(&mut execution_binding.artifact_id);
    assert!(matches!(
        replay_with_machine_authority(machine, [started.clone(), rebuild(wrong_binding)]),
        Err(CoreError::NotFound(_) | CoreError::Validation(_) | CoreError::IdentityMismatch(_))
    ));

    let mut wrong_schema = proposed.payload.clone();
    let EventPayload::EffectProposed {
        effect_schema_version,
        ..
    } = &mut wrong_schema
    else {
        panic!("fixture payload should be an effect proposal");
    };
    "cymule.effect-schema/invalid".clone_into(effect_schema_version);
    assert!(matches!(
        replay_with_machine_authority(machine, [started.clone(), rebuild(wrong_schema)]),
        Err(CoreError::Validation(_) | CoreError::IdentityMismatch(_))
    ));

    let mut wrong_profile = proposed.payload.clone();
    let EventPayload::EffectProposed { profile, .. } = &mut wrong_profile else {
        panic!("fixture payload should be an effect proposal");
    };
    profile.keyed_idempotency = !profile.keyed_idempotency;
    assert!(matches!(
        replay_with_machine_authority(machine, [started.clone(), rebuild(wrong_profile)]),
        Err(CoreError::Validation(_) | CoreError::IdentityMismatch(_))
    ));
}

#[test]
fn run_and_migration_bindings_require_retained_execution_binding_artifacts() {
    let mut machine = Machine::new();
    let source = insert_plan(&mut machine, candidate());
    let mut target_candidate = candidate();
    target_candidate
        .metadata
        .insert("revision".to_owned(), "target".to_owned());
    let target = insert_plan(&mut machine, target_candidate);
    let target_plan_id = target.plan_id.clone();
    let wrong_kind = machine
        .put_artifact("test/not-an-execution-binding/1", b"wrong".to_vec())
        .expect("wrong-kind Artifact stores");
    let input = machine
        .put_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, b"{}".to_vec())
        .expect("Run input stores");
    assert_invalid_start_bindings(&mut machine, &source, &wrong_kind, &input);
    let source_binding = test_execution_binding(&mut machine, "binding-source");
    let target_binding = test_execution_binding(&mut machine, "binding-target");
    machine
        .submit(envelope(
            &machine,
            3,
            "run:binding-authority",
            Command::StartRun {
                plan_id: source.plan_id.clone(),
                binding_context: source_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("exact source execution binding starts Run");
    let initial = initial_attempt(&machine, "run:binding-authority");
    machine
        .submit(envelope(
            &machine,
            0,
            "run:binding-authority",
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("migration binding probes start from an inactive Attempt");
    assert!(
        machine
            .submit(envelope(
                &machine,
                4,
                "run:binding-authority",
                Command::UpdateBinding {
                    binding_context: wrong_kind.artifact_id.clone(),
                },
            ))
            .is_err()
    );
    assert!(
        machine
            .submit(envelope(
                &machine,
                5,
                "run:binding-authority",
                Command::MigrateRun {
                    from_plan: source.plan_id.clone(),
                    to_plan: target.plan_id.clone(),
                    from_binding: source_binding.artifact_id.clone(),
                    to_binding: wrong_kind.artifact_id,
                    safe_point_id:
                        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                            .to_owned(),
                    target_epoch: 1,
                    target_continuation_digest: "b".repeat(64),
                },
            ))
            .is_err()
    );
    machine
        .submit(envelope(
            &machine,
            6,
            "run:binding-authority",
            Command::MigrateRun {
                from_plan: source.plan_id,
                to_plan: target_plan_id.clone(),
                from_binding: source_binding.artifact_id,
                to_binding: target_binding.artifact_id,
                safe_point_id:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_owned(),
                target_epoch: 1,
                target_continuation_digest: "c".repeat(64),
            },
        ))
        .expect("exact target execution binding migrates Run");
}

fn assert_invalid_start_bindings(
    machine: &mut Machine,
    source: &SealedPlan,
    wrong_kind: &ArtifactRef,
    input: &ArtifactRef,
) {
    for (sequence, binding_context) in [
        (
            1,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        ),
        (2, wrong_kind.artifact_id.clone()),
    ] {
        assert!(
            machine
                .submit(envelope(
                    machine,
                    sequence,
                    "run:binding-authority",
                    Command::StartRun {
                        plan_id: source.plan_id.clone(),
                        binding_context,
                        input: input.clone(),
                        material_digest: String::new(),
                        initial_attempt: placeholder_initial_attempt(),
                    },
                ))
                .is_err()
        );
        assert!(machine.projection().runs.is_empty());
    }
}

#[test]
fn migration_command_event_replay_and_restore_share_one_closed_payload_validator() {
    let mut machine = Machine::new();
    let source = insert_plan(&mut machine, candidate());
    let mut target_candidate = candidate();
    target_candidate
        .metadata
        .insert("revision".to_owned(), "migration-target".to_owned());
    let target = insert_plan(&mut machine, target_candidate);
    let source_binding = test_execution_binding(&mut machine, "migration-source");
    let target_binding = test_execution_binding(&mut machine, "migration-target");
    let run_id = "run:migration-payload-closure";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: source.plan_id.clone(),
                binding_context: source_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before migration payload validation");
    let before_migration = machine.clone();
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::MigrateRun {
                from_plan: source.plan_id.clone(),
                to_plan: target.plan_id.clone(),
                from_binding: source_binding.artifact_id.clone(),
                to_binding: target_binding.artifact_id.clone(),
                safe_point_id: format!("sha256:{}", "c".repeat(64)),
                target_epoch: 1,
                target_continuation_digest: "c".repeat(64),
            },
        ))
        .expect("content-addressed migration admits");
    let events = machine.events().cloned().collect::<Vec<_>>();
    let [started, attempted, yielded, migrated] = events.as_slice() else {
        panic!("fixture contains the StartRun pair, explicit yield, and migration");
    };
    assert!(matches!(started.payload, EventPayload::RunStarted { .. }));
    assert!(matches!(
        attempted.payload,
        EventPayload::AttemptStarted { .. }
    ));
    assert_eq!(started.command_id, attempted.command_id);
    assert!(matches!(
        yielded.payload,
        EventPayload::AttemptYielded { .. }
    ));
    let replayed = replay_with_machine_authority(&machine, events.clone())
        .expect("legal migration Event replays");
    assert_eq!(replayed.runs[run_id].current_plan, target.plan_id);
    Machine::restore(machine.snapshot()).expect("legal migration snapshot restores");
    for payload in invalid_migration_payloads(migrated, &source, &source_binding) {
        assert_forged_migration_payload_rejected(
            &machine,
            &before_migration,
            migrated,
            run_id,
            payload,
        );
    }
}

fn invalid_migration_payloads(
    migrated: &Event,
    source: &SealedPlan,
    source_binding: &ArtifactRef,
) -> Vec<EventPayload> {
    let mut same_plan = migrated.payload.clone();
    let EventPayload::RunMigrated {
        to_plan,
        to_binding,
        ..
    } = &mut same_plan
    else {
        panic!("fixture is a migration Event");
    };
    to_plan.clone_from(&source.plan_id);
    to_binding.clone_from(&source_binding.artifact_id);

    let mut empty_binding = migrated.payload.clone();
    let EventPayload::RunMigrated { to_binding, .. } = &mut empty_binding else {
        panic!("fixture is a migration Event");
    };
    to_binding.clear();

    let mut empty_safe_point = migrated.payload.clone();
    let EventPayload::RunMigrated { safe_point_id, .. } = &mut empty_safe_point else {
        panic!("fixture is a migration Event");
    };
    safe_point_id.clear();

    let mut non_content_safe_point = migrated.payload.clone();
    let EventPayload::RunMigrated { safe_point_id, .. } = &mut non_content_safe_point else {
        panic!("fixture is a migration Event");
    };
    "safe-point:not-content-addressed".clone_into(safe_point_id);

    let mut zero_target_epoch = migrated.payload.clone();
    let EventPayload::RunMigrated { target_epoch, .. } = &mut zero_target_epoch else {
        panic!("fixture is a migration Event");
    };
    *target_epoch = 0;

    let mut non_next_target_epoch = migrated.payload.clone();
    let EventPayload::RunMigrated { target_epoch, .. } = &mut non_next_target_epoch else {
        panic!("fixture is a migration Event");
    };
    *target_epoch = 2;

    let mut malformed_target_digest = migrated.payload.clone();
    let EventPayload::RunMigrated {
        target_continuation_digest,
        ..
    } = &mut malformed_target_digest
    else {
        panic!("fixture is a migration Event");
    };
    "NOT-A-CANONICAL-DIGEST".clone_into(target_continuation_digest);
    vec![
        same_plan,
        empty_binding,
        empty_safe_point,
        non_content_safe_point,
        zero_target_epoch,
        non_next_target_epoch,
        malformed_target_digest,
    ]
}

#[test]
fn execution_frames_accept_only_current_or_explicit_migration_lineage_plans() {
    let mut machine = Machine::new();
    let source = insert_plan(&mut machine, candidate());
    let mut target_candidate = candidate();
    target_candidate
        .metadata
        .insert("revision".to_owned(), "target".to_owned());
    let target = insert_plan(&mut machine, target_candidate);
    let target_plan_id = target.plan_id.clone();
    let mut unrelated_candidate = candidate();
    unrelated_candidate
        .metadata
        .insert("revision".to_owned(), "unrelated".to_owned());
    let unrelated = insert_plan(&mut machine, unrelated_candidate);
    let source_binding = test_execution_binding(&mut machine, "frame-source");
    let target_binding = test_execution_binding(&mut machine, "frame-target");
    let run_id = "run:frame-plan-lineage";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: source.plan_id.clone(),
                binding_context: source_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let source_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let source_frame = cymule_core::ExecutionFrameLocation {
        run_id,
        plan_id: &source.plan_id,
        invocation_id: &source_invocation,
        invocation_path: &[],
        definition_id: "main",
        region_path: &[],
        scope_id: cymule_core::ROOT_SCOPE_ID,
        next_step: 0,
    };
    machine
        .validate_historical_execution_location(&source_frame)
        .expect("current source Plan owns the frame");
    machine
        .validate_resumable_execution_frame(&cymule_core::ResumableExecutionFrame {
            location: source_frame,
            binding_context: &source_binding.artifact_id,
            epoch: 0,
        })
        .expect("current open source frame is resumable");
    assert_unrelated_frame_lineage(&machine, source_frame, &unrelated);
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("source execution yields before the migration safe point");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::MigrateRun {
                from_plan: source.plan_id.clone(),
                to_plan: target_plan_id.clone(),
                from_binding: source_binding.artifact_id.clone(),
                to_binding: target_binding.artifact_id.clone(),
                safe_point_id:
                    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                        .to_owned(),
                target_epoch: 1,
                target_continuation_digest: "d".repeat(64),
            },
        ))
        .expect("Run migrates");
    assert_eq!(machine.projection().runs[run_id].epoch, 1);
    assert_migrated_frame_authority(
        &machine,
        source_frame,
        &source_binding,
        &target_plan_id,
        &target_binding,
    );
}

fn assert_unrelated_frame_lineage(
    machine: &Machine,
    source_frame: cymule_core::ExecutionFrameLocation<'_>,
    unrelated: &SealedPlan,
) {
    let run_id = source_frame.run_id;
    let unrelated_invocation =
        cymule_core::plan_invocation_id(run_id, &unrelated.plan_id, "main", &[])
            .expect("unrelated invocation derives");
    assert!(matches!(
        machine.validate_historical_execution_location(&cymule_core::ExecutionFrameLocation {
            plan_id: &unrelated.plan_id,
            invocation_id: &unrelated_invocation,
            ..source_frame
        }),
        Err(CoreError::Validation(message)) if message.contains("migration lineage")
    ));
}

fn assert_migrated_frame_authority(
    machine: &Machine,
    source_frame: cymule_core::ExecutionFrameLocation<'_>,
    source_binding: &ArtifactRef,
    target_plan_id: &str,
    target_binding: &ArtifactRef,
) {
    let run_id = source_frame.run_id;
    machine
        .validate_historical_execution_location(&source_frame)
        .expect("source Plan remains explicit historical lineage authority");
    assert!(
        machine
            .validate_resumable_execution_frame(&cymule_core::ResumableExecutionFrame {
                location: source_frame,
                binding_context: &source_binding.artifact_id,
                epoch: 0,
            })
            .is_err()
    );
    let target_invocation = cymule_core::plan_invocation_id(run_id, target_plan_id, "main", &[])
        .expect("target invocation derives");
    let target_frame = cymule_core::ExecutionFrameLocation {
        plan_id: target_plan_id,
        invocation_id: &target_invocation,
        ..source_frame
    };
    machine
        .validate_historical_execution_location(&target_frame)
        .expect("current target Plan owns its migrated frame");
    let current_target = cymule_core::ResumableExecutionFrame {
        location: target_frame,
        binding_context: &target_binding.artifact_id,
        epoch: 1,
    };
    machine
        .validate_resumable_execution_frame(&current_target)
        .expect("current target frame is ordinarily resumable");
    let replacement = machine
        .migration_frame_replacement_receipt("command:2")
        .expect("replacement receipt derives")
        .expect("migration receipt exists");
    assert_eq!(replacement.target_epoch, 1);
    assert_eq!(replacement.target_continuation_digest, "d".repeat(64));
    machine
        .validate_migration_replacement_frame(&current_target, &replacement, &"d".repeat(64))
        .expect("typed receipt admits the exact replacement frame");
    assert!(
        machine
            .validate_migration_replacement_frame(&current_target, &replacement, &"e".repeat(64),)
            .is_err()
    );
    assert!(
        machine
            .validate_migration_replacement_frame(
                &cymule_core::ResumableExecutionFrame {
                    epoch: 0,
                    ..current_target
                },
                &replacement,
                &"d".repeat(64),
            )
            .is_err()
    );
    let mut forged = replacement;
    forged.safe_point_id = format!("sha256:{}", "a".repeat(64));
    assert!(
        machine
            .validate_migration_replacement_frame(&current_target, &forged, &"d".repeat(64))
            .is_err()
    );
}

#[test]
fn frame_admission_separates_resume_effect_completion_and_historical_authority() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, root_effect_candidate());
    let binding = test_execution_binding(&mut machine, "frame-boundaries");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("Effect arguments store");
    let run_id = "run:frame-boundaries";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let open_location = cymule_core::ExecutionFrameLocation {
        run_id,
        plan_id: &plan.plan_id,
        invocation_id: &invocation,
        invocation_path: &[],
        definition_id: "main",
        region_path: &[],
        scope_id: cymule_core::ROOT_SCOPE_ID,
        next_step: 0,
    };
    machine
        .validate_resumable_execution_frame(&cymule_core::ResumableExecutionFrame {
            location: open_location,
            binding_context: &binding.artifact_id,
            epoch: 0,
        })
        .expect("open current frame is resumable");
    let intent_id = propose_root_effect(&mut machine, 2, run_id, args, binding.clone());
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        ))
        .expect("Effect prepares");
    machine
        .submit(envelope(
            &machine,
            4,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("root scope commits");
    let terminal_pc = cymule_core::ExecutionFrameLocation {
        next_step: 1,
        ..open_location
    };
    let closed_frame = cymule_core::ResumableExecutionFrame {
        location: terminal_pc,
        binding_context: &binding.artifact_id,
        epoch: 0,
    };
    let boundary = cymule_core::ClosedExecutionBoundary {
        frame: closed_frame,
        frame_count: 1,
        scope_stack: &[cymule_core::ROOT_SCOPE_ID.to_owned()],
        wait_count: 0,
        disposition: cymule_core::ClosedBoundaryDisposition::Running,
        has_execution_claim: true,
    };
    assert_closed_effect_boundary(&machine, &closed_frame, &boundary, &intent_id);
    assert_completed_frame_rejection(
        &mut machine,
        run_id,
        intent_id,
        &boundary,
        terminal_pc,
        closed_frame,
    );
}

fn assert_closed_effect_boundary(
    machine: &Machine,
    closed_frame: &cymule_core::ResumableExecutionFrame<'_>,
    boundary: &cymule_core::ClosedExecutionBoundary<'_>,
    intent_id: &str,
) {
    assert!(
        machine
            .validate_resumable_execution_frame(closed_frame)
            .is_err()
    );
    let effect_boundary = machine
        .validate_effect_boundary_frame(boundary)
        .expect("closed scope retains one exact pending Effect boundary");
    assert_eq!(
        effect_boundary.intent_ids,
        BTreeSet::from([intent_id.to_owned()])
    );
    assert!(
        machine
            .validate_completion_boundary_frame(boundary)
            .is_err()
    );
}

fn assert_completed_frame_rejection(
    machine: &mut Machine,
    run_id: &str,
    intent_id: String,
    boundary: &cymule_core::ClosedExecutionBoundary<'_>,
    terminal_pc: cymule_core::ExecutionFrameLocation<'_>,
    closed_frame: cymule_core::ResumableExecutionFrame<'_>,
) {
    machine
        .submit(envelope(
            machine,
            5,
            run_id,
            Command::TransitionEffect {
                intent_id,
                transition: EffectTransition::MarkUnavailable,
            },
        ))
        .expect("unavailable pre-dispatch Effect settles NotApplied");
    machine
        .validate_completion_boundary_frame(boundary)
        .expect("settled terminal PC is a completion boundary");
    let initial = initial_attempt(machine, run_id);
    machine
        .submit(envelope(
            machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("completion yields the live Attempt before terminal admission");
    machine
        .submit(envelope(
            machine,
            6,
            run_id,
            Command::CompleteRun { result: None },
        ))
        .expect("Run completes");
    machine
        .validate_historical_execution_location(&terminal_pc)
        .expect("terminal frame remains structurally inspectable");
    assert!(
        machine
            .validate_resumable_execution_frame(&closed_frame)
            .is_err()
    );
    assert!(
        machine
            .validate_completion_boundary_frame(boundary)
            .is_err()
    );
}

#[test]
fn post_effect_ready_boundary_accepts_only_settled_claim_free_terminal_frames() {
    for outcome in [
        Some(WorldOutcome::Applied),
        Some(WorldOutcome::NotApplied),
        None,
    ] {
        assert_post_effect_ready_outcome(outcome);
    }
}

fn post_effect_ready_fixture(
    outcome: Option<WorldOutcome>,
) -> (Machine, SealedPlan, ArtifactRef, String, String) {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, root_effect_candidate());
    let binding = test_execution_binding(&mut machine, "post-effect-ready");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("Effect arguments store");
    let suffix = outcome.map_or("cancelled", |value| match value {
        WorldOutcome::Applied => "applied",
        WorldOutcome::NotApplied => "not-applied",
        WorldOutcome::Unobserved | WorldOutcome::Unknown => {
            unreachable!("test enumerates terminal outcomes")
        }
    });
    let run_id = format!("run:post-effect-ready:{suffix}");
    machine
        .submit(envelope(
            &machine,
            1,
            &run_id,
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let intent_id = propose_root_effect(&mut machine, 2, &run_id, args, binding.clone());
    machine
        .submit(envelope(
            &machine,
            3,
            &run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        ))
        .expect("Effect prepares");
    (machine, plan, binding, run_id, intent_id)
}

fn assert_post_effect_ready_outcome(outcome: Option<WorldOutcome>) {
    let (mut machine, plan, binding, run_id, intent_id) = post_effect_ready_fixture(outcome);
    let invocation = machine.projection().runs[&run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let terminal_location = cymule_core::ExecutionFrameLocation {
        run_id: &run_id,
        plan_id: &plan.plan_id,
        invocation_id: &invocation,
        invocation_path: &[],
        definition_id: "main",
        region_path: &[],
        scope_id: cymule_core::ROOT_SCOPE_ID,
        next_step: 1,
    };
    let frame = cymule_core::ResumableExecutionFrame {
        location: terminal_location,
        binding_context: &binding.artifact_id,
        epoch: 0,
    };
    let root_stack = [cymule_core::ROOT_SCOPE_ID.to_owned()];
    let ready = cymule_core::ClosedExecutionBoundary {
        frame,
        frame_count: 1,
        scope_stack: &root_stack,
        wait_count: 0,
        disposition: cymule_core::ClosedBoundaryDisposition::Ready,
        has_execution_claim: false,
    };
    assert!(machine.validate_post_effect_ready_frame(&ready).is_err());
    machine
        .submit(envelope(
            &machine,
            4,
            &run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("root scope commits");
    assert!(machine.validate_post_effect_ready_frame(&ready).is_err());
    if let Some(outcome) = outcome {
        for (sequence, transition) in [
            (5, EffectTransition::AuthorizeRelease),
            (6, EffectTransition::StartDispatch),
            (7, EffectTransition::Observe(outcome)),
        ] {
            machine
                .submit(envelope(
                    &machine,
                    sequence,
                    &run_id,
                    Command::TransitionEffect {
                        intent_id: intent_id.clone(),
                        transition,
                    },
                ))
                .expect("Effect reaches terminal observation");
        }
    } else {
        machine
            .submit(envelope(
                &machine,
                5,
                &run_id,
                Command::TransitionEffect {
                    intent_id,
                    transition: EffectTransition::MarkUnavailable,
                },
            ))
            .expect("Effect cancels before release");
    }
    machine
        .validate_post_effect_ready_frame(&ready)
        .expect("settled Effect admits claim-free Ready continuation");
    assert!(machine.validate_completion_boundary_frame(&ready).is_err());
    let wrong_pc = cymule_core::ClosedExecutionBoundary {
        frame: cymule_core::ResumableExecutionFrame {
            location: cymule_core::ExecutionFrameLocation {
                next_step: 0,
                ..terminal_location
            },
            ..frame
        },
        ..ready
    };
    assert!(machine.validate_post_effect_ready_frame(&wrong_pc).is_err());
}

#[test]
fn effect_profiles_gate_release_and_reconciliation_independently() {
    let (mut machine, run_id, intents) = effect_profiles_machine();
    for (offset, site) in ["effect.human", "effect.impossible", "effect.explicit"]
        .into_iter()
        .enumerate()
    {
        machine
            .submit(envelope(
                &machine,
                10 + offset as u64,
                run_id,
                Command::TransitionEffect {
                    intent_id: intents[site].clone(),
                    transition: EffectTransition::Prepare,
                },
            ))
            .expect("effect prepares");
    }
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            20,
            run_id,
            Command::TransitionEffect {
                intent_id: intents["effect.explicit"].clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert_observation_reconciliation_profiles(&mut machine, run_id, &intents);
    machine
        .submit(envelope(
            &machine,
            50,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("scope commits with the exact explicit obligation");
    machine
        .submit(envelope(
            &machine,
            51,
            run_id,
            Command::TransitionEffect {
                intent_id: intents["effect.explicit"].clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        ))
        .expect("explicit effect may release only after commit");
    machine.verify_replay().expect("profile transitions replay");
}

fn effect_profiles_candidate() -> PlanCandidate {
    let mut candidate = candidate();
    candidate.effects = vec![
        EffectContract {
            id: "test.human".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Observational,
                dispatch: DispatchPolicy::Eager,
                reconciliation: ReconciliationMode::Human,
                keyed_idempotency: false,
                irreversible: false,
            },
            requirements: BTreeMap::new(),
        },
        EffectContract {
            id: "test.impossible".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Observational,
                dispatch: DispatchPolicy::Eager,
                reconciliation: ReconciliationMode::Impossible,
                keyed_idempotency: false,
                irreversible: false,
            },
            requirements: BTreeMap::new(),
        },
        EffectContract {
            id: "test.explicit".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Mutating,
                dispatch: DispatchPolicy::Explicit,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
            requirements: BTreeMap::new(),
        },
    ];
    for (site, operation, occurrence) in [
        ("effect.human", "test.human", "human"),
        ("effect.impossible", "test.impossible", "impossible"),
        ("effect.explicit", "test.explicit", "explicit"),
    ] {
        candidate.definitions[0].body.steps.push(Step {
            id: site.to_owned(),
            operation: Operation::Effect {
                effect: operation.to_owned(),
                input: Expression::Input,
                occurrence: occurrence.to_owned(),
                bind: None,
            },
        });
    }
    candidate
}

fn effect_profiles_machine() -> (Machine, &'static str, BTreeMap<String, String>) {
    let candidate = effect_profiles_candidate();
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate);
    let run_id = "run:effect-profiles";
    let execution_binding = test_execution_binding(&mut machine, "effect-profiles");
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let root_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let args = machine
        .put_artifact("cymule.effect-args/1", b"{}".to_vec())
        .expect("arguments store");

    for (sequence, site, operation, occurrence) in [
        (2, "effect.human", "test.human", "human"),
        (3, "effect.impossible", "test.impossible", "impossible"),
        (4, "effect.explicit", "test.explicit", "explicit"),
    ] {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::ProposeEffect {
                    scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                    invocation_id: root_invocation.clone(),
                    invocation_path: Vec::new(),
                    definition_id: "main".to_owned(),
                    region_path: Vec::new(),
                    site_id: site.to_owned(),
                    occurrence: occurrence.to_owned(),
                    operation: operation.to_owned(),
                    args: args.clone(),
                    execution_binding: execution_binding.clone(),
                    occurrence_binding: test_content_id(&format!("binding:{occurrence}/v1")),
                },
            ))
            .expect("effect admits");
    }
    let intents: BTreeMap<String, String> = machine.projection().runs[run_id]
        .effects
        .values()
        .map(|effect| (effect.site_id.clone(), effect.intent_id.clone()))
        .collect();
    (machine, run_id, intents)
}

fn assert_observation_reconciliation_profiles(
    machine: &mut Machine,
    run_id: &str,
    intents: &BTreeMap<String, String>,
) {
    for (offset, site) in ["effect.human", "effect.impossible"]
        .into_iter()
        .enumerate()
    {
        for transition in [
            EffectTransition::AuthorizeRelease,
            EffectTransition::StartDispatch,
            EffectTransition::Observe(WorldOutcome::Unknown),
        ] {
            machine
                .submit(envelope(
                    machine,
                    30 + (offset as u64 * 3) + transition_index(&transition),
                    run_id,
                    Command::TransitionEffect {
                        intent_id: intents[site].clone(),
                        transition,
                    },
                ))
                .expect("eager observation advances while the scope is open");
        }
    }
    let run = &machine.projection().runs[run_id];
    assert_eq!(
        run.effects[&intents["effect.human"]].reconciliation,
        ReconciliationState::GovernanceRequired
    );
    assert_eq!(
        run.effects[&intents["effect.impossible"]].reconciliation,
        ReconciliationState::GovernanceRequired
    );
    assert!(matches!(
        machine.submit(envelope(
            machine,
            40,
            run_id,
            Command::TransitionEffect {
                intent_id: intents["effect.human"].clone(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::StillUnknown),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            machine,
            41,
            run_id,
            Command::TransitionEffect {
                intent_id: intents["effect.human"].clone(),
                transition: EffectTransition::Reconcile(
                    ReconciliationResolution::ResolvedNotApplied,
                ),
            },
        ))
        .expect("human authority may settle an ambiguous effect");
    machine
        .submit(envelope(
            machine,
            42,
            run_id,
            Command::TransitionEffect {
                intent_id: intents["effect.impossible"].clone(),
                transition: EffectTransition::Reconcile(
                    ReconciliationResolution::ResolvedNotApplied,
                ),
            },
        ))
        .expect("governance may close an impossible reconciliation obligation");
}

const fn transition_index(transition: &EffectTransition) -> u64 {
    match transition {
        EffectTransition::AuthorizeRelease => 0,
        EffectTransition::StartDispatch => 1,
        EffectTransition::Observe(_) => 2,
        EffectTransition::Prepare | EffectTransition::Reconcile(_) => 3,
        EffectTransition::MarkUnavailable => 4,
    }
}

#[test]
fn aborting_an_unreleased_unavailable_effect_closes_reconciliation() {
    let run_id = "run:unavailable-abort";
    let (mut machine, intent_id) = unavailable_abort_machine();
    for (sequence, transition) in [
        (3, EffectTransition::Prepare),
        (4, EffectTransition::MarkUnavailable),
    ] {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::TransitionEffect {
                    intent_id: intent_id.clone(),
                    transition,
                },
            ))
            .expect("Effect transition admits");
    }
    let effect = &machine.projection().runs[run_id].effects[&intent_id];
    assert_eq!(effect.phase, EffectPhase::CancelledBeforeRelease);
    assert_eq!(effect.outcome, WorldOutcome::NotApplied);
    assert_eq!(effect.reconciliation, ReconciliationState::Resolved);
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            40,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied,),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            &machine,
            5,
            run_id,
            Command::AbortScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("root scope aborts before dispatch");

    let run = &machine.projection().runs[run_id];
    assert_eq!(
        run.effects[&intent_id].phase,
        EffectPhase::CancelledBeforeRelease
    );
    assert_eq!(
        run.effects[&intent_id].reconciliation,
        ReconciliationState::Resolved
    );
    assert_eq!(
        run.world_settlement,
        cymule_core::WorldSettlementStatus::Settled
    );
    machine.verify_replay().expect("abort transition replays");
}

fn unavailable_abort_machine() -> (Machine, String) {
    let mut candidate = candidate();
    candidate.definitions[0].body.steps = vec![Step {
        id: "effect.capture".to_owned(),
        operation: Operation::Effect {
            effect: "test.capture".to_owned(),
            input: Expression::Input,
            occurrence: "capture".to_owned(),
            bind: None,
        },
    }];
    candidate.definitions[0].body.result = Expression::Literal { value: json!(null) };

    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate);
    let execution_binding = test_execution_binding(&mut machine, "unavailable-abort");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("arguments store");
    let run_id = "run:unavailable-abort";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let root_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: root_invocation,
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "effect.capture".to_owned(),
                occurrence: "capture".to_owned(),
                operation: "test.capture".to_owned(),
                args,
                execution_binding,
                occurrence_binding: test_content_id("binding:unavailable-abort/1"),
            },
        ))
        .expect("Effect admits");
    let intent_id = machine.projection().runs[run_id]
        .effects
        .keys()
        .next()
        .expect("Effect exists")
        .clone();
    (machine, intent_id)
}

#[test]
fn aborting_an_unreleased_effect_records_not_applied() {
    let mut candidate = candidate();
    candidate.definitions[0].body.steps = vec![Step {
        id: "effect.capture".to_owned(),
        operation: Operation::Effect {
            effect: "test.capture".to_owned(),
            input: Expression::Input,
            occurrence: "capture".to_owned(),
            bind: None,
        },
    }];
    candidate.definitions[0].body.result = Expression::Literal { value: json!(null) };

    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate);
    let execution_binding = test_execution_binding(&mut machine, "abort-before-release");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("arguments store");
    let run_id = "run:abort-before-release";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: execution_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let root_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: root_invocation,
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "effect.capture".to_owned(),
                occurrence: "capture".to_owned(),
                operation: "test.capture".to_owned(),
                args,
                execution_binding,
                occurrence_binding: test_content_id("binding:abort-before-release/1"),
            },
        ))
        .expect("Effect admits");
    let intent_id = machine.projection().runs[run_id]
        .effects
        .keys()
        .next()
        .expect("Effect exists")
        .clone();
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::AbortScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("root scope aborts before release");

    let effect = &machine.projection().runs[run_id].effects[&intent_id];
    assert_eq!(effect.phase, EffectPhase::CancelledBeforeRelease);
    assert_eq!(effect.outcome, WorldOutcome::NotApplied);
    assert_eq!(effect.reconciliation, ReconciliationState::Resolved);
    machine.verify_replay().expect("abort transition replays");
}

#[test]
fn terminal_execution_fences_dispatch_and_accepts_only_reconciliation() {
    let (mut machine, run_id, intent_ids, cancellation) = terminal_dispatch_machine();
    dispatch_terminal_fixture(&mut machine, run_id, &intent_ids);
    assert_terminal_dispatch_fences(&mut machine, run_id, &intent_ids, cancellation);
    machine
        .submit(envelope(
            &machine,
            20,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_ids[0].clone(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied),
            },
        ))
        .expect("late reconciliation settles the first dispatched Effect");
    machine
        .submit(envelope(
            &machine,
            21,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_ids[1].clone(),
                transition: EffectTransition::Reconcile(
                    ReconciliationResolution::ResolvedNotApplied,
                ),
            },
        ))
        .expect("late reconciliation settles the other dispatched Effect");
    machine
        .submit(envelope(
            &machine,
            22,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_ids[2].clone(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied),
            },
        ))
        .expect("Applied reconciliation requires retained dispatch evidence");
    let run = &machine.projection().runs[run_id];
    assert_eq!(
        run.world_settlement,
        cymule_core::WorldSettlementStatus::Settled
    );
    assert!(
        run.obligations
            .values()
            .all(|obligation| obligation.resolved)
    );
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            23,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_ids[0].clone(),
                transition: EffectTransition::MarkUnavailable,
            },
        )),
        Err(CoreError::IllegalTransition(message)) if message.contains("terminal")
    ));
    machine
        .verify_replay()
        .expect("terminal settlement replays exactly");
}

fn terminal_effect_candidate() -> PlanCandidate {
    let mut effect_candidate = candidate();
    effect_candidate.definitions[0].body = Region {
        steps: [
            ("effect.first", "first"),
            ("effect.second", "second"),
            ("effect.third", "third"),
        ]
        .into_iter()
        .map(|(site, occurrence)| Step {
            id: site.to_owned(),
            operation: Operation::Effect {
                effect: "test.capture".to_owned(),
                input: Expression::Input,
                occurrence: occurrence.to_owned(),
                bind: None,
            },
        })
        .collect(),
        result: Expression::Literal { value: json!(null) },
    };
    effect_candidate
}

fn terminal_dispatch_machine() -> (Machine, &'static str, Vec<String>, ArtifactRef) {
    let effect_candidate = terminal_effect_candidate();
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, effect_candidate);
    let binding = test_execution_binding(&mut machine, "terminal-settlement");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("effect arguments store");
    let cancellation = machine
        .put_artifact(
            "cymule.cancellation-reason/1",
            b"operator_cancelled".to_vec(),
        )
        .expect("cancellation reason stores");
    let run_id = "run:terminal-settlement";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let invocation_id = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    for (sequence, site, occurrence) in [
        (2, "effect.first", "first"),
        (3, "effect.second", "second"),
        (4, "effect.third", "third"),
    ] {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::ProposeEffect {
                    scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                    invocation_id: invocation_id.clone(),
                    invocation_path: Vec::new(),
                    definition_id: "main".to_owned(),
                    region_path: Vec::new(),
                    site_id: site.to_owned(),
                    occurrence: occurrence.to_owned(),
                    operation: "test.capture".to_owned(),
                    args: args.clone(),
                    execution_binding: binding.clone(),
                    occurrence_binding: test_content_id(&format!("binding:{occurrence}/1")),
                },
            ))
            .expect("Effect admits");
    }
    let intent_ids = machine.projection().runs[run_id]
        .effects
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    (machine, run_id, intent_ids, cancellation)
}

fn dispatch_terminal_fixture(machine: &mut Machine, run_id: &str, intent_ids: &[String]) {
    for (offset, intent_id) in intent_ids.iter().enumerate() {
        machine
            .submit(envelope(
                machine,
                5 + u64::try_from(offset).expect("small offset"),
                run_id,
                Command::TransitionEffect {
                    intent_id: intent_id.clone(),
                    transition: EffectTransition::Prepare,
                },
            ))
            .expect("Effect prepares");
    }
    machine
        .submit(envelope(
            machine,
            8,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("scope commits");
    for (index, intent_id) in intent_ids.iter().enumerate() {
        for (phase, transition) in [
            (0_u64, EffectTransition::AuthorizeRelease),
            (1, EffectTransition::StartDispatch),
        ] {
            machine
                .submit(envelope(
                    machine,
                    9 + u64::try_from(index).expect("small index") * 2 + phase,
                    run_id,
                    Command::TransitionEffect {
                        intent_id: intent_id.clone(),
                        transition,
                    },
                ))
                .expect("Effect dispatch begins");
        }
    }
    machine
        .submit(envelope(
            machine,
            15,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_ids[2].clone(),
                transition: EffectTransition::MarkUnavailable,
            },
        ))
        .expect("post-dispatch implementation loss retains unknown world authority");
    let unavailable = &machine.projection().runs[run_id].effects[&intent_ids[2]];
    assert_eq!(unavailable.phase, EffectPhase::DispatchStarted);
    assert_eq!(unavailable.outcome, WorldOutcome::Unknown);
    assert_eq!(
        unavailable.reconciliation,
        ReconciliationState::GovernanceRequired
    );
}

fn assert_terminal_dispatch_fences(
    machine: &mut Machine,
    run_id: &str,
    intent_ids: &[String],
    cancellation: ArtifactRef,
) {
    machine
        .submit(envelope(
            machine,
            16,
            run_id,
            Command::CancelRun {
                reason: cancellation,
            },
        ))
        .expect("Run cancellation fences execution");
    let run = &machine.projection().runs[run_id];
    assert!(matches!(
        run.execution_status,
        cymule_core::RunExecutionStatus::Cancelled { .. }
    ));
    assert_eq!(
        run.world_settlement,
        cymule_core::WorldSettlementStatus::GovernanceRequired
    );
    assert!(run.effects.values().all(|effect| {
        effect.phase == EffectPhase::DispatchStarted && effect.outcome == WorldOutcome::Unknown
    }));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            17,
            run_id,
            Command::BeginAttempt {
                attempt_id: test_content_id("attempt:late"),
                continuation_id: test_content_id("continuation:late"),
                occurrence_binding: test_content_id("binding:late"),
                continuation_epoch: 1,
                execution_fence: 1,
            },
        )),
        Err(CoreError::IllegalTransition(message)) if message.contains("terminal")
    ));
    for (sequence, outcome) in [(18, WorldOutcome::Applied), (19, WorldOutcome::NotApplied)] {
        assert!(matches!(
            machine.submit(envelope(
                machine,
                sequence,
                run_id,
                Command::TransitionEffect {
                    intent_id: intent_ids[0].clone(),
                    transition: EffectTransition::Observe(outcome),
                },
            )),
            Err(CoreError::IllegalTransition(message)) if message.contains("terminal")
        ));
    }
}

#[test]
fn failed_run_rejects_terminal_observation_and_settles_through_reconcile() {
    let run_id = "run:failed-terminal-reconciliation";
    let (mut machine, intent_id) = observational_unknown_machine(run_id);
    let failure_detail = machine
        .put_artifact("cymule.failure-detail/1", b"declared failure".to_vec())
        .expect("failure detail stores");
    machine
        .submit(envelope(
            &machine,
            8,
            run_id,
            Command::FailRun {
                failure: RunFailure {
                    class: RunFailureClass::DeclaredFailure,
                    code: "declared_failure".to_owned(),
                    detail: failure_detail,
                },
            },
        ))
        .expect("Run fails while retaining unknown world state");

    for (sequence, outcome) in [(9, WorldOutcome::Applied), (10, WorldOutcome::NotApplied)] {
        assert!(matches!(
            machine.submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::TransitionEffect {
                    intent_id: intent_id.clone(),
                    transition: EffectTransition::Observe(outcome),
                },
            )),
            Err(CoreError::IllegalTransition(message)) if message.contains("terminal")
        ));
    }
    machine
        .submit(envelope(
            &machine,
            11,
            run_id,
            Command::TransitionEffect {
                intent_id,
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied),
            },
        ))
        .expect("failed Run settles only through reconciliation");
    let run = &machine.projection().runs[run_id];
    assert!(matches!(
        run.execution_status,
        RunExecutionStatus::Failed { .. }
    ));
    assert_eq!(run.world_settlement, WorldSettlementStatus::Settled);
    machine
        .verify_replay()
        .expect("failed terminal reconciliation replays exactly");
}

#[test]
fn completion_waits_for_observational_effect_world_settlement() {
    let run_id = "run:observational-completion";
    let (mut machine, intent_id) = observational_unknown_machine(run_id);
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("completion probe isolates world settlement from active execution");
    let run = &machine.projection().runs[run_id];
    assert!(run.obligations.is_empty());
    assert_eq!(run.world_settlement, WorldSettlementStatus::Unknown);
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            8,
            run_id,
            Command::CompleteRun { result: None },
        )),
        Err(CoreError::IllegalTransition(message))
            if message.contains("external-world Effect remains unsettled")
    ));

    let reconciliation = envelope(
        &machine,
        9,
        run_id,
        Command::TransitionEffect {
            intent_id,
            transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedNotApplied),
        },
    );
    let reconciliation_receipt = machine
        .submit(reconciliation.clone())
        .expect("observational Effect reconciles before completion");
    machine
        .submit(envelope(
            &machine,
            10,
            run_id,
            Command::CompleteRun { result: None },
        ))
        .expect("settled observational Run completes");
    let run = &machine.projection().runs[run_id];
    assert_eq!(run.execution_status, RunExecutionStatus::Completed);
    assert_eq!(run.world_settlement, WorldSettlementStatus::Settled);
    assert_eq!(
        machine
            .submit(reconciliation)
            .expect("exact pre-completion reconciliation replays after completion"),
        reconciliation_receipt
    );
    machine
        .verify_replay()
        .expect("observational completion replays exactly");
}

#[test]
fn compacted_base_rejects_completed_run_with_unknown_world_settlement() {
    let run_id = "run:compacted-completed-unknown";
    let (mut machine, _) = observational_unknown_machine(run_id);
    machine
        .compact_event_history(0)
        .expect("unknown observational history compacts");
    let mut snapshot = machine.snapshot();
    snapshot
        .base
        .as_mut()
        .expect("compacted base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .execution_status = RunExecutionStatus::Completed;
    reauthenticate_compacted_base(&mut snapshot);

    assert!(matches!(
        snapshot.base.as_ref().expect("base exists").identity(),
        Err(CoreError::Validation(message)) if message.contains("completed Run")
    ));
    assert!(matches!(
        Machine::restore(snapshot),
        Err(CoreError::Validation(message)) if message.contains("completed Run")
    ));
}

#[test]
fn compacted_base_rejects_unproved_plan_and_binding_lineage_after_reauthentication() {
    let mut machine = Machine::new();
    let initial = insert_plan(&mut machine, candidate());
    let mut unrelated_candidate = candidate();
    unrelated_candidate
        .metadata
        .insert("revision".to_owned(), "never-migrated".to_owned());
    let unrelated = insert_plan(&mut machine, unrelated_candidate);
    let initial_binding = test_execution_binding(&mut machine, "lineage-initial");
    let unrelated_binding = test_execution_binding(&mut machine, "lineage-never-selected");
    let run_id = "run:compacted-lineage-forgery";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: initial.plan_id.clone(),
                binding_context: initial_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let compaction = machine
        .compact_event_history(0)
        .expect("Run history compacts");
    let mut snapshot = machine.snapshot();
    let run = snapshot
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists");
    run.plan_lineage = vec![initial.plan_id.clone(), unrelated.plan_id, initial.plan_id];
    run.binding_lineage = vec![
        initial_binding.artifact_id.clone(),
        unrelated_binding.artifact_id,
        initial_binding.artifact_id,
    ];
    reauthenticate_compacted_base(&mut snapshot);

    assert!(Machine::restore_with_archive(snapshot, [compaction.archive_segment]).is_err());
}

#[test]
fn compacted_authority_events_rebuild_lineage_across_repeated_compaction() {
    let mut machine = Machine::new();
    let source = insert_plan(&mut machine, candidate());
    let mut target_candidate = candidate();
    target_candidate
        .metadata
        .insert("revision".to_owned(), "compacted-target".to_owned());
    let target = insert_plan(&mut machine, target_candidate);
    let source_binding = test_execution_binding(&mut machine, "compacted-authority-source");
    let target_binding = test_execution_binding(&mut machine, "compacted-authority-target");
    let future_binding = test_execution_binding(&mut machine, "compacted-authority-future");
    let run_id = "run:repeated-authority-compaction";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: source.plan_id.clone(),
                binding_context: source_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before source authority compaction");
    let mut archive_segments = vec![
        machine
            .compact_event_history(0)
            .expect("Run start compacts")
            .archive_segment,
    ];
    let migration = envelope(
        &machine,
        2,
        run_id,
        Command::MigrateRun {
            from_plan: source.plan_id.clone(),
            to_plan: target.plan_id.clone(),
            from_binding: source_binding.artifact_id.clone(),
            to_binding: target_binding.artifact_id.clone(),
            safe_point_id: format!("sha256:{}", "d".repeat(64)),
            target_epoch: 1,
            target_continuation_digest: "d".repeat(64),
        },
    );
    submit_new_with_archive(&mut machine, &archive_segments, migration).expect("Run migrates");
    archive_segments.push(
        machine
            .compact_event_history(0)
            .expect("migration compacts cumulatively")
            .archive_segment,
    );
    let binding_update = envelope(
        &machine,
        3,
        run_id,
        Command::UpdateBinding {
            binding_context: future_binding.artifact_id.clone(),
        },
    );
    submit_new_with_archive(&mut machine, &archive_segments, binding_update)
        .expect("future binding updates");
    archive_segments.push(
        machine
            .compact_event_history(0)
            .expect("binding update compacts cumulatively")
            .archive_segment,
    );
    assert_compacted_authority_lineage(
        machine.snapshot(),
        archive_segments,
        [source, target],
        [source_binding, target_binding, future_binding],
        run_id,
    );
}

fn assert_compacted_authority_lineage(
    snapshot: cymule_core::MachineSnapshot,
    archive_segments: Vec<cymule_core::MachineCommandArchiveSegment>,
    [source, target]: [SealedPlan; 2],
    [source_binding, target_binding, future_binding]: [ArtifactRef; 3],
    run_id: &str,
) {
    let base = snapshot.base.as_ref().expect("base exists");
    assert_eq!(base.archive_count, 4);
    assert_eq!(archive_segments.len(), 3);
    assert!(matches!(
        archive_segments[0].entries[0]
            .events
            .first()
            .map(|event| &event.payload),
        Some(EventPayload::RunStarted { .. })
    ));
    assert!(matches!(
        archive_segments[1].entries[0]
            .events
            .first()
            .map(|event| &event.payload),
        Some(EventPayload::RunMigrated { .. })
    ));
    assert!(matches!(
        archive_segments[2].entries[0]
            .events
            .first()
            .map(|event| &event.payload),
        Some(EventPayload::BindingUpdated { .. })
    ));
    let restored = Machine::restore_with_archive(snapshot, archive_segments)
        .expect("cumulative authority archive restores");
    let run = &restored.projection().runs[run_id];
    assert_eq!(
        run.plan_lineage,
        vec![source.plan_id, target.plan_id.clone()]
    );
    assert_eq!(run.current_plan, target.plan_id);
    assert_eq!(
        run.binding_lineage,
        vec![
            source_binding.artifact_id,
            target_binding.artifact_id,
            future_binding.artifact_id.clone(),
        ]
    );
    assert_eq!(run.current_binding_context, future_binding.artifact_id);
}

#[test]
fn trusted_base_anchor_is_exact_and_validates_the_post_cut_suffix() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "trusted-anchor");
    let run_id = "run:trusted-anchor";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let compaction = machine
        .compact_event_history(0)
        .expect("Run start compacts");
    let archive_segment = compaction.archive_segment;
    let anchor = machine
        .base_anchor()
        .expect("anchor derives")
        .expect("base exists");
    let before = machine.snapshot();
    Machine::restore_anchored(before.clone(), &anchor).expect("exact trusted anchor restores");

    let suffix = envelope(
        &machine,
        2,
        run_id,
        Command::RecordFact {
            key: "fact:anchored-suffix".to_owned(),
            value: test_fact_value("anchored-suffix-stable"),
        },
    );
    let lookup = new_archive_nonmembership_lookup(
        &machine,
        std::slice::from_ref(&archive_segment),
        suffix.command_id.as_str(),
    )
    .expect("suffix command archive non-membership resolves");
    machine
        .submit_with_archive_lookup(suffix, lookup)
        .expect("suffix fact records");
    let next = machine.snapshot();
    let restored = Machine::restore_anchored(next.clone(), &anchor)
        .expect("anchored restore replays the exact new suffix");
    assert_eq!(restored.snapshot(), next);
    assert_eq!(restored.projection(), machine.projection());

    let mut wrong_anchor = anchor.clone();
    wrong_anchor.projection_digest = "0".repeat(64);
    assert!(Machine::restore_anchored(next.clone(), &wrong_anchor).is_err());

    let mut wrong_base = next;
    wrong_base
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .facts
        .insert("fact:forged-base".to_owned(), "forged".to_owned());
    reauthenticate_compacted_base(&mut wrong_base);
    assert!(Machine::restore_anchored(wrong_base.clone(), &anchor).is_err());
    assert!(Machine::restore(wrong_base).is_err());
}

#[test]
fn compacted_authority_transition_cannot_downgrade_behind_an_applied_command() {
    let mut machine = Machine::new();
    let source = insert_plan(&mut machine, candidate());
    let mut target_candidate = candidate();
    target_candidate
        .metadata
        .insert("revision".to_owned(), "downgrade-target".to_owned());
    let target = insert_plan(&mut machine, target_candidate);
    let source_binding = test_execution_binding(&mut machine, "downgrade-source");
    let target_binding = test_execution_binding(&mut machine, "downgrade-target");
    let run_id = "run:authority-downgrade";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: source.plan_id.clone(),
                binding_context: source_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before the migration downgrade fixture");
    let migrate = envelope(
        &machine,
        2,
        run_id,
        Command::MigrateRun {
            from_plan: source.plan_id.clone(),
            to_plan: target.plan_id,
            from_binding: source_binding.artifact_id.clone(),
            to_binding: target_binding.artifact_id,
            safe_point_id: format!("sha256:{}", "f".repeat(64)),
            target_epoch: 1,
            target_continuation_digest: "f".repeat(64),
        },
    );
    let migration_receipt = machine.submit(migrate.clone()).expect("Run migrates");
    assert_eq!(migration_receipt.status, CommandReceiptStatus::Applied);
    let compaction = machine
        .compact_event_history(0)
        .expect("migration history compacts");

    let mut forged = machine.snapshot();
    let base = forged.base.as_mut().expect("base exists");
    let run = base.projection.runs.get_mut(run_id).expect("Run exists");
    run.current_plan.clone_from(&source.plan_id);
    run.plan_lineage = vec![source.plan_id];
    run.current_binding_context
        .clone_from(&source_binding.artifact_id);
    run.binding_lineage = vec![source_binding.artifact_id];
    reauthenticate_compacted_base(&mut forged);

    let error = Machine::restore_with_archive(forged, [compaction.archive_segment])
        .expect_err("downgraded migration must fail raw archive audit");
    assert!(
        matches!(error, CoreError::IdentityMismatch(_)),
        "unexpected downgrade error: {error:?}"
    );
}

#[test]
fn compacted_effect_requires_one_historical_plan_binding_authority_pair() {
    let mut machine = Machine::new();
    let source = insert_plan(&mut machine, root_effect_candidate());
    let mut target_candidate = root_effect_candidate();
    target_candidate
        .metadata
        .insert("revision".to_owned(), "pair-target".to_owned());
    let target = insert_plan(&mut machine, target_candidate);
    let source_binding = test_execution_binding(&mut machine, "pair-source");
    let target_binding = test_execution_binding(&mut machine, "pair-target");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("effect arguments store");
    let run_id = "run:compacted-authority-pair";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: source.plan_id.clone(),
                binding_context: source_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before historical Plan-binding migration");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::MigrateRun {
                from_plan: source.plan_id,
                to_plan: target.plan_id.clone(),
                from_binding: source_binding.artifact_id.clone(),
                to_binding: target_binding.artifact_id.clone(),
                safe_point_id: format!("sha256:{}", "e".repeat(64)),
                target_epoch: 1,
                target_continuation_digest: "e".repeat(64),
            },
        ))
        .expect("Run migrates");
    let intent_id = propose_root_effect(&mut machine, 3, run_id, args, target_binding.clone());
    let compaction = machine
        .compact_event_history(0)
        .expect("effect authority history compacts");
    let mut forged = machine.snapshot();
    forged
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .effects
        .get_mut(&intent_id)
        .expect("Effect exists")
        .execution_binding = source_binding;
    reauthenticate_compacted_base(&mut forged);
    assert!(Machine::restore_with_archive(forged, [compaction.archive_segment]).is_err());
}

#[test]
fn compacted_effect_binding_must_match_its_exact_admission_command() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, root_effect_candidate());
    let first_binding = test_execution_binding(&mut machine, "exact-effect-first");
    let selected_binding = test_execution_binding(&mut machine, "exact-effect-selected");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("effect arguments store");
    let run_id = "run:exact-effect-binding";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: first_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::UpdateBinding {
                binding_context: selected_binding.artifact_id.clone(),
            },
        ))
        .expect("binding updates");
    let intent_id = propose_root_effect(&mut machine, 3, run_id, args, selected_binding);
    machine
        .compact_event_history(0)
        .expect("Effect admission history compacts");
    let mut forged = machine.snapshot();
    forged
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .effects
        .get_mut(&intent_id)
        .expect("Effect exists")
        .execution_binding = first_binding;
    reauthenticate_compacted_base(&mut forged);
    assert!(matches!(
        Machine::restore(forged),
        Err(CoreError::IdentityMismatch(_))
    ));
}

#[test]
fn compacted_projection_double_digest_cannot_replace_command_derived_state() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "projection-command-replay");
    let run_id = "run:projection-command-replay";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    machine
        .submit(envelope(&machine, 2, run_id, Command::AdvanceEpoch))
        .expect("epoch advances");
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::RecordFact {
                key: "fact:command-replay".to_owned(),
                value: test_fact_value("original"),
            },
        ))
        .expect("fact records");
    machine
        .compact_event_history(0)
        .expect("command-derived history compacts");
    let snapshot = machine.snapshot();

    let mut epoch_forgery = snapshot.clone();
    epoch_forgery
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .epoch = 2;
    reauthenticate_compacted_base(&mut epoch_forgery);
    assert!(
        epoch_forgery
            .base
            .as_ref()
            .expect("base exists")
            .identity()
            .is_ok(),
        "the reducer shape alone remains reachable"
    );
    assert!(matches!(
        Machine::restore(epoch_forgery),
        Err(CoreError::IdentityMismatch(_))
    ));

    let mut fact_forgery = snapshot;
    fact_forgery
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .facts
        .insert("fact:command-replay".to_owned(), "forged".to_owned());
    reauthenticate_compacted_base(&mut fact_forgery);
    assert!(matches!(
        Machine::restore(fact_forgery),
        Err(CoreError::IdentityMismatch(_))
    ));
}

#[test]
fn compacted_base_rejects_cross_run_and_stale_run_frontiers_after_reauthentication() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "frontier");
    for (sequence, run_id) in [(1, "run:frontier-a"), (2, "run:frontier-b")] {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::StartRun {
                    plan_id: plan.plan_id.clone(),
                    binding_context: binding.artifact_id.clone(),
                    input: test_run_input_ref(),
                    material_digest: String::new(),
                    initial_attempt: placeholder_initial_attempt(),
                },
            ))
            .expect("Run starts");
    }
    for (sequence, run_id) in [(3, "run:frontier-a"), (4, "run:frontier-b")] {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::RecordFact {
                    key: format!("fact:{run_id}"),
                    value: test_fact_value("frontier-stable"),
                },
            ))
            .expect("Run fact records");
    }
    let compaction = machine
        .compact_event_history(0)
        .expect("multi-Run history compacts");
    let snapshot = machine.snapshot();
    let anchor = machine
        .base_anchor()
        .expect("anchor derives")
        .expect("base exists");
    let a_events = compaction
        .archive_segment
        .entries
        .iter()
        .flat_map(|entry| entry.events.iter())
        .filter(|event| event.run_id == "run:frontier-a")
        .map(|event| event.event_id.clone())
        .collect::<Vec<_>>();
    let b_last = compaction
        .archive_segment
        .entries
        .iter()
        .rev()
        .flat_map(|entry| entry.events.iter().rev())
        .find(|event| event.run_id == "run:frontier-b")
        .expect("Run B has a frontier")
        .event_id
        .clone();

    for forged_frontier in [a_events[0].clone(), b_last] {
        let mut forged = snapshot.clone();
        forged
            .base
            .as_mut()
            .expect("base exists")
            .projection
            .runs
            .get_mut("run:frontier-a")
            .expect("Run A exists")
            .last_event = forged_frontier;
        reauthenticate_compacted_base(&mut forged);
        assert!(Machine::restore_anchored(forged.clone(), &anchor).is_err());
        assert!(
            Machine::restore_with_archive(forged, [compaction.archive_segment.clone()]).is_err()
        );
    }
}

#[test]
fn compacted_base_rejects_deferred_release_before_scope_commit() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, root_effect_candidate());
    let binding = test_execution_binding(&mut machine, "deferred-release");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("effect arguments store");
    let run_id = "run:deferred-release";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let intent_id = propose_root_effect(&mut machine, 2, run_id, args, binding);
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        ))
        .expect("Effect prepares");
    machine
        .compact_event_history(0)
        .expect("open-scope Effect history compacts");
    let mut forged = machine.snapshot();
    forged
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .effects
        .get_mut(&intent_id)
        .expect("Effect exists")
        .phase = EffectPhase::ReleaseAuthorized;
    reauthenticate_compacted_base(&mut forged);
    assert!(matches!(
        forged.base.as_ref().expect("base exists").identity(),
        Err(CoreError::Validation(message)) if message.contains("dispatch policy")
    ));
    assert!(matches!(
        Machine::restore(forged),
        Err(CoreError::Validation(_))
    ));
}

#[test]
fn compacted_child_scope_requires_exact_plan_site_and_parent_authority() {
    let mut scoped = candidate();
    for site_id in ["scope.child", "scope.sibling"] {
        scoped.definitions[0].body.steps.push(Step {
            id: site_id.to_owned(),
            operation: Operation::Scope {
                body: Box::new(Region {
                    steps: Vec::new(),
                    result: Expression::Input,
                }),
                bind: None,
            },
        });
    }
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, scoped);
    let binding = test_execution_binding(&mut machine, "scope-authority");
    let run_id = "run:scope-authority";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let root_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let child = cymule_core::plan_scope_id(run_id, &plan.plan_id, &root_invocation, "main", &[1])
        .expect("child scope derives");
    let sibling = cymule_core::plan_scope_id(run_id, &plan.plan_id, &root_invocation, "main", &[2])
        .expect("sibling scope derives");
    for (sequence, (scope_id, site_id)) in [
        (2, (child.clone(), "scope.child")),
        (3, (sibling.clone(), "scope.sibling")),
    ] {
        machine
            .submit(envelope(
                &machine,
                sequence,
                run_id,
                Command::OpenScope {
                    scope_id,
                    parent_scope: cymule_core::ROOT_SCOPE_ID.to_owned(),
                    invocation_id: root_invocation.clone(),
                    invocation_path: Vec::new(),
                    definition_id: "main".to_owned(),
                    region_path: Vec::new(),
                    site_id: site_id.to_owned(),
                },
            ))
            .expect("child scope opens");
    }
    machine
        .compact_event_history(0)
        .expect("scope history compacts");
    let snapshot = machine.snapshot();
    assert_compacted_scope_forgeries(snapshot, run_id, &child, sibling);
}

fn assert_compacted_scope_forgeries(
    snapshot: cymule_core::MachineSnapshot,
    run_id: &str,
    child: &str,
    sibling: String,
) {
    let mut wrong_site = snapshot.clone();
    wrong_site
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .scopes
        .get_mut(child)
        .expect("child exists")
        .site_id = Some("scope.sibling".to_owned());
    reauthenticate_compacted_base(&mut wrong_site);
    let error = Machine::restore(wrong_site).expect_err("forged scope site must fail restore");
    assert!(
        matches!(
            error,
            CoreError::IdentityMismatch(_) | CoreError::Validation(_)
        ),
        "unexpected scope-site error: {error:?}"
    );

    let mut wrong_parent = snapshot;
    wrong_parent
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .scopes
        .get_mut(child)
        .expect("child exists")
        .parent_scope = Some(sibling);
    reauthenticate_compacted_base(&mut wrong_parent);
    let error = Machine::restore(wrong_parent).expect_err("forged scope parent must fail restore");
    assert!(
        matches!(
            error,
            CoreError::IdentityMismatch(_) | CoreError::Validation(_)
        ),
        "unexpected scope-parent error: {error:?}"
    );
}

#[test]
fn attempt_authority_allows_only_one_active_attempt_and_terminal_epochs_are_prior() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "attempt-authority");
    let run_id = "run:attempt-authority";
    let first_attempt_id = test_content_id("attempt:first");
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before the distinct first Attempt starts");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::BeginAttempt {
                attempt_id: first_attempt_id.clone(),
                continuation_id: test_content_id("continuation:root"),
                occurrence_binding: test_content_id("binding:first"),
                continuation_epoch: 0,
                execution_fence: 1,
            },
        ))
        .expect("first Attempt starts");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            3,
            run_id,
            Command::BeginAttempt {
                attempt_id: test_content_id("attempt:second"),
                continuation_id: test_content_id("continuation:root"),
                occurrence_binding: test_content_id("binding:second"),
                continuation_epoch: 0,
                execution_fence: 2,
            },
        )),
        Err(CoreError::IllegalTransition(message)) if message.contains("active Attempt")
    ));
    assert_compacted_attempt_authority(machine, run_id, &first_attempt_id);
}

fn assert_compacted_attempt_authority(mut machine: Machine, run_id: &str, first_attempt_id: &str) {
    let first_compaction = machine
        .compact_event_history(0)
        .expect("active Attempt history compacts");
    let first_segment = first_compaction.archive_segment;
    let mut double_active = machine.snapshot();
    let run = double_active
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists");
    let mut duplicate = run.attempts[first_attempt_id].clone();
    duplicate.attempt_id = test_content_id("attempt:forged");
    duplicate.execution_fence = 2;
    run.attempts.insert(duplicate.attempt_id.clone(), duplicate);
    reauthenticate_compacted_base(&mut double_active);
    assert!(matches!(
        Machine::restore(double_active),
        Err(CoreError::Validation(message)) if message.contains("more than one active Attempt")
    ));

    let failure = machine
        .put_artifact("cymule.attempt-failure/1", b"failure".to_vec())
        .expect("failure Artifact stores");
    let fail = envelope(
        &machine,
        4,
        run_id,
        Command::FailRun {
            failure: RunFailure {
                class: RunFailureClass::DeclaredFailure,
                code: "attempt_failed".to_owned(),
                detail: failure,
            },
        },
    );
    submit_new_with_archive(&mut machine, std::slice::from_ref(&first_segment), fail)
        .expect("Run fails and fences Attempt");
    machine
        .compact_event_history(0)
        .expect("terminal Attempt history compacts");
    let mut forged_epoch = machine.snapshot();
    let run = forged_epoch
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists");
    run.attempts
        .get_mut(first_attempt_id)
        .expect("Attempt exists")
        .continuation_epoch = run.epoch;
    reauthenticate_compacted_base(&mut forged_epoch);
    assert!(matches!(
        Machine::restore(forged_epoch),
        Err(CoreError::Validation(message)) if message.contains("terminal reducer state")
    ));
}

#[test]
fn compacted_terminal_effect_and_result_artifact_must_match_reducer_state() {
    assert_compacted_terminal_effect_projection();
    assert_compacted_terminal_result_reference();
}

fn assert_compacted_terminal_effect_projection() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, root_effect_candidate());
    let binding = test_execution_binding(&mut machine, "terminal-effect");
    let args = machine
        .put_artifact(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}".to_vec())
        .expect("effect arguments store");
    let failure = machine
        .put_artifact("cymule.terminal-failure/1", b"failure".to_vec())
        .expect("failure Artifact stores");
    let run_id = "run:terminal-effect-state";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let intent_id = propose_root_effect(&mut machine, 2, run_id, args, binding);
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::FailRun {
                failure: RunFailure {
                    class: RunFailureClass::DeclaredFailure,
                    code: "terminal_effect_failed".to_owned(),
                    detail: failure,
                },
            },
        ))
        .expect("Run fails");
    machine
        .compact_event_history(0)
        .expect("terminal Effect history compacts");
    let mut forged = machine.snapshot();
    forged
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .effects
        .get_mut(&intent_id)
        .expect("Effect exists")
        .outcome = WorldOutcome::Unobserved;
    reauthenticate_compacted_base(&mut forged);
    assert!(matches!(
        Machine::restore(forged),
        Err(CoreError::Validation(message)) if message.contains("reducer-unreachable")
    ));
}

fn assert_compacted_terminal_result_reference() {
    let mut completed = Machine::new();
    let plan = insert_plan(&mut completed, candidate());
    let binding = test_execution_binding(&mut completed, "completed-result");
    let result = completed
        .put_artifact("cymule.completed-result/1", b"result".to_vec())
        .expect("Result Artifact stores");
    let completed_run = "run:completed-result-shape";
    completed
        .submit(envelope(
            &completed,
            1,
            completed_run,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    completed
        .submit(envelope(
            &completed,
            2,
            completed_run,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("root scope commits");
    let initial = initial_attempt(&completed, completed_run);
    completed
        .submit(envelope(
            &completed,
            0,
            completed_run,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("completed-result fixture yields its initial Attempt");
    completed
        .submit(envelope(
            &completed,
            3,
            completed_run,
            Command::CompleteRun {
                result: Some(result),
            },
        ))
        .expect("Run completes");
    completed
        .compact_event_history(0)
        .expect("completed history compacts");
    let mut malformed_result = completed.snapshot();
    let result = malformed_result
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(completed_run)
        .expect("Run exists")
        .result
        .as_mut()
        .expect("Result exists");
    "cymule.artifact/forged".clone_into(&mut result.identity_version);
    reauthenticate_compacted_base(&mut malformed_result);
    assert!(matches!(
        malformed_result.base.as_ref().expect("base exists").identity(),
        Err(CoreError::Validation(message)) if message.contains("Artifact identity version")
    ));
    assert!(matches!(
        Machine::restore(malformed_result),
        Err(CoreError::Validation(_))
    ));
}

#[test]
fn compacted_base_rejects_reducer_unreachable_run_and_effect_states() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "unreachable-completed");
    let run_id = "run:unreachable-completed";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    assert!(initial_attempt(&machine, run_id).active);
    machine
        .compact_event_history(0)
        .expect("active history compacts");
    let mut completed = machine.snapshot();
    completed
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(run_id)
        .expect("Run exists")
        .execution_status = RunExecutionStatus::Completed;
    reauthenticate_compacted_base(&mut completed);
    assert!(matches!(
        Machine::restore(completed),
        Err(CoreError::Validation(message)) if message.contains("active Attempt or open scope")
    ));

    let effect_run_id = "run:unreachable-cancelled-effect";
    let (mut effect_machine, intent_id) = observational_unknown_machine(effect_run_id);
    effect_machine
        .compact_event_history(0)
        .expect("unknown Effect history compacts");
    let mut cancelled_unknown = effect_machine.snapshot();
    cancelled_unknown
        .base
        .as_mut()
        .expect("base exists")
        .projection
        .runs
        .get_mut(effect_run_id)
        .expect("Run exists")
        .effects
        .get_mut(&intent_id)
        .expect("Effect exists")
        .phase = EffectPhase::CancelledBeforeRelease;
    reauthenticate_compacted_base(&mut cancelled_unknown);
    assert!(matches!(
        Machine::restore(cancelled_unknown),
        Err(CoreError::Validation(message)) if message.contains("reducer-unreachable")
    ));
}

#[test]
fn compacted_terminal_runs_require_retained_result_failure_and_cancellation_artifacts() {
    for status in ["completed", "failed", "cancelled"] {
        let (snapshot, terminal_artifact, archive_segment) = terminal_artifact_fixture(status);
        Machine::restore_with_archive(snapshot.clone(), [archive_segment.clone()])
            .expect("legal terminal archive restores");
        let mut missing = snapshot;
        missing
            .artifacts
            .retain(|record| record.reference.artifact_id != terminal_artifact.artifact_id);
        let error = Machine::restore_with_archive(missing, [archive_segment])
            .expect_err("terminal Artifact removal must fail");
        assert!(
            matches!(error, CoreError::NotFound(_)),
            "unexpected terminal Artifact error: {error:?}"
        );
    }
}

fn terminal_artifact_fixture(
    status: &str,
) -> (
    cymule_core::MachineSnapshot,
    ArtifactRef,
    cymule_core::MachineCommandArchiveSegment,
) {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, status);
    let terminal_artifact = machine
        .put_artifact(
            format!("cymule.{status}-evidence/1"),
            status.as_bytes().to_vec(),
        )
        .expect("terminal Artifact stores");
    let run_id = format!("run:terminal-artifact-{status}");
    machine
        .submit(envelope(
            &machine,
            1,
            &run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    match status {
        "completed" => {
            assert_completed_terminal_evidence(&mut machine, &run_id, &terminal_artifact);
        }
        "failed" => {
            machine
                .submit(envelope(
                    &machine,
                    2,
                    &run_id,
                    Command::FailRun {
                        failure: RunFailure {
                            class: RunFailureClass::DeclaredFailure,
                            code: "declared_failure".to_owned(),
                            detail: terminal_artifact.clone(),
                        },
                    },
                ))
                .expect("Run fails");
        }
        "cancelled" => {
            machine
                .submit(envelope(
                    &machine,
                    2,
                    &run_id,
                    Command::CancelRun {
                        reason: terminal_artifact.clone(),
                    },
                ))
                .expect("Run cancels");
        }
        _ => unreachable!("closed terminal test matrix"),
    }
    let compaction = machine
        .compact_event_history(0)
        .expect("terminal history compacts");
    (
        machine.snapshot(),
        terminal_artifact,
        compaction.archive_segment,
    )
}

fn assert_completed_terminal_evidence(
    machine: &mut Machine,
    run_id: &str,
    terminal_artifact: &ArtifactRef,
) {
    let initial = initial_attempt(machine, run_id);
    machine
        .submit(envelope(
            machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("completed evidence fixture explicitly yields its Attempt");
    machine
        .submit(envelope(
            machine,
            2,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("root scope commits");
    machine
        .submit(envelope(
            machine,
            3,
            run_id,
            Command::CompleteRun {
                result: Some(terminal_artifact.clone()),
            },
        ))
        .expect("Run completes");
}

#[test]
fn recursive_definition_sccs_fail_closed_and_diamonds_remain_valid() {
    fn definition(id: &str, targets: &[(&str, &str)]) -> Definition {
        Definition {
            id: id.to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: targets
                    .iter()
                    .map(|(site, target)| Step {
                        id: (*site).to_owned(),
                        operation: Operation::Invoke {
                            definition: (*target).to_owned(),
                            input: Expression::Input,
                            bind: None,
                        },
                    })
                    .collect(),
                result: Expression::Literal { value: json!(null) },
            },
        }
    }

    let mut self_cycle = candidate();
    self_cycle.components.clear();
    self_cycle.definitions = vec![definition("main", &[("invoke.self", "main")])];
    assert!(matches!(
        cymule_core::seal_plan(self_cycle),
        Err(CoreError::Validation(message)) if message.contains("recursive definition invocation")
    ));

    for definitions in [
        vec![
            definition("main", &[("invoke.a-b", "b")]),
            definition("b", &[("invoke.b-a", "main")]),
        ],
        vec![
            definition("main", &[("invoke.a-b", "b")]),
            definition("b", &[("invoke.b-c", "c")]),
            definition("c", &[("invoke.c-a", "main")]),
        ],
    ] {
        let mut cycle = candidate();
        cycle.components.clear();
        cycle.definitions = definitions;
        assert!(matches!(
            cymule_core::seal_plan(cycle),
            Err(CoreError::Validation(message)) if message.contains("recursive definition invocation")
        ));
    }

    let mut nested_cycle = candidate();
    nested_cycle.components.clear();
    nested_cycle.definitions = vec![Definition {
        id: "main".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: vec![Step {
                id: "scope.recursive".to_owned(),
                operation: Operation::Scope {
                    body: Box::new(Region {
                        steps: vec![Step {
                            id: "invoke.nested-self".to_owned(),
                            operation: Operation::Invoke {
                                definition: "main".to_owned(),
                                input: Expression::Input,
                                bind: None,
                            },
                        }],
                        result: Expression::Literal { value: json!(null) },
                    }),
                    bind: None,
                },
            }],
            result: Expression::Literal { value: json!(null) },
        },
    }];
    assert!(cymule_core::seal_plan(nested_cycle).is_err());

    let mut diamond = candidate();
    diamond.components.clear();
    diamond.definitions = vec![
        definition(
            "main",
            &[("invoke.left", "left"), ("invoke.right", "right")],
        ),
        definition("left", &[("invoke.left-leaf", "leaf")]),
        definition("right", &[("invoke.right-leaf", "leaf")]),
        definition("leaf", &[]),
    ];
    cymule_core::seal_plan(diamond).expect("acyclic diamond seals");
}

#[test]
fn machine_insert_and_restore_reject_invalid_executable_plan_schemas() {
    let mut malformed = candidate();
    malformed.definitions[0].input_schema = json!({"type": 42});
    let plan = SealedPlan {
        plan_id: cymule_core::content_id("cymule.plan/1", &malformed)
            .expect("malformed candidate still canonicalizes"),
        candidate: malformed,
    };
    let mut machine = Machine::new();
    assert!(matches!(
        machine.insert_plan(plan.clone()),
        Err(CoreError::Validation(message)) if message.contains("schema is invalid")
    ));
    let mut snapshot = Machine::new().snapshot();
    snapshot.plans.push(plan);
    assert!(matches!(
        Machine::restore(snapshot),
        Err(CoreError::Validation(message)) if message.contains("schema is invalid")
    ));
}

#[test]
fn command_idempotency_and_stale_action_are_explicit() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "idempotency");
    let start = envelope(
        &machine,
        1,
        "run:idempotency",
        Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: binding.artifact_id,
            input: test_run_input_ref(),
            material_digest: String::new(),
            initial_attempt: placeholder_initial_attempt(),
        },
    );
    let first = machine.submit(start.clone()).expect("start applies");
    assert_eq!(
        first,
        machine.submit(start.clone()).expect("retry is idempotent")
    );

    let mut reused = start;
    reused.actor = "test:different".to_owned();
    assert!(matches!(
        machine.submit(reused),
        Err(CoreError::CommandReuse(_))
    ));

    let stale = machine
        .projection()
        .runs
        .get("run:idempotency")
        .expect("run exists")
        .precondition_token();
    machine
        .submit(envelope(
            &machine,
            2,
            "run:idempotency",
            Command::RecordFact {
                key: "fact:one".to_owned(),
                value: test_fact_value("v1"),
            },
        ))
        .expect("fact applies");
    let mut outdated = envelope(
        &machine,
        3,
        "run:idempotency",
        Command::RecordFact {
            key: "fact:two".to_owned(),
            value: test_fact_value("v2"),
        },
    );
    outdated.expected_precondition = Some(stale);
    let receipt = machine.submit(outdated).expect("conflict is a receipt");
    assert_eq!(receipt.status, CommandReceiptStatus::Conflict);
    assert_eq!(receipt.error_code.as_deref(), Some("stale_action"));
}

#[test]
fn ordered_command_admissions_replay_exact_conflicts_without_advancing_run_frontier() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "conflict-admission");
    let run_id = "run:conflict-admission";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let stale = machine.projection().runs[run_id].precondition_token();
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::RecordFact {
                key: "fact:advance".to_owned(),
                value: test_fact_value("conflict-frontier-stable"),
            },
        ))
        .expect("frontier advances");
    let compaction = machine
        .compact_event_history(0)
        .expect("applied Events compact");
    let archive_segment = compaction.archive_segment;
    let anchor = machine
        .base_anchor()
        .expect("anchor derives")
        .expect("base exists");
    let (stale_command, conflict, after_conflict, projection_digest) =
        admit_stale_conflict_fixture(&mut machine, run_id, &archive_segment, &anchor, &stale);
    let snapshot = after_conflict;
    for (field, value) in [
        ("current_precondition", json!(stale)),
        ("error_code", json!("forged_conflict")),
        ("message", json!("forged message")),
    ] {
        let mut encoded = serde_json::to_value(&snapshot).expect("snapshot encodes");
        encoded["commands"][&stale_command.command_id]["receipt"][field] = value;
        let mut forged: cymule_core::MachineSnapshot =
            serde_json::from_value(encoded).expect("forged snapshot shape decodes");
        reauthenticate_last_command_admission(&mut forged);
        assert!(Machine::restore_with_archive(forged.clone(), [archive_segment.clone()]).is_err());
        assert!(Machine::restore_anchored(forged, &anchor).is_err());
    }
    assert_archived_conflict_replay(
        &mut machine,
        &archive_segment,
        &stale_command,
        &conflict,
        &projection_digest,
    );
    assert_genesis_conflict_archive();
}

fn admit_stale_conflict_fixture(
    machine: &mut Machine,
    run_id: &str,
    archive_segment: &cymule_core::MachineCommandArchiveSegment,
    anchor: &cymule_core::MachineBaseAnchor,
    stale: &str,
) -> (
    CommandEnvelope,
    cymule_core::CommandReceipt,
    cymule_core::MachineSnapshot,
    String,
) {
    let event_count = machine.events().count();
    let projection_digest = machine.projection().digest().expect("projection hashes");
    let mut stale_command = envelope(
        machine,
        3,
        run_id,
        Command::RecordFact {
            key: "fact:conflict".to_owned(),
            value: test_fact_value("never-applied"),
        },
    );
    stale_command.expected_precondition = Some(stale.to_owned());
    let lookup = new_archive_nonmembership_lookup(
        machine,
        std::slice::from_ref(archive_segment),
        stale_command.command_id.as_str(),
    )
    .expect("stale command archive non-membership resolves");
    let conflict = machine
        .submit_with_archive_lookup(stale_command.clone(), lookup)
        .expect("stale admission returns receipt");
    assert_eq!(conflict.status, CommandReceiptStatus::Conflict);
    assert_eq!(conflict.error_code.as_deref(), Some("stale_action"));
    assert_eq!(
        conflict.message.as_deref(),
        Some("the Run changed after the caller's view")
    );
    assert_eq!(conflict.observed_precondition.as_deref(), Some(stale));
    assert_eq!(machine.events().count(), event_count);
    assert_eq!(
        machine.projection().digest().expect("projection hashes"),
        projection_digest
    );
    let after_conflict = machine.snapshot();
    Machine::restore_with_archive(after_conflict.clone(), [archive_segment.clone()])
        .expect("full audit replays exact Conflict");
    Machine::restore_anchored(after_conflict.clone(), anchor)
        .expect("anchored suffix replays exact Conflict");
    (stale_command, conflict, after_conflict, projection_digest)
}

fn assert_archived_conflict_replay(
    machine: &mut Machine,
    archive_segment: &cymule_core::MachineCommandArchiveSegment,
    stale_command: &CommandEnvelope,
    conflict: &cymule_core::CommandReceipt,
    projection_digest: &str,
) {
    let before_conflict_rotation = machine.snapshot();
    let conflict_rotation = machine
        .compact_event_free_admissions()
        .expect("conflict-only hot tail rotates independently");
    assert_eq!(conflict_rotation.archive_segment.header.event_count, 0);
    assert_eq!(conflict_rotation.compacted_events, 3);
    assert_eq!(
        machine.projection().digest().expect("projection hashes"),
        projection_digest
    );
    let after_conflict_rotation = machine.snapshot();
    assert!(after_conflict_rotation.admissions.is_empty());
    assert!(
        after_conflict_rotation
            .command_digests()
            .expect("command catalog hashes")
            .is_empty()
    );
    let rotation_delta = cymule_core::MachineDelta::between_compaction(
        &before_conflict_rotation,
        &after_conflict_rotation,
        &conflict_rotation.archive_segment,
    )
    .expect("conflict-only compaction delta binds its archive segment");
    assert!(rotation_delta.compacted_event_ids.is_empty());
    let mut assembled_rotation = before_conflict_rotation;
    assembled_rotation
        .apply_compaction_delta_anchored(
            &rotation_delta,
            after_conflict_rotation
                .base_anchor
                .as_ref()
                .expect("rotated base has anchor"),
            &conflict_rotation.archive_segment,
        )
        .expect("conflict-only compaction applies transactionally");
    assert_eq!(assembled_rotation, after_conflict_rotation);
    let archived_conflict_proof = conflict_rotation
        .archive_segment
        .command_index_proof(0, &[])
        .expect("archived Conflict membership proof derives");
    let archived_conflict_entry = Box::new(conflict_rotation.archive_segment.entries[0].clone());
    let after_rotation_bytes = machine.snapshot();
    assert_eq!(
        machine
            .submit_with_archive_lookup(
                stale_command.clone(),
                cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof: archived_conflict_proof.clone(),
                    entry: archived_conflict_entry.clone(),
                },
            )
            .expect("archived Conflict lost acknowledgment replays"),
        *conflict
    );
    assert_eq!(machine.snapshot(), after_rotation_bytes);
    let mut changed_conflict = stale_command.clone();
    "actor:changed-conflict".clone_into(&mut changed_conflict.actor);
    assert!(matches!(
        machine.submit_with_archive_lookup(
            changed_conflict,
            cymule_core::MachineCommandArchiveLookup::Member {
                index_proof: archived_conflict_proof,
                entry: archived_conflict_entry,
            },
        ),
        Err(CoreError::CommandReuse(_))
    ));
    assert_eq!(machine.snapshot(), after_rotation_bytes);
    Machine::restore_with_archive(
        after_conflict_rotation,
        [archive_segment.clone(), conflict_rotation.archive_segment],
    )
    .expect("raw audit accepts an Event-free descendant segment");
}

fn assert_genesis_conflict_archive() {
    let mut missing_run = Machine::new();
    let missing_envelope = CommandEnvelope {
        command_version: COMMAND_VERSION.to_owned(),
        command_id: "command:missing-run-conflict".to_owned(),
        actor: "actor:test".to_owned(),
        run_id: "run:missing-conflict".to_owned(),
        expected_precondition: Some("pre:0:missing".to_owned()),
        command: Command::RecordFact {
            key: "fact:missing".to_owned(),
            value: test_fact_value("missing-never-applied"),
        },
    };
    let missing_conflict = missing_run
        .submit(missing_envelope.clone())
        .expect("missing Run is a canonical stale conflict");
    assert_eq!(missing_conflict.status, CommandReceiptStatus::Conflict);
    assert!(missing_conflict.current_precondition.is_none());
    Machine::restore(missing_run.snapshot()).expect("pre-Run Conflict replays exactly");
    let genesis_parent = missing_run.snapshot();
    let genesis_rotation = missing_run
        .compact_event_free_admissions()
        .expect("genesis conflict-only tail compacts without an Event");
    assert_eq!(genesis_rotation.compacted_events, 0);
    assert_eq!(genesis_rotation.archive_segment.header.event_count, 0);
    let genesis_snapshot = missing_run.snapshot();
    let genesis_delta = cymule_core::MachineDelta::between_compaction(
        &genesis_parent,
        &genesis_snapshot,
        &genesis_rotation.archive_segment,
    )
    .expect("genesis conflict compaction delta derives");
    let mut genesis_assembled = genesis_parent;
    genesis_assembled
        .apply_compaction_delta(&genesis_delta, &genesis_rotation.archive_segment)
        .expect("genesis conflict compaction applies");
    assert_eq!(genesis_assembled, genesis_snapshot);
    let genesis_restored = Machine::restore_with_archive(
        genesis_snapshot.clone(),
        [genesis_rotation.archive_segment.clone()],
    )
    .expect("genesis conflict archive audits");
    Machine::restore_anchored(
        genesis_snapshot,
        genesis_restored
            .base_anchor()
            .expect("anchor reads")
            .as_ref()
            .expect("genesis conflict creates base"),
    )
    .expect("genesis conflict base restores from exact anchor");
    let proof = genesis_rotation
        .archive_segment
        .command_proof(0)
        .expect("genesis conflict proof derives");
    let index_proof = genesis_rotation
        .archive_segment
        .command_index_proof(0, &[])
        .expect("genesis conflict index proof derives");
    assert_eq!(
        genesis_restored
            .replay_archived_command(&missing_envelope, &proof, &index_proof)
            .expect("genesis archived conflict replays"),
        missing_conflict
    );
}

fn assert_command_identity_boundaries(plan: &SealedPlan) {
    let mut machine = Machine::new();
    machine
        .insert_plan(plan.clone())
        .expect("identity Plan stages");
    let binding = test_execution_binding(&mut machine, "identity-boundary");
    let valid = envelope(
        &machine,
        1,
        "run:identity-boundary",
        Command::StartRun {
            plan_id: plan.plan_id.clone(),
            binding_context: binding.artifact_id,
            input: test_run_input_ref(),
            material_digest: String::new(),
            initial_attempt: placeholder_initial_attempt(),
        },
    );
    for (field, value) in [
        ("command_version", json!("cymule.command/4")),
        ("command_version", json!("cymule.command/invalid")),
        ("command_id", json!("")),
        ("command_id", json!("🦀".repeat(513))),
        ("actor", json!("")),
        ("run_id", json!("")),
        ("actor", json!("actor:\ninvalid")),
        ("run_id", json!("run:\u{7f}invalid")),
    ] {
        let mut encoded = serde_json::to_value(&valid).expect("valid envelope encodes");
        encoded[field] = value;
        let invalid = serde_json::from_value(encoded).expect("invalid identity retains wire shape");
        let mut rejected = machine.clone();
        let before = rejected.snapshot();
        assert!(matches!(
            rejected.submit(invalid),
            Err(CoreError::Validation(_))
        ));
        assert_eq!(rejected.snapshot(), before);
        assert!(rejected.projection().runs.is_empty());
    }
    let mut boundary = valid;
    boundary.command_id = "🦀".repeat(512);
    boundary.actor = "界".repeat(512);
    boundary.run_id = "跑".repeat(512);
    bind_start_material(&machine, &mut boundary);
    machine
        .submit(boundary)
        .expect("512 Unicode-scalar identities are legal regardless of UTF-8 byte length");
}

#[test]
fn envelopes_footprints_facts_attempts_and_scope_parents_fail_closed() {
    let mut scope_candidate = candidate();
    for site_id in ["scope.child", "scope.sibling"] {
        scope_candidate.definitions[0].body.steps.push(Step {
            id: site_id.to_owned(),
            operation: Operation::Scope {
                body: Box::new(Region {
                    steps: Vec::new(),
                    result: Expression::Input,
                }),
                bind: None,
            },
        });
    }
    let sealed = seal_for_kernel(scope_candidate).expect("Plan seals");
    assert_command_identity_boundaries(&sealed);

    let mut machine = Machine::new();
    machine.insert_plan(sealed.clone()).expect("Plan inserts");
    machine
        .insert_plan(sealed.clone())
        .expect("identical Plan insertion is idempotent");
    let binding = test_execution_binding(&mut machine, "invariants");
    assert_eq!(machine.plan(&sealed.plan_id), Some(&sealed));
    let run_id = "run:invariants";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: sealed.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let root_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let start = machine
        .events()
        .find(|event| matches!(event.payload, EventPayload::RunStarted { .. }))
        .expect("RunStarted Event exists before the initial AttemptStarted");
    assert_eq!(start.reads, BTreeSet::new());
    assert_eq!(start.writes, BTreeSet::from([format!("run:{run_id}")]));
    assert_eq!(start.coordination_key, Some(format!("run:{run_id}")));
    assert_fact_footprints_and_immutability(&mut machine, run_id);
    assert_explicit_attempt_cycle(&mut machine, run_id);
    let (child_scope, sibling_scope) =
        open_child_scope_fixture(&mut machine, run_id, &root_invocation);
    assert_open_scope_parent_fences(&mut machine, run_id, &root_invocation, &sibling_scope);
    assert_scope_closure_order(
        &mut machine,
        run_id,
        root_invocation,
        child_scope,
        sibling_scope,
    );
}

fn assert_fact_footprints_and_immutability(machine: &mut Machine, run_id: &str) {
    let before_fact = machine.projection().digest().expect("projection hashes");
    machine
        .submit(envelope(
            machine,
            2,
            run_id,
            Command::RecordFact {
                key: "fact:stable".to_owned(),
                value: test_fact_value("one"),
            },
        ))
        .expect("fact records");
    let fact = machine.events().last().expect("fact event");
    assert_eq!(fact.reads, BTreeSet::from(["fact:fact:stable".to_owned()]));
    assert_eq!(fact.writes, BTreeSet::from(["fact:fact:stable".to_owned()]));
    assert_eq!(fact.coordination_key.as_deref(), Some("fact:fact:stable"));
    let after_fact = machine.projection().digest().expect("projection hashes");
    assert_eq!(after_fact.len(), 64);
    assert_ne!(before_fact, after_fact);
    machine
        .submit(envelope(
            machine,
            3,
            run_id,
            Command::RecordFact {
                key: "fact:stable".to_owned(),
                value: test_fact_value("one"),
            },
        ))
        .expect("identical fact repeats");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            4,
            run_id,
            Command::RecordFact {
                key: "fact:stable".to_owned(),
                value: test_fact_value("different"),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

fn assert_explicit_attempt_cycle(machine: &mut Machine, run_id: &str) {
    let initial = initial_attempt(machine, run_id);
    machine
        .submit(envelope(
            machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before the separate Attempt lifecycle probe");
    machine
        .submit(envelope(
            machine,
            5,
            run_id,
            Command::BeginAttempt {
                attempt_id: test_content_id("attempt:invariants"),
                continuation_id: test_content_id("continuation:invariants"),
                occurrence_binding: test_content_id("binding:worker"),
                continuation_epoch: 0,
                execution_fence: 1,
            },
        ))
        .expect("attempt starts");
    machine
        .submit(envelope(
            machine,
            6,
            run_id,
            Command::YieldAttempt {
                attempt_id: test_content_id("attempt:invariants"),
                continuation_epoch: 0,
                execution_fence: 1,
            },
        ))
        .expect("active attempt yields");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            7,
            run_id,
            Command::YieldAttempt {
                attempt_id: test_content_id("attempt:invariants"),
                continuation_epoch: 0,
                execution_fence: 1,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

fn open_child_scope_fixture(
    machine: &mut Machine,
    run_id: &str,
    root_invocation: &str,
) -> (String, String) {
    let child_scope = cymule_core::plan_scope_id(
        run_id,
        &machine.projection().runs[run_id].current_plan,
        root_invocation,
        "main",
        &[1],
    )
    .expect("child scope hashes");
    let sibling_scope = cymule_core::plan_scope_id(
        run_id,
        &machine.projection().runs[run_id].current_plan,
        root_invocation,
        "main",
        &[2],
    )
    .expect("sibling scope hashes");
    machine
        .submit(envelope(
            machine,
            8,
            run_id,
            Command::OpenScope {
                scope_id: child_scope.clone(),
                parent_scope: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: root_invocation.to_owned(),
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "scope.child".to_owned(),
            },
        ))
        .expect("child scope opens");
    let scope_open = machine.events().last().expect("scope open event");
    assert_eq!(
        scope_open.coordination_key.as_deref(),
        Some("scope-tree:run:invariants")
    );
    assert!(
        scope_open
            .writes
            .contains("scope:run:invariants:scope:root")
    );
    assert!(scope_open.writes.contains("scope-tree:run:invariants"));
    (child_scope, sibling_scope)
}

fn assert_open_scope_parent_fences(
    machine: &mut Machine,
    run_id: &str,
    root_invocation: &str,
    sibling_scope: &str,
) {
    for (sequence, command) in [
        (
            80,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ),
        (
            81,
            Command::AbortScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ),
    ] {
        assert!(matches!(
            machine.submit(envelope(machine, sequence, run_id, command)),
            Err(CoreError::IllegalTransition(message))
                if message.contains("open child scope")
        ));
    }
    machine
        .submit(envelope(
            machine,
            9,
            run_id,
            Command::OpenScope {
                scope_id: sibling_scope.to_owned(),
                parent_scope: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: root_invocation.to_owned(),
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "scope.sibling".to_owned(),
            },
        ))
        .expect("sibling scope opens on the ordered Run frontier");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            10,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(message)) if message.contains("open child")
    ));
}

fn assert_scope_closure_order(
    machine: &mut Machine,
    run_id: &str,
    root_invocation: String,
    child_scope: String,
    sibling_scope: String,
) {
    machine
        .submit(envelope(
            machine,
            11,
            run_id,
            Command::CommitScope {
                scope_id: child_scope.clone(),
            },
        ))
        .expect("child scope commits");
    assert_eq!(
        machine
            .events()
            .last()
            .expect("scope commit event")
            .coordination_key
            .as_deref(),
        Some("scope-tree:run:invariants")
    );
    assert!(matches!(
        machine.submit(envelope(
            machine,
            12,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(message)) if message.contains("open child")
    ));
    machine
        .submit(envelope(
            machine,
            13,
            run_id,
            Command::AbortScope {
                scope_id: sibling_scope,
            },
        ))
        .expect("sibling scope aborts");
    machine
        .submit(envelope(
            machine,
            14,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("parent closes after every sibling closes");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            15,
            run_id,
            Command::OpenScope {
                scope_id: "scope:grandchild".to_owned(),
                parent_scope: child_scope.clone(),
                invocation_id: root_invocation,
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "scope.child".to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            16,
            run_id,
            Command::AbortScope {
                scope_id: child_scope,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

#[test]
fn machine_snapshot_requires_bidirectional_event_receipt_hash_closure() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "snapshot-closure");
    machine
        .submit(envelope(
            &machine,
            1,
            "run:snapshot-closure",
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("run starts");
    let snapshot = machine.snapshot();

    let mut missing_receipt = serde_json::to_value(&snapshot).expect("snapshot encodes");
    missing_receipt["commands"] = json!({});
    let missing_receipt = serde_json::from_value(missing_receipt).expect("shape decodes");
    assert!(matches!(
        Machine::restore(missing_receipt),
        Err(CoreError::NotFound(_))
    ));

    let mut mismatched_hash = serde_json::to_value(&snapshot).expect("snapshot encodes");
    mismatched_hash["commands"]["command:1"]["semantic_hash"] = json!("different");
    let mismatched_hash = serde_json::from_value(mismatched_hash).expect("shape decodes");
    assert!(matches!(
        Machine::restore(mismatched_hash),
        Err(CoreError::IdentityMismatch(_))
    ));

    let mut duplicate_receipt = serde_json::to_value(&snapshot).expect("snapshot encodes");
    let mut second = duplicate_receipt["commands"]["command:1"].clone();
    second["receipt"]["command_id"] = json!("command:2");
    second["envelope"]["command_id"] = json!("command:2");
    duplicate_receipt["commands"]["command:2"] = second;
    let duplicate_receipt = serde_json::from_value(duplicate_receipt).expect("shape decodes");
    assert!(matches!(
        Machine::restore(duplicate_receipt),
        Err(CoreError::IdentityMismatch(_))
    ));

    let mut applied_with_error = serde_json::to_value(&snapshot).expect("snapshot encodes");
    applied_with_error["commands"]["command:1"]["receipt"]["error_code"] =
        json!("impossible_error");
    let applied_with_error = serde_json::from_value(applied_with_error).expect("shape decodes");
    assert!(matches!(
        Machine::restore(applied_with_error),
        Err(CoreError::IdentityMismatch(_))
    ));

    let mut untyped_conflict = serde_json::to_value(&snapshot).expect("snapshot encodes");
    untyped_conflict["commands"]["command:1"]["receipt"]["status"] = json!("conflict");
    untyped_conflict["commands"]["command:1"]["receipt"]["event_ids"] = json!([]);
    let untyped_conflict = serde_json::from_value(untyped_conflict).expect("shape decodes");
    assert!(matches!(
        Machine::restore(untyped_conflict),
        Err(CoreError::IdentityMismatch(_))
    ));
}

#[test]
fn machine_snapshot_restores_projection_artifacts_and_command_deduplication() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "snapshot");
    let artifact = binding.clone();
    let start = envelope(
        &machine,
        1,
        "run:snapshot",
        Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: binding.artifact_id,
            input: test_run_input_ref(),
            material_digest: String::new(),
            initial_attempt: placeholder_initial_attempt(),
        },
    );
    let receipt = machine.submit(start.clone()).expect("run starts");
    let staged = machine
        .put_artifact("example/state", b"durable".to_vec())
        .expect("Artifact stores");
    let snapshot = machine.snapshot();
    let snapshot_digest = snapshot.digest().expect("snapshot hashes");
    let command_digests = snapshot.command_digests().expect("commands hash");
    assert_eq!(command_digests.len(), 1);
    assert!(command_digests.contains_key("command:1"));

    let mut restored = Machine::restore(snapshot).expect("snapshot restores");
    assert_eq!(
        restored.projection().digest().expect("projection hashes"),
        machine.projection().digest().expect("projection hashes")
    );
    assert_eq!(
        restored
            .artifact(&artifact)
            .expect("artifact restores")
            .bytes,
        b"snapshot"
    );
    assert!(
        restored.artifact(&staged).is_none(),
        "unadmitted material is not restored"
    );
    assert_eq!(
        restored.submit(start).expect("command retry restores"),
        receipt
    );
    assert_eq!(
        restored.snapshot().digest().expect("snapshot hashes"),
        snapshot_digest
    );
    assert_eq!(
        restored
            .snapshot()
            .command_digests()
            .expect("restored commands hash"),
        command_digests
    );
    restored
        .put_artifact("example/state", b"changed".to_vec())
        .expect("Artifact stores");
    assert_eq!(
        restored
            .snapshot()
            .digest()
            .expect("staging leaves the snapshot unchanged"),
        snapshot_digest
    );
}

#[test]
fn compacted_machine_base_rehydrates_suffix_and_command_receipts() {
    let (mut machine, run_id, start, start_receipt) = compaction_machine_fixture();
    assert_epoch_admission_and_replay(&mut machine, run_id);
    let expected_projection = machine.projection().clone();
    assert_legacy_snapshot_generations(&machine);
    let before_compaction = machine.snapshot();
    let compaction = machine.compact_event_history(2).expect("prefix compacts");
    let first_segment = compaction.archive_segment.clone();
    assert_eq!(compaction.compacted_events, 4);
    assert_eq!(compaction.retained_events, 2);
    assert_eq!(first_segment.header.result_count, 3);
    assert_eq!(first_segment.header.result_event_count, 4);
    let snapshot = machine.snapshot();
    assert_archive_compaction_delta(&before_compaction, &snapshot, &first_segment);
    let mut restored = Machine::restore_with_archive(snapshot.clone(), [first_segment.clone()])
        .expect("raw archive audit restores");
    assert_eq!(restored.projection(), &expected_projection);
    let (proof, first_index_proof) =
        assert_archived_start_replay(&mut restored, &start, &start_receipt, &first_segment);
    assert_archive_index_nonmembership(&restored, run_id, &first_segment, &first_index_proof);
    assert_archive_proof_wire_rejections(
        &restored,
        &first_segment,
        &start,
        &proof,
        &first_index_proof,
    );
    let second = restored
        .compact_event_history(1)
        .expect("later hot Event compacts");
    let second_segment = second.archive_segment.clone();
    assert_eq!(second.compacted_events, 5);
    assert_eq!(second.retained_events, 1);
    assert!(
        restored
            .replay_archived_command(&start, &proof, &first_index_proof)
            .is_err()
    );
    let current_index_proof = first_segment
        .command_index_proof(0, std::slice::from_ref(&second_segment))
        .expect("current archive index proof derives");
    assert_eq!(
        restored
            .replay_archived_command(&start, &proof, &current_index_proof)
            .expect("descendant header reaches current archive head"),
        start_receipt
    );
    Machine::restore_with_archive(
        restored.snapshot(),
        [first_segment.clone(), second_segment.clone()],
    )
    .expect("twice-compacted raw archive restores");

    let mut malformed_segment = first_segment;
    malformed_segment.entries[0].command.semantic_hash = "0".repeat(64);
    assert!(malformed_segment.verify().is_err());
}

fn compaction_machine_fixture() -> (
    Machine,
    &'static str,
    CommandEnvelope,
    cymule_core::CommandReceipt,
) {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "compacted-snapshot");
    let run_id = "run:compacted-snapshot";
    let start = envelope(
        &machine,
        1,
        run_id,
        Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: binding.artifact_id,
            input: test_run_input_ref(),
            material_digest: String::new(),
            initial_attempt: placeholder_initial_attempt(),
        },
    );
    let start_receipt = machine.submit(start.clone()).expect("run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("initial Attempt yields before the compaction-specific Attempt");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::BeginAttempt {
                attempt_id: test_content_id("attempt:compaction:1"),
                continuation_id: test_content_id("continuation:compaction"),
                occurrence_binding: test_content_id("binding:worker/1"),
                continuation_epoch: 0,
                execution_fence: 1,
            },
        ))
        .expect("attempt starts");
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::YieldAttempt {
                attempt_id: test_content_id("attempt:compaction:1"),
                continuation_epoch: 0,
                execution_fence: 1,
            },
        ))
        .expect("attempt yields");
    (machine, run_id, start, start_receipt)
}

fn assert_epoch_admission_and_replay(machine: &mut Machine, run_id: &str) {
    let before_epoch = machine.snapshot();
    let epoch_command = envelope(machine, 4, run_id, Command::AdvanceEpoch);
    let receipt = machine
        .submit(epoch_command.clone())
        .expect("epoch advances");
    let epoch_snapshot = machine.snapshot();
    assert_eq!(epoch_snapshot.events.len(), before_epoch.events.len() + 1);
    assert_eq!(epoch_snapshot.plans, before_epoch.plans);
    assert_eq!(epoch_snapshot.artifacts, before_epoch.artifacts);
    let mut restored = Machine::restore(epoch_snapshot.clone()).expect("epoch result restores");
    assert_eq!(restored.projection(), machine.projection());
    assert_eq!(
        restored
            .submit(epoch_command)
            .expect("epoch receipt replays"),
        receipt
    );
    assert_eq!(restored.snapshot(), epoch_snapshot);
}

fn assert_legacy_snapshot_generations(machine: &Machine) {
    for old_version in [
        "cymule.machine-snapshot/1",
        "cymule.machine-snapshot/2",
        "cymule.machine-snapshot/3",
        "cymule.machine-snapshot/4",
        "cymule.machine-snapshot/5",
        "cymule.machine-snapshot/6",
        "cymule.machine-snapshot/7",
    ] {
        let mut old_snapshot = machine.snapshot();
        old_version.clone_into(&mut old_snapshot.snapshot_version);
        assert!(matches!(
            Machine::restore(old_snapshot),
            Err(CoreError::Validation(_))
        ));
    }
    let mut unsupported = machine.snapshot();
    "cymule.machine-snapshot/999".clone_into(&mut unsupported.snapshot_version);
    assert!(matches!(
        Machine::restore(unsupported),
        Err(CoreError::Validation(_))
    ));
    let mut legacy_event = machine.snapshot();
    "cymule.event/5".clone_into(&mut legacy_event.events[0].event_version);
    assert!(matches!(
        Machine::restore(legacy_event),
        Err(CoreError::Validation(_) | CoreError::IdentityMismatch(_))
    ));
}

fn assert_archive_compaction_delta(
    before_compaction: &cymule_core::MachineSnapshot,
    snapshot: &cymule_core::MachineSnapshot,
    first_segment: &cymule_core::MachineCommandArchiveSegment,
) {
    let delta =
        cymule_core::MachineDelta::between_compaction(before_compaction, snapshot, first_segment)
            .expect("compaction delta binds its archive segment");
    let mut wrong_parent = delta.clone();
    wrong_parent.parent_snapshot_digest = "0".repeat(64);
    let mut rejected_parent = before_compaction.clone();
    let parent_bytes = cymule_core::canonical_bytes(&rejected_parent).expect("parent encodes");
    assert!(
        rejected_parent
            .apply_compaction_delta(&wrong_parent, first_segment)
            .is_err()
    );
    assert_eq!(
        cymule_core::canonical_bytes(&rejected_parent).expect("rejected parent encodes"),
        parent_bytes
    );
    let mut wrong_result = delta.clone();
    wrong_result.result_snapshot_digest = "0".repeat(64);
    let mut rejected_result = before_compaction.clone();
    assert!(
        rejected_result
            .apply_compaction_delta(&wrong_result, first_segment)
            .is_err()
    );
    assert_eq!(rejected_result, *before_compaction);
    let mut short_cut = delta.clone();
    short_cut.compacted_event_ids.pop();
    let mut rejected_cut = before_compaction.clone();
    assert!(
        rejected_cut
            .apply_compaction_delta(&short_cut, first_segment)
            .is_err()
    );
    assert_eq!(rejected_cut, *before_compaction);
    let mut incremental = Machine::restore(before_compaction.clone()).expect("parent restores");
    incremental
        .apply_compaction_delta(&delta, first_segment)
        .expect("Machine applies exact archive compaction");
    assert_eq!(incremental.snapshot(), *snapshot);
    let mut assembled = before_compaction.clone();
    assembled
        .apply_compaction_delta(&delta, first_segment)
        .expect("Snapshot applies exact archive compaction");
    assert_eq!(assembled, *snapshot);
    assert!(matches!(
        Machine::restore(snapshot.clone()),
        Err(CoreError::ArchivedCommandReplayRequired { .. })
    ));
}

fn assert_archived_start_replay(
    restored: &mut Machine,
    start: &CommandEnvelope,
    start_receipt: &cymule_core::CommandReceipt,
    first_segment: &cymule_core::MachineCommandArchiveSegment,
) -> (
    cymule_core::MachineArchivedCommandProof,
    cymule_core::MachineCommandIndexProof,
) {
    assert!(matches!(
        restored.replay_command(start),
        Err(CoreError::ArchivedCommandReplayRequired { .. })
    ));
    let proof = first_segment
        .command_proof(0)
        .expect("archived start proof derives");
    let first_index_proof = first_segment
        .command_index_proof(0, &[])
        .expect("archived start index proof derives");
    assert_eq!(
        restored
            .replay_archived_command(start, &proof, &first_index_proof)
            .expect("proof replays archived start"),
        *start_receipt
    );
    assert!(matches!(
        restored.command_receipt(&start.command_id),
        Err(CoreError::ArchivedCommandReplayRequired { .. })
    ));
    let before_lost_ack = restored.snapshot();
    assert!(matches!(
        restored.submit(start.clone()),
        Err(CoreError::ArchivedCommandReplayRequired { .. })
    ));
    assert_eq!(restored.snapshot(), before_lost_ack);
    assert_eq!(
        restored
            .submit_with_archive_lookup(
                start.clone(),
                cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof: first_index_proof.clone(),
                    entry: Box::new(first_segment.entries[0].clone()),
                },
            )
            .expect("membership proof resolves lost acknowledgment"),
        *start_receipt
    );
    assert_eq!(restored.snapshot(), before_lost_ack);
    let mut reused = start.clone();
    "actor:different-semantics".clone_into(&mut reused.actor);
    assert!(matches!(
        restored.submit_with_archive_lookup(
            reused,
            cymule_core::MachineCommandArchiveLookup::Member {
                index_proof: first_index_proof.clone(),
                entry: Box::new(first_segment.entries[0].clone()),
            },
        ),
        Err(CoreError::CommandReuse(_))
    ));
    assert_eq!(restored.snapshot(), before_lost_ack);
    let mut wrong_entry = restored.clone();
    assert!(
        wrong_entry
            .submit_with_archive_lookup(
                start.clone(),
                cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof: first_index_proof.clone(),
                    entry: Box::new(first_segment.entries[1].clone()),
                },
            )
            .is_err()
    );
    assert_eq!(wrong_entry.snapshot(), before_lost_ack);
    (proof, first_index_proof)
}

fn assert_archive_index_nonmembership(
    restored: &Machine,
    run_id: &str,
    first_segment: &cymule_core::MachineCommandArchiveSegment,
    first_index_proof: &cymule_core::MachineCommandIndexProof,
) {
    let new_envelope = envelope(
        restored,
        99,
        run_id,
        Command::RecordFact {
            key: "fact:smt-nonmembership".to_owned(),
            value: test_fact_value("accepted"),
        },
    );
    let nodes = first_segment
        .command_index_nodes()
        .expect("segment index nodes materialize")
        .into_iter()
        .map(|node| (node.identity().expect("node verifies").to_owned(), node))
        .collect::<BTreeMap<_, _>>();
    let absent = cymule_core::resolve_machine_command_index_proof(
        &restored
            .base_anchor()
            .expect("anchor reads")
            .expect("base exists")
            .command_index_root,
        &new_envelope.command_id,
        |node_id| Ok(nodes.get(node_id).cloned()),
    )
    .expect("new command non-membership resolves");
    assert!(absent.value.is_none());
    let mut false_absence = restored.clone();
    assert!(
        false_absence
            .submit_with_archive_lookup(
                new_envelope.clone(),
                cymule_core::MachineCommandArchiveLookup::NonMember {
                    index_proof: first_index_proof.clone(),
                },
            )
            .is_err()
    );
    let mut false_membership = restored.clone();
    assert!(
        false_membership
            .submit_with_archive_lookup(
                new_envelope.clone(),
                cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof: absent.clone(),
                    entry: Box::new(first_segment.entries[0].clone()),
                },
            )
            .is_err()
    );
    let mut wrong_length = absent.clone();
    wrong_length.siblings.pop();
    let mut rejected = restored.clone();
    assert!(
        rejected
            .submit_with_archive_lookup(
                new_envelope.clone(),
                cymule_core::MachineCommandArchiveLookup::NonMember {
                    index_proof: wrong_length,
                },
            )
            .is_err()
    );
    assert_eq!(rejected.snapshot(), restored.snapshot());
    let mut admitted = restored.clone();
    assert_eq!(
        admitted
            .submit_with_archive_lookup(
                new_envelope,
                cymule_core::MachineCommandArchiveLookup::NonMember {
                    index_proof: absent,
                },
            )
            .expect("current-root non-membership admits new ID")
            .status,
        CommandReceiptStatus::Applied
    );
}

fn assert_archive_proof_wire_rejections(
    restored: &Machine,
    first_segment: &cymule_core::MachineCommandArchiveSegment,
    start: &CommandEnvelope,
    proof: &cymule_core::MachineArchivedCommandProof,
    first_index_proof: &cymule_core::MachineCommandIndexProof,
) {
    let persisted = first_segment
        .persistence_objects()
        .expect("segment expands into immutable archive objects");
    assert!(persisted.len() > first_segment.entries.len());
    for object in &persisted {
        object.identity().expect("archive object identity verifies");
        let mut wire = serde_json::to_value(object).expect("archive object serializes");
        wire.as_object_mut()
            .expect("archive union is an object")
            .insert("unexpected".to_owned(), json!(true));
        assert!(
            serde_json::from_value::<cymule_core::MachineCommandArchiveObject>(wire).is_err(),
            "archive outer union must reject unknown fields"
        );
    }
    let mut redundant_empty =
        cymule_core::MachineCommandIndexProof::empty_nonmembership("command:redundant-empty-proof")
            .expect("empty proof derives");
    redundant_empty.empty_depth = Some(1);
    redundant_empty.siblings = vec![
        cymule_core::MachineCommandIndexProof::empty_hash(1)
            .expect("canonical empty sibling derives"),
    ];
    assert!(
        redundant_empty
            .verify(
                &cymule_core::MachineCommandIndexProof::empty_root().expect("empty root derives")
            )
            .is_err(),
        "non-membership proofs must use maximal empty-depth compression"
    );
    let mut malformed_index_segment = first_segment.clone();
    malformed_index_segment.command_index_updates[0].empty_depth = Some(1);
    assert!(malformed_index_segment.verify().is_err());
    let mut wrong_position = proof.clone();
    wrong_position.entry_index = 1;
    assert!(
        restored
            .replay_archived_command(start, &wrong_position, first_index_proof)
            .is_err()
    );
}

#[test]
fn structural_effect_identifiers_are_content_sensitive() {
    let args = ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: format!("sha256:{}", "a".repeat(64)),
        kind: "cymule.effect-args/1".to_owned(),
    };
    let first = effect_intent_id(&EffectIntentIdentityInput {
        run_id: "run:id",
        plan_id: "plan:first",
        invocation_id: "main",
        site_id: "effect.capture",
        scope_id: cymule_core::ROOT_SCOPE_ID,
        occurrence: "primary",
        args: &args,
        effect_schema_version: "cymule.effect-schema/1",
    })
    .expect("intent hashes");
    let second = effect_intent_id(&EffectIntentIdentityInput {
        run_id: "run:id",
        plan_id: "plan:first",
        invocation_id: "main",
        site_id: "effect.capture",
        scope_id: cymule_core::ROOT_SCOPE_ID,
        occurrence: "secondary",
        args: &args,
        effect_schema_version: "cymule.effect-schema/1",
    })
    .expect("changed intent hashes");
    assert!(first.starts_with("sha256:"));
    assert_eq!(first.len(), 71);
    assert_ne!(first, second);

    let obligation = effect_obligation_id(&first).expect("obligation hashes");
    let other = effect_obligation_id(&second).expect("changed obligation hashes");
    assert!(obligation.starts_with("sha256:"));
    assert_eq!(obligation.len(), 71);
    assert_ne!(obligation, other);
}

#[test]
fn binding_is_pinned_and_unknown_effect_must_reconcile() {
    let (mut machine, run_id, intent_id) = binding_reconciliation_machine();
    assert_effect_prepare_gate(&mut machine, run_id, &intent_id);
    assert_scope_commit_obligation_authority(&mut machine, run_id, &intent_id);
    assert_effect_dispatch_gate(&mut machine, run_id, &intent_id);
    assert_unknown_effect_reconciliation_gate(&mut machine, run_id, &intent_id);
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            0,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("world-settlement probe explicitly yields its initial Attempt");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            9,
            run_id,
            Command::CompleteRun { result: None },
        )),
        Err(CoreError::IllegalTransition(message)) if message.contains("unresolved blocking effect obligations")
    ));
    machine
        .submit(envelope(
            &machine,
            10,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied),
            },
        ))
        .expect("unknown result reconciles");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            100,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied,),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            &machine,
            11,
            run_id,
            Command::CompleteRun { result: None },
        ))
        .expect("settled run completes");
    machine.verify_replay().expect("projection replays exactly");
}

fn binding_reconciliation_machine() -> (Machine, &'static str, String) {
    let mut machine = Machine::new();
    let mut effect_plan = candidate();
    effect_plan.definitions[0].body.steps.push(Step {
        id: "effect.capture".to_owned(),
        operation: Operation::Effect {
            effect: "test.capture".to_owned(),
            input: Expression::Input,
            occurrence: "primary".to_owned(),
            bind: None,
        },
    });
    let plan = insert_plan(&mut machine, effect_plan);
    let run_id = "run:effect";
    let original_binding = test_execution_binding(&mut machine, "effect-default-v1");
    let updated_binding = test_execution_binding(&mut machine, "effect-default-v2");
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: original_binding.artifact_id.clone(),
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("run starts");
    let root_invocation = machine.projection().runs[run_id].scopes[cymule_core::ROOT_SCOPE_ID]
        .invocation_id
        .clone();
    let args = machine
        .put_artifact("cymule.effect-args/1", br#"{"value":1}"#.to_vec())
        .expect("Artifact stores");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: root_invocation,
                invocation_path: Vec::new(),
                definition_id: "main".to_owned(),
                region_path: Vec::new(),
                site_id: "effect.capture".to_owned(),
                occurrence: "primary".to_owned(),
                operation: "test.capture".to_owned(),
                args,
                execution_binding: original_binding.clone(),
                occurrence_binding: test_content_id("binding:adapter/v1"),
            },
        ))
        .expect("effect is proposed");
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::UpdateBinding {
                binding_context: updated_binding.artifact_id,
            },
        ))
        .expect("future default changes");

    let intent_id = machine
        .projection()
        .runs
        .get(run_id)
        .expect("run exists")
        .effects
        .keys()
        .next()
        .expect("effect exists")
        .clone();
    (machine, run_id, intent_id)
}

fn assert_effect_prepare_gate(machine: &mut Machine, run_id: &str, intent_id: &str) {
    assert!(matches!(
        machine.submit(envelope(
            machine,
            40,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::StartDispatch,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            41,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Observe(WorldOutcome::Applied),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            42,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied,),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            43,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

fn assert_scope_commit_obligation_authority(machine: &mut Machine, run_id: &str, intent_id: &str) {
    machine
        .submit(envelope(
            machine,
            4,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Prepare,
            },
        ))
        .expect("effect prepares");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            44,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Prepare,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            46,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            machine,
            8,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("scope commits and transfers the unresolved obligation");
    let committed = machine
        .events()
        .last()
        .expect("admitted Scope commit exists");
    let inexact_commit = event_with_payload(
        committed,
        EventPayload::ScopeCommitted {
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            obligation_count: 0,
            obligation_commitment: format!("sha256:{}", "0".repeat(64)),
        },
    );
    let inexact_history = machine.events().map(|event| {
        if event.event_id == committed.event_id {
            inexact_commit.clone()
        } else {
            event.clone()
        }
    });
    assert!(matches!(
        replay_with_machine_authority(machine, inexact_history),
        Err(CoreError::IllegalTransition(_) | CoreError::IdentityMismatch(_))
    ));
}

fn assert_effect_dispatch_gate(machine: &mut Machine, run_id: &str, intent_id: &str) {
    machine
        .submit(envelope(
            machine,
            5,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::AuthorizeRelease,
            },
        ))
        .expect("release authorizes");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            45,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            machine,
            6,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::StartDispatch,
            },
        ))
        .expect("dispatch starts");
    assert!(matches!(
        machine.submit(envelope(
            machine,
            60,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Observe(WorldOutcome::Unobserved),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            machine,
            61,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied,),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

fn assert_unknown_effect_reconciliation_gate(machine: &mut Machine, run_id: &str, intent_id: &str) {
    machine
        .submit(envelope(
            machine,
            7,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Observe(WorldOutcome::Unknown),
            },
        ))
        .expect("unknown observation applies");
    for (sequence, outcome) in [(70, WorldOutcome::Applied), (71, WorldOutcome::Unknown)] {
        assert!(matches!(
            machine.submit(envelope(
                machine,
                sequence,
                run_id,
                Command::TransitionEffect {
                    intent_id: intent_id.to_owned(),
                    transition: EffectTransition::Observe(outcome),
                },
            )),
            Err(CoreError::IllegalTransition(_))
        ));
    }
    assert!(matches!(
        machine.submit(envelope(
            machine,
            72,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.to_owned(),
                transition: EffectTransition::Reconcile(
                    ReconciliationResolution::GovernanceRequired,
                ),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    let run = machine.projection().runs.get(run_id).expect("run exists");
    let effect = run.effects.get(intent_id).expect("effect exists");
    assert_eq!(
        effect.occurrence_binding,
        test_content_id("binding:adapter/v1")
    );
    assert_eq!(effect.phase, EffectPhase::DispatchStarted);
    assert_eq!(effect.outcome, WorldOutcome::Unknown);
    assert_eq!(effect.reconciliation, ReconciliationState::Pending);
    assert_eq!(
        run.scopes[cymule_core::ROOT_SCOPE_ID].status,
        ScopeStatus::ClosedCommitted
    );
    assert!(
        run.obligations
            .values()
            .any(|obligation| !obligation.resolved)
    );
    assert!(matches!(
        machine.submit(envelope(
            machine,
            80,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

#[test]
fn epoch_fences_prior_attempts() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "fence");
    let run_id = "run:fence";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(&machine, 3, run_id, Command::AdvanceEpoch))
        .expect("epoch advances");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            4,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

#[test]
fn run_completion_requires_every_attempt_to_be_inactive() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "completion-fence");
    let run_id = "run:completion-fence";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    let initial = initial_attempt(&machine, run_id);
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("root scope commits");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            4,
            run_id,
            Command::CompleteRun { result: None },
        )),
        Err(CoreError::IllegalTransition(message))
            if message.contains("Attempt remains active")
    ));
    machine
        .submit(envelope(
            &machine,
            5,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial.attempt_id,
                continuation_epoch: initial.continuation_epoch,
                execution_fence: initial.execution_fence,
            },
        ))
        .expect("Attempt yields");
    machine
        .submit(envelope(
            &machine,
            6,
            run_id,
            Command::CompleteRun { result: None },
        ))
        .expect("inactive Run completes");
    let run = &machine.projection().runs[run_id];
    assert_eq!(
        run.execution_status,
        cymule_core::RunExecutionStatus::Completed
    );
    assert!(run.attempts.values().all(|attempt| !attempt.active));
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            7,
            run_id,
            Command::BeginAttempt {
                attempt_id: test_content_id("attempt:late"),
                continuation_id: test_content_id("continuation:late"),
                occurrence_binding: test_content_id("binding:v1"),
                continuation_epoch: 0,
                execution_fence: 2,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

#[test]
fn start_run_replay_requires_atomic_initial_attempt_and_batch_closure() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "start-replay-closure");
    let receipt = machine
        .submit(envelope(
            &machine,
            1,
            "run:start-replay-closure",
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("StartRun admits its complete pair");
    let snapshot = machine.snapshot();
    let entries = machine.replay_entries().expect("StartRun closure exports");
    let [entry] = entries.as_slice() else {
        panic!("StartRun owns one command admission");
    };
    let [started, attempted] = entry.events.as_slice() else {
        panic!("StartRun owns exactly two Events");
    };
    assert!(matches!(started.payload, EventPayload::RunStarted { .. }));
    assert!(matches!(
        attempted.payload,
        EventPayload::AttemptStarted { .. }
    ));
    assert_eq!(started.command_id, attempted.command_id);
    assert_eq!(
        receipt.event_ids,
        [started.event_id.clone(), attempted.event_id.clone()]
    );
    assert_eq!(
        Machine::replay(
            snapshot.plans.clone(),
            snapshot.artifacts.clone(),
            snapshot.batches.clone(),
            entries.clone(),
        )
        .expect("complete StartRun batch replays"),
        *machine.projection()
    );
    for removed in 0..2 {
        let mut partial = entry.clone();
        partial.events.remove(removed);
        assert!(
            Machine::replay(
                snapshot.plans.clone(),
                snapshot.artifacts.clone(),
                snapshot.batches.clone(),
                [partial],
            )
            .is_err()
        );
    }
    assert!(
        Machine::replay(
            snapshot.plans.clone(),
            snapshot.artifacts.clone(),
            Vec::<cymule_core::MachineCommandBatchRecord>::new(),
            entries.clone(),
        )
        .is_err()
    );
    assert!(
        Machine::replay(
            snapshot.plans,
            snapshot.artifacts,
            snapshot.batches,
            [entry.clone(), entry.clone()],
        )
        .is_err()
    );
    assert!(matches!(
        replay_with_machine_authority(&machine, [started.clone(), started.clone()]),
        Err(CoreError::Validation(message)) if message.contains("repeats an Event")
    ));
    let mut unknown = started.clone();
    unknown.command_id = "command:unadmitted".to_owned();
    assert!(matches!(
        replay_with_machine_authority(&machine, [unknown]),
        Err(CoreError::NotFound(message)) if message.contains("no command admission")
    ));
}

#[test]
fn replay_requires_exact_command_admission_closure_and_reports_retention_loss() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "replay");
    let run_id = "run:replay";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("run starts");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::RecordFact {
                key: "a".to_owned(),
                value: test_fact_value("replay-one"),
            },
        ))
        .expect("first fact records");
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::RecordFact {
                key: "b".to_owned(),
                value: test_fact_value("replay-two"),
            },
        ))
        .expect("second fact records");
    let events = machine.events().cloned().collect::<Vec<_>>();
    let [start, attempted, fact_a, fact_b] = events.as_slice() else {
        panic!("fixture has the StartRun pair followed by two fact Events");
    };
    assert!(matches!(
        attempted.payload,
        EventPayload::AttemptStarted { .. }
    ));
    assert_eq!(start.command_id, attempted.command_id);
    assert_replay_event_validation(&machine, start, fact_a, fact_b);
    let left = replay_with_machine_authority(
        &machine,
        vec![fact_a.clone(), start.clone(), fact_b.clone()],
    )
    .expect("causal set replays");
    let right = replay_with_machine_authority(
        &machine,
        vec![fact_b.clone(), fact_a.clone(), start.clone()],
    )
    .expect("order is irrelevant");
    assert_eq!(
        left.digest().expect("digest"),
        right.digest().expect("digest")
    );

    let artifact = machine
        .put_artifact("test/value", b"retained".to_vec())
        .expect("Artifact stores");
    assert!(matches!(
        machine.replay_availability(std::slice::from_ref(&artifact)),
        ReplayAvailability::ProjectionOnly { missing } if missing == [artifact.artifact_id]
    ));
    assert_eq!(
        machine.replay_availability(&[test_run_input_ref()]),
        ReplayAvailability::Exact
    );
    let missing = cymule_core::artifact_ref("test/value", b"not-retained")
        .expect("missing Artifact reference derives");
    assert!(matches!(
        machine.replay_availability(std::slice::from_ref(&missing)),
        ReplayAvailability::ProjectionOnly { .. }
    ));
    machine
        .compact_event_history(0)
        .expect("complete hot Event history compacts into a base");
    assert_eq!(machine.events().count(), 0);
    assert!(machine.base_anchor().expect("base reads").is_some());
    assert!(matches!(
        machine.replay_availability(std::slice::from_ref(&missing)),
        ReplayAvailability::ProjectionOnly { .. }
    ));
}

fn assert_replay_event_validation(
    machine: &Machine,
    start: &Event,
    fact_a: &Event,
    fact_b: &Event,
) {
    assert!(replay_with_machine_authority(machine, [start.clone(), start.clone()]).is_err());
    let mut tampered = fact_a.clone();
    tampered.event_id = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        tampered.verify(),
        Err(CoreError::IdentityMismatch(_))
    ));
    let mut duplicate_parent = fact_a.clone();
    duplicate_parent.parents.push(fact_a.parents[0].clone());
    duplicate_parent.parents.sort();
    assert!(matches!(
        duplicate_parent.verify(),
        Err(CoreError::Validation(message)) if message.contains("duplicate-free")
    ));
    let false_footprint = Event::new(EventContent {
        command_id: fact_b.command_id.clone(),
        command_hash: fact_b.command_hash.clone(),
        run_id: fact_b.run_id.clone(),
        parents: fact_b.parents.clone(),
        reads: BTreeSet::new(),
        writes: BTreeSet::new(),
        coordination_key: None,
        payload: EventPayload::FactRecorded {
            key: "false-footprint".to_owned(),
            value: test_fact_value("false-footprint-one"),
        },
    })
    .expect("self-consistent but semantically false Event constructs");
    assert!(matches!(
        replay_with_machine_authority(machine, [start.clone(), fact_a.clone(), false_footprint]),
        Err(CoreError::IdentityMismatch(_))
    ));
    let missing_parent = Event::new(EventContent {
        command_id: fact_b.command_id.clone(),
        command_hash: fact_b.command_hash.clone(),
        run_id: fact_b.run_id.clone(),
        parents: vec![format!("sha256:{}", "f".repeat(64))],
        reads: fact_b.reads.clone(),
        writes: fact_b.writes.clone(),
        coordination_key: fact_b.coordination_key.clone(),
        payload: fact_b.payload.clone(),
    })
    .expect("event hashes");
    assert!(matches!(
        replay_with_machine_authority(machine, [missing_parent]),
        Err(CoreError::IdentityMismatch(_) | CoreError::Causal(_))
    ));
    assert!(
        Event::new(EventContent {
            command_id: fact_a.command_id.clone(),
            command_hash: "hash:forged".to_owned(),
            run_id: fact_a.run_id.clone(),
            parents: fact_a.parents.clone(),
            reads: fact_a.reads.clone(),
            writes: fact_a.writes.clone(),
            coordination_key: fact_a.coordination_key.clone(),
            payload: fact_a.payload.clone(),
        })
        .is_err()
    );
    let forged_hash = Event::new(EventContent {
        command_id: fact_a.command_id.clone(),
        command_hash: "3".repeat(64),
        run_id: fact_a.run_id.clone(),
        parents: fact_a.parents.clone(),
        reads: fact_a.reads.clone(),
        writes: fact_a.writes.clone(),
        coordination_key: fact_a.coordination_key.clone(),
        payload: fact_a.payload.clone(),
    })
    .expect("forged Event remains independently content-addressable");
    assert!(matches!(
        replay_with_machine_authority(machine, [start.clone(), forged_hash]),
        Err(CoreError::IdentityMismatch(_))
    ));
}

#[test]
fn archive_entry_and_replay_reject_a_fully_reauthenticated_event_with_other_command_semantics() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let binding = test_execution_binding(&mut machine, "replay-command-event-closure");
    let run_id = "run:replay-command-event-closure";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input: test_run_input_ref(),
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        ))
        .expect("Run starts");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::RecordFact {
                key: "declared".to_owned(),
                value: test_fact_value("command-value"),
            },
        ))
        .expect("declared fact records");

    let snapshot = machine.snapshot();
    let mut entries = machine.replay_entries().expect("replay closure exports");
    let fact_entry = entries.last_mut().expect("fact admission exists");
    let original_event = fact_entry.events.first().expect("fact Event exists");
    let forged_key = "event-selected";
    let fact_key = format!("fact:{forged_key}");
    let forged_event = Event::new(EventContent {
        command_id: original_event.command_id.clone(),
        command_hash: original_event.command_hash.clone(),
        run_id: original_event.run_id.clone(),
        parents: original_event.parents.clone(),
        reads: BTreeSet::from([fact_key.clone()]),
        writes: BTreeSet::from([fact_key.clone()]),
        coordination_key: Some(fact_key),
        payload: EventPayload::FactRecorded {
            key: forged_key.to_owned(),
            value: test_fact_value("event-value"),
        },
    })
    .expect("forged Event remains independently content-addressed");

    let mut forged_projection =
        serde_json::to_value(machine.projection()).expect("Projection encodes");
    forged_projection["facts"] = json!({"event-selected": test_fact_value("event-value")});
    forged_projection["runs"][run_id]["last_event"] = json!(forged_event.event_id.clone());
    let forged_projection: cymule_core::Projection =
        serde_json::from_value(forged_projection).expect("forged Projection shape decodes");

    fact_entry.command.receipt.event_ids = vec![forged_event.event_id.clone()];
    fact_entry.command.receipt.current_precondition =
        Some(format!("pre:0:{}", forged_event.event_id));
    fact_entry.admission.event_ids = vec![forged_event.event_id.clone()];
    fact_entry.admission.after_projection_digest = forged_projection
        .digest()
        .expect("forged Projection hashes");
    fact_entry.admission.command_record_digest =
        cymule_core::canonical_digest(&fact_entry.command).expect("forged record hashes");
    fact_entry.admission.admission_id = cymule_core::content_id(
        cymule_core::COMMAND_ADMISSION_VERSION,
        &CommandAdmissionPreimageForTest {
            admission_version: cymule_core::COMMAND_ADMISSION_VERSION,
            sequence: fact_entry.admission.sequence,
            parent_admission: &fact_entry.admission.parent_admission,
            command_id: &fact_entry.admission.command_id,
            semantic_hash: &fact_entry.admission.semantic_hash,
            command_record_digest: &fact_entry.admission.command_record_digest,
            batch_id: &fact_entry.admission.batch_id,
            batch_position: fact_entry.admission.batch_position,
            batch_len: fact_entry.admission.batch_len,
            before_projection_digest: &fact_entry.admission.before_projection_digest,
            after_projection_digest: &fact_entry.admission.after_projection_digest,
            status: fact_entry.admission.status,
            event_ids: &fact_entry.admission.event_ids,
        },
    )
    .expect("forged admission identity recomputes");
    fact_entry.events = vec![forged_event];

    assert!(matches!(
        fact_entry.verify(),
        Err(CoreError::IdentityMismatch(message))
            if message.contains("does not match retained Event")
    ));
    assert!(matches!(
        Machine::replay(
            snapshot.plans,
            snapshot.artifacts,
            snapshot.batches,
            entries
        ),
        Err(CoreError::IdentityMismatch(_))
    ));
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/proptest-regressions/semantic_kernel.txt"
        )))),
        ..ProptestConfig::default()
    })]

    #[test]
    fn independent_causal_facts_replay_to_one_digest(
        generated in prop::collection::vec((any::<u64>(), any::<u64>()), 1..32),
        start_position in 0usize..64,
    ) {
        let run_id = "run:causal-property";
        let mut machine = Machine::new();
        let plan = insert_plan(&mut machine, candidate());
        let binding = test_execution_binding(&mut machine, "causal-property");
        machine
            .submit(envelope(
                &machine,
                1,
                run_id,
                Command::StartRun {
                    plan_id: plan.plan_id.clone(),
                    binding_context: binding.artifact_id,
                    input: test_run_input_ref(),
                    material_digest: String::new(),
                    initial_attempt: placeholder_initial_attempt(),
                },
            ))
            .expect("run starts");
        let start_events = machine.events().cloned().collect::<Vec<_>>();
        prop_assert_eq!(start_events.len(), 2);
        for (index, (_, value)) in generated.iter().enumerate() {
            machine
                .submit(envelope(
                    &machine,
                    2 + u64::try_from(index).expect("small property index fits"),
                    run_id,
                    Command::RecordFact {
                        key: format!("property:{index}"),
                        value: test_fact_value(&format!("property:{value}")),
                    },
                ))
                .expect("property fact records through command admission");
        }
        let admitted_facts = machine.events().skip(start_events.len()).cloned().collect::<Vec<_>>();
        let mut facts = generated
            .iter()
            .zip(admitted_facts)
            .enumerate()
            .map(|(index, ((priority, _), event))| (*priority, index, event))
            .collect::<Vec<_>>();

        let mut canonical = start_events.clone();
        canonical.extend(facts.iter().map(|(_, _, event)| event.clone()));
        facts.sort_by_key(|(priority, index, _)| (*priority, *index));
        let mut permuted = facts
            .into_iter()
            .map(|(_, _, event)| event)
            .collect::<Vec<_>>();
        let insertion = start_position.min(permuted.len());
        permuted.splice(insertion..insertion, start_events);

        let expected = replay_with_machine_authority(&machine, canonical)
            .expect("canonical order replays");
        let actual = replay_with_machine_authority(&machine, permuted)
            .expect("permuted order replays");
        prop_assert_eq!(
            actual.digest().expect("actual digest"),
            expected.digest().expect("expected digest")
        );
        prop_assert_eq!(actual.facts.len(), generated.len());
    }
}
