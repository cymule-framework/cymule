//! Fault-oriented durable single-domain contract tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, Definition, Expression, Machine, PlanCandidate,
    Region,
};
use cymule_durable::{
    ComponentOccurrence, Continuation, ContinuationStatus, DurableCoordinator, DurableError,
    EffectDispatch, FrameState, JournalBatch, JournalRecord, MemoryStore, OutboxState,
    WaitCondition, WaitKind, WaitState,
};
use serde_json::json;

fn machine_with_run() -> (Machine, String) {
    let mut machine = Machine::new();
    let plan = machine
        .seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "durable_test".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Literal { value: json!(null) },
                },
            }],
            metadata: BTreeMap::new(),
        })
        .expect("plan seals");
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:start".to_owned(),
            actor: "test".to_owned(),
            run_id: "run:durable".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: "binding:test/1".to_owned(),
            },
        })
        .expect("run starts");
    (machine, plan.plan_id)
}

fn continuation(plan_id: String) -> Continuation {
    Continuation {
        run_id: "run:durable".to_owned(),
        plan_id,
        binding_context: "binding:test/1".to_owned(),
        frames: vec![FrameState {
            invocation_id: "main".to_owned(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: None,
        wait_set: BTreeSet::new(),
        scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Ready,
    }
}

#[test]
fn wait_completion_survives_reopen_and_readies_the_continuation() {
    let (mut machine, plan_id) = machine_with_run();
    let result = machine.put_artifact("example/input", b"accepted".to_vec());
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(continuation(plan_id))
        .expect("continuation persists");
    coordinator
        .register_wait(WaitCondition {
            wait_id: "wait:approval".to_owned(),
            run_id: "run:durable".to_owned(),
            kind: WaitKind::Input {
                correlation: "approval".to_owned(),
                schema: json!({"type": "string"}),
            },
            consume_once: true,
            state: WaitState::Pending,
            result: None,
        })
        .expect("wait registers");
    coordinator
        .complete_wait("wait:approval", result.clone())
        .expect("wait completes");
    coordinator
        .complete_wait("wait:approval", result)
        .expect("completion retry is idempotent");

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let state = reopened.state().expect("state exists");
    assert_eq!(state.waits["wait:approval"].state, WaitState::Completed);
    assert_eq!(
        state.continuations["run:durable"].status,
        ContinuationStatus::Ready
    );
    assert!(state.continuations["run:durable"].wait_set.is_empty());
}

#[test]
fn stale_coordinator_and_stale_dispatch_owner_fail_closed() {
    let (mut machine, plan_id) = machine_with_run();
    let input = machine.put_artifact("example/effect-input", b"payload".to_vec());
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut stale = DurableCoordinator::open(store.clone()).expect("second view opens");
    current
        .put_continuation(continuation(plan_id))
        .expect("current writer commits");
    assert!(matches!(
        stale.persist_machine(&machine),
        Err(DurableError::Conflict { .. })
    ));

    let lease = current
        .acquire_lease("dispatch:partition/0", "worker:a", 10, 20)
        .expect("lease acquired");
    assert!(matches!(
        current.acquire_lease("dispatch:partition/0", "worker:b", 11, 20),
        Err(DurableError::Conflict { .. })
    ));
    current
        .enqueue_effect(EffectDispatch {
            intent_id: "intent:1".to_owned(),
            run_id: "run:durable".to_owned(),
            operation: "example.effect".to_owned(),
            input,
            occurrence_binding: "binding:effect/1".to_owned(),
            state: OutboxState::Pending,
            claim_epoch: 0,
            claim_owner: None,
            result: None,
        })
        .expect("effect enqueues");
    current
        .claim_effect("intent:1", "worker:a", lease.epoch)
        .expect("effect claimed");
    assert!(matches!(
        current.settle_effect(
            "intent:1",
            "worker:b",
            lease.epoch,
            OutboxState::Applied,
            None,
        ),
        Err(DurableError::Conflict { .. })
    ));
    current
        .settle_effect(
            "intent:1",
            "worker:a",
            lease.epoch,
            OutboxState::Unknown,
            None,
        )
        .expect("original claim records ambiguity");
    assert_eq!(
        current.state().expect("state").outbox["intent:1"].state,
        OutboxState::Unknown
    );
    current
        .settle_effect(
            "intent:1",
            "worker:a",
            lease.epoch,
            OutboxState::Applied,
            None,
        )
        .expect("the original unknown claim reconciles as applied");
    assert_eq!(
        current.state().expect("state").outbox["intent:1"].state,
        OutboxState::Applied
    );
}

#[test]
fn component_occurrence_is_exactly_once_by_content() {
    let (mut machine, _) = machine_with_run();
    let input = machine.put_artifact("example/component-input", b"in".to_vec());
    let output = machine.put_artifact("example/component-output", b"out".to_vec());
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let occurrence = ComponentOccurrence {
        occurrence_id: "component:1".to_owned(),
        run_id: "run:durable".to_owned(),
        site_id: "call.example".to_owned(),
        component: "example.component".to_owned(),
        input,
        output,
        occurrence_binding: "binding:component/1".to_owned(),
        implementation_revision: "1".to_owned(),
    };
    coordinator
        .record_component(occurrence.clone())
        .expect("occurrence records");
    coordinator
        .record_component(occurrence.clone())
        .expect("retry is idempotent");
    let mut conflicting = occurrence;
    conflicting.implementation_revision = "2".to_owned();
    assert!(matches!(
        coordinator.record_component(conflicting),
        Err(DurableError::IllegalTransition(_))
    ));
}

#[test]
fn higher_profile_journal_is_cas_committed_and_replayed_in_order() {
    let (machine, _) = machine_with_run();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let first = JournalRecord::new("record:1", "example.record/1", json!({"sequence": 1}))
        .expect("record seals");
    coordinator
        .append_journal_record("journal:example", first.clone())
        .expect("record appends");
    coordinator
        .append_journal_record("journal:example", first)
        .expect("retry is idempotent");
    coordinator
        .append_journal_record(
            "journal:example",
            JournalRecord::new("record:2", "example.record/1", json!({"sequence": 2}))
                .expect("record seals"),
        )
        .expect("second record appends");
    assert!(matches!(
        coordinator.append_journal_record(
            "journal:example",
            JournalRecord::new("record:1", "example.record/1", json!({"sequence": 999}),)
                .expect("conflicting record seals"),
        ),
        Err(DurableError::IllegalTransition(_))
    ));

    let first_atomic = JournalRecord::new("record:a1", "example.atomic/1", json!({"a": 1}))
        .expect("first atomic record seals");
    let second_atomic = JournalRecord::new("record:b1", "example.atomic/1", json!({"b": 1}))
        .expect("second atomic record seals");
    coordinator
        .checkpoint_journals(&[
            JournalBatch {
                journal_id: "journal:a".to_owned(),
                records: vec![first_atomic],
            },
            JournalBatch {
                journal_id: "journal:b".to_owned(),
                records: vec![second_atomic],
            },
        ])
        .expect("two journals commit atomically");
    let uncommitted = JournalRecord::new("record:a2", "example.atomic/1", json!({"a": 2}))
        .expect("uncommitted record seals");
    let conflicting = JournalRecord::new("record:b1", "example.atomic/1", json!({"b": 999}))
        .expect("conflicting record seals");
    assert!(matches!(
        coordinator.checkpoint_journals(&[
            JournalBatch {
                journal_id: "journal:a".to_owned(),
                records: vec![uncommitted],
            },
            JournalBatch {
                journal_id: "journal:b".to_owned(),
                records: vec![conflicting],
            },
        ]),
        Err(DurableError::IllegalTransition(_))
    ));
    assert_eq!(
        coordinator
            .journal_records("journal:a")
            .expect("journal reads")
            .len(),
        1
    );
    drop(coordinator);

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let records = reopened
        .journal_records("journal:example")
        .expect("journal reads");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record_id, "record:1");
    assert_eq!(records[1].record_id, "record:2");
}
