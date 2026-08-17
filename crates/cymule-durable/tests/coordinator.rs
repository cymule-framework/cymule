//! Fault-oriented durable single-domain contract tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, Definition, DispatchPolicy, EffectContract,
    EffectProfile, EffectTransition, Expression, Machine, MutationKind, PlanCandidate,
    ROOT_SCOPE_ID, ReconciliationMode, Region, WorldOutcome, effect_intent_id,
};
use cymule_durable::{
    ComponentOccurrence, Continuation, ContinuationStatus, DurableCoordinator, DurableError,
    EffectDispatch, FrameState, JournalBatch, JournalRecord, MemoryStore, OutboxState,
    WaitActivation, WaitActivationSource, WaitCondition, WaitKind, WaitState,
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

fn submit(machine: &mut Machine, run_id: &str, command_id: &str, command: Command) {
    let precondition = machine.projection().runs[run_id].precondition_token();
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: command_id.to_owned(),
            actor: "actor:test".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition: Some(precondition),
            command,
        })
        .expect("command submits");
}

fn prepared_effect_transition() -> (Machine, Machine, Continuation, EffectDispatch) {
    let mut machine = Machine::new();
    let plan = machine
        .seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "effect_delta_test".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: vec![EffectContract {
                id: "example.effect".to_owned(),
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
            command_id: "command:effect-run-start".to_owned(),
            actor: "actor:test".to_owned(),
            run_id: "run:effect-delta".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: "binding:test".to_owned(),
            },
        })
        .expect("Run starts");
    let base = machine.clone();
    let args = machine.put_artifact("cymule.effect-args/1", b"{}".to_vec());
    let binding = "binding:effect/test@1".to_owned();
    let intent_id = effect_intent_id(
        "run:effect-delta",
        "main",
        "effect.site",
        ROOT_SCOPE_ID,
        0,
        "primary",
        &args,
        "cymule.effect-schema/1",
    )
    .expect("effect intent derives");
    submit(
        &mut machine,
        "run:effect-delta",
        "command:effect-propose",
        Command::ProposeEffect {
            scope_id: ROOT_SCOPE_ID.to_owned(),
            invocation_id: "main".to_owned(),
            site_id: "effect.site".to_owned(),
            occurrence: "primary".to_owned(),
            operation: "example.effect".to_owned(),
            args: args.clone(),
            occurrence_binding: binding.clone(),
        },
    );
    submit(
        &mut machine,
        "run:effect-delta",
        "command:effect-prepare",
        Command::TransitionEffect {
            intent_id: intent_id.clone(),
            transition: EffectTransition::Prepare,
        },
    );
    (
        base,
        machine,
        continuation(plan.plan_id),
        EffectDispatch {
            intent_id,
            run_id: "run:effect-delta".to_owned(),
            operation: "example.effect".to_owned(),
            input: args,
            occurrence_binding: binding,
            state: OutboxState::Pending,
            claim_epoch: 0,
            claim_owner: None,
            result: None,
        },
    )
}

fn continuation(plan_id: String) -> Continuation {
    Continuation {
        run_id: "run:durable".to_owned(),
        plan_id,
        binding_context: "binding:test/1".to_owned(),
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            input: cymule_core::ArtifactRef {
                artifact_id: format!("sha256:{}", "0".repeat(64)),
                kind: "test/input".to_owned(),
            },
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
fn frozen_wait_activation_fixture_matches_the_rust_contract() {
    let activation: WaitActivation =
        serde_json::from_str(include_str!("../../../tests/fixtures/wait-activation.json"))
            .expect("wait activation fixture deserializes");
    activation.verify().expect("wait activation verifies");

    let mut malformed = activation;
    malformed.result.artifact_id = "artifact:not-content-addressed".to_owned();
    assert!(matches!(
        malformed.verify(),
        Err(DurableError::Validation(_))
    ));
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
fn identified_signal_activation_is_atomic_idempotent_and_reopenable() {
    let (mut machine, plan_id) = machine_with_run();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(continuation(plan_id))
        .expect("continuation persists");
    for (wait_id, consume_once) in [
        ("wait:signal:broadcast:1", false),
        ("wait:signal:broadcast:2", false),
        ("wait:signal:consumer", true),
    ] {
        coordinator
            .register_wait(WaitCondition {
                wait_id: wait_id.to_owned(),
                run_id: "run:durable".to_owned(),
                kind: WaitKind::Signal {
                    key: "signal:approved".to_owned(),
                },
                consume_once,
                state: WaitState::Pending,
                result: None,
            })
            .expect("signal wait registers");
    }
    let result = machine.put_artifact("example/signal", b"approved".to_vec());
    let activation = WaitActivation::new(
        "activation:signal:1",
        WaitActivationSource::Signal {
            key: "signal:approved".to_owned(),
        },
        BTreeSet::from([
            "wait:signal:broadcast:1".to_owned(),
            "wait:signal:broadcast:2".to_owned(),
            "wait:signal:consumer".to_owned(),
        ]),
        result,
    )
    .expect("activation validates");
    coordinator
        .activate_waits(&machine, activation.clone())
        .expect("activation commits");
    coordinator
        .activate_waits(&machine, activation.clone())
        .expect("redelivery is idempotent");
    let conflicting = WaitActivation::new(
        "activation:signal:1",
        WaitActivationSource::Signal {
            key: "signal:approved".to_owned(),
        },
        BTreeSet::from(["wait:signal:broadcast:1".to_owned()]),
        activation.result.clone(),
    )
    .expect("conflicting activation shape validates");
    assert!(matches!(
        coordinator.activate_waits(&machine, conflicting),
        Err(DurableError::IllegalTransition(_))
    ));
    assert_eq!(
        coordinator.state().expect("state").wait_activations["activation:signal:1"],
        activation
    );
    assert!(
        coordinator
            .state()
            .expect("state")
            .waits
            .values()
            .all(|wait| wait.state == WaitState::Completed)
    );
    assert_eq!(
        coordinator.state().expect("state").continuations["run:durable"].status,
        ContinuationStatus::Ready
    );
    assert!(matches!(
        coordinator.checkpoint_wait_activation_journals(
            &machine,
            activation.clone(),
            &[JournalBatch {
                journal_id: "journal:late-projection".to_owned(),
                records: vec![
                    JournalRecord::new(
                        "projection:late:1",
                        "example.projection/1",
                        json!({"late": true}),
                    )
                    .expect("late projection seals")
                ],
            }],
        ),
        Err(DurableError::IllegalTransition(_))
    ));
    assert!(
        coordinator
            .journal_records("journal:late-projection")
            .expect("journal reads")
            .is_empty()
    );

    drop(coordinator);
    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    reopened
        .activate_waits(&machine, activation)
        .expect("redelivery after reopen is idempotent");
    assert_eq!(reopened.state().expect("state").wait_activations.len(), 1);
}

#[test]
fn signal_activation_rejects_wrong_or_multiple_consume_once_targets_atomically() {
    let (mut machine, plan_id) = machine_with_run();
    let result = machine.put_artifact("example/signal", b"payload".to_vec());
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(continuation(plan_id))
        .expect("continuation persists");
    for wait_id in ["wait:signal:one", "wait:signal:two"] {
        coordinator
            .register_wait(WaitCondition {
                wait_id: wait_id.to_owned(),
                run_id: "run:durable".to_owned(),
                kind: WaitKind::Signal {
                    key: "signal:exclusive".to_owned(),
                },
                consume_once: true,
                state: WaitState::Pending,
                result: None,
            })
            .expect("signal wait registers");
    }
    let before = coordinator.revision().expect("revision").to_owned();
    let multiple = WaitActivation::new(
        "activation:signal:multiple",
        WaitActivationSource::Signal {
            key: "signal:exclusive".to_owned(),
        },
        BTreeSet::from(["wait:signal:one".to_owned(), "wait:signal:two".to_owned()]),
        result.clone(),
    )
    .expect("activation shape validates");
    assert!(matches!(
        coordinator.activate_waits(&machine, multiple),
        Err(DurableError::Validation(_))
    ));
    let wrong_key = WaitActivation::new(
        "activation:signal:wrong-key",
        WaitActivationSource::Signal {
            key: "signal:other".to_owned(),
        },
        BTreeSet::from(["wait:signal:one".to_owned()]),
        result.clone(),
    )
    .expect("activation shape validates");
    assert!(matches!(
        coordinator.activate_waits(&machine, wrong_key),
        Err(DurableError::Validation(_))
    ));
    let mut unrelated_machine = coordinator
        .restore_machine()
        .expect("durable Machine restores");
    unrelated_machine.put_artifact("example/unrelated", b"unrelated".to_vec());
    let unrelated = WaitActivation::new(
        "activation:signal:unrelated-machine",
        WaitActivationSource::Signal {
            key: "signal:exclusive".to_owned(),
        },
        BTreeSet::from(["wait:signal:one".to_owned()]),
        result.clone(),
    )
    .expect("activation shape validates");
    assert!(matches!(
        coordinator.activate_waits(&unrelated_machine, unrelated),
        Err(DurableError::Validation(_))
    ));
    assert!(matches!(
        coordinator.complete_wait("wait:signal:one", result),
        Err(DurableError::Validation(_))
    ));
    assert_eq!(coordinator.revision(), Some(before.as_str()));
    assert!(
        coordinator
            .state()
            .expect("state")
            .waits
            .values()
            .all(|wait| wait.state == WaitState::Pending)
    );
    assert!(
        coordinator
            .state()
            .expect("state")
            .wait_activations
            .is_empty()
    );
}

#[test]
fn timer_activation_is_exactly_identified_and_stale_writers_fail_closed() {
    let (mut machine, plan_id) = machine_with_run();
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    current
        .put_continuation(continuation(plan_id))
        .expect("continuation persists");
    current
        .register_wait(WaitCondition {
            wait_id: "wait:timer:1".to_owned(),
            run_id: "run:durable".to_owned(),
            kind: WaitKind::Timer {
                timer_id: "timer:deadline".to_owned(),
            },
            consume_once: false,
            state: WaitState::Pending,
            result: None,
        })
        .expect("timer wait registers");
    let mut stale = DurableCoordinator::open(store).expect("stale view opens");
    let result = machine.put_artifact("example/timer", b"fired".to_vec());
    let activation = WaitActivation::new(
        "activation:timer:1",
        WaitActivationSource::Timer {
            timer_id: "timer:deadline".to_owned(),
        },
        BTreeSet::from(["wait:timer:1".to_owned()]),
        result,
    )
    .expect("timer activation validates");
    current
        .activate_waits(&machine, activation.clone())
        .expect("timer activation commits");
    assert!(matches!(
        stale.activate_waits(&machine, activation),
        Err(DurableError::Conflict { .. })
    ));
    assert_eq!(
        current.state().expect("state").waits["wait:timer:1"].state,
        WaitState::Completed
    );
}

#[test]
fn conflicting_projection_checkpoint_rejects_wait_activation_atomically() {
    let (mut machine, plan_id) = machine_with_run();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(continuation(plan_id))
        .expect("continuation persists");
    coordinator
        .register_wait(WaitCondition {
            wait_id: "wait:atomic-projection".to_owned(),
            run_id: "run:durable".to_owned(),
            kind: WaitKind::Signal {
                key: "signal:atomic-projection".to_owned(),
            },
            consume_once: true,
            state: WaitState::Pending,
            result: None,
        })
        .expect("signal wait registers");
    coordinator
        .append_journal_record(
            "journal:projection",
            JournalRecord::new(
                "projection:wake:1",
                "example.projection/1",
                json!({"state": "old"}),
            )
            .expect("existing record seals"),
        )
        .expect("existing record appends");
    let before = coordinator.revision().expect("revision").to_owned();
    let result = machine.put_artifact("example/signal", b"accepted".to_vec());
    let activation = WaitActivation::new(
        "activation:atomic-projection",
        WaitActivationSource::Signal {
            key: "signal:atomic-projection".to_owned(),
        },
        BTreeSet::from(["wait:atomic-projection".to_owned()]),
        result,
    )
    .expect("activation validates");
    let conflicting = JournalRecord::new(
        "projection:wake:1",
        "example.projection/1",
        json!({"state": "new"}),
    )
    .expect("conflicting record seals");
    assert!(matches!(
        coordinator.checkpoint_wait_activation_journals(
            &machine,
            activation,
            &[JournalBatch {
                journal_id: "journal:projection".to_owned(),
                records: vec![conflicting],
            }],
        ),
        Err(DurableError::IllegalTransition(_))
    ));
    assert_eq!(coordinator.revision(), Some(before.as_str()));
    assert_eq!(
        coordinator.state().expect("state").waits["wait:atomic-projection"].state,
        WaitState::Pending
    );
    assert!(
        coordinator
            .state()
            .expect("state")
            .wait_activations
            .is_empty()
    );
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
fn previewed_lease_and_higher_profile_record_share_one_cas() {
    let (machine, _) = machine_with_run();
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    let lease = current
        .preview_lease("virtual-slot:worker-a:0", "worker:a", 10, 20)
        .expect("lease previews");
    let record = JournalRecord::new(
        "virtual:claim:1",
        "test.virtual/1",
        json!({"claim": "work:1"}),
    )
    .expect("record creates");
    let batch = JournalBatch {
        journal_id: "journal:virtual".to_owned(),
        records: vec![record],
    };
    current
        .checkpoint_lease_journals(&lease, 10, 20, std::slice::from_ref(&batch))
        .expect("lease and record checkpoint");
    assert_eq!(
        current.state().expect("state").leases["virtual-slot:worker-a:0"],
        lease
    );
    assert_eq!(
        current
            .journal_records("journal:virtual")
            .expect("journal reads")
            .len(),
        1
    );

    assert!(matches!(
        stale.checkpoint_lease_journals(&lease, 10, 20, &[batch]),
        Err(DurableError::Conflict { .. })
    ));
    assert!(stale.state().expect("stale state").leases.is_empty());
    assert!(
        stale
            .journal_records("journal:virtual")
            .expect("stale journal reads")
            .is_empty()
    );

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    assert_eq!(
        reopened.state().expect("state").leases["virtual-slot:worker-a:0"],
        lease
    );
    assert_eq!(
        reopened
            .journal_records("journal:virtual")
            .expect("journal reopens")
            .len(),
        1
    );
}

#[test]
fn effect_outbox_stages_reject_unrelated_canonical_machine_changes() {
    let (base, prepared, continuation, dispatch) = prepared_effect_transition();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&base)
        .expect("store initializes");

    let mut unrelated_enqueue = prepared.clone();
    submit(
        &mut unrelated_enqueue,
        "run:effect-delta",
        "command:unrelated-enqueue-fact",
        Command::RecordFact {
            key: "unrelated.enqueue".to_owned(),
            value: "invalid".to_owned(),
        },
    );
    assert!(matches!(
        coordinator.checkpoint_effect_enqueue(
            &unrelated_enqueue,
            continuation.clone(),
            dispatch.clone(),
        ),
        Err(DurableError::Validation(_))
    ));
    assert!(coordinator.state().expect("state").outbox.is_empty());
    assert_eq!(
        coordinator
            .restore_machine()
            .expect("Machine restores")
            .snapshot(),
        base.snapshot()
    );

    coordinator
        .checkpoint_effect_enqueue(&prepared, continuation.clone(), dispatch.clone())
        .expect("exact prepared Effect and outbox enqueue atomically");
    let mut committed = prepared.clone();
    submit(
        &mut committed,
        "run:effect-delta",
        "command:effect-scope-commit",
        Command::CommitScope {
            scope_id: ROOT_SCOPE_ID.to_owned(),
        },
    );
    coordinator
        .checkpoint(&committed, continuation.clone(), None)
        .expect("scope commit checkpoints");
    let lease = coordinator
        .acquire_lease("effect:delta", "worker:effect", 1, 10)
        .expect("effect lease acquires");
    let mut claimed = committed.clone();
    submit(
        &mut claimed,
        "run:effect-delta",
        "command:effect-authorize",
        Command::TransitionEffect {
            intent_id: dispatch.intent_id.clone(),
            transition: EffectTransition::AuthorizeRelease,
        },
    );
    submit(
        &mut claimed,
        "run:effect-delta",
        "command:effect-start-dispatch",
        Command::TransitionEffect {
            intent_id: dispatch.intent_id.clone(),
            transition: EffectTransition::StartDispatch,
        },
    );
    let mut unrelated_claim = claimed.clone();
    submit(
        &mut unrelated_claim,
        "run:effect-delta",
        "command:unrelated-claim-fact",
        Command::RecordFact {
            key: "unrelated.claim".to_owned(),
            value: "invalid".to_owned(),
        },
    );
    assert!(matches!(
        coordinator.checkpoint_effect_claim(
            &unrelated_claim,
            &dispatch.intent_id,
            "worker:effect",
            lease.epoch,
        ),
        Err(DurableError::Validation(_))
    ));
    assert_eq!(
        coordinator.state().expect("state").outbox[&dispatch.intent_id].state,
        OutboxState::Pending
    );
    assert_eq!(
        coordinator
            .restore_machine()
            .expect("Machine restores")
            .snapshot(),
        committed.snapshot()
    );
    coordinator
        .checkpoint_effect_claim(&claimed, &dispatch.intent_id, "worker:effect", lease.epoch)
        .expect("exact release and dispatch-start claim atomically");

    let mut observed = claimed.clone();
    submit(
        &mut observed,
        "run:effect-delta",
        "command:effect-observe-applied",
        Command::TransitionEffect {
            intent_id: dispatch.intent_id.clone(),
            transition: EffectTransition::Observe(WorldOutcome::Applied),
        },
    );
    let result = observed.put_artifact("cymule.effect-result/1", b"result".to_vec());
    let mut unrelated_settlement = observed.clone();
    submit(
        &mut unrelated_settlement,
        "run:effect-delta",
        "command:unrelated-settlement-fact",
        Command::RecordFact {
            key: "unrelated.settlement".to_owned(),
            value: "invalid".to_owned(),
        },
    );
    assert!(matches!(
        coordinator.checkpoint_effect_settlement(
            &unrelated_settlement,
            &dispatch.intent_id,
            "worker:effect",
            lease.epoch,
            OutboxState::Applied,
            Some(result.clone()),
        ),
        Err(DurableError::Validation(_))
    ));
    assert_eq!(
        coordinator.state().expect("state").outbox[&dispatch.intent_id].state,
        OutboxState::Claimed
    );
    assert_eq!(
        coordinator
            .restore_machine()
            .expect("Machine restores")
            .snapshot(),
        claimed.snapshot()
    );
    coordinator
        .checkpoint_effect_settlement(
            &observed,
            &dispatch.intent_id,
            "worker:effect",
            lease.epoch,
            OutboxState::Applied,
            Some(result),
        )
        .expect("exact observation and outbox settlement atomically");
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

#[test]
fn artifact_journal_checkpoint_rejects_unlisted_machine_changes_atomically() {
    let (machine, _) = machine_with_run();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut proposed = coordinator.restore_machine().expect("Machine restores");
    let result = proposed.put_artifact("example/result", b"result".to_vec());
    proposed.put_artifact("example/unrelated", b"unrelated".to_vec());
    let record = JournalRecord::new(
        "record:artifact-result",
        "example.result/1",
        json!({"result": result.clone()}),
    )
    .expect("result record seals");
    let before = coordinator.revision().expect("revision").to_owned();
    assert!(matches!(
        coordinator.checkpoint_artifact_journals(
            &proposed,
            &BTreeSet::from([result.clone()]),
            &[JournalBatch {
                journal_id: "journal:artifact-result".to_owned(),
                records: vec![record.clone()],
            }],
        ),
        Err(DurableError::Validation(_))
    ));
    assert_eq!(coordinator.revision(), Some(before.as_str()));
    assert!(
        coordinator
            .journal_records("journal:artifact-result")
            .expect("journal reads")
            .is_empty()
    );

    let mut valid = coordinator.restore_machine().expect("Machine restores");
    assert_eq!(
        valid.put_artifact("example/result", b"result".to_vec()),
        result
    );
    coordinator
        .checkpoint_artifact_journals(
            &valid,
            &BTreeSet::from([result.clone()]),
            &[JournalBatch {
                journal_id: "journal:artifact-result".to_owned(),
                records: vec![record],
            }],
        )
        .expect("Artifact and journal commit atomically");
    drop(coordinator);

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    assert!(
        reopened
            .restore_machine()
            .expect("Machine restores")
            .artifact(&result)
            .is_some()
    );
    assert_eq!(
        reopened
            .journal_records("journal:artifact-result")
            .expect("journal reads")
            .len(),
        1
    );
}
