//! Fault-oriented semantic kernel conformance tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, ComponentContract, CoreError,
    Definition, DispatchPolicy, EffectContract, EffectPhase, EffectProfile, EffectTransition,
    Event, EventPayload, Expression, Machine, MutationKind, Operation, PlanCandidate,
    ReconciliationMode, ReconciliationResolution, ReconciliationState, Region, ReplayAvailability,
    ScopeStatus, Step, WorldOutcome,
};
use serde_json::json;

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "semantic_kernel_test".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.echo".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
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

fn envelope(machine: &Machine, sequence: u64, run_id: &str, command: Command) -> CommandEnvelope {
    CommandEnvelope {
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
    }
}

#[test]
fn plan_identity_is_canonical_and_tamper_evident() {
    let first = candidate().seal().expect("candidate seals");
    let mut reordered = candidate();
    reordered.metadata.insert("z".to_owned(), "last".to_owned());
    reordered
        .metadata
        .insert("a".to_owned(), "first".to_owned());
    let reordered = reordered.seal().expect("candidate seals");
    let mut same = candidate();
    same.metadata.insert("a".to_owned(), "first".to_owned());
    same.metadata.insert("z".to_owned(), "last".to_owned());
    assert_eq!(
        reordered.plan_id,
        same.seal().expect("candidate seals").plan_id
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
    assert!(matches!(invalid.seal(), Err(CoreError::Validation(_))));
}

#[test]
fn command_idempotency_and_stale_action_are_explicit() {
    let mut machine = Machine::new();
    let plan = machine.seal_plan(candidate()).expect("plan seals");
    let start = envelope(
        &machine,
        1,
        "run:idempotency",
        Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: "binding:v1".to_owned(),
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
                value: "v1".to_owned(),
            },
        ))
        .expect("fact applies");
    let mut outdated = envelope(
        &machine,
        3,
        "run:idempotency",
        Command::RecordFact {
            key: "fact:two".to_owned(),
            value: "v2".to_owned(),
        },
    );
    outdated.expected_precondition = Some(stale);
    let receipt = machine.submit(outdated).expect("conflict is a receipt");
    assert_eq!(receipt.status, CommandReceiptStatus::Conflict);
    assert_eq!(receipt.error_code.as_deref(), Some("stale_action"));
}

#[test]
fn binding_is_pinned_and_unknown_effect_must_reconcile() {
    let mut machine = Machine::new();
    let plan = machine.seal_plan(candidate()).expect("plan seals");
    let run_id = "run:effect";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: "binding:default/v1".to_owned(),
            },
        ))
        .expect("run starts");
    let args = machine.put_artifact("cymule.effect-args/1", br#"{"value":1}"#.to_vec());
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::ProposeEffect {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                invocation_id: "main".to_owned(),
                site_id: "effect.capture".to_owned(),
                occurrence: "primary".to_owned(),
                operation: "test.capture".to_owned(),
                args,
                occurrence_binding: "binding:adapter/v1".to_owned(),
            },
        ))
        .expect("effect is proposed");
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::UpdateBinding {
                binding_context: "binding:default/v2".to_owned(),
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
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            40,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::StartDispatch,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    for (sequence, transition) in [
        (4, EffectTransition::Prepare),
        (5, EffectTransition::AuthorizeRelease),
        (6, EffectTransition::StartDispatch),
        (7, EffectTransition::Observe(WorldOutcome::Unknown)),
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
            .expect("effect transition applies");
    }
    machine
        .submit(envelope(
            &machine,
            8,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("scope commits independently of world settlement");

    let run = machine.projection().runs.get(run_id).expect("run exists");
    let effect = run.effects.get(&intent_id).expect("effect exists");
    assert_eq!(effect.occurrence_binding, "binding:adapter/v1");
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
            &machine,
            80,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));

    assert!(matches!(
        machine.submit(envelope(
            &machine,
            9,
            run_id,
            Command::CompleteRun { result: None },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            &machine,
            10,
            run_id,
            Command::TransitionEffect {
                intent_id,
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied),
            },
        ))
        .expect("unknown result reconciles");
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

#[test]
fn epoch_fences_prior_attempts() {
    let mut machine = Machine::new();
    let plan = machine.seal_plan(candidate()).expect("plan seals");
    let run_id = "run:fence";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: "binding:v1".to_owned(),
            },
        ))
        .expect("run starts");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::BeginAttempt {
                attempt_id: "attempt:1".to_owned(),
                continuation_id: "continuation:1".to_owned(),
                occurrence_binding: "binding:worker/1".to_owned(),
                epoch: 0,
            },
        ))
        .expect("attempt starts");
    machine
        .submit(envelope(&machine, 3, run_id, Command::AdvanceEpoch))
        .expect("epoch advances");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            4,
            run_id,
            Command::YieldAttempt {
                attempt_id: "attempt:1".to_owned(),
                epoch: 0,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

#[test]
fn replay_orders_a_causal_set_and_reports_retention_loss() {
    let mut machine = Machine::new();
    let plan = machine.seal_plan(candidate()).expect("plan seals");
    let run_id = "run:replay";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: "binding:v1".to_owned(),
            },
        ))
        .expect("run starts");
    let start = machine.events().next().expect("start event").clone();
    let fact_a = Event::new(
        "command:a".to_owned(),
        "hash:a".to_owned(),
        run_id.to_owned(),
        vec![start.event_id.clone()],
        BTreeSet::new(),
        BTreeSet::from(["fact:a".to_owned()]),
        None,
        EventPayload::FactRecorded {
            key: "a".to_owned(),
            value: "1".to_owned(),
        },
    )
    .expect("event hashes");
    let fact_b = Event::new(
        "command:b".to_owned(),
        "hash:b".to_owned(),
        run_id.to_owned(),
        vec![start.event_id.clone()],
        BTreeSet::new(),
        BTreeSet::from(["fact:b".to_owned()]),
        None,
        EventPayload::FactRecorded {
            key: "b".to_owned(),
            value: "2".to_owned(),
        },
    )
    .expect("event hashes");
    let mut tampered = fact_a.clone();
    tampered.event_id = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        tampered.verify(),
        Err(CoreError::IdentityMismatch(_))
    ));
    let missing_parent = Event::new(
        "command:orphan".to_owned(),
        "hash:orphan".to_owned(),
        run_id.to_owned(),
        vec![format!("sha256:{}", "f".repeat(64))],
        BTreeSet::new(),
        BTreeSet::new(),
        None,
        EventPayload::FactRecorded {
            key: "orphan".to_owned(),
            value: "1".to_owned(),
        },
    )
    .expect("event hashes");
    assert!(matches!(
        Machine::replay([missing_parent]),
        Err(CoreError::Causal(_))
    ));
    let left = Machine::replay(vec![fact_a.clone(), start.clone(), fact_b.clone()])
        .expect("causal set replays");
    let right = Machine::replay(vec![fact_b, fact_a, start]).expect("order is irrelevant");
    assert_eq!(
        left.digest().expect("digest"),
        right.digest().expect("digest")
    );

    let artifact = machine.put_artifact("test/value", b"retained".to_vec());
    assert_eq!(
        machine.replay_availability(std::slice::from_ref(&artifact)),
        ReplayAvailability::Exact
    );
    machine.remove_artifact_for_test(&artifact.artifact_id);
    assert!(matches!(
        machine.replay_availability(&[artifact]),
        ReplayAvailability::ProjectionOnly { .. }
    ));
}
