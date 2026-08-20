//! Fault-oriented semantic kernel conformance tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus,
    ComponentContract, CoreError, Definition, DispatchPolicy, EffectContract, EffectPhase,
    EffectProfile, EffectTransition, Event, EventPayload, Expression, Machine, MutationKind,
    Operation, PlanCandidate, ReconciliationMode, ReconciliationResolution, ReconciliationState,
    Region, ReplayAvailability, ScopeStatus, SealedPlan, Step, WorldOutcome, effect_intent_id,
    effect_obligation_id,
};
use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use serde_json::json;

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
    let mut snapshot = machine.snapshot();
    snapshot.artifacts[0].reference.identity_version = "cymule.artifact/1".to_owned();
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
}

#[test]
fn public_validation_errors_and_effect_policy_boundaries_are_stable() {
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

    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate);
    let run_id = "run:effect-site";
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
        .expect("Run starts");
    let args = machine
        .put_artifact("cymule.effect-args/1", b"{}".to_vec())
        .expect("arguments store");
    let propose = |scope_id: &str, invocation_id: &str, site_id: &str, occurrence: &str| {
        Command::ProposeEffect {
            scope_id: scope_id.to_owned(),
            invocation_id: invocation_id.to_owned(),
            site_id: site_id.to_owned(),
            occurrence: occurrence.to_owned(),
            operation: "test.capture".to_owned(),
            args: args.clone(),
            occurrence_binding: "binding:effect/v1".to_owned(),
        }
    };

    assert!(matches!(
        machine.submit(envelope(
            &machine,
            2,
            run_id,
            propose(
                cymule_core::ROOT_SCOPE_ID,
                "main",
                "effect.unreachable",
                "unreachable",
            ),
        )),
        Err(CoreError::NotFound(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            &machine,
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
            &machine,
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
            &machine,
            5,
            run_id,
            propose("scope:missing", "main", "effect.reachable", "reachable",),
        )),
        Err(CoreError::NotFound(_))
    ));
    let run = &machine.projection().runs[run_id];
    let mut wrong_profile = machine
        .plan(&run.current_plan)
        .expect("Plan exists")
        .candidate
        .effects[0]
        .profile
        .clone();
    wrong_profile.irreversible = true;
    let intent_id = effect_intent_id(
        run_id,
        "main",
        "effect.reachable",
        cymule_core::ROOT_SCOPE_ID,
        run.epoch,
        "reachable",
        &args,
        "cymule.effect-schema/1",
    )
    .expect("intent hashes");
    let wrong_profile_event = Event::new(
        "command:wrong-effect-profile".to_owned(),
        "hash:wrong-effect-profile".to_owned(),
        run_id.to_owned(),
        vec![run.last_event.clone()],
        BTreeSet::new(),
        BTreeSet::new(),
        None,
        EventPayload::EffectProposed {
            intent_id,
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            invocation_id: "main".to_owned(),
            site_id: "effect.reachable".to_owned(),
            occurrence: "reachable".to_owned(),
            scope_epoch: run.epoch,
            effect_schema_version: "cymule.effect-schema/1".to_owned(),
            operation: "test.capture".to_owned(),
            profile: wrong_profile,
            args: args.clone(),
            occurrence_binding: "binding:effect/v1".to_owned(),
        },
    )
    .expect("event hashes");
    assert!(matches!(
        machine.append_event(wrong_profile_event),
        Err(CoreError::Validation(message)) if message.contains("Plan-declared profile")
    ));
    machine
        .submit(envelope(
            &machine,
            6,
            run_id,
            propose(
                cymule_core::ROOT_SCOPE_ID,
                "main",
                "effect.reachable",
                "reachable",
            ),
        ))
        .expect("exact reachable effect tuple admits");

    let effect = machine.projection().runs[run_id]
        .effects
        .values()
        .next()
        .expect("effect exists");
    assert_eq!(effect.invocation_id, "main");
    assert_eq!(effect.site_id, "effect.reachable");
    assert_eq!(effect.occurrence, "reachable");
    assert_eq!(effect.profile.dispatch, DispatchPolicy::OnScopeCommit);
    assert_eq!(effect.profile.reconciliation, ReconciliationMode::Queryable);
    machine
        .verify_replay()
        .expect("profile rules replay exactly");
}

#[test]
fn effect_profiles_gate_release_and_reconciliation_independently() {
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

    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate);
    let run_id = "run:effect-profiles";
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
        .expect("Run starts");
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
                    invocation_id: "main".to_owned(),
                    site_id: site.to_owned(),
                    occurrence: occurrence.to_owned(),
                    operation: operation.to_owned(),
                    args: args.clone(),
                    occurrence_binding: format!("binding:{occurrence}/v1"),
                },
            ))
            .expect("effect admits");
    }
    let intents: BTreeMap<String, String> = machine.projection().runs[run_id]
        .effects
        .values()
        .map(|effect| (effect.site_id.clone(), effect.intent_id.clone()))
        .collect();

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
                    &machine,
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
            &machine,
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
            &machine,
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
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            42,
            run_id,
            Command::TransitionEffect {
                intent_id: intents["effect.impossible"].clone(),
                transition: EffectTransition::Reconcile(
                    ReconciliationResolution::ResolvedNotApplied,
                ),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));

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

const fn transition_index(transition: &EffectTransition) -> u64 {
    match transition {
        EffectTransition::AuthorizeRelease => 0,
        EffectTransition::StartDispatch => 1,
        EffectTransition::Observe(_) => 2,
        EffectTransition::Prepare | EffectTransition::Reconcile(_) => 3,
    }
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
                    mode: cymule_core::ScopeMode::Transactional,
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
fn envelopes_footprints_facts_attempts_and_scope_parents_fail_closed() {
    let sealed = seal_for_kernel(candidate()).expect("Plan seals");
    let invalid = [
        CommandEnvelope {
            command_version: "cymule.command/invalid".to_owned(),
            command_id: "command:invalid-version".to_owned(),
            actor: "actor:test".to_owned(),
            run_id: "run:invalid".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: sealed.plan_id.clone(),
                binding_context: "binding:test".to_owned(),
            },
        },
        CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: String::new(),
            actor: "actor:test".to_owned(),
            run_id: "run:invalid".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: sealed.plan_id.clone(),
                binding_context: "binding:test".to_owned(),
            },
        },
        CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "c".repeat(201),
            actor: "actor:test".to_owned(),
            run_id: "run:invalid".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: sealed.plan_id.clone(),
                binding_context: "binding:test".to_owned(),
            },
        },
        CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:invalid-actor".to_owned(),
            actor: String::new(),
            run_id: "run:invalid".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: sealed.plan_id.clone(),
                binding_context: "binding:test".to_owned(),
            },
        },
        CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:invalid-run".to_owned(),
            actor: "actor:test".to_owned(),
            run_id: String::new(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: sealed.plan_id.clone(),
                binding_context: "binding:test".to_owned(),
            },
        },
    ];
    for envelope in invalid {
        let mut machine = Machine::new();
        machine
            .insert_plan(sealed.clone())
            .expect("Plan inserts for invalid envelope test");
        assert!(matches!(
            machine.submit(envelope),
            Err(CoreError::Validation(_))
        ));
        assert!(machine.projection().runs.is_empty());
    }

    let mut boundary_machine = Machine::new();
    boundary_machine
        .insert_plan(sealed.clone())
        .expect("Plan inserts for boundary envelope test");
    boundary_machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "c".repeat(200),
            actor: "actor:test".to_owned(),
            run_id: "run:boundary".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: sealed.plan_id.clone(),
                binding_context: "binding:test".to_owned(),
            },
        })
        .expect("200-character command identity is legal");

    let mut machine = Machine::new();
    machine.insert_plan(sealed.clone()).expect("Plan inserts");
    machine
        .insert_plan(sealed.clone())
        .expect("identical Plan insertion is idempotent");
    assert_eq!(machine.plan(&sealed.plan_id), Some(&sealed));
    let run_id = "run:invariants";
    machine
        .submit(envelope(
            &machine,
            1,
            run_id,
            Command::StartRun {
                plan_id: sealed.plan_id,
                binding_context: "binding:test".to_owned(),
            },
        ))
        .expect("Run starts");
    let start = machine.events().last().expect("start event");
    assert_eq!(start.reads, BTreeSet::new());
    assert_eq!(start.writes, BTreeSet::from([format!("run:{run_id}")]));
    assert_eq!(start.coordination_key, Some(format!("run:{run_id}")));

    let before_fact = machine.projection().digest().expect("projection hashes");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::RecordFact {
                key: "fact:stable".to_owned(),
                value: "one".to_owned(),
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
            &machine,
            3,
            run_id,
            Command::RecordFact {
                key: "fact:stable".to_owned(),
                value: "one".to_owned(),
            },
        ))
        .expect("identical fact repeats");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            4,
            run_id,
            Command::RecordFact {
                key: "fact:stable".to_owned(),
                value: "different".to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));

    machine
        .submit(envelope(
            &machine,
            5,
            run_id,
            Command::BeginAttempt {
                attempt_id: "attempt:invariants".to_owned(),
                continuation_id: "continuation:invariants".to_owned(),
                occurrence_binding: "binding:worker".to_owned(),
                epoch: 0,
            },
        ))
        .expect("attempt starts");
    machine
        .submit(envelope(
            &machine,
            6,
            run_id,
            Command::YieldAttempt {
                attempt_id: "attempt:invariants".to_owned(),
                epoch: 0,
            },
        ))
        .expect("active attempt yields");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            7,
            run_id,
            Command::YieldAttempt {
                attempt_id: "attempt:invariants".to_owned(),
                epoch: 0,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            &machine,
            8,
            run_id,
            Command::OpenScope {
                scope_id: "scope:child".to_owned(),
                parent_scope: cymule_core::ROOT_SCOPE_ID.to_owned(),
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
            machine.submit(envelope(&machine, sequence, run_id, command)),
            Err(CoreError::IllegalTransition(message))
                if message.contains("open child scope")
        ));
    }
    machine
        .submit(envelope(
            &machine,
            9,
            run_id,
            Command::OpenScope {
                scope_id: "scope:sibling".to_owned(),
                parent_scope: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("sibling scope opens on the ordered Run frontier");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            10,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(message)) if message.contains("open child")
    ));
    machine
        .submit(envelope(
            &machine,
            11,
            run_id,
            Command::CommitScope {
                scope_id: "scope:child".to_owned(),
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
            &machine,
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
            &machine,
            13,
            run_id,
            Command::AbortScope {
                scope_id: "scope:sibling".to_owned(),
            },
        ))
        .expect("sibling scope aborts");
    machine
        .submit(envelope(
            &machine,
            14,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("parent closes after every sibling closes");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            15,
            run_id,
            Command::OpenScope {
                scope_id: "scope:grandchild".to_owned(),
                parent_scope: "scope:child".to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            16,
            run_id,
            Command::AbortScope {
                scope_id: "scope:child".to_owned(),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
}

#[test]
fn machine_snapshot_requires_bidirectional_event_receipt_hash_closure() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    machine
        .submit(envelope(
            &machine,
            1,
            "run:snapshot-closure",
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: "binding:v1".to_owned(),
            },
        ))
        .expect("run starts");
    let snapshot = machine.snapshot();
    let event_id = snapshot.events[0].event_id.clone();

    let mut missing_receipt = serde_json::to_value(&snapshot).expect("snapshot encodes");
    missing_receipt["commands"] = json!({});
    let missing_receipt = serde_json::from_value(missing_receipt).expect("shape decodes");
    assert!(matches!(
        Machine::restore(missing_receipt),
        Err(CoreError::NotFound(message)) if message.contains("no command receipt")
    ));

    let mut mismatched_hash = serde_json::to_value(&snapshot).expect("snapshot encodes");
    mismatched_hash["commands"]["command:1"]["semantic_hash"] = json!("different");
    let mismatched_hash = serde_json::from_value(mismatched_hash).expect("shape decodes");
    assert!(matches!(
        Machine::restore(mismatched_hash),
        Err(CoreError::IdentityMismatch(message)) if message.contains("does not match retained event")
    ));

    let mut duplicate_receipt = serde_json::to_value(&snapshot).expect("snapshot encodes");
    let mut second = duplicate_receipt["commands"]["command:1"].clone();
    second["receipt"]["command_id"] = json!("command:2");
    duplicate_receipt["commands"]["command:2"] = second;
    let duplicate_receipt = serde_json::from_value(duplicate_receipt).expect("shape decodes");
    assert!(matches!(
        Machine::restore(duplicate_receipt),
        Err(CoreError::IdentityMismatch(message))
            if message.contains(&event_id) && message.contains("claimed by commands")
    ));

    let mut applied_with_error = serde_json::to_value(&snapshot).expect("snapshot encodes");
    applied_with_error["commands"]["command:1"]["receipt"]["error_code"] =
        json!("impossible_error");
    let applied_with_error = serde_json::from_value(applied_with_error).expect("shape decodes");
    assert!(matches!(
        Machine::restore(applied_with_error),
        Err(CoreError::IdentityMismatch(message)) if message.contains("one event and no error")
    ));

    let mut untyped_conflict = serde_json::to_value(&snapshot).expect("snapshot encodes");
    untyped_conflict["commands"]["command:1"]["receipt"]["status"] = json!("conflict");
    untyped_conflict["commands"]["command:1"]["receipt"]["event_id"] = json!(null);
    let untyped_conflict = serde_json::from_value(untyped_conflict).expect("shape decodes");
    assert!(matches!(
        Machine::restore(untyped_conflict),
        Err(CoreError::IdentityMismatch(message)) if message.contains("no typed error")
    ));
}

#[test]
fn machine_snapshot_restores_projection_artifacts_and_command_deduplication() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let start = envelope(
        &machine,
        1,
        "run:snapshot",
        Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: "binding:v1".to_owned(),
        },
    );
    let receipt = machine.submit(start.clone()).expect("run starts");
    let artifact = machine
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
        b"durable"
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
    assert_ne!(
        restored
            .snapshot()
            .digest()
            .expect("changed snapshot hashes"),
        snapshot_digest
    );
}

#[test]
fn compacted_machine_base_rehydrates_suffix_and_command_receipts() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
    let run_id = "run:compacted-snapshot";
    let start = envelope(
        &machine,
        1,
        run_id,
        Command::StartRun {
            plan_id: plan.plan_id,
            binding_context: "binding:v1".to_owned(),
        },
    );
    let start_receipt = machine.submit(start.clone()).expect("run starts");
    machine
        .submit(envelope(
            &machine,
            2,
            run_id,
            Command::BeginAttempt {
                attempt_id: "attempt:compaction:1".to_owned(),
                continuation_id: "continuation:compaction".to_owned(),
                occurrence_binding: "binding:worker/1".to_owned(),
                epoch: 0,
            },
        ))
        .expect("attempt starts");
    machine
        .submit(envelope(
            &machine,
            3,
            run_id,
            Command::YieldAttempt {
                attempt_id: "attempt:compaction:1".to_owned(),
                epoch: 0,
            },
        ))
        .expect("attempt yields");
    machine
        .submit(envelope(&machine, 4, run_id, Command::AdvanceEpoch))
        .expect("epoch advances");
    let expected_projection = machine.projection().clone();
    let event_ids: Vec<String> = machine
        .events()
        .map(|event| event.event_id.clone())
        .collect();
    for old_version in [
        "cymule.machine-snapshot/1",
        "cymule.machine-snapshot/2",
        "cymule.machine-snapshot/3",
    ] {
        let mut old_snapshot = machine.snapshot();
        old_snapshot.snapshot_version = old_version.to_owned();
        assert!(matches!(
            Machine::restore(old_snapshot),
            Err(CoreError::Validation(_))
        ));
    }
    let mut unsupported = machine.snapshot();
    unsupported.snapshot_version = "cymule.machine-snapshot/999".to_owned();
    assert!(matches!(
        Machine::restore(unsupported),
        Err(CoreError::Validation(_))
    ));

    let compaction = machine.compact_event_history(2).expect("prefix compacts");
    assert_eq!(compaction.compacted_events, 2);
    assert_eq!(compaction.retained_events, 2);
    assert_eq!(
        compaction.causal_frontier,
        BTreeSet::from([event_ids[1].clone()])
    );
    machine.verify_replay().expect("base plus suffix replays");
    let snapshot = machine.snapshot();
    assert!(snapshot.base.is_some());
    assert_eq!(snapshot.events.len(), 2);

    let mut restored = Machine::restore(snapshot.clone()).expect("suffix rehydrates");
    assert_eq!(restored.projection(), &expected_projection);
    assert_eq!(
        restored.submit(start).expect("old command receipt replays"),
        start_receipt
    );
    assert_eq!(restored.events().count(), 2);
    let second = restored
        .compact_event_history(1)
        .expect("later suffix prefix compacts");
    assert_eq!(second.compacted_events, 3);
    assert_eq!(second.retained_events, 1);
    Machine::restore(restored.snapshot())
        .expect("twice-compacted snapshot restores")
        .verify_replay()
        .expect("twice-compacted suffix replays");

    let mut tampered = snapshot.clone();
    tampered
        .base
        .as_mut()
        .expect("base exists")
        .projection_digest = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        Machine::restore(tampered),
        Err(CoreError::IdentityMismatch(_))
    ));

    for malformed_base in [
        {
            let mut value = snapshot.clone();
            value.base.as_mut().expect("base exists").prefix_digest = "x".repeat(71);
            value
        },
        {
            let mut value = snapshot.clone();
            value.base.as_mut().expect("base exists").prefix_digest = "sha256:0".to_owned();
            value
        },
        {
            let mut value = snapshot.clone();
            value
                .base
                .as_mut()
                .expect("base exists")
                .compacted_event_ids
                .clear();
            value
        },
        {
            let mut value = snapshot.clone();
            value
                .base
                .as_mut()
                .expect("base exists")
                .compacted_event_ids
                .insert(String::new());
            value
        },
    ] {
        assert!(matches!(
            Machine::restore(malformed_base),
            Err(CoreError::Validation(_))
        ));
    }

    let mut orphaned = snapshot;
    orphaned.events = vec![
        Event::new(
            "command:orphan-suffix".to_owned(),
            "hash:orphan-suffix".to_owned(),
            run_id.to_owned(),
            vec![format!("sha256:{}", "f".repeat(64))],
            BTreeSet::from(["fact:orphan-suffix".to_owned()]),
            BTreeSet::from(["fact:orphan-suffix".to_owned()]),
            Some("fact:orphan-suffix".to_owned()),
            EventPayload::FactRecorded {
                key: "orphan-suffix".to_owned(),
                value: "1".to_owned(),
            },
        )
        .expect("orphan event identity is valid"),
    ];
    assert!(matches!(
        Machine::restore(orphaned),
        Err(CoreError::Causal(_))
    ));
}

#[test]
fn structural_effect_identifiers_are_content_sensitive() {
    let args = ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: format!("sha256:{}", "a".repeat(64)),
        kind: "cymule.effect-args/1".to_owned(),
    };
    let first = effect_intent_id(
        "run:id",
        "main",
        "effect.capture",
        cymule_core::ROOT_SCOPE_ID,
        7,
        "primary",
        &args,
        "cymule.effect-schema/1",
    )
    .expect("intent hashes");
    let second = effect_intent_id(
        "run:id",
        "main",
        "effect.capture",
        cymule_core::ROOT_SCOPE_ID,
        7,
        "secondary",
        &args,
        "cymule.effect-schema/1",
    )
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
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            41,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Observe(WorldOutcome::Applied),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            42,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied,),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            43,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            &machine,
            4,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        ))
        .expect("effect prepares");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            44,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Prepare,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            46,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    let inexact_commit = Event::new(
        "command:inexact-obligations".to_owned(),
        "hash:inexact-obligations".to_owned(),
        run_id.to_owned(),
        vec![machine.projection().runs[run_id].last_event.clone()],
        BTreeSet::new(),
        BTreeSet::new(),
        None,
        EventPayload::ScopeCommitted {
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            obligations: Vec::new(),
        },
    )
    .expect("inexact event hashes");
    assert!(matches!(
        machine.append_event(inexact_commit),
        Err(CoreError::IllegalTransition(message))
            if message.contains("inexact effect obligation set")
    ));
    machine
        .submit(envelope(
            &machine,
            8,
            run_id,
            Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        ))
        .expect("scope commits and transfers the unresolved obligation");
    machine
        .submit(envelope(
            &machine,
            5,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        ))
        .expect("release authorizes");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            45,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::AuthorizeRelease,
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    machine
        .submit(envelope(
            &machine,
            6,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::StartDispatch,
            },
        ))
        .expect("dispatch starts");
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            60,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Observe(WorldOutcome::Unobserved),
            },
        )),
        Err(CoreError::IllegalTransition(_))
    ));
    assert!(matches!(
        machine.submit(envelope(
            &machine,
            61,
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
            7,
            run_id,
            Command::TransitionEffect {
                intent_id: intent_id.clone(),
                transition: EffectTransition::Observe(WorldOutcome::Unknown),
            },
        ))
        .expect("unknown observation applies");
    for (sequence, outcome) in [(70, WorldOutcome::Applied), (71, WorldOutcome::Unknown)] {
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
            Err(CoreError::IllegalTransition(_))
        ));
    }
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

#[test]
fn epoch_fences_prior_attempts() {
    let mut machine = Machine::new();
    let plan = insert_plan(&mut machine, candidate());
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
    let plan = insert_plan(&mut machine, candidate());
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
        BTreeSet::from(["fact:a".to_owned()]),
        BTreeSet::from(["fact:a".to_owned()]),
        Some("fact:a".to_owned()),
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
        BTreeSet::from(["fact:b".to_owned()]),
        BTreeSet::from(["fact:b".to_owned()]),
        Some("fact:b".to_owned()),
        EventPayload::FactRecorded {
            key: "b".to_owned(),
            value: "2".to_owned(),
        },
    )
    .expect("event hashes");
    let mut appended = Machine::new();
    appended
        .append_event(start.clone())
        .expect("trusted start appends");
    appended
        .append_event(start.clone())
        .expect("identical trusted event append is idempotent");
    assert_eq!(appended.events().count(), 1);
    appended
        .append_event(fact_a.clone())
        .expect("first child extends the Run frontier");
    assert!(matches!(
        appended.append_event(fact_b.clone()),
        Err(CoreError::Causal(message)) if message.contains("causal frontier")
    ));
    let duplicate_replay =
        Machine::replay([start.clone(), start.clone()]).expect("identical event set deduplicates");
    assert_eq!(duplicate_replay.runs.len(), 1);
    let mut tampered = fact_a.clone();
    tampered.event_id = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        tampered.verify(),
        Err(CoreError::IdentityMismatch(_))
    ));
    let mut duplicate_parent = fact_a.clone();
    duplicate_parent.parents.push(start.event_id.clone());
    duplicate_parent.parents.sort();
    assert!(matches!(
        duplicate_parent.verify(),
        Err(CoreError::Validation(message)) if message.contains("duplicate-free")
    ));
    let false_footprint = Event::new(
        "command:false-footprint".to_owned(),
        "hash:false-footprint".to_owned(),
        run_id.to_owned(),
        vec![fact_a.event_id.clone()],
        BTreeSet::new(),
        BTreeSet::new(),
        None,
        EventPayload::FactRecorded {
            key: "false-footprint".to_owned(),
            value: "1".to_owned(),
        },
    )
    .expect("self-consistent but semantically false Event constructs");
    assert!(matches!(
        appended.append_event(false_footprint),
        Err(CoreError::IdentityMismatch(message)) if message.contains("semantic footprint")
    ));
    let missing_parent = Event::new(
        "command:orphan".to_owned(),
        "hash:orphan".to_owned(),
        run_id.to_owned(),
        vec![format!("sha256:{}", "f".repeat(64))],
        BTreeSet::from(["fact:orphan".to_owned()]),
        BTreeSet::from(["fact:orphan".to_owned()]),
        Some("fact:orphan".to_owned()),
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

    let artifact = machine
        .put_artifact("test/value", b"retained".to_vec())
        .expect("Artifact stores");
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
        machine
            .submit(envelope(
                &machine,
                1,
                run_id,
                Command::StartRun {
                    plan_id: plan.plan_id,
                    binding_context: "binding:property".to_owned(),
                },
            ))
            .expect("run starts");
        let start = machine.events().next().expect("start event").clone();
        let mut facts = generated
            .iter()
            .enumerate()
            .map(|(index, (priority, value))| {
                let key = format!("property:{index}");
                let fact_key = format!("fact:{key}");
                let event = Event::new(
                    format!("command:property:{index}"),
                    format!("hash:property:{index}:{value}"),
                    run_id.to_owned(),
                    vec![start.event_id.clone()],
                    BTreeSet::from([fact_key.clone()]),
                    BTreeSet::from([fact_key.clone()]),
                    Some(fact_key),
                    EventPayload::FactRecorded {
                        key,
                        value: value.to_string(),
                    },
                )
                .expect("fact hashes");
                (*priority, index, event)
            })
            .collect::<Vec<_>>();

        let mut canonical = vec![start.clone()];
        canonical.extend(facts.iter().map(|(_, _, event)| event.clone()));
        facts.sort_by_key(|(priority, index, _)| (*priority, *index));
        let mut permuted = facts
            .into_iter()
            .map(|(_, _, event)| event)
            .collect::<Vec<_>>();
        permuted.insert(start_position.min(permuted.len()), start);

        let expected = Machine::replay(canonical).expect("canonical order replays");
        let actual = Machine::replay(permuted).expect("permuted order replays");
        prop_assert_eq!(
            actual.digest().expect("actual digest"),
            expected.digest().expect("expected digest")
        );
        prop_assert_eq!(actual.facts.len(), generated.len());
    }
}
