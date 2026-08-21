//! Fault-oriented durable single-domain contract tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, ComponentContract, Definition, DispatchPolicy,
    EffectContract, EffectProfile, EffectTransition, Expression, Machine, MutationKind, Operation,
    PlanCandidate, ROOT_SCOPE_ID, ReconciliationMode, Region, Step, WorldOutcome, effect_intent_id,
    seal_plan,
};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, DurableError, DurableResult,
    DurableStore, EffectDispatch, FrameState, JournalBatch, JournalRecord, MemoryStore,
    OutboxState, StoreCommit, StoredState, WaitActivation, WaitActivationSource, WaitCondition,
    WaitKind, WaitOwner, WaitState,
};
use cymule_runtime::{
    EXECUTION_BINDING_VERSION, ExecutionBinding, PLUGIN_VERSION, PluginManifest, PluginOperation,
};
use serde_json::json;

#[derive(Clone)]
struct LostCompactionReceiptStore {
    inner: MemoryStore,
    armed: Arc<AtomicBool>,
}

impl DurableStore for LostCompactionReceiptStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&cymule_durable::StoreHead>,
        batch: &cymule_durable::StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_commit(expected, batch)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated lost compaction receipt".to_owned(),
            ));
        }
        Ok(commit)
    }
}

fn seal_into(machine: &mut Machine, candidate: PlanCandidate) -> cymule_core::SealedPlan {
    let plan = seal_plan(candidate).expect("test Plan seals");
    machine
        .insert_plan(plan.clone())
        .expect("test Plan inserts");
    plan
}

fn machine_with_run() -> (Machine, String) {
    let mut machine = Machine::new();
    let plan = seal_into(
        &mut machine,
        PlanCandidate {
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
                    steps: vec![Step {
                        id: "wait.test".to_owned(),
                        operation: Operation::Wait {
                            wait: cymule_core::WaitSpec::Input {
                                correlation: "test".to_owned(),
                                schema: json!({}),
                            },
                            bind: None,
                        },
                    }],
                    result: Expression::Literal { value: json!(null) },
                },
            }],
            metadata: BTreeMap::new(),
        },
    );
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
    machine
        .put_artifact("test/input", b"durable input".to_vec())
        .expect("Continuation input stores");
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
    let plan = seal_into(
        &mut machine,
        PlanCandidate {
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
                    steps: vec![Step {
                        id: "effect.site".to_owned(),
                        operation: Operation::Effect {
                            effect: "example.effect".to_owned(),
                            input: Expression::Input,
                            occurrence: "primary".to_owned(),
                            bind: None,
                        },
                    }],
                    result: Expression::Literal { value: json!(null) },
                },
            }],
            metadata: BTreeMap::new(),
        },
    );
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
    machine
        .put_artifact("test/input", b"durable input".to_vec())
        .expect("Continuation input stores");
    let base = machine.clone();
    let args = machine
        .put_artifact("cymule.effect-args/1", b"{}".to_vec())
        .expect("Artifact stores");
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
            invocation_path: Vec::new(),
            definition_id: "main".to_owned(),
            region_path: Vec::new(),
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
    let mut effect_continuation = continuation(plan.plan_id);
    "run:effect-delta".clone_into(&mut effect_continuation.run_id);
    "binding:test".clone_into(&mut effect_continuation.binding_context);
    (
        base,
        machine,
        effect_continuation,
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
            invocation_path: Vec::new(),
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            input: cymule_core::artifact_ref("test/input", b"durable input")
                .expect("Continuation input reference derives"),
            region_path: Vec::new(),
            next_step: 1,
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

fn wait_owner() -> WaitOwner {
    WaitOwner {
        invocation_id: "main".to_owned(),
        definition_id: "main".to_owned(),
        site_id: "wait.test".to_owned(),
        region_path: Vec::new(),
        step_index: 0,
        bind: None,
    }
}

fn direct_run(candidate: PlanCandidate, binding: &ExecutionBinding) -> (Machine, Continuation) {
    let mut machine = Machine::new();
    let plan = seal_into(&mut machine, candidate);
    let binding_ref = machine
        .put_artifact(
            EXECUTION_BINDING_VERSION,
            binding.canonical_bytes().expect("binding encodes"),
        )
        .expect("binding Artifact stores");
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "run:direct:start".to_owned(),
            actor: "actor:test".to_owned(),
            run_id: "run:direct".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding_ref.artifact_id.clone(),
            },
        })
        .expect("direct Run starts");
    submit(
        &mut machine,
        "run:direct",
        "run:direct:begin",
        Command::BeginAttempt {
            attempt_id: "attempt:run:direct:0".to_owned(),
            continuation_id: "continuation:run:direct".to_owned(),
            occurrence_binding: binding_ref.artifact_id.clone(),
            epoch: 0,
        },
    );
    let input = machine
        .put_artifact("test/direct-input", b"direct input".to_vec())
        .expect("direct input stores");
    let continuation = Continuation {
        run_id: "run:direct".to_owned(),
        plan_id: plan.plan_id,
        binding_context: binding_ref.artifact_id,
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            invocation_path: Vec::new(),
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            input: input.clone(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: Some(input),
        wait_set: BTreeSet::new(),
        scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Running,
    };
    (machine, continuation)
}

fn component_checkpoint_fixture() -> (
    DurableCoordinator<MemoryStore>,
    Machine,
    Continuation,
    cymule_core::ArtifactRef,
    cymule_core::ArtifactRef,
) {
    let manifest = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "component-checkpoint-plugin".to_owned(),
        components: BTreeMap::from([(
            "example.component".to_owned(),
            PluginOperation {
                implementation_revision: "component-v1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    };
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("component binding seals");
    let mut machine = Machine::new();
    let plan = seal_into(
        &mut machine,
        PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "component_checkpoint".to_owned(),
            entry: "main".to_owned(),
            components: vec![ComponentContract {
                id: "example.component".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                requirements: BTreeMap::new(),
            }],
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: vec![Step {
                        id: "call.example".to_owned(),
                        operation: Operation::Call {
                            component: "example.component".to_owned(),
                            input: Expression::Input,
                            bind: Some("result".to_owned()),
                        },
                    }],
                    result: Expression::Binding {
                        name: "result".to_owned(),
                    },
                },
            }],
            metadata: BTreeMap::new(),
        },
    );
    let binding_ref = machine
        .put_artifact(
            EXECUTION_BINDING_VERSION,
            binding.canonical_bytes().unwrap(),
        )
        .unwrap();
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "component:start".to_owned(),
            actor: "actor:test".to_owned(),
            run_id: "run:component".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: binding_ref.artifact_id.clone(),
            },
        })
        .unwrap();
    submit(
        &mut machine,
        "run:component",
        "component:attempt",
        Command::BeginAttempt {
            attempt_id: "attempt:component:0".to_owned(),
            continuation_id: "continuation:component".to_owned(),
            occurrence_binding: binding_ref.artifact_id.clone(),
            epoch: 0,
        },
    );
    let frame_input = machine
        .put_artifact("cymule.input/1", b"{}".to_vec())
        .unwrap();
    let source = Continuation {
        run_id: "run:component".to_owned(),
        plan_id: plan.plan_id,
        binding_context: binding_ref.artifact_id,
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            invocation_path: Vec::new(),
            scope_id: ROOT_SCOPE_ID.to_owned(),
            input: frame_input.clone(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: Some(frame_input),
        wait_set: BTreeSet::new(),
        scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Running,
    };
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .unwrap()
        .initialize(&machine)
        .unwrap();
    coordinator.put_continuation(source.clone()).unwrap();
    let input = machine
        .put_artifact("cymule.component-input/1", b"{}".to_vec())
        .unwrap();
    let output = machine
        .put_artifact("cymule.component-output/1", br#"{"ok":true}"#.to_vec())
        .unwrap();
    let mut target = source;
    target.frames[0].next_step = 1;
    target.frames[0]
        .locals
        .insert("result".to_owned(), output.clone());
    (coordinator, machine, target, input, output)
}

#[test]
fn direct_run_creation_cannot_bypass_execution_binding_admission() {
    let manifest = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "direct-coordinator-plugin@1".to_owned(),
        components: BTreeMap::from([(
            "provided.component".to_owned(),
            PluginOperation {
                implementation_revision: "1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    };
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("binding seals");
    let candidate = PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "direct_admission".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "required.component".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            requirements: BTreeMap::new(),
        }],
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
    };
    let (machine, continuation) = direct_run(candidate, &binding);
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone()).expect("store opens");

    assert!(matches!(
        coordinator.create_run(&machine, continuation),
        Err(DurableError::Validation(message))
            if message.contains("required.component")
    ));
    assert!(
        DurableCoordinator::open(store)
            .expect("store reopens")
            .revision()
            .is_none()
    );
}

#[test]
fn public_coordinator_rejects_every_dangling_or_legacy_artifact_reference() {
    let (machine, plan_id) = machine_with_run();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let initial_revision = coordinator.revision().expect("revision").to_owned();
    let valid = continuation(plan_id);
    let mut missing = valid.frames[0].input.clone();
    missing.artifact_id = format!("sha256:{}", "f".repeat(64));
    let mut legacy = valid.frames[0].input.clone();
    legacy.identity_version = "cymule.artifact/1".to_owned();

    for invalid in [missing.clone(), legacy] {
        for site in ["frame", "state", "local"] {
            let mut proposed = valid.clone();
            match site {
                "frame" => proposed.frames[0].input = invalid.clone(),
                "state" => proposed.state = Some(invalid.clone()),
                "local" => {
                    proposed.frames[0]
                        .locals
                        .insert("invalid".to_owned(), invalid.clone());
                }
                _ => unreachable!(),
            }
            assert!(matches!(
                coordinator.put_continuation(proposed),
                Err(DurableError::Validation(_))
            ));
            assert_eq!(coordinator.revision(), Some(initial_revision.as_str()));
        }
    }

    let mut wrong_invocation = valid.clone();
    wrong_invocation.frames[0].invocation_id = "invocation:forged".to_owned();
    let mut wrong_epoch = valid.clone();
    wrong_epoch.epoch = 1;
    let mut wrong_scope = valid.clone();
    wrong_scope.scope_stack = vec!["scope:forged".to_owned()];
    let mut wrong_plan = valid.clone();
    wrong_plan.plan_id = "sha256:forged-plan".to_owned();
    for (case, proposed) in [
        ("invocation", wrong_invocation),
        ("epoch", wrong_epoch),
        ("scope", wrong_scope),
        ("plan", wrong_plan),
    ] {
        coordinator.put_continuation(proposed).expect_err(case);
        assert_eq!(coordinator.revision(), Some(initial_revision.as_str()));
    }

    coordinator
        .put_continuation(valid)
        .expect("valid Continuation persists");
    let valid_revision = coordinator.revision().expect("revision").to_owned();
    coordinator
        .register_wait(WaitCondition {
            wait_id: "wait:dangling".to_owned(),
            run_id: "run:durable".to_owned(),
            kind: WaitKind::Input {
                correlation: "dangling".to_owned(),
                schema: json!({}),
            },
            consume_once: true,
            owner: wait_owner(),
            state: WaitState::Pending,
            result: None,
        })
        .expect("wait registers");
    let wait_revision = coordinator.revision().expect("revision").to_owned();
    assert!(matches!(
        coordinator.complete_wait("wait:dangling", &missing),
        Err(DurableError::NotFound(_))
    ));
    assert_eq!(coordinator.revision(), Some(wait_revision.as_str()));

    assert_ne!(valid_revision, wait_revision);
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
fn frozen_wait_condition_requires_owner_when_bind_is_absent() {
    let fixture = include_str!("../../../tests/fixtures/wait-condition.json");
    let wait: WaitCondition = serde_json::from_str(fixture).expect("wait fixture deserializes");
    assert!(wait.owner.bind.is_none());
    let mut missing_owner: serde_json::Value =
        serde_json::from_str(fixture).expect("wait fixture parses");
    missing_owner
        .as_object_mut()
        .expect("wait fixture is an object")
        .remove("owner");
    assert!(serde_json::from_value::<WaitCondition>(missing_owner).is_err());
}

#[test]
fn wait_completion_survives_reopen_and_readies_the_continuation() {
    let (mut machine, plan_id) = machine_with_run();
    let result = machine
        .put_artifact("example/input", br#""accepted""#.to_vec())
        .expect("Artifact stores");
    let invalid_result = machine
        .put_artifact("example/input", b"42".to_vec())
        .expect("schema-invalid Artifact stores");
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    coordinator
        .put_continuation(continuation(plan_id))
        .expect("continuation persists");
    let revision = coordinator.revision().expect("revision").to_owned();
    let mut wrong_owner = wait_owner();
    wrong_owner.site_id = "wait.wrong".to_owned();
    assert!(matches!(
        coordinator.register_wait(WaitCondition {
            wait_id: "wait:wrong-owner".to_owned(),
            run_id: "run:durable".to_owned(),
            kind: WaitKind::Input {
                correlation: "approval".to_owned(),
                schema: json!({"type": "string"}),
            },
            consume_once: true,
            owner: wrong_owner,
            state: WaitState::Pending,
            result: None,
        }),
        Err(DurableError::Validation(_))
    ));
    assert_eq!(coordinator.revision(), Some(revision.as_str()));
    coordinator
        .register_wait(WaitCondition {
            wait_id: "wait:approval".to_owned(),
            run_id: "run:durable".to_owned(),
            kind: WaitKind::Input {
                correlation: "approval".to_owned(),
                schema: json!({"type": "string"}),
            },
            consume_once: true,
            owner: wait_owner(),
            state: WaitState::Pending,
            result: None,
        })
        .expect("wait registers");
    let before_invalid = coordinator.revision().expect("revision").to_owned();
    assert!(matches!(
        coordinator.complete_wait("wait:approval", &invalid_result),
        Err(DurableError::Contract(_))
    ));
    assert_eq!(coordinator.revision(), Some(before_invalid.as_str()));
    coordinator
        .complete_wait("wait:approval", &result)
        .expect("wait completes");
    coordinator
        .complete_wait("wait:approval", &result)
        .expect("completion retry is idempotent");

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let state = reopened.state().expect("state exists");
    assert_eq!(state.waits["wait:approval"].state, WaitState::Completed);
    assert_eq!(
        state.continuations["run:durable"].status,
        ContinuationStatus::Ready
    );
    assert!(state.continuations["run:durable"].wait_set.is_empty());
    assert!(
        state.continuations["run:durable"].frames[0]
            .locals
            .is_empty()
    );
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
                owner: wait_owner(),
                state: WaitState::Pending,
                result: None,
            })
            .expect("signal wait registers");
    }
    let result = machine
        .put_artifact("example/signal", b"approved".to_vec())
        .expect("Artifact stores");
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
    let result = machine
        .put_artifact("example/signal", b"payload".to_vec())
        .expect("Artifact stores");
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
                owner: wait_owner(),
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
    unrelated_machine
        .put_artifact("example/unrelated", b"unrelated".to_vec())
        .expect("Artifact stores");
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
        coordinator.complete_wait("wait:signal:one", &result),
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
            owner: wait_owner(),
            state: WaitState::Pending,
            result: None,
        })
        .expect("timer wait registers");
    let mut stale = DurableCoordinator::open(store).expect("stale view opens");
    let result = machine
        .put_artifact("example/timer", b"fired".to_vec())
        .expect("Artifact stores");
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
            owner: wait_owner(),
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
    let result = machine
        .put_artifact("example/signal", b"accepted".to_vec())
        .expect("Artifact stores");
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
fn stale_coordinator_and_lease_owner_fail_closed() {
    let (machine, plan_id) = machine_with_run();
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut stale = DurableCoordinator::open(store.clone()).expect("second view opens");
    current
        .put_continuation(continuation(plan_id.clone()))
        .expect("current writer commits");
    assert!(matches!(
        stale.put_continuation(continuation(plan_id)),
        Err(DurableError::Conflict { .. })
    ));

    let lease = current
        .acquire_lease("dispatch:partition/0", "worker:a", 10, 20)
        .expect("lease acquired");
    assert!(matches!(
        current.acquire_lease("dispatch:partition/0", "worker:b", 11, 20),
        Err(DurableError::Conflict { .. })
    ));
    assert_eq!(lease.owner, "worker:a");
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
    assert!(matches!(
        coordinator.checkpoint(&prepared, continuation.clone()),
        Err(DurableError::Validation(message))
            if message.contains("outside its atomic outbox boundary")
    ));
    assert!(coordinator.state().expect("state").outbox.is_empty());

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
        .checkpoint(&committed, continuation.clone())
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
    let result = observed
        .put_artifact("cymule.effect-result/1", b"result".to_vec())
        .expect("Artifact stores");
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
    let (mut coordinator, machine, target, input, output) = component_checkpoint_fixture();
    coordinator
        .checkpoint_component(&machine, target.clone(), &input, &output)
        .expect("derived occurrence and Continuation commit atomically");
    let state = coordinator.state().unwrap();
    let occurrence = state.component_occurrences.values().next().unwrap();
    assert_eq!(occurrence.plan_id, target.plan_id);
    assert_eq!(occurrence.invocation_id, "main");
    assert_eq!(occurrence.site_id, "call.example");
    assert_eq!(occurrence.step_index, 0);
    assert_eq!(occurrence.epoch, 0);
    assert_eq!(
        occurrence.continuation_digest,
        cymule_core::canonical_digest(&target).unwrap()
    );

    let mut forged = target;
    forged.frames[0].next_step = 0;
    assert!(matches!(
        coordinator.checkpoint_component(&machine, forged, &input, &output),
        Err(DurableError::Validation(_) | DurableError::IllegalTransition(_))
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
fn machine_history_compaction_reopens_and_replays_after_lost_receipt() {
    let (mut machine, plan_id) = machine_with_run();
    submit(
        &mut machine,
        "run:durable",
        "command:compaction:attempt",
        Command::BeginAttempt {
            attempt_id: "attempt:compaction".to_owned(),
            continuation_id: "continuation:compaction".to_owned(),
            occurrence_binding: "binding:worker/compaction@1".to_owned(),
            epoch: 0,
        },
    );
    submit(
        &mut machine,
        "run:durable",
        "command:compaction:yield",
        Command::YieldAttempt {
            attempt_id: "attempt:compaction".to_owned(),
            epoch: 0,
        },
    );
    submit(
        &mut machine,
        "run:durable",
        "command:compaction:epoch",
        Command::AdvanceEpoch,
    );
    let expected_projection = machine.projection().clone();
    let armed = Arc::new(AtomicBool::new(false));
    let store = LostCompactionReceiptStore {
        inner: MemoryStore::new(),
        armed: armed.clone(),
    };
    let mut coordinator = DurableCoordinator::open(store)
        .expect("coordinator opens")
        .initialize(&machine)
        .expect("state initializes");

    armed.store(true, Ordering::SeqCst);
    let error = coordinator
        .compact_history("compaction:events:1", 1)
        .expect_err("compaction acknowledgement is lost");
    assert!(
        matches!(&error, DurableError::Substrate(message) if message == "simulated lost compaction receipt"),
        "unexpected compaction error: {error:?}"
    );
    let store = coordinator.into_store();
    let mut reopened = DurableCoordinator::open(store).expect("compacted state reopens");
    let receipt =
        reopened.state().expect("state reads").history_compactions["compaction:events:1"].clone();
    assert_eq!(receipt.result.compacted_events, 3);
    assert_eq!(receipt.result.retained_events, 1);
    let restored = reopened.restore_machine().expect("suffix rehydrates");
    assert_eq!(restored.projection(), &expected_projection);
    restored.verify_replay().expect("base plus suffix replays");
    assert_eq!(
        reopened
            .compact_history("compaction:events:1", 1)
            .expect("lost receipt retry returns original"),
        receipt
    );

    let mut restored = reopened.restore_machine().expect("Machine restores");
    let events_before = restored.events().count();
    restored
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:start".to_owned(),
            actor: "test".to_owned(),
            run_id: "run:durable".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: restored.projection().runs["run:durable"]
                    .initial_plan
                    .clone(),
                binding_context: "binding:test/1".to_owned(),
            },
        })
        .expect("compacted command receipt replays");
    assert_eq!(restored.events().count(), events_before);
    let mut resumed = continuation(plan_id);
    resumed.epoch = 1;
    resumed.status = ContinuationStatus::Running;
    reopened
        .put_continuation(resumed.clone())
        .expect("resumed Continuation persists");
    submit(
        &mut restored,
        "run:durable",
        "command:compaction:attempt:2",
        Command::BeginAttempt {
            attempt_id: "attempt:compaction:2".to_owned(),
            continuation_id: "continuation:compaction".to_owned(),
            occurrence_binding: "binding:worker/compaction@2".to_owned(),
            epoch: 1,
        },
    );
    reopened
        .checkpoint(&restored, resumed)
        .expect("new suffix Event persists");
    let second = reopened
        .compact_history("compaction:events:2", 1)
        .expect("later prefix compacts");
    assert_eq!(
        second.parent_compaction.as_deref(),
        Some("compaction:events:1")
    );
    assert_eq!(second.result.compacted_events, 4);
    assert_eq!(second.result.retained_events, 1);
    reopened
        .restore_machine()
        .expect("cumulative base restores")
        .verify_replay()
        .expect("cumulative base plus suffix replays");
}

#[test]
fn stale_history_compaction_loses_without_changing_committed_base() {
    let (mut machine, _) = machine_with_run();
    submit(
        &mut machine,
        "run:durable",
        "command:compaction:epoch",
        Command::AdvanceEpoch,
    );
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("current opens")
        .initialize(&machine)
        .expect("state initializes");
    let mut stale = DurableCoordinator::open(store).expect("stale opens");
    let committed = current
        .compact_history("compaction:current", 1)
        .expect("current compacts");
    assert!(matches!(
        stale.compact_history("compaction:stale", 1),
        Err(DurableError::Conflict { .. })
    ));
    let current_state = current.state().expect("current state remains valid");
    assert_eq!(current_state.history_compactions.len(), 1);
    assert_eq!(
        current_state.history_compactions["compaction:current"],
        committed
    );
    current
        .restore_machine()
        .expect("committed base restores")
        .verify_replay()
        .expect("committed suffix replays");
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
    let result = proposed
        .put_artifact("example/result", b"result".to_vec())
        .expect("Artifact stores");
    proposed
        .put_artifact("example/unrelated", b"unrelated".to_vec())
        .expect("Artifact stores");
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
        valid
            .put_artifact("example/result", b"result".to_vec())
            .expect("Artifact stores"),
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
