//! Bounded parked-index and replaceable wait-source driver tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec};
use cymule_durable::{
    Continuation, ContinuationStatus, DriveOutcome, DurableError, DurableResult, DurableState,
    FrameState, MemoryStore, ParkedWaitIndex, ResumableRuntime, WaitActivationSource,
    WaitCondition, WaitDelivery, WaitKind, WaitSourceDriver, WaitState,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse,
    RuntimeError, RuntimeResult,
};
use serde_json::json;

struct EmptyPlugin;

fn open_runtime<S: cymule_durable::DurableStore, P: PluginHost>(
    store: S,
    mut plugin: P,
) -> cymule_durable::DurableResult<ResumableRuntime<S, P>> {
    let manifest = plugin
        .describe()
        .map_err(|error| DurableError::Substrate(error.to_string()))?;
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    )
    .map_err(|error| DurableError::Validation(error.to_string()))?;
    ResumableRuntime::open(store, plugin, binding)
}

impl PluginHost for EmptyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "wait-source-test@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::new(),
                },
            }),
            request => Err(RuntimeError::PluginDefect {
                code: "unexpected_test_request".to_owned(),
                message: format!("unexpected wait-source request: {request:?}"),
            }),
        }
    }
}

struct RedeliveringDriver {
    delivery: WaitDelivery,
    lose_first_ack: bool,
    acknowledgements: usize,
}

impl WaitSourceDriver for RedeliveringDriver {
    fn receive(
        &mut self,
        index: &ParkedWaitIndex,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        if self.acknowledgements > 0 {
            return Ok(None);
        }
        if index
            .select(&self.delivery.source, max_targets)?
            .wait_ids
            .is_empty()
        {
            // A committed activation disappears from the parked index. The
            // transport still redelivers the identical delivery until ack.
            return Ok(Some(self.delivery.clone()));
        }
        Ok(Some(self.delivery.clone()))
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        if activation_id != self.delivery.activation_id {
            return Err(DurableError::Validation(
                "driver acknowledged the wrong activation".to_owned(),
            ));
        }
        if self.lose_first_ack {
            self.lose_first_ack = false;
            return Err(DurableError::Substrate(
                "simulated lost source acknowledgement".to_owned(),
            ));
        }
        self.acknowledgements += 1;
        Ok(())
    }
}

fn signal_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "wait_source_signal".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "wait.signal".to_owned(),
                    operation: Operation::Wait {
                        wait: WaitSpec::Signal {
                            key: "signal:continue".to_owned(),
                            consume_once: true,
                        },
                        bind: Some("signal_result".to_owned()),
                    },
                }],
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn continuation(wait_ids: &[&str]) -> Continuation {
    Continuation {
        run_id: "run:index".to_owned(),
        plan_id: "sha256:plan".to_owned(),
        binding_context: "binding:test".to_owned(),
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            input: cymule_core::ArtifactRef {
                identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
                artifact_id: format!("sha256:{}", "0".repeat(64)),
                kind: "test/input".to_owned(),
            },
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: None,
        wait_set: wait_ids
            .iter()
            .map(|wait_id| (*wait_id).to_owned())
            .collect(),
        scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Waiting,
    }
}

#[test]
fn parked_index_selects_bounded_signal_and_exact_timer_candidates() {
    let wait_ids = [
        "wait:broadcast:a",
        "wait:broadcast:b",
        "wait:once:a",
        "wait:once:b",
        "wait:timer:a",
        "wait:timer:b",
    ];
    let mut state = DurableState::new(cymule_core::Machine::new().snapshot());
    state
        .continuations
        .insert("run:index".to_owned(), continuation(&wait_ids));
    for (wait_id, consume_once) in [
        ("wait:broadcast:a", false),
        ("wait:broadcast:b", false),
        ("wait:once:a", true),
        ("wait:once:b", true),
    ] {
        state.waits.insert(
            wait_id.to_owned(),
            WaitCondition {
                wait_id: wait_id.to_owned(),
                run_id: "run:index".to_owned(),
                kind: WaitKind::Signal {
                    key: "signal:batch".to_owned(),
                },
                consume_once,
                result_binding: None,
                state: WaitState::Pending,
                result: None,
            },
        );
    }
    for wait_id in ["wait:timer:a", "wait:timer:b"] {
        state.waits.insert(
            wait_id.to_owned(),
            WaitCondition {
                wait_id: wait_id.to_owned(),
                run_id: "run:index".to_owned(),
                kind: WaitKind::Timer {
                    timer_id: "timer:batch".to_owned(),
                },
                consume_once: false,
                result_binding: None,
                state: WaitState::Pending,
                result: None,
            },
        );
    }

    let index = ParkedWaitIndex::rebuild(&state).expect("index rebuilds");
    let signal = WaitActivationSource::Signal {
        key: "signal:batch".to_owned(),
    };
    let selected = index.select(&signal, 2).expect("signal selects");
    assert_eq!(
        selected.wait_ids,
        BTreeSet::from(["wait:broadcast:a".to_owned(), "wait:once:a".to_owned()])
    );
    assert_eq!(selected.remaining, 2);
    index
        .validate_delivery(&WaitDelivery {
            activation_id: "delivery:signal".to_owned(),
            source: signal.clone(),
            wait_ids: selected.wait_ids,
            value: json!({"ok": true}),
        })
        .expect("selected signal targets validate");
    assert!(
        index
            .validate_delivery(&WaitDelivery {
                activation_id: "delivery:invalid".to_owned(),
                source: signal,
                wait_ids: BTreeSet::from(["wait:once:a".to_owned(), "wait:once:b".to_owned(),]),
                value: json!(null),
            })
            .is_err()
    );

    let timer = index
        .select(
            &WaitActivationSource::Timer {
                timer_id: "timer:batch".to_owned(),
            },
            8,
        )
        .expect("timer selects");
    assert_eq!(timer.wait_ids, BTreeSet::from(["wait:timer:a".to_owned()]));
    assert_eq!(timer.remaining, 1);
    assert!(
        index
            .select(
                &WaitActivationSource::Signal {
                    key: "signal:batch".to_owned()
                },
                0
            )
            .is_err()
    );
}

#[test]
fn wait_source_ack_loss_redelivers_one_committed_activation_after_reopen() {
    let run_id = "run:wait-source";
    let mut runtime = open_runtime(MemoryStore::new(), EmptyPlugin).expect("runtime opens");
    let DriveOutcome::Suspended { wait_id } = runtime
        .start(signal_candidate(), &json!({"value": 7}), run_id)
        .expect("Run parks")
    else {
        panic!("Run should park");
    };
    let mut driver = RedeliveringDriver {
        delivery: WaitDelivery {
            activation_id: "delivery:continue:1".to_owned(),
            source: WaitActivationSource::Signal {
                key: "signal:continue".to_owned(),
            },
            wait_ids: BTreeSet::from([wait_id]),
            value: json!({"delivered": true}),
        },
        lose_first_ack: true,
        acknowledgements: 0,
    };
    assert!(runtime.drive_wait_source(&mut driver, 16).is_err());
    assert_eq!(
        runtime
            .coordinator()
            .state()
            .expect("state")
            .wait_activations
            .len(),
        1
    );

    let (store, _) = runtime.into_parts();
    let mut reopened = open_runtime(store, EmptyPlugin).expect("runtime reopens");
    assert_eq!(
        reopened
            .drive_wait_source(&mut driver, 16)
            .expect("redelivery replays"),
        Some(BTreeSet::from([run_id.to_owned()]))
    );
    assert_eq!(driver.acknowledgements, 1);
    let DriveOutcome::Completed(result) = reopened.resume(run_id).expect("Run resumes") else {
        panic!("Run should complete");
    };
    assert_eq!(result.value, json!({"value": 7}));
    assert_eq!(
        reopened
            .drive_wait_source(&mut driver, 16)
            .expect("acknowledged source is empty"),
        None
    );
}
