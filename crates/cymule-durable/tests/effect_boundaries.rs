//! Public Effect settlement, release, and terminal-receipt recovery.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

#[path = "support/interleaving_store.rs"]
mod interleaving_store;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cymule_core::{
    DispatchPolicy, EffectContract, EffectProfile, Expression, MutationKind, Operation,
    PlanCandidate, ReconciliationMode, ReconciliationResolution, Step, WorldOutcome,
};
use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableResponse, DurableStore,
    DurableStoreControl, HistoryCompactionKind, HistoryCompactionRequest, MemoryStore, OutboxState,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::{Value, json};

use interleaving_store::{InterleavingStore, activate_unrelated_signal, park_unrelated_signal};

#[derive(Default)]
struct Calls {
    prepares: AtomicUsize,
    dispatches: AtomicUsize,
    reconciliations: AtomicUsize,
}

#[derive(Clone)]
struct EffectPlugin {
    calls: Arc<Calls>,
    outcome: WorldOutcome,
    value: Option<Value>,
}

impl PluginHost for EffectPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: manifest(),
            }),
            PluginRequest::PrepareEffect { .. } => {
                self.calls.prepares.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::DispatchEffect { attempt, .. } => {
                self.calls.dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::EffectResult {
                    attempt,
                    outcome: self.outcome,
                    value: self.value.clone(),
                })
            }
            PluginRequest::ReconcileEffect { attempt, .. } => {
                self.calls.reconciliations.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::ReconciliationResult {
                    attempt,
                    resolution: ReconciliationResolution::ResolvedApplied,
                    value: self.value.clone(),
                })
            }
            other @ PluginRequest::Call { .. } => Err(RuntimeError::PluginDefect {
                code: "unexpected_effect_test_request".to_owned(),
                message: format!("unexpected request {other:?}"),
            }),
        }
    }
}

fn manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "durable-effect-boundaries@1".to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::from([(
            "test.effect".to_owned(),
            PluginEffect {
                implementation_revision: "1".to_owned(),
                can_reconcile: true,
            },
        )]),
    }
}

fn binding() -> ExecutionBinding {
    ExecutionBinding::for_local_process(&manifest(), format!("sha256:{}", "6".repeat(64)))
        .expect("test provider binding derives")
}

fn candidate(dispatch: DispatchPolicy, schema: Value) -> PlanCandidate {
    let mut candidate = support::identity_candidate("effect-boundaries");
    candidate.effects.push(EffectContract {
        id: "test.effect".to_owned(),
        input_schema: json!({}),
        output_schema: schema,
        profile: EffectProfile {
            mutation: MutationKind::Mutating,
            dispatch,
            reconciliation: ReconciliationMode::Queryable,
            keyed_idempotency: true,
            irreversible: false,
        },
        requirements: BTreeMap::new(),
    });
    candidate.definitions[0].body.steps.push(Step {
        id: "effect.test".to_owned(),
        operation: Operation::Effect {
            effect: "test.effect".to_owned(),
            input: Expression::Input,
            occurrence: "once".to_owned(),
            bind: None,
        },
    });
    candidate
}

fn start(run_id: &str, dispatch: DispatchPolicy, schema: Value) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate: candidate(dispatch, schema),
        input: json!({"run": run_id}),
        execution: support::execution(run_id),
    }
}

#[test]
fn scope_commit_effect_settles_then_completes_from_fresh_execution_state() {
    let run_id = "run:effect:scope-commit";
    let calls = Arc::new(Calls::default());
    let mut runtime = support::open_control(
        MemoryStore::new(),
        EffectPlugin {
            calls: calls.clone(),
            outcome: WorldOutcome::Applied,
            value: None,
        },
        binding(),
    )
    .expect("runtime opens");
    let result = runtime
        .submit(start(run_id, DispatchPolicy::OnScopeCommit, json!({})))
        .expect("scope-gated dispatch and Run completion commit");
    assert_eq!(
        support::expect_completed_value(result),
        json!({"run": run_id})
    );
    assert_eq!(calls.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(calls.dispatches.load(Ordering::SeqCst), 1);
    let (mut store, _) = runtime.into_parts();
    let state = store
        .load_full_audit()
        .expect("state fully audits")
        .expect("state exists")
        .state;
    let dispatch = state.outbox.values().next().expect("Effect retained");
    assert_eq!(dispatch.state, OutboxState::Applied);
    let reference = dispatch
        .result
        .as_ref()
        .expect("Applied always retains a canonical result");
    let artifact = state
        .machine
        .artifacts
        .iter()
        .find(|record| record.reference == *reference)
        .expect("result material retained");
    assert_eq!(artifact.bytes, b"null");
}

#[test]
fn explicit_release_completes_and_replays_without_a_second_dispatch() {
    let run_id = "run:effect:explicit";
    let calls = Arc::new(Calls::default());
    let mut runtime = support::open_control(
        MemoryStore::new(),
        EffectPlugin {
            calls: calls.clone(),
            outcome: WorldOutcome::Applied,
            value: Some(json!("settled")),
        },
        binding(),
    )
    .expect("runtime opens");
    let response = runtime
        .submit(start(run_id, DispatchPolicy::Explicit, json!({})))
        .expect("Run yields its release boundary");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::ReleaseRequired { intent_ids },
    } = response
    else {
        panic!("explicit Effect was not retained for release")
    };
    assert_eq!(calls.dispatches.load(Ordering::SeqCst), 0);
    let intent_id = intent_ids
        .into_iter()
        .next()
        .expect("one explicit intent exists");
    let (store, plugin) = runtime.into_parts();
    let mut runtime = support::open_control(store, plugin, binding()).expect("runtime reopens");
    let release = DurableCommand::ReleaseEffect {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        intent_id,
        execution: support::execution(run_id),
    };
    let response = runtime
        .submit(release.clone())
        .expect("release completes the Run");
    assert_eq!(
        support::expect_completed_value(response),
        json!({"run": run_id})
    );
    let (store, plugin) = runtime.into_parts();
    let mut runtime =
        support::open_control(store, plugin, binding()).expect("terminal runtime reopens");
    runtime.submit(release).expect("terminal release replays");
    assert_eq!(calls.dispatches.load(Ordering::SeqCst), 1);
    let (mut store, _) = runtime.into_parts();
    store
        .load_full_audit()
        .expect("explicit release state audits");
}

#[test]
fn unrelated_writer_after_effect_claim_does_not_fabricate_unknown_before_dispatch() {
    let run_id = "run:effect:claim-interleaving";
    let unrelated_run = "run:effect:claim-interleaving:unrelated";
    let unrelated_signal = "signal:effect:claim-interleaving:unrelated";
    let calls = Arc::new(Calls::default());
    let plugin = EffectPlugin {
        calls: calls.clone(),
        outcome: WorldOutcome::Applied,
        value: Some(json!("settled")),
    };
    let mut runtime =
        support::open_control(MemoryStore::new(), plugin, binding()).expect("runtime opens");
    let response = runtime
        .submit(start(run_id, DispatchPolicy::Explicit, json!({})))
        .expect("Run yields its release boundary");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::ReleaseRequired { intent_ids },
    } = response
    else {
        panic!("explicit Effect was not retained for release")
    };
    let intent_id = intent_ids
        .into_iter()
        .next()
        .expect("one explicit intent exists");
    let (store, plugin) = runtime.into_parts();
    let (store, unrelated_wait) = park_unrelated_signal(store, unrelated_run, unrelated_signal)
        .expect("unrelated writer Run parks");
    let store = InterleavingStore::new(store, 1, move |store| {
        activate_unrelated_signal(
            store,
            "activation:effect:claim-interleaving:unrelated",
            unrelated_signal,
            unrelated_wait,
        )
        .map(|_| ())
    });
    let mut runtime =
        support::open_control(store, plugin, binding()).expect("runtime reopens with interleaving");
    let response = runtime
        .submit(DurableCommand::ReleaseEffect {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            intent_id,
            execution: support::execution(run_id),
        })
        .expect("acknowledged Effect claim survives the unrelated writer");
    assert_eq!(
        support::expect_completed_value(response),
        json!({"run": run_id})
    );
    assert_eq!(calls.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(calls.reconciliations.load(Ordering::SeqCst), 0);
    let (store, _) = runtime.into_parts();
    let mut store = store.into_inner();
    let state = store
        .load_full_audit()
        .expect("interleaved Effect state fully audits")
        .expect("interleaved Effect state exists")
        .state;
    let dispatch = state
        .outbox
        .values()
        .find(|dispatch| dispatch.run_id == run_id)
        .expect("target Effect remains retained");
    assert_eq!(dispatch.state, OutboxState::Applied);
}

#[test]
fn schema_invalid_missing_applied_value_retains_unknown_instead_of_false_success() {
    let run_id = "run:effect:invalid-null";
    let calls = Arc::new(Calls::default());
    let mut runtime = support::open_control(
        MemoryStore::new(),
        EffectPlugin {
            calls: calls.clone(),
            outcome: WorldOutcome::Applied,
            value: None,
        },
        binding(),
    )
    .expect("runtime opens");
    let result = runtime
        .submit(start(
            run_id,
            DispatchPolicy::OnScopeCommit,
            json!({"type": "string"}),
        ))
        .expect("invalid provider output becomes an unknown-world boundary");
    assert!(matches!(
        result,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::ReconciliationRequired { .. }
        }
    ));
    let (mut store, _) = runtime.into_parts();
    let state = store
        .load_full_audit()
        .expect("unknown state fully audits")
        .expect("state exists")
        .state;
    let dispatch = state.outbox.values().next().expect("Effect retained");
    assert_eq!(dispatch.state, OutboxState::Unknown);
    assert!(dispatch.result.is_none());
    assert_eq!(calls.dispatches.load(Ordering::SeqCst), 1);
}

#[test]
fn applied_null_resolution_after_cancellation_reopens_as_an_exact_typed_receipt() {
    let run_id = "run:effect:cancel-resolve";
    let calls = Arc::new(Calls::default());
    let plugin = EffectPlugin {
        calls: calls.clone(),
        outcome: WorldOutcome::Unknown,
        value: None,
    };
    let mut runtime =
        support::open_control(MemoryStore::new(), plugin, binding()).expect("runtime opens");
    runtime
        .submit(start(run_id, DispatchPolicy::OnScopeCommit, json!({})))
        .expect("Run yields Unknown");
    let (mut store, plugin) = runtime.into_parts();
    let dispatch = store
        .load_full_audit()
        .expect("unknown state audits")
        .expect("state exists")
        .state
        .outbox
        .into_values()
        .next()
        .expect("Effect retained");
    let mut control = DurableStoreControl::open(store).expect("store-only control reopens");
    control
        .submit(DurableCommand::CancelRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            cancellation_id: "cancel:effect-resolution".to_owned(),
            reason: json!("stop executing"),
        })
        .expect("cancellation retains unknown-world work");
    let mut runtime = support::open_control(control.into_store(), plugin, binding())
        .expect("resolution runtime reopens");
    let resolution = DurableCommand::ResolveEffect {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        resolution_id: "resolve:effect:null".to_owned(),
        run_id: run_id.to_owned(),
        intent_id: dispatch.intent_id,
        execution_binding: dispatch.execution_binding,
        occurrence_binding: dispatch.occurrence_binding,
        claim_owner: dispatch.claim_owner.expect("original claim retained"),
        claim_epoch: dispatch.claim_epoch,
        resolution: ReconciliationResolution::ResolvedApplied,
        value: None,
    };
    let result = runtime
        .submit(resolution.clone())
        .expect("provider-linearized resolution commits");
    let DurableResponse::EffectResolved { receipt } = result else {
        panic!("resolution returned another response")
    };
    assert_eq!(receipt.actual_value, Some(Value::Null));
    assert!(receipt.result.is_some());
    let (mut store, _) = runtime.into_parts();
    store.load_full_audit().expect("typed resolution audits");
    let expected_revision = store
        .load_head()
        .expect("head reads")
        .expect("head exists")
        .revision;
    let mut reads = DurableStoreControl::open(store).expect("read-only authority reopens");
    reads
        .compact_machine_history(&HistoryCompactionRequest {
            compaction_id: "history:effect-resolution".to_owned(),
            expected_revision,
            kind: HistoryCompactionKind::EventPrefix,
            requested_suffix: 0,
        })
        .expect("resolved Effect history compacts through the public maintenance capability");
    let mut reads =
        DurableStoreControl::open(reads.into_store()).expect("cold receipt authority reopens");
    let replay = reads
        .replay_effect_resolution(&resolution)
        .expect("exact receipt replays without provider")
        .expect("receipt exists");
    assert_eq!(replay, DurableResponse::EffectResolved { receipt });
    assert_eq!(calls.dispatches.load(Ordering::SeqCst), 1);
    assert_eq!(calls.reconciliations.load(Ordering::SeqCst), 1);
}
