//! Public execution-current-head ownership conformance.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

#[path = "support/ownership_store.rs"]
mod ownership_store;

#[path = "support/interleaving_store.rs"]
mod interleaving_store;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cymule_core::{
    COMPONENT_OUTPUT_ARTIFACT_KIND, ComponentContract, Definition, Expression, Operation,
    PlanCandidate, Region, Step,
};
use cymule_durable::{
    ComponentOccurrence, ComponentOccurrenceState, ComponentOutcome, DURABLE_CONTROL_VERSION,
    DurableCommand, DurableError, DurableResponse, DurableResult, DurableRunCurrent,
    DurableRunItem, DurableRunItemSelector, DurableStore, DurableStoreControl,
    MAX_DURABLE_QUERY_PAGE_BYTES, MemoryStore, OperationAttempt, OperationAttemptState,
};
use cymule_durable_protocol::{ContinuationStatus, ExecutionClaimRequest};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::{Value, json};

use interleaving_store::{InterleavingStore, activate_unrelated_signal, park_unrelated_signal};
use ownership_store::ReceiptLossStore;

#[derive(Clone)]
struct CountingPlugin {
    calls: Arc<AtomicUsize>,
}

impl PluginHost for CountingPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: manifest(),
            }),
            PluginRequest::Call { component, input } if component == "test.echo" => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::CallResult { value: input })
            }
            other => Err(RuntimeError::PluginDefect {
                code: "unexpected_execution_ownership_request".to_owned(),
                message: format!("unexpected request: {other:?}"),
            }),
        }
    }
}

fn manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "durable-execution-ownership@1".to_owned(),
        components: BTreeMap::from([(
            "test.echo".to_owned(),
            PluginOperation {
                implementation_revision: "1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    }
}

fn binding() -> ExecutionBinding {
    ExecutionBinding::for_local_process(
        &manifest(),
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    )
    .expect("test binding derives")
}

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "execution_current_head".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.echo".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            output_artifact_kind: COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
            requirements: BTreeMap::new(),
        }],
        effects: Vec::new(),
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
                        bind: Some("result".to_owned()),
                    },
                }],
                result: Expression::Binding {
                    name: "result".to_owned(),
                },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn invoked_definition(id: &str, target: &str) -> Definition {
    Definition {
        id: id.to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: vec![Step {
                id: format!("invoke.{target}"),
                operation: Operation::Invoke {
                    definition: target.to_owned(),
                    input: Expression::Input,
                    bind: Some("result".to_owned()),
                },
            }],
            result: Expression::Binding {
                name: "result".to_owned(),
            },
        },
    }
}

fn in_scope(region: Region, site: &str) -> Region {
    Region {
        steps: vec![Step {
            id: site.to_owned(),
            operation: Operation::Scope {
                body: Box::new(region),
                bind: Some("scoped".to_owned()),
            },
        }],
        result: Expression::Binding {
            name: "scoped".to_owned(),
        },
    }
}

fn structured_candidate(
    scope_before_invoke: bool,
    scope_after_invoke: bool,
    wait: bool,
) -> PlanCandidate {
    let mut plan = candidate();
    let mut leaf = plan.definitions.remove(0);
    "leaf".clone_into(&mut leaf.id);
    if wait {
        leaf.body.steps.insert(
            0,
            Step {
                id: "wait.structured".to_owned(),
                operation: Operation::Wait {
                    wait: cymule_core::WaitSpec::Signal {
                        key: "signal:structured".to_owned(),
                        consume_once: true,
                    },
                    bind: Some("delivered".to_owned()),
                },
            },
        );
        let Operation::Call { input, .. } = &mut leaf.body.steps[1].operation else {
            panic!("fixture has a Call")
        };
        *input = Expression::Binding {
            name: "delivered".to_owned(),
        };
    }
    if scope_after_invoke {
        leaf.body = in_scope(leaf.body, "scope.leaf");
    }
    let mut middle = invoked_definition("middle", "leaf");
    if scope_before_invoke {
        middle.body = in_scope(middle.body, "scope.middle");
    }
    plan.definitions = vec![invoked_definition("main", "middle"), middle, leaf];
    plan
}

#[test]
fn nested_invocations_and_scope_orderings_preserve_one_component_attempt() {
    for (label, before, after) in [
        ("plain", false, false),
        ("scope-invoke", true, false),
        ("invoke-scope", false, true),
    ] {
        let run_id = format!("run:structured:{label}");
        let calls = Arc::new(AtomicUsize::new(0));
        let mut runtime = support::open_control(
            MemoryStore::new(),
            CountingPlugin {
                calls: calls.clone(),
            },
            binding(),
        )
        .expect("runtime opens");
        let result = runtime
            .submit(DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.clone(),
                candidate: structured_candidate(before, after, false),
                input: json!({"scope": label}),
                execution: support::execution(&run_id),
            })
            .expect("structured invocation completes");
        assert_eq!(
            support::expect_completed_value(result),
            json!({"scope": label})
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let (mut store, _) = runtime.into_parts();
        let audited = store
            .load_full_audit()
            .expect("full audit succeeds")
            .expect("state exists");
        assert_eq!(audited.state.component_occurrences.len(), 1);
        assert_eq!(audited.state.operation_attempts.len(), 1);
    }
}

#[test]
fn nested_scope_invocation_wait_reopens_and_resumes_with_inherited_scope() {
    let run_id = "run:structured:wait";
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = support::open_control(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )
    .expect("runtime opens");
    let response = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: structured_candidate(true, true, true),
            input: json!("initial"),
            execution: support::execution(run_id),
        })
        .expect("nested Wait parks");
    let DurableResponse::RunBoundary {
        boundary: cymule_durable::DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("nested invocation did not park")
    };
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (store, _) = runtime.into_parts();
    let mut control = DurableStoreControl::open(store).expect("store-only authority reopens");
    control
        .submit(DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: "activation:structured:wait".to_owned(),
            source: cymule_durable_protocol::WaitActivationSource::Signal {
                key: "signal:structured".to_owned(),
            },
            wait_ids: std::collections::BTreeSet::from([wait_id]),
            value: json!({"resumed": true}),
        })
        .expect("nested activation commits");
    let mut runtime = support::open_control(
        control.into_store(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )
    .expect("runtime reopens");
    let response = runtime
        .submit(DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            execution: support::execution(run_id),
        })
        .expect("nested invocation resumes");
    assert_eq!(
        support::expect_completed_value(response),
        json!({"resumed": true})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (mut store, _) = runtime.into_parts();
    store
        .load_full_audit()
        .expect("nested final state fully audits");
}

#[test]
fn resume_claim_ack_survives_an_unrelated_writer_before_drive() -> DurableResult<()> {
    let run_id = "run:resume-claim-interleaving";
    let unrelated_run = "run:resume-claim-interleaving:unrelated";
    let unrelated_signal = "signal:resume-claim-interleaving:unrelated";
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = support::open_control(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    let response = runtime.submit(DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate: structured_candidate(false, false, true),
        input: json!({"resume": "target"}),
        execution: support::execution(run_id),
    })?;
    let DurableResponse::RunBoundary {
        boundary: cymule_durable::DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("target Resume Run did not park")
    };
    let (store, _) = runtime.into_parts();
    let store = activate_unrelated_signal(
        store,
        "activation:resume-claim-interleaving:target",
        "signal:structured",
        wait_id,
    )?;
    let (store, unrelated_wait) = park_unrelated_signal(store, unrelated_run, unrelated_signal)?;
    let store = InterleavingStore::new(store, 0, move |store| {
        activate_unrelated_signal(
            store,
            "activation:resume-claim-interleaving:unrelated",
            unrelated_signal,
            unrelated_wait,
        )
        .map(|_| ())
    });
    let mut runtime = support::open_control(
        store,
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    let response = runtime.submit(resume_command(run_id, support::execution(run_id)))?;
    assert_eq!(
        support::expect_completed_value(response),
        json!({"unrelated": "advanced"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (store, _) = runtime.into_parts();
    let store = store.into_inner();
    assert_eq!(
        read_ownership(&store, run_id)?.current.continuation_status,
        ContinuationStatus::Completed
    );
    assert_full_audit(&store)
}

#[test]
fn stale_clock_head_fails_before_store_admission_and_provider_io() {
    let run_id = "run:stale-clock";
    let stale = support::execution(run_id);
    let current = support::execution(run_id);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = support::open_control(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )
    .expect("runtime opens");
    let command = |execution| DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate: candidate(),
        input: json!({"request": "echo"}),
        execution,
    };
    assert!(matches!(
        runtime.submit(command(stale)),
        Err(DurableError::Conflict { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let completed = runtime
        .submit(command(current))
        .expect("current Clock head admits execution");
    assert_eq!(
        support::expect_completed_value(completed),
        json!({"request": "echo"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct CallbackPlugin<F> {
    calls: Arc<AtomicUsize>,
    on_call: F,
}

impl<F> PluginHost for CallbackPlugin<F>
where
    F: FnMut(Value) -> RuntimeResult<PluginResponse>,
{
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: manifest(),
            }),
            PluginRequest::Call { component, input } if component == "test.echo" => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                (self.on_call)(input)
            }
            other => Err(RuntimeError::plugin_defect(format!(
                "unexpected ownership callback request: {other:?}"
            ))),
        }
    }
}

#[derive(Debug, PartialEq)]
struct OwnershipSnapshot {
    revision: String,
    current: DurableRunCurrent,
    occurrence: ComponentOccurrence,
    attempts: Vec<OperationAttempt>,
}

fn checked_query(
    control: &mut DurableStoreControl<MemoryStore>,
    command: &DurableCommand,
) -> DurableResult<DurableResponse> {
    let response = control.submit(command.clone())?;
    response.verify_query_for(command)?;
    Ok(response)
}

fn read_item(
    control: &mut DurableStoreControl<MemoryStore>,
    run_id: &str,
    revision: &str,
    selector: DurableRunItemSelector,
) -> DurableResult<DurableRunItem> {
    let response = checked_query(
        control,
        &DurableCommand::RunItem {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: Some(revision.to_owned()),
            selector,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        },
    )?;
    let DurableResponse::RunItem {
        item: Some(item), ..
    } = response
    else {
        panic!("Run-owned item is absent or has another response variant: {response:?}")
    };
    Ok(*item)
}

fn read_ownership(store: &MemoryStore, run_id: &str) -> DurableResult<OwnershipSnapshot> {
    let mut control = DurableStoreControl::open(store.clone())?;
    let response = checked_query(
        &mut control,
        &DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: None,
        },
    )?;
    let DurableResponse::RunCurrent {
        observed_revision: revision,
        current: Some(current),
        ..
    } = response
    else {
        panic!("Run current is absent: {response:?}")
    };
    let response = checked_query(
        &mut control,
        &DurableCommand::RunOccurrencePage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: Some(revision.clone()),
            cursor: None,
            limit: 2,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        },
    )?;
    let DurableResponse::RunOccurrencePage { page, .. } = response else {
        panic!("occurrence query returned another response")
    };
    assert!(page.next_cursor.is_none());
    let [summary] = page.items.as_slice() else {
        panic!("single-call Plan must retain exactly one semantic occurrence")
    };
    let item = read_item(
        &mut control,
        run_id,
        &revision,
        DurableRunItemSelector::Occurrence {
            occurrence_id: summary.occurrence_id.clone(),
        },
    )?;
    let DurableRunItem::Occurrence { occurrence } = item else {
        panic!("occurrence selector returned another leaf")
    };
    let response = checked_query(
        &mut control,
        &DurableCommand::RunAttemptPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: Some(revision.clone()),
            cursor: None,
            limit: 3,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        },
    )?;
    let DurableResponse::RunAttemptPage { page, .. } = response else {
        panic!("Attempt query returned another response")
    };
    assert!(page.next_cursor.is_none());
    let mut attempts = Vec::new();
    for summary in page.items {
        let item = read_item(
            &mut control,
            run_id,
            &revision,
            DurableRunItemSelector::Attempt {
                attempt_id: summary.attempt_id,
            },
        )?;
        let DurableRunItem::Attempt { attempt } = item else {
            panic!("Attempt selector returned another leaf")
        };
        attempts.push(*attempt);
    }
    attempts.sort_by_key(|attempt| attempt.attempt_ordinal);
    assert_eq!(occurrence.attempt_count, attempts.len() as u64);
    assert_eq!(
        attempts.last().map(|attempt| attempt.attempt_id.as_str()),
        Some(occurrence.latest_attempt_id.as_str())
    );
    Ok(OwnershipSnapshot {
        revision,
        current: *current,
        occurrence: *occurrence,
        attempts,
    })
}

fn assert_pending(snapshot: &OwnershipSnapshot) {
    assert_eq!(
        snapshot.current.continuation_status,
        ContinuationStatus::Running
    );
    assert_eq!(snapshot.occurrence.state, ComponentOccurrenceState::Pending);
    assert!(snapshot.occurrence.outcome.is_none());
    assert!(snapshot.occurrence.continuation_digest.is_none());
    let Some(latest) = snapshot.attempts.last() else {
        panic!("pending occurrence has no provider Attempt")
    };
    assert_eq!(latest.state, OperationAttemptState::Running);
    assert_eq!(
        latest.execution_claim_fence,
        snapshot.current.execution_fence
    );
    assert!(latest.outcome.is_none());
}

fn assert_successor_attempt(before: &OwnershipSnapshot, after: &OwnershipSnapshot) {
    let [original] = before.attempts.as_slice() else {
        panic!("takeover source must have exactly one provider Attempt")
    };
    let [superseded, successor] = after.attempts.as_slice() else {
        panic!("takeover must retain exactly two provider Attempts")
    };
    let mut expected_old = original.clone();
    expected_old.state = OperationAttemptState::Superseded;
    assert_eq!(*superseded, expected_old);
    assert_eq!(successor.attempt_ordinal, 2);
    assert_eq!(
        successor.previous_attempt_id.as_ref(),
        Some(&original.attempt_id)
    );
    assert_ne!(successor.attempt_id, original.attempt_id);
    assert_ne!(
        successor.transport_request_id,
        original.transport_request_id
    );
    assert_ne!(
        successor.continuation_attempt_id,
        original.continuation_attempt_id
    );
    assert_ne!(
        successor.execution_claim_owner,
        original.execution_claim_owner
    );
    assert_eq!(
        successor.execution_claim_fence,
        original.execution_claim_fence + 1
    );
    assert_eq!(successor.occurrence_id, original.occurrence_id);
    assert_eq!(
        successor.operation_occurrence_binding,
        original.operation_occurrence_binding
    );
    let mut expected_occurrence = before.occurrence.clone();
    expected_occurrence.attempt_count = after.occurrence.attempt_count;
    expected_occurrence
        .latest_attempt_id
        .clone_from(&after.occurrence.latest_attempt_id);
    expected_occurrence.state = after.occurrence.state;
    expected_occurrence
        .outcome
        .clone_from(&after.occurrence.outcome);
    expected_occurrence
        .continuation_digest
        .clone_from(&after.occurrence.continuation_digest);
    assert_eq!(after.occurrence, expected_occurrence);
}

fn start_command(run_id: &str, execution: ExecutionClaimRequest) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate: candidate(),
        input: json!({"request": "echo"}),
        execution,
    }
}

fn resume_command(run_id: &str, execution: ExecutionClaimRequest) -> DurableCommand {
    DurableCommand::ResumeRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        execution,
    }
}

fn takeover_command(
    run_id: &str,
    expected_fence: u64,
    execution: ExecutionClaimRequest,
) -> DurableCommand {
    DurableCommand::TakeoverRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_fence,
        execution,
    }
}

fn replacement_execution(run_id: &str) -> ExecutionClaimRequest {
    let mut execution = support::execution(run_id);
    "driver:replacement".clone_into(&mut execution.owner);
    execution
}

fn assert_full_audit(store: &MemoryStore) -> DurableResult<()> {
    let Some(stored) = store.clone().load_full_audit()? else {
        panic!("ownership test has no durable head")
    };
    stored.verify()?;
    cymule_core::Machine::restore(stored.state.machine)?.verify_replay()?;
    Ok(())
}

fn recover_pending_occurrence(
    store: &MemoryStore,
    calls: &Arc<AtomicUsize>,
    run_id: &str,
    before: &OwnershipSnapshot,
) -> DurableResult<OwnershipSnapshot> {
    let calls_before = calls.load(Ordering::SeqCst);
    let mut runtime = support::open_control(
        store.clone(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    let execution = replacement_execution(run_id);
    // Clock expiry is evidence for a command, never an autonomous state mutation.
    assert_eq!(read_ownership(store, run_id)?, *before);
    assert!(matches!(
        runtime.submit(resume_command(run_id, execution.clone())),
        Err(DurableError::Busy { fence, .. }) if fence == before.current.execution_fence
    ));
    assert!(matches!(
        runtime.submit(takeover_command(
            run_id,
            before.current.execution_fence + 1,
            execution.clone(),
        )),
        Err(DurableError::Conflict { .. })
    ));
    assert_eq!(read_ownership(store, run_id)?, *before);
    assert_eq!(calls.load(Ordering::SeqCst), calls_before);
    let completed = runtime.submit(takeover_command(
        run_id,
        before.current.execution_fence,
        execution,
    ))?;
    assert_eq!(
        support::expect_completed_value(completed),
        json!({"request": "echo"})
    );
    let after = read_ownership(store, run_id)?;
    assert_successor_attempt(before, &after);
    assert_eq!(
        after.current.continuation_status,
        ContinuationStatus::Completed
    );
    assert_eq!(after.occurrence.state, ComponentOccurrenceState::Completed);
    assert!(matches!(
        after.occurrence.outcome,
        Some(ComponentOutcome::Succeeded { .. })
    ));
    assert_eq!(
        after.attempts.last().map(|attempt| attempt.state),
        Some(OperationAttemptState::Completed)
    );
    assert_eq!(calls.load(Ordering::SeqCst), calls_before + 1);
    assert_full_audit(store)?;
    Ok(after)
}

#[test]
fn component_attempt_ack_loss_requires_takeover_before_first_provider_call() -> DurableResult<()> {
    // The Attempt CAS commits, but its receipt is lost before any provider I/O.
    let run_id = "run:component-attempt-ack-loss";
    let store = MemoryStore::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let execution = support::execution(run_id);
    let command = start_command(run_id, execution.clone());
    let mut first = support::open_control(
        ReceiptLossStore::new(store.clone(), run_id, OperationAttemptState::Running),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    assert!(matches!(
        first.submit(command.clone()),
        Err(DurableError::CommitOutcomeUnknown { .. })
    ));
    drop(first);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let before = read_ownership(&store, run_id)?;
    assert_pending(&before);
    assert_eq!(before.attempts.len(), 1);
    assert_full_audit(&store)?;

    let mut replay = support::open_control(
        store.clone(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    for command in [command, resume_command(run_id, execution)] {
        assert!(
            matches!(replay.submit(command), Err(DurableError::Busy { fence, .. }) if fence == before.current.execution_fence)
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(read_ownership(&store, run_id)?, before);
    recover_pending_occurrence(&store, &calls, run_id, &before)?;
    Ok(())
}

fn start_with_lost_component_result(
    store: &MemoryStore,
    calls: &Arc<AtomicUsize>,
    run_id: &str,
) -> DurableResult<OwnershipSnapshot> {
    let mut first = support::open_control(
        store.clone(),
        CallbackPlugin {
            calls: calls.clone(),
            on_call: |_| {
                Err(RuntimeError::Substrate {
                    code: "injected_component_response_loss".to_owned(),
                    message: "provider response was lost after invocation".to_owned(),
                })
            },
        },
        binding(),
    )?;
    assert!(matches!(
        first.submit(start_command(run_id, support::execution(run_id))),
        Err(DurableError::TimedOut { code, .. }) if code == "component_invocation_interrupted"
    ));
    drop(first);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let before = read_ownership(store, run_id)?;
    assert_pending(&before);
    assert_eq!(before.attempts.len(), 1);
    assert_full_audit(store)?;
    Ok(before)
}

#[test]
fn component_result_loss_requires_fresh_attempt_and_retains_occurrence() -> DurableResult<()> {
    // Provider I/O ran, but no result reached the atomic outcome checkpoint.
    let run_id = "run:component-result-loss";
    let store = MemoryStore::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let before = start_with_lost_component_result(&store, &calls, run_id)?;
    recover_pending_occurrence(&store, &calls, run_id, &before)?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test]
fn takeover_claim_ack_survives_an_unrelated_writer_before_drive() -> DurableResult<()> {
    let run_id = "run:takeover-claim-interleaving";
    let unrelated_run = "run:takeover-claim-interleaving:unrelated";
    let unrelated_signal = "signal:takeover-claim-interleaving:unrelated";
    let store = MemoryStore::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let before = start_with_lost_component_result(&store, &calls, run_id)?;
    let (store, unrelated_wait) = park_unrelated_signal(store, unrelated_run, unrelated_signal)?;
    let store = InterleavingStore::new(store, 0, move |store| {
        activate_unrelated_signal(
            store,
            "activation:takeover-claim-interleaving:unrelated",
            unrelated_signal,
            unrelated_wait,
        )
        .map(|_| ())
    });
    let mut runtime = support::open_control(
        store,
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    let response = runtime.submit(takeover_command(
        run_id,
        before.current.execution_fence,
        replacement_execution(run_id),
    ))?;
    assert_eq!(
        support::expect_completed_value(response),
        json!({"request": "echo"})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let (store, _) = runtime.into_parts();
    let store = store.into_inner();
    let after = read_ownership(&store, run_id)?;
    assert_eq!(
        after.current.continuation_status,
        ContinuationStatus::Completed
    );
    assert_successor_attempt(&before, &after);
    assert_full_audit(&store)
}

#[test]
fn takeover_ack_loss_before_new_attempt_allows_a_later_expired_takeover() -> DurableResult<()> {
    let run_id = "run:component-takeover-ack-loss";
    let store = MemoryStore::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let before = start_with_lost_component_result(&store, &calls, run_id)?;
    let [original] = before.attempts.as_slice() else {
        panic!("interrupted component must retain one provider Attempt")
    };
    let mut interrupted = support::open_control(
        ReceiptLossStore::new(store.clone(), run_id, OperationAttemptState::Superseded),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    // The ownership CAS commits; no new provider Attempt or provider I/O follows it.
    assert!(matches!(
        interrupted.submit(takeover_command(
            run_id,
            before.current.execution_fence,
            replacement_execution(run_id),
        )),
        Err(DurableError::CommitOutcomeUnknown { .. })
    ));
    drop(interrupted);
    let lost = read_ownership(&store, run_id)?;
    assert_eq!(
        lost.current.continuation_status,
        ContinuationStatus::Running
    );
    assert_eq!(
        lost.current.execution_fence,
        before.current.execution_fence + 1
    );
    assert_eq!(lost.current.epoch, before.current.epoch + 1);
    assert_eq!(lost.occurrence, before.occurrence);
    let mut superseded = original.clone();
    superseded.state = OperationAttemptState::Superseded;
    assert_eq!(lost.attempts, vec![superseded.clone()]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_full_audit(&store)?;

    let mut recovered = support::open_control(
        store.clone(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    let mut execution = replacement_execution(run_id);
    execution.owner = "driver:after-takeover-ack-loss".to_owned();
    assert_eq!(read_ownership(&store, run_id)?, lost);
    assert!(matches!(
        recovered.submit(resume_command(run_id, execution.clone())),
        Err(DurableError::Busy { fence, .. }) if fence == lost.current.execution_fence
    ));
    let completed = recovered.submit(takeover_command(
        run_id,
        lost.current.execution_fence,
        execution,
    ))?;
    assert_eq!(
        support::expect_completed_value(completed),
        json!({"request": "echo"})
    );
    let after = read_ownership(&store, run_id)?;
    let [retained, successor] = after.attempts.as_slice() else {
        panic!("a claim without provider admission must not add a provider Attempt")
    };
    assert_eq!(*retained, superseded);
    assert_eq!(
        after.current.continuation_status,
        ContinuationStatus::Completed
    );
    assert_eq!(
        after.occurrence.occurrence_id,
        before.occurrence.occurrence_id
    );
    assert_eq!(successor.attempt_ordinal, 2);
    assert_eq!(
        successor.previous_attempt_id.as_ref(),
        Some(&original.attempt_id)
    );
    assert_eq!(
        successor.execution_claim_fence,
        original.execution_claim_fence + 2
    );
    assert_eq!(successor.state, OperationAttemptState::Completed);
    assert_eq!(successor.outcome, after.occurrence.outcome);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_full_audit(&store)?;
    Ok(())
}

#[test]
fn component_result_ack_loss_reopens_completed_occurrence_without_reinvocation() -> DurableResult<()>
{
    // Outcome, Attempt completion, and post-call position commit before receipt loss.
    let run_id = "run:component-result-ack-loss";
    let store = MemoryStore::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let execution = support::execution(run_id);
    let mut first = support::open_control(
        ReceiptLossStore::new(store.clone(), run_id, OperationAttemptState::Completed),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    assert!(matches!(
        first.submit(start_command(run_id, execution.clone())),
        Err(DurableError::CommitOutcomeUnknown { .. })
    ));
    drop(first);
    let before = read_ownership(&store, run_id)?;
    assert_eq!(
        before.current.continuation_status,
        ContinuationStatus::Running
    );
    assert_eq!(before.occurrence.state, ComponentOccurrenceState::Completed);
    assert!(before.occurrence.continuation_digest.is_some());
    let [attempt] = before.attempts.as_slice() else {
        panic!("committed result must retain exactly one provider Attempt")
    };
    assert_eq!(attempt.state, OperationAttemptState::Completed);
    assert_eq!(attempt.outcome, before.occurrence.outcome);
    assert!(matches!(
        attempt.outcome,
        Some(ComponentOutcome::Succeeded { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_full_audit(&store)?;

    let mut reopened = support::open_control(
        store.clone(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    assert!(matches!(
        reopened.submit(resume_command(run_id, execution)),
        Err(DurableError::Busy { .. })
    ));
    assert_eq!(read_ownership(&store, run_id)?, before);
    let completed = reopened.submit(takeover_command(
        run_id,
        before.current.execution_fence,
        replacement_execution(run_id),
    ))?;
    assert_eq!(
        support::expect_completed_value(completed),
        json!({"request": "echo"})
    );
    let after = read_ownership(&store, run_id)?;
    assert_eq!(
        after.current.continuation_status,
        ContinuationStatus::Completed
    );
    assert_eq!(after.occurrence, before.occurrence);
    assert_eq!(after.attempts, before.attempts);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_full_audit(&store)?;
    Ok(())
}

fn callback_error(error: &DurableError) -> RuntimeError {
    RuntimeError::plugin_defect(error.to_string())
}

#[test]
fn takeover_supersedes_running_attempt_and_rejects_late_old_fence_result() -> DurableResult<()> {
    // A reentrant provider callback pins the interleaving without a wall-clock race.
    let run_id = "run:component-late-result";
    let store = MemoryStore::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut successor_state = None;
    let mut old_driver = support::open_control(
        store.clone(),
        CallbackPlugin {
            calls: calls.clone(),
            on_call: |_| {
                let before =
                    read_ownership(&store, run_id).map_err(|error| callback_error(&error))?;
                assert_pending(&before);
                let mut replacement = support::open_control(
                    store.clone(),
                    CallbackPlugin {
                        calls: calls.clone(),
                        on_call: |_| {
                            let running = read_ownership(&store, run_id)
                                .map_err(|error| callback_error(&error))?;
                            assert_pending(&running);
                            assert_successor_attempt(&before, &running);
                            Ok(PluginResponse::CallResult {
                                value: json!({"winner": "new-fence"}),
                            })
                        },
                    },
                    binding(),
                )
                .map_err(|error| callback_error(&error))?;
                let execution = replacement_execution(run_id);
                assert_eq!(
                    read_ownership(&store, run_id).map_err(|error| callback_error(&error))?,
                    before
                );
                assert!(matches!(
                    replacement.submit(resume_command(run_id, execution.clone())),
                    Err(DurableError::Busy { fence, .. }) if fence == before.current.execution_fence
                ));
                let completed = replacement
                    .submit(takeover_command(
                        run_id,
                        before.current.execution_fence,
                        execution,
                    ))
                    .map_err(|error| callback_error(&error))?;
                assert_eq!(
                    support::expect_completed_value(completed),
                    json!({"winner": "new-fence"})
                );
                successor_state =
                    Some(read_ownership(&store, run_id).map_err(|error| callback_error(&error))?);
                Ok(PluginResponse::CallResult {
                    value: json!({"winner": "old-fence"}),
                })
            },
        },
        binding(),
    )?;
    assert!(matches!(
        old_driver.submit(start_command(run_id, support::execution(run_id))),
        Err(DurableError::Conflict { .. })
    ));
    drop(old_driver);
    let Some(successor_state) = successor_state else {
        panic!("replacement driver never completed")
    };
    assert_eq!(read_ownership(&store, run_id)?, successor_state);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let mut replay = support::open_control(
        store.clone(),
        CountingPlugin {
            calls: calls.clone(),
        },
        binding(),
    )?;
    let completed = replay.submit(resume_command(run_id, support::execution(run_id)))?;
    assert_eq!(
        support::expect_completed_value(completed),
        json!({"winner": "new-fence"})
    );
    assert_eq!(read_ownership(&store, run_id)?, successor_state);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_full_audit(&store)?;
    Ok(())
}
