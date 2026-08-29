//! Public Running-to-terminal paging, recovery, and stale-result conformance.

/// Shared issued Clock and public admission fixtures.
pub mod support;

#[path = "support/paged_store.rs"]
mod paged_store;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use cymule_core::durable_internal::MachineRunReducerState;
use cymule_core::{
    AttemptProjection, COMPONENT_OUTPUT_ARTIFACT_KIND, ComponentContract, Definition,
    DispatchPolicy, EffectContract, EffectProfile, Expression, MutationKind, Operation,
    PlanCandidate, ReconciliationMode, Region, RunExecutionStatus, Step,
};
use cymule_durable::{
    ComponentOccurrenceState, ComponentOutcome, DURABLE_CONTROL_VERSION, DurableBoundary,
    DurableCommand, DurableError, DurableResponse, DurableResult, DurableStore,
    DurableStoreControl, MemoryStore, OperationAttempt, OperationAttemptState, OutboxState,
    StateRootLeafKind, StateRootValue, StoredState, state_map_get,
};
use cymule_durable_protocol::{Continuation, ContinuationStatus, ExecutionClaimRequest};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginEffect, PluginExpectedFailure, PluginHost,
    PluginManifest, PluginOperation, PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::{Value, json};

use paged_store::{FaultMoment, PageFault, PageStage, PageTrace, PagedStore};

const EFFECT_COUNT: usize = 2;
const FAILURE_CODE: &str = "paged_expected_failure";
const LATE_PROVIDER_DEFECT_CODE: &str = "test_late_provider_defect";
const LATE_PROVIDER_DEFECT_MESSAGE: &str = "the in-flight provider failed after terminal admission";

#[derive(Default)]
struct ProviderCounts {
    calls: Cell<usize>,
    prepares: Cell<usize>,
    world_calls: Cell<usize>,
}

struct TerminalProvider<F> {
    counts: Rc<ProviderCounts>,
    on_call: F,
}

impl<F> PluginHost for TerminalProvider<F>
where
    F: FnMut(Value) -> RuntimeResult<PluginResponse>,
{
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: manifest(),
            }),
            PluginRequest::PrepareEffect { operation, .. } if operation == "test.pending" => {
                self.counts.prepares.set(self.counts.prepares.get() + 1);
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::Call { component, input } if component == "test.terminal" => {
                self.counts.calls.set(self.counts.calls.get() + 1);
                (self.on_call)(input)
            }
            other => {
                self.counts
                    .world_calls
                    .set(self.counts.world_calls.get() + 1);
                Err(RuntimeError::plugin_defect(format!(
                    "terminal fixture must never dispatch or reconcile: {other:?}"
                )))
            }
        }
    }
}

fn manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "paged-terminal-fixture".to_owned(),
        components: BTreeMap::from([(
            "test.terminal".to_owned(),
            PluginOperation {
                implementation_revision: "1".to_owned(),
            },
        )]),
        effects: BTreeMap::from([(
            "test.pending".to_owned(),
            PluginEffect {
                implementation_revision: "1".to_owned(),
                can_reconcile: true,
            },
        )]),
    }
}

fn binding() -> DurableResult<ExecutionBinding> {
    ExecutionBinding::for_local_process(
        &manifest(),
        cymule_core::content_id("test.paged-terminal-runtime/1", &())?,
    )
    .map_err(Into::into)
}

fn candidate() -> PlanCandidate {
    let mut steps: Vec<_> = (0..EFFECT_COUNT)
        .map(|index| Step {
            id: format!("pending.{index}"),
            operation: Operation::Effect {
                effect: "test.pending".to_owned(),
                input: Expression::Literal {
                    value: json!({"ordinal": index}),
                },
                occurrence: "primary".to_owned(),
                bind: None,
            },
        })
        .collect();
    steps.push(Step {
        id: "terminal.call".to_owned(),
        operation: Operation::Call {
            component: "test.terminal".to_owned(),
            input: Expression::Input,
            bind: Some("result".to_owned()),
        },
    });
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "paged_terminal".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.terminal".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            output_artifact_kind: COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
            requirements: BTreeMap::new(),
        }],
        effects: vec![EffectContract {
            id: "test.pending".to_owned(),
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
                steps,
                result: Expression::Binding {
                    name: "result".to_owned(),
                },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn start(run_id: &str, execution: ExecutionClaimRequest) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate: candidate(),
        input: json!({"source": "paged-terminal"}),
        execution,
    }
}

fn resume(run_id: &str, execution: ExecutionClaimRequest) -> DurableCommand {
    DurableCommand::ResumeRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        execution,
    }
}

fn cancellation(run_id: &str) -> DurableCommand {
    DurableCommand::CancelRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        cancellation_id: format!("cancel:{run_id}"),
        run_id: run_id.to_owned(),
        reason: json!({"cause": "paged-terminal"}),
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    stored: StoredState,
    core_status: RunExecutionStatus,
    core_reducer_state: MachineRunReducerState,
    active_core_attempt: Option<String>,
    core_attempts: BTreeMap<String, AttemptProjection>,
    pending_commands: u64,
    pending_transitions: u64,
}

fn inspect(inner: &MemoryStore, run_id: &str) -> DurableResult<Snapshot> {
    let mut reader = inner.clone();
    let stored = reader.load_full_audit()?.ok_or_else(|| {
        DurableError::Validation("public Running fixture has no persisted state".to_owned())
    })?;
    let attempt_ids: BTreeSet<_> = stored
        .state
        .operation_attempts
        .values()
        .filter(|attempt| attempt.run_id == run_id)
        .map(|attempt| attempt.continuation_attempt_id.clone())
        .collect();
    let manifest = &stored.state_root_manifest;
    let (core_status, core_reducer_state, active_core_attempt, core_attempts) = reader
        .with_state_root_resolver(manifest, |resolver| {
            let Some(StateRootValue::MachineRunCurrent { current }) =
                state_map_get(&manifest.machine_frontier().runs, run_id, resolver)?
            else {
                return Err(DurableError::Validation(
                    "public Running fixture has no exact Core Run".to_owned(),
                ));
            };
            current.verify()?;
            assert_eq!(
                usize::try_from(current.children.attempts.entries),
                Ok(attempt_ids.len())
            );
            let mut attempts = BTreeMap::new();
            for id in &attempt_ids {
                let Some(StateRootValue::Leaf {
                    kind: StateRootLeafKind::MachineAttempt,
                    canonical_json,
                }) = state_map_get(&current.children.attempts, id, resolver)?
                else {
                    return Err(DurableError::Validation(
                        "admitted provider Attempt has no physical Core Attempt".to_owned(),
                    ));
                };
                let attempt: AttemptProjection =
                    cymule_core::decode_json(canonical_json.as_bytes())?;
                assert_eq!(&attempt.attempt_id, id);
                attempts.insert(id.clone(), attempt);
            }
            Ok((
                current.execution_status.clone(),
                current.reducer_state.clone(),
                current.active_attempt_id.clone(),
                attempts,
            ))
        })?;
    Ok(Snapshot {
        pending_commands: manifest.machine_frontier().pending_commands.entries,
        pending_transitions: manifest.machine_frontier().paged_transitions.entries,
        stored,
        core_status,
        core_reducer_state,
        active_core_attempt,
        core_attempts,
    })
}

fn continuation<'a>(snapshot: &'a Snapshot, run_id: &str) -> DurableResult<&'a Continuation> {
    snapshot
        .stored
        .state
        .continuations
        .get(run_id)
        .ok_or_else(|| DurableError::Validation("fixture Continuation is missing".to_owned()))
}

fn provider_attempts(snapshot: &Snapshot) -> Vec<&OperationAttempt> {
    let mut attempts: Vec<_> = snapshot.stored.state.operation_attempts.values().collect();
    attempts.sort_by_key(|attempt| attempt.attempt_ordinal);
    attempts
}

fn assert_running(snapshot: &Snapshot, run_id: &str, ordinal: u64) -> DurableResult<()> {
    let continuation = continuation(snapshot, run_id)?;
    assert_eq!(continuation.status, ContinuationStatus::Running);
    assert_eq!(snapshot.core_status, RunExecutionStatus::Active);
    assert_eq!(
        matches!(
            &snapshot.core_reducer_state,
            MachineRunReducerState::Transitioning { .. }
        ),
        snapshot.pending_commands != 0
    );
    let claim = continuation.execution_claim.as_ref().ok_or_else(|| {
        DurableError::Validation("Running fixture lost its execution claim".to_owned())
    })?;
    assert_eq!(
        snapshot.active_core_attempt.as_deref(),
        Some(claim.continuation_attempt_id.as_str())
    );
    let core_attempt = snapshot
        .core_attempts
        .get(&claim.continuation_attempt_id)
        .ok_or_else(|| DurableError::Validation("active Core Attempt is missing".to_owned()))?;
    assert!(core_attempt.active);
    assert_eq!(core_attempt.execution_fence, claim.fence);
    let attempts = provider_attempts(snapshot);
    let latest = attempts.last().ok_or_else(|| {
        DurableError::Validation("Running provider Attempt is missing".to_owned())
    })?;
    assert_eq!(latest.attempt_ordinal, ordinal);
    assert_eq!(latest.state, OperationAttemptState::Running);
    assert_eq!(snapshot.stored.state.outbox.len(), EFFECT_COUNT);
    assert!(
        snapshot
            .stored
            .state
            .outbox
            .values()
            .all(|effect| effect.state == OutboxState::Pending)
    );
    Ok(())
}

fn assert_terminal(
    snapshot: &Snapshot,
    run_id: &str,
    failed: bool,
    provider_count: usize,
) -> DurableResult<()> {
    let continuation = continuation(snapshot, run_id)?;
    let status = if failed {
        ContinuationStatus::Failed
    } else {
        ContinuationStatus::Cancelled
    };
    assert_eq!(continuation.status, status);
    assert!(continuation.execution_claim.is_none());
    assert_eq!(snapshot.core_reducer_state, MachineRunReducerState::Ready);
    assert!(snapshot.active_core_attempt.is_none());
    assert_eq!(snapshot.core_attempts.len(), provider_count);
    assert!(
        snapshot
            .core_attempts
            .values()
            .all(|attempt| !attempt.active)
    );
    assert_eq!(snapshot.pending_commands, 0);
    assert_eq!(snapshot.pending_transitions, 0);
    assert_eq!(snapshot.stored.state.outbox.len(), EFFECT_COUNT);
    assert_eq!(snapshot.stored.state.component_occurrences.len(), 1);
    assert!(
        snapshot
            .stored
            .state
            .outbox
            .values()
            .all(|effect| effect.state == OutboxState::CancelledBeforeRelease)
    );
    let attempts = provider_attempts(snapshot);
    assert_eq!(attempts.len(), provider_count);
    if failed {
        assert!(matches!(
            &snapshot.core_status,
            RunExecutionStatus::Failed { failure } if failure.code == FAILURE_CODE
        ));
        let latest = attempts.last().ok_or_else(|| {
            DurableError::Validation("failed provider Attempt is missing".to_owned())
        })?;
        assert_eq!(latest.state, OperationAttemptState::Completed);
        assert!(matches!(
            &latest.outcome,
            Some(ComponentOutcome::ExpectedFailure { code, .. }) if code == FAILURE_CODE
        ));
        assert!(
            snapshot
                .stored
                .state
                .component_occurrences
                .values()
                .all(|occurrence| occurrence.state == ComponentOccurrenceState::Completed)
        );
    } else {
        assert!(matches!(
            snapshot.core_status,
            RunExecutionStatus::Cancelled { .. }
        ));
        assert!(
            attempts
                .iter()
                .all(|attempt| attempt.state == OperationAttemptState::Superseded)
        );
    }
    Ok(())
}

fn assert_fault<T>(result: DurableResult<T>, moment: FaultMoment) -> DurableResult<()> {
    match (result, moment) {
        (Err(DurableError::Substrate { code, .. }), FaultMoment::Before)
            if code == "injected_paged_pre_cas" =>
        {
            Ok(())
        }
        (Err(DurableError::CommitOutcomeUnknown { .. }), FaultMoment::After) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(_), _) => Err(DurableError::Validation(
            "selected terminal page fault was not observed".to_owned(),
        )),
    }
}

fn provider_error(error: &DurableError) -> RuntimeError {
    RuntimeError::plugin_defect(error.to_string())
}

fn assert_paged(trace: &PageTrace) {
    assert_eq!(trace.commits.first(), Some(&PageStage::Begin));
    assert!(trace.commits.contains(&PageStage::Progress));
    assert_eq!(trace.commits.last(), Some(&PageStage::Finalize));
}

fn assert_no_extra_provider_work(counts: &ProviderCounts, calls: usize) {
    assert_eq!(counts.calls.get(), calls);
    assert_eq!(counts.prepares.get(), EFFECT_COUNT);
    assert_eq!(counts.world_calls.get(), 0);
}

fn finish_cancel_from_provider(
    inner: &MemoryStore,
    store: &PagedStore,
    run_id: &str,
    fault: Option<PageFault>,
) -> DurableResult<Snapshot> {
    let running = inspect(inner, run_id)?;
    assert_running(&running, run_id, 1)?;
    store.arm(fault);
    let command = cancellation(run_id);
    let result = DurableStoreControl::open(store.clone())?.submit(command.clone());
    if let Some(fault) = fault {
        assert_fault(result, fault.moment)?;
        assert_eq!(store.trace().fault_hits, 1);
        let interrupted = inspect(inner, run_id)?;
        if store.trace().commits.last() == Some(&PageStage::Finalize) {
            assert_terminal(&interrupted, run_id, false, 1)?;
        } else {
            assert_running(&interrupted, run_id, 1)?;
        }
    } else {
        result?.verify()?;
    }
    let replay = DurableStoreControl::open(store.clone())?.submit(command)?;
    assert!(matches!(replay, DurableResponse::RunCancelled { .. }));
    let terminal = inspect(inner, run_id)?;
    assert_terminal(&terminal, run_id, false, 1)?;
    assert_eq!(
        continuation(&terminal, run_id)?.execution_fence,
        continuation(&running, run_id)?.execution_fence + 1
    );
    Ok(terminal)
}

fn cancel_case(run_id: &str, fault: Option<PageFault>) -> DurableResult<PageTrace> {
    let inner = MemoryStore::new();
    let store = PagedStore::new(inner.clone());
    let counts = Rc::new(ProviderCounts::default());
    let mut completed = None;
    let mut runtime = support::open_control(
        store.clone(),
        TerminalProvider {
            counts: counts.clone(),
            on_call: |_: Value| {
                let result = finish_cancel_from_provider(&inner, &store, run_id, fault);
                let reply = result
                    .as_ref()
                    .map(|_| PluginResponse::CallResult {
                        value: json!({"late": "cancelled-owner"}),
                    })
                    .map_err(provider_error);
                completed = Some(result);
                reply
            },
        },
        binding()?,
    )?;
    let late_result = runtime.submit(start(run_id, support::execution(run_id)));
    drop(runtime);
    let terminal = completed.ok_or_else(|| {
        DurableError::Validation("cancel fixture never reached its Running provider".to_owned())
    })??;
    assert!(matches!(late_result, Err(DurableError::Conflict { .. })));
    assert_eq!(inspect(&inner, run_id)?, terminal);
    assert_no_extra_provider_work(&counts, 1);
    let replay = DurableStoreControl::open(store.clone())?.submit(cancellation(run_id))?;
    assert!(matches!(replay, DurableResponse::RunCancelled { .. }));
    assert_eq!(inspect(&inner, run_id)?, terminal);
    let trace = store.trace();
    assert_paged(&trace);
    Ok(trace)
}

fn expected_failure_response() -> PluginResponse {
    PluginResponse::ExpectedFailure {
        error: PluginExpectedFailure {
            code: FAILURE_CODE.to_owned(),
            message: "declared failure while a Core Attempt is active".to_owned(),
        },
    }
}

fn resume_failed_without_provider(
    store: &PagedStore,
    counts: &Rc<ProviderCounts>,
    run_id: &str,
    execution: ExecutionClaimRequest,
) -> DurableResult<()> {
    let mut runtime = support::open_control(
        store.clone(),
        TerminalProvider {
            counts: counts.clone(),
            on_call: |_: Value| {
                Err(RuntimeError::plugin_defect(
                    "terminal recovery reinvoked the provider",
                ))
            },
        },
        binding()?,
    )?;
    assert!(matches!(
        runtime.submit(start(run_id, support::execution(run_id)))?,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Failed { failure }, ..
        } if failure.code == FAILURE_CODE
    ));
    assert!(matches!(
        runtime.submit(resume(run_id, execution))?,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Failed { failure }, ..
        } if failure.code == FAILURE_CODE
    ));
    Ok(())
}

fn fail_takeover_from_provider(
    inner: &MemoryStore,
    store: &PagedStore,
    counts: &Rc<ProviderCounts>,
    run_id: &str,
    fault: Option<PageFault>,
) -> DurableResult<Snapshot> {
    let original = inspect(inner, run_id)?;
    assert_running(&original, run_id, 1)?;
    let mut execution = support::execution(run_id);
    execution.owner = cymule_core::content_id("test.paged-terminal-owner/1", &run_id)?;
    let mut admitted = None;
    let mut replacement = support::open_control(
        store.clone(),
        TerminalProvider {
            counts: counts.clone(),
            on_call: |_: Value| {
                let running = inspect(inner, run_id).map_err(|error| provider_error(&error))?;
                assert_running(&running, run_id, 2).map_err(|error| provider_error(&error))?;
                store.arm(fault);
                admitted = Some(running);
                Ok(expected_failure_response())
            },
        },
        binding()?,
    )?;
    let result = replacement.submit(DurableCommand::TakeoverRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_fence: continuation(&original, run_id)?.execution_fence,
        execution: execution.clone(),
    });
    drop(replacement);
    let running = admitted.ok_or_else(|| {
        DurableError::Validation("takeover never reached its new provider Attempt".to_owned())
    })?;
    if let Some(fault) = fault {
        assert_fault(result, fault.moment)?;
        assert_eq!(store.trace().fault_hits, 1);
        let interrupted = inspect(inner, run_id)?;
        if store.trace().commits.last() == Some(&PageStage::Finalize) {
            assert_terminal(&interrupted, run_id, true, 2)?;
        } else {
            assert_running(&interrupted, run_id, 2)?;
            assert_eq!(interrupted.pending_commands, 1);
        }
    } else {
        assert!(matches!(
            result?,
            DurableResponse::RunBoundary {
                boundary: DurableBoundary::Failed { failure }, ..
            } if failure.code == FAILURE_CODE
        ));
    }
    resume_failed_without_provider(store, counts, run_id, execution)?;
    let terminal = inspect(inner, run_id)?;
    assert_terminal(&terminal, run_id, true, 2)?;
    assert_eq!(
        continuation(&terminal, run_id)?.execution_fence,
        continuation(&running, run_id)?.execution_fence + 1
    );
    assert_eq!(
        provider_attempts(&terminal)[0].state,
        OperationAttemptState::Superseded
    );
    Ok(terminal)
}

fn failure_case(run_id: &str, fault: Option<PageFault>) -> DurableResult<PageTrace> {
    let inner = MemoryStore::new();
    let store = PagedStore::new(inner.clone());
    let counts = Rc::new(ProviderCounts::default());
    let mut completed = None;
    let mut runtime = support::open_control(
        store.clone(),
        TerminalProvider {
            counts: counts.clone(),
            on_call: |_: Value| {
                let result = fail_takeover_from_provider(&inner, &store, &counts, run_id, fault);
                let reply = result
                    .as_ref()
                    .map(|_| PluginResponse::CallResult {
                        value: json!({"late": "superseded-before-failure"}),
                    })
                    .map_err(provider_error);
                completed = Some(result);
                reply
            },
        },
        binding()?,
    )?;
    let late_result = runtime.submit(start(run_id, support::execution(run_id)));
    drop(runtime);
    let terminal = completed.ok_or_else(|| {
        DurableError::Validation("failure fixture never reached its Running provider".to_owned())
    })??;
    assert!(matches!(late_result, Err(DurableError::Conflict { .. })));
    assert_eq!(inspect(&inner, run_id)?, terminal);
    assert_no_extra_provider_work(&counts, 2);
    let trace = store.trace();
    assert_paged(&trace);
    Ok(trace)
}

fn interrupt_cancel_from_provider(
    inner: &MemoryStore,
    store: &PagedStore,
    run_id: &str,
    ordinal: usize,
) -> DurableResult<Snapshot> {
    let running = inspect(inner, run_id)?;
    assert_running(&running, run_id, 1)?;
    store.arm(Some(PageFault {
        ordinal,
        moment: FaultMoment::After,
    }));
    assert_fault(
        DurableStoreControl::open(store.clone())?.submit(cancellation(run_id)),
        FaultMoment::After,
    )?;
    assert_eq!(store.trace().fault_hits, 1);
    assert!(matches!(
        store.trace().commits.last(),
        Some(PageStage::Begin | PageStage::Progress)
    ));
    let interrupted = inspect(inner, run_id)?;
    assert_running(&interrupted, run_id, 1)?;
    assert_eq!(interrupted.pending_commands, 1);
    assert_eq!(interrupted.pending_transitions, 1);
    assert_eq!(
        continuation(&interrupted, run_id)?,
        continuation(&running, run_id)?
    );
    Ok(interrupted)
}

fn finish_interrupted_cancel(
    inner: &MemoryStore,
    store: &PagedStore,
    run_id: &str,
) -> DurableResult<Snapshot> {
    let replay = DurableStoreControl::open(store.clone())?.submit(cancellation(run_id))?;
    assert!(matches!(replay, DurableResponse::RunCancelled { .. }));
    let terminal = inspect(inner, run_id)?;
    assert_terminal(&terminal, run_id, false, 1)?;
    assert_paged(&store.trace());
    Ok(terminal)
}

#[derive(Clone, Copy)]
enum LateProviderReply {
    Succeeded,
    Defect,
}

impl LateProviderReply {
    fn response(self) -> RuntimeResult<PluginResponse> {
        match self {
            Self::Succeeded => Ok(PluginResponse::CallResult {
                value: json!({"late": "while-terminal-paging"}),
            }),
            Self::Defect => Err(RuntimeError::PluginDefect {
                code: LATE_PROVIDER_DEFECT_CODE.to_owned(),
                message: LATE_PROVIDER_DEFECT_MESSAGE.to_owned(),
            }),
        }
    }

    fn assert_result(self, result: DurableResult<DurableResponse>) {
        match self {
            Self::Succeeded => assert!(matches!(result, Err(DurableError::Conflict { .. }))),
            Self::Defect => assert!(matches!(
                result,
                Err(DurableError::RuntimeDefect { code, message })
                    if code == LATE_PROVIDER_DEFECT_CODE && message == LATE_PROVIDER_DEFECT_MESSAGE
            )),
        }
    }
}

fn late_response_during_paging_case(
    run_id: &str,
    ordinal: usize,
    response: LateProviderReply,
) -> DurableResult<()> {
    let inner = MemoryStore::new();
    let store = PagedStore::new(inner.clone());
    let counts = Rc::new(ProviderCounts::default());
    let mut interrupted = None;
    let mut runtime = support::open_control(
        store.clone(),
        TerminalProvider {
            counts: counts.clone(),
            on_call: |_: Value| {
                let result = interrupt_cancel_from_provider(&inner, &store, run_id, ordinal)
                    .map(|snapshot| (snapshot, store.trace()));
                let reply = result
                    .as_ref()
                    .map_err(provider_error)
                    .and_then(|_| response.response());
                interrupted = Some(result);
                reply
            },
        },
        binding()?,
    )?;
    let late_result = runtime.submit(start(run_id, support::execution(run_id)));
    drop(runtime);
    let (before_result, before_trace) = interrupted.ok_or_else(|| {
        DurableError::Validation("paging race never reached its Running provider".to_owned())
    })??;
    response.assert_result(late_result);
    assert_eq!(inspect(&inner, run_id)?, before_result);
    assert_eq!(store.trace(), before_trace);
    assert_no_extra_provider_work(&counts, 1);
    finish_interrupted_cancel(&inner, &store, run_id)?;
    assert_no_extra_provider_work(&counts, 1);
    Ok(())
}

#[derive(Debug, PartialEq)]
struct OtherRunSnapshot {
    continuation: Continuation,
    core: StateRootValue,
}

fn other_run_snapshot(inner: &MemoryStore, run_id: &str) -> DurableResult<OtherRunSnapshot> {
    let mut reader = inner.clone();
    let stored = reader.load_full_audit()?.ok_or_else(|| {
        DurableError::Validation("interleaved Run has no persisted state".to_owned())
    })?;
    let continuation = stored
        .state
        .continuations
        .get(run_id)
        .ok_or_else(|| DurableError::Validation("interleaved Run is missing".to_owned()))?
        .clone();
    assert_eq!(continuation.status, ContinuationStatus::Completed);
    let manifest = &stored.state_root_manifest;
    let core = reader
        .with_state_root_resolver(manifest, |resolver| {
            state_map_get(&manifest.machine_frontier().runs, run_id, resolver)
        })?
        .ok_or_else(|| DurableError::Validation("interleaved Core Run is missing".to_owned()))?;
    assert!(matches!(
        &core,
        StateRootValue::MachineRunCurrent { current }
            if current.execution_status == RunExecutionStatus::Completed
                && current.active_attempt_id.is_none()
    ));
    Ok(OtherRunSnapshot { continuation, core })
}

fn complete_other_run(inner: &MemoryStore, run_id: &str) -> DurableResult<OtherRunSnapshot> {
    let mut runtime = support::open_control(
        inner.clone(),
        support::EmptyPlugin,
        support::empty_binding(),
    )?;
    let input = json!({"independent_run": run_id});
    let result = runtime.submit(DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate: support::identity_candidate("interleaved-terminal-pages"),
        input: input.clone(),
        execution: support::execution(run_id),
    })?;
    assert_eq!(support::expect_completed_value(result), input);
    other_run_snapshot(inner, run_id)
}

fn cancel_around_other_run(
    inner: &MemoryStore,
    store: &PagedStore,
    run_id: &str,
    ordinal: usize,
) -> DurableResult<Snapshot> {
    let interrupted = interrupt_cancel_from_provider(inner, store, run_id, ordinal)?;
    let other_run_id = format!("{run_id}:independent");
    let other = complete_other_run(inner, &other_run_id)?;
    let after_other = inspect(inner, run_id)?;
    assert_running(&after_other, run_id, 1)?;
    assert_eq!(after_other.pending_commands, 1);
    assert_eq!(after_other.pending_transitions, 1);
    assert_eq!(
        continuation(&after_other, run_id)?,
        continuation(&interrupted, run_id)?
    );
    assert_ne!(after_other.stored.head, interrupted.stored.head);
    let terminal = finish_interrupted_cancel(inner, store, run_id)?;
    assert_eq!(other_run_snapshot(inner, &other_run_id)?, other);
    Ok(terminal)
}

fn other_run_during_paging_case(run_id: &str, ordinal: usize) -> DurableResult<()> {
    let inner = MemoryStore::new();
    let store = PagedStore::new(inner.clone());
    let counts = Rc::new(ProviderCounts::default());
    let mut completed = None;
    let mut runtime = support::open_control(
        store.clone(),
        TerminalProvider {
            counts: counts.clone(),
            on_call: |_: Value| {
                let result = cancel_around_other_run(&inner, &store, run_id, ordinal);
                let reply = result
                    .as_ref()
                    .map(|_| PluginResponse::CallResult {
                        value: json!({"late": "after-other-run-and-cancel"}),
                    })
                    .map_err(provider_error);
                completed = Some(result);
                reply
            },
        },
        binding()?,
    )?;
    let late_result = runtime.submit(start(run_id, support::execution(run_id)));
    drop(runtime);
    let terminal = completed.ok_or_else(|| {
        DurableError::Validation("interleaving never reached its Running provider".to_owned())
    })??;
    assert!(matches!(late_result, Err(DurableError::Conflict { .. })));
    assert_eq!(inspect(&inner, run_id)?, terminal);
    assert_no_extra_provider_work(&counts, 1);
    Ok(())
}

#[test]
fn paged_cancel_closes_the_active_core_attempt_and_rejects_late_output() -> DurableResult<()> {
    cancel_case("run:paged-cancel-baseline", None)?;
    Ok(())
}

#[test]
fn paged_cancel_recovers_before_and_after_every_page_cas() -> DurableResult<()> {
    let baseline = cancel_case("run:paged-cancel-discovery", None)?;
    for ordinal in 0..baseline.commits.len() {
        for moment in [FaultMoment::Before, FaultMoment::After] {
            let run_id = format!("run:paged-cancel:{ordinal}:{moment:?}");
            let trace = cancel_case(&run_id, Some(PageFault { ordinal, moment }))?;
            assert_eq!(trace.commits, baseline.commits);
            assert_eq!(trace.fault_hits, 1);
            assert_eq!(
                trace.attempts.len(),
                baseline.attempts.len() + usize::from(moment == FaultMoment::Before)
            );
        }
    }
    Ok(())
}

#[test]
fn paged_expected_failure_closes_both_core_attempts_and_rejects_late_output() -> DurableResult<()> {
    failure_case("run:paged-failure-baseline", None)?;
    Ok(())
}

#[test]
fn paged_expected_failure_recovers_admitted_pages_without_reinvocation() -> DurableResult<()> {
    let baseline = failure_case("run:paged-failure-discovery", None)?;
    for (ordinal, stage) in baseline.commits.iter().enumerate() {
        for moment in [FaultMoment::Before, FaultMoment::After] {
            // Only a failed first Begin has no durable provider failure to recover.
            if *stage == PageStage::Begin && moment == FaultMoment::Before {
                continue;
            }
            let run_id = format!("run:paged-failure:{ordinal}:{moment:?}");
            let trace = failure_case(&run_id, Some(PageFault { ordinal, moment }))?;
            assert_eq!(trace.commits, baseline.commits);
            assert_eq!(trace.fault_hits, 1);
            assert_eq!(
                trace.attempts.len(),
                baseline.attempts.len() + usize::from(moment == FaultMoment::Before)
            );
        }
    }
    Ok(())
}

#[test]
fn paged_terminal_fences_same_run_material_only_late_success() -> DurableResult<()> {
    let baseline = cancel_case("run:paged-late-success-discovery", None)?;
    for (ordinal, stage) in baseline.commits.iter().enumerate() {
        if *stage != PageStage::Finalize {
            late_response_during_paging_case(
                &format!("run:paged-late-success:{ordinal}"),
                ordinal,
                LateProviderReply::Succeeded,
            )?;
        }
    }
    Ok(())
}

#[test]
fn paged_terminal_keeps_other_run_commits_between_every_source_page() -> DurableResult<()> {
    let baseline = cancel_case("run:paged-other-run-discovery", None)?;
    for (ordinal, stage) in baseline.commits.iter().enumerate() {
        if *stage != PageStage::Finalize {
            other_run_during_paging_case(&format!("run:paged-other-run:{ordinal}"), ordinal)?;
        }
    }
    Ok(())
}

#[test]
fn paged_terminal_preserves_provider_error_without_mutating_reserved_run() -> DurableResult<()> {
    let baseline = cancel_case("run:paged-late-error-discovery", None)?;
    for (ordinal, stage) in baseline.commits.iter().enumerate() {
        if *stage != PageStage::Finalize {
            late_response_during_paging_case(
                &format!("run:paged-late-error:{ordinal}"),
                ordinal,
                LateProviderReply::Defect,
            )?;
        }
    }
    Ok(())
}
