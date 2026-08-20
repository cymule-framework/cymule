//! Generated command traces checked against an independent durable-domain model.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

use cymule_core::{
    Definition, DispatchPolicy, EffectContract, EffectProfile, Expression, MutationKind, Operation,
    PlanCandidate, ROOT_SCOPE_ID, ReconciliationMode, ReconciliationResolution, Region, Step,
    WaitSpec, WorldOutcome, artifact_ref, canonical_bytes, content_id, effect_intent_id,
};
use cymule_durable::{
    ContinuationStatus, DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableError,
    DurableResponse, DurableResult, DurableRuntimeControl, DurableState, DurableStore, MemoryStore,
    OutboxState, ResumableRuntime, StoreCommit, StoredState, WaitActivationSource, WaitState,
};
use cymule_runtime::{
    ExecutionBinding, ExecutionResult, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest,
    PluginRequest, PluginResponse, RuntimeError, RuntimeResult, seal_plan,
};
use cymule_test_world::{
    FailureFingerprint, FaultAction, FaultPlan, FaultSchedule, FaultStep, ReplaySpec, SeededRandom,
    TestWorld, TraceCase, TraceFailure, requested_seeds,
};
use serde_json::{Value, json};

const CAS_OPERATION: &str = "durable.compare_and_swap";
const EFFECT_OPERATION: &str = "test.capture";
const SIGNAL_KEY: &str = "trace.signal";
const WAIT_SITE: &str = "wait.signal";
const EFFECT_SITE: &str = "effect.capture";
const EFFECT_OCCURRENCE: &str = "primary";
const BINDING_REVISION: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

#[derive(Default)]
struct TracePlugin {
    applied_intents: BTreeSet<String>,
}

impl PluginHost for TracePlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: trace_manifest(),
            }),
            PluginRequest::PrepareEffect { operation, .. } if operation == EFFECT_OPERATION => {
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::DispatchEffect {
                operation,
                intent_id,
                input,
            } if operation == EFFECT_OPERATION => {
                self.applied_intents.insert(intent_id);
                Ok(PluginResponse::EffectResult {
                    outcome: WorldOutcome::Applied,
                    value: Some(input),
                })
            }
            PluginRequest::ReconcileEffect {
                operation,
                intent_id,
                input,
            } if operation == EFFECT_OPERATION => {
                let applied = self.applied_intents.contains(&intent_id);
                Ok(PluginResponse::ReconciliationResult {
                    resolution: if applied {
                        ReconciliationResolution::ResolvedApplied
                    } else {
                        ReconciliationResolution::ResolvedNotApplied
                    },
                    value: applied.then_some(input),
                })
            }
            other => Err(RuntimeError::plugin_defect(format!(
                "model trace received unexpected plugin request: {other:?}"
            ))),
        }
    }
}

fn trace_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "model-trace@1".to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::from([(
            EFFECT_OPERATION.to_owned(),
            PluginEffect {
                implementation_revision: "1".to_owned(),
                can_reconcile: true,
            },
        )]),
    }
}

fn open_runtime<S: DurableStore>(
    store: S,
    mut plugin: TracePlugin,
) -> DurableResult<ResumableRuntime<S, TracePlugin>> {
    let manifest = plugin
        .describe()
        .map_err(|error| DurableError::Substrate(error.to_string()))?;
    let binding = ExecutionBinding::for_local_process(&manifest, BINDING_REVISION)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    ResumableRuntime::open(store, plugin, binding)
}

struct FaultingStore {
    inner: MemoryStore,
    faults: FaultSchedule,
    active_path: Rc<Cell<usize>>,
}

impl DurableStore for FaultingStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let path = [self.active_path.get()];
        let action = self.faults.observe_path(CAS_OPERATION, &path);
        if action == Some(FaultAction::ErrorBefore) {
            return Err(DurableError::Substrate(format!(
                "generated failure before durable CAS at path {path:?}"
            )));
        }
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if action == Some(FaultAction::AcknowledgementLostAfter) {
            return Err(DurableError::Substrate(format!(
                "generated acknowledgement loss after durable CAS at path {path:?}"
            )));
        }
        Ok(commit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelStatus {
    Waiting,
    Ready,
    ReleaseRequired,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelKind {
    Immediate,
    Signal { key: String, wait_id: String },
    ExplicitEffect { intent_id: String },
}

#[derive(Debug, Clone)]
struct ModelRun {
    input: Value,
    plan_id: String,
    kind: ModelKind,
    status: ModelStatus,
    epoch: u64,
}

#[derive(Debug, Clone)]
struct ModelFailure {
    fingerprint: FailureFingerprint,
    detail: String,
}

impl fmt::Display for ModelFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

fn failure(code: &str, phase: &str, invariant: &str, detail: impl Into<String>) -> ModelFailure {
    ModelFailure {
        fingerprint: FailureFingerprint::new(code, phase, invariant)
            .expect("static model failure fingerprint validates"),
        detail: detail.into(),
    }
}

#[derive(Default)]
struct DomainModel {
    runs: BTreeMap<String, ModelRun>,
    observed_variants: BTreeSet<&'static str>,
}

impl DomainModel {
    fn check(
        &mut self,
        command: &DurableCommand,
        response: &DurableResponse,
    ) -> Result<(), ModelFailure> {
        self.observed_variants.insert(command_phase(command));
        match command {
            DurableCommand::StartRun {
                run_id,
                candidate,
                input,
                ..
            } => self.check_start(run_id, candidate, input, response),
            DurableCommand::ResumeRun { run_id, .. } => self.check_resume(run_id, response),
            DurableCommand::ActivateWait {
                source, wait_ids, ..
            } => self.check_activation(source, wait_ids, response),
            DurableCommand::ReleaseEffect { intent_id, .. } => {
                self.check_release(intent_id, response)
            }
            DurableCommand::QueryRun { run_id, .. } => self.check_query(run_id, response),
            DurableCommand::QueryDomain { .. } => self.check_domain(response),
        }
    }

    fn check_start(
        &mut self,
        run_id: &str,
        candidate: &PlanCandidate,
        input: &Value,
        response: &DurableResponse,
    ) -> Result<(), ModelFailure> {
        if self.runs.contains_key(run_id) {
            return Err(failure(
                "trace_invalid",
                "start_run",
                "run_identity_unique",
                format!("generated trace started Run {run_id} twice"),
            ));
        }
        let plan_id = seal_plan(candidate.clone())
            .map_err(|error| {
                failure(
                    "trace_invalid",
                    "start_run",
                    "candidate_seals",
                    error.to_string(),
                )
            })?
            .plan_id;
        let kind = classify_candidate(run_id, candidate, input)?;
        let status = match &kind {
            ModelKind::Immediate => {
                check_completed(response, run_id, &plan_id, input, &[])?;
                ModelStatus::Completed
            }
            ModelKind::Signal { wait_id, .. } => {
                expect_response(
                    response,
                    &DurableResponse::RunBoundary {
                        boundary: DurableBoundary::Suspended {
                            wait_id: wait_id.clone(),
                        },
                    },
                    "start_run",
                    "signal_suspends",
                )?;
                ModelStatus::Waiting
            }
            ModelKind::ExplicitEffect { intent_id } => {
                expect_response(
                    response,
                    &DurableResponse::RunBoundary {
                        boundary: DurableBoundary::ReleaseRequired {
                            intent_ids: BTreeSet::from([intent_id.clone()]),
                        },
                    },
                    "start_run",
                    "explicit_requires_release",
                )?;
                ModelStatus::ReleaseRequired
            }
        };
        self.runs.insert(
            run_id.to_owned(),
            ModelRun {
                input: input.clone(),
                plan_id,
                kind,
                status,
                epoch: 0,
            },
        );
        Ok(())
    }

    fn check_activation(
        &mut self,
        source: &WaitActivationSource,
        wait_ids: &BTreeSet<String>,
        response: &DurableResponse,
    ) -> Result<(), ModelFailure> {
        let mut ready = BTreeSet::new();
        for wait_id in wait_ids {
            let Some((run_id, run)) = self.runs.iter_mut().find(|(_, run)|
                matches!(&run.kind, ModelKind::Signal { wait_id: expected, .. } if expected == wait_id)) else {
                return Err(failure("trace_invalid", "activate_wait", "target_exists", format!("unknown wait {wait_id}")));
            };
            let ModelKind::Signal { key, .. } = &run.kind else {
                unreachable!()
            };
            if source != &(WaitActivationSource::Signal { key: key.clone() })
                || run.status != ModelStatus::Waiting
            {
                return Err(failure(
                    "trace_invalid",
                    "activate_wait",
                    "source_and_state_match",
                    format!("delivery did not match pending wait {wait_id}"),
                ));
            }
            run.status = ModelStatus::Ready;
            ready.insert(run_id.clone());
        }
        expect_response(
            response,
            &DurableResponse::WaitActivated {
                ready_run_ids: ready,
            },
            "activate_wait",
            "ready_set_exact",
        )
    }

    fn check_resume(
        &mut self,
        run_id: &str,
        response: &DurableResponse,
    ) -> Result<(), ModelFailure> {
        let run = self.runs.get_mut(run_id).ok_or_else(|| {
            failure(
                "trace_invalid",
                "resume_run",
                "run_exists",
                format!("unknown Run {run_id}"),
            )
        })?;
        if run.status != ModelStatus::Ready {
            return Err(failure(
                "trace_invalid",
                "resume_run",
                "run_ready",
                format!("Run {run_id} was {:?}", run.status),
            ));
        }
        run.epoch += 1;
        check_completed(response, run_id, &run.plan_id, &run.input, &[])?;
        run.status = ModelStatus::Completed;
        Ok(())
    }

    fn check_release(
        &mut self,
        intent_id: &str,
        response: &DurableResponse,
    ) -> Result<(), ModelFailure> {
        let Some((run_id, run)) = self.runs.iter_mut().find(|(_, run)|
            matches!(&run.kind, ModelKind::ExplicitEffect { intent_id: expected } if expected == intent_id)) else {
            return Err(failure("trace_invalid", "release_effect", "intent_exists", format!("unknown intent {intent_id}")));
        };
        if run.status != ModelStatus::ReleaseRequired {
            return Err(failure(
                "trace_invalid",
                "release_effect",
                "release_pending",
                format!("intent {intent_id} was not pending release"),
            ));
        }
        check_completed(
            response,
            run_id,
            &run.plan_id,
            &run.input,
            &[intent_id.to_owned()],
        )?;
        run.status = ModelStatus::Completed;
        Ok(())
    }

    fn check_query(&self, run_id: &str, response: &DurableResponse) -> Result<(), ModelFailure> {
        let DurableResponse::Run { run: actual } = response else {
            return Err(failure(
                "model_mismatch",
                "query_run",
                "response_variant",
                format!("unexpected response {response:?}"),
            ));
        };
        let Some(expected) = self.runs.get(run_id) else {
            return if actual.is_none() {
                Ok(())
            } else {
                Err(failure(
                    "model_mismatch",
                    "query_run",
                    "absence_exact",
                    format!("unknown Run {run_id} was returned"),
                ))
            };
        };
        let actual = actual.as_ref().ok_or_else(|| {
            failure(
                "model_mismatch",
                "query_run",
                "presence_exact",
                format!("Run {run_id} was missing"),
            )
        })?;
        let expected_status = match expected.status {
            ModelStatus::Waiting | ModelStatus::ReleaseRequired => ContinuationStatus::Waiting,
            ModelStatus::Ready => ContinuationStatus::Ready,
            ModelStatus::Completed => ContinuationStatus::Completed,
        };
        if actual.continuation.run_id != run_id
            || actual.continuation.plan_id != expected.plan_id
            || actual.continuation.epoch != expected.epoch
            || actual.continuation.status != expected_status
        {
            return Err(failure(
                "model_mismatch",
                "query_run",
                "continuation_projection",
                format!("Run {run_id} projection diverged: {actual:?}"),
            ));
        }
        match &expected.kind {
            ModelKind::Signal { wait_id, .. } => {
                let state = actual
                    .waits
                    .iter()
                    .find(|wait| wait.wait_id == *wait_id)
                    .map(|wait| wait.state);
                let expected_state = match expected.status {
                    ModelStatus::Waiting => Some(WaitState::Pending),
                    ModelStatus::Ready | ModelStatus::Completed => Some(WaitState::Completed),
                    ModelStatus::ReleaseRequired => None,
                };
                if state != expected_state {
                    return Err(failure(
                        "model_mismatch",
                        "query_run",
                        "wait_projection",
                        format!("Run {run_id} wait projection diverged"),
                    ));
                }
            }
            ModelKind::ExplicitEffect { intent_id } => {
                let state = actual
                    .effects
                    .iter()
                    .find(|effect| effect.intent_id == *intent_id)
                    .map(|effect| effect.state);
                let expected_state = match expected.status {
                    ModelStatus::ReleaseRequired => Some(OutboxState::Pending),
                    ModelStatus::Completed => Some(OutboxState::Applied),
                    ModelStatus::Waiting | ModelStatus::Ready => None,
                };
                if state != expected_state {
                    return Err(failure(
                        "model_mismatch",
                        "query_run",
                        "effect_projection",
                        format!("Run {run_id} effect projection diverged"),
                    ));
                }
            }
            ModelKind::Immediate => {}
        }
        if actual.result.is_some() != (expected.status == ModelStatus::Completed) {
            return Err(failure(
                "model_mismatch",
                "query_run",
                "result_presence",
                format!("Run {run_id} result presence diverged"),
            ));
        }
        Ok(())
    }

    fn check_domain(&self, response: &DurableResponse) -> Result<(), ModelFailure> {
        let DurableResponse::Domain { domain } = response else {
            return Err(failure(
                "model_mismatch",
                "query_domain",
                "response_variant",
                format!("unexpected response {response:?}"),
            ));
        };
        let expected: Vec<_> = self.runs.keys().cloned().collect();
        if domain.run_ids != expected || domain.revision.is_some() == expected.is_empty() {
            return Err(failure(
                "model_mismatch",
                "query_domain",
                "run_index_and_revision",
                format!("domain {domain:?} diverged from {expected:?}"),
            ));
        }
        Ok(())
    }

    fn verify_coverage(&self) -> Result<(), ModelFailure> {
        let expected = BTreeSet::from([
            "start_run",
            "resume_run",
            "activate_wait",
            "release_effect",
            "query_run",
            "query_domain",
        ]);
        if self.observed_variants != expected {
            return Err(failure(
                "coverage_mismatch",
                "durable_trace",
                "all_command_variants",
                format!(
                    "observed {:?}, expected {expected:?}",
                    self.observed_variants
                ),
            ));
        }
        Ok(())
    }
}

#[test]
fn generated_durable_commands_match_the_reference_model() {
    for seed in requested_seeds(32).expect("generated seeds select") {
        let case = generate_case(seed).expect("generated case validates");
        if let Err(cause) = run_case(&case) {
            let fingerprint = cause.fingerprint.clone();
            let minimized = case.minimize_failure(&fingerprint, |candidate| {
                run_case(candidate).map_err(|failure| failure.fingerprint)
            });
            let failure = TraceFailure {
                seed,
                path: minimized.identity.path.clone(),
                replay_command: ReplaySpec {
                    package: "cymule-durable",
                    test_target: "model_trace",
                    test_name: "generated_durable_commands_match_the_reference_model",
                }
                .command(seed),
                minimized_fixture: minimized
                    .fixture_json()
                    .expect("minimized fixture serializes"),
                fingerprint,
                cause: cause.to_string(),
            };
            panic!("{failure}");
        }
    }
}

fn generate_case(seed: u64) -> Result<TraceCase<DurableCommand>, String> {
    let immediate_run = "run:trace:immediate";
    let signal_run = "run:trace:signal";
    let effect_run = "run:trace:effect";
    let signal_input = json!({"kind": "signal", "seed": seed});
    let effect_input = json!({"kind": "effect", "seed": seed});
    let wait_id = signal_wait_id(signal_run)?;
    let intent_id = explicit_intent_id(effect_run, &effect_input)?;
    let mut commands = vec![
        start_command(
            immediate_run,
            identity_candidate(),
            json!({"kind": "immediate", "seed": seed}),
        ),
        query_run(seed, 1, immediate_run),
        start_command(signal_run, signal_candidate(), signal_input),
        query_run(seed, 3, signal_run),
        DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: format!("activation:trace:{seed}"),
            source: WaitActivationSource::Signal {
                key: SIGNAL_KEY.to_owned(),
            },
            wait_ids: BTreeSet::from([wait_id]),
            value: json!({"approved": true, "seed": seed}),
        },
        DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: signal_run.to_owned(),
        },
        start_command(effect_run, explicit_effect_candidate(), effect_input),
        DurableCommand::ReleaseEffect {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            intent_id,
        },
        query_run(seed, 8, effect_run),
        DurableCommand::QueryDomain {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            query_id: format!("query:trace:{seed}:domain"),
        },
    ];
    let mut random = SeededRandom::new(seed);
    for position in 10..12 {
        let run_id = [immediate_run, signal_run, effect_run]
            [random.index(3).map_err(|error| error.to_string())?];
        commands.push(query_run(seed, position, run_id));
    }
    let mut positions = vec![0usize, 2, 4, 5, 6, 7];
    let mut steps = Vec::new();
    while steps.len() < 4 {
        let index = random
            .index(positions.len())
            .map_err(|error| error.to_string())?;
        let position = positions.remove(index);
        steps.push(FaultStep {
            operation: CAS_OPERATION.to_owned(),
            path: vec![position],
            occurrence: 1,
            action: if random.next_u64() & 1 == 0 {
                FaultAction::ErrorBefore
            } else {
                FaultAction::AcknowledgementLostAfter
            },
        });
    }
    TraceCase::new(
        seed,
        commands,
        FaultPlan::new(steps).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn run_case(case: &TraceCase<DurableCommand>) -> Result<(), ModelFailure> {
    let mut world =
        TestWorld::with_faults(case.identity.seed, case.faults.clone()).map_err(|error| {
            failure(
                "fixture_error",
                "durable_trace",
                "test_world",
                error.to_string(),
            )
        })?;
    let active_path = Rc::new(Cell::new(0));
    let store = FaultingStore {
        inner: MemoryStore::new(),
        faults: world.faults_mut().clone(),
        active_path: Rc::clone(&active_path),
    };
    let runtime = open_runtime(store, TracePlugin::default()).map_err(|error| {
        failure(
            "runtime_error",
            "durable_trace",
            "runtime_opens",
            error.to_string(),
        )
    })?;
    let mut control = DurableRuntimeControl::new(runtime);
    let mut model = DomainModel::default();
    for (position, command) in case.commands.iter().enumerate() {
        active_path.set(case.identity.path[position]);
        let response = loop {
            match control.submit(command.clone()) {
                Ok(response) => break response,
                Err(DurableError::Substrate(_)) => {
                    let (store, plugin) = control.into_runtime().into_parts();
                    control = DurableRuntimeControl::new(open_runtime(store, plugin).map_err(
                        |error| {
                            failure(
                                "runtime_error",
                                command_phase(command),
                                "reopen_succeeds",
                                error.to_string(),
                            )
                        },
                    )?);
                }
                Err(error) => {
                    return Err(failure(
                        "runtime_error",
                        command_phase(command),
                        "command_succeeds",
                        format!("command {position} failed: {error}"),
                    ));
                }
            }
        };
        model.check(command, &response)?;
        let now = world.clock_mut().advance(1).map_err(|error| {
            failure(
                "fixture_error",
                "durable_trace",
                "clock_advances",
                error.to_string(),
            )
        })?;
        world
            .observer_mut()
            .record(
                now,
                "durable_command_completed",
                BTreeMap::from([("position".to_owned(), json!(case.identity.path[position]))]),
            )
            .map_err(|error| {
                failure(
                    "fixture_error",
                    "durable_trace",
                    "observation_records",
                    error.to_string(),
                )
            })?;
    }
    model.verify_coverage()?;
    if world.observer().observations().len() != case.commands.len() {
        return Err(failure(
            "model_mismatch",
            "durable_trace",
            "observation_count",
            "recording observer lost a completed command",
        ));
    }
    Ok(())
}

fn expect_response(
    actual: &DurableResponse,
    expected: &DurableResponse,
    phase: &str,
    invariant: &str,
) -> Result<(), ModelFailure> {
    if actual == expected {
        Ok(())
    } else {
        Err(failure(
            "model_mismatch",
            phase,
            invariant,
            format!("actual {actual:?}, expected {expected:?}"),
        ))
    }
}

fn check_completed(
    response: &DurableResponse,
    run_id: &str,
    plan_id: &str,
    value: &Value,
    effects: &[String],
) -> Result<(), ModelFailure> {
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Completed { result },
    } = response
    else {
        return Err(failure(
            "model_mismatch",
            "run_completion",
            "completed_boundary",
            format!("Run {run_id} returned {response:?}"),
        ));
    };
    let ExecutionResult {
        run_id: actual_run,
        plan_id: actual_plan,
        value: actual_value,
        projection_digest,
        precondition_token,
        effects: actual_effects,
    } = result;
    if actual_run != run_id
        || actual_plan != plan_id
        || actual_value != value
        || actual_effects != effects
        || projection_digest.is_empty()
        || precondition_token.is_empty()
    {
        return Err(failure(
            "model_mismatch",
            "run_completion",
            "receipt_exact",
            format!("Run {run_id} completion diverged: {result:?}"),
        ));
    }
    Ok(())
}

fn classify_candidate(
    run_id: &str,
    candidate: &PlanCandidate,
    input: &Value,
) -> Result<ModelKind, ModelFailure> {
    match candidate.name.as_str() {
        "trace_identity" => Ok(ModelKind::Immediate),
        "trace_signal" => Ok(ModelKind::Signal {
            key: SIGNAL_KEY.to_owned(),
            wait_id: signal_wait_id(run_id)
                .map_err(|error| failure("fixture_error", "start_run", "wait_identity", error))?,
        }),
        "trace_explicit_effect" => Ok(ModelKind::ExplicitEffect {
            intent_id: explicit_intent_id(run_id, input)
                .map_err(|error| failure("fixture_error", "start_run", "effect_identity", error))?,
        }),
        other => Err(failure(
            "trace_invalid",
            "start_run",
            "known_candidate",
            format!("unknown generated candidate {other}"),
        )),
    }
}

fn command_phase(command: &DurableCommand) -> &'static str {
    match command {
        DurableCommand::StartRun { .. } => "start_run",
        DurableCommand::ResumeRun { .. } => "resume_run",
        DurableCommand::ActivateWait { .. } => "activate_wait",
        DurableCommand::ReleaseEffect { .. } => "release_effect",
        DurableCommand::QueryRun { .. } => "query_run",
        DurableCommand::QueryDomain { .. } => "query_domain",
    }
}

fn start_command(run_id: &str, candidate: PlanCandidate, input: Value) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate,
        input,
    }
}

fn query_run(seed: u64, position: usize, run_id: &str) -> DurableCommand {
    DurableCommand::QueryRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        query_id: format!("query:trace:{seed}:{position}"),
        run_id: run_id.to_owned(),
    }
}

fn signal_wait_id(run_id: &str) -> Result<String, String> {
    content_id("cymule.wait/1", &(run_id, "main", WAIT_SITE, 0_u64))
        .map_err(|error| error.to_string())
}

fn explicit_intent_id(run_id: &str, input: &Value) -> Result<String, String> {
    let bytes = canonical_bytes(input).map_err(|error| error.to_string())?;
    let args = artifact_ref("cymule.effect-args/1", &bytes).map_err(|error| error.to_string())?;
    effect_intent_id(
        run_id,
        "main",
        EFFECT_SITE,
        ROOT_SCOPE_ID,
        0,
        EFFECT_OCCURRENCE,
        &args,
        "cymule.effect-schema/1",
    )
    .map_err(|error| error.to_string())
}

fn base_candidate(name: &str, steps: Vec<Step>) -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: name.to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps,
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn identity_candidate() -> PlanCandidate {
    base_candidate("trace_identity", Vec::new())
}

fn signal_candidate() -> PlanCandidate {
    base_candidate(
        "trace_signal",
        vec![Step {
            id: WAIT_SITE.to_owned(),
            operation: Operation::Wait {
                wait: WaitSpec::Signal {
                    key: SIGNAL_KEY.to_owned(),
                    consume_once: false,
                },
            },
        }],
    )
}

fn explicit_effect_candidate() -> PlanCandidate {
    let mut candidate = base_candidate(
        "trace_explicit_effect",
        vec![Step {
            id: EFFECT_SITE.to_owned(),
            operation: Operation::Effect {
                effect: EFFECT_OPERATION.to_owned(),
                input: Expression::Input,
                occurrence: EFFECT_OCCURRENCE.to_owned(),
                bind: None,
            },
        }],
    );
    candidate.effects.push(EffectContract {
        id: EFFECT_OPERATION.to_owned(),
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
    });
    candidate
}
