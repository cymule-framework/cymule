//! Generated command traces checked against a small durable-domain reference model.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{Definition, Expression, PlanCandidate, Region};
use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableError, DurableResponse,
    DurableResult, DurableRuntimeControl, DurableState, DurableStore, MemoryStore,
    ResumableRuntime, StoreCommit, StoredState,
};
use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError,
    RuntimeResult,
};
use cymule_test_world::{
    FaultAction, FaultPlan, FaultSchedule, FaultStep, ReplaySpec, SeededRandom, TestWorld,
    TraceCase, TraceFailure, requested_seeds,
};
use serde_json::{Value, json};

const CAS_OPERATION: &str = "durable.compare_and_swap";

struct EmptyPlugin;

impl PluginHost for EmptyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "model-trace-empty@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::new(),
                },
            }),
            other => Err(RuntimeError::plugin_defect(format!(
                "model trace received unexpected plugin request: {other:?}"
            ))),
        }
    }
}

struct FaultingStore {
    inner: MemoryStore,
    faults: FaultSchedule,
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
        let action = self.faults.observe(CAS_OPERATION);
        if action == Some(FaultAction::ErrorBefore) {
            return Err(DurableError::Substrate(
                "generated failure before durable CAS".to_owned(),
            ));
        }
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if action == Some(FaultAction::AcknowledgementLostAfter) {
            return Err(DurableError::Substrate(
                "generated acknowledgement loss after durable CAS".to_owned(),
            ));
        }
        Ok(commit)
    }
}

#[derive(Default)]
struct DomainModel {
    inputs: BTreeMap<String, Value>,
}

impl DomainModel {
    fn check(
        &mut self,
        command: &DurableCommand,
        response: &DurableResponse,
    ) -> Result<(), String> {
        match (command, response) {
            (
                DurableCommand::StartRun { run_id, input, .. },
                DurableResponse::RunBoundary {
                    boundary: DurableBoundary::Completed { result },
                },
            ) => {
                if result.value != *input {
                    return Err(format!(
                        "Run {run_id} returned {}, expected {input}",
                        result.value
                    ));
                }
                match self.inputs.get(run_id) {
                    Some(retained) if retained != input => {
                        return Err(format!("Run {run_id} changed its retained input"));
                    }
                    Some(_) => {}
                    None => {
                        self.inputs.insert(run_id.clone(), input.clone());
                    }
                }
            }
            (DurableCommand::StartRun { run_id, .. }, other) => {
                return Err(format!("Run {run_id} did not complete: {other:?}"));
            }
            (DurableCommand::QueryRun { run_id, .. }, DurableResponse::Run { run }) => {
                if run.is_some() != self.inputs.contains_key(run_id) {
                    return Err(format!("Run query presence diverged for {run_id}"));
                }
            }
            (DurableCommand::QueryDomain { .. }, DurableResponse::Domain { domain }) => {
                let expected: Vec<_> = self.inputs.keys().cloned().collect();
                if domain.run_ids != expected {
                    return Err(format!(
                        "domain index diverged: actual {:?}, expected {expected:?}",
                        domain.run_ids
                    ));
                }
                if domain.revision.is_some() == expected.is_empty() {
                    return Err("domain revision presence diverged from retained Runs".to_owned());
                }
            }
            (command, response) => {
                return Err(format!(
                    "unexpected command/response pair: {command:?} -> {response:?}"
                ));
            }
        }
        Ok(())
    }
}

#[test]
fn generated_durable_commands_match_the_reference_model() {
    for seed in requested_seeds(32).expect("generated seeds select") {
        let case = generate_case(seed).expect("generated case validates");
        if let Err(cause) = run_case(&case) {
            let minimized = case.minimize_failure(run_case);
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
                cause,
            };
            panic!("{failure}");
        }
    }
}

fn generate_case(seed: u64) -> Result<TraceCase<DurableCommand>, String> {
    let mut random = SeededRandom::new(seed);
    let mut commands = vec![start_command(0)];
    for command_index in 1..12 {
        let run_index = random.index(4).map_err(|error| error.to_string())?;
        let command = match random.index(3).map_err(|error| error.to_string())? {
            0 => start_command(run_index),
            1 => DurableCommand::QueryRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                query_id: format!("query:trace:{seed}:{command_index}:run"),
                run_id: format!("run:trace:{run_index}"),
            },
            _ => DurableCommand::QueryDomain {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                query_id: format!("query:trace:{seed}:{command_index}:domain"),
            },
        };
        commands.push(command);
    }

    let mut selected = BTreeSet::new();
    let mut steps = Vec::new();
    while steps.len() < 4 {
        let occurrence = u64::try_from(random.index(32).map_err(|error| error.to_string())? + 1)
            .map_err(|error| error.to_string())?;
        if selected.insert(occurrence) {
            steps.push(FaultStep {
                operation: CAS_OPERATION.to_owned(),
                occurrence,
                action: if random.next_u64() & 1 == 0 {
                    FaultAction::ErrorBefore
                } else {
                    FaultAction::AcknowledgementLostAfter
                },
            });
        }
    }
    TraceCase::new(
        seed,
        commands,
        FaultPlan::new(steps).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn run_case(case: &TraceCase<DurableCommand>) -> Result<(), String> {
    let mut world = TestWorld::with_faults(case.identity.seed, case.faults.clone())
        .map_err(|error| error.to_string())?;
    let _fixture_path = world
        .domain()
        .path("minimized-trace.json")
        .map_err(|error| error.to_string())?;
    let store = FaultingStore {
        inner: MemoryStore::new(),
        faults: world.faults_mut().clone(),
    };
    let runtime = ResumableRuntime::open(store, EmptyPlugin).map_err(|error| error.to_string())?;
    let mut control = DurableRuntimeControl::new(runtime);
    let mut model = DomainModel::default();

    for (position, command) in case.commands.iter().enumerate() {
        let response = loop {
            match control.submit(command.clone()) {
                Ok(response) => break response,
                Err(DurableError::Substrate(_)) => {
                    let (store, plugin) = control.into_runtime().into_parts();
                    control = DurableRuntimeControl::new(
                        ResumableRuntime::open(store, plugin)
                            .map_err(|error| format!("reopen failed: {error}"))?,
                    );
                }
                Err(error) => return Err(format!("command {position} failed: {error}")),
            }
        };
        model.check(command, &response)?;
        let now = world
            .clock_mut()
            .advance(1)
            .map_err(|error| error.to_string())?;
        world
            .observer_mut()
            .record(
                now,
                "durable_command_completed",
                BTreeMap::from([("position".to_owned(), json!(position))]),
            )
            .map_err(|error| error.to_string())?;
    }
    if world.observer().observations().len() != case.commands.len() {
        return Err("recording observer lost a completed command".to_owned());
    }
    Ok(())
}

fn start_command(run_index: usize) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: format!("run:trace:{run_index}"),
        candidate: identity_candidate(),
        input: json!({"run": run_index}),
    }
}

fn identity_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "generated_identity".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: Vec::new(),
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}
