use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use cymule_core::content_id;
use cymule_core::{
    Definition, Expression, IR_VERSION, Operation, PlanCandidate, Region, Step, WaitSpec,
};
use cymule_durable::{
    ClockObservationAuthority, DurableError, DurableResult, DurableRuntimeControl, DurableStore,
    ExecutionClockAuthority,
};
use cymule_durable_protocol::{
    CLOCK_OBSERVATION_VERSION, ClockObservation, ClockObservationRef, ExecutionClaimRequest,
    clock_observation_id, execution_clock_scope,
};
use cymule_runtime::{BoundPluginHost, ExecutionBinding, ExecutionBindingAdmission};
use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError,
    RuntimeResult,
};
use serde_json::Value;

const SOURCE_ID: &str = "clock:durable-test";
const SOURCE_GENERATION: &str =
    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

#[derive(Default)]
struct IssuedLedger {
    logical_by_scope: BTreeMap<String, u64>,
    observations: BTreeMap<String, ClockObservation>,
    current_by_scope: BTreeMap<String, String>,
}

fn ledger() -> &'static Mutex<IssuedLedger> {
    static LEDGER: OnceLock<Mutex<IssuedLedger>> = OnceLock::new();
    LEDGER.get_or_init(|| Mutex::new(IssuedLedger::default()))
}

/// Test-only current-head authority backed by a retained issuance ledger.
#[derive(Debug, Clone, Copy, Default)]
pub struct IssuedClock;

impl ClockObservationAuthority for IssuedClock {
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        let observation = ledger()
            .lock()
            .map_err(|error| DurableError::Substrate {
                code: "test_clock_ledger_poisoned".to_owned(),
                message: error.to_string(),
            })?
            .observations
            .get(&reference.observation_id)
            .cloned()
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "Clock observation {} was not issued",
                    reference.observation_id
                ))
            })?;
        if observation.reference() != *reference {
            return Err(DurableError::Integrity {
                code: "test_clock_reference_mismatch".to_owned(),
                message: "Clock reference does not match its retained receipt".to_owned(),
            });
        }
        Ok(observation)
    }
}

impl ExecutionClockAuthority for IssuedClock {
    fn with_current_head(
        &mut self,
        reference: &ClockObservationRef,
        commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        let ledger = ledger().lock().map_err(|error| DurableError::Substrate {
            code: "test_clock_ledger_poisoned".to_owned(),
            message: error.to_string(),
        })?;
        let observation = ledger
            .observations
            .get(&reference.observation_id)
            .cloned()
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "Clock observation {} was not issued",
                    reference.observation_id
                ))
            })?;
        if observation.reference() != *reference {
            return Err(DurableError::Integrity {
                code: "test_clock_reference_mismatch".to_owned(),
                message: "Clock reference does not match its retained receipt".to_owned(),
            });
        }
        let current = ledger
            .current_by_scope
            .get(&reference.scope)
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "Clock scope {} has no issued head",
                    reference.scope
                ))
            })?;
        if current != &reference.observation_id {
            return Err(DurableError::Conflict {
                expected: Some(reference.observation_id.clone()),
                current: Some(current.clone()),
            });
        }
        commit(&observation)
    }
}

/// Issue the next exact Clock observation for a Run's execution scope.
///
/// # Panics
///
/// Panics for an invalid Run identity, a poisoned test ledger, a logical Clock
/// outside its validated range, or test evidence that cannot be sealed.
pub fn execution(run_id: &str) -> ExecutionClaimRequest {
    let scope = execution_clock_scope(run_id).expect("test Run has a valid Clock scope");
    let mut ledger = ledger().lock().expect("test Clock ledger locks");
    let logical_time = ledger
        .logical_by_scope
        .get(&scope)
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .expect("test logical Clock does not overflow");
    ledger.logical_by_scope.insert(scope.clone(), logical_time);
    let observation_id = clock_observation_id(
        SOURCE_ID,
        SOURCE_GENERATION,
        &scope,
        logical_time,
        logical_time,
    )
    .expect("test Clock observation seals");
    let observation = ClockObservation {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id: observation_id.clone(),
        source_id: SOURCE_ID.to_owned(),
        source_generation: SOURCE_GENERATION.to_owned(),
        scope,
        logical_time,
        observed_unix_ms: logical_time,
    };
    observation.verify().expect("test Clock receipt verifies");
    ledger
        .observations
        .insert(observation_id.clone(), observation.clone());
    ledger
        .current_by_scope
        .insert(observation.scope.clone(), observation_id);
    ExecutionClaimRequest {
        owner: content_id("cymule.test-driver/1", &run_id).expect("test owner seals"),
        clock: observation.reference(),
        ttl: 1,
    }
}

/// Admit a test provider before opening writable durable control.
///
/// # Errors
///
/// Returns an error when provider binding admission or opening the store fails.
pub fn open_control<S, P>(
    store: S,
    plugin: P,
    binding: ExecutionBinding,
) -> DurableResult<DurableRuntimeControl<S, P>>
where
    S: DurableStore,
    P: BoundPluginHost,
{
    let admission =
        ExecutionBindingAdmission::admit(plugin, binding).map_err(DurableError::from)?;
    DurableRuntimeControl::open(store, admission, IssuedClock)
}

/// Provider used by Plans containing no external component or Effect operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyPlugin;

impl PluginHost for EmptyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: empty_manifest(),
            }),
            other => Err(RuntimeError::plugin_defect(format!(
                "unexpected empty-plugin request: {other:?}"
            ))),
        }
    }
}

/// Return the exact provider manifest for operation-free execution.
pub fn empty_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "durable-public-tests@1".to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::new(),
    }
}

/// Bind the operation-free test provider to one immutable process generation.
///
/// # Panics
///
/// Panics if the fixed test manifest or process identity cannot form a binding.
pub fn empty_binding() -> ExecutionBinding {
    ExecutionBinding::for_local_process(
        &empty_manifest(),
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    )
    .expect("test binding derives")
}

/// Build a Plan candidate that returns its initial input.
pub fn identity_candidate(name: &str) -> PlanCandidate {
    candidate(name, Vec::new())
}

/// Build an input-preserving Plan parked on one identified signal.
pub fn signal_candidate(name: &str, key: &str, consume_once: bool) -> PlanCandidate {
    candidate(
        name,
        vec![Step {
            id: "wait.signal".to_owned(),
            operation: Operation::Wait {
                wait: WaitSpec::Signal {
                    key: key.to_owned(),
                    consume_once,
                },
                bind: None,
            },
        }],
    )
}

fn candidate(name: &str, steps: Vec<Step>) -> PlanCandidate {
    PlanCandidate {
        ir_version: IR_VERSION.to_owned(),
        name: name.to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            body: Region {
                steps,
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

/// Require a completed public Run boundary and return its canonical value.
///
/// # Panics
///
/// Panics when the response is not a completed Run boundary.
pub fn expect_completed_value(response: cymule_durable::DurableResponse) -> Value {
    let cymule_durable::DurableResponse::RunBoundary {
        boundary: cymule_durable::DurableBoundary::Completed { result },
    } = response
    else {
        panic!("expected completed Run boundary, got {response:?}")
    };
    result.value
}
