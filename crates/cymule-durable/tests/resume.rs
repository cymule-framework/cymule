//! Restart-level resumable interpreter tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cymule_core::{
    ComponentContract, Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec,
};
use cymule_durable::{DriveOutcome, MemoryStore, ResumableRuntime};
use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginOperation, PluginRequest, PluginResponse,
    RuntimeError, RuntimeResult,
};
use serde_json::json;

struct CountingPlugin {
    calls: Arc<AtomicUsize>,
}

impl PluginHost for CountingPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "resume-test@1".to_owned(),
                    components: BTreeMap::from([(
                        "test.greet".to_owned(),
                        PluginOperation {
                            implementation_revision: "1".to_owned(),
                        },
                    )]),
                    effects: BTreeMap::new(),
                },
            }),
            PluginRequest::Call { component, input } if component == "test.greet" => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(PluginResponse::CallResult {
                    value: json!({"greeting": format!("Hello, {}!", input["name"].as_str().unwrap())}),
                })
            }
            request => Err(RuntimeError::Plugin(format!(
                "unsupported resume test request: {request:?}"
            ))),
        }
    }
}

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "resume_after_input".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.greet".to_owned(),
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
                steps: vec![
                    Step {
                        id: "call.greet".to_owned(),
                        operation: Operation::Call {
                            component: "test.greet".to_owned(),
                            input: Expression::Input,
                            bind: Some("greeting".to_owned()),
                        },
                    },
                    Step {
                        id: "wait.approval".to_owned(),
                        operation: Operation::Wait {
                            wait: WaitSpec::Input {
                                correlation: "approval".to_owned(),
                                schema: json!({"type": "boolean"}),
                            },
                        },
                    },
                ],
                result: Expression::Binding {
                    name: "greeting".to_owned(),
                },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

#[test]
fn process_reopen_resumes_after_wait_without_reinvoking_component() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = ResumableRuntime::open(
        MemoryStore::new(),
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime opens");
    let DriveOutcome::Suspended { wait_id } = runtime
        .start(candidate(), &json!({"name": "Ada"}), "run:resume")
        .expect("run reaches wait")
    else {
        panic!("run should suspend");
    };
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let (store, _) = runtime.into_parts();
    let mut reopened = ResumableRuntime::open(
        store,
        CountingPlugin {
            calls: calls.clone(),
        },
    )
    .expect("runtime reopens");
    let DriveOutcome::Completed(result) = reopened
        .complete_wait(&wait_id, &json!(true))
        .expect("run resumes and completes")
    else {
        panic!("run should complete");
    };
    assert_eq!(result.value, json!({"greeting": "Hello, Ada!"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reopened
            .coordinator()
            .state()
            .expect("state")
            .component_occurrences
            .len(),
        1
    );
}
