use std::collections::BTreeMap;

use cymule_core::{ReconciliationResolution, WorldOutcome};
use cymule_runtime::{
    PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::json;

/// Reviewed implementation generation for the in-process plugin/3 provider.
/// Advancing provider behavior requires advancing this source before producing
/// a new execution binding.
pub const HELLO_PLUGIN_REVIEWED_REVISION_SOURCE: &[u8] = b"cymule-example-hello-world-plugin/2";

/// In-process Embedded example provider. Its settlement ledger demonstrates
/// plugin/3 linearization but is intentionally not durable across process
/// restart and therefore is not an M1 recovery provider.
pub struct HelloPlugin {
    unknown_once: bool,
    settlements: BTreeMap<String, HelloEffectEntry>,
}

#[derive(Clone)]
struct HelloEffectEntry {
    attempt: cymule_runtime::EffectProviderAttempt,
    input: serde_json::Value,
    state: HelloEffectState,
    result: Option<serde_json::Value>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HelloEffectState {
    Dispatching,
    Applied,
    NotApplied,
}

impl HelloPlugin {
    pub fn new(unknown_once: bool) -> Self {
        Self {
            unknown_once,
            settlements: BTreeMap::new(),
        }
    }

    /// Immutable reviewed implementation revision used by the example binding.
    pub fn implementation_revision() -> String {
        format!(
            "sha256:{}",
            cymule_core::sha256_bytes(HELLO_PLUGIN_REVIEWED_REVISION_SOURCE)
        )
    }
}

impl PluginHost for HelloPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "example-hello-plugin@2".to_owned(),
                    components: BTreeMap::from([(
                        "example.greet".to_owned(),
                        PluginOperation {
                            implementation_revision: "2".to_owned(),
                        },
                    )]),
                    effects: BTreeMap::from([(
                        "example.capture".to_owned(),
                        PluginEffect {
                            implementation_revision: "2".to_owned(),
                            can_reconcile: true,
                        },
                    )]),
                },
            }),
            PluginRequest::Call { component, input } if component == "example.greet" => {
                let name = input
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        RuntimeError::plugin_defect("example.greet requires a string name")
                    })?;
                Ok(PluginResponse::CallResult {
                    value: json!({"message": format!("Hello, {name}!")}),
                })
            }
            PluginRequest::PrepareEffect { operation, .. } if operation == "example.capture" => {
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::DispatchEffect {
                operation,
                intent_id,
                attempt,
                input,
            } if operation == "example.capture" => {
                if let Some(entry) = self.settlements.get(&intent_id) {
                    if entry.attempt != attempt || entry.input != input {
                        return Err(RuntimeError::plugin_defect(
                            "capture intent was reused outside its exact provider attempt",
                        ));
                    }
                    let outcome = match entry.state {
                        HelloEffectState::Dispatching => WorldOutcome::Unknown,
                        HelloEffectState::Applied => WorldOutcome::Applied,
                        HelloEffectState::NotApplied => WorldOutcome::NotApplied,
                    };
                    return Ok(PluginResponse::EffectResult {
                        attempt,
                        outcome,
                        value: entry.result.clone(),
                    });
                }
                self.settlements.insert(
                    intent_id.clone(),
                    HelloEffectEntry {
                        attempt: attempt.clone(),
                        input: input.clone(),
                        state: HelloEffectState::Dispatching,
                        result: None,
                    },
                );
                eprintln!("captured {intent_id}: {}", input["message"]);
                let entry = self
                    .settlements
                    .get_mut(&intent_id)
                    .expect("inserted capture remains");
                entry.state = HelloEffectState::Applied;
                entry.result = Some(input.clone());
                if self.unknown_once {
                    self.unknown_once = false;
                    eprintln!("simulating an ambiguous dispatch observation");
                    return Ok(PluginResponse::EffectResult {
                        attempt,
                        outcome: WorldOutcome::Unknown,
                        value: None,
                    });
                }
                Ok(PluginResponse::EffectResult {
                    attempt,
                    outcome: WorldOutcome::Applied,
                    value: Some(input),
                })
            }
            PluginRequest::ReconcileEffect {
                operation,
                intent_id,
                attempt,
                decision,
                resolution_value,
                input,
            } if operation == "example.capture" => {
                let entry = self.settlements.entry(intent_id).or_insert_with(|| {
                    let (state, result) = match decision {
                        cymule_runtime::EffectReconciliationDecision::ResolveApplied => {
                            (HelloEffectState::Applied, resolution_value)
                        }
                        cymule_runtime::EffectReconciliationDecision::Observe
                        | cymule_runtime::EffectReconciliationDecision::ResolveNotApplied => {
                            (HelloEffectState::NotApplied, None)
                        }
                    };
                    HelloEffectEntry {
                        attempt: attempt.clone(),
                        input: input.clone(),
                        state,
                        result,
                    }
                });
                if entry.attempt != attempt || entry.input != input {
                    return Err(RuntimeError::plugin_defect(
                        "capture intent was reused outside its exact provider attempt",
                    ));
                }
                let resolution = match entry.state {
                    HelloEffectState::Dispatching => ReconciliationResolution::StillUnknown,
                    HelloEffectState::Applied => ReconciliationResolution::ResolvedApplied,
                    HelloEffectState::NotApplied => ReconciliationResolution::ResolvedNotApplied,
                };
                eprintln!("reconciled the original capture intent as {resolution:?}");
                Ok(PluginResponse::ReconciliationResult {
                    attempt,
                    resolution,
                    value: entry.result.clone(),
                })
            }
            request => Err(RuntimeError::plugin_defect(format!(
                "unsupported Hello World request: {request:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_runtime::{
        EffectProviderAttempt, EffectReconciliationDecision, EmbeddedRuntime, ExecutionBinding,
        PluginRequest, PluginResponse,
    };

    #[test]
    fn governance_applied_result_is_replayed_exactly() {
        let mut plugin = HelloPlugin::new(false);
        let intent_id = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let attempt = EffectProviderAttempt::new(intent_id, "owner:hello-governance", 1).unwrap();
        let input = json!({"message": "hello"});
        let governed = json!({"receipt": "provider-truth"});

        let first = plugin
            .invoke(PluginRequest::ReconcileEffect {
                operation: "example.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt: attempt.clone(),
                decision: EffectReconciliationDecision::ResolveApplied,
                resolution_value: Some(governed.clone()),
                input: input.clone(),
            })
            .unwrap();
        assert!(matches!(
            first,
            PluginResponse::ReconciliationResult {
                resolution: ReconciliationResolution::ResolvedApplied,
                value: Some(value),
                ..
            } if value == governed
        ));

        let replay = plugin
            .invoke(PluginRequest::ReconcileEffect {
                operation: "example.capture".to_owned(),
                intent_id: intent_id.to_owned(),
                attempt,
                decision: EffectReconciliationDecision::Observe,
                resolution_value: None,
                input,
            })
            .unwrap();
        assert!(matches!(
            replay,
            PluginResponse::ReconciliationResult {
                resolution: ReconciliationResolution::ResolvedApplied,
                value: Some(value),
                ..
            } if value == governed
        ));
    }

    #[test]
    fn resultless_applied_resolution_remains_resultless_on_replay() {
        let mut plugin = HelloPlugin::new(false);
        let intent_id = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let attempt = EffectProviderAttempt::new(intent_id, "owner:hello-resultless", 1).unwrap();
        let input = json!({"message": "hello"});

        for decision in [
            EffectReconciliationDecision::ResolveApplied,
            EffectReconciliationDecision::Observe,
        ] {
            let response = plugin
                .invoke(PluginRequest::ReconcileEffect {
                    operation: "example.capture".to_owned(),
                    intent_id: intent_id.to_owned(),
                    attempt: attempt.clone(),
                    decision,
                    resolution_value: None,
                    input: input.clone(),
                })
                .unwrap();
            assert!(matches!(
                response,
                PluginResponse::ReconciliationResult {
                    resolution: ReconciliationResolution::ResolvedApplied,
                    value: None,
                    ..
                }
            ));
        }
    }

    #[test]
    fn reviewed_provider_change_advances_binding_without_changing_the_plan() {
        let mut plugin = HelloPlugin::new(false);
        let manifest = plugin.describe().unwrap();
        let current =
            ExecutionBinding::for_local_process(&manifest, HelloPlugin::implementation_revision())
                .unwrap();
        let old_revision = format!(
            "sha256:{}",
            cymule_core::sha256_bytes(b"cymule-example-hello-world-plugin/1")
        );
        let historical = ExecutionBinding::for_local_process(&manifest, old_revision).unwrap();

        assert_ne!(
            current.artifact_ref().unwrap(),
            historical.artifact_ref().unwrap()
        );
        let mut current_runtime = EmbeddedRuntime::new(HelloPlugin::new(false), current).unwrap();
        let mut historical_runtime =
            EmbeddedRuntime::new(HelloPlugin::new(false), historical).unwrap();
        assert_eq!(
            current_runtime.seal(crate::flow::build()).unwrap().plan_id,
            historical_runtime
                .seal(crate::flow::build())
                .unwrap()
                .plan_id,
            "provider implementation revisions never enter canonical Plan meaning"
        );
    }
}
