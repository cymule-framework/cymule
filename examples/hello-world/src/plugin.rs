use std::collections::BTreeMap;

use cymule_core::{ReconciliationResolution, WorldOutcome};
use cymule_runtime::{
    PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::json;

pub struct HelloPlugin {
    unknown_once: bool,
}

impl HelloPlugin {
    pub const fn new(unknown_once: bool) -> Self {
        Self { unknown_once }
    }
}

impl PluginHost for HelloPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "example-hello-plugin@1".to_owned(),
                    components: BTreeMap::from([(
                        "example.greet".to_owned(),
                        PluginOperation {
                            implementation_revision: "1".to_owned(),
                        },
                    )]),
                    effects: BTreeMap::from([(
                        "example.capture".to_owned(),
                        PluginEffect {
                            implementation_revision: "1".to_owned(),
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
                input,
            } if operation == "example.capture" => {
                eprintln!("captured {intent_id}: {}", input["message"]);
                let outcome = if self.unknown_once {
                    self.unknown_once = false;
                    eprintln!("simulating a lost dispatch response; outcome is unknown");
                    WorldOutcome::Unknown
                } else {
                    WorldOutcome::Applied
                };
                Ok(PluginResponse::EffectResult {
                    outcome,
                    value: Some(input),
                })
            }
            PluginRequest::ReconcileEffect {
                operation, input, ..
            } if operation == "example.capture" => {
                eprintln!("reconciled the original capture intent as applied");
                Ok(PluginResponse::ReconciliationResult {
                    resolution: ReconciliationResolution::ResolvedApplied,
                    value: Some(input),
                })
            }
            request => Err(RuntimeError::plugin_defect(format!(
                "unsupported Hello World request: {request:?}"
            ))),
        }
    }
}
