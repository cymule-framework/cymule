//! Deterministic external adapter used by Cymule conformance tests.

use std::collections::BTreeMap;
use std::io::{self, Read};

use cymule_core::{ReconciliationResolution, WorldOutcome, decode_json};
use cymule_runtime::{
    PLUGIN_VERSION, PluginEffect, PluginExpectedFailure, PluginManifest, PluginOperation,
    PluginRequest, PluginResponse,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    io::stdin().read_to_end(&mut bytes)?;
    let request: PluginRequest = decode_json(&bytes)?;
    let response = match request {
        PluginRequest::Describe => PluginResponse::Manifest {
            manifest: PluginManifest {
                plugin_version: PLUGIN_VERSION.to_owned(),
                implementation_id: "test-adapter@1".to_owned(),
                components: BTreeMap::from([(
                    "test.echo".to_owned(),
                    PluginOperation {
                        implementation_revision: "1".to_owned(),
                    },
                )]),
                effects: BTreeMap::from([(
                    "test.capture".to_owned(),
                    PluginEffect {
                        implementation_revision: "1".to_owned(),
                        can_reconcile: true,
                    },
                )]),
            },
        },
        PluginRequest::Call { component, input }
            if component == "test.echo"
                && input.get("simulate").and_then(serde_json::Value::as_str)
                    == Some("expected_failure") =>
        {
            PluginResponse::ExpectedFailure {
                error: PluginExpectedFailure {
                    code: "evaluation_rejected".to_owned(),
                    message: "the test evaluation was rejected".to_owned(),
                },
            }
        }
        PluginRequest::Call { component, input } if component == "test.echo" => {
            PluginResponse::CallResult { value: input }
        }
        PluginRequest::PrepareEffect { operation, .. } if operation == "test.capture" => {
            PluginResponse::Prepared
        }
        PluginRequest::DispatchEffect {
            operation, input, ..
        } if operation == "test.capture" => {
            let unknown = input
                .get("simulate")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == "unknown");
            PluginResponse::EffectResult {
                outcome: if unknown {
                    WorldOutcome::Unknown
                } else {
                    WorldOutcome::Applied
                },
                value: Some(input),
            }
        }
        PluginRequest::ReconcileEffect {
            operation, input, ..
        } if operation == "test.capture" => PluginResponse::ReconciliationResult {
            resolution: ReconciliationResolution::ResolvedApplied,
            value: Some(input),
        },
        request => PluginResponse::Defect {
            code: "unsupported_request".to_owned(),
            message: format!("unsupported test request: {request:?}"),
        },
    };
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
