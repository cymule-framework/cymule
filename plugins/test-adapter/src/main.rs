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
    let value: serde_json::Value = decode_json(&bytes)?;
    if value.get("evolution_plugin_protocol").is_some() {
        return evolution(value);
    }
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

fn evolution(value: serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    let revision = value
        .get("implementation_revision")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "missing implementation revision",
            )
        })?;
    let request_type = value
        .get("request")
        .and_then(|request| request.get("type"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing evolution request type")
        })?;
    let response = match request_type {
        "describe_migration" => serde_json::json!({
            "type": "migration_descriptor",
            "descriptor": {
                "adapter_id": "test.migration",
                "adapter_revision": revision,
                "from_plan": "sha256:test-from",
                "to_plan": "sha256:test-to",
                "from_schema": "schema:test-from",
                "to_schema": "schema:test-to",
                "state_coverage": "total_reachable_state",
                "failure_and_cancellation": "preserved",
                "budget_and_ownership": "preserved",
                "authority_and_effects": "no_widening"
            }
        }),
        "describe_shadow" => serde_json::json!({
            "type": "shadow_descriptor",
            "descriptor": {
                "driver_id": "test.shadow",
                "driver_revision": revision,
                "target_effects": "suppressed_or_simulated",
                "occurrence_bindings": "pinned"
            }
        }),
        _ => return Err(format!("unsupported evolution request {request_type}").into()),
    };
    println!(
        "{}",
        serde_json::json!({
            "outcome": "success",
            "evolution_plugin_protocol": "cymule.evolution-plugin/1",
            "response": response
        })
    );
    Ok(())
}
