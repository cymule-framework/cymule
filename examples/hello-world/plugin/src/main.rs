//! Standalone plugin used by the Cymule Hello World example.

use std::collections::BTreeMap;
use std::io::{self, Read};

use cymule_core::{ReconciliationResolution, WorldOutcome};
use cymule_runtime::{
    PLUGIN_VERSION, PluginEffect, PluginManifest, PluginOperation, PluginRequest, PluginResponse,
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
    let request: PluginRequest = serde_json::from_slice(&bytes)?;
    let response = match request {
        PluginRequest::Describe => PluginResponse::Manifest {
            manifest: PluginManifest {
                plugin_version: PLUGIN_VERSION.to_owned(),
                implementation_id: "example-hello-plugin@1".to_owned(),
                components: BTreeMap::from([(
                    "example.echo".to_owned(),
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
        },
        PluginRequest::Call { component, input } if component == "example.echo" => {
            PluginResponse::CallResult { value: input }
        }
        PluginRequest::PrepareEffect { operation, .. } if operation == "example.capture" => {
            PluginResponse::Prepared
        }
        PluginRequest::DispatchEffect {
            operation, input, ..
        } if operation == "example.capture" => PluginResponse::EffectResult {
            outcome: WorldOutcome::Applied,
            value: Some(input),
        },
        PluginRequest::ReconcileEffect {
            operation, input, ..
        } if operation == "example.capture" => PluginResponse::ReconciliationResult {
            resolution: ReconciliationResolution::ResolvedApplied,
            value: Some(input),
        },
        request => PluginResponse::Error {
            code: "unsupported_request".to_owned(),
            message: format!("unsupported Hello World request: {request:?}"),
        },
    };
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
