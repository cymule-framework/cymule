//! Command-line and JSON RPC transport for the Cymule engine.

use std::env;
use std::fs;
use std::io::{self, Read};
use std::path::Path;

use cymule_core::{PlanCandidate, SealedPlan};
use cymule_durable::{DurableCommand, WaitActivation};
use cymule_evolution::{EvolutionCommand, LiveEvolutionCommand};
use cymule_resource::{ResourceCandidate, ResourceHandle};
use cymule_runtime::{EmbeddedRuntime, ExecutionResult, ProcessPlugin};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EngineRequest {
    Seal {
        candidate: PlanCandidate,
    },
    Verify {
        plan: SealedPlan,
    },
    SealResource {
        candidate: ResourceCandidate,
    },
    VerifyWaitActivation {
        activation: WaitActivation,
    },
    VerifyDurableCommand {
        command: DurableCommand,
    },
    VerifyEvolutionCommand {
        command: EvolutionCommand,
    },
    VerifyLiveEvolutionCommand {
        command: LiveEvolutionCommand,
    },
    Run {
        plan: SealedPlan,
        input: Value,
        plugin: String,
        run_id: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EngineResponse {
    Sealed { plan: SealedPlan },
    SealedResource { resource: ResourceHandle },
    VerifiedWaitActivation { activation: WaitActivation },
    VerifiedDurableCommand { command: DurableCommand },
    VerifiedEvolutionCommand { command: EvolutionCommand },
    VerifiedLiveEvolutionCommand { command: LiveEvolutionCommand },
    Executed { result: ExecutionResult },
    Verified,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    match arguments.first().map(String::as_str) {
        Some("rpc") => rpc(),
        Some("seal") => {
            let candidate: PlanCandidate = read_path(argument_value(&arguments, "--input")?)?;
            let plan = candidate.seal()?;
            print_json(&plan)
        }
        Some("verify") => {
            let plan: SealedPlan = read_path(argument_value(&arguments, "--plan")?)?;
            plan.verify()?;
            println!("verified {}", plan.plan_id);
            Ok(())
        }
        Some("resource") if arguments.get(1).map(String::as_str) == Some("seal") => {
            let candidate: ResourceCandidate = read_path(argument_value(&arguments, "--input")?)?;
            print_json(&candidate.seal()?)
        }
        Some("wait-activation") if arguments.get(1).map(String::as_str) == Some("verify") => {
            let activation: WaitActivation = read_path(argument_value(&arguments, "--input")?)?;
            activation.verify()?;
            print_json(&activation)
        }
        Some("durable-command") if arguments.get(1).map(String::as_str) == Some("verify") => {
            let command: DurableCommand = read_path(argument_value(&arguments, "--input")?)?;
            command.verify()?;
            print_json(&command)
        }
        Some("evolution-command") if arguments.get(1).map(String::as_str) == Some("verify") => {
            let command: EvolutionCommand = read_path(argument_value(&arguments, "--input")?)?;
            command.verify()?;
            print_json(&command)
        }
        Some("live-evolution-command")
            if arguments.get(1).map(String::as_str) == Some("verify") =>
        {
            let command: LiveEvolutionCommand =
                read_path(argument_value(&arguments, "--input")?)?;
            command.verify()?;
            print_json(&command)
        }
        Some("run") => {
            let plan: SealedPlan = read_path(argument_value(&arguments, "--plan")?)?;
            let input: Value = read_path(argument_value(&arguments, "--input")?)?;
            let plugin = argument_value(&arguments, "--plugin")?;
            let run_id = argument_value(&arguments, "--run-id")?;
            let mut runtime = EmbeddedRuntime::new(ProcessPlugin::new(plugin));
            let result = runtime.execute(plan, &input, run_id)?;
            print_json(&result)
        }
        _ => Err(
            "usage: cymule <rpc|seal|verify|run|resource seal|wait-activation verify|durable-command verify|evolution-command verify|live-evolution-command verify> [options]"
                .into(),
        ),
    }
}

fn rpc() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input)?;
    let request: EngineRequest = serde_json::from_slice(&input)?;
    let response = match request {
        EngineRequest::Seal { candidate } => EngineResponse::Sealed {
            plan: candidate.seal()?,
        },
        EngineRequest::Verify { plan } => {
            plan.verify()?;
            EngineResponse::Verified
        }
        EngineRequest::SealResource { candidate } => EngineResponse::SealedResource {
            resource: candidate.seal()?,
        },
        EngineRequest::VerifyWaitActivation { activation } => {
            activation.verify()?;
            EngineResponse::VerifiedWaitActivation { activation }
        }
        EngineRequest::VerifyDurableCommand { command } => {
            command.verify()?;
            EngineResponse::VerifiedDurableCommand { command }
        }
        EngineRequest::VerifyEvolutionCommand { command } => {
            command.verify()?;
            EngineResponse::VerifiedEvolutionCommand { command }
        }
        EngineRequest::VerifyLiveEvolutionCommand { command } => {
            command.verify()?;
            EngineResponse::VerifiedLiveEvolutionCommand { command }
        }
        EngineRequest::Run {
            plan,
            input,
            plugin,
            run_id,
        } => {
            let mut runtime = EmbeddedRuntime::new(ProcessPlugin::new(plugin));
            EngineResponse::Executed {
                result: runtime.execute(plan, &input, run_id)?,
            }
        }
    };
    print_json(&response)
}

fn argument_value<'a>(arguments: &'a [String], flag: &str) -> Result<&'a str, String> {
    arguments
        .windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
        .ok_or_else(|| format!("missing required option {flag}"))
}

fn read_path<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, Box<dyn std::error::Error>> {
    let bytes = if path == "-" {
        let mut bytes = Vec::new();
        io::stdin().read_to_end(&mut bytes)?;
        bytes
    } else {
        fs::read(Path::new(path))?
    };
    Ok(serde_json::from_slice(&bytes)?)
}

fn print_json<T: Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
