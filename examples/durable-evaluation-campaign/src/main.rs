//! Command-line entry point and child process-plugin protocol endpoint.

use std::env;
use std::io::{self, Read};
use std::path::PathBuf;

use cymule_example_durable_evaluation_campaign::{
    CampaignOptions, FaultPoint, RunDisposition, plugin::EvaluationPlugin,
};
use cymule_runtime::PluginHost;

fn main() {
    if let Err(error) = dispatch() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn dispatch() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments
        .first()
        .is_some_and(|argument| argument == "__plugin")
    {
        return plugin_main();
    }
    let command = arguments.first().map_or("help", String::as_str);
    if matches!(command, "help" | "--help" | "-h") {
        print_help();
        return Ok(());
    }
    let executable = env::current_exe()?.canonicalize()?;
    let mut options = parse_options(&arguments[1..], executable)?;
    match command {
        "run" => {
            if options.suite_path.is_none() {
                return Err("run requires --suite for a new campaign".into());
            }
            let result = cymule_example_durable_evaluation_campaign::campaign::run(&options)?;
            println!("{}", serde_json::to_string_pretty(&result.report)?);
            if result.disposition == RunDisposition::SimulatedCrash {
                eprintln!("simulated process crash after a durable boundary");
                std::process::exit(75);
            }
        }
        "status" => {
            options.suite_path = option_value(&arguments[1..], "--suite").map(PathBuf::from);
            let report = cymule_example_durable_evaluation_campaign::campaign::status(&options)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        "evolve" => {
            let policy = option_value(&arguments[1..], "--policy")
                .ok_or("evolve requires --policy weighted|incompatible")?;
            let report =
                cymule_example_durable_evaluation_campaign::campaign::evolve(&options, policy)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        _ => return Err(format!("unknown command {command:?}; run with --help").into()),
    }
    Ok(())
}

fn parse_options(
    arguments: &[String],
    executable: PathBuf,
) -> Result<CampaignOptions, Box<dyn std::error::Error>> {
    let state = option_value(arguments, "--state").ok_or("--state is required")?;
    let run_id = option_value(arguments, "--run-id").unwrap_or("run:evaluation-demo");
    let suite = option_value(arguments, "--suite").map(PathBuf::from);
    let mut options = CampaignOptions {
        state_dir: PathBuf::from(state),
        suite_path: suite,
        run_id: run_id.to_owned(),
        plugin_executable: executable,
        worker_id: option_value(arguments, "--worker-id").map_or_else(
            || format!("worker:local:{}", std::process::id()),
            str::to_owned,
        ),
        logical_now: option_value(arguments, "--logical-now")
            .map(str::parse)
            .transpose()?,
        lease_ttl: option_value(arguments, "--lease-ttl")
            .map(str::parse)
            .transpose()?
            .unwrap_or(60_000),
        fault: FaultPoint::None,
    };
    if let Some(count) = option_value(arguments, "--simulate-crash-after-claim") {
        options.fault = FaultPoint::AfterClaim(count.parse()?);
    }
    if let Some(count) = option_value(arguments, "--simulate-crash-after-commit") {
        if options.fault != FaultPoint::None {
            return Err("select only one simulated crash point".into());
        }
        options.fault = FaultPoint::AfterCommit(count.parse()?);
    }
    reject_unknown_options(arguments)?;
    Ok(options)
}

fn option_value<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

fn reject_unknown_options(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let known = [
        "--state",
        "--suite",
        "--run-id",
        "--worker-id",
        "--logical-now",
        "--lease-ttl",
        "--policy",
        "--simulate-crash-after-claim",
        "--simulate-crash-after-commit",
    ];
    let mut seen = std::collections::BTreeSet::new();
    let mut index = 0;
    while index < arguments.len() {
        let option = &arguments[index];
        if !known.contains(&option.as_str()) {
            return Err(format!("unknown option {option:?}").into());
        }
        if !seen.insert(option) {
            return Err(format!("option {option} was repeated").into());
        }
        if index + 1 >= arguments.len() || arguments[index + 1].starts_with("--") {
            return Err(format!("option {option} requires a value").into());
        }
        index += 2;
    }
    Ok(())
}

fn plugin_main() -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    io::stdin().take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err("plugin request exceeds 1 MiB".into());
    }
    let request = serde_json::from_slice(&bytes)?;
    let response = EvaluationPlugin.invoke(request)?;
    serde_json::to_writer(io::stdout().lock(), &response)?;
    Ok(())
}

fn print_help() {
    println!(
        "Cymule durable evaluation campaign\n\n\
         Usage:\n\
           cymule-example-durable-evaluation-campaign run --state DIR --suite FILE [--run-id ID]\n\
           cymule-example-durable-evaluation-campaign status --state DIR [--suite FILE] [--run-id ID]\n\
           cymule-example-durable-evaluation-campaign evolve --state DIR --policy weighted|incompatible [--run-id ID]\n\n\
         The child subject/scorer is a process plugin. Simulated crash flags are documented in the example README."
    );
}
