//! Command-line entry point and child process-plugin protocol endpoint.

use std::env;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use cymule_core::decode_json;
use cymule_example_durable_evaluation_campaign::{
    CampaignOptions, CampaignReport, EvolutionReport, FaultPoint, RunDisposition,
    campaign::BUNDLED_PLUGIN_RUNTIME_REVISION, plugin::EvaluationPlugin,
};
use cymule_runtime::{MAX_PLUGIN_MESSAGE_BYTES, PluginHost, decode_plugin_request};

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
    if command == "demo" {
        return demo(&arguments[1..], &executable);
    }
    reject_unknown_options(&arguments[1..])?;
    reject_irrelevant_options(command, &arguments[1..])?;
    let mut options = parse_options(&arguments[1..], executable)?;
    match command {
        "run" => {
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

fn demo(arguments: &[String], executable: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let state = match arguments {
        [] => {
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            env::temp_dir().join(format!(
                "cymule-feature-tour-{}-{nonce}",
                std::process::id()
            ))
        }
        [option, path] if option == "--state" => PathBuf::from(path),
        _ => return Err("demo accepts only an optional --state DIR".into()),
    };
    fs::create_dir(&state).map_err(|error| {
        format!(
            "demo state directory {} must not already exist: {error}",
            state.display()
        )
    })?;
    let suite = state.join("support-tickets.jsonl");
    fs::write(&suite, include_bytes!("../fixtures/support-tickets.jsonl"))?;
    let state_arg = state.to_str().ok_or("demo state path must be UTF-8")?;
    let suite_arg = suite.to_str().ok_or("demo suite path must be UTF-8")?;
    let run_id = "run:five-minute-tour";

    let crashed = child(
        executable,
        &[
            "run",
            "--state",
            state_arg,
            "--suite",
            suite_arg,
            "--run-id",
            run_id,
            "--worker-id",
            "worker:tour:before-update",
            "--logical-now",
            "10",
            "--simulate-crash-after-commit",
            "3",
        ],
    )?;
    if crashed.status.code() != Some(75) {
        return Err(child_failure("crash phase", &crashed).into());
    }
    let before: CampaignReport = decode_json(&crashed.stdout)?;
    if before.succeeded != 3 {
        return Err("crash phase did not retain exactly three completed cases".into());
    }

    let compatible = successful_child(
        "compatible evolution",
        child(
            executable,
            &[
                "evolve", "--state", state_arg, "--run-id", run_id, "--policy", "weighted",
            ],
        )?,
    )?;
    let compatible: EvolutionReport = decode_json(&compatible.stdout)?;
    if !compatible.advanced {
        return Err("compatible scorer revision did not advance future work".into());
    }

    let resumed = successful_child(
        "resume",
        child(
            executable,
            &[
                "run",
                "--state",
                state_arg,
                "--suite",
                suite_arg,
                "--run-id",
                run_id,
                "--worker-id",
                "worker:tour:after-update",
                "--logical-now",
                "20",
            ],
        )?,
    )?;
    let final_report: CampaignReport = decode_json(&resumed.stdout)?;
    let strict = final_report
        .cases
        .iter()
        .filter(|case| {
            case.output
                .as_ref()
                .is_some_and(|output| output.score.policy == "strict")
        })
        .count();
    let weighted = final_report
        .cases
        .iter()
        .filter(|case| {
            case.output
                .as_ref()
                .is_some_and(|output| output.score.policy == "weighted")
        })
        .count();
    if final_report.succeeded != final_report.total_cases
        || final_report.total_occurrences != final_report.total_cases
        || final_report.recovered_attempts != 0
        || (strict, weighted) != (3, 9)
    {
        return Err("resumed campaign did not preserve the 3/9 Plan boundary".into());
    }

    let incompatible = successful_child(
        "incompatible evolution",
        child(
            executable,
            &[
                "evolve",
                "--state",
                state_arg,
                "--run-id",
                run_id,
                "--policy",
                "incompatible",
            ],
        )?,
    )?;
    let incompatible: EvolutionReport = decode_json(&incompatible.stdout)?;
    if incompatible.advanced || incompatible.current_plan_id != compatible.current_plan_id {
        return Err("incompatible scorer revision changed the future Plan".into());
    }

    println!("Cymule: safely upgrade a running evaluation");
    println!();
    println!(
        "Scenario        Evaluate {} support tickets while the scoring policy changes.",
        before.total_cases
    );
    println!("✓ Crash recovery  The worker stopped after 3 results; restart reused all 3.");
    println!(
        concat!(
            "✓ Safe upgrade    {} completed results kept the original policy;\n",
            "                  {} future results used the update."
        ),
        strict, weighted
    );
    println!(
        "✓ Compatibility   An incompatible scoring update was blocked before it changed work."
    );
    println!(
        "✓ Outcome         {}/{} finished without repeating completed evaluations.",
        final_report.succeeded, final_report.total_cases
    );
    println!();
    println!("State retained at {}", state.display());
    Ok(())
}

fn child(executable: &Path, arguments: &[&str]) -> Result<Output, std::io::Error> {
    Command::new(executable).args(arguments).output()
}

fn successful_child(phase: &str, output: Output) -> Result<Output, Box<dyn std::error::Error>> {
    if output.status.success() {
        Ok(output)
    } else {
        Err(child_failure(phase, &output).into())
    }
}

fn child_failure(phase: &str, output: &Output) -> String {
    format!(
        "{phase} failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

fn parse_options(
    arguments: &[String],
    executable: PathBuf,
) -> Result<CampaignOptions, Box<dyn std::error::Error>> {
    let state = option_value(arguments, "--state").ok_or("--state is required")?;
    let run_id = option_value(arguments, "--run-id").unwrap_or("run:evaluation-demo");
    let suite = option_value(arguments, "--suite").map(PathBuf::from);
    let plugin = option_value(arguments, "--plugin");
    let configured_runtime_revision = option_value(arguments, "--plugin-runtime-revision");
    let (plugin_executable, plugin_runtime_revision) = match plugin {
        Some(path) => {
            let revision = configured_runtime_revision
                .ok_or("--plugin requires --plugin-runtime-revision")?
                .to_owned();
            (fs::canonicalize(path)?, revision)
        }
        None => (
            executable,
            configured_runtime_revision
                .unwrap_or(BUNDLED_PLUGIN_RUNTIME_REVISION)
                .to_owned(),
        ),
    };
    let mut options = CampaignOptions {
        state_dir: PathBuf::from(state),
        suite_path: suite,
        run_id: run_id.to_owned(),
        plugin_executable,
        plugin_runtime_revision,
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
        "--plugin",
        "--plugin-runtime-revision",
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

fn reject_irrelevant_options(
    command: &str,
    arguments: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let allowed: &[&str] = match command {
        "run" => &[
            "--state",
            "--suite",
            "--run-id",
            "--worker-id",
            "--plugin",
            "--plugin-runtime-revision",
            "--logical-now",
            "--lease-ttl",
            "--simulate-crash-after-claim",
            "--simulate-crash-after-commit",
        ],
        "status" => &["--state", "--suite", "--run-id"],
        "evolve" => &["--state", "--run-id", "--policy"],
        _ => return Ok(()),
    };
    for option in arguments.iter().step_by(2) {
        if option.starts_with("--") && !allowed.contains(&option.as_str()) {
            return Err(format!("option {option} is not valid for {command}").into());
        }
    }
    Ok(())
}

fn plugin_main() -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    io::stdin()
        .take(u64::try_from(MAX_PLUGIN_MESSAGE_BYTES + 1)?)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_PLUGIN_MESSAGE_BYTES {
        return Err(format!(
            "plugin request exceeds the {MAX_PLUGIN_MESSAGE_BYTES} byte protocol bound"
        )
        .into());
    }
    let request = decode_plugin_request(&bytes)?;
    let admitted = request.clone();
    let response = EvaluationPlugin.invoke(request)?;
    response.verify_for(&admitted)?;
    serde_json::to_writer(io::stdout().lock(), &response)?;
    Ok(())
}

fn print_help() {
    println!(
        "Cymule durable evaluation campaign\n\n\
         Usage:\n\
           cymule-example-durable-evaluation-campaign demo [--state NEW_DIR]\n\
           cymule-example-durable-evaluation-campaign run --state DIR [--suite FILE] [--run-id ID] [--plugin EXECUTABLE --plugin-runtime-revision REVISION]\n\
           cymule-example-durable-evaluation-campaign status --state DIR [--suite FILE] [--run-id ID]\n\
           cymule-example-durable-evaluation-campaign evolve --state DIR --policy weighted|incompatible [--run-id ID]\n\n\
         The child subject/scorer is a process plugin. Simulated crash flags are documented in the example README."
    );
}
