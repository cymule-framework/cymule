//! Code-first Cymule Hello World application.

mod flow;
mod plugin;

use std::env;

use cymule_core::sha256_bytes;
use cymule_runtime::{EmbeddedRuntime, ExecutionBinding, PluginHost};
use serde_json::json;

use crate::plugin::HelloPlugin;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let name = arguments.next().unwrap_or_else(|| "World".to_owned());
    let unknown_once = arguments.any(|argument| argument == "--unknown-once");
    let mut plugin = HelloPlugin::new(unknown_once);
    let manifest = plugin.describe()?;
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        format!("sha256:{}", sha256_bytes(b"cymule-example-hello-world/1")),
    )?;
    let mut runtime = EmbeddedRuntime::new(plugin, binding)?;
    let plan = runtime.seal(flow::build())?;
    eprintln!("sealed {}", plan.plan_id);

    let result = runtime.execute(plan, &json!({"name": name}), "run:hello-world")?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
