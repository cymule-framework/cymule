//! Code-first Cymule Hello World application.

mod flow;
mod plugin;

use std::env;

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
    let (name, unknown_once) = parse_arguments(env::args().skip(1))?;
    let mut plugin = HelloPlugin::new(unknown_once);
    let manifest = plugin.describe()?;
    let binding =
        ExecutionBinding::for_local_process(&manifest, HelloPlugin::implementation_revision())?;
    let mut runtime = EmbeddedRuntime::new(plugin, binding)?;
    let plan = runtime.seal(flow::build())?;
    eprintln!("sealed {}", plan.plan_id);

    let result = runtime
        .execute(plan, &json!({"name": name}), "run:hello-world")?
        .into_completed()?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn parse_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> Result<(String, bool), Box<dyn std::error::Error>> {
    let mut name = None;
    let mut unknown_once = false;
    for argument in arguments {
        if argument == "--unknown-once" {
            if unknown_once {
                return Err("--unknown-once was repeated".into());
            }
            unknown_once = true;
        } else if argument.starts_with('-') {
            return Err(format!("unknown option {argument:?}").into());
        } else if name.replace(argument).is_some() {
            return Err("Hello World accepts at most one name".into());
        }
    }
    Ok((name.unwrap_or_else(|| "World".to_owned()), unknown_once))
}

#[cfg(test)]
mod tests {
    use super::parse_arguments;

    #[test]
    fn command_line_is_closed_and_order_independent() {
        assert_eq!(
            parse_arguments(["--unknown-once".to_owned(), "Ada".to_owned()]).unwrap(),
            ("Ada".to_owned(), true)
        );
        assert!(parse_arguments(["--unknown".to_owned()]).is_err());
        assert!(parse_arguments(["Ada".to_owned(), "Grace".to_owned()]).is_err());
        assert!(
            parse_arguments(["--unknown-once".to_owned(), "--unknown-once".to_owned()]).is_err()
        );
    }
}
