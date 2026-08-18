//! Process environment, protocol, output-bound, and timeout tests.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use cymule_executor_process::{ProcessExecutor, ProcessExecutorConfig};
use cymule_runtime::{PluginHost, RuntimeError};

#[cfg(unix)]
fn shell(script: &str) -> ProcessExecutor {
    let mut config = ProcessExecutorConfig::new(PathBuf::from("/bin/sh"));
    config.arguments = vec!["-c".to_owned(), script.to_owned()];
    config.timeout = Duration::from_secs(2);
    ProcessExecutor::new(config).expect("executor configures")
}

#[cfg(unix)]
#[test]
fn process_manifest_uses_only_explicit_environment() {
    let script = r#"
      test -z "${SHOULD_NOT_LEAK:-}" || exit 9
      printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/1","implementation_id":"process:test","components":{},"effects":{}}}'
    "#;
    let mut executor = shell(script);
    let manifest = executor.describe().expect("manifest returns");
    assert_eq!(manifest.implementation_id, "process:test");
    assert_eq!(executor.config().environment, BTreeMap::new());
}

#[cfg(unix)]
#[test]
fn process_timeout_is_reported_as_ambiguous() {
    let mut config = ProcessExecutorConfig::new(PathBuf::from("/bin/sh"));
    config.arguments = vec!["-c".to_owned(), "/bin/sleep 2".to_owned()];
    config.timeout = Duration::from_millis(10);
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::Plugin(message)) if message.contains("process_timeout_unknown")
    ));
}

#[cfg(unix)]
#[test]
fn process_output_limit_fails_closed() {
    let mut config = ProcessExecutorConfig::new(PathBuf::from("/bin/sh"));
    config.arguments = vec![
        "-c".to_owned(),
        "printf '1234567890123456789012345678901234567890'".to_owned(),
    ];
    config.message_limit = 32;
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    assert!(matches!(executor.describe(), Err(RuntimeError::Io(_))));
}

#[cfg(unix)]
#[test]
fn process_output_exactly_at_limit_is_accepted() {
    let response = r#"{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/1","implementation_id":"process:limit","components":{},"effects":{}}}"#;
    let mut config = ProcessExecutorConfig::new(PathBuf::from("/bin/sh"));
    config.arguments = vec!["-c".to_owned(), format!("printf '%s' '{response}'")];
    config.message_limit = response.len();
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    assert_eq!(
        executor
            .describe()
            .expect("boundary response parses")
            .implementation_id,
        "process:limit"
    );
}

#[test]
fn relative_executable_is_rejected() {
    assert!(matches!(
        ProcessExecutor::new(ProcessExecutorConfig::new("plugin")),
        Err(RuntimeError::Plugin(_))
    ));
}
