//! Process environment, protocol, output-bound, and timeout tests.

use std::collections::BTreeMap;
use std::time::Duration;

use cymule_executor_process::{ProcessExecutor, ProcessExecutorConfig};
use cymule_runtime::{PluginHost, PluginRequest, RuntimeError};

#[cfg(unix)]
fn shell(script: &str) -> ProcessExecutor {
    shell_with(script, Duration::from_secs(2), 8 * 1024 * 1024)
}

#[cfg(unix)]
fn shell_with(script: &str, timeout: Duration, message_limit: usize) -> ProcessExecutor {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("script directory creates");
    let executable = directory.path().join("plugin.sh");
    fs::write(&executable, format!("#!/bin/sh\n{script}\n")).expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let mut config = ProcessExecutorConfig::new(executable);
    config.timeout = timeout;
    config.message_limit = message_limit;
    ProcessExecutor::new(config).expect("executor configures")
}

#[cfg(unix)]
#[test]
fn process_manifest_uses_only_explicit_environment() {
    let script = r#"
      test -z "${SHOULD_NOT_LEAK:-}" || exit 9
      request=$(/bin/cat)
      test -n "$request" || exit 8
      printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"process:test","components":{},"effects":{}}}'
    "#;
    let mut executor = shell(script);
    let manifest = executor.describe().expect("manifest returns");
    assert_eq!(manifest.implementation_id, "process:test");
    assert_eq!(executor.config().environment, BTreeMap::new());
}

#[cfg(unix)]
#[test]
fn process_timeout_is_reported_as_ambiguous() {
    let mut executor = shell_with("/bin/sleep 2", Duration::from_millis(10), 1024);
    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::TimedOut { code, .. }) if code == "process_response_timed_out"
    ));
}

#[cfg(unix)]
#[test]
fn process_output_limit_fails_closed() {
    let mut executor = shell_with(
        "printf '1234567890123456789012345678901234567890'",
        Duration::from_secs(2),
        32,
    );
    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::Substrate { .. })
    ));
}

#[cfg(unix)]
#[test]
fn process_output_exactly_at_limit_is_accepted() {
    let response = r#"{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"process:limit","components":{},"effects":{}}}"#;
    let mut executor = shell_with(
        &format!("request=$(/bin/cat); test -n \"$request\" || exit 8; printf '%s' '{response}'"),
        Duration::from_secs(2),
        response.len(),
    );
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
        Err(RuntimeError::PluginDefect { .. })
    ));
}

#[cfg(unix)]
#[test]
fn executor_launches_the_bytes_sealed_at_construction() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("source directory creates");
    let source = directory.path().join("plugin.sh");
    let original = r#"#!/bin/sh
/bin/cat >/dev/null
printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"sealed:original","components":{},"effects":{}}}'
"#;
    fs::write(&source, original).expect("source writes");
    fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).expect("source executes");
    let mut executor =
        ProcessExecutor::new(ProcessExecutorConfig::new(&source)).expect("source bytes seal");
    let revision = executor.implementation_revision().to_owned();

    fs::write(
        &source,
        original.replace("sealed:original", "mutable:replacement"),
    )
    .expect("source mutates after sealing");
    assert_eq!(
        executor
            .describe()
            .expect("sealed copy still executes")
            .implementation_id,
        "sealed:original"
    );
    assert_eq!(executor.implementation_revision(), revision);
}

#[cfg(unix)]
#[test]
fn stderr_is_never_projected_into_process_failures() {
    let secret = "stderr-secret-must-not-escape";
    let script = format!(
        "request=$(/bin/cat); test -n \"$request\" || exit 8; printf '%s' '{secret}' >&2; exit 7"
    );
    let mut executor = shell(&script);
    let error = executor
        .invoke(PluginRequest::Call {
            component: "test".to_owned(),
            input: serde_json::Value::Null,
        })
        .expect_err("failed component process is a defect");
    assert!(!error.to_string().contains(secret));

    let error = executor
        .invoke(PluginRequest::DispatchEffect {
            operation: "test".to_owned(),
            intent_id: "intent:test".to_owned(),
            input: serde_json::Value::Null,
        })
        .expect_err("failed dispatch process is ambiguous");
    assert!(matches!(error, RuntimeError::UnknownWorld { .. }));
    assert!(!error.to_string().contains(secret));
}
