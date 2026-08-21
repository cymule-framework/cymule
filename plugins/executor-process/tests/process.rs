//! Process environment, protocol, output-bound, and timeout tests.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

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

#[cfg(unix)]
#[test]
fn process_response_rejects_duplicate_protocol_members() {
    let script = r#"
      request=$(/bin/cat)
      test -n "$request" || exit 8
      printf '%s' '{"type":"manifest","type":"defect","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"process:test","components":{},"effects":{}}}'
    "#;
    let mut executor = shell(script);
    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::Substrate { code, .. }) if code == "invalid_plugin_response"
    ));
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
fn plugin_cannot_replace_the_executable_used_by_its_next_invocation() {
    let script = r#"
      request=$(/bin/cat)
      test -n "$request" || exit 8
      printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"sealed:self-original","components":{},"effects":{}}}'
      /bin/chmod 700 "$0"
      printf '%s\n' '#!/bin/sh' 'request=$(/bin/cat)' 'printf '\''%s'\'' '\''{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"mutable:self-replacement","components":{},"effects":{}}}'\''' > "$0"
    "#;
    let mut executor = shell(script);

    assert_eq!(
        executor
            .describe()
            .expect("first disposable occurrence executes")
            .implementation_id,
        "sealed:self-original"
    );
    assert_eq!(
        executor
            .describe()
            .expect("next occurrence rematerializes captured bytes")
            .implementation_id,
        "sealed:self-original"
    );
}

#[cfg(unix)]
#[test]
fn forked_descendant_cannot_hold_response_pipes_or_outlive_the_occurrence() {
    let script = r#"
      request=$(/bin/cat)
      test -n "$request" || exit 8
      (/bin/sleep 10) &
      printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"process:tree","components":{},"effects":{}}}'
    "#;
    let mut executor = shell_with(script, Duration::from_secs(2), 1024 * 1024);
    let started = Instant::now();

    assert_eq!(
        executor
            .describe()
            .expect("leader response remains authoritative")
            .implementation_id,
        "process:tree"
    );
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "a descendant retained the process pipes past bounded cleanup"
    );
}

#[cfg(unix)]
#[test]
fn timeout_kills_and_reaps_the_occurrence_process_group() {
    let script = r"
      (/bin/sleep 10) &
      /bin/sleep 10
    ";
    let mut executor = shell_with(script, Duration::from_millis(100), 1024);
    let started = Instant::now();

    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::TimedOut { code, .. }) if code == "process_response_timed_out"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "tree termination exceeded the bounded invocation window"
    );
}

#[cfg(unix)]
#[test]
fn binding_identity_covers_arguments_environment_working_tree_and_runtime_closure() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let executable = directory.path().join("plugin.sh");
    fs::write(&executable, "#!/bin/sh\n/bin/cat >/dev/null\nexit 0\n").expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let working = directory.path().join("working");
    fs::create_dir(&working).expect("working directory creates");
    fs::write(working.join("input.txt"), "one").expect("working input writes");

    let mut baseline = ProcessExecutorConfig::new(&executable);
    baseline.arguments = vec!["one".to_owned()];
    baseline
        .environment
        .insert("MODE".to_owned(), "one".to_owned());
    baseline.working_directory = Some(working.clone());
    baseline
        .runtime_closure
        .insert("loader".to_owned(), "sha256:one".to_owned());
    let baseline_revision = ProcessExecutor::new(baseline.clone())
        .expect("baseline captures")
        .implementation_revision()
        .to_owned();

    let mut arguments = baseline.clone();
    arguments.arguments = vec!["two".to_owned()];
    let mut environment = baseline.clone();
    environment
        .environment
        .insert("MODE".to_owned(), "two".to_owned());
    let mut runtime = baseline.clone();
    runtime
        .runtime_closure
        .insert("loader".to_owned(), "sha256:two".to_owned());
    let mut timeout = baseline.clone();
    timeout.timeout = Duration::from_secs(2);
    let mut message_limit = baseline.clone();
    message_limit.message_limit /= 2;
    fs::write(working.join("input.txt"), "two").expect("working input changes");
    let working_revision = ProcessExecutor::new(baseline)
        .expect("changed working tree captures")
        .implementation_revision()
        .to_owned();

    for revision in [
        ProcessExecutor::new(arguments)
            .expect("argument variant captures")
            .implementation_revision()
            .to_owned(),
        ProcessExecutor::new(environment)
            .expect("environment variant captures")
            .implementation_revision()
            .to_owned(),
        ProcessExecutor::new(runtime)
            .expect("runtime variant captures")
            .implementation_revision()
            .to_owned(),
        ProcessExecutor::new(timeout)
            .expect("timeout variant captures")
            .implementation_revision()
            .to_owned(),
        ProcessExecutor::new(message_limit)
            .expect("limit variant captures")
            .implementation_revision()
            .to_owned(),
        working_revision,
    ] {
        assert_ne!(revision, baseline_revision);
    }
}

#[cfg(unix)]
#[test]
fn working_directory_is_a_fresh_captured_tree_for_each_invocation() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        r#"#!/bin/sh
/bin/cat >/dev/null
value=$(/bin/cat value.txt)
printf '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/2","implementation_id":"cwd:%s","components":{},"effects":{}}}' "$value"
printf '%s' changed > value.txt
"#,
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let working = directory.path().join("working");
    fs::create_dir(&working).expect("working directory creates");
    fs::write(working.join("value.txt"), "captured").expect("working value writes");
    let mut config = ProcessExecutorConfig::new(executable);
    config.working_directory = Some(working.clone());
    let mut executor = ProcessExecutor::new(config).expect("executor captures working tree");
    fs::write(working.join("value.txt"), "source-mutated").expect("source tree mutates");

    for _ in 0..2 {
        assert_eq!(
            executor
                .describe()
                .expect("fresh captured tree executes")
                .implementation_id,
            "cwd:captured"
        );
    }
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

#[cfg(not(unix))]
#[test]
fn unsupported_platform_is_rejected_before_execution() {
    assert!(matches!(
        ProcessExecutor::new(ProcessExecutorConfig::new(r"C:\plugin.exe")),
        Err(RuntimeError::PluginDefect { .. })
    ));
}
