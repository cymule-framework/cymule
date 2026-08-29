//! Process environment, protocol, output-bound, and timeout tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::path::Path;
#[cfg(unix)]
use std::process::{Command, Stdio};

use cymule_executor_process::{ProcessCancellation, ProcessExecutor, ProcessExecutorConfig};
use cymule_runtime::{
    EffectProviderAttempt, EffectReconciliationDecision, PluginHost, PluginRequest, RuntimeError,
};

const TEST_INTENT_ID: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_INTENT_ID: &str =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn effect_attempt() -> EffectProviderAttempt {
    EffectProviderAttempt::new(TEST_INTENT_ID, "owner:test", 1).expect("attempt derives")
}

fn executor_config(executable: impl AsRef<Path>) -> ProcessExecutorConfig {
    ProcessExecutorConfig::new(
        executable,
        BTreeMap::from([(
            "test-runtime".to_owned(),
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
        )]),
    )
}

#[test]
fn runtime_binding_is_required_and_never_inferred_from_the_host() {
    assert!(matches!(
        ProcessExecutor::new(ProcessExecutorConfig::new("/bin/sh", BTreeMap::new())),
        Err(RuntimeError::PluginDefect { .. })
    ));
    for revision in [
        "unix:macos:arm64",
        "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "sha256:short",
    ] {
        assert!(matches!(
            ProcessExecutor::new(ProcessExecutorConfig::new(
                "/bin/sh",
                BTreeMap::from([("test-runtime".to_owned(), revision.to_owned())]),
            )),
            Err(RuntimeError::PluginDefect { .. })
        ));
    }
}

fn reconciliation_request() -> PluginRequest {
    PluginRequest::ReconcileEffect {
        operation: "effect:test".to_owned(),
        intent_id: TEST_INTENT_ID.to_owned(),
        attempt: effect_attempt(),
        decision: EffectReconciliationDecision::Observe,
        resolution_value: None,
        input: serde_json::Value::Null,
    }
}

#[cfg(unix)]
fn wait_for_file(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    path.exists()
}

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
    let mut config = executor_config(executable);
    config.timeout = timeout;
    config.message_limit = message_limit;
    ProcessExecutor::new(config).expect("executor configures")
}

#[cfg(unix)]
#[test]
fn process_manifest_uses_only_explicit_environment() {
    let script = r#"
      test -z "${SHOULD_NOT_LEAK:-}" || exit 9
      test ! -e ./Cargo.toml || exit 7
      request=$(/bin/cat)
      test -n "$request" || exit 8
      printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"process:test","components":{},"effects":{}}}'
    "#;
    let mut executor = shell(script);
    let manifest = executor.describe().expect("manifest returns");
    assert_eq!(manifest.implementation_id, "process:test");
    assert_eq!(executor.config().environment, BTreeMap::new());
}

#[cfg(unix)]
#[test]
fn process_timeout_is_reported_as_ambiguous() {
    let mut executor = shell_with(
        "/bin/sleep 2",
        Duration::from_millis(10),
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
    );
    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::TimedOut { code, .. }) if code == "process_response_timed_out"
    ));
}

#[cfg(unix)]
#[test]
fn reconciliation_timeout_after_spawn_is_unknown_world() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\nprintf started > '{}'\n/bin/cat >/dev/null\n/bin/sleep 5\n",
            marker.display()
        ),
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let mut config = executor_config(executable);
    config.timeout = Duration::from_secs(2);
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    let invocation = std::thread::spawn(move || executor.invoke(reconciliation_request()));
    assert!(
        wait_for_file(&marker, Duration::from_secs(3)),
        "reconciliation process must start before the deadline"
    );
    let error = invocation
        .join()
        .expect("reconciliation invocation joins")
        .expect_err("started reconciliation must cross the admitted deadline");
    assert!(
        matches!(
            &error,
            RuntimeError::UnknownWorld { code, .. } if code == "effect_dispatch_timed_out"
        ),
        "unexpected post-spawn timeout classification: {error:?}"
    );
}

#[cfg(unix)]
fn invoke_after_process_start(request: PluginRequest) -> RuntimeError {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\nprintf started > '{}'\n/bin/cat >/dev/null\n/bin/sleep 5\n",
            marker.display()
        ),
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let cancelled = ProcessCancellation::new().expect("cancellation authority creates");
    let mut config = executor_config(executable);
    config.timeout = Duration::from_secs(10);
    config.cancellation = Some(cancelled.clone());
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    let invocation = std::thread::spawn(move || executor.invoke(request));
    assert!(
        wait_for_file(&marker, Duration::from_secs(3)),
        "effect process must start before cancellation"
    );
    cancelled.cancel();
    invocation
        .join()
        .expect("effect invocation joins")
        .expect_err("started effect invocation must observe cancellation")
}

#[cfg(unix)]
#[test]
fn non_effect_process_output_limits_are_plugin_defects() {
    let script = format!(
        "/bin/cat >/dev/null; /usr/bin/yes x | /usr/bin/tr -d '\\n' | /usr/bin/head -c {}",
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES + 1
    );
    let requests = [
        PluginRequest::Describe,
        PluginRequest::Call {
            component: "test".to_owned(),
            input: serde_json::Value::Null,
        },
    ];
    for request in requests {
        let mut executor = shell_with(
            &script,
            Duration::from_secs(10),
            cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
        );
        assert!(matches!(
            executor.invoke(request),
            Err(RuntimeError::PluginDefect { code, .. })
                if code == "plugin_output_limit_exceeded"
        ));
    }
}

#[cfg(unix)]
#[test]
fn world_mutating_effect_output_limit_remains_unknown_world() {
    let script = format!(
        "/bin/cat >/dev/null; /usr/bin/yes x | /usr/bin/tr -d '\\n' | /usr/bin/head -c {}",
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES + 1
    );
    let requests = [
        PluginRequest::DispatchEffect {
            operation: "effect:test".to_owned(),
            intent_id: TEST_INTENT_ID.to_owned(),
            attempt: effect_attempt(),
            input: serde_json::Value::Null,
        },
        reconciliation_request(),
    ];
    for request in requests {
        let mut executor = shell_with(
            &script,
            Duration::from_secs(10),
            cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
        );
        assert!(matches!(
            executor.invoke(request),
            Err(RuntimeError::UnknownWorld { code, .. })
                if code == "effect_dispatch_output_limit_exceeded"
        ));
    }
}

#[cfg(unix)]
#[test]
fn provider_governance_response_after_reconciliation_start_is_unknown_world() {
    let attempt = serde_json::to_string(&effect_attempt()).expect("attempt serializes");
    let response = format!(
        "{{\"type\":\"reconciliation_result\",\"attempt\":{attempt},\"resolution\":\"governance_required\",\"value\":null}}"
    );
    let mut executor = shell(&format!("/bin/cat >/dev/null; printf '%s' '{response}'"));
    assert!(matches!(
        executor.invoke(reconciliation_request()),
        Err(RuntimeError::UnknownWorld { code, .. })
            if code == "invalid_reconciliation_resolution"
    ));
}

#[cfg(unix)]
#[test]
fn genuine_pipe_failure_remains_substrate_except_after_world_mutation_start() {
    let large = serde_json::Value::String("x".repeat(2 * 1024 * 1024));
    let mut call_executor = shell_with(
        "exit 0",
        Duration::from_secs(2),
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
    );
    assert!(matches!(
        call_executor.invoke(PluginRequest::Call {
            component: "test".to_owned(),
            input: large.clone(),
        }),
        Err(RuntimeError::Substrate { code, .. }) if code == "process_io_failed"
    ));

    let mut dispatch_executor = shell_with(
        "exit 0",
        Duration::from_secs(2),
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
    );
    assert!(matches!(
        dispatch_executor.invoke(PluginRequest::DispatchEffect {
            operation: "effect:test".to_owned(),
            intent_id: TEST_INTENT_ID.to_owned(),
            attempt: effect_attempt(),
            input: large,
        }),
        Err(RuntimeError::UnknownWorld { code, .. }) if code == "process_io_failed"
    ));

    let mut reconcile_executor = shell_with(
        "exit 0",
        Duration::from_secs(2),
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
    );
    let mut request = reconciliation_request();
    let PluginRequest::ReconcileEffect { input, .. } = &mut request else {
        unreachable!("helper returns reconciliation")
    };
    *input = serde_json::Value::String("x".repeat(2 * 1024 * 1024));
    assert!(matches!(
        reconcile_executor.invoke(request),
        Err(RuntimeError::UnknownWorld { code, .. }) if code == "process_io_failed"
    ));
}

#[cfg(unix)]
#[test]
fn process_output_exactly_at_limit_is_accepted() {
    let response = r#"{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"process:limit","components":{},"effects":{}}}"#;
    let padding = cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES - response.len();
    let mut executor = shell_with(
        &format!(
            "request=$(/bin/cat); test -n \"$request\" || exit 8; printf '%s' '{response}'; /usr/bin/yes ' ' | /usr/bin/tr -d '\\n' | /usr/bin/head -c {padding}"
        ),
        Duration::from_secs(10),
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
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
fn ordinary_plugin_requires_the_exact_semantic_process_limit_before_spawn() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf started > \"$1\"\n/bin/cat >/dev/null\n",
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");

    for limit in [
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES - 1,
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES + 1,
    ] {
        let mut config = executor_config(&executable);
        config.arguments = vec![marker.to_string_lossy().into_owned()];
        config.message_limit = limit;
        let mut executor = ProcessExecutor::new(config).expect("generic process config seals");
        assert!(matches!(
            executor.describe(),
            Err(RuntimeError::PluginDefect { code, .. })
                if code == "plugin_process_message_limit_mismatch"
        ));
        assert!(
            !marker.exists(),
            "message-limit mismatch must fail before spawn"
        );
    }
}

#[cfg(unix)]
#[test]
fn process_response_rejects_duplicate_protocol_members() {
    let script = r#"
      request=$(/bin/cat)
      test -n "$request" || exit 8
      printf '%s' '{"type":"manifest","type":"defect","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"process:test","components":{},"effects":{}}}'
    "#;
    let mut executor = shell(script);
    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::PluginDefect { code, .. }) if code == "invalid_plugin_response"
    ));
}

#[cfg(unix)]
#[test]
fn process_response_rejects_integer_outside_shared_json_domain() {
    let script = r#"
      request=$(/bin/cat)
      test -n "$request" || exit 8
      printf '%s' '{"type":"call_result","value":9007199254740992}'
    "#;
    let mut executor = shell(script);
    assert!(matches!(
        executor.invoke(PluginRequest::Call {
            component: "test".to_owned(),
            input: serde_json::Value::Null,
        }),
        Err(RuntimeError::PluginDefect { code, .. }) if code == "invalid_plugin_response"
    ));
}

#[cfg(unix)]
#[test]
fn malformed_world_mutating_effect_response_remains_unknown_world() {
    let requests = [
        PluginRequest::DispatchEffect {
            operation: "effect:test".to_owned(),
            intent_id: TEST_INTENT_ID.to_owned(),
            attempt: effect_attempt(),
            input: serde_json::Value::Null,
        },
        reconciliation_request(),
    ];
    for request in requests {
        let mut executor = shell("/bin/cat >/dev/null; printf '%s' '{not-json}'");
        let result = executor.invoke(request);
        assert!(
            matches!(
                &result,
                Err(RuntimeError::UnknownWorld { code, .. })
                    if code == "invalid_plugin_response"
            ),
            "malformed mutating response returned an unexpected result: {result:?}"
        );
    }
}

#[cfg(unix)]
#[test]
fn process_rejects_a_provider_response_for_another_attempt() {
    let response = format!(
        "{{\"type\":\"effect_result\",\"attempt\":{{\"attempt_version\":\"cymule.effect-provider-attempt/1\",\"attempt_id\":\"sha256:{}\",\"claim_owner\":\"owner:other\",\"claim_epoch\":2}},\"outcome\":\"not_applied\",\"value\":null}}",
        "f".repeat(64)
    );
    let mut executor = shell(&format!("/bin/cat >/dev/null; printf '%s' '{response}'"));
    assert!(matches!(
        executor.invoke(PluginRequest::DispatchEffect {
            operation: "effect:test".to_owned(),
            intent_id: TEST_INTENT_ID.to_owned(),
            attempt: effect_attempt(),
            input: serde_json::Value::Null,
        }),
        Err(RuntimeError::UnknownWorld { code, .. }) if code == "invalid_plugin_response"
    ));

    let response = response
        .replace("effect_result", "reconciliation_result")
        .replace(
            "\"outcome\":\"not_applied\"",
            "\"resolution\":\"resolved_not_applied\"",
        );
    let mut executor = shell(&format!("/bin/cat >/dev/null; printf '%s' '{response}'"));
    assert!(matches!(
        executor.invoke(PluginRequest::ReconcileEffect {
            operation: "effect:test".to_owned(),
            intent_id: TEST_INTENT_ID.to_owned(),
            attempt: effect_attempt(),
            decision: EffectReconciliationDecision::ResolveNotApplied,
            resolution_value: None,
            input: serde_json::Value::Null,
        }),
        Err(RuntimeError::UnknownWorld { code, .. }) if code == "invalid_plugin_response"
    ));
}

#[cfg(unix)]
#[test]
fn invalid_reconciliation_attempt_is_rejected_before_process_start() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf started > \"$1\"\n/bin/cat >/dev/null\n",
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let mut config = executor_config(executable);
    config.arguments = vec![marker.to_string_lossy().into_owned()];
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    let mut request = reconciliation_request();
    let PluginRequest::ReconcileEffect { intent_id, .. } = &mut request else {
        unreachable!("helper returns reconciliation")
    };
    *intent_id = OTHER_INTENT_ID.to_owned();

    assert!(matches!(
        executor.invoke(request),
        Err(RuntimeError::PluginDefect { .. })
    ));
    assert!(!marker.exists(), "invalid attempts must fail before spawn");
}

#[cfg(unix)]
#[test]
fn outbound_plugin_and_adjacent_protocol_json_are_strict_before_spawn() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf started > \"$1\"\n/bin/cat >/dev/null\n",
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let mut config = executor_config(&executable);
    config.arguments = vec![marker.to_string_lossy().into_owned()];
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    let unsafe_integer = u64::MAX;

    assert!(matches!(
        executor.invoke(PluginRequest::Call {
            component: "test".to_owned(),
            input: serde_json::json!(unsafe_integer),
        }),
        Err(RuntimeError::PluginDefect { code, .. }) if code == "invalid_plugin_message_encoding"
    ));
    let mut evolution_config = executor_config(executable);
    evolution_config.arguments = vec![marker.to_string_lossy().into_owned()];
    evolution_config.message_limit = cymule_runtime::EVOLUTION_PLUGIN_MESSAGE_LIMIT;
    let evolution_executor =
        ProcessExecutor::new(evolution_config).expect("Evolution executor configures");
    let evolution_request = serde_json::to_vec(&serde_json::json!({ "value": unsafe_integer }))
        .expect("unsafe request serializes");
    let adjacent_result = evolution_executor.invoke_evolution_bytes(&evolution_request);
    assert!(matches!(
        adjacent_result,
        Err(RuntimeError::PluginDefect { code, .. }) if code == "invalid_process_request"
    ));
    assert!(
        !marker.exists(),
        "outbound JSON outside I-JSON must fail before any plugin or evolution process starts"
    );
}

#[cfg(unix)]
#[test]
fn evolution_transport_accepts_exact_raw_bound_and_rejects_max_plus_one_before_spawn() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf started > \"$1\"\n/bin/cat >/dev/null\nprintf '{}'\n",
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let mut config = executor_config(executable);
    config.arguments = vec![marker.to_string_lossy().into_owned()];
    config.timeout = Duration::from_secs(10);
    config.message_limit = cymule_runtime::EVOLUTION_PLUGIN_MESSAGE_LIMIT;
    let executor = ProcessExecutor::new(config).expect("executor configures");

    let mut request = vec![b' '; cymule_runtime::EVOLUTION_PLUGIN_MESSAGE_LIMIT];
    request[0] = b'{';
    request[1] = b'}';
    let response = executor
        .invoke_evolution_bytes(&request)
        .expect("the exact raw request bound crosses the process transport");
    assert_eq!(response.as_slice(), b"{}");
    fs::remove_file(&marker).expect("exact-bound marker removes");

    request.push(b' ');
    assert!(matches!(
        executor.invoke_evolution_bytes(&request),
        Err(RuntimeError::PluginDefect { code, .. })
            if code == "evolution_process_request_too_large"
    ));
    assert!(
        !marker.exists(),
        "max-plus-one rejection must happen before process materialization or spawn"
    );
}

#[cfg(unix)]
#[test]
fn owning_engine_cancellation_terminates_the_plugin_occurrence() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("escaped-marker");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\n/bin/cat >/dev/null\n/bin/sleep 1\nprintf escaped > '{}'\n",
            marker.display()
        ),
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let cancelled = ProcessCancellation::new().expect("cancellation authority creates");
    let mut config = executor_config(executable);
    config.timeout = Duration::from_secs(5);
    config.cancellation = Some(cancelled.clone());
    let mut executor = ProcessExecutor::new(config).expect("executor configures");
    let trigger = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        cancelled.cancel();
    });
    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::Cancelled { code, .. }) if code == "process_invocation_cancelled"
    ));
    trigger.join().expect("cancellation trigger joins");
    std::thread::sleep(Duration::from_millis(1100));
    assert!(
        !marker.exists(),
        "cancelled plugin occurrence must not retain execution authority"
    );
}

#[cfg(unix)]
#[test]
fn cancellation_before_invocation_never_starts_the_process() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        format!("#!/bin/sh\nprintf started > '{}'\n", marker.display()),
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let cancelled = ProcessCancellation::new().expect("cancellation authority creates");
    cancelled.cancel();
    let mut config = executor_config(executable);
    config.cancellation = Some(cancelled);
    let mut executor = ProcessExecutor::new(config).expect("executor configures");

    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::Cancelled { code, .. }) if code == "process_invocation_cancelled"
    ));
    assert!(
        !marker.exists(),
        "pre-cancelled work performs no provider I/O"
    );
}

#[cfg(unix)]
#[test]
fn cancellation_during_closure_materialization_prevents_spawn() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let source_tree = directory.path().join("source-tree");
    fs::create_dir(&source_tree).expect("source tree creates");
    fs::write(source_tree.join("large.bin"), vec![0_u8; 32 * 1024 * 1024])
        .expect("bounded source file writes");
    let marker = directory.path().join("started");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\nprintf started > '{}'\n/bin/cat >/dev/null\n",
            marker.display()
        ),
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let cancelled = ProcessCancellation::new().expect("cancellation authority creates");
    let mut config = executor_config(executable);
    config.working_directory = Some(source_tree);
    config.closure_limit = 40 * 1024 * 1024;
    config.timeout = Duration::from_secs(5);
    config.cancellation = Some(cancelled.clone());
    let mut executor = ProcessExecutor::new(config).expect("executor captures closure");
    let trigger = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1));
        cancelled.cancel();
    });

    assert!(matches!(
        executor.describe(),
        Err(RuntimeError::Cancelled { code, .. }) if code == "process_invocation_cancelled"
    ));
    trigger.join().expect("cancellation trigger joins");
    assert!(
        !marker.exists(),
        "cancellation observed during private copy must linearize before spawn"
    );
}

#[cfg(unix)]
#[test]
fn cancellation_after_effect_dispatch_start_remains_unknown_world() {
    let error = invoke_after_process_start(PluginRequest::DispatchEffect {
        operation: "effect:test".to_owned(),
        intent_id: TEST_INTENT_ID.to_owned(),
        attempt: effect_attempt(),
        input: serde_json::Value::Null,
    });
    assert!(
        matches!(
            &error,
            RuntimeError::UnknownWorld { code, .. } if code == "effect_dispatch_cancelled"
        ),
        "unexpected post-spawn cancellation classification: {error:?}"
    );
}

#[cfg(unix)]
#[test]
fn cancellation_after_reconciliation_start_remains_unknown_world() {
    let error = invoke_after_process_start(reconciliation_request());
    assert!(
        matches!(
            &error,
            RuntimeError::UnknownWorld { code, .. } if code == "effect_dispatch_cancelled"
        ),
        "unexpected post-spawn cancellation classification: {error:?}"
    );
}

#[test]
fn relative_executable_is_rejected() {
    assert!(matches!(
        ProcessExecutor::new(executor_config("plugin")),
        Err(RuntimeError::PluginDefect { .. })
    ));
}

#[cfg(unix)]
#[test]
fn executable_capture_rejects_special_and_over_limit_files_without_blocking() {
    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;
    use std::fs::File;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let fifo = directory.path().join("plugin.fifo");
    mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR).expect("FIFO creates");
    let started = Instant::now();
    assert!(matches!(
        ProcessExecutor::new(executor_config(&fifo)),
        Err(RuntimeError::PluginDefect { .. })
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "FIFO admission must not wait for a writer"
    );

    assert!(matches!(
        ProcessExecutor::new(executor_config("/dev/null")),
        Err(RuntimeError::PluginDefect { .. })
    ));

    let oversized = directory.path().join("oversized-plugin");
    File::create(&oversized)
        .expect("oversized fixture creates")
        .set_len(65)
        .expect("oversized fixture length sets");
    let mut config = executor_config(oversized);
    config.closure_limit = 64;
    assert!(matches!(
        ProcessExecutor::new(config),
        Err(RuntimeError::PluginDefect { .. })
    ));
}

#[cfg(unix)]
#[test]
fn complete_closure_limit_rejects_config_metadata_and_empty_entry_fanout() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let executable = directory.path().join("plugin.sh");
    fs::write(&executable, "#!/bin/sh\nexit 0\n").expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");

    let mut oversized_config = executor_config(&executable);
    oversized_config.arguments = vec!["x".repeat(2 * 1024)];
    oversized_config.closure_limit = 1024;
    assert!(matches!(
        ProcessExecutor::new(oversized_config),
        Err(RuntimeError::PluginDefect { .. })
    ));

    let working = directory.path().join("working");
    fs::create_dir(&working).expect("working directory creates");
    for index in 0..128 {
        fs::write(working.join(format!("empty-{index:03}")), []).expect("empty entry writes");
    }
    let mut empty_fanout = executor_config(executable);
    empty_fanout.working_directory = Some(working);
    empty_fanout.closure_limit = 1024;
    assert!(matches!(
        ProcessExecutor::new(empty_fanout),
        Err(RuntimeError::PluginDefect { .. })
    ));
}

#[cfg(unix)]
#[test]
fn plugin_exec_inherits_only_standard_descriptors() {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    use std::fs::{self, File};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let sentinel_path = directory.path().join("sentinel");
    fs::write(&sentinel_path, b"ambient authority").expect("sentinel writes");
    let sentinel = File::open(&sentinel_path).expect("sentinel opens");
    fcntl(&sentinel, FcntlArg::F_SETFD(FdFlag::empty())).expect("sentinel clears CLOEXEC");
    let sentinel_fd = sentinel.as_raw_fd();

    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        r#"#!/bin/sh
test ! -r "/dev/fd/$1" || exit 41
/bin/cat >/dev/null
printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"process:fd-closed","components":{},"effects":{}}}'
"#,
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let mut config = executor_config(executable);
    config.arguments = vec![sentinel_fd.to_string()];
    let executors = [
        ProcessExecutor::new(config.clone()).expect("first executor configures"),
        ProcessExecutor::new(config).expect("second executor configures"),
    ];
    let barrier = Arc::new(std::sync::Barrier::new(3));
    std::thread::scope(|scope| {
        let handles = executors.map(|mut executor| {
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                executor
                    .describe()
                    .expect("plugin cannot observe the ambient descriptor")
                    .implementation_id
            })
        });
        barrier.wait();
        for handle in handles {
            assert_eq!(
                handle.join().expect("executor thread joins"),
                "process:fd-closed"
            );
        }
    });
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
printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"sealed:original","components":{},"effects":{}}}'
"#;
    fs::write(&source, original).expect("source writes");
    fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).expect("source executes");
    let mut executor = ProcessExecutor::new(executor_config(&source)).expect("source bytes seal");
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
      printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"sealed:self-original","components":{},"effects":{}}}'
      /bin/chmod 700 "$0"
      printf '%s\n' '#!/bin/sh' 'request=$(/bin/cat)' 'printf '\''%s'\'' '\''{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"mutable:self-replacement","components":{},"effects":{}}}'\''' > "$0"
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
      printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"process:tree","components":{},"effects":{}}}'
    "#;
    let mut executor = shell_with(
        script,
        Duration::from_secs(2),
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
    );
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
fn parent_liveness_engine_helper() {
    let Some(executable) = std::env::var_os("CYMULE_TEST_WATCHDOG_EXECUTABLE") else {
        return;
    };
    let started =
        std::env::var_os("CYMULE_TEST_WATCHDOG_STARTED").expect("helper receives started marker");
    let late = std::env::var_os("CYMULE_TEST_WATCHDOG_LATE").expect("helper receives late marker");
    let invoke = |started: &std::ffi::OsStr, late: &std::ffi::OsStr| {
        let mut config = executor_config(&executable);
        config.arguments = vec![
            started.to_string_lossy().into_owned(),
            late.to_string_lossy().into_owned(),
        ];
        config.timeout = Duration::from_secs(10);
        let mut executor = ProcessExecutor::new(config).expect("helper executor configures");
        let _ = executor.describe();
    };
    if let (Some(second_started), Some(second_late)) = (
        std::env::var_os("CYMULE_TEST_WATCHDOG_STARTED_2"),
        std::env::var_os("CYMULE_TEST_WATCHDOG_LATE_2"),
    ) {
        std::thread::scope(|scope| {
            scope.spawn(|| invoke(&started, &late));
            scope.spawn(|| invoke(&second_started, &second_late));
        });
    } else {
        invoke(&started, &late);
    }
}

#[cfg(unix)]
#[test]
fn sigkill_of_the_engine_cannot_orphan_the_plugin_group() {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let started = directory.path().join("started");
    let late = directory.path().join("late");
    let second_started = directory.path().join("started-2");
    let second_late = directory.path().join("late-2");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        r#"#!/bin/sh
/bin/cat >/dev/null
printf started > "$1"
/bin/sleep 1
printf late > "$2"
"#,
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");

    let mut engine = Command::new(std::env::current_exe().expect("test executable resolves"));
    engine
        .arg("--exact")
        .arg("parent_liveness_engine_helper")
        .arg("--nocapture")
        .env_clear()
        .env("CYMULE_TEST_WATCHDOG_EXECUTABLE", &executable)
        .env("CYMULE_TEST_WATCHDOG_STARTED", &started)
        .env("CYMULE_TEST_WATCHDOG_LATE", &late)
        .env("CYMULE_TEST_WATCHDOG_STARTED_2", &second_started)
        .env("CYMULE_TEST_WATCHDOG_LATE_2", &second_late)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut engine = engine.spawn().expect("real Engine helper starts");
    if !wait_for_file(&started, Duration::from_secs(5)) {
        let _ = engine.kill();
        let _ = engine.wait();
        panic!("plugin did not reach its started marker");
    }
    if !wait_for_file(&second_started, Duration::from_secs(5)) {
        let _ = engine.kill();
        let _ = engine.wait();
        panic!("second plugin did not reach its started marker");
    }
    let engine_pid = i32::try_from(engine.id()).expect("Engine pid fits pid_t");
    kill(Pid::from_raw(engine_pid), Signal::SIGKILL).expect("Engine receives SIGKILL");
    let status = engine.wait().expect("killed Engine is reaped");
    assert!(!status.success(), "SIGKILL must terminate the real Engine");

    std::thread::sleep(Duration::from_millis(1300));
    assert!(
        !late.exists() && !second_late.exists(),
        "concurrent group watchdogs must not retain each other's Engine-liveness descriptors"
    );
}

#[cfg(unix)]
#[test]
fn natural_completion_closes_the_supervised_group_without_late_work() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("fixture directory creates");
    let late = directory.path().join("late");
    let executable = directory.path().join("plugin.sh");
    fs::write(
        &executable,
        r#"#!/bin/sh
/bin/cat >/dev/null
(/bin/sleep 1; printf late > "$1") &
printf '%s' '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"process:natural","components":{},"effects":{}}}'
"#,
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let mut config = executor_config(executable);
    config.arguments = vec![late.to_string_lossy().into_owned()];
    config.timeout = Duration::from_secs(3);
    let mut executor = ProcessExecutor::new(config).expect("executor configures");

    assert_eq!(
        executor
            .describe()
            .expect("natural plugin completion remains authoritative")
            .implementation_id,
        "process:natural"
    );
    std::thread::sleep(Duration::from_millis(1200));
    assert!(
        !late.exists(),
        "natural completion must still terminate the complete supervised group"
    );
}

#[cfg(unix)]
#[test]
fn timeout_kills_and_reaps_the_occurrence_process_group() {
    let script = r"
      (/bin/sleep 10) &
      /bin/sleep 10
    ";
    let mut executor = shell_with(
        script,
        Duration::from_millis(100),
        cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES,
    );
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

    let mut baseline = executor_config(&executable);
    baseline.arguments = vec!["one".to_owned()];
    baseline
        .environment
        .insert("MODE".to_owned(), "one".to_owned());
    baseline.working_directory = Some(working.clone());
    baseline
        .runtime_closure
        .insert("loader".to_owned(), format!("sha256:{}", "1".repeat(64)));
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
        .insert("loader".to_owned(), format!("sha256:{}", "2".repeat(64)));
    let mut timeout = baseline.clone();
    timeout.timeout = Duration::from_secs(2);
    let mut message_limit = baseline.clone();
    message_limit.message_limit /= 2;
    let mut closure_limit = baseline.clone();
    closure_limit.closure_limit /= 2;
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
        ProcessExecutor::new(closure_limit)
            .expect("closure-limit variant captures")
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
printf '{"type":"manifest","manifest":{"plugin_version":"cymule.plugin/3","implementation_id":"cwd:%s","components":{},"effects":{}}}' "$value"
printf '%s' changed > value.txt
"#,
    )
    .expect("script writes");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("script executes");
    let working = directory.path().join("working");
    fs::create_dir(&working).expect("working directory creates");
    fs::write(working.join("value.txt"), "captured").expect("working value writes");
    let mut config = executor_config(executable);
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
            intent_id: TEST_INTENT_ID.to_owned(),
            attempt: effect_attempt(),
            input: serde_json::Value::Null,
        })
        .expect_err("failed dispatch process is ambiguous");
    assert!(matches!(error, RuntimeError::UnknownWorld { .. }));
    assert!(!error.to_string().contains(secret));

    let error = executor
        .invoke(reconciliation_request())
        .expect_err("failed reconciliation process is ambiguous");
    assert!(matches!(error, RuntimeError::UnknownWorld { .. }));
    assert!(!error.to_string().contains(secret));
}

#[cfg(not(unix))]
#[test]
fn unsupported_platform_is_rejected_before_execution() {
    assert!(matches!(
        ProcessExecutor::new(executor_config(r"C:\plugin.exe")),
        Err(RuntimeError::PluginDefect { .. })
    ));
}
