//! Real-process Engine RPC ingress boundaries.

#![cfg(unix)]

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;

fn spawn_rpc() -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_cymule"))
        .arg("rpc")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("real Cymule RPC process starts")
}

fn assert_blocked_rpc_output_is_interrupted(signal: nix::sys::signal::Signal) {
    let mut child = spawn_rpc();
    let large_value = "x".repeat(900_000);
    let request = serde_json::to_vec(&serde_json::json!({
        "engine_protocol": "cymule.engine/5",
        "request": {
            "type": "seal_resource",
            "candidate": {
                "resource_version": "cymule.resource/4",
                "shape": "inline",
                "media_type": "application/json",
                "inline": {
                    "encoding": "json",
                    "value": large_value
                },
                "integrity": { "kind": "inline" }
            }
        }
    }))
    .expect("large legal Engine request serializes");
    child
        .stdin
        .take()
        .expect("child stdin is piped")
        .write_all(&request)
        .expect("large legal Engine request writes");

    {
        use std::os::fd::AsFd;

        let stdout = child.stdout.as_ref().expect("child stdout is piped");
        let mut readiness = [nix::poll::PollFd::new(
            stdout.as_fd(),
            nix::poll::PollFlags::POLLIN,
        )];
        assert!(
            nix::poll::poll(&mut readiness, 5_000_u16).expect("stdout readiness polls") > 0,
            "large response begins writing before cancellation"
        );
    }
    let child_pid = i32::try_from(child.id()).expect("child PID fits the platform PID domain");
    let child_pid = nix::unistd::Pid::from_raw(child_pid);
    nix::sys::signal::kill(child_pid, nix::sys::signal::Signal::SIGSTOP)
        .expect("SIGSTOP freezes the child after response output begins");
    assert!(
        matches!(
            nix::sys::wait::waitpid(child_pid, Some(nix::sys::wait::WaitPidFlag::WUNTRACED))
                .expect("stopped child status reads"),
            nix::sys::wait::WaitStatus::Stopped(_, nix::sys::signal::Signal::SIGSTOP)
        ),
        "large-response child reaches a real stopped output state"
    );
    nix::sys::signal::kill(child_pid, signal).expect("signal reaches the output-blocked RPC child");
    nix::sys::signal::kill(child_pid, nix::sys::signal::Signal::SIGCONT)
        .expect("SIGCONT releases the signal-pending RPC child");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("child status reads") {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "RPC child ignored {signal:?} while stdout remained blocked"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("child stderr is piped")
        .read_to_string(&mut stderr)
        .expect("transport diagnostic reads");
    assert!(
        !status.success(),
        "failure to carry the response is a process transport failure: {stderr:?}"
    );
    assert!(
        stderr.contains("Engine response output was cancelled"),
        "unexpected output cancellation diagnostic: {stderr:?}"
    );
}

fn run_direct(arguments: &[&str], input: &[u8]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cymule"))
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("real Cymule direct command starts");
    child
        .stdin
        .take()
        .expect("child stdin is piped")
        .write_all(input)
        .expect("direct command input writes");
    child.wait_with_output().expect("direct command exits")
}

fn run_rpc(input: &[u8]) -> std::process::Output {
    let mut child = spawn_rpc();
    child
        .stdin
        .take()
        .expect("child stdin is piped")
        .write_all(input)
        .expect("RPC input writes");
    child.wait_with_output().expect("RPC child exits")
}

#[test]
fn partial_open_stdin_is_interrupted_by_sigint() {
    let mut child = spawn_rpc();
    let mut stdin = child.stdin.take().expect("child stdin is piped");
    let (written_tx, written_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let chunk = vec![b' '; 64 * 1024].into_boxed_slice();
        for _ in 0..16 {
            stdin.write_all(&chunk).expect("partial request writes");
        }
        written_tx
            .send(())
            .expect("writer proves the child drained beyond pipe capacity");
        release_rx
            .recv()
            .expect("test releases the still-open stdin pipe");
    });
    written_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("real RPC reader consumes the partial request");
    let child_pid = i32::try_from(child.id()).expect("child PID fits the platform PID domain");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child_pid),
        nix::sys::signal::Signal::SIGINT,
    )
    .expect("SIGINT reaches the live RPC child");

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("child status reads") {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "RPC child ignored SIGINT while stdin remained open"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    release_tx.send(()).expect("stdin writer is released");
    writer.join().expect("stdin writer exits");
    assert!(status.success(), "protocol failure remains an RPC envelope");

    let mut stdout = Vec::new();
    child
        .stdout
        .take()
        .expect("child stdout is piped")
        .read_to_end(&mut stdout)
        .expect("RPC response reads");
    let envelope: Value = serde_json::from_slice(&stdout).expect("RPC response is JSON");
    assert_eq!(envelope["outcome"], "failure");
    assert_eq!(envelope["error"]["category"], "cancelled");
    assert_eq!(envelope["error"]["code"], "engine_read_cancelled");
    assert_eq!(envelope["error"]["retry_disposition"], "never");
}

#[test]
fn blocked_rpc_stdout_is_interrupted_by_sigterm_and_sigint() {
    for signal in [
        nix::sys::signal::Signal::SIGTERM,
        nix::sys::signal::Signal::SIGINT,
    ] {
        assert_blocked_rpc_output_is_interrupted(signal);
    }
}

#[test]
fn rpc_rejects_typed_decoding_that_collapses_array_elements() {
    let mut activation: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/wait-activation.json"))
            .expect("shared wait activation fixture decodes");
    let repeated = activation["wait_ids"][0].clone();
    activation["wait_ids"]
        .as_array_mut()
        .expect("wait IDs are an array")
        .push(repeated);
    let request = serde_json::to_vec(&serde_json::json!({
        "engine_protocol": "cymule.engine/5",
        "request": {
            "type": "verify_wait_activation",
            "activation": activation
        }
    }))
    .expect("duplicate wait request serializes");
    let output = run_rpc(&request);
    assert!(
        output.status.success(),
        "semantic rejection remains a valid Engine envelope: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let envelope: Value = serde_json::from_slice(&output.stdout).expect("response is JSON");
    assert_eq!(envelope["outcome"], "failure");
    assert_eq!(envelope["error"]["category"], "validation");
    assert_eq!(envelope["error"]["code"], "invalid_engine_request");
    assert_eq!(envelope["error"]["retry_disposition"], "correct_and_retry");
}

#[test]
fn direct_cli_uses_the_lossless_strict_typed_json_gate() {
    let base = r#"{
      "resource_version":"cymule.resource/4",
      "shape":"inline",
      "media_type":"application/json",
      "inline":{"encoding":"json","value":null},
      "integrity":{"kind":"inline"}
    }"#;
    let omitted = run_direct(&["resource", "seal", "--input", "-"], base.as_bytes());
    assert!(
        omitted.status.success(),
        "omitted optional members are canonical"
    );

    for (label, malformed, expected) in [
        (
            "empty annotations",
            base.replacen("\n    }", ",\n      \"annotations\":{}\n    }", 1),
            "/annotations",
        ),
        (
            "explicit nullable omission",
            base.replacen("\n    }", ",\n      \"manifest\":null\n    }", 1),
            "/manifest",
        ),
        (
            "duplicate member",
            base.replacen(
                "\"resource_version\":\"cymule.resource/4\"",
                "\"resource_version\":\"cymule.resource/4\",\"resource_version\":\"cymule.resource/4\"",
                1,
            ),
            "duplicate JSON object",
        ),
        (
            "unsafe integer",
            base.replacen("\"value\":null", "\"value\":9007199254740992", 1),
            "exact cross-language range",
        ),
        (
            "fractional decimal collision",
            base.replacen(
                "\"value\":null",
                "\"value\":0.100000000000000005",
                1,
            ),
            "/inline/value",
        ),
    ] {
        let output = run_direct(
            &["resource", "seal", "--input", "-"],
            malformed.as_bytes(),
        );
        assert!(!output.status.success(), "{label} must fail closed");
        let stderr = String::from_utf8(output.stderr).expect("diagnostic is UTF-8");
        assert!(stderr.contains(expected), "{label} diagnostic was {stderr:?}");
    }

    let fractional_limit = br#"{
      "type":"run_index_page",
      "control_version":"cymule.durable-control/4",
      "expected_revision":null,
      "cursor":null,
      "limit":1.5,
      "max_canonical_bytes":1048576
    }"#;
    let output = run_direct(
        &["durable-command", "verify", "--input", "-"],
        fractional_limit,
    );
    assert!(
        !output.status.success(),
        "fractional typed integer is rejected"
    );
}

#[test]
fn direct_cli_hard_cuts_resource_media_type_and_predecessor_generation() {
    let candidate = |resource_version: &str, media_type: &str| {
        serde_json::to_vec(&serde_json::json!({
            "resource_version": resource_version,
            "shape": "inline",
            "media_type": media_type,
            "inline": {"encoding": "utf8", "text": "value"},
            "integrity": {"kind": "inline"}
        }))
        .expect("Resource candidate serializes")
    };

    let vendor = run_direct(
        &["resource", "seal", "--input", "-"],
        &candidate("cymule.resource/4", "application/vnd.cymule.resource+json"),
    );
    assert!(
        vendor.status.success(),
        "valid vendor media type was rejected: {}",
        String::from_utf8_lossy(&vendor.stderr)
    );

    for media_type in [
        "text/\0plain",
        "text/",
        "/plain",
        "a/b/c",
        "Text/plain",
        "text/Plain",
        "text/plain;charset=utf-8",
        "text/ plain",
    ] {
        let output = run_direct(
            &["resource", "seal", "--input", "-"],
            &candidate("cymule.resource/4", media_type),
        );
        assert!(
            !output.status.success(),
            "invalid media type {media_type:?} was accepted"
        );
    }

    let predecessor = run_direct(
        &["resource", "seal", "--input", "-"],
        &candidate("cymule.resource/3", "text/plain"),
    );
    assert!(
        !predecessor.status.success(),
        "predecessor Resource generation was accepted"
    );
}
