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
fn direct_cli_uses_the_lossless_strict_typed_json_gate() {
    let base = r#"{
      "resource_version":"cymule.resource/3",
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
                "\"resource_version\":\"cymule.resource/3\"",
                "\"resource_version\":\"cymule.resource/3\",\"resource_version\":\"cymule.resource/3\"",
                1,
            ),
            "duplicate JSON object",
        ),
        (
            "unsafe integer",
            base.replacen("\"value\":null", "\"value\":9007199254740992", 1),
            "exact cross-language range",
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
