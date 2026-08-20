#![cfg(unix)]

//! Live child-process lifecycle and teardown tests.

use std::fs;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use cymule_test_world::{ManagedChild, TestWorld};

#[test]
fn managed_child_worker_entry() {
    let Ok(marker) = std::env::var("CYMULE_TEST_WORLD_MARKER") else {
        return;
    };
    fs::write(marker, b"ready").expect("worker barrier writes");
    loop {
        thread::park_timeout(Duration::from_mins(1));
    }
}

#[test]
fn managed_child_drop_reaps_when_caller_forgets_to_terminate() {
    let world = TestWorld::new(17).expect("world creates");
    let marker = world.domain().path("child-ready").expect("marker resolves");
    let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
    command
        .arg("--exact")
        .arg("managed_child_worker_entry")
        .arg("--nocapture")
        .env("CYMULE_TEST_WORLD_MARKER", &marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let mut child = ManagedChild::spawn(&mut command).expect("child starts");
    let process_id = child.id().expect("child has an id");
    child
        .wait_for_path(&marker, Duration::from_secs(10))
        .expect("child reaches barrier");
    drop(child);

    let status = Command::new("/bin/kill")
        .arg("-0")
        .arg(process_id.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("leak probe runs");
    assert!(!status.success(), "reaped child must not remain live");
}
