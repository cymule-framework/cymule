//! Real process-death witnesses around the durable clock-observation boundary.

#![cfg(unix)]

use std::fs;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use cymule_clock_system::{ClockObservation, SqliteClock, WallClock};
use cymule_durable::DurableResult;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::Connection;

#[derive(Clone, Copy)]
struct FixedWall(u64);

impl WallClock for FixedWall {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        Ok(self.0)
    }
}

#[test]
fn clock_process_kill_worker_entry() {
    let Ok(database) = std::env::var("CYMULE_CLOCK_KILL_DB") else {
        return;
    };
    let marker = PathBuf::from(std::env::var("CYMULE_CLOCK_KILL_MARKER").expect("marker exists"));
    let receipt =
        PathBuf::from(std::env::var("CYMULE_CLOCK_KILL_RECEIPT").expect("receipt path exists"));
    let mode = std::env::var("CYMULE_CLOCK_KILL_MODE").expect("kill mode exists");
    let mut clock =
        SqliteClock::open_with_wall_clock(database, "clock:process-kill", FixedWall(100))
            .expect("clock opens");
    match mode.as_str() {
        "before_observe" => {
            fs::write(marker, "before_observe").expect("pre-observation barrier writes");
        }
        "after_observe" => {
            let observation = clock.observe("leases").expect("observation commits");
            fs::write(
                receipt,
                serde_json::to_vec(&observation).expect("observation encodes"),
            )
            .expect("observation receipt writes");
            fs::write(marker, "after_observe").expect("post-observation barrier writes");
        }
        mode => panic!("unknown clock kill mode {mode}"),
    }
    loop {
        thread::park_timeout(Duration::from_mins(1));
    }
}

#[test]
fn logical_clock_survives_real_process_death_on_both_observation_sides() {
    for (seed, mode) in [(21, "before_observe"), (22, "after_observe")] {
        let world = TestWorld::new(seed).expect("clock test world creates");
        let database = world
            .domain()
            .path("clock.sqlite")
            .expect("clock path resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("marker path resolves");
        let receipt = world
            .domain()
            .path("observation.json")
            .expect("receipt path resolves");
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("clock_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_CLOCK_KILL_DB", &database)
            .env("CYMULE_CLOCK_KILL_MARKER", &marker)
            .env("CYMULE_CLOCK_KILL_RECEIPT", &receipt)
            .env("CYMULE_CLOCK_KILL_MODE", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("clock worker starts");
        child
            .wait_for_path(&marker, Duration::from_secs(20))
            .expect("clock worker reaches exact barrier");
        assert_eq!(fs::read_to_string(&marker).expect("barrier reads"), mode);
        assert_eq!(
            child.terminate().expect("clock worker reaps").signal(),
            Some(9)
        );
        assert!(child.is_reaped());

        assert_sqlite_integrity(&database);
        if mode == "before_observe" {
            assert!(!receipt.exists());
            let first =
                SqliteClock::open_with_wall_clock(&database, "clock:process-kill", FixedWall(100))
                    .expect("clock reopens")
                    .observe("leases")
                    .expect("first observation commits after reopen");
            assert_eq!(first.logical_time, 100);
        } else {
            let retained: ClockObservation =
                serde_json::from_slice(&fs::read(&receipt).expect("observation receipt reads"))
                    .expect("observation receipt decodes");
            retained.verify().expect("retained observation verifies");
            assert_eq!(retained.logical_time, 100);
            let next =
                SqliteClock::open_with_wall_clock(&database, "clock:process-kill", FixedWall(90))
                    .expect("clock reopens with regressed wall time")
                    .observe("leases")
                    .expect("next observation commits");
            assert_eq!(next.logical_time, retained.logical_time + 1);
            assert_eq!(next.observed_unix_ms, 90);
            next.verify().expect("next observation verifies");
        }
        assert_sqlite_integrity(&database);
    }
}

fn assert_sqlite_integrity(path: &Path) {
    let connection = Connection::open(path).expect("clock database opens for integrity probe");
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode reads");
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    let results = connection
        .prepare("PRAGMA integrity_check")
        .expect("integrity statement prepares")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("SQLite integrity check runs")
        .collect::<Result<Vec<_>, _>>()
        .expect("integrity rows read");
    assert_eq!(results, ["ok"]);
}
