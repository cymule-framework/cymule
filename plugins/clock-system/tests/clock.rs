//! Restart, wall-clock regression, integrity, and contention tests.

use cymule_clock_system::{ClockObservation, SqliteClock, WallClock};
use cymule_durable::{DurableError, DurableResult};
use rusqlite::Connection;
use tempfile::tempdir;

#[derive(Clone, Copy)]
struct FixedWall(u64);

impl WallClock for FixedWall {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        Ok(self.0)
    }
}

#[test]
fn logical_time_advances_across_reopen_and_backward_wall_time() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let first = SqliteClock::open_with_wall_clock(&database, "clock:one", FixedWall(100))
        .expect("clock opens")
        .observe("leases")
        .expect("first observation allocates");
    assert_eq!(first.logical_time, 100);

    let second = SqliteClock::open_with_wall_clock(&database, "clock:one", FixedWall(90))
        .expect("clock reopens")
        .observe("leases")
        .expect("second observation allocates");
    assert_eq!(second.logical_time, 101);
    assert_eq!(second.observed_unix_ms, 90);
    second.verify().expect("observation verifies");

    let other = SqliteClock::open_with_wall_clock(&database, "clock:one", FixedWall(7))
        .expect("clock reopens")
        .observe("timers")
        .expect("independent scope allocates");
    assert_eq!(other.logical_time, 7);
}

#[test]
fn tampered_observation_and_active_writer_fail_closed() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock = SqliteClock::open_with_wall_clock(&database, "clock:one", FixedWall(100))
        .expect("clock opens");
    let mut observation: ClockObservation = clock.observe("leases").expect("observes");
    observation.logical_time += 1;
    assert!(matches!(
        observation.verify(),
        Err(DurableError::Validation(_))
    ));

    let blocker = Connection::open(&database).expect("blocking connection opens");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("writer transaction begins");
    assert!(matches!(
        clock.observe("leases"),
        Err(DurableError::Conflict { .. })
    ));
    blocker.execute_batch("ROLLBACK").expect("writer releases");
}
