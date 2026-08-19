//! `SQLite` reopen, fencing, contention, and integrity tests.

use std::time::Duration;

use cymule_core::Machine;
use cymule_durable::{DurableError, DurableState, DurableStore};
use cymule_store_sqlite::SqliteStore;
use rusqlite::Connection;
use tempfile::tempdir;

fn state() -> DurableState {
    DurableState::new(Machine::new().snapshot())
}

#[test]
fn sqlite_store_reopens_replays_and_rejects_stale_writers() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("cymule.sqlite");
    let mut first = SqliteStore::open(&path, "domain:one").expect("store opens");
    let mut stale = SqliteStore::open(&path, "domain:one").expect("second store opens");
    assert!(first.load().expect("empty store loads").is_none());
    let candidate_state = state();
    let receipt = first
        .compare_and_swap(None, &candidate_state)
        .expect("initial state commits");
    let reopened = stale
        .load()
        .expect("committed state reopens")
        .expect("state");
    assert_eq!(reopened.revision, receipt.revision);
    assert_eq!(reopened.state, candidate_state);
    assert!(matches!(
        stale.compare_and_swap(None, &candidate_state),
        Err(DurableError::Conflict { current: Some(current), .. }) if current == receipt.revision
    ));
}

#[test]
fn separate_domains_do_not_share_authority() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("domains.sqlite");
    let mut first = SqliteStore::open(&path, "domain:first").expect("first opens");
    let mut second = SqliteStore::open(&path, "domain:second").expect("second opens");
    first
        .compare_and_swap(None, &state())
        .expect("first commits");
    assert!(second.load().expect("second loads").is_none());
}

#[test]
fn active_sqlite_writer_returns_immediately_as_conflict() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("contention.sqlite");
    let mut store = SqliteStore::open(&path, "domain:one").expect("store opens");
    let blocker = Connection::open(&path).expect("blocking connection opens");
    blocker
        .busy_timeout(Duration::ZERO)
        .expect("zero timeout configures");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("writer transaction starts");
    assert!(matches!(
        store.compare_and_swap(None, &state()),
        Err(DurableError::Conflict { current: Some(current), .. })
            if current == "sqlite-writer-active"
    ));
    blocker.execute_batch("ROLLBACK").expect("writer releases");
}

#[test]
fn read_only_observer_neither_configures_nor_contends_as_a_writer() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("observer.sqlite");
    let candidate = state();
    let mut writer = SqliteStore::open(&path, "domain:one").expect("writer opens");
    let commit = writer
        .compare_and_swap(None, &candidate)
        .expect("initial state commits");
    let blocker = Connection::open(&path).expect("blocking connection opens");
    blocker
        .busy_timeout(Duration::ZERO)
        .expect("zero timeout configures");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("writer transaction starts");

    let mut observer =
        SqliteStore::open_read_only(&path, "domain:one").expect("observer opens read-only");
    let retained = observer
        .load()
        .expect("observer reads through active writer")
        .expect("committed state exists");
    assert_eq!(retained.revision, commit.revision);
    assert_eq!(retained.state, candidate);
    assert!(matches!(
        observer.compare_and_swap(Some(&commit.revision), &state()),
        Err(DurableError::Validation(message)) if message.contains("read-only")
    ));
    blocker.execute_batch("ROLLBACK").expect("writer releases");
}

#[test]
fn corrupted_state_fails_closed() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("corrupt.sqlite");
    let mut store = SqliteStore::open(&path, "domain:one").expect("store opens");
    store
        .compare_and_swap(None, &state())
        .expect("initial state commits");
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "UPDATE cymule_state SET state_json = ?1 WHERE domain = ?2",
            (b"{}".as_slice(), "domain:one"),
        )
        .expect("test corrupts state");
    assert!(matches!(store.load(), Err(DurableError::Encoding(_))));
}
