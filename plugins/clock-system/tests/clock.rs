//! Restart, wall-clock regression, integrity, and contention tests.

use cymule_clock_system::{SqliteClock, WallClock};
use cymule_core::MAX_EXACT_INTEGER;
use cymule_durable::{
    ClockObservationAuthority, DurableError, DurableResult, ExecutionClockAuthority,
};
use cymule_durable_protocol::{ClockObservation, ClockObservationRef};
use rusqlite::Connection;
use std::fs;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[derive(Clone, Copy)]
struct FixedWall(u64);

const GENERATION: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

impl WallClock for FixedWall {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        Ok(self.0)
    }
}

fn probe_current_head(
    clock: &mut impl ExecutionClockAuthority,
    reference: &ClockObservationRef,
) -> DurableResult<ClockObservation> {
    let mut resolved = None;
    let mut no_store_mutation = |observation: &ClockObservation| {
        resolved = Some(observation.clone());
        Ok(())
    };
    clock.with_current_head(reference, &mut no_store_mutation)?;
    resolved.ok_or_else(|| DurableError::Validation("Clock guard skipped its callback".to_owned()))
}

#[test]
fn logical_time_advances_across_reopen_and_backward_wall_time() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let first =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock opens")
            .observe("leases")
            .expect("first observation allocates");
    assert_eq!(first.logical_time, 100);

    let second =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(90))
            .expect("clock reopens")
            .observe("leases")
            .expect("second observation allocates");
    assert_eq!(second.logical_time, 101);
    assert_eq!(second.observed_unix_ms, 90);
    second.verify().expect("observation verifies");

    let other = SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(7))
        .expect("clock reopens")
        .observe("timers")
        .expect("independent scope allocates");
    assert_eq!(other.logical_time, 7);
}

#[test]
fn process_local_sqlite_backends_are_rejected_before_clock_schema_mutation() {
    for backend in [":memory:", ""] {
        let probe = Connection::open(backend).expect("process-local database opens");
        assert_eq!(sqlite_main_filename(&probe), "");
        assert_eq!(sqlite_object_count(&probe), 0);
        assert!(matches!(
            SqliteClock::open_with_wall_clock(
                backend,
                "clock:one",
                GENERATION,
                FixedWall(100),
            ),
            Err(DurableError::Validation(message))
                if message == "Clock SQLite authority must be file-backed"
        ));
        assert_eq!(sqlite_object_count(&probe), 0);
    }

    let memory_uri = format!(
        "file:clock-authority-negative-{}?mode=memory&cache=shared",
        std::process::id()
    );
    let keeper = Connection::open(&memory_uri).expect("shared memory database opens");
    assert_eq!(sqlite_main_filename(&keeper), "");
    assert_eq!(sqlite_object_count(&keeper), 0);
    assert!(matches!(
        SqliteClock::open_with_wall_clock(
            &memory_uri,
            "clock:one",
            GENERATION,
            FixedWall(100),
        ),
        Err(DurableError::Validation(message))
            if message == "Clock SQLite authority must be file-backed"
    ));
    assert_eq!(sqlite_object_count(&keeper), 0);
    assert!(!std::path::Path::new(&memory_uri).exists());
}

#[test]
fn file_backend_detection_does_not_blacklist_memory_like_filenames() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock-mode=memory.sqlite");
    let observation =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("file-backed Clock opens")
            .observe("leases")
            .expect("file-backed Clock observes");
    assert_eq!(observation.logical_time, 100);
    assert!(database.is_file());
}

#[test]
fn out_of_range_wall_observation_changes_no_clock_state() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock = SqliteClock::open_with_wall_clock(
        &database,
        "clock:one",
        GENERATION,
        FixedWall(MAX_EXACT_INTEGER + 1),
    )
    .expect("file-backed Clock opens");
    assert!(matches!(
        clock.observe("leases"),
        Err(DurableError::Validation(message))
            if message == "Clock observation exceeds the exact cross-language integer range"
    ));
    drop(clock);

    let first = SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(7))
        .expect("Clock reopens after rejected observation")
        .observe("leases")
        .expect("first valid observation commits");
    assert_eq!(first.logical_time, 7);
}

#[test]
fn tampered_observation_and_active_writer_fail_closed() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock opens");
    let mut observation: ClockObservation = clock.observe("leases").expect("observes");
    observation.logical_time += 1;
    assert!(observation.verify().is_err());

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

#[test]
fn retained_clock_integer_corruption_is_integrity_not_caller_validation() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock opens");
    let invalid_head = clock.observe("invalid-head").expect("head fixture issues");
    let invalid_receipt_logical = clock
        .observe("invalid-receipt-logical")
        .expect("logical receipt fixture issues");
    let invalid_receipt_wall = clock
        .observe("invalid-receipt-wall")
        .expect("wall receipt fixture issues");
    let invalid_current_logical = clock
        .observe("invalid-current-logical")
        .expect("current logical fixture issues");
    let invalid_current_wall = clock
        .observe("invalid-current-wall")
        .expect("current wall fixture issues");
    let connection = Connection::open(&database).expect("tamper connection opens");
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .expect("test enables direct corruption");

    assert_allocated_head_corruption_is_integrity(&connection, &mut clock, &invalid_head);
    assert_observation_corruption_is_integrity(
        &connection,
        &mut clock,
        &invalid_receipt_logical,
        &invalid_receipt_wall,
    );
    assert_current_head_corruption_is_integrity(
        &connection,
        &mut clock,
        &invalid_current_logical,
        &invalid_current_wall,
    );
}

#[derive(Clone, Copy)]
enum AllocationCorruptionTarget {
    HeadLogical,
    HeadWall,
    ReceiptLogical,
    ReceiptWall,
}

type AllocationCorruptionCase = (&'static str, AllocationCorruptionTarget, i64, &'static str);

#[test]
fn allocation_rejects_corrupt_head_and_exact_receipt_before_overwrite() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock opens");
    let connection = Connection::open(&database).expect("tamper connection opens");
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .expect("test enables direct corruption");

    for (scope, target, value, expected_code) in allocation_corruption_cases() {
        let issued = clock.observe(scope).expect("corruption fixture issues");
        let (table, column, selector, selector_value) = match target {
            AllocationCorruptionTarget::HeadLogical => {
                ("cymule_clock_scopes_v2", "logical_time", "scope", scope)
            }
            AllocationCorruptionTarget::HeadWall => {
                ("cymule_clock_scopes_v2", "observed_unix_ms", "scope", scope)
            }
            AllocationCorruptionTarget::ReceiptLogical => (
                "cymule_clock_observations_v2",
                "logical_time",
                "observation_id",
                issued.observation_id.as_str(),
            ),
            AllocationCorruptionTarget::ReceiptWall => (
                "cymule_clock_observations_v2",
                "observed_unix_ms",
                "observation_id",
                issued.observation_id.as_str(),
            ),
        };
        connection
            .execute(
                &format!("UPDATE {table} SET {column} = ?1 WHERE {selector} = ?2"),
                rusqlite::params![value, selector_value],
            )
            .expect("test corrupts retained Clock authority");
        let before = retained_scope_and_receipt_values(&connection, &issued);

        let error = clock
            .observe(scope)
            .expect_err("allocation must reject corrupt retained authority");
        assert!(
            matches!(error, DurableError::Integrity { ref code, .. } if code == expected_code),
            "unexpected allocation error for {scope}: {error:?}"
        );
        assert_eq!(
            retained_scope_and_receipt_values(&connection, &issued),
            before
        );
    }
}

fn allocation_corruption_cases() -> [AllocationCorruptionCase; 8] {
    use AllocationCorruptionTarget::{HeadLogical, HeadWall, ReceiptLogical, ReceiptWall};

    [
        (
            "allocation-head-logical-negative",
            HeadLogical,
            -1,
            "clock_scope_head_logical_time_invalid",
        ),
        (
            "allocation-head-logical-above-exact",
            HeadLogical,
            above_exact_integer(),
            "clock_scope_head_logical_time_invalid",
        ),
        (
            "allocation-head-wall-negative",
            HeadWall,
            -1,
            "clock_scope_head_wall_time_invalid",
        ),
        (
            "allocation-head-wall-above-exact",
            HeadWall,
            above_exact_integer(),
            "clock_scope_head_wall_time_invalid",
        ),
        (
            "allocation-receipt-logical-negative",
            ReceiptLogical,
            -1,
            "clock_observation_logical_time_invalid",
        ),
        (
            "allocation-receipt-logical-above-exact",
            ReceiptLogical,
            above_exact_integer(),
            "clock_observation_logical_time_invalid",
        ),
        (
            "allocation-receipt-wall-negative",
            ReceiptWall,
            -1,
            "clock_observation_wall_time_invalid",
        ),
        (
            "allocation-receipt-wall-above-exact",
            ReceiptWall,
            above_exact_integer(),
            "clock_observation_wall_time_invalid",
        ),
    ]
}

fn retained_scope_and_receipt_values(
    connection: &Connection,
    observation: &ClockObservation,
) -> ((i64, i64), (i64, i64)) {
    let head = connection
        .query_row(
            "SELECT logical_time, observed_unix_ms FROM cymule_clock_scopes_v2
             WHERE source_id = ?1 AND source_generation = ?2 AND scope = ?3",
            rusqlite::params![
                &observation.source_id,
                &observation.source_generation,
                &observation.scope
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("retained scope head reads");
    let receipt = connection
        .query_row(
            "SELECT logical_time, observed_unix_ms FROM cymule_clock_observations_v2
             WHERE observation_id = ?1",
            [&observation.observation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("retained Clock receipt reads");
    (head, receipt)
}

fn assert_allocated_head_corruption_is_integrity(
    connection: &Connection,
    clock: &mut SqliteClock<FixedWall>,
    observation: &ClockObservation,
) {
    connection
        .execute(
            "UPDATE cymule_clock_scopes_v2 SET logical_time = -1
             WHERE source_id = ?1 AND source_generation = ?2 AND scope = ?3",
            rusqlite::params![
                &observation.source_id,
                &observation.source_generation,
                &observation.scope
            ],
        )
        .expect("test corrupts retained head");
    assert!(matches!(
        clock.observe(&observation.scope),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_scope_head_logical_time_invalid"
    ));
}

fn assert_observation_corruption_is_integrity(
    connection: &Connection,
    clock: &mut SqliteClock<FixedWall>,
    logical: &ClockObservation,
    wall: &ClockObservation,
) {
    connection
        .execute(
            "UPDATE cymule_clock_observations_v2 SET logical_time = -1
             WHERE observation_id = ?1",
            [&logical.observation_id],
        )
        .expect("test corrupts retained receipt logical time");
    assert!(matches!(
        clock.resolve(&logical.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_observation_logical_time_invalid"
    ));
    connection
        .execute(
            "UPDATE cymule_clock_observations_v2 SET logical_time = ?1
             WHERE observation_id = ?2",
            rusqlite::params![above_exact_integer(), &logical.observation_id],
        )
        .expect("test corrupts retained receipt above the exact logical range");
    assert!(matches!(
        clock.resolve(&logical.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_observation_logical_time_invalid"
    ));

    connection
        .execute(
            "UPDATE cymule_clock_observations_v2 SET observed_unix_ms = -1
             WHERE observation_id = ?1",
            [&wall.observation_id],
        )
        .expect("test corrupts retained receipt wall time");
    assert!(matches!(
        clock.resolve(&wall.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_observation_wall_time_invalid"
    ));
    connection
        .execute(
            "UPDATE cymule_clock_observations_v2 SET observed_unix_ms = ?1
             WHERE observation_id = ?2",
            rusqlite::params![above_exact_integer(), &wall.observation_id],
        )
        .expect("test corrupts retained receipt above the exact wall range");
    assert!(matches!(
        clock.resolve(&wall.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_observation_wall_time_invalid"
    ));
}

fn assert_current_head_corruption_is_integrity(
    connection: &Connection,
    clock: &mut SqliteClock<FixedWall>,
    logical: &ClockObservation,
    wall: &ClockObservation,
) {
    corrupt_current_head(
        connection,
        logical,
        -1,
        100,
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    assert!(matches!(
        probe_current_head(clock, &logical.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_scope_head_logical_time_invalid"
    ));
    corrupt_current_head(
        connection,
        logical,
        above_exact_integer(),
        100,
        "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    );
    assert!(matches!(
        probe_current_head(clock, &logical.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_scope_head_logical_time_invalid"
    ));

    corrupt_current_head(
        connection,
        wall,
        101,
        -1,
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    );
    assert!(matches!(
        probe_current_head(clock, &wall.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_scope_head_wall_time_invalid"
    ));
    corrupt_current_head(
        connection,
        wall,
        102,
        above_exact_integer(),
        "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    );
    assert!(matches!(
        probe_current_head(clock, &wall.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_scope_head_wall_time_invalid"
    ));
}

fn above_exact_integer() -> i64 {
    i64::try_from(MAX_EXACT_INTEGER + 1).expect("upper corrupt value fits SQLite")
}

fn corrupt_current_head(
    connection: &Connection,
    observation: &ClockObservation,
    logical_time: i64,
    observed_unix_ms: i64,
    receipt_id: &str,
) {
    connection
        .execute(
            "UPDATE cymule_clock_scopes_v2
             SET logical_time = ?1, observed_unix_ms = ?2
             WHERE source_id = ?3 AND source_generation = ?4 AND scope = ?5",
            rusqlite::params![
                logical_time,
                observed_unix_ms,
                &observation.source_id,
                &observation.source_generation,
                &observation.scope
            ],
        )
        .expect("test corrupts retained current head");
    connection
        .execute(
            "INSERT INTO cymule_clock_observations_v2(
                observation_id, source_id, source_generation, scope,
                logical_time, observed_unix_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                receipt_id,
                &observation.source_id,
                &observation.source_generation,
                &observation.scope,
                logical_time,
                observed_unix_ms
            ],
        )
        .expect("test inserts matching corrupt head receipt");
}

#[test]
fn open_preflight_uses_zero_busy_timeout_and_reports_conflict() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock-open-contention.sqlite");
    drop(
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock schema initializes"),
    );
    let blocker = Connection::open(&database).expect("blocking connection opens");
    blocker
        .pragma_update(None, "journal_mode", "DELETE")
        .expect("rollback journal mode enables an exclusive read blocker");
    blocker
        .execute_batch("BEGIN EXCLUSIVE")
        .expect("exclusive transaction begins");

    let started = Instant::now();
    let result =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(101));
    let elapsed = started.elapsed();
    assert!(matches!(result, Err(DurableError::Conflict { .. })));
    assert!(
        elapsed < Duration::from_millis(500),
        "Clock open waited {elapsed:?} instead of surfacing contention immediately"
    );
    blocker
        .execute_batch("ROLLBACK")
        .expect("exclusive transaction rolls back");
}

#[test]
fn resolve_requires_the_exact_issued_source_generation_and_content() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock opens");
    let observation = clock.observe("leases").expect("observation issues");
    assert_eq!(
        clock
            .resolve(&observation.reference())
            .expect("issued receipt resolves"),
        observation
    );

    let mut unissued = observation.reference();
    unissued.observation_id =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    assert!(matches!(
        clock.resolve(&unissued),
        Err(DurableError::NotFound(_))
    ));

    let other_generation =
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let mut other =
        SqliteClock::open_with_wall_clock(&database, "clock:one", other_generation, FixedWall(100))
            .expect("other generation opens");
    assert!(matches!(
        other.resolve(&observation.reference()),
        Err(DurableError::Validation(_))
    ));

    Connection::open(&database)
        .expect("tamper connection opens")
        .execute(
            "UPDATE cymule_clock_observations_v2 SET logical_time = logical_time + 1
             WHERE observation_id = ?1",
            [&observation.observation_id],
        )
        .expect("test corrupts retained receipt");
    assert!(matches!(
        clock.resolve(&observation.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_observation_receipt_invalid"
    ));
}

#[test]
fn execution_head_rejects_an_older_receipt_without_removing_exact_replay() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock opens");
    let older = clock.observe("leases").expect("older observation issues");
    let current = clock.observe("leases").expect("current observation issues");

    assert_eq!(
        clock
            .resolve(&older.reference())
            .expect("historical exact receipt remains replayable"),
        older
    );
    assert!(matches!(
        probe_current_head(&mut clock, &older.reference()),
        Err(DurableError::Conflict {
            expected: Some(expected),
            current: Some(observed),
        }) if expected == older.observation_id && observed == current.observation_id
    ));
    assert_eq!(
        probe_current_head(&mut clock, &current.reference())
            .expect("latest scope receipt is execution-current"),
        current
    );

    Connection::open(&database)
        .expect("tamper connection opens")
        .execute(
            "UPDATE cymule_clock_scopes_v2 SET observed_unix_ms = observed_unix_ms + 1
             WHERE source_id = ?1 AND source_generation = ?2 AND scope = ?3",
            rusqlite::params![
                &current.source_id,
                &current.source_generation,
                &current.scope
            ],
        )
        .expect("test corrupts the scope head");
    assert!(matches!(
        probe_current_head(&mut clock, &current.reference()),
        Err(DurableError::Integrity { code, .. })
            if code == "clock_scope_head_receipt_missing"
    ));
}

#[test]
fn scope_head_and_immutable_receipt_commit_in_one_transaction() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let mut clock =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("clock opens");
    let connection = Connection::open(&database).expect("fault connection opens");
    connection
        .execute_batch(
            "CREATE TRIGGER reject_clock_receipt BEFORE INSERT ON cymule_clock_observations_v2
             BEGIN SELECT RAISE(ABORT, 'receipt rejected'); END;",
        )
        .expect("fault trigger installs");
    assert!(matches!(
        clock.observe("leases"),
        Err(DurableError::Substrate { .. })
    ));
    let retained_heads: i64 = connection
        .query_row("SELECT COUNT(*) FROM cymule_clock_scopes_v2", [], |row| {
            row.get(0)
        })
        .expect("scope count reads");
    assert_eq!(retained_heads, 0);
    connection
        .execute_batch("DROP TRIGGER reject_clock_receipt")
        .expect("fault trigger removes");
    let observation = clock
        .observe("leases")
        .expect("observation succeeds after rollback");
    assert_eq!(observation.logical_time, 100);

    assert!(
        connection
            .execute(
                "UPDATE cymule_clock_scopes_v2 SET logical_time = -1 WHERE scope = 'leases'",
                [],
            )
            .is_err()
    );
    assert_eq!(
        clock
            .observe("leases")
            .expect("constraint failure did not alter the Clock")
            .logical_time,
        101
    );
}

#[test]
fn current_head_guard_excludes_interleaved_observe_through_store_commit() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("clock.sqlite");
    let scope = "execution:guarded-store-cas";
    let mut authority =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(100))
            .expect("authority Clock opens");
    let head = authority.observe(scope).expect("execution head issues");
    let mut contender =
        SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(101))
            .expect("contending Clock opens before the guard");

    let mut store_commits = 0;
    let mut commit_store_cas = |resolved: &ClockObservation| {
        assert_eq!(resolved, &head);
        assert!(matches!(
            contender.observe(scope),
            Err(DurableError::Conflict { .. })
        ));
        store_commits += 1;
        Ok(())
    };
    authority
        .with_current_head(&head.reference(), &mut commit_store_cas)
        .expect("head remains guarded through the Store CAS");
    assert_eq!(store_commits, 1);

    let advanced = contender
        .observe(scope)
        .expect("head advances after the guard releases");
    assert!(advanced.logical_time > head.logical_time);
    let mut stale_store_commits = 0;
    let mut stale_store_cas = |_: &ClockObservation| {
        stale_store_commits += 1;
        Ok(())
    };
    assert!(matches!(
        authority.with_current_head(&head.reference(), &mut stale_store_cas),
        Err(DurableError::Conflict {
            expected: Some(expected),
            current: Some(current),
        }) if expected == head.observation_id && current == advanced.observation_id
    ));
    assert_eq!(stale_store_commits, 0);
}

#[test]
fn partial_wrong_or_extended_clock_schema_is_never_repaired() {
    let partial = tempdir().expect("partial directory creates");
    let partial_database = partial.path().join("clock.sqlite");
    Connection::open(&partial_database)
        .expect("partial database opens")
        .execute_batch(
            "CREATE TABLE cymule_clock_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                schema_version TEXT NOT NULL
             ) STRICT;
             INSERT INTO cymule_clock_meta VALUES (1, 'cymule.clock-system/2');",
        )
        .expect("partial schema creates");
    assert!(matches!(
        SqliteClock::open_with_wall_clock(&partial_database, "clock:one", GENERATION, FixedWall(1),),
        Err(DurableError::Validation(_))
    ));
    let connection = Connection::open(&partial_database).expect("partial database reopens");
    let scope_exists: i64 = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'cymule_clock_scopes_v2')",
            [],
            |row| row.get(0),
        )
        .expect("partial schema reads");
    assert_eq!(scope_exists, 0);

    for mutation in [
        "ALTER TABLE cymule_clock_scopes_v2 ADD COLUMN extra TEXT",
        "CREATE INDEX unexpected_clock_index ON cymule_clock_scopes_v2(scope)",
    ] {
        let directory = tempdir().expect("wrong-shape directory creates");
        let database = directory.path().join("clock.sqlite");
        drop(
            SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(1))
                .expect("exact schema initializes"),
        );
        Connection::open(&database)
            .expect("mutation connection opens")
            .execute_batch(mutation)
            .expect("test mutates schema");
        assert!(matches!(
            SqliteClock::open_with_wall_clock(&database, "clock:one", GENERATION, FixedWall(2),),
            Err(DurableError::Validation(_))
        ));
    }
}

#[test]
fn foreign_cymule_database_is_rejected_before_clock_schema_or_file_mutation() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("foreign.sqlite");
    Connection::open(&database)
        .expect("foreign database opens")
        .execute_batch(
            "CREATE TABLE cymule_foreign_authority (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                authority TEXT NOT NULL
             ) STRICT;
             INSERT INTO cymule_foreign_authority VALUES (1, 'foreign');",
        )
        .expect("foreign Cymule authority initializes");

    let before_objects = sqlite_objects(&database);
    let before_bytes = fs::read(&database).expect("foreign database bytes read");
    let Err(error) = SqliteClock::open_with_wall_clock(
        &database,
        "clock:must-not-cohabit",
        GENERATION,
        FixedWall(1),
    ) else {
        panic!("Clock must reject a foreign Cymule database");
    };
    assert!(matches!(error, DurableError::Validation(_)));

    let after_bytes = fs::read(&database).expect("rejected database bytes read");
    let after_objects = sqlite_objects(&database);
    assert_eq!(after_bytes, before_bytes);
    assert_eq!(after_objects, before_objects);
    assert!(
        before_objects
            .iter()
            .all(|(_, name, table)| !name.starts_with("cymule_clock_")
                && !table.starts_with("cymule_clock_"))
    );
}

fn sqlite_objects(path: &std::path::Path) -> Vec<(String, String, String)> {
    let connection = Connection::open(path).expect("database observer opens");
    let mut statement = connection
        .prepare("SELECT type, name, tbl_name FROM sqlite_master ORDER BY type, name")
        .expect("database schema query prepares");
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .expect("database schema query runs")
        .collect::<Result<Vec<_>, _>>()
        .expect("database schema rows decode")
}

fn sqlite_object_count(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))
        .expect("database schema count reads")
}

fn sqlite_main_filename(connection: &Connection) -> String {
    let mut statement = connection
        .prepare("PRAGMA database_list")
        .expect("database list prepares");
    let mut rows = statement.query([]).expect("database list reads");
    while let Some(row) = rows.next().expect("database list advances") {
        if row.get::<_, String>(1).expect("database name decodes") == "main" {
            return row.get(2).expect("database filename decodes");
        }
    }
    panic!("SQLite main database is absent")
}
