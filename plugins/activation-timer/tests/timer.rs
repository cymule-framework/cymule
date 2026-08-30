//! Deterministic timer due-time, redelivery, and acknowledgement tests.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::{Arc, Barrier};

use cymule_activation_timer::{
    Clock, SqliteTimerDriver, TIMER_STORE_SCHEMA_VERSION, UNSUPPORTED_STORE_GENERATION_CODE,
};
use cymule_core::{ArtifactRef, Machine, ROOT_SCOPE_ID};
use cymule_durable::{
    DurableError, DurableResult, DurableState, ParkedWaitIndex, ParkedWaitView,
    SignalKeyPageOutcome, WaitCondition, WaitKind, WaitSelection, WaitSourceCursor,
    WaitSourceDriver, WaitState,
};
use cymule_durable_protocol::{
    CONTINUATION_STATE_VERSION, Continuation, ContinuationStatus, FrameState, WaitActivationSource,
    WaitOwner,
};
use serde_json::json;
use tempfile::tempdir;

#[derive(Clone, Copy)]
struct ManualClock(u64);

impl Clock for ManualClock {
    fn now_unix_ms(&self) -> cymule_durable::DurableResult<u64> {
        Ok(self.0)
    }
}

fn index() -> ParkedWaitIndex {
    index_for_timer("timer:one")
}

fn index_for_timer(timer_id: &str) -> ParkedWaitIndex {
    let wait_id = test_wait_id(timer_id);
    let mut state = DurableState::new(Machine::new().snapshot());
    state.continuations.insert(
        "run:timer".to_owned(),
        Continuation {
            continuation_version: CONTINUATION_STATE_VERSION.to_owned(),
            run_id: "run:timer".to_owned(),
            plan_id: "sha256:plan".to_owned(),
            binding_context: "binding:test".to_owned(),
            frames: vec![FrameState {
                definition_id: "main".to_owned(),
                invocation_id: "main".to_owned(),
                invocation_path: Vec::new(),
                scope_id: ROOT_SCOPE_ID.to_owned(),
                input: ArtifactRef {
                    identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
                    artifact_id: format!("sha256:{}", "0".repeat(64)),
                    kind: "test/input".to_owned(),
                },
                region_path: Vec::new(),
                next_step: 0,
                locals: BTreeMap::new(),
            }],
            state: None,
            wait_set: BTreeSet::from([wait_id.clone()]),
            scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
            epoch: 0,
            execution_fence: 0,
            execution_claim: None,
            status: ContinuationStatus::Waiting,
        },
    );
    state.waits.insert(
        wait_id.clone(),
        WaitCondition {
            wait_id,
            run_id: "run:timer".to_owned(),
            kind: WaitKind::Timer {
                timer_id: timer_id.to_owned(),
            },
            consume_once: false,
            owner: WaitOwner {
                invocation_id: "main".to_owned(),
                definition_id: "main".to_owned(),
                site_id: "wait.timer".to_owned(),
                region_path: Vec::new(),
                step_index: 0,
                bind: None,
            },
            state: WaitState::Pending,
            result: None,
        },
    );
    ParkedWaitIndex::rebuild(&state).expect("index rebuilds")
}

fn test_wait_id(label: &str) -> String {
    cymule_core::content_id("cymule.test.timer-wait/1", &label).expect("wait ID derives")
}

#[derive(Default)]
struct ObservedTimerView {
    waits: ParkedWaitIndex,
    selection_error: Option<DurableError>,
    selection_override: Option<WaitSelection>,
    selections: Vec<(WaitActivationSource, usize)>,
}

impl ParkedWaitView for ObservedTimerView {
    fn select(
        &mut self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<WaitSelection> {
        self.selections.push((source.clone(), max_targets));
        if let Some(error) = self.selection_error.take() {
            return Err(error);
        }
        if let Some(selection) = self.selection_override.take() {
            return Ok(selection);
        }
        self.waits.select(source, max_targets)
    }

    fn signal_key_page(
        &mut self,
        _cursor: Option<&WaitSourceCursor>,
        _limit: usize,
    ) -> DurableResult<SignalKeyPageOutcome> {
        panic!("timer selection must never enumerate signal sources");
    }
}

fn timer_driver(clock: ManualClock) -> (tempfile::TempDir, SqliteTimerDriver<ManualClock>) {
    let directory = tempdir().expect("temporary directory creates");
    let driver = SqliteTimerDriver::open_with_clock(directory.path().join("timer.sqlite"), clock)
        .expect("driver opens");
    (directory, driver)
}

#[test]
fn due_timer_redelivers_until_acknowledged() {
    let (_directory, mut driver) = timer_driver(ManualClock(100));
    driver
        .schedule("activation:one", "timer:one", 100, &json!({"due": true}))
        .expect("timer schedules");
    driver
        .schedule("activation:one", "timer:one", 100, &json!({"due": true}))
        .expect("schedule replay is idempotent");
    let first = driver
        .receive(&mut index(), 1)
        .expect("timer receives")
        .expect("delivery exists");
    assert_eq!(first.activation_id, "activation:one");
    let first = first.into_delivery();
    assert_eq!(
        driver
            .receive(&mut index(), 1)
            .expect("redelivers")
            .map(cymule_durable::WaitSourceDelivery::into_delivery),
        Some(first)
    );
    driver.acknowledge("activation:one").expect("acknowledges");
    driver
        .acknowledge("activation:one")
        .expect("ack replay succeeds");
    driver
        .schedule("activation:one", "timer:one", 100, &json!({"due": true}))
        .expect("schedule replay after ack succeeds");
    for (timer_id, due, value) in [
        ("timer:other", 100, json!({"due": true})),
        ("timer:one", 101, json!({"due": true})),
        ("timer:one", 100, json!({"due": false})),
    ] {
        assert!(matches!(
            driver.schedule("activation:one", timer_id, due, &value),
            Err(cymule_durable::DurableError::Conflict { .. })
        ));
    }
    assert!(driver.receive(&mut index(), 1).expect("polls").is_none());
}

#[test]
fn concurrent_exact_schedule_is_atomic_and_contention_is_typed() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("concurrent-schedule.sqlite");
    let first = SqliteTimerDriver::open_with_clock(&database, ManualClock(100))
        .expect("first driver opens");
    let second = SqliteTimerDriver::open_with_clock(&database, ManualClock(100))
        .expect("second driver opens");
    let barrier = Arc::new(Barrier::new(2));
    let run = |mut driver: SqliteTimerDriver<ManualClock>, barrier: Arc<Barrier>| {
        barrier.wait();
        driver.schedule(
            "activation:concurrent",
            "timer:concurrent",
            100,
            &json!({"due": true}),
        )
    };
    let first_barrier = Arc::clone(&barrier);
    let first_result = std::thread::spawn(move || run(first, first_barrier));
    let second_result = std::thread::spawn(move || run(second, barrier));
    let outcomes = [
        first_result.join().expect("first scheduler joins"),
        second_result.join().expect("second scheduler joins"),
    ];

    assert!(outcomes.iter().any(Result::is_ok));
    assert!(outcomes.iter().all(|outcome| {
        outcome.is_ok() || matches!(outcome, Err(cymule_durable::DurableError::Conflict { .. }))
    }));

    let mut reopened =
        SqliteTimerDriver::open_with_clock(&database, ManualClock(100)).expect("store reopens");
    reopened
        .schedule(
            "activation:concurrent",
            "timer:concurrent",
            100,
            &json!({"due": true}),
        )
        .expect("exact schedule replay converges");
}

#[test]
fn concurrent_initialization_surfaces_only_typed_contention() {
    for attempt in 0..16 {
        let directory = tempdir().expect("temporary directory creates");
        let database = directory
            .path()
            .join(format!("concurrent-open-{attempt}.sqlite"));
        let barrier = Arc::new(Barrier::new(2));
        let open = |database: std::path::PathBuf, barrier: Arc<Barrier>| {
            barrier.wait();
            SqliteTimerDriver::open_with_clock(database, ManualClock(100)).map(drop)
        };
        let first_database = database.clone();
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || open(first_database, first_barrier));
        let second = std::thread::spawn(move || open(database, barrier));
        let outcomes = [
            first.join().expect("first opener joins"),
            second.join().expect("second opener joins"),
        ];
        assert!(outcomes.iter().any(Result::is_ok));
        assert!(outcomes.iter().all(|outcome| {
            outcome.is_ok() || matches!(outcome, Err(cymule_durable::DurableError::Conflict { .. }))
        }));
    }
}

#[test]
fn timer_before_due_or_without_a_wait_stays_pending() {
    let (directory, mut driver) = timer_driver(ManualClock(99));
    driver
        .schedule("activation:one", "timer:one", 100, &json!(null))
        .expect("timer schedules");
    assert!(driver.receive(&mut index(), 1).expect("polls").is_none());
    drop(driver);

    let mut due_driver =
        SqliteTimerDriver::open_with_clock(directory.path().join("timer.sqlite"), ManualClock(100))
            .expect("due driver reopens");
    let mut view = ObservedTimerView::default();
    assert!(
        due_driver
            .receive(&mut view, 1)
            .expect("unmatched poll succeeds")
            .is_none()
    );
    assert!(matches!(
        due_driver.acknowledge("activation:one"),
        Err(DurableError::Validation(_))
    ));
    view.waits = index();
    assert_eq!(
        due_driver
            .receive(&mut view, 1)
            .expect("matching poll succeeds")
            .expect("selects")
            .activation_id,
        "activation:one"
    );
    assert_eq!(
        view.selections,
        vec![
            (
                WaitActivationSource::Timer {
                    timer_id: "timer:one".to_owned()
                },
                1
            );
            2
        ]
    );
}

#[test]
fn bounded_timer_scan_continues_past_a_large_unmatched_prefix() {
    let (_directory, mut driver) = timer_driver(ManualClock(100));
    for index in 0..257 {
        driver
            .schedule(
                &format!("activation:unmatched:{index:04}"),
                &format!("timer:unmatched:{index:04}"),
                100,
                &json!(null),
            )
            .expect("unmatched timer schedules");
    }
    driver
        .schedule(
            "activation:zzzz-match",
            "timer:match",
            100,
            &json!({"matched": true}),
        )
        .expect("later matching timer schedules");
    let mut view = ObservedTimerView {
        waits: index_for_timer("timer:match"),
        ..ObservedTimerView::default()
    };

    assert!(
        driver
            .receive(&mut view, 1)
            .expect("first bounded scan succeeds")
            .is_none()
    );
    assert_eq!(
        view.selections.len(),
        256,
        "one receive call must enforce the fixed source-scan budget"
    );

    let matched = driver
        .receive(&mut view, 1)
        .expect("continued scan succeeds")
        .expect("later matching timer is eventually selected");
    assert_eq!(matched.activation_id, "activation:zzzz-match");
    assert_eq!(matched.value, json!({"matched": true}));
    assert_eq!(
        view.selections.len(),
        258,
        "the stable continuation visits the remaining unmatched timer and later match"
    );
}

#[test]
fn timer_cannot_acknowledge_before_target_selection() {
    let (_directory, mut driver) = timer_driver(ManualClock(100));
    driver
        .schedule("activation:unselected", "timer:one", 100, &json!(null))
        .expect("timer schedules");
    assert!(matches!(
        driver.acknowledge("activation:unselected"),
        Err(cymule_durable::DurableError::Validation(_))
    ));
    assert_eq!(
        driver
            .receive(&mut index(), 1)
            .expect("unacknowledged timer receives")
            .expect("delivery exists")
            .activation_id,
        "activation:unselected"
    );
}

#[test]
fn timer_identities_use_the_shared_unicode_scalar_boundary() {
    let (_directory, mut driver) = timer_driver(ManualClock(100));
    let maximum = "界".repeat(512);
    driver
        .schedule(&maximum, &maximum, 100, &json!(null))
        .expect("512 Unicode scalar identities are valid");

    let too_long = "界".repeat(513);
    for (activation_id, timer_id) in [
        (too_long.as_str(), "timer:valid"),
        ("activation:valid", too_long.as_str()),
        ("activation:\ninvalid", "timer:valid"),
        ("activation:valid", "timer:\u{7f}invalid"),
    ] {
        assert!(matches!(
            driver.schedule(activation_id, timer_id, 100, &json!(null)),
            Err(cymule_durable::DurableError::Validation(_))
        ));
    }
}

#[test]
fn schedule_enforces_the_canonical_value_limit_before_write_without_blocking_due_work() {
    let (directory, mut driver) = timer_driver(ManualClock(100));
    let exact = serde_json::Value::String("x".repeat(cymule_core::MAX_ARTIFACT_BYTES - 2));
    driver
        .schedule("activation:exact", "timer:exact", 200, &exact)
        .expect("exact-limit value schedules");

    let oversized = serde_json::Value::String("x".repeat(cymule_core::MAX_ARTIFACT_BYTES - 1));
    assert!(matches!(
        driver.schedule("activation:oversized", "timer:oversized", 100, &oversized),
        Err(DurableError::Validation(message))
            if message == format!(
                "timer value has {} canonical bytes; maximum is {}",
                cymule_core::MAX_ARTIFACT_BYTES + 1,
                cymule_core::MAX_ARTIFACT_BYTES
            )
    ));

    driver
        .schedule("activation:due", "timer:one", 100, &json!({"due": true}))
        .expect("later valid timer schedules");
    let connection = rusqlite::Connection::open(directory.path().join("timer.sqlite"))
        .expect("timer store opens for readback");
    let oversized_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM cymule_timers WHERE activation_id = 'activation:oversized'",
            [],
            |row| row.get(0),
        )
        .expect("oversized row absence reads");
    assert_eq!(oversized_rows, 0);
    let exact_bytes: i64 = connection
        .query_row(
            "SELECT length(value_json) FROM cymule_timers WHERE activation_id = 'activation:exact'",
            [],
            |row| row.get(0),
        )
        .expect("exact-limit bytes read");
    assert_eq!(
        usize::try_from(exact_bytes).expect("SQLite length fits usize"),
        cymule_core::MAX_ARTIFACT_BYTES
    );

    assert_eq!(
        driver
            .receive(&mut index(), 1)
            .expect("valid due timer is not blocked")
            .expect("valid due timer selects")
            .activation_id,
        "activation:due"
    );
    driver
        .acknowledge("activation:due")
        .expect("valid due timer acknowledges");
    drop(connection);
    drop(driver);
    let mut reopened =
        SqliteTimerDriver::open_with_clock(directory.path().join("timer.sqlite"), ManualClock(200))
            .expect("timer store reopens at the exact timer due time");
    let exact_delivery = reopened
        .receive(&mut index_for_timer("timer:exact"), 1)
        .expect("exact-limit timer loads")
        .expect("exact-limit timer delivers")
        .into_delivery();
    assert_eq!(exact_delivery.value, exact);
}

#[test]
fn timer_selection_requires_one_content_addressed_wait() {
    for (wait_ids, expected) in [
        (
            BTreeSet::from([format!("sha256:{}", "A".repeat(64))]),
            "lowercase SHA-256 content ID",
        ),
        (
            BTreeSet::from([test_wait_id("first"), test_wait_id("second")]),
            "exactly one target",
        ),
    ] {
        let (directory, mut driver) = timer_driver(ManualClock(100));
        driver
            .schedule("activation:targets", "timer:one", 100, &json!(null))
            .expect("timer schedules");
        let mut view = ObservedTimerView {
            waits: index(),
            selection_override: Some(WaitSelection {
                wait_ids,
                remaining: 0,
            }),
            ..ObservedTimerView::default()
        };
        let error = driver
            .receive(&mut view, 2)
            .expect_err("invalid target set fails");
        assert!(
            matches!(&error, DurableError::Validation(message) if message.contains(expected)),
            "unexpected target validation error: {error:?}"
        );
        let selected: bool = rusqlite::Connection::open(directory.path().join("timer.sqlite"))
            .expect("timer store opens")
            .query_row(
                "SELECT selected_wait_ids IS NOT NULL FROM cymule_timers",
                [],
                |row| row.get(0),
            )
            .expect("selection state reads");
        assert!(!selected, "invalid target set cannot become durable");
    }
}

#[test]
fn retained_timer_target_corruption_is_integrity() {
    for wait_ids in [
        BTreeSet::<String>::new(),
        BTreeSet::from([format!("sha256:{}", "A".repeat(64))]),
        BTreeSet::from([test_wait_id("first"), test_wait_id("second")]),
    ] {
        let (directory, mut driver) = timer_driver(ManualClock(100));
        driver
            .schedule("activation:retained", "timer:one", 100, &json!(null))
            .expect("timer schedules");
        rusqlite::Connection::open(directory.path().join("timer.sqlite"))
            .expect("timer store opens")
            .execute(
                "UPDATE cymule_timers SET selected_wait_ids = ?1",
                [cymule_core::canonical_bytes(&wait_ids).expect("targets encode")],
            )
            .expect("corrupt targets install");
        let mut view = ObservedTimerView::default();
        assert!(matches!(
            driver.receive(&mut view, 2),
            Err(DurableError::Integrity { .. })
        ));
        assert!(view.selections.is_empty());
    }
}

#[test]
fn oversized_generation_two_blobs_are_gated_before_receive_and_acknowledgement() {
    for field in ["value_json", "selected_wait_ids"] {
        let (directory, mut driver) = timer_driver(ManualClock(100));
        driver
            .schedule("activation:oversized-row", "timer:one", 100, &json!(null))
            .expect("timer schedules");
        if field == "value_json" {
            driver
                .receive(&mut index(), 1)
                .expect("timer selects")
                .expect("delivery exists");
        }
        let connection = rusqlite::Connection::open(directory.path().join("timer.sqlite"))
            .expect("timer store opens");
        let oversized = if field == "value_json" {
            i64::try_from(cymule_core::MAX_ARTIFACT_BYTES + 1)
                .expect("artifact bound fits SQLite INTEGER")
        } else {
            76
        };
        connection
            .execute(
                &format!("UPDATE cymule_timers SET {field} = zeroblob(?1)"),
                [oversized],
            )
            .expect("oversized generation-two BLOB installs");

        for error in [
            driver
                .schedule("activation:oversized-row", "timer:one", 100, &json!(null))
                .expect_err("oversized BLOB cannot replay a schedule"),
            driver
                .receive(&mut ObservedTimerView::default(), 1)
                .expect_err("oversized BLOB cannot become a delivery"),
            driver
                .acknowledge("activation:oversized-row")
                .expect_err("oversized BLOB cannot be acknowledged"),
        ] {
            assert!(
                matches!(
                    error,
                    DurableError::Integrity { ref code, .. }
                        if code == "timer_row_blob_too_large"
                ),
                "{field} produced {error:?}"
            );
        }
        let durable: (i64, bool) = connection
            .query_row(
                &format!(
                    "SELECT length({field}), acknowledged FROM cymule_timers
                     WHERE activation_id = 'activation:oversized-row'"
                ),
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("oversized BLOB state reads");
        assert_eq!(durable, (oversized, false));
    }
}

#[test]
fn timer_store_rejects_process_local_sqlite_backends() {
    for path in [std::path::Path::new(":memory:"), std::path::Path::new("")] {
        assert!(matches!(
            SqliteTimerDriver::open_with_clock(path, ManualClock(100)),
            Err(cymule_durable::DurableError::Validation(message))
                if message == "timer SQLite store must be file-backed"
        ));
    }
}

#[test]
fn selected_delivery_survives_reopen_after_the_wait_leaves_the_index() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("timer.sqlite");
    let mut driver =
        SqliteTimerDriver::open_with_clock(&database, ManualClock(100)).expect("driver opens");
    let generation: String = rusqlite::Connection::open(&database)
        .expect("timer store opens")
        .query_row(
            "SELECT schema_version FROM cymule_timer_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .expect("generation reads");
    assert_eq!(generation, TIMER_STORE_SCHEMA_VERSION);
    driver
        .schedule("activation:reopen", "timer:one", 100, &json!({"due": true}))
        .expect("timer schedules");
    let selected = driver
        .receive(&mut index(), 1)
        .expect("timer receives")
        .expect("delivery exists")
        .into_delivery();
    drop(driver);

    let mut unavailable = ObservedTimerView {
        selection_error: Some(DurableError::NotFound("target view unavailable".to_owned())),
        ..ObservedTimerView::default()
    };
    let mut reopened =
        SqliteTimerDriver::open_with_clock(&database, ManualClock(100)).expect("driver reopens");
    assert_eq!(
        reopened
            .receive(&mut unavailable, 1)
            .expect("redelivery reads")
            .map(cymule_durable::WaitSourceDelivery::into_delivery),
        Some(selected),
        "acknowledgement loss must not trigger target reselection"
    );
    assert!(unavailable.selections.is_empty());
    reopened
        .acknowledge("activation:reopen")
        .expect("retained delivery acknowledges");
    assert!(
        reopened
            .receive(&mut unavailable, 1)
            .expect("polls")
            .is_none()
    );
    assert!(unavailable.selections.is_empty());
}

#[test]
fn retained_delivery_precedes_earlier_unselected_timer_source_errors() {
    let (directory, mut driver) = timer_driver(ManualClock(100));
    driver
        .schedule(
            "activation:retained",
            "timer:one",
            100,
            &json!({"retained": true}),
        )
        .expect("timer schedules");
    let retained = driver
        .receive(&mut index(), 1)
        .expect("polls")
        .expect("selects")
        .into_delivery();
    driver
        .schedule(
            "activation:earlier",
            "timer:other",
            99,
            &json!({"earlier": true}),
        )
        .expect("earlier timer schedules");
    drop(driver);

    let database = directory.path().join("timer.sqlite");
    let mut reopened =
        SqliteTimerDriver::open_with_clock(&database, ManualClock(100)).expect("driver reopens");
    let error = DurableError::Integrity {
        code: "target_proof_invalid".to_owned(),
        message: "injected".to_owned(),
    };
    let mut view = ObservedTimerView {
        waits: index_for_timer("timer:other"),
        selection_error: Some(error.clone()),
        ..ObservedTimerView::default()
    };
    assert_eq!(
        reopened
            .receive(&mut view, 1)
            .expect("retained selection bypasses the view")
            .map(cymule_durable::WaitSourceDelivery::into_delivery),
        Some(retained)
    );
    assert!(view.selections.is_empty());
    reopened
        .acknowledge("activation:retained")
        .expect("retained delivery acknowledges");

    assert_eq!(
        reopened.receive(&mut view, 1),
        Err(error),
        "fresh selection must preserve actual view errors"
    );
    let connection = rusqlite::Connection::open(&database).expect("timer store opens for readback");
    let durable_state: (bool, bool) = connection.query_row(
        "SELECT selected_wait_ids IS NULL, acknowledged FROM cymule_timers WHERE activation_id = 'activation:earlier'",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).expect("selection and acknowledgement read");
    assert_eq!(durable_state, (true, false));
    let selected = reopened
        .receive(&mut view, 1)
        .expect("selection recovers")
        .expect("selects");
    assert_eq!(selected.activation_id, "activation:earlier");
    assert_eq!(
        view.selections,
        vec![
            (
                WaitActivationSource::Timer {
                    timer_id: "timer:other".to_owned()
                },
                1
            );
            2
        ]
    );
}

#[test]
fn predecessor_timer_generation_is_rejected_without_mutation() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("legacy-timer.sqlite");
    let connection = rusqlite::Connection::open(&database).expect("legacy timer store opens");
    connection
        .execute_batch(
            "CREATE TABLE cymule_timer_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                schema_version TEXT NOT NULL
             ) STRICT;
             CREATE TABLE cymule_timers (
                activation_id TEXT PRIMARY KEY NOT NULL,
                timer_id TEXT NOT NULL,
                due_unix_ms INTEGER NOT NULL,
                value_json BLOB NOT NULL,
                selected_wait_ids BLOB,
                acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1))
             ) STRICT;
             CREATE INDEX cymule_timers_due
                ON cymule_timers(acknowledged, due_unix_ms, activation_id);
             INSERT INTO cymule_timer_meta(singleton, schema_version)
                VALUES (1, 'cymule.activation-timer-store/1');
             INSERT INTO cymule_timers(
                activation_id, timer_id, due_unix_ms, value_json,
                selected_wait_ids, acknowledged
             ) VALUES ('activation:legacy', 'timer:legacy', 100, X'74727565', NULL, 0);",
        )
        .expect("legacy generation writes");
    drop(connection);
    let before = fs::read(&database).expect("legacy bytes read");

    let Err(error) = SqliteTimerDriver::open_with_clock(&database, ManualClock(100)) else {
        panic!("legacy generation must not open");
    };
    assert!(
        error
            .to_string()
            .contains(UNSUPPORTED_STORE_GENERATION_CODE)
    );
    assert_eq!(
        fs::read(&database).expect("legacy bytes reread"),
        before,
        "generation rejection must not mutate the database"
    );

    let connection =
        rusqlite::Connection::open(&database).expect("legacy timer store reopens for inspection");
    let retained: Vec<u8> = connection
        .query_row(
            "SELECT value_json FROM cymule_timers
             WHERE activation_id = 'activation:legacy'",
            [],
            |row| row.get(0),
        )
        .expect("legacy payload remains");
    assert_eq!(retained, b"true");
    let generation: String = connection
        .query_row(
            "SELECT schema_version FROM cymule_timer_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .expect("predecessor generation remains inspectable");
    assert_eq!(generation, "cymule.activation-timer-store/1");
}

#[test]
fn fresh_and_retained_timer_row_tamper_fails_before_target_selection() {
    for retained in [false, true] {
        for (name, mutation) in [
            (
                "activation",
                "UPDATE cymule_timers SET activation_id = 'activation:tampered'",
            ),
            (
                "timer",
                "UPDATE cymule_timers SET timer_id = 'timer:tampered'",
            ),
            ("due", "UPDATE cymule_timers SET due_unix_ms = 99"),
            (
                "value",
                "UPDATE cymule_timers SET value_json = X'66616c7365'",
            ),
            (
                "noncanonical-value",
                "UPDATE cymule_timers SET value_json = X'2074727565'",
            ),
            (
                "schedule-digest",
                "UPDATE cymule_timers SET schedule_digest = 'sha256:tampered'",
            ),
            (
                "selected-targets",
                "UPDATE cymule_timers
                 SET selected_wait_ids = CAST(' [\"wait:timer\"]' AS BLOB)",
            ),
            (
                "selected-target-identity",
                "UPDATE cymule_timers SET selected_wait_ids = CAST('[\"\"]' AS BLOB)",
            ),
            (
                "acknowledgement",
                "PRAGMA ignore_check_constraints = ON;
                 UPDATE cymule_timers SET acknowledged = 2",
            ),
        ] {
            let directory = tempdir().expect("temporary directory creates");
            let database = directory.path().join(format!(
                "timer-{name}-{}.sqlite",
                if retained { "retained" } else { "fresh" }
            ));
            let mut driver = SqliteTimerDriver::open_with_clock(&database, ManualClock(100))
                .expect("timer driver opens");
            driver
                .schedule("activation:one", "timer:one", 100, &json!({"due": true}))
                .expect("timer schedules");
            if retained {
                driver
                    .receive(&mut index(), 1)
                    .expect("timer selects")
                    .expect("delivery exists");
            }
            rusqlite::Connection::open(&database)
                .expect("timer store opens for tamper")
                .execute_batch(mutation)
                .expect("test tampers one schedule field");

            let mut view = ObservedTimerView {
                waits: index_for_timer("timer:tampered"),
                ..ObservedTimerView::default()
            };
            let error = if name == "acknowledgement" {
                assert!(
                    driver
                        .receive(&mut view, 1)
                        .expect("invalid acknowledgement is not legal pending work")
                        .is_none()
                );
                driver
                    .acknowledge("activation:one")
                    .expect_err("exact acknowledgement read rejects the invalid flag")
            } else {
                driver
                    .receive(&mut view, 1)
                    .expect_err("tampered schedule cannot become a delivery")
            };
            assert!(
                matches!(error, DurableError::Integrity { .. }),
                "{name} {retained:?} produced {error:?}"
            );
            assert!(
                view.selections.is_empty(),
                "{name} {retained:?} reached parked-wait selection"
            );
        }
    }
}
