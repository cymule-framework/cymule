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
    let wait_id = "wait:timer";
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
            wait_set: BTreeSet::from([wait_id.to_owned()]),
            scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
            epoch: 0,
            execution_fence: 0,
            execution_claim: None,
            status: ContinuationStatus::Waiting,
        },
    );
    state.waits.insert(
        wait_id.to_owned(),
        WaitCondition {
            wait_id: wait_id.to_owned(),
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

#[derive(Default)]
struct ObservedTimerView {
    waits: ParkedWaitIndex,
    selection_error: Option<DurableError>,
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
fn malformed_timer_generation_is_rejected_without_mutation() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("legacy-timer.sqlite");
    let connection = rusqlite::Connection::open(&database).expect("legacy timer store opens");
    connection
        .execute_batch(
            "CREATE TABLE cymule_timers (
                activation_id TEXT PRIMARY KEY NOT NULL,
                timer_id TEXT NOT NULL,
                due_unix_ms INTEGER NOT NULL,
                value_json BLOB NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0
             ) STRICT;
             INSERT INTO cymule_timers(
                activation_id, timer_id, due_unix_ms, value_json, acknowledged
             ) VALUES ('activation:legacy', 'timer:legacy', 100, X'74727565', 0);",
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
    let meta_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'cymule_timer_meta'",
            [],
            |row| row.get(0),
        )
        .expect("schema remains inspectable");
    assert_eq!(meta_count, 0, "rejection must not heal the generation");
}
