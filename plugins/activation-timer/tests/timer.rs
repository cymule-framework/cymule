//! Deterministic timer due-time, redelivery, and acknowledgement tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_activation_timer::{Clock, SqliteTimerDriver};
use cymule_core::{ArtifactRef, Machine, ROOT_SCOPE_ID};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableState, FrameState, ParkedWaitIndex, WaitCondition,
    WaitKind, WaitSourceDriver, WaitState,
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
    let wait_id = "wait:timer";
    let mut state = DurableState::new(Machine::new().snapshot());
    state.continuations.insert(
        "run:timer".to_owned(),
        Continuation {
            run_id: "run:timer".to_owned(),
            plan_id: "sha256:plan".to_owned(),
            binding_context: "binding:test".to_owned(),
            frames: vec![FrameState {
                definition_id: "main".to_owned(),
                invocation_id: "main".to_owned(),
                input: ArtifactRef {
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
            effect_obligations: BTreeSet::new(),
            authority_leases: BTreeSet::new(),
            budget: BTreeMap::new(),
            causal_frontier: BTreeSet::new(),
            epoch: 0,
            status: ContinuationStatus::Waiting,
        },
    );
    state.waits.insert(
        wait_id.to_owned(),
        WaitCondition {
            wait_id: wait_id.to_owned(),
            run_id: "run:timer".to_owned(),
            kind: WaitKind::Timer {
                timer_id: "timer:one".to_owned(),
            },
            consume_once: false,
            state: WaitState::Pending,
            result: None,
        },
    );
    ParkedWaitIndex::rebuild(&state).expect("index rebuilds")
}

#[test]
fn due_timer_redelivers_until_acknowledged() {
    let mut driver =
        SqliteTimerDriver::in_memory_with_clock(ManualClock(100)).expect("driver opens");
    driver
        .schedule("activation:one", "timer:one", 100, &json!({"due": true}))
        .expect("timer schedules");
    driver
        .schedule("activation:one", "timer:one", 100, &json!({"due": true}))
        .expect("schedule replay is idempotent");
    let first = driver
        .receive(&index(), 1)
        .expect("timer receives")
        .expect("delivery exists");
    assert_eq!(first.activation_id, "activation:one");
    assert_eq!(
        driver.receive(&index(), 1).expect("redelivers"),
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
    assert!(driver.receive(&index(), 1).expect("polls").is_none());
}

#[test]
fn timer_before_due_or_without_a_wait_stays_pending() {
    let mut driver =
        SqliteTimerDriver::in_memory_with_clock(ManualClock(99)).expect("driver opens");
    driver
        .schedule("activation:one", "timer:one", 100, &json!(null))
        .expect("timer schedules");
    assert!(driver.receive(&index(), 1).expect("polls").is_none());
}

#[test]
fn selected_delivery_survives_reopen_after_the_wait_leaves_the_index() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("timer.sqlite");
    let mut driver =
        SqliteTimerDriver::open_with_clock(&database, ManualClock(100)).expect("driver opens");
    driver
        .schedule("activation:reopen", "timer:one", 100, &json!({"due": true}))
        .expect("timer schedules");
    let selected = driver
        .receive(&index(), 1)
        .expect("timer receives")
        .expect("delivery exists");
    drop(driver);

    let empty = ParkedWaitIndex::rebuild(&DurableState::new(Machine::new().snapshot()))
        .expect("empty index rebuilds");
    let mut reopened =
        SqliteTimerDriver::open_with_clock(&database, ManualClock(100)).expect("driver reopens");
    assert_eq!(
        reopened.receive(&empty, 1).expect("redelivery reads"),
        Some(selected),
        "acknowledgement loss must not trigger target reselection"
    );
    reopened
        .acknowledge("activation:reopen")
        .expect("retained delivery acknowledges");
    assert!(reopened.receive(&empty, 1).expect("polls").is_none());
}
