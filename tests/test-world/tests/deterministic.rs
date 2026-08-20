//! Deterministic test-world component and trace tests.

use std::collections::BTreeMap;

use cymule_test_world::{
    FailureFingerprint, FaultAction, FaultPlan, FaultStep, ManualClock, RecordingObserver,
    ReplaySpec, ScriptedClock, SeededRandom, TRACE_VERSION, TestWorld, TraceCase, requested_seeds,
};
use serde_json::json;

#[test]
fn deterministic_inputs_replay_without_global_state() {
    let mut clock = ManualClock::new(11);
    assert_eq!(clock.advance(7).expect("clock advances"), 18);
    assert!(clock.set(17).is_err());

    let mut scripted = ScriptedClock::new([2, 2, 9]).expect("script validates");
    assert_eq!(scripted.observe().expect("first observation"), 2);
    assert_eq!(scripted.observe().expect("second observation"), 2);
    assert_eq!(scripted.observe().expect("third observation"), 9);
    assert!(scripted.observe().is_err());

    let mut left = SeededRandom::new(42);
    let mut right = SeededRandom::new(42);
    assert_eq!(
        (0..32).map(|_| left.next_u64()).collect::<Vec<_>>(),
        (0..32).map(|_| right.next_u64()).collect::<Vec<_>>()
    );
}

#[test]
fn fault_and_observation_records_are_explicit_owned_values() {
    let plan = FaultPlan::new(vec![
        FaultStep {
            operation: "durable.cas".to_owned(),
            path: Vec::new(),
            occurrence: 2,
            action: FaultAction::ErrorBefore,
        },
        FaultStep {
            operation: "durable.cas".to_owned(),
            path: Vec::new(),
            occurrence: 4,
            action: FaultAction::AcknowledgementLostAfter,
        },
    ])
    .expect("fault plan validates");
    let mut world = TestWorld::with_faults(91, plan).expect("world creates");
    assert_eq!(world.faults_mut().observe("durable.cas"), None);
    assert_eq!(
        world.faults_mut().observe("durable.cas"),
        Some(FaultAction::ErrorBefore)
    );
    assert_eq!(world.faults_mut().observe("durable.cas"), None);
    assert_eq!(
        world.faults_mut().observe("durable.cas"),
        Some(FaultAction::AcknowledgementLostAfter)
    );

    let path_plan = FaultPlan::new(vec![FaultStep {
        operation: "durable.cas".to_owned(),
        path: vec![7],
        occurrence: 1,
        action: FaultAction::ErrorBefore,
    }])
    .expect("path fault validates");
    let mut path_schedule = path_plan.schedule();
    assert_eq!(path_schedule.observe_path("durable.cas", &[6]), None);
    assert_eq!(
        path_schedule.observe_path("durable.cas", &[7]),
        Some(FaultAction::ErrorBefore)
    );

    world.clock_mut().advance(3).expect("clock advances");
    world
        .observer_mut()
        .record(
            3,
            "reopened",
            BTreeMap::from([("revision".to_owned(), json!("sha256:test"))]),
        )
        .expect("observation records");
    assert_eq!(world.observer().observations()[0].sequence, 0);
}

#[test]
fn generated_failures_minimize_to_language_neutral_fixtures() {
    let case = TraceCase::new(
        7,
        vec![
            json!({"type": "start"}),
            json!({"type": "break"}),
            json!({"type": "query"}),
        ],
        FaultPlan::default(),
    )
    .expect("trace creates");
    let expected = FailureFingerprint::new("model_mismatch", "command", "break_retained")
        .expect("fingerprint validates");
    let unrelated = FailureFingerprint::new("other", "command", "other_failure")
        .expect("fingerprint validates");
    let minimized = case.minimize_failure(&expected, |candidate| {
        if candidate
            .commands
            .iter()
            .any(|command| command["type"] == "break")
        {
            Err(expected.clone())
        } else if candidate
            .commands
            .iter()
            .any(|command| command["type"] == "query")
        {
            Err(unrelated.clone())
        } else {
            Ok(())
        }
    });
    assert_eq!(minimized.identity.path, [1]);
    assert_eq!(minimized.expected_failure.as_ref(), Some(&expected));
    assert!(
        minimized
            .fixture_json()
            .expect("fixture encodes")
            .contains(TRACE_VERSION)
    );
    assert!(
        minimized
            .fixture_json()
            .expect("fixture encodes")
            .contains("break_retained")
    );
    let replay = ReplaySpec {
        package: "cymule-durable",
        test_target: "model_trace",
        test_name: "generated_durable_commands_match_the_reference_model",
    }
    .command(7);
    assert!(replay.starts_with("CYMULE_TRACE_SEED=7 cargo test"));
    assert!(!requested_seeds(2).expect("seeds select").is_empty());
}

#[test]
fn temporary_domain_rejects_escape_and_deletes_on_drop() {
    let root = {
        let world = TestWorld::new(5).expect("world creates");
        let root = world.domain().root().to_owned();
        assert!(world.domain().path("state/domain.sqlite").is_ok());
        assert!(world.domain().path("../outside").is_err());
        root
    };
    assert!(
        !root.exists(),
        "temporary durable domain must delete on drop"
    );
}

#[test]
fn recording_observer_orders_without_a_global_subscriber() {
    let mut observer = RecordingObserver::default();
    observer
        .record(4, "first", BTreeMap::new())
        .expect("first records");
    observer
        .record(4, "second", BTreeMap::new())
        .expect("second records");
    assert_eq!(observer.observations()[1].sequence, 1);
}
