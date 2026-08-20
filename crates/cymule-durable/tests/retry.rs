//! Deterministic and fault-oriented durable retry policy tests.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cymule_core::Machine;
use cymule_durable::{
    DurableCoordinator, DurableError, DurableResult, DurableState, DurableStore, FailureClass,
    FailureOperation, JitterEvidence, JitterStrategy, MemoryStore, RetryCommand, RetryDelay,
    RetryDisposition, RetryFailure, RetryPolicy, RetryStopReason, StoreCommit, StoredState,
};

#[derive(Clone)]
struct LostReceiptStore {
    inner: MemoryStore,
    armed: Arc<AtomicBool>,
}

impl DurableStore for LostReceiptStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated lost retry decision receipt".to_owned(),
            ));
        }
        Ok(commit)
    }
}

fn classes(classes: impl IntoIterator<Item = FailureClass>) -> BTreeSet<FailureClass> {
    classes.into_iter().collect()
}

fn command(
    retry_id: &str,
    attempt: u32,
    class: FailureClass,
    operation: FailureOperation,
    observed_at: u64,
    jitter: Option<u64>,
) -> RetryCommand {
    RetryCommand {
        retry_id: retry_id.to_owned(),
        decision_id: format!("decision:{retry_id}:{attempt}"),
        attempt,
        failure: RetryFailure {
            failure_id: format!("failure:{retry_id}:{attempt}"),
            class,
            operation,
        },
        logical_observed_at: observed_at,
        occurrence_binding: format!("binding:worker/{attempt}"),
        jitter_evidence: jitter.map(|delay| JitterEvidence {
            evidence_id: format!("clock:jitter:{retry_id}:{attempt}"),
            delay,
        }),
    }
}

#[test]
fn exponential_schedule_is_integer_bounded_and_replay_deterministic() {
    let policy = RetryPolicy::seal(
        4,
        classes([FailureClass::Transient]),
        RetryDelay::Exponential {
            initial_delay: 10,
            multiplier: 3,
            max_delay: 50,
        },
        JitterStrategy::Recorded { max_delay: 9 },
    )
    .expect("policy seals");
    let round_trip: RetryPolicy =
        serde_json::from_value(serde_json::to_value(&policy).expect("policy serializes"))
            .expect("policy deserializes");
    round_trip.verify().expect("round-trip policy verifies");
    assert_eq!(round_trip, policy);

    let first = command(
        "exponential",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        100,
        Some(5),
    );
    let first_result = policy.evaluate(&first).expect("first retry evaluates");
    assert_eq!(
        first_result,
        RetryDisposition::RetryAt {
            next_due_at: 115,
            delay: 15,
            jitter_evidence: first.jitter_evidence.clone(),
        }
    );
    assert_eq!(
        policy.evaluate(&first).expect("same input replays"),
        first_result
    );

    let second = command(
        "exponential",
        2,
        FailureClass::Transient,
        FailureOperation::Computation,
        115,
        Some(7),
    );
    assert!(matches!(
        policy.evaluate(&second).expect("second retry evaluates"),
        RetryDisposition::RetryAt {
            next_due_at: 152,
            delay: 37,
            ..
        }
    ));
    let third = command(
        "exponential",
        3,
        FailureClass::Transient,
        FailureOperation::Computation,
        152,
        Some(9),
    );
    assert!(matches!(
        policy.evaluate(&third).expect("capped retry evaluates"),
        RetryDisposition::RetryAt {
            next_due_at: 211,
            delay: 59,
            ..
        }
    ));
    let fourth = command(
        "exponential",
        4,
        FailureClass::Transient,
        FailureOperation::Computation,
        211,
        None,
    );
    assert_eq!(
        policy.evaluate(&fourth).expect("attempt bound evaluates"),
        RetryDisposition::Stop {
            reason: RetryStopReason::AttemptsExhausted,
        }
    );
}

#[test]
fn closed_failure_classes_drive_admission_without_string_matching() {
    let retryable = [
        FailureClass::Expected,
        FailureClass::Transient,
        FailureClass::Defect,
        FailureClass::Cancelled,
        FailureClass::TimedOut,
        FailureClass::LeaseLost,
        FailureClass::UnknownWorld,
    ];
    let policy = RetryPolicy::seal(
        2,
        classes(retryable),
        RetryDelay::Immediate,
        JitterStrategy::None,
    )
    .expect("policy seals");
    for class in retryable {
        let operation = if class == FailureClass::UnknownWorld {
            FailureOperation::ObservationalEffect {
                intent_id: "effect:read".to_owned(),
            }
        } else {
            FailureOperation::Computation
        };
        let decision = policy
            .evaluate(&command(
                &format!("class:{class:?}"),
                1,
                class,
                operation,
                0,
                None,
            ))
            .expect("closed class evaluates");
        assert!(matches!(decision, RetryDisposition::RetryAt { .. }));
    }

    let non_retryable = RetryPolicy::seal(
        2,
        BTreeSet::new(),
        RetryDelay::Immediate,
        JitterStrategy::None,
    )
    .expect("policy seals");
    assert_eq!(
        non_retryable
            .evaluate(&command(
                "expected-stop",
                1,
                FailureClass::Expected,
                FailureOperation::Computation,
                0,
                None,
            ))
            .expect("non-retryable failure evaluates"),
        RetryDisposition::Stop {
            reason: RetryStopReason::FailureNotRetryable,
        }
    );
}

#[test]
fn unknown_mutating_effect_never_retries_and_preserves_reconciliation_identity() {
    let policy = RetryPolicy::seal(
        10,
        classes([FailureClass::UnknownWorld]),
        RetryDelay::Immediate,
        JitterStrategy::None,
    )
    .expect("policy seals");
    let decision = policy
        .evaluate(&command(
            "mutating-effect",
            1,
            FailureClass::UnknownWorld,
            FailureOperation::MutatingEffect {
                intent_id: "effect:charge:42".to_owned(),
            },
            80,
            None,
        ))
        .expect("unknown mutating outcome evaluates");
    assert_eq!(
        decision,
        RetryDisposition::Stop {
            reason: RetryStopReason::ReconciliationRequired {
                intent_id: "effect:charge:42".to_owned(),
            },
        }
    );

    let invalid = command(
        "unknown-computation",
        1,
        FailureClass::UnknownWorld,
        FailureOperation::Computation,
        80,
        None,
    );
    assert!(matches!(
        policy.evaluate(&invalid),
        Err(DurableError::Validation(_))
    ));
}

#[test]
fn delay_and_due_time_overflow_fail_closed() {
    let schedule_overflow = RetryPolicy::seal(
        3,
        classes([FailureClass::Transient]),
        RetryDelay::Exponential {
            initial_delay: u64::MAX / 2,
            multiplier: 3,
            max_delay: u64::MAX,
        },
        JitterStrategy::Recorded { max_delay: 1 },
    )
    .expect("policy seals");
    assert!(matches!(
        schedule_overflow.evaluate(&command(
            "delay-overflow",
            2,
            FailureClass::Transient,
            FailureOperation::Computation,
            0,
            Some(1),
        )),
        Err(DurableError::Validation(message)) if message.contains("delay exceeds")
    ));

    let clock_overflow = RetryPolicy::seal(
        2,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 10 },
        JitterStrategy::None,
    )
    .expect("policy seals");
    assert!(matches!(
        clock_overflow.evaluate(&command(
            "clock-overflow",
            1,
            FailureClass::Transient,
            FailureOperation::Computation,
            u64::MAX - 5,
            None,
        )),
        Err(DurableError::Validation(message)) if message.contains("due time exceeds")
    ));
}

#[test]
fn durable_retry_stream_enforces_due_time_attempt_order_and_terminal_state() {
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("store initializes");
    let policy = RetryPolicy::seal(
        2,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 10 },
        JitterStrategy::None,
    )
    .expect("policy seals");
    let first = command(
        "ordered",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        100,
        None,
    );
    let first_decision = coordinator
        .decide_retry(&policy, first.clone())
        .expect("first decision commits");
    assert_eq!(first_decision.policy_id, policy.policy_id);
    assert_eq!(
        coordinator
            .decide_retry(&policy, first)
            .expect("same command replays"),
        first_decision
    );

    assert!(matches!(
        coordinator.decide_retry(
            &policy,
            command(
                "ordered",
                2,
                FailureClass::Transient,
                FailureOperation::Computation,
                109,
                None,
            ),
        ),
        Err(DurableError::IllegalTransition(message)) if message.contains("before")
    ));
    let changed_policy = RetryPolicy::seal(
        2,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 11 },
        JitterStrategy::None,
    )
    .expect("changed policy seals");
    assert!(matches!(
        coordinator.retry_decisions(&changed_policy, "ordered"),
        Err(DurableError::Validation(message)) if message.contains("policy identity")
    ));
    let second = command(
        "ordered",
        2,
        FailureClass::Transient,
        FailureOperation::Computation,
        110,
        None,
    );
    assert!(matches!(
        coordinator
            .decide_retry(&policy, second)
            .expect("second decision commits")
            .disposition,
        RetryDisposition::Stop {
            reason: RetryStopReason::AttemptsExhausted,
            ..
        }
    ));
    assert!(matches!(
        coordinator.decide_retry(
            &policy,
            command(
                "ordered",
                3,
                FailureClass::Transient,
                FailureOperation::Computation,
                120,
                None,
            ),
        ),
        Err(DurableError::IllegalTransition(message)) if message.contains("terminal")
    ));
    assert_eq!(
        coordinator
            .retry_decisions(&policy, "ordered")
            .expect("decisions restore")
            .len(),
        2
    );
}

#[test]
fn lost_receipt_reopen_returns_original_jitter_and_binding_decision() {
    let inner = MemoryStore::new();
    let armed = Arc::new(AtomicBool::new(false));
    let store = LostReceiptStore {
        inner: inner.clone(),
        armed: armed.clone(),
    };
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("store initializes");
    let policy = RetryPolicy::seal(
        3,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 20 },
        JitterStrategy::Recorded { max_delay: 10 },
    )
    .expect("policy seals");
    let original = command(
        "lost-receipt",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        500,
        Some(7),
    );
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        coordinator.decide_retry(&policy, original.clone()),
        Err(DurableError::Substrate(message)) if message == "simulated lost retry decision receipt"
    ));
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(inner).expect("store reopens");
    let replayed = reopened
        .decide_retry(&policy, original.clone())
        .expect("lost receipt returns original decision");
    assert_eq!(replayed.command, original);
    assert_eq!(
        replayed.disposition,
        RetryDisposition::RetryAt {
            next_due_at: 527,
            delay: 27,
            jitter_evidence: Some(JitterEvidence {
                evidence_id: "clock:jitter:lost-receipt:1".to_owned(),
                delay: 7,
            }),
        }
    );
    assert_eq!(
        reopened
            .retry_decisions(&policy, "lost-receipt")
            .expect("stream restores")
            .len(),
        1
    );

    let mut conflicting = original;
    conflicting.jitter_evidence = Some(JitterEvidence {
        evidence_id: "clock:jitter:lost-receipt:1:changed".to_owned(),
        delay: 8,
    });
    assert!(matches!(
        reopened.decide_retry(&policy, conflicting),
        Err(DurableError::IllegalTransition(message)) if message.contains("different content")
    ));
}
