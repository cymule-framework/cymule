//! Deterministic and fault-oriented retry policy reducer tests.

use std::collections::BTreeSet;

use cymule_durable::{
    ClockObservation, DurableError, FailureClass, FailureOperation, JitterEvidence, JitterStrategy,
    RetryCommand, RetryDelay, RetryDisposition, RetryFailure, RetryPolicy, RetryStopReason,
    RetryStream,
};

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
        clock: ClockObservation::seal("binding:clock/1", observed_at)
            .expect("Clock observation seals"),
        occurrence_binding: format!("binding:worker/{attempt}"),
        jitter_evidence: jitter.map(|delay| {
            JitterEvidence::seal(format!("binding:jitter:{retry_id}:{attempt}"), delay)
                .expect("jitter evidence seals")
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
    ];
    let policy = RetryPolicy::seal(
        2,
        classes(retryable),
        RetryDelay::Immediate,
        JitterStrategy::None,
    )
    .expect("policy seals");
    for class in retryable {
        let decision = policy
            .evaluate(&command(
                &format!("class:{class:?}"),
                1,
                class,
                FailureOperation::Computation,
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
fn unknown_external_effect_never_retries_and_preserves_reconciliation_identity() {
    let policy = RetryPolicy::seal(
        10,
        classes([FailureClass::UnknownWorld]),
        RetryDelay::Immediate,
        JitterStrategy::None,
    )
    .expect("policy seals");
    for (operation, intent_id) in [
        (
            FailureOperation::ObservationalEffect {
                intent_id: "effect:read:42".to_owned(),
            },
            "effect:read:42".to_owned(),
        ),
        (
            FailureOperation::MutatingEffect {
                intent_id: "effect:charge:42".to_owned(),
            },
            "effect:charge:42".to_owned(),
        ),
    ] {
        let decision = policy
            .evaluate(&command(
                &format!("unknown:{intent_id}"),
                1,
                FailureClass::UnknownWorld,
                operation,
                80,
                None,
            ))
            .expect("unknown external outcome evaluates");
        assert_eq!(
            decision,
            RetryDisposition::Stop {
                reason: RetryStopReason::ReconciliationRequired { intent_id },
            }
        );
    }

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
fn retry_stream_enforces_due_time_attempt_order_and_terminal_state() {
    let policy = RetryPolicy::seal(
        2,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 10 },
        JitterStrategy::None,
    )
    .expect("policy seals");
    let mut stream = RetryStream::new("ordered", policy.clone()).expect("stream creates");
    let first = command(
        "ordered",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        100,
        None,
    );
    let first_decision = stream.apply(first.clone()).expect("first decision applies");
    assert_eq!(first_decision.policy_id, policy.policy_id);
    assert_eq!(
        stream.apply(first).expect("same command replays"),
        first_decision
    );

    assert!(matches!(
        stream.apply(command(
                "ordered",
                2,
                FailureClass::Transient,
                FailureOperation::Computation,
                109,
                None,
            )),
        Err(DurableError::IllegalTransition(message)) if message.contains("before")
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
        stream
            .apply(second)
            .expect("second decision applies")
            .disposition,
        RetryDisposition::Stop {
            reason: RetryStopReason::AttemptsExhausted,
            ..
        }
    ));
    assert!(matches!(
        stream.apply(command(
                "ordered",
                3,
                FailureClass::Transient,
                FailureOperation::Computation,
                120,
                None,
            )),
        Err(DurableError::IllegalTransition(message)) if message.contains("terminal")
    ));
    assert_eq!(stream.decisions.len(), 2);
    stream.verify().expect("complete stream replays");
    let mut duplicated = stream;
    duplicated.decisions.push(duplicated.decisions[1].clone());
    assert!(matches!(
        duplicated.verify(),
        Err(DurableError::Validation(message)) if message.contains("duplicated")
    ));
}

#[test]
fn serialized_stream_retains_canonical_policy_and_reopens_without_caller_policy() {
    let policy = RetryPolicy::seal(
        3,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 20 },
        JitterStrategy::Recorded { max_delay: 10 },
    )
    .expect("policy seals");
    let mut stream = RetryStream::new("reopen", policy.clone()).expect("stream creates");
    let original = command(
        "reopen",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        500,
        Some(7),
    );
    let first = stream.apply(original.clone()).expect("decision applies");
    let encoded = serde_json::to_value(&stream).expect("stream serializes");
    let mut reopened: RetryStream = serde_json::from_value(encoded).expect("stream deserializes");
    reopened
        .verify()
        .expect("stream verifies from retained policy");
    assert_eq!(reopened.policy, policy);
    let replayed = reopened
        .apply(original.clone())
        .expect("same decision replays after reopen");
    assert_eq!(replayed, first);
    assert_eq!(replayed.command, original);
    assert_eq!(
        replayed.disposition,
        RetryDisposition::RetryAt {
            next_due_at: 527,
            delay: 27,
            jitter_evidence: original.jitter_evidence.clone(),
        }
    );
    assert_eq!(reopened.decisions.len(), 1);

    let mut conflicting = original;
    conflicting.jitter_evidence = Some(
        JitterEvidence::seal("binding:jitter:reopen:changed", 8)
            .expect("changed jitter evidence seals"),
    );
    assert!(matches!(
        reopened.apply(conflicting),
        Err(DurableError::IllegalTransition(message)) if message.contains("different content")
    ));

    let mut altered_policy_stream = reopened;
    altered_policy_stream.policy.delay = RetryDelay::Fixed { delay: 21 };
    assert!(matches!(
        altered_policy_stream.verify(),
        Err(DurableError::Validation(message)) if message.contains("policy identity")
    ));
}

#[test]
fn clock_and_jitter_evidence_reject_identity_preserving_content_changes() {
    let policy = RetryPolicy::seal(
        2,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 10 },
        JitterStrategy::Recorded { max_delay: 10 },
    )
    .expect("policy seals");
    let original = command(
        "evidence",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        100,
        Some(4),
    );

    let mut altered_clock = original.clone();
    altered_clock.clock.logical_time = 101;
    assert!(matches!(
        policy.evaluate(&altered_clock),
        Err(DurableError::Validation(message)) if message.contains("Clock observation identity")
    ));

    let mut altered_jitter = original;
    altered_jitter
        .jitter_evidence
        .as_mut()
        .expect("jitter exists")
        .delay = 5;
    assert!(matches!(
        policy.evaluate(&altered_jitter),
        Err(DurableError::Validation(message)) if message.contains("jitter evidence identity")
    ));
}
