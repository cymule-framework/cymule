//! Deterministic and fault-oriented retry policy reducer tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use cymule_durable::{
    ClockObservationAuthority, DurableError, DurableResult, FailureClass, FailureOperation,
    JitterEvidence, JitterStrategy, RetryCommand, RetryDecision, RetryDelay, RetryDisposition,
    RetryFailure, RetryPolicy, RetryStopReason, RetryStream, VerifiedRetryStream,
};
use cymule_durable_protocol::{
    CLOCK_OBSERVATION_VERSION, ClockObservation, ClockObservationRef, clock_observation_id,
};

#[derive(Clone, Copy)]
struct IssuedClock;

fn observations() -> &'static Mutex<BTreeMap<String, ClockObservation>> {
    static OBSERVATIONS: OnceLock<Mutex<BTreeMap<String, ClockObservation>>> = OnceLock::new();
    OBSERVATIONS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

impl ClockObservationAuthority for IssuedClock {
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        observations()
            .lock()
            .map_err(|error| DurableError::Substrate {
                code: "test_retry_clock_ledger_poisoned".to_owned(),
                message: error.to_string(),
            })?
            .get(&reference.observation_id)
            .filter(|observation| observation.reference() == *reference)
            .cloned()
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "retry Clock observation {} was not issued",
                    reference.observation_id
                ))
            })
    }
}

#[derive(Default)]
struct CountingClock {
    resolutions: usize,
}

impl ClockObservationAuthority for CountingClock {
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        self.resolutions += 1;
        IssuedClock.resolve(reference)
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
    let source_id = "clock:retry-test";
    let source_generation =
        "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let scope = cymule_core::content_id("cymule.retry-clock-scope/1", &retry_id)
        .expect("retry Clock scope derives");
    let observation_id = clock_observation_id(
        source_id,
        source_generation,
        &scope,
        observed_at,
        observed_at,
    )
    .expect("Clock observation identifies");
    let observation = ClockObservation {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id,
        source_id: source_id.to_owned(),
        source_generation: source_generation.to_owned(),
        scope,
        logical_time: observed_at,
        observed_unix_ms: observed_at,
    };
    observations()
        .lock()
        .expect("retry Clock ledger locks")
        .insert(observation.observation_id.clone(), observation.clone());
    RetryCommand {
        retry_id: retry_id.to_owned(),
        decision_id: format!("decision:{retry_id}:{attempt}"),
        attempt,
        failure: RetryFailure {
            failure_id: format!("failure:{retry_id}:{attempt}"),
            class,
            operation,
        },
        clock: observation.reference(),
        occurrence_binding: format!("binding:worker/{attempt}"),
        jitter_evidence: jitter.map(|delay| {
            JitterEvidence::seal(format!("binding:jitter:{retry_id}:{attempt}"), delay)
                .expect("jitter evidence seals")
        }),
    }
}

fn evaluate(policy: &RetryPolicy, command: &RetryCommand) -> DurableResult<RetryDisposition> {
    policy.evaluate(command.clone(), &mut IssuedClock)
}

fn apply(stream: &mut VerifiedRetryStream, command: RetryCommand) -> DurableResult<RetryDecision> {
    stream.apply(command, &mut IssuedClock)
}

fn verify(stream: &VerifiedRetryStream) -> DurableResult<()> {
    stream.audit(&mut IssuedClock)
}

#[test]
fn retry_command_requires_explicit_nullable_jitter_evidence() {
    let command = command(
        "required-nullable",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        7,
        None,
    );
    let mut value = serde_json::to_value(&command).expect("Retry command serializes");
    assert_eq!(value["jitter_evidence"], serde_json::Value::Null);
    let decoded: RetryCommand =
        serde_json::from_value(value.clone()).expect("explicit null is admitted");
    assert_eq!(decoded, command);

    value
        .as_object_mut()
        .expect("Retry command is an object")
        .remove("jitter_evidence");
    let error = serde_json::from_value::<RetryCommand>(value)
        .expect_err("missing required-nullable jitter evidence is rejected");
    assert!(error.to_string().contains("jitter_evidence"));
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
    let first_result = evaluate(&policy, &first).expect("first retry evaluates");
    assert_eq!(
        first_result,
        RetryDisposition::RetryAt {
            next_due_at: 115,
            delay: 15,
            jitter_evidence: first.jitter_evidence.clone(),
        }
    );
    assert_eq!(
        evaluate(&policy, &first).expect("same input replays"),
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
        evaluate(&policy, &second).expect("second retry evaluates"),
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
        evaluate(&policy, &third).expect("capped retry evaluates"),
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
        evaluate(&policy, &fourth).expect("attempt bound evaluates"),
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
        let decision = evaluate(
            &policy,
            &command(
                &format!("class:{class:?}"),
                1,
                class,
                FailureOperation::Computation,
                0,
                None,
            ),
        )
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
        evaluate(
            &non_retryable,
            &command(
                "expected-stop",
                1,
                FailureClass::Expected,
                FailureOperation::Computation,
                0,
                None,
            ),
        )
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
    let observational_intent = format!("sha256:{}", "1".repeat(64));
    let mutating_intent = format!("sha256:{}", "2".repeat(64));
    for (operation, intent_id) in [
        (
            FailureOperation::ObservationalEffect {
                intent_id: observational_intent.clone(),
            },
            observational_intent,
        ),
        (
            FailureOperation::MutatingEffect {
                intent_id: mutating_intent.clone(),
            },
            mutating_intent,
        ),
    ] {
        let decision = evaluate(
            &policy,
            &command(
                &format!("unknown:{intent_id}"),
                1,
                FailureClass::UnknownWorld,
                operation,
                80,
                None,
            ),
        )
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
        evaluate(&policy, &invalid),
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
    .expect_err("policy delay outside the exact range is rejected");
    assert!(
        matches!(&schedule_overflow, DurableError::Validation(_))
            || matches!(
                &schedule_overflow,
                DurableError::Integrity { code, .. } if code == "encoding_failed"
            ),
        "unexpected schedule overflow error: {schedule_overflow:?}"
    );

    let clock_overflow = RetryPolicy::seal(
        2,
        classes([FailureClass::Transient]),
        RetryDelay::Fixed { delay: 10 },
        JitterStrategy::None,
    )
    .expect("policy seals");
    assert!(matches!(
        evaluate(&clock_overflow, &command(
            "clock-overflow",
            1,
            FailureClass::Transient,
            FailureOperation::Computation,
            cymule_core::MAX_EXACT_INTEGER - 5,
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
    let mut stream = VerifiedRetryStream::new("ordered", policy.clone()).expect("stream creates");
    let first = command(
        "ordered",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        100,
        None,
    );
    let first_decision = apply(&mut stream, first.clone()).expect("first decision applies");
    assert_eq!(first_decision.policy_id, policy.policy_id);
    assert_eq!(
        apply(&mut stream, first).expect("same command replays"),
        first_decision
    );

    assert!(matches!(
        apply(&mut stream, command(
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
        apply(&mut stream, second)
            .expect("second decision applies")
            .disposition,
        RetryDisposition::Stop {
            reason: RetryStopReason::AttemptsExhausted,
            ..
        }
    ));
    assert!(matches!(
        apply(&mut stream, command(
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
    verify(&stream).expect("complete stream replays");
    let mut duplicated = stream.into_stream();
    duplicated.decisions.push(duplicated.decisions[1].clone());
    assert!(matches!(
        duplicated.verify(&mut IssuedClock),
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
    let mut stream = VerifiedRetryStream::new("reopen", policy.clone()).expect("stream creates");
    let original = command(
        "reopen",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        500,
        Some(7),
    );
    let first = apply(&mut stream, original.clone()).expect("decision applies");
    let encoded = serde_json::to_value(&stream).expect("stream serializes");
    let reopened: RetryStream = serde_json::from_value(encoded).expect("stream deserializes");
    let mut reopened = reopened
        .verify(&mut IssuedClock)
        .expect("stream verifies from retained policy");
    assert_eq!(reopened.policy, policy);
    let replayed =
        apply(&mut reopened, original.clone()).expect("same decision replays after reopen");
    assert_eq!(replayed, first);
    assert_eq!(replayed.admission.command(), &original);
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
        apply(&mut reopened, conflicting),
        Err(DurableError::IllegalTransition(message)) if message.contains("different content")
    ));

    let mut altered_policy_stream = reopened.into_stream();
    altered_policy_stream.policy.delay = RetryDelay::Fixed { delay: 21 };
    assert!(matches!(
        altered_policy_stream.verify(&mut IssuedClock),
        Err(DurableError::Validation(message)) if message.contains("policy identity")
    ));
}

#[test]
fn verified_retry_stream_audits_once_then_verifies_only_the_new_suffix() {
    let policy = RetryPolicy::seal(
        5,
        classes([FailureClass::Transient]),
        RetryDelay::Immediate,
        JitterStrategy::None,
    )
    .expect("policy seals");
    let mut clock = CountingClock::default();
    let mut verified = VerifiedRetryStream::new("incremental", policy).expect("stream creates");
    let first = command(
        "incremental",
        1,
        FailureClass::Transient,
        FailureOperation::Computation,
        1,
        None,
    );
    let second = command(
        "incremental",
        2,
        FailureClass::Transient,
        FailureOperation::Computation,
        2,
        None,
    );
    verified
        .apply(first.clone(), &mut clock)
        .expect("first decision applies");
    verified
        .apply(second, &mut clock)
        .expect("second decision applies");
    assert_eq!(clock.resolutions, 2);

    let encoded = serde_json::to_value(&verified).expect("verified stream serializes raw state");
    let raw: RetryStream = serde_json::from_value(encoded).expect("raw stream deserializes");
    let mut reopen_clock = CountingClock::default();
    let mut reopened = raw
        .verify(&mut reopen_clock)
        .expect("raw stream audits once on reopen");
    assert_eq!(reopen_clock.resolutions, 2);
    reopened
        .apply(
            command(
                "incremental",
                3,
                FailureClass::Transient,
                FailureOperation::Computation,
                3,
                None,
            ),
            &mut reopen_clock,
        )
        .expect("new suffix verifies incrementally");
    assert_eq!(reopen_clock.resolutions, 3);
    reopened
        .apply(first, &mut reopen_clock)
        .expect("exact retained command replays without Clock I/O");
    assert_eq!(reopen_clock.resolutions, 3);
}

#[test]
fn forged_clock_reference_and_tampered_jitter_fail_before_retry_progress() {
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
    altered_clock.clock.observation_id =
        "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned();
    assert!(matches!(
        evaluate(&policy, &altered_clock),
        Err(DurableError::NotFound(_))
    ));

    let mut stream = VerifiedRetryStream::new("evidence", policy.clone()).expect("stream creates");
    assert!(matches!(
        apply(&mut stream, altered_clock),
        Err(DurableError::NotFound(_))
    ));
    assert!(stream.decisions.is_empty());

    let mut altered_jitter = original;
    altered_jitter
        .jitter_evidence
        .as_mut()
        .expect("jitter exists")
        .delay = 5;
    assert!(matches!(
        evaluate(&policy, &altered_jitter),
        Err(DurableError::Validation(message)) if message.contains("jitter evidence identity")
    ));
}
