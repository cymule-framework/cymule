//! Contract tests for the closed durable protocol authority.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ArtifactRef, EXECUTION_BINDING_ARTIFACT_KIND, InvocationPathSegment, MAX_EXACT_INTEGER,
    artifact_ref,
};
use cymule_durable_protocol::{
    CLOCK_OBSERVATION_VERSION, CONTINUATION_STATE_VERSION, ClockObservation, ClockObservationRef,
    ClockObservationResult, Continuation, ContinuationExecutionClaim, ContinuationStatus,
    EXECUTION_CLAIM_VERSION, FrameState, MAX_CONTINUATION_AGGREGATE_ITEMS, MAX_CONTINUATION_FRAMES,
    MAX_CONTINUATION_IDENTITY_SCALARS, MAX_CONTINUATION_WIRE_BYTES, MAX_FRAME_INVOCATION_DEPTH,
    MAX_REGION_PATH_DEPTH, MAX_WAIT_DELIVERY_TARGETS, WAIT_ACTIVATION_RECEIPT_VERSION,
    WAIT_RESULT_ARTIFACT_KIND, WaitActivation, WaitActivationReceipt, WaitActivationSource,
    WaitOwner, clock_observation_id, continuation_id, execution_clock_scope,
};
use serde_json::{Value, json};

fn digest(index: usize) -> String {
    format!("sha256:{index:064x}")
}

fn artifact(kind: &str, seed: &[u8]) -> ArtifactRef {
    artifact_ref(kind, seed).expect("test Artifact must seal")
}

fn artifact_identity_scalars(reference: &ArtifactRef) -> usize {
    reference.identity_version.chars().count()
        + reference.artifact_id.chars().count()
        + reference.kind.chars().count()
}

fn clock_ref_identity_scalars(reference: &ClockObservationRef) -> usize {
    reference.clock_version.chars().count()
        + reference.observation_id.chars().count()
        + reference.source_id.chars().count()
        + reference.source_generation.chars().count()
        + reference.scope.chars().count()
}

fn claim_identity_scalars(claim: &ContinuationExecutionClaim) -> usize {
    claim.claim_version.chars().count()
        + claim.run_id.chars().count()
        + claim.continuation_id.chars().count()
        + claim.owner.chars().count()
        + claim.continuation_attempt_id.chars().count()
        + claim.plan_id.chars().count()
        + artifact_identity_scalars(&claim.execution_binding_ref)
        + clock_ref_identity_scalars(&claim.clock_observation_ref)
}

fn continuation_identity_scalars(continuation: &Continuation) -> usize {
    continuation.continuation_version.chars().count()
        + continuation.run_id.chars().count()
        + continuation.plan_id.chars().count()
        + continuation.binding_context.chars().count()
        + continuation
            .state
            .as_ref()
            .map_or(0, artifact_identity_scalars)
        + continuation
            .wait_set
            .iter()
            .chain(&continuation.scope_stack)
            .map(|value| value.chars().count())
            .sum::<usize>()
        + continuation
            .execution_claim
            .as_ref()
            .map_or(0, claim_identity_scalars)
        + continuation
            .frames
            .iter()
            .map(|frame| {
                frame.definition_id.chars().count()
                    + frame.invocation_id.chars().count()
                    + frame.scope_id.chars().count()
                    + artifact_identity_scalars(&frame.input)
                    + frame
                        .locals
                        .iter()
                        .map(|(name, local)| {
                            name.chars().count() + artifact_identity_scalars(local)
                        })
                        .sum::<usize>()
                    + frame
                        .invocation_path
                        .iter()
                        .map(|segment| {
                            segment.site_id.chars().count() + segment.scope_id.chars().count()
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
}

fn observation(logical_time: u64, observed_unix_ms: u64) -> ClockObservation {
    let source_generation = digest(1);
    let scope = execution_clock_scope("run-1").expect("Run scope must derive");
    ClockObservation {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id: clock_observation_id(
            "clock-1",
            &source_generation,
            &scope,
            logical_time,
            observed_unix_ms,
        )
        .expect("Clock identity must derive"),
        source_id: "clock-1".to_owned(),
        source_generation,
        scope,
        logical_time,
        observed_unix_ms,
    }
}

fn ready_continuation() -> Continuation {
    let binding = artifact(EXECUTION_BINDING_ARTIFACT_KIND, b"binding");
    Continuation {
        continuation_version: CONTINUATION_STATE_VERSION.to_owned(),
        run_id: "run-1".to_owned(),
        plan_id: digest(2),
        binding_context: binding.artifact_id,
        frames: vec![FrameState {
            definition_id: "definition-1".to_owned(),
            invocation_id: "invocation-1".to_owned(),
            invocation_path: Vec::new(),
            scope_id: "scope-1".to_owned(),
            input: artifact("test.input/1", b"input"),
            region_path: vec![0],
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: None,
        wait_set: BTreeSet::new(),
        scope_stack: vec!["scope-1".to_owned()],
        epoch: 0,
        execution_fence: 0,
        execution_claim: None,
        status: ContinuationStatus::Ready,
    }
}

fn running_continuation() -> (Continuation, ClockObservation) {
    let clock = observation(7, 11);
    let mut continuation = ready_continuation();
    continuation.execution_fence = 1;
    continuation.status = ContinuationStatus::Running;
    continuation.execution_claim = Some(ContinuationExecutionClaim {
        claim_version: EXECUTION_CLAIM_VERSION.to_owned(),
        run_id: continuation.run_id.clone(),
        continuation_id: continuation_id(&continuation.run_id)
            .expect("Continuation identity must derive"),
        owner: "driver-1".to_owned(),
        continuation_attempt_id: digest(3),
        fence: 1,
        plan_id: continuation.plan_id.clone(),
        execution_binding_ref: ArtifactRef {
            identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
            artifact_id: continuation.binding_context.clone(),
            kind: EXECUTION_BINDING_ARTIFACT_KIND.to_owned(),
        },
        clock_observation_ref: clock.reference(),
        logical_acquired_at: 7,
        logical_ttl: 5,
        logical_expires_at: 12,
    });
    (continuation, clock)
}

fn activation(wait_ids: BTreeSet<String>) -> WaitActivation {
    WaitActivation::new(
        "delivery-1",
        WaitActivationSource::Signal {
            key: "signal-1".to_owned(),
        },
        wait_ids,
        artifact(WAIT_RESULT_ARTIFACT_KIND, b"result"),
    )
    .expect("activation must be valid")
}

#[test]
fn clock_identity_and_exact_integer_bounds_are_closed() {
    let receipt = observation(MAX_EXACT_INTEGER, MAX_EXACT_INTEGER);
    receipt.verify().expect("maximum exact receipt must verify");

    let mut forged = receipt.clone();
    forged.source_id = "clock-2".to_owned();
    assert!(forged.verify().is_err());

    let mut over = receipt;
    over.logical_time = MAX_EXACT_INTEGER + 1;
    assert!(over.verify().is_err());
}

#[test]
fn clock_reference_validates_its_closed_shape() {
    let mut reference = observation(1, 2).reference();
    reference.observation_id = digest(9);
    reference.clock_version = "cymule.clock-observation/999".to_owned();
    assert!(reference.verify().is_err());

    let malformed = ClockObservationRef {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id: "sha256:ABC".to_owned(),
        source_id: "clock-1".to_owned(),
        source_generation: digest(1),
        scope: "scope-1".to_owned(),
    };
    assert!(malformed.verify().is_err());
}

#[test]
fn clock_observation_result_binds_the_opaque_scope_to_one_run() {
    let observation = observation(1, 2).reference();
    let result = ClockObservationResult::new("run-1", observation.clone())
        .expect("matching Run scope must verify");
    assert_eq!(result.run_id, "run-1");
    assert_eq!(result.observation, observation);

    let error = ClockObservationResult::new("run-other", observation)
        .expect_err("another Run must not claim the observation scope");
    assert!(error.to_string().contains("does not match its Run scope"));
}

#[test]
fn continuation_claim_requires_content_addressed_plan_and_attempt_ids() {
    let (continuation, clock) = running_continuation();
    continuation.verify_wire().expect("claim wire verifies");
    continuation
        .execution_claim
        .as_ref()
        .expect("running claim exists")
        .verify(
            &continuation,
            &BTreeMap::from([(clock.observation_id.clone(), clock)]),
        )
        .expect("content-addressed claim fixture verifies");

    let mut malformed_attempt = continuation.clone();
    malformed_attempt
        .execution_claim
        .as_mut()
        .expect("running claim exists")
        .continuation_attempt_id = "attempt-1".to_owned();
    assert!(malformed_attempt.verify_wire().is_err());

    let mut malformed_plan = continuation;
    malformed_plan.plan_id = "plan-1".to_owned();
    malformed_plan
        .execution_claim
        .as_mut()
        .expect("running claim exists")
        .plan_id = "plan-1".to_owned();
    assert!(malformed_plan.verify_wire().is_err());
}

#[test]
fn required_nullable_continuation_fields_reject_omission() {
    let continuation = ready_continuation();
    let mut value = serde_json::to_value(&continuation).expect("Continuation must encode");
    value
        .as_object_mut()
        .expect("Continuation must be an object")
        .remove("state");
    assert!(serde_json::from_value::<Continuation>(value).is_err());

    let mut value = serde_json::to_value(&continuation).expect("Continuation must encode");
    value
        .as_object_mut()
        .expect("Continuation must be an object")
        .remove("execution_claim");
    assert!(serde_json::from_value::<Continuation>(value).is_err());

    let explicit_null = serde_json::to_value(continuation).expect("Continuation must encode");
    serde_json::from_value::<Continuation>(explicit_null)
        .expect("explicit null fields must decode");
}

#[test]
fn continuation_state_generation_is_required_and_exact() {
    let continuation = ready_continuation();
    let mut missing = serde_json::to_value(&continuation).expect("Continuation must encode");
    missing
        .as_object_mut()
        .expect("Continuation must be an object")
        .remove("continuation_version");
    assert!(serde_json::from_value::<Continuation>(missing).is_err());

    let mut unsupported = continuation;
    unsupported.continuation_version = "cymule.continuation-state/999".to_owned();
    assert!(unsupported.verify_wire().is_err());
}

#[test]
fn continuation_count_bounds_admit_exact_maximum_and_reject_successor() {
    let source = ready_continuation();
    let frame = source.frames[0].clone();
    let mut exact = source.clone();
    exact.frames = vec![frame.clone(); MAX_CONTINUATION_FRAMES];
    exact
        .verify_wire()
        .expect("exact maximum frame count must verify");

    let mut over = source.clone();
    over.frames = vec![frame; MAX_CONTINUATION_FRAMES + 1];
    assert!(over.verify_wire().is_err());

    let mut invocation_over = source;
    invocation_over.frames[0].invocation_path = vec![
        InvocationPathSegment {
            site_id: "site-1".to_owned(),
            region_path: Vec::new(),
            scope_id: "scope-1".to_owned(),
        };
        MAX_FRAME_INVOCATION_DEPTH + 1
    ];
    assert!(invocation_over.verify_wire().is_err());
}

#[test]
fn continuation_rejects_aggregate_nested_items_and_invalid_local_names() {
    let mut aggregate = ready_continuation();
    let nested_depth = MAX_CONTINUATION_AGGREGATE_ITEMS / MAX_FRAME_INVOCATION_DEPTH + 1;
    aggregate.frames[0].invocation_path = vec![
        InvocationPathSegment {
            site_id: "site-1".to_owned(),
            region_path: vec![0; nested_depth],
            scope_id: "scope-1".to_owned(),
        };
        MAX_FRAME_INVOCATION_DEPTH
    ];
    assert!(aggregate.verify_wire().is_err());

    let mut invalid_local = ready_continuation();
    invalid_local.frames[0]
        .locals
        .insert(String::new(), artifact("test.local/1", b"local"));
    assert!(invalid_local.verify_wire().is_err());
}

#[test]
fn continuation_identity_scalar_bound_accepts_exact_maximum_and_rejects_successor() {
    let mut exact = ready_continuation();
    let local = artifact("test.local/1", b"local");
    let mut remaining = MAX_CONTINUATION_IDENTITY_SCALARS
        .checked_sub(continuation_identity_scalars(&exact))
        .expect("base Continuation must fit the scalar budget");
    let run_padding_capacity = 512 - exact.run_id.chars().count();
    let mut index = 0_usize;
    while remaining > run_padding_capacity {
        let name = format!("local-{index:04}");
        let consumed = name.chars().count() + artifact_identity_scalars(&local);
        assert!(consumed < remaining);
        exact
            .frames
            .first_mut()
            .expect("frame must exist")
            .locals
            .insert(name, local.clone());
        remaining -= consumed;
        index += 1;
    }
    exact.run_id.push_str(&"x".repeat(remaining));
    assert_eq!(
        continuation_identity_scalars(&exact),
        MAX_CONTINUATION_IDENTITY_SCALARS
    );
    exact
        .verify_wire()
        .expect("exact aggregate identity scalar maximum must verify");

    let mut over = exact.clone();
    over.plan_id.push('x');
    assert!(over.verify_wire().is_err());

    let mut frame_input_over = exact.clone();
    frame_input_over.frames[0].input.kind.push('x');
    assert_eq!(
        continuation_identity_scalars(&frame_input_over),
        MAX_CONTINUATION_IDENTITY_SCALARS + 1
    );
    assert!(frame_input_over.verify_wire().is_err());

    let mut local_over = exact.clone();
    local_over.frames[0]
        .locals
        .first_entry()
        .expect("exact fixture must have local Artifacts")
        .get_mut()
        .kind
        .push('x');
    assert_eq!(
        continuation_identity_scalars(&local_over),
        MAX_CONTINUATION_IDENTITY_SCALARS + 1
    );
    assert!(local_over.verify_wire().is_err());

    let mut state_over = exact.clone();
    state_over.state = Some(artifact("test.state/1", b"state"));
    assert!(continuation_identity_scalars(&state_over) > MAX_CONTINUATION_IDENTITY_SCALARS);
    assert!(state_over.verify_wire().is_err());

    let (running, _) = running_continuation();
    let mut claim = running
        .execution_claim
        .expect("running fixture must carry a claim");
    claim.run_id.clone_from(&exact.run_id);
    claim.continuation_id = continuation_id(&exact.run_id).expect("identity must derive");
    claim.plan_id.clone_from(&exact.plan_id);
    claim.execution_binding_ref.artifact_id = exact.binding_context.clone();
    claim.clock_observation_ref.scope =
        execution_clock_scope(&exact.run_id).expect("Clock scope must derive");
    let mut claim_over = exact;
    claim_over.execution_fence = claim.fence;
    claim_over.execution_claim = Some(claim);
    claim_over.status = ContinuationStatus::Running;
    assert!(continuation_identity_scalars(&claim_over) > MAX_CONTINUATION_IDENTITY_SCALARS);
    assert!(claim_over.verify_wire().is_err());
}

#[test]
fn strict_continuation_decoder_rejects_oversize_before_json_decode() {
    let oversized = vec![b' '; MAX_CONTINUATION_WIRE_BYTES + 1];
    assert!(Continuation::decode_strict(&oversized).is_err());

    let encoded = serde_json::to_vec(&ready_continuation()).expect("Continuation must encode");
    Continuation::decode_strict(&encoded).expect("bounded Continuation must decode");
}

#[test]
fn strict_continuation_decoder_rejects_nested_duplicates_and_unsafe_numbers() {
    let encoded = String::from_utf8(
        serde_json::to_vec(&ready_continuation()).expect("Continuation must encode"),
    )
    .expect("Continuation JSON must be UTF-8");
    let local = serde_json::to_string(&artifact("test.local/1", b"local"))
        .expect("local Artifact reference must encode");
    let duplicated = encoded.replacen(
        "\"locals\":{}",
        &format!("\"locals\":{{\"duplicate\":{local},\"duplicate\":{local}}}"),
        1,
    );
    assert_ne!(duplicated, encoded);
    assert!(Continuation::decode_strict(duplicated.as_bytes()).is_err());

    let unsafe_number = encoded.replacen(
        "\"epoch\":0",
        &format!("\"epoch\":{}", MAX_EXACT_INTEGER + 1),
        1,
    );
    assert_ne!(unsafe_number, encoded);
    assert!(Continuation::decode_strict(unsafe_number.as_bytes()).is_err());
}

#[test]
fn required_nullable_wait_owner_bind_rejects_omission() {
    let owner = WaitOwner {
        invocation_id: "invocation-1".to_owned(),
        definition_id: "definition-1".to_owned(),
        site_id: "site-1".to_owned(),
        region_path: vec![0],
        step_index: 0,
        bind: None,
    };
    let mut value = serde_json::to_value(&owner).expect("Wait owner must encode");
    value
        .as_object_mut()
        .expect("Wait owner must be an object")
        .remove("bind");
    assert!(serde_json::from_value::<WaitOwner>(value).is_err());
    owner.verify().expect("explicit null bind must verify");

    let mut exact = owner.clone();
    exact.region_path = vec![0; MAX_REGION_PATH_DEPTH];
    exact
        .verify()
        .expect("exact maximum Region depth must verify");
    exact.region_path.push(0);
    assert!(exact.verify().is_err());
}

#[test]
fn running_claim_requires_exact_continuation_and_retained_clock() {
    let (continuation, clock) = running_continuation();
    continuation.verify_wire().expect("claim must match wire");
    let observations = BTreeMap::from([(clock.observation_id.clone(), clock.clone())]);
    continuation
        .execution_claim
        .as_ref()
        .expect("claim must exist")
        .verify(&continuation, &observations)
        .expect("retained Clock must authorize claim");

    let mut wrong_fence = continuation.clone();
    wrong_fence.execution_fence = 2;
    assert!(wrong_fence.verify_wire().is_err());

    let mut wrong_binding = continuation.clone();
    wrong_binding.binding_context = digest(17);
    assert!(wrong_binding.verify_wire().is_err());

    let mut wrong_time = clock;
    wrong_time.logical_time = 8;
    let observations = BTreeMap::from([(wrong_time.observation_id.clone(), wrong_time)]);
    assert!(
        continuation
            .execution_claim
            .as_ref()
            .expect("claim must exist")
            .verify(&continuation, &observations)
            .is_err()
    );
}

#[test]
fn wait_activation_closes_shape_count_and_unknown_fields() {
    let valid = activation(BTreeSet::from([digest(1)]));
    valid.verify().expect("single target must verify");
    assert!(
        WaitActivation::new(
            "delivery-1",
            WaitActivationSource::Signal {
                key: "signal-1".to_owned(),
            },
            BTreeSet::new(),
            artifact(WAIT_RESULT_ARTIFACT_KIND, b"result"),
        )
        .is_err()
    );

    let too_many = (0..=MAX_WAIT_DELIVERY_TARGETS).map(digest).collect();
    assert!(
        WaitActivation::new(
            "delivery-1",
            WaitActivationSource::Signal {
                key: "signal-1".to_owned(),
            },
            too_many,
            artifact(WAIT_RESULT_ARTIFACT_KIND, b"result"),
        )
        .is_err()
    );

    let mut wrong_kind = valid.clone();
    wrong_kind.result = artifact("test.result/1", b"result");
    assert!(wrong_kind.verify().is_err());

    let mut value = serde_json::to_value(valid).expect("activation must encode");
    value
        .as_object_mut()
        .expect("activation must be an object")
        .insert("unknown".to_owned(), Value::Null);
    assert!(serde_json::from_value::<WaitActivation>(value).is_err());
}

#[test]
fn wait_sources_enforce_consume_once_and_timer_cardinality() {
    let signal = WaitActivationSource::Signal {
        key: "signal-1".to_owned(),
    };
    signal
        .validate_target_cardinality(7, 1)
        .expect("one consume-once signal target is legal");
    assert!(signal.validate_target_cardinality(7, 2).is_err());

    let timer = WaitActivationSource::Timer {
        timer_id: "timer-1".to_owned(),
    };
    timer
        .validate_target_cardinality(1, 0)
        .expect("one timer target is legal");
    assert!(timer.validate_target_cardinality(2, 0).is_err());
}

#[test]
fn receipt_applied_targets_are_an_exact_subset() {
    let receipt = WaitActivationReceipt {
        receipt_version: WAIT_ACTIVATION_RECEIPT_VERSION.to_owned(),
        activation: activation(BTreeSet::from([digest(1)])),
        applied_wait_ids: BTreeSet::from([digest(2)]),
        ready_run_ids: BTreeSet::new(),
    };
    assert!(receipt.verify().is_err());

    let non_winner_ready = WaitActivationReceipt {
        receipt_version: WAIT_ACTIVATION_RECEIPT_VERSION.to_owned(),
        activation: activation(BTreeSet::from([digest(1)])),
        applied_wait_ids: BTreeSet::new(),
        ready_run_ids: BTreeSet::from(["run-1".to_owned()]),
    };
    assert!(non_winner_ready.verify().is_err());
}

#[test]
fn closed_source_enum_rejects_mixed_variant_fields() {
    let mixed = json!({
        "kind": "timer",
        "timer_id": "timer-1",
        "key": "signal-1"
    });
    assert!(serde_json::from_value::<WaitActivationSource>(mixed).is_err());
}
