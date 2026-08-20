//! HTTP acknowledgement and backpressure tests.

use std::collections::{BTreeMap, BTreeSet};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cymule_activation_http::{AllowAll, durable_signal_router, signal_router};
use cymule_core::{ArtifactRef, Machine, ROOT_SCOPE_ID};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableState, FrameState, ParkedWaitIndex, WaitCondition,
    WaitKind, WaitSourceDriver, WaitState,
};
use tempfile::tempdir;
use tower::ServiceExt;

fn index() -> ParkedWaitIndex {
    let mut state = DurableState::new(Machine::new().snapshot());
    state.continuations.insert(
        "run:http".to_owned(),
        Continuation {
            run_id: "run:http".to_owned(),
            plan_id: "sha256:plan".to_owned(),
            binding_context: "binding:test".to_owned(),
            frames: vec![FrameState {
                definition_id: "main".to_owned(),
                invocation_id: "main".to_owned(),
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
            wait_set: BTreeSet::from(["wait:http".to_owned()]),
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
        "wait:http".to_owned(),
        WaitCondition {
            wait_id: "wait:http".to_owned(),
            run_id: "run:http".to_owned(),
            kind: WaitKind::Signal {
                key: "signal:http".to_owned(),
            },
            consume_once: true,
            result_binding: None,
            state: WaitState::Pending,
            result: None,
        },
    );
    ParkedWaitIndex::rebuild(&state).expect("index rebuilds")
}

fn request() -> Request<Body> {
    request_with(r#"{"ok":true}"#)
}

fn request_with(value: &str) -> Request<Body> {
    Request::post("/v1/signals")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"activation_id":"activation:http","key":"signal:http","value":{value}}}"#
        )))
        .expect("request builds")
}

#[tokio::test]
async fn http_response_waits_for_durable_acknowledgement() {
    let (router, mut driver) = signal_router(4, AllowAll).expect("router builds");
    let response = tokio::spawn(router.oneshot(request()));
    tokio::task::yield_now().await;
    let delivery = driver
        .receive(&index(), 1)
        .expect("driver receives")
        .expect("delivery exists");
    assert!(!response.is_finished());
    assert_eq!(delivery.activation_id, "activation:http");
    assert_eq!(
        driver.receive(&index(), 1).expect("redelivers"),
        Some(delivery.clone())
    );
    driver
        .acknowledge(&delivery.activation_id)
        .expect("acknowledges");
    assert_eq!(
        response
            .await
            .expect("task joins")
            .expect("router responds")
            .status(),
        StatusCode::ACCEPTED
    );
}

#[tokio::test]
async fn bounded_channel_returns_backpressure() {
    let (router, _driver) = signal_router(1, AllowAll).expect("router builds");
    let first = tokio::spawn(router.clone().oneshot(request()));
    tokio::task::yield_now().await;
    let second = router.oneshot(request()).await.expect("router responds");
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    first.abort();
}

#[tokio::test]
async fn acknowledged_identity_replays_and_conflicting_reuse_fails() {
    let (router, mut driver) = signal_router(4, AllowAll).expect("router builds");
    let first = tokio::spawn(router.clone().oneshot(request()));
    tokio::task::yield_now().await;
    let delivery = driver
        .receive(&index(), 1)
        .expect("driver receives")
        .expect("delivery exists");
    driver
        .acknowledge(&delivery.activation_id)
        .expect("acknowledges");
    assert_eq!(
        first.await.expect("task joins").expect("responds").status(),
        StatusCode::ACCEPTED
    );

    let replay = tokio::spawn(router.clone().oneshot(request()));
    tokio::task::yield_now().await;
    assert!(
        driver
            .receive(&index(), 1)
            .expect("replay drains")
            .is_none()
    );
    assert_eq!(
        replay
            .await
            .expect("task joins")
            .expect("responds")
            .status(),
        StatusCode::ACCEPTED
    );

    let conflict = tokio::spawn(router.oneshot(request_with(r#"{"ok":false}"#)));
    tokio::task::yield_now().await;
    assert!(
        driver
            .receive(&index(), 1)
            .expect("conflict drains")
            .is_none()
    );
    assert_eq!(
        conflict
            .await
            .expect("task joins")
            .expect("responds")
            .status(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn durable_ingress_reopens_with_the_exact_selected_delivery() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (router, mut driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let first_response = tokio::spawn(router.oneshot(request()));
    let selected = loop {
        if let Some(delivery) = driver.receive(&index(), 1).expect("driver polls") {
            break delivery;
        }
        tokio::task::yield_now().await;
    };
    assert!(!first_response.is_finished());
    first_response.abort();
    drop(driver);

    let empty = ParkedWaitIndex::rebuild(&DurableState::new(Machine::new().snapshot()))
        .expect("empty index rebuilds");
    let (reopened_router, mut reopened) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router reopens");
    let retry = tokio::spawn(reopened_router.oneshot(request()));
    let redelivered = loop {
        if let Some(delivery) = reopened.receive(&empty, 1).expect("driver polls") {
            break delivery;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(redelivered, selected);
    reopened
        .acknowledge(&redelivered.activation_id)
        .expect("retained delivery acknowledges");
    assert!(
        reopened
            .receive(&empty, 1)
            .expect("acknowledged ingress drains")
            .is_none()
    );
    assert_eq!(
        retry
            .await
            .expect("task joins")
            .expect("router responds")
            .status(),
        StatusCode::ACCEPTED
    );

    let replay = durable_signal_router(&database, 4, AllowAll)
        .expect("durable router reopens again")
        .0
        .oneshot(request())
        .await
        .expect("router responds");
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
}
