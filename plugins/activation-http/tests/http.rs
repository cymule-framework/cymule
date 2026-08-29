//! HTTP acknowledgement and backpressure tests.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cymule_activation_http::{
    AllowAll, HTTP_SPOOL_SCHEMA_VERSION, HttpSignalRequest, SqliteHttpSignalDriver,
    UNSUPPORTED_STORE_GENERATION_CODE, durable_signal_router,
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
use rusqlite::{Connection, params};
use tempfile::{TempDir, tempdir};
use tower::ServiceExt;

fn index() -> ParkedWaitIndex {
    index_for_signal("signal:http")
}

fn index_for_signal(signal_key: &str) -> ParkedWaitIndex {
    index_for_signals(&[("wait:http", signal_key)])
}

fn index_for_signals(signals: &[(&str, &str)]) -> ParkedWaitIndex {
    let mut state = DurableState::new(Machine::new().snapshot());
    state.continuations.insert(
        "run:http".to_owned(),
        Continuation {
            continuation_version: CONTINUATION_STATE_VERSION.to_owned(),
            run_id: "run:http".to_owned(),
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
            wait_set: signals
                .iter()
                .map(|(wait_id, _)| (*wait_id).to_owned())
                .collect(),
            scope_stack: vec![ROOT_SCOPE_ID.to_owned()],
            epoch: 0,
            execution_fence: 0,
            execution_claim: None,
            status: ContinuationStatus::Waiting,
        },
    );
    for (wait_id, signal_key) in signals {
        state.waits.insert(
            (*wait_id).to_owned(),
            WaitCondition {
                wait_id: (*wait_id).to_owned(),
                run_id: "run:http".to_owned(),
                kind: WaitKind::Signal {
                    key: (*signal_key).to_owned(),
                },
                consume_once: true,
                owner: WaitOwner {
                    invocation_id: "main".to_owned(),
                    definition_id: "main".to_owned(),
                    site_id: "wait.http".to_owned(),
                    region_path: Vec::new(),
                    step_index: 0,
                    bind: None,
                },
                state: WaitState::Pending,
                result: None,
            },
        );
    }
    ParkedWaitIndex::rebuild(&state).expect("index rebuilds")
}

#[derive(Default)]
struct ObservedWaitView {
    waits: ParkedWaitIndex,
    page_requests: Vec<Option<WaitSourceCursor>>,
    selection_requests: Vec<(WaitActivationSource, usize)>,
    page_error: Option<DurableError>,
    selection_error: Option<DurableError>,
    selection_override: Option<WaitSelection>,
}

impl ParkedWaitView for ObservedWaitView {
    fn select(
        &mut self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<WaitSelection> {
        self.selection_requests.push((source.clone(), max_targets));
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
        cursor: Option<&WaitSourceCursor>,
        limit: usize,
    ) -> DurableResult<SignalKeyPageOutcome> {
        assert_eq!(limit, 1, "HTTP reads one bounded source at a time");
        self.page_requests.push(cursor.cloned());
        if let Some(error) = self.page_error.take() {
            return Err(error);
        }
        self.waits.signal_key_page(cursor, limit)
    }
}

fn insert_signal_fixture(connection: &Connection, activation_id: &str, signal_key: &str) {
    let request = HttpSignalRequest {
        activation_id: activation_id.to_owned(),
        key: signal_key.to_owned(),
        value: serde_json::json!(true),
    };
    connection
        .execute(
            "INSERT INTO cymule_http_signals(
                activation_id, signal_key, value_json, request_digest,
                selected_wait_ids, acknowledged
             ) VALUES (?1, ?2, ?3, ?4, NULL, 0)",
            params![
                activation_id,
                signal_key,
                b"true",
                cymule_core::canonical_digest(&request).expect("request digest derives"),
            ],
        )
        .expect("signal fixture inserts");
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

fn raw_request(body: impl Into<Body>) -> Request<Body> {
    Request::post("/v1/signals")
        .header("content-type", "application/json")
        .body(body.into())
        .expect("request builds")
}

fn durable_router(capacity: usize) -> (TempDir, axum::Router, SqliteHttpSignalDriver) {
    let directory = tempdir().expect("temporary directory creates");
    let (router, driver) =
        durable_signal_router(directory.path().join("http.sqlite"), capacity, AllowAll)
            .expect("durable router builds");
    (directory, router, driver)
}

async fn receive_pending(
    driver: &mut SqliteHttpSignalDriver,
    view: &mut dyn ParkedWaitView,
) -> cymule_durable::WaitDelivery {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match driver.receive(view, 1) {
                Ok(Some(delivery)) => return delivery.into_delivery(),
                Ok(None) => {}
                Err(DurableError::Conflict {
                    expected: Some(expected),
                    current: Some(current),
                }) if expected == "sqlite-HTTP-writer-available"
                    && current == "sqlite-HTTP-writer-active" => {}
                Err(error) => {
                    panic!("HTTP driver failed outside permitted writer contention: {error}")
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("committed HTTP ingress becomes selectable within the test bound")
}

#[tokio::test]
async fn http_response_waits_for_durable_acknowledgement() {
    let (_directory, router, mut driver) = durable_router(4);
    let response = tokio::spawn(router.oneshot(request()));
    let delivery = receive_pending(&mut driver, &mut index()).await;
    assert!(!response.is_finished());
    assert_eq!(delivery.activation_id, "activation:http");
    assert_eq!(
        driver
            .receive(&mut index(), 1)
            .expect("redelivers")
            .map(cymule_durable::WaitSourceDelivery::into_delivery),
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
async fn nested_duplicate_json_members_are_rejected_before_ingress() {
    let (_directory, router, mut driver) = durable_router(1);
    let response = router
        .oneshot(raw_request(
            r#"{"activation_id":"activation:duplicate","key":"signal:http","value":{"approved":false,"approved":true}}"#,
        ))
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        driver
            .receive(&mut index(), 1)
            .expect("driver polls")
            .is_none()
    );
}

#[tokio::test]
async fn strict_raw_decoder_preserves_the_json_content_type_boundary() {
    let (_directory, router, _driver) = durable_router(1);
    let response = router
        .oneshot(
            Request::post("/v1/signals")
                .body(Body::from(
                    r#"{"activation_id":"activation:no-content-type","key":"signal:http","value":true}"#,
                ))
                .expect("request builds"),
        )
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn raw_signal_body_has_an_explicit_two_mebibyte_limit() {
    let (_directory, router, _driver) = durable_router(1);
    let oversized = format!(
        r#"{{"activation_id":"activation:large","key":"signal:http","value":"{}"}}"#,
        "x".repeat(2 * 1024 * 1024)
    );
    let response = router
        .oneshot(raw_request(oversized))
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn acknowledged_identity_replays_and_conflicting_reuse_fails() {
    let (_directory, router, mut driver) = durable_router(4);
    let first = tokio::spawn(router.clone().oneshot(request()));
    let delivery = receive_pending(&mut driver, &mut index()).await;
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
            .receive(&mut index(), 1)
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
            .receive(&mut index(), 1)
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

#[test]
fn http_spool_rejects_process_local_sqlite_backends() {
    for path in [std::path::Path::new(":memory:"), std::path::Path::new("")] {
        let error = durable_signal_router(path, 1, AllowAll)
            .err()
            .expect("process-local SQLite backend is rejected");
        assert!(matches!(
            error,
            cymule_durable::DurableError::Validation(message)
                if message == "HTTP SQLite spool must be file-backed"
        ));
    }
}

#[tokio::test]
async fn durable_ingress_reopens_with_the_exact_selected_delivery() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (router, mut driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let generation: String = Connection::open(&database)
        .expect("spool opens")
        .query_row(
            "SELECT schema_version FROM cymule_http_spool_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .expect("generation reads");
    assert_eq!(generation, HTTP_SPOOL_SCHEMA_VERSION);
    let first_response = tokio::spawn(router.oneshot(request()));
    let selected = receive_pending(&mut driver, &mut index()).await;
    assert!(!first_response.is_finished());
    first_response.abort();
    let _ = first_response.await;
    drop(driver);

    let mut unavailable = ObservedWaitView {
        page_error: Some(DurableError::NotFound("source view unavailable".to_owned())),
        selection_error: Some(DurableError::NotFound("target view unavailable".to_owned())),
        ..ObservedWaitView::default()
    };
    let (reopened_router, mut reopened) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router reopens");
    let redelivered = reopened
        .receive(&mut unavailable, 1)
        .expect("retained delivery does not require the view")
        .expect("retained delivery exists")
        .into_delivery();
    assert_eq!(redelivered, selected);
    assert!(unavailable.page_requests.is_empty());
    assert!(unavailable.selection_requests.is_empty());
    reopened
        .acknowledge(&redelivered.activation_id)
        .expect("retained delivery acknowledges");
    assert!(
        reopened
            .receive(&mut unavailable.waits, 1)
            .expect("acknowledged ingress drains")
            .is_none()
    );
    assert_eq!(
        reopened_router
            .oneshot(request())
            .await
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

#[test]
fn malformed_http_generation_is_rejected_without_mutation() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("legacy-http.sqlite");
    let connection = Connection::open(&database).expect("legacy spool opens");
    connection
        .execute_batch(
            "CREATE TABLE cymule_http_signals (
                activation_id TEXT PRIMARY KEY NOT NULL,
                signal_key TEXT NOT NULL,
                value_json BLOB NOT NULL,
                request_digest TEXT NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0
             ) STRICT;
             INSERT INTO cymule_http_signals(
                activation_id, signal_key, value_json, request_digest, acknowledged
             ) VALUES (
                'activation:legacy', 'signal:legacy', X'74727565', 'legacy-digest', 0
             );",
        )
        .expect("legacy generation writes");
    drop(connection);
    let before = fs::read(&database).expect("legacy bytes read");

    let Err(error) = durable_signal_router(&database, 4, AllowAll) else {
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

    let connection = Connection::open(&database).expect("legacy spool reopens for inspection");
    let retained: Vec<u8> = connection
        .query_row(
            "SELECT value_json FROM cymule_http_signals
             WHERE activation_id = 'activation:legacy'",
            [],
            |row| row.get(0),
        )
        .expect("legacy payload remains");
    assert_eq!(retained, b"true");
    let meta_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'cymule_http_spool_meta'",
            [],
            |row| row.get(0),
        )
        .expect("schema remains inspectable");
    assert_eq!(meta_count, 0, "rejection must not heal the generation");
}

#[tokio::test]
async fn every_http_handler_connection_revalidates_the_generation() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (router, _driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let invalid_generation = format!("{HTTP_SPOOL_SCHEMA_VERSION}-unsupported");
    let connection = Connection::open(&database).expect("spool opens for corruption");
    connection
        .execute(
            "UPDATE cymule_http_spool_meta SET schema_version = ?1 WHERE singleton = 1",
            [&invalid_generation],
        )
        .expect("generation is changed");
    drop(connection);

    let response = router.oneshot(request()).await.expect("router responds");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let connection = Connection::open(&database).expect("spool reopens for inspection");
    let retained_generation: String = connection
        .query_row(
            "SELECT schema_version FROM cymule_http_spool_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .expect("generation remains");
    assert_eq!(
        retained_generation, invalid_generation,
        "handler connection must not heal the rejected generation"
    );
    let signal_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM cymule_http_signals", [], |row| {
            row.get(0)
        })
        .expect("signal count reads");
    assert_eq!(signal_count, 0, "revalidation must precede ingress writes");
}

#[tokio::test]
async fn durable_sqlite_contention_is_unavailable_not_an_identity_conflict() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (router, _driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let blocker = Connection::open(&database).expect("blocking connection opens");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("blocking writer begins");

    let response = router.oneshot(request()).await.expect("router responds");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    blocker
        .execute_batch("ROLLBACK")
        .expect("blocking writer rolls back");
}

#[tokio::test]
async fn concurrent_exact_durable_waiters_complete_from_one_acknowledgement() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (router, mut driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let first = tokio::spawn(router.clone().oneshot(request()));
    let selected = receive_pending(&mut driver, &mut index()).await;
    let second = tokio::spawn(router.oneshot(request()));
    tokio::task::yield_now().await;
    driver
        .acknowledge(&selected.activation_id)
        .expect("selected delivery acknowledges once");

    for response in [first, second] {
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), response)
                .await
                .expect("durable waiter completes")
                .expect("response task joins")
                .expect("router responds")
                .status(),
            StatusCode::ACCEPTED
        );
    }
}

#[test]
fn durable_ingress_matches_beyond_an_unrelated_1024_record_prefix() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (_router, mut driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let connection = Connection::open(&database).expect("spool opens for fixture insertion");
    let transaction = connection
        .unchecked_transaction()
        .expect("fixture transaction begins");
    for index in 0..1_024 {
        transaction
            .execute(
                "INSERT INTO cymule_http_signals(
                    activation_id, signal_key, value_json, request_digest,
                    selected_wait_ids, acknowledged
                 ) VALUES (?1, ?2, ?3, ?4, NULL, 0)",
                params![
                    format!("activation:prefix:{index:04}"),
                    "signal:unmatched",
                    br#"{"prefix":true}"#,
                    format!("digest:{index:04}"),
                ],
            )
            .expect("unmatched fixture inserts");
    }
    transaction
        .execute(
            "INSERT INTO cymule_http_signals(
                activation_id, signal_key, value_json, request_digest,
                selected_wait_ids, acknowledged
             ) VALUES (?1, ?2, ?3, ?4, NULL, 0)",
            params![
                "activation:target",
                "signal:target",
                br#"{"matched":true}"#,
                "digest:target",
            ],
        )
        .expect("matching fixture inserts");
    transaction.commit().expect("fixture transaction commits");

    let delivery = driver
        .receive(&mut index_for_signal("signal:target"), 1)
        .expect("driver scans indexed signal keys")
        .expect("matching delivery is not prefix-starved");
    assert_eq!(delivery.activation_id, "activation:target");
    assert_eq!(
        delivery.source,
        WaitActivationSource::Signal {
            key: "signal:target".to_owned()
        }
    );
}

#[test]
fn durable_ingress_resets_stale_cursor_and_rotates_across_signal_keys() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (_router, mut driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let connection = Connection::open(&database).expect("spool opens for fixture insertion");
    for (activation_id, signal_key) in [
        ("activation:alpha", "signal:alpha"),
        ("activation:beta", "signal:beta"),
    ] {
        connection
            .execute(
                "INSERT INTO cymule_http_signals(
                    activation_id, signal_key, value_json, request_digest,
                    selected_wait_ids, acknowledged
                 ) VALUES (?1, ?2, ?3, ?4, NULL, 0)",
                params![activation_id, signal_key, b"true", activation_id],
            )
            .expect("signal fixture inserts");
    }
    let mut view = ObservedWaitView {
        waits: index_for_signals(&[("wait:alpha", "signal:alpha"), ("wait:beta", "signal:beta")]),
        ..ObservedWaitView::default()
    };
    let SignalKeyPageOutcome::Page(first_page) = view
        .waits
        .signal_key_page(None, 1)
        .expect("first source page reads")
    else {
        panic!("an empty cursor cannot be stale");
    };
    let expected_cursor = first_page.next_cursor.expect("second source remains");

    let first = driver
        .receive(&mut view, 1)
        .expect("first signal polls")
        .expect("first signal exists");
    driver
        .acknowledge(&first.activation_id)
        .expect("first signal acknowledges");
    let remaining_signal = match &first.source {
        WaitActivationSource::Signal { key } if key == "signal:alpha" => "signal:beta",
        WaitActivationSource::Signal { key } if key == "signal:beta" => "signal:alpha",
        source => panic!("unexpected first source {source:?}"),
    };
    view.waits = index_for_signal(remaining_signal);
    let second = driver
        .receive(&mut view, 1)
        .expect("second signal polls")
        .expect("second signal exists");

    assert_ne!(first.source, second.source);
    assert_eq!(view.page_requests, vec![None, Some(expected_cursor), None]);
    assert_eq!(
        BTreeSet::from([first.activation_id.clone(), second.activation_id.clone(),]),
        BTreeSet::from(["activation:alpha".to_owned(), "activation:beta".to_owned(),])
    );
}

#[test]
fn source_page_errors_preserve_the_cursor_and_fair_rotation() {
    let (directory, _router, mut driver) = durable_router(4);
    let connection =
        Connection::open(directory.path().join("http.sqlite")).expect("spool opens for fixtures");
    for (activation_id, signal_key) in [
        ("activation:alpha:1", "signal:alpha"),
        ("activation:alpha:2", "signal:alpha"),
        ("activation:beta:1", "signal:beta"),
        ("activation:beta:2", "signal:beta"),
    ] {
        insert_signal_fixture(&connection, activation_id, signal_key);
    }
    let mut view = ObservedWaitView {
        waits: index_for_signals(&[("wait:alpha", "signal:alpha"), ("wait:beta", "signal:beta")]),
        ..ObservedWaitView::default()
    };
    let SignalKeyPageOutcome::Page(page) = view.waits.signal_key_page(None, 1).expect("page reads")
    else {
        panic!("an empty cursor cannot be stale");
    };
    let expected_cursor = page.next_cursor.expect("second source remains");
    let first = driver
        .receive(&mut view, 1)
        .expect("polls")
        .expect("selects");
    driver
        .acknowledge(&first.activation_id)
        .expect("acknowledges");

    for error in [
        DurableError::Substrate {
            code: "source_read_failed".to_owned(),
            message: "injected".to_owned(),
        },
        DurableError::Integrity {
            code: "source_proof_invalid".to_owned(),
            message: "injected".to_owned(),
        },
        DurableError::Validation("invalid source cursor".to_owned()),
        DurableError::Conflict {
            expected: Some("expected".to_owned()),
            current: Some("current".to_owned()),
        },
    ] {
        let before = view.page_requests.len();
        view.page_error = Some(error.clone());
        assert_eq!(driver.receive(&mut view, 1), Err(error));
        assert_eq!(
            view.page_requests.len(),
            before + 1,
            "errors must not retry from the first page"
        );
        assert_eq!(
            view.page_requests.last(),
            Some(&Some(expected_cursor.clone()))
        );
    }

    let second = driver
        .receive(&mut view, 1)
        .expect("recovers")
        .expect("selects");
    assert_ne!(
        first.source, second.source,
        "earlier-source backlog cannot starve the next key"
    );
    assert_eq!(view.page_requests.last(), Some(&Some(expected_cursor)));
}

#[test]
fn source_scan_limit_retains_the_exact_cursor_for_the_next_poll() {
    let (directory, _router, mut driver) = durable_router(1);
    let sources = (0..1_025)
        .map(|number| (format!("wait:{number}"), format!("signal:{number}")))
        .collect::<Vec<_>>();
    let borrowed = sources
        .iter()
        .map(|(wait, key)| (wait.as_str(), key.as_str()))
        .collect::<Vec<_>>();
    let mut view = ObservedWaitView {
        waits: index_for_signals(&borrowed),
        ..ObservedWaitView::default()
    };
    let SignalKeyPageOutcome::Page(page) = view
        .waits
        .signal_key_page(None, sources.len())
        .expect("sources read")
    else {
        panic!("an empty cursor cannot be stale");
    };
    let target_key = page.keys.last().expect("last source exists");
    let connection = Connection::open(directory.path().join("http.sqlite")).expect("spool opens");
    insert_signal_fixture(&connection, "activation:last-source", target_key);

    assert!(
        driver
            .receive(&mut view, 1)
            .expect("bounded scan succeeds")
            .is_none()
    );
    assert_eq!(view.page_requests.len(), 1_024);
    assert!(view.selection_requests.is_empty());
    let delivery = driver
        .receive(&mut view, 1)
        .expect("scan continues")
        .expect("last source selects");
    assert_eq!(delivery.activation_id, "activation:last-source");
    assert_eq!(view.page_requests.len(), 1_025);
    assert!(
        view.page_requests
            .last()
            .expect("last request exists")
            .is_some()
    );
    assert_eq!(
        view.selection_requests,
        vec![(
            WaitActivationSource::Signal {
                key: target_key.clone()
            },
            1
        )]
    );
}

#[test]
fn target_selection_error_leaves_ingress_unselected_and_unacknowledged() {
    let (directory, _router, mut driver) = durable_router(1);
    let connection = Connection::open(directory.path().join("http.sqlite")).expect("spool opens");
    insert_signal_fixture(&connection, "activation:http", "signal:http");
    let error = DurableError::Integrity {
        code: "target_proof_invalid".to_owned(),
        message: "injected".to_owned(),
    };
    let mut view = ObservedWaitView {
        waits: index(),
        selection_error: Some(error.clone()),
        ..ObservedWaitView::default()
    };
    assert_eq!(driver.receive(&mut view, 1), Err(error));
    let durable_state: (bool, bool) = connection.query_row(
        "SELECT selected_wait_ids IS NULL, acknowledged FROM cymule_http_signals WHERE activation_id = 'activation:http'",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).expect("selection and acknowledgement read");
    assert_eq!(durable_state, (true, false));
    assert_eq!(
        driver
            .receive(&mut view, 1)
            .expect("selection recovers")
            .expect("selects")
            .activation_id,
        "activation:http"
    );
    assert_eq!(view.selection_requests.len(), 2);
    assert!(view.selection_requests.iter().all(|(_, limit)| *limit == 1));
}

#[test]
fn oversized_new_selection_is_rejected_before_it_can_become_retained() {
    let (directory, _router, mut driver) = durable_router(1);
    let connection = Connection::open(directory.path().join("http.sqlite")).expect("spool opens");
    insert_signal_fixture(&connection, "activation:http", "signal:http");
    let mut view = ObservedWaitView {
        waits: index(),
        selection_override: Some(WaitSelection {
            wait_ids: BTreeSet::from(["wait:first".to_owned(), "wait:second".to_owned()]),
            remaining: 0,
        }),
        ..ObservedWaitView::default()
    };

    assert!(matches!(
        driver.receive(&mut view, 1),
        Err(DurableError::Validation(message))
            if message == "new HTTP delivery has 2 targets outside requested bound 1"
    ));
    let durable_state: (bool, bool) = connection
        .query_row(
            "SELECT selected_wait_ids IS NULL, acknowledged
             FROM cymule_http_signals WHERE activation_id = 'activation:http'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("selection and acknowledgement read");
    assert_eq!(durable_state, (true, false));
}

#[test]
fn retained_selected_value_rejects_nested_duplicate_members() {
    let directory = tempdir().expect("temporary directory creates");
    let database = directory.path().join("http.sqlite");
    let (_router, mut driver) =
        durable_signal_router(&database, 4, AllowAll).expect("durable router builds");
    let connection = Connection::open(&database).expect("spool opens for fixture insertion");
    let selected = cymule_core::canonical_bytes(&BTreeSet::from(["wait:http".to_owned()]))
        .expect("selected targets encode");
    connection
        .execute(
            "INSERT INTO cymule_http_signals(
                activation_id, signal_key, value_json, request_digest,
                selected_wait_ids, acknowledged
             ) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
            params![
                "activation:retained-duplicate",
                "signal:http",
                br#"{"approved":false,"approved":true}"#,
                "digest:retained-duplicate",
                selected,
            ],
        )
        .expect("selected fixture inserts");

    assert!(matches!(
        driver.receive(&mut index(), 1),
        Err(cymule_durable::DurableError::Integrity { .. })
    ));
}
