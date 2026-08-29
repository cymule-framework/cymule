//! Public Durable control/query wire conformance.

use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableCommand, DurableResponse, DurableStoreControl,
    MAX_DURABLE_QUERY_PAGE_BYTES, MAX_DURABLE_QUERY_PAGE_ITEMS, MemoryStore,
};
use serde_json::json;

#[test]
fn control_four_rejects_removed_query_and_ambient_provider_shapes() {
    for removed in [
        json!({
            "type": "query_run",
            "control_version": "cymule.durable-control/3",
            "query_id": "query:removed",
            "run_id": "run:removed"
        }),
        json!({
            "type": "query_domain",
            "control_version": "cymule.durable-control/3",
            "query_id": "query:removed"
        }),
        json!({
            "type": "resume_run",
            "control_version": DURABLE_CONTROL_VERSION,
            "run_id": "run:ambient-provider",
            "provider": "must-not-cross-control"
        }),
    ] {
        assert!(serde_json::from_value::<DurableCommand>(removed).is_err());
    }
}

#[test]
fn query_page_requires_explicit_nulls_and_closed_budgets() {
    let command = DurableCommand::RunIndexPage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        expected_revision: None,
        cursor: None,
        limit: MAX_DURABLE_QUERY_PAGE_ITEMS,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    };
    command.verify().expect("maximum bounded query verifies");
    let mut encoded = serde_json::to_value(&command).expect("query serializes");
    assert_eq!(encoded["expected_revision"], serde_json::Value::Null);
    assert_eq!(encoded["cursor"], serde_json::Value::Null);
    encoded
        .as_object_mut()
        .expect("query is an object")
        .remove("cursor");
    assert!(serde_json::from_value::<DurableCommand>(encoded).is_err());

    let oversized = DurableCommand::RunIndexPage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        expected_revision: None,
        cursor: None,
        limit: MAX_DURABLE_QUERY_PAGE_ITEMS + 1,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    };
    assert!(oversized.verify().is_err());
}

#[test]
fn initialized_domain_has_one_revision_pinned_empty_run_page() {
    let mut control = DurableStoreControl::initialize(MemoryStore::new())
        .expect("parameter-free durable genesis initializes");
    let command = DurableCommand::RunIndexPage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        expected_revision: None,
        cursor: None,
        limit: 1,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    };
    let response = control.submit(command.clone()).expect("empty index reads");
    response
        .verify_query_for(&command)
        .expect("response binds exact query authority");
    let DurableResponse::RunIndexPage { page } = response else {
        panic!("Run-index query returned another response")
    };
    assert!(page.items.is_empty());
    assert!(page.next_cursor.is_none());

    let stale = DurableCommand::RunIndexPage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        expected_revision: Some(
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned(),
        ),
        cursor: None,
        limit: 1,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    };
    assert!(control.submit(stale).is_err());
}
