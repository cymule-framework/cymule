//! Public exact-head coordinator behavior across reopen.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

use cymule_core::RunExecutionStatus;
use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableResponse, DurableStoreControl,
    MAX_DURABLE_QUERY_PAGE_BYTES, MemoryStore,
};
use cymule_durable_protocol::ContinuationStatus;
use serde_json::json;

use support::{
    EmptyPlugin, empty_binding, execution, identity_candidate, open_control, signal_candidate,
};

fn start(run_id: &str, name: &str) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate: identity_candidate(name),
        input: json!({"run": run_id}),
        execution: execution(run_id),
    }
}

#[test]
fn second_run_starts_from_exact_pinned_manifest_after_reopen() {
    let mut first = open_control(MemoryStore::new(), EmptyPlugin, empty_binding())
        .expect("first runtime opens");
    let first_response = first
        .submit(start("run:first", "first"))
        .expect("first starts");
    assert!(matches!(
        first_response,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. }
        }
    ));
    let (store, _) = first.into_parts();

    let mut second = open_control(store, EmptyPlugin, empty_binding()).expect("runtime reopens");
    let second_response = second
        .submit(start("run:second", "second"))
        .expect("second Run starts without full-domain hydration");
    assert!(matches!(
        second_response,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. }
        }
    ));
    let (store, _) = second.into_parts();

    let mut reads = DurableStoreControl::open(store).expect("read-only authority opens");
    let first_page = DurableCommand::RunIndexPage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        expected_revision: None,
        cursor: None,
        limit: 1,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    };
    let DurableResponse::RunIndexPage { page } = reads
        .submit(first_page.clone())
        .expect("first bounded index page reads")
    else {
        panic!("Run index returned another response")
    };
    assert_eq!(page.items.len(), 1);
    let cursor = page.next_cursor.expect("two Runs require a second page");
    let second_page = DurableCommand::RunIndexPage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        expected_revision: Some(page.observed_revision.clone()),
        cursor: Some(cursor),
        limit: 1,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    };
    let DurableResponse::RunIndexPage { page: tail } = reads
        .submit(second_page)
        .expect("second bounded index page reads")
    else {
        panic!("Run index tail returned another response")
    };
    assert_eq!(tail.observed_revision, page.observed_revision);
    assert_eq!(tail.items.len(), 1);
    assert!(tail.next_cursor.is_none());
    let ids = page
        .items
        .into_iter()
        .chain(tail.items)
        .map(|item| item.run_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        ids,
        std::collections::BTreeSet::from(["run:first".to_owned(), "run:second".to_owned()])
    );
}

#[test]
fn store_only_cancellation_projects_one_terminal_run_current() {
    let run_id = "run:cancelled";
    let mut runtime =
        open_control(MemoryStore::new(), EmptyPlugin, empty_binding()).expect("runtime opens");
    let response = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: signal_candidate("cancelled", "signal:cancelled", true),
            input: json!({"pending": true}),
            execution: execution(run_id),
        })
        .expect("Run parks");
    assert!(matches!(
        response,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Suspended { .. }
        }
    ));
    let (store, _) = runtime.into_parts();
    let mut control = DurableStoreControl::open(store).expect("store-only control opens");
    let cancelled = control
        .submit(DurableCommand::CancelRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            cancellation_id: "cancel:terminal".to_owned(),
            run_id: run_id.to_owned(),
            reason: json!({"code": "operator_cancelled"}),
        })
        .expect("cancellation commits without a provider");
    assert!(matches!(cancelled, DurableResponse::RunCancelled { .. }));

    let query = DurableCommand::RunCurrent {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_revision: None,
    };
    let response = control.submit(query.clone()).expect("cancelled Run reads");
    response
        .verify_query_for(&query)
        .expect("Run-current response verifies");
    let DurableResponse::RunCurrent {
        current: Some(current),
        ..
    } = response
    else {
        panic!("cancelled Run current is absent")
    };
    assert_eq!(current.continuation_status, ContinuationStatus::Cancelled);
    assert!(matches!(
        current.execution_status,
        RunExecutionStatus::Cancelled { .. }
    ));
}
