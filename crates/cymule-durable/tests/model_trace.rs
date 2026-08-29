//! Deterministic public command sequence checked against an independent Run set.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

use std::collections::BTreeSet;

use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableResponse, DurableStoreControl,
    MAX_DURABLE_QUERY_PAGE_BYTES, MemoryStore,
};
use serde_json::json;

use support::{EmptyPlugin, empty_binding, execution, identity_candidate, open_control};

#[test]
fn generated_reopen_trace_preserves_exact_run_index_without_duplicates_or_omissions() {
    const RUNS: usize = 19;
    let mut expected = BTreeSet::new();
    let mut store = MemoryStore::new();
    for index in 0..RUNS {
        let run_id = format!("run:trace:{index:02}");
        expected.insert(run_id.clone());
        let mut runtime = open_control(store, EmptyPlugin, empty_binding())
            .expect("trace runtime opens from exact head");
        let response = runtime
            .submit(DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.clone(),
                candidate: identity_candidate(&format!("trace-{index}")),
                input: json!({"index": index}),
                execution: execution(&run_id),
            })
            .expect("generated Run starts");
        assert!(matches!(
            response,
            DurableResponse::RunBoundary {
                boundary: DurableBoundary::Completed { .. }
            }
        ));
        (store, _) = runtime.into_parts();
    }

    let mut control = DurableStoreControl::open(store).expect("trace read authority opens");
    let mut actual = BTreeSet::new();
    let mut cursor = None;
    let mut revision = None;
    loop {
        let query = DurableCommand::RunIndexPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            expected_revision: revision.clone(),
            cursor,
            limit: 4,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        };
        let response = control.submit(query.clone()).expect("trace page reads");
        response
            .verify_query_for(&query)
            .expect("trace page binds exact cursor and revision");
        let DurableResponse::RunIndexPage { page } = response else {
            panic!("Run-index trace returned another response")
        };
        if let Some(pinned) = &revision {
            assert_eq!(pinned, &page.observed_revision);
        } else {
            revision = Some(page.observed_revision.clone());
        }
        for item in page.items {
            assert!(actual.insert(item.run_id), "Run index repeated an identity");
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(actual, expected);
}
