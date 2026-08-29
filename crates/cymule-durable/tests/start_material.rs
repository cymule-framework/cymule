//! Public Start admission respects the raw Artifact bound, not helper duplication.

/// Shared public-control fixtures and issued current-head Clock authority.
pub mod support;

use cymule_durable::{DURABLE_CONTROL_VERSION, DurableCommand, DurableStore, MemoryStore};
use serde_json::Value;

use support::{EmptyPlugin, empty_binding, execution, identity_candidate, open_control};

#[test]
fn five_mebibyte_input_starts_and_replays_without_material_size_conflation() {
    let run_id = "run:start:five-mebibytes";
    let candidate = identity_candidate("start-five-mebibytes");
    let input = Value::String("x".repeat(5 * 1024 * 1024));
    let raw = cymule_core::canonical_bytes(&input).expect("input is canonical JSON");
    assert_eq!(raw.len(), 5 * 1024 * 1024 + 2);
    assert!(raw.len() <= cymule_core::MAX_ARTIFACT_BYTES);
    let mut runtime = open_control(MemoryStore::new(), EmptyPlugin, empty_binding())
        .expect("runtime admits its exact provider before writable open");
    let first = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: candidate.clone(),
            input: input.clone(),
            execution: execution(run_id),
        })
        .expect("a legal five-MiB input is not rejected by duplicated internal helper bytes");
    assert!(
        support::expect_completed_value(first) == input,
        "the exact large value is retained"
    );
    let (mut store, _) = runtime.into_parts();
    let committed = store
        .load_head()
        .expect("head reads")
        .expect("Run committed");
    let observer = store.clone();
    let mut reopened = open_control(store, EmptyPlugin, empty_binding()).expect("runtime reopens");
    let replay = reopened
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate,
            input: input.clone(),
            execution: execution(run_id),
        })
        .expect("the original large input replays from its exact retained identity");
    assert!(
        support::expect_completed_value(replay) == input,
        "replay preserves all input bytes"
    );
    assert_eq!(
        observer.clone().load_head().expect("head rereads"),
        Some(committed)
    );
}
