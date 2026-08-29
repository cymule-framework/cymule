//! Public incremental `StateRoot` persistence cost checks.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

use cymule_authenticated_collections::{MAX_LOG_HEIGHT, MAX_MAP_PATH_NODES};
use cymule_durable::{DURABLE_CONTROL_VERSION, DurableCommand, DurableStore, MemoryStore};
use serde_json::json;

use support::{EmptyPlugin, empty_binding, execution, identity_candidate, open_control};

#[test]
fn one_new_run_copies_only_bounded_authenticated_paths() {
    let mut store = MemoryStore::new();
    let mut previous_objects = 0_u64;
    for index in 0..12 {
        let run_id = format!("run:incremental:{index}");
        let probe = store.clone();
        let mut runtime = open_control(store, EmptyPlugin, empty_binding()).expect("runtime opens");
        runtime
            .submit(DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.clone(),
                candidate: identity_candidate(&format!("incremental-{index}")),
                input: json!({"index": index}),
                execution: execution(&run_id),
            })
            .expect("Run starts");
        (store, _) = runtime.into_parts();
        let stats = probe.stats().expect("store stats read");
        let introduced = stats
            .state_root_objects
            .checked_sub(previous_objects)
            .expect("immutable object count is monotonic before GC");
        let conservative_path_bound =
            u64::try_from(16 * MAX_MAP_PATH_NODES + 8 * MAX_LOG_HEIGHT + 1_024)
                .expect("test bound fits u64");
        assert!(
            introduced <= conservative_path_bound,
            "one Run introduced {introduced} objects, above the bounded path envelope {conservative_path_bound}"
        );
        previous_objects = stats.state_root_objects;
    }
}
