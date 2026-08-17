//! Restart and stale-writer tests for the directory store adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, Definition, Expression, Machine, PlanCandidate,
    Region,
};
use cymule_directory_store::DirectoryStore;
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, DurableError, FrameState,
};
use serde_json::json;

static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn test_directory() -> PathBuf {
    let sequence = DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cymule-directory-store-{}-{sequence}",
        std::process::id()
    ))
}

fn machine_with_run() -> (Machine, String) {
    let mut machine = Machine::new();
    let plan = machine
        .seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "directory_store_test".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Literal { value: json!(null) },
                },
            }],
            metadata: BTreeMap::new(),
        })
        .expect("plan seals");
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:start".to_owned(),
            actor: "test".to_owned(),
            run_id: "run:directory".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: "binding:directory/1".to_owned(),
            },
        })
        .expect("run starts");
    (machine, plan.plan_id)
}

fn continuation(plan_id: String) -> Continuation {
    Continuation {
        run_id: "run:directory".to_owned(),
        plan_id,
        binding_context: "binding:directory/1".to_owned(),
        frames: vec![FrameState {
            invocation_id: "main".to_owned(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: None,
        wait_set: BTreeSet::new(),
        scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Ready,
    }
}

#[test]
fn committed_state_reopens_and_stale_writer_is_rejected() {
    let directory = test_directory();
    let (machine, plan_id) = machine_with_run();
    let mut current = DurableCoordinator::open(DirectoryStore::open(&directory).expect("opens"))
        .expect("coordinator opens")
        .initialize(&machine)
        .expect("initializes");
    let mut stale = DurableCoordinator::open(DirectoryStore::open(&directory).expect("opens"))
        .expect("second coordinator opens");
    current
        .put_continuation(continuation(plan_id))
        .expect("continuation commits");
    assert!(matches!(
        stale.persist_machine(&machine),
        Err(DurableError::Conflict { .. })
    ));

    fs::write(directory.join("state.next"), b"interrupted staging bytes")
        .expect("staging residue writes");
    let reopened = DurableCoordinator::open(DirectoryStore::open(&directory).expect("opens"))
        .expect("committed state reopens");
    assert!(
        reopened
            .state()
            .expect("state")
            .continuations
            .contains_key("run:directory")
    );
    fs::remove_dir_all(directory).expect("test directory removes");
}
