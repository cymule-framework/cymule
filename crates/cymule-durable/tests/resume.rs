//! Restart-level public resumability conformance.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

use std::collections::BTreeSet;

use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableResponse, DurableStoreControl,
    MemoryStore,
};
use cymule_durable_protocol::{ContinuationStatus, WaitActivationSource};
use serde_json::json;

use support::{EmptyPlugin, empty_binding, execution, open_control, signal_candidate};

#[test]
fn wait_activation_and_resume_survive_two_independent_reopens() {
    let run_id = "run:resume";
    let candidate = signal_candidate("resume", "signal:resume", true);
    let input = json!({"message": "resume exactly"});
    let mut first = open_control(MemoryStore::new(), EmptyPlugin, empty_binding())
        .expect("initial runtime opens");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = first
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: candidate.clone(),
            input: input.clone(),
            execution: execution(run_id),
        })
        .expect("Run parks")
    else {
        panic!("wait Run did not suspend")
    };
    let (store, _) = first.into_parts();

    let activation = DurableCommand::ActivateWait {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        activation_id: "activation:resume".to_owned(),
        source: WaitActivationSource::Signal {
            key: "signal:resume".to_owned(),
        },
        wait_ids: BTreeSet::from([wait_id]),
        value: input.clone(),
    };
    let mut store_only = DurableStoreControl::open(store).expect("store-only control opens");
    let first_receipt = store_only
        .submit(activation.clone())
        .expect("activation commits");
    let DurableResponse::WaitActivated { receipt } = &first_receipt else {
        panic!("activation returned another response")
    };
    let committed_activation = receipt.clone();
    let replay = store_only
        .submit(activation)
        .expect("identical activation replays");
    let DurableResponse::WaitActivated { receipt: replayed } = replay else {
        panic!("activation replay returned another response")
    };
    assert_eq!(replayed, committed_activation);
    let store = store_only.into_store();

    let mut resumed = open_control(store, EmptyPlugin, empty_binding()).expect("runtime reopens");
    let value = support::expect_completed_value(
        resumed
            .submit(DurableCommand::ResumeRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.to_owned(),
                execution: execution(run_id),
            })
            .expect("ready Run resumes"),
    );
    assert_eq!(value, input);
    let replay = support::expect_completed_value(
        resumed
            .submit(DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.to_owned(),
                candidate,
                input: input.clone(),
                execution: execution(run_id),
            })
            .expect("identical terminal start replays"),
    );
    assert_eq!(replay, input);
    let (store, _) = resumed.into_parts();

    let mut reads = DurableStoreControl::open(store).expect("read authority reopens");
    let query = DurableCommand::RunCurrent {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_revision: None,
    };
    let DurableResponse::RunCurrent {
        current: Some(current),
        ..
    } = reads.submit(query).expect("terminal Run reads")
    else {
        panic!("terminal Run current is absent")
    };
    assert_eq!(current.continuation_status, ContinuationStatus::Completed);
}
