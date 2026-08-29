//! Bounded parked-wait index selection conformance.

use std::collections::{BTreeMap, BTreeSet};

use cymule_durable::{
    DurableState, ParkedWaitIndex, SignalKeyPageOutcome, WaitCondition, WaitDelivery, WaitKind,
    WaitState,
};
use cymule_durable_protocol::{
    CONTINUATION_STATE_VERSION, Continuation, ContinuationStatus, FrameState, WaitActivationSource,
    WaitOwner,
};
use serde_json::json;

fn continuation(wait_ids: &BTreeSet<String>) -> Continuation {
    Continuation {
        continuation_version: CONTINUATION_STATE_VERSION.to_owned(),
        run_id: "run:index".to_owned(),
        plan_id: format!("sha256:{}", "1".repeat(64)),
        binding_context: format!("sha256:{}", "2".repeat(64)),
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            invocation_path: Vec::new(),
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            input: cymule_core::ArtifactRef {
                identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
                artifact_id: format!("sha256:{}", "0".repeat(64)),
                kind: "test/input".to_owned(),
            },
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: None,
        wait_set: wait_ids.clone(),
        scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
        epoch: 0,
        execution_fence: 0,
        execution_claim: None,
        status: ContinuationStatus::Waiting,
    }
}

fn wait_owner() -> WaitOwner {
    WaitOwner {
        invocation_id: "main".to_owned(),
        definition_id: "main".to_owned(),
        site_id: "wait.signal".to_owned(),
        region_path: Vec::new(),
        step_index: 0,
        bind: None,
    }
}

fn wait_id(index: usize) -> String {
    format!("sha256:{index:064x}")
}

#[test]
fn parked_index_selects_bounded_signal_and_exact_timer_candidates() {
    let wait_ids = (1..=6).map(wait_id).collect::<BTreeSet<_>>();
    let mut state = DurableState::new(cymule_core::Machine::new().snapshot());
    state
        .continuations
        .insert("run:index".to_owned(), continuation(&wait_ids));
    for (wait_id, consume_once) in [
        (wait_id(1), false),
        (wait_id(2), false),
        (wait_id(3), true),
        (wait_id(4), true),
    ] {
        state.waits.insert(
            wait_id.clone(),
            WaitCondition {
                wait_id,
                run_id: "run:index".to_owned(),
                kind: WaitKind::Signal {
                    key: "signal:batch".to_owned(),
                },
                consume_once,
                owner: wait_owner(),
                state: WaitState::Pending,
                result: None,
            },
        );
    }
    for wait_id in [wait_id(5), wait_id(6)] {
        state.waits.insert(
            wait_id.clone(),
            WaitCondition {
                wait_id,
                run_id: "run:index".to_owned(),
                kind: WaitKind::Timer {
                    timer_id: "timer:batch".to_owned(),
                },
                consume_once: false,
                owner: wait_owner(),
                state: WaitState::Pending,
                result: None,
            },
        );
    }

    let index = ParkedWaitIndex::rebuild(&state).expect("index rebuilds");
    let source = WaitActivationSource::Signal {
        key: "signal:batch".to_owned(),
    };
    let selected = index.select(&source, 2).expect("signal selects");
    assert_eq!(selected.wait_ids, BTreeSet::from([wait_id(1), wait_id(3)]));
    assert_eq!(selected.remaining, 2);
    index
        .validate_delivery(&WaitDelivery {
            activation_id: "delivery:signal".to_owned(),
            source,
            wait_ids: selected.wait_ids,
            value: json!({"ok": true}),
        })
        .expect("selected signal targets validate");

    let timer = index
        .select(
            &WaitActivationSource::Timer {
                timer_id: "timer:batch".to_owned(),
            },
            8,
        )
        .expect("timer selects");
    assert_eq!(timer.wait_ids, BTreeSet::from([wait_id(5)]));
    assert_eq!(timer.remaining, 1);
}

#[test]
fn signal_key_cursor_pages_beyond_one_thousand_active_sources() {
    let mut state = DurableState::new(cymule_core::Machine::new().snapshot());
    let wait_ids = (0..1_025).map(wait_id).collect::<BTreeSet<_>>();
    state
        .continuations
        .insert("run:index".to_owned(), continuation(&wait_ids));
    for index in 0..1_025 {
        let wait_id = wait_id(index);
        state.waits.insert(
            wait_id.clone(),
            WaitCondition {
                wait_id,
                run_id: "run:index".to_owned(),
                kind: WaitKind::Signal {
                    key: format!("signal:cursor:{index:04}"),
                },
                consume_once: true,
                owner: wait_owner(),
                state: WaitState::Pending,
                result: None,
            },
        );
    }

    let index = ParkedWaitIndex::rebuild(&state).expect("index rebuilds");
    let SignalKeyPageOutcome::Page(first) = index.signal_key_page(None, 1_024).expect("first page")
    else {
        panic!("cursor-free page cannot be stale");
    };
    assert_eq!(first.keys.len(), 1_024);
    assert_eq!(first.remaining, 1);
    let SignalKeyPageOutcome::Page(second) = index
        .signal_key_page(first.next_cursor.as_ref(), 1_024)
        .expect("cursor advances")
    else {
        panic!("same-root cursor cannot be stale");
    };
    assert_eq!(second.keys.len(), 1);
    let all = first
        .keys
        .into_iter()
        .chain(second.keys)
        .collect::<BTreeSet<_>>();
    assert_eq!(all.len(), 1_025);
    assert!(all.contains("signal:cursor:1024"));
}

#[test]
fn signal_key_cursor_reports_typed_stale_after_authority_changes() {
    let first_wait_ids = BTreeSet::from([wait_id(1), wait_id(4)]);
    let mut first_state = DurableState::new(cymule_core::Machine::new().snapshot());
    first_state
        .continuations
        .insert("run:index".to_owned(), continuation(&first_wait_ids));
    for (wait_id, key) in [
        (wait_id(1), "signal:before-a"),
        (wait_id(4), "signal:before-b"),
    ] {
        first_state.waits.insert(
            wait_id.clone(),
            WaitCondition {
                wait_id,
                run_id: "run:index".to_owned(),
                kind: WaitKind::Signal {
                    key: key.to_owned(),
                },
                consume_once: true,
                owner: wait_owner(),
                state: WaitState::Pending,
                result: None,
            },
        );
    }
    let first = ParkedWaitIndex::rebuild(&first_state).expect("first index rebuilds");
    let SignalKeyPageOutcome::Page(page) =
        first.signal_key_page(None, 1).expect("first page reads")
    else {
        panic!("cursor-free page cannot be stale");
    };
    let cursor = page
        .next_cursor
        .expect("two-key fixture retains an authenticated cursor");

    let second_wait_ids = BTreeSet::from([wait_id(2), wait_id(3)]);
    let mut second_state = DurableState::new(cymule_core::Machine::new().snapshot());
    second_state
        .continuations
        .insert("run:index".to_owned(), continuation(&second_wait_ids));
    for (wait_id, key) in [
        (wait_id(2), "signal:after-a"),
        (wait_id(3), "signal:after-b"),
    ] {
        second_state.waits.insert(
            wait_id.clone(),
            WaitCondition {
                wait_id,
                run_id: "run:index".to_owned(),
                kind: WaitKind::Signal {
                    key: key.to_owned(),
                },
                consume_once: true,
                owner: wait_owner(),
                state: WaitState::Pending,
                result: None,
            },
        );
    }
    let second = ParkedWaitIndex::rebuild(&second_state).expect("second index rebuilds");
    assert!(matches!(
        second.signal_key_page(Some(&cursor), 1),
        Ok(SignalKeyPageOutcome::Stale { .. })
    ));
}
