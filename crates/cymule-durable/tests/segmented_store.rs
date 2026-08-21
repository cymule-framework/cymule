//! Property tests for authenticated segmented-store replay.

use cymule_core::Machine;
use cymule_durable::{
    DurableDelta, DurableOperation, DurableState, DurableStore, JournalRecord,
    MAX_CHECKPOINT_PACKS, MAX_HOT_SEGMENTS, MemoryStore, StoreBatch,
};
use proptest::prelude::*;
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct CountingStore {
    inner: MemoryStore,
    loads: Arc<Mutex<usize>>,
    commit_bytes: Arc<Mutex<Vec<usize>>>,
}

impl DurableStore for CountingStore {
    fn load(&mut self) -> cymule_durable::DurableResult<Option<cymule_durable::StoredState>> {
        *self.loads.lock().expect("load counter") += 1;
        self.inner.load()
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&cymule_durable::StoreHead>,
        batch: &StoreBatch,
    ) -> cymule_durable::DurableResult<cymule_durable::StoreCommit> {
        let bytes = batch
            .segment()
            .map(cymule_core::canonical_bytes)
            .transpose()?
            .map_or(0, |value| value.len())
            + batch
                .checkpoint()
                .map(cymule_core::canonical_bytes)
                .transpose()?
                .map_or(0, |value| value.len())
            + cymule_core::canonical_bytes(batch.head())?.len();
        self.commit_bytes.lock().expect("byte counter").push(bytes);
        self.inner.compare_and_commit(expected, batch)
    }
}

#[test]
fn fixed_size_journal_append_has_history_independent_hot_work() {
    let loads = Arc::new(Mutex::new(0));
    let commit_bytes = Arc::new(Mutex::new(Vec::new()));
    let store = CountingStore {
        inner: MemoryStore::new(),
        loads: Arc::clone(&loads),
        commit_bytes: Arc::clone(&commit_bytes),
    };
    let mut coordinator = cymule_durable::DurableCoordinator::open(store)
        .expect("opens")
        .initialize(&Machine::new())
        .expect("initializes");
    for index in 0..128 {
        coordinator
            .append_journal_record(
                "journal:complexity",
                JournalRecord::new(
                    format!("record:{index:04}"),
                    "test.fixed/1",
                    json!({"value": 7}),
                )
                .expect("record"),
            )
            .expect("append");
    }
    assert_eq!(*loads.lock().expect("load counter"), 1);
    let bytes = commit_bytes.lock().expect("byte counter");
    let hot = &bytes[1..];
    assert!(hot.iter().all(|value| *value < 4_096));
    assert!(hot.iter().max().expect("max") - hot.iter().min().expect("min") < 512);
}

#[test]
fn automatic_materialized_bases_bound_manifest_recovery() {
    let mut store = MemoryStore::new();
    let initial = StoreBatch::initialize(DurableState::new(Machine::new().snapshot()))
        .expect("initial batch");
    store
        .compare_and_commit(None, &initial)
        .expect("initial commit");
    let mut current = store.load().expect("loads").expect("state");
    let commits = MAX_HOT_SEGMENTS * MAX_CHECKPOINT_PACKS + 7;
    for index in 0..commits {
        let record = JournalRecord::new(
            format!("record:{index:04}"),
            "test.bounded-manifest/1",
            json!({"value": 7}),
        )
        .expect("record");
        let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:bounded-manifest".to_owned(),
            records: vec![record],
        }])
        .expect("delta");
        let batch = StoreBatch::transition(&current, delta).expect("transition");
        let commit = store
            .compare_and_commit(Some(&current.head), &batch)
            .expect("commit");
        batch
            .apply_committed(&mut current, &commit)
            .expect("local projection advances");
    }
    let reopened = store.load().expect("reopens").expect("state");
    assert_eq!(
        reopened.state.application_journals["journal:bounded-manifest"].len(),
        commits as usize
    );
    assert!(reopened.head.checkpoint_depth < MAX_CHECKPOINT_PACKS);
    assert!(
        store.stats().expect("stats").reopened_segments < MAX_HOT_SEGMENTS * MAX_CHECKPOINT_PACKS
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]

    #[test]
    fn arbitrary_journal_deltas_reopen_exactly_and_remain_bounded(values in prop::collection::vec(any::<u16>(), 1..40)) {
        let mut store = MemoryStore::new();
        let batch = StoreBatch::initialize(DurableState::new(Machine::new().snapshot())).expect("initial batch");
        store.compare_and_commit(None, &batch).expect("initial commit");
        let mut current = store.load().expect("loads").expect("state");
        for (index, value) in values.iter().enumerate() {
            let record = JournalRecord::new(
                    format!("record:{index}"),
                    "test.segment-property/1",
                    json!({"value": value}),
                ).expect("record");
            let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
                journal_id: "journal:property".to_owned(), records: vec![record],
            }]).expect("delta");
            let batch = StoreBatch::transition(&current, delta).expect("transition");
            store.compare_and_commit(Some(&current.head), &batch).expect("commit");
            current = store.load().expect("loads").expect("state");
            prop_assert!(current.head.suffix_len < MAX_HOT_SEGMENTS);
        }
        let reopened = store.load().expect("reopens").expect("state");
        prop_assert_eq!(
            reopened.state.application_journals["journal:property"].len(),
            values.len()
        );
        prop_assert!(store.stats().expect("stats").reopened_segments < MAX_HOT_SEGMENTS);
    }
}
