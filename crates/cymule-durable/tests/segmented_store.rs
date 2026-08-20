//! Property tests for authenticated segmented-store replay.

use cymule_core::Machine;
use cymule_durable::{
    DurableState, DurableStore, JournalRecord, MAX_HOT_SEGMENTS, MemoryStore, StoreBatch,
};
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]

    #[test]
    fn arbitrary_journal_deltas_reopen_exactly_and_remain_bounded(values in prop::collection::vec(any::<u16>(), 1..40)) {
        let mut store = MemoryStore::new();
        let batch = StoreBatch::initialize(DurableState::new(Machine::new().snapshot())).expect("initial batch");
        store.compare_and_commit(None, &batch).expect("initial commit");
        let mut current = store.load().expect("loads").expect("state");
        for (index, value) in values.iter().enumerate() {
            let mut next = current.state.clone();
            next.application_journals.entry("journal:property".to_owned()).or_default().push(
                JournalRecord::new(
                    format!("record:{index}"),
                    "test.segment-property/1",
                    json!({"value": value}),
                ).expect("record"),
            );
            let batch = StoreBatch::transition(&current, next).expect("transition").expect("delta");
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
