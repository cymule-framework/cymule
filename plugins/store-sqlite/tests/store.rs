//! `SQLite` segmented-store reopen, migration, fencing, and complexity tests.
use cymule_core::Machine;
use cymule_durable::{
    DurableDelta, DurableError, DurableOperation, DurableState, DurableStore, JournalRecord,
    MAX_CHECKPOINT_PACKS, MAX_HOT_SEGMENTS, StoreBatch, StoreHead, StoredState,
};
use cymule_store_sqlite::SqliteStore;
use rusqlite::Connection;
use serde_json::json;
use std::time::Duration;
use tempfile::tempdir;

fn state() -> DurableState {
    DurableState::new(Machine::new().snapshot())
}
fn initialize(store: &mut impl DurableStore) -> StoredState {
    let batch = StoreBatch::initialize(state()).expect("initial batch");
    store
        .compare_and_commit(None, &batch)
        .expect("initial commit");
    store.load().expect("loads").expect("state exists")
}
fn append(store: &mut impl DurableStore, current: &StoredState, index: usize) -> StoredState {
    let record = JournalRecord::new(
        format!("record:{index}"),
        "test.record/1",
        json!({"index": index}),
    )
    .expect("record");
    let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
        journal_id: "journal:test".to_owned(),
        records: vec![record],
    }])
    .expect("delta");
    let batch = StoreBatch::transition(current, delta).expect("transition");
    store
        .compare_and_commit(Some(&current.head), &batch)
        .expect("delta commits");
    store.load().expect("loads").expect("state exists")
}

#[test]
fn sqlite_reopens_authenticated_suffix_and_rejects_stale_head() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("cymule.sqlite");
    let mut writer = SqliteStore::open(&path, "domain:one").expect("store opens");
    let mut stale = SqliteStore::open(&path, "domain:one").expect("second store opens");
    let initial = initialize(&mut writer);
    let stale_view = stale.load().expect("stale view loads").expect("state");
    let next = append(&mut writer, &initial, 1);
    let stale_batch = StoreBatch::transition(
        &stale_view,
        DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:stale".to_owned(),
            records: vec![
                JournalRecord::new("stale", "test.record/1", json!(null)).expect("record"),
            ],
        }])
        .expect("delta"),
    )
    .expect("transition");
    assert!(matches!(
        stale.compare_and_commit(Some(&stale_view.head), &stale_batch),
        Err(DurableError::Conflict { .. })
    ));
    let reopened = SqliteStore::open(&path, "domain:one")
        .expect("reopens")
        .load()
        .expect("loads")
        .expect("state");
    assert_eq!(reopened.revision, next.revision);
    assert_eq!(reopened.state, next.state);
}

#[test]
fn suffix_reopen_is_bounded_and_cold_objects_are_reclaimable() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("bounded.sqlite");
    let mut store = SqliteStore::open(&path, "domain:bounded").expect("opens");
    let mut current = initialize(&mut store);
    for index in 0..(MAX_HOT_SEGMENTS as usize + 3) {
        current = append(&mut store, &current, index);
    }
    let reopened = SqliteStore::open(&path, "domain:bounded")
        .expect("reopens")
        .load()
        .expect("loads")
        .expect("state");
    let before = store.stats().expect("stats");
    assert!(before.reopened_segments < MAX_HOT_SEGMENTS * MAX_CHECKPOINT_PACKS);
    assert!(before.checkpoints >= 2);
    let database = Connection::open(&path).expect("complexity observer opens");
    let largest_segment: i64 = database
        .query_row(
            "SELECT MAX(length(object_json)) FROM cymule_segments WHERE domain = ?1",
            ["domain:bounded"],
            |row| row.get(0),
        )
        .expect("segment sizes read");
    let projection_bytes = cymule_core::canonical_bytes(&reopened.state).expect("projection bytes");
    assert!(largest_segment < i64::try_from(projection_bytes.len()).expect("size fits"));
    let receipt = store
        .reclaim_cold(&reopened.head)
        .expect("cold archive reclaims");
    assert!(receipt.reclaimed_objects > 0);
    let after = store.stats().expect("stats");
    assert_eq!(after.checkpoints, 1);
    assert!(after.segments < u64::from(MAX_HOT_SEGMENTS));
    assert_eq!(after.gc_receipts, 1);
    assert_eq!(
        store.load().expect("loads after GC").expect("state").state,
        reopened.state
    );
    database
        .execute(
            "UPDATE cymule_gc_receipts SET object_json = ?1
             WHERE domain = ?2 AND object_id = ?3",
            (
                b"{}".as_slice(),
                "domain:bounded",
                receipt.receipt_id.as_str(),
            ),
        )
        .expect("test corrupts GC receipt");
    assert!(matches!(store.load(), Err(DurableError::Encoding(_))));
    database
        .execute(
            "UPDATE cymule_gc_receipts SET object_json = ?1
             WHERE domain = ?2 AND object_id = ?3",
            (
                cymule_core::canonical_bytes(&receipt).expect("receipt bytes"),
                "domain:bounded",
                receipt.receipt_id.as_str(),
            ),
        )
        .expect("test restores GC receipt");
    let after_gc = store
        .load()
        .expect("loads restored receipt")
        .expect("state");
    let advanced = append(&mut store, &after_gc, 10_000);
    assert!(advanced.head.gc_receipt.is_none());
    assert_eq!(
        store
            .load()
            .expect("loads after new delta")
            .expect("state")
            .state,
        advanced.state
    );
}

#[test]
fn separate_domains_and_nonblocking_writer_contention_hold() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("domains.sqlite");
    let mut first = SqliteStore::open(&path, "domain:first").expect("first opens");
    let mut second = SqliteStore::open(&path, "domain:second").expect("second opens");
    initialize(&mut first);
    assert!(second.load().expect("second loads").is_none());
    let blocker = Connection::open(&path).expect("blocking connection opens");
    blocker.busy_timeout(Duration::ZERO).expect("zero timeout");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("writer begins");
    let batch = StoreBatch::initialize(state()).expect("batch");
    assert!(
        matches!(second.compare_and_commit(None, &batch), Err(DurableError::Conflict { current: Some(current), .. }) if current == "sqlite-writer-active")
    );
    blocker.execute_batch("ROLLBACK").expect("writer releases");
}

#[test]
fn read_only_observer_reads_but_cannot_commit() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("observer.sqlite");
    let mut writer = SqliteStore::open(&path, "domain:one").expect("writer opens");
    let retained = initialize(&mut writer);
    let mut observer = SqliteStore::open_read_only(&path, "domain:one").expect("observer opens");
    assert_eq!(
        observer.load().expect("reads").expect("state").revision,
        retained.revision
    );
    let batch = StoreBatch::transition(
        &retained,
        DurableDelta::new(vec![DurableOperation::AppendJournal {
            journal_id: "journal:x".to_owned(),
            records: vec![JournalRecord::new("x", "x/1", json!(null)).expect("record")],
        }])
        .expect("delta"),
    )
    .expect("transition");
    assert!(
        matches!(observer.compare_and_commit(Some(&retained.head), &batch), Err(DurableError::Validation(message)) if message.contains("read-only"))
    );
}

#[test]
fn corrupted_head_fails_closed() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("corrupt.sqlite");
    let mut store = SqliteStore::open(&path, "domain:one").expect("opens");
    initialize(&mut store);
    Connection::open(&path)
        .expect("raw connection")
        .execute(
            "UPDATE cymule_heads SET head_json = ?1 WHERE domain = ?2",
            (b"{}".as_slice(), "domain:one"),
        )
        .expect("corrupts head");
    assert!(matches!(store.load(), Err(DurableError::Encoding(_))));
}

#[test]
fn reopen_reads_only_head_reachable_objects() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("reachable.sqlite");
    let mut store = SqliteStore::open(&path, "domain:reachable").expect("opens");
    let initial = initialize(&mut store);
    let expected = append(&mut store, &initial, 1);
    let connection = Connection::open(&path).expect("raw connection opens");
    for table in ["cymule_checkpoints", "cymule_segments"] {
        connection
            .execute(
                &format!("INSERT INTO {table}(domain, object_id, object_json) VALUES (?1, ?2, ?3)"),
                (
                    "domain:reachable",
                    format!("unreachable:{table}"),
                    b"{}".as_slice(),
                ),
            )
            .expect("unreachable corrupt row writes");
    }
    let reopened = store
        .load()
        .expect("reachable lineage loads")
        .expect("state");
    assert_eq!(reopened, expected);
}

#[test]
fn sqlite_statement_failures_roll_back_immutable_rows_and_head_together() {
    for (name, trigger) in [
        (
            "segment_insert",
            "CREATE TRIGGER fail_segment BEFORE INSERT ON cymule_segments
             BEGIN SELECT RAISE(ABORT, 'injected segment failure'); END;",
        ),
        (
            "head_update",
            "CREATE TRIGGER fail_head BEFORE UPDATE ON cymule_heads
             BEGIN SELECT RAISE(ABORT, 'injected head failure'); END;",
        ),
    ] {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join(format!("fault-{name}.sqlite"));
        let mut store = SqliteStore::open(&path, "domain:fault").expect("opens");
        let initial = initialize(&mut store);
        let raw = Connection::open(&path).expect("fault connection opens");
        raw.execute_batch(trigger).expect("fault trigger installs");
        let record =
            JournalRecord::new("fault", "test.fault/1", json!({"name": name})).expect("record");
        let batch = StoreBatch::transition(
            &initial,
            DurableDelta::new(vec![DurableOperation::AppendJournal {
                journal_id: "journal:fault".to_owned(),
                records: vec![record],
            }])
            .expect("delta"),
        )
        .expect("transition");
        assert!(matches!(
            store.compare_and_commit(Some(&initial.head), &batch),
            Err(DurableError::Substrate(_))
        ));
        raw.execute_batch("DROP TRIGGER IF EXISTS fail_segment; DROP TRIGGER IF EXISTS fail_head;")
            .expect("fault trigger removes");
        let reopened = store
            .load()
            .expect("store remains readable")
            .expect("state");
        assert_eq!(reopened, initial);
        let segments: i64 = raw
            .query_row(
                "SELECT COUNT(*) FROM cymule_segments WHERE domain = ?1",
                ["domain:fault"],
                |row| row.get(0),
            )
            .expect("segment count reads");
        assert_eq!(segments, 0, "{name} rolls back the whole transaction");
    }
}

#[test]
fn content_addressed_row_locator_must_match_embedded_segment_id() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("aliased-segment.sqlite");
    let mut store = SqliteStore::open(&path, "domain:alias").expect("opens");
    let initial = initialize(&mut store);
    let current = append(&mut store, &initial, 1);
    let actual = current
        .head
        .suffix_head
        .as_ref()
        .expect("suffix exists")
        .clone();
    let alias = format!("sha256:{}", "f".repeat(64));
    let connection = Connection::open(&path).expect("raw observer opens");
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT object_json FROM cymule_segments WHERE domain=?1 AND object_id=?2",
            ("domain:alias", &actual),
            |row| row.get(0),
        )
        .expect("segment bytes read");
    connection
        .execute(
            "INSERT INTO cymule_segments(domain, object_id, object_json) VALUES (?1, ?2, ?3)",
            ("domain:alias", &alias, bytes),
        )
        .expect("aliased segment writes");
    let mut aliased: StoreHead = current.head.clone();
    aliased.suffix_head = Some(alias);
    connection
        .execute(
            "UPDATE cymule_heads SET head_json=?1 WHERE domain=?2",
            (
                cymule_core::canonical_bytes(&aliased).expect("aliased head bytes"),
                "domain:alias",
            ),
        )
        .expect("aliased head writes");
    assert!(
        matches!(store.load(), Err(DurableError::Validation(message)) if message.contains("locator"))
    );
}

#[test]
fn legacy_format_requires_explicit_offline_migration() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("legacy.sqlite");
    let legacy = state();
    let revision = legacy.revision().expect("revision");
    let connection = Connection::open(&path).expect("opens raw SQLite");
    connection.execute_batch("CREATE TABLE cymule_state (domain TEXT PRIMARY KEY NOT NULL, schema_version TEXT NOT NULL, revision TEXT NOT NULL, state_json BLOB NOT NULL) STRICT;").expect("legacy schema");
    connection
        .execute(
            "INSERT INTO cymule_state VALUES (?1, ?2, ?3, ?4)",
            (
                "domain:legacy",
                "cymule.sqlite-store/1",
                &revision,
                cymule_core::canonical_bytes(&legacy).expect("bytes"),
            ),
        )
        .expect("legacy row");
    drop(connection);
    assert!(
        matches!(SqliteStore::open(&path, "domain:legacy"), Err(DurableError::Validation(message)) if message.contains("explicit offline"))
    );
    let receipts = SqliteStore::migrate_v1(&path).expect("offline migration");
    assert_eq!(receipts.len(), 1);
    let mut migrated = SqliteStore::open(&path, "domain:legacy").expect("segmented opens");
    assert_eq!(
        migrated.load().expect("loads").expect("state").revision,
        revision
    );
    assert!(SqliteStore::migrate_v1(&path).is_err());

    let mixed_path = directory.path().join("mixed.sqlite");
    let mixed = Connection::open(&mixed_path).expect("mixed database opens");
    mixed.execute_batch(
        "CREATE TABLE cymule_state (domain TEXT PRIMARY KEY NOT NULL, schema_version TEXT NOT NULL, revision TEXT NOT NULL, state_json BLOB NOT NULL) STRICT;
         CREATE TABLE cymule_store_meta (singleton INTEGER PRIMARY KEY, schema_version TEXT NOT NULL) STRICT;
         INSERT INTO cymule_store_meta VALUES (1, 'future-segmented-format');",
    ).expect("mixed schema creates");
    mixed
        .execute(
            "INSERT INTO cymule_state VALUES (?1, ?2, ?3, ?4)",
            (
                "domain:mixed",
                "cymule.sqlite-store/1",
                &revision,
                cymule_core::canonical_bytes(&legacy).expect("mixed legacy bytes"),
            ),
        )
        .expect("mixed legacy row writes");
    drop(mixed);
    assert!(matches!(
        SqliteStore::migrate_v1(&mixed_path),
        Err(DurableError::Validation(message)) if message.contains("mixed legacy")
    ));
    let mixed = Connection::open(&mixed_path).expect("mixed database reopens");
    let legacy_retained: bool = mixed
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='cymule_state')",
            [],
            |row| row.get(0),
        )
        .expect("legacy table retention reads");
    assert!(legacy_retained);
}
