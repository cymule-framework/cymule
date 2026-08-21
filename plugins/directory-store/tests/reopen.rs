//! Restart and stale-writer tests for the directory store adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use cymule_core::seal_plan;
use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, Definition, Expression, Machine, PlanCandidate,
    Region,
};
use cymule_directory_store::DirectoryStore;
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, DurableError, DurableStore, FrameState,
    GcReceipt, JournalRecord, MAX_HOT_SEGMENTS, StateCheckpoint, StateSegment, StoreHead,
};
use fs4::FileExt;
use serde_json::json;

static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn test_directory() -> PathBuf {
    let sequence = DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cymule-directory-store-{}-{sequence}",
        std::process::id()
    ))
}

#[test]
fn writer_contention_returns_immediately_as_conflict() {
    let directory = test_directory();
    let mut store = DirectoryStore::open(&directory).expect("opens");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("head.lock"))
        .expect("lock opens");
    FileExt::lock(&lock).expect("test holds writer claim");
    assert!(matches!(store.load(), Err(DurableError::Conflict { .. })));
    drop(lock);
    fs::remove_dir_all(directory).expect("test directory removes");
}

fn machine_with_run() -> (Machine, String, cymule_core::ArtifactRef) {
    let mut machine = Machine::new();
    let plan = seal_plan(PlanCandidate {
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
    machine.insert_plan(plan.clone()).expect("Plan inserts");
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
    let input = machine
        .put_artifact("test/input", b"directory test input".to_vec())
        .expect("input stores");
    (machine, plan.plan_id, input)
}

fn continuation(plan_id: String, input: cymule_core::ArtifactRef) -> Continuation {
    Continuation {
        run_id: "run:directory".to_owned(),
        plan_id,
        binding_context: "binding:directory/1".to_owned(),
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            invocation_path: Vec::new(),
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            input,
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
    let (machine, plan_id, input) = machine_with_run();
    let mut current = DurableCoordinator::open(DirectoryStore::open(&directory).expect("opens"))
        .expect("coordinator opens")
        .initialize(&machine)
        .expect("initializes");
    let mut stale = DurableCoordinator::open(DirectoryStore::open(&directory).expect("opens"))
        .expect("second coordinator opens");
    current
        .put_continuation(continuation(plan_id.clone(), input.clone()))
        .expect("continuation commits");
    assert!(matches!(
        stale.put_continuation(continuation(plan_id, input)),
        Err(DurableError::Conflict { .. })
    ));

    fs::write(directory.join("head.next"), b"interrupted staging bytes")
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

#[test]
fn malformed_or_revision_tampered_state_fails_closed() {
    let directory = test_directory();
    let (machine, _, _) = machine_with_run();
    DurableCoordinator::open(DirectoryStore::open(&directory).expect("opens"))
        .expect("coordinator opens")
        .initialize(&machine)
        .expect("state initializes");
    let state_path = directory.join("head.json");
    let committed = fs::read(&state_path).expect("committed bytes read");

    fs::write(&state_path, b"{\"revision\":").expect("truncated state writes");
    let mut truncated = DirectoryStore::open(&directory).expect("truncated store opens");
    assert!(matches!(truncated.load(), Err(DurableError::Encoding(_))));

    let mut tampered: StoreHead = serde_json::from_slice(&committed).expect("head decodes");
    tampered.revision = format!("sha256:{}", "0".repeat(64));
    fs::write(
        &state_path,
        cymule_core::canonical_bytes(&tampered).expect("tampered state encodes"),
    )
    .expect("tampered state writes");
    let mut invalid_revision = DirectoryStore::open(&directory).expect("tampered store opens");
    assert!(matches!(
        invalid_revision.load(),
        Err(DurableError::Validation(_))
    ));

    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn content_addressed_filename_must_match_embedded_segment_id() {
    let directory = test_directory();
    let mut coordinator =
        DurableCoordinator::open(DirectoryStore::open(&directory).expect("store opens"))
            .expect("coordinator opens")
            .initialize(&Machine::new())
            .expect("state initializes");
    coordinator
        .append_journal_record(
            "journal:alias",
            JournalRecord::new("record:1", "test.alias/1", json!({"value": 1})).expect("record"),
        )
        .expect("segment commits");
    let mut store = coordinator.into_store();
    let current = store.load().expect("loads").expect("state");
    let actual = current.head.suffix_head.as_ref().expect("suffix exists");
    let alias = format!("sha256:{}", "f".repeat(64));
    let actual_path = directory
        .join("segments")
        .join(cymule_core::sha256_bytes(actual.as_bytes()));
    let alias_path = directory
        .join("segments")
        .join(cymule_core::sha256_bytes(alias.as_bytes()));
    fs::copy(actual_path, alias_path).expect("aliased segment copies");
    let mut aliased = current.head;
    aliased.suffix_head = Some(alias);
    fs::write(
        directory.join("head.json"),
        cymule_core::canonical_bytes(&aliased).expect("aliased head bytes"),
    )
    .expect("aliased head writes");
    assert!(matches!(
        store.load(),
        Err(DurableError::Validation(message)) if message.contains("locator")
    ));
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn legacy_directory_requires_explicit_offline_migration() {
    let directory = test_directory();
    fs::create_dir_all(&directory).expect("legacy directory creates");
    let state = cymule_durable::DurableState::new(Machine::new().snapshot());
    let revision = state.revision().expect("legacy revision");
    let legacy_bytes = cymule_core::canonical_bytes(&json!({"revision": revision, "state": state}))
        .expect("legacy bytes");
    fs::write(directory.join("state.json"), &legacy_bytes).expect("legacy state writes");
    assert!(matches!(
        DirectoryStore::open(&directory),
        Err(DurableError::Validation(message)) if message.contains("explicit offline")
    ));
    let receipt = DirectoryStore::migrate_v1(&directory).expect("legacy migrates");
    assert_eq!(receipt.legacy_revision, revision);
    fs::write(directory.join("state.json"), legacy_bytes)
        .expect("simulated post-head migration crash restores legacy file");
    assert!(DirectoryStore::open(&directory).is_err());
    assert_eq!(
        DirectoryStore::migrate_v1(&directory)
            .expect("matching partial migration resumes")
            .receipt_id,
        receipt.receipt_id
    );
    let mut migrated = DirectoryStore::open(&directory).expect("segmented store opens");
    assert_eq!(
        migrated.load().expect("loads").expect("state").revision,
        revision
    );
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn checkpoint_rotation_bounds_reopen_and_cold_gc_preserves_state() {
    let directory = test_directory();
    let mut coordinator =
        DurableCoordinator::open(DirectoryStore::open(&directory).expect("directory store opens"))
            .expect("coordinator opens")
            .initialize(&Machine::new())
            .expect("state initializes");
    for index in 0..(MAX_HOT_SEGMENTS + 2) {
        coordinator
            .append_journal_record(
                "journal:directory-gc",
                JournalRecord::new(
                    format!("record:{index}"),
                    "test.directory-gc/1",
                    json!({"index": index}),
                )
                .expect("journal record"),
            )
            .expect("delta commits");
    }
    let expected = coordinator.state().expect("state").clone();
    let mut store = coordinator.into_store();
    let stored = store.load().expect("loads").expect("state");
    let head = stored.head.clone();
    assert!(store.stats().expect("stats").checkpoints >= 2);
    assert!(
        store
            .reclaim_cold(&head)
            .expect("cold GC")
            .reclaimed_objects
            > 0
    );
    let reopened = store.load().expect("reopens").expect("state");
    assert_eq!(reopened.state, expected);
    let stats = store.stats().expect("stats");
    assert_eq!(stats.checkpoints, 1);
    assert!(stats.segments < u64::from(MAX_HOT_SEGMENTS));
    assert_eq!(stats.gc_receipts, 1);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn normal_load_ignores_uncommitted_objects_and_finishes_published_gc() {
    let directory = test_directory();
    let mut coordinator =
        DurableCoordinator::open(DirectoryStore::open(&directory).expect("store opens"))
            .expect("coordinator opens")
            .initialize(&Machine::new())
            .expect("state initializes");
    for index in 0..(MAX_HOT_SEGMENTS + 2) {
        coordinator
            .append_journal_record(
                "journal:pending-gc",
                JournalRecord::new(
                    format!("record:{index}"),
                    "test.pending-gc/1",
                    json!({"index": index}),
                )
                .expect("record"),
            )
            .expect("delta commits");
    }
    let mut store = coordinator.into_store();
    let stored = store.load().expect("loads").expect("state");
    let head = stored.head.clone();
    let object_path = |family: &str, id: &str| {
        directory
            .join(family)
            .join(cymule_core::sha256_bytes(id.as_bytes()))
    };
    let mut reclaimed = BTreeSet::new();
    let mut paths = BTreeMap::new();
    for entry in fs::read_dir(directory.join("checkpoints")).expect("checkpoints list") {
        let path = entry.expect("checkpoint entry").path();
        let value: StateCheckpoint =
            cymule_core::decode_json(&fs::read(&path).expect("checkpoint reads"))
                .expect("checkpoint decodes");
        paths.insert(value.checkpoint_id.clone(), path);
        reclaimed.insert(value.checkpoint_id);
    }
    for entry in fs::read_dir(directory.join("segments")).expect("segments list") {
        let path = entry.expect("segment entry").path();
        let value: StateSegment =
            cymule_core::decode_json(&fs::read(&path).expect("segment reads"))
                .expect("segment decodes");
        paths.insert(value.segment_id.clone(), path);
        reclaimed.insert(value.segment_id);
    }
    assert!(!reclaimed.is_empty());
    let checkpoint = StateCheckpoint::for_revision(
        None,
        None,
        head.sequence,
        head.revision.clone(),
        Some(stored.state),
    )
    .expect("materialized checkpoint constructs");
    fs::write(
        object_path("checkpoints", &checkpoint.checkpoint_id),
        cymule_core::canonical_bytes(&checkpoint).expect("checkpoint bytes"),
    )
    .expect("materialized checkpoint writes");
    let mut published_head = head.clone();
    published_head
        .checkpoint_id
        .clone_from(&checkpoint.checkpoint_id);
    published_head.checkpoint_depth = 0;
    published_head.suffix_head = None;
    published_head.suffix_len = 0;
    reclaimed.remove(&checkpoint.checkpoint_id);
    let receipt = GcReceipt::new(&published_head, &reclaimed).expect("GC receipt constructs");
    fs::write(
        object_path("gc-receipts", &receipt.receipt_id),
        cymule_core::canonical_bytes(&receipt).expect("receipt bytes"),
    )
    .expect("pending receipt writes");
    let uncommitted = directory.join("segments").join("orphan.next");
    fs::write(&uncommitted, b"partial immutable bytes").expect("staging residue writes");
    fs::write(
        directory.join("segments").join("unreachable-corrupt"),
        b"{}",
    )
    .expect("unreachable corrupt object writes");

    let before_publish = store
        .load()
        .expect("old head remains readable")
        .expect("state");
    assert_eq!(before_publish.head, head);
    assert!(
        !uncommitted.exists(),
        "normal load removes uncommitted staging"
    );

    published_head.gc_receipt = Some(receipt.receipt_id.clone());
    fs::write(
        directory.join("head.json"),
        cymule_core::canonical_bytes(&published_head).expect("published head bytes"),
    )
    .expect("published head writes");
    let recovered = store
        .load()
        .expect("normal load completes published reclamation")
        .expect("state");
    assert_eq!(recovered.state, before_publish.state);
    assert_eq!(recovered.head.gc_receipt, Some(receipt.receipt_id));
    for object_id in reclaimed {
        if let Some(path) = paths.get(&object_id) {
            assert!(!path.exists(), "published GC removes {object_id}");
        }
    }
    fs::remove_dir_all(directory).expect("test directory removes");
}
