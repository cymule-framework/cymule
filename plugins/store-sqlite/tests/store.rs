//! Embedded persistent-root store generation, integrity, fencing, and GC tests.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, Definition, Expression, Machine,
    MachineCommandArchiveObject, MachineCommandArchiveSegment, PlanCandidate, Region, seal_plan,
};
use cymule_durable::{
    ClockObservationAuthority, DurableError, DurableResult, DurableRuntimeControl, DurableStore,
    DurableStoreControl, ExecutionClockAuthority, GcReceipt, MAX_STORE_HEAD_BYTES,
    STATE_ROOT_VALUE_VERSION, StateRootLeafKind, StateRootValue, StoreHead, StoredState,
};
use cymule_durable_protocol::{ClockObservation, ClockObservationRef};
use cymule_profile_protocol::{
    ProtocolError, ProtocolResult,
    agent::{
        AgentCommand, AgentCommandAction, AgentHostBinding, AgentHostOccurrence, AgentMessage,
        AgentMessageCurrent, AgentMessagePageQuery, AgentProviders, AgentSessionCurrent,
        AgentSessionQuery, AgentStreamPublicationIntent, AgentStreamPublicationObservation,
        AgentUpdate, AgentWorkspaceCommand, AgentWorkspaceObservation, AgentWorkspaceSubmission,
        ContentBlock, MAX_AGENT_PAGE_BYTES, MessageRole,
    },
};
use cymule_runtime::{
    EXECUTION_BINDING_VERSION, ExecutionBinding, ExecutionBindingAdmission, PLUGIN_VERSION,
    PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use rusqlite::{Connection, OptionalExtension};
use serde_json::json;
use tempfile::tempdir;

fn initialize(store: &mut impl DurableStore) -> StoredState {
    DurableStoreControl::initialize(&mut *store).expect("initial commit");
    store
        .load_full_audit()
        .expect("state loads")
        .expect("state exists")
}

fn advance_current(store: &mut impl DurableStore) -> Result<GcReceipt, DurableError> {
    DurableStoreControl::open(store)?.advance_cold_reclamation()
}

fn reconcile_current(store: &mut impl DurableStore) -> Result<GcReceipt, DurableError> {
    DurableStoreControl::open(store)?.reconcile_cold_reclamation()
}

fn publish_definition(store: &mut SqliteStore) {
    use cymule_profile_protocol::evolution::{
        EvolutionPersistenceCommand, LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand,
        NoEvolutionProviders,
    };
    let command = EvolutionPersistenceCommand::new(
        "evolution:sqlite-probe",
        LiveEvolutionCommand::PublishDefinition {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "definition:sqlite-probe".to_owned(),
            logical_ref: "example.sqlite-probe".to_owned(),
            definition: Definition {
                id: "probe".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Input,
                },
            },
            references: Vec::new(),
        },
    )
    .expect("public profile command seals");
    DurableStoreControl::open(store)
        .expect("public control opens")
        .evolution(&mut NoEvolutionProviders)
        .commit(&command)
        .expect("new immutable collection nodes publish through public control");
}

#[test]
fn absent_new_node_probe_does_not_block_public_profile_publication() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("new-node-probe.sqlite");
    let domain = "domain:new-node-probe";
    let mut store = SqliteStore::open(&path, domain).expect("Store opens");
    let initial = initialize(&mut store);
    let missing = cymule_core::content_id("test.sqlite.new-node/1", &"probe")
        .expect("new content identity derives");
    store
        .with_state_root_resolver(&initial.state_root_manifest, |resolver| {
            assert!(resolver.load_state_root_object(&missing)?.is_none());
            Ok(())
        })
        .expect("an absent new-node probe is not reachable corruption");
    assert_eq!(
        store.load_head().expect("head reads"),
        Some(initial.head.clone())
    );
    publish_definition(&mut store);
    let current = store
        .load_full_audit()
        .expect("new closure audits")
        .expect("state exists");
    assert_ne!(current.head.revision, initial.head.revision);
    assert_eq!(current.head.sequence, initial.head.sequence + 1);
}

#[test]
fn missing_reachable_children_still_fail_audit_and_gc_without_advancing_head() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("missing-reachable-child.sqlite");
    let domain = "domain:missing-reachable-child";
    let mut store = SqliteStore::open(&path, domain).expect("Store opens");
    initialize(&mut store);
    publish_definition(&mut store);
    let head = store.load_head().expect("head reads").expect("head exists");
    let raw = Connection::open(&path).expect("corruption fixture opens");
    let before = head_bytes(&raw, domain);
    assert!(
        raw.execute(
            "DELETE FROM cymule_state_root_objects WHERE domain=?1 AND object_id<>?2",
            (domain, &head.state_root_manifest_id),
        )
        .expect("fixture removes reachable children")
            > 0
    );
    assert!(matches!(
        store.load_full_audit(),
        Err(DurableError::Integrity { .. })
    ));
    assert!(matches!(
        advance_current(&mut store),
        Err(DurableError::Integrity { .. })
    ));
    assert_eq!(head_bytes(&raw, domain), before);
}

fn assert_integrity(error: &DurableError) {
    assert!(
        matches!(error, DurableError::Integrity { .. }),
        "expected durable integrity failure, received {error:?}"
    );
}

fn assert_integrity_code(error: DurableError, expected: &str) {
    match error {
        DurableError::Integrity { code, .. } => assert_eq!(code, expected),
        other => panic!("expected integrity error {expected}, received {other}"),
    }
}

fn sqlite_sidecar(path: &Path, suffix: &str) -> std::path::PathBuf {
    let name = path
        .file_name()
        .expect("SQLite path has a file name")
        .to_string_lossy();
    path.with_file_name(format!("{name}-{suffix}"))
}

fn head_bytes(connection: &Connection, domain: &str) -> Vec<u8> {
    connection
        .query_row(
            "SELECT head_json FROM cymule_heads WHERE domain=?1",
            [domain],
            |row| row.get(0),
        )
        .expect("head bytes read")
}

fn read_head(connection: &Connection, domain: &str) -> StoreHead {
    cymule_core::decode_json(&head_bytes(connection, domain)).expect("head decodes")
}

fn write_head(connection: &Connection, domain: &str, head: &StoreHead) {
    connection
        .execute(
            "UPDATE cymule_heads SET head_json=?1 WHERE domain=?2",
            (
                cymule_core::canonical_bytes(head).expect("head encodes"),
                domain,
            ),
        )
        .expect("head updates");
}

fn standalone_archive(name: &str, run_id: &str) -> MachineCommandArchiveSegment {
    let mut machine = Machine::new();
    let plan = seal_plan(PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: name.to_owned(),
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
    .expect("standalone Plan seals");
    machine.insert_plan(plan.clone()).expect("Plan inserts");
    let execution_binding = ExecutionBinding::for_local_process(
        &PluginManifest {
            plugin_version: PLUGIN_VERSION.to_owned(),
            implementation_id: format!("sqlite-standalone-archive:{name}"),
            components: BTreeMap::new(),
            effects: BTreeMap::new(),
        },
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    )
    .expect("execution binding seals");
    execution_binding
        .admit_plan(&plan)
        .expect("binding admits Plan");
    let binding = machine
        .put_artifact(
            EXECUTION_BINDING_VERSION,
            execution_binding
                .canonical_bytes()
                .expect("binding encodes"),
        )
        .expect("binding Artifact stores");
    let input = machine
        .put_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, b"null".to_vec())
        .expect("input Artifact stores");
    let command_id = format!("command:{name}");
    let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
        command_id.clone(),
        vec![plan.clone()],
        vec![
            machine.artifact(&binding).expect("binding reads").clone(),
            machine.artifact(&input).expect("input reads").clone(),
        ],
    )
    .expect("start material admits");
    let initial_attempt = cymule_core::InitialAttemptSpec {
        attempt_id: cymule_core::content_id("test.sqlite.initial-attempt/1", &command_id)
            .expect("Attempt derives"),
        continuation_id: cymule_core::content_id("test.sqlite.initial-continuation/1", &command_id)
            .expect("Continuation derives"),
        occurrence_binding: binding.artifact_id.clone(),
        continuation_epoch: 0,
        execution_fence: 1,
    };
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: "test".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input,
                material_digest: material.material_digest().to_owned(),
                initial_attempt,
            },
        })
        .expect("Run starts");
    machine
        .compact_event_history(0)
        .expect("history compacts")
        .archive_segment
}

#[test]
fn sqlite_reopens_exact_state_root_and_rejects_stale_physical_head() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("reopen.sqlite");
    let mut writer = SqliteStore::open(&path, "domain:one").expect("writer opens");
    initialize(&mut writer);
    let mut stale = SqliteStore::open(&path, "domain:one").expect("stale writer opens");
    let stale_view = stale
        .load_full_audit()
        .expect("stale view loads")
        .expect("state exists");
    let mut stale_control = DurableStoreControl::open(&mut stale).expect("stale control opens");
    advance_current(&mut writer).expect("writer publishes a physical generation");
    let next = writer
        .load_full_audit()
        .expect("next state loads")
        .expect("next state exists");

    let error = stale_control
        .advance_cold_reclamation()
        .expect_err("stale physical head loses CAS");
    assert!(matches!(error, DurableError::Conflict { .. }));
    drop(stale_control);
    assert_ne!(stale_view.head.physical_token, next.head.physical_token);
    assert_eq!(
        stale
            .load_full_audit()
            .expect("reopen loads")
            .expect("state"),
        next
    );

    let stats = writer.stats().expect("stats read");
    assert!(stats.state_root_objects > 0);
}

#[test]
fn gc_with_no_candidates_publishes_an_exact_empty_inventory_receipt() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("empty-gc.sqlite");
    let domain = "domain:empty-gc";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let initial = initialize(&mut store);
    let receipt = advance_current(&mut store).expect("empty inventory receipt commits");
    assert_eq!(receipt.reclaimed_objects, 0);
    assert_eq!(receipt.remaining_objects, 0);
    let retained = store
        .load_full_audit()
        .expect("state reloads")
        .expect("state");
    assert_eq!(retained.head.revision, initial.head.revision);
    assert_eq!(retained.head.sequence, initial.head.sequence);
    assert_eq!(retained.head.gc_sequence, initial.head.gc_sequence + 1);
    assert_ne!(retained.head.physical_token, initial.head.physical_token);
    assert_eq!(store.stats().expect("stats read").gc_receipts, 1);
    let reconciled =
        reconcile_current(&mut store).expect("lost terminal-page acknowledgement reconciles");
    assert_eq!(reconciled, receipt);
    assert_eq!(
        store
            .load_full_audit()
            .expect("state reloads")
            .expect("state")
            .head,
        retained.head,
        "terminal reconciliation must not create another physical generation"
    );
}

#[test]
fn gc_reconciliation_requires_a_current_head_pinned_receipt() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("gc-reconcile-without-receipt.sqlite");
    let mut store =
        SqliteStore::open(&path, "domain:gc-reconcile-without-receipt").expect("store opens");
    let initial = initialize(&mut store);
    assert!(matches!(
        reconcile_current(&mut store),
        Err(DurableError::Validation(message))
            if message.contains("head-pinned GC receipt")
    ));
    assert_eq!(
        store
            .load_full_audit()
            .expect("state reloads")
            .expect("state"),
        initial
    );
}

#[test]
fn post_receipt_orphan_requires_an_explicit_new_gc_generation() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("post-receipt-orphan.sqlite");
    let domain = "domain:post-receipt-orphan";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    initialize(&mut store);
    let first = advance_current(&mut store).expect("clean-inventory generation commits");
    let first_state = store
        .load_full_audit()
        .expect("state loads")
        .expect("state");
    let orphan_id = format!("sha256:{}", "8".repeat(64));
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
             VALUES (?1,?2,x'7b7d')",
            (domain, &orphan_id),
        )
        .expect("post-receipt orphan inserts");

    assert_eq!(
        reconcile_current(&mut store).expect("existing receipt reconciles"),
        first
    );
    let orphan_exists = || {
        connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM cymule_state_root_objects
                    WHERE domain=?1 AND object_id=?2
                 )",
                (domain, &orphan_id),
                |row| row.get::<_, bool>(0),
            )
            .expect("orphan presence reads")
    };
    assert!(orphan_exists(), "reconciliation cannot select new work");
    assert_eq!(
        store
            .load_head()
            .expect("ordinary head reloads")
            .expect("head"),
        first_state.head
    );

    let second = advance_current(&mut store).expect("explicit successor generation commits");
    assert!(second.reclaimed_ids.contains(&first.receipt_id));
    assert!(second.reclaimed_ids.contains(&orphan_id));
    assert_eq!(second.remaining_objects, 0);
    assert!(!orphan_exists());
    assert_eq!(store.stats().expect("stats read").gc_receipts, 1);
}

#[test]
fn gc_rejects_cross_family_identity_aliases_without_publishing() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("gc-cross-family-alias.sqlite");
    let domain = "domain:gc-cross-family-alias";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let initial = initialize(&mut store);
    let alias = format!("sha256:{}", "6".repeat(64));
    let connection = Connection::open(&path).expect("raw connection opens");
    for table in [
        "cymule_state_root_objects",
        "cymule_machine_command_archive_objects",
    ] {
        connection
            .execute(
                &format!(
                    "INSERT INTO {table}(domain,object_id,object_json) VALUES (?1,?2,x'7b7d')"
                ),
                (domain, &alias),
            )
            .expect("aliased physical row inserts");
    }
    assert_integrity_code(
        advance_current(&mut store).expect_err("cross-family alias blocks GC"),
        "sqlite_gc_cross_family_identity_alias",
    );
    assert_eq!(
        store
            .load_full_audit()
            .expect("state reloads")
            .expect("state")
            .head,
        initial.head
    );
    let receipt_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM cymule_gc_receipts WHERE domain=?1",
            [domain],
            |row| row.get(0),
        )
        .expect("receipt count reads");
    assert_eq!(receipt_count, 0);
}

#[test]
fn full_audit_rejects_a_cross_family_alias_outside_the_bounded_head() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("reopen-cross-family-alias.sqlite");
    let domain = "domain:reopen-cross-family-alias";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let current = initialize(&mut store);
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "INSERT INTO cymule_machine_command_archive_objects(
                 domain,object_id,object_json
             ) VALUES (?1,?2,x'7b7d')",
            (domain, &current.head.state_root_manifest_id),
        )
        .expect("cross-family alias inserts");

    assert_eq!(
        store.load_head().expect("bounded head load succeeds"),
        Some(current.head),
        "ordinary head load must not resolve StateRoot physical identity"
    );

    assert_integrity_code(
        store
            .load_full_audit()
            .expect_err("active cross-family alias blocks reopen"),
        "sqlite_physical_object_identity_alias",
    );
}

#[test]
fn gc_rejects_a_stale_head_before_resolving_its_stale_manifest() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("gc-stale-manifest.sqlite");
    let domain = "domain:gc-stale-manifest";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let initial = initialize(&mut store);
    let stale_store = SqliteStore::open(&path, domain).expect("stale Store opens");
    let mut stale = DurableStoreControl::open(stale_store).expect("stale control opens");
    advance_current(&mut store).expect("current physical generation advances");
    let connection = Connection::open(&path).expect("raw connection opens");
    let manifest_bytes: Vec<u8> = connection
        .query_row(
            "SELECT object_json FROM cymule_state_root_objects
             WHERE domain=?1 AND object_id=?2",
            (domain, &initial.head.state_root_manifest_id),
            |row| row.get(0),
        )
        .expect("stale manifest bytes read");
    connection
        .execute(
            "UPDATE cymule_state_root_objects SET object_json=x'7b7d'
             WHERE domain=?1 AND object_id=?2",
            (domain, &initial.head.state_root_manifest_id),
        )
        .expect("stale manifest corrupts");
    assert!(matches!(
        stale.advance_cold_reclamation(),
        Err(DurableError::Conflict { .. })
    ));
    connection
        .execute(
            "UPDATE cymule_state_root_objects SET object_json=?1
             WHERE domain=?2 AND object_id=?3",
            (
                &manifest_bytes,
                domain,
                &initial.head.state_root_manifest_id,
            ),
        )
        .expect("manifest bytes restore");
    store
        .load_full_audit()
        .expect("current state loads")
        .expect("state");
}

#[test]
fn typed_state_root_reads_reject_current_head_metadata_that_disagrees_with_its_manifest() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("typed-head-manifest-mismatch.sqlite");
    let domain = "domain:typed-head-manifest-mismatch";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let current = initialize(&mut store);
    let mut forged_head = current.head.clone();
    forged_head.sequence += 1;
    forged_head
        .verify()
        .expect("forged head remains well shaped");
    Connection::open(&path)
        .expect("raw connection opens")
        .execute(
            "UPDATE cymule_heads SET head_json=?1 WHERE domain=?2",
            (
                cymule_core::canonical_bytes(&forged_head).expect("forged head encodes"),
                domain,
            ),
        )
        .expect("head metadata is forged");

    assert_eq!(
        store.load_head().expect("bounded head load succeeds"),
        Some(forged_head),
        "ordinary head load must not read the pinned StateRoot manifest"
    );

    assert_integrity_code(
        store
            .load_full_audit()
            .expect_err("materialization rejects mismatched current head metadata"),
        "sqlite_state_root_head_mismatch",
    );
    assert_integrity_code(
        store
            .application_journal_record_manifest(
                &current.state_root_manifest,
                "journal:missing",
                "record:missing",
            )
            .expect_err("typed exact lookup rejects mismatched current head metadata"),
        "sqlite_state_root_head_mismatch",
    );
}

#[test]
fn separate_domains_and_nonblocking_writer_contention_hold() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("domains.sqlite");
    let mut first = SqliteStore::open(&path, "domain:first").expect("first opens");
    let mut second = SqliteStore::open(&path, "domain:second").expect("second opens");
    initialize(&mut first);
    assert!(second.load_head().expect("second head loads").is_none());

    let blocker = Connection::open(&path).expect("blocking connection opens");
    blocker.busy_timeout(Duration::ZERO).expect("zero timeout");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("writer begins");
    assert!(matches!(
        DurableStoreControl::initialize(&mut second),
        Err(DurableError::Conflict { current: Some(current), .. })
            if current == "sqlite-writer-active"
    ));
    blocker.execute_batch("ROLLBACK").expect("writer releases");
}

#[test]
fn read_only_observer_reads_without_mutation_and_rejects_writes() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("observer.sqlite");
    let mut writer = SqliteStore::open(&path, "domain:observer").expect("writer opens");
    let retained = initialize(&mut writer);
    drop(writer);

    let database_before = fs::read(&path).expect("database bytes read");
    let wal_path = sqlite_sidecar(&path, "wal");
    let wal_before = fs::read(&wal_path).unwrap_or_default();
    let mut observer =
        SqliteStore::open_read_only(&path, "domain:observer").expect("observer opens");
    assert_eq!(
        observer.load_head().expect("observer head loads"),
        Some(retained.head.clone())
    );
    assert_eq!(
        observer
            .load_full_audit()
            .expect("observer loads")
            .expect("state"),
        retained
    );
    assert!(matches!(
        advance_current(&mut observer),
        Err(DurableError::Validation(message)) if message.contains("read-only")
    ));
    assert!(matches!(
        reconcile_current(&mut observer),
        Err(DurableError::Validation(message)) if message.contains("read-only")
    ));
    drop(observer);
    assert_eq!(
        fs::read(&path).expect("database bytes reread"),
        database_before
    );
    assert_eq!(fs::read(&wal_path).unwrap_or_default(), wal_before);
}

#[test]
fn read_only_open_does_not_create_missing_database_or_sidecars() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("missing.sqlite");
    assert!(matches!(
        SqliteStore::open_read_only(&path, "domain:missing"),
        Err(DurableError::Substrate { code, .. }) if code == "sqlite_open_failure"
    ));
    assert!(!path.exists());
    assert!(!sqlite_sidecar(&path, "wal").exists());
    assert!(!sqlite_sidecar(&path, "shm").exists());
}

#[test]
fn read_only_observer_can_read_while_immediate_writer_is_active() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("concurrent-observer.sqlite");
    let mut writer = SqliteStore::open(&path, "domain:observer").expect("writer opens");
    let retained = initialize(&mut writer);
    drop(writer);
    let blocker = Connection::open(&path).expect("blocking connection opens");
    blocker.busy_timeout(Duration::ZERO).expect("zero timeout");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("writer begins");
    let mut observer =
        SqliteStore::open_read_only(&path, "domain:observer").expect("observer opens");
    assert_eq!(
        observer.load_head().expect("snapshot head loads"),
        Some(retained.head)
    );
    blocker.execute_batch("ROLLBACK").expect("writer releases");
}

#[test]
fn noncanonical_or_malformed_head_fails_as_integrity() {
    let directory = tempdir().expect("temporary directory");
    for name in ["malformed", "noncanonical"] {
        let path = directory.path().join(format!("{name}.sqlite"));
        let domain = format!("domain:{name}");
        let mut store = SqliteStore::open(&path, &domain).expect("store opens");
        initialize(&mut store);
        let connection = Connection::open(&path).expect("raw connection opens");
        let replacement = if name == "malformed" {
            b"{}".to_vec()
        } else {
            let mut bytes = vec![b' '];
            bytes.extend(head_bytes(&connection, &domain));
            bytes
        };
        connection
            .execute(
                "UPDATE cymule_heads SET head_json=?1 WHERE domain=?2",
                (&replacement, &domain),
            )
            .expect("head corrupts");
        assert_integrity(&store.load_head().expect_err("corrupt head is rejected"));
    }
}

#[test]
fn oversized_head_blob_is_rejected_before_json_decode() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("oversized-head.sqlite");
    let domain = "domain:oversized-head";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    initialize(&mut store);
    let oversize = i64::try_from(MAX_STORE_HEAD_BYTES + 1).expect("head limit fits SQLite integer");
    Connection::open(&path)
        .expect("raw connection opens")
        .execute(
            "UPDATE cymule_heads SET head_json=zeroblob(?1) WHERE domain=?2",
            (oversize, domain),
        )
        .expect("oversized head BLOB writes");
    assert_integrity_code(
        store.load_head().expect_err("oversized head is rejected"),
        "sqlite_head_integrity",
    );
}

#[test]
fn current_manifest_missing_alias_and_wrong_kind_are_integrity_failures() {
    let directory = tempdir().expect("temporary directory");

    let missing_path = directory.path().join("missing-manifest.sqlite");
    let missing_domain = "domain:missing-manifest";
    let mut missing =
        SqliteStore::open(&missing_path, missing_domain).expect("missing test store opens");
    let state = initialize(&mut missing);
    Connection::open(&missing_path)
        .expect("raw connection opens")
        .execute(
            "DELETE FROM cymule_state_root_objects WHERE domain=?1 AND object_id=?2",
            (missing_domain, &state.head.state_root_manifest_id),
        )
        .expect("manifest deletes");
    assert_eq!(
        missing.load_head().expect("bounded head load succeeds"),
        Some(state.head.clone()),
        "ordinary head load must not read the StateRoot family"
    );
    assert_integrity_code(
        missing
            .load_full_audit()
            .expect_err("missing manifest is rejected"),
        "sqlite_state_root_manifest_missing",
    );

    let alias_path = directory.path().join("alias-manifest.sqlite");
    let alias_domain = "domain:alias-manifest";
    let mut alias = SqliteStore::open(&alias_path, alias_domain).expect("alias test store opens");
    let state = initialize(&mut alias);
    let connection = Connection::open(&alias_path).expect("raw connection opens");
    let manifest_bytes: Vec<u8> = connection
        .query_row(
            "SELECT object_json FROM cymule_state_root_objects
             WHERE domain=?1 AND object_id=?2",
            (alias_domain, &state.head.state_root_manifest_id),
            |row| row.get(0),
        )
        .expect("manifest bytes read");
    let alias_id = format!("sha256:{}", "a".repeat(64));
    connection
        .execute(
            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
             VALUES (?1,?2,?3)",
            (alias_domain, &alias_id, manifest_bytes),
        )
        .expect("manifest alias inserts");
    let mut aliased_head = state.head;
    aliased_head.state_root_manifest_id = alias_id;
    write_head(&connection, alias_domain, &aliased_head);
    assert_integrity_code(
        alias
            .load_full_audit()
            .expect_err("aliased manifest is rejected"),
        "sqlite_state_root_object_locator",
    );

    let kind_path = directory.path().join("wrong-kind.sqlite");
    let kind_domain = "domain:wrong-kind";
    let mut wrong_kind =
        SqliteStore::open(&kind_path, kind_domain).expect("wrong-kind test store opens");
    initialize(&mut wrong_kind);
    publish_definition(&mut wrong_kind);
    let state = wrong_kind
        .load_full_audit()
        .expect("published state audits")
        .expect("state exists");
    let connection = Connection::open(&kind_path).expect("raw connection opens");
    let (object_id, _): (String, Vec<u8>) = connection
        .query_row(
            "SELECT object_id, object_json FROM cymule_state_root_objects
             WHERE domain=?1 AND object_id<>?2
               AND json_extract(CAST(object_json AS TEXT), '$.object') <> 'manifest'
             ORDER BY object_id LIMIT 1",
            (kind_domain, &state.head.state_root_manifest_id),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("non-manifest object reads");
    let mut kind_head = state.head;
    kind_head.state_root_manifest_id = object_id;
    write_head(&connection, kind_domain, &kind_head);
    assert_integrity_code(
        wrong_kind
            .load_full_audit()
            .expect_err("wrong manifest object kind is rejected"),
        "sqlite_state_root_manifest_kind",
    );
}

#[test]
fn reachable_state_root_noncanonical_bytes_and_locator_alias_fail_closed() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("reachable-corruption.sqlite");
    let domain = "domain:reachable-corruption";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    initialize(&mut store);
    publish_definition(&mut store);
    let state = store
        .load_full_audit()
        .expect("published state audits")
        .expect("state exists");
    let connection = Connection::open(&path).expect("raw connection opens");
    let (object_id, bytes): (String, Vec<u8>) = connection
        .query_row(
            "SELECT object_id, object_json FROM cymule_state_root_objects
             WHERE domain=?1 AND object_id<>?2
               AND json_extract(CAST(object_json AS TEXT), '$.object') <> 'manifest'
             ORDER BY object_id LIMIT 1",
            (domain, &state.head.state_root_manifest_id),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("reachable object reads");
    let mut noncanonical = vec![b' '];
    noncanonical.extend(bytes);
    connection
        .execute(
            "UPDATE cymule_state_root_objects SET object_json=?1
             WHERE domain=?2 AND object_id=?3",
            (&noncanonical, domain, &object_id),
        )
        .expect("reachable object becomes noncanonical");
    assert_integrity(
        &store
            .load_full_audit()
            .expect_err("noncanonical object is rejected"),
    );
}

#[test]
fn unreachable_malformed_object_is_ignored_by_head_load_and_deleted_by_gc() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("unreachable.sqlite");
    let domain = "domain:unreachable";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let state = initialize(&mut store);
    let orphan_id = format!("sha256:{}", "9".repeat(64));
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
             VALUES (?1,?2,?3)",
            (domain, &orphan_id, b"{}".as_slice()),
        )
        .expect("unreachable malformed row inserts");
    assert_eq!(
        store.load_head().expect("ordinary head loads"),
        Some(state.head.clone())
    );
    assert_eq!(
        store
            .load_full_audit()
            .expect("reachable state loads")
            .expect("state"),
        state
    );
    let receipt = advance_current(&mut store)
        .expect("GC deletes unreachable malformed row without treating it as authority");
    assert!(receipt.reclaimed_ids.contains(&orphan_id));
    let retained: bool = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM cymule_state_root_objects
                WHERE domain=?1 AND object_id=?2
             )",
            (domain, &orphan_id),
            |row| row.get(0),
        )
        .expect("orphan absence reads");
    assert!(!retained);
}

#[test]
fn gc_rejects_invalid_physical_locator_before_head_change() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("invalid-locator.sqlite");
    let domain = "domain:invalid-locator";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let state = initialize(&mut store);
    Connection::open(&path)
        .expect("raw connection opens")
        .execute(
            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
             VALUES (?1,?2,?3)",
            (domain, "not-a-content-id", b"{}".as_slice()),
        )
        .expect("invalid locator inserts");
    assert_integrity_code(
        advance_current(&mut store).expect_err("invalid physical locator blocks GC"),
        "sqlite_object_locator_length",
    );
    assert_eq!(
        store
            .load_full_audit()
            .expect("state reloads")
            .expect("state")
            .head,
        state.head
    );
}

#[test]
fn command_archive_row_keys_must_match_embedded_object_identities() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("archive-alias.sqlite");
    let domain = "domain:archive-alias";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let connection = Connection::open(&path).expect("raw connection opens");
    let archive = standalone_archive("sqlite_locator_archive", "run:sqlite-locator");
    for (index, object) in archive
        .persistence_objects()
        .expect("objects derive")
        .into_iter()
        .enumerate()
    {
        let alias = format!("sha256:{:064x}", index + 1);
        let batch_id = match &object {
            MachineCommandArchiveObject::Batch(batch) => Some(batch.batch_id.as_str()),
            _ => None,
        };
        connection
            .execute(
                "INSERT INTO cymule_machine_command_archive_objects(
                    domain,object_id,batch_id,object_json
                 ) VALUES (?1,?2,?3,?4)",
                (
                    domain,
                    &alias,
                    batch_id,
                    cymule_core::canonical_bytes(&object).expect("object encodes"),
                ),
            )
            .expect("aliased object writes");
        let error = match object {
            MachineCommandArchiveObject::Segment(_) => store
                .load_machine_command_archive_segment(&alias)
                .expect_err("aliased segment is rejected"),
            MachineCommandArchiveObject::Entry(_) => store
                .load_machine_command_archive_entry(&alias)
                .expect_err("aliased entry is rejected"),
            MachineCommandArchiveObject::Batch(batch) => store
                .load_machine_command_archive_batch(&batch.batch_id)
                .expect_err("aliased batch is rejected"),
            MachineCommandArchiveObject::CommandIndexNode(_) => store
                .load_machine_command_index_node(&alias)
                .expect_err("aliased node is rejected"),
        };
        assert_integrity_code(error, "sqlite_command_archive_object_locator");
    }
}

#[test]
fn command_archive_batch_index_resolves_the_exact_receipt_object() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("archive-batch-index.sqlite");
    let domain = "domain:archive-batch-index";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let archive = standalone_archive("sqlite_batch_index", "run:sqlite-batch-index");
    let batch = archive
        .batches
        .first()
        .cloned()
        .expect("archive batch exists");
    let object = MachineCommandArchiveObject::Batch(Box::new(batch.clone()));
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "INSERT INTO cymule_machine_command_archive_objects(
                domain,object_id,batch_id,object_json
             ) VALUES (?1,?2,?3,?4)",
            (
                domain,
                &batch.batch_receipt_id,
                &batch.batch_id,
                cymule_core::canonical_bytes(&object).expect("batch object encodes"),
            ),
        )
        .expect("batch object and index insert");

    assert_eq!(
        store
            .load_machine_command_archive_batch(&batch.batch_id)
            .expect("batch index reads"),
        Some(batch),
    );
    assert_eq!(
        store
            .stats()
            .expect("batch inventory validates")
            .machine_command_archive_batches,
        1
    );
}

#[test]
fn gc_receipt_row_key_must_match_embedded_receipt_id() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("gc-receipt-alias.sqlite");
    let domain = "domain:gc-receipt-alias";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    initialize(&mut store);
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
             VALUES (?1,?2,x'7b7d')",
            (domain, format!("sha256:{}", "c".repeat(64))),
        )
        .expect("unreachable object inserts");
    let receipt = advance_current(&mut store).expect("GC commits");
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT object_json FROM cymule_gc_receipts
             WHERE domain=?1 AND object_id=?2",
            (domain, &receipt.receipt_id),
            |row| row.get(0),
        )
        .expect("receipt bytes read");
    let alias = format!("sha256:{}", "d".repeat(64));
    connection
        .execute(
            "INSERT INTO cymule_gc_receipts(domain,object_id,object_json)
             VALUES (?1,?2,?3)",
            (domain, &alias, bytes),
        )
        .expect("receipt alias writes");
    let mut head = read_head(&connection, domain);
    head.gc_receipt = Some(alias);
    write_head(&connection, domain, &head);
    assert_eq!(
        store
            .load_full_audit()
            .expect("semantic audit ignores physical receipt bytes")
            .expect("state exists")
            .head,
        head
    );
    assert_integrity_code(
        reconcile_current(&mut store).expect_err("explicit reconciliation rejects receipt alias"),
        "sqlite_gc_receipt_locator",
    );
}

#[test]
fn gc_reconciliation_rejects_a_receipt_that_reclaims_current_authority() {
    let directory = tempdir().expect("temporary directory");
    let path = directory
        .path()
        .join("gc-reconcile-current-authority.sqlite");
    let domain = "domain:gc-reconcile-current-authority";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    let initial = initialize(&mut store);
    let forged = GcReceipt::new_bounded(
        &initial.head,
        [initial.head.state_root_manifest_id.clone()]
            .into_iter()
            .collect(),
        0,
    )
    .expect("shape-valid forged receipt constructs");
    let mut forged_head = initial.head.clone();
    forged_head.gc_sequence = forged.gc_sequence;
    forged_head
        .physical_token
        .clone_from(&forged.result_physical_token);
    forged_head.gc_receipt = Some(forged.receipt_id.clone());
    forged
        .verify_for(&forged_head)
        .expect("forged receipt matches forged head");
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "INSERT INTO cymule_gc_receipts(domain,object_id,object_json)
             VALUES (?1,?2,?3)",
            (
                domain,
                &forged.receipt_id,
                cymule_core::canonical_bytes(&forged).expect("receipt encodes"),
            ),
        )
        .expect("forged receipt persists");
    write_head(&connection, domain, &forged_head);

    assert_integrity_code(
        reconcile_current(&mut store)
            .expect_err("current authority cannot be receipt-authorized garbage"),
        "sqlite_gc_receipt_reclaims_current_authority",
    );
    let manifest_exists: bool = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM cymule_state_root_objects
                WHERE domain=?1 AND object_id=?2
             )",
            (domain, &initial.head.state_root_manifest_id),
            |row| row.get(0),
        )
        .expect("manifest presence reads");
    assert!(manifest_exists);
    assert_eq!(read_head(&connection, domain), forged_head);
}

#[test]
fn gc_audits_orphan_receipt_identity_before_publishing_new_head() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("gc-orphan-receipt.sqlite");
    let domain = "domain:gc-orphan-receipt";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    initialize(&mut store);
    let connection = Connection::open(&path).expect("raw connection opens");
    connection
        .execute(
            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
             VALUES (?1,?2,x'7b7d')",
            (domain, format!("sha256:{}", "b".repeat(64))),
        )
        .expect("unreachable object inserts");
    let first = advance_current(&mut store).expect("first GC commits");
    let after_first = store
        .load_full_audit()
        .expect("state loads")
        .expect("state");
    let bytes: Vec<u8> = connection
        .query_row(
            "SELECT object_json FROM cymule_gc_receipts
             WHERE domain=?1 AND object_id=?2",
            (domain, &first.receipt_id),
            |row| row.get(0),
        )
        .expect("receipt bytes read");
    let alias = format!("sha256:{}", "e".repeat(64));
    connection
        .execute(
            "INSERT INTO cymule_gc_receipts(domain,object_id,object_json)
             VALUES (?1,?2,?3)",
            (domain, &alias, bytes),
        )
        .expect("orphan receipt alias inserts");
    assert_integrity_code(
        advance_current(&mut store).expect_err("orphan receipt alias blocks GC"),
        "sqlite_gc_receipt_locator",
    );
    assert_eq!(
        store
            .load_full_audit()
            .expect("head reloads")
            .expect("state")
            .head,
        after_first.head
    );
}

#[test]
fn sqlite_statement_failures_roll_back_objects_and_head_together() {
    for (name, trigger, expected_code) in [
        (
            "object",
            "CREATE TRIGGER fail_object BEFORE INSERT ON cymule_state_root_objects
             BEGIN SELECT RAISE(ABORT, 'injected object failure'); END;",
            "sqlite_state_root_failure",
        ),
        (
            "head",
            "CREATE TRIGGER fail_head BEFORE INSERT ON cymule_heads
             BEGIN SELECT RAISE(ABORT, 'injected head failure'); END;",
            "sqlite_head_cas_failure",
        ),
    ] {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join(format!("fault-{name}.sqlite"));
        let domain = "domain:fault";
        let mut store = SqliteStore::open(&path, domain).expect("store opens");
        let raw = Connection::open(&path).expect("raw connection opens");
        let object_count_before: i64 = raw
            .query_row(
                "SELECT COUNT(*) FROM cymule_state_root_objects WHERE domain=?1",
                [domain],
                |row| row.get(0),
            )
            .expect("object count reads");
        raw.execute_batch(trigger).expect("fault trigger installs");
        let Err(error) = DurableStoreControl::initialize(&mut store) else {
            panic!("injected statement failure must abort the transaction")
        };
        assert!(matches!(
            error,
            DurableError::Substrate { code, .. } if code == expected_code
        ));
        raw.execute_batch("DROP TRIGGER IF EXISTS fail_object; DROP TRIGGER IF EXISTS fail_head;")
            .expect("fault trigger removes");
        assert_eq!(store.load_full_audit().expect("state reloads"), None);
        let object_count_after: i64 = raw
            .query_row(
                "SELECT COUNT(*) FROM cymule_state_root_objects WHERE domain=?1",
                [domain],
                |row| row.get(0),
            )
            .expect("object count rereads");
        assert_eq!(object_count_after, object_count_before);
    }
}

#[test]
fn gc_advance_statement_failures_roll_back_receipt_sweep_and_head_together() {
    for (name, trigger, expected_code) in [
        (
            "sweep",
            "CREATE TRIGGER fail_gc_sweep BEFORE DELETE ON cymule_state_root_objects
             BEGIN SELECT RAISE(ABORT, 'injected GC sweep failure'); END;",
            "sqlite_gc_sweep_failure",
        ),
        (
            "head",
            "CREATE TRIGGER fail_gc_head BEFORE UPDATE ON cymule_heads
             BEGIN SELECT RAISE(ABORT, 'injected GC head failure'); END;",
            "sqlite_head_cas_failure",
        ),
    ] {
        let directory = tempdir().expect("temporary directory");
        let path = directory.path().join(format!("gc-fault-{name}.sqlite"));
        let domain = "domain:gc-fault";
        let mut store = SqliteStore::open(&path, domain).expect("store opens");
        let initial = initialize(&mut store);
        let orphan_id = format!("sha256:{}", "5".repeat(64));
        let raw = Connection::open(&path).expect("raw connection opens");
        raw.execute(
            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
             VALUES (?1,?2,x'7b7d')",
            (domain, &orphan_id),
        )
        .expect("orphan inserts");
        raw.execute_batch(trigger).expect("fault trigger installs");
        let error = advance_current(&mut store)
            .expect_err("injected GC statement failure aborts transaction");
        assert!(matches!(
            error,
            DurableError::Substrate { code, .. } if code == expected_code
        ));
        raw.execute_batch(
            "DROP TRIGGER IF EXISTS fail_gc_sweep; DROP TRIGGER IF EXISTS fail_gc_head;",
        )
        .expect("fault trigger removes");
        assert_eq!(
            store
                .load_full_audit()
                .expect("state reloads")
                .expect("state"),
            initial
        );
        let orphan_exists: bool = raw
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM cymule_state_root_objects
                    WHERE domain=?1 AND object_id=?2
                 )",
                (domain, &orphan_id),
                |row| row.get(0),
            )
            .expect("orphan presence reads");
        assert!(orphan_exists);
        assert_eq!(store.stats().expect("stats read").gc_receipts, 0);
    }
}

#[test]
fn gc_reconciliation_delete_failure_is_retryable_under_the_same_head() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("gc-reconcile-fault.sqlite");
    let domain = "domain:gc-reconcile-fault";
    let mut store = SqliteStore::open(&path, domain).expect("store opens");
    initialize(&mut store);
    let orphan_id = format!("sha256:{}", "4".repeat(64));
    let raw = Connection::open(&path).expect("raw connection opens");
    raw.execute(
        "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
         VALUES (?1,?2,x'7b7d')",
        (domain, &orphan_id),
    )
    .expect("orphan inserts");
    let receipt = advance_current(&mut store).expect("GC generation commits");
    let current = store
        .load_full_audit()
        .expect("state loads")
        .expect("state");
    raw.execute(
        "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
         VALUES (?1,?2,x'7b7d')",
        (domain, &orphan_id),
    )
    .expect("authorized object reappears for reconciliation fault test");
    raw.execute_batch(
        "CREATE TRIGGER fail_gc_reconcile BEFORE DELETE ON cymule_state_root_objects
         BEGIN SELECT RAISE(ABORT, 'injected GC reconcile failure'); END;",
    )
    .expect("reconcile fault trigger installs");
    assert!(matches!(
        reconcile_current(&mut store),
        Err(DurableError::Substrate { code, .. }) if code == "sqlite_gc_sweep_failure"
    ));
    assert_eq!(
        store
            .load_full_audit()
            .expect("state reloads")
            .expect("state")
            .head,
        current.head
    );
    raw.execute_batch("DROP TRIGGER fail_gc_reconcile;")
        .expect("reconcile fault trigger removes");
    assert_eq!(
        reconcile_current(&mut store).expect("same-head reconciliation retries"),
        receipt
    );
    let orphan_exists: bool = raw
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM cymule_state_root_objects
                WHERE domain=?1 AND object_id=?2
             )",
            (domain, &orphan_id),
            |row| row.get(0),
        )
        .expect("orphan absence reads");
    assert!(!orphan_exists);
}

#[test]
fn whole_state_and_every_prior_generation_are_rejected_without_mutation() {
    let directory = tempdir().expect("temporary directory");

    let whole_path = directory.path().join("whole.sqlite");
    let whole = Connection::open(&whole_path).expect("raw database opens");
    whole
        .execute_batch(
            "CREATE TABLE cymule_state (
                domain TEXT PRIMARY KEY NOT NULL,
                schema_version TEXT NOT NULL,
                revision TEXT NOT NULL,
                state_json BLOB NOT NULL
             ) STRICT;",
        )
        .expect("whole-state table creates");
    drop(whole);
    assert!(
        matches!(SqliteStore::open(&whole_path, "domain:whole"), Err(DurableError::Substrate { code, .. })
            if code == cymule_store_sqlite::UNSUPPORTED_STORE_GENERATION_CODE)
    );
    let whole = Connection::open(&whole_path).expect("whole database reopens");
    let meta_exists: bool = whole
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_schema
                WHERE type='table' AND name='cymule_store_meta'
             )",
            [],
            |row| row.get(0),
        )
        .expect("metadata absence reads");
    assert!(!meta_exists);

    for version in [
        "cymule.sqlite-store/1",
        "cymule.sqlite-store/2",
        "cymule.sqlite-store/3",
        "cymule.sqlite-store/4",
        "cymule.sqlite-store/5",
    ] {
        let path = directory.path().join(format!(
            "prior-{}.sqlite",
            version.rsplit('/').next().expect("generation exists")
        ));
        let connection = Connection::open(&path).expect("prior database opens");
        connection
            .execute_batch(
                "CREATE TABLE cymule_store_meta (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    schema_version TEXT NOT NULL
                 ) STRICT;",
            )
            .expect("prior metadata creates");
        connection
            .execute(
                "INSERT INTO cymule_store_meta(singleton,schema_version) VALUES (1,?1)",
                [version],
            )
            .expect("prior generation records");
        drop(connection);
        assert!(
            matches!(SqliteStore::open(&path, "domain:prior"), Err(DurableError::Substrate { code, .. })
                if code == cymule_store_sqlite::UNSUPPORTED_STORE_GENERATION_CODE),
            "{version} must be rejected exactly"
        );
        let retained = Connection::open(&path).expect("prior database reopens");
        let heads_exist: bool = retained
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_schema
                    WHERE type='table' AND name='cymule_heads'
                 )",
                [],
                |row| row.get(0),
            )
            .expect("head absence reads");
        assert!(!heads_exist, "{version} must not be repaired");
    }
}

#[test]
fn partial_or_wrong_current_schema_is_never_repaired() {
    let directory = tempdir().expect("temporary directory");
    let partial_path = directory.path().join("partial.sqlite");
    let partial = Connection::open(&partial_path).expect("partial database opens");
    partial
        .execute_batch(
            "CREATE TABLE cymule_store_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                schema_version TEXT NOT NULL
             ) STRICT;
             INSERT INTO cymule_store_meta VALUES (1,'cymule.sqlite-store/6');",
        )
        .expect("partial schema creates");
    drop(partial);
    assert!(
        matches!(SqliteStore::open(&partial_path, "domain:partial"), Err(DurableError::Substrate { code, .. })
            if code == cymule_store_sqlite::UNSUPPORTED_STORE_GENERATION_CODE)
    );
    let partial = Connection::open(&partial_path).expect("partial database reopens");
    let heads_exist: bool = partial
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_schema WHERE type='table' AND name='cymule_heads'
             )",
            [],
            |row| row.get(0),
        )
        .expect("head absence reads");
    assert!(!heads_exist);

    let wrong_path = directory.path().join("wrong-ddl.sqlite");
    drop(SqliteStore::open(&wrong_path, "domain:wrong-ddl").expect("schema initializes"));
    let wrong = Connection::open(&wrong_path).expect("raw database opens");
    wrong
        .execute_batch(
            "ALTER TABLE cymule_heads RENAME TO replaced_heads;
             CREATE TABLE cymule_heads (
                domain TEXT PRIMARY KEY NOT NULL, head_json TEXT NOT NULL
             ) STRICT;
             DROP TABLE replaced_heads;",
        )
        .expect("head DDL changes");
    drop(wrong);
    assert!(
        matches!(SqliteStore::open(&wrong_path, "domain:wrong-ddl"), Err(DurableError::Substrate { code, .. })
            if code == cymule_store_sqlite::UNSUPPORTED_STORE_GENERATION_CODE)
    );
}

#[test]
fn every_foreign_non_sqlite_schema_object_is_rejected() {
    let directory = tempdir().expect("temporary directory");
    for (kind, ddl) in [
        (
            "table",
            "CREATE TABLE unrelated_table (value INTEGER NOT NULL) STRICT;",
        ),
        (
            "index",
            "CREATE INDEX unrelated_index ON cymule_heads(domain);",
        ),
        (
            "view",
            "CREATE VIEW unrelated_view AS SELECT domain FROM cymule_heads;",
        ),
        (
            "trigger",
            "CREATE TRIGGER unrelated_trigger AFTER INSERT ON cymule_heads
             BEGIN SELECT 1; END;",
        ),
    ] {
        let path = directory.path().join(format!("foreign-{kind}.sqlite"));
        if kind == "table" {
            let connection = Connection::open(&path).expect("raw database opens");
            connection
                .execute_batch(ddl)
                .expect("foreign table creates");
            drop(connection);
        } else {
            drop(SqliteStore::open(&path, "domain:foreign").expect("schema initializes"));
            let connection = Connection::open(&path).expect("raw database opens");
            connection
                .execute_batch(ddl)
                .expect("foreign object creates");
            drop(connection);
        }
        assert!(
            matches!(SqliteStore::open(&path, "domain:foreign"), Err(DurableError::Substrate { code, .. })
                if code == cymule_store_sqlite::UNSUPPORTED_STORE_GENERATION_CODE),
            "foreign {kind} must be rejected"
        );
        let retained = Connection::open(&path).expect("foreign database reopens");
        let exists: bool = retained
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM sqlite_schema
                    WHERE name=?1 AND name NOT GLOB 'sqlite_*'
                 )",
                [format!("unrelated_{kind}")],
                |row| row.get(0),
            )
            .expect("foreign object retention reads");
        assert!(exists, "rejected open must not mutate foreign {kind}");
    }
}

#[test]
fn concurrent_empty_initializers_publish_one_complete_generation() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("concurrent-init.sqlite");
    let barrier = Arc::new(Barrier::new(3));
    let workers = (0..2)
        .map(|index| {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                SqliteStore::open(&path, format!("domain:{index}")).map(drop)
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let outcomes = workers
        .into_iter()
        .map(|worker| worker.join().expect("initializer joins"))
        .collect::<Vec<_>>();
    assert!(outcomes.iter().any(Result::is_ok));
    drop(SqliteStore::open(&path, "domain:readback").expect("exact schema reopens"));
}

#[test]
fn physical_schema_is_exactly_five_tables() {
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("exact-schema.sqlite");
    drop(SqliteStore::open(&path, "domain:schema").expect("store opens"));
    let connection = Connection::open(&path).expect("raw connection opens");
    let mut statement = connection
        .prepare(
            "SELECT name FROM sqlite_schema
             WHERE type='table' AND name NOT GLOB 'sqlite_*'
             ORDER BY name",
        )
        .expect("schema query prepares");
    let tables = statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("schema query runs")
        .collect::<Result<Vec<_>, _>>()
        .expect("schema rows read");
    assert_eq!(
        tables,
        [
            "cymule_gc_receipts",
            "cymule_heads",
            "cymule_machine_command_archive_objects",
            "cymule_state_root_objects",
            "cymule_store_meta",
        ]
    );
    let segment_table: Option<String> = connection
        .query_row(
            "SELECT name FROM sqlite_schema
             WHERE name IN ('cymule_checkpoints','cymule_segments') LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .expect("legacy table query runs");
    assert!(segment_table.is_none());
}

#[derive(Debug, Clone, Copy)]
struct AgentPrefixPlugin;

fn agent_prefix_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "sqlite-agent-prefix-tests@1".to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::new(),
    }
}

impl PluginHost for AgentPrefixPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: agent_prefix_manifest(),
            }),
            other => Err(RuntimeError::plugin_defect(format!(
                "unexpected Agent-prefix test plugin request: {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UnusedAgentPrefixClock;

fn unused_agent_prefix_clock<T>() -> DurableResult<T> {
    Err(DurableError::RuntimeDefect {
        code: "sqlite_agent_prefix_clock_unused".to_owned(),
        message: "Agent Session message persistence must not consult an execution Clock".to_owned(),
    })
}

impl ClockObservationAuthority for UnusedAgentPrefixClock {
    fn resolve(&mut self, _reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        unused_agent_prefix_clock()
    }
}

impl ExecutionClockAuthority for UnusedAgentPrefixClock {
    fn with_current_head(
        &mut self,
        _reference: &ClockObservationRef,
        _commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        unused_agent_prefix_clock()
    }
}

#[derive(Debug, Default)]
struct UnusedAgentPrefixProviders;

fn unused_agent_prefix_provider<T>() -> ProtocolResult<T> {
    Err(ProtocolError::Validation(
        "Agent Session message persistence must not consult an Agent provider".to_owned(),
    ))
}

impl AgentProviders for UnusedAgentPrefixProviders {
    fn publish_agent_stream(
        &mut self,
        _intent: &AgentStreamPublicationIntent,
    ) -> ProtocolResult<AgentStreamPublicationObservation> {
        unused_agent_prefix_provider()
    }

    fn observe_agent_stream_publication(
        &mut self,
        _intent: &AgentStreamPublicationIntent,
    ) -> ProtocolResult<AgentStreamPublicationObservation> {
        unused_agent_prefix_provider()
    }

    fn bind_agent_workspace(
        &mut self,
        _command: &AgentWorkspaceCommand,
    ) -> ProtocolResult<AgentHostBinding> {
        unused_agent_prefix_provider()
    }

    fn dispatch_agent_workspace(
        &mut self,
        _command: &AgentWorkspaceCommand,
        _occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentWorkspaceSubmission> {
        unused_agent_prefix_provider()
    }

    fn observe_agent_workspace(
        &mut self,
        _command: &AgentWorkspaceCommand,
        _occurrence: &AgentHostOccurrence,
    ) -> ProtocolResult<AgentWorkspaceObservation> {
        unused_agent_prefix_provider()
    }
}

type AgentPrefixRuntime = DurableRuntimeControl<SqliteStore, AgentPrefixPlugin>;

fn open_agent_prefix_runtime(path: &Path, domain: &str) -> AgentPrefixRuntime {
    let mut store = SqliteStore::open(path, domain).expect("Agent-prefix Store opens");
    initialize(&mut store);
    let binding = ExecutionBinding::for_local_process(
        &agent_prefix_manifest(),
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("Agent-prefix execution binding seals");
    let admission = ExecutionBindingAdmission::admit(AgentPrefixPlugin, binding)
        .expect("Agent-prefix plugin binding admits");
    DurableRuntimeControl::open(store, admission, UnusedAgentPrefixClock)
        .expect("Agent-prefix runtime opens")
}

fn append_agent_prefix_message(
    runtime: &mut AgentPrefixRuntime,
    providers: &mut UnusedAgentPrefixProviders,
    session_id: &str,
    index: u64,
) {
    let source_revision = runtime
        .agent(providers)
        .read_agent_session(&AgentSessionQuery {
            session_id: session_id.to_owned(),
            expected_revision: None,
        })
        .expect("Agent Session source reads")
        .revision;
    let command = AgentCommand::new(
        source_revision,
        AgentCommandAction::SessionUpdate {
            session_id: session_id.to_owned(),
            update: AgentUpdate::Message {
                update_id: format!("update:sqlite-prefix:{index}"),
                message: AgentMessage {
                    message_id: format!("message:sqlite-prefix:{index}"),
                    role: MessageRole::Agent,
                    content: vec![ContentBlock::Text {
                        text: format!("SQLite historical Context message {index}"),
                    }],
                },
            },
        },
    )
    .expect("Agent message command seals");
    let commit = runtime
        .agent(providers)
        .commit_agent(&command)
        .expect("Agent message commits through public Durable Agent control");
    assert_eq!(
        commit.committed_revision.as_ref(),
        Some(&commit.observed_revision),
        "a fresh public Agent command must acknowledge its CAS"
    );
}

fn read_agent_prefix_session(
    runtime: &mut AgentPrefixRuntime,
    providers: &mut UnusedAgentPrefixProviders,
    session_id: &str,
) -> (String, AgentSessionCurrent) {
    let read = runtime
        .agent(providers)
        .read_agent_session(&AgentSessionQuery {
            session_id: session_id.to_owned(),
            expected_revision: None,
        })
        .expect("Agent Session current reads through public control");
    (
        read.revision,
        read.current.expect("Agent Session current exists"),
    )
}

fn agent_prefix_page_query(
    revision: &str,
    session_id: &str,
    message_head: Option<String>,
    message_count: u64,
    end_exclusive: Option<u64>,
    max_entries: u64,
) -> AgentMessagePageQuery {
    AgentMessagePageQuery {
        session_id: session_id.to_owned(),
        expected_message_head: message_head,
        source_message_count: message_count,
        end_exclusive,
        max_entries,
        max_message_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
        max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
        expected_revision: Some(revision.to_owned()),
    }
}

fn read_agent_prefix_pages(
    control: &mut DurableStoreControl<SqliteStore>,
    revision: &str,
    session_id: &str,
    message_head: Option<&str>,
    message_count: u64,
    max_entries: u64,
) -> Vec<AgentMessageCurrent> {
    let mut end_exclusive = None;
    let mut entries = Vec::new();
    loop {
        let read = control
            .agent_read()
            .read_agent_messages(&agent_prefix_page_query(
                revision,
                session_id,
                message_head.map(str::to_owned),
                message_count,
                end_exclusive,
                max_entries,
            ))
            .expect("historical Agent message prefix reads after SQLite reopen");
        assert_eq!(read.revision, revision);
        assert_eq!(read.page.expected_message_head.as_deref(), message_head);
        assert_eq!(read.page.source_message_count, message_count);
        entries.extend(read.page.entries);
        end_exclusive = read.page.next_end_exclusive;
        if end_exclusive.is_none() {
            break;
        }
    }
    entries.sort_by_key(|entry| entry.order.index);
    entries
}

fn agent_message_current_bytes(entries: &[AgentMessageCurrent]) -> usize {
    entries
        .iter()
        .map(|entry| {
            cymule_core::canonical_bytes(entry)
                .expect("Agent message current encodes canonically")
                .len()
        })
        .sum()
}

fn assert_bad_agent_prefix_descriptors_do_not_mutate(
    mut store: SqliteStore,
    revision: &str,
    session_id: &str,
    source_head: Option<&str>,
    source_count: u64,
    later: &AgentSessionCurrent,
) {
    let wrong_head = cymule_core::content_id("test.sqlite.agent-message-head/1", &"wrong")
        .expect("wrong message head derives");
    let invalid_queries = [
        (
            "wrong head",
            agent_prefix_page_query(
                revision,
                session_id,
                Some(wrong_head),
                source_count,
                None,
                256,
            ),
            "agent_message_page_source_head_mismatch",
        ),
        (
            "head/count mismatch",
            agent_prefix_page_query(
                revision,
                session_id,
                source_head.map(str::to_owned),
                source_count - 1,
                None,
                256,
            ),
            "agent_message_page_source_head_mismatch",
        ),
        (
            "count beyond log",
            agent_prefix_page_query(
                revision,
                session_id,
                later.message_head.clone(),
                later.message_count + 1,
                None,
                256,
            ),
            "agent_message_page_source_count_mismatch",
        ),
    ];
    for (case, query, expected_code) in invalid_queries {
        let before_head = store.load_head().expect("pre-failure head reads");
        let before_stats = store.stats().expect("pre-failure Store stats read");
        let mut control = DurableStoreControl::open(store).expect("negative read control opens");
        let error = control
            .agent_read()
            .read_agent_messages(&query)
            .expect_err("invalid historical prefix must fail closed");
        assert!(
            matches!(error, DurableError::HistoryConflict { ref code, .. } if code == expected_code),
            "{case} returned {error:?}"
        );
        store = control.into_store();
        assert_eq!(
            store.load_head().expect("post-failure head reads"),
            before_head,
            "{case} must not mutate the complete Store head"
        );
        assert_eq!(
            store.stats().expect("post-failure Store stats read"),
            before_stats,
            "{case} must not mutate physical Store inventory"
        );
    }
}

#[test]
fn sqlite_reopen_reads_an_old_agent_message_prefix_with_any_page_size_and_rejects_bad_descriptors()
{
    let directory = tempdir().expect("temporary directory");
    let path = directory.path().join("agent-message-prefix.sqlite");
    let domain = "domain:agent-message-prefix";
    let session_id = "session:sqlite-message-prefix";
    let mut runtime = open_agent_prefix_runtime(&path, domain);
    let mut providers = UnusedAgentPrefixProviders;

    for index in 0..3 {
        append_agent_prefix_message(&mut runtime, &mut providers, session_id, index);
    }
    let (source_revision, source) =
        read_agent_prefix_session(&mut runtime, &mut providers, session_id);
    let source_head = source.message_head.clone();
    let source_count = source.message_count;
    assert_eq!(source_count, 3);
    let original = runtime
        .agent(&mut providers)
        .read_agent_messages(&agent_prefix_page_query(
            &source_revision,
            session_id,
            source_head.clone(),
            source_count,
            None,
            256,
        ))
        .expect("original Agent message prefix reads")
        .page
        .entries;
    assert_eq!(original.len(), usize::try_from(source_count).unwrap());

    append_agent_prefix_message(&mut runtime, &mut providers, session_id, 3);
    let (_, later) = read_agent_prefix_session(&mut runtime, &mut providers, session_id);
    assert_eq!(later.message_count, source_count + 1);
    assert_ne!(later.message_head, source_head);

    let (store, _) = runtime.into_parts();
    drop(store);
    let mut reopened =
        SqliteStore::open_read_only(&path, domain).expect("SQLite Store physically reopens");
    let reopened_revision = reopened
        .load_head()
        .expect("reopened head reads")
        .expect("reopened head exists")
        .revision;
    let mut control = DurableStoreControl::open(reopened).expect("reopened Store control opens");

    let page_256 = read_agent_prefix_pages(
        &mut control,
        &reopened_revision,
        session_id,
        source_head.as_deref(),
        source_count,
        256,
    );
    let page_1 = read_agent_prefix_pages(
        &mut control,
        &reopened_revision,
        session_id,
        source_head.as_deref(),
        source_count,
        1,
    );
    assert_eq!(page_256.len(), usize::try_from(source_count).unwrap());
    assert_eq!(page_256, original);
    assert_eq!(page_1, page_256);
    assert_eq!(
        agent_message_current_bytes(&page_1),
        agent_message_current_bytes(&page_256)
    );
    assert_eq!(
        agent_message_current_bytes(&page_256),
        agent_message_current_bytes(&original)
    );

    assert_bad_agent_prefix_descriptors_do_not_mutate(
        control.into_store(),
        &reopened_revision,
        session_id,
        source_head.as_deref(),
        source_count,
        &later,
    );
}

fn seed_reachable_agent_prefix_leaf(
    path: &Path,
    domain: &str,
    session_id: &str,
) -> (String, AgentMessagePageQuery) {
    let mut runtime = open_agent_prefix_runtime(path, domain);
    let mut providers = UnusedAgentPrefixProviders;
    for index in 0..2 {
        append_agent_prefix_message(&mut runtime, &mut providers, session_id, index);
    }
    let (source_revision, source) =
        read_agent_prefix_session(&mut runtime, &mut providers, session_id);
    let query = agent_prefix_page_query(
        &source_revision,
        session_id,
        source.message_head,
        source.message_count,
        None,
        256,
    );
    let retained = runtime
        .agent(&mut providers)
        .read_agent_messages(&query)
        .expect("public Agent page identifies a reachable message current")
        .page
        .entries
        .pop()
        .expect("reachable message exists");
    let leaf = StateRootValue::Leaf {
        kind: StateRootLeafKind::AgentMessageCurrent,
        canonical_json: String::from_utf8(
            cymule_core::canonical_bytes(&retained).expect("message current encodes"),
        )
        .expect("canonical Agent message current is UTF-8"),
    };
    let object_id = cymule_core::content_id(STATE_ROOT_VALUE_VERSION, &leaf)
        .expect("Agent message leaf identity derives");
    let (store, _) = runtime.into_parts();
    drop(store);
    (object_id, query)
}

fn corrupt_reachable_agent_prefix_leaf_and_assert(
    path: &Path,
    domain: &str,
    object_id: &str,
    mut query: AgentMessagePageQuery,
    corruption: &str,
) {
    let raw = Connection::open(path).expect("corruption fixture opens");
    let unchanged_head_bytes = head_bytes(&raw, domain);
    let object_bytes: Vec<u8> = raw
        .query_row(
            "SELECT object_json FROM cymule_state_root_objects
             WHERE domain=?1 AND object_id=?2",
            (domain, object_id),
            |row| row.get(0),
        )
        .expect("reachable Agent message object resolves physically");
    let changed = if corruption == "missing" {
        raw.execute(
            "DELETE FROM cymule_state_root_objects WHERE domain=?1 AND object_id=?2",
            (domain, object_id),
        )
        .expect("reachable Agent message object deletes")
    } else {
        let mut noncanonical = vec![b' '];
        noncanonical.extend(object_bytes);
        raw.execute(
            "UPDATE cymule_state_root_objects SET object_json=?1
             WHERE domain=?2 AND object_id=?3",
            (&noncanonical, domain, object_id),
        )
        .expect("reachable Agent message object becomes noncanonical")
    };
    assert_eq!(changed, 1);
    assert_eq!(head_bytes(&raw, domain), unchanged_head_bytes);
    drop(raw);

    let mut reopened =
        SqliteStore::open_read_only(path, domain).expect("corrupt Store reopens read-only");
    let before_head = reopened.load_head().expect("corrupt Store head reads");
    let before_stats = reopened.stats().expect("corrupt Store stats read");
    query.expected_revision = Some(
        before_head
            .as_ref()
            .expect("corrupt Store retains its head")
            .revision
            .clone(),
    );
    let mut control = DurableStoreControl::open(reopened).expect("corrupt Store control opens");
    assert!(
        matches!(
            control.agent_read().read_agent_messages(&query),
            Err(DurableError::Integrity { .. })
        ),
        "{corruption} reachable Agent message object must fail closed"
    );
    reopened = control.into_store();
    assert_eq!(
        reopened.load_head().expect("post-failure head reads"),
        before_head,
        "{corruption} read must not mutate the Store head"
    );
    assert_eq!(
        reopened.stats().expect("post-failure Store stats read"),
        before_stats,
        "{corruption} read must not mutate physical Store inventory"
    );
    let raw = Connection::open(path).expect("post-failure observer opens");
    assert_eq!(head_bytes(&raw, domain), unchanged_head_bytes);
}

#[test]
fn sqlite_agent_message_prefix_rejects_missing_or_corrupt_reachable_entries_without_head_mutation()
{
    let directory = tempdir().expect("temporary directory");
    for corruption in ["missing", "noncanonical"] {
        let path = directory
            .path()
            .join(format!("agent-message-{corruption}.sqlite"));
        let domain = format!("domain:agent-message-{corruption}");
        let session_id = format!("session:sqlite-message-{corruption}");
        let (object_id, query) = seed_reachable_agent_prefix_leaf(&path, &domain, &session_id);
        corrupt_reachable_agent_prefix_leaf_and_assert(
            &path, &domain, &object_id, query, corruption,
        );
    }
}
