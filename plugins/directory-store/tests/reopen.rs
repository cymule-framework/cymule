//! Physical-generation, reopen, integrity, and GC tests.
#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use cymule_directory_store::{DirectoryStore, UNSUPPORTED_STORE_GENERATION_CODE};
use cymule_durable::{
    ClockObservationAuthority, DurableError, DurableResult, DurableRuntimeControl, DurableStore,
    DurableStoreControl, ExecutionClockAuthority, GcReceipt, STATE_ROOT_VALUE_VERSION,
    StateRootLeafKind, StateRootValue, StoreHead, StoreStats,
};
use cymule_durable_protocol::{ClockObservation, ClockObservationRef};
use cymule_profile_protocol::ProtocolResult;
use cymule_profile_protocol::agent as agent_protocol;
use cymule_runtime::{
    ExecutionBindingAdmission, PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
};
use fs4::FileExt;

static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn test_directory() -> PathBuf {
    let sequence = DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cymule-directory-store-v5-{}-{sequence}",
        std::process::id()
    ))
}

fn initialize(root: &Path) -> StoreHead {
    let store = DirectoryStore::open(root).expect("store opens");
    let control = DurableStoreControl::initialize(store).expect("zero-Run authority initializes");
    let mut store = control.into_store();
    store
        .load_full_audit()
        .expect("initial StateRoot loads")
        .expect("initial StateRoot exists")
        .head
}

fn advance_current(store: &mut DirectoryStore) -> Result<GcReceipt, DurableError> {
    DurableStoreControl::open(store)?.advance_cold_reclamation()
}

fn reconcile_current(store: &mut DirectoryStore) -> Result<GcReceipt, DurableError> {
    DurableStoreControl::open(store)?.reconcile_cold_reclamation()
}

fn publish_definition(store: &mut DirectoryStore) {
    use cymule_profile_protocol::evolution::{
        EvolutionPersistenceCommand, LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand,
        NoEvolutionProviders,
    };
    let command = EvolutionPersistenceCommand::new(
        "evolution:directory-probe",
        LiveEvolutionCommand::PublishDefinition {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "definition:directory-probe".to_owned(),
            logical_ref: "example.directory-probe".to_owned(),
            definition: cymule_core::Definition {
                id: "probe".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: cymule_core::Region {
                    steps: Vec::new(),
                    result: cymule_core::Expression::Input,
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

const AGENT_PREFIX_SESSION_ID: &str = "session:directory-prefix";

#[derive(Debug, Clone, Copy)]
struct DirectoryPrefixPlugin;

impl PluginHost for DirectoryPrefixPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "directory-prefix-test@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::new(),
                },
            }),
            other => Err(RuntimeError::plugin_defect(format!(
                "directory prefix fixture does not execute providers: {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UnusedClock;

impl ClockObservationAuthority for UnusedClock {
    fn resolve(&mut self, _reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        Err(DurableError::RuntimeDefect {
            code: "directory_prefix_clock_unexpected".to_owned(),
            message: "Agent Session message persistence does not use a Clock".to_owned(),
        })
    }
}

impl ExecutionClockAuthority for UnusedClock {
    fn with_current_head(
        &mut self,
        _reference: &ClockObservationRef,
        _commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        Err(DurableError::RuntimeDefect {
            code: "directory_prefix_clock_unexpected".to_owned(),
            message: "Agent Session message persistence does not use a Clock".to_owned(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct UnusedAgentProviders;

impl agent_protocol::AgentProviders for UnusedAgentProviders {
    fn publish_agent_stream(
        &mut self,
        _intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        unreachable!("Agent Session message persistence does not publish streams")
    }

    fn observe_agent_stream_publication(
        &mut self,
        _intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        unreachable!("Agent Session message persistence does not observe streams")
    }

    fn bind_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
    ) -> ProtocolResult<agent_protocol::AgentHostBinding> {
        unreachable!("Agent Session message persistence does not bind workspaces")
    }

    fn dispatch_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceSubmission> {
        unreachable!("Agent Session message persistence does not dispatch workspaces")
    }

    fn observe_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceObservation> {
        unreachable!("Agent Session message persistence does not observe workspaces")
    }
}

type DirectoryAgentRuntime = DurableRuntimeControl<DirectoryStore, DirectoryPrefixPlugin>;

fn open_agent_runtime(root: &Path) -> DirectoryAgentRuntime {
    let admission = ExecutionBindingAdmission::for_local_process(
        DirectoryPrefixPlugin,
        "sha256:7777777777777777777777777777777777777777777777777777777777777777",
    )
    .expect("operation-free execution binding admits");
    DurableRuntimeControl::open(
        DirectoryStore::open(root).expect("Agent seed Store opens"),
        admission,
        UnusedClock,
    )
    .expect("Agent seed Runtime opens")
}

fn agent_message_command(source_revision: &str, index: u64) -> agent_protocol::AgentCommand {
    agent_protocol::AgentCommand::new(
        source_revision.to_owned(),
        agent_protocol::AgentCommandAction::SessionUpdate {
            session_id: AGENT_PREFIX_SESSION_ID.to_owned(),
            update: agent_protocol::AgentUpdate::Message {
                update_id: format!("update:directory-prefix:{index}"),
                message: agent_protocol::AgentMessage {
                    message_id: format!("message:directory-prefix:{index}"),
                    role: agent_protocol::MessageRole::Agent,
                    content: vec![agent_protocol::ContentBlock::Text {
                        text: format!("directory prefix message {index}"),
                    }],
                },
            },
        },
    )
    .expect("Agent message command seals")
}

fn commit_agent_message(
    runtime: &mut DirectoryAgentRuntime,
    providers: &mut UnusedAgentProviders,
    source_revision: &str,
    index: u64,
) -> String {
    let commit = runtime
        .agent(providers)
        .commit_agent(&agent_message_command(source_revision, index))
        .expect("Agent message commits through public Runtime control");
    assert_eq!(
        commit.committed_revision,
        Some(commit.observed_revision.clone()),
        "a fresh seed command must acknowledge its own CAS"
    );
    commit.observed_revision
}

fn agent_message_page_query(
    revision: &str,
    head: Option<String>,
    count: u64,
    end_exclusive: Option<u64>,
    max_entries: u64,
) -> agent_protocol::AgentMessagePageQuery {
    agent_protocol::AgentMessagePageQuery {
        session_id: AGENT_PREFIX_SESSION_ID.to_owned(),
        expected_message_head: head,
        source_message_count: count,
        end_exclusive,
        max_entries,
        max_message_canonical_bytes: agent_protocol::MAX_AGENT_PAGE_BYTES as u64,
        max_canonical_bytes: agent_protocol::MAX_AGENT_PAGE_BYTES as u64,
        expected_revision: Some(revision.to_owned()),
    }
}

fn agent_message_canonical_bytes(entries: &[agent_protocol::AgentMessageCurrent]) -> usize {
    entries
        .iter()
        .map(|entry| {
            cymule_core::canonical_bytes(entry)
                .expect("Agent message current encodes")
                .len()
        })
        .sum()
}

fn physical_snapshot(root: &Path) -> (Vec<u8>, StoreHead, StoreStats) {
    let head_bytes = fs::read(root.join("head.json")).expect("physical head bytes read");
    let head = cymule_core::decode_json(&head_bytes).expect("physical head decodes");
    let store = DirectoryStore::open_read_only(root).expect("physical observer opens");
    let stats = store.stats().expect("physical stats read");
    (head_bytes, head, stats)
}

#[test]
fn absent_new_node_probe_does_not_block_public_profile_publication() {
    let directory = test_directory();
    let initial = initialize(&directory);
    let mut store = DirectoryStore::open(&directory).expect("Store opens");
    let manifest = store
        .load_state_root_manifest(&initial.state_root_manifest_id)
        .expect("manifest reads")
        .expect("manifest exists");
    let missing = cymule_core::content_id("test.directory.new-node/1", &"probe")
        .expect("new content identity derives");
    store
        .with_state_root_resolver(&manifest, |resolver| {
            assert!(resolver.load_state_root_object(&missing)?.is_none());
            Ok(())
        })
        .expect("an absent new-node probe is not reachable corruption");
    assert_eq!(
        store.load_head().expect("head reads"),
        Some(initial.clone())
    );
    publish_definition(&mut store);
    let current = store
        .load_full_audit()
        .expect("new complete closure audits")
        .expect("state exists");
    assert_ne!(current.head.revision, initial.revision);
    assert_eq!(current.head.sequence, initial.sequence + 1);
    fs::remove_dir_all(directory).expect("test directory removes");
}

fn decode_head(root: &Path) -> StoreHead {
    cymule_core::decode_json(&fs::read(root.join("head.json")).expect("head reads"))
        .expect("head decodes")
}

fn state_root_path(root: &Path, id: &str) -> PathBuf {
    root.join("state-root-objects")
        .join(cymule_core::sha256_bytes(id.as_bytes()))
}

fn assert_unsupported(error: DurableError) {
    match error {
        DurableError::Substrate { code, .. } => {
            assert_eq!(code, UNSUPPORTED_STORE_GENERATION_CODE);
        }
        other => panic!("expected unsupported generation, received {other}"),
    }
}

fn root_entry_names(root: &Path) -> BTreeSet<String> {
    fs::read_dir(root)
        .expect("root entries list")
        .map(|entry| {
            entry
                .expect("root entry reads")
                .file_name()
                .into_string()
                .expect("test entry name is Unicode")
        })
        .collect()
}

struct HistoricalAgentPrefix {
    revision: String,
    head: String,
    count: u64,
    original_entries: Vec<agent_protocol::AgentMessageCurrent>,
    latest_session: agent_protocol::AgentSessionCurrent,
}

fn seed_historical_agent_prefix(root: &Path) -> HistoricalAgentPrefix {
    let initial = initialize(root);
    let mut runtime = open_agent_runtime(root);
    let mut providers = UnusedAgentProviders;
    let mut revision = initial.revision;
    for index in 0..3 {
        revision = commit_agent_message(&mut runtime, &mut providers, &revision, index);
    }
    let prefix_session = runtime
        .agent(&mut providers)
        .read_agent_session(&agent_protocol::AgentSessionQuery {
            session_id: AGENT_PREFIX_SESSION_ID.to_owned(),
            expected_revision: Some(revision.clone()),
        })
        .expect("Agent prefix Session reads through public Runtime control")
        .current
        .expect("Agent prefix Session exists");
    assert_eq!(prefix_session.message_count, 3);
    let head = prefix_session
        .message_head
        .expect("nonempty Agent prefix has a head");
    let count = prefix_session.message_count;
    let original = runtime
        .agent(&mut providers)
        .read_agent_messages(&agent_message_page_query(
            &revision,
            Some(head.clone()),
            count,
            None,
            256,
        ))
        .expect("original Agent prefix reads through public Runtime control");
    assert_eq!(original.page.end_exclusive, None);
    assert_eq!(original.page.next_end_exclusive, None);
    assert_eq!(original.page.entries.len() as u64, count);
    revision = commit_agent_message(&mut runtime, &mut providers, &revision, 3);
    let latest_session = runtime
        .agent(&mut providers)
        .read_agent_session(&agent_protocol::AgentSessionQuery {
            session_id: AGENT_PREFIX_SESSION_ID.to_owned(),
            expected_revision: Some(revision.clone()),
        })
        .expect("later Agent Session reads")
        .current
        .expect("later Agent Session exists");
    assert_eq!(latest_session.message_count, count + 1);
    assert_ne!(latest_session.message_head.as_deref(), Some(head.as_str()));
    let (store, _plugin) = runtime.into_parts();
    drop(store);
    HistoricalAgentPrefix {
        revision,
        head,
        count,
        original_entries: original.page.entries,
        latest_session,
    }
}

fn read_agent_prefix_by_page_size(
    reader: &mut cymule_durable::DurableAgentReadControl<'_, DirectoryStore>,
    prefix: &HistoricalAgentPrefix,
    max_entries: u64,
) -> Vec<agent_protocol::AgentMessageCurrent> {
    let mut end_exclusive = None;
    let mut entries = Vec::new();
    loop {
        let read = reader
            .read_agent_messages(&agent_message_page_query(
                &prefix.revision,
                Some(prefix.head.clone()),
                prefix.count,
                end_exclusive,
                max_entries,
            ))
            .expect("historical Agent prefix page reopens");
        entries.extend(read.page.entries);
        end_exclusive = read.page.next_end_exclusive;
        if end_exclusive.is_none() {
            break;
        }
    }
    entries.sort_by_key(|entry| entry.order.index);
    entries
}

fn assert_invalid_agent_prefix_descriptors(
    reader: &mut cymule_durable::DurableAgentReadControl<'_, DirectoryStore>,
    prefix: &HistoricalAgentPrefix,
) {
    let wrong_count = reader
        .read_agent_messages(&agent_message_page_query(
            &prefix.revision,
            Some(prefix.head.clone()),
            prefix.count - 1,
            None,
            256,
        ))
        .expect_err("wrong historical Agent prefix count fails closed");
    assert!(matches!(
        wrong_count,
        DurableError::HistoryConflict { code, .. }
            if code == "agent_message_page_source_head_mismatch"
    ));
    let wrong_head = reader
        .read_agent_messages(&agent_message_page_query(
            &prefix.revision,
            prefix.latest_session.message_head.clone(),
            prefix.count,
            None,
            256,
        ))
        .expect_err("wrong historical Agent prefix head fails closed");
    assert!(matches!(
        wrong_head,
        DurableError::HistoryConflict { code, .. }
            if code == "agent_message_page_source_head_mismatch"
    ));
    let beyond_log = reader
        .read_agent_messages(&agent_message_page_query(
            &prefix.revision,
            prefix.latest_session.message_head.clone(),
            prefix.latest_session.message_count + 1,
            None,
            256,
        ))
        .expect_err("Agent prefix count beyond the retained log fails closed");
    assert!(matches!(
        beyond_log,
        DurableError::HistoryConflict { code, .. }
            if code == "agent_message_page_source_count_mismatch"
    ));
}

#[test]
fn historical_agent_context_prefix_reopens_with_page_size_independent_bytes() {
    let directory = test_directory();
    let prefix = seed_historical_agent_prefix(&directory);
    let before = physical_snapshot(&directory);
    let store = DirectoryStore::open_read_only(&directory).expect("Directory Store reopens");
    let mut control = DurableStoreControl::open(store).expect("Store read control reopens");
    {
        let mut reader = control.agent_read();
        let full = reader
            .read_agent_messages(&agent_message_page_query(
                &prefix.revision,
                Some(prefix.head.clone()),
                prefix.count,
                None,
                256,
            ))
            .expect("historical Agent prefix reopens as one page");
        assert_eq!(
            full.page.expected_message_head.as_deref(),
            Some(prefix.head.as_str())
        );
        assert_eq!(full.page.source_message_count, prefix.count);
        assert_eq!(full.page.end_exclusive, None);
        assert_eq!(full.page.next_end_exclusive, None);
        assert_eq!(full.page.entries.len() as u64, prefix.count);
        assert_eq!(full.page.entries, prefix.original_entries);
        let single_entry_pages = read_agent_prefix_by_page_size(&mut reader, &prefix, 1);
        assert_eq!(single_entry_pages, full.page.entries);
        assert_eq!(
            agent_message_canonical_bytes(&single_entry_pages),
            agent_message_canonical_bytes(&full.page.entries)
        );
        assert_eq!(
            agent_message_canonical_bytes(&full.page.entries),
            agent_message_canonical_bytes(&prefix.original_entries)
        );
        assert_invalid_agent_prefix_descriptors(&mut reader, &prefix);
    }
    drop(control.into_store());
    assert_eq!(physical_snapshot(&directory), before);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn reachable_agent_context_entry_corruption_fails_without_head_mutation() {
    let directory = test_directory();
    let initial = initialize(&directory);
    let mut runtime = open_agent_runtime(&directory);
    let mut providers = UnusedAgentProviders;
    let revision = commit_agent_message(&mut runtime, &mut providers, &initial.revision, 0);
    let session = runtime
        .agent(&mut providers)
        .read_agent_session(&agent_protocol::AgentSessionQuery {
            session_id: AGENT_PREFIX_SESSION_ID.to_owned(),
            expected_revision: Some(revision.clone()),
        })
        .expect("Agent Session reads")
        .current
        .expect("Agent Session exists");
    let query = agent_message_page_query(
        &revision,
        session.message_head.clone(),
        session.message_count,
        None,
        256,
    );
    let current = runtime
        .agent(&mut providers)
        .read_agent_messages(&query)
        .expect("reachable Agent message reads")
        .page
        .entries
        .into_iter()
        .next()
        .expect("one reachable Agent message exists");
    let (store, _plugin) = runtime.into_parts();
    drop(store);

    let leaf = StateRootValue::Leaf {
        kind: StateRootLeafKind::AgentMessageCurrent,
        canonical_json: String::from_utf8(
            cymule_core::canonical_bytes(&current).expect("Agent message current encodes"),
        )
        .expect("canonical Agent message current is UTF-8"),
    };
    let object_id = cymule_core::content_id(STATE_ROOT_VALUE_VERSION, &leaf)
        .expect("Agent message StateRoot value identity derives");
    let object_path = state_root_path(&directory, &object_id);
    let retained = fs::read(&object_path).expect("reachable Agent message object reads");
    let authoritative_head = fs::read(directory.join("head.json")).expect("head bytes read");
    let authoritative_decoded: StoreHead =
        cymule_core::decode_json(&authoritative_head).expect("head decodes");

    fs::remove_file(&object_path).expect("reachable Agent message object removes");
    let missing = physical_snapshot(&directory);
    let store = DirectoryStore::open_read_only(&directory).expect("missing leaf layout reopens");
    let mut control = DurableStoreControl::open(store).expect("missing leaf read control opens");
    assert!(matches!(
        control.agent_read().read_agent_messages(&query),
        Err(DurableError::Integrity { .. })
    ));
    drop(control.into_store());
    assert_eq!(physical_snapshot(&directory), missing);
    assert_eq!(
        fs::read(directory.join("head.json")).expect("head rereads"),
        authoritative_head
    );
    assert_eq!(decode_head(&directory), authoritative_decoded);

    fs::write(&object_path, &retained).expect("reachable Agent message object restores");
    fs::write(&object_path, b"{}").expect("reachable Agent message object corrupts");
    let corrupt = physical_snapshot(&directory);
    let store = DirectoryStore::open_read_only(&directory).expect("corrupt leaf layout reopens");
    let mut control = DurableStoreControl::open(store).expect("corrupt leaf read control opens");
    assert!(matches!(
        control.agent_read().read_agent_messages(&query),
        Err(DurableError::Integrity { .. })
    ));
    drop(control.into_store());
    assert_eq!(physical_snapshot(&directory), corrupt);
    assert_eq!(
        fs::read(directory.join("head.json")).expect("head rereads"),
        authoritative_head
    );
    assert_eq!(decode_head(&directory), authoritative_decoded);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn writer_contention_is_an_immediate_conflict() {
    let directory = test_directory();
    DirectoryStore::open(&directory).expect("Store layout initializes");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("head.lock"))
        .expect("writer lock opens");
    FileExt::lock(&lock).expect("test owns writer lock");
    let mut store = DirectoryStore::open_read_only(&directory).expect("observer opens");
    assert!(
        store
            .load_head()
            .expect("read-only observer never takes writer lock")
            .is_none(),
        "empty read-only observation remains available under writer contention"
    );
    let mut writer = DirectoryStore::open(&directory).expect("writer handle opens");
    assert_eq!(
        writer.load_head().expect_err("writer does not wait"),
        DurableError::Conflict {
            expected: Some("writer_available".to_owned()),
            current: Some("writer_active".to_owned()),
        }
    );
    FileExt::unlock(&lock).expect("test writer lock releases");
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn v5_reopens_from_typed_state_root_without_segment_authority() {
    let directory = test_directory();
    let initial = initialize(&directory);
    assert!(directory.join("cymule.directory-store-5").is_dir());
    assert!(
        fs::read_dir(directory.join("cymule.directory-store-5"))
            .expect("bootstrap marker lists")
            .next()
            .is_none()
    );
    assert!(directory.join("state-root-objects").is_dir());
    assert!(directory.join("gc-receipts").is_dir());
    assert!(directory.join("command-archives").is_dir());
    assert!(!directory.join("checkpoints").exists());
    assert!(!directory.join("segments").exists());

    let mut reopened = DirectoryStore::open_read_only(&directory).expect("v5 reopens read-only");
    let stored = reopened
        .load_full_audit()
        .expect("StateRoot reopens")
        .expect("state exists");
    assert_eq!(stored.head, initial);
    stored.verify().expect("reopened closure verifies");
    let stats = reopened.stats().expect("stats read");
    assert!(stats.state_root_objects > 0);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn prior_markers_are_rejected_before_any_io_mutation() {
    for version in [
        "cymule.directory-store/1",
        "cymule.directory-store/2",
        "cymule.directory-store/3",
        "cymule.directory-store/4",
    ] {
        let directory = test_directory();
        fs::create_dir_all(&directory).expect("root creates");
        fs::write(
            directory.join("store-meta.json"),
            format!(r#"{{"schema_version":"{version}"}}"#),
        )
        .expect("prior marker writes");
        let before = root_entry_names(&directory);
        assert_unsupported(DirectoryStore::open(&directory).expect_err("prior marker rejects"));
        assert_eq!(root_entry_names(&directory), before);
        fs::remove_dir_all(directory).expect("test directory removes");
    }
}

#[test]
fn exact_pre_state_root_fixture_is_rejected_without_mutation() {
    let directory = test_directory();
    fs::create_dir_all(&directory).expect("legacy fixture root creates");
    let repository_payload =
        include_bytes!("../../../tests/fixtures/directory-store-66a432c-empty-state.json");
    let writer_payload = repository_payload
        .strip_suffix(b"\n")
        .expect("repository fixture carries one non-writer trailing newline");
    let metadata: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/directory-store-66a432c-empty-state.metadata.json"
    ))
    .expect("legacy fixture metadata decodes");
    assert_eq!(
        metadata["source_commit"],
        "66a432c45c81a74dfa3b030783a75ad7df5b772e"
    );
    assert_eq!(
        metadata["payload_sha256"],
        cymule_core::sha256_bytes(writer_payload)
    );
    let legacy_state = directory.join("state.json");
    fs::write(&legacy_state, writer_payload).expect("exact legacy writer bytes install");
    let before = root_entry_names(&directory);

    assert_unsupported(
        DirectoryStore::open_read_only(&directory)
            .expect_err("read-only open rejects the exact predecessor payload"),
    );
    assert_eq!(root_entry_names(&directory), before);
    assert_eq!(
        fs::read(&legacy_state).expect("legacy payload rereads"),
        writer_payload
    );

    assert_unsupported(
        DirectoryStore::open(&directory)
            .expect_err("writable open rejects the exact predecessor payload"),
    );
    assert_eq!(root_entry_names(&directory), before);
    assert_eq!(
        fs::read(&legacy_state).expect("legacy payload remains exact"),
        writer_payload
    );
    assert!(!directory.join("store-meta.json").exists());
    assert!(!directory.join("cymule.directory-store-5").exists());
    assert!(!directory.join("state-root-objects").exists());
    fs::remove_dir_all(directory).expect("legacy fixture removes");
}

#[test]
fn noncanonical_current_marker_is_an_exact_unsupported_generation() {
    let directory = test_directory();
    DirectoryStore::open(&directory).expect("current layout initializes");
    fs::write(
        directory.join("store-meta.json"),
        b"{ \"schema_version\": \"cymule.directory-store/5\" }",
    )
    .expect("noncanonical marker writes");
    assert_unsupported(
        DirectoryStore::open_read_only(&directory)
            .expect_err("noncanonical current marker is rejected"),
    );
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn generation_marker_read_io_remains_its_original_substrate_error() {
    let directory = test_directory();
    DirectoryStore::open(&directory).expect("current layout initializes");
    let marker = directory.join("store-meta.json");
    fs::set_permissions(&marker, fs::Permissions::from_mode(0o000))
        .expect("marker permissions remove read access");
    let error = DirectoryStore::open_read_only(&directory)
        .expect_err("unreadable marker fails as provider I/O");
    fs::set_permissions(&marker, fs::Permissions::from_mode(0o600))
        .expect("marker permissions restore");
    assert!(matches!(
        error,
        DurableError::Substrate { code, .. } if code == "directory_object_read_io"
    ));
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn unmarked_head_lock_is_rejected_without_becoming_initialization_authority() {
    let directory = test_directory();
    fs::create_dir_all(&directory).expect("root creates");
    let lock_path = directory.join("head.lock");
    fs::write(&lock_path, b"unrelated lock bytes").expect("unrelated lock writes");
    let before = root_entry_names(&directory);
    let retained = fs::read(&lock_path).expect("unrelated lock reads");

    assert_unsupported(
        DirectoryStore::open(&directory).expect_err("unmarked lock-only directory rejects"),
    );
    assert_eq!(root_entry_names(&directory), before);
    assert_eq!(fs::read(&lock_path).expect("lock rereads"), retained);
    assert!(!directory.join("store-meta.next").exists());
    assert!(!directory.join("store-meta.json").exists());
    assert!(!directory.join("cymule.directory-store-5").exists());
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn legacy_families_are_rejected_independently_of_the_marker() {
    for current_marker in [false, true] {
        let directory = test_directory();
        fs::create_dir_all(&directory).expect("root creates");
        if current_marker {
            fs::write(
                directory.join("store-meta.json"),
                r#"{"schema_version":"cymule.directory-store/5"}"#,
            )
            .expect("v5 marker writes");
        }
        fs::create_dir(directory.join("checkpoints")).expect("legacy family creates");
        fs::create_dir(directory.join("segments")).expect("legacy family creates");
        let before = root_entry_names(&directory);
        assert_unsupported(DirectoryStore::open(&directory).expect_err("legacy families reject"));
        assert_eq!(root_entry_names(&directory), before);
        fs::remove_dir_all(directory).expect("test directory removes");
    }
}

#[test]
fn partial_v5_layout_is_rejected_without_repair() {
    let directory = test_directory();
    fs::create_dir_all(&directory).expect("root creates");
    fs::write(
        directory.join("store-meta.json"),
        r#"{"schema_version":"cymule.directory-store/5"}"#,
    )
    .expect("v5 marker writes");
    fs::write(directory.join("head.lock"), b"").expect("head lock writes");
    for family in ["state-root-objects", "gc-receipts", "command-archives"] {
        fs::create_dir(directory.join(family)).expect("required family creates");
    }
    let before = root_entry_names(&directory);
    assert_unsupported(DirectoryStore::open(&directory).expect_err("partial v5 rejects"));
    assert_eq!(root_entry_names(&directory), before);
    assert!(!directory.join("objects.lock").exists());
    assert!(!directory.join("object-staging").exists());
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn stale_gc_cas_conflicts_on_physical_token_even_when_revision_is_equal() {
    let directory = test_directory();
    let semantic_head = initialize(&directory);
    let current = DirectoryStore::open(&directory).expect("current opens");
    let stale = DirectoryStore::open(&directory).expect("stale handle opens");
    let mut current = DurableStoreControl::open(current).expect("current control opens");
    let mut stale = DurableStoreControl::open(stale).expect("stale control opens");
    current
        .advance_cold_reclamation()
        .expect("physical GC generation publishes");
    let physical_head = DirectoryStore::open_read_only(&directory)
        .expect("current observer opens")
        .load_full_audit()
        .expect("current reloads")
        .expect("state remains")
        .head;
    assert_eq!(physical_head.revision, semantic_head.revision);
    assert_ne!(physical_head.physical_token, semantic_head.physical_token);
    assert!(physical_head.gc_sequence > semantic_head.gc_sequence);

    let error = stale
        .advance_cold_reclamation()
        .expect_err("stale physical head cannot reclaim");
    assert_eq!(
        error,
        DurableError::Conflict {
            expected: Some(semantic_head.physical_token),
            current: Some(physical_head.physical_token),
        }
    );
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn reconciliation_requires_an_exact_head_pinned_receipt() {
    let directory = test_directory();
    let head = initialize(&directory);
    let mut store = DirectoryStore::open(&directory).expect("store opens");
    let error = reconcile_current(&mut store)
        .expect_err("a semantic head has no reclamation receipt to reconcile");
    assert!(matches!(
        error,
        DurableError::Validation(message) if message.contains("head-pinned GC receipt")
    ));
    assert_eq!(decode_head(&directory), head);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn noncanonical_head_is_integrity_failure() {
    let directory = test_directory();
    initialize(&directory);
    let path = directory.join("head.json");
    let mut bytes = fs::read(&path).expect("head reads");
    bytes.push(b'\n');
    fs::write(&path, bytes).expect("noncanonical head writes");
    let mut store = DirectoryStore::open(&directory).expect("layout still opens");
    assert!(matches!(
        store.load_full_audit(),
        Err(DurableError::Integrity { code, .. }) if code == "directory_head_object"
    ));
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn head_sequence_must_match_the_pinned_manifest() {
    let directory = test_directory();
    initialize(&directory);
    let mut head = decode_head(&directory);
    head.sequence += 1;
    fs::write(
        directory.join("head.json"),
        cymule_core::canonical_bytes(&head).expect("tampered head encodes"),
    )
    .expect("tampered head writes");
    let mut store = DirectoryStore::open(&directory).expect("layout still opens");
    assert!(matches!(
        store.load_full_audit(),
        Err(DurableError::Integrity { code, .. })
            if code == "directory_state_root_head_mismatch"
    ));
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn over_limit_head_is_rejected_before_decode() {
    let directory = test_directory();
    initialize(&directory);
    fs::write(directory.join("head.json"), vec![b' '; 256 * 1_024]).expect("oversized head writes");
    let mut store = DirectoryStore::open(&directory).expect("layout still opens");
    assert!(matches!(
        store.load_full_audit(),
        Err(DurableError::Integrity { code, .. }) if code == "directory_head_object"
    ));
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[cfg(unix)]
#[test]
fn symlink_head_and_fifo_manifest_are_rejected_without_following_or_blocking() {
    use std::os::unix::fs::symlink;
    use std::process::Command;

    let symlink_root = test_directory();
    initialize(&symlink_root);
    let head = symlink_root.join("head.json");
    let target = symlink_root.join("head.real");
    fs::rename(&head, &target).expect("head moves");
    symlink(&target, &head).expect("head symlink creates");
    assert_unsupported(
        DirectoryStore::open_read_only(&symlink_root).expect_err("symlink head rejects"),
    );
    fs::remove_dir_all(&symlink_root).expect("symlink test directory removes");

    let fifo_root = test_directory();
    let head = initialize(&fifo_root);
    let manifest = state_root_path(&fifo_root, &head.state_root_manifest_id);
    fs::remove_file(&manifest).expect("manifest removes");
    let status = Command::new("mkfifo")
        .arg(&manifest)
        .status()
        .expect("mkfifo executes");
    assert!(status.success());
    let mut store = DirectoryStore::open_read_only(&fifo_root).expect("layout opens");
    assert!(matches!(
        store.load_full_audit(),
        Err(DurableError::Integrity { code, .. })
            if code == "directory_state_root_object_locator"
    ));
    fs::remove_dir_all(&fifo_root).expect("FIFO test directory removes");
}

#[test]
fn orphan_alias_is_rejected_before_gc_can_mutate_head() {
    let directory = test_directory();
    let head = initialize(&directory);
    let source = state_root_path(&directory, &head.state_root_manifest_id);
    let alias = directory
        .join("state-root-objects")
        .join("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    fs::copy(&source, &alias).expect("alias copies");
    let before = decode_head(&directory);
    let mut store = DirectoryStore::open(&directory).expect("store opens");
    assert!(matches!(
        advance_current(&mut store),
        Err(DurableError::Integrity { code, .. })
            if code == "directory_state_root_object_locator"
    ));
    assert_eq!(decode_head(&directory), before);
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn reopen_rejects_a_cross_family_alias_of_current_authority() {
    let directory = test_directory();
    let head = initialize(&directory);
    let alias = directory.join("command-archives").join(format!(
        "segment-{}",
        cymule_core::sha256_bytes(head.state_root_manifest_id.as_bytes())
    ));
    fs::write(alias, b"{}").expect("cross-family alias writes");

    let mut store = DirectoryStore::open_read_only(&directory).expect("layout opens");
    assert!(matches!(
        store.load_full_audit(),
        Err(DurableError::Integrity { code, .. })
            if code == "directory_physical_object_alias"
    ));
    fs::remove_dir_all(directory).expect("test directory removes");
}

#[test]
fn missing_reachable_state_root_object_is_integrity_corruption() {
    let directory = test_directory();
    initialize(&directory);
    let mut store = DirectoryStore::open(&directory).expect("store opens");
    publish_definition(&mut store);
    let current = store
        .load_full_audit()
        .expect("current state loads")
        .expect("current state exists");
    let manifest_path = state_root_path(&directory, &current.head.state_root_manifest_id);
    let removable = fs::read_dir(directory.join("state-root-objects"))
        .expect("state-root objects list")
        .map(|entry| entry.expect("state-root entry").path())
        .filter(|path| path != &manifest_path)
        .collect::<Vec<_>>();
    assert!(!removable.is_empty());
    for path in removable {
        fs::remove_file(path).expect("non-manifest state-root object removes");
    }
    assert!(matches!(
        store.load_full_audit(),
        Err(DurableError::Integrity { .. })
    ));
    assert!(matches!(
        advance_current(&mut store),
        Err(DurableError::Integrity { .. })
    ));
    assert_eq!(
        store
            .load_head()
            .expect("corrupt closure does not change head"),
        Some(current.head)
    );
    fs::remove_dir_all(directory).expect("test directory removes");
}
