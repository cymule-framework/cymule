//! Real `SQLite` `/6` process-death conformance for Agent-owned transitions.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use cymule_agent::{
    AgentError, AgentHost, AgentInteractionController, AgentMessageReader, AgentPersistence,
    AgentRecoveryController, AgentResult, AgentStreamController,
};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    ClockObservationAuthority, CoupledCheckpointReceipt, DurableResult, DurableRuntimeControl,
    DurableStore, DurableStoreControl, ExecutionClockAuthority, GcReceipt, JournalRecordManifest,
    StateRootLeafKind, StateRootManifest, StateRootObject, StateRootResolver, StateRootValue,
    StoreBatch, StoreCommit, StoreHead, StoreReclamation, StoreStats, decode_state_root_object,
};
use cymule_durable_protocol::{ClockObservation, ClockObservationRef};
use cymule_profile_protocol::ProtocolResult;
use cymule_profile_protocol::agent as agent_protocol;
use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError,
    RuntimeResult,
};
use cymule_store_sqlite::{SqliteProcessDeathBarrier, SqliteStore};
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::json;

const STORE_DOMAIN: &str = "domain:agent-process-kill";
const SESSION_ID: &str = "session:agent-process-kill";
const TOOL_ID: &str = "tool:agent-process-kill";
const CLOSE_SESSION_ID: &str = "session:agent-close-process-kill";
const CLOSE_PENDING_TOOL_ID: &str = "tool:agent-close-pending";
const CLOSE_IN_PROGRESS_TOOL_ID: &str = "tool:agent-close-in-progress";
const HOST_TOOL_ID: &str = "tool:agent-process-kill-host";
const OCCURRENCE_ID: &str = "occurrence:agent-process-kill";
const STREAM_ID: &str = "stream:agent-process-kill";
const BINDING_ID: &str = "binding:agent-process-kill/1";
const EXTERNAL_RESOLVER: &str = "resolver:agent-abort-process-kill/1";
const EXTERNAL_DIGEST: &str =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

type AgentRuntime = DurableRuntimeControl<SqliteStore, EmptyPlugin>;

struct EmptyPlugin;

impl PluginHost for EmptyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "agent-process-kill@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::new(),
                },
            }),
            request => Err(RuntimeError::plugin_defect(format!(
                "unexpected Agent process-kill plugin request {request:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UnusedClock;

fn unused_clock<T>() -> DurableResult<T> {
    Err(cymule_durable::DurableError::RuntimeDefect {
        code: "agent_process_kill_clock_unused".to_owned(),
        message:
            "Agent Session, Tool, occurrence, and staged stream persistence does not use a Clock"
                .to_owned(),
    })
}

impl ClockObservationAuthority for UnusedClock {
    fn resolve(&mut self, _reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        unused_clock()
    }
}

impl ExecutionClockAuthority for UnusedClock {
    fn with_current_head(
        &mut self,
        _reference: &ClockObservationRef,
        _commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        unused_clock()
    }
}

#[derive(Default)]
struct UnusedAgentProviders;

impl agent_protocol::AgentProviders for UnusedAgentProviders {
    fn publish_agent_stream(
        &mut self,
        _intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        unreachable!("staged Agent streams do not publish through a provider")
    }

    fn observe_agent_stream_publication(
        &mut self,
        _intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        unreachable!("staged Agent streams do not observe a publication provider")
    }

    fn bind_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
    ) -> ProtocolResult<agent_protocol::AgentHostBinding> {
        unreachable!("Agent process-death conformance does not bind workspaces")
    }

    fn dispatch_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceSubmission> {
        unreachable!("Agent process-death conformance does not dispatch workspaces")
    }

    fn observe_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceObservation> {
        unreachable!("Agent process-death conformance does not observe workspaces")
    }
}

#[derive(Default)]
struct NotAppliedPublicationProvider {
    publish_calls: usize,
}

#[derive(Default)]
struct PublishedPublicationProvider {
    publish_calls: usize,
    unknown: bool,
}

impl agent_protocol::AgentProviders for PublishedPublicationProvider {
    fn publish_agent_stream(
        &mut self,
        intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        self.publish_calls += 1;
        if self.unknown {
            return Ok(agent_protocol::AgentStreamPublicationObservation::Unknown);
        }
        let resource = intent.resource_handle()?;
        Ok(
            agent_protocol::AgentStreamPublicationObservation::Published {
                publication: Box::new(cymule_profile_protocol::resource::ResourcePublication {
                    locators: cymule_profile_protocol::resource::ResourceLocatorSet {
                        locator_version:
                            cymule_profile_protocol::resource::RESOURCE_LOCATOR_VERSION.to_owned(),
                        resource_id: resource.resource_id.clone(),
                        resolver_binding: intent.resolver_binding().to_owned(),
                        locations: vec![
                            cymule_profile_protocol::resource::ResourceLocation::Opaque {
                                reference: "object:agent-process-kill".to_owned(),
                            },
                        ],
                    },
                    resource,
                }),
            },
        )
    }

    fn observe_agent_stream_publication(
        &mut self,
        intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        self.publish_agent_stream(intent)
    }

    fn bind_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
    ) -> ProtocolResult<agent_protocol::AgentHostBinding> {
        unreachable!("publication replay tests do not bind workspaces")
    }

    fn dispatch_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceSubmission> {
        unreachable!("publication replay tests do not dispatch workspaces")
    }

    fn observe_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceObservation> {
        unreachable!("publication replay tests do not observe workspaces")
    }
}

impl agent_protocol::AgentProviders for NotAppliedPublicationProvider {
    fn publish_agent_stream(
        &mut self,
        _intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        self.publish_calls += 1;
        Ok(agent_protocol::AgentStreamPublicationObservation::NotApplied)
    }

    fn observe_agent_stream_publication(
        &mut self,
        _intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> ProtocolResult<agent_protocol::AgentStreamPublicationObservation> {
        unreachable!("NotApplied baseline does not reconcile publication")
    }

    fn bind_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
    ) -> ProtocolResult<agent_protocol::AgentHostBinding> {
        unreachable!("external Abort conformance does not bind workspaces")
    }

    fn dispatch_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceSubmission> {
        unreachable!("external Abort conformance does not dispatch workspaces")
    }

    fn observe_agent_workspace(
        &mut self,
        _command: &agent_protocol::AgentWorkspaceCommand,
        _occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> ProtocolResult<agent_protocol::AgentWorkspaceObservation> {
        unreachable!("external Abort conformance does not observe workspaces")
    }
}

fn initialize_store(path: &Path) {
    let mut store = SqliteStore::open(path, STORE_DOMAIN).expect("Agent SQLite Store opens");
    DurableStoreControl::initialize(&mut store).expect("Agent SQLite /6 genesis commits");
}

fn open_runtime(path: &Path) -> AgentRuntime {
    open_runtime_with_store(
        SqliteStore::open(path, STORE_DOMAIN).expect("Agent SQLite Store reopens"),
    )
}

fn open_runtime_with_store<S: DurableStore>(store: S) -> DurableRuntimeControl<S, EmptyPlugin> {
    let admission = cymule_runtime::ExecutionBindingAdmission::for_local_process(
        EmptyPlugin,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("Agent test execution binding admits");
    DurableRuntimeControl::open(store, admission, UnusedClock).expect("Agent runtime opens")
}

#[derive(Clone)]
struct LedgerHost {
    database: PathBuf,
}

impl LedgerHost {
    fn initialize(path: &Path) {
        let connection = Connection::open(path).expect("Agent provider ledger opens");
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .expect("Agent provider ledger enables WAL");
        connection
            .pragma_update(None, "synchronous", "FULL")
            .expect("Agent provider ledger enables FULL sync");
        connection
            .execute_batch(
                "CREATE TABLE provider_meta (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    dispatch_attempts INTEGER NOT NULL,
                    reconciliations INTEGER NOT NULL
                 ) STRICT;
                 CREATE TABLE provider_results (
                    occurrence_id TEXT PRIMARY KEY NOT NULL,
                    response_json BLOB NOT NULL
                 ) STRICT;
                 INSERT INTO provider_meta(singleton, dispatch_attempts, reconciliations)
                 VALUES (1, 0, 0);",
            )
            .expect("Agent provider ledger initializes");
    }

    fn binding() -> agent_protocol::AgentHostBinding {
        agent_protocol::AgentHostBinding::standalone("host:agent-process-kill/1", BINDING_ID)
            .expect("Agent host binding seals")
    }

    fn response() -> agent_protocol::ToolResponse {
        agent_protocol::ToolResponse {
            tool_call_id: HOST_TOOL_ID.to_owned(),
            content: vec![agent_protocol::ContentBlock::Json {
                value: json!({"provider": "applied"}),
            }],
            occurrence_binding: BINDING_ID.to_owned(),
        }
    }

    fn dispatch(&self, request: &agent_protocol::ToolRequest) -> agent_protocol::ToolResponse {
        assert_eq!(request, &host_request());
        let mut connection = Connection::open(&self.database).expect("provider ledger opens");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("provider ledger transaction begins");
        transaction
            .execute(
                "UPDATE provider_meta SET dispatch_attempts = dispatch_attempts + 1
                 WHERE singleton = 1",
                [],
            )
            .expect("provider attempt records");
        let response = Self::response();
        let bytes = cymule_core::canonical_bytes(&response).expect("provider response encodes");
        transaction
            .execute(
                "INSERT INTO provider_results(occurrence_id, response_json)
                 VALUES (?1, ?2) ON CONFLICT(occurrence_id) DO NOTHING",
                params![OCCURRENCE_ID, bytes],
            )
            .expect("provider result records");
        let retained: Vec<u8> = transaction
            .query_row(
                "SELECT response_json FROM provider_results WHERE occurrence_id = ?1",
                [OCCURRENCE_ID],
                |row| row.get(0),
            )
            .expect("provider result reads");
        assert_eq!(retained, cymule_core::canonical_bytes(&response).unwrap());
        transaction.commit().expect("provider result commits");
        response
    }

    fn reconcile(&self) -> agent_protocol::AgentOccurrenceResolution {
        let mut connection = Connection::open(&self.database).expect("provider ledger opens");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("provider reconciliation transaction begins");
        transaction
            .execute(
                "UPDATE provider_meta SET reconciliations = reconciliations + 1
                 WHERE singleton = 1",
                [],
            )
            .expect("provider reconciliation records");
        let response: Option<Vec<u8>> = transaction
            .query_row(
                "SELECT response_json FROM provider_results WHERE occurrence_id = ?1",
                [OCCURRENCE_ID],
                |row| row.get(0),
            )
            .optional()
            .expect("provider reconciliation reads");
        transaction
            .commit()
            .expect("provider reconciliation commits");
        response.map_or_else(
            || agent_protocol::AgentOccurrenceResolution::NotApplied {
                evidence: vec![agent_protocol::ContentBlock::Text {
                    text: "provider ledger proves no dispatch".to_owned(),
                }],
            },
            |response| agent_protocol::AgentOccurrenceResolution::Completed {
                response: agent_protocol::AgentHostResponse::Tool(
                    cymule_core::decode_json(&response).expect("provider response decodes"),
                ),
            },
        )
    }

    fn counts(&self) -> (u64, u64, u64) {
        let connection = Connection::open(&self.database).expect("provider ledger opens");
        let (attempts, reconciliations): (i64, i64) = connection
            .query_row(
                "SELECT dispatch_attempts, reconciliations FROM provider_meta WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("provider counts read");
        let results: i64 = connection
            .query_row("SELECT COUNT(*) FROM provider_results", [], |row| {
                row.get(0)
            })
            .expect("provider result count reads");
        (
            u64::try_from(attempts).expect("provider attempts are non-negative"),
            u64::try_from(reconciliations).expect("provider reconciliations are non-negative"),
            u64::try_from(results).expect("provider result count is non-negative"),
        )
    }
}

impl AgentHost for LedgerHost {
    fn bind_occurrence(
        &mut self,
        request: &agent_protocol::AgentHostRequest,
    ) -> AgentResult<agent_protocol::AgentHostBinding> {
        assert_eq!(
            request,
            &agent_protocol::AgentHostRequest::Tool(host_request())
        );
        Ok(Self::binding())
    }

    fn select_context(
        &mut self,
        _request: agent_protocol::ContextRequest,
        _messages: &mut dyn AgentMessageReader,
    ) -> AgentResult<agent_protocol::ContextSnapshot> {
        unreachable!("Agent process-death conformance invokes only a Tool host occurrence")
    }

    fn invoke_model(
        &mut self,
        _request: agent_protocol::ModelRequest,
    ) -> AgentResult<agent_protocol::ModelResponse> {
        unreachable!("Agent process-death conformance invokes only a Tool host occurrence")
    }

    fn request_permission(
        &mut self,
        _request: agent_protocol::PermissionRequest,
    ) -> AgentResult<agent_protocol::PermissionResponse> {
        unreachable!("Agent process-death conformance invokes only a Tool host occurrence")
    }

    fn invoke_tool(
        &mut self,
        request: agent_protocol::ToolRequest,
    ) -> AgentResult<agent_protocol::ToolResponse> {
        Ok(self.dispatch(&request))
    }

    fn elicit(
        &mut self,
        _request: agent_protocol::ElicitationRequest,
    ) -> AgentResult<agent_protocol::ElicitationResponse> {
        unreachable!("Agent process-death conformance invokes only a Tool host occurrence")
    }

    fn apply_workspace(
        &mut self,
        _change: agent_protocol::WorkspaceChange,
    ) -> AgentResult<agent_protocol::WorkspaceReceipt> {
        unreachable!("Agent process-death conformance invokes only a Tool host occurrence")
    }

    fn reconcile_occurrence(
        &mut self,
        occurrence: &agent_protocol::AgentHostOccurrence,
    ) -> AgentResult<agent_protocol::AgentOccurrenceResolution> {
        assert_eq!(occurrence.occurrence_binding, Self::binding());
        Ok(self.reconcile())
    }
}

fn host_request() -> agent_protocol::ToolRequest {
    agent_protocol::ToolRequest {
        tool_call_id: HOST_TOOL_ID.to_owned(),
        operation: "test.agent.process-kill".to_owned(),
        input: json!({"value": 1}),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillPhase {
    ObjectsStaged,
    AcknowledgementLost,
}

impl KillPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::ObjectsStaged => "objects_staged_before_head_cas",
            Self::AcknowledgementLost => "cas_acknowledgement_lost",
        }
    }
}

struct FaultingSqliteStore {
    inner: SqliteStore,
    phase: Option<KillPhase>,
    fail_at: usize,
    calls: usize,
    marker: Option<PathBuf>,
}

impl FaultingSqliteStore {
    fn counting(path: &Path) -> Self {
        Self {
            inner: SqliteStore::open(path, STORE_DOMAIN).expect("Agent SQLite Store reopens"),
            phase: None,
            fail_at: 0,
            calls: 0,
            marker: None,
        }
    }

    fn killing(path: &Path, phase: KillPhase, fail_at: usize, marker: PathBuf) -> Self {
        Self {
            inner: SqliteStore::open(path, STORE_DOMAIN).expect("Agent SQLite Store reopens"),
            phase: Some(phase),
            fail_at,
            calls: 0,
            marker: Some(marker),
        }
    }

    const fn calls(&self) -> usize {
        self.calls
    }
}

fn stop_at_store_boundary(marker: &Path, call: usize, phase: KillPhase) -> ! {
    fs::write(marker, format!("{call}:{}", phase.label())).expect("Agent Store kill marker writes");
    loop {
        thread::park_timeout(Duration::from_mins(1));
    }
}

impl DurableStore for FaultingSqliteStore {
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
        self.inner.load_head()
    }

    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> DurableResult<Option<StateRootManifest>> {
        self.inner.load_state_root_manifest(manifest_id)
    }

    fn with_state_root_resolver<T>(
        &mut self,
        current: &StateRootManifest,
        read: impl FnOnce(&mut dyn StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T> {
        self.inner.with_state_root_resolver(current, read)
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<ApplicationJournalPrefix> {
        self.inner
            .application_journal_prefix(manifest, journal_id, count)
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<JournalRecordManifest>> {
        self.inner
            .application_journal_record_manifest(manifest, journal_id, record_id)
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<ApplicationJournalPrefixReplacementAuthority>> {
        self.inner
            .application_journal_prefix_replacement_authority(manifest, replacement_id)
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<CoupledCheckpointReceipt>> {
        self.inner.coupled_checkpoint_receipt(manifest, coupling_id)
    }

    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
        self.inner.load_machine_command_archive_segment(segment_id)
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
        self.inner.load_machine_command_archive_entry(entry_id)
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
        self.inner.load_machine_command_archive_batch(batch_id)
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
        self.inner.load_machine_command_index_node(node_id)
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        self.calls += 1;
        let call = self.calls;
        let selected = self.phase.filter(|_| call == self.fail_at);
        match selected {
            Some(KillPhase::ObjectsStaged) => {
                let marker = self.marker.clone().expect("fault Store owns a marker");
                self.inner.compare_and_commit_with_process_death_barrier(
                    expected,
                    batch,
                    |barrier| match barrier {
                        SqliteProcessDeathBarrier::ObjectsStaged => {
                            stop_at_store_boundary(&marker, call, KillPhase::ObjectsStaged)
                        }
                    },
                )
            }
            Some(KillPhase::AcknowledgementLost) => {
                let result = self.inner.compare_and_commit(expected, batch);
                if result.is_ok() {
                    stop_at_store_boundary(
                        self.marker.as_deref().expect("fault Store owns a marker"),
                        call,
                        KillPhase::AcknowledgementLost,
                    );
                }
                result
            }
            None => self.inner.compare_and_commit(expected, batch),
        }
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<GcReceipt> {
        self.inner.reconcile_cold_reclamation(request)
    }

    fn advance_cold_reclamation(&mut self, request: &StoreReclamation) -> DurableResult<GcReceipt> {
        self.inner.advance_cold_reclamation(request)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        self.inner.stats()
    }
}

struct RecordingPersistence<P> {
    inner: P,
    command_path: Option<PathBuf>,
}

impl<P> RecordingPersistence<P> {
    const fn new(inner: P, command_path: Option<PathBuf>) -> Self {
        Self {
            inner,
            command_path,
        }
    }

    fn record(&self, command: &agent_protocol::AgentCommand) {
        if let Some(path) = &self.command_path {
            fs::write(
                path,
                cymule_core::canonical_bytes(command).expect("Agent command encodes"),
            )
            .expect("Agent recovery command records");
        }
    }
}

impl<P: AgentPersistence> AgentPersistence for RecordingPersistence<P> {
    fn commit_agent(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> AgentResult<agent_protocol::AgentCommit> {
        self.record(command);
        self.inner.commit_agent(command)
    }

    fn finalize_agent_stream(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> AgentResult<agent_protocol::AgentStreamFinalizeOutcome> {
        self.record(command);
        self.inner.finalize_agent_stream(command)
    }

    fn reconcile_agent_stream(
        &mut self,
        command: &agent_protocol::AgentCommand,
        expected_intent: &agent_protocol::AgentStreamPublicationIntent,
    ) -> AgentResult<agent_protocol::AgentStreamFinalizeOutcome> {
        self.inner.reconcile_agent_stream(command, expected_intent)
    }

    fn commit_agent_workspace(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> AgentResult<agent_protocol::AgentWorkspaceCommitOutcome> {
        self.record(command);
        self.inner.commit_agent_workspace(command)
    }

    fn read_agent_session(
        &mut self,
        query: &agent_protocol::AgentSessionQuery,
    ) -> AgentResult<agent_protocol::AgentSessionRead> {
        self.inner.read_agent_session(query)
    }

    fn read_agent_messages(
        &mut self,
        query: &agent_protocol::AgentMessagePageQuery,
    ) -> AgentResult<agent_protocol::AgentMessagePageRead> {
        self.inner.read_agent_messages(query)
    }

    fn read_agent_message(
        &mut self,
        query: &agent_protocol::AgentMessageQuery,
    ) -> AgentResult<agent_protocol::AgentMessageRead> {
        self.inner.read_agent_message(query)
    }

    fn read_agent_tool(
        &mut self,
        query: &agent_protocol::AgentToolQuery,
    ) -> AgentResult<agent_protocol::AgentToolRead> {
        self.inner.read_agent_tool(query)
    }

    fn read_agent_elicitation(
        &mut self,
        query: &agent_protocol::AgentElicitationQuery,
    ) -> AgentResult<agent_protocol::AgentElicitationRead> {
        self.inner.read_agent_elicitation(query)
    }

    fn read_agent_occurrence(
        &mut self,
        query: &agent_protocol::AgentOccurrenceQuery,
    ) -> AgentResult<agent_protocol::AgentOccurrenceRead> {
        self.inner.read_agent_occurrence(query)
    }

    fn read_agent_occurrences(
        &mut self,
        query: &agent_protocol::AgentOccurrencePageQuery,
    ) -> AgentResult<agent_protocol::AgentOccurrencePageRead> {
        self.inner.read_agent_occurrences(query)
    }

    fn read_agent_stream(
        &mut self,
        query: &agent_protocol::AgentStreamQuery,
    ) -> AgentResult<agent_protocol::AgentStreamRead> {
        self.inner.read_agent_stream(query)
    }

    fn read_agent_workspace_admission(
        &mut self,
        query: &agent_protocol::AgentWorkspaceAdmissionQuery,
    ) -> AgentResult<agent_protocol::AgentWorkspaceAdmissionRead> {
        self.inner.read_agent_workspace_admission(query)
    }
}

fn session_read<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentSessionRead> {
    persistence.read_agent_session(&agent_protocol::AgentSessionQuery {
        session_id: SESSION_ID.to_owned(),
        expected_revision: None,
    })
}

fn tool_read<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentToolRead> {
    persistence.read_agent_tool(&agent_protocol::AgentToolQuery {
        session_id: SESSION_ID.to_owned(),
        tool_call_id: TOOL_ID.to_owned(),
        expected_revision: None,
    })
}

fn occurrence_read<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentOccurrenceRead> {
    persistence.read_agent_occurrence(&agent_protocol::AgentOccurrenceQuery {
        session_id: SESSION_ID.to_owned(),
        occurrence_id: OCCURRENCE_ID.to_owned(),
        expected_revision: None,
    })
}

fn stream_read<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentStreamRead> {
    AgentStreamController::load(
        persistence,
        &agent_protocol::AgentStreamQuery {
            session_id: SESSION_ID.to_owned(),
            stream_id: STREAM_ID.to_owned(),
            expected_revision: None,
        },
    )
}

fn open_external_finalize_command<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentCommand> {
    let initial = stream_read(persistence)?;
    assert!(initial.current.is_none());
    AgentStreamController::open(
        persistence,
        &initial.revision,
        SESSION_ID,
        STREAM_ID,
        agent_protocol::AgentStreamTarget::Message {
            message_id: "message:agent-sidecar-replay".to_owned(),
            role: agent_protocol::MessageRole::Agent,
        },
        agent_protocol::AgentStreamDelivery::ExternalResource {
            resolver_binding: EXTERNAL_RESOLVER.to_owned(),
            content: agent_protocol::AgentStreamPublicationContent {
                media_type: "application/octet-stream".to_owned(),
                digest: EXTERNAL_DIGEST.to_owned(),
                size: 7,
            },
        },
    )?;
    agent_protocol::AgentCommand::new(
        stream_read(persistence)?.revision,
        agent_protocol::AgentCommandAction::Stream(agent_protocol::AgentStreamCommand::Finalize {
            session_id: SESSION_ID.to_owned(),
            stream_id: STREAM_ID.to_owned(),
        }),
    )
    .map_err(Into::into)
}

fn prepare_external_not_applied<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<cymule_profile_protocol::resource::ResourcePin> {
    let initial = stream_read(persistence)?;
    assert!(initial.current.is_none());
    AgentStreamController::open(
        persistence,
        &initial.revision,
        SESSION_ID,
        STREAM_ID,
        agent_protocol::AgentStreamTarget::Message {
            message_id: "message:agent-abort-process-kill".to_owned(),
            role: agent_protocol::MessageRole::Agent,
        },
        agent_protocol::AgentStreamDelivery::ExternalResource {
            resolver_binding: EXTERNAL_RESOLVER.to_owned(),
            content: agent_protocol::AgentStreamPublicationContent {
                media_type: "application/octet-stream".to_owned(),
                digest: EXTERNAL_DIGEST.to_owned(),
                size: 7,
            },
        },
    )?;
    let opened = stream_read(persistence)?;
    let outcome =
        AgentStreamController::finalize(persistence, &opened.revision, SESSION_ID, STREAM_ID)?;
    assert!(matches!(
        outcome,
        agent_protocol::AgentStreamFinalizeOutcome::PublicationNotApplied { .. }
    ));
    let current = stream_read(persistence)?
        .current
        .ok_or_else(|| AgentError::NotFound("NotApplied external stream is missing".to_owned()))?;
    let reservation = current
        .publication_reservation
        .ok_or_else(|| AgentError::Integrity {
            code: "agent_abort_process_kill_reservation_missing".to_owned(),
            message: "NotApplied external stream lost its reservation".to_owned(),
        })?;
    assert_eq!(
        reservation.phase,
        agent_protocol::AgentStreamPublicationReservationPhase::NotApplied
    );
    Ok(reservation.resource_pin_receipt.pin.clone())
}

fn external_abort_command<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentCommand> {
    agent_protocol::AgentCommand::new(
        stream_read(persistence)?.revision,
        agent_protocol::AgentCommandAction::Stream(agent_protocol::AgentStreamCommand::Abort {
            session_id: SESSION_ID.to_owned(),
            stream_id: STREAM_ID.to_owned(),
            reason: "caller:abort-not-applied-process-kill".to_owned(),
        }),
    )
    .map_err(Into::into)
}

fn tool_projection(status: agent_protocol::ToolCallStatus) -> agent_protocol::ToolCall {
    let output = matches!(status, agent_protocol::ToolCallStatus::Completed).then(|| {
        vec![agent_protocol::ContentBlock::Text {
            text: "tool completed".to_owned(),
        }]
    });
    agent_protocol::ToolCall {
        tool_call_id: TOOL_ID.to_owned(),
        operation: "test.project".to_owned(),
        status,
        input: json!({"path": "README.md"}),
        output,
        locations: vec!["workspace:agent-process-kill".to_owned()],
    }
}

fn tool_update_id(status: agent_protocol::ToolCallStatus) -> &'static str {
    match status {
        agent_protocol::ToolCallStatus::Pending => "update:tool:pending:1",
        agent_protocol::ToolCallStatus::InProgress => "update:tool:in-progress:2",
        agent_protocol::ToolCallStatus::Completed => "update:tool:completed:3",
        agent_protocol::ToolCallStatus::AwaitingPermission
        | agent_protocol::ToolCallStatus::Failed
        | agent_protocol::ToolCallStatus::Cancelled => {
            unreachable!("Agent process-death Tool uses one fixed lifecycle")
        }
    }
}

fn commit_tool<P: AgentPersistence>(
    persistence: &mut P,
    status: agent_protocol::ToolCallStatus,
) -> AgentResult<agent_protocol::AgentCommit> {
    let source_revision = session_read(persistence)?.revision;
    let command = agent_protocol::AgentCommand::new(
        source_revision,
        agent_protocol::AgentCommandAction::SessionUpdate {
            session_id: SESSION_ID.to_owned(),
            update: agent_protocol::AgentUpdate::Tool {
                update_id: tool_update_id(status).to_owned(),
                tool: tool_projection(status),
            },
        },
    )?;
    let commit = persistence.commit_agent(&command)?;
    commit.verify_for(&command)?;
    Ok(commit)
}

fn ensure_tool_exists<P: AgentPersistence>(persistence: &mut P) -> AgentResult<()> {
    if tool_read(persistence)?.current.is_none() {
        commit_tool(persistence, agent_protocol::ToolCallStatus::Pending)?;
    }
    Ok(())
}

fn complete_tool<P: AgentPersistence>(persistence: &mut P) -> AgentResult<()> {
    loop {
        let current = tool_read(persistence)?
            .current
            .ok_or_else(|| AgentError::NotFound("Agent Tool current is missing".to_owned()))?;
        match current.tool.status {
            agent_protocol::ToolCallStatus::Pending => {
                commit_tool(persistence, agent_protocol::ToolCallStatus::InProgress)?;
            }
            agent_protocol::ToolCallStatus::InProgress => {
                commit_tool(persistence, agent_protocol::ToolCallStatus::Completed)?;
            }
            agent_protocol::ToolCallStatus::Completed => return Ok(()),
            status => {
                return Err(AgentError::IllegalTransition(format!(
                    "Agent process-death Tool reached unexpected state {status:?}"
                )));
            }
        }
    }
}

fn close_session_read<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentSessionRead> {
    let query = agent_protocol::AgentSessionQuery {
        session_id: CLOSE_SESSION_ID.to_owned(),
        expected_revision: None,
    };
    let read = persistence.read_agent_session(&query)?;
    read.verify_for(&query)?;
    Ok(read)
}

fn close_tool_read<P: AgentPersistence>(
    persistence: &mut P,
    tool_call_id: &str,
) -> AgentResult<agent_protocol::AgentToolRead> {
    let query = agent_protocol::AgentToolQuery {
        session_id: CLOSE_SESSION_ID.to_owned(),
        tool_call_id: tool_call_id.to_owned(),
        expected_revision: None,
    };
    let read = persistence.read_agent_tool(&query)?;
    read.verify_for(&query)?;
    Ok(read)
}

fn close_tool_projection(
    tool_call_id: &str,
    status: agent_protocol::ToolCallStatus,
) -> agent_protocol::ToolCall {
    agent_protocol::ToolCall {
        tool_call_id: tool_call_id.to_owned(),
        operation: "test.agent.close".to_owned(),
        status,
        input: json!({"tool": tool_call_id}),
        output: None,
        locations: vec!["workspace:agent-close-process-kill".to_owned()],
    }
}

fn commit_close_tool<P: AgentPersistence>(
    persistence: &mut P,
    update_id: &str,
    tool_call_id: &str,
    status: agent_protocol::ToolCallStatus,
) -> AgentResult<agent_protocol::AgentCommit> {
    let command = agent_protocol::AgentCommand::new(
        close_session_read(persistence)?.revision,
        agent_protocol::AgentCommandAction::SessionUpdate {
            session_id: CLOSE_SESSION_ID.to_owned(),
            update: agent_protocol::AgentUpdate::Tool {
                update_id: update_id.to_owned(),
                tool: close_tool_projection(tool_call_id, status),
            },
        },
    )?;
    let commit = persistence.commit_agent(&command)?;
    commit.verify_for(&command)?;
    Ok(commit)
}

fn prepare_close_baseline<P: AgentPersistence>(persistence: &mut P) -> AgentResult<()> {
    commit_close_tool(
        persistence,
        "update:agent-close:pending:1",
        CLOSE_PENDING_TOOL_ID,
        agent_protocol::ToolCallStatus::Pending,
    )?;
    commit_close_tool(
        persistence,
        "update:agent-close:second-pending:2",
        CLOSE_IN_PROGRESS_TOOL_ID,
        agent_protocol::ToolCallStatus::Pending,
    )?;
    commit_close_tool(
        persistence,
        "update:agent-close:in-progress:3",
        CLOSE_IN_PROGRESS_TOOL_ID,
        agent_protocol::ToolCallStatus::InProgress,
    )?;

    let session = close_session_read(persistence)?
        .current
        .ok_or_else(|| AgentError::NotFound("Agent Close Session is missing".to_owned()))?;
    assert_eq!(session.state, agent_protocol::AgentState::Idle);
    assert_eq!(
        session
            .nonterminal_tools
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [CLOSE_IN_PROGRESS_TOOL_ID, CLOSE_PENDING_TOOL_ID]
    );
    for (tool_call_id, expected_status) in [
        (
            CLOSE_IN_PROGRESS_TOOL_ID,
            agent_protocol::ToolCallStatus::InProgress,
        ),
        (
            CLOSE_PENDING_TOOL_ID,
            agent_protocol::ToolCallStatus::Pending,
        ),
    ] {
        let current = close_tool_read(persistence, tool_call_id)?
            .current
            .ok_or_else(|| AgentError::NotFound(format!("Agent Tool {tool_call_id} is missing")))?;
        assert_eq!(current.tool.status, expected_status);
        session.nonterminal_tools[tool_call_id].verify_for(&current)?;
    }
    Ok(())
}

fn close_command<P: AgentPersistence>(
    persistence: &mut P,
) -> AgentResult<agent_protocol::AgentCommand> {
    agent_protocol::AgentCommand::new(
        close_session_read(persistence)?.revision,
        agent_protocol::AgentCommandAction::SessionUpdate {
            session_id: CLOSE_SESSION_ID.to_owned(),
            update: agent_protocol::AgentUpdate::State {
                update_id: "update:agent-close:closed:4".to_owned(),
                state: agent_protocol::AgentState::Closed,
                stop_reason: None,
            },
        },
    )
    .map_err(Into::into)
}

fn converge_occurrence<P: AgentPersistence>(
    mut persistence: P,
    host: LedgerHost,
) -> AgentResult<(P, LedgerHost)> {
    let current = occurrence_read(&mut persistence)?.current;
    converge_occurrence_from_current(persistence, host, current)
}

fn converge_occurrence_from_current<P: AgentPersistence>(
    persistence: P,
    host: LedgerHost,
    current: Option<agent_protocol::AgentOccurrenceCurrent>,
) -> AgentResult<(P, LedgerHost)> {
    match current.map(|current| current.occurrence) {
        Some(occurrence)
            if occurrence.state == agent_protocol::AgentHostOccurrenceState::NotApplied =>
        {
            Ok((persistence, host))
        }
        Some(occurrence)
            if matches!(
                occurrence.state,
                agent_protocol::AgentHostOccurrenceState::Prepared
                    | agent_protocol::AgentHostOccurrenceState::Started
                    | agent_protocol::AgentHostOccurrenceState::Unknown
            ) =>
        {
            Err(AgentError::RecoveryRequired(format!(
                "Agent occurrence {} must be reconciled before convergence",
                occurrence.occurrence_id
            )))
        }
        None | Some(_) => {
            let mut controller = AgentInteractionController::resume(SESSION_ID, host, persistence)?;
            let response = controller.execute(
                OCCURRENCE_ID,
                agent_protocol::AgentHostRequest::Tool(host_request()),
            )?;
            assert_eq!(
                response,
                agent_protocol::AgentHostResponse::Tool(LedgerHost::response())
            );
            let (host, persistence) = controller.into_parts();
            Ok((persistence, host))
        }
    }
}

fn converge_stream<P: AgentPersistence>(persistence: &mut P) -> AgentResult<()> {
    loop {
        let read = stream_read(persistence)?;
        match read.current {
            None => {
                AgentStreamController::open(
                    persistence,
                    &read.revision,
                    SESSION_ID,
                    STREAM_ID,
                    agent_protocol::AgentStreamTarget::Message {
                        message_id: "message:agent-process-kill".to_owned(),
                        role: agent_protocol::MessageRole::Agent,
                    },
                    agent_protocol::AgentStreamDelivery::Staged,
                )?;
            }
            Some(current)
                if current.state == agent_protocol::AgentStreamState::Open
                    && current.next_chunk_sequence == 0 =>
            {
                AgentStreamController::append(
                    persistence,
                    &read.revision,
                    SESSION_ID,
                    STREAM_ID,
                    agent_protocol::AgentStreamChunk {
                        sequence: 0,
                        content: vec![agent_protocol::ContentBlock::Text {
                            text: "stream output".to_owned(),
                        }],
                    },
                )?;
            }
            Some(current)
                if current.state == agent_protocol::AgentStreamState::Open
                    && current.next_chunk_sequence == 1 =>
            {
                let outcome = AgentStreamController::finalize(
                    persistence,
                    &read.revision,
                    SESSION_ID,
                    STREAM_ID,
                )?;
                if !matches!(
                    outcome,
                    agent_protocol::AgentStreamFinalizeOutcome::Committed { .. }
                ) {
                    return Err(AgentError::RuntimeDefect {
                        code: "agent_process_kill_staged_finalize_uncommitted".to_owned(),
                        message: "staged Agent stream finalization did not commit".to_owned(),
                    });
                }
            }
            Some(current) if current.state == agent_protocol::AgentStreamState::Finalized => {
                return Ok(());
            }
            Some(current) => {
                return Err(AgentError::IllegalTransition(format!(
                    "Agent stream reached unexpected state {:?} at sequence {}",
                    current.state, current.next_chunk_sequence
                )));
            }
        }
    }
}

fn converge_scenario<P: AgentPersistence>(
    mut persistence: P,
    host: LedgerHost,
) -> AgentResult<(P, LedgerHost)> {
    ensure_tool_exists(&mut persistence)?;
    let (mut persistence, host) = converge_occurrence(persistence, host)?;
    complete_tool(&mut persistence)?;
    converge_stream(&mut persistence)?;
    Ok((persistence, host))
}

fn execute_exact_command<P: AgentPersistence>(
    persistence: &mut P,
    command: &agent_protocol::AgentCommand,
) -> AgentResult<agent_protocol::AgentCommit> {
    if matches!(
        command.action,
        agent_protocol::AgentCommandAction::Stream(
            agent_protocol::AgentStreamCommand::Finalize { .. }
        )
    ) {
        let outcome = persistence.finalize_agent_stream(command)?;
        let agent_protocol::AgentStreamFinalizeOutcome::Committed { commit } = outcome else {
            return Err(AgentError::RuntimeDefect {
                code: "agent_process_kill_exact_finalize_uncommitted".to_owned(),
                message: "exact staged Finalize replay did not return its Agent commit".to_owned(),
            });
        };
        Ok(*commit)
    } else {
        persistence.commit_agent(command)
    }
}

fn replay_exact_command<P: AgentPersistence>(
    persistence: &mut P,
    command_path: &Path,
) -> AgentResult<()> {
    let bytes = fs::read(command_path).expect("recorded Agent command reads");
    let command: agent_protocol::AgentCommand =
        cymule_core::decode_json(&bytes).map_err(|error| AgentError::Encoding {
            message: error.to_string(),
        })?;
    command.verify()?;
    let first = execute_exact_command(persistence, &command)?;
    first.verify_for(&command)?;
    let replay = execute_exact_command(persistence, &command)?;
    replay.verify_for(&command)?;
    assert_eq!(replay.receipt, first.receipt);
    assert!(
        replay.committed_revision.is_none(),
        "second exact Agent command replay must not publish another CAS"
    );
    Ok(())
}

fn recover_occurrence<P: AgentPersistence>(
    persistence: &mut P,
    host: &mut LedgerHost,
) -> AgentResult<()> {
    let Some(current) = occurrence_read(persistence)?.current else {
        return Ok(());
    };
    match current.occurrence.state {
        agent_protocol::AgentHostOccurrenceState::Prepared => {
            AgentRecoveryController::cancel_prepared(
                persistence,
                SESSION_ID,
                OCCURRENCE_ID,
                vec![agent_protocol::ContentBlock::Text {
                    text: "prepared occurrence never reached dispatch".to_owned(),
                }],
            )?;
        }
        agent_protocol::AgentHostOccurrenceState::Started
        | agent_protocol::AgentHostOccurrenceState::Unknown => {
            AgentRecoveryController::reconcile(host, persistence, SESSION_ID, OCCURRENCE_ID)?;
        }
        agent_protocol::AgentHostOccurrenceState::Completed
        | agent_protocol::AgentHostOccurrenceState::NotApplied => {}
    }
    Ok(())
}

fn assert_terminal_scenario<P: AgentPersistence>(
    persistence: &mut P,
    host: &LedgerHost,
) -> AgentResult<()> {
    let session = session_read(persistence)?
        .current
        .ok_or_else(|| AgentError::NotFound("terminal Agent Session is missing".to_owned()))?;
    assert_eq!(session.state, agent_protocol::AgentState::Idle);
    assert!(session.nonterminal_tools.is_empty());
    assert_eq!(session.unresolved_occurrence_count, 0);
    assert_eq!(session.open_stream_count, 0);

    let tool = tool_read(persistence)?
        .current
        .ok_or_else(|| AgentError::NotFound("terminal Agent Tool is missing".to_owned()))?;
    assert_eq!(tool.tool.status, agent_protocol::ToolCallStatus::Completed);
    let occurrence = occurrence_read(persistence)?
        .current
        .ok_or_else(|| AgentError::NotFound("terminal Agent occurrence is missing".to_owned()))?;
    assert!(matches!(
        occurrence.occurrence.state,
        agent_protocol::AgentHostOccurrenceState::Completed
            | agent_protocol::AgentHostOccurrenceState::NotApplied
    ));
    let stream = stream_read(persistence)?
        .current
        .ok_or_else(|| AgentError::NotFound("terminal Agent stream is missing".to_owned()))?;
    assert_eq!(stream.state, agent_protocol::AgentStreamState::Finalized);
    assert_eq!(stream.next_chunk_sequence, 1);

    let (attempts, reconciliations, results) = host.counts();
    assert!(attempts <= 1, "host provider must not be dispatched twice");
    assert!(
        reconciliations <= 1,
        "one occurrence needs at most one recovery observation"
    );
    assert_eq!(results, attempts);
    if occurrence.occurrence.state == agent_protocol::AgentHostOccurrenceState::Completed {
        assert_eq!(attempts, 1);
    } else {
        assert_eq!(attempts, 0);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
struct CloseTerminalEvidence {
    command: agent_protocol::AgentCommand,
    receipt: agent_protocol::AgentCommandReceipt,
    session: agent_protocol::AgentSessionCurrent,
    tools: BTreeMap<String, agent_protocol::AgentToolCurrent>,
}

fn verify_close_receipt_source(receipt: &agent_protocol::AgentCommandReceipt) -> AgentResult<()> {
    let agent_protocol::AgentCommandSource::Session {
        session: source_session,
        update: source_update,
    } = &receipt.source
    else {
        return Err(AgentError::RuntimeDefect {
            code: "agent_close_process_kill_source_shape".to_owned(),
            message: "Agent Close receipt did not retain its Session source".to_owned(),
        });
    };
    assert_eq!(source_session.session_id, CLOSE_SESSION_ID);
    assert_eq!(source_session.state, agent_protocol::AgentState::Idle);
    assert_eq!(source_session.nonterminal_tools.len(), 2);
    assert!(source_update.update.is_none());
    let agent_protocol::AgentSessionEntrySource::Close {
        tools: source_tools,
    } = &source_update.entry
    else {
        return Err(AgentError::RuntimeDefect {
            code: "agent_close_process_kill_entry_shape".to_owned(),
            message: "Agent Close receipt did not retain its complete Tool source".to_owned(),
        });
    };
    let source_statuses = source_tools
        .iter()
        .map(|current| {
            source_session.nonterminal_tools[&current.tool.tool_call_id].verify_for(current)?;
            Ok((current.tool.tool_call_id.as_str(), current.tool.status))
        })
        .collect::<AgentResult<Vec<_>>>()?;
    assert_eq!(
        source_statuses,
        [
            (
                CLOSE_IN_PROGRESS_TOOL_ID,
                agent_protocol::ToolCallStatus::InProgress,
            ),
            (
                CLOSE_PENDING_TOOL_ID,
                agent_protocol::ToolCallStatus::Pending,
            ),
        ]
    );
    Ok(())
}

fn close_receipt_outcome(
    command: &agent_protocol::AgentCommand,
    receipt: &agent_protocol::AgentCommandReceipt,
) -> AgentResult<(
    agent_protocol::AgentSessionCurrent,
    BTreeMap<String, agent_protocol::AgentToolCurrent>,
)> {
    let agent_protocol::AgentCommandOutcome::Session(postcondition) = &receipt.outcome else {
        return Err(AgentError::RuntimeDefect {
            code: "agent_close_process_kill_outcome_shape".to_owned(),
            message: "Agent Close receipt did not return a Session postcondition".to_owned(),
        });
    };
    let agent_protocol::AgentSessionUpdateEffect::Closed {
        tools: cancelled_tools,
    } = &postcondition.effect
    else {
        return Err(AgentError::RuntimeDefect {
            code: "agent_close_process_kill_effect_shape".to_owned(),
            message: "Agent Close receipt did not return its Tool cancellations".to_owned(),
        });
    };
    assert_eq!(
        postcondition.session.state,
        agent_protocol::AgentState::Closed
    );
    assert!(postcondition.session.nonterminal_tools.is_empty());
    assert_eq!(cancelled_tools.len(), 2);

    let mut tools = BTreeMap::new();
    for cancelled in cancelled_tools {
        assert_eq!(cancelled.session_id, CLOSE_SESSION_ID);
        assert_eq!(
            cancelled.tool.status,
            agent_protocol::ToolCallStatus::Cancelled
        );
        assert_eq!(cancelled.admitted_by, command.command_id);
        let tool_call_id = cancelled.tool.tool_call_id.clone();
        assert!(
            tools
                .insert(tool_call_id.clone(), cancelled.clone())
                .is_none(),
            "Agent Close receipt must not duplicate Tool {tool_call_id}"
        );
    }
    assert_eq!(
        tools.keys().map(String::as_str).collect::<Vec<_>>(),
        [CLOSE_IN_PROGRESS_TOOL_ID, CLOSE_PENDING_TOOL_ID]
    );
    Ok((postcondition.session.clone(), tools))
}

fn close_terminal_evidence<P: AgentPersistence>(
    persistence: &mut P,
    command: &agent_protocol::AgentCommand,
    receipt: &agent_protocol::AgentCommandReceipt,
) -> AgentResult<CloseTerminalEvidence> {
    receipt.verify_for(command)?;
    verify_close_receipt_source(receipt)?;
    let (expected_session, tools) = close_receipt_outcome(command, receipt)?;

    let session = close_session_read(persistence)?
        .current
        .ok_or_else(|| AgentError::NotFound("closed Agent Session is missing".to_owned()))?;
    assert_eq!(session, expected_session);
    assert_eq!(session.state, agent_protocol::AgentState::Closed);
    assert!(session.nonterminal_tools.is_empty());
    for (tool_call_id, expected) in &tools {
        let current = close_tool_read(persistence, tool_call_id)?
            .current
            .ok_or_else(|| {
                AgentError::NotFound(format!("cancelled Tool {tool_call_id} is missing"))
            })?;
        assert_eq!(&current, expected);
        assert!(matches!(
            current.tool.status,
            agent_protocol::ToolCallStatus::Completed
                | agent_protocol::ToolCallStatus::Failed
                | agent_protocol::ToolCallStatus::Cancelled
        ));
    }

    Ok(CloseTerminalEvidence {
        command: command.clone(),
        receipt: receipt.clone(),
        session,
        tools,
    })
}

fn read_recorded_command(path: &Path) -> AgentResult<agent_protocol::AgentCommand> {
    let bytes = fs::read(path).expect("recorded Agent Close command reads");
    let command: agent_protocol::AgentCommand =
        cymule_core::decode_json(&bytes).map_err(|error| AgentError::Encoding {
            message: error.to_string(),
        })?;
    command.verify()?;
    Ok(command)
}

#[test]
fn agent_process_kill_worker_entry() {
    let Ok(database) = std::env::var("CYMULE_AGENT_KILL_DB") else {
        return;
    };
    let ledger = PathBuf::from(
        std::env::var("CYMULE_AGENT_KILL_LEDGER").expect("Agent provider ledger exists"),
    );
    let marker =
        PathBuf::from(std::env::var("CYMULE_AGENT_KILL_MARKER").expect("Agent kill marker exists"));
    let command_path = PathBuf::from(
        std::env::var("CYMULE_AGENT_KILL_COMMAND").expect("Agent command path exists"),
    );
    let phase = match std::env::var("CYMULE_AGENT_KILL_PHASE")
        .expect("Agent kill phase exists")
        .as_str()
    {
        "objects_staged_before_head_cas" => KillPhase::ObjectsStaged,
        "cas_acknowledgement_lost" => KillPhase::AcknowledgementLost,
        phase => panic!("unknown Agent kill phase {phase}"),
    };
    let fail_at = std::env::var("CYMULE_AGENT_KILL_AT")
        .expect("Agent kill boundary exists")
        .parse()
        .expect("Agent kill boundary parses");
    let store = FaultingSqliteStore::killing(Path::new(&database), phase, fail_at, marker);
    let mut runtime = open_runtime_with_store(store);
    let mut providers = UnusedAgentProviders;
    let persistence = RecordingPersistence::new(runtime.agent(&mut providers), Some(command_path));
    converge_scenario(persistence, LedgerHost { database: ledger })
        .expect("Agent worker reaches its selected CAS boundary");
    panic!("Agent kill worker unexpectedly completed");
}

#[test]
fn agent_close_process_kill_worker_entry() {
    let Ok(database) = std::env::var("CYMULE_AGENT_CLOSE_KILL_DB") else {
        return;
    };
    let marker = PathBuf::from(
        std::env::var("CYMULE_AGENT_CLOSE_KILL_MARKER").expect("Agent Close kill marker exists"),
    );
    let command_path = PathBuf::from(
        std::env::var("CYMULE_AGENT_CLOSE_KILL_COMMAND").expect("Agent Close command path exists"),
    );
    let phase = match std::env::var("CYMULE_AGENT_CLOSE_KILL_PHASE")
        .expect("Agent Close kill phase exists")
        .as_str()
    {
        "objects_staged_before_head_cas" => KillPhase::ObjectsStaged,
        "cas_acknowledgement_lost" => KillPhase::AcknowledgementLost,
        phase => panic!("unknown Agent Close kill phase {phase}"),
    };
    let store = FaultingSqliteStore::killing(Path::new(&database), phase, 1, marker);
    let mut runtime = open_runtime_with_store(store);
    let mut providers = UnusedAgentProviders;
    let mut persistence =
        RecordingPersistence::new(runtime.agent(&mut providers), Some(command_path));
    let command = close_command(&mut persistence).expect("Agent Close command seals");
    persistence
        .commit_agent(&command)
        .expect("Agent Close worker reaches its selected CAS boundary");
    panic!("Agent Close kill worker unexpectedly completed");
}

#[test]
fn agent_external_abort_process_kill_worker_entry() {
    let Ok(database) = std::env::var("CYMULE_AGENT_ABORT_KILL_DB") else {
        return;
    };
    let marker = PathBuf::from(
        std::env::var("CYMULE_AGENT_ABORT_KILL_MARKER")
            .expect("Agent external Abort kill marker exists"),
    );
    let command_path = PathBuf::from(
        std::env::var("CYMULE_AGENT_ABORT_KILL_COMMAND")
            .expect("Agent external Abort command path exists"),
    );
    let phase = match std::env::var("CYMULE_AGENT_ABORT_KILL_PHASE")
        .expect("Agent external Abort kill phase exists")
        .as_str()
    {
        "objects_staged_before_head_cas" => KillPhase::ObjectsStaged,
        "cas_acknowledgement_lost" => KillPhase::AcknowledgementLost,
        phase => panic!("unknown Agent external Abort kill phase {phase}"),
    };
    let store = FaultingSqliteStore::killing(Path::new(&database), phase, 1, marker);
    let mut runtime = open_runtime_with_store(store);
    let mut providers = UnusedAgentProviders;
    let mut persistence =
        RecordingPersistence::new(runtime.agent(&mut providers), Some(command_path));
    let read = stream_read(&mut persistence).expect("NotApplied stream current reads");
    AgentStreamController::abort(
        &mut persistence,
        &read.revision,
        SESSION_ID,
        STREAM_ID,
        "caller:abort-not-applied-process-kill",
    )
    .expect("Agent external Abort worker reaches its selected CAS boundary");
    panic!("Agent external Abort kill worker unexpectedly completed");
}

struct AgentFaultCase {
    _world: TestWorld,
    database: PathBuf,
    ledger: PathBuf,
    marker: PathBuf,
    command_path: PathBuf,
}

impl AgentFaultCase {
    fn new(seed: u64) -> Self {
        let world = TestWorld::new(seed).expect("Agent fault world creates");
        let database = world
            .domain()
            .path("agent.sqlite")
            .expect("Agent Store path resolves");
        let ledger = world
            .domain()
            .path("provider.sqlite")
            .expect("Agent provider ledger path resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("Agent kill marker path resolves");
        let command_path = world
            .domain()
            .path("last-command.json")
            .expect("Agent command path resolves");
        initialize_store(&database);
        LedgerHost::initialize(&ledger);
        Self {
            _world: world,
            database,
            ledger,
            marker,
            command_path,
        }
    }

    fn kill_at(&self, phase: KillPhase, fail_at: usize) {
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("agent_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_AGENT_KILL_DB", &self.database)
            .env("CYMULE_AGENT_KILL_LEDGER", &self.ledger)
            .env("CYMULE_AGENT_KILL_MARKER", &self.marker)
            .env("CYMULE_AGENT_KILL_COMMAND", &self.command_path)
            .env("CYMULE_AGENT_KILL_PHASE", phase.label())
            .env("CYMULE_AGENT_KILL_AT", fail_at.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("Agent kill worker starts");
        child
            .wait_for_content(
                &self.marker,
                format!("{fail_at}:{}", phase.label()).as_bytes(),
                Duration::from_secs(20),
            )
            .expect("Agent worker reaches the selected CAS barrier");
        assert_eq!(
            child.terminate().expect("Agent worker is reaped").signal(),
            Some(9)
        );
        assert!(child.is_reaped());
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.ledger);
    }

    fn recover(&self) {
        {
            let mut runtime = open_runtime(&self.database);
            let mut providers = UnusedAgentProviders;
            let mut persistence = runtime.agent(&mut providers);
            replay_exact_command(&mut persistence, &self.command_path)
                .expect("exact Agent command receipt reconciles");
            let mut host = LedgerHost {
                database: self.ledger.clone(),
            };
            recover_occurrence(&mut persistence, &mut host)
                .expect("Agent occurrence reconciles from its exact current");
            let (mut persistence, host) = converge_scenario(persistence, host)
                .expect("Agent Session, Tool, occurrence, and stream converge");
            assert_terminal_scenario(&mut persistence, &host)
                .expect("terminal Agent state reads exactly");
        }
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.ledger);
    }
}

struct AgentCloseFaultCase {
    _world: TestWorld,
    database: PathBuf,
    marker: PathBuf,
    command_path: PathBuf,
}

impl AgentCloseFaultCase {
    fn new(seed: u64) -> Self {
        let world = TestWorld::new(seed).expect("Agent Close fault world creates");
        let database = world
            .domain()
            .path("agent-close.sqlite")
            .expect("Agent Close Store path resolves");
        let marker = world
            .domain()
            .path("close-kill-ready")
            .expect("Agent Close kill marker path resolves");
        let command_path = world
            .domain()
            .path("close-command.json")
            .expect("Agent Close command path resolves");
        initialize_store(&database);
        {
            let mut runtime = open_runtime(&database);
            let mut providers = UnusedAgentProviders;
            prepare_close_baseline(&mut runtime.agent(&mut providers))
                .expect("Agent Close baseline retains both non-terminal Tools");
        }
        assert_sqlite_integrity(&database);
        Self {
            _world: world,
            database,
            marker,
            command_path,
        }
    }

    fn kill_close(&self, phase: KillPhase) {
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("agent_close_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_AGENT_CLOSE_KILL_DB", &self.database)
            .env("CYMULE_AGENT_CLOSE_KILL_MARKER", &self.marker)
            .env("CYMULE_AGENT_CLOSE_KILL_COMMAND", &self.command_path)
            .env("CYMULE_AGENT_CLOSE_KILL_PHASE", phase.label())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("Agent Close kill worker starts");
        child
            .wait_for_content(
                &self.marker,
                format!("1:{}", phase.label()).as_bytes(),
                Duration::from_secs(20),
            )
            .expect("Agent Close worker reaches its CAS barrier");
        assert_eq!(
            child
                .terminate()
                .expect("Agent Close worker is reaped")
                .signal(),
            Some(9)
        );
        assert!(child.is_reaped());
        assert_sqlite_integrity(&self.database);
    }

    fn converge_close(&self, phase: KillPhase) -> CloseTerminalEvidence {
        let evidence = {
            let mut runtime = open_runtime(&self.database);
            let mut providers = UnusedAgentProviders;
            let mut persistence = runtime.agent(&mut providers);
            let command = read_recorded_command(&self.command_path)
                .expect("recorded Agent Close command verifies");
            let revision_before = close_session_read(&mut persistence)
                .expect("Agent Close source revision reads")
                .revision;
            let first = persistence
                .commit_agent(&command)
                .expect("same Agent Close command converges after process death");
            first
                .verify_for(&command)
                .expect("converged Agent Close commit verifies");
            match phase {
                KillPhase::ObjectsStaged => assert_eq!(
                    first.committed_revision.as_ref(),
                    Some(&first.observed_revision),
                    "pre-CAS death must leave the same Close command to commit"
                ),
                KillPhase::AcknowledgementLost => assert!(
                    first.committed_revision.is_none(),
                    "post-CAS death must replay the already retained Close receipt"
                ),
            }
            let evidence = close_terminal_evidence(&mut persistence, &command, &first.receipt)
                .expect("converged Agent Close terminal state reads exactly");
            let revision_after_first = close_session_read(&mut persistence)
                .expect("converged Agent Close revision reads")
                .revision;
            match phase {
                KillPhase::ObjectsStaged => assert_ne!(
                    revision_before, revision_after_first,
                    "pre-CAS death must leave the open and closed StateRoot heads distinct"
                ),
                KillPhase::AcknowledgementLost => assert_eq!(
                    revision_before, revision_after_first,
                    "post-CAS death recovery must observe the already closed StateRoot head"
                ),
            }

            let replay = persistence
                .commit_agent(&command)
                .expect("exact Agent Close command replays");
            replay
                .verify_for(&command)
                .expect("exact Agent Close replay verifies");
            assert!(replay.committed_revision.is_none());
            assert_eq!(replay.observed_revision, revision_after_first);
            assert_eq!(replay.receipt, first.receipt);
            assert_eq!(
                close_session_read(&mut persistence)
                    .expect("post-replay Agent Close revision reads")
                    .revision,
                revision_after_first,
                "exact Close replay must not write another StateRoot"
            );
            assert_eq!(
                close_terminal_evidence(&mut persistence, &command, &replay.receipt)
                    .expect("post-replay Agent Close state reads exactly"),
                evidence,
                "exact Close replay must not duplicate or omit Session/Tool state"
            );
            evidence
        };
        assert_sqlite_integrity(&self.database);
        evidence
    }

    fn close_without_death(&self) -> CloseTerminalEvidence {
        let evidence = {
            let mut runtime = open_runtime(&self.database);
            let mut providers = UnusedAgentProviders;
            let mut persistence = runtime.agent(&mut providers);
            let command =
                close_command(&mut persistence).expect("baseline Agent Close command seals");
            let commit = persistence
                .commit_agent(&command)
                .expect("baseline Agent Close commits");
            commit
                .verify_for(&command)
                .expect("baseline Agent Close commit verifies");
            assert_eq!(
                commit.committed_revision.as_ref(),
                Some(&commit.observed_revision)
            );
            close_terminal_evidence(&mut persistence, &command, &commit.receipt)
                .expect("baseline Agent Close terminal state reads exactly")
        };
        assert_sqlite_integrity(&self.database);
        evidence
    }
}

struct AgentExternalAbortFaultCase {
    _world: TestWorld,
    database: PathBuf,
    marker: PathBuf,
    command_path: PathBuf,
    pin: cymule_profile_protocol::resource::ResourcePin,
}

impl AgentExternalAbortFaultCase {
    fn new(seed: u64) -> Self {
        let world = TestWorld::new(seed).expect("Agent external Abort fault world creates");
        let database = world
            .domain()
            .path("agent-external-abort.sqlite")
            .expect("Agent external Abort Store path resolves");
        let marker = world
            .domain()
            .path("external-abort-kill-ready")
            .expect("Agent external Abort marker path resolves");
        let command_path = world
            .domain()
            .path("external-abort-command.json")
            .expect("Agent external Abort command path resolves");
        initialize_store(&database);
        let pin = {
            let mut runtime = open_runtime(&database);
            let mut provider = NotAppliedPublicationProvider::default();
            let pin = {
                let mut persistence = runtime.agent(&mut provider);
                let pin = prepare_external_not_applied(&mut persistence)
                    .expect("external publication persists NotApplied before Abort");
                let session = session_read(&mut persistence)
                    .expect("NotApplied Session reads")
                    .current
                    .expect("NotApplied Session exists");
                assert_eq!(session.open_stream_count, 1);
                pin
            };
            assert_eq!(provider.publish_calls, 1);
            let current = runtime
                .resource()
                .pin_current(&pin.pin_id)
                .expect("reserved pin reads")
                .expect("reserved pin exists");
            assert_eq!(
                current.status,
                cymule_profile_protocol::resource::ResourcePinStatus::Reserved
            );
            pin
        };
        assert_sqlite_integrity(&database);
        Self {
            _world: world,
            database,
            marker,
            command_path,
            pin,
        }
    }

    fn kill_abort(&self, phase: KillPhase) {
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("agent_external_abort_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_AGENT_ABORT_KILL_DB", &self.database)
            .env("CYMULE_AGENT_ABORT_KILL_MARKER", &self.marker)
            .env("CYMULE_AGENT_ABORT_KILL_COMMAND", &self.command_path)
            .env("CYMULE_AGENT_ABORT_KILL_PHASE", phase.label())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child =
            ManagedChild::spawn(&mut command).expect("Agent external Abort kill worker starts");
        child
            .wait_for_content(
                &self.marker,
                format!("1:{}", phase.label()).as_bytes(),
                Duration::from_secs(20),
            )
            .expect("Agent external Abort reaches its real Store CAS barrier");
        assert_eq!(
            child
                .terminate()
                .expect("Agent external Abort worker is reaped")
                .signal(),
            Some(9)
        );
        assert!(child.is_reaped());
        assert_sqlite_integrity(&self.database);
    }

    fn converge_abort(&self, phase: KillPhase) {
        let mut runtime = open_runtime(&self.database);
        let mut providers = UnusedAgentProviders;
        let release = {
            let mut persistence = runtime.agent(&mut providers);
            let command = read_recorded_command(&self.command_path)
                .expect("recorded external Abort command verifies");
            let revision_before = stream_read(&mut persistence)
                .expect("pre-recovery stream reads")
                .revision;
            let first = persistence
                .commit_agent(&command)
                .expect("same external Abort command converges after process death");
            first
                .verify_for(&command)
                .expect("converged external Abort receipt verifies");
            match phase {
                KillPhase::ObjectsStaged => assert_eq!(
                    first.committed_revision.as_ref(),
                    Some(&first.observed_revision),
                    "pre-head death leaves Abort to commit"
                ),
                KillPhase::AcknowledgementLost => assert!(
                    first.committed_revision.is_none(),
                    "post-commit death replays the retained Abort receipt"
                ),
            }
            let release = first
                .receipt
                .resource_release_receipt_for(&command)
                .expect("Abort Resource release resolves")
                .expect("NotApplied Abort retains its Resource release")
                .clone();
            let stream = stream_read(&mut persistence)
                .expect("aborted stream reads")
                .current
                .expect("aborted stream exists");
            assert_eq!(stream.state, agent_protocol::AgentStreamState::Aborted);
            assert!(stream.publication_reservation.is_none());
            let session = session_read(&mut persistence)
                .expect("aborted Session reads")
                .current
                .expect("aborted Session exists");
            assert_eq!(session.open_stream_count, 0);
            let revision_after = stream_read(&mut persistence)
                .expect("terminal Abort revision reads")
                .revision;
            match phase {
                KillPhase::ObjectsStaged => assert_ne!(revision_before, revision_after),
                KillPhase::AcknowledgementLost => assert_eq!(revision_before, revision_after),
            }
            let replay = persistence
                .commit_agent(&command)
                .expect("external Abort replays a second time");
            replay
                .verify_for(&command)
                .expect("second external Abort replay verifies");
            assert!(replay.committed_revision.is_none());
            assert_eq!(replay.receipt, first.receipt);
            assert_eq!(
                stream_read(&mut persistence)
                    .expect("post-replay revision reads")
                    .revision,
                revision_after,
                "second external Abort replay must publish zero writes"
            );
            release
        };
        assert_eq!(release.pin, self.pin);
        assert_eq!(release.active_pin_count, 0);
        let pin = runtime
            .resource()
            .pin_current(&self.pin.pin_id)
            .expect("released pin reads")
            .expect("released pin exists");
        assert_eq!(
            pin.status,
            cymule_profile_protocol::resource::ResourcePinStatus::Released
        );
        let retention = runtime
            .resource()
            .retention_current(&self.pin.subject.family.retention_key)
            .expect("released family reads")
            .expect("released family exists");
        assert_eq!(retention.family, self.pin.subject.family);
        assert_eq!(retention.active_pin_count, 0);
        assert_eq!(
            retention.disposition,
            cymule_profile_protocol::resource::ResourceRetentionDisposition::Unretained
        );
        assert_sqlite_integrity(&self.database);
    }

    fn commit_abort_without_death(&self) -> agent_protocol::AgentCommand {
        let mut runtime = open_runtime(&self.database);
        let mut providers = UnusedAgentProviders;
        let mut persistence = RecordingPersistence::new(
            runtime.agent(&mut providers),
            Some(self.command_path.clone()),
        );
        let command = external_abort_command(&mut persistence)
            .expect("external Abort command seals without process death");
        persistence
            .commit_agent(&command)
            .expect("external Abort baseline commits");
        command
    }
}

#[derive(Debug, Clone, Copy)]
enum SidecarFault {
    Missing,
    Tampered,
}

fn fault_state_root_leaf_kind(path: &Path, kind: StateRootLeafKind, fault: SidecarFault) {
    let connection = Connection::open(path).expect("Agent tamper database opens");
    let mut statement = connection
        .prepare("SELECT object_id, object_json FROM cymule_state_root_objects WHERE domain = ?1")
        .expect("Agent StateRoot inventory query prepares");
    let rows = statement
        .query_map([STORE_DOMAIN], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .expect("Agent StateRoot inventory reads");
    let mut targets = Vec::new();
    for row in rows {
        let (object_id, bytes) = row.expect("Agent StateRoot inventory row reads");
        let object = decode_state_root_object(&bytes).expect("Agent StateRoot object decodes");
        if matches!(
            &object,
            StateRootObject::Value(value)
                if matches!(&value.value, StateRootValue::Leaf { kind: observed, .. } if *observed == kind)
        ) {
            targets.push((object_id, object));
        }
    }
    drop(statement);
    assert!(!targets.is_empty(), "Agent tamper target {kind:?} exists");
    for (object_id, object) in targets {
        match fault {
            SidecarFault::Missing => {
                connection
                    .execute(
                        "DELETE FROM cymule_state_root_objects WHERE domain = ?1 AND object_id = ?2",
                        params![STORE_DOMAIN, object_id],
                    )
                    .expect("Agent StateRoot sidecar deletes");
            }
            SidecarFault::Tampered => {
                let StateRootObject::Value(mut value) = object else {
                    unreachable!("selected sidecar is a value object")
                };
                let StateRootValue::Leaf { canonical_json, .. } = &mut value.value else {
                    unreachable!("selected sidecar is a typed leaf")
                };
                canonical_json.push(' ');
                let bytes = cymule_core::canonical_bytes(&StateRootObject::Value(value))
                    .expect("tampered sidecar still encodes");
                connection
                    .execute(
                        "UPDATE cymule_state_root_objects SET object_json = ?1 WHERE domain = ?2 AND object_id = ?3",
                        params![bytes, STORE_DOMAIN, object_id],
                    )
                    .expect("Agent StateRoot sidecar tampers");
            }
        }
    }
    assert_sqlite_integrity(path);
}

fn agent_cas_boundary_count() -> usize {
    let case = AgentFaultCase::new(0);
    let store = FaultingSqliteStore::counting(&case.database);
    let mut runtime = open_runtime_with_store(store);
    let mut providers = UnusedAgentProviders;
    let persistence = RecordingPersistence::new(runtime.agent(&mut providers), None);
    let host = LedgerHost {
        database: case.ledger.clone(),
    };
    let (mut persistence, host) =
        converge_scenario(persistence, host).expect("successful Agent baseline converges");
    assert_terminal_scenario(&mut persistence, &host).expect("baseline Agent state is terminal");
    drop(persistence);
    let (store, _) = runtime.into_parts();
    let boundary_count = store.calls();
    assert!(
        boundary_count >= 9,
        "Agent baseline crosses Tool, occurrence, Session, and stream CAS boundaries"
    );
    boundary_count
}

#[test]
fn every_agent_owned_cas_boundary_survives_real_process_death() {
    let boundary_count = agent_cas_boundary_count();
    for phase in [KillPhase::ObjectsStaged, KillPhase::AcknowledgementLost] {
        for fail_at in 1..=boundary_count {
            let phase_offset =
                usize::from(phase == KillPhase::AcknowledgementLost) * boundary_count;
            let seed = u64::try_from(phase_offset + fail_at)
                .expect("Agent fault-matrix position fits u64");
            let case = AgentFaultCase::new(seed);
            case.kill_at(phase, fail_at);
            case.recover();
        }
    }
}

#[test]
fn session_close_atomically_cancels_all_tools_across_real_process_death() {
    let expected = AgentCloseFaultCase::new(10_000).close_without_death();
    for (index, phase) in [KillPhase::ObjectsStaged, KillPhase::AcknowledgementLost]
        .into_iter()
        .enumerate()
    {
        let case = AgentCloseFaultCase::new(
            10_001 + u64::try_from(index).expect("Agent Close phase index fits u64"),
        );
        case.kill_close(phase);
        assert_eq!(
            case.converge_close(phase),
            expected,
            "{phase:?} recovery must exactly match the no-death terminal authority"
        );
    }
}

#[test]
fn external_not_applied_abort_survives_real_store_process_death() {
    for (index, phase) in [KillPhase::ObjectsStaged, KillPhase::AcknowledgementLost]
        .into_iter()
        .enumerate()
    {
        let case = AgentExternalAbortFaultCase::new(
            20_000 + u64::try_from(index).expect("external Abort phase index fits u64"),
        );
        case.kill_abort(phase);
        case.converge_abort(phase);
    }
}

#[test]
fn retained_external_abort_replay_rejects_missing_or_tampered_terminal_sidecars() {
    for (index, (kind, fault)) in [
        (StateRootLeafKind::AgentStreamCurrent, SidecarFault::Missing),
        (
            StateRootLeafKind::AgentStreamCurrent,
            SidecarFault::Tampered,
        ),
        (StateRootLeafKind::ResourcePinCurrent, SidecarFault::Missing),
        (
            StateRootLeafKind::ResourcePinCurrent,
            SidecarFault::Tampered,
        ),
        (
            StateRootLeafKind::ResourceRetentionCurrent,
            SidecarFault::Missing,
        ),
        (
            StateRootLeafKind::ResourceRetentionCurrent,
            SidecarFault::Tampered,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let case = AgentExternalAbortFaultCase::new(
            21_000 + u64::try_from(index).expect("Abort tamper index fits u64"),
        );
        let command = case.commit_abort_without_death();
        fault_state_root_leaf_kind(&case.database, kind, fault);
        let mut runtime = open_runtime(&case.database);
        let mut providers = UnusedAgentProviders;
        let error = runtime
            .agent(&mut providers)
            .commit_agent(&command)
            .expect_err("retained Abort replay must authenticate Resource sidecars");
        assert!(
            matches!(error, cymule_durable::DurableError::Integrity { .. }),
            "{fault:?} {kind:?} returned {error:?}"
        );
    }
}

#[test]
fn retained_publication_reservation_rejects_missing_or_tampered_claim_and_resource_sidecars() {
    for (index, (kind, fault)) in [
        (StateRootLeafKind::AgentStreamCurrent, SidecarFault::Missing),
        (
            StateRootLeafKind::AgentStreamCurrent,
            SidecarFault::Tampered,
        ),
        (
            StateRootLeafKind::AgentTargetClaimCurrent,
            SidecarFault::Missing,
        ),
        (
            StateRootLeafKind::AgentTargetClaimCurrent,
            SidecarFault::Tampered,
        ),
        (StateRootLeafKind::ResourcePinCurrent, SidecarFault::Missing),
        (
            StateRootLeafKind::ResourcePinCurrent,
            SidecarFault::Tampered,
        ),
        (
            StateRootLeafKind::ResourceRetentionCurrent,
            SidecarFault::Missing,
        ),
        (
            StateRootLeafKind::ResourceRetentionCurrent,
            SidecarFault::Tampered,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let world = TestWorld::new(22_000 + u64::try_from(index).unwrap()).unwrap();
        let database = world
            .domain()
            .path("agent-reservation-replay.sqlite")
            .unwrap();
        initialize_store(&database);
        let command = {
            let mut runtime = open_runtime(&database);
            let mut provider = PublishedPublicationProvider {
                publish_calls: 0,
                unknown: true,
            };
            let command = {
                let mut persistence = runtime.agent(&mut provider);
                let command = open_external_finalize_command(&mut persistence).unwrap();
                assert!(matches!(
                    persistence.finalize_agent_stream(&command).unwrap(),
                    agent_protocol::AgentStreamFinalizeOutcome::PublicationOutcomeUnknown { .. }
                ));
                command
            };
            assert_eq!(provider.publish_calls, 1);
            command
        };
        fault_state_root_leaf_kind(&database, kind, fault);
        let mut runtime = open_runtime(&database);
        let mut providers = UnusedAgentProviders;
        let error = runtime
            .agent(&mut providers)
            .finalize_agent_stream(&command)
            .expect_err("reservation replay authenticates target and Resource sidecars");
        assert!(
            matches!(error, cymule_durable::DurableError::Integrity { .. }),
            "{fault:?} {kind:?} returned {error:?}"
        );
    }
}

#[test]
fn retained_external_finalization_rejects_missing_or_tampered_terminal_sidecars() {
    for (index, (kind, fault)) in [
        (StateRootLeafKind::AgentUpdateCurrent, SidecarFault::Missing),
        (
            StateRootLeafKind::AgentUpdateCurrent,
            SidecarFault::Tampered,
        ),
        (StateRootLeafKind::AgentStreamCurrent, SidecarFault::Missing),
        (
            StateRootLeafKind::AgentStreamCurrent,
            SidecarFault::Tampered,
        ),
        (
            StateRootLeafKind::AgentTargetClaimCurrent,
            SidecarFault::Missing,
        ),
        (
            StateRootLeafKind::AgentTargetClaimCurrent,
            SidecarFault::Tampered,
        ),
        (
            StateRootLeafKind::AgentMessageCurrent,
            SidecarFault::Missing,
        ),
        (
            StateRootLeafKind::AgentMessageCurrent,
            SidecarFault::Tampered,
        ),
        (StateRootLeafKind::ResourcePinCurrent, SidecarFault::Missing),
        (
            StateRootLeafKind::ResourcePinCurrent,
            SidecarFault::Tampered,
        ),
        (
            StateRootLeafKind::ResourceRetentionCurrent,
            SidecarFault::Missing,
        ),
        (
            StateRootLeafKind::ResourceRetentionCurrent,
            SidecarFault::Tampered,
        ),
        (
            StateRootLeafKind::ResourceCatalogRecord,
            SidecarFault::Missing,
        ),
        (
            StateRootLeafKind::ResourceCatalogRecord,
            SidecarFault::Tampered,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let world = TestWorld::new(23_000 + u64::try_from(index).unwrap()).unwrap();
        let database = world.domain().path("agent-finalize-replay.sqlite").unwrap();
        initialize_store(&database);
        let command = {
            let mut runtime = open_runtime(&database);
            let mut provider = PublishedPublicationProvider::default();
            let command = {
                let mut persistence = runtime.agent(&mut provider);
                let command = open_external_finalize_command(&mut persistence).unwrap();
                assert!(matches!(
                    persistence.finalize_agent_stream(&command).unwrap(),
                    agent_protocol::AgentStreamFinalizeOutcome::Committed { .. }
                ));
                command
            };
            assert_eq!(provider.publish_calls, 1);
            command
        };
        fault_state_root_leaf_kind(&database, kind, fault);
        let mut runtime = open_runtime(&database);
        let mut providers = UnusedAgentProviders;
        let error = runtime
            .agent(&mut providers)
            .finalize_agent_stream(&command)
            .expect_err("finalization replay authenticates claim, catalog, pin, and retention");
        assert!(
            matches!(error, cymule_durable::DurableError::Integrity { .. }),
            "{fault:?} {kind:?} returned {error:?}"
        );
    }
}

fn assert_sqlite_integrity(path: &Path) {
    let connection = Connection::open(path).expect("SQLite database opens for integrity probe");
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("SQLite journal mode reads");
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    let results = connection
        .prepare("PRAGMA integrity_check")
        .expect("SQLite integrity statement prepares")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("SQLite integrity check runs")
        .collect::<Result<Vec<_>, _>>()
        .expect("SQLite integrity rows read");
    assert_eq!(results, ["ok"]);
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("SQLite WAL checkpoint completes");
    let after_checkpoint: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("post-checkpoint SQLite integrity check runs");
    assert_eq!(after_checkpoint, "ok");
}
