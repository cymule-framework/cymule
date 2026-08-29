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
    ClockObservationAuthority, DurableResult, DurableRuntimeControl, DurableStoreControl,
    ExecutionClockAuthority,
};
use cymule_durable_protocol::{ClockObservation, ClockObservationRef};
use cymule_profile_protocol::ProtocolResult;
use cymule_profile_protocol::agent as agent_protocol;
use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError,
    RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::json;

const STORE_DOMAIN: &str = "domain:agent-process-kill";
const SESSION_ID: &str = "session:agent-process-kill";
const TOOL_ID: &str = "tool:agent-process-kill";
const HOST_TOOL_ID: &str = "tool:agent-process-kill-host";
const OCCURRENCE_ID: &str = "occurrence:agent-process-kill";
const STREAM_ID: &str = "stream:agent-process-kill";
const BINDING_ID: &str = "binding:agent-process-kill/1";

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

fn initialize_store(path: &Path) {
    let mut store = SqliteStore::open(path, STORE_DOMAIN).expect("Agent SQLite Store opens");
    DurableStoreControl::initialize(&mut store).expect("Agent SQLite /6 genesis commits");
}

fn open_runtime(path: &Path) -> AgentRuntime {
    let admission = cymule_runtime::ExecutionBindingAdmission::for_local_process(
        EmptyPlugin,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("Agent test execution binding admits");
    DurableRuntimeControl::open(
        SqliteStore::open(path, STORE_DOMAIN).expect("Agent SQLite Store reopens"),
        admission,
        UnusedClock,
    )
    .expect("Agent runtime opens")
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
    BeforeCommit,
    AfterCommit,
}

impl KillPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::BeforeCommit => "before_commit",
            Self::AfterCommit => "after_commit",
        }
    }
}

struct FaultingPersistence<P> {
    inner: P,
    phase: Option<KillPhase>,
    fail_at: usize,
    calls: usize,
    marker: Option<PathBuf>,
    command_path: Option<PathBuf>,
}

impl<P> FaultingPersistence<P> {
    const fn counting(inner: P) -> Self {
        Self {
            inner,
            phase: None,
            fail_at: 0,
            calls: 0,
            marker: None,
            command_path: None,
        }
    }

    fn killing(
        inner: P,
        phase: KillPhase,
        fail_at: usize,
        marker: PathBuf,
        command_path: PathBuf,
    ) -> Self {
        Self {
            inner,
            phase: Some(phase),
            fail_at,
            calls: 0,
            marker: Some(marker),
            command_path: Some(command_path),
        }
    }

    const fn calls(&self) -> usize {
        self.calls
    }

    fn begin(&mut self, command: &agent_protocol::AgentCommand) -> usize {
        self.calls += 1;
        if let Some(path) = &self.command_path {
            fs::write(
                path,
                cymule_core::canonical_bytes(command).expect("Agent command encodes"),
            )
            .expect("Agent recovery command records");
        }
        if self.phase == Some(KillPhase::BeforeCommit) && self.calls == self.fail_at {
            self.stop();
        }
        self.calls
    }

    fn finish(&self, call: usize) {
        if self.phase == Some(KillPhase::AfterCommit) && call == self.fail_at {
            self.stop();
        }
    }

    fn stop(&self) -> ! {
        let phase = self.phase.expect("kill persistence owns a phase");
        fs::write(
            self.marker
                .as_ref()
                .expect("kill persistence owns a marker"),
            format!("{}:{}", self.calls, phase.label()),
        )
        .expect("Agent kill marker writes");
        loop {
            thread::park_timeout(Duration::from_mins(1));
        }
    }
}

impl<P: AgentPersistence> AgentPersistence for FaultingPersistence<P> {
    fn commit_agent(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> AgentResult<agent_protocol::AgentCommit> {
        let call = self.begin(command);
        let outcome = self.inner.commit_agent(command);
        self.finish(call);
        outcome
    }

    fn finalize_agent_stream(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> AgentResult<agent_protocol::AgentStreamFinalizeOutcome> {
        let call = self.begin(command);
        let outcome = self.inner.finalize_agent_stream(command);
        self.finish(call);
        outcome
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
        let call = self.begin(command);
        let outcome = self.inner.commit_agent_workspace(command);
        self.finish(call);
        outcome
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
        "before_commit" => KillPhase::BeforeCommit,
        "after_commit" => KillPhase::AfterCommit,
        phase => panic!("unknown Agent kill phase {phase}"),
    };
    let fail_at = std::env::var("CYMULE_AGENT_KILL_AT")
        .expect("Agent kill boundary exists")
        .parse()
        .expect("Agent kill boundary parses");
    let mut runtime = open_runtime(Path::new(&database));
    let mut providers = UnusedAgentProviders;
    let persistence = FaultingPersistence::killing(
        runtime.agent(&mut providers),
        phase,
        fail_at,
        marker,
        command_path,
    );
    converge_scenario(persistence, LedgerHost { database: ledger })
        .expect("Agent worker reaches its selected CAS boundary");
    panic!("Agent kill worker unexpectedly completed");
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

fn agent_cas_boundary_count() -> usize {
    let case = AgentFaultCase::new(0);
    let mut runtime = open_runtime(&case.database);
    let mut providers = UnusedAgentProviders;
    let persistence = FaultingPersistence::counting(runtime.agent(&mut providers));
    let host = LedgerHost {
        database: case.ledger.clone(),
    };
    let (mut persistence, host) =
        converge_scenario(persistence, host).expect("successful Agent baseline converges");
    let boundary_count = persistence.calls();
    assert_terminal_scenario(&mut persistence, &host).expect("baseline Agent state is terminal");
    assert!(
        boundary_count >= 9,
        "Agent baseline crosses Tool, occurrence, Session, and stream CAS boundaries"
    );
    boundary_count
}

#[test]
fn every_agent_owned_cas_boundary_survives_real_process_death() {
    let boundary_count = agent_cas_boundary_count();
    for phase in [KillPhase::BeforeCommit, KillPhase::AfterCommit] {
        for fail_at in 1..=boundary_count {
            let phase_offset = usize::from(phase == KillPhase::AfterCommit) * boundary_count;
            let seed = u64::try_from(phase_offset + fail_at)
                .expect("Agent fault-matrix position fits u64");
            let case = AgentFaultCase::new(seed);
            case.kill_at(phase, fail_at);
            case.recover();
        }
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
