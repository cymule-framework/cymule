//! Real process-death coverage for Agent occurrence and stream journals.

#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use cymule_agent::{
    AgentError, AgentHost, AgentHostOccurrence, AgentHostOccurrenceState, AgentHostRequest,
    AgentHostResponse, AgentInteractionController, AgentJournal, AgentOccurrenceResolution,
    AgentOccurrenceStore, AgentRecoveryController, AgentResult, AgentSession, AgentState,
    AgentStreamChunk, AgentStreamController, AgentStreamTarget, AgentUpdate, ContentBlock,
    ContextRequest, ContextSnapshot, ElicitationRequest, ElicitationResponse, MessageRole,
    ModelRequest, ModelResponse, PermissionRequest, PermissionResponse, ToolRequest, ToolResponse,
    WorkspaceChange, WorkspaceReceipt,
};
use cymule_core::Machine;
use cymule_durable::{
    DurableCoordinator, DurableResult, DurableState, DurableStore, StoreCommit, StoredState,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::{Connection, OptionalExtension};
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillPhase {
    BeforeCommit,
    AfterCommit,
}

struct KillStore {
    inner: SqliteStore,
    phase: KillPhase,
    fail_at: usize,
    calls: usize,
    marker: PathBuf,
}

impl KillStore {
    fn stop(&self) -> ! {
        fs::write(&self.marker, self.calls.to_string()).expect("kill marker writes");
        loop {
            thread::park_timeout(Duration::from_mins(1));
        }
    }
}

impl DurableStore for KillStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        self.calls += 1;
        if self.calls != self.fail_at {
            return self.inner.compare_and_swap(expected_revision, next);
        }
        match self.phase {
            KillPhase::BeforeCommit => self.stop(),
            KillPhase::AfterCommit => {
                self.inner.compare_and_swap(expected_revision, next)?;
                self.stop();
            }
        }
    }
}

struct LedgerHost {
    database: PathBuf,
}

impl LedgerHost {
    fn initialize(path: &Path) {
        let connection = Connection::open(path).expect("ledger opens");
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS agent_dispatches (
                    occurrence_key TEXT PRIMARY KEY NOT NULL,
                    dispatch_count INTEGER NOT NULL
                ) STRICT;",
            )
            .expect("ledger initializes");
    }

    fn dispatch(&self, request: &ToolRequest) -> AgentResult<ToolResponse> {
        let connection = Connection::open(&self.database)
            .map_err(|error| AgentError::Host(error.to_string()))?;
        connection
            .execute(
                "INSERT INTO agent_dispatches(occurrence_key, dispatch_count)
                 VALUES (?1, 1)
                 ON CONFLICT(occurrence_key) DO UPDATE SET
                    dispatch_count = dispatch_count + 1",
                [&request.tool_call_id],
            )
            .map_err(|error| AgentError::Host(error.to_string()))?;
        Ok(tool_response(request))
    }

    fn dispatched(&self, tool_call_id: &str) -> AgentResult<bool> {
        let connection = Connection::open(&self.database)
            .map_err(|error| AgentError::Host(error.to_string()))?;
        connection
            .query_row(
                "SELECT 1 FROM agent_dispatches WHERE occurrence_key = ?1",
                [tool_call_id],
                |_| Ok(()),
            )
            .optional()
            .map(|value| value.is_some())
            .map_err(|error| AgentError::Host(error.to_string()))
    }

    fn total_dispatches(path: &Path) -> usize {
        let connection = Connection::open(path).expect("ledger opens");
        let count: i64 = connection
            .query_row(
                "SELECT COALESCE(SUM(dispatch_count), 0) FROM agent_dispatches",
                [],
                |row| row.get(0),
            )
            .expect("dispatch count reads");
        usize::try_from(count).expect("dispatch count is non-negative")
    }
}

impl AgentHost for LedgerHost {
    fn bind_occurrence(&mut self, request: &AgentHostRequest) -> AgentResult<String> {
        if matches!(request, AgentHostRequest::Tool(_)) {
            Ok("binding:process-kill-tool/1".to_owned())
        } else {
            Err(AgentError::Host(
                "only tool requests are expected".to_owned(),
            ))
        }
    }

    fn select_context(&mut self, _request: ContextRequest) -> AgentResult<ContextSnapshot> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn invoke_model(&mut self, _request: ModelRequest) -> AgentResult<ModelResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn request_permission(
        &mut self,
        _request: PermissionRequest,
    ) -> AgentResult<PermissionResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn invoke_tool(&mut self, request: ToolRequest) -> AgentResult<ToolResponse> {
        self.dispatch(&request)
    }

    fn elicit(&mut self, _request: ElicitationRequest) -> AgentResult<ElicitationResponse> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn apply_workspace(&mut self, _change: WorkspaceChange) -> AgentResult<WorkspaceReceipt> {
        Err(AgentError::Host("not used".to_owned()))
    }

    fn reconcile_occurrence(
        &mut self,
        occurrence: &AgentHostOccurrence,
    ) -> AgentResult<AgentOccurrenceResolution> {
        let AgentHostRequest::Tool(request) = &occurrence.request else {
            return Err(AgentError::Host(
                "only tool requests are expected".to_owned(),
            ));
        };
        if self.dispatched(&request.tool_call_id)? {
            Ok(AgentOccurrenceResolution::Completed {
                response: AgentHostResponse::Tool(tool_response(request)),
            })
        } else {
            Ok(AgentOccurrenceResolution::NotApplied {
                evidence: vec![ContentBlock::Text {
                    text: "durable host ledger contains no dispatch".to_owned(),
                }],
            })
        }
    }
}

fn tool_request(tool_call_id: &str) -> AgentHostRequest {
    AgentHostRequest::Tool(ToolRequest {
        tool_call_id: tool_call_id.to_owned(),
        operation: "workspace.read".to_owned(),
        input: json!({"path": "README.md"}),
    })
}

fn tool_response(request: &ToolRequest) -> ToolResponse {
    ToolResponse {
        tool_call_id: request.tool_call_id.clone(),
        content: vec![ContentBlock::Json {
            value: json!({"exists": true}),
        }],
        occurrence_binding: "binding:process-kill-tool/1".to_owned(),
    }
}

#[test]
fn agent_process_kill_worker_entry() {
    let Ok(durable_database) = std::env::var("CYMULE_AGENT_KILL_DB") else {
        return;
    };
    let phase = match std::env::var("CYMULE_AGENT_KILL_PHASE")
        .expect("kill phase exists")
        .as_str()
    {
        "before_commit" => KillPhase::BeforeCommit,
        "after_commit" => KillPhase::AfterCommit,
        phase => panic!("unknown kill phase {phase}"),
    };
    let fail_at = std::env::var("CYMULE_AGENT_KILL_AT")
        .expect("kill boundary exists")
        .parse()
        .expect("kill boundary parses");
    let marker =
        PathBuf::from(std::env::var("CYMULE_AGENT_KILL_MARKER").expect("kill marker exists"));
    let ledger =
        PathBuf::from(std::env::var("CYMULE_AGENT_KILL_LEDGER").expect("host ledger exists"));
    let store = KillStore {
        inner: SqliteStore::open(durable_database, "domain:agent-kill")
            .expect("durable store opens"),
        phase,
        fail_at,
        calls: 0,
        marker,
    };
    let coordinator = DurableCoordinator::open(store).expect("domain opens");
    let mut controller = AgentInteractionController::resume(
        "session:process-kill",
        LedgerHost { database: ledger },
        coordinator,
    )
    .expect("controller opens");
    controller
        .execute("occurrence:process-kill", tool_request("tool:process-kill"))
        .expect("worker reaches its selected journal boundary");
    panic!("Agent kill worker unexpectedly completed");
}

#[test]
fn agent_session_process_kill_worker_entry() {
    let Ok(durable_database) = std::env::var("CYMULE_AGENT_SESSION_KILL_DB") else {
        return;
    };
    let marker = PathBuf::from(
        std::env::var("CYMULE_AGENT_SESSION_KILL_MARKER").expect("kill marker exists"),
    );
    let store = KillStore {
        inner: SqliteStore::open(durable_database, "domain:agent-session-kill")
            .expect("durable store opens"),
        phase: kill_phase("CYMULE_AGENT_SESSION_KILL_PHASE"),
        fail_at: 1,
        calls: 0,
        marker,
    };
    let mut coordinator = DurableCoordinator::open(store).expect("domain opens");
    coordinator
        .append(
            "session:journal-kill",
            &AgentUpdate::State {
                update_id: "update:session:running".to_owned(),
                state: AgentState::Running,
                stop_reason: None,
            },
        )
        .expect("worker reaches the selected Session-journal boundary");
    panic!("Agent Session kill worker unexpectedly completed");
}

#[test]
fn agent_stream_process_kill_worker_entry() {
    let Ok(durable_database) = std::env::var("CYMULE_AGENT_STREAM_KILL_DB") else {
        return;
    };
    let marker = PathBuf::from(
        std::env::var("CYMULE_AGENT_STREAM_KILL_MARKER").expect("kill marker exists"),
    );
    let store = KillStore {
        inner: SqliteStore::open(durable_database, "domain:agent-stream-kill")
            .expect("durable store opens"),
        phase: kill_phase("CYMULE_AGENT_STREAM_KILL_PHASE"),
        fail_at: 1,
        calls: 0,
        marker,
    };
    let mut coordinator = DurableCoordinator::open(store).expect("domain opens");
    AgentStreamController::finalize(
        &mut coordinator,
        "session:stream-kill",
        "stream:process-kill",
    )
    .expect("worker reaches the selected stream-finalization boundary");
    panic!("Agent stream kill worker unexpectedly completed");
}

#[test]
fn every_agent_occurrence_journal_boundary_survives_real_process_death() {
    for phase in ["before_commit", "after_commit"] {
        for fail_at in 1..=3 {
            let phase_seed = usize::from(phase == "after_commit") * 3 + fail_at;
            let world =
                TestWorld::new(u64::try_from(phase_seed).expect("fault-matrix position fits u64"))
                    .expect("Agent fault test world creates");
            let durable_database = world
                .domain()
                .path("durable.sqlite")
                .expect("durable database path resolves");
            let ledger = world
                .domain()
                .path("host.sqlite")
                .expect("host ledger path resolves");
            let marker = world
                .domain()
                .path("kill-ready")
                .expect("kill marker path resolves");
            LedgerHost::initialize(&ledger);
            let store = SqliteStore::open(&durable_database, "domain:agent-kill")
                .expect("durable store opens");
            DurableCoordinator::open(store)
                .expect("domain opens")
                .initialize(&Machine::new())
                .expect("domain initializes");

            let mut command =
                Command::new(std::env::current_exe().expect("test executable resolves"));
            command
                .arg("--exact")
                .arg("agent_process_kill_worker_entry")
                .arg("--nocapture")
                .env("CYMULE_AGENT_KILL_DB", &durable_database)
                .env("CYMULE_AGENT_KILL_LEDGER", &ledger)
                .env("CYMULE_AGENT_KILL_PHASE", phase)
                .env("CYMULE_AGENT_KILL_AT", fail_at.to_string())
                .env("CYMULE_AGENT_KILL_MARKER", &marker)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            let mut child = ManagedChild::spawn(&mut command).expect("kill worker starts");
            child
                .wait_for_path(&marker, Duration::from_secs(20))
                .expect("kill worker reaches the selected journal barrier");
            assert!(!child.terminate().expect("worker is reaped").success());
            assert!(child.is_reaped());

            let store = SqliteStore::open(&durable_database, "domain:agent-kill")
                .expect("durable store reopens");
            let mut coordinator = DurableCoordinator::open(store).expect("domain reopens");
            let occurrences = coordinator
                .load_occurrences("session:process-kill")
                .expect("occurrences reload");
            let current = occurrences
                .iter()
                .find(|occurrence| occurrence.occurrence_id == "occurrence:process-kill")
                .cloned();
            let host = LedgerHost {
                database: ledger.clone(),
            };
            match current.map(|occurrence| occurrence.state) {
                None => {
                    let mut controller = AgentInteractionController::resume(
                        "session:process-kill",
                        host,
                        coordinator,
                    )
                    .expect("controller reopens");
                    controller
                        .execute("occurrence:process-kill", tool_request("tool:process-kill"))
                        .expect("absent occurrence retries");
                }
                Some(AgentHostOccurrenceState::Prepared) => {
                    AgentRecoveryController::cancel_prepared(
                        &mut coordinator,
                        "session:process-kill",
                        "occurrence:process-kill",
                        vec![ContentBlock::Text {
                            text: "process barrier proves dispatch was not entered".to_owned(),
                        }],
                    )
                    .expect("prepared occurrence cancels");
                    execute_replacement(coordinator, host);
                }
                Some(AgentHostOccurrenceState::Started | AgentHostOccurrenceState::Unknown) => {
                    let mut host = host;
                    let recovered = AgentRecoveryController::reconcile(
                        &mut host,
                        &mut coordinator,
                        "session:process-kill",
                        "occurrence:process-kill",
                    )
                    .expect("started occurrence reconciles");
                    if recovered.state == AgentHostOccurrenceState::NotApplied {
                        execute_replacement(coordinator, host);
                    }
                }
                Some(AgentHostOccurrenceState::Completed) => {
                    let mut controller = AgentInteractionController::resume(
                        "session:process-kill",
                        host,
                        coordinator,
                    )
                    .expect("controller reopens");
                    controller
                        .execute("occurrence:process-kill", tool_request("tool:process-kill"))
                        .expect("completed response replays");
                }
                Some(AgentHostOccurrenceState::NotApplied) => {
                    execute_replacement(coordinator, host);
                }
            }
            assert_eq!(
                LedgerHost::total_dispatches(&ledger),
                1,
                "one logical recovery path dispatches the host at most once"
            );
            let store = SqliteStore::open(&durable_database, "domain:agent-kill")
                .expect("durable store reopens for integrity read");
            let mut coordinator = DurableCoordinator::open(store).expect("domain reopens");
            let terminal = coordinator
                .load_occurrences("session:process-kill")
                .expect("occurrences reload");
            assert!(terminal.iter().any(AgentHostOccurrence::is_terminal));
            coordinator
                .restore_machine()
                .expect("shared M1 Machine remains replayable");
        }
    }
}

#[test]
fn session_and_stream_journals_survive_real_process_death_on_both_cas_sides() {
    for phase in ["before_commit", "after_commit"] {
        let world = TestWorld::new(u64::from(phase == "after_commit"))
            .expect("Agent journal test world creates");
        let session_database = world
            .domain()
            .path("session.sqlite")
            .expect("Session database path resolves");
        DurableCoordinator::open(
            SqliteStore::open(&session_database, "domain:agent-session-kill")
                .expect("Session store opens"),
        )
        .expect("Session domain opens")
        .initialize(&Machine::new())
        .expect("Session domain initializes");
        let session_marker = world
            .domain()
            .path("session-ready")
            .expect("Session marker path resolves");
        run_and_kill(
            "agent_session_process_kill_worker_entry",
            &session_marker,
            &[
                ("CYMULE_AGENT_SESSION_KILL_DB", session_database.as_path()),
                ("CYMULE_AGENT_SESSION_KILL_PHASE", Path::new(phase)),
                ("CYMULE_AGENT_SESSION_KILL_MARKER", session_marker.as_path()),
            ],
        );
        let mut session_coordinator = DurableCoordinator::open(
            SqliteStore::open(&session_database, "domain:agent-session-kill")
                .expect("Session store reopens"),
        )
        .expect("Session domain reopens");
        let running = AgentUpdate::State {
            update_id: "update:session:running".to_owned(),
            state: AgentState::Running,
            stop_reason: None,
        };
        session_coordinator
            .append("session:journal-kill", &running)
            .expect("Session append converges");
        assert_eq!(
            session_coordinator
                .load("session:journal-kill")
                .expect("Session journal loads"),
            vec![running]
        );

        let stream_database = world
            .domain()
            .path("stream.sqlite")
            .expect("stream database path resolves");
        let mut stream_coordinator = DurableCoordinator::open(
            SqliteStore::open(&stream_database, "domain:agent-stream-kill")
                .expect("stream store opens"),
        )
        .expect("stream domain opens")
        .initialize(&Machine::new())
        .expect("stream domain initializes");
        AgentStreamController::open(
            &mut stream_coordinator,
            "session:stream-kill",
            "stream:process-kill",
            AgentStreamTarget::Message {
                message_id: "message:process-kill".to_owned(),
                role: MessageRole::Agent,
            },
        )
        .expect("stream opens");
        AgentStreamController::append(
            &mut stream_coordinator,
            "session:stream-kill",
            "stream:process-kill",
            AgentStreamChunk {
                sequence: 0,
                content: vec![ContentBlock::Text {
                    text: "durable output".to_owned(),
                }],
            },
        )
        .expect("stream chunk persists");
        drop(stream_coordinator);
        let stream_marker = world
            .domain()
            .path("stream-ready")
            .expect("stream marker path resolves");
        run_and_kill(
            "agent_stream_process_kill_worker_entry",
            &stream_marker,
            &[
                ("CYMULE_AGENT_STREAM_KILL_DB", stream_database.as_path()),
                ("CYMULE_AGENT_STREAM_KILL_PHASE", Path::new(phase)),
                ("CYMULE_AGENT_STREAM_KILL_MARKER", stream_marker.as_path()),
            ],
        );
        let mut reopened_stream = DurableCoordinator::open(
            SqliteStore::open(&stream_database, "domain:agent-stream-kill")
                .expect("stream store reopens"),
        )
        .expect("stream domain reopens");
        AgentStreamController::finalize(
            &mut reopened_stream,
            "session:stream-kill",
            "stream:process-kill",
        )
        .expect("stream finalization converges");
        let session = AgentSession::replay(
            "session:stream-kill",
            reopened_stream
                .load("session:stream-kill")
                .expect("Session journal loads"),
        )
        .expect("Session replays");
        assert_eq!(session.message_order, ["message:process-kill"]);
    }
}

fn kill_phase(variable: &str) -> KillPhase {
    match std::env::var(variable).expect("kill phase exists").as_str() {
        "before_commit" => KillPhase::BeforeCommit,
        "after_commit" => KillPhase::AfterCommit,
        phase => panic!("unknown kill phase {phase}"),
    }
}

fn run_and_kill(test_name: &str, marker: &Path, environment: &[(&str, &Path)]) {
    let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
    command
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    for (key, value) in environment {
        command.env(key, value);
    }
    let mut child = ManagedChild::spawn(&mut command).expect("kill worker starts");
    child
        .wait_for_path(marker, Duration::from_secs(20))
        .expect("kill worker reaches the selected journal barrier");
    assert!(!child.terminate().expect("worker is reaped").success());
    assert!(child.is_reaped());
}

fn execute_replacement(coordinator: DurableCoordinator<SqliteStore>, host: LedgerHost) {
    let mut controller =
        AgentInteractionController::resume("session:process-kill", host, coordinator)
            .expect("replacement controller opens");
    controller
        .execute(
            "occurrence:process-kill:replacement",
            tool_request("tool:process-kill"),
        )
        .expect("explicit replacement executes");
}
