//! Durable staging and finalization tests for protocol-neutral Agent streams.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use cymule_agent::{
    AgentError, AgentJournal, AgentMessage, AgentSession, AgentStreamChunk, AgentStreamController,
    AgentStreamRecord, AgentStreamState, AgentStreamTarget, AgentUpdate, ContentBlock, MessageRole,
    ToolCall, ToolCallStatus,
};
use cymule_core::Machine;
use cymule_durable::{
    DurableCoordinator, DurableError, DurableResult, DurableState, DurableStore, MemoryStore,
    StoreCommit, StoredState,
};
use cymule_resource::ResourceCandidate;
use serde_json::json;

fn coordinator(store: MemoryStore) -> DurableCoordinator<MemoryStore> {
    DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("store initializes")
}

fn message_target(message_id: &str) -> AgentStreamTarget {
    AgentStreamTarget::Message {
        message_id: message_id.to_owned(),
        role: MessageRole::Agent,
    }
}

fn text_chunk(sequence: u64, text: &str) -> AgentStreamChunk {
    AgentStreamChunk {
        sequence,
        content: vec![ContentBlock::Text {
            text: text.to_owned(),
        }],
    }
}

fn session<S: DurableStore>(
    coordinator: &mut DurableCoordinator<S>,
    session_id: &str,
) -> AgentSession {
    AgentSession::replay(
        session_id,
        AgentJournal::load(coordinator, session_id).expect("Session journal loads"),
    )
    .expect("Session replays")
}

#[test]
fn frozen_stream_fixture_replays_to_one_finalized_message() {
    let records: Vec<AgentStreamRecord> =
        serde_json::from_str(include_str!("fixtures/agent-stream-records.json"))
            .expect("stream fixture deserializes");
    let stream =
        cymule_agent::AgentStreamProjection::replay(records).expect("stream fixture replays");
    assert_eq!(stream.state, AgentStreamState::Finalized);
    assert_eq!(
        stream.content_digest.as_deref(),
        Some("57e90e6cb7aff1276e78399ad62cee581909f0d4944c24801d529c141c23a241")
    );
}

#[test]
fn message_chunks_are_invisible_until_atomic_finalization_and_reopen() {
    let store = MemoryStore::new();
    let mut coordinator = coordinator(store.clone());
    AgentStreamController::open(
        &mut coordinator,
        "session:stream-message",
        "stream:message:1",
        message_target("message:streamed:1"),
    )
    .expect("stream opens");
    AgentStreamController::append(
        &mut coordinator,
        "session:stream-message",
        "stream:message:1",
        text_chunk(0, "hello "),
    )
    .expect("first chunk stages");
    let resource = ResourceCandidate::text("large output reference")
        .seal()
        .expect("Resource seals");
    AgentStreamController::append(
        &mut coordinator,
        "session:stream-message",
        "stream:message:1",
        AgentStreamChunk {
            sequence: 1,
            content: vec![
                ContentBlock::Text {
                    text: "world".to_owned(),
                },
                ContentBlock::ResourceHandle {
                    resource: resource.clone(),
                },
            ],
        },
    )
    .expect("second chunk stages");
    assert!(
        session(&mut coordinator, "session:stream-message")
            .messages
            .is_empty()
    );

    let finalized = AgentStreamController::finalize(
        &mut coordinator,
        "session:stream-message",
        "stream:message:1",
    )
    .expect("stream finalizes");
    assert_eq!(finalized.stream.state, AgentStreamState::Finalized);
    let message = &finalized.session.expect("Session returned").messages["message:streamed:1"];
    assert_eq!(message.content.len(), 3);
    assert_eq!(
        message.content[2],
        ContentBlock::ResourceHandle { resource }
    );
    let revision = finalized.revision;
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    let replay = AgentStreamController::finalize(
        &mut reopened,
        "session:stream-message",
        "stream:message:1",
    )
    .expect("finalization retry replays");
    assert_eq!(replay.revision, revision);
    assert_eq!(replay.stream.state, AgentStreamState::Finalized);
    assert!(matches!(
        AgentStreamController::append(
            &mut reopened,
            "session:stream-message",
            "stream:message:1",
            text_chunk(2, "late"),
        ),
        Err(AgentError::IllegalTransition(_))
    ));
}

#[test]
fn chunk_order_retry_conflict_and_abort_fail_closed() {
    let mut coordinator = coordinator(MemoryStore::new());
    AgentStreamController::open(
        &mut coordinator,
        "session:stream-abort",
        "stream:abort:1",
        message_target("message:aborted:1"),
    )
    .expect("stream opens");
    let before = coordinator.revision().expect("revision").to_owned();
    assert!(matches!(
        AgentStreamController::append(
            &mut coordinator,
            "session:stream-abort",
            "stream:abort:1",
            text_chunk(1, "out of order"),
        ),
        Err(AgentError::IllegalTransition(_))
    ));
    assert_eq!(coordinator.revision(), Some(before.as_str()));
    assert!(matches!(
        AgentStreamController::append(
            &mut coordinator,
            "session:stream-abort",
            "stream:abort:1",
            text_chunk(0, &"x".repeat(cymule_agent::AGENT_STREAM_CHUNK_LIMIT + 1)),
        ),
        Err(AgentError::Validation(_))
    ));
    let accepted = AgentStreamController::append(
        &mut coordinator,
        "session:stream-abort",
        "stream:abort:1",
        text_chunk(0, "tentative"),
    )
    .expect("chunk stages");
    let retried = AgentStreamController::append(
        &mut coordinator,
        "session:stream-abort",
        "stream:abort:1",
        text_chunk(0, "tentative"),
    )
    .expect("chunk retry is idempotent");
    assert_eq!(accepted.revision, retried.revision);
    assert!(matches!(
        AgentStreamController::append(
            &mut coordinator,
            "session:stream-abort",
            "stream:abort:1",
            text_chunk(0, "conflicting"),
        ),
        Err(AgentError::IllegalTransition(_))
    ));
    let aborted = AgentStreamController::abort(
        &mut coordinator,
        "session:stream-abort",
        "stream:abort:1",
        "caller cancelled output",
    )
    .expect("stream aborts");
    assert_eq!(aborted.stream.state, AgentStreamState::Aborted);
    assert!(matches!(
        AgentStreamController::finalize(&mut coordinator, "session:stream-abort", "stream:abort:1",),
        Err(AgentError::IllegalTransition(_))
    ));
    assert!(
        session(&mut coordinator, "session:stream-abort")
            .messages
            .is_empty()
    );
}

#[test]
fn tool_stream_finalization_uses_the_existing_tool_identity() {
    let mut coordinator = coordinator(MemoryStore::new());
    let pending = ToolCall {
        tool_call_id: "tool:streamed:1".to_owned(),
        operation: "workspace.read".to_owned(),
        status: ToolCallStatus::Pending,
        input: json!({"path": "README.md"}),
        output: None,
        locations: Vec::new(),
    };
    AgentJournal::append(
        &mut coordinator,
        "session:stream-tool",
        &AgentUpdate::Tool {
            update_id: "update:tool:pending".to_owned(),
            tool: pending.clone(),
        },
    )
    .expect("pending tool records");
    let mut in_progress = pending;
    in_progress.status = ToolCallStatus::InProgress;
    AgentJournal::append(
        &mut coordinator,
        "session:stream-tool",
        &AgentUpdate::Tool {
            update_id: "update:tool:in-progress".to_owned(),
            tool: in_progress,
        },
    )
    .expect("running tool records");
    AgentStreamController::open(
        &mut coordinator,
        "session:stream-tool",
        "stream:tool:1",
        AgentStreamTarget::Tool {
            tool_call_id: "tool:streamed:1".to_owned(),
        },
    )
    .expect("tool stream opens");
    AgentStreamController::append(
        &mut coordinator,
        "session:stream-tool",
        "stream:tool:1",
        AgentStreamChunk {
            sequence: 0,
            content: vec![ContentBlock::Json {
                value: json!({"exists": true}),
            }],
        },
    )
    .expect("tool output stages");
    let checkpoint =
        AgentStreamController::finalize(&mut coordinator, "session:stream-tool", "stream:tool:1")
            .expect("tool output finalizes");
    let tool = &checkpoint.session.expect("Session returned").tools["tool:streamed:1"];
    assert_eq!(tool.status, ToolCallStatus::Completed);
    assert_eq!(
        tool.output,
        Some(vec![ContentBlock::Json {
            value: json!({"exists": true}),
        }])
    );
}

#[test]
fn stale_finalizer_commits_neither_terminal_stream_nor_session_message() {
    let store = MemoryStore::new();
    let mut current = coordinator(store.clone());
    AgentStreamController::open(
        &mut current,
        "session:stream-stale",
        "stream:stale:1",
        message_target("message:stale:1"),
    )
    .expect("stream opens");
    AgentStreamController::append(
        &mut current,
        "session:stream-stale",
        "stream:stale:1",
        text_chunk(0, "first"),
    )
    .expect("first chunk stages");
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    AgentStreamController::append(
        &mut current,
        "session:stream-stale",
        "stream:stale:1",
        text_chunk(1, "second"),
    )
    .expect("current writer advances");
    assert!(matches!(
        AgentStreamController::finalize(&mut stale, "session:stream-stale", "stream:stale:1",),
        Err(AgentError::Persistence(_))
    ));
    drop(current);

    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    let stream =
        AgentStreamController::load(&mut reopened, "session:stream-stale", "stream:stale:1")
            .expect("stream replays");
    assert_eq!(stream.state, AgentStreamState::Open);
    assert_eq!(stream.chunks.len(), 2);
    assert!(
        session(&mut reopened, "session:stream-stale")
            .messages
            .is_empty()
    );
}

#[derive(Clone)]
struct LostReceiptStore {
    inner: MemoryStore,
    lose_next_commit_receipt: Arc<AtomicBool>,
}

impl DurableStore for LostReceiptStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if self.lose_next_commit_receipt.swap(false, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated loss after durable stream finalization".to_owned(),
            ));
        }
        Ok(commit)
    }
}

#[test]
fn lost_finalization_receipt_reopens_to_one_final_message() {
    let fail = Arc::new(AtomicBool::new(false));
    let store = LostReceiptStore {
        inner: MemoryStore::new(),
        lose_next_commit_receipt: fail.clone(),
    };
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("store initializes");
    AgentStreamController::open(
        &mut coordinator,
        "session:stream-receipt-loss",
        "stream:receipt-loss:1",
        message_target("message:receipt-loss:1"),
    )
    .expect("stream opens");
    AgentStreamController::append(
        &mut coordinator,
        "session:stream-receipt-loss",
        "stream:receipt-loss:1",
        text_chunk(0, "durable final output"),
    )
    .expect("chunk stages");
    fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        AgentStreamController::finalize(
            &mut coordinator,
            "session:stream-receipt-loss",
            "stream:receipt-loss:1",
        ),
        Err(AgentError::Persistence(_))
    ));
    drop(coordinator);

    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    let recovered = AgentStreamController::finalize(
        &mut reopened,
        "session:stream-receipt-loss",
        "stream:receipt-loss:1",
    )
    .expect("retained finalization replays");
    assert_eq!(recovered.stream.state, AgentStreamState::Finalized);
    let session = recovered.session.expect("Session returned");
    assert_eq!(session.messages.len(), 1);
    assert_eq!(session.message_order, ["message:receipt-loss:1"]);
}

#[test]
fn finalized_message_identity_cannot_be_reinterpreted() {
    let mut session = AgentSession::new("session:message-conflict");
    let original = AgentMessage {
        message_id: "message:fixed".to_owned(),
        role: MessageRole::Agent,
        content: vec![ContentBlock::Text {
            text: "original".to_owned(),
        }],
    };
    session
        .apply(AgentUpdate::Message {
            update_id: "update:message:original".to_owned(),
            message: original,
        })
        .expect("message applies");
    assert!(matches!(
        session.apply(AgentUpdate::Message {
            update_id: "update:message:replacement".to_owned(),
            message: AgentMessage {
                message_id: "message:fixed".to_owned(),
                role: MessageRole::Agent,
                content: vec![ContentBlock::Text {
                    text: "replacement".to_owned(),
                }],
            },
        }),
        Err(AgentError::IllegalTransition(_))
    ));
}
