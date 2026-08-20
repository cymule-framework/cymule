//! Artifact-reference closure tests for every Agent journal surface.

use cymule_agent::{
    AgentError, AgentHostOccurrence, AgentHostRequest, AgentHostResponse, AgentJournal,
    AgentMessage, AgentOccurrenceStore, AgentUpdate, ContentBlock, ContextRequest, ContextSnapshot,
    ElicitationRequest, MemoryAgentJournal, MessageRole, ModelRequest, ToolResponse,
    WorkspaceChange, WorkspaceReceipt,
};
use cymule_core::{ARTIFACT_IDENTITY_VERSION, ArtifactRef, Machine};
use cymule_durable::{DurableCoordinator, JournalRecord, MemoryStore};
use serde_json::json;

fn artifact(digit: char) -> ArtifactRef {
    ArtifactRef {
        identity_version: ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: format!("sha256:{}", digit.to_string().repeat(64)),
        kind: "agent/content".to_owned(),
    }
}

fn malformed_artifacts() -> [ArtifactRef; 3] {
    [
        ArtifactRef {
            identity_version: "cymule.artifact/1".to_owned(),
            ..artifact('a')
        },
        ArtifactRef {
            artifact_id: "sha256:not-a-digest".to_owned(),
            ..artifact('b')
        },
        ArtifactRef {
            kind: "Invalid Kind".to_owned(),
            ..artifact('c')
        },
    ]
}

fn artifact_block(reference: ArtifactRef) -> ContentBlock {
    ContentBlock::Artifact {
        artifact: reference,
    }
}

fn artifact_message(reference: ArtifactRef) -> AgentMessage {
    AgentMessage {
        message_id: "message:artifact".to_owned(),
        role: MessageRole::Agent,
        content: vec![artifact_block(reference)],
    }
}

#[test]
fn session_journals_reject_every_malformed_artifact_before_append() {
    for (index, malformed) in malformed_artifacts().into_iter().enumerate() {
        let mut journal = MemoryAgentJournal::default();
        let update = AgentUpdate::Message {
            update_id: format!("update:malformed:{index}"),
            message: artifact_message(malformed),
        };
        assert!(matches!(
            journal.append("session:malformed", &update),
            Err(AgentError::Validation(_))
        ));
        assert!(
            journal
                .load("session:malformed")
                .expect("rejected update leaves a readable journal")
                .is_empty()
        );
    }
}

#[test]
fn occurrence_validation_walks_requests_responses_and_recovery_evidence() {
    let invalid = malformed_artifacts()[0].clone();
    let message = artifact_message(invalid.clone());
    let requests = [
        AgentHostRequest::Context(ContextRequest {
            session_id: "session:artifact".to_owned(),
            messages: vec![message],
            budget: 1,
        }),
        AgentHostRequest::Model(ModelRequest {
            session_id: "session:artifact".to_owned(),
            context: ContextSnapshot {
                snapshot_id: "context:artifact".to_owned(),
                content: vec![artifact_block(invalid.clone())],
                occurrence_binding: "binding:context/1".to_owned(),
            },
            tools: Vec::new(),
        }),
        AgentHostRequest::Elicitation(ElicitationRequest {
            request_id: "elicitation:artifact".to_owned(),
            schema: json!({"type": "string"}),
            prompt: vec![artifact_block(invalid.clone())],
        }),
        AgentHostRequest::Workspace(WorkspaceChange {
            change_id: "change:artifact".to_owned(),
            overlay: invalid.clone(),
            commit: true,
        }),
    ];
    for (index, request) in requests.into_iter().enumerate() {
        assert!(matches!(
            AgentHostOccurrence::prepare(
                format!("occurrence:invalid:{index}"),
                "session:artifact",
                request,
                "binding:artifact/1",
            ),
            Err(AgentError::Validation(_))
        ));
    }

    let prepared = AgentHostOccurrence::prepare(
        "occurrence:valid",
        "session:artifact",
        AgentHostRequest::Workspace(WorkspaceChange {
            change_id: "change:valid".to_owned(),
            overlay: artifact('d'),
            commit: true,
        }),
        "binding:workspace/1",
    )
    .expect("valid occurrence prepares");
    let started = prepared.start().expect("valid occurrence starts");
    assert!(matches!(
        started.complete(AgentHostResponse::Workspace(WorkspaceReceipt {
            change_id: "change:valid".to_owned(),
            committed: true,
            evidence: invalid.clone(),
            occurrence_binding: "binding:workspace/1".to_owned(),
        })),
        Err(AgentError::Validation(_))
    ));
    assert!(matches!(
        started.mark_unknown_with_evidence("ambiguous", vec![artifact_block(invalid.clone())],),
        Err(AgentError::Validation(_))
    ));
    assert!(matches!(
        started.mark_not_applied(vec![artifact_block(invalid)]),
        Err(AgentError::Validation(_))
    ));

    let tool_prepared = AgentHostOccurrence::prepare(
        "occurrence:tool",
        "session:artifact",
        AgentHostRequest::Tool(cymule_agent::ToolRequest {
            tool_call_id: "tool:artifact".to_owned(),
            operation: "artifact.read".to_owned(),
            input: json!({}),
        }),
        "binding:tool/1",
    )
    .expect("tool occurrence prepares")
    .start()
    .expect("tool occurrence starts");
    assert!(matches!(
        tool_prepared.complete(AgentHostResponse::Tool(ToolResponse {
            tool_call_id: "tool:artifact".to_owned(),
            content: vec![artifact_block(malformed_artifacts()[1].clone())],
            occurrence_binding: "binding:tool/1".to_owned(),
        })),
        Err(AgentError::Validation(_))
    ));
}

#[test]
fn durable_replay_rejects_a_missing_artifact_reference_field() {
    let mut coordinator = DurableCoordinator::open(MemoryStore::new())
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("store initializes");
    coordinator
        .append_journal_record(
            "session:corrupt",
            JournalRecord::new(
                "update:corrupt",
                "cymule.agent-update/1",
                json!({
                    "type": "message",
                    "update_id": "update:corrupt",
                    "message": {
                        "message_id": "message:corrupt",
                        "role": "agent",
                        "content": [{
                            "type": "artifact",
                            "artifact": {
                                "artifact_id": format!("sha256:{}", "a".repeat(64)),
                                "kind": "agent/content"
                            }
                        }]
                    }
                }),
            )
            .expect("generic journal record seals"),
        )
        .expect("generic durable boundary stores opaque application bytes");

    assert!(matches!(
        AgentJournal::load(&mut coordinator, "session:corrupt"),
        Err(AgentError::Persistence(_))
    ));
}

#[test]
fn occurrence_store_never_retains_a_malformed_artifact_snapshot() {
    let mut journal = MemoryAgentJournal::default();
    let valid = AgentHostOccurrence::prepare(
        "occurrence:store",
        "session:store",
        AgentHostRequest::Workspace(WorkspaceChange {
            change_id: "change:store".to_owned(),
            overlay: artifact('e'),
            commit: true,
        }),
        "binding:workspace/1",
    )
    .expect("valid occurrence prepares");
    let mut malformed = valid;
    let AgentHostRequest::Workspace(change) = &mut malformed.request else {
        unreachable!();
    };
    change.overlay.artifact_id = "sha256:fake".to_owned();
    assert!(matches!(
        journal.record_occurrence(&malformed),
        Err(AgentError::Validation(_))
    ));
    assert!(
        journal
            .load_occurrences("session:store")
            .expect("rejected occurrence leaves a readable journal")
            .is_empty()
    );
}
