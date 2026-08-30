pub use cymule_profile_protocol::agent::{
    AGENT_STREAM_CHUNK_LIMIT, AGENT_STREAM_STAGING_BYTES_LIMIT, AgentStreamChunk,
    AgentStreamCurrent, AgentStreamDelivery, AgentStreamEffect, AgentStreamFinalizeOutcome,
    AgentStreamPostcondition, AgentStreamPublicationContent, AgentStreamPublicationIntent,
    AgentStreamPublicationObservation, AgentStreamPublicationResult, AgentStreamQuery,
    AgentStreamRead, AgentStreamState, AgentStreamTarget, MAX_AGENT_STREAM_CHUNKS,
};

use crate::{AgentPersistence, AgentResult};
use cymule_profile_protocol::agent::{
    AgentCommand, AgentCommandAction, AgentCommit, AgentStreamCommand,
};

/// Typed controller for one Agent stream lifecycle.
///
/// Every mutation is one closed [`AgentStreamCommand`] committed through the
/// Agent persistence capability. The caller supplies the exact source
/// revision so a lost commit response can be retried as the identical
/// [`AgentCommand`] and resolved from its retained receipt.
pub struct AgentStreamController;

impl AgentStreamController {
    /// Admit one caller-identified stream at an exact `StateRoot` revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is malformed, its source revision is
    /// stale, its target is not admissible, or persistence cannot establish a
    /// definitive commit result.
    pub fn open<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        source_revision: &str,
        session_id: &str,
        stream_id: &str,
        target: AgentStreamTarget,
        delivery: AgentStreamDelivery,
    ) -> AgentResult<AgentCommit> {
        Self::commit(
            persistence,
            source_revision,
            AgentStreamCommand::Open {
                session_id: session_id.to_owned(),
                stream_id: stream_id.to_owned(),
                target,
                delivery,
            },
        )
    }

    /// Append the exact next contiguous chunk at an exact `StateRoot` revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the chunk is malformed or out of order, the
    /// stream is not open, the source revision is stale, or persistence cannot
    /// establish a definitive commit result.
    pub fn append<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        source_revision: &str,
        session_id: &str,
        stream_id: &str,
        chunk: AgentStreamChunk,
    ) -> AgentResult<AgentCommit> {
        Self::commit(
            persistence,
            source_revision,
            AgentStreamCommand::AppendChunk {
                session_id: session_id.to_owned(),
                stream_id: stream_id.to_owned(),
                chunk,
            },
        )
    }

    /// Abort one open stream at an exact `StateRoot` revision.
    ///
    /// # Errors
    ///
    /// Returns an error when the reason is invalid, the stream is not open,
    /// the source revision is stale, or persistence cannot establish a
    /// definitive commit result.
    pub fn abort<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        source_revision: &str,
        session_id: &str,
        stream_id: &str,
        reason: &str,
    ) -> AgentResult<AgentCommit> {
        Self::commit(
            persistence,
            source_revision,
            AgentStreamCommand::Abort {
                session_id: session_id.to_owned(),
                stream_id: stream_id.to_owned(),
                reason: reason.to_owned(),
            },
        )
    }

    /// Atomically finalize one stream and its Session projection.
    ///
    /// The command contains only semantic intent. Staged delivery is handled by
    /// the ordinary closed commit path. External delivery requires the Durable
    /// binding-pinned publication-authority method and is rejected by ordinary
    /// `commit_agent`; no provider publication is accepted as command input.
    ///
    /// # Errors
    ///
    /// Returns an error when inline/external finalization does not match the
    /// retained stream, the source revision is stale, or persistence cannot
    /// establish a definitive commit result.
    pub fn finalize<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        source_revision: &str,
        session_id: &str,
        stream_id: &str,
    ) -> AgentResult<AgentStreamFinalizeOutcome> {
        let command = AgentCommand::new(
            source_revision.to_owned(),
            AgentCommandAction::Stream(AgentStreamCommand::Finalize {
                session_id: session_id.to_owned(),
                stream_id: stream_id.to_owned(),
            }),
        )?;
        let outcome = persistence.finalize_agent_stream(&command)?;
        if let AgentStreamFinalizeOutcome::Committed { commit } = &outcome {
            commit.verify_for(&command)?;
        }
        Ok(outcome)
    }

    /// Reconcile one prior external finalization without publishing again.
    ///
    /// The identical Finalize command reselects its pinned source revision and
    /// deterministically derives the same immutable publication intent and
    /// exact-matches it against `expected_intent`, which may be restored from
    /// the prior Unknown outcome. The persistence implementation may only
    /// observe that intent.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is malformed or stale, the provider
    /// cannot observe the exact intent, or a resulting commit cannot verify.
    pub fn reconcile_finalization<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        source_revision: &str,
        session_id: &str,
        stream_id: &str,
        expected_intent: &AgentStreamPublicationIntent,
    ) -> AgentResult<AgentStreamFinalizeOutcome> {
        let command = AgentCommand::new(
            source_revision.to_owned(),
            AgentCommandAction::Stream(AgentStreamCommand::Finalize {
                session_id: session_id.to_owned(),
                stream_id: stream_id.to_owned(),
            }),
        )?;
        let outcome = persistence.reconcile_agent_stream(&command, expected_intent)?;
        if let AgentStreamFinalizeOutcome::Committed { commit } = &outcome {
            commit.verify_for(&command)?;
        }
        Ok(outcome)
    }

    /// Read one exact, optionally revision-pinned stream current.
    ///
    /// # Errors
    ///
    /// Returns an error when the query is invalid, its revision is stale, or
    /// persistence returns a read that does not verify against the query.
    pub fn load<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        query: &AgentStreamQuery,
    ) -> AgentResult<AgentStreamRead> {
        query.verify()?;
        let read = persistence.read_agent_stream(query)?;
        read.verify_for(query)?;
        Ok(read)
    }

    fn commit<P: AgentPersistence + ?Sized>(
        persistence: &mut P,
        source_revision: &str,
        stream: AgentStreamCommand,
    ) -> AgentResult<AgentCommit> {
        let command = AgentCommand::new(
            source_revision.to_owned(),
            AgentCommandAction::Stream(stream),
        )?;
        let commit = persistence.commit_agent(&command)?;
        commit.verify_for(&command)?;
        Ok(commit)
    }
}

#[cfg(test)]
mod tests {
    use cymule_profile_protocol::agent::{AgentCommandOutcome, ContentBlock, MessageRole};

    use super::*;
    use crate::{AgentError, EphemeralAgentPersistence};

    const SESSION_ID: &str = "session:stream-controller";
    const STREAM_ID: &str = "stream:controller";

    fn stream_query(expected_revision: Option<String>) -> AgentStreamQuery {
        stream_query_for(STREAM_ID, expected_revision)
    }

    fn stream_query_for(stream_id: &str, expected_revision: Option<String>) -> AgentStreamQuery {
        AgentStreamQuery {
            session_id: SESSION_ID.to_owned(),
            stream_id: stream_id.to_owned(),
            expected_revision,
        }
    }

    #[test]
    fn staged_stream_loser_aborts_without_consuming_the_winning_target_claim() {
        const LOSER_STREAM_ID: &str = "stream:controller:loser";
        let mut persistence = EphemeralAgentPersistence::default();
        let initial = AgentStreamController::load(&mut persistence, &stream_query(None)).unwrap();
        let target = AgentStreamTarget::Message {
            message_id: "message:shared-staged-target".to_owned(),
            role: MessageRole::Agent,
        };
        let winner = AgentStreamController::open(
            &mut persistence,
            &initial.revision,
            SESSION_ID,
            STREAM_ID,
            target.clone(),
            AgentStreamDelivery::Staged,
        )
        .unwrap();
        let loser = AgentStreamController::open(
            &mut persistence,
            &winner.observed_revision,
            SESSION_ID,
            LOSER_STREAM_ID,
            target,
            AgentStreamDelivery::Staged,
        )
        .unwrap();
        let appended = AgentStreamController::append(
            &mut persistence,
            &loser.observed_revision,
            SESSION_ID,
            STREAM_ID,
            AgentStreamChunk {
                sequence: 0,
                content: vec![ContentBlock::Text {
                    text: "winner".to_owned(),
                }],
            },
        )
        .unwrap();
        let finalized = AgentStreamController::finalize(
            &mut persistence,
            &appended.observed_revision,
            SESSION_ID,
            STREAM_ID,
        )
        .unwrap();
        let AgentStreamFinalizeOutcome::Committed { commit } = finalized else {
            panic!("staged winner commits directly")
        };
        let aborted = AgentStreamController::abort(
            &mut persistence,
            &commit.observed_revision,
            SESSION_ID,
            LOSER_STREAM_ID,
            "caller:loser",
        )
        .expect("unreserved loser aborts without consuming the winner claim");
        let loser = AgentStreamController::load(
            &mut persistence,
            &stream_query_for(LOSER_STREAM_ID, Some(aborted.observed_revision)),
        )
        .unwrap()
        .current
        .expect("loser stream remains queryable");
        assert_eq!(loser.state, AgentStreamState::Aborted);
    }

    #[test]
    fn closed_stream_commands_replay_only_from_their_exact_source_revision() {
        let mut persistence = EphemeralAgentPersistence::default();
        let initial = AgentStreamController::load(&mut persistence, &stream_query(None))
            .expect("initial exact stream read succeeds");
        assert!(initial.current.is_none());

        let target = AgentStreamTarget::Message {
            message_id: "message:controller".to_owned(),
            role: MessageRole::Agent,
        };
        let opened = AgentStreamController::open(
            &mut persistence,
            &initial.revision,
            SESSION_ID,
            STREAM_ID,
            target.clone(),
            AgentStreamDelivery::Staged,
        )
        .expect("stream opens");
        let replay = AgentStreamController::open(
            &mut persistence,
            &initial.revision,
            SESSION_ID,
            STREAM_ID,
            target,
            AgentStreamDelivery::Staged,
        )
        .expect("identical command replays its retained receipt");
        assert_eq!(replay.receipt, opened.receipt);
        assert_eq!(replay.observed_revision, opened.observed_revision);
        assert_eq!(
            opened.committed_revision.as_deref(),
            Some(opened.observed_revision.as_str())
        );
        assert!(replay.committed_revision.is_none());

        let chunk = AgentStreamChunk {
            sequence: 0,
            content: vec![ContentBlock::Text {
                text: "complete output".to_owned(),
            }],
        };
        let stale = AgentStreamController::append(
            &mut persistence,
            &initial.revision,
            SESSION_ID,
            STREAM_ID,
            chunk.clone(),
        );
        assert!(matches!(stale, Err(AgentError::Persistence { .. })));

        let appended = AgentStreamController::append(
            &mut persistence,
            &opened.observed_revision,
            SESSION_ID,
            STREAM_ID,
            chunk,
        )
        .expect("next chunk commits from the open revision");
        let finalized = AgentStreamController::finalize(
            &mut persistence,
            &appended.observed_revision,
            SESSION_ID,
            STREAM_ID,
        )
        .expect("inline stream finalizes");
        let AgentStreamFinalizeOutcome::Committed { commit: finalized } = finalized else {
            panic!("staged stream finalization must commit directly")
        };
        let finalized = *finalized;
        let AgentCommandOutcome::Stream(postcondition) = &finalized.receipt.outcome else {
            panic!("stream command returned a non-stream outcome");
        };
        assert_eq!(postcondition.stream.state, AgentStreamState::Finalized);

        let retained = AgentStreamController::load(
            &mut persistence,
            &stream_query(Some(finalized.observed_revision.clone())),
        )
        .expect("final revision-pinned stream read succeeds");
        assert_eq!(retained.current, Some(postcondition.stream.clone()));
    }

    #[test]
    fn ephemeral_external_finalize_fails_without_advancing_or_mutating_the_stream() {
        let mut persistence = EphemeralAgentPersistence::default();
        let initial = AgentStreamController::load(&mut persistence, &stream_query(None))
            .expect("initial exact stream read succeeds");
        let opened = AgentStreamController::open(
            &mut persistence,
            &initial.revision,
            SESSION_ID,
            STREAM_ID,
            AgentStreamTarget::Message {
                message_id: "message:external-controller".to_owned(),
                role: MessageRole::Agent,
            },
            AgentStreamDelivery::ExternalResource {
                resolver_binding: "resolver:external-controller/1".to_owned(),
                content: AgentStreamPublicationContent {
                    media_type: "application/octet-stream".to_owned(),
                    digest: format!("sha256:{}", "a".repeat(64)),
                    size: 1,
                },
            },
        )
        .expect("external stream intent opens without provider I/O");
        let before = AgentStreamController::load(
            &mut persistence,
            &stream_query(Some(opened.observed_revision.clone())),
        )
        .expect("opened stream read is pinned");

        let error = AgentStreamController::finalize(
            &mut persistence,
            &opened.observed_revision,
            SESSION_ID,
            STREAM_ID,
        )
        .expect_err("ephemeral persistence has no external Resource authority");
        assert!(matches!(error, AgentError::Validation(_)));

        let after = AgentStreamController::load(&mut persistence, &stream_query(None))
            .expect("stream remains readable after rejected finalization");
        assert_eq!(after, before);
    }
}
