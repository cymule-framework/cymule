//! Content-addressed persistent roots for the durable projection.
//!
//! This module deliberately separates the physical projection shape from the
//! semantic transition. The coordinator remains the sole authority that admits
//! a Core command batch and its closed profile sidecars. The pinned bridge
//! resolves only the command-shaped read set, stages every touched Machine
//! root in one unpublished overlay, and this layer combines that stage with an
//! optional [`crate::DurableDelta`] into one successor. Applying it copies only
//! changed immutable map and log paths and produces one fixed-shape
//! [`StateRootTransition`] with its [`StateRootManifest`].

use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[cfg(test)]
use cymule_authenticated_collections::map_key_hash;
use cymule_authenticated_collections::{
    CollectionError, CollectionResolver, LogMutation, LogNode, LogRoot, MAX_LOG_VALUES_PER_APPLY,
    MapMutation, MapNode, MapPosition, MapRoot, apply_log_mutations, apply_map_mutations,
    audit_log, audit_map, decode_log_node, decode_map_node, prove_log_exact, prove_map_exact,
    prove_map_range, split_log, verify_log_exact, verify_map_exact, verify_map_range,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{DurableError, DurableResult, MAX_EXACT_INTEGER};

#[path = "pinned_machine.rs"]
pub(crate) mod pinned_machine;
#[path = "pinned_wait.rs"]
pub(crate) mod pinned_wait;

/// Fixed-shape durable root manifest schema.
pub const STATE_ROOT_MANIFEST_VERSION: &str = "cymule.durable-state-root/5";
/// Immutable canonical JSON value object schema.
pub const STATE_ROOT_VALUE_VERSION: &str = "cymule.durable-state-value/5";
/// Domain-separated revision chain over exact persistent roots.
pub const DURABLE_REVISION_VERSION: &str = "cymule.durable-revision/3";
pub const RUN_QUERY_INDEXES_VERSION: &str = "cymule.run-query-indexes/3";
pub const PENDING_WAIT_SOURCE_VERSION: &str = "cymule.pending-wait-source/1";
/// Maximum canonical byte size of one root manifest.
pub const MAX_STATE_ROOT_MANIFEST_BYTES: usize = 32 * 1024;
/// Maximum canonical bytes of one immutable state-root object, independently
/// of the decoded typed-leaf and Machine-base chunk bounds.
pub const MAX_STATE_ROOT_OBJECT_BYTES: usize = 64 * 1024 * 1024;
/// Maximum canonical JSON bytes carried by one typed state-root leaf.
pub const MAX_STATE_ROOT_LEAF_BYTES: usize = 12 * 1024 * 1024;
/// Maximum raw canonical Machine-base bytes in one immutable chunk value.
pub const MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub(crate) const PINNED_MACHINE_SIDECAR_TRANSITION_DOMAIN: &str =
    "cymule.pinned-machine-sidecar-transition/1";

/// One closed state-root value.
///
/// Nested references are derived from the typed map/log descriptor variants;
/// callers cannot pair arbitrary JSON with an incomplete or spurious reference
/// set and thereby hide reachable objects from GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateRootLeafKind {
    /// Sealed Plan.
    MachinePlan,
    /// Immutable Artifact.
    MachineArtifact,
    /// Hot Machine Event.
    MachineEvent,
    /// Hot Machine command admission.
    MachineAdmission,
    /// Hot atomic Machine command-batch authority.
    MachineCommandBatch,
    /// One exact Machine Effect current.
    MachineEffect,
    /// One exact Machine obligation current.
    MachineObligation,
    /// One exact Machine Attempt current.
    MachineAttempt,
    /// One immutable Machine fact value.
    MachineFact,
    /// Durable Continuation.
    Continuation,
    /// Bounded semantic Run current used by ordinary queries.
    RunCurrent,
    /// Durable wait.
    Wait,
    /// Bounded Run-query summary derived from one complete durable wait.
    WaitSummary,
    /// Identified wait activation.
    WaitActivation,
    /// Exact immutable Run cancellation receipt.
    CancellationReceipt,
    /// Exact immutable terminal Effect-resolution receipt.
    EffectResolutionReceipt,
    /// Coordination lease.
    Lease,
    /// Effect outbox entry.
    Outbox,
    /// Immutable Effect-to-Run lookup; mutable dispatches live under their Run.
    OutboxOwner,
    /// Component occurrence.
    ComponentOccurrence,
    /// Provider operation Attempt.
    OperationAttempt,
    /// Logical Clock observation.
    ClockObservation,
    /// Portable snapshot record.
    Snapshot,
    /// Machine-history compaction receipt.
    HistoryCompaction,
    /// Application-journal record.
    JournalRecord,
    /// Latest application-journal prefix-replacement receipt.
    JournalPrefixReplacement,
    /// Payload-free all-ever journal-record manifest.
    JournalRecordManifest,
    /// Cumulative journal prefix-replacement authority.
    JournalPrefixReplacementAuthority,
    /// Complete coupled-checkpoint receipt.
    CoupledCheckpointReceipt,
    /// Exact closed Resource command receipt.
    ResourceCommandReceipt,
    /// Current physical Resource-retention projection.
    ResourceRetentionCurrent,
    /// Current exact Resource-pin projection.
    ResourcePinCurrent,
    /// Current exact Resource-deletion projection.
    ResourceDeleteCurrent,
    /// Current immutable Resource-handoff authority.
    ResourceHandoffCurrent,
    /// Current immutable Resource-handoff activation authority.
    ResourceHandoffActivationCurrent,
    /// One position-bound Resource target-index entry.
    ResourceHandoffIndex,
    /// One position-bound Resource activation-index entry.
    ResourceHandoffActivationIndex,
    /// Exact closed Agent persistence command.
    AgentCommand,
    /// Exact closed Agent persistence receipt.
    AgentCommandReceipt,
    /// Durable-private receipt for an Agent input suspension.
    AgentInputSuspensionReceipt,
    /// Durable-private receipt for an Agent input completion.
    AgentInputCompletionReceipt,
    /// Bounded keyed Agent Session metadata.
    AgentSessionCurrent,
    /// Agent Session-local update idempotency authority.
    AgentUpdateCurrent,
    /// Immutable keyed Agent message and order entry.
    AgentMessageCurrent,
    /// Keyed Agent tool-call projection.
    AgentToolCurrent,
    /// Exact generation-bearing Agent target claim.
    AgentTargetClaimCurrent,
    /// Keyed Agent elicitation projection.
    AgentElicitationCurrent,
    /// Keyed Agent host-occurrence projection.
    AgentOccurrenceCurrent,
    /// Keyed Agent stream projection.
    AgentStreamCurrent,
    /// Immutable keyed Agent stream chunk.
    AgentStreamChunkCurrent,
    /// Bounded scalar Evolution partition current.
    EvolutionCurrent,
    /// Exact all-ever Evolution command alias.
    EvolutionCommandAlias,
    /// Exact all-ever Evolution persistence receipt.
    EvolutionPersistenceReceipt,
    /// One normalized typed Evolution state leaf.
    EvolutionMutation,
    /// Exact bounded Virtual scheduler current.
    VirtualCurrent,
    /// Exact all-ever Virtual persistence receipt.
    VirtualPersistenceReceipt,
    /// One normalized typed Virtual state leaf.
    VirtualStateLeaf,
    /// Immutable cross-profile Resource catalog record.
    ResourceCatalogRecord,
}

/// Closed value object stored beneath a persistent map or log node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateRootValue {
    /// One canonical typed leaf with no nested state-root references.
    Leaf {
        /// Closed semantic leaf kind.
        kind: StateRootLeafKind,
        /// Exact canonical UTF-8 JSON text for that kind.
        canonical_json: String,
    },
    /// Composite hot-command authority addressable by exact command identity.
    ///
    /// The admission and archived-index non-membership proof live beside the
    /// private command record so ordinary replay never scans an admission log
    /// or joins independently mutable aliases.
    MachineCommandCurrent {
        /// Exact private command record.
        record: Box<cymule_core::ArchivedCommandRecord>,
        /// Exact ordered admission for `record`.
        admission: Box<cymule_core::CommandAdmission>,
        /// Exact current archive-index non-membership proof retained at hot
        /// admission time.
        index_proof: Box<cymule_core::MachineCommandIndexProof>,
        /// Global one-based position of the first Event in this command's
        /// contiguous Event range; absent exactly for Event-free conflicts.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        first_event_position: Option<u64>,
    },
    /// Scalar Run authority plus every nested persistent collection root.
    MachineRunCurrent {
        /// Exact Core-owned Run current.
        current: Box<cymule_core::durable_internal::MachineRunCurrent>,
    },
    /// Scalar Scope authority, its nested roots, and the exact bounded lexical
    /// witness whose digests are pinned by `current`.
    MachineScopeCurrent {
        /// Exact Core-owned Scope current.
        current: Box<cymule_core::durable_internal::MachineScopeCurrent>,
        /// Exact entry-rooted invocation path.
        invocation_path: Vec<cymule_core::InvocationPathSegment>,
        /// Exact lexical Region path.
        region_path: Vec<usize>,
    },
    /// Persisted global reservation for one in-progress paged command.
    MachinePendingCommand {
        /// Exact command identity and map key.
        command_id: String,
        /// Exact paged-transition identity.
        transition_id: String,
    },
    /// O(1) paged-transition authority plus every source and shadow root.
    MachinePagedTransitionCurrent {
        /// Exact Core-owned transition current.
        current: Box<cymule_core::durable_internal::MachinePagedTransitionCurrent>,
    },
    /// Typed reducer-index membership value. Its object identity deliberately
    /// uses Core's membership domain because that identity is the map value
    /// authenticated by the reducer root.
    MachineIndexMembership {
        /// Exact owning Run.
        run_id: String,
        /// Exact reducer index.
        selector: cymule_core::durable_internal::MachineRunIndexSelector,
        /// Exact member identity and map key.
        entry: String,
    },
    /// Typed proposal-order entry. Its object identity deliberately uses
    /// Core's order-entry domain because that identity is authenticated by the
    /// persistent log.
    MachineOrderEntry {
        /// Exact owning Run.
        run_id: String,
        /// Exact proposal-order log.
        selector: cymule_core::durable_internal::MachineRunLogSelector,
        /// Exact ordered semantic identity.
        entry: String,
    },
    /// One indexed canonical byte chunk of a compacted Machine base.
    MachineBaseChunk {
        /// Zero-based chunk index.
        index: u64,
        /// Exact canonical byte slice, encoded as strict padded Base64 on wire.
        #[serde(with = "machine_base_chunk_base64")]
        bytes: Vec<u8>,
    },
    /// Closed descriptor of a chunked compacted Machine base.
    MachineBaseDescriptor {
        /// Exact canonical byte length.
        canonical_len: u64,
        /// SHA-256 digest of the concatenated canonical bytes.
        canonical_digest: String,
        /// Exact number of indexed chunks.
        chunk_count: u64,
        /// Ordered indexed chunk log.
        chunks: LogRoot,
    },
    /// Owner-bound application-journal log descriptor.
    ApplicationJournal {
        /// Exact journal owner.
        journal_id: String,
        /// Exact persistent log root.
        root: LogRoot,
    },
    /// Owner-bound all-ever record-manifest map descriptor.
    ApplicationJournalRecordManifests {
        /// Exact journal owner.
        journal_id: String,
        /// Exact persistent map root.
        root: MapRoot,
    },
    /// Target-bound ordered Resource-handoff index descriptor.
    ResourceHandoffIndex {
        /// Exact target Run owning the index.
        to_run: String,
        /// Exact persistent log root.
        root: LogRoot,
    },
    /// Target-bound ordered Resource-handoff activation-index descriptor.
    ResourceHandoffActivationIndex {
        /// Exact target Run owning the index.
        to_run: String,
        /// Exact persistent log root.
        root: LogRoot,
    },
    /// Target-bound exact slot-to-handoff map descriptor.
    ResourceHandoffSlots {
        /// Exact target Run owning the slot map.
        to_run: String,
        /// Exact persistent map root.
        root: MapRoot,
    },
    /// Session-bound ordered Agent message log descriptor.
    AgentMessageIndex {
        /// Exact owning Session.
        session_id: String,
        /// Exact persistent log root.
        root: LogRoot,
    },
    /// Session-bound deletable unresolved-occurrence index descriptor.
    AgentUnresolvedOccurrenceIndex {
        /// Exact owning Session.
        session_id: String,
        /// Exact persistent log root.
        root: LogRoot,
    },
    /// Session-bound deletable open-stream index descriptor.
    AgentOpenStreamIndex {
        /// Exact owning Session.
        session_id: String,
        /// Exact persistent log root.
        root: LogRoot,
    },
    /// Run-bound exact current-index roots used by ordinary bounded queries.
    RunQueryIndexes {
        /// Closed descriptor generation.
        index_version: String,
        /// Exact owning Run.
        run_id: String,
        /// Current waits keyed by wait identity.
        waits: MapRoot,
        /// Current Effect dispatches keyed by intent identity.
        effects: MapRoot,
        /// Current component occurrences keyed by occurrence identity.
        occurrences: MapRoot,
        /// Current provider Attempts keyed by Attempt identity.
        attempts: MapRoot,
        /// Pending Wait records only.
        pending_waits: MapRoot,
        /// Unsettled Effect outbox records only.
        active_effects: MapRoot,
        /// Currently claimed Effect coordination leases only.
        active_leases: MapRoot,
        /// Hidden Effect roots bound to the Run's exact Core terminal transition.
        #[serde(deserialize_with = "deserialize_required_nullable")]
        terminal: Option<Box<RunTerminalSidecarCurrent>>,
    },
    /// Signal- or timer-owned exact pending-Wait membership root.
    PendingWaitSource {
        /// Closed descriptor generation.
        source_version: String,
        /// Exact typed signal or timer source.
        source: crate::WaitActivationSource,
        /// Pending Wait records keyed by wait identity.
        waits: MapRoot,
    },
}

/// Durable-private shadow roots for one Core-owned Run terminal transition.
///
/// The Core transition digest, not a second cursor, identifies the exact page
/// whose Effect changes these roots contain. Source digests cover only this
/// Run's retained authority so unrelated Runs remain independently writable.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunTerminalSidecarCurrent {
    /// Exact Core-owned transition identity.
    pub transition_id: String,
    /// Canonical digest of the current Core page authority.
    pub transition_digest: String,
    /// Canonical digest of the unchanged source Continuation.
    pub source_continuation_digest: String,
    /// Canonical digest of the unchanged Run query roots without this companion.
    pub source_query_digest: String,
    /// Hidden complete Run-owned Effect dispatch map.
    pub effects: MapRoot,
    /// Hidden unsettled Effect dispatch membership.
    pub active_effects: MapRoot,
    /// Hidden currently claimed Effect lease membership.
    pub active_leases: MapRoot,
}

impl RunTerminalSidecarCurrent {
    fn verify(&self) -> DurableResult<()> {
        cymule_core::validate_content_id("terminal Core transition", &self.transition_id)?;
        for digest in [
            &self.transition_digest,
            &self.source_continuation_digest,
            &self.source_query_digest,
        ] {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(DurableError::Validation(
                    "terminal sidecar source is not an exact canonical digest".to_owned(),
                ));
            }
        }
        self.effects.verify()?;
        self.active_effects.verify()?;
        self.active_leases.verify()?;
        if self.active_effects.entries > self.effects.entries
            || self.active_leases.entries > self.active_effects.entries
        {
            return Err(DurableError::Integrity {
                code: "terminal_sidecar_membership_count_mismatch".to_owned(),
                message: "terminal shadow indexes exceed their owning Effect set".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxOwner {
    intent_id: String,
    run_id: String,
}

impl OutboxOwner {
    fn verify(&self) -> DurableResult<()> {
        cymule_core::validate_content_id("outbox owner intent", &self.intent_id)?;
        cymule_core::validate_identity("outbox owner Run", &self.run_id)?;
        Ok(())
    }
}

impl StateRootValue {
    fn encode<T: Serialize>(kind: StateRootLeafKind, value: &T) -> DurableResult<Self> {
        let value = Self::Leaf {
            kind,
            canonical_json: String::from_utf8(cymule_core::canonical_bytes(value)?)
                .map_err(|error| DurableError::Encoding(error.to_string()))?,
        };
        value.verify()?;
        Ok(value)
    }

    fn machine_command_current(
        record: cymule_core::ArchivedCommandRecord,
        admission: cymule_core::CommandAdmission,
        index_proof: cymule_core::MachineCommandIndexProof,
        first_event_position: Option<u64>,
    ) -> DurableResult<Self> {
        let value = Self::MachineCommandCurrent {
            record: Box::new(record),
            admission: Box::new(admission),
            index_proof: Box::new(index_proof),
            first_event_position,
        };
        value.verify()?;
        Ok(value)
    }

    fn machine_run_current(
        current: cymule_core::durable_internal::MachineRunCurrent,
    ) -> DurableResult<Self> {
        let value = Self::MachineRunCurrent {
            current: Box::new(current),
        };
        value.verify()?;
        Ok(value)
    }

    fn machine_scope_current(
        current: cymule_core::durable_internal::MachineScopeCurrent,
        invocation_path: Vec<cymule_core::InvocationPathSegment>,
        region_path: Vec<usize>,
    ) -> DurableResult<Self> {
        let value = Self::MachineScopeCurrent {
            current: Box::new(current),
            invocation_path,
            region_path,
        };
        value.verify()?;
        Ok(value)
    }

    fn machine_pending_command(command_id: String, transition_id: String) -> DurableResult<Self> {
        let value = Self::MachinePendingCommand {
            command_id,
            transition_id,
        };
        value.verify()?;
        Ok(value)
    }

    fn machine_paged_transition_current(
        current: cymule_core::durable_internal::MachinePagedTransitionCurrent,
    ) -> DurableResult<Self> {
        let value = Self::MachinePagedTransitionCurrent {
            current: Box::new(current),
        };
        value.verify()?;
        Ok(value)
    }

    fn machine_index_membership(
        run_id: String,
        selector: cymule_core::durable_internal::MachineRunIndexSelector,
        entry: String,
    ) -> DurableResult<Self> {
        let value = Self::MachineIndexMembership {
            run_id,
            selector,
            entry,
        };
        value.verify()?;
        Ok(value)
    }

    fn machine_order_entry(
        run_id: String,
        selector: cymule_core::durable_internal::MachineRunLogSelector,
        entry: String,
    ) -> DurableResult<Self> {
        let value = Self::MachineOrderEntry {
            run_id,
            selector,
            entry,
        };
        value.verify()?;
        Ok(value)
    }

    fn application_journal(journal_id: &str, root: &LogRoot) -> DurableResult<Self> {
        if journal_id.is_empty() {
            return Err(DurableError::Validation(
                "application-journal root has an empty owner".to_owned(),
            ));
        }
        root.verify()?;
        Ok(Self::ApplicationJournal {
            journal_id: journal_id.to_owned(),
            root: root.clone(),
        })
    }

    fn application_journal_record_manifests(
        journal_id: &str,
        root: &MapRoot,
    ) -> DurableResult<Self> {
        if journal_id.is_empty() {
            return Err(DurableError::Validation(
                "application-journal record-manifest root has an empty owner".to_owned(),
            ));
        }
        root.verify()?;
        Ok(Self::ApplicationJournalRecordManifests {
            journal_id: journal_id.to_owned(),
            root: root.clone(),
        })
    }

    fn resource_handoff_index(to_run: &str, root: &LogRoot) -> DurableResult<Self> {
        validate_resource_target(to_run)?;
        root.verify()?;
        Ok(Self::ResourceHandoffIndex {
            to_run: to_run.to_owned(),
            root: root.clone(),
        })
    }

    fn resource_handoff_activation_index(to_run: &str, root: &LogRoot) -> DurableResult<Self> {
        validate_resource_target(to_run)?;
        root.verify()?;
        Ok(Self::ResourceHandoffActivationIndex {
            to_run: to_run.to_owned(),
            root: root.clone(),
        })
    }

    fn resource_handoff_slots(to_run: &str, root: &MapRoot) -> DurableResult<Self> {
        validate_resource_target(to_run)?;
        root.verify()?;
        Ok(Self::ResourceHandoffSlots {
            to_run: to_run.to_owned(),
            root: root.clone(),
        })
    }

    fn agent_message_index(session_id: &str, root: &LogRoot) -> DurableResult<Self> {
        validate_agent_session(session_id)?;
        root.verify()?;
        Ok(Self::AgentMessageIndex {
            session_id: session_id.to_owned(),
            root: root.clone(),
        })
    }

    fn agent_unresolved_occurrence_index(session_id: &str, root: &LogRoot) -> DurableResult<Self> {
        validate_agent_session(session_id)?;
        root.verify()?;
        Ok(Self::AgentUnresolvedOccurrenceIndex {
            session_id: session_id.to_owned(),
            root: root.clone(),
        })
    }

    fn agent_open_stream_index(session_id: &str, root: &LogRoot) -> DurableResult<Self> {
        validate_agent_session(session_id)?;
        root.verify()?;
        Ok(Self::AgentOpenStreamIndex {
            session_id: session_id.to_owned(),
            root: root.clone(),
        })
    }

    fn run_query_indexes(run_id: &str, roots: RunQueryIndexRoots) -> DurableResult<Self> {
        cymule_core::validate_identity("Run query-index owner", run_id)?;
        roots.verify()?;
        let RunQueryIndexRoots {
            waits,
            effects,
            occurrences,
            attempts,
            pending_waits,
            active_effects,
            active_leases,
            terminal,
        } = roots;
        Ok(Self::RunQueryIndexes {
            index_version: RUN_QUERY_INDEXES_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            waits,
            effects,
            occurrences,
            attempts,
            pending_waits,
            active_effects,
            active_leases,
            terminal,
        })
    }

    fn pending_wait_source(
        source: crate::WaitActivationSource,
        waits: MapRoot,
    ) -> DurableResult<Self> {
        source.verify()?;
        waits.verify()?;
        if waits.entries == 0 {
            return Err(DurableError::Validation(
                "pending-Wait source descriptor cannot retain an empty map".to_owned(),
            ));
        }
        Ok(Self::PendingWaitSource {
            source_version: PENDING_WAIT_SOURCE_VERSION.to_owned(),
            source,
            waits,
        })
    }

    /// Decode a typed value and require exact canonical round-trip equality.
    fn decode<T>(&self, expected_kind: StateRootLeafKind) -> DurableResult<T>
    where
        T: DeserializeOwned + Serialize,
    {
        self.verify()?;
        let Self::Leaf {
            kind,
            canonical_json,
        } = self
        else {
            return Err(DurableError::Validation(
                "nested state-root descriptor is not an ordinary JSON value".to_owned(),
            ));
        };
        if *kind != expected_kind {
            return Err(DurableError::Integrity {
                code: "state_root_leaf_kind_mismatch".to_owned(),
                message: format!("state-root leaf kind {kind:?} does not match {expected_kind:?}"),
            });
        }
        let canonical_bytes = canonical_json.as_bytes();
        let decoded = cymule_core::decode_json::<T>(canonical_bytes)?;
        if cymule_core::canonical_bytes(&decoded)? != canonical_bytes {
            return Err(DurableError::Integrity {
                code: "state_root_value_typed_roundtrip_mismatch".to_owned(),
                message: "state-root value changes under its typed canonical round trip".to_owned(),
            });
        }
        Ok(decoded)
    }

    /// Decode and verify a nested persistent-map root descriptor.
    fn decode_record_manifest_root(&self, owner: &str) -> DurableResult<MapRoot> {
        let Self::ApplicationJournalRecordManifests { journal_id, root } = self else {
            return Err(DurableError::Integrity {
                code: "state_root_nested_map_value_kind_mismatch".to_owned(),
                message: "persistent-map descriptor was not stored as a typed nested root"
                    .to_owned(),
            });
        };
        if journal_id != owner {
            return Err(DurableError::Integrity {
                code: "state_root_nested_map_owner_mismatch".to_owned(),
                message: "application-journal record-manifest root changed owner".to_owned(),
            });
        }
        root.verify()?;
        Ok(root.clone())
    }

    /// Decode and verify a nested persistent-log root descriptor.
    fn decode_application_journal_root(&self, owner: &str) -> DurableResult<LogRoot> {
        let Self::ApplicationJournal { journal_id, root } = self else {
            return Err(DurableError::Integrity {
                code: "state_root_nested_log_value_kind_mismatch".to_owned(),
                message: "persistent-log descriptor was not stored as a typed nested root"
                    .to_owned(),
            });
        };
        if journal_id != owner {
            return Err(DurableError::Integrity {
                code: "state_root_nested_log_owner_mismatch".to_owned(),
                message: "application-journal root changed owner".to_owned(),
            });
        }
        root.verify()?;
        Ok(root.clone())
    }

    fn decode_resource_handoff_index_root(&self, to_run: &str) -> DurableResult<LogRoot> {
        decode_resource_index_root(self, to_run, false)
    }

    fn decode_resource_handoff_activation_index_root(
        &self,
        to_run: &str,
    ) -> DurableResult<LogRoot> {
        decode_resource_index_root(self, to_run, true)
    }

    fn decode_resource_handoff_slots_root(&self, to_run: &str) -> DurableResult<MapRoot> {
        let Self::ResourceHandoffSlots {
            to_run: owner,
            root,
        } = self
        else {
            return Err(DurableError::Integrity {
                code: "state_root_resource_slot_map_value_kind_mismatch".to_owned(),
                message: "Resource slot-map descriptor has the wrong closed kind".to_owned(),
            });
        };
        if owner != to_run {
            return Err(DurableError::Integrity {
                code: "state_root_resource_slot_map_owner_mismatch".to_owned(),
                message: "Resource slot map changed its owning Run".to_owned(),
            });
        }
        root.verify()?;
        Ok(root.clone())
    }

    fn decode_agent_message_index_root(&self, session_id: &str) -> DurableResult<LogRoot> {
        decode_agent_index_root(self, session_id, AgentIndexKind::Messages)
    }

    fn decode_agent_unresolved_occurrence_index_root(
        &self,
        session_id: &str,
    ) -> DurableResult<LogRoot> {
        decode_agent_index_root(self, session_id, AgentIndexKind::UnresolvedOccurrences)
    }

    fn decode_agent_open_stream_index_root(&self, session_id: &str) -> DurableResult<LogRoot> {
        decode_agent_index_root(self, session_id, AgentIndexKind::OpenStreams)
    }

    fn decode_run_query_indexes(&self, run_id: &str) -> DurableResult<RunQueryIndexRoots> {
        self.verify()?;
        let Self::RunQueryIndexes {
            index_version,
            run_id: owner,
            waits,
            effects,
            occurrences,
            attempts,
            pending_waits,
            active_effects,
            active_leases,
            terminal,
        } = self
        else {
            return Err(DurableError::Integrity {
                code: "state_root_run_query_indexes_value_kind_mismatch".to_owned(),
                message: "Run query-index descriptor has the wrong closed kind".to_owned(),
            });
        };
        if index_version != RUN_QUERY_INDEXES_VERSION || owner != run_id {
            return Err(DurableError::Integrity {
                code: "state_root_run_query_indexes_owner_mismatch".to_owned(),
                message: "Run query-index descriptor changed its owning Run".to_owned(),
            });
        }
        let roots = RunQueryIndexRoots {
            waits: waits.clone(),
            effects: effects.clone(),
            occurrences: occurrences.clone(),
            attempts: attempts.clone(),
            pending_waits: pending_waits.clone(),
            active_effects: active_effects.clone(),
            active_leases: active_leases.clone(),
            terminal: terminal.clone(),
        };
        roots.verify()?;
        Ok(roots)
    }

    fn decode_pending_wait_source(
        &self,
        expected: &crate::WaitActivationSource,
    ) -> DurableResult<MapRoot> {
        self.verify()?;
        let Self::PendingWaitSource {
            source_version,
            source,
            waits,
        } = self
        else {
            return Err(DurableError::Integrity {
                code: "state_root_pending_wait_source_value_kind_mismatch".to_owned(),
                message: "pending-Wait source has the wrong closed descriptor kind".to_owned(),
            });
        };
        if source_version != PENDING_WAIT_SOURCE_VERSION || source != expected {
            return Err(DurableError::Integrity {
                code: "state_root_pending_wait_source_owner_mismatch".to_owned(),
                message: "pending-Wait source descriptor changed source identity".to_owned(),
            });
        }
        Ok(waits.clone())
    }

    /// Borrow the canonical JSON bytes.
    pub fn canonical_json(&self) -> Option<&[u8]> {
        match self {
            Self::Leaf { canonical_json, .. } => Some(canonical_json.as_bytes()),
            Self::MachineCommandCurrent { .. }
            | Self::MachineRunCurrent { .. }
            | Self::MachineScopeCurrent { .. }
            | Self::MachinePendingCommand { .. }
            | Self::MachinePagedTransitionCurrent { .. }
            | Self::MachineIndexMembership { .. }
            | Self::MachineOrderEntry { .. }
            | Self::MachineBaseChunk { .. }
            | Self::MachineBaseDescriptor { .. }
            | Self::ApplicationJournal { .. }
            | Self::ApplicationJournalRecordManifests { .. }
            | Self::ResourceHandoffIndex { .. }
            | Self::ResourceHandoffActivationIndex { .. }
            | Self::ResourceHandoffSlots { .. }
            | Self::AgentMessageIndex { .. }
            | Self::AgentUnresolvedOccurrenceIndex { .. }
            | Self::AgentOpenStreamIndex { .. }
            | Self::RunQueryIndexes { .. }
            | Self::PendingWaitSource { .. } => None,
        }
    }

    /// Verify canonical bytes and reference identities.
    ///
    /// # Errors
    ///
    /// Rejects invalid canonical values, nested root identities, or size bounds.
    pub fn verify(&self) -> DurableResult<()> {
        match self {
            Self::Leaf {
                kind,
                canonical_json,
            } => {
                if canonical_json.len() > MAX_STATE_ROOT_LEAF_BYTES {
                    return Err(DurableError::Validation(format!(
                        "state-root leaf exceeds {MAX_STATE_ROOT_LEAF_BYTES} canonical bytes"
                    )));
                }
                verify_leaf_bytes(*kind, canonical_json.as_bytes())?;
            }
            Self::MachineCommandCurrent { .. }
            | Self::MachineRunCurrent { .. }
            | Self::MachineScopeCurrent { .. }
            | Self::MachinePendingCommand { .. }
            | Self::MachinePagedTransitionCurrent { .. }
            | Self::MachineIndexMembership { .. }
            | Self::MachineOrderEntry { .. }
            | Self::MachineBaseChunk { .. }
            | Self::MachineBaseDescriptor { .. } => self.verify_machine_value()?,
            Self::ApplicationJournal { .. }
            | Self::ApplicationJournalRecordManifests { .. }
            | Self::ResourceHandoffIndex { .. }
            | Self::ResourceHandoffActivationIndex { .. }
            | Self::ResourceHandoffSlots { .. }
            | Self::AgentMessageIndex { .. }
            | Self::AgentUnresolvedOccurrenceIndex { .. }
            | Self::AgentOpenStreamIndex { .. }
            | Self::RunQueryIndexes { .. }
            | Self::PendingWaitSource { .. } => self.verify_collection_descriptor()?,
        }
        Ok(())
    }

    fn verify_machine_value(&self) -> DurableResult<()> {
        match self {
            Self::MachineCommandCurrent {
                record,
                admission,
                index_proof,
                first_event_position,
            } => {
                verify_machine_command_current(
                    record,
                    admission,
                    index_proof,
                    *first_event_position,
                )?;
                verify_nested_state_value_bound(self, "Machine hot command")?;
            }
            Self::MachineRunCurrent { current } => {
                current.verify()?;
                verify_nested_state_value_bound(self, "Machine Run current")?;
            }
            Self::MachineScopeCurrent {
                current,
                invocation_path,
                region_path,
            } => {
                verify_machine_scope_witness(current, invocation_path, region_path)?;
                verify_nested_state_value_bound(self, "Machine Scope current")?;
            }
            Self::MachinePendingCommand {
                command_id,
                transition_id,
            } => {
                cymule_core::validate_identity("Machine pending command", command_id)?;
                cymule_core::validate_content_id("Machine pending transition", transition_id)?;
                verify_nested_state_value_bound(self, "Machine pending command")?;
            }
            Self::MachinePagedTransitionCurrent { current } => {
                current.verify()?;
                verify_nested_state_value_bound(self, "Machine paged transition")?;
            }
            Self::MachineIndexMembership {
                run_id,
                selector,
                entry,
            } => {
                cymule_core::validate_identity("Machine index owner Run", run_id)?;
                verify_machine_index_entry(selector, entry)?;
                verify_nested_state_value_bound(self, "Machine index membership")?;
            }
            Self::MachineOrderEntry {
                run_id,
                selector,
                entry,
            } => {
                cymule_core::validate_identity("Machine log owner Run", run_id)?;
                verify_machine_order_entry(selector, entry)?;
                verify_nested_state_value_bound(self, "Machine order entry")?;
            }
            Self::MachineBaseChunk { index, bytes } => {
                verify_machine_base_chunk(*index, bytes)?;
            }
            Self::MachineBaseDescriptor {
                canonical_len,
                canonical_digest,
                chunk_count,
                chunks,
            } => {
                verify_machine_base_descriptor(
                    *canonical_len,
                    canonical_digest,
                    *chunk_count,
                    chunks,
                )?;
            }
            _ => unreachable!("Machine value variant was selected by verify"),
        }
        Ok(())
    }

    fn verify_collection_descriptor(&self) -> DurableResult<()> {
        match self {
            Self::ApplicationJournal { journal_id, root } => {
                if journal_id.is_empty() {
                    return Err(DurableError::Validation(
                        "application-journal root has an empty owner".to_owned(),
                    ));
                }
                root.verify()?;
            }
            Self::ApplicationJournalRecordManifests { journal_id, root } => {
                if journal_id.is_empty() {
                    return Err(DurableError::Validation(
                        "application-journal record-manifest root has an empty owner".to_owned(),
                    ));
                }
                root.verify()?;
            }
            Self::ResourceHandoffIndex { to_run, root }
            | Self::ResourceHandoffActivationIndex { to_run, root } => {
                validate_resource_target(to_run)?;
                root.verify()?;
            }
            Self::ResourceHandoffSlots { to_run, root } => {
                validate_resource_target(to_run)?;
                root.verify()?;
            }
            Self::AgentMessageIndex { session_id, root }
            | Self::AgentUnresolvedOccurrenceIndex { session_id, root }
            | Self::AgentOpenStreamIndex { session_id, root } => {
                validate_agent_session(session_id)?;
                root.verify()?;
            }
            Self::RunQueryIndexes {
                index_version,
                run_id,
                waits,
                effects,
                occurrences,
                attempts,
                pending_waits,
                active_effects,
                active_leases,
                terminal,
            } => {
                if index_version != RUN_QUERY_INDEXES_VERSION {
                    return Err(DurableError::Validation(format!(
                        "unsupported Run query-index version {index_version:?}"
                    )));
                }
                cymule_core::validate_identity("Run query-index owner", run_id)?;
                for root in [
                    waits,
                    effects,
                    occurrences,
                    attempts,
                    pending_waits,
                    active_effects,
                    active_leases,
                ] {
                    root.verify()?;
                }
                if let Some(terminal) = terminal {
                    terminal.verify()?;
                }
            }
            Self::PendingWaitSource {
                source_version,
                source,
                waits,
            } => {
                if source_version != PENDING_WAIT_SOURCE_VERSION || waits.entries == 0 {
                    return Err(DurableError::Validation(
                        "pending-Wait source descriptor has an invalid version or empty root"
                            .to_owned(),
                    ));
                }
                source.verify()?;
                waits.verify()?;
            }
            _ => unreachable!("collection descriptor variant was selected by verify"),
        }
        Ok(())
    }

    fn pending_references(&self) -> Vec<String> {
        match self {
            Self::Leaf { .. }
            | Self::MachineCommandCurrent { .. }
            | Self::MachinePendingCommand { .. }
            | Self::MachineIndexMembership { .. }
            | Self::MachineOrderEntry { .. }
            | Self::MachineBaseChunk { .. } => Vec::new(),
            Self::MachineRunCurrent { current } => machine_run_current_roots(current),
            Self::MachineScopeCurrent { current, .. } => machine_scope_current_roots(current),
            Self::MachinePagedTransitionCurrent { current } => {
                machine_paged_transition_roots(current)
            }
            Self::MachineBaseDescriptor { chunks: root, .. }
            | Self::ApplicationJournal { root, .. }
            | Self::ResourceHandoffIndex { root, .. }
            | Self::ResourceHandoffActivationIndex { root, .. }
            | Self::AgentMessageIndex { root, .. }
            | Self::AgentUnresolvedOccurrenceIndex { root, .. }
            | Self::AgentOpenStreamIndex { root, .. } => root.node.iter().cloned().collect(),
            Self::ApplicationJournalRecordManifests { root, .. }
            | Self::ResourceHandoffSlots { root, .. }
            | Self::PendingWaitSource { waits: root, .. } => root.node.iter().cloned().collect(),
            Self::RunQueryIndexes {
                waits,
                effects,
                occurrences,
                attempts,
                pending_waits,
                active_effects,
                active_leases,
                terminal,
                ..
            } => {
                let mut references: Vec<_> = [
                    waits,
                    effects,
                    occurrences,
                    attempts,
                    pending_waits,
                    active_effects,
                    active_leases,
                ]
                .into_iter()
                .filter_map(|root| root.node.clone())
                .collect();
                if let Some(terminal) = terminal {
                    references.extend(
                        [
                            &terminal.effects,
                            &terminal.active_effects,
                            &terminal.active_leases,
                        ]
                        .into_iter()
                        .filter_map(|root| root.node.clone()),
                    );
                }
                references
            }
        }
    }
}

fn verify_machine_command_current(
    record: &cymule_core::ArchivedCommandRecord,
    admission: &cymule_core::CommandAdmission,
    index_proof: &cymule_core::MachineCommandIndexProof,
    first_event_position: Option<u64>,
) -> DurableResult<()> {
    record.verify()?;
    cymule_core::validate_identity("Machine hot command", &record.envelope.command_id)?;
    if admission.command_id != record.envelope.command_id
        || admission.semantic_hash != record.semantic_hash
        || admission.command_record_digest != cymule_core::canonical_digest(record)?
        || admission.status != record.receipt.status
        || admission.event_ids != record.receipt.event_ids
        || index_proof.command_id != record.envelope.command_id
        || index_proof.value.is_some()
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_authority_mismatch".to_owned(),
            message: "Machine hot command leaf does not bind one record, admission, and archive non-membership proof".to_owned(),
        });
    }
    let event_count = u64::try_from(record.receipt.event_ids.len())
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    if match first_event_position {
        None => event_count != 0,
        Some(first) => {
            first == 0
                || event_count == 0
                || first
                    .checked_add(event_count - 1)
                    .is_none_or(|last| last > MAX_EXACT_INTEGER)
        }
    } {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_event_position_invalid".to_owned(),
            message: "Machine hot command has an invalid exact Event range".to_owned(),
        });
    }
    Ok(())
}

fn verify_machine_scope_witness(
    current: &cymule_core::durable_internal::MachineScopeCurrent,
    invocation_path: &[cymule_core::InvocationPathSegment],
    region_path: &[usize],
) -> DurableResult<()> {
    current.verify()?;
    if current.invocation_path_digest != cymule_core::canonical_digest(&invocation_path)?
        || current.region_path_digest != cymule_core::canonical_digest(&region_path)?
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_scope_witness_mismatch".to_owned(),
            message: format!(
                "Machine Scope {} lexical witness does not match its pinned digests",
                current.scope_id
            ),
        });
    }
    Ok(())
}

mod machine_base_chunk_base64 {
    use std::fmt;

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use serde::de::Visitor;
    use serde::{Deserializer, Serializer};

    const MAX_ENCODED_BYTES: usize = super::MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES.div_ceil(3) * 4;

    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    fn decoded_length(encoded: &str) -> Result<usize, &'static str> {
        let encoded = encoded.as_bytes();
        if encoded.len() > MAX_ENCODED_BYTES || !encoded.len().is_multiple_of(4) {
            return Err("Machine base chunk Base64 exceeds its encoded bound or padding length");
        }
        if encoded.is_empty() {
            return Ok(0);
        }
        let padding = if encoded.ends_with(b"==") {
            2
        } else {
            usize::from(encoded.ends_with(b"="))
        };
        // The encoded-length bound makes this arithmetic exact and bounded.
        let decoded_len = encoded.len() / 4 * 3 - padding;
        if decoded_len > super::MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES {
            return Err("Machine base chunk exceeds its decoded byte bound");
        }
        let data = &encoded[..encoded.len() - padding];
        if data.iter().any(|byte| sextet(*byte).is_none()) {
            return Err(
                "Machine base chunk requires the standard Base64 alphabet and final padding",
            );
        }
        if padding != 0 {
            let last = sextet(data[data.len() - 1])
                .ok_or("Machine base chunk has an invalid terminal Base64 symbol")?;
            if (padding == 2 && last & 0x0f != 0) || (padding == 1 && last & 0x03 != 0) {
                return Err("Machine base chunk Base64 has nonzero unused padding bits");
            }
        }
        Ok(decoded_len)
    }

    fn decode(encoded: &str) -> Result<Vec<u8>, &'static str> {
        let decoded_len = decoded_length(encoded)?;
        // No output allocation occurs before the complete canonicality and
        // decoded-byte preflight, including the maximum-size padded suffix.
        let mut bytes = vec![0; decoded_len];
        let written = STANDARD
            .decode_slice(encoded, &mut bytes)
            .map_err(|_| "Machine base chunk is not canonical padded Base64")?;
        if written != decoded_len {
            return Err("Machine base chunk Base64 decoded length changed after preflight");
        }
        Ok(bytes)
    }

    struct ChunkVisitor;

    impl Visitor<'_> for ChunkVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bounded canonical padded Base64 Machine-base bytes")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            decode(value).map_err(E::custom)
        }
    }

    pub(super) fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if bytes.len() > super::MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES {
            return Err(serde::ser::Error::custom(
                "Machine base chunk exceeds its decoded byte bound",
            ));
        }
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ChunkVisitor)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn preflight_checks_exact_decoded_bound_before_output_allocation() {
            let exact = STANDARD.encode(vec![
                0;
                super::super::MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES
            ]);
            let oversized = STANDARD.encode(vec![
                0;
                super::super::MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES
                    + 1
            ]);
            assert_eq!(exact.len(), oversized.len());
            assert_eq!(
                decoded_length(&exact),
                Ok(super::super::MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES)
            );
            assert_eq!(
                decoded_length(&oversized),
                Err("Machine base chunk exceeds its decoded byte bound")
            );
            assert!(decode(&oversized).is_err());
            assert_eq!(
                decode(&exact).expect("maximum padded chunk decodes").len(),
                super::super::MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES
            );
        }

        #[test]
        fn preflight_rejects_noncanonical_padding_and_alphabet() {
            for encoded in [
                "AA", "AAA", "A===", "====", "AA=A", "AA==AA==", "AA==\n", "AA-_", "AB==", "AAB=",
            ] {
                assert!(decoded_length(encoded).is_err(), "accepted {encoded:?}");
                assert!(decode(encoded).is_err(), "decoded {encoded:?}");
            }
            for (encoded, decoded) in [
                ("AA==", vec![0]),
                ("AAA=", vec![0, 0]),
                ("AAAA", vec![0, 0, 0]),
            ] {
                assert_eq!(decode(encoded).expect("canonical chunk decodes"), decoded);
            }
        }
    }
}

fn verify_machine_base_chunk(index: u64, bytes: &[u8]) -> DurableResult<()> {
    if index > MAX_EXACT_INTEGER
        || bytes.is_empty()
        || bytes.len() > MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES
    {
        return Err(DurableError::Validation(
            "Machine-base chunk has an invalid index or byte length".to_owned(),
        ));
    }
    Ok(())
}

fn verify_machine_base_descriptor(
    canonical_len: u64,
    canonical_digest: &str,
    chunk_count: u64,
    chunks: &LogRoot,
) -> DurableResult<()> {
    let expected_chunks = canonical_len
        .checked_add(
            u64::try_from(MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES - 1)
                .map_err(|error| DurableError::Validation(error.to_string()))?,
        )
        .and_then(|len| {
            len.checked_div(u64::try_from(MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES).ok()?)
        });
    if canonical_len == 0
        || canonical_len > MAX_EXACT_INTEGER
        || chunk_count == 0
        || chunk_count != chunks.len
        || expected_chunks != Some(chunk_count)
    {
        return Err(DurableError::Validation(
            "Machine-base descriptor has an invalid byte or chunk count".to_owned(),
        ));
    }
    validate_digest("Machine-base canonical digest", canonical_digest)?;
    chunks.verify()?;
    Ok(())
}

fn map_and_log_root_ids<'a>(
    maps: impl IntoIterator<Item = &'a MapRoot>,
    logs: impl IntoIterator<Item = &'a LogRoot>,
) -> Vec<String> {
    maps.into_iter()
        .filter_map(|root| root.node.clone())
        .chain(logs.into_iter().filter_map(|root| root.node.clone()))
        .collect()
}

fn machine_run_current_roots(
    current: &cymule_core::durable_internal::MachineRunCurrent,
) -> Vec<String> {
    map_and_log_root_ids(
        [
            &current.children.scopes,
            &current.children.effects,
            &current.children.obligations,
            &current.children.attempts,
            &current.indexes.governance_effects,
            &current.indexes.unknown_effects,
            &current.indexes.pending_effects,
            &current.indexes.terminal_transition_effects,
            &current.indexes.open_scopes,
            &current.indexes.unresolved_obligations,
        ],
        [
            &current.order.scopes,
            &current.order.effects,
            &current.order.obligations,
            &current.order.attempts,
            &current.order.plans,
            &current.order.bindings,
        ],
    )
}

fn machine_scope_current_roots(
    current: &cymule_core::durable_internal::MachineScopeCurrent,
) -> Vec<String> {
    map_and_log_root_ids(
        [
            &current.effects,
            &current.mutating_effects,
            &current.abort_transitions,
            &current.abort_blockers,
        ],
        [&current.effect_order, &current.mutating_effect_order],
    )
}

fn machine_paged_transition_roots(
    current: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
) -> Vec<String> {
    let mut roots = map_and_log_root_ids(
        [
            &current.staged_material.plans,
            &current.staged_material.artifacts,
            &current.shadow.children.scopes,
            &current.shadow.children.effects,
            &current.shadow.children.obligations,
            &current.shadow.children.attempts,
            &current.shadow.indexes.governance_effects,
            &current.shadow.indexes.unknown_effects,
            &current.shadow.indexes.pending_effects,
            &current.shadow.indexes.terminal_transition_effects,
            &current.shadow.indexes.open_scopes,
            &current.shadow.indexes.unresolved_obligations,
        ],
        [
            &current.effect_source,
            &current.scope_source,
            &current.shadow.order.scopes,
            &current.shadow.order.effects,
            &current.shadow.order.obligations,
            &current.shadow.order.attempts,
            &current.shadow.order.plans,
            &current.shadow.order.bindings,
        ],
    );
    roots.sort();
    roots.dedup();
    roots
}

fn verify_nested_state_value_bound(value: &StateRootValue, kind: &str) -> DurableResult<()> {
    let len = cymule_core::canonical_bytes(value)?.len();
    if len > MAX_STATE_ROOT_LEAF_BYTES {
        return Err(DurableError::Validation(format!(
            "{kind} has {len} canonical bytes; maximum is {MAX_STATE_ROOT_LEAF_BYTES}"
        )));
    }
    Ok(())
}

fn verify_machine_index_entry(
    selector: &cymule_core::durable_internal::MachineRunIndexSelector,
    entry: &str,
) -> DurableResult<()> {
    use cymule_core::durable_internal::MachineRunIndexSelector as Selector;

    match selector {
        Selector::OpenScopes => cymule_core::validate_identity("Machine open Scope", entry)?,
        Selector::UnresolvedObligations => {
            cymule_core::validate_content_id("Machine obligation index entry", entry)?;
        }
        Selector::GovernanceEffects
        | Selector::UnknownEffects
        | Selector::PendingEffects
        | Selector::TerminalTransitionEffects
        | Selector::ScopeEffects { .. }
        | Selector::ScopeMutatingEffects { .. }
        | Selector::ScopeAbortTransitions { .. }
        | Selector::ScopeAbortBlockers { .. } => {
            cymule_core::validate_content_id("Machine Effect index entry", entry)?;
        }
    }
    match selector {
        Selector::ScopeEffects { scope_id }
        | Selector::ScopeMutatingEffects { scope_id }
        | Selector::ScopeAbortTransitions { scope_id }
        | Selector::ScopeAbortBlockers { scope_id } => {
            cymule_core::validate_identity("Machine index Scope", scope_id)?;
        }
        Selector::GovernanceEffects
        | Selector::UnknownEffects
        | Selector::PendingEffects
        | Selector::TerminalTransitionEffects
        | Selector::OpenScopes
        | Selector::UnresolvedObligations => {}
    }
    Ok(())
}

fn verify_machine_order_entry(
    selector: &cymule_core::durable_internal::MachineRunLogSelector,
    entry: &str,
) -> DurableResult<()> {
    use cymule_core::durable_internal::MachineRunLogSelector as Selector;

    match selector {
        Selector::Scopes => cymule_core::validate_identity("Machine Scope log entry", entry)?,
        Selector::Effects
        | Selector::Obligations
        | Selector::Attempts
        | Selector::Plans
        | Selector::Bindings
        | Selector::ScopeEffects { .. }
        | Selector::ScopeMutatingEffects { .. } => {
            cymule_core::validate_content_id("Machine ordered log entry", entry)?;
        }
    }
    match selector {
        Selector::ScopeEffects { scope_id } | Selector::ScopeMutatingEffects { scope_id } => {
            cymule_core::validate_identity("Machine log Scope", scope_id)?;
        }
        Selector::Scopes
        | Selector::Effects
        | Selector::Obligations
        | Selector::Attempts
        | Selector::Plans
        | Selector::Bindings => {}
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RunQueryIndexRoots {
    waits: MapRoot,
    effects: MapRoot,
    occurrences: MapRoot,
    attempts: MapRoot,
    pending_waits: MapRoot,
    active_effects: MapRoot,
    active_leases: MapRoot,
    terminal: Option<Box<RunTerminalSidecarCurrent>>,
}

impl RunQueryIndexRoots {
    fn verify(&self) -> DurableResult<()> {
        for root in [
            &self.waits,
            &self.effects,
            &self.occurrences,
            &self.attempts,
            &self.pending_waits,
            &self.active_effects,
            &self.active_leases,
        ] {
            root.verify()?;
        }
        if let Some(terminal) = &self.terminal {
            terminal.verify()?;
        }
        Ok(())
    }
}

fn verify_leaf_bytes(kind: StateRootLeafKind, bytes: &[u8]) -> DurableResult<()> {
    use StateRootLeafKind as Kind;
    use cymule_profile_protocol::{agent, evolution, resource, virtual_work};

    match kind {
        Kind::MachinePlan => verify_typed_leaf::<cymule_core::SealedPlan>(bytes),
        Kind::MachineArtifact => verify_machine_artifact_leaf(bytes),
        Kind::MachineEvent => verify_typed_leaf::<cymule_core::Event>(bytes),
        Kind::MachineAdmission => verify_typed_leaf::<cymule_core::CommandAdmission>(bytes),
        Kind::MachineCommandBatch => {
            verify_typed_leaf::<cymule_core::durable_internal::MachineCommandBatchRecord>(bytes)
        }
        Kind::MachineEffect => verify_typed_leaf::<cymule_core::EffectProjection>(bytes),
        Kind::MachineObligation => verify_typed_leaf::<cymule_core::ObligationProjection>(bytes),
        Kind::MachineAttempt => verify_typed_leaf::<cymule_core::AttemptProjection>(bytes),
        Kind::MachineFact => verify_typed_leaf::<String>(bytes),
        Kind::Continuation => verify_typed_leaf::<crate::Continuation>(bytes),
        Kind::RunCurrent => verify_typed_leaf::<crate::DurableRunCurrent>(bytes),
        Kind::Wait => verify_typed_leaf::<crate::WaitCondition>(bytes),
        Kind::WaitSummary => verify_wait_summary_leaf(bytes),
        Kind::WaitActivation => verify_typed_leaf::<crate::WaitActivationReceipt>(bytes),
        Kind::CancellationReceipt => verify_typed_leaf::<crate::CancellationReceipt>(bytes),
        Kind::EffectResolutionReceipt => verify_typed_leaf::<crate::EffectResolutionReceipt>(bytes),
        Kind::Lease => verify_typed_leaf::<crate::CoordinationLease>(bytes),
        Kind::Outbox => verify_effect_dispatch_leaf(bytes),
        Kind::OutboxOwner => verify_outbox_owner_leaf(bytes),
        Kind::ComponentOccurrence => verify_typed_leaf::<crate::ComponentOccurrence>(bytes),
        Kind::OperationAttempt => verify_typed_leaf::<crate::OperationAttempt>(bytes),
        Kind::ClockObservation => verify_typed_leaf::<crate::ClockObservation>(bytes),
        Kind::Snapshot => verify_typed_leaf::<crate::SnapshotRecord>(bytes),
        Kind::HistoryCompaction => verify_typed_leaf::<crate::HistoryCompactionReceipt>(bytes),
        Kind::JournalRecord => verify_typed_leaf::<crate::JournalRecord>(bytes),
        Kind::JournalPrefixReplacement => {
            verify_typed_leaf::<crate::ApplicationJournalPrefixReplacementReceipt>(bytes)
        }
        Kind::JournalRecordManifest => verify_typed_leaf::<crate::JournalRecordManifest>(bytes),
        Kind::JournalPrefixReplacementAuthority => {
            verify_typed_leaf::<crate::ApplicationJournalPrefixReplacementAuthority>(bytes)
        }
        Kind::CoupledCheckpointReceipt => {
            verify_typed_leaf::<crate::CoupledCheckpointReceipt>(bytes)
        }
        Kind::ResourceCommandReceipt => {
            verify_typed_leaf::<resource::ResourceCommandReceipt>(bytes)
        }
        Kind::ResourceRetentionCurrent => {
            verify_typed_leaf::<resource::ResourceRetentionCurrent>(bytes)
        }
        Kind::ResourcePinCurrent => verify_typed_leaf::<resource::ResourcePinCurrent>(bytes),
        Kind::ResourceDeleteCurrent => verify_typed_leaf::<resource::ResourceDeleteCurrent>(bytes),
        Kind::ResourceHandoffCurrent => {
            verify_typed_leaf::<resource::ResourceHandoffCurrent>(bytes)
        }
        Kind::ResourceHandoffActivationCurrent => {
            verify_typed_leaf::<resource::ResourceHandoffActivationCurrent>(bytes)
        }
        Kind::ResourceHandoffIndex => {
            verify_typed_leaf::<resource::ResourceHandoffIndexEntry>(bytes)
        }
        Kind::ResourceHandoffActivationIndex => {
            verify_typed_leaf::<resource::ResourceHandoffActivationIndexEntry>(bytes)
        }
        Kind::AgentCommand => verify_typed_leaf::<agent::AgentCommand>(bytes),
        Kind::AgentCommandReceipt => verify_typed_leaf::<agent::AgentCommandReceipt>(bytes),
        Kind::AgentInputSuspensionReceipt => {
            verify_typed_leaf::<crate::model::AgentInputSuspensionReceipt>(bytes)
        }
        Kind::AgentInputCompletionReceipt => {
            verify_typed_leaf::<crate::model::AgentInputCompletionReceipt>(bytes)
        }
        Kind::AgentSessionCurrent => verify_typed_leaf::<agent::AgentSessionCurrent>(bytes),
        Kind::AgentUpdateCurrent => verify_typed_leaf::<agent::AgentUpdateCurrent>(bytes),
        Kind::AgentMessageCurrent => verify_typed_leaf::<agent::AgentMessageCurrent>(bytes),
        Kind::AgentToolCurrent => verify_typed_leaf::<agent::AgentToolCurrent>(bytes),
        Kind::AgentTargetClaimCurrent => verify_typed_leaf::<agent::AgentTargetClaimCurrent>(bytes),
        Kind::AgentElicitationCurrent => verify_typed_leaf::<agent::AgentElicitationCurrent>(bytes),
        Kind::AgentOccurrenceCurrent => verify_typed_leaf::<agent::AgentOccurrenceCurrent>(bytes),
        Kind::AgentStreamCurrent => verify_typed_leaf::<agent::AgentStreamCurrent>(bytes),
        Kind::AgentStreamChunkCurrent => verify_typed_leaf::<agent::AgentStreamChunkCurrent>(bytes),
        Kind::EvolutionCurrent => verify_typed_leaf::<evolution::EvolutionCurrent>(bytes),
        Kind::EvolutionCommandAlias => verify_typed_leaf::<evolution::EvolutionCommandAlias>(bytes),
        Kind::EvolutionPersistenceReceipt => {
            verify_typed_leaf::<evolution::EvolutionPersistenceReceipt>(bytes)
        }
        Kind::EvolutionMutation => verify_typed_leaf::<evolution::EvolutionMutation>(bytes),
        Kind::VirtualCurrent => verify_typed_leaf::<virtual_work::VirtualCurrent>(bytes),
        Kind::VirtualPersistenceReceipt => {
            verify_typed_leaf::<virtual_work::VirtualPersistenceReceipt>(bytes)
        }
        Kind::VirtualStateLeaf => verify_typed_leaf::<virtual_work::VirtualStateLeaf>(bytes),
        Kind::ResourceCatalogRecord => verify_typed_leaf::<resource::ResourceCatalogRecord>(bytes),
    }
}

fn verify_machine_artifact_leaf(bytes: &[u8]) -> DurableResult<()> {
    let artifact = cymule_core::ArtifactRecord::decode(bytes)?;
    if cymule_core::canonical_bytes(&artifact)? != bytes {
        return Err(DurableError::Integrity {
            code: "state_root_machine_artifact_canonical_mismatch".to_owned(),
            message: "Machine Artifact leaf changes under its strict canonical round trip"
                .to_owned(),
        });
    }
    Ok(())
}

fn verify_wait_summary_leaf(bytes: &[u8]) -> DurableResult<()> {
    verify_checked_typed_leaf::<crate::DurableWaitSummary>(bytes, crate::DurableWaitSummary::verify)
}

fn verify_effect_dispatch_leaf(bytes: &[u8]) -> DurableResult<()> {
    verify_checked_typed_leaf::<crate::EffectDispatch>(bytes, crate::EffectDispatch::verify_wire)
}

fn validate_resource_target(to_run: &str) -> DurableResult<()> {
    cymule_core::validate_identity("Resource target Run", to_run).map_err(Into::into)
}

fn validate_agent_session(session_id: &str) -> DurableResult<()> {
    cymule_core::validate_identity("Agent Session", session_id).map_err(Into::into)
}

pub(crate) fn pending_wait_source_key(
    source: &crate::WaitActivationSource,
) -> DurableResult<String> {
    source.verify()?;
    cymule_core::content_id(PENDING_WAIT_SOURCE_VERSION, source).map_err(Into::into)
}

fn pending_wait_source_for(wait: &crate::WaitCondition) -> Option<crate::WaitActivationSource> {
    match &wait.kind {
        crate::WaitKind::Signal { key } => {
            Some(crate::WaitActivationSource::Signal { key: key.clone() })
        }
        crate::WaitKind::Timer { timer_id } => Some(crate::WaitActivationSource::Timer {
            timer_id: timer_id.clone(),
        }),
        crate::WaitKind::Input { .. } => None,
    }
}

#[derive(Clone, Copy)]
enum AgentIndexKind {
    Messages,
    UnresolvedOccurrences,
    OpenStreams,
}

fn decode_agent_index_root(
    value: &StateRootValue,
    session_id: &str,
    kind: AgentIndexKind,
) -> DurableResult<LogRoot> {
    let ((
        AgentIndexKind::Messages,
        StateRootValue::AgentMessageIndex {
            session_id: owner,
            root,
        },
    )
    | (
        AgentIndexKind::UnresolvedOccurrences,
        StateRootValue::AgentUnresolvedOccurrenceIndex {
            session_id: owner,
            root,
        },
    )
    | (
        AgentIndexKind::OpenStreams,
        StateRootValue::AgentOpenStreamIndex {
            session_id: owner,
            root,
        },
    )) = (kind, value)
    else {
        return Err(DurableError::Integrity {
            code: "state_root_agent_index_value_kind_mismatch".to_owned(),
            message: "Agent persistent-log descriptor has the wrong closed kind".to_owned(),
        });
    };
    if owner != session_id {
        return Err(DurableError::Integrity {
            code: "state_root_agent_index_owner_mismatch".to_owned(),
            message: "Agent persistent-log descriptor changed its owning Session".to_owned(),
        });
    }
    root.verify()?;
    Ok(root.clone())
}

fn decode_resource_index_root(
    value: &StateRootValue,
    to_run: &str,
    activation: bool,
) -> DurableResult<LogRoot> {
    let ((
        false,
        StateRootValue::ResourceHandoffIndex {
            to_run: owner,
            root,
        },
    )
    | (
        true,
        StateRootValue::ResourceHandoffActivationIndex {
            to_run: owner,
            root,
        },
    )) = (activation, value)
    else {
        return Err(DurableError::Integrity {
            code: "state_root_resource_index_value_kind_mismatch".to_owned(),
            message: "Resource persistent-log descriptor has the wrong closed kind".to_owned(),
        });
    };
    if owner != to_run {
        return Err(DurableError::Integrity {
            code: "state_root_resource_index_owner_mismatch".to_owned(),
            message: "Resource target index changed its owning Run".to_owned(),
        });
    }
    root.verify()?;
    Ok(root.clone())
}

fn verify_typed_leaf<T>(bytes: &[u8]) -> DurableResult<()>
where
    T: DeserializeOwned + Serialize,
{
    verify_checked_typed_leaf::<T>(bytes, |_| Ok(()))
}

fn verify_checked_typed_leaf<T>(
    bytes: &[u8],
    verify: impl FnOnce(&T) -> DurableResult<()>,
) -> DurableResult<()>
where
    T: DeserializeOwned + Serialize,
{
    let decoded = cymule_core::decode_json::<T>(bytes)?;
    if cymule_core::canonical_bytes(&decoded)? != bytes {
        return Err(DurableError::Integrity {
            code: "state_root_leaf_canonical_mismatch".to_owned(),
            message: "state-root leaf changes under its typed canonical round trip".to_owned(),
        });
    }
    verify(&decoded)
}

fn verify_outbox_owner_leaf(bytes: &[u8]) -> DurableResult<()> {
    verify_typed_leaf::<OutboxOwner>(bytes)?;
    cymule_core::decode_json::<OutboxOwner>(bytes)?.verify()
}

/// Immutable content-addressed canonical JSON value object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateValueObject {
    /// Value object schema.
    pub value_version: String,
    /// Content identity of `value`.
    pub object_id: String,
    /// Canonical value and explicit nested references.
    pub value: StateRootValue,
}

impl StateValueObject {
    fn new(value: StateRootValue) -> DurableResult<Self> {
        value.verify()?;
        let object_id = state_root_value_id(&value)?;
        Ok(Self {
            value_version: STATE_ROOT_VALUE_VERSION.to_owned(),
            object_id,
            value,
        })
    }

    /// Verify schema, content identity, canonical bytes, and references.
    ///
    /// # Errors
    ///
    /// Rejects unsupported schemas, invalid values, or mismatched content IDs.
    pub fn verify(&self) -> DurableResult<()> {
        if self.value_version != STATE_ROOT_VALUE_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported state-root value version {:?}",
                self.value_version
            )));
        }
        self.value.verify()?;
        let expected = state_root_value_id(&self.value)?;
        if self.object_id != expected {
            return Err(DurableError::Integrity {
                code: "state_root_value_identity_mismatch".to_owned(),
                message: format!(
                    "state-root value identity {} does not match {expected}",
                    self.object_id
                ),
            });
        }
        Ok(())
    }
}

fn state_root_value_id(value: &StateRootValue) -> DurableResult<String> {
    match value {
        StateRootValue::MachineIndexMembership {
            run_id,
            selector,
            entry,
        } => cymule_core::durable_internal::machine_index_membership_value_id(
            run_id, selector, entry,
        )
        .map_err(Into::into),
        StateRootValue::MachineOrderEntry {
            run_id,
            selector,
            entry,
        } => cymule_core::durable_internal::machine_order_entry_value_id(run_id, selector, entry)
            .map_err(Into::into),
        _ => cymule_core::content_id(STATE_ROOT_VALUE_VERSION, value).map_err(Into::into),
    }
}

/// One immutable object in the state-root graph.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "object",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum StateRootObject {
    /// Canonical typed value bytes.
    Value(StateValueObject),
    /// Persistent-map node.
    MapNode(MapNode),
    /// Persistent-log node.
    LogNode(LogNode),
    /// Fixed root manifest.
    Manifest(StateRootManifest),
}

impl StateRootObject {
    /// Borrow the content identity used by physical stores.
    pub fn object_id(&self) -> &str {
        match self {
            Self::Value(value) => &value.object_id,
            Self::MapNode(value) => &value.object_id,
            Self::LogNode(value) => &value.object_id,
            Self::Manifest(value) => &value.manifest_id,
        }
    }

    /// Verify the closed object variant and content identity.
    ///
    /// # Errors
    ///
    /// Rejects oversized objects or invalid variant-specific content authority.
    pub fn verify(&self) -> DurableResult<()> {
        if cymule_core::canonical_bytes(self)?.len() > MAX_STATE_ROOT_OBJECT_BYTES {
            return Err(DurableError::Validation(format!(
                "state-root object exceeds {MAX_STATE_ROOT_OBJECT_BYTES} canonical bytes"
            )));
        }
        match self {
            Self::Value(value) => value.verify(),
            Self::MapNode(value) => value.verify().map_err(Into::into),
            Self::LogNode(value) => value.verify().map_err(Into::into),
            Self::Manifest(value) => value.verify(),
        }
    }

    fn pending_references(&self) -> Vec<String> {
        match self {
            Self::Value(value) => value.value.pending_references(),
            Self::MapNode(node) => node
                .child_object_ids()
                .into_iter()
                .chain(node.value_object_id())
                .map(str::to_owned)
                .collect(),
            Self::LogNode(node) => {
                let mut references = node
                    .child_object_ids()
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                references.extend(node.value_object_ids().iter().cloned());
                references
            }
            Self::Manifest(manifest) => {
                let mut references = manifest.roots.root_object_ids();
                references.extend(
                    [
                        &manifest.machine_frontier.runs,
                        &manifest.machine_frontier.facts,
                        &manifest.machine_frontier.pending_commands,
                        &manifest.machine_frontier.paged_transitions,
                    ]
                    .into_iter()
                    .filter_map(|root| root.node.clone()),
                );
                references.sort();
                references.dedup();
                references
            }
        }
    }
}

/// Decode one physical state-root object through its canonical bounded
/// envelope. Collection nodes are decoded only by the lower collection crate.
///
/// # Errors
///
/// Rejects oversized, noncanonical, unsupported, or unauthenticated objects.
pub fn decode_state_root_object(bytes: &[u8]) -> DurableResult<StateRootObject> {
    const VALUE: &[u8] = b"{\"object\":\"value\",\"payload\":";
    const MAP: &[u8] = b"{\"object\":\"map_node\",\"payload\":";
    const LOG: &[u8] = b"{\"object\":\"log_node\",\"payload\":";
    const MANIFEST: &[u8] = b"{\"object\":\"manifest\",\"payload\":";
    if bytes.len() > MAX_STATE_ROOT_OBJECT_BYTES {
        return Err(DurableError::Validation(format!(
            "state-root object exceeds {MAX_STATE_ROOT_OBJECT_BYTES} canonical bytes"
        )));
    }
    let payload = |prefix: &[u8]| -> DurableResult<&[u8]> {
        bytes
            .strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(b"}"))
            .ok_or_else(|| {
                DurableError::Encoding(
                    "state-root object does not use its canonical physical envelope".to_owned(),
                )
            })
    };
    let object = if bytes.starts_with(VALUE) {
        StateRootObject::decode_value_object(payload(VALUE)?)?
    } else if bytes.starts_with(MAP) {
        StateRootObject::MapNode(decode_map_node(payload(MAP)?)?)
    } else if bytes.starts_with(LOG) {
        StateRootObject::LogNode(decode_log_node(payload(LOG)?)?)
    } else if bytes.starts_with(MANIFEST) {
        StateRootObject::Manifest(serde_json::from_slice(payload(MANIFEST)?)?)
    } else {
        return Err(DurableError::Encoding(
            "state-root object has an unknown physical variant".to_owned(),
        ));
    };
    object.verify()?;
    if cymule_core::canonical_bytes(&object)? != bytes {
        return Err(DurableError::Integrity {
            code: "state_root_object_noncanonical_transport".to_owned(),
            message: "state-root object transport is not its unique canonical encoding".to_owned(),
        });
    }
    Ok(object)
}

impl StateRootObject {
    fn decode_value_object(bytes: &[u8]) -> DurableResult<Self> {
        Ok(Self::Value(serde_json::from_slice(bytes)?))
    }
}

/// Closed durable-state collection selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateRootFamily {
    /// Sealed Machine Plans keyed by Plan identity.
    MachinePlans,
    /// Immutable Machine Artifacts keyed by Artifact identity.
    MachineArtifacts,
    /// Hot Machine command records keyed by command identity.
    MachineCommands,
    /// Hot atomic Machine command batches keyed by batch identity.
    MachineCommandBatches,
    /// Run Continuations.
    Continuations,
    /// Bounded semantic Run currents used by ordinary control queries.
    RunCurrents,
    /// Per-Run nested roots for waits, Effects, occurrences, and Attempts.
    RunQueryIndexes,
    /// Registered waits.
    Waits,
    /// Pageable signal sources with pending waits.
    PendingSignalSources,
    /// Exact timer sources with pending waits.
    PendingTimerSources,
    /// Identified wait-activation receipts.
    WaitActivations,
    /// Exact Run cancellation receipts keyed by cancellation command identity.
    CancellationReceipts,
    /// Exact terminal Effect receipts keyed by resolution command identity.
    EffectResolutionReceipts,
    /// Coordination leases.
    Leases,
    /// Immutable Effect-to-Run lookups; mutable outbox entries are Run-owned.
    Outbox,
    /// Component occurrences.
    ComponentOccurrences,
    /// Provider operation Attempts.
    OperationAttempts,
    /// Hot Clock observations.
    ClockObservations,
    /// Portable snapshot records.
    Snapshots,
    /// Machine-history compaction receipts.
    HistoryCompactions,
    /// Application-journal descriptors.
    ApplicationJournals,
    /// Latest application-journal prefix replacement per journal.
    ApplicationJournalPrefixReplacements,
    /// Nested all-ever record-manifest roots per journal.
    ApplicationJournalRecordManifests,
    /// Cumulative application-journal prefix replacement authority.
    ApplicationJournalPrefixReplacementHistory,
    /// Complete higher-profile coupled-checkpoint receipts.
    CoupledCheckpointReceipts,
    /// Exact all-ever Resource command receipts and authority aliases.
    ResourceCommandReceipts,
    /// Current keyed physical Resource-retention projections.
    ResourceRetentionCurrent,
    /// Current keyed exact Resource-pin projections.
    ResourcePinCurrent,
    /// Current keyed Resource-deletion projections.
    ResourceDeleteCurrent,
    /// Current immutable Resource-handoff authorities.
    ResourceHandoffCurrent,
    /// Exact target-slot Resource-handoff entries.
    ResourceHandoffSlots,
    /// Current immutable Resource-handoff activation authorities.
    ResourceHandoffActivationCurrent,
    /// Immutable Resource-handoff activations keyed by source transfer.
    ResourceHandoffActivationsByTransfer,
    /// Per-target ordered Resource-handoff indexes.
    ResourceHandoffIndexes,
    /// Per-target ordered Resource-handoff activation indexes.
    ResourceHandoffActivationIndexes,
    /// Exact all-ever Agent persistence commands.
    AgentCommands,
    /// Exact all-ever Agent persistence receipts keyed by command identity.
    AgentCommandReceipts,
    /// Durable-private Agent input suspension receipts keyed by Wait identity.
    AgentInputSuspensionReceipts,
    /// Durable-private Agent input completion receipts keyed by Wait identity.
    AgentInputCompletionReceipts,
    /// Bounded Agent Session metadata keyed by Session identity.
    AgentSessions,
    /// Agent update idempotency authorities keyed by protocol storage identity.
    AgentUpdates,
    /// Immutable Agent messages keyed by protocol storage identity.
    AgentMessages,
    /// Per-Session ordered Agent message indexes.
    AgentMessageIndexes,
    /// Agent tool-call currents keyed by protocol storage identity.
    AgentTools,
    /// Agent target claims keyed by Session, target kind, and local identity.
    AgentTargetClaims,
    /// Agent elicitation currents keyed by protocol storage identity.
    AgentElicitations,
    /// Agent occurrence currents keyed by protocol storage identity.
    AgentOccurrences,
    /// Per-Session unresolved Agent occurrence indexes.
    AgentUnresolvedOccurrenceIndexes,
    /// Agent stream currents keyed by protocol storage identity.
    AgentStreams,
    /// Immutable Agent stream chunks keyed by protocol storage identity.
    AgentStreamChunks,
    /// Per-Session open Agent stream indexes.
    AgentOpenStreamIndexes,
    /// Bounded scalar Evolution partition currents.
    EvolutionCurrents,
    /// Exact all-ever Evolution command aliases.
    EvolutionCommandAliases,
    /// Exact all-ever Evolution persistence receipts.
    EvolutionReceipts,
    /// Evolution definition-current leaves.
    EvolutionDefinitionCurrent,
    /// Evolution definition-compatible-current leaves.
    EvolutionDefinitionCompatibilityCurrent,
    /// Evolution immutable definition records.
    EvolutionDefinitionRecord,
    /// Evolution dependency-current leaves.
    EvolutionDependencyCurrent,
    /// Evolution template-current leaves.
    EvolutionTemplateCurrent,
    /// Evolution immutable link records.
    EvolutionLinkRecord,
    /// Evolution immutable executable Plan records.
    EvolutionPlanRecord,
    /// Evolution immutable compatibility-edge records.
    EvolutionEdgeRecord,
    /// Evolution rollout-current leaves.
    EvolutionRolloutCurrent,
    /// Evolution bounded rollout-evidence currents.
    EvolutionRolloutEvidenceCurrent,
    /// Evolution immutable rollout decisions.
    EvolutionRolloutDecision,
    /// Evolution occurrence-current leaves.
    EvolutionOccurrenceCurrent,
    /// Evolution deterministic-selection currents.
    EvolutionSelectionCurrent,
    /// Evolution immutable migration records.
    EvolutionMigrationRecord,
    /// Evolution immutable restart records.
    EvolutionRestartRecord,
    /// Evolution immutable shadow records.
    EvolutionShadowRecord,
    /// Evolution shadow-subject currents.
    EvolutionShadowSubjectCurrent,
    /// Evolution immutable observation records.
    EvolutionObservationRecord,
    /// Evolution occurrence-observation currents.
    EvolutionObservationOccurrenceCurrent,
    /// Evolution evidence-owner currents.
    EvolutionEvidenceCurrent,
    /// Evolution completed-decision transition currents.
    EvolutionDecisionTransitionCurrent,
    /// Evolution immutable rollout-transition records.
    EvolutionTransitionRecord,
    /// Bounded scalar Virtual scheduler currents.
    VirtualCurrents,
    /// Exact all-ever Virtual persistence receipts.
    VirtualReceipts,
    /// Virtual region lifecycle leaves.
    VirtualRegions,
    /// Virtual materializable-region ordered authority.
    VirtualActiveRegions,
    /// Virtual parked-work leaves.
    VirtualParked,
    /// Virtual bounded parked-reason index pages.
    VirtualParkedIndexes,
    /// Virtual hot work and fence leaves.
    VirtualWork,
    /// Virtual hot occurrence leaves.
    VirtualOccurrences,
    /// Virtual Run fairness leaves.
    VirtualRuns,
    /// Virtual immutable migration receipts.
    VirtualMigrations,
    /// Virtual active or retired compaction certificates.
    VirtualCertificates,
    /// Immutable cross-profile Resource catalog records.
    ResourceCatalogRecords,
}

impl StateRootFamily {
    /// Every family in manifest field order.
    pub const ALL: [Self; 88] = [
        Self::MachinePlans,
        Self::MachineArtifacts,
        Self::MachineCommands,
        Self::MachineCommandBatches,
        Self::Continuations,
        Self::RunCurrents,
        Self::RunQueryIndexes,
        Self::Waits,
        Self::PendingSignalSources,
        Self::PendingTimerSources,
        Self::WaitActivations,
        Self::CancellationReceipts,
        Self::EffectResolutionReceipts,
        Self::Leases,
        Self::Outbox,
        Self::ComponentOccurrences,
        Self::OperationAttempts,
        Self::ClockObservations,
        Self::Snapshots,
        Self::HistoryCompactions,
        Self::ApplicationJournals,
        Self::ApplicationJournalPrefixReplacements,
        Self::ApplicationJournalRecordManifests,
        Self::ApplicationJournalPrefixReplacementHistory,
        Self::CoupledCheckpointReceipts,
        Self::ResourceCommandReceipts,
        Self::ResourceRetentionCurrent,
        Self::ResourcePinCurrent,
        Self::ResourceDeleteCurrent,
        Self::ResourceHandoffCurrent,
        Self::ResourceHandoffSlots,
        Self::ResourceHandoffActivationCurrent,
        Self::ResourceHandoffActivationsByTransfer,
        Self::ResourceHandoffIndexes,
        Self::ResourceHandoffActivationIndexes,
        Self::AgentCommands,
        Self::AgentCommandReceipts,
        Self::AgentInputSuspensionReceipts,
        Self::AgentInputCompletionReceipts,
        Self::AgentSessions,
        Self::AgentUpdates,
        Self::AgentMessages,
        Self::AgentMessageIndexes,
        Self::AgentTools,
        Self::AgentTargetClaims,
        Self::AgentElicitations,
        Self::AgentOccurrences,
        Self::AgentUnresolvedOccurrenceIndexes,
        Self::AgentStreams,
        Self::AgentStreamChunks,
        Self::AgentOpenStreamIndexes,
        Self::EvolutionCurrents,
        Self::EvolutionCommandAliases,
        Self::EvolutionReceipts,
        Self::EvolutionDefinitionCurrent,
        Self::EvolutionDefinitionCompatibilityCurrent,
        Self::EvolutionDefinitionRecord,
        Self::EvolutionDependencyCurrent,
        Self::EvolutionTemplateCurrent,
        Self::EvolutionLinkRecord,
        Self::EvolutionPlanRecord,
        Self::EvolutionEdgeRecord,
        Self::EvolutionRolloutCurrent,
        Self::EvolutionRolloutEvidenceCurrent,
        Self::EvolutionRolloutDecision,
        Self::EvolutionOccurrenceCurrent,
        Self::EvolutionSelectionCurrent,
        Self::EvolutionMigrationRecord,
        Self::EvolutionRestartRecord,
        Self::EvolutionShadowRecord,
        Self::EvolutionShadowSubjectCurrent,
        Self::EvolutionObservationRecord,
        Self::EvolutionObservationOccurrenceCurrent,
        Self::EvolutionEvidenceCurrent,
        Self::EvolutionDecisionTransitionCurrent,
        Self::EvolutionTransitionRecord,
        Self::VirtualCurrents,
        Self::VirtualReceipts,
        Self::VirtualRegions,
        Self::VirtualActiveRegions,
        Self::VirtualParked,
        Self::VirtualParkedIndexes,
        Self::VirtualWork,
        Self::VirtualOccurrences,
        Self::VirtualRuns,
        Self::VirtualMigrations,
        Self::VirtualCertificates,
        Self::ResourceCatalogRecords,
    ];

    /// Legacy families traversed only by the explicit full semantic audit.
    ///
    /// Ordinary reopen, commands, and queries resolve their closed read sets
    /// from the pinned manifest by exact key or bounded authenticated page;
    /// they must never materialize this collection set.
    const FULL_AUDIT: [Self; 18] = [
        Self::MachinePlans,
        Self::MachineArtifacts,
        Self::MachineCommands,
        Self::MachineCommandBatches,
        Self::Continuations,
        Self::Waits,
        Self::WaitActivations,
        Self::CancellationReceipts,
        Self::EffectResolutionReceipts,
        Self::Leases,
        Self::Outbox,
        Self::ComponentOccurrences,
        Self::OperationAttempts,
        Self::ClockObservations,
        Self::Snapshots,
        Self::HistoryCompactions,
        Self::ApplicationJournals,
        Self::ApplicationJournalPrefixReplacements,
    ];

    const fn expected_leaf_kind(self) -> Option<StateRootLeafKind> {
        match self {
            Self::MachineCommands
            | Self::RunQueryIndexes
            | Self::PendingSignalSources
            | Self::PendingTimerSources
            | Self::ApplicationJournals
            | Self::ApplicationJournalRecordManifests
            | Self::ResourceHandoffSlots
            | Self::ResourceHandoffIndexes
            | Self::ResourceHandoffActivationIndexes
            | Self::AgentMessageIndexes
            | Self::AgentUnresolvedOccurrenceIndexes
            | Self::AgentOpenStreamIndexes => None,
            Self::MachinePlans => Some(StateRootLeafKind::MachinePlan),
            Self::MachineArtifacts => Some(StateRootLeafKind::MachineArtifact),
            Self::MachineCommandBatches => Some(StateRootLeafKind::MachineCommandBatch),
            Self::Continuations => Some(StateRootLeafKind::Continuation),
            Self::RunCurrents => Some(StateRootLeafKind::RunCurrent),
            Self::Waits => Some(StateRootLeafKind::Wait),
            Self::WaitActivations => Some(StateRootLeafKind::WaitActivation),
            Self::CancellationReceipts => Some(StateRootLeafKind::CancellationReceipt),
            Self::EffectResolutionReceipts => Some(StateRootLeafKind::EffectResolutionReceipt),
            Self::Leases => Some(StateRootLeafKind::Lease),
            Self::Outbox => Some(StateRootLeafKind::OutboxOwner),
            Self::ComponentOccurrences => Some(StateRootLeafKind::ComponentOccurrence),
            Self::OperationAttempts => Some(StateRootLeafKind::OperationAttempt),
            Self::ClockObservations => Some(StateRootLeafKind::ClockObservation),
            Self::Snapshots => Some(StateRootLeafKind::Snapshot),
            Self::HistoryCompactions => Some(StateRootLeafKind::HistoryCompaction),
            Self::ApplicationJournalPrefixReplacements => {
                Some(StateRootLeafKind::JournalPrefixReplacement)
            }
            Self::ApplicationJournalPrefixReplacementHistory => {
                Some(StateRootLeafKind::JournalPrefixReplacementAuthority)
            }
            Self::CoupledCheckpointReceipts => Some(StateRootLeafKind::CoupledCheckpointReceipt),
            Self::ResourceCommandReceipts => Some(StateRootLeafKind::ResourceCommandReceipt),
            Self::ResourceRetentionCurrent => Some(StateRootLeafKind::ResourceRetentionCurrent),
            Self::ResourcePinCurrent => Some(StateRootLeafKind::ResourcePinCurrent),
            Self::ResourceDeleteCurrent => Some(StateRootLeafKind::ResourceDeleteCurrent),
            Self::ResourceHandoffCurrent => Some(StateRootLeafKind::ResourceHandoffCurrent),
            Self::ResourceHandoffActivationCurrent | Self::ResourceHandoffActivationsByTransfer => {
                Some(StateRootLeafKind::ResourceHandoffActivationCurrent)
            }
            Self::AgentCommands => Some(StateRootLeafKind::AgentCommand),
            Self::AgentCommandReceipts => Some(StateRootLeafKind::AgentCommandReceipt),
            Self::AgentInputSuspensionReceipts => {
                Some(StateRootLeafKind::AgentInputSuspensionReceipt)
            }
            Self::AgentInputCompletionReceipts => {
                Some(StateRootLeafKind::AgentInputCompletionReceipt)
            }
            Self::AgentSessions => Some(StateRootLeafKind::AgentSessionCurrent),
            Self::AgentUpdates => Some(StateRootLeafKind::AgentUpdateCurrent),
            Self::AgentMessages => Some(StateRootLeafKind::AgentMessageCurrent),
            Self::AgentTools => Some(StateRootLeafKind::AgentToolCurrent),
            Self::AgentTargetClaims => Some(StateRootLeafKind::AgentTargetClaimCurrent),
            Self::AgentElicitations => Some(StateRootLeafKind::AgentElicitationCurrent),
            Self::AgentOccurrences => Some(StateRootLeafKind::AgentOccurrenceCurrent),
            Self::AgentStreams => Some(StateRootLeafKind::AgentStreamCurrent),
            Self::AgentStreamChunks => Some(StateRootLeafKind::AgentStreamChunkCurrent),
            Self::EvolutionCurrents => Some(StateRootLeafKind::EvolutionCurrent),
            Self::EvolutionCommandAliases => Some(StateRootLeafKind::EvolutionCommandAlias),
            Self::EvolutionReceipts => Some(StateRootLeafKind::EvolutionPersistenceReceipt),
            Self::EvolutionDefinitionCurrent
            | Self::EvolutionDefinitionCompatibilityCurrent
            | Self::EvolutionDefinitionRecord
            | Self::EvolutionDependencyCurrent
            | Self::EvolutionTemplateCurrent
            | Self::EvolutionLinkRecord
            | Self::EvolutionPlanRecord
            | Self::EvolutionEdgeRecord
            | Self::EvolutionRolloutCurrent
            | Self::EvolutionRolloutEvidenceCurrent
            | Self::EvolutionRolloutDecision
            | Self::EvolutionOccurrenceCurrent
            | Self::EvolutionSelectionCurrent
            | Self::EvolutionMigrationRecord
            | Self::EvolutionRestartRecord
            | Self::EvolutionShadowRecord
            | Self::EvolutionShadowSubjectCurrent
            | Self::EvolutionObservationRecord
            | Self::EvolutionObservationOccurrenceCurrent
            | Self::EvolutionEvidenceCurrent
            | Self::EvolutionDecisionTransitionCurrent
            | Self::EvolutionTransitionRecord => Some(StateRootLeafKind::EvolutionMutation),
            Self::VirtualCurrents => Some(StateRootLeafKind::VirtualCurrent),
            Self::VirtualReceipts => Some(StateRootLeafKind::VirtualPersistenceReceipt),
            Self::VirtualRegions
            | Self::VirtualActiveRegions
            | Self::VirtualParked
            | Self::VirtualParkedIndexes
            | Self::VirtualWork
            | Self::VirtualOccurrences
            | Self::VirtualRuns
            | Self::VirtualMigrations
            | Self::VirtualCertificates => Some(StateRootLeafKind::VirtualStateLeaf),
            Self::ResourceCatalogRecords => Some(StateRootLeafKind::ResourceCatalogRecord),
        }
    }
}

/// Fixed normalized M4 Evolution root set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionRootSet {
    /// Bounded scalar partition currents.
    pub currents: MapRoot,
    /// Exact all-ever command aliases.
    pub command_aliases: MapRoot,
    /// Exact all-ever semantic receipts.
    pub receipts: MapRoot,
    /// Latest definition revision by logical reference.
    pub definition_current: MapRoot,
    /// Latest compatible definition revision by logical reference and contract.
    pub definition_compatibility_current: MapRoot,
    /// Immutable definition revisions.
    pub definition_record: MapRoot,
    /// Definition dependency indexes.
    pub dependency_current: MapRoot,
    /// Current linked template state.
    pub template_current: MapRoot,
    /// Immutable link records.
    pub link_record: MapRoot,
    /// Immutable executable Plan records.
    pub plan_record: MapRoot,
    /// Immutable compatibility edges.
    pub edge_record: MapRoot,
    /// Current rollout decisions.
    pub rollout_current: MapRoot,
    /// Bounded rollout evidence aggregates.
    pub rollout_evidence_current: MapRoot,
    /// Immutable rollout decisions.
    pub rollout_decision: MapRoot,
    /// Exact occurrence pins.
    pub occurrence_current: MapRoot,
    /// Deterministic selection reverse indexes.
    pub selection_current: MapRoot,
    /// Immutable migration records.
    pub migration_record: MapRoot,
    /// Immutable restart records.
    pub restart_record: MapRoot,
    /// Immutable shadow records.
    pub shadow_record: MapRoot,
    /// Shadow-subject reverse indexes.
    pub shadow_subject_current: MapRoot,
    /// Immutable observation records.
    pub observation_record: MapRoot,
    /// Occurrence-observation reverse indexes.
    pub observation_occurrence_current: MapRoot,
    /// Cross-family evidence owners.
    pub evidence_current: MapRoot,
    /// Completed-decision transition currents.
    pub decision_transition_current: MapRoot,
    /// Immutable rollout-transition records.
    pub transition_record: MapRoot,
}

impl EvolutionRootSet {
    fn state(&self, family: cymule_profile_protocol::evolution::EvolutionStateFamily) -> &MapRoot {
        use cymule_profile_protocol::evolution::EvolutionStateFamily as Family;
        match family {
            Family::DefinitionCurrent => &self.definition_current,
            Family::DefinitionCompatibilityCurrent => &self.definition_compatibility_current,
            Family::DefinitionRecord => &self.definition_record,
            Family::DependencyCurrent => &self.dependency_current,
            Family::TemplateCurrent => &self.template_current,
            Family::LinkRecord => &self.link_record,
            Family::PlanRecord => &self.plan_record,
            Family::EdgeRecord => &self.edge_record,
            Family::RolloutCurrent => &self.rollout_current,
            Family::RolloutEvidenceCurrent => &self.rollout_evidence_current,
            Family::RolloutDecision => &self.rollout_decision,
            Family::OccurrenceCurrent => &self.occurrence_current,
            Family::SelectionCurrent => &self.selection_current,
            Family::MigrationRecord => &self.migration_record,
            Family::RestartRecord => &self.restart_record,
            Family::ShadowRecord => &self.shadow_record,
            Family::ShadowSubjectCurrent => &self.shadow_subject_current,
            Family::ObservationRecord => &self.observation_record,
            Family::ObservationOccurrenceCurrent => &self.observation_occurrence_current,
            Family::EvidenceCurrent => &self.evidence_current,
            Family::DecisionTransitionCurrent => &self.decision_transition_current,
            Family::TransitionRecord => &self.transition_record,
        }
    }

    fn state_mut(
        &mut self,
        family: cymule_profile_protocol::evolution::EvolutionStateFamily,
    ) -> &mut MapRoot {
        use cymule_profile_protocol::evolution::EvolutionStateFamily as Family;
        match family {
            Family::DefinitionCurrent => &mut self.definition_current,
            Family::DefinitionCompatibilityCurrent => &mut self.definition_compatibility_current,
            Family::DefinitionRecord => &mut self.definition_record,
            Family::DependencyCurrent => &mut self.dependency_current,
            Family::TemplateCurrent => &mut self.template_current,
            Family::LinkRecord => &mut self.link_record,
            Family::PlanRecord => &mut self.plan_record,
            Family::EdgeRecord => &mut self.edge_record,
            Family::RolloutCurrent => &mut self.rollout_current,
            Family::RolloutEvidenceCurrent => &mut self.rollout_evidence_current,
            Family::RolloutDecision => &mut self.rollout_decision,
            Family::OccurrenceCurrent => &mut self.occurrence_current,
            Family::SelectionCurrent => &mut self.selection_current,
            Family::MigrationRecord => &mut self.migration_record,
            Family::RestartRecord => &mut self.restart_record,
            Family::ShadowRecord => &mut self.shadow_record,
            Family::ShadowSubjectCurrent => &mut self.shadow_subject_current,
            Family::ObservationRecord => &mut self.observation_record,
            Family::ObservationOccurrenceCurrent => &mut self.observation_occurrence_current,
            Family::EvidenceCurrent => &mut self.evidence_current,
            Family::DecisionTransitionCurrent => &mut self.decision_transition_current,
            Family::TransitionRecord => &mut self.transition_record,
        }
    }
}

/// Fixed normalized M3 Virtual-work root set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualRootSet {
    /// Bounded scalar current per scheduler.
    pub currents: MapRoot,
    /// Exact all-ever semantic receipt per scheduler and command.
    pub receipts: MapRoot,
    /// Region lifecycle and cursor authority.
    pub regions: MapRoot,
    /// Exactly materializable regions in authenticated map order.
    pub active_regions: MapRoot,
    /// Parked work by exact work identity.
    pub parked: MapRoot,
    /// Bounded parked-reason index pages.
    pub parked_indexes: MapRoot,
    /// Hot work identity and latest fence.
    pub work: MapRoot,
    /// Hot exact occurrences.
    pub occurrences: MapRoot,
    /// Run fairness state.
    pub runs: MapRoot,
    /// Applied migration receipts.
    pub migrations: MapRoot,
    /// Active or retired compaction certificates.
    pub certificates: MapRoot,
}

impl VirtualRootSet {
    fn state(&self, family: cymule_profile_protocol::virtual_work::VirtualStateFamily) -> &MapRoot {
        use cymule_profile_protocol::virtual_work::VirtualStateFamily as Family;
        match family {
            Family::Regions => &self.regions,
            Family::ActiveRegions => &self.active_regions,
            Family::Parked => &self.parked,
            Family::ParkedIndex => &self.parked_indexes,
            Family::Work => &self.work,
            Family::Occurrences => &self.occurrences,
            Family::Runs => &self.runs,
            Family::Migrations => &self.migrations,
            Family::Certificates => &self.certificates,
        }
    }

    fn state_mut(
        &mut self,
        family: cymule_profile_protocol::virtual_work::VirtualStateFamily,
    ) -> &mut MapRoot {
        use cymule_profile_protocol::virtual_work::VirtualStateFamily as Family;
        match family {
            Family::Regions => &mut self.regions,
            Family::ActiveRegions => &mut self.active_regions,
            Family::Parked => &mut self.parked,
            Family::ParkedIndex => &mut self.parked_indexes,
            Family::Work => &mut self.work,
            Family::Occurrences => &mut self.occurrences,
            Family::Runs => &mut self.runs,
            Family::Migrations => &mut self.migrations,
            Family::Certificates => &mut self.certificates,
        }
    }
}

/// Fixed collection-root set for one complete durable projection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateRoots {
    /// Sealed Machine Plans.
    pub machine_plans: MapRoot,
    /// Exact unique Machine Plan admission order.
    pub machine_plan_admissions: LogRoot,
    /// Immutable Machine Artifacts.
    pub machine_artifacts: MapRoot,
    /// Exact unique Machine Artifact admission order.
    pub machine_artifact_admissions: LogRoot,
    /// Optional authenticated Machine base value-object identity.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub machine_base: Option<String>,
    /// Ordered hot Machine Events.
    pub machine_events: LogRoot,
    /// Ordered hot Machine command admissions.
    pub machine_admissions: LogRoot,
    /// Hot Machine command records.
    pub machine_commands: MapRoot,
    /// Hot atomic Machine command-batch records.
    pub machine_command_batches: MapRoot,
    /// Exact hot Machine command-batch admission order.
    pub machine_command_batch_admissions: LogRoot,
    /// Continuations.
    pub continuations: MapRoot,
    /// Bounded semantic Run currents.
    pub run_currents: MapRoot,
    /// Per-Run nested query-index roots.
    pub run_query_indexes: MapRoot,
    /// Waits.
    pub waits: MapRoot,
    /// Pageable signal-source descriptors with pending Waits.
    pub pending_signal_sources: MapRoot,
    /// Exact timer-source descriptors with pending Waits.
    pub pending_timer_sources: MapRoot,
    /// Wait activations.
    pub wait_activations: MapRoot,
    /// Exact immutable Run cancellation receipts.
    pub cancellation_receipts: MapRoot,
    /// Exact immutable terminal Effect-resolution receipts.
    pub effect_resolution_receipts: MapRoot,
    /// Coordination leases.
    pub leases: MapRoot,
    /// Immutable Effect-to-Run locators; current dispatches are Run-local.
    pub outbox: MapRoot,
    /// Component occurrences.
    pub component_occurrences: MapRoot,
    /// Operation Attempts.
    pub operation_attempts: MapRoot,
    /// Clock observations.
    pub clock_observations: MapRoot,
    /// Portable snapshots.
    pub snapshots: MapRoot,
    /// Machine-history compactions.
    pub history_compactions: MapRoot,
    /// Exact latest compaction receipt value, null only before the first base.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub history_compaction_head: Option<String>,
    /// Application-journal log descriptors.
    pub application_journals: MapRoot,
    /// Latest journal prefix replacements.
    pub application_journal_prefix_replacements: MapRoot,
    /// Nested all-ever record-manifest roots.
    pub application_journal_record_manifests: MapRoot,
    /// Journal prefix replacement history.
    pub application_journal_prefix_replacement_history: MapRoot,
    /// Coupled checkpoint receipts.
    pub coupled_checkpoint_receipts: MapRoot,
    /// Exact all-ever Resource command receipts and authority aliases.
    pub resource_command_receipts: MapRoot,
    /// Current keyed physical Resource-retention projections.
    pub resource_retention_current: MapRoot,
    /// Current keyed exact Resource-pin projections.
    pub resource_pin_current: MapRoot,
    /// Current keyed Resource-deletion projections.
    pub resource_delete_current: MapRoot,
    /// Current immutable Resource-handoff authorities.
    pub resource_handoff_current: MapRoot,
    /// Exact target-slot Resource-handoff entries.
    pub resource_handoff_slots: MapRoot,
    /// Current immutable Resource-handoff activation authorities.
    pub resource_handoff_activation_current: MapRoot,
    /// Immutable Resource-handoff activations keyed by source transfer.
    pub resource_handoff_activations_by_transfer: MapRoot,
    /// Per-target ordered Resource-handoff indexes.
    pub resource_handoff_indexes: MapRoot,
    /// Per-target ordered Resource-handoff activation indexes.
    pub resource_handoff_activation_indexes: MapRoot,
    /// Exact all-ever Agent persistence commands.
    pub agent_commands: MapRoot,
    /// Exact all-ever Agent persistence receipts keyed by command identity.
    pub agent_command_receipts: MapRoot,
    /// Durable-private Agent input suspension receipts keyed by Wait identity.
    pub agent_input_suspension_receipts: MapRoot,
    /// Durable-private Agent input completion receipts keyed by Wait identity.
    pub agent_input_completion_receipts: MapRoot,
    /// Bounded Agent Session metadata.
    pub agent_sessions: MapRoot,
    /// Agent update idempotency authorities.
    pub agent_updates: MapRoot,
    /// Immutable Agent messages.
    pub agent_messages: MapRoot,
    /// Per-Session ordered Agent message indexes.
    pub agent_message_indexes: MapRoot,
    /// Agent tool-call currents.
    pub agent_tools: MapRoot,
    /// Exact generation-bearing Agent target claims.
    pub agent_target_claims: MapRoot,
    /// Agent elicitation currents.
    pub agent_elicitations: MapRoot,
    /// Agent occurrence currents.
    pub agent_occurrences: MapRoot,
    /// Per-Session unresolved Agent occurrence indexes.
    pub agent_unresolved_occurrence_indexes: MapRoot,
    /// Agent stream currents.
    pub agent_streams: MapRoot,
    /// Immutable Agent stream chunks.
    pub agent_stream_chunks: MapRoot,
    /// Per-Session open Agent stream indexes.
    pub agent_open_stream_indexes: MapRoot,
    /// Fixed normalized M4 Evolution roots.
    pub evolution: EvolutionRootSet,
    /// Fixed normalized M3 Virtual-work roots.
    pub virtual_work: VirtualRootSet,
    /// Immutable cross-profile Resource catalog records.
    pub resource_catalog_records: MapRoot,
}

impl StateRoots {
    /// Construct the unique all-empty root set.
    pub fn empty() -> Self {
        Self {
            machine_plans: MapRoot::empty(),
            machine_plan_admissions: LogRoot::empty(),
            machine_artifacts: MapRoot::empty(),
            machine_artifact_admissions: LogRoot::empty(),
            machine_base: None,
            machine_events: LogRoot::empty(),
            machine_admissions: LogRoot::empty(),
            machine_commands: MapRoot::empty(),
            machine_command_batches: MapRoot::empty(),
            machine_command_batch_admissions: LogRoot::empty(),
            continuations: MapRoot::empty(),
            run_currents: MapRoot::empty(),
            run_query_indexes: MapRoot::empty(),
            waits: MapRoot::empty(),
            pending_signal_sources: MapRoot::empty(),
            pending_timer_sources: MapRoot::empty(),
            wait_activations: MapRoot::empty(),
            cancellation_receipts: MapRoot::empty(),
            effect_resolution_receipts: MapRoot::empty(),
            leases: MapRoot::empty(),
            outbox: MapRoot::empty(),
            component_occurrences: MapRoot::empty(),
            operation_attempts: MapRoot::empty(),
            clock_observations: MapRoot::empty(),
            snapshots: MapRoot::empty(),
            history_compactions: MapRoot::empty(),
            history_compaction_head: None,
            application_journals: MapRoot::empty(),
            application_journal_prefix_replacements: MapRoot::empty(),
            application_journal_record_manifests: MapRoot::empty(),
            application_journal_prefix_replacement_history: MapRoot::empty(),
            coupled_checkpoint_receipts: MapRoot::empty(),
            resource_command_receipts: MapRoot::empty(),
            resource_retention_current: MapRoot::empty(),
            resource_pin_current: MapRoot::empty(),
            resource_delete_current: MapRoot::empty(),
            resource_handoff_current: MapRoot::empty(),
            resource_handoff_slots: MapRoot::empty(),
            resource_handoff_activation_current: MapRoot::empty(),
            resource_handoff_activations_by_transfer: MapRoot::empty(),
            resource_handoff_indexes: MapRoot::empty(),
            resource_handoff_activation_indexes: MapRoot::empty(),
            agent_commands: MapRoot::empty(),
            agent_command_receipts: MapRoot::empty(),
            agent_input_suspension_receipts: MapRoot::empty(),
            agent_input_completion_receipts: MapRoot::empty(),
            agent_sessions: MapRoot::empty(),
            agent_updates: MapRoot::empty(),
            agent_messages: MapRoot::empty(),
            agent_message_indexes: MapRoot::empty(),
            agent_tools: MapRoot::empty(),
            agent_target_claims: MapRoot::empty(),
            agent_elicitations: MapRoot::empty(),
            agent_occurrences: MapRoot::empty(),
            agent_unresolved_occurrence_indexes: MapRoot::empty(),
            agent_streams: MapRoot::empty(),
            agent_stream_chunks: MapRoot::empty(),
            agent_open_stream_indexes: MapRoot::empty(),
            evolution: EvolutionRootSet::default(),
            virtual_work: VirtualRootSet::default(),
            resource_catalog_records: MapRoot::empty(),
        }
    }

    /// Borrow one closed collection root.
    pub fn get(&self, family: StateRootFamily) -> &MapRoot {
        use StateRootFamily as Family;
        use cymule_profile_protocol::virtual_work::VirtualStateFamily as Virtual;

        match family {
            Family::MachinePlans => &self.machine_plans,
            Family::MachineArtifacts => &self.machine_artifacts,
            Family::MachineCommands => &self.machine_commands,
            Family::MachineCommandBatches => &self.machine_command_batches,
            Family::Continuations => &self.continuations,
            Family::RunCurrents => &self.run_currents,
            Family::RunQueryIndexes => &self.run_query_indexes,
            Family::Waits => &self.waits,
            Family::PendingSignalSources => &self.pending_signal_sources,
            Family::PendingTimerSources => &self.pending_timer_sources,
            Family::WaitActivations => &self.wait_activations,
            Family::CancellationReceipts => &self.cancellation_receipts,
            Family::EffectResolutionReceipts => &self.effect_resolution_receipts,
            Family::Leases => &self.leases,
            Family::Outbox => &self.outbox,
            Family::ComponentOccurrences => &self.component_occurrences,
            Family::OperationAttempts => &self.operation_attempts,
            Family::ClockObservations => &self.clock_observations,
            Family::Snapshots => &self.snapshots,
            Family::HistoryCompactions => &self.history_compactions,
            Family::ApplicationJournals => &self.application_journals,
            Family::ApplicationJournalPrefixReplacements => {
                &self.application_journal_prefix_replacements
            }
            Family::ApplicationJournalRecordManifests => &self.application_journal_record_manifests,
            Family::ApplicationJournalPrefixReplacementHistory => {
                &self.application_journal_prefix_replacement_history
            }
            Family::CoupledCheckpointReceipts => &self.coupled_checkpoint_receipts,
            Family::ResourceCommandReceipts => &self.resource_command_receipts,
            Family::ResourceRetentionCurrent => &self.resource_retention_current,
            Family::ResourcePinCurrent => &self.resource_pin_current,
            Family::ResourceDeleteCurrent => &self.resource_delete_current,
            Family::ResourceHandoffCurrent => &self.resource_handoff_current,
            Family::ResourceHandoffSlots => &self.resource_handoff_slots,
            Family::ResourceHandoffActivationCurrent => &self.resource_handoff_activation_current,
            Family::ResourceHandoffActivationsByTransfer => {
                &self.resource_handoff_activations_by_transfer
            }
            Family::ResourceHandoffIndexes => &self.resource_handoff_indexes,
            Family::ResourceHandoffActivationIndexes => &self.resource_handoff_activation_indexes,
            Family::AgentCommands => &self.agent_commands,
            Family::AgentCommandReceipts => &self.agent_command_receipts,
            Family::AgentInputSuspensionReceipts => &self.agent_input_suspension_receipts,
            Family::AgentInputCompletionReceipts => &self.agent_input_completion_receipts,
            Family::AgentSessions => &self.agent_sessions,
            Family::AgentUpdates => &self.agent_updates,
            Family::AgentMessages => &self.agent_messages,
            Family::AgentMessageIndexes => &self.agent_message_indexes,
            Family::AgentTools => &self.agent_tools,
            Family::AgentTargetClaims => &self.agent_target_claims,
            Family::AgentElicitations => &self.agent_elicitations,
            Family::AgentOccurrences => &self.agent_occurrences,
            Family::AgentUnresolvedOccurrenceIndexes => &self.agent_unresolved_occurrence_indexes,
            Family::AgentStreams => &self.agent_streams,
            Family::AgentStreamChunks => &self.agent_stream_chunks,
            Family::AgentOpenStreamIndexes => &self.agent_open_stream_indexes,
            Family::EvolutionCurrents
            | Family::EvolutionCommandAliases
            | Family::EvolutionReceipts
            | Family::EvolutionDefinitionCurrent
            | Family::EvolutionDefinitionCompatibilityCurrent
            | Family::EvolutionDefinitionRecord
            | Family::EvolutionDependencyCurrent
            | Family::EvolutionTemplateCurrent
            | Family::EvolutionLinkRecord
            | Family::EvolutionPlanRecord
            | Family::EvolutionEdgeRecord
            | Family::EvolutionRolloutCurrent
            | Family::EvolutionRolloutEvidenceCurrent
            | Family::EvolutionRolloutDecision
            | Family::EvolutionOccurrenceCurrent
            | Family::EvolutionSelectionCurrent
            | Family::EvolutionMigrationRecord
            | Family::EvolutionRestartRecord
            | Family::EvolutionShadowRecord
            | Family::EvolutionShadowSubjectCurrent
            | Family::EvolutionObservationRecord
            | Family::EvolutionObservationOccurrenceCurrent
            | Family::EvolutionEvidenceCurrent
            | Family::EvolutionDecisionTransitionCurrent
            | Family::EvolutionTransitionRecord => self.evolution_root(family),
            Family::VirtualCurrents => &self.virtual_work.currents,
            Family::VirtualReceipts => &self.virtual_work.receipts,
            Family::VirtualRegions => self.virtual_work.state(Virtual::Regions),
            Family::VirtualActiveRegions => self.virtual_work.state(Virtual::ActiveRegions),
            Family::VirtualParked => self.virtual_work.state(Virtual::Parked),
            Family::VirtualParkedIndexes => self.virtual_work.state(Virtual::ParkedIndex),
            Family::VirtualWork => self.virtual_work.state(Virtual::Work),
            Family::VirtualOccurrences => self.virtual_work.state(Virtual::Occurrences),
            Family::VirtualRuns => self.virtual_work.state(Virtual::Runs),
            Family::VirtualMigrations => self.virtual_work.state(Virtual::Migrations),
            Family::VirtualCertificates => self.virtual_work.state(Virtual::Certificates),
            Family::ResourceCatalogRecords => &self.resource_catalog_records,
        }
    }

    fn evolution_root(&self, family: StateRootFamily) -> &MapRoot {
        use StateRootFamily as Family;
        use cymule_profile_protocol::evolution::EvolutionStateFamily as Evo;

        match family {
            Family::EvolutionCurrents => &self.evolution.currents,
            Family::EvolutionCommandAliases => &self.evolution.command_aliases,
            Family::EvolutionReceipts => &self.evolution.receipts,
            Family::EvolutionDefinitionCurrent => self.evolution.state(Evo::DefinitionCurrent),
            Family::EvolutionDefinitionCompatibilityCurrent => {
                self.evolution.state(Evo::DefinitionCompatibilityCurrent)
            }
            Family::EvolutionDefinitionRecord => self.evolution.state(Evo::DefinitionRecord),
            Family::EvolutionDependencyCurrent => self.evolution.state(Evo::DependencyCurrent),
            Family::EvolutionTemplateCurrent => self.evolution.state(Evo::TemplateCurrent),
            Family::EvolutionLinkRecord => self.evolution.state(Evo::LinkRecord),
            Family::EvolutionPlanRecord => self.evolution.state(Evo::PlanRecord),
            Family::EvolutionEdgeRecord => self.evolution.state(Evo::EdgeRecord),
            Family::EvolutionRolloutCurrent => self.evolution.state(Evo::RolloutCurrent),
            Family::EvolutionRolloutEvidenceCurrent => {
                self.evolution.state(Evo::RolloutEvidenceCurrent)
            }
            Family::EvolutionRolloutDecision => self.evolution.state(Evo::RolloutDecision),
            Family::EvolutionOccurrenceCurrent => self.evolution.state(Evo::OccurrenceCurrent),
            Family::EvolutionSelectionCurrent => self.evolution.state(Evo::SelectionCurrent),
            Family::EvolutionMigrationRecord => self.evolution.state(Evo::MigrationRecord),
            Family::EvolutionRestartRecord => self.evolution.state(Evo::RestartRecord),
            Family::EvolutionShadowRecord => self.evolution.state(Evo::ShadowRecord),
            Family::EvolutionShadowSubjectCurrent => {
                self.evolution.state(Evo::ShadowSubjectCurrent)
            }
            Family::EvolutionObservationRecord => self.evolution.state(Evo::ObservationRecord),
            Family::EvolutionObservationOccurrenceCurrent => {
                self.evolution.state(Evo::ObservationOccurrenceCurrent)
            }
            Family::EvolutionEvidenceCurrent => self.evolution.state(Evo::EvidenceCurrent),
            Family::EvolutionDecisionTransitionCurrent => {
                self.evolution.state(Evo::DecisionTransitionCurrent)
            }
            Family::EvolutionTransitionRecord => self.evolution.state(Evo::TransitionRecord),
            _ => unreachable!("Evolution family was selected by get"),
        }
    }

    fn verify(&self) -> DurableResult<()> {
        for family in StateRootFamily::ALL {
            self.get(family).verify()?;
        }
        if let Some(base) = &self.machine_base {
            cymule_core::validate_content_id("Machine base state-root value", base)?;
        }
        if let Some(head) = &self.history_compaction_head {
            cymule_core::validate_content_id("Machine compaction receipt head", head)?;
        }
        if self.machine_base.is_some() != self.history_compaction_head.is_some()
            || self.history_compaction_head.is_some() != (self.history_compactions.entries != 0)
        {
            return Err(DurableError::Integrity {
                code: "state_root_history_compaction_head_presence_mismatch".to_owned(),
                message:
                    "Machine base, compaction receipt map, and receipt head must appear together"
                        .to_owned(),
            });
        }
        self.machine_events.verify()?;
        self.machine_admissions.verify()?;
        self.machine_command_batch_admissions.verify()?;
        self.machine_plan_admissions.verify()?;
        self.machine_artifact_admissions.verify()?;
        Ok(())
    }

    fn root_object_ids(&self) -> Vec<String> {
        let mut ids = StateRootFamily::ALL
            .into_iter()
            .filter_map(|family| self.get(family).node.clone())
            .collect::<Vec<_>>();
        ids.extend(self.machine_base.iter().cloned());
        ids.extend(self.history_compaction_head.iter().cloned());
        ids.extend(self.machine_events.node.iter().cloned());
        ids.extend(self.machine_admissions.node.iter().cloned());
        ids.extend(self.machine_command_batch_admissions.node.iter().cloned());
        ids.extend(self.machine_plan_admissions.node.iter().cloned());
        ids.extend(self.machine_artifact_admissions.node.iter().cloned());
        ids
    }
}

/// One fixed-size root manifest published by the small Store head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateRootManifest {
    /// Manifest schema.
    pub(crate) manifest_version: String,
    /// Content identity of every following field.
    pub(crate) manifest_id: String,
    /// Exact parent manifest identity; null only at genesis.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) parent_manifest: Option<String>,
    /// Exact parent revision; null only at genesis.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) parent_revision: Option<String>,
    /// Canonical admitted `DurableDelta` digest; null only at genesis.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) delta_digest: Option<String>,
    /// Durable semantic schema represented by these roots.
    pub(crate) durable_version: String,
    /// Exact semantic revision represented by these roots.
    pub(crate) revision: String,
    /// Exact physical commit sequence.
    pub(crate) sequence: u64,
    /// Exact Machine snapshot schema represented by the fixed Machine roots.
    pub(crate) machine_snapshot_version: String,
    /// Fixed exact-load Machine semantic and physical frontier.
    pub(crate) machine_frontier: Box<cymule_core::durable_internal::MachineAuthorityFrontier>,
    /// Exact trusted compacted-Machine anchor.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) machine_base_anchor: Option<cymule_core::MachineBaseAnchor>,
    /// Fixed closed collection roots.
    pub(crate) roots: Box<StateRoots>,
}

pub(crate) struct StateRootManifestMetadata {
    pub(crate) durable_version: String,
    pub(crate) revision: String,
    pub(crate) sequence: u64,
    pub(crate) parent_manifest: Option<String>,
    pub(crate) parent_revision: Option<String>,
    pub(crate) delta_digest: Option<String>,
    pub(crate) machine_snapshot_version: String,
}

impl StateRootManifest {
    /// Manifest wire version.
    pub fn manifest_version(&self) -> &str {
        &self.manifest_version
    }

    /// Exact content identity of this manifest.
    pub fn manifest_id(&self) -> &str {
        &self.manifest_id
    }

    /// Exact parent manifest identity, absent only at genesis.
    pub fn parent_manifest(&self) -> Option<&str> {
        self.parent_manifest.as_deref()
    }

    /// Exact parent semantic revision, absent only at genesis.
    pub fn parent_revision(&self) -> Option<&str> {
        self.parent_revision.as_deref()
    }

    /// Canonical admitted delta digest, absent only at genesis.
    pub fn delta_digest(&self) -> Option<&str> {
        self.delta_digest.as_deref()
    }

    /// Durable semantic schema represented by the roots.
    pub fn durable_version(&self) -> &str {
        &self.durable_version
    }

    /// Exact semantic revision represented by the roots.
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Exact physical commit sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Exact Machine snapshot schema represented by the roots.
    pub fn machine_snapshot_version(&self) -> &str {
        &self.machine_snapshot_version
    }

    /// Exact incremental Machine authority root represented by the roots.
    pub fn machine_authority_root(&self) -> &str {
        &self.machine_frontier.authority_root
    }

    /// Borrow the exact Core-owned Machine semantic and physical frontier.
    pub fn machine_frontier(&self) -> &cymule_core::durable_internal::MachineAuthorityFrontier {
        &self.machine_frontier
    }

    /// Exact trusted compacted-Machine anchor.
    pub fn machine_base_anchor(&self) -> Option<&cymule_core::MachineBaseAnchor> {
        self.machine_base_anchor.as_ref()
    }

    /// Borrow the fixed closed collection roots represented by this manifest.
    pub fn roots(&self) -> &StateRoots {
        &self.roots
    }

    /// Construct and authenticate a fixed root manifest inside the semantic
    /// lowering boundary. Providers only deserialize, verify, and persist it.
    pub(crate) fn new(
        metadata: StateRootManifestMetadata,
        machine_frontier: cymule_core::durable_internal::MachineAuthorityFrontier,
        machine_base_anchor: Option<cymule_core::MachineBaseAnchor>,
        roots: StateRoots,
    ) -> DurableResult<Self> {
        let mut manifest = Self {
            manifest_version: STATE_ROOT_MANIFEST_VERSION.to_owned(),
            manifest_id: String::new(),
            parent_manifest: metadata.parent_manifest,
            parent_revision: metadata.parent_revision,
            delta_digest: metadata.delta_digest,
            durable_version: metadata.durable_version,
            revision: metadata.revision,
            sequence: metadata.sequence,
            machine_snapshot_version: metadata.machine_snapshot_version,
            machine_frontier: Box::new(machine_frontier),
            machine_base_anchor,
            roots: Box::new(roots),
        };
        manifest.manifest_id = manifest.identity()?;
        manifest.verify()?;
        Ok(manifest)
    }

    fn identity(&self) -> DurableResult<String> {
        cymule_core::content_id(
            STATE_ROOT_MANIFEST_VERSION,
            &(
                self.parent_manifest.as_deref(),
                self.parent_revision.as_deref(),
                self.delta_digest.as_deref(),
                self.durable_version.as_str(),
                self.revision.as_str(),
                self.sequence,
                self.machine_snapshot_version.as_str(),
                &self.machine_frontier,
                self.machine_base_anchor.as_ref(),
                &self.roots,
            ),
        )
        .map_err(Into::into)
    }

    /// Verify the closed shape, exact identity, and encoded-size bound.
    ///
    /// # Errors
    ///
    /// Rejects invalid lineage, schemas, roots, identities, or encoded sizes.
    pub fn verify(&self) -> DurableResult<()> {
        if self.manifest_version != STATE_ROOT_MANIFEST_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported durable state-root manifest version {:?}",
                self.manifest_version
            )));
        }
        if self.durable_version != crate::DURABLE_STATE_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported state-root durable version {:?}",
                self.durable_version
            )));
        }
        match (
            self.sequence,
            &self.parent_manifest,
            &self.parent_revision,
            &self.delta_digest,
        ) {
            (0, None, None, None) => {}
            (_, Some(parent_manifest), Some(parent_revision), Some(delta_digest))
                if self.sequence > 0 =>
            {
                cymule_core::validate_content_id("state-root parent manifest", parent_manifest)?;
                cymule_core::validate_content_id("state-root parent revision", parent_revision)?;
                validate_digest("state-root delta digest", delta_digest)?;
            }
            _ => {
                return Err(DurableError::Validation(
                    "state-root manifest has inconsistent genesis or successor lineage".to_owned(),
                ));
            }
        }
        cymule_core::validate_content_id("state-root semantic revision", &self.revision)?;
        if self.machine_snapshot_version != cymule_core::MachineSnapshot::VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported rooted Machine snapshot version {:?}",
                self.machine_snapshot_version
            )));
        }
        self.machine_frontier.verify()?;
        if self.sequence > MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "state-root sequence exceeds the exact integer range".to_owned(),
            ));
        }
        if let Some(anchor) = &self.machine_base_anchor {
            anchor.verify()?;
        }
        self.roots.verify()?;
        self.verify_machine_roots()?;
        let expected_revision = match (&self.parent_revision, &self.delta_digest) {
            (None, None) => derive_genesis_revision(self.revision_state())?,
            (Some(parent_revision), Some(delta_digest)) => derive_transition_revision(
                DurableRevisionLineage {
                    parent_revision,
                    delta_digest,
                    sequence: self.sequence,
                },
                self.revision_state(),
            )?,
            _ => unreachable!("lineage shape was checked above"),
        };
        if self.revision != expected_revision {
            return Err(DurableError::Integrity {
                code: "state_root_manifest_revision_mismatch".to_owned(),
                message: "state-root manifest revision does not bind its exact lineage and roots"
                    .to_owned(),
            });
        }
        let expected = self.identity()?;
        if self.manifest_id != expected {
            return Err(DurableError::Integrity {
                code: "state_root_manifest_identity_mismatch".to_owned(),
                message: format!(
                    "state-root manifest identity {} does not match {expected}",
                    self.manifest_id
                ),
            });
        }
        if cymule_core::canonical_bytes(self)?.len() > MAX_STATE_ROOT_MANIFEST_BYTES {
            return Err(DurableError::Validation(format!(
                "state-root manifest exceeds {MAX_STATE_ROOT_MANIFEST_BYTES} canonical bytes"
            )));
        }
        Ok(())
    }

    fn verify_machine_roots(&self) -> DurableResult<()> {
        let batch_count = cumulative_batch_count(
            self.machine_base_anchor.as_ref(),
            self.roots.machine_command_batches.entries,
        )?;
        if self.machine_frontier.base_anchor_id.as_deref()
            != self
                .machine_base_anchor
                .as_ref()
                .map(|anchor| anchor.anchor_id.as_str())
            || self.machine_base_anchor.is_some() != self.roots.machine_base.is_some()
            || self.machine_frontier.plan_count != self.roots.machine_plans.entries
            || self.machine_frontier.plan_count != self.roots.machine_plan_admissions.len
            || self.machine_frontier.artifact_count != self.roots.machine_artifacts.entries
            || self.machine_frontier.artifact_count != self.roots.machine_artifact_admissions.len
            || self.machine_frontier.batch_count != batch_count
            || self.roots.machine_command_batches.entries
                != self.roots.machine_command_batch_admissions.len
            || self.roots.machine_events.len > self.machine_frontier.event_count
            || self.roots.machine_admissions.len > self.machine_frontier.admission_sequence
            || self.roots.machine_commands.entries > self.machine_frontier.admission_sequence
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_frontier_physical_mismatch".to_owned(),
                message: "StateRoot Machine collections do not match their exact pinned frontier"
                    .to_owned(),
            });
        }
        Ok(())
    }

    fn revision_state(&self) -> DurableRevisionState<'_> {
        DurableRevisionState {
            durable_version: &self.durable_version,
            machine_snapshot_version: &self.machine_snapshot_version,
            machine_frontier: &self.machine_frontier,
            machine_base_anchor: self.machine_base_anchor.as_ref(),
            roots: &self.roots,
        }
    }

    /// Build sequence-zero state-root authority from one fully admitted durable
    /// state. This is the only path that materializes the complete input while
    /// creating roots; later commits lower [`crate::DurableDelta`] directly.
    pub(crate) fn genesis(state: &crate::DurableState) -> DurableResult<StateRootTransition> {
        build_state_root_genesis(state)
    }

    /// Lower one already-admitted semantic delta to changed immutable paths.
    pub(crate) fn apply<R: StateRootResolver + ?Sized>(
        &self,
        delta: &crate::DurableDelta,
        resolver: &mut R,
    ) -> DurableResult<StateRootTransition> {
        apply_durable_state_root_delta(self, delta, resolver)
    }

    /// Materialize and validate the bounded active durable state for ordinary
    /// reopen. Historical proof and receipt families remain exact-key-only.
    pub(crate) fn materialize<R: StateRootResolver + ?Sized>(
        &self,
        resolver: &mut R,
    ) -> DurableResult<crate::DurableState> {
        materialize_durable_state_root(self, resolver)
    }
}

/// Exact immutable object set and manifest produced by one root update.
#[derive(Debug, Clone, PartialEq)]
pub struct StateRootTransition {
    /// Exact parent manifest for a transition; absent only at genesis.
    pub(crate) parent_manifest: Option<String>,
    /// Canonical digest of the exact lowered semantic delta; absent only at
    /// genesis.
    pub(crate) delta_digest: Option<String>,
    /// Exact next fixed root manifest.
    pub(crate) manifest: StateRootManifest,
    /// Deduplicated new immutable objects in identity order, including the
    /// manifest itself.
    pub(crate) objects: Vec<StateRootObject>,
}

impl StateRootTransition {
    /// Exact parent manifest identity, absent only at genesis.
    pub fn parent_manifest(&self) -> Option<&str> {
        self.parent_manifest.as_deref()
    }

    /// Canonical admitted delta digest, absent only at genesis.
    pub fn delta_digest(&self) -> Option<&str> {
        self.delta_digest.as_deref()
    }

    /// Exact next fixed root manifest.
    pub fn manifest(&self) -> &StateRootManifest {
        &self.manifest
    }

    /// Deduplicated immutable objects in identity order.
    pub fn objects(&self) -> &[StateRootObject] {
        &self.objects
    }

    /// Verify the exact closed object batch and manifest lineage shape.
    ///
    /// # Errors
    ///
    /// Rejects invalid lineage, mismatched authority, or an unclosed object set.
    pub fn verify(&self, parent: Option<&StateRootManifest>) -> DurableResult<()> {
        self.manifest.verify()?;
        if self.parent_manifest != self.manifest.parent_manifest
            || self.delta_digest != self.manifest.delta_digest
        {
            return Err(DurableError::Integrity {
                code: "state_root_transition_lineage_echo_mismatch".to_owned(),
                message: "state-root transition lineage does not match its manifest".to_owned(),
            });
        }
        match (parent, &self.parent_manifest, &self.delta_digest) {
            (None, None, None) if self.manifest.sequence == 0 => {
                let expected = derive_genesis_revision(self.manifest.revision_state())?;
                if self.manifest.revision != expected {
                    return Err(DurableError::Integrity {
                        code: "state_root_genesis_revision_mismatch".to_owned(),
                        message: "genesis revision does not bind its exact result roots".to_owned(),
                    });
                }
            }
            (Some(parent), Some(parent_id), Some(delta)) => {
                parent.verify()?;
                if parent_id != &parent.manifest_id
                    || self.manifest.sequence
                        != parent.sequence.checked_add(1).ok_or_else(|| {
                            DurableError::Validation("state-root sequence overflowed".to_owned())
                        })?
                    || self.manifest.durable_version != parent.durable_version
                    || self.manifest.machine_snapshot_version != parent.machine_snapshot_version
                {
                    return Err(DurableError::Integrity {
                        code: "state_root_transition_parent_mismatch".to_owned(),
                        message: "state-root transition does not extend its exact parent manifest"
                            .to_owned(),
                    });
                }
                validate_digest("state-root delta digest", delta)?;
                let expected = derive_transition_revision(
                    DurableRevisionLineage {
                        parent_revision: &parent.revision,
                        delta_digest: delta,
                        sequence: self.manifest.sequence,
                    },
                    self.manifest.revision_state(),
                )?;
                if self.manifest.revision != expected {
                    return Err(DurableError::Integrity {
                        code: "state_root_transition_revision_mismatch".to_owned(),
                        message:
                            "state-root revision does not bind parent, delta, and result roots"
                                .to_owned(),
                    });
                }
            }
            _ => {
                return Err(DurableError::Validation(
                    "state-root transition has inconsistent genesis or successor lineage"
                        .to_owned(),
                ));
            }
        }
        self.verify_object_closure()
    }

    fn verify_object_closure(&self) -> DurableResult<()> {
        let mut objects = BTreeMap::new();
        let mut previous = None;
        for object in &self.objects {
            object.verify()?;
            let object_id = object.object_id();
            if previous.is_some_and(|previous: &str| previous >= object_id)
                || objects.insert(object_id.to_owned(), object).is_some()
            {
                return Err(DurableError::Validation(
                    "state-root transition objects are repeated or unordered".to_owned(),
                ));
            }
            previous = Some(object_id);
        }
        if objects.get(&self.manifest.manifest_id).copied()
            != Some(&StateRootObject::Manifest(self.manifest.clone()))
        {
            return Err(DurableError::Integrity {
                code: "state_root_transition_manifest_missing".to_owned(),
                message: "state-root transition lacks its exact manifest object".to_owned(),
            });
        }
        let mut reachable = BTreeSet::new();
        let mut queue = VecDeque::from([self.manifest.manifest_id.clone()]);
        while let Some(object_id) = queue.pop_front() {
            if !reachable.insert(object_id.clone()) {
                continue;
            }
            if let Some(object) = objects.get(&object_id) {
                queue.extend(object.pending_references());
            }
        }
        if objects
            .keys()
            .any(|object_id| !reachable.contains(object_id))
        {
            return Err(DurableError::Validation(
                "state-root transition contains an unreachable immutable object".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Loader for immutable state-root objects.
pub trait StateRootResolver {
    /// Exact manifest identity whose immutable-object snapshot is pinned for
    /// the lifetime of this resolver. Database adapters use one read
    /// transaction; directory adapters recheck the unchanged physical head
    /// before releasing the snapshot.
    fn pinned_manifest_id(&self) -> &str;

    /// Resolve one object by exact content identity.
    ///
    /// # Errors
    ///
    /// Returns the provider's storage or pinned-snapshot failure.
    fn load_state_root_object(&mut self, object_id: &str)
    -> DurableResult<Option<StateRootObject>>;
}

impl<T: StateRootResolver + ?Sized> StateRootResolver for &mut T {
    fn pinned_manifest_id(&self) -> &str {
        (**self).pinned_manifest_id()
    }

    fn load_state_root_object(
        &mut self,
        object_id: &str,
    ) -> DurableResult<Option<StateRootObject>> {
        (**self).load_state_root_object(object_id)
    }
}

struct ObjectOverlay<'a, R: StateRootResolver + ?Sized> {
    resolver: &'a mut R,
    pending: BTreeMap<String, StateRootObject>,
}

impl<'a, R: StateRootResolver + ?Sized> ObjectOverlay<'a, R> {
    fn new(resolver: &'a mut R) -> Self {
        Self {
            resolver,
            pending: BTreeMap::new(),
        }
    }

    fn with_pending(
        resolver: &'a mut R,
        pending: BTreeMap<String, StateRootObject>,
    ) -> DurableResult<Self> {
        for (object_id, object) in &pending {
            object.verify()?;
            if object.object_id() != object_id {
                return Err(DurableError::Integrity {
                    code: "state_root_staged_object_locator_mismatch".to_owned(),
                    message: format!(
                        "staged state-root object {object_id} resolves to {}",
                        object.object_id()
                    ),
                });
            }
        }
        Ok(Self { resolver, pending })
    }

    fn into_pending(self) -> BTreeMap<String, StateRootObject> {
        self.pending
    }

    fn load(&mut self, object_id: &str) -> DurableResult<StateRootObject> {
        self.load_optional(object_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_reachable_object_missing".to_owned(),
                message: format!("referenced state-root object {object_id} does not exist"),
            })
    }

    fn load_optional(&mut self, object_id: &str) -> DurableResult<Option<StateRootObject>> {
        let object = match self.pending.get(object_id) {
            Some(object) => Some(object.clone()),
            None => self.resolver.load_state_root_object(object_id)?,
        };
        let Some(object) = object else {
            return Ok(None);
        };
        object.verify()?;
        if object.object_id() != object_id {
            return Err(DurableError::Integrity {
                code: "state_root_object_locator_mismatch".to_owned(),
                message: format!(
                    "state-root object {object_id} resolves to {}",
                    object.object_id()
                ),
            });
        }
        Ok(Some(object))
    }

    fn insert(&mut self, object: StateRootObject) -> DurableResult<String> {
        object.verify()?;
        let object_id = object.object_id().to_owned();
        match self.pending.get(&object_id) {
            Some(existing) if existing != &object => {
                return Err(DurableError::Integrity {
                    code: "state_root_object_identity_conflict".to_owned(),
                    message: format!(
                        "state-root object {object_id} has conflicting canonical content"
                    ),
                });
            }
            Some(_) => {}
            None => {
                self.pending.insert(object_id.clone(), object);
            }
        }
        Ok(object_id)
    }

    fn insert_value(&mut self, value: StateRootValue) -> DurableResult<String> {
        self.insert(StateRootObject::Value(StateValueObject::new(value)?))
    }

    fn load_value(&mut self, object_id: &str) -> DurableResult<StateRootValue> {
        match self.load(object_id)? {
            StateRootObject::Value(value) => Ok(value.value),
            _ => Err(DurableError::Integrity {
                code: "state_root_value_kind_mismatch".to_owned(),
                message: format!(
                    "state-root value reference {object_id} resolves to another object kind"
                ),
            }),
        }
    }

    fn insert_map_nodes(&mut self, nodes: &[MapNode]) -> DurableResult<()> {
        for node in nodes {
            self.insert(StateRootObject::MapNode(node.clone()))?;
        }
        Ok(())
    }

    fn insert_log_nodes(&mut self, nodes: &[LogNode]) -> DurableResult<()> {
        for node in nodes {
            self.insert(StateRootObject::LogNode(node.clone()))?;
        }
        Ok(())
    }

    fn finish(mut self, manifest: &StateRootManifest) -> DurableResult<Vec<StateRootObject>> {
        self.insert(StateRootObject::Manifest(manifest.clone()))?;
        let mut reachable = BTreeSet::new();
        let mut queue = VecDeque::from([manifest.manifest_id.clone()]);
        while let Some(object_id) = queue.pop_front() {
            if !reachable.insert(object_id.clone()) {
                continue;
            }
            if let Some(object) = self.pending.get(&object_id) {
                queue.extend(object.pending_references());
            }
        }
        Ok(self
            .pending
            .into_iter()
            .filter_map(|(object_id, object)| reachable.contains(&object_id).then_some(object))
            .collect())
    }
}

impl<R: StateRootResolver + ?Sized> CollectionResolver for ObjectOverlay<'_, R> {
    fn load_map_node(
        &mut self,
        object_id: &str,
    ) -> cymule_authenticated_collections::Result<Option<MapNode>> {
        match self.load_optional(object_id) {
            Ok(Some(StateRootObject::MapNode(node))) => Ok(Some(node)),
            Ok(Some(_)) => Err(CollectionError::Integrity {
                code: "state_map_object_kind_mismatch",
                message: format!(
                    "authenticated-map reference {object_id} resolves to another object kind"
                ),
            }),
            Ok(None) => Ok(None),
            Err(error) => Err(CollectionError::Provider(collection_provider_failure(
                error,
            ))),
        }
    }

    fn load_log_node(
        &mut self,
        object_id: &str,
    ) -> cymule_authenticated_collections::Result<Option<LogNode>> {
        match self.load_optional(object_id) {
            Ok(Some(StateRootObject::LogNode(node))) => Ok(Some(node)),
            Ok(Some(_)) => Err(CollectionError::Integrity {
                code: "state_log_object_kind_mismatch",
                message: format!(
                    "authenticated-log reference {object_id} resolves to another object kind"
                ),
            }),
            Ok(None) => Ok(None),
            Err(error) => Err(CollectionError::Provider(collection_provider_failure(
                error,
            ))),
        }
    }
}

fn collection_provider_failure(
    error: DurableError,
) -> cymule_authenticated_collections::ProviderFailure {
    use cymule_authenticated_collections::{ProviderConflict, ProviderFailure};

    match error {
        DurableError::Validation(message) => ProviderFailure::Validation { message },
        DurableError::Contract(error) => ProviderFailure::Validation {
            message: error.to_string(),
        },
        DurableError::NotFound(message) => ProviderFailure::Integrity {
            code: "state_root_provider_not_found_escape".to_owned(),
            message,
        },
        DurableError::Integrity { code, message }
        | DurableError::RuntimeDefect { code, message } => {
            ProviderFailure::Integrity { code, message }
        }
        DurableError::Encoding(message) => ProviderFailure::Integrity {
            code: "state_root_provider_encoding_failed".to_owned(),
            message,
        },
        DurableError::Conflict { expected, current } => ProviderFailure::Conflict {
            evidence: ProviderConflict::Revision { expected, current },
        },
        DurableError::HistoryConflict { code, message } => ProviderFailure::Conflict {
            evidence: ProviderConflict::History { code, message },
        },
        DurableError::IllegalTransition(message) => ProviderFailure::Conflict {
            evidence: ProviderConflict::History {
                code: "state_root_provider_illegal_transition".to_owned(),
                message,
            },
        },
        DurableError::PagedScopeRequired {
            run_id,
            scope_id,
            entries,
        } => ProviderFailure::Integrity {
            code: "state_root_provider_paged_scope_escape".to_owned(),
            message: format!(
                "immutable-object provider returned Scope reduction authority for {run_id}/{scope_id} with {entries} entries"
            ),
        },
        DurableError::Busy {
            run_id,
            owner,
            fence,
        } => ProviderFailure::Conflict {
            evidence: ProviderConflict::History {
                code: "state_root_provider_busy".to_owned(),
                message: format!("Run {run_id} is owned by {owner} at fence {fence}"),
            },
        },
        DurableError::ReconciliationRequired { intent_id } => ProviderFailure::Conflict {
            evidence: ProviderConflict::History {
                code: "state_root_provider_reconciliation_required".to_owned(),
                message: format!("Effect {intent_id} remains unknown"),
            },
        },
        DurableError::ArchivedCommandReplayRequired {
            command_id,
            archive_head,
            command_index_root,
        } => ProviderFailure::Conflict {
            evidence: ProviderConflict::History {
                code: "state_root_provider_archived_command_replay_required".to_owned(),
                message: format!(
                    "command {command_id} requires archive {archive_head} at index {command_index_root}"
                ),
            },
        },
        DurableError::Substrate { code, message }
        | DurableError::Persistence { code, message }
        | DurableError::Cancelled { code, message }
        | DurableError::TimedOut { code, message } => ProviderFailure::Substrate { code, message },
        DurableError::CommitOutcomeUnknown { message } => ProviderFailure::Substrate {
            code: "state_root_provider_commit_outcome_unknown".to_owned(),
            message,
        },
    }
}

/// Resolve one map value from an authenticated persistent root.
///
/// # Errors
///
/// Rejects invalid keys or authenticated proofs and preserves resolver failures.
pub fn state_map_get<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    resolver: &mut R,
) -> DurableResult<Option<StateRootValue>> {
    root.verify()?;
    let mut overlay = ObjectOverlay::new(resolver);
    map_get(root, key, &mut overlay)
}

/// Resolve and decode one exact typed leaf beneath an authenticated map root.
pub(crate) fn load_typed_state_map_value<T, R>(
    root: &MapRoot,
    key: &str,
    kind: StateRootLeafKind,
    resolver: &mut R,
) -> DurableResult<Option<T>>
where
    T: DeserializeOwned + Serialize,
    R: StateRootResolver + ?Sized,
{
    state_map_get(root, key, resolver)?
        .map(|value| value.decode(kind))
        .transpose()
}

/// Load one exact typed value object already authenticated by a collection
/// proof. Callers must obtain `value_id` from a verified map or log proof under
/// their pinned source root; this function deliberately does not repeat the
/// collection proof.
pub(crate) fn load_typed_state_value<T, R>(
    value_id: &str,
    kind: StateRootLeafKind,
    resolver: &mut R,
) -> DurableResult<T>
where
    T: DeserializeOwned + Serialize,
    R: StateRootResolver + ?Sized,
{
    let object = load_reachable_object(resolver, value_id)?;
    let StateRootObject::Value(value) = object else {
        return Err(DurableError::Integrity {
            code: "state_root_reachable_value_kind_mismatch".to_owned(),
            message: format!(
                "authenticated collection value {value_id} resolves to a non-value object"
            ),
        });
    };
    value.value.decode(kind)
}

/// Canonical authority digest bound into an ordinary query response/cursor.
pub(crate) fn state_map_root_digest(root: &MapRoot) -> DurableResult<String> {
    root.verify()?;
    cymule_core::canonical_digest(root).map_err(Into::into)
}

/// Resolve one exact bounded Run current from a pinned manifest.
pub(crate) fn load_run_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    run_id: &str,
) -> DurableResult<Option<crate::DurableRunCurrent>> {
    ensure_resolver_pinned(manifest, resolver)?;
    load_typed_state_map_value(
        &manifest.roots.run_currents,
        run_id,
        StateRootLeafKind::RunCurrent,
        resolver,
    )
}

pub(crate) fn load_continuation<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    run_id: &str,
) -> DurableResult<Option<crate::Continuation>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let continuation: Option<crate::Continuation> = load_typed_state_map_value(
        &manifest.roots.continuations,
        run_id,
        StateRootLeafKind::Continuation,
        resolver,
    )?;
    if let Some(continuation) = &continuation {
        continuation.verify_wire()?;
        if continuation.run_id != run_id {
            return Err(DurableError::Integrity {
                code: "state_root_continuation_key_mismatch".to_owned(),
                message: "Continuation changed its exact Run key".to_owned(),
            });
        }
    }
    Ok(continuation)
}

pub(crate) fn load_wait<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    wait_id: &str,
) -> DurableResult<Option<crate::WaitCondition>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let wait: Option<crate::WaitCondition> = load_typed_state_map_value(
        &manifest.roots.waits,
        wait_id,
        StateRootLeafKind::Wait,
        resolver,
    )?;
    if let Some(wait) = &wait {
        wait.verify_wire()?;
        if wait.wait_id != wait_id {
            return Err(DurableError::Integrity {
                code: "state_root_wait_key_mismatch".to_owned(),
                message: "Wait changed its exact storage key".to_owned(),
            });
        }
    }
    Ok(wait)
}

pub(crate) fn load_wait_activation<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    activation_id: &str,
) -> DurableResult<Option<crate::WaitActivationReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("wait activation", activation_id)?;
    let receipt: Option<crate::WaitActivationReceipt> = load_typed_state_map_value(
        &manifest.roots.wait_activations,
        activation_id,
        StateRootLeafKind::WaitActivation,
        resolver,
    )?;
    if let Some(receipt) = &receipt {
        receipt.verify()?;
        if receipt.activation.activation_id != activation_id {
            return Err(DurableError::Integrity {
                code: "state_root_wait_activation_key_mismatch".to_owned(),
                message: "Wait activation receipt changed its exact storage key".to_owned(),
            });
        }
    }
    Ok(receipt)
}

/// Resolve one immutable cancellation command receipt without reading history.
pub(crate) fn load_cancellation_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    cancellation_id: &str,
) -> DurableResult<Option<crate::CancellationReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("Run cancellation", cancellation_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let receipt: Option<crate::CancellationReceipt> = map_get(
        &manifest.roots.cancellation_receipts,
        cancellation_id,
        &mut overlay,
    )?
    .map(|value| value.decode(StateRootLeafKind::CancellationReceipt))
    .transpose()?;
    if let Some(receipt) = &receipt {
        verify_cancellation_receipt_leaf(&manifest.roots, cancellation_id, receipt, &mut overlay)?;
        let run = require_machine_run_current(
            &manifest.machine_frontier,
            &receipt.command.run_id,
            &mut overlay,
        )?;
        crate::model::validate_cancellation_receipt_closure(
            receipt,
            &run.run_id,
            &run.execution_status,
        )?;
    }
    Ok(receipt)
}

/// Resolve one immutable Effect-resolution receipt without reading history.
pub(crate) fn load_effect_resolution_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    resolution_id: &str,
) -> DurableResult<Option<crate::EffectResolutionReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("Effect resolution", resolution_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let receipt: Option<crate::EffectResolutionReceipt> = map_get(
        &manifest.roots.effect_resolution_receipts,
        resolution_id,
        &mut overlay,
    )?
    .map(|value| value.decode(StateRootLeafKind::EffectResolutionReceipt))
    .transpose()?;
    if let Some(receipt) = &receipt {
        verify_effect_resolution_receipt_leaf(
            &manifest.roots,
            resolution_id,
            receipt,
            &mut overlay,
        )?;
        verify_effect_resolution_current(
            &manifest.machine_frontier,
            &manifest.roots,
            receipt,
            &mut overlay,
        )?;
    }
    Ok(receipt)
}

fn verify_cancellation_receipt_leaf<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    cancellation_id: &str,
    receipt: &crate::CancellationReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    receipt.verify()?;
    if receipt.command.cancellation_id != cancellation_id {
        return Err(DurableError::Integrity {
            code: "state_root_cancellation_receipt_key_mismatch".to_owned(),
            message: "Run cancellation receipt changed its exact command key".to_owned(),
        });
    }
    let crate::DurableBoundary::Cancelled { reason } = &receipt.boundary else {
        return Err(DurableError::Integrity {
            code: "state_root_cancellation_receipt_boundary_mismatch".to_owned(),
            message: "Run cancellation receipt lacks its terminal reason".to_owned(),
        });
    };
    require_receipt_artifact(roots, reason, overlay)
}

fn verify_effect_resolution_receipt_leaf<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    resolution_id: &str,
    receipt: &crate::EffectResolutionReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    receipt.verify()?;
    if receipt.command.resolution_id != resolution_id {
        return Err(DurableError::Integrity {
            code: "state_root_effect_resolution_receipt_key_mismatch".to_owned(),
            message: "Effect-resolution receipt changed its exact command key".to_owned(),
        });
    }
    require_receipt_artifact(roots, &receipt.command.execution_binding, overlay)?;
    if let Some(result) = &receipt.result {
        require_receipt_artifact(roots, result, overlay)?;
    }
    Ok(())
}

fn require_receipt_artifact<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    reference: &cymule_core::ArtifactRef,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    reference.validate()?;
    let record: cymule_core::ArtifactRecord =
        map_get(&roots.machine_artifacts, &reference.artifact_id, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_control_receipt_artifact_missing".to_owned(),
                message: format!(
                    "control receipt references absent Artifact {}",
                    reference.artifact_id
                ),
            })?
            .decode(StateRootLeafKind::MachineArtifact)?;
    record.validate()?;
    if &record.reference != reference {
        return Err(DurableError::Integrity {
            code: "state_root_control_receipt_artifact_mismatch".to_owned(),
            message: format!(
                "control receipt Artifact {} changed its exact reference",
                reference.artifact_id
            ),
        });
    }
    Ok(())
}

fn require_machine_run_current<R: StateRootResolver + ?Sized>(
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    run_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<cymule_core::durable_internal::MachineRunCurrent> {
    let value =
        map_get(&frontier.runs, run_id, overlay)?.ok_or_else(|| DurableError::Integrity {
            code: "state_root_receipt_run_missing".to_owned(),
            message: format!("control receipt Run {run_id} has no exact Machine current"),
        })?;
    let StateRootValue::MachineRunCurrent { current } = value else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_run_value_kind_mismatch".to_owned(),
            message: format!("Machine Run {run_id} has the wrong current value"),
        });
    };
    current.verify()?;
    if current.run_id != run_id {
        return Err(DurableError::Integrity {
            code: "state_root_machine_run_key_mismatch".to_owned(),
            message: "Machine Run current changed its exact key".to_owned(),
        });
    }
    Ok(*current)
}

fn verify_effect_resolution_current<R: StateRootResolver + ?Sized>(
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    roots: &StateRoots,
    receipt: &crate::EffectResolutionReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<crate::EffectDispatch> {
    let dispatch: crate::EffectDispatch = load_run_effect_dispatch(
        roots,
        &receipt.command.run_id,
        &receipt.command.intent_id,
        overlay,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "state_root_effect_resolution_dispatch_missing".to_owned(),
        message: "Effect resolution receipt has no exact terminal dispatch".to_owned(),
    })?;
    dispatch.verify_wire()?;
    crate::model::validate_effect_resolution_receipt_closure(receipt, &dispatch)?;
    let run = require_machine_run_current(frontier, &receipt.command.run_id, overlay)?;
    let effect: cymule_core::EffectProjection =
        map_get(&run.children.effects, &receipt.command.intent_id, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_effect_resolution_effect_missing".to_owned(),
                message: "Effect resolution receipt has no exact Core Effect".to_owned(),
            })?
            .decode(StateRootLeafKind::MachineEffect)?;
    crate::model::validate_dispatch_effect_projection(&effect, &dispatch)?;
    Ok(dispatch)
}

pub(crate) fn load_machine_artifact<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    reference: &cymule_core::ArtifactRef,
) -> DurableResult<Option<cymule_core::ArtifactRecord>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    reference
        .validate()
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let artifact: Option<cymule_core::ArtifactRecord> = load_typed_state_map_value(
        &manifest.roots.machine_artifacts,
        &reference.artifact_id,
        StateRootLeafKind::MachineArtifact,
        resolver,
    )?;
    if let Some(artifact) = &artifact {
        artifact
            .reference
            .validate()
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let derived = cymule_core::artifact_ref(&artifact.reference.kind, &artifact.bytes)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if derived != artifact.reference {
            return Err(DurableError::Integrity {
                code: "state_root_machine_artifact_bytes_mismatch".to_owned(),
                message: "Machine Artifact bytes do not match their exact reference".to_owned(),
            });
        }
        if &artifact.reference != reference {
            return Err(DurableError::Integrity {
                code: "state_root_machine_artifact_key_mismatch".to_owned(),
                message: "Machine Artifact changed its exact reference key".to_owned(),
            });
        }
    }
    Ok(artifact)
}

/// Resolve one hot command, its exact bounded Event range, and complete batch.
/// Authenticated absence is the only result that permits the Store owner to
/// consult its separate current-root cold-command index.
pub(crate) fn load_hot_machine_command_entry<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    command_id: &str,
) -> DurableResult<
    Option<(
        cymule_core::MachineCommandArchiveEntry,
        cymule_core::MachineCommandBatchRecord,
    )>,
> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("Machine command", command_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    load_hot_machine_command_entry_from_overlay(manifest, command_id, &mut overlay)
}

fn load_hot_machine_command_entry_from_overlay<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    command_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<
    Option<(
        cymule_core::MachineCommandArchiveEntry,
        cymule_core::MachineCommandBatchRecord,
    )>,
> {
    let Some(value) = map_get(&manifest.roots.machine_commands, command_id, overlay)? else {
        return Ok(None);
    };
    let StateRootValue::MachineCommandCurrent {
        record,
        admission,
        index_proof,
        first_event_position,
    } = value
    else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_value_kind_mismatch".to_owned(),
            message: format!("Machine command {command_id} has the wrong authority leaf"),
        });
    };
    if record.envelope.command_id != command_id {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_key_mismatch".to_owned(),
            message: "Machine hot command changed its exact key".to_owned(),
        });
    }
    let proof = cymule_core::durable_internal::MachinePinnedCommandProof::retained(
        (*record).clone(),
        (*admission).clone(),
        *index_proof,
    );
    let _ = cymule_core::durable_internal::prepare_pinned_command(
        &manifest.machine_frontier,
        &proof,
        record.envelope.clone(),
    )?;
    let events = load_hot_command_events(
        &manifest.roots.machine_events,
        manifest.machine_frontier.event_count,
        &record,
        first_event_position,
        overlay,
    )?;
    let entry = cymule_core::MachineCommandArchiveEntry {
        command: *record,
        admission: *admission,
        events,
    };
    let batch: cymule_core::MachineCommandBatchRecord = map_get(
        &manifest.roots.machine_command_batches,
        &entry.command.batch_id,
        overlay,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "state_root_machine_command_batch_missing".to_owned(),
        message: format!("Machine command {command_id} has no exact hot batch"),
    })?
    .decode(StateRootLeafKind::MachineCommandBatch)?;
    batch.verify_entry(&entry)?;
    Ok(Some((entry, batch)))
}

fn load_hot_command_events<R: StateRootResolver + ?Sized>(
    event_root: &LogRoot,
    total_event_count: u64,
    record: &cymule_core::ArchivedCommandRecord,
    first_event_position: Option<u64>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Vec<cymule_core::Event>> {
    let expected_count = match (&record.receipt.status, &record.envelope.command) {
        (cymule_core::CommandReceiptStatus::Conflict, _) => 0,
        (cymule_core::CommandReceiptStatus::Applied, cymule_core::Command::StartRun { .. }) => 2,
        (cymule_core::CommandReceiptStatus::Applied, _) => 1,
    };
    if record.receipt.event_ids.len() != expected_count
        || (expected_count == 0) != first_event_position.is_none()
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_event_count_mismatch".to_owned(),
            message: "Machine command Event range differs from its closed command kind".to_owned(),
        });
    }
    let Some(first) = first_event_position else {
        return Ok(Vec::new());
    };
    let cut = total_event_count
        .checked_sub(event_root.len)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_machine_event_cut_invalid".to_owned(),
            message: "hot Machine Event log exceeds its cumulative frontier".to_owned(),
        })?;
    let start = first
        .checked_sub(1)
        .and_then(|value| value.checked_sub(cut))
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_machine_command_event_range_compacted".to_owned(),
            message: "hot command Event range lies below the retained causal cut".to_owned(),
        })?;
    if start
        .checked_add(
            u64::try_from(expected_count)
                .map_err(|error| DurableError::Validation(error.to_string()))?,
        )
        .is_none_or(|end| end > event_root.len)
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_event_range_out_of_bounds".to_owned(),
            message: "hot command Event range exceeds the retained log".to_owned(),
        });
    }
    let mut events = Vec::with_capacity(expected_count);
    for (offset, event_id) in record.receipt.event_ids.iter().enumerate() {
        let position = start
            .checked_add(
                u64::try_from(offset)
                    .map_err(|error| DurableError::Validation(error.to_string()))?,
            )
            .ok_or_else(|| {
                DurableError::Validation("Machine Event position overflowed".to_owned())
            })?;
        let event: cymule_core::Event =
            log_value_at(event_root, position, overlay)?.decode(StateRootLeafKind::MachineEvent)?;
        if &event.event_id != event_id {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_event_position_mismatch".to_owned(),
                message: "hot command Event position selected another Event identity".to_owned(),
            });
        }
        events.push(event);
    }
    Ok(events)
}

/// Resolve one exact Run-owned child-index root. A genuinely absent Run has
/// the unique empty root; a retained Run missing its descriptor is corruption.
pub(crate) fn load_run_query_index_root<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    run_id: &str,
    index: RunQueryIndexKind,
) -> DurableResult<MapRoot> {
    ensure_resolver_pinned(manifest, resolver)?;
    let descriptor = state_map_get(&manifest.roots.run_query_indexes, run_id, resolver)?;
    let roots = if let Some(value) = descriptor {
        value.decode_run_query_indexes(run_id)?
    } else {
        if load_run_current(manifest, resolver, run_id)?.is_some() {
            return Err(DurableError::Integrity {
                code: "run_query_indexes_missing".to_owned(),
                message: format!(
                    "retained Run {run_id} has no authenticated query-index descriptor"
                ),
            });
        }
        RunQueryIndexRoots::default()
    };
    Ok(match index {
        RunQueryIndexKind::Waits => roots.waits,
        RunQueryIndexKind::Effects => roots.effects,
        RunQueryIndexKind::Occurrences => roots.occurrences,
        RunQueryIndexKind::Attempts => roots.attempts,
        RunQueryIndexKind::PendingWaits => roots.pending_waits,
        RunQueryIndexKind::ActiveEffects => roots.active_effects,
        RunQueryIndexKind::ActiveLeases => roots.active_leases,
    })
}

/// Resolve the sole current dispatch through its Run-local root. Intent-only
/// controls first use the immutable owner locator; no global payload fallback
/// is admitted.
pub(crate) fn load_effect_dispatch<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    expected_run_id: Option<&str>,
    intent_id: &str,
) -> DurableResult<Option<crate::EffectDispatch>> {
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_content_id("Effect dispatch intent", intent_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    match expected_run_id {
        Some(run_id) => load_run_effect_dispatch(&manifest.roots, run_id, intent_id, &mut overlay),
        None => load_owned_effect_dispatch(&manifest.roots, intent_id, &mut overlay),
    }
}

fn load_outbox_owner<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    intent_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<OutboxOwner>> {
    let owner = map_get(&roots.outbox, intent_id, overlay)?
        .map(|value| value.decode::<OutboxOwner>(StateRootLeafKind::OutboxOwner))
        .transpose()?;
    if let Some(owner) = &owner {
        owner.verify()?;
        if owner.intent_id != intent_id {
            return Err(DurableError::Integrity {
                code: "state_root_outbox_owner_key_mismatch".to_owned(),
                message: "Effect owner locator changed its exact intent identity".to_owned(),
            });
        }
    }
    Ok(owner)
}

fn load_run_effect_dispatch<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    run_id: &str,
    intent_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<crate::EffectDispatch>> {
    let Some(descriptor) = map_get(&roots.run_query_indexes, run_id, overlay)? else {
        return Ok(None);
    };
    let run_roots = descriptor.decode_run_query_indexes(run_id)?;
    let dispatch = map_get(&run_roots.effects, intent_id, overlay)?
        .map(|value| value.decode::<crate::EffectDispatch>(StateRootLeafKind::Outbox))
        .transpose()?;
    if let Some(dispatch) = &dispatch {
        dispatch.verify_wire()?;
        if dispatch.intent_id != intent_id || dispatch.run_id != run_id {
            return Err(DurableError::Integrity {
                code: "state_root_run_outbox_key_mismatch".to_owned(),
                message: "Run-local Effect dispatch changed its intent or Run owner".to_owned(),
            });
        }
    }
    Ok(dispatch)
}

fn load_owned_effect_dispatch<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    intent_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<crate::EffectDispatch>> {
    let Some(owner) = load_outbox_owner(roots, intent_id, overlay)? else {
        return Ok(None);
    };
    load_run_effect_dispatch(roots, &owner.run_id, intent_id, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_owned_outbox_missing".to_owned(),
            message: format!("Effect {intent_id} has an owner but no Run-local dispatch"),
        })
        .map(Some)
}

fn is_run_terminal_transition(
    transition: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
) -> bool {
    matches!(
        transition.action,
        cymule_core::durable_internal::MachinePagedTransitionAction::FailRun
            | cymule_core::durable_internal::MachinePagedTransitionAction::CancelRun
    )
}

fn require_run_query_roots<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    run_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<RunQueryIndexRoots> {
    map_get(&roots.run_query_indexes, run_id, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "run_query_indexes_missing".to_owned(),
            message: format!("retained Run {run_id} has no current-membership descriptor"),
        })?
        .decode_run_query_indexes(run_id)
}

fn run_query_source_digest(run_id: &str, roots: &RunQueryIndexRoots) -> DurableResult<String> {
    let mut source = roots.clone();
    source.terminal = None;
    cymule_core::canonical_digest(&StateRootValue::run_query_indexes(run_id, source)?)
        .map_err(Into::into)
}

fn terminal_continuation_digest<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    run_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<String> {
    let continuation: crate::Continuation = map_get(&roots.continuations, run_id, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "terminal_source_continuation_missing".to_owned(),
            message: format!("terminal Run {run_id} has no source Continuation"),
        })?
        .decode(StateRootLeafKind::Continuation)?;
    continuation.verify_wire()?;
    if continuation.run_id != run_id {
        return Err(DurableError::Integrity {
            code: "terminal_source_continuation_owner_mismatch".to_owned(),
            message: "terminal source Continuation changed its Run owner".to_owned(),
        });
    }
    cymule_core::canonical_digest(&continuation).map_err(Into::into)
}

fn verify_terminal_sidecar_source<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    transition: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
    roots: &StateRoots,
    query: &RunQueryIndexRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<RunTerminalSidecarCurrent> {
    transition.verify()?;
    let terminal = query
        .terminal
        .as_deref()
        .ok_or_else(|| DurableError::Integrity {
            code: "terminal_sidecar_companion_missing".to_owned(),
            message: "retained terminal Core transition has no exact sidecar companion".to_owned(),
        })?;
    terminal.verify()?;
    let mut run =
        require_machine_run_current(&current.machine_frontier, &transition.run_id, overlay)?;
    if !matches!(&run.reducer_state,
        cymule_core::durable_internal::MachineRunReducerState::Transitioning { transition_id }
            if transition_id == &transition.transition_id)
    {
        return Err(DurableError::Integrity {
            code: "terminal_sidecar_run_fence_mismatch".to_owned(),
            message: "terminal sidecar companion does not own the exact Run fence".to_owned(),
        });
    }
    run.reducer_state = cymule_core::durable_internal::MachineRunReducerState::Ready;
    if !is_run_terminal_transition(transition)
        || terminal.transition_id != transition.transition_id
        || terminal.transition_digest != cymule_core::canonical_digest(transition)?
        || terminal.source_query_digest != run_query_source_digest(&transition.run_id, query)?
        || terminal.source_continuation_digest
            != terminal_continuation_digest(roots, &transition.run_id, overlay)?
        || cymule_core::canonical_digest(&run)? != transition.source_run_current_digest
    {
        return Err(DurableError::Integrity {
            code: "terminal_sidecar_source_mismatch".to_owned(),
            message: "terminal sidecar source or paired Core progress changed".to_owned(),
        });
    }
    Ok(terminal.clone())
}

/// Validate recovery against the retained companion before deriving any result.
fn verify_pending_terminal_sidecars<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    transition: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    if !is_run_terminal_transition(transition) {
        return Ok(());
    }
    let query = require_run_query_roots(&manifest.roots, &transition.run_id, overlay)?;
    verify_terminal_sidecar_source(manifest, transition, &manifest.roots, &query, overlay)?;
    Ok(())
}

/// Retain hidden Run-local Effect roots in the same CAS as Core reservation.
fn begin_terminal_sidecars<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    begin: &cymule_core::durable_internal::PinnedMachinePagedBegin,
    roots: &mut StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let transition = &begin.transition;
    if !is_run_terminal_transition(transition) {
        return Ok(());
    }
    transition.verify()?;
    let run_id = &transition.run_id;
    let source = require_machine_run_current(&current.machine_frontier, run_id, overlay)?;
    let mut query = require_run_query_roots(roots, run_id, overlay)?;
    if query.terminal.is_some()
        || transition.parent_revision != current.revision
        || transition.source_run_current_digest != cymule_core::canonical_digest(&source)?
        || !matches!(
            source.reducer_state,
            cymule_core::durable_internal::MachineRunReducerState::Ready
        )
        || query.effects.entries != source.children.effects.entries
    {
        return Err(DurableError::Integrity {
            code: "terminal_sidecar_begin_source_mismatch".to_owned(),
            message: "terminal reservation did not begin from its exact unfenced Run source"
                .to_owned(),
        });
    }
    let terminal = RunTerminalSidecarCurrent {
        transition_id: transition.transition_id.clone(),
        transition_digest: cymule_core::canonical_digest(transition)?,
        source_continuation_digest: terminal_continuation_digest(roots, run_id, overlay)?,
        source_query_digest: run_query_source_digest(run_id, &query)?,
        effects: query.effects.clone(),
        active_effects: query.active_effects.clone(),
        active_leases: query.active_leases.clone(),
    };
    terminal.verify()?;
    query.terminal = Some(Box::new(terminal));
    roots.run_query_indexes = map_put(
        &roots.run_query_indexes,
        run_id,
        StateRootValue::run_query_indexes(run_id, query)?,
        overlay,
    )?;
    Ok(())
}

/// Couple one Core Effect page with precisely that page's hidden outbox work.
fn advance_terminal_sidecars<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    previous: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
    progress: &cymule_core::durable_internal::PinnedMachinePagedProgress,
    roots: &mut StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    if !is_run_terminal_transition(previous) {
        return Ok(());
    }
    let run_id = &previous.run_id;
    let mut query = require_run_query_roots(roots, run_id, overlay)?;
    let mut terminal = verify_terminal_sidecar_source(current, previous, roots, &query, overlay)?;
    progress.transition.verify()?;
    if progress.transition.transition_id != previous.transition_id
        || progress.effects.len()
            > cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES
    {
        return Err(DurableError::Integrity {
            code: "terminal_sidecar_page_mismatch".to_owned(),
            message: "terminal sidecar page changed its owner or exceeded the Core page bound"
                .to_owned(),
        });
    }
    let mut bytes = 0_usize;
    for (intent_id, effect) in &progress.effects {
        let source_value = map_get(&terminal.effects, intent_id, overlay)?.ok_or_else(|| {
            DurableError::Integrity {
                code: "terminal_effect_outbox_missing".to_owned(),
                message: format!("terminal Effect {intent_id} has no exact shadow outbox"),
            }
        })?;
        let source_effect: cymule_core::EffectProjection =
            map_get(&previous.shadow.children.effects, intent_id, overlay)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "terminal_effect_source_missing".to_owned(),
                    message: format!("terminal Effect {intent_id} has no exact Core source"),
                })?
                .decode(StateRootLeafKind::MachineEffect)?;
        let mut dispatch: crate::EffectDispatch = source_value.decode(StateRootLeafKind::Outbox)?;
        dispatch.verify_wire()?;
        crate::model::validate_dispatch_effect_projection(&source_effect, &dispatch)?;
        let effect_bytes = cymule_core::canonical_bytes(effect)?.len();
        let source_effect_bytes = cymule_core::canonical_bytes(&source_effect)?.len();
        bytes = bytes
            .checked_add(cymule_core::canonical_bytes(&source_value)?.len())
            .and_then(|count| count.checked_add(effect_bytes))
            .and_then(|count| count.checked_add(source_effect_bytes))
            .filter(|count| {
                *count <= cymule_core::durable_internal::MAX_PINNED_MACHINE_READ_SET_BYTES
            })
            .ok_or_else(|| {
                DurableError::Validation(
                    "terminal sidecar page exceeds its bounded read set".to_owned(),
                )
            })?;
        if dispatch.run_id != *run_id
            || dispatch.intent_id != *intent_id
            || map_get(&terminal.active_effects, intent_id, overlay)?.as_ref()
                != Some(&source_value)
        {
            return Err(DurableError::Integrity {
                code: "terminal_effect_source_membership_mismatch".to_owned(),
                message: "terminal Effect page changed its exact Run or active membership"
                    .to_owned(),
            });
        }
        let claimed = dispatch.state == crate::OutboxState::Claimed;
        dispatch.state = match dispatch.state {
            crate::OutboxState::Pending => crate::OutboxState::CancelledBeforeRelease,
            crate::OutboxState::Claimed => crate::OutboxState::Unknown,
            _ => {
                return Err(DurableError::Integrity {
                    code: "terminal_effect_source_changed".to_owned(),
                    message: "Core terminal page changed an already terminal outbox".to_owned(),
                });
            }
        };
        crate::model::synchronize_pinned_effect_projection(effect, &mut dispatch)?;
        remove_terminal_effect_lease(&mut terminal, roots, &dispatch, claimed, overlay)?;
        let value = StateRootValue::encode(StateRootLeafKind::Outbox, &dispatch)?;
        terminal.effects = map_put(&terminal.effects, intent_id, value.clone(), overlay)?;
        terminal.active_effects = if claimed {
            map_put(&terminal.active_effects, intent_id, value, overlay)?
        } else {
            map_remove(&terminal.active_effects, intent_id, overlay)?
        };
    }
    terminal.transition_digest = cymule_core::canonical_digest(&progress.transition)?;
    terminal.verify()?;
    query.terminal = Some(Box::new(terminal));
    roots.run_query_indexes = map_put(
        &roots.run_query_indexes,
        run_id,
        StateRootValue::run_query_indexes(run_id, query)?,
        overlay,
    )?;
    Ok(())
}

fn remove_terminal_effect_lease<R: StateRootResolver + ?Sized>(
    terminal: &mut RunTerminalSidecarCurrent,
    roots: &StateRoots,
    dispatch: &crate::EffectDispatch,
    was_claimed: bool,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let intent_id = &dispatch.intent_id;
    let lease = map_get(&terminal.active_leases, intent_id, overlay)?;
    if was_claimed {
        let lease_value = lease.ok_or_else(|| DurableError::Integrity {
            code: "terminal_effect_lease_missing".to_owned(),
            message: "claimed terminal Effect has no active lease membership".to_owned(),
        })?;
        let lease: crate::CoordinationLease = lease_value.decode(StateRootLeafKind::Lease)?;
        lease.verify()?;
        if lease.resource != *intent_id
            || dispatch.claim_owner.as_deref() != Some(lease.owner.as_str())
            || dispatch.claim_epoch != lease.epoch
            || map_get(&roots.leases, intent_id, overlay)?.as_ref() != Some(&lease_value)
        {
            return Err(DurableError::Integrity {
                code: "terminal_effect_lease_mismatch".to_owned(),
                message: "terminal Effect changed its retained dispatch claim".to_owned(),
            });
        }
        terminal.active_leases = map_remove(&terminal.active_leases, intent_id, overlay)?;
    } else if lease.is_some() {
        return Err(DurableError::Integrity {
            code: "terminal_pending_effect_has_lease".to_owned(),
            message: "undispatched terminal Effect retained an active lease".to_owned(),
        });
    }
    Ok(())
}

/// Select this Run's finished Effect roots before the final small sidecar batch.
fn finish_terminal_sidecars<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    transition: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
    result: &cymule_core::durable_internal::MachineRunCurrent,
    roots: &mut StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    if !is_run_terminal_transition(transition) {
        return Ok(());
    }
    let run_id = &transition.run_id;
    let mut query = require_run_query_roots(roots, run_id, overlay)?;
    let terminal = verify_terminal_sidecar_source(current, transition, roots, &query, overlay)?;
    result.verify()?;
    let terminal_matches = match (&transition.envelope.command, &result.execution_status) {
        (
            cymule_core::Command::FailRun { failure },
            cymule_core::RunExecutionStatus::Failed { failure: actual },
        ) => failure == actual,
        (
            cymule_core::Command::CancelRun { reason },
            cymule_core::RunExecutionStatus::Cancelled { reason: actual },
        ) => reason == actual,
        _ => false,
    };
    if !terminal_matches
        || transition.phase != cymule_core::durable_internal::MachinePagedTransitionPhase::Finalize
        || result.run_id != *run_id
        || result.children.effects != transition.shadow.children.effects
        || terminal.effects.entries != result.children.effects.entries
        || terminal.active_effects.entries
            != result
                .indexes
                .unknown_effects
                .entries
                .checked_add(result.indexes.governance_effects.entries)
                .ok_or_else(|| {
                    DurableError::Validation("terminal Effect count overflowed".to_owned())
                })?
        || terminal.active_leases.entries != 0
    {
        return Err(DurableError::Integrity {
            code: "terminal_sidecar_final_mismatch".to_owned(),
            message: "final Core Run does not close its exact paired Effect sidecar roots"
                .to_owned(),
        });
    }
    query.effects = terminal.effects;
    query.active_effects = terminal.active_effects;
    query.active_leases = terminal.active_leases;
    query.terminal = None;
    roots.run_query_indexes = map_put(
        &roots.run_query_indexes,
        run_id,
        StateRootValue::run_query_indexes(run_id, query)?,
        overlay,
    )?;
    Ok(())
}

pub(crate) const MAX_STATE_MAP_KEY_PAGE_BYTES: usize =
    cymule_authenticated_collections::MAX_PAGE_BYTES;

/// One key selected from the persistent map's authenticated key-hash order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateMapKeyPageEntry {
    pub(crate) key: String,
    pub(crate) key_hash: String,
    /// Exact value object authenticated for this key by the range proof.
    pub(crate) value_id: String,
}

/// Complete physical position consumed by the next hash-trie page.
///
/// The exact leaf is re-resolved under the page's source root before traversal;
/// an arbitrary digest cannot skip an authenticated prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateMapTraversalPosition {
    pub(crate) key: String,
    pub(crate) key_hash: String,
}

impl StateMapTraversalPosition {
    fn verify(&self) -> DurableResult<()> {
        let derived = MapPosition::for_key(&self.key)?;
        if derived.key_hash() != self.key_hash {
            return Err(DurableError::Validation(
                "persistent-map traversal position key and hash disagree".to_owned(),
            ));
        }
        Ok(())
    }
}

/// One bounded key-only page from an exact persistent-map root.
///
/// Values are intentionally not loaded while resolving the page. Each entry
/// retains the value identity already authenticated by the range proof, so the
/// caller can load exactly one selected typed value object without proving the
/// same key again. A corrupt or unavailable unrelated value cannot widen an
/// ordinary command or query read set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateMapKeyPage {
    pub(crate) entries: Vec<StateMapKeyPageEntry>,
    pub(crate) next_position: Option<StateMapTraversalPosition>,
}

/// Resolve a bounded key page without traversing entries preceding the opaque
/// hash cursor and without loading any value object.
pub(crate) fn load_state_map_key_page<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    position: Option<&StateMapTraversalPosition>,
    limit: usize,
    max_key_bytes: usize,
    resolver: &mut R,
) -> DurableResult<StateMapKeyPage> {
    let position = position
        .map(|position| {
            position.verify()?;
            MapPosition::for_key(&position.key).map_err(DurableError::from)
        })
        .transpose()?;
    let mut overlay = ObjectOverlay::new(resolver);
    let proof = prove_map_range(root, position.as_ref(), limit, max_key_bytes, &mut overlay)?;
    let page = verify_map_range(root, position.as_ref(), limit, max_key_bytes, &proof)?;
    let entries = page
        .entries()
        .iter()
        .map(|(position, value_id)| StateMapKeyPageEntry {
            key: position.key().to_owned(),
            key_hash: position.key_hash().to_owned(),
            value_id: value_id.to_owned(),
        })
        .collect();
    let next_position = page
        .next_position()
        .map(|position| StateMapTraversalPosition {
            key: position.key().to_owned(),
            key_hash: position.key_hash().to_owned(),
        });
    Ok(StateMapKeyPage {
        entries,
        next_position,
    })
}

/// Resolve one all-ever journal-record manifest by its exact journal and
/// record identity.
///
/// Absence is authenticated by the two persistent-map lookups. The outer
/// journal descriptor and the inner typed leaf must retain their exact keys;
/// another value kind or a key/content mismatch is corrupt authority.
///
/// # Errors
///
/// Rejects mismatched owners, root proofs, or retained values and preserves storage failures.
pub fn load_application_journal_record_manifest<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    journal_id: &str,
    record_id: &str,
) -> DurableResult<Option<crate::JournalRecordManifest>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    crate::model::validate_wire_non_empty("application journal identity", journal_id)?;
    crate::model::validate_wire_non_empty("application journal record identity", record_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    load_application_journal_record_manifest_from_roots(
        &manifest.roots,
        &mut overlay,
        journal_id,
        record_id,
    )
}

fn load_application_journal_record_manifest_from_roots<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
    journal_id: &str,
    record_id: &str,
) -> DurableResult<Option<crate::JournalRecordManifest>> {
    let Some(descriptor) = map_get(
        &roots.application_journal_record_manifests,
        journal_id,
        overlay,
    )?
    else {
        return Ok(None);
    };
    let records = descriptor.decode_record_manifest_root(journal_id)?;
    let Some(value) = map_get(&records, record_id, overlay)? else {
        return Ok(None);
    };
    let value: crate::JournalRecordManifest =
        value.decode(StateRootLeafKind::JournalRecordManifest)?;
    value.verify()?;
    if value.record_id != record_id {
        return Err(DurableError::Integrity {
            code: "state_root_journal_record_manifest_key_mismatch".to_owned(),
            message: format!(
                "application journal {journal_id} record-manifest key {record_id} resolves to {}",
                value.record_id
            ),
        });
    }
    Ok(Some(value))
}

/// Resolve one cumulative application-journal prefix-replacement authority by
/// its exact replacement identity.
///
/// # Errors
///
/// Rejects invalid replacement identities or retained authority and preserves storage failures.
pub fn load_application_journal_prefix_replacement_authority<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    replacement_id: &str,
) -> DurableResult<Option<crate::ApplicationJournalPrefixReplacementAuthority>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    crate::model::validate_wire_non_empty(
        "application journal prefix replacement identity",
        replacement_id,
    )?;
    let mut overlay = ObjectOverlay::new(resolver);
    let Some(value) = map_get(
        &manifest
            .roots
            .application_journal_prefix_replacement_history,
        replacement_id,
        &mut overlay,
    )?
    else {
        return Ok(None);
    };
    let value: crate::ApplicationJournalPrefixReplacementAuthority =
        value.decode(StateRootLeafKind::JournalPrefixReplacementAuthority)?;
    value.verify()?;
    if value.replacement_id != replacement_id {
        return Err(DurableError::Integrity {
            code: "state_root_journal_replacement_authority_key_mismatch".to_owned(),
            message: format!(
                "application journal replacement key {replacement_id} resolves to {}",
                value.replacement_id
            ),
        });
    }
    Ok(Some(value))
}

/// Resolve one complete coupled-checkpoint receipt by its stable exact key.
///
/// Receipt resolution also authenticates every payload-free journal manifest
/// named by that receipt through exact nested-map lookups. This keeps lost-ack
/// replay closed without materializing either cumulative history family.
///
/// # Errors
///
/// Rejects mismatched checkpoint identities or unclosed authority and preserves storage failures.
pub fn load_coupled_checkpoint_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    coupling_id: &str,
) -> DurableResult<Option<crate::CoupledCheckpointReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    crate::model::validate_sha256_identity("coupled checkpoint key", coupling_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let Some(value) = map_get(
        &manifest.roots.coupled_checkpoint_receipts,
        coupling_id,
        &mut overlay,
    )?
    else {
        return Ok(None);
    };
    let value: crate::CoupledCheckpointReceipt =
        value.decode(StateRootLeafKind::CoupledCheckpointReceipt)?;
    value.verify()?;
    if value.coupling_id != coupling_id {
        return Err(DurableError::Integrity {
            code: "state_root_coupled_checkpoint_receipt_key_mismatch".to_owned(),
            message: format!(
                "coupled checkpoint key {coupling_id} resolves to {}",
                value.coupling_id
            ),
        });
    }
    for journal in value.manifests() {
        for expected in &journal.records {
            let retained = load_application_journal_record_manifest_from_roots(
                &manifest.roots,
                &mut overlay,
                &journal.journal_id,
                &expected.record_id,
            )?;
            if retained.as_ref() != Some(expected) {
                return Err(DurableError::Integrity {
                    code: "state_root_coupled_checkpoint_journal_history_missing".to_owned(),
                    message: format!(
                        "coupled checkpoint {coupling_id} lost journal {} record {}",
                        journal.journal_id, expected.record_id
                    ),
                });
            }
        }
    }
    if let crate::CoupledCheckpoint::AgentWorkspace { checkpoint } = &value.checkpoint {
        verify_agent_workspace_artifacts(&manifest.roots, checkpoint, &mut overlay)?;
    }
    Ok(Some(value))
}

fn load_agent_leaf_from_roots<T, R>(
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
    family: StateRootFamily,
    key: &str,
    kind: StateRootLeafKind,
) -> DurableResult<Option<T>>
where
    T: DeserializeOwned + Serialize,
    R: StateRootResolver + ?Sized,
{
    map_get(roots.get(family), key, overlay)?
        .map(|value| value.decode(kind))
        .transpose()
}

pub(crate) fn load_agent_command<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    command_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::agent::AgentCommand>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::agent::agent_command_key(command_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let command: Option<cymule_profile_protocol::agent::AgentCommand> = load_agent_leaf_from_roots(
        &manifest.roots,
        &mut overlay,
        StateRootFamily::AgentCommands,
        &key,
        StateRootLeafKind::AgentCommand,
    )?;
    if let Some(command) = &command {
        command.verify()?;
        if command.command_id != command_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_command_key_mismatch".to_owned(),
                message: "Agent command changed its exact storage key".to_owned(),
            });
        }
    }
    Ok(command)
}

pub(crate) fn load_agent_command_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    command_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::agent::AgentCommandReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::agent::agent_command_key(command_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let receipt: Option<cymule_profile_protocol::agent::AgentCommandReceipt> =
        load_agent_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::AgentCommandReceipts,
            &key,
            StateRootLeafKind::AgentCommandReceipt,
        )?;
    if let Some(receipt) = &receipt {
        let command: cymule_profile_protocol::agent::AgentCommand = load_agent_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::AgentCommands,
            &key,
            StateRootLeafKind::AgentCommand,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_agent_receipt_command_missing".to_owned(),
            message: format!(
                "Agent receipt {} has no exact persisted command",
                receipt.receipt_id
            ),
        })?;
        command.verify()?;
        receipt.verify_for(&command)?;
        if command.command_id != command_id || receipt.command_id != command_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_receipt_key_mismatch".to_owned(),
                message: "Agent receipt changed its exact command key".to_owned(),
            });
        }
    }
    Ok(receipt)
}

pub(crate) fn load_agent_input_suspension_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    wait_id: &str,
) -> DurableResult<Option<crate::model::AgentInputSuspensionReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = crate::model::agent_input_suspension_key(wait_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let receipt: Option<crate::model::AgentInputSuspensionReceipt> = load_agent_leaf_from_roots(
        &manifest.roots,
        &mut overlay,
        StateRootFamily::AgentInputSuspensionReceipts,
        &key,
        StateRootLeafKind::AgentInputSuspensionReceipt,
    )?;
    if let Some(receipt) = &receipt {
        receipt.verify()?;
        if receipt.wait.wait_id != wait_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_input_suspension_key_mismatch".to_owned(),
                message: "Agent input suspension receipt changed its exact Wait key".to_owned(),
            });
        }
    }
    Ok(receipt)
}

pub(crate) fn load_agent_input_completion_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    wait_id: &str,
) -> DurableResult<Option<crate::model::AgentInputCompletionReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = crate::model::agent_input_completion_key(wait_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let receipt: Option<crate::model::AgentInputCompletionReceipt> = load_agent_leaf_from_roots(
        &manifest.roots,
        &mut overlay,
        StateRootFamily::AgentInputCompletionReceipts,
        &key,
        StateRootLeafKind::AgentInputCompletionReceipt,
    )?;
    if let Some(receipt) = &receipt {
        receipt.verify()?;
        if receipt.wait.wait_id != wait_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_input_completion_key_mismatch".to_owned(),
                message: "Agent input completion receipt changed its exact Wait key".to_owned(),
            });
        }
    }
    Ok(receipt)
}

pub(crate) fn load_agent_session_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    session_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::agent::AgentSessionCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::agent::agent_session_key(session_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let session: Option<cymule_profile_protocol::agent::AgentSessionCurrent> =
        load_agent_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::AgentSessions,
            &key,
            StateRootLeafKind::AgentSessionCurrent,
        )?;
    if let Some(session) = &session {
        if session.session_id != session_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_session_key_mismatch".to_owned(),
                message: "Agent Session current changed its exact owner key".to_owned(),
            });
        }
        validate_agent_session_roots(&manifest.roots, session, &mut overlay)?;
    }
    Ok(session)
}

macro_rules! load_agent_exact_current {
    (
        $name:ident,
        $type:ty,
        $family:ident,
        $kind:ident,
        $key:expr,
        $verify:expr
    ) => {
        pub(crate) fn $name<R: StateRootResolver + ?Sized>(
            manifest: &StateRootManifest,
            resolver: &mut R,
            session_id: &str,
            local_id: &str,
        ) -> DurableResult<Option<$type>> {
            manifest.verify()?;
            ensure_resolver_pinned(manifest, resolver)?;
            let key = $key(session_id, local_id)?;
            let mut overlay = ObjectOverlay::new(resolver);
            let value: Option<$type> = load_agent_leaf_from_roots(
                &manifest.roots,
                &mut overlay,
                StateRootFamily::$family,
                &key,
                StateRootLeafKind::$kind,
            )?;
            if let Some(value) = &value {
                ($verify)(value, session_id, local_id)?;
            }
            Ok(value)
        }
    };
}

load_agent_exact_current!(
    load_agent_message_current,
    cymule_profile_protocol::agent::AgentMessageCurrent,
    AgentMessages,
    AgentMessageCurrent,
    cymule_profile_protocol::agent::agent_message_key,
    |value: &cymule_profile_protocol::agent::AgentMessageCurrent,
     session_id: &str,
     message_id: &str| {
        value.verify()?;
        if value.session_id != session_id || value.message.message_id != message_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_message_key_mismatch".to_owned(),
                message: "Agent message current changed its exact owner key".to_owned(),
            });
        }
        Ok(())
    }
);

load_agent_exact_current!(
    load_agent_tool_current,
    cymule_profile_protocol::agent::AgentToolCurrent,
    AgentTools,
    AgentToolCurrent,
    cymule_profile_protocol::agent::agent_tool_key,
    |value: &cymule_profile_protocol::agent::AgentToolCurrent, session_id: &str, tool_id: &str| {
        value.verify()?;
        if value.session_id != session_id || value.tool.tool_call_id != tool_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_tool_key_mismatch".to_owned(),
                message: "Agent tool current changed its exact owner key".to_owned(),
            });
        }
        Ok(())
    }
);

pub(crate) fn load_agent_target_claim_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    session_id: &str,
    target: &cymule_profile_protocol::agent::AgentTargetClaimTarget,
) -> DurableResult<Option<cymule_profile_protocol::agent::AgentTargetClaimCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::agent::agent_target_claim_key(session_id, target)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let current: Option<cymule_profile_protocol::agent::AgentTargetClaimCurrent> =
        load_agent_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::AgentTargetClaims,
            &key,
            StateRootLeafKind::AgentTargetClaimCurrent,
        )?;
    if let Some(current) = &current {
        current.verify()?;
        if current.session_id != session_id || current.target != *target {
            return Err(DurableError::Integrity {
                code: "state_root_agent_target_claim_key_mismatch".to_owned(),
                message: "Agent target claim changed its exact owner key".to_owned(),
            });
        }
    }
    Ok(current)
}

load_agent_exact_current!(
    load_agent_elicitation_current,
    cymule_profile_protocol::agent::AgentElicitationCurrent,
    AgentElicitations,
    AgentElicitationCurrent,
    cymule_profile_protocol::agent::agent_elicitation_key,
    |value: &cymule_profile_protocol::agent::AgentElicitationCurrent,
     session_id: &str,
     request_id: &str| {
        value.verify()?;
        if value.session_id != session_id || value.elicitation.request.request_id != request_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_elicitation_key_mismatch".to_owned(),
                message: "Agent elicitation current changed its exact owner key".to_owned(),
            });
        }
        Ok(())
    }
);

load_agent_exact_current!(
    load_agent_occurrence_current,
    cymule_profile_protocol::agent::AgentOccurrenceCurrent,
    AgentOccurrences,
    AgentOccurrenceCurrent,
    cymule_profile_protocol::agent::agent_occurrence_key,
    |value: &cymule_profile_protocol::agent::AgentOccurrenceCurrent,
     session_id: &str,
     occurrence_id: &str| {
        value.verify()?;
        if value.occurrence.session_id != session_id
            || value.occurrence.occurrence_id != occurrence_id
        {
            return Err(DurableError::Integrity {
                code: "state_root_agent_occurrence_key_mismatch".to_owned(),
                message: "Agent occurrence current changed its exact owner key".to_owned(),
            });
        }
        Ok(())
    }
);

load_agent_exact_current!(
    load_agent_stream_current,
    cymule_profile_protocol::agent::AgentStreamCurrent,
    AgentStreams,
    AgentStreamCurrent,
    cymule_profile_protocol::agent::agent_stream_key,
    |value: &cymule_profile_protocol::agent::AgentStreamCurrent,
     session_id: &str,
     stream_id: &str| {
        value.verify()?;
        if value.session_id != session_id || value.stream_id != stream_id {
            return Err(DurableError::Integrity {
                code: "state_root_agent_stream_key_mismatch".to_owned(),
                message: "Agent stream current changed its exact owner key".to_owned(),
            });
        }
        Ok(())
    }
);

pub(crate) fn load_agent_update_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    session_id: &str,
    update_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::agent::AgentUpdateCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::agent::agent_update_key(session_id, update_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::agent::AgentUpdateCurrent> =
        load_agent_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::AgentUpdates,
            &key,
            StateRootLeafKind::AgentUpdateCurrent,
        )?;
    if let Some(value) = &value
        && (value.session_id != session_id || value.update_id != update_id)
    {
        return Err(DurableError::Integrity {
            code: "state_root_agent_update_key_mismatch".to_owned(),
            message: "Agent update current changed its exact owner key".to_owned(),
        });
    }
    Ok(value)
}

pub(crate) fn load_agent_stream_chunk_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    session_id: &str,
    stream_id: &str,
    sequence: u64,
) -> DurableResult<Option<cymule_profile_protocol::agent::AgentStreamChunkCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key =
        cymule_profile_protocol::agent::agent_stream_chunk_key(session_id, stream_id, sequence)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::agent::AgentStreamChunkCurrent> =
        load_agent_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::AgentStreamChunks,
            &key,
            StateRootLeafKind::AgentStreamChunkCurrent,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.session_id != session_id
            || value.stream_id != stream_id
            || value.chunk.sequence != sequence
        {
            return Err(DurableError::Integrity {
                code: "state_root_agent_stream_chunk_key_mismatch".to_owned(),
                message: "Agent stream chunk changed its exact owner key".to_owned(),
            });
        }
    }
    Ok(value)
}

pub(crate) fn load_resource_catalog_record<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    record_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceCatalogRecord>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_content_id("Resource catalog record", record_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourceCatalogRecord> =
        load_agent_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourceCatalogRecords,
            record_id,
            StateRootLeafKind::ResourceCatalogRecord,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.record_id != record_id {
            return Err(DurableError::Integrity {
                code: "state_root_resource_catalog_key_mismatch".to_owned(),
                message: "Resource catalog record changed its exact key".to_owned(),
            });
        }
    }
    Ok(value)
}

fn load_evolution_leaf_from_roots<T, R>(
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
    family: StateRootFamily,
    key: &str,
    kind: StateRootLeafKind,
) -> DurableResult<Option<T>>
where
    T: DeserializeOwned + Serialize,
    R: StateRootResolver + ?Sized,
{
    map_get(roots.get(family), key, overlay)?
        .map(|value| value.decode(kind))
        .transpose()
}

/// Resolve one exact M4 scalar current from a pinned `StateRoot`.
pub(crate) fn load_evolution_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    evolution_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::evolution::EvolutionCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::evolution::evolution_current_key(evolution_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let current: Option<cymule_profile_protocol::evolution::EvolutionCurrent> =
        load_evolution_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::EvolutionCurrents,
            &key,
            StateRootLeafKind::EvolutionCurrent,
        )?;
    if let Some(current) = &current {
        current.verify()?;
        if current.evolution_id != evolution_id {
            return Err(DurableError::Integrity {
                code: "state_root_evolution_current_key_mismatch".to_owned(),
                message: "Evolution current changed its exact authority partition".to_owned(),
            });
        }
    }
    Ok(current)
}

/// Resolve one exact M4 command alias and its immutable semantic receipt.
pub(crate) fn load_evolution_command_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    evolution_id: &str,
    command_id: &str,
) -> DurableResult<
    Option<(
        cymule_profile_protocol::evolution::EvolutionCommandAlias,
        cymule_profile_protocol::evolution::EvolutionPersistenceReceipt,
    )>,
> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let alias_key =
        cymule_profile_protocol::evolution::evolution_command_alias_key(evolution_id, command_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let Some(alias): Option<cymule_profile_protocol::evolution::EvolutionCommandAlias> =
        load_evolution_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::EvolutionCommandAliases,
            &alias_key,
            StateRootLeafKind::EvolutionCommandAlias,
        )?
    else {
        return Ok(None);
    };
    alias.verify()?;
    if alias.evolution_id != evolution_id || alias.command_id != command_id {
        return Err(DurableError::Integrity {
            code: "state_root_evolution_alias_key_mismatch".to_owned(),
            message: "Evolution command alias changed its exact partition or command key"
                .to_owned(),
        });
    }
    let receipt_key =
        cymule_profile_protocol::evolution::evolution_receipt_key(evolution_id, &alias.receipt_id)?;
    let receipt: cymule_profile_protocol::evolution::EvolutionPersistenceReceipt =
        load_evolution_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::EvolutionReceipts,
            &receipt_key,
            StateRootLeafKind::EvolutionPersistenceReceipt,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_evolution_alias_receipt_missing".to_owned(),
            message: format!(
                "Evolution command alias {command_id} lost receipt {}",
                alias.receipt_id
            ),
        })?;
    receipt.verify()?;
    if receipt.receipt_id != alias.receipt_id
        || receipt.command.evolution_id != evolution_id
        || receipt.command.persistence_id != alias.persistence_id
        || receipt.command.command.command_id() != command_id
    {
        return Err(DurableError::Integrity {
            code: "state_root_evolution_alias_receipt_mismatch".to_owned(),
            message: "Evolution command alias does not select its exact semantic receipt"
                .to_owned(),
        });
    }
    Ok(Some((alias, receipt)))
}

/// Resolve one exact typed normalized M4 leaf from a pinned family root.
pub(crate) fn load_evolution_mutation<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    family: cymule_profile_protocol::evolution::EvolutionStateFamily,
    storage_key: &str,
) -> DurableResult<Option<cymule_profile_protocol::evolution::EvolutionMutation>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_content_id("Evolution normalized storage key", storage_key)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let mutation: Option<cymule_profile_protocol::evolution::EvolutionMutation> = map_get(
        manifest.roots.evolution.state(family),
        storage_key,
        &mut overlay,
    )?
    .map(|value| value.decode(StateRootLeafKind::EvolutionMutation))
    .transpose()?;
    if let Some(mutation) = &mutation {
        mutation.verify()?;
        if mutation.family() != family || mutation.storage_key()?.1 != storage_key {
            return Err(DurableError::Integrity {
                code: "state_root_evolution_mutation_key_mismatch".to_owned(),
                message: "Evolution normalized leaf changed its exact family or storage key"
                    .to_owned(),
            });
        }
    }
    Ok(mutation)
}

/// Resolve one exact M3 Virtual scheduler current from a pinned `StateRoot`.
pub(crate) fn load_virtual_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    scheduler_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::virtual_work::VirtualCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::virtual_work::virtual_current_key(scheduler_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let current: Option<cymule_profile_protocol::virtual_work::VirtualCurrent> =
        map_get(&manifest.roots.virtual_work.currents, &key, &mut overlay)?
            .map(|value| value.decode(StateRootLeafKind::VirtualCurrent))
            .transpose()?;
    if let Some(current) = &current {
        current.verify()?;
        if current.body.scheduler_id != scheduler_id {
            return Err(DurableError::Integrity {
                code: "state_root_virtual_current_key_mismatch".to_owned(),
                message: "Virtual current changed its exact scheduler partition".to_owned(),
            });
        }
    }
    Ok(current)
}

/// Resolve one exact all-ever M3 receipt by scheduler and semantic command.
pub(crate) fn load_virtual_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    scheduler_id: &str,
    command_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::virtual_work::VirtualPersistenceReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let key = cymule_profile_protocol::virtual_work::virtual_receipt_key(scheduler_id, command_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let receipt: Option<cymule_profile_protocol::virtual_work::VirtualPersistenceReceipt> =
        map_get(&manifest.roots.virtual_work.receipts, &key, &mut overlay)?
            .map(|value| value.decode(StateRootLeafKind::VirtualPersistenceReceipt))
            .transpose()?;
    if let Some(receipt) = &receipt {
        receipt.verify()?;
        if receipt.command.scheduler_id() != scheduler_id
            || receipt.command.command_id() != command_id
        {
            return Err(DurableError::Integrity {
                code: "state_root_virtual_receipt_key_mismatch".to_owned(),
                message: "Virtual receipt changed its exact scheduler or command key".to_owned(),
            });
        }
    }
    Ok(receipt)
}

/// Resolve one exact typed normalized M3 leaf from a pinned family root.
pub(crate) fn load_virtual_leaf<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    scheduler_id: &str,
    family: cymule_profile_protocol::virtual_work::VirtualStateFamily,
    storage_key: &str,
) -> DurableResult<Option<cymule_profile_protocol::virtual_work::VirtualStateLeaf>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_content_id("Virtual normalized storage key", storage_key)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let leaf: Option<cymule_profile_protocol::virtual_work::VirtualStateLeaf> = map_get(
        manifest.roots.virtual_work.state(family),
        storage_key,
        &mut overlay,
    )?
    .map(|value| value.decode(StateRootLeafKind::VirtualStateLeaf))
    .transpose()?;
    if let Some(leaf) = &leaf {
        leaf.verify()?;
        if leaf.scheduler_id() != scheduler_id
            || leaf.family() != family
            || leaf.storage_key()? != storage_key
        {
            return Err(DurableError::Integrity {
                code: "state_root_virtual_leaf_key_mismatch".to_owned(),
                message: "Virtual normalized leaf changed its scheduler, family, or exact key"
                    .to_owned(),
            });
        }
    }
    Ok(leaf)
}

pub(crate) fn load_agent_message_page<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    query: &cymule_profile_protocol::agent::AgentMessagePageQuery,
) -> DurableResult<cymule_profile_protocol::agent::AgentMessagePageRead> {
    use cymule_profile_protocol::agent::{AgentMessagePage, AgentMessagePageRead};

    query.verify()?;
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let session_key = cymule_profile_protocol::agent::agent_session_key(&query.session_id)?;
    let session: cymule_profile_protocol::agent::AgentSessionCurrent = load_agent_leaf_from_roots(
        &manifest.roots,
        &mut overlay,
        StateRootFamily::AgentSessions,
        &session_key,
        StateRootLeafKind::AgentSessionCurrent,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!("Agent Session {} does not exist", query.session_id))
    })?;
    validate_agent_session_roots(&manifest.roots, &session, &mut overlay)?;
    let root = match map_get(
        &manifest.roots.agent_message_indexes,
        &query.session_id,
        &mut overlay,
    )? {
        Some(descriptor) => descriptor.decode_agent_message_index_root(&query.session_id)?,
        None => LogRoot::empty(),
    };
    validate_agent_message_page_source(&root, query, &mut overlay)?;
    let entries = load_agent_message_page_entries(&root, manifest.revision(), query, &mut overlay)?;
    let read = AgentMessagePageRead {
        revision: manifest.revision.clone(),
        page: AgentMessagePage {
            session_id: query.session_id.clone(),
            expected_message_head: query.expected_message_head.clone(),
            source_message_count: query.source_message_count,
            end_exclusive: query.end_exclusive,
            next_end_exclusive: entries
                .first()
                .and_then(|entry| (entry.order.index > 0).then_some(entry.order.index)),
            entries,
        },
    };
    read.verify_for(query)?;
    Ok(read)
}

fn load_agent_message_page_entries<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    revision: &str,
    query: &cymule_profile_protocol::agent::AgentMessagePageQuery,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Vec<cymule_profile_protocol::agent::AgentMessageCurrent>> {
    use cymule_profile_protocol::agent::{
        AgentMessageCurrent, AgentMessagePage, AgentMessagePageRead,
    };

    let end = query.end_exclusive.unwrap_or(query.source_message_count);
    let entry_limit = usize::try_from(query.max_entries)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let message_byte_limit = usize::try_from(query.max_message_canonical_bytes)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let wire_byte_limit = usize::try_from(query.max_canonical_bytes)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let mut newest_first = Vec::new();
    let mut message_bytes = 0_usize;
    let mut cursor = end;
    while cursor > 0 && newest_first.len() < entry_limit {
        let index = cursor - 1;
        let entry: AgentMessageCurrent =
            log_value_at(root, index, overlay)?.decode(StateRootLeafKind::AgentMessageCurrent)?;
        entry.verify()?;
        if entry.session_id != query.session_id || entry.order.index != index {
            return Err(DurableError::Integrity {
                code: "agent_message_page_index_mismatch".to_owned(),
                message: "Agent message index changed its Session owner or ordinal".to_owned(),
            });
        }
        let next_message_bytes = message_bytes
            .checked_add(cymule_core::canonical_bytes(&entry)?.len())
            .ok_or_else(|| {
                DurableError::Validation(
                    "Agent message page entry byte accounting overflowed".to_owned(),
                )
            })?;
        if next_message_bytes > message_byte_limit {
            if newest_first.is_empty() {
                return Err(DurableError::Validation(
                    "Agent message page message budget cannot admit its first exact entry"
                        .to_owned(),
                ));
            }
            break;
        }
        let mut candidate_entries = newest_first.clone();
        candidate_entries.push(entry.clone());
        candidate_entries.reverse();
        let candidate = AgentMessagePageRead {
            revision: revision.to_owned(),
            page: AgentMessagePage {
                session_id: query.session_id.clone(),
                expected_message_head: query.expected_message_head.clone(),
                source_message_count: query.source_message_count,
                end_exclusive: query.end_exclusive,
                next_end_exclusive: (index > 0).then_some(index),
                entries: candidate_entries,
            },
        };
        if cymule_core::canonical_bytes(&candidate)?.len() > wire_byte_limit {
            if newest_first.is_empty() {
                return Err(DurableError::Validation(
                    "Agent message page byte budget cannot admit its first exact entry".to_owned(),
                ));
            }
            break;
        }
        newest_first.push(entry);
        message_bytes = next_message_bytes;
        cursor = index;
    }
    newest_first.reverse();
    Ok(newest_first)
}

fn validate_agent_message_page_source<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    query: &cymule_profile_protocol::agent::AgentMessagePageQuery,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::AgentMessageCurrent;

    if query.source_message_count > root.len {
        return Err(DurableError::HistoryConflict {
            code: "agent_message_page_source_count_mismatch".to_owned(),
            message: "Agent message page source exceeds the retained immutable log".to_owned(),
        });
    }
    if query.source_message_count == 0 {
        return Ok(());
    }
    let source_index = query.source_message_count - 1;
    let source: AgentMessageCurrent = log_value_at(root, source_index, overlay)?
        .decode(StateRootLeafKind::AgentMessageCurrent)?;
    source.verify()?;
    if source.session_id != query.session_id || source.order.index != source_index {
        return Err(DurableError::Integrity {
            code: "agent_message_page_source_owner_mismatch".to_owned(),
            message: "Agent message page source entry changed its Session or ordinal".to_owned(),
        });
    }
    if query.expected_message_head.as_deref() != Some(source.order.head.as_str()) {
        return Err(DurableError::HistoryConflict {
            code: "agent_message_page_source_head_mismatch".to_owned(),
            message: "Agent message page source head differs from its immutable log prefix"
                .to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn load_agent_occurrence_page<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    query: &cymule_profile_protocol::agent::AgentOccurrencePageQuery,
) -> DurableResult<cymule_profile_protocol::agent::AgentOccurrencePageRead> {
    use cymule_profile_protocol::agent::{
        AgentOccurrenceCurrent, AgentOccurrencePage, AgentOccurrencePageRead,
    };

    query.verify()?;
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let session_key = cymule_profile_protocol::agent::agent_session_key(&query.session_id)?;
    let session: cymule_profile_protocol::agent::AgentSessionCurrent = load_agent_leaf_from_roots(
        &manifest.roots,
        &mut overlay,
        StateRootFamily::AgentSessions,
        &session_key,
        StateRootLeafKind::AgentSessionCurrent,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!("Agent Session {} does not exist", query.session_id))
    })?;
    validate_agent_session_roots(&manifest.roots, &session, &mut overlay)?;
    if session.unresolved_occurrence_generation != query.index_generation {
        return Err(DurableError::HistoryConflict {
            code: "agent_occurrence_page_generation_mismatch".to_owned(),
            message: "Agent occurrence page query does not match the current index generation"
                .to_owned(),
        });
    }
    let root = match map_get(
        &manifest.roots.agent_unresolved_occurrence_indexes,
        &query.session_id,
        &mut overlay,
    )? {
        Some(descriptor) => {
            descriptor.decode_agent_unresolved_occurrence_index_root(&query.session_id)?
        }
        None => LogRoot::empty(),
    };
    let start =
        agent_occurrence_page_start(&root, &query.session_id, query.after_ordinal, &mut overlay)?;
    let entry_limit = usize::try_from(query.max_entries)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let byte_limit = usize::try_from(query.max_canonical_bytes)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let mut entries = Vec::new();
    let mut position = start;
    while position < root.len && entries.len() < entry_limit {
        let entry: AgentOccurrenceCurrent = log_value_at(&root, position, &mut overlay)?
            .decode(StateRootLeafKind::AgentOccurrenceCurrent)?;
        entry.verify()?;
        if entry.occurrence.session_id != query.session_id || entry.occurrence.is_terminal() {
            return Err(DurableError::Integrity {
                code: "agent_unresolved_occurrence_index_invalid".to_owned(),
                message: "Agent unresolved occurrence index contains a foreign or terminal entry"
                    .to_owned(),
            });
        }
        let mut candidate_entries = entries.clone();
        candidate_entries.push(entry.clone());
        let candidate = AgentOccurrencePageRead {
            revision: manifest.revision.clone(),
            page: AgentOccurrencePage {
                session_id: query.session_id.clone(),
                index_generation: query.index_generation.clone(),
                after_ordinal: query.after_ordinal,
                next_after_ordinal: (position + 1 < root.len).then_some(entry.ordinal),
                entries: candidate_entries,
            },
        };
        if cymule_core::canonical_bytes(&candidate)?.len() > byte_limit {
            if entries.is_empty() {
                return Err(DurableError::Validation(
                    "Agent occurrence page byte budget cannot admit its first exact entry"
                        .to_owned(),
                ));
            }
            break;
        }
        entries.push(entry);
        position += 1;
    }
    let read = AgentOccurrencePageRead {
        revision: manifest.revision.clone(),
        page: AgentOccurrencePage {
            session_id: query.session_id.clone(),
            index_generation: query.index_generation.clone(),
            after_ordinal: query.after_ordinal,
            next_after_ordinal: (position < root.len)
                .then(|| entries.last().map(|entry| entry.ordinal))
                .flatten(),
            entries,
        },
    };
    read.verify_for(query)?;
    Ok(read)
}

fn agent_occurrence_page_start<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    session_id: &str,
    after_ordinal: Option<u64>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<u64> {
    use cymule_profile_protocol::agent::AgentOccurrenceCurrent;

    let mut start = 0;
    let mut end = root.len;
    if let Some(after) = after_ordinal {
        while start < end {
            let middle = start + (end - start) / 2;
            let current: AgentOccurrenceCurrent = log_value_at(root, middle, overlay)?
                .decode(StateRootLeafKind::AgentOccurrenceCurrent)?;
            current.verify()?;
            if current.occurrence.session_id != session_id || current.occurrence.is_terminal() {
                return Err(DurableError::Integrity {
                    code: "agent_unresolved_occurrence_index_invalid".to_owned(),
                    message:
                        "Agent unresolved occurrence index contains a foreign or terminal entry"
                            .to_owned(),
                });
            }
            if current.ordinal <= after {
                start = middle + 1;
            } else {
                end = middle;
            }
        }
    }
    Ok(start)
}

fn load_resource_leaf_from_roots<T, R>(
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
    family: StateRootFamily,
    key: &str,
    kind: StateRootLeafKind,
) -> DurableResult<Option<T>>
where
    T: DeserializeOwned + Serialize,
    R: StateRootResolver + ?Sized,
{
    map_get(roots.get(family), key, overlay)?
        .map(|value| value.decode(kind))
        .transpose()
}

/// Resolve one exact Resource command receipt or typed authority alias.
pub fn load_resource_command_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    authority_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceCommandReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_content_id("Resource receipt authority", authority_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourceCommandReceipt> =
        load_resource_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourceCommandReceipts,
            authority_id,
            StateRootLeafKind::ResourceCommandReceipt,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if !resource_receipt_authority_ids(value).contains(authority_id) {
            return Err(DurableError::Integrity {
                code: "state_root_resource_receipt_authority_mismatch".to_owned(),
                message: format!(
                    "Resource receipt authority {authority_id} does not select the retained receipt"
                ),
            });
        }
    }
    Ok(value)
}

/// Resolve one exact current physical Resource-retention projection.
pub fn load_resource_retention_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    retention_key: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceRetentionCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_content_id("Resource retention key", retention_key)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourceRetentionCurrent> =
        load_resource_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourceRetentionCurrent,
            retention_key,
            StateRootLeafKind::ResourceRetentionCurrent,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.family.retention_key != retention_key {
            return Err(DurableError::Integrity {
                code: "state_root_resource_retention_key_mismatch".to_owned(),
                message: "Resource retention projection changed its exact key".to_owned(),
            });
        }
    }
    Ok(value)
}

/// Resolve one exact current Resource-pin projection.
pub fn load_resource_pin_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    pin_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourcePinCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("Resource pin", pin_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourcePinCurrent> =
        load_resource_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourcePinCurrent,
            pin_id,
            StateRootLeafKind::ResourcePinCurrent,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.pin.pin_id != pin_id {
            return Err(DurableError::Integrity {
                code: "state_root_resource_pin_key_mismatch".to_owned(),
                message: "Resource pin projection changed its exact key".to_owned(),
            });
        }
    }
    Ok(value)
}

/// Resolve one exact current Resource-deletion projection.
pub fn load_resource_delete_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    delete_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceDeleteCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("Resource delete", delete_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourceDeleteCurrent> =
        load_resource_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourceDeleteCurrent,
            delete_id,
            StateRootLeafKind::ResourceDeleteCurrent,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.intent.delete_id != delete_id {
            return Err(DurableError::Integrity {
                code: "state_root_resource_delete_key_mismatch".to_owned(),
                message: "Resource deletion projection changed its exact key".to_owned(),
            });
        }
    }
    Ok(value)
}

/// Resolve one exact immutable Resource-handoff authority.
pub fn load_resource_handoff_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    transfer_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceHandoffCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("Resource transfer", transfer_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourceHandoffCurrent> =
        load_resource_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourceHandoffCurrent,
            transfer_id,
            StateRootLeafKind::ResourceHandoffCurrent,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.receipt.handoff.transfer_id != transfer_id {
            return Err(DurableError::Integrity {
                code: "state_root_resource_handoff_key_mismatch".to_owned(),
                message: "Resource handoff current changed its exact transfer key".to_owned(),
            });
        }
    }
    Ok(value)
}

/// Resolve one exact immutable Resource-handoff activation authority.
pub fn load_resource_handoff_activation_current<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    activation_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceHandoffActivationCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_content_id("Resource handoff activation", activation_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourceHandoffActivationCurrent> =
        load_resource_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourceHandoffActivationCurrent,
            activation_id,
            StateRootLeafKind::ResourceHandoffActivationCurrent,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.receipt.activation.activation_id != activation_id {
            return Err(DurableError::Integrity {
                code: "state_root_resource_activation_key_mismatch".to_owned(),
                message: "Resource activation current changed its exact activation key".to_owned(),
            });
        }
    }
    Ok(value)
}

/// Resolve the immutable Resource-handoff activation for one source transfer.
pub fn load_resource_handoff_activation_by_transfer<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    transfer_id: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceHandoffActivationCurrent>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    cymule_core::validate_identity("Resource transfer", transfer_id)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let value: Option<cymule_profile_protocol::resource::ResourceHandoffActivationCurrent> =
        load_resource_leaf_from_roots(
            &manifest.roots,
            &mut overlay,
            StateRootFamily::ResourceHandoffActivationsByTransfer,
            transfer_id,
            StateRootLeafKind::ResourceHandoffActivationCurrent,
        )?;
    if let Some(value) = &value {
        value.verify()?;
        if value.receipt.activation.transfer_id != transfer_id {
            return Err(DurableError::Integrity {
                code: "state_root_resource_activation_transfer_key_mismatch".to_owned(),
                message: "Resource activation current changed its source transfer key".to_owned(),
            });
        }
    }
    Ok(value)
}

/// Resolve one exact target-slot Resource-handoff entry.
pub fn load_resource_handoff_slot<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    to_run: &str,
    slot: &str,
) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceHandoffIndexEntry>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    validate_resource_target(to_run)?;
    cymule_core::validate_identity("Resource target slot", slot)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let Some(descriptor) = map_get(&manifest.roots.resource_handoff_slots, to_run, &mut overlay)?
    else {
        return Ok(None);
    };
    let slots = descriptor.decode_resource_handoff_slots_root(to_run)?;
    let value: Option<cymule_profile_protocol::resource::ResourceHandoffIndexEntry> =
        map_get(&slots, slot, &mut overlay)?
            .map(|value| value.decode(StateRootLeafKind::ResourceHandoffIndex))
            .transpose()?;
    if let Some(value) = &value {
        value.verify()?;
        if value.to_run != to_run || value.slot != slot {
            return Err(DurableError::Integrity {
                code: "state_root_resource_handoff_slot_key_mismatch".to_owned(),
                message: "Resource handoff slot changed its exact target or slot key".to_owned(),
            });
        }
    }
    Ok(value)
}

/// Return the exact next append position for one target's handoff index.
pub fn load_resource_handoff_index_len<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    to_run: &str,
) -> DurableResult<u64> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    validate_resource_target(to_run)?;
    let mut overlay = ObjectOverlay::new(resolver);
    match map_get(
        &manifest.roots.resource_handoff_indexes,
        to_run,
        &mut overlay,
    )? {
        Some(value) => Ok(value.decode_resource_handoff_index_root(to_run)?.len),
        None => Ok(0),
    }
}

/// Return the exact next append position for one target's activation index.
pub fn load_resource_handoff_activation_index_len<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    to_run: &str,
) -> DurableResult<u64> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    validate_resource_target(to_run)?;
    let mut overlay = ObjectOverlay::new(resolver);
    match map_get(
        &manifest.roots.resource_handoff_activation_indexes,
        to_run,
        &mut overlay,
    )? {
        Some(value) => Ok(value
            .decode_resource_handoff_activation_index_root(to_run)?
            .len),
        None => Ok(0),
    }
}

/// Resolve one bounded contiguous Resource-handoff page for one target Run.
pub fn load_resource_handoff_page<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    to_run: &str,
    start_index: u64,
    limit: usize,
) -> DurableResult<cymule_profile_protocol::resource::ResourceHandoffPage> {
    use cymule_profile_protocol::resource::{MAX_HANDOFF_INDEX_PAGE, ResourceHandoffPage};

    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    validate_resource_target(to_run)?;
    if start_index > MAX_EXACT_INTEGER || !(1..=MAX_HANDOFF_INDEX_PAGE).contains(&limit) {
        return Err(DurableError::Validation(format!(
            "Resource handoff page requires an exact start and limit within 1..={MAX_HANDOFF_INDEX_PAGE}"
        )));
    }
    let mut overlay = ObjectOverlay::new(resolver);
    let root = match map_get(
        &manifest.roots.resource_handoff_indexes,
        to_run,
        &mut overlay,
    )? {
        Some(value) => value.decode_resource_handoff_index_root(to_run)?,
        None if start_index == 0 => {
            return Ok(ResourceHandoffPage {
                handoffs: Vec::new(),
                next_index: None,
            });
        }
        None => {
            return Err(DurableError::Validation(format!(
                "Resource handoff page starts beyond the empty index for Run {to_run}"
            )));
        }
    };
    if start_index > root.len {
        return Err(DurableError::Validation(format!(
            "Resource handoff page start {start_index} exceeds index length {}",
            root.len
        )));
    }
    let limit =
        u64::try_from(limit).map_err(|error| DurableError::Validation(error.to_string()))?;
    let requested_end = start_index.checked_add(limit).ok_or_else(|| {
        DurableError::Validation("Resource handoff page range overflowed".to_owned())
    })?;
    let end = requested_end.min(root.len);
    let page_len = end
        .checked_sub(start_index)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_resource_handoff_page_range_reversed".to_owned(),
            message: "Resource handoff page end precedes its validated start".to_owned(),
        })?;
    let mut handoffs = Vec::with_capacity(
        usize::try_from(page_len).map_err(|error| DurableError::Validation(error.to_string()))?,
    );
    for index in start_index..end {
        let entry: cymule_profile_protocol::resource::ResourceHandoffIndexEntry =
            log_get(&root, index, &mut overlay)?.decode(StateRootLeafKind::ResourceHandoffIndex)?;
        entry.verify()?;
        if entry.to_run != to_run || entry.target_index != index {
            return Err(DurableError::Integrity {
                code: "state_root_resource_handoff_index_position_mismatch".to_owned(),
                message: "Resource handoff index entry changed its target or position".to_owned(),
            });
        }
        let current: cymule_profile_protocol::resource::ResourceHandoffCurrent =
            load_resource_leaf_from_roots(
                &manifest.roots,
                &mut overlay,
                StateRootFamily::ResourceHandoffCurrent,
                &entry.transfer_id,
                StateRootLeafKind::ResourceHandoffCurrent,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_resource_handoff_index_authority_missing".to_owned(),
                message: format!(
                    "Resource handoff index entry {} lost transfer authority {}",
                    index, entry.transfer_id
                ),
            })?;
        current.verify()?;
        if current.receipt.index != entry
            || current.receipt.receipt_id != entry.authority_receipt_id
        {
            return Err(DurableError::Integrity {
                code: "state_root_resource_handoff_index_authority_mismatch".to_owned(),
                message: "Resource handoff index entry does not match its transfer receipt"
                    .to_owned(),
            });
        }
        handoffs.push(current.receipt.handoff);
    }
    Ok(ResourceHandoffPage {
        handoffs,
        next_index: (end < root.len).then_some(end),
    })
}

/// Resolve one ordered value from an authenticated persistent log root.
///
/// # Errors
///
/// Rejects invalid positions or authenticated proofs and preserves resolver failures.
pub fn state_log_get<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    resolver: &mut R,
) -> DurableResult<StateRootValue> {
    root.verify()?;
    if index >= root.len {
        return Err(DurableError::NotFound(format!(
            "persistent-log index {index} is outside length {}",
            root.len
        )));
    }
    let mut overlay = ObjectOverlay::new(resolver);
    log_get(root, index, &mut overlay)
}

/// Materialized values for a selected closed set of top-level collections.
#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedStateRoots {
    /// Exact source manifest.
    pub manifest: StateRootManifest,
    /// Canonical values by selected closed family and stable key.
    pub collections: BTreeMap<StateRootFamily, BTreeMap<String, StateRootValue>>,
}

/// Materialize every top-level map from one exact root manifest.
///
/// This is an explicit offline audit/GC boundary. Ordinary reopen, commands,
/// and queries never invoke it; they resolve exact keys or bounded pages from
/// the pinned manifest.
///
/// # Errors
///
/// Rejects invalid manifests or reachable objects and preserves resolver failures.
pub fn materialize_state_roots<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<MaterializedStateRoots> {
    materialize_state_root_families(manifest, resolver, &StateRootFamily::ALL)
}

/// Materialize every semantic family required by an explicit full audit.
///
/// This helper is intentionally isolated from every ordinary runtime entry
/// point. It reconstructs the aggregate projection only at this named offline
/// boundary.
fn materialize_full_semantic_audit_roots<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<MaterializedStateRoots> {
    materialize_state_root_families(manifest, resolver, &StateRootFamily::FULL_AUDIT)
}

fn materialize_state_root_families<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    families: &[StateRootFamily],
) -> DurableResult<MaterializedStateRoots> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let mut collections = BTreeMap::new();
    for &family in families {
        collections.insert(
            family,
            materialize_map(manifest.roots.get(family), &mut overlay)?,
        );
    }
    Ok(MaterializedStateRoots {
        manifest: manifest.clone(),
        collections,
    })
}

/// Materialize one complete ordered persistent log.
///
/// This is an open/audit operation; append paths update only the bounded AVL
/// spine.
///
/// # Errors
///
/// Rejects invalid log structure or reachable values and preserves resolver failures.
pub fn materialize_state_log<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    resolver: &mut R,
) -> DurableResult<Vec<StateRootValue>> {
    let mut overlay = ObjectOverlay::new(resolver);
    let audit = audit_log(root, &mut overlay)?;
    let mut values = Vec::with_capacity(audit.values().len());
    for value_id in audit.values() {
        values.push(overlay.load_value(value_id)?);
    }
    Ok(values)
}

/// Return the exact immutable object closure reachable from one manifest.
///
/// Explicit GC and full audit use this traversal. Ordinary reopen can
/// materialize only the collections it needs.
///
/// # Errors
///
/// Rejects invalid pinned authority or an unclosed object graph and preserves storage failures.
pub fn reachable_state_root_objects<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<BTreeSet<String>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let physical_manifest = load_reachable_object(resolver, &manifest.manifest_id)?;
    if physical_manifest != StateRootObject::Manifest(manifest.clone()) {
        return Err(DurableError::Integrity {
            code: "state_root_physical_manifest_mismatch".to_owned(),
            message: "pinned state-root manifest object differs from its exact authority"
                .to_owned(),
        });
    }

    let materialized = materialize_state_roots(manifest, resolver)?;
    for (family, values) in &materialized.collections {
        for (key, value) in values {
            verify_expected_reachable_value(value, &expected_value_for_family(*family, key)?)?;
        }
    }
    for root in [
        &manifest.roots.machine_plan_admissions,
        &manifest.roots.machine_artifact_admissions,
        &manifest.roots.machine_events,
        &manifest.roots.machine_admissions,
        &manifest.roots.machine_command_batch_admissions,
    ] {
        let _ = materialize_state_log(root, resolver)?;
    }
    if let Some(object_id) = &manifest.roots.machine_base {
        match load_reachable_object(resolver, object_id)? {
            StateRootObject::Value(value) => verify_expected_reachable_value(
                &value.value,
                &ExpectedStateRootValue::MachineBaseDescriptor,
            )?,
            _ => {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_base_kind_mismatch".to_owned(),
                    message: "Machine base reference resolves to another object kind".to_owned(),
                });
            }
        }
    }
    audit_pinned_machine_frontier(manifest, resolver)?;
    audit_history_compaction_receipts(manifest, resolver)?;
    audit_control_receipts(manifest, resolver)?;
    audit_run_current_memberships(manifest, resolver)?;
    audit_component_attempt_frontiers(manifest, resolver)?;
    audit_pending_wait_sources(manifest, resolver)?;
    audit_agent_target_claim_closure(manifest, resolver, &materialized)?;

    let mut reachable = BTreeSet::new();
    let mut queue = VecDeque::from([manifest.manifest_id.clone()]);
    while let Some(object_id) = queue.pop_front() {
        if !reachable.insert(object_id.clone()) {
            continue;
        }
        let object = load_reachable_object(resolver, &object_id)?;
        queue.extend(object.pending_references());
    }
    Ok(reachable)
}

fn audit_control_receipts<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<()> {
    let mut overlay = ObjectOverlay::new(resolver);
    for (key, value) in materialize_map(&manifest.roots.cancellation_receipts, &mut overlay)? {
        let receipt = value.decode(StateRootLeafKind::CancellationReceipt)?;
        verify_cancellation_receipt_leaf(&manifest.roots, &key, &receipt, &mut overlay)?;
    }
    for (key, value) in materialize_map(&manifest.roots.effect_resolution_receipts, &mut overlay)? {
        let receipt = value.decode(StateRootLeafKind::EffectResolutionReceipt)?;
        verify_effect_resolution_receipt_leaf(&manifest.roots, &key, &receipt, &mut overlay)?;
    }
    Ok(())
}

fn agent_tool_is_terminal(tool: &cymule_profile_protocol::agent::AgentToolCurrent) -> bool {
    matches!(
        tool.tool.status,
        cymule_profile_protocol::agent::ToolCallStatus::Completed
            | cymule_profile_protocol::agent::ToolCallStatus::Failed
            | cymule_profile_protocol::agent::ToolCallStatus::Cancelled
    )
}

fn audit_unmaterialized_agent_tool(
    phase: &cymule_profile_protocol::agent::AgentTargetClaimPhase,
    tool: Option<&cymule_profile_protocol::agent::AgentToolCurrent>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentTargetClaimPhase, ToolCallStatus};

    match phase {
        AgentTargetClaimPhase::Reserved { .. } => {
            if tool.is_none_or(|tool| tool.tool.status != ToolCallStatus::InProgress) {
                return Err(DurableError::Integrity {
                    code: "agent_target_claim_reserved_tool_invalid".to_owned(),
                    message: "Reserved Agent Tool claim lost its InProgress target".to_owned(),
                });
            }
        }
        AgentTargetClaimPhase::Released { .. } => {
            if tool.is_none_or(|tool| tool.tool.status != ToolCallStatus::InProgress) {
                return Err(DurableError::Integrity {
                    code: "agent_target_claim_released_tool_missing".to_owned(),
                    message: "Released Agent Tool claim lost its InProgress target".to_owned(),
                });
            }
        }
        AgentTargetClaimPhase::Materialized => unreachable!("caller selects unmaterialized claims"),
    }
    Ok(())
}

fn audit_agent_claim_currents<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    claims: &BTreeMap<String, cymule_profile_protocol::agent::AgentTargetClaimCurrent>,
    messages: &BTreeMap<String, cymule_profile_protocol::agent::AgentMessageCurrent>,
    tools: &BTreeMap<String, cymule_profile_protocol::agent::AgentToolCurrent>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentTargetClaimPhase, AgentTargetClaimTarget};

    for claim in claims.values() {
        let _ = crate::coordinator::verify_agent_target_claim_current_origin(
            manifest, resolver, claim,
        )?;
        if let AgentTargetClaimPhase::Released { stream_id, .. } = &claim.phase {
            let stream =
                load_agent_stream_current(manifest, resolver, &claim.session_id, stream_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "agent_target_claim_released_stream_missing".to_owned(),
                        message: "Released Agent target claim lost its Aborted stream".to_owned(),
                    })?;
            if stream.state != cymule_profile_protocol::agent::AgentStreamState::Aborted
                || AgentTargetClaimTarget::from_stream_target(&stream.target) != claim.target
            {
                return Err(DurableError::Integrity {
                    code: "agent_target_claim_released_stream_mismatch".to_owned(),
                    message: "Released Agent target claim changed its Aborted stream".to_owned(),
                });
            }
            audit_terminal_agent_stream(manifest, resolver, &stream)?;
        }
        let target_key = match &claim.target {
            AgentTargetClaimTarget::Message { message_id } => {
                cymule_profile_protocol::agent::agent_message_key(&claim.session_id, message_id)?
            }
            AgentTargetClaimTarget::Tool { tool_call_id } => {
                cymule_profile_protocol::agent::agent_tool_key(&claim.session_id, tool_call_id)?
            }
        };
        match (&claim.phase, &claim.target) {
            (AgentTargetClaimPhase::Materialized, AgentTargetClaimTarget::Message { .. }) => {
                let message = messages
                    .get(&target_key)
                    .ok_or_else(|| DurableError::Integrity {
                        code: "agent_target_claim_message_missing".to_owned(),
                        message: "Materialized Agent Message claim lost its immutable target"
                            .to_owned(),
                    })?;
                if message.order.admitted_by != claim.admitted_by {
                    return Err(DurableError::Integrity {
                        code: "agent_target_claim_message_origin_mismatch".to_owned(),
                        message: "Materialized Agent Message claim changed its admitting command"
                            .to_owned(),
                    });
                }
            }
            (AgentTargetClaimPhase::Materialized, AgentTargetClaimTarget::Tool { .. }) => {
                let tool = tools
                    .get(&target_key)
                    .ok_or_else(|| DurableError::Integrity {
                        code: "agent_target_claim_tool_missing".to_owned(),
                        message: "Materialized Agent Tool claim lost its terminal target"
                            .to_owned(),
                    })?;
                if !agent_tool_is_terminal(tool) || tool.admitted_by != claim.admitted_by {
                    return Err(DurableError::Integrity {
                        code: "agent_target_claim_tool_origin_mismatch".to_owned(),
                        message: "Materialized Agent Tool claim changed its terminal origin"
                            .to_owned(),
                    });
                }
            }
            (AgentTargetClaimPhase::Reserved { .. }, AgentTargetClaimTarget::Message { .. })
            | (AgentTargetClaimPhase::Released { .. }, AgentTargetClaimTarget::Message { .. }) => {
                if messages.contains_key(&target_key) {
                    return Err(DurableError::Integrity {
                        code: "agent_target_claim_message_phase_mismatch".to_owned(),
                        message: "Unmaterialized Agent Message claim has a persisted target"
                            .to_owned(),
                    });
                }
            }
            (AgentTargetClaimPhase::Reserved { .. }, AgentTargetClaimTarget::Tool { .. })
            | (AgentTargetClaimPhase::Released { .. }, AgentTargetClaimTarget::Tool { .. }) => {
                audit_unmaterialized_agent_tool(&claim.phase, tools.get(&target_key))?;
            }
        }
    }
    Ok(())
}

fn audit_agent_message_claims(
    claims: &BTreeMap<String, cymule_profile_protocol::agent::AgentTargetClaimCurrent>,
    messages: &BTreeMap<String, cymule_profile_protocol::agent::AgentMessageCurrent>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentTargetClaimPhase, AgentTargetClaimTarget};

    for (key, message) in messages {
        let target = AgentTargetClaimTarget::Message {
            message_id: message.message.message_id.clone(),
        };
        let claim_key =
            cymule_profile_protocol::agent::agent_target_claim_key(&message.session_id, &target)?;
        let claim = claims
            .get(&claim_key)
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_message_target_claim_missing".to_owned(),
                message: "Persisted Agent Message lost its materialized target claim".to_owned(),
            })?;
        if !matches!(claim.phase, AgentTargetClaimPhase::Materialized)
            || claim.admitted_by != message.order.admitted_by
            || *key
                != cymule_profile_protocol::agent::agent_message_key(
                    &message.session_id,
                    &message.message.message_id,
                )?
        {
            return Err(DurableError::Integrity {
                code: "agent_message_target_claim_mismatch".to_owned(),
                message: "Persisted Agent Message differs from its target claim".to_owned(),
            });
        }
    }
    Ok(())
}

fn audit_agent_tool_claims(
    claims: &BTreeMap<String, cymule_profile_protocol::agent::AgentTargetClaimCurrent>,
    tools: &BTreeMap<String, cymule_profile_protocol::agent::AgentToolCurrent>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentTargetClaimPhase, AgentTargetClaimTarget};

    for (key, tool) in tools {
        let target = AgentTargetClaimTarget::Tool {
            tool_call_id: tool.tool.tool_call_id.clone(),
        };
        let claim_key =
            cymule_profile_protocol::agent::agent_target_claim_key(&tool.session_id, &target)?;
        let claim = claims.get(&claim_key);
        if agent_tool_is_terminal(tool) {
            if claim.is_none_or(|claim| {
                !matches!(claim.phase, AgentTargetClaimPhase::Materialized)
                    || claim.admitted_by != tool.admitted_by
            }) {
                return Err(DurableError::Integrity {
                    code: "agent_tool_target_claim_missing".to_owned(),
                    message: "Terminal Agent Tool lost its materialized target claim".to_owned(),
                });
            }
        } else if claim
            .is_some_and(|claim| matches!(claim.phase, AgentTargetClaimPhase::Materialized))
        {
            return Err(DurableError::Integrity {
                code: "agent_tool_target_claim_phase_mismatch".to_owned(),
                message: "Non-terminal Agent Tool is materialized".to_owned(),
            });
        }
        if *key
            != cymule_profile_protocol::agent::agent_tool_key(
                &tool.session_id,
                &tool.tool.tool_call_id,
            )?
        {
            return Err(DurableError::Integrity {
                code: "agent_tool_key_mismatch".to_owned(),
                message: "Persisted Agent Tool changed its exact StateRoot key".to_owned(),
            });
        }
    }
    Ok(())
}

fn audit_agent_stream_claims<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    claims: &BTreeMap<String, cymule_profile_protocol::agent::AgentTargetClaimCurrent>,
    streams: &BTreeMap<String, cymule_profile_protocol::agent::AgentStreamCurrent>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{
        AgentStreamState, AgentTargetClaimPhase, AgentTargetClaimTarget,
    };

    for stream in streams.values() {
        crate::coordinator::verify_agent_stream_origin(manifest, resolver, stream)?;
        if stream.state != AgentStreamState::Open {
            audit_terminal_agent_stream(manifest, resolver, stream)?;
        }
        if let Some(reservation) = &stream.publication_reservation {
            let target = AgentTargetClaimTarget::from_stream_target(&stream.target);
            let claim_key = cymule_profile_protocol::agent::agent_target_claim_key(
                &stream.session_id,
                &target,
            )?;
            if claims.get(&claim_key).is_none_or(|claim| {
                claim.admitted_by != reservation.intent.command_id()
                    || claim.phase
                        != (AgentTargetClaimPhase::Reserved {
                            stream_id: stream.stream_id.clone(),
                            reservation_id: reservation.reservation_id.clone(),
                        })
            }) {
                return Err(DurableError::Integrity {
                    code: "agent_stream_target_claim_missing".to_owned(),
                    message: "Reserved Agent stream lost its exact target claim".to_owned(),
                });
            }
        }
    }
    Ok(())
}

fn audit_terminal_agent_stream<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    stream: &cymule_profile_protocol::agent::AgentStreamCurrent,
) -> DurableResult<()> {
    let command =
        load_agent_command(manifest, resolver, &stream.admitted_by)?.ok_or_else(|| {
            DurableError::Integrity {
                code: "agent_terminal_stream_command_missing".to_owned(),
                message: "Terminal Agent stream lost its admitting command".to_owned(),
            }
        })?;
    let receipt =
        load_agent_command_receipt(manifest, resolver, &stream.admitted_by)?.ok_or_else(|| {
            DurableError::Integrity {
                code: "agent_terminal_stream_receipt_missing".to_owned(),
                message: "Terminal Agent stream lost its admitting receipt".to_owned(),
            }
        })?;
    crate::coordinator::verify_agent_target_claim_receipt_graph(
        manifest, resolver, &command, &receipt,
    )?;
    if stream.state == cymule_profile_protocol::agent::AgentStreamState::Finalized {
        crate::coordinator::verify_agent_stream_finalization_graph(
            manifest, resolver, &command, &receipt,
        )?;
    } else if stream.state == cymule_profile_protocol::agent::AgentStreamState::Aborted {
        crate::coordinator::verify_agent_stream_abort_graph(
            manifest, resolver, &command, &receipt,
        )?;
    }
    Ok(())
}

fn audit_agent_target_claim_closure<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    materialized: &MaterializedStateRoots,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{
        AgentMessageCurrent, AgentStreamCurrent, AgentTargetClaimCurrent, AgentToolCurrent,
    };

    let claims: BTreeMap<String, AgentTargetClaimCurrent> = decode_family_map(
        &materialized.collections,
        StateRootFamily::AgentTargetClaims,
        StateRootLeafKind::AgentTargetClaimCurrent,
    )?;
    for (key, current) in &claims {
        current.verify()?;
        if cymule_profile_protocol::agent::agent_target_claim_key(
            &current.session_id,
            &current.target,
        )? != *key
        {
            return Err(DurableError::Integrity {
                code: "agent_target_claim_key_mismatch".to_owned(),
                message: "Agent target claim changed its exact StateRoot key".to_owned(),
            });
        }
    }
    let messages: BTreeMap<String, AgentMessageCurrent> = decode_family_map(
        &materialized.collections,
        StateRootFamily::AgentMessages,
        StateRootLeafKind::AgentMessageCurrent,
    )?;
    let tools: BTreeMap<String, AgentToolCurrent> = decode_family_map(
        &materialized.collections,
        StateRootFamily::AgentTools,
        StateRootLeafKind::AgentToolCurrent,
    )?;
    let streams: BTreeMap<String, AgentStreamCurrent> = decode_family_map(
        &materialized.collections,
        StateRootFamily::AgentStreams,
        StateRootLeafKind::AgentStreamCurrent,
    )?;
    for current in messages.values() {
        current.verify()?;
    }
    for current in tools.values() {
        current.verify()?;
    }
    for current in streams.values() {
        current.verify()?;
    }

    audit_agent_claim_currents(manifest, resolver, &claims, &messages, &tools)?;
    audit_agent_message_claims(&claims, &messages)?;
    audit_agent_tool_claims(&claims, &tools)?;
    audit_agent_stream_claims(manifest, resolver, &claims, &streams)?;
    Ok(())
}

fn audit_component_attempt_frontiers<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<()> {
    let mut overlay = ObjectOverlay::new(resolver);
    let occurrence_values = materialize_map(&manifest.roots.component_occurrences, &mut overlay)?;
    let attempt_values = materialize_map(&manifest.roots.operation_attempts, &mut overlay)?;
    let mut attempts_by_occurrence = group_component_attempt_history(attempt_values)?;
    for (occurrence_id, value) in occurrence_values {
        let occurrence: crate::ComponentOccurrence =
            value.decode(StateRootLeafKind::ComponentOccurrence)?;
        occurrence.verify()?;
        if occurrence.occurrence_id != occurrence_id {
            return Err(DurableError::Integrity {
                code: "state_root_component_occurrence_key_mismatch".to_owned(),
                message: format!("component occurrence key {occurrence_id} changed identity"),
            });
        }
        let mut attempts = attempts_by_occurrence
            .remove(&occurrence_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_component_attempt_history_missing".to_owned(),
                message: format!(
                    "component occurrence {occurrence_id} has no provider Attempt history"
                ),
            })?;
        attempts.sort_by_key(|attempt| attempt.attempt_ordinal);
        if attempts.len() as u64 != occurrence.attempt_count
            || attempts.last().map(|attempt| attempt.attempt_id.as_str())
                != Some(occurrence.latest_attempt_id.as_str())
            || attempts.windows(2).any(|pair| {
                pair[1].previous_attempt_id.as_deref() != Some(pair[0].attempt_id.as_str())
            })
        {
            return Err(DurableError::Integrity {
                code: "state_root_component_attempt_chain_mismatch".to_owned(),
                message: format!(
                    "component occurrence {occurrence_id} Attempt chain is forked or incomplete"
                ),
            });
        }
        let latest = attempts.last().expect("non-empty history was checked");
        pinned_machine::validate_component_attempt_frontier(&occurrence, latest)?;
        let continuation: crate::Continuation = map_get(
            &manifest.roots.continuations,
            &occurrence.run_id,
            &mut overlay,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_component_continuation_missing".to_owned(),
            message: format!("component occurrence {occurrence_id} has no owning Continuation"),
        })?
        .decode(StateRootLeafKind::Continuation)?;
        continuation.verify_wire()?;
        if continuation.run_id != occurrence.run_id {
            return Err(DurableError::Integrity {
                code: "state_root_component_continuation_owner_mismatch".to_owned(),
                message: format!("component occurrence {occurrence_id} Continuation changed Run"),
            });
        }
        let mut attempt_refs = attempts.iter().collect::<Vec<_>>();
        crate::model::validate_operation_attempt_history(
            &occurrence,
            &mut attempt_refs,
            continuation.status,
        )?;
        validate_component_query_member(
            &manifest.roots,
            &occurrence.run_id,
            RunQueryIndexKind::Occurrences,
            &occurrence.occurrence_id,
            StateRootLeafKind::ComponentOccurrence,
            &occurrence,
            &mut overlay,
        )?;
        for attempt in &attempts {
            validate_component_query_member(
                &manifest.roots,
                &attempt.run_id,
                RunQueryIndexKind::Attempts,
                &attempt.attempt_id,
                StateRootLeafKind::OperationAttempt,
                attempt,
                &mut overlay,
            )?;
        }
    }
    if let Some((occurrence_id, _)) = attempts_by_occurrence.into_iter().next() {
        return Err(DurableError::Integrity {
            code: "state_root_operation_attempt_orphan".to_owned(),
            message: format!(
                "operation Attempt history references missing occurrence {occurrence_id}"
            ),
        });
    }
    Ok(())
}

fn group_component_attempt_history(
    attempt_values: BTreeMap<String, StateRootValue>,
) -> DurableResult<BTreeMap<String, Vec<crate::OperationAttempt>>> {
    let mut attempts_by_occurrence = BTreeMap::<String, Vec<crate::OperationAttempt>>::new();
    for (attempt_id, value) in attempt_values {
        let attempt: crate::OperationAttempt = value.decode(StateRootLeafKind::OperationAttempt)?;
        attempt.verify()?;
        if attempt.attempt_id != attempt_id {
            return Err(DurableError::Integrity {
                code: "state_root_operation_attempt_key_mismatch".to_owned(),
                message: format!("operation Attempt key {attempt_id} changed identity"),
            });
        }
        attempts_by_occurrence
            .entry(attempt.occurrence_id.clone())
            .or_default()
            .push(attempt);
    }
    Ok(attempts_by_occurrence)
}

fn audit_pending_wait_sources<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<()> {
    let mut overlay = ObjectOverlay::new(resolver);
    let mut externally_indexed = BTreeSet::new();
    for (signal, root) in [
        (true, &manifest.roots.pending_signal_sources),
        (false, &manifest.roots.pending_timer_sources),
    ] {
        for (source_key, descriptor) in materialize_map(root, &mut overlay)? {
            let StateRootValue::PendingWaitSource { source, .. } = &descriptor else {
                return Err(DurableError::Integrity {
                    code: "state_root_pending_wait_source_value_kind_mismatch".to_owned(),
                    message: format!("pending-Wait source {source_key} has the wrong descriptor"),
                });
            };
            if pending_wait_source_key(source)? != source_key
                || signal != matches!(source, crate::WaitActivationSource::Signal { .. })
            {
                return Err(DurableError::Integrity {
                    code: "state_root_pending_wait_source_key_mismatch".to_owned(),
                    message: format!("pending-Wait source key {source_key} changed typed identity"),
                });
            }
            let waits = descriptor.decode_pending_wait_source(source)?;
            for (wait_id, value) in materialize_map(&waits, &mut overlay)? {
                let wait: crate::WaitCondition = value.decode(StateRootLeafKind::Wait)?;
                wait.verify_wire()?;
                if wait.wait_id != wait_id
                    || wait.state != crate::WaitState::Pending
                    || !wait_matches_source(&wait, source)
                    || !externally_indexed.insert(wait_id.clone())
                {
                    return Err(DurableError::Integrity {
                        code: "state_root_pending_wait_source_member_mismatch".to_owned(),
                        message: format!(
                            "pending-Wait source member {wait_id} changed owner, state, or source"
                        ),
                    });
                }
                if map_get(&manifest.roots.waits, &wait_id, &mut overlay)?.as_ref() != Some(&value)
                {
                    return Err(DurableError::Integrity {
                        code: "state_root_pending_wait_source_current_mismatch".to_owned(),
                        message: format!(
                            "pending-Wait source member {wait_id} differs from global current"
                        ),
                    });
                }
                let run_indexes = map_get(
                    &manifest.roots.run_query_indexes,
                    &wait.run_id,
                    &mut overlay,
                )?
                .ok_or_else(|| DurableError::Integrity {
                    code: "run_query_indexes_missing".to_owned(),
                    message: format!(
                        "pending Wait {wait_id} has no Run current-membership descriptor"
                    ),
                })?
                .decode_run_query_indexes(&wait.run_id)?;
                if map_get(&run_indexes.pending_waits, &wait_id, &mut overlay)?.as_ref()
                    != Some(&value)
                {
                    return Err(DurableError::Integrity {
                        code: "state_root_pending_wait_run_index_mismatch".to_owned(),
                        message: format!(
                            "pending Wait {wait_id} differs from its per-Run current index"
                        ),
                    });
                }
            }
        }
    }
    for (run_id, descriptor) in materialize_map(&manifest.roots.run_query_indexes, &mut overlay)? {
        let roots = descriptor.decode_run_query_indexes(&run_id)?;
        for (wait_id, value) in materialize_map(&roots.pending_waits, &mut overlay)? {
            let wait: crate::WaitCondition = value.decode(StateRootLeafKind::Wait)?;
            if !matches!(wait.kind, crate::WaitKind::Input { .. })
                && !externally_indexed.contains(&wait_id)
            {
                return Err(DurableError::Integrity {
                    code: "state_root_pending_wait_source_reverse_missing".to_owned(),
                    message: format!(
                        "Run {run_id} pending Wait {wait_id} has no external source membership"
                    ),
                });
            }
        }
    }
    Ok(())
}

fn wait_matches_source(wait: &crate::WaitCondition, source: &crate::WaitActivationSource) -> bool {
    matches!(
        (&wait.kind, source),
        (
            crate::WaitKind::Signal { key },
            crate::WaitActivationSource::Signal { key: expected }
        ) if key == expected
    ) || matches!(
        (&wait.kind, source),
        (
            crate::WaitKind::Timer { timer_id },
            crate::WaitActivationSource::Timer { timer_id: expected }
        ) if timer_id == expected
    )
}

fn audit_run_current_memberships<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<()> {
    let mut overlay = ObjectOverlay::new(resolver);
    let mut owned_waits = BTreeSet::new();
    let mut owned_intents = BTreeSet::new();
    for (run_id, descriptor) in materialize_map(&manifest.roots.run_query_indexes, &mut overlay)? {
        let roots = descriptor.decode_run_query_indexes(&run_id)?;
        audit_run_wait_memberships(manifest, &run_id, &roots, &mut owned_waits, &mut overlay)?;
        audit_run_pending_wait_members(manifest, &run_id, &roots, &mut overlay)?;
        let (effects, active_effects) = audit_run_effect_memberships(
            manifest,
            &run_id,
            &roots,
            &mut owned_intents,
            &mut overlay,
        )?;
        let active_leases = materialize_map(&roots.active_leases, &mut overlay)?;
        audit_run_active_leases(
            manifest,
            &run_id,
            &active_effects,
            &active_leases,
            &mut overlay,
        )?;
        for (intent_id, value) in &active_effects {
            let dispatch: crate::EffectDispatch = value.decode(StateRootLeafKind::Outbox)?;
            dispatch.verify_wire()?;
            if dispatch.state == crate::OutboxState::Claimed
                && !active_leases.contains_key(intent_id)
            {
                return Err(DurableError::Integrity {
                    code: "state_root_claimed_effect_lease_missing".to_owned(),
                    message: format!(
                        "Run {run_id} claimed Effect {intent_id} has no active lease member"
                    ),
                });
            }
        }
        if let Some(terminal) = &roots.terminal {
            audit_terminal_sidecar_shadow(
                manifest,
                &run_id,
                &roots,
                terminal,
                &effects,
                &mut overlay,
            )?;
        }
    }
    if u64::try_from(owned_waits.len()).ok() != Some(manifest.roots.waits.entries) {
        return Err(DurableError::Integrity {
            code: "state_root_wait_summary_set_mismatch".to_owned(),
            message: "global Waits and Run-local query summaries have different key sets"
                .to_owned(),
        });
    }
    if u64::try_from(owned_intents.len()).ok() != Some(manifest.roots.outbox.entries) {
        return Err(DurableError::Integrity {
            code: "state_root_outbox_owner_set_mismatch".to_owned(),
            message: "immutable Effect owners and Run-local outboxes have different key sets"
                .to_owned(),
        });
    }
    Ok(())
}

fn audit_run_wait_memberships<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    run_id: &str,
    roots: &RunQueryIndexRoots,
    owned_waits: &mut BTreeSet<String>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (wait_id, value) in materialize_map(&roots.waits, overlay)? {
        let summary: crate::DurableWaitSummary = value.decode(StateRootLeafKind::WaitSummary)?;
        summary.verify()?;
        let wait: crate::WaitCondition = map_get(&manifest.roots.waits, &wait_id, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_wait_summary_current_missing".to_owned(),
                message: format!(
                    "Run {run_id} Wait summary {wait_id} has no complete global current"
                ),
            })?
            .decode(StateRootLeafKind::Wait)?;
        wait.verify_wire()?;
        if summary.wait_id != wait_id
            || summary.run_id != run_id
            || wait.wait_id != wait_id
            || wait.run_id != run_id
            || summary != crate::DurableWaitSummary::from_wait(&wait)
            || !owned_waits.insert(wait_id.clone())
        {
            return Err(DurableError::Integrity {
                code: "state_root_wait_summary_mismatch".to_owned(),
                message: format!(
                    "Run {run_id} Wait summary {wait_id} changed its key, owner, or current projection"
                ),
            });
        }
    }
    Ok(())
}

fn audit_run_effect_memberships<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    run_id: &str,
    roots: &RunQueryIndexRoots,
    owned_intents: &mut BTreeSet<String>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<(
    BTreeMap<String, StateRootValue>,
    BTreeMap<String, StateRootValue>,
)> {
    let effects = materialize_map(&roots.effects, overlay)?;
    for (intent_id, value) in &effects {
        let dispatch: crate::EffectDispatch = value.decode(StateRootLeafKind::Outbox)?;
        dispatch.verify_wire()?;
        let owner = load_outbox_owner(&manifest.roots, intent_id, overlay)?;
        if dispatch.run_id != run_id
            || dispatch.intent_id != *intent_id
            || !owned_intents.insert(intent_id.clone())
            || owner.as_ref().is_none_or(|owner| owner.run_id != run_id)
        {
            return Err(DurableError::Integrity {
                code: "state_root_outbox_owner_mismatch".to_owned(),
                message: "Run-local Effect and immutable owner locator are not one authority"
                    .to_owned(),
            });
        }
    }
    let active = materialize_map(&roots.active_effects, overlay)?;
    for (intent_id, value) in &active {
        let dispatch: crate::EffectDispatch = value.decode(StateRootLeafKind::Outbox)?;
        dispatch.verify_wire()?;
        if dispatch.intent_id != *intent_id
            || dispatch.run_id != run_id
            || !matches!(
                dispatch.state,
                crate::OutboxState::Pending
                    | crate::OutboxState::Claimed
                    | crate::OutboxState::Unknown
            )
            || effects.get(intent_id) != Some(value)
        {
            return Err(DurableError::Integrity {
                code: "state_root_active_effect_index_mismatch".to_owned(),
                message: format!(
                    "Run {run_id} active Effect {intent_id} changed owner, state, or current"
                ),
            });
        }
    }
    for (intent_id, value) in &effects {
        let dispatch: crate::EffectDispatch = value.decode(StateRootLeafKind::Outbox)?;
        let is_active = matches!(
            dispatch.state,
            crate::OutboxState::Pending | crate::OutboxState::Claimed | crate::OutboxState::Unknown
        );
        if is_active != active.contains_key(intent_id) {
            return Err(DurableError::Integrity {
                code: "state_root_active_effect_membership_missing".to_owned(),
                message: "Run-local Effect state and active membership disagree".to_owned(),
            });
        }
    }
    Ok((effects, active))
}

fn audit_run_active_leases<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    run_id: &str,
    active_effects: &BTreeMap<String, StateRootValue>,
    active_leases: &BTreeMap<String, StateRootValue>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (resource, value) in active_leases {
        let lease: crate::CoordinationLease = value.decode(StateRootLeafKind::Lease)?;
        lease.verify()?;
        let dispatch = active_effects
            .get(resource)
            .map(|value| value.decode::<crate::EffectDispatch>(StateRootLeafKind::Outbox))
            .transpose()?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_active_lease_effect_missing".to_owned(),
                message: format!("Run {run_id} active lease {resource} has no active Effect"),
            })?;
        if lease.resource != *resource
            || dispatch.state != crate::OutboxState::Claimed
            || dispatch.claim_owner.as_deref() != Some(lease.owner.as_str())
            || dispatch.claim_epoch != lease.epoch
            || map_get(&manifest.roots.leases, resource, overlay)?.as_ref() != Some(value)
        {
            return Err(DurableError::Integrity {
                code: "state_root_active_lease_index_mismatch".to_owned(),
                message: format!(
                    "Run {run_id} active lease {resource} changed owner, fence, Effect, or current"
                ),
            });
        }
    }
    Ok(())
}

fn audit_terminal_sidecar_shadow<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    run_id: &str,
    query: &RunQueryIndexRoots,
    terminal: &RunTerminalSidecarCurrent,
    source_effects: &BTreeMap<String, StateRootValue>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let value = map_get(
        &manifest.machine_frontier.paged_transitions,
        &terminal.transition_id,
        overlay,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "terminal_sidecar_core_transition_missing".to_owned(),
        message: "terminal companion has no exact retained Core transition".to_owned(),
    })?;
    let StateRootValue::MachinePagedTransitionCurrent {
        current: transition,
    } = value
    else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_paged_value_kind_mismatch".to_owned(),
            message: "terminal companion references a non-transition value".to_owned(),
        });
    };
    verify_terminal_sidecar_source(manifest, &transition, &manifest.roots, query, overlay)?;
    if transition.run_id != run_id {
        return Err(DurableError::Integrity {
            code: "terminal_sidecar_run_fence_mismatch".to_owned(),
            message: "terminal shadow companion changed its owning Run".to_owned(),
        });
    }
    let effects = materialize_map(&terminal.effects, overlay)?;
    let active_effects = materialize_map(&terminal.active_effects, overlay)?;
    let active_leases = materialize_map(&terminal.active_leases, overlay)?;
    if effects.len() != source_effects.len()
        || u64::try_from(effects.len()).ok() != Some(transition.shadow.children.effects.entries)
    {
        return Err(DurableError::Integrity {
            code: "terminal_shadow_effect_set_mismatch".to_owned(),
            message: "terminal shadow Effect sets do not match their exact source".to_owned(),
        });
    }
    let mut expected_active = BTreeMap::new();
    let mut expected_leases = BTreeMap::new();
    for (intent_id, value) in effects {
        let source = source_effects
            .get(&intent_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "terminal_shadow_effect_source_missing".to_owned(),
                message: "terminal shadow introduced a new Effect".to_owned(),
            })?;
        let mut expected: crate::EffectDispatch = source.decode(StateRootLeafKind::Outbox)?;
        let effect: cymule_core::EffectProjection =
            map_get(&transition.shadow.children.effects, &intent_id, overlay)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "terminal_shadow_core_effect_missing".to_owned(),
                    message: "terminal shadow outbox has no exact Core Effect".to_owned(),
                })?
                .decode(StateRootLeafKind::MachineEffect)?;
        if expected.state == crate::OutboxState::Pending
            && effect.phase == cymule_core::EffectPhase::CancelledBeforeRelease
        {
            expected.state = crate::OutboxState::CancelledBeforeRelease;
        } else if expected.state == crate::OutboxState::Claimed
            && effect.outcome == cymule_core::WorldOutcome::Unknown
        {
            expected.state = crate::OutboxState::Unknown;
        }
        crate::model::synchronize_pinned_effect_projection(&effect, &mut expected)?;
        if value != StateRootValue::encode(StateRootLeafKind::Outbox, &expected)? {
            return Err(DurableError::Integrity {
                code: "terminal_shadow_outbox_mismatch".to_owned(),
                message: "terminal shadow outbox differs from its Core-derived post".to_owned(),
            });
        }
        if matches!(
            expected.state,
            crate::OutboxState::Pending | crate::OutboxState::Claimed | crate::OutboxState::Unknown
        ) {
            expected_active.insert(intent_id.clone(), value);
        }
        if expected.state == crate::OutboxState::Claimed {
            let lease = map_get(&query.active_leases, &intent_id, overlay)?.ok_or_else(|| {
                DurableError::Integrity {
                    code: "terminal_shadow_source_lease_missing".to_owned(),
                    message: "terminal shadow claim has no exact source lease".to_owned(),
                }
            })?;
            expected_leases.insert(intent_id, lease);
        }
    }
    if expected_active != active_effects || expected_leases != active_leases {
        return Err(DurableError::Integrity {
            code: "terminal_shadow_membership_mismatch".to_owned(),
            message: "terminal shadow active Effect or lease membership is not exact".to_owned(),
        });
    }
    Ok(())
}

fn audit_run_pending_wait_members<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    run_id: &str,
    roots: &RunQueryIndexRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let pending_waits = materialize_map(&roots.pending_waits, overlay)?;
    for (wait_id, value) in &pending_waits {
        let wait: crate::WaitCondition = value.decode(StateRootLeafKind::Wait)?;
        wait.verify_wire()?;
        if wait.wait_id != *wait_id
            || wait.run_id != run_id
            || wait.state != crate::WaitState::Pending
        {
            return Err(DurableError::Integrity {
                code: "state_root_pending_wait_index_mismatch".to_owned(),
                message: format!(
                    "Run {run_id} pending-Wait member {wait_id} changed owner or state"
                ),
            });
        }
        if map_get(&manifest.roots.waits, wait_id, overlay)?.as_ref() != Some(value) {
            return Err(DurableError::Integrity {
                code: "state_root_pending_wait_current_mismatch".to_owned(),
                message: format!(
                    "Run {run_id} pending-Wait member {wait_id} differs from global current"
                ),
            });
        }
    }
    Ok(())
}

fn audit_pinned_machine_frontier<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<()> {
    let mut overlay = ObjectOverlay::new(resolver);
    audit_hot_machine_batches(manifest, &mut overlay)?;
    let run_values = materialize_map(&manifest.machine_frontier.runs, &mut overlay)?;
    for (run_id, value) in run_values {
        let StateRootValue::MachineRunCurrent { current } = value else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_run_value_kind_mismatch".to_owned(),
                message: format!("Machine Run {run_id} has the wrong typed value"),
            });
        };
        if current.run_id != run_id {
            return Err(DurableError::Integrity {
                code: "state_root_machine_run_key_mismatch".to_owned(),
                message: format!("Machine Run key {run_id} changed identity"),
            });
        }
        audit_machine_run_roots(
            &run_id,
            &current.children,
            &current.order,
            &current.indexes,
            &mut overlay,
        )?;
    }

    audit_machine_leaf_map(
        &manifest.machine_frontier.facts,
        StateRootLeafKind::MachineFact,
        &mut overlay,
    )?;
    let pending = materialize_map(&manifest.machine_frontier.pending_commands, &mut overlay)?;
    let transitions = materialize_map(&manifest.machine_frontier.paged_transitions, &mut overlay)?;
    if pending.len() != transitions.len() {
        return Err(DurableError::Integrity {
            code: "state_root_machine_paged_cardinality_mismatch".to_owned(),
            message: "Machine pending-command and paged-transition maps disagree".to_owned(),
        });
    }
    for (command_id, value) in pending {
        let StateRootValue::MachinePendingCommand {
            command_id: retained_command,
            transition_id,
        } = value
        else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_pending_value_kind_mismatch".to_owned(),
                message: format!("Machine pending command {command_id} has the wrong value"),
            });
        };
        let Some(StateRootValue::MachinePagedTransitionCurrent { current }) =
            transitions.get(&transition_id)
        else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_pending_transition_missing".to_owned(),
                message: format!(
                    "Machine pending command {command_id} has no exact paged transition"
                ),
            });
        };
        if retained_command != command_id
            || current.command_id != command_id
            || current.transition_id != transition_id
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_pending_transition_mismatch".to_owned(),
                message: "Machine pending command and transition identities disagree".to_owned(),
            });
        }
        if map_get(&manifest.roots.machine_commands, &command_id, &mut overlay)?.is_some() {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_authority_overlap".to_owned(),
                message: format!(
                    "Machine command {command_id} exists in both pending and hot authority"
                ),
            });
        }
        audit_paged_transition_roots(current, &mut overlay)?;
        verify_pending_terminal_sidecars(manifest, current, &mut overlay)?;
    }
    for (transition_id, value) in transitions {
        let StateRootValue::MachinePagedTransitionCurrent { current } = value else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_paged_value_kind_mismatch".to_owned(),
                message: format!("Machine paged transition {transition_id} has the wrong value"),
            });
        };
        if current.transition_id != transition_id {
            return Err(DurableError::Integrity {
                code: "state_root_machine_paged_key_mismatch".to_owned(),
                message: format!("Machine paged transition key {transition_id} changed identity"),
            });
        }
    }

    Ok(())
}

fn audit_paged_transition_roots<R: StateRootResolver + ?Sized>(
    current: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_core::durable_internal::MachineRunLogSelector as Log;

    audit_machine_run_roots(
        &current.run_id,
        &current.shadow.children,
        &current.shadow.order,
        &current.shadow.indexes,
        overlay,
    )?;
    let _ = pinned_machine::load_paged_material_admission(current, overlay)?;
    let effect_selector = match &current.action {
        cymule_core::durable_internal::MachinePagedTransitionAction::CommitScope { scope_id } => {
            Log::ScopeMutatingEffects {
                scope_id: scope_id.clone(),
            }
        }
        cymule_core::durable_internal::MachinePagedTransitionAction::AbortScope { scope_id } => {
            Log::ScopeEffects {
                scope_id: scope_id.clone(),
            }
        }
        cymule_core::durable_internal::MachinePagedTransitionAction::FailRun
        | cymule_core::durable_internal::MachinePagedTransitionAction::CancelRun => Log::Effects,
    };
    audit_machine_order_log(
        &current.effect_source,
        &current.run_id,
        &effect_selector,
        overlay,
    )?;
    audit_machine_order_log(
        &current.scope_source,
        &current.run_id,
        &Log::Scopes,
        overlay,
    )?;
    Ok(())
}

fn audit_hot_machine_batches<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let batches = materialize_map(&manifest.roots.machine_command_batches, overlay)?;
    audit_machine_batch_order(
        &manifest.roots.machine_command_batch_admissions,
        &batches,
        overlay,
    )?;
    let commands = materialize_map(&manifest.roots.machine_commands, overlay)?;
    for (command_id, value) in &commands {
        let StateRootValue::MachineCommandCurrent { record, .. } = value else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_value_kind_mismatch".to_owned(),
                message: format!(
                    "Machine hot command {command_id} is not a composite authority leaf"
                ),
            });
        };
        let batch = batches
            .get(&record.batch_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_command_batch_missing".to_owned(),
                message: format!(
                    "Machine hot command {command_id} references missing batch {}",
                    record.batch_id
                ),
            })?
            .decode::<cymule_core::durable_internal::MachineCommandBatchRecord>(
                StateRootLeafKind::MachineCommandBatch,
            )?;
        let position = usize::try_from(record.batch_position)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        if usize::try_from(record.batch_len).ok() != Some(batch.members.len())
            || batch.members.get(position).is_none_or(|member| {
                member.command_id != *command_id || member.semantic_hash != record.semantic_hash
            })
            || batch.receipts.get(position) != Some(&record.receipt)
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_batch_membership_mismatch".to_owned(),
                message: format!(
                    "Machine hot command {command_id} changed its exact batch membership"
                ),
            });
        }
    }
    for value in batches.values() {
        let batch: cymule_core::durable_internal::MachineCommandBatchRecord =
            value.decode(StateRootLeafKind::MachineCommandBatch)?;
        if batch
            .members
            .iter()
            .any(|member| !commands.contains_key(&member.command_id))
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_batch_member_missing".to_owned(),
                message: format!(
                    "Machine hot batch {} has a missing command member",
                    batch.batch_id
                ),
            });
        }
    }
    Ok(())
}

fn audit_machine_batch_order<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    batches: &BTreeMap<String, StateRootValue>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let order = audit_log(root, &mut *overlay)?;
    if order.values().len() != batches.len() {
        return Err(DurableError::Integrity {
            code: "state_root_machine_batch_order_mismatch".to_owned(),
            message: "Machine command-batch map and order lengths disagree".to_owned(),
        });
    }
    let mut ordered_ids = Vec::with_capacity(order.values().len());
    for value_id in order.values() {
        let batch: cymule_core::durable_internal::MachineCommandBatchRecord = overlay
            .load_value(value_id)?
            .decode(StateRootLeafKind::MachineCommandBatch)?;
        batch.verify()?;
        let stored = batches
            .get(&batch.batch_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_batch_order_value_missing".to_owned(),
                message: format!(
                    "Machine command-batch order references missing batch {}",
                    batch.batch_id
                ),
            })?
            .decode::<cymule_core::durable_internal::MachineCommandBatchRecord>(
                StateRootLeafKind::MachineCommandBatch,
            )?;
        if stored != batch {
            return Err(DurableError::Integrity {
                code: "state_root_machine_batch_order_value_mismatch".to_owned(),
                message: format!(
                    "Machine command-batch order changed batch {}",
                    batch.batch_id
                ),
            });
        }
        ordered_ids.push(batch.batch_id);
    }
    if ordered_ids.iter().collect::<BTreeSet<_>>().len() != ordered_ids.len() {
        return Err(DurableError::Integrity {
            code: "state_root_machine_batch_order_duplicate".to_owned(),
            message: "Machine command-batch order repeats an identity".to_owned(),
        });
    }
    Ok(())
}

fn audit_machine_run_roots<R: StateRootResolver + ?Sized>(
    run_id: &str,
    children: &cymule_core::durable_internal::MachineRunChildRoots,
    order: &cymule_core::durable_internal::MachineRunOrderRoots,
    indexes: &cymule_core::durable_internal::MachineRunIndexRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_core::durable_internal::{
        MachineRunIndexSelector as Index, MachineRunLogSelector as Log,
    };

    let scopes = materialize_map(&children.scopes, overlay)?;
    for (scope_id, value) in scopes {
        let StateRootValue::MachineScopeCurrent { current, .. } = value else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_scope_value_kind_mismatch".to_owned(),
                message: format!("Machine Scope {scope_id} has the wrong typed value"),
            });
        };
        if current.scope_id != scope_id {
            return Err(DurableError::Integrity {
                code: "state_root_machine_scope_key_mismatch".to_owned(),
                message: format!("Machine Scope key {scope_id} changed identity"),
            });
        }
        for (root, selector) in [
            (
                &current.effects,
                Index::ScopeEffects {
                    scope_id: scope_id.clone(),
                },
            ),
            (
                &current.mutating_effects,
                Index::ScopeMutatingEffects {
                    scope_id: scope_id.clone(),
                },
            ),
            (
                &current.abort_transitions,
                Index::ScopeAbortTransitions {
                    scope_id: scope_id.clone(),
                },
            ),
            (
                &current.abort_blockers,
                Index::ScopeAbortBlockers {
                    scope_id: scope_id.clone(),
                },
            ),
        ] {
            audit_machine_index_map(root, run_id, &selector, overlay)?;
        }
        audit_machine_order_log(
            &current.effect_order,
            run_id,
            &Log::ScopeEffects {
                scope_id: scope_id.clone(),
            },
            overlay,
        )?;
        audit_machine_order_log(
            &current.mutating_effect_order,
            run_id,
            &Log::ScopeMutatingEffects { scope_id },
            overlay,
        )?;
    }
    audit_machine_leaf_map(&children.effects, StateRootLeafKind::MachineEffect, overlay)?;
    audit_machine_leaf_map(
        &children.obligations,
        StateRootLeafKind::MachineObligation,
        overlay,
    )?;
    audit_machine_leaf_map(
        &children.attempts,
        StateRootLeafKind::MachineAttempt,
        overlay,
    )?;
    for (root, selector) in [
        (&indexes.governance_effects, Index::GovernanceEffects),
        (&indexes.unknown_effects, Index::UnknownEffects),
        (&indexes.pending_effects, Index::PendingEffects),
        (
            &indexes.terminal_transition_effects,
            Index::TerminalTransitionEffects,
        ),
        (&indexes.open_scopes, Index::OpenScopes),
        (
            &indexes.unresolved_obligations,
            Index::UnresolvedObligations,
        ),
    ] {
        audit_machine_index_map(root, run_id, &selector, overlay)?;
    }
    for (root, selector) in [
        (&order.scopes, Log::Scopes),
        (&order.effects, Log::Effects),
        (&order.obligations, Log::Obligations),
        (&order.attempts, Log::Attempts),
        (&order.plans, Log::Plans),
        (&order.bindings, Log::Bindings),
    ] {
        audit_machine_order_log(root, run_id, &selector, overlay)?;
    }
    Ok(())
}

fn audit_machine_leaf_map<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    kind: StateRootLeafKind,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for value in materialize_map(root, overlay)?.into_values() {
        verify_expected_reachable_value(&value, &ExpectedStateRootValue::Leaf(kind))?;
    }
    Ok(())
}

fn audit_machine_index_map<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    run_id: &str,
    selector: &cymule_core::durable_internal::MachineRunIndexSelector,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (entry, value) in materialize_map(root, overlay)? {
        if value
            != StateRootValue::machine_index_membership(
                run_id.to_owned(),
                selector.clone(),
                entry.clone(),
            )?
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_index_value_mismatch".to_owned(),
                message: format!("Machine index member {entry} changed typed authority"),
            });
        }
    }
    Ok(())
}

fn audit_machine_order_log<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    run_id: &str,
    selector: &cymule_core::durable_internal::MachineRunLogSelector,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let audit = audit_log(root, &mut *overlay)?;
    for value_id in audit.values() {
        let StateRootValue::MachineOrderEntry {
            run_id: retained_run,
            selector: retained_selector,
            entry,
        } = overlay.load_value(value_id)?
        else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_order_value_kind_mismatch".to_owned(),
                message: format!("Machine ordered value {value_id} has the wrong kind"),
            });
        };
        if retained_run != run_id
            || retained_selector != *selector
            || StateValueObject::new(StateRootValue::machine_order_entry(
                retained_run,
                retained_selector,
                entry,
            )?)?
            .object_id
                != *value_id
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_order_value_mismatch".to_owned(),
                message: format!("Machine ordered value {value_id} changed typed authority"),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExpectedStateRootValue {
    Leaf(StateRootLeafKind),
    MachineBaseDescriptor,
    MachineCommandCurrent(String),
    ApplicationJournal(String),
    ApplicationJournalRecordManifests(String),
    ResourceHandoffIndex(String),
    ResourceHandoffActivationIndex(String),
    ResourceHandoffSlots(String),
    AgentMessageIndex(String),
    AgentUnresolvedOccurrenceIndex(String),
    AgentOpenStreamIndex(String),
    RunQueryIndexes(String),
    PendingWaitSource { signal: bool, key: String },
}

fn expected_value_for_family(
    family: StateRootFamily,
    key: &str,
) -> DurableResult<ExpectedStateRootValue> {
    match family.expected_leaf_kind() {
        Some(kind) => Ok(ExpectedStateRootValue::Leaf(kind)),
        None if family == StateRootFamily::ApplicationJournals => {
            Ok(ExpectedStateRootValue::ApplicationJournal(key.to_owned()))
        }
        None if family == StateRootFamily::ApplicationJournalRecordManifests => Ok(
            ExpectedStateRootValue::ApplicationJournalRecordManifests(key.to_owned()),
        ),
        None if family == StateRootFamily::ResourceHandoffIndexes => {
            Ok(ExpectedStateRootValue::ResourceHandoffIndex(key.to_owned()))
        }
        None if family == StateRootFamily::ResourceHandoffActivationIndexes => Ok(
            ExpectedStateRootValue::ResourceHandoffActivationIndex(key.to_owned()),
        ),
        None if family == StateRootFamily::ResourceHandoffSlots => {
            Ok(ExpectedStateRootValue::ResourceHandoffSlots(key.to_owned()))
        }
        None if family == StateRootFamily::AgentMessageIndexes => {
            Ok(ExpectedStateRootValue::AgentMessageIndex(key.to_owned()))
        }
        None if family == StateRootFamily::AgentUnresolvedOccurrenceIndexes => Ok(
            ExpectedStateRootValue::AgentUnresolvedOccurrenceIndex(key.to_owned()),
        ),
        None if family == StateRootFamily::AgentOpenStreamIndexes => {
            Ok(ExpectedStateRootValue::AgentOpenStreamIndex(key.to_owned()))
        }
        None if family == StateRootFamily::RunQueryIndexes => {
            Ok(ExpectedStateRootValue::RunQueryIndexes(key.to_owned()))
        }
        None if family == StateRootFamily::PendingSignalSources => {
            Ok(ExpectedStateRootValue::PendingWaitSource {
                signal: true,
                key: key.to_owned(),
            })
        }
        None if family == StateRootFamily::PendingTimerSources => {
            Ok(ExpectedStateRootValue::PendingWaitSource {
                signal: false,
                key: key.to_owned(),
            })
        }
        None if family == StateRootFamily::MachineCommands => Ok(
            ExpectedStateRootValue::MachineCommandCurrent(key.to_owned()),
        ),
        None => Err(DurableError::Integrity {
            code: "state_root_family_value_kind_missing".to_owned(),
            message: format!("state-root family {family:?} has no closed value kind"),
        }),
    }
}

fn verify_expected_reachable_value(
    value: &StateRootValue,
    expected: &ExpectedStateRootValue,
) -> DurableResult<()> {
    let matches = match (value, expected) {
        (StateRootValue::Leaf { kind, .. }, ExpectedStateRootValue::Leaf(expected)) => {
            kind == expected
        }
        (
            StateRootValue::MachineBaseDescriptor { .. },
            ExpectedStateRootValue::MachineBaseDescriptor,
        ) => true,
        (
            StateRootValue::ApplicationJournal { journal_id, .. },
            ExpectedStateRootValue::ApplicationJournal(expected),
        )
        | (
            StateRootValue::ApplicationJournalRecordManifests { journal_id, .. },
            ExpectedStateRootValue::ApplicationJournalRecordManifests(expected),
        ) => journal_id == expected,
        (
            StateRootValue::ResourceHandoffIndex { to_run, .. },
            ExpectedStateRootValue::ResourceHandoffIndex(expected),
        )
        | (
            StateRootValue::ResourceHandoffActivationIndex { to_run, .. },
            ExpectedStateRootValue::ResourceHandoffActivationIndex(expected),
        )
        | (
            StateRootValue::ResourceHandoffSlots { to_run, .. },
            ExpectedStateRootValue::ResourceHandoffSlots(expected),
        ) => to_run == expected,
        (
            StateRootValue::AgentMessageIndex { session_id, .. },
            ExpectedStateRootValue::AgentMessageIndex(expected),
        )
        | (
            StateRootValue::AgentUnresolvedOccurrenceIndex { session_id, .. },
            ExpectedStateRootValue::AgentUnresolvedOccurrenceIndex(expected),
        )
        | (
            StateRootValue::AgentOpenStreamIndex { session_id, .. },
            ExpectedStateRootValue::AgentOpenStreamIndex(expected),
        ) => session_id == expected,
        (
            StateRootValue::RunQueryIndexes { run_id, .. },
            ExpectedStateRootValue::RunQueryIndexes(expected),
        ) => run_id == expected,
        (
            StateRootValue::MachineCommandCurrent { record, .. },
            ExpectedStateRootValue::MachineCommandCurrent(expected),
        ) => record.envelope.command_id == *expected,
        (
            StateRootValue::PendingWaitSource { source, .. },
            ExpectedStateRootValue::PendingWaitSource { signal, key },
        ) => {
            matches!(
                (*signal, source),
                (true, crate::WaitActivationSource::Signal { .. })
                    | (false, crate::WaitActivationSource::Timer { .. })
            ) && pending_wait_source_key(source).is_ok_and(|derived| &derived == key)
        }
        _ => false,
    };
    if !matches {
        return Err(DurableError::Integrity {
            code: "state_root_reachable_value_kind_mismatch".to_owned(),
            message: "reachable state-root value does not match its typed owning edge".to_owned(),
        });
    }
    Ok(())
}

fn load_reachable_object<R: StateRootResolver + ?Sized>(
    resolver: &mut R,
    object_id: &str,
) -> DurableResult<StateRootObject> {
    let object =
        resolver
            .load_state_root_object(object_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_reachable_object_missing".to_owned(),
                message: format!("reachable state-root object {object_id} does not exist"),
            })?;
    object.verify()?;
    if object.object_id() != object_id {
        return Err(DurableError::Integrity {
            code: "state_root_object_locator_mismatch".to_owned(),
            message: format!(
                "reachable state-root object {object_id} resolves to {}",
                object.object_id()
            ),
        });
    }
    Ok(object)
}

fn map_put<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    value: StateRootValue,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MapRoot> {
    let proof = prove_map_exact(root, key, overlay)?;
    let current = verify_map_exact(root, key, &proof)?;
    let value_id = overlay.insert_value(value)?;
    let mutation = match current.value() {
        None => MapMutation::insert(key, &value_id),
        Some(previous) if previous == value_id => {
            return Err(DurableError::Validation(format!(
                "authenticated-map key {key:?} already has the requested value"
            )));
        }
        Some(previous) => MapMutation::replace(key, previous, &value_id),
    };
    let output = apply_map_mutations(root, &[mutation], overlay)?;
    overlay.insert_map_nodes(output.objects())?;
    Ok(output.verified().result().clone())
}

fn map_remove<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MapRoot> {
    let proof = prove_map_exact(root, key, overlay)?;
    let current = verify_map_exact(root, key, &proof)?;
    let previous = current.value().ok_or_else(|| {
        DurableError::NotFound(format!("authenticated-map key {key:?} does not exist"))
    })?;
    let output = apply_map_mutations(root, &[MapMutation::remove(key, previous)], overlay)?;
    overlay.insert_map_nodes(output.objects())?;
    Ok(output.verified().result().clone())
}

fn map_get<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<StateRootValue>> {
    let proof = prove_map_exact(root, key, overlay)?;
    let current = verify_map_exact(root, key, &proof)?;
    current
        .value()
        .map(|value_id| overlay.load_value(value_id))
        .transpose()
}

fn materialize_map<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<BTreeMap<String, StateRootValue>> {
    let audit = audit_map(root, overlay)?;
    let mut values = BTreeMap::new();
    for (position, value_id) in audit.entries() {
        values.insert(position.key().to_owned(), overlay.load_value(value_id)?);
    }
    Ok(values)
}

fn log_append<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    values: &[StateRootValue],
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot> {
    if values.is_empty() {
        root.verify()?;
        return Ok(root.clone());
    }
    let value_ids = values
        .iter()
        .cloned()
        .map(|value| overlay.insert_value(value))
        .collect::<DurableResult<Vec<_>>>()?;
    let mut current = root.clone();
    for chunk in value_ids.chunks(MAX_LOG_VALUES_PER_APPLY) {
        let output =
            apply_log_mutations(&current, &[LogMutation::append(chunk.to_vec())], overlay)?;
        overlay.insert_log_nodes(output.objects())?;
        current = output.verified().result().clone();
    }
    Ok(current)
}

fn log_get<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<StateRootValue> {
    if index >= root.len {
        return Err(DurableError::NotFound(format!(
            "authenticated-log index {index} is outside length {}",
            root.len
        )));
    }
    let proof = prove_log_exact(root, index, overlay)?;
    let value = verify_log_exact(root, index, &proof)?;
    overlay.load_value(value.value())
}

fn split_log_root<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<(LogRoot, LogRoot)> {
    let output = split_log(root, index, overlay)?;
    overlay.insert_log_nodes(output.objects())?;
    Ok((
        output.verified().prefix().clone(),
        output.verified().suffix().clone(),
    ))
}

fn apply_log_mutation<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    mutation: LogMutation,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot> {
    let output = apply_log_mutations(root, &[mutation], overlay)?;
    overlay.insert_log_nodes(output.objects())?;
    Ok(output.verified().result().clone())
}

fn slice_log_prefix<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    count: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot> {
    split_log_root(root, count, overlay).map(|(_, suffix)| suffix)
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn build_state_root_genesis(state: &crate::DurableState) -> DurableResult<StateRootTransition> {
    state.validate_anchored(state.machine.base_anchor.as_ref())?;
    if !state.application_journal_prefix_replacements.is_empty() {
        return Err(DurableError::Validation(
            "StateRoot genesis cannot import journal prefix-replacement history; replacements must enter through their typed transition"
                .to_owned(),
        ));
    }
    let parts = state.machine.root_parts()?;
    if !parts.plans.is_empty()
        || !parts.artifacts.is_empty()
        || !parts.batches.is_empty()
        || parts.base.is_some()
        || parts.base_anchor.is_some()
        || !parts.events.is_empty()
        || !parts.admissions.is_empty()
        || !parts.commands.is_empty()
        || !parts.command_index_proofs.is_empty()
    {
        return Err(DurableError::Validation(
            "StateRoot genesis requires the exact empty Machine frontier; the first Run is a successor CAS"
                .to_owned(),
        ));
    }
    let mut empty = EmptyStateRootResolver;
    let mut overlay = ObjectOverlay::new(&mut empty);
    let roots = build_roots_from_state(state, parts, &mut overlay)?;
    let machine_frontier = cymule_core::durable_internal::MachineAuthorityFrontier::genesis(
        MapRoot::empty(),
        MapRoot::empty(),
        MapRoot::empty(),
        MapRoot::empty(),
    )?;
    let revision = derive_genesis_revision(DurableRevisionState {
        durable_version: &state.durable_version,
        machine_snapshot_version: &state.machine.snapshot_version,
        machine_frontier: &machine_frontier,
        machine_base_anchor: None,
        roots: &roots,
    })?;
    let manifest = StateRootManifest::new(
        StateRootManifestMetadata {
            durable_version: state.durable_version.clone(),
            revision,
            sequence: 0,
            parent_manifest: None,
            parent_revision: None,
            delta_digest: None,
            machine_snapshot_version: state.machine.snapshot_version.clone(),
        },
        machine_frontier,
        None,
        roots,
    )?;
    let objects = overlay.finish(&manifest)?;
    let transition = StateRootTransition {
        parent_manifest: None,
        delta_digest: None,
        manifest,
        objects,
    };
    transition.verify(None)?;
    Ok(transition)
}

fn apply_durable_state_root_delta<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    delta: &crate::DurableDelta,
    resolver: &mut R,
) -> DurableResult<StateRootTransition> {
    apply_state_root_successor(current, Some(delta), None, resolver)
}

pub(super) struct PinnedStateRootStageParts {
    pub(super) stage_digest: String,
    pub(super) machine_root_delta: Option<cymule_core::MachineRootDelta>,
    pub(super) compaction_summary: Option<crate::MachineCompactionSummary>,
    pub(super) machine_frontier: cymule_core::durable_internal::MachineAuthorityFrontier,
    pub(super) machine_base_anchor: Option<cymule_core::MachineBaseAnchor>,
    pub(super) roots: StateRoots,
    pub(super) pending: BTreeMap<String, StateRootObject>,
}

pub(super) fn finish_pinned_machine_stage<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    sidecar: Option<&crate::DurableDelta>,
    stage: PinnedStateRootStageParts,
    resolver: &mut R,
) -> DurableResult<StateRootTransition> {
    apply_state_root_successor(current, sidecar, Some(stage), resolver)
}

fn apply_state_root_successor<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    delta: Option<&crate::DurableDelta>,
    staged: Option<PinnedStateRootStageParts>,
    resolver: &mut R,
) -> DurableResult<StateRootTransition> {
    current.verify()?;
    ensure_resolver_pinned(current, resolver)?;
    let delta_digest = state_root_successor_delta_digest(delta, staged.as_ref())?;
    let sequence = next_state_root_sequence(current)?;
    let (
        mut roots,
        machine_frontier,
        machine_base_anchor,
        pending,
        staged_machine_root_delta,
        compaction_summary,
    ) = staged.map_or_else(
        || {
            (
                current.roots().clone(),
                current.machine_frontier().clone(),
                current.machine_base_anchor.clone(),
                BTreeMap::new(),
                None,
                None,
            )
        },
        |stage| {
            (
                stage.roots,
                stage.machine_frontier,
                stage.machine_base_anchor,
                stage.pending,
                stage.machine_root_delta,
                stage.compaction_summary,
            )
        },
    );
    let mut overlay = ObjectOverlay::with_pending(resolver, pending)?;
    let operations = delta.map_or(&[][..], crate::DurableDelta::operations);
    validate_transitioning_run_sidecars(
        current,
        operations,
        &machine_frontier,
        staged_machine_root_delta.as_ref(),
        &mut overlay,
    )?;

    apply_durable_sidecars(&mut roots, operations, &mut overlay)?;
    validate_state_root_sidecars(
        current,
        operations,
        &roots,
        &machine_frontier,
        staged_machine_root_delta.as_ref(),
        compaction_summary.as_ref(),
        &mut overlay,
    )?;
    if &roots == current.roots()
        && &machine_frontier == current.machine_frontier()
        && machine_base_anchor == current.machine_base_anchor
    {
        return Err(DurableError::Validation(
            "durable delta produced no state-root change".to_owned(),
        ));
    }
    let revision = derive_transition_revision(
        DurableRevisionLineage {
            parent_revision: &current.revision,
            delta_digest: &delta_digest,
            sequence,
        },
        DurableRevisionState {
            durable_version: &current.durable_version,
            machine_snapshot_version: &current.machine_snapshot_version,
            machine_frontier: &machine_frontier,
            machine_base_anchor: machine_base_anchor.as_ref(),
            roots: &roots,
        },
    )?;
    let manifest = StateRootManifest::new(
        StateRootManifestMetadata {
            durable_version: current.durable_version.clone(),
            revision,
            sequence,
            parent_manifest: Some(current.manifest_id.clone()),
            parent_revision: Some(current.revision.clone()),
            delta_digest: Some(delta_digest.clone()),
            machine_snapshot_version: current.machine_snapshot_version.clone(),
        },
        machine_frontier,
        machine_base_anchor,
        roots,
    )?;
    let objects = overlay.finish(&manifest)?;
    let transition = StateRootTransition {
        parent_manifest: Some(current.manifest_id.clone()),
        delta_digest: Some(delta_digest),
        manifest,
        objects,
    };
    transition.verify(Some(current))?;
    Ok(transition)
}

fn next_state_root_sequence(current: &StateRootManifest) -> DurableResult<u64> {
    current
        .sequence
        .checked_add(1)
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or_else(|| DurableError::Validation("state-root sequence overflowed".to_owned()))
}

fn state_root_successor_delta_digest(
    delta: Option<&crate::DurableDelta>,
    staged: Option<&PinnedStateRootStageParts>,
) -> DurableResult<String> {
    let sidecar_digest = delta.map(cymule_core::canonical_digest).transpose()?;
    let staged_digest = staged.map(|stage| stage.stage_digest.as_str());
    let digest = match (staged_digest, sidecar_digest.as_deref()) {
        (None, Some(sidecar)) => sidecar.to_owned(),
        (Some(stage), None) => stage.to_owned(),
        (Some(stage), Some(sidecar)) => cymule_core::canonical_digest(&(
            PINNED_MACHINE_SIDECAR_TRANSITION_DOMAIN,
            stage,
            sidecar,
        ))?,
        (None, None) => {
            return Err(DurableError::Validation(
                "StateRoot successor requires a pinned Machine stage or sidecar delta".to_owned(),
            ));
        }
    };
    Ok(digest)
}

struct CompactionDeltaOutputs<'a> {
    base: &'a cymule_core::MachineBaseSnapshot,
    anchor: &'a cymule_core::MachineBaseAnchor,
    header: &'a cymule_core::MachineCommandArchiveSegmentHeader,
}

impl<'a> CompactionDeltaOutputs<'a> {
    fn new(delta: &'a cymule_core::MachineRootDelta) -> DurableResult<Self> {
        let (Some(base), Some(anchor), Some(header)) =
            (&delta.base, &delta.base_anchor, &delta.archive_segment)
        else {
            return Err(compaction_integrity(
                "state_root_history_compaction_stage_incomplete",
                "Machine compaction requires one paired base, anchor, and archive segment",
            ));
        };
        Ok(Self {
            base,
            anchor,
            header,
        })
    }
}

fn validate_history_compaction_operations<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    delta: Option<&cymule_core::MachineRootDelta>,
    summary: Option<&crate::MachineCompactionSummary>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let receipt = operations.iter().find_map(|operation| match operation {
        crate::DurableOperation::PutHistoryCompaction { value } => Some(value),
        _ => None,
    });
    let has_cut = delta.is_some_and(|delta| {
        delta.base.is_some()
            || delta.base_anchor.is_some()
            || delta.archive_segment.is_some()
            || delta.parent_anchor_id != delta.result_anchor_id
    }) || roots.machine_base != current.roots.machine_base;
    if !has_cut && summary.is_none() && receipt.is_none() {
        return Ok(());
    }
    let (Some(delta), Some(summary), Some(receipt)) = (delta, summary, receipt) else {
        return Err(compaction_integrity(
            "state_root_history_compaction_stage_missing",
            "Machine compaction and its receipt require the same verified Core stage",
        ));
    };
    if operations.len() != 1 {
        return Err(compaction_integrity(
            "state_root_history_compaction_sidecar_mismatch",
            "Machine compaction permits exactly its one receipt sidecar",
        ));
    }
    ensure_machine_compaction_source(current, &*overlay.resolver)?;
    let outputs = CompactionDeltaOutputs::new(delta)?;
    verify_compaction_source_delta(current, frontier, delta, &outputs)?;
    verify_compaction_result_roots(roots, delta, summary, &outputs, overlay)?;
    receipt.verify()?;
    let parent = load_parent_compaction_from_overlay(current, overlay)?;
    if receipt.source_revision != current.revision
        || receipt.parent_compaction.as_deref()
            != parent.as_ref().map(|parent| parent.compaction_id.as_str())
        || receipt.result != *summary
    {
        return Err(compaction_integrity(
            "state_root_history_compaction_receipt_mismatch",
            "Machine compaction receipt changed its exact source, parent, or Core result",
        ));
    }
    let stored = load_history_compaction_leaf(roots, &receipt.compaction_id, overlay)?;
    let expected_head = state_root_value_id(&StateRootValue::encode(
        StateRootLeafKind::HistoryCompaction,
        receipt,
    )?)?;
    if stored.as_ref() != Some(receipt)
        || roots.history_compaction_head.as_deref() != Some(expected_head.as_str())
    {
        return Err(compaction_integrity(
            "state_root_history_compaction_publication_mismatch",
            "Machine compaction did not publish its exact primary receipt and head value",
        ));
    }
    verify_history_compaction_parent(roots, receipt, overlay)
}

fn verify_compaction_source_delta(
    current: &StateRootManifest,
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    delta: &cymule_core::MachineRootDelta,
    outputs: &CompactionDeltaOutputs<'_>,
) -> DurableResult<()> {
    let mut expected = current.machine_frontier().clone();
    expected.base_anchor_id = Some(outputs.anchor.anchor_id.clone());
    expected
        .command_index_root
        .clone_from(&outputs.anchor.command_index_root);
    let archived_batches = current
        .machine_base_anchor
        .as_ref()
        .map_or(0, |anchor| anchor.archive_batch_count);
    if delta.root_delta_version != cymule_core::MachineRootDelta::VERSION
        || delta.delta_version != cymule_core::MachineDelta::VERSION
        || delta.parent_authority_root != current.machine_frontier.authority_root
        || delta.result_authority_root != current.machine_frontier.authority_root
        || delta.parent_anchor_id.as_deref()
            != current
                .machine_base_anchor
                .as_ref()
                .map(|anchor| anchor.anchor_id.as_str())
        || delta.result_anchor_id.as_deref() != Some(outputs.anchor.anchor_id.as_str())
        || *frontier != expected
        || !delta.plans.is_empty()
        || !delta.plan_admission_order.is_empty()
        || !delta.artifacts.is_empty()
        || !delta.artifact_admission_order.is_empty()
        || !delta.batches.is_empty()
        || !delta.batch_admission_order.is_empty()
        || !delta.events.is_empty()
        || !delta.admissions.is_empty()
        || !delta.commands.is_empty()
        || delta.removed_batch_ids.is_empty()
        || u64::try_from(delta.removed_batch_ids.len()).ok() != Some(outputs.header.batch_count)
        || u64::try_from(delta.removed_admission_ids.len()).ok() != Some(outputs.header.entry_count)
        || u64::try_from(delta.removed_command_ids.len()).ok() != Some(outputs.header.entry_count)
        || u64::try_from(delta.removed_event_ids.len()).ok() != Some(outputs.header.event_count)
        || archived_batches.checked_add(outputs.header.batch_count)
            != Some(outputs.anchor.archive_batch_count)
    {
        return Err(compaction_integrity(
            "state_root_history_compaction_source_mismatch",
            "Machine compaction changed semantic authority, normalized current roots, or its exact archive cut",
        ));
    }
    Ok(())
}

fn verify_compaction_result_roots<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    delta: &cymule_core::MachineRootDelta,
    summary: &crate::MachineCompactionSummary,
    outputs: &CompactionDeltaOutputs<'_>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let CompactionDeltaOutputs {
        base,
        anchor,
        header,
    } = *outputs;
    base.verify()?;
    verify_compaction_summary_anchor(summary, anchor)?;
    if summary.archive_segment != *header
        || summary.retained_events != roots.machine_events.len
        || base.identity()? != anchor.base_id
        || base.archive_head != anchor.archive_head
        || base.archive_count != anchor.archive_count
        || base.archive_event_count != anchor.archive_event_count
        || base.batch_count != anchor.archive_batch_count
        || base.admission_head != anchor.admission_head
        || base.command_index_root != anchor.command_index_root
        || base.prefix_digest != anchor.prefix_digest
        || base.projection_digest != anchor.projection_digest
        || base.projection_root != anchor.projection_root
        || delta.removed_command_ids != delta.removed_command_index_proof_ids
    {
        return Err(compaction_integrity(
            "state_root_history_compaction_result_mismatch",
            "Machine compaction base, archive, receipt summary, and retained suffix disagree",
        ));
    }
    verify_compaction_base_bytes(roots, base, overlay)?;
    for (key, value) in materialize_map(&roots.machine_commands, overlay)? {
        let StateRootValue::MachineCommandCurrent { index_proof, .. } = value else {
            return Err(compaction_integrity(
                "state_root_machine_command_value_kind_mismatch",
                "Machine compaction retained a non-command hot leaf",
            ));
        };
        if index_proof.command_id != key || index_proof.value.is_some() {
            return Err(compaction_integrity(
                "state_root_history_compaction_hot_proof_mismatch",
                "Machine compaction changed a retained command proof identity",
            ));
        }
        index_proof.verify(&anchor.command_index_root)?;
    }
    Ok(())
}

fn verify_compaction_base_bytes<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    base: &cymule_core::MachineBaseSnapshot,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let object_id = roots.machine_base.as_ref().ok_or_else(|| {
        compaction_integrity(
            "state_root_history_compaction_base_missing",
            "Machine compaction result has no rooted base",
        )
    })?;
    let StateRootValue::MachineBaseDescriptor {
        canonical_len,
        canonical_digest,
        chunks,
        ..
    } = overlay.load_value(object_id)?
    else {
        return Err(compaction_integrity(
            "state_root_machine_base_descriptor_kind",
            "Machine compaction base root has the wrong descriptor kind",
        ));
    };
    let canonical = cymule_core::canonical_bytes(base)?;
    if u64::try_from(canonical.len()).ok() != Some(canonical_len)
        || cymule_core::sha256_bytes(&canonical) != canonical_digest
    {
        return Err(compaction_integrity(
            "state_root_history_compaction_base_mismatch",
            "Machine compaction base descriptor differs from its exact Core base",
        ));
    }
    let verified = audit_log(&chunks, &mut *overlay)?;
    let expected = canonical.chunks(MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES);
    if expected.len() != verified.values().len() {
        return Err(compaction_integrity(
            "state_root_machine_base_chunk_count",
            "Machine compaction base chunk count differs from its exact bytes",
        ));
    }
    for (index, (object_id, expected)) in verified.values().iter().zip(expected).enumerate() {
        let value = overlay.load_value(object_id)?;
        if !matches!(value, StateRootValue::MachineBaseChunk { index: actual, bytes }
            if u64::try_from(index).ok() == Some(actual) && bytes == expected)
        {
            return Err(compaction_integrity(
                "state_root_history_compaction_base_chunk_mismatch",
                "Machine compaction base chunk differs from its exact ordered bytes",
            ));
        }
    }
    Ok(())
}

fn validate_transitioning_run_sidecars<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    operations: &[crate::DurableOperation],
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    delta: Option<&cymule_core::MachineRootDelta>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use crate::DurableOperation as Op;
    let mut touched_runs = BTreeSet::new();
    for operation in operations {
        let run_id = match operation {
            Op::PutContinuation { value } => Some(value.run_id.as_str()),
            Op::PutRunCurrent { value } => Some(value.run_id.as_str()),
            Op::PutWait { value } => Some(value.run_id.as_str()),
            Op::PutOutbox { value } => Some(value.run_id.as_str()),
            Op::PutComponentOccurrence { value } => Some(value.run_id.as_str()),
            Op::PutOperationAttempt { value } => Some(value.run_id.as_str()),
            Op::PutCancellationReceipt { value } => Some(value.command.run_id.as_str()),
            Op::PutEffectResolutionReceipt { value } => Some(value.command.run_id.as_str()),
            Op::PutLease { value } => {
                if let Some(owner) = load_outbox_owner(&current.roots, &value.resource, overlay)? {
                    touched_runs.insert(owner.run_id);
                }
                None
            }
            _ => None,
        };
        if let Some(run_id) = run_id {
            touched_runs.insert(run_id.to_owned());
        }
    }
    for run_id in touched_runs {
        let Some(value) = map_get(&current.machine_frontier.runs, &run_id, overlay)? else {
            continue;
        };
        let StateRootValue::MachineRunCurrent { current: run } = value else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_run_value_kind_mismatch".to_owned(),
                message: "sidecar fence selected a non-Run value".to_owned(),
            });
        };
        let cymule_core::durable_internal::MachineRunReducerState::Transitioning { transition_id } =
            &run.reducer_state
        else {
            continue;
        };
        let value = map_get(
            &current.machine_frontier.paged_transitions,
            transition_id,
            overlay,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_machine_pending_transition_missing".to_owned(),
            message: "fenced sidecar Run has no retained Core transition".to_owned(),
        })?;
        let StateRootValue::MachinePagedTransitionCurrent {
            current: transition,
        } = value
        else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_paged_value_kind_mismatch".to_owned(),
                message: "sidecar fence selected a non-transition value".to_owned(),
            });
        };
        transition.verify()?;
        let record = delta.and_then(|delta| delta.commands.get(&transition.command_id));
        let final_command = record.is_some_and(|record| {
            record.envelope == transition.envelope
                && record.semantic_hash == transition.command_hash
                && record.receipt.status == cymule_core::CommandReceiptStatus::Applied
                && record.batch_id == transition.batch_manifest.batch_id
        });
        let result = require_machine_run_current(frontier, &run_id, overlay)?;
        if !final_command
            || !matches!(
                result.reducer_state,
                cymule_core::durable_internal::MachineRunReducerState::Ready
            )
            || map_get(&frontier.paged_transitions, transition_id, overlay)?.is_some()
        {
            return Err(DurableError::Conflict {
                expected: Some(format!("completed Core transition {transition_id}")),
                current: Some(format!(
                    "Run {run_id} is owned by Core transition {transition_id}"
                )),
            });
        }
    }
    Ok(())
}

fn validate_state_root_sidecars<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    machine_frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    staged_machine_root_delta: Option<&cymule_core::MachineRootDelta>,
    compaction_summary: Option<&crate::MachineCompactionSummary>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    validate_history_compaction_operations(
        current,
        operations,
        roots,
        machine_frontier,
        staged_machine_root_delta,
        compaction_summary,
        overlay,
    )?;
    validate_evolution_operation_set(operations, roots, overlay)?;
    validate_virtual_operation_set(operations, roots, overlay)?;
    validate_component_attempt_operation_set(current, operations, roots, overlay)?;
    validate_terminal_receipt_operations(
        current,
        operations,
        roots,
        machine_frontier,
        staged_machine_root_delta,
        overlay,
    )?;
    validate_agent_target_claim_operation_set(operations, roots, overlay)?;
    for operation in operations {
        match operation {
            crate::DurableOperation::PutCoupledCheckpointReceipt { value } => {
                if matches!(
                    &value.checkpoint,
                    crate::CoupledCheckpoint::AgentWorkspace { .. }
                ) && map_get(
                    &current.roots.coupled_checkpoint_receipts,
                    &value.coupling_id,
                    overlay,
                )?
                .is_none()
                {
                    validate_agent_workspace_transition(
                        current,
                        roots,
                        machine_frontier,
                        staged_machine_root_delta,
                        value,
                        overlay,
                    )?;
                }
                validate_coupled_checkpoint_history(
                    roots,
                    &machine_frontier.authority_root,
                    value,
                    overlay,
                )?;
            }
            crate::DurableOperation::PutAgentCommandReceipt { value } => {
                validate_agent_command_receipt(roots, value, overlay)?;
            }
            crate::DurableOperation::PutAgentSessionCurrent { value } => {
                validate_agent_session_roots(roots, value, overlay)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn receipt_target_claim_transitions(
    command: &cymule_profile_protocol::agent::AgentCommand,
    receipt: &cymule_profile_protocol::agent::AgentCommandReceipt,
) -> DurableResult<Vec<cymule_profile_protocol::agent::AgentTargetClaimTransition>> {
    use cymule_profile_protocol::agent::{AgentCommandOutcome, AgentCommandSource};

    receipt.verify_for(command)?;
    match (&receipt.source, &receipt.outcome) {
        (
            AgentCommandSource::Session { session, update },
            AgentCommandOutcome::Session(postcondition),
        ) => cymule_profile_protocol::agent::agent_session_target_claim_transitions(
            command,
            session,
            update,
            postcondition,
        )
        .map_err(Into::into),
        (AgentCommandSource::Stream(source), AgentCommandOutcome::Stream(postcondition)) => Ok(
            cymule_profile_protocol::agent::agent_stream_target_claim_transition(
                command,
                source,
                postcondition,
            )?
            .into_iter()
            .collect(),
        ),
        _ => Ok(Vec::new()),
    }
}

fn validate_agent_target_claim_operation_set<R: StateRootResolver + ?Sized>(
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentCommand, AgentTargetClaimPhase};

    let mut expected = Vec::new();
    for operation in operations {
        if let crate::DurableOperation::PutAgentCommandReceipt { value: receipt } = operation {
            let key = cymule_profile_protocol::agent::agent_command_key(&receipt.command_id)?;
            let command: AgentCommand = map_get(&roots.agent_commands, &key, overlay)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "agent_target_claim_command_missing".to_owned(),
                    message: "Agent target-claim receipt lost its exact command".to_owned(),
                })?
                .decode(StateRootLeafKind::AgentCommand)?;
            expected.extend(receipt_target_claim_transitions(&command, receipt)?);
        }
    }

    for operation in operations {
        let crate::DurableOperation::ApplyAgentTargetClaim { value: transition } = operation else {
            continue;
        };
        transition.verify()?;
        validate_agent_target_write_operations(operations, transition)?;
        if let Some(position) = expected.iter().position(|value| value == transition) {
            expected.remove(position);
            continue;
        }
        if !matches!(
            transition.current.phase,
            AgentTargetClaimPhase::Reserved { .. }
        ) {
            return Err(DurableError::Integrity {
                code: "agent_target_claim_operation_unowned".to_owned(),
                message: "Agent target-claim mutation has no owning receipt or reservation"
                    .to_owned(),
            });
        }
        validate_reserved_agent_target_claim_operation(operations, roots, overlay, transition)?;
    }
    if !expected.is_empty() {
        return Err(DurableError::Integrity {
            code: "agent_target_claim_operation_missing".to_owned(),
            message: "Agent command receipt did not atomically apply every target claim".to_owned(),
        });
    }
    validate_agent_target_write_reverse_closure(operations)?;
    Ok(())
}

fn operation_agent_target(
    operation: &crate::DurableOperation,
) -> Option<(
    &str,
    cymule_profile_protocol::agent::AgentTargetClaimTarget,
    &str,
    bool,
)> {
    match operation {
        crate::DurableOperation::PutAgentMessageCurrent { value } => Some((
            &value.session_id,
            cymule_profile_protocol::agent::AgentTargetClaimTarget::Message {
                message_id: value.message.message_id.clone(),
            },
            &value.order.admitted_by,
            true,
        )),
        crate::DurableOperation::PutAgentToolCurrent { value } => Some((
            &value.session_id,
            cymule_profile_protocol::agent::AgentTargetClaimTarget::Tool {
                tool_call_id: value.tool.tool_call_id.clone(),
            },
            &value.admitted_by,
            agent_tool_is_terminal(value),
        )),
        _ => None,
    }
}

fn validate_agent_target_write_operations(
    operations: &[crate::DurableOperation],
    transition: &cymule_profile_protocol::agent::AgentTargetClaimTransition,
) -> DurableResult<()> {
    let writes = operations
        .iter()
        .filter_map(operation_agent_target)
        .filter(|(session_id, target, _, _)| {
            *session_id == transition.current.session_id && *target == transition.current.target
        })
        .collect::<Vec<_>>();
    let valid = match transition.current.phase {
        cymule_profile_protocol::agent::AgentTargetClaimPhase::Materialized => {
            matches!(writes.as_slice(), [(_, _, admitted_by, true)] if *admitted_by == transition.current.admitted_by)
        }
        cymule_profile_protocol::agent::AgentTargetClaimPhase::Reserved { .. }
        | cymule_profile_protocol::agent::AgentTargetClaimPhase::Released { .. } => {
            writes.is_empty()
        }
    };
    if !valid {
        return Err(DurableError::Integrity {
            code: "agent_target_claim_write_operation_mismatch".to_owned(),
            message: "Agent target-claim transition is not closed with its exact target write"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_agent_target_write_reverse_closure(
    operations: &[crate::DurableOperation],
) -> DurableResult<()> {
    for (session_id, target, admitted_by, terminal) in
        operations.iter().filter_map(operation_agent_target)
    {
        if !terminal {
            continue;
        }
        let matching = operations
            .iter()
            .filter_map(|operation| match operation {
                crate::DurableOperation::ApplyAgentTargetClaim { value }
                    if value.current.session_id == session_id
                        && value.current.target == target
                        && value.current.admitted_by == admitted_by
                        && matches!(
                            value.current.phase,
                            cymule_profile_protocol::agent::AgentTargetClaimPhase::Materialized
                        ) =>
                {
                    Some(())
                }
                _ => None,
            })
            .count();
        if matching != 1 {
            return Err(DurableError::Integrity {
                code: "agent_target_write_claim_operation_missing".to_owned(),
                message: "Terminal Agent target write lacks one Materialized claim transition"
                    .to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_reserved_agent_target_claim_operation<R: StateRootResolver + ?Sized>(
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
    transition: &cymule_profile_protocol::agent::AgentTargetClaimTransition,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::AgentStreamPublicationReservationPhase;
    use cymule_profile_protocol::resource::{
        ResourceLifecycleReceiptRef, ResourcePinStatus, ResourceRetentionDisposition,
    };

    let cymule_profile_protocol::agent::AgentTargetClaimPhase::Reserved {
        stream_id,
        reservation_id,
    } = &transition.current.phase
    else {
        unreachable!("caller selects only Reserved target claims")
    };
    let stream_key = cymule_profile_protocol::agent::agent_stream_key(
        &transition.current.session_id,
        stream_id,
    )?;
    let stream: cymule_profile_protocol::agent::AgentStreamCurrent =
        map_get(&roots.agent_streams, &stream_key, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_target_claim_reserved_stream_missing".to_owned(),
                message: "Reserved Agent target claim lost its owning stream".to_owned(),
            })?
            .decode(StateRootLeafKind::AgentStreamCurrent)?;
    stream.verify()?;
    let reservation =
        stream
            .publication_reservation
            .as_ref()
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_target_claim_reservation_missing".to_owned(),
                message: "Reserved Agent target claim lost its publication reservation".to_owned(),
            })?;
    let origin = ResourceLifecycleReceiptRef::from_agent_publication_reservation(
        transition.current.admitted_by.clone(),
        transition.current.session_id.clone(),
        stream_id.clone(),
        reservation_id.clone(),
    )?;
    let pin = operations.iter().find_map(|operation| match operation {
        crate::DurableOperation::PutResourcePinCurrent { value }
            if value.pin == reservation.resource_pin_receipt.pin
                && value.status == ResourcePinStatus::Reserved
                && value.last_receipt == origin =>
        {
            Some(value)
        }
        _ => None,
    });
    let family = pin.and_then(|pin| {
        operations.iter().find_map(|operation| match operation {
            crate::DurableOperation::PutResourceRetentionCurrent { value }
                if value.family == pin.pin.subject.family
                    && value.active_pin_count
                        == reservation.resource_pin_receipt.active_pin_count
                    && value.disposition == ResourceRetentionDisposition::Active =>
            {
                Some(value)
            }
            _ => None,
        })
    });
    if reservation.reservation_id != *reservation_id
        || reservation.phase != AgentStreamPublicationReservationPhase::DispatchClaimed
        || reservation.intent.command_id() != transition.current.admitted_by
        || cymule_profile_protocol::agent::AgentTargetClaimTarget::from_stream_target(
            &stream.target,
        ) != transition.current.target
        || !operations.iter().any(|operation| {
            matches!(
                operation,
                crate::DurableOperation::PutAgentStreamCurrent { value } if value == &stream
            )
        })
        || !operations.iter().any(|operation| {
            matches!(
                operation,
                crate::DurableOperation::PutAgentCommand { value }
                    if value.command_id == transition.current.admitted_by
            )
        })
        || pin.is_none()
        || family.is_none()
    {
        return Err(DurableError::Integrity {
            code: "agent_target_claim_reservation_operation_mismatch".to_owned(),
            message: "Reserved Agent target claim is not atomic with its stream and command"
                .to_owned(),
        });
    }
    Ok(())
}

struct StateRootSidecarWriter<'stage, 'resolver, R: StateRootResolver + ?Sized> {
    roots: &'stage mut StateRoots,
    overlay: &'stage mut ObjectOverlay<'resolver, R>,
}

fn apply_durable_sidecars<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    operations: &[crate::DurableOperation],
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let mut writer = StateRootSidecarWriter { roots, overlay };
    for operation in operations {
        writer.apply(operation)?;
    }
    Ok(())
}

impl<R: StateRootResolver + ?Sized> StateRootSidecarWriter<'_, '_, R> {
    fn apply(&mut self, operation: &crate::DurableOperation) -> DurableResult<()> {
        use crate::DurableOperation as Op;

        match operation {
            Op::PutContinuation { value } => self.put_continuation(value),
            Op::PutRunCurrent { value } => self.put_run_current(value),
            Op::PutWait { value } => self.put_wait(value),
            Op::PutWaitActivation { value } => self.put_wait_activation(value),
            Op::PutCancellationReceipt { value } => self.put_cancellation_receipt(value),
            Op::PutEffectResolutionReceipt { value } => self.put_effect_resolution_receipt(value),
            Op::PutLease { value } => self.put_lease(value),
            Op::PutOutbox { value } => self.put_outbox(value),
            Op::PutComponentOccurrence { value } => self.put_component_occurrence(value),
            Op::PutOperationAttempt { value } => self.put_operation_attempt(value),
            Op::PutClockObservation { value } => self.put_clock_observation(value),
            Op::RemoveClockObservation { observation_id } => {
                self.remove_clock_observation(observation_id)
            }
            Op::PutHistoryCompaction { value } => self.put_history_compaction(value),
            #[cfg(test)]
            Op::AppendJournal {
                journal_id,
                records,
            } => self.append_journal(journal_id, records),
            #[cfg(test)]
            Op::ReplaceJournalPrefix { receipt } => self.replace_journal_prefix(receipt),
            Op::PutCoupledCheckpointReceipt { value } => self.put_coupled_checkpoint_receipt(value),
            Op::PutResourceCommandReceipt { value } => self.put_resource_command_receipt(value),
            Op::PutResourceRetentionCurrent { value } => self.put_resource_retention_current(value),
            Op::PutResourcePinCurrent { value } => self.put_resource_pin_current(value),
            Op::PutResourceDeleteCurrent { value } => self.put_resource_delete_current(value),
            Op::PutResourceHandoffCurrent { value } => self.put_resource_handoff_current(value),
            Op::PutResourceHandoffSlot { value } => self.put_resource_handoff_slot(value),
            Op::PutResourceHandoffActivationCurrent { value } => {
                self.put_resource_handoff_activation_current(value)
            }
            Op::AppendResourceHandoffIndex { value } => self.append_resource_handoff_index(value),
            Op::AppendResourceHandoffActivationIndex { value } => {
                self.append_resource_handoff_activation_index(value)
            }
            Op::PutAgentCommand { value } => self.put_agent_command(value),
            Op::PutAgentCommandReceipt { value } => self.put_agent_command_receipt(value),
            Op::PutAgentInputSuspensionReceipt { value } => {
                self.put_agent_input_suspension_receipt(value)
            }
            Op::PutAgentInputCompletionReceipt { value } => {
                self.put_agent_input_completion_receipt(value)
            }
            Op::PutAgentSessionCurrent { value } => self.put_agent_session_current(value),
            Op::PutAgentUpdateCurrent { value } => self.put_agent_update_current(value),
            Op::PutAgentMessageCurrent { value } => self.put_agent_message_current(value),
            Op::PutAgentToolCurrent { value } => self.put_agent_tool_current(value),
            Op::ApplyAgentTargetClaim { value } => self.apply_agent_target_claim(value),
            Op::PutAgentElicitationCurrent { value } => self.put_agent_elicitation_current(value),
            Op::PutAgentOccurrenceCurrent { value } => self.put_agent_occurrence_current(value),
            Op::PutAgentStreamCurrent { value } => self.put_agent_stream_current(value),
            Op::PutAgentStreamChunkCurrent { value } => self.put_agent_stream_chunk_current(value),
            Op::PutEvolutionCurrent { value } => self.put_evolution_current(value),
            Op::PutEvolutionCommandAlias { value } => self.put_evolution_command_alias(value),
            Op::PutEvolutionPersistenceReceipt { value } => {
                self.put_evolution_persistence_receipt(value)
            }
            Op::PutEvolutionMutation { value } => self.put_evolution_mutation(value),
            Op::PutVirtualCurrent { value } => self.put_virtual_current(value),
            Op::PutVirtualPersistenceReceipt { value } => {
                self.put_virtual_persistence_receipt(value)
            }
            Op::ApplyVirtualMutation { value } => self.apply_virtual_mutation(value),
            Op::PutResourceCatalogRecord { value } => self.put_resource_catalog_record(value),
        }
    }

    fn put_continuation(&mut self, value: &crate::Continuation) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.continuations,
            &value.run_id,
            StateRootLeafKind::Continuation,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_run_current(&mut self, value: &crate::DurableRunCurrent) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.run_currents,
            &value.run_id,
            StateRootLeafKind::RunCurrent,
            value,
            self.overlay,
        )?;
        ensure_run_query_indexes(
            &mut self.roots.run_query_indexes,
            &value.run_id,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_wait(&mut self, value: &crate::WaitCondition) -> DurableResult<()> {
        value.verify_wire()?;
        let summary = crate::DurableWaitSummary::from_wait(value);
        summary.verify()?;
        let previous = map_get(&self.roots.waits, &value.wait_id, self.overlay)?
            .map(|retained| retained.decode::<crate::WaitCondition>(StateRootLeafKind::Wait))
            .transpose()?;
        if let Some(previous) = &previous {
            if previous.wait_id != value.wait_id
                || previous.run_id != value.run_id
                || previous.kind != value.kind
                || previous.consume_once != value.consume_once
                || previous.owner != value.owner
                || (previous.state != crate::WaitState::Pending && previous != value)
            {
                return Err(DurableError::HistoryConflict {
                    code: "state_root_wait_semantic_reuse".to_owned(),
                    message: format!(
                        "Wait {} changed immutable semantics or terminal history",
                        value.wait_id
                    ),
                });
            }
            if previous.state == crate::WaitState::Pending
                && let Some(source) = pending_wait_source_for(previous)
            {
                remove_pending_wait_source(self.roots, &source, previous, self.overlay)?;
            }
        }
        put_typed_value(
            &mut self.roots.waits,
            &value.wait_id,
            StateRootLeafKind::Wait,
            value,
            self.overlay,
        )?;
        put_run_query_item(
            &mut self.roots.run_query_indexes,
            &value.run_id,
            RunQueryIndexKind::Waits,
            &value.wait_id,
            StateRootLeafKind::WaitSummary,
            &summary,
            self.overlay,
        )?;
        sync_run_query_item(
            &mut self.roots.run_query_indexes,
            &value.run_id,
            RunQueryMembership {
                index: RunQueryIndexKind::PendingWaits,
                active: value.state == crate::WaitState::Pending,
            },
            &value.wait_id,
            StateRootLeafKind::Wait,
            value,
            self.overlay,
        )?;
        if value.state == crate::WaitState::Pending
            && let Some(source) = pending_wait_source_for(value)
        {
            insert_pending_wait_source(self.roots, source, value, self.overlay)?;
        }
        Ok(())
    }

    fn put_wait_activation(&mut self, value: &crate::WaitActivationReceipt) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.wait_activations,
            &value.activation.activation_id,
            StateRootLeafKind::WaitActivation,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_cancellation_receipt(
        &mut self,
        value: &crate::CancellationReceipt,
    ) -> DurableResult<()> {
        verify_cancellation_receipt_leaf(
            self.roots,
            &value.command.cancellation_id,
            value,
            self.overlay,
        )?;
        insert_immutable_typed_value(
            &mut self.roots.cancellation_receipts,
            &value.command.cancellation_id,
            StateRootLeafKind::CancellationReceipt,
            value,
            "Run cancellation receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_effect_resolution_receipt(
        &mut self,
        value: &crate::EffectResolutionReceipt,
    ) -> DurableResult<()> {
        verify_effect_resolution_receipt_leaf(
            self.roots,
            &value.command.resolution_id,
            value,
            self.overlay,
        )?;
        insert_immutable_typed_value(
            &mut self.roots.effect_resolution_receipts,
            &value.command.resolution_id,
            StateRootLeafKind::EffectResolutionReceipt,
            value,
            "Effect-resolution receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_lease(&mut self, value: &crate::CoordinationLease) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.leases,
            &value.resource,
            StateRootLeafKind::Lease,
            value,
            self.overlay,
        )?;
        if let Some(dispatch) =
            load_owned_effect_dispatch(self.roots, &value.resource, self.overlay)?
            && dispatch.state == crate::OutboxState::Claimed
        {
            if dispatch.claim_owner.as_deref() != Some(value.owner.as_str())
                || dispatch.claim_epoch != value.epoch
            {
                return Err(DurableError::Integrity {
                    code: "state_root_claimed_effect_lease_mismatch".to_owned(),
                    message: format!(
                        "claimed Effect {} and coordination lease disagree",
                        value.resource
                    ),
                });
            }
            sync_run_query_item(
                &mut self.roots.run_query_indexes,
                &dispatch.run_id,
                RunQueryMembership {
                    index: RunQueryIndexKind::ActiveLeases,
                    active: true,
                },
                &value.resource,
                StateRootLeafKind::Lease,
                value,
                self.overlay,
            )?;
        }
        Ok(())
    }

    fn put_outbox(&mut self, value: &crate::EffectDispatch) -> DurableResult<()> {
        value.verify_wire()?;
        let owner = OutboxOwner {
            intent_id: value.intent_id.clone(),
            run_id: value.run_id.clone(),
        };
        owner.verify()?;
        insert_immutable_typed_value(
            &mut self.roots.outbox,
            &value.intent_id,
            StateRootLeafKind::OutboxOwner,
            &owner,
            "Effect owner locator",
            self.overlay,
        )?;
        put_run_query_item(
            &mut self.roots.run_query_indexes,
            &value.run_id,
            RunQueryIndexKind::Effects,
            &value.intent_id,
            StateRootLeafKind::Outbox,
            value,
            self.overlay,
        )?;
        sync_run_query_item(
            &mut self.roots.run_query_indexes,
            &value.run_id,
            RunQueryMembership {
                index: RunQueryIndexKind::ActiveEffects,
                active: matches!(
                    value.state,
                    crate::OutboxState::Pending
                        | crate::OutboxState::Claimed
                        | crate::OutboxState::Unknown
                ),
            },
            &value.intent_id,
            StateRootLeafKind::Outbox,
            value,
            self.overlay,
        )?;
        if value.state == crate::OutboxState::Claimed {
            let lease: crate::CoordinationLease =
                map_get(&self.roots.leases, &value.intent_id, self.overlay)?
                    .map(|retained| retained.decode(StateRootLeafKind::Lease))
                    .transpose()?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "state_root_claimed_effect_lease_missing".to_owned(),
                        message: format!(
                            "claimed Effect {} has no exact coordination lease",
                            value.intent_id
                        ),
                    })?;
            if lease.resource != value.intent_id
                || value.claim_owner.as_deref() != Some(lease.owner.as_str())
                || value.claim_epoch != lease.epoch
            {
                return Err(DurableError::Integrity {
                    code: "state_root_claimed_effect_lease_mismatch".to_owned(),
                    message: format!(
                        "claimed Effect {} and coordination lease disagree",
                        value.intent_id
                    ),
                });
            }
            sync_run_query_item(
                &mut self.roots.run_query_indexes,
                &value.run_id,
                RunQueryMembership {
                    index: RunQueryIndexKind::ActiveLeases,
                    active: true,
                },
                &value.intent_id,
                StateRootLeafKind::Lease,
                &lease,
                self.overlay,
            )?;
        } else {
            remove_run_query_item(
                &mut self.roots.run_query_indexes,
                &value.run_id,
                RunQueryIndexKind::ActiveLeases,
                &value.intent_id,
                self.overlay,
            )?;
        }
        Ok(())
    }

    fn put_component_occurrence(
        &mut self,
        value: &crate::ComponentOccurrence,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.component_occurrences,
            &value.occurrence_id,
            StateRootLeafKind::ComponentOccurrence,
            value,
            self.overlay,
        )?;
        put_run_query_item(
            &mut self.roots.run_query_indexes,
            &value.run_id,
            RunQueryIndexKind::Occurrences,
            &value.occurrence_id,
            StateRootLeafKind::ComponentOccurrence,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_operation_attempt(&mut self, value: &crate::OperationAttempt) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.operation_attempts,
            &value.attempt_id,
            StateRootLeafKind::OperationAttempt,
            value,
            self.overlay,
        )?;
        put_run_query_item(
            &mut self.roots.run_query_indexes,
            &value.run_id,
            RunQueryIndexKind::Attempts,
            &value.attempt_id,
            StateRootLeafKind::OperationAttempt,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_clock_observation(&mut self, value: &crate::ClockObservation) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.clock_observations,
            &value.observation_id,
            StateRootLeafKind::ClockObservation,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn remove_clock_observation(&mut self, observation_id: &str) -> DurableResult<()> {
        self.roots.clock_observations =
            map_remove(&self.roots.clock_observations, observation_id, self.overlay)?;
        Ok(())
    }

    fn put_history_compaction(
        &mut self,
        value: &crate::HistoryCompactionReceipt,
    ) -> DurableResult<()> {
        value.verify()?;
        let head = state_root_value_id(&StateRootValue::encode(
            StateRootLeafKind::HistoryCompaction,
            value,
        )?)?;
        insert_immutable_typed_value(
            &mut self.roots.history_compactions,
            &value.compaction_id,
            StateRootLeafKind::HistoryCompaction,
            value,
            "Machine history compaction receipt",
            self.overlay,
        )?;
        self.roots.history_compaction_head = Some(head);
        Ok(())
    }

    #[cfg(test)]
    fn append_journal(
        &mut self,
        journal_id: &str,
        records: &[crate::JournalRecord],
    ) -> DurableResult<()> {
        append_application_journal(self.roots, journal_id, records, self.overlay)?;
        Ok(())
    }

    #[cfg(test)]
    fn replace_journal_prefix(
        &mut self,
        receipt: &crate::ApplicationJournalPrefixReplacementReceipt,
    ) -> DurableResult<()> {
        replace_application_journal_prefix(self.roots, receipt, self.overlay)?;
        Ok(())
    }

    fn put_coupled_checkpoint_receipt(
        &mut self,
        value: &crate::CoupledCheckpointReceipt,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.coupled_checkpoint_receipts,
            &value.coupling_id,
            StateRootLeafKind::CoupledCheckpointReceipt,
            value,
            "coupled checkpoint receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_resource_command_receipt(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceCommandReceipt,
    ) -> DurableResult<()> {
        insert_resource_command_receipt(self.roots, value, self.overlay)?;
        Ok(())
    }

    fn put_resource_retention_current(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceRetentionCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.resource_retention_current,
            &value.family.retention_key,
            StateRootLeafKind::ResourceRetentionCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_resource_pin_current(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourcePinCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.resource_pin_current,
            &value.pin.pin_id,
            StateRootLeafKind::ResourcePinCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_resource_delete_current(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceDeleteCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.resource_delete_current,
            &value.intent.delete_id,
            StateRootLeafKind::ResourceDeleteCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_resource_handoff_current(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceHandoffCurrent,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.resource_handoff_current,
            &value.receipt.handoff.transfer_id,
            StateRootLeafKind::ResourceHandoffCurrent,
            value,
            "Resource handoff current",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_resource_handoff_slot(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceHandoffIndexEntry,
    ) -> DurableResult<()> {
        insert_resource_handoff_slot(self.roots, value, self.overlay)?;
        Ok(())
    }

    fn put_resource_handoff_activation_current(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceHandoffActivationCurrent,
    ) -> DurableResult<()> {
        insert_resource_handoff_activation_current(self.roots, value, self.overlay)?;
        Ok(())
    }

    fn append_resource_handoff_index(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceHandoffIndexEntry,
    ) -> DurableResult<()> {
        append_resource_handoff_index(self.roots, value, false, self.overlay)?;
        Ok(())
    }

    fn append_resource_handoff_activation_index(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceHandoffActivationIndexEntry,
    ) -> DurableResult<()> {
        append_resource_handoff_activation_index(self.roots, value, self.overlay)?;
        Ok(())
    }

    fn put_agent_command(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentCommand,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.agent_commands,
            &cymule_profile_protocol::agent::agent_command_key(&value.command_id)?,
            StateRootLeafKind::AgentCommand,
            value,
            "Agent command",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_agent_command_receipt(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentCommandReceipt,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.agent_command_receipts,
            &cymule_profile_protocol::agent::agent_command_key(&value.command_id)?,
            StateRootLeafKind::AgentCommandReceipt,
            value,
            "Agent command receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_agent_input_suspension_receipt(
        &mut self,
        value: &crate::model::AgentInputSuspensionReceipt,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.agent_input_suspension_receipts,
            &crate::model::agent_input_suspension_key(&value.wait.wait_id)?,
            StateRootLeafKind::AgentInputSuspensionReceipt,
            value,
            "Agent input suspension receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_agent_input_completion_receipt(
        &mut self,
        value: &crate::model::AgentInputCompletionReceipt,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.agent_input_completion_receipts,
            &crate::model::agent_input_completion_key(&value.wait.wait_id)?,
            StateRootLeafKind::AgentInputCompletionReceipt,
            value,
            "Agent input completion receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_agent_session_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentSessionCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.agent_sessions,
            &cymule_profile_protocol::agent::agent_session_key(&value.session_id)?,
            StateRootLeafKind::AgentSessionCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_agent_update_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentUpdateCurrent,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.agent_updates,
            &cymule_profile_protocol::agent::agent_update_key(&value.session_id, &value.update_id)?,
            StateRootLeafKind::AgentUpdateCurrent,
            value,
            "Agent update identity",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_agent_message_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentMessageCurrent,
    ) -> DurableResult<()> {
        insert_agent_message_current(self.roots, value, self.overlay)?;
        Ok(())
    }

    fn put_agent_tool_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentToolCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.agent_tools,
            &cymule_profile_protocol::agent::agent_tool_key(
                &value.session_id,
                &value.tool.tool_call_id,
            )?,
            StateRootLeafKind::AgentToolCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn apply_agent_target_claim(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentTargetClaimTransition,
    ) -> DurableResult<()> {
        apply_agent_target_claim(self.roots, value, self.overlay)
    }

    fn put_agent_elicitation_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentElicitationCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.agent_elicitations,
            &cymule_profile_protocol::agent::agent_elicitation_key(
                &value.session_id,
                &value.elicitation.request.request_id,
            )?,
            StateRootLeafKind::AgentElicitationCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_agent_occurrence_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentOccurrenceCurrent,
    ) -> DurableResult<()> {
        put_agent_occurrence_current(self.roots, value, self.overlay)?;
        Ok(())
    }

    fn put_agent_stream_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentStreamCurrent,
    ) -> DurableResult<()> {
        put_agent_stream_current(self.roots, value, self.overlay)?;
        Ok(())
    }

    fn put_agent_stream_chunk_current(
        &mut self,
        value: &cymule_profile_protocol::agent::AgentStreamChunkCurrent,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.agent_stream_chunks,
            &cymule_profile_protocol::agent::agent_stream_chunk_key(
                &value.session_id,
                &value.stream_id,
                value.chunk.sequence,
            )?,
            StateRootLeafKind::AgentStreamChunkCurrent,
            value,
            "Agent stream chunk",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_evolution_current(
        &mut self,
        value: &cymule_profile_protocol::evolution::EvolutionCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.evolution.currents,
            &cymule_profile_protocol::evolution::evolution_current_key(&value.evolution_id)?,
            StateRootLeafKind::EvolutionCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_evolution_command_alias(
        &mut self,
        value: &cymule_profile_protocol::evolution::EvolutionCommandAlias,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.evolution.command_aliases,
            &cymule_profile_protocol::evolution::evolution_command_alias_key(
                &value.evolution_id,
                &value.command_id,
            )?,
            StateRootLeafKind::EvolutionCommandAlias,
            value,
            "Evolution command alias",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_evolution_persistence_receipt(
        &mut self,
        value: &cymule_profile_protocol::evolution::EvolutionPersistenceReceipt,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.evolution.receipts,
            &cymule_profile_protocol::evolution::evolution_receipt_key(
                &value.command.evolution_id,
                &value.receipt_id,
            )?,
            StateRootLeafKind::EvolutionPersistenceReceipt,
            value,
            "Evolution persistence receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn put_evolution_mutation(
        &mut self,
        value: &cymule_profile_protocol::evolution::EvolutionMutation,
    ) -> DurableResult<()> {
        put_evolution_mutation(&mut self.roots.evolution, value, self.overlay)?;
        Ok(())
    }

    fn put_virtual_current(
        &mut self,
        value: &cymule_profile_protocol::virtual_work::VirtualCurrent,
    ) -> DurableResult<()> {
        put_typed_value(
            &mut self.roots.virtual_work.currents,
            &cymule_profile_protocol::virtual_work::virtual_current_key(&value.body.scheduler_id)?,
            StateRootLeafKind::VirtualCurrent,
            value,
            self.overlay,
        )?;
        Ok(())
    }

    fn put_virtual_persistence_receipt(
        &mut self,
        value: &cymule_profile_protocol::virtual_work::VirtualPersistenceReceipt,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.virtual_work.receipts,
            &cymule_profile_protocol::virtual_work::virtual_receipt_key(
                value.command.scheduler_id(),
                value.command.command_id(),
            )?,
            StateRootLeafKind::VirtualPersistenceReceipt,
            value,
            "Virtual persistence receipt",
            self.overlay,
        )?;
        Ok(())
    }

    fn apply_virtual_mutation(
        &mut self,
        value: &cymule_profile_protocol::virtual_work::VirtualStateMutation,
    ) -> DurableResult<()> {
        apply_virtual_mutation(&mut self.roots.virtual_work, value, self.overlay)?;
        Ok(())
    }

    fn put_resource_catalog_record(
        &mut self,
        value: &cymule_profile_protocol::resource::ResourceCatalogRecord,
    ) -> DurableResult<()> {
        insert_immutable_typed_value(
            &mut self.roots.resource_catalog_records,
            &value.record_id,
            StateRootLeafKind::ResourceCatalogRecord,
            value,
            "Resource catalog record",
            self.overlay,
        )?;
        Ok(())
    }
}

fn validate_agent_workspace_transition<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    roots: &StateRoots,
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    delta: Option<&cymule_core::MachineRootDelta>,
    receipt: &crate::CoupledCheckpointReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let crate::CoupledCheckpoint::AgentWorkspace { checkpoint } = &receipt.checkpoint else {
        return Err(DurableError::Validation(
            "workspace validation received another receipt kind".to_owned(),
        ));
    };
    checkpoint.verify()?;
    if checkpoint.source_machine_authority_root != current.machine_frontier.authority_root
        || checkpoint.machine_authority_root != frontier.authority_root
    {
        return Err(DurableError::Integrity {
            code: "state_root_agent_workspace_machine_mismatch".to_owned(),
            message: "Agent workspace receipt does not bind the actual source/result Core roots"
                .to_owned(),
        });
    }
    let before: crate::Continuation =
        map_get(&current.roots.continuations, &checkpoint.run_id, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_agent_workspace_continuation_missing".to_owned(),
                message: "Agent workspace has no exact source Continuation".to_owned(),
            })?
            .decode(StateRootLeafKind::Continuation)?;
    before.verify_wire()?;
    if crate::model::agent_workspace_continuation_digest(&before)?
        != checkpoint.source_continuation_digest
        || map_get(&roots.continuations, &checkpoint.run_id, overlay)?.as_ref()
            != Some(&StateRootValue::encode(
                StateRootLeafKind::Continuation,
                checkpoint.continuation.as_ref(),
            )?)
    {
        return Err(DurableError::Integrity {
            code: "state_root_agent_workspace_continuation_mismatch".to_owned(),
            message: "Agent workspace receipt changed its exact source or target Continuation"
                .to_owned(),
        });
    }
    verify_continuation_receipt_artifacts(&current.roots, &before, overlay)?;
    verify_agent_workspace_artifacts(roots, checkpoint, overlay)?;
    let before_run =
        require_machine_run_current(&current.machine_frontier, &checkpoint.run_id, overlay)?;
    let after_run = require_machine_run_current(frontier, &checkpoint.run_id, overlay)?;
    let intent = checkpoint
        .effect_after
        .as_ref()
        .or(checkpoint.effect_before.as_ref())
        .map(|effect| effect.intent_id.as_str());
    let before_values = load_workspace_neighborhood(&current.roots, &before_run, intent, overlay)?;
    let after_values = load_workspace_neighborhood(roots, &after_run, intent, overlay)?;
    if before_values.effect != checkpoint.effect_before
        || before_values.outbox != checkpoint.outbox_before
        || before_values.lease != checkpoint.lease_before
        || after_values.effect != checkpoint.effect_after
        || after_values.outbox != checkpoint.outbox_after
        || after_values.lease != checkpoint.lease_after
    {
        return Err(DurableError::Integrity {
            code: "state_root_agent_workspace_neighborhood_mismatch".to_owned(),
            message:
                "Agent workspace receipt differs from its exact Effect/outbox/lease neighborhood"
                    .to_owned(),
        });
    }
    validate_workspace_batch(
        current, roots, frontier, delta, receipt, checkpoint, overlay,
    )
}

fn validate_workspace_batch<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    roots: &StateRoots,
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    delta: Option<&cymule_core::MachineRootDelta>,
    coupled: &crate::CoupledCheckpointReceipt,
    checkpoint: &crate::model::AgentWorkspaceCheckpoint,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let (command, agent_receipt) = load_workspace_agent_receipt(roots, checkpoint, overlay)?;
    let (
        cymule_profile_protocol::agent::AgentCommandAction::Workspace(workspace),
        cymule_profile_protocol::agent::AgentCommandOutcome::Workspace(agent),
    ) = (&command.action, &agent_receipt.outcome)
    else {
        return Err(DurableError::Integrity {
            code: "state_root_agent_workspace_receipt_kind_mismatch".to_owned(),
            message: "Agent workspace coupling has an unrelated outer command or receipt"
                .to_owned(),
        });
    };
    crate::model::verify_workspace_receipt_link(&command, workspace, agent, coupled, checkpoint)?;
    let Some(batch_id) = &checkpoint.core_batch_id else {
        if delta.is_some()
            || frontier != current.machine_frontier()
            || roots.continuations != current.roots.continuations
            || roots.outbox != current.roots.outbox
            || roots.leases != current.roots.leases
            || !crate::model::workspace_checkpoint_commands(workspace, checkpoint)?.is_empty()
        {
            return Err(DurableError::Integrity {
                code: "state_root_agent_workspace_unreceipted_machine_change".to_owned(),
                message: "Agent workspace without a Core batch changed M1 authority".to_owned(),
            });
        }
        for reference in workspace_observer_references(agent) {
            require_receipt_artifact(roots, &reference, overlay)?;
        }
        return Ok(());
    };
    let delta = delta.ok_or_else(|| DurableError::Integrity {
        code: "state_root_agent_workspace_core_stage_missing".to_owned(),
        message: "Agent workspace Core batch must be produced in the same StateRoot transition"
            .to_owned(),
    })?;
    let batch = delta
        .batches
        .get(batch_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_agent_workspace_core_batch_missing".to_owned(),
            message: "Agent workspace receipt has no exact batch in the actual Core stage"
                .to_owned(),
        })?;
    batch.verify()?;
    if delta.batches.len() != 1
        || checkpoint.core_batch_receipt_id.as_deref() != Some(batch.batch_receipt_id.as_str())
        || batch.admission_parent_authority_root != checkpoint.source_machine_authority_root
        || batch.result_authority_root != checkpoint.machine_authority_root
        || delta
            .commands
            .values()
            .any(|record| record.envelope.run_id != checkpoint.run_id)
        || delta.commands.len() != batch.members.len()
        || delta.admissions.len() != batch.members.len()
        || delta.events.len() != batch.event_ids.len()
    {
        return Err(DurableError::Integrity {
            code: "state_root_agent_workspace_core_batch_mismatch".to_owned(),
            message: "Agent workspace receipt differs from its exact source/result Core batch"
                .to_owned(),
        });
    }
    let entries = batch
        .members
        .iter()
        .map(|member| {
            terminal_receipt_delta_entry(Some(delta), &member.command_id).map(|(entry, _)| entry)
        })
        .collect::<DurableResult<Vec<_>>>()?;
    crate::model::validate_workspace_checkpoint_batch(
        &command, workspace, agent, checkpoint, batch, &entries,
    )?;
    verify_workspace_material_records(&current.roots, roots, delta, batch, agent, overlay)?;
    Ok(())
}

fn load_workspace_agent_receipt<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    checkpoint: &crate::model::AgentWorkspaceCheckpoint,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<(
    cymule_profile_protocol::agent::AgentCommand,
    cymule_profile_protocol::agent::AgentCommandReceipt,
)> {
    let key = cymule_profile_protocol::agent::agent_command_key(&checkpoint.agent_command_id)?;
    let command: cymule_profile_protocol::agent::AgentCommand =
        map_get(&roots.agent_commands, &key, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_agent_workspace_command_missing".to_owned(),
                message: "Agent workspace coupling has no exact outer command".to_owned(),
            })?
            .decode(StateRootLeafKind::AgentCommand)?;
    let receipt: cymule_profile_protocol::agent::AgentCommandReceipt =
        map_get(&roots.agent_command_receipts, &key, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_agent_workspace_receipt_missing".to_owned(),
                message: "Agent workspace coupling has no exact outer receipt".to_owned(),
            })?
            .decode(StateRootLeafKind::AgentCommandReceipt)?;
    receipt.verify_for(&command)?;
    if command.command_id != checkpoint.agent_command_id {
        return Err(DurableError::Integrity {
            code: "state_root_agent_workspace_command_key_mismatch".to_owned(),
            message: "Agent workspace outer command changed its exact storage key".to_owned(),
        });
    }
    Ok((command, receipt))
}

fn workspace_observer_references(
    agent: &cymule_profile_protocol::agent::WorkspaceScopeCheckpoint,
) -> BTreeSet<cymule_core::ArtifactRef> {
    let mut references = BTreeSet::new();
    if let Some(receipt) = &agent.receipt {
        references.insert(receipt.evidence.clone());
    }
    if let Some(observation) = agent
        .occurrence
        .current
        .occurrence
        .recovery_observations
        .last()
    {
        for block in &observation.evidence {
            if let cymule_profile_protocol::agent::ContentBlock::Artifact { artifact } = block {
                references.insert(artifact.clone());
            }
        }
    }
    references
}

fn verify_workspace_material_records<R: StateRootResolver + ?Sized>(
    parent: &StateRoots,
    roots: &StateRoots,
    delta: &cymule_core::MachineRootDelta,
    batch: &cymule_core::MachineCommandBatchRecord,
    agent: &cymule_profile_protocol::agent::WorkspaceScopeCheckpoint,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let mut required = workspace_observer_references(agent);
    let declared = batch
        .material_source
        .as_ref()
        .map_or_else(BTreeSet::new, |source| {
            source.artifacts.iter().cloned().collect()
        });
    required.extend(declared.iter().cloned());
    let mut records = BTreeMap::new();
    let mut bytes = 0_usize;
    for reference in &required {
        require_receipt_artifact(roots, reference, overlay)?;
        let record: cymule_core::ArtifactRecord =
            map_get(&roots.machine_artifacts, &reference.artifact_id, overlay)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "state_root_agent_workspace_material_missing".to_owned(),
                    message: "workspace material disappeared during its exact read".to_owned(),
                })?
                .decode(StateRootLeafKind::MachineArtifact)?;
        bytes = bytes
            .checked_add(cymule_core::canonical_bytes(&record)?.len())
            .filter(|bytes| {
                *bytes <= cymule_core::durable_internal::MAX_PINNED_MACHINE_READ_SET_BYTES
            })
            .ok_or_else(|| {
                DurableError::Validation(
                    "workspace material closure exceeds its bounded read set".to_owned(),
                )
            })?;
        let previous = map_get(&parent.machine_artifacts, &reference.artifact_id, overlay)?;
        if previous.is_none() {
            if !declared.contains(reference)
                || delta.artifacts.get(&reference.artifact_id) != Some(&record)
            {
                return Err(DurableError::Integrity {
                    code: "state_root_agent_workspace_new_material_unclaimed".to_owned(),
                    message: "new workspace evidence was not in its exact Core material admission"
                        .to_owned(),
                });
            }
        } else if previous
            != Some(StateRootValue::encode(
                StateRootLeafKind::MachineArtifact,
                &record,
            )?)
        {
            return Err(DurableError::Integrity {
                code: "state_root_agent_workspace_material_rewrite".to_owned(),
                message: "workspace material changed its exact parent Artifact".to_owned(),
            });
        }
        records.insert(reference.clone(), record);
    }
    if let Some(source) = &batch.material_source {
        let artifacts = source
            .artifacts
            .iter()
            .map(|reference| records[reference].clone())
            .collect();
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            source.source_command_id.clone(),
            Vec::new(),
            artifacts,
        )?;
        if batch.material_digest.as_deref() != Some(material.material_digest()) {
            return Err(DurableError::Integrity {
                code: "state_root_agent_workspace_material_digest_mismatch".to_owned(),
                message: "workspace records do not reproduce the Core batch material digest"
                    .to_owned(),
            });
        }
    }
    Ok(())
}

#[derive(Default)]
struct WorkspaceNeighborhood {
    effect: Option<cymule_core::EffectProjection>,
    outbox: Option<crate::EffectDispatch>,
    lease: Option<crate::CoordinationLease>,
}

fn load_workspace_neighborhood<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    run: &cymule_core::durable_internal::MachineRunCurrent,
    intent: Option<&str>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<WorkspaceNeighborhood> {
    let Some(intent) = intent else {
        return Ok(WorkspaceNeighborhood::default());
    };
    Ok(WorkspaceNeighborhood {
        effect: map_get(&run.children.effects, intent, overlay)?
            .map(|value| value.decode(StateRootLeafKind::MachineEffect))
            .transpose()?,
        outbox: load_run_effect_dispatch(roots, &run.run_id, intent, overlay)?,
        lease: map_get(&roots.leases, intent, overlay)?
            .map(|value| value.decode(StateRootLeafKind::Lease))
            .transpose()?,
    })
}

fn verify_continuation_receipt_artifacts<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    continuation: &crate::Continuation,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    continuation.verify_wire()?;
    let mut references = BTreeSet::from([cymule_core::ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: continuation.binding_context.clone(),
        kind: cymule_core::EXECUTION_BINDING_ARTIFACT_KIND.to_owned(),
    }]);
    references.extend(continuation.state.iter().cloned());
    for frame in &continuation.frames {
        references.insert(frame.input.clone());
        references.extend(frame.locals.values().cloned());
    }
    for reference in references {
        require_receipt_artifact(roots, &reference, overlay)?;
    }
    Ok(())
}

fn verify_agent_workspace_artifacts<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    checkpoint: &crate::model::AgentWorkspaceCheckpoint,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    checkpoint.verify()?;
    verify_continuation_receipt_artifacts(roots, &checkpoint.continuation, overlay)?;
    let mut references = BTreeSet::new();
    for effect in checkpoint
        .effect_before
        .iter()
        .chain(checkpoint.effect_after.iter())
    {
        references.insert(effect.args.clone());
        references.insert(effect.execution_binding.clone());
    }
    for dispatch in checkpoint
        .outbox_before
        .iter()
        .chain(checkpoint.outbox_after.iter())
    {
        references.insert(dispatch.input.clone());
        references.insert(dispatch.execution_binding.clone());
        references.extend(dispatch.result.iter().cloned());
    }
    for reference in references {
        require_receipt_artifact(roots, &reference, overlay)?;
    }
    Ok(())
}

fn validate_terminal_receipt_operations<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    delta: Option<&cymule_core::MachineRootDelta>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for operation in operations {
        match operation {
            crate::DurableOperation::PutCancellationReceipt { value }
                if map_get(
                    &current.roots.cancellation_receipts,
                    &value.command.cancellation_id,
                    overlay,
                )?
                .is_none() =>
            {
                let run = require_machine_run_current(frontier, &value.command.run_id, overlay)?;
                crate::model::validate_cancellation_receipt_closure(
                    value,
                    &run.run_id,
                    &run.execution_status,
                )?;
                let (entry, batch) =
                    terminal_receipt_delta_entry(delta, &value.command.cancellation_id)?;
                crate::model::validate_cancellation_receipt_command(value, &entry, batch)?;
            }
            crate::DurableOperation::PutEffectResolutionReceipt { value }
                if map_get(
                    &current.roots.effect_resolution_receipts,
                    &value.command.resolution_id,
                    overlay,
                )?
                .is_none() =>
            {
                let dispatch = verify_effect_resolution_current(frontier, roots, value, overlay)?;
                verify_effect_resolution_source(&current.roots, &dispatch, overlay)?;
                let (entry, batch) =
                    terminal_receipt_delta_entry(delta, &value.command.resolution_id)?;
                crate::model::validate_effect_resolution_receipt_command(value, &entry, batch)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn terminal_receipt_delta_entry<'a>(
    delta: Option<&'a cymule_core::MachineRootDelta>,
    command_id: &str,
) -> DurableResult<(
    cymule_core::MachineCommandArchiveEntry,
    &'a cymule_core::MachineCommandBatchRecord,
)> {
    let delta = delta.ok_or_else(|| DurableError::Integrity {
        code: "state_root_terminal_receipt_core_stage_missing".to_owned(),
        message: "a fresh terminal receipt requires its exact new Core command batch".to_owned(),
    })?;
    let command = delta
        .commands
        .get(command_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_terminal_receipt_command_missing".to_owned(),
            message: format!("terminal receipt {command_id} has no command in this Core stage"),
        })?;
    let admission = delta
        .admissions
        .iter()
        .find(|admission| admission.command_id == command_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_terminal_receipt_admission_missing".to_owned(),
            message: format!("terminal receipt {command_id} has no new Core admission"),
        })?;
    let batch = delta
        .batches
        .get(&command.batch_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_terminal_receipt_batch_missing".to_owned(),
            message: format!("terminal receipt {command_id} has no new Core batch"),
        })?;
    let entry = cymule_core::MachineCommandArchiveEntry {
        command: command.clone(),
        admission: admission.clone(),
        events: delta
            .events
            .iter()
            .filter(|event| event.command_id == command_id)
            .cloned()
            .collect(),
    };
    batch.verify_entry(&entry)?;
    Ok((entry, batch))
}

fn verify_effect_resolution_source<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    after: &crate::EffectDispatch,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let before: crate::EffectDispatch =
        load_run_effect_dispatch(roots, &after.run_id, &after.intent_id, overlay)?.ok_or_else(
            || DurableError::Integrity {
                code: "state_root_effect_resolution_source_missing".to_owned(),
                message: "terminal Effect resolution has no exact original Unknown dispatch"
                    .to_owned(),
            },
        )?;
    before.verify_wire()?;
    let mut expected = before.clone();
    expected.state = after.state;
    expected.result.clone_from(&after.result);
    expected.reconciliation = after.reconciliation;
    expected.execution_availability = after.execution_availability;
    if before.state != crate::OutboxState::Unknown || &expected != after {
        return Err(DurableError::Integrity {
            code: "state_root_effect_resolution_source_mismatch".to_owned(),
            message:
                "terminal Effect resolution did not preserve its original Unknown dispatch pin"
                    .to_owned(),
        });
    }
    Ok(())
}

fn validate_component_attempt_operation_set<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let updates = collect_component_frontier_updates(operations)?;
    if updates.occurrence_updates.is_empty() && updates.attempt_updates.is_empty() {
        return Ok(());
    }
    let mut accounted_attempts = BTreeSet::new();
    for (occurrence_id, proposed) in updates.occurrence_updates {
        let latest_id = validate_component_occurrence_update(
            current,
            roots,
            &occurrence_id,
            proposed,
            &updates.attempt_updates,
            overlay,
        )?;
        accounted_attempts.insert(latest_id);
    }
    for (attempt_id, proposed) in &updates.attempt_updates {
        if accounted_attempts.contains(attempt_id) {
            continue;
        }
        validate_component_supersede_update(current, roots, attempt_id, proposed, overlay)?;
        accounted_attempts.insert(attempt_id.clone());
    }
    Ok(())
}

struct ComponentFrontierUpdates<'a> {
    occurrence_updates: BTreeMap<String, &'a crate::ComponentOccurrence>,
    attempt_updates: BTreeMap<String, &'a crate::OperationAttempt>,
}

fn collect_component_frontier_updates(
    operations: &[crate::DurableOperation],
) -> DurableResult<ComponentFrontierUpdates<'_>> {
    let mut occurrence_updates = BTreeMap::new();
    let mut attempt_updates = BTreeMap::new();
    for operation in operations {
        match operation {
            crate::DurableOperation::PutComponentOccurrence { value }
                if occurrence_updates
                    .insert(value.occurrence_id.clone(), value)
                    .is_some() =>
            {
                return Err(DurableError::Validation(format!(
                    "one StateRoot transition updates component occurrence {} more than once",
                    value.occurrence_id
                )));
            }
            crate::DurableOperation::PutOperationAttempt { value }
                if attempt_updates
                    .insert(value.attempt_id.clone(), value)
                    .is_some() =>
            {
                return Err(DurableError::Validation(format!(
                    "one StateRoot transition updates operation Attempt {} more than once",
                    value.attempt_id
                )));
            }
            _ => {}
        }
    }
    Ok(ComponentFrontierUpdates {
        occurrence_updates,
        attempt_updates,
    })
}

fn validate_component_occurrence_update<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    roots: &StateRoots,
    occurrence_id: &str,
    proposed: &crate::ComponentOccurrence,
    attempt_updates: &BTreeMap<String, &crate::OperationAttempt>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<String> {
    proposed.verify()?;
    let occurrence =
        load_component_occurrence_from_root(&roots.component_occurrences, occurrence_id, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_component_occurrence_missing_after_put".to_owned(),
                message: format!(
                    "component occurrence {occurrence_id} is absent after its exact put"
                ),
            })?;
    if &occurrence != proposed {
        return Err(DurableError::Integrity {
            code: "state_root_component_occurrence_value_mismatch".to_owned(),
            message: format!("component occurrence {occurrence_id} differs from its exact put"),
        });
    }
    let latest = load_operation_attempt_from_root(
        &roots.operation_attempts,
        &occurrence.latest_attempt_id,
        overlay,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "state_root_component_latest_attempt_missing".to_owned(),
        message: format!("component occurrence {occurrence_id} has no latest provider Attempt"),
    })?;
    pinned_machine::validate_component_attempt_frontier(&occurrence, &latest)?;
    validate_component_query_member(
        roots,
        &occurrence.run_id,
        RunQueryIndexKind::Occurrences,
        &occurrence.occurrence_id,
        StateRootLeafKind::ComponentOccurrence,
        &occurrence,
        overlay,
    )?;
    validate_component_query_member(
        roots,
        &latest.run_id,
        RunQueryIndexKind::Attempts,
        &latest.attempt_id,
        StateRootLeafKind::OperationAttempt,
        &latest,
        overlay,
    )?;

    validate_component_occurrence_predecessor(
        current,
        &occurrence,
        &latest,
        attempt_updates,
        overlay,
    )?;
    Ok(latest.attempt_id)
}

fn validate_component_occurrence_predecessor<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    occurrence: &crate::ComponentOccurrence,
    latest: &crate::OperationAttempt,
    attempt_updates: &BTreeMap<String, &crate::OperationAttempt>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let occurrence_id = &occurrence.occurrence_id;
    let previous = load_component_occurrence_from_root(
        &current.roots.component_occurrences,
        occurrence_id,
        overlay,
    )?;
    match previous {
        None => {
            if occurrence.state != crate::ComponentOccurrenceState::Pending
                || latest.state != crate::OperationAttemptState::Running
                || occurrence.attempt_count != 1
                || latest.attempt_ordinal != 1
                || latest.previous_attempt_id.is_some()
                || attempt_updates.get(&latest.attempt_id) != Some(&latest)
            {
                return Err(DurableError::Integrity {
                    code: "state_root_component_initial_attempt_mismatch".to_owned(),
                    message: format!(
                        "new Pending component occurrence {occurrence_id} does not publish its exact first Running Attempt"
                    ),
                });
            }
        }
        Some(previous) => {
            previous.verify()?;
            let previous_latest = load_operation_attempt_from_root(
                &current.roots.operation_attempts,
                &previous.latest_attempt_id,
                overlay,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_component_previous_attempt_missing".to_owned(),
                message: format!(
                    "component occurrence {occurrence_id} lost its previous latest Attempt"
                ),
            })?;
            pinned_machine::validate_component_attempt_frontier(&previous, &previous_latest)?;
            validate_component_occurrence_stable_identity(&previous, occurrence)?;
            validate_component_existing_frontier(
                &previous,
                &previous_latest,
                occurrence,
                latest,
                attempt_updates,
            )?;
        }
    }
    Ok(())
}

fn validate_component_existing_frontier(
    previous: &crate::ComponentOccurrence,
    previous_latest: &crate::OperationAttempt,
    occurrence: &crate::ComponentOccurrence,
    latest: &crate::OperationAttempt,
    attempt_updates: &BTreeMap<String, &crate::OperationAttempt>,
) -> DurableResult<()> {
    let occurrence_id = &occurrence.occurrence_id;
    if occurrence.attempt_count == previous.attempt_count
        && occurrence.latest_attempt_id == previous.latest_attempt_id
    {
        if previous.state != crate::ComponentOccurrenceState::Pending
            || occurrence.state != crate::ComponentOccurrenceState::Completed
            || previous_latest.state != crate::OperationAttemptState::Running
            || latest.state != crate::OperationAttemptState::Completed
            || attempt_updates.get(&latest.attempt_id) != Some(&latest)
        {
            return Err(DurableError::Integrity {
                code: "state_root_component_completion_frontier_mismatch".to_owned(),
                message: format!(
                    "component occurrence {occurrence_id} completion did not close its exact latest Attempt"
                ),
            });
        }
        validate_operation_attempt_stable_identity(previous_latest, latest)?;
    } else if occurrence.attempt_count
        == previous.attempt_count.checked_add(1).ok_or_else(|| {
            DurableError::Validation("component occurrence Attempt count overflowed".to_owned())
        })?
        && previous.state == crate::ComponentOccurrenceState::Pending
        && occurrence.state == crate::ComponentOccurrenceState::Pending
        && latest.previous_attempt_id.as_deref() == Some(previous.latest_attempt_id.as_str())
    {
        if previous_latest.state != crate::OperationAttemptState::Superseded
            || latest.state != crate::OperationAttemptState::Running
            || attempt_updates.get(&latest.attempt_id) != Some(&latest)
        {
            return Err(DurableError::Integrity {
                code: "state_root_component_attempt_successor_mismatch".to_owned(),
                message: format!(
                    "component occurrence {occurrence_id} did not append Running after its exact Superseded frontier"
                ),
            });
        }
    } else {
        return Err(DurableError::Integrity {
            code: "state_root_component_attempt_frontier_jump".to_owned(),
            message: format!(
                "component occurrence {occurrence_id} skipped or rewrote its Attempt frontier"
            ),
        });
    }
    Ok(())
}

fn validate_component_supersede_update<R: StateRootResolver + ?Sized>(
    current: &StateRootManifest,
    roots: &StateRoots,
    attempt_id: &str,
    proposed: &crate::OperationAttempt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let previous =
        load_operation_attempt_from_root(&current.roots.operation_attempts, attempt_id, overlay)?
            .ok_or_else(|| DurableError::Integrity {
            code: "state_root_component_detached_attempt_update".to_owned(),
            message: format!("operation Attempt {attempt_id} has no retained frontier"),
        })?;
    let next = load_operation_attempt_from_root(&roots.operation_attempts, attempt_id, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_component_superseded_attempt_missing".to_owned(),
            message: format!("operation Attempt {attempt_id} disappeared while superseding"),
        })?;
    if &next != proposed
        || previous.state != crate::OperationAttemptState::Running
        || next.state != crate::OperationAttemptState::Superseded
    {
        return Err(DurableError::Integrity {
            code: "state_root_component_supersede_frontier_mismatch".to_owned(),
            message: format!(
                "operation Attempt {attempt_id} did not make the sole Running-to-Superseded edge"
            ),
        });
    }
    validate_operation_attempt_stable_identity(&previous, &next)?;
    let occurrence = load_component_occurrence_from_root(
        &roots.component_occurrences,
        &next.occurrence_id,
        overlay,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "state_root_component_occurrence_missing".to_owned(),
        message: format!("operation Attempt {attempt_id} has no owning component occurrence"),
    })?;
    pinned_machine::validate_component_attempt_frontier(&occurrence, &next)?;
    validate_component_query_member(
        roots,
        &occurrence.run_id,
        RunQueryIndexKind::Occurrences,
        &occurrence.occurrence_id,
        StateRootLeafKind::ComponentOccurrence,
        &occurrence,
        overlay,
    )?;
    validate_component_query_member(
        roots,
        &next.run_id,
        RunQueryIndexKind::Attempts,
        &next.attempt_id,
        StateRootLeafKind::OperationAttempt,
        &next,
        overlay,
    )?;
    Ok(())
}

fn load_component_occurrence_from_root<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    occurrence_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<crate::ComponentOccurrence>> {
    let occurrence: Option<crate::ComponentOccurrence> = map_get(root, occurrence_id, overlay)?
        .map(|value| value.decode(StateRootLeafKind::ComponentOccurrence))
        .transpose()?;
    if let Some(value) = &occurrence {
        value.verify()?;
        if value.occurrence_id != occurrence_id {
            return Err(DurableError::Integrity {
                code: "state_root_component_occurrence_key_mismatch".to_owned(),
                message: format!("component occurrence key {occurrence_id} changed identity"),
            });
        }
    }
    Ok(occurrence)
}

fn load_operation_attempt_from_root<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    attempt_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<crate::OperationAttempt>> {
    let attempt: Option<crate::OperationAttempt> = map_get(root, attempt_id, overlay)?
        .map(|value| value.decode(StateRootLeafKind::OperationAttempt))
        .transpose()?;
    if let Some(value) = &attempt {
        value.verify()?;
        if value.attempt_id != attempt_id {
            return Err(DurableError::Integrity {
                code: "state_root_operation_attempt_key_mismatch".to_owned(),
                message: format!("operation Attempt key {attempt_id} changed identity"),
            });
        }
    }
    Ok(attempt)
}

fn validate_component_occurrence_stable_identity(
    previous: &crate::ComponentOccurrence,
    next: &crate::ComponentOccurrence,
) -> DurableResult<()> {
    let mut expected = next.clone();
    expected.outcome.clone_from(&previous.outcome);
    expected.attempt_count = previous.attempt_count;
    expected
        .latest_attempt_id
        .clone_from(&previous.latest_attempt_id);
    expected
        .continuation_digest
        .clone_from(&previous.continuation_digest);
    expected.state = previous.state;
    if &expected != previous {
        return Err(DurableError::Integrity {
            code: "state_root_component_occurrence_semantic_rewrite".to_owned(),
            message: format!(
                "component occurrence {} changed immutable semantics",
                next.occurrence_id
            ),
        });
    }
    Ok(())
}

fn validate_operation_attempt_stable_identity(
    previous: &crate::OperationAttempt,
    next: &crate::OperationAttempt,
) -> DurableResult<()> {
    let mut expected = next.clone();
    expected.state = previous.state;
    expected.outcome.clone_from(&previous.outcome);
    if &expected != previous {
        return Err(DurableError::Integrity {
            code: "state_root_operation_attempt_semantic_rewrite".to_owned(),
            message: format!(
                "operation Attempt {} changed immutable semantics",
                next.attempt_id
            ),
        });
    }
    Ok(())
}

fn validate_component_query_member<T, R>(
    roots: &StateRoots,
    run_id: &str,
    index: RunQueryIndexKind,
    key: &str,
    kind: StateRootLeafKind,
    value: &T,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    let descriptor = map_get(&roots.run_query_indexes, run_id, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "run_query_indexes_missing".to_owned(),
            message: format!("component frontier Run {run_id} has no query indexes"),
        })?
        .decode_run_query_indexes(run_id)?;
    let selected = match index {
        RunQueryIndexKind::Occurrences => &descriptor.occurrences,
        RunQueryIndexKind::Attempts => &descriptor.attempts,
        _ => {
            return Err(DurableError::Integrity {
                code: "state_root_component_query_family_mismatch".to_owned(),
                message: "component frontier selected an unrelated Run query family".to_owned(),
            });
        }
    };
    let expected = StateRootValue::encode(kind, value)?;
    if map_get(selected, key, overlay)?.as_ref() != Some(&expected) {
        return Err(DurableError::Integrity {
            code: "state_root_component_query_member_mismatch".to_owned(),
            message: format!("component frontier key {key} differs from its Run query member"),
        });
    }
    Ok(())
}

fn validate_evolution_operation_set<R: StateRootResolver + ?Sized>(
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let mut current = None;
    let mut alias = None;
    let mut receipt = None;
    let mut mutations = Vec::new();
    for operation in operations {
        match operation {
            crate::DurableOperation::PutEvolutionCurrent { value } => {
                if current.replace(value).is_some() {
                    return Err(DurableError::Validation(
                        "one durable transition cannot contain multiple Evolution currents"
                            .to_owned(),
                    ));
                }
            }
            crate::DurableOperation::PutEvolutionCommandAlias { value } => {
                if alias.replace(value).is_some() {
                    return Err(DurableError::Validation(
                        "one durable transition cannot contain multiple Evolution aliases"
                            .to_owned(),
                    ));
                }
            }
            crate::DurableOperation::PutEvolutionPersistenceReceipt { value } => {
                if receipt.replace(value).is_some() {
                    return Err(DurableError::Validation(
                        "one durable transition cannot contain multiple Evolution receipts"
                            .to_owned(),
                    ));
                }
            }
            crate::DurableOperation::PutEvolutionMutation { value } => {
                mutations.push(value);
            }
            _ => {}
        }
    }
    if current.is_none() && alias.is_none() && receipt.is_none() && mutations.is_empty() {
        return Ok(());
    }
    let (Some(current), Some(alias), Some(receipt)) = (current, alias, receipt) else {
        return Err(DurableError::Validation(
            "an Evolution transition requires one current, command alias, and semantic receipt"
                .to_owned(),
        ));
    };
    current.verify()?;
    alias.verify()?;
    receipt.verify()?;
    let mut writes = mutations
        .iter()
        .map(|mutation| mutation.write())
        .collect::<Result<Vec<_>, _>>()?;
    writes.sort_by(|left, right| {
        (left.family, left.storage_key.as_str()).cmp(&(right.family, right.storage_key.as_str()))
    });
    if current.evolution_id != alias.evolution_id
        || current.evolution_id != receipt.command.evolution_id
        || current.last_receipt_id != receipt.receipt_id
        || alias.command_id != receipt.command.command.command_id()
        || alias.persistence_id != receipt.command.persistence_id
        || alias.receipt_id != receipt.receipt_id
        || receipt.mutations != writes
    {
        return Err(DurableError::Integrity {
            code: "state_root_evolution_postcondition_mismatch".to_owned(),
            message: "Evolution current, alias, receipt, and normalized writes are not one exact postcondition"
                .to_owned(),
        });
    }
    if mutations.iter().any(|mutation| {
        mutation.evolution_id() != current.evolution_id || mutation.revision() != current.revision
    }) {
        return Err(DurableError::Integrity {
            code: "state_root_evolution_mutation_partition_mismatch".to_owned(),
            message: "Evolution normalized writes do not belong to the exact result partition and revision"
                .to_owned(),
        });
    }

    validate_evolution_retained_authority(roots, current, alias, receipt, mutations, overlay)
}

fn validate_evolution_retained_authority<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    current: &cymule_profile_protocol::evolution::EvolutionCurrent,
    alias: &cymule_profile_protocol::evolution::EvolutionCommandAlias,
    receipt: &cymule_profile_protocol::evolution::EvolutionPersistenceReceipt,
    mutations: Vec<&cymule_profile_protocol::evolution::EvolutionMutation>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let current_key =
        cymule_profile_protocol::evolution::evolution_current_key(&current.evolution_id)?;
    let stored_current = map_get(&roots.evolution.currents, &current_key, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_evolution_current_missing".to_owned(),
            message: "Evolution transition did not retain its scalar current".to_owned(),
        })?
        .decode::<cymule_profile_protocol::evolution::EvolutionCurrent>(
            StateRootLeafKind::EvolutionCurrent,
        )?;
    let alias_key = cymule_profile_protocol::evolution::evolution_command_alias_key(
        &alias.evolution_id,
        &alias.command_id,
    )?;
    let stored_alias = map_get(&roots.evolution.command_aliases, &alias_key, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_evolution_alias_missing".to_owned(),
            message: "Evolution transition did not retain its command alias".to_owned(),
        })?
        .decode::<cymule_profile_protocol::evolution::EvolutionCommandAlias>(
            StateRootLeafKind::EvolutionCommandAlias,
        )?;
    let receipt_key = cymule_profile_protocol::evolution::evolution_receipt_key(
        &receipt.command.evolution_id,
        &receipt.receipt_id,
    )?;
    let stored_receipt = map_get(&roots.evolution.receipts, &receipt_key, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_evolution_receipt_missing".to_owned(),
            message: "Evolution transition did not retain its semantic receipt".to_owned(),
        })?
        .decode::<cymule_profile_protocol::evolution::EvolutionPersistenceReceipt>(
            StateRootLeafKind::EvolutionPersistenceReceipt,
        )?;
    if &stored_current != current || &stored_alias != alias || &stored_receipt != receipt {
        return Err(DurableError::Integrity {
            code: "state_root_evolution_authority_value_mismatch".to_owned(),
            message: "Evolution transition roots changed a current, alias, or receipt value"
                .to_owned(),
        });
    }
    for mutation in mutations {
        let (family, storage_key) = mutation.storage_key()?;
        let stored = map_get(roots.evolution.state(family), &storage_key, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_evolution_mutation_missing".to_owned(),
                message: format!(
                    "Evolution transition did not retain {family:?} key {storage_key}"
                ),
            })?
            .decode::<cymule_profile_protocol::evolution::EvolutionMutation>(
                StateRootLeafKind::EvolutionMutation,
            )?;
        if &stored != mutation {
            return Err(DurableError::Integrity {
                code: "state_root_evolution_mutation_value_mismatch".to_owned(),
                message: format!(
                    "Evolution normalized leaf {family:?} {storage_key} changed value"
                ),
            });
        }
    }
    Ok(())
}

fn validate_virtual_operation_set<R: StateRootResolver + ?Sized>(
    operations: &[crate::DurableOperation],
    roots: &StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let mut current = None;
    let mut receipt = None;
    let mut mutations = Vec::new();
    for operation in operations {
        match operation {
            crate::DurableOperation::PutVirtualCurrent { value } => {
                if current.replace(value).is_some() {
                    return Err(DurableError::Validation(
                        "one durable transition cannot contain multiple Virtual currents"
                            .to_owned(),
                    ));
                }
            }
            crate::DurableOperation::PutVirtualPersistenceReceipt { value } => {
                if receipt.replace(value).is_some() {
                    return Err(DurableError::Validation(
                        "one durable transition cannot contain multiple Virtual receipts"
                            .to_owned(),
                    ));
                }
            }
            crate::DurableOperation::ApplyVirtualMutation { value } => mutations.push(value),
            _ => {}
        }
    }
    if current.is_none() && receipt.is_none() && mutations.is_empty() {
        return Ok(());
    }
    let (Some(current), Some(receipt)) = (current, receipt) else {
        return Err(DurableError::Validation(
            "a Virtual transition requires one scalar current and semantic receipt".to_owned(),
        ));
    };
    current.verify()?;
    receipt.verify()?;
    for mutation in &mutations {
        mutation.verify()?;
    }
    if current.body.scheduler_id != receipt.command.scheduler_id()
        || current.last_receipt_id != receipt.receipt_id
        || current.body.body_id != receipt.result_body_id
        || receipt.mutations.operations.len() != mutations.len()
        || receipt
            .mutations
            .operations
            .iter()
            .zip(&mutations)
            .any(|(expected, actual)| expected != *actual)
    {
        return Err(DurableError::Integrity {
            code: "state_root_virtual_postcondition_mismatch".to_owned(),
            message:
                "Virtual current, receipt, and normalized writes are not one exact postcondition"
                    .to_owned(),
        });
    }
    let counts = current.body.counts;
    if counts.regions != roots.virtual_work.regions.entries
        || counts.active_regions != roots.virtual_work.active_regions.entries
        || counts.parked != roots.virtual_work.parked.entries
        || counts.hot_work != roots.virtual_work.work.entries
        || counts.hot_occurrences != roots.virtual_work.occurrences.entries
        || counts.runs != roots.virtual_work.runs.entries
        || counts.migrations != roots.virtual_work.migrations.entries
        || counts.certificates != roots.virtual_work.certificates.entries
    {
        return Err(DurableError::Integrity {
            code: "state_root_virtual_family_count_mismatch".to_owned(),
            message: "Virtual current counts do not match the exact normalized map roots"
                .to_owned(),
        });
    }
    let physical_roots = virtual_semantic_roots(&roots.virtual_work)?;
    if current.body.roots != physical_roots {
        return Err(DurableError::Integrity {
            code: "state_root_virtual_family_root_mismatch".to_owned(),
            message: "Virtual current roots do not match the exact normalized physical maps"
                .to_owned(),
        });
    }

    validate_virtual_retained_authority(roots, current, receipt, mutations, overlay)
}

fn validate_virtual_retained_authority<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    current: &cymule_profile_protocol::virtual_work::VirtualCurrent,
    receipt: &cymule_profile_protocol::virtual_work::VirtualPersistenceReceipt,
    mutations: Vec<&cymule_profile_protocol::virtual_work::VirtualStateMutation>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let current_key =
        cymule_profile_protocol::virtual_work::virtual_current_key(&current.body.scheduler_id)?;
    let stored_current = map_get(&roots.virtual_work.currents, &current_key, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_virtual_current_missing".to_owned(),
            message: "Virtual transition did not retain its scalar current".to_owned(),
        })?
        .decode::<cymule_profile_protocol::virtual_work::VirtualCurrent>(
            StateRootLeafKind::VirtualCurrent,
        )?;
    let receipt_key = cymule_profile_protocol::virtual_work::virtual_receipt_key(
        receipt.command.scheduler_id(),
        receipt.command.command_id(),
    )?;
    let stored_receipt = map_get(&roots.virtual_work.receipts, &receipt_key, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_virtual_receipt_missing".to_owned(),
            message: "Virtual transition did not retain its semantic receipt".to_owned(),
        })?
        .decode::<cymule_profile_protocol::virtual_work::VirtualPersistenceReceipt>(
            StateRootLeafKind::VirtualPersistenceReceipt,
        )?;
    if &stored_current != current || &stored_receipt != receipt {
        return Err(DurableError::Integrity {
            code: "state_root_virtual_authority_value_mismatch".to_owned(),
            message: "Virtual transition roots changed a current or receipt value".to_owned(),
        });
    }
    for mutation in mutations {
        let family = mutation.family();
        let storage_key = mutation.storage_key()?;
        let stored = map_get(roots.virtual_work.state(family), &storage_key, overlay)?
            .map(|value| value.decode(StateRootLeafKind::VirtualStateLeaf))
            .transpose()?;
        if stored != mutation.after_leaf() {
            return Err(DurableError::Integrity {
                code: "state_root_virtual_mutation_result_mismatch".to_owned(),
                message: format!(
                    "Virtual normalized leaf {family:?} {storage_key} differs from its exact after value"
                ),
            });
        }
    }
    Ok(())
}

fn virtual_semantic_roots(
    roots: &VirtualRootSet,
) -> DurableResult<cymule_profile_protocol::virtual_work::VirtualStateRoots> {
    Ok(cymule_profile_protocol::virtual_work::VirtualStateRoots {
        regions: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::Regions,
            &roots.regions,
        )?,
        active_regions: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::ActiveRegions,
            &roots.active_regions,
        )?,
        parked: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::Parked,
            &roots.parked,
        )?,
        parked_index: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::ParkedIndex,
            &roots.parked_indexes,
        )?,
        work: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::Work,
            &roots.work,
        )?,
        occurrences: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::Occurrences,
            &roots.occurrences,
        )?,
        runs: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::Runs,
            &roots.runs,
        )?,
        migrations: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::Migrations,
            &roots.migrations,
        )?,
        certificates: virtual_state_map_root_id(
            cymule_profile_protocol::virtual_work::VirtualStateFamily::Certificates,
            &roots.certificates,
        )?,
    })
}

fn virtual_state_map_root_id(
    family: cymule_profile_protocol::virtual_work::VirtualStateFamily,
    root: &MapRoot,
) -> DurableResult<String> {
    cymule_profile_protocol::virtual_work::virtual_state_root_id(
        family,
        root.node.as_deref(),
        root.entries,
    )
    .map_err(Into::into)
}

fn compaction_integrity(code: &str, message: impl Into<String>) -> DurableError {
    DurableError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

pub(crate) fn load_history_compaction_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    compaction_id: &str,
) -> DurableResult<Option<crate::HistoryCompactionReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let receipt = load_history_compaction_leaf(&manifest.roots, compaction_id, &mut overlay)?;
    if let Some(receipt) = &receipt {
        verify_history_compaction_parent(&manifest.roots, receipt, &mut overlay)?;
    }
    Ok(receipt)
}

pub(crate) fn load_parent_history_compaction_receipt<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<Option<crate::HistoryCompactionReceipt>> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    load_parent_compaction_from_overlay(manifest, &mut ObjectOverlay::new(resolver))
}

fn load_history_compaction_leaf<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    compaction_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<crate::HistoryCompactionReceipt>> {
    let Some(value) = map_get(&roots.history_compactions, compaction_id, overlay)? else {
        return Ok(None);
    };
    let receipt: crate::HistoryCompactionReceipt =
        value.decode(StateRootLeafKind::HistoryCompaction)?;
    receipt.verify()?;
    if receipt.compaction_id != compaction_id {
        return Err(compaction_integrity(
            "state_root_history_compaction_key_mismatch",
            "Machine compaction receipt changed its exact key",
        ));
    }
    Ok(Some(receipt))
}

fn verify_history_compaction_parent<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    receipt: &crate::HistoryCompactionReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let parent = if let Some(parent_id) = &receipt.parent_compaction {
        if parent_id == &receipt.compaction_id {
            return Err(compaction_integrity(
                "state_root_history_compaction_parent_cycle",
                "Machine compaction receipt names itself as parent",
            ));
        }
        Some(
            load_history_compaction_leaf(roots, parent_id, overlay)?.ok_or_else(|| {
                compaction_integrity(
                    "state_root_history_compaction_parent_missing",
                    "Machine compaction receipt lost its exact parent",
                )
            })?,
        )
    } else {
        None
    };
    let parent_header = parent.as_ref().map(|parent| &parent.result.archive_segment);
    let empty_index = cymule_core::MachineCommandIndexProof::empty_root()?;
    let header = &receipt.result.archive_segment;
    if header.parent_segment.as_deref() != parent_header.map(|parent| parent.segment_id.as_str())
        || header.parent_count != parent_header.map_or(0, |parent| parent.result_count)
        || header.parent_event_count != parent_header.map_or(0, |parent| parent.result_event_count)
        || header.parent_admission_head.as_deref()
            != parent_header.and_then(|parent| parent.result_admission_head.as_deref())
        || header.parent_command_index_root
            != parent_header.map_or(empty_index.as_str(), |parent| {
                parent.result_command_index_root.as_str()
            })
    {
        return Err(compaction_integrity(
            "state_root_history_compaction_parent_mismatch",
            "Machine compaction receipt does not extend its exact parent archive",
        ));
    }
    Ok(())
}

fn verify_compaction_summary_anchor(
    summary: &crate::MachineCompactionSummary,
    anchor: &cymule_core::MachineBaseAnchor,
) -> DurableResult<()> {
    anchor.verify()?;
    let header = &summary.archive_segment;
    if summary.base_id != anchor.base_id
        || summary.projection_digest != anchor.projection_digest
        || summary.compacted_events != anchor.archive_event_count
        || header.segment_id != anchor.archive_head
        || header.result_count != anchor.archive_count
        || header.result_event_count != anchor.archive_event_count
        || header.result_admission_head != anchor.admission_head
        || header.result_command_index_root != anchor.command_index_root
        || header.batch_count > anchor.archive_batch_count
    {
        return Err(compaction_integrity(
            "state_root_history_compaction_anchor_mismatch",
            "Machine compaction receipt does not match the exact base anchor",
        ));
    }
    Ok(())
}

fn load_parent_compaction_from_overlay<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<crate::HistoryCompactionReceipt>> {
    let Some(head) = &manifest.roots.history_compaction_head else {
        return Ok(None);
    };
    let value = overlay.load_value(head)?;
    let receipt: crate::HistoryCompactionReceipt =
        value.decode(StateRootLeafKind::HistoryCompaction)?;
    receipt.verify()?;
    let primary = map_get(
        &manifest.roots.history_compactions,
        &receipt.compaction_id,
        overlay,
    )?;
    if primary.as_ref() != Some(&value) {
        return Err(compaction_integrity(
            "state_root_history_compaction_head_mismatch",
            "Machine compaction head is not its exact primary receipt value",
        ));
    }
    verify_history_compaction_parent(&manifest.roots, &receipt, overlay)?;
    let anchor = manifest.machine_base_anchor.as_ref().ok_or_else(|| {
        compaction_integrity(
            "state_root_history_compaction_anchor_missing",
            "Machine compaction head has no exact base anchor",
        )
    })?;
    verify_compaction_summary_anchor(&receipt.result, anchor)?;
    if receipt.result.retained_events > manifest.roots.machine_events.len {
        return Err(compaction_integrity(
            "state_root_history_compaction_suffix_mismatch",
            "Machine compaction head exceeds the retained hot Event suffix",
        ));
    }
    Ok(Some(receipt))
}

fn audit_history_compaction_receipts<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<()> {
    let mut overlay = ObjectOverlay::new(resolver);
    for (key, value) in materialize_map(&manifest.roots.history_compactions, &mut overlay)? {
        let receipt: crate::HistoryCompactionReceipt =
            value.decode(StateRootLeafKind::HistoryCompaction)?;
        receipt.verify()?;
        if receipt.compaction_id != key {
            return Err(compaction_integrity(
                "state_root_history_compaction_key_mismatch",
                "Machine compaction receipt changed its exact key",
            ));
        }
        verify_history_compaction_parent(&manifest.roots, &receipt, &mut overlay)?;
    }
    let _ = load_parent_compaction_from_overlay(manifest, &mut overlay)?;
    Ok(())
}

fn materialize_durable_state_root<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<crate::DurableState> {
    let materialized = materialize_full_semantic_audit_roots(manifest, resolver)?;
    let machine = materialize_machine_snapshot(manifest, &materialized, resolver)?;
    let state = materialize_durable_projection(manifest, &materialized, machine, resolver)?;
    state.validate_anchored(manifest.machine_base_anchor.as_ref())?;
    validate_active_history_closure(manifest, resolver, &state)?;
    let _ = load_parent_history_compaction_receipt(manifest, resolver)?;
    Ok(state)
}

/// Verify the exact maintenance source before any Core or receipt traversal.
pub(super) fn ensure_machine_compaction_source<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &R,
) -> DurableResult<()> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    if manifest.machine_frontier.pending_commands.entries != 0
        || manifest.machine_frontier.paged_transitions.entries != 0
    {
        return Err(DurableError::HistoryConflict {
            code: "state_root_machine_compaction_pending_transition".to_owned(),
            message: "Machine history compaction requires empty pending command and paged transition roots"
                .to_owned(),
        });
    }
    Ok(())
}

/// Load only the authenticated Core source of an explicit offline compaction.
///
/// This maintenance boundary retains the complete Core base and hot history;
/// ordinary open and transitions must not call it. Core consumes these parts
/// and performs the one full semantic audit while preparing the compaction.
pub(super) fn load_machine_compaction_source<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<cymule_core::MachineRootParts> {
    ensure_machine_compaction_source(manifest, resolver)?;
    audit_pinned_machine_frontier(manifest, resolver)?;
    let materialized = materialize_state_root_families(
        manifest,
        resolver,
        &[
            StateRootFamily::MachinePlans,
            StateRootFamily::MachineArtifacts,
            StateRootFamily::MachineCommands,
            StateRootFamily::MachineCommandBatches,
        ],
    )?;
    materialize_machine_root_parts(manifest, &materialized, resolver)
}

fn materialize_machine_snapshot<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    materialized: &MaterializedStateRoots,
    resolver: &mut R,
) -> DurableResult<cymule_core::MachineSnapshot> {
    let parts = materialize_machine_root_parts(manifest, materialized, resolver)?;
    let machine = cymule_core::MachineSnapshot::from_root_parts(parts)?;
    if machine_authority_root(&machine, manifest.machine_base_anchor.as_ref())?
        != manifest.machine_authority_root()
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_authority_mismatch".to_owned(),
            message: "materialized Machine roots do not match their exact authority root"
                .to_owned(),
        });
    }
    Ok(machine)
}

fn materialize_machine_root_parts<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    materialized: &MaterializedStateRoots,
    resolver: &mut R,
) -> DurableResult<cymule_core::MachineRootParts> {
    let collections = &materialized.collections;
    let base = match &manifest.roots.machine_base {
        Some(object_id) => Some(materialize_machine_base(object_id, &mut *resolver)?),
        None => None,
    };
    let events = materialize_state_log(&manifest.roots.machine_events, resolver)?;
    let admissions = materialize_state_log(&manifest.roots.machine_admissions, resolver)?;
    let plan_admissions = decode_value_vec::<cymule_core::SealedPlan>(
        materialize_state_log(&manifest.roots.machine_plan_admissions, resolver)?,
        StateRootLeafKind::MachinePlan,
    )?;
    let artifact_admissions = decode_value_vec::<cymule_core::ArtifactRecord>(
        materialize_state_log(&manifest.roots.machine_artifact_admissions, resolver)?,
        StateRootLeafKind::MachineArtifact,
    )?;
    let plans = decode_family_map(
        collections,
        StateRootFamily::MachinePlans,
        StateRootLeafKind::MachinePlan,
    )?;
    let artifacts = decode_family_map(
        collections,
        StateRootFamily::MachineArtifacts,
        StateRootLeafKind::MachineArtifact,
    )?;
    if plan_admissions
        .iter()
        .any(|plan| plans.get(&plan.plan_id) != Some(plan))
        || artifact_admissions
            .iter()
            .any(|artifact| artifacts.get(&artifact.reference.artifact_id) != Some(artifact))
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_admission_order_value_mismatch".to_owned(),
            message:
                "Machine Plan or Artifact admission order disagrees with its exact keyed value"
                    .to_owned(),
        });
    }
    let MaterializedHotCommands {
        commands,
        command_index_proofs,
    } = materialize_hot_commands(materialized, &admissions)?;
    let MaterializedHotBatches {
        batches,
        batch_order,
    } = materialize_hot_batches(manifest, materialized, &commands, resolver)?;
    let parts = cymule_core::MachineRootParts {
        root_parts_version: cymule_core::MachineRootParts::VERSION.to_owned(),
        snapshot_version: manifest.machine_snapshot_version.clone(),
        plans,
        plan_admission_order: plan_admissions
            .into_iter()
            .map(|plan| plan.plan_id)
            .collect(),
        artifacts,
        artifact_admission_order: artifact_admissions
            .into_iter()
            .map(|artifact| artifact.reference.artifact_id)
            .collect(),
        batches,
        batch_admission_order: batch_order
            .into_iter()
            .map(|batch| batch.batch_id)
            .collect(),
        base,
        base_anchor: manifest.machine_base_anchor.clone(),
        events: decode_value_vec(events, StateRootLeafKind::MachineEvent)?,
        admissions: decode_value_vec(admissions, StateRootLeafKind::MachineAdmission)?,
        commands,
        command_index_proofs,
    };
    Ok(parts)
}

struct MaterializedHotCommands {
    commands: BTreeMap<String, cymule_core::ArchivedCommandRecord>,
    command_index_proofs: BTreeMap<String, cymule_core::MachineCommandIndexProof>,
}

fn materialize_hot_commands(
    materialized: &MaterializedStateRoots,
    admissions: &[StateRootValue],
) -> DurableResult<MaterializedHotCommands> {
    let collections = &materialized.collections;
    let command_values = collections
        .get(&StateRootFamily::MachineCommands)
        .expect("fixed StateRoot family was materialized");
    let mut commands = BTreeMap::new();
    let mut command_index_proofs = BTreeMap::new();
    let admission_by_command = decode_value_vec::<cymule_core::CommandAdmission>(
        admissions.to_vec(),
        StateRootLeafKind::MachineAdmission,
    )?
    .into_iter()
    .map(|admission| (admission.command_id.clone(), admission))
    .collect::<BTreeMap<_, _>>();
    for (command_id, value) in command_values {
        let StateRootValue::MachineCommandCurrent {
            record,
            admission,
            index_proof,
            ..
        } = value
        else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_value_kind_mismatch".to_owned(),
                message: format!(
                    "Machine hot command {command_id} is not a composite authority leaf"
                ),
            });
        };
        if record.envelope.command_id != *command_id
            || admission_by_command.get(command_id) != Some(admission.as_ref())
            || index_proof.command_id != *command_id
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_composite_mismatch".to_owned(),
                message: format!(
                    "Machine hot command {command_id} does not match its admission log"
                ),
            });
        }
        commands.insert(command_id.clone(), record.as_ref().clone());
        command_index_proofs.insert(command_id.clone(), index_proof.as_ref().clone());
    }
    if admission_by_command.len() != commands.len()
        || admission_by_command
            .keys()
            .any(|command_id| !commands.contains_key(command_id))
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_admission_closure_mismatch".to_owned(),
            message: "Machine hot command composites and admission log are not closed".to_owned(),
        });
    }
    Ok(MaterializedHotCommands {
        commands,
        command_index_proofs,
    })
}

struct MaterializedHotBatches {
    batches: BTreeMap<String, cymule_core::durable_internal::MachineCommandBatchRecord>,
    batch_order: Vec<cymule_core::durable_internal::MachineCommandBatchRecord>,
}

fn materialize_hot_batches<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    materialized: &MaterializedStateRoots,
    commands: &BTreeMap<String, cymule_core::ArchivedCommandRecord>,
    resolver: &mut R,
) -> DurableResult<MaterializedHotBatches> {
    let collections = &materialized.collections;
    let batches: BTreeMap<String, cymule_core::durable_internal::MachineCommandBatchRecord> =
        decode_family_map(
            collections,
            StateRootFamily::MachineCommandBatches,
            StateRootLeafKind::MachineCommandBatch,
        )?;
    let batch_order = decode_value_vec::<cymule_core::durable_internal::MachineCommandBatchRecord>(
        materialize_state_log(&manifest.roots.machine_command_batch_admissions, resolver)?,
        StateRootLeafKind::MachineCommandBatch,
    )?;
    if batch_order.len() != batches.len()
        || batch_order
            .iter()
            .any(|batch| batches.get(&batch.batch_id) != Some(batch))
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_batch_order_mismatch".to_owned(),
            message: "Machine command-batch map and admission order disagree".to_owned(),
        });
    }
    for (command_id, record) in commands {
        let batch = batches
            .get(&record.batch_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_command_batch_missing".to_owned(),
                message: format!(
                    "Machine hot command {command_id} references missing batch {}",
                    record.batch_id
                ),
            })?;
        let position = usize::try_from(record.batch_position)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let member = batch
            .members
            .get(position)
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_command_batch_position_missing".to_owned(),
                message: format!("Machine hot command {command_id} has no batch position"),
            })?;
        if usize::try_from(record.batch_len).ok() != Some(batch.members.len())
            || member.command_id != *command_id
            || member.semantic_hash != record.semantic_hash
            || batch.receipts.get(position) != Some(&record.receipt)
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_batch_membership_mismatch".to_owned(),
                message: format!(
                    "Machine hot command {command_id} changed its exact batch membership"
                ),
            });
        }
    }
    Ok(MaterializedHotBatches {
        batches,
        batch_order,
    })
}

fn materialize_run_outboxes<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
) -> DurableResult<BTreeMap<String, crate::EffectDispatch>> {
    let mut overlay = ObjectOverlay::new(resolver);
    let mut outboxes = BTreeMap::new();
    for (run_id, descriptor) in materialize_map(&manifest.roots.run_query_indexes, &mut overlay)? {
        let roots = descriptor.decode_run_query_indexes(&run_id)?;
        for (intent_id, value) in materialize_map(&roots.effects, &mut overlay)? {
            let dispatch: crate::EffectDispatch = value.decode(StateRootLeafKind::Outbox)?;
            dispatch.verify_wire()?;
            if dispatch.intent_id != intent_id
                || dispatch.run_id != run_id
                || outboxes.insert(intent_id, dispatch).is_some()
            {
                return Err(DurableError::Integrity {
                    code: "state_root_outbox_owner_mismatch".to_owned(),
                    message: "materialized Run-local outbox changed owner or repeated an intent"
                        .to_owned(),
                });
            }
        }
    }
    Ok(outboxes)
}

fn materialize_durable_projection<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    materialized: &MaterializedStateRoots,
    machine: cymule_core::MachineSnapshot,
    resolver: &mut R,
) -> DurableResult<crate::DurableState> {
    let collections = &materialized.collections;
    let state = crate::DurableState {
        durable_version: manifest.durable_version.clone(),
        machine,
        continuations: decode_family_map(
            collections,
            StateRootFamily::Continuations,
            StateRootLeafKind::Continuation,
        )?,
        waits: decode_family_map(collections, StateRootFamily::Waits, StateRootLeafKind::Wait)?,
        wait_activations: decode_family_map(
            collections,
            StateRootFamily::WaitActivations,
            StateRootLeafKind::WaitActivation,
        )?,
        cancellation_receipts: decode_family_map(
            collections,
            StateRootFamily::CancellationReceipts,
            StateRootLeafKind::CancellationReceipt,
        )?,
        effect_resolution_receipts: decode_family_map(
            collections,
            StateRootFamily::EffectResolutionReceipts,
            StateRootLeafKind::EffectResolutionReceipt,
        )?,
        leases: decode_family_map(
            collections,
            StateRootFamily::Leases,
            StateRootLeafKind::Lease,
        )?,
        outbox: materialize_run_outboxes(manifest, resolver)?,
        component_occurrences: decode_family_map(
            collections,
            StateRootFamily::ComponentOccurrences,
            StateRootLeafKind::ComponentOccurrence,
        )?,
        operation_attempts: decode_family_map(
            collections,
            StateRootFamily::OperationAttempts,
            StateRootLeafKind::OperationAttempt,
        )?,
        clock_observations: decode_family_map(
            collections,
            StateRootFamily::ClockObservations,
            StateRootLeafKind::ClockObservation,
        )?,
        snapshots: decode_family_map(
            collections,
            StateRootFamily::Snapshots,
            StateRootLeafKind::Snapshot,
        )?,
        history_compactions: decode_family_map(
            collections,
            StateRootFamily::HistoryCompactions,
            StateRootLeafKind::HistoryCompaction,
        )?,
        application_journals: materialize_application_journals(collections, resolver)?,
        application_journal_prefix_replacements: decode_family_map(
            collections,
            StateRootFamily::ApplicationJournalPrefixReplacements,
            StateRootLeafKind::JournalPrefixReplacement,
        )?,
    };
    Ok(state)
}

fn build_machine_base<R: StateRootResolver + ?Sized>(
    base: &cymule_core::MachineBaseSnapshot,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<String> {
    base.verify()?;
    let canonical = cymule_core::canonical_bytes(base)?;
    build_machine_base_bytes(&canonical, overlay)
}

fn build_machine_base_bytes<R: StateRootResolver + ?Sized>(
    canonical: &[u8],
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<String> {
    if canonical.is_empty() {
        return Err(DurableError::Validation(
            "Machine-base canonical bytes are empty".to_owned(),
        ));
    }
    let canonical_len = u64::try_from(canonical.len())
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let chunks = canonical
        .chunks(MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES)
        .enumerate()
        .map(|(index, bytes)| {
            Ok(StateRootValue::MachineBaseChunk {
                index: u64::try_from(index)
                    .map_err(|error| DurableError::Validation(error.to_string()))?,
                bytes: bytes.to_vec(),
            })
        })
        .collect::<DurableResult<Vec<_>>>()?;
    let chunk_count =
        u64::try_from(chunks.len()).map_err(|error| DurableError::Validation(error.to_string()))?;
    let chunks = log_append(&LogRoot::empty(), &chunks, overlay)?;
    overlay.insert_value(StateRootValue::MachineBaseDescriptor {
        canonical_len,
        canonical_digest: cymule_core::sha256_bytes(canonical),
        chunk_count,
        chunks,
    })
}

fn materialize_machine_base<R: StateRootResolver + ?Sized>(
    object_id: &str,
    resolver: &mut R,
) -> DurableResult<cymule_core::MachineBaseSnapshot> {
    let canonical = materialize_machine_base_bytes(object_id, resolver)?;
    let base = cymule_core::decode_json::<cymule_core::MachineBaseSnapshot>(&canonical)?;
    if cymule_core::canonical_bytes(&base)? != canonical {
        return Err(DurableError::Integrity {
            code: "state_root_machine_base_canonical_mismatch".to_owned(),
            message: "Machine base changes under its typed canonical round trip".to_owned(),
        });
    }
    base.verify()?;
    Ok(base)
}

fn materialize_machine_base_bytes<R: StateRootResolver + ?Sized>(
    object_id: &str,
    resolver: &mut R,
) -> DurableResult<Vec<u8>> {
    let descriptor = {
        let mut overlay = ObjectOverlay::new(&mut *resolver);
        overlay.load_value(object_id)?
    };
    let StateRootValue::MachineBaseDescriptor {
        canonical_len,
        canonical_digest,
        chunk_count,
        chunks,
    } = descriptor
    else {
        return Err(DurableError::Integrity {
            code: "state_root_machine_base_descriptor_kind".to_owned(),
            message: "Machine base root does not resolve to its closed descriptor".to_owned(),
        });
    };
    if chunk_count != chunks.len {
        return Err(DurableError::Integrity {
            code: "state_root_machine_base_chunk_count".to_owned(),
            message: "Machine base descriptor chunk count does not match its log".to_owned(),
        });
    }
    let values = materialize_state_log(&chunks, resolver)?;
    let expected_chunks = usize::try_from(chunk_count)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    // Grow only as authenticated chunk bytes arrive. The descriptor length is
    // content-bound but must not be used as an up-front allocation request.
    let mut canonical = Vec::new();
    let mut materialized_len = 0_u64;
    for (expected_index, value) in values.into_iter().enumerate() {
        let StateRootValue::MachineBaseChunk { index, bytes } = value else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_base_chunk_kind".to_owned(),
                message: "Machine base chunk log contains another value kind".to_owned(),
            });
        };
        if index
            != u64::try_from(expected_index)
                .map_err(|error| DurableError::Validation(error.to_string()))?
            || (expected_index + 1 < expected_chunks
                && bytes.len() != MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES)
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_base_chunk_order".to_owned(),
                message: "Machine base chunks are missing, misordered, or short".to_owned(),
            });
        }
        materialized_len = materialized_len
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|error| DurableError::Validation(error.to_string()))?,
            )
            .filter(|len| *len <= canonical_len)
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_base_length_overflow".to_owned(),
                message: "Machine base chunks exceed the descriptor length".to_owned(),
            })?;
        canonical.extend_from_slice(&bytes);
    }
    if materialized_len != canonical_len
        || cymule_core::sha256_bytes(&canonical) != canonical_digest
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_base_bytes_mismatch".to_owned(),
            message: "Machine base chunks do not match descriptor length or digest".to_owned(),
        });
    }
    Ok(canonical)
}

fn validate_active_history_closure<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    state: &crate::DurableState,
) -> DurableResult<()> {
    for (journal_id, records) in &state.application_journals {
        for record in records {
            let expected = crate::JournalRecordManifest::from_record(record)?;
            if load_application_journal_record_manifest(
                manifest,
                &mut *resolver,
                journal_id,
                &record.record_id,
            )?
            .as_ref()
                != Some(&expected)
            {
                return Err(DurableError::Integrity {
                    code: "state_root_journal_record_manifest_missing".to_owned(),
                    message: format!(
                        "application journal {journal_id} record {} lacks its exact all-ever manifest",
                        record.record_id
                    ),
                });
            }
        }
    }
    for receipt in state.application_journal_prefix_replacements.values() {
        let replacement_id = &receipt.replacement.replacement_id;
        let authority = load_application_journal_prefix_replacement_authority(
            manifest,
            &mut *resolver,
            replacement_id,
        )?;
        let matches = match authority.as_ref() {
            Some(authority) => authority.matches(receipt)?,
            None => false,
        };
        if !matches {
            return Err(DurableError::Integrity {
                code: "state_root_journal_replacement_authority_missing".to_owned(),
                message: format!(
                    "latest application journal replacement {replacement_id} lacks its exact all-ever authority"
                ),
            });
        }
    }
    Ok(())
}

struct EmptyStateRootResolver;

impl StateRootResolver for EmptyStateRootResolver {
    fn pinned_manifest_id(&self) -> &'static str {
        ""
    }

    fn load_state_root_object(
        &mut self,
        _object_id: &str,
    ) -> DurableResult<Option<StateRootObject>> {
        Ok(None)
    }
}

pub(crate) fn durable_run_current(
    projection: &cymule_core::Projection,
    continuation: &crate::Continuation,
) -> DurableResult<crate::DurableRunCurrent> {
    continuation.verify_wire()?;
    let run = projection
        .runs
        .get(&continuation.run_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "run_query_current_projection_missing".to_owned(),
            message: format!(
                "Continuation {} has no exact Machine Run projection",
                continuation.run_id
            ),
        })?;
    if run.run_id != continuation.run_id
        || run.current_plan != continuation.plan_id
        || run.current_binding_context != continuation.binding_context
        || run.epoch != continuation.epoch
    {
        return Err(DurableError::Integrity {
            code: "run_query_current_projection_mismatch".to_owned(),
            message: format!(
                "Continuation {} and its Machine Run projection disagree",
                continuation.run_id
            ),
        });
    }
    let current = crate::DurableRunCurrent {
        run_id: continuation.run_id.clone(),
        plan_id: continuation.plan_id.clone(),
        execution_binding: cymule_core::ArtifactRef {
            identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
            artifact_id: continuation.binding_context.clone(),
            kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
        },
        continuation_status: continuation.status,
        epoch: continuation.epoch,
        execution_fence: continuation.execution_fence,
        result: run.result.clone(),
        execution_status: run.execution_status.clone(),
        world_settlement: run.world_settlement,
    };
    current.verify()?;
    Ok(current)
}

fn build_roots_from_state<R: StateRootResolver + ?Sized>(
    state: &crate::DurableState,
    parts: cymule_core::MachineRootParts,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<StateRoots> {
    let queries = build_genesis_run_queries(state, overlay)?;
    let machine_roots = build_genesis_machine_roots(parts, overlay)?;
    let journals = build_genesis_journal_values(state, overlay)?;
    build_genesis_sidecar_roots(state, queries, journals, machine_roots, overlay)
}

struct GenesisRunQueries {
    currents: BTreeMap<String, crate::DurableRunCurrent>,
    indexes: Vec<(String, StateRootValue)>,
}

fn build_genesis_run_queries<R: StateRootResolver + ?Sized>(
    state: &crate::DurableState,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<GenesisRunQueries> {
    let restored = match state.machine.base_anchor.as_ref() {
        Some(anchor) => cymule_core::Machine::restore_anchored(state.machine.clone(), anchor)?,
        None => cymule_core::Machine::restore(state.machine.clone())?,
    };
    let run_currents = state
        .continuations
        .iter()
        .map(|(run_id, continuation)| {
            durable_run_current(restored.projection(), continuation)
                .map(|current| (run_id.clone(), current))
        })
        .collect::<DurableResult<BTreeMap<_, _>>>()?;
    let mut run_query_indexes = Vec::with_capacity(run_currents.len());
    for run_id in run_currents.keys() {
        run_query_indexes.push((
            run_id.clone(),
            build_genesis_run_query_indexes(run_id, state, overlay)?,
        ));
    }
    Ok(GenesisRunQueries {
        currents: run_currents,
        indexes: run_query_indexes,
    })
}

fn build_genesis_run_query_indexes<R: StateRootResolver + ?Sized>(
    run_id: &str,
    state: &crate::DurableState,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<StateRootValue> {
    let waits = state
        .waits
        .iter()
        .filter(|(_, value)| value.run_id == run_id)
        .map(|(key, value)| {
            let summary = crate::DurableWaitSummary::from_wait(value);
            summary.verify()?;
            Ok((key.clone(), summary))
        })
        .collect::<DurableResult<BTreeMap<_, _>>>()?;
    let effects = state
        .outbox
        .iter()
        .filter(|(_, value)| value.run_id == run_id)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let occurrences = state
        .component_occurrences
        .iter()
        .filter(|(_, value)| value.run_id == run_id)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let attempts = state
        .operation_attempts
        .iter()
        .filter(|(_, value)| value.run_id == run_id)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let pending_waits = state
        .waits
        .iter()
        .filter(|(_, value)| value.run_id == run_id && value.state == crate::WaitState::Pending)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let active_effects = state
        .outbox
        .iter()
        .filter(|(_, value)| {
            value.run_id == run_id
                && matches!(
                    value.state,
                    crate::OutboxState::Pending
                        | crate::OutboxState::Claimed
                        | crate::OutboxState::Unknown
                )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let active_leases = genesis_active_leases(state, &active_effects)?;
    StateRootValue::run_query_indexes(
        run_id,
        RunQueryIndexRoots {
            waits: build_typed_map(StateRootLeafKind::WaitSummary, waits, overlay)?,
            effects: build_typed_map(StateRootLeafKind::Outbox, effects, overlay)?,
            occurrences: build_typed_map(
                StateRootLeafKind::ComponentOccurrence,
                occurrences,
                overlay,
            )?,
            attempts: build_typed_map(StateRootLeafKind::OperationAttempt, attempts, overlay)?,
            pending_waits: build_typed_map(StateRootLeafKind::Wait, pending_waits, overlay)?,
            active_effects: build_typed_map(StateRootLeafKind::Outbox, active_effects, overlay)?,
            active_leases: build_typed_map(StateRootLeafKind::Lease, active_leases, overlay)?,
            terminal: None,
        },
    )
}

fn genesis_active_leases(
    state: &crate::DurableState,
    active_effects: &BTreeMap<String, crate::EffectDispatch>,
) -> DurableResult<BTreeMap<String, crate::CoordinationLease>> {
    let active_leases = active_effects
        .iter()
        .filter(|(_, value)| value.state == crate::OutboxState::Claimed)
        .map(|(intent_id, _)| {
            state
                .leases
                .get(intent_id)
                .cloned()
                .map(|lease| (intent_id.clone(), lease))
                .ok_or_else(|| DurableError::Integrity {
                    code: "state_root_claimed_effect_lease_missing".to_owned(),
                    message: format!("claimed Effect {intent_id} has no exact coordination lease"),
                })
        })
        .collect::<DurableResult<BTreeMap<_, _>>>()?;
    Ok(active_leases)
}

fn build_genesis_machine_roots<R: StateRootResolver + ?Sized>(
    parts: cymule_core::MachineRootParts,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<StateRoots> {
    let plan_admissions = parts
        .plan_admission_order
        .iter()
        .map(|plan_id| {
            parts
                .plans
                .get(plan_id)
                .cloned()
                .ok_or_else(|| DurableError::Integrity {
                    code: "machine_plan_admission_missing_value".to_owned(),
                    message: format!(
                        "Machine Plan admission order references missing Plan {plan_id}"
                    ),
                })
        })
        .collect::<DurableResult<Vec<_>>>()?;
    let artifact_admissions =
        parts
            .artifact_admission_order
            .iter()
            .map(|artifact_id| {
                parts.artifacts.get(artifact_id).cloned().ok_or_else(|| {
                DurableError::Integrity {
                    code: "machine_artifact_admission_missing_value".to_owned(),
                    message: format!(
                        "Machine Artifact admission order references missing Artifact {artifact_id}"
                    ),
                }
            })
            })
            .collect::<DurableResult<Vec<_>>>()?;
    let plan_root = build_typed_map(StateRootLeafKind::MachinePlan, parts.plans, overlay)?;
    let plan_admission_root =
        build_typed_log(StateRootLeafKind::MachinePlan, plan_admissions, overlay)?;
    let artifact_root =
        build_typed_map(StateRootLeafKind::MachineArtifact, parts.artifacts, overlay)?;
    let artifact_admission_root = build_typed_log(
        StateRootLeafKind::MachineArtifact,
        artifact_admissions,
        overlay,
    )?;
    let event_root = build_typed_log(StateRootLeafKind::MachineEvent, parts.events, overlay)?;
    let admission_root = build_typed_log(
        StateRootLeafKind::MachineAdmission,
        parts.admissions,
        overlay,
    )?;
    if !parts.commands.is_empty() || !parts.command_index_proofs.is_empty() {
        return Err(DurableError::Validation(
            "StateRoot genesis cannot import hot command authority".to_owned(),
        ));
    }
    let command_root = MapRoot::empty();
    let machine_base = parts
        .base
        .as_ref()
        .map(|base| build_machine_base(base, overlay))
        .transpose()?;
    Ok(StateRoots {
        machine_plans: plan_root,
        machine_plan_admissions: plan_admission_root,
        machine_artifacts: artifact_root,
        machine_artifact_admissions: artifact_admission_root,
        machine_base,
        machine_events: event_root,
        machine_admissions: admission_root,
        machine_commands: command_root,
        machine_command_batches: MapRoot::empty(),
        machine_command_batch_admissions: LogRoot::empty(),
        ..StateRoots::empty()
    })
}

struct GenesisJournalValues {
    journals: Vec<(String, StateRootValue)>,
    record_manifests: Vec<(String, StateRootValue)>,
}

fn build_genesis_journal_values<R: StateRootResolver + ?Sized>(
    state: &crate::DurableState,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<GenesisJournalValues> {
    let application_journals = state
        .application_journals
        .iter()
        .map(|(journal_id, records)| {
            build_typed_log(StateRootLeafKind::JournalRecord, records.to_vec(), overlay).and_then(
                |root| {
                    StateRootValue::application_journal(journal_id, &root)
                        .map(|value| (journal_id.clone(), value))
                },
            )
        })
        .collect::<DurableResult<Vec<_>>>()?;
    let record_manifests = state
        .application_journals
        .iter()
        .map(|(journal_id, records)| {
            let records = records
                .iter()
                .map(|record| {
                    crate::JournalRecordManifest::from_record(record)
                        .map(|manifest| (record.record_id.clone(), manifest))
                })
                .collect::<DurableResult<BTreeMap<_, _>>>()?;
            build_typed_map(StateRootLeafKind::JournalRecordManifest, records, overlay).and_then(
                |root| {
                    StateRootValue::application_journal_record_manifests(journal_id, &root)
                        .map(|value| (journal_id.clone(), value))
                },
            )
        })
        .collect::<DurableResult<Vec<_>>>()?;
    Ok(GenesisJournalValues {
        journals: application_journals,
        record_manifests,
    })
}

fn build_genesis_sidecar_roots<R: StateRootResolver + ?Sized>(
    state: &crate::DurableState,
    queries: GenesisRunQueries,
    journals: GenesisJournalValues,
    machine_roots: StateRoots,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<StateRoots> {
    let history_compaction_head = genesis_history_compaction_head(state)?;
    Ok(StateRoots {
        continuations: build_typed_map(
            StateRootLeafKind::Continuation,
            state.continuations.clone(),
            overlay,
        )?,
        run_currents: build_typed_map(StateRootLeafKind::RunCurrent, queries.currents, overlay)?,
        run_query_indexes: build_value_map(queries.indexes, overlay)?,
        waits: build_typed_map(StateRootLeafKind::Wait, state.waits.clone(), overlay)?,
        wait_activations: build_typed_map(
            StateRootLeafKind::WaitActivation,
            state.wait_activations.clone(),
            overlay,
        )?,
        cancellation_receipts: build_typed_map(
            StateRootLeafKind::CancellationReceipt,
            state.cancellation_receipts.clone(),
            overlay,
        )?,
        effect_resolution_receipts: build_typed_map(
            StateRootLeafKind::EffectResolutionReceipt,
            state.effect_resolution_receipts.clone(),
            overlay,
        )?,
        leases: build_typed_map(StateRootLeafKind::Lease, state.leases.clone(), overlay)?,
        outbox: build_typed_map(
            StateRootLeafKind::OutboxOwner,
            state
                .outbox
                .iter()
                .map(|(intent_id, dispatch)| {
                    (
                        intent_id.clone(),
                        OutboxOwner {
                            intent_id: intent_id.clone(),
                            run_id: dispatch.run_id.clone(),
                        },
                    )
                })
                .collect(),
            overlay,
        )?,
        component_occurrences: build_typed_map(
            StateRootLeafKind::ComponentOccurrence,
            state.component_occurrences.clone(),
            overlay,
        )?,
        operation_attempts: build_typed_map(
            StateRootLeafKind::OperationAttempt,
            state.operation_attempts.clone(),
            overlay,
        )?,
        clock_observations: build_typed_map(
            StateRootLeafKind::ClockObservation,
            state.clock_observations.clone(),
            overlay,
        )?,
        snapshots: build_typed_map(
            StateRootLeafKind::Snapshot,
            state.snapshots.clone(),
            overlay,
        )?,
        history_compactions: build_typed_map(
            StateRootLeafKind::HistoryCompaction,
            state.history_compactions.clone(),
            overlay,
        )?,
        history_compaction_head,
        application_journals: build_value_map(journals.journals, overlay)?,
        application_journal_prefix_replacements: build_typed_map(
            StateRootLeafKind::JournalPrefixReplacement,
            state.application_journal_prefix_replacements.clone(),
            overlay,
        )?,
        application_journal_record_manifests: build_value_map(journals.record_manifests, overlay)?,
        ..machine_roots
    })
}

fn genesis_history_compaction_head(state: &crate::DurableState) -> DurableResult<Option<String>> {
    let Some(anchor) = &state.machine.base_anchor else {
        return Ok(None);
    };
    let mut matches = state.history_compactions.values().filter(|receipt| {
        receipt.result.base_id == anchor.base_id
            && receipt.result.archive_segment.segment_id == anchor.archive_head
    });
    let receipt = matches.next().ok_or_else(|| {
        compaction_integrity(
            "state_root_history_compaction_head_missing",
            "Machine genesis base has no exact current compaction receipt",
        )
    })?;
    if matches.next().is_some() {
        return Err(compaction_integrity(
            "state_root_history_compaction_head_ambiguous",
            "Machine genesis base has multiple current compaction receipts",
        ));
    }
    verify_compaction_summary_anchor(&receipt.result, anchor)?;
    state_root_value_id(&StateRootValue::encode(
        StateRootLeafKind::HistoryCompaction,
        receipt,
    )?)
    .map(Some)
}

fn build_typed_map<T, R>(
    kind: StateRootLeafKind,
    values: BTreeMap<String, T>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MapRoot>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    let values = values
        .into_iter()
        .map(|(key, value)| StateRootValue::encode(kind, &value).map(|value| (key, value)))
        .collect::<DurableResult<Vec<_>>>()?;
    build_value_map(values, overlay)
}

fn build_value_map<R: StateRootResolver + ?Sized>(
    values: Vec<(String, StateRootValue)>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<MapRoot> {
    let mut root = MapRoot::empty();
    for (key, value) in values {
        root = map_put(&root, &key, value, overlay)?;
    }
    Ok(root)
}

fn build_typed_log<T, R>(
    kind: StateRootLeafKind,
    values: Vec<T>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    let values = values
        .into_iter()
        .map(|value| StateRootValue::encode(kind, &value))
        .collect::<DurableResult<Vec<_>>>()?;
    log_append(&LogRoot::empty(), &values, overlay)
}

fn put_typed_value<T, R>(
    root: &mut MapRoot,
    key: &str,
    kind: StateRootLeafKind,
    value: &T,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    let next = StateRootValue::encode(kind, value)?;
    if map_get(root, key, overlay)?.as_ref() != Some(&next) {
        *root = map_put(root, key, next, overlay)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum RunQueryIndexKind {
    Waits,
    Effects,
    Occurrences,
    Attempts,
    PendingWaits,
    ActiveEffects,
    ActiveLeases,
}

fn ensure_run_query_indexes<R: StateRootResolver + ?Sized>(
    index_root: &mut MapRoot,
    run_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    cymule_core::validate_identity("Run query-index owner", run_id)?;
    if map_get(index_root, run_id, overlay)?.is_none() {
        *index_root = map_put(
            index_root,
            run_id,
            StateRootValue::run_query_indexes(run_id, RunQueryIndexRoots::default())?,
            overlay,
        )?;
    }
    Ok(())
}

fn put_run_query_item<T, R>(
    index_root: &mut MapRoot,
    run_id: &str,
    index: RunQueryIndexKind,
    key: &str,
    kind: StateRootLeafKind,
    value: &T,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    cymule_core::validate_identity("Run query-index owner", run_id)?;
    let mut roots = match map_get(index_root, run_id, overlay)? {
        Some(value) => value.decode_run_query_indexes(run_id)?,
        None => RunQueryIndexRoots::default(),
    };
    let selected = match index {
        RunQueryIndexKind::Waits => &mut roots.waits,
        RunQueryIndexKind::Effects => &mut roots.effects,
        RunQueryIndexKind::Occurrences => &mut roots.occurrences,
        RunQueryIndexKind::Attempts => &mut roots.attempts,
        RunQueryIndexKind::PendingWaits => &mut roots.pending_waits,
        RunQueryIndexKind::ActiveEffects => &mut roots.active_effects,
        RunQueryIndexKind::ActiveLeases => &mut roots.active_leases,
    };
    let next = StateRootValue::encode(kind, value)?;
    if map_get(selected, key, overlay)?.as_ref() == Some(&next) {
        return Ok(());
    }
    *selected = map_put(selected, key, next, overlay)?;
    roots.verify()?;
    *index_root = map_put(
        index_root,
        run_id,
        StateRootValue::run_query_indexes(run_id, roots)?,
        overlay,
    )?;
    Ok(())
}

#[derive(Clone, Copy)]
struct RunQueryMembership {
    index: RunQueryIndexKind,
    active: bool,
}

fn sync_run_query_item<T, R>(
    index_root: &mut MapRoot,
    run_id: &str,
    membership: RunQueryMembership,
    key: &str,
    kind: StateRootLeafKind,
    value: &T,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    if membership.active {
        put_run_query_item(
            index_root,
            run_id,
            membership.index,
            key,
            kind,
            value,
            overlay,
        )
    } else {
        remove_run_query_item(index_root, run_id, membership.index, key, overlay)
    }
}

fn remove_run_query_item<R: StateRootResolver + ?Sized>(
    index_root: &mut MapRoot,
    run_id: &str,
    index: RunQueryIndexKind,
    key: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    cymule_core::validate_identity("Run query-index owner", run_id)?;
    let Some(value) = map_get(index_root, run_id, overlay)? else {
        return Err(DurableError::Integrity {
            code: "run_query_indexes_missing".to_owned(),
            message: format!("retained Run {run_id} has no current-membership indexes"),
        });
    };
    let mut roots = value.decode_run_query_indexes(run_id)?;
    let selected = match index {
        RunQueryIndexKind::Waits => &mut roots.waits,
        RunQueryIndexKind::Effects => &mut roots.effects,
        RunQueryIndexKind::Occurrences => &mut roots.occurrences,
        RunQueryIndexKind::Attempts => &mut roots.attempts,
        RunQueryIndexKind::PendingWaits => &mut roots.pending_waits,
        RunQueryIndexKind::ActiveEffects => &mut roots.active_effects,
        RunQueryIndexKind::ActiveLeases => &mut roots.active_leases,
    };
    if map_get(selected, key, overlay)?.is_none() {
        return Ok(());
    }
    *selected = map_remove(selected, key, overlay)?;
    roots.verify()?;
    *index_root = map_put(
        index_root,
        run_id,
        StateRootValue::run_query_indexes(run_id, roots)?,
        overlay,
    )?;
    Ok(())
}

fn pending_wait_source_root_mut<'a>(
    roots: &'a mut StateRoots,
    source: &crate::WaitActivationSource,
) -> &'a mut MapRoot {
    match source {
        crate::WaitActivationSource::Signal { .. } => &mut roots.pending_signal_sources,
        crate::WaitActivationSource::Timer { .. } => &mut roots.pending_timer_sources,
    }
}

fn insert_pending_wait_source<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    source: crate::WaitActivationSource,
    wait: &crate::WaitCondition,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let source_key = pending_wait_source_key(&source)?;
    let source_root = pending_wait_source_root_mut(roots, &source);
    let mut waits = match map_get(source_root, &source_key, overlay)? {
        Some(value) => value.decode_pending_wait_source(&source)?,
        None => MapRoot::empty(),
    };
    let encoded = StateRootValue::encode(StateRootLeafKind::Wait, wait)?;
    if let Some(retained) = map_get(&waits, &wait.wait_id, overlay)? {
        if retained != encoded {
            return Err(DurableError::HistoryConflict {
                code: "state_root_pending_wait_source_member_reuse".to_owned(),
                message: format!(
                    "pending-Wait source member {} changed exact content",
                    wait.wait_id
                ),
            });
        }
    } else {
        waits = map_put(&waits, &wait.wait_id, encoded, overlay)?;
    }
    *source_root = map_put(
        source_root,
        &source_key,
        StateRootValue::pending_wait_source(source, waits)?,
        overlay,
    )?;
    Ok(())
}

fn remove_pending_wait_source<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    source: &crate::WaitActivationSource,
    wait: &crate::WaitCondition,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let source_key = pending_wait_source_key(source)?;
    let source_root = pending_wait_source_root_mut(roots, source);
    let value =
        map_get(source_root, &source_key, overlay)?.ok_or_else(|| DurableError::Integrity {
            code: "state_root_pending_wait_source_missing".to_owned(),
            message: format!(
                "pending Wait {} has no exact source descriptor",
                wait.wait_id
            ),
        })?;
    let mut waits = value.decode_pending_wait_source(source)?;
    let retained =
        map_get(&waits, &wait.wait_id, overlay)?.ok_or_else(|| DurableError::Integrity {
            code: "state_root_pending_wait_source_member_missing".to_owned(),
            message: format!(
                "pending Wait {} is absent from its source descriptor",
                wait.wait_id
            ),
        })?;
    if retained != StateRootValue::encode(StateRootLeafKind::Wait, wait)? {
        return Err(DurableError::Integrity {
            code: "state_root_pending_wait_source_member_mismatch".to_owned(),
            message: format!(
                "pending Wait {} differs from its source descriptor",
                wait.wait_id
            ),
        });
    }
    waits = map_remove(&waits, &wait.wait_id, overlay)?;
    *source_root = if waits.entries == 0 {
        map_remove(source_root, &source_key, overlay)?
    } else {
        map_put(
            source_root,
            &source_key,
            StateRootValue::pending_wait_source(source.clone(), waits)?,
            overlay,
        )?
    };
    Ok(())
}

fn insert_immutable_typed_value<T, R>(
    root: &mut MapRoot,
    key: &str,
    kind: StateRootLeafKind,
    value: &T,
    family: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    let next = StateRootValue::encode(kind, value)?;
    match map_get(root, key, overlay)? {
        Some(current) if current == next => Ok(()),
        Some(StateRootValue::Leaf {
            kind: current_kind, ..
        }) if current_kind != kind => Err(DurableError::Integrity {
            code: "state_root_immutable_history_kind_mismatch".to_owned(),
            message: format!("{family} {key} has leaf kind {current_kind:?} instead of {kind:?}"),
        }),
        Some(StateRootValue::Leaf { .. }) => Err(DurableError::HistoryConflict {
            code: "state_root_immutable_history_rewrite".to_owned(),
            message: format!("{family} {key} was reused with different semantics"),
        }),
        Some(_) => Err(DurableError::Integrity {
            code: "state_root_immutable_history_kind_mismatch".to_owned(),
            message: format!("{family} {key} resolves to a nested root descriptor"),
        }),
        None => {
            *root = map_put(root, key, next, overlay)?;
            Ok(())
        }
    }
}

fn insert_agent_message_current<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    value: &cymule_profile_protocol::agent::AgentMessageCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentMessageCurrent, agent_message_key};

    value.verify()?;
    let key = agent_message_key(&value.session_id, &value.message.message_id)?;
    let encoded = StateRootValue::encode(StateRootLeafKind::AgentMessageCurrent, value)?;
    match map_get(&roots.agent_messages, &key, overlay)? {
        Some(current) if current == encoded => return Ok(()),
        Some(_) => {
            return Err(DurableError::HistoryConflict {
                code: "agent_message_identity_reused".to_owned(),
                message: format!(
                    "Agent message {} in Session {} was reused with different content",
                    value.message.message_id, value.session_id
                ),
            });
        }
        None => {}
    }

    let current = match map_get(&roots.agent_message_indexes, &value.session_id, overlay)? {
        Some(descriptor) => descriptor.decode_agent_message_index_root(&value.session_id)?,
        None => LogRoot::empty(),
    };
    if value.order.index != current.len {
        return Err(DurableError::HistoryConflict {
            code: "agent_message_order_index_mismatch".to_owned(),
            message: format!(
                "Agent message {} requested ordinal {} but the exact next ordinal is {}",
                value.message.message_id, value.order.index, current.len
            ),
        });
    }
    let previous_head = if current.len == 0 {
        None
    } else {
        let previous: AgentMessageCurrent = log_value_at(&current, current.len - 1, overlay)?
            .decode(StateRootLeafKind::AgentMessageCurrent)?;
        previous.verify()?;
        if previous.session_id != value.session_id || previous.order.index + 1 != value.order.index
        {
            return Err(DurableError::Integrity {
                code: "agent_message_order_predecessor_mismatch".to_owned(),
                message: "Agent message index does not contain the exact preceding Session entry"
                    .to_owned(),
            });
        }
        Some(previous.order.head)
    };
    if value.order.previous_head != previous_head {
        return Err(DurableError::HistoryConflict {
            code: "agent_message_order_head_mismatch".to_owned(),
            message: "Agent message append does not extend the exact Session message head"
                .to_owned(),
        });
    }

    let next = log_append(&current, std::slice::from_ref(&encoded), overlay)?;
    roots.agent_message_indexes = map_put(
        &roots.agent_message_indexes,
        &value.session_id,
        StateRootValue::agent_message_index(&value.session_id, &next)?,
        overlay,
    )?;
    roots.agent_messages = map_put(&roots.agent_messages, &key, encoded, overlay)?;
    Ok(())
}

fn put_agent_occurrence_current<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    value: &cymule_profile_protocol::agent::AgentOccurrenceCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentOccurrenceCurrent, agent_occurrence_key};

    value.verify()?;
    let session_id = &value.occurrence.session_id;
    let occurrence_id = &value.occurrence.occurrence_id;
    let key = agent_occurrence_key(session_id, occurrence_id)?;
    let encoded = StateRootValue::encode(StateRootLeafKind::AgentOccurrenceCurrent, value)?;
    let previous = match map_get(&roots.agent_occurrences, &key, overlay)? {
        Some(current) if current == encoded => return Ok(()),
        Some(current) => Some(
            current.decode::<AgentOccurrenceCurrent>(StateRootLeafKind::AgentOccurrenceCurrent)?,
        ),
        None => None,
    };
    let current_index = match map_get(
        &roots.agent_unresolved_occurrence_indexes,
        session_id,
        overlay,
    )? {
        Some(descriptor) => descriptor.decode_agent_unresolved_occurrence_index_root(session_id)?,
        None => LogRoot::empty(),
    };
    let next_index = match previous {
        None if value.occurrence.is_terminal() => {
            return Err(DurableError::HistoryConflict {
                code: "agent_occurrence_terminal_without_current".to_owned(),
                message: format!(
                    "terminal Agent occurrence {occurrence_id} has no exact prior current"
                ),
            });
        }
        None => {
            if current_index.len > 0 {
                let last: AgentOccurrenceCurrent =
                    log_value_at(&current_index, current_index.len - 1, overlay)?
                        .decode(StateRootLeafKind::AgentOccurrenceCurrent)?;
                if last.occurrence.session_id.as_str() != session_id
                    || last.ordinal >= value.ordinal
                {
                    return Err(DurableError::HistoryConflict {
                        code: "agent_occurrence_ordinal_order_mismatch".to_owned(),
                        message:
                            "new Agent occurrence does not extend the exact Session ordinal index"
                                .to_owned(),
                    });
                }
            }
            log_append(&current_index, std::slice::from_ref(&encoded), overlay)?
        }
        Some(previous) => {
            previous.verify()?;
            if previous.ordinal != value.ordinal
                || previous.occurrence.session_id.as_str() != session_id
                || previous.occurrence.occurrence_id.as_str() != occurrence_id
                || previous.occurrence.is_terminal()
            {
                return Err(DurableError::HistoryConflict {
                    code: "agent_occurrence_current_rewrite".to_owned(),
                    message: format!(
                        "Agent occurrence {occurrence_id} cannot rewrite its exact owner, ordinal, or terminal current"
                    ),
                });
            }
            let position = find_agent_occurrence_ordinal(
                &current_index,
                session_id,
                previous.ordinal,
                overlay,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_unresolved_occurrence_missing".to_owned(),
                message: format!(
                    "unresolved Agent occurrence {occurrence_id} is absent from its Session index"
                ),
            })?;
            let indexed: AgentOccurrenceCurrent = log_value_at(&current_index, position, overlay)?
                .decode(StateRootLeafKind::AgentOccurrenceCurrent)?;
            if indexed != previous {
                return Err(DurableError::Integrity {
                    code: "agent_unresolved_occurrence_current_mismatch".to_owned(),
                    message: format!(
                        "unresolved Agent occurrence {occurrence_id} index differs from its exact current"
                    ),
                });
            }
            if value.occurrence.is_terminal() {
                remove_log_value_at(&current_index, position, overlay)?
            } else {
                replace_log_value_at(&current_index, position, encoded.clone(), overlay)?
            }
        }
    };
    roots.agent_unresolved_occurrence_indexes = map_put(
        &roots.agent_unresolved_occurrence_indexes,
        session_id,
        StateRootValue::agent_unresolved_occurrence_index(session_id, &next_index)?,
        overlay,
    )?;
    roots.agent_occurrences = map_put(&roots.agent_occurrences, &key, encoded, overlay)?;
    Ok(())
}

fn put_agent_stream_current<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    value: &cymule_profile_protocol::agent::AgentStreamCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentStreamCurrent, AgentStreamState, agent_stream_key};

    value.verify()?;
    let key = agent_stream_key(&value.session_id, &value.stream_id)?;
    let encoded = StateRootValue::encode(StateRootLeafKind::AgentStreamCurrent, value)?;
    let previous = match map_get(&roots.agent_streams, &key, overlay)? {
        Some(current) if current == encoded => return Ok(()),
        Some(current) => {
            Some(current.decode::<AgentStreamCurrent>(StateRootLeafKind::AgentStreamCurrent)?)
        }
        None => None,
    };
    let current_index = match map_get(&roots.agent_open_stream_indexes, &value.session_id, overlay)?
    {
        Some(descriptor) => descriptor.decode_agent_open_stream_index_root(&value.session_id)?,
        None => LogRoot::empty(),
    };
    let position =
        find_agent_stream_id(&current_index, &value.session_id, &value.stream_id, overlay)?;
    let next_index = match previous {
        None if value.state != AgentStreamState::Open => {
            return Err(DurableError::HistoryConflict {
                code: "agent_stream_terminal_without_current".to_owned(),
                message: format!(
                    "terminal Agent stream {} has no exact prior current",
                    value.stream_id
                ),
            });
        }
        None => {
            let Err(insertion) = position else {
                return Err(DurableError::Integrity {
                    code: "agent_open_stream_alias_missing".to_owned(),
                    message: format!(
                        "open Agent stream {} exists in its index without an exact current",
                        value.stream_id
                    ),
                });
            };
            insert_log_value_at(&current_index, insertion, encoded.clone(), overlay)?
        }
        Some(previous) => {
            previous.verify()?;
            if previous.session_id != value.session_id
                || previous.stream_id != value.stream_id
                || previous.state != AgentStreamState::Open
            {
                return Err(DurableError::HistoryConflict {
                    code: "agent_stream_current_rewrite".to_owned(),
                    message: format!(
                        "Agent stream {} cannot rewrite its owner, identity, or terminal current",
                        value.stream_id
                    ),
                });
            }
            let position = position.map_err(|_| DurableError::Integrity {
                code: "agent_open_stream_missing".to_owned(),
                message: format!(
                    "open Agent stream {} is absent from its Session index",
                    value.stream_id
                ),
            })?;
            let indexed: AgentStreamCurrent = log_value_at(&current_index, position, overlay)?
                .decode(StateRootLeafKind::AgentStreamCurrent)?;
            if indexed != previous {
                return Err(DurableError::Integrity {
                    code: "agent_open_stream_current_mismatch".to_owned(),
                    message: format!(
                        "open Agent stream {} index differs from its exact current",
                        value.stream_id
                    ),
                });
            }
            if value.state == AgentStreamState::Open {
                replace_log_value_at(&current_index, position, encoded.clone(), overlay)?
            } else {
                remove_log_value_at(&current_index, position, overlay)?
            }
        }
    };
    roots.agent_open_stream_indexes = map_put(
        &roots.agent_open_stream_indexes,
        &value.session_id,
        StateRootValue::agent_open_stream_index(&value.session_id, &next_index)?,
        overlay,
    )?;
    roots.agent_streams = map_put(&roots.agent_streams, &key, encoded, overlay)?;
    Ok(())
}

fn apply_agent_target_claim<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    transition: &cymule_profile_protocol::agent::AgentTargetClaimTransition,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::{AgentTargetClaimCurrent, agent_target_claim_key};

    transition.verify()?;
    let key = agent_target_claim_key(&transition.current.session_id, &transition.current.target)?;
    let retained = map_get(&roots.agent_target_claims, &key, overlay)?
        .map(|value| {
            value.decode::<AgentTargetClaimCurrent>(StateRootLeafKind::AgentTargetClaimCurrent)
        })
        .transpose()?;
    if retained != transition.source {
        return Err(DurableError::HistoryConflict {
            code: "agent_target_claim_source_changed".to_owned(),
            message: "Agent target claim no longer matches its exact source generation".to_owned(),
        });
    }
    roots.agent_target_claims = map_put(
        &roots.agent_target_claims,
        &key,
        StateRootValue::encode(
            StateRootLeafKind::AgentTargetClaimCurrent,
            &transition.current,
        )?,
        overlay,
    )?;
    Ok(())
}

fn insert_log_value_at<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    value: StateRootValue,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot> {
    let value_id = overlay.insert_value(value)?;
    apply_log_mutation(root, LogMutation::insert_at(index, value_id), overlay)
}

fn replace_log_value_at<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    value: StateRootValue,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot> {
    let exact = prove_log_exact(root, index, overlay)?;
    let expected = verify_log_exact(root, index, &exact)?.value().to_owned();
    let value_id = overlay.insert_value(value)?;
    apply_log_mutation(
        root,
        LogMutation::replace_at(index, expected, value_id),
        overlay,
    )
}

fn remove_log_value_at<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot> {
    let exact = prove_log_exact(root, index, overlay)?;
    let expected = verify_log_exact(root, index, &exact)?.value().to_owned();
    apply_log_mutation(root, LogMutation::remove_at(index, expected), overlay)
}

fn find_agent_occurrence_ordinal<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    session_id: &str,
    ordinal: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Option<u64>> {
    use cymule_profile_protocol::agent::AgentOccurrenceCurrent;

    let mut start = 0;
    let mut end = root.len;
    while start < end {
        let middle = start + (end - start) / 2;
        let current: AgentOccurrenceCurrent = log_value_at(root, middle, overlay)?
            .decode(StateRootLeafKind::AgentOccurrenceCurrent)?;
        current.verify()?;
        if current.occurrence.session_id != session_id || current.occurrence.is_terminal() {
            return Err(DurableError::Integrity {
                code: "agent_unresolved_occurrence_index_invalid".to_owned(),
                message: "Agent unresolved occurrence index contains a foreign or terminal entry"
                    .to_owned(),
            });
        }
        match current.ordinal.cmp(&ordinal) {
            std::cmp::Ordering::Less => start = middle + 1,
            std::cmp::Ordering::Greater => end = middle,
            std::cmp::Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
}

fn find_agent_stream_id<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    session_id: &str,
    stream_id: &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<Result<u64, u64>> {
    use cymule_profile_protocol::agent::{AgentStreamCurrent, AgentStreamState};

    let mut start = 0;
    let mut end = root.len;
    while start < end {
        let middle = start + (end - start) / 2;
        let current: AgentStreamCurrent =
            log_value_at(root, middle, overlay)?.decode(StateRootLeafKind::AgentStreamCurrent)?;
        current.verify()?;
        if current.session_id != session_id || current.state != AgentStreamState::Open {
            return Err(DurableError::Integrity {
                code: "agent_open_stream_index_invalid".to_owned(),
                message: "Agent open-stream index contains a foreign or terminal entry".to_owned(),
            });
        }
        match current.stream_id.as_str().cmp(stream_id) {
            std::cmp::Ordering::Less => start = middle + 1,
            std::cmp::Ordering::Greater => end = middle,
            std::cmp::Ordering::Equal => return Ok(Ok(middle)),
        }
    }
    Ok(Err(start))
}

fn validate_agent_command_receipt<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    receipt: &cymule_profile_protocol::agent::AgentCommandReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let key = cymule_profile_protocol::agent::agent_command_key(&receipt.command_id)?;
    let command: cymule_profile_protocol::agent::AgentCommand =
        map_get(&roots.agent_commands, &key, overlay)?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_receipt_command_missing".to_owned(),
                message: format!(
                    "Agent receipt {} has no exact persisted command",
                    receipt.receipt_id
                ),
            })?
            .decode(StateRootLeafKind::AgentCommand)?;
    receipt.verify_for(&command)?;
    Ok(())
}

fn validate_agent_session_roots<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    session: &cymule_profile_protocol::agent::AgentSessionCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::agent::AgentMessageCurrent;

    session.verify()?;
    let messages = match map_get(&roots.agent_message_indexes, &session.session_id, overlay)? {
        Some(descriptor) => descriptor.decode_agent_message_index_root(&session.session_id)?,
        None => LogRoot::empty(),
    };
    if messages.len != session.message_count {
        return Err(DurableError::Integrity {
            code: "agent_session_message_count_mismatch".to_owned(),
            message: format!(
                "Agent Session {} message count differs from its exact index",
                session.session_id
            ),
        });
    }
    let exact_message_head = if messages.len == 0 {
        None
    } else {
        let last: AgentMessageCurrent = log_value_at(&messages, messages.len - 1, overlay)?
            .decode(StateRootLeafKind::AgentMessageCurrent)?;
        last.verify()?;
        if last.session_id != session.session_id || last.order.index + 1 != messages.len {
            return Err(DurableError::Integrity {
                code: "agent_session_message_head_owner_mismatch".to_owned(),
                message: "Agent Session message head is owned by a different Session or ordinal"
                    .to_owned(),
            });
        }
        Some(last.order.head)
    };
    if session.message_head != exact_message_head {
        return Err(DurableError::Integrity {
            code: "agent_session_message_head_mismatch".to_owned(),
            message: format!(
                "Agent Session {} message head differs from its exact index",
                session.session_id
            ),
        });
    }
    let unresolved = match map_get(
        &roots.agent_unresolved_occurrence_indexes,
        &session.session_id,
        overlay,
    )? {
        Some(descriptor) => {
            descriptor.decode_agent_unresolved_occurrence_index_root(&session.session_id)?
        }
        None => LogRoot::empty(),
    };
    if unresolved.len != session.unresolved_occurrence_count {
        return Err(DurableError::Integrity {
            code: "agent_session_unresolved_occurrence_count_mismatch".to_owned(),
            message: format!(
                "Agent Session {} unresolved occurrence count differs from its exact index",
                session.session_id
            ),
        });
    }
    let open_streams = match map_get(
        &roots.agent_open_stream_indexes,
        &session.session_id,
        overlay,
    )? {
        Some(descriptor) => descriptor.decode_agent_open_stream_index_root(&session.session_id)?,
        None => LogRoot::empty(),
    };
    if open_streams.len != session.open_stream_count {
        return Err(DurableError::Integrity {
            code: "agent_session_open_stream_count_mismatch".to_owned(),
            message: format!(
                "Agent Session {} open stream count differs from its exact index",
                session.session_id
            ),
        });
    }
    validate_agent_nonterminal_tools(roots, session, overlay)
}

fn validate_agent_nonterminal_tools<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    session: &cymule_profile_protocol::agent::AgentSessionCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (tool_call_id, directory_entry) in &session.nonterminal_tools {
        let key =
            cymule_profile_protocol::agent::agent_tool_key(&session.session_id, tool_call_id)?;
        let current: cymule_profile_protocol::agent::AgentToolCurrent =
            map_get(&roots.agent_tools, &key, overlay)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "agent_session_nonterminal_tool_missing".to_owned(),
                    message: format!(
                        "Agent Session {} non-terminal Tool {tool_call_id} is missing",
                        session.session_id
                    ),
                })?
                .decode(StateRootLeafKind::AgentToolCurrent)?;
        current.verify()?;
        if current.session_id != session.session_id
            || current.tool.tool_call_id != *tool_call_id
            || directory_entry.verify_for(&current).is_err()
        {
            return Err(DurableError::Integrity {
                code: "agent_session_nonterminal_tool_mismatch".to_owned(),
                message: format!(
                    "Agent Session {} non-terminal Tool {tool_call_id} differs from its capacity directory",
                    session.session_id
                ),
            });
        }
    }
    Ok(())
}

fn resource_receipt_authority_ids(
    receipt: &cymule_profile_protocol::resource::ResourceCommandReceipt,
) -> BTreeSet<String> {
    use cymule_profile_protocol::resource::ResourceCommandOutcome;

    let outcome_id = match &receipt.outcome {
        ResourceCommandOutcome::Pin { receipt } => receipt.receipt_id.clone(),
        ResourceCommandOutcome::Release { receipt } => receipt.receipt_id.clone(),
        ResourceCommandOutcome::GarbageCollect { receipt } => receipt.receipt_id.clone(),
        ResourceCommandOutcome::ReconcileDelete { receipt } => receipt.receipt_id.clone(),
        ResourceCommandOutcome::BeginDelete { intent } => intent.intent_id.clone(),
        ResourceCommandOutcome::Transfer { receipt } => receipt.receipt_id.clone(),
        ResourceCommandOutcome::ActivateTransfer { receipt } => receipt.receipt_id.clone(),
    };
    BTreeSet::from([
        receipt.command.command_id.clone(),
        receipt.receipt_id.clone(),
        outcome_id,
    ])
}

fn insert_resource_command_receipt<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    receipt: &cymule_profile_protocol::resource::ResourceCommandReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    receipt.verify()?;
    for authority_id in resource_receipt_authority_ids(receipt) {
        insert_immutable_typed_value(
            &mut roots.resource_command_receipts,
            &authority_id,
            StateRootLeafKind::ResourceCommandReceipt,
            receipt,
            "Resource command receipt authority",
            overlay,
        )?;
    }
    Ok(())
}

fn insert_resource_handoff_slot<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    entry: &cymule_profile_protocol::resource::ResourceHandoffIndexEntry,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    entry.verify()?;
    let mut slots = match map_get(&roots.resource_handoff_slots, &entry.to_run, overlay)? {
        Some(value) => value.decode_resource_handoff_slots_root(&entry.to_run)?,
        None => MapRoot::empty(),
    };
    insert_immutable_typed_value(
        &mut slots,
        &entry.slot,
        StateRootLeafKind::ResourceHandoffIndex,
        entry,
        "Resource target slot",
        overlay,
    )?;
    roots.resource_handoff_slots = map_put(
        &roots.resource_handoff_slots,
        &entry.to_run,
        StateRootValue::resource_handoff_slots(&entry.to_run, &slots)?,
        overlay,
    )?;
    Ok(())
}

fn insert_resource_handoff_activation_current<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    current: &cymule_profile_protocol::resource::ResourceHandoffActivationCurrent,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    current.verify()?;
    insert_immutable_typed_value(
        &mut roots.resource_handoff_activation_current,
        &current.receipt.activation.activation_id,
        StateRootLeafKind::ResourceHandoffActivationCurrent,
        current,
        "Resource handoff activation current",
        overlay,
    )?;
    insert_immutable_typed_value(
        &mut roots.resource_handoff_activations_by_transfer,
        &current.receipt.activation.transfer_id,
        StateRootLeafKind::ResourceHandoffActivationCurrent,
        current,
        "Resource handoff activation by transfer",
        overlay,
    )
}

fn append_resource_handoff_index<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    entry: &cymule_profile_protocol::resource::ResourceHandoffIndexEntry,
    _activation: bool,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    entry.verify()?;
    let current = match map_get(&roots.resource_handoff_indexes, &entry.to_run, overlay)? {
        Some(value) => value.decode_resource_handoff_index_root(&entry.to_run)?,
        None => LogRoot::empty(),
    };
    if entry.target_index != current.len {
        return Err(DurableError::HistoryConflict {
            code: "resource_handoff_target_index_mismatch".to_owned(),
            message: format!(
                "Resource transfer {} requested target index {} but the exact next index is {}",
                entry.transfer_id, entry.target_index, current.len
            ),
        });
    }
    let next = log_append(
        &current,
        &[StateRootValue::encode(
            StateRootLeafKind::ResourceHandoffIndex,
            entry,
        )?],
        overlay,
    )?;
    roots.resource_handoff_indexes = map_put(
        &roots.resource_handoff_indexes,
        &entry.to_run,
        StateRootValue::resource_handoff_index(&entry.to_run, &next)?,
        overlay,
    )?;
    Ok(())
}

fn append_resource_handoff_activation_index<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    entry: &cymule_profile_protocol::resource::ResourceHandoffActivationIndexEntry,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    entry.verify()?;
    let current = match map_get(
        &roots.resource_handoff_activation_indexes,
        &entry.to_run,
        overlay,
    )? {
        Some(value) => value.decode_resource_handoff_activation_index_root(&entry.to_run)?,
        None => LogRoot::empty(),
    };
    if entry.activation_index != current.len {
        return Err(DurableError::HistoryConflict {
            code: "resource_handoff_activation_index_mismatch".to_owned(),
            message: format!(
                "Resource activation {} requested target index {} but the exact next index is {}",
                entry.activation_id, entry.activation_index, current.len
            ),
        });
    }
    let next = log_append(
        &current,
        &[StateRootValue::encode(
            StateRootLeafKind::ResourceHandoffActivationIndex,
            entry,
        )?],
        overlay,
    )?;
    roots.resource_handoff_activation_indexes = map_put(
        &roots.resource_handoff_activation_indexes,
        &entry.to_run,
        StateRootValue::resource_handoff_activation_index(&entry.to_run, &next)?,
        overlay,
    )?;
    Ok(())
}

fn put_evolution_mutation<R: StateRootResolver + ?Sized>(
    roots: &mut EvolutionRootSet,
    value: &cymule_profile_protocol::evolution::EvolutionMutation,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    use cymule_profile_protocol::evolution::EvolutionStateFamily as Family;

    value.verify()?;
    let (family, storage_key) = value.storage_key()?;
    let root = roots.state_mut(family);
    if matches!(
        family,
        Family::DefinitionRecord
            | Family::LinkRecord
            | Family::PlanRecord
            | Family::EdgeRecord
            | Family::RolloutDecision
            | Family::OccurrenceCurrent
            | Family::SelectionCurrent
            | Family::MigrationRecord
            | Family::RestartRecord
            | Family::ShadowRecord
            | Family::ShadowSubjectCurrent
            | Family::ObservationRecord
            | Family::ObservationOccurrenceCurrent
            | Family::EvidenceCurrent
            | Family::DecisionTransitionCurrent
            | Family::TransitionRecord
    ) {
        insert_immutable_typed_value(
            root,
            &storage_key,
            StateRootLeafKind::EvolutionMutation,
            value,
            "Evolution immutable state leaf",
            overlay,
        )?;
    } else {
        put_typed_value(
            root,
            &storage_key,
            StateRootLeafKind::EvolutionMutation,
            value,
            overlay,
        )?;
    }
    Ok(())
}

fn apply_virtual_mutation<R: StateRootResolver + ?Sized>(
    roots: &mut VirtualRootSet,
    value: &cymule_profile_protocol::virtual_work::VirtualStateMutation,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    value.verify()?;
    let family = value.family();
    let storage_key = value.storage_key()?;
    let before = value.before_leaf();
    let after = value.after_leaf();
    let root = roots.state_mut(family);
    let retained = map_get(root, &storage_key, overlay)?
        .map(|stored| stored.decode(StateRootLeafKind::VirtualStateLeaf))
        .transpose()?;
    if retained != before {
        return Err(DurableError::HistoryConflict {
            code: "virtual_mutation_parent_mismatch".to_owned(),
            message: format!(
                "Virtual {family:?} leaf {storage_key} no longer matches its exact parent"
            ),
        });
    }
    *root = match after {
        Some(after) => map_put(
            root,
            &storage_key,
            StateRootValue::encode(StateRootLeafKind::VirtualStateLeaf, &after)?,
            overlay,
        )?,
        None => map_remove(root, &storage_key, overlay)?,
    };
    Ok(())
}

/// Preview one canonical Virtual mutation set against the exact pinned
/// manifest and return only the resulting semantic family roots.
///
/// The temporary immutable nodes are deliberately dropped. Final `StateRoot`
/// lowering deterministically reapplies the same mutations in the one owning
/// CAS, so preview creates no second persistence authority or orphan object.
pub(crate) fn preview_virtual_mutations<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    mutations: &[cymule_profile_protocol::virtual_work::VirtualStateMutation],
    resolver: &mut R,
) -> DurableResult<cymule_profile_protocol::virtual_work::VirtualStateRoots> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let canonical =
        cymule_profile_protocol::virtual_work::VirtualMutationSet::new(mutations.to_vec())?;
    if canonical.operations.as_slice() != mutations {
        return Err(DurableError::Validation(
            "Virtual mutation preview requires strict canonical family-and-key order".to_owned(),
        ));
    }
    let mut roots = manifest.roots.virtual_work.clone();
    let mut overlay = ObjectOverlay::new(resolver);
    for mutation in mutations {
        apply_virtual_mutation(&mut roots, mutation, &mut overlay)?;
    }
    virtual_semantic_roots(&roots)
}

struct CommandEventPosition<'a> {
    first: u64,
    event_ids: Vec<&'a str>,
}

fn cumulative_batch_count(
    anchor: Option<&cymule_core::MachineBaseAnchor>,
    hot_count: u64,
) -> DurableResult<u64> {
    anchor
        .map_or(0, |anchor| anchor.archive_batch_count)
        .checked_add(hot_count)
        .filter(|count| *count <= MAX_EXACT_INTEGER)
        .ok_or_else(|| DurableError::Integrity {
            code: "state_root_machine_batch_count_overflow".to_owned(),
            message: "archived plus hot Machine batch count exceeds the exact range".to_owned(),
        })
}

fn command_event_positions(
    parent_count: u64,
    events: &[cymule_core::Event],
) -> DurableResult<BTreeMap<&str, CommandEventPosition<'_>>> {
    let mut positions = BTreeMap::<&str, CommandEventPosition<'_>>::new();
    for (index, event) in events.iter().enumerate() {
        let position = parent_count
            .checked_add(
                u64::try_from(index)
                    .map_err(|error| DurableError::Validation(error.to_string()))?,
            )
            .and_then(|value| value.checked_add(1))
            .filter(|value| *value <= MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                DurableError::Validation("Machine Event position overflowed".to_owned())
            })?;
        let group = positions
            .entry(&event.command_id)
            .or_insert_with(|| CommandEventPosition {
                first: position,
                event_ids: Vec::new(),
            });
        if group.first.checked_add(
            u64::try_from(group.event_ids.len())
                .map_err(|error| DurableError::Validation(error.to_string()))?,
        ) != Some(position)
        {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_events_noncontiguous".to_owned(),
                message: format!(
                    "Machine command {} Events are not contiguous",
                    event.command_id
                ),
            });
        }
        group.event_ids.push(&event.event_id);
    }
    Ok(positions)
}

fn apply_machine_root_delta<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    root_delta: &cymule_core::MachineRootDelta,
    parent_frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    result_frontier: &cymule_core::durable_internal::MachineAuthorityFrontier,
    base_anchor: &mut Option<cymule_core::MachineBaseAnchor>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    parent_frontier.verify()?;
    result_frontier.verify()?;
    if parent_frontier.authority_root != root_delta.parent_authority_root
        || result_frontier.authority_root != root_delta.result_authority_root
    {
        return Err(DurableError::HistoryConflict {
            code: "state_root_machine_parent_mismatch".to_owned(),
            message: "Machine root delta does not extend the exact pinned frontier".to_owned(),
        });
    }
    let current_anchor_id = base_anchor.as_ref().map(|anchor| anchor.anchor_id.clone());
    if current_anchor_id != root_delta.parent_anchor_id {
        return Err(DurableError::Integrity {
            code: "state_root_machine_parent_anchor_mismatch".to_owned(),
            message: "Machine root delta does not match the current base anchor".to_owned(),
        });
    }
    apply_machine_material_delta(roots, root_delta, overlay)?;
    apply_machine_batch_delta(roots, root_delta, overlay)?;
    apply_machine_history_logs(roots, root_delta, overlay)?;
    apply_machine_command_delta(roots, root_delta, parent_frontier.event_count, overlay)?;
    apply_machine_command_proofs(roots, root_delta, overlay)?;
    apply_machine_base_delta(roots, root_delta, base_anchor, overlay)?;
    if roots.machine_plans.entries != result_frontier.plan_count
        || roots.machine_plan_admissions.len != result_frontier.plan_count
        || roots.machine_artifacts.entries != result_frontier.artifact_count
        || roots.machine_artifact_admissions.len != result_frontier.artifact_count
        || cumulative_batch_count(base_anchor.as_ref(), roots.machine_command_batches.entries)?
            != result_frontier.batch_count
        || roots.machine_command_batch_admissions.len != roots.machine_command_batches.entries
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_material_frontier_mismatch".to_owned(),
            message: "Machine Plan or Artifact roots do not match the result frontier".to_owned(),
        });
    }
    Ok(())
}

fn apply_machine_material_delta<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    root_delta: &cymule_core::MachineRootDelta,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let plan_admissions = root_delta
        .plan_admission_order
        .iter()
        .map(|plan_id| {
            root_delta
                .plans
                .get(plan_id)
                .cloned()
                .ok_or_else(|| DurableError::Integrity {
                    code: "machine_delta_plan_admission_missing_value".to_owned(),
                    message: format!(
                        "Machine delta Plan admission order references missing Plan {plan_id}"
                    ),
                })
        })
        .collect::<DurableResult<Vec<_>>>()?;
    roots.machine_plan_admissions = append_typed_log(
        &roots.machine_plan_admissions,
        StateRootLeafKind::MachinePlan,
        &plan_admissions,
        overlay,
    )?;
    for (key, value) in &root_delta.plans {
        roots.machine_plans = map_put(
            &roots.machine_plans,
            key,
            StateRootValue::encode(StateRootLeafKind::MachinePlan, value)?,
            overlay,
        )?;
    }
    let artifact_admissions = root_delta
        .artifact_admission_order
        .iter()
        .map(|artifact_id| {
            root_delta
                .artifacts
                .get(artifact_id)
                .cloned()
                .ok_or_else(|| DurableError::Integrity {
                    code: "machine_delta_artifact_admission_missing_value".to_owned(),
                    message: format!(
                        "Machine delta Artifact admission order references missing Artifact {artifact_id}"
                    ),
                })
        })
        .collect::<DurableResult<Vec<_>>>()?;
    roots.machine_artifact_admissions = append_typed_log(
        &roots.machine_artifact_admissions,
        StateRootLeafKind::MachineArtifact,
        &artifact_admissions,
        overlay,
    )?;
    for (key, value) in &root_delta.artifacts {
        roots.machine_artifacts = map_put(
            &roots.machine_artifacts,
            key,
            StateRootValue::encode(StateRootLeafKind::MachineArtifact, value)?,
            overlay,
        )?;
    }
    Ok(())
}

fn apply_machine_batch_delta<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    root_delta: &cymule_core::MachineRootDelta,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let removed_batch_count = u64::try_from(root_delta.removed_batch_ids.len())
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    if removed_batch_count != 0 {
        let mut removed = BTreeSet::new();
        for index in 0..removed_batch_count {
            let batch: cymule_core::durable_internal::MachineCommandBatchRecord =
                log_value_at(&roots.machine_command_batch_admissions, index, overlay)?
                    .decode(StateRootLeafKind::MachineCommandBatch)?;
            removed.insert(batch.batch_id);
        }
        if removed != root_delta.removed_batch_ids {
            return Err(DurableError::Integrity {
                code: "state_root_machine_batch_compaction_prefix_mismatch".to_owned(),
                message: "Machine batch compaction does not remove its exact ordered prefix"
                    .to_owned(),
            });
        }
        roots.machine_command_batch_admissions = slice_log_prefix(
            &roots.machine_command_batch_admissions,
            removed_batch_count,
            overlay,
        )?;
        for batch_id in &root_delta.removed_batch_ids {
            roots.machine_command_batches =
                map_remove(&roots.machine_command_batches, batch_id, overlay)?;
        }
    }
    let batch_admissions = root_delta
        .batch_admission_order
        .iter()
        .map(|batch_id| {
            root_delta
                .batches
                .get(batch_id)
                .cloned()
                .ok_or_else(|| DurableError::Integrity {
                    code: "machine_delta_batch_admission_missing_value".to_owned(),
                    message: format!(
                        "Machine batch admission order references missing batch {batch_id}"
                    ),
                })
        })
        .collect::<DurableResult<Vec<_>>>()?;
    roots.machine_command_batch_admissions = append_typed_log(
        &roots.machine_command_batch_admissions,
        StateRootLeafKind::MachineCommandBatch,
        &batch_admissions,
        overlay,
    )?;
    for (batch_id, batch) in &root_delta.batches {
        if &batch.batch_id != batch_id {
            return Err(DurableError::Integrity {
                code: "state_root_machine_batch_key_mismatch".to_owned(),
                message: format!("Machine command batch key {batch_id} changed identity"),
            });
        }
        roots.machine_command_batches = map_put(
            &roots.machine_command_batches,
            batch_id,
            StateRootValue::encode(StateRootLeafKind::MachineCommandBatch, batch)?,
            overlay,
        )?;
    }
    Ok(())
}

fn apply_machine_history_logs<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    root_delta: &cymule_core::MachineRootDelta,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    verify_removed_log_ids(
        &roots.machine_events,
        &root_delta.removed_event_ids,
        StateRootLeafKind::MachineEvent,
        |value: &cymule_core::Event| value.event_id.as_str(),
        overlay,
    )?;
    roots.machine_events = slice_log_prefix(
        &roots.machine_events,
        u64::try_from(root_delta.removed_event_ids.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?,
        overlay,
    )?;
    roots.machine_events = append_typed_log(
        &roots.machine_events,
        StateRootLeafKind::MachineEvent,
        &root_delta.events,
        overlay,
    )?;
    verify_removed_log_ids(
        &roots.machine_admissions,
        &root_delta.removed_admission_ids,
        StateRootLeafKind::MachineAdmission,
        |value: &cymule_core::CommandAdmission| value.admission_id.as_str(),
        overlay,
    )?;
    roots.machine_admissions = slice_log_prefix(
        &roots.machine_admissions,
        u64::try_from(root_delta.removed_admission_ids.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?,
        overlay,
    )?;
    roots.machine_admissions = append_typed_log(
        &roots.machine_admissions,
        StateRootLeafKind::MachineAdmission,
        &root_delta.admissions,
        overlay,
    )?;
    Ok(())
}

fn apply_machine_command_delta<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    root_delta: &cymule_core::MachineRootDelta,
    parent_event_count: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    if root_delta.removed_command_ids != root_delta.removed_command_index_proof_ids {
        return Err(DurableError::Integrity {
            code: "state_root_machine_command_composite_removal_mismatch".to_owned(),
            message:
                "Machine command and archive-proof removals do not name the same composite leaves"
                    .to_owned(),
        });
    }
    for key in &root_delta.removed_command_ids {
        roots.machine_commands = map_remove(&roots.machine_commands, key, overlay)?;
    }
    let admissions = root_delta
        .admissions
        .iter()
        .map(|admission| (admission.command_id.as_str(), admission))
        .collect::<BTreeMap<_, _>>();
    let mut event_positions = command_event_positions(parent_event_count, &root_delta.events)?;
    for (key, record) in &root_delta.commands {
        let admission = admissions
            .get(key.as_str())
            .ok_or_else(|| DurableError::Integrity {
                code: "state_root_machine_command_admission_missing".to_owned(),
                message: format!("Machine command {key} has no exact admission in its root delta"),
            })?;
        let index_proof =
            root_delta
                .command_index_proofs
                .get(key)
                .ok_or_else(|| DurableError::Integrity {
                    code: "state_root_machine_command_index_proof_missing".to_owned(),
                    message: format!(
                        "Machine command {key} has no exact archive non-membership proof"
                    ),
                })?;
        let first_event_position = match event_positions.remove(key.as_str()) {
            Some(position) => {
                if !position.event_ids.iter().copied().eq(record
                    .receipt
                    .event_ids
                    .iter()
                    .map(String::as_str))
                {
                    return Err(DurableError::Integrity {
                        code: "state_root_machine_command_event_range_mismatch".to_owned(),
                        message: format!("Machine command {key} changed its ordered Event range"),
                    });
                }
                Some(position.first)
            }
            None if record.receipt.event_ids.is_empty() => None,
            None => {
                return Err(DurableError::Integrity {
                    code: "state_root_machine_command_event_range_missing".to_owned(),
                    message: format!("Machine command {key} has no exact Event range in its delta"),
                });
            }
        };
        roots.machine_commands = map_put(
            &roots.machine_commands,
            key,
            StateRootValue::machine_command_current(
                record.clone(),
                (*admission).clone(),
                index_proof.clone(),
                first_event_position,
            )?,
            overlay,
        )?;
    }
    if !event_positions.is_empty() {
        return Err(DurableError::Integrity {
            code: "state_root_machine_event_command_missing".to_owned(),
            message: "Machine delta contains Events outside its exact command set".to_owned(),
        });
    }
    Ok(())
}

fn apply_machine_command_proofs<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    root_delta: &cymule_core::MachineRootDelta,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for (key, index_proof) in &root_delta.command_index_proofs {
        if root_delta.commands.contains_key(key) {
            continue;
        }
        let retained = map_get(&roots.machine_commands, key, overlay)?.ok_or_else(|| {
            DurableError::Integrity {
                code: "state_root_machine_command_proof_orphan".to_owned(),
                message: format!(
                    "Machine archive non-membership proof {key} has no retained hot command"
                ),
            }
        })?;
        let StateRootValue::MachineCommandCurrent {
            record,
            admission,
            first_event_position,
            ..
        } = retained
        else {
            return Err(DurableError::Integrity {
                code: "state_root_machine_command_value_kind_mismatch".to_owned(),
                message: format!("Machine hot command {key} is not a composite authority leaf"),
            });
        };
        roots.machine_commands = map_put(
            &roots.machine_commands,
            key,
            StateRootValue::machine_command_current(
                *record,
                *admission,
                index_proof.clone(),
                first_event_position,
            )?,
            overlay,
        )?;
    }
    Ok(())
}

fn apply_machine_base_delta<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    root_delta: &cymule_core::MachineRootDelta,
    base_anchor: &mut Option<cymule_core::MachineBaseAnchor>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    if let Some(base) = &root_delta.base {
        let next_anchor =
            root_delta
                .base_anchor
                .clone()
                .ok_or_else(|| DurableError::Integrity {
                    code: "state_root_machine_result_anchor_missing".to_owned(),
                    message: "Machine base replacement has no result anchor".to_owned(),
                })?;
        if Some(next_anchor.anchor_id.clone()) != root_delta.result_anchor_id {
            return Err(DurableError::Integrity {
                code: "state_root_machine_result_anchor_mismatch".to_owned(),
                message: "Machine base replacement does not match its result anchor".to_owned(),
            });
        }
        roots.machine_base = Some(build_machine_base(base, overlay)?);
        *base_anchor = Some(next_anchor);
    } else if root_delta.base_anchor.is_some()
        || root_delta.result_anchor_id != root_delta.parent_anchor_id
    {
        return Err(DurableError::Integrity {
            code: "state_root_machine_unpaired_anchor_transition".to_owned(),
            message: "Machine anchor changed without a base replacement".to_owned(),
        });
    }
    Ok(())
}

fn append_typed_log<T, R>(
    root: &LogRoot,
    kind: StateRootLeafKind,
    values: &[T],
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<LogRoot>
where
    T: Serialize,
    R: StateRootResolver + ?Sized,
{
    let values = values
        .iter()
        .map(|value| StateRootValue::encode(kind, value))
        .collect::<DurableResult<Vec<_>>>()?;
    log_append(root, &values, overlay)
}

#[cfg(test)]
pub(crate) fn application_journal_ordered_root_from_records(
    records: &[crate::JournalRecord],
) -> DurableResult<String> {
    if records.is_empty() {
        return Err(DurableError::Validation(
            "application-journal prefix commitment requires records".to_owned(),
        ));
    }
    for record in records {
        record.verify()?;
    }
    let mut empty = EmptyStateRootResolver;
    let mut overlay = ObjectOverlay::new(&mut empty);
    build_typed_log(
        StateRootLeafKind::JournalRecord,
        records.to_vec(),
        &mut overlay,
    )
    .map(|root| root.ordered_root)
}

fn verify_removed_log_ids<T, R>(
    root: &LogRoot,
    expected: &[String],
    kind: StateRootLeafKind,
    identity: impl Fn(&T) -> &str,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()>
where
    T: DeserializeOwned + Serialize,
    R: StateRootResolver + ?Sized,
{
    for (index, expected) in expected.iter().enumerate() {
        let value: T = log_value_at(
            root,
            u64::try_from(index).map_err(|error| DurableError::Validation(error.to_string()))?,
            overlay,
        )?
        .decode(kind)?;
        if identity(&value) != expected {
            return Err(DurableError::Integrity {
                code: "state_root_machine_compaction_prefix_mismatch".to_owned(),
                message: "Machine compaction removal does not match its exact rooted prefix"
                    .to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
fn append_application_journal<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    journal_id: &str,
    records: &[crate::JournalRecord],
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let current = match map_get(&roots.application_journals, journal_id, overlay)? {
        Some(value) => value.decode_application_journal_root(journal_id)?,
        None => LogRoot::empty(),
    };
    let mut manifests = match map_get(
        &roots.application_journal_record_manifests,
        journal_id,
        overlay,
    )? {
        Some(value) => value.decode_record_manifest_root(journal_id)?,
        None => MapRoot::empty(),
    };
    for record in records {
        if map_get(&manifests, &record.record_id, overlay)?.is_some() {
            return Err(DurableError::HistoryConflict {
                code: "state_root_journal_record_reuse".to_owned(),
                message: format!(
                    "application journal record {} was already admitted",
                    record.record_id
                ),
            });
        }
        let manifest = crate::JournalRecordManifest::from_record(record)?;
        manifests = map_put(
            &manifests,
            &record.record_id,
            StateRootValue::encode(StateRootLeafKind::JournalRecordManifest, &manifest)?,
            overlay,
        )?;
    }
    let next = append_typed_log(&current, StateRootLeafKind::JournalRecord, records, overlay)?;
    roots.application_journals = map_put(
        &roots.application_journals,
        journal_id,
        StateRootValue::application_journal(journal_id, &next)?,
        overlay,
    )?;
    roots.application_journal_record_manifests = map_put(
        &roots.application_journal_record_manifests,
        journal_id,
        StateRootValue::application_journal_record_manifests(journal_id, &manifests)?,
        overlay,
    )?;
    Ok(())
}

fn validate_coupled_checkpoint_history<R: StateRootResolver + ?Sized>(
    roots: &StateRoots,
    machine_authority: &str,
    receipt: &crate::CoupledCheckpointReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    for journal in receipt.manifests() {
        for expected in &journal.records {
            let retained = load_application_journal_record_manifest_from_roots(
                roots,
                overlay,
                &journal.journal_id,
                &expected.record_id,
            )?;
            if retained.as_ref() != Some(expected) {
                return Err(DurableError::HistoryConflict {
                    code: "state_root_coupled_checkpoint_journal_history_conflict".to_owned(),
                    message: format!(
                        "coupled checkpoint {} does not match journal {} record {} history",
                        receipt.coupling_id, journal.journal_id, expected.record_id
                    ),
                });
            }
        }
    }
    let Some(checkpoint) = ResourceHandoffCheckpoint::from_receipt(receipt) else {
        return Ok(());
    };
    if checkpoint.machine_authority_root != machine_authority {
        return Err(DurableError::HistoryConflict {
            code: "state_root_resource_activation_machine_mismatch".to_owned(),
            message: format!(
                "Resource activation {} does not bind the exact result Machine",
                checkpoint.activation_id
            ),
        });
    }
    checkpoint.verify_wait_and_continuation(roots, overlay)?;
    checkpoint.verify_command_receipts(roots, overlay)
}

struct ResourceHandoffCheckpoint<'a> {
    machine_authority_root: &'a str,
    transfer_id: &'a str,
    activation_id: &'a str,
    resource_command_id: &'a str,
    source_receipt_id: &'a str,
    run_id: &'a str,
    owner: &'a cymule_durable_protocol::WaitOwner,
    wait_id: &'a str,
    result: &'a cymule_core::ArtifactRef,
    continuation_digest: &'a str,
    receipt_id: &'a str,
}

impl<'a> ResourceHandoffCheckpoint<'a> {
    fn from_receipt(receipt: &'a crate::CoupledCheckpointReceipt) -> Option<Self> {
        let crate::CoupledCheckpoint::ResourceHandoffInput {
            machine_authority_root,
            transfer_id,
            activation_id,
            resource_command_id,
            source_receipt_id,
            run_id,
            owner,
            wait_id,
            result,
            continuation_digest,
        } = &receipt.checkpoint
        else {
            return None;
        };
        Some(Self {
            machine_authority_root,
            transfer_id,
            activation_id,
            resource_command_id,
            source_receipt_id,
            run_id,
            owner,
            wait_id,
            result,
            continuation_digest,
            receipt_id: &receipt.receipt_id,
        })
    }

    fn verify_wait_and_continuation<R: StateRootResolver + ?Sized>(
        &self,
        roots: &StateRoots,
        overlay: &mut ObjectOverlay<'_, R>,
    ) -> DurableResult<()> {
        let Self {
            activation_id,
            run_id,
            owner,
            wait_id,
            result,
            continuation_digest,
            ..
        } = *self;
        let wait: crate::WaitCondition = map_get(&roots.waits, wait_id, overlay)?
            .ok_or_else(|| DurableError::HistoryConflict {
                code: "state_root_resource_activation_wait_missing".to_owned(),
                message: format!(
                    "Resource activation {activation_id} did not retain Wait {wait_id}"
                ),
            })?
            .decode(StateRootLeafKind::Wait)?;
        wait.verify_wire()?;
        if wait.run_id != *run_id
            || wait.owner != *owner
            || wait.state != crate::WaitState::Completed
            || wait.result.as_ref() != Some(result)
        {
            return Err(DurableError::HistoryConflict {
                code: "state_root_resource_activation_wait_mismatch".to_owned(),
                message: format!(
                    "Resource activation {activation_id} does not match its exact completed Wait"
                ),
            });
        }
        let continuation: crate::Continuation = map_get(&roots.continuations, run_id, overlay)?
            .ok_or_else(|| DurableError::HistoryConflict {
                code: "state_root_resource_activation_continuation_missing".to_owned(),
                message: format!(
                    "Resource activation {activation_id} did not retain Continuation {run_id}"
                ),
            })?
            .decode(StateRootLeafKind::Continuation)?;
        continuation.verify_wire()?;
        if continuation.run_id != *run_id
            || continuation.wait_set.contains(wait_id)
            || cymule_core::canonical_digest(&continuation)? != *continuation_digest
        {
            return Err(DurableError::HistoryConflict {
                code: "state_root_resource_activation_continuation_mismatch".to_owned(),
                message: format!(
                    "Resource activation {activation_id} does not match its exact resulting Continuation"
                ),
            });
        }
        Ok(())
    }

    fn verify_command_receipts<R: StateRootResolver + ?Sized>(
        &self,
        roots: &StateRoots,
        overlay: &mut ObjectOverlay<'_, R>,
    ) -> DurableResult<()> {
        let Self {
            activation_id,
            transfer_id,
            resource_command_id,
            source_receipt_id,
            run_id,
            wait_id,
            result,
            receipt_id,
            ..
        } = *self;
        let command: cymule_profile_protocol::resource::ResourceCommandReceipt = map_get(
            &roots.resource_command_receipts,
            resource_command_id,
            overlay,
        )?
        .ok_or_else(|| DurableError::HistoryConflict {
            code: "state_root_resource_activation_command_missing".to_owned(),
            message: format!(
                "Resource activation {activation_id} did not retain command {resource_command_id}"
            ),
        })?
        .decode(StateRootLeafKind::ResourceCommandReceipt)?;
        command.verify()?;
        let activation = match &command.outcome {
            cymule_profile_protocol::resource::ResourceCommandOutcome::ActivateTransfer {
                receipt: activation,
            } if command.command.command_id == *resource_command_id
                && activation.activation.activation_id == *activation_id
                && activation.activation.transfer_id == *transfer_id
                && activation.activation.to_run == *run_id
                && activation.activation.wait_id == *wait_id
                && activation.activation.result == *result
                && activation.source_receipt_id == *source_receipt_id
                && activation.coupled_wait_receipt_id == receipt_id =>
            {
                activation
            }
            _ => {
                return Err(DurableError::HistoryConflict {
                    code: "state_root_resource_activation_command_mismatch".to_owned(),
                    message: format!(
                        "Resource activation {activation_id} does not match its exact typed command receipt"
                    ),
                });
            }
        };
        let current: cymule_profile_protocol::resource::ResourceHandoffActivationCurrent = map_get(
            &roots.resource_handoff_activation_current,
            activation_id,
            overlay,
        )?
        .ok_or_else(|| DurableError::HistoryConflict {
            code: "state_root_resource_activation_current_missing".to_owned(),
            message: format!(
                "Resource activation {activation_id} did not retain its exact current authority"
            ),
        })?
        .decode(StateRootLeafKind::ResourceHandoffActivationCurrent)?;
        current.verify()?;
        if current.receipt != *activation {
            return Err(DurableError::HistoryConflict {
                code: "state_root_resource_activation_current_mismatch".to_owned(),
                message: format!(
                    "Resource activation {activation_id} current authority does not match its receipt"
                ),
            });
        }
        let source: cymule_profile_protocol::resource::ResourceCommandReceipt = map_get(
            &roots.resource_command_receipts,
            source_receipt_id,
            overlay,
        )?
        .ok_or_else(|| DurableError::HistoryConflict {
            code: "state_root_resource_activation_source_missing".to_owned(),
            message: format!(
                "Resource activation {activation_id} lost source receipt {source_receipt_id}"
            ),
        })?
        .decode(StateRootLeafKind::ResourceCommandReceipt)?;
        source.verify()?;
        if !matches!(
            source.outcome,
            cymule_profile_protocol::resource::ResourceCommandOutcome::Transfer {
                receipt: source
            } if source.receipt_id == *source_receipt_id
                && source.handoff.transfer_id == *transfer_id
                && source.handoff.to_run == *run_id
                && source.handoff.resource == *result
        ) {
            return Err(DurableError::HistoryConflict {
                code: "state_root_resource_activation_source_mismatch".to_owned(),
                message: format!(
                    "Resource activation {activation_id} does not consume its exact transfer receipt"
                ),
            });
        }
        Ok(())
    }
}

struct JournalRootEvidence {
    record_count: u64,
    first: crate::JournalRecord,
    last: crate::JournalRecord,
    ordered_root: String,
}

fn journal_root_evidence<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<JournalRootEvidence> {
    root.verify()?;
    if root.len == 0 {
        return Err(DurableError::Validation(
            "application-journal prefix evidence cannot be empty".to_owned(),
        ));
    }
    let first = log_value_at(root, 0, overlay)?
        .decode::<crate::JournalRecord>(StateRootLeafKind::JournalRecord)?;
    let last = log_value_at(root, root.len - 1, overlay)?
        .decode::<crate::JournalRecord>(StateRootLeafKind::JournalRecord)?;
    Ok(JournalRootEvidence {
        record_count: root.len,
        first,
        last,
        ordered_root: root.ordered_root.clone(),
    })
}

#[cfg(test)]
fn preview_journal_replacement_roots<R: StateRootResolver + ?Sized>(
    current: &LogRoot,
    count: u64,
    replacement: &[crate::JournalRecord],
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<(LogRoot, JournalRootEvidence, LogRoot, JournalRootEvidence)> {
    if count == 0 || count > current.len || replacement.is_empty() {
        return Err(DurableError::Validation(
            "application-journal replacement has an invalid prefix or replacement".to_owned(),
        ));
    }
    let (prefix, _) = split_log_root(current, count, overlay)?;
    let expected = journal_root_evidence(&prefix, overlay)?;
    let replacement_ids = replacement
        .iter()
        .map(|record| StateRootValue::encode(StateRootLeafKind::JournalRecord, record))
        .map(|value| value.and_then(|value| overlay.insert_value(value)))
        .collect::<DurableResult<Vec<_>>>()?;
    let result_root = apply_log_mutation(
        current,
        LogMutation::replace_prefix(count, prefix.clone(), replacement_ids),
        overlay,
    )?;
    let result = journal_root_evidence(&result_root, overlay)?;
    Ok((prefix, expected, result_root, result))
}

/// Preview the exact current and result prefix descriptors for one replacement.
///
/// The resolver must remain pinned to `manifest`; no object is published.
#[cfg(test)]
pub(crate) fn preview_application_journal_replacement<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    journal_id: &str,
    count: u64,
    replacement: &[crate::JournalRecord],
) -> DurableResult<(
    crate::ApplicationJournalPrefix,
    crate::ApplicationJournalPrefix,
)> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let current = map_get(
        &manifest.roots.application_journals,
        journal_id,
        &mut overlay,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!("application journal {journal_id} does not exist"))
    })?
    .decode_application_journal_root(journal_id)?;
    let (_, expected, _, result) =
        preview_journal_replacement_roots(&current, count, replacement, &mut overlay)?;
    let expected = crate::ApplicationJournalPrefix::from_state_log_evidence(
        expected.record_count,
        &expected.first,
        &expected.last,
        expected.ordered_root,
    )?;
    let result = crate::ApplicationJournalPrefix::from_state_log_evidence(
        result.record_count,
        &result.first,
        &result.last,
        result.ordered_root,
    )?;
    Ok((expected, result))
}

/// Load one exact non-empty prefix descriptor from a pinned journal root.
///
/// # Errors
///
/// Rejects invalid pinned authority, absent journals, invalid prefixes, or storage failures.
pub fn load_application_journal_prefix<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &mut R,
    journal_id: &str,
    count: u64,
) -> DurableResult<crate::ApplicationJournalPrefix> {
    manifest.verify()?;
    ensure_resolver_pinned(manifest, resolver)?;
    let mut overlay = ObjectOverlay::new(resolver);
    let current = map_get(
        &manifest.roots.application_journals,
        journal_id,
        &mut overlay,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!("application journal {journal_id} does not exist"))
    })?
    .decode_application_journal_root(journal_id)?;
    let (prefix, _) = split_log_root(&current, count, &mut overlay)?;
    let evidence = journal_root_evidence(&prefix, &mut overlay)?;
    crate::ApplicationJournalPrefix::from_state_log_evidence(
        evidence.record_count,
        &evidence.first,
        &evidence.last,
        evidence.ordered_root,
    )
}

#[cfg(test)]
fn replace_application_journal_prefix<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    receipt: &crate::ApplicationJournalPrefixReplacementReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    receipt.verify()?;
    let replacement = &receipt.replacement;
    let current = map_get(
        &roots.application_journals,
        &replacement.journal_id,
        overlay,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!(
            "application journal {} does not exist",
            replacement.journal_id
        ))
    })?
    .decode_application_journal_root(&replacement.journal_id)?;
    let count = replacement.expected_prefix.record_count;
    if count > current.len {
        return Err(DurableError::HistoryConflict {
            code: "state_root_journal_prefix_length_mismatch".to_owned(),
            message: "application journal replacement exceeds the rooted journal".to_owned(),
        });
    }
    let (_removed_root, expected_evidence, next, result_evidence) =
        preview_journal_replacement_roots(&current, count, &replacement.replacement, overlay)?;
    let expected_prefix = crate::ApplicationJournalPrefix::from_state_log_evidence(
        expected_evidence.record_count,
        &expected_evidence.first,
        &expected_evidence.last,
        expected_evidence.ordered_root,
    )?;
    if expected_prefix != replacement.expected_prefix {
        return Err(DurableError::HistoryConflict {
            code: "state_root_journal_prefix_mismatch".to_owned(),
            message: "application journal replacement does not match its exact rooted prefix"
                .to_owned(),
        });
    }
    let result_prefix = crate::ApplicationJournalPrefix::from_state_log_evidence(
        result_evidence.record_count,
        &result_evidence.first,
        &result_evidence.last,
        result_evidence.ordered_root,
    )?;
    if result_prefix != receipt.result {
        return Err(DurableError::Integrity {
            code: "state_root_journal_result_prefix_mismatch".to_owned(),
            message: "journal replacement receipt does not bind the actual result root".to_owned(),
        });
    }
    let mut manifests = match map_get(
        &roots.application_journal_record_manifests,
        &replacement.journal_id,
        overlay,
    )? {
        Some(value) => value.decode_record_manifest_root(&replacement.journal_id)?,
        None => MapRoot::empty(),
    };
    for record in &replacement.replacement {
        if map_get(&manifests, &record.record_id, overlay)?.is_some() {
            return Err(DurableError::HistoryConflict {
                code: "state_root_journal_replacement_record_reuse".to_owned(),
                message: format!(
                    "application journal replacement record {} was already admitted",
                    record.record_id
                ),
            });
        }
        let manifest = crate::JournalRecordManifest::from_record(record)?;
        manifests = map_put(
            &manifests,
            &record.record_id,
            StateRootValue::encode(StateRootLeafKind::JournalRecordManifest, &manifest)?,
            overlay,
        )?;
    }
    roots.application_journal_record_manifests = map_put(
        &roots.application_journal_record_manifests,
        &replacement.journal_id,
        StateRootValue::application_journal_record_manifests(&replacement.journal_id, &manifests)?,
        overlay,
    )?;
    if next != current {
        roots.application_journals = map_put(
            &roots.application_journals,
            &replacement.journal_id,
            StateRootValue::application_journal(&replacement.journal_id, &next)?,
            overlay,
        )?;
    }
    retain_journal_replacement_authority(roots, receipt, overlay)
}

#[cfg(test)]
fn retain_journal_replacement_authority<R: StateRootResolver + ?Sized>(
    roots: &mut StateRoots,
    receipt: &crate::ApplicationJournalPrefixReplacementReceipt,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let replacement = &receipt.replacement;
    put_typed_value(
        &mut roots.application_journal_prefix_replacements,
        &replacement.journal_id,
        StateRootLeafKind::JournalPrefixReplacement,
        receipt,
        overlay,
    )?;
    let authority = crate::ApplicationJournalPrefixReplacementAuthority::new(receipt)?;
    insert_immutable_typed_value(
        &mut roots.application_journal_prefix_replacement_history,
        &replacement.replacement_id,
        StateRootLeafKind::JournalPrefixReplacementAuthority,
        &authority,
        "application journal prefix replacement",
        overlay,
    )?;
    Ok(())
}

fn log_value_at<R: StateRootResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<StateRootValue> {
    if index >= root.len {
        return Err(DurableError::NotFound(format!(
            "persistent-log index {index} is outside length {}",
            root.len
        )));
    }
    log_get(root, index, overlay)
}

#[derive(Serialize)]
struct DurableRevisionState<'a> {
    durable_version: &'a str,
    machine_snapshot_version: &'a str,
    machine_frontier: &'a cymule_core::durable_internal::MachineAuthorityFrontier,
    machine_base_anchor: Option<&'a cymule_core::MachineBaseAnchor>,
    roots: &'a StateRoots,
}

#[derive(Serialize)]
struct DurableRevisionLineage<'a> {
    parent_revision: &'a str,
    delta_digest: &'a str,
    sequence: u64,
}

fn derive_genesis_revision(state: DurableRevisionState<'_>) -> DurableResult<String> {
    cymule_core::content_id(
        DURABLE_REVISION_VERSION,
        &DurableRevisionPreimage::Genesis { sequence: 0, state },
    )
    .map_err(Into::into)
}

fn derive_transition_revision(
    lineage: DurableRevisionLineage<'_>,
    state: DurableRevisionState<'_>,
) -> DurableResult<String> {
    cymule_core::content_id(
        DURABLE_REVISION_VERSION,
        &DurableRevisionPreimage::Transition { lineage, state },
    )
    .map_err(Into::into)
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum DurableRevisionPreimage<'a> {
    Genesis {
        sequence: u64,
        state: DurableRevisionState<'a>,
    },
    Transition {
        lineage: DurableRevisionLineage<'a>,
        state: DurableRevisionState<'a>,
    },
}

fn machine_authority_root(
    snapshot: &cymule_core::MachineSnapshot,
    anchor: Option<&cymule_core::MachineBaseAnchor>,
) -> DurableResult<String> {
    let machine = match anchor {
        Some(anchor) => cymule_core::Machine::restore_anchored(snapshot.clone(), anchor)?,
        None => cymule_core::Machine::restore(snapshot.clone())?,
    };
    machine.authority_root().map_err(Into::into)
}

fn decode_value_map<T>(
    values: BTreeMap<String, StateRootValue>,
    kind: StateRootLeafKind,
) -> DurableResult<BTreeMap<String, T>>
where
    T: DeserializeOwned + Serialize,
{
    values
        .into_iter()
        .map(|(key, value)| value.decode(kind).map(|value| (key, value)))
        .collect()
}

fn decode_value_vec<T>(
    values: Vec<StateRootValue>,
    kind: StateRootLeafKind,
) -> DurableResult<Vec<T>>
where
    T: DeserializeOwned + Serialize,
{
    values.into_iter().map(|value| value.decode(kind)).collect()
}

fn decode_family_map<T>(
    collections: &BTreeMap<StateRootFamily, BTreeMap<String, StateRootValue>>,
    family: StateRootFamily,
    kind: StateRootLeafKind,
) -> DurableResult<BTreeMap<String, T>>
where
    T: DeserializeOwned + Serialize,
{
    decode_value_map(
        collections
            .get(&family)
            .expect("fixed state-root family was materialized")
            .clone(),
        kind,
    )
}

fn materialize_application_journals<R: StateRootResolver + ?Sized>(
    collections: &BTreeMap<StateRootFamily, BTreeMap<String, StateRootValue>>,
    resolver: &mut R,
) -> DurableResult<BTreeMap<String, crate::ApplicationJournal>> {
    collections
        .get(&StateRootFamily::ApplicationJournals)
        .expect("fixed application-journal family was materialized")
        .iter()
        .map(|(journal_id, value)| {
            let root = value.decode_application_journal_root(journal_id)?;
            materialize_state_log(&root, resolver)
                .and_then(|values| decode_value_vec(values, StateRootLeafKind::JournalRecord))
                .and_then(crate::ApplicationJournal::try_from_records)
                .map(|records| (journal_id.clone(), records))
        })
        .collect()
}

fn validate_digest(kind: &str, value: &str) -> DurableResult<()> {
    if value.len() == 64 && is_lower_hex(value) {
        Ok(())
    } else {
        Err(DurableError::Validation(format!(
            "{kind} must be a 64-character lowercase SHA-256 digest"
        )))
    }
}

fn ensure_resolver_pinned<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    resolver: &R,
) -> DurableResult<()> {
    if resolver.pinned_manifest_id() != manifest.manifest_id {
        return Err(DurableError::Integrity {
            code: "state_root_resolver_snapshot_mismatch".to_owned(),
            message: format!(
                "state-root resolver is pinned to {}, expected {}",
                resolver.pinned_manifest_id(),
                manifest.manifest_id
            ),
        });
    }
    Ok(())
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compaction_source_fixture() -> (StateRootManifest, TestResolver) {
        let genesis = StateRootManifest::genesis(&crate::DurableState::new(
            cymule_core::Machine::new().snapshot(),
        ))
        .expect("empty compaction source initializes");
        let mut manifest = genesis.manifest;
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(genesis.objects);
        let source = started_core_machine().snapshot();
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            "material:compaction-source".to_owned(),
            source.plans,
            source.artifacts,
        )
        .expect("compaction source material seals");
        install_material_transition(
            &mut manifest,
            &mut resolver,
            &material,
            vec![crate::DurableOperation::AppendJournal {
                journal_id: "journal:compaction-source".to_owned(),
                records: vec![record(0)],
            }],
        );
        (manifest, resolver)
    }

    fn prepare_test_compaction(
        manifest: &StateRootManifest,
        resolver: &mut TestResolver,
        compaction_id: &str,
    ) -> pinned_machine::PinnedMachineStagedMutation {
        pinned_machine::PinnedMachineView::open(manifest, resolver)
            .expect("compaction source pins")
            .prepare_history_compaction(&crate::HistoryCompactionRequest {
                compaction_id: compaction_id.to_owned(),
                expected_revision: manifest.revision.clone(),
                kind: crate::HistoryCompactionKind::EventFreeAdmissions,
                requested_suffix: 0,
            })
            .expect("real production compaction prepares")
    }

    fn install_test_compaction(
        manifest: &mut StateRootManifest,
        resolver: &mut TestResolver,
        compaction_id: &str,
    ) -> crate::HistoryCompactionReceipt {
        let stage = prepare_test_compaction(manifest, resolver, compaction_id);
        let receipt = stage
            .compaction_receipt()
            .expect("compaction owns receipt")
            .clone();
        let delta = crate::DurableDelta::new(vec![crate::DurableOperation::PutHistoryCompaction {
            value: receipt.clone(),
        }])
        .expect("receipt sidecar seals");
        let (_, transition) = stage
            .finish(manifest, Some(&delta), resolver)
            .expect("real compaction and receipt close together")
            .into_parts();
        resolver.insert_all(transition.objects);
        manifest.clone_from(&transition.manifest);
        resolver.pinned.clone_from(&manifest.manifest_id);
        receipt
    }

    #[test]
    fn compaction_source_ignores_unavailable_profile_history() {
        let (manifest, mut resolver) = compaction_source_fixture();
        let journal = manifest
            .roots
            .application_journals
            .node
            .as_ref()
            .expect("profile root exists");
        resolver.objects.remove(journal);
        let manifests = manifest
            .roots
            .application_journal_record_manifests
            .node
            .as_ref()
            .expect("profile manifests exist");
        resolver.objects.remove(manifests);
        let parts = load_machine_compaction_source(&manifest, &mut resolver)
            .expect("Core source does not read profile roots");
        assert!(!parts.plans.is_empty());
        assert!(!parts.artifacts.is_empty());
        assert_eq!(parts.batches.len(), 1);
        let snapshot = cymule_core::MachineSnapshot::from_root_parts(parts)
            .expect("selected Core source fully audits");
        assert_eq!(snapshot.batches.len(), 1);
        assert!(snapshot.events.is_empty());
    }

    #[test]
    fn compaction_source_rejects_missing_reachable_core_values() {
        let (manifest, mut resolver) = compaction_source_fixture();
        let plans = manifest
            .roots
            .machine_plans
            .node
            .as_ref()
            .expect("Core Plan root exists");
        resolver.objects.remove(plans);
        assert!(matches!(
            load_machine_compaction_source(&manifest, &mut resolver),
            Err(DurableError::Integrity { .. })
        ));
    }

    #[test]
    fn compaction_source_rejects_pending_roots_before_object_io() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let transition_id = cymule_core::content_id("test.pending-compaction/1", &0)
            .expect("pending identity derives");
        let pending = map_put(
            &MapRoot::empty(),
            "command:pending",
            StateRootValue::machine_pending_command("command:pending".to_owned(), transition_id)
                .expect("pending value verifies"),
            &mut overlay,
        )
        .expect("pending map builds");
        let mut frontier = empty_machine_frontier();
        frontier.pending_commands = pending.clone();
        frontier.paged_transitions = pending;
        let roots = StateRoots::empty();
        let revision = derive_genesis_revision(revision_state(&frontier, &roots))
            .expect("pending source revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("authenticated pending roots seal");
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        assert!(
            matches!(load_machine_compaction_source(&manifest, &mut resolver),
                Err(DurableError::HistoryConflict { code, .. }) if code == "state_root_machine_compaction_pending_transition")
        );
        assert_eq!(resolver.loads, 0);
    }

    #[test]
    fn compaction_receipt_requires_its_real_core_stage() {
        let (manifest, mut resolver) = compaction_source_fixture();
        let stage = prepare_test_compaction(&manifest, &mut resolver, "compact:detached");
        let receipt = stage
            .compaction_receipt()
            .expect("real receipt derives")
            .clone();
        let delta = crate::DurableDelta::new(vec![crate::DurableOperation::PutHistoryCompaction {
            value: receipt,
        }])
        .expect("receipt delta seals");
        assert!(matches!(manifest.apply(&delta, &mut resolver),
            Err(DurableError::Integrity { code, .. }) if code == "state_root_history_compaction_stage_missing"));
    }

    #[test]
    fn compaction_head_tracks_latest_value_without_hiding_historical_receipts() {
        let (mut manifest, mut resolver) = compaction_source_fixture();
        let first = install_test_compaction(&mut manifest, &mut resolver, "compact:first");
        let first_head = manifest
            .roots
            .history_compaction_head
            .clone()
            .expect("first head exists");
        let bytes = b"next material".to_vec();
        let reference = cymule_core::artifact_ref("test.compaction-material/1", &bytes)
            .expect("next Artifact derives");
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            "material:compaction-second".to_owned(),
            Vec::new(),
            vec![cymule_core::ArtifactRecord { reference, bytes }],
        )
        .expect("next material seals");
        install_material_transition(
            &mut manifest,
            &mut resolver,
            &material,
            vec![crate::DurableOperation::AppendJournal {
                journal_id: "journal:compaction-source".to_owned(),
                records: vec![record(1)],
            }],
        );
        let second = install_test_compaction(&mut manifest, &mut resolver, "compact:second");
        assert_eq!(
            second.parent_compaction.as_deref(),
            Some(first.compaction_id.as_str())
        );
        assert_eq!(
            load_history_compaction_receipt(&manifest, &mut resolver, &first.compaction_id)
                .expect("historical receipt resolves"),
            Some(first)
        );
        assert_eq!(
            load_parent_history_compaction_receipt(&manifest, &mut resolver)
                .expect("current parent resolves"),
            Some(second)
        );
        let current_head = manifest
            .roots
            .history_compaction_head
            .clone()
            .expect("current head exists");
        assert_ne!(current_head, first_head);
        let reachable = reachable_state_root_objects(&manifest, &mut resolver)
            .expect("compaction graph fully audits");
        assert!(reachable.contains(&first_head));
        assert!(reachable.contains(&current_head));
        resolver.objects.remove(&current_head);
        assert!(matches!(
            load_parent_history_compaction_receipt(&manifest, &mut resolver),
            Err(DurableError::Integrity { .. })
        ));
    }

    #[test]
    fn compaction_head_is_required_nullable_and_paired_with_base() {
        let roots = StateRoots::empty();
        let mut wire = serde_json::to_value(&roots).expect("roots encode");
        assert!(wire["history_compaction_head"].is_null());
        wire.as_object_mut()
            .expect("roots are an object")
            .remove("history_compaction_head");
        assert!(
            cymule_core::decode_json::<StateRoots>(
                &cymule_core::canonical_bytes(&wire).expect("wire is canonical")
            )
            .is_err()
        );
        let mut unpaired = roots;
        unpaired.history_compaction_head = Some(
            cymule_core::content_id("test.compaction-head/1", &0).expect("head identity derives"),
        );
        assert!(
            matches!(unpaired.verify(), Err(DurableError::Integrity { code, .. })
            if code == "state_root_history_compaction_head_presence_mismatch")
        );
    }

    #[test]
    fn inactive_run_query_membership_preserves_parent_root_and_overlay() {
        let mut resolver = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut resolver);
        let mut indexes = MapRoot::empty();
        ensure_run_query_indexes(&mut indexes, "run:inactive-lease", &mut overlay)
            .expect("Run query descriptor initializes");
        let original_root = indexes.clone();
        let original_pending = overlay.pending.clone();
        remove_run_query_item(
            &mut indexes,
            "run:inactive-lease",
            RunQueryIndexKind::ActiveLeases,
            "lease:absent",
            &mut overlay,
        )
        .expect("absent derived membership is unchanged");
        assert_eq!(indexes, original_root);
        assert_eq!(overlay.pending, original_pending);
    }

    #[test]
    fn identical_run_query_membership_preserves_parent_root_and_overlay() {
        let mut resolver = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut resolver);
        let mut indexes = MapRoot::empty();
        let lease = crate::CoordinationLease {
            resource: "lease:unchanged".to_owned(),
            owner: "worker:unchanged".to_owned(),
            epoch: 1,
            expires_at: 10,
        };
        put_run_query_item(
            &mut indexes,
            "run:unchanged-lease",
            RunQueryIndexKind::ActiveLeases,
            &lease.resource,
            StateRootLeafKind::Lease,
            &lease,
            &mut overlay,
        )
        .expect("derived membership initializes");
        let original_root = indexes.clone();
        let original_pending = overlay.pending.clone();
        put_run_query_item(
            &mut indexes,
            "run:unchanged-lease",
            RunQueryIndexKind::ActiveLeases,
            &lease.resource,
            StateRootLeafKind::Lease,
            &lease,
            &mut overlay,
        )
        .expect("identical derived membership is unchanged");
        assert_eq!(indexes, original_root);
        assert_eq!(overlay.pending, original_pending);
    }

    fn empty_machine_frontier() -> cymule_core::durable_internal::MachineAuthorityFrontier {
        cymule_core::durable_internal::MachineAuthorityFrontier::genesis(
            MapRoot::empty(),
            MapRoot::empty(),
            MapRoot::empty(),
            MapRoot::empty(),
        )
        .expect("empty Machine frontier derives")
    }

    fn revision_state<'a>(
        frontier: &'a cymule_core::durable_internal::MachineAuthorityFrontier,
        roots: &'a StateRoots,
    ) -> DurableRevisionState<'a> {
        DurableRevisionState {
            durable_version: crate::DURABLE_STATE_VERSION,
            machine_snapshot_version: cymule_core::MachineSnapshot::VERSION,
            machine_frontier: frontier,
            machine_base_anchor: None,
            roots,
        }
    }

    fn claimed_dispatch(run_id: &str, owner: &str, epoch: u64) -> crate::EffectDispatch {
        let intent_id =
            cymule_core::content_id("cymule.test.state-root-effect/1", &(run_id, owner, epoch))
                .expect("test Effect identity derives");
        let input = cymule_core::artifact_ref(cymule_core::EFFECT_ARGS_ARTIFACT_KIND, b"{}")
            .expect("test Effect input derives");
        let execution_binding =
            cymule_core::artifact_ref(cymule_core::EXECUTION_BINDING_ARTIFACT_KIND, b"binding")
                .expect("test execution binding derives");
        let dispatch = crate::EffectDispatch {
            intent_id,
            run_id: run_id.to_owned(),
            origin_plan_id: cymule_core::content_id("cymule.test.state-root-plan/1", &run_id)
                .expect("test Plan identity derives"),
            operation: "test.effect".to_owned(),
            input,
            execution_binding,
            occurrence_binding: cymule_core::content_id(
                "cymule.test.state-root-occurrence-binding/1",
                &(run_id, owner),
            )
            .expect("test occurrence binding derives"),
            execution_availability: cymule_core::EffectExecutionAvailability::Available,
            reconciliation: cymule_core::ReconciliationState::NotRequired,
            state: crate::OutboxState::Claimed,
            claim_epoch: epoch,
            claim_owner: Some(owner.to_owned()),
            result: None,
        };
        dispatch.verify_wire().expect("test dispatch verifies");
        dispatch
    }

    fn pending_dispatch(run_id: &str) -> crate::EffectDispatch {
        let mut dispatch = claimed_dispatch(run_id, "worker:pending", 1);
        dispatch.state = crate::OutboxState::Pending;
        dispatch.claim_owner = None;
        dispatch.claim_epoch = 0;
        dispatch.verify_wire().expect("pending dispatch verifies");
        dispatch
    }

    fn insert_raw_value(
        resolver: &mut TestResolver,
        value: StateRootValue,
    ) -> (String, StateRootObject) {
        let object_id = state_root_value_id(&value).expect("raw test value identity derives");
        let object = StateRootObject::Value(StateValueObject {
            value_version: STATE_ROOT_VALUE_VERSION.to_owned(),
            object_id: object_id.clone(),
            value,
        });
        resolver.objects.insert(object_id.clone(), object.clone());
        (object_id, object)
    }

    fn insert_raw_map(resolver: &mut TestResolver, entries: Vec<(String, String)>) -> MapRoot {
        let (root, nodes) = cymule_authenticated_collections::build_map(entries)
            .expect("raw test map builds")
            .into_parts();
        for node in nodes {
            let object = StateRootObject::MapNode(node);
            resolver
                .objects
                .insert(object.object_id().to_owned(), object);
        }
        root
    }

    fn tampered_effect_manifest(
        dispatch: &crate::EffectDispatch,
    ) -> (StateRootManifest, TestResolver, StateRootObject) {
        let mut resolver = TestResolver::default();
        let malformed = StateRootValue::Leaf {
            kind: StateRootLeafKind::Outbox,
            canonical_json: String::from_utf8(
                cymule_core::canonical_bytes(dispatch).expect("tampered dispatch canonicalizes"),
            )
            .expect("canonical dispatch is UTF-8"),
        };
        let (dispatch_value_id, malformed_object) = insert_raw_value(&mut resolver, malformed);
        let effects = insert_raw_map(
            &mut resolver,
            vec![(dispatch.intent_id.clone(), dispatch_value_id)],
        );

        let descriptor = StateRootValue::run_query_indexes(
            &dispatch.run_id,
            RunQueryIndexRoots {
                effects,
                ..RunQueryIndexRoots::default()
            },
        )
        .expect("tampered Effect query descriptor closes");
        let (descriptor_id, _) = insert_raw_value(&mut resolver, descriptor);
        let run_query_indexes = insert_raw_map(
            &mut resolver,
            vec![(dispatch.run_id.clone(), descriptor_id)],
        );

        let owner = OutboxOwner {
            intent_id: dispatch.intent_id.clone(),
            run_id: dispatch.run_id.clone(),
        };
        owner.verify().expect("tampered Effect owner verifies");
        let owner_value = StateRootValue::encode(StateRootLeafKind::OutboxOwner, &owner)
            .expect("Effect owner value closes");
        let (owner_id, _) = insert_raw_value(&mut resolver, owner_value);
        let outbox = insert_raw_map(&mut resolver, vec![(dispatch.intent_id.clone(), owner_id)]);

        let mut roots = StateRoots::empty();
        roots.run_query_indexes = run_query_indexes;
        roots.outbox = outbox;
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("tampered Effect manifest shape closes");
        resolver.pinned = manifest.manifest_id.clone();
        resolver.objects.insert(
            manifest.manifest_id.clone(),
            StateRootObject::Manifest(manifest.clone()),
        );
        (manifest, resolver, malformed_object)
    }

    #[test]
    fn effect_result_tamper_fails_leaf_reopen_exact_read_and_full_audit() {
        let mut missing = claimed_dispatch("run:effect-result-missing", "worker:effect", 1);
        missing.state = crate::OutboxState::Applied;
        let mut wrong_kind = missing.clone();
        wrong_kind.run_id = "run:effect-result-wrong-kind".to_owned();
        wrong_kind.intent_id = cymule_core::content_id(
            "cymule.test.state-root-effect-wrong-kind/1",
            &wrong_kind.run_id,
        )
        .expect("wrong-kind Effect identity derives");
        wrong_kind.result = Some(
            cymule_core::artifact_ref("cymule.test.wrong-effect-result/1", b"null")
                .expect("wrong-kind result reference derives"),
        );

        for dispatch in [missing, wrong_kind] {
            let (manifest, mut resolver, malformed_object) = tampered_effect_manifest(&dispatch);
            let encoded = cymule_core::canonical_bytes(&malformed_object)
                .expect("malformed value object serializes");
            assert!(decode_state_root_object(&encoded).is_err());
            assert!(
                load_effect_dispatch(
                    &manifest,
                    &mut resolver,
                    Some(&dispatch.run_id),
                    &dispatch.intent_id,
                )
                .is_err()
            );
            assert!(reachable_state_root_objects(&manifest, &mut resolver).is_err());
        }
    }

    #[test]
    fn full_audit_rejects_wait_summary_that_differs_from_complete_wait() {
        let wait = crate::WaitCondition {
            wait_id: cymule_core::content_id("cymule.test.wait-summary-audit/1", &())
                .expect("Wait identity derives"),
            run_id: "run:wait-summary-audit".to_owned(),
            kind: crate::WaitKind::Input {
                correlation: "input:wait-summary-audit".to_owned(),
                schema: serde_json::json!(true),
            },
            consume_once: true,
            owner: crate::WaitOwner {
                invocation_id: "invocation:wait-summary-audit".to_owned(),
                definition_id: "definition:wait-summary-audit".to_owned(),
                region_path: Vec::new(),
                site_id: "site:wait-summary-audit".to_owned(),
                step_index: 0,
                bind: None,
            },
            state: crate::WaitState::Cancelled,
            result: None,
        };
        wait.verify_wire().expect("complete Wait verifies");
        let mut summary = crate::DurableWaitSummary::from_wait(&wait);
        summary.state = crate::WaitState::Pending;
        summary
            .verify()
            .expect("tampered summary is independently valid");

        let mut resolver = TestResolver::default();
        let (wait_id, _) = insert_raw_value(
            &mut resolver,
            StateRootValue::encode(StateRootLeafKind::Wait, &wait)
                .expect("complete Wait leaf encodes"),
        );
        let waits = insert_raw_map(&mut resolver, vec![(wait.wait_id.clone(), wait_id)]);
        let (summary_id, _) = insert_raw_value(
            &mut resolver,
            StateRootValue::encode(StateRootLeafKind::WaitSummary, &summary)
                .expect("Wait summary leaf encodes"),
        );
        let summary_root = insert_raw_map(&mut resolver, vec![(wait.wait_id.clone(), summary_id)]);
        let descriptor = StateRootValue::run_query_indexes(
            &wait.run_id,
            RunQueryIndexRoots {
                waits: summary_root,
                ..RunQueryIndexRoots::default()
            },
        )
        .expect("Wait query descriptor closes");
        let (descriptor_id, _) = insert_raw_value(&mut resolver, descriptor);
        let run_query_indexes =
            insert_raw_map(&mut resolver, vec![(wait.run_id.clone(), descriptor_id)]);

        let mut roots = StateRoots::empty();
        roots.waits = waits;
        roots.run_query_indexes = run_query_indexes;
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("Wait summary tamper manifest closes");
        resolver.pinned = manifest.manifest_id.clone();
        resolver.objects.insert(
            manifest.manifest_id.clone(),
            StateRootObject::Manifest(manifest.clone()),
        );
        assert!(matches!(
            reachable_state_root_objects(&manifest, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_wait_summary_mismatch"
        ));
    }

    fn absent_agent_message_claim(
        message_id: &str,
    ) -> Vec<cymule_profile_protocol::agent::AgentTargetClaimSource> {
        vec![cymule_profile_protocol::agent::AgentTargetClaimSource {
            target: cymule_profile_protocol::agent::AgentTargetClaimTarget::Message {
                message_id: message_id.to_owned(),
            },
            current: None,
        }]
    }

    fn agent_message_fixture(
        count: u64,
    ) -> (
        StateRootManifest,
        TestResolver,
        cymule_profile_protocol::agent::AgentSessionCurrent,
        Vec<cymule_profile_protocol::agent::AgentMessageCurrent>,
    ) {
        use cymule_profile_protocol::agent::{
            AgentCommand, AgentCommandAction, AgentCommandOutcome, AgentCommandReceipt,
            AgentCommandSource, AgentMessage, AgentSessionCurrent, AgentSessionEntrySource,
            AgentSessionUpdateEffect, AgentSessionUpdateSource, AgentUpdate, ContentBlock,
            MessageRole,
        };

        let state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        let genesis = StateRootManifest::genesis(&state).expect("Agent genesis builds");
        let mut manifest = genesis.manifest;
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(genesis.objects);
        let mut session =
            AgentSessionCurrent::new("session:message-prefix").expect("Agent Session constructs");
        install_transition(
            &mut manifest,
            &mut resolver,
            &crate::DurableDelta::new(vec![crate::DurableOperation::PutAgentSessionCurrent {
                value: session.clone(),
            }])
            .expect("Agent Session delta seals"),
        );
        let mut messages = Vec::new();
        for index in 0..count {
            let update = AgentUpdate::Message {
                update_id: format!("update:message:{index}"),
                message: AgentMessage {
                    message_id: format!("message:{index}"),
                    role: MessageRole::Agent,
                    content: vec![ContentBlock::Text {
                        text: format!("message body {index}"),
                    }],
                },
            };
            let command = AgentCommand::new(
                manifest.revision.clone(),
                AgentCommandAction::SessionUpdate {
                    session_id: session.session_id.clone(),
                    update: update.clone(),
                },
            )
            .expect("Agent message command derives");
            let source_session = session.clone();
            let source = AgentSessionUpdateSource {
                update: None,
                entry: AgentSessionEntrySource::Message { current: None },
                target_claims: absent_agent_message_claim(&format!("message:{index}")),
            };
            let post = session
                .reduce_update(&command.command_id, &update, &source)
                .expect("Agent message reduces");
            let claim = cymule_profile_protocol::agent::agent_session_target_claim_transitions(
                &command,
                &source_session,
                &source,
                &post,
            )
            .expect("Agent message target claim derives")
            .into_iter()
            .next()
            .expect("Agent message materializes one target claim");
            let receipt = AgentCommandReceipt::new(
                &command,
                AgentCommandSource::Session {
                    session: source_session,
                    update: source.clone(),
                },
                AgentCommandOutcome::Session(post.clone()),
            )
            .expect("Agent message receipt derives");
            let AgentSessionUpdateEffect::Message { current } = post.effect else {
                panic!("Agent message update returned another effect")
            };
            install_transition(
                &mut manifest,
                &mut resolver,
                &crate::DurableDelta::new(vec![
                    crate::DurableOperation::PutAgentMessageCurrent {
                        value: current.clone(),
                    },
                    crate::DurableOperation::PutAgentSessionCurrent {
                        value: post.session.clone(),
                    },
                    crate::DurableOperation::PutAgentUpdateCurrent {
                        value: post.update.clone(),
                    },
                    crate::DurableOperation::ApplyAgentTargetClaim { value: claim },
                    crate::DurableOperation::PutAgentCommand { value: command },
                    crate::DurableOperation::PutAgentCommandReceipt {
                        value: Box::new(receipt),
                    },
                ])
                .expect("Agent message delta seals"),
            );
            messages.push(current);
            session = post.session;
        }
        (manifest, resolver, session, messages)
    }

    fn agent_message_query(
        manifest: &StateRootManifest,
        head: Option<String>,
        count: u64,
        end_exclusive: Option<u64>,
        max_entries: u64,
    ) -> cymule_profile_protocol::agent::AgentMessagePageQuery {
        cymule_profile_protocol::agent::AgentMessagePageQuery {
            session_id: "session:message-prefix".to_owned(),
            expected_message_head: head,
            source_message_count: count,
            end_exclusive,
            max_entries,
            max_message_canonical_bytes: cymule_profile_protocol::agent::MAX_AGENT_PAGE_BYTES
                as u64,
            max_canonical_bytes: cymule_profile_protocol::agent::MAX_AGENT_PAGE_BYTES as u64,
            expected_revision: Some(manifest.revision.clone()),
        }
    }

    fn agent_target_claim_value_id(resolver: &TestResolver) -> String {
        resolver
            .objects
            .iter()
            .find_map(|(object_id, object)| {
                matches!(
                    object,
                    StateRootObject::Value(value)
                        if matches!(
                            &value.value,
                            StateRootValue::Leaf {
                                kind: StateRootLeafKind::AgentTargetClaimCurrent,
                                ..
                            }
                        )
                )
                .then(|| object_id.clone())
            })
            .expect("Agent target-claim value object exists")
    }

    #[test]
    fn released_tool_claim_requires_its_inprogress_target_during_full_audit() {
        let phase = cymule_profile_protocol::agent::AgentTargetClaimPhase::Released {
            stream_id: "stream:released-tool-audit".to_owned(),
            reservation_id: format!("sha256:{}", "a".repeat(64)),
        };
        assert!(matches!(
            audit_unmaterialized_agent_tool(&phase, None),
            Err(DurableError::Integrity { ref code, .. })
                if code == "agent_target_claim_released_tool_missing"
        ));
    }

    #[test]
    fn reserved_tool_claim_requires_its_inprogress_target_during_full_audit() {
        let phase = cymule_profile_protocol::agent::AgentTargetClaimPhase::Reserved {
            stream_id: "stream:reserved-tool-audit".to_owned(),
            reservation_id: format!("sha256:{}", "b".repeat(64)),
        };
        assert!(matches!(
            audit_unmaterialized_agent_tool(&phase, None),
            Err(DurableError::Integrity { ref code, .. })
                if code == "agent_target_claim_reserved_tool_invalid"
        ));
        let pending = cymule_profile_protocol::agent::AgentToolCurrent {
            session_id: "session:reserved-tool-audit".to_owned(),
            tool: cymule_profile_protocol::agent::ToolCall {
                tool_call_id: "tool:reserved-tool-audit".to_owned(),
                operation: "test".to_owned(),
                status: cymule_profile_protocol::agent::ToolCallStatus::Pending,
                input: serde_json::Value::Null,
                output: None,
                locations: Vec::new(),
            },
            admitted_by: format!("sha256:{}", "c".repeat(64)),
        };
        pending
            .verify()
            .expect("Pending Tool current is valid alone");
        assert!(matches!(
            audit_unmaterialized_agent_tool(&phase, Some(&pending)),
            Err(DurableError::Integrity { ref code, .. })
                if code == "agent_target_claim_reserved_tool_invalid"
        ));
    }

    #[test]
    fn full_audit_and_reachability_reject_missing_or_tampered_target_claim() {
        let (manifest, mut missing, _, _) = agent_message_fixture(1);
        let missing_id = agent_target_claim_value_id(&missing);
        missing.objects.remove(&missing_id);
        assert!(matches!(
            reachable_state_root_objects(&manifest, &mut missing),
            Err(DurableError::Integrity { .. })
        ));

        let (manifest, mut tampered, _, _) = agent_message_fixture(1);
        let tampered_id = agent_target_claim_value_id(&tampered);
        let StateRootObject::Value(value) = tampered.objects.get_mut(&tampered_id).unwrap() else {
            unreachable!("selected target-claim object is a value")
        };
        let StateRootValue::Leaf { canonical_json, .. } = &mut value.value else {
            unreachable!("selected target-claim value is a leaf")
        };
        canonical_json.push(' ');
        assert!(matches!(
            reachable_state_root_objects(&manifest, &mut tampered),
            Err(DurableError::Integrity { .. })
        ));
    }

    #[test]
    fn agent_message_page_retains_old_prefix_after_later_append() {
        let (manifest, mut resolver, session, messages) = agent_message_fixture(4);
        let old_count = 3;
        let old_head = Some(messages[2].order.head.clone());
        assert_ne!(old_head, session.message_head);
        let full = load_agent_message_page(
            &manifest,
            &mut resolver,
            &agent_message_query(&manifest, old_head.clone(), old_count, None, 256),
        )
        .expect("old immutable message prefix reads after append");
        assert_eq!(full.page.entries, messages[..3]);

        let mut end = None;
        let mut one_at_a_time = Vec::new();
        loop {
            let page = load_agent_message_page(
                &manifest,
                &mut resolver,
                &agent_message_query(&manifest, old_head.clone(), old_count, end, 1),
            )
            .expect("one-entry immutable prefix page reads");
            one_at_a_time.extend(page.page.entries);
            end = page.page.next_end_exclusive;
            if end.is_none() {
                break;
            }
        }
        one_at_a_time.sort_by_key(|entry| entry.order.index);
        assert_eq!(one_at_a_time, full.page.entries);
        assert_eq!(
            one_at_a_time
                .iter()
                .map(|entry| cymule_core::canonical_bytes(entry)
                    .expect("message encodes")
                    .len())
                .sum::<usize>(),
            full.page
                .entries
                .iter()
                .map(|entry| cymule_core::canonical_bytes(entry)
                    .expect("message encodes")
                    .len())
                .sum::<usize>()
        );
        let empty = load_agent_message_page(
            &manifest,
            &mut resolver,
            &agent_message_query(&manifest, None, 0, None, 256),
        )
        .expect("empty immutable prefix remains readable");
        assert!(empty.page.entries.is_empty());
    }

    #[test]
    fn agent_message_page_rejects_wrong_source_and_missing_or_corrupt_entry() {
        let (manifest, mut resolver, _, messages) = agent_message_fixture(3);
        let wrong_head = Some(
            cymule_core::content_id("test.wrong-message-head/1", &()).expect("wrong head derives"),
        );
        assert!(matches!(
            load_agent_message_page(
                &manifest,
                &mut resolver,
                &agent_message_query(&manifest, wrong_head, 3, None, 256),
            ),
            Err(DurableError::HistoryConflict { .. })
        ));
        assert!(matches!(
            load_agent_message_page(
                &manifest,
                &mut resolver,
                &agent_message_query(
                    &manifest,
                    Some(messages[2].order.head.clone()),
                    4,
                    None,
                    256,
                ),
            ),
            Err(DurableError::HistoryConflict { .. })
        ));

        let encoded = StateRootValue::encode(StateRootLeafKind::AgentMessageCurrent, &messages[2])
            .expect("message leaf encodes");
        let object_id = state_root_value_id(&encoded).expect("message leaf identity derives");
        let retained = resolver
            .objects
            .remove(&object_id)
            .expect("source message value is reachable");
        assert!(matches!(
            load_agent_message_page(
                &manifest,
                &mut resolver,
                &agent_message_query(
                    &manifest,
                    Some(messages[2].order.head.clone()),
                    3,
                    None,
                    256,
                ),
            ),
            Err(DurableError::Integrity { .. })
        ));
        let StateRootObject::Value(mut corrupted) = retained else {
            panic!("message leaf was not stored as a value object")
        };
        corrupted.value =
            StateRootValue::encode(StateRootLeafKind::AgentMessageCurrent, &messages[1])
                .expect("foreign message leaf encodes");
        resolver
            .objects
            .insert(object_id, StateRootObject::Value(corrupted));
        assert!(matches!(
            load_agent_message_page(
                &manifest,
                &mut resolver,
                &agent_message_query(
                    &manifest,
                    Some(messages[2].order.head.clone()),
                    3,
                    None,
                    256,
                ),
            ),
            Err(DurableError::Integrity { .. })
        ));
    }

    #[test]
    fn agent_message_page_enforces_entry_and_whole_read_budgets_independently() {
        let (manifest, mut resolver, session, messages) = agent_message_fixture(1);
        let mut message_limited = agent_message_query(
            &manifest,
            session.message_head.clone(),
            session.message_count,
            None,
            1,
        );
        message_limited.max_message_canonical_bytes = u64::try_from(
            cymule_core::canonical_bytes(&messages[0])
                .expect("message encodes")
                .len()
                - 1,
        )
        .expect("message bound fits");
        assert!(matches!(
            load_agent_message_page(&manifest, &mut resolver, &message_limited),
            Err(DurableError::Validation(message))
                if message.contains("message budget")
        ));

        let mut wire_limited = agent_message_query(
            &manifest,
            session.message_head,
            session.message_count,
            None,
            1,
        );
        wire_limited.max_canonical_bytes = 1;
        assert!(matches!(
            load_agent_message_page(&manifest, &mut resolver, &wire_limited),
            Err(DurableError::Validation(message))
                if message.contains("byte budget")
        ));
    }

    #[test]
    fn run_local_outbox_keeps_only_an_immutable_global_owner() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let mut roots = StateRoots::empty();
        let mut dispatch = pending_dispatch("run:local-outbox");
        StateRootSidecarWriter {
            roots: &mut roots,
            overlay: &mut overlay,
        }
        .put_outbox(&dispatch)
        .expect("pending outbox publishes");
        let owner_root = roots.outbox.clone();
        let owner_value = map_get(&owner_root, &dispatch.intent_id, &mut overlay)
            .expect("owner resolves")
            .expect("owner exists");
        let owner: OutboxOwner = owner_value
            .decode(StateRootLeafKind::OutboxOwner)
            .expect("global entry is only an owner");
        assert_eq!(owner.run_id, dispatch.run_id);
        assert!(
            owner_value
                .decode::<crate::EffectDispatch>(StateRootLeafKind::Outbox)
                .is_err()
        );
        dispatch.state = crate::OutboxState::CancelledBeforeRelease;
        dispatch.reconciliation = cymule_core::ReconciliationState::Resolved;
        StateRootSidecarWriter {
            roots: &mut roots,
            overlay: &mut overlay,
        }
        .put_outbox(&dispatch)
        .expect("terminal outbox publishes");
        assert_eq!(roots.outbox, owner_root);
        assert_eq!(
            load_owned_effect_dispatch(&roots, &dispatch.intent_id, &mut overlay)
                .expect("intent-only lookup uses current Run root"),
            Some(dispatch.clone())
        );
        let before = roots.clone();
        dispatch.run_id = "run:foreign-outbox".to_owned();
        assert!(matches!(
            StateRootSidecarWriter {
                roots: &mut roots,
                overlay: &mut overlay
            }
            .put_outbox(&dispatch),
            Err(DurableError::HistoryConflict { .. })
        ));
        assert_eq!(roots, before);
    }

    #[test]
    fn run_local_outbox_locator_never_substitutes_for_a_missing_dispatch() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let mut roots = StateRoots::empty();
        let dispatch = pending_dispatch("run:missing-local-outbox");
        StateRootSidecarWriter {
            roots: &mut roots,
            overlay: &mut overlay,
        }
        .put_outbox(&dispatch)
        .expect("outbox publishes");
        let mut query = require_run_query_roots(&roots, &dispatch.run_id, &mut overlay)
            .expect("Run roots resolve");
        query.effects = MapRoot::empty();
        roots.run_query_indexes = map_put(
            &roots.run_query_indexes,
            &dispatch.run_id,
            StateRootValue::run_query_indexes(&dispatch.run_id, query).expect("descriptor encodes"),
            &mut overlay,
        )
        .expect("missing-leaf fixture publishes");
        assert!(
            matches!(load_owned_effect_dispatch(&roots, &dispatch.intent_id, &mut overlay),
            Err(DurableError::Integrity { code, .. }) if code == "state_root_owned_outbox_missing")
        );
    }

    #[test]
    fn terminal_companion_is_required_nullable_and_roots_hidden_values() {
        let run_id = "run:terminal-shadow-root";
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let dispatch = pending_dispatch(run_id);
        let shadow = build_typed_map(
            StateRootLeafKind::Outbox,
            BTreeMap::from([(dispatch.intent_id.clone(), dispatch.clone())]),
            &mut overlay,
        )
        .expect("hidden Effect root builds");
        let mut query = RunQueryIndexRoots::default();
        let empty = StateRootValue::run_query_indexes(run_id, query.clone())
            .expect("empty descriptor encodes");
        let mut wire = serde_json::to_value(empty).expect("descriptor serializes");
        assert!(wire["terminal"].is_null());
        wire.as_object_mut()
            .expect("descriptor is an object")
            .remove("terminal");
        assert!(
            cymule_core::decode_json::<StateRootValue>(
                &cymule_core::canonical_bytes(&wire).expect("wire canonicalizes")
            )
            .is_err()
        );
        query.terminal = Some(Box::new(RunTerminalSidecarCurrent {
            transition_id: dispatch.intent_id,
            transition_digest: "0".repeat(64),
            source_continuation_digest: "1".repeat(64),
            source_query_digest: "2".repeat(64),
            effects: shadow.clone(),
            active_effects: shadow.clone(),
            active_leases: MapRoot::empty(),
        }));
        let value =
            StateRootValue::run_query_indexes(run_id, query).expect("shadow descriptor encodes");
        assert!(
            value
                .pending_references()
                .contains(shadow.node.as_ref().expect("shadow node exists"))
        );
    }

    fn component_occurrence_fixture(
        run_id: &str,
    ) -> (crate::ComponentOccurrence, crate::OperationAttempt) {
        let input = cymule_core::artifact_ref("cymule.test.component-input/1", b"input")
            .expect("component input derives");
        let binding_context =
            cymule_core::artifact_ref(cymule_core::EXECUTION_BINDING_ARTIFACT_KIND, b"binding")
                .expect("component binding derives")
                .artifact_id;
        let occurrence_binding =
            cymule_core::content_id("cymule.test.component-occurrence-binding/1", &run_id)
                .expect("occurrence binding derives");
        let mut occurrence = crate::ComponentOccurrence {
            occurrence_version: crate::COMPONENT_OCCURRENCE_VERSION.to_owned(),
            occurrence_id: String::new(),
            run_id: run_id.to_owned(),
            plan_id: cymule_core::content_id("cymule.test.component-plan/1", &run_id)
                .expect("component Plan derives"),
            binding_context,
            invocation_id: "invocation:component-test".to_owned(),
            invocation_path: Vec::new(),
            definition_id: "definition:component-test".to_owned(),
            region_path: Vec::new(),
            site_id: "site:component-test".to_owned(),
            step_index: 0,
            component: "component.test".to_owned(),
            input,
            outcome: None,
            occurrence_binding: occurrence_binding.clone(),
            implementation_revision: "implementation:component-test".to_owned(),
            attempt_count: 1,
            latest_attempt_id: String::new(),
            continuation_digest: None,
            state: crate::ComponentOccurrenceState::Pending,
        };
        occurrence.occurrence_id = crate::model::component_occurrence_id(&occurrence)
            .expect("occurrence identity derives");
        let continuation_attempt_id = cymule_core::content_id(
            "cymule.test.component-continuation-attempt/1",
            &(run_id, 1_u64),
        )
        .expect("continuation Attempt derives");
        let identity = crate::model::OperationAttemptIdentity {
            occurrence_id: &occurrence.occurrence_id,
            attempt_ordinal: 1,
            previous_attempt_id: None,
            run_id,
            continuation_attempt_id: &continuation_attempt_id,
            execution_claim_owner: "worker:component-test",
            execution_claim_fence: 1,
            operation_occurrence_binding: &occurrence_binding,
        };
        let attempt_id = crate::model::operation_attempt_id(&identity)
            .expect("provider Attempt identity derives");
        let attempt = crate::OperationAttempt {
            attempt_version: crate::OPERATION_ATTEMPT_VERSION.to_owned(),
            attempt_id: attempt_id.clone(),
            occurrence_id: occurrence.occurrence_id.clone(),
            run_id: run_id.to_owned(),
            attempt_ordinal: 1,
            previous_attempt_id: None,
            continuation_attempt_id: continuation_attempt_id.clone(),
            execution_claim_owner: "worker:component-test".to_owned(),
            execution_claim_fence: 1,
            operation_occurrence_binding: occurrence_binding,
            transport_request_id: cymule_core::content_id(
                crate::model::TRANSPORT_REQUEST_ID_DOMAIN,
                &(attempt_id.as_str(), continuation_attempt_id.as_str()),
            )
            .expect("transport request identity derives"),
            state: crate::OperationAttemptState::Running,
            outcome: None,
        };
        occurrence.latest_attempt_id = attempt_id;
        occurrence.verify().expect("occurrence verifies");
        attempt.verify().expect("Attempt verifies");
        (occurrence, attempt)
    }

    fn next_component_attempt(
        occurrence: &crate::ComponentOccurrence,
        previous_attempt_id: &str,
        ordinal: u64,
    ) -> crate::OperationAttempt {
        let continuation_attempt_id = cymule_core::content_id(
            "cymule.test.component-continuation-attempt/1",
            &(occurrence.run_id.as_str(), ordinal),
        )
        .expect("continuation Attempt derives");
        let owner = format!("worker:component-test:{ordinal}");
        let identity = crate::model::OperationAttemptIdentity {
            occurrence_id: &occurrence.occurrence_id,
            attempt_ordinal: ordinal,
            previous_attempt_id: Some(previous_attempt_id),
            run_id: &occurrence.run_id,
            continuation_attempt_id: &continuation_attempt_id,
            execution_claim_owner: &owner,
            execution_claim_fence: ordinal,
            operation_occurrence_binding: &occurrence.occurrence_binding,
        };
        let attempt_id = crate::model::operation_attempt_id(&identity)
            .expect("next provider Attempt identity derives");
        let attempt = crate::OperationAttempt {
            attempt_version: crate::OPERATION_ATTEMPT_VERSION.to_owned(),
            attempt_id: attempt_id.clone(),
            occurrence_id: occurrence.occurrence_id.clone(),
            run_id: occurrence.run_id.clone(),
            attempt_ordinal: ordinal,
            previous_attempt_id: Some(previous_attempt_id.to_owned()),
            continuation_attempt_id: continuation_attempt_id.clone(),
            execution_claim_owner: owner,
            execution_claim_fence: ordinal,
            operation_occurrence_binding: occurrence.occurrence_binding.clone(),
            transport_request_id: cymule_core::content_id(
                crate::model::TRANSPORT_REQUEST_ID_DOMAIN,
                &(attempt_id.as_str(), continuation_attempt_id.as_str()),
            )
            .expect("transport request identity derives"),
            state: crate::OperationAttemptState::Running,
            outcome: None,
        };
        attempt.verify().expect("next Attempt verifies");
        attempt
    }

    #[derive(Default)]
    struct TestResolver {
        pinned: String,
        objects: BTreeMap<String, StateRootObject>,
        loads: usize,
    }

    impl TestResolver {
        fn insert_all(&mut self, objects: impl IntoIterator<Item = StateRootObject>) {
            for object in objects {
                self.objects.insert(object.object_id().to_owned(), object);
            }
        }
    }

    impl StateRootResolver for TestResolver {
        fn pinned_manifest_id(&self) -> &str {
            &self.pinned
        }

        fn load_state_root_object(
            &mut self,
            object_id: &str,
        ) -> DurableResult<Option<StateRootObject>> {
            self.loads += 1;
            Ok(self.objects.get(object_id).cloned())
        }
    }

    #[test]
    fn durable_provider_failures_enter_collections_without_string_flattening() {
        use cymule_authenticated_collections::{ProviderConflict, ProviderFailure};

        let cases = [
            (
                DurableError::Validation("invalid key".to_owned()),
                ProviderFailure::Validation {
                    message: "invalid key".to_owned(),
                },
            ),
            (
                DurableError::Integrity {
                    code: "forged_node".to_owned(),
                    message: "node mismatch".to_owned(),
                },
                ProviderFailure::Integrity {
                    code: "forged_node".to_owned(),
                    message: "node mismatch".to_owned(),
                },
            ),
            (
                DurableError::Conflict {
                    expected: Some("sha256:expected".to_owned()),
                    current: Some("sha256:current".to_owned()),
                },
                ProviderFailure::Conflict {
                    evidence: ProviderConflict::Revision {
                        expected: Some("sha256:expected".to_owned()),
                        current: Some("sha256:current".to_owned()),
                    },
                },
            ),
            (
                DurableError::HistoryConflict {
                    code: "identity_reused".to_owned(),
                    message: "history mismatch".to_owned(),
                },
                ProviderFailure::Conflict {
                    evidence: ProviderConflict::History {
                        code: "identity_reused".to_owned(),
                        message: "history mismatch".to_owned(),
                    },
                },
            ),
            (
                DurableError::Substrate {
                    code: "store_unavailable".to_owned(),
                    message: "offline".to_owned(),
                },
                ProviderFailure::Substrate {
                    code: "store_unavailable".to_owned(),
                    message: "offline".to_owned(),
                },
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(collection_provider_failure(error), expected);
        }
    }

    fn record(index: usize) -> crate::JournalRecord {
        crate::JournalRecord::new(
            format!("record:{index}"),
            "test.record/1",
            serde_json::json!({ "index": index }),
        )
        .expect("record is valid")
    }

    fn install_transition(
        manifest: &mut StateRootManifest,
        resolver: &mut TestResolver,
        delta: &crate::DurableDelta,
    ) {
        let transition = manifest
            .apply(delta, resolver)
            .expect("state-root transition applies");
        transition
            .verify(Some(manifest))
            .expect("state-root transition verifies");
        resolver.insert_all(transition.objects);
        manifest.clone_from(&transition.manifest);
        resolver.pinned.clone_from(&manifest.manifest_id);
    }

    fn install_material_transition(
        manifest: &mut StateRootManifest,
        resolver: &mut TestResolver,
        material: &cymule_core::durable_internal::MachineMaterialAdmission,
        operations: Vec<crate::DurableOperation>,
    ) {
        let outer = cymule_core::content_id("cymule.test.material-sidecars/1", &operations)
            .expect("outer material owner derives");
        let stage = pinned_machine::PinnedMachineView::open(manifest, resolver)
            .expect("pinned Machine material view opens")
            .prepare_material_admission(material, &outer)
            .expect("exact immutable material prepares");
        let delta = crate::DurableDelta::new(operations).expect("material sidecar seals");
        let (_, transition) = stage
            .finish(manifest, Some(&delta), resolver)
            .expect("material and its owning sidecar close together")
            .into_parts();
        transition
            .verify(Some(manifest))
            .expect("material transition verifies");
        resolver.insert_all(transition.objects);
        manifest.clone_from(&transition.manifest);
        resolver.pinned.clone_from(&manifest.manifest_id);
    }

    fn long_history_fixture(
        record_count: usize,
        replacement_count: usize,
        coupled_count: usize,
    ) -> (
        StateRootManifest,
        TestResolver,
        crate::JournalRecord,
        String,
        String,
    ) {
        assert!(record_count > 1);
        assert!(replacement_count > 0);
        assert!(coupled_count > 0);
        let source = (0..record_count).map(record).collect::<Vec<_>>();
        let historical = source[0].clone();
        let mut state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        state.application_journals.insert(
            "journal:history".to_owned(),
            crate::ApplicationJournal::try_from_records(source)
                .expect("unique history journal seals"),
        );
        let genesis = StateRootManifest::genesis(&state).expect("history genesis builds");
        let mut manifest = genesis.manifest;
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(genesis.objects);

        let mut parent_replacement_id = None;
        for index in 0..replacement_count {
            let current_count = if index == 0 {
                u64::try_from(record_count).expect("test record count fits u64")
            } else {
                1
            };
            let expected = load_application_journal_prefix(
                &manifest,
                &mut resolver,
                "journal:history",
                current_count,
            )
            .expect("current journal prefix resolves");
            let replacement = vec![record(record_count + index)];
            let (_, result) = preview_application_journal_replacement(
                &manifest,
                &mut resolver,
                "journal:history",
                current_count,
                &replacement,
            )
            .expect("replacement previews");
            let replacement_id = format!("replacement:{index}");
            let receipt = crate::ApplicationJournalPrefixReplacementReceipt::new(
                crate::ApplicationJournalPrefixReplacement {
                    replacement_id: replacement_id.clone(),
                    journal_id: "journal:history".to_owned(),
                    parent_replacement_id: parent_replacement_id.clone(),
                    expected_prefix: expected,
                    replacement,
                },
                result,
            )
            .expect("replacement receipt seals");
            install_transition(
                &mut manifest,
                &mut resolver,
                &crate::DurableDelta::new(vec![crate::DurableOperation::ReplaceJournalPrefix {
                    receipt,
                }])
                .expect("replacement delta seals"),
            );
            parent_replacement_id = Some(replacement_id);
        }

        let journal_manifest = crate::JournalBatchManifest::from_batch(&crate::JournalBatch {
            journal_id: "journal:history".to_owned(),
            records: vec![historical.clone()],
        })
        .expect("coupled journal manifest derives");
        let mut last_coupling_id = String::new();
        for index in 0..coupled_count {
            let coupling_id = cymule_core::content_id("test.coupled-history/1", &index)
                .expect("coupling identity derives");
            let result_revision = cymule_core::content_id("test.coupled-result/1", &index)
                .expect("result revision derives");
            let receipt =
                crate::CoupledCheckpointReceipt::new(crate::CoupledCheckpoint::JournalSet {
                    coupling_key: coupling_id.clone(),
                    source_revision: manifest.revision.clone(),
                    result_revision,
                    manifest: vec![journal_manifest.clone()],
                })
                .expect("coupled receipt seals");
            install_transition(
                &mut manifest,
                &mut resolver,
                &crate::DurableDelta::new(vec![
                    crate::DurableOperation::PutCoupledCheckpointReceipt { value: receipt },
                ])
                .expect("coupled receipt delta seals"),
            );
            last_coupling_id = coupling_id;
        }

        (
            manifest,
            resolver,
            historical,
            parent_replacement_id.expect("replacement history exists"),
            last_coupling_id,
        )
    }

    fn started_core_machine() -> cymule_core::Machine {
        use cymule_core::{
            Command, CommandEnvelope, Definition, Expression, PlanCandidate, Region,
        };
        let mut machine = cymule_core::Machine::new();
        let plan = cymule_core::seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "state_root_base".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Literal {
                        value: serde_json::Value::Null,
                    },
                },
            }],
            metadata: BTreeMap::new(),
        })
        .expect("base test Plan seals");
        machine.insert_plan(plan.clone()).expect("Plan inserts");
        let binding = cymule_runtime::ExecutionBinding::for_local_process(
            &cymule_runtime::PluginManifest {
                plugin_version: cymule_runtime::PLUGIN_VERSION.to_owned(),
                implementation_id: "state-root-base-test".to_owned(),
                components: BTreeMap::new(),
                effects: BTreeMap::new(),
            },
            "sha256:abababababababababababababababababababababababababababababababab",
        )
        .expect("binding seals");
        let binding_bytes = cymule_core::canonical_bytes(&binding).expect("binding encodes");
        let binding_ref = machine
            .put_artifact(
                cymule_runtime::EXECUTION_BINDING_VERSION,
                binding_bytes.clone(),
            )
            .expect("binding retains");
        let input_bytes =
            cymule_core::canonical_bytes(&serde_json::json!({})).expect("input encodes");
        let input_ref = machine
            .put_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, input_bytes.clone())
            .expect("input retains");
        let command_id = "command:state-root-base".to_owned();
        let material = cymule_core::durable_internal::MachineStartRunMaterial::new(
            command_id.clone(),
            plan.clone(),
            cymule_core::ArtifactRecord {
                reference: binding_ref.clone(),
                bytes: binding_bytes,
            },
            cymule_core::ArtifactRecord {
                reference: input_ref.clone(),
                bytes: input_bytes,
            },
        )
        .expect("StartRun material seals");
        let initial_attempt = cymule_core::InitialAttemptSpec {
            attempt_id: cymule_core::content_id("cymule.test.state-root-attempt/1", &command_id)
                .expect("initial Attempt derives"),
            continuation_id: cymule_core::content_id(
                "cymule.test.state-root-continuation/1",
                &command_id,
            )
            .expect("initial Continuation derives"),
            occurrence_binding: binding_ref.artifact_id.clone(),
            continuation_epoch: 0,
            execution_fence: 1,
        };
        machine
            .submit(CommandEnvelope {
                command_version: cymule_core::COMMAND_VERSION.to_owned(),
                command_id,
                actor: "state-root-test".to_owned(),
                run_id: "run:state-root-base".to_owned(),
                expected_precondition: None,
                command: Command::StartRun {
                    plan_id: plan.plan_id,
                    binding_context: binding_ref.artifact_id,
                    input: input_ref,
                    material_digest: material.material_digest().to_owned(),
                    initial_attempt,
                },
            })
            .expect("Run starts");
        machine
    }

    fn compacted_machine_base() -> cymule_core::MachineBaseSnapshot {
        let mut machine = started_core_machine();
        machine.compact_event_history(0).expect("history compacts");
        machine
            .snapshot()
            .base
            .expect("compacted snapshot has a base")
    }

    fn decode_records(values: Vec<StateRootValue>) -> Vec<crate::JournalRecord> {
        decode_value_vec(values, StateRootLeafKind::JournalRecord).expect("journal leaves decode")
    }

    #[test]
    fn hot_command_event_position_is_required_and_bounded() {
        let parts = started_core_machine()
            .snapshot()
            .root_parts()
            .expect("Core parts verify");
        let record = parts
            .commands
            .values()
            .next()
            .expect("StartRun command exists");
        let admission = &parts.admissions[0];
        let proof = &parts.command_index_proofs[&record.envelope.command_id];
        let value = StateRootValue::machine_command_current(
            record.clone(),
            admission.clone(),
            proof.clone(),
            Some(1),
        )
        .expect("exact first two Events bind the hot command");
        for invalid in [None, Some(0), Some(MAX_EXACT_INTEGER)] {
            assert!(
                StateRootValue::machine_command_current(
                    record.clone(),
                    admission.clone(),
                    proof.clone(),
                    invalid,
                )
                .is_err()
            );
        }
        let mut encoded = serde_json::to_value(&value).expect("hot command serializes");
        encoded
            .as_object_mut()
            .expect("hot command is an object")
            .remove("first_event_position");
        assert!(
            cymule_core::decode_json::<StateRootValue>(
                &cymule_core::canonical_bytes(&encoded).expect("omitted position serializes"),
            )
            .is_err()
        );
    }

    #[test]
    fn exact_hot_command_events_use_absolute_positions_across_a_cut() {
        let parts = started_core_machine()
            .snapshot()
            .root_parts()
            .expect("Core parts verify");
        let record = parts
            .commands
            .values()
            .next()
            .expect("StartRun command exists");
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let root = build_typed_log(
            StateRootLeafKind::MachineEvent,
            parts.events.clone(),
            &mut overlay,
        )
        .expect("exact Event log builds");
        assert_eq!(
            load_hot_command_events(&root, 2, record, Some(1), &mut overlay)
                .expect("genesis positions read"),
            parts.events,
        );
        assert_eq!(
            load_hot_command_events(&root, 9, record, Some(8), &mut overlay)
                .expect("seven-Event cut translates exact positions"),
            parts.events,
        );
        for invalid in [None, Some(0), Some(7), Some(9), Some(MAX_EXACT_INTEGER)] {
            assert!(load_hot_command_events(&root, 9, record, invalid, &mut overlay).is_err());
        }
    }

    #[test]
    fn command_event_positions_reject_interleaving_and_overflow() {
        let parts = started_core_machine()
            .snapshot()
            .root_parts()
            .expect("Core parts verify");
        let positions =
            command_event_positions(7, &parts.events).expect("contiguous positions derive");
        let position = positions
            .values()
            .next()
            .expect("one command owns both Events");
        assert_eq!(position.first, 8);
        assert_eq!(position.event_ids.len(), 2);
        assert!(command_event_positions(MAX_EXACT_INTEGER, &parts.events).is_err());
        let mut interleaved = parts.events.clone();
        let mut foreign = parts.events[0].clone();
        foreign.command_id = "command:another".to_owned();
        interleaved.insert(1, foreign);
        assert!(matches!(command_event_positions(0, &interleaved),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_machine_command_events_noncontiguous"));
    }

    #[test]
    fn genesis_round_trips_through_fixed_roots() {
        let state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        let transition = StateRootManifest::genesis(&state).expect("genesis roots build");
        transition
            .verify(None)
            .expect("genesis transition verifies");
        assert!(transition.manifest.revision.starts_with("sha256:"));

        let mut resolver = TestResolver {
            pinned: transition.manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(transition.objects.clone());
        let restored = transition
            .manifest
            .materialize(&mut resolver)
            .expect("fixed roots materialize");
        assert_eq!(restored, state);
        let reachable = reachable_state_root_objects(&transition.manifest, &mut resolver)
            .expect("reachable closure verifies");
        assert_eq!(
            reachable,
            transition
                .objects
                .iter()
                .map(|object| object.object_id().to_owned())
                .collect()
        );
    }

    #[test]
    fn component_initial_frontier_requires_pending_and_first_running_attempt() {
        let state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        let genesis = StateRootManifest::genesis(&state).expect("empty genesis builds");
        let manifest = genesis.manifest;
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(genesis.objects);
        let initial_objects = resolver.objects.len();
        let (occurrence, first) = component_occurrence_fixture("run:component-initial");

        let mut superseded = first.clone();
        superseded.state = crate::OperationAttemptState::Superseded;
        let outcome = crate::ComponentOutcome::Succeeded {
            output: cymule_core::artifact_ref("cymule.test.component-output/1", b"output")
                .expect("component output derives"),
        };
        let mut completed = occurrence.clone();
        completed.state = crate::ComponentOccurrenceState::Completed;
        completed.outcome = Some(outcome.clone());
        completed.continuation_digest = Some("ab".repeat(32));
        let mut completed_attempt = first;
        completed_attempt.state = crate::OperationAttemptState::Completed;
        completed_attempt.outcome = Some(outcome);

        for (occurrence, attempt) in [(occurrence, superseded), (completed, completed_attempt)] {
            occurrence.verify().expect("individual occurrence verifies");
            attempt.verify().expect("individual Attempt verifies");
            let error = manifest
                .apply(
                    &crate::DurableDelta::new(vec![
                        crate::DurableOperation::PutComponentOccurrence { value: occurrence },
                        crate::DurableOperation::PutOperationAttempt { value: attempt },
                    ])
                    .expect("initial component delta seals"),
                    &mut resolver,
                )
                .expect_err("initial frontier must admit provider I/O before any terminal edge");
            assert!(matches!(
                error,
                DurableError::Integrity { code, .. }
                    if code == "state_root_component_initial_attempt_mismatch"
            ));
            assert_eq!(resolver.objects.len(), initial_objects);
        }
    }

    fn seeded_component_frontier() -> (
        StateRootManifest,
        TestResolver,
        crate::ComponentOccurrence,
        crate::OperationAttempt,
    ) {
        let state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        let genesis = StateRootManifest::genesis(&state).expect("empty genesis builds");
        let mut manifest = genesis.manifest;
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(genesis.objects);
        let (occurrence, first) = component_occurrence_fixture("run:component-frontier");
        install_transition(
            &mut manifest,
            &mut resolver,
            &component_pair_delta(&occurrence, &first),
        );
        (manifest, resolver, occurrence, first)
    }

    fn component_pair_delta(
        occurrence: &crate::ComponentOccurrence,
        attempt: &crate::OperationAttempt,
    ) -> crate::DurableDelta {
        crate::DurableDelta::new(vec![
            crate::DurableOperation::PutComponentOccurrence {
                value: occurrence.clone(),
            },
            crate::DurableOperation::PutOperationAttempt {
                value: attempt.clone(),
            },
        ])
        .expect("component frontier delta seals")
    }

    fn supersede_component_attempt(
        manifest: &mut StateRootManifest,
        resolver: &mut TestResolver,
        attempt: &crate::OperationAttempt,
    ) -> crate::OperationAttempt {
        let mut superseded = attempt.clone();
        superseded.state = crate::OperationAttemptState::Superseded;
        install_transition(
            manifest,
            resolver,
            &crate::DurableDelta::new(vec![crate::DurableOperation::PutOperationAttempt {
                value: superseded.clone(),
            }])
            .expect("supersede delta seals"),
        );
        superseded
    }

    fn component_successor(
        occurrence: &crate::ComponentOccurrence,
        previous_id: &str,
    ) -> (crate::ComponentOccurrence, crate::OperationAttempt) {
        let mut next = occurrence.clone();
        next.attempt_count += 1;
        let attempt = next_component_attempt(&next, previous_id, next.attempt_count);
        next.latest_attempt_id = attempt.attempt_id.clone();
        (next, attempt)
    }

    fn assert_component_frontier(
        manifest: &StateRootManifest,
        resolver: &mut TestResolver,
        occurrence: &crate::ComponentOccurrence,
        attempt: &crate::OperationAttempt,
    ) {
        resolver.loads = 0;
        let frontier = pinned_machine::PinnedMachineView::open(manifest, resolver)
            .expect("pinned frontier reopens")
            .component_attempt_frontier(&occurrence.occurrence_id)
            .expect("exact frontier resolves")
            .expect("frontier exists");
        assert_eq!(&frontier.occurrence, occurrence);
        assert_eq!(&frontier.latest_attempt, attempt);
        assert!(
            resolver.loads < 64,
            "frontier read loaded {} objects",
            resolver.loads
        );
    }

    #[test]
    fn component_attempt_successor_requires_independently_committed_takeover() {
        let (mut manifest, mut resolver, occurrence, first) = seeded_component_frontier();
        let (advanced, second) = component_successor(&occurrence, &first.attempt_id);
        let error = manifest
            .apply(&component_pair_delta(&advanced, &second), &mut resolver)
            .expect_err("Running frontier cannot be skipped");
        assert!(matches!(error, DurableError::Integrity { code, .. }
            if code == "state_root_component_attempt_successor_mismatch"));
        let mut superseded = first.clone();
        superseded.state = crate::OperationAttemptState::Superseded;
        let combined = crate::DurableDelta::new(vec![
            crate::DurableOperation::PutOperationAttempt { value: superseded },
            crate::DurableOperation::PutComponentOccurrence {
                value: advanced.clone(),
            },
            crate::DurableOperation::PutOperationAttempt {
                value: second.clone(),
            },
        ])
        .expect("combined frontier delta seals");
        let error = manifest
            .apply(&combined, &mut resolver)
            .expect_err("successor must follow a separate takeover CAS");
        assert!(matches!(error, DurableError::Integrity { code, .. }
            if code == "state_root_component_attempt_successor_mismatch"));

        let superseded = supersede_component_attempt(&mut manifest, &mut resolver, &first);
        assert_component_frontier(&manifest, &mut resolver, &occurrence, &superseded);
        install_transition(
            &mut manifest,
            &mut resolver,
            &component_pair_delta(&advanced, &second),
        );
        assert_component_frontier(&manifest, &mut resolver, &advanced, &second);
    }

    #[test]
    fn component_attempt_frontier_rejects_a_forked_predecessor() {
        let (mut manifest, mut resolver, occurrence, first) = seeded_component_frontier();
        supersede_component_attempt(&mut manifest, &mut resolver, &first);
        let foreign = cymule_core::content_id(
            "cymule.test.component-fork-parent/1",
            &occurrence.occurrence_id,
        )
        .expect("foreign predecessor derives");
        let (forked, fork) = component_successor(&occurrence, &foreign);
        forked.verify().expect("fork is independently well shaped");
        let error = manifest
            .apply(&component_pair_delta(&forked, &fork), &mut resolver)
            .expect_err("the latest exact predecessor cannot be substituted");
        assert!(matches!(error, DurableError::Integrity { code, .. }
            if code == "state_root_component_attempt_frontier_jump"));
    }

    #[test]
    fn component_completion_is_atomic_terminal_and_reopens_with_exact_history() {
        let (mut manifest, mut resolver, occurrence, first) = seeded_component_frontier();
        let superseded = supersede_component_attempt(&mut manifest, &mut resolver, &first);
        let (mut completed, mut completed_attempt) =
            component_successor(&occurrence, &first.attempt_id);
        install_transition(
            &mut manifest,
            &mut resolver,
            &component_pair_delta(&completed, &completed_attempt),
        );
        let outcome = crate::ComponentOutcome::Succeeded {
            output: cymule_core::artifact_ref("cymule.test.component-output/1", b"output")
                .expect("component output derives"),
        };
        completed.state = crate::ComponentOccurrenceState::Completed;
        completed.outcome = Some(outcome.clone());
        completed.continuation_digest = Some("cd".repeat(32));
        completed_attempt.state = crate::OperationAttemptState::Completed;
        completed_attempt.outcome = Some(outcome);
        let detached =
            crate::DurableDelta::new(vec![crate::DurableOperation::PutOperationAttempt {
                value: completed_attempt.clone(),
            }])
            .expect("detached completion seals");
        let error = manifest
            .apply(&detached, &mut resolver)
            .expect_err("Attempt cannot complete without the occurrence");
        assert!(matches!(error, DurableError::Integrity { code, .. }
            if code == "state_root_component_supersede_frontier_mismatch"));
        install_transition(
            &mut manifest,
            &mut resolver,
            &component_pair_delta(&completed, &completed_attempt),
        );
        assert_component_frontier(&manifest, &mut resolver, &completed, &completed_attempt);
        crate::model::validate_operation_attempt_history(
            &completed,
            &mut vec![&superseded, &completed_attempt],
            crate::ContinuationStatus::Running,
        )
        .expect("only Superseded predecessors survive completion");
        assert!(
            crate::model::validate_operation_attempt_history(
                &completed,
                &mut vec![&first, &completed_attempt],
                crate::ContinuationStatus::Running,
            )
            .is_err()
        );

        let (mut reopened, third) = component_successor(&completed, &completed_attempt.attempt_id);
        reopened.state = crate::ComponentOccurrenceState::Pending;
        reopened.outcome = None;
        reopened.continuation_digest = None;
        let error = manifest
            .apply(&component_pair_delta(&reopened, &third), &mut resolver)
            .expect_err("terminal occurrence cannot gain another Attempt");
        assert!(matches!(error, DurableError::Integrity { code, .. }
            if code == "state_root_component_attempt_frontier_jump"));
    }

    #[test]
    fn machine_plan_and_artifact_admission_order_round_trips_exactly() {
        use cymule_core::{Definition, Expression, PlanCandidate, Region};

        let mut plans = (0..8_u8)
            .map(|index| {
                cymule_core::seal_plan(PlanCandidate {
                    ir_version: cymule_core::IR_VERSION.to_owned(),
                    name: format!("state_root_order_{index}"),
                    entry: "main".to_owned(),
                    components: Vec::new(),
                    effects: Vec::new(),
                    definitions: vec![Definition {
                        id: "main".to_owned(),
                        input_schema: serde_json::json!({}),
                        output_schema: serde_json::json!({}),
                        body: Region {
                            steps: Vec::new(),
                            result: Expression::Literal {
                                value: serde_json::json!(index),
                            },
                        },
                    }],
                    metadata: BTreeMap::new(),
                })
                .expect("ordered test Plan seals")
            })
            .collect::<Vec<_>>();
        plans.sort_by(|left, right| left.plan_id.cmp(&right.plan_id));
        let low_plan = plans.first().expect("test Plans exist").clone();
        let high_plan = plans.last().expect("test Plans exist").clone();

        let artifact_kind = "test.state-root-order/1";
        let mut artifacts = (0..8_u8)
            .map(|index| {
                let bytes = vec![index];
                cymule_core::artifact_ref(artifact_kind, &bytes)
                    .map(|reference| (reference, bytes))
                    .expect("ordered test Artifact derives")
            })
            .collect::<Vec<_>>();
        artifacts.sort_by(|left, right| left.0.artifact_id.cmp(&right.0.artifact_id));
        let (low_artifact, low_bytes) = artifacts.first().expect("test Artifacts exist").clone();
        let (high_artifact, high_bytes) = artifacts.last().expect("test Artifacts exist").clone();

        let genesis = StateRootManifest::genesis(&crate::DurableState::new(
            cymule_core::Machine::new().snapshot(),
        ))
        .expect("empty roots build");
        let mut manifest = genesis.manifest;
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(genesis.objects);
        for (index, plan, reference, bytes) in [
            (0, high_plan.clone(), high_artifact.clone(), high_bytes),
            (1, low_plan.clone(), low_artifact.clone(), low_bytes),
        ] {
            let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
                format!("material:ordered:{index}"),
                vec![plan],
                vec![cymule_core::ArtifactRecord { reference, bytes }],
            )
            .expect("ordered material seals");
            install_material_transition(
                &mut manifest,
                &mut resolver,
                &material,
                vec![crate::DurableOperation::AppendJournal {
                    journal_id: "journal:ordered-material".to_owned(),
                    records: vec![record(index)],
                }],
            );
        }
        let state = manifest
            .materialize(&mut resolver)
            .expect("explicit material admissions materialize");
        assert_eq!(
            state
                .machine
                .plans
                .iter()
                .map(|plan| plan.plan_id.as_str())
                .collect::<Vec<_>>(),
            vec![high_plan.plan_id.as_str(), low_plan.plan_id.as_str()]
        );
        assert_eq!(
            state
                .machine
                .artifacts
                .iter()
                .map(|artifact| artifact.reference.artifact_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                high_artifact.artifact_id.as_str(),
                low_artifact.artifact_id.as_str(),
            ]
        );

        let restored = manifest
            .materialize(&mut resolver)
            .expect("ordered roots materialize");
        assert_eq!(restored.machine.plans, state.machine.plans);
        assert_eq!(restored.machine.artifacts, state.machine.artifacts);
    }

    #[test]
    fn genesis_derives_all_ever_manifest_from_the_active_journal() {
        let mut state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        let expected = record(1);
        state.application_journals.insert(
            "journal:derived".to_owned(),
            crate::ApplicationJournal::try_from_records(vec![expected.clone()])
                .expect("unique derived journal seals"),
        );
        let transition = StateRootManifest::genesis(&state).expect("genesis derives history");
        let mut resolver = TestResolver {
            pinned: transition.manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(transition.objects);
        assert_eq!(
            load_application_journal_record_manifest(
                &transition.manifest,
                &mut resolver,
                "journal:derived",
                &expected.record_id,
            )
            .expect("exact derived manifest resolves"),
            Some(
                crate::JournalRecordManifest::from_record(&expected)
                    .expect("record manifest derives")
            )
        );
    }

    #[test]
    fn ordinary_reopen_does_not_materialize_all_ever_history() {
        let (manifest, mut resolver, historical, replacement_id, coupling_id) =
            long_history_fixture(2_048, 128, 128);
        resolver.loads = 0;
        let state = manifest
            .materialize(&mut resolver)
            .expect("bounded runtime state materializes");
        assert_eq!(state.application_journals["journal:history"].len(), 1);
        assert_eq!(state.application_journal_prefix_replacements.len(), 1);
        assert!(
            cymule_core::canonical_bytes(&state)
                .expect("active projection encodes")
                .len()
                < 64 * 1_024,
            "active projection copied cumulative history into memory"
        );
        assert!(
            resolver.loads < 1_024,
            "ordinary reopen loaded {} objects for more than 2,000 historical entries",
            resolver.loads
        );

        assert_eq!(
            load_application_journal_record_manifest(
                &manifest,
                &mut resolver,
                "journal:history",
                &historical.record_id,
            )
            .expect("historical record manifest resolves"),
            Some(
                crate::JournalRecordManifest::from_record(&historical)
                    .expect("historical manifest derives")
            )
        );
        assert_eq!(
            load_application_journal_prefix_replacement_authority(
                &manifest,
                &mut resolver,
                &replacement_id,
            )
            .expect("historical replacement resolves")
            .expect("historical replacement exists")
            .replacement_id,
            replacement_id
        );
        assert_eq!(
            load_coupled_checkpoint_receipt(&manifest, &mut resolver, &coupling_id)
                .expect("historical coupled receipt resolves")
                .expect("historical coupled receipt exists")
                .coupling_id,
            coupling_id
        );
    }

    #[test]
    fn exact_history_lookup_authenticates_non_membership_without_scanning() {
        let (manifest, mut resolver, _, _, _) = long_history_fixture(1_024, 64, 64);
        let missing_coupling = cymule_core::content_id("test.coupled-history/1", &999_999_usize)
            .expect("missing coupling identity derives");
        resolver.loads = 0;
        assert_eq!(
            load_application_journal_record_manifest(
                &manifest,
                &mut resolver,
                "journal:history",
                "record:missing",
            )
            .expect("missing nested record proves non-membership"),
            None
        );
        assert_eq!(
            load_application_journal_record_manifest(
                &manifest,
                &mut resolver,
                "journal:missing",
                "record:missing",
            )
            .expect("missing journal proves non-membership"),
            None
        );
        assert_eq!(
            load_application_journal_prefix_replacement_authority(
                &manifest,
                &mut resolver,
                "replacement:missing",
            )
            .expect("missing replacement proves non-membership"),
            None
        );
        assert_eq!(
            load_coupled_checkpoint_receipt(&manifest, &mut resolver, &missing_coupling)
                .expect("missing coupled receipt proves non-membership"),
            None
        );
        assert!(
            resolver.loads < 1_024,
            "four exact negative lookups loaded {} objects",
            resolver.loads
        );
    }

    #[test]
    fn coupled_receipt_transition_rejects_a_missing_record_manifest() {
        let (manifest, mut resolver, _, _, _) = long_history_fixture(4, 1, 1);
        let missing = record(100_000);
        let journal = crate::JournalBatchManifest::from_batch(&crate::JournalBatch {
            journal_id: "journal:history".to_owned(),
            records: vec![missing],
        })
        .expect("dangling journal manifest derives");
        let coupling_id = cymule_core::content_id("test.coupled-dangling/1", &0_u8)
            .expect("dangling coupling identity derives");
        let receipt = crate::CoupledCheckpointReceipt::new(crate::CoupledCheckpoint::JournalSet {
            coupling_key: coupling_id.clone(),
            source_revision: manifest.revision.clone(),
            result_revision: cymule_core::content_id("test.coupled-dangling-result/1", &0_u8)
                .expect("dangling result identity derives"),
            manifest: vec![journal],
        })
        .expect("dangling receipt is locally well-formed");
        let error = manifest
            .apply(
                &crate::DurableDelta::new(vec![
                    crate::DurableOperation::PutCoupledCheckpointReceipt { value: receipt },
                ])
                .expect("dangling receipt delta seals"),
                &mut resolver,
            )
            .expect_err("dangling receipt cannot enter StateRoot history");
        assert!(matches!(
            error,
            DurableError::HistoryConflict { code, .. }
                if code == "state_root_coupled_checkpoint_journal_history_conflict"
        ));
        assert_eq!(
            load_coupled_checkpoint_receipt(&manifest, &mut resolver, &coupling_id)
                .expect("rejected receipt remains absent"),
            None
        );
    }

    #[test]
    fn immutable_history_keys_reject_different_semantics() {
        let (manifest, mut resolver, _, replacement_id, coupling_id) =
            long_history_fixture(4, 1, 1);
        let existing_coupled =
            load_coupled_checkpoint_receipt(&manifest, &mut resolver, &coupling_id)
                .expect("existing coupled receipt resolves")
                .expect("existing coupled receipt exists");
        let (source_revision, coupled_manifest) = match &existing_coupled.checkpoint {
            crate::CoupledCheckpoint::JournalSet {
                source_revision,
                manifest,
                ..
            } => (source_revision.clone(), manifest.clone()),
            _ => panic!("long-history fixture uses a journal-set receipt"),
        };
        let conflicting_coupled =
            crate::CoupledCheckpointReceipt::new(crate::CoupledCheckpoint::JournalSet {
                coupling_key: coupling_id,
                source_revision,
                result_revision: cymule_core::content_id(
                    "test.conflicting-coupled-result/1",
                    &0_u8,
                )
                .expect("conflicting result identity derives"),
                manifest: coupled_manifest,
            })
            .expect("conflicting coupled receipt seals");
        let coupled_error = manifest
            .apply(
                &crate::DurableDelta::new(vec![
                    crate::DurableOperation::PutCoupledCheckpointReceipt {
                        value: conflicting_coupled,
                    },
                ])
                .expect("conflicting coupled delta seals"),
                &mut resolver,
            )
            .expect_err("coupled history key cannot be rewritten");
        assert!(matches!(
            coupled_error,
            DurableError::HistoryConflict { code, .. }
                if code == "state_root_immutable_history_rewrite"
        ));

        let expected =
            load_application_journal_prefix(&manifest, &mut resolver, "journal:history", 1)
                .expect("current prefix resolves");
        let replacement_records = vec![record(200_000)];
        let (_, result) = preview_application_journal_replacement(
            &manifest,
            &mut resolver,
            "journal:history",
            1,
            &replacement_records,
        )
        .expect("conflicting replacement previews");
        let receipt = crate::ApplicationJournalPrefixReplacementReceipt::new(
            crate::ApplicationJournalPrefixReplacement {
                replacement_id,
                journal_id: "journal:history".to_owned(),
                parent_replacement_id: None,
                expected_prefix: expected,
                replacement: replacement_records,
            },
            result,
        )
        .expect("conflicting replacement receipt seals");
        let replacement_error = manifest
            .apply(
                &crate::DurableDelta::new(vec![crate::DurableOperation::ReplaceJournalPrefix {
                    receipt,
                }])
                .expect("conflicting replacement delta seals"),
                &mut resolver,
            )
            .expect_err("replacement history key cannot be rewritten");
        assert!(matches!(
            replacement_error,
            DurableError::HistoryConflict { code, .. }
                if code == "state_root_immutable_history_rewrite"
        ));
    }

    #[test]
    fn rope_rejects_tampered_root_height_and_count() {
        let records = (0..9).map(record).collect::<Vec<_>>();
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let root = build_typed_log(StateRootLeafKind::JournalRecord, records, &mut overlay)
            .expect("frontier builds");
        let pending = std::mem::take(&mut overlay.pending);
        drop(overlay);
        let mut resolver = TestResolver::default();
        resolver.insert_all(pending.into_values());
        let mut wrong_height = root.clone();
        wrong_height.height += 1;
        assert!(materialize_state_log(&wrong_height, &mut resolver).is_err());
        let mut wrong_count = root;
        wrong_count.len -= 1;
        assert!(materialize_state_log(&wrong_count, &mut resolver).is_err());
    }

    #[test]
    fn state_map_key_pages_follow_authenticated_hash_order_with_bounded_reads() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let values = (0..1024_u64)
            .map(|index| {
                let key = cymule_core::content_id("test.state-map-page-key/1", &index)
                    .expect("page key derives");
                let value = StateRootValue::encode(
                    StateRootLeafKind::JournalRecord,
                    &record(usize::try_from(index).expect("test index fits usize")),
                )
                .expect("page value encodes");
                (key, value)
            })
            .collect::<Vec<_>>();
        let mut expected = values
            .iter()
            .map(|(key, _)| (map_key_hash(key).expect("key hash derives"), key.clone()))
            .collect::<Vec<_>>();
        expected.sort();
        let root = build_value_map(values, &mut overlay).expect("page map builds");
        let pending = std::mem::take(&mut overlay.pending);
        drop(overlay);
        let mut resolver = TestResolver::default();
        resolver.insert_all(pending.into_values());

        let mut cursor = None;
        let mut actual = Vec::new();
        for _ in 0..100 {
            resolver.loads = 0;
            let page = load_state_map_key_page(
                &root,
                cursor.as_ref(),
                17,
                MAX_STATE_MAP_KEY_PAGE_BYTES,
                &mut resolver,
            )
            .expect("bounded key page resolves");
            assert!(
                resolver.loads <= cymule_authenticated_collections::MAX_MAP_PATH_NODES + 2 * 18,
                "one page read {} objects",
                resolver.loads
            );
            actual.extend(
                page.entries
                    .iter()
                    .map(|entry| (entry.key_hash.clone(), entry.key.clone())),
            );
            let Some(next) = page.next_position else {
                break;
            };
            assert_ne!(cursor.as_ref(), Some(&next));
            cursor = Some(next);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn state_map_key_page_reuses_authenticated_value_ids_without_exact_reproof() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let values = (0..512_u64)
            .map(|index| {
                let key = cymule_core::content_id("test.state-map-value-page-key/1", &index)
                    .expect("page key derives");
                let value = StateRootValue::encode(
                    StateRootLeafKind::JournalRecord,
                    &record(usize::try_from(index).expect("test index fits usize")),
                )
                .expect("page value encodes");
                (key, value)
            })
            .collect::<Vec<_>>();
        let root = build_value_map(values, &mut overlay).expect("page map builds");
        let pending = std::mem::take(&mut overlay.pending);
        drop(overlay);
        let mut resolver = TestResolver::default();
        resolver.insert_all(pending.into_values());

        let page = load_state_map_key_page(
            &root,
            None,
            256,
            MAX_STATE_MAP_KEY_PAGE_BYTES,
            &mut resolver,
        )
        .expect("maximum public query page resolves");
        assert_eq!(page.entries.len(), 256);
        let page_loads = resolver.loads;
        for entry in &page.entries {
            let value = load_typed_state_value::<crate::JournalRecord, _>(
                &entry.value_id,
                StateRootLeafKind::JournalRecord,
                &mut resolver,
            )
            .expect("range-authenticated value loads directly");
            value
                .verify()
                .expect("selected journal value remains valid");
        }
        assert_eq!(
            resolver.loads - page_loads,
            page.entries.len(),
            "each returned item performs exactly one value-object read"
        );

        let first = page.entries.first().expect("page has an entry");
        let kind_error = load_typed_state_value::<crate::WaitCondition, _>(
            &first.value_id,
            StateRootLeafKind::Wait,
            &mut resolver,
        )
        .expect_err("the authenticated value cannot change closed kind");
        assert!(matches!(kind_error, DurableError::Integrity { .. }));
        resolver.objects.remove(&first.value_id);
        let missing_error = load_typed_state_value::<crate::JournalRecord, _>(
            &first.value_id,
            StateRootLeafKind::JournalRecord,
            &mut resolver,
        )
        .expect_err("a missing selected value fails closed");
        assert!(matches!(missing_error, DurableError::Integrity { code, .. }
            if code == "state_root_reachable_object_missing"));
    }

    #[test]
    fn state_map_key_page_does_not_load_an_unrelated_poisoned_value() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let values = (0..128_u64)
            .map(|index| {
                let key = cymule_core::content_id("test.state-map-poison-key/1", &index)
                    .expect("poison key derives");
                let value = StateRootValue::encode(
                    StateRootLeafKind::JournalRecord,
                    &record(usize::try_from(index).expect("test index fits usize")),
                )
                .expect("poison value encodes");
                (key, value)
            })
            .collect::<Vec<_>>();
        let mut ordered_keys = values
            .iter()
            .map(|(key, _)| (map_key_hash(key).expect("key hash derives"), key.clone()))
            .collect::<Vec<_>>();
        ordered_keys.sort();
        let poison_key = ordered_keys.last().expect("map is non-empty").1.clone();
        let root = build_value_map(values, &mut overlay).expect("poison map builds");
        let pending = std::mem::take(&mut overlay.pending);
        drop(overlay);
        let mut resolver = TestResolver::default();
        resolver.insert_all(pending.into_values());
        let poisoned_proof = {
            let mut proof_overlay = ObjectOverlay::new(&mut resolver);
            prove_map_exact(&root, &poison_key, &mut proof_overlay)
                .expect("poisoned key proof resolves")
        };
        let poisoned_value = verify_map_exact(&root, &poison_key, &poisoned_proof)
            .expect("poisoned key proof verifies")
            .value()
            .expect("poisoned key is present")
            .to_owned();
        resolver.objects.remove(&poisoned_value);

        let page =
            load_state_map_key_page(&root, None, 8, MAX_STATE_MAP_KEY_PAGE_BYTES, &mut resolver)
                .expect("unrelated poisoned value is outside the key-only read set");
        assert_eq!(page.entries.len(), 8);
        assert!(page.entries.iter().all(|entry| entry.key != poison_key));
        assert!(page.next_position.is_some());

        let mut audit_overlay = ObjectOverlay::new(&mut resolver);
        let error = materialize_map(&root, &mut audit_overlay)
            .expect_err("the explicit full audit must reach the poisoned value");
        assert!(matches!(error, DurableError::Integrity { code, .. }
            if code == "state_root_reachable_object_missing"));
    }

    #[test]
    fn state_map_key_page_rejects_invalid_cursor_and_first_key_over_budget() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let root = build_value_map(
            vec![(
                "key:page-budget".to_owned(),
                StateRootValue::encode(StateRootLeafKind::JournalRecord, &record(1))
                    .expect("page value encodes"),
            )],
            &mut overlay,
        )
        .expect("single-key map builds");
        let pending = std::mem::take(&mut overlay.pending);
        drop(overlay);
        let mut resolver = TestResolver::default();
        resolver.insert_all(pending.into_values());

        let invalid_cursor = StateMapTraversalPosition {
            key: "key:page-budget".to_owned(),
            key_hash: "ff".to_owned(),
        };
        let cursor_error =
            load_state_map_key_page(&root, Some(&invalid_cursor), 1, 1024, &mut resolver)
                .expect_err("truncated hash cursor is rejected");
        assert!(matches!(cursor_error, DurableError::Validation(_)));
        let budget_error = load_state_map_key_page(&root, None, 1, 1, &mut resolver)
            .expect_err("the first exact key cannot be silently skipped by a byte cap");
        assert!(matches!(budget_error, DurableError::Validation(_)));
    }

    #[test]
    fn state_map_key_limit_keeps_every_valid_leaf_pageable() {
        let max_key_len = cymule_authenticated_collections::MAX_MAP_KEY_BYTES;
        let boundary_key = "k".repeat(max_key_len);
        assert!(
            cymule_core::canonical_bytes(&(
                boundary_key.as_str(),
                map_key_hash(&boundary_key)
                    .expect("boundary key hash derives")
                    .as_str(),
            ))
            .expect("boundary key tuple canonicalizes")
            .len()
                <= MAX_STATE_MAP_KEY_PAGE_BYTES
        );

        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let root = build_value_map(
            vec![(
                boundary_key.clone(),
                StateRootValue::encode(StateRootLeafKind::JournalRecord, &record(2))
                    .expect("page value encodes"),
            )],
            &mut overlay,
        )
        .expect("maximum valid key builds");
        let pending = std::mem::take(&mut overlay.pending);
        drop(overlay);
        let mut resolver = TestResolver::default();
        resolver.insert_all(pending.into_values());
        let page =
            load_state_map_key_page(&root, None, 1, MAX_STATE_MAP_KEY_PAGE_BYTES, &mut resolver)
                .expect("maximum valid key remains pageable");
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key, boundary_key);
        assert!(page.next_position.is_none());

        let oversized_key = "k".repeat(
            max_key_len
                .checked_add(1)
                .expect("test key length increments"),
        );
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let error = build_value_map(
            vec![(
                oversized_key.clone(),
                StateRootValue::encode(StateRootLeafKind::JournalRecord, &record(3))
                    .expect("page value encodes"),
            )],
            &mut overlay,
        )
        .expect_err("a key that cannot fit the maximum page is rejected at insertion");
        assert!(matches!(error, DurableError::Validation(_)));
        assert!(overlay.pending.is_empty());

        assert!(!oversized_key.is_empty());
    }

    #[test]
    fn resource_handoff_page_rejects_extreme_cursor_before_range_arithmetic() {
        let state = crate::DurableState::new(cymule_core::Machine::new().snapshot());
        let transition = StateRootManifest::genesis(&state).expect("genesis roots build");
        let mut resolver = TestResolver {
            pinned: transition.manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(transition.objects);
        let error = load_resource_handoff_page(
            &transition.manifest,
            &mut resolver,
            "run:extreme-page",
            u64::MAX,
            1,
        )
        .expect_err("an out-of-range external cursor must fail closed");
        assert!(matches!(error, DurableError::Validation(_)));
    }

    #[test]
    fn ordered_log_preserves_duplicate_value_objects() {
        let records = vec![record(7); 65];
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let root = build_typed_log(
            StateRootLeafKind::JournalRecord,
            records.clone(),
            &mut overlay,
        )
        .expect("ordered logs preserve repeated value identities");
        let pending = std::mem::take(&mut overlay.pending);
        drop(overlay);
        let mut resolver = TestResolver::default();
        resolver.insert_all(pending.into_values());
        assert_eq!(
            decode_records(
                materialize_state_log(&root, &mut resolver).expect("duplicate log materializes")
            ),
            records
        );
    }

    #[test]
    fn ordered_root_is_independent_of_append_batching() {
        let records = (0..73).map(record).collect::<Vec<_>>();
        let values = records
            .iter()
            .map(|record| {
                StateRootValue::encode(StateRootLeafKind::JournalRecord, record)
                    .expect("journal leaf encodes")
            })
            .collect::<Vec<_>>();

        let mut empty_a = EmptyStateRootResolver;
        let mut overlay_a = ObjectOverlay::new(&mut empty_a);
        let one_batch =
            log_append(&LogRoot::empty(), &values, &mut overlay_a).expect("single batch appends");

        let mut empty_b = EmptyStateRootResolver;
        let mut overlay_b = ObjectOverlay::new(&mut empty_b);
        let mut many_batches = LogRoot::empty();
        for chunk in values.chunks(7) {
            many_batches = log_append(&many_batches, chunk, &mut overlay_b).expect("chunk appends");
        }
        assert_eq!(one_batch, many_batches);
        assert_eq!(
            one_batch.ordered_root,
            application_journal_ordered_root_from_records(&records)
                .expect("public commitment helper agrees")
        );
    }

    #[test]
    fn prefix_replacement_cost_is_independent_of_retained_suffix() {
        let records = (0..=(1 << 15)).map(record).collect::<Vec<_>>();
        let mut empty = EmptyStateRootResolver;
        let mut initial = ObjectOverlay::new(&mut empty);
        let root = build_typed_log(StateRootLeafKind::JournalRecord, records, &mut initial)
            .expect("large rope builds");
        let mut reachable = BTreeSet::new();
        let mut queue = VecDeque::from([root.node.clone().expect("large rope has a root")]);
        while let Some(object_id) = queue.pop_front() {
            if !reachable.insert(object_id.clone()) {
                continue;
            }
            if let Some(object) = initial.pending.get(&object_id) {
                queue.extend(object.pending_references());
            }
        }
        let retained = std::mem::take(&mut initial.pending)
            .into_iter()
            .filter_map(|(object_id, object)| reachable.contains(&object_id).then_some(object));
        drop(initial);
        let mut resolver = TestResolver::default();
        resolver.insert_all(retained);
        resolver.loads = 0;

        let replacement = [record(100_000)];
        let mut overlay = ObjectOverlay::new(&mut resolver);
        let (_, expected, result_root, result) =
            preview_journal_replacement_roots(&root, 1, &replacement, &mut overlay)
                .expect("one-record replacement previews");
        let created = overlay.pending.len();
        drop(overlay);
        assert_eq!(expected.record_count, 1);
        assert_eq!(result.record_count, root.len);
        assert_eq!(result_root.len, root.len);
        assert!(resolver.loads <= cymule_authenticated_collections::MAX_LOG_HEIGHT * 16);
        assert!(created <= cymule_authenticated_collections::MAX_LOG_HEIGHT * 16);
    }

    #[test]
    fn journal_prefix_preview_and_apply_bind_exact_result_root() {
        let records = (0..100).map(record).collect::<Vec<_>>();
        let mut state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        state.application_journals.insert(
            "journal:test".to_owned(),
            crate::ApplicationJournal::try_from_records(records.clone())
                .expect("unique test journal seals"),
        );
        let genesis = StateRootManifest::genesis(&state).expect("journal genesis builds");
        let mut resolver = TestResolver {
            pinned: genesis.manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(genesis.objects.clone());

        let replacement_records = vec![record(100_000)];
        let expected =
            load_application_journal_prefix(&genesis.manifest, &mut resolver, "journal:test", 40)
                .expect("exact prefix loads");
        let (preview_expected, result) = preview_application_journal_replacement(
            &genesis.manifest,
            &mut resolver,
            "journal:test",
            40,
            &replacement_records,
        )
        .expect("replacement previews");
        assert_eq!(preview_expected, expected);
        let command = crate::ApplicationJournalPrefixReplacement {
            replacement_id: "replacement:test".to_owned(),
            journal_id: "journal:test".to_owned(),
            parent_replacement_id: None,
            expected_prefix: expected,
            replacement: replacement_records.clone(),
        };
        let receipt =
            crate::ApplicationJournalPrefixReplacementReceipt::new(command, result.clone())
                .expect("receipt derives");
        let delta = crate::DurableDelta::new(vec![crate::DurableOperation::ReplaceJournalPrefix {
            receipt,
        }])
        .expect("replacement delta builds");
        let transition = genesis
            .manifest
            .apply(&delta, &mut resolver)
            .expect("replacement roots apply");
        transition
            .verify(Some(&genesis.manifest))
            .expect("replacement transition verifies");
        resolver.insert_all(transition.objects.clone());
        resolver.pinned.clone_from(&transition.manifest.manifest_id);
        let materialized = transition
            .manifest
            .materialize(&mut resolver)
            .expect("replacement state materializes");
        let mut expected_records = replacement_records;
        expected_records.extend_from_slice(&records[40..]);
        assert_eq!(
            materialized
                .application_journals
                .get("journal:test")
                .expect("journal remains")
                .to_vec(),
            expected_records
        );
        for expected in records
            .iter()
            .chain(materialized.application_journals["journal:test"].iter())
        {
            assert_eq!(
                load_application_journal_record_manifest(
                    &transition.manifest,
                    &mut resolver,
                    "journal:test",
                    &expected.record_id,
                )
                .expect("all-ever manifest resolves"),
                Some(
                    crate::JournalRecordManifest::from_record(expected)
                        .expect("expected manifest derives")
                )
            );
        }
        assert_eq!(
            load_application_journal_prefix(
                &transition.manifest,
                &mut resolver,
                "journal:test",
                result.record_count,
            )
            .expect("result prefix reloads"),
            result
        );
    }

    #[test]
    fn prefix_split_removes_payload_from_manifest_closure() {
        let records = (0..96).map(record).collect::<Vec<_>>();
        let removed_value_id = StateValueObject::new(
            StateRootValue::encode(StateRootLeafKind::JournalRecord, &records[0])
                .expect("leaf encodes"),
        )
        .expect("value object derives")
        .object_id;
        let retained_value_id = StateValueObject::new(
            StateRootValue::encode(StateRootLeafKind::JournalRecord, &records[40])
                .expect("leaf encodes"),
        )
        .expect("value object derives")
        .object_id;

        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let root = build_typed_log(StateRootLeafKind::JournalRecord, records, &mut overlay)
            .expect("journal rope builds");
        let (_, suffix) = split_log_root(&root, 40, &mut overlay).expect("prefix splits");
        let descriptor = StateRootValue::application_journal("journal:test", &suffix)
            .expect("descriptor builds");
        let journal_map =
            build_value_map(vec![("journal:test".to_owned(), descriptor)], &mut overlay)
                .expect("journal map builds");
        let mut roots = StateRoots::empty();
        roots.application_journals = journal_map;
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("manifest builds");
        let objects = overlay.finish(&manifest).expect("pending closure prunes");
        assert!(
            objects
                .iter()
                .all(|object| object.object_id() != removed_value_id)
        );
        assert!(
            objects
                .iter()
                .any(|object| object.object_id() == retained_value_id)
        );

        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(objects);
        let reachable =
            reachable_state_root_objects(&manifest, &mut resolver).expect("closure audits");
        assert!(!reachable.contains(&removed_value_id));
        assert!(reachable.contains(&retained_value_id));
    }

    #[test]
    fn reachable_audit_requires_the_exact_physical_manifest() {
        let state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        let transition = StateRootManifest::genesis(&state).expect("genesis builds");
        let mut resolver = TestResolver {
            pinned: transition.manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(
            transition
                .objects
                .iter()
                .filter(|object| object.object_id() != transition.manifest.manifest_id)
                .cloned(),
        );
        assert!(matches!(
            reachable_state_root_objects(&transition.manifest, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_reachable_object_missing"
        ));
    }

    #[test]
    fn predecessor_state_root_value_manifest_and_query_index_generations_are_rejected() {
        let leaf = StateRootValue::encode(StateRootLeafKind::JournalRecord, &record(0))
            .expect("current value leaf encodes");
        let mut value = StateValueObject {
            value_version: "cymule.durable-state-value/4".to_owned(),
            object_id: state_root_value_id(&leaf).expect("current value identity derives"),
            value: leaf,
        };
        assert!(value.verify().is_err());
        value.value_version = STATE_ROOT_VALUE_VERSION.to_owned();
        value.verify().expect("current value verifies");

        let state = crate::DurableState::new(cymule_core::Machine::new().snapshot());
        let transition = StateRootManifest::genesis(&state).expect("current genesis builds");
        let mut manifest = transition.manifest().clone();
        manifest.manifest_version = "cymule.durable-state-root/4".to_owned();
        assert!(manifest.verify().is_err());

        let mut query = StateRootValue::run_query_indexes(
            "run:predecessor-query-index",
            RunQueryIndexRoots::default(),
        )
        .expect("current query index encodes");
        let StateRootValue::RunQueryIndexes { index_version, .. } = &mut query else {
            panic!("query constructor returned another value kind")
        };
        *index_version = "cymule.run-query-indexes/2".to_owned();
        assert!(query.verify().is_err());
    }

    #[test]
    fn reachable_audit_rejects_cross_family_leaf_kind() {
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let wrong = StateRootValue::encode(StateRootLeafKind::JournalRecord, &record(1))
            .expect("wrong-kind leaf is still internally valid");
        let machine_plans = build_value_map(vec![("not-a-plan".to_owned(), wrong)], &mut overlay)
            .expect("map builds");
        let mut roots = StateRoots::empty();
        roots.continuations = machine_plans;
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("manifest builds");
        let objects = overlay.finish(&manifest).expect("object batch closes");
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(objects);
        assert!(matches!(
            reachable_state_root_objects(&manifest, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_reachable_value_kind_mismatch"
        ));
    }

    #[test]
    fn full_audit_rejects_extra_active_lease_without_claimed_effect() {
        let run_id = "run:lease-extra";
        let intent_id = cymule_core::content_id("cymule.test.state-root-extra-lease/1", &run_id)
            .expect("lease resource derives");
        let lease = crate::CoordinationLease {
            resource: intent_id.clone(),
            owner: "worker:lease-extra".to_owned(),
            epoch: 1,
            expires_at: 10,
        };
        lease.verify().expect("test lease verifies");
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let leases = build_typed_map(
            StateRootLeafKind::Lease,
            BTreeMap::from([(intent_id, lease)]),
            &mut overlay,
        )
        .expect("lease map builds");
        let descriptor = StateRootValue::run_query_indexes(
            run_id,
            RunQueryIndexRoots {
                active_leases: leases.clone(),
                ..RunQueryIndexRoots::default()
            },
        )
        .expect("query descriptor builds");
        let mut roots = StateRoots::empty();
        roots.leases = leases;
        roots.run_query_indexes =
            build_value_map(vec![(run_id.to_owned(), descriptor)], &mut overlay)
                .expect("query index builds");
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("manifest builds");
        let objects = overlay.finish(&manifest).expect("object graph closes");
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(objects);
        assert!(matches!(
            reachable_state_root_objects(&manifest, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_active_lease_effect_missing"
        ));
    }

    #[test]
    fn unrelated_global_lease_does_not_enter_any_run_current_index() {
        let lease = crate::CoordinationLease {
            resource: "profile:unrelated".to_owned(),
            owner: "worker:profile".to_owned(),
            epoch: 1,
            expires_at: 10,
        };
        lease.verify().expect("test lease verifies");
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let mut roots = StateRoots::empty();
        roots.leases = build_typed_map(
            StateRootLeafKind::Lease,
            BTreeMap::from([(lease.resource.clone(), lease)]),
            &mut overlay,
        )
        .expect("global lease map builds");
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("manifest builds");
        let objects = overlay.finish(&manifest).expect("object graph closes");
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(objects);
        reachable_state_root_objects(&manifest, &mut resolver)
            .expect("unrelated lease remains outside every Run current index");
    }

    #[test]
    fn full_audit_rejects_claimed_effect_lease_owner_or_epoch_mismatch() {
        let run_id = "run:lease-mismatch";
        let dispatch = claimed_dispatch(run_id, "worker:expected", 3);
        let lease = crate::CoordinationLease {
            resource: dispatch.intent_id.clone(),
            owner: "worker:wrong".to_owned(),
            epoch: 4,
            expires_at: 10,
        };
        lease.verify().expect("test lease verifies");
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let effects = build_typed_map(
            StateRootLeafKind::Outbox,
            BTreeMap::from([(dispatch.intent_id.clone(), dispatch.clone())]),
            &mut overlay,
        )
        .expect("Effect map builds");
        let leases = build_typed_map(
            StateRootLeafKind::Lease,
            BTreeMap::from([(lease.resource.clone(), lease)]),
            &mut overlay,
        )
        .expect("lease map builds");
        let descriptor = StateRootValue::run_query_indexes(
            run_id,
            RunQueryIndexRoots {
                effects: effects.clone(),
                active_effects: effects.clone(),
                active_leases: leases.clone(),
                ..RunQueryIndexRoots::default()
            },
        )
        .expect("query descriptor builds");
        let mut roots = StateRoots::empty();
        roots.outbox = build_typed_map(
            StateRootLeafKind::OutboxOwner,
            BTreeMap::from([(
                dispatch.intent_id.clone(),
                OutboxOwner {
                    intent_id: dispatch.intent_id.clone(),
                    run_id: run_id.to_owned(),
                },
            )]),
            &mut overlay,
        )
        .expect("owner locator builds");
        roots.leases = leases;
        roots.run_query_indexes =
            build_value_map(vec![(run_id.to_owned(), descriptor)], &mut overlay)
                .expect("query index builds");
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("manifest builds");
        let objects = overlay.finish(&manifest).expect("object graph closes");
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(objects);
        assert!(matches!(
            reachable_state_root_objects(&manifest, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_active_lease_index_mismatch"
        ));
    }

    #[test]
    fn full_audit_rejects_claimed_effect_missing_active_lease() {
        let run_id = "run:lease-missing";
        let dispatch = claimed_dispatch(run_id, "worker:missing", 2);
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let effects = build_typed_map(
            StateRootLeafKind::Outbox,
            BTreeMap::from([(dispatch.intent_id.clone(), dispatch.clone())]),
            &mut overlay,
        )
        .expect("Effect map builds");
        let descriptor = StateRootValue::run_query_indexes(
            run_id,
            RunQueryIndexRoots {
                effects: effects.clone(),
                active_effects: effects.clone(),
                ..RunQueryIndexRoots::default()
            },
        )
        .expect("query descriptor builds");
        let mut roots = StateRoots::empty();
        roots.outbox = build_typed_map(
            StateRootLeafKind::OutboxOwner,
            BTreeMap::from([(
                dispatch.intent_id.clone(),
                OutboxOwner {
                    intent_id: dispatch.intent_id.clone(),
                    run_id: run_id.to_owned(),
                },
            )]),
            &mut overlay,
        )
        .expect("owner locator builds");
        roots.run_query_indexes =
            build_value_map(vec![(run_id.to_owned(), descriptor)], &mut overlay)
                .expect("query index builds");
        let frontier = empty_machine_frontier();
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("manifest builds");
        let objects = overlay.finish(&manifest).expect("object graph closes");
        let mut resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            ..TestResolver::default()
        };
        resolver.insert_all(objects);
        assert!(matches!(
            reachable_state_root_objects(&manifest, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_claimed_effect_lease_missing"
        ));
    }

    #[test]
    fn revision_binds_result_roots_sequence_and_parent_delta() {
        let roots = StateRoots::empty();
        let frontier = empty_machine_frontier();
        let genesis = derive_genesis_revision(revision_state(&frontier, &roots))
            .expect("genesis revision derives");
        let first = derive_transition_revision(
            DurableRevisionLineage {
                parent_revision: &genesis,
                delta_digest: &"2".repeat(64),
                sequence: 1,
            },
            revision_state(&frontier, &roots),
        )
        .expect("transition revision derives");
        let next_sequence = derive_transition_revision(
            DurableRevisionLineage {
                parent_revision: &genesis,
                delta_digest: &"2".repeat(64),
                sequence: 2,
            },
            revision_state(&frontier, &roots),
        )
        .expect("sequence revision derives");
        let next_delta = derive_transition_revision(
            DurableRevisionLineage {
                parent_revision: &genesis,
                delta_digest: &"3".repeat(64),
                sequence: 1,
            },
            revision_state(&frontier, &roots),
        )
        .expect("delta revision derives");
        assert_ne!(first, next_sequence);
        assert_ne!(first, next_delta);
    }

    #[test]
    fn fixed_manifest_bound_covers_maximal_machine_physical_frontiers() {
        let object_id = format!("sha256:{}", "6".repeat(64));
        let maximal = MapRoot {
            node: Some(object_id.clone()),
            entries: MAX_EXACT_INTEGER,
        };
        maximal.verify().expect("maximal frontier verifies");
        let roots = StateRoots::empty();
        let mut frontier = empty_machine_frontier();
        frontier.runs = maximal.clone();
        frontier.facts = maximal.clone();
        frontier.pending_commands = maximal.clone();
        frontier.paged_transitions = maximal;
        frontier.verify().expect("max frontier verifies");
        let revision =
            derive_genesis_revision(revision_state(&frontier, &roots)).expect("revision derives");
        let manifest = StateRootManifest::new(
            StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("maximal fixed manifest fits");
        assert!(
            cymule_core::canonical_bytes(&manifest)
                .expect("manifest encodes")
                .len()
                <= MAX_STATE_ROOT_MANIFEST_BYTES
        );
    }

    #[test]
    fn state_root_leaf_uses_utf8_json_and_rejects_old_numeric_arrays() {
        let record = record(7);
        let canonical = cymule_core::canonical_bytes(&record).expect("record canonicalizes");
        let leaf = StateRootValue::encode(StateRootLeafKind::JournalRecord, &record)
            .expect("typed UTF-8 leaf encodes");
        assert_eq!(leaf.canonical_json(), Some(canonical.as_slice()));
        assert_eq!(
            leaf.decode::<crate::JournalRecord>(StateRootLeafKind::JournalRecord)
                .expect("typed round trip verifies"),
            record
        );
        let wire_value = serde_json::to_value(&leaf).expect("leaf serializes");
        assert_eq!(
            wire_value["canonical_json"].as_str(),
            Some(std::str::from_utf8(&canonical).expect("canonical JSON is UTF-8"))
        );
        let value = StateValueObject::new(leaf.clone()).expect("value seals");
        assert_eq!(
            value.object_id,
            cymule_core::content_id(STATE_ROOT_VALUE_VERSION, &leaf)
                .expect("existing domain framing derives identity")
        );
        let object = StateRootObject::Value(value);
        assert_eq!(
            decode_state_root_object(&cymule_core::canonical_bytes(&object).expect("wire encodes"))
                .expect("UTF-8 wire round trips"),
            object
        );
        let mut legacy_leaf = wire_value;
        legacy_leaf["canonical_json"] =
            serde_json::to_value(&canonical).expect("legacy bytes encode");
        assert!(serde_json::from_value::<StateRootValue>(legacy_leaf.clone()).is_err());
        let legacy_object = serde_json::json!({
            "object": "value",
            "payload": {
                "value_version": STATE_ROOT_VALUE_VERSION,
                "object_id": cymule_core::content_id(STATE_ROOT_VALUE_VERSION, &legacy_leaf)
                    .expect("old array shape has its own self-consistent identity"),
                "value": legacy_leaf,
            },
        });
        assert!(
            decode_state_root_object(
                &cymule_core::canonical_bytes(&legacy_object).expect("old transport canonicalizes")
            )
            .is_err()
        );
    }

    #[test]
    fn state_root_leaf_utf8_keeps_exact_byte_limit_and_canonical_gate() {
        let text = "é".repeat((MAX_STATE_ROOT_LEAF_BYTES - 2) / 2);
        let leaf = StateRootValue::encode(StateRootLeafKind::MachineFact, &text)
            .expect("maximum canonical UTF-8 byte count fits");
        assert_eq!(
            leaf.canonical_json().expect("leaf bytes exist").len(),
            MAX_STATE_ROOT_LEAF_BYTES
        );
        assert!(text.chars().count() < MAX_STATE_ROOT_LEAF_BYTES);
        let mut too_large = text;
        too_large.push('x');
        assert!(matches!(
            StateRootValue::encode(StateRootLeafKind::MachineFact, &too_large),
            Err(DurableError::Validation(message)) if message.contains("state-root leaf exceeds")
        ));
        for canonical_json in [" \"fact\"", "\"\\u0066act\"", "null"] {
            let invalid = StateRootValue::Leaf {
                kind: StateRootLeafKind::MachineFact,
                canonical_json: canonical_json.to_owned(),
            };
            assert!(invalid.verify().is_err(), "accepted {canonical_json:?}");
        }
    }

    #[test]
    fn state_root_leaf_utf8_retains_core_safe_number_validation() {
        let mut wire = serde_json::to_value(record(9)).expect("record serializes");
        wire["payload"] = serde_json::json!({"unsafe": 9_007_199_254_740_992_u64});
        let leaf = StateRootValue::Leaf {
            kind: StateRootLeafKind::JournalRecord,
            canonical_json: serde_json::to_string(&wire).expect("untrusted JSON encodes"),
        };
        assert!(matches!(
            leaf.verify(),
            Err(DurableError::Integrity { message, .. })
                if message.contains("exact cross-language range")
        ));
    }

    #[test]
    fn machine_base_chunk_uses_base64_and_rejects_numeric_wire() {
        use base64::Engine as _;

        let bytes = vec![0, 0xff, 0x80, b'a', b'\n'];
        assert!(std::str::from_utf8(&bytes).is_err());
        let chunk = StateRootValue::MachineBaseChunk {
            index: 0,
            bytes: bytes.clone(),
        };
        let mut value = serde_json::to_value(&chunk).expect("binary chunk serializes");
        assert_eq!(
            value["bytes"],
            base64::engine::general_purpose::STANDARD.encode(&bytes)
        );
        let object =
            StateRootObject::Value(StateValueObject::new(chunk).expect("chunk identity seals"));
        assert_eq!(
            decode_state_root_object(
                &cymule_core::canonical_bytes(&object).expect("chunk wire encodes")
            )
            .expect("binary chunk wire round trips"),
            object
        );
        value["bytes"] = serde_json::to_value(&bytes).expect("old numeric bytes encode");
        assert!(serde_json::from_value::<StateRootValue>(value.clone()).is_err());
        for invalid in ["AA", "AB==", "AAB=", "AA-_", "AA==AA=="] {
            value["bytes"] = serde_json::Value::String(invalid.to_owned());
            assert!(serde_json::from_value::<StateRootValue>(value.clone()).is_err());
        }
    }

    #[test]
    fn machine_base_chunks_preserve_split_utf8_codepoints() {
        let mut canonical = Vec::with_capacity(MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES + 2);
        canonical.push(b'"');
        canonical.resize(MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES - 1, b'x');
        canonical.extend_from_slice("é\"".as_bytes());
        let text: String =
            cymule_core::decode_json(&canonical).expect("complete JSON is valid UTF-8");
        assert_eq!(
            cymule_core::canonical_bytes(&text).expect("JSON canonicalizes"),
            canonical
        );
        assert!(
            std::str::from_utf8(&canonical[..MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES]).is_err()
        );
        assert!(
            std::str::from_utf8(&canonical[MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES..]).is_err()
        );
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let descriptor =
            build_machine_base_bytes(&canonical, &mut overlay).expect("binary chunks build");
        let mut resolver = TestResolver::default();
        for object in overlay.pending.into_values() {
            let physical = cymule_core::canonical_bytes(&object).expect("chunk object encodes");
            let decoded = decode_state_root_object(&physical).expect("chunk object decodes");
            resolver.insert_all([decoded]);
        }
        assert_eq!(
            materialize_machine_base_bytes(&descriptor, &mut resolver)
                .expect("split UTF-8 reassembles"),
            canonical
        );
    }

    #[test]
    fn state_root_object_rejects_canonical_bytes_over_protocol_bound() {
        const {
            assert!(MAX_STATE_ROOT_OBJECT_BYTES == 64 * 1024 * 1024);
        }
        let oversized = StateRootObject::Value(StateValueObject {
            value_version: STATE_ROOT_VALUE_VERSION.to_owned(),
            object_id: format!("sha256:{}", "7".repeat(64)),
            value: StateRootValue::Leaf {
                kind: StateRootLeafKind::MachineArtifact,
                canonical_json: "\0".repeat(MAX_STATE_ROOT_OBJECT_BYTES / 6 + 1),
            },
        });
        assert!(
            matches!(oversized.verify(), Err(DurableError::Validation(message)) if message.contains("state-root object exceeds"))
        );
    }

    #[test]
    fn core_max_artifact_fits_the_unique_state_root_leaf_and_object_bounds() {
        let bytes = vec![u8::MAX; cymule_core::MAX_ARTIFACT_BYTES];
        let reference = cymule_core::artifact_ref("cymule.test-artifact/1", &bytes)
            .expect("Core maximum Artifact reference derives");
        let artifact = cymule_core::ArtifactRecord { reference, bytes };
        artifact
            .validate()
            .expect("Core maximum Artifact validates");
        let canonical = cymule_core::canonical_bytes(&artifact).expect("Artifact canonicalizes");
        assert!(canonical.len() <= MAX_STATE_ROOT_LEAF_BYTES);
        let object = StateRootObject::Value(
            StateValueObject::new(
                StateRootValue::encode(StateRootLeafKind::MachineArtifact, &artifact)
                    .expect("maximum Artifact leaf encodes"),
            )
            .expect("maximum Artifact value object seals"),
        );
        object
            .verify()
            .expect("every Core-valid maximum Artifact fits StateRoot");
        assert!(
            cymule_core::canonical_bytes(&object)
                .expect("maximum Artifact object canonicalizes")
                .len()
                <= MAX_STATE_ROOT_OBJECT_BYTES
        );
        assert!(
            cymule_core::artifact_ref(
                "cymule.test-artifact/1",
                &vec![0; cymule_core::MAX_ARTIFACT_BYTES + 1],
            )
            .is_err()
        );
    }

    #[test]
    fn state_root_object_decoder_round_trips_every_physical_variant() {
        let mut state = crate::DurableState::new(cymule_core::Machine::default().snapshot());
        state.application_journals.insert(
            "journal:decoder".to_owned(),
            crate::ApplicationJournal::try_from_records(vec![record(1), record(2)])
                .expect("decoder journal seals"),
        );
        let transition = StateRootManifest::genesis(&state).expect("decoder graph builds");
        let kinds = transition
            .objects
            .iter()
            .map(|object| match object {
                StateRootObject::Value(_) => "value",
                StateRootObject::MapNode(_) => "map",
                StateRootObject::LogNode(_) => "log",
                StateRootObject::Manifest(_) => "manifest",
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(kinds, BTreeSet::from(["value", "map", "log", "manifest"]));
        for object in transition.objects {
            let bytes = cymule_core::canonical_bytes(&object).expect("object canonicalizes");
            assert_eq!(
                decode_state_root_object(&bytes).expect("bounded decoder accepts canonical object"),
                object
            );
        }
    }

    #[test]
    fn state_root_object_decoder_rejects_oversized_and_noncanonical_transport() {
        let oversized = vec![b' '; MAX_STATE_ROOT_OBJECT_BYTES + 1];
        assert!(matches!(
            decode_state_root_object(&oversized),
            Err(DurableError::Validation(message)) if message.contains("state-root object exceeds")
        ));

        let value = StateRootObject::Value(
            StateValueObject::new(
                StateRootValue::encode(StateRootLeafKind::JournalRecord, &record(1))
                    .expect("record encodes"),
            )
            .expect("value object seals"),
        );
        let canonical = cymule_core::canonical_bytes(&value).expect("value canonicalizes");
        let mut noncanonical = Vec::with_capacity(canonical.len() + 1);
        noncanonical.push(b' ');
        noncanonical.extend(canonical);
        assert!(decode_state_root_object(&noncanonical).is_err());
    }

    #[test]
    fn state_root_leaf_rejects_canonical_bytes_over_leaf_bound() {
        let oversized = StateRootObject::Value(StateValueObject {
            value_version: STATE_ROOT_VALUE_VERSION.to_owned(),
            object_id: format!("sha256:{}", "8".repeat(64)),
            value: StateRootValue::Leaf {
                kind: StateRootLeafKind::MachineArtifact,
                canonical_json: "x".repeat(MAX_STATE_ROOT_LEAF_BYTES + 1),
            },
        });
        assert!(
            matches!(oversized.verify(), Err(DurableError::Validation(message)) if message.contains("state-root leaf exceeds"))
        );
    }

    #[test]
    fn machine_base_codec_round_trips_beyond_leaf_bound_and_chunk_boundary() {
        let canonical = (0..(MAX_STATE_ROOT_LEAF_BYTES + 257))
            .map(|index| u8::try_from(index % 251).expect("modulo fits"))
            .collect::<Vec<_>>();
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let descriptor =
            build_machine_base_bytes(&canonical, &mut overlay).expect("large base chunks");
        let mut resolver = TestResolver::default();
        resolver.insert_all(overlay.pending.into_values());
        assert_eq!(
            materialize_machine_base_bytes(&descriptor, &mut resolver)
                .expect("large base reassembles"),
            canonical
        );
    }

    #[test]
    fn machine_base_codec_round_trips_real_typed_base() {
        let base = compacted_machine_base();
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let descriptor = build_machine_base(&base, &mut overlay).expect("typed base chunks");
        let mut resolver = TestResolver::default();
        resolver.insert_all(overlay.pending.into_values());
        assert_eq!(
            materialize_machine_base(&descriptor, &mut resolver).expect("typed base reassembles"),
            base
        );
    }

    #[test]
    fn machine_base_codec_rejects_missing_misordered_and_wrong_digest_chunks() {
        assert_missing_base_chunk_rejected();
        assert_misordered_base_chunks_rejected();
        assert_wrong_base_count_and_digest_rejected();
        assert_wrong_base_length_rejected();
        assert_non_json_machine_base_rejected();
    }

    fn assert_missing_base_chunk_rejected() {
        let canonical = vec![b'x'; MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES + 1];
        let mut empty = EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let descriptor =
            build_machine_base_bytes(&canonical, &mut overlay).expect("base chunks build");
        let mut resolver = TestResolver::default();
        resolver.insert_all(overlay.pending.clone().into_values());
        let missing = resolver
            .objects
            .iter()
            .find_map(|(object_id, object)| {
                matches!(
                    object,
                    StateRootObject::Value(StateValueObject {
                        value: StateRootValue::MachineBaseChunk { index: 1, .. },
                        ..
                    })
                )
                .then_some(object_id.clone())
            })
            .expect("second chunk exists");
        resolver.objects.remove(&missing);
        assert!(matches!(
            materialize_machine_base_bytes(&descriptor, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_reachable_object_missing"
        ));
    }

    fn assert_misordered_base_chunks_rejected() {
        let mut empty = EmptyStateRootResolver;
        let mut wrong = ObjectOverlay::new(&mut empty);
        let chunks = vec![
            StateRootValue::MachineBaseChunk {
                index: 1,
                bytes: vec![b'a'; MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES],
            },
            StateRootValue::MachineBaseChunk {
                index: 0,
                bytes: vec![b'b'],
            },
        ];
        let chunks = log_append(&LogRoot::empty(), &chunks, &mut wrong)
            .expect("misordered chunk log builds");
        let misordered = wrong
            .insert_value(StateRootValue::MachineBaseDescriptor {
                canonical_len: u64::try_from(MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES + 1)
                    .expect("chunk length fits"),
                canonical_digest: cymule_core::sha256_bytes(b"wrong-order"),
                chunk_count: 2,
                chunks,
            })
            .expect("misordered descriptor builds");
        let mut resolver = TestResolver::default();
        resolver.insert_all(wrong.pending.into_values());
        assert!(matches!(
            materialize_machine_base_bytes(&misordered, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_machine_base_chunk_order"
        ));
    }

    fn assert_wrong_base_count_and_digest_rejected() {
        let mut empty = EmptyStateRootResolver;
        let mut wrong = ObjectOverlay::new(&mut empty);
        let chunks = log_append(
            &LogRoot::empty(),
            &[StateRootValue::MachineBaseChunk {
                index: 0,
                bytes: vec![b'a'],
            }],
            &mut wrong,
        )
        .expect("single chunk builds");
        assert!(
            StateRootValue::MachineBaseDescriptor {
                canonical_len: 1,
                canonical_digest: cymule_core::sha256_bytes(b"a"),
                chunk_count: 2,
                chunks: chunks.clone(),
            }
            .verify()
            .is_err()
        );

        let wrong_digest = wrong
            .insert_value(StateRootValue::MachineBaseDescriptor {
                canonical_len: 1,
                canonical_digest: "0".repeat(64),
                chunk_count: 1,
                chunks,
            })
            .expect("wrong-digest descriptor builds");
        let mut resolver = TestResolver::default();
        resolver.insert_all(wrong.pending.into_values());
        assert!(matches!(
            materialize_machine_base_bytes(&wrong_digest, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_machine_base_bytes_mismatch"
        ));
    }

    fn assert_wrong_base_length_rejected() {
        let mut empty = EmptyStateRootResolver;
        let mut wrong = ObjectOverlay::new(&mut empty);
        let chunks = log_append(
            &LogRoot::empty(),
            &[StateRootValue::MachineBaseChunk {
                index: 0,
                bytes: b"ab".to_vec(),
            }],
            &mut wrong,
        )
        .expect("overlong chunk builds");
        let wrong_length = wrong
            .insert_value(StateRootValue::MachineBaseDescriptor {
                canonical_len: 1,
                canonical_digest: cymule_core::sha256_bytes(b"a"),
                chunk_count: 1,
                chunks,
            })
            .expect("overlong descriptor builds");
        let mut resolver = TestResolver::default();
        resolver.insert_all(wrong.pending.into_values());
        assert!(matches!(
            materialize_machine_base_bytes(&wrong_length, &mut resolver),
            Err(DurableError::Integrity { code, .. })
                if code == "state_root_machine_base_length_overflow"
        ));
    }

    fn assert_non_json_machine_base_rejected() {
        let mut empty = EmptyStateRootResolver;
        let mut invalid = ObjectOverlay::new(&mut empty);
        let descriptor = build_machine_base_bytes(b"not-json", &mut invalid)
            .expect("raw codec accepts opaque canonical candidate bytes");
        let mut resolver = TestResolver::default();
        resolver.insert_all(invalid.pending.into_values());
        assert!(materialize_machine_base(&descriptor, &mut resolver).is_err());
    }
}
