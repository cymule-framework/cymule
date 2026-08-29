//! Provider-neutral durable single-domain contracts and reference coordinator.

mod clock;
mod control;
mod coordinator;
mod error;
mod executor;
mod model;
mod retry;
mod state_root;
mod store;
mod wait_source;

pub use clock::{ClockObservationAuthority, ExecutionClockAuthority};
pub use control::{
    CancellationCommand, CancellationReceipt, DURABLE_CONTROL_VERSION, DurableAgentControl,
    DurableAgentReadControl, DurableAttemptSummary, DurableBoundary, DurableCommand,
    DurableEffectSummary, DurableEvolutionControl, DurableExactRead, DurableOccurrenceSummary,
    DurablePageCursor, DurablePagePosition, DurablePageQueryKind, DurableProviderControl,
    DurableQueryPage, DurableResourceControl, DurableResponse, DurableRunAttemptPage,
    DurableRunCurrent, DurableRunEffectPage, DurableRunIndexPage, DurableRunIndexSummary,
    DurableRunItem, DurableRunItemSelector, DurableRunOccurrencePage, DurableRunWaitPage,
    DurableRuntimeControl, DurableStoreControl, DurableVirtualControl, DurableVirtualReadControl,
    DurableWaitSummary, EFFECT_RESOLUTION_RECEIPT_VERSION, EffectResolutionCommand,
    EffectResolutionReceipt, HistoryCompactionRequest, MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
    MAX_DURABLE_QUERY_PAGE_BYTES, MAX_DURABLE_QUERY_PAGE_ITEMS, MAX_DURABLE_QUERY_RUN_KEY_SCALARS,
    MAX_DURABLE_QUERY_SUMMARY_BYTES, RUN_CANCELLATION_RECEIPT_VERSION,
};
pub use coordinator::CANCELLATION_REASON_ARTIFACT_KIND;
pub(crate) use cymule_core::MAX_EXACT_INTEGER;
pub(crate) use cymule_durable_protocol::{
    ClockObservation, ClockObservationRef, Continuation, ContinuationExecutionClaim,
    ContinuationStatus, EXECUTION_CLAIM_VERSION, ExecutionClaimRequest, FrameState,
    MAX_WAIT_DELIVERY_TARGETS, WAIT_ACTIVATION_RECEIPT_VERSION, WAIT_RESULT_ARTIFACT_KIND,
    WaitActivation, WaitActivationDisposition, WaitActivationReceipt, WaitActivationSource,
    WaitOwner, execution_clock_scope,
};
pub use error::{DurableError, DurableResult};
pub use executor::{DriveOutcome, WaitAdmissionOutcome};
pub use model::validate_continuation_plan_frames;
pub use model::{
    APPLICATION_JOURNAL_PREFIX_REPLACEMENT_AUTHORITY_VERSION,
    APPLICATION_JOURNAL_PREFIX_REPLACEMENT_RECEIPT_VERSION, APPLICATION_JOURNAL_PREFIX_VERSION,
    AgentWorkspaceCheckpoint, ApplicationJournal, ApplicationJournalPrefix,
    ApplicationJournalPrefixReplacement, ApplicationJournalPrefixReplacementAuthority,
    ApplicationJournalPrefixReplacementReceipt, ApplicationJournalRecordRef,
    COMPONENT_OCCURRENCE_VERSION, COUPLED_CHECKPOINT_RECEIPT_VERSION, ComponentOccurrence,
    ComponentOccurrenceState, ComponentOutcome, CoordinationLease, CoupledCheckpoint,
    CoupledCheckpointReceipt, DURABLE_STATE_VERSION, DurableState, EffectDispatch,
    HISTORY_COMPACTION_VERSION, HistoryCompactionKind, HistoryCompactionReceipt, JournalBatch,
    JournalBatchManifest, JournalRecord, JournalRecordManifest,
    MAX_AGENT_WORKSPACE_CHECKPOINT_RECEIPT_BYTES, MAX_APPLICATION_JOURNAL_RECORD_BYTES,
    MAX_APPLICATION_JOURNAL_REPLACEMENT_RECORDS, MachineCompactionSummary,
    OPERATION_ATTEMPT_VERSION, OperationAttempt, OperationAttemptState, OutboxState,
    SnapshotRecord, StoredState, WaitCondition, WaitKind, WaitState, derive_wait_id,
};
pub use retry::{
    FailureClass, FailureOperation, JITTER_EVIDENCE_VERSION, JitterEvidence, JitterStrategy,
    RETRY_DECISION_VERSION, RETRY_POLICY_VERSION, RETRY_STREAM_VERSION, RetryAdmission,
    RetryCommand, RetryDecision, RetryDelay, RetryDisposition, RetryFailure, RetryPolicy,
    RetryStopReason, RetryStream, VerifiedRetryStream,
};
pub use state_root::{
    DURABLE_REVISION_VERSION, MAX_STATE_ROOT_LEAF_BYTES, MAX_STATE_ROOT_MACHINE_BASE_CHUNK_BYTES,
    MAX_STATE_ROOT_MANIFEST_BYTES, MAX_STATE_ROOT_OBJECT_BYTES, MaterializedStateRoots,
    RunTerminalSidecarCurrent, STATE_ROOT_MANIFEST_VERSION, STATE_ROOT_VALUE_VERSION,
    StateRootFamily, StateRootLeafKind, StateRootManifest, StateRootObject, StateRootResolver,
    StateRootTransition, StateRootValue, StateRoots, StateValueObject, decode_state_root_object,
    load_application_journal_prefix, load_application_journal_prefix_replacement_authority,
    load_application_journal_record_manifest, load_coupled_checkpoint_receipt,
    materialize_state_log, materialize_state_roots, reachable_state_root_objects, state_log_get,
    state_map_get,
};
pub(crate) use store::{DurableDelta, DurableOperation};
pub use store::{
    DurableStore, GC_RECEIPT_VERSION, GcReceipt, MAX_GC_RECEIPT_BYTES, MAX_GC_RECLAIMED_OBJECTS,
    MAX_STORE_HEAD_BYTES, MachineCommandIndexReachability, MemoryStore, PHYSICAL_TOKEN_VERSION,
    STORE_HEAD_VERSION, StoreBatch, StoreCommit, StoreHead, StoreReclamation, StoreStats,
    load_machine_command_archive, reachable_machine_command_archive_ids,
    reachable_machine_command_index_objects,
};
pub use wait_source::{
    ParkedWaitIndex, ParkedWaitView, SignalKeyPage, SignalKeyPageOutcome, WaitDelivery,
    WaitSelection, WaitSourceCursor, WaitSourceDelivery, WaitSourceDriver,
};
