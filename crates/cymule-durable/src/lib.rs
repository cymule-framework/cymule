//! Provider-neutral durable single-domain contracts and reference coordinator.

mod control;
mod coordinator;
mod error;
mod executor;
mod model;
mod retry;
mod store;
mod wait_source;

pub use control::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableDomainView, DurableResponse,
    DurableRunView, DurableRuntimeControl,
};
pub use coordinator::DurableCoordinator;
pub use error::{DurableError, DurableResult};
pub use executor::{DriveOutcome, ResumableRuntime};
pub use model::{
    AuthorityLease, ComponentOccurrence, Continuation, ContinuationStatus, DurableState,
    EffectDispatch, FrameState, HISTORY_COMPACTION_VERSION, HistoryCompactionReceipt, JournalBatch,
    JournalRecord, OutboxState, SnapshotRecord, StoredState, WAIT_ACTIVATION_VERSION,
    WaitActivation, WaitActivationSource, WaitCondition, WaitKind, WaitOwner, WaitState,
};
pub use retry::{
    CLOCK_OBSERVATION_VERSION, ClockObservation, FailureClass, FailureOperation,
    JITTER_EVIDENCE_VERSION, JitterEvidence, JitterStrategy, RETRY_DECISION_VERSION,
    RETRY_POLICY_VERSION, RETRY_STREAM_VERSION, RetryCommand, RetryDecision, RetryDelay,
    RetryDisposition, RetryFailure, RetryPolicy, RetryStopReason, RetryStream,
};
pub use store::{
    DurableStore, GC_RECEIPT_VERSION, GcReceipt, JsonDelta, MAX_HOT_SEGMENTS, MemoryStore,
    STATE_CHECKPOINT_VERSION, STATE_SEGMENT_VERSION, STORE_HEAD_VERSION, StateCheckpoint,
    StateSegment, StoreBatch, StoreCommit, StoreHead, StoreStats, restore,
};
pub use wait_source::{
    MAX_WAIT_DELIVERY_TARGETS, ParkedWaitIndex, WaitDelivery, WaitSelection, WaitSourceDriver,
};
