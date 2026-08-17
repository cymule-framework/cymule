//! Provider-neutral durable single-domain contracts and reference coordinator.

mod coordinator;
mod error;
mod executor;
mod model;
mod store;
mod wait_source;

pub use coordinator::DurableCoordinator;
pub use error::{DurableError, DurableResult};
pub use executor::{DriveOutcome, ResumableRuntime};
pub use model::{
    AuthorityLease, ComponentOccurrence, Continuation, ContinuationStatus, DurableState,
    EffectDispatch, FrameState, JournalBatch, JournalRecord, OutboxState, SnapshotRecord,
    StoredState, WAIT_ACTIVATION_VERSION, WaitActivation, WaitActivationSource, WaitCondition,
    WaitKind, WaitState,
};
pub use store::{DurableStore, MemoryStore, StoreCommit};
pub use wait_source::{
    MAX_WAIT_DELIVERY_TARGETS, ParkedWaitIndex, WaitDelivery, WaitSelection, WaitSourceDriver,
};
