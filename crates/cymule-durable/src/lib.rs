//! Provider-neutral durable single-domain contracts and reference coordinator.

mod coordinator;
mod error;
mod model;
mod store;

pub use coordinator::DurableCoordinator;
pub use error::{DurableError, DurableResult};
pub use model::{
    AuthorityLease, ComponentOccurrence, Continuation, ContinuationStatus, DurableState,
    EffectDispatch, FrameState, OutboxState, SnapshotRecord, StoredState, WaitCondition, WaitKind,
    WaitState,
};
pub use store::{DurableStore, MemoryStore, StoreCommit};
