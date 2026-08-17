//! Provider-neutral virtual regions and bounded deterministic scheduling.

mod durable;
mod error;
mod model;
mod scheduler;

pub use durable::{DurableVirtualController, VIRTUAL_CHECKPOINT_SCHEMA, VirtualCheckpoint};
pub use error::{VirtualError, VirtualResult};
pub use model::{
    ClaimedWork, FrontierLimits, MaterializedPage, ParkReason, ParkedWork, VirtualCursor,
    VirtualRegion, VirtualSnapshot, WorkItem,
};
pub use scheduler::{RegionSource, VirtualScheduler};
