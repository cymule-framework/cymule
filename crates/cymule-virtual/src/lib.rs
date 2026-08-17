//! Provider-neutral virtual regions and bounded deterministic scheduling.

mod durable;
mod error;
mod model;
mod scheduler;

pub use durable::{DurableVirtualController, VIRTUAL_CHECKPOINT_SCHEMA, VirtualCheckpoint};
pub use error::{VirtualError, VirtualResult};
pub use model::{
    ClaimedWork, FrontierLimits, MaterializedPage, ParkReason, ParkedWork, RegionMigrationCommand,
    RegionMigrationKind, RegionMigrationPlan, RegionMigrationReceipt, RegionMigrationRequest,
    SchedulingPolicy, VIRTUAL_REGION_MIGRATION_CONTROL_VERSION, VIRTUAL_REGION_MIGRATION_VERSION,
    VIRTUAL_WORK_CONTROL_VERSION, VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualCursor, VirtualRegion,
    VirtualSnapshot, WorkItem, WorkOccurrence, WorkOccurrenceState, WorkResolution,
    WorkResolutionCommand,
};
pub use scheduler::{RegionMigrator, RegionSource, VirtualScheduler};
