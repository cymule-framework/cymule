//! Provider-neutral virtual regions and bounded deterministic scheduling.

mod archive;
mod durable;
mod error;
mod model;
mod scheduler;

pub use archive::{VIRTUAL_ARCHIVE_MANIFEST_KIND, VirtualArchive, virtual_archive_record};
pub use durable::{DurableVirtualController, VIRTUAL_CHECKPOINT_SCHEMA, VirtualCheckpoint};
pub use error::{VirtualError, VirtualResult};
pub use model::{
    ArchivedWorkIndex, ClaimedWork, CompactedWorkIndex, FrontierLimits, MaterializedPage,
    ParkReason, ParkedWork, RegionMigrationCommand, RegionMigrationKind, RegionMigrationPlan,
    RegionMigrationReceipt, RegionMigrationRequest, SchedulingPolicy,
    VIRTUAL_ARCHIVE_MANIFEST_VERSION, VIRTUAL_COMPACTION_CERTIFICATE_VERSION,
    VIRTUAL_COMPACTION_CONTROL_VERSION, VIRTUAL_REGION_MIGRATION_CONTROL_VERSION,
    VIRTUAL_REGION_MIGRATION_VERSION, VIRTUAL_REHYDRATION_CONTROL_VERSION,
    VIRTUAL_WORK_CONTROL_VERSION, VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualArchiveManifest,
    VirtualCompactionCertificate, VirtualCompactionCommand, VirtualCompactionReceipt,
    VirtualCompletionSummary, VirtualCursor, VirtualRegion, VirtualRehydrationCommand,
    VirtualRehydrationReceipt, VirtualSnapshot, WorkItem, WorkOccurrence, WorkOccurrenceState,
    WorkResolution, WorkResolutionCommand,
};
pub use scheduler::{RegionMigrator, RegionSource, VirtualScheduler};
