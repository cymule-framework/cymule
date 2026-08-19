//! Provider-neutral Plan DAG, impact, rollout, migration, and rollback semantics.

mod adapters;
mod compatibility;
mod control;
mod controller;
mod durable;
mod error;
mod linker;
mod live;
mod live_control;
mod model;
mod registry_durable;

pub use adapters::{
    MIGRATION_SAFE_POINT_VERSION, MigrationAdapter, MigrationAdapterDescriptor,
    MigrationCapabilityChange, MigrationOutput, MigrationPreservation, MigrationRequest,
    MigrationSafePoint, MigrationStateCoverage, ShadowBindingMode, ShadowDriver,
    ShadowDriverDescriptor, ShadowEffectMode, ShadowOutput, ShadowRequest,
};
pub use compatibility::{
    RELINK_COMPATIBILITY_VERSION, RelinkCompatibility, RelinkViolation, analyze_relink,
};
pub use control::{EVOLUTION_CONTROL_VERSION, EvolutionCommand};
pub use controller::{EvolutionController, diff_plans};
pub use durable::{DurableEvolutionController, EVOLUTION_CHECKPOINT_SCHEMA, EvolutionCheckpoint};
pub use error::{EvolutionError, EvolutionResult};
pub use linker::{
    DEFINITION_REGISTRY_VERSION, DefinitionRegistry, DefinitionRegistrySnapshot, LinkedPlan,
    PlanTemplate, ReferenceStrategy, SUBFLOW_REVISION_VERSION, SubflowReference, SubflowRevision,
};
pub use live::{
    DurableLiveEvolutionController, LIVE_EVOLUTION_CHECKPOINT_SCHEMA, LIVE_EVOLUTION_VERSION,
    LiveEvolutionCheckpoint, LiveEvolutionController, LiveEvolutionSnapshot, LiveMigrationCommand,
    LivePublicationCommand, LivePublicationReceipt, LivePublicationRecord, LiveTemplateUpdate,
};
pub use live::{LiveVirtualClaimCommand, LiveVirtualClaimReceipt};
pub use live_control::{
    LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand, LiveEvolutionResponse,
};
pub use model::{
    EvolutionSnapshot, GateOutcome, ImpactCone, MigrationReceipt, ObservationOutcome,
    PatchOperation, PlanEdge, PlanNode, PlanPatch, RestartReceipt, RestartRequest, RolloutDecision,
    RolloutEvaluation, RolloutGate, RolloutMode, RolloutObservation, RolloutTransition,
    ShadowComparison,
};
pub use registry_durable::{
    DEFINITION_REGISTRY_CHECKPOINT_SCHEMA, DefinitionRegistryCheckpoint, DurableDefinitionRegistry,
};
