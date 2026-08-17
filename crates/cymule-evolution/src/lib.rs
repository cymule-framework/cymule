//! Provider-neutral Plan DAG, impact, rollout, migration, and rollback semantics.

mod controller;
mod durable;
mod error;
mod linker;
mod model;

pub use controller::{EvolutionController, diff_plans};
pub use durable::{DurableEvolutionController, EVOLUTION_CHECKPOINT_SCHEMA, EvolutionCheckpoint};
pub use error::{EvolutionError, EvolutionResult};
pub use linker::{
    DefinitionRegistry, LinkedPlan, PlanTemplate, ReferenceStrategy, SUBFLOW_REVISION_VERSION,
    SubflowReference, SubflowRevision,
};
pub use model::{
    EvolutionSnapshot, ImpactCone, MigrationReceipt, PatchOperation, PlanEdge, PlanNode,
    RolloutDecision, RolloutMode, ShadowComparison,
};
