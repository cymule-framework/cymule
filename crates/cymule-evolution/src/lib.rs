//! Provider-neutral Plan DAG, impact, rollout, migration, and rollback semantics.

mod controller;
mod durable;
mod error;
mod model;

pub use controller::{EvolutionController, diff_plans};
pub use durable::{DurableEvolutionController, EVOLUTION_CHECKPOINT_SCHEMA, EvolutionCheckpoint};
pub use error::{EvolutionError, EvolutionResult};
pub use model::{
    EvolutionSnapshot, ImpactCone, MigrationReceipt, PatchOperation, PlanEdge, PlanNode,
    RolloutDecision, RolloutMode, ShadowComparison,
};
