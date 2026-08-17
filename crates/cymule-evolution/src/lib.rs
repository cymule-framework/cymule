//! Provider-neutral Plan DAG, impact, rollout, migration, and rollback semantics.

mod controller;
mod error;
mod model;

pub use controller::EvolutionController;
pub use error::{EvolutionError, EvolutionResult};
pub use model::{
    EvolutionSnapshot, ImpactCone, MigrationReceipt, PatchOperation, PlanEdge, PlanNode,
    RolloutDecision, RolloutMode, ShadowComparison,
};
