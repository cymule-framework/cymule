//! Rust authoring and engine client facade.

mod builder;
mod client;
mod control;

pub use builder::FlowBuilder;
pub use client::{CliEngine, Engine};
pub use control::VirtualWorkControl;
pub use cymule_core::{
    ArtifactRef, DispatchPolicy, EffectProfile, Expression, MutationKind, PlanCandidate,
    ReconciliationMode, ScopeMode, SealedPlan, WaitSpec,
};
pub use cymule_durable::{WAIT_ACTIVATION_VERSION, WaitActivation, WaitActivationSource};
pub use cymule_resource::{
    InlineData, ResourceCandidate, ResourceHandle, ResourceHandoff, ResourceIntegrity,
    ResourceLocation, ResourceReplayClass, ResourceShape,
};
pub use cymule_runtime::ExecutionResult;
pub use cymule_virtual::{
    ParkReason, VIRTUAL_WORK_CONTROL_VERSION, VIRTUAL_WORK_OCCURRENCE_VERSION, WorkOccurrence,
    WorkOccurrenceState, WorkResolution, WorkResolutionCommand,
};
