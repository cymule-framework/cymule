//! The small, deterministic Cymule semantic kernel.
//!
//! This crate owns canonical identity, the frozen IR, command admission,
//! state-machine reduction, and replay. It deliberately performs no ambient I/O.

mod canonical;
mod error;
mod ir;
mod machine;
mod model;

pub use canonical::{canonical_bytes, canonical_digest, content_id, decode_json, sha256_bytes};
pub use error::{CoreError, Result};
pub use ir::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    IR_VERSION, MutationKind, Operation, PlanCandidate, ReconciliationMode, Region, ScopeMode,
    SealedPlan, Step, WaitSpec, seal_plan,
};
pub use machine::{Machine, MachineBaseSnapshot, MachineCompaction, MachineSnapshot};
pub use model::{
    ARTIFACT_IDENTITY_VERSION, ArtifactRecord, ArtifactRef, AttemptProjection, COMMAND_VERSION,
    Command, CommandEnvelope, CommandReceipt, CommandReceiptStatus, CompactionCertificate,
    EVENT_VERSION, EffectPhase, EffectProjection, EffectTransition, Event, EventPayload,
    ObligationProjection, Projection, ROOT_SCOPE_ID, ReconciliationResolution, ReconciliationState,
    ReplayAvailability, RunProjection, RunStatus, SEMANTIC_VERSION, ScopeProjection, ScopeStatus,
    WorldOutcome, artifact_ref, effect_intent_id, effect_obligation_id,
};
