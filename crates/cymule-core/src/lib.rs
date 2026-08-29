//! The small, deterministic Cymule semantic kernel.
//!
//! This crate owns canonical identity, the frozen IR, command admission,
//! state-machine reduction, and replay. It deliberately performs no ambient I/O.
//! Event replay requires the exact sealed Plans and retained Artifacts because
//! an Event is evidence, not authority for its own Effect profile or execution
//! binding. Runs retain ordered Plan and binding lineages for historical frame
//! validation. Failure and cancellation fence execution while dispatched
//! unknown world outcomes remain independently settleable.
//! `cymule.ir/3` is the only admitted IR generation; legacy `/2` input has no
//! reader or translation path.

mod canonical;
mod error;
mod ir;
mod machine;
mod model;

pub use canonical::{
    canonical_bytes, canonical_digest, content_id, decode_json, sha256_bytes, validate_content_id,
};
pub use error::{CoreError, Result};
pub use ir::{
    ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    IR_VERSION, MutationKind, Operation, PlanCandidate, ReconciliationMode, Region, SealedPlan,
    Step, WaitSpec, seal_plan, validate_semantic_id,
};
pub use machine::{
    ArchivedCommandRecord, COMMAND_ADMISSION_VERSION, ClosedBoundaryDisposition,
    ClosedExecutionBoundary, CommandAdmission, CommandArchiveMerkleSibling, EffectBoundary,
    ExecutionFrameLocation, MACHINE_COMMAND_BATCH_RECEIPT_VERSION, MACHINE_COMMAND_BATCH_VERSION,
    MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES, MAX_MACHINE_COMMAND_BATCH_ARTIFACTS,
    MAX_MACHINE_COMMAND_BATCH_MEMBERS, MAX_MACHINE_COMMAND_BATCH_PLANS, Machine,
    MachineArchivedCommandProof, MachineBaseAnchor, MachineBaseSnapshot,
    MachineCommandArchiveEntry, MachineCommandArchiveLookup, MachineCommandArchiveObject,
    MachineCommandArchiveSegment, MachineCommandArchiveSegmentHeader,
    MachineCommandBatchMaterialSource, MachineCommandBatchMember, MachineCommandBatchRecord,
    MachineCommandIndexNode, MachineCommandIndexProof, MachineCommandIndexValue, MachineCompaction,
    MachineDelta, MachineRootDelta, MachineRootParts, MachineSnapshot, MerkleSiblingSide,
    MigrationFrameReplacementReceipt, ResumableExecutionFrame, resolve_machine_command_index_proof,
    validate_identity,
};

/// Framework-internal substrate for the durable exact-load reducer.
///
/// These pure types and functions convey no persistence authority. A durable
/// implementation must assemble them only from one resolver-owned pinned
/// manifest and may commit only Store roots it computed from the returned typed
/// mutations in the same CAS. SDK, wire, and runtime control facades must not
/// accept these values from callers.
#[doc(hidden)]
pub mod durable_internal {
    pub use crate::machine::pinned::{
        MAX_MACHINE_MATERIAL_ARTIFACTS, MAX_MACHINE_MATERIAL_PLANS,
        MAX_PINNED_COMMAND_BATCH_COMMANDS, MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
        MAX_PINNED_MACHINE_INDEX_PAGES, MAX_PINNED_MACHINE_READ_LEAF_BYTES,
        MAX_PINNED_MACHINE_READ_SET_BYTES, MAX_PINNED_MACHINE_READ_SET_ENTRIES,
        MachineAuthorityFrontier, MachineCompactionIntent, MachineInlineScopeReadRequirement,
        MachineLogRoot, MachineMapRoot, MachineMaterialAdmission, MachineMaterialParentReads,
        MachinePagedBatchManifest, MachinePagedFinalizeInputs, MachinePagedMaterialRoots,
        MachinePagedReadInputs, MachinePagedShadowRoots, MachinePagedTransitionAction,
        MachinePagedTransitionCurrent, MachinePagedTransitionPhase, MachinePhysicalRoot,
        MachinePinnedBatchCommand, MachinePinnedBatchPrecondition, MachinePinnedCommandProof,
        MachinePinnedRunLookup, MachinePreparedRootMutation, MachineRunChildRoots,
        MachineRunCurrent, MachineRunDelta, MachineRunIndexMembershipDelta, MachineRunIndexPage,
        MachineRunIndexRoots, MachineRunIndexSelector, MachineRunLogAppendDelta, MachineRunLogPage,
        MachineRunLogSelector, MachineRunOrderRoots, MachineRunReadInputs, MachineRunReadSet,
        MachineRunReducerState, MachineRunRootUpdate, MachineRunRootUpdateTarget,
        MachineScopeCurrent, MachineScopeLocationWitness, MachineStartRunMaterial,
        MachineTypedRootMutation, PinnedMachineBatchTransition, PinnedMachineCommandPreparation,
        PinnedMachineCommandReplay, PinnedMachineFreshPreparation, PinnedMachinePagedBegin,
        PinnedMachinePagedProgress, PinnedMachineRootDelta, PinnedMachineRunPreparation,
        PinnedMachineTransition, PreparedMachineMaterialAdmission, PreparedPinnedCommandBatch,
        PreparedPinnedGlobalTransition, PreparedPinnedMachineCompaction,
        PreparedPinnedMachineTransition, PreparedPinnedPagedBegin, PreparedPinnedPagedFinalize,
        PreparedPinnedPagedMaterial, PreparedPinnedPagedProgress, PreparedPinnedPagedPublish,
        PreparedPinnedPagedStep, PreparedPinnedReadCommand, PreparedPinnedRunLookup,
        PreparedPinnedRunTransition, machine_index_membership_value_id,
        machine_order_entry_value_id, pinned_paged_log_selector, pinned_paged_obligation_read,
        prepare_machine_material_admission, prepare_pinned_command, prepare_pinned_command_batch,
        prepare_pinned_compaction, prepare_pinned_transition_final, prepare_pinned_transition_page,
        validate_pinned_execution_frame, verify_pinned_command_batch_replay,
    };
    pub use crate::{
        MachineCommandBatchMaterialSource, MachineCommandBatchMember, MachineCommandBatchRecord,
    };
}
pub use model::{
    ARTIFACT_IDENTITY_VERSION, ArtifactRecord, ArtifactRef, AttemptProjection, COMMAND_VERSION,
    COMPONENT_OUTPUT_ARTIFACT_KIND, Command, CommandEnvelope, CommandReceipt, CommandReceiptStatus,
    CompactionCertificate, DECLARED_FAILURE_ARTIFACT_KIND, EFFECT_ARGS_ARTIFACT_KIND,
    EFFECT_SCHEMA_VERSION, EVENT_VERSION, EXECUTION_BINDING_ARTIFACT_KIND,
    EffectExecutionAvailability, EffectIntentIdentityInput, EffectPhase, EffectProjection,
    EffectTransition, Event, EventContent, EventPayload, InitialAttemptSpec, InvocationPathSegment,
    MAX_ARTIFACT_BASE64_BYTES, MAX_ARTIFACT_BYTES, MAX_ARTIFACT_KIND_BYTES,
    MAX_ARTIFACT_RECORD_CANONICAL_BYTES, MAX_EXACT_INTEGER, ObligationProjection, Projection,
    ROOT_SCOPE_ID, RUN_INPUT_ARTIFACT_KIND, ReconciliationResolution, ReconciliationState,
    ReplayAvailability, RunExecutionStatus, RunFailure, RunFailureClass, RunProjection,
    SEMANTIC_VERSION, ScopeProjection, ScopeStatus, WorldOutcome, WorldSettlementStatus,
    artifact_ref, effect_intent_id, effect_obligation_id, plan_invocation_id, plan_scope_id,
    validate_artifact_kind, validate_failure_code,
};
