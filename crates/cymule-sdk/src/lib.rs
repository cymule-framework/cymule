//! Rust authoring and engine client facade.

mod builder;
mod client;
mod control;

pub use builder::FlowBuilder;
pub use client::{CliEngine, Engine};
pub use control::{
    DurableControl, EvolutionControl, LiveEvolutionControl, VirtualSchedulingControl,
    VirtualWorkControl,
};
pub use cymule_core::{
    ArtifactRef, DispatchPolicy, EffectProfile, Expression, MutationKind, Operation, PlanCandidate,
    ReconciliationMode, Region, ScopeMode, SealedPlan, Step, WaitSpec,
};
pub use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableDomainView, DurableResponse,
    DurableRunView, WAIT_ACTIVATION_VERSION, WaitActivation, WaitActivationSource,
};
pub use cymule_evolution::{
    EVOLUTION_CONTROL_VERSION, EvolutionCommand, GateOutcome, LIVE_EVOLUTION_CONTROL_VERSION,
    LiveEvolutionCommand, LiveMigrationCommand, LivePublicationCommand, LivePublicationReceipt,
    LiveTemplateUpdate, MIGRATION_SAFE_POINT_VERSION, MigrationAdapterDescriptor,
    MigrationCapabilityChange, MigrationOutput, MigrationPreservation, MigrationReceipt,
    MigrationRequest, MigrationSafePoint, MigrationStateCoverage, ObservationOutcome, PlanPatch,
    PlanTemplate, ReferenceStrategy, RestartReceipt, RestartRequest, RolloutDecision,
    RolloutEvaluation, RolloutGate, RolloutMode, RolloutObservation, RolloutTransition,
    ShadowBindingMode, ShadowComparison, ShadowDriverDescriptor, ShadowEffectMode, ShadowOutput,
    ShadowRequest, SubflowReference,
};
pub use cymule_resource::{
    ARTIFACT_TYPE_CONTRACT_KIND, ARTIFACT_TYPE_CONTRACT_VERSION, ArtifactTypeCandidate,
    ArtifactTypeContract, ArtifactTypeRegistry, InlineData, ResourceCandidate, ResourceHandle,
    ResourceHandoff, ResourceIntegrity, ResourceLocation, ResourceReplayClass, ResourceSchemaIssue,
    ResourceShape,
};
pub use cymule_runtime::{
    ENGINE_PROTOCOL_VERSION, EffectReconciliationBoundary, EffectReleaseBoundary,
    EngineContractSide, EngineFailure, EngineFailureCategory, EngineIssue, EnginePhase,
    EngineResult, EngineRetryDisposition, ExecutionOutcome, ExecutionResult, SuspensionBoundary,
};
pub use cymule_virtual::{
    ArchivedWorkIndex, CompactedWorkIndex, ParkReason, RegionMigrationCommand, RegionMigrationKind,
    RegionMigrationPlan, RegionMigrationReceipt, RegionMigrationRequest, RegionMigrator,
    VIRTUAL_ARCHIVE_MANIFEST_KIND, VIRTUAL_ARCHIVE_MANIFEST_VERSION, VIRTUAL_CLAIM_CONTROL_VERSION,
    VIRTUAL_COMPACTION_CERTIFICATE_VERSION, VIRTUAL_COMPACTION_CONTROL_VERSION,
    VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION, VIRTUAL_RECOVERY_CONTROL_VERSION,
    VIRTUAL_REGION_MIGRATION_CONTROL_VERSION, VIRTUAL_REGION_MIGRATION_VERSION,
    VIRTUAL_REHYDRATION_CONTROL_VERSION, VIRTUAL_RUN_WEIGHT_CONTROL_VERSION,
    VIRTUAL_WORK_CONTROL_VERSION, VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualArchive,
    VirtualArchiveManifest, VirtualClaimCommand, VirtualClaimLease, VirtualClaimReceipt,
    VirtualCompactionCertificate, VirtualCompactionCommand, VirtualCompactionReceipt,
    VirtualCompletionSummary, VirtualLeaseRenewalCommand, VirtualLeaseRenewalReceipt,
    VirtualRecoveryCommand, VirtualRecoveryReceipt, VirtualRehydrationCommand,
    VirtualRehydrationReceipt, VirtualRunWeightCommand, VirtualRunWeightReceipt, WorkOccurrence,
    WorkOccurrenceState, WorkResolution, WorkResolutionCommand,
};
