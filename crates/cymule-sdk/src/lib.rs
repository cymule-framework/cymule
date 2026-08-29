//! Rust authoring and engine client facade.

mod builder;
mod client;

pub use builder::FlowBuilder;
pub use client::{
    CliEngine, DurableEngine, DurablePageQueryOptions, DurableRunCurrentRead, DurableRunItemQuery,
    DurableRunItemRead, Engine,
};
pub use cymule_core::{
    ArtifactRecord, ArtifactRef, COMPONENT_OUTPUT_ARTIFACT_KIND, ComponentContract, Definition,
    DispatchPolicy, EffectContract, EffectExecutionAvailability, EffectProfile, Expression,
    InvocationPathSegment, MutationKind, Operation, PlanCandidate, ReconciliationMode,
    ReconciliationResolution, ReconciliationState, Region, ReplayAvailability, RunExecutionStatus,
    RunFailure, RunFailureClass, SealedPlan, Step, WaitSpec, WorldSettlementStatus,
};
pub use cymule_durable::{
    CancellationCommand, CancellationReceipt, ComponentOccurrence, ComponentOccurrenceState,
    ComponentOutcome, DURABLE_CONTROL_VERSION, DURABLE_STATE_VERSION, DurableAttemptSummary,
    DurableBoundary, DurableCommand, DurableEffectSummary, DurableOccurrenceSummary,
    DurablePageCursor, DurablePagePosition, DurablePageQueryKind, DurableQueryPage,
    DurableResponse, DurableRunAttemptPage, DurableRunCurrent, DurableRunEffectPage,
    DurableRunIndexPage, DurableRunIndexSummary, DurableRunItem, DurableRunItemSelector,
    DurableRunOccurrencePage, DurableRunWaitPage, DurableWaitSummary,
    EFFECT_RESOLUTION_RECEIPT_VERSION, EffectDispatch, EffectResolutionCommand,
    EffectResolutionReceipt, JournalRecord, MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
    MAX_DURABLE_QUERY_PAGE_BYTES, MAX_DURABLE_QUERY_PAGE_ITEMS, MAX_DURABLE_QUERY_RUN_KEY_SCALARS,
    MAX_DURABLE_QUERY_SUMMARY_BYTES, OperationAttempt, OperationAttemptState, OutboxState,
    RUN_CANCELLATION_RECEIPT_VERSION, WaitCondition, WaitKind, WaitState,
};
pub use cymule_durable_protocol::{
    CLOCK_OBSERVATION_VERSION, CONTINUATION_STATE_VERSION, ClockObservation, ClockObservationRef,
    Continuation, ContinuationExecutionClaim, ContinuationStatus, ExecutionClaimRequest,
    FrameState, WAIT_ACTIVATION_RECEIPT_VERSION, WAIT_ACTIVATION_VERSION, WaitActivation,
    WaitActivationDisposition, WaitActivationReceipt, WaitActivationSource, WaitOwner,
};
pub use cymule_evolution::{
    EVOLUTION_CONTROL_VERSION, EvolutionCommand, EvolutionCommit, EvolutionCurrent,
    EvolutionMutationWrite, EvolutionPersistenceCommand, EvolutionPersistenceReceipt,
    EvolutionStateFamily, GateOutcome, LIVE_EVOLUTION_CONTROL_VERSION, LinkedPlan,
    LiveEvolutionCommand, LiveEvolutionOutcome, LivePublicationCommand, LivePublicationReceipt,
    LiveTemplateUpdate, MigrationAdapterDescriptor, MigrationCapabilityChange, MigrationOutput,
    MigrationPreservation, MigrationReceipt, MigrationRequest, MigrationStateCoverage,
    ObservationOutcome, OccurrencePin, PatchOperation, PlanEdge, PlanPatch, PlanTemplate,
    ReferenceStrategy, RestartReceipt, RestartRequest, RolloutDecision, RolloutEvaluation,
    RolloutGate, RolloutMode, RolloutObservation, RolloutTransition, ShadowBindingMode,
    ShadowComparison, ShadowDriverDescriptor, ShadowEffectMode, ShadowOutput, ShadowRequest,
    SubflowReference, SubflowRevision,
};
pub use cymule_resource::{
    ARTIFACT_TYPE_CONTRACT_KIND, ARTIFACT_TYPE_CONTRACT_VERSION, ArtifactTypeCandidate,
    ArtifactTypeContract, ArtifactTypeRegistry, InlineData, ResourceCandidate, ResourceHandle,
    ResourceHandoff, ResourceHandoffActivation, ResourceIntegrity, ResourceListProof,
    ResourceLocation, ResourceLocatorSet, ResourceManifestDescriptor, ResourceManifestEntry,
    ResourcePin, ResourcePinKind, ResourcePinReceipt, ResourceProducerProvenance,
    ResourcePublication, ResourceReleaseReceipt, ResourceReplayClass, ResourceRetentionFamily,
    ResourceRetentionSubject, ResourceSchemaIssue, ResourceShape,
};
pub use cymule_runtime::{
    ENGINE_CLOCK_SYSTEM_PROVIDER, ENGINE_DIRECTORY_STORE_PROVIDER,
    ENGINE_PROCESS_EXECUTOR_PROVIDER, ENGINE_PROTOCOL_VERSION, ENGINE_SQLITE_STORE_PROVIDER,
    EVOLUTION_PLUGIN_PROTOCOL_VERSION, EffectReconciliationBoundary, EffectReleaseBoundary,
    EngineClockTarget, EngineContractSide, EngineDurableTarget, EngineEvolutionTarget,
    EngineFailure, EngineFailureCategory, EngineIssue, EngineMigrationProviderTarget, EnginePhase,
    EnginePluginTarget, EngineProcessConfig, EngineResult, EngineRetryDisposition,
    EngineShadowProviderTarget, EngineStoreTarget, ExecutionOutcome, ExecutionResult,
    SuspensionBoundary,
};
pub use cymule_virtual::{
    ArchivedCommandIndex, ArchivedWorkIndex, ClaimedWork, FrontierLimits, MaterializedPage,
    ParkReason, ParkedWork, RegionMigrationCommand, RegionMigrationKind, RegionMigrationPlan,
    RegionMigrationReceipt, RegionMigrationRequest, RegionSourceBinding, RegionSourceCheckpoint,
    SchedulingPolicy, VIRTUAL_ARCHIVE_MANIFEST_KIND, VIRTUAL_ARCHIVE_MANIFEST_VERSION,
    VIRTUAL_CLAIM_CONTROL_VERSION, VIRTUAL_COMPACTION_CERTIFICATE_VERSION,
    VIRTUAL_COMPACTION_CONTROL_VERSION, VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION,
    VIRTUAL_RECOVERY_CONTROL_VERSION, VIRTUAL_REGION_MIGRATION_CONTROL_VERSION,
    VIRTUAL_REGION_MIGRATION_VERSION, VIRTUAL_REHYDRATION_CONTROL_VERSION,
    VIRTUAL_RUN_WEIGHT_CONTROL_VERSION, VIRTUAL_WORK_CONTROL_VERSION,
    VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualActivationCommand, VirtualActiveRegionCurrent,
    VirtualArchiveBinding, VirtualArchiveCommandIndexNode, VirtualArchiveCommandIndexProof,
    VirtualArchiveCommandIndexUpdate, VirtualArchiveCommandProof, VirtualArchiveManifest,
    VirtualArchiveMerkleSide, VirtualArchiveMerkleStep, VirtualArchiveOccurrenceProof,
    VirtualArchiveRetirementCommand, VirtualArchiveRetirementPersistenceCommand,
    VirtualArchiveRetirementReceipt, VirtualArchiveWorkIndexNode, VirtualArchiveWorkIndexUpdate,
    VirtualArchiveWorkProof, VirtualArchivedCommand, VirtualCertificateCurrent,
    VirtualCertificateLifecycle, VirtualClaimCommand, VirtualClaimLease, VirtualClaimOutcome,
    VirtualClaimPersistenceCommand, VirtualClaimReceipt, VirtualCompactionCertificate,
    VirtualCompactionCommand, VirtualCompactionPersistenceCommand, VirtualCompactionPublication,
    VirtualCompactionReceipt, VirtualCompletionSummary, VirtualCursor,
    VirtualEvolutionSelectionLink, VirtualInitializationCommand, VirtualLeaseRenewalCommand,
    VirtualLeaseRenewalPersistenceCommand, VirtualLeaseRenewalReceipt,
    VirtualMaterializationCommand, VirtualMigrationCurrent, VirtualMigrationPersistenceCommand,
    VirtualMutationSet, VirtualOccurrenceCurrent, VirtualParkedCurrent, VirtualParkedIndexPage,
    VirtualPersistenceCommand, VirtualPersistenceEvidence, VirtualPersistenceOperation,
    VirtualPersistenceOutcome, VirtualPersistenceReceipt, VirtualRecoveryCommand,
    VirtualRecoveryPersistenceCommand, VirtualRecoveryReceipt, VirtualRegion, VirtualRegionCurrent,
    VirtualRegionLifecycle, VirtualRehydratedOccurrence, VirtualRehydrationCommand,
    VirtualRehydrationPersistenceCommand, VirtualRehydrationReceipt,
    VirtualResolutionPersistenceCommand, VirtualRunCurrent, VirtualRunDefinition,
    VirtualRunExecution, VirtualRunWeightCommand, VirtualRunWeightPersistenceCommand,
    VirtualRunWeightReceipt, VirtualStateMutation, VirtualWorkCurrent, VirtualWorkPlacement,
    WorkItem, WorkOccurrence, WorkOccurrenceState, WorkResolution, WorkResolutionCommand,
    WorkResolutionReceipt, build_virtual_work_index_update, resolve_virtual_work_index_proof,
    virtual_work_index_empty_root,
};
