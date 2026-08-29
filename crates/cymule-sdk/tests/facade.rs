//! Compile-time coverage for the public `cymule` facade.

use cymule::{
    ArchivedCommandIndex, ArtifactRecord, CancellationCommand, CancellationReceipt, ClaimedWork,
    ClockObservation, ComponentContract, Continuation, ContinuationStatus, Definition,
    EffectContract, EffectDispatch, EffectResolutionCommand, EffectResolutionReceipt,
    EnginePluginTarget, EngineProcessConfig, FrameState, JournalRecord, LinkedPlan, OutboxState,
    ParkedWork, PatchOperation, PlanEdge, ReplayAvailability, ResourceHandoff,
    ResourceHandoffActivation, SchedulingPolicy, SubflowRevision, VirtualArchiveCommandIndexNode,
    VirtualArchiveCommandIndexProof, VirtualArchiveCommandIndexUpdate, VirtualArchiveCommandProof,
    VirtualArchiveManifest, VirtualArchiveOccurrenceProof, VirtualArchiveWorkIndexNode,
    VirtualArchiveWorkIndexUpdate, VirtualArchiveWorkProof, VirtualArchivedCommand, VirtualCursor,
    VirtualRegion, WaitActivationReceipt, WaitCondition, WaitKind, WaitOwner, WaitState, WorkItem,
    WorkResolutionReceipt, virtual_work_index_empty_root,
};
use serde::{Serialize, de::DeserializeOwned};

fn assert_wire<T: Serialize + DeserializeOwned>() {}

#[test]
fn facade_exports_complete_authoring_and_control_wire_types() {
    assert_wire::<ComponentContract>();
    assert_wire::<ArtifactRecord>();
    assert_wire::<Definition>();
    assert_wire::<EffectContract>();
    assert_wire::<ResourceHandoff>();
    assert_wire::<ResourceHandoffActivation>();

    assert_wire::<CancellationCommand>();
    assert_wire::<CancellationReceipt>();
    assert_wire::<EffectResolutionCommand>();
    assert_wire::<EffectResolutionReceipt>();
    assert_wire::<WaitActivationReceipt>();
    assert_wire::<ClockObservation>();
    assert_wire::<Continuation>();
    assert_wire::<ContinuationStatus>();
    assert_wire::<FrameState>();
    assert_wire::<WaitCondition>();
    assert_wire::<WaitKind>();
    assert_wire::<WaitOwner>();
    assert_wire::<WaitState>();
    assert_wire::<EffectDispatch>();
    assert_wire::<OutboxState>();
    assert_wire::<JournalRecord>();

    assert_wire::<PatchOperation>();
    assert_wire::<PlanEdge>();
    assert_wire::<LinkedPlan>();
    assert_wire::<SubflowRevision>();

    assert_wire::<EngineProcessConfig>();
    assert_wire::<EnginePluginTarget>();
}

#[test]
fn facade_exports_virtual_archive_authority() {
    assert_wire::<ArchivedCommandIndex>();
    assert_wire::<VirtualArchivedCommand>();
    assert_wire::<VirtualArchiveCommandIndexProof>();
    assert_wire::<VirtualArchiveCommandIndexNode>();
    assert_wire::<VirtualArchiveCommandIndexUpdate>();
    assert_wire::<VirtualArchiveCommandProof>();
    assert_wire::<VirtualArchiveManifest>();
    assert_wire::<VirtualArchiveOccurrenceProof>();
    assert_wire::<VirtualArchiveWorkProof>();
    assert_wire::<VirtualArchiveWorkIndexNode>();
    assert_wire::<VirtualArchiveWorkIndexUpdate>();
    assert_wire::<ReplayAvailability>();
    assert_wire::<VirtualCursor>();
    assert_wire::<VirtualRegion>();
    assert_wire::<WorkItem>();
    assert_wire::<ClaimedWork>();
    assert_wire::<ParkedWork>();
    assert_wire::<SchedulingPolicy>();
    assert_wire::<WorkResolutionReceipt>();

    let _: fn() -> String = virtual_work_index_empty_root;
}

#[test]
fn facade_closes_public_virtual_receipt_field_types() {
    fn receipt(outcome: &cymule::VirtualClaimOutcome) -> &cymule::VirtualPersistenceReceipt {
        match outcome {
            cymule::VirtualClaimOutcome::NoWork { receipt } => receipt,
            cymule::VirtualClaimOutcome::Claimed {
                receipt,
                claim,
                plan,
            } => {
                let _: &cymule::ClaimedWork = claim;
                let _: &cymule::SealedPlan = plan;
                receipt
            }
        }
    }

    assert_wire::<cymule::VirtualClaimOutcome>();
    assert_wire::<cymule::FrontierLimits>();
    assert_wire::<cymule::MaterializedPage>();
    assert_wire::<cymule::VirtualActivationCommand>();
    assert_wire::<cymule::VirtualActiveRegionCurrent>();
    assert_wire::<cymule::VirtualArchiveBinding>();
    assert_wire::<cymule::VirtualArchiveMerkleSide>();
    assert_wire::<cymule::VirtualArchiveMerkleStep>();
    assert_wire::<cymule::VirtualArchiveRetirementCommand>();
    assert_wire::<cymule::VirtualArchiveRetirementPersistenceCommand>();
    assert_wire::<cymule::VirtualArchiveRetirementReceipt>();
    assert_wire::<cymule::VirtualCertificateCurrent>();
    assert_wire::<cymule::VirtualCertificateLifecycle>();
    assert_wire::<cymule::VirtualClaimPersistenceCommand>();
    assert_wire::<cymule::VirtualCompactionPersistenceCommand>();
    assert_wire::<cymule::VirtualCompactionPublication>();
    assert_wire::<cymule::VirtualEvolutionSelectionLink>();
    assert_wire::<cymule::VirtualInitializationCommand>();
    assert_wire::<cymule::VirtualLeaseRenewalPersistenceCommand>();
    assert_wire::<cymule::VirtualMaterializationCommand>();
    assert_wire::<cymule::VirtualMigrationCurrent>();
    assert_wire::<cymule::VirtualMigrationPersistenceCommand>();
    assert_wire::<cymule::VirtualMutationSet>();
    assert_wire::<cymule::VirtualOccurrenceCurrent>();
    assert_wire::<cymule::VirtualParkedCurrent>();
    assert_wire::<cymule::VirtualParkedIndexPage>();
    assert_wire::<cymule::VirtualPersistenceCommand>();
    assert_wire::<cymule::VirtualPersistenceEvidence>();
    assert_wire::<cymule::VirtualPersistenceOperation>();
    assert_wire::<cymule::VirtualPersistenceOutcome>();
    assert_wire::<cymule::VirtualPersistenceReceipt>();
    assert_wire::<cymule::VirtualRecoveryPersistenceCommand>();
    assert_wire::<cymule::VirtualRegionCurrent>();
    assert_wire::<cymule::VirtualRegionLifecycle>();
    assert_wire::<cymule::VirtualRehydratedOccurrence>();
    assert_wire::<cymule::VirtualRehydrationPersistenceCommand>();
    assert_wire::<cymule::VirtualResolutionPersistenceCommand>();
    assert_wire::<cymule::VirtualRunCurrent>();
    assert_wire::<cymule::VirtualRunDefinition>();
    assert_wire::<cymule::VirtualRunExecution>();
    assert_wire::<cymule::VirtualRunWeightPersistenceCommand>();
    assert_wire::<cymule::VirtualStateMutation>();
    assert_wire::<cymule::VirtualWorkCurrent>();
    assert_wire::<cymule::VirtualWorkPlacement>();
    assert_wire::<cymule::EvolutionCurrent>();
    assert_wire::<cymule::EvolutionPersistenceCommand>();
    assert_wire::<cymule::EvolutionStateFamily>();
    assert_wire::<cymule::ResourcePin>();
    assert_wire::<cymule::ResourcePinKind>();
    assert_wire::<cymule::ResourcePinReceipt>();
    assert_wire::<cymule::ResourceReleaseReceipt>();
    assert_wire::<cymule::ResourceRetentionFamily>();
    assert_wire::<cymule::ResourceRetentionSubject>();

    let _: fn(&cymule::VirtualClaimOutcome) -> &cymule::VirtualPersistenceReceipt = receipt;
}
