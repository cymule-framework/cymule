use std::collections::{BTreeMap, BTreeSet};

use cymule_authenticated_collections::MapRoot;
use cymule_core::{
    ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, CommandReceiptStatus, EffectTransition,
    Machine, Operation, ReconciliationResolution, RunExecutionStatus, SealedPlan, WorldOutcome,
    canonical_digest, content_id,
};
use cymule_profile_protocol::{
    agent as agent_protocol, evolution as evolution_protocol, resource as resource_protocol,
    virtual_work as virtual_protocol,
};
use cymule_runtime::{
    ExecutionBinding, ExecutionOperationKind, PlanContracts, RESULT_ARTIFACT_KIND,
};

use crate::model::{
    COMPONENT_INPUT_ARTIFACT_KIND, DURABLE_RUNTIME_ACTOR, DerivedCommandOperation,
    EFFECT_RESULT_ARTIFACT_KIND, INVOCATION_INPUT_ARTIFACT_KIND, INVOCATION_RESULT_ARTIFACT_KIND,
    SCOPE_RESULT_ARTIFACT_KIND, TRANSPORT_REQUEST_ID_DOMAIN, component_occurrence_id,
    continuation_id, derive_wait_id, derived_command_id, synchronize_pinned_effect_projection,
    validate_wire_non_empty,
};
use crate::{
    CancellationCommand, CancellationReceipt, ClockObservation, ComponentOccurrence,
    ComponentOutcome, Continuation, ContinuationExecutionClaim, ContinuationStatus,
    CoordinationLease, DurableDelta, DurableError, DurableOperation, DurableResult, DurableState,
    DurableStore, EffectDispatch, EffectResolutionCommand, EffectResolutionReceipt,
    ExecutionClaimRequest, GcReceipt, MAX_EXACT_INTEGER, OperationAttempt, OutboxState, StoreBatch,
    StoreReclamation, WAIT_ACTIVATION_RECEIPT_VERSION, WaitActivation, WaitActivationReceipt,
    WaitActivationSource, WaitCondition, WaitKind, WaitOwner, WaitState, execution_clock_scope,
};

mod agent_workspace;
mod resource_handoff;

/// Canonical Artifact kind for one semantic Run-cancellation reason.
pub const CANCELLATION_REASON_ARTIFACT_KIND: &str = "cymule.cancellation-reason/1";
const CONTINUATION_ATTEMPT_ID_DOMAIN: &str = "cymule.continuation-attempt/1";
const WAIT_ACTIVATION_MATERIAL_DOMAIN: &str = "cymule.wait-activation-material/1";

/// Transactional coordinator over one provider-neutral durable store.
pub(crate) struct DurableCoordinator<S> {
    store: S,
    pinned: Option<PinnedHead>,
}

/// Fixed-size semantic authority loaded by every ordinary coordinator open.
///
/// A `PinnedHead` is constructed only from a Store-returned head and the exact
/// manifest named by that head. Complete semantic materialization is reserved
/// for the Store's explicitly named offline audit operation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PinnedHead {
    head: crate::StoreHead,
    manifest: crate::StateRootManifest,
}

struct PinnedCommandCommit {
    receipt: cymule_core::CommandReceipt,
    committed_revision: Option<String>,
}

struct PinnedBatchCommit {
    batch_id: String,
    batch_receipt_id: String,
    receipts: Vec<cymule_core::CommandReceipt>,
    committed_revision: Option<String>,
}

struct PreparedStartRun {
    envelope: CommandEnvelope,
    material: cymule_core::durable_internal::MachineStartRunMaterial,
    attempt_id: String,
}

enum PinnedEvolutionPreparation {
    Selection {
        view: Box<evolution_protocol::EvolutionAuthorityView>,
        binding: Box<cymule_core::ArtifactRecord>,
    },
    General {
        view: Box<evolution_protocol::EvolutionAuthorityView>,
        source: Box<evolution_protocol::EvolutionReductionSource>,
        migration_target: Option<Box<SealedPlan>>,
    },
    FreshMigration {
        view: Box<evolution_protocol::EvolutionAuthorityView>,
        target_plan: Box<SealedPlan>,
        safe_point: Box<evolution_protocol::MigrationSafePoint>,
        continuation: Box<Continuation>,
        source_binding: Box<ArtifactRef>,
    },
}

pub(crate) enum PinnedStartRunOutcome {
    Committed(Box<ContinuationExecutionClaim>),
    Replayed,
}

#[derive(Clone, PartialEq)]
pub(crate) struct ExecutorRunRead {
    pub(crate) revision: String,
    pub(crate) projection_root: String,
    pub(crate) run: cymule_core::durable_internal::MachineRunCurrent,
    pub(crate) plan: SealedPlan,
    pub(crate) binding: cymule_core::ArtifactRecord,
    pub(crate) continuation: Continuation,
    pub(crate) root_input: cymule_core::ArtifactRecord,
    pub(crate) terminal_result: Option<cymule_core::ArtifactRecord>,
}

pub(crate) struct ExecutorStepRead {
    pub(crate) run: ExecutorRunRead,
    pub(crate) current_scope: cymule_core::durable_internal::MachineScopeCurrent,
    pub(crate) referenced_artifacts: BTreeMap<String, cymule_core::ArtifactRecord>,
}

pub(crate) struct ExecutorReadyBoundaryRead {
    pub(crate) revision: String,
    pub(crate) unknown_intent: Option<String>,
    pub(crate) explicit_intents: BTreeSet<String>,
}

#[derive(Clone, PartialEq)]
pub(crate) struct ExecutorEffectRead {
    pub(crate) revision: String,
    pub(crate) run: ExecutorRunRead,
    pub(crate) origin_plan: SealedPlan,
    pub(crate) origin_binding: cymule_core::ArtifactRecord,
    pub(crate) effect: cymule_core::EffectProjection,
    pub(crate) scope: cymule_core::durable_internal::MachineScopeCurrent,
    pub(crate) dispatch: EffectDispatch,
    pub(crate) input: cymule_core::ArtifactRecord,
    pub(crate) result: Option<cymule_core::ArtifactRecord>,
}

pub(crate) struct ExecutorDispatchRead {
    pub(crate) revision: String,
    pub(crate) next: Option<ExecutorEffectRead>,
    pub(crate) explicit_intents: BTreeSet<String>,
}

pub(crate) struct ExecutorEffectClaimRead {
    pub(crate) read: ExecutorEffectRead,
    pub(crate) owner: String,
    pub(crate) epoch: u64,
    pub(crate) provider_attempt: cymule_runtime::EffectProviderAttempt,
}

struct TerminalSidecarsRead {
    waits: Vec<WaitCondition>,
    attempt: Option<OperationAttempt>,
}

struct PreparedComponentResult {
    run: ExecutorRunRead,
    continuation: Continuation,
    occurrence: ComponentOccurrence,
    attempt: OperationAttempt,
    material: cymule_core::durable_internal::MachineMaterialAdmission,
}

struct VirtualCompactionSource {
    current: virtual_protocol::VirtualCurrent,
    region: virtual_protocol::VirtualRegion,
    occurrences: BTreeMap<String, virtual_protocol::WorkOccurrence>,
    work_index: BTreeMap<String, virtual_protocol::ArchivedWorkIndex>,
    command_receipts: BTreeMap<String, virtual_protocol::VirtualPersistenceReceipt>,
    journal_id: Option<String>,
}

struct VirtualCommandPreparation {
    current: Option<virtual_protocol::VirtualCurrent>,
    reads: Vec<virtual_protocol::VirtualStateRead>,
    operations: Vec<DurableOperation>,
    plans: Vec<SealedPlan>,
    artifacts: Vec<cymule_core::ArtifactRecord>,
    claim_plan: Option<SealedPlan>,
}

struct VirtualCommandCommit {
    commit: virtual_protocol::VirtualCommit,
    claim_plan: Option<SealedPlan>,
}

impl VirtualCommandPreparation {
    fn source(&self, scheduler_id: &str) -> DurableResult<virtual_protocol::VirtualKeyedSource> {
        virtual_protocol::VirtualKeyedSource::from_reads(
            scheduler_id,
            self.current.clone(),
            self.reads.clone(),
        )
        .map_err(Into::into)
    }

    fn current(&self) -> DurableResult<&virtual_protocol::VirtualCurrent> {
        self.current
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("Virtual scheduler does not exist".to_owned()))
    }

    fn region(&self, region_id: &str) -> DurableResult<&virtual_protocol::VirtualRegionCurrent> {
        self.reads
            .iter()
            .filter_map(virtual_protocol::VirtualStateRead::leaf)
            .find_map(|leaf| match leaf {
                virtual_protocol::VirtualStateLeaf::Regions(region)
                    if region.region.region_id == region_id =>
                {
                    Some(region)
                }
                _ => None,
            })
            .ok_or_else(|| {
                DurableError::NotFound(format!("Virtual region {region_id} does not exist"))
            })
    }

    fn work(&self, work_id: &str) -> DurableResult<&virtual_protocol::VirtualWorkCurrent> {
        self.reads
            .iter()
            .filter_map(virtual_protocol::VirtualStateRead::leaf)
            .find_map(|leaf| match leaf {
                virtual_protocol::VirtualStateLeaf::Work(work) if work.item.work_id == work_id => {
                    Some(work)
                }
                _ => None,
            })
            .ok_or_else(|| DurableError::NotFound(format!("Virtual work {work_id} does not exist")))
    }

    fn certificate(
        &self,
        certificate_id: &str,
    ) -> DurableResult<&virtual_protocol::VirtualCertificateCurrent> {
        self.reads
            .iter()
            .filter_map(virtual_protocol::VirtualStateRead::leaf)
            .find_map(|leaf| match leaf {
                virtual_protocol::VirtualStateLeaf::Certificates(certificate)
                    if certificate.certificate.certificate_id == certificate_id =>
                {
                    Some(certificate.as_ref())
                }
                _ => None,
            })
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "Virtual certificate {certificate_id} does not exist"
                ))
            })
    }
}

impl TerminalSidecarsRead {
    fn finish(self) -> DurableResult<Vec<DurableOperation>> {
        let mut operations = Vec::new();
        for mut wait in self.waits {
            wait.state = WaitState::Cancelled;
            wait.result = None;
            operations.push(DurableOperation::PutWait { value: wait });
        }
        if let Some(mut attempt) = self.attempt {
            attempt.state = crate::OperationAttemptState::Superseded;
            attempt.verify()?;
            operations.push(DurableOperation::PutOperationAttempt { value: attempt });
        }
        Ok(operations)
    }
}

enum DerivedExecutorBoundaryAction {
    Projection {
        artifacts: Vec<cymule_core::ArtifactRecord>,
    },
    OpenScope {
        command: Command,
    },
    CommitScope {
        command: Command,
        result: cymule_core::ArtifactRecord,
    },
    CommitRootScope {
        command: Command,
    },
    Yield {
        wait: Option<WaitCondition>,
    },
    Complete {
        result: cymule_core::ArtifactRecord,
    },
}

struct DerivedExecutorBoundary {
    next: Continuation,
    action: DerivedExecutorBoundaryAction,
}

struct StoreParkedWaitView<'a, S> {
    store: &'a mut S,
    manifest: crate::StateRootManifest,
}

impl<S: DurableStore> crate::ParkedWaitView for StoreParkedWaitView<'_, S> {
    fn select(
        &mut self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<crate::WaitSelection> {
        self.store
            .with_state_root_resolver(&self.manifest, |resolver| {
                let mut view = crate::state_root::pinned_wait::PinnedParkedWaitView::open(
                    &self.manifest,
                    resolver,
                )?;
                crate::ParkedWaitView::select(&mut view, source, max_targets)
            })
    }

    fn signal_key_page(
        &mut self,
        cursor: Option<&crate::WaitSourceCursor>,
        limit: usize,
    ) -> DurableResult<crate::SignalKeyPageOutcome> {
        self.store
            .with_state_root_resolver(&self.manifest, |resolver| {
                let mut view = crate::state_root::pinned_wait::PinnedParkedWaitView::open(
                    &self.manifest,
                    resolver,
                )?;
                crate::ParkedWaitView::signal_key_page(&mut view, cursor, limit)
            })
    }
}

impl PinnedHead {
    fn new(head: crate::StoreHead, manifest: crate::StateRootManifest) -> DurableResult<Self> {
        head.verify()?;
        manifest.verify()?;
        if head.state_root_manifest_id != manifest.manifest_id()
            || head.revision != manifest.revision()
            || head.sequence != manifest.sequence()
            || head.machine_base_anchor.as_ref() != manifest.machine_base_anchor()
        {
            return Err(DurableError::Integrity {
                code: "pinned_head_manifest_mismatch".to_owned(),
                message: "Store head does not exactly match its named StateRoot manifest"
                    .to_owned(),
            });
        }
        Ok(Self { head, manifest })
    }

    fn revision(&self) -> &str {
        &self.head.revision
    }
}

fn load_pinned_head<S: DurableStore>(store: &mut S) -> DurableResult<Option<PinnedHead>> {
    let Some(head) = store.load_head()? else {
        return Ok(None);
    };
    let manifest = store
        .load_state_root_manifest(&head.state_root_manifest_id)?
        .ok_or_else(|| DurableError::Integrity {
            code: "store_head_manifest_missing".to_owned(),
            message: format!(
                "Store head references missing StateRoot manifest {}",
                head.state_root_manifest_id
            ),
        })?;
    PinnedHead::new(head, manifest).map(Some)
}

impl<S: DurableStore> DurableCoordinator<S> {
    fn read_terminal_sidecars(
        &mut self,
        read: &ExecutorRunRead,
        completed_attempt_id: Option<&str>,
    ) -> DurableResult<TerminalSidecarsRead> {
        let step = self.read_execution_step(&read.run.run_id)?;
        if step.run.revision != read.revision || step.run.continuation != read.continuation {
            return Err(DurableError::Conflict {
                expected: Some(read.revision.clone()),
                current: Some(step.run.revision),
            });
        }
        let occurrence_id = if read.continuation.status == ContinuationStatus::Running {
            current_component_attempt_for_takeover(&step)?
        } else {
            None
        };
        self.read_current_state_root(|manifest, resolver| {
            let pending_waits = crate::state_root::load_run_query_index_root(
                manifest,
                resolver,
                &read.run.run_id,
                crate::state_root::RunQueryIndexKind::PendingWaits,
            )?;
            // The sequential executor parks one current Plan step. Running
            // terminalization has no Wait; Waiting owns exactly that one Wait.
            if read.continuation.wait_set.len() > 1
                || pending_waits.entries
                    != u64::try_from(read.continuation.wait_set.len())
                        .map_err(|error| DurableError::Validation(error.to_string()))?
            {
                return Err(DurableError::Integrity {
                    code: "terminal_pending_wait_frontier_mismatch".to_owned(),
                    message: "terminal source does not have the executor's exact parked Wait"
                        .to_owned(),
                });
            }
            let waits: Vec<WaitCondition> = load_bounded_run_index_values(
                manifest,
                resolver,
                &read.run.run_id,
                crate::state_root::RunQueryIndexKind::PendingWaits,
                crate::StateRootLeafKind::Wait,
            )?;
            let wait_ids = waits
                .iter()
                .map(|wait| wait.wait_id.clone())
                .collect::<BTreeSet<_>>();
            if wait_ids != read.continuation.wait_set
                || waits
                    .iter()
                    .any(|wait| wait.run_id != read.run.run_id || wait.state != WaitState::Pending)
            {
                return Err(DurableError::Integrity {
                    code: "terminal_pending_wait_set_mismatch".to_owned(),
                    message: "Run terminalization found inconsistent pending Wait authority"
                        .to_owned(),
                });
            }
            let attempt = occurrence_id
                .as_deref()
                .map(|occurrence_id| {
                    crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                        .component_attempt_frontier(occurrence_id)
                })
                .transpose()?
                .flatten()
                .map(|frontier| frontier.latest_attempt)
                .filter(|attempt| {
                    attempt.state == crate::OperationAttemptState::Running
                        && Some(attempt.attempt_id.as_str()) != completed_attempt_id
                });
            if let Some(attempt) = &attempt
                && !read
                    .continuation
                    .execution_claim
                    .as_ref()
                    .is_some_and(|claim| {
                        attempt.execution_claim_owner == claim.owner
                            && attempt.execution_claim_fence == claim.fence
                            && attempt.continuation_attempt_id == claim.continuation_attempt_id
                    })
            {
                return Err(DurableError::Integrity {
                    code: "terminal_component_claim_mismatch".to_owned(),
                    message: "Run terminalization found an unowned Running component Attempt"
                        .to_owned(),
                });
            }
            Ok(TerminalSidecarsRead { waits, attempt })
        })
    }

    pub(crate) fn commit_component_attempt_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
        expected_revision: &str,
        source: &Continuation,
        input: cymule_core::ArtifactRecord,
    ) -> DurableResult<crate::executor::ExecutorComponentAttemptAdmission> {
        let read = self.read_execution_step(&claim.run_id)?;
        if read.run.revision != expected_revision
            || read.run.continuation != *source
            || source.execution_claim.as_ref() != Some(claim)
            || source.status != ContinuationStatus::Running
        {
            return Err(DurableError::Conflict {
                expected: Some(expected_revision.to_owned()),
                current: Some(read.run.revision),
            });
        }
        let mut occurrence = derive_pinned_component_occurrence(&read, &input)?;
        let frontier = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                .component_attempt_frontier(&occurrence.occurrence_id)
        })?;
        let (ordinal, previous_attempt_id) = match frontier {
            None => (1, None),
            Some(frontier) => {
                occurrence.attempt_count = frontier.occurrence.attempt_count;
                occurrence
                    .latest_attempt_id
                    .clone_from(&frontier.occurrence.latest_attempt_id);
                if occurrence != frontier.occurrence
                    || frontier.occurrence.state != crate::ComponentOccurrenceState::Pending
                {
                    return Err(DurableError::HistoryConflict {
                        code: "component_occurrence_reused".to_owned(),
                        message: format!(
                            "component occurrence {} changed semantic authority",
                            occurrence.occurrence_id
                        ),
                    });
                }
                match frontier.latest_attempt.state {
                    crate::OperationAttemptState::Running
                        if frontier.latest_attempt.execution_claim_owner == claim.owner
                            && frontier.latest_attempt.execution_claim_fence == claim.fence
                            && frontier.latest_attempt.continuation_attempt_id
                                == claim.continuation_attempt_id =>
                    {
                        return Ok(
                            crate::executor::ExecutorComponentAttemptAdmission::InFlight(
                                frontier.latest_attempt,
                            ),
                        );
                    }
                    crate::OperationAttemptState::Running => {
                        return Err(DurableError::Busy {
                            run_id: claim.run_id.clone(),
                            owner: frontier.latest_attempt.execution_claim_owner,
                            fence: frontier.latest_attempt.execution_claim_fence,
                        });
                    }
                    crate::OperationAttemptState::Superseded => (
                        frontier
                            .occurrence
                            .attempt_count
                            .checked_add(1)
                            .filter(|value| *value <= MAX_EXACT_INTEGER)
                            .ok_or_else(|| {
                                DurableError::Validation(
                                    "component Attempt ordinal overflowed".to_owned(),
                                )
                            })?,
                        Some(frontier.latest_attempt.attempt_id),
                    ),
                    crate::OperationAttemptState::Completed => {
                        return Err(DurableError::Integrity {
                            code: "component_pending_latest_completed".to_owned(),
                            message: "pending component occurrence has a completed latest Attempt"
                                .to_owned(),
                        });
                    }
                }
            }
        };
        let attempt = self.commit_new_component_attempt_pinned(
            claim,
            occurrence,
            ordinal,
            previous_attempt_id,
            input,
        )?;
        Ok(crate::executor::ExecutorComponentAttemptAdmission::Admitted(attempt))
    }

    fn commit_new_component_attempt_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
        mut occurrence: ComponentOccurrence,
        ordinal: u64,
        previous_attempt_id: Option<String>,
        input: cymule_core::ArtifactRecord,
    ) -> DurableResult<OperationAttempt> {
        let attempt_id =
            crate::model::operation_attempt_id(&crate::model::OperationAttemptIdentity {
                occurrence_id: &occurrence.occurrence_id,
                attempt_ordinal: ordinal,
                previous_attempt_id: previous_attempt_id.as_deref(),
                run_id: &claim.run_id,
                continuation_attempt_id: &claim.continuation_attempt_id,
                execution_claim_owner: &claim.owner,
                execution_claim_fence: claim.fence,
                operation_occurrence_binding: &occurrence.occurrence_binding,
            })?;
        let attempt = OperationAttempt {
            attempt_version: crate::OPERATION_ATTEMPT_VERSION.to_owned(),
            attempt_id: attempt_id.clone(),
            occurrence_id: occurrence.occurrence_id.clone(),
            run_id: claim.run_id.clone(),
            attempt_ordinal: ordinal,
            previous_attempt_id,
            continuation_attempt_id: claim.continuation_attempt_id.clone(),
            execution_claim_owner: claim.owner.clone(),
            execution_claim_fence: claim.fence,
            operation_occurrence_binding: occurrence.occurrence_binding.clone(),
            transport_request_id: content_id(
                TRANSPORT_REQUEST_ID_DOMAIN,
                &(attempt_id.as_str(), claim.continuation_attempt_id.as_str()),
            )?,
            state: crate::OperationAttemptState::Running,
            outcome: None,
        };
        attempt.verify()?;
        occurrence.attempt_count = ordinal;
        occurrence.latest_attempt_id.clone_from(&attempt_id);
        occurrence.verify()?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            attempt_id.clone(),
            Vec::new(),
            vec![input],
        )?;
        self.commit_material_sidecars(
            &material,
            &attempt_id,
            vec![
                DurableOperation::PutComponentOccurrence { value: occurrence },
                DurableOperation::PutOperationAttempt {
                    value: attempt.clone(),
                },
            ],
        )?;
        Ok(attempt)
    }

    /// Close only the provider Attempt admitted by the exact current claim.
    /// The immutable Plan, not executor-authored continuation fields, owns the
    /// successor position and terminal failure disposition.
    pub(crate) fn commit_component_result_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
        attempt_id: &str,
        result: &crate::executor::ExecutorComponentResult,
    ) -> DurableResult<()> {
        let (attempt, occurrence) = self.read_component_result_frontier(claim, attempt_id)?;
        let (outcome, artifact) = validate_component_result_material(result)?;

        if attempt.state == crate::OperationAttemptState::Completed {
            if attempt.outcome.as_ref() == Some(&outcome)
                && occurrence.outcome.as_ref() == Some(&outcome)
                && occurrence.state == crate::ComponentOccurrenceState::Completed
            {
                return Ok(());
            }
            return Err(DurableError::HistoryConflict {
                code: "component_result_replay_mismatch".to_owned(),
                message: format!("component Attempt {attempt_id} changed its retained result"),
            });
        }
        self.require_execution_claim_pinned(claim)?;
        let prepared = self.prepare_component_result_pinned(
            claim,
            attempt,
            occurrence,
            result,
            (outcome, artifact),
        )?;
        match result {
            crate::executor::ExecutorComponentResult::Succeeded { .. } => {
                self.commit_component_success(prepared)
            }
            crate::executor::ExecutorComponentResult::ExpectedFailure { failure, .. } => {
                self.commit_component_failure(claim, attempt_id, failure, prepared)
            }
        }
    }

    fn prepare_component_result_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
        mut attempt: OperationAttempt,
        mut occurrence: ComponentOccurrence,
        result: &crate::executor::ExecutorComponentResult,
        validated: (ComponentOutcome, &cymule_core::ArtifactRecord),
    ) -> DurableResult<PreparedComponentResult> {
        let (outcome, artifact) = validated;
        if attempt.state != crate::OperationAttemptState::Running
            || occurrence.state != crate::ComponentOccurrenceState::Pending
        {
            return Err(DurableError::Conflict {
                expected: Some(format!("{}:{}", claim.owner, claim.fence)),
                current: Some(format!("{:?}", attempt.state)),
            });
        }
        let read = self.read_execution_step(&claim.run_id)?;
        let input = self.read_current_state_root(|manifest, resolver| {
            Self::load_evolution_machine_artifact(manifest, resolver, &occurrence.input)
        })?;
        let mut derived = derive_pinned_component_occurrence(&read, &input)?;
        derived.attempt_count = occurrence.attempt_count;
        derived
            .latest_attempt_id
            .clone_from(&occurrence.latest_attempt_id);
        if derived != occurrence {
            return Err(DurableError::HistoryConflict {
                code: "component_result_source_mismatch".to_owned(),
                message: "component result is not at its admitted Plan position".to_owned(),
            });
        }
        let next = derive_pinned_component_result(&read, result)?;
        occurrence.state = crate::ComponentOccurrenceState::Completed;
        occurrence.outcome = Some(outcome.clone());
        occurrence.continuation_digest = Some(canonical_digest(&next)?);
        occurrence.verify()?;
        attempt.state = crate::OperationAttemptState::Completed;
        attempt.outcome = Some(outcome);
        attempt.verify()?;
        let material_source_id = derived_command_id(
            DerivedCommandOperation::AdvanceContinuation,
            &(
                attempt.attempt_id.as_str(),
                &attempt.outcome,
                &occurrence.continuation_digest,
            ),
        )?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            material_source_id,
            Vec::new(),
            vec![artifact.clone()],
        )?;
        Ok(PreparedComponentResult {
            run: read.run,
            continuation: next,
            occurrence,
            attempt,
            material,
        })
    }

    fn read_component_result_frontier(
        &mut self,
        claim: &ContinuationExecutionClaim,
        attempt_id: &str,
    ) -> DurableResult<(OperationAttempt, ComponentOccurrence)> {
        self.read_current_state_root(|manifest, resolver| {
            let mut view =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
            let attempt = view.operation_attempt(attempt_id)?.ok_or_else(|| {
                DurableError::NotFound(format!("component Attempt {attempt_id} does not exist"))
            })?;
            let frontier = view
                .component_attempt_frontier(&attempt.occurrence_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "component_attempt_occurrence_missing".to_owned(),
                    message: format!("component Attempt {attempt_id} has no occurrence"),
                })?;
            if attempt.run_id != claim.run_id
                || attempt.execution_claim_owner != claim.owner
                || attempt.execution_claim_fence != claim.fence
                || attempt.continuation_attempt_id != claim.continuation_attempt_id
                || attempt != frontier.latest_attempt
            {
                return Err(DurableError::Conflict {
                    expected: Some(attempt_id.to_owned()),
                    current: Some(frontier.latest_attempt.attempt_id),
                });
            }
            Ok((attempt, frontier.occurrence))
        })
    }

    fn commit_component_success(&mut self, prepared: PreparedComponentResult) -> DurableResult<()> {
        let PreparedComponentResult {
            run,
            continuation,
            occurrence,
            attempt,
            material,
        } = prepared;
        let attempt_id = attempt.attempt_id.clone();
        self.commit_material_sidecars(
            &material,
            &attempt_id,
            vec![
                DurableOperation::PutContinuation {
                    value: continuation.clone(),
                },
                DurableOperation::PutRunCurrent {
                    value: pinned_durable_run_current(&run.run, &continuation)?,
                },
                DurableOperation::PutComponentOccurrence { value: occurrence },
                DurableOperation::PutOperationAttempt { value: attempt },
            ],
        )?;
        Ok(())
    }

    fn commit_component_failure(
        &mut self,
        claim: &ContinuationExecutionClaim,
        attempt_id: &str,
        failure: &cymule_core::RunFailure,
        prepared: PreparedComponentResult,
    ) -> DurableResult<()> {
        let PreparedComponentResult {
            run: run_read,
            continuation: mut next,
            occurrence,
            attempt,
            material,
        } = prepared;
        let command = Command::FailRun {
            failure: failure.clone(),
        };
        let command_id = derived_command_id(
            DerivedCommandOperation::FailRun,
            &(claim.run_id.as_str(), attempt_id, &command),
        )?;
        let commands = vec![cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: claim.run_id.clone(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                Some(run_read.run.precondition_token()),
            ),
            command,
        }];
        let terminal = self.read_terminal_sidecars(&run_read, Some(attempt_id))?;
        let clock_id = claim.clock_observation_ref.observation_id.clone();
        let commit =
            self.commit_pinned_command_batch(commands, Some(material), move |transition| {
                let run = pinned_batch_final_run(transition)?;
                next.epoch = run.result_current.epoch;
                next.verify_wire()?;
                if occurrence.continuation_digest.as_deref()
                    != Some(canonical_digest(&next)?.as_str())
                {
                    return Err(DurableError::Integrity {
                        code: "component_failure_continuation_mismatch".to_owned(),
                        message: "Core failure changed the derived terminal epoch".to_owned(),
                    });
                }
                let mut operations = terminal.finish()?;
                operations.extend([
                    DurableOperation::PutContinuation {
                        value: next.clone(),
                    },
                    DurableOperation::PutRunCurrent {
                        value: pinned_durable_run_current(&run.result_current, &next)?,
                    },
                    DurableOperation::RemoveClockObservation {
                        observation_id: clock_id,
                    },
                    DurableOperation::PutComponentOccurrence { value: occurrence },
                    DurableOperation::PutOperationAttempt { value: attempt },
                ]);
                Ok(operations)
            })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        Ok(())
    }

    /// Finish only a previously admitted `ExpectedFailure`. The retained Core
    /// transition and staged material are the authority; this path performs no
    /// provider or Clock operation and cannot invent a new failure.
    pub(crate) fn recover_admitted_component_failure(
        &mut self,
        run_id: &str,
    ) -> DurableResult<bool> {
        let recovery = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                .pending_terminal_recovery(run_id)
        })?;
        let Some(recovery) = recovery else {
            return Ok(false);
        };
        let Command::FailRun { failure } = &recovery.transition.envelope.command else {
            return Ok(false);
        };
        let read = self
            .read_executor_run(run_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "component_failure_recovery_run_missing".to_owned(),
                message: format!("admitted failure Run {run_id} disappeared"),
            })?;
        let claim =
            read.continuation
                .execution_claim
                .clone()
                .ok_or_else(|| DurableError::Integrity {
                    code: "component_failure_recovery_claim_missing".to_owned(),
                    message: "admitted failure lost its original execution claim".to_owned(),
                })?;
        let step = self.read_execution_step(run_id)?;
        let occurrence_id = current_component_attempt_for_takeover(&step)?.ok_or_else(|| {
            DurableError::Integrity {
                code: "component_failure_recovery_occurrence_missing".to_owned(),
                message: "admitted failure is not at its retained component Call".to_owned(),
            }
        })?;
        let (attempt, occurrence) = self.read_current_state_root(|manifest, resolver| {
            let mut view =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
            let frontier = view
                .component_attempt_frontier(&occurrence_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "component_failure_recovery_frontier_missing".to_owned(),
                    message: "admitted failure lost its component Attempt frontier".to_owned(),
                })?;
            Ok((frontier.latest_attempt, frontier.occurrence))
        })?;
        if attempt.execution_claim_owner != claim.owner
            || attempt.execution_claim_fence != claim.fence
            || attempt.continuation_attempt_id != claim.continuation_attempt_id
            || !recovery.material.plans().is_empty()
            || recovery.material.artifacts().len() != 1
        {
            return Err(DurableError::Integrity {
                code: "component_failure_recovery_source_mismatch".to_owned(),
                message: "admitted failure changed its claim or staged material closure".to_owned(),
            });
        }
        let detail = recovery.material.artifacts()[0].clone();
        if detail.reference != failure.detail {
            return Err(DurableError::Integrity {
                code: "component_failure_recovery_detail_mismatch".to_owned(),
                message: "admitted failure detail differs from its staged Artifact".to_owned(),
            });
        }
        let result = crate::executor::ExecutorComponentResult::ExpectedFailure {
            failure: failure.clone(),
            detail,
        };
        let validated = validate_component_result_material(&result)?;
        let prepared =
            self.prepare_component_result_pinned(&claim, attempt, occurrence, &result, validated)?;
        if prepared.material != recovery.material {
            return Err(DurableError::Integrity {
                code: "component_failure_recovery_material_mismatch".to_owned(),
                message: "rederived component failure changed its admitted material".to_owned(),
            });
        }
        let attempt_id = prepared.attempt.attempt_id.clone();
        self.commit_component_failure(&claim, &attempt_id, failure, prepared)?;
        Ok(true)
    }

    pub(crate) fn commit_effect_enqueue_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
        expected_revision: &str,
        source: &Continuation,
        args: cymule_core::ArtifactRecord,
        dispatch: EffectDispatch,
    ) -> DurableResult<()> {
        self.require_execution_claim_pinned(claim)?;
        let read = self.read_execution_step(&claim.run_id)?;
        if read.run.revision != expected_revision || read.run.continuation != *source {
            return Err(DurableError::Conflict {
                expected: Some(expected_revision.to_owned()),
                current: Some(read.run.revision),
            });
        }
        let (proposal, next, eager) = derive_pinned_effect_enqueue(&read, &args, &dispatch)?;
        if self
            .read_effect_execution(&claim.run_id, &dispatch.intent_id, expected_revision)?
            .is_some()
        {
            return Err(DurableError::HistoryConflict {
                code: "effect_enqueue_already_retained".to_owned(),
                message: format!(
                    "Effect {} already has durable admission",
                    dispatch.intent_id
                ),
            });
        }
        let proposal_id = derived_command_id(
            DerivedCommandOperation::ProposeEffect,
            &(claim.run_id.as_str(), &proposal),
        )?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            proposal_id.clone(),
            Vec::new(),
            vec![args],
        )?;
        let mut commands = vec![cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id: proposal_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: claim.run_id.clone(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                Some(read.run.run.precondition_token()),
            ),
            command: proposal,
        }];
        commands.push(derived_effect_batch_command(
            &claim.run_id,
            &dispatch.intent_id,
            DerivedCommandOperation::PrepareEffect,
            EffectTransition::Prepare,
            &(),
        )?);
        if eager {
            commands.push(derived_effect_batch_command(
                &claim.run_id,
                &dispatch.intent_id,
                DerivedCommandOperation::AuthorizeEffect,
                EffectTransition::AuthorizeRelease,
                &(),
            )?);
        }
        let mut dispatch = dispatch;
        let commit =
            self.commit_pinned_command_batch(commands, Some(material), move |transition| {
                let run = pinned_batch_final_run(transition)?;
                let effect = run.effects.get(&dispatch.intent_id).ok_or_else(|| {
                    DurableError::Integrity {
                        code: "effect_enqueue_core_missing".to_owned(),
                        message: "Effect enqueue batch lost its exact final Effect".to_owned(),
                    }
                })?;
                synchronize_pinned_effect_projection(effect, &mut dispatch)?;
                Ok(vec![
                    DurableOperation::PutOutbox { value: dispatch },
                    DurableOperation::PutContinuation {
                        value: next.clone(),
                    },
                    DurableOperation::PutRunCurrent {
                        value: pinned_durable_run_current(&run.result_current, &next)?,
                    },
                ])
            })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        Ok(())
    }

    pub(crate) fn commit_effect_claim_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
        read: &ExecutorEffectRead,
    ) -> DurableResult<ExecutorEffectClaimRead> {
        self.require_execution_claim_pinned(claim)?;
        if self.current_revision()? != read.revision
            || read.run.continuation.execution_claim.as_ref() != Some(claim)
            || read.dispatch.state != OutboxState::Pending
            || read.dispatch.claim_owner.is_some()
            || read.dispatch.claim_epoch != 0
            || read.dispatch.run_id != claim.run_id
        {
            return Err(DurableError::Conflict {
                expected: Some(read.revision.clone()),
                current: Some(self.current_revision()?.to_owned()),
            });
        }
        let lease = self.read_current_state_root(|manifest, resolver| {
            let previous: Option<CoordinationLease> =
                crate::state_root::load_typed_state_map_value(
                    &manifest.roots().leases,
                    &read.dispatch.intent_id,
                    crate::StateRootLeafKind::Lease,
                    resolver,
                )?;
            proposed_pinned_lease(
                previous.as_ref(),
                &read.dispatch.intent_id,
                &claim.owner,
                claim.logical_acquired_at,
                claim.logical_ttl,
            )
        })?;
        let mut commands = Vec::new();
        if read.effect.phase == cymule_core::EffectPhase::Prepared {
            commands.push(derived_effect_batch_command(
                &claim.run_id,
                &read.dispatch.intent_id,
                DerivedCommandOperation::AuthorizeEffect,
                EffectTransition::AuthorizeRelease,
                &(),
            )?);
        } else if read.effect.phase != cymule_core::EffectPhase::ReleaseAuthorized {
            return Err(DurableError::IllegalTransition(
                "Effect claim is not prepared or release-authorized".to_owned(),
            ));
        }
        commands.push(derived_effect_batch_command(
            &claim.run_id,
            &read.dispatch.intent_id,
            DerivedCommandOperation::StartEffectDispatch,
            EffectTransition::StartDispatch,
            &(lease.owner.as_str(), lease.epoch),
        )?);
        let first = commands
            .first_mut()
            .ok_or_else(|| DurableError::Integrity {
                code: "effect_claim_commands_missing".to_owned(),
                message: "Effect claim has no command".to_owned(),
            })?;
        first.precondition = cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
            Some(read.run.run.precondition_token()),
        );
        let next = read.run.continuation.clone();
        let mut acknowledgement = None;
        let commit = self.commit_pinned_command_batch(commands, None, |transition| {
            let (derived, operations) =
                derive_effect_claim_acknowledgement(read, transition, &lease, &next)?;
            acknowledgement = Some(derived);
            Ok(operations)
        })?;
        finish_effect_claim_acknowledgement(&read.dispatch.intent_id, commit, acknowledgement)
    }

    pub(crate) fn require_effect_claim_current(
        &mut self,
        claim: &ContinuationExecutionClaim,
        acknowledged: ExecutorEffectClaimRead,
    ) -> DurableResult<ExecutorEffectClaimRead> {
        self.require_execution_claim_pinned(claim)?;
        let revision = self.current_revision()?.to_owned();
        let retained = self
            .read_effect_execution(
                &acknowledged.read.dispatch.run_id,
                &acknowledged.read.dispatch.intent_id,
                &revision,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "effect_claim_readback_missing".to_owned(),
                message: "acknowledged Effect claim lost its current outbox".to_owned(),
            })?;
        let mut expected = acknowledged.read;
        expected.revision.clone_from(&retained.revision);
        expected.run.revision.clone_from(&retained.run.revision);
        expected
            .run
            .projection_root
            .clone_from(&retained.run.projection_root);
        if retained.dispatch.state != OutboxState::Claimed
            || retained.dispatch.claim_owner.as_deref() != Some(acknowledged.owner.as_str())
            || retained.dispatch.claim_epoch != acknowledged.epoch
            || retained != expected
        {
            return Err(DurableError::Integrity {
                code: "effect_claim_readback_mismatch".to_owned(),
                message: "acknowledged Effect claim changed before provider invocation".to_owned(),
            });
        }
        let provider_attempt = cymule_runtime::EffectProviderAttempt::new(
            &retained.dispatch.intent_id,
            &acknowledged.owner,
            acknowledged.epoch,
        )?;
        if provider_attempt != acknowledged.provider_attempt {
            return Err(DurableError::Integrity {
                code: "effect_claim_provider_attempt_mismatch".to_owned(),
                message: "acknowledged Effect claim changed its provider Attempt".to_owned(),
            });
        }
        Ok(ExecutorEffectClaimRead {
            read: retained,
            owner: acknowledged.owner,
            epoch: acknowledged.epoch,
            provider_attempt,
        })
    }

    pub(crate) fn commit_effect_settlement_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
        read: &ExecutorEffectRead,
        settlement: crate::executor::ExecutorEffectSettlement,
    ) -> DurableResult<()> {
        self.require_execution_claim_pinned(claim)?;
        if self.current_revision()? != read.revision
            || read.dispatch.run_id != claim.run_id
            || read.run.continuation.execution_claim.as_ref() != Some(claim)
        {
            return Err(DurableError::Conflict {
                expected: Some(read.revision.clone()),
                current: Some(self.current_revision()?.to_owned()),
            });
        }
        let (operation, transition, state, result) =
            derive_pinned_effect_settlement(read, settlement)?;
        let mut command = derived_effect_batch_command(
            &claim.run_id,
            &read.dispatch.intent_id,
            operation,
            transition,
            &(
                read.dispatch.claim_owner.as_deref(),
                read.dispatch.claim_epoch,
                result.as_ref().map(|record| &record.reference),
            ),
        )?;
        command.precondition =
            cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(Some(
                read.run.run.precondition_token(),
            ));
        let material = result
            .as_ref()
            .map(|record| {
                cymule_core::durable_internal::MachineMaterialAdmission::new(
                    command.command_id.clone(),
                    Vec::new(),
                    vec![record.clone()],
                )
            })
            .transpose()?;
        let mut dispatch = read.dispatch.clone();
        dispatch.state = state;
        dispatch.result = result.map(|record| record.reference);
        let next = read.run.continuation.clone();
        let commit =
            self.commit_pinned_command_batch(vec![command], material, move |transition| {
                let run = pinned_batch_final_run(transition)?;
                let effect = run.effects.get(&dispatch.intent_id).ok_or_else(|| {
                    DurableError::Integrity {
                        code: "effect_settlement_core_missing".to_owned(),
                        message: "Effect settlement batch lost its exact final Effect".to_owned(),
                    }
                })?;
                synchronize_pinned_effect_projection(effect, &mut dispatch)?;
                Ok(vec![
                    DurableOperation::PutOutbox { value: dispatch },
                    DurableOperation::PutRunCurrent {
                        value: pinned_durable_run_current(&run.result_current, &next)?,
                    },
                ])
            })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        Ok(())
    }

    pub(crate) fn read_effect_resolution_material(
        &mut self,
        command: &EffectResolutionCommand,
    ) -> DurableResult<ExecutorEffectRead> {
        command.verify()?;
        let revision = self.current_revision()?.to_owned();
        let read = self
            .read_effect_execution(&command.run_id, &command.intent_id, &revision)?
            .ok_or_else(|| {
                DurableError::NotFound(format!("Effect {} does not exist", command.intent_id))
            })?;
        if read.dispatch.execution_binding != command.execution_binding
            || read.dispatch.occurrence_binding != command.occurrence_binding
            || read.dispatch.claim_owner.as_deref() != Some(command.claim_owner.as_str())
            || read.dispatch.claim_epoch != command.claim_epoch
        {
            return Err(DurableError::Conflict {
                expected: Some(format!("{}:{}", command.claim_owner, command.claim_epoch)),
                current: read
                    .dispatch
                    .claim_owner
                    .as_ref()
                    .map(|owner| format!("{owner}:{}", read.dispatch.claim_epoch)),
            });
        }
        if read.dispatch.state != OutboxState::Unknown
            || read.effect.outcome != WorldOutcome::Unknown
        {
            return Err(DurableError::IllegalTransition(format!(
                "Effect {} is not awaiting external resolution",
                command.intent_id
            )));
        }
        Ok(read)
    }

    pub(crate) fn commit_effect_resolution_pinned(
        &mut self,
        command: &EffectResolutionCommand,
        resolution: ReconciliationResolution,
        result: Option<cymule_core::ArtifactRecord>,
    ) -> DurableResult<EffectResolutionReceipt> {
        if let Some(receipt) = self.effect_resolution_receipt(&command.resolution_id)? {
            if receipt.command_matches(command) {
                return Ok(receipt);
            }
            return Err(DurableError::HistoryConflict {
                code: "effect_resolution_command_reused".to_owned(),
                message: "Effect resolution identity was reused with different semantics"
                    .to_owned(),
            });
        }
        if !matches!(
            resolution,
            ReconciliationResolution::ResolvedApplied
                | ReconciliationResolution::ResolvedNotApplied
        ) {
            return Err(DurableError::Validation(
                "Effect resolution must be terminal".to_owned(),
            ));
        }
        let read = self.read_effect_resolution_material(command)?;
        let (_, transition, state, result) = derive_pinned_effect_settlement(
            &read,
            crate::executor::ExecutorEffectSettlement::Reconciliation { resolution, result },
        )?;
        let value = result
            .as_ref()
            .map(|record| crate::model::decode_artifact_value(&record.reference, record))
            .transpose()?;
        let result_ref = result.as_ref().map(|record| record.reference.clone());
        let receipt =
            EffectResolutionReceipt::new(command.clone(), resolution, value, result_ref.clone())?;
        let retained_receipt = receipt.clone();
        let material = result
            .map(|record| {
                cymule_core::durable_internal::MachineMaterialAdmission::new(
                    command.resolution_id.clone(),
                    Vec::new(),
                    vec![record],
                )
            })
            .transpose()?;
        let member = cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id: command.resolution_id.clone(),
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: command.run_id.clone(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                Some(read.run.run.precondition_token()),
            ),
            command: Command::TransitionEffect {
                intent_id: command.intent_id.clone(),
                transition,
            },
        };
        let mut dispatch = read.dispatch;
        dispatch.state = state;
        dispatch.result = result_ref;
        let next = read.run.continuation;
        let commit =
            self.commit_pinned_command_batch(vec![member], material, move |transition| {
                let run = pinned_batch_final_run(transition)?;
                let effect = run.effects.get(&dispatch.intent_id).ok_or_else(|| {
                    DurableError::Integrity {
                        code: "effect_resolution_core_missing".to_owned(),
                        message: "Effect resolution batch lost its exact final Effect".to_owned(),
                    }
                })?;
                synchronize_pinned_effect_projection(effect, &mut dispatch)?;
                Ok(vec![
                    DurableOperation::PutOutbox { value: dispatch },
                    DurableOperation::PutRunCurrent {
                        value: pinned_durable_run_current(&run.result_current, &next)?,
                    },
                    DurableOperation::PutEffectResolutionReceipt {
                        value: retained_receipt,
                    },
                ])
            })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        if commit.committed_revision.is_none() {
            return Err(DurableError::HistoryConflict {
                code: "effect_resolution_sidecar_missing".to_owned(),
                message: "Effect resolution Core command exists without its immutable receipt"
                    .to_owned(),
            });
        }
        Ok(receipt)
    }

    fn commit_executor_projection(
        &mut self,
        read: &ExecutorRunRead,
        claim: &ContinuationExecutionClaim,
        next: &Continuation,
        artifacts: Vec<cymule_core::ArtifactRecord>,
    ) -> DurableResult<()> {
        if next.status != ContinuationStatus::Running
            || next.execution_claim.as_ref() != Some(claim)
        {
            return Err(DurableError::Validation(
                "executor projection must retain the exact Running claim".to_owned(),
            ));
        }
        let operations = vec![
            DurableOperation::PutContinuation {
                value: next.clone(),
            },
            DurableOperation::PutRunCurrent {
                value: pinned_durable_run_current(&read.run, next)?,
            },
        ];
        if artifacts.is_empty() {
            self.commit_profile_operations(operations)?;
        } else {
            let source_id = derived_command_id(
                DerivedCommandOperation::AdvanceContinuation,
                &(claim.run_id.as_str(), canonical_digest(next)?, &artifacts),
            )?;
            let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
                source_id.clone(),
                Vec::new(),
                artifacts,
            )?;
            self.commit_material_sidecars(&material, &source_id, operations)?;
        }
        Ok(())
    }

    fn commit_executor_single_command(
        &mut self,
        read: &ExecutorRunRead,
        claim: &ContinuationExecutionClaim,
        operation: DerivedCommandOperation,
        command: Command,
        next: &Continuation,
    ) -> DurableResult<()> {
        if next.status != ContinuationStatus::Running
            || next.execution_claim.as_ref() != Some(claim)
        {
            return Err(DurableError::Validation(
                "executor Machine boundary must retain the exact Running claim".to_owned(),
            ));
        }
        let command_id = derived_command_id(operation, &(claim.run_id.as_str(), &command))?;
        let envelope = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: claim.run_id.clone(),
            expected_precondition: Some(read.run.precondition_token()),
            command,
        };
        let retained = next.clone();
        let commit = self.commit_pinned_command(envelope, None, move |transition| {
            let run = transition
                .delta
                .run
                .as_ref()
                .ok_or_else(|| DurableError::Integrity {
                    code: "executor_command_core_current_missing".to_owned(),
                    message: "executor Machine transition has no final Run current".to_owned(),
                })?;
            Ok(vec![
                DurableOperation::PutContinuation {
                    value: retained.clone(),
                },
                DurableOperation::PutRunCurrent {
                    value: pinned_durable_run_current(&run.result_current, &retained)?,
                },
            ])
        })?;
        require_applied_command_receipt(commit.receipt)
    }

    fn commit_executor_scope(
        &mut self,
        read: &ExecutorRunRead,
        claim: &ContinuationExecutionClaim,
        command: Command,
        result: cymule_core::ArtifactRecord,
        next: &Continuation,
    ) -> DurableResult<()> {
        if next.status != ContinuationStatus::Running
            || next.execution_claim.as_ref() != Some(claim)
        {
            return Err(DurableError::Validation(
                "CommitScope must retain the exact Running claim".to_owned(),
            ));
        }
        result.validate()?;
        let command_id = derived_command_id(
            DerivedCommandOperation::CommitScope,
            &(claim.run_id.as_str(), &command),
        )?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            command_id.clone(),
            Vec::new(),
            vec![result],
        )?;
        let commands = vec![cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: claim.run_id.clone(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                Some(read.run.precondition_token()),
            ),
            command,
        }];
        let retained = next.clone();
        let commit =
            self.commit_pinned_command_batch(commands, Some(material), move |transition| {
                let run = transition
                    .steps
                    .last()
                    .and_then(|step| step.run.as_ref())
                    .ok_or_else(|| DurableError::Integrity {
                        code: "commit_scope_core_current_missing".to_owned(),
                        message: "CommitScope batch has no final Run current".to_owned(),
                    })?;
                Ok(vec![
                    DurableOperation::PutContinuation {
                        value: retained.clone(),
                    },
                    DurableOperation::PutRunCurrent {
                        value: pinned_durable_run_current(&run.result_current, &retained)?,
                    },
                ])
            })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        Ok(())
    }

    fn commit_yield_boundary(
        &mut self,
        read: &ExecutorRunRead,
        claim: &ContinuationExecutionClaim,
        next: Continuation,
        wait: Option<WaitCondition>,
    ) -> DurableResult<()> {
        let command = Command::YieldAttempt {
            attempt_id: claim.continuation_attempt_id.clone(),
            continuation_epoch: read.continuation.epoch,
            execution_fence: claim.fence,
        };
        let command_id = derived_command_id(
            DerivedCommandOperation::YieldContinuationAttempt,
            &(
                claim.run_id.as_str(),
                &command,
                wait.as_ref().map(|wait| wait.wait_id.as_str()),
            ),
        )?;
        let envelope = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: claim.run_id.clone(),
            expected_precondition: Some(read.run.precondition_token()),
            command,
        };
        let retained = next;
        let clock_id = claim.clock_observation_ref.observation_id.clone();
        let commit = self.commit_pinned_command(envelope, None, move |transition| {
            let run = transition
                .delta
                .run
                .as_ref()
                .ok_or_else(|| DurableError::Integrity {
                    code: "yield_boundary_core_current_missing".to_owned(),
                    message: "YieldAttempt transition has no final Run current".to_owned(),
                })?;
            let mut operations = vec![
                DurableOperation::PutContinuation {
                    value: retained.clone(),
                },
                DurableOperation::PutRunCurrent {
                    value: pinned_durable_run_current(&run.result_current, &retained)?,
                },
                DurableOperation::RemoveClockObservation {
                    observation_id: clock_id,
                },
            ];
            if let Some(wait) = wait {
                operations.push(DurableOperation::PutWait { value: wait });
            }
            Ok(operations)
        })?;
        require_applied_command_receipt(commit.receipt)
    }

    fn commit_complete_boundary(
        &mut self,
        read: &ExecutorRunRead,
        claim: &ContinuationExecutionClaim,
        next: Continuation,
        result: cymule_core::ArtifactRecord,
    ) -> DurableResult<()> {
        let yield_command = Command::YieldAttempt {
            attempt_id: claim.continuation_attempt_id.clone(),
            continuation_epoch: read.continuation.epoch,
            execution_fence: claim.fence,
        };
        let yield_id = derived_command_id(
            DerivedCommandOperation::YieldContinuationAttempt,
            &(claim.run_id.as_str(), &yield_command, "complete"),
        )?;
        let complete_command = Command::CompleteRun {
            result: Some(result.reference.clone()),
        };
        let complete_id = derived_command_id(
            DerivedCommandOperation::CompleteRun,
            &(claim.run_id.as_str(), &complete_command),
        )?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            complete_id.clone(),
            Vec::new(),
            vec![result],
        )?;
        let commands = vec![
            cymule_core::durable_internal::MachinePinnedBatchCommand {
                command_id: yield_id,
                actor: DURABLE_RUNTIME_ACTOR.to_owned(),
                run_id: claim.run_id.clone(),
                precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                    Some(read.run.precondition_token()),
                ),
                command: yield_command,
            },
            cymule_core::durable_internal::MachinePinnedBatchCommand {
                command_id: complete_id,
                actor: DURABLE_RUNTIME_ACTOR.to_owned(),
                run_id: claim.run_id.clone(),
                precondition:
                    cymule_core::durable_internal::MachinePinnedBatchPrecondition::Derived,
                command: complete_command,
            },
        ];
        let retained = next;
        let clock_id = claim.clock_observation_ref.observation_id.clone();
        let commit =
            self.commit_pinned_command_batch(commands, Some(material), move |transition| {
                let run = transition
                    .steps
                    .last()
                    .and_then(|step| step.run.as_ref())
                    .ok_or_else(|| DurableError::Integrity {
                        code: "complete_boundary_core_current_missing".to_owned(),
                        message: "completion batch has no final Run current".to_owned(),
                    })?;
                Ok(vec![
                    DurableOperation::PutContinuation {
                        value: retained.clone(),
                    },
                    DurableOperation::PutRunCurrent {
                        value: pinned_durable_run_current(&run.result_current, &retained)?,
                    },
                    DurableOperation::RemoveClockObservation {
                        observation_id: clock_id,
                    },
                ])
            })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        Ok(())
    }

    pub(crate) fn admit_wait_activation_pinned(
        &mut self,
        activation_id: impl Into<String>,
        source: WaitActivationSource,
        wait_ids: BTreeSet<String>,
        value: &serde_json::Value,
    ) -> DurableResult<crate::WaitAdmissionOutcome> {
        let receipt =
            self.admit_wait_activation_receipt_pinned(activation_id, source, wait_ids, value)?;
        Ok(crate::WaitAdmissionOutcome {
            disposition: receipt.disposition(),
            ready_run_ids: receipt.ready_run_ids,
        })
    }

    fn admit_wait_activation_receipt_pinned(
        &mut self,
        activation_id: impl Into<String>,
        source: WaitActivationSource,
        wait_ids: BTreeSet<String>,
        value: &serde_json::Value,
    ) -> DurableResult<WaitActivationReceipt> {
        let result = cymule_core::ArtifactRecord {
            reference: cymule_core::artifact_ref(
                crate::WAIT_RESULT_ARTIFACT_KIND,
                &cymule_core::canonical_bytes(value)?,
            )?,
            bytes: cymule_core::canonical_bytes(value)?,
        };
        result.validate()?;
        let activation =
            WaitActivation::new(activation_id, source, wait_ids, result.reference.clone())?;
        let WaitActivationRead {
            existing,
            waits,
            mut continuations,
            run_currents,
        } = self.read_current_state_root(|manifest, resolver| {
            load_wait_activation_neighborhood(manifest, resolver, &activation)
        })?;
        if let Some(existing) = existing {
            if existing.activation != activation {
                return Err(DurableError::HistoryConflict {
                    code: "wait_activation_reused".to_owned(),
                    message: format!(
                        "Wait activation {} was reused with different semantics",
                        activation.activation_id
                    ),
                });
            }
            return Ok(existing);
        }

        let consume_once_targets = waits.values().filter(|wait| wait.consume_once).count();
        activation
            .source
            .validate_target_cardinality(activation.wait_ids.len(), consume_once_targets)?;
        let mut applied_wait_ids = BTreeSet::new();
        let mut completed_waits = Vec::new();
        for (wait_id, wait) in waits {
            if wait.state != WaitState::Pending {
                continue;
            }
            let continuation =
                continuations
                    .get_mut(&wait.run_id)
                    .ok_or_else(|| DurableError::Integrity {
                        code: "wait_activation_projection_missing".to_owned(),
                        message: format!("Wait {wait_id} has no projected Continuation"),
                    })?;
            apply_wait_result(&wait, &activation.result, continuation)?;
            let mut completed = wait;
            completed.state = WaitState::Completed;
            completed.result = Some(activation.result.clone());
            completed.verify_wire()?;
            applied_wait_ids.insert(wait_id);
            completed_waits.push(completed);
        }
        let ready_run_ids = continuations
            .iter()
            .filter(|(_, continuation)| continuation.status == ContinuationStatus::Ready)
            .map(|(run_id, _)| run_id.clone())
            .collect::<BTreeSet<_>>();
        let receipt = WaitActivationReceipt {
            receipt_version: WAIT_ACTIVATION_RECEIPT_VERSION.to_owned(),
            activation,
            applied_wait_ids,
            ready_run_ids,
        };
        receipt.verify()?;
        let mut operations = Vec::new();
        for wait in completed_waits {
            operations.push(DurableOperation::PutWait { value: wait });
        }
        for (run_id, continuation) in continuations {
            let run = run_currents
                .get(&run_id)
                .ok_or_else(|| DurableError::Integrity {
                    code: "wait_activation_core_projection_missing".to_owned(),
                    message: format!("Run {run_id} has no Core projection for Wait activation"),
                })?;
            operations.push(DurableOperation::PutContinuation {
                value: continuation.clone(),
            });
            operations.push(DurableOperation::PutRunCurrent {
                value: pinned_durable_run_current(run, &continuation)?,
            });
        }
        operations.push(DurableOperation::PutWaitActivation {
            value: receipt.clone(),
        });
        let source_id = content_id(WAIT_ACTIVATION_MATERIAL_DOMAIN, &receipt)?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            source_id.clone(),
            Vec::new(),
            vec![result],
        )?;
        self.commit_material_sidecars(&material, &source_id, operations)?;
        Ok(receipt)
    }

    pub(crate) fn admit_wait_source_delivery_pinned<D: crate::WaitSourceDriver>(
        &mut self,
        driver: &mut D,
        max_targets: usize,
    ) -> DurableResult<Option<(String, crate::WaitAdmissionOutcome)>> {
        if max_targets == 0 || max_targets > crate::MAX_WAIT_DELIVERY_TARGETS {
            return Err(DurableError::Validation(format!(
                "wait source target limit must be between 1 and {}",
                crate::MAX_WAIT_DELIVERY_TARGETS
            )));
        }
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        let delivery = driver.receive(
            &mut StoreParkedWaitView {
                store: &mut self.store,
                manifest,
            },
            max_targets,
        )?;
        let Some(source_delivery) = delivery else {
            return Ok(None);
        };
        let selected_now = source_delivery.is_selected_now();
        let delivery = source_delivery.into_delivery();
        if delivery.wait_ids.is_empty()
            || delivery.wait_ids.len() > crate::MAX_WAIT_DELIVERY_TARGETS
            || (selected_now && delivery.wait_ids.len() > max_targets)
        {
            let bound = if selected_now {
                max_targets
            } else {
                crate::MAX_WAIT_DELIVERY_TARGETS
            };
            return Err(DurableError::Validation(format!(
                "wait source returned {} targets outside its selection bound {bound}",
                delivery.wait_ids.len()
            )));
        }
        let result_bytes = cymule_core::canonical_bytes(&delivery.value)?;
        let expected_activation = WaitActivation::new(
            delivery.activation_id.clone(),
            delivery.source.clone(),
            delivery.wait_ids.clone(),
            cymule_core::artifact_ref(crate::WAIT_RESULT_ARTIFACT_KIND, &result_bytes)?,
        )?;
        let current_manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        if let Some(existing) =
            self.store
                .with_state_root_resolver(&current_manifest, |resolver| {
                    crate::state_root::load_wait_activation(
                        &current_manifest,
                        resolver,
                        &delivery.activation_id,
                    )
                })?
        {
            if existing.activation != expected_activation {
                return Err(DurableError::HistoryConflict {
                    code: "wait_source_activation_reused".to_owned(),
                    message: format!(
                        "Wait activation {} redelivery changed source, targets, or value",
                        delivery.activation_id
                    ),
                });
            }
            return Ok(Some((
                delivery.activation_id,
                crate::WaitAdmissionOutcome {
                    disposition: existing.disposition(),
                    ready_run_ids: existing.ready_run_ids,
                },
            )));
        }
        self.store
            .with_state_root_resolver(&current_manifest, |resolver| {
                crate::state_root::pinned_wait::PinnedParkedWaitView::open(
                    &current_manifest,
                    resolver,
                )?
                .validate_delivery_targets(&delivery.source, &delivery.wait_ids)
            })?;
        let activation_id = delivery.activation_id.clone();
        let outcome = self.admit_wait_activation_pinned(
            delivery.activation_id,
            delivery.source,
            delivery.wait_ids,
            &delivery.value,
        )?;
        Ok(Some((activation_id, outcome)))
    }
}

impl<S: DurableStore> DurableCoordinator<S> {
    /// Open a coordinator from only the current Store head and its exact
    /// manifest. Ordinary open never materializes semantic families or rebuilds
    /// history-wide indexes.
    pub(crate) fn open(mut store: S) -> DurableResult<Self> {
        let pinned = load_pinned_head(&mut store)?;
        Ok(Self { store, pinned })
    }

    /// Initialize an empty durable domain.
    ///
    /// Genesis is intentionally parameter-free: immutable Plans, Artifacts,
    /// Runs, and profile state enter only through their closed typed admission
    /// paths, never through caller-assembled Machine state.
    pub(crate) fn initialize(mut self) -> DurableResult<Self> {
        self.initialize_in_place()?;
        Ok(self)
    }

    /// Initialize an empty coordinator without consuming it.
    pub(crate) fn initialize_in_place(&mut self) -> DurableResult<String> {
        if self.pinned.is_some() {
            return Err(DurableError::IllegalTransition(
                "durable store is already initialized".to_owned(),
            ));
        }
        let machine = Machine::new();
        let snapshot = machine.snapshot();
        let state = DurableState::new(snapshot);
        let batch = StoreBatch::initialize_state(state)?;
        batch.verify_against(None)?;
        let commit = self.store.compare_and_commit(None, &batch)?;
        batch.verify_commit(&commit)?;
        let pinned = PinnedHead::new(
            batch.head().clone(),
            batch.state_root_transition().manifest().clone(),
        )?;
        let revision = pinned.revision().to_owned();
        self.pinned = Some(pinned);
        Ok(revision)
    }

    pub(crate) fn initialize_if_empty(&mut self) -> DurableResult<()> {
        if self.pinned.is_none() {
            self.initialize_in_place()?;
        }
        Ok(())
    }

    /// Atomically admit one new Run's immutable material, Run/first-Attempt
    /// Core transition, current Clock receipt, and Running Continuation through
    /// the exact-load pinned `StateRoot` path.
    pub(crate) fn start_run_pinned(
        &mut self,
        plan: SealedPlan,
        binding: cymule_core::ArtifactRecord,
        input: cymule_core::ArtifactRecord,
        mut continuation: Continuation,
        execution: &ExecutionClaimRequest,
        clock: ClockObservation,
    ) -> DurableResult<PinnedStartRunOutcome> {
        validate_start_run_material(&plan, &binding, &input, &continuation)?;
        validate_clock_receipt(&continuation.run_id, execution, &clock)?;
        let fence = 1;
        let prepared = prepare_start_run(plan, binding, input, &continuation)?;
        let claim = derive_execution_claim(
            &continuation,
            execution,
            &clock,
            fence,
            prepared.attempt_id.clone(),
        )?;
        continuation.execution_fence = fence;
        continuation.execution_claim = Some(claim.clone());
        continuation.status = ContinuationStatus::Running;
        let retained_continuation = continuation.clone();
        let retained_clock = clock;
        let retained_attempt = prepared.attempt_id;
        let commit = self.commit_pinned_command(
            prepared.envelope,
            Some(prepared.material),
            move |transition| {
                let run = transition
                    .delta
                    .run
                    .as_ref()
                    .ok_or_else(|| DurableError::Integrity {
                        code: "start_run_core_current_missing".to_owned(),
                        message: "StartRun transition has no exact Core Run current".to_owned(),
                    })?;
                if run.result_current.active_attempt_id.as_deref()
                    != Some(retained_attempt.as_str())
                {
                    return Err(DurableError::Integrity {
                        code: "start_run_initial_attempt_mismatch".to_owned(),
                        message: "StartRun Core transition did not retain its first Attempt"
                            .to_owned(),
                    });
                }
                let current =
                    pinned_durable_run_current(&run.result_current, &retained_continuation)?;
                Ok(vec![
                    DurableOperation::PutClockObservation {
                        value: retained_clock,
                    },
                    DurableOperation::PutContinuation {
                        value: retained_continuation,
                    },
                    DurableOperation::PutRunCurrent { value: current },
                ])
            },
        )?;
        require_applied_command_receipt(commit.receipt)?;
        if commit.committed_revision.is_some() {
            Ok(PinnedStartRunOutcome::Committed(Box::new(claim)))
        } else {
            Ok(PinnedStartRunOutcome::Replayed)
        }
    }

    /// Authenticate an existing exact `StartRun` before any Clock operation.
    /// A genuinely absent command and Run returns `false` without constructing
    /// a Machine stage; this function never publishes a Store successor.
    pub(crate) fn replay_start_run_pinned(
        &mut self,
        plan: SealedPlan,
        binding: cymule_core::ArtifactRecord,
        input: cymule_core::ArtifactRecord,
        continuation: &Continuation,
    ) -> DurableResult<bool> {
        let command_id = start_run_command_id(&plan, &binding, &input, continuation)?;
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        let retained =
            crate::store::load_pinned_machine_command(&mut self.store, &manifest, &command_id)?;
        let run = self.store.with_state_root_resolver(&manifest, |resolver| {
            crate::state_root::pinned_machine::PinnedMachineView::open(&manifest, resolver)?
                .run_current(&continuation.run_id)
        })?;
        let Some((entry, batch)) = retained else {
            if run.is_some() {
                return Err(DurableError::HistoryConflict {
                    code: "start_run_identity_conflict".to_owned(),
                    message: "Run identity is already owned by another StartRun command".to_owned(),
                });
            }
            return Ok(false);
        };
        if run.is_none() {
            return Err(DurableError::Integrity {
                code: "start_run_current_missing".to_owned(),
                message: "retained StartRun command has no exact Run current".to_owned(),
            });
        }
        validate_start_run_material(&plan, &binding, &input, continuation)?;
        self.verify_start_run_replay_material(&manifest, &plan, &binding, &input)?;
        let prepared = prepare_start_run(plan, binding, input, continuation)?;
        verify_start_run_replay(&entry, &batch, &prepared)?;
        Ok(true)
    }

    fn verify_start_run_replay_material(
        &mut self,
        manifest: &crate::StateRootManifest,
        plan: &SealedPlan,
        binding: &cymule_core::ArtifactRecord,
        input: &cymule_core::ArtifactRecord,
    ) -> DurableResult<()> {
        self.store.with_state_root_resolver(manifest, |resolver| {
            let mut view =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
            if view.plan(&plan.plan_id)?.as_ref() != Some(plan)
                || view.artifact(&binding.reference.artifact_id)?.as_ref() != Some(binding)
                || view.artifact(&input.reference.artifact_id)?.as_ref() != Some(input)
            {
                return Err(DurableError::Integrity {
                    code: "start_run_replay_material_corrupt".to_owned(),
                    message: "retained StartRun Plan, binding, or input changed identity"
                        .to_owned(),
                });
            }
            Ok(())
        })
    }

    pub(crate) fn claim_ready_pinned(
        &mut self,
        run_id: &str,
        execution: &ExecutionClaimRequest,
        clock: ClockObservation,
    ) -> DurableResult<ContinuationExecutionClaim> {
        let read = self
            .read_executor_run(run_id)?
            .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} does not exist")))?;
        let source = read.continuation;
        if source.status != ContinuationStatus::Ready
            || source.execution_claim.is_some()
            || read.run.active_attempt_id.is_some()
        {
            if let Some(active) = source.execution_claim {
                return Err(DurableError::Busy {
                    run_id: run_id.to_owned(),
                    owner: active.owner,
                    fence: active.fence,
                });
            }
            return Err(DurableError::IllegalTransition(format!(
                "Continuation {run_id} is not claim-free Ready"
            )));
        }
        validate_clock_receipt(run_id, execution, &clock)?;
        let epoch = source
            .epoch
            .checked_add(1)
            .filter(|epoch| *epoch <= MAX_EXACT_INTEGER)
            .ok_or_else(|| DurableError::Validation("Continuation epoch overflowed".to_owned()))?;
        let fence = source
            .execution_fence
            .checked_add(1)
            .filter(|fence| *fence <= MAX_EXACT_INTEGER)
            .ok_or_else(|| DurableError::Validation("execution fence overflowed".to_owned()))?;
        let attempt_id = continuation_attempt_id(run_id, epoch, fence)?;
        let claim = derive_execution_claim(&source, execution, &clock, fence, attempt_id.clone())?;
        let commands = derive_continuation_claim_batch(&read.run, &source, &claim, epoch)?;
        let mut target = source;
        target.epoch = epoch;
        target.execution_fence = fence;
        target.execution_claim = Some(claim.clone());
        target.status = ContinuationStatus::Running;
        let retained_target = target.clone();
        let retained_clock = clock;
        let retained_attempt = attempt_id;
        let commit = self.commit_pinned_command_batch(commands, None, move |transition| {
            let run = transition
                .steps
                .last()
                .and_then(|step| step.run.as_ref())
                .ok_or_else(|| DurableError::Integrity {
                    code: "claim_ready_core_current_missing".to_owned(),
                    message: "claim batch has no exact final Core Run current".to_owned(),
                })?;
            if run.result_current.active_attempt_id.as_deref() != Some(retained_attempt.as_str()) {
                return Err(DurableError::Integrity {
                    code: "claim_ready_attempt_mismatch".to_owned(),
                    message: "BeginAttempt did not retain its exact active Attempt".to_owned(),
                });
            }
            let current = pinned_durable_run_current(&run.result_current, &retained_target)?;
            Ok(vec![
                DurableOperation::PutClockObservation {
                    value: retained_clock,
                },
                DurableOperation::PutContinuation {
                    value: retained_target,
                },
                DurableOperation::PutRunCurrent { value: current },
            ])
        })?;
        for receipt in &commit.receipts {
            require_applied_command_receipt(receipt.clone())?;
        }
        cymule_core::validate_content_id("Machine claim batch", &commit.batch_id)?;
        cymule_core::validate_content_id("Machine claim batch receipt", &commit.batch_receipt_id)?;
        if commit
            .committed_revision
            .as_deref()
            .is_some_and(|revision| self.revision() != Some(revision))
        {
            return Err(DurableError::Integrity {
                code: "claim_ready_batch_revision_mismatch".to_owned(),
                message: "claim batch commit revision is not the current pinned head".to_owned(),
            });
        }
        Ok(claim)
    }

    pub(crate) fn require_execution_claim_pinned(
        &mut self,
        claim: &ContinuationExecutionClaim,
    ) -> DurableResult<()> {
        self.refresh_pinned_head()?;
        self.read_current_state_root(|manifest, resolver| {
            let mut view =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
            let material = view.run_execution_material(&claim.run_id)?;
            if let cymule_core::durable_internal::MachineRunReducerState::Transitioning {
                transition_id,
            } = &material.run.reducer_state
            {
                return Err(DurableError::Conflict {
                    expected: Some(format!("{}:{}", claim.owner, claim.fence)),
                    current: Some(transition_id.clone()),
                });
            }
            if material.continuation.execution_claim.as_ref() != Some(claim)
                || material.continuation.status != ContinuationStatus::Running
                || material.run.active_attempt_id.as_deref()
                    != Some(claim.continuation_attempt_id.as_str())
            {
                return Err(DurableError::Conflict {
                    expected: Some(format!("{}:{}", claim.owner, claim.fence)),
                    current: material
                        .continuation
                        .execution_claim
                        .as_ref()
                        .map(|active| format!("{}:{}", active.owner, active.fence)),
                });
            }
            claim.verify_wire(&material.continuation)?;
            let attempt = view
                .attempt_current(&material.run, &claim.continuation_attempt_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "execution_claim_attempt_missing".to_owned(),
                    message: format!(
                        "Run {} active claim has no Core Attempt {}",
                        claim.run_id, claim.continuation_attempt_id
                    ),
                })?;
            if !attempt.active
                || attempt.continuation_id != claim.continuation_id
                || attempt.occurrence_binding != claim.execution_binding_ref.artifact_id
                || attempt.continuation_epoch != material.continuation.epoch
                || attempt.execution_fence != claim.fence
            {
                return Err(DurableError::Integrity {
                    code: "execution_claim_attempt_mismatch".to_owned(),
                    message: format!(
                        "Run {} active claim and Core Attempt disagree",
                        claim.run_id
                    ),
                });
            }
            Ok(())
        })
    }

    pub(crate) fn preflight_takeover_pinned(
        &mut self,
        run_id: &str,
        expected_fence: u64,
    ) -> DurableResult<ExecutorRunRead> {
        let read = self
            .read_executor_run(run_id)?
            .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} does not exist")))?;
        let active = read.continuation.execution_claim.as_ref().ok_or_else(|| {
            DurableError::IllegalTransition(format!(
                "Run {run_id} is not owned by an active execution claim"
            ))
        })?;
        if read.continuation.status != ContinuationStatus::Running
            || active.fence != expected_fence
            || read.run.active_attempt_id.as_deref()
                != Some(active.continuation_attempt_id.as_str())
        {
            return Err(DurableError::Conflict {
                expected: Some(expected_fence.to_string()),
                current: Some(active.fence.to_string()),
            });
        }
        Ok(read)
    }

    pub(crate) fn takeover_running_pinned(
        &mut self,
        run_id: &str,
        expected_fence: u64,
        execution: &ExecutionClaimRequest,
        clock: ClockObservation,
    ) -> DurableResult<ContinuationExecutionClaim> {
        let step_read = self.read_execution_step(run_id)?;
        let source = step_read.run.continuation.clone();
        let active = source.execution_claim.clone().ok_or_else(|| {
            DurableError::IllegalTransition(format!("Run {run_id} has no active execution claim"))
        })?;
        if source.status != ContinuationStatus::Running
            || source.execution_fence != expected_fence
            || active.fence != expected_fence
            || step_read.run.run.active_attempt_id.as_deref()
                != Some(active.continuation_attempt_id.as_str())
        {
            return Err(DurableError::Conflict {
                expected: Some(expected_fence.to_string()),
                current: Some(source.execution_fence.to_string()),
            });
        }
        validate_clock_receipt(run_id, execution, &clock)?;
        if execution.clock.source_id != active.clock_observation_ref.source_id
            || execution.clock.source_generation != active.clock_observation_ref.source_generation
            || execution.clock.scope != active.clock_observation_ref.scope
            || clock.logical_time < active.logical_expires_at
        {
            return Err(DurableError::Busy {
                run_id: run_id.to_owned(),
                owner: active.owner.clone(),
                fence: active.fence,
            });
        }
        let epoch = source
            .epoch
            .checked_add(1)
            .filter(|value| *value <= MAX_EXACT_INTEGER)
            .ok_or_else(|| DurableError::Validation("Continuation epoch overflowed".to_owned()))?;
        let fence = source
            .execution_fence
            .checked_add(1)
            .filter(|value| *value <= MAX_EXACT_INTEGER)
            .ok_or_else(|| DurableError::Validation("execution fence overflowed".to_owned()))?;
        let continuation_attempt_id = continuation_attempt_id(run_id, epoch, fence)?;
        let claim = derive_execution_claim(
            &source,
            execution,
            &clock,
            fence,
            continuation_attempt_id.clone(),
        )?;
        let superseded = self.read_takeover_component_attempt(&step_read, &active)?;
        let commands = derive_continuation_claim_batch(&step_read.run.run, &source, &claim, epoch)?;
        let mut target = source;
        target.epoch = epoch;
        target.execution_fence = fence;
        target.execution_claim = Some(claim.clone());
        target.status = ContinuationStatus::Running;
        target.verify_wire()?;
        let retained_target = target;
        let retained_clock = clock;
        let old_clock_id = active.clock_observation_ref.observation_id.clone();
        let retained_attempt_id = claim.continuation_attempt_id.clone();
        let commit = self.commit_pinned_command_batch(commands, None, move |transition| {
            let run = transition
                .steps
                .last()
                .and_then(|step| step.run.as_ref())
                .ok_or_else(|| DurableError::Integrity {
                    code: "takeover_core_current_missing".to_owned(),
                    message: "takeover batch has no final Run current".to_owned(),
                })?;
            if run.result_current.active_attempt_id.as_deref() != Some(retained_attempt_id.as_str())
            {
                return Err(DurableError::Integrity {
                    code: "takeover_core_attempt_mismatch".to_owned(),
                    message: "takeover batch did not retain its new Core Attempt".to_owned(),
                });
            }
            let mut operations = vec![
                DurableOperation::PutClockObservation {
                    value: retained_clock,
                },
                DurableOperation::RemoveClockObservation {
                    observation_id: old_clock_id,
                },
                DurableOperation::PutContinuation {
                    value: retained_target.clone(),
                },
                DurableOperation::PutRunCurrent {
                    value: pinned_durable_run_current(&run.result_current, &retained_target)?,
                },
            ];
            if let Some(attempt) = superseded {
                operations.push(DurableOperation::PutOperationAttempt { value: attempt });
            }
            Ok(operations)
        })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        Ok(claim)
    }

    fn read_takeover_component_attempt(
        &mut self,
        step_read: &ExecutorStepRead,
        active: &ContinuationExecutionClaim,
    ) -> DurableResult<Option<OperationAttempt>> {
        Ok(current_component_attempt_for_takeover(step_read)?
            .map(|occurrence_id| {
                self.read_current_state_root(|manifest, resolver| {
                    crate::state_root::pinned_machine::PinnedMachineView::open(
                        manifest, resolver,
                    )?
                    .component_attempt_frontier(&occurrence_id)
                })
            })
            .transpose()?
            .flatten()
            .map(|frontier| {
                if frontier.latest_attempt.state == crate::OperationAttemptState::Superseded
                    && frontier.latest_attempt.execution_claim_fence < active.fence
                {
                    return Ok(None);
                }
                if frontier.latest_attempt.state != crate::OperationAttemptState::Running
                    || frontier.latest_attempt.execution_claim_owner != active.owner
                    || frontier.latest_attempt.execution_claim_fence != active.fence
                    || frontier.latest_attempt.continuation_attempt_id
                        != active.continuation_attempt_id
                {
                    return Err(DurableError::Integrity {
                        code: "takeover_component_attempt_mismatch".to_owned(),
                        message: "takeover component Attempt is not owned by the superseded execution claim"
                            .to_owned(),
                    });
                }
                let mut attempt = frontier.latest_attempt;
                attempt.state = crate::OperationAttemptState::Superseded;
                attempt.verify()?;
                Ok(Some(attempt))
            })
            .transpose()?
            .flatten())
    }

    pub(crate) fn commit_executor_boundary(
        &mut self,
        claim: &ContinuationExecutionClaim,
        expected_revision: &str,
        source: &Continuation,
        boundary: &crate::executor::ExecutorCoreBoundary,
    ) -> DurableResult<()> {
        let read = self.read_execution_step(&claim.run_id)?;
        if read.run.revision != expected_revision
            || read.run.continuation != *source
            || source.status != ContinuationStatus::Running
            || source.execution_claim.as_ref() != Some(claim)
            || read.run.run.active_attempt_id.as_deref()
                != Some(claim.continuation_attempt_id.as_str())
        {
            return Err(DurableError::Conflict {
                expected: Some(expected_revision.to_owned()),
                current: Some(read.run.revision),
            });
        }
        claim.verify_wire(source)?;
        let settled_effect = match boundary {
            crate::executor::ExecutorCoreBoundary::AdvanceSettledEffect { intent_id, .. }
            | crate::executor::ExecutorCoreBoundary::YieldReady {
                reason: crate::executor::ExecutorYieldReadyReason::EffectBoundary { intent_id },
            } => self.read_effect_execution(&claim.run_id, intent_id, expected_revision)?,
            _ => None,
        };
        let ready_boundary = match boundary {
            crate::executor::ExecutorCoreBoundary::YieldReady {
                reason: crate::executor::ExecutorYieldReadyReason::ReleaseBoundary { .. },
            } => Some(self.read_ready_boundary(&claim.run_id)?),
            _ => None,
        };
        let derived = derive_executor_boundary(
            &read,
            boundary,
            settled_effect.as_ref(),
            ready_boundary.as_ref(),
        )?;
        derived.next.verify_wire()?;
        match derived.action {
            DerivedExecutorBoundaryAction::Projection { artifacts } => {
                self.commit_executor_projection(&read.run, claim, &derived.next, artifacts)?;
            }
            DerivedExecutorBoundaryAction::OpenScope { command } => {
                self.commit_executor_single_command(
                    &read.run,
                    claim,
                    DerivedCommandOperation::OpenScope,
                    command,
                    &derived.next,
                )?;
            }
            DerivedExecutorBoundaryAction::CommitScope { command, result } => {
                self.commit_executor_scope(&read.run, claim, command, result, &derived.next)?;
            }
            DerivedExecutorBoundaryAction::CommitRootScope { command } => {
                self.commit_executor_single_command(
                    &read.run,
                    claim,
                    DerivedCommandOperation::CommitRootScope,
                    command,
                    &derived.next,
                )?;
            }
            DerivedExecutorBoundaryAction::Yield { wait } => {
                self.commit_yield_boundary(&read.run, claim, derived.next.clone(), wait)?;
            }
            DerivedExecutorBoundaryAction::Complete { result } => {
                self.commit_complete_boundary(&read.run, claim, derived.next.clone(), result)?;
            }
        }
        let observed =
            self.read_executor_run(&claim.run_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "executor_boundary_run_missing".to_owned(),
                    message: format!("Run {} disappeared after its boundary", claim.run_id),
                })?;
        if observed.continuation != derived.next {
            return Err(DurableError::HistoryConflict {
                code: "executor_boundary_replay_mismatch".to_owned(),
                message: format!(
                    "Run {} boundary did not retain its exact derived Continuation",
                    claim.run_id
                ),
            });
        }
        Ok(())
    }

    /// Current revision.
    pub(crate) fn revision(&self) -> Option<&str> {
        self.pinned.as_ref().map(PinnedHead::revision)
    }

    /// Resolve one closed revision/root-pinned ordinary query without
    /// materializing the aggregate durable projection.
    pub(crate) fn query(
        &mut self,
        command: &crate::DurableCommand,
    ) -> DurableResult<crate::DurableResponse> {
        command.verify()?;
        if !command.is_read_only() {
            return Err(DurableError::Validation(
                "durable query authority accepts only read-only commands".to_owned(),
            ));
        }
        let response = self.read_current_state_root(|manifest, resolver| {
            query_command_at_manifest(manifest, resolver, command)
        })?;
        response.verify_query_for(command)?;
        Ok(response)
    }

    /// Explicit offline maintenance over one exact pinned Machine source.
    /// Retained receipt replay never materializes the current source.
    pub(crate) fn compact_machine_history(
        &mut self,
        request: &crate::HistoryCompactionRequest,
    ) -> DurableResult<crate::HistoryCompactionReceipt> {
        request.verify()?;
        let existing = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_history_compaction_receipt(
                manifest,
                resolver,
                &request.compaction_id,
            )
        })?;
        if let Some(receipt) = existing {
            if history_compaction_matches(request, &receipt) {
                return Ok(receipt);
            }
            return Err(DurableError::HistoryConflict {
                code: "history_compaction_request_reused".to_owned(),
                message: format!(
                    "history compaction {} was reused with different request semantics",
                    request.compaction_id
                ),
            });
        }

        let stage = self.read_current_state_root(|manifest, resolver| {
            require_exact_query_revision(manifest, &request.expected_revision)?;
            crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                .prepare_history_compaction(request)
        })?;
        let receipt =
            stage
                .compaction_receipt()
                .cloned()
                .ok_or_else(|| DurableError::RuntimeDefect {
                    code: "history_compaction_stage_receipt_missing".to_owned(),
                    message: "prepared history compaction has no exact typed receipt".to_owned(),
                })?;
        receipt.verify()?;
        if !history_compaction_matches(request, &receipt) {
            return Err(DurableError::Integrity {
                code: "history_compaction_preparation_mismatch".to_owned(),
                message: "prepared history compaction changed its exact source or intent"
                    .to_owned(),
            });
        }
        let sidecar = DurableDelta::new(vec![DurableOperation::PutHistoryCompaction {
            value: receipt.clone(),
        }])?;
        self.publish_pinned_stage(stage, Some(sidecar))?;
        // The verified CAS acknowledgement owns this immutable historical receipt.
        // A later writer may advance the head without invalidating that success.
        Ok(receipt)
    }

    /// Idempotently complete deletion authorized by the current head-pinned
    /// physical reclamation receipt.
    pub(crate) fn reconcile_cold_reclamation(&mut self) -> DurableResult<GcReceipt> {
        let expected = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .head
            .clone();
        let request = StoreReclamation::new(&expected)?;
        let receipt = self.store.reconcile_cold_reclamation(&request)?;
        receipt.verify_for(&expected)?;
        Ok(receipt)
    }

    /// Publish and reconcile the next bounded physical reclamation generation.
    pub(crate) fn advance_cold_reclamation(&mut self) -> DurableResult<GcReceipt> {
        let before = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .clone();
        let request = StoreReclamation::new(&before.head)?;
        let receipt = self.store.advance_cold_reclamation(&request)?;
        let acknowledged = receipt
            .successor_head(&before.head)
            .and_then(|head| PinnedHead::new(head, before.manifest))
            .map_err(|error| DurableError::CommitOutcomeUnknown {
                message: format!(
                    "GC acknowledgement does not bind the requested physical successor: {error}"
                ),
            })?;
        self.pinned = Some(acknowledged);
        Ok(receipt)
    }

    /// Admit one provider-independent semantic cancellation and atomically
    /// close every M1 execution surface owned by the Run.
    pub(crate) fn cancel_run(
        &mut self,
        run_id: &str,
        cancellation_id: &str,
        reason: &serde_json::Value,
    ) -> DurableResult<CancellationReceipt> {
        let command = CancellationCommand {
            cancellation_id: cancellation_id.to_owned(),
            run_id: run_id.to_owned(),
            reason: reason.clone(),
        };
        command.verify()?;
        if let Some(receipt) = self.cancellation_receipt(&command.cancellation_id)? {
            if receipt.command_matches(&command) {
                return Ok(receipt);
            }
            return Err(DurableError::HistoryConflict {
                code: "run_cancellation_command_reused".to_owned(),
                message: format!(
                    "Run cancellation identity {} was reused with different command semantics",
                    command.cancellation_id
                ),
            });
        }
        let read = self.read_cancellable_run(&command.run_id)?;

        let bytes = cymule_core::canonical_bytes(&command.reason)?;
        let reason = cymule_core::ArtifactRecord {
            reference: cymule_core::artifact_ref(CANCELLATION_REASON_ARTIFACT_KIND, &bytes)?,
            bytes,
        };
        let receipt = CancellationReceipt::new(command.clone(), reason.reference.clone())?;
        let terminal = self.read_terminal_sidecars(&read, None)?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            command.cancellation_id.clone(),
            Vec::new(),
            vec![reason.clone()],
        )?;
        let member = cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id: command.cancellation_id.clone(),
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: command.run_id.clone(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                Some(read.run.precondition_token()),
            ),
            command: Command::CancelRun {
                reason: reason.reference,
            },
        };
        let mut next = read.continuation.clone();
        next.epoch = checked_next("cancelled Run epoch", next.epoch)?;
        next.execution_fence = checked_next("cancelled Run fence", next.execution_fence)?;
        next.status = ContinuationStatus::Cancelled;
        next.execution_claim = None;
        next.wait_set.clear();
        next.verify_wire()?;
        let retained_receipt = receipt.clone();
        let clock_id = read
            .continuation
            .execution_claim
            .as_ref()
            .map(|claim| claim.clock_observation_ref.observation_id.clone());
        let commit =
            self.commit_pinned_command_batch(vec![member], Some(material), move |transition| {
                let run = pinned_batch_final_run(transition)?;
                let mut operations = terminal.finish()?;
                operations.extend([
                    DurableOperation::PutContinuation {
                        value: next.clone(),
                    },
                    DurableOperation::PutRunCurrent {
                        value: pinned_durable_run_current(&run.result_current, &next)?,
                    },
                    DurableOperation::PutCancellationReceipt {
                        value: retained_receipt,
                    },
                ]);
                if let Some(observation_id) = clock_id {
                    operations.push(DurableOperation::RemoveClockObservation { observation_id });
                }
                Ok(operations)
            })?;
        for receipt in commit.receipts {
            require_applied_command_receipt(receipt)?;
        }
        if commit.committed_revision.is_none() {
            return Err(DurableError::HistoryConflict {
                code: "run_cancellation_sidecar_missing".to_owned(),
                message: "Run cancellation Core command exists without its immutable receipt"
                    .to_owned(),
            });
        }
        Ok(receipt)
    }

    fn read_cancellable_run(&mut self, run_id: &str) -> DurableResult<ExecutorRunRead> {
        let read = self
            .read_executor_run(run_id)?
            .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} is missing")))?;
        match &read.run.execution_status {
            cymule_core::RunExecutionStatus::Cancelled { .. } => {
                return Err(DurableError::IllegalTransition(format!(
                    "Run {run_id} is already cancelled by a different semantic request"
                )));
            }
            cymule_core::RunExecutionStatus::Completed
            | cymule_core::RunExecutionStatus::Failed { .. } => {
                return Err(DurableError::IllegalTransition(format!(
                    "Run {run_id} execution is already terminal"
                )));
            }
            cymule_core::RunExecutionStatus::Active => {}
        }
        Ok(read)
    }

    /// Admit one provider-independent activation and return its complete stable
    /// selected/applied/Ready receipt for control and transport acknowledgement.
    pub(crate) fn admit_wait_activation_receipt(
        &mut self,
        activation_id: impl Into<String>,
        source: WaitActivationSource,
        wait_ids: BTreeSet<String>,
        value: &serde_json::Value,
    ) -> DurableResult<WaitActivationReceipt> {
        self.admit_wait_activation_receipt_pinned(activation_id, source, wait_ids, value)
    }

    /// Commit one provider-independent Agent transition against exact keyed
    /// currents. Input, workspace, and external stream finalization use their
    /// dedicated coupled capabilities and are deliberately not accepted here.
    pub(crate) fn commit_agent_local(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> DurableResult<agent_protocol::AgentCommit> {
        command.verify()?;
        ensure_agent_local_command(command)?;
        if let Some(receipt) = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_agent_command_receipt(manifest, resolver, &command.command_id)
        })? {
            let retained = self
                .read_current_state_root(|manifest, resolver| {
                    crate::state_root::load_agent_command(manifest, resolver, &command.command_id)
                })?
                .ok_or_else(|| DurableError::Integrity {
                    code: "agent_command_receipt_without_command".to_owned(),
                    message: format!(
                        "Agent receipt {} lost command {}",
                        receipt.receipt_id, command.command_id
                    ),
                })?;
            if retained != *command {
                return Err(DurableError::HistoryConflict {
                    code: "agent_command_reused".to_owned(),
                    message: format!(
                        "Agent command {} was reused with different semantics",
                        command.command_id
                    ),
                });
            }
            receipt.verify_for(command)?;
            let commit = agent_protocol::AgentCommit {
                observed_revision: self.current_revision()?.to_owned(),
                committed_revision: None,
                receipt,
            };
            commit.verify_for(command)?;
            return Ok(commit);
        }
        if self.current_revision()? != command.source_revision {
            return Err(DurableError::Conflict {
                expected: Some(command.source_revision.clone()),
                current: Some(self.current_revision()?.to_owned()),
            });
        }
        self.verify_agent_workspace_source_session(command)?;
        let (receipt, operations) = self.read_current_state_root(|manifest, resolver| {
            prepare_agent_local_transition(manifest, resolver, command)
        })?;
        let observed_revision = self.commit_profile_operations(operations)?;
        let commit = agent_protocol::AgentCommit {
            observed_revision: observed_revision.clone(),
            committed_revision: Some(observed_revision),
            receipt,
        };
        commit.verify_for(command)?;
        Ok(commit)
    }

    /// Finalize one exact Agent stream through its retained delivery authority.
    ///
    /// Staged streams reduce locally. External streams invoke only the resolver
    /// binding retained by the stream current, then reload the derived Resource
    /// family and pin keys from the same pinned `StateRoot` before the single CAS.
    pub(crate) fn finalize_agent_stream(
        &mut self,
        command: &agent_protocol::AgentCommand,
        providers: &mut dyn agent_protocol::AgentProviders,
    ) -> DurableResult<agent_protocol::AgentStreamFinalizeOutcome> {
        command.verify()?;
        let agent_protocol::AgentCommandAction::Stream(
            stream_command @ agent_protocol::AgentStreamCommand::Finalize { .. },
        ) = &command.action
        else {
            return Err(DurableError::Validation(
                "Agent stream-finalization control accepts only Finalize".to_owned(),
            ));
        };
        if let Some(commit) = self.replay_agent_stream_finalization(command)? {
            return Ok(agent_protocol::AgentStreamFinalizeOutcome::Committed {
                commit: Box::new(commit),
            });
        }
        if self.current_revision()? != command.source_revision {
            return Err(DurableError::Conflict {
                expected: Some(command.source_revision.clone()),
                current: Some(self.current_revision()?.to_owned()),
            });
        }

        self.verify_agent_workspace_source_session(command)?;
        let source = self.read_current_state_root(|manifest, resolver| {
            load_agent_stream_finalization_source(manifest, resolver, stream_command)
        })?;
        let external = matches!(
            &source,
            agent_protocol::AgentStreamSource::Finalize {
                stream: agent_protocol::AgentStreamCurrent {
                    delivery: agent_protocol::AgentStreamDelivery::ExternalResource { .. },
                    ..
                },
                ..
            }
        );
        if external {
            let result =
                agent_protocol::execute_agent_stream_publication(&source, command, providers)?;
            return Ok(self.finish_agent_stream_publication_result(command, source, result));
        }

        let postcondition = source.reduce(&command.command_id, stream_command)?;
        let commit =
            self.commit_agent_stream_postcondition(command, source, &postcondition, None)?;
        Ok(agent_protocol::AgentStreamFinalizeOutcome::Committed {
            commit: Box::new(commit),
        })
    }

    pub(crate) fn reconcile_agent_stream(
        &mut self,
        command: &agent_protocol::AgentCommand,
        expected_intent: &agent_protocol::AgentStreamPublicationIntent,
        providers: &mut dyn agent_protocol::AgentProviders,
    ) -> DurableResult<agent_protocol::AgentStreamFinalizeOutcome> {
        command.verify()?;
        expected_intent.verify()?;
        let agent_protocol::AgentCommandAction::Stream(
            stream_command @ agent_protocol::AgentStreamCommand::Finalize { .. },
        ) = &command.action
        else {
            return Err(DurableError::Validation(
                "Agent stream reconciliation accepts only Finalize".to_owned(),
            ));
        };
        if let Some(commit) = self.replay_agent_stream_finalization(command)? {
            return Ok(agent_protocol::AgentStreamFinalizeOutcome::Committed {
                commit: Box::new(commit),
            });
        }
        if expected_intent.source_revision() != command.source_revision
            || expected_intent.command_id() != command.command_id
        {
            return Err(DurableError::Validation(
                "Agent stream reconciliation intent does not belong to the exact Finalize command"
                    .to_owned(),
            ));
        }
        self.verify_agent_workspace_source_session(command)?;
        let source = self.read_current_state_root(|manifest, resolver| {
            load_agent_stream_finalization_source(manifest, resolver, stream_command)
        })?;
        if !matches!(
            &source,
            agent_protocol::AgentStreamSource::Finalize {
                stream: agent_protocol::AgentStreamCurrent {
                    delivery: agent_protocol::AgentStreamDelivery::ExternalResource { .. },
                    ..
                },
                ..
            }
        ) {
            return Err(DurableError::Validation(
                "staged Agent stream finalization has no external publication to reconcile"
                    .to_owned(),
            ));
        }
        let result = match agent_protocol::reconcile_agent_stream_publication(
            &source,
            command,
            expected_intent,
            providers,
        ) {
            Ok(result) => result,
            Err(cymule_profile_protocol::ProtocolError::Conflict { .. }) => {
                return Ok(
                    agent_protocol::AgentStreamFinalizeOutcome::PublicationOutcomeUnknown {
                        intent: expected_intent.clone(),
                    },
                );
            }
            Err(error) => return Err(error.into()),
        };
        Ok(self.finish_agent_stream_publication_result(command, source, result))
    }

    fn replay_agent_stream_finalization(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> DurableResult<Option<agent_protocol::AgentCommit>> {
        let Some(receipt) = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_agent_command_receipt(manifest, resolver, &command.command_id)
        })?
        else {
            return Ok(None);
        };
        let retained = self
            .read_current_state_root(|manifest, resolver| {
                crate::state_root::load_agent_command(manifest, resolver, &command.command_id)
            })?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_finalize_receipt_without_command".to_owned(),
                message: format!(
                    "Agent stream receipt {} lost command {}",
                    receipt.receipt_id, command.command_id
                ),
            })?;
        if retained != *command {
            return Err(DurableError::HistoryConflict {
                code: "agent_finalize_command_reused".to_owned(),
                message: format!(
                    "Agent command {} was reused with different finalization semantics",
                    command.command_id
                ),
            });
        }
        receipt.verify_for(command)?;
        self.read_current_state_root(|manifest, resolver| {
            verify_agent_stream_finalization_graph(manifest, resolver, command, &receipt)
        })?;
        let commit = agent_protocol::AgentCommit {
            observed_revision: self.current_revision()?.to_owned(),
            committed_revision: None,
            receipt,
        };
        commit.verify_for(command)?;
        Ok(Some(commit))
    }

    fn finish_agent_stream_publication_result(
        &mut self,
        command: &agent_protocol::AgentCommand,
        source: agent_protocol::AgentStreamSource,
        result: agent_protocol::AgentStreamPublicationResult,
    ) -> agent_protocol::AgentStreamFinalizeOutcome {
        match result {
            agent_protocol::AgentStreamPublicationResult::NotApplied { intent } => {
                agent_protocol::AgentStreamFinalizeOutcome::PublicationNotApplied { intent }
            }
            agent_protocol::AgentStreamPublicationResult::Unknown { intent } => {
                agent_protocol::AgentStreamFinalizeOutcome::PublicationOutcomeUnknown { intent }
            }
            agent_protocol::AgentStreamPublicationResult::Published { product } => {
                let intent = product.intent().clone();
                let committed = (|| {
                    let profile_pin = product.resource_profile_pin();
                    let resource_source = self.read_current_state_root(|manifest, resolver| {
                        let retention = crate::state_root::load_resource_retention_current(
                            manifest,
                            resolver,
                            &profile_pin.pin.subject.family.retention_key,
                        )?;
                        if let Some(current) = &retention {
                            verify_resource_retention_origin(manifest, resolver, current)?;
                        }
                        let pin = crate::state_root::load_resource_pin_current(
                            manifest,
                            resolver,
                            &profile_pin.pin.pin_id,
                        )?;
                        if let Some(current) = &pin {
                            verify_resource_pin_origin(manifest, resolver, current)?;
                        }
                        Ok(agent_protocol::AgentStreamResourceSource { retention, pin })
                    })?;
                    let source =
                        attach_agent_stream_resource_source(source, resource_source.clone())?;
                    let postcondition = source.reduce_with_publication(command, &product)?;
                    self.commit_agent_stream_postcondition(
                        command,
                        source,
                        &postcondition,
                        Some(resource_source),
                    )
                })();
                match committed {
                    Ok(commit) => agent_protocol::AgentStreamFinalizeOutcome::Committed {
                        commit: Box::new(commit),
                    },
                    Err(_) => {
                        agent_protocol::AgentStreamFinalizeOutcome::PublicationOutcomeUnknown {
                            intent,
                        }
                    }
                }
            }
        }
    }

    fn commit_agent_stream_postcondition(
        &mut self,
        command: &agent_protocol::AgentCommand,
        source: agent_protocol::AgentStreamSource,
        postcondition: &agent_protocol::AgentStreamPostcondition,
        resource_source: Option<agent_protocol::AgentStreamResourceSource>,
    ) -> DurableResult<agent_protocol::AgentCommit> {
        let receipt = agent_protocol::AgentCommandReceipt::new(
            command,
            agent_protocol::AgentCommandSource::Stream(Box::new(source)),
            agent_protocol::AgentCommandOutcome::Stream(postcondition.clone()),
        )?;
        let mut operations = agent_stream_finalization_operations(postcondition)?;
        if let Some(resource_source) = resource_source {
            let pin_receipt = receipt.resource_pin_receipt_for(command)?.ok_or_else(|| {
                DurableError::Integrity {
                    code: "agent_finalize_resource_pin_receipt_missing".to_owned(),
                    message: "external Agent finalization lost its Resource pin receipt".to_owned(),
                }
            })?;
            let origin =
                resource_protocol::ResourceLifecycleReceiptRef::from_agent(command, &receipt)?;
            let resource_post = resource_protocol::project_resource_pin_receipt(
                pin_receipt,
                origin,
                resource_source.retention.as_ref(),
                resource_source.pin.as_ref(),
            )?;
            operations.push(DurableOperation::PutResourceRetentionCurrent {
                value: resource_post.retention,
            });
            operations.push(DurableOperation::PutResourcePinCurrent {
                value: resource_post.pin,
            });
        }
        operations.push(DurableOperation::PutAgentCommand {
            value: command.clone(),
        });
        operations.push(DurableOperation::PutAgentCommandReceipt {
            value: Box::new(receipt.clone()),
        });
        let observed_revision = self.commit_profile_operations(operations)?;
        let commit = agent_protocol::AgentCommit {
            observed_revision: observed_revision.clone(),
            committed_revision: Some(observed_revision),
            receipt,
        };
        commit.verify_for(command)?;
        Ok(commit)
    }

    pub(crate) fn commit_agent_input(
        &mut self,
        command: &agent_protocol::AgentCommand,
    ) -> DurableResult<agent_protocol::AgentCommit> {
        command.verify()?;
        let agent_protocol::AgentCommandAction::Input(input) = &command.action else {
            return Err(DurableError::Validation(
                "Agent input control accepts only one typed input command".to_owned(),
            ));
        };
        if let Some(receipt) = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_agent_command_receipt(manifest, resolver, &command.command_id)
        })? {
            let retained = self
                .read_current_state_root(|manifest, resolver| {
                    crate::state_root::load_agent_command(manifest, resolver, &command.command_id)
                })?
                .ok_or_else(|| DurableError::Integrity {
                    code: "agent_input_receipt_without_command".to_owned(),
                    message: format!(
                        "Agent input receipt {} lost command {}",
                        receipt.receipt_id, command.command_id
                    ),
                })?;
            if retained != *command {
                return Err(DurableError::HistoryConflict {
                    code: "agent_input_command_reused".to_owned(),
                    message: format!(
                        "Agent input command {} was reused with different semantics",
                        command.command_id
                    ),
                });
            }
            self.read_current_state_root(|manifest, resolver| {
                verify_agent_input_receipt_graph(manifest, resolver, command, &receipt)
            })?;
            let commit = agent_protocol::AgentCommit {
                observed_revision: self.current_revision()?.to_owned(),
                committed_revision: None,
                receipt,
            };
            commit.verify_for(command)?;
            return Ok(commit);
        }
        if self.current_revision()? != command.source_revision {
            return Err(DurableError::Conflict {
                expected: Some(command.source_revision.clone()),
                current: Some(self.current_revision()?.to_owned()),
            });
        }
        self.verify_agent_workspace_source_session(command)?;
        let (receipt, observed_revision) = match input {
            agent_protocol::AgentInputCommand::Suspend { .. } => {
                let (receipt, operations) =
                    self.read_current_state_root(|manifest, resolver| {
                        prepare_agent_input_suspension(manifest, resolver, command, input)
                    })?;
                let observed_revision = self.commit_profile_operations(operations)?;
                (receipt, observed_revision)
            }
            agent_protocol::AgentInputCommand::Complete { .. } => {
                self.commit_agent_input_completion(command, input)?
            }
        };
        let commit = agent_protocol::AgentCommit {
            observed_revision: observed_revision.clone(),
            committed_revision: Some(observed_revision),
            receipt,
        };
        commit.verify_for(command)?;
        Ok(commit)
    }

    fn commit_agent_input_completion(
        &mut self,
        command: &agent_protocol::AgentCommand,
        input: &agent_protocol::AgentInputCommand,
    ) -> DurableResult<(agent_protocol::AgentCommandReceipt, String)> {
        let plan = self.read_current_state_root(|manifest, resolver| {
            prepare_agent_input_completion(manifest, resolver, command, input)
        })?;
        let receipt = plan.agent_receipt.clone();
        let run = self
            .read_executor_run(&plan.source_continuation.run_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_input_run_missing".to_owned(),
                message: "Agent input completion lost its exact Run".to_owned(),
            })?;
        if run.continuation != plan.source_continuation || run.revision != command.source_revision {
            return Err(DurableError::Conflict {
                expected: Some(command.source_revision.clone()),
                current: Some(run.revision),
            });
        }
        let artifact = cymule_core::ArtifactRecord {
            reference: plan.result,
            bytes: plan.result_bytes,
        };
        artifact.validate()?;
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            command.command_id.clone(),
            Vec::new(),
            vec![artifact],
        )?;
        let mut operations = plan.operations;
        operations.push(DurableOperation::PutWait {
            value: plan.completed_wait,
        });
        operations.push(DurableOperation::PutRunCurrent {
            value: pinned_durable_run_current(&run.run, &plan.completed_continuation)?,
        });
        operations.push(DurableOperation::PutContinuation {
            value: plan.completed_continuation,
        });
        let observed_revision =
            self.commit_material_sidecars(&material, &receipt.receipt_id, operations)?;
        Ok((receipt, observed_revision))
    }

    pub(crate) fn read_executor_run(
        &mut self,
        run_id: &str,
    ) -> DurableResult<Option<ExecutorRunRead>> {
        self.read_current_state_root(|manifest, resolver| {
            load_executor_run_at_manifest(manifest, resolver, run_id)
        })
    }

    pub(crate) fn read_artifact(
        &mut self,
        reference: &ArtifactRef,
        expected_revision: &str,
    ) -> DurableResult<crate::DurableExactRead<cymule_core::ArtifactRecord>> {
        reference.validate()?;
        self.read_current_state_root(|manifest, resolver| {
            require_exact_query_revision(manifest, expected_revision)?;
            let value =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                    .artifact(&reference.artifact_id)?;
            if value
                .as_ref()
                .is_some_and(|record| record.reference != *reference)
            {
                return Err(DurableError::Integrity {
                    code: "artifact_exact_reference_mismatch".to_owned(),
                    message: "Artifact exact read changed the requested type or identity"
                        .to_owned(),
                });
            }
            Ok(crate::DurableExactRead {
                observed_revision: manifest.revision().to_owned(),
                value,
            })
        })
    }

    fn read_virtual_exact_leaf(
        &mut self,
        scheduler_id: &str,
        semantic_id: &str,
        expected_revision: &str,
        family: virtual_protocol::VirtualStateFamily,
    ) -> DurableResult<crate::DurableExactRead<virtual_protocol::VirtualStateLeaf>> {
        let storage_key = match family {
            virtual_protocol::VirtualStateFamily::Regions => {
                virtual_protocol::virtual_region_key(scheduler_id, semantic_id)?
            }
            virtual_protocol::VirtualStateFamily::Work => {
                virtual_protocol::virtual_work_key(scheduler_id, semantic_id)?
            }
            virtual_protocol::VirtualStateFamily::Occurrences => {
                virtual_protocol::virtual_occurrence_key(scheduler_id, semantic_id)?
            }
            virtual_protocol::VirtualStateFamily::Runs => {
                virtual_protocol::virtual_run_key(scheduler_id, semantic_id)?
            }
            _ => {
                return Err(DurableError::Validation(
                    "exact Virtual query selected a non-query family".to_owned(),
                ));
            }
        };
        self.read_current_state_root(|manifest, resolver| {
            require_exact_query_revision(manifest, expected_revision)?;
            let value = crate::state_root::load_virtual_leaf(
                manifest,
                resolver,
                scheduler_id,
                family,
                &storage_key,
            )?;
            Ok(crate::DurableExactRead {
                observed_revision: manifest.revision().to_owned(),
                value,
            })
        })
    }

    pub(crate) fn read_virtual_region(
        &mut self,
        scheduler_id: &str,
        region_id: &str,
        expected_revision: &str,
    ) -> DurableResult<crate::DurableExactRead<virtual_protocol::VirtualRegionCurrent>> {
        let read = self.read_virtual_exact_leaf(
            scheduler_id,
            region_id,
            expected_revision,
            virtual_protocol::VirtualStateFamily::Regions,
        )?;
        let value = read
            .value
            .map(|leaf| match leaf {
                virtual_protocol::VirtualStateLeaf::Regions(value) => Ok(value),
                _ => Err(exact_leaf_kind_mismatch("Virtual region")),
            })
            .transpose()?;
        Ok(crate::DurableExactRead {
            observed_revision: read.observed_revision,
            value,
        })
    }

    pub(crate) fn read_virtual_work(
        &mut self,
        scheduler_id: &str,
        work_id: &str,
        expected_revision: &str,
    ) -> DurableResult<crate::DurableExactRead<virtual_protocol::VirtualWorkCurrent>> {
        let read = self.read_virtual_exact_leaf(
            scheduler_id,
            work_id,
            expected_revision,
            virtual_protocol::VirtualStateFamily::Work,
        )?;
        let value = read
            .value
            .map(|leaf| match leaf {
                virtual_protocol::VirtualStateLeaf::Work(value) => Ok(value),
                _ => Err(exact_leaf_kind_mismatch("Virtual work")),
            })
            .transpose()?;
        Ok(crate::DurableExactRead {
            observed_revision: read.observed_revision,
            value,
        })
    }

    pub(crate) fn read_virtual_occurrence(
        &mut self,
        scheduler_id: &str,
        occurrence_id: &str,
        expected_revision: &str,
    ) -> DurableResult<crate::DurableExactRead<virtual_protocol::VirtualOccurrenceCurrent>> {
        let read = self.read_virtual_exact_leaf(
            scheduler_id,
            occurrence_id,
            expected_revision,
            virtual_protocol::VirtualStateFamily::Occurrences,
        )?;
        let value = read
            .value
            .map(|leaf| match leaf {
                virtual_protocol::VirtualStateLeaf::Occurrences(value) => Ok(*value),
                _ => Err(exact_leaf_kind_mismatch("Virtual occurrence")),
            })
            .transpose()?;
        Ok(crate::DurableExactRead {
            observed_revision: read.observed_revision,
            value,
        })
    }

    pub(crate) fn read_virtual_run(
        &mut self,
        scheduler_id: &str,
        run_id: &str,
        expected_revision: &str,
    ) -> DurableResult<crate::DurableExactRead<virtual_protocol::VirtualRunCurrent>> {
        let read = self.read_virtual_exact_leaf(
            scheduler_id,
            run_id,
            expected_revision,
            virtual_protocol::VirtualStateFamily::Runs,
        )?;
        let value = read
            .value
            .map(|leaf| match leaf {
                virtual_protocol::VirtualStateLeaf::Runs(value) => Ok(value),
                _ => Err(exact_leaf_kind_mismatch("Virtual Run")),
            })
            .transpose()?;
        Ok(crate::DurableExactRead {
            observed_revision: read.observed_revision,
            value,
        })
    }

    pub(crate) fn read_evolution_template_plan_id(
        &mut self,
        evolution_id: &str,
        template_id: &str,
        expected_revision: &str,
    ) -> DurableResult<crate::DurableExactRead<String>> {
        let family = evolution_protocol::EvolutionStateFamily::TemplateCurrent;
        let storage_key =
            evolution_protocol::evolution_state_key(family, evolution_id, template_id, "current")?;
        self.read_current_state_root(|manifest, resolver| {
            require_exact_query_revision(manifest, expected_revision)?;
            let value = crate::state_root::load_evolution_mutation(
                manifest,
                resolver,
                family,
                &storage_key,
            )?
            .map(|leaf| match leaf {
                evolution_protocol::EvolutionMutation::TemplateCurrent(value) => {
                    Ok(value.linked_plan_id().to_owned())
                }
                _ => Err(exact_leaf_kind_mismatch("Evolution template")),
            })
            .transpose()?;
            Ok(crate::DurableExactRead {
                observed_revision: manifest.revision().to_owned(),
                value,
            })
        })
    }

    pub(crate) fn read_execution_step(&mut self, run_id: &str) -> DurableResult<ExecutorStepRead> {
        self.read_current_state_root(|manifest, resolver| {
            let run = load_executor_run_at_manifest(manifest, resolver, run_id)?
                .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} does not exist")))?;
            let frame = run.continuation.frames.last().ok_or_else(|| {
                DurableError::IllegalTransition(format!(
                    "Run {run_id} has no active execution frame"
                ))
            })?;
            let mut references = BTreeSet::from([frame.input.clone()]);
            references.extend(frame.locals.values().cloned());
            references.extend(run.continuation.state.iter().cloned());
            let mut view =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
            let scope = view
                .scope_current(&run.run, &frame.scope_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "executor_current_scope_missing".to_owned(),
                    message: format!(
                        "Run {run_id} active frame references missing Scope {}",
                        frame.scope_id
                    ),
                })?;
            cymule_core::durable_internal::validate_pinned_execution_frame(
                &run.plan,
                &cymule_core::ExecutionFrameLocation {
                    run_id,
                    plan_id: &run.plan.plan_id,
                    invocation_id: &frame.invocation_id,
                    invocation_path: &frame.invocation_path,
                    definition_id: &frame.definition_id,
                    region_path: &frame.region_path,
                    scope_id: &frame.scope_id,
                    next_step: frame.next_step,
                },
                &scope.current,
                &scope.invocation_path,
                &scope.region_path,
            )?;
            let mut referenced_artifacts = BTreeMap::new();
            let mut aggregate_bytes = 0_usize;
            for reference in references {
                let record = view.artifact(&reference.artifact_id)?.ok_or_else(|| {
                    DurableError::Integrity {
                        code: "executor_referenced_artifact_missing".to_owned(),
                        message: format!(
                            "Run {run_id} references missing Artifact {}",
                            reference.artifact_id
                        ),
                    }
                })?;
                if record.reference != reference {
                    return Err(DurableError::Integrity {
                        code: "executor_referenced_artifact_mismatch".to_owned(),
                        message: format!(
                            "Run {run_id} Artifact {} changed its exact reference",
                            reference.artifact_id
                        ),
                    });
                }
                aggregate_bytes = aggregate_bytes
                    .checked_add(cymule_core::canonical_bytes(&record)?.len())
                    .ok_or_else(|| {
                        DurableError::Validation(
                            "executor Artifact read-set byte accounting overflowed".to_owned(),
                        )
                    })?;
                if aggregate_bytes
                    > cymule_core::durable_internal::MAX_PINNED_MACHINE_READ_SET_BYTES
                {
                    return Err(DurableError::Validation(format!(
                        "executor Artifact read set exceeds {} canonical bytes",
                        cymule_core::durable_internal::MAX_PINNED_MACHINE_READ_SET_BYTES
                    )));
                }
                referenced_artifacts.insert(reference.artifact_id, record);
            }
            Ok(ExecutorStepRead {
                run,
                current_scope: scope.current,
                referenced_artifacts,
            })
        })
    }

    pub(crate) fn read_ready_boundary(
        &mut self,
        run_id: &str,
    ) -> DurableResult<ExecutorReadyBoundaryRead> {
        self.read_current_state_root(|manifest, resolver| {
            let material = load_executor_run_at_manifest(manifest, resolver, run_id)?
                .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} does not exist")))?;
            if !matches!(
                material.continuation.status,
                ContinuationStatus::Ready | ContinuationStatus::Running
            ) {
                return Err(DurableError::IllegalTransition(format!(
                    "Run {run_id} has no executable Effect boundary"
                )));
            }
            let root = crate::state_root::load_run_query_index_root(
                manifest,
                resolver,
                run_id,
                crate::state_root::RunQueryIndexKind::ActiveEffects,
            )?;
            let maximum = u64::try_from(
                cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES
                    * cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGES,
            )
            .map_err(|error| DurableError::Validation(error.to_string()))?;
            if root.entries > maximum {
                return Err(DurableError::Validation(format!(
                    "Run {run_id} active Effect boundary exceeds {maximum} exact entries"
                )));
            }
            let mut unknown_intent = None;
            let mut explicit_intents = BTreeSet::new();
            let mut position = None;
            loop {
                let page = crate::state_root::load_state_map_key_page(
                    &root,
                    position.as_ref(),
                    cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
                    crate::state_root::MAX_STATE_MAP_KEY_PAGE_BYTES,
                    resolver,
                )?;
                for entry in page.entries {
                    let dispatch: EffectDispatch = crate::state_root::load_typed_state_map_value(
                        &root,
                        &entry.key,
                        crate::StateRootLeafKind::Outbox,
                        resolver,
                    )?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "executor_active_effect_missing".to_owned(),
                        message: format!("Run {run_id} active Effect {} is missing", entry.key),
                    })?;
                    if dispatch.run_id != run_id || dispatch.intent_id != entry.key {
                        return Err(DurableError::Integrity {
                            code: "executor_active_effect_owner_mismatch".to_owned(),
                            message: format!(
                                "Run {run_id} active Effect {} changed owner or identity",
                                entry.key
                            ),
                        });
                    }
                    if dispatch.state == OutboxState::Unknown {
                        unknown_intent.get_or_insert(dispatch.intent_id.clone());
                        continue;
                    }
                    if dispatch.state != OutboxState::Pending {
                        continue;
                    }
                    if load_explicit_effect_release_ready(
                        manifest,
                        resolver,
                        &material.run,
                        &dispatch,
                    )? {
                        explicit_intents.insert(dispatch.intent_id);
                    }
                }
                let Some(next) = page.next_position else {
                    break;
                };
                position = Some(next);
            }
            Ok(ExecutorReadyBoundaryRead {
                revision: manifest.revision().to_owned(),
                unknown_intent,
                explicit_intents,
            })
        })
    }

    pub(crate) fn completed_execution_result(
        &mut self,
        run_id: &str,
    ) -> DurableResult<cymule_runtime::ExecutionResult> {
        self.read_current_state_root(|manifest, resolver| {
            let read = load_executor_run_at_manifest(manifest, resolver, run_id)?
                .ok_or_else(|| DurableError::NotFound(format!("Run {run_id} does not exist")))?;
            if read.continuation.status != ContinuationStatus::Completed
                || read.run.execution_status != RunExecutionStatus::Completed
            {
                return Err(DurableError::IllegalTransition(format!(
                    "Run {run_id} is not completed"
                )));
            }
            let value = read
                .terminal_result
                .as_ref()
                .map(|record| cymule_core::decode_json(&record.bytes))
                .transpose()?
                .unwrap_or(serde_json::Value::Null);
            PlanContracts::compile(&read.plan.candidate)?
                .validate_definition_output(&read.plan.candidate.entry, &value)?;
            let root = &read.run.children.effects;
            let maximum = u64::try_from(
                cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES
                    * cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGES,
            )
            .map_err(|error| DurableError::Validation(error.to_string()))?;
            if root.entries > maximum {
                return Err(DurableError::Validation(format!(
                    "Run {run_id} completed Effect set exceeds {maximum} exact entries"
                )));
            }
            let mut effects = BTreeSet::new();
            let mut position = None;
            loop {
                let page = crate::state_root::load_state_map_key_page(
                    root,
                    position.as_ref(),
                    cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
                    crate::state_root::MAX_STATE_MAP_KEY_PAGE_BYTES,
                    resolver,
                )?;
                for entry in page.entries {
                    let effect: cymule_core::EffectProjection =
                        crate::state_root::load_typed_state_map_value(
                            root,
                            &entry.key,
                            crate::StateRootLeafKind::MachineEffect,
                            resolver,
                        )?
                        .ok_or_else(|| DurableError::Integrity {
                            code: "executor_completed_effect_missing".to_owned(),
                            message: format!(
                                "Run {run_id} completed Effect {} is missing",
                                entry.key
                            ),
                        })?;
                    if effect.intent_id != entry.key {
                        return Err(DurableError::Integrity {
                            code: "executor_completed_effect_mismatch".to_owned(),
                            message: format!(
                                "Run {run_id} completed Effect {} changed identity",
                                entry.key
                            ),
                        });
                    }
                    effects.insert(effect.intent_id);
                }
                let Some(next) = page.next_position else {
                    break;
                };
                position = Some(next);
            }
            Ok(cymule_runtime::ExecutionResult {
                run_id: run_id.to_owned(),
                plan_id: read.run.current_plan.clone(),
                value,
                projection_digest: read.projection_root,
                precondition_token: read.run.precondition_token(),
                effects: effects.into_iter().collect(),
            })
        })
    }

    pub(crate) fn read_effect_execution(
        &mut self,
        run_id: &str,
        intent_id: &str,
        expected_revision: &str,
    ) -> DurableResult<Option<ExecutorEffectRead>> {
        self.read_current_state_root(|manifest, resolver| {
            if manifest.revision() != expected_revision {
                return Err(DurableError::Conflict {
                    expected: Some(expected_revision.to_owned()),
                    current: Some(manifest.revision().to_owned()),
                });
            }
            load_executor_effect_at_manifest(manifest, resolver, Some(run_id), intent_id)
        })
    }

    pub(crate) fn read_release_effect(
        &mut self,
        intent_id: &str,
    ) -> DurableResult<Option<ExecutorEffectRead>> {
        self.read_current_state_root(|manifest, resolver| {
            load_executor_effect_at_manifest(manifest, resolver, None, intent_id)
        })
    }

    pub(crate) fn read_next_dispatch(
        &mut self,
        run_id: &str,
        explicit_target: Option<&str>,
    ) -> DurableResult<ExecutorDispatchRead> {
        self.read_current_state_root(|manifest, resolver| {
            let root = crate::state_root::load_run_query_index_root(
                manifest,
                resolver,
                run_id,
                crate::state_root::RunQueryIndexKind::ActiveEffects,
            )?;
            let maximum = u64::try_from(
                cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES
                    * cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGES,
            )
            .map_err(|error| DurableError::Validation(error.to_string()))?;
            if root.entries > maximum {
                return Err(DurableError::Validation(format!(
                    "Run {run_id} dispatch set exceeds {maximum} exact entries"
                )));
            }
            let mut intents = BTreeSet::new();
            let mut position = None;
            loop {
                let page = crate::state_root::load_state_map_key_page(
                    &root,
                    position.as_ref(),
                    cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
                    crate::state_root::MAX_STATE_MAP_KEY_PAGE_BYTES,
                    resolver,
                )?;
                intents.extend(page.entries.into_iter().map(|entry| entry.key));
                let Some(next) = page.next_position else {
                    break;
                };
                position = Some(next);
            }
            if let Some(target) = explicit_target
                && !intents.contains(target)
            {
                return Ok(ExecutorDispatchRead {
                    revision: manifest.revision().to_owned(),
                    next: None,
                    explicit_intents: BTreeSet::new(),
                });
            }
            let mut explicit_intents = BTreeSet::new();
            let mut next = None;
            for intent_id in intents {
                let read =
                    load_executor_effect_at_manifest(manifest, resolver, Some(run_id), &intent_id)?
                        .ok_or_else(|| DurableError::Integrity {
                            code: "executor_dispatch_effect_missing".to_owned(),
                            message: format!(
                                "Run {run_id} active dispatch {intent_id} lost its exact leaves"
                            ),
                        })?;
                if read.effect.profile.dispatch == cymule_core::DispatchPolicy::Explicit
                    && read.dispatch.state == OutboxState::Pending
                    && read.effect.phase == cymule_core::EffectPhase::Prepared
                    && read.scope.status == cymule_core::ScopeStatus::ClosedCommitted
                {
                    explicit_intents.insert(intent_id.clone());
                }
                let selected = match explicit_target {
                    Some(target) => {
                        intent_id == target
                            && (matches!(
                                read.dispatch.state,
                                OutboxState::Claimed | OutboxState::Unknown
                            ) || read.dispatch.state == OutboxState::Pending
                                && read.effect.profile.dispatch
                                    == cymule_core::DispatchPolicy::Explicit
                                && read.effect.phase == cymule_core::EffectPhase::Prepared
                                && read.scope.status == cymule_core::ScopeStatus::ClosedCommitted)
                    }
                    None => {
                        matches!(
                            read.dispatch.state,
                            OutboxState::Claimed | OutboxState::Unknown
                        ) || read.dispatch.state == OutboxState::Pending
                            && (read.effect.profile.dispatch == cymule_core::DispatchPolicy::Eager
                                || read.effect.profile.dispatch
                                    == cymule_core::DispatchPolicy::OnScopeCommit
                                    && read.scope.status
                                        == cymule_core::ScopeStatus::ClosedCommitted)
                    }
                };
                if selected && next.is_none() {
                    next = Some(read);
                }
            }
            Ok(ExecutorDispatchRead {
                revision: manifest.revision().to_owned(),
                next,
                explicit_intents,
            })
        })
    }

    pub(crate) fn read_agent_session(
        &mut self,
        query: &agent_protocol::AgentSessionQuery,
    ) -> DurableResult<agent_protocol::AgentSessionRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        let read = self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_agent_session_current(
                manifest,
                resolver,
                &query.session_id,
            )?;
            if let Some(current) = &current {
                verify_agent_session_origin(manifest, resolver, current)?;
            }
            Ok(agent_protocol::AgentSessionRead {
                revision: manifest.revision().to_owned(),
                current,
            })
        })?;
        read.verify_for(query)?;
        if let Some(witness) = read
            .current
            .as_ref()
            .and_then(|current| current.last_transition.as_ref())
        {
            self.verify_agent_workspace_origin(&witness.command_id)?;
        }
        Ok(read)
    }

    pub(crate) fn read_agent_messages(
        &mut self,
        query: &agent_protocol::AgentMessagePageQuery,
    ) -> DurableResult<agent_protocol::AgentMessagePageRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        self.read_current_state_root(|manifest, resolver| {
            let read = crate::state_root::load_agent_message_page(manifest, resolver, query)?;
            for current in &read.page.entries {
                verify_agent_message_origin(manifest, resolver, current)?;
            }
            Ok(read)
        })
    }

    pub(crate) fn read_agent_message(
        &mut self,
        query: &agent_protocol::AgentMessageQuery,
    ) -> DurableResult<agent_protocol::AgentMessageRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        let read = self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_agent_message_current(
                manifest,
                resolver,
                &query.session_id,
                &query.message_id,
            )?;
            if let Some(current) = &current {
                verify_agent_message_origin(manifest, resolver, current)?;
            }
            Ok(agent_protocol::AgentMessageRead {
                revision: manifest.revision().to_owned(),
                current,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(crate) fn read_agent_tool(
        &mut self,
        query: &agent_protocol::AgentToolQuery,
    ) -> DurableResult<agent_protocol::AgentToolRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        let read = self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_agent_tool_current(
                manifest,
                resolver,
                &query.session_id,
                &query.tool_call_id,
            )?;
            if let Some(current) = &current {
                verify_agent_tool_origin(manifest, resolver, current)?;
            }
            Ok(agent_protocol::AgentToolRead {
                revision: manifest.revision().to_owned(),
                current,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(crate) fn read_agent_elicitation(
        &mut self,
        query: &agent_protocol::AgentElicitationQuery,
    ) -> DurableResult<agent_protocol::AgentElicitationRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        let read = self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_agent_elicitation_current(
                manifest,
                resolver,
                &query.session_id,
                &query.request_id,
            )?;
            if let Some(current) = &current {
                verify_agent_elicitation_origin(manifest, resolver, current)?;
            }
            Ok(agent_protocol::AgentElicitationRead {
                revision: manifest.revision().to_owned(),
                current,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(crate) fn read_agent_occurrence(
        &mut self,
        query: &agent_protocol::AgentOccurrenceQuery,
    ) -> DurableResult<agent_protocol::AgentOccurrenceRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        let read = self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_agent_occurrence_current(
                manifest,
                resolver,
                &query.session_id,
                &query.occurrence_id,
            )?;
            if let Some(current) = &current {
                verify_agent_occurrence_origin(manifest, resolver, current)?;
            }
            Ok(agent_protocol::AgentOccurrenceRead {
                revision: manifest.revision().to_owned(),
                current,
            })
        })?;
        read.verify_for(query)?;
        if let Some(current) = &read.current {
            self.verify_agent_workspace_origin(&current.admitted_by)?;
        }
        Ok(read)
    }

    pub(crate) fn read_agent_occurrences(
        &mut self,
        query: &agent_protocol::AgentOccurrencePageQuery,
    ) -> DurableResult<agent_protocol::AgentOccurrencePageRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        let read = self.read_current_state_root(|manifest, resolver| {
            let read = crate::state_root::load_agent_occurrence_page(manifest, resolver, query)?;
            for current in &read.page.entries {
                verify_agent_occurrence_origin(manifest, resolver, current)?;
            }
            Ok(read)
        })?;
        let origins = read
            .page
            .entries
            .iter()
            .map(|current| current.admitted_by.as_str())
            .collect::<BTreeSet<_>>();
        for command_id in origins {
            self.verify_agent_workspace_origin(command_id)?;
        }
        Ok(read)
    }

    pub(crate) fn read_agent_stream(
        &mut self,
        query: &agent_protocol::AgentStreamQuery,
    ) -> DurableResult<agent_protocol::AgentStreamRead> {
        query.verify()?;
        ensure_agent_query_revision(query.expected_revision.as_ref(), self.current_revision()?)?;
        let read = self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_agent_stream_current(
                manifest,
                resolver,
                &query.session_id,
                &query.stream_id,
            )?;
            if let Some(current) = &current {
                verify_agent_stream_origin(manifest, resolver, current)?;
            }
            Ok(agent_protocol::AgentStreamRead {
                revision: manifest.revision().to_owned(),
                current,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(crate) fn read_evolution_current(
        &mut self,
        query: &evolution_protocol::EvolutionCurrentQuery,
    ) -> DurableResult<evolution_protocol::EvolutionCurrentRead> {
        query.verify()?;
        let read = self.read_current_state_root(|manifest, resolver| {
            if query
                .expected_revision
                .as_deref()
                .is_some_and(|expected| expected != manifest.revision())
            {
                return Err(DurableError::Conflict {
                    expected: query.expected_revision.clone(),
                    current: Some(manifest.revision().to_owned()),
                });
            }
            Ok(evolution_protocol::EvolutionCurrentRead {
                observed_revision: manifest.revision().to_owned(),
                current: crate::state_root::load_evolution_current(
                    manifest,
                    resolver,
                    &query.evolution_id,
                )?,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(crate) fn read_evolution_receipt(
        &mut self,
        query: &evolution_protocol::EvolutionReceiptQuery,
    ) -> DurableResult<evolution_protocol::EvolutionReceiptRead> {
        query.verify()?;
        let read = self.read_current_state_root(|manifest, resolver| {
            if query
                .expected_revision
                .as_deref()
                .is_some_and(|expected| expected != manifest.revision())
            {
                return Err(DurableError::Conflict {
                    expected: query.expected_revision.clone(),
                    current: Some(manifest.revision().to_owned()),
                });
            }
            let retained = crate::state_root::load_evolution_command_receipt(
                manifest,
                resolver,
                &query.evolution_id,
                &query.command_id,
            )?;
            let (alias, receipt) = retained.unzip();
            Ok(evolution_protocol::EvolutionReceiptRead {
                observed_revision: manifest.revision().to_owned(),
                alias,
                receipt,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(crate) fn read_virtual_current(
        &mut self,
        query: &virtual_protocol::VirtualCurrentQuery,
    ) -> DurableResult<virtual_protocol::VirtualCurrentRead> {
        query.verify()?;
        let read = self.read_current_state_root(|manifest, resolver| {
            if query
                .expected_revision
                .as_deref()
                .is_some_and(|expected| expected != manifest.revision())
            {
                return Err(DurableError::Conflict {
                    expected: query.expected_revision.clone(),
                    current: Some(manifest.revision().to_owned()),
                });
            }
            Ok(virtual_protocol::VirtualCurrentRead {
                observed_revision: manifest.revision().to_owned(),
                current: crate::state_root::load_virtual_current(
                    manifest,
                    resolver,
                    &query.scheduler_id,
                )?,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    pub(crate) fn read_virtual_receipt(
        &mut self,
        query: &virtual_protocol::VirtualReceiptQuery,
    ) -> DurableResult<virtual_protocol::VirtualReceiptRead> {
        query.verify()?;
        let read = self.read_current_state_root(|manifest, resolver| {
            if query
                .expected_revision
                .as_deref()
                .is_some_and(|expected| expected != manifest.revision())
            {
                return Err(DurableError::Conflict {
                    expected: query.expected_revision.clone(),
                    current: Some(manifest.revision().to_owned()),
                });
            }
            Ok(virtual_protocol::VirtualReceiptRead {
                observed_revision: manifest.revision().to_owned(),
                receipt: crate::state_root::load_virtual_receipt(
                    manifest,
                    resolver,
                    &query.scheduler_id,
                    &query.command_id,
                )?,
            })
        })?;
        read.verify_for(query)?;
        Ok(read)
    }

    fn load_evolution_required_read(
        manifest: &crate::StateRootManifest,
        resolver: &mut dyn crate::StateRootResolver,
        view: &mut evolution_protocol::EvolutionAuthorityView,
        family: evolution_protocol::EvolutionStateFamily,
        storage_key: String,
    ) -> DurableResult<()> {
        match crate::state_root::load_evolution_mutation(manifest, resolver, family, &storage_key)?
        {
            Some(mutation) => view.insert(mutation)?,
            None => view.record_missing(family, storage_key)?,
        }
        Ok(())
    }

    fn load_evolution_machine_artifact(
        manifest: &crate::StateRootManifest,
        resolver: &mut dyn crate::StateRootResolver,
        reference: &ArtifactRef,
    ) -> DurableResult<cymule_core::ArtifactRecord> {
        reference.validate()?;
        let artifact =
            crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                .artifact(&reference.artifact_id)?
                .ok_or_else(|| {
                    DurableError::NotFound(format!(
                        "Evolution requires Machine Artifact {}",
                        reference.artifact_id
                    ))
                })?;
        if artifact.reference != *reference {
            return Err(DurableError::Integrity {
                code: "evolution_machine_artifact_reference_mismatch".to_owned(),
                message: format!(
                    "Machine Artifact {} changed its exact Evolution reference",
                    reference.artifact_id
                ),
            });
        }
        Ok(artifact)
    }

    fn verify_evolution_machine_artifacts(
        &mut self,
        manifest: &crate::StateRootManifest,
        references: &BTreeSet<ArtifactRef>,
    ) -> DurableResult<()> {
        if references.is_empty() {
            return Ok(());
        }
        self.store.with_state_root_resolver(manifest, |resolver| {
            for reference in references {
                let _ = Self::load_evolution_machine_artifact(manifest, resolver, reference)?;
            }
            Ok(())
        })
    }

    fn complete_evolution_read_set(
        &mut self,
        manifest: &crate::StateRootManifest,
        mut view: evolution_protocol::EvolutionAuthorityView,
        command: &evolution_protocol::EvolutionPersistenceCommand,
        source: &evolution_protocol::EvolutionReductionSource,
    ) -> DurableResult<evolution_protocol::EvolutionAuthorityView> {
        self.store.with_state_root_resolver(manifest, |resolver| {
            loop {
                match evolution_protocol::prepare_evolution(&view, command, source) {
                    Ok(_) => return Ok(()),
                    Err(evolution_protocol::EvolutionError::ReadRequired {
                        family,
                        storage_key,
                    }) => Self::load_evolution_required_read(
                        manifest,
                        resolver,
                        &mut view,
                        family,
                        storage_key,
                    )?,
                    Err(error) => return Err(error.into()),
                }
            }
        })?;
        Ok(view)
    }

    fn prepare_evolution_postcondition(
        &mut self,
        command: &evolution_protocol::EvolutionPersistenceCommand,
        providers: &mut dyn evolution_protocol::EvolutionProviders,
    ) -> DurableResult<evolution_protocol::EvolutionPostcondition> {
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        let preparation = self.store.with_state_root_resolver(&manifest, |resolver| {
            Self::load_evolution_preparation(&manifest, resolver, command)
        })?;

        let postcondition = match preparation {
            PinnedEvolutionPreparation::Selection { view, binding } => {
                let prepared =
                    evolution_protocol::prepare_evolution_selection(view.as_ref(), command)?;
                evolution_protocol::reduce_evolution_selection(prepared, *binding)?
            }
            PinnedEvolutionPreparation::FreshMigration {
                view,
                target_plan,
                safe_point,
                continuation,
                source_binding,
            } => {
                let target_binding = providers.target_execution_binding(&target_plan.plan_id)?;
                let target_binding = evolution_protocol::admit_evolution_target_binding(
                    target_plan.as_ref(),
                    &target_binding,
                )?;
                let source = evolution_protocol::EvolutionReductionSource::migration(
                    *safe_point,
                    *continuation,
                    *source_binding,
                    target_binding,
                )?;
                let view = self.complete_evolution_read_set(&manifest, *view, command, &source)?;
                let prepared = evolution_protocol::prepare_evolution(&view, command, &source)?;
                if prepared.migration_target_plan()? != Some(target_plan.as_ref()) {
                    return Err(DurableError::Integrity {
                        code: "evolution_migration_target_plan_changed".to_owned(),
                        message: "prepared Evolution migration changed its exact target Plan"
                            .to_owned(),
                    });
                }
                self.verify_evolution_machine_artifacts(
                    &manifest,
                    &prepared.provider_required_artifacts()?,
                )?;
                let provider =
                    evolution_protocol::execute_evolution_provider(&prepared, providers)?;
                evolution_protocol::reduce_prepared_evolution(prepared, provider)?
            }
            PinnedEvolutionPreparation::General {
                view,
                source,
                migration_target,
            } => {
                let view =
                    self.complete_evolution_read_set(&manifest, *view, command, source.as_ref())?;
                let prepared =
                    evolution_protocol::prepare_evolution(&view, command, source.as_ref())?;
                if let Some(target_plan) = migration_target.as_deref()
                    && prepared.migration_target_plan()? != Some(target_plan)
                {
                    return Err(DurableError::Integrity {
                        code: "evolution_migration_target_plan_changed".to_owned(),
                        message: "prepared Evolution migration changed its exact target Plan"
                            .to_owned(),
                    });
                }
                self.verify_evolution_machine_artifacts(
                    &manifest,
                    &prepared.provider_required_artifacts()?,
                )?;
                let provider =
                    evolution_protocol::execute_evolution_provider(&prepared, providers)?;
                evolution_protocol::reduce_prepared_evolution(prepared, provider)?
            }
        };
        postcondition.verify()?;
        if postcondition.receipt.command != *command {
            return Err(DurableError::Integrity {
                code: "evolution_postcondition_command_mismatch".to_owned(),
                message: "Evolution reducer returned a postcondition for another command"
                    .to_owned(),
            });
        }
        self.verify_evolution_machine_artifacts(&manifest, &postcondition.required_artifacts)?;
        Ok(postcondition)
    }

    fn load_evolution_preparation(
        manifest: &crate::StateRootManifest,
        resolver: &mut dyn crate::StateRootResolver,
        command: &evolution_protocol::EvolutionPersistenceCommand,
    ) -> DurableResult<PinnedEvolutionPreparation> {
        let current =
            crate::state_root::load_evolution_current(manifest, resolver, &command.evolution_id)?;
        let view =
            evolution_protocol::EvolutionAuthorityView::new(command.evolution_id.clone(), current)?;
        match &command.command {
            evolution_protocol::LiveEvolutionCommand::Apply { command: inner, .. } => match inner
                .as_ref()
            {
                evolution_protocol::EvolutionCommand::SelectOccurrence {
                    execution_binding,
                    ..
                } => Self::load_evolution_selection_preparation(
                    manifest,
                    resolver,
                    command,
                    view,
                    execution_binding,
                ),
                evolution_protocol::EvolutionCommand::Migrate { request, .. } => {
                    Self::load_evolution_migration_preparation(
                        manifest,
                        resolver,
                        command,
                        view,
                        &request.run_id,
                    )
                }
                evolution_protocol::EvolutionCommand::RestartUnderNewPlan { request, .. } => {
                    let safe_point = crate::state_root::pinned_machine::PinnedMachineView::open(
                        manifest, resolver,
                    )?
                    .migration_safe_point(&request.run_id)?;
                    Ok(PinnedEvolutionPreparation::General {
                        view: Box::new(view),
                        source: Box::new(evolution_protocol::EvolutionReductionSource::restart(
                            safe_point.safe_point,
                            safe_point.source.continuation,
                        )?),
                        migration_target: None,
                    })
                }
                _ => Ok(PinnedEvolutionPreparation::General {
                    view: Box::new(view),
                    source: Box::new(evolution_protocol::EvolutionReductionSource::none()),
                    migration_target: None,
                }),
            },
            _ => Ok(PinnedEvolutionPreparation::General {
                view: Box::new(view),
                source: Box::new(evolution_protocol::EvolutionReductionSource::none()),
                migration_target: None,
            }),
        }
    }

    fn load_evolution_selection_preparation(
        manifest: &crate::StateRootManifest,
        resolver: &mut dyn crate::StateRootResolver,
        command: &evolution_protocol::EvolutionPersistenceCommand,
        mut view: evolution_protocol::EvolutionAuthorityView,
        execution_binding: &ArtifactRef,
    ) -> DurableResult<PinnedEvolutionPreparation> {
        loop {
            match evolution_protocol::prepare_evolution_selection(&view, command) {
                Ok(_) => break,
                Err(evolution_protocol::EvolutionError::ReadRequired {
                    family,
                    storage_key,
                }) => Self::load_evolution_required_read(
                    manifest,
                    resolver,
                    &mut view,
                    family,
                    storage_key,
                )?,
                Err(error) => return Err(error.into()),
            }
        }
        let binding = Self::load_evolution_machine_artifact(manifest, resolver, execution_binding)?;
        Ok(PinnedEvolutionPreparation::Selection {
            view: Box::new(view),
            binding: Box::new(binding),
        })
    }

    fn load_evolution_migration_preparation(
        manifest: &crate::StateRootManifest,
        resolver: &mut dyn crate::StateRootResolver,
        command: &evolution_protocol::EvolutionPersistenceCommand,
        mut view: evolution_protocol::EvolutionAuthorityView,
        run_id: &str,
    ) -> DurableResult<PinnedEvolutionPreparation> {
        let target = loop {
            match evolution_protocol::prepare_evolution_migration_target(&view, command) {
                Ok(target) => break target,
                Err(evolution_protocol::EvolutionError::ReadRequired {
                    family,
                    storage_key,
                }) => Self::load_evolution_required_read(
                    manifest,
                    resolver,
                    &mut view,
                    family,
                    storage_key,
                )?,
                Err(error) => return Err(error.into()),
            }
        };
        let target_plan = target.plan().clone();
        if let Some(reference) = target.retained_target_binding() {
            let binding = Self::load_evolution_machine_artifact(manifest, resolver, reference)?;
            evolution_protocol::verify_evolution_target_binding_record(&target_plan, &binding)?;
            Ok(PinnedEvolutionPreparation::General {
                view: Box::new(view),
                source: Box::new(
                    evolution_protocol::EvolutionReductionSource::retained_migration(binding)?,
                ),
                migration_target: Some(Box::new(target_plan)),
            })
        } else {
            let safe_point =
                crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                    .migration_safe_point(run_id)?;
            Ok(PinnedEvolutionPreparation::FreshMigration {
                view: Box::new(view),
                target_plan: Box::new(target_plan),
                safe_point: Box::new(safe_point.safe_point),
                continuation: Box::new(safe_point.source.continuation),
                source_binding: Box::new(safe_point.source.binding.reference),
            })
        }
    }

    fn evolution_postcondition_operations(
        postcondition: &evolution_protocol::EvolutionPostcondition,
    ) -> Vec<DurableOperation> {
        let mut operations = Vec::with_capacity(3 + postcondition.mutations.len());
        operations.push(DurableOperation::PutEvolutionCurrent {
            value: postcondition.current.clone(),
        });
        operations.push(DurableOperation::PutEvolutionCommandAlias {
            value: postcondition.alias.clone(),
        });
        operations.push(DurableOperation::PutEvolutionPersistenceReceipt {
            value: postcondition.receipt.clone(),
        });
        operations.extend(
            postcondition
                .mutations
                .iter()
                .cloned()
                .map(|value| DurableOperation::PutEvolutionMutation { value }),
        );
        operations
    }

    pub(crate) fn commit_evolution(
        &mut self,
        command: &evolution_protocol::EvolutionPersistenceCommand,
        providers: &mut dyn evolution_protocol::EvolutionProviders,
    ) -> DurableResult<evolution_protocol::EvolutionCommit> {
        command.verify()?;
        let pinned = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .clone();
        let retained = self
            .store
            .with_state_root_resolver(&pinned.manifest, |resolver| {
                crate::state_root::load_evolution_command_receipt(
                    &pinned.manifest,
                    resolver,
                    &command.evolution_id,
                    command.command.command_id(),
                )
            })?;
        if let Some((_, receipt)) = retained {
            if receipt.command != *command {
                return Err(DurableError::HistoryConflict {
                    code: "evolution_command_reused".to_owned(),
                    message: format!(
                        "Evolution command {} was reused with different semantics",
                        command.command.command_id()
                    ),
                });
            }
            let commit = evolution_protocol::EvolutionCommit {
                observed_revision: pinned.revision().to_owned(),
                committed_revision: None,
                receipt,
            };
            commit.verify_for(command)?;
            return Ok(commit);
        }

        let postcondition = self.prepare_evolution_postcondition(command, providers)?;
        let migration = postcondition.migration_sidecar()?;
        let operations = Self::evolution_postcondition_operations(&postcondition);
        let material = (!postcondition.plans.is_empty() || !postcondition.artifacts.is_empty())
            .then(|| {
                cymule_core::durable_internal::MachineMaterialAdmission::new(
                    command.persistence_id.clone(),
                    postcondition.plans.clone(),
                    postcondition.artifacts.clone(),
                )
            })
            .transpose()?;

        let committed_revision = match migration {
            Some(migration) => {
                self.commit_evolution_migration(command, &migration, material, operations)?
            }
            None => match material {
                Some(material) => self.commit_material_sidecars(
                    &material,
                    &postcondition.receipt.receipt_id,
                    operations,
                )?,
                None => self.commit_profile_operations(operations)?,
            },
        };
        let commit = evolution_protocol::EvolutionCommit {
            observed_revision: committed_revision.clone(),
            committed_revision: Some(committed_revision),
            receipt: postcondition.receipt,
        };
        commit.verify_for(command)?;
        Ok(commit)
    }

    fn commit_evolution_migration(
        &mut self,
        command: &evolution_protocol::EvolutionPersistenceCommand,
        migration: &evolution_protocol::EvolutionMigrationSidecar,
        material: Option<cymule_core::durable_internal::MachineMaterialAdmission>,
        operations: Vec<DurableOperation>,
    ) -> DurableResult<String> {
        if migration.command_id() != command.persistence_id {
            return Err(DurableError::Integrity {
                code: "evolution_migration_command_id_mismatch".to_owned(),
                message: "Evolution migration sidecar changed its persistence command identity"
                    .to_owned(),
            });
        }
        let material = material.ok_or_else(|| DurableError::Integrity {
            code: "evolution_migration_material_missing".to_owned(),
            message: "fresh Evolution migration has no target binding or provider material"
                .to_owned(),
        })?;
        let source = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                .run_current(migration.run_id())?
                .ok_or_else(|| {
                    DurableError::NotFound(format!(
                        "Evolution migration Run {} does not exist",
                        migration.run_id()
                    ))
                })
        })?;
        let batch_command = cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id: migration.command_id().to_owned(),
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: migration.run_id().to_owned(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                Some(source.precondition_token()),
            ),
            command: migration.command().clone(),
        };
        let target = migration.target_continuation().clone();
        let expected_command_id = migration.command_id().to_owned();
        let batch = self.commit_pinned_command_batch(
            vec![batch_command],
            Some(material),
            move |transition| {
                evolution_migration_sidecars(transition, &expected_command_id, target, operations)
            },
        )?;
        cymule_core::validate_content_id("Evolution Machine batch", &batch.batch_id)?;
        cymule_core::validate_content_id(
            "Evolution Machine batch receipt",
            &batch.batch_receipt_id,
        )?;
        if batch.receipts.len() != 1
            || batch.receipts[0].command_id != command.persistence_id
            || batch.receipts[0].status != CommandReceiptStatus::Applied
        {
            return Err(DurableError::Integrity {
                code: "evolution_migration_batch_receipt_mismatch".to_owned(),
                message: "Evolution migration batch returned another command receipt".to_owned(),
            });
        }
        batch
            .committed_revision
            .ok_or_else(|| DurableError::HistoryConflict {
                code: "evolution_migration_sidecar_missing".to_owned(),
                message: "Machine migration batch exists without its atomic Evolution receipt"
                    .to_owned(),
            })
    }

    pub(crate) fn commit_virtual(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        providers: &mut dyn virtual_protocol::VirtualProviders,
        clock: &mut dyn crate::ExecutionClockAuthority,
    ) -> DurableResult<virtual_protocol::VirtualCommit> {
        command.verify()?;
        if matches!(
            command.operation,
            virtual_protocol::VirtualPersistenceOperation::Claim(_)
        ) {
            return self.replay_virtual_claim_alias(command);
        }
        if matches!(
            command.operation,
            virtual_protocol::VirtualPersistenceOperation::Initialize(_)
        ) {
            self.initialize_if_empty()?;
        }
        if let Some(receipt) = self.read_current_state_root(|manifest, resolver| {
            load_virtual_commit_receipt(manifest, resolver, command)
        })? {
            let commit = virtual_protocol::VirtualCommit {
                observed_revision: self.current_revision()?.to_owned(),
                committed_revision: None,
                receipt,
            };
            commit.verify_for(command)?;
            return Ok(commit);
        }
        let clock_reference = match &command.operation {
            virtual_protocol::VirtualPersistenceOperation::RenewLease(operation) => {
                Some(&operation.command.clock)
            }
            virtual_protocol::VirtualPersistenceOperation::Resolve(operation) => {
                Some(&operation.command.clock)
            }
            virtual_protocol::VirtualPersistenceOperation::Recover(operation) => {
                Some(&operation.command.clock)
            }
            _ => None,
        };
        let result = match clock_reference {
            Some(reference) => {
                self.with_current_clock(clock, reference, |coordinator, observation| {
                    coordinator.commit_virtual_non_claim_observed(
                        command,
                        providers,
                        Some(observation),
                    )
                })
            }
            None => self.commit_virtual_non_claim_observed(command, providers, None),
        }?;
        Ok(result.commit)
    }

    fn replay_virtual_claim_alias(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
    ) -> DurableResult<virtual_protocol::VirtualCommit> {
        let receipt = self.read_current_state_root(|manifest, resolver| {
            load_virtual_commit_receipt(manifest, resolver, command)
        })?;
        let Some(receipt) = receipt else {
            return Err(DurableError::Validation(
                "fresh Virtual Claim requires DurableVirtualControl::claim".to_owned(),
            ));
        };
        let commit = virtual_protocol::VirtualCommit {
            observed_revision: self.current_revision()?.to_owned(),
            committed_revision: None,
            receipt,
        };
        commit.verify_for(command)?;
        Ok(commit)
    }

    fn with_current_clock<T>(
        &mut self,
        clock: &mut dyn crate::ExecutionClockAuthority,
        reference: &crate::ClockObservationRef,
        commit: impl FnOnce(&mut Self, ClockObservation) -> DurableResult<T>,
    ) -> DurableResult<T> {
        reference.verify()?;
        let mut commit = Some(commit);
        let mut outcome = None;
        let guard_result = clock.with_current_head(reference, &mut |observation| {
            if outcome.is_some() {
                return Err(DurableError::Validation(
                    "Clock invoked a Store operation more than once".to_owned(),
                ));
            }
            let result = (|| {
                observation.verify()?;
                if observation.reference() != *reference {
                    return Err(DurableError::Validation(
                        "Clock returned another observation".to_owned(),
                    ));
                }
                let action = commit.take().ok_or_else(|| {
                    DurableError::Validation("Clock reused the one-shot Store operation".to_owned())
                })?;
                action(self, observation.clone())
            })();
            match result {
                Ok(value) => {
                    outcome = Some(Ok(value));
                    Ok(())
                }
                Err(error) => {
                    let message = error.to_string();
                    outcome = Some(Err(error));
                    Err(DurableError::Validation(format!(
                        "Clock-owned Store operation failed: {message}"
                    )))
                }
            }
        });
        match outcome {
            Some(Err(error)) => Err(error),
            Some(Ok(value)) => match guard_result {
                Ok(()) => Ok(value),
                Err(error @ DurableError::CommitOutcomeUnknown { .. }) => Err(error),
                Err(error) => Err(DurableError::CommitOutcomeUnknown {
                    message: format!("Clock guard failed after Store commit: {error}"),
                }),
            },
            None => match guard_result {
                Err(error) => Err(error),
                Ok(()) => Err(DurableError::Validation(
                    "Clock did not invoke its Store operation".to_owned(),
                )),
            },
        }
    }

    fn satisfy_virtual_reads<T>(
        &mut self,
        scheduler_id: &str,
        preparation: &mut VirtualCommandPreparation,
        mut prepare: impl FnMut(
            &virtual_protocol::VirtualKeyedSource,
        ) -> virtual_protocol::VirtualPreparationResult<T>,
    ) -> DurableResult<T> {
        loop {
            let source = preparation.source(scheduler_id)?;
            match prepare(&source) {
                Ok(value) => return Ok(value),
                Err(virtual_protocol::VirtualPreparationError::Protocol(error)) => {
                    return Err(error.into());
                }
                Err(virtual_protocol::VirtualPreparationError::ReadRequired {
                    family,
                    storage_key,
                }) => {
                    self.load_virtual_preparation_read(
                        scheduler_id,
                        preparation,
                        family,
                        &storage_key,
                    )?;
                }
            }
        }
    }

    fn load_virtual_preparation_read(
        &mut self,
        scheduler_id: &str,
        preparation: &mut VirtualCommandPreparation,
        family: virtual_protocol::VirtualStateFamily,
        storage_key: &str,
    ) -> DurableResult<()> {
        if preparation
            .reads
            .iter()
            .any(|read| read.family() == family && read.storage_key() == storage_key)
        {
            return Err(DurableError::Integrity {
                code: "virtual_repeated_read_requirement".to_owned(),
                message: "Virtual preparation repeated an already satisfied exact key".to_owned(),
            });
        }
        let leaf = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_virtual_leaf(
                manifest,
                resolver,
                scheduler_id,
                family,
                storage_key,
            )
        })?;
        preparation
            .reads
            .push(virtual_protocol::VirtualStateRead::new(
                family,
                storage_key,
                leaf,
            )?);
        Ok(())
    }

    fn commit_virtual_non_claim_observed(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        providers: &mut dyn virtual_protocol::VirtualProviders,
        observation: Option<ClockObservation>,
    ) -> DurableResult<VirtualCommandCommit> {
        let mut preparation = self.begin_virtual_command(command)?;
        let operation =
            self.prepare_virtual_operation(command, providers, observation, &mut preparation)?;
        self.commit_prepared_virtual_command(command, preparation, &operation)
    }

    fn commit_virtual_claim_observed(
        &mut self,
        claim: &virtual_protocol::VirtualClaimPersistenceCommand,
        binding: &ExecutionBinding,
        observation: ClockObservation,
    ) -> DurableResult<VirtualCommandCommit> {
        let command = virtual_protocol::VirtualPersistenceCommand::new(
            virtual_protocol::VirtualPersistenceOperation::Claim(claim.clone()),
        )?;
        let mut preparation = self.begin_virtual_command(&command)?;
        let operation =
            self.prepare_virtual_claim(&command, claim, binding, observation, &mut preparation)?;
        self.commit_prepared_virtual_command(&command, preparation, &operation)
    }

    fn begin_virtual_command(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
    ) -> DurableResult<VirtualCommandPreparation> {
        let current = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_virtual_current(manifest, resolver, command.scheduler_id())
        })?;
        Ok(VirtualCommandPreparation {
            current,
            reads: Vec::new(),
            operations: Vec::new(),
            plans: Vec::new(),
            artifacts: Vec::new(),
            claim_plan: None,
        })
    }

    fn commit_prepared_virtual_command(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        mut preparation: VirtualCommandPreparation,
        operation: &virtual_protocol::VirtualOperationAuthority,
    ) -> DurableResult<VirtualCommandCommit> {
        let reduction =
            self.satisfy_virtual_reads(command.scheduler_id(), &mut preparation, |source| {
                virtual_protocol::prepare_virtual(
                    command,
                    &virtual_protocol::VirtualReductionAuthority::new(
                        source.clone(),
                        (*operation).clone(),
                    ),
                )
            })?;
        let roots = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::preview_virtual_mutations(
                manifest,
                &reduction.mutations.operations,
                resolver,
            )
        })?;
        let postcondition = reduction.finish(roots)?;
        postcondition.verify()?;
        verify_virtual_claim_plan(&postcondition.receipt, preparation.claim_plan.as_ref())?;
        if postcondition.receipt.command != *command {
            return Err(DurableError::Integrity {
                code: "virtual_postcondition_command_mismatch".to_owned(),
                message: "Virtual reducer changed its exact command".to_owned(),
            });
        }
        if let virtual_protocol::VirtualPersistenceOutcome::Claimed(receipt) =
            &postcondition.receipt.outcome
            && receipt.claim.is_none()
            && (!preparation.operations.is_empty() || !preparation.plans.is_empty())
        {
            return Err(DurableError::Integrity {
                code: "virtual_empty_claim_sidecars".to_owned(),
                message: "empty Virtual claim acquired lease or selection authority".to_owned(),
            });
        }
        preparation
            .operations
            .extend(self.virtual_resource_postcondition(&postcondition)?);
        preparation.operations.extend([
            DurableOperation::PutVirtualCurrent {
                value: postcondition.current.clone(),
            },
            DurableOperation::PutVirtualPersistenceReceipt {
                value: Box::new(postcondition.receipt.clone()),
            },
        ]);
        preparation.operations.extend(
            postcondition
                .receipt
                .mutations
                .operations
                .iter()
                .cloned()
                .map(|value| DurableOperation::ApplyVirtualMutation { value }),
        );
        preparation.artifacts.extend(postcondition.artifacts);
        let artifacts = unique_artifact_records(preparation.artifacts)?;
        let revision = if artifacts.is_empty() && preparation.plans.is_empty() {
            self.commit_profile_operations(preparation.operations)?
        } else {
            let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
                command.persistence_id.clone(),
                preparation.plans,
                artifacts,
            )?;
            self.commit_material_sidecars(
                &material,
                &postcondition.receipt.receipt_id,
                preparation.operations,
            )?
        };
        let commit = virtual_protocol::VirtualCommit {
            observed_revision: revision.clone(),
            committed_revision: Some(revision),
            receipt: postcondition.receipt,
        };
        commit.verify_for(command)?;
        Ok(VirtualCommandCommit {
            commit,
            claim_plan: preparation.claim_plan,
        })
    }

    fn prepare_virtual_operation(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        providers: &mut dyn virtual_protocol::VirtualProviders,
        observation: Option<ClockObservation>,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        use virtual_protocol::{
            VirtualOperationAuthority as Authority, VirtualPersistenceOperation as Operation,
        };
        match &command.operation {
            Operation::Initialize(_) => Ok(Authority::Initialize),
            Operation::Claim(_) => Err(DurableError::Validation(
                "fresh Virtual Claim requires DurableVirtualControl::claim".to_owned(),
            )),
            Operation::RenewLease(operation) => {
                self.prepare_virtual_lease_renewal(operation, observation, preparation)
            }
            Operation::Resolve(operation) => {
                let active = preparation
                    .current()?
                    .body
                    .frontier
                    .active
                    .get(&operation.command.work_id)
                    .ok_or_else(|| {
                        DurableError::NotFound("Virtual resolution work is not active".to_owned())
                    })?;
                let _ = self.load_virtual_coordination_lease(&active.lease)?;
                Ok(Authority::Resolve {
                    clock: required_virtual_clock(observation)?,
                })
            }
            Operation::Recover(operation) => {
                let active = preparation
                    .current()?
                    .body
                    .frontier
                    .active
                    .get(&operation.command.work_id)
                    .ok_or_else(|| {
                        DurableError::NotFound("Virtual recovery work is not active".to_owned())
                    })?;
                let _ = self.load_virtual_coordination_lease(&active.lease)?;
                Ok(Authority::Recover {
                    clock: required_virtual_clock(observation)?,
                })
            }
            Operation::SetRunWeight(_) => Ok(Authority::SetRunWeight),
            Operation::ActivateWait(operation) => {
                self.read_current_state_root(|manifest, resolver| {
                    let receipt = crate::state_root::load_wait_activation(
                        manifest,
                        resolver,
                        &operation.activation_id,
                    )?
                    .ok_or_else(|| {
                        DurableError::NotFound(format!(
                            "Wait activation {} does not exist",
                            operation.activation_id
                        ))
                    })?;
                    let result = Self::load_evolution_machine_artifact(
                        manifest,
                        resolver,
                        &receipt.activation.result,
                    )?;
                    Ok(Authority::ActivateWait { receipt, result })
                })
            }
            Operation::Materialize(operation) => {
                self.prepare_virtual_materialization(command, operation, providers, preparation)
            }
            Operation::MigrateRegion(operation) => {
                self.prepare_virtual_migration(command, operation, providers, preparation)
            }
            Operation::Compact(operation) => {
                self.prepare_virtual_compaction(command, operation, providers, preparation)
            }
            Operation::Rehydrate(operation) => {
                self.prepare_virtual_rehydration(command, operation, providers, preparation)
            }
            Operation::RetireArchive(operation) => {
                self.prepare_virtual_archive_retirement(command, operation, preparation)
            }
        }
    }

    fn prepare_virtual_lease_renewal(
        &mut self,
        operation: &virtual_protocol::VirtualLeaseRenewalPersistenceCommand,
        observation: Option<ClockObservation>,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        let observation = required_virtual_clock(observation)?;
        let active = preparation
            .current()?
            .body
            .frontier
            .active
            .get(&operation.command.work_id)
            .cloned()
            .ok_or_else(|| {
                DurableError::NotFound("Virtual renewal work is not active".to_owned())
            })?;
        let lease = self.load_virtual_coordination_lease(&active.lease)?;
        let next = proposed_pinned_lease(
            Some(&lease),
            &lease.resource,
            &operation.command.owner,
            observation.logical_time,
            operation.command.lease_ttl,
        )?;
        preparation.operations.push(DurableOperation::PutLease {
            value: next.clone(),
        });
        Ok(virtual_protocol::VirtualOperationAuthority::RenewLease {
            clock: observation,
            lease: virtual_protocol::VirtualClaimLease {
                resource: next.resource,
                owner: next.owner,
                epoch: next.epoch,
                expires_at: next.expires_at,
                clock: operation.command.clock.clone(),
            },
        })
    }

    fn prepare_virtual_materialization(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        operation: &virtual_protocol::VirtualMaterializationCommand,
        providers: &mut dyn virtual_protocol::VirtualProviders,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        let selection = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::pinned_machine::load_virtual_active_region_selection(
                manifest,
                preparation.current()?,
                resolver,
            )
        })?;
        self.satisfy_virtual_reads(command.scheduler_id(), preparation, |source| {
            virtual_protocol::preflight_virtual_provider(command, source, Some(&selection.proof))
        })?;
        let region = preparation.region(&operation.region_id)?.region.clone();
        let current = preparation.current()?;
        let ready = current
            .body
            .frontier
            .ready
            .values()
            .map(std::collections::VecDeque::len)
            .sum::<usize>();
        let parked = usize::try_from(current.body.counts.parked)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let materialized = ready
            .checked_add(current.body.frontier.active.len())
            .and_then(|count| count.checked_add(parked))
            .ok_or_else(|| {
                DurableError::Validation("Virtual materialized count overflowed".to_owned())
            })?;
        let available = current
            .body
            .limits
            .max_materialized
            .checked_sub(materialized)
            .ok_or_else(|| DurableError::Integrity {
                code: "virtual_capacity_inconsistent".to_owned(),
                message: "Virtual frontier exceeds its materialized bound".to_owned(),
            })?;
        let limit = available.min(current.body.limits.materialize_batch);
        if limit == 0
            || region.source != operation.expected_source
            || region.cursor != operation.expected_cursor
        {
            return Err(DurableError::IllegalTransition(
                "Virtual materialization has no capacity or its source/cursor changed".to_owned(),
            ));
        }
        let provider = providers.region_source(&region.source)?;
        if provider.source_binding() != region.source {
            return Err(DurableError::Validation(
                "Virtual source provider changed its exact binding".to_owned(),
            ));
        }
        let page = provider.materialize(&region, limit)?;
        let archive_binding = current.body.archive.clone();
        let archive_root = current.body.archived_work_index_root_digest.clone();
        let archive = providers.archive(&archive_binding)?;
        if archive.archive_binding() != archive_binding {
            return Err(DurableError::Validation(
                "Virtual archive provider changed its exact binding".to_owned(),
            ));
        }
        let mut archived_work_proofs = BTreeMap::new();
        for item in &page.items {
            let proof = archive.work_index_proof(&archive_root, &item.work_id)?;
            if archived_work_proofs
                .insert(item.work_id.clone(), proof)
                .is_some()
            {
                return Err(DurableError::Validation(
                    "Virtual source returned duplicate work identities".to_owned(),
                ));
            }
        }
        Ok(virtual_protocol::VirtualOperationAuthority::Materialize {
            selection: selection.proof,
            page,
            archived_work_proofs,
        })
    }

    fn prepare_virtual_migration(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        operation: &virtual_protocol::VirtualMigrationPersistenceCommand,
        providers: &mut dyn virtual_protocol::VirtualProviders,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        self.satisfy_virtual_reads(command.scheduler_id(), preparation, |source| {
            virtual_protocol::preflight_virtual_provider(command, source, None)
        })?;
        let sources = operation
            .request
            .source_region_ids
            .iter()
            .map(|region_id| {
                preparation
                    .region(region_id)
                    .map(|current| current.region.clone())
            })
            .collect::<DurableResult<Vec<_>>>()?;
        let provider = providers.region_migrator(
            &operation.request.migration_binding,
            &operation.request.migration_revision,
        )?;
        if provider.binding() != operation.request.migration_binding
            || provider.revision() != operation.request.migration_revision
        {
            return Err(DurableError::Validation(
                "Virtual migration provider changed its exact binding".to_owned(),
            ));
        }
        let proposal = provider.plan(&operation.request, &sources)?;
        proposal.verify_for(operation)?;
        provider.verify(&proposal.plan)?;
        proposal.into_authority(operation).map_err(Into::into)
    }

    fn prepare_virtual_rehydration(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        operation: &virtual_protocol::VirtualRehydrationPersistenceCommand,
        providers: &mut dyn virtual_protocol::VirtualProviders,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        self.satisfy_virtual_reads(command.scheduler_id(), preparation, |source| {
            virtual_protocol::preflight_virtual_provider(command, source, None)
        })?;
        let certificate = preparation.certificate(&operation.command.certificate_id)?;
        let archive_binding = preparation.current()?.body.archive.clone();
        let provider = providers.archive(&archive_binding)?;
        if provider.archive_binding() != archive_binding {
            return Err(DurableError::Validation(
                "Virtual rehydration provider changed its exact binding".to_owned(),
            ));
        }
        let occurrences = operation
            .command
            .occurrence_ids
            .iter()
            .map(|occurrence_id| {
                provider
                    .rehydrate_occurrence(
                        &certificate.certificate.rehydration_manifest,
                        occurrence_id,
                    )
                    .map_err(DurableError::from)
            })
            .collect::<DurableResult<Vec<_>>>()?;
        Ok(virtual_protocol::VirtualOperationAuthority::Rehydrate { occurrences })
    }

    fn load_virtual_coordination_lease(
        &mut self,
        expected: &virtual_protocol::VirtualClaimLease,
    ) -> DurableResult<CoordinationLease> {
        self.read_current_state_root(|manifest, resolver| {
            let lease: CoordinationLease = crate::state_root::load_typed_state_map_value(
                &manifest.roots().leases,
                &expected.resource,
                crate::StateRootLeafKind::Lease,
                resolver,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "virtual_lease_missing".to_owned(),
                message: "Virtual claim has no exact M1 capacity lease".to_owned(),
            })?;
            if lease.resource != expected.resource
                || lease.owner != expected.owner
                || lease.epoch != expected.epoch
                || lease.expires_at != expected.expires_at
            {
                return Err(DurableError::Conflict {
                    expected: Some(format!("{}:{}", expected.owner, expected.epoch)),
                    current: Some(format!("{}:{}", lease.owner, lease.epoch)),
                });
            }
            Ok(lease)
        })
    }

    fn read_virtual_compaction_source(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        operation: &virtual_protocol::VirtualCompactionPersistenceCommand,
        preparation: &VirtualCommandPreparation,
    ) -> DurableResult<VirtualCompactionSource> {
        let current = preparation.current()?.clone();
        let region = preparation
            .region(&operation.command.region_id)?
            .region
            .clone();
        let mut occurrences = BTreeMap::new();
        for read in &preparation.reads {
            if let Some(virtual_protocol::VirtualStateLeaf::Occurrences(value)) = read.leaf() {
                occurrences.insert(
                    value.occurrence.occurrence_id.clone(),
                    value.occurrence.clone(),
                );
            }
        }
        let mut work_index = BTreeMap::new();
        for work_id in &operation.command.work_ids {
            let work = preparation.work(work_id)?;
            let occurrence_id = work.latest_occurrence_id.as_ref().ok_or_else(|| {
                DurableError::IllegalTransition(
                    "Virtual compaction work has no occurrence".to_owned(),
                )
            })?;
            let occurrence = occurrences.get(occurrence_id).ok_or_else(|| {
                DurableError::IllegalTransition(
                    "Virtual compaction did not select its latest work occurrence".to_owned(),
                )
            })?;
            work_index.insert(
                work_id.clone(),
                virtual_protocol::ArchivedWorkIndex {
                    work_id: work_id.clone(),
                    region_id: work.item.region_id.clone(),
                    run_id: work.item.run_id.clone(),
                    occurrence_id: occurrence_id.clone(),
                    max_epoch: work.max_epoch,
                    terminal_state: occurrence.state,
                },
            );
        }
        let command_receipts = self.read_current_state_root(|manifest, resolver| {
            operation
                .command
                .archived_command_ids
                .iter()
                .map(|command_id| {
                    let receipt = crate::state_root::load_virtual_receipt(
                        manifest,
                        resolver,
                        command.scheduler_id(),
                        command_id,
                    )?
                    .ok_or_else(|| {
                        DurableError::NotFound(format!(
                            "Virtual compaction receipt {command_id} does not exist"
                        ))
                    })?;
                    Ok((command_id.clone(), receipt))
                })
                .collect::<DurableResult<BTreeMap<_, _>>>()
        })?;
        let journal_id = (!command_receipts.is_empty())
            .then(|| virtual_protocol::virtual_scheduler_journal_id(command.scheduler_id()))
            .transpose()?;
        Ok(VirtualCompactionSource {
            current,
            region,
            occurrences,
            work_index,
            command_receipts,
            journal_id,
        })
    }

    fn prepare_virtual_compaction(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        operation: &virtual_protocol::VirtualCompactionPersistenceCommand,
        providers: &mut dyn virtual_protocol::VirtualProviders,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        self.satisfy_virtual_reads(command.scheduler_id(), preparation, |source| {
            virtual_protocol::preflight_virtual_provider(command, source, None)
        })?;
        let VirtualCompactionSource {
            current,
            region,
            occurrences,
            work_index,
            command_receipts,
            journal_id,
        } = self.read_virtual_compaction_source(command, operation, preparation)?;
        let archive = providers.archive(&current.body.archive)?;
        if archive.archive_binding() != current.body.archive
            || operation.command.archive != current.body.archive
        {
            return Err(DurableError::Validation(
                "Virtual compaction changed its exact archive binding".to_owned(),
            ));
        }
        let mut work_root = current.body.archived_work_index_root_digest.clone();
        let mut work_index_updates = Vec::new();
        for value in work_index.values() {
            let update = archive.insert_work_index(&work_root, value)?;
            if update.parent_root_digest != work_root || update.value != *value {
                return Err(DurableError::Integrity {
                    code: "virtual_archive_work_update_mismatch".to_owned(),
                    message: "archive returned a different work-index insertion".to_owned(),
                });
            }
            work_root.clone_from(&update.result_root_digest);
            work_index_updates.push(update);
        }
        let manifest = virtual_protocol::VirtualArchiveManifest {
            manifest_version: virtual_protocol::VIRTUAL_ARCHIVE_MANIFEST_VERSION.to_owned(),
            region_id: region.region_id,
            run_id: region.run_id,
            journal_id,
            source_causal_cut: operation.command.source_causal_cut.clone(),
            occurrences,
            work_index,
            parent_work_index_root_digest: current.body.archived_work_index_root_digest.clone(),
            work_index_updates: work_index_updates.clone(),
            result_work_index_root_digest: work_root,
            command_receipts,
        };
        manifest.verify()?;
        let (occurrence_root_digest, command_root_digest) =
            virtual_protocol::virtual_archive_roots(&manifest)?;
        let publication = archive.publish_archive(&manifest)?;
        let mut product = virtual_protocol::VirtualCompactionPublication {
            publication,
            occurrence_root_digest,
            command_root_digest,
            work_index_updates,
            command_index_updates: Vec::new(),
        };
        let source = preparation.source(command.scheduler_id())?;
        let certificate = virtual_protocol::prepare_virtual_compaction_certificate(
            operation, &source, &manifest, &product,
        )?;
        let mut command_root = current.body.archived_command_index_root_digest.clone();
        if let Some(journal_id) = &manifest.journal_id {
            for command_id in manifest.command_receipts.keys() {
                let value = virtual_protocol::ArchivedCommandIndex {
                    journal_id: journal_id.clone(),
                    command_id: command_id.clone(),
                    certificate_id: certificate.certificate_id.clone(),
                    archive_resource_id: product.publication.resource.resource_id.clone(),
                };
                let update = archive.insert_command_index(&command_root, &value)?;
                if update.parent_root_digest != command_root || update.value != value {
                    return Err(DurableError::Integrity {
                        code: "virtual_archive_command_update_mismatch".to_owned(),
                        message: "archive returned a different command-index insertion".to_owned(),
                    });
                }
                command_root.clone_from(&update.result_root_digest);
                product.command_index_updates.push(update);
            }
        }
        let pin = resource_protocol::ResourcePin::profile(
            resource_protocol::ResourceRetentionSubject::from_publication(&product.publication)?,
            resource_protocol::ResourcePinKind::VirtualArchive {
                archive_id: product.publication.resource.resource_id.clone(),
            },
        )?;
        let (retention, current_pin) = self.read_resource_lifecycle_sources(&pin)?;
        let archive_pin = resource_protocol::reduce_resource_pin_receipt(
            &operation.command.command_id,
            &pin,
            retention.as_ref(),
            current_pin.as_ref(),
        )?;
        Ok(virtual_protocol::VirtualOperationAuthority::Compact {
            manifest,
            archive: product,
            archive_pin,
        })
    }

    fn prepare_virtual_archive_retirement(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        operation: &virtual_protocol::VirtualArchiveRetirementPersistenceCommand,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        let key = virtual_protocol::virtual_certificate_key(
            command.scheduler_id(),
            &operation.command.certificate_id,
        )?;
        self.load_virtual_preparation_read(
            command.scheduler_id(),
            preparation,
            virtual_protocol::VirtualStateFamily::Certificates,
            &key,
        )?;
        let certificate = preparation.certificate(&operation.command.certificate_id)?;
        let pin_id = resource_protocol::resource_archive_pin_id(
            &certificate.certificate.rehydration_manifest.resource_id,
        )?;
        let pin = self.read_current_state_root(|manifest, resolver| {
            let current =
                crate::state_root::load_resource_pin_current(manifest, resolver, &pin_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "virtual_archive_pin_missing".to_owned(),
                        message: "Virtual archive retirement lost its exact Resource pin"
                            .to_owned(),
                    })?;
            verify_resource_pin_origin(manifest, resolver, &current)?;
            Ok(current)
        })?;
        let (retention, current_pin) = self.read_resource_lifecycle_sources(&pin.pin)?;
        let retention = retention.ok_or_else(|| DurableError::Integrity {
            code: "virtual_archive_retention_missing".to_owned(),
            message: "Virtual archive pin lost its retention current".to_owned(),
        })?;
        let current_pin = current_pin.ok_or_else(|| DurableError::Integrity {
            code: "virtual_archive_pin_disappeared".to_owned(),
            message: "Virtual archive pin disappeared from the pinned root".to_owned(),
        })?;
        let release = operation.command.release(&current_pin.pin)?;
        let receipt = resource_protocol::reduce_resource_release_receipt(
            &operation.command.command_id,
            &release.release_id,
            &release.pin_id,
            &current_pin.pin.owner,
            &retention,
            &current_pin,
        )?;
        Ok(virtual_protocol::VirtualOperationAuthority::RetireArchive { release, receipt })
    }

    fn read_resource_lifecycle_sources(
        &mut self,
        pin: &resource_protocol::ResourcePin,
    ) -> DurableResult<(
        Option<resource_protocol::ResourceRetentionCurrent>,
        Option<resource_protocol::ResourcePinCurrent>,
    )> {
        self.read_current_state_root(|manifest, resolver| {
            let retention = crate::state_root::load_resource_retention_current(
                manifest,
                resolver,
                &pin.subject.family.retention_key,
            )?;
            if let Some(current) = &retention {
                verify_resource_retention_origin(manifest, resolver, current)?;
            }
            let current_pin =
                crate::state_root::load_resource_pin_current(manifest, resolver, &pin.pin_id)?;
            if let Some(current) = &current_pin {
                verify_resource_pin_origin(manifest, resolver, current)?;
            }
            Ok((retention, current_pin))
        })
    }

    fn virtual_resource_postcondition(
        &mut self,
        postcondition: &virtual_protocol::VirtualPostcondition,
    ) -> DurableResult<Vec<DurableOperation>> {
        let post = match (&postcondition.archive_pin, &postcondition.archive_release) {
            (None, None) => return Ok(Vec::new()),
            (Some(receipt), None) => {
                let (retention, pin) = self.read_resource_lifecycle_sources(&receipt.pin)?;
                let origin =
                    resource_protocol::ResourceLifecycleReceiptRef::from_virtual_compaction(
                        &postcondition.receipt,
                    )?;
                resource_protocol::project_resource_pin_receipt(
                    receipt,
                    origin,
                    retention.as_ref(),
                    pin.as_ref(),
                )?
            }
            (None, Some(receipt)) => {
                let (retention, pin) = self.read_resource_lifecycle_sources(&receipt.pin)?;
                let retention = retention.ok_or_else(|| DurableError::Integrity {
                    code: "virtual_retirement_retention_missing".to_owned(),
                    message: "Virtual retirement lost its exact retention source".to_owned(),
                })?;
                let pin = pin.ok_or_else(|| DurableError::Integrity {
                    code: "virtual_retirement_pin_missing".to_owned(),
                    message: "Virtual retirement lost its exact pin source".to_owned(),
                })?;
                let origin = resource_protocol::ResourceLifecycleReceiptRef::from_virtual_archive_retirement(&postcondition.receipt)?;
                resource_protocol::project_resource_release_receipt(
                    receipt, origin, &retention, &pin,
                )?
            }
            (Some(_), Some(_)) => {
                return Err(DurableError::Integrity {
                    code: "virtual_resource_disposition_conflict".to_owned(),
                    message: "one Virtual transition both pins and releases an archive".to_owned(),
                });
            }
        };
        Ok(vec![
            DurableOperation::PutResourceRetentionCurrent {
                value: post.retention,
            },
            DurableOperation::PutResourcePinCurrent { value: post.pin },
        ])
    }

    fn prepare_virtual_claim(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        persistence: &virtual_protocol::VirtualClaimPersistenceCommand,
        binding: &ExecutionBinding,
        observation: ClockObservation,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<virtual_protocol::VirtualOperationAuthority> {
        binding.verify()?;
        let binding_record = cymule_core::ArtifactRecord {
            reference: binding.artifact_ref()?,
            bytes: cymule_core::canonical_bytes(binding)?,
        };
        binding_record.validate()?;
        if binding_record.reference != persistence.command.execution_binding {
            return Err(DurableError::Validation(
                "Virtual claim binding differs from its runtime admission".to_owned(),
            ));
        }
        let preview =
            self.satisfy_virtual_reads(command.scheduler_id(), preparation, |source| {
                virtual_protocol::preview_virtual_claim(persistence, source)
            })?;
        let lease = self.read_current_state_root(|manifest, resolver| {
            let previous: Option<CoordinationLease> =
                crate::state_root::load_typed_state_map_value(
                    &manifest.roots().leases,
                    &persistence.command.slot_id,
                    crate::StateRootLeafKind::Lease,
                    resolver,
                )?;
            proposed_pinned_lease(
                previous.as_ref(),
                &persistence.command.slot_id,
                &persistence.command.owner,
                observation.logical_time,
                persistence.command.lease_ttl,
            )
        })?;
        preparation.artifacts.push(binding_record.clone());
        let (execution, evolution_selection) = match preview {
            None => (
                virtual_protocol::VirtualExecutionAuthority::binding_only(binding_record)?,
                None,
            ),
            Some(preview) => {
                let work_key = virtual_protocol::virtual_work_key(
                    command.scheduler_id(),
                    &preview.item.work_id,
                )?;
                self.load_virtual_preparation_read(
                    command.scheduler_id(),
                    preparation,
                    virtual_protocol::VirtualStateFamily::Work,
                    &work_key,
                )?;
                let occurrence_id =
                    preview.occurrence_id(preparation.work(&preview.item.work_id)?)?;
                let (plan, evolution_selection) = self.prepare_virtual_claim_plan(
                    command,
                    &preview,
                    &occurrence_id,
                    &binding_record,
                    preparation,
                )?;
                preparation.operations.push(DurableOperation::PutLease {
                    value: lease.clone(),
                });
                preparation.claim_plan = Some(plan.clone());
                (
                    virtual_protocol::VirtualExecutionAuthority::new(
                        &preview.item.run_id,
                        plan,
                        binding_record,
                    )?,
                    evolution_selection,
                )
            }
        };
        Ok(virtual_protocol::VirtualOperationAuthority::Claim {
            clock: observation,
            lease: virtual_protocol::VirtualClaimLease {
                resource: lease.resource,
                owner: lease.owner,
                epoch: lease.epoch,
                expires_at: lease.expires_at,
                clock: persistence.command.clock.clone(),
            },
            execution,
            evolution_selection,
        })
    }

    fn prepare_virtual_claim_plan(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        preview: &virtual_protocol::VirtualClaimPreview,
        occurrence_id: &str,
        binding: &cymule_core::ArtifactRecord,
        preparation: &mut VirtualCommandPreparation,
    ) -> DurableResult<(
        SealedPlan,
        Option<virtual_protocol::VirtualEvolutionSelectionLink>,
    )> {
        Ok(match &preview.execution {
            virtual_protocol::VirtualRunExecution::Direct { plan_id } => {
                let plan = self.read_current_state_root(|manifest, resolver| {
                    crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                        .plan(plan_id)?
                        .ok_or_else(|| {
                            DurableError::NotFound(format!(
                                "Virtual selected Plan {plan_id} does not exist"
                            ))
                        })
                })?;
                (plan, None)
            }
            virtual_protocol::VirtualRunExecution::Evolution { evolution_id, .. } => {
                let postcondition = self.prepare_virtual_evolution(
                    command,
                    &preview.execution,
                    evolution_id,
                    occurrence_id,
                    binding,
                )?;
                let evolution_protocol::LiveEvolutionOutcome::OccurrenceSelected { pin } =
                    &postcondition.receipt.outcome
                else {
                    return Err(DurableError::Integrity {
                        code: "virtual_evolution_selection_outcome_mismatch".to_owned(),
                        message: "Virtual evolution selection returned another outcome".to_owned(),
                    });
                };
                let plan_id = pin.plan_id.clone();
                let plan = self.read_current_state_root(|manifest, resolver| {
                    crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                        .plan(&plan_id)?
                        .ok_or_else(|| DurableError::Integrity {
                            code: "virtual_evolution_plan_missing".to_owned(),
                            message: "Evolution selection lost its exact Plan".to_owned(),
                        })
                })?;
                let link = virtual_protocol::VirtualEvolutionSelectionLink {
                    evolution_current: postcondition.current.clone(),
                    receipt_id: postcondition.receipt.receipt_id.clone(),
                    pin: pin.clone(),
                };
                preparation
                    .operations
                    .extend(Self::evolution_postcondition_operations(&postcondition));
                preparation.plans.extend(postcondition.plans);
                preparation.artifacts.extend(postcondition.artifacts);
                (plan, Some(link))
            }
        })
    }

    fn prepare_virtual_evolution(
        &mut self,
        command: &virtual_protocol::VirtualPersistenceCommand,
        execution: &virtual_protocol::VirtualRunExecution,
        evolution_id: &str,
        occurrence_id: &str,
        binding: &cymule_core::ArtifactRecord,
    ) -> DurableResult<evolution_protocol::EvolutionPostcondition> {
        self.read_current_state_root(|manifest, resolver| {
            let current =
                crate::state_root::load_evolution_current(manifest, resolver, evolution_id)?;
            let mut view = evolution_protocol::EvolutionAuthorityView::new(evolution_id, current)?;
            loop {
                match evolution_protocol::prepare_virtual_evolution_selection(
                    &view,
                    execution,
                    &command.persistence_id,
                    occurrence_id,
                    &binding.reference,
                ) {
                    Ok(prepared) => {
                        let postcondition = evolution_protocol::reduce_evolution_selection(
                            prepared,
                            binding.clone(),
                        )?;
                        postcondition.verify()?;
                        return Ok(postcondition);
                    }
                    Err(evolution_protocol::EvolutionError::ReadRequired {
                        family,
                        storage_key,
                    }) => {
                        Self::load_evolution_required_read(
                            manifest,
                            resolver,
                            &mut view,
                            family,
                            storage_key,
                        )?;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        })
    }

    pub(crate) fn claim_virtual(
        &mut self,
        command: &virtual_protocol::VirtualClaimPersistenceCommand,
        clock: &mut dyn crate::ExecutionClockAuthority,
        binding: &ExecutionBinding,
    ) -> DurableResult<virtual_protocol::VirtualClaimOutcome> {
        let persistence = virtual_protocol::VirtualPersistenceCommand::new(
            virtual_protocol::VirtualPersistenceOperation::Claim(command.clone()),
        )?;
        if let Some(outcome) = self.read_current_state_root(|manifest, resolver| {
            load_virtual_claim_replay(manifest, resolver, &persistence)
        })? {
            return Ok(outcome);
        }
        let result =
            self.with_current_clock(clock, &command.command.clock, |coordinator, observation| {
                coordinator.commit_virtual_claim_observed(command, binding, observation)
            })?;
        virtual_claim_outcome(result.commit.receipt, result.claim_plan)
    }

    fn prepare_pinned_command_batch(
        &mut self,
        mut commands: Vec<cymule_core::durable_internal::MachinePinnedBatchCommand>,
        material: Option<cymule_core::durable_internal::MachineMaterialAdmission>,
        start_material: Option<cymule_core::durable_internal::MachineStartRunMaterial>,
    ) -> DurableResult<crate::state_root::pinned_machine::PinnedMachineBatchOutcome> {
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        let outcome = self.store.with_state_root_resolver(&manifest, |resolver| {
            let mut view =
                crate::state_root::pinned_machine::PinnedMachineView::open(&manifest, resolver)?;
            match start_material {
                Some(start) => {
                    if commands.len() != 1 || material.is_some() {
                        return Err(DurableError::Validation(
                            "StartRun requires its exact singleton material batch".to_owned(),
                        ));
                    }
                    let command = commands.pop().ok_or_else(|| DurableError::Integrity {
                        code: "start_batch_command_missing".to_owned(),
                        message: "StartRun batch lost its command".to_owned(),
                    })?;
                    view.prepare_start_run_batch(command, start)
                }
                None => view.prepare_command_batch(commands, material),
            }
        })?;
        let outcome = match outcome {
            crate::state_root::pinned_machine::PinnedMachineBatchOutcome::NeedsArchive(request) => {
                let lookups = request
                    .command_ids()
                    .iter()
                    .map(|command_id| {
                        self.store
                            .lookup_machine_command_archive(request.anchor(), command_id)
                    })
                    .collect::<DurableResult<Vec<_>>>()?;
                self.store.with_state_root_resolver(&manifest, |resolver| {
                    request.finish(&manifest, lookups, resolver)
                })?
            }
            outcome => outcome,
        };
        let crate::state_root::pinned_machine::PinnedMachineBatchOutcome::NeedsArchivedBatch(
            request,
        ) = outcome
        else {
            return Ok(outcome);
        };
        let batch_id = request.batch_id().to_owned();
        let batch = self
            .store
            .load_machine_command_archive_batch(&batch_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "machine_command_archive_batch_missing".to_owned(),
                message: format!("archived Machine batch {batch_id} does not exist"),
            })?;
        self.store.with_state_root_resolver(&manifest, |resolver| {
            request.finish(&manifest, batch, resolver)
        })
    }

    fn continue_pinned_command(
        &mut self,
        transition: &cymule_core::durable_internal::MachinePagedTransitionCurrent,
    ) -> DurableResult<crate::state_root::pinned_machine::PinnedMachinePagedOutcome> {
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        let outcome = self.store.with_state_root_resolver(&manifest, |resolver| {
            crate::state_root::pinned_machine::PinnedMachineView::open(&manifest, resolver)?
                .continue_paged(transition)
        })?;
        let crate::state_root::pinned_machine::PinnedMachinePagedOutcome::NeedsArchive(request) =
            outcome
        else {
            return Ok(outcome);
        };
        let lookup = self
            .store
            .lookup_machine_command_archive(request.anchor(), request.command_id())?;
        self.store.with_state_root_resolver(&manifest, |resolver| {
            request.finish(&manifest, lookup, resolver)
        })
    }

    fn publish_pinned_stage(
        &mut self,
        stage: crate::state_root::pinned_machine::PinnedMachineStagedMutation,
        sidecar: Option<DurableDelta>,
    ) -> DurableResult<(
        crate::state_root::pinned_machine::PinnedMachineStageTransition,
        String,
    )> {
        let pinned = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .clone();
        let prepared = self
            .store
            .with_state_root_resolver(&pinned.manifest, |resolver| {
                stage.finish(&pinned.manifest, sidecar.as_ref(), resolver)
            })?;
        let stage_digest = prepared.stage_digest().to_owned();
        let (transition, state_root_transition) = prepared.into_parts();
        let archives = transition.archive_segments().to_vec();
        let batch = StoreBatch::transition_pinned(
            pinned.revision(),
            &pinned.head,
            stage_digest,
            sidecar,
            state_root_transition,
            archives,
        )?;
        let commit = self.store.compare_and_commit(Some(&pinned.head), &batch)?;
        batch.verify_commit(&commit)?;
        self.pinned = Some(PinnedHead::new(
            batch.head().clone(),
            batch.state_root_transition().manifest().clone(),
        )?);
        Ok((transition, commit.revision))
    }

    fn commit_material_sidecars(
        &mut self,
        material: &cymule_core::durable_internal::MachineMaterialAdmission,
        outer_receipt_digest: &str,
        operations: Vec<DurableOperation>,
    ) -> DurableResult<String> {
        let pinned = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .clone();
        let stage = self
            .store
            .with_state_root_resolver(&pinned.manifest, |resolver| {
                crate::state_root::pinned_machine::PinnedMachineView::open(
                    &pinned.manifest,
                    resolver,
                )?
                .prepare_material_admission(material, outer_receipt_digest)
            })?;
        let sidecar = DurableDelta::new(operations)?;
        let (transition, revision) = self.publish_pinned_stage(stage, Some(sidecar))?;
        let crate::state_root::pinned_machine::PinnedMachineStageTransition::Material {
            prepared,
            outer_receipt_digest: retained_outer,
        } = transition
        else {
            return Err(DurableError::RuntimeDefect {
                code: "material_stage_shape_mismatch".to_owned(),
                message: "Machine material admission published another stage kind".to_owned(),
            });
        };
        if prepared.source_command_id != material.source_command_id()
            || prepared.material_digest != material.material_digest()
            || retained_outer != outer_receipt_digest
        {
            return Err(DurableError::Integrity {
                code: "material_stage_authority_mismatch".to_owned(),
                message: "Machine material stage changed its source, material, or outer receipt"
                    .to_owned(),
            });
        }
        Ok(revision)
    }

    fn commit_pinned_command_batch<F>(
        &mut self,
        commands: Vec<cymule_core::durable_internal::MachinePinnedBatchCommand>,
        material: Option<cymule_core::durable_internal::MachineMaterialAdmission>,
        sidecars: F,
    ) -> DurableResult<PinnedBatchCommit>
    where
        F: FnOnce(
            &cymule_core::durable_internal::PinnedMachineBatchTransition,
        ) -> DurableResult<Vec<DurableOperation>>,
    {
        let outcome = self.prepare_pinned_command_batch(commands, material, None)?;
        self.commit_prepared_pinned_batch(outcome, sidecars)
    }

    fn commit_prepared_pinned_batch<F>(
        &mut self,
        mut outcome: crate::state_root::pinned_machine::PinnedMachineBatchOutcome,
        sidecars: F,
    ) -> DurableResult<PinnedBatchCommit>
    where
        F: FnOnce(
            &cymule_core::durable_internal::PinnedMachineBatchTransition,
        ) -> DurableResult<Vec<DurableOperation>>,
    {
        use crate::state_root::pinned_machine::{
            PinnedMachineBatchOutcome, PinnedMachinePagedOutcome, PinnedMachineStageTransition,
        };

        let mut final_sidecars = Some(sidecars);
        loop {
            match outcome {
                PinnedMachineBatchOutcome::Replay(replay) => {
                    return Ok(PinnedBatchCommit {
                        batch_id: replay.batch_id,
                        batch_receipt_id: replay.batch_receipt_id,
                        receipts: replay.receipts,
                        committed_revision: None,
                    });
                }
                PinnedMachineBatchOutcome::Staged(stage) => {
                    let transition =
                        stage
                            .batch_transition()
                            .ok_or_else(|| DurableError::RuntimeDefect {
                                code: "pinned_batch_transition_missing".to_owned(),
                                message: "fresh pinned batch stage lost its aggregate transition"
                                    .to_owned(),
                            })?;
                    let operations =
                        final_sidecars
                            .take()
                            .ok_or_else(|| DurableError::Integrity {
                                code: "pinned_batch_sidecars_reused".to_owned(),
                                message: "pinned batch attempted to derive final sidecars twice"
                                    .to_owned(),
                            })?(transition)?;
                    let sidecar = (!operations.is_empty())
                        .then(|| DurableDelta::new(operations))
                        .transpose()?;
                    let (published, revision) = self.publish_pinned_stage(stage, sidecar)?;
                    let PinnedMachineStageTransition::Batch(transition) = published else {
                        return Err(DurableError::RuntimeDefect {
                            code: "pinned_batch_final_shape_mismatch".to_owned(),
                            message: "fresh pinned batch published another stage kind".to_owned(),
                        });
                    };
                    return Ok(PinnedBatchCommit {
                        batch_id: transition.batch.batch_id.clone(),
                        batch_receipt_id: transition.batch.batch_receipt_id.clone(),
                        receipts: transition.batch.receipts.clone(),
                        committed_revision: Some(revision),
                    });
                }
                PinnedMachineBatchOutcome::NeedsArchive(_)
                | PinnedMachineBatchOutcome::NeedsArchivedBatch(_) => {
                    return Err(DurableError::RuntimeDefect {
                        code: "pinned_batch_archive_request_escaped".to_owned(),
                        message: "resolved pinned batch returned another archive request"
                            .to_owned(),
                    });
                }
                PinnedMachineBatchOutcome::PagedBegin(stage) => {
                    let (published, _) = self.publish_pinned_stage(stage, None)?;
                    let PinnedMachineStageTransition::PagedBegin(begin) = published else {
                        return Err(DurableError::Integrity {
                            code: "pinned_batch_begin_shape_mismatch".to_owned(),
                            message: "paged batch begin published another stage kind".to_owned(),
                        });
                    };
                    outcome = PinnedMachineBatchOutcome::Pending(begin.transition);
                }
                PinnedMachineBatchOutcome::Pending(transition) => {
                    outcome = match self.continue_pinned_command(&transition)? {
                        PinnedMachinePagedOutcome::Progress(stage) => {
                            let (published, _) = self.publish_pinned_stage(stage, None)?;
                            let PinnedMachineStageTransition::PagedProgress(progress) = published
                            else {
                                return Err(DurableError::Integrity {
                                    code: "pinned_batch_progress_shape_mismatch".to_owned(),
                                    message: "paged batch progress published another stage kind"
                                        .to_owned(),
                                });
                            };
                            PinnedMachineBatchOutcome::Pending(progress.transition)
                        }
                        PinnedMachinePagedOutcome::Final(stage) => {
                            PinnedMachineBatchOutcome::Staged(stage)
                        }
                        PinnedMachinePagedOutcome::NeedsArchive(_) => {
                            return Err(DurableError::Integrity {
                                code: "pinned_batch_paged_archive_request_escaped".to_owned(),
                                message: "resolved paged batch returned another archive request"
                                    .to_owned(),
                            });
                        }
                    };
                }
            }
        }
    }

    fn commit_pinned_command<F>(
        &mut self,
        envelope: CommandEnvelope,
        start_material: Option<cymule_core::durable_internal::MachineStartRunMaterial>,
        sidecars: F,
    ) -> DurableResult<PinnedCommandCommit>
    where
        F: FnOnce(
            &cymule_core::durable_internal::PinnedMachineTransition,
        ) -> DurableResult<Vec<DurableOperation>>,
    {
        let command_id = envelope.command_id.clone();
        let member = cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id: envelope.command_id,
            actor: envelope.actor,
            run_id: envelope.run_id,
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                envelope.expected_precondition,
            ),
            command: envelope.command,
        };
        let prepared = self.prepare_pinned_command_batch(vec![member], None, start_material)?;
        let commit = self.commit_prepared_pinned_batch(prepared, move |batch| {
            let [step] = batch.steps.as_slice() else {
                return Err(DurableError::Integrity {
                    code: "single_command_batch_step_count".to_owned(),
                    message: "single command admission returned a non-singleton batch".to_owned(),
                });
            };
            let [receipt] = batch.batch.receipts.as_slice() else {
                return Err(DurableError::Integrity {
                    code: "single_command_batch_receipt_count".to_owned(),
                    message: "single command admission returned a non-singleton receipt".to_owned(),
                });
            };
            let mut delta = step.clone();
            delta.machine = batch.machine.clone();
            sidecars(&cymule_core::durable_internal::PinnedMachineTransition {
                receipt: receipt.clone(),
                frontier: batch.frontier.clone(),
                delta,
            })
        })?;
        let [receipt] = commit.receipts.as_slice() else {
            return Err(DurableError::Integrity {
                code: "single_command_replay_receipt_count".to_owned(),
                message: "single command replay returned a non-singleton receipt".to_owned(),
            });
        };
        if receipt.command_id != command_id {
            return Err(DurableError::Integrity {
                code: "single_command_replay_identity".to_owned(),
                message: "single command replay changed its command identity".to_owned(),
            });
        }
        Ok(PinnedCommandCommit {
            receipt: receipt.clone(),
            committed_revision: commit.committed_revision,
        })
    }

    fn current_revision(&self) -> DurableResult<&str> {
        self.revision()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))
    }

    fn refresh_pinned_head(&mut self) -> DurableResult<()> {
        let current = load_pinned_head(&mut self.store)?
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?;
        self.pinned = Some(current);
        Ok(())
    }

    /// Admit one closed Resource-profile command against the exact current
    /// keyed projections and publish its complete postcondition in one CAS.
    pub(crate) fn commit_resource(
        &mut self,
        command: &resource_protocol::ResourceCommand,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        command.verify()?;
        if let Some(existing) = self.resource_command_receipt(&command.command_id)? {
            if existing.command != *command {
                return Err(DurableError::HistoryConflict {
                    code: "resource_command_reused".to_owned(),
                    message: format!(
                        "Resource command {} was reused with different semantics",
                        command.command_id
                    ),
                });
            }
            self.verify_resource_command_replay(&existing)?;
            return Ok(existing);
        }
        match &command.operation {
            resource_protocol::ResourceOperation::Pin { pin } => {
                self.commit_resource_pin(command, pin)
            }
            resource_protocol::ResourceOperation::Release {
                release_id,
                pin_id,
                owner,
            } => self.commit_resource_release(command, release_id, pin_id, owner),
            resource_protocol::ResourceOperation::GarbageCollect { gc_id, family } => {
                self.commit_resource_gc(command, gc_id, family)
            }
            resource_protocol::ResourceOperation::BeginDelete {
                delete_id,
                gc_command_id,
                gc_receipt_id,
                target,
            } => self.commit_resource_begin_delete(
                command,
                delete_id,
                gc_command_id,
                gc_receipt_id,
                target,
            ),
            resource_protocol::ResourceOperation::ReconcileDelete { .. } => {
                Err(DurableError::Validation(
                    "Resource deletion reconciliation requires the provider-bound coordinator API"
                        .to_owned(),
                ))
            }
            resource_protocol::ResourceOperation::Transfer { handoff } => {
                self.commit_resource_transfer(command, handoff)
            }
            resource_protocol::ResourceOperation::ActivateTransfer {
                activation,
                source_receipt_id,
            } => self.commit_resource_activation(command, activation, source_receipt_id),
        }
    }

    /// Execute and durably close one exact provider-bound Resource deletion.
    ///
    /// Exact receipt replay returns before provider I/O. After the provider
    /// proves absence, any unacknowledged durable commit is reported as an
    /// unknown outcome so callers reopen and resolve the retained receipt.
    pub(crate) fn reconcile_resource_delete(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        deleter: &mut impl resource_protocol::ResourceDeleter,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        command.verify()?;
        let resource_protocol::ResourceOperation::ReconcileDelete {
            delete_id,
            intent_id,
        } = &command.operation
        else {
            return Err(DurableError::Validation(
                "provider-bound Resource reconciliation accepts only ReconcileDelete".to_owned(),
            ));
        };
        if let Some(existing) = self.resource_command_receipt(&command.command_id)? {
            if existing.command != *command {
                return Err(DurableError::HistoryConflict {
                    code: "resource_reconcile_command_reused".to_owned(),
                    message: format!(
                        "Resource reconciliation command {} was reused with different semantics",
                        command.command_id
                    ),
                });
            }
            self.verify_resource_command_replay(&existing)?;
            return Ok(existing);
        }
        let (delete, retention) = self.read_current_state_root(|manifest, resolver| {
            let delete =
                crate::state_root::load_resource_delete_current(manifest, resolver, delete_id)?
                    .ok_or_else(|| {
                        DurableError::NotFound(format!(
                            "Resource delete {delete_id} does not exist"
                        ))
                    })?;
            verify_resource_delete_origin(manifest, resolver, &delete)?;
            let retention = crate::state_root::load_resource_retention_current(
                manifest,
                resolver,
                &delete.intent.target.subject.family.retention_key,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "resource_delete_retention_missing".to_owned(),
                message: format!(
                    "Resource delete {delete_id} lost its physical retention projection"
                ),
            })?;
            verify_resource_retention_origin(manifest, resolver, &retention)?;
            Ok((delete, retention))
        })?;
        if deleter.binding() != delete.intent.target.subject.family.store_binding {
            return Err(DurableError::Validation(format!(
                "Resource delete {delete_id} requires provider binding {}",
                delete.intent.target.subject.family.store_binding
            )));
        }
        let deletion_receipt = resource_protocol::reduce_resource_reconcile_delete_receipt(
            &command.command_id,
            delete_id,
            intent_id,
            &retention,
            &delete,
        )?;
        let receipt = resource_protocol::ResourceCommandReceipt::new(
            command.clone(),
            resource_protocol::ResourceCommandOutcome::ReconcileDelete {
                receipt: deletion_receipt,
            },
        )?;
        let origin = resource_protocol::ResourceLifecycleReceiptRef::from_resource(&receipt)?;
        let post = resource_protocol::project_resource_reconcile_delete_receipt(
            match &receipt.outcome {
                resource_protocol::ResourceCommandOutcome::ReconcileDelete { receipt } => receipt,
                _ => unreachable!("reconciliation receipt was constructed with its exact outcome"),
            },
            origin,
            &retention,
            &delete,
        )?;
        deleter.delete_and_verify_absent(&post.deletion.intent.target)?;
        let operations = vec![
            DurableOperation::PutResourceCommandReceipt {
                value: receipt.clone(),
            },
            DurableOperation::PutResourceRetentionCurrent {
                value: post.retention,
            },
            DurableOperation::PutResourceDeleteCurrent {
                value: post.deletion,
            },
        ];
        self.commit_profile_operations(operations)
            .map_err(|error| DurableError::CommitOutcomeUnknown {
                message: format!(
                    "Resource delete {delete_id} was verified absent but its terminal durable commit was not acknowledged: {error}"
                ),
            })?;
        Ok(receipt)
    }

    fn commit_resource_pin(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        pin: &resource_protocol::ResourcePin,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        let (retention, current_pin) = self.read_current_state_root(|manifest, resolver| {
            let retention = crate::state_root::load_resource_retention_current(
                manifest,
                resolver,
                &pin.subject.family.retention_key,
            )?;
            if let Some(current) = &retention {
                verify_resource_retention_origin(manifest, resolver, current)?;
            }
            let current_pin =
                crate::state_root::load_resource_pin_current(manifest, resolver, &pin.pin_id)?;
            if let Some(current) = &current_pin {
                verify_resource_pin_origin(manifest, resolver, current)?;
            }
            Ok((retention, current_pin))
        })?;
        let pin_receipt = resource_protocol::reduce_resource_pin_receipt(
            &command.command_id,
            pin,
            retention.as_ref(),
            current_pin.as_ref(),
        )?;
        let receipt = resource_protocol::ResourceCommandReceipt::new(
            command.clone(),
            resource_protocol::ResourceCommandOutcome::Pin {
                receipt: pin_receipt.clone(),
            },
        )?;
        let origin = resource_protocol::ResourceLifecycleReceiptRef::from_resource(&receipt)?;
        let post = resource_protocol::project_resource_pin_receipt(
            &pin_receipt,
            origin,
            retention.as_ref(),
            current_pin.as_ref(),
        )?;
        self.commit_profile_operations(vec![
            DurableOperation::PutResourceCommandReceipt {
                value: receipt.clone(),
            },
            DurableOperation::PutResourceRetentionCurrent {
                value: post.retention,
            },
            DurableOperation::PutResourcePinCurrent { value: post.pin },
        ])?;
        Ok(receipt)
    }

    fn commit_resource_release(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        release_id: &str,
        pin_id: &str,
        owner: &str,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        let (retention, current_pin) = self.read_current_state_root(|manifest, resolver| {
            let current_pin =
                crate::state_root::load_resource_pin_current(manifest, resolver, pin_id)?
                    .ok_or_else(|| DurableError::NotFound(format!("Resource pin {pin_id}")))?;
            verify_resource_pin_origin(manifest, resolver, &current_pin)?;
            let retention = crate::state_root::load_resource_retention_current(
                manifest,
                resolver,
                &current_pin.pin.subject.family.retention_key,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "resource_pin_retention_missing".to_owned(),
                message: format!("Resource pin {pin_id} lost its retention projection"),
            })?;
            verify_resource_retention_origin(manifest, resolver, &retention)?;
            Ok((retention, current_pin))
        })?;
        if current_pin.pin.kind != resource_protocol::ResourcePinKind::Explicit {
            return Err(DurableError::HistoryConflict {
                code: "resource_profile_pin_release_forbidden".to_owned(),
                message: format!(
                    "Resource pin {pin_id} can be released only by its owning profile"
                ),
            });
        }
        let release_receipt = resource_protocol::reduce_resource_release_receipt(
            &command.command_id,
            release_id,
            pin_id,
            owner,
            &retention,
            &current_pin,
        )?;
        let receipt = resource_protocol::ResourceCommandReceipt::new(
            command.clone(),
            resource_protocol::ResourceCommandOutcome::Release {
                receipt: release_receipt.clone(),
            },
        )?;
        let origin = resource_protocol::ResourceLifecycleReceiptRef::from_resource(&receipt)?;
        let post = resource_protocol::project_resource_release_receipt(
            &release_receipt,
            origin,
            &retention,
            &current_pin,
        )?;
        self.commit_profile_operations(vec![
            DurableOperation::PutResourceCommandReceipt {
                value: receipt.clone(),
            },
            DurableOperation::PutResourceRetentionCurrent {
                value: post.retention,
            },
            DurableOperation::PutResourcePinCurrent { value: post.pin },
        ])?;
        Ok(receipt)
    }

    fn commit_resource_gc(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        gc_id: &str,
        family: &resource_protocol::ResourceRetentionFamily,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        let retention = self.read_current_state_root(|manifest, resolver| {
            let retention = crate::state_root::load_resource_retention_current(
                manifest,
                resolver,
                &family.retention_key,
            )?;
            if let Some(current) = &retention {
                verify_resource_retention_origin(manifest, resolver, current)?;
            }
            Ok(retention)
        })?;
        let gc_receipt = resource_protocol::reduce_resource_gc_receipt(
            &command.command_id,
            gc_id,
            family,
            retention.as_ref(),
        )?;
        let receipt = resource_protocol::ResourceCommandReceipt::new(
            command.clone(),
            resource_protocol::ResourceCommandOutcome::GarbageCollect {
                receipt: gc_receipt,
            },
        )?;
        self.commit_profile_operations(vec![DurableOperation::PutResourceCommandReceipt {
            value: receipt.clone(),
        }])?;
        Ok(receipt)
    }

    fn commit_resource_begin_delete(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        delete_id: &str,
        gc_command_id: &str,
        gc_receipt_id: &str,
        target: &resource_protocol::ResourceDeletionTarget,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        let (gc, retention, current_delete) =
            self.read_current_state_root(|manifest, resolver| {
                let gc = crate::state_root::load_resource_command_receipt(
                    manifest,
                    resolver,
                    gc_command_id,
                )?
                .ok_or_else(|| {
                    DurableError::NotFound(format!(
                        "Resource GC command {gc_command_id} does not exist"
                    ))
                })?;
                gc.verify()?;
                let retention = crate::state_root::load_resource_retention_current(
                    manifest,
                    resolver,
                    &target.subject.family.retention_key,
                )?;
                if let Some(current) = &retention {
                    verify_resource_retention_origin(manifest, resolver, current)?;
                }
                let current_delete =
                    crate::state_root::load_resource_delete_current(manifest, resolver, delete_id)?;
                if let Some(current) = &current_delete {
                    verify_resource_delete_origin(manifest, resolver, current)?;
                }
                Ok((gc, retention, current_delete))
            })?;
        let gc_receipt = match &gc.outcome {
            resource_protocol::ResourceCommandOutcome::GarbageCollect { receipt }
                if gc.command.command_id == gc_command_id
                    && receipt.command_id == gc_command_id
                    && receipt.receipt_id == gc_receipt_id =>
            {
                receipt
            }
            _ => {
                return Err(DurableError::HistoryConflict {
                    code: "resource_delete_gc_receipt_mismatch".to_owned(),
                    message: format!(
                        "Resource deletion {delete_id} does not consume an exact GC receipt"
                    ),
                });
            }
        };
        let intent = resource_protocol::reduce_resource_begin_delete_intent(
            &command.command_id,
            delete_id,
            gc_receipt,
            target,
            retention.as_ref(),
            current_delete.as_ref(),
        )?;
        let receipt = resource_protocol::ResourceCommandReceipt::new(
            command.clone(),
            resource_protocol::ResourceCommandOutcome::BeginDelete {
                intent: intent.clone(),
            },
        )?;
        let origin = resource_protocol::ResourceLifecycleReceiptRef::from_resource(&receipt)?;
        let post = resource_protocol::project_resource_begin_delete_intent(
            &intent,
            gc_receipt,
            origin,
            retention.as_ref(),
            current_delete.as_ref(),
        )?;
        self.commit_profile_operations(vec![
            DurableOperation::PutResourceCommandReceipt {
                value: receipt.clone(),
            },
            DurableOperation::PutResourceRetentionCurrent {
                value: post.retention,
            },
            DurableOperation::PutResourceDeleteCurrent {
                value: post.deletion,
            },
        ])?;
        Ok(receipt)
    }

    fn verify_resource_command_replay(
        &mut self,
        receipt: &resource_protocol::ResourceCommandReceipt,
    ) -> DurableResult<()> {
        receipt.verify()?;
        match &receipt.outcome {
            resource_protocol::ResourceCommandOutcome::Pin {
                receipt: pin_receipt,
            } => {
                let current = self
                    .resource_pin_current(&pin_receipt.pin.pin_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "resource_pin_replay_current_missing".to_owned(),
                        message: format!(
                            "Resource pin receipt {} lost its current projection",
                            pin_receipt.receipt_id
                        ),
                    })?;
                if current.pin != pin_receipt.pin {
                    return Err(DurableError::Integrity {
                        code: "resource_pin_replay_current_mismatch".to_owned(),
                        message: format!(
                            "Resource pin receipt {} no longer selects its exact pin",
                            pin_receipt.receipt_id
                        ),
                    });
                }
            }
            resource_protocol::ResourceCommandOutcome::Release {
                receipt: release_receipt,
            } => {
                self.verify_resource_release_replay(receipt, release_receipt)?;
            }
            resource_protocol::ResourceCommandOutcome::GarbageCollect { .. } => {}
            resource_protocol::ResourceCommandOutcome::BeginDelete { intent } => {
                let current = self
                    .resource_delete_current(&intent.delete_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "resource_delete_replay_current_missing".to_owned(),
                        message: format!(
                            "Resource delete intent {} lost its current projection",
                            intent.intent_id
                        ),
                    })?;
                if current.intent != *intent {
                    return Err(DurableError::Integrity {
                        code: "resource_delete_replay_current_mismatch".to_owned(),
                        message: format!(
                            "Resource delete intent {} changed its exact target",
                            intent.intent_id
                        ),
                    });
                }
            }
            resource_protocol::ResourceCommandOutcome::ReconcileDelete {
                receipt: deletion_receipt,
            } => {
                self.verify_resource_deletion_replay(receipt, deletion_receipt)?;
            }
            resource_protocol::ResourceCommandOutcome::Transfer {
                receipt: handoff_receipt,
            } => {
                let current = self
                    .resource_handoff_current(&handoff_receipt.handoff.transfer_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "resource_handoff_replay_current_missing".to_owned(),
                        message: format!(
                            "Resource transfer receipt {} lost its current authority",
                            handoff_receipt.receipt_id
                        ),
                    })?;
                if current.receipt != *handoff_receipt {
                    return Err(DurableError::Integrity {
                        code: "resource_handoff_replay_current_mismatch".to_owned(),
                        message: format!(
                            "Resource transfer receipt {} changed its current authority",
                            handoff_receipt.receipt_id
                        ),
                    });
                }
            }
            resource_protocol::ResourceCommandOutcome::ActivateTransfer {
                receipt: activation_receipt,
            } => self.verify_resource_activation_replay(receipt, activation_receipt)?,
        }
        Ok(())
    }

    fn verify_resource_release_replay(
        &mut self,
        receipt: &resource_protocol::ResourceCommandReceipt,
        release_receipt: &resource_protocol::ResourceReleaseReceipt,
    ) -> DurableResult<()> {
        let current = self
            .resource_pin_current(&release_receipt.pin.pin_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "resource_release_replay_current_missing".to_owned(),
                message: format!(
                    "Resource release receipt {} lost its current projection",
                    release_receipt.receipt_id
                ),
            })?;
        let expected = resource_protocol::ResourceLifecycleReceiptRef::from_resource(receipt)?;
        if current.pin != release_receipt.pin
            || current.status != resource_protocol::ResourcePinStatus::Released
            || current.last_receipt != expected
        {
            return Err(DurableError::Integrity {
                code: "resource_release_replay_current_mismatch".to_owned(),
                message: format!(
                    "Resource release receipt {} does not match its terminal pin projection",
                    release_receipt.receipt_id
                ),
            });
        }
        Ok(())
    }

    fn verify_resource_deletion_replay(
        &mut self,
        receipt: &resource_protocol::ResourceCommandReceipt,
        deletion_receipt: &resource_protocol::ResourceDeleteReceipt,
    ) -> DurableResult<()> {
        let current = self
            .resource_delete_current(&deletion_receipt.intent.delete_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "resource_reconcile_replay_current_missing".to_owned(),
                message: format!(
                    "Resource deletion receipt {} lost its terminal projection",
                    deletion_receipt.receipt_id
                ),
            })?;
        let expected = resource_protocol::ResourceLifecycleReceiptRef::from_resource(receipt)?;
        if current.intent != deletion_receipt.intent
            || current.status != resource_protocol::ResourceDeleteStatus::Completed
            || current.last_receipt != expected
        {
            return Err(DurableError::Integrity {
                code: "resource_reconcile_replay_current_mismatch".to_owned(),
                message: format!(
                    "Resource deletion receipt {} does not match its terminal projection",
                    deletion_receipt.receipt_id
                ),
            });
        }
        Ok(())
    }

    /// Resolve one exact immutable Resource command receipt or typed authority
    /// alias through the current head-pinned `StateRoot`.
    pub(crate) fn resource_command_receipt(
        &mut self,
        authority_id: &str,
    ) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceCommandReceipt>> {
        self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_resource_command_receipt(manifest, resolver, authority_id)
        })
    }

    /// Resolve one exact current Resource pin projection.
    pub(crate) fn resource_pin_current(
        &mut self,
        pin_id: &str,
    ) -> DurableResult<Option<cymule_profile_protocol::resource::ResourcePinCurrent>> {
        self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_resource_pin_current(manifest, resolver, pin_id)?;
            if let Some(current) = &current {
                verify_resource_pin_origin(manifest, resolver, current)?;
            }
            Ok(current)
        })
    }

    /// Resolve one exact current physical Resource retention projection.
    pub(crate) fn resource_retention_current(
        &mut self,
        retention_key: &str,
    ) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceRetentionCurrent>> {
        self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_resource_retention_current(
                manifest,
                resolver,
                retention_key,
            )?;
            if let Some(current) = &current {
                verify_resource_retention_origin(manifest, resolver, current)?;
            }
            Ok(current)
        })
    }

    /// Resolve one exact current Resource deletion projection.
    pub(crate) fn resource_delete_current(
        &mut self,
        delete_id: &str,
    ) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceDeleteCurrent>> {
        self.read_current_state_root(|manifest, resolver| {
            let current =
                crate::state_root::load_resource_delete_current(manifest, resolver, delete_id)?;
            if let Some(current) = &current {
                verify_resource_delete_origin(manifest, resolver, current)?;
            }
            Ok(current)
        })
    }

    /// Resolve one exact immutable Resource handoff authority.
    pub(crate) fn resource_handoff_current(
        &mut self,
        transfer_id: &str,
    ) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceHandoffCurrent>> {
        self.read_current_state_root(|manifest, resolver| {
            let current =
                crate::state_root::load_resource_handoff_current(manifest, resolver, transfer_id)?;
            if let Some(current) = &current {
                verify_resource_handoff_origin(manifest, resolver, current)?;
            }
            Ok(current)
        })
    }

    /// Resolve one exact immutable Resource handoff activation authority.
    pub(crate) fn resource_handoff_activation_current(
        &mut self,
        activation_id: &str,
    ) -> DurableResult<Option<cymule_profile_protocol::resource::ResourceHandoffActivationCurrent>>
    {
        let current = self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_resource_handoff_activation_current(
                manifest,
                resolver,
                activation_id,
            )?;
            if let Some(current) = &current {
                verify_resource_handoff_activation_origin(manifest, resolver, current)?;
            }
            Ok(current)
        })?;
        if let Some(current) = &current {
            let command = self
                .resource_command_receipt(&current.receipt.command_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "resource_activation_command_missing".to_owned(),
                    message: format!(
                        "Resource activation {} lost command {}",
                        current.receipt.activation.activation_id, current.receipt.command_id
                    ),
                })?;
            self.verify_resource_activation_replay(&command, &current.receipt)?;
        }
        Ok(current)
    }

    /// Resolve one bounded contiguous page of exact Resource handoffs for a
    /// target Run.
    pub(crate) fn resource_handoff_page(
        &mut self,
        to_run: &str,
        start_index: u64,
        limit: usize,
    ) -> DurableResult<cymule_profile_protocol::resource::ResourceHandoffPage> {
        self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_resource_handoff_page(
                manifest,
                resolver,
                to_run,
                start_index,
                limit,
            )
        })
    }

    pub(crate) fn cancellation_receipt(
        &mut self,
        cancellation_id: &str,
    ) -> DurableResult<Option<CancellationReceipt>> {
        let receipt = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_cancellation_receipt(manifest, resolver, cancellation_id)
        })?;
        if let Some(receipt) = &receipt {
            let (entry, batch) = self.load_required_terminal_command(cancellation_id)?;
            crate::model::validate_cancellation_receipt_command(receipt, &entry, &batch)?;
        }
        Ok(receipt)
    }

    pub(crate) fn effect_resolution_receipt(
        &mut self,
        resolution_id: &str,
    ) -> DurableResult<Option<EffectResolutionReceipt>> {
        let receipt = self.read_current_state_root(|manifest, resolver| {
            crate::state_root::load_effect_resolution_receipt(manifest, resolver, resolution_id)
        })?;
        if let Some(receipt) = &receipt {
            let (entry, batch) = self.load_required_terminal_command(resolution_id)?;
            crate::model::validate_effect_resolution_receipt_command(receipt, &entry, &batch)?;
        }
        Ok(receipt)
    }

    fn load_required_terminal_command(
        &mut self,
        command_id: &str,
    ) -> DurableResult<(
        cymule_core::MachineCommandArchiveEntry,
        cymule_core::MachineCommandBatchRecord,
    )> {
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        crate::store::load_pinned_machine_command(&mut self.store, &manifest, command_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "terminal_receipt_command_missing".to_owned(),
                message: format!(
                    "terminal receipt {command_id} has no exact retained Core command"
                ),
            })
    }

    /// Consume the store after coordination.
    pub(crate) fn into_store(self) -> S {
        self.store
    }

    /// Commit a closed profile reducer result directly against the pinned
    /// `StateRoot`. This path never materializes `DurableState` and admits no
    /// Machine transition or archive object.
    fn commit_profile_operations(
        &mut self,
        operations: Vec<DurableOperation>,
    ) -> DurableResult<String> {
        let pinned = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .clone();
        let delta = DurableDelta::new(operations)?;
        let transition = self
            .store
            .with_state_root_resolver(&pinned.manifest, |resolver| {
                pinned.manifest.apply(&delta, resolver)
            })?;
        let batch = StoreBatch::transition_prepared(
            pinned.revision(),
            &pinned.head,
            delta,
            transition,
            Vec::new(),
        )?;
        let commit = self.store.compare_and_commit(Some(&pinned.head), &batch)?;
        batch.verify_commit(&commit)?;
        self.pinned = Some(PinnedHead::new(
            batch.head().clone(),
            batch.state_root_transition().manifest().clone(),
        )?);
        Ok(commit.revision)
    }

    fn read_current_state_root<T>(
        &mut self,
        read: impl FnOnce(
            &crate::StateRootManifest,
            &mut dyn crate::StateRootResolver,
        ) -> DurableResult<T>,
    ) -> DurableResult<T> {
        let manifest = self
            .pinned
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .manifest
            .clone();
        self.store
            .with_state_root_resolver(&manifest, |resolver| read(&manifest, resolver))
    }
}

fn load_executor_run_at_manifest(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    run_id: &str,
) -> DurableResult<Option<ExecutorRunRead>> {
    let mut view = crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
    if view.run_current(run_id)?.is_none() {
        if crate::state_root::load_continuation(manifest, resolver, run_id)?.is_some() {
            return Err(DurableError::Integrity {
                code: "executor_orphan_continuation".to_owned(),
                message: format!("Run {run_id} has a Continuation but no Core current"),
            });
        }
        return Ok(None);
    }
    let material = view.run_execution_material(run_id)?;
    crate::validate_continuation_plan_frames(&material.plan, &material.continuation)?;
    for frame in &material.continuation.frames {
        let scope = view
            .scope_current(&material.run, &frame.scope_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_frame_scope_missing".to_owned(),
                message: format!("Run {run_id} frame references a missing Scope"),
            })?;
        cymule_core::durable_internal::validate_pinned_execution_frame(
            &material.plan,
            &cymule_core::ExecutionFrameLocation {
                run_id,
                plan_id: &material.plan.plan_id,
                invocation_id: &frame.invocation_id,
                invocation_path: &frame.invocation_path,
                definition_id: &frame.definition_id,
                region_path: &frame.region_path,
                scope_id: &frame.scope_id,
                next_step: frame.next_step,
            },
            &scope.current,
            &scope.invocation_path,
            &scope.region_path,
        )?;
    }
    let root_input_ref = material
        .continuation
        .frames
        .first()
        .map(|frame| &frame.input)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_root_input_reference_missing".to_owned(),
            message: format!("Run {run_id} has no retained root input reference"),
        })?;
    let root_input =
        view.artifact(&root_input_ref.artifact_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_root_input_missing".to_owned(),
                message: format!(
                    "Run {run_id} root input Artifact {} is missing",
                    root_input_ref.artifact_id
                ),
            })?;
    if &root_input.reference != root_input_ref
        || root_input.reference.kind != cymule_core::RUN_INPUT_ARTIFACT_KIND
    {
        return Err(DurableError::Integrity {
            code: "executor_root_input_mismatch".to_owned(),
            message: format!("Run {run_id} root input changed identity or kind"),
        });
    }
    let terminal_result = material
        .run
        .result
        .as_ref()
        .map(|reference| {
            view.artifact(&reference.artifact_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "executor_terminal_result_missing".to_owned(),
                    message: format!(
                        "Run {run_id} terminal result Artifact {} is missing",
                        reference.artifact_id
                    ),
                })
        })
        .transpose()?;
    if terminal_result
        .as_ref()
        .zip(material.run.result.as_ref())
        .is_some_and(|(record, reference)| &record.reference != reference)
    {
        return Err(DurableError::Integrity {
            code: "executor_terminal_result_mismatch".to_owned(),
            message: format!("Run {run_id} terminal result changed its exact reference"),
        });
    }
    Ok(Some(ExecutorRunRead {
        revision: manifest.revision().to_owned(),
        projection_root: manifest.machine_frontier().projection_root.clone(),
        run: material.run,
        plan: material.plan,
        binding: material.binding,
        continuation: material.continuation,
        root_input,
        terminal_result,
    }))
}

fn load_executor_effect_at_manifest(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    expected_run_id: Option<&str>,
    intent_id: &str,
) -> DurableResult<Option<ExecutorEffectRead>> {
    crate::model::validate_sha256_identity("Effect intent", intent_id)?;
    let Some(dispatch) =
        crate::state_root::load_effect_dispatch(manifest, resolver, expected_run_id, intent_id)?
    else {
        return Ok(None);
    };
    if dispatch.intent_id != intent_id
        || expected_run_id.is_some_and(|run_id| dispatch.run_id != run_id)
    {
        return Err(DurableError::Integrity {
            code: "executor_effect_dispatch_key_mismatch".to_owned(),
            message: format!("Effect dispatch {intent_id} changed identity or Run owner"),
        });
    }
    let run =
        load_executor_run_at_manifest(manifest, resolver, &dispatch.run_id)?.ok_or_else(|| {
            DurableError::Integrity {
                code: "executor_effect_run_missing".to_owned(),
                message: format!(
                    "Effect {intent_id} references missing Run {}",
                    dispatch.run_id
                ),
            }
        })?;
    let mut view = crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
    let effect =
        view.effect_current(&run.run, intent_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_effect_core_missing".to_owned(),
                message: format!("Effect {intent_id} has no exact Core current"),
            })?;
    if effect.origin_plan_id != dispatch.origin_plan_id
        || effect.args != dispatch.input
        || effect.execution_binding != dispatch.execution_binding
        || effect.occurrence_binding != dispatch.occurrence_binding
        || effect.operation != dispatch.operation
    {
        return Err(DurableError::Integrity {
            code: "executor_effect_sidecar_mismatch".to_owned(),
            message: format!("Effect {intent_id} Core and dispatch authorities disagree"),
        });
    }
    let scope = view
        .scope_current(&run.run, &effect.scope_id)?
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_effect_scope_missing".to_owned(),
            message: format!("Effect {intent_id} has no owning Scope {}", effect.scope_id),
        })?;
    let (origin_plan, origin_binding) = load_executor_effect_origin(&mut view, &effect, intent_id)?;
    let (input, result) = load_executor_effect_values(&mut view, &effect, &dispatch, intent_id)?;
    Ok(Some(ExecutorEffectRead {
        revision: manifest.revision().to_owned(),
        run,
        origin_plan,
        origin_binding,
        effect,
        scope: scope.current,
        dispatch,
        input,
        result,
    }))
}

fn load_executor_effect_values<R: crate::StateRootResolver + ?Sized>(
    view: &mut crate::state_root::pinned_machine::PinnedMachineView<'_, R>,
    effect: &cymule_core::EffectProjection,
    dispatch: &EffectDispatch,
    intent_id: &str,
) -> DurableResult<(
    cymule_core::ArtifactRecord,
    Option<cymule_core::ArtifactRecord>,
)> {
    let input =
        view.artifact(&effect.args.artifact_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_effect_input_missing".to_owned(),
                message: format!(
                    "Effect {intent_id} input Artifact {} is missing",
                    effect.args.artifact_id
                ),
            })?;
    if input.reference != effect.args {
        return Err(DurableError::Integrity {
            code: "executor_effect_input_mismatch".to_owned(),
            message: format!("Effect {intent_id} input changed reference"),
        });
    }
    let result = dispatch
        .result
        .as_ref()
        .map(|reference| {
            view.artifact(&reference.artifact_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "executor_effect_result_missing".to_owned(),
                    message: format!(
                        "Effect {intent_id} result Artifact {} is missing",
                        reference.artifact_id
                    ),
                })
        })
        .transpose()?;
    if result
        .as_ref()
        .zip(dispatch.result.as_ref())
        .is_some_and(|(record, reference)| &record.reference != reference)
    {
        return Err(DurableError::Integrity {
            code: "executor_effect_result_mismatch".to_owned(),
            message: format!("Effect {intent_id} result changed reference"),
        });
    }
    Ok((input, result))
}

fn load_executor_effect_origin<R: crate::StateRootResolver + ?Sized>(
    view: &mut crate::state_root::pinned_machine::PinnedMachineView<'_, R>,
    effect: &cymule_core::EffectProjection,
    intent_id: &str,
) -> DurableResult<(SealedPlan, cymule_core::ArtifactRecord)> {
    let origin_plan =
        view.plan(&effect.origin_plan_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_effect_origin_plan_missing".to_owned(),
                message: format!(
                    "Effect {intent_id} references missing origin Plan {}",
                    effect.origin_plan_id
                ),
            })?;
    let origin_binding = view
        .artifact(&effect.execution_binding.artifact_id)?
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_effect_origin_binding_missing".to_owned(),
            message: format!(
                "Effect {intent_id} references missing execution binding {}",
                effect.execution_binding.artifact_id
            ),
        })?;
    if origin_binding.reference != effect.execution_binding {
        return Err(DurableError::Integrity {
            code: "executor_effect_origin_binding_mismatch".to_owned(),
            message: format!("Effect {intent_id} binding changed exact reference"),
        });
    }
    let binding = ExecutionBinding::decode(&origin_binding.bytes)?;
    if binding.artifact_ref()? != origin_binding.reference {
        return Err(DurableError::Integrity {
            code: "executor_effect_origin_binding_identity_mismatch".to_owned(),
            message: format!("Effect {intent_id} binding bytes changed identity"),
        });
    }
    binding.admit_plan(&origin_plan)?;
    let expected_occurrence =
        binding.occurrence_binding(ExecutionOperationKind::Effect, &effect.operation)?;
    if expected_occurrence != effect.occurrence_binding {
        return Err(DurableError::Integrity {
            code: "executor_effect_occurrence_binding_mismatch".to_owned(),
            message: format!("Effect {intent_id} occurrence binding is not admitted"),
        });
    }
    Ok((origin_plan, origin_binding))
}

fn load_explicit_effect_release_ready(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    run: &cymule_core::durable_internal::MachineRunCurrent,
    dispatch: &EffectDispatch,
) -> DurableResult<bool> {
    let run_id = run.run_id.as_str();
    let mut view = crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
    let effect = view
        .effect_current(run, &dispatch.intent_id)?
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_active_effect_core_missing".to_owned(),
            message: format!(
                "Run {run_id} Effect {} has no Core current",
                dispatch.intent_id
            ),
        })?;
    let scope =
        view.scope_current(run, &effect.scope_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_active_effect_scope_missing".to_owned(),
                message: format!(
                    "Run {run_id} Effect {} has no owning Scope",
                    dispatch.intent_id
                ),
            })?;
    Ok(
        effect.profile.dispatch == cymule_core::DispatchPolicy::Explicit
            && effect.phase == cymule_core::EffectPhase::Prepared
            && scope.current.status == cymule_core::ScopeStatus::ClosedCommitted,
    )
}

fn history_compaction_matches(
    request: &crate::HistoryCompactionRequest,
    receipt: &crate::HistoryCompactionReceipt,
) -> bool {
    receipt.compaction_id == request.compaction_id
        && receipt.source_revision == request.expected_revision
        && receipt.kind == request.kind
        && receipt.requested_suffix == request.requested_suffix
}

fn require_exact_query_revision(
    manifest: &crate::StateRootManifest,
    expected: &str,
) -> DurableResult<()> {
    cymule_core::validate_content_id("exact query revision", expected)?;
    if manifest.revision() != expected {
        return Err(DurableError::Conflict {
            expected: Some(expected.to_owned()),
            current: Some(manifest.revision().to_owned()),
        });
    }
    Ok(())
}

fn load_virtual_commit_receipt(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    command: &virtual_protocol::VirtualPersistenceCommand,
) -> DurableResult<Option<virtual_protocol::VirtualPersistenceReceipt>> {
    let receipt = crate::state_root::load_virtual_receipt(
        manifest,
        resolver,
        command.scheduler_id(),
        command.command_id(),
    )?;
    if receipt
        .as_ref()
        .is_some_and(|receipt| receipt.command != *command)
    {
        return Err(DurableError::HistoryConflict {
            code: "virtual_command_reused".to_owned(),
            message: "Virtual command identity was reused with different semantics".to_owned(),
        });
    }
    Ok(receipt)
}

fn load_virtual_claim_replay(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    command: &virtual_protocol::VirtualPersistenceCommand,
) -> DurableResult<Option<virtual_protocol::VirtualClaimOutcome>> {
    let Some(receipt) = load_virtual_commit_receipt(manifest, resolver, command)? else {
        return Ok(None);
    };
    let virtual_protocol::VirtualPersistenceOutcome::Claimed(claim_receipt) = &receipt.outcome
    else {
        return Err(DurableError::Integrity {
            code: "virtual_claim_outcome_mismatch".to_owned(),
            message: "Virtual claim returned another operation outcome".to_owned(),
        });
    };
    let plan = claim_receipt
        .claim
        .as_ref()
        .map(|claim| {
            crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?
                .plan(&claim.plan_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "virtual_claim_plan_missing".to_owned(),
                    message: "Virtual claim lost its admitted Plan at the same pinned StateRoot"
                        .to_owned(),
                })
        })
        .transpose()?;
    virtual_claim_outcome(receipt, plan).map(Some)
}

fn verify_virtual_claim_plan(
    receipt: &virtual_protocol::VirtualPersistenceReceipt,
    plan: Option<&SealedPlan>,
) -> DurableResult<()> {
    let valid = match &receipt.outcome {
        virtual_protocol::VirtualPersistenceOutcome::Claimed(value) => {
            match (value.claim.as_ref(), plan) {
                (Some(claim), Some(plan)) => claim.plan_id == plan.plan_id,
                (None, None) => true,
                (Some(_), None) | (None, Some(_)) => false,
            }
        }
        _ => plan.is_none(),
    };
    if !valid {
        return Err(DurableError::Integrity {
            code: "virtual_claim_plan_mismatch".to_owned(),
            message: "Virtual claim receipt and its pre-CAS selected Plan disagree".to_owned(),
        });
    }
    Ok(())
}

fn virtual_claim_outcome(
    receipt: virtual_protocol::VirtualPersistenceReceipt,
    plan: Option<SealedPlan>,
) -> DurableResult<virtual_protocol::VirtualClaimOutcome> {
    verify_virtual_claim_plan(&receipt, plan.as_ref())?;
    match plan {
        Some(plan) => virtual_protocol::VirtualClaimOutcome::claimed(receipt, plan),
        None => virtual_protocol::VirtualClaimOutcome::no_work(receipt),
    }
    .map_err(Into::into)
}

fn required_virtual_clock(
    observation: Option<ClockObservation>,
) -> DurableResult<ClockObservation> {
    observation.ok_or_else(|| DurableError::Integrity {
        code: "virtual_current_clock_missing".to_owned(),
        message: "Virtual leased mutation escaped the current Clock guard".to_owned(),
    })
}

fn unique_artifact_records(
    records: Vec<cymule_core::ArtifactRecord>,
) -> DurableResult<Vec<cymule_core::ArtifactRecord>> {
    let mut unique = BTreeMap::new();
    for record in records {
        record.validate()?;
        if let Some(retained) = unique.insert(record.reference.artifact_id.clone(), record.clone())
            && retained != record
        {
            return Err(DurableError::Integrity {
                code: "material_artifact_identity_conflict".to_owned(),
                message: "one material admission contains conflicting Artifact bytes".to_owned(),
            });
        }
    }
    Ok(unique.into_values().collect())
}

fn exact_leaf_kind_mismatch(kind: &str) -> DurableError {
    DurableError::Integrity {
        code: "exact_query_leaf_kind_mismatch".to_owned(),
        message: format!("{kind} exact query returned another typed leaf"),
    }
}

fn load_bounded_run_index_values<T: serde::de::DeserializeOwned + serde::Serialize>(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    run_id: &str,
    index: crate::state_root::RunQueryIndexKind,
    kind: crate::StateRootLeafKind,
) -> DurableResult<Vec<T>> {
    let root = crate::state_root::load_run_query_index_root(manifest, resolver, run_id, index)?;
    let maximum = u64::try_from(
        cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES
            * cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGES,
    )
    .map_err(|error| DurableError::Validation(error.to_string()))?;
    if root.entries > maximum {
        return Err(DurableError::Validation(format!(
            "Run {run_id} command read set exceeds {maximum} entries"
        )));
    }
    let mut result = Vec::new();
    let mut position = None;
    let mut bytes = 0_usize;
    loop {
        let page = crate::state_root::load_state_map_key_page(
            &root,
            position.as_ref(),
            cymule_core::durable_internal::MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
            crate::state_root::MAX_STATE_MAP_KEY_PAGE_BYTES,
            resolver,
        )?;
        for entry in page.entries {
            let value = crate::state_map_get(&root, &entry.key, resolver)?.ok_or_else(|| {
                DurableError::Integrity {
                    code: "run_index_value_missing".to_owned(),
                    message: format!("Run {run_id} indexed value {} is absent", entry.key),
                }
            })?;
            bytes = bytes
                .checked_add(cymule_core::canonical_bytes(&value)?.len())
                .ok_or_else(|| {
                    DurableError::Validation("Run read-set byte accounting overflowed".to_owned())
                })?;
            if bytes > cymule_core::durable_internal::MAX_PINNED_MACHINE_READ_SET_BYTES {
                return Err(DurableError::Validation(
                    "Run command read set exceeds its byte bound".to_owned(),
                ));
            }
            result.push(
                crate::state_root::load_typed_state_map_value::<T, _>(
                    &root, &entry.key, kind, resolver,
                )?
                .ok_or_else(|| DurableError::Integrity {
                    code: "run_index_typed_value_missing".to_owned(),
                    message: format!("Run {run_id} typed indexed value {} is absent", entry.key),
                })?,
            );
        }
        let Some(next) = page.next_position else {
            break;
        };
        position = Some(next);
    }
    Ok(result)
}

fn evolution_migration_sidecars(
    transition: &cymule_core::durable_internal::PinnedMachineBatchTransition,
    expected_command_id: &str,
    target: Continuation,
    operations: Vec<DurableOperation>,
) -> DurableResult<Vec<DurableOperation>> {
    if transition.batch.members.len() != 1
        || transition.batch.receipts.len() != 1
        || transition.steps.len() != 1
        || transition.batch.members[0].command_id != expected_command_id
        || transition.batch.receipts[0].command_id != expected_command_id
        || transition.batch.receipts[0].status != CommandReceiptStatus::Applied
    {
        return Err(DurableError::Integrity {
            code: "evolution_migration_batch_mismatch".to_owned(),
            message: "Evolution migration did not produce one exact applied Machine batch"
                .to_owned(),
        });
    }
    let run = transition.steps[0]
        .run
        .as_ref()
        .ok_or_else(|| DurableError::Integrity {
            code: "evolution_migration_run_current_missing".to_owned(),
            message: "Evolution migration Machine batch has no result Run current".to_owned(),
        })?;
    let current = pinned_durable_run_current(&run.result_current, &target)?;
    let mut sidecars = operations;
    sidecars.push(DurableOperation::PutContinuation { value: target });
    sidecars.push(DurableOperation::PutRunCurrent { value: current });
    Ok(sidecars)
}

fn pinned_batch_final_run(
    transition: &cymule_core::durable_internal::PinnedMachineBatchTransition,
) -> DurableResult<&cymule_core::durable_internal::MachineRunDelta> {
    transition
        .steps
        .last()
        .and_then(|step| step.run.as_ref())
        .ok_or_else(|| DurableError::Integrity {
            code: "pinned_batch_run_current_missing".to_owned(),
            message: "pinned command batch has no final Run current".to_owned(),
        })
}

fn derive_effect_claim_acknowledgement(
    read: &ExecutorEffectRead,
    transition: &cymule_core::durable_internal::PinnedMachineBatchTransition,
    lease: &CoordinationLease,
    continuation: &Continuation,
) -> DurableResult<(ExecutorEffectClaimRead, Vec<DurableOperation>)> {
    let run = pinned_batch_final_run(transition)?;
    let mut dispatch = read.dispatch.clone();
    dispatch.state = OutboxState::Claimed;
    dispatch.claim_owner = Some(lease.owner.clone());
    dispatch.claim_epoch = lease.epoch;
    let effect = run
        .effects
        .get(&dispatch.intent_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "effect_claim_core_missing".to_owned(),
            message: "Effect claim batch lost its exact final Effect".to_owned(),
        })?;
    synchronize_pinned_effect_projection(effect, &mut dispatch)?;
    let mut committed_run = read.run.clone();
    committed_run.revision.clear();
    committed_run
        .projection_root
        .clone_from(&transition.frontier.projection_root);
    committed_run.run.clone_from(&run.result_current);
    let acknowledgement = ExecutorEffectClaimRead {
        read: ExecutorEffectRead {
            revision: String::new(),
            run: committed_run,
            origin_plan: read.origin_plan.clone(),
            origin_binding: read.origin_binding.clone(),
            effect: effect.clone(),
            scope: run
                .scopes
                .get(&read.scope.scope_id)
                .cloned()
                .unwrap_or_else(|| read.scope.clone()),
            dispatch: dispatch.clone(),
            input: read.input.clone(),
            result: read.result.clone(),
        },
        owner: lease.owner.clone(),
        epoch: lease.epoch,
        provider_attempt: cymule_runtime::EffectProviderAttempt::new(
            &dispatch.intent_id,
            &lease.owner,
            lease.epoch,
        )?,
    };
    let operations = vec![
        DurableOperation::PutLease {
            value: lease.clone(),
        },
        DurableOperation::PutOutbox { value: dispatch },
        DurableOperation::PutRunCurrent {
            value: pinned_durable_run_current(&run.result_current, continuation)?,
        },
    ];
    Ok((acknowledgement, operations))
}

fn finish_effect_claim_acknowledgement(
    intent_id: &str,
    commit: PinnedBatchCommit,
    acknowledgement: Option<ExecutorEffectClaimRead>,
) -> DurableResult<ExecutorEffectClaimRead> {
    for receipt in &commit.receipts {
        require_applied_command_receipt(receipt.clone())?;
    }
    let committed_revision =
        commit
            .committed_revision
            .ok_or_else(|| DurableError::ReconciliationRequired {
                intent_id: intent_id.to_owned(),
            })?;
    let mut acknowledgement = acknowledgement.ok_or_else(|| DurableError::Integrity {
        code: "effect_claim_transition_acknowledgement_missing".to_owned(),
        message: "fresh Effect claim CAS returned no transition-derived acknowledgement".to_owned(),
    })?;
    acknowledgement
        .read
        .revision
        .clone_from(&committed_revision);
    acknowledgement
        .read
        .run
        .revision
        .clone_from(&committed_revision);
    Ok(acknowledgement)
}

fn pinned_durable_run_current(
    run: &cymule_core::durable_internal::MachineRunCurrent,
    continuation: &Continuation,
) -> DurableResult<crate::DurableRunCurrent> {
    run.verify()?;
    continuation.verify_wire()?;
    if continuation.run_id != run.run_id
        || continuation.plan_id != run.current_plan
        || continuation.binding_context != run.current_binding_context
        || continuation.epoch != run.epoch
        || (continuation.status == ContinuationStatus::Running) != run.active_attempt_id.is_some()
        || continuation
            .execution_claim
            .as_ref()
            .map(|claim| claim.continuation_attempt_id.as_str())
            != run.active_attempt_id.as_deref()
    {
        return Err(DurableError::Integrity {
            code: "pinned_run_current_continuation_mismatch".to_owned(),
            message: format!(
                "pinned Core Run {} and its durable Continuation disagree",
                run.run_id
            ),
        });
    }
    let value = crate::DurableRunCurrent {
        run_id: run.run_id.clone(),
        plan_id: run.current_plan.clone(),
        execution_binding: ArtifactRef {
            identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
            artifact_id: run.current_binding_context.clone(),
            kind: cymule_core::EXECUTION_BINDING_ARTIFACT_KIND.to_owned(),
        },
        continuation_status: continuation.status,
        epoch: continuation.epoch,
        execution_fence: continuation.execution_fence,
        result: run.result.clone(),
        execution_status: run.execution_status.clone(),
        world_settlement: run.world_settlement,
    };
    value.verify()?;
    Ok(value)
}

#[derive(Clone, Copy)]
struct QueryPageRequest<'a> {
    kind: crate::DurablePageQueryKind,
    run_id: Option<&'a str>,
    expected_revision: Option<&'a str>,
    cursor: Option<&'a crate::DurablePageCursor>,
    limit: u32,
    max_canonical_bytes: u64,
}

fn query_command_at_manifest(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    command: &crate::DurableCommand,
) -> DurableResult<crate::DurableResponse> {
    if let Some(request) = query_page_request(command) {
        return match request.kind {
            crate::DurablePageQueryKind::RunIndex => {
                query_run_index_page(manifest, resolver, request)
            }
            crate::DurablePageQueryKind::RunWaits => {
                query_run_wait_page(manifest, resolver, request)
            }
            crate::DurablePageQueryKind::RunEffects => {
                query_run_effect_page(manifest, resolver, request)
            }
            crate::DurablePageQueryKind::RunOccurrences => {
                query_run_occurrence_page(manifest, resolver, request)
            }
            crate::DurablePageQueryKind::RunAttempts => {
                query_run_attempt_page(manifest, resolver, request)
            }
        };
    }
    match command {
        crate::DurableCommand::RunCurrent {
            run_id,
            expected_revision,
            ..
        } => query_run_current(manifest, resolver, run_id, expected_revision.as_deref()),
        crate::DurableCommand::RunItem {
            run_id,
            expected_revision,
            selector,
            max_canonical_bytes,
            ..
        } => query_run_item(
            manifest,
            resolver,
            run_id,
            expected_revision.as_deref(),
            selector,
            *max_canonical_bytes,
        ),
        _ => Err(DurableError::RuntimeDefect {
            code: "non_query_reached_query_lowering".to_owned(),
            message: "a non-query command reached the closed query lowering".to_owned(),
        }),
    }
}

fn query_page_request(command: &crate::DurableCommand) -> Option<QueryPageRequest<'_>> {
    use crate::DurableCommand as Command;
    use crate::DurablePageQueryKind as Kind;
    let (kind, run_id, expected_revision, cursor, limit, max_canonical_bytes) = match command {
        Command::RunIndexPage {
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
            ..
        } => (
            Kind::RunIndex,
            None,
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
        ),
        Command::RunWaitPage {
            run_id,
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
            ..
        } => (
            Kind::RunWaits,
            Some(run_id.as_str()),
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
        ),
        Command::RunEffectPage {
            run_id,
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
            ..
        } => (
            Kind::RunEffects,
            Some(run_id.as_str()),
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
        ),
        Command::RunOccurrencePage {
            run_id,
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
            ..
        } => (
            Kind::RunOccurrences,
            Some(run_id.as_str()),
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
        ),
        Command::RunAttemptPage {
            run_id,
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
            ..
        } => (
            Kind::RunAttempts,
            Some(run_id.as_str()),
            expected_revision,
            cursor,
            limit,
            max_canonical_bytes,
        ),
        _ => return None,
    };
    Some(QueryPageRequest {
        kind,
        run_id,
        expected_revision: expected_revision.as_deref(),
        cursor: cursor.as_ref(),
        limit: *limit,
        max_canonical_bytes: *max_canonical_bytes,
    })
}

fn query_run_index_page(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    request: QueryPageRequest<'_>,
) -> DurableResult<crate::DurableResponse> {
    let root = &manifest.roots().run_currents;
    query_typed_page(
        manifest,
        resolver,
        root,
        request,
        |key, value_id, resolver| {
            let current = load_required_query_leaf::<crate::DurableRunCurrent>(
                key,
                value_id,
                crate::StateRootLeafKind::RunCurrent,
                resolver,
            )?;
            Ok(crate::DurableRunIndexSummary {
                run_id: current.run_id,
                continuation_status: current.continuation_status,
                execution_status: current.execution_status,
                world_settlement: current.world_settlement,
            })
        },
        |page| crate::DurableResponse::RunIndexPage { page },
    )
}

fn query_run_current(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    run_id: &str,
    expected_revision: Option<&str>,
) -> DurableResult<crate::DurableResponse> {
    let source_root = crate::state_root::state_map_root_digest(&manifest.roots().run_currents)?;
    ensure_query_source(expected_revision, None, manifest.revision(), &source_root)?;
    let current = crate::state_root::load_run_current(manifest, resolver, run_id)?.map(Box::new);
    Ok(crate::DurableResponse::RunCurrent {
        observed_revision: manifest.revision().to_owned(),
        source_root,
        current,
    })
}

fn query_run_wait_page(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    request: QueryPageRequest<'_>,
) -> DurableResult<crate::DurableResponse> {
    let run_id = required_query_run_id(&request)?;
    let root = crate::state_root::load_run_query_index_root(
        manifest,
        resolver,
        run_id,
        crate::state_root::RunQueryIndexKind::Waits,
    )?;
    query_typed_page(
        manifest,
        resolver,
        &root,
        request,
        |key, value_id, resolver| {
            let value = load_required_query_leaf::<WaitCondition>(
                key,
                value_id,
                crate::StateRootLeafKind::Wait,
                resolver,
            )?;
            Ok(crate::DurableWaitSummary {
                wait_id: value.wait_id,
                run_id: value.run_id,
                state: value.state,
                result: value.result,
            })
        },
        |page| crate::DurableResponse::RunWaitPage {
            run_id: run_id.to_owned(),
            page,
        },
    )
}

fn query_run_effect_page(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    request: QueryPageRequest<'_>,
) -> DurableResult<crate::DurableResponse> {
    let run_id = required_query_run_id(&request)?;
    let root = crate::state_root::load_run_query_index_root(
        manifest,
        resolver,
        run_id,
        crate::state_root::RunQueryIndexKind::Effects,
    )?;
    query_typed_page(
        manifest,
        resolver,
        &root,
        request,
        |key, value_id, resolver| {
            let value = load_required_query_leaf::<EffectDispatch>(
                key,
                value_id,
                crate::StateRootLeafKind::Outbox,
                resolver,
            )?;
            Ok(crate::DurableEffectSummary {
                intent_id: value.intent_id,
                run_id: value.run_id,
                state: value.state,
                execution_availability: value.execution_availability,
                reconciliation: value.reconciliation,
                result: value.result,
            })
        },
        |page| crate::DurableResponse::RunEffectPage {
            run_id: run_id.to_owned(),
            page,
        },
    )
}

fn query_run_occurrence_page(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    request: QueryPageRequest<'_>,
) -> DurableResult<crate::DurableResponse> {
    let run_id = required_query_run_id(&request)?;
    let root = crate::state_root::load_run_query_index_root(
        manifest,
        resolver,
        run_id,
        crate::state_root::RunQueryIndexKind::Occurrences,
    )?;
    query_typed_page(
        manifest,
        resolver,
        &root,
        request,
        |key, value_id, resolver| {
            let value = load_required_query_leaf::<ComponentOccurrence>(
                key,
                value_id,
                crate::StateRootLeafKind::ComponentOccurrence,
                resolver,
            )?;
            Ok(crate::DurableOccurrenceSummary {
                occurrence_id: value.occurrence_id,
                run_id: value.run_id,
                state: value.state,
                outcome: value.outcome,
            })
        },
        |page| crate::DurableResponse::RunOccurrencePage {
            run_id: run_id.to_owned(),
            page,
        },
    )
}

fn query_run_attempt_page(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    request: QueryPageRequest<'_>,
) -> DurableResult<crate::DurableResponse> {
    let run_id = required_query_run_id(&request)?;
    let root = crate::state_root::load_run_query_index_root(
        manifest,
        resolver,
        run_id,
        crate::state_root::RunQueryIndexKind::Attempts,
    )?;
    query_typed_page(
        manifest,
        resolver,
        &root,
        request,
        |key, value_id, resolver| {
            let value = load_required_query_leaf::<OperationAttempt>(
                key,
                value_id,
                crate::StateRootLeafKind::OperationAttempt,
                resolver,
            )?;
            Ok(crate::DurableAttemptSummary {
                attempt_id: value.attempt_id,
                occurrence_id: value.occurrence_id,
                run_id: value.run_id,
                attempt_ordinal: value.attempt_ordinal,
                state: value.state,
                outcome: value.outcome,
            })
        },
        |page| crate::DurableResponse::RunAttemptPage {
            run_id: run_id.to_owned(),
            page,
        },
    )
}

fn query_run_item(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    run_id: &str,
    expected_revision: Option<&str>,
    selector: &crate::DurableRunItemSelector,
    max_canonical_bytes: u64,
) -> DurableResult<crate::DurableResponse> {
    let (root, item) = match selector {
        crate::DurableRunItemSelector::Wait { wait_id } => {
            let root = &manifest.roots().waits;
            let value = crate::state_root::load_typed_state_map_value::<WaitCondition, _>(
                root,
                wait_id,
                crate::StateRootLeafKind::Wait,
                resolver,
            )?
            .map(|wait| crate::DurableRunItem::Wait {
                wait: Box::new(wait),
            });
            (root.clone(), value)
        }
        crate::DurableRunItemSelector::Effect { intent_id } => {
            let root = crate::state_root::load_run_query_index_root(
                manifest,
                resolver,
                run_id,
                crate::state_root::RunQueryIndexKind::Effects,
            )?;
            let value = crate::state_root::load_effect_dispatch(
                manifest,
                resolver,
                Some(run_id),
                intent_id,
            )?
            .map(|effect| crate::DurableRunItem::Effect {
                effect: Box::new(effect),
            });
            (root.clone(), value)
        }
        crate::DurableRunItemSelector::Occurrence { occurrence_id } => {
            let root = &manifest.roots().component_occurrences;
            let value = crate::state_root::load_typed_state_map_value::<ComponentOccurrence, _>(
                root,
                occurrence_id,
                crate::StateRootLeafKind::ComponentOccurrence,
                resolver,
            )?
            .map(|occurrence| crate::DurableRunItem::Occurrence {
                occurrence: Box::new(occurrence),
            });
            (root.clone(), value)
        }
        crate::DurableRunItemSelector::Attempt { attempt_id } => {
            let root = &manifest.roots().operation_attempts;
            let value = crate::state_root::load_typed_state_map_value::<OperationAttempt, _>(
                root,
                attempt_id,
                crate::StateRootLeafKind::OperationAttempt,
                resolver,
            )?
            .map(|attempt| crate::DurableRunItem::Attempt {
                attempt: Box::new(attempt),
            });
            (root.clone(), value)
        }
    };
    let source_root = crate::state_root::state_map_root_digest(&root)?;
    ensure_query_source(expected_revision, None, manifest.revision(), &source_root)?;
    let response = crate::DurableResponse::RunItem {
        run_id: run_id.to_owned(),
        observed_revision: manifest.revision().to_owned(),
        source_root,
        item: item.map(Box::new),
    };
    let maximum = usize::try_from(max_canonical_bytes).map_err(|_| {
        DurableError::Validation(
            "exact Run-item response byte budget is not representable on this target".to_owned(),
        )
    })?;
    if cymule_core::canonical_bytes(&response)?.len() > maximum {
        return Err(DurableError::Validation(
            "exact Run-item response does not fit the requested canonical byte budget".to_owned(),
        ));
    }
    Ok(response)
}

fn required_query_run_id<'a>(request: &'a QueryPageRequest<'_>) -> DurableResult<&'a str> {
    request.run_id.ok_or_else(|| DurableError::RuntimeDefect {
        code: "run_query_owner_missing".to_owned(),
        message: "Run-scoped query lowering lost its exact owner".to_owned(),
    })
}

fn query_typed_page<T>(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    root: &MapRoot,
    request: QueryPageRequest<'_>,
    mut load: impl FnMut(&str, &str, &mut dyn crate::StateRootResolver) -> DurableResult<T>,
    wrap: impl Fn(crate::DurableQueryPage<T>) -> crate::DurableResponse,
) -> DurableResult<crate::DurableResponse>
where
    T: Clone + serde::Serialize,
{
    let source_root = crate::state_root::state_map_root_digest(root)?;
    ensure_query_source(
        request.expected_revision,
        request.cursor,
        manifest.revision(),
        &source_root,
    )?;
    let position = request
        .cursor
        .map(|cursor| crate::state_root::StateMapTraversalPosition {
            key: cursor.position.canonical_key.clone(),
            key_hash: cursor.position.key_hash.clone(),
        });
    let limit = usize::try_from(request.limit).map_err(|_| {
        DurableError::Validation(
            "durable query item limit is not representable on this target".to_owned(),
        )
    })?;
    let physical = crate::state_root::load_state_map_key_page(
        root,
        position.as_ref(),
        limit,
        crate::state_root::MAX_STATE_MAP_KEY_PAGE_BYTES,
        resolver,
    )?;
    let mut items = Vec::with_capacity(physical.entries.len());
    let maximum = usize::try_from(request.max_canonical_bytes).map_err(|_| {
        DurableError::Validation(
            "durable query response byte budget is not representable on this target".to_owned(),
        )
    })?;
    let mut response = wrap(crate::DurableQueryPage {
        observed_revision: manifest.revision().to_owned(),
        source_root: source_root.clone(),
        items: Vec::new(),
        next_cursor: None,
    });
    for (index, entry) in physical.entries.iter().enumerate() {
        items.push(load(&entry.key, &entry.value_id, resolver)?);
        let has_more = index + 1 < physical.entries.len() || physical.next_position.is_some();
        let next_cursor = has_more
            .then(|| {
                crate::DurablePageCursor::new(
                    request.kind,
                    request.run_id,
                    manifest.revision(),
                    &source_root,
                    &entry.key,
                )
            })
            .transpose()?;
        let candidate = wrap(crate::DurableQueryPage {
            observed_revision: manifest.revision().to_owned(),
            source_root: source_root.clone(),
            items: items.clone(),
            next_cursor,
        });
        if cymule_core::canonical_bytes(&candidate)?.len() > maximum {
            items.pop();
            let Some(last) = physical.entries.get(index.checked_sub(1).ok_or_else(|| {
                DurableError::Validation(
                    "the first legal query item does not fit the requested canonical byte budget"
                        .to_owned(),
                )
            })?) else {
                return Err(DurableError::RuntimeDefect {
                    code: "query_page_prefix_missing".to_owned(),
                    message: "bounded query truncation lost its retained prefix".to_owned(),
                });
            };
            response = wrap(crate::DurableQueryPage {
                observed_revision: manifest.revision().to_owned(),
                source_root: source_root.clone(),
                items,
                next_cursor: Some(crate::DurablePageCursor::new(
                    request.kind,
                    request.run_id,
                    manifest.revision(),
                    &source_root,
                    &last.key,
                )?),
            });
            return Ok(response);
        }
        response = candidate;
    }
    Ok(response)
}

fn load_required_query_leaf<T>(
    _key: &str,
    value_id: &str,
    kind: crate::StateRootLeafKind,
    resolver: &mut dyn crate::StateRootResolver,
) -> DurableResult<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    crate::state_root::load_typed_state_value(value_id, kind, resolver)
}

fn ensure_query_source(
    expected_revision: Option<&str>,
    cursor: Option<&crate::DurablePageCursor>,
    observed_revision: &str,
    source_root: &str,
) -> DurableResult<()> {
    if let Some(cursor) = cursor {
        if cursor.source_revision != observed_revision {
            return Err(DurableError::HistoryConflict {
                code: "durable_query_revision_changed".to_owned(),
                message: "durable query continuation observed a different semantic revision"
                    .to_owned(),
            });
        }
        if cursor.source_root != source_root {
            return Err(DurableError::HistoryConflict {
                code: "durable_query_source_root_changed".to_owned(),
                message:
                    "durable query continuation observed a different authenticated source root"
                        .to_owned(),
            });
        }
    } else if let Some(expected) = expected_revision
        && expected != observed_revision
    {
        return Err(DurableError::Conflict {
            expected: Some(expected.to_owned()),
            current: Some(observed_revision.to_owned()),
        });
    }
    Ok(())
}

fn ensure_agent_query_revision(expected: Option<&String>, observed: &str) -> DurableResult<()> {
    if expected.is_some_and(|expected| expected != observed) {
        return Err(DurableError::Conflict {
            expected: expected.cloned(),
            current: Some(observed.to_owned()),
        });
    }
    Ok(())
}

struct AgentInputCompletionPlan {
    agent_receipt: agent_protocol::AgentCommandReceipt,
    completed_wait: WaitCondition,
    source_continuation: Continuation,
    completed_continuation: Continuation,
    result: ArtifactRef,
    result_bytes: Vec<u8>,
    operations: Vec<DurableOperation>,
}

fn prepare_agent_input_suspension<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
    input: &agent_protocol::AgentInputCommand,
) -> DurableResult<(agent_protocol::AgentCommandReceipt, Vec<DurableOperation>)> {
    let agent_protocol::AgentInputCommand::Suspend {
        session_id,
        wait_id,
        expected_run_id,
        expected_owner,
        request,
    } = input
    else {
        return Err(DurableError::RuntimeDefect {
            code: "agent_input_suspend_shape_mismatch".to_owned(),
            message: "Agent input suspension preparation received a completion".to_owned(),
        });
    };
    if crate::state_root::load_agent_input_suspension_receipt(manifest, resolver, wait_id)?
        .is_some()
        || crate::state_root::load_agent_input_completion_receipt(manifest, resolver, wait_id)?
            .is_some()
    {
        return Err(DurableError::HistoryConflict {
            code: "agent_input_partial_history".to_owned(),
            message: format!(
                "Agent input Wait {wait_id} has an M1 receipt without its exact Agent command receipt"
            ),
        });
    }
    let session = require_agent_session(manifest, resolver, session_id)?;
    let elicitation = crate::state_root::load_agent_elicitation_current(
        manifest,
        resolver,
        session_id,
        &request.request_id,
    )?;
    let wait = crate::state_root::load_wait(manifest, resolver, wait_id)?
        .ok_or_else(|| DurableError::NotFound(format!("input Wait {wait_id} does not exist")))?;
    ensure_direct_wait_completion(&wait)?;
    require_expected_wait_owner(&wait, expected_run_id, expected_owner)?;
    if wait.state != WaitState::Pending || wait.result.is_some() {
        return Err(DurableError::IllegalTransition(format!(
            "input Wait {wait_id} is not pending"
        )));
    }
    let continuation = crate::state_root::load_continuation(manifest, resolver, expected_run_id)?
        .ok_or_else(|| {
        DurableError::NotFound(format!(
            "input Wait {wait_id} Continuation {expected_run_id} does not exist"
        ))
    })?;
    let m1_receipt =
        crate::model::AgentInputSuspensionReceipt::new(&command.command_id, wait, &continuation)?;
    let witness = agent_protocol::AgentInputWaitWitness::Suspended {
        run_id: expected_run_id.clone(),
        owner: expected_owner.clone(),
        suspension_receipt_id: m1_receipt.receipt_id.clone(),
    };
    let source = agent_protocol::AgentInputSource::Suspend {
        session,
        elicitation,
    };
    let checkpoint = source.reduce(&command.command_id, input, witness)?;
    let receipt = agent_protocol::AgentCommandReceipt::new(
        command,
        agent_protocol::AgentCommandSource::Input(source),
        agent_protocol::AgentCommandOutcome::Input(checkpoint.clone()),
    )?;
    Ok((
        receipt.clone(),
        vec![
            DurableOperation::PutAgentCommand {
                value: command.clone(),
            },
            DurableOperation::PutAgentCommandReceipt {
                value: Box::new(receipt),
            },
            DurableOperation::PutAgentInputSuspensionReceipt { value: m1_receipt },
            DurableOperation::PutAgentSessionCurrent {
                value: checkpoint.session,
            },
            DurableOperation::PutAgentElicitationCurrent {
                value: checkpoint.elicitation,
            },
        ],
    ))
}

fn prepare_agent_input_completion<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
    input: &agent_protocol::AgentInputCommand,
) -> DurableResult<AgentInputCompletionPlan> {
    let agent_protocol::AgentInputCommand::Complete {
        session_id,
        wait_id,
        expected_run_id,
        expected_owner,
        response,
    } = input
    else {
        return Err(DurableError::RuntimeDefect {
            code: "agent_input_complete_shape_mismatch".to_owned(),
            message: "Agent input completion preparation received a suspension".to_owned(),
        });
    };
    if crate::state_root::load_agent_input_completion_receipt(manifest, resolver, wait_id)?
        .is_some()
    {
        return Err(DurableError::HistoryConflict {
            code: "agent_input_partial_history".to_owned(),
            message: format!(
                "Agent input Wait {wait_id} has a completion receipt without its exact Agent command receipt"
            ),
        });
    }
    let suspension =
        crate::state_root::load_agent_input_suspension_receipt(manifest, resolver, wait_id)?
            .ok_or_else(|| {
                DurableError::NotFound(format!(
                    "input Wait {wait_id} has no retained typed suspension receipt"
                ))
            })?;
    verify_agent_input_suspension_graph(manifest, resolver, &suspension)?;
    let source =
        load_agent_input_completion_source(manifest, resolver, session_id, &response.request_id)?;
    let (source_wait, source_continuation) = load_pending_agent_input_continuation(
        manifest,
        resolver,
        wait_id,
        expected_run_id,
        expected_owner,
        &suspension,
    )?;
    let result_value = agent_protocol::AgentInputResult::from_response(response)?;
    let result_bytes = result_value.canonical_bytes()?;
    crate::executor::validate_wait_completion(
        &source_wait,
        &cymule_core::decode_json(&result_bytes)?,
    )?;
    let result = cymule_core::artifact_ref(crate::WAIT_RESULT_ARTIFACT_KIND, &result_bytes)?;
    let mut completed_wait = source_wait.clone();
    completed_wait.state = WaitState::Completed;
    completed_wait.result = Some(result.clone());
    let mut completed_continuation = source_continuation.clone();
    apply_wait_result(&completed_wait, &result, &mut completed_continuation)?;
    let completion = crate::model::AgentInputCompletionReceipt::new(
        &command.command_id,
        &suspension.receipt_id,
        completed_wait.clone(),
        result.clone(),
        &completed_continuation,
    )?;
    let witness = agent_protocol::AgentInputWaitWitness::Completed {
        run_id: expected_run_id.clone(),
        owner: expected_owner.clone(),
        suspension_receipt_id: suspension.receipt_id,
        completion_receipt_id: completion.receipt_id.clone(),
        result: result.clone(),
    };

    let checkpoint = source.reduce(&command.command_id, input, witness)?;
    let agent_receipt = agent_protocol::AgentCommandReceipt::new(
        command,
        agent_protocol::AgentCommandSource::Input(source),
        agent_protocol::AgentCommandOutcome::Input(checkpoint.clone()),
    )?;
    Ok(AgentInputCompletionPlan {
        agent_receipt: agent_receipt.clone(),
        completed_wait,
        source_continuation,
        completed_continuation,
        result,
        result_bytes,
        operations: vec![
            DurableOperation::PutAgentCommand {
                value: command.clone(),
            },
            DurableOperation::PutAgentCommandReceipt {
                value: Box::new(agent_receipt),
            },
            DurableOperation::PutAgentInputCompletionReceipt { value: completion },
            DurableOperation::PutAgentSessionCurrent {
                value: checkpoint.session,
            },
            DurableOperation::PutAgentElicitationCurrent {
                value: checkpoint.elicitation,
            },
        ],
    })
}

fn load_agent_input_completion_source<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    session_id: &str,
    request_id: &str,
) -> DurableResult<agent_protocol::AgentInputSource> {
    let session = require_agent_session(manifest, resolver, session_id)?;
    let elicitation = crate::state_root::load_agent_elicitation_current(
        manifest, resolver, session_id, request_id,
    )?
    .ok_or_else(|| {
        DurableError::NotFound(format!(
            "Agent Session {session_id} has no pending elicitation {request_id}"
        ))
    })?;
    Ok(agent_protocol::AgentInputSource::Complete {
        session,
        elicitation,
    })
}

fn load_pending_agent_input_continuation<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    wait_id: &str,
    expected_run_id: &str,
    expected_owner: &WaitOwner,
    suspension: &crate::model::AgentInputSuspensionReceipt,
) -> DurableResult<(WaitCondition, Continuation)> {
    let source_wait = crate::state_root::load_wait(manifest, resolver, wait_id)?
        .ok_or_else(|| DurableError::NotFound(format!("input Wait {wait_id} does not exist")))?;
    ensure_direct_wait_completion(&source_wait)?;
    require_expected_wait_owner(&source_wait, expected_run_id, expected_owner)?;
    if source_wait != suspension.wait
        || source_wait.state != WaitState::Pending
        || source_wait.result.is_some()
    {
        return Err(DurableError::HistoryConflict {
            code: "agent_input_suspension_projection_mismatch".to_owned(),
            message: format!("input Wait {wait_id} no longer matches its exact pending suspension"),
        });
    }
    let source_continuation =
        crate::state_root::load_continuation(manifest, resolver, expected_run_id)?.ok_or_else(
            || {
                DurableError::NotFound(format!(
                    "input Wait {wait_id} Continuation {expected_run_id} does not exist"
                ))
            },
        )?;
    if source_continuation.status != ContinuationStatus::Waiting
        || !source_continuation.wait_set.contains(wait_id)
    {
        return Err(DurableError::IllegalTransition(format!(
            "input Wait {wait_id} is not retained by a Waiting Continuation"
        )));
    }
    Ok((source_wait, source_continuation))
}

fn verify_agent_input_suspension_graph<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    suspension: &crate::model::AgentInputSuspensionReceipt,
) -> DurableResult<()> {
    suspension.verify()?;
    let command =
        crate::state_root::load_agent_command(manifest, resolver, &suspension.agent_command_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_input_suspension_command_missing".to_owned(),
                message: "Agent input suspension lost its exact owning command".to_owned(),
            })?;
    let receipt = crate::state_root::load_agent_command_receipt(
        manifest,
        resolver,
        &suspension.agent_command_id,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "agent_input_suspension_agent_receipt_missing".to_owned(),
        message: "Agent input suspension lost its exact owning Agent receipt".to_owned(),
    })?;
    receipt.verify_for(&command)?;
    match (&command.action, &receipt.outcome) {
        (
            agent_protocol::AgentCommandAction::Input(agent_protocol::AgentInputCommand::Suspend {
                wait_id,
                ..
            }),
            agent_protocol::AgentCommandOutcome::Input(agent_protocol::AgentInputCheckpoint {
                wait:
                    agent_protocol::AgentInputWaitWitness::Suspended {
                        suspension_receipt_id,
                        ..
                    },
                ..
            }),
        ) if wait_id == &suspension.wait.wait_id
            && suspension_receipt_id == &suspension.receipt_id =>
        {
            Ok(())
        }
        _ => Err(DurableError::Integrity {
            code: "agent_input_suspension_graph_mismatch".to_owned(),
            message: "Agent input suspension does not match its owning Agent command graph"
                .to_owned(),
        }),
    }
}

fn verify_agent_input_receipt_graph<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
    receipt: &agent_protocol::AgentCommandReceipt,
) -> DurableResult<()> {
    receipt.verify_for(command)?;
    let (
        agent_protocol::AgentCommandAction::Input(input),
        agent_protocol::AgentCommandOutcome::Input(checkpoint),
    ) = (&command.action, &receipt.outcome)
    else {
        return Err(DurableError::Integrity {
            code: "agent_input_receipt_shape_mismatch".to_owned(),
            message: "Agent input receipt does not retain an input checkpoint".to_owned(),
        });
    };
    checkpoint.verify_for(input)?;
    let suspension = crate::state_root::load_agent_input_suspension_receipt(
        manifest,
        resolver,
        &checkpoint.wait_id,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "agent_input_suspension_receipt_missing".to_owned(),
        message: "Agent input checkpoint lost its typed suspension receipt".to_owned(),
    })?;
    verify_agent_input_suspension_graph(manifest, resolver, &suspension)?;
    match &checkpoint.wait {
        agent_protocol::AgentInputWaitWitness::Suspended {
            suspension_receipt_id,
            ..
        } if suspension_receipt_id == &suspension.receipt_id
            && suspension.agent_command_id == command.command_id =>
        {
            Ok(())
        }
        agent_protocol::AgentInputWaitWitness::Completed {
            suspension_receipt_id,
            completion_receipt_id,
            result,
            ..
        } if suspension_receipt_id == &suspension.receipt_id => {
            let completion = crate::state_root::load_agent_input_completion_receipt(
                manifest,
                resolver,
                &checkpoint.wait_id,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_input_completion_receipt_missing".to_owned(),
                message: "Agent input checkpoint lost its typed completion receipt".to_owned(),
            })?;
            if completion.receipt_id != *completion_receipt_id
                || completion.agent_command_id != command.command_id
                || completion.suspension_receipt_id != suspension.receipt_id
                || completion.result != *result
            {
                return Err(DurableError::Integrity {
                    code: "agent_input_completion_graph_mismatch".to_owned(),
                    message: "Agent input completion receipt changed its exact graph".to_owned(),
                });
            }
            let artifact = crate::state_root::load_machine_artifact(manifest, resolver, result)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "agent_input_completion_artifact_missing".to_owned(),
                    message: "Agent input completion lost its exact result Artifact".to_owned(),
                })?;
            if artifact.reference != *result {
                return Err(DurableError::Integrity {
                    code: "agent_input_completion_artifact_mismatch".to_owned(),
                    message: "Agent input completion result Artifact changed identity".to_owned(),
                });
            }
            Ok(())
        }
        _ => Err(DurableError::Integrity {
            code: "agent_input_receipt_graph_mismatch".to_owned(),
            message: "Agent input receipt does not match its typed M1 receipt graph".to_owned(),
        }),
    }
}

fn load_verified_agent_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command_id: &str,
) -> DurableResult<(
    agent_protocol::AgentCommand,
    agent_protocol::AgentCommandReceipt,
)> {
    let command = crate::state_root::load_agent_command(manifest, resolver, command_id)?
        .ok_or_else(|| DurableError::Integrity {
            code: "agent_current_origin_command_missing".to_owned(),
            message: format!("Agent current lost its admitting command {command_id}"),
        })?;
    let receipt = crate::state_root::load_agent_command_receipt(manifest, resolver, command_id)?
        .ok_or_else(|| DurableError::Integrity {
            code: "agent_current_origin_receipt_missing".to_owned(),
            message: format!("Agent current lost receipt for admitting command {command_id}"),
        })?;
    receipt.verify_for(&command)?;
    match &command.action {
        agent_protocol::AgentCommandAction::Input(_) => {
            verify_agent_input_receipt_graph(manifest, resolver, &command, &receipt)?;
        }
        agent_protocol::AgentCommandAction::Stream(
            agent_protocol::AgentStreamCommand::Finalize { .. },
        ) => {
            verify_agent_stream_finalization_graph(manifest, resolver, &command, &receipt)?;
        }
        agent_protocol::AgentCommandAction::SessionUpdate { .. }
        | agent_protocol::AgentCommandAction::Occurrence { .. }
        | agent_protocol::AgentCommandAction::Stream(_)
        | agent_protocol::AgentCommandAction::Workspace(_) => {}
    }
    Ok((command, receipt))
}

fn verify_agent_session_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &agent_protocol::AgentSessionCurrent,
) -> DurableResult<()> {
    let witness = current
        .last_transition
        .as_ref()
        .ok_or_else(|| DurableError::Integrity {
            code: "agent_session_origin_missing".to_owned(),
            message: format!(
                "persisted Agent Session {} has no admitting transition",
                current.session_id
            ),
        })?;
    let (_, receipt) = load_verified_agent_origin(manifest, resolver, &witness.command_id)?;
    let (kind, retained) = match &receipt.outcome {
        agent_protocol::AgentCommandOutcome::Session(postcondition) => (
            agent_protocol::AgentSessionTransitionKind::SessionUpdate,
            &postcondition.session,
        ),
        agent_protocol::AgentCommandOutcome::Occurrence(postcondition) => (
            agent_protocol::AgentSessionTransitionKind::Occurrence,
            &postcondition.session,
        ),
        agent_protocol::AgentCommandOutcome::Stream(postcondition) => {
            let session = match &postcondition.effect {
                agent_protocol::AgentStreamEffect::Opened { session }
                | agent_protocol::AgentStreamEffect::Aborted { session } => session,
                agent_protocol::AgentStreamEffect::Finalized { session, .. } => &session.session,
                agent_protocol::AgentStreamEffect::Chunk { .. } => {
                    return Err(DurableError::Integrity {
                        code: "agent_session_origin_stream_chunk".to_owned(),
                        message: "Agent Session cannot cite a chunk-only stream transition"
                            .to_owned(),
                    });
                }
            };
            (agent_protocol::AgentSessionTransitionKind::Stream, session)
        }
        agent_protocol::AgentCommandOutcome::Input(checkpoint) => (
            agent_protocol::AgentSessionTransitionKind::Input,
            &checkpoint.session,
        ),
        agent_protocol::AgentCommandOutcome::Workspace(checkpoint) => (
            agent_protocol::AgentSessionTransitionKind::Workspace,
            &checkpoint.occurrence.session,
        ),
    };
    if witness.kind != kind || retained != current {
        return Err(DurableError::Integrity {
            code: "agent_session_origin_mismatch".to_owned(),
            message: "Agent Session does not equal its admitting receipt outcome".to_owned(),
        });
    }
    Ok(())
}

fn verify_agent_message_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &agent_protocol::AgentMessageCurrent,
) -> DurableResult<()> {
    let (_, receipt) = load_verified_agent_origin(manifest, resolver, &current.order.admitted_by)?;
    let retained = match &receipt.outcome {
        agent_protocol::AgentCommandOutcome::Session(postcondition) => {
            match &postcondition.effect {
                agent_protocol::AgentSessionUpdateEffect::Message { current } => Some(current),
                agent_protocol::AgentSessionUpdateEffect::Metadata
                | agent_protocol::AgentSessionUpdateEffect::Closed { .. }
                | agent_protocol::AgentSessionUpdateEffect::Tool { .. } => None,
            }
        }
        agent_protocol::AgentCommandOutcome::Stream(postcondition) => match &postcondition.effect {
            agent_protocol::AgentStreamEffect::Finalized { session, .. } => match &session.effect {
                agent_protocol::AgentSessionUpdateEffect::Message { current } => Some(current),
                agent_protocol::AgentSessionUpdateEffect::Metadata
                | agent_protocol::AgentSessionUpdateEffect::Closed { .. }
                | agent_protocol::AgentSessionUpdateEffect::Tool { .. } => None,
            },
            agent_protocol::AgentStreamEffect::Opened { .. }
            | agent_protocol::AgentStreamEffect::Chunk { .. }
            | agent_protocol::AgentStreamEffect::Aborted { .. } => None,
        },
        agent_protocol::AgentCommandOutcome::Occurrence(_)
        | agent_protocol::AgentCommandOutcome::Input(_)
        | agent_protocol::AgentCommandOutcome::Workspace(_) => None,
    };
    if retained != Some(current) {
        return Err(DurableError::Integrity {
            code: "agent_message_origin_mismatch".to_owned(),
            message: "Agent message does not equal its admitting receipt outcome".to_owned(),
        });
    }
    Ok(())
}

fn verify_agent_tool_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &agent_protocol::AgentToolCurrent,
) -> DurableResult<()> {
    let (_, receipt) = load_verified_agent_origin(manifest, resolver, &current.admitted_by)?;
    let retained = match &receipt.outcome {
        agent_protocol::AgentCommandOutcome::Session(postcondition) => {
            match &postcondition.effect {
                agent_protocol::AgentSessionUpdateEffect::Tool { current } => Some(current),
                agent_protocol::AgentSessionUpdateEffect::Closed { tools } => tools
                    .iter()
                    .find(|retained| retained.tool.tool_call_id == current.tool.tool_call_id),
                agent_protocol::AgentSessionUpdateEffect::Metadata
                | agent_protocol::AgentSessionUpdateEffect::Message { .. } => None,
            }
        }
        agent_protocol::AgentCommandOutcome::Stream(postcondition) => match &postcondition.effect {
            agent_protocol::AgentStreamEffect::Finalized { session, .. } => match &session.effect {
                agent_protocol::AgentSessionUpdateEffect::Tool { current } => Some(current),
                agent_protocol::AgentSessionUpdateEffect::Metadata
                | agent_protocol::AgentSessionUpdateEffect::Closed { .. }
                | agent_protocol::AgentSessionUpdateEffect::Message { .. } => None,
            },
            agent_protocol::AgentStreamEffect::Opened { .. }
            | agent_protocol::AgentStreamEffect::Chunk { .. }
            | agent_protocol::AgentStreamEffect::Aborted { .. } => None,
        },
        agent_protocol::AgentCommandOutcome::Occurrence(_)
        | agent_protocol::AgentCommandOutcome::Input(_)
        | agent_protocol::AgentCommandOutcome::Workspace(_) => None,
    };
    if retained != Some(current) {
        return Err(DurableError::Integrity {
            code: "agent_tool_origin_mismatch".to_owned(),
            message: "Agent tool does not equal its admitting receipt outcome".to_owned(),
        });
    }
    Ok(())
}

fn verify_agent_elicitation_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &agent_protocol::AgentElicitationCurrent,
) -> DurableResult<()> {
    let (_, receipt) = load_verified_agent_origin(manifest, resolver, &current.admitted_by)?;
    let retained = match &receipt.outcome {
        agent_protocol::AgentCommandOutcome::Input(checkpoint) => Some(&checkpoint.elicitation),
        agent_protocol::AgentCommandOutcome::Session(_)
        | agent_protocol::AgentCommandOutcome::Occurrence(_)
        | agent_protocol::AgentCommandOutcome::Stream(_)
        | agent_protocol::AgentCommandOutcome::Workspace(_) => None,
    };
    if retained != Some(current) {
        return Err(DurableError::Integrity {
            code: "agent_elicitation_origin_mismatch".to_owned(),
            message: "Agent elicitation does not equal its admitting receipt outcome".to_owned(),
        });
    }
    Ok(())
}

fn verify_agent_occurrence_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &agent_protocol::AgentOccurrenceCurrent,
) -> DurableResult<()> {
    let (_, receipt) = load_verified_agent_origin(manifest, resolver, &current.admitted_by)?;
    let retained = match &receipt.outcome {
        agent_protocol::AgentCommandOutcome::Occurrence(postcondition) => {
            Some(&postcondition.current)
        }
        agent_protocol::AgentCommandOutcome::Workspace(checkpoint) => {
            Some(&checkpoint.occurrence.current)
        }
        agent_protocol::AgentCommandOutcome::Session(_)
        | agent_protocol::AgentCommandOutcome::Stream(_)
        | agent_protocol::AgentCommandOutcome::Input(_) => None,
    };
    if retained != Some(current) {
        return Err(DurableError::Integrity {
            code: "agent_occurrence_origin_mismatch".to_owned(),
            message: "Agent occurrence does not equal its admitting receipt outcome".to_owned(),
        });
    }
    Ok(())
}

fn verify_agent_stream_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &agent_protocol::AgentStreamCurrent,
) -> DurableResult<()> {
    let (_, receipt) = load_verified_agent_origin(manifest, resolver, &current.admitted_by)?;
    let retained = match &receipt.outcome {
        agent_protocol::AgentCommandOutcome::Stream(postcondition) => Some(&postcondition.stream),
        agent_protocol::AgentCommandOutcome::Session(_)
        | agent_protocol::AgentCommandOutcome::Occurrence(_)
        | agent_protocol::AgentCommandOutcome::Input(_)
        | agent_protocol::AgentCommandOutcome::Workspace(_) => None,
    };
    if retained != Some(current) {
        return Err(DurableError::Integrity {
            code: "agent_stream_origin_mismatch".to_owned(),
            message: "Agent stream does not equal its admitting receipt outcome".to_owned(),
        });
    }
    Ok(())
}

fn ensure_agent_local_command(command: &agent_protocol::AgentCommand) -> DurableResult<()> {
    match &command.action {
        agent_protocol::AgentCommandAction::SessionUpdate {
            update: agent_protocol::AgentUpdate::Elicitation { .. },
            ..
        } => Err(DurableError::Validation(
            "Agent elicitation changes require the coupled input capability".to_owned(),
        )),
        agent_protocol::AgentCommandAction::Occurrence { occurrence }
            if occurrence.request.is_m1_workspace() =>
        {
            Err(DurableError::Validation(
                "M1 workspace occurrences require the coupled workspace capability".to_owned(),
            ))
        }
        agent_protocol::AgentCommandAction::Stream(
            agent_protocol::AgentStreamCommand::Finalize { .. },
        ) => Err(DurableError::Validation(
            "Agent stream finalization requires the binding-pinned finalization capability"
                .to_owned(),
        )),
        agent_protocol::AgentCommandAction::Input(_) => Err(DurableError::Validation(
            "Agent input changes require the coupled M1 input capability".to_owned(),
        )),
        agent_protocol::AgentCommandAction::Workspace(_) => Err(DurableError::Validation(
            "Agent workspace changes require the binding-pinned workspace capability".to_owned(),
        )),
        agent_protocol::AgentCommandAction::SessionUpdate { .. }
        | agent_protocol::AgentCommandAction::Occurrence { .. }
        | agent_protocol::AgentCommandAction::Stream(_) => Ok(()),
    }
}

fn prepare_agent_local_transition<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
) -> DurableResult<(agent_protocol::AgentCommandReceipt, Vec<DurableOperation>)> {
    ensure_agent_local_command(command)?;
    let (source, outcome, mut operations) = match &command.action {
        agent_protocol::AgentCommandAction::SessionUpdate { session_id, update } => {
            prepare_agent_session_update(manifest, resolver, command, session_id, update)?
        }
        agent_protocol::AgentCommandAction::Occurrence { occurrence } => {
            prepare_agent_occurrence(manifest, resolver, command, occurrence)?
        }
        agent_protocol::AgentCommandAction::Stream(stream) => {
            prepare_agent_stream(manifest, resolver, command, stream)?
        }
        agent_protocol::AgentCommandAction::Input(_)
        | agent_protocol::AgentCommandAction::Workspace(_) => {
            return Err(DurableError::RuntimeDefect {
                code: "agent_specialized_command_reached_local_reducer".to_owned(),
                message: "specialized Agent command reached the local reducer".to_owned(),
            });
        }
    };
    let receipt = agent_protocol::AgentCommandReceipt::new(command, source, outcome)?;
    operations.push(DurableOperation::PutAgentCommand {
        value: command.clone(),
    });
    operations.push(DurableOperation::PutAgentCommandReceipt {
        value: Box::new(receipt.clone()),
    });
    Ok((receipt, operations))
}

fn prepare_agent_session_update<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
    session_id: &str,
    update: &agent_protocol::AgentUpdate,
) -> DurableResult<(
    agent_protocol::AgentCommandSource,
    agent_protocol::AgentCommandOutcome,
    Vec<DurableOperation>,
)> {
    let session = crate::state_root::load_agent_session_current(manifest, resolver, session_id)?
        .map_or_else(|| agent_protocol::AgentSessionCurrent::new(session_id), Ok)?;
    let update_current = crate::state_root::load_agent_update_current(
        manifest,
        resolver,
        session_id,
        update.update_id(),
    )?;
    let entry = match update {
        agent_protocol::AgentUpdate::Message { message, .. } => {
            agent_protocol::AgentSessionEntrySource::Message {
                current: crate::state_root::load_agent_message_current(
                    manifest,
                    resolver,
                    session_id,
                    &message.message_id,
                )?,
            }
        }
        agent_protocol::AgentUpdate::Tool { tool, .. } => {
            agent_protocol::AgentSessionEntrySource::Tool {
                current: crate::state_root::load_agent_tool_current(
                    manifest,
                    resolver,
                    session_id,
                    &tool.tool_call_id,
                )?,
            }
        }
        agent_protocol::AgentUpdate::State {
            state: agent_protocol::AgentState::Closed,
            ..
        } => {
            let mut tools = Vec::with_capacity(session.nonterminal_tools.len());
            for tool_call_id in session.nonterminal_tools.keys() {
                let current = crate::state_root::load_agent_tool_current(
                    manifest,
                    resolver,
                    session_id,
                    tool_call_id,
                )?
                .ok_or_else(|| DurableError::Integrity {
                    code: "agent_nonterminal_tool_missing".to_owned(),
                    message: format!(
                        "Agent Session {session_id} non-terminal Tool {tool_call_id} is missing"
                    ),
                })?;
                tools.push(current);
            }
            agent_protocol::AgentSessionEntrySource::Close { tools }
        }
        agent_protocol::AgentUpdate::State { .. }
        | agent_protocol::AgentUpdate::Plan { .. }
        | agent_protocol::AgentUpdate::Usage { .. } => {
            agent_protocol::AgentSessionEntrySource::Metadata
        }
        agent_protocol::AgentUpdate::Elicitation { .. } => {
            return Err(DurableError::RuntimeDefect {
                code: "agent_elicitation_reached_local_reducer".to_owned(),
                message: "Agent elicitation update reached the local Session reducer".to_owned(),
            });
        }
    };
    let update_source = agent_protocol::AgentSessionUpdateSource {
        update: update_current,
        entry,
    };
    let postcondition = session.reduce_update(&command.command_id, update, &update_source)?;
    let mut operations = Vec::new();
    append_agent_session_postcondition(&mut operations, &postcondition);
    Ok((
        agent_protocol::AgentCommandSource::Session {
            session,
            update: update_source,
        },
        agent_protocol::AgentCommandOutcome::Session(postcondition),
        operations,
    ))
}

fn prepare_agent_occurrence<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
    occurrence: &agent_protocol::AgentHostOccurrence,
) -> DurableResult<(
    agent_protocol::AgentCommandSource,
    agent_protocol::AgentCommandOutcome,
    Vec<DurableOperation>,
)> {
    let session =
        crate::state_root::load_agent_session_current(manifest, resolver, &occurrence.session_id)?
            .map_or_else(
                || agent_protocol::AgentSessionCurrent::new(&occurrence.session_id),
                Ok,
            )?;
    let source = agent_protocol::AgentOccurrenceSource {
        session,
        current: crate::state_root::load_agent_occurrence_current(
            manifest,
            resolver,
            &occurrence.session_id,
            &occurrence.occurrence_id,
        )?,
    };
    let postcondition = source.reduce(&command.command_id, occurrence)?;
    let operations = vec![
        DurableOperation::PutAgentSessionCurrent {
            value: postcondition.session.clone(),
        },
        DurableOperation::PutAgentOccurrenceCurrent {
            value: postcondition.current.clone(),
        },
    ];
    Ok((
        agent_protocol::AgentCommandSource::Occurrence(source),
        agent_protocol::AgentCommandOutcome::Occurrence(postcondition),
        operations,
    ))
}

fn prepare_agent_stream<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
    stream_command: &agent_protocol::AgentStreamCommand,
) -> DurableResult<(
    agent_protocol::AgentCommandSource,
    agent_protocol::AgentCommandOutcome,
    Vec<DurableOperation>,
)> {
    let source = match stream_command {
        agent_protocol::AgentStreamCommand::Open {
            session_id,
            stream_id,
            target,
            ..
        } => agent_protocol::AgentStreamSource::Open {
            session: crate::state_root::load_agent_session_current(manifest, resolver, session_id)?
                .map_or_else(|| agent_protocol::AgentSessionCurrent::new(session_id), Ok)?,
            stream: crate::state_root::load_agent_stream_current(
                manifest, resolver, session_id, stream_id,
            )?,
            target: load_agent_stream_target(manifest, resolver, session_id, target)?,
        },
        agent_protocol::AgentStreamCommand::AppendChunk {
            session_id,
            stream_id,
            chunk,
        } => agent_protocol::AgentStreamSource::AppendChunk {
            stream: require_agent_stream(manifest, resolver, session_id, stream_id)?,
            current_chunk: crate::state_root::load_agent_stream_chunk_current(
                manifest,
                resolver,
                session_id,
                stream_id,
                chunk.sequence,
            )?,
        },
        agent_protocol::AgentStreamCommand::Abort {
            session_id,
            stream_id,
            ..
        } => agent_protocol::AgentStreamSource::Abort {
            session: require_agent_session(manifest, resolver, session_id)?,
            stream: require_agent_stream(manifest, resolver, session_id, stream_id)?,
        },
        agent_protocol::AgentStreamCommand::Finalize { .. } => {
            return Err(DurableError::RuntimeDefect {
                code: "agent_finalize_reached_local_stream_reducer".to_owned(),
                message: "Agent stream finalization reached the local stream reducer".to_owned(),
            });
        }
    };
    let postcondition = source.reduce(&command.command_id, stream_command)?;
    let mut operations = vec![DurableOperation::PutAgentStreamCurrent {
        value: postcondition.stream.clone(),
    }];
    match &postcondition.effect {
        agent_protocol::AgentStreamEffect::Opened { session }
        | agent_protocol::AgentStreamEffect::Aborted { session } => {
            operations.push(DurableOperation::PutAgentSessionCurrent {
                value: session.clone(),
            });
        }
        agent_protocol::AgentStreamEffect::Chunk { current } => {
            operations.push(DurableOperation::PutAgentStreamChunkCurrent {
                value: current.clone(),
            });
        }
        agent_protocol::AgentStreamEffect::Finalized { .. } => {
            return Err(DurableError::RuntimeDefect {
                code: "agent_local_stream_reducer_finalized".to_owned(),
                message: "local Agent stream reducer produced a finalization".to_owned(),
            });
        }
    }
    Ok((
        agent_protocol::AgentCommandSource::Stream(Box::new(source)),
        agent_protocol::AgentCommandOutcome::Stream(postcondition),
        operations,
    ))
}

fn load_agent_stream_finalization_source<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentStreamCommand,
) -> DurableResult<agent_protocol::AgentStreamSource> {
    let agent_protocol::AgentStreamCommand::Finalize {
        session_id,
        stream_id,
    } = command
    else {
        return Err(DurableError::RuntimeDefect {
            code: "agent_finalize_source_command_mismatch".to_owned(),
            message: "Agent finalization source loader received another stream command".to_owned(),
        });
    };
    let session = require_agent_session(manifest, resolver, session_id)?;
    let stream = require_agent_stream(manifest, resolver, session_id, stream_id)?;
    let mut chunks = Vec::with_capacity(
        usize::try_from(stream.next_chunk_sequence)
            .map_err(|error| DurableError::Validation(error.to_string()))?,
    );
    for sequence in 0..stream.next_chunk_sequence {
        let chunk = crate::state_root::load_agent_stream_chunk_current(
            manifest, resolver, session_id, stream_id, sequence,
        )?
        .ok_or_else(|| DurableError::Integrity {
            code: "agent_finalize_chunk_missing".to_owned(),
            message: format!("Agent stream {stream_id} lost staged chunk ordinal {sequence}"),
        })?;
        chunks.push(chunk);
    }
    let target = load_agent_stream_target(manifest, resolver, session_id, &stream.target)?;
    let update_id = agent_protocol::agent_stream_final_update_id(session_id, stream_id)?;
    let update =
        crate::state_root::load_agent_update_current(manifest, resolver, session_id, &update_id)?;
    Ok(agent_protocol::AgentStreamSource::Finalize {
        session,
        stream,
        chunks,
        target,
        update,
        resource: None,
    })
}

fn attach_agent_stream_resource_source(
    source: agent_protocol::AgentStreamSource,
    resource_source: agent_protocol::AgentStreamResourceSource,
) -> DurableResult<agent_protocol::AgentStreamSource> {
    let agent_protocol::AgentStreamSource::Finalize {
        session,
        stream,
        chunks,
        target,
        update,
        resource,
    } = source
    else {
        return Err(DurableError::RuntimeDefect {
            code: "agent_finalize_resource_source_mismatch".to_owned(),
            message: "Agent Resource source was attached outside stream finalization".to_owned(),
        });
    };
    if resource.is_some() {
        return Err(DurableError::RuntimeDefect {
            code: "agent_finalize_resource_source_reused".to_owned(),
            message: "Agent stream finalization already carried a Resource source".to_owned(),
        });
    }
    Ok(agent_protocol::AgentStreamSource::Finalize {
        session,
        stream,
        chunks,
        target,
        update,
        resource: Some(Box::new(resource_source)),
    })
}

fn agent_stream_finalization_operations(
    postcondition: &agent_protocol::AgentStreamPostcondition,
) -> DurableResult<Vec<DurableOperation>> {
    let agent_protocol::AgentStreamEffect::Finalized {
        session,
        publication_record,
        ..
    } = &postcondition.effect
    else {
        return Err(DurableError::RuntimeDefect {
            code: "agent_finalize_postcondition_mismatch".to_owned(),
            message: "Agent finalization reducer returned a non-final postcondition".to_owned(),
        });
    };
    let mut operations = vec![DurableOperation::PutAgentStreamCurrent {
        value: postcondition.stream.clone(),
    }];
    append_agent_session_postcondition(&mut operations, session);
    if let Some(record) = publication_record {
        operations.push(DurableOperation::PutResourceCatalogRecord {
            value: record.clone(),
        });
    }
    Ok(operations)
}

fn verify_agent_stream_finalization_graph<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    command: &agent_protocol::AgentCommand,
    receipt: &agent_protocol::AgentCommandReceipt,
) -> DurableResult<()> {
    receipt.verify_for(command)?;
    let agent_protocol::AgentCommandOutcome::Stream(agent_protocol::AgentStreamPostcondition {
        effect:
            agent_protocol::AgentStreamEffect::Finalized {
                publication_record,
                resource_pin_receipt,
                ..
            },
        ..
    }) = &receipt.outcome
    else {
        return Err(DurableError::Integrity {
            code: "agent_finalize_receipt_outcome_mismatch".to_owned(),
            message: "Agent finalization command retained a non-final stream outcome".to_owned(),
        });
    };
    match (publication_record, resource_pin_receipt) {
        (Some(record), Some(_)) => {
            let retained = crate::state_root::load_resource_catalog_record(
                manifest,
                resolver,
                &record.record_id,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_finalize_publication_record_missing".to_owned(),
                message: format!(
                    "Agent finalization lost Resource catalog record {}",
                    record.record_id
                ),
            })?;
            if retained != *record {
                return Err(DurableError::Integrity {
                    code: "agent_finalize_publication_record_mismatch".to_owned(),
                    message: "Agent finalization catalog record changed after commit".to_owned(),
                });
            }
            let expected =
                resource_protocol::ResourceLifecycleReceiptRef::from_agent(command, receipt)?;
            let origin_value = resource_lifecycle_origin(manifest, resolver, &expected)?;
            if !matches!(origin_value, ResolvedResourceLifecycleOrigin::Pin(_)) {
                return Err(DurableError::Integrity {
                    code: "agent_finalize_resource_origin_mismatch".to_owned(),
                    message: "Agent finalization did not retain its exact Resource pin origin"
                        .to_owned(),
                });
            }
        }
        (None, None) => {}
        _ => {
            return Err(DurableError::Integrity {
                code: "agent_finalize_resource_graph_mismatch".to_owned(),
                message: "Agent finalization retained a partial Resource graph".to_owned(),
            });
        }
    }
    Ok(())
}

fn load_agent_stream_target<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    session_id: &str,
    target: &agent_protocol::AgentStreamTarget,
) -> DurableResult<agent_protocol::AgentStreamTargetSource> {
    Ok(match target {
        agent_protocol::AgentStreamTarget::Message { message_id, .. } => {
            agent_protocol::AgentStreamTargetSource::Message {
                current: crate::state_root::load_agent_message_current(
                    manifest, resolver, session_id, message_id,
                )?,
            }
        }
        agent_protocol::AgentStreamTarget::Tool { tool_call_id } => {
            agent_protocol::AgentStreamTargetSource::Tool {
                current: crate::state_root::load_agent_tool_current(
                    manifest,
                    resolver,
                    session_id,
                    tool_call_id,
                )?,
            }
        }
    })
}

fn require_agent_session<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    session_id: &str,
) -> DurableResult<agent_protocol::AgentSessionCurrent> {
    crate::state_root::load_agent_session_current(manifest, resolver, session_id)?
        .ok_or_else(|| DurableError::NotFound(format!("Agent Session {session_id} does not exist")))
}

fn require_agent_stream<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    session_id: &str,
    stream_id: &str,
) -> DurableResult<agent_protocol::AgentStreamCurrent> {
    crate::state_root::load_agent_stream_current(manifest, resolver, session_id, stream_id)?
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "Agent stream {stream_id} does not exist in Session {session_id}"
            ))
        })
}

fn append_agent_session_postcondition(
    operations: &mut Vec<DurableOperation>,
    postcondition: &agent_protocol::AgentSessionPostcondition,
) {
    operations.push(DurableOperation::PutAgentSessionCurrent {
        value: postcondition.session.clone(),
    });
    operations.push(DurableOperation::PutAgentUpdateCurrent {
        value: postcondition.update.clone(),
    });
    match &postcondition.effect {
        agent_protocol::AgentSessionUpdateEffect::Metadata => {}
        agent_protocol::AgentSessionUpdateEffect::Closed { tools } => {
            operations.extend(
                tools
                    .iter()
                    .cloned()
                    .map(|value| DurableOperation::PutAgentToolCurrent { value }),
            );
        }
        agent_protocol::AgentSessionUpdateEffect::Message { current } => {
            operations.push(DurableOperation::PutAgentMessageCurrent {
                value: current.clone(),
            });
        }
        agent_protocol::AgentSessionUpdateEffect::Tool { current } => {
            operations.push(DurableOperation::PutAgentToolCurrent {
                value: current.clone(),
            });
        }
    }
}

enum ResolvedResourceLifecycleOrigin {
    Pin(resource_protocol::ResourcePinReceipt),
    Release(resource_protocol::ResourceReleaseReceipt),
    BeginDelete(resource_protocol::ResourceDeleteIntent),
    ReconcileDelete(resource_protocol::ResourceDeleteReceipt),
}

fn resource_lifecycle_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    reference: &resource_protocol::ResourceLifecycleReceiptRef,
) -> DurableResult<ResolvedResourceLifecycleOrigin> {
    reference.verify()?;
    match reference.profile() {
        resource_protocol::ResourceLifecycleProfile::Resource => {
            let receipt = crate::state_root::load_resource_command_receipt(
                manifest,
                resolver,
                reference.command_id(),
            )?
            .ok_or_else(|| lifecycle_origin_missing(reference))?;
            let expected = resource_protocol::ResourceLifecycleReceiptRef::from_resource(&receipt)?;
            ensure_lifecycle_reference(reference, &expected)?;
            match receipt.outcome {
                resource_protocol::ResourceCommandOutcome::Pin { receipt } => {
                    Ok(ResolvedResourceLifecycleOrigin::Pin(receipt))
                }
                resource_protocol::ResourceCommandOutcome::Release { receipt } => {
                    Ok(ResolvedResourceLifecycleOrigin::Release(receipt))
                }
                resource_protocol::ResourceCommandOutcome::BeginDelete { intent } => {
                    Ok(ResolvedResourceLifecycleOrigin::BeginDelete(intent))
                }
                resource_protocol::ResourceCommandOutcome::ReconcileDelete { receipt } => {
                    Ok(ResolvedResourceLifecycleOrigin::ReconcileDelete(receipt))
                }
                resource_protocol::ResourceCommandOutcome::GarbageCollect { .. }
                | resource_protocol::ResourceCommandOutcome::Transfer { .. }
                | resource_protocol::ResourceCommandOutcome::ActivateTransfer { .. } => {
                    Err(DurableError::Integrity {
                        code: "resource_lifecycle_origin_outcome_mismatch".to_owned(),
                        message: "Resource lifecycle reference selected a command that does not produce a current projection"
                            .to_owned(),
                    })
                }
            }
        }
        resource_protocol::ResourceLifecycleProfile::Agent => {
            let command =
                crate::state_root::load_agent_command(manifest, resolver, reference.command_id())?
                    .ok_or_else(|| lifecycle_origin_missing(reference))?;
            let receipt = crate::state_root::load_agent_command_receipt(
                manifest,
                resolver,
                reference.command_id(),
            )?
            .ok_or_else(|| lifecycle_origin_missing(reference))?;
            let expected =
                resource_protocol::ResourceLifecycleReceiptRef::from_agent(&command, &receipt)?;
            ensure_lifecycle_reference(reference, &expected)?;
            let pin = receipt.resource_pin_receipt_for(&command)?.ok_or_else(|| {
                DurableError::Integrity {
                    code: "resource_lifecycle_agent_pin_missing".to_owned(),
                    message: "Agent lifecycle origin did not produce its exact Resource pin"
                        .to_owned(),
                }
            })?;
            Ok(ResolvedResourceLifecycleOrigin::Pin(pin.clone()))
        }
        resource_protocol::ResourceLifecycleProfile::Virtual => {
            virtual_resource_lifecycle_origin(manifest, resolver, reference)
        }
    }
}

fn virtual_resource_lifecycle_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    reference: &resource_protocol::ResourceLifecycleReceiptRef,
) -> DurableResult<ResolvedResourceLifecycleOrigin> {
    let scheduler_id = reference
        .virtual_scheduler_id()
        .ok_or_else(|| DurableError::Integrity {
            code: "resource_lifecycle_virtual_partition_missing".to_owned(),
            message: "Virtual lifecycle reference lost its scheduler partition".to_owned(),
        })?;
    let receipt = crate::state_root::load_virtual_receipt(
        manifest,
        resolver,
        scheduler_id,
        reference.command_id(),
    )?
    .ok_or_else(|| lifecycle_origin_missing(reference))?;
    match &receipt.outcome {
        cymule_profile_protocol::virtual_work::VirtualPersistenceOutcome::Compacted(outcome) => {
            let expected =
                resource_protocol::ResourceLifecycleReceiptRef::from_virtual_compaction(&receipt)?;
            ensure_lifecycle_reference(reference, &expected)?;
            Ok(ResolvedResourceLifecycleOrigin::Pin(
                outcome.resource_pin.clone(),
            ))
        }
        cymule_profile_protocol::virtual_work::VirtualPersistenceOutcome::ArchiveRetired(
            outcome,
        ) => {
            let expected =
                resource_protocol::ResourceLifecycleReceiptRef::from_virtual_archive_retirement(
                    &receipt,
                )?;
            ensure_lifecycle_reference(reference, &expected)?;
            Ok(ResolvedResourceLifecycleOrigin::Release(
                outcome.resource_release.clone(),
            ))
        }
        _ => Err(DurableError::Integrity {
            code: "resource_lifecycle_virtual_outcome_mismatch".to_owned(),
            message:
                "Virtual lifecycle reference selected a receipt without an archive pin or release"
                    .to_owned(),
        }),
    }
}

fn lifecycle_origin_missing(
    reference: &resource_protocol::ResourceLifecycleReceiptRef,
) -> DurableError {
    DurableError::Integrity {
        code: "resource_lifecycle_origin_missing".to_owned(),
        message: format!(
            "Resource lifecycle reference {} lost command {}",
            reference.receipt_id(),
            reference.command_id()
        ),
    }
}

fn ensure_lifecycle_reference(
    actual: &resource_protocol::ResourceLifecycleReceiptRef,
    expected: &resource_protocol::ResourceLifecycleReceiptRef,
) -> DurableResult<()> {
    if actual != expected {
        return Err(DurableError::Integrity {
            code: "resource_lifecycle_origin_mismatch".to_owned(),
            message: format!(
                "Resource lifecycle reference {} does not match its exact command receipt",
                actual.receipt_id()
            ),
        });
    }
    Ok(())
}

fn verify_resource_retention_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &resource_protocol::ResourceRetentionCurrent,
) -> DurableResult<()> {
    current.verify()?;
    let origin = resource_lifecycle_origin(manifest, resolver, &current.last_receipt)?;
    let matches = match &origin {
        ResolvedResourceLifecycleOrigin::Pin(receipt) => {
            current.family == receipt.pin.subject.family
                && current.active_pin_count == receipt.active_pin_count
                && current.disposition == resource_protocol::ResourceRetentionDisposition::Active
        }
        ResolvedResourceLifecycleOrigin::Release(receipt) => {
            current.family == receipt.pin.subject.family
                && current.active_pin_count == receipt.active_pin_count
                && current.disposition
                    == if receipt.active_pin_count == 0 {
                        resource_protocol::ResourceRetentionDisposition::Unretained
                    } else {
                        resource_protocol::ResourceRetentionDisposition::Active
                    }
        }
        ResolvedResourceLifecycleOrigin::BeginDelete(intent) => {
            current.family == intent.target.subject.family
                && current.active_pin_count == 0
                && current.disposition
                    == resource_protocol::ResourceRetentionDisposition::DeleteFenced
        }
        ResolvedResourceLifecycleOrigin::ReconcileDelete(receipt) => {
            current.family == receipt.intent.target.subject.family
                && current.active_pin_count == 0
                && current.disposition == resource_protocol::ResourceRetentionDisposition::Deleted
        }
    };
    if !matches {
        return Err(DurableError::Integrity {
            code: "resource_retention_origin_mismatch".to_owned(),
            message: format!(
                "Resource retention {} does not match its exact lifecycle receipt",
                current.family.retention_key
            ),
        });
    }
    Ok(())
}

fn verify_resource_pin_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &resource_protocol::ResourcePinCurrent,
) -> DurableResult<()> {
    current.verify()?;
    let origin = resource_lifecycle_origin(manifest, resolver, &current.last_receipt)?;
    let matches = match (&origin, current.status) {
        (
            ResolvedResourceLifecycleOrigin::Pin(receipt),
            resource_protocol::ResourcePinStatus::Active,
        ) => current.pin == receipt.pin,
        (
            ResolvedResourceLifecycleOrigin::Release(receipt),
            resource_protocol::ResourcePinStatus::Released,
        ) => current.pin == receipt.pin,
        _ => false,
    };
    if !matches {
        return Err(DurableError::Integrity {
            code: "resource_pin_origin_mismatch".to_owned(),
            message: format!(
                "Resource pin {} does not match its exact lifecycle receipt",
                current.pin.pin_id
            ),
        });
    }
    Ok(())
}

fn verify_resource_delete_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &resource_protocol::ResourceDeleteCurrent,
) -> DurableResult<()> {
    current.verify()?;
    let origin = resource_lifecycle_origin(manifest, resolver, &current.last_receipt)?;
    let matches = match (&origin, current.status) {
        (
            ResolvedResourceLifecycleOrigin::BeginDelete(intent),
            resource_protocol::ResourceDeleteStatus::Fenced,
        ) => current.intent == *intent,
        (
            ResolvedResourceLifecycleOrigin::ReconcileDelete(receipt),
            resource_protocol::ResourceDeleteStatus::Completed,
        ) => current.intent == receipt.intent,
        _ => false,
    };
    if !matches {
        return Err(DurableError::Integrity {
            code: "resource_delete_origin_mismatch".to_owned(),
            message: format!(
                "Resource delete {} does not match its exact lifecycle receipt",
                current.intent.delete_id
            ),
        });
    }
    Ok(())
}

fn verify_resource_handoff_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &resource_protocol::ResourceHandoffCurrent,
) -> DurableResult<()> {
    current.verify()?;
    let receipt = crate::state_root::load_resource_command_receipt(
        manifest,
        resolver,
        &current.receipt.command_id,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "resource_handoff_origin_missing".to_owned(),
        message: format!(
            "Resource transfer {} lost its command receipt",
            current.receipt.handoff.transfer_id
        ),
    })?;
    if !matches!(
        receipt.outcome,
        resource_protocol::ResourceCommandOutcome::Transfer { receipt: retained }
            if retained == current.receipt
    ) {
        return Err(DurableError::Integrity {
            code: "resource_handoff_origin_mismatch".to_owned(),
            message: format!(
                "Resource transfer {} does not match its exact command receipt",
                current.receipt.handoff.transfer_id
            ),
        });
    }
    Ok(())
}

fn verify_resource_handoff_activation_origin<R: crate::StateRootResolver + ?Sized>(
    manifest: &crate::StateRootManifest,
    resolver: &mut R,
    current: &resource_protocol::ResourceHandoffActivationCurrent,
) -> DurableResult<()> {
    current.verify()?;
    let receipt = crate::state_root::load_resource_command_receipt(
        manifest,
        resolver,
        &current.receipt.command_id,
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "resource_handoff_activation_origin_missing".to_owned(),
        message: format!(
            "Resource activation {} lost its command receipt",
            current.receipt.activation.activation_id
        ),
    })?;
    if !matches!(
        receipt.outcome,
        resource_protocol::ResourceCommandOutcome::ActivateTransfer { receipt: retained }
            if retained == current.receipt
    ) {
        return Err(DurableError::Integrity {
            code: "resource_handoff_activation_origin_mismatch".to_owned(),
            message: format!(
                "Resource activation {} does not match its exact command receipt",
                current.receipt.activation.activation_id
            ),
        });
    }
    Ok(())
}

#[derive(Default)]
struct WaitActivationRead {
    existing: Option<WaitActivationReceipt>,
    waits: BTreeMap<String, WaitCondition>,
    continuations: BTreeMap<String, Continuation>,
    run_currents: BTreeMap<String, cymule_core::durable_internal::MachineRunCurrent>,
}

fn load_wait_activation_neighborhood(
    manifest: &crate::StateRootManifest,
    resolver: &mut dyn crate::StateRootResolver,
    activation: &WaitActivation,
) -> DurableResult<WaitActivationRead> {
    let existing =
        crate::state_root::load_wait_activation(manifest, resolver, &activation.activation_id)?;
    if let Some(existing) = &existing {
        existing.verify()?;
        return Ok(WaitActivationRead {
            existing: Some(existing.clone()),
            ..WaitActivationRead::default()
        });
    }
    let mut waits = BTreeMap::new();
    let mut continuations = BTreeMap::new();
    let mut run_currents = BTreeMap::new();
    for wait_id in &activation.wait_ids {
        let wait = crate::state_root::load_wait(manifest, resolver, wait_id)?
            .ok_or_else(|| DurableError::NotFound(format!("Wait {wait_id} does not exist")))?;
        crate::model::ensure_wait_activation_source_matches(&activation.source, &wait)?;
        if wait.state == WaitState::Pending {
            let continuation = continuations.entry(wait.run_id.clone()).or_insert(
                crate::state_root::load_continuation(manifest, resolver, &wait.run_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "wait_activation_continuation_missing".to_owned(),
                        message: format!(
                            "pending Wait {wait_id} lost Continuation {}",
                            wait.run_id
                        ),
                    })?,
            );
            if !continuation.wait_set.contains(wait_id) {
                return Err(DurableError::Integrity {
                    code: "wait_activation_continuation_membership_missing".to_owned(),
                    message: format!(
                        "pending Wait {wait_id} is absent from Continuation {}",
                        wait.run_id
                    ),
                });
            }
        }
        waits.insert(wait_id.clone(), wait);
    }
    let mut view = crate::state_root::pinned_machine::PinnedMachineView::open(manifest, resolver)?;
    for run_id in continuations.keys() {
        let current = view
            .run_current(run_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "wait_activation_core_run_missing".to_owned(),
                message: format!("pending Wait activation lost Core Run {run_id}"),
            })?;
        run_currents.insert(run_id.clone(), current);
    }
    Ok(WaitActivationRead {
        existing: None,
        waits,
        continuations,
        run_currents,
    })
}

fn apply_wait_result(
    wait: &WaitCondition,
    result: &cymule_core::ArtifactRef,
    continuation: &mut Continuation,
) -> DurableResult<()> {
    let frame = continuation
        .frames
        .iter_mut()
        .find(|frame| {
            frame.invocation_id == wait.owner.invocation_id
                && frame.definition_id == wait.owner.definition_id
                && frame.region_path == wait.owner.region_path
        })
        .ok_or_else(|| {
            DurableError::Validation(format!(
                "wait {} owning frame {} is missing",
                wait.wait_id, wait.owner.invocation_id
            ))
        })?;
    if let Some(bind) = &wait.owner.bind {
        match frame.locals.get(bind) {
            Some(existing) if existing == result => {}
            Some(_) => {
                return Err(DurableError::IllegalTransition(format!(
                    "wait {} owner bind {bind} already has a different Artifact",
                    wait.wait_id
                )));
            }
            None => {
                frame.locals.insert(bind.clone(), result.clone());
            }
        }
    }
    continuation.wait_set.remove(&wait.wait_id);
    if continuation.wait_set.is_empty() {
        continuation.status = ContinuationStatus::Ready;
    }
    Ok(())
}

fn ensure_direct_wait_completion(wait: &WaitCondition) -> DurableResult<()> {
    if !matches!(wait.kind, WaitKind::Input { .. }) {
        return Err(DurableError::Validation(format!(
            "wait {} requires an identified signal or timer activation",
            wait.wait_id
        )));
    }
    Ok(())
}

fn require_expected_wait_owner(
    wait: &WaitCondition,
    expected_run_id: &str,
    expected_owner: &WaitOwner,
) -> DurableResult<()> {
    if wait.run_id != expected_run_id || &wait.owner != expected_owner {
        return Err(DurableError::Validation(format!(
            "wait {} does not match the caller's exact Run and structural owner",
            wait.wait_id
        )));
    }
    Ok(())
}

fn validate_start_run_material(
    plan: &SealedPlan,
    binding: &cymule_core::ArtifactRecord,
    input: &cymule_core::ArtifactRecord,
    continuation: &Continuation,
) -> DurableResult<()> {
    plan.verify()?;
    binding.validate()?;
    input.validate()?;
    if binding.reference.kind != cymule_core::EXECUTION_BINDING_ARTIFACT_KIND
        || input.reference.kind != cymule_core::RUN_INPUT_ARTIFACT_KIND
        || continuation.status != ContinuationStatus::Ready
        || continuation.execution_claim.is_some()
        || continuation.epoch != 0
        || continuation.execution_fence != 0
        || continuation.plan_id != plan.plan_id
        || continuation.binding_context != binding.reference.artifact_id
        || continuation.state.as_ref() != Some(&input.reference)
        || continuation.frames.first().map(|frame| &frame.input) != Some(&input.reference)
    {
        return Err(DurableError::Validation(
            "StartRun material and claim-free Continuation do not form one exact initial boundary"
                .to_owned(),
        ));
    }
    let decoded_binding = ExecutionBinding::decode(&binding.bytes)?;
    if decoded_binding.artifact_ref()? != binding.reference {
        return Err(DurableError::Integrity {
            code: "start_run_binding_identity_mismatch".to_owned(),
            message: "StartRun binding bytes changed their exact Artifact identity".to_owned(),
        });
    }
    decoded_binding.admit_plan(plan)?;
    Ok(())
}

fn derive_continuation_claim_batch(
    run: &cymule_core::durable_internal::MachineRunCurrent,
    source: &Continuation,
    claim: &ContinuationExecutionClaim,
    epoch: u64,
) -> DurableResult<Vec<cymule_core::durable_internal::MachinePinnedBatchCommand>> {
    let run_id = source.run_id.as_str();
    let fence = claim.fence;
    let advance = Command::AdvanceEpoch;
    let advance_id = derived_command_id(
        DerivedCommandOperation::AdvanceContinuationEpoch,
        &(run_id, epoch, fence, &advance),
    )?;
    let begin = Command::BeginAttempt {
        attempt_id: claim.continuation_attempt_id.clone(),
        continuation_id: claim.continuation_id.clone(),
        occurrence_binding: source.binding_context.clone(),
        continuation_epoch: epoch,
        execution_fence: fence,
    };
    let begin_id = derived_command_id(
        DerivedCommandOperation::BeginContinuationAttempt,
        &(run_id, &begin),
    )?;
    Ok(vec![
        cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id: advance_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: run_id.to_owned(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Parent(
                Some(run.precondition_token()),
            ),
            command: advance,
        },
        cymule_core::durable_internal::MachinePinnedBatchCommand {
            command_id: begin_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: run_id.to_owned(),
            precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Derived,
            command: begin,
        },
    ])
}

fn prepare_start_run(
    plan: SealedPlan,
    binding: cymule_core::ArtifactRecord,
    input: cymule_core::ArtifactRecord,
    continuation: &Continuation,
) -> DurableResult<PreparedStartRun> {
    let fence = 1;
    let attempt_id = continuation_attempt_id(&continuation.run_id, 0, fence)?;
    let command_id = start_run_command_id(&plan, &binding, &input, continuation)?;
    let input_reference = input.reference.clone();
    let material = cymule_core::durable_internal::MachineStartRunMaterial::new(
        command_id.clone(),
        plan,
        binding,
        input,
    )?;
    let command = Command::StartRun {
        plan_id: continuation.plan_id.clone(),
        binding_context: continuation.binding_context.clone(),
        input: input_reference,
        material_digest: material.material_digest().to_owned(),
        initial_attempt: cymule_core::InitialAttemptSpec {
            attempt_id: attempt_id.clone(),
            continuation_id: continuation_id(&continuation.run_id)?,
            occurrence_binding: continuation.binding_context.clone(),
            continuation_epoch: 0,
            execution_fence: fence,
        },
    };
    Ok(PreparedStartRun {
        envelope: CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: DURABLE_RUNTIME_ACTOR.to_owned(),
            run_id: continuation.run_id.clone(),
            expected_precondition: None,
            command,
        },
        material,
        attempt_id,
    })
}

fn start_run_command_id(
    plan: &SealedPlan,
    binding: &cymule_core::ArtifactRecord,
    input: &cymule_core::ArtifactRecord,
    continuation: &Continuation,
) -> DurableResult<String> {
    derived_command_id(
        DerivedCommandOperation::StartRun,
        &(
            continuation.run_id.as_str(),
            plan.plan_id.as_str(),
            binding.reference.artifact_id.as_str(),
            input.reference.artifact_id.as_str(),
        ),
    )
}

fn verify_start_run_replay(
    entry: &cymule_core::MachineCommandArchiveEntry,
    batch: &cymule_core::MachineCommandBatchRecord,
    expected: &PreparedStartRun,
) -> DurableResult<()> {
    let admission = expected.material.admission();
    let plan_ids = admission
        .plans()
        .iter()
        .map(|plan| plan.plan_id.clone())
        .collect::<Vec<_>>();
    let artifacts = admission
        .artifacts()
        .iter()
        .map(|artifact| artifact.reference.clone())
        .collect::<Vec<_>>();
    let [member] = batch.members.as_slice() else {
        return Err(DurableError::Integrity {
            code: "start_run_replay_batch_member_count".to_owned(),
            message: "StartRun replay batch is not a singleton".to_owned(),
        });
    };
    let [receipt] = batch.receipts.as_slice() else {
        return Err(DurableError::Integrity {
            code: "start_run_replay_receipt_count".to_owned(),
            message: "StartRun replay did not retain its singleton receipt".to_owned(),
        });
    };
    let Some(source) = &batch.material_source else {
        return Err(DurableError::Integrity {
            code: "start_run_replay_material_source_missing".to_owned(),
            message: "StartRun replay batch lost its material source".to_owned(),
        });
    };
    if entry.command.envelope != expected.envelope
        || entry.command.batch_position != 0
        || entry.command.batch_len != 1
        || member.command_id != expected.envelope.command_id
        || receipt.command_id != expected.envelope.command_id
        || batch.material_digest.as_deref() != Some(expected.material.material_digest())
        || source.source_command_id != expected.envelope.command_id
        || source.plan_ids != plan_ids
        || source.artifacts != artifacts
        || batch.plan_ids != plan_ids
        || batch.artifacts != artifacts
    {
        return Err(DurableError::Integrity {
            code: "start_run_replay_batch_corrupt".to_owned(),
            message: "retained StartRun command, material, or batch authority changed".to_owned(),
        });
    }
    require_applied_command_receipt(receipt.clone())
}

fn derive_execution_claim(
    continuation: &Continuation,
    request: &ExecutionClaimRequest,
    clock: &ClockObservation,
    fence: u64,
    continuation_attempt_id: String,
) -> DurableResult<ContinuationExecutionClaim> {
    let logical_expires_at = clock
        .logical_time
        .checked_add(request.ttl)
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or_else(|| DurableError::Validation("execution claim expiry overflowed".to_owned()))?;
    let execution_binding_ref = ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: continuation.binding_context.clone(),
        kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
    };
    execution_binding_ref
        .validate()
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    Ok(ContinuationExecutionClaim {
        claim_version: crate::EXECUTION_CLAIM_VERSION.to_owned(),
        run_id: continuation.run_id.clone(),
        continuation_id: continuation_id(&continuation.run_id)?,
        owner: request.owner.clone(),
        continuation_attempt_id,
        fence,
        plan_id: continuation.plan_id.clone(),
        execution_binding_ref,
        clock_observation_ref: request.clock.clone(),
        logical_acquired_at: clock.logical_time,
        logical_ttl: request.ttl,
        logical_expires_at,
    })
}

fn validate_clock_receipt(
    run_id: &str,
    request: &ExecutionClaimRequest,
    clock: &ClockObservation,
) -> DurableResult<()> {
    request.verify()?;
    clock.verify()?;
    if clock.reference() != request.clock || request.clock.scope != execution_clock_scope(run_id)? {
        return Err(DurableError::Validation(
            "execution claim Clock receipt does not match its exact Run scope".to_owned(),
        ));
    }
    Ok(())
}

fn checked_next(kind: &str, current: u64) -> DurableResult<u64> {
    current
        .checked_add(1)
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or_else(|| DurableError::Validation(format!("{kind} overflowed")))
}

fn continuation_attempt_id(run_id: &str, epoch: u64, fence: u64) -> DurableResult<String> {
    content_id(CONTINUATION_ATTEMPT_ID_DOMAIN, &(run_id, epoch, fence)).map_err(Into::into)
}

fn require_applied_command_receipt(receipt: cymule_core::CommandReceipt) -> DurableResult<()> {
    if receipt.status != CommandReceiptStatus::Applied {
        return Err(DurableError::Conflict {
            expected: receipt.observed_precondition,
            current: receipt.current_precondition,
        });
    }
    Ok(())
}

fn executor_step_value(
    read: &ExecutorStepRead,
    reference: &ArtifactRef,
) -> DurableResult<serde_json::Value> {
    let record = read
        .referenced_artifacts
        .get(&reference.artifact_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_boundary_artifact_missing".to_owned(),
            message: format!(
                "executor boundary omitted exact Artifact {}",
                reference.artifact_id
            ),
        })?;
    crate::model::decode_artifact_value(reference, record)
}

fn executor_evaluate(
    read: &ExecutorStepRead,
    expression: &cymule_core::Expression,
) -> DurableResult<serde_json::Value> {
    let frame = read
        .run
        .continuation
        .frames
        .last()
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_boundary_frame_missing".to_owned(),
            message: format!("Run {} has no active frame", read.run.run.run_id),
        })?;
    let input = executor_step_value(read, &frame.input)?;
    crate::model::evaluate_expression_with(expression, &input, &frame.locals, &mut |reference| {
        read.referenced_artifacts
            .get(&reference.artifact_id)
            .cloned()
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_boundary_local_artifact_missing".to_owned(),
                message: format!(
                    "Run {} omitted local Artifact {}",
                    read.run.run.run_id, reference.artifact_id
                ),
            })
    })
}

fn verify_executor_value_artifact(
    record: &cymule_core::ArtifactRecord,
    expected_kind: &str,
    value: &serde_json::Value,
) -> DurableResult<()> {
    record.validate()?;
    let bytes = cymule_core::canonical_bytes(value)?;
    let expected = cymule_core::artifact_ref(expected_kind, &bytes)?;
    if record.reference != expected || record.bytes != bytes {
        return Err(DurableError::Integrity {
            code: "executor_boundary_artifact_mismatch".to_owned(),
            message: format!(
                "executor boundary Artifact {} is not the exact derived {expected_kind} value",
                record.reference.artifact_id
            ),
        });
    }
    Ok(())
}

struct ExecutorBoundaryContext<'a> {
    read: &'a ExecutorStepRead,
    frame: &'a crate::FrameState,
    region: &'a cymule_core::Region,
    step: Option<&'a cymule_core::Step>,
    contracts: PlanContracts,
}

impl<'a> ExecutorBoundaryContext<'a> {
    fn new(read: &'a ExecutorStepRead) -> DurableResult<Self> {
        let frame = read
            .run
            .continuation
            .frames
            .last()
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_boundary_frame_missing".to_owned(),
                message: format!("Run {} has no active frame", read.run.run.run_id),
            })?;
        let definition = read
            .run
            .plan
            .candidate
            .definitions
            .iter()
            .find(|definition| definition.id == frame.definition_id)
            .ok_or_else(|| DurableError::Integrity {
                code: "executor_boundary_definition_missing".to_owned(),
                message: format!("active definition {} is missing", frame.definition_id),
            })?;
        let region = region_at_path(&definition.body, &frame.region_path)?;
        Ok(Self {
            read,
            frame,
            region,
            step: region.steps.get(frame.next_step),
            contracts: PlanContracts::compile(&read.run.plan.candidate)?,
        })
    }

    fn running(&self, next: Continuation) -> DurableResult<Continuation> {
        let source = &self.read.run.continuation;
        next.verify_wire()?;
        if next.run_id != source.run_id
            || next.plan_id != source.plan_id
            || next.binding_context != source.binding_context
            || next.epoch != source.epoch
            || next.execution_fence != source.execution_fence
            || next.status != ContinuationStatus::Running
            || next.execution_claim != source.execution_claim
        {
            return Err(DurableError::Integrity {
                code: "executor_boundary_running_projection_mismatch".to_owned(),
                message: "derived Running Continuation changed execution authority".to_owned(),
            });
        }
        Ok(next)
    }
}

fn derive_executor_boundary(
    read: &ExecutorStepRead,
    boundary: &crate::executor::ExecutorCoreBoundary,
    settled_effect: Option<&ExecutorEffectRead>,
    ready_boundary: Option<&ExecutorReadyBoundaryRead>,
) -> DurableResult<DerivedExecutorBoundary> {
    use crate::executor::ExecutorCoreBoundary as Boundary;
    let context = ExecutorBoundaryContext::new(read)?;
    match boundary {
        Boundary::EnterInvocation { input } => derive_enter_invocation(&context, input),
        Boundary::CompleteInvocation { result } => derive_complete_invocation(&context, result),
        Boundary::AdvanceSettledEffect { intent_id, result } => {
            derive_advance_settled_effect(&context, intent_id, result.as_ref(), settled_effect)
        }
        Boundary::OpenScope => derive_open_scope(&context),
        Boundary::CommitScope { result } => derive_commit_scope(&context, result),
        Boundary::CommitRootScope => derive_commit_root_scope(&context),
        Boundary::ParkWait => derive_park_wait(&context),
        Boundary::YieldReady { reason } => {
            derive_yield_ready(&context, reason, settled_effect, ready_boundary)
        }
        Boundary::CompleteRun { result } => derive_complete_run(&context, result),
    }
}

fn derive_enter_invocation(
    context: &ExecutorBoundaryContext<'_>,
    input: &cymule_core::ArtifactRecord,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let step = context.step;
    let contracts = &context.contracts;
    let step = step.ok_or_else(|| {
        DurableError::IllegalTransition(
            "cannot enter an invocation after the Region result boundary".to_owned(),
        )
    })?;
    let Operation::Invoke {
        definition: target_definition,
        input: expression,
        ..
    } = &step.operation
    else {
        return Err(DurableError::IllegalTransition(
            "EnterInvocation requires the exact current Invoke step".to_owned(),
        ));
    };
    let value = executor_evaluate(read, expression)?;
    contracts.validate_definition_input(target_definition, &value)?;
    verify_executor_value_artifact(input, INVOCATION_INPUT_ARTIFACT_KIND, &value)?;
    let target = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|candidate| candidate.id == *target_definition)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_invoked_definition_missing".to_owned(),
            message: format!("invoked definition {target_definition} is missing"),
        })?;
    let mut invocation_path = frame.invocation_path.clone();
    invocation_path.push(cymule_core::InvocationPathSegment {
        site_id: step.id.clone(),
        region_path: frame.region_path.clone(),
        scope_id: frame.scope_id.clone(),
    });
    let invocation_id = cymule_core::plan_invocation_id(
        &source.run_id,
        &read.run.plan.plan_id,
        &read.run.plan.candidate.entry,
        &invocation_path,
    )?;
    let mut next = source.clone();
    next.frames.push(crate::FrameState {
        definition_id: target.id.clone(),
        invocation_id,
        invocation_path,
        scope_id: frame.scope_id.clone(),
        input: input.reference.clone(),
        region_path: Vec::new(),
        next_step: 0,
        locals: BTreeMap::new(),
    });
    Ok(DerivedExecutorBoundary {
        next: context.running(next)?,
        action: DerivedExecutorBoundaryAction::Projection {
            artifacts: vec![input.clone()],
        },
    })
}

fn derive_complete_invocation(
    context: &ExecutorBoundaryContext<'_>,
    result: &cymule_core::ArtifactRecord,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let region = context.region;
    let step = context.step;
    let contracts = &context.contracts;
    if step.is_some() || source.frames.len() < 2 {
        return Err(DurableError::IllegalTransition(
            "CompleteInvocation requires an exhausted non-root frame".to_owned(),
        ));
    }
    let value = executor_evaluate(read, &region.result)?;
    contracts.validate_definition_output(&frame.definition_id, &value)?;
    verify_executor_value_artifact(result, INVOCATION_RESULT_ARTIFACT_KIND, &value)?;
    let parent_index = source.frames.len() - 2;
    let parent = &source.frames[parent_index];
    let parent_definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|candidate| candidate.id == parent.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_parent_definition_missing".to_owned(),
            message: format!("parent definition {} is missing", parent.definition_id),
        })?;
    let parent_step = region_at_path(&parent_definition.body, &parent.region_path)?
        .steps
        .get(parent.next_step)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_parent_step_missing".to_owned(),
            message: "parent invocation step is missing".to_owned(),
        })?;
    let Operation::Invoke {
        definition, bind, ..
    } = &parent_step.operation
    else {
        return Err(DurableError::Integrity {
            code: "executor_parent_step_kind_mismatch".to_owned(),
            message: "parent does not point at Invoke".to_owned(),
        });
    };
    if definition != &frame.definition_id {
        return Err(DurableError::Integrity {
            code: "executor_invocation_definition_mismatch".to_owned(),
            message: "parent Invoke changed its target definition".to_owned(),
        });
    }
    let mut next = source.clone();
    next.frames.pop();
    let parent = next
        .frames
        .get_mut(parent_index)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_parent_frame_lost".to_owned(),
            message: "derived invocation completion lost its parent frame".to_owned(),
        })?;
    parent.next_step = parent
        .next_step
        .checked_add(1)
        .ok_or_else(|| DurableError::Validation("executor step index overflowed".to_owned()))?;
    if let Some(bind) = bind {
        parent.locals.insert(bind.clone(), result.reference.clone());
    }
    Ok(DerivedExecutorBoundary {
        next: context.running(next)?,
        action: DerivedExecutorBoundaryAction::Projection {
            artifacts: vec![result.clone()],
        },
    })
}

fn derive_advance_settled_effect(
    context: &ExecutorBoundaryContext<'_>,
    intent_id: &str,
    result: Option<&ArtifactRef>,
    settled_effect: Option<&ExecutorEffectRead>,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let step = context.step;
    let contracts = &context.contracts;
    let step = step.ok_or_else(|| {
        DurableError::IllegalTransition(
            "cannot advance an Effect after the Region result boundary".to_owned(),
        )
    })?;
    let Operation::Effect {
        effect,
        input,
        occurrence,
        bind,
    } = &step.operation
    else {
        return Err(DurableError::IllegalTransition(
            "AdvanceSettledEffect requires the exact current Effect step".to_owned(),
        ));
    };
    let value = executor_evaluate(read, input)?;
    contracts.validate_effect_input(effect, &value)?;
    let args = cymule_core::artifact_ref(
        cymule_core::EFFECT_ARGS_ARTIFACT_KIND,
        &cymule_core::canonical_bytes(&value)?,
    )?;
    let expected_intent = cymule_core::effect_intent_id(&cymule_core::EffectIntentIdentityInput {
        run_id: &source.run_id,
        plan_id: &read.run.plan.plan_id,
        invocation_id: &frame.invocation_id,
        site_id: &step.id,
        scope_id: &frame.scope_id,
        occurrence,
        args: &args,
        effect_schema_version: cymule_core::EFFECT_SCHEMA_VERSION,
    })?;
    let effect_read = settled_effect.ok_or_else(|| DurableError::Integrity {
        code: "executor_settled_effect_read_missing".to_owned(),
        message: "settled Effect boundary has no exact StateRoot read".to_owned(),
    })?;
    if expected_intent != *intent_id
        || effect_read.dispatch.intent_id != *intent_id
        || effect_read.dispatch.input != args
        || !matches!(
            effect_read.dispatch.state,
            OutboxState::Applied | OutboxState::NotApplied | OutboxState::CancelledBeforeRelease
        )
        || match effect_read.dispatch.state {
            OutboxState::Applied => result.is_none() || effect_read.result.is_none(),
            OutboxState::NotApplied | OutboxState::CancelledBeforeRelease => {
                result.is_some() || effect_read.result.is_some()
            }
            _ => true,
        }
        || effect_read.result.as_ref().map(|record| &record.reference) != result
        || (bind.is_some() && result.is_none())
    {
        return Err(DurableError::HistoryConflict {
            code: "executor_settled_effect_boundary_mismatch".to_owned(),
            message: "settled Effect boundary changed its exact intent or result".to_owned(),
        });
    }
    let mut next = source.clone();
    let next_frame = next
        .frames
        .last_mut()
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_effect_frame_lost".to_owned(),
            message: "derived Effect completion lost its active frame".to_owned(),
        })?;
    next_frame.next_step = next_frame
        .next_step
        .checked_add(1)
        .ok_or_else(|| DurableError::Validation("executor step index overflowed".to_owned()))?;
    if let (Some(bind), Some(result)) = (bind, result) {
        next_frame.locals.insert(bind.clone(), result.clone());
    }
    Ok(DerivedExecutorBoundary {
        next: context.running(next)?,
        action: DerivedExecutorBoundaryAction::Projection {
            artifacts: Vec::new(),
        },
    })
}

fn derive_open_scope(
    context: &ExecutorBoundaryContext<'_>,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let step = context.step;
    let step = step.ok_or_else(|| {
        DurableError::IllegalTransition(
            "cannot open a Scope after the Region result boundary".to_owned(),
        )
    })?;
    let Operation::Scope { .. } = &step.operation else {
        return Err(DurableError::IllegalTransition(
            "OpenScope requires the exact current Scope step".to_owned(),
        ));
    };
    let mut child_path = frame.region_path.clone();
    child_path.push(frame.next_step);
    let scope_id = cymule_core::plan_scope_id(
        &source.run_id,
        &read.run.plan.plan_id,
        &frame.invocation_id,
        &frame.definition_id,
        &child_path,
    )?;
    let command = Command::OpenScope {
        scope_id: scope_id.clone(),
        parent_scope: frame.scope_id.clone(),
        invocation_id: frame.invocation_id.clone(),
        invocation_path: frame.invocation_path.clone(),
        definition_id: frame.definition_id.clone(),
        region_path: frame.region_path.clone(),
        site_id: step.id.clone(),
    };
    let mut next = source.clone();
    next.scope_stack.push(scope_id.clone());
    next.frames.push(crate::FrameState {
        definition_id: frame.definition_id.clone(),
        invocation_id: frame.invocation_id.clone(),
        invocation_path: frame.invocation_path.clone(),
        scope_id,
        input: frame.input.clone(),
        region_path: child_path,
        next_step: 0,
        locals: frame.locals.clone(),
    });
    Ok(DerivedExecutorBoundary {
        next: context.running(next)?,
        action: DerivedExecutorBoundaryAction::OpenScope { command },
    })
}

fn derive_commit_scope(
    context: &ExecutorBoundaryContext<'_>,
    result: &cymule_core::ArtifactRecord,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let region = context.region;
    let step = context.step;
    if step.is_some() || source.frames.len() < 2 || source.scope_stack.len() < 2 {
        return Err(DurableError::IllegalTransition(
            "CommitScope requires an exhausted nested Scope frame".to_owned(),
        ));
    }
    let value = executor_evaluate(read, &region.result)?;
    verify_executor_value_artifact(result, SCOPE_RESULT_ARTIFACT_KIND, &value)?;
    let parent_index = source.frames.len() - 2;
    let parent = &source.frames[parent_index];
    let parent_definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|candidate| candidate.id == parent.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_parent_definition_missing".to_owned(),
            message: format!("parent definition {} is missing", parent.definition_id),
        })?;
    let parent_step = region_at_path(&parent_definition.body, &parent.region_path)?
        .steps
        .get(parent.next_step)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_parent_step_missing".to_owned(),
            message: "parent Scope step is missing".to_owned(),
        })?;
    let Operation::Scope { bind, .. } = &parent_step.operation else {
        return Err(DurableError::Integrity {
            code: "executor_parent_step_kind_mismatch".to_owned(),
            message: "parent does not point at Scope".to_owned(),
        });
    };
    let mut next = source.clone();
    next.frames.pop();
    next.scope_stack.pop();
    let parent = next
        .frames
        .get_mut(parent_index)
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_parent_frame_lost".to_owned(),
            message: "derived Scope completion lost its parent frame".to_owned(),
        })?;
    parent.next_step = parent
        .next_step
        .checked_add(1)
        .ok_or_else(|| DurableError::Validation("executor step index overflowed".to_owned()))?;
    if let Some(bind) = bind {
        parent.locals.insert(bind.clone(), result.reference.clone());
    }
    Ok(DerivedExecutorBoundary {
        next: context.running(next)?,
        action: DerivedExecutorBoundaryAction::CommitScope {
            command: Command::CommitScope {
                scope_id: frame.scope_id.clone(),
            },
            result: result.clone(),
        },
    })
}

fn derive_commit_root_scope(
    context: &ExecutorBoundaryContext<'_>,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let region = context.region;
    let step = context.step;
    let contracts = &context.contracts;
    if step.is_some()
        || source.frames.len() != 1
        || source.scope_stack.as_slice() != [cymule_core::ROOT_SCOPE_ID]
        || read.current_scope.scope_id != cymule_core::ROOT_SCOPE_ID
        || read.current_scope.status != cymule_core::ScopeStatus::Open
    {
        return Err(DurableError::IllegalTransition(
            "CommitRootScope requires the exhausted open root Scope".to_owned(),
        ));
    }
    let value = executor_evaluate(read, &region.result)?;
    contracts.validate_definition_output(&frame.definition_id, &value)?;
    Ok(DerivedExecutorBoundary {
        next: context.running(source.clone())?,
        action: DerivedExecutorBoundaryAction::CommitRootScope {
            command: Command::CommitScope {
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            },
        },
    })
}

fn derive_park_wait(
    context: &ExecutorBoundaryContext<'_>,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let step = context.step;
    let step = step.ok_or_else(|| {
        DurableError::IllegalTransition(
            "cannot park a Wait after the Region result boundary".to_owned(),
        )
    })?;
    let Operation::Wait { wait, bind } = &step.operation else {
        return Err(DurableError::IllegalTransition(
            "ParkWait requires the exact current Wait step".to_owned(),
        ));
    };
    let wait_id = derive_wait_id(
        &source.run_id,
        &read.run.plan.plan_id,
        &frame.invocation_id,
        &step.id,
    )?;
    let condition = WaitCondition {
        wait_id: wait_id.clone(),
        run_id: source.run_id.clone(),
        kind: match wait {
            cymule_core::WaitSpec::Signal { key, .. } => WaitKind::Signal { key: key.clone() },
            cymule_core::WaitSpec::Timer { timer_id } => WaitKind::Timer {
                timer_id: timer_id.clone(),
            },
            cymule_core::WaitSpec::Input {
                correlation,
                schema,
            } => WaitKind::Input {
                correlation: correlation.clone(),
                schema: schema.clone(),
            },
        },
        consume_once: matches!(
            wait,
            cymule_core::WaitSpec::Signal {
                consume_once: true,
                ..
            }
        ),
        owner: WaitOwner {
            invocation_id: frame.invocation_id.clone(),
            definition_id: frame.definition_id.clone(),
            site_id: step.id.clone(),
            region_path: frame.region_path.clone(),
            step_index: frame.next_step,
            bind: bind.clone(),
        },
        state: WaitState::Pending,
        result: None,
    };
    condition.verify_wire()?;
    let mut next = source.clone();
    let next_frame = next
        .frames
        .last_mut()
        .ok_or_else(|| DurableError::Integrity {
            code: "executor_wait_frame_lost".to_owned(),
            message: "derived Wait boundary lost its active frame".to_owned(),
        })?;
    next_frame.next_step = next_frame
        .next_step
        .checked_add(1)
        .ok_or_else(|| DurableError::Validation("executor step index overflowed".to_owned()))?;
    next.wait_set.insert(wait_id);
    next.execution_claim = None;
    next.status = ContinuationStatus::Waiting;
    Ok(DerivedExecutorBoundary {
        next,
        action: DerivedExecutorBoundaryAction::Yield {
            wait: Some(condition),
        },
    })
}

fn derive_yield_ready(
    context: &ExecutorBoundaryContext<'_>,
    reason: &crate::executor::ExecutorYieldReadyReason,
    settled_effect: Option<&ExecutorEffectRead>,
    ready_boundary: Option<&ExecutorReadyBoundaryRead>,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let step = context.step;
    match reason {
        crate::executor::ExecutorYieldReadyReason::EffectBoundary { intent_id } => {
            let effect = settled_effect.ok_or_else(|| DurableError::Integrity {
                code: "executor_yield_effect_read_missing".to_owned(),
                message: "Effect yield boundary has no exact Effect read".to_owned(),
            })?;
            if effect.dispatch.intent_id != *intent_id
                || !(matches!(
                    effect.dispatch.state,
                    OutboxState::NotApplied | OutboxState::Unknown
                ) || effect.dispatch.execution_availability
                    == cymule_core::EffectExecutionAvailability::Unavailable)
            {
                return Err(DurableError::HistoryConflict {
                    code: "executor_yield_effect_mismatch".to_owned(),
                    message: "Effect yield boundary is not owned by its exact retained intent"
                        .to_owned(),
                });
            }
        }
        crate::executor::ExecutorYieldReadyReason::ReleaseBoundary { intent_ids } => {
            let ready = ready_boundary.ok_or_else(|| DurableError::Integrity {
                code: "executor_yield_release_read_missing".to_owned(),
                message: "release yield boundary has no exact release-set read".to_owned(),
            })?;
            if intent_ids.is_empty()
                || ready.revision != read.run.revision
                || ready.unknown_intent.is_some()
                || ready.explicit_intents != *intent_ids
                || step.is_some()
                || read.current_scope.status != cymule_core::ScopeStatus::ClosedCommitted
            {
                return Err(DurableError::HistoryConflict {
                    code: "executor_yield_release_set_mismatch".to_owned(),
                    message: "release yield boundary changed the complete explicit intent set"
                        .to_owned(),
                });
            }
        }
    }
    let mut next = source.clone();
    next.execution_claim = None;
    next.status = ContinuationStatus::Ready;
    next.verify_wire()?;
    Ok(DerivedExecutorBoundary {
        next,
        action: DerivedExecutorBoundaryAction::Yield { wait: None },
    })
}

fn derive_complete_run(
    context: &ExecutorBoundaryContext<'_>,
    result: &cymule_core::ArtifactRecord,
) -> DurableResult<DerivedExecutorBoundary> {
    let read = context.read;
    let source = &context.read.run.continuation;
    let frame = context.frame;
    let region = context.region;
    let step = context.step;
    let contracts = &context.contracts;
    if step.is_some()
        || source.frames.len() != 1
        || source.scope_stack.as_slice() != [cymule_core::ROOT_SCOPE_ID]
        || read.current_scope.status != cymule_core::ScopeStatus::ClosedCommitted
    {
        return Err(DurableError::IllegalTransition(
            "CompleteRun requires an exhausted committed root Scope".to_owned(),
        ));
    }
    let value = executor_evaluate(read, &region.result)?;
    contracts.validate_definition_output(&frame.definition_id, &value)?;
    verify_executor_value_artifact(result, RESULT_ARTIFACT_KIND, &value)?;
    let mut next = source.clone();
    next.execution_claim = None;
    next.status = ContinuationStatus::Completed;
    next.verify_wire()?;
    Ok(DerivedExecutorBoundary {
        next,
        action: DerivedExecutorBoundaryAction::Complete {
            result: result.clone(),
        },
    })
}

fn derived_effect_batch_command<T: serde::Serialize>(
    run_id: &str,
    intent_id: &str,
    operation: DerivedCommandOperation,
    transition: EffectTransition,
    evidence: &T,
) -> DurableResult<cymule_core::durable_internal::MachinePinnedBatchCommand> {
    let command = Command::TransitionEffect {
        intent_id: intent_id.to_owned(),
        transition,
    };
    Ok(cymule_core::durable_internal::MachinePinnedBatchCommand {
        command_id: derived_command_id(operation, &(run_id, &command, evidence))?,
        actor: DURABLE_RUNTIME_ACTOR.to_owned(),
        run_id: run_id.to_owned(),
        precondition: cymule_core::durable_internal::MachinePinnedBatchPrecondition::Derived,
        command,
    })
}

fn derive_pinned_effect_enqueue(
    read: &ExecutorStepRead,
    args: &cymule_core::ArtifactRecord,
    dispatch: &EffectDispatch,
) -> DurableResult<(Command, Continuation, bool)> {
    let source = &read.run.continuation;
    let (frame, step) = effect_enqueue_location(read)?;
    let Operation::Effect {
        effect,
        input,
        occurrence,
        ..
    } = &step.operation
    else {
        return Err(DurableError::IllegalTransition(
            "Effect enqueue is not at an Effect step".to_owned(),
        ));
    };
    let value = executor_evaluate(read, input)?;
    PlanContracts::compile(&read.run.plan.candidate)?.validate_effect_input(effect, &value)?;
    verify_executor_value_artifact(args, cymule_core::EFFECT_ARGS_ARTIFACT_KIND, &value)?;
    let binding = ExecutionBinding::decode(&read.run.binding.bytes)?;
    binding.admit_plan(&read.run.plan)?;
    let execution_binding = binding.artifact_ref()?;
    if execution_binding != read.run.binding.reference {
        return Err(DurableError::Integrity {
            code: "effect_enqueue_binding_mismatch".to_owned(),
            message: "Effect enqueue changed its retained binding Artifact".to_owned(),
        });
    }
    let occurrence_binding = binding.occurrence_binding(ExecutionOperationKind::Effect, effect)?;
    let intent_id = cymule_core::effect_intent_id(&cymule_core::EffectIntentIdentityInput {
        run_id: &source.run_id,
        plan_id: &source.plan_id,
        invocation_id: &frame.invocation_id,
        site_id: &step.id,
        scope_id: &frame.scope_id,
        occurrence,
        args: &args.reference,
        effect_schema_version: cymule_core::EFFECT_SCHEMA_VERSION,
    })?;
    let derived_dispatch = EffectDispatch {
        intent_id,
        run_id: source.run_id.clone(),
        origin_plan_id: source.plan_id.clone(),
        operation: effect.clone(),
        input: args.reference.clone(),
        execution_binding: execution_binding.clone(),
        occurrence_binding: occurrence_binding.clone(),
        execution_availability: cymule_core::EffectExecutionAvailability::Available,
        reconciliation: cymule_core::ReconciliationState::NotRequired,
        state: OutboxState::Pending,
        claim_epoch: 0,
        claim_owner: None,
        result: None,
    };
    if *dispatch != derived_dispatch {
        return Err(DurableError::Validation(
            "Effect enqueue changed its Plan-derived intent or binding".to_owned(),
        ));
    }
    dispatch.verify_wire()?;
    let (next, eager) = derive_effect_enqueue_successor(read, effect)?;
    Ok((
        Command::ProposeEffect {
            scope_id: frame.scope_id.clone(),
            invocation_id: frame.invocation_id.clone(),
            invocation_path: frame.invocation_path.clone(),
            definition_id: frame.definition_id.clone(),
            region_path: frame.region_path.clone(),
            site_id: step.id.clone(),
            occurrence: occurrence.clone(),
            operation: effect.clone(),
            args: args.reference.clone(),
            execution_binding,
            occurrence_binding,
        },
        next,
        eager,
    ))
}

fn derive_effect_enqueue_successor(
    read: &ExecutorStepRead,
    effect: &str,
) -> DurableResult<(Continuation, bool)> {
    let source = &read.run.continuation;
    let contract = read
        .run
        .plan
        .candidate
        .effects
        .iter()
        .find(|contract| contract.id == effect)
        .ok_or_else(|| DurableError::Integrity {
            code: "effect_enqueue_contract_missing".to_owned(),
            message: "Effect enqueue has no sealed contract".to_owned(),
        })?;
    let eager = contract.profile.mutation == cymule_core::MutationKind::Observational
        && contract.profile.dispatch == cymule_core::DispatchPolicy::Eager;
    let mut next = source.clone();
    if !eager {
        let target = next
            .frames
            .last_mut()
            .ok_or_else(|| DurableError::Integrity {
                code: "effect_enqueue_frame_lost".to_owned(),
                message: "Effect enqueue derivation lost its frame".to_owned(),
            })?;
        target.next_step = target
            .next_step
            .checked_add(1)
            .ok_or_else(|| DurableError::Validation("Effect step index overflowed".to_owned()))?;
    }
    next.verify_wire()?;
    Ok((next, eager))
}

fn effect_enqueue_location(
    read: &ExecutorStepRead,
) -> DurableResult<(&crate::FrameState, &cymule_core::Step)> {
    let source = &read.run.continuation;
    let frame = source
        .frames
        .last()
        .ok_or_else(|| DurableError::Integrity {
            code: "effect_enqueue_frame_missing".to_owned(),
            message: "Effect enqueue has no current frame".to_owned(),
        })?;
    if read.current_scope.scope_id != frame.scope_id
        || read.current_scope.status != cymule_core::ScopeStatus::Open
    {
        return Err(DurableError::IllegalTransition(
            "Effect enqueue requires its exact open Scope".to_owned(),
        ));
    }
    let definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == frame.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "effect_enqueue_definition_missing".to_owned(),
            message: "Effect enqueue frame has no admitted definition".to_owned(),
        })?;
    let step = region_at_path(&definition.body, &frame.region_path)?
        .steps
        .get(frame.next_step)
        .ok_or_else(|| {
            DurableError::IllegalTransition("Effect enqueue has no current step".to_owned())
        })?;
    Ok((frame, step))
}

fn derive_pinned_effect_settlement(
    read: &ExecutorEffectRead,
    settlement: crate::executor::ExecutorEffectSettlement,
) -> DurableResult<(
    DerivedCommandOperation,
    EffectTransition,
    OutboxState,
    Option<cymule_core::ArtifactRecord>,
)> {
    let (operation, transition, state, result) = match settlement {
        crate::executor::ExecutorEffectSettlement::Observation { outcome, result } => {
            if read.dispatch.state != OutboxState::Claimed
                || read.dispatch.claim_owner.is_none()
                || read.dispatch.claim_epoch == 0
            {
                return Err(DurableError::IllegalTransition(
                    "Effect observation requires the exact retained dispatch claim".to_owned(),
                ));
            }
            let state = match outcome {
                WorldOutcome::Applied => OutboxState::Applied,
                WorldOutcome::NotApplied => OutboxState::NotApplied,
                WorldOutcome::Unknown => OutboxState::Unknown,
                WorldOutcome::Unobserved => {
                    return Err(DurableError::Validation(
                        "Effect observation cannot be Unobserved".to_owned(),
                    ));
                }
            };
            (
                DerivedCommandOperation::ObserveEffect,
                EffectTransition::Observe(outcome),
                state,
                result,
            )
        }
        crate::executor::ExecutorEffectSettlement::Reconciliation { resolution, result } => {
            if read.dispatch.state != OutboxState::Unknown
                || read.dispatch.claim_owner.is_none()
                || read.dispatch.claim_epoch == 0
            {
                return Err(DurableError::IllegalTransition(
                    "Effect reconciliation requires the original unknown dispatch claim".to_owned(),
                ));
            }
            let state = match resolution {
                ReconciliationResolution::ResolvedApplied => OutboxState::Applied,
                ReconciliationResolution::ResolvedNotApplied => OutboxState::NotApplied,
                ReconciliationResolution::StillUnknown
                | ReconciliationResolution::GovernanceRequired => OutboxState::Unknown,
            };
            (
                DerivedCommandOperation::ReconcileEffect,
                EffectTransition::Reconcile(resolution),
                state,
                result,
            )
        }
        crate::executor::ExecutorEffectSettlement::Unavailable => {
            if read.dispatch.execution_availability
                != cymule_core::EffectExecutionAvailability::Available
            {
                return Err(DurableError::IllegalTransition(
                    "Effect is already unavailable".to_owned(),
                ));
            }
            let state = match read.dispatch.state {
                OutboxState::Pending => OutboxState::CancelledBeforeRelease,
                OutboxState::Claimed | OutboxState::Unknown => OutboxState::Unknown,
                OutboxState::Applied
                | OutboxState::NotApplied
                | OutboxState::CancelledBeforeRelease => {
                    return Err(DurableError::IllegalTransition(
                        "terminal Effect cannot be made unavailable".to_owned(),
                    ));
                }
            };
            (
                DerivedCommandOperation::MarkEffectUnavailable,
                EffectTransition::MarkUnavailable,
                state,
                None,
            )
        }
    };
    if (state == OutboxState::Applied) != result.is_some() {
        return Err(DurableError::Validation(
            "Effect settlement must carry a result exactly when Applied".to_owned(),
        ));
    }
    if let Some(record) = &result {
        let value = crate::model::decode_artifact_value(&record.reference, record)?;
        PlanContracts::compile(&read.origin_plan.candidate)?
            .validate_effect_output(&read.dispatch.operation, &value)?;
        verify_executor_value_artifact(record, EFFECT_RESULT_ARTIFACT_KIND, &value)?;
    }
    Ok((operation, transition, state, result))
}

fn proposed_pinned_lease(
    previous: Option<&CoordinationLease>,
    resource: &str,
    owner: &str,
    now: u64,
    ttl: u64,
) -> DurableResult<CoordinationLease> {
    validate_wire_non_empty("lease resource", resource)?;
    validate_wire_non_empty("lease owner", owner)?;
    if now > MAX_EXACT_INTEGER || ttl == 0 || ttl > MAX_EXACT_INTEGER {
        return Err(DurableError::Validation(
            "lease requires exact logical time and positive TTL".to_owned(),
        ));
    }
    if let Some(previous) = previous {
        previous.verify()?;
        if previous.resource != resource {
            return Err(DurableError::Integrity {
                code: "lease_resource_key_mismatch".to_owned(),
                message: "lease changed its keyed resource".to_owned(),
            });
        }
        if previous.owner != owner && previous.expires_at > now {
            return Err(DurableError::Conflict {
                expected: Some(owner.to_owned()),
                current: Some(previous.owner.clone()),
            });
        }
    }
    let epoch = previous
        .map_or(Some(1), |lease| lease.epoch.checked_add(1))
        .filter(|epoch| *epoch <= MAX_EXACT_INTEGER)
        .ok_or_else(|| DurableError::Validation("lease fence overflowed".to_owned()))?;
    let expires_at = now
        .checked_add(ttl)
        .filter(|expires_at| *expires_at <= MAX_EXACT_INTEGER)
        .ok_or_else(|| DurableError::Validation("lease expiry overflowed".to_owned()))?;
    let lease = CoordinationLease {
        resource: resource.to_owned(),
        owner: owner.to_owned(),
        epoch,
        expires_at,
    };
    lease.verify()?;
    Ok(lease)
}

fn current_component_attempt_for_takeover(
    read: &ExecutorStepRead,
) -> DurableResult<Option<String>> {
    let frame = read
        .run
        .continuation
        .frames
        .last()
        .ok_or_else(|| DurableError::Integrity {
            code: "component_frontier_frame_missing".to_owned(),
            message: "component frontier lookup has no active frame".to_owned(),
        })?;
    let definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == frame.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "component_frontier_definition_missing".to_owned(),
            message: "component frontier frame has no admitted definition".to_owned(),
        })?;
    let Some(step) = region_at_path(&definition.body, &frame.region_path)?
        .steps
        .get(frame.next_step)
    else {
        return Ok(None);
    };
    let Operation::Call { input, .. } = &step.operation else {
        return Ok(None);
    };
    let value = executor_evaluate(read, input)?;
    let bytes = cymule_core::canonical_bytes(&value)?;
    let artifact = cymule_core::ArtifactRecord {
        reference: cymule_core::artifact_ref(COMPONENT_INPUT_ARTIFACT_KIND, &bytes)?,
        bytes,
    };
    derive_pinned_component_occurrence(read, &artifact)
        .map(|occurrence| Some(occurrence.occurrence_id))
}

fn validate_component_result_material(
    result: &crate::executor::ExecutorComponentResult,
) -> DurableResult<(ComponentOutcome, &cymule_core::ArtifactRecord)> {
    let (outcome, artifact) = match result {
        crate::executor::ExecutorComponentResult::Succeeded { output } => (
            ComponentOutcome::Succeeded {
                output: output.reference.clone(),
            },
            output,
        ),
        crate::executor::ExecutorComponentResult::ExpectedFailure { failure, detail } => {
            let declared: cymule_runtime::PluginExpectedFailure =
                cymule_core::decode_json(&detail.bytes)?;
            declared.verify()?;
            verify_executor_value_artifact(
                detail,
                cymule_core::DECLARED_FAILURE_ARTIFACT_KIND,
                &serde_json::to_value(&declared)?,
            )?;
            if failure.class != cymule_core::RunFailureClass::DeclaredFailure
                || failure.code != declared.code
                || failure.detail != detail.reference
            {
                return Err(DurableError::Validation(
                    "component failure does not match its declared detail".to_owned(),
                ));
            }
            (
                ComponentOutcome::ExpectedFailure {
                    code: failure.code.clone(),
                    detail: detail.reference.clone(),
                },
                detail,
            )
        }
    };
    artifact.validate()?;
    outcome.verify_wire()?;
    Ok((outcome, artifact))
}

fn derive_pinned_component_result(
    read: &ExecutorStepRead,
    result: &crate::executor::ExecutorComponentResult,
) -> DurableResult<Continuation> {
    let source = &read.run.continuation;
    let frame = source
        .frames
        .last()
        .ok_or_else(|| DurableError::Integrity {
            code: "component_result_frame_missing".to_owned(),
            message: "component result has no current frame".to_owned(),
        })?;
    let definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == frame.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "component_result_definition_missing".to_owned(),
            message: "component result frame has no admitted definition".to_owned(),
        })?;
    let step = region_at_path(&definition.body, &frame.region_path)?
        .steps
        .get(frame.next_step)
        .ok_or_else(|| {
            DurableError::IllegalTransition("component result has no current Call".to_owned())
        })?;
    let Operation::Call {
        component, bind, ..
    } = &step.operation
    else {
        return Err(DurableError::IllegalTransition(
            "component result is not at a Call".to_owned(),
        ));
    };
    let mut next = source.clone();
    match result {
        crate::executor::ExecutorComponentResult::Succeeded { output } => {
            let contract = read
                .run
                .plan
                .candidate
                .components
                .iter()
                .find(|contract| contract.id == *component)
                .ok_or_else(|| DurableError::Integrity {
                    code: "component_result_contract_missing".to_owned(),
                    message: "component result has no sealed contract".to_owned(),
                })?;
            let value = crate::model::decode_artifact_value(&output.reference, output)?;
            PlanContracts::compile(&read.run.plan.candidate)?
                .validate_component_output(component, &value)?;
            verify_executor_value_artifact(output, &contract.output_artifact_kind, &value)?;
            let frame = next
                .frames
                .last_mut()
                .ok_or_else(|| DurableError::Integrity {
                    code: "component_result_frame_lost".to_owned(),
                    message: "component result derivation lost its frame".to_owned(),
                })?;
            frame.next_step = frame.next_step.checked_add(1).ok_or_else(|| {
                DurableError::Validation("component result step overflowed".to_owned())
            })?;
            if let Some(bind) = bind {
                frame.locals.insert(bind.clone(), output.reference.clone());
            }
        }
        crate::executor::ExecutorComponentResult::ExpectedFailure { .. } => {
            next.epoch = next
                .epoch
                .checked_add(1)
                .filter(|epoch| *epoch <= MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    DurableError::Validation("component failure epoch overflowed".to_owned())
                })?;
            next.execution_fence = next
                .execution_fence
                .checked_add(1)
                .filter(|fence| *fence <= MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    DurableError::Validation("component failure fence overflowed".to_owned())
                })?;
            next.execution_claim = None;
            next.wait_set.clear();
            next.status = ContinuationStatus::Failed;
        }
    }
    next.verify_wire()?;
    Ok(next)
}

fn derive_pinned_component_occurrence(
    read: &ExecutorStepRead,
    input: &cymule_core::ArtifactRecord,
) -> DurableResult<ComponentOccurrence> {
    let source = &read.run.continuation;
    let frame = source
        .frames
        .last()
        .ok_or_else(|| DurableError::Integrity {
            code: "component_frame_missing".to_owned(),
            message: format!("Run {} has no active component frame", source.run_id),
        })?;
    if read.current_scope.scope_id != frame.scope_id
        || read.current_scope.status != cymule_core::ScopeStatus::Open
    {
        return Err(DurableError::IllegalTransition(
            "component provider Attempt requires its exact open Scope".to_owned(),
        ));
    }
    let definition = read
        .run
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == frame.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "component_definition_missing".to_owned(),
            message: format!("component definition {} is missing", frame.definition_id),
        })?;
    let step = region_at_path(&definition.body, &frame.region_path)?
        .steps
        .get(frame.next_step)
        .ok_or_else(|| {
            DurableError::IllegalTransition(
                "component provider Attempt has no current Call step".to_owned(),
            )
        })?;
    let Operation::Call {
        component,
        input: expression,
        ..
    } = &step.operation
    else {
        return Err(DurableError::IllegalTransition(
            "component provider Attempt requires the exact current Call step".to_owned(),
        ));
    };
    let value = executor_evaluate(read, expression)?;
    PlanContracts::compile(&read.run.plan.candidate)?
        .validate_component_input(component, &value)?;
    verify_executor_value_artifact(input, COMPONENT_INPUT_ARTIFACT_KIND, &value)?;
    let binding = ExecutionBinding::decode(&read.run.binding.bytes)?;
    if binding.artifact_ref()? != read.run.binding.reference {
        return Err(DurableError::Integrity {
            code: "component_binding_identity_mismatch".to_owned(),
            message: "component execution binding changed Artifact identity".to_owned(),
        });
    }
    binding.admit_plan(&read.run.plan)?;
    let selected = binding
        .components
        .get(component)
        .ok_or_else(|| DurableError::Integrity {
            code: "component_binding_missing".to_owned(),
            message: format!("component operation {component} has no exact binding"),
        })?;
    let occurrence_binding =
        binding.occurrence_binding(ExecutionOperationKind::Component, component)?;
    let mut occurrence = ComponentOccurrence {
        occurrence_version: crate::COMPONENT_OCCURRENCE_VERSION.to_owned(),
        occurrence_id: String::new(),
        run_id: source.run_id.clone(),
        plan_id: source.plan_id.clone(),
        binding_context: source.binding_context.clone(),
        invocation_id: frame.invocation_id.clone(),
        invocation_path: frame.invocation_path.clone(),
        definition_id: frame.definition_id.clone(),
        region_path: frame.region_path.clone(),
        site_id: step.id.clone(),
        step_index: frame.next_step,
        component: component.clone(),
        input: input.reference.clone(),
        outcome: None,
        occurrence_binding,
        implementation_revision: selected.operation_revision.clone(),
        attempt_count: 0,
        latest_attempt_id: String::new(),
        continuation_digest: None,
        state: crate::ComponentOccurrenceState::Pending,
    };
    occurrence.occurrence_id = component_occurrence_id(&occurrence)?;
    Ok(occurrence)
}

fn region_at_path<'a>(
    root: &'a cymule_core::Region,
    path: &[usize],
) -> DurableResult<&'a cymule_core::Region> {
    let mut region = root;
    for index in path {
        let step = region.steps.get(*index).ok_or_else(|| {
            DurableError::Validation("component Region path is invalid".to_owned())
        })?;
        let Operation::Scope { body, .. } = &step.operation else {
            return Err(DurableError::Validation(
                "component Region path crosses a non-scope step".to_owned(),
            ));
        };
        region = body;
    }
    Ok(region)
}

#[cfg(test)]
mod evolution_pinned_commit_tests {
    use super::*;
    use cymule_core::{ArtifactRecord, Definition, Expression, PlanCandidate, Region, Step};
    use cymule_profile_protocol::evolution::{
        EVOLUTION_CONTROL_VERSION, EvolutionCommand, EvolutionError, EvolutionPersistenceCommand,
        EvolutionProviders, EvolutionResult, LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand,
        LiveEvolutionOutcome, LivePublicationCommand, MigrationAdapter, PlanTemplate, RolloutMode,
        ShadowDriver, ShadowDriverDescriptor, ShadowEffectMode, ShadowOutput, SubflowReference,
    };
    use serde_json::json;

    const EVOLUTION_ID: &str = "evolution-focused";
    const TEMPLATE_ID: &str = "template-focused";
    const LOGICAL_REF: &str = "focused-flow";
    const LOCAL_DEFINITION: &str = "focused-dependency";

    struct CountingShadowDriver {
        descriptor: ShadowDriverDescriptor,
        output: ShadowOutput,
        describe_calls: usize,
        execute_calls: usize,
    }

    impl ShadowDriver for CountingShadowDriver {
        fn describe(&mut self) -> EvolutionResult<ShadowDriverDescriptor> {
            self.describe_calls += 1;
            Ok(self.descriptor.clone())
        }

        fn execute(
            &mut self,
            _request: &evolution_protocol::ShadowRequest,
        ) -> EvolutionResult<ShadowOutput> {
            self.execute_calls += 1;
            Ok(self.output.clone())
        }
    }

    struct CountingEvolutionProviders {
        target_binding_calls: usize,
        migration_adapter_calls: usize,
        shadow_driver_calls: usize,
        shadow: CountingShadowDriver,
    }

    impl CountingEvolutionProviders {
        fn new() -> Self {
            let driver_revision =
                content_id("cymule.test-shadow-driver/1", &()).expect("driver revision derives");
            let evidence_bytes = b"focused shadow evidence".to_vec();
            let evidence = ArtifactRecord {
                reference: cymule_core::artifact_ref(
                    "cymule.test-shadow-evidence/1",
                    &evidence_bytes,
                )
                .expect("shadow evidence reference derives"),
                bytes: evidence_bytes,
            };
            Self {
                target_binding_calls: 0,
                migration_adapter_calls: 0,
                shadow_driver_calls: 0,
                shadow: CountingShadowDriver {
                    descriptor: ShadowDriverDescriptor {
                        driver_id: "shadow-focused".to_owned(),
                        driver_revision,
                        target_effects: ShadowEffectMode::SuppressedOrSimulated,
                        occurrence_bindings: evolution_protocol::ShadowBindingMode::Pinned,
                    },
                    output: ShadowOutput {
                        primary_digest: "1".repeat(64),
                        shadow_digest: "2".repeat(64),
                        equivalent: false,
                        evidence,
                    },
                    describe_calls: 0,
                    execute_calls: 0,
                },
            }
        }

        fn provider_calls(&self) -> (usize, usize, usize, usize, usize) {
            (
                self.target_binding_calls,
                self.migration_adapter_calls,
                self.shadow_driver_calls,
                self.shadow.describe_calls,
                self.shadow.execute_calls,
            )
        }
    }

    impl EvolutionProviders for CountingEvolutionProviders {
        fn target_execution_binding(
            &mut self,
            plan_id: &str,
        ) -> EvolutionResult<cymule_runtime::ExecutionBinding> {
            self.target_binding_calls += 1;
            Err(EvolutionError::NotFound(format!(
                "test has no target binding for {plan_id}"
            )))
        }

        fn migration_adapter(
            &mut self,
            adapter_id: &str,
            adapter_revision: &str,
        ) -> EvolutionResult<&mut dyn MigrationAdapter> {
            self.migration_adapter_calls += 1;
            Err(EvolutionError::NotFound(format!(
                "test has no migration adapter {adapter_id}@{adapter_revision}"
            )))
        }

        fn shadow_driver(
            &mut self,
            driver_id: &str,
            driver_revision: &str,
        ) -> EvolutionResult<&mut dyn ShadowDriver> {
            self.shadow_driver_calls += 1;
            if driver_id != self.shadow.descriptor.driver_id
                || driver_revision != self.shadow.descriptor.driver_revision
            {
                return Err(EvolutionError::NotFound(format!(
                    "test has no shadow driver {driver_id}@{driver_revision}"
                )));
            }
            Ok(&mut self.shadow)
        }
    }

    struct ShadowEvolutionFixture {
        coordinator: DurableCoordinator<crate::MemoryStore>,
        providers: CountingEvolutionProviders,
        previous_plan: String,
        current_plan: String,
        decision_id: String,
        retained_input: ArtifactRef,
    }

    fn definition(value: &str) -> Definition {
        Definition {
            id: "focused".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: Vec::new(),
                result: Expression::Literal {
                    value: json!(value),
                },
            },
        }
    }

    fn template() -> PlanTemplate {
        PlanTemplate {
            template_id: TEMPLATE_ID.to_owned(),
            candidate: PlanCandidate {
                ir_version: cymule_core::IR_VERSION.to_owned(),
                name: "focused-parent".to_owned(),
                entry: "main".to_owned(),
                components: Vec::new(),
                effects: Vec::new(),
                definitions: vec![Definition {
                    id: "main".to_owned(),
                    input_schema: json!({}),
                    output_schema: json!({}),
                    body: Region {
                        steps: vec![Step {
                            id: "invoke-focused".to_owned(),
                            operation: cymule_core::Operation::Invoke {
                                definition: LOCAL_DEFINITION.to_owned(),
                                input: Expression::Input,
                                bind: Some("focused-result".to_owned()),
                            },
                        }],
                        result: Expression::Binding {
                            name: "focused-result".to_owned(),
                        },
                    },
                }],
                metadata: BTreeMap::new(),
            },
            references: vec![SubflowReference::latest_compatible(
                LOGICAL_REF,
                LOCAL_DEFINITION,
                json!({}),
                json!({}),
            )],
        }
    }

    fn persistence(command: LiveEvolutionCommand) -> EvolutionPersistenceCommand {
        EvolutionPersistenceCommand::new(EVOLUTION_ID, command)
            .expect("focused Evolution command seals")
    }

    fn publish_definition(command_id: &str, value: &str) -> EvolutionPersistenceCommand {
        persistence(LiveEvolutionCommand::PublishDefinition {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: command_id.to_owned(),
            logical_ref: LOGICAL_REF.to_owned(),
            definition: definition(value),
            references: Vec::new(),
        })
    }

    fn initialized_coordinator() -> DurableCoordinator<crate::MemoryStore> {
        DurableCoordinator::open(crate::MemoryStore::new())
            .expect("focused durable coordinator initializes")
            .initialize()
            .expect("focused durable domain initializes")
    }

    fn shadow_fixture() -> ShadowEvolutionFixture {
        let mut coordinator = initialized_coordinator();
        let mut providers = CountingEvolutionProviders::new();
        coordinator
            .commit_evolution(
                &publish_definition("publish-focused-v1", "v1"),
                &mut providers,
            )
            .expect("initial definition publishes");
        let registered = coordinator
            .commit_evolution(
                &persistence(LiveEvolutionCommand::RegisterTemplate {
                    control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "register-focused".to_owned(),
                    template: template(),
                }),
                &mut providers,
            )
            .expect("focused template registers");
        let LiveEvolutionOutcome::TemplateRegistered { linked } = &registered.receipt.outcome
        else {
            panic!("focused registration returned another outcome")
        };
        let previous_plan = linked.plan.plan_id.clone();
        let evidence_bytes = b"focused publication evidence".to_vec();
        let evidence = ArtifactRecord {
            reference: cymule_core::artifact_ref(
                "cymule.test-publication-evidence/1",
                &evidence_bytes,
            )
            .expect("publication evidence reference derives"),
            bytes: evidence_bytes,
        };
        let advanced = coordinator
            .commit_evolution(
                &persistence(LiveEvolutionCommand::PublishAndRelink {
                    control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                    command_id: "publish-focused-v2".to_owned(),
                    publication: LivePublicationCommand {
                        logical_ref: LOGICAL_REF.to_owned(),
                        definition: definition("v2"),
                        references: Vec::new(),
                        evidence: evidence.clone(),
                        mode: RolloutMode::Shadow,
                    },
                }),
                &mut providers,
            )
            .expect("compatible publication relinks the focused template");
        let LiveEvolutionOutcome::PublicationApplied { receipt } = &advanced.receipt.outcome else {
            panic!("focused publication returned another outcome")
        };
        let [update] = receipt.updates.as_slice() else {
            panic!("focused publication must advance exactly one template")
        };
        assert!(update.advanced);
        assert_eq!(update.previous_plan_id, previous_plan);
        assert_ne!(update.current_plan_id, update.previous_plan_id);
        let decision_id = update
            .decision_id
            .clone()
            .expect("advanced publication retains a rollout decision");
        assert_eq!(providers.provider_calls(), (0, 0, 0, 0, 0));
        ShadowEvolutionFixture {
            coordinator,
            providers,
            previous_plan,
            current_plan: update.current_plan_id.clone(),
            decision_id,
            retained_input: evidence.reference,
        }
    }

    fn shadow_command(
        outer_command_id: &str,
        inner_command_id: &str,
        comparison_id: &str,
        subject: &str,
        fixture: &ShadowEvolutionFixture,
        input: ArtifactRef,
    ) -> EvolutionPersistenceCommand {
        persistence(LiveEvolutionCommand::Apply {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: outer_command_id.to_owned(),
            template_id: TEMPLATE_ID.to_owned(),
            command: Box::new(EvolutionCommand::Shadow {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: inner_command_id.to_owned(),
                request: evolution_protocol::ShadowRequest {
                    comparison_id: comparison_id.to_owned(),
                    decision_id: fixture.decision_id.clone(),
                    subject: subject.to_owned(),
                    primary_plan: fixture.previous_plan.clone(),
                    shadow_plan: fixture.current_plan.clone(),
                    input,
                    driver_id: fixture.providers.shadow.descriptor.driver_id.clone(),
                    driver_revision: fixture.providers.shadow.descriptor.driver_revision.clone(),
                    comparison_policy: "exact-output/1".to_owned(),
                },
            }),
        })
    }

    #[test]
    fn exact_outer_receipt_replay_performs_no_provider_or_store_mutation() {
        let mut coordinator = initialized_coordinator();
        let mut providers = CountingEvolutionProviders::new();
        let command = publish_definition("publish-replay", "replay");
        let first = coordinator
            .commit_evolution(&command, &mut providers)
            .expect("first publication commits");
        let first_revision = first
            .committed_revision
            .clone()
            .expect("first publication has a committed revision");
        let store = coordinator.into_store();
        let mut coordinator =
            DurableCoordinator::open(store).expect("replay coordinator reopens exact head");
        let replay = coordinator
            .commit_evolution(&command, &mut providers)
            .expect("exact publication replays");
        assert_eq!(replay.committed_revision, None);
        assert_eq!(replay.observed_revision, first_revision);
        assert_eq!(replay.receipt, first.receipt);
        assert_eq!(providers.provider_calls(), (0, 0, 0, 0, 0));
    }

    #[test]
    fn missing_shadow_input_fails_before_any_provider_lookup_or_call() {
        let mut fixture = shadow_fixture();
        let missing = cymule_core::artifact_ref("cymule.test-shadow-input/1", b"missing")
            .expect("missing input still has a valid reference");
        let command = shadow_command(
            "shadow-missing-outer",
            "shadow-missing-inner",
            "comparison-missing",
            "subject-missing",
            &fixture,
            missing,
        );
        let error = fixture
            .coordinator
            .commit_evolution(&command, &mut fixture.providers)
            .expect_err("missing exact input must fail");
        assert!(matches!(error, DurableError::NotFound(_)));
        assert_eq!(fixture.providers.provider_calls(), (0, 0, 0, 0, 0));
    }

    #[test]
    fn fresh_shadow_calls_provider_once_and_both_replay_forms_call_it_zero_times() {
        let mut fixture = shadow_fixture();
        let command = shadow_command(
            "shadow-fresh-outer",
            "shadow-fresh-inner",
            "comparison-fresh",
            "subject-fresh",
            &fixture,
            fixture.retained_input.clone(),
        );
        let first = fixture
            .coordinator
            .commit_evolution(&command, &mut fixture.providers)
            .expect("fresh shadow commits");
        assert!(first.committed_revision.is_some());
        assert_eq!(fixture.providers.provider_calls(), (0, 0, 1, 1, 1));

        let store = fixture.coordinator.into_store();
        fixture.coordinator =
            DurableCoordinator::open(store).expect("shadow replay coordinator reopens");
        let exact = fixture
            .coordinator
            .commit_evolution(&command, &mut fixture.providers)
            .expect("outer shadow command replays exactly");
        assert_eq!(exact.committed_revision, None);
        assert_eq!(exact.receipt, first.receipt);
        assert_eq!(fixture.providers.provider_calls(), (0, 0, 1, 1, 1));

        let semantic_replay = shadow_command(
            "shadow-retained-outer",
            "shadow-retained-inner",
            "comparison-fresh",
            "subject-fresh",
            &fixture,
            fixture.retained_input.clone(),
        );
        let retained = fixture
            .coordinator
            .commit_evolution(&semantic_replay, &mut fixture.providers)
            .expect("retained semantic shadow commits a new alias without provider I/O");
        assert!(retained.committed_revision.is_some());
        assert_eq!(fixture.providers.provider_calls(), (0, 0, 1, 1, 1));
    }
}

#[cfg(test)]
mod agent_message_page_tests {
    use super::*;
    use cymule_profile_protocol::agent::{
        AgentCommand, AgentCommandAction, AgentMessage, AgentMessagePageQuery, AgentSessionQuery,
        AgentUpdate, ContentBlock, MAX_AGENT_PAGE_BYTES, MessageRole,
    };

    fn message_command(source_revision: &str, index: u64) -> DurableResult<AgentCommand> {
        AgentCommand::new(
            source_revision.to_owned(),
            AgentCommandAction::SessionUpdate {
                session_id: "session:memory-prefix".to_owned(),
                update: AgentUpdate::Message {
                    update_id: format!("update:memory:{index}"),
                    message: AgentMessage {
                        message_id: format!("message:memory:{index}"),
                        role: MessageRole::Agent,
                        content: vec![ContentBlock::Text {
                            text: format!("memory message {index}"),
                        }],
                    },
                },
            },
        )
        .map_err(Into::into)
    }

    fn page_query(
        revision: &str,
        head: Option<String>,
        count: u64,
        end_exclusive: Option<u64>,
        max_entries: u64,
    ) -> AgentMessagePageQuery {
        AgentMessagePageQuery {
            session_id: "session:memory-prefix".to_owned(),
            expected_message_head: head,
            source_message_count: count,
            end_exclusive,
            max_entries,
            max_message_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
            max_canonical_bytes: MAX_AGENT_PAGE_BYTES as u64,
            expected_revision: Some(revision.to_owned()),
        }
    }

    #[test]
    fn memory_reopen_reads_an_old_agent_message_prefix_with_any_page_size() -> DurableResult<()> {
        let mut coordinator = DurableCoordinator::open(crate::MemoryStore::new())?;
        coordinator.initialize_if_empty()?;
        for index in 0..3 {
            let source_revision = coordinator.current_revision()?.to_owned();
            let command = message_command(&source_revision, index)?;
            coordinator
                .commit_agent_local(&command)
                .expect("Agent message commits");
        }
        let revision = coordinator.current_revision()?.to_owned();
        let source = coordinator
            .read_agent_session(&AgentSessionQuery {
                session_id: "session:memory-prefix".to_owned(),
                expected_revision: Some(revision),
            })
            .expect("Agent Session reads at its exact current revision")
            .current
            .ok_or_else(|| DurableError::Integrity {
                code: "agent_message_page_test_session_missing".to_owned(),
                message: "Memory fixture lost its Agent Session".to_owned(),
            })?;
        let source_revision = coordinator.current_revision()?.to_owned();
        coordinator
            .commit_agent_local(&message_command(&source_revision, 3)?)
            .expect("later Agent message commits");
        let store = coordinator.into_store();
        let mut reopened = DurableCoordinator::open(store)?;
        let revision = reopened.current_revision()?.to_owned();
        let full = reopened.read_agent_messages(&page_query(
            &revision,
            source.message_head.clone(),
            source.message_count,
            None,
            256,
        ))?;
        assert_eq!(full.page.entries.len(), 3);
        assert_eq!(full.page.source_message_count, 3);

        let mut end = None;
        let mut single = Vec::new();
        loop {
            let read = reopened.read_agent_messages(&page_query(
                &revision,
                source.message_head.clone(),
                source.message_count,
                end,
                1,
            ))?;
            single.extend(read.page.entries);
            end = read.page.next_end_exclusive;
            if end.is_none() {
                break;
            }
        }
        single.sort_by_key(|entry| entry.order.index);
        assert_eq!(single, full.page.entries);
        assert_eq!(
            single
                .iter()
                .map(|entry| cymule_core::canonical_bytes(entry)
                    .expect("message encodes")
                    .len())
                .sum::<usize>(),
            full.page
                .entries
                .iter()
                .map(|entry| cymule_core::canonical_bytes(entry)
                    .expect("message encodes")
                    .len())
                .sum::<usize>()
        );
        Ok(())
    }
}

#[cfg(test)]
mod agent_session_close_tests {
    use super::*;
    use cymule_profile_protocol::agent::{
        AgentCommand, AgentCommandAction, AgentCommandOutcome, AgentSessionQuery,
        AgentSessionUpdateEffect, AgentState, AgentToolQuery, AgentUpdate, ToolCall,
        ToolCallStatus,
    };

    const SESSION_ID: &str = "session:durable-close";
    const TOOL_ID: &str = "tool:durable-close";

    fn tool_command(
        source_revision: &str,
        update_id: &str,
        status: ToolCallStatus,
    ) -> DurableResult<AgentCommand> {
        AgentCommand::new(
            source_revision.to_owned(),
            AgentCommandAction::SessionUpdate {
                session_id: SESSION_ID.to_owned(),
                update: AgentUpdate::Tool {
                    update_id: update_id.to_owned(),
                    tool: ToolCall {
                        tool_call_id: TOOL_ID.to_owned(),
                        operation: "test.execute".to_owned(),
                        status,
                        input: serde_json::json!({"path": "README.md"}),
                        output: None,
                        locations: vec!["workspace:test".to_owned()],
                    },
                },
            },
        )
        .map_err(Into::into)
    }

    fn close_command(source_revision: &str) -> DurableResult<AgentCommand> {
        AgentCommand::new(
            source_revision.to_owned(),
            AgentCommandAction::SessionUpdate {
                session_id: SESSION_ID.to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:session:durable-close".to_owned(),
                    state: AgentState::Closed,
                    stop_reason: None,
                },
            },
        )
        .map_err(Into::into)
    }

    #[test]
    fn memory_close_commits_session_and_tool_terminalization_in_one_cas() -> DurableResult<()> {
        let mut coordinator = DurableCoordinator::open(crate::MemoryStore::new())?;
        coordinator.initialize_if_empty()?;
        let pending_source = coordinator.current_revision()?.to_owned();
        coordinator.commit_agent_local(&tool_command(
            &pending_source,
            "update:tool:durable-pending",
            ToolCallStatus::Pending,
        )?)?;
        let pending_revision = coordinator.current_revision()?.to_owned();
        let stale_close = close_command(&pending_revision)?;
        coordinator.commit_agent_local(&tool_command(
            &pending_revision,
            "update:tool:durable-in-progress",
            ToolCallStatus::InProgress,
        )?)?;
        let in_progress_revision = coordinator.current_revision()?.to_owned();

        assert!(coordinator.commit_agent_local(&stale_close).is_err());
        assert_eq!(coordinator.current_revision()?, in_progress_revision);

        let close = close_command(&in_progress_revision)?;
        let committed = coordinator.commit_agent_local(&close)?;
        let close_revision = committed
            .committed_revision
            .clone()
            .expect("fresh close returns its exact CAS revision");
        assert_eq!(close_revision, committed.observed_revision);
        let AgentCommandOutcome::Session(postcondition) = &committed.receipt.outcome else {
            panic!("close returns one Session postcondition")
        };
        let AgentSessionUpdateEffect::Closed { tools } = &postcondition.effect else {
            panic!("close returns explicit Tool terminalization")
        };
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].tool.status, ToolCallStatus::Cancelled);

        let session = coordinator
            .read_agent_session(&AgentSessionQuery {
                session_id: SESSION_ID.to_owned(),
                expected_revision: Some(close_revision.clone()),
            })?
            .current
            .expect("closed Session exists at the close revision");
        let tool = coordinator
            .read_agent_tool(&AgentToolQuery {
                session_id: SESSION_ID.to_owned(),
                tool_call_id: TOOL_ID.to_owned(),
                expected_revision: Some(close_revision.clone()),
            })?
            .current
            .expect("cancelled Tool exists at the close revision");
        assert_eq!(session.state, AgentState::Closed);
        assert!(session.nonterminal_tools.is_empty());
        assert_eq!(tool.tool.status, ToolCallStatus::Cancelled);

        let unrelated = AgentCommand::new(
            close_revision.clone(),
            AgentCommandAction::SessionUpdate {
                session_id: "session:after-durable-close".to_owned(),
                update: AgentUpdate::State {
                    update_id: "update:after-durable-close:running".to_owned(),
                    state: AgentState::Running,
                    stop_reason: None,
                },
            },
        )?;
        let unrelated = coordinator.commit_agent_local(&unrelated)?;
        let replay = coordinator.commit_agent_local(&close)?;
        assert_eq!(replay.committed_revision, None);
        assert_eq!(replay.observed_revision, unrelated.observed_revision);
        assert_eq!(replay.receipt, committed.receipt);
        assert_eq!(coordinator.current_revision()?, replay.observed_revision);
        Ok(())
    }
}
